use std::{collections::HashMap, time::Duration};
use serde_json::Value;
use thiserror;
use uuid::{self, Uuid};
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;

use crate::storage::Storage;
use crate::wasm::*;
use crate::followup::*;
use crate::consistency::*;

#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("linker error: {0}")]
    LinkerError(String),
    #[error("wasm execution error: {0}")]
    WasmExecError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum WrapperError {
    #[error("check error: {0}")]
    CheckError(String),
    #[error("followup error: {0}")]
    FollowupError(String),
    #[error("function not found: {0}")]
    FunctionNotFound(String),
}

#[derive(serde::Serialize)]
pub struct HandlerResponse {
    pub result: WasmResult,
    pub updates: Vec<Value>,
    pub latencies: HashMap<String, u128>,
    pub remote_latencies: HashMap<String, i64>,
    pub check_status: bool,
}

pub struct RadicalHandler<D: Storage> {
    store: D,
    latencies: HashMap<String, u128>,
    remote_latencies: HashMap<String, i64>,
    check_url: String,
    update_sender: UnboundedSender<FollowupContent>,
}

impl <D: Storage> RadicalHandler<D> {
    pub fn new(input_store: D, update_channel: UnboundedSender<FollowupContent>, check_url: String) -> Self {
        Self {
            store: input_store,
            latencies: HashMap::new(),
            remote_latencies: HashMap::new(),
            check_url,
            update_sender: update_channel
        }
    }

    fn add_latency(&mut self, label: &str, val: Duration) {
        self.latencies.insert(label.to_string(), val.as_millis());
    }

    fn construct_response(
        &self,
        result: WasmResult,
        updates: Vec<Value>,
        check_status: bool
    ) -> HandlerResponse {
        HandlerResponse {
            result,
            updates,
            latencies: self.latencies.clone(),
            remote_latencies: self.remote_latencies.clone(),
            check_status,
        }
    }

    pub async fn setup_wasm_blob(
        &mut self,
        module_cache: &WasmModuleCache,
        function_name: &str,
        args: &Value
    ) -> Result<WasmBlob<D>, WrapperError> {
        let wasm_setup_start = Instant::now();
        
        // Get precompiled module from cache
        let module = module_cache.get_module(function_name)
            .ok_or_else(|| WrapperError::FunctionNotFound(function_name.to_string()))?;
        
        let blob_start = Instant::now();
        let mut wasm_blob = WasmBlob::from_module(
            module_cache.engine(),
            module.clone(),
            self.store.clone()
        );
        self.add_latency("blob_load", blob_start.elapsed());

        let blob_link_start = Instant::now();
        match wasm_blob.link_blob() {
            Ok(_) => tracing::info!("wasm module linked successfully"),
            Err(e) => return Err(WrapperError::CheckError(format!("Link error: {}", e))),
        }
        self.add_latency("blob_link", blob_link_start.elapsed());

        let instance_setup_start = Instant::now();
        tracing::info!("Setting up wasm blob with args: {}", serde_json::to_string(args).unwrap());
        let args_vec = serde_json::to_vec(args).unwrap();
        let _instance = match wasm_blob.setup_instance(args_vec).await {
            Ok(instance) => instance,
            Err(e) => return Err(WrapperError::CheckError(format!("Instance setup error: {}", e))),
        };
        self.add_latency("instance_setup", instance_setup_start.elapsed());
        self.add_latency("wasm_setup", wasm_setup_start.elapsed());

        Ok(wasm_blob)
    }

    pub async fn edge_handler(
        &mut self,
        module_cache: &WasmModuleCache,
        function_name: &str,
        args: Value,
        exec_id: Option<Uuid>,
        client: reqwest::Client,
        remote_url: &str,
        edge_id: &str,
        edge_endpoint: &str,
    ) -> Result<HandlerResponse, WrapperError> {
        let e2e_start = Instant::now();
        tracing::info!("Entering into the function at the edge");

        let args_vec = serde_json::to_vec(&args).unwrap();
        let args_len = args_vec.len() as i32;

        let exec_id = exec_id.unwrap_or_else(|| Uuid::new_v4());

        let mut wasm_blob = self.setup_wasm_blob(module_cache, function_name, &args).await?;
        
        // Setup instance for key guessing
        let instance = match wasm_blob.setup_instance(args_vec.clone()).await {
            Ok(i) => i,
            Err(e) => return Err(WrapperError::CheckError(format!("Instance error: {}", e))),
        };

        tracing::info!("[{}] Instance setup; going to guess the key", exec_id);
        let key_guess_start = Instant::now();
        let key_set = match wasm_blob.guess_key(instance, args_len).await {
            Ok(ks) => ks,
            Err(e) => return Err(WrapperError::CheckError(format!("Key guess error: {}", e))),
        };
        self.add_latency("key_guess", key_guess_start.elapsed());
        tracing::info!("[{}] Key set contains {} read keys and {} write keys", exec_id, key_set.read_set.len(), key_set.write_set.len());

        let check_store = self.store.clone();
        let remote_endpoint = format!("{}/{}", remote_url, function_name);

        let split_start = Instant::now();

        let body_start = Instant::now();
        let check_body = ConsistencyCheckBody::create(
            &check_store,
            exec_id,
            key_set,
            args.clone(),
            remote_endpoint,
            edge_id.to_string(),
            edge_endpoint.to_string()
        ).await;
        self.add_latency("body_end", body_start.elapsed());
        
        let spawn_start = Instant::now();
        let check_client = ConsistencyClient::new(self.check_url.clone(), client);
        let consistency_handle = tokio::spawn(async move {
            let check_start = Instant::now();
            match check_client.do_check(check_body).await {
                Ok(res) => {
                    let duration = check_start.elapsed();
                    tracing::info!("[{}] Check duration: {} ms", exec_id, duration.clone().as_millis());
                    Ok((res, duration))
                },
                Err(e) => Err(e),
            }
        });
        self.add_latency("spawn_check", spawn_start.elapsed());

        // Create new instance for execution
        let instance = match wasm_blob.setup_instance(args_vec).await {
            Ok(i) => i,
            Err(e) => return Err(WrapperError::CheckError(format!("Instance error: {}", e))),
        };

        let reset_start = Instant::now();
        wasm_blob.store.data_mut().reset_writes();
        self.add_latency("reset_writes", reset_start.elapsed());
        
        let wasm_blob_start = Instant::now();
        let wasm_result = match wasm_blob.run_blob(instance, args_len).await {
            Ok(res) => res,
            Err(e) => return Err(WrapperError::CheckError(format!("WASM execution error: {}", e))),
        };
        tracing::info!("[{}] result of wasm execution: {}", exec_id, serde_json::to_string(&wasm_result).unwrap());
        self.add_latency("wasm_execution", wasm_blob_start.elapsed());
        
        let get_write_start = Instant::now();
        let updates = wasm_blob.store.data().get_all_writes();
        self.add_latency("collect_writes", get_write_start.elapsed());

        let check_wait_start = Instant::now();
        let (check_result, check_duration) = match consistency_handle.await.unwrap() {
            Ok(res) => res,
            Err(_e) => return Err(WrapperError::CheckError("Consistency check error".to_string())),
        };
        self.add_latency("check_wait", check_wait_start.elapsed());
        self.add_latency("consistency_check", check_duration);
        self.add_latency("split", split_start.elapsed());
        self.remote_latencies = check_result.latencies.clone();

        if check_result.check_result {
            tracing::info!("[{}] Consistency check passed. Collect updates and forward along.", exec_id);
            self.add_latency("e2e", e2e_start.elapsed());
            let follow_up_start = Instant::now();
            self.update_sender.send(FollowupContent { updates, id: exec_id })
                .map_err(|e| WrapperError::FollowupError(e.to_string()))?;
            self.add_latency("followup", follow_up_start.elapsed());
            return Ok(self.construct_response(wasm_result, Vec::new(), check_result.check_result));
        } else {
            tracing::info!("[{}] Consistency check failed. Return result from DC", exec_id);
            if check_result.updates.len() == 0 {
                tracing::info!("[{}] No updates to apply", exec_id);
            } else {
                tracing::info!("[{}] Should apply {} updates from the DC", exec_id, check_result.updates.len());
                let update_start = Instant::now();
                self.store.batch_update(&check_result.updates).await;
                self.add_latency("update_state", update_start.elapsed());
            }
            self.add_latency("e2e", e2e_start.elapsed());
            return Ok(self.construct_response(wasm_result, Vec::new(), check_result.check_result));
        }
    }

    pub async fn dc_handler(
        &mut self,
        module_cache: &WasmModuleCache,
        function_name: &str,
        args: Value,
        exec_id: Option<Uuid>,
    ) -> Result<HandlerResponse, WrapperError> {
        let e2e_start = Instant::now();
        tracing::info!("Entering into the function in the datacenter");

        let args_vec = serde_json::to_vec(&args).unwrap();
        let args_len = args_vec.len() as i32;

        let exec_id = exec_id.unwrap_or_else(|| Uuid::new_v4());
        tracing::info!("Starting execution {} in the datacenter", exec_id);

        let mut wasm_blob = self.setup_wasm_blob(module_cache, function_name, &args).await?;
        
        let instance = match wasm_blob.setup_instance(args_vec).await {
            Ok(i) => i,
            Err(e) => return Err(WrapperError::CheckError(format!("Instance error: {}", e))),
        };

        wasm_blob.store.data_mut().reset_writes();
        let wasm_blob_start = Instant::now();
        let wasm_result = match wasm_blob.run_blob(instance, args_len).await {
            Ok(res) => res,
            Err(e) => return Err(WrapperError::CheckError(format!("WASM execution error: {}", e))),
        };
        tracing::info!("result of wasm execution: {}", serde_json::to_string(&wasm_result).unwrap());
        let updates = wasm_blob.store.data().get_all_writes();
        self.add_latency("wasm_execution", wasm_blob_start.elapsed());

        let collect_updates_start = Instant::now();
        self.add_latency("collect_updates", collect_updates_start.elapsed());
        self.add_latency("e2e", e2e_start.elapsed());
        Ok(self.construct_response(wasm_result, updates, false))
    }
}

