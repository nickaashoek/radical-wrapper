use std::{collections::HashMap, time::Duration};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use lambda_extension::{service_fn, tracing};
use lambda_http::{lambda_runtime::Diagnostic, run, Body, Error, Request, Response};
use storage::Storage;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use wasmtime::*;
use thiserror;
use uuid::{self, Uuid};
use std::time::Instant;
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::Serialize;
use serde_json::Value;

mod storage;
use storage::*;

mod consistency;
use consistency::*;

mod wasm;
use wasm::*;

mod followup;
use followup::*;


#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("linker error: {0}")]
    LinkerError(String),
    #[error("wasm execution error: {0}")]
    WasmExecError(String),
}

impl From<WasmError> for Diagnostic {
    fn from(value: WasmError) -> Diagnostic {
        let (error_type, error_message) = match value {
            WasmError::LinkerError(message) => ("LinkerError", message.to_string()),
            WasmError::WasmExecError(message) => ("WasmExecError", message.to_string()),
        };
        Diagnostic {
            error_type: error_type.into(),
            error_message: error_message.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WrapperError {
    #[error("check error: {0}")]
    CheckError(String),
    #[error("followup error: {0}")]
    FollowupError(String),
}

impl From<WrapperError> for Diagnostic {
    fn from(value: WrapperError) -> Diagnostic {
        let (error_type, error_message) = match value {
            WrapperError::CheckError(message) => ("CheckError", message.to_string()),
            WrapperError::FollowupError(message) => ("FollowupError", message.to_string()),
        };
        Diagnostic {
            error_type: error_type.into(),
            error_message: error_message.into(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ExecutionResult {
    pub result: WasmResult,
    pub read_keys: Vec<(String, Vec<u8>)>,
    pub write_updates: Vec<Value>,
    pub latencies: HashMap<String, u128>,
    pub remote_latencies: HashMap<String, i64>,
}

struct RadicalHandler<D: Storage> {
    store: D,
    latencies: HashMap<String, u128>,
    remote_latencies: HashMap<String, i64>,
    check_url: String,
    update_sender: UnboundedSender<FollowupContent>,
}

impl <D: Storage> RadicalHandler<D> {
    pub fn new(input_store: D, update_channel: UnboundedSender<FollowupContent>) -> Self {
        let check_url = match std::env::var("CHECK_URL") {
            Ok(url) => url,
            Err(_) => panic!("CHECK_URL not set"),
        };

        Self {
            store: input_store,
            latencies: HashMap::new(),
            remote_latencies: HashMap::new(),
            check_url: check_url.clone(),
            update_sender: update_channel
        }
    }

    fn add_latency(&mut self, label: &str, val: Duration) {
        self.latencies.insert(label.to_string(), val.as_millis());
    }

    fn construct_response(
        &self,
        result: WasmResult,
        updates: Vec<serde_json::Value>,
        check_status: bool
    ) -> Result<Response<Body>, Error> {
        let response_body = serde_json::json!({
            "result": result,
            "updates": updates,
            "latencies": self.latencies,
            "remote_latencies": self.remote_latencies,
            "check_status": check_status,
        });


        let resp = Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(response_body.to_string().into())
            .map_err(Box::new)?;
        Ok(resp)
    }

    pub async fn setup_wasm_blob(&mut self, args: &serde_json::Value) -> Result<(WasmBlob<D>, wasmtime::Instance), Error> {
        let wasm_setup_start = Instant::now();
        let mut config = Config::new();
        config.async_support(true);
        let blob_start = Instant::now();
        let (mut wasm_blob, read_time, setup_time) = WasmBlob::setup_blob(config, "function.serialized", self.store.clone());
        self.add_latency("blob_read", read_time);

        self.add_latency("blob_compile", setup_time);
        self.add_latency("blob_load", blob_start.elapsed());

        let blob_link_start = Instant::now();
        match wasm_blob.link_blob() {
            Ok(_) => tracing::info!("wasm module linked successfully"),
            Err(e) => return Err(WasmError::LinkerError(e.to_string()).into()),
        }
        self.add_latency("blob_link", blob_link_start.elapsed());

        let instance_setup_start = Instant::now();
        tracing::info!("Setting up wasm blob with args: {}", serde_json::to_string(args).unwrap());
        let args_vec = serde_json::to_vec(args).unwrap();
        let instance = match wasm_blob.setup_instance(args_vec).await {
            Ok(instance) => instance,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
        };
        self.add_latency("instance_setup", instance_setup_start.elapsed());
        self.add_latency("wasm_setup", wasm_setup_start.elapsed());

        Ok((wasm_blob, instance))
    }

    pub async fn edge_handler(&mut self, event: Request, client: reqwest::Client) -> Result<Response<Body>, Error> {
        let e2e_start = Instant::now();
        tracing::info!("Entering into the function at the edge");

        let body = event.body().to_vec();
        let body_json = serde_json::from_slice::<serde_json::Value>(&body)?;
        let args = body_json["args"].clone();
        let args_vec = serde_json::to_vec(&args).unwrap();
        let args_len = args_vec.len() as i32;

        let exec_id = match body_json["id"].is_null() {
            true => Uuid::new_v4(),
            false => Uuid::parse_str(body_json["id"].as_str().unwrap()).unwrap(),
        };

        let (mut wasm_blob, instance) = match self.setup_wasm_blob(&args).await {
                Ok((w, i)) => (w, i),
                Err(e) => return Err(e),
        };

        // Track read keys during execution
        let mut read_keys = Vec::new();
        let mut encountered_stale_key = false;

        // Wrap the storage operations to track reads and check for stale keys
        let store_wrapper = StorageWrapper {
            inner: &mut wasm_blob.store,
            read_keys: &mut read_keys,
            encountered_stale: &mut encountered_stale_key,
        };

        // Execute the function
        let wasm_blob_start = Instant::now();
        let wasm_result = match wasm_blob.run_blob(instance, args_len).await {
            Ok(res) => res,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
        };
        self.add_latency("wasm_execution", wasm_blob_start.elapsed());

        // Get write updates
        let get_write_start = Instant::now();
        let updates = wasm_blob.store.data().get_all_writes();
        self.add_latency("collect_writes", get_write_start.elapsed());

        // If we encountered a stale key, we need to execute at DC
        if encountered_stale_key {
            tracing::info!("[{}] Encountered stale key, executing at DC", exec_id);
            let dc_result = self.execute_at_dc(args, exec_id).await?;
            self.add_latency("e2e", e2e_start.elapsed());
            return Ok(dc_result);
        }

        // Create execution result
        let execution_result = ExecutionResult {
            result: wasm_result,
            read_keys,
            write_updates: updates,
            latencies: self.latencies.clone(),
            remote_latencies: self.remote_latencies.clone(),
        };

        // Send execution result to DC
        let dc_url = match std::env::var("DC_URL") {
            Ok(url) => url,
            Err(_) => panic!("DC_URL not set"),
        };

        let dc_client = reqwest::Client::new();
        let _ = dc_client.post(format!("{}/execution_result", dc_url))
            .json(&execution_result)
            .send()
            .await?;

        self.add_latency("e2e", e2e_start.elapsed());
        
        // Construct response
        let response_body = serde_json::json!({
            "result": execution_result.result,
            "latencies": execution_result.latencies,
            "remote_latencies": execution_result.remote_latencies,
        });

        let resp = Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(response_body.to_string().into())
            .map_err(Box::new)?;
        Ok(resp)
    }

    async fn execute_at_dc(&mut self, args: Value, exec_id: Uuid) -> Result<Response<Body>, Error> {
        let dc_url = match std::env::var("DC_URL") {
            Ok(url) => url,
            Err(_) => panic!("DC_URL not set"),
        };

        let dc_client = reqwest::Client::new();
        let dc_response = dc_client.post(&dc_url)
            .json(&serde_json::json!({
                "args": args,
                "id": exec_id.to_string()
            }))
            .send()
            .await?;

        let dc_result: serde_json::Value = dc_response.json().await?;
        
        let resp = Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(dc_result.to_string().into())
            .map_err(Box::new)?;
        Ok(resp)
    }

    pub async fn dc_handler(&mut self, event: Request) -> Result<Response<Body>, Error> {
        let e2e_start = Instant::now();
        tracing::info!("Entering into the function in the datacenter");

        let body = event.body().to_vec();
        let body_json = serde_json::from_slice::<serde_json::Value>(&body)?;
        let args = body_json["args"].clone();
        let args_vec = serde_json::to_vec(&args).unwrap();
        let args_len = args_vec.len() as i32;

        let exec_id = match body_json["id"].is_null() {
            true => Uuid::new_v4(),
            false => Uuid::parse_str(body_json["id"].as_str().unwrap()).unwrap(),
        };
        tracing::info!("Starting execution {} in the datacenter", exec_id);

        let (mut wasm_blob, instance) = match self.setup_wasm_blob(&args).await {
                Ok((w, i)) => (w, i),
                Err(e) => return Err(e),
        };

        wasm_blob.store.data_mut().reset_writes();
        let wasm_blob_start = Instant::now();
        let wasm_result = match wasm_blob.run_blob(instance, args_len).await {
            Ok(res) => res,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
        };
        tracing::info!("result of wasm execution: {}", serde_json::to_string(&wasm_result).unwrap());
        let updates = wasm_blob.store.data().get_all_writes();
        self.add_latency("wasm_execution", wasm_blob_start.elapsed());

        let collect_updates_start = Instant::now();
        self.add_latency("collect_updates", collect_updates_start.elapsed());
        self.add_latency("e2e", e2e_start.elapsed());
        self.construct_response(wasm_result, updates, false)
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    let deployment_env = match std::env::var("DEPLOYMENT") {
        Ok(env) => env,
        Err(_) => panic!("DEPLOYMENT not set"),
    };

    let use_scylla = match std::env::var("USE_SCYLLA") {
        Ok(env) => env.as_str() == "true",
        Err(_) => false
    };

    if use_scylla {
        tracing::info!("Using scylla as the dynamo backend through the alternator");
    } else {
        tracing::info!("Using DynamoDB instead of Scylla as the storage backend");
    }

    let near_user = match deployment_env.as_str() {
        "edge" => true,
        "datacenter" => false,
        _ => panic!("unknown deployment env")
    };

    let handler_client = reqwest::Client::new();
    let followup_client = reqwest::Client::new();

    // Set up the store for the wasm function to use
    let store: StorageProvider = match use_scylla {
        true => {
            let scylla_ep = match std::env::var("SCYLLA_EP") {
                Ok(env) => env,
                Err(_) => panic!("Trying to use scylla without a specified endpoint"),
            };
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region("None")
                .endpoint_url(scylla_ep)
                .load().await;
            StorageProvider::Dynamo(DynamoStore {
                client: aws_sdk_dynamodb::Client::new(&config),
                all_writes: Vec::new(),
                stale_keys: Vec::new(),
            })
        },
        false => {
            let use_local = match std::env::var("DYNAMODB_LOCAL") {
                Ok(env) => env.as_str() == "true",
                Err(_) => false
            };

            let config = if use_local {
                let local_endpoint = match std::env::var("DYNAMODB_LOCAL_ENDPOINT") {
                    Ok(endpoint) => endpoint,
                    Err(_) => String::from("http://localhost:8000"),
                };
                tracing::info!("Using DynamoDB Local at endpoint: {}", local_endpoint);
                aws_config::defaults(BehaviorVersion::latest())
                    .region("local")
                    .endpoint_url(local_endpoint)
                    .load()
                    .await
            } else {
                let region = RegionProviderChain::default_provider().or_else("eu-central-1");
                aws_config::defaults(BehaviorVersion::latest())
                    .region(region)
                    .load()
                    .await
            };

            let client = aws_sdk_dynamodb::Client::new(&config);
            tracing::info!("Setting up dynamo client to region {}", config.region().unwrap());
            StorageProvider::Dynamo(DynamoStore {
                client,
                all_writes: Vec::new(),
                stale_keys: Vec::new(),
            })
        },
    };

    // Setup the radical handler
    let (update_sender, update_receiver) = unbounded_channel::<FollowupContent>();
    let radical_handler = Arc::new(Mutex::new(RadicalHandler::new(store.clone(), update_sender)));
    if !near_user {
        // If we're in the datacenter, don't need to worry about the extension since we never follow up
        run(service_fn(|event: Request| async {
            radical_handler.lock().await.dc_handler(event).await
        })).await
    } else {
        // Setup the lambda_extension
        let followup_ext = Arc::new(
            FollowupExtension::new(followup_client.clone(), update_receiver)
        );
        let extension = lambda_extension::Extension::new()
            .with_events(&["INVOKE"])
            .with_events_processor(service_fn(|event| {
                let followup_ext = followup_ext.clone();
                async move { followup_ext.invoke(event).await }
            }))
            .with_extension_name("followup-extension")
            .register()
            .await?;

        tracing::info!("[main] setup extension");

        tokio::try_join!(
            run(service_fn(|event: Request| async {
                radical_handler.lock().await.edge_handler(event, handler_client.clone()).await
            })),
            extension.run(),
        )?;
        Ok(())
    }
}
