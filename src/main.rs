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

        tracing::info!("Instance setup; going to guess the key");
        let key_guess_start = Instant::now();
        let key_set = match wasm_blob.guess_key(instance, args_len).await {
            Ok(ks) => ks,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into())
        };
        self.add_latency("key_guess", key_guess_start.elapsed());
        tracing::info!("Key set contains {} read keys and {} write keys", key_set.read_set.len(), key_set.write_set.len());

        let check_store = self.store.clone();
        let remote_endpoint = match std::env::var("REMOTE_URL") {
            Ok(url) => url,
            Err(_) => panic!("REMOTE_ENDPOINT not set"),
        };

        let split_start = Instant::now();

        let check_body = ConsistencyCheckBody::create(&check_store, exec_id, key_set, args, remote_endpoint).await;
        let spawn_start = Instant::now();
        let check_client = ConsistencyClient::new(self.check_url.clone(), client);
        let consistency_handle = tokio::spawn(async move {
            let check_start = Instant::now();
            match check_client.do_check(check_body).await {
                Ok(res) => {
                    let duration = check_start.elapsed();
                    tracing::info!("Check duration: {} ms", duration.clone().as_millis());
                    Ok((res, duration))
                },
                Err(e) => Err(e),
            }
        });
        self.add_latency("spawn_check", spawn_start.elapsed());

        wasm_blob.store.data_mut().reset_writes();
        let wasm_blob_start = Instant::now();
        let wasm_result = match wasm_blob.run_blob(instance, args_len).await {
            Ok(res) => res,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
        };
        tracing::info!("result of wasm execution: {}", serde_json::to_string(&wasm_result).unwrap());
        let updates = wasm_blob.store.data().get_all_writes();
        self.add_latency("wasm_execution", wasm_blob_start.elapsed());

        let check_wait_start = Instant::now();
        let (check_result, check_duration) = match consistency_handle.await.unwrap() {
            Ok(res) => res,
            Err(_e) => return Err(WrapperError::CheckError("Consistency check error".to_string()).into()),
        };
        self.add_latency("check_wait", check_wait_start.elapsed());
        self.add_latency("consistency_check", check_duration);
        self.add_latency("split", split_start.elapsed());
        self.remote_latencies = check_result.latencies.clone();

        if check_result.check_result {
            tracing::info!("Consistency check passed. Collect updates and forward along.");
            self.add_latency("e2e", e2e_start.elapsed());
            let follow_up_start = Instant::now();
            self.update_sender.send(FollowupContent { updates, id: exec_id }).map_err(Box::new)?;
            self.add_latency("followup", follow_up_start.elapsed());
            return self.construct_response(wasm_result, Vec::new(), check_result.check_result);
        } else {
            tracing::info!("Consistency check failed. Return result from DC");
            if check_result.updates.len() == 0 {
                tracing::info!("No updates to apply");
            } else {
                tracing::info!("Should apply {} updates from the DC", check_result.updates.len());
                let update_start = Instant::now();
                self.store.batch_update(&check_result.updates).await;
                self.add_latency("update_state", update_start.elapsed());
            }
            self.add_latency("e2e", e2e_start.elapsed());
            return self.construct_response(wasm_result, Vec::new(), check_result.check_result);
        }
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
            })
        },
        false => {
            let region = RegionProviderChain::default_provider().or_else("eu-central-1");
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .load()
                .await;
            let client = aws_sdk_dynamodb::Client::new(&config);
            tracing::info!("Setting up dynamo client to region {}", config.region().unwrap());
            StorageProvider::Dynamo(DynamoStore {
                client,
                all_writes: Vec::new(),
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
