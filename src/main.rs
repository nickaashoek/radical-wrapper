use std::collections::HashMap;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use lambda_http::{lambda_runtime::Diagnostic, run, service_fn, tracing, Body, Error, Request, Response};
use storage::Storage;
use wasmtime::*;
use thiserror;
use uuid::{self, Uuid};
use std::time::Instant;

mod storage;
use storage::*;

mod consistency;
use consistency::*;

mod wasm;
use wasm::*;


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

async fn entry_point<D: Storage + 'static>(_event: Request, store: &mut D, near_user: bool, client: reqwest::Client) -> Result<Response<Body>, Error> {
    let e2e_start = Instant::now();
    tracing::info!("Entering into the function");
    let check_url = match std::env::var("CHECK_URL") {
        Ok(url) => url,
        Err(_) => panic!("CHECK_URL not set"),
    };

    tracing::info!("Getting the body json from the request to the function");
    let body = _event.body().to_vec();
    let body_json= serde_json::from_slice::<serde_json::Value>(&body)?;
    let args = body_json["args"].clone();
    let mut latencies: HashMap<String, u128> = HashMap::new();
    let mut exec_id = Uuid::new_v4();
    if !body_json["id"].is_null() {
        exec_id = Uuid::parse_str(body_json["id"].as_str().unwrap()).unwrap();
    }

    let check_client = ConsistencyClient::new(check_url.clone(), client);

    let wasm_setup_start = Instant::now();
    let mut config = Config::new();
    config.async_support(true);
    let blob_start= Instant::now();
    let (mut wasm_blob, read_time, setup_time) = WasmBlob::setup_blob(config, "function.serialized", store.clone());
    latencies.insert("blob_read".to_string(), read_time.as_millis());
    latencies.insert("blob_compile".to_string(), setup_time.as_millis());
    let blob_load_duration = blob_start.elapsed();
    latencies.insert("blob_load".to_string(), blob_load_duration.as_millis());

    // Setup the wasm blob to link the read/write functions
    let blob_link_start = Instant::now();
    match wasm_blob.link_blob() {
        Ok(_) => tracing::info!("Wasm module linked successfully"),
        Err(e) => return Err(WasmError::LinkerError(e.to_string()).into()),
    }
    let blob_link_duration = blob_link_start.elapsed();
    latencies.insert("blob_link".to_string(), blob_link_duration.as_millis());

    tracing::info!("Setting up the instance");
    // Setup an instance of the blob that we can use to run the function + guess
    let instant_setup_start = Instant::now();
    tracing::info!("Setting up wasm blob with args: {}", serde_json::to_string(&args).unwrap());
    let args_vec = serde_json::to_vec(&args).unwrap();
    let arg_len = args_vec.len() as i32;
    let instance = match wasm_blob.setup_instance(args_vec).await {
        Ok(instance) => instance,
        Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
    };
    let instant_setup_duration = instant_setup_start.elapsed();
    latencies.insert("instance_setup".to_string(), instant_setup_duration.as_millis());
    let wasm_setup_duration = wasm_setup_start.elapsed();
    latencies.insert("wasm_setup".to_string(), wasm_setup_duration.as_millis());

    let mut key_set = KeySet {
        read_set: Vec::new(),
        write_set: Vec::new(),
    };

    if near_user {
        tracing::info!("Instance setup; going to guess the key");
        // Guess the key set so we can hand it to the consistency check
        let key_guess_start = Instant::now();

        key_set = match wasm_blob.guess_key(instance, arg_len).await {
            Ok(ks) => ks,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
        };

        let key_guess_duration = key_guess_start.elapsed();
        latencies.insert("key_guess".to_string(), key_guess_duration.as_millis());
        tracing::info!("Key set contains {} read keys and {} write keys", key_set.read_set.len(), key_set.write_set.len());
    }


    let check_store = store.clone();

    let split_start = Instant::now();
    let consistency_handle;
    let check_start = Instant::now();
    if near_user {
        let remote_endpoint = match std::env::var("REMOTE_URL") {
            Ok(endpoint) => endpoint,
            Err(_) => panic!("REMOTE_ENDPOINT not set"),
        };
        let check_body = ConsistencyCheckBody::create(&check_store, exec_id, key_set, args, remote_endpoint).await;
        let spawn_start = Instant::now();
        tracing::info!("Sending consistency check request");
        consistency_handle = tokio::spawn(async move {
            // let second_check = ConsistencyCheckBody {
            //     read_keys: check_body.read_keys.clone(),
            //     write_keys: check_body.write_keys.clone(),
            //     id: check_body.id.clone(),
            //     args: check_body.args.clone(),
            //     function: check_body.function.clone(),
            //     consistency_rate: check_body.consistency_rate,
            // };
            // let second_check_start = Instant::now();
            // _ = check_client.do_check(second_check).await;
            // let second_check_duration = second_check_start.elapsed();
            // tracing::info!("Second check duration: {:?}", second_check_duration);
            let check_start = Instant::now();
            match check_client.do_check(check_body).await {
                Ok(res) => {
                    let check_duration = check_start.elapsed();
                    tracing::info!("Check duration: {:?}", check_duration);
                    Ok((res, check_duration))
                },
                Err(e) => Err(e),
            }
        });
        let spawn_end = Instant::now();
        latencies.insert("spawn_check".to_string(), spawn_end.duration_since(spawn_start).as_millis());
    } else {
        consistency_handle = tokio::spawn(async move {
            Ok((CheckResult {
                check_result: false,
                result: serde_json::Value::Null,
                updates: Vec::new(),
                latencies: HashMap::new(),
            }, Instant::now().duration_since(check_start)))
        });
    }

    // Run the wasm blob, this should be happening in parallel with the check
    wasm_blob.store.data_mut().reset_writes();
    // let wasm_handle = tokio::spawn(wasm_blob.run_blob(instance, arg_len));
    let wasm_blob_start = Instant::now();
    wasm_blob.store.data_mut().reset_writes();
    let wasm_result = match wasm_blob.run_blob(instance, arg_len).await {
        Ok(res) => res,
        Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
    };
    tracing::info!("Result of wasm execution: {}", serde_json::to_string(&wasm_result).unwrap());
    let updates = wasm_blob.store.data().get_all_writes();
    // println!("All writes: {}", serde_json::to_string_pretty(&all_writes).unwrap());
    let wasm_blob_duration = wasm_blob_start.elapsed();

    let response: serde_json::Value;

    // Only do the consistency check if we're near the user
    if near_user {
        let check_wait_start = Instant::now();
        let (check_result, check_duration) = match consistency_handle.await.unwrap() {
            Ok(res) => res,
            Err(_) => return Err(WrapperError::CheckError("Consistency Check error".to_string()).into()),
        };
        latencies.insert("consistency_check".to_string(), check_duration.as_millis());
        let check_wait_duration = check_wait_start.elapsed();
        latencies.insert("check_wait".to_string(), check_wait_duration.as_millis());
        let split_end = split_start.elapsed();
        latencies.insert("split".to_string(), split_end.as_millis());
        latencies.insert("wasm_execution".to_string(), wasm_blob_duration.as_millis());

        if check_result.check_result {
            tracing::info!("Consistency check passed. Collect updates and forward.");
            // Spawn a thread to send the followup in the background
            let followup_start = Instant::now();
            if updates.len() == 0 {
                tracing::info!("No updates to apply");
            } else {
                tracing::info!("Sending over {} updates", updates.len());
                tokio::spawn(async move {
                    let client = reqwest::Client::new();
                    let check_client = ConsistencyClient::new(check_url.clone(), client);
                    match check_client.do_followup(exec_id, updates).await {
                        Ok(_) => tracing::info!("Followup sent successfully"),
                        Err(_) => tracing::info!("Failed to send followup"),
                    }
                });
            }
            let followup_duration = followup_start.elapsed();
            latencies.insert("followup".to_string(), followup_duration.as_millis());
            let e2e_end = e2e_start.elapsed();
            latencies.insert("e2e".to_string(), e2e_end.as_millis());
            response = serde_json::json!({
                "result": wasm_result,
                "latencies": latencies,
                "remote_latencies": check_result.latencies,
                "check_status": true,
            });
        } else {
            tracing::info!("Consistency check failed. Syncing state and returning near data result");
            if check_result.updates.len() == 0 {
                tracing::info!("No updates to apply");
            } else {
                tracing::info!("Should apply {} updates", check_result.updates.len());
                let update_start = Instant::now();
                store.batch_update(&check_result.updates).await;
                let update_duration = update_start.elapsed();
                latencies.insert("update_state".to_string(), update_duration.as_millis());
            }
            let e2e_end = e2e_start.elapsed();
            latencies.insert("e2e".to_string(), e2e_end.as_millis());
            response = serde_json::json!({
                "result": check_result.result,
                "latencies": latencies,
                "remote_latencies": check_result.latencies,
                "check_status": false,
            });
        }
    } else {
        let collect_updates_start = Instant::now();
        let updates = store.get_all_writes();
        let collect_updates_duration = collect_updates_start.elapsed();
        latencies.insert("wasm_execution".to_string(), wasm_blob_duration.as_millis());
        latencies.insert("collect_updates".to_string(), collect_updates_duration.as_millis());
        tracing::info!("Made {} updates", updates.len());
        let e2e_end = e2e_start.elapsed();
        latencies.insert("e2e".to_string(), e2e_end.as_millis());
        response = serde_json::json!({
            "result": wasm_result,
            "updates": updates,
            "latencies": latencies,
        });
    }

    tracing::info!("Done with function, returning back to user");


    let resp = Response::builder()
    .status(200)
    .header("Content-Type", "application/json")
    .body(response.to_string().into())
    .map_err(Box::new)?;
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    let deployment_env = match std::env::var("DEPLOYMENT") {
        Ok(env) => env,
        Err(_) => panic!("DEPLOYMENT not set"),
    };

    let use_scylla = match std::env::var("USE_SCYLLA") {
        Ok(_) => true,
        Err(_) => false
    };

    if use_scylla {
        tracing::info!("Using scylla as the dynamo backend through the alternator")
    }

    let near_user = match deployment_env.as_str() {
        "edge" => true,
        "datacenter" => false,
        _ => panic!("unknown deployment env")
    };
    let client = reqwest::Client::new();

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
                // table_partition: None,
            })
        },
        false => {
            let region = RegionProviderChain::default_provider().or_else("eu-central-1");
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .load()
                .await;

            StorageProvider::Dynamo(DynamoStore {
                client: aws_sdk_dynamodb::Client::new(&config),
                all_writes: Vec::new(),
                // table_partition: None,
            })
        },
    };

    run(service_fn(|event: Request| async {
        entry_point(event, &mut store.clone(), near_user, client.clone()).await
    })).await
}
