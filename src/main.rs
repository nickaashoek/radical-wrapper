use std::{collections::HashMap, sync::Arc};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use lambda_http::{lambda_runtime::Diagnostic, run, service_fn, tracing, Body, Error, Request, Response};
use storage::Storage;
use tokio::sync::Mutex;
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

async fn entry_point<D: Storage + 'static>(_event: Request, store: &mut D, near_user: bool) -> Result<Response<Body>, Error> {
    
    println!("Entering into the function");
    let check_url = match std::env::var("CHECK_URL") {
        Ok(url) => url,
        Err(_) => panic!("CHECK_URL not set"),
    };

    println!("Getting the body json from the request to the function");
    let body = _event.body().to_vec();
    let body_json= serde_json::from_slice::<serde_json::Value>(&body)?;
    let args = body_json["args"].clone();
    let mut latencies: HashMap<String, u128> = HashMap::new();
    let mut exec_id = Uuid::new_v4();
    if !body_json["id"].is_null() {
        exec_id = Uuid::parse_str(body_json["id"].as_str().unwrap()).unwrap();
    }

    let check_client = ConsistencyClient::new(check_url.clone());
    
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
        Ok(_) => println!("Wasm module linked successfully"),
        Err(e) => return Err(WasmError::LinkerError(e.to_string()).into()),
    }
    let blob_link_duration = blob_link_start.elapsed();
    latencies.insert("blob_link".to_string(), blob_link_duration.as_millis());

    println!("Setting up the instance");
    // Setup an instance of the blob that we can use to run the function + guess
    let instant_setup_start = Instant::now();
    let instance = match wasm_blob.setup_instance(serde_json::json!({
        "target-user": "user-1",
    })).await {
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
        println!("Instance setup; going to guess the key");
        // Guess the key set so we can hand it to the consistency check
        let key_guess_start = Instant::now();
        key_set = match wasm_blob.guess_key(instance).await {
            Ok(ks) => ks,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
        };
        let key_guess_duration = key_guess_start.elapsed();
        latencies.insert("key_guess".to_string(), key_guess_duration.as_millis());
        println!("Key set: {}", serde_json::to_string_pretty(&key_set).unwrap());
    }


    let check_store = store.clone();

    // Run the wasm blob, this should be happening in parallel with the check
    let wasm_handle= tokio::spawn(async move {
        let wasm_blob_start = Instant::now();
        wasm_blob.store.data_mut().reset_writes();
        let wasm_result = match wasm_blob.run_blob(instance).await {
            Ok(res) => res,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string())),
        };
        println!("Result of wasm execution: {}", serde_json::to_string_pretty(&wasm_result).unwrap());
        let all_writes = wasm_blob.store.data().get_all_writes();
        // println!("All writes: {}", serde_json::to_string_pretty(&all_writes).unwrap());
        let wasm_blob_duration = wasm_blob_start.elapsed();
        Ok((wasm_result, all_writes, wasm_blob_duration))
    });

    let response: serde_json::Value;

    // Only do the consistency check if we're near the user
    if near_user {
        let remote_endpoint = match std::env::var("REMOTE_URL") {
            Ok(endpoint) => endpoint,
            Err(_) => panic!("REMOTE_ENDPOINT not set"),
        };
        let check_start = Instant::now();
        let check_body = ConsistencyCheckBody::create(&check_store, exec_id, key_set, args, remote_endpoint).await;
        println!("Sending consistency check request");
        let check_result = match check_client.do_check(check_body).await {
            Ok(res) => res,
            Err(_) => return Err(WrapperError::CheckError("Consistency check failed".to_string()).into()),
        };
        let check_duration = check_start.elapsed();
        latencies.insert("consistency_check".to_string(), check_duration.as_millis());

        if check_result.check_result {
            println!("Consistency check passed. Collect updates and forward.");
            // Grab the writes the function made
            let (result, updates, duration) = wasm_handle.await.unwrap()?;
            latencies.insert("wasm_execution".to_string(), duration.as_millis());
            println!("Updates: {}", serde_json::to_string_pretty(&updates).unwrap());

            // Spawn a thread to send the followup in the background
            let followup_start = Instant::now();
            tokio::spawn(async move {
                match check_client.do_followup(exec_id, updates).await {
                    Ok(_) => println!("Followup sent successfully"),
                    Err(_) => println!("Failed to send followup"),
                }
            });
            let followup_duration = followup_start.elapsed();
            latencies.insert("followup".to_string(), followup_duration.as_millis());
            response = serde_json::json!({
                "result": result,
                "latencies": latencies,
            });
        } else {
            println!("Consistency check failed. Syncing state and returning near data result");
            if check_result.updates.len() == 0 {
                println!("No updates to apply");
            } else {
                println!("Updates to apply: {}", serde_json::to_string_pretty(&check_result.updates).unwrap());
                let update_start = Instant::now();
                store.batch_update(&check_result.updates).await;
                let update_duration = update_start.elapsed();
                latencies.insert("update_state".to_string(), update_duration.as_millis());
            }
            response = serde_json::json!({
                "result": check_result.result,
                "latencies": latencies,
                "remote_latencies": check_result.latencies,
            });
        }
    } else {
        let (result, _, duration) = wasm_handle.await.unwrap()?;
        let collect_updates_start = Instant::now();
        let updates = store.get_all_writes();
        let collect_updates_duration = collect_updates_start.elapsed();
        latencies.insert("wasm_execution".to_string(), duration.as_millis());
        latencies.insert("collect_updates".to_string(), collect_updates_duration.as_millis());
        println!("Updates: {}", serde_json::to_string_pretty(&updates).unwrap());
        response = serde_json::json!({
            "result": result,
            "updates": updates,
            "latencies": latencies,
        });
    }

    println!("Done with function, returning back to user");


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

    let near_user: bool;

    // Set up the store for the wasm function to use
    let dummy_data = ["apple", "banana", "pear"].iter().map(|s| s.to_string()).collect::<Vec<String>>();
    let store: StorageProvider = match deployment_env.as_str() {
        "local" => {
            near_user = true;
            let mut store = StorageProvider::Dummy(DummyStorage {
                store: Arc::new(Mutex::new(HashMap::new())),
                writes: Vec::new(),
                // table_partition: None,
            });

            for (i, data) in dummy_data.iter().enumerate() {
                store.put("radical_testing".into(), format!("user-{}", i).into(), serde_json::json!({
                    "password": data,
                }).to_string().into_bytes()).await;
            } 
            store
        },
        "edge" => {
            near_user = true;
            let region = RegionProviderChain::default_provider().or_else("eu-central-1");
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .load()
                .await;

            let store = StorageProvider::Dynamo(DynamoStore {
                client: aws_sdk_dynamodb::Client::new(&config),
                all_writes: Vec::new(),
                // table_partition: None,
            });

            // for (i, _) in dummy_data.iter().enumerate() {
            //     let (version, _) = store.get("radical_testing".into(), &format!("user-{}", i).into()).await.unwrap();
            //     println!("Version for key user-{}: {}", i, version);
            // }

            store
        },
        "datacenter" => {
            near_user = false;
            let region = RegionProviderChain::default_provider().or_else("eu-central-1");
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .load()
                .await;

            let store = StorageProvider::Dynamo(DynamoStore {
                client: aws_sdk_dynamodb::Client::new(&config),
                all_writes: Vec::new(),
                // table_partition: None,
            });

            store
        }
        _ => panic!("Invalid deployment environment"),
    };

    
    run(service_fn(|event: Request| async {
        entry_point(event, &mut store.clone(), near_user).await
    })).await
}
