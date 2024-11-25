use std::{collections::HashMap, sync::Arc};
use lambda_http::{lambda_runtime::Diagnostic, run, service_fn, tracing, Body, Error, Request, Response};
use storage::Storage;
use tokio::sync::Mutex;
use wasmtime::*;
use thiserror;

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

async fn entry_point<D: Storage + 'static>(_event: Request, store: D) -> Result<Response<Body>, Error> {
    
    let check_url = match std::env::var("CHECK_URL") {
        Ok(url) => url,
        Err(_) => panic!("CHECK_URL not set"),
    };

    let body = _event.body().to_vec();
    let body_json= serde_json::from_slice::<serde_json::Value>(&body)?;
    let args = body_json["args"].clone();

    let check_client = ConsistencyClient::new(check_url.clone());
    
    let mut config = Config::new();
    config.async_support(true);
    let mut wasm_blob = WasmBlob::setup_blob(config, "function.wasm", store.clone());

    // Setup the wasm blob to link the read/write functions
    match wasm_blob.link_blob() {
        Ok(_) => println!("Wasm module linked successfully"),
        Err(e) => return Err(WasmError::LinkerError(e.to_string()).into()),
    }

    // Setup an instance of the blob that we can use to run the function + guess
    let instance = match wasm_blob.setup_instance(serde_json::json!({
        "target-user": "user-1",
    })).await {
        Ok(instance) => instance,
        Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
    };

    // Guess the key set so we can hand it to the consistency check
    let key_set = match wasm_blob.guess_key(instance).await {
        Ok(ks) => ks,
        Err(e) => return Err(WasmError::WasmExecError(e.to_string()).into()),
    };

    println!("Key set: {}", serde_json::to_string_pretty(&key_set).unwrap());
    let check_store = store.clone();

    // Run the wasm blob, this should be happening in parallel with the check
    let wasm_handle= tokio::spawn(async move {
        wasm_blob.store.data_mut().reset_writes();
        let wasm_result = match wasm_blob.run_blob(instance).await {
            Ok(res) => res,
            Err(e) => return Err(WasmError::WasmExecError(e.to_string())),
        };
        println!("Result of wasm execution: {}", serde_json::to_string_pretty(&wasm_result).unwrap());
        let all_writes = wasm_blob.store.data().get_all_writes();
        Ok(all_writes)
    });

    let check_body = ConsistencyCheckBody::create(&check_store, key_set, args, "remote_endpoint".to_string()).await;
    // Fire off another thread to handle the consistency check
    match check_client.do_check(check_body).await {
        Ok(res) => res,
        Err(_) => return Err(WrapperError::CheckError("Consistency check failed".to_string()).into()),
    };
    check_client.do_ping().await.unwrap();

    // Grab the writes the function made
    let updates = wasm_handle.await.unwrap()?;
    println!("Updates: {}", serde_json::to_string_pretty(&updates).unwrap());

    // match check_client.do_followup(all_writes).await {
    //     Ok(_) => println!("Followup sent successfully"),
    //     Err(_) => return Err(WrapperError::FollowupError("Failed to send followup".to_string()).into()),
    // }

    // let check_result = match check_handle.await {
    //     Ok(res) => res,
    //     Err(_) => return Err(WrapperError::CheckError("Consistency check failed".to_string()).into()),
    // };
    // println!("Check result: {}", serde_json::to_string_pretty(&check_result).unwrap());
    
    let resp = Response::builder()
    .status(200)
    .header("Content-Type", "text/html")
    .body("Hello, world!".into())
    .map_err(Box::new)?;
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    
    // Set up the store for the wasm function to use
    let mut store = DummyStorage {
        store: Arc::new(Mutex::new(HashMap::new())),
        writes: Vec::new(),
        // table_partition: None,
    };

    let dummy_data = ["apple", "banana", "pear"].iter().map(|s| s.to_string()).collect::<Vec<String>>();
    for (i, data) in dummy_data.iter().enumerate() {
        store.put("dummy-data".into(), format!("user-{}", i).into(), serde_json::json!({
            "password": data,
        }).to_string().into_bytes()).await;
    } 
    
    run(service_fn(|event: Request| async {
        entry_point(event, store.clone()).await
    })).await
}
