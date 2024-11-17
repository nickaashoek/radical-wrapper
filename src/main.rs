use std::{collections::{hash_map::Entry, HashMap}, sync::Arc};
use lambda_http::{run, service_fn, tracing, Error, Body, Request, Response};
use storage::Storage;
use tokio::time::{sleep, Duration};
use tokio::sync::Mutex;
use serde_json::Value;
use wasmtime::*;

mod storage;
use storage::*;

#[derive(Debug)]
struct MyState<D: Storage> {
    external_store: D,
}

impl <D : Storage> MyState<D> {
    pub fn reset_writes(&mut self) {
        self.external_store.reset_writes();
    }
    pub fn get_all_writes(&self) -> Vec<Value> {
        self.external_store.get_all_writes()
    }
}

fn read_result_from_wasm<D: Storage>(memory: &Memory, store: &Store<MyState<D>>, offset: usize) -> Result<Vec<u8>> {
    let mut result_base_bytes = [0; 4];
    let mut result_len_bytes = [0; 4];
    memory.read(&store, offset, &mut result_base_bytes)?;
    memory.read(&store, offset + 4, &mut result_len_bytes)?;

    let result_base = i32::from_le_bytes(result_base_bytes) as usize;
    let result_len = i32::from_le_bytes(result_len_bytes) as usize;
    let result_slice = &memory.data(&store)[result_base..][..result_len];

    Ok(result_slice.into())
}

async fn entry_point<D: Storage + 'static>(_event: Request, store: D) -> Result<Response<Body>, Error> {
    
    let check_handle = tokio::spawn(check_branch());
    let wasm_handle = tokio::spawn(wasm_branch(store.clone()));
    
    check_handle.await.unwrap();
    wasm_handle.await.unwrap()?; 
    
    let resp = Response::builder()
    .status(200)
    .header("Content-Type", "text/html")
    .body("Hello, world!".into())
    .map_err(Box::new)?;
    Ok(resp)
}

async fn check_branch() {
    println!("Execute the consistency check from here");
}

async fn wasm_branch<D: Storage + 'static>(mut external_store: D) -> wasmtime::Result<()> {
    println!("Load, setup and run the wasm module from here");
    let mut config = Config::new();
    config.async_support(true);
    let engine = Engine::new(&config).unwrap();
    let module = wasmtime::Module::from_file(&engine, "function.wasm")?;
    let mut linker = Linker::new(&engine);
    
    let state = MyState {
        external_store: external_store.clone(),
    };
    let mut store  = Store::new(&engine, state);

    linker.func_wrap_async("env", "get_key", | mut caller: Caller<'_, MyState<D>>, input: (u32, u32, u32, u32, u32) | {
        Box::new(async move {
            let (result_base, table_base, table_len, key_base, key_len) = input;
            println!("Calling into the get wrapper");
            let memory = caller.get_export("memory").and_then(|m| m.into_memory()).unwrap();
            let mut table = Vec::new();
            table.resize(table_len as usize, 0);
            memory.read(caller.as_context_mut(), table_base as usize, table.as_mut_slice()).unwrap();
            let table = String::from_utf8_lossy(&table).to_string();

            let mut key = Vec::new();
            key.resize(key_len as usize, 0);
            memory.read(caller.as_context_mut(), key_base as usize, key.as_mut_slice()).unwrap();

            let state = caller.data_mut();

            let result = state.external_store.get(table, &key).await.map(|e| e.1).unwrap_or(Vec::new());
            let result_offset = memory.data_size(caller.as_context()) - result.len();
            memory.write(caller.as_context_mut(), result_offset, result.as_slice()).unwrap();
            memory.write(caller.as_context_mut(), result_base as usize, &((result_offset as u32).to_le_bytes())).unwrap();
            memory.write(caller.as_context_mut(), result_base as usize + 4, &((result.len() as u32).to_le_bytes())).unwrap();

            println!("reading {:?} {:?}", String::from_utf8(key.clone()), String::from_utf8(result));
        })
    })?;

    linker.func_wrap_async("env", "put_key", | mut caller: Caller<'_, MyState<D>>, input: (u32, u32, u32, u32, u32, u32) | {
        Box::new(async move {
            println!("Calling into the put wrapper");
            let (table_base, table_len, key_base, key_len, value_base, value_len) = input;

            let memory = caller.get_export("memory").and_then(|m| m.into_memory()).unwrap();
            let mut table = Vec::new();
            table.resize(table_len as usize, 0);
            memory.read(caller.as_context_mut(), table_base as usize, table.as_mut_slice()).unwrap();
            let table = String::from_utf8_lossy(&table).to_string();

            let mut key = Vec::new();
            key.resize(key_len as usize, 0);
            memory.read(caller.as_context_mut(), key_base as usize, key.as_mut_slice()).unwrap();

            let mut value = Vec::new();
            value.resize(value_len as usize, 0);
            memory.read(caller.as_context_mut(), value_base as usize, value.as_mut_slice()).unwrap();

            let state = caller.data_mut();
            println!("writing {:?} {:?}", String::from_utf8(key.clone()), String::from_utf8(value.clone()));

            state.external_store.put(table, key, value).await;
        })
    })?;

    let args = serde_json::json!({
        "target-user": "user-1",
    });
    let args_vec = serde_json::to_vec(&args).unwrap();
    let instance = linker.instantiate_async(&mut store, &module).await?;
    let memory = instance.get_memory(&mut store, "memory").unwrap();
    memory.write(&mut store, 8, &args_vec)?;

    let entry = instance.get_typed_func::<(i32, i32, i32), ()>(&mut store, "entry")?;
    entry.call_async(&mut store, (0, 8, vec!["user-1"].len() as i32)).await?;
    let result_slice = read_result_from_wasm(&memory, &store, 0)?;

    println!("Wasm module executed successfully");
    let result_obj = serde_json::from_slice::<Value>(&result_slice).unwrap();
    println!("Result: {}", serde_json::to_string_pretty(&result_obj).unwrap());
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    
    // Set up the store for the wasm function to use
    let mut store = DummyStorage {
        store: Arc::new(Mutex::new(HashMap::new())),
        writes: Vec::new(),
        table_partition: None,
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
