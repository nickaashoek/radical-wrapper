use std::io::Read;

use wasmtime::*;
use serde_json::Value;
use serde::{Serialize, Deserialize};
use std::time::{Duration, Instant};
// use tokio::time::{sleep, Duration};

use super::storage::{Storage, KeySet};

#[derive(Debug)]
pub struct MyState<D: Storage> {
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

#[derive(Serialize, Deserialize)]
pub struct WasmResult {
    pub result: Value,
}

pub struct WasmBlob<D: Storage> {
    pub store: Store<MyState<D>>,
    pub module: wasmtime::Module,
    pub linker: Linker<MyState<D>>,
}


impl<D: Storage> WasmBlob<D> {
    pub fn setup_blob(config: Config, path: &str, external_store: D) -> (Self, Duration, Duration) {
        let engine = Engine::new(&config).unwrap();
        let read_start = Instant::now();
        let mut wasm_file = std::fs::File::open(path).unwrap();
        let read_duration = read_start.elapsed();
        let mut buf = Vec::new();
        wasm_file.read_to_end(&mut buf).unwrap();
        let setup_start = Instant::now();
        // This has to be unsafe. Could allow for ACE if we aren't careful, but the blobs should always come from the user
        // Worth thinking about this a little for future work on security side
        let module = unsafe {
            wasmtime::Module::deserialize(&engine, &buf).unwrap()
        };
        let setup_duration = setup_start.elapsed();

        let linker = Linker::new(&engine);
        let state = MyState {
            external_store: external_store.clone(),
        };
        let store = Store::new(&engine, state);
        (Self {
            store,
            module,
            linker,
        }, read_duration, setup_duration)
    }

    pub fn link_blob(&mut self) -> wasmtime::Result<()> {
        self.linker.func_wrap_async("env", "get_key", | mut caller: Caller<'_, MyState<D>>, input: (u32, u32, u32, u32, u32) | {
            Box::new(async move {
                // Sanity check to make sure we're getting some interleaving
                // for i in 0..10 {
                //     println!("Read function wrapper: {}", i);
                //     sleep(Duration::from_millis(100)).await;
                // }
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

        self.linker.func_wrap_async("env", "put_key", | mut caller: Caller<'_, MyState<D>>, input: (u32, u32, u32, u32, u32, u32) | {
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
                println!("writing {:?} {:?} to table {}", String::from_utf8(key.clone()), String::from_utf8(value.clone()), table);

                state.external_store.put(table, key, value).await;
            })
        })?;
        Ok(())
    }

    pub async fn setup_instance(&mut self, args: Value) -> wasmtime::Result<wasmtime::Instance> {
        let args_vec = serde_json::to_vec(&args).unwrap();
        let instance = self.linker.instantiate_async(&mut self.store, &self.module).await?;

        let memory = instance.get_memory(&mut self.store, "memory").unwrap();
        memory.write(&mut self.store, 8, &args_vec)?;
        Ok(instance)
    }

    pub async fn run_blob(&mut self, instance: wasmtime::Instance) -> wasmtime::Result<WasmResult> {
        // Reset the writes before running the blob
        self.store.data_mut().reset_writes();

        // Get memory and entry point. Need memory to get the result out
        let memory = instance.get_memory(&mut self.store, "memory").unwrap();
        let entry = instance.get_typed_func::<(i32, i32, i32), ()>(&mut self.store, "entry")?;
        // Can now call the function
        entry.call_async(&mut self.store, (0, 8, vec!["user-1"].len() as i32)).await?;

        // Fetch and return the result
        let result_slice = read_result_from_wasm(&memory, &self.store, 0)?;
        println!("Result slice: {:?}", String::from_utf8(result_slice.clone()));
        let result_obj = serde_json::from_slice::<WasmResult>(&result_slice).unwrap();
        Ok(result_obj)
    }

    pub async fn guess_key(&mut self, instance: wasmtime::Instance) -> wasmtime::Result<KeySet> {
        let memory = instance.get_memory(&mut self.store, "memory").unwrap();
        let entry = instance.get_typed_func::<(i32, i32, i32), ()>(&mut self.store, "key_guess")?;
        entry.call_async(&mut self.store, (0, 8, vec!["user-1"].len() as i32)).await?;
        let result_slice = read_result_from_wasm(&memory, &self.store, 0)?;

        let result_obj = serde_json::from_slice::<KeySet>(&result_slice).unwrap();
        Ok(result_obj)
    }
}