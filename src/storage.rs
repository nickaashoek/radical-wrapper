use serde_json::{json, Value};
use std::{collections::{hash_map::Entry, HashMap}, sync::Arc};
use tokio::sync::Mutex;

pub trait Storage: Clone + Send {
    fn put(&mut self, table: String, key: Vec<u8>, value: Vec<u8>) -> impl std::future::Future<Output = ()> + Send;
    fn get(&mut self, table: String, key: &Vec<u8>) -> impl std::future::Future<Output = Option<(i64, Vec<u8>)>> + Send;

    fn batch_get(&mut self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> impl std::future::Future<Output = Vec<(String, Vec<u8>, Option<(i64, Vec<u8>)>)>> + Send;

    fn reset_writes(&mut self);
    fn get_all_writes(&self) -> Vec<Value>;

    fn set_partition(&mut self, partition: i64);
}

fn get_partition_name(table: String, partition: Option<i64>) -> String {
    if let Some(partition) = partition {
        return format!("{table}-part-{partition}");
    }
    table
}

#[derive(Clone)]
pub struct DummyStorage {
    pub store: Arc<Mutex<HashMap<(String, Vec<u8>), (i64, Vec<u8>)>>>,
    pub writes: Vec<Value>,
    pub table_partition: Option<i64>,
}

impl Storage for DummyStorage {
    async fn put(&mut self, table: String, key: Vec<u8>, value: Vec<u8>) {
        let partition = get_partition_name(table.clone(), self.table_partition);
        let mut map = self.store.lock().await;

        self.writes.push(json!({
            "key": &key,
            "value": serde_json::from_slice::<Value>(&value).unwrap(),
            "table": table.clone(),
        }));

        match map.entry((table, key)) {
            Entry::Occupied(e) => {
                let e = e.into_mut();
                e.0 += 1;
                e.1 = value;
            },
            Entry::Vacant(e) => { e.insert((0, value)); }
        }
    }
    async fn get(&mut self, table: String, key: &Vec<u8>) -> Option<(i64, Vec<u8>)> {
        self.store.lock().await.get(&(table.clone(), key.clone())).map(Clone::clone)
    }

    async fn batch_get(&mut self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>, Option<(i64, Vec<u8>)>)> {
        let mut items = Vec::new();
        for (table, key) in table_key_pairs {
            let item = self.get(table.clone(), &key.clone()).await;
            items.push((table.clone(), key.clone(), item));
        }
        items
    }

    fn reset_writes(&mut self) {
        self.writes.clear();
    }

    fn get_all_writes(&self) -> Vec<Value> {
        self.writes.clone()
    }

    fn set_partition(&mut self, partition: i64) {
        self.table_partition = Some(partition);
    }
}