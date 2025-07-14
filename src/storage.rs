use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest};
use aws_sdk_dynamodb::primitives::Blob;
use lambda_http::tracing;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::{hash_map::Entry, HashMap}, sync::Arc};
use tokio::sync::Mutex;
use base64::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct KeySet {
    pub read_set: Vec<(String, Vec<u8>)>,
    pub write_set: Vec<(String, Vec<u8>)>,
}

#[derive(Serialize, Deserialize)]
pub struct StaleKeyInfo {
    pub table: String,
    pub key: Vec<u8>,
    pub is_value_replicated: bool,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateItem {
    #[serde(rename = "table")]
    pub table: String,
    #[serde(rename = "value", deserialize_with = "deserialize_bytes")]
    pub value: Vec<u8>,
    #[serde(rename = "key", deserialize_with = "deserialize_bytes")]
    pub key: Vec<u8>,
    #[serde(rename = "version")]
    pub version: i64,
}

fn deserialize_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(BASE64_STANDARD.decode(s).unwrap())
}

pub trait Storage: Clone + Send {
    fn put(&mut self, table: String, key: Vec<u8>, value: Vec<u8>) -> impl std::future::Future<Output = ()> + Send;
    fn get(&self, table: String, key: &Vec<u8>) -> impl std::future::Future<Output = Option<(i64, Vec<u8>)>> + Send;

    fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> impl std::future::Future<Output = Vec<(String, Vec<u8>, Option<i64>)>> + Send;
    fn batch_update(&mut self, table_key_pairs: &Vec<UpdateItem>) -> impl std::future::Future<Output = ()> + Send;

    fn reset_writes(&mut self);
    fn get_all_writes(&self) -> Vec<Value>;

    // New functions for stale key tracking
    fn is_key_stale(&self, table: &str, key: &Vec<u8>) -> bool;
    fn update_stale_keys(&mut self, stale_keys: Vec<StaleKeyInfo>);
    fn get_stale_keys(&self) -> Vec<StaleKeyInfo>;
    fn clear_stale_keys(&mut self);
}

// fn get_partition_name(table: String, partition: Option<i64>) -> String {
//     if let Some(partition) = partition {
//         return format!("{table}-part-{partition}");
//     }
//     table
// }

#[derive(Clone)]
pub struct DummyStorage {
    pub store: Arc<Mutex<HashMap<(String, Vec<u8>), (i64, Vec<u8>)>>>,
    pub writes: Vec<Value>,
    // pub table_partition: Option<i64>,
}

impl Storage for DummyStorage {
    async fn put(&mut self, table: String, key: Vec<u8>, value: Vec<u8>) {
        // let partition = get_partition_name(table.clone(), self.table_partition);
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
    async fn get(&self, table: String, key: &Vec<u8>) -> Option<(i64, Vec<u8>)> {
        self.store.lock().await.get(&(table.clone(), key.clone())).map(Clone::clone)
    }

    async fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>, Option<i64>)> {
        let mut items = Vec::new();
        for (table, key) in table_key_pairs {
            let mut ver = None;
            if let Some((v, _)) = self.get(table.clone(), &key.clone()).await {
                ver = Some(v);
            }
            items.push((table.clone(), key.clone(), ver));
        }
        items
    }

    async fn batch_update(&mut self, table_key_pairs: &Vec<UpdateItem>) -> () {
        for item in table_key_pairs {
            self.put(item.table.clone(), item.key.clone(), item.value.clone()).await;
        }
    }

    fn reset_writes(&mut self) {
        self.writes.clear();
    }

    fn get_all_writes(&self) -> Vec<Value> {
        self.writes.clone()
    }

    // fn set_partition(&mut self, partition: i64) {
    //     self.table_partition = Some(partition);
    // }
}

#[derive(Clone)]
pub struct DynamoStore {
    pub client: aws_sdk_dynamodb::Client,
    pub all_writes: Vec<Value>,
    pub stale_keys: Vec<StaleKeyInfo>,
    // pub table_partition: Option<i64>,
}

impl Storage for DynamoStore {

    async fn put(&mut self, table: String, key: Vec<u8>, value: Vec<u8>) {
        // let partition = get_partition_name(table.clone(), self.table_partition);
        self.all_writes.push(json!({
            "key": &key,
            "value": &value,
            "table": table.clone(),
        }));

        self.client
            .update_item()
            .table_name(table)
            .key("id", AttributeValue::B(Blob::new(key.clone())))
            .expression_attribute_values(":value", AttributeValue::B(Blob::new(value)))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .update_expression("ADD version :one SET object_value = :value")
            .send()
            .await
            .expect("failed to put_item");
        tracing::info!("Put item {}", String::from_utf8(key.clone()).unwrap());
    }

    async fn get(&self, table: String, key: &Vec<u8>) -> Option<(i64, Vec<u8>)> {
        // tracing::info!("Going to get item from table {} with key {}", table, String::from_utf8(key.clone()).unwrap());
        let result = self.client
            .get_item()
            .table_name(table)
            .consistent_read(true)
            .key("id", AttributeValue::B(Blob::new(key.clone())))
            .send()
            .await
            .expect("failed to get_item");


        result.item().and_then(|item| {
            tracing::info!("Got item {}", String::from_utf8(key.clone()).unwrap());
            let version = item.get("version")?.as_n().ok()?.parse::<i64>().ok()?;
            let value = item.get("object_value")?.as_b().ok()?.clone().into_inner();
            Some((version, value))
        })
    }

    async fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>, Option<i64>)> {
        let mut key_map = HashMap::<String, Vec<HashMap<String, AttributeValue>>>::new();
        for (table, key) in table_key_pairs {
            // let table = make_partition(table.to_string(), self.table_partition);
            let mut new_key_av_map = HashMap::<String, AttributeValue>::new();
            new_key_av_map.insert("id".to_string(), AttributeValue::B(Blob::new(key.to_vec())));

            if key_map.contains_key(table) {
                let curr_keys = key_map.get_mut(&table.clone()).unwrap();
                curr_keys.push(new_key_av_map);
            } else {
                key_map.insert(
                    table.clone(),
                    vec![new_key_av_map]
                );
            }
        }

        let mut batch_input = HashMap::<String, KeysAndAttributes>::new();
        for (table, keys) in key_map {
            batch_input.insert(
                table,
                KeysAndAttributes::builder()
                    .set_keys(Some(keys))
                    .projection_expression("id,version")
                    .consistent_read(true)
                    .build()
                    .unwrap(),
            );
        }

        let result = self.client
            .batch_get_item()
            .set_request_items(Some(batch_input))
            .send()
            .await
            .expect("batch_get_item failed");

        let mut output_vec: Vec<(String, Vec<u8>, Option<i64>)> = Vec::new();
        if let Some(responses) = result.responses() {
            for (table, items) in responses {
                for item in items {
                    let key = item.get("id").unwrap().as_b().unwrap().clone().into_inner();
                    let version = item.get("version")
                        .and_then(|v| { v.as_n().ok() })
                        .and_then(|v| { v.parse::<i64>().ok() });

                    // We don't actually need to get the value here, since we're just using it to check
                    // The version numbers. This should hopefully speed things up!
                    // let value = item.get("object_value")
                    //     .and_then(|v| { v.as_b().ok() })
                    //     .and_then(|v| { Some(v.clone().into_inner()) });

                    // let version_value = if let (Some(version), Some(value)) = (version, value) {
                    //     Some((version, value))
                    // } else {
                    //     None
                    // };

                    output_vec.push((table.to_string(), key.to_vec(), version));
                }
            }
        }
        output_vec
    }

    async fn batch_update(&mut self, table_key_pairs: &Vec<UpdateItem>) -> () {
        // Split the input items into groups of size 20 since that's the max
        // on a batch write. For some reason this isn't handled automatically?

        let batch_size = 20;

        let mut batches = Vec::<Vec<WriteRequest>>::new();
        for _ in 0..table_key_pairs.len().div_ceil(batch_size) {
            batches.push(Vec::new());
        }

        let mut current_batch = 0;
        let mut batch_items = 0;
        for item in table_key_pairs {
            let put_request =  PutRequest::builder()
                    .item("id", AttributeValue::B(Blob::new(item.key.clone())))
                    .item("object_value", AttributeValue::B(Blob::new(item.value.clone())))
                    .item("version", AttributeValue::N(item.version.to_string()))
                    .build()
                    .unwrap();
            tracing::info!("Setup put request for item {:?}", put_request.item());
            let write_request = WriteRequest::builder()
                .put_request(put_request)
                .build();
            batches[current_batch].push(write_request);
            batch_items += 1;
            if batch_items == batch_size {
                current_batch += 1;
                batch_items = 0;
            }
        }

        tracing::info!("Writing in {} batches for {} total items", batches.len(), table_key_pairs.len());
        for batch in batches.iter() {
            tracing::info!("Writing batch with {} items", batch.len());
            self.client.batch_write_item()
                .request_items("radical_testing", batch.to_vec())
                .send()
                .await
                .expect("batch_write_item failed");
        }
    }

    fn reset_writes(&mut self) {
        self.all_writes.clear();
    }

    fn get_all_writes(&self) -> Vec<Value> {
        self.all_writes.clone()
    }

    fn is_key_stale(&self, table: &str, key: &Vec<u8>) -> bool {
        self.stale_keys.iter().any(|sk| sk.table == table && sk.key == *key)
    }

    fn update_stale_keys(&mut self, stale_keys: Vec<StaleKeyInfo>) {
        self.stale_keys = stale_keys;
    }

    fn get_stale_keys(&self) -> Vec<StaleKeyInfo> {
        self.stale_keys.clone()
    }

    fn clear_stale_keys(&mut self) {
        self.stale_keys.clear();
    }
}

#[derive(Clone)]
pub enum StorageProvider {
    Dummy(DummyStorage),
    Dynamo(DynamoStore),
}

impl Storage for StorageProvider {
    async fn put(&mut self, table: String, key: Vec<u8>, value: Vec<u8>) {
        match self {
            StorageProvider::Dummy(store) => store.put(table, key, value).await,
            StorageProvider::Dynamo(store) => store.put(table, key, value).await,
        }
    }

    async fn get(&self, table: String, key: &Vec<u8>) -> Option<(i64, Vec<u8>)> {
        match self {
            StorageProvider::Dummy(store) => store.get(table, key).await,
            StorageProvider::Dynamo(store) => store.get(table, key).await,
        }
    }

    async fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>, Option<i64>)> {
        match self {
            StorageProvider::Dummy(store) => store.batch_get(table_key_pairs).await,
            StorageProvider::Dynamo(store) => store.batch_get(table_key_pairs).await,
        }
    }

    async fn batch_update(&mut self, table_key_pairs: &Vec<UpdateItem>) -> () {
        match self {
            StorageProvider::Dummy(store) => store.batch_update(table_key_pairs).await,
            StorageProvider::Dynamo(store) => store.batch_update(table_key_pairs).await,
        }
    }

    fn reset_writes(&mut self) {
        match self {
            StorageProvider::Dummy(store) => store.reset_writes(),
            StorageProvider::Dynamo(store) => store.reset_writes(),
        }
    }

    fn get_all_writes(&self) -> Vec<Value> {
        match self {
            StorageProvider::Dummy(store) => store.get_all_writes(),
            StorageProvider::Dynamo(store) => store.get_all_writes(),
        }
    }

    // fn set_partition(&mut self, partition: i64) {
    //     match self {
    //         StorageType::Dummy(store) => store.set_partition(partition),
    //         StorageType::Dynamo(store) => store.set_partition(partition),
    //     }
    // }
}

pub struct StorageWrapper<'a, D: Storage> {
    pub inner: &'a mut Store<MyState<D>>,
    pub read_keys: &'a mut Vec<(String, Vec<u8>)>,
    pub encountered_stale: &'a mut bool,
}

impl<'a, D: Storage> StorageWrapper<'a, D> {
    async fn get(&mut self, table: String, key: &Vec<u8>) -> Option<(i64, Vec<u8>)> {
        // Track the read
        self.read_keys.push((table.clone(), key.clone()));
        
        // Check if key is stale
        if self.inner.data().is_key_stale(&table, key) {
            *self.encountered_stale = true;
            return None;
        }
        
        self.inner.data().get(table, key).await
    }

    async fn put(&mut self, table: String, key: Vec<u8>, value: Vec<u8>) {
        self.inner.data_mut().put(table, key, value).await
    }
}
