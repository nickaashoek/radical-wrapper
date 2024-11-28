use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest};
use aws_sdk_dynamodb::primitives::Blob;
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

    fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> impl std::future::Future<Output = Vec<(String, Vec<u8>, Option<(i64, Vec<u8>)>)>> + Send;
    fn batch_update(&mut self, table_key_pairs: &Vec<UpdateItem>) -> impl std::future::Future<Output = ()> + Send;

    fn reset_writes(&mut self);
    fn get_all_writes(&self) -> Vec<Value>;

    // fn set_partition(&mut self, partition: i64);
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

    async fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>, Option<(i64, Vec<u8>)>)> {
        let mut items = Vec::new();
        for (table, key) in table_key_pairs {
            let item = self.get(table.clone(), &key.clone()).await;
            items.push((table.clone(), key.clone(), item));
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
            .key("id", AttributeValue::B(Blob::new(key)))
            .expression_attribute_values(":value", AttributeValue::B(Blob::new(value)))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .update_expression("ADD version :one SET object_value = :value")
            .send()
            .await
            .expect("failed to put_item");
    }

    async fn get(&self, table: String, key: &Vec<u8>) -> Option<(i64, Vec<u8>)> {
        let result = self.client
            .get_item()
            .table_name(table)
            .key("id", AttributeValue::B(Blob::new(key.clone())))
            .send()
            .await
            .expect("failed to get_item");

        result.item().and_then(|item| {
            let version = item.get("version")?.as_n().ok()?.parse::<i64>().ok()?;
            let value = item.get("object_value")?.as_b().ok()?.clone().into_inner();
            Some((version, value))
        })
    }

    async fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>, Option<(i64, Vec<u8>)>)> {
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
                KeysAndAttributes::builder().set_keys(Some(keys)).build().unwrap(),
            );
        }

        let result = self.client
            .batch_get_item()
            .set_request_items(Some(batch_input))
            .send()
            .await
            .expect("batch_get_item failed");

        let mut output_vec = Vec::new();
        if let Some(responses) = result.responses() {
            for (table, items) in responses {
                for item in items {
                    let key = item.get("id").unwrap().as_b().unwrap().clone().into_inner();
                    let version = item.get("version")
                        .and_then(|v| { v.as_n().ok() })
                        .and_then(|v| { v.parse::<i64>().ok() });

                    let value = item.get("object_value")
                        .and_then(|v| { v.as_b().ok() })
                        .and_then(|v| { Some(v.clone().into_inner()) });

                    let version_value = if let (Some(version), Some(value)) = (version, value) {
                        Some((version, value))
                    } else {
                        None
                    };

                    output_vec.push((table.to_string(), key.to_vec(), version_value));
                }
            }
        }
        output_vec
    }

    async fn batch_update(&mut self, table_key_pairs: &Vec<UpdateItem>) -> () {
        let mut input_items = Vec::<WriteRequest>::new();

        for item in     table_key_pairs {
            println!("Setting up put request for item {} {:?}", String::from_utf8(item.key.clone()).unwrap(), item.key.clone());
            let put_request =  PutRequest::builder()
                    .item("id", AttributeValue::B(Blob::new(item.key.clone())))
                    .item("object_value", AttributeValue::B(Blob::new(item.value.clone())))
                    .item("version", AttributeValue::N(item.version.to_string()))
                    .build()
                    .unwrap();
            println!("Setup put request for item {:?}", put_request.item());
            let write_request = WriteRequest::builder()
                .put_request(put_request)
                .build();
            input_items.push(write_request);
        }

        self.client.batch_write_item()
            .request_items("radical_testing", input_items)
            .send()
            .await
            .expect("batch_write_item failed");
    }

    fn reset_writes(&mut self) {
        self.all_writes.clear();
    }

    fn get_all_writes(&self) -> Vec<Value> {
        self.all_writes.clone()
    }

    // fn set_partition(&mut self, partition: i64) {
    //     self.table_partition = Some(partition);
    // }
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

    async fn batch_get(&self, table_key_pairs: &Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>, Option<(i64, Vec<u8>)>)> {
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