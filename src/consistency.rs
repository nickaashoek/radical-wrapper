use std::collections::HashSet;
// use tokio::time::{sleep, Duration};

// Module for interacting with the consistency check server
use serde::{Deserialize, Serialize};
use reqwest::{Client, StatusCode};
use serde_json::Value;

use super::storage::{Storage, KeySet};

#[derive(Serialize, Deserialize)]
pub struct CheckResult {
    pub result: bool,
    pub latency: i64
}

pub struct ConsistencyClient {
    client: Client,
    url: String
}

#[derive(Serialize, Deserialize)]
pub struct KeyInfo {
    pub table: String,
    pub key: Vec<u8>,
    pub version: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ConsistencyCheckBody {
    pub read_keys: Vec<KeyInfo>,
    pub write_keys: Vec<KeyInfo>,
    pub id: u64,
    pub args: Value,
    pub function: String,
}

impl ConsistencyCheckBody {
    pub async fn create<D: Storage>(external_store: &D, key_set: KeySet, args: Value, remote_endpoint: String) -> Self {
        // Sanity check to make sure we're interleaving with the execution of the function
        // for i in 0..10 {
        //     println!("Check body creation: {}", i);
        //     sleep(Duration::from_millis(100)).await;
        // }

        let read_key_set: HashSet<(String, Vec<u8>)> = HashSet::from_iter(key_set.read_set);
        let write_key_set: HashSet<(String, Vec<u8>)> = HashSet::from_iter(key_set.write_set);

        let mut read_keys = Vec::new();
        let mut write_keys = Vec::new();

        let mut track_reads= HashSet::new();
        let mut track_writes = HashSet::new();  

        let mut table_key_pairs = Vec::new();
        for (table, key) in read_key_set {
            table_key_pairs.push((table, key.clone()));
            track_reads.insert(key);
        }

        for (table, key) in write_key_set {
            table_key_pairs.push((table, key.clone()));
            track_writes.insert(key);
        }

        let items = external_store.batch_get(&table_key_pairs).await;
        for (table, key, version_value) in items {
            let mut version = -1;
            if let Some((v, _)) = version_value {
                version = v;
            }
            let key_info = KeyInfo {
                table: table,
                key: key.clone(),
                version: version,
            };
            if track_reads.contains(&key) {
                read_keys.push(key_info);
            } else if track_writes.contains(&key) {
                write_keys.push(key_info);
            } else {
                panic!("Key not found in read or write set");
            }
        }

        return ConsistencyCheckBody {
            read_keys: read_keys,
            write_keys: write_keys,
            id: 0,
            args: args,
            function: remote_endpoint.clone()
        }
    }
}

impl ConsistencyClient {
    pub fn new(url: String) -> Self {
        Self {
            client: Client::new(),
            url: url.clone()
        }
    }

    fn ping_endpoint(&self) -> String {
        format!("{}/", self.url)
    }

    fn check_endpoint(&self) -> String {
        format!("{}/check", self.url)
    }

    // fn followup_endpoint(&self) -> String {
    //     format!("{}/update", self.url)
    // }

    pub async fn do_ping(&self) -> Result<(), String> {
        let response = self.client.get(self.ping_endpoint()).send().await.unwrap();
        match response.status() {
            StatusCode::OK => Ok(()),
            _ => Err("Failed to ping".to_string())
        }
    }

    pub async fn do_check(&self, check_body: ConsistencyCheckBody) -> Result<CheckResult, ()> {
        let res = self.client.post(self.check_endpoint())
            .json(&check_body)
            .send()
            .await
            .unwrap();

        let resp_json: Value = res.json().await.unwrap();

        println!("Reponse json: {:?}", resp_json);   
        Ok(CheckResult {
            result: true,
            latency: 0
        })
    }

    // pub async fn do_followup(&self, writes: Vec<Value>) -> Result<(), ()> {
    //     let res = self.client.post(self.followup_endpoint())
    //         .json(&serde_json::json!({
    //             "writes": writes
    //         }))
    //         .send()
    //         .await
    //         .unwrap();

    //     let resp_json: Value = res.json().await.unwrap();

    //     println!("Reponse json: {:?}", resp_json);   
    //     Ok(())
    // }
}