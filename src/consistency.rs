use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use lambda_http::tracing;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use super::storage::{Storage, KeySet, UpdateItem};

#[derive(Serialize, Deserialize)]
pub struct CheckResult {
    #[serde(rename = "dcResult")]
    pub result: Value,
    #[serde(rename = "checkResult")]
    pub check_result: bool,
    #[serde(rename = "latencies")]
    pub latencies: HashMap<String, i64>,
    #[serde(rename = "updates")]
    pub updates: Vec<UpdateItem>,
}

#[derive(Clone)]
pub struct ConsistencyClient {
    client: Client,
    url: String
}

#[derive(Serialize, Deserialize, Clone)]
pub struct KeyInfo {
    pub table: String,
    pub key: Vec<u8>,
    pub version: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ConsistencyCheckBody {
    pub read_keys: Vec<KeyInfo>,
    pub write_keys: Vec<KeyInfo>,
    pub id: String,
    pub args: Value,
    pub function: String,
    #[serde(rename = "ccRate")]
    pub consistency_rate: f64,
}

impl ConsistencyCheckBody {
    pub async fn create<D: Storage>(
        external_store: &D,
        execution_id: Uuid,
        key_set: KeySet,
        args: Value,
        remote_endpoint: String) -> Self {
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

        let batch_start = std::time::Instant::now();
        let items = external_store.batch_get(&table_key_pairs).await;
        let batch_duration = batch_start.elapsed();
        tracing::info!("Batch get duration: {:?}", batch_duration);
        for (table, key, version_value) in items {
            let mut version = -1;
            if let Some((v, _)) = version_value {
                version = v;
            }
            let key_info = KeyInfo {
                table,
                key: key.clone(),
                version,
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
            read_keys,
            write_keys,
            id: execution_id.to_string(),
            args,
            function: remote_endpoint.clone(),
            consistency_rate: -1.0,
        }
    }
}

impl ConsistencyClient {
    pub fn new(url: String, client: Client) -> Self {
        Self {
            client,
            url: url.clone()
        }
    }

    fn check_endpoint(&self) -> String {
        format!("{}/check", self.url)
    }

    fn followup_endpoint(&self) -> String {
        format!("{}/update", self.url)
    }

    pub async fn do_check(&self, check_body: ConsistencyCheckBody) -> Result<CheckResult, ()> {
        let res = self.client.post(self.check_endpoint())
            .json(&check_body)
            .send()
            .await
            .unwrap();

        let resp_json: Value = res.json().await.unwrap();
        println!("Reponse json: {:?}", resp_json);
        let response = serde_json::from_value::<CheckResult>(resp_json).unwrap();
        Ok(response)
    }

    pub async fn do_followup(&self, id: Uuid, writes: Vec<Value>) -> Result<(), ()> {
        let follow_up = serde_json::json!({
            "Updates": writes,
            "Id": id.to_string()
        });
        tracing::info!("Follow up: {:?} to {}", follow_up, self.followup_endpoint());
        self.client.post(self.followup_endpoint())
            .json(&follow_up)
            .send()
            .await
            .unwrap();
        Ok(())
    }
}
