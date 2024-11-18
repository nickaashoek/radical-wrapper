// Module for interacting with the consistency check server
use serde::{Deserialize, Serialize};
use reqwest::{Client, StatusCode};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct CheckResult {
    pub result: bool,
    pub latency: i64
}

pub struct ConsistencyClient {
    client: Client,
    url: String
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

    fn followup_endpoint(&self) -> String {
        format!("{}/update", self.url)
    }

    pub async fn do_ping(&self) -> Result<(), String> {
        let response = self.client.get(self.ping_endpoint()).send().await.unwrap();
        match response.status() {
            StatusCode::OK => Ok(()),
            _ => Err("Failed to ping".to_string())
        }
    }

    pub async fn do_check(&self) -> Result<CheckResult, ()> {
        let res = self.client.post(self.check_endpoint())
            .json(&serde_json::json!({}))
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

    pub async fn do_followup(&self, writes: Vec<Value>) -> Result<(), ()> {
        let res = self.client.post(self.followup_endpoint())
            .json(&serde_json::json!({
                "writes": writes
            }))
            .send()
            .await
            .unwrap();

        let resp_json: Value = res.json().await.unwrap();

        println!("Reponse json: {:?}", resp_json);   
        Ok(())
    }
}