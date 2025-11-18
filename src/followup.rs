use tokio::sync::mpsc::UnboundedReceiver;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Serialize)]
pub struct FollowupContent {
    pub updates: Vec<Value>,
    pub id: Uuid,
}

async fn send_followups(
    client: &reqwest::Client,
    endpoint: &str,
    followup_content: FollowupContent
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let follow_up = serde_json::json!(followup_content);
    tracing::info!("Sending followup for {} with {} updates", followup_content.id, followup_content.updates.len());
    client.post(format!("{}/update", endpoint))
        .json(&follow_up)
        .send()
        .await?;
    Ok(())
}

/// Background task that continuously processes followup updates
/// Replaces the Lambda extension for HTTP server deployments
pub async fn run_followup_task(
    client: reqwest::Client,
    mut receiver: UnboundedReceiver<FollowupContent>,
    endpoint: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("[followup] Background task started");
    
    while let Some(followup_content) = receiver.recv().await {
        tracing::info!("[followup] Received followup content for {}", followup_content.id);
        
        if followup_content.updates.is_empty() {
            tracing::info!("[followup] No updates included, no need to do anything");
        } else {
            tracing::info!("[followup] Going to send {} updates", followup_content.updates.len());
            if let Err(e) = send_followups(&client, &endpoint, followup_content).await {
                tracing::error!("[followup] Failed to send followups: {}", e);
                // Continue processing even if one fails
            } else {
                tracing::info!("[followup] Successfully sent followups");
            }
        }
    }
    
    tracing::info!("[followup] Channel closed, task shutting down");
    Ok(())
}

// Legacy Lambda extension code (kept for backward compatibility)
#[cfg(feature = "lambda")]
pub mod lambda_extension {
    use super::*;
    use lambda_extension::{NextEvent, LambdaEvent, Error, tracing};
    use tokio::sync::Mutex;
    use anyhow::anyhow;

    pub struct FollowupExtension {
        update_receiver: Mutex<UnboundedReceiver<FollowupContent>>,
        client: reqwest::Client,
        endpoint: String,
    }

    impl FollowupExtension {
        pub fn new(
            client: reqwest::Client,
            receiver: UnboundedReceiver<FollowupContent>
        ) -> Self {
            let check_url = match std::env::var("CHECK_URL") {
                Ok(url) => url,
                Err(_) => panic!("CHECK_URL not set"),
            };

            Self {
                update_receiver: Mutex::new(receiver),
                client,
                endpoint: check_url,
            }
        }

        pub async fn send_followups(&self, followup_content: FollowupContent) -> Result<(), Error> {
            let follow_up = serde_json::json!(followup_content);
            tracing::info!("Sending followup for {} with {} updates", followup_content.id, followup_content.updates.len());
            self.client.post(format!("{}/update", self.endpoint))
                .json(&follow_up)
                .send()
                .await?;
            Ok(())
        }

        pub async fn invoke(&self, event: LambdaEvent) -> Result<(), Error> {
            tracing::info!("[followup] received an event");
            match event.next {
                NextEvent::Shutdown(shutdown) => {
                    return Err(anyhow!("followup received unexpected shutdown evvent: {:?}",  shutdown).into())
                },
                NextEvent::Invoke(_e) => {}
            }

            tracing::info!("[followup] waiting for updates to arrive");
            let followup_content = self.update_receiver
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| anyhow!("channel is closed"))?;

            if followup_content.updates.len() == 0 {
                tracing::info!("[followup] no updates included, no need to do anything");
            } else {
                tracing::info!("[followup] going to send followup");
                self.send_followups(followup_content).await?;
            }
            tracing::info!("[followup] done handling updates");
            Ok(())
        }
    }
}
