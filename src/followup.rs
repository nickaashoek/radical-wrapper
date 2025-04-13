use lambda_extension::{NextEvent, LambdaEvent, Error, tracing};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use serde_json::Value;
use anyhow::anyhow;

pub struct FollowupExtension {
    update_receiver: Mutex<UnboundedReceiver<Vec<Value>>>
}

impl FollowupExtension {
    pub fn new(receiver: UnboundedReceiver<Vec<Value>>) -> Self {
        Self {
            update_receiver: Mutex::new(receiver),
        }
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
        let updates = self.update_receiver
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| anyhow!("channel is closed"))?;

        if updates.len() == 0 {
            tracing::info!("[followup] no updates included, no need to do anything");
        } else {
            tracing::info!("[followup] going to send followup");
        }
        tracing::info!("[followup] done handling updates");
        Ok(())
    }
}
