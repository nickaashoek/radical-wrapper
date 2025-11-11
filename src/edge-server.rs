use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceBuilder;

use crate::storage::{DynamoStore, Storage, StorageProvider, UpdateItem};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateItem {
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    pub version: i64,
    pub table: String,
}

impl From<ReplicateItem> for UpdateItem {
    fn from(item: ReplicateItem) -> Self {
        UpdateItem {
            key: item.key,
            value: item.value,
            version: item.version,
            table: item.table,
        }
    }
}

#[derive(Clone)]
pub struct EdgeServerState {
    pub storage: StorageProvider,
    pub edge_id: String,
}

/// Handler for POST /replicate endpoint
/// Receives replication updates from the primary datacenter and writes them to local storage
async fn replicate_handler(
    State(state): State<Arc<EdgeServerState>>,
    Json(items): Json<Vec<ReplicateItem>>,
) -> impl IntoResponse {
    let edge_id = &state.edge_id;
    
    tracing::info!("[{}] Received {} replication items", edge_id, items.len());
    
    // Convert ReplicateItem to UpdateItem
    let update_items: Vec<UpdateItem> = items.into_iter().map(|item| item.into()).collect();
    
    // Write to storage
    let mut storage = state.storage.clone();
    storage.batch_update(&update_items).await;
    
    tracing::info!("[{}] Successfully replicated {} items", edge_id, update_items.len());
    
    // Return 200 OK with no body (async replication)
    StatusCode::OK
}

/// Health check endpoint
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

pub async fn run_edge_server() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    // Read configuration from environment variables
    let edge_id = std::env::var("EDGE_ID")
        .unwrap_or_else(|_| "edge-unknown".to_string());
    
    let edge_endpoint = std::env::var("EDGE_ENDPOINT")
        .expect("EDGE_ENDPOINT must be set");
    
    let port = std::env::var("EDGE_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .expect("EDGE_PORT must be a valid port number");
    
    let use_scylla = std::env::var("USE_SCYLLA")
        .map(|v| v.as_str() == "true")
        .unwrap_or(false);
    
    tracing::info!("Starting edge replication server");
    tracing::info!("  Edge ID: {}", edge_id);
    tracing::info!("  Edge Endpoint: {}", edge_endpoint);
    tracing::info!("  Port: {}", port);
    tracing::info!("  Using Scylla: {}", use_scylla);
    
    // Set up storage backend (same as main.rs)
    let storage: StorageProvider = if use_scylla {
        let scylla_ep = std::env::var("SCYLLA_EP")
            .expect("SCYLLA_EP must be set when USE_SCYLLA=true");
        
        tracing::info!("Connecting to Scylla at {}", scylla_ep);
        
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region("None")
            .endpoint_url(scylla_ep)
            .load()
            .await;
        
        StorageProvider::Dynamo(DynamoStore {
            client: aws_sdk_dynamodb::Client::new(&config),
            all_writes: Vec::new(),
        })
    } else {
        tracing::info!("Connecting to DynamoDB");
        
        let region = RegionProviderChain::default_provider().or_else("eu-central-1");
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .load()
            .await;
        
        let client = aws_sdk_dynamodb::Client::new(&config);
        
        StorageProvider::Dynamo(DynamoStore {
            client,
            all_writes: Vec::new(),
        })
    };
    
    // Create shared state
    let state = Arc::new(EdgeServerState {
        storage,
        edge_id: edge_id.clone(),
    });
    
    // Build router
    let app = Router::new()
        .route("/replicate", post(replicate_handler))
        .route("/health", axum::routing::get(health_handler))
        .with_state(state)
        .layer(ServiceBuilder::new());
    
    // Bind and serve
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    tracing::info!("Edge replication server listening on {}", addr);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

