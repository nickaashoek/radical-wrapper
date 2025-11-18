use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use std::sync::Arc;
use test_rust::http_server::create_edge_server;
use test_rust::storage::{DynamoStore, StorageProvider};
use test_rust::wasm::WasmModuleCache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    tracing::info!("Starting edge handler");

    // Read configuration from environment
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid port number");

    let check_url = std::env::var("CHECK_URL")
        .expect("CHECK_URL must be set");

    let remote_url = std::env::var("REMOTE_URL")
        .expect("REMOTE_URL must be set");

    let edge_id = std::env::var("EDGE_ID")
        .expect("EDGE_ID must be set");

    let edge_endpoint = std::env::var("EDGE_ENDPOINT")
        .expect("EDGE_ENDPOINT must be set");

    let use_scylla = std::env::var("USE_SCYLLA")
        .map(|v| v.as_str() == "true")
        .unwrap_or(false);

    let functions_dir = std::env::var("FUNCTIONS_DIR")
        .unwrap_or_else(|_| "src/functions".to_string());

    tracing::info!("Configuration:");
    tracing::info!("  Port: {}", port);
    tracing::info!("  Check URL: {}", check_url);
    tracing::info!("  Remote URL: {}", remote_url);
    tracing::info!("  Edge ID: {}", edge_id);
    tracing::info!("  Edge Endpoint: {}", edge_endpoint);
    tracing::info!("  Use Scylla: {}", use_scylla);
    tracing::info!("  Functions Directory: {}", functions_dir);

    // Load and precompile all WASM modules
    tracing::info!("Loading WASM modules...");
    let module_cache = Arc::new(WasmModuleCache::load_all_modules(&functions_dir)?);
    tracing::info!("Loaded {} functions", module_cache.function_names().len());

    // Set up storage backend
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

    tracing::info!("Storage initialized");

    // Start the server
    create_edge_server(
        module_cache,
        storage,
        check_url,
        remote_url,
        edge_id,
        edge_endpoint,
        port,
    )
    .await?;

    Ok(())
}

