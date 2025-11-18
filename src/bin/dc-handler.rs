use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use std::sync::Arc;
use test_rust::http_server::create_dc_server;
use test_rust::storage::{DynamoStore, StorageProvider};
use test_rust::wasm::WasmModuleCache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    tracing::info!("Starting datacenter handler");

    // Read configuration from environment
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid port number");

    let check_url = std::env::var("CHECK_URL")
        .unwrap_or_else(|_| "http://localhost:8000".to_string());

    let use_scylla = std::env::var("USE_SCYLLA")
        .map(|v| v.as_str() == "true")
        .unwrap_or(false);

    let functions_dir = std::env::var("FUNCTIONS_DIR")
        .unwrap_or_else(|_| "src/functions".to_string());

    tracing::info!("Configuration:");
    tracing::info!("  Port: {}", port);
    tracing::info!("  Check URL: {}", check_url);
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
    create_dc_server(module_cache, storage, check_url, port).await?;

    Ok(())
}

