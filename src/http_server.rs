use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response, Json},
    routing::post,
    Router,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::handler::{RadicalHandler, HandlerResponse, WrapperError};
use crate::storage::StorageProvider;
use crate::wasm::WasmModuleCache;
use crate::followup::FollowupContent;

#[derive(Debug, Deserialize)]
pub struct RequestBody {
    pub args: serde_json::Value,
    #[serde(default)]
    pub id: Option<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub module_cache: Arc<WasmModuleCache>,
    pub storage: StorageProvider,
    pub http_client: reqwest::Client,
    pub update_sender: mpsc::UnboundedSender<FollowupContent>,
    pub check_url: String,
    pub remote_url: String,
    pub edge_id: Option<String>,
    pub edge_endpoint: Option<String>,
}

/// Edge handler endpoint
async fn edge_handler(
    State(state): State<Arc<AppState>>,
    Path(function_name): Path<String>,
    Json(body): Json<RequestBody>,
) -> Result<Json<HandlerResponse>, AppError> {
    tracing::info!("Edge request for function: {}", function_name);

    let exec_id = body.id.as_ref()
        .and_then(|id| Uuid::parse_str(id).ok());

    let edge_id = state.edge_id.as_ref()
        .ok_or_else(|| AppError::Config("EDGE_ID not set".to_string()))?;
    let edge_endpoint = state.edge_endpoint.as_ref()
        .ok_or_else(|| AppError::Config("EDGE_ENDPOINT not set".to_string()))?;

    let mut handler = RadicalHandler::new(
        state.storage.clone(),
        state.update_sender.clone(),
        state.check_url.clone(),
    );

    let response = handler.edge_handler(
        &state.module_cache,
        &function_name,
        body.args,
        exec_id,
        state.http_client.clone(),
        &state.remote_url,
        edge_id,
        edge_endpoint,
    ).await?;

    Ok(Json(response))
}

/// Datacenter handler endpoint
async fn dc_handler(
    State(state): State<Arc<AppState>>,
    Path(function_name): Path<String>,
    Json(body): Json<RequestBody>,
) -> Result<Json<HandlerResponse>, AppError> {
    tracing::info!("Datacenter request for function: {}", function_name);

    let exec_id = body.id.as_ref()
        .and_then(|id| Uuid::parse_str(id).ok());

    let mut handler = RadicalHandler::new(
        state.storage.clone(),
        state.update_sender.clone(),
        state.check_url.clone(),
    );

    let response = handler.dc_handler(
        &state.module_cache,
        &function_name,
        body.args,
        exec_id,
    ).await?;

    Ok(Json(response))
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// List available functions
async fn list_functions(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let functions = state.module_cache.function_names();
    Json(serde_json::json!({
        "functions": functions,
        "count": functions.len()
    }))
}

pub async fn create_edge_server(
    module_cache: Arc<WasmModuleCache>,
    storage: StorageProvider,
    check_url: String,
    remote_url: String,
    edge_id: String,
    edge_endpoint: String,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = reqwest::Client::new();
    let (update_sender, update_receiver) = mpsc::unbounded_channel::<FollowupContent>();
    
    // Start the followup background task
    let followup_client = reqwest::Client::new();
    let followup_check_url = check_url.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::followup::run_followup_task(followup_client, update_receiver, followup_check_url).await {
            tracing::error!("Followup task error: {}", e);
        }
    });
    
    let state = Arc::new(AppState {
        module_cache,
        storage,
        http_client,
        update_sender,
        check_url,
        remote_url,
        edge_id: Some(edge_id),
        edge_endpoint: Some(edge_endpoint),
    });

    let app = Router::new()
        .route("/:function_name", post(edge_handler))
        .route("/health", axum::routing::get(health_check))
        .route("/functions", axum::routing::get(list_functions))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    tracing::info!("Edge handler server listening on {}", addr);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

pub async fn create_dc_server(
    module_cache: Arc<WasmModuleCache>,
    storage: StorageProvider,
    check_url: String,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = reqwest::Client::new();
    let (update_sender, _update_receiver) = mpsc::unbounded_channel::<FollowupContent>();
    
    let state = Arc::new(AppState {
        module_cache,
        storage,
        http_client,
        update_sender,
        check_url,
        remote_url: String::new(), // Not needed for DC
        edge_id: None,
        edge_endpoint: None,
    });

    let app = Router::new()
        .route("/:function_name", post(dc_handler))
        .route("/health", axum::routing::get(health_check))
        .route("/functions", axum::routing::get(list_functions))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    tracing::info!("Datacenter handler server listening on {}", addr);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

// Error handling
#[derive(Debug)]
pub enum AppError {
    Handler(WrapperError),
    Config(String),
}

impl From<WrapperError> for AppError {
    fn from(err: WrapperError) -> Self {
        AppError::Handler(err)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::Handler(WrapperError::FunctionNotFound(name)) => {
                (StatusCode::NOT_FOUND, format!("Function not found: {}", name))
            }
            AppError::Handler(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Handler error: {}", e))
            }
            AppError::Config(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Configuration error: {}", msg))
            }
        };

        let body = Json(serde_json::json!({
            "error": error_message
        }));

        (status, body).into_response()
    }
}

