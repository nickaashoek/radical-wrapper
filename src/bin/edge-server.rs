use test_rust::edge_server::run_edge_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_edge_server().await
}

