//! bisque server CLI entry point.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use bisque::BisqueConfig;
use bisque::server;

#[derive(Parser)]
#[command(name = "bisque", about = "Bisque unified server")]
struct Cli {
    /// Address for the unified HTTP server (S3, OTLP HTTP, management API).
    #[arg(long, default_value = "0.0.0.0:3200", env = "BISQUE_HTTP_ADDR")]
    http_addr: SocketAddr,

    /// Address for the Flight SQL gRPC server.
    #[arg(long, default_value = "0.0.0.0:50051", env = "BISQUE_FLIGHT_ADDR")]
    flight_addr: SocketAddr,

    /// Address for the OTLP gRPC server.
    #[arg(long, default_value = "0.0.0.0:4317", env = "BISQUE_OTLP_GRPC_ADDR")]
    otlp_grpc_addr: SocketAddr,

    /// Base directory for Raft logs and data files.
    #[arg(long, default_value = "./data", env = "BISQUE_DATA_DIR")]
    data_dir: PathBuf,

    /// HMAC signing secret for token verification (32+ bytes recommended).
    #[arg(long, env = "BISQUE_TOKEN_SECRET")]
    token_secret: String,

    /// Token lifetime in seconds.
    #[arg(long, default_value = "3600", env = "BISQUE_TOKEN_TTL_SECS")]
    token_ttl_secs: u64,

    /// Raft node identity.
    #[arg(long, default_value = "1", env = "BISQUE_NODE_ID")]
    node_id: u64,

    /// Address for the PostgreSQL wire protocol server (disabled if not set).
    #[arg(long, env = "BISQUE_POSTGRES_ADDR")]
    postgres_addr: Option<SocketAddr>,

    /// Directory containing built UI static files (e.g. `ui/dist`).
    #[arg(long, env = "BISQUE_UI_DIR")]
    ui_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let config = BisqueConfig::new(&cli.data_dir, cli.token_secret.into_bytes())
        .with_http_addr(cli.http_addr)
        .with_flight_addr(cli.flight_addr)
        .with_otlp_grpc_addr(cli.otlp_grpc_addr)
        .with_node_id(cli.node_id)
        .with_token_ttl_secs(cli.token_ttl_secs)
        .with_postgres_addr(cli.postgres_addr)
        .with_ui_dir(cli.ui_dir);

    server::run(config).await
}
