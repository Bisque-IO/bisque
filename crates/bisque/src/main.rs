//! bisque server CLI entry point.

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use bisque::BisqueConfig;
use bisque::server;

#[derive(Parser)]
#[command(name = "bisque", about = "bisque data platform")]
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

    /// Peer nodes for federated sys catalog queries.
    /// Format: `node_id=host:port` (e.g. `2=10.0.0.2:3200,3=10.0.0.3:3200`).
    #[arg(long, value_delimiter = ',', env = "BISQUE_PEERS")]
    peers: Vec<String>,

    /// Address for the MQ consumer TCP protocol server (disabled if not set).
    #[arg(long, default_value = "0.0.0.0:4222", env = "BISQUE_MQ_ADDR")]
    mq_addr: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    //     println!(
    //         r#"
    //                 ()
    //                /
    //           |   /
    //     |     |  /        _     _
    //     |  |  | |        | |__ (_)___  __ _ _   _  ___
    //     |  |  | | |      | '_ \| / __|/ _` | | | |/ _ \
    //    _|__|__|_|_|_     | |_) | \__ \ (_| | |_| |  __/
    //   /  ~~~~~~~~~~  \   |_.__/|_|___/\__, |\__,_|\___|
    //   \              /                   |_|
    //    \____________/
    //       |______|
    // "#
    //     );

    println!(
        r#"
 _     _
| |__ (_)___  __ _ _   _  ___
| '_ \| / __|/ _` | | | |/ _ \
| |_) | \__ \ (_| | |_| |  __/
|_.__/|_|___/\__, |\__,_|\___|
                |_| v0.1.0
"#
    );

    let cli = Cli::parse();

    let peers: Vec<(u64, SocketAddr)> = cli
        .peers
        .iter()
        .filter_map(|s| {
            let (id_str, addr_str) = s.split_once('=')?;
            let id: u64 = id_str.parse().ok()?;
            let addr: SocketAddr = addr_str.parse().ok()?;
            Some((id, addr))
        })
        .collect();

    let config = BisqueConfig::new(&cli.data_dir, cli.token_secret.into_bytes())
        .with_http_addr(cli.http_addr)
        .with_flight_addr(cli.flight_addr)
        .with_otlp_grpc_addr(cli.otlp_grpc_addr)
        .with_node_id(cli.node_id)
        .with_token_ttl_secs(cli.token_ttl_secs)
        .with_postgres_addr(cli.postgres_addr)
        .with_ui_dir(cli.ui_dir)
        .with_peers(peers)
        .with_mq_addr(cli.mq_addr);

    server::run(config).await
}
