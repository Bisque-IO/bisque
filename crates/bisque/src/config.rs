//! Server configuration for the bisque unified server.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Configuration for the unified bisque server.
#[derive(Debug, Clone)]
pub struct BisqueConfig {
    /// Address for the unified HTTP server (S3, OTLP HTTP, management API).
    pub http_addr: SocketAddr,
    /// Address for the Flight SQL gRPC server.
    pub flight_addr: SocketAddr,
    /// Address for the OTLP gRPC server.
    pub otlp_grpc_addr: SocketAddr,
    /// Base directory for Raft logs and Lance data.
    pub data_dir: PathBuf,
    /// HMAC signing secret for token verification (32+ bytes).
    pub token_secret: Vec<u8>,
    /// Token lifetime in seconds (default: 3600).
    pub token_ttl_secs: u64,
    /// Raft node identity.
    pub node_id: u64,
    /// Optional address for the PostgreSQL wire protocol server.
    pub postgres_addr: Option<SocketAddr>,
    /// Optional directory containing built UI static files (e.g. `ui/dist`).
    pub ui_dir: Option<PathBuf>,
    /// Optional S3 URI for per-node OTel deep storage (e.g. `s3://bucket/otel/node-1/`).
    pub otel_s3_uri: Option<String>,
    /// S3 credentials/options for OTel deep storage.
    pub otel_s3_storage_options: HashMap<String, String>,
    /// Peer nodes for federated sys catalog queries: `(node_id, http_addr)`.
    pub peers: Vec<(u64, SocketAddr)>,
}

impl BisqueConfig {
    pub fn new(data_dir: impl Into<PathBuf>, token_secret: Vec<u8>) -> Self {
        Self {
            http_addr: "0.0.0.0:3200".parse().unwrap(),
            flight_addr: "0.0.0.0:50051".parse().unwrap(),
            otlp_grpc_addr: "0.0.0.0:4317".parse().unwrap(),
            data_dir: data_dir.into(),
            token_secret,
            token_ttl_secs: 3600,
            node_id: 1,
            postgres_addr: None,
            ui_dir: None,
            otel_s3_uri: None,
            otel_s3_storage_options: HashMap::new(),
            peers: Vec::new(),
        }
    }

    pub fn with_http_addr(mut self, addr: SocketAddr) -> Self {
        self.http_addr = addr;
        self
    }

    pub fn with_flight_addr(mut self, addr: SocketAddr) -> Self {
        self.flight_addr = addr;
        self
    }

    pub fn with_otlp_grpc_addr(mut self, addr: SocketAddr) -> Self {
        self.otlp_grpc_addr = addr;
        self
    }

    pub fn with_node_id(mut self, id: u64) -> Self {
        self.node_id = id;
        self
    }

    pub fn with_token_ttl_secs(mut self, secs: u64) -> Self {
        self.token_ttl_secs = secs;
        self
    }

    pub fn with_postgres_addr(mut self, addr: Option<SocketAddr>) -> Self {
        self.postgres_addr = addr;
        self
    }

    pub fn with_ui_dir(mut self, dir: Option<PathBuf>) -> Self {
        self.ui_dir = dir;
        self
    }

    pub fn with_otel_s3_uri(mut self, uri: Option<String>) -> Self {
        self.otel_s3_uri = uri;
        self
    }

    pub fn with_otel_s3_storage_options(mut self, opts: HashMap<String, String>) -> Self {
        self.otel_s3_storage_options = opts;
        self
    }

    pub fn with_peers(mut self, peers: Vec<(u64, SocketAddr)>) -> Self {
        self.peers = peers;
        self
    }
}
