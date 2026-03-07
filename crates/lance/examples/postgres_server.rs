//! Dual Protocol Example: Arrow Flight SQL + PostgreSQL Wire Protocol
//!
//! Demonstrates running both protocol servers simultaneously:
//!
//! 1. Start a single-node Raft cluster
//! 2. Start Arrow Flight SQL server (for writes + reads)
//! 3. Create tables and ingest data via Flight SQL
//! 4. Start PostgreSQL wire protocol server (for reads via psql / JDBC)
//! 5. Keep running until Ctrl+C
//!
//! After the example starts, connect with any PostgreSQL client:
//!
//! ```text
//! psql -h 127.0.0.1 -p 5433
//!
//! \dt
//! SELECT * FROM events;
//! SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
//! ```
//!
//! Run with: cargo run --example postgres_server -p bisque-lance

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_flight::Action;
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use tonic::transport::Channel;

use bisque_lance::flight::serve_flight;
use bisque_lance::postgres::{PostgresServerConfig, serve_postgres};
use bisque_lance::{
    BisqueLance, BisqueLanceConfig, LanceRaftNode, LanceStateMachine, LanceTypeConfig,
};
use bisque_raft::multi::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapStorageConfig,
    MultiRaftManager, MultiplexedLogStorage, NodeAddressResolver,
};
use openraft::Config;
use openraft::impls::BasicNode;

// =============================================================================
// Schema definitions
// =============================================================================

fn events_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("page", DataType::Utf8, true),
        Field::new("duration_ms", DataType::Int32, true),
    ])
}

fn metrics_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, false),
        Field::new("cpu_pct", DataType::Float64, false),
        Field::new("mem_mb", DataType::Int32, false),
    ])
}

// =============================================================================
// Test data generation
// =============================================================================

fn generate_events(schema: &Schema) -> RecordBatch {
    let base_ts = 1_700_000_000_000i64;
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![
                base_ts,
                base_ts + 1_000,
                base_ts + 2_000,
                base_ts + 3_000,
                base_ts + 4_000,
                base_ts + 5_000,
                base_ts + 6_000,
                base_ts + 7_000,
            ])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "alice", "carol", "bob", "alice", "dave", "carol",
            ])),
            Arc::new(StringArray::from(vec![
                "page_view",
                "click",
                "page_view",
                "signup",
                "page_view",
                "click",
                "page_view",
                "purchase",
            ])),
            Arc::new(StringArray::from(vec![
                Some("/home"),
                Some("/products"),
                Some("/about"),
                None,
                Some("/home"),
                Some("/checkout"),
                Some("/products"),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(120),
                Some(45),
                Some(200),
                Some(350),
                Some(90),
                Some(30),
                Some(150),
                Some(500),
            ])),
        ],
    )
    .expect("failed to create events batch")
}

fn generate_metrics(schema: &Schema) -> RecordBatch {
    let base_ts = 1_700_000_000_000i64;
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![
                base_ts,
                base_ts,
                base_ts + 60_000,
                base_ts + 60_000,
            ])),
            Arc::new(StringArray::from(vec![
                "web-01", "web-02", "web-01", "web-02",
            ])),
            Arc::new(Float64Array::from(vec![42.5, 78.3, 55.1, 82.7])),
            Arc::new(Int32Array::from(vec![4096, 8192, 4200, 8100])),
        ],
    )
    .expect("failed to create metrics batch")
}

// =============================================================================
// Helpers
// =============================================================================

fn encode_create_table(name: &str, schema: &Schema) -> bytes::Bytes {
    let name_bytes = name.as_bytes();
    let schema_bytes = bisque_lance::ipc::schema_to_ipc(schema).expect("schema_to_ipc");
    let mut buf = Vec::with_capacity(2 + name_bytes.len() + schema_bytes.len());
    buf.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&schema_bytes);
    bytes::Bytes::from(buf)
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== bisque-lance: Dual Protocol (Flight SQL + PostgreSQL) ===\n");

    let temp_dir = tempfile::tempdir()?;

    // =========================================================================
    // Step 1: Set up single-node Raft cluster
    // =========================================================================
    println!("--- Step 1: Start Raft Node ---\n");

    let (raft_node, _manager) = setup_single_node_raft(temp_dir.path()).await?;
    raft_node.start();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(raft_node.is_leader(), "single node should be leader");
    println!("  Raft node is leader");

    // =========================================================================
    // Step 2: Start Flight SQL server
    // =========================================================================
    println!("\n--- Step 2: Start Flight SQL Server ---\n");

    let flight_addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(listener);
        addr
    };
    println!("  Flight SQL server listening on {flight_addr}");

    let raft_for_flight = raft_node.clone();
    tokio::spawn(async move {
        serve_flight(raft_for_flight, flight_addr).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // =========================================================================
    // Step 3: Ingest data via Flight SQL client
    // =========================================================================
    println!("\n--- Step 3: Create Tables + Ingest Data via Flight SQL ---\n");

    let channel = Channel::from_shared(format!("http://{flight_addr}"))
        .unwrap()
        .connect()
        .await?;
    let mut client = FlightSqlServiceClient::new(channel);
    client.handshake("anonymous", "").await?;

    let ev_schema = events_schema();
    let met_schema = metrics_schema();

    // Create tables
    let action = Action::new("create_table", encode_create_table("events", &ev_schema));
    let mut results = client.do_action(action).await?;
    let result = results.message().await?.expect("expected result");
    println!(
        "  Created 'events': {}",
        String::from_utf8_lossy(&result.body)
    );

    let action = Action::new("create_table", encode_create_table("metrics", &met_schema));
    let mut results = client.do_action(action).await?;
    let result = results.message().await?.expect("expected result");
    println!(
        "  Created 'metrics': {}",
        String::from_utf8_lossy(&result.body)
    );

    // Ingest data
    let events_batch = generate_events(&ev_schema);
    let cmd = CommandStatementIngest {
        table: "events".to_string(),
        schema: None,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: Default::default(),
        table_definition_options: Default::default(),
    };
    let rows = client
        .execute_ingest(cmd, futures::stream::iter(vec![Ok(events_batch)]))
        .await?;
    println!("  Ingested {rows} rows into 'events'");

    let metrics_batch = generate_metrics(&met_schema);
    let cmd = CommandStatementIngest {
        table: "metrics".to_string(),
        schema: None,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: Default::default(),
        table_definition_options: Default::default(),
    };
    let rows = client
        .execute_ingest(cmd, futures::stream::iter(vec![Ok(metrics_batch)]))
        .await?;
    println!("  Ingested {rows} rows into 'metrics'");

    // =========================================================================
    // Step 4: Start PostgreSQL wire protocol server
    // =========================================================================
    println!("\n--- Step 4: Start PostgreSQL Wire Protocol Server ---\n");

    let pg_port = 5433u16;
    let pg_config = PostgresServerConfig {
        host: "127.0.0.1".to_string(),
        port: pg_port,
    };

    println!("  PostgreSQL server starting on 127.0.0.1:{pg_port}");
    println!();
    println!("  Connect with:");
    println!("    psql -h 127.0.0.1 -p {pg_port}");
    println!();
    println!("  Try these queries:");
    println!("    \\dt");
    println!("    SELECT * FROM events;");
    println!("    SELECT event_type, COUNT(*) FROM events GROUP BY event_type;");
    println!("    SELECT host, AVG(cpu_pct) as avg_cpu FROM metrics GROUP BY host;");
    println!();
    println!("  Press Ctrl+C to stop.\n");

    // This blocks until the server shuts down
    serve_postgres(raft_node, pg_config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}

// =============================================================================
// Raft bootstrap (single-node cluster)
// =============================================================================

type NodeRegistry = DefaultNodeRegistry<u64>;
type Transport = BisqueTcpTransport<LanceTypeConfig>;
type Storage = MultiplexedLogStorage<LanceTypeConfig>;
type Manager = MultiRaftManager<LanceTypeConfig, Transport, Storage>;

async fn setup_single_node_raft(base_dir: &std::path::Path) -> anyhow::Result<(Arc<LanceRaftNode>, Arc<Manager>)> {
    let node_id: u64 = 1;
    let group_id: u64 = 1;

    let lance_dir = base_dir.join("lance-data");
    let config = BisqueLanceConfig::new(&lance_dir);
    let engine = Arc::new(BisqueLance::open(config).await?);

    let raft_dir = base_dir.join("raft-data");
    std::fs::create_dir_all(&raft_dir)?;
    let storage_config = MmapStorageConfig::new(&raft_dir).with_segment_size(8 * 1024 * 1024);
    let storage = Storage::new(storage_config).await.expect("storage");

    let registry = Arc::new(NodeRegistry::new());
    registry.register(node_id, "127.0.0.1:0".parse().unwrap());
    let transport = Transport::new(BisqueTcpTransportConfig::default(), registry);

    let manager: Arc<Manager> = Arc::new(MultiRaftManager::new(transport, storage));

    let raft_config = Arc::new(
        Config {
            heartbeat_interval: 200,
            election_timeout_min: 400,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .expect("invalid raft config"),
    );

    let state_machine = LanceStateMachine::new(engine.clone());
    let raft = manager
        .add_group(group_id, node_id, raft_config, state_machine)
        .await
        .expect("add_group");

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    match raft.initialize(members).await {
        Ok(_) => {}
        Err(e) => {
            if !raft.is_initialized().await.unwrap_or(false) {
                return Err(e.into());
            }
        }
    }

    let raft_node = Arc::new(LanceRaftNode::new(raft, engine, node_id));
    Ok((raft_node, manager))
}
