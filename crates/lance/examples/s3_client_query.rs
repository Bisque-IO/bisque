//! S3 Client-Side Query Example
//!
//! Demonstrates bisque-lance's `BisqueClient` for fully client-side query execution
//! with automatic catalog sync via WebSocket:
//!
//! 1. Start a single-node Raft cluster with catalog event bus
//! 2. Start the S3-compatible HTTP server (with WebSocket endpoint)
//! 3. Ingest initial data via Flight SQL
//! 4. Connect `BisqueClient` — auto-discovers tables, opens datasets
//! 5. Run SQL queries locally — client reads data through HTTP/S3
//! 6. Ingest more data — client auto-updates via WebSocket push
//! 7. Re-query to see updated results without manual refresh
//!
//! ```text
//!  ┌─────────────────────────────┐   HTTP/S3 + WebSocket   ┌───────────────────────┐
//!  │  BisqueClient               │ ◄──────────────────────►│  bisque-lance cluster │
//!  │  - auto-syncing datasets    │   GET/HEAD/LIST + WS    │  S3 API + WS push     │
//!  │  - SessionContext + SQL     │                         │  catalog event bus    │
//!  └─────────────────────────────┘                         └───────────────────────┘
//! ```
//!
//! Run with: cargo run --example s3_client_query -p bisque-lance

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use bisque_lance::flight::serve_flight;
use bisque_lance::{
    BisqueClient, BisqueLance, BisqueLanceConfig, CatalogEventBus, LanceRaftNode,
    LanceStateMachine, LanceTypeConfig, serve_s3,
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

fn generate_events(schema: &Schema, base_ts: i64, count: usize) -> RecordBatch {
    let users = ["alice", "bob", "carol", "dave"];
    let events = ["page_view", "click", "signup", "purchase"];
    let pages = [
        Some("/home"),
        Some("/products"),
        Some("/about"),
        None,
        Some("/checkout"),
    ];

    let timestamps: Vec<i64> = (0..count).map(|i| base_ts + (i as i64) * 1_000).collect();
    let user_ids: Vec<&str> = (0..count).map(|i| users[i % users.len()]).collect();
    let event_types: Vec<&str> = (0..count).map(|i| events[i % events.len()]).collect();
    let page_list: Vec<Option<&str>> = (0..count).map(|i| pages[i % pages.len()]).collect();
    let durations: Vec<Option<i32>> = (0..count)
        .map(|i| Some(30 + (i as i32 * 47) % 500))
        .collect();

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(StringArray::from(user_ids)),
            Arc::new(StringArray::from(event_types)),
            Arc::new(StringArray::from(page_list)),
            Arc::new(Int32Array::from(durations)),
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

/// Encode a create_table action body: u16 BE name_len + name + IPC schema
fn encode_create_table(name: &str, schema: &Schema) -> bytes::Bytes {
    let name_bytes = name.as_bytes();
    let schema_bytes = bisque_lance::ipc::schema_to_ipc(schema).expect("schema_to_ipc");
    let mut buf = Vec::with_capacity(2 + name_bytes.len() + schema_bytes.len());
    buf.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&schema_bytes);
    bytes::Bytes::from(buf)
}

fn print_batches(batches: &[RecordBatch]) {
    if batches.is_empty() {
        println!("    (no results)");
        return;
    }
    let formatted = arrow_cast::pretty::pretty_format_batches(batches).expect("formatting error");
    for line in formatted.to_string().lines() {
        println!("    {line}");
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    println!("=== bisque-lance: BisqueClient Auto-Sync Example ===\n");

    let temp_dir = tempfile::tempdir()?;

    // =========================================================================
    // Step 1: Set up single-node Raft cluster with catalog event bus
    // =========================================================================
    println!("--- Step 1: Start Raft Node (with catalog events) ---\n");

    let catalog_bus = Arc::new(CatalogEventBus::new(0));
    let raft_node = setup_single_node_raft(temp_dir.path(), catalog_bus.clone()).await?;
    raft_node.start();

    for _ in 0..20 {
        if raft_node.is_leader() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(raft_node.is_leader(), "single node should be leader");
    println!("  Raft node is leader (catalog event bus active)");

    // =========================================================================
    // Step 2: Start S3 + WebSocket server
    // =========================================================================
    println!("\n--- Step 2: Start S3 + WebSocket Server ---\n");

    let s3_addr: std::net::SocketAddr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(listener);
        addr
    };

    let engine = raft_node.engine().clone();
    let bus_for_s3 = catalog_bus.clone();
    let s3_addr_clone = s3_addr;
    tokio::spawn(async move {
        serve_s3(engine, s3_addr_clone, Some(bus_for_s3), None, 1)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("  S3 + WebSocket server listening on {s3_addr}");

    // =========================================================================
    // Step 3: Start Flight SQL server and ingest initial data
    // =========================================================================
    println!("\n--- Step 3: Create Tables & Ingest Initial Data ---\n");

    let flight_addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(listener);
        addr
    };

    let raft_for_flight = raft_node.clone();
    tokio::spawn(async move {
        serve_flight(raft_for_flight, flight_addr).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    use arrow_flight::Action;
    use arrow_flight::sql::CommandStatementIngest;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use tonic::transport::Channel;

    let channel = Channel::from_shared(format!("http://{flight_addr}"))
        .unwrap()
        .connect()
        .await?;
    let mut flight_client = FlightSqlServiceClient::new(channel);
    flight_client.handshake("anonymous", "").await?;

    let ev_schema = events_schema();
    let met_schema = metrics_schema();

    // Create tables
    let action = Action::new("create_table", encode_create_table("events", &ev_schema));
    let mut results = flight_client.do_action(action).await?;
    let _ = results.message().await?;
    println!("  Created 'events' table");

    let action = Action::new("create_table", encode_create_table("metrics", &met_schema));
    let mut results = flight_client.do_action(action).await?;
    let _ = results.message().await?;
    println!("  Created 'metrics' table");

    // Ingest initial data (8 events, 4 metrics)
    let events_batch = generate_events(&ev_schema, 1_700_000_000_000, 8);
    let cmd = CommandStatementIngest {
        table: "events".to_string(),
        schema: None,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: Default::default(),
        table_definition_options: Default::default(),
    };
    let rows = flight_client
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
    let rows = flight_client
        .execute_ingest(cmd, futures::stream::iter(vec![Ok(metrics_batch)]))
        .await?;
    println!("  Ingested {rows} rows into 'metrics'");

    // =========================================================================
    // Step 4: Connect BisqueClient — auto-discovers tables
    // =========================================================================
    println!("\n--- Step 4: Connect BisqueClient ---\n");

    let credentials = Arc::new(bisque_lance::CredentialConfig::new());
    let persist_dir = temp_dir.path().join("bisque-client");
    let bisque_client = BisqueClient::connect(
        format!("http://{}", s3_addr),
        vec!["bisque".to_string()],
        credentials,
        Some(&persist_dir),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{}", e))?;

    println!("  BisqueClient connected (tables auto-discovered)");

    // =========================================================================
    // Step 5: Run SQL queries via BisqueClient
    // =========================================================================
    println!("\n--- Step 5: Query via BisqueClient ---\n");

    // Query 1: Count events
    println!("  >> SELECT COUNT(*) as total FROM events\n");
    let batches = bisque_client
        .sql("SELECT COUNT(*) as total FROM events")
        .await?;
    print_batches(&batches);

    // Query 2: Events by type
    println!(
        "\n  >> SELECT event_type, COUNT(*) as cnt FROM events GROUP BY event_type ORDER BY cnt DESC\n"
    );
    let batches = bisque_client
        .sql("SELECT event_type, COUNT(*) as cnt FROM events GROUP BY event_type ORDER BY cnt DESC")
        .await?;
    print_batches(&batches);

    // Query 3: Metrics aggregation
    println!(
        "\n  >> SELECT host, AVG(cpu_pct) as avg_cpu, MAX(mem_mb) as max_mem FROM metrics GROUP BY host\n"
    );
    let batches = bisque_client
        .sql("SELECT host, AVG(cpu_pct) as avg_cpu, MAX(mem_mb) as max_mem FROM metrics GROUP BY host")
        .await?;
    print_batches(&batches);

    // Query 4: Cross-table summary
    println!(
        "\n  >> SELECT 'events' as source, COUNT(*) as rows FROM events UNION ALL SELECT 'metrics', COUNT(*) FROM metrics\n"
    );
    let batches = bisque_client
        .sql("SELECT 'events' as source, COUNT(*) as rows FROM events UNION ALL SELECT 'metrics', COUNT(*) FROM metrics")
        .await?;
    print_batches(&batches);

    // =========================================================================
    // Done
    // =========================================================================
    println!("\n--- Shutdown ---\n");
    flight_client.close().await?;
    bisque_client.close().await;
    raft_node.shutdown().await;
    println!("  All queries executed on the client via BisqueClient");
    println!("  Shut down gracefully");
    println!("\n=== Example completed successfully! ===\n");

    Ok(())
}

// =============================================================================
// Raft bootstrap (single-node cluster with catalog events)
// =============================================================================

type NodeRegistry = DefaultNodeRegistry<u64>;
type Transport = BisqueTcpTransport<LanceTypeConfig>;
type Storage = MultiplexedLogStorage<LanceTypeConfig>;
type Manager = MultiRaftManager<LanceTypeConfig, Transport, Storage>;

async fn setup_single_node_raft(
    base_dir: &std::path::Path,
    catalog_bus: Arc<CatalogEventBus>,
) -> anyhow::Result<Arc<LanceRaftNode>> {
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

    let state_machine = LanceStateMachine::new(engine.clone())
        .with_catalog_events(catalog_bus)
        .with_catalog_name("bisque".to_string());
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
    Ok(raft_node)
}
