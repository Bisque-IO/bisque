//! Arrow Flight SQL Example
//!
//! Demonstrates bisque-lance's Flight SQL server and client interaction:
//!
//! 1. Start a single-node Raft cluster with a Flight SQL gRPC server
//! 2. Connect with a `FlightSqlServiceClient` (same client ADBC/JDBC uses)
//! 3. Create tables via custom Flight actions
//! 4. Ingest data via `CommandStatementIngest` (bulk writes)
//! 5. Query data via SQL (`execute` + `do_get`)
//! 6. Inspect metadata (catalogs, schemas, tables)
//! 7. Use prepared statements
//!
//! ```text
//!  ┌──────────────────────────┐         gRPC          ┌──────────────────────┐
//!  │   FlightSqlServiceClient │ ◄───────────────────► │  BisqueFlightService │
//!  │   (ADBC / JDBC / custom) │    Flight SQL         │   (Raft + Lance)     │
//!  └──────────────────────────┘                       └──────────────────────┘
//! ```
//!
//! Run with: cargo run --example flight_sql -p bisque-lance

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables, CommandStatementIngest};
use arrow_flight::{Action, FlightInfo};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::TryStreamExt;
use tonic::transport::Channel;

use bisque_lance::flight::serve_flight;
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

/// Fetch FlightInfo and stream all record batches from the result.
async fn fetch_results(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Vec<RecordBatch> {
    let mut all_batches = Vec::new();
    for endpoint in info.endpoint {
        if let Some(ticket) = endpoint.ticket {
            let stream: FlightRecordBatchStream = client.do_get(ticket).await.unwrap();
            let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            all_batches.extend(batches);
        }
    }
    all_batches
}

/// Pretty-print record batches using DataFusion's formatting.
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

    println!("=== bisque-lance: Arrow Flight SQL Example ===\n");

    let temp_dir = tempfile::tempdir()?;

    // =========================================================================
    // Step 1: Set up single-node Raft cluster + Flight SQL server
    // =========================================================================
    println!("--- Step 1: Start Raft Node + Flight SQL Server ---\n");

    let (raft_node, _manager) = setup_single_node_raft(temp_dir.path()).await?;
    raft_node.start();

    // Wait for leadership
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(raft_node.is_leader(), "single node should be leader");
    println!("  Raft node is leader");

    // Start Flight SQL server on a random available port.
    // Bind first to discover the port, then drop the listener before serve_flight rebinds.
    let flight_addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(listener);
        addr
    };
    println!("  Flight SQL server listening on {flight_addr}");

    let raft_for_server = raft_node.clone();
    tokio::spawn(async move {
        serve_flight(raft_for_server, flight_addr).await.unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // =========================================================================
    // Step 2: Connect with FlightSqlServiceClient
    // =========================================================================
    println!("\n--- Step 2: Connect FlightSqlServiceClient ---\n");

    let channel = Channel::from_shared(format!("http://{flight_addr}"))
        .unwrap()
        .connect()
        .await?;
    let mut client = FlightSqlServiceClient::new(channel);

    // Handshake
    client.handshake("anonymous", "").await?;
    println!("  Handshake complete");

    // =========================================================================
    // Step 3: Create tables via custom Flight actions
    // =========================================================================
    println!("\n--- Step 3: Create Tables (Custom Flight Actions) ---\n");

    let ev_schema = events_schema();
    let met_schema = metrics_schema();

    // Create "events" table
    let action = Action::new("create_table", encode_create_table("events", &ev_schema));
    let mut results = client.do_action(action).await?;
    let result = results.message().await?.expect("expected result");
    println!(
        "  Created 'events' table: {}",
        String::from_utf8_lossy(&result.body)
    );

    // Create "metrics" table
    let action = Action::new("create_table", encode_create_table("metrics", &met_schema));
    let mut results = client.do_action(action).await?;
    let result = results.message().await?.expect("expected result");
    println!(
        "  Created 'metrics' table: {}",
        String::from_utf8_lossy(&result.body)
    );

    // =========================================================================
    // Step 4: Ingest data via CommandStatementIngest
    // =========================================================================
    println!("\n--- Step 4: Bulk Ingest Data (Flight SQL CommandStatementIngest) ---\n");

    let events_batch = generate_events(&ev_schema);
    let metrics_batch = generate_metrics(&met_schema);

    // Ingest events
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
        .execute_ingest(cmd, futures::stream::iter(vec![Ok(events_batch.clone())]))
        .await?;
    println!("  Ingested {rows} rows into 'events'");

    // Ingest metrics
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
        .execute_ingest(cmd, futures::stream::iter(vec![Ok(metrics_batch.clone())]))
        .await?;
    println!("  Ingested {rows} rows into 'metrics'");

    // =========================================================================
    // Step 5: SQL queries via Flight SQL
    // =========================================================================
    println!("\n--- Step 5: SQL Queries (Flight SQL) ---\n");

    // Count all events
    println!("  >> SELECT COUNT(*) as total FROM events\n");
    let info = client
        .execute("SELECT COUNT(*) as total FROM events".into(), None)
        .await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // Events by type
    println!(
        "\n  >> SELECT event_type, COUNT(*) as cnt FROM events GROUP BY event_type ORDER BY cnt DESC\n"
    );
    let info = client
        .execute(
            "SELECT event_type, COUNT(*) as cnt FROM events GROUP BY event_type ORDER BY cnt DESC"
                .into(),
            None,
        )
        .await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // Average duration by event type
    println!(
        "\n  >> SELECT event_type, AVG(duration_ms) as avg_duration FROM events GROUP BY event_type\n"
    );
    let info = client
        .execute(
            "SELECT event_type, AVG(duration_ms) as avg_duration FROM events GROUP BY event_type"
                .into(),
            None,
        )
        .await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // Filter query
    println!(
        "\n  >> SELECT user_id, page, duration_ms FROM events WHERE duration_ms > 100 ORDER BY duration_ms DESC\n"
    );
    let info = client
        .execute(
            "SELECT user_id, page, duration_ms FROM events WHERE duration_ms > 100 ORDER BY duration_ms DESC"
                .into(),
            None,
        )
        .await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // Metrics query
    println!(
        "\n  >> SELECT host, AVG(cpu_pct) as avg_cpu, MAX(mem_mb) as max_mem FROM metrics GROUP BY host\n"
    );
    let info = client
        .execute(
            "SELECT host, AVG(cpu_pct) as avg_cpu, MAX(mem_mb) as max_mem FROM metrics GROUP BY host"
                .into(),
            None,
        )
        .await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // =========================================================================
    // Step 6: Metadata queries (catalogs, schemas, tables)
    // =========================================================================
    println!("\n--- Step 6: Metadata Queries (Flight SQL) ---\n");

    // Get catalogs
    println!("  >> GetCatalogs\n");
    let info = client.get_catalogs().await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // Get DB schemas
    println!("\n  >> GetDbSchemas\n");
    let info = client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        })
        .await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // Get tables
    println!("\n  >> GetTables (include_schema=false)\n");
    let info = client
        .get_tables(CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        })
        .await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    // =========================================================================
    // Step 7: Prepared statements
    // =========================================================================
    println!("\n--- Step 7: Prepared Statements (Flight SQL) ---\n");

    println!(
        "  >> Prepare: SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id ORDER BY cnt DESC\n"
    );
    let mut prepared = client
        .prepare(
            "SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id ORDER BY cnt DESC".into(),
            None,
        )
        .await?;

    let info = prepared.execute().await?;
    let batches = fetch_results(&mut client, info).await;
    print_batches(&batches);

    prepared.close().await?;
    println!("\n  Prepared statement closed");

    // =========================================================================
    // Step 8: Drop a table
    // =========================================================================
    println!("\n--- Step 8: Drop Table (Custom Flight Action) ---\n");

    let action = Action::new("drop_table", bytes::Bytes::from("metrics"));
    let mut results = client.do_action(action).await?;
    let result = results.message().await?.expect("expected result");
    println!(
        "  Dropped 'metrics' table: {}",
        String::from_utf8_lossy(&result.body)
    );

    // Verify it's gone
    let info = client
        .get_tables(CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        })
        .await?;
    let batches = fetch_results(&mut client, info).await;
    println!("\n  Remaining tables:");
    print_batches(&batches);

    // =========================================================================
    // Done
    // =========================================================================
    println!("\n--- Shutdown ---\n");
    client.close().await?;
    raft_node.shutdown().await;
    println!("  Shut down gracefully");
    println!("\n=== Example completed successfully! ===\n");

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

    // -- Lance engine --
    let lance_dir = base_dir.join("lance-data");
    let config = BisqueLanceConfig::new(&lance_dir);
    let engine = Arc::new(BisqueLance::open(config).await?);

    // -- Raft storage --
    let raft_dir = base_dir.join("raft-data");
    std::fs::create_dir_all(&raft_dir)?;
    let storage_config = MmapStorageConfig::new(&raft_dir).with_segment_size(8 * 1024 * 1024);
    let storage = Storage::new(storage_config).await.expect("storage");

    // -- Transport (unused for single node, but required by the API) --
    let registry = Arc::new(NodeRegistry::new());
    registry.register(node_id, "127.0.0.1:0".parse().unwrap());
    let transport = Transport::new(BisqueTcpTransportConfig::default(), registry);

    // -- Multi-raft manager --
    let manager: Arc<Manager> = Arc::new(MultiRaftManager::new(transport, storage));

    // -- Raft config --
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

    // -- Create group with LanceStateMachine --
    let state_machine = LanceStateMachine::new(engine.clone());
    let raft = manager
        .add_group(group_id, node_id, raft_config, state_machine)
        .await
        .expect("add_group");

    // -- Initialize single-node membership --
    // When read_vote() returns None (fresh storage), openraft creates a default vote
    // with the node's own ID, which makes is_initialized() return true. The docs say
    // NotAllowed from initialize() is safe to ignore — it means the cluster is ready.
    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    match raft.initialize(members).await {
        Ok(_) => {}
        Err(e) => {
            // Safe to ignore NotAllowed — cluster is already bootstrapped.
            if !raft.is_initialized().await.unwrap_or(false) {
                return Err(e.into());
            }
        }
    }

    // -- Wrap in LanceRaftNode --
    let raft_node = Arc::new(LanceRaftNode::new(raft, engine, node_id));
    Ok((raft_node, manager))
}
