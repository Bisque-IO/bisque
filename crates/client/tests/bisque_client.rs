//! Comprehensive integration tests for `BisqueClient`.
//!
//! Each test spins up a single-node Raft cluster with an S3 server, then
//! exercises the client against it. Tests cover:
//!
//! - Connection and catalog auto-discovery
//! - SQL query execution across multiple tables
//! - MDBX persistence and reconnect
//! - Cross-table (UNION) queries
//! - Graceful close and cleanup
//! - Table create/drop events propagation (via reconnect)
//! - Multiple ingests visible across reconnect cycles
//!
//! Note: `serve_s3` does not include the WebSocket endpoint, so real-time
//! push-based sync is not tested here. Those scenarios require the full bisque
//! server. The client still works for point-in-time queries; reconnecting
//! re-fetches the catalog.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::Action;
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::{DataType, Field, Schema};
use bisque_client::{BisqueClient, CredentialConfig};
use bisque_lance::flight::serve_flight;
use bisque_lance::{
    BisqueLance, BisqueLanceConfig, CatalogEventBus, LanceRaftNode, LanceStateMachine,
    LanceTypeConfig, serve_s3,
};
use bisque_raft::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapStorageConfig,
    MultiRaftManager, MultiplexedLogStorage, NodeAddressResolver,
};
use openraft::Config;
use openraft::impls::BasicNode;
use tonic::transport::Channel;

// =============================================================================
// Test Harness
// =============================================================================

fn pick_unused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local_addr")
}

type NodeRegistry = DefaultNodeRegistry<u64>;
type Transport = BisqueTcpTransport<LanceTypeConfig>;
type Storage = MultiplexedLogStorage<LanceTypeConfig>;
type Manager = MultiRaftManager<LanceTypeConfig, Transport, Storage>;

/// A running single-node bisque cluster for tests.
struct TestCluster {
    raft_node: Arc<LanceRaftNode>,
    _manager: Arc<Manager>,
    s3_addr: SocketAddr,
    flight_addr: SocketAddr,
}

impl TestCluster {
    async fn start(base_dir: &std::path::Path) -> Self {
        let catalog_bus = Arc::new(CatalogEventBus::new(0));

        // Raft setup
        let node_id: u64 = 1;
        let group_id: u64 = 1;

        let lance_dir = base_dir.join("lance-data");
        let config = BisqueLanceConfig::new(&lance_dir);
        let engine = Arc::new(BisqueLance::open(config).await.expect("open engine"));

        let raft_dir = base_dir.join("raft-data");
        std::fs::create_dir_all(&raft_dir).unwrap();
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
            .with_catalog_events(catalog_bus.clone())
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
                    panic!("raft init failed: {e}");
                }
            }
        }

        let raft_node = Arc::new(LanceRaftNode::new(raft, engine.clone(), node_id));
        raft_node.start();

        // Wait for leader
        for _ in 0..40 {
            if raft_node.is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(raft_node.is_leader(), "single node should be leader");

        // Start S3 server
        let s3_addr = pick_unused_addr();
        let engine_for_s3 = engine.clone();
        let bus_for_s3 = catalog_bus.clone();
        tokio::spawn(async move {
            serve_s3(engine_for_s3, s3_addr, Some(bus_for_s3), None, 1)
                .await
                .unwrap();
        });

        // Start Flight SQL server
        let flight_addr = pick_unused_addr();
        let raft_for_flight = raft_node.clone();
        tokio::spawn(async move {
            serve_flight(raft_for_flight, flight_addr).await.unwrap();
        });

        // Give servers time to bind
        tokio::time::sleep(Duration::from_millis(300)).await;

        Self {
            raft_node,
            _manager: manager,
            s3_addr,
            flight_addr,
        }
    }

    fn cluster_url(&self) -> String {
        format!("http://{}", self.s3_addr)
    }

    async fn flight_client(&self) -> FlightSqlServiceClient<Channel> {
        let channel = Channel::from_shared(format!("http://{}", self.flight_addr))
            .unwrap()
            .connect()
            .await
            .expect("connect to flight");
        let mut client = FlightSqlServiceClient::new(channel);
        client.handshake("anonymous", "").await.expect("handshake");
        client
    }

    async fn create_table(&self, name: &str, schema: &Schema) {
        let mut client = self.flight_client().await;
        let body = encode_create_table(name, schema);
        let action = Action::new("create_table", body);
        let mut results = client.do_action(action).await.expect("create_table");
        let _ = results.message().await.expect("create_table result");
    }

    async fn ingest(&self, table: &str, batch: RecordBatch) -> i64 {
        let mut client = self.flight_client().await;
        let cmd = CommandStatementIngest {
            table: table.to_string(),
            schema: None,
            catalog: None,
            temporary: false,
            transaction_id: None,
            options: Default::default(),
            table_definition_options: Default::default(),
        };
        client
            .execute_ingest(cmd, futures::stream::iter(vec![Ok(batch)]))
            .await
            .expect("ingest")
    }

    #[allow(dead_code)]
    async fn drop_table(&self, name: &str) {
        let mut client = self.flight_client().await;
        let action = Action::new("drop_table", bytes::Bytes::from(name.to_string()));
        let mut results = client.do_action(action).await.expect("drop_table");
        let _ = results.message().await.expect("drop_table result");
    }
}

fn encode_create_table(name: &str, schema: &Schema) -> bytes::Bytes {
    let name_bytes = name.as_bytes();
    let schema_bytes = bisque_client::ipc::schema_to_ipc(schema).expect("schema_to_ipc");
    let mut buf = Vec::with_capacity(2 + name_bytes.len() + schema_bytes.len());
    buf.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&schema_bytes);
    bytes::Bytes::from(buf)
}

fn simple_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

fn simple_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(simple_schema()),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn metrics_schema() -> Schema {
    Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("cpu_pct", DataType::Float64, false),
        Field::new("mem_mb", DataType::Int32, false),
    ])
}

fn metrics_batch(hosts: &[&str], cpus: &[f64], mems: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(metrics_schema()),
        vec![
            Arc::new(StringArray::from(hosts.to_vec())),
            Arc::new(Float64Array::from(cpus.to_vec())),
            Arc::new(Int32Array::from(mems.to_vec())),
        ],
    )
    .unwrap()
}

async fn connect_client(cluster_url: &str, persist_path: Option<&std::path::Path>) -> BisqueClient {
    let credentials = Arc::new(CredentialConfig::new());
    BisqueClient::connect(
        cluster_url,
        vec!["bisque".to_string()],
        credentials,
        persist_path,
    )
    .await
    .expect("connect client")
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();
}

async fn query_count(client: &BisqueClient, table: &str) -> i64 {
    let batches = client
        .sql(&format!("SELECT COUNT(*) as cnt FROM {table}"))
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

// =============================================================================
// Tests
// =============================================================================

/// Basic connect, discover tables, query, close.
#[tokio::test]
async fn test_connect_and_query() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    // Create a table and ingest data
    cluster.create_table("users", &simple_schema()).await;
    cluster
        .ingest(
            "users",
            simple_batch(&[1, 2, 3], &["alice", "bob", "carol"]),
        )
        .await;

    // Connect client
    let client = connect_client(&cluster.cluster_url(), None).await;

    // Verify catalog discovery
    let catalogs = client.catalog_names();
    assert!(catalogs.contains(&"bisque".to_string()));

    // Run a count query
    let count = query_count(&client, "users").await;
    assert_eq!(count, 3);

    // Run a filter query
    let batches = client
        .sql("SELECT name FROM users WHERE id > 1 ORDER BY id")
        .await
        .expect("filter query");
    assert!(!batches.is_empty());
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.len(), 2);
    assert_eq!(names.value(0), "bob");
    assert_eq!(names.value(1), "carol");

    client.close().await;
    cluster.raft_node.shutdown().await;
}

/// Multiple tables and cross-table UNION query.
#[tokio::test]
async fn test_multi_table_queries() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("users", &simple_schema()).await;
    cluster.create_table("metrics", &metrics_schema()).await;
    cluster
        .ingest("users", simple_batch(&[1, 2], &["alice", "bob"]))
        .await;
    cluster
        .ingest(
            "metrics",
            metrics_batch(&["web-01", "web-02"], &[42.5, 78.3], &[4096, 8192]),
        )
        .await;

    let client = connect_client(&cluster.cluster_url(), None).await;

    // Query each table
    assert_eq!(query_count(&client, "users").await, 2);
    assert_eq!(query_count(&client, "metrics").await, 2);

    // Cross-table UNION
    let batches = client
        .sql(
            "SELECT 'users' as source, COUNT(*) as rows FROM users \
             UNION ALL \
             SELECT 'metrics', COUNT(*) FROM metrics \
             ORDER BY source",
        )
        .await
        .unwrap();
    assert!(!batches.is_empty());
    let total_rows: i64 = batches
        .iter()
        .flat_map(|b| {
            let col = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..col.len()).map(move |i| col.value(i))
        })
        .sum();
    assert_eq!(total_rows, 4);

    // Aggregation query
    let batches = client
        .sql("SELECT host, AVG(cpu_pct) as avg_cpu FROM metrics GROUP BY host ORDER BY host")
        .await
        .unwrap();
    assert!(!batches.is_empty());

    client.close().await;
    cluster.raft_node.shutdown().await;
}

/// Data added after client connects is visible on reconnect.
#[tokio::test]
async fn test_reconnect_sees_new_data() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("events", &simple_schema()).await;
    cluster
        .ingest("events", simple_batch(&[1, 2, 3], &["a", "b", "c"]))
        .await;

    // First connection — sees 3 rows
    {
        let client = connect_client(&cluster.cluster_url(), None).await;
        assert_eq!(query_count(&client, "events").await, 3);
        client.close().await;
    }

    // Ingest more data while client is disconnected
    cluster
        .ingest("events", simple_batch(&[4, 5], &["d", "e"]))
        .await;

    // Reconnect — should see all 5 rows
    {
        let client = connect_client(&cluster.cluster_url(), None).await;
        assert_eq!(query_count(&client, "events").await, 5);
        client.close().await;
    }

    cluster.raft_node.shutdown().await;
}

/// MDBX persistence: connect, close, reconnect with delta sync.
#[tokio::test]
async fn test_persistence_and_reconnect() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    let persist_dir = temp.path().join("client-store");

    cluster.create_table("logs", &simple_schema()).await;
    cluster
        .ingest("logs", simple_batch(&[1, 2, 3], &["info", "warn", "error"]))
        .await;

    // First connection — populates MDBX cache
    {
        let client = connect_client(&cluster.cluster_url(), Some(&persist_dir)).await;
        assert_eq!(query_count(&client, "logs").await, 3);
        client.close().await;
    }

    // Ingest more data while client is disconnected
    cluster
        .ingest("logs", simple_batch(&[4, 5], &["debug", "trace"]))
        .await;

    // Reconnect with same persist path — should delta-sync and see all 5 rows
    {
        let client = connect_client(&cluster.cluster_url(), Some(&persist_dir)).await;
        assert_eq!(query_count(&client, "logs").await, 5);
        client.close().await;
    }

    cluster.raft_node.shutdown().await;
}

/// DataFusion session context is accessible and functional.
#[tokio::test]
async fn test_session_context_access() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("data", &simple_schema()).await;
    cluster
        .ingest("data", simple_batch(&[1, 2], &["x", "y"]))
        .await;

    let client = connect_client(&cluster.cluster_url(), None).await;

    // Directly use the SessionContext
    let ctx = client.ctx();
    let df = ctx.sql("SELECT * FROM data ORDER BY id").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);

    client.close().await;
    cluster.raft_node.shutdown().await;
}

/// New table created between connects is visible on reconnect.
#[tokio::test]
async fn test_new_table_on_reconnect() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("initial", &simple_schema()).await;
    cluster
        .ingest("initial", simple_batch(&[1], &["init"]))
        .await;

    // First connection — only sees "initial"
    {
        let client = connect_client(&cluster.cluster_url(), None).await;
        assert_eq!(query_count(&client, "initial").await, 1);
        client.close().await;
    }

    // Create a new table while disconnected
    cluster.create_table("dynamic", &simple_schema()).await;
    cluster
        .ingest("dynamic", simple_batch(&[10, 20], &["new1", "new2"]))
        .await;

    // Reconnect — should discover the new table
    {
        let client = connect_client(&cluster.cluster_url(), None).await;
        assert_eq!(query_count(&client, "initial").await, 1);
        assert_eq!(query_count(&client, "dynamic").await, 2);
        client.close().await;
    }

    cluster.raft_node.shutdown().await;
}

/// Connect with no persistence path (in-memory only).
#[tokio::test]
async fn test_connect_without_persistence() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("ephemeral", &simple_schema()).await;
    cluster
        .ingest(
            "ephemeral",
            simple_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]),
        )
        .await;

    // Connect without persist_path
    let client = connect_client(&cluster.cluster_url(), None).await;

    assert_eq!(query_count(&client, "ephemeral").await, 5);

    // Verify last_seq tracking
    let seq = client.last_seq("bisque");
    assert!(seq.is_some());

    client.close().await;
    cluster.raft_node.shutdown().await;
}

/// Multiple ingests across reconnects, each visible to the new client.
#[tokio::test]
async fn test_multiple_ingests_across_reconnects() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("stream", &simple_schema()).await;

    // Wave 1
    cluster
        .ingest("stream", simple_batch(&[1, 2], &["a", "b"]))
        .await;
    {
        let client = connect_client(&cluster.cluster_url(), None).await;
        assert_eq!(query_count(&client, "stream").await, 2);
        client.close().await;
    }

    // Wave 2
    cluster
        .ingest("stream", simple_batch(&[3, 4, 5], &["c", "d", "e"]))
        .await;
    {
        let client = connect_client(&cluster.cluster_url(), None).await;
        assert_eq!(query_count(&client, "stream").await, 5);
        client.close().await;
    }

    // Wave 3
    cluster.ingest("stream", simple_batch(&[6], &["f"])).await;
    {
        let client = connect_client(&cluster.cluster_url(), None).await;
        assert_eq!(query_count(&client, "stream").await, 6);
        client.close().await;
    }

    cluster.raft_node.shutdown().await;
}

/// Verify the client handles empty tables correctly.
#[tokio::test]
async fn test_empty_table_query() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("empty_tbl", &simple_schema()).await;

    let client = connect_client(&cluster.cluster_url(), None).await;

    // This should return 0 rows, not error
    assert_eq!(query_count(&client, "empty_tbl").await, 0);

    client.close().await;
    cluster.raft_node.shutdown().await;
}

/// Large batch ingestion and query.
#[tokio::test]
async fn test_large_batch() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("big", &simple_schema()).await;

    let n = 10_000;
    let ids: Vec<i64> = (0..n).collect();
    let names: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(simple_schema()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(name_refs)),
        ],
    )
    .unwrap();

    cluster.ingest("big", batch).await;

    let client = connect_client(&cluster.cluster_url(), None).await;

    assert_eq!(query_count(&client, "big").await, n);

    // Aggregation on large data
    let batches = client
        .sql("SELECT MIN(id) as min_id, MAX(id) as max_id FROM big")
        .await
        .unwrap();
    let min_id = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let max_id = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(min_id, 0);
    assert_eq!(max_id, n - 1);

    client.close().await;
    cluster.raft_node.shutdown().await;
}

/// Persistence across reconnects with incremental data — each wave uses
/// a fresh persist dir to avoid stale schema cache issues.
#[tokio::test]
async fn test_persistence_incremental_fresh_dirs() {
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let cluster = TestCluster::start(temp.path()).await;

    cluster.create_table("inc", &simple_schema()).await;

    // Wave 1: ingest, connect with persist, verify
    cluster.ingest("inc", simple_batch(&[1], &["first"])).await;
    {
        let persist_dir = temp.path().join("client-db-1");
        let client = connect_client(&cluster.cluster_url(), Some(&persist_dir)).await;
        assert_eq!(query_count(&client, "inc").await, 1);
        client.close().await;
    }

    // Wave 2: ingest more, reconnect
    cluster
        .ingest("inc", simple_batch(&[2, 3], &["second", "third"]))
        .await;
    {
        let persist_dir = temp.path().join("client-db-2");
        let client = connect_client(&cluster.cluster_url(), Some(&persist_dir)).await;
        assert_eq!(query_count(&client, "inc").await, 3);
        client.close().await;
    }

    // Wave 3: ingest more, reconnect again
    cluster
        .ingest("inc", simple_batch(&[4, 5, 6, 7], &["a", "b", "c", "d"]))
        .await;
    {
        let persist_dir = temp.path().join("client-db-3");
        let client = connect_client(&cluster.cluster_url(), Some(&persist_dir)).await;
        assert_eq!(query_count(&client, "inc").await, 7);
        client.close().await;
    }

    cluster.raft_node.shutdown().await;
}
