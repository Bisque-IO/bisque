//! Integration tests for the write batcher.
//!
//! Each test bootstraps a single-node Raft cluster, creates a table,
//! and exercises the write batcher through `LanceRaftNode::write_records()`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use openraft::Config;
use openraft::impls::BasicNode;

use bisque_lance::{
    BisqueLance, BisqueLanceConfig, LanceRaftNode, LanceResponse, LanceStateMachine,
    LanceTypeConfig, WriteBatcherConfig,
};
use bisque_raft::multi::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapStorageConfig,
    MultiRaftManager, MultiplexedLogStorage, NodeAddressResolver,
};

// =============================================================================
// Test helpers
// =============================================================================

type NodeRegistry = DefaultNodeRegistry<u64>;
type Transport = BisqueTcpTransport<LanceTypeConfig>;
type Storage = MultiplexedLogStorage<LanceTypeConfig>;
type Manager = MultiRaftManager<LanceTypeConfig, Transport, Storage>;

/// Bootstrap a single-node Raft cluster with write batching enabled.
async fn setup_with_batcher(
    base_dir: &std::path::Path,
    batcher_config: WriteBatcherConfig,
) -> (Arc<LanceRaftNode>, Arc<Manager>) {
    let node_id: u64 = 1;
    let group_id: u64 = 1;

    let lance_dir = base_dir.join("lance-data");
    let config = BisqueLanceConfig::new(&lance_dir);
    let engine = Arc::new(BisqueLance::open(config).await.unwrap());

    let raft_dir = base_dir.join("raft-data");
    std::fs::create_dir_all(&raft_dir).unwrap();
    let storage_config = MmapStorageConfig::new(&raft_dir)
        .with_segment_size(8 * 1024 * 1024)
        .with_fsync_delay(Duration::ZERO);
    let storage = Storage::new(storage_config).await.unwrap();

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
        .unwrap(),
    );

    let state_machine = LanceStateMachine::new(engine.clone());
    let raft = manager
        .add_group(group_id, node_id, raft_config, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    let _ = raft.initialize(members).await;

    let raft_node =
        Arc::new(LanceRaftNode::new(raft, engine, node_id).with_write_batcher(batcher_config));
    raft_node.start();

    // Wait for leadership.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(raft_node.is_leader(), "single node should be leader");

    (raft_node, manager)
}

/// Bootstrap a single-node Raft cluster without write batching.
async fn setup_without_batcher(base_dir: &std::path::Path) -> (Arc<LanceRaftNode>, Arc<Manager>) {
    let node_id: u64 = 1;
    let group_id: u64 = 1;

    let lance_dir = base_dir.join("lance-data");
    let config = BisqueLanceConfig::new(&lance_dir);
    let engine = Arc::new(BisqueLance::open(config).await.unwrap());

    let raft_dir = base_dir.join("raft-data");
    std::fs::create_dir_all(&raft_dir).unwrap();
    let storage_config = MmapStorageConfig::new(&raft_dir)
        .with_segment_size(8 * 1024 * 1024)
        .with_fsync_delay(Duration::ZERO);
    let storage = Storage::new(storage_config).await.unwrap();

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
        .unwrap(),
    );

    let state_machine = LanceStateMachine::new(engine.clone());
    let raft = manager
        .add_group(group_id, node_id, raft_config, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    let _ = raft.initialize(members).await;

    let raft_node = Arc::new(LanceRaftNode::new(raft, engine, node_id));
    raft_node.start();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(raft_node.is_leader(), "single node should be leader");

    (raft_node, manager)
}

fn test_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

fn make_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(test_schema());
    let ids: Vec<i32> = (0..n as i32).collect();
    let names: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

async fn count_rows(node: &LanceRaftNode, table_name: &str) -> usize {
    let table = node.engine().get_table(table_name).unwrap();
    let ds = table.active_dataset_snapshot().await.unwrap();
    ds.count_rows(None).await.unwrap()
}

// =============================================================================
// Tests
// =============================================================================

/// Basic: a single write through the batcher succeeds and data lands.
#[tokio::test(flavor = "multi_thread")]
async fn single_write_through_batcher() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(50)),
    )
    .await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();

    let batch = make_batch(10);
    let result = node.write_records("t1", &[batch]).await.unwrap();

    // Verify the WriteResult indicates success.
    assert!(
        matches!(result.response, LanceResponse::Ok),
        "expected LanceResponse::Ok, got {:?}",
        result.response
    );

    // Allow the batcher linger + Raft apply.
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(count_rows(&node, "t1").await, 10);
    node.shutdown().await;
}

/// Multiple sequential writes accumulate correctly.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_sequential_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(10)),
    )
    .await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();

    for _ in 0..5 {
        let batch = make_batch(10);
        node.write_records("t1", &[batch]).await.unwrap();
    }

    assert_eq!(count_rows(&node, "t1").await, 50);
    node.shutdown().await;
}

/// Concurrent writes within the linger window are all served.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_within_linger_window() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(100)),
    )
    .await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();

    // Spawn 10 concurrent writers, each writing 5 rows.
    let mut handles = Vec::new();
    for _ in 0..10 {
        let n = node.clone();
        handles.push(tokio::spawn(async move {
            let batch = make_batch(5);
            n.write_records("t1", &[batch]).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(count_rows(&node, "t1").await, 50);
    node.shutdown().await;
}

/// Writes to different tables are isolated and all succeed.
#[tokio::test(flavor = "multi_thread")]
async fn per_table_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(50)),
    )
    .await;

    let schema = test_schema();
    node.create_table("events", &schema).await.unwrap();
    node.create_table("metrics", &schema).await.unwrap();

    let batch_a = make_batch(7);
    let batch_b = make_batch(13);

    let n1 = node.clone();
    let n2 = node.clone();
    let h1 = tokio::spawn(async move { n1.write_records("events", &[batch_a]).await });
    let h2 = tokio::spawn(async move { n2.write_records("metrics", &[batch_b]).await });

    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();

    assert_eq!(count_rows(&node, "events").await, 7);
    assert_eq!(count_rows(&node, "metrics").await, 13);
    node.shutdown().await;
}

/// Per-table config overrides the default.
#[tokio::test(flavor = "multi_thread")]
async fn per_table_config() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(10)),
    )
    .await;

    // Override "fast_table" with a very short linger.
    node.configure_table_batcher(
        "fast_table",
        WriteBatcherConfig::default().with_linger(Duration::from_millis(1)),
    );

    let schema = test_schema();
    node.create_table("fast_table", &schema).await.unwrap();
    node.create_table("normal_table", &schema).await.unwrap();

    let batch = make_batch(5);
    node.write_records("fast_table", &[batch.clone()])
        .await
        .unwrap();
    node.write_records("normal_table", &[batch]).await.unwrap();

    assert_eq!(count_rows(&node, "fast_table").await, 5);
    assert_eq!(count_rows(&node, "normal_table").await, 5);
    node.shutdown().await;
}

/// When max_batch_bytes is exceeded, the batcher flushes early
/// without waiting for the full linger window.
#[tokio::test(flavor = "multi_thread")]
async fn max_batch_bytes_triggers_early_flush() {
    let tmp = tempfile::tempdir().unwrap();
    // Very long linger, but tiny byte threshold — should flush immediately.
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default()
            .with_linger(Duration::from_secs(10))
            .with_max_batch_bytes(1), // 1 byte — always exceeded
    )
    .await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();

    let batch = make_batch(10);
    // This should not wait 10s because the byte threshold is immediately hit.
    let start = tokio::time::Instant::now();
    node.write_records("t1", &[batch]).await.unwrap();
    let elapsed = start.elapsed();

    // Should complete in well under 10s (the linger).
    assert!(
        elapsed < Duration::from_secs(5),
        "write took {elapsed:?}, expected < 5s (byte threshold should trigger early flush)"
    );

    assert_eq!(count_rows(&node, "t1").await, 10);
    node.shutdown().await;
}

/// Writing with multiple batches in a single call works.
#[tokio::test(flavor = "multi_thread")]
async fn multi_batch_single_call() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(10)),
    )
    .await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();

    let batch1 = make_batch(3);
    let batch2 = make_batch(7);
    node.write_records("t1", &[batch1, batch2]).await.unwrap();

    assert_eq!(count_rows(&node, "t1").await, 10);
    node.shutdown().await;
}

/// Empty batch writes are handled gracefully.
#[tokio::test(flavor = "multi_thread")]
async fn empty_batch_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(10)),
    )
    .await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();

    // Empty batches should return Ok immediately without touching the batcher.
    let result = node.write_records("t1", &[]).await;
    assert!(result.is_ok());

    node.shutdown().await;
}

/// Without a batcher configured, writes go through the direct Raft path.
#[tokio::test(flavor = "multi_thread")]
async fn direct_write_without_batcher() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_without_batcher(tmp.path()).await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();

    let batch = make_batch(10);
    node.write_records("t1", &[batch]).await.unwrap();

    assert_eq!(count_rows(&node, "t1").await, 10);
    node.shutdown().await;
}

/// High-volume concurrent writes: many small writes across multiple tables.
#[tokio::test(flavor = "multi_thread")]
async fn high_volume_concurrent_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _manager) = setup_with_batcher(
        tmp.path(),
        WriteBatcherConfig::default().with_linger(Duration::from_millis(20)),
    )
    .await;

    let schema = test_schema();
    node.create_table("t1", &schema).await.unwrap();
    node.create_table("t2", &schema).await.unwrap();

    let mut handles = Vec::new();
    for i in 0..20 {
        let n = node.clone();
        let table = if i % 2 == 0 { "t1" } else { "t2" };
        handles.push(tokio::spawn(async move {
            let batch = make_batch(3);
            n.write_records(table, &[batch]).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 10 writes of 3 rows each to t1, 10 writes of 3 rows each to t2.
    assert_eq!(count_rows(&node, "t1").await, 30);
    assert_eq!(count_rows(&node, "t2").await, 30);
    node.shutdown().await;
}
