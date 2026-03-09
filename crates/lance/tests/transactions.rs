//! Integration tests for multi-table transactions.
//!
//! These tests spin up a real 3-node Raft cluster and exercise the full
//! transaction lifecycle: TxnChunk → TxnCommit/TxnAbort, including
//! multi-table atomicity, rollback on failure, and GC of abandoned txns.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Limits the number of Raft clusters running concurrently within this test
/// binary. Each 3-node cluster spawns heartbeat timers and election timeouts;
/// too many in parallel starves the tokio runtime and triggers spurious
/// elections that violate openraft debug assertions.
static CLUSTER_SEMAPHORE: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(4)));

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bisque_lance::{
    AsyncApplyConfig, BisqueLance, BisqueLanceConfig, CatalogEventBus, CatalogEventKind,
    LanceManifestManager, LanceRaftNode, LanceResponse, LanceStateMachine, LanceTypeConfig,
    TxnConfig, TxnId, TxnOp, VersionPinTracker,
};
use bisque_raft::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MmapStorageConfig, MultiRaftManager, MultiplexedLogStorage,
    NodeAddressResolver,
};
use openraft::async_runtime::watch::WatchReceiver;
use std::net::{SocketAddr, TcpListener};
use tempfile::TempDir;

// =============================================================================
// Test Helpers
// =============================================================================

fn pick_unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local_addr")
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

async fn count_active_rows(engine: &BisqueLance, table_name: &str) -> usize {
    let table = engine.require_table(table_name).unwrap();
    let cat = table.catalog();
    let seg_path = table.config().segment_path(cat.active_segment);
    let uri = seg_path.to_str().unwrap();
    match lance::Dataset::open(uri).await {
        Ok(ds) => ds.count_rows(None).await.unwrap_or(0),
        Err(_) => 0,
    }
}

/// Count rows using the in-memory dataset handle (respects rollback).
async fn count_active_rows_via_handle(engine: &BisqueLance, table_name: &str) -> usize {
    let table = engine.require_table(table_name).unwrap();
    match table.active_dataset_snapshot().await {
        Some(ds) => ds.count_rows(None).await.unwrap_or(0),
        None => 0,
    }
}

fn encode_batches(batches: &[RecordBatch]) -> bytes::Bytes {
    bisque_lance::ipc::encode_record_batches(batches).unwrap()
}

fn encode_schema(schema: &Schema) -> bytes::Bytes {
    bisque_lance::ipc::schema_to_ipc(schema).unwrap()
}

// =============================================================================
// TestCluster (duplicated from raft_cluster.rs to keep test files independent)
// =============================================================================

struct TestCluster {
    node1: Arc<LanceRaftNode>,
    node2: Arc<LanceRaftNode>,
    node3: Arc<LanceRaftNode>,
    engine1: Arc<BisqueLance>,
    engine2: Arc<BisqueLance>,
    engine3: Arc<BisqueLance>,
    bus1: Arc<CatalogEventBus>,
    _dirs: Vec<TempDir>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl TestCluster {
    async fn new() -> Self {
        let permit = Arc::clone(&CLUSTER_SEMAPHORE)
            .acquire_owned()
            .await
            .unwrap();
        let addr1 = pick_unused_local_addr();
        let addr2 = pick_unused_local_addr();
        let addr3 = pick_unused_local_addr();

        let registry = Arc::new(DefaultNodeRegistry::<u64>::new());
        registry.register(1, addr1);
        registry.register(2, addr2);
        registry.register(3, addr3);

        let lance_dir1 = tempfile::tempdir().unwrap();
        let lance_dir2 = tempfile::tempdir().unwrap();
        let lance_dir3 = tempfile::tempdir().unwrap();
        let raft_dir1 = tempfile::tempdir().unwrap();
        let raft_dir2 = tempfile::tempdir().unwrap();
        let raft_dir3 = tempfile::tempdir().unwrap();

        let engine1 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir1.path()))
                .await
                .unwrap(),
        );
        let engine2 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir2.path()))
                .await
                .unwrap(),
        );
        let engine3 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir3.path()))
                .await
                .unwrap(),
        );

        let bus1 = Arc::new(CatalogEventBus::new(0));
        let bus2 = Arc::new(CatalogEventBus::new(0));
        let bus3 = Arc::new(CatalogEventBus::new(0));

        let pins1 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins2 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins3 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));

        let sm1 = LanceStateMachine::new(engine1.clone())
            .with_catalog_events(bus1.clone())
            .with_version_pins(pins1.clone());
        let sm2 = LanceStateMachine::new(engine2.clone())
            .with_catalog_events(bus2.clone())
            .with_version_pins(pins2.clone());
        let sm3 = LanceStateMachine::new(engine3.clone())
            .with_catalog_events(bus3.clone())
            .with_version_pins(pins3.clone());

        let storage1 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir1.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();
        let storage2 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir2.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();
        let storage3 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir3.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();

        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
            ..Default::default()
        };
        let transport1 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
        let transport2 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
        let transport3 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg, registry.clone());

        let manager1 = Arc::new(MultiRaftManager::new(transport1, storage1));
        let manager2 = Arc::new(MultiRaftManager::new(transport2, storage2));
        let manager3 = Arc::new(MultiRaftManager::new(transport3, storage3));

        let server1 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr1,
                ..Default::default()
            },
            manager1.clone(),
        ));
        let server2 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr2,
                ..Default::default()
            },
            manager2.clone(),
        ));
        let server3 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr3,
                ..Default::default()
            },
            manager3.clone(),
        ));

        tokio::spawn({
            let s = server1.clone();
            async move {
                let _ = s.serve().await;
            }
        });
        tokio::spawn({
            let s = server2.clone();
            async move {
                let _ = s.serve().await;
            }
        });
        tokio::spawn({
            let s = server3.clone();
            async move {
                let _ = s.serve().await;
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 1000,
                election_timeout_min: 5000,
                election_timeout_max: 10000,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::impls::BasicNode::default());
        members.insert(2u64, openraft::impls::BasicNode::default());
        members.insert(3u64, openraft::impls::BasicNode::default());

        let raft1 = manager1
            .add_group(0, 1, raft_cfg.clone(), sm1)
            .await
            .unwrap();
        let _raft2 = manager2
            .add_group(0, 2, raft_cfg.clone(), sm2)
            .await
            .unwrap();
        let _raft3 = manager3.add_group(0, 3, raft_cfg, sm3).await.unwrap();

        tokio::time::timeout(Duration::from_secs(15), raft1.initialize(members))
            .await
            .expect("init timeout")
            .expect("init failed");

        let raft1_clone = raft1.clone();
        tokio::time::timeout(Duration::from_secs(15), async move {
            loop {
                let m = raft1_clone.metrics().borrow_watched().clone();
                if m.current_leader == Some(1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("leader election timeout");

        let node1 = Arc::new(
            LanceRaftNode::new(raft1, engine1.clone(), 1)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node1.start();

        let raft2 = manager2.get_group(0).unwrap();
        let node2 = Arc::new(
            LanceRaftNode::new(raft2, engine2.clone(), 2)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node2.start();

        let raft3 = manager3.get_group(0).unwrap();
        let node3 = Arc::new(
            LanceRaftNode::new(raft3, engine3.clone(), 3)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node3.start();

        TestCluster {
            node1,
            node2,
            node3,
            engine1,
            engine2,
            engine3,
            bus1,
            _dirs: vec![
                lance_dir1, lance_dir2, lance_dir3, raft_dir1, raft_dir2, raft_dir3,
            ],
            _permit: permit,
        }
    }

    fn leader(&self) -> &Arc<LanceRaftNode> {
        &self.node1
    }

    fn leader_engine(&self) -> &Arc<BisqueLance> {
        &self.engine1
    }

    fn follower_engine(&self) -> &Arc<BisqueLance> {
        &self.engine2
    }

    fn follower2_engine(&self) -> &Arc<BisqueLance> {
        &self.engine3
    }

    async fn wait_for_replication(&self, min_log_index: u64) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let m2 = self.node2.raft().metrics().borrow_watched().clone();
                let m3 = self.node3.raft().metrics().borrow_watched().clone();
                let applied2 = m2.last_applied.map(|id| id.index).unwrap_or(0);
                let applied3 = m3.last_applied.map(|id| id.index).unwrap_or(0);
                if applied2 >= min_log_index && applied3 >= min_log_index {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replication wait timeout");
    }
}

// =============================================================================
// A. Happy Path: Multi-table Append Transaction
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_multi_table_append() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    // Create two tables.
    let _r1 = leader.create_table("orders", &test_schema()).await.unwrap();
    let r2 = leader
        .create_table("inventory", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(r2.log_index).await;

    // Begin a transaction.
    let txn_id = leader.begin_txn();

    // Chunk 0: append to orders.
    let orders_data = encode_batches(&[test_batch(&[1, 2], &["order_a", "order_b"])]);
    let chunk0 = leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "orders".into(),
                data: orders_data,
            }],
        )
        .await
        .unwrap();
    assert!(matches!(chunk0.response, LanceResponse::Ok));

    // Chunk 1: append to inventory.
    let inv_data = encode_batches(&[test_batch(&[10, 20], &["item_x", "item_y"])]);
    let chunk1 = leader
        .txn_chunk(
            txn_id,
            1,
            vec![TxnOp::Append {
                table: "inventory".into(),
                data: inv_data,
            }],
        )
        .await
        .unwrap();
    assert!(matches!(chunk1.response, LanceResponse::Ok));

    // Commit.
    let commit = leader.txn_commit(txn_id, 2).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "commit should succeed, got: {:?}",
        commit.response
    );

    // Wait for replication.
    cluster.wait_for_replication(commit.log_index).await;

    // Verify data on all nodes.
    for (name, engine) in [
        ("leader", cluster.leader_engine()),
        ("follower", cluster.follower_engine()),
        ("follower2", cluster.follower2_engine()),
    ] {
        let orders_rows = count_active_rows(engine, "orders").await;
        let inv_rows = count_active_rows(engine, "inventory").await;
        assert_eq!(orders_rows, 2, "{} should have 2 order rows", name);
        assert_eq!(inv_rows, 2, "{} should have 2 inventory rows", name);
    }
}

// =============================================================================
// B. Single-Chunk Transaction
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_single_chunk_multi_op() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    leader.create_table("t1", &test_schema()).await.unwrap();
    let r = leader.create_table("t2", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();

    // One chunk with ops for both tables.
    let data1 = encode_batches(&[test_batch(&[1], &["a"])]);
    let data2 = encode_batches(&[test_batch(&[2], &["b"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::Append {
                    table: "t1".into(),
                    data: data1,
                },
                TxnOp::Append {
                    table: "t2".into(),
                    data: data2,
                },
            ],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit.response, LanceResponse::Ok));
    cluster.wait_for_replication(commit.log_index).await;

    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 1);
    assert_eq!(count_active_rows(cluster.leader_engine(), "t2").await, 1);
}

// =============================================================================
// C. Abort: Discards Buffered Chunks
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_abort_discards_data() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();

    let data = encode_batches(&[test_batch(&[1, 2, 3], &["a", "b", "c"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data,
            }],
        )
        .await
        .unwrap();

    // Abort instead of committing.
    let abort = leader.txn_abort(txn_id).await.unwrap();
    assert!(matches!(abort.response, LanceResponse::Ok));
    cluster.wait_for_replication(abort.log_index).await;

    // No data should be written.
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 0);
    assert_eq!(count_active_rows(cluster.follower_engine(), "t1").await, 0);
}

// =============================================================================
// D. Commit Non-Existent Transaction
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_unknown_txn_returns_error() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let bogus_id = TxnId::new();
    let result = leader.txn_commit(bogus_id, 1).await;
    match result {
        Err(e) => {
            let msg = format!("{}", e);
            assert!(
                msg.contains("not found"),
                "expected 'not found' error, got: {}",
                msg
            );
        }
        Ok(wr) => match &wr.response {
            LanceResponse::Error(msg) => {
                assert!(
                    msg.contains("not found"),
                    "expected 'not found', got: {}",
                    msg
                );
            }
            other => panic!("expected error, got {:?}", other),
        },
    }
}

// =============================================================================
// E. Commit with Missing Chunk
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_missing_chunk_returns_error() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();

    // Send chunk 0 but claim total_chunks=2 (missing chunk 1).
    let data = encode_batches(&[test_batch(&[1], &["a"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let result = leader.txn_commit(txn_id, 2).await;
    match result {
        Err(e) => {
            let msg = format!("{}", e);
            assert!(
                msg.contains("missing chunk"),
                "expected 'missing chunk' error, got: {}",
                msg
            );
        }
        Ok(wr) => match &wr.response {
            LanceResponse::Error(msg) => {
                assert!(
                    msg.contains("missing chunk"),
                    "expected 'missing chunk', got: {}",
                    msg
                );
            }
            other => panic!("expected error, got {:?}", other),
        },
    }
}

// =============================================================================
// F. Commit Fails When Op Targets Non-Existent Table
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_fails_on_nonexistent_table() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Transaction with an op targeting a table that doesn't exist.
    let txn_id = leader.begin_txn();
    let data = encode_batches(&[test_batch(&[1], &["a"])]);

    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "nonexistent_table".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let result = leader.txn_commit(txn_id, 1).await;
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(is_error, "commit should fail for nonexistent table");
}

// =============================================================================
// F2. Rollback Restores Dataset on Failure (first op fails, nothing to undo)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_rollback_preserves_pre_txn_state() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Write initial data.
    let w = leader
        .write_records("t1", &[test_batch(&[100], &["initial"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w.log_index).await;
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 1);

    // Transaction that ONLY targets a nonexistent table (no ops on t1).
    let txn_id = leader.begin_txn();
    let data = encode_batches(&[test_batch(&[200], &["bad"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "no_such_table".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let _ = leader.txn_commit(txn_id, 1).await;

    // t1 should still have its original 1 row — the failed txn didn't touch it.
    let post_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(post_rows, 1, "t1 should be unaffected by failed txn");
}

// =============================================================================
// G. Abort Unknown Transaction is a No-Op
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_abort_unknown_is_noop() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    // Aborting a non-existent txn should succeed silently.
    let result = leader.txn_abort(TxnId::new()).await.unwrap();
    assert!(matches!(result.response, LanceResponse::Ok));
}

// =============================================================================
// H. Empty Transaction Commit
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_empty_commit() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let txn_id = leader.begin_txn();

    // Send a chunk with zero ops.
    leader.txn_chunk(txn_id, 0, vec![]).await.unwrap();

    let result = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(result.response, LanceResponse::Ok),
        "empty transaction commit should succeed"
    );
}

// =============================================================================
// I. Transaction with CreateTable + Append in Same Txn
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_create_and_append() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let txn_id = leader.begin_txn();

    let schema_ipc = encode_schema(&test_schema());
    let data = encode_batches(&[test_batch(&[1, 2], &["x", "y"])]);

    // Chunk 0: create table.
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::CreateTable {
                table: "new_tbl".into(),
                schema_ipc,
            }],
        )
        .await
        .unwrap();

    // Chunk 1: append data to it.
    leader
        .txn_chunk(
            txn_id,
            1,
            vec![TxnOp::Append {
                table: "new_tbl".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 2).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "create+append txn should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // Verify table exists and has data.
    let rows = count_active_rows(cluster.leader_engine(), "new_tbl").await;
    assert_eq!(rows, 2);
}

// =============================================================================
// J. Transaction with DropTable
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_drop_table() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("doomed", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::DropTable {
                table: "doomed".into(),
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "drop table txn should succeed"
    );
    cluster.wait_for_replication(commit.log_index).await;

    // Table should no longer exist.
    assert!(
        cluster.leader_engine().get_table("doomed").is_none(),
        "table should be dropped on leader"
    );
    assert!(
        cluster.follower_engine().get_table("doomed").is_none(),
        "table should be dropped on follower"
    );
}

// =============================================================================
// K. Multiple Independent Transactions
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_independent_txns() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Start two transactions concurrently.
    let txn_a = leader.begin_txn();
    let txn_b = leader.begin_txn();

    let data_a = encode_batches(&[test_batch(&[1], &["a"])]);
    let data_b = encode_batches(&[test_batch(&[2], &["b"])]);

    leader
        .txn_chunk(
            txn_a,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data_a,
            }],
        )
        .await
        .unwrap();

    leader
        .txn_chunk(
            txn_b,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data_b,
            }],
        )
        .await
        .unwrap();

    // Commit txn_a.
    let commit_a = leader.txn_commit(txn_a, 1).await.unwrap();
    assert!(matches!(commit_a.response, LanceResponse::Ok));

    // Abort txn_b.
    let abort_b = leader.txn_abort(txn_b).await.unwrap();
    assert!(matches!(abort_b.response, LanceResponse::Ok));

    cluster.wait_for_replication(abort_b.log_index).await;

    // Only txn_a's data should be present.
    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(rows, 1, "only txn_a's row should be present");
}

// =============================================================================
// L. Transaction Append after Regular Write
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_append_after_regular_write() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Regular write first.
    let w = leader
        .write_records("t1", &[test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w.log_index).await;

    // Then a transaction append.
    let txn_id = leader.begin_txn();
    let data = encode_batches(&[test_batch(&[3, 4], &["c", "d"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit.response, LanceResponse::Ok));
    cluster.wait_for_replication(commit.log_index).await;

    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(rows, 4, "should have 2 regular + 2 txn rows");
}

// =============================================================================
// M. TxnId uniqueness
// =============================================================================

#[test]
fn test_txn_id_begin_txn_unique() {
    // Verify begin_txn() generates unique IDs even in rapid succession.
    let mut ids = std::collections::HashSet::new();
    for _ in 0..1000 {
        let id = TxnId::new();
        assert!(ids.insert(id), "TxnId collision detected");
    }
}

// =============================================================================
// N. Transaction with Delete Op
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_delete_records() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Insert initial data.
    let w = leader
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w.log_index).await;
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 3);

    // Transaction: delete where id > 1.
    let txn_id = leader.begin_txn();
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Delete {
                table: "t1".into(),
                filter: "id > 1".to_string(),
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "delete txn should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(rows, 1, "should have 1 row after deleting where id > 1");
}

// =============================================================================
// O. Transaction with Update Op (delete + re-append)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_update_records() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Insert initial data: ids 1,2,3.
    let w = leader
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w.log_index).await;
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 3);

    // Transaction: update rows where id > 1 → replace with new data.
    let txn_id = leader.begin_txn();
    let replacement = encode_batches(&[test_batch(&[20, 30], &["bb", "cc"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Update {
                table: "t1".into(),
                filter: "id > 1".to_string(),
                data: replacement,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "update txn should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // After update: original row id=1 remains, rows id=2,3 deleted and replaced with id=20,30.
    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(
        rows, 3,
        "should have 3 rows: id=1 (kept) + id=20,30 (replaced)"
    );
}

// =============================================================================
// P. Multiple Chunks Targeting the Same Table
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_multiple_chunks_same_table() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();

    // Chunk 0: append 2 rows to t1.
    let data0 = encode_batches(&[test_batch(&[1, 2], &["a", "b"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data0,
            }],
        )
        .await
        .unwrap();

    // Chunk 1: append 3 more rows to t1 (same table, different chunk).
    let data1 = encode_batches(&[test_batch(&[3, 4, 5], &["c", "d", "e"])]);
    leader
        .txn_chunk(
            txn_id,
            1,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data1,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 2).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "multi-chunk same-table txn should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // All 5 rows should be present from both chunks.
    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(rows, 5, "should have 5 rows from 2 chunks");
}

// =============================================================================
// Q. Mixed Ops Across Multiple Chunks and Tables
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_mixed_ops_multi_chunk_multi_table() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let _r1 = leader.create_table("t1", &test_schema()).await.unwrap();
    let r2 = leader.create_table("t2", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r2.log_index).await;

    // Pre-populate t1 with data that we'll later delete.
    let w = leader
        .write_records("t1", &[test_batch(&[10, 20, 30], &["x", "y", "z"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w.log_index).await;

    let txn_id = leader.begin_txn();

    // Chunk 0: delete from t1, append to t2.
    let t2_data = encode_batches(&[test_batch(&[1, 2], &["a", "b"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::Delete {
                    table: "t1".into(),
                    filter: "id > 10".to_string(),
                },
                TxnOp::Append {
                    table: "t2".into(),
                    data: t2_data,
                },
            ],
        )
        .await
        .unwrap();

    // Chunk 1: append more to t1.
    let t1_data = encode_batches(&[test_batch(&[40, 50], &["aa", "bb"])]);
    leader
        .txn_chunk(
            txn_id,
            1,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: t1_data,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 2).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "mixed ops txn should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // t1: started with 3 rows, deleted 2 (id > 10), appended 2 → 3 rows.
    let t1_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(t1_rows, 3, "t1: 1 kept + 2 appended = 3 rows");

    // t2: appended 2 rows.
    let t2_rows = count_active_rows(cluster.leader_engine(), "t2").await;
    assert_eq!(t2_rows, 2, "t2: 2 appended rows");
}

// =============================================================================
// R0. Partial Rollback: first table append succeeds, second table fails
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_partial_rollback_restores_all_tables() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    // Create t1 and pre-populate with initial data.
    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let w = leader
        .write_records("t1", &[test_batch(&[100], &["initial"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w.log_index).await;
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 1);

    // Transaction: append to t1 (will succeed), then append to nonexistent table (will fail).
    // The rollback should restore t1's dataset handle so the append is reverted.
    let txn_id = leader.begin_txn();

    let t1_data = encode_batches(&[test_batch(&[200, 300], &["new_a", "new_b"])]);
    let bad_data = encode_batches(&[test_batch(&[999], &["bad"])]);

    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::Append {
                    table: "t1".into(),
                    data: t1_data,
                },
                TxnOp::Append {
                    table: "nonexistent_table".into(),
                    data: bad_data,
                },
            ],
        )
        .await
        .unwrap();

    let result = leader.txn_commit(txn_id, 1).await;

    // Commit should fail.
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(is_error, "commit should fail for nonexistent table");

    // t1 should be rolled back to its pre-txn state (only 1 row).
    // We use the handle-based count because rollback restores the in-memory
    // dataset handle to the pre-txn version (the on-disk Lance dataset retains
    // all versions, so opening fresh would see the latest physical version).
    let rows = count_active_rows_via_handle(cluster.leader_engine(), "t1").await;
    assert_eq!(
        rows, 1,
        "t1 should have been rolled back to 1 row, but has {}",
        rows
    );
}

// =============================================================================
// R. Duplicate Chunk Sequence Numbers (overwrites)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_duplicate_chunk_seq_overwrites() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();

    // Send chunk seq=0 with 2 rows.
    let data1 = encode_batches(&[test_batch(&[1, 2], &["a", "b"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data1,
            }],
        )
        .await
        .unwrap();

    // Send chunk seq=0 AGAIN with 3 rows (BTreeMap insert overwrites).
    let data2 = encode_batches(&[test_batch(&[3, 4, 5], &["c", "d", "e"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data2,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit.response, LanceResponse::Ok));
    cluster.wait_for_replication(commit.log_index).await;

    // The second chunk seq=0 should have overwritten the first.
    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(
        rows, 3,
        "duplicate seq=0 should use the last value (3 rows)"
    );
}

// =============================================================================
// Configurable Test Cluster
// =============================================================================
//
// Supports optional TxnConfig and LanceManifestManager for testing GC,
// manifest persistence, and snapshot-install clearing pending txns.

#[allow(dead_code)]
struct ConfigurableTestCluster {
    node1: Arc<LanceRaftNode>,
    node2: Arc<LanceRaftNode>,
    node3: Arc<LanceRaftNode>,
    engine1: Arc<BisqueLance>,
    engine2: Arc<BisqueLance>,
    engine3: Arc<BisqueLance>,
    manifest: Option<Arc<LanceManifestManager>>,
    _dirs: Vec<TempDir>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ConfigurableTestCluster {
    async fn new(txn_config: Option<TxnConfig>, enable_manifest: bool) -> Self {
        let permit = Arc::clone(&CLUSTER_SEMAPHORE)
            .acquire_owned()
            .await
            .unwrap();
        let addr1 = pick_unused_local_addr();
        let addr2 = pick_unused_local_addr();
        let addr3 = pick_unused_local_addr();

        let registry = Arc::new(DefaultNodeRegistry::<u64>::new());
        registry.register(1, addr1);
        registry.register(2, addr2);
        registry.register(3, addr3);

        let lance_dir1 = tempfile::tempdir().unwrap();
        let lance_dir2 = tempfile::tempdir().unwrap();
        let lance_dir3 = tempfile::tempdir().unwrap();
        let raft_dir1 = tempfile::tempdir().unwrap();
        let raft_dir2 = tempfile::tempdir().unwrap();
        let raft_dir3 = tempfile::tempdir().unwrap();
        let manifest_dir1 = tempfile::tempdir().unwrap();
        let manifest_dir2 = tempfile::tempdir().unwrap();
        let manifest_dir3 = tempfile::tempdir().unwrap();

        let engine1 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir1.path()))
                .await
                .unwrap(),
        );
        let engine2 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir2.path()))
                .await
                .unwrap(),
        );
        let engine3 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir3.path()))
                .await
                .unwrap(),
        );

        let bus1 = Arc::new(CatalogEventBus::new(0));
        let bus2 = Arc::new(CatalogEventBus::new(0));
        let bus3 = Arc::new(CatalogEventBus::new(0));

        let pins1 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins2 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins3 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));

        // Each node gets its own manifest manager for isolation.
        let (manifest1, manifest2, manifest3) = if enable_manifest {
            let m1 = Arc::new(LanceManifestManager::new(manifest_dir1.path()).unwrap());
            m1.open_group(0).unwrap();
            let m2 = Arc::new(LanceManifestManager::new(manifest_dir2.path()).unwrap());
            m2.open_group(0).unwrap();
            let m3 = Arc::new(LanceManifestManager::new(manifest_dir3.path()).unwrap());
            m3.open_group(0).unwrap();
            (Some(m1), Some(m2), Some(m3))
        } else {
            (None, None, None)
        };

        let mut sm1 = LanceStateMachine::new(engine1.clone())
            .with_catalog_events(bus1.clone())
            .with_version_pins(pins1.clone());
        let mut sm2 = LanceStateMachine::new(engine2.clone())
            .with_catalog_events(bus2.clone())
            .with_version_pins(pins2.clone());
        let mut sm3 = LanceStateMachine::new(engine3.clone())
            .with_catalog_events(bus3.clone())
            .with_version_pins(pins3.clone());

        if let Some(config) = &txn_config {
            sm1 = sm1.with_txn_config(config.clone());
            sm2 = sm2.with_txn_config(config.clone());
            sm3 = sm3.with_txn_config(config.clone());
        }

        if let Some(m) = &manifest1 {
            sm1 = sm1.with_manifest(m.clone(), 0);
        }
        if let Some(m) = &manifest2 {
            sm2 = sm2.with_manifest(m.clone(), 0);
        }
        if let Some(m) = &manifest3 {
            sm3 = sm3.with_manifest(m.clone(), 0);
        }

        let storage1 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir1.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();
        let storage2 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir2.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();
        let storage3 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir3.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();

        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
            ..Default::default()
        };
        let transport1 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
        let transport2 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
        let transport3 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg, registry.clone());

        let manager1 = Arc::new(MultiRaftManager::new(transport1, storage1));
        let manager2 = Arc::new(MultiRaftManager::new(transport2, storage2));
        let manager3 = Arc::new(MultiRaftManager::new(transport3, storage3));

        let server1 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr1,
                ..Default::default()
            },
            manager1.clone(),
        ));
        let server2 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr2,
                ..Default::default()
            },
            manager2.clone(),
        ));
        let server3 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr3,
                ..Default::default()
            },
            manager3.clone(),
        ));

        tokio::spawn({
            let s = server1.clone();
            async move {
                let _ = s.serve().await;
            }
        });
        tokio::spawn({
            let s = server2.clone();
            async move {
                let _ = s.serve().await;
            }
        });
        tokio::spawn({
            let s = server3.clone();
            async move {
                let _ = s.serve().await;
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 1000,
                election_timeout_min: 5000,
                election_timeout_max: 10000,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::impls::BasicNode::default());
        members.insert(2u64, openraft::impls::BasicNode::default());
        members.insert(3u64, openraft::impls::BasicNode::default());

        let raft1 = manager1
            .add_group(0, 1, raft_cfg.clone(), sm1)
            .await
            .unwrap();
        let _raft2 = manager2
            .add_group(0, 2, raft_cfg.clone(), sm2)
            .await
            .unwrap();
        let _raft3 = manager3.add_group(0, 3, raft_cfg, sm3).await.unwrap();

        tokio::time::timeout(Duration::from_secs(15), raft1.initialize(members))
            .await
            .expect("init timeout")
            .expect("init failed");

        let raft1_clone = raft1.clone();
        tokio::time::timeout(Duration::from_secs(15), async move {
            loop {
                let m = raft1_clone.metrics().borrow_watched().clone();
                if m.current_leader == Some(1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("leader election timeout");

        let node1 = Arc::new(
            LanceRaftNode::new(raft1, engine1.clone(), 1)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node1.start();

        let raft2 = manager2.get_group(0).unwrap();
        let node2 = Arc::new(
            LanceRaftNode::new(raft2, engine2.clone(), 2)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node2.start();

        let raft3 = manager3.get_group(0).unwrap();
        let node3 = Arc::new(
            LanceRaftNode::new(raft3, engine3.clone(), 3)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node3.start();

        let dirs = vec![
            lance_dir1,
            lance_dir2,
            lance_dir3,
            raft_dir1,
            raft_dir2,
            raft_dir3,
            manifest_dir1,
            manifest_dir2,
            manifest_dir3,
        ];

        ConfigurableTestCluster {
            node1,
            node2,
            node3,
            engine1,
            engine2,
            engine3,
            manifest: manifest1,
            _dirs: dirs,
            _permit: permit,
        }
    }

    fn leader(&self) -> &Arc<LanceRaftNode> {
        &self.node1
    }

    #[allow(dead_code)]
    fn leader_engine(&self) -> &Arc<BisqueLance> {
        &self.engine1
    }

    async fn wait_for_replication(&self, min_log_index: u64) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let m2 = self.node2.raft().metrics().borrow_watched().clone();
                let m3 = self.node3.raft().metrics().borrow_watched().clone();
                let applied2 = m2.last_applied.map(|id| id.index).unwrap_or(0);
                let applied3 = m3.last_applied.map(|id| id.index).unwrap_or(0);
                if applied2 >= min_log_index && applied3 >= min_log_index {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replication wait timeout");
    }
}

// =============================================================================
// S. GC Integration Test: Abandoned transactions evicted after enough log entries
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_gc_evicts_abandoned_transactions() {
    // Use a very aggressive GC config: check every 5 entries, timeout after 10 log entries.
    let config = TxnConfig {
        max_txn_bytes: 256 * 1024 * 1024,
        max_total_pending_bytes: 1024 * 1024 * 1024,
        gc_log_index_timeout: 10,
        gc_check_interval: 5,
    };
    let cluster = ConfigurableTestCluster::new(Some(config), false).await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Start a transaction and send a chunk, but never commit.
    let txn_id = leader.begin_txn();
    let data = encode_batches(&[test_batch(&[1], &["abandoned"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data,
            }],
        )
        .await
        .unwrap();

    // Issue enough regular writes to advance the log index past the GC timeout.
    // We need at least gc_log_index_timeout + gc_check_interval log entries.
    for i in 0..20 {
        let batch = test_batch(&[1000 + i], &["filler"]);
        let w = leader.write_records("t1", &[batch]).await.unwrap();
        if i == 19 {
            cluster.wait_for_replication(w.log_index).await;
        }
    }

    // Now try to commit the abandoned transaction — it should have been GC'd.
    let result = leader.txn_commit(txn_id, 1).await;
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(
        is_error,
        "commit should fail because txn was GC'd, got: {:?}",
        result
    );
}

// =============================================================================
// T. MDBX Manifest Persistence After Transaction Commit
// =============================================================================

/// Poll manifest until a condition is met, with timeout.
async fn wait_for_manifest(
    manifest: &LanceManifestManager,
    group_id: u64,
    check: impl Fn(&std::collections::HashMap<String, bisque_lance::PersistedTableEntry>) -> bool,
    msg: &str,
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if manifest.read_all_tables(group_id).is_ok_and(|t| check(&t)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("manifest wait timeout: {}", msg));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_manifest_persistence_after_commit() {
    let cluster = ConfigurableTestCluster::new(None, true).await;
    let leader = cluster.leader();
    let manifest = cluster.manifest.as_ref().unwrap();

    // Create two tables via regular commands first.
    let _r1 = leader.create_table("orders", &test_schema()).await.unwrap();
    let r2 = leader
        .create_table("inventory", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(r2.log_index).await;

    // Poll until both tables appear in manifest.
    wait_for_manifest(
        manifest,
        0,
        |t| t.contains_key("orders") && t.contains_key("inventory"),
        "tables should appear after create",
    )
    .await;

    // Transaction: append to both tables.
    let txn_id = leader.begin_txn();
    let orders_data = encode_batches(&[test_batch(&[1, 2], &["o_a", "o_b"])]);
    let inv_data = encode_batches(&[test_batch(&[10], &["item_x"])]);

    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::Append {
                    table: "orders".into(),
                    data: orders_data,
                },
                TxnOp::Append {
                    table: "inventory".into(),
                    data: inv_data,
                },
            ],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "commit should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // Poll until manifest reflects the txn commit (both tables persisted).
    wait_for_manifest(
        manifest,
        0,
        |t| t.contains_key("orders") && t.contains_key("inventory"),
        "both tables should be persisted after txn commit",
    )
    .await;

    let tables_after = manifest.read_all_tables(0).unwrap();
    let orders_entry = &tables_after["orders"];
    let inv_entry = &tables_after["inventory"];
    assert!(
        orders_entry.catalog.active_segment > 0,
        "orders active_segment should be > 0"
    );
    assert!(
        inv_entry.catalog.active_segment > 0,
        "inventory active_segment should be > 0"
    );
}

// =============================================================================
// U. Per-Transaction Memory Cap Enforcement
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_per_txn_memory_cap() {
    // Tiny per-txn limit: 100 bytes.
    let config = TxnConfig {
        max_txn_bytes: 100,
        max_total_pending_bytes: 1024 * 1024 * 1024,
        gc_log_index_timeout: 100_000,
        gc_check_interval: 1_000,
    };
    let cluster = ConfigurableTestCluster::new(Some(config), false).await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();

    // First chunk: small data, should succeed.
    let small_data = encode_batches(&[test_batch(&[1], &["a"])]);
    let r1 = leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: small_data,
            }],
        )
        .await;
    // May or may not exceed 100 bytes depending on IPC encoding overhead.
    // The exact behavior depends on IPC batch size, so let's just test
    // that the system enforces limits by sending definitely-large data.
    drop(r1);

    let txn_id2 = leader.begin_txn();
    // Large data that definitely exceeds 100 bytes.
    let large_data = encode_batches(&[test_batch(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    )]);

    let result = leader
        .txn_chunk(
            txn_id2,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: large_data,
            }],
        )
        .await;

    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(
        is_error,
        "chunk should be rejected for exceeding per-txn memory cap, got: {:?}",
        result
    );
}

// =============================================================================
// V. Global Pending Memory Cap Enforcement
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_global_memory_cap() {
    // Set global limit to 10KB so one large chunk fills it.
    let config = TxnConfig {
        max_txn_bytes: 1024 * 1024,
        max_total_pending_bytes: 10_000,
        gc_log_index_timeout: 100_000,
        gc_check_interval: 1_000,
    };
    let cluster = ConfigurableTestCluster::new(Some(config), false).await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // First txn: send a chunk with a large payload that takes most of the budget.
    // TxnOp::Append payload_bytes() returns data.len(), so use raw Bytes.
    let txn1 = leader.begin_txn();
    leader
        .txn_chunk(
            txn1,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: bytes::Bytes::from(vec![0u8; 8_000]),
            }],
        )
        .await
        .unwrap();

    // Second txn: another large chunk that would exceed the 10KB global cap.
    let txn2 = leader.begin_txn();
    let result = leader
        .txn_chunk(
            txn2,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: bytes::Bytes::from(vec![0u8; 8_000]),
            }],
        )
        .await;

    // Second chunk should be rejected: 8000 + 8000 = 16000 > 10000.
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(
        is_error,
        "second chunk should be rejected due to global memory cap, got: {:?}",
        result
    );
}

// =============================================================================
// W. Transaction with CreateTable in same txn as DropTable
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_create_drop_in_same_txn() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    // Pre-create a table to drop.
    let r = leader.create_table("old_t", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();
    let schema_ipc = encode_schema(&test_schema());

    // Drop old table and create new table in same transaction.
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::DropTable {
                    table: "old_t".into(),
                },
                TxnOp::CreateTable {
                    table: "new_t".into(),
                    schema_ipc,
                },
            ],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "drop+create txn should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // old_t should be gone, new_t should exist.
    assert!(
        cluster.leader_engine().get_table("old_t").is_none(),
        "old_t should be dropped"
    );
    assert!(
        cluster.leader_engine().get_table("new_t").is_some(),
        "new_t should exist"
    );
}

// =============================================================================
// X. Malformed IPC Data Causes Rollback
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_malformed_ipc_causes_rollback() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Pre-populate with 1 row.
    let w = leader
        .write_records("t1", &[test_batch(&[1], &["initial"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w.log_index).await;
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 1);

    // Transaction: valid append to t1, then append with garbage IPC data.
    let txn_id = leader.begin_txn();
    let good_data = encode_batches(&[test_batch(&[2], &["good"])]);
    let bad_ipc = bytes::Bytes::from_static(&[0xFF, 0xFE, 0xFD, 0x00, 0x01]);

    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::Append {
                    table: "t1".into(),
                    data: good_data,
                },
                TxnOp::Append {
                    table: "t1".into(),
                    data: bad_ipc,
                },
            ],
        )
        .await
        .unwrap();

    let result = leader.txn_commit(txn_id, 1).await;

    // Commit should fail due to IPC decode error.
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(is_error, "commit should fail for malformed IPC data");

    // t1 should be rolled back to 1 row (the good append reverted).
    let rows = count_active_rows_via_handle(cluster.leader_engine(), "t1").await;
    assert_eq!(
        rows, 1,
        "t1 should have been rolled back after malformed IPC, but has {}",
        rows
    );
}

// =============================================================================
// Y. Manifest Persistence for 3+ Tables (exercises 2nd+ table branch)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_txn_manifest_persistence_three_tables() {
    let cluster = ConfigurableTestCluster::new(None, true).await;
    let leader = cluster.leader();
    let manifest = cluster.manifest.as_ref().unwrap();

    // Create three tables.
    let _r1 = leader.create_table("t1", &test_schema()).await.unwrap();
    let _r2 = leader.create_table("t2", &test_schema()).await.unwrap();
    let r3 = leader.create_table("t3", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r3.log_index).await;

    // Poll until all three appear in manifest.
    wait_for_manifest(
        manifest,
        0,
        |t| t.contains_key("t1") && t.contains_key("t2") && t.contains_key("t3"),
        "all 3 tables should appear after create",
    )
    .await;

    // Transaction: append to all three tables.
    // In apply_txn_commit, the first table uses the manifest_table_update slot,
    // and subsequent tables go through the `else if let Some(manifest)` branch.
    let txn_id = leader.begin_txn();
    let d1 = encode_batches(&[test_batch(&[1], &["a"])]);
    let d2 = encode_batches(&[test_batch(&[2], &["b"])]);
    let d3 = encode_batches(&[test_batch(&[3], &["c"])]);

    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::Append {
                    table: "t1".into(),
                    data: d1,
                },
                TxnOp::Append {
                    table: "t2".into(),
                    data: d2,
                },
                TxnOp::Append {
                    table: "t3".into(),
                    data: d3,
                },
            ],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "3-table txn commit should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // Poll until all three tables are persisted in manifest.
    wait_for_manifest(
        manifest,
        0,
        |t| t.contains_key("t1") && t.contains_key("t2") && t.contains_key("t3"),
        "all 3 tables should be persisted after txn commit",
    )
    .await;

    // Verify data on leader.
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 1);
    assert_eq!(count_active_rows(cluster.leader_engine(), "t2").await, 1);
    assert_eq!(count_active_rows(cluster.leader_engine(), "t3").await, 1);
}

// =============================================================================
// Catalog Event Emission Tests
// =============================================================================

/// Verify that committing a transaction with appends emits DataMutated events.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_emits_catalog_events_for_appends() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();
    let mut rx = cluster.bus1.subscribe();

    // Create two tables (these emit TableCreated events — drain them).
    leader.create_table("t1", &test_schema()).await.unwrap();
    let r = leader.create_table("t2", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Drain create events.
    while rx.try_recv().is_ok() {}

    // Transaction: append to both tables.
    let txn_id = leader.begin_txn();
    let d1 = encode_batches(&[test_batch(&[1], &["a"])]);
    let d2 = encode_batches(&[test_batch(&[2], &["b"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::Append {
                    table: "t1".into(),
                    data: d1,
                },
                TxnOp::Append {
                    table: "t2".into(),
                    data: d2,
                },
            ],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit.response, LanceResponse::Ok));

    // Collect events within a short window.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }

    // Should have 2 DataMutated events (one per table).
    let data_mutated: Vec<_> = events
        .iter()
        .filter(|e| matches!(&e.event, CatalogEventKind::DataMutated { .. }))
        .collect();
    assert_eq!(
        data_mutated.len(),
        2,
        "Expected 2 DataMutated events, got {}: {:?}",
        data_mutated.len(),
        events
    );

    // Verify table names.
    let mut tables: Vec<&str> = data_mutated
        .iter()
        .filter_map(|e| match &e.event {
            CatalogEventKind::DataMutated { table, .. } => Some(&**table),
            _ => None,
        })
        .collect();
    tables.sort();
    assert_eq!(tables, vec!["t1", "t2"]);
}

/// Verify that a txn with CreateTable + DropTable emits the right events.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_emits_catalog_events_for_create_and_drop() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();
    let mut rx = cluster.bus1.subscribe();

    // Create a table to drop later.
    let r = leader
        .create_table("to_drop", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Drain create event.
    while rx.try_recv().is_ok() {}

    // Transaction: create a new table + drop the existing one.
    let txn_id = leader.begin_txn();
    let schema_ipc = encode_schema(&test_schema());
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::CreateTable {
                    table: "new_table".into(),
                    schema_ipc,
                },
                TxnOp::DropTable {
                    table: "to_drop".into(),
                },
            ],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit.response, LanceResponse::Ok));

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }

    let created: Vec<_> = events
        .iter()
        .filter(|e| matches!(&e.event, CatalogEventKind::TableCreated { .. }))
        .collect();
    let dropped: Vec<_> = events
        .iter()
        .filter(|e| matches!(&e.event, CatalogEventKind::TableDropped { .. }))
        .collect();
    assert_eq!(created.len(), 1, "Expected 1 TableCreated event");
    assert_eq!(dropped.len(), 1, "Expected 1 TableDropped event");

    // Verify table names.
    match &created[0].event {
        CatalogEventKind::TableCreated { table, .. } => assert_eq!(&**table, "new_table"),
        _ => unreachable!(),
    }
    match &dropped[0].event {
        CatalogEventKind::TableDropped { table } => assert_eq!(&**table, "to_drop"),
        _ => unreachable!(),
    }
}

/// Verify that a failed txn commit does NOT emit catalog events.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_rollback_emits_no_catalog_events() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();
    let mut rx = cluster.bus1.subscribe();

    // Create one table.
    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Drain create event.
    while rx.try_recv().is_ok() {}

    // Transaction targeting a nonexistent table → will fail on commit.
    let txn_id = leader.begin_txn();
    let data = encode_batches(&[test_batch(&[1], &["a"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "nonexistent".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let result = leader.txn_commit(txn_id, 1).await;
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(is_error, "commit should fail for nonexistent table");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }

    assert!(
        events.is_empty(),
        "Failed txn should emit no catalog events, got: {:?}",
        events
    );
}

/// Verify that a txn with delete/update ops emits DataMutated events.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_emits_catalog_events_for_delete_and_update() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();
    let mut rx = cluster.bus1.subscribe();

    // Create table and insert initial data.
    leader.create_table("t1", &test_schema()).await.unwrap();

    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    let r = leader.write_records("t1", &[batch]).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Drain prior events.
    while rx.try_recv().is_ok() {}

    // Transaction: delete from t1.
    let txn_id = leader.begin_txn();
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Delete {
                table: "t1".into(),
                filter: "id = 1".to_string(),
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit.response, LanceResponse::Ok));

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }

    let data_mutated: Vec<_> = events
        .iter()
        .filter(|e| matches!(&e.event, CatalogEventKind::DataMutated { .. }))
        .collect();
    assert_eq!(
        data_mutated.len(),
        1,
        "Expected 1 DataMutated event for delete, got {:?}",
        events
    );
}

// =============================================================================
// Async Buffer Drain Test
// =============================================================================

/// Cluster variant that uses async apply (background writer) for appends.
struct AsyncApplyTestCluster {
    node1: Arc<LanceRaftNode>,
    node2: Arc<LanceRaftNode>,
    node3: Arc<LanceRaftNode>,
    engine1: Arc<BisqueLance>,
    _dirs: Vec<TempDir>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl AsyncApplyTestCluster {
    async fn new() -> Self {
        let permit = Arc::clone(&CLUSTER_SEMAPHORE)
            .acquire_owned()
            .await
            .unwrap();
        let addr1 = pick_unused_local_addr();
        let addr2 = pick_unused_local_addr();
        let addr3 = pick_unused_local_addr();

        let registry = Arc::new(DefaultNodeRegistry::<u64>::new());
        registry.register(1, addr1);
        registry.register(2, addr2);
        registry.register(3, addr3);

        let lance_dir1 = tempfile::tempdir().unwrap();
        let lance_dir2 = tempfile::tempdir().unwrap();
        let lance_dir3 = tempfile::tempdir().unwrap();
        let raft_dir1 = tempfile::tempdir().unwrap();
        let raft_dir2 = tempfile::tempdir().unwrap();
        let raft_dir3 = tempfile::tempdir().unwrap();

        let engine1 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir1.path()))
                .await
                .unwrap(),
        );
        let engine2 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir2.path()))
                .await
                .unwrap(),
        );
        let engine3 = Arc::new(
            BisqueLance::open(BisqueLanceConfig::new(lance_dir3.path()))
                .await
                .unwrap(),
        );

        let pins1 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins2 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins3 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));

        // Use async apply — background writer for appends.
        let async_cfg = AsyncApplyConfig::default();
        let (sm1, _wm1) =
            LanceStateMachine::with_async_apply(engine1.clone(), async_cfg.clone(), "test");
        let sm1 = sm1.with_version_pins(pins1.clone());
        let (sm2, _wm2) =
            LanceStateMachine::with_async_apply(engine2.clone(), async_cfg.clone(), "test");
        let sm2 = sm2.with_version_pins(pins2.clone());
        let (sm3, _wm3) = LanceStateMachine::with_async_apply(engine3.clone(), async_cfg, "test");
        let sm3 = sm3.with_version_pins(pins3.clone());

        let storage1 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir1.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();
        let storage2 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir2.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();
        let storage3 = MultiplexedLogStorage::<LanceTypeConfig>::new(
            MmapStorageConfig::new(raft_dir3.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .unwrap();

        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
            ..Default::default()
        };
        let transport1 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
        let transport2 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
        let transport3 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg, registry.clone());

        let manager1 = Arc::new(MultiRaftManager::new(transport1, storage1));
        let manager2 = Arc::new(MultiRaftManager::new(transport2, storage2));
        let manager3 = Arc::new(MultiRaftManager::new(transport3, storage3));

        let server1 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr1,
                ..Default::default()
            },
            manager1.clone(),
        ));
        let server2 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr2,
                ..Default::default()
            },
            manager2.clone(),
        ));
        let server3 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr3,
                ..Default::default()
            },
            manager3.clone(),
        ));

        tokio::spawn({
            let s = server1.clone();
            async move {
                let _ = s.serve().await;
            }
        });
        tokio::spawn({
            let s = server2.clone();
            async move {
                let _ = s.serve().await;
            }
        });
        tokio::spawn({
            let s = server3.clone();
            async move {
                let _ = s.serve().await;
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 1000,
                election_timeout_min: 5000,
                election_timeout_max: 10000,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::impls::BasicNode::default());
        members.insert(2u64, openraft::impls::BasicNode::default());
        members.insert(3u64, openraft::impls::BasicNode::default());

        let raft1 = manager1
            .add_group(0, 1, raft_cfg.clone(), sm1)
            .await
            .unwrap();
        let _raft2 = manager2
            .add_group(0, 2, raft_cfg.clone(), sm2)
            .await
            .unwrap();
        let _raft3 = manager3.add_group(0, 3, raft_cfg, sm3).await.unwrap();

        tokio::time::timeout(Duration::from_secs(15), raft1.initialize(members))
            .await
            .expect("init timeout")
            .expect("init failed");

        let raft1_clone = raft1.clone();
        tokio::time::timeout(Duration::from_secs(15), async move {
            loop {
                let m = raft1_clone.metrics().borrow_watched().clone();
                if m.current_leader == Some(1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("leader election timeout");

        let node1 = Arc::new(
            LanceRaftNode::new(raft1, engine1.clone(), 1)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node1.start();

        let raft2 = manager2.get_group(0).unwrap();
        let node2 = Arc::new(
            LanceRaftNode::new(raft2, engine2.clone(), 2)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node2.start();

        let raft3 = manager3.get_group(0).unwrap();
        let node3 = Arc::new(
            LanceRaftNode::new(raft3, engine3.clone(), 3)
                .with_seal_check_interval(Duration::from_secs(999))
                .with_flush_check_interval(Duration::from_secs(999))
                .with_compaction_check_interval(Duration::from_secs(999)),
        );
        node3.start();

        AsyncApplyTestCluster {
            node1,
            node2,
            node3,
            engine1,
            _dirs: vec![
                lance_dir1, lance_dir2, lance_dir3, raft_dir1, raft_dir2, raft_dir3,
            ],
            _permit: permit,
        }
    }

    fn leader(&self) -> &Arc<LanceRaftNode> {
        &self.node1
    }

    fn leader_engine(&self) -> &Arc<BisqueLance> {
        &self.engine1
    }

    async fn wait_for_replication(&self, min_log_index: u64) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let m2 = self.node2.raft().metrics().borrow_watched().clone();
                let m3 = self.node3.raft().metrics().borrow_watched().clone();
                let applied2 = m2.last_applied.map(|id| id.index).unwrap_or(0);
                let applied3 = m3.last_applied.map(|id| id.index).unwrap_or(0);
                if applied2 >= min_log_index && applied3 >= min_log_index {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replication wait timeout");
    }
}

/// Test that the async buffer is drained before a txn commit applies its ops.
///
/// This verifies that `apply_txn_commit()` calls `drain_table()` on the async
/// buffer before snapshotting and applying transaction ops, ensuring that
/// any data enqueued via regular `write_records()` is flushed first.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_drains_async_buffer() {
    let cluster = AsyncApplyTestCluster::new().await;
    let leader = cluster.leader();

    // Create a table.
    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Write data via regular path (goes through async buffer).
    let batch1 = test_batch(&[1, 2], &["async_a", "async_b"]);
    let r = leader.write_records("t1", &[batch1]).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Now commit a transaction that also touches t1.
    // The txn commit must drain the async buffer first, so the 2 rows from
    // write_records should be visible + the 1 row from the txn.
    let txn_id = leader.begin_txn();
    let txn_data = encode_batches(&[test_batch(&[3], &["txn_c"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: txn_data,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(
        matches!(commit.response, LanceResponse::Ok),
        "txn commit should succeed, got: {:?}",
        commit.response
    );
    cluster.wait_for_replication(commit.log_index).await;

    // Verify all 3 rows are present — proves async buffer was drained before txn apply.
    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(
        rows, 3,
        "Expected 3 rows (2 from async buffer + 1 from txn), got {}",
        rows
    );
}

// =============================================================================
// CreateTable/DropTable Rollback Tests
// =============================================================================

/// Verify that a CreateTable within a txn is rolled back when a later op fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_rollback_undoes_create_table() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    // Transaction: create a new table, then append to a nonexistent table (will fail).
    let txn_id = leader.begin_txn();
    let schema_ipc = encode_schema(&test_schema());
    let bad_data = encode_batches(&[test_batch(&[1], &["a"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::CreateTable {
                    table: "new_table".into(),
                    schema_ipc,
                },
                TxnOp::Append {
                    table: "nonexistent".into(),
                    data: bad_data,
                },
            ],
        )
        .await
        .unwrap();

    let result = leader.txn_commit(txn_id, 1).await;
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(is_error, "commit should fail for nonexistent table");

    // The created table should have been rolled back (dropped).
    assert!(
        cluster.leader_engine().require_table("new_table").is_err(),
        "new_table should not exist after rollback"
    );
}

/// Verify that DropTable ops are deferred until after non-destructive ops succeed.
/// If a non-drop op fails, the table targeted for drop should still exist.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_rollback_preserves_table_targeted_for_drop() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    // Create a table that will be targeted for drop.
    let r = leader
        .create_table("keep_me", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Write some data so we can verify it's intact after rollback.
    let r = leader
        .write_records("keep_me", &[test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Transaction: drop keep_me + append to nonexistent (fails).
    // Because drops are deferred, the append failure should prevent the drop.
    let txn_id = leader.begin_txn();
    let bad_data = encode_batches(&[test_batch(&[99], &["bad"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![
                TxnOp::DropTable {
                    table: "keep_me".into(),
                },
                TxnOp::Append {
                    table: "nonexistent".into(),
                    data: bad_data,
                },
            ],
        )
        .await
        .unwrap();

    let result = leader.txn_commit(txn_id, 1).await;
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(is_error, "commit should fail for nonexistent table");

    // keep_me should still exist with its original data.
    assert!(
        cluster.leader_engine().require_table("keep_me").is_ok(),
        "keep_me should still exist after failed txn"
    );
    let rows = count_active_rows(cluster.leader_engine(), "keep_me").await;
    assert_eq!(rows, 2, "keep_me should retain its 2 original rows");
}

// =============================================================================
// Edge Case Tests
// =============================================================================

/// Abort of an already-committed transaction is a no-op.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_abort_after_commit_is_noop() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();
    let data = encode_batches(&[test_batch(&[1], &["a"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let commit = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit.response, LanceResponse::Ok));
    cluster.wait_for_replication(commit.log_index).await;

    // Abort the already-committed txn — should succeed (no-op).
    let abort = leader.txn_abort(txn_id).await.unwrap();
    assert!(
        matches!(abort.response, LanceResponse::Ok),
        "abort of committed txn should be Ok, got: {:?}",
        abort.response
    );

    // Data should still be there.
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 1);
}

/// Double-commit of the same txn_id returns an error.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_double_commit_returns_error() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    let txn_id = leader.begin_txn();
    let data = encode_batches(&[test_batch(&[1], &["a"])]);
    leader
        .txn_chunk(
            txn_id,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data,
            }],
        )
        .await
        .unwrap();

    let commit1 = leader.txn_commit(txn_id, 1).await.unwrap();
    assert!(matches!(commit1.response, LanceResponse::Ok));
    cluster.wait_for_replication(commit1.log_index).await;

    // Second commit should fail — txn already consumed.
    let result = leader.txn_commit(txn_id, 1).await;
    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(is_error, "second commit of same txn should fail");

    // Data from first commit should still be there.
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 1);
}

/// Two concurrent transactions targeting the same table both commit successfully.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_concurrent_same_table_both_commit() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    let r = leader.create_table("t1", &test_schema()).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Start two transactions targeting the same table.
    let txn_a = leader.begin_txn();
    let txn_b = leader.begin_txn();

    let data_a = encode_batches(&[test_batch(&[1, 2], &["a1", "a2"])]);
    let data_b = encode_batches(&[test_batch(&[3, 4, 5], &["b1", "b2", "b3"])]);

    leader
        .txn_chunk(
            txn_a,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data_a,
            }],
        )
        .await
        .unwrap();
    leader
        .txn_chunk(
            txn_b,
            0,
            vec![TxnOp::Append {
                table: "t1".into(),
                data: data_b,
            }],
        )
        .await
        .unwrap();

    // Commit both.
    let commit_a = leader.txn_commit(txn_a, 1).await.unwrap();
    assert!(matches!(commit_a.response, LanceResponse::Ok));

    let commit_b = leader.txn_commit(txn_b, 1).await.unwrap();
    assert!(matches!(commit_b.response, LanceResponse::Ok));

    cluster.wait_for_replication(commit_b.log_index).await;

    // All 5 rows should be present.
    let rows = count_active_rows(cluster.leader_engine(), "t1").await;
    assert_eq!(rows, 5, "Both txns should contribute rows, got {}", rows);
}

/// Commit with total_chunks=0 on a never-chunked txn returns "not found" error.
#[tokio::test(flavor = "multi_thread")]
async fn test_txn_commit_zero_chunks_returns_error() {
    let cluster = TestCluster::new().await;
    let leader = cluster.leader();

    // Generate a txn_id but never send any chunks.
    let txn_id = leader.begin_txn();
    let result = leader.txn_commit(txn_id, 0).await;

    let is_error = match &result {
        Err(_) => true,
        Ok(wr) => matches!(&wr.response, LanceResponse::Error(_)),
    };
    assert!(
        is_error,
        "commit with 0 chunks on unknown txn should fail, got: {:?}",
        result
    );
}
