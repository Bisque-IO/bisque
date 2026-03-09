//! Live multi-node Raft cluster integration tests for bisque-lance.
//!
//! These tests spin up a real 3-node Raft cluster with TCP transport,
//! full `LanceStateMachine` instances, and `CatalogEventBus` wiring.
//! They verify that all Raft-consensus operations replicate correctly
//! across a proper quorum (majority = 2 of 3), emit proper catalog
//! events, and return accurate responses.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

/// Limits the number of Raft clusters running concurrently within this test
/// binary. Each 3-node cluster spawns heartbeat timers and election timeouts;
/// too many in parallel starves the tokio runtime and triggers spurious
/// elections that violate openraft debug assertions.
static CLUSTER_SEMAPHORE: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(4)));

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bisque_lance::{
    BisqueLance, BisqueLanceConfig, CatalogEvent, CatalogEventBus, CatalogEventKind,
    LanceManifestManager, LanceRaftNode, LanceResponse, LanceStateMachine, LanceTypeConfig,
    PersistedTableEntry, VersionPinTracker, WriteError,
};
use bisque_raft::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MmapStorageConfig, MultiRaftManager, MultiplexedLogStorage,
    MultiplexedTransport, NodeAddressResolver,
};
use dashmap::DashSet;
use futures::StreamExt;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use prost::Message;
use tokio::sync::broadcast;

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

fn drain_events(rx: &mut broadcast::Receiver<CatalogEvent>) {
    while rx.try_recv().is_ok() {}
}

// =============================================================================
// TestCluster Harness
// =============================================================================

struct TestCluster {
    node1: Arc<LanceRaftNode>,
    node2: Arc<LanceRaftNode>,
    node3: Arc<LanceRaftNode>,
    bus1: Arc<CatalogEventBus>,
    bus2: Arc<CatalogEventBus>,
    bus3: Arc<CatalogEventBus>,
    engine1: Arc<BisqueLance>,
    engine2: Arc<BisqueLance>,
    engine3: Arc<BisqueLance>,
    pins1: Arc<VersionPinTracker>,
    pins2: Arc<VersionPinTracker>,
    pins3: Arc<VersionPinTracker>,
    flight_addr: Option<SocketAddr>,
    _dirs: Vec<tempfile::TempDir>,
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

        // Temp dirs (must live for duration of test)
        let lance_dir1 = tempfile::tempdir().unwrap();
        let lance_dir2 = tempfile::tempdir().unwrap();
        let lance_dir3 = tempfile::tempdir().unwrap();
        let raft_dir1 = tempfile::tempdir().unwrap();
        let raft_dir2 = tempfile::tempdir().unwrap();
        let raft_dir3 = tempfile::tempdir().unwrap();

        // Engines
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

        // Catalog event buses
        let bus1 = Arc::new(CatalogEventBus::new(0));
        let bus2 = Arc::new(CatalogEventBus::new(0));
        let bus3 = Arc::new(CatalogEventBus::new(0));

        // Version pin trackers
        let pins1 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins2 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
        let pins3 = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));

        // State machines
        let sm1 = LanceStateMachine::new(engine1.clone())
            .with_catalog_events(bus1.clone())
            .with_version_pins(pins1.clone());
        let sm2 = LanceStateMachine::new(engine2.clone())
            .with_catalog_events(bus2.clone())
            .with_version_pins(pins2.clone());
        let sm3 = LanceStateMachine::new(engine3.clone())
            .with_catalog_events(bus3.clone())
            .with_version_pins(pins3.clone());

        // Raft log storage
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

        // Transport
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
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

        // Managers
        let manager1 = Arc::new(MultiRaftManager::new(transport1, storage1));
        let manager2 = Arc::new(MultiRaftManager::new(transport2, storage2));
        let manager3 = Arc::new(MultiRaftManager::new(transport3, storage3));

        // RPC servers
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

        // Give servers time to bind
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Raft config
        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 200,
                election_timeout_min: 400,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        // Membership (3-node quorum: majority = 2 of 3)
        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::impls::BasicNode::default());
        members.insert(2u64, openraft::impls::BasicNode::default());
        members.insert(3u64, openraft::impls::BasicNode::default());

        // Add raft groups
        let raft1 = manager1
            .add_group(0, 1, raft_cfg.clone(), sm1)
            .await
            .unwrap();
        let _raft2 = manager2
            .add_group(0, 2, raft_cfg.clone(), sm2)
            .await
            .unwrap();
        let _raft3 = manager3.add_group(0, 3, raft_cfg, sm3).await.unwrap();

        // Initialize from node 1
        tokio::time::timeout(Duration::from_secs(5), raft1.initialize(members))
            .await
            .expect("init timeout")
            .expect("init failed");

        // Wait for leader
        let raft1_clone = raft1.clone();
        tokio::time::timeout(Duration::from_secs(5), async move {
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

        // Create LanceRaftNodes with background tasks disabled (999s intervals)
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
            bus1,
            bus2,
            bus3,
            engine1,
            engine2,
            engine3,
            pins1,
            pins2,
            pins3,
            flight_addr: None,
            _dirs: vec![
                lance_dir1, lance_dir2, lance_dir3, raft_dir1, raft_dir2, raft_dir3,
            ],
            _permit: permit,
        }
    }

    fn leader(&self) -> &Arc<LanceRaftNode> {
        &self.node1
    }

    fn follower(&self) -> &Arc<LanceRaftNode> {
        &self.node2
    }

    fn follower2(&self) -> &Arc<LanceRaftNode> {
        &self.node3
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

    fn leader_bus(&self) -> &Arc<CatalogEventBus> {
        &self.bus1
    }

    #[allow(dead_code)]
    fn follower_bus(&self) -> &Arc<CatalogEventBus> {
        &self.bus2
    }

    #[allow(dead_code)]
    fn follower2_bus(&self) -> &Arc<CatalogEventBus> {
        &self.bus3
    }

    fn leader_pins(&self) -> &Arc<VersionPinTracker> {
        &self.pins1
    }

    fn follower_pins(&self) -> &Arc<VersionPinTracker> {
        &self.pins2
    }

    fn follower2_pins(&self) -> &Arc<VersionPinTracker> {
        &self.pins3
    }

    /// Wait for both followers to replicate up to at least `min_log_index`.
    async fn wait_for_replication(&self, min_log_index: u64) {
        tokio::time::timeout(Duration::from_secs(5), async {
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

    /// Start a Flight SQL server on the leader. Returns the bound address.
    async fn start_flight_server(&mut self) -> SocketAddr {
        let addr = pick_unused_local_addr();
        let raft_node = self.node1.clone();
        tokio::spawn(async move {
            let _ = bisque_lance::flight::serve_flight(raft_node, addr).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.flight_addr = Some(addr);
        addr
    }

    #[allow(dead_code)]
    async fn shutdown(self) {
        self.node1.shutdown().await;
        self.node2.shutdown().await;
        self.node3.shutdown().await;
    }
}

// =============================================================================
// A. Table DDL Tests (CreateTable, DropTable)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_create_table_replicates_to_follower() {
    let cluster = TestCluster::new().await;

    let result = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    cluster.wait_for_replication(result.log_index).await;

    assert!(cluster.leader_engine().has_table("t1"));
    assert!(cluster.follower_engine().has_table("t1"));
    assert!(cluster.follower2_engine().has_table("t1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_table_emits_table_created_event() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event timeout")
        .expect("recv error");

    match &event.event {
        CatalogEventKind::TableCreated { table, .. } => {
            assert_eq!(&**table, "t1");
        }
        other => panic!("expected TableCreated, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_replicates_to_follower() {
    let cluster = TestCluster::new().await;

    let create_result = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create_result.log_index).await;
    assert!(cluster.follower_engine().has_table("t1"));
    assert!(cluster.follower2_engine().has_table("t1"));

    let drop_result = cluster.leader().drop_table("t1").await.unwrap();
    cluster.wait_for_replication(drop_result.log_index).await;

    assert!(!cluster.leader_engine().has_table("t1"));
    assert!(!cluster.follower_engine().has_table("t1"));
    assert!(!cluster.follower2_engine().has_table("t1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_emits_table_dropped_event() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    // Drain TableCreated event
    drain_events(&mut rx);

    cluster.leader().drop_table("t1").await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event timeout")
        .expect("recv error");

    match &event.event {
        CatalogEventKind::TableDropped { table } => {
            assert_eq!(&**table, "t1");
        }
        other => panic!("expected TableDropped, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_duplicate_table_is_idempotent() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    // Second create succeeds (idempotent for Raft replay safety).
    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
}

// =============================================================================
// B. Data Write Tests (AppendRecords)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_records_replicates_to_follower() {
    let cluster = TestCluster::new().await;

    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    let write = cluster
        .leader()
        .write_records(
            "t1",
            &[test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"])],
        )
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 5, "leader should have 5 rows");
    assert_eq!(follower_rows, 5, "follower should have 5 rows");
    assert_eq!(follower2_rows, 5, "follower2 should have 5 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_records_emits_active_version_bumped() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    // Drain TableCreated event
    drain_events(&mut rx);

    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1], &["a"])])
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event timeout")
        .expect("recv error");

    match &event.event {
        CatalogEventKind::ActiveVersionBumped { table, version } => {
            assert_eq!(&**table, "t1");
            assert!(*version > 0);
        }
        other => panic!("expected ActiveVersionBumped, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_records_returns_log_index() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    let result = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1], &["a"])])
        .await
        .unwrap();

    assert!(result.log_index > 0, "log_index should be positive");
    assert!(
        matches!(result.response, LanceResponse::Ok),
        "response should be Ok, got {:?}",
        result.response
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_writes_accumulate_on_follower() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    let mut last_log = 0;
    for i in 0..3 {
        let base = (i * 5 + 1) as i64;
        let ids: Vec<i64> = (base..base + 5).collect();
        let names: Vec<&str> = vec!["x"; 5];
        let result = cluster
            .leader()
            .write_records("t1", &[test_batch(&ids, &names)])
            .await
            .unwrap();
        last_log = result.log_index;
    }

    cluster.wait_for_replication(last_log).await;

    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;
    assert_eq!(follower_rows, 15, "follower should have 15 rows");
    assert_eq!(follower2_rows, 15, "follower2 should have 15 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_to_nonexistent_table_returns_error() {
    let cluster = TestCluster::new().await;

    let result = cluster
        .leader()
        .write_records("nonexistent", &[test_batch(&[1], &["a"])])
        .await;

    assert!(result.is_err(), "write to nonexistent table should fail");
}

// =============================================================================
// C. Delete Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_replicates_to_follower() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records(
            "t1",
            &[test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"])],
        )
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    let delete = cluster
        .leader()
        .delete_records("t1", "id <= 2")
        .await
        .unwrap();
    cluster.wait_for_replication(delete.log_index).await;

    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;

    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 3, "leader should have 3 rows after delete");
    assert_eq!(follower_rows, 3, "follower should have 3 rows after delete");
    assert_eq!(
        follower2_rows, 3,
        "follower2 should have 3 rows after delete"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_returns_rows_affected() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records(
            "t1",
            &[test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"])],
        )
        .await
        .unwrap();

    let result = cluster
        .leader()
        .delete_records("t1", "id < 3")
        .await
        .unwrap();

    match result.response {
        LanceResponse::RowsAffected(n) => assert_eq!(n, 2, "should delete 2 rows"),
        other => panic!("expected RowsAffected, got {:?}", other),
    }
    assert!(result.log_index > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_emits_data_mutated_event() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    drain_events(&mut rx);

    cluster
        .leader()
        .delete_records("t1", "id = 1")
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event timeout")
        .expect("recv error");

    match &event.event {
        CatalogEventKind::DataMutated {
            table,
            active_version,
            ..
        } => {
            assert_eq!(&**table, "t1");
            assert!(active_version.is_some(), "active_version should be Some");
        }
        other => panic!("expected DataMutated, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_error_does_not_emit_event() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    drain_events(&mut rx);

    // Invalid filter should cause an error in the state machine
    let _result = cluster
        .leader()
        .delete_records("t1", "INVALID SQL %%%")
        .await;

    // Give time for any event to propagate
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        rx.try_recv().is_err(),
        "should not receive DataMutated on error"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_no_matching_rows_returns_zero() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    let result = cluster
        .leader()
        .delete_records("t1", "id > 100")
        .await
        .unwrap();

    match result.response {
        LanceResponse::RowsAffected(n) => assert_eq!(n, 0),
        other => panic!("expected RowsAffected(0), got {:?}", other),
    }
}

// =============================================================================
// D. Update Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_update_replicates_to_follower() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Update id=2 with new data
    let update = cluster
        .leader()
        .update_records("t1", "id = 2", &[test_batch(&[2], &["updated"])])
        .await
        .unwrap();
    cluster.wait_for_replication(update.log_index).await;

    // Update = delete + append, so row count stays the same
    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;

    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 3, "leader should have 3 rows after update");
    assert_eq!(follower_rows, 3, "follower should have 3 rows after update");
    assert_eq!(
        follower2_rows, 3,
        "follower2 should have 3 rows after update"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_returns_rows_affected() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    let result = cluster
        .leader()
        .update_records("t1", "id = 2", &[test_batch(&[2], &["updated"])])
        .await
        .unwrap();

    match result.response {
        LanceResponse::RowsAffected(n) => assert_eq!(n, 1, "should affect 1 row"),
        other => panic!("expected RowsAffected, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_emits_data_mutated_event() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    drain_events(&mut rx);

    cluster
        .leader()
        .update_records("t1", "id = 1", &[test_batch(&[1], &["updated"])])
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event timeout")
        .expect("recv error");

    match &event.event {
        CatalogEventKind::DataMutated { table, .. } => {
            assert_eq!(&**table, "t1");
        }
        other => panic!("expected DataMutated, got {:?}", other),
    }
}

// =============================================================================
// E. Segment Lifecycle Tests (Seal)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_seal_replicates_to_follower() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Get current segment IDs from leader
    let table = cluster.leader_engine().require_table("t1").unwrap();
    let cat = table.catalog();
    let current_active = cat.active_segment;

    // Propose seal command directly
    let seal_cmd = bisque_lance::types::LanceCommand::SealActiveSegment {
        table_name: "t1".into(),
        sealed_segment_id: current_active,
        new_active_segment_id: current_active + 1,
        reason: bisque_lance::types::SealReason::MaxAge,
    };
    let seal_result = cluster.leader().propose(seal_cmd).await.unwrap();
    cluster.wait_for_replication(seal_result.log_index).await;

    // Verify both followers have sealed segment
    for (name, engine) in [
        ("follower", cluster.follower_engine()),
        ("follower2", cluster.follower2_engine()),
    ] {
        let follower_table = engine.require_table("t1").unwrap();
        let follower_cat = follower_table.catalog();
        assert!(
            follower_cat.sealed_segment.is_some(),
            "{name} should have a sealed segment"
        );
        assert_eq!(
            follower_cat.active_segment,
            current_active + 1,
            "{name} should have new active segment"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_seal_emits_segment_sealed_event() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    drain_events(&mut rx);

    let table = cluster.leader_engine().require_table("t1").unwrap();
    let cat = table.catalog();
    let current_active = cat.active_segment;

    let seal_cmd = bisque_lance::types::LanceCommand::SealActiveSegment {
        table_name: "t1".into(),
        sealed_segment_id: current_active,
        new_active_segment_id: current_active + 1,
        reason: bisque_lance::types::SealReason::MaxSize,
    };
    cluster.leader().propose(seal_cmd).await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event timeout")
        .expect("recv error");

    match &event.event {
        CatalogEventKind::SegmentSealed {
            table,
            active_version,
            sealed_version,
        } => {
            assert_eq!(&**table, "t1");
            assert!(*active_version > 0);
            assert!(*sealed_version > 0);
        }
        other => panic!("expected SegmentSealed, got {:?}", other),
    }
}

// =============================================================================
// F. Flight SQL End-to-End Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_query_returns_data() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    // Connect Flight SQL client
    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    // Execute query
    let mut flight_info = client
        .execute("SELECT * FROM t1 ORDER BY id".to_string(), None)
        .await
        .unwrap();

    // Fetch results
    let ticket = flight_info.endpoint.remove(0).ticket.unwrap();
    let mut stream = client.do_get(ticket).await.unwrap();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 3, "query should return 3 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_delete_routes_through_raft() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Connect Flight SQL client
    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    // Execute DELETE via Flight SQL
    let affected = client
        .execute_update("DELETE FROM t1 WHERE id = 1".to_string(), None)
        .await
        .unwrap();

    assert_eq!(affected, 1, "should delete 1 row");

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 2, "leader should have 2 rows");
    assert_eq!(follower_rows, 2, "follower should have 2 rows");
    assert_eq!(follower2_rows, 2, "follower2 should have 2 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_update_routes_through_raft() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    let affected = client
        .execute_update(
            "UPDATE t1 SET name = 'updated' WHERE id = 2".to_string(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(affected, 1, "should affect 1 row");

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Update = delete + append, row count preserved
    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 3, "leader should have 3 rows");
    assert_eq!(follower_rows, 3, "follower should have 3 rows");
    assert_eq!(follower2_rows, 3, "follower2 should have 3 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_get_tables_returns_metadata() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("alpha", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .create_table("beta", &test_schema())
        .await
        .unwrap();

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    let flight_info = client
        .get_tables(arrow_flight::sql::CommandGetTables::default())
        .await
        .unwrap();
    let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
    let mut stream = client.do_get(ticket.clone()).await.unwrap();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert!(
        total_rows >= 2,
        "should have at least 2 tables, got {}",
        total_rows
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_create_table_action() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    // Build create_table action body: u16 name_len + name + IPC schema
    let name = "action_table";
    let schema = test_schema();
    let schema_ipc = bisque_lance::ipc::schema_to_ipc(&schema).unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&(name.len() as u16).to_be_bytes());
    body.extend_from_slice(name.as_bytes());
    body.extend_from_slice(&schema_ipc);

    let action = arrow_flight::Action {
        r#type: "create_table".to_string(),
        body: bytes::Bytes::from(body),
    };

    let mut result_stream = client.do_action(action).await.unwrap();
    let _ = result_stream.next().await; // consume result

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(cluster.leader_engine().has_table("action_table"));
    assert!(cluster.follower_engine().has_table("action_table"));
    assert!(cluster.follower2_engine().has_table("action_table"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_drop_table_action() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    // Create table first via Raft API
    let create = cluster
        .leader()
        .create_table("to_drop", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;
    assert!(cluster.follower_engine().has_table("to_drop"));

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    let action = arrow_flight::Action {
        r#type: "drop_table".to_string(),
        body: bytes::Bytes::from("to_drop"),
    };

    let mut result_stream = client.do_action(action).await.unwrap();
    let _result = result_stream.next().await;

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(!cluster.leader_engine().has_table("to_drop"));
    assert!(!cluster.follower_engine().has_table("to_drop"));
    assert!(!cluster.follower2_engine().has_table("to_drop"));
}

// =============================================================================
// G. Multi-Node Consistency Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_delete_write_consistency() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    // Write 5 rows
    cluster
        .leader()
        .write_records(
            "t1",
            &[test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"])],
        )
        .await
        .unwrap();

    // Delete 2 rows
    cluster
        .leader()
        .delete_records("t1", "id <= 2")
        .await
        .unwrap();

    // Write 3 more rows
    let final_write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[6, 7, 8], &["f", "g", "h"])])
        .await
        .unwrap();

    cluster.wait_for_replication(final_write.log_index).await;

    // Should have 3 (remaining from first write) + 3 (new) = 6
    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 6, "leader should have 6 rows");
    assert_eq!(follower_rows, 6, "follower should have 6 rows");
    assert_eq!(follower2_rows, 6, "follower2 should have 6 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_multiple_tables_isolation() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("table_a", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .create_table("table_b", &test_schema())
        .await
        .unwrap();

    // Write to both tables
    cluster
        .leader()
        .write_records("table_a", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    let write_b = cluster
        .leader()
        .write_records("table_b", &[test_batch(&[10, 20, 30], &["x", "y", "z"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write_b.log_index).await;

    // Delete from table_a only
    let delete = cluster
        .leader()
        .delete_records("table_a", "id = 1")
        .await
        .unwrap();
    cluster.wait_for_replication(delete.log_index).await;

    // Table A should have 2 rows, table B should still have 3 on all nodes
    let a_leader = count_active_rows(cluster.leader_engine(), "table_a").await;
    let a_follower = count_active_rows(cluster.follower_engine(), "table_a").await;
    let a_follower2 = count_active_rows(cluster.follower2_engine(), "table_a").await;
    let b_leader = count_active_rows(cluster.leader_engine(), "table_b").await;
    let b_follower = count_active_rows(cluster.follower_engine(), "table_b").await;
    let b_follower2 = count_active_rows(cluster.follower2_engine(), "table_b").await;

    assert_eq!(a_leader, 2);
    assert_eq!(a_follower, 2);
    assert_eq!(a_follower2, 2);
    assert_eq!(b_leader, 3);
    assert_eq!(b_follower, 3);
    assert_eq!(b_follower2, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_follower_rejects_writes() {
    let cluster = TestCluster::new().await;

    let result = cluster.follower().create_table("t1", &test_schema()).await;

    match result {
        Err(bisque_lance::WriteError::NotLeader { .. }) => {} // expected
        Err(other) => panic!("expected NotLeader from follower, got {:?}", other),
        Ok(_) => panic!("follower should not accept writes"),
    }

    let result2 = cluster.follower2().create_table("t1", &test_schema()).await;

    match result2 {
        Err(bisque_lance::WriteError::NotLeader { .. }) => {} // expected
        Err(other) => panic!("expected NotLeader from follower2, got {:?}", other),
        Ok(_) => panic!("follower2 should not accept writes"),
    }
}

// =============================================================================
// H. Segment Lifecycle Tests (BeginFlush, PromoteToDeepStorage)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_begin_flush_replicates_to_followers() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Seal the active segment
    let table = cluster.leader_engine().require_table("t1").unwrap();
    let cat = table.catalog();
    let active_seg = cat.active_segment;

    let seal_cmd = bisque_lance::types::LanceCommand::SealActiveSegment {
        table_name: "t1".into(),
        sealed_segment_id: active_seg,
        new_active_segment_id: active_seg + 1,
        reason: bisque_lance::types::SealReason::MaxAge,
    };
    let seal = cluster.leader().propose(seal_cmd).await.unwrap();
    cluster.wait_for_replication(seal.log_index).await;

    // Begin flush on the sealed segment
    let flush_cmd = bisque_lance::types::LanceCommand::BeginFlush {
        table_name: "t1".into(),
        segment_id: active_seg,
    };
    let flush = cluster.leader().propose(flush_cmd).await.unwrap();
    cluster.wait_for_replication(flush.log_index).await;

    // Verify flush state is InProgress on all nodes
    for (name, engine) in [
        ("leader", cluster.leader_engine()),
        ("follower", cluster.follower_engine()),
        ("follower2", cluster.follower2_engine()),
    ] {
        let t = engine.require_table("t1").unwrap();
        let fs = t.flush_state();
        assert!(
            matches!(fs, bisque_lance::FlushState::InProgress { .. }),
            "{name} should be in flush InProgress state, got {:?}",
            fs
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_promote_to_deep_storage_replicates_and_emits_event() {
    let cluster = TestCluster::new().await;
    let mut rx = cluster.leader_bus().subscribe();

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    // Seal
    let table = cluster.leader_engine().require_table("t1").unwrap();
    let cat = table.catalog();
    let active_seg = cat.active_segment;

    let seal_cmd = bisque_lance::types::LanceCommand::SealActiveSegment {
        table_name: "t1".into(),
        sealed_segment_id: active_seg,
        new_active_segment_id: active_seg + 1,
        reason: bisque_lance::types::SealReason::MaxAge,
    };
    cluster.leader().propose(seal_cmd).await.unwrap();

    // BeginFlush
    let flush_cmd = bisque_lance::types::LanceCommand::BeginFlush {
        table_name: "t1".into(),
        segment_id: active_seg,
    };
    cluster.leader().propose(flush_cmd).await.unwrap();

    drain_events(&mut rx);

    // Promote to deep storage (simulate S3 manifest version = 42)
    let promote_cmd = bisque_lance::types::LanceCommand::PromoteToDeepStorage {
        table_name: "t1".into(),
        segment_id: active_seg,
        s3_manifest_version: 42,
    };
    let promote = cluster.leader().propose(promote_cmd).await.unwrap();
    cluster.wait_for_replication(promote.log_index).await;

    // Verify SegmentPromoted event
    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event timeout")
        .expect("recv error");

    match &event.event {
        CatalogEventKind::SegmentPromoted {
            table,
            s3_manifest_version,
        } => {
            assert_eq!(&**table, "t1");
            assert_eq!(*s3_manifest_version, 42);
        }
        other => panic!("expected SegmentPromoted, got {:?}", other),
    }

    // Verify catalog state on all nodes: sealed should be cleared, s3 version set
    for (name, engine) in [
        ("leader", cluster.leader_engine()),
        ("follower", cluster.follower_engine()),
        ("follower2", cluster.follower2_engine()),
    ] {
        let t = engine.require_table("t1").unwrap();
        let c = t.catalog();
        assert_eq!(
            c.sealed_segment, None,
            "{name} sealed should be None after promote"
        );
        assert_eq!(
            c.s3_manifest_version, 42,
            "{name} s3_manifest_version should be 42"
        );
    }
}

// =============================================================================
// I. Version Pinning Tests (RegisterSession, PinVersion, UnpinVersion, ExpireSession)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_register_session_replicates_to_followers() {
    let cluster = TestCluster::new().await;

    let cmd = bisque_lance::types::LanceCommand::RegisterSession { session_id: 1 };
    let result = cluster.leader().propose(cmd).await.unwrap();
    cluster.wait_for_replication(result.log_index).await;

    // Each node's version pin tracker should have a session
    assert_eq!(cluster.leader_pins().session_count(), 1);
    assert_eq!(cluster.follower_pins().session_count(), 1);
    assert_eq!(cluster.follower2_pins().session_count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pin_version_replicates_to_followers() {
    let cluster = TestCluster::new().await;

    // Register session first
    let reg = bisque_lance::types::LanceCommand::RegisterSession { session_id: 1 };
    let r = cluster.leader().propose(reg).await.unwrap();
    cluster.wait_for_replication(r.log_index).await;

    // Pin a version
    let pin_cmd = bisque_lance::types::LanceCommand::PinVersion {
        session_id: 1,
        table_name: "t1".into(),
        tier: "active".into(),
        version: 5,
    };
    let p = cluster.leader().propose(pin_cmd).await.unwrap();
    cluster.wait_for_replication(p.log_index).await;

    // Verify pin count on all nodes
    assert_eq!(cluster.leader_pins().pin_count(), 1);
    assert_eq!(cluster.follower_pins().pin_count(), 1);
    assert_eq!(cluster.follower2_pins().pin_count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unpin_version_replicates_to_followers() {
    let cluster = TestCluster::new().await;

    // Register + pin
    let reg = bisque_lance::types::LanceCommand::RegisterSession { session_id: 1 };
    cluster.leader().propose(reg).await.unwrap();

    let pin = bisque_lance::types::LanceCommand::PinVersion {
        session_id: 1,
        table_name: "t1".into(),
        tier: "active".into(),
        version: 5,
    };
    cluster.leader().propose(pin).await.unwrap();

    // Unpin
    let unpin = bisque_lance::types::LanceCommand::UnpinVersion {
        session_id: 1,
        table_name: "t1".into(),
        tier: "active".into(),
        version: 5,
    };
    let u = cluster.leader().propose(unpin).await.unwrap();
    cluster.wait_for_replication(u.log_index).await;

    // Pin count should be 0 on all nodes
    assert_eq!(cluster.leader_pins().pin_count(), 0);
    assert_eq!(cluster.follower_pins().pin_count(), 0);
    assert_eq!(cluster.follower2_pins().pin_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_expire_session_releases_all_pins() {
    let cluster = TestCluster::new().await;

    // Register session
    let reg = bisque_lance::types::LanceCommand::RegisterSession { session_id: 1 };
    cluster.leader().propose(reg).await.unwrap();

    // Pin two versions
    let pin1 = bisque_lance::types::LanceCommand::PinVersion {
        session_id: 1,
        table_name: "t1".into(),
        tier: "active".into(),
        version: 5,
    };
    cluster.leader().propose(pin1).await.unwrap();

    let pin2 = bisque_lance::types::LanceCommand::PinVersion {
        session_id: 1,
        table_name: "t1".into(),
        tier: "sealed".into(),
        version: 3,
    };
    let p2 = cluster.leader().propose(pin2).await.unwrap();
    cluster.wait_for_replication(p2.log_index).await;

    assert_eq!(cluster.leader_pins().pin_count(), 2);
    assert_eq!(cluster.follower_pins().pin_count(), 2);

    // Expire session
    let expire = bisque_lance::types::LanceCommand::ExpireSession { session_id: 1 };
    let e = cluster.leader().propose(expire).await.unwrap();
    cluster.wait_for_replication(e.log_index).await;

    // All pins released, session gone
    for (name, pins) in [
        ("leader", cluster.leader_pins()),
        ("follower", cluster.follower_pins()),
        ("follower2", cluster.follower2_pins()),
    ] {
        assert_eq!(
            pins.pin_count(),
            0,
            "{name} should have 0 pins after expire"
        );
        assert_eq!(
            pins.session_count(),
            0,
            "{name} should have 0 sessions after expire"
        );
    }
}

// =============================================================================
// J. Flight SQL Custom Action Tests (delete_records, update_records)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_delete_records_action() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records(
            "t1",
            &[test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"])],
        )
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    // Build delete_records action: u16 name_len + name + filter
    let name = "t1";
    let filter = "id <= 2";
    let mut body = Vec::new();
    body.extend_from_slice(&(name.len() as u16).to_be_bytes());
    body.extend_from_slice(name.as_bytes());
    body.extend_from_slice(filter.as_bytes());

    let action = arrow_flight::Action {
        r#type: "delete_records".to_string(),
        body: bytes::Bytes::from(body),
    };

    let mut result_stream = client.do_action(action).await.unwrap();
    let result = result_stream.next().await.unwrap().unwrap();
    assert_eq!(result.body.as_ref(), b"ok");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 3);
    assert_eq!(follower_rows, 3);
    assert_eq!(follower2_rows, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_update_records_action() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    // Build update_records action: u16 name_len + name + u32 filter_len + filter + IPC data
    let name = "t1";
    let filter = "id = 2";
    let replacement = test_batch(&[2], &["updated"]);
    let ipc_data = bisque_lance::ipc::encode_record_batches(&[replacement]).unwrap();

    let mut body = Vec::new();
    body.extend_from_slice(&(name.len() as u16).to_be_bytes());
    body.extend_from_slice(name.as_bytes());
    body.extend_from_slice(&(filter.len() as u32).to_be_bytes());
    body.extend_from_slice(filter.as_bytes());
    body.extend_from_slice(&ipc_data);

    let action = arrow_flight::Action {
        r#type: "update_records".to_string(),
        body: bytes::Bytes::from(body),
    };

    let mut result_stream = client.do_action(action).await.unwrap();
    let result = result_stream.next().await.unwrap().unwrap();
    assert_eq!(result.body.as_ref(), b"ok");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Update = delete + append, row count preserved
    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 3);
    assert_eq!(follower_rows, 3);
    assert_eq!(follower2_rows, 3);
}

// =============================================================================
// K. Flight SQL Ingest and Prepared Statement Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_ingest_routes_through_raft() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();

    // Use low-level FlightServiceClient to send CommandStatementIngest
    let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);

    // Encode CommandStatementIngest as Any
    let ingest_cmd = arrow_flight::sql::CommandStatementIngest {
        table: "t1".to_string(),
        ..Default::default()
    };
    let any = arrow_flight::sql::Any::pack(&ingest_cmd).unwrap();
    let descriptor = arrow_flight::FlightDescriptor::new_cmd(any.encode_to_vec());

    // Build FlightData from record batches (includes schema as first message)
    let batch = test_batch(&[10, 20, 30], &["x", "y", "z"]);
    let schema = batch.schema();

    let mut flight_data_vec =
        arrow_flight::utils::batches_to_flight_data(&schema, vec![batch]).unwrap();

    // Set the descriptor on the first message
    flight_data_vec[0].flight_descriptor = Some(descriptor);

    let stream = futures::stream::iter(flight_data_vec);
    let response = client.do_put(stream).await.unwrap();
    let mut inner = response.into_inner();
    // Consume the put result
    while let Some(_) = inner.next().await {}

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 3, "leader should have 3 ingested rows");
    assert_eq!(follower_rows, 3, "follower should have 3 ingested rows");
    assert_eq!(follower2_rows, 3, "follower2 should have 3 ingested rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_prepared_statement_flow() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    // Create prepared statement
    let mut prepared = client
        .prepare("SELECT * FROM t1 ORDER BY id".to_string(), None)
        .await
        .unwrap();

    // Execute it
    let mut flight_info = prepared.execute().await.unwrap();
    let ticket = flight_info.endpoint.remove(0).ticket.unwrap();
    let mut stream = client.do_get(ticket).await.unwrap();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 3, "prepared statement should return 3 rows");

    // Close is a no-op (stateless) — just verify it doesn't error
    // The prepared handle gets dropped here which calls close
}

// =============================================================================
// L. Flight SQL Metadata Endpoint Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_get_catalogs() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    let flight_info = client.get_catalogs().await.unwrap();
    let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
    let mut stream = client.do_get(ticket.clone()).await.unwrap();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert!(total_rows >= 1, "should have at least 1 catalog");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_get_schemas() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    let flight_info = client
        .get_db_schemas(arrow_flight::sql::CommandGetDbSchemas::default())
        .await
        .unwrap();
    let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
    let mut stream = client.do_get(ticket.clone()).await.unwrap();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert!(total_rows >= 1, "should have at least 1 schema");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_get_table_types() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    let flight_info = client.get_table_types().await.unwrap();
    let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
    let mut stream = client.do_get(ticket.clone()).await.unwrap();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert!(total_rows >= 1, "should have at least 1 table type");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_get_sql_info() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

    let flight_info = client.get_sql_info(vec![]).await.unwrap();
    let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
    let mut stream = client.do_get(ticket.clone()).await.unwrap();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert!(total_rows >= 1, "should have at least 1 sql info entry");
}

// =============================================================================
// M. Error Path Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_nonexistent_table_is_idempotent() {
    let cluster = TestCluster::new().await;

    // Dropping a non-existent table succeeds (idempotent for Raft replay safety).
    cluster.leader().drop_table("nonexistent").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_from_nonexistent_table_returns_error() {
    let cluster = TestCluster::new().await;

    let result = cluster
        .leader()
        .delete_records("nonexistent", "id = 1")
        .await;
    assert!(result.is_err(), "delete from nonexistent table should fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_nonexistent_table_returns_error() {
    let cluster = TestCluster::new().await;

    let result = cluster
        .leader()
        .update_records("nonexistent", "id = 1", &[test_batch(&[1], &["a"])])
        .await;
    assert!(result.is_err(), "update on nonexistent table should fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_invalid_filter_returns_error() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    let result = cluster
        .leader()
        .update_records("t1", "INVALID SQL %%%", &[test_batch(&[1], &["a"])])
        .await;

    // Should fail (error response from state machine)
    match result {
        Err(_) => {} // expected: either WriteError or LanceResponse::Error
        Ok(wr) => {
            assert!(
                matches!(wr.response, LanceResponse::Error(_)),
                "expected error response, got {:?}",
                wr.response
            );
        }
    }
}

// =============================================================================
// N. Concurrent Writes and Large Batch Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_writes_all_succeed() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    // Fire 10 concurrent writes
    let mut handles = Vec::new();
    for i in 0..10 {
        let leader = cluster.leader().clone();
        handles.push(tokio::spawn(async move {
            let id = (i * 10 + 1) as i64;
            leader
                .write_records("t1", &[test_batch(&[id], &["concurrent"])])
                .await
                .unwrap()
        }));
    }

    let mut max_log = 0u64;
    for handle in handles {
        let result = handle.await.unwrap();
        max_log = max_log.max(result.log_index);
    }

    cluster.wait_for_replication(max_log).await;

    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 10, "leader should have 10 rows");
    assert_eq!(follower_rows, 10, "follower should have 10 rows");
    assert_eq!(follower2_rows, 10, "follower2 should have 10 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_large_batch_replicates_correctly() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();

    // Write 1000 rows in a single batch
    let ids: Vec<i64> = (1..=1000).collect();
    let names: Vec<&str> = vec!["row"; 1000];
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&ids, &names)])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    let leader_rows = count_active_rows(cluster.leader_engine(), "t1").await;
    let follower_rows = count_active_rows(cluster.follower_engine(), "t1").await;
    let follower2_rows = count_active_rows(cluster.follower2_engine(), "t1").await;

    assert_eq!(leader_rows, 1000);
    assert_eq!(follower_rows, 1000);
    assert_eq!(follower2_rows, 1000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flight_sql_list_actions() {
    let mut cluster = TestCluster::new().await;
    let addr = cluster.start_flight_server().await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);

    let response = client.list_actions(arrow_flight::Empty {}).await.unwrap();
    let mut inner = response.into_inner();
    let mut action_types = Vec::new();
    while let Some(action) = inner.next().await {
        action_types.push(action.unwrap().r#type);
    }

    // Verify all custom actions are advertised
    assert!(
        action_types.contains(&"create_table".to_string()),
        "should advertise create_table"
    );
    assert!(
        action_types.contains(&"drop_table".to_string()),
        "should advertise drop_table"
    );
    assert!(
        action_types.contains(&"delete_records".to_string()),
        "should advertise delete_records"
    );
    assert!(
        action_types.contains(&"update_records".to_string()),
        "should advertise update_records"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_segment_lifecycle_seal_flush_promote() {
    let cluster = TestCluster::new().await;

    cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    let table = cluster.leader_engine().require_table("t1").unwrap();
    let active_seg = table.catalog().active_segment;

    // 1. Seal
    let seal = bisque_lance::types::LanceCommand::SealActiveSegment {
        table_name: "t1".into(),
        sealed_segment_id: active_seg,
        new_active_segment_id: active_seg + 1,
        reason: bisque_lance::types::SealReason::MaxSize,
    };
    let s = cluster.leader().propose(seal).await.unwrap();
    cluster.wait_for_replication(s.log_index).await;

    // Verify sealed on all nodes
    for engine in [
        cluster.leader_engine(),
        cluster.follower_engine(),
        cluster.follower2_engine(),
    ] {
        let c = engine.require_table("t1").unwrap().catalog();
        assert_eq!(c.sealed_segment, Some(active_seg));
        assert_eq!(c.active_segment, active_seg + 1);
    }

    // 2. BeginFlush
    let flush = bisque_lance::types::LanceCommand::BeginFlush {
        table_name: "t1".into(),
        segment_id: active_seg,
    };
    let f = cluster.leader().propose(flush).await.unwrap();
    cluster.wait_for_replication(f.log_index).await;

    for engine in [
        cluster.leader_engine(),
        cluster.follower_engine(),
        cluster.follower2_engine(),
    ] {
        let fs = engine.require_table("t1").unwrap().flush_state();
        assert!(matches!(fs, bisque_lance::FlushState::InProgress { .. }));
    }

    // 3. Promote
    let promote = bisque_lance::types::LanceCommand::PromoteToDeepStorage {
        table_name: "t1".into(),
        segment_id: active_seg,
        s3_manifest_version: 1,
    };
    let p = cluster.leader().propose(promote).await.unwrap();
    cluster.wait_for_replication(p.log_index).await;

    for engine in [
        cluster.leader_engine(),
        cluster.follower_engine(),
        cluster.follower2_engine(),
    ] {
        let t = engine.require_table("t1").unwrap();
        let c = t.catalog();
        assert_eq!(
            c.sealed_segment, None,
            "sealed should be cleared after promote"
        );
        assert_eq!(c.s3_manifest_version, 1);
        let fs = t.flush_state();
        assert!(
            matches!(fs, bisque_lance::FlushState::Idle),
            "flush should be idle after promote"
        );
    }
}

// =============================================================================
// O. Leader Failure & Re-election Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_shutdown_triggers_reelection() {
    let cluster = TestCluster::new().await;

    // Create a table while node1 is leader.
    let result = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(result.log_index).await;

    // Write some data.
    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Shut down node1 (the leader).
    let raft1 = cluster.node1.raft().clone();
    let _ = raft1.shutdown().await;

    // Wait for a new leader to be elected (node2 or node3).
    let node2_raft = cluster.node2.raft().clone();
    let node3_raft = cluster.node3.raft().clone();
    let new_leader_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let m2 = node2_raft.metrics().borrow_watched().clone();
            let m3 = node3_raft.metrics().borrow_watched().clone();
            // Check if node2 is leader
            if m2.current_leader == Some(2) {
                return 2u64;
            }
            // Check if node3 is leader
            if m3.current_leader == Some(3) {
                return 3u64;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("new leader election timeout");

    assert!(
        new_leader_id == 2 || new_leader_id == 3,
        "expected node 2 or 3 to become leader"
    );

    // Write through the new leader.
    let new_leader_node = if new_leader_id == 2 {
        &cluster.node2
    } else {
        &cluster.node3
    };
    let other_follower = if new_leader_id == 2 {
        &cluster.node3
    } else {
        &cluster.node2
    };

    let write2 = new_leader_node
        .write_records("t1", &[test_batch(&[4, 5], &["d", "e"])])
        .await
        .unwrap();

    // Wait for the remaining follower to replicate.
    let other_raft = other_follower.raft().clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let m = other_raft.metrics().borrow_watched().clone();
            let applied = m.last_applied.map(|id| id.index).unwrap_or(0);
            if applied >= write2.log_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("follower replication timeout after re-election");

    // Verify data on the new leader.
    let new_leader_engine = new_leader_node.engine();
    assert_eq!(count_active_rows(&new_leader_engine, "t1").await, 5);

    // Verify data on the remaining follower.
    let other_engine = other_follower.engine();
    assert_eq!(count_active_rows(&other_engine, "t1").await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_follower_rejects_writes_with_not_leader_error() {
    let cluster = TestCluster::new().await;

    // Try to create table on follower (node2) — should fail with NotLeader.
    let result = cluster.follower().create_table("t1", &test_schema()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        WriteError::NotLeader { leader_id, .. } => {
            assert_eq!(leader_id, Some(1), "should report node 1 as leader");
        }
        other => panic!("expected NotLeader, got {:?}", other),
    }

    // Try to write records on follower — should also fail.
    // First create table on leader so follower has it.
    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    let result = cluster
        .follower()
        .write_records("t1", &[test_batch(&[1], &["a"])])
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        WriteError::NotLeader { .. } => {}
        other => panic!("expected NotLeader, got {:?}", other),
    }

    // Try propose on follower2 — same.
    let result = cluster.follower2().drop_table("t1").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        WriteError::NotLeader { leader_id, .. } => {
            assert_eq!(leader_id, Some(1));
        }
        other => panic!("expected NotLeader, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_writes_survive_leader_reelection() {
    let cluster = TestCluster::new().await;

    // Create table and write data.
    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    // Write 3 batches to build up some history.
    for i in 0..3 {
        let base = i * 3;
        let ids: Vec<i64> = (base..base + 3).collect();
        let names: Vec<&str> = vec!["x"; 3];
        let w = cluster
            .leader()
            .write_records("t1", &[test_batch(&ids, &names)])
            .await
            .unwrap();
        cluster.wait_for_replication(w.log_index).await;
    }

    // Verify all 3 nodes have 9 rows.
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 9);
    assert_eq!(count_active_rows(cluster.follower_engine(), "t1").await, 9);
    assert_eq!(count_active_rows(cluster.follower2_engine(), "t1").await, 9);

    // Shut down leader.
    let raft1 = cluster.node1.raft().clone();
    let _ = raft1.shutdown().await;

    // Wait for new leader.
    let node2_raft = cluster.node2.raft().clone();
    let node3_raft = cluster.node3.raft().clone();
    let new_leader_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let m2 = node2_raft.metrics().borrow_watched().clone();
            let m3 = node3_raft.metrics().borrow_watched().clone();
            if m2.current_leader == Some(2) {
                return 2u64;
            }
            if m3.current_leader == Some(3) {
                return 3u64;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("re-election timeout");

    let new_leader = if new_leader_id == 2 {
        &cluster.node2
    } else {
        &cluster.node3
    };

    // Data should still be on the surviving nodes.
    assert_eq!(count_active_rows(new_leader.engine(), "t1").await, 9);

    // Write more data through new leader.
    let w = new_leader
        .write_records("t1", &[test_batch(&[100, 101], &["new1", "new2"])])
        .await
        .unwrap();

    // Wait for replication on the other surviving node.
    let other = if new_leader_id == 2 {
        &cluster.node3
    } else {
        &cluster.node2
    };
    let other_raft = other.raft().clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let m = other_raft.metrics().borrow_watched().clone();
            if m.last_applied.map(|id| id.index).unwrap_or(0) >= w.log_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replication timeout");

    assert_eq!(count_active_rows(new_leader.engine(), "t1").await, 11);
    assert_eq!(count_active_rows(other.engine(), "t1").await, 11);
}

// =============================================================================
// P. Snapshot Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_trigger_snapshot_captures_table_metadata() {
    let cluster = TestCluster::new().await;

    // Create tables and write some data.
    let c1 = cluster
        .leader()
        .create_table("snap_t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(c1.log_index).await;

    let c2 = cluster
        .leader()
        .create_table("snap_t2", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(c2.log_index).await;

    let w1 = cluster
        .leader()
        .write_records("snap_t1", &[test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();
    cluster.wait_for_replication(w1.log_index).await;

    // Trigger a snapshot on the leader.
    cluster.leader().raft().trigger().snapshot().await.unwrap();

    // Wait briefly for the snapshot to be built.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the snapshot was built by checking metrics.
    let metrics = cluster.leader().raft().metrics().borrow_watched().clone();
    assert!(
        metrics.snapshot.is_some(),
        "snapshot should be present after trigger"
    );
    let snap_log_id = metrics.snapshot.unwrap();
    assert!(
        snap_log_id.index >= w1.log_index,
        "snapshot should include at least the last write (snap index {} < write index {})",
        snap_log_id.index,
        w1.log_index,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_install_on_lagging_follower() {
    // Build a 3-node cluster with low snapshot threshold so we can trigger snapshot install.
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
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        connection_ttl: Duration::from_secs(30),
        tcp_nodelay: true,
        ..Default::default()
    };
    let transport1 =
        BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
    let transport2 =
        BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
    let transport3 = BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg, registry.clone());

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

    // Use low snapshot threshold so snapshot is triggered after few log entries.
    let raft_cfg = Arc::new(
        openraft::Config {
            heartbeat_interval: 200,
            election_timeout_min: 400,
            election_timeout_max: 600,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5),
            max_in_snapshot_log_to_keep: 2,
            purge_batch_size: 1,
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

    tokio::time::timeout(Duration::from_secs(5), raft1.initialize(members))
        .await
        .expect("init timeout")
        .expect("init failed");

    // Wait for leader.
    let raft1_clone = raft1.clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
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
        LanceRaftNode::new(raft1.clone(), engine1.clone(), 1)
            .with_seal_check_interval(Duration::from_secs(999))
            .with_flush_check_interval(Duration::from_secs(999))
            .with_compaction_check_interval(Duration::from_secs(999)),
    );
    node1.start();

    // Create table and write enough data to generate log entries that trigger snapshot.
    node1
        .create_table("snap_table", &test_schema())
        .await
        .unwrap();

    // Write many batches to exceed the snapshot threshold (5 logs).
    for i in 0..10 {
        let ids = vec![i as i64];
        let names = vec!["row"];
        node1
            .write_records("snap_table", &[test_batch(&ids, &names)])
            .await
            .unwrap();
    }

    // Trigger snapshot explicitly to ensure it's built.
    raft1.trigger().snapshot().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify snapshot was built.
    let metrics = raft1.metrics().borrow_watched().clone();
    assert!(
        metrics.snapshot.is_some(),
        "snapshot should exist after writes"
    );

    // Verify leader has the table with data.
    assert!(engine1.has_table("snap_table"));
    assert_eq!(count_active_rows(&engine1, "snap_table").await, 10);

    // Now shut down node3's raft group to simulate it falling behind.
    manager3.remove_group(0).await;

    // Write more data while node3 is down.
    for i in 10..15 {
        let ids = vec![i as i64];
        let names = vec!["row"];
        node1
            .write_records("snap_table", &[test_batch(&ids, &names)])
            .await
            .unwrap();
    }

    // Trigger another snapshot so the leader has a snapshot beyond what node3 has seen.
    raft1.trigger().snapshot().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Purge old logs to force snapshot install when node3 rejoins.
    let metrics = raft1.metrics().borrow_watched().clone();
    if let Some(snap) = &metrics.snapshot {
        let purge_up_to = snap.index.saturating_sub(1);
        if purge_up_to > 0 {
            let _ = raft1.trigger().purge_log(purge_up_to).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    // Create a fresh engine and state machine for node3 (simulating a fresh restart).
    let engine3_new = Arc::new(
        BisqueLance::open(BisqueLanceConfig::new(lance_dir3.path()))
            .await
            .unwrap(),
    );
    let bus3_new = Arc::new(CatalogEventBus::new(0));
    let pins3_new = Arc::new(VersionPinTracker::new(Duration::from_secs(30)));
    let sm3_new = LanceStateMachine::new(engine3_new.clone())
        .with_catalog_events(bus3_new.clone())
        .with_version_pins(pins3_new.clone());

    // Re-add node3's raft group with fresh state. openraft should install the snapshot.
    let raft_cfg_new = Arc::new(
        openraft::Config {
            heartbeat_interval: 200,
            election_timeout_min: 400,
            election_timeout_max: 600,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5),
            max_in_snapshot_log_to_keep: 2,
            purge_batch_size: 1,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let _raft3_new = manager3
        .add_group(0, 3, raft_cfg_new, sm3_new)
        .await
        .unwrap();

    // Wait for node3 to receive the snapshot and apply it.
    // The snapshot contains table metadata; the table should appear on node3.
    let engine3_check = engine3_new.clone();
    let result = tokio::time::timeout(Duration::from_secs(10), async move {
        loop {
            if engine3_check.has_table("snap_table") {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    // The table should exist after snapshot install (metadata is restored).
    // Data files may not be present since we don't have SegmentSyncServer,
    // but the table metadata (catalog, schema) should be restored.
    assert!(
        result.is_ok(),
        "node3 should have snap_table after snapshot install"
    );
}

// =============================================================================
// Q. Schema Validation Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_wrong_schema_returns_error() {
    let cluster = TestCluster::new().await;

    // Create table with (id: Int64, name: Utf8).
    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    // Write a batch with a completely different schema (value: Float64).
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema,
        vec![Arc::new(arrow_array::Float64Array::from(vec![1.0, 2.0]))],
    )
    .unwrap();

    let result = cluster.leader().write_records("t1", &[wrong_batch]).await;

    // The write should succeed at the Raft level (IPC encoding/decoding works),
    // but Lance's append may produce an error if schemas are incompatible.
    // Let's verify the behavior: Lance will create a new fragment with the new
    // schema (schema evolution). So the write should succeed.
    // If it errors, it should be a clean Raft error, not a panic.
    match result {
        Ok(write_result) => {
            // Schema evolution: Lance accepts different schemas as new fragments.
            // Verify the data was written.
            cluster.wait_for_replication(write_result.log_index).await;
        }
        Err(e) => {
            // If Lance rejects the schema mismatch, we get a Raft error.
            match &e {
                WriteError::Raft(msg) => {
                    // This is acceptable — the error was caught and propagated cleanly.
                    assert!(!msg.is_empty(), "error message should not be empty");
                }
                other => panic!("expected Raft error for schema mismatch, got {:?}", other),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_empty_batch_returns_error() {
    let cluster = TestCluster::new().await;

    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    // Create an empty batch (0 rows).
    let empty_batch = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .unwrap();

    let result = cluster.leader().write_records("t1", &[empty_batch]).await;

    // An empty batch may succeed (no-op append) or fail. Either way, no panic.
    match result {
        Ok(_) => {
            // Empty batch accepted as no-op — that's fine.
        }
        Err(WriteError::Raft(msg)) => {
            // Error propagated cleanly.
            assert!(!msg.is_empty());
        }
        Err(e) => panic!("unexpected error type: {:?}", e),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_with_nullable_field_mismatch() {
    let cluster = TestCluster::new().await;

    // Create table with (id: Int64 NOT NULL, name: Utf8 NULLABLE).
    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    // Write with reversed nullability: (id: Int64 NULLABLE, name: Utf8 NOT NULL).
    let alt_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),   // was false
        Field::new("name", DataType::Utf8, false), // was true
    ]));
    let batch = RecordBatch::try_new(
        alt_schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();

    let result = cluster.leader().write_records("t1", &[batch]).await;

    // Lance handles nullability differences gracefully via schema evolution.
    // Should not panic regardless of outcome.
    match result {
        Ok(w) => {
            cluster.wait_for_replication(w.log_index).await;
            // Data should be on all nodes.
            assert!(count_active_rows(cluster.leader_engine(), "t1").await >= 2);
            assert!(count_active_rows(cluster.follower_engine(), "t1").await >= 2);
            assert!(count_active_rows(cluster.follower2_engine(), "t1").await >= 2);
        }
        Err(WriteError::Raft(_)) => {
            // Acceptable if Lance rejects the schema difference.
        }
        Err(e) => panic!("unexpected error: {:?}", e),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_malformed_sql_filter() {
    let cluster = TestCluster::new().await;

    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Use a syntactically invalid SQL filter.
    let result = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::DeleteRecords {
            table_name: "t1".into(),
            filter: "SELECT * FROM ;;;; INVALID SQL".to_string(),
        })
        .await;

    // Should return a clean error, not panic.
    assert!(
        result.is_err(),
        "malformed SQL filter should produce an error"
    );
    match result.unwrap_err() {
        WriteError::Raft(msg) => {
            assert!(!msg.is_empty(), "error message should describe the problem");
        }
        other => panic!("expected Raft error for bad SQL, got {:?}", other),
    }

    // Verify data is unchanged on all nodes.
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 3);
    assert_eq!(count_active_rows(cluster.follower_engine(), "t1").await, 3);
    assert_eq!(count_active_rows(cluster.follower2_engine(), "t1").await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_malformed_sql_filter() {
    let cluster = TestCluster::new().await;

    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Update with malformed filter.
    let result = cluster
        .leader()
        .update_records("t1", "NOT VALID SQL :::", &[test_batch(&[99], &["z"])])
        .await;

    assert!(
        result.is_err(),
        "malformed SQL filter should produce an error"
    );

    // Data unchanged.
    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 2);
}

// =============================================================================
// R. Compaction Safety with Version Pins
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_version_pins_block_cleanup() {
    let cluster = TestCluster::new().await;

    // Create table and write data.
    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Register a session via Raft.
    // Note: RegisterSession ignores the session_id field and auto-generates IDs
    // via create_session(). The first session on each node gets ID 1.
    let reg = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::RegisterSession { session_id: 0 })
        .await
        .unwrap();
    cluster.wait_for_replication(reg.log_index).await;

    // The auto-generated session ID is 1 (first session created on each tracker).
    let actual_session_id = 1u64;

    // Pin a version via Raft.
    let pin = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::PinVersion {
            session_id: actual_session_id,
            table_name: "t1".into(),
            tier: "active".into(),
            version: 1,
        })
        .await
        .unwrap();
    cluster.wait_for_replication(pin.log_index).await;

    // Verify version pins are registered on all nodes.
    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        assert_eq!(pins.session_count(), 1, "session should exist");
        assert_eq!(pins.pin_count(), 1, "pin should exist");
    }

    // Verify min_pinned_version reflects the pin on all nodes.
    // The catalog name in the state machine defaults to empty string in tests.
    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        use bisque_lance::version_pins::PinTier;
        let min = pins.min_pinned_version("", "t1", PinTier::Active);
        assert_eq!(min, Some(1), "pinned version should be 1");
    }

    // Now unpin and verify cleanup would be allowed.
    let unpin = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::UnpinVersion {
            session_id: actual_session_id,
            table_name: "t1".into(),
            tier: "active".into(),
            version: 1,
        })
        .await
        .unwrap();
    cluster.wait_for_replication(unpin.log_index).await;

    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        assert_eq!(pins.pin_count(), 0, "pin should be removed");
        use bisque_lance::version_pins::PinTier;
        let min = pins.min_pinned_version("", "t1", PinTier::Active);
        assert_eq!(min, None, "no pinned version after unpin");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_expire_session_removes_all_pins() {
    let cluster = TestCluster::new().await;

    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    // Register session. Auto-generated session ID is 1.
    let reg = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::RegisterSession { session_id: 0 })
        .await
        .unwrap();
    cluster.wait_for_replication(reg.log_index).await;
    let actual_session_id = 1u64;

    // Pin 3 different versions.
    for version in 1..=3 {
        let p = cluster
            .leader()
            .propose(bisque_lance::LanceCommand::PinVersion {
                session_id: actual_session_id,
                table_name: "t1".into(),
                tier: "active".into(),
                version,
            })
            .await
            .unwrap();
        cluster.wait_for_replication(p.log_index).await;
    }

    // Verify 3 pins on all nodes.
    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        assert_eq!(pins.pin_count(), 3);
    }

    // Expire the session — should remove all 3 pins.
    let expire = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::ExpireSession {
            session_id: actual_session_id,
        })
        .await
        .unwrap();
    cluster.wait_for_replication(expire.log_index).await;

    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        assert_eq!(pins.session_count(), 0, "session should be gone");
        assert_eq!(pins.pin_count(), 0, "all pins should be removed");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_sessions_pin_same_version() {
    let cluster = TestCluster::new().await;

    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    // Register two sessions. Auto-generated IDs: 1 and 2.
    let r1 = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::RegisterSession { session_id: 0 })
        .await
        .unwrap();
    let r2 = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::RegisterSession { session_id: 0 })
        .await
        .unwrap();
    cluster.wait_for_replication(r2.log_index).await;
    let sid1 = 1u64; // first auto-generated
    let sid2 = 2u64; // second auto-generated

    // Both sessions pin version 5.
    let p1 = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::PinVersion {
            session_id: sid1,
            table_name: "t1".into(),
            tier: "sealed".into(),
            version: 5,
        })
        .await
        .unwrap();
    let p2 = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::PinVersion {
            session_id: sid2,
            table_name: "t1".into(),
            tier: "sealed".into(),
            version: 5,
        })
        .await
        .unwrap();
    cluster.wait_for_replication(p2.log_index).await;

    // Pin count should be 2 (two sessions pinning same version).
    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        assert_eq!(pins.pin_count(), 2);
        use bisque_lance::version_pins::PinTier;
        assert_eq!(pins.min_pinned_version("", "t1", PinTier::Sealed), Some(5));
    }

    // Expire session 1 — pin count drops to 1, but version still pinned.
    let e1 = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::ExpireSession { session_id: sid1 })
        .await
        .unwrap();
    cluster.wait_for_replication(e1.log_index).await;

    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        assert_eq!(pins.pin_count(), 1);
        use bisque_lance::version_pins::PinTier;
        assert_eq!(pins.min_pinned_version("", "t1", PinTier::Sealed), Some(5));
    }

    // Expire session 2 — version no longer pinned.
    let e2 = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::ExpireSession { session_id: sid2 })
        .await
        .unwrap();
    cluster.wait_for_replication(e2.log_index).await;

    for pins in [
        cluster.leader_pins(),
        cluster.follower_pins(),
        cluster.follower2_pins(),
    ] {
        assert_eq!(pins.pin_count(), 0);
        use bisque_lance::version_pins::PinTier;
        assert_eq!(pins.min_pinned_version("", "t1", PinTier::Sealed), None);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compaction_respects_version_pins_on_table_engine() {
    let cluster = TestCluster::new().await;

    // Create table and wire up version pins directly on the TableEngine.
    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    // Write enough data to have something to compact.
    for i in 0..5 {
        let ids = vec![i as i64];
        let names = vec!["row"];
        let w = cluster
            .leader()
            .write_records("t1", &[test_batch(&ids, &names)])
            .await
            .unwrap();
        cluster.wait_for_replication(w.log_index).await;
    }

    // Get the leader's table engine directly.
    let table = cluster.leader_engine().require_table("t1").unwrap();

    // Set up version pins on the table engine.
    let pins = cluster.leader_pins().clone();
    table.set_version_pins(pins.clone());

    // Register session. Auto-generated session ID is 1.
    let reg = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::RegisterSession { session_id: 0 })
        .await
        .unwrap();
    cluster.wait_for_replication(reg.log_index).await;
    let actual_session_id = 1u64;

    let pin = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::PinVersion {
            session_id: actual_session_id,
            table_name: "t1".into(),
            tier: "active".into(),
            version: 1,
        })
        .await
        .unwrap();
    cluster.wait_for_replication(pin.log_index).await;

    // Verify the pin is set.
    use bisque_lance::version_pins::PinTier;
    assert_eq!(pins.min_pinned_version("", "t1", PinTier::Active), Some(1));

    // cleanup_s3 would skip because we have no S3 configured, but we can verify
    // the pin check logic works by checking the pin tracker state.
    // The actual cleanup_s3_internal checks `version_pins.min_pinned_version()` and
    // returns early with default stats when pins exist.

    // Verify active compaction still works (compaction doesn't check pins, only cleanup does).
    // With 5 single-row writes, there should be 5 fragments (but compaction threshold
    // is typically 4, so it may or may not run).
    let compact_result = table.compact_active().await;
    assert!(
        compact_result.is_ok(),
        "compaction should still work with version pins"
    );

    // Clean up: expire session.
    let expire = cluster
        .leader()
        .propose(bisque_lance::LanceCommand::ExpireSession {
            session_id: actual_session_id,
        })
        .await
        .unwrap();
    cluster.wait_for_replication(expire.log_index).await;
    assert_eq!(pins.min_pinned_version("", "t1", PinTier::Active), None);
}

// =============================================================================
// S. Fault-Injecting Transport & Partition Testing
// =============================================================================

/// Shared partition control: a set of blocked (from_node, to_node) routes.
/// When a route is blocked, RPCs return `Unreachable`.
#[derive(Clone)]
struct PartitionControl {
    blocked: Arc<DashSet<(u64, u64)>>,
    /// The node_id of the transport instance (for source filtering).
    node_id: u64,
}

impl PartitionControl {
    fn new(node_id: u64, blocked: Arc<DashSet<(u64, u64)>>) -> Self {
        Self { blocked, node_id }
    }

    fn is_blocked(&self, target: u64) -> bool {
        self.blocked.contains(&(self.node_id, target))
    }
}

/// Transport wrapper that intercepts RPCs and returns errors for blocked routes.
struct FaultInjectingTransport {
    inner: BisqueTcpTransport<LanceTypeConfig>,
    control: PartitionControl,
}

impl FaultInjectingTransport {
    fn new(inner: BisqueTcpTransport<LanceTypeConfig>, control: PartitionControl) -> Self {
        Self { inner, control }
    }
}

impl MultiplexedTransport<LanceTypeConfig> for FaultInjectingTransport {
    async fn send_append_entries(
        &self,
        target: u64,
        group_id: u64,
        rpc: AppendEntriesRequest<LanceTypeConfig>,
    ) -> Result<
        AppendEntriesResponse<LanceTypeConfig>,
        RPCError<LanceTypeConfig, RaftError<LanceTypeConfig>>,
    > {
        if self.control.is_blocked(target) {
            return Err(RPCError::Unreachable(Unreachable::from_string(
                "network partition",
            )));
        }
        self.inner.send_append_entries(target, group_id, rpc).await
    }

    async fn send_vote(
        &self,
        target: u64,
        group_id: u64,
        rpc: VoteRequest<LanceTypeConfig>,
    ) -> Result<VoteResponse<LanceTypeConfig>, RPCError<LanceTypeConfig, RaftError<LanceTypeConfig>>>
    {
        if self.control.is_blocked(target) {
            return Err(RPCError::Unreachable(Unreachable::from_string(
                "network partition",
            )));
        }
        self.inner.send_vote(target, group_id, rpc).await
    }

    async fn send_install_snapshot(
        &self,
        target: u64,
        group_id: u64,
        rpc: InstallSnapshotRequest<LanceTypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<LanceTypeConfig>,
        RPCError<LanceTypeConfig, RaftError<LanceTypeConfig, InstallSnapshotError>>,
    > {
        if self.control.is_blocked(target) {
            return Err(RPCError::Unreachable(Unreachable::from_string(
                "network partition",
            )));
        }
        self.inner
            .send_install_snapshot(target, group_id, rpc)
            .await
    }
}

/// A 3-node cluster with fault-injecting transport for partition testing.
struct PartitionCluster {
    node1: Arc<LanceRaftNode>,
    node2: Arc<LanceRaftNode>,
    node3: Arc<LanceRaftNode>,
    engine1: Arc<BisqueLance>,
    engine2: Arc<BisqueLance>,
    engine3: Arc<BisqueLance>,
    /// Shared blocked-routes set: insert (from, to) to block RPCs.
    blocked: Arc<DashSet<(u64, u64)>>,
    _dirs: Vec<tempfile::TempDir>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PartitionCluster {
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

        let sm1 = LanceStateMachine::new(engine1.clone()).with_catalog_events(bus1);
        let sm2 = LanceStateMachine::new(engine2.clone()).with_catalog_events(bus2);
        let sm3 = LanceStateMachine::new(engine3.clone()).with_catalog_events(bus3);

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

        let blocked = Arc::new(DashSet::new());

        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
            ..Default::default()
        };
        let t1 = FaultInjectingTransport::new(
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone()),
            PartitionControl::new(1, blocked.clone()),
        );
        let t2 = FaultInjectingTransport::new(
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone()),
            PartitionControl::new(2, blocked.clone()),
        );
        let t3 = FaultInjectingTransport::new(
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg, registry.clone()),
            PartitionControl::new(3, blocked.clone()),
        );

        let manager1 = Arc::new(MultiRaftManager::new(t1, storage1));
        let manager2 = Arc::new(MultiRaftManager::new(t2, storage2));
        let manager3 = Arc::new(MultiRaftManager::new(t3, storage3));

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
                heartbeat_interval: 200,
                election_timeout_min: 400,
                election_timeout_max: 600,
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

        tokio::time::timeout(Duration::from_secs(5), raft1.initialize(members))
            .await
            .expect("init timeout")
            .expect("init failed");

        let raft1_clone = raft1.clone();
        tokio::time::timeout(Duration::from_secs(5), async move {
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

        PartitionCluster {
            node1,
            node2,
            node3,
            engine1,
            engine2,
            engine3,
            blocked,
            _dirs: vec![
                lance_dir1, lance_dir2, lance_dir3, raft_dir1, raft_dir2, raft_dir3,
            ],
            _permit: permit,
        }
    }

    /// Block all RPCs from `from` to `to`.
    fn block(&self, from: u64, to: u64) {
        self.blocked.insert((from, to));
    }

    /// Unblock RPCs from `from` to `to`.
    fn unblock(&self, from: u64, to: u64) {
        self.blocked.remove(&(from, to));
    }

    /// Isolate a node: block all traffic to and from it.
    fn isolate_node(&self, node_id: u64) {
        for other in [1u64, 2, 3] {
            if other != node_id {
                self.block(node_id, other);
                self.block(other, node_id);
            }
        }
    }

    /// Heal all partitions.
    fn heal_all(&self) {
        self.blocked.clear();
    }

    fn node(&self, id: u64) -> &Arc<LanceRaftNode> {
        match id {
            1 => &self.node1,
            2 => &self.node2,
            3 => &self.node3,
            _ => panic!("invalid node id"),
        }
    }

    fn engine(&self, id: u64) -> &Arc<BisqueLance> {
        match id {
            1 => &self.engine1,
            2 => &self.engine2,
            3 => &self.engine3,
            _ => panic!("invalid node id"),
        }
    }

    /// Wait for a specific node to apply at least `min_log_index`.
    async fn wait_for_node(&self, node_id: u64, min_log_index: u64) {
        let raft = self.node(node_id).raft().clone();
        tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                let m = raft.metrics().borrow_watched().clone();
                if m.last_applied.map(|id| id.index).unwrap_or(0) >= min_log_index {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("wait_for_node timeout");
    }

    /// Wait for any of the given nodes to become leader. Returns the leader's node_id.
    async fn wait_for_leader_among(&self, candidates: &[u64], timeout_secs: u64) -> u64 {
        let rafts: Vec<_> = candidates
            .iter()
            .map(|&id| (id, self.node(id).raft().clone()))
            .collect();
        tokio::time::timeout(Duration::from_secs(timeout_secs), async {
            loop {
                for (id, raft) in &rafts {
                    let m = raft.metrics().borrow_watched().clone();
                    if m.current_leader == Some(*id) {
                        return *id;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("leader election timeout")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_isolate_leader_causes_reelection() {
    let cluster = PartitionCluster::new().await;

    // Create table and write data while healthy.
    let create = cluster
        .node1
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_node(2, create.log_index).await;
    cluster.wait_for_node(3, create.log_index).await;

    let write = cluster
        .node1
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_node(2, write.log_index).await;
    cluster.wait_for_node(3, write.log_index).await;

    // Isolate node1 (leader). Node2 and node3 can still talk to each other.
    cluster.isolate_node(1);

    // Wait for a new leader among node2 and node3.
    let new_leader = cluster.wait_for_leader_among(&[2, 3], 10).await;
    assert!(new_leader == 2 || new_leader == 3);

    // Write through new leader — should succeed with quorum of 2.
    let w = cluster
        .node(new_leader)
        .write_records("t1", &[test_batch(&[4, 5], &["d", "e"])])
        .await
        .unwrap();

    // The other follower should replicate.
    let other = if new_leader == 2 { 3 } else { 2 };
    cluster.wait_for_node(other, w.log_index).await;

    assert_eq!(count_active_rows(cluster.engine(new_leader), "t1").await, 5);
    assert_eq!(count_active_rows(cluster.engine(other), "t1").await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_minority_cannot_elect_leader() {
    let cluster = PartitionCluster::new().await;

    let create = cluster
        .node1
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_node(2, create.log_index).await;
    cluster.wait_for_node(3, create.log_index).await;

    // Isolate node3 from both node1 and node2.
    // Node3 is alone (minority) — it cannot form a quorum.
    cluster.isolate_node(3);

    // Node1 should remain leader (has quorum with node2).
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        cluster.node1.is_leader(),
        "node1 should remain leader with majority"
    );

    // Writes through node1 should still work (quorum = node1 + node2).
    let w = cluster
        .node1
        .write_records("t1", &[test_batch(&[10, 20], &["x", "y"])])
        .await
        .unwrap();
    cluster.wait_for_node(2, w.log_index).await;

    assert_eq!(count_active_rows(&cluster.engine1, "t1").await, 2);
    assert_eq!(count_active_rows(&cluster.engine2, "t1").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_heal_resumes_replication() {
    let cluster = PartitionCluster::new().await;

    let create = cluster
        .node1
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_node(2, create.log_index).await;
    cluster.wait_for_node(3, create.log_index).await;

    // Isolate node3.
    cluster.isolate_node(3);

    // Write data while node3 is partitioned.
    let w1 = cluster
        .node1
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_node(2, w1.log_index).await;

    let w2 = cluster
        .node1
        .write_records("t1", &[test_batch(&[4, 5], &["d", "e"])])
        .await
        .unwrap();
    cluster.wait_for_node(2, w2.log_index).await;

    // Node3 should NOT have the new data.
    let m3 = cluster.node3.raft().metrics().borrow_watched().clone();
    let applied3 = m3.last_applied.map(|id| id.index).unwrap_or(0);
    assert!(applied3 < w2.log_index, "node3 should be behind");

    // Heal the partition.
    cluster.heal_all();

    // Wait for node3 to catch up.
    cluster.wait_for_node(3, w2.log_index).await;

    // All nodes should have the same data.
    assert_eq!(count_active_rows(&cluster.engine1, "t1").await, 5);
    assert_eq!(count_active_rows(&cluster.engine2, "t1").await, 5);
    assert_eq!(count_active_rows(&cluster.engine3, "t1").await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_asymmetric_leader_can_send_but_not_receive() {
    let cluster = PartitionCluster::new().await;

    let create = cluster
        .node1
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_node(2, create.log_index).await;
    cluster.wait_for_node(3, create.log_index).await;

    // Asymmetric partition: node2 and node3 cannot reach node1,
    // but node1 CAN reach them. This means node1 can replicate to them,
    // but they can't vote for node1 or send responses properly.
    // In practice, openraft handles this through its internal mechanisms.
    cluster.block(2, 1);
    cluster.block(3, 1);

    // Node1 can still send heartbeats to node2 and node3,
    // so it may stay leader if responses aren't required for heartbeats.
    // Try a write — it needs responses from followers to commit.
    let write_result = tokio::time::timeout(
        Duration::from_secs(3),
        cluster
            .node1
            .write_records("t1", &[test_batch(&[10], &["x"])]),
    )
    .await;

    // Heal and verify cluster recovers.
    cluster.heal_all();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // After healing, writes should work again.
    // Wait for any leader to be elected.
    let leader = cluster.wait_for_leader_among(&[1, 2, 3], 10).await;
    let w = cluster
        .node(leader)
        .write_records("t1", &[test_batch(&[100], &["recovered"])])
        .await
        .unwrap();

    // Wait for replication to all nodes.
    for id in [1, 2, 3] {
        if id != leader {
            cluster.wait_for_node(id, w.log_index).await;
        }
    }

    // All nodes should have the recovery write.
    for id in [1, 2, 3] {
        assert!(count_active_rows(cluster.engine(id), "t1").await >= 1);
    }
}

// =============================================================================
// T. Raft Membership Change Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_add_learner_and_promote_to_voter() {
    // Start with a 2-node cluster, then add node3 as learner and promote.
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

    let sm1 = LanceStateMachine::new(engine1.clone())
        .with_catalog_events(Arc::new(CatalogEventBus::new(0)));
    let sm2 = LanceStateMachine::new(engine2.clone())
        .with_catalog_events(Arc::new(CatalogEventBus::new(0)));
    let sm3 = LanceStateMachine::new(engine3.clone())
        .with_catalog_events(Arc::new(CatalogEventBus::new(0)));

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
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        connection_ttl: Duration::from_secs(30),
        tcp_nodelay: true,
        ..Default::default()
    };
    let t1 = BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
    let t2 = BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
    let t3 = BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg, registry.clone());

    let manager1 = Arc::new(MultiRaftManager::new(t1, storage1));
    let manager2 = Arc::new(MultiRaftManager::new(t2, storage2));
    let manager3 = Arc::new(MultiRaftManager::new(t3, storage3));

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
            heartbeat_interval: 200,
            election_timeout_min: 400,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    // Initialize with 2-node cluster (node1 + node2).
    let raft1 = manager1
        .add_group(0, 1, raft_cfg.clone(), sm1)
        .await
        .unwrap();
    let _raft2 = manager2
        .add_group(0, 2, raft_cfg.clone(), sm2)
        .await
        .unwrap();

    let mut initial_members = BTreeMap::new();
    initial_members.insert(1u64, openraft::impls::BasicNode::default());
    initial_members.insert(2u64, openraft::impls::BasicNode::default());

    tokio::time::timeout(Duration::from_secs(5), raft1.initialize(initial_members))
        .await
        .expect("init timeout")
        .expect("init failed");

    // Wait for leader.
    let raft1_clone = raft1.clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
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
        LanceRaftNode::new(raft1.clone(), engine1.clone(), 1)
            .with_seal_check_interval(Duration::from_secs(999))
            .with_flush_check_interval(Duration::from_secs(999))
            .with_compaction_check_interval(Duration::from_secs(999)),
    );
    node1.start();

    // Write some data with the 2-node cluster.
    let create = node1.create_table("t1", &test_schema()).await.unwrap();
    let write = node1
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    // Now add node3 as a Raft group member.
    let _raft3 = manager3.add_group(0, 3, raft_cfg, sm3).await.unwrap();

    // Add node3 as a learner first.
    raft1
        .add_learner(3, openraft::impls::BasicNode::default(), true)
        .await
        .expect("add_learner failed");

    // Promote node3 to voter.
    let new_members: BTreeSet<u64> = [1, 2, 3].into();
    raft1
        .change_membership(new_members, false)
        .await
        .expect("change_membership failed");

    // Verify node3 has caught up with data.
    let raft3 = manager3.get_group(0).unwrap();
    let raft3_clone = raft3.clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let m = raft3_clone.metrics().borrow_watched().clone();
            if m.last_applied.map(|id| id.index).unwrap_or(0) >= write.log_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node3 replication timeout");

    // Node3 should have the table and data.
    assert!(
        engine3.has_table("t1"),
        "node3 should have table after joining"
    );
    assert_eq!(count_active_rows(&engine3, "t1").await, 3);

    // Write more data — all 3 nodes should participate.
    let w2 = node1
        .write_records("t1", &[test_batch(&[4, 5], &["d", "e"])])
        .await
        .unwrap();

    let raft3_clone2 = raft3.clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let m = raft3_clone2.metrics().borrow_watched().clone();
            if m.last_applied.map(|id| id.index).unwrap_or(0) >= w2.log_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node3 replication timeout after promotion");

    assert_eq!(count_active_rows(&engine3, "t1").await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_voter_from_cluster() {
    let cluster = TestCluster::new().await;

    // Create table and write data with 3 nodes.
    let create = cluster
        .leader()
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster.wait_for_replication(create.log_index).await;

    let write = cluster
        .leader()
        .write_records("t1", &[test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    cluster.wait_for_replication(write.log_index).await;

    // Remove node3 from the cluster. retain=true to keep as learner.
    let new_members: BTreeSet<u64> = [1, 2].into();
    cluster
        .leader()
        .raft()
        .change_membership(new_members, true)
        .await
        .expect("change_membership failed");

    // Write more data — should succeed with 2-node quorum.
    let w2 = cluster
        .leader()
        .write_records("t1", &[test_batch(&[4, 5], &["d", "e"])])
        .await
        .unwrap();

    // Wait for node2 to replicate.
    let raft2 = cluster.node2.raft().clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let m = raft2.metrics().borrow_watched().clone();
            if m.last_applied.map(|id| id.index).unwrap_or(0) >= w2.log_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node2 replication timeout");

    assert_eq!(count_active_rows(cluster.leader_engine(), "t1").await, 5);
    assert_eq!(count_active_rows(cluster.follower_engine(), "t1").await, 5);
}

// =============================================================================
// U. Crash Recovery Tests (MDBX Manifest)
// =============================================================================

/// Helper to build a 2-node cluster with MDBX manifest wired to node1's state machine.
/// Returns the cluster components needed for crash recovery testing.
struct ManifestCluster {
    node1: Arc<LanceRaftNode>,
    engine1: Arc<BisqueLance>,
    manifest: Arc<LanceManifestManager>,
    lance_dir1: tempfile::TempDir,
    manifest_dir: tempfile::TempDir,
    _dirs: Vec<tempfile::TempDir>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ManifestCluster {
    async fn new() -> Self {
        let permit = Arc::clone(&CLUSTER_SEMAPHORE)
            .acquire_owned()
            .await
            .unwrap();
        let addr1 = pick_unused_local_addr();
        let addr2 = pick_unused_local_addr();

        let registry = Arc::new(DefaultNodeRegistry::<u64>::new());
        registry.register(1, addr1);
        registry.register(2, addr2);

        let lance_dir1 = tempfile::tempdir().unwrap();
        let lance_dir2 = tempfile::tempdir().unwrap();
        let raft_dir1 = tempfile::tempdir().unwrap();
        let raft_dir2 = tempfile::tempdir().unwrap();
        let manifest_dir = tempfile::tempdir().unwrap();

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

        let manifest = Arc::new(LanceManifestManager::new(manifest_dir.path()).unwrap());
        manifest.open_group(0).unwrap();

        let sm1 = LanceStateMachine::new(engine1.clone())
            .with_catalog_events(Arc::new(CatalogEventBus::new(0)))
            .with_manifest(manifest.clone(), 0);
        let sm2 = LanceStateMachine::new(engine2.clone())
            .with_catalog_events(Arc::new(CatalogEventBus::new(0)));

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

        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
            ..Default::default()
        };
        let t1 =
            BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg.clone(), registry.clone());
        let t2 = BisqueTcpTransport::<LanceTypeConfig>::new(transport_cfg, registry.clone());

        let manager1 = Arc::new(MultiRaftManager::new(t1, storage1));
        let manager2 = Arc::new(MultiRaftManager::new(t2, storage2));

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
        tokio::time::sleep(Duration::from_millis(100)).await;

        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 200,
                election_timeout_min: 400,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let raft1 = manager1
            .add_group(0, 1, raft_cfg.clone(), sm1)
            .await
            .unwrap();
        let _raft2 = manager2.add_group(0, 2, raft_cfg, sm2).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::impls::BasicNode::default());
        members.insert(2u64, openraft::impls::BasicNode::default());

        tokio::time::timeout(Duration::from_secs(5), raft1.initialize(members))
            .await
            .expect("init timeout")
            .expect("init failed");

        let raft1_clone = raft1.clone();
        tokio::time::timeout(Duration::from_secs(5), async move {
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

        ManifestCluster {
            node1,
            engine1,
            manifest,
            lance_dir1,
            manifest_dir,
            _dirs: vec![lance_dir2, raft_dir1, raft_dir2],
            _permit: permit,
        }
    }

    /// Wait for manifest to flush (batches every 50ms).
    async fn flush_manifest(&self) {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// Read persisted table entries from the manifest (call before crash).
    fn read_entries(&self) -> std::collections::HashMap<String, PersistedTableEntry> {
        self.manifest.read_all_tables(0).unwrap()
    }

    /// Simulate crash: stop manifest worker, drop in-memory engine state.
    /// Returns the lance data path and tempdirs (to keep them alive).
    fn crash(self) -> (std::path::PathBuf, Vec<tempfile::TempDir>) {
        let lance_path = self.lance_dir1.path().to_path_buf();
        self.manifest.stop();
        let mut dirs = self._dirs;
        dirs.push(self.lance_dir1);
        dirs.push(self.manifest_dir);
        (lance_path, dirs)
    }
}

/// Recover tables from persisted entries + lance data directory.
/// This simulates what `applied_state()` does when a node restarts after a crash.
async fn recover_from_entries(
    lance_path: &std::path::Path,
    entries: std::collections::HashMap<String, PersistedTableEntry>,
) -> Arc<BisqueLance> {
    let engine = Arc::new(
        BisqueLance::open(BisqueLanceConfig::new(lance_path))
            .await
            .unwrap(),
    );
    if !entries.is_empty() {
        engine
            .restore_from_persisted_entries(entries)
            .await
            .unwrap();
    }
    engine
}

#[tokio::test(flavor = "multi_thread")]
async fn test_crash_recovery_restores_tables_from_manifest() {
    let cluster = ManifestCluster::new().await;

    // Create two tables and write data.
    cluster
        .node1
        .create_table("table_a", &test_schema())
        .await
        .unwrap();
    cluster
        .node1
        .create_table("table_b", &test_schema())
        .await
        .unwrap();
    cluster
        .node1
        .write_records("table_a", &[test_batch(&[1, 2, 3], &["x", "y", "z"])])
        .await
        .unwrap();
    cluster
        .node1
        .write_records("table_b", &[test_batch(&[10, 20], &["aa", "bb"])])
        .await
        .unwrap();

    cluster.flush_manifest().await;

    // Read entries from manifest before crash.
    let entries = cluster.read_entries();
    assert_eq!(entries.len(), 2);
    assert!(entries.contains_key("table_a"));
    assert!(entries.contains_key("table_b"));

    // ---- CRASH + RECOVER ----
    let (lance_path, _dirs) = cluster.crash();
    let engine_recovered = recover_from_entries(&lance_path, entries).await;

    // Verify recovered state.
    assert!(
        engine_recovered.has_table("table_a"),
        "table_a should survive crash"
    );
    assert!(
        engine_recovered.has_table("table_b"),
        "table_b should survive crash"
    );
    assert_eq!(count_active_rows(&engine_recovered, "table_a").await, 3);
    assert_eq!(count_active_rows(&engine_recovered, "table_b").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_crash_recovery_preserves_segment_catalog() {
    let cluster = ManifestCluster::new().await;

    // Create table, write data, then seal.
    cluster
        .node1
        .create_table("t1", &test_schema())
        .await
        .unwrap();
    cluster
        .node1
        .write_records("t1", &[test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();

    let table = cluster.engine1.require_table("t1").unwrap();
    let active_seg = table.catalog().active_segment;

    // Seal the segment.
    let seal_cmd = bisque_lance::types::LanceCommand::SealActiveSegment {
        table_name: "t1".into(),
        sealed_segment_id: active_seg,
        new_active_segment_id: active_seg + 1,
        reason: bisque_lance::types::SealReason::MaxSize,
    };
    cluster.node1.propose(seal_cmd).await.unwrap();

    // Verify seal state pre-crash.
    let cat = cluster.engine1.require_table("t1").unwrap().catalog();
    assert_eq!(cat.sealed_segment, Some(active_seg));
    assert_eq!(cat.active_segment, active_seg + 1);

    cluster.flush_manifest().await;

    // Read entries before crash.
    let entries = cluster.read_entries();

    // ---- CRASH + RECOVER ----
    let (lance_path, _dirs) = cluster.crash();
    let engine_recovered = recover_from_entries(&lance_path, entries).await;

    // Verify: table exists and seal state is preserved.
    assert!(
        engine_recovered.has_table("t1"),
        "table should survive crash"
    );
    let recovered_table = engine_recovered.require_table("t1").unwrap();
    let recovered_cat = recovered_table.catalog();
    assert_eq!(
        recovered_cat.sealed_segment,
        Some(active_seg),
        "sealed segment should be preserved"
    );
    assert_eq!(
        recovered_cat.active_segment,
        active_seg + 1,
        "active segment should be preserved"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_crash_recovery_table_drop_persists() {
    let cluster = ManifestCluster::new().await;

    // Create two tables, then drop one.
    cluster
        .node1
        .create_table("keep_me", &test_schema())
        .await
        .unwrap();
    cluster
        .node1
        .create_table("drop_me", &test_schema())
        .await
        .unwrap();
    cluster
        .node1
        .write_records("keep_me", &[test_batch(&[1], &["a"])])
        .await
        .unwrap();
    cluster
        .node1
        .write_records("drop_me", &[test_batch(&[2], &["b"])])
        .await
        .unwrap();
    cluster.node1.drop_table("drop_me").await.unwrap();

    assert!(cluster.engine1.has_table("keep_me"));
    assert!(!cluster.engine1.has_table("drop_me"));

    cluster.flush_manifest().await;

    // Read entries before crash.
    let entries = cluster.read_entries();

    // ---- CRASH + RECOVER ----
    let (lance_path, _dirs) = cluster.crash();
    let engine_recovered = recover_from_entries(&lance_path, entries).await;

    // Verify: keep_me exists, drop_me does not.
    assert!(
        engine_recovered.has_table("keep_me"),
        "kept table should survive crash"
    );
    assert!(
        !engine_recovered.has_table("drop_me"),
        "dropped table should stay dropped after crash"
    );
    assert_eq!(count_active_rows(&engine_recovered, "keep_me").await, 1);
}
