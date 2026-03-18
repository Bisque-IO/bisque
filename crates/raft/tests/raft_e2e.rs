//! End-to-end tests for multi-raft consensus scenarios.
//!
//! These tests exercise full multi-node raft cluster behavior:
//! leader election, replication, leader failure/re-election,
//! node restart/catch-up, membership changes, and snapshots.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{SocketAddr, TcpListener};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bisque_raft::BisqueRaftTypeConfig;
use bisque_raft::NodeAddressResolver;
use bisque_raft::codec;
use bisque_raft::test_support::TestTempDir;
use bisque_raft::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MmapStorageConfig, MultiRaftManager, MultiplexedLogStorage,
};
use futures::FutureExt;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ClientWriteError, RaftError};
use openraft::storage::RaftStateMachine;
use openraft::{LogId, OptionalSend, Raft, StoredMembership};
use parking_lot::RwLock;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Shared test types
// ---------------------------------------------------------------------------

fn pick_unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephem");
    listener.local_addr().expect("local_addr")
}

fn run_async<F>(f: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    rt.block_on(async move {
        match AssertUnwindSafe(f).catch_unwind().await {
            Ok(v) => v,
            Err(p) => std::panic::resume_unwind(p),
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct TestData(Vec<u8>);

impl fmt::Display for TestData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestData({} bytes)", self.0.len())
    }
}

impl codec::Encode for TestData {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), codec::CodecError> {
        (self.0.len() as u32).encode(writer)?;
        writer.write_all(&self.0)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.0.len()
    }
}

impl codec::Decode for TestData {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, codec::CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(Self(buf))
    }
}

impl codec::BorrowPayload for TestData {
    fn payload_bytes(&self) -> &[u8] {
        &self.0
    }
}

type TestConfig = BisqueRaftTypeConfig<TestData, ()>;

// ---------------------------------------------------------------------------
// Enhanced TestStateMachine with snapshot support
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct TestStateMachine {
    applied_normal: Arc<AtomicU64>,
    last_applied: Arc<RwLock<Option<LogId<TestConfig>>>>,
    last_membership: Arc<RwLock<StoredMembership<TestConfig>>>,
    current_snapshot: Arc<RwLock<Option<openraft::storage::Snapshot<TestConfig>>>>,
}

impl TestStateMachine {
    fn new() -> Self {
        Self {
            applied_normal: Arc::new(AtomicU64::new(0)),
            last_applied: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            current_snapshot: Arc::new(RwLock::new(None)),
        }
    }

    fn applied(&self) -> u64 {
        self.applied_normal.load(Ordering::SeqCst)
    }
}

impl RaftStateMachine<TestConfig> for TestStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TestConfig>>, StoredMembership<TestConfig>), std::io::Error> {
        let la = self.last_applied.read().clone();
        let lm = self.last_membership.read().clone();
        Ok((la, lm))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), std::io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<TestConfig>, std::io::Error>,
            > + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;
        while let Some(Ok((entry, responder))) = entries.next().await {
            *self.last_applied.write() = Some(entry.log_id);

            match &entry.payload {
                openraft::EntryPayload::Normal(_) => {
                    self.applied_normal.fetch_add(1, Ordering::SeqCst);
                }
                openraft::EntryPayload::Membership(m) => {
                    *self.last_membership.write() =
                        StoredMembership::new(Some(entry.log_id), m.clone());
                }
                _ => {}
            }

            if let Some(r) = responder {
                r.send(());
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<std::io::Cursor<Vec<u8>>, std::io::Error> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::storage::SnapshotMeta<TestConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), std::io::Error> {
        let data = snapshot.into_inner();
        if data.len() >= 8 {
            let count = u64::from_le_bytes(data[..8].try_into().unwrap());
            self.applied_normal.store(count, Ordering::SeqCst);
        }
        *self.last_applied.write() = meta.last_log_id;
        *self.last_membership.write() = meta.last_membership.clone();
        *self.current_snapshot.write() = Some(openraft::storage::Snapshot {
            meta: meta.clone(),
            snapshot: std::io::Cursor::new(data),
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::storage::Snapshot<TestConfig>>, std::io::Error> {
        let snap = self.current_snapshot.read();
        Ok(snap.as_ref().map(|s| openraft::storage::Snapshot {
            meta: s.meta.clone(),
            snapshot: std::io::Cursor::new(s.snapshot.get_ref().clone()),
        }))
    }
}

impl openraft::RaftSnapshotBuilder<TestConfig> for TestStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<TestConfig>, std::io::Error> {
        let count = self.applied_normal.load(Ordering::SeqCst);
        let data = count.to_le_bytes().to_vec();
        let meta = openraft::storage::SnapshotMeta {
            last_log_id: self.last_applied.read().clone(),
            last_membership: self.last_membership.read().clone(),
            snapshot_id: format!("snap-{}", count),
        };
        let snapshot = openraft::storage::Snapshot {
            meta: meta.clone(),
            snapshot: std::io::Cursor::new(data.clone()),
        };
        *self.current_snapshot.write() = Some(openraft::storage::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(data),
        });
        Ok(snapshot)
    }
}

// ---------------------------------------------------------------------------
// E2eCluster harness
// ---------------------------------------------------------------------------

type Manager =
    MultiRaftManager<TestConfig, BisqueTcpTransport<TestConfig>, MultiplexedLogStorage<TestConfig>>;

type Server =
    BisqueRpcServer<TestConfig, BisqueTcpTransport<TestConfig>, MultiplexedLogStorage<TestConfig>>;

struct E2eNode {
    node_id: u64,
    #[allow(dead_code)]
    addr: SocketAddr,
    manager: Arc<Manager>,
    server: Arc<Server>,
    serve_handle: tokio::task::JoinHandle<()>,
    data_dir: PathBuf,
    _dir_handle: Option<TestTempDir>,
}

struct StoppedNode {
    node_id: u64,
    data_dir: PathBuf,
    _dir_handle: Option<TestTempDir>,
}

struct E2eCluster {
    nodes: Vec<E2eNode>,
    node_registry: Arc<DefaultNodeRegistry<u64>>,
    transport_cfg: BisqueTcpTransportConfig,
    raft_cfg: Arc<openraft::Config>,
    stopped_nodes: Vec<StoppedNode>,
}

impl E2eCluster {
    async fn new(num_nodes: usize) -> Self {
        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 200,
                election_timeout_min: 400,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .expect("raft config"),
        );
        Self::new_with_config(num_nodes, raft_cfg).await
    }

    async fn new_with_config(num_nodes: usize, raft_cfg: Arc<openraft::Config>) -> Self {
        let node_registry = Arc::new(DefaultNodeRegistry::<u64>::new());
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
            ..Default::default()
        };

        let mut nodes = Vec::with_capacity(num_nodes);

        for i in 0..num_nodes {
            let node_id = (i + 1) as u64;
            let node = Self::create_node(
                node_id,
                &node_registry,
                &transport_cfg,
                None, // new tempdir
            )
            .await;
            nodes.push(node);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        E2eCluster {
            nodes,
            node_registry,
            transport_cfg,
            raft_cfg,
            stopped_nodes: Vec::new(),
        }
    }

    async fn create_node(
        node_id: u64,
        registry: &Arc<DefaultNodeRegistry<u64>>,
        transport_cfg: &BisqueTcpTransportConfig,
        existing_dir: Option<(PathBuf, Option<TestTempDir>)>,
    ) -> E2eNode {
        let addr = pick_unused_local_addr();
        registry.register(node_id, addr);

        let (data_dir, dir_handle) = match existing_dir {
            Some((path, handle)) => (path, handle),
            None => {
                let td = TestTempDir::new();
                let path = td.path().to_path_buf();
                (path, Some(td))
            }
        };

        // Retry storage creation in case MDBX lock is still held from a previous instance
        let config = MmapStorageConfig::new(&data_dir)
            .with_segment_size(4 * 1024 * 1024)
            .with_fsync_delay(Duration::ZERO);
        let mut storage = None;
        for attempt in 0..20u64 {
            match MultiplexedLogStorage::<TestConfig>::new(config.clone()).await {
                Ok(s) => {
                    storage = Some(s);
                    break;
                }
                Err(e) => {
                    if attempt == 19 {
                        panic!("storage creation failed after 20 retries: {}", e);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        let storage = storage.unwrap();

        let transport =
            BisqueTcpTransport::<TestConfig>::new(transport_cfg.clone(), registry.clone());
        let manager = Arc::new(MultiRaftManager::new(transport, storage));

        let server = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr,
                ..Default::default()
            },
            manager.clone(),
        ));

        let serve_handle = tokio::spawn({
            let s = server.clone();
            async move {
                let _ = s.serve().await;
            }
        });

        E2eNode {
            node_id,
            addr,
            manager,
            server,
            serve_handle,
            data_dir,
            _dir_handle: dir_handle,
        }
    }

    fn membership(&self) -> BTreeMap<u64, openraft::impls::BasicNode> {
        let mut members = BTreeMap::new();
        for node in &self.nodes {
            members.insert(node.node_id, openraft::impls::BasicNode::default());
        }
        members
    }

    async fn add_group(&self, group_id: u64) -> Vec<TestStateMachine> {
        let mut state_machines = Vec::new();

        for node in &self.nodes {
            let sm = TestStateMachine::new();
            node.manager
                .add_group(group_id, node.node_id, self.raft_cfg.clone(), sm.clone())
                .await
                .expect("add group");
            state_machines.push(sm);
        }

        // Initialize from node 1
        let raft = self.nodes[0]
            .manager
            .get_group(group_id)
            .expect("group exists");
        let members = self.membership();
        tokio::time::timeout(Duration::from_secs(5), raft.initialize(members))
            .await
            .expect("init timeout")
            .expect("init failed");

        state_machines
    }

    fn get_node(&self, node_id: u64) -> &E2eNode {
        self.nodes
            .iter()
            .find(|n| n.node_id == node_id)
            .unwrap_or_else(|| panic!("node {} not found", node_id))
    }

    fn get_raft(&self, group_id: u64, node_id: u64) -> Raft<TestConfig> {
        self.get_node(node_id)
            .manager
            .get_group(group_id)
            .unwrap_or_else(|| panic!("group {} not on node {}", group_id, node_id))
    }

    async fn wait_for_any_leader(&self, group_id: u64, timeout_secs: u64) -> u64 {
        tokio::time::timeout(Duration::from_secs(timeout_secs), async {
            loop {
                for node in &self.nodes {
                    if let Some(raft) = node.manager.get_group(group_id) {
                        let m = raft.metrics().borrow_watched().clone();
                        if let Some(leader) = m.current_leader {
                            if leader == node.node_id {
                                return leader;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("wait_for_any_leader timeout")
    }

    async fn wait_for_replication(
        &self,
        group_id: u64,
        min_index: u64,
        node_ids: &[u64],
        timeout_secs: u64,
    ) {
        tokio::time::timeout(Duration::from_secs(timeout_secs), async {
            loop {
                let mut all_caught_up = true;
                for &nid in node_ids {
                    let raft = self.get_raft(group_id, nid);
                    let m = raft.metrics().borrow_watched().clone();
                    let applied_idx = m.last_applied.map(|l| l.index).unwrap_or(0);
                    if applied_idx < min_index {
                        all_caught_up = false;
                        break;
                    }
                }
                if all_caught_up {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("wait_for_replication timeout");
    }

    async fn stop_node(&mut self, node_id: u64) {
        let idx = self
            .nodes
            .iter()
            .position(|n| n.node_id == node_id)
            .unwrap_or_else(|| panic!("node {} not found", node_id));
        let node = self.nodes.remove(idx);

        // 1. Shut down raft groups first (stops replication traffic)
        node.manager.shutdown_all().await;

        // 2. Drain connections — shutdown_rx propagates to all connection
        //    handler tasks so they exit promptly, releasing Arc refs.
        node.server.shutdown_and_drain(Duration::from_secs(5)).await;

        // 3. Stop storage (fsync thread, manifest worker)
        node.manager.storage().stop();

        // 4. Wait for the serve task to finish
        let E2eNode {
            node_id,
            data_dir,
            _dir_handle,
            serve_handle,
            server,
            manager,
            ..
        } = node;
        let _ = serve_handle.await;

        // 5. Drop server before manager so connection handler Arc<Manager> refs
        //    are the only remaining ones. They should already be gone after drain.
        drop(server);
        drop(manager);

        self.stopped_nodes.push(StoppedNode {
            node_id,
            data_dir,
            _dir_handle,
        });
    }

    async fn restart_node(&mut self, node_id: u64, group_ids: &[u64]) -> Vec<TestStateMachine> {
        let stopped_idx = self
            .stopped_nodes
            .iter()
            .position(|n| n.node_id == node_id)
            .unwrap_or_else(|| panic!("node {} not in stopped_nodes", node_id));
        let stopped = self.stopped_nodes.remove(stopped_idx);

        let new_node = Self::create_node(
            node_id,
            &self.node_registry,
            &self.transport_cfg,
            Some((stopped.data_dir, stopped._dir_handle)),
        )
        .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut sms = Vec::new();
        for &gid in group_ids {
            let sm = TestStateMachine::new();
            new_node
                .manager
                .add_group(gid, node_id, self.raft_cfg.clone(), sm.clone())
                .await
                .expect("re-add group");
            sms.push(sm);
        }

        self.nodes.push(new_node);
        sms
    }

    async fn add_new_node(&mut self, node_id: u64, group_ids: &[u64]) -> Vec<TestStateMachine> {
        let new_node =
            Self::create_node(node_id, &self.node_registry, &self.transport_cfg, None).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut sms = Vec::new();
        for &gid in group_ids {
            let sm = TestStateMachine::new();
            new_node
                .manager
                .add_group(gid, node_id, self.raft_cfg.clone(), sm.clone())
                .await
                .expect("add group on new node");
            sms.push(sm);
        }

        self.nodes.push(new_node);
        sms
    }

    fn shutdown(&self) {
        for node in &self.nodes {
            node.server.shutdown();
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: wait for state machines to reach a minimum applied count
// ---------------------------------------------------------------------------

async fn wait_for_applied(sms: &[TestStateMachine], min_count: u64, timeout_secs: u64) {
    tokio::time::timeout(Duration::from_secs(timeout_secs), async {
        loop {
            if sms.iter().all(|sm| sm.applied() >= min_count) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        let counts: Vec<u64> = sms.iter().map(|sm| sm.applied()).collect();
        panic!(
            "wait_for_applied timeout: wanted >= {}, got {:?}",
            min_count, counts
        );
    });
}

// ---------------------------------------------------------------------------
// Helper: write N entries to a group through its leader
// ---------------------------------------------------------------------------

async fn write_entries(cluster: &E2eCluster, group_id: u64, leader_id: u64, count: u64) {
    let raft = cluster.get_raft(group_id, leader_id);
    for i in 0..count {
        raft.client_write(TestData(format!("entry-{}", i).into_bytes()))
            .await
            .unwrap_or_else(|e| panic!("client_write {} failed: {:?}", i, e));
    }
}

// ===========================================================================
// Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. Basic operations
// ---------------------------------------------------------------------------

#[test]
fn test_3node_basic_write_and_replication() {
    run_async(async {
        let cluster = E2eCluster::new(3).await;
        let sms = cluster.add_group(1).await;

        let leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 10 entries
        write_entries(&cluster, 1, leader, 10).await;

        // Wait for all 3 state machines to apply 10 normal entries
        wait_for_applied(&sms, 10, 10).await;

        cluster.shutdown();
    });
}

#[test]
fn test_write_to_follower_returns_forward_error() {
    run_async(async {
        let cluster = E2eCluster::new(3).await;
        let _sms = cluster.add_group(1).await;

        let leader = cluster.wait_for_any_leader(1, 10).await;

        // Find a follower
        let follower = cluster
            .nodes
            .iter()
            .find(|n| n.node_id != leader)
            .unwrap()
            .node_id;

        let follower_raft = cluster.get_raft(1, follower);
        let result = follower_raft
            .client_write(TestData(b"should_fail".to_vec()))
            .await;

        match result {
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                assert_eq!(
                    fwd.leader_id,
                    Some(leader),
                    "ForwardToLeader should point to actual leader"
                );
            }
            Ok(_) => panic!("write to follower should not succeed"),
            Err(e) => panic!("unexpected error type: {:?}", e),
        }

        cluster.shutdown();
    });
}

#[test]
fn test_multi_group_isolation_3node() {
    run_async(async {
        let cluster = E2eCluster::new(3).await;
        let sms1 = cluster.add_group(1).await;
        let sms2 = cluster.add_group(2).await;

        let leader1 = cluster.wait_for_any_leader(1, 10).await;
        let leader2 = cluster.wait_for_any_leader(2, 10).await;

        // Write 5 to group 1 only
        write_entries(&cluster, 1, leader1, 5).await;
        wait_for_applied(&sms1, 5, 10).await;

        // Group 2 should be unaffected
        for sm in &sms2 {
            assert_eq!(sm.applied(), 0, "group 2 should have 0 applied entries");
        }

        // Write 3 to group 2
        write_entries(&cluster, 2, leader2, 3).await;
        wait_for_applied(&sms2, 3, 10).await;

        // Group 1 still at 5
        for sm in &sms1 {
            assert_eq!(sm.applied(), 5, "group 1 should still have 5 applied");
        }

        cluster.shutdown();
    });
}

// ---------------------------------------------------------------------------
// 2. Fault tolerance
// ---------------------------------------------------------------------------

#[test]
fn test_leader_shutdown_triggers_reelection() {
    run_async(async {
        let mut cluster = E2eCluster::new(3).await;
        let _sms = cluster.add_group(1).await;

        let old_leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 5 entries
        write_entries(&cluster, 1, old_leader, 5).await;
        let all_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 5, &all_ids, 10).await;

        // Kill the leader
        cluster.stop_node(old_leader).await;

        // Wait for re-election
        let new_leader = cluster.wait_for_any_leader(1, 15).await;
        assert_ne!(new_leader, old_leader, "new leader must be different");

        // Write 5 more entries through the new leader
        write_entries(&cluster, 1, new_leader, 5).await;

        // Wait for replication on surviving nodes
        let survivor_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 10, &survivor_ids, 10).await;

        cluster.shutdown();
    });
}

#[test]
fn test_follower_restart_catches_up() {
    run_async(async {
        let mut cluster = E2eCluster::new(3).await;
        let _sms = cluster.add_group(1).await;

        let leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 5 entries, fully replicated
        write_entries(&cluster, 1, leader, 5).await;
        let all_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 5, &all_ids, 10).await;

        // Stop a follower
        let follower = cluster
            .nodes
            .iter()
            .find(|n| n.node_id != leader)
            .unwrap()
            .node_id;
        cluster.stop_node(follower).await;

        // Write 10 more entries while follower is down
        write_entries(&cluster, 1, leader, 10).await;
        let survivor_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 15, &survivor_ids, 10).await;

        // Restart the follower
        let new_sms = cluster.restart_node(follower, &[1]).await;

        // Wait for the restarted node to catch up
        cluster.wait_for_replication(1, 15, &[follower], 15).await;

        // The restarted SM replays all log entries from disk (applied_state returns None)
        assert!(
            new_sms[0].applied() >= 15,
            "restarted node should have >= 15 applied, got {}",
            new_sms[0].applied()
        );

        cluster.shutdown();
    });
}

#[test]
fn test_leader_restart_rejoins_as_follower() {
    run_async(async {
        let mut cluster = E2eCluster::new(3).await;
        let _sms = cluster.add_group(1).await;

        let old_leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 5 entries
        write_entries(&cluster, 1, old_leader, 5).await;
        let all_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 5, &all_ids, 10).await;

        // Stop the leader
        cluster.stop_node(old_leader).await;

        // New leader elected
        let new_leader = cluster.wait_for_any_leader(1, 15).await;
        assert_ne!(new_leader, old_leader);

        // Write 5 more
        write_entries(&cluster, 1, new_leader, 5).await;
        let survivor_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 10, &survivor_ids, 10).await;

        // Restart old leader
        let new_sms = cluster.restart_node(old_leader, &[1]).await;

        // Wait for it to catch up
        cluster.wait_for_replication(1, 10, &[old_leader], 15).await;

        assert!(
            new_sms[0].applied() >= 10,
            "restarted leader should have >= 10 applied, got {}",
            new_sms[0].applied()
        );

        cluster.shutdown();
    });
}

// ---------------------------------------------------------------------------
// 3. Membership changes
// ---------------------------------------------------------------------------

#[test]
fn test_add_learner_and_promote_to_voter() {
    run_async(async {
        let mut cluster = E2eCluster::new(3).await;
        let _sms = cluster.add_group(1).await;

        let leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 5 entries
        write_entries(&cluster, 1, leader, 5).await;
        let all_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 5, &all_ids, 10).await;

        // Add a 4th node
        let node4_sms = cluster.add_new_node(4, &[1]).await;

        // Add as learner (blocking = true waits for catch-up)
        let leader_raft = cluster.get_raft(1, leader);
        leader_raft
            .add_learner(4, openraft::impls::BasicNode::default(), true)
            .await
            .expect("add_learner failed");

        // Promote to voter
        let new_members: BTreeSet<u64> = [1, 2, 3, 4].into();
        leader_raft
            .change_membership(new_members, false)
            .await
            .expect("change_membership failed");

        // Write 5 more entries
        write_entries(&cluster, 1, leader, 5).await;

        // Wait for node 4 to apply all 10 normal entries
        wait_for_applied(&node4_sms, 10, 10).await;

        cluster.shutdown();
    });
}

#[test]
fn test_remove_voter_from_cluster() {
    run_async(async {
        let cluster = E2eCluster::new(3).await;
        let _sms = cluster.add_group(1).await;

        let leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 5 entries
        write_entries(&cluster, 1, leader, 5).await;
        let all_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 5, &all_ids, 10).await;

        // Remove node 3 from voters (retain as learner)
        // Pick a node to remove that is NOT the leader
        let remove_id = cluster
            .nodes
            .iter()
            .find(|n| n.node_id != leader)
            .unwrap()
            .node_id;
        let other_id = cluster
            .nodes
            .iter()
            .find(|n| n.node_id != leader && n.node_id != remove_id)
            .unwrap()
            .node_id;

        let leader_raft = cluster.get_raft(1, leader);
        let remaining: BTreeSet<u64> = [leader, other_id].into();
        leader_raft
            .change_membership(remaining, true)
            .await
            .expect("change_membership failed");

        // Write 5 more entries
        write_entries(&cluster, 1, leader, 5).await;

        // Wait for replication on the 2 voters
        cluster
            .wait_for_replication(1, 10, &[leader, other_id], 10)
            .await;

        cluster.shutdown();
    });
}

// ---------------------------------------------------------------------------
// 4. Snapshots
// ---------------------------------------------------------------------------

#[test]
fn test_snapshot_trigger_and_verify() {
    run_async(async {
        let cluster = E2eCluster::new(3).await;
        let _sms = cluster.add_group(1).await;

        let leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 10 entries
        write_entries(&cluster, 1, leader, 10).await;
        let all_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 10, &all_ids, 10).await;

        // Trigger snapshot
        let leader_raft = cluster.get_raft(1, leader);
        leader_raft.trigger().snapshot().await.unwrap();

        // Wait for snapshot to appear in metrics
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let m = leader_raft.metrics().borrow_watched().clone();
                if m.snapshot.is_some() {
                    return m.snapshot.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("snapshot should appear in metrics");

        cluster.shutdown();
    });
}

#[test]
fn test_snapshot_install_on_lagging_follower() {
    run_async(async {
        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 200,
                election_timeout_min: 400,
                election_timeout_max: 600,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5),
                max_in_snapshot_log_to_keep: 0,
                purge_batch_size: 1,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let mut cluster = E2eCluster::new_with_config(3, raft_cfg).await;
        let _sms = cluster.add_group(1).await;

        let leader = cluster.wait_for_any_leader(1, 10).await;

        // Write 5 entries, fully replicated
        write_entries(&cluster, 1, leader, 5).await;
        let all_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 5, &all_ids, 10).await;

        // Stop a follower
        let follower = cluster
            .nodes
            .iter()
            .find(|n| n.node_id != leader)
            .unwrap()
            .node_id;
        cluster.stop_node(follower).await;

        // Write 15 more entries while follower is down (20 total)
        write_entries(&cluster, 1, leader, 15).await;
        let survivor_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();
        cluster.wait_for_replication(1, 20, &survivor_ids, 10).await;

        // Trigger snapshot on leader
        let leader_raft = cluster.get_raft(1, leader);
        leader_raft.trigger().snapshot().await.unwrap();

        // Wait for snapshot to be built
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let m = leader_raft.metrics().borrow_watched().clone();
                if m.snapshot.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("snapshot should be built");

        // Purge old logs to force snapshot install when follower rejoins
        let metrics = leader_raft.metrics().borrow_watched().clone();
        if let Some(snap) = &metrics.snapshot {
            let purge_up_to = snap.index.saturating_sub(1);
            if purge_up_to > 0 {
                let _ = leader_raft.trigger().purge_log(purge_up_to).await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        // Restart the follower — it should receive a snapshot install
        let new_sms = cluster.restart_node(follower, &[1]).await;

        // Wait for the follower to catch up via snapshot install
        cluster.wait_for_replication(1, 20, &[follower], 15).await;

        // The snapshot carries the applied_normal count, so the SM should reflect it
        assert!(
            new_sms[0].applied() >= 15,
            "follower should have >= 15 applied after snapshot install, got {}",
            new_sms[0].applied()
        );

        cluster.shutdown();
    });
}
