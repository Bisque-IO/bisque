use crate::network::GroupNetworkFactory;
use crate::network::MultiRaftNetworkFactory;
use crate::network::MultiplexedTransport;
use crate::storage::MultiRaftLogStorage;
use dashmap::DashMap;
use openraft::Config;
use openraft::Raft;
use openraft::RaftTypeConfig;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use std::marker::Unpin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

/// Error returned when adding a Raft group fails.
#[derive(Debug)]
pub enum AddGroupError<C: RaftTypeConfig> {
    /// Failed to create or recover the group's log storage.
    Storage(std::io::Error),
    /// The Raft instance failed to initialize.
    Raft(openraft::error::Fatal<C>),
}

impl<C: RaftTypeConfig> std::fmt::Display for AddGroupError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "log storage error: {e}"),
            Self::Raft(e) => write!(f, "raft initialization error: {e}"),
        }
    }
}

impl<C: RaftTypeConfig> std::error::Error for AddGroupError<C> {}

/// Manager for multiple Raft groups.
///
/// The manager handles:
/// - Group lifecycle (add/remove groups)
/// - Shared transport layer for all groups
/// - Log storage (multiplexed across groups)
///
/// State machines are provided externally when adding groups since
/// different groups may have completely different state machine types.
pub struct MultiRaftManager<
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    S: MultiRaftLogStorage<C>,
    SM = (),
> {
    groups: DashMap<u64, Raft<C, SM>>,
    network_factory: Arc<MultiRaftNetworkFactory<C, T>>,
    storage: Arc<S>,
}

impl<C, T, S, SM> MultiRaftManager<C, T, S, SM>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    S: MultiRaftLogStorage<C>,
{
    pub fn new(transport: T, storage: S) -> Self
    where
        C::SnapshotData: AsyncRead + AsyncWrite + Unpin,
        C::Entry: Clone,
    {
        Self {
            groups: DashMap::new(),
            network_factory: Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport))),
            storage: Arc::new(storage),
        }
    }

    /// Add a new Raft group with the provided state machine.
    ///
    /// Each group can have its own state machine type/implementation.
    /// The log storage is obtained from the shared multiplexed storage.
    pub async fn add_group(
        &self,
        group_id: u64,
        node_id: C::NodeId,
        raft_config: Arc<Config>,
        state_machine: SM,
    ) -> Result<Raft<C, SM>, AddGroupError<C>>
    where
        C::SnapshotData: AsyncRead + AsyncWrite + Unpin,
        C::Entry: Clone,
        SM: RaftStateMachine<C>,
    {
        let group_factory = GroupNetworkFactory::new(self.network_factory.clone(), group_id);
        let log_store = self
            .storage
            .get_log_storage(group_id)
            .await
            .map_err(AddGroupError::Storage)?;

        let raft = Raft::new(
            node_id,
            raft_config,
            group_factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(AddGroupError::Raft)?;

        self.groups.insert(group_id, raft.clone());
        Ok(raft)
    }

    /// Add a new Raft group with the provided state machine and storage.
    ///
    /// Each group can have its own state machine type/implementation.
    /// The log storage is provided directly instead of from the shared storage.
    pub async fn add_group_with_storage<LS>(
        &self,
        group_id: u64,
        node_id: C::NodeId,
        raft_config: Arc<Config>,
        state_machine: SM,
        log_store: LS,
    ) -> Result<Raft<C, SM>, AddGroupError<C>>
    where
        C::SnapshotData: AsyncRead + AsyncWrite + Unpin,
        C::Entry: Clone,
        SM: RaftStateMachine<C>,
        LS: RaftLogStorage<C>,
    {
        let group_factory = GroupNetworkFactory::new(self.network_factory.clone(), group_id);

        let raft = Raft::new(
            node_id,
            raft_config,
            group_factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(AddGroupError::Raft)?;

        self.groups.insert(group_id, raft.clone());
        Ok(raft)
    }

    /// Get a handle to an existing group
    pub fn get_group(&self, group_id: u64) -> Option<Raft<C, SM>> {
        self.groups.get(&group_id).map(|r| r.clone())
    }

    /// Remove a group, shutting down its Raft instance
    pub async fn remove_group(&self, group_id: u64) {
        if let Some((_, raft)) = self.groups.remove(&group_id) {
            let _ = raft.shutdown().await;
        }
        // Also remove from storage
        self.storage.remove_group(group_id);
    }

    /// Get all active group IDs
    pub fn group_ids(&self) -> Vec<u64> {
        self.groups.iter().map(|e| *e.key()).collect()
    }

    /// Get the number of active groups
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Shut down all Raft groups and remove them from the manager.
    pub async fn shutdown_all(&self) {
        let groups: Vec<(u64, Raft<C, SM>)> = self
            .groups
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        for (group_id, raft) in groups {
            let _ = raft.shutdown().await;
            self.groups.remove(&group_id);
            self.storage.remove_group(group_id);
        }
    }

    /// Get a reference to the underlying storage.
    ///
    /// Useful for calling `stop()` after `shutdown_all()` to release
    /// background threads and MDBX databases.
    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    /// Get the purge floor handle for a specific group.
    ///
    /// Returns `None` if the group's log storage has not been initialized yet.
    /// Call after `add_group` or after manually initializing the group's storage.
    pub fn get_purge_floor(
        &self,
        group_id: u64,
    ) -> Option<std::sync::Arc<std::sync::atomic::AtomicU64>> {
        self.storage.get_purge_floor(group_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::run_async;
    use crate::type_config::{
        BisqueCommittedLeaderId, BisqueLeaderId, BisqueNodeId, ManiacRaftTypeConfig,
    };
    use dashmap::DashMap;
    use openraft::error::{InstallSnapshotError, RPCError, RaftError};
    use openraft::impls::BasicNode;
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine};
    use openraft::{LogId, OptionalSend};
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    // Concrete type aliases matching the openraft 0.10 degenericized API
    type TestEntry =
        openraft::impls::Entry<BisqueCommittedLeaderId, TestData, BisqueNodeId, BasicNode>;
    type TestVote = openraft::impls::Vote<BisqueLeaderId>;
    type TestLogId = LogId<BisqueCommittedLeaderId>;
    type TestStoredMembership =
        openraft::StoredMembership<BisqueCommittedLeaderId, BisqueNodeId, BasicNode>;
    type TestSnapshotMeta =
        openraft::storage::SnapshotMeta<BisqueCommittedLeaderId, BisqueNodeId, BasicNode>;
    type TestSnapshot = openraft::storage::Snapshot<
        BisqueCommittedLeaderId,
        BisqueNodeId,
        BasicNode,
        std::io::Cursor<Vec<u8>>,
    >;
    type TestManager =
        MultiRaftManager<TestConfig, FakeTransport, InMemoryMultiStorage, TestStateMachine>;

    // Test data type that implements AppData
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct TestData(Vec<u8>);

    impl fmt::Display for TestData {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestData({} bytes)", self.0.len())
        }
    }

    impl From<Vec<u8>> for TestData {
        fn from(v: Vec<u8>) -> Self {
            TestData(v)
        }
    }

    impl From<TestData> for Vec<u8> {
        fn from(t: TestData) -> Self {
            t.0
        }
    }

    type TestConfig = ManiacRaftTypeConfig<TestData, ()>;

    // In-memory log storage for testing
    #[derive(Clone)]
    struct InMemoryLogStorage {
        log: Arc<DashMap<u64, TestEntry>>,
        vote: Arc<parking_lot::RwLock<Option<TestVote>>>,
        last_purged: Arc<AtomicU64>,
    }

    impl InMemoryLogStorage {
        fn new() -> Self {
            Self {
                log: Arc::new(DashMap::new()),
                vote: Arc::new(parking_lot::RwLock::new(None)),
                last_purged: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl RaftLogStorage<TestConfig> for InMemoryLogStorage {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<TestConfig>, std::io::Error> {
            let last_log_id = self
                .log
                .iter()
                .max_by_key(|e| *e.key())
                .map(|e| e.value().log_id);
            let last_purged_index = self.last_purged.load(Ordering::Relaxed);
            let last_purged_log_id = if last_purged_index > 0 {
                self.log.get(&last_purged_index).map(|e| e.value().log_id)
            } else {
                None
            };

            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }

        async fn save_vote(&mut self, vote: &TestVote) -> Result<(), std::io::Error> {
            *self.vote.write() = Some(vote.clone());
            Ok(())
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: IOFlushed<TestConfig>,
        ) -> Result<(), std::io::Error>
        where
            I: IntoIterator<Item = TestEntry> + Send,
        {
            for entry in entries {
                self.log.insert(entry.log_id.index, entry);
            }
            callback.io_completed(Ok(()));
            Ok(())
        }

        async fn truncate_after(&mut self, after: Option<TestLogId>) -> Result<(), std::io::Error> {
            match after {
                Some(log_id) => {
                    let keys: Vec<u64> = self
                        .log
                        .iter()
                        .filter(|e| e.key() > &log_id.index)
                        .map(|e| *e.key())
                        .collect();
                    for key in keys {
                        self.log.remove(&key);
                    }
                }
                None => {
                    self.log.clear();
                }
            }
            Ok(())
        }

        async fn purge(&mut self, log_id: TestLogId) -> Result<(), std::io::Error> {
            let keys: Vec<u64> = self
                .log
                .iter()
                .filter(|e| e.key() <= &log_id.index)
                .map(|e| *e.key())
                .collect();
            for key in keys {
                self.log.remove(&key);
            }
            self.last_purged.store(log_id.index, Ordering::Relaxed);
            Ok(())
        }
    }

    impl RaftLogReader<TestConfig> for InMemoryLogStorage {
        async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Send>(
            &mut self,
            range: RB,
        ) -> Result<Vec<TestEntry>, std::io::Error> {
            let entries: Vec<_> = self
                .log
                .iter()
                .filter(|e| range.contains(e.key()))
                .map(|e| e.value().clone())
                .collect();
            Ok(entries)
        }

        async fn read_vote(&mut self) -> Result<Option<TestVote>, std::io::Error> {
            Ok(self.vote.read().clone())
        }
    }

    // In-memory multi-raft storage
    struct InMemoryMultiStorage {
        storages: Arc<DashMap<u64, InMemoryLogStorage>>,
    }

    impl InMemoryMultiStorage {
        fn new() -> Self {
            Self {
                storages: Arc::new(DashMap::new()),
            }
        }
    }

    impl MultiRaftLogStorage<TestConfig> for InMemoryMultiStorage {
        type GroupLogStorage = InMemoryLogStorage;

        async fn get_log_storage(&self, group_id: u64) -> std::io::Result<Self::GroupLogStorage> {
            Ok(self
                .storages
                .entry(group_id)
                .or_insert_with(InMemoryLogStorage::new)
                .clone())
        }

        fn remove_group(&self, group_id: u64) {
            self.storages.remove(&group_id);
        }

        fn group_ids(&self) -> Vec<u64> {
            self.storages.iter().map(|e| *e.key()).collect()
        }

        fn get_purge_floor(&self, _group_id: u64) -> Option<Arc<AtomicU64>> {
            None
        }

        fn get_pin_ceiling(&self, _group_id: u64) -> Option<Arc<AtomicU64>> {
            None
        }

        fn get_purged_segments(&self, _group_id: u64) -> Option<Arc<parking_lot::Mutex<Vec<u64>>>> {
            None
        }
    }

    // Simple state machine for testing
    #[derive(Clone)]
    struct TestStateMachine {
        data: Arc<parking_lot::RwLock<BTreeMap<String, Vec<u8>>>>,
    }

    impl TestStateMachine {
        fn new() -> Self {
            Self {
                data: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            }
        }
    }

    impl RaftStateMachine<TestConfig> for TestStateMachine {
        type SnapshotBuilder = Self;

        async fn applied_state(
            &mut self,
        ) -> Result<(Option<TestLogId>, TestStoredMembership), std::io::Error> {
            Ok((None, openraft::StoredMembership::default()))
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
                if let openraft::EntryPayload::Normal(data) = &entry.payload {
                    let key = format!("key_{}", entry.log_id.index);
                    self.data.write().insert(key, data.clone().into());
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
            _meta: &TestSnapshotMeta,
            snapshot: std::io::Cursor<Vec<u8>>,
        ) -> Result<(), std::io::Error> {
            let _ = snapshot.into_inner();
            Ok(())
        }

        async fn get_current_snapshot(&mut self) -> Result<Option<TestSnapshot>, std::io::Error> {
            Ok(None)
        }
    }

    impl openraft::RaftSnapshotBuilder<TestConfig> for TestStateMachine {
        async fn build_snapshot(&mut self) -> Result<TestSnapshot, std::io::Error> {
            let data = self.data.read();
            let content = bincode::serde::encode_to_vec(&*data, bincode::config::standard())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let meta = openraft::storage::SnapshotMeta {
                last_log_id: None,
                last_membership: openraft::StoredMembership::default(),
                snapshot_id: "test-snapshot".to_string(),
            };
            Ok(openraft::storage::Snapshot {
                meta,
                snapshot: std::io::Cursor::new(content),
            })
        }
    }

    // Fake transport for manager tests
    struct FakeTransport;

    impl MultiplexedTransport<TestConfig> for FakeTransport {
        async fn send_append_entries(
            &self,
            _target: BisqueNodeId,
            _group_id: u64,
            _rpc: AppendEntriesRequest<TestConfig>,
        ) -> Result<AppendEntriesResponse<TestConfig>, RPCError<TestConfig, RaftError<TestConfig>>>
        {
            Ok(AppendEntriesResponse::Success)
        }

        async fn send_vote(
            &self,
            _target: BisqueNodeId,
            _group_id: u64,
            _rpc: VoteRequest<TestConfig>,
        ) -> Result<VoteResponse<TestConfig>, RPCError<TestConfig, RaftError<TestConfig>>> {
            Ok(VoteResponse {
                vote: TestVote::new(1, 1),
                vote_granted: true,
                last_log_id: None,
            })
        }

        async fn send_install_snapshot(
            &self,
            _target: BisqueNodeId,
            _group_id: u64,
            _rpc: InstallSnapshotRequest<TestConfig>,
        ) -> Result<
            InstallSnapshotResponse<TestConfig>,
            RPCError<TestConfig, RaftError<TestConfig, InstallSnapshotError>>,
        > {
            Ok(InstallSnapshotResponse {
                vote: TestVote::new(1, 1),
            })
        }
    }

    #[test]
    fn test_add_and_get_group() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());
            let state_machine = TestStateMachine::new();

            let _raft = manager
                .add_group(1, 1, raft_config, state_machine)
                .await
                .expect("Failed to add group");

            assert!(manager.get_group(1).is_some());
            assert_eq!(manager.group_count(), 1);
            assert_eq!(manager.group_ids(), vec![1]);
        });
    }

    #[test]
    fn test_multiple_groups_isolation() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            let _raft1 = manager
                .add_group(1, 1, raft_config.clone(), TestStateMachine::new())
                .await
                .expect("Failed to add group 1");

            let _raft2 = manager
                .add_group(2, 1, raft_config.clone(), TestStateMachine::new())
                .await
                .expect("Failed to add group 2");

            assert_eq!(manager.group_count(), 2);
            let mut group_ids = manager.group_ids();
            group_ids.sort();
            assert_eq!(group_ids, vec![1, 2]);
        });
    }

    #[test]
    fn test_remove_group() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            let _raft = manager
                .add_group(1, 1, raft_config, TestStateMachine::new())
                .await
                .expect("Failed to add group");

            assert_eq!(manager.group_count(), 1);

            manager.remove_group(1).await;

            assert_eq!(manager.group_count(), 0);
            assert!(manager.get_group(1).is_none());
        });
    }

    // ===================================================================
    // Reliability — additional coverage
    // ===================================================================

    #[test]
    fn test_get_nonexistent_group() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            assert!(manager.get_group(42).is_none());
            assert!(manager.get_group(0).is_none());
            assert!(manager.get_group(u64::MAX).is_none());
        });
    }

    #[test]
    fn test_remove_nonexistent_group() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            // Should not panic
            manager.remove_group(999).await;
            assert_eq!(manager.group_count(), 0);
        });
    }

    #[test]
    fn test_group_ids_empty_manager() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            assert!(manager.group_ids().is_empty());
            assert_eq!(manager.group_count(), 0);
        });
    }

    #[test]
    fn test_readd_group_after_remove() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            let _raft1 = manager
                .add_group(1, 1, raft_config.clone(), TestStateMachine::new())
                .await
                .unwrap();
            assert_eq!(manager.group_count(), 1);

            manager.remove_group(1).await;
            assert_eq!(manager.group_count(), 0);

            // Re-add the same group
            let _raft2 = manager
                .add_group(1, 1, raft_config, TestStateMachine::new())
                .await
                .unwrap();
            assert_eq!(manager.group_count(), 1);
            assert!(manager.get_group(1).is_some());
        });
    }

    #[test]
    fn test_get_purge_floor() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            // InMemoryMultiStorage always returns None for purge_floor
            assert!(manager.get_purge_floor(0).is_none());
            assert!(manager.get_purge_floor(42).is_none());
        });
    }

    #[test]
    fn test_many_groups() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            for i in 0..50 {
                manager
                    .add_group(i, 1, raft_config.clone(), TestStateMachine::new())
                    .await
                    .unwrap();
            }

            assert_eq!(manager.group_count(), 50);
            let mut ids = manager.group_ids();
            ids.sort();
            assert_eq!(ids, (0..50).collect::<Vec<_>>());
        });
    }

    #[test]
    fn test_remove_all_groups() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            for i in 0..5 {
                manager
                    .add_group(i, 1, raft_config.clone(), TestStateMachine::new())
                    .await
                    .unwrap();
            }
            assert_eq!(manager.group_count(), 5);

            for i in 0..5 {
                manager.remove_group(i).await;
            }
            assert_eq!(manager.group_count(), 0);
            assert!(manager.group_ids().is_empty());
        });
    }

    #[test]
    fn test_add_group_same_id_overwrites() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            let _raft1 = manager
                .add_group(1, 1, raft_config.clone(), TestStateMachine::new())
                .await
                .unwrap();

            // Adding same ID again — overwrites the previous
            let _raft2 = manager
                .add_group(1, 2, raft_config, TestStateMachine::new())
                .await
                .unwrap();

            assert_eq!(manager.group_count(), 1);
            assert!(manager.get_group(1).is_some());
        });
    }

    #[test]
    fn test_add_group_with_different_configs() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let mut config1 = openraft::Config::default();
            config1.heartbeat_interval = 100;
            let mut config2 = openraft::Config::default();
            config2.heartbeat_interval = 500;

            let _raft1 = manager
                .add_group(1, 1, Arc::new(config1), TestStateMachine::new())
                .await
                .unwrap();
            let _raft2 = manager
                .add_group(2, 1, Arc::new(config2), TestStateMachine::new())
                .await
                .unwrap();

            assert_eq!(manager.group_count(), 2);
        });
    }

    // ===================================================================
    // Concurrency
    // ===================================================================

    #[test]
    fn test_concurrent_add_remove_groups() {
        use std::sync::Arc;
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            // Add groups 0..10
            let mut handles = Vec::new();
            for i in 0..10u64 {
                let m = manager.clone();
                let cfg = raft_config.clone();
                handles.push(tokio::spawn(async move {
                    m.add_group(i, 1, cfg, TestStateMachine::new())
                        .await
                        .unwrap();
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            assert_eq!(manager.group_count(), 10);

            // Remove odd groups concurrently
            let mut handles = Vec::new();
            for i in (1..10u64).step_by(2) {
                let m = manager.clone();
                handles.push(tokio::spawn(async move {
                    m.remove_group(i).await;
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            assert_eq!(manager.group_count(), 5);
        });
    }

    #[test]
    fn test_concurrent_get_during_modification() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            // Add a group first
            manager
                .add_group(1, 1, raft_config.clone(), TestStateMachine::new())
                .await
                .unwrap();

            // Concurrent gets and adds should not panic
            let mut handles = Vec::new();
            for i in 2..20u64 {
                let m = manager.clone();
                let cfg = raft_config.clone();
                handles.push(tokio::spawn(async move {
                    let _ = m.get_group(1);
                    let _ = m.group_ids();
                    let _ = m.group_count();
                    m.add_group(i, 1, cfg, TestStateMachine::new())
                        .await
                        .unwrap();
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        });
    }

    // ===================================================================
    // Edge cases
    // ===================================================================

    #[test]
    fn test_manager_with_zero_groups_operations() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            // All operations on empty manager should work
            assert!(manager.get_group(0).is_none());
            assert_eq!(manager.group_count(), 0);
            assert!(manager.group_ids().is_empty());
            assert!(manager.get_purge_floor(0).is_none());
            manager.remove_group(0).await; // no-op, no panic
        });
    }

    #[test]
    fn test_group_ids_sorted_consistency() {
        run_async(async {
            let transport = FakeTransport;
            let storage = InMemoryMultiStorage::new();
            let manager = Arc::new(TestManager::new(transport, storage));

            let raft_config = Arc::new(openraft::Config::default());

            // Add groups in random order
            for &id in &[5, 2, 8, 1, 9, 3] {
                manager
                    .add_group(id, 1, raft_config.clone(), TestStateMachine::new())
                    .await
                    .unwrap();
            }

            let mut ids = manager.group_ids();
            ids.sort();
            assert_eq!(ids, vec![1, 2, 3, 5, 8, 9]);
        });
    }
}
