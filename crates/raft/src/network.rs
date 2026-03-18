use openraft::OptionalSend;
use openraft::OptionalSync;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::StreamingError;
use openraft::error::decompose::DecomposeResult;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::Snapshot;
use openraft::{ErrorSubject, ErrorVerb};
use std::borrow::Cow;
use std::future::Future;
use std::marker::Unpin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait MultiplexedTransport<C: RaftTypeConfig>: OptionalSend + OptionalSync + 'static {
    fn send_append_entries(
        &self,
        target: C::NodeId,
        group_id: u64,
        rpc: AppendEntriesRequest<C>,
    ) -> impl Future<Output = Result<AppendEntriesResponse<C>, RPCError<C, RaftError<C>>>> + OptionalSend;

    fn send_vote(
        &self,
        target: C::NodeId,
        group_id: u64,
        rpc: VoteRequest<C>,
    ) -> impl Future<Output = Result<VoteResponse<C>, RPCError<C, RaftError<C>>>> + OptionalSend;

    fn send_install_snapshot(
        &self,
        target: C::NodeId,
        group_id: u64,
        rpc: InstallSnapshotRequest<C>,
    ) -> impl Future<
        Output = Result<
            InstallSnapshotResponse<C>,
            RPCError<C, RaftError<C, InstallSnapshotError>>,
        >,
    > + OptionalSend;
}

pub struct MultiRaftNetworkFactory<C: RaftTypeConfig, T: MultiplexedTransport<C>> {
    transport: Arc<T>,
    _phantom: std::marker::PhantomData<C>,
}

/// Error type for batch operations using Cow to avoid allocations for static messages
#[derive(Debug, Clone)]
struct BatchError(Cow<'static, str>);

impl BatchError {
    /// Create a BatchError with a static string (no allocation)
    const fn new_static(msg: &'static str) -> Self {
        Self(Cow::Borrowed(msg))
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BatchError {}

impl<C, T> MultiRaftNetworkFactory<C, T>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    C::SnapshotData: AsyncRead + AsyncWrite + Unpin,
    C::Entry: Clone,
{
    pub fn new(transport: Arc<T>) -> Self {
        Self {
            transport,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C, T> RaftNetworkFactory<C> for MultiRaftNetworkFactory<C, T>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    C::SnapshotData: AsyncRead + AsyncWrite + Unpin,
    C::Entry: Clone,
{
    type Network = MultiRaftNetwork<C, T>;

    async fn new_client(&mut self, _target: C::NodeId, _node: &C::Node) -> Self::Network {
        // This trait impl exists only to satisfy OpenRaft's type requirements.
        // All actual networking goes through GroupNetworkFactory, which wraps
        // this factory with a group_id. Reaching this code is a programming error.
        unreachable!("MultiRaftNetworkFactory cannot be used directly. Use GroupNetworkFactory.");
    }
}

pub struct GroupNetworkFactory<C: RaftTypeConfig, T: MultiplexedTransport<C>> {
    parent: Arc<MultiRaftNetworkFactory<C, T>>,
    group_id: u64,
}

impl<C, T> GroupNetworkFactory<C, T>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
{
    pub fn new(parent: Arc<MultiRaftNetworkFactory<C, T>>, group_id: u64) -> Self {
        Self { parent, group_id }
    }
}

impl<C, T> RaftNetworkFactory<C> for GroupNetworkFactory<C, T>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    C::SnapshotData: AsyncRead + AsyncWrite + Unpin,
    C::Entry: Clone,
{
    type Network = MultiRaftNetwork<C, T>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        MultiRaftNetwork {
            transport: self.parent.transport.clone(),
            target,
            group_id: self.group_id,
        }
    }
}

pub struct MultiRaftNetwork<C: RaftTypeConfig, T: MultiplexedTransport<C>> {
    transport: Arc<T>,
    target: C::NodeId,
    group_id: u64,
}

impl<C, T> RaftNetworkV2<C> for MultiRaftNetwork<C, T>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    C::SnapshotData: AsyncRead + AsyncWrite + Unpin,
    C::Entry: Clone,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        self.transport
            .send_append_entries(self.target.clone(), self.group_id, rpc)
            .await
            .decompose_infallible()
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        self.transport
            .send_vote(self.target.clone(), self.group_id, rpc)
            .await
            .decompose_infallible()
    }

    async fn full_snapshot(
        &mut self,
        vote: openraft::type_config::alias::VoteOf<C>,
        snapshot: Snapshot<C>,
        _cancel: impl Future<Output = openraft::error::ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let mut offset = 0u64;
        let mut snapshot_data = snapshot.snapshot;
        let snapshot_meta = snapshot.meta;
        let chunk_size = option.snapshot_chunk_size().unwrap_or(1024 * 1024);

        // Reusable read buffer - allocated once, reused across loop iterations
        let mut buf = vec![0u8; chunk_size];

        loop {
            use tokio::io::AsyncReadExt;
            let n = snapshot_data.read(&mut buf).await.map_err(|e| {
                StreamingError::from(StorageError::from_io_error(
                    ErrorSubject::Snapshot(Some(snapshot_meta.signature())),
                    ErrorVerb::Read,
                    e,
                ))
            })?;

            if n == 0 {
                // EOF - send final empty chunk with done=true
                let req = InstallSnapshotRequest {
                    vote: vote.clone(),
                    meta: snapshot_meta.clone(),
                    offset,
                    data: Vec::new(),
                    done: true,
                };
                let res = match self
                    .transport
                    .send_install_snapshot(self.target.clone(), self.group_id, req)
                    .await
                    .decompose()
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(_snapshot_err)) => {
                        // InstallSnapshotError is received - convert to network error
                        return Err(StreamingError::Network(openraft::error::NetworkError::new(
                            &BatchError::new_static("Snapshot rejected by remote"),
                        )));
                    }
                    Err(rpc_err) => return Err(StreamingError::from(rpc_err)),
                };
                return Ok(SnapshotResponse::new(res.vote));
            }

            let req = InstallSnapshotRequest {
                vote: vote.clone(),
                meta: snapshot_meta.clone(),
                offset,
                data: buf[..n].to_vec(),
                done: false,
            };

            match self
                .transport
                .send_install_snapshot(self.target.clone(), self.group_id, req)
                .await
                .decompose()
            {
                Ok(Ok(_)) => {}
                Ok(Err(_snapshot_err)) => {
                    return Err(StreamingError::Network(openraft::error::NetworkError::new(
                        &BatchError::new_static("Snapshot rejected by remote"),
                    )));
                }
                Err(rpc_err) => return Err(StreamingError::from(rpc_err)),
            }

            offset += n as u64;
        }
    }

    fn backoff(&self) -> openraft::network::Backoff {
        // Exponential backoff: 500ms, 1s, 2s, 4s, 8s, 16s, then cap at 30s.
        let iter = (0u32..6)
            .map(|i| Duration::from_millis(500 * 2u64.pow(i)))
            .chain(std::iter::repeat(Duration::from_secs(30)));
        openraft::network::Backoff::new(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::run_async;
    use crate::type_config::ManiacRaftTypeConfig;
    use dashmap::DashMap;
    use openraft::error::{RPCError, RaftError};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    // Simple test data type that implements AppData
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct TestData(String);

    impl std::fmt::Display for TestData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl From<Vec<u8>> for TestData {
        fn from(v: Vec<u8>) -> Self {
            TestData(String::from_utf8_lossy(&v).to_string())
        }
    }

    type TestConfig = ManiacRaftTypeConfig<TestData, ()>;

    /// Fake transport that records calls
    #[derive(Clone)]
    struct FakeTransport {
        append_entries_calls: Arc<DashMap<(u64, u64), Vec<AppendEntriesRequest<TestConfig>>>>,
        vote_calls: Arc<DashMap<(u64, u64), Vec<VoteRequest<TestConfig>>>>,
        call_count: Arc<AtomicU64>,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self {
                append_entries_calls: Arc::new(DashMap::new()),
                vote_calls: Arc::new(DashMap::new()),
                call_count: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl MultiplexedTransport<TestConfig> for FakeTransport {
        async fn send_append_entries(
            &self,
            target: u64,
            group_id: u64,
            rpc: AppendEntriesRequest<TestConfig>,
        ) -> Result<AppendEntriesResponse<TestConfig>, RPCError<TestConfig, RaftError<TestConfig>>>
        {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let key = (target, group_id);
            let rpc_clone = AppendEntriesRequest {
                vote: rpc.vote.clone(),
                prev_log_id: rpc.prev_log_id.clone(),
                entries: rpc.entries.clone(),
                leader_commit: rpc.leader_commit.clone(),
            };
            self.append_entries_calls
                .entry(key)
                .or_insert_with(Vec::new)
                .push(rpc_clone);
            Ok(AppendEntriesResponse::Success)
        }

        async fn send_vote(
            &self,
            target: u64,
            group_id: u64,
            rpc: VoteRequest<TestConfig>,
        ) -> Result<VoteResponse<TestConfig>, RPCError<TestConfig, RaftError<TestConfig>>> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let key = (target, group_id);
            self.vote_calls
                .entry(key)
                .or_insert_with(Vec::new)
                .push(rpc);
            Ok(VoteResponse {
                vote: openraft::impls::Vote::new(1, 1),
                vote_granted: true,
                last_log_id: None,
            })
        }

        async fn send_install_snapshot(
            &self,
            _target: u64,
            _group_id: u64,
            _rpc: InstallSnapshotRequest<TestConfig>,
        ) -> Result<
            InstallSnapshotResponse<TestConfig>,
            RPCError<TestConfig, RaftError<TestConfig, InstallSnapshotError>>,
        > {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            Ok(InstallSnapshotResponse {
                vote: openraft::impls::Vote::new(1, 1),
            })
        }
    }

    fn make_heartbeat_request(term: u64, node_id: u64) -> AppendEntriesRequest<TestConfig> {
        AppendEntriesRequest {
            vote: openraft::impls::Vote::new(term, node_id),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        }
    }

    fn make_append_request(
        term: u64,
        node_id: u64,
        data: String,
    ) -> AppendEntriesRequest<TestConfig> {
        use openraft::LogId;
        use openraft::entry::RaftEntry;
        let leader_id = openraft::impls::leader_id_adv::LeaderId { term, node_id };
        AppendEntriesRequest {
            vote: openraft::impls::Vote::new(term, node_id),
            prev_log_id: None,
            entries: vec![openraft::impls::Entry::new(
                LogId::new(leader_id, 1),
                openraft::EntryPayload::Normal(TestData(data)),
            )],
            leader_commit: None,
        }
    }

    #[test]
    fn test_heartbeat_direct_transport() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut network = group_factory.new_client(1, &node).await;

            // Empty entries (heartbeat) should go directly through transport
            let heartbeat = make_heartbeat_request(1, 1);
            let result = network
                .append_entries(heartbeat, RPCOption::new(Duration::from_secs(30)))
                .await;
            assert!(result.is_ok());

            // Should have gone directly to transport
            let key = (1, 1);
            assert!(transport.append_entries_calls.get(&key).is_some());
            let calls = transport.append_entries_calls.get(&key).unwrap();
            assert_eq!(calls.len(), 1);
            assert!(calls[0].entries.is_empty());
        });
    }

    #[test]
    fn test_append_entries_direct_transport() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut network = group_factory.new_client(1, &node).await;

            let append = make_append_request(1, 1, "test data".to_string());
            let result = network
                .append_entries(append, RPCOption::new(Duration::from_secs(30)))
                .await;
            assert!(result.is_ok());

            let key = (1, 1);
            let calls = transport.append_entries_calls.get(&key).unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].entries.len(), 1);
        });
    }

    #[test]
    fn test_vote_request_direct_transport() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut network = group_factory.new_client(1, &node).await;

            let vote_req = VoteRequest {
                vote: openraft::impls::Vote::new(1, 1),
                last_log_id: None,
            };
            let result = network
                .vote(vote_req, RPCOption::new(Duration::from_secs(30)))
                .await;
            assert!(result.is_ok());
            assert!(result.unwrap().vote_granted);

            let key = (1, 1);
            let calls = transport.vote_calls.get(&key).unwrap();
            assert_eq!(calls.len(), 1);
        });
    }

    #[test]
    fn test_multiple_targets_isolation() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };

            let mut gf1 = GroupNetworkFactory::new(factory.clone(), 1);
            let mut net1 = gf1.new_client(1, &node).await;

            let mut gf2 = GroupNetworkFactory::new(factory.clone(), 1);
            let mut net2 = gf2.new_client(2, &node).await;

            let hb1 = make_heartbeat_request(1, 1);
            let hb2 = make_heartbeat_request(1, 1);

            let (r1, r2) = futures::future::join(
                net1.append_entries(hb1, RPCOption::new(Duration::from_secs(30))),
                net2.append_entries(hb2, RPCOption::new(Duration::from_secs(30))),
            )
            .await;

            assert!(r1.is_ok());
            assert!(r2.is_ok());

            // Each target should have its own call
            assert!(transport.append_entries_calls.get(&(1, 1)).is_some());
            assert!(transport.append_entries_calls.get(&(2, 1)).is_some());
        });
    }

    #[test]
    fn test_concurrent_groups_same_target() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };

            let mut handles = Vec::new();
            for group_id in 1..=10 {
                let factory = factory.clone();
                let node = node.clone();
                let handle = tokio::spawn(async move {
                    let mut gf = GroupNetworkFactory::new(factory, group_id);
                    let mut net = gf.new_client(1, &node).await;
                    let hb = make_heartbeat_request(1, 1);
                    net.append_entries(hb, RPCOption::new(Duration::from_secs(30)))
                        .await
                });
                handles.push(handle);
            }

            for handle in handles {
                let result = handle.await.unwrap();
                assert!(result.is_ok());
            }

            // Each group should have its own call to the transport
            assert_eq!(transport.call_count.load(Ordering::Relaxed), 10);
        });
    }

    #[test]
    fn test_install_snapshot_success() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut network = group_factory.new_client(1, &node).await;

            let snapshot_data: Vec<u8> = vec![1, 2, 3, 4, 5];
            let cursor = std::io::Cursor::new(snapshot_data);
            let snapshot_meta = openraft::storage::SnapshotMeta::<TestConfig> {
                last_log_id: None,
                last_membership: openraft::StoredMembership::default(),
                snapshot_id: "test-snapshot".to_string(),
            };

            let snapshot = openraft::storage::Snapshot::<TestConfig> {
                meta: snapshot_meta,
                snapshot: cursor,
            };

            let vote = openraft::impls::Vote::new(1, 1);
            let cancel_fut =
                async { std::future::pending::<openraft::error::ReplicationClosed>().await };

            let result = network
                .full_snapshot(
                    vote,
                    snapshot,
                    cancel_fut,
                    RPCOption::new(Duration::from_secs(30)),
                )
                .await;

            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_install_snapshot_rejection() {
        run_async(async {
            struct RejectingTransport {
                call_count: Arc<AtomicU64>,
            }

            impl Clone for RejectingTransport {
                fn clone(&self) -> Self {
                    Self {
                        call_count: self.call_count.clone(),
                    }
                }
            }

            impl MultiplexedTransport<TestConfig> for RejectingTransport {
                async fn send_append_entries(
                    &self,
                    _target: u64,
                    _group_id: u64,
                    _rpc: AppendEntriesRequest<TestConfig>,
                ) -> Result<
                    AppendEntriesResponse<TestConfig>,
                    RPCError<TestConfig, RaftError<TestConfig>>,
                > {
                    Ok(AppendEntriesResponse::Success)
                }

                async fn send_vote(
                    &self,
                    _target: u64,
                    _group_id: u64,
                    _rpc: VoteRequest<TestConfig>,
                ) -> Result<VoteResponse<TestConfig>, RPCError<TestConfig, RaftError<TestConfig>>>
                {
                    Ok(VoteResponse {
                        vote: openraft::impls::Vote::new(1, 1),
                        vote_granted: true,
                        last_log_id: None,
                    })
                }

                async fn send_install_snapshot(
                    &self,
                    _target: u64,
                    _group_id: u64,
                    _rpc: InstallSnapshotRequest<TestConfig>,
                ) -> Result<
                    InstallSnapshotResponse<TestConfig>,
                    RPCError<TestConfig, RaftError<TestConfig, InstallSnapshotError>>,
                > {
                    self.call_count.fetch_add(1, Ordering::Relaxed);
                    Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(std::io::ErrorKind::Other, "snapshot rejected"),
                    )))
                }
            }

            let transport = RejectingTransport {
                call_count: Arc::new(AtomicU64::new(0)),
            };
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut network = group_factory.new_client(1, &node).await;

            let snapshot_data: Vec<u8> = vec![1, 2, 3, 4, 5];
            let cursor = std::io::Cursor::new(snapshot_data);
            let snapshot_meta = openraft::storage::SnapshotMeta::<TestConfig> {
                last_log_id: None,
                last_membership: openraft::StoredMembership::default(),
                snapshot_id: "test-snapshot".to_string(),
            };

            let snapshot = openraft::storage::Snapshot {
                meta: snapshot_meta,
                snapshot: cursor,
            };

            let vote = openraft::impls::Vote::new(1, 1);
            let cancel_fut =
                async { std::future::pending::<openraft::error::ReplicationClosed>().await };

            let result = network
                .full_snapshot(
                    vote,
                    snapshot,
                    cancel_fut,
                    RPCOption::new(Duration::from_secs(30)),
                )
                .await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_backoff_configuration() {
        let transport = FakeTransport::new();
        let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport)));

        let node = openraft::impls::BasicNode {
            addr: "127.0.0.1:5000".to_string(),
        };

        run_async(async move {
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);
            let network = group_factory.new_client(1, &node).await;
            let _backoff = network.backoff();
        });
    }

    #[test]
    fn test_factory_construction() {
        let transport = FakeTransport::new();
        let _factory = MultiRaftNetworkFactory::new(Arc::new(transport));
    }

    #[test]
    fn test_group_network_factory_new() {
        let transport = FakeTransport::new();
        let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport)));
        let group_factory = GroupNetworkFactory::new(factory, 42);
        assert_eq!(group_factory.group_id, 42);
    }

    #[test]
    fn test_concurrent_targets_stress() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };

            let mut handles = Vec::new();
            for target_id in 1..=5 {
                for group_id in 1..=10 {
                    let factory = factory.clone();
                    let node = node.clone();
                    let handle = tokio::spawn(async move {
                        let mut gf = GroupNetworkFactory::new(factory, group_id);
                        let mut net = gf.new_client(target_id, &node).await;
                        let hb = make_heartbeat_request(1, 1);
                        net.append_entries(hb, RPCOption::new(Duration::from_secs(30)))
                            .await
                    });
                    handles.push((target_id, group_id, handle));
                }
            }

            for (target_id, group_id, handle) in handles {
                let result = handle.await.unwrap();
                assert!(
                    result.is_ok(),
                    "Target {} Group {} failed",
                    target_id,
                    group_id
                );
            }

            // All 50 calls should have gone through
            assert_eq!(transport.call_count.load(Ordering::Relaxed), 50);
        });
    }

    // ===================================================================
    // Network factory — additional coverage
    // ===================================================================

    #[test]
    fn test_group_network_factory_routes_correctly() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };

            // Group 1
            let mut gf1 = GroupNetworkFactory::new(factory.clone(), 1);
            let mut net1 = gf1.new_client(10, &node).await;
            let hb = make_heartbeat_request(1, 1);
            net1.append_entries(hb, RPCOption::new(Duration::from_secs(30)))
                .await
                .unwrap();

            // Group 2
            let mut gf2 = GroupNetworkFactory::new(factory, 2);
            let mut net2 = gf2.new_client(10, &node).await;
            let hb = make_heartbeat_request(1, 1);
            net2.append_entries(hb, RPCOption::new(Duration::from_secs(30)))
                .await
                .unwrap();

            // Verify group_id routing: both went to target=10 but with different group_ids
            assert!(transport.append_entries_calls.contains_key(&(10, 1)));
            assert!(transport.append_entries_calls.contains_key(&(10, 2)));
        });
    }

    #[test]
    fn test_network_factory_group_isolation() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };

            // Send vote requests to different groups
            for group_id in 1..=5 {
                let mut gf = GroupNetworkFactory::new(factory.clone(), group_id);
                let mut net = gf.new_client(1, &node).await;
                let vote_req = openraft::raft::VoteRequest {
                    vote: openraft::impls::Vote::new(1, 1),
                    last_log_id: None,
                };
                net.vote(vote_req, RPCOption::new(Duration::from_secs(30)))
                    .await
                    .unwrap();
            }

            // Each group should have exactly one vote call
            for group_id in 1..=5u64 {
                let calls = transport.vote_calls.get(&(1, group_id)).unwrap();
                assert_eq!(calls.len(), 1);
            }
            assert_eq!(transport.call_count.load(Ordering::Relaxed), 5);
        });
    }

    #[test]
    fn test_vote_request_through_factory() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };

            let mut gf = GroupNetworkFactory::new(factory, 1);
            let mut net = gf.new_client(10, &node).await;

            let vote_req = openraft::raft::VoteRequest::<TestConfig> {
                vote: openraft::impls::Vote::new(3, 2),
                last_log_id: None,
            };

            let result = net
                .vote(vote_req, RPCOption::new(Duration::from_secs(30)))
                .await;
            assert!(result.is_ok());
            let resp = result.unwrap();
            assert!(resp.vote_granted);
            assert_eq!(transport.call_count.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn test_multiple_clients_same_factory() {
        run_async(async {
            let transport = FakeTransport::new();
            let factory = Arc::new(MultiRaftNetworkFactory::new(Arc::new(transport.clone())));

            let node1 = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let node2 = openraft::impls::BasicNode {
                addr: "127.0.0.1:6000".to_string(),
            };

            let mut gf = GroupNetworkFactory::new(factory, 1);

            // Create multiple clients from the same factory
            let mut net1 = gf.new_client(10, &node1).await;
            let mut net2 = gf.new_client(20, &node2).await;

            let hb1 = make_heartbeat_request(1, 1);
            let hb2 = make_heartbeat_request(1, 1);

            net1.append_entries(hb1, RPCOption::new(Duration::from_secs(30)))
                .await
                .unwrap();
            net2.append_entries(hb2, RPCOption::new(Duration::from_secs(30)))
                .await
                .unwrap();

            // Both should be routed correctly
            assert!(transport.append_entries_calls.contains_key(&(10, 1)));
            assert!(transport.append_entries_calls.contains_key(&(20, 1)));
            assert_eq!(transport.call_count.load(Ordering::Relaxed), 2);
        });
    }
}
