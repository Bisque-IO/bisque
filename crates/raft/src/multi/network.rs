use crate::multi::config::MultiRaftConfig;
use dashmap::DashMap;
use openraft::Instant;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::oneshot;
use openraft::OptionalSend;
use openraft::OptionalSync;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::async_runtime::AsyncRuntime;
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

    fn send_heartbeat_batch(
        &self,
        target: C::NodeId,
        batch: &[(u64, AppendEntriesRequest<C>)],
    ) -> impl Future<
        Output = Result<Vec<(u64, AppendEntriesResponse<C>)>, RPCError<C, RaftError<C>>>,
    > + OptionalSend;
}

type HeartbeatTx<C> = oneshot::Sender<Result<AppendEntriesResponse<C>, RPCError<C, RaftError<C>>>>;

type HeartbeatMsg<C> = (u64, AppendEntriesRequest<C>, HeartbeatTx<C>);

struct HeartbeatBuffer<C: RaftTypeConfig> {
    tx: tokio_mpsc::Sender<HeartbeatMsg<C>>,
}

impl<C: RaftTypeConfig> Clone for HeartbeatBuffer<C> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

pub struct MultiRaftNetworkFactory<C: RaftTypeConfig, T: MultiplexedTransport<C>> {
    transport: Arc<T>,
    config: MultiRaftConfig,
    buffers: Arc<DashMap<C::NodeId, HeartbeatBuffer<C>>>,
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
    pub fn new(transport: Arc<T>, config: MultiRaftConfig) -> Self {
        Self {
            transport,
            config,
            buffers: Arc::new(DashMap::new()),
        }
    }

    fn get_buffer(&self, target: C::NodeId) -> HeartbeatBuffer<C> {
        if let Some(buffer) = self.buffers.get(&target) {
            return buffer.clone();
        }

        let (tx, mut rx) = tokio_mpsc::channel(256);
        let buffer = HeartbeatBuffer { tx };
        self.buffers.insert(target.clone(), buffer.clone());

        // Spawn flush task
        let flush_transport = self.transport.clone();
        let flush_target = target.clone();
        let flush_interval = self.config.heartbeat_interval;

        // Maximum number of distinct group heartbeats to coalesce into a single on-wire batch.
        // This is a safety valve to avoid unbounded batching under extreme load.
        const MAX_COALESCED_HEARTBEATS: usize = 256;

        // Use C::AsyncRuntime to spawn
        let _ = C::AsyncRuntime::spawn(async move {
            // Deduplicate by group_id within the batching window, but preserve all waiters:
            // group_id -> (latest rpc, waiters)
            let mut pending: std::collections::HashMap<
                u64,
                (AppendEntriesRequest<C>, Vec<HeartbeatTx<C>>),
            > = std::collections::HashMap::new();

            let mut batch_req: Vec<(u64, AppendEntriesRequest<C>)> = Vec::new();
            let mut resp_map: std::collections::HashMap<u64, AppendEntriesResponse<C>> =
                std::collections::HashMap::new();

            loop {
                // Wait for the first message
                let first = match rx.recv().await {
                    Some(msg) => msg,
                    None => return, // Channel closed
                };

                pending.clear();

                // Start batching window from the first message.
                let start = <C::AsyncRuntime as AsyncRuntime>::Instant::now();
                let deadline = start + flush_interval;

                // Add first message (avoid closure move issues)
                match pending.entry(first.0) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        let (rpc, waiters) = e.get_mut();
                        *rpc = first.1;
                        waiters.push(first.2);
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert((first.1, vec![first.2]));
                    }
                }

                // Collect until deadline or max batch size.
                loop {
                    // Drain any immediately available messages first.
                    while pending.len() < MAX_COALESCED_HEARTBEATS {
                        match rx.try_recv() {
                            Ok((gid, rpc, tx)) => match pending.entry(gid) {
                                std::collections::hash_map::Entry::Occupied(mut e) => {
                                    let (existing_rpc, waiters) = e.get_mut();
                                    *existing_rpc = rpc;
                                    waiters.push(tx);
                                }
                                std::collections::hash_map::Entry::Vacant(e) => {
                                    e.insert((rpc, vec![tx]));
                                }
                            },
                            Err(_) => break,
                        }
                    }

                    if pending.len() >= MAX_COALESCED_HEARTBEATS {
                        break;
                    }

                    // If the window elapsed, flush.
                    if <C::AsyncRuntime as AsyncRuntime>::Instant::now() >= deadline {
                        break;
                    }

                    // Wait for the next message, but only until the deadline.
                    match C::AsyncRuntime::timeout_at(deadline, rx.recv()).await {
                        Ok(Some((gid, rpc, tx))) => match pending.entry(gid) {
                            std::collections::hash_map::Entry::Occupied(mut e) => {
                                let (existing_rpc, waiters) = e.get_mut();
                                *existing_rpc = rpc;
                                waiters.push(tx);
                            }
                            std::collections::hash_map::Entry::Vacant(e) => {
                                e.insert((rpc, vec![tx]));
                            }
                        },
                        Ok(None) => return, // Channel closed
                        Err(_) => break,    // Deadline elapsed
                    }
                }

                // Build the batch request from the deduped map by draining to avoid clones.
                // We'll rebuild the waiter map as we go.
                batch_req.clear();
                batch_req.reserve(pending.len());

                // Temporary storage for waiters while we build the batch
                let mut waiter_map: std::collections::HashMap<u64, Vec<HeartbeatTx<C>>> =
                    std::collections::HashMap::with_capacity(pending.len());

                for (gid, (rpc, waiters)) in pending.drain() {
                    batch_req.push((gid, rpc));
                    waiter_map.insert(gid, waiters);
                }

                let result = flush_transport
                    .send_heartbeat_batch(flush_target.clone(), &batch_req)
                    .await;

                match result {
                    Ok(responses) => {
                        resp_map.clear();
                        for (gid, resp) in responses {
                            resp_map.insert(gid, resp);
                        }

                        for (gid, mut waiters) in waiter_map.drain() {
                            if let Some(resp) = resp_map.remove(&gid) {
                                // Give ownership to the last waiter, clone for others
                                if let Some(last_sender) = waiters.pop() {
                                    for sender in waiters {
                                        let _ = sender.send(Ok(resp.clone()));
                                    }
                                    let _ = last_sender.send(Ok(resp));
                                }
                            } else {
                                let err = BatchError::new_static("Missing response in batch");
                                let rpc_err =
                                    RPCError::Network(openraft::error::NetworkError::new(&err));
                                for sender in waiters {
                                    let _ = sender.send(Err(rpc_err.clone()));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        for (_gid, waiters) in waiter_map.drain() {
                            for sender in waiters {
                                let _ = sender.send(Err(e.clone()));
                            }
                        }
                    }
                }
            }
        });

        buffer
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
        panic!("MultiRaftNetworkFactory cannot be used directly. Use GroupNetworkFactory.");
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
        let buffer = self.parent.get_buffer(target.clone());
        MultiRaftNetwork {
            transport: self.parent.transport.clone(),
            buffer,
            target,
            group_id: self.group_id,
        }
    }
}

pub struct MultiRaftNetwork<C: RaftTypeConfig, T: MultiplexedTransport<C>> {
    transport: Arc<T>,
    buffer: HeartbeatBuffer<C>,
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
        // Check if heartbeat (empty entries)
        if rpc.entries.is_empty() {
            let (tx, rx) = oneshot::channel();

            if let Err(_) = self.buffer.tx.send((self.group_id, rpc, tx)).await {
                let err = BatchError::new_static("Heartbeat buffer closed");
                return Err(RPCError::Network(openraft::error::NetworkError::new(&err)));
            }

            // Wait for response
            match rx.await {
                Ok(res) => res.decompose_infallible(),
                Err(_) => {
                    let err = BatchError::new_static("Heartbeat channel closed");
                    Err(RPCError::Network(openraft::error::NetworkError::new(&err)))
                }
            }
        } else {
            self.transport
                .send_append_entries(self.target.clone(), self.group_id, rpc)
                .await
                .decompose_infallible()
        }
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
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(500)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi::test_support::run_async;
    use crate::multi::type_config::ManiacRaftTypeConfig;
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

    /// Fake transport that records calls and allows test control
    #[derive(Clone)]
    struct FakeTransport {
        append_entries_calls: Arc<DashMap<(u64, u64), Vec<AppendEntriesRequest<TestConfig>>>>,
        vote_calls: Arc<DashMap<(u64, u64), Vec<VoteRequest<TestConfig>>>>,
        heartbeat_batch_calls: Arc<DashMap<u64, Vec<Vec<(u64, AppendEntriesRequest<TestConfig>)>>>>,
        batch_responses: Arc<DashMap<u64, Vec<(u64, AppendEntriesResponse<TestConfig>)>>>,
        batch_errors: Arc<DashMap<u64, RPCError<TestConfig, RaftError<TestConfig>>>>,
        call_count: Arc<AtomicU64>,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self {
                append_entries_calls: Arc::new(DashMap::new()),
                vote_calls: Arc::new(DashMap::new()),
                heartbeat_batch_calls: Arc::new(DashMap::new()),
                batch_responses: Arc::new(DashMap::new()),
                batch_errors: Arc::new(DashMap::new()),
                call_count: Arc::new(AtomicU64::new(0)),
            }
        }

        fn set_batch_response(
            &self,
            target: u64,
            responses: Vec<(u64, AppendEntriesResponse<TestConfig>)>,
        ) {
            self.batch_responses.insert(target, responses);
        }

        fn set_batch_error(&self, target: u64, error: RPCError<TestConfig, RaftError<TestConfig>>) {
            self.batch_errors.insert(target, error);
        }

        fn get_heartbeat_batch_count(&self, target: u64) -> usize {
            self.heartbeat_batch_calls
                .get(&target)
                .map(|v| v.len())
                .unwrap_or(0)
        }

        fn get_heartbeat_batch(
            &self,
            target: u64,
            idx: usize,
        ) -> Option<Vec<(u64, AppendEntriesRequest<TestConfig>)>> {
            self.heartbeat_batch_calls.get(&target).and_then(|v| {
                if idx < v.len() {
                    Some(v[idx].clone())
                } else {
                    None
                }
            })
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
            // Clone manually since AppendEntriesRequest may not implement Clone
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

        async fn send_heartbeat_batch(
            &self,
            target: u64,
            batch: &[(u64, AppendEntriesRequest<TestConfig>)],
        ) -> Result<
            Vec<(u64, AppendEntriesResponse<TestConfig>)>,
            RPCError<TestConfig, RaftError<TestConfig>>,
        > {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let batch_vec = batch.to_vec();
            self.heartbeat_batch_calls
                .entry(target)
                .or_insert_with(Vec::new)
                .push(batch_vec);

            // Check for error first
            if let Some(err) = self.batch_errors.get(&target) {
                return Err(err.clone());
            }

            // Return configured responses or defaults
            if let Some(responses) = self.batch_responses.get(&target) {
                Ok(responses.clone())
            } else {
                // Default: success for all groups
                Ok(batch
                    .iter()
                    .map(|(gid, _)| (*gid, AppendEntriesResponse::Success))
                    .collect())
            }
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
    fn test_heartbeat_detection() {
        run_async(async {
            let transport = FakeTransport::new();
            let transport_arc = Arc::new(transport.clone());
            let config = MultiRaftConfig {
                heartbeat_interval: Duration::from_millis(50),
            };
            let factory = Arc::new(MultiRaftNetworkFactory::new(transport_arc.clone(), config));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut group_factory_mut = group_factory;
            let mut network = group_factory_mut.new_client(1, &node).await;

            // Empty entries = heartbeat, should go through buffer
            let heartbeat = make_heartbeat_request(1, 1);
            let result = network
                .append_entries(heartbeat, RPCOption::new(Duration::from_secs(30)))
                .await;
            assert!(result.is_ok());

            // Wait for flush
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Should have been batched
            assert!(transport.clone().get_heartbeat_batch_count(1) > 0);
        });
    }

    #[test]
    fn test_non_heartbeat_bypass() {
        run_async(async {
            let transport = FakeTransport::new();
            let transport_arc = Arc::new(transport.clone());
            let config = MultiRaftConfig {
                heartbeat_interval: Duration::from_millis(50),
            };
            let factory = Arc::new(MultiRaftNetworkFactory::new(transport_arc.clone(), config));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut group_factory_mut = group_factory;
            let mut network = group_factory_mut.new_client(1, &node).await;

            // Non-empty entries = not heartbeat, should bypass buffer
            let append = make_append_request(1, 1, "test data".to_string());
            let result = network
                .append_entries(append.clone(), RPCOption::new(Duration::from_secs(30)))
                .await;
            assert!(result.is_ok());

            // Should have gone directly to transport
            let key = (1, 1);
            assert!(transport.append_entries_calls.get(&key).is_some());
            let calls = transport.append_entries_calls.get(&key).unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].entries.len(), 1);
        });
    }

    #[test]
    fn test_heartbeat_dedup() {
        run_async(async {
            let transport = FakeTransport::new();
            let config = MultiRaftConfig {
                heartbeat_interval: Duration::from_millis(50),
            };
            let factory = Arc::new(MultiRaftNetworkFactory::new(
                Arc::new(transport.clone()),
                config,
            ));

            // Send multiple heartbeats for same group
            let hb1 = make_heartbeat_request(1, 1);
            let hb2 = make_heartbeat_request(2, 1); // Different term
            let hb3 = make_heartbeat_request(3, 1); // Different term

            // Use the buffer directly so we can deterministically control arrival order without
            // having to concurrently borrow a `&mut MultiRaftNetwork`.
            let mut buffer = factory.get_buffer(1);
            let (tx1, rx1) = oneshot::channel();
            let (tx2, rx2) = oneshot::channel();
            let (tx3, rx3) = oneshot::channel();

            // Enqueue in order: hb1 -> hb2 -> hb3. The buffer should keep only the latest RPC per
            // group within the batching window, while still resolving all waiters.
            buffer.tx.send((1, hb1, tx1)).await.unwrap();
            buffer.tx.send((1, hb2, tx2)).await.unwrap();
            buffer.tx.send((1, hb3, tx3)).await.unwrap();

            let (r1, r2, r3) = futures::future::join3(rx1, rx2, rx3).await;
            assert!(r1.unwrap().is_ok());
            assert!(r2.unwrap().is_ok());
            assert!(r3.unwrap().is_ok());

            // Should have only one batch with the latest heartbeat
            assert_eq!(transport.clone().get_heartbeat_batch_count(1), 1);
            if let Some(batch) = transport.clone().get_heartbeat_batch(1, 0) {
                assert_eq!(batch.len(), 1); // Only one group
                assert_eq!(batch[0].0, 1); // group_id
                // Should have the latest term (3)
                assert_eq!(batch[0].1.vote.leader_id.term, 3);
            }
        });
    }

    #[test]
    fn test_batch_size_cap() {
        run_async(async {
            let transport = FakeTransport::new();
            let config = MultiRaftConfig {
                // Keep the batching window long enough that many heartbeats can accumulate,
                // but short enough for the test to complete quickly.
                heartbeat_interval: Duration::from_millis(100),
            };
            let factory = Arc::new(MultiRaftNetworkFactory::new(
                Arc::new(transport.clone()),
                config,
            ));

            // Exceed MAX_COALESCED_HEARTBEATS (256) by creating many distinct groups and issuing
            // heartbeats concurrently. This avoids a slow "sleep per heartbeat interval" loop.
            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut futs = Vec::new();
            for group_id in 1..=260 {
                let mut group_factory = GroupNetworkFactory::new(factory.clone(), group_id);
                let mut network = group_factory.new_client(1, &node).await;
                let hb = make_heartbeat_request(1, 1);
                futs.push(async move {
                    network
                        .append_entries(hb, RPCOption::new(Duration::from_secs(30)))
                        .await
                });
            }
            let results = futures::future::join_all(futs).await;
            assert!(results.iter().all(|r| r.is_ok()));

            // Should have at least one batch (may have multiple if cap was hit)
            assert!(transport.get_heartbeat_batch_count(1) > 0);
        });
    }

    #[test]
    fn test_error_propagation() {
        run_async(async {
            let transport = FakeTransport::new();
            let error = RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::Other, "test error"),
            ));
            transport.set_batch_error(1, error.clone());

            let config = MultiRaftConfig {
                heartbeat_interval: Duration::from_millis(50),
            };
            let factory = Arc::new(MultiRaftNetworkFactory::new(
                Arc::new(transport.clone()),
                config,
            ));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut group_factory_mut = group_factory;
            let mut network = group_factory_mut.new_client(1, &node).await;

            let hb = make_heartbeat_request(1, 1);
            let result = network
                .append_entries(hb, RPCOption::new(Duration::from_secs(30)))
                .await;

            // Wait for flush
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Should have received error
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_missing_response_in_batch() {
        run_async(async {
            let transport = FakeTransport::new();
            // Return batch with missing group
            transport.set_batch_response(
                1,
                vec![
                    (2, AppendEntriesResponse::Success), // Missing group 1
                ],
            );

            let config = MultiRaftConfig {
                heartbeat_interval: Duration::from_millis(50),
            };
            let factory = Arc::new(MultiRaftNetworkFactory::new(
                Arc::new(transport.clone()),
                config,
            ));
            let mut group_factory = GroupNetworkFactory::new(factory.clone(), 1);

            let node = openraft::impls::BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            };
            let mut group_factory_mut = group_factory;
            let mut network = group_factory_mut.new_client(1, &node).await;

            let hb = make_heartbeat_request(1, 1);
            let result = network
                .append_entries(hb, RPCOption::new(Duration::from_secs(30)))
                .await;

            // Wait for flush
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Should have received error about missing response
            assert!(result.is_err());
        });
    }
}
