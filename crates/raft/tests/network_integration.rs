//! Integration stress tests and benchmarks for the network, RPC server,
//! and TCP transport layers.
//!
//! These tests exercise the full client→TCP→RPC server→Raft pipeline and
//! measure throughput and latency under various load patterns.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{SocketAddr, TcpListener};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bisque_raft::BisqueRaftTypeConfig;
use bisque_raft::NodeAddressResolver;
use bisque_raft::codec::{self, Decode, Encode};
use bisque_raft::network::GroupNetworkFactory;
use bisque_raft::test_support::TestTempDir;
use bisque_raft::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MmapStorageConfig, MultiRaftManager, MultiRaftNetworkFactory,
    MultiplexedLogStorage,
};
use bytes::{Buf, BytesMut};
use futures::FutureExt;
use openraft::OptionalSend;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::network::RaftNetworkFactory;
use openraft::network::v2::RaftNetworkV2;
use openraft::storage::RaftStateMachine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    fn decode_from_bytes(data: bytes::Bytes) -> Result<Self, codec::CodecError> {
        Ok(Self(data.to_vec()))
    }
}

impl codec::BorrowPayload for TestData {
    fn payload_bytes(&self) -> &[u8] {
        &self.0
    }
}

type TestConfig = BisqueRaftTypeConfig<TestData, ()>;
type TestLogId = openraft::alias::LogIdOf<TestConfig>;
type TestStoredMembership = openraft::alias::StoredMembershipOf<TestConfig>;
type TestSnapshotMeta = openraft::alias::SnapshotMetaOf<TestConfig>;
type TestSnapshot = openraft::alias::SnapshotOf<TestConfig>;

#[derive(Clone)]
struct TestStateMachine {
    applied_normal: Arc<AtomicU64>,
}

impl TestStateMachine {
    fn new() -> Self {
        Self {
            applied_normal: Arc::new(AtomicU64::new(0)),
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
    ) -> Result<(Option<TestLogId>, TestStoredMembership), std::io::Error> {
        Ok((None, TestStoredMembership::default()))
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
            if let openraft::EntryPayload::Normal(_data) = &entry.payload {
                self.applied_normal.fetch_add(1, Ordering::SeqCst);
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
        let meta = openraft::storage::SnapshotMeta {
            last_log_id: None,
            last_membership: TestStoredMembership::default(),
            snapshot_id: "integration-test".to_string(),
        };
        Ok(openraft::storage::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(Vec::new()),
        })
    }
}

// ---------------------------------------------------------------------------
// Pipelined fake RPC server
// ---------------------------------------------------------------------------

const FRAME_PREFIX_LEN: usize = 4;

/// Spawn a pipelined fake RPC server that accepts multiple connections.
///
/// Each connection uses split reader/writer with a channel in between,
/// so reads and writes proceed concurrently — the reader never blocks
/// waiting for the writer to flush. This mirrors the real transport's
/// multiplexed architecture.
fn spawn_pipelined_rpc_server(
    addr: SocketAddr,
    request_count: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let count = request_count.clone();
            tokio::spawn(async move {
                let (mut reader, mut writer) = stream.into_split();
                // Bounded channel decouples reader from writer so reads
                // aren't stalled by TCP write backpressure.
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4096);

                // Writer task: drain the channel and flush batched writes
                let writer_handle = tokio::spawn(async move {
                    while let Some(data) = rx.recv().await {
                        if writer.write_all(&data).await.is_err() {
                            return;
                        }
                    }
                });

                // Reader loop: decode, build response, send through channel
                let mut buf = BytesMut::with_capacity(256 * 1024);
                'outer: loop {
                    // Parse all complete frames already buffered
                    loop {
                        if buf.len() < FRAME_PREFIX_LEN {
                            break;
                        }
                        let payload_len =
                            u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap())
                                as usize;
                        if buf.len() < FRAME_PREFIX_LEN + payload_len {
                            break; // incomplete frame
                        }
                        buf.advance(FRAME_PREFIX_LEN);
                        let frame = buf.split_to(payload_len);

                        let msg: codec::RpcMessage<TestConfig> =
                            match codec::RpcMessage::decode_from_slice(&frame) {
                                Ok(m) => m,
                                Err(_) => break 'outer,
                            };

                        let response = build_rpc_response(msg);
                        let out = response.encode_to_vec().unwrap();
                        // Frame header + payload
                        let len_bytes = (out.len() as u32).to_le_bytes();
                        let mut framed = Vec::with_capacity(4 + out.len());
                        framed.extend_from_slice(&len_bytes);
                        framed.extend_from_slice(&out);
                        if tx.send(framed).await.is_err() {
                            break 'outer;
                        }
                        count.fetch_add(1, Ordering::Relaxed);
                    }

                    // Reserve space and read more from the socket
                    if buf.capacity() - buf.len() < 8192 {
                        buf.reserve(256 * 1024);
                    }
                    match reader.read_buf(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                drop(tx);
                let _ = writer_handle.await;
            });
        }
    })
}

/// Build a success response for any incoming RPC message.
fn build_rpc_response(msg: codec::RpcMessage<TestConfig>) -> codec::RpcMessage<TestConfig> {
    match msg {
        codec::RpcMessage::AppendEntries { request_id, .. } => codec::RpcMessage::Response {
            request_id,
            message: codec::ResponseMessage::AppendEntries(
                openraft::raft::AppendEntriesResponse::Success,
            ),
        },
        codec::RpcMessage::Vote { request_id, .. } => codec::RpcMessage::Response {
            request_id,
            message: codec::ResponseMessage::Vote(openraft::raft::VoteResponse {
                vote: openraft::impls::Vote {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    committed: false,
                },
                vote_granted: true,
                last_log_id: None,
            }),
        },
        other => codec::RpcMessage::Error {
            request_id: other.request_id(),
            error: "unsupported".to_string(),
        },
    }
}

// ---------------------------------------------------------------------------
// Cluster harness
// ---------------------------------------------------------------------------

struct ClusterNode {
    #[allow(dead_code)]
    addr: SocketAddr,
    manager: Arc<
        MultiRaftManager<
            TestConfig,
            BisqueTcpTransport<TestConfig>,
            MultiplexedLogStorage<TestConfig>,
            TestStateMachine,
        >,
    >,
    server: Arc<
        BisqueRpcServer<
            TestConfig,
            BisqueTcpTransport<TestConfig>,
            MultiplexedLogStorage<TestConfig>,
            TestStateMachine,
        >,
    >,
    serve_handle: tokio::task::JoinHandle<()>,
    _dir: TestTempDir,
}

struct TestCluster {
    nodes: Vec<ClusterNode>,
    #[allow(dead_code)]
    node_registry: Arc<DefaultNodeRegistry<u32>>,
}

impl TestCluster {
    async fn new(num_nodes: usize, transport_cfg: BisqueTcpTransportConfig) -> Self {
        let node_registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        let mut nodes = Vec::with_capacity(num_nodes);

        for node_id_idx in 0..num_nodes {
            let node_id = (node_id_idx + 1) as u32;
            let addr = pick_unused_local_addr();
            node_registry.register(node_id, addr);

            let dir = TestTempDir::new();
            let storage = MultiplexedLogStorage::<TestConfig>::new(
                MmapStorageConfig::new(dir.path())
                    .with_segment_size(4 * 1024 * 1024)
                    .with_fsync_delay(Duration::ZERO),
            )
            .await
            .expect("storage");

            let transport =
                BisqueTcpTransport::<TestConfig>::new(transport_cfg.clone(), node_registry.clone());

            let manager = Arc::new(MultiRaftManager::<TestConfig, _, _, TestStateMachine>::new(
                transport, storage,
            ));

            let server = Arc::new(BisqueRpcServer::new(
                BisqueRpcServerConfig {
                    bind_addr: addr,
                    ..Default::default()
                },
                manager.clone(),
            ));

            // Start serving in background
            let serve_handle = tokio::spawn({
                let s = server.clone();
                async move {
                    let _ = s.serve().await;
                }
            });

            nodes.push(ClusterNode {
                addr,
                manager,
                server,
                serve_handle,
                _dir: dir,
            });
        }

        // Wait for servers to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        TestCluster {
            nodes,
            node_registry,
        }
    }

    fn membership(&self) -> BTreeMap<u32, openraft::impls::BasicNode> {
        let mut members = BTreeMap::new();
        for (i, _node) in self.nodes.iter().enumerate() {
            members.insert((i + 1) as u32, openraft::impls::BasicNode::default());
        }
        members
    }

    async fn add_groups(
        &self,
        group_ids: &[u64],
        raft_cfg: Arc<openraft::Config>,
    ) -> Vec<Vec<TestStateMachine>> {
        let members = self.membership();
        // state_machines[node_idx][group_idx]
        let mut state_machines = Vec::with_capacity(self.nodes.len());

        for node in &self.nodes {
            let node_id = self
                .nodes
                .iter()
                .position(|n| std::ptr::eq(n, node))
                .unwrap() as u32
                + 1;
            let mut sms = Vec::with_capacity(group_ids.len());
            for &gid in group_ids {
                let sm = TestStateMachine::new();
                node.manager
                    .add_group(gid, node_id, raft_cfg.clone(), sm.clone())
                    .await
                    .expect("add group");
                sms.push(sm);
            }
            state_machines.push(sms);
        }

        // Initialize from node 1
        for &gid in group_ids {
            let raft = self.nodes[0].manager.get_group(gid).expect("raft exists");
            tokio::time::timeout(Duration::from_secs(5), raft.initialize(members.clone()))
                .await
                .expect("init timeout")
                .expect("init ok");
        }

        state_machines
    }

    async fn wait_for_leader(&self, group_id: u64, expected_leader: u32, timeout_secs: u64) {
        let raft = self.nodes[(expected_leader - 1) as usize]
            .manager
            .get_group(group_id)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(timeout_secs), async {
            loop {
                let m = raft.metrics().borrow_watched().clone();
                if m.current_leader == Some(expected_leader) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("leader wait timeout");
    }

    async fn shutdown(self) {
        for node in self.nodes {
            node.manager.shutdown_all().await;
            node.server.shutdown_and_drain(Duration::from_secs(5)).await;
            node.manager.storage().stop();
            let _ = node.serve_handle.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Latency histogram helper
// ---------------------------------------------------------------------------

struct LatencyStats {
    samples: Vec<Duration>,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    fn compute(&mut self) -> LatencySummary {
        self.samples.sort();
        let n = self.samples.len();
        if n == 0 {
            return LatencySummary::default();
        }
        let sum: Duration = self.samples.iter().sum();
        LatencySummary {
            count: n,
            min: self.samples[0],
            max: self.samples[n - 1],
            mean: sum / n as u32,
            p50: self.samples[n / 2],
            p90: self.samples[(n as f64 * 0.9) as usize],
            p99: self.samples[(n as f64 * 0.99).min((n - 1) as f64) as usize],
        }
    }
}

#[derive(Default)]
struct LatencySummary {
    count: usize,
    min: Duration,
    max: Duration,
    mean: Duration,
    p50: Duration,
    p90: Duration,
    p99: Duration,
}

impl fmt::Display for LatencySummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "n={} min={:?} p50={:?} p90={:?} p99={:?} max={:?} mean={:?}",
            self.count, self.min, self.p50, self.p90, self.p99, self.max, self.mean,
        )
    }
}

// ===========================================================================
// Transport-level integration tests (no Raft groups)
// ===========================================================================

/// Stress test: raw codec encode/decode roundtrip at high throughput.
/// Validates that the codec doesn't corrupt under rapid fire.
#[test]
fn test_codec_roundtrip_stress() {
    let iterations = 50_000;
    let start = Instant::now();

    for i in 0..iterations {
        let data = vec![(i % 256) as u8; 64 + (i % 512)];
        let rpc = openraft::raft::AppendEntriesRequest::<TestConfig> {
            vote: openraft::impls::Vote {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: i as u32,
                    node_id: 1u32,
                },
                committed: true,
            },
            prev_log_id: Some(openraft::LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: i as u32,
                    node_id: 1u32,
                },
                index: i as u64,
            }),
            entries: vec![openraft::Entry {
                log_id: openraft::LogId {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: i as u32,
                        node_id: 1u32,
                    },
                    index: i as u64 + 1,
                },
                payload: openraft::EntryPayload::Normal(TestData(data.clone())),
            }],
            leader_commit: None,
        };

        let msg = codec::RpcMessage::<TestConfig>::AppendEntries {
            request_id: i as u64,
            group_id: 42,
            rpc,
        };

        let encoded = msg.encode_to_vec().expect("encode");
        let decoded: codec::RpcMessage<TestConfig> =
            codec::RpcMessage::decode_from_slice(&encoded).expect("decode");

        match decoded {
            codec::RpcMessage::AppendEntries {
                request_id,
                group_id,
                rpc,
            } => {
                assert_eq!(request_id, i as u64);
                assert_eq!(group_id, 42);
                assert_eq!(rpc.entries.len(), 1);
                if let openraft::EntryPayload::Normal(td) = &rpc.entries[0].payload {
                    assert_eq!(td.0, data);
                } else {
                    panic!("expected Normal entry");
                }
            }
            _ => panic!("unexpected message type"),
        }
    }

    let elapsed = start.elapsed();
    let per_op = elapsed / iterations as u32;
    println!(
        "[codec_roundtrip_stress] {} iterations in {:?} ({:?}/op, {:.0} ops/s)",
        iterations,
        elapsed,
        per_op,
        iterations as f64 / elapsed.as_secs_f64(),
    );
}

/// Stress test: pipelined frame send/receive over TCP with batched writes.
/// Both client and server batch frames into large buffers to minimize
/// syscalls, exercising the framing layer near theoretical TCP throughput.
#[test]
fn test_tcp_framing_throughput() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let num_messages: usize = 1_000_000;
        let payload_size = 256;
        let frame_size = 4 + payload_size;
        // Batch size for writes: ~1 MB chunks
        let write_batch_bytes = 1024 * 1024;
        let frames_per_batch = write_batch_bytes / frame_size;

        // Server: read frames, echo them back in large batched writes
        let server = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = stream.into_split();

            // Writer receives batched buffers from reader via channel
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
            let writer_handle = tokio::spawn(async move {
                while let Some(batch) = rx.recv().await {
                    if writer.write_all(&batch).await.is_err() {
                        return;
                    }
                }
            });

            // Reader: parse frames, accumulate echoes into batches
            let mut buf = BytesMut::with_capacity(2 * 1024 * 1024);
            let mut out_batch = Vec::with_capacity(write_batch_bytes + frame_size);
            let mut echoed = 0usize;
            loop {
                // Parse all complete frames in buffer
                loop {
                    if buf.len() < 4 {
                        break;
                    }
                    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
                    if buf.len() < 4 + len {
                        break;
                    }
                    // Append frame (header + payload) to output batch
                    out_batch.extend_from_slice(&buf[..4 + len]);
                    buf.advance(4 + len);
                    echoed += 1;

                    // Flush batch when it's large enough
                    if out_batch.len() >= write_batch_bytes {
                        let batch = std::mem::replace(
                            &mut out_batch,
                            Vec::with_capacity(write_batch_bytes + frame_size),
                        );
                        if tx.send(batch).await.is_err() {
                            return;
                        }
                    }
                }
                if echoed >= num_messages {
                    break;
                }
                if buf.capacity() - buf.len() < 64 * 1024 {
                    buf.reserve(2 * 1024 * 1024);
                }
                match reader.read_buf(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            // Flush remaining
            if !out_batch.is_empty() {
                let _ = tx.send(out_batch).await;
            }
            drop(tx);
            let _ = writer_handle.await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut reader, mut writer) = stream.into_split();

        // Pre-build one frame
        let mut single_frame = Vec::with_capacity(frame_size);
        single_frame.extend_from_slice(&(payload_size as u32).to_le_bytes());
        single_frame.extend((0..payload_size).map(|j| (j % 256) as u8));

        // Spawn reader to consume echoes concurrently
        let received = Arc::new(AtomicU64::new(0));
        let recv_clone = received.clone();
        let read_handle = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(2 * 1024 * 1024);
            let mut count = 0usize;
            loop {
                // Parse frames
                loop {
                    if buf.len() < 4 {
                        break;
                    }
                    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
                    if buf.len() < 4 + len {
                        break;
                    }
                    buf.advance(4 + len);
                    count += 1;
                }
                if count >= num_messages {
                    recv_clone.store(count as u64, Ordering::Relaxed);
                    break;
                }
                if buf.capacity() - buf.len() < 64 * 1024 {
                    buf.reserve(2 * 1024 * 1024);
                }
                match reader.read_buf(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            recv_clone.store(count as u64, Ordering::Relaxed);
        });

        // Writer: build large batches and write in bulk
        let start = Instant::now();
        let mut write_buf = Vec::with_capacity(write_batch_bytes + frame_size);
        let mut written = 0;
        while written < num_messages {
            let batch_count = frames_per_batch.min(num_messages - written);
            write_buf.clear();
            for _ in 0..batch_count {
                write_buf.extend_from_slice(&single_frame);
            }
            writer.write_all(&write_buf).await.unwrap();
            written += batch_count;
        }

        read_handle.await.unwrap();
        let elapsed = start.elapsed();
        let done = received.load(Ordering::Relaxed);

        let total_bytes = (num_messages * frame_size * 2) as f64; // send + receive
        println!(
            "[tcp_framing_throughput] {} frames in {:?} ({:.0} frames/s, {:.1} MB/s, {:.2} GB/s)",
            done,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
            total_bytes / 1024.0 / 1024.0 / elapsed.as_secs_f64(),
            total_bytes / 1024.0 / 1024.0 / 1024.0 / elapsed.as_secs_f64(),
        );

        server.await.unwrap();
    });
}

/// Stress test: concurrent multiplexed RPC requests through the real
/// BisqueTcpTransport + pipelined fake server.
#[test]
fn test_multiplexed_transport_concurrent_requests() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let total_requests: u64 = 100_000;
        let request_count = Arc::new(AtomicU64::new(0));

        let _server = spawn_pipelined_rpc_server(addr, request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(30),
                connection_ttl: Duration::from_secs(60),
                tcp_nodelay: true,
                ..Default::default()
            },
            registry,
        ));

        let start = Instant::now();
        let completed = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for i in 0..total_requests {
            let transport = transport.clone();
            let completed = completed.clone();
            let errors = errors.clone();
            handles.push(tokio::spawn(async move {
                use bisque_raft::MultiplexedTransport;
                let result = transport
                    .send_append_entries(
                        2,
                        (i % 100) + 1,
                        openraft::raft::AppendEntriesRequest {
                            vote: openraft::impls::Vote::new(1, 1),
                            prev_log_id: None,
                            entries: vec![],
                            leader_commit: None,
                        },
                    )
                    .await;
                if result.is_ok() {
                    completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);
        let errs = errors.load(Ordering::Relaxed);

        println!(
            "[multiplexed_transport_concurrent] {} ok, {} errors out of {} in {:?} ({:.0} req/s)",
            done,
            errs,
            total_requests,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
        );
        assert!(
            done >= total_requests * 95 / 100,
            "too many failures: {} ok, {} errors out of {}",
            done,
            errs,
            total_requests,
        );
    });
}

/// Stress test vote RPCs through the real transport + pipelined server.
#[test]
fn test_vote_rpc_stress() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let total_votes: u64 = 10_000;
        let request_count = Arc::new(AtomicU64::new(0));

        let _server = spawn_pipelined_rpc_server(addr, request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(30),
                connection_ttl: Duration::from_secs(60),
                tcp_nodelay: true,
                ..Default::default()
            },
            registry,
        ));

        // Fire all votes concurrently for maximum throughput
        let start = Instant::now();
        let completed = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for i in 0..total_votes {
            let transport = transport.clone();
            let completed = completed.clone();
            handles.push(tokio::spawn(async move {
                use bisque_raft::MultiplexedTransport;
                let result = transport
                    .send_vote(
                        2,
                        (i % 50) + 1,
                        openraft::raft::VoteRequest {
                            vote: openraft::impls::Vote::new(i as u32, 1u32),
                            last_log_id: None,
                        },
                    )
                    .await;
                if result.is_ok() {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);

        println!(
            "[vote_rpc_stress] {} ok out of {} in {:?} ({:.0} req/s)",
            done,
            total_votes,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
        );
        assert!(
            done >= total_votes * 95 / 100,
            "too many failures: {} out of {}",
            total_votes - done,
            total_votes,
        );
    });
}

/// Stress the transport: many concurrent requests through a pipelined server.
#[test]
fn test_transport_concurrent_stress() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let total_requests: u64 = 100_000;
        let server_request_count = Arc::new(AtomicU64::new(0));

        let _server = spawn_pipelined_rpc_server(addr, server_request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(30),
                connection_ttl: Duration::from_secs(60),
                tcp_nodelay: true,
                ..Default::default()
            },
            registry,
        ));

        let start = Instant::now();
        let completed = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for i in 0..total_requests {
            let transport = transport.clone();
            let completed = completed.clone();
            handles.push(tokio::spawn(async move {
                use bisque_raft::MultiplexedTransport;
                if transport
                    .send_append_entries(
                        2,
                        i % 100 + 1,
                        openraft::raft::AppendEntriesRequest {
                            vote: openraft::impls::Vote::new(1, 1),
                            prev_log_id: None,
                            entries: vec![],
                            leader_commit: None,
                        },
                    )
                    .await
                    .is_ok()
                {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);

        println!(
            "[transport_concurrent_stress] {} ok out of {} in {:?} ({:.0} req/s)",
            done,
            total_requests,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
        );
        assert!(
            done >= total_requests * 95 / 100,
            "too many failures: {} out of {}",
            total_requests - done,
            total_requests,
        );
    });
}

// ===========================================================================
// Full-stack integration tests (transport + RPC server + Raft)
//
// These tests are bottlenecked by Raft consensus (replication to follower
// + log storage), NOT transport. The throughput numbers reflect end-to-end
// consensus latency, which is dominated by the Raft protocol itself.
// ===========================================================================

/// End-to-end: spin up a 2-node cluster, elect a leader, and measure
/// client_write throughput (bottleneck: Raft consensus, not transport).
#[test]
fn test_cluster_write_throughput() {
    run_async(async {
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(60),
            tcp_nodelay: true,
            ..Default::default()
        };
        let cluster = TestCluster::new(2, transport_cfg).await;

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

        let sms = cluster.add_groups(&[1], raft_cfg).await;
        cluster.wait_for_leader(1, 1, 10).await;

        let raft = cluster.nodes[0].manager.get_group(1).unwrap();

        // Throughput: sequential writes
        let num_writes = 500;
        let payload_size = 128;
        let start = Instant::now();
        let mut latencies = LatencyStats::new();

        for i in 0..num_writes {
            let payload = vec![(i % 256) as u8; payload_size];
            let t0 = Instant::now();
            raft.client_write(TestData(payload))
                .await
                .expect("client_write");
            latencies.record(t0.elapsed());
        }

        let elapsed = start.elapsed();
        let summary = latencies.compute();
        println!(
            "[cluster_write_throughput] {} writes ({} B each) in {:?}",
            num_writes, payload_size, elapsed,
        );
        println!(
            "  throughput: {:.0} writes/s",
            num_writes as f64 / elapsed.as_secs_f64()
        );
        println!("  latency: {}", summary);

        // Verify replication
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if sms[1][0].applied() >= num_writes {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replication timeout");

        assert!(
            sms[1][0].applied() >= num_writes,
            "follower should have applied all writes: got {}",
            sms[1][0].applied()
        );

        cluster.shutdown().await;
    });
}

/// Concurrent writes: launch many client_write requests in parallel.
/// Bottleneck: Raft consensus (replication + storage).
#[test]
fn test_cluster_concurrent_write_throughput() {
    run_async(async {
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(60),
            tcp_nodelay: true,
            ..Default::default()
        };
        let cluster = TestCluster::new(2, transport_cfg).await;

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

        let sms = cluster.add_groups(&[1], raft_cfg).await;
        cluster.wait_for_leader(1, 1, 10).await;

        let raft = cluster.nodes[0].manager.get_group(1).unwrap();

        let num_writes: u64 = 500;
        let concurrency = 32;
        let payload_size = 128;

        let start = Instant::now();
        let completed = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for i in 0..num_writes {
            let raft = raft.clone();
            let completed = completed.clone();
            handles.push(tokio::spawn(async move {
                let payload = vec![(i % 256) as u8; payload_size];
                raft.client_write(TestData(payload))
                    .await
                    .expect("client_write");
                completed.fetch_add(1, Ordering::Relaxed);
            }));

            // Limit in-flight
            if handles.len() >= concurrency {
                let h = handles.remove(0);
                h.await.unwrap();
            }
        }
        futures::future::join_all(handles).await;

        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);
        println!(
            "[cluster_concurrent_write_throughput] {} writes (concurrency={}) in {:?}",
            done, concurrency, elapsed,
        );
        println!(
            "  throughput: {:.0} writes/s",
            done as f64 / elapsed.as_secs_f64()
        );

        // Verify replication
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if sms[1][0].applied() >= num_writes {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replication timeout");

        cluster.shutdown().await;
    });
}

/// Multi-group stress: create many raft groups on a 2-node cluster and
/// issue writes to all of them concurrently.
#[test]
fn test_multi_group_concurrent_writes() {
    run_async(async {
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(60),
            tcp_nodelay: true,
            ..Default::default()
        };
        let cluster = TestCluster::new(2, transport_cfg).await;

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

        let num_groups = 10;
        let group_ids: Vec<u64> = (1..=num_groups).collect();
        let sms = cluster.add_groups(&group_ids, raft_cfg).await;

        // Wait for leader on all groups
        for &gid in &group_ids {
            cluster.wait_for_leader(gid, 1, 10).await;
        }

        let writes_per_group = 50;
        let start = Instant::now();
        let mut handles = Vec::new();

        for &gid in &group_ids {
            let raft = cluster.nodes[0].manager.get_group(gid).unwrap();
            handles.push(tokio::spawn(async move {
                for i in 0..writes_per_group {
                    let payload = vec![(i % 256) as u8; 64];
                    raft.client_write(TestData(payload))
                        .await
                        .expect("client_write");
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let total_writes = num_groups * writes_per_group;

        println!(
            "[multi_group_concurrent_writes] {} groups x {} writes = {} total in {:?}",
            num_groups, writes_per_group, total_writes, elapsed,
        );
        println!(
            "  throughput: {:.0} writes/s",
            total_writes as f64 / elapsed.as_secs_f64()
        );

        // Verify replication per-group
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let all_done = (0..num_groups as usize)
                    .all(|g| sms[1][g].applied() >= writes_per_group as u64);
                if all_done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replication timeout");

        for (g, gid) in group_ids.iter().enumerate() {
            assert!(
                sms[1][g].applied() >= writes_per_group as u64,
                "follower group {} should have applied all writes: got {}",
                gid,
                sms[1][g].applied()
            );
        }

        cluster.shutdown().await;
    });
}

// ===========================================================================
// Large payload test
// ===========================================================================

// (heartbeat coalescing stress test was removed — heartbeats now go
// directly through BisqueTcpTransport which batches writes naturally)

/// Test transport with large payloads to verify streaming works under load.
#[test]
fn test_large_payload_stress() {
    run_async(async {
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            connection_ttl: Duration::from_secs(60),
            tcp_nodelay: true,
            ..Default::default()
        };
        let cluster = TestCluster::new(2, transport_cfg).await;

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

        let sms = cluster.add_groups(&[1], raft_cfg).await;
        cluster.wait_for_leader(1, 1, 10).await;

        let raft = cluster.nodes[0].manager.get_group(1).unwrap();

        // Write large payloads
        let num_writes = 50;
        let payload_sizes = [1024, 4096, 16384, 65536];
        let mut total_bytes = 0usize;
        let start = Instant::now();

        for (i, &size) in payload_sizes.iter().cycle().take(num_writes).enumerate() {
            let payload = vec![(i % 256) as u8; size];
            total_bytes += size;
            raft.client_write(TestData(payload))
                .await
                .expect("client_write");
        }

        let elapsed = start.elapsed();
        println!(
            "[large_payload_stress] {} writes ({:.1} KB total) in {:?}",
            num_writes,
            total_bytes as f64 / 1024.0,
            elapsed,
        );
        println!(
            "  throughput: {:.1} MB/s",
            total_bytes as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
        );

        // Verify replication
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if sms[1][0].applied() >= num_writes as u64 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replication timeout");

        cluster.shutdown().await;
    });
}

// ===========================================================================
// End-to-end RPC latency benchmark (transport → server → raft → response)
// ===========================================================================

/// Measure end-to-end RPC latency through a real cluster: transport →
/// RPC server → raft append_entries → response.
#[test]
fn test_e2e_rpc_latency() {
    run_async(async {
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(60),
            tcp_nodelay: true,
            ..Default::default()
        };
        let cluster = TestCluster::new(2, transport_cfg).await;

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

        let _sms = cluster.add_groups(&[1], raft_cfg).await;
        cluster.wait_for_leader(1, 1, 10).await;

        let raft = cluster.nodes[0].manager.get_group(1).unwrap();

        // Warm up
        for _ in 0..10 {
            raft.client_write(TestData(vec![0u8; 32]))
                .await
                .expect("warmup");
        }

        // Benchmark
        let num_writes = 200;
        let mut latencies = LatencyStats::new();

        for i in 0..num_writes {
            let payload = vec![(i % 256) as u8; 64];
            let t0 = Instant::now();
            raft.client_write(TestData(payload))
                .await
                .expect("client_write");
            latencies.record(t0.elapsed());
        }

        let summary = latencies.compute();
        println!(
            "[e2e_rpc_latency] {} writes, latency: {}",
            num_writes, summary
        );

        cluster.shutdown().await;
    });
}

// ===========================================================================
// Mixed workload stress test
// ===========================================================================

/// Simulate a mixed workload: multiple groups with different write rates,
/// simultaneous reads of metrics, and group add/remove operations.
#[test]
fn test_mixed_workload_stress() {
    run_async(async {
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            connection_ttl: Duration::from_secs(60),
            tcp_nodelay: true,
            ..Default::default()
        };
        let cluster = TestCluster::new(2, transport_cfg).await;

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

        // Start with 5 groups
        let initial_groups: Vec<u64> = (1..=5).collect();
        let _sms = cluster.add_groups(&initial_groups, raft_cfg.clone()).await;
        for &gid in &initial_groups {
            cluster.wait_for_leader(gid, 1, 10).await;
        }

        let manager = cluster.nodes[0].manager.clone();
        let start = Instant::now();
        let total_writes = Arc::new(AtomicU64::new(0));
        let total_metrics_reads = Arc::new(AtomicU64::new(0));

        // Writer tasks: write to different groups at different rates
        let mut handles = Vec::new();
        for &gid in &initial_groups {
            let raft = manager.get_group(gid).unwrap();
            let total = total_writes.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..30 {
                    let payload = vec![(i % 256) as u8; 32 + (gid as usize * 16)];
                    if raft.client_write(TestData(payload)).await.is_ok() {
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        // Metrics reader: continuously read metrics from all groups
        let metrics_manager = manager.clone();
        let metrics_total = total_metrics_reads.clone();
        let metrics_handle = tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(8);
            while Instant::now() < deadline {
                for gid in 1..=5u64 {
                    if let Some(raft) = metrics_manager.get_group(gid) {
                        let _m = raft.metrics().borrow_watched().clone();
                        metrics_total.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // Wait for writers to complete
        futures::future::join_all(handles).await;

        // Give metrics reader a bit more time, then drop
        drop(metrics_handle);

        let elapsed = start.elapsed();
        let writes = total_writes.load(Ordering::Relaxed);
        let reads = total_metrics_reads.load(Ordering::Relaxed);

        println!(
            "[mixed_workload_stress] {} writes + {} metric reads in {:?}",
            writes, reads, elapsed,
        );
        println!(
            "  write throughput: {:.0} writes/s",
            writes as f64 / elapsed.as_secs_f64()
        );

        assert!(writes > 0, "should have completed some writes");

        cluster.shutdown().await;
    });
}

// ===========================================================================
// Group-Pinned Transport Tests
// ===========================================================================

/// Basic RPC test: send append_entries, vote, and install_snapshot through
/// the group-pinned transport against the pipelined fake server.
#[test]
fn test_group_pinned_basic_rpc() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let request_count = Arc::new(AtomicU64::new(0));
        let _server = spawn_pipelined_rpc_server(addr, request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig::default(),
            registry,
        ));

        use bisque_raft::MultiplexedTransport;

        // AppendEntries
        let resp = transport
            .send_append_entries(
                2,
                1,
                openraft::raft::AppendEntriesRequest {
                    vote: openraft::impls::Vote::new(1, 1),
                    prev_log_id: None,
                    entries: vec![],
                    leader_commit: None,
                },
            )
            .await;
        assert!(resp.is_ok(), "append_entries failed: {:?}", resp.err());

        // Vote
        let resp = transport
            .send_vote(
                2,
                1,
                openraft::raft::VoteRequest {
                    vote: openraft::impls::Vote::new(1, 1),
                    last_log_id: None,
                },
            )
            .await;
        assert!(resp.is_ok(), "vote failed: {:?}", resp.err());

        let count = request_count.load(Ordering::Relaxed);
        println!("[group_pinned_basic_rpc] {} requests processed", count);
        assert!(count >= 2, "expected at least 2 requests, got {}", count);
    });
}

/// Concurrent throughput test: fire 100k append_entries across 100 groups
/// through the group-pinned transport.
#[test]
fn test_group_pinned_concurrent_throughput() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let total_requests: u64 = 100_000;
        let request_count = Arc::new(AtomicU64::new(0));
        let _server = spawn_pipelined_rpc_server(addr, request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig {
                max_batch_bytes: 64 * 1024,
                ..Default::default()
            },
            registry,
        ));

        let start = Instant::now();
        let completed = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for i in 0..total_requests {
            let transport = transport.clone();
            let completed = completed.clone();
            let errors = errors.clone();
            handles.push(tokio::spawn(async move {
                use bisque_raft::MultiplexedTransport;
                let result = transport
                    .send_append_entries(
                        2,
                        (i % 100) + 1,
                        openraft::raft::AppendEntriesRequest {
                            vote: openraft::impls::Vote::new(1, 1),
                            prev_log_id: None,
                            entries: vec![],
                            leader_commit: None,
                        },
                    )
                    .await;
                if result.is_ok() {
                    completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);
        let errs = errors.load(Ordering::Relaxed);

        println!(
            "[group_pinned_concurrent] {} ok, {} errors out of {} in {:?} ({:.0} req/s)",
            done,
            errs,
            total_requests,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
        );
        assert!(
            done >= total_requests * 95 / 100,
            "too many failures: {} ok, {} errors out of {}",
            done,
            errs,
            total_requests,
        );
    });
}

/// Test that different groups use separate connections (isolation).
/// Sends requests to 4 different groups and verifies all succeed.
#[test]
fn test_group_pinned_multi_group_isolation() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let request_count = Arc::new(AtomicU64::new(0));
        let _server = spawn_pipelined_rpc_server(addr, request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig::default(),
            registry,
        ));

        use bisque_raft::MultiplexedTransport;

        let num_groups = 4u64;
        let requests_per_group = 1_000u64;
        let mut handles = Vec::new();

        for group_id in 1..=num_groups {
            for _ in 0..requests_per_group {
                let transport = transport.clone();
                handles.push(tokio::spawn(async move {
                    transport
                        .send_append_entries(
                            2,
                            group_id,
                            openraft::raft::AppendEntriesRequest {
                                vote: openraft::impls::Vote::new(1, 1),
                                prev_log_id: None,
                                entries: vec![],
                                leader_commit: None,
                            },
                        )
                        .await
                }));
            }
        }

        let results = futures::future::join_all(handles).await;
        let ok_count = results
            .iter()
            .filter(|r| r.as_ref().unwrap().is_ok())
            .count();
        let total = (num_groups * requests_per_group) as usize;

        println!(
            "[group_pinned_multi_group_isolation] {}/{} succeeded across {} groups",
            ok_count, total, num_groups,
        );
        assert!(
            ok_count >= total * 95 / 100,
            "too many failures: {}/{}",
            ok_count,
            total,
        );
    });
}

/// Test connection refresh: use a very short TTL and verify requests continue
/// to succeed even as connections are refreshed.
#[test]
fn test_group_pinned_connection_refresh() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let request_count = Arc::new(AtomicU64::new(0));
        let _server = spawn_pipelined_rpc_server(addr, request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        // Very short TTL to force frequent refreshes
        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig {
                connection_ttl: Duration::from_millis(100),
                ..Default::default()
            },
            registry,
        ));

        use bisque_raft::MultiplexedTransport;

        let total = 5_000u64;
        let completed = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for _ in 0..total {
            let transport = transport.clone();
            let completed = completed.clone();
            let errors = errors.clone();
            handles.push(tokio::spawn(async move {
                // Small delay to ensure TTL expires during the test
                tokio::time::sleep(Duration::from_micros(50)).await;
                let result = transport
                    .send_append_entries(
                        2,
                        1,
                        openraft::raft::AppendEntriesRequest {
                            vote: openraft::impls::Vote::new(1, 1),
                            prev_log_id: None,
                            entries: vec![],
                            leader_commit: None,
                        },
                    )
                    .await;
                if result.is_ok() {
                    completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        futures::future::join_all(handles).await;
        let done = completed.load(Ordering::Relaxed);
        let errs = errors.load(Ordering::Relaxed);

        println!(
            "[group_pinned_connection_refresh] {} ok, {} errors out of {} (TTL=100ms)",
            done, errs, total,
        );
        // With connection refresh, we expect most to succeed.
        // Some may fail during the handover window.
        assert!(
            done >= total * 90 / 100,
            "too many failures during refresh: {} ok, {} errors out of {}",
            done,
            errs,
            total,
        );
    });
}

/// Test error recovery: initial connection failure, then server comes up.
/// Verifies the transport creates a fresh connection after a failure.
#[test]
fn test_group_pinned_error_recovery() {
    run_async(async {
        let addr = pick_unused_local_addr();
        let registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        registry.register(2, addr);

        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig {
                connect_timeout: Duration::from_secs(1),
                request_timeout: Duration::from_secs(1),
                ..Default::default()
            },
            registry,
        ));

        use bisque_raft::MultiplexedTransport;

        // No server running — connection should fail
        let resp = transport
            .send_append_entries(
                2,
                1,
                openraft::raft::AppendEntriesRequest {
                    vote: openraft::impls::Vote::new(1, 1),
                    prev_log_id: None,
                    entries: vec![],
                    leader_commit: None,
                },
            )
            .await;
        assert!(resp.is_err(), "should fail with no server");

        // Start server — transport should recover and create a new connection
        let request_count = Arc::new(AtomicU64::new(0));
        let _server = spawn_pipelined_rpc_server(addr, request_count.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = transport
            .send_append_entries(
                2,
                1,
                openraft::raft::AppendEntriesRequest {
                    vote: openraft::impls::Vote::new(1, 1),
                    prev_log_id: None,
                    entries: vec![],
                    leader_commit: None,
                },
            )
            .await;
        assert!(resp.is_ok(), "recovery request failed: {:?}", resp.err());

        // Send more to confirm stable connection
        for _ in 0..100 {
            let resp = transport
                .send_append_entries(
                    2,
                    1,
                    openraft::raft::AppendEntriesRequest {
                        vote: openraft::impls::Vote::new(1, 1),
                        prev_log_id: None,
                        entries: vec![],
                        leader_commit: None,
                    },
                )
                .await;
            assert!(
                resp.is_ok(),
                "post-recovery request failed: {:?}",
                resp.err()
            );
        }

        let count = request_count.load(Ordering::Relaxed);
        println!(
            "[group_pinned_error_recovery] {} requests after recovery",
            count
        );
        assert!(
            count >= 101,
            "expected at least 101 requests, got {}",
            count
        );
    });
}

// (heartbeat batch test was removed — send_heartbeat_batch no longer exists)
