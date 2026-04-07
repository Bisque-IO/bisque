//! Real network stack benchmarks.
//!
//! Exercises the full `BisqueTcpTransport → TCP → BisqueRpcServer → Raft` path
//! using transport-level RPCs (`send_append_entries`, `send_vote`) that go over
//! the wire. Single-node cluster (no replication) with a no-op state machine
//! to minimize Raft overhead and isolate networking performance.
//!
//! NOTE: `client_write()` is a LOCAL Raft call that never touches the network.
//! These benchmarks use the transport directly to exercise the real TCP path.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{SocketAddr, TcpListener};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bisque_raft::BisqueRaftTypeConfig;
use bisque_raft::codec;
use bisque_raft::network::MultiplexedTransport;
use bisque_raft::test_support::TestTempDir;
use bisque_raft::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MmapStorageConfig, MultiRaftManager, MultiplexedLogStorage,
    NodeAddressResolver,
};
use futures::FutureExt;
use openraft::OptionalSend;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::entry::RaftEntry;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;

// ---------------------------------------------------------------------------
// Shared types
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
        .expect("runtime");
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
            if let openraft::EntryPayload::Normal(_) = &entry.payload {
                self.applied_normal.fetch_add(1, Ordering::Relaxed);
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
        _snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), std::io::Error> {
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<TestSnapshot>, std::io::Error> {
        Ok(None)
    }
}

impl openraft::RaftSnapshotBuilder<TestConfig> for TestStateMachine {
    async fn build_snapshot(&mut self) -> Result<TestSnapshot, std::io::Error> {
        Ok(openraft::storage::Snapshot {
            meta: openraft::storage::SnapshotMeta {
                last_log_id: None,
                last_membership: TestStoredMembership::default(),
                snapshot_id: "bench".to_string(),
            },
            snapshot: std::io::Cursor::new(Vec::new()),
        })
    }
}

// ---------------------------------------------------------------------------
// Latency stats
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

// ---------------------------------------------------------------------------
// Single-node harness
// ---------------------------------------------------------------------------

struct SingleNodeHarness {
    transport: Arc<BisqueTcpTransport<TestConfig>>,
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

impl SingleNodeHarness {
    async fn new() -> Self {
        Self::with_transport_config(BisqueTcpTransportConfig::default()).await
    }

    async fn with_transport_config(transport_config: BisqueTcpTransportConfig) -> Self {
        let addr = pick_unused_local_addr();
        let node_registry = Arc::new(DefaultNodeRegistry::<u32>::new());
        node_registry.register(1, addr);

        let dir = TestTempDir::new();
        let storage = MultiplexedLogStorage::<TestConfig>::new(
            MmapStorageConfig::new(dir.path())
                .with_segment_size(64 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .expect("storage");

        // Client-side transport (sends RPCs over TCP)
        let transport = Arc::new(BisqueTcpTransport::<TestConfig>::new(
            transport_config,
            node_registry.clone(),
        ));

        // Server-side transport (used by Raft for inter-node RPCs — unused in single-node)
        let server_transport = BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig::default(),
            node_registry,
        );

        let manager = Arc::new(MultiRaftManager::<TestConfig, _, _, TestStateMachine>::new(
            server_transport,
            storage,
        ));

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

        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            transport,
            manager,
            server,
            serve_handle,
            _dir: dir,
        }
    }

    async fn add_groups(&self, group_ids: &[u64]) {
        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 500,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            }
            .validate()
            .expect("raft config"),
        );

        let mut members = BTreeMap::new();
        members.insert(1u32, openraft::impls::BasicNode::default());

        for &gid in group_ids {
            self.manager
                .add_group(gid, 1, raft_cfg.clone(), TestStateMachine::new())
                .await
                .expect("add group");
        }

        for &gid in group_ids {
            let raft = self.manager.get_group(gid).expect("group exists");
            tokio::time::timeout(Duration::from_secs(5), raft.initialize(members.clone()))
                .await
                .expect("init timeout")
                .expect("init ok");
        }

        for &gid in group_ids {
            let raft = self.manager.get_group(gid).expect("group exists");
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let m = raft.metrics().borrow_watched().clone();
                    if m.current_leader == Some(1) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("leader election timeout");
        }
    }

    async fn shutdown(self) {
        self.manager.shutdown_all().await;
        self.server.shutdown_and_drain(Duration::from_secs(5)).await;
        self.manager.storage().stop();
        let _ = self.serve_handle.await;
    }
}

// ---------------------------------------------------------------------------
// Helper: build an AppendEntriesRequest with N entries of given payload size
// ---------------------------------------------------------------------------

fn make_append_request(
    entries: usize,
    payload_size: usize,
) -> openraft::raft::AppendEntriesRequest<TestConfig> {
    let vote = openraft::impls::Vote::new(1, 1);
    let entry_vec: Vec<openraft::alias::DefaultEntryOf<TestConfig>> = (0..entries)
        .map(|i| {
            let log_id = openraft::LogId::new(
                openraft::impls::leader_id_adv::LeaderId::new(1, 1),
                (i + 1) as u64,
            );
            openraft::Entry::new_normal(log_id, TestData(vec![0x42u8; payload_size]))
        })
        .collect();

    openraft::raft::AppendEntriesRequest {
        vote,
        prev_log_id: None,
        entries: entry_vec,
        leader_commit: None,
    }
}

fn make_empty_append_request() -> openraft::raft::AppendEntriesRequest<TestConfig> {
    openraft::raft::AppendEntriesRequest {
        vote: openraft::impls::Vote::new(1, 1),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    }
}

fn make_vote_request(term: u32) -> openraft::raft::VoteRequest<TestConfig> {
    openraft::raft::VoteRequest {
        vote: openraft::impls::Vote::new(term, 1u32),
        last_log_id: None,
    }
}

// ===========================================================================
// Benchmarks — all use transport.send_*() which goes over TCP.
// Fixed worker pool: N tasks loop sending requests sequentially.
// ===========================================================================

const CONCURRENCY: usize = 32;

/// Empty append_entries (heartbeat-like) — pure server overhead, no log entries.
/// Shows the baseline cost of: TCP round-trip + codec encode/decode + Raft processing.
#[test]
fn bench_heartbeat_throughput() {
    run_async(async {
        let harness = SingleNodeHarness::new().await;
        harness.add_groups(&[1]).await;

        let total: u64 = 100_000;
        let per_worker = total / CONCURRENCY as u64;
        let completed = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut handles = Vec::new();

        for w in 0..CONCURRENCY {
            let transport = harness.transport.clone();
            let completed = completed.clone();
            let errors = errors.clone();
            let n = per_worker
                + if (w as u64) < total % CONCURRENCY as u64 {
                    1
                } else {
                    0
                };
            handles.push(tokio::spawn(async move {
                for _ in 0..n {
                    match transport
                        .send_append_entries(1, 1, make_empty_append_request())
                        .await
                    {
                        Ok(_) => {
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);
        let errs = errors.load(Ordering::Relaxed);

        println!(
            "[heartbeat] {} ok, {} errors out of {} in {:?} ({:.0} req/s)",
            done,
            errs,
            total,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
        );
        assert!(
            done >= total * 95 / 100,
            "too many failures: {} ok, {} errors",
            done,
            errs,
        );

        harness.shutdown().await;
    });
}

/// Append entries with varying payload sizes — measures codec + framing overhead.
#[test]
fn bench_append_entries_payload_sizes() {
    run_async(async {
        let harness = SingleNodeHarness::new().await;
        harness.add_groups(&[1]).await;

        for &(payload_size, count) in &[
            (0usize, 100_000u64),
            (128, 100_000),
            (1024, 50_000),
            (16384, 10_000),
            (65536, 10_000),
        ] {
            let per_worker = count / CONCURRENCY as u64;
            let completed = Arc::new(AtomicU64::new(0));

            let start = Instant::now();
            let mut handles = Vec::new();

            for w in 0..CONCURRENCY {
                let transport = harness.transport.clone();
                let completed = completed.clone();
                let n = per_worker
                    + if (w as u64) < count % CONCURRENCY as u64 {
                        1
                    } else {
                        0
                    };
                handles.push(tokio::spawn(async move {
                    for _ in 0..n {
                        let req = make_append_request(1, payload_size);
                        if transport.send_append_entries(1, 1, req).await.is_ok() {
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }));
            }

            futures::future::join_all(handles).await;
            let elapsed = start.elapsed();
            let done = completed.load(Ordering::Relaxed);
            let total_bytes = done * (payload_size as u64);

            println!(
                "[append {}B] {} ok / {} in {:?} ({:.0} req/s, {:.1} MB/s)",
                payload_size,
                done,
                count,
                elapsed,
                done as f64 / elapsed.as_secs_f64(),
                total_bytes as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64(),
            );
        }

        harness.shutdown().await;
    });
}

/// Multi-group concurrent throughput — exercises group-pinned connection fanout.
/// Each group_id gets a dedicated TCP connection to the server.
#[test]
fn bench_multi_group_fanout() {
    run_async(async {
        let harness = SingleNodeHarness::new().await;
        let num_groups = 10u64;
        let group_ids: Vec<u64> = (1..=num_groups).collect();
        harness.add_groups(&group_ids).await;

        let requests_per_group: u64 = 10_000;
        let total = num_groups * requests_per_group;
        let completed = Arc::new(AtomicU64::new(0));

        // One worker per group — each loops sending requests sequentially.
        let start = Instant::now();
        let mut handles = Vec::new();

        for gid in 1..=num_groups {
            let transport = harness.transport.clone();
            let completed = completed.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..requests_per_group {
                    if transport
                        .send_append_entries(1, gid, make_empty_append_request())
                        .await
                        .is_ok()
                    {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);

        println!(
            "[multi_group] {} groups x {} req = {} total, {} ok in {:?} ({:.0} req/s)",
            num_groups,
            requests_per_group,
            total,
            done,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
        );
        assert!(
            done >= total * 95 / 100,
            "too many failures: {}/{}",
            done,
            total,
        );

        harness.shutdown().await;
    });
}

/// Vote RPC throughput — lightweight request type, best-case server overhead.
#[test]
fn bench_vote_throughput() {
    run_async(async {
        let harness = SingleNodeHarness::new().await;
        harness.add_groups(&[1]).await;

        let total: u64 = 100_000;
        let per_worker = total / CONCURRENCY as u64;
        let completed = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut handles = Vec::new();

        for w in 0..CONCURRENCY {
            let transport = harness.transport.clone();
            let completed = completed.clone();
            let n = per_worker
                + if (w as u64) < total % CONCURRENCY as u64 {
                    1
                } else {
                    0
                };
            let base = w as u64 * per_worker + (w as u64).min(total % CONCURRENCY as u64);
            handles.push(tokio::spawn(async move {
                for i in 0..n {
                    if transport
                        .send_vote(1, 1, make_vote_request((base + i) as u32))
                        .await
                        .is_ok()
                    {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        futures::future::join_all(handles).await;
        let elapsed = start.elapsed();
        let done = completed.load(Ordering::Relaxed);

        println!(
            "[vote] {} ok / {} in {:?} ({:.0} req/s)",
            done,
            total,
            elapsed,
            done as f64 / elapsed.as_secs_f64(),
        );
        assert!(
            done >= total * 95 / 100,
            "too many failures: {}/{}",
            done,
            total,
        );

        harness.shutdown().await;
    });
}

/// Latency distribution for append_entries under steady concurrent load.
#[test]
fn bench_append_latency() {
    run_async(async {
        let harness = SingleNodeHarness::new().await;
        harness.add_groups(&[1]).await;

        // Warmup — sequential, single worker
        for _ in 0..500 {
            let _ = harness
                .transport
                .send_append_entries(1, 1, make_empty_append_request())
                .await;
        }

        let total: u64 = 10_000;
        let per_worker = total / CONCURRENCY as u64;

        let mut handles = Vec::new();

        for w in 0..CONCURRENCY {
            let transport = harness.transport.clone();
            let n = per_worker
                + if (w as u64) < total % CONCURRENCY as u64 {
                    1
                } else {
                    0
                };
            handles.push(tokio::spawn(async move {
                let mut local_samples = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let t0 = Instant::now();
                    if transport
                        .send_append_entries(1, 1, make_empty_append_request())
                        .await
                        .is_ok()
                    {
                        local_samples.push(t0.elapsed());
                    }
                }
                local_samples
            }));
        }

        let results = futures::future::join_all(handles).await;
        let all_samples: Vec<Duration> = results.into_iter().flat_map(|r| r.unwrap()).collect();

        let mut stats = LatencyStats {
            samples: all_samples,
        };
        let summary = stats.compute();

        println!("[append_latency] concurrency={}", CONCURRENCY);
        println!("  {}", summary);

        harness.shutdown().await;
    });
}

/// Connection refresh throughput — verifies that TTL-based connection refresh
/// does not drop requests or significantly degrade throughput.
///
/// Runs the same workload twice:
///   1. Baseline: long TTL (no refreshes during the test)
///   2. Refresh: short TTL (many refreshes during the test)
///
/// Asserts zero errors in both runs and that refresh throughput is >= 70%
/// of baseline (generous margin for CI variability).
#[test]
fn bench_connection_refresh_throughput() {
    run_async(async {
        let total: u64 = 50_000;

        // ---- Baseline run: long TTL, no refreshes ----
        let baseline_harness = SingleNodeHarness::with_transport_config(BisqueTcpTransportConfig {
            connection_ttl: Duration::from_secs(300),
            ..Default::default()
        })
        .await;
        baseline_harness.add_groups(&[1]).await;

        let (baseline_done, baseline_errs, baseline_elapsed) =
            run_throughput_workload(&baseline_harness.transport, total, 1).await;
        baseline_harness.shutdown().await;

        let baseline_rps = baseline_done as f64 / baseline_elapsed.as_secs_f64();
        println!(
            "[conn_refresh baseline] {} ok, {} errors in {:?} ({:.0} req/s)",
            baseline_done, baseline_errs, baseline_elapsed, baseline_rps,
        );
        assert_eq!(
            baseline_errs, 0,
            "baseline should have zero errors, got {}",
            baseline_errs,
        );

        // ---- Refresh run: 50ms TTL, forces ~60+ refreshes over a ~3s test ----
        let refresh_harness = SingleNodeHarness::with_transport_config(BisqueTcpTransportConfig {
            connection_ttl: Duration::from_millis(50),
            ..Default::default()
        })
        .await;
        refresh_harness.add_groups(&[1]).await;

        let (refresh_done, refresh_errs, refresh_elapsed) =
            run_throughput_workload(&refresh_harness.transport, total, 1).await;
        refresh_harness.shutdown().await;

        let refresh_rps = refresh_done as f64 / refresh_elapsed.as_secs_f64();
        println!(
            "[conn_refresh refresh] {} ok, {} errors in {:?} ({:.0} req/s)",
            refresh_done, refresh_errs, refresh_elapsed, refresh_rps,
        );
        assert_eq!(
            refresh_errs, 0,
            "refresh run should have zero errors, got {}",
            refresh_errs,
        );

        let ratio = refresh_rps / baseline_rps;
        println!(
            "[conn_refresh] ratio={:.2} (refresh/baseline: {:.0}/{:.0})",
            ratio, refresh_rps, baseline_rps,
        );
        assert!(
            ratio >= 0.70,
            "throughput with refresh ({:.0} req/s) dropped to {:.0}% of baseline ({:.0} req/s)",
            refresh_rps,
            ratio * 100.0,
            baseline_rps,
        );
    });
}

/// Shared workload runner: 32 workers, each looping sequentially.
/// Returns (completed, errors, elapsed).
async fn run_throughput_workload(
    transport: &Arc<BisqueTcpTransport<TestConfig>>,
    total: u64,
    group_id: u64,
) -> (u64, u64, Duration) {
    let per_worker = total / CONCURRENCY as u64;
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::new();

    for w in 0..CONCURRENCY {
        let transport = transport.clone();
        let completed = completed.clone();
        let errors = errors.clone();
        let n = per_worker
            + if (w as u64) < total % CONCURRENCY as u64 {
                1
            } else {
                0
            };
        handles.push(tokio::spawn(async move {
            for _ in 0..n {
                match transport
                    .send_append_entries(1, group_id, make_empty_append_request())
                    .await
                {
                    Ok(_) => {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    futures::future::join_all(handles).await;
    let elapsed = start.elapsed();
    (
        completed.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
        elapsed,
    )
}
