//! Production pipeline benchmark for bisque-mq command ingestion.
//!
//! Benchmarks two ingestion paths using the real Raft cluster:
//!
//! **Local path**: `LocalWriter::send` → `LocalBatcher` → `raft.client_write`
//! → state machine apply
//!
//! **Forward path**: `ForwardClient::forward` → TCP → `ForwardAcceptor`
//! → `raft.client_write` → state machine apply
//!
//! Both paths produce `TAG_FORWARDED_BATCH` log entries; semantics are identical.
//!
//! Usage:
//!   cargo run --release --example forward_bench
//!   cargo run --release --example forward_bench -- --messages 500000 --sizes 16,1024

use std::collections::BTreeMap;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use openraft::{BasicNode, Raft};
use tempfile::TempDir;

use bisque_mq::async_apply::{ClientRegistry, ResponderBroadcast};
use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::forward::RaftBacklog;
use bisque_mq::{
    ForwardAcceptor, ForwardClient, ForwardConfig, ForwardFrameBatch, LocalBatcher,
    LocalFrameBatch, MqStateMachine, MqTypeConfig, MqWriteBatcherConfig, RaftWriter,
    RaftWriterConfig,
};

use bisque_raft::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapStorageConfig,
    MultiRaftManager, MultiplexedLogStorage, NodeAddressResolver,
};

// ─── Raft cluster type aliases ────────────────────────────────────────────────

type MqManager = MultiRaftManager<
    MqTypeConfig,
    BisqueTcpTransport<MqTypeConfig>,
    MultiplexedLogStorage<MqTypeConfig>,
    MqStateMachine,
>;

// ─── Configuration ───────────────────────────────────────────────────────────

struct BenchConfig {
    messages: usize,
    sizes: Vec<usize>,
}

impl BenchConfig {
    fn from_args() -> Self {
        let mut messages = 200_000;
        let mut sizes = vec![64, 256, 1024, 4096];
        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--messages" | "-n" => {
                    i += 1;
                    messages = args[i].parse().expect("invalid --messages");
                }
                "--sizes" | "-s" => {
                    i += 1;
                    sizes = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().expect("invalid size"))
                        .collect();
                }
                "--help" | "-h" => {
                    eprintln!("Usage: forward_bench [--messages N] [--sizes 64,256,1024]");
                    std::process::exit(0);
                }
                _ => {
                    eprintln!("unknown arg: {}", args[i]);
                    std::process::exit(1);
                }
            }
            i += 1;
        }
        Self { messages, sizes }
    }
}

// ─── Formatting ──────────────────────────────────────────────────────────────

fn format_throughput(count: usize, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    let msgs_per_sec = count as f64 / secs;
    if msgs_per_sec >= 1_000_000.0 {
        format!("{:.2}M msg/s", msgs_per_sec / 1_000_000.0)
    } else if msgs_per_sec >= 1_000.0 {
        format!("{:.1}K msg/s", msgs_per_sec / 1_000.0)
    } else {
        format!("{:.0} msg/s", msgs_per_sec)
    }
}

fn format_bandwidth(bytes: usize, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    let bps = bytes as f64 / secs;
    if bps >= 1_073_741_824.0 {
        format!("{:.2} GB/s", bps / 1_073_741_824.0)
    } else if bps >= 1_048_576.0 {
        format!("{:.1} MB/s", bps / 1_048_576.0)
    } else {
        format!("{:.1} KB/s", bps / 1024.0)
    }
}

fn format_latency(elapsed: Duration, count: usize) -> String {
    let nanos = elapsed.as_nanos() as f64 / count as f64;
    if nanos >= 1_000_000.0 {
        format!("{:.2} ms", nanos / 1_000_000.0)
    } else if nanos >= 1_000.0 {
        format!("{:.1} us", nanos / 1_000.0)
    } else {
        format!("{:.0} ns", nanos)
    }
}

fn print_result(label: &str, count: usize, payload_size: usize, elapsed: Duration) {
    let total_bytes = count * (payload_size + 16);
    println!(
        "  {:<52} {:>12}  {:>12}  {:>10}",
        label,
        format_throughput(count, elapsed),
        format_bandwidth(total_bytes, elapsed),
        format_latency(elapsed, count),
    );
}

/// Like `print_result` but prefixes `\r` to overwrite the current progress line.
fn print_result_cr(label: &str, count: usize, payload_size: usize, elapsed: Duration) {
    let total_bytes = count * (payload_size + 16);
    println!(
        "\r  {:<52} {:>12}  {:>12}  {:>10}",
        label,
        format_throughput(count, elapsed),
        format_bandwidth(total_bytes, elapsed),
        format_latency(elapsed, count),
    );
}

/// Print a live progress line (no newline, overwrites with `\r` on next call).
fn print_progress(label: &str, count: usize, payload_size: usize, elapsed: Duration) {
    let total_bytes = count * (payload_size + 16);
    print!(
        "\r  {:<52} {:>12}  {:>12}  {:>10}",
        label,
        format_throughput(count, elapsed),
        format_bandwidth(total_bytes, elapsed),
        format_latency(elapsed, count.max(1)),
    );
    let _ = std::io::stdout().flush();
}

fn print_header() {
    println!(
        "  {:<52} {:>12}  {:>12}  {:>10}",
        "Benchmark", "Throughput", "Bandwidth", "Latency"
    );
    println!("  {}", "-".repeat(92));
}

// ─── Raft cluster setup ───────────────────────────────────────────────────────

async fn setup_raft(
    backlog: Arc<RaftBacklog>,
) -> (Raft<MqTypeConfig, MqStateMachine>, Arc<MqManager>, TempDir) {
    let tmp = TempDir::new_in(".tmp").unwrap();
    let node_id = 1u32;
    let group_id = 0u64;

    let mq_config = MqConfig::new(tmp.path().join("mq").to_str().unwrap());
    let engine = MqEngine::new(mq_config);
    let sm = MqStateMachine::new(engine).with_raft_backlog(Arc::clone(&backlog));

    let storage_config = MmapStorageConfig::new(tmp.path().join("raft"))
        .with_prealloc_pool_size(16)
        .with_fsync_delay(Duration::from_millis(250))
        .with_disable_fsync(false)
        .with_disable_crc(true);

    let storage = MultiplexedLogStorage::new(storage_config).await.unwrap();

    let node_registry = Arc::new(DefaultNodeRegistry::new());
    node_registry.register(node_id, "127.0.0.1:0".parse().unwrap());
    let transport = BisqueTcpTransport::new(BisqueTcpTransportConfig::default(), node_registry);

    // let log_store = mem_storage::MemLogStorage::new();

    let manager: Arc<MqManager> = Arc::new(MultiRaftManager::new(transport, storage));
    let raft_config = Arc::new(
        openraft::Config {
            heartbeat_interval: 1000,
            election_timeout_min: 2000,
            election_timeout_max: 4000,
            max_payload_entries: 1024,
            max_append_entries: Some(1024),
            api_channel_size: Some(1024),
            state_machine_channel_size: Some(1024),
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = manager
        .add_group(group_id, node_id, raft_config, sm)
        // .add_group_with_storage(
        // group_id,
        // node_id,
        // raft_config,
        // sm,
        // mem_storage::MemLogStorage::new(),
        // )
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    raft.initialize(members).await.unwrap();

    for _ in 0..60 {
        if raft.current_leader().await == Some(node_id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        raft.current_leader().await,
        Some(node_id),
        "node must become leader"
    );

    (raft, manager, tmp)
}

// ─── Local path benchmark ─────────────────────────────────────────────────────
//
// LocalWriter::send → LocalBatcher drain loop → raft.client_write → state machine apply
//
// Completion: LocalBatcher::shutdown() joins the drain task, which only exits
// after all raft.client_write() calls have returned (commit + apply).
//
// Sub-frame packing: each LocalWriter::send() carries PACK_SIZE sub-frames so
// the batcher always receives a full batch and proposes immediately.  Sending
// one sub-frame at a time leaves try_recv() empty on every loop iteration
// (the sender hasn't written the next frame yet), collapsing batch size to 1
// and capping throughput at ~1/raft_rt.  In production a connection task
// buffers a read-cycle's worth of commands before calling send(), so pre-packing
// is the realistic workload.

/// Compute sub-frames per send so each LocalFrameBatch stays under
/// `max_raft_entry_bytes`.  At least 1 sub-frame per send.
fn pack_size(frame_wire_len: usize, max_entry_bytes: usize) -> u32 {
    // Reserve 32 bytes for the TAG_FORWARDED_BATCH header.
    let usable = max_entry_bytes.saturating_sub(32);
    (usable / frame_wire_len).max(1) as u32
}

async fn bench_local_batcher(
    raft_writer: Arc<RaftWriter>,
    messages: usize,
    payload_size: usize,
    num_clients: usize,
) {
    let total = messages * num_clients;

    // Ensure no leftover backlog from a previous benchmark (e.g. forward path).
    {
        let bl = raft_writer.backlog();
        let cur = bl.current();
        if cur > 0 {
            eprintln!("    [sync] waiting for {cur} bytes of leftover backlog...");
            bl.wait_for_drain().await;
        }
        let avail = bl.semaphore_permits();
        let max = bl.max();
        assert_eq!(
            avail, max,
            "BUG: semaphore permits ({avail}) != max ({max}) before benchmark start"
        );
    }

    // Sub-frame wire format: [payload_len:4][client_id:4][request_seq:8][cmd_bytes...]
    let frame_body_len = 12 + payload_size;
    let frame_wire_len = 4 + frame_body_len; // length prefix + body

    let max_entry = bisque_mq::write_batcher::DEFAULT_MAX_RAFT_ENTRY_BYTES;
    let ps = pack_size(frame_wire_len, max_entry);
    let config = MqWriteBatcherConfig {
        max_batch_count: ps as usize,
        channel_capacity: 1024,
        ..Default::default()
    };
    let batcher = LocalBatcher::new(Arc::clone(&raft_writer), 0, config);

    let label = format!(
        "  local ({num_clients} client{})",
        if num_clients == 1 { "" } else { "s" }
    );

    // ── Pre-build all batches (excluded from timing) ─────────────────────
    let mut per_client_batches: Vec<Vec<LocalFrameBatch>> = Vec::with_capacity(num_clients);
    for cid in 0..num_clients as u32 {
        let mut batches = Vec::new();
        let mut buf = BytesMut::with_capacity(ps as usize * frame_wire_len);
        let mut packed: u32 = 0;
        let mut seq: u64 = 0;

        for _ in 0..messages {
            buf.put_u32_le(frame_body_len as u32);
            buf.put_u32_le(cid);
            buf.put_u64_le(seq);
            buf.put_bytes(0, payload_size);
            packed += 1;
            seq += 1;

            if packed == ps {
                batches.push(LocalFrameBatch {
                    bytes: buf.split().freeze(),
                    count: packed,
                });
                packed = 0;
            }
        }
        if packed > 0 {
            batches.push(LocalFrameBatch {
                bytes: buf.freeze(),
                count: packed,
            });
        }
        per_client_batches.push(batches);
    }

    // ── Timed section: send only ─────────────────────────────────────────
    let sent = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Spawn 250ms progress printer.
    let progress = {
        let sent = Arc::clone(&sent);
        let label = label.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                let n = sent.load(Ordering::Relaxed) as usize;
                if n > 0 {
                    print_progress(&label, n, payload_size, start.elapsed());
                }
            }
        })
    };

    // Spawn one task per client — sends pre-built batches.
    let mut handles = Vec::with_capacity(num_clients);
    for batches in per_client_batches {
        let writer = batcher.writer();
        let sent = Arc::clone(&sent);
        handles.push(tokio::spawn(async move {
            for batch in batches {
                let count = batch.count;
                writer.send(batch).await.unwrap();
                sent.fetch_add(count as u64, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Grab stats before shutdown (which consumes batcher).
    let bs = Arc::clone(batcher.stats());

    // Drain the batcher channel, then wait for the RaftWriter backlog to reach
    // zero (all proposals committed + applied by the state machine).
    batcher.shutdown().await;
    let pre_drain_backlog = raft_writer.backlog().current();
    let drain_start = Instant::now();
    tokio::select! {
        _ = raft_writer.backlog().wait_for_drain() => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            eprintln!("  WARNING: local batcher backlog drain timed out (backlog={})", raft_writer.backlog().current());
        }
    }
    let drain_elapsed = drain_start.elapsed();

    progress.abort();
    print_result_cr(&label, total, payload_size, start.elapsed());

    // Diagnostic: charge/release balance
    let bl = raft_writer.backlog();
    let charged = bl.total_charged.load(std::sync::atomic::Ordering::Relaxed);
    let released = bl.total_released.load(std::sync::atomic::Ordering::Relaxed);
    let charges = bl.charge_count.load(std::sync::atomic::Ordering::Relaxed);
    let releases = bl.release_count.load(std::sync::atomic::Ordering::Relaxed);
    let post_avail = bl.semaphore_permits();
    let post_max = bl.max();
    let total_elapsed = start.elapsed();
    let send_elapsed = total_elapsed - drain_elapsed;
    eprintln!(
        "    timing: send={:.1}ms drain={:.1}ms total={:.1}ms | pre_drain_backlog={:.1} MB",
        send_elapsed.as_secs_f64() * 1000.0,
        drain_elapsed.as_secs_f64() * 1000.0,
        total_elapsed.as_secs_f64() * 1000.0,
        pre_drain_backlog as f64 / 1024.0 / 1024.0,
    );
    eprintln!(
        "    backlog: charged={:.1} MB released={:.1} MB | charges={} releases={} (expect={}) | permits={}/{}",
        charged as f64 / 1024.0 / 1024.0,
        released as f64 / 1024.0 / 1024.0,
        charges,
        releases,
        total / ps.max(1) as usize, // expected entry count
        post_avail,
        post_max,
    );
    if charges != releases {
        eprintln!(
            "    BUG: charge_count ({}) != release_count ({}) — {} entries not released!",
            charges,
            releases,
            charges as i64 - releases as i64,
        );
    }
    if post_avail != post_max {
        eprintln!(
            "    BUG: permits mismatch after drain! avail={} max={} (delta={})",
            post_avail,
            post_max,
            post_avail as i64 - post_max as i64,
        );
    }
    bl.reset_counters();

    // Print batching stats.
    let bs = &bs;
    let batches = bs.batch_count.load(Ordering::Relaxed);
    let payload = bs.total_payload_bytes.load(Ordering::Relaxed);
    let recvs = bs.channel_recvs.load(Ordering::Relaxed);
    let rs = raft_writer.stats();
    let proposals = rs.proposals.load(Ordering::Relaxed);
    let entries = rs.entries.load(Ordering::Relaxed);
    let rbytes = rs.bytes.load(Ordering::Relaxed);
    let prop_errs = rs.proposal_errors.load(Ordering::Relaxed);
    let entry_errs = rs.entry_errors.load(Ordering::Relaxed);
    let err_bytes = rs.error_released_bytes.load(Ordering::Relaxed);
    if batches > 0 && proposals > 0 {
        eprintln!(
            "    drain: {} batches, avg {:.1} KB payload, {:.1} recvs/batch | raft: {} proposals, {:.1} entries/proposal, avg {:.1} KB/entry",
            batches,
            payload as f64 / batches as f64 / 1024.0,
            recvs as f64 / batches as f64,
            proposals,
            entries as f64 / proposals as f64,
            rbytes as f64 / entries as f64 / 1024.0,
        );
    }
    if prop_errs > 0 || entry_errs > 0 {
        eprintln!(
            "    ERRORS: {} proposal failures, {} entry failures, {:.1} MB released on error",
            prop_errs,
            entry_errs,
            err_bytes as f64 / 1024.0 / 1024.0,
        );
    }
    rs.reset();
}

// ─── Forward path benchmark ───────────────────────────────────────────────────
//
// ForwardClient::forward → OutboundBuf → TCP → ForwardAcceptor reads → charge
// backlog → raft.client_write → state machine apply → release backlog
//
// The forward() call provides end-to-end backpressure: it blocks when the
// OutboundBuf is full, which happens when TCP is blocked, which happens when
// the leader pauses reads via backlog.wait_for_capacity().  This means
// send_elapsed naturally captures full-pipeline throughput.
//
// For e2e_elapsed we additionally drain the final in-flight batch: we poll
// the backlog until it reaches zero, using the "ever seen non-zero" flag to
// guard against checking before the first batch has been charged.

async fn bench_forward_path(
    raft_writer: Arc<RaftWriter>,
    messages: usize,
    payload_size: usize,
    num_clients: usize,
) {
    let total = messages * num_clients;
    let config = ForwardConfig::default();

    let mut acceptor = ForwardAcceptor::start(
        config.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&raft_writer),
        ResponderBroadcast::new_empty(),
    )
    .await
    .unwrap();
    let acceptor_addr = acceptor.local_addr();

    // Simulated follower: node_id=2 distinguishes it from the Raft leader (node_id=1).
    let client_registry = ClientRegistry::new(4, 16384); // returns Arc<ClientRegistry>
    let mut fc = ForwardClient::start(config, 2, 0, Some(acceptor_addr), client_registry);

    // Wait for handshake to complete.
    for _ in 0..100 {
        if acceptor.connected_nodes() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        acceptor.connected_nodes() >= 1,
        "ForwardClient must connect to acceptor"
    );

    // Sub-frame wire format: [payload_len:4][client_id:4][request_seq:8][cmd_bytes...]
    let frame_body_len = 12 + payload_size;
    let frame_wire_len = 4 + frame_body_len; // length prefix + body
    let max_entry = bisque_mq::write_batcher::DEFAULT_MAX_RAFT_ENTRY_BYTES;
    let ps = pack_size(frame_wire_len, max_entry);

    let send_label = format!(
        "  forward send  ({num_clients} client{})",
        if num_clients == 1 { "" } else { "s" }
    );
    let e2e_label = format!(
        "  forward e2e   ({num_clients} client{})",
        if num_clients == 1 { "" } else { "s" }
    );

    let sent = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Spawn 250ms progress printer for the send phase.
    let progress_send = {
        let sent = Arc::clone(&sent);
        let label = send_label.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.tick().await;
            loop {
                interval.tick().await;
                let n = sent.load(Ordering::Relaxed) as usize;
                if n > 0 {
                    print_progress(&label, n, payload_size, start.elapsed());
                }
            }
        })
    };

    // Spawn one task per client — each packs sub-frames and sends batches
    // through ForwardWriter (channel-based, zero cross-core contention).
    let mut send_handles = Vec::with_capacity(num_clients);
    for cid in 0..num_clients as u32 {
        let writer = fc.writer();
        let sent = Arc::clone(&sent);
        send_handles.push(tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(ps as usize * frame_wire_len);
            let mut packed: u32 = 0;
            let mut seq: u64 = 0;

            for _ in 0..messages {
                buf.put_u32_le(frame_body_len as u32);
                buf.put_u32_le(cid);
                buf.put_u64_le(seq);
                buf.put_bytes(0, payload_size);
                packed += 1;
                seq += 1;

                if packed == ps {
                    writer
                        .send(ForwardFrameBatch {
                            bytes: buf.split().freeze(),
                            count: packed,
                        })
                        .await
                        .unwrap();
                    sent.fetch_add(packed as u64, Ordering::Relaxed);
                    packed = 0;
                }
            }
            // Flush any remaining sub-frames.
            if packed > 0 {
                writer
                    .send(ForwardFrameBatch {
                        bytes: buf.freeze(),
                        count: packed,
                    })
                    .await
                    .unwrap();
                sent.fetch_add(packed as u64, Ordering::Relaxed);
            }
        }));
    }
    for h in send_handles {
        h.await.unwrap();
    }
    progress_send.abort();
    let send_elapsed = start.elapsed();
    print_result_cr(&send_label, total, payload_size, send_elapsed);

    // Spawn 250ms progress printer for the e2e drain phase.
    let progress_e2e = {
        let label = e2e_label.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.tick().await;
            loop {
                interval.tick().await;
                let elapsed = start.elapsed();
                print_progress(&label, total, payload_size, elapsed);
            }
        })
    };

    // Shut down the ForwardClient (cancel drain loop + TCP), then wait for
    // all charged batches to be applied.  Cancel drops at most 1 in-flight
    // batch — negligible.  Most data already flowed through Raft during
    // the send phase (writers experienced backpressure).
    fc.shutdown().await;

    tokio::select! {
        _ = raft_writer.backlog().wait_for_drain() => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            eprintln!("  WARNING: forward path backlog drain timed out (backlog={})", raft_writer.backlog().current());
        }
    }

    progress_e2e.abort();
    let e2e_elapsed = start.elapsed();

    acceptor.shutdown().await;

    print_result_cr(&e2e_label, total, payload_size, e2e_elapsed);

    // Print raft batching stats for forward path.
    let rs = raft_writer.stats();
    let proposals = rs.proposals.load(Ordering::Relaxed);
    let entries = rs.entries.load(Ordering::Relaxed);
    let rbytes = rs.bytes.load(Ordering::Relaxed);
    if proposals > 0 {
        eprintln!(
            "    raft: {} proposals, {:.1} entries/proposal, avg {:.1} KB/entry",
            proposals,
            entries as f64 / proposals as f64,
            rbytes as f64 / entries as f64 / 1024.0,
        );
    }
    rs.reset();
    raft_writer.backlog().reset_counters();
}

// ─── In-memory raft storage (benchmark-only) ────────────────────────────────
//
// No disk, no mmap, no CRC. IOFlushed fires immediately. Entries are discarded
// after commit — only vote and log state are tracked in memory.

mod mem_storage {
    use std::io;
    use std::sync::atomic::{AtomicU64, Ordering};

    use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
    use openraft::{LogId, OptionalSend, RaftTypeConfig};

    use bisque_mq::MqTypeConfig;

    type C = MqTypeConfig;

    #[derive(Clone)]
    pub struct MemLogStorage {
        last_log_id: Option<openraft::alias::LogIdOf<C>>,
        last_purged_log_id: Option<openraft::alias::LogIdOf<C>>,
        vote: Option<<C as RaftTypeConfig>::Vote>,
        committed: Arc<AtomicU64>,
        entries:
            Arc<parking_lot::Mutex<std::collections::BTreeMap<u64, <C as RaftTypeConfig>::Entry>>>,
    }

    use std::sync::Arc;

    impl MemLogStorage {
        pub fn new() -> Self {
            Self {
                last_log_id: None,
                last_purged_log_id: None,
                vote: None,
                committed: Arc::new(AtomicU64::new(0)),
                entries: Arc::new(parking_lot::Mutex::new(std::collections::BTreeMap::new())),
            }
        }
    }

    impl RaftLogReader<C> for MemLogStorage {
        async fn try_get_log_entries<
            RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
        >(
            &mut self,
            range: RB,
        ) -> Result<Vec<<C as RaftTypeConfig>::Entry>, io::Error> {
            let entries = self.entries.lock();
            Ok(entries.range(range).map(|(_, e)| e.clone()).collect())
        }

        async fn read_vote(&mut self) -> Result<Option<<C as RaftTypeConfig>::Vote>, io::Error> {
            Ok(self.vote.clone())
        }
    }

    impl RaftLogStorage<C> for MemLogStorage {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
            Ok(LogState {
                last_purged_log_id: self.last_purged_log_id,
                last_log_id: self.last_log_id,
            })
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }

        async fn save_vote(&mut self, vote: &<C as RaftTypeConfig>::Vote) -> Result<(), io::Error> {
            self.vote = Some(vote.clone());
            Ok(())
        }

        async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
        where
            I: IntoIterator<Item = <C as RaftTypeConfig>::Entry> + Send,
        {
            use bisque_raft::codec::{BorrowPayload, Decode};
            use openraft::entry::RaftEntry;
            let mut map = self.entries.lock();
            for mut entry in entries {
                // Materialize scattered commands into contiguous form.
                // This forces allocation + memcpy (simulating real storage)
                // and ensures the stored entry can be decoded correctly
                // (scattered MqCommands have empty buf, causing panics on tag()).
                if let openraft::EntryPayload::Normal(ref data) = entry.payload {
                    let primary = data.payload_bytes();
                    let extra = data.extra_payload_segments();
                    let total =
                        primary.len() + extra.iter().map(|s| s.as_ref().len()).sum::<usize>();
                    let mut buf = Vec::with_capacity(total);
                    buf.extend_from_slice(primary);
                    for seg in extra {
                        buf.extend_from_slice(seg.as_ref());
                    }
                    // Decode from contiguous bytes — same as mmap read path.
                    let contiguous =
                        bisque_mq::MqCommand::decode_from_bytes(bytes::Bytes::from(buf))
                            .expect("decode contiguous command");
                    entry.payload = openraft::EntryPayload::Normal(contiguous);
                }
                let idx = entry.log_id().index;
                self.last_log_id = Some(entry.log_id().clone());
                map.insert(idx, entry);
            }
            drop(map);
            // Fire immediately — no fsync, no delay.
            callback.io_completed(Ok(()));
            Ok(())
        }

        async fn truncate_after(
            &mut self,
            after: Option<openraft::alias::LogIdOf<C>>,
        ) -> Result<(), io::Error> {
            if let Some(ref log_id) = after {
                let idx = log_id.index;
                self.last_log_id = Some(*log_id);
                let mut map = self.entries.lock();
                let to_remove: Vec<_> = map.range((idx + 1)..).map(|(&k, _)| k).collect();
                for k in to_remove {
                    map.remove(&k);
                }
            } else {
                self.last_log_id = None;
                self.entries.lock().clear();
            }
            Ok(())
        }

        async fn purge(&mut self, log_id: openraft::alias::LogIdOf<C>) -> Result<(), io::Error> {
            self.last_purged_log_id = Some(log_id);
            let mut map = self.entries.lock();
            let to_remove: Vec<_> = map.range(..=log_id.index).map(|(&k, _)| k).collect();
            for k in to_remove {
                map.remove(&k);
            }
            Ok(())
        }
    }
}

// ─── No-op network factory (benchmark-only, single-node) ────────────────────

mod mem_network {
    use openraft::error::{RPCError, StreamingError};
    use openraft::network::RPCOption;
    use openraft::network::RaftNetworkFactory;
    use openraft::network::v2::RaftNetworkV2;
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::storage::Snapshot;

    use bisque_mq::MqTypeConfig;

    type C = MqTypeConfig;

    pub struct NoopNetworkFactory;

    pub struct NoopNetwork;

    impl RaftNetworkFactory<C> for NoopNetworkFactory {
        type Network = NoopNetwork;

        async fn new_client(&mut self, _target: u32, _node: &openraft::BasicNode) -> Self::Network {
            NoopNetwork
        }
    }

    impl RaftNetworkV2<C> for NoopNetwork {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<C>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<C>, RPCError<C, openraft::errors::Infallible>> {
            unreachable!("single-node: no replication")
        }

        async fn full_snapshot(
            &mut self,
            _vote: <C as openraft::RaftTypeConfig>::Vote,
            _snapshot: openraft::alias::SnapshotOf<C>,
            _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
            _option: RPCOption,
        ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
            unreachable!("single-node: no snapshot replication")
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<C>,
            _option: RPCOption,
        ) -> Result<VoteResponse<C>, RPCError<C, openraft::errors::Infallible>> {
            unreachable!("single-node: no vote RPC")
        }
    }
}

// ─── Raw raft benchmark ──────────────────────────────────────────────────────
//
// Bypasses RaftWriter, backlog, LocalBatcher — calls raft.client_write_many()
// directly with pre-built MqCommands on an in-memory raft (no disk, no network).
// Measures pure raft core throughput.

async fn setup_mem_raft() -> Raft<MqTypeConfig, bisque_mq::MqStateMachine> {
    use bisque_mq::MqStateMachine;
    use bisque_mq::config::MqConfig;
    use bisque_mq::engine::MqEngine;

    let tmp = TempDir::new_in(".tmp").unwrap();
    let mq_config = MqConfig::new(tmp.path().join("mq").to_str().unwrap());
    let engine = MqEngine::new(mq_config);
    let sm = MqStateMachine::new(engine);

    let log_store = mem_storage::MemLogStorage::new();
    let network = mem_network::NoopNetworkFactory;

    let raft_config = Arc::new(
        openraft::Config {
            heartbeat_interval: 1000,
            election_timeout_min: 2000,
            election_timeout_max: 4000,
            max_payload_entries: 1024,
            max_append_entries: Some(1024),
            api_channel_size: Some(1024),
            state_machine_channel_size: Some(1024),
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let raft = Raft::new(1u32, raft_config, network, log_store, sm)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u32, openraft::BasicNode::default());
    raft.initialize(members).await.unwrap();

    for _ in 0..60 {
        if raft.current_leader().await == Some(1u32) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(raft.current_leader().await, Some(1u32));

    // Leak the TempDir so the state machine directory stays alive.
    std::mem::forget(tmp);

    raft
}

async fn bench_raw_raft(
    raft: Raft<MqTypeConfig, bisque_mq::MqStateMachine>,
    backlog: Arc<RaftBacklog>,
    messages: usize,
    payload_size: usize,
    num_clients: usize,
) {
    use bisque_mq::types::MqCommand;

    let total = messages * num_clients;

    let frame_body_len = 12 + payload_size;
    let frame_wire_len = 4 + frame_body_len;
    let max_entry = bisque_mq::write_batcher::DEFAULT_MAX_RAFT_ENTRY_BYTES;
    let ps = pack_size(frame_wire_len, max_entry);

    let label = format!(
        "  raw raft ({num_clients} client{})",
        if num_clients == 1 { "" } else { "s" }
    );

    let messages = (messages / num_clients) * num_clients;

    // Pre-build all TAG_FORWARDED_BATCH commands (excluded from timing).
    let mut per_client_cmds: Vec<Vec<MqCommand>> = Vec::with_capacity(num_clients);
    for cid in 0..num_clients as u32 {
        let mut cmds = Vec::new();
        let mut buf = BytesMut::with_capacity(ps as usize * frame_wire_len);
        let mut packed: u32 = 0;
        let mut seq: u64 = 0;

        for _ in 0..messages {
            buf.put_u32_le(frame_body_len as u32);
            buf.put_u32_le(cid);
            buf.put_u64_le(seq);
            buf.put_bytes(0, payload_size);
            packed += 1;
            seq += 1;

            if packed == ps {
                let payload = buf.split().freeze();
                let total_size = (32 + payload.len()) as u32;
                let mut hdr = BytesMut::with_capacity(32 + payload.len());
                hdr.put_u32_le(total_size);
                hdr.put_u16_le(32); // fixed region size
                hdr.put_u8(MqCommand::TAG_FORWARDED_BATCH);
                hdr.put_u8(0); // flags
                hdr.put_u32_le(0); // node_id
                hdr.put_u32_le(packed); // count
                hdr.put_bytes(0, 16); // batch_seq, leader_seq
                hdr.put_slice(&payload);
                cmds.push(MqCommand::split_from(&mut hdr));
                packed = 0;
            }
        }
        if packed > 0 {
            let payload = buf.freeze();
            let total_size = (32 + payload.len()) as u32;
            let mut hdr = BytesMut::with_capacity(32 + payload.len());
            hdr.put_u32_le(total_size);
            hdr.put_u16_le(32);
            hdr.put_u8(MqCommand::TAG_FORWARDED_BATCH);
            hdr.put_u8(0);
            hdr.put_u32_le(0);
            hdr.put_u32_le(packed);
            hdr.put_bytes(0, 16);
            hdr.put_slice(&payload);
            cmds.push(MqCommand::split_from(&mut hdr));
        }
        per_client_cmds.push(cmds);
    }

    let sent = Arc::new(AtomicU64::new(0));
    let max_inflight = 512usize;
    let all_cmds: Vec<_> = per_client_cmds.into_iter().flatten().collect();

    let start = Instant::now();

    let progress = {
        let sent = Arc::clone(&sent);
        let label = label.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.tick().await;
            loop {
                interval.tick().await;
                let n = sent.load(Ordering::Relaxed) as usize;
                if n > 0 {
                    print_progress(&label, n, payload_size, start.elapsed());
                }
            }
        })
    };

    // Single task per client, pipelining up to `max_inflight` concurrent
    // client_write_ff futures. No task spawning — just FuturesUnordered.

    // let msgs_per_cmd = ps as u64;
    // {
    //     use futures::stream::FuturesUnordered;
    //     use futures::{FutureExt, StreamExt};

    //     let inflight_sem = Arc::new(tokio::sync::Semaphore::new(max_inflight));
    //     let mut pending = FuturesUnordered::new();

    //     for cmd in all_cmds {
    //         let size = cmd.total_encoded_size();
    //         backlog.charge(size).await;

    //         // Wait for an inflight slot.
    //         let permit = inflight_sem.clone().acquire_owned().await.unwrap();

    //         let raft = raft.clone();
    //         let bl = backlog.clone();
    //         pending.push(async move {
    //             if let Err(e) = raft.client_write_ff(cmd, None).await {
    //                 bl.release(size);
    //                 eprintln!("    raw raft write error: {e}");
    //             }
    //             drop(permit);
    //         });

    //         sent.fetch_add(msgs_per_cmd, Ordering::Relaxed);

    //         // Drain any completed futures without blocking.
    //         while let Some(()) = pending.next().now_or_never().flatten() {}
    //     }

    //     // Drain remaining inflight.
    //     while pending.next().await.is_some() {}
    // }

    let msgs_per_cmd = ps as u64;
    {
        use futures::stream::FuturesUnordered;
        use futures::{FutureExt, StreamExt};

        let inflight_sem = Arc::new(tokio::sync::Semaphore::new(max_inflight));
        // let mut pending = FuturesUnordered::new();
        let bl = backlog.clone();

        for cmd in all_cmds {
            let size = cmd.total_encoded_size();
            backlog.charge(size).await;

            // Wait for an inflight slot.
            let permit = inflight_sem.clone().acquire_owned().await.unwrap();
            let sent = Arc::clone(&sent);
            let raft = raft.clone();
            // tokio::spawn(async move {
            //     if let Err(e) = raft.client_write_ff(cmd, None).await {
            //         // bl.release(size);
            //         eprintln!("    raw raft write error: {e}");
            //     }
            //     sent.fetch_add(msgs_per_cmd, Ordering::Relaxed);
            //     drop(permit);
            // });

            if let Err(e) = raft.client_write_ff(cmd, None).await {
                // bl.release(size);
                eprintln!("    raw raft write error: {e}");
            }
            sent.fetch_add(msgs_per_cmd, Ordering::Relaxed);
            drop(permit);
        }

        // Drain remaining inflight.
        // while pending.next().await.is_some() {}
    }

    // Wait for all entries to be applied (state machine releases backlog).
    tokio::select! {
        _ = backlog.wait_for_drain() => {}
        _ = tokio::time::sleep(Duration::from_secs(60)) => {
            eprintln!("    WARNING: raw raft backlog drain timed out");
        }
    }

    progress.abort();
    print_result_cr(&label, total, payload_size, start.elapsed());
}

async fn print_storage_stats(manager: &Arc<MqManager>) {
    let storage = manager.storage();
    let Ok(log_storage) = storage.get_log_storage(0).await else {
        return;
    };
    let s = log_storage.stats();

    let appends = s.append_calls.load(Ordering::Relaxed);
    if appends == 0 {
        return;
    }
    let entries = s.entries_written.load(Ordering::Relaxed);
    let payload_mb = s.payload_bytes_written.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0;
    let record_mb = s.record_bytes_written.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0;
    let append_ms = s.append_nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let lock_ms = s.lock_nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let memcpy_ms = s.memcpy_nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let crc_ms = s.crc_nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let rotations = s.rotations.load(Ordering::Relaxed);
    let rot_ms = s.rotation_nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let prealloc_waits = s.rotation_prealloc_waits.load(Ordering::Relaxed);
    let sync_creates = s.rotation_sync_creates.load(Ordering::Relaxed);

    let other_ms = append_ms - lock_ms - memcpy_ms - crc_ms - rot_ms;
    let append_gbps = if append_ms > 0.0 {
        record_mb / 1024.0 / (append_ms / 1000.0)
    } else {
        0.0
    };

    eprintln!(
        "    storage: {} appends, {} entries, {:.1} MB payload, {:.1} MB records",
        appends, entries, payload_mb, record_mb
    );
    eprintln!(
        "    timing:  append={:.1}ms (lock={:.1} memcpy={:.1} crc={:.1} rotation={:.1} other={:.1}) | {:.2} GB/s",
        append_ms, lock_ms, memcpy_ms, crc_ms, rot_ms, other_ms, append_gbps,
    );
    if rotations > 0 {
        eprintln!(
            "    rotation: {} total, avg {:.1}ms, prealloc_waits={}, sync_creates={}",
            rotations,
            rot_ms / rotations as f64,
            prealloc_waits,
            sync_creates,
        );
    }
    s.reset();
}

// ─── Raw I/O benchmark ───────────────────────────────────────────────────────
//
// Measures the local drive's maximum write throughput independent of raft.
// Tests: mmap write, mmap+CRC, sequential write(2), fdatasync overhead.

fn bench_raw_io(total_bytes: usize) {
    use std::fs::{self, OpenOptions};
    use std::io::Write as IoWrite;

    let dir = std::path::Path::new(".tmp/io_bench");
    fs::create_dir_all(dir).unwrap();

    let entry_size = 1024 * 1024; // 1 MiB per write
    let iterations = total_bytes / entry_size;
    let data = vec![0xABu8; entry_size];

    println!();
    println!("=== Raw I/O Benchmark ===");
    println!(
        "    total: {:.0} MB  entry: {:.0} KB  iterations: {}",
        total_bytes as f64 / 1024.0 / 1024.0,
        entry_size as f64 / 1024.0,
        iterations,
    );
    println!();
    print_header();
    println!();

    // // ── memcpy baseline (memory → memory) ────────────────────────────────
    // {
    //     let mut buf = vec![0u8; entry_size];
    //     let start = Instant::now();
    //     for _ in 0..iterations {
    //         buf.copy_from_slice(&data);
    //         std::hint::black_box(&buf);
    //     }
    //     let elapsed = start.elapsed();
    //     println!(
    //         "  {:<52} {:>12}  {:>12}",
    //         "memcpy (baseline)",
    //         format_bandwidth(total_bytes, elapsed),
    //         format_latency(elapsed, iterations),
    //     );
    // }

    // // ── memcpy + CRC64 ───────────────────────────────────────────────────
    // {
    //     let mut buf = vec![0u8; entry_size];
    //     let start = Instant::now();
    //     for _ in 0..iterations {
    //         buf.copy_from_slice(&data);
    //         let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme);
    //         digest.update(&buf);
    //         std::hint::black_box(digest.finalize());
    //     }
    //     let elapsed = start.elapsed();
    //     println!(
    //         "  {:<52} {:>12}  {:>12}",
    //         "memcpy + CRC64-NVME",
    //         format_bandwidth(total_bytes, elapsed),
    //         format_latency(elapsed, iterations),
    //     );
    // }

    // ── mmap write (pre-allocated file, no fsync) ────────────────────────
    // {
    //     let path = dir.join("mmap_bench.dat");
    //     let file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .truncate(true)
    //         .open(&path)
    //         .unwrap();
    //     file.set_len(total_bytes as u64).unwrap();
    //     let mmap = unsafe {
    //         memmap2::MmapOptions::new()
    //             .len(total_bytes)
    //             .map_mut(&file)
    //             .unwrap()
    //     };
    //     let start = Instant::now();
    //     for i in 0..iterations {
    //         let offset = i * entry_size;
    //         mmap[offset..offset + entry_size].as_ptr(); // fault page
    //         unsafe {
    //             std::ptr::copy_nonoverlapping(
    //                 data.as_ptr(),
    //                 mmap.as_ptr().add(offset) as *mut u8,
    //                 entry_size,
    //             );
    //         }
    //     }
    //     let elapsed = start.elapsed();
    //     println!(
    //         "  {:<52} {:>12}  {:>12}",
    //         "mmap write (no fsync)",
    //         format_bandwidth(total_bytes, elapsed),
    //         format_latency(elapsed, iterations),
    //     );
    //     drop(mmap);
    //     let _ = fs::remove_file(&path);
    // }

    // ── mmap write (pre-faulted, no fsync) ──────────────────────────────
    // {
    //     let path = dir.join("mmap_prefault_bench.dat");
    //     let file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .truncate(true)
    //         .open(&path)
    //         .unwrap();
    //     file.set_len(total_bytes as u64).unwrap();
    //     let mut mmap = unsafe {
    //         memmap2::MmapOptions::new()
    //             .len(total_bytes)
    //             .populate()
    //             .map_mut(&file)
    //             .unwrap()
    //     };
    //     // Write every page to ensure all pages are resident and dirty (no
    //     // faults during the timed section).
    //     mmap.fill(0);
    //     let start = Instant::now();
    //     for i in 0..iterations {
    //         let offset = i * entry_size;
    //         unsafe {
    //             std::ptr::copy_nonoverlapping(
    //                 data.as_ptr(),
    //                 mmap.as_ptr().add(offset) as *mut u8,
    //                 entry_size,
    //             );
    //         }
    //     }
    //     let elapsed = start.elapsed();
    //     println!(
    //         "  {:<52} {:>12}  {:>12}",
    //         "mmap write (pre-faulted, no fsync)",
    //         format_bandwidth(total_bytes, elapsed),
    //         format_latency(elapsed, iterations),
    //     );
    //     drop(mmap);
    //     let _ = fs::remove_file(&path);
    // }

    // ── mmap tuning variants (Linux-only) ────────────────────────────────
    //
    // Test combinations of:
    //   - fallocate (pre-allocate disk blocks, avoids metadata updates on write)
    //   - MAP_POPULATE (kernel pre-faults all pages at mmap time)
    //   - MADV_SEQUENTIAL (aggressive read-ahead, drop-behind)
    //   - MADV_HUGEPAGE (transparent huge pages, 2 MiB TLB entries)
    //   - MADV_WILLNEED (async pre-fault)
    //   - pre-write (dirty all pages to avoid first-write faults)
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;

        struct MmapVariant {
            label: &'static str,
            fallocate: bool,
            populate: bool,
            madvise: Option<libc::c_int>,
            pre_write: bool,
        }

        let variants = [
            // MmapVariant {
            //     label: "mmap + fallocate",
            //     fallocate: true,
            //     populate: false,
            //     madvise: None,
            //     pre_write: false,
            // },
            MmapVariant {
                label: "mmap + fallocate + populate",
                fallocate: true,
                populate: true,
                // madvise: None,
                madvise: Some(
                    // libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED | libc::MADV_POPULATE_READ,
                    // libc::MADV_POPULATE_READ,
                    libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED | libc::MADV_POPULATE_WRITE,
                    // libc::MADV_WILLNEED,
                ),
                pre_write: false,
            },
            // MmapVariant {
            //     label: "mmap + MADV_SEQUENTIAL",
            //     fallocate: false,
            //     populate: false,
            //     madvise: Some(libc::MADV_SEQUENTIAL),
            //     pre_write: false,
            // },
            // MmapVariant {
            //     label: "mmap + MADV_HUGEPAGE",
            //     fallocate: false,
            //     populate: false,
            //     madvise: Some(libc::MADV_HUGEPAGE),
            //     pre_write: false,
            // },
            // MmapVariant {
            //     label: "mmap + populate + MADV_SEQUENTIAL",
            //     fallocate: false,
            //     populate: true,
            //     madvise: Some(libc::MADV_SEQUENTIAL),
            //     pre_write: false,
            // },
            // MmapVariant {
            //     label: "mmap + populate + MADV_HUGEPAGE",
            //     fallocate: false,
            //     populate: true,
            //     madvise: Some(libc::MADV_HUGEPAGE),
            //     pre_write: false,
            // },
            // MmapVariant {
            //     label: "mmap + fallocate + populate + MADV_HUGEPAGE",
            //     fallocate: true,
            //     populate: true,
            //     madvise: Some(libc::MADV_HUGEPAGE),
            //     pre_write: false,
            // },
            // MmapVariant {
            //     label: "mmap + fallocate + pre-write",
            //     fallocate: true,
            //     populate: false,
            //     madvise: None,
            //     pre_write: true,
            // },
            // MmapVariant {
            //     label: "mmap + fallocate + pre-write + MADV_HUGEPAGE",
            //     fallocate: true,
            //     populate: false,
            //     madvise: Some(libc::MADV_HUGEPAGE),
            //     pre_write: true,
            // },
        ];

        for v in &variants {
            let path = dir.join("mmap_variant.dat");
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();

            file.set_len(total_bytes as u64).unwrap();
            // file.sync_all().unwrap();

            let mut opts = memmap2::MmapOptions::new();
            opts.len(total_bytes);
            if v.populate {
                opts.populate();
            }
            opts.no_reserve_swap();
            let mut mmap = unsafe { opts.map_mut(&file).unwrap() };

            if let Some(advice) = v.madvise {
                unsafe {
                    libc::madvise(mmap.as_ptr() as *mut libc::c_void, total_bytes, advice);
                }
            }

            if v.pre_write {
                opts.populate();
                // mmap.fill(0);
            }

            let start = Instant::now();
            for i in 0..iterations {
                let offset = i * entry_size;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        mmap.as_ptr().add(offset) as *mut u8,
                        entry_size,
                    );
                }
            }
            let elapsed = start.elapsed();
            println!(
                "  {:<52} {:>12}  {:>12}",
                v.label,
                format_bandwidth(total_bytes, elapsed),
                format_latency(elapsed, iterations),
            );
            drop(mmap);
            let _ = fs::remove_file(&path);
        }
    }

    // ── mmap write + CRC ─────────────────────────────────────────────────
    {
        let path = dir.join("mmap_crc_bench.dat");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(total_bytes as u64).unwrap();
        let mmap = unsafe {
            memmap2::MmapOptions::new()
                .len(total_bytes)
                .populate()
                .no_reserve_swap()
                .map_mut(&file)
                .unwrap()
        };
        file.set_len(total_bytes as u64).unwrap();
        file.sync_all().unwrap();
        let start = Instant::now();
        for i in 0..iterations {
            let offset = i * entry_size;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    mmap.as_ptr().add(offset) as *mut u8,
                    entry_size,
                );
            }
            let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme);
            digest.update(&mmap[offset..offset + entry_size]);
            std::hint::black_box(digest.finalize());
        }
        let elapsed = start.elapsed();
        println!(
            "  {:<52} {:>12}  {:>12}",
            "mmap write + CRC64-NVME",
            format_bandwidth(total_bytes, elapsed),
            format_latency(elapsed, iterations),
        );
        drop(mmap);
        let _ = fs::remove_file(&path);
    }

    // ── write(2) sequential (no fsync) ───────────────────────────────────
    // {
    //     let path = dir.join("write_bench.dat");
    //     let mut file = OpenOptions::new()
    //         .write(true)
    //         .create(true)
    //         .truncate(true)
    //         .open(&path)
    //         .unwrap();
    //     let start = Instant::now();
    //     for _ in 0..iterations {
    //         file.write_all(&data).unwrap();
    //     }
    //     let elapsed = start.elapsed();
    //     println!(
    //         "  {:<52} {:>12}  {:>12}",
    //         "write(2) sequential (no fsync)",
    //         format_bandwidth(total_bytes, elapsed),
    //         format_latency(elapsed, iterations),
    //     );
    //     drop(file);
    //     let _ = fs::remove_file(&path);
    // }

    // ── write(2) + fdatasync every N writes ──────────────────────────────
    // for sync_interval in [1, 10, 100] {
    //     let label = format!("write(2) + fdatasync every {} writes", sync_interval);
    //     let path = dir.join(format!("write_sync_{}.dat", sync_interval));
    //     let mut file = OpenOptions::new()
    //         .write(true)
    //         .create(true)
    //         .truncate(true)
    //         .open(&path)
    //         .unwrap();
    //     let start = Instant::now();
    //     for i in 0..iterations {
    //         file.write_all(&data).unwrap();
    //         if (i + 1) % sync_interval == 0 {
    //             file.sync_data().unwrap();
    //         }
    //     }
    //     file.sync_data().unwrap(); // final sync
    //     let elapsed = start.elapsed();
    //     println!(
    //         "  {:<52} {:>12}  {:>12}",
    //         label,
    //         format_bandwidth(total_bytes, elapsed),
    //         format_latency(elapsed, iterations),
    //     );
    //     drop(file);
    //     let _ = fs::remove_file(&path);
    // }

    // ── mmap write + fdatasync every N writes ────────────────────────────
    // for sync_interval in [1, 10, 100] {
    //     let label = format!("mmap write + fdatasync every {} writes", sync_interval);
    //     let path = dir.join(format!("mmap_sync_{}.dat", sync_interval));
    //     let file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .truncate(true)
    //         .open(&path)
    //         .unwrap();
    //     file.set_len(total_bytes as u64).unwrap();
    //     let mmap = unsafe {
    //         memmap2::MmapOptions::new()
    //             .len(total_bytes)
    //             .map_mut(&file)
    //             .unwrap()
    //     };
    //     let start = Instant::now();
    //     for i in 0..iterations {
    //         let offset = i * entry_size;
    //         unsafe {
    //             std::ptr::copy_nonoverlapping(
    //                 data.as_ptr(),
    //                 mmap.as_ptr().add(offset) as *mut u8,
    //                 entry_size,
    //             );
    //         }
    //         if (i + 1) % sync_interval == 0 {
    //             file.sync_data().unwrap();
    //         }
    //     }
    //     file.sync_data().unwrap();
    //     let elapsed = start.elapsed();
    //     println!(
    //         "  {:<52} {:>12}  {:>12}",
    //         label,
    //         format_bandwidth(total_bytes, elapsed),
    //         format_latency(elapsed, iterations),
    //     );
    //     drop(mmap);
    //     let _ = fs::remove_file(&path);
    // }

    // ── O_DIRECT write (bypasses page cache) ───────────────────────────
    //
    // O_DIRECT requires aligned buffers and aligned file offsets.
    // This measures raw NVMe device throughput without page-cache interference.
    #[cfg(target_os = "linux")]
    if false {
        use std::os::unix::fs::OpenOptionsExt;

        const ALIGN: usize = 4096;

        // Allocate page-aligned buffer.
        let layout = std::alloc::Layout::from_size_align(entry_size, ALIGN).unwrap();
        let aligned_buf = unsafe {
            let ptr = std::alloc::alloc(layout);
            assert!(!ptr.is_null(), "aligned alloc failed");
            std::ptr::write_bytes(ptr, 0xAB, entry_size);
            std::slice::from_raw_parts(ptr, entry_size)
        };

        // O_DIRECT sequential write (no fsync needed — already durable on completion)
        {
            let path = dir.join("direct_bench.dat");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(libc::O_DIRECT)
                .open(&path)
                .unwrap();
            let start = Instant::now();
            for _ in 0..iterations {
                file.write_all(aligned_buf).unwrap();
            }
            let elapsed = start.elapsed();
            println!(
                "  {:<52} {:>12}  {:>12}",
                "O_DIRECT write (bypass page cache)",
                format_bandwidth(total_bytes, elapsed),
                format_latency(elapsed, iterations),
            );
            drop(file);
            let _ = fs::remove_file(&path);
        }

        // O_DIRECT + fdatasync every N writes
        for sync_interval in [1, 10, 100] {
            let label = format!("O_DIRECT + fdatasync every {} writes", sync_interval);
            let path = dir.join(format!("direct_sync_{}.dat", sync_interval));
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(libc::O_DIRECT)
                .open(&path)
                .unwrap();
            let start = Instant::now();
            for i in 0..iterations {
                file.write_all(aligned_buf).unwrap();
                if (i + 1) % sync_interval == 0 {
                    file.sync_data().unwrap();
                }
            }
            file.sync_data().unwrap();
            let elapsed = start.elapsed();
            println!(
                "  {:<52} {:>12}  {:>12}",
                label,
                format_bandwidth(total_bytes, elapsed),
                format_latency(elapsed, iterations),
            );
            drop(file);
            let _ = fs::remove_file(&path);
        }

        unsafe {
            std::alloc::dealloc(aligned_buf.as_ptr() as *mut u8, layout);
        }
    }

    let _ = fs::remove_dir(dir);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config = BenchConfig::from_args();

    println!();
    println!("=== Command Ingestion Benchmark (Real Raft) ===");
    println!(
        "    messages: {}  sizes: {:?}",
        config.messages, config.sizes
    );
    println!();

    // Run raw I/O benchmark first to establish drive baseline.
    // bench_raw_io(2 * 1024 * 1024 * 1024); // 2 GiB

    for &size in &config.sizes {
        println!("--- payload size: {} bytes ---", size);
        print_header();
        println!();

        // Create backlog first so it can be shared between the state machine
        // (for apply-side release) and the RaftWriter (for submit-side charge).
        let backlog = Arc::new(bisque_mq::forward::RaftBacklog::new(1024 * 1024 * 1024 * 2));
        let (raft, manager, _tmp) = setup_raft(Arc::clone(&backlog)).await;
        let raw_backlog = Arc::clone(&backlog);
        let raft_writer = RaftWriter::with_backlog(
            raft.clone(),
            RaftWriterConfig {
                worker_count: 16,
                queue_capacity: 4096,
                max_backlog_bytes: 512 * 1024 * 1024,
                max_entries_per_proposal: 16,
            },
            backlog,
        );

        for &clients in &[1usize] {
            bench_local_batcher(Arc::clone(&raft_writer), config.messages, size, clients).await;

            bench_forward_path(Arc::clone(&raft_writer), config.messages, size, clients).await;

            println!();
        }

        // Raw raft: in-memory storage + no-op network — isolates raft core.
        // let mem_raft = setup_mem_raft().await;
        // let mem_raft = setup_raft(Arc::clone(&backlog)).await;
        println!();
        for &clients in &[1, 16] {
            // for &clients in &[1usize] {
            bench_raw_raft(
                raft.clone(),
                Arc::clone(&raw_backlog),
                config.messages,
                size,
                clients,
            )
            .await;
            print_storage_stats(&manager).await;
        }
        // let _ = mem_raft.shutdown().await;

        let _ = raft.shutdown().await;
        manager.shutdown_all().await;
    }
}
