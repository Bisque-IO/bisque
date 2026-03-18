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
    let total_bytes = count * payload_size;
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
    let total_bytes = count * payload_size;
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
    let total_bytes = count * payload_size;
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

async fn setup_raft(backlog: Arc<RaftBacklog>) -> (Raft<MqTypeConfig>, Arc<MqManager>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let node_id = 1u64;
    let group_id = 0u64;

    let mq_config = MqConfig::new(tmp.path().join("mq").to_str().unwrap());
    let engine = MqEngine::new(mq_config);
    let sm = MqStateMachine::new(engine).with_raft_backlog(Arc::clone(&backlog));

    let storage_config = MmapStorageConfig::new(tmp.path().join("raft"));
    let storage = MultiplexedLogStorage::new(storage_config).await.unwrap();

    let node_registry = Arc::new(DefaultNodeRegistry::new());
    node_registry.register(node_id, "127.0.0.1:0".parse().unwrap());
    let transport = BisqueTcpTransport::new(BisqueTcpTransportConfig::default(), node_registry);

    let manager: Arc<MqManager> = Arc::new(MultiRaftManager::new(transport, storage));
    let raft_config = Arc::new(
        openraft::Config {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = manager
        .add_group(group_id, node_id, raft_config, sm)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    raft.initialize(members).await.unwrap();

    for _ in 0..60 {
        if raft.current_leader().await == Some(node_id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    use_scattered: bool,
) {
    let total = messages * num_clients;

    // Sub-frame wire format: [payload_len:4][client_id:4][request_seq:8][cmd_bytes...]
    let frame_body_len = 12 + payload_size;
    let frame_wire_len = 4 + frame_body_len; // length prefix + body

    let max_entry = bisque_mq::write_batcher::DEFAULT_MAX_RAFT_ENTRY_BYTES;
    let ps = pack_size(frame_wire_len, max_entry);
    let config = MqWriteBatcherConfig {
        max_batch_count: ps as usize,
        channel_capacity: 1024,
        use_scattered,
        ..Default::default()
    };
    let batcher = LocalBatcher::new(Arc::clone(&raft_writer), 0, config);

    let mode = if use_scattered { "vec" } else { "cpy" };
    let label = format!(
        "  local-{mode} ({num_clients} client{})",
        if num_clients == 1 { "" } else { "s" }
    );
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

    // Spawn one task per client — each packs and sends independently,
    // matching production where each connection task calls writer.send().
    let mut handles = Vec::with_capacity(num_clients);
    for cid in 0..num_clients as u32 {
        let writer = batcher.writer();
        let sent = Arc::clone(&sent);
        handles.push(tokio::spawn(async move {
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
                        .send(LocalFrameBatch {
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
                    .send(LocalFrameBatch {
                        bytes: buf.freeze(),
                        count: packed,
                    })
                    .await
                    .unwrap();
                sent.fetch_add(packed as u64, Ordering::Relaxed);
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
    tokio::select! {
        _ = raft_writer.backlog().wait_for_drain() => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            eprintln!("  WARNING: local batcher backlog drain timed out (backlog={})", raft_writer.backlog().current());
        }
    }

    progress.abort();
    print_result_cr(&label, total, payload_size, start.elapsed());

    // Print batching stats.
    let bs = &bs;
    let batches = bs.batch_count.load(Ordering::Relaxed);
    let payload = bs.total_payload_bytes.load(Ordering::Relaxed);
    let recvs = bs.channel_recvs.load(Ordering::Relaxed);
    let rs = raft_writer.stats();
    let proposals = rs.proposals.load(Ordering::Relaxed);
    let entries = rs.entries.load(Ordering::Relaxed);
    let rbytes = rs.bytes.load(Ordering::Relaxed);
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

    for &size in &config.sizes {
        println!("--- payload size: {} bytes ---", size);
        print_header();
        println!();

        // Create backlog first so it can be shared between the state machine
        // (for apply-side release) and the RaftWriter (for submit-side charge).
        let backlog = Arc::new(bisque_mq::forward::RaftBacklog::new(256 * 1024 * 1024));
        let (raft, manager, _tmp) = setup_raft(Arc::clone(&backlog)).await;
        let raft_writer =
            RaftWriter::with_backlog(raft.clone(), RaftWriterConfig::default(), backlog);

        for &clients in &[1usize, 4, 16] {
            bench_local_batcher(
                Arc::clone(&raft_writer),
                config.messages,
                size,
                clients,
                true, // scattered (vectored)
            )
            .await;

            bench_local_batcher(
                Arc::clone(&raft_writer),
                config.messages,
                size,
                clients,
                false, // contiguous (memcpy)
            )
            .await;

            bench_forward_path(Arc::clone(&raft_writer), config.messages, size, clients).await;

            println!();
        }

        let _ = raft.shutdown().await;
        manager.shutdown_all().await;
    }
}
