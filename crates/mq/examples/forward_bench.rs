//! Forwarding transport benchmark for bisque-mq.
//!
//! Measures throughput and latency for the full forwarding pipeline:
//! follower → TCP → leader → NodeRoute → TCP → follower.
//!
//! Usage:
//!   cargo run --release --example forward_bench
//!   cargo run --release --example forward_bench -- --messages 500000 --sizes 64,256,1024
//!   cargo run --release --example forward_bench -- --partitions 1,2,4,8
//!   cargo run --release --example forward_bench -- --stats        # p50/p99 percentiles

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};

use bisque_mq::async_apply::{ClientRegistry, ResponseCallback};
use bisque_mq::forward::{ForwardAcceptor, ForwardClient, ForwardConfig};

// ─── Configuration ──────────────────────────────────────────────────────────

struct BenchConfig {
    messages: usize,
    sizes: Vec<usize>,
    partitions: Vec<usize>,
    connections: Vec<usize>,
    /// Collect full size distributions and print p50/p99 percentiles.
    stats: bool,
    /// Capacity of the batch_rx channel (leader inbound queue). Default: 4096.
    /// Higher values let more batches queue up, increasing greedy-drain effectiveness.
    inbound_cap: usize,
    /// Capacity of the per-follower responder channel (leader → TCP writer). Default: 256.
    responder_cap: usize,
}

impl BenchConfig {
    fn from_args() -> Self {
        let mut messages = 200_000;
        let mut sizes = vec![64, 256, 1024, 4096];
        let mut partitions = vec![1, 2, 4, 8];
        let mut connections = vec![1, 2, 4, 8];
        let mut stats = false;
        let mut inbound_cap = 65536usize;
        let mut responder_cap = 65536usize;
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
                "--partitions" | "-p" => {
                    i += 1;
                    partitions = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().expect("invalid partition count"))
                        .collect();
                }
                "--connections" | "-c" => {
                    i += 1;
                    connections = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().expect("invalid connection count"))
                        .collect();
                }
                "--stats" => {
                    stats = true;
                }
                "--inbound-cap" => {
                    i += 1;
                    inbound_cap = args[i].parse().expect("invalid --inbound-cap");
                }
                "--responder-cap" => {
                    i += 1;
                    responder_cap = args[i].parse().expect("invalid --responder-cap");
                }
                "--help" | "-h" => {
                    eprintln!(
                        "Usage: forward_bench [--messages N] [--sizes 64,256,1024] \
                         [--partitions 1,2,4,8] [--connections 1,2,4,8] [--stats] \
                         [--inbound-cap N] [--responder-cap N]"
                    );
                    std::process::exit(0);
                }
                _ => {
                    eprintln!("unknown arg: {}", args[i]);
                    std::process::exit(1);
                }
            }
            i += 1;
        }
        Self {
            messages,
            sizes,
            partitions,
            connections,
            stats,
            inbound_cap,
            responder_cap,
        }
    }
}

// ─── Formatting ─────────────────────────────────────────────────────────────

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
        "  {:<50} {:>12}  {:>12}  {:>10}",
        label,
        format_throughput(count, elapsed),
        format_bandwidth(total_bytes, elapsed),
        format_latency(elapsed, count),
    );
}

fn print_header() {
    println!(
        "  {:<50} {:>12}  {:>12}  {:>10}",
        "Benchmark", "Throughput", "Bandwidth", "Latency"
    );
    println!("  {}", "-".repeat(88));
}

// ─── Telemetry ───────────────────────────────────────────────────────────────

/// Collects a distribution of u64 samples.
/// Always tracks min/max/mean; collects full distribution for percentiles when
/// `full` is true (controlled by `--stats`).
struct Dist {
    count: u64,
    total: u64,
    min: u64,
    max: u64,
    samples: Vec<u32>,
    full: bool,
}

impl Dist {
    fn new(full: bool) -> Self {
        Self {
            count: 0,
            total: 0,
            min: u64::MAX,
            max: 0,
            samples: Vec::new(),
            full,
        }
    }

    #[inline]
    fn push(&mut self, n: u64) {
        self.count += 1;
        self.total += n;
        if n < self.min {
            self.min = n;
        }
        if n > self.max {
            self.max = n;
        }
        if self.full {
            self.samples.push(n as u32);
        }
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total as f64 / self.count as f64
        }
    }

    fn percentile(&mut self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples.sort_unstable();
        let idx = ((self.samples.len() as f64 * p / 100.0) as usize).min(self.samples.len() - 1);
        self.samples[idx] as u64
    }

    /// Print stats, indented under the bench result line.
    fn print(&mut self, label: &str) {
        if self.count == 0 {
            return;
        }
        let min = if self.min == u64::MAX { 0 } else { self.min };
        if self.full {
            let p50 = self.percentile(50.0);
            let p99 = self.percentile(99.0);
            println!(
                "    ↳ {:<28} batches={:>6}  min={:>5}  mean={:>7.1}  p50={:>5}  p99={:>5}  max={:>5}",
                label,
                self.count,
                min,
                self.mean(),
                p50,
                p99,
                self.max,
            );
        } else {
            println!(
                "    ↳ {:<28} batches={:>6}  min={:>5}  mean={:>7.1}  max={:>5}",
                label,
                self.count,
                min,
                self.mean(),
                self.max,
            );
        }
    }
}

// ─── Bench: Inbound frame boundary scan ─────────────────────────────────────

fn bench_frame_boundary_scan(messages: usize, payload_size: usize) {
    // Build wire-format buffer: [len:4][client_id:4][payload:N]...
    let mut buf = BytesMut::with_capacity(messages * (4 + 4 + payload_size));
    let payload = vec![0xABu8; payload_size];
    for i in 0..messages as u32 {
        let frame_len = (4 + payload.len()) as u32;
        buf.put_u32_le(frame_len);
        buf.put_u32_le(i);
        buf.put_slice(&payload);
    }

    // Simulate the frame boundary scan in run_inbound_loop.
    let start = Instant::now();
    let mut pos = 0usize;
    let mut count = 0u32;
    while pos + 4 <= buf.len() {
        let frame_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        if frame_len == 0 || pos + 4 + frame_len > buf.len() {
            break;
        }
        pos += 4 + frame_len;
        count += 1;
    }
    let elapsed = start.elapsed();
    assert_eq!(count as usize, messages);

    std::hint::black_box(pos);
    print_result("  frame boundary scan", messages, payload_size, elapsed);
}

// ─── Bench: ForwardedBatch iter parse ───────────────────────────────────────

fn bench_batch_iter_parse(messages: usize, payload_size: usize) {
    // Build wire-format buffer: [len:4][client_id:4][payload:N]... (no padding).
    let mut buf = BytesMut::with_capacity(messages * (4 + 4 + payload_size));
    let payload = vec![0xABu8; payload_size];
    for i in 0..messages as u32 {
        let frame_len = (4 + payload.len()) as u32;
        buf.put_u32_le(frame_len);
        buf.put_u32_le(i);
        buf.put_slice(&payload);
    }
    let frozen = buf.freeze();

    // Parse using the same logic as ForwardedBatchIter.
    let start = Instant::now();
    let mut pos = 0usize;
    let mut count = 0usize;
    while pos + 4 <= frozen.len() {
        let frame_len = u32::from_le_bytes(frozen[pos..pos + 4].try_into().unwrap()) as usize;
        if frame_len < 4 || pos + 4 + frame_len > frozen.len() {
            break;
        }
        let payload_start = pos + 4;
        let client_id =
            u32::from_le_bytes(frozen[payload_start..payload_start + 4].try_into().unwrap());
        let cmd = &frozen[payload_start + 4..payload_start + frame_len];
        std::hint::black_box(client_id);
        std::hint::black_box(cmd);
        pos = payload_start + frame_len;
        count += 1;
    }
    let elapsed = start.elapsed();
    assert_eq!(count, messages);

    print_result("  batch iter parse", messages, payload_size, elapsed);
}

// ─── Bench: Local ClientRegistry callback dispatch ───────────────────────────
//
// Measures the full async dispatch pipeline:
//   encode_log_index_frame (batch per partition) → send_to_partition_async → ClientPartition task
//   → callback invocation.
//
// Uses a counter + oneshot channel to wait for all callbacks to fire.

async fn bench_local_registry_dispatch(
    messages: usize,
    payload_size: usize,
    num_clients: usize,
    full_stats: bool,
) {
    let registry = ClientRegistry::new(4, 16384);
    let total = messages * num_clients;

    let received = Arc::new(AtomicUsize::new(0));
    // channel(1) is race-free: the signal persists even if sent before recv().await.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

    let mut cids = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        let received_cb = Arc::clone(&received);
        let done_tx_cb = done_tx.clone();
        let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
            if !is_done {
                if received_cb.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                    let _ = done_tx_cb.try_send(());
                }
            }
        });
        cids.push(registry.register(cb));
    }
    // Drop the original sender; only callback clones keep senders alive.
    drop(done_tx);

    // Yield so partition tasks are scheduled before we start sending.
    tokio::task::yield_now().await;

    let n = registry.num_partitions();
    let mut part_bufs: Vec<BytesMut> = (0..n).map(|_| BytesMut::new()).collect();

    // Telemetry: frames per send call per active partition.
    let mut frames_per_send = Dist::new(full_stats);
    // Telemetry: how many partitions are active (non-empty) per message iteration.
    let mut active_parts_per_iter = Dist::new(full_stats);

    let start = Instant::now();
    for _ in 0..messages {
        for &cid in &cids {
            let p = (cid as usize) % n;
            // Frame: [len=20:4][client_id:4][request_seq:8][log_index:8]
            part_bufs[p].put_u32_le(20u32);
            part_bufs[p].put_u32_le(cid);
            part_bufs[p].put_u64_le(0u64); // request_seq
            part_bufs[p].put_u64_le(42u64); // log_index
        }
        let mut active = 0u64;
        for (p, buf) in part_bufs.iter_mut().enumerate() {
            if !buf.is_empty() {
                // Each 24-byte record is one frame (4 len + 4 cid + 8 req_seq + 8 log_idx).
                let nframes = buf.len() as u64 / 24;
                frames_per_send.push(nframes);
                active += 1;
                registry
                    .send_to_partition_async(p, buf.split().freeze())
                    .await;
            }
        }
        active_parts_per_iter.push(active);
    }
    let send_elapsed = start.elapsed();

    done_rx.recv().await;
    let e2e_elapsed = start.elapsed();

    for cid in cids {
        registry.unregister(cid);
    }

    print_result(
        &format!("  registry send    ({num_clients} clients)"),
        total,
        payload_size,
        send_elapsed,
    );
    frames_per_send.print("frames/send");
    active_parts_per_iter.print("active parts/iter");
    print_result(
        &format!("  registry e2e     ({num_clients} clients)"),
        total,
        payload_size,
        e2e_elapsed,
    );
}

// ─── Bench: One-way throughput (follower → leader only) ─────────────────────

async fn bench_one_way_throughput(
    messages: usize,
    payload_size: usize,
    full_stats: bool,
    inbound_cap: usize,
) {
    let config = ForwardConfig {
        inbound_buffer_capacity: inbound_cap,
        ..Default::default()
    };

    let (mut acceptor, mut batch_rx) =
        ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let addr = acceptor.local_addr();

    // ClientRegistry with 4 partitions, capacity 4096.
    let client_registry = ClientRegistry::new(4, 4096);
    let mut client = ForwardClient::start(
        config.clone(),
        1,
        0,
        Some(addr),
        Arc::clone(&client_registry),
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = vec![0xABu8; payload_size];

    // Producer: use client_id=1 (no responses expected).
    let producer_payload = payload.clone();
    let producer_client = client.clone_handle();
    let producer = tokio::spawn(async move {
        for _ in 0..messages {
            producer_client.forward(1u32, 0u64, &producer_payload).await;
        }
    });

    // Consumer on leader side — count individual commands from batches.
    let consumer = tokio::spawn(async move {
        let mut batch_dist = Dist::new(full_stats);
        // Telemetry: batches already queued (try_recv after first recv) shows
        // how much TCP coalescing is occurring from the leader's perspective.
        let mut pending_on_drain = Dist::new(full_stats);

        let start = Instant::now();
        let mut consumed = 0usize;
        while consumed < messages {
            if let Some(batch) = batch_rx.recv().await {
                batch_dist.push(batch.len() as u64);
                consumed += batch.len();

                // Drain any already-queued batches (greedy).
                let mut extra = 0u64;
                while consumed < messages {
                    match batch_rx.try_recv() {
                        Ok(b) => {
                            batch_dist.push(b.len() as u64);
                            consumed += b.len();
                            extra += 1;
                        }
                        Err(_) => break,
                    }
                }
                pending_on_drain.push(extra);

                std::hint::black_box(consumed);
            }
        }
        let elapsed = start.elapsed();
        acceptor.shutdown().await;
        (elapsed, consumed, batch_dist, pending_on_drain)
    });

    let (_, consumer_result) = tokio::join!(producer, consumer);
    let (elapsed, consumed, mut batch_dist, mut pending_dist) = consumer_result.unwrap();

    print_result(
        &format!("  one-way follower->leader ({consumed} msgs)"),
        consumed,
        payload_size,
        elapsed,
    );
    batch_dist.print("rx batch size (msgs)");
    pending_dist.print("extra batches drained");

    client.shutdown().await;
}

// ─── Bench: Full roundtrip (follower → leader → follower) ──────────────────

async fn bench_full_roundtrip(
    messages: usize,
    payload_size: usize,
    num_partitions: usize,
    full_stats: bool,
    inbound_cap: usize,
    responder_cap: usize,
) {
    let config = ForwardConfig {
        responder_channel_capacity: responder_cap,
        inbound_buffer_capacity: inbound_cap,
        ..Default::default()
    };

    let (mut acceptor, mut batch_rx) =
        ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let addr = acceptor.local_addr();

    let client_registry = ClientRegistry::new(num_partitions, 4096);

    // Register one local client; its callback counts received responses.
    let received = Arc::new(AtomicUsize::new(0));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    let received_cb = Arc::clone(&received);
    let done_tx_cb = done_tx.clone();
    let target = messages;
    let callback: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
        if !is_done {
            if received_cb.fetch_add(1, Ordering::Relaxed) + 1 >= target {
                let _ = done_tx_cb.try_send(());
            }
        }
    });
    drop(done_tx);
    let local_cid = client_registry.register(callback);

    let mut client = ForwardClient::start(
        config.clone(),
        1,
        0,
        Some(addr),
        Arc::clone(&client_registry),
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = vec![0xABu8; payload_size];

    // Producer: forwards commands through the client.
    let producer_payload = payload.clone();
    let producer_client = client.clone_handle();
    let producer = tokio::spawn(async move {
        let start = Instant::now();
        for _ in 0..messages {
            producer_client
                .forward(local_cid, 0u64, &producer_payload)
                .await;
        }
        start.elapsed()
    });

    // Leader: receives batches, sends log-index responses via push_response_async.
    // Greedy drain: after blocking recv, try_recv all queued batches before responding.
    // This coalesces responses for multiple TCP batches into one send.
    let leader = tokio::spawn(async move {
        let start = Instant::now();
        let mut consumed = 0usize;

        let mut rx_batch_dist = Dist::new(full_stats); // msgs per TCP batch
        let mut resp_batch_dist = Dist::new(full_stats); // msgs per push_response_async
        let mut drain_dist = Dist::new(full_stats); // extra batches per greedy drain

        while consumed < messages {
            if let Some(batch) = batch_rx.recv().await {
                let node_id = batch.node_id;
                let mut resp = BytesMut::with_capacity(batch.len() * 24);
                rx_batch_dist.push(batch.len() as u64);
                for (client_id, _request_seq, _cmd) in batch.iter() {
                    resp.put_u32_le(20u32);
                    resp.put_u32_le(client_id);
                    resp.put_u64_le(0u64); // request_seq
                    resp.put_u64_le(consumed as u64);
                    consumed += 1;
                }

                // Greedy drain: coalesce all already-queued batches from the same
                // node into a single response send.
                let mut extra = 0u64;
                while consumed < messages {
                    match batch_rx.try_recv() {
                        Ok(b) if b.node_id == node_id => {
                            rx_batch_dist.push(b.len() as u64);
                            for (client_id, _request_seq, _cmd) in b.iter() {
                                resp.put_u32_le(20u32);
                                resp.put_u32_le(client_id);
                                resp.put_u64_le(0u64);
                                resp.put_u64_le(consumed as u64);
                                consumed += 1;
                            }
                            extra += 1;
                        }
                        Ok(b) => {
                            // Different node — put stats but can't unrecv; just process it.
                            rx_batch_dist.push(b.len() as u64);
                            for (client_id, _request_seq, _cmd) in b.iter() {
                                resp.put_u32_le(20u32);
                                resp.put_u32_le(client_id);
                                resp.put_u64_le(0u64);
                                resp.put_u64_le(consumed as u64);
                                consumed += 1;
                            }
                            extra += 1;
                        }
                        Err(_) => break,
                    }
                }
                drain_dist.push(extra);
                resp_batch_dist.push(resp.len() as u64 / 24);
                acceptor.push_response_async(node_id, resp.freeze()).await;
            }
        }
        (
            start.elapsed(),
            acceptor,
            rx_batch_dist,
            resp_batch_dist,
            drain_dist,
        )
    });

    // Consumer: wait for all responses via callback notification.
    let consumer_start = Instant::now();
    done_rx.recv().await;
    let consumer_elapsed = consumer_start.elapsed();

    let (producer_elapsed, leader_result) = tokio::join!(producer, leader);
    let producer_elapsed = producer_elapsed.unwrap();
    let (leader_elapsed, mut acceptor, mut rx_dist, mut resp_dist, mut drain_dist) =
        leader_result.unwrap();

    acceptor.shutdown().await;
    client.shutdown().await;
    client_registry.unregister(local_cid);

    print_result(
        &format!("  producer send    ({num_partitions} parts)"),
        messages,
        payload_size,
        producer_elapsed,
    );
    print_result(
        &format!("  leader apply     ({num_partitions} parts)"),
        messages,
        payload_size,
        leader_elapsed,
    );
    rx_dist.print("rx batch size (msgs)");
    drain_dist.print("extra batches drained");
    resp_dist.print("resp batch size (msgs)");
    print_result(
        &format!("  e2e roundtrip    ({num_partitions} parts)"),
        messages,
        payload_size,
        consumer_elapsed,
    );
}

// ─── Bench: Full roundtrip with N concurrent connections ────────────────────
//
// Spins up `num_conns` ForwardClient instances (simulating N follower nodes),
// each with a dedicated producer task.  All producers start simultaneously via
// a barrier.  Throughput and bandwidth are reported as the aggregate across all
// connections.

async fn bench_full_roundtrip_n_conns(
    messages_per_conn: usize,
    payload_size: usize,
    num_conns: usize,
    full_stats: bool,
    inbound_cap: usize,
    responder_cap: usize,
) {
    use tokio::sync::Barrier;

    let config = ForwardConfig {
        responder_channel_capacity: responder_cap,
        inbound_buffer_capacity: inbound_cap,
        ..Default::default()
    };

    let (mut acceptor, mut batch_rx) =
        ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let addr = acceptor.local_addr();

    // 4 partitions; each connection registers one client.
    let client_registry = ClientRegistry::new(4, 4096);
    let total_messages = messages_per_conn * num_conns;

    // Shared counter + channel for completion signaling.
    let received = Arc::new(AtomicUsize::new(0));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Start N ForwardClients (one per simulated follower node), each with one
    // registered client.
    let mut clients: Vec<ForwardClient> = Vec::with_capacity(num_conns);
    let mut cids: Vec<u32> = Vec::with_capacity(num_conns);
    for node_id in 1..=num_conns as u32 {
        let received_cb = Arc::clone(&received);
        let done_tx_cb = done_tx.clone();
        let total = total_messages;
        let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
            if !is_done {
                if received_cb.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                    let _ = done_tx_cb.try_send(());
                }
            }
        });
        let cid = client_registry.register(cb);
        cids.push(cid);
        // node_id used as the positional arg, not keyword
        clients.push(ForwardClient::start(
            config.clone(),
            node_id,
            0,
            Some(addr),
            Arc::clone(&client_registry),
        ));
    }

    // Drop original sender; only callback clones keep senders alive.
    drop(done_tx);

    // Wait for all TCP connections to establish.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = vec![0xABu8; payload_size];

    // Barrier: all producer tasks + main wait here before starting.
    let barrier = Arc::new(Barrier::new(num_conns + 1));

    // Spawn one producer task per connection.
    let mut producer_handles = Vec::with_capacity(num_conns);
    for (i, &cid) in cids.iter().enumerate() {
        let handle = clients[i].clone_handle();
        let payload = payload.clone();
        let barrier = Arc::clone(&barrier);
        producer_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..messages_per_conn {
                handle.forward(cid, 0u64, &payload).await;
            }
        }));
    }

    // Leader: receive batches from all connections and route responses.
    // Greedy drain to coalesce responses across multiple in-flight batches.
    let leader = tokio::spawn(async move {
        let mut consumed = 0usize;
        let mut rx_batch_dist = Dist::new(full_stats);
        let mut resp_batch_dist = Dist::new(full_stats);
        let mut drain_dist = Dist::new(full_stats);

        // Per-node accumulation buffers: node_id < 64.
        let mut node_bufs: Vec<(u32, BytesMut)> = Vec::with_capacity(8);

        while consumed < total_messages {
            if let Some(batch) = batch_rx.recv().await {
                // Accumulate the first batch.
                rx_batch_dist.push(batch.len() as u64);
                let first_node = batch.node_id;
                let mut first_resp = BytesMut::with_capacity(batch.len() * 24);
                for (client_id, _request_seq, _cmd) in batch.iter() {
                    first_resp.put_u32_le(20u32);
                    first_resp.put_u32_le(client_id);
                    first_resp.put_u64_le(0u64); // request_seq
                    first_resp.put_u64_le(consumed as u64);
                    consumed += 1;
                }
                node_bufs.push((first_node, first_resp));

                // Greedy drain: accumulate per-node so responses reach the right follower.
                let mut extra = 0u64;
                while consumed < total_messages {
                    match batch_rx.try_recv() {
                        Ok(b) => {
                            rx_batch_dist.push(b.len() as u64);
                            let nid = b.node_id;
                            let buf =
                                if let Some(e) = node_bufs.iter_mut().find(|(id, _)| *id == nid) {
                                    &mut e.1
                                } else {
                                    node_bufs.push((nid, BytesMut::with_capacity(b.len() * 24)));
                                    &mut node_bufs.last_mut().unwrap().1
                                };
                            for (client_id, _request_seq, _cmd) in b.iter() {
                                buf.put_u32_le(20u32);
                                buf.put_u32_le(client_id);
                                buf.put_u64_le(0u64); // request_seq
                                buf.put_u64_le(consumed as u64);
                                consumed += 1;
                            }
                            extra += 1;
                        }
                        Err(_) => break,
                    }
                }
                drain_dist.push(extra);

                // Send one coalesced response per node.
                for (nid, buf) in node_bufs.drain(..) {
                    resp_batch_dist.push(buf.len() as u64 / 24);
                    acceptor.push_response_async(nid, buf.freeze()).await;
                }
            }
        }
        (acceptor, rx_batch_dist, resp_batch_dist, drain_dist)
    });

    // Release all producers simultaneously, start the wall clock.
    barrier.wait().await;
    let wall_start = Instant::now();

    // Wait for all producers to finish sending.
    for h in producer_handles {
        h.await.unwrap();
    }
    let prod_elapsed = wall_start.elapsed();

    // Wait for all consumers to receive their responses via callbacks.
    done_rx.recv().await;
    let e2e_elapsed = wall_start.elapsed();

    let (mut acceptor, mut rx_dist, mut resp_dist, mut drain_dist) = leader.await.unwrap();
    acceptor.shutdown().await;
    for mut client in clients {
        client.shutdown().await;
    }
    for cid in cids {
        client_registry.unregister(cid);
    }

    print_result(
        &format!("  producer send    ({num_conns} conns)"),
        total_messages,
        payload_size,
        prod_elapsed,
    );
    rx_dist.print("rx batch size (msgs)");
    drain_dist.print("extra batches drained");
    resp_dist.print("resp batch size (msgs)");
    print_result(
        &format!("  e2e roundtrip    ({num_conns} conns)"),
        total_messages,
        payload_size,
        e2e_elapsed,
    );
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config = BenchConfig::from_args();

    println!();
    println!("=== Forward Transport Benchmark ===");
    println!(
        "    messages: {}  sizes: {:?}  partitions: {:?}  connections: {:?}  stats: {}",
        config.messages,
        config.sizes,
        config.partitions,
        config.connections,
        if config.stats {
            "full (p50/p99)"
        } else {
            "min/mean/max"
        },
    );
    println!(
        "    inbound-cap: {}  responder-cap: {}",
        config.inbound_cap, config.responder_cap,
    );
    println!();

    for &size in &config.sizes {
        println!("--- payload size: {} bytes ---", size);
        print_header();

        // 1. Pure CPU: frame boundary scan.
        bench_frame_boundary_scan(config.messages, size);

        // 2. Pure CPU: batch iter parse.
        bench_batch_iter_parse(config.messages, size);

        // 3. Local ClientRegistry callback dispatch.
        println!();
        for &clients in &[1usize, 4, 16] {
            bench_local_registry_dispatch(config.messages, size, clients, config.stats).await;
        }

        // Scale TCP message count down for larger payloads to keep runtime bounded.
        let scale = (64.0 / size as f64).sqrt().max(0.1);
        let one_way_msgs = ((config.messages as f64 * scale) as usize).max(1000);
        let rt_msgs = ((config.messages as f64 * scale) as usize).max(1000);

        // 4. TCP one-way (follower→leader only).
        println!();
        bench_one_way_throughput(one_way_msgs, size, config.stats, config.inbound_cap).await;

        // 5. Full roundtrip per partition count (single connection).
        println!();
        for &parts in &config.partitions {
            bench_full_roundtrip(
                rt_msgs,
                size,
                parts,
                config.stats,
                config.inbound_cap,
                config.responder_cap,
            )
            .await;
        }

        // 6. Full roundtrip per connection count.
        println!();
        for &conns in &config.connections {
            bench_full_roundtrip_n_conns(
                rt_msgs,
                size,
                conns,
                config.stats,
                config.inbound_cap,
                config.responder_cap,
            )
            .await;
        }

        println!();
    }
}
