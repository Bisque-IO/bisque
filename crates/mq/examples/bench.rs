//! Throughput and latency benchmark for bisque-mq.
//!
//! Measures publish, deliver, and ack performance across all consumer group
//! variants (Offset, Ack, Actor) with configurable message sizes.
//!
//! Usage:
//!   cargo run --release --example bench
//!   cargo run --release --example bench -- --messages 500000 --sizes 64,256,1024,4096

use std::time::Instant;

use bytes::Bytes;

use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::*;

// ─── Configuration ──────────────────────────────────────────────────────────

struct BenchConfig {
    messages: usize,
    sizes: Vec<usize>,
    batch_size: usize,
}

impl BenchConfig {
    fn from_args() -> Self {
        let mut messages = 200_000;
        let mut sizes = vec![64, 256, 1024, 4096, 16384];
        let mut batch_size = 100;

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
                "--batch" | "-b" => {
                    i += 1;
                    batch_size = args[i].parse().expect("invalid --batch");
                }
                "--help" | "-h" => {
                    eprintln!("Usage: bench [--messages N] [--sizes 64,256,1024] [--batch N]");
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
            batch_size,
        }
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/bisque-bench"))
}

fn make_payload(size: usize) -> Bytes {
    Bytes::from(vec![0xABu8; size])
}

fn make_flat_msg(payload: &Bytes) -> Bytes {
    FlatMessageBuilder::new(payload).timestamp(1000).build()
}

fn format_throughput(count: usize, elapsed: std::time::Duration) -> String {
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

fn format_bytes_throughput(bytes: usize, elapsed: std::time::Duration) -> String {
    let secs = elapsed.as_secs_f64();
    let bytes_per_sec = bytes as f64 / secs;
    if bytes_per_sec >= 1_073_741_824.0 {
        format!("{:.2} GB/s", bytes_per_sec / 1_073_741_824.0)
    } else if bytes_per_sec >= 1_048_576.0 {
        format!("{:.1} MB/s", bytes_per_sec / 1_048_576.0)
    } else {
        format!("{:.1} KB/s", bytes_per_sec / 1024.0)
    }
}

fn format_latency(elapsed: std::time::Duration, count: usize) -> String {
    let nanos = elapsed.as_nanos() as f64 / count as f64;
    if nanos >= 1_000_000.0 {
        format!("{:.2} ms", nanos / 1_000_000.0)
    } else if nanos >= 1_000.0 {
        format!("{:.1} us", nanos / 1_000.0)
    } else {
        format!("{:.0} ns", nanos)
    }
}

struct BenchResult {
    label: String,
    count: usize,
    payload_bytes: usize,
    elapsed: std::time::Duration,
}

impl BenchResult {
    fn print(&self) {
        let total_bytes = self.count * self.payload_bytes;
        println!(
            "  {:<42} {:>12}  {:>12}  {:>10}",
            self.label,
            format_throughput(self.count, self.elapsed),
            format_bytes_throughput(total_bytes, self.elapsed),
            format_latency(self.elapsed, self.count),
        );
    }
}

// ─── Benchmark: Topic Publish ───────────────────────────────────────────────

fn bench_topic_publish(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);

    let topic_id = match engine.apply_command(
        &MqCommand::create_topic("bench-topic", RetentionPolicy::default(), 0),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Pre-build batches
    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();
    let num_batches = config.messages / config.batch_size;
    let total = num_batches * config.batch_size;

    let start = Instant::now();
    for i in 0..num_batches {
        let log_index = (i * config.batch_size + 2) as u64;
        engine.apply_command(
            &MqCommand::publish(topic_id, &batch),
            log_index,
            1000 + i as u64,
            None,
        );
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: format!("topic::publish  batch={}", config.batch_size),
        count: total,
        payload_bytes: msg_size,
        elapsed,
    }
}

// ─── Benchmark: Topic Publish (single message) ─────────────────────────────

fn bench_topic_publish_single(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);
    let single = [flat_msg];

    let topic_id = match engine.apply_command(
        &MqCommand::create_topic("bench-topic-single", RetentionPolicy::default(), 0),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let count = config.messages.min(100_000); // cap single-message bench

    let cmd = &MqCommand::publish(topic_id, &single);
    let start = Instant::now();
    for i in 0..count {
        engine.apply_command(
            cmd,
            // &MqCommand::publish(topic_id, &single),
            (i + 2) as u64,
            1000 + i as u64,
            None,
        );
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: "topic::publish  batch=1".to_string(),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

// ─── Benchmark: Offset Consumer Group (commit offsets) ──────────────────────

fn bench_offset_commit(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);

    // Create topic + offset consumer group
    let topic_id = match engine.apply_command(
        &MqCommand::create_topic("offset-topic", RetentionPolicy::default(), 0),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group_id = match engine.apply_command(
        &MqCommand::create_consumer_group("offset-group", 1),
        2,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Publish messages
    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();
    let num_batches = config.messages / config.batch_size;
    for i in 0..num_batches {
        engine.apply_command(
            &MqCommand::publish(topic_id, &batch),
            (i * config.batch_size + 10) as u64,
            1000,
            None,
        );
    }

    // Benchmark: commit offsets
    let count = config.messages.min(100_000);
    let start = Instant::now();
    for i in 0..count {
        engine.apply_command(
            &MqCommand::commit_group_offset(
                group_id,
                0, // generation
                topic_id,
                0, // partition
                i as u64,
                None,
                2000 + i as u64,
            ),
            (config.messages + i + 100) as u64,
            2000 + i as u64,
            None,
        );
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: "offset::commit_offset".to_string(),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

// ─── Benchmark: Ack Variant (enqueue + deliver + ack) ───────────────────────

fn bench_ack_enqueue(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "ack-queue",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();
    let num_batches = config.messages / config.batch_size;
    let total = num_batches * config.batch_size;

    let start = Instant::now();
    for i in 0..num_batches {
        let log_index = (i * config.batch_size + 10) as u64;
        engine.apply_command(
            &MqCommand::publish(source_topic_id, &batch),
            log_index,
            1000 + i as u64,
            None,
        );
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: format!("ack::enqueue  batch={}", config.batch_size),
        count: total,
        payload_bytes: msg_size,
        elapsed,
    }
}

fn bench_ack_deliver(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "ack-deliver-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    // Enqueue all messages first
    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();
    let num_batches = config.messages / config.batch_size;
    let total = num_batches * config.batch_size;
    for i in 0..num_batches {
        engine.apply_command(
            &MqCommand::publish(source_topic_id, &batch),
            (i * config.batch_size + 10) as u64,
            1000,
            None,
        );
    }

    // Benchmark: deliver all (in batches)
    let consumer_id = 42u64;
    let mut delivered = 0usize;
    let start = Instant::now();
    loop {
        let resp = engine.apply_command(
            &MqCommand::group_deliver(group_id, consumer_id, config.batch_size as u32),
            (total + delivered + 100) as u64,
            2000,
            None,
        );
        match resp {
            MqResponse::Messages { messages } => {
                if messages.is_empty() {
                    break;
                }
                delivered += messages.len();
            }
            _ => break,
        }
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: format!("ack::deliver  batch={}", config.batch_size),
        count: delivered,
        payload_bytes: msg_size,
        elapsed,
    }
}

fn bench_ack_full_cycle(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "ack-cycle-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();
    let num_batches = config.messages / config.batch_size;
    let total = num_batches * config.batch_size;
    let consumer_id = 42u64;
    let mut log_idx = 10u64;

    // Benchmark: full enqueue → deliver → ack cycle
    let start = Instant::now();
    for _ in 0..num_batches {
        // Enqueue batch
        engine.apply_command(
            &MqCommand::publish(source_topic_id, &batch),
            log_idx,
            1000,
            None,
        );
        log_idx += config.batch_size as u64;

        // Deliver batch
        let resp = engine.apply_command(
            &MqCommand::group_deliver(group_id, consumer_id, config.batch_size as u32),
            log_idx,
            2000,
            None,
        );
        log_idx += 1;

        // Ack all
        if let MqResponse::Messages { messages } = resp {
            let ids: Vec<u64> = messages.iter().map(|m| m.message_id).collect();
            engine.apply_command(
                &MqCommand::group_ack(group_id, &ids, None),
                log_idx,
                2001,
                None,
            );
            log_idx += 1;
        }
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: format!("ack::enqueue+deliver+ack  batch={}", config.batch_size),
        count: total,
        payload_bytes: msg_size,
        elapsed,
    }
}

// ─── Benchmark: Actor Variant ───────────────────────────────────────────────

fn bench_actor_send(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "actor-bench",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Send to N distinct actors, 1 message each (to test actor creation + send)
    let count = config.messages.min(200_000);
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    let actor_config = group.actor_config().unwrap();

    let start = Instant::now();
    for i in 0..count {
        let actor_id = Bytes::from(format!("actor-{i}"));
        actor_state
            .apply_send(
                actor_config,
                group_id,
                &actor_id,
                (i + 100) as u64,
                1000,
                None,
                msg_size as u32,
            )
            .unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: "actor::send (1 msg/actor)".to_string(),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

fn bench_actor_send_same(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();

    let mut actor_config = ActorVariantConfig::default();
    actor_config.max_mailbox_depth = u32::MAX;

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "actor-same-bench",
            actor_config,
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let count = config.messages.min(200_000);
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    let actor_config = group.actor_config().unwrap();
    let actor_id = Bytes::from_static(b"single-actor");

    let start = Instant::now();
    for i in 0..count {
        actor_state
            .apply_send(
                actor_config,
                group_id,
                &actor_id,
                (i + 100) as u64,
                1000,
                None,
                msg_size as u32,
            )
            .unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: "actor::send (N msgs/1 actor)".to_string(),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

fn bench_actor_full_cycle(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "actor-cycle-bench",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let count = config.messages.min(200_000);
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    let actor_config = group.actor_config().unwrap();
    let consumer_id = 42u64;

    // Pre-create actors and assign
    let actor_ids: Vec<Bytes> = (0..count).map(|i| Bytes::from(format!("a-{i}"))).collect();
    for (i, actor_id) in actor_ids.iter().enumerate() {
        actor_state
            .apply_send(
                actor_config,
                group_id,
                actor_id,
                (i + 100) as u64,
                1000,
                None,
                msg_size as u32,
            )
            .unwrap();
    }
    actor_state.apply_assign(consumer_id, &actor_ids);

    // Benchmark: deliver + ack cycle
    let start = Instant::now();
    for actor_id in actor_ids.iter() {
        let msg_idx = actor_state
            .apply_deliver(actor_id, consumer_id, msg_size as u32)
            .unwrap();
        actor_state.apply_ack(actor_id, msg_idx, msg_size as u32);
    }
    let elapsed = start.elapsed();
    drop(group);

    BenchResult {
        label: "actor::deliver+ack".to_string(),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

// ─── Benchmark: Command encoding ────────────────────────────────────────────

fn bench_command_encoding(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);
    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();

    let count = config.messages.min(200_000);

    let start = Instant::now();
    for i in 0..count {
        let _cmd = MqCommand::publish(i as u64, &batch);
        std::hint::black_box(&_cmd);
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: format!("codec::encode_publish  batch={}", config.batch_size),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

fn bench_command_encoding_scatter(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);
    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();

    let count = config.messages.min(200_000);

    let start = Instant::now();
    let mut owned = batch.clone();
    for i in 0..count {
        // Clone the batch Vec to simulate owning a fresh Vec from protocol layer.
        // Bytes::clone is atomic refcount bump — same cost as the contiguous
        // benchmark's &[Bytes] borrow since both start from the same batch.
        // let owned = batch.clone();
        let mut cmd = MqCommand::publish_scatter(i as u64, owned);
        std::hint::black_box(&cmd);
        owned = cmd.take_publish_segments();
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: format!("codec::publish_scatter  batch={}", config.batch_size),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

/// Measures the full pipeline: construct command + encode to Vec<u8> (simulating
/// the Raft log write path). Scatter avoids double-copy: contiguous copies
/// payloads into command buffer then into Raft buffer, scatter copies only into
/// Raft buffer.
fn bench_full_encode_pipeline(config: &BenchConfig, msg_size: usize) -> (BenchResult, BenchResult) {
    use bisque_raft::codec::Encode;

    let payload = make_payload(msg_size);
    let flat_msg = make_flat_msg(&payload);
    let batch: Vec<Bytes> = (0..config.batch_size).map(|_| flat_msg.clone()).collect();
    let count = config.messages.min(200_000);
    let mut encode_buf = Vec::with_capacity(256 * 1024);

    // Contiguous: construct + encode
    let start = Instant::now();
    for i in 0..count {
        let cmd = MqCommand::publish(i as u64, &batch);
        encode_buf.clear();
        cmd.encode(&mut encode_buf).unwrap();
        std::hint::black_box(&encode_buf);
    }
    let elapsed_contig = start.elapsed();

    // Scatter: construct + encode
    let start = Instant::now();
    for i in 0..count {
        let cmd = MqCommand::publish_scatter(i as u64, batch.clone());
        encode_buf.clear();
        cmd.encode(&mut encode_buf).unwrap();
        std::hint::black_box(&encode_buf);
    }
    let elapsed_scatter = start.elapsed();

    (
        BenchResult {
            label: format!("full::contig+encode  batch={}", config.batch_size),
            count,
            payload_bytes: msg_size,
            elapsed: elapsed_contig,
        },
        BenchResult {
            label: format!("full::scatter+encode  batch={}", config.batch_size),
            count,
            payload_bytes: msg_size,
            elapsed: elapsed_scatter,
        },
    )
}

fn bench_flat_message_build(config: &BenchConfig, msg_size: usize) -> BenchResult {
    let payload = make_payload(msg_size);
    let count = config.messages.min(500_000);

    let start = Instant::now();
    for _ in 0..count {
        let msg = FlatMessageBuilder::new(&payload).timestamp(1000).build();
        std::hint::black_box(&msg);
    }
    let elapsed = start.elapsed();

    BenchResult {
        label: "codec::FlatMessageBuilder".to_string(),
        count,
        payload_bytes: msg_size,
        elapsed,
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn main() {
    let config = BenchConfig::from_args();

    println!("bisque-mq benchmark");
    println!("  messages per test : {}", config.messages);
    println!("  batch size        : {}", config.batch_size);
    println!("  payload sizes     : {:?}", config.sizes);
    println!();

    for &msg_size in &config.sizes {
        println!(
            "━━━ payload = {} bytes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            msg_size
        );
        println!(
            "  {:<42} {:>12}  {:>12}  {:>10}",
            "benchmark", "throughput", "bandwidth", "latency"
        );
        println!("  {}", "─".repeat(80));

        // Codec layer
        bench_flat_message_build(&config, msg_size).print();
        bench_command_encoding(&config, msg_size).print();
        bench_command_encoding_scatter(&config, msg_size).print();
        let (contig, scatter) = bench_full_encode_pipeline(&config, msg_size);
        contig.print();
        scatter.print();

        // Topic publish
        bench_topic_publish_single(&config, msg_size).print();
        bench_topic_publish(&config, msg_size).print();

        // Offset consumer group
        bench_offset_commit(&config, msg_size).print();

        // Ack variant (queue)
        bench_ack_enqueue(&config, msg_size).print();
        bench_ack_deliver(&config, msg_size).print();
        bench_ack_full_cycle(&config, msg_size).print();

        // Actor variant
        bench_actor_send(&config, msg_size).print();
        bench_actor_send_same(&config, msg_size).print();
        bench_actor_full_cycle(&config, msg_size).print();

        println!();
    }
}
