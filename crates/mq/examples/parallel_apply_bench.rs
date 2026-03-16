//! Benchmark: async apply throughput through a real raft mmap log.
//!
//! Measures how many publish commands the async apply workers can process
//! per second, varying the number of partition workers and active topics.
//! Commands are written to a real mmap raft log and workers pull them
//! via cursor-based sequential scanning.
//!
//! Run with: cargo run --release -p bisque-mq --example parallel_apply_bench

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tempfile::TempDir;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::async_runtime::{AsyncRuntime, oneshot::Oneshot};
use openraft::{EntryPayload, LogId};

use bisque_mq::MqTypeConfig;
use bisque_mq::async_apply::AsyncApplyManager;
use bisque_mq::config::MqConfig;
use bisque_mq::config::ParallelApplyConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::*;

use bisque_raft::{MmapPerGroupLogStorage, MmapStorageConfig};

type Rt = <MqTypeConfig as openraft::RaftTypeConfig>::AsyncRuntime;
type Os = <Rt as AsyncRuntime>::Oneshot;

// =============================================================================
// Helpers
// =============================================================================

use bisque_mq::async_apply::ResponseEntry;
use bytes::BytesMut;

fn apply_engine(
    engine: &bisque_mq::engine::MqEngine,
    cmd: &MqCommand,
    log_index: u64,
    current_time: u64,
) -> ResponseEntry {
    let mut buf = BytesMut::new();
    engine.apply_command(cmd, &mut buf, log_index, current_time, None);
    ResponseEntry::split_from(&mut buf)
}

fn make_entry(index: u64, term: u64, cmd: MqCommand) -> openraft::impls::Entry<MqTypeConfig> {
    openraft::impls::Entry::<MqTypeConfig> {
        log_id: LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
            index,
        },
        payload: EntryPayload::Normal(cmd),
    }
}

fn make_callback() -> (
    IOFlushed<MqTypeConfig>,
    <Os as Oneshot>::Receiver<Result<(), std::io::Error>>,
) {
    let (tx, rx) = Os::channel::<Result<(), std::io::Error>>();
    let cb = IOFlushed::<MqTypeConfig>::signal(tx);
    (cb, rx)
}

async fn append_and_flush(
    log: &mut bisque_raft::MmapGroupLogStorage<MqTypeConfig>,
    entries: Vec<openraft::impls::Entry<MqTypeConfig>>,
) {
    let (cb, rx) = make_callback();
    log.append(entries, cb).await.unwrap();
    rx.await.unwrap().unwrap();
}

fn make_flat_msg(payload_size: usize) -> Bytes {
    let value = vec![0xABu8; payload_size];
    FlatMessageBuilder::new(&value).timestamp(1000).build()
}

// =============================================================================
// Benchmark scenarios
// =============================================================================

struct BenchResult {
    partitions: usize,
    topics: usize,
    total_msgs: u64,
    elapsed_ms: f64,
    msgs_per_sec: f64,
    bandwidth_mbps: f64,
}

async fn bench_async_apply(
    num_partitions: usize,
    num_topics: usize,
    msgs_per_topic: usize,
    msg_size: usize,
    batch_size: usize,
) -> BenchResult {
    let tmp = TempDir::new().unwrap();
    let mut buf = BytesMut::new();
    let raft_config = MmapStorageConfig::new(tmp.path()).with_segment_size(4 * 1024 * 1024); // 4MB — avoids double-rotation for ~2.5MB batches
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Create topics via the raft log.
    let mut entries = Vec::with_capacity(num_topics);
    for i in 0..num_topics {
        entries.push(make_entry(
            i as u64 + 1,
            1,
            MqCommand::create_topic(
                &mut buf,
                &format!("bench-topic-{i}"),
                RetentionPolicy::default(),
                0,
            ),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let dir = tmp.path().join("engine");
    let engine = Arc::new(MqEngine::new(MqConfig::new(dir.to_str().unwrap())));
    let config = ParallelApplyConfig {
        num_partitions,
        ..Default::default()
    };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        None,
        0,
        0,
        "bench",
    );

    // Let workers process topic creation.
    // eprintln!(
    //     "  [debug] advance_and_wait({}) for topic creation...",
    //     num_topics
    // );
    manager.advance_and_wait(num_topics as u64).await;
    // eprintln!("  [debug] topic creation done, checking snapshot...");

    let snap = engine.snapshot();
    let topic_ids: Vec<u64> = snap.topics.iter().map(|t| t.meta.topic_id).collect();
    assert_eq!(topic_ids.len(), num_topics);
    // eprintln!(
    //     "  [debug] {} topics created: {:?}",
    //     topic_ids.len(),
    //     &topic_ids
    // );

    // Pre-build message payload.
    let flat_msg = make_flat_msg(msg_size);
    let msgs_payload: Vec<Bytes> = (0..batch_size).map(|_| flat_msg.clone()).collect();
    let bytes_per_msg = flat_msg.len();

    let total_cmds = num_topics * msgs_per_topic;
    let total_msgs = (total_cmds * batch_size) as u64;

    // Write all publish commands to the raft log.
    let base_index = num_topics as u64 + 1;
    let write_batch_size = 500;
    let mut log_index = base_index;

    // eprintln!(
    //     "  [debug] writing {} publish cmds to raft log...",
    //     total_cmds
    // );
    let write_start = std::time::Instant::now();
    for chunk_start in (0..total_cmds).step_by(write_batch_size) {
        let chunk_end = (chunk_start + write_batch_size).min(total_cmds);
        let mut entries = Vec::with_capacity(chunk_end - chunk_start);
        for i in chunk_start..chunk_end {
            let tid = topic_ids[i % num_topics];
            entries.push(make_entry(
                log_index,
                1,
                MqCommand::publish(&mut buf, tid, &msgs_payload),
            ));
            log_index += 1;
        }
        let batch_start = std::time::Instant::now();
        append_and_flush(&mut log, entries).await;
        let batch_ms = batch_start.elapsed().as_secs_f64() * 1000.0;
        if batch_ms > 100.0 {
            eprintln!(
                "    [warn] batch {}/{} took {:.1}ms (log_index={})",
                chunk_start / write_batch_size + 1,
                (total_cmds + write_batch_size - 1) / write_batch_size,
                batch_ms,
                log_index
            );
        }
    }

    let final_index = log_index - 1;
    // eprintln!(
    //     "  [debug] raft log write done. final_index={}, advancing workers...",
    //     final_index
    // );

    // Benchmark: measure how long workers take to process all entries.
    let start = Instant::now();
    manager.advance_and_wait(final_index).await;
    // eprintln!(
    //     "  [debug] workers done in {:.1}ms",
    //     start.elapsed().as_secs_f64() * 1000.0
    // );
    let elapsed = start.elapsed();

    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let msgs_per_sec = total_msgs as f64 / elapsed.as_secs_f64();
    let bandwidth_mbps =
        (total_msgs as f64 * bytes_per_msg as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);

    manager.shutdown().await;
    storage.stop();

    BenchResult {
        partitions: num_partitions,
        topics: num_topics,
        total_msgs,
        elapsed_ms,
        msgs_per_sec,
        bandwidth_mbps,
    }
}

/// Benchmark sequential (no dispatcher) baseline for comparison.
async fn bench_sequential(
    num_topics: usize,
    msgs_per_topic: usize,
    msg_size: usize,
    batch_size: usize,
) -> BenchResult {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(MqEngine::new(MqConfig::new(
        tmp.path().join("engine").to_str().unwrap(),
    )));
    let mut buf = BytesMut::new();

    let flat_msg = make_flat_msg(msg_size);
    let msgs_payload: Vec<Bytes> = (0..batch_size).map(|_| flat_msg.clone()).collect();
    let bytes_per_msg = flat_msg.len();

    // Create topics directly.
    let mut topic_ids = Vec::with_capacity(num_topics);
    for i in 0..num_topics {
        let cmd = MqCommand::create_topic(
            &mut buf,
            &format!("bench-topic-{i}"),
            RetentionPolicy::default(),
            1,
        );
        let resp = apply_engine(&engine, &cmd, i as u64 + 1, 1000);
        if resp.tag() == ResponseEntry::TAG_ENTITY_CREATED {
            topic_ids.push(resp.entity_id());
        } else {
            panic!("unexpected response creating topic {i}: tag={}", resp.tag());
        }
    }

    let total_msgs = (num_topics * msgs_per_topic * batch_size) as u64;
    let mut log_index = (num_topics + 1) as u64;

    let start = Instant::now();
    for _round in 0..msgs_per_topic {
        for &tid in &topic_ids {
            let cmd = MqCommand::publish(&mut buf, tid, &msgs_payload);
            let _resp = apply_engine(&engine, &cmd, log_index, 1000);
            log_index += 1;
        }
    }
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let msgs_per_sec = total_msgs as f64 / elapsed.as_secs_f64();
    let bandwidth_mbps =
        (total_msgs as f64 * bytes_per_msg as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);

    BenchResult {
        partitions: 0, // sequential
        topics: num_topics,
        total_msgs,
        elapsed_ms,
        msgs_per_sec,
        bandwidth_mbps,
    }
}

// =============================================================================
// Output
// =============================================================================

fn print_header() {
    println!(
        "\n{:>10} {:>8} {:>12} {:>12} {:>14} {:>14}",
        "Partitions", "Topics", "Messages", "Time(ms)", "Msgs/sec", "MB/s"
    );
    println!("{:-<76}", "");
}

fn print_row(r: &BenchResult) {
    let label = if r.partitions == 0 {
        "seq".to_string()
    } else {
        r.partitions.to_string()
    };
    println!(
        "{:>10} {:>8} {:>12} {:>12.1} {:>14.0} {:>14.1}",
        label, r.topics, r.total_msgs, r.elapsed_ms, r.msgs_per_sec, r.bandwidth_mbps
    );
}

fn print_speedup_table(baseline: &BenchResult, results: &[BenchResult]) {
    println!(
        "\n{:>10} {:>12} {:>12}",
        "Partitions", "Speedup", "Efficiency"
    );
    println!("{:-<38}", "");
    for r in results {
        let speedup = r.msgs_per_sec / baseline.msgs_per_sec;
        let ideal = if r.topics == 1 {
            1.0
        } else {
            r.topics.min(r.partitions) as f64
        };
        let efficiency = if ideal > 0.0 {
            speedup / ideal * 100.0
        } else {
            0.0
        };
        println!(
            "{:>10} {:>11.2}x {:>11.1}%",
            r.partitions, speedup, efficiency
        );
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() {
    println!("Async Apply Benchmark (pull-based workers over mmap raft log)");
    println!("==============================================================\n");

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    println!("Available CPUs: {cpus}");

    let msg_size = 64;
    let msgs_per_topic = 2_000;
    let partition_counts: Vec<usize> = [1, 2, 4, 8, 16]
        .into_iter()
        .filter(|&n| n <= cpus * 2)
        .chain(std::iter::once(cpus))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    let batch_size = 64;
    let num_topics = 8;

    // --- Sequential baseline ---
    println!(
        "\n### Scenario 1: {num_topics} topics ({msgs_per_topic} cmds × {batch_size} msgs/cmd, {msg_size}B payload)"
    );

    let baseline = bench_sequential(num_topics, msgs_per_topic, msg_size, batch_size).await;
    print_header();
    print_row(&baseline);

    let mut results = Vec::new();
    for &n in &partition_counts {
        let r = bench_async_apply(n, num_topics, msgs_per_topic, msg_size, batch_size).await;
        print_row(&r);
        results.push(r);
    }
    print_speedup_table(&baseline, &results);

    // --- Multi-topic scaling ---
    for num_topics in [16, 32, 64, 128] {
        println!(
            "\n### Scenario 2: {num_topics} topics ({msgs_per_topic} cmds/topic × {batch_size} msgs/cmd, {msg_size}B payload)"
        );

        let baseline = bench_sequential(num_topics, msgs_per_topic, msg_size, batch_size).await;
        print_header();
        print_row(&baseline);

        let mut results = Vec::new();
        for &n in &partition_counts {
            let r = bench_async_apply(n, num_topics, msgs_per_topic, msg_size, batch_size).await;
            print_row(&r);
            results.push(r);
        }
        print_speedup_table(&baseline, &results);
    }

    println!("\nDone.");
}
