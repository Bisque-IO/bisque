//! End-to-end integration tests for async apply through the raft log.
//!
//! Writes MqCommand entries to a real mmap raft log, creates an
//! AsyncApplyManager with a real SegmentPrefetcher, and verifies that
//! workers process entries correctly by reading from the mmap segments.

use std::sync::Arc;

use tempfile::TempDir;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::async_runtime::{AsyncRuntime, oneshot::Oneshot};
use openraft::{EntryPayload, LogId};

use bisque_mq::MqTypeConfig;
use bisque_mq::async_apply::AsyncApplyManager;
use bisque_mq::config::MqConfig;
use bisque_mq::config::ParallelApplyConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::types::*;

use bisque_raft::{MmapPerGroupLogStorage, MmapStorageConfig};

type Rt = <MqTypeConfig as openraft::RaftTypeConfig>::AsyncRuntime;
type Os = <Rt as AsyncRuntime>::Oneshot;

fn make_entry(index: u64, term: u64, cmd: MqCommand) -> openraft::impls::Entry<MqTypeConfig> {
    openraft::impls::Entry::<MqTypeConfig> {
        log_id: LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
            index,
        },
        payload: EntryPayload::Normal(cmd),
    }
}

fn make_blank_entry(index: u64, term: u64) -> openraft::impls::Entry<MqTypeConfig> {
    openraft::impls::Entry::<MqTypeConfig> {
        log_id: LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
            index,
        },
        payload: EntryPayload::Blank,
    }
}

fn make_flat_msg(payload_size: usize) -> bytes::Bytes {
    let value = vec![0xABu8; payload_size];
    bisque_mq::flat::FlatMessageBuilder::new(&value)
        .timestamp(1000)
        .build()
}

fn make_callback() -> (
    IOFlushed<MqTypeConfig>,
    <Os as Oneshot>::Receiver<Result<(), std::io::Error>>,
) {
    let (tx, rx) = Os::channel::<Result<(), std::io::Error>>();
    let cb = IOFlushed::<MqTypeConfig>::signal(tx);
    (cb, rx)
}

/// Helper: append entries to the raft log and wait for flush.
async fn append_and_flush(
    log: &mut bisque_raft::MmapGroupLogStorage<MqTypeConfig>,
    entries: Vec<openraft::impls::Entry<MqTypeConfig>>,
) {
    let (cb, rx) = make_callback();
    log.append(entries, cb).await.unwrap();
    rx.await.unwrap().unwrap();
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn async_apply_workers_process_entries_from_raft_log() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Create a topic first (structural command at index 1).
    let create_cmd = MqCommand::create_topic("test-topic", RetentionPolicy::default(), 0);
    append_and_flush(&mut log, vec![make_entry(1, 1, create_cmd)]).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-raft-test")));

    let config = ParallelApplyConfig { num_partitions: 2 };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None, // no manifest in test
        0,
        0, // initial cursor
        "test",
    );

    // Advance HWM to index 1 and wait for workers to process.
    manager.advance_and_wait(1).await;

    // Verify the topic was created.
    let snap = engine.snapshot();
    assert_eq!(
        snap.topics.len(),
        1,
        "topic should be created by async worker"
    );
    assert_eq!(snap.topics[0].meta.name, "test-topic");
    let topic_id = snap.topics[0].meta.topic_id;

    // Now publish a message to the topic (data-plane command at index 2).
    let msg = bisque_mq::flat::FlatMessageBuilder::new(b"hello")
        .timestamp(1000)
        .build();
    let publish_cmd = MqCommand::publish(topic_id, &[msg]);
    append_and_flush(&mut log, vec![make_entry(2, 1, publish_cmd)]).await;

    // Advance and wait.
    manager.advance_and_wait(2).await;

    // Verify the message was published.
    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.topic_id == topic_id)
        .unwrap();
    assert_eq!(
        topic.meta.message_count, 1,
        "one message should be published"
    );

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn async_apply_multiple_topics_and_publishes() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Create 4 topics.
    let mut entries = Vec::new();
    for i in 1..=4u64 {
        entries.push(make_entry(
            i,
            1,
            MqCommand::create_topic(&format!("topic-{}", i), RetentionPolicy::default(), 0),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-raft-multi")));

    let config = ParallelApplyConfig { num_partitions: 4 };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    manager.advance_and_wait(4).await;

    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 4, "all 4 topics should be created");

    // Collect topic IDs.
    let topic_ids: Vec<u64> = snap.topics.iter().map(|t| t.meta.topic_id).collect();

    // Publish to each topic.
    let mut entries = Vec::new();
    for (i, &tid) in topic_ids.iter().enumerate() {
        let msg = bisque_mq::flat::FlatMessageBuilder::new(format!("msg-{}", i).as_bytes())
            .timestamp(2000)
            .build();
        entries.push(make_entry(5 + i as u64, 1, MqCommand::publish(tid, &[msg])));
    }
    append_and_flush(&mut log, entries).await;

    manager.advance_and_wait(8).await;

    // Verify each topic has 1 message.
    let snap = engine.snapshot();
    for &tid in &topic_ids {
        let topic = snap.topics.iter().find(|t| t.meta.topic_id == tid).unwrap();
        assert_eq!(
            topic.meta.message_count, 1,
            "topic {} should have 1 message",
            tid
        );
    }

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn async_apply_skips_blank_entries() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Mix blank and normal entries.
    let entries = vec![
        make_blank_entry(1, 1),
        make_entry(
            2,
            1,
            MqCommand::create_topic("after-blank", RetentionPolicy::default(), 0),
        ),
        make_blank_entry(3, 1),
    ];
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-blank")));

    let config = ParallelApplyConfig { num_partitions: 2 };

    let mut manager =
        AsyncApplyManager::new(&config, Arc::clone(&engine), prefetcher, None, 0, 0, "test");

    manager.advance_and_wait(3).await;

    let snap = engine.snapshot();
    assert_eq!(
        snap.topics.len(),
        1,
        "topic should be created despite blanks"
    );

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn async_apply_batch_entries_skipped_by_workers() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Create a topic.
    append_and_flush(
        &mut log,
        vec![make_entry(
            1,
            1,
            MqCommand::create_topic("batch-test", RetentionPolicy::default(), 0),
        )],
    )
    .await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-batch")));

    let config = ParallelApplyConfig { num_partitions: 2 };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    manager.advance_and_wait(1).await;
    let topic_id = engine.snapshot().topics[0].meta.topic_id;

    // Write a batch command at index 2, then a normal publish at index 3.
    let msg1 = bisque_mq::flat::FlatMessageBuilder::new(b"batch-msg")
        .timestamp(1000)
        .build();
    let msg2 = bisque_mq::flat::FlatMessageBuilder::new(b"normal-msg")
        .timestamp(1000)
        .build();

    let batch = MqCommand::batch(&[MqCommand::publish(topic_id, &[msg1])]);
    let normal = MqCommand::publish(topic_id, &[msg2]);

    append_and_flush(
        &mut log,
        vec![make_entry(2, 1, batch), make_entry(3, 1, normal)],
    )
    .await;

    // Workers should skip the batch entry (index 2) and apply the normal (index 3).
    manager.advance_and_wait(3).await;

    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.topic_id == topic_id)
        .unwrap();
    // Only the normal publish at index 3 should have been applied.
    // The batch at index 2 is skipped by workers (it would be handled
    // by apply() synchronously in production).
    assert_eq!(
        topic.meta.message_count, 1,
        "only normal publish should be applied"
    );

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn async_apply_cursor_tracks_worker_progress() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Write several entries.
    let mut entries = Vec::new();
    for i in 1..=10u64 {
        entries.push(make_entry(
            i,
            1,
            MqCommand::create_topic(&format!("cursor-{}", i), RetentionPolicy::default(), 0),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-cursor")));

    let config = ParallelApplyConfig { num_partitions: 4 };

    let mut manager =
        AsyncApplyManager::new(&config, Arc::clone(&engine), prefetcher, None, 0, 0, "test");

    // Advance to 5 first.
    manager.advance_and_wait(5).await;
    assert!(
        manager.min_worker_cursor() >= 5,
        "min cursor should be >= 5 after advance_and_wait(5)"
    );

    // Advance to 10.
    manager.advance_and_wait(10).await;
    assert!(
        manager.min_worker_cursor() >= 10,
        "min cursor should be >= 10 after advance_and_wait(10)"
    );

    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 10, "all 10 topics should be created");

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn async_apply_high_throughput() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Create one topic.
    append_and_flush(
        &mut log,
        vec![make_entry(
            1,
            1,
            MqCommand::create_topic("throughput", RetentionPolicy::default(), 0),
        )],
    )
    .await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-throughput")));

    let config = ParallelApplyConfig { num_partitions: 4 };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    manager.advance_and_wait(1).await;
    let topic_id = engine.snapshot().topics[0].meta.topic_id;

    // Write 200 publishes in batches of 50.
    for batch_start in (0..200u64).step_by(50) {
        let mut entries = Vec::with_capacity(50);
        for i in 0..50u64 {
            let idx = 2 + batch_start + i;
            let msg = bisque_mq::flat::FlatMessageBuilder::new(format!("msg-{}", idx).as_bytes())
                .timestamp(3000)
                .build();
            entries.push(make_entry(idx, 1, MqCommand::publish(topic_id, &[msg])));
        }
        append_and_flush(&mut log, entries).await;
    }

    // All entries 1..=201.
    manager.advance_and_wait(201).await;

    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.topic_id == topic_id)
        .unwrap();
    assert_eq!(
        topic.meta.message_count, 200,
        "all 200 messages should be published"
    );

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn async_apply_segment_index_tracking() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Create a topic and publish.
    let create_cmd = MqCommand::create_topic("idx-test", RetentionPolicy::default(), 0);
    append_and_flush(&mut log, vec![make_entry(1, 1, create_cmd)]).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-segidx")));

    let config = ParallelApplyConfig { num_partitions: 2 };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    manager.advance_and_wait(1).await;
    let topic_id = engine.snapshot().topics[0].meta.topic_id;

    // Publish messages.
    let mut entries = Vec::new();
    for i in 0..5u64 {
        let msg = bisque_mq::flat::FlatMessageBuilder::new(format!("idx-{}", i).as_bytes())
            .timestamp(4000)
            .build();
        entries.push(make_entry(2 + i, 1, MqCommand::publish(topic_id, &[msg])));
    }
    append_and_flush(&mut log, entries).await;

    manager.advance_and_wait(6).await;

    // Verify the publishes were applied.
    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.topic_id == topic_id)
        .unwrap();
    assert_eq!(topic.meta.message_count, 5);

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn async_apply_shutdown_sentinel() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let log = storage.get_log_storage(0).await.unwrap();

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-shutdown")));

    let config = ParallelApplyConfig { num_partitions: 4 };

    let mut manager =
        AsyncApplyManager::new(&config, Arc::clone(&engine), prefetcher, None, 0, 0, "test");

    // Shutdown without processing any entries — workers should exit cleanly.
    manager.shutdown().await;

    // Verify no panic, no hang.
    assert_eq!(manager.min_worker_cursor(), 0);

    storage.stop();
}

#[tokio::test]
async fn async_apply_purge_floor_accounts_for_workers() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    // Write entries.
    let mut entries = Vec::new();
    for i in 1..=5u64 {
        entries.push(make_entry(
            i,
            1,
            MqCommand::create_topic(&format!("purge-{}", i), RetentionPolicy::default(), 0),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new("/tmp/mq-async-purge")));

    let config = ParallelApplyConfig { num_partitions: 2 };

    let mut manager =
        AsyncApplyManager::new(&config, Arc::clone(&engine), prefetcher, None, 0, 0, "test");

    // Advance only to 3.
    manager.advance_and_wait(3).await;

    // min_worker_cursor should be at least 3.
    let min = manager.min_worker_cursor();
    assert!(min >= 3, "min cursor should be >= 3, got {}", min);

    // Now advance to 5.
    manager.advance_and_wait(5).await;
    let min = manager.min_worker_cursor();
    assert!(min >= 5, "min cursor should be >= 5, got {}", min);

    manager.shutdown().await;
    storage.stop();
}

// =============================================================================
// Scale tests — mirror benchmark scenarios
// =============================================================================

/// Two-phase create-then-publish with multiple topics and many entries.
/// Mirrors the benchmark flow: write topics, advance_and_wait, write publishes,
/// advance_and_wait again. Tests persistent scanner across range boundaries.
#[tokio::test]
async fn async_apply_benchmark_flow_8_topics_4_partitions() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    let num_topics = 8;
    let num_partitions = 4;
    let msgs_per_topic = 100; // 100 cmds per topic
    let batch_size = 8; // 8 messages per publish cmd

    // Phase 1: Create topics.
    let mut entries = Vec::new();
    for i in 0..num_topics {
        entries.push(make_entry(
            i as u64 + 1,
            1,
            MqCommand::create_topic(&format!("bench-topic-{i}"), RetentionPolicy::default(), 0),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let dir = tmp.path().join("engine");
    let engine = Arc::new(bisque_mq::engine::MqEngine::new(
        bisque_mq::config::MqConfig::new(dir.to_str().unwrap()),
    ));

    let config = ParallelApplyConfig { num_partitions };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    // Wait for topic creation.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.advance_and_wait(num_topics as u64),
    )
    .await
    .expect("topic creation advance_and_wait timed out");

    let snap = engine.snapshot();
    let topic_ids: Vec<u64> = snap.topics.iter().map(|t| t.meta.topic_id).collect();
    assert_eq!(topic_ids.len(), num_topics);

    // Phase 2: Write publish entries.
    let flat_msg = make_flat_msg(64);
    let msgs_payload: Vec<bytes::Bytes> = (0..batch_size).map(|_| flat_msg.clone()).collect();

    let total_cmds = num_topics * msgs_per_topic;
    let base_index = num_topics as u64 + 1;
    let write_batch = 100;
    let mut log_index = base_index;

    for chunk_start in (0..total_cmds).step_by(write_batch) {
        let chunk_end = (chunk_start + write_batch).min(total_cmds);
        let mut entries = Vec::with_capacity(chunk_end - chunk_start);
        for i in chunk_start..chunk_end {
            let tid = topic_ids[i % num_topics];
            entries.push(make_entry(
                log_index,
                1,
                MqCommand::publish(tid, &msgs_payload),
            ));
            log_index += 1;
        }
        append_and_flush(&mut log, entries).await;
    }

    let final_index = log_index - 1;

    // Wait for publish processing.
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        manager.advance_and_wait(final_index),
    )
    .await
    .expect("publish advance_and_wait timed out — workers hung");

    // Verify: each topic should have msgs_per_topic * batch_size messages.
    let expected_msgs = (msgs_per_topic * batch_size) as u64;
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), num_topics);
    for topic in &snap.topics {
        assert_eq!(
            topic.meta.message_count, expected_msgs,
            "topic {} should have {expected_msgs} messages, got {}",
            topic.meta.topic_id, topic.meta.message_count
        );
    }

    manager.shutdown().await;
    storage.stop();
}

/// Three-phase test: create, publish batch 1, publish batch 2.
/// Tests that persistent scanner correctly resumes across multiple advance cycles.
#[tokio::test]
async fn async_apply_three_phase_persistent_scanner() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    let num_topics = 4;
    let num_partitions = 2;

    // Phase 1: Create topics.
    let mut entries = Vec::new();
    for i in 0..num_topics {
        entries.push(make_entry(
            i as u64 + 1,
            1,
            MqCommand::create_topic(&format!("3phase-{i}"), RetentionPolicy::default(), 0),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let dir = tmp.path().join("engine");
    let engine = Arc::new(bisque_mq::engine::MqEngine::new(
        bisque_mq::config::MqConfig::new(dir.to_str().unwrap()),
    ));

    let config = ParallelApplyConfig { num_partitions };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.advance_and_wait(num_topics as u64),
    )
    .await
    .expect("phase 1 timed out");

    let snap = engine.snapshot();
    let topic_ids: Vec<u64> = snap.topics.iter().map(|t| t.meta.topic_id).collect();

    // Phase 2: Publish 20 commands per topic.
    let flat_msg = make_flat_msg(32);
    let msgs: Vec<bytes::Bytes> = vec![flat_msg.clone()];
    let msgs_per_topic_p2 = 20usize;
    let mut log_index = num_topics as u64 + 1;
    let mut entries = Vec::new();
    for _round in 0..msgs_per_topic_p2 {
        for &tid in &topic_ids {
            entries.push(make_entry(log_index, 1, MqCommand::publish(tid, &msgs)));
            log_index += 1;
        }
    }
    let p2_final = log_index - 1;
    append_and_flush(&mut log, entries).await;

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.advance_and_wait(p2_final),
    )
    .await
    .expect("phase 2 timed out");

    // Phase 3: Publish 20 more commands per topic.
    let msgs_per_topic_p3 = 20usize;
    let mut entries = Vec::new();
    for _round in 0..msgs_per_topic_p3 {
        for &tid in &topic_ids {
            entries.push(make_entry(log_index, 1, MqCommand::publish(tid, &msgs)));
            log_index += 1;
        }
    }
    let p3_final = log_index - 1;
    append_and_flush(&mut log, entries).await;

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.advance_and_wait(p3_final),
    )
    .await
    .expect("phase 3 timed out");

    // Each topic should have 40 messages.
    let snap = engine.snapshot();
    for topic in &snap.topics {
        assert_eq!(
            topic.meta.message_count, 40,
            "topic {} should have 40 messages, got {}",
            topic.meta.topic_id, topic.meta.message_count
        );
    }

    manager.shutdown().await;
    storage.stop();
}

/// Stress test: many partitions, many topics, high message count.
#[tokio::test]
async fn async_apply_stress_16_partitions_16_topics() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    let num_topics = 16;
    let num_partitions = 16;
    let msgs_per_topic = 500usize;
    let batch_size = 16;

    // Create topics.
    let mut entries = Vec::new();
    for i in 0..num_topics {
        entries.push(make_entry(
            i as u64 + 1,
            1,
            MqCommand::create_topic(&format!("stress-{i}"), RetentionPolicy::default(), 0),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let dir = tmp.path().join("engine");
    let engine = Arc::new(bisque_mq::engine::MqEngine::new(
        bisque_mq::config::MqConfig::new(dir.to_str().unwrap()),
    ));

    let config = ParallelApplyConfig { num_partitions };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.advance_and_wait(num_topics as u64),
    )
    .await
    .expect("topic creation timed out");

    let snap = engine.snapshot();
    let topic_ids: Vec<u64> = snap.topics.iter().map(|t| t.meta.topic_id).collect();

    // Write publish entries in batches.
    let flat_msg = make_flat_msg(64);
    let msgs_payload: Vec<bytes::Bytes> = (0..batch_size).map(|_| flat_msg.clone()).collect();

    let total_cmds = num_topics * msgs_per_topic;
    let base_index = num_topics as u64 + 1;
    let write_batch = 200;
    let mut log_index = base_index;

    for chunk_start in (0..total_cmds).step_by(write_batch) {
        let chunk_end = (chunk_start + write_batch).min(total_cmds);
        let mut entries = Vec::with_capacity(chunk_end - chunk_start);
        for i in chunk_start..chunk_end {
            let tid = topic_ids[i % num_topics];
            entries.push(make_entry(
                log_index,
                1,
                MqCommand::publish(tid, &msgs_payload),
            ));
            log_index += 1;
        }
        append_and_flush(&mut log, entries).await;
    }

    let final_index = log_index - 1;

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        manager.advance_and_wait(final_index),
    )
    .await
    .expect("stress test advance_and_wait timed out — workers hung");

    let expected_msgs = (msgs_per_topic * batch_size) as u64;
    let snap = engine.snapshot();
    for topic in &snap.topics {
        assert_eq!(
            topic.meta.message_count, expected_msgs,
            "topic {} should have {expected_msgs} messages, got {}",
            topic.meta.topic_id, topic.meta.message_count
        );
    }

    manager.shutdown().await;
    storage.stop();
}

/// Exact benchmark mirror: 16 topics, 2000 cmds/topic, 64 msgs/cmd, 16 partitions.
/// This is the scenario that hangs in the benchmark.
#[tokio::test]
async fn async_apply_benchmark_exact_mirror() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();

    let num_topics = 16;
    let num_partitions = 16;
    let msgs_per_topic = 2000usize;
    let batch_size = 64;

    // Create topics.
    let mut entries = Vec::new();
    for i in 0..num_topics {
        entries.push(make_entry(
            i as u64 + 1,
            1,
            MqCommand::create_topic(&format!("bench-topic-{i}"), RetentionPolicy::default(), 0),
        ));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let dir = tmp.path().join("engine");
    let engine = Arc::new(bisque_mq::engine::MqEngine::new(
        bisque_mq::config::MqConfig::new(dir.to_str().unwrap()),
    ));

    let config = ParallelApplyConfig { num_partitions };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher.clone(),
        None,
        0,
        0,
        "test",
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.advance_and_wait(num_topics as u64),
    )
    .await
    .expect("topic creation timed out");

    let snap = engine.snapshot();
    let topic_ids: Vec<u64> = snap.topics.iter().map(|t| t.meta.topic_id).collect();
    assert_eq!(topic_ids.len(), num_topics);

    // Write publish entries in batches of 500 (same as benchmark).
    let flat_msg = make_flat_msg(64);
    let msgs_payload: Vec<bytes::Bytes> = (0..batch_size).map(|_| flat_msg.clone()).collect();

    let total_cmds = num_topics * msgs_per_topic;
    let base_index = num_topics as u64 + 1;
    let write_batch = 500;
    let mut log_index = base_index;

    for chunk_start in (0..total_cmds).step_by(write_batch) {
        let chunk_end = (chunk_start + write_batch).min(total_cmds);
        let mut entries = Vec::with_capacity(chunk_end - chunk_start);
        for i in chunk_start..chunk_end {
            let tid = topic_ids[i % num_topics];
            entries.push(make_entry(
                log_index,
                1,
                MqCommand::publish(tid, &msgs_payload),
            ));
            log_index += 1;
        }
        append_and_flush(&mut log, entries).await;
    }

    let final_index = log_index - 1;
    eprintln!(
        "  wrote {} cmds to raft log, final_index={}",
        total_cmds, final_index
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        manager.advance_and_wait(final_index),
    )
    .await
    .expect("benchmark mirror timed out — workers hung");

    let expected_msgs = (msgs_per_topic * batch_size) as u64;
    let snap = engine.snapshot();
    for topic in &snap.topics {
        assert_eq!(
            topic.meta.message_count, expected_msgs,
            "topic {} should have {expected_msgs} messages, got {}",
            topic.meta.topic_id, topic.meta.message_count
        );
    }

    manager.shutdown().await;
    storage.stop();
}
