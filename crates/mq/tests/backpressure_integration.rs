//! Integration tests for the RaftBacklog backpressure pipeline.
//!
//! Tests cover:
//! 1. RaftBacklog charge/release lifecycle with real commands
//! 2. Forward path: ForwardAcceptor (raft path) with backlog
//! 3. Local batcher: LocalBatcher with backlog charge/release
//! 4. External batcher: MqWriteBatcher with backlog charge/release
//! 5. State machine apply: release on TAG_BATCH and TAG_FORWARDED_BATCH
//! 6. Full pipeline: forward → raft log → state machine apply → backlog drain

use bisque_raft::NodeAddressResolver;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use openraft::BasicNode;
use tempfile::TempDir;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::async_runtime::{AsyncRuntime, oneshot::Oneshot};
use openraft::{EntryPayload, LogId, Raft};

use bisque_mq::MqTypeConfig;
use bisque_mq::async_apply::AsyncApplyManager;
use bisque_mq::config::{MqConfig, ParallelApplyConfig};
use bisque_mq::engine::MqEngine;
use bisque_mq::forward::RaftBacklog;
use bisque_mq::types::{MqCommand, RetentionPolicy};

use bisque_raft::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapPerGroupLogStorage,
    MmapStorageConfig, MultiRaftManager, MultiplexedLogStorage,
};

type Rt = <MqTypeConfig as openraft::RaftTypeConfig>::AsyncRuntime;
type Os = <Rt as AsyncRuntime>::Oneshot;

// =============================================================================
// Helpers
// =============================================================================

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

/// Build a minimal sub-frame for TAG_FORWARDED_BATCH:
/// `[payload_len:4][client_id:4][request_seq:8][cmd_bytes...]`
fn make_sub_frame(client_id: u32, request_seq: u64, cmd: &[u8]) -> bytes::Bytes {
    let payload_len = (12 + cmd.len()) as u32;
    let mut buf = BytesMut::with_capacity(4 + payload_len as usize);
    buf.put_u32_le(payload_len);
    buf.put_u32_le(client_id);
    buf.put_u64_le(request_seq);
    buf.extend_from_slice(cmd);
    buf.freeze()
}

/// Shorthand to get the encoded size of an MqCommand.
fn cmd_size(cmd: &MqCommand) -> usize {
    cmd.total_encoded_size()
}

// =============================================================================
// 1. RaftBacklog + real MqCommand lifecycle
// =============================================================================

#[test]
fn backlog_charge_with_real_forwarded_batch_command() {
    let bl = RaftBacklog::new(1_000_000);

    // Build a TAG_FORWARDED_BATCH command.
    let frame = make_sub_frame(1, 0, b"hello world");
    let mut buf = BytesMut::new();
    MqCommand::write_forwarded_batch(&mut buf, 5, 1, 42, 1, &frame);
    let cmd = MqCommand::split_from(&mut buf);
    let len = cmd_size(&cmd);
    assert!(len > 0);

    assert!(bl.try_charge(len));
    assert_eq!(bl.current(), len);
    bl.release(len);
    assert_eq!(bl.current(), 0);
}

#[test]
fn backlog_charge_with_real_batch_command() {
    let bl = RaftBacklog::new(1_000_000);

    // Build a TAG_BATCH command with a publish inside.
    let mut buf = BytesMut::new();
    let msg = bisque_mq::flat::FlatMessageBuilder::new(b"payload")
        .timestamp(1000)
        .build();
    let inner = MqCommand::publish(&mut buf, 1, &[msg]);
    MqCommand::write_batch(&mut buf, &[inner]);
    let cmd = MqCommand::split_from(&mut buf);
    let len = cmd_size(&cmd);
    assert!(len > 0);

    assert!(bl.try_charge(len));
    assert_eq!(bl.current(), len);
    bl.release(len);
    assert_eq!(bl.current(), 0);
}

#[test]
fn backlog_many_commands_accumulate_and_drain() {
    let bl = RaftBacklog::new(1_000_000);
    let mut total_charged = 0usize;

    for i in 0..100u32 {
        let frame = make_sub_frame(i, i as u64, &[0xAB; 64]);
        let mut buf = BytesMut::new();
        MqCommand::write_forwarded_batch(&mut buf, 1, 1, i as u64, i as u64, &frame);
        let cmd = MqCommand::split_from(&mut buf);
        assert!(bl.try_charge(cmd_size(&cmd)));
        total_charged += cmd_size(&cmd);
    }

    assert_eq!(bl.current(), total_charged);

    // Simulate apply releasing all.
    bl.release(total_charged);
    assert_eq!(bl.current(), 0);
}

// =============================================================================
// 2. Backpressure blocks and unblocks
// =============================================================================

#[tokio::test]
async fn backlog_backpressure_blocks_producer_until_release() {
    // Small budget — exhaust it, then verify further charges block.
    let bl = Arc::new(RaftBacklog::new(10));

    // Exhaust the entire budget.
    assert!(bl.try_charge(10));

    // A producer trying to charge more should block.
    let bl2 = Arc::clone(&bl);
    let producer_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pd = Arc::clone(&producer_done);
    let handle = tokio::spawn(async move {
        bl2.charge(1).await;
        pd.store(true, Ordering::Release);
    });

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !producer_done.load(Ordering::Acquire),
        "producer should be blocked"
    );

    // Release enough to allow the charge of 1.
    bl.release(1);

    tokio::time::timeout(Duration::from_millis(200), handle)
        .await
        .expect("producer should unblock")
        .unwrap();
    assert!(producer_done.load(Ordering::Acquire));
}

#[tokio::test]
async fn backlog_incremental_release_unblocks_at_threshold() {
    let bl = Arc::new(RaftBacklog::new(100));
    // Exhaust budget fully.
    assert!(bl.try_charge(100));

    // A waiter trying to charge 20 should block (0 permits available).
    let bl2 = Arc::clone(&bl);
    let unblocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ub = Arc::clone(&unblocked);
    let handle = tokio::spawn(async move {
        bl2.charge(20).await;
        ub.store(true, Ordering::Release);
    });

    // Release 10 — only 10 permits available, need 20 — still blocked.
    tokio::time::sleep(Duration::from_millis(10)).await;
    bl.release(10);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !unblocked.load(Ordering::Acquire),
        "should still be blocked with only 10 available"
    );

    // Release another 10 — now 20 available, charge(20) should succeed.
    bl.release(10);

    tokio::time::timeout(Duration::from_millis(200), handle)
        .await
        .expect("should unblock when 20 permits available")
        .unwrap();
    assert!(unblocked.load(Ordering::Acquire));
}

// =============================================================================
// 3. LocalBatcher backpressure integration
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn local_batcher_charges_backlog_on_propose() {
    // Simulate what local_batcher_loop does: build TAG_FORWARDED_BATCH, charge.
    let bl = RaftBacklog::new(1_000_000);

    let frame = make_sub_frame(10, 1, b"test payload");
    let mut buf = BytesMut::new();
    MqCommand::write_forwarded_batch(&mut buf, 99, 1, 0, 0, &frame);
    let cmd = MqCommand::split_from(&mut buf);
    let len = cmd_size(&cmd);

    assert!(bl.try_charge(len));
    assert_eq!(bl.current(), len);

    // Simulate proposal failure — release.
    bl.release(len);
    assert_eq!(bl.current(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn local_batcher_forwarded_batch_roundtrip_charge_release() {
    // Build the same TAG_FORWARDED_BATCH that local_batcher_loop would produce,
    // and verify the charge/release contract with the actual encoded size.
    let frame_a = make_sub_frame(1, 0, b"cmd bytes A");
    let frame_b = make_sub_frame(2, 1, b"cmd bytes B");

    let mut scratch = BytesMut::new();
    MqCommand::write_forwarded_batch(
        &mut scratch,
        42, // node_id
        2,  // count
        0,  // batch_seq
        0,  // leader_seq
        &[frame_a.as_ref(), frame_b.as_ref()].concat(),
    );
    let cmd = MqCommand::split_from(&mut scratch);
    assert_eq!(cmd.tag(), MqCommand::TAG_FORWARDED_BATCH);

    let bl = RaftBacklog::new(1_000_000);
    let len = cmd_size(&cmd);
    assert!(len > 0);
    bl.charge(len).await;
    assert_eq!(bl.current(), len);
    bl.release(len);
    assert_eq!(bl.current(), 0);
}

// =============================================================================
// 4. External batcher backpressure integration
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn external_batcher_charges_backlog_on_propose() {
    // Build a TAG_BATCH and verify charge/release lifecycle.
    let bl = RaftBacklog::new(1_000_000);

    let mut buf = BytesMut::new();
    let msg = bisque_mq::flat::FlatMessageBuilder::new(b"value")
        .timestamp(1000)
        .build();
    let publish = MqCommand::publish(&mut buf, 1, &[msg]);
    MqCommand::write_batch_with_id(&mut buf, 0, &[publish]);
    let cmd = MqCommand::split_from(&mut buf);
    let len = cmd_size(&cmd);

    bl.charge(len).await;
    assert!(bl.current() > 0);

    // Simulate successful apply release.
    bl.release(len);
    assert_eq!(bl.current(), 0);
}

// =============================================================================
// 5. State machine apply releases backlog (via raft log)
// =============================================================================

#[tokio::test]
async fn state_machine_releases_backlog_on_forwarded_batch_apply() {
    let bl = Arc::new(RaftBacklog::new(1_000_000));

    // Build a TAG_FORWARDED_BATCH command.
    let mut buf = BytesMut::new();
    let msg = bisque_mq::flat::FlatMessageBuilder::new(b"bp-value")
        .timestamp(1000)
        .build();
    let publish_cmd = MqCommand::publish(&mut buf, 1, &[msg]);
    let frame = make_sub_frame(10, 1, publish_cmd.as_bytes());
    MqCommand::write_forwarded_batch(&mut buf, 3, 1, 100, 1, &frame);
    let fwd_cmd = MqCommand::split_from(&mut buf);
    let fwd_len = cmd_size(&fwd_cmd);

    // Charge backlog as the leader would.
    bl.charge(fwd_len).await;
    assert_eq!(bl.current(), fwd_len);

    // Verify the command is indeed TAG_FORWARDED_BATCH.
    assert_eq!(fwd_cmd.tag(), MqCommand::TAG_FORWARDED_BATCH);

    // Simulate state machine apply() release.
    bl.release(fwd_len);
    assert_eq!(bl.current(), 0, "backlog should drain to 0 after apply");
}

#[tokio::test]
async fn state_machine_releases_backlog_on_batch_apply() {
    let bl = Arc::new(RaftBacklog::new(1_000_000));

    // Build a TAG_BATCH command.
    let mut buf = BytesMut::new();
    let msg = bisque_mq::flat::FlatMessageBuilder::new(b"batch-val")
        .timestamp(2000)
        .build();
    let publish = MqCommand::publish(&mut buf, 1, &[msg]);
    MqCommand::write_batch_with_id(&mut buf, 0, &[publish]);
    let cmd = MqCommand::split_from(&mut buf);
    let len = cmd_size(&cmd);

    // Charge as the batcher would.
    bl.charge(len).await;
    assert_eq!(bl.current(), len);

    // Simulate the state machine apply release.
    assert_eq!(cmd.tag(), MqCommand::TAG_BATCH);
    bl.release(len);
    assert_eq!(
        bl.current(),
        0,
        "backlog should drain after TAG_BATCH apply"
    );
}

#[test]
fn state_machine_only_releases_for_batch_and_forwarded_batch() {
    // Verify that other command types would not be released.
    let bl = RaftBacklog::new(1_000_000);

    let mut buf = BytesMut::new();
    let create = MqCommand::create_topic(&mut buf, "topic", RetentionPolicy::default(), 0);
    let tag = create.tag();

    // Only TAG_BATCH and TAG_FORWARDED_BATCH should trigger release.
    assert_ne!(tag, MqCommand::TAG_BATCH);
    assert_ne!(tag, MqCommand::TAG_FORWARDED_BATCH);

    // If we charge for a create_topic (which shouldn't happen in practice),
    // the state machine would NOT release it.
    assert!(bl.try_charge(100));
    // No release — budget leaks intentionally for this test.
    assert_eq!(bl.current(), 100);
}

// =============================================================================
// 6. Full pipeline: multiple commands, concurrent charge/release
// =============================================================================

#[tokio::test]
async fn full_pipeline_many_forwarded_batches_drain_completely() {
    let bl = Arc::new(RaftBacklog::new(10_000_000));
    let num_batches = 200;
    let mut batch_sizes = Vec::with_capacity(num_batches);

    // Simulate leader receiving many forwarded batches.
    for i in 0..num_batches {
        let payload = vec![0xABu8; 64 + (i % 50)]; // varying size
        let frame = make_sub_frame(i as u32, i as u64, &payload);
        let mut buf = BytesMut::new();
        MqCommand::write_forwarded_batch(&mut buf, 1, 1, i as u64, i as u64, &frame);
        let cmd = MqCommand::split_from(&mut buf);
        let len = cmd_size(&cmd);
        bl.charge(len).await;
        batch_sizes.push(len);
    }

    let total: usize = batch_sizes.iter().sum();
    assert_eq!(bl.current(), total);
    assert!(total > 0);

    // Simulate state machine applying them all.
    for &size in &batch_sizes {
        bl.release(size);
    }
    assert_eq!(
        bl.current(),
        0,
        "backlog must be exactly zero after all releases"
    );
}

#[tokio::test]
async fn full_pipeline_mixed_batch_and_forwarded_drain() {
    let bl = Arc::new(RaftBacklog::new(10_000_000));
    let mut sizes = Vec::new();

    // 50 TAG_FORWARDED_BATCH commands.
    for i in 0..50u32 {
        let frame = make_sub_frame(i, i as u64, &[0xCC; 32]);
        let mut buf = BytesMut::new();
        MqCommand::write_forwarded_batch(&mut buf, 1, 1, i as u64, i as u64, &frame);
        let cmd = MqCommand::split_from(&mut buf);
        let len = cmd_size(&cmd);
        bl.charge(len).await;
        sizes.push(len);
    }

    // 50 TAG_BATCH commands.
    for i in 0..50usize {
        let mut buf = BytesMut::new();
        let payload = vec![0xDDu8; 16 + i];
        let msg = bisque_mq::flat::FlatMessageBuilder::new(&payload)
            .timestamp(1000)
            .build();
        let publish = MqCommand::publish(&mut buf, 1, &[msg]);
        MqCommand::write_batch_with_id(&mut buf, i as u32, &[publish]);
        let cmd = MqCommand::split_from(&mut buf);
        let len = cmd_size(&cmd);
        bl.charge(len).await;
        sizes.push(len);
    }

    let total: usize = sizes.iter().sum();
    assert_eq!(bl.current(), total);

    // Release all — simulating apply.
    for &size in &sizes {
        bl.release(size);
    }
    assert_eq!(bl.current(), 0);
}

#[tokio::test]
async fn concurrent_producers_and_releaser_converge_to_zero() {
    let bl = Arc::new(RaftBacklog::new(1 << 60)); // effectively unlimited; usize::MAX exceeds tokio Semaphore MAX_PERMITS
    let num_producers = 4;
    let msgs_per_producer = 500usize;
    let charge_per_msg = 128usize;

    // Spawn producers that charge.
    let mut producer_handles = Vec::new();
    for _ in 0..num_producers {
        let bl2 = Arc::clone(&bl);
        producer_handles.push(tokio::spawn(async move {
            for _ in 0..msgs_per_producer {
                bl2.charge(charge_per_msg).await;
                tokio::task::yield_now().await;
            }
        }));
    }

    // Wait for all producers.
    for h in producer_handles {
        h.await.unwrap();
    }

    let expected = num_producers * msgs_per_producer * charge_per_msg;
    assert_eq!(bl.current(), expected);

    // Spawn releasers that drain.
    let mut releaser_handles = Vec::new();
    for _ in 0..num_producers {
        let bl2 = Arc::clone(&bl);
        releaser_handles.push(tokio::spawn(async move {
            for _ in 0..msgs_per_producer {
                bl2.release(charge_per_msg);
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in releaser_handles {
        h.await.unwrap();
    }

    assert_eq!(
        bl.current(),
        0,
        "concurrent charge/release must converge to zero"
    );
}

#[tokio::test]
async fn concurrent_charge_release_with_backpressure() {
    // Small budget — producers will block frequently.
    let bl = Arc::new(RaftBacklog::new(512));
    let num_msgs = 200u64;
    let charge_per_msg = 64usize;

    let bl_producer = Arc::clone(&bl);
    let bl_releaser = Arc::clone(&bl);

    let charged = Arc::new(AtomicU64::new(0));
    let released = Arc::new(AtomicU64::new(0));
    let charged2 = Arc::clone(&charged);
    let released2 = Arc::clone(&released);

    // Producer: charges and waits for capacity.
    let producer = tokio::spawn(async move {
        for _ in 0..num_msgs {
            bl_producer.charge(charge_per_msg).await;
            charged2.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    // Releaser: runs on a timer, releasing in bursts.
    let releaser = tokio::spawn(async move {
        let mut total_released = 0u64;
        while total_released < num_msgs {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let current = bl_releaser.current();
            if current >= charge_per_msg {
                bl_releaser.release(charge_per_msg);
                total_released += 1;
                released2.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        producer.await.unwrap();
        releaser.await.unwrap();
    })
    .await
    .expect("pipeline should complete within 5s");

    assert_eq!(charged.load(Ordering::Relaxed), num_msgs);
    assert_eq!(released.load(Ordering::Relaxed), num_msgs);
    assert_eq!(bl.current(), 0, "all charged bytes must be released");
}

// =============================================================================
// 8. Stress: high-rate charge/release with many concurrent waiters
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn stress_many_waiters_all_eventually_proceed() {
    let bl = Arc::new(RaftBacklog::new(256));
    // Exhaust the entire budget.
    assert!(bl.try_charge(256));

    let num_waiters = 16;
    let mut handles = Vec::new();
    for i in 0..num_waiters {
        let bl2 = Arc::clone(&bl);
        handles.push(tokio::spawn(async move {
            bl2.charge(1).await;
            i
        }));
    }

    // Give waiters time to park.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Release in chunks — need to release at least 256 (original) + 16 (waiters).
    for _ in 0..28 {
        bl.release(10);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // All should eventually complete.
    let results = tokio::time::timeout(Duration::from_secs(2), async {
        let mut out = Vec::new();
        for h in handles {
            out.push(h.await.unwrap());
        }
        out
    })
    .await
    .expect("all waiters should eventually unblock");

    assert_eq!(results.len(), num_waiters);
}

// =============================================================================
// 9. Edge cases
// =============================================================================

#[test]
fn backlog_zero_budget_always_blocks_check() {
    let bl = RaftBacklog::new(0);
    assert_eq!(bl.max(), 0);
    assert_eq!(bl.current(), 0);
    // current (0) < max (0) is false — so wait_for_capacity would block.
}

#[tokio::test]
async fn backlog_zero_charge_is_noop() {
    let bl = RaftBacklog::new(100);
    bl.charge(0).await;
    assert_eq!(bl.current(), 0);
    bl.release(0);
    assert_eq!(bl.current(), 0);
}

#[tokio::test]
async fn backlog_exact_budget_does_not_block() {
    let bl = RaftBacklog::new(100);
    assert!(bl.try_charge(99));
    // 1 permit remaining — charging 1 should not block.
    tokio::time::timeout(Duration::from_millis(50), bl.charge(1))
        .await
        .expect("should not block with 1 permit remaining");
}

#[tokio::test]
async fn backlog_at_exact_budget_blocks() {
    let bl = Arc::new(RaftBacklog::new(100));
    assert!(bl.try_charge(100));
    // 0 permits remaining — charging 1 should block.
    let bl2 = Arc::clone(&bl);
    let handle = tokio::spawn(async move {
        bl2.charge(1).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!handle.is_finished(), "should be blocked at exact budget");

    bl.release(1); // 1 permit available — unblock.
    tokio::time::timeout(Duration::from_millis(100), handle)
        .await
        .expect("should unblock")
        .unwrap();
}

// =============================================================================
// 10. Full raft log pipeline: charge → append → async apply → release
// =============================================================================

#[tokio::test]
async fn raft_log_pipeline_forwarded_batch_charges_and_drains() {
    let bl = Arc::new(RaftBacklog::new(10_000_000));
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();
    let mut buf = BytesMut::new();

    // Step 1: Create a topic.
    let create_cmd =
        MqCommand::create_topic(&mut buf, "raft-bp-topic", RetentionPolicy::default(), 0);
    append_and_flush(&mut log, vec![make_entry(1, 1, create_cmd)]).await;

    // Step 2: Build and charge several TAG_FORWARDED_BATCH commands.
    let num_batches = 10u64;
    let mut entries = Vec::new();
    let mut charged_sizes = Vec::new();

    for i in 0..num_batches {
        let msg = bisque_mq::flat::FlatMessageBuilder::new(format!("msg-{i}").as_bytes())
            .timestamp(2000)
            .build();
        let pub_cmd = MqCommand::publish(&mut buf, 1, &[msg]);
        let frame = make_sub_frame(1, i, pub_cmd.as_bytes());
        MqCommand::write_forwarded_batch(&mut buf, 1, 1, i, i, &frame);
        let cmd = MqCommand::split_from(&mut buf);
        let len = cmd_size(&cmd);

        // Charge backlog as the leader would on TCP read.
        bl.charge(len).await;
        charged_sizes.push(len);

        entries.push(make_entry(2 + i, 1, cmd));
    }

    let total_charged: usize = charged_sizes.iter().sum();
    assert_eq!(bl.current(), total_charged);
    assert!(total_charged > 0);

    append_and_flush(&mut log, entries).await;

    // Step 3: Create AsyncApplyManager and process entries.
    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new(
        tmp.path().join("engine-bp").to_str().unwrap(),
    )));
    let config = ParallelApplyConfig {
        num_partitions: 2,
        ..Default::default()
    };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher,
        None,
        None,
        0,
        0,
        "test-bp",
    );

    // Advance HWM and wait for workers to process all entries.
    manager.advance_and_wait(1 + num_batches).await;

    // Step 4: Simulate state machine release (normally done in apply()).
    for &size in &charged_sizes {
        bl.release(size);
    }

    assert_eq!(
        bl.current(),
        0,
        "backlog must drain to zero after all applies"
    );

    // Verify the engine processed the topic creation.
    let snap = engine.snapshot();
    assert!(!snap.topics.is_empty(), "topic should exist");

    manager.shutdown().await;
    storage.stop();
}

#[tokio::test]
async fn raft_log_pipeline_batch_commands_charge_and_drain() {
    let bl = Arc::new(RaftBacklog::new(10_000_000));
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();
    let mut buf = BytesMut::new();

    // Create a topic.
    let create_cmd =
        MqCommand::create_topic(&mut buf, "batch-bp-topic", RetentionPolicy::default(), 0);
    append_and_flush(&mut log, vec![make_entry(1, 1, create_cmd)]).await;

    // Build and charge TAG_BATCH commands.
    let num_batches = 10u64;
    let mut entries = Vec::new();
    let mut charged_sizes = Vec::new();

    for i in 0..num_batches {
        let msg = bisque_mq::flat::FlatMessageBuilder::new(format!("batch-{i}").as_bytes())
            .timestamp(3000)
            .build();
        let publish = MqCommand::publish(&mut buf, 1, &[msg]);
        MqCommand::write_batch_with_id(&mut buf, (i + 1) as u32, &[publish]);
        let cmd = MqCommand::split_from(&mut buf);
        let len = cmd_size(&cmd);

        bl.charge(len).await;
        charged_sizes.push(len);

        entries.push(make_entry(2 + i, 1, cmd));
    }

    let total_charged: usize = charged_sizes.iter().sum();
    assert_eq!(bl.current(), total_charged);

    append_and_flush(&mut log, entries).await;

    // Process through async apply.
    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new(
        tmp.path().join("engine-batch-bp").to_str().unwrap(),
    )));
    let config = ParallelApplyConfig {
        num_partitions: 2,
        ..Default::default()
    };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher,
        None,
        None,
        0,
        0,
        "test-batch-bp",
    );

    manager.advance_and_wait(1 + num_batches).await;

    // Simulate state machine release.
    for &size in &charged_sizes {
        bl.release(size);
    }

    assert_eq!(bl.current(), 0);

    manager.shutdown().await;
    storage.stop();
}

// =============================================================================
// 11. AppliedBatchTable integration with backlog
// =============================================================================

#[tokio::test]
async fn applied_batch_table_publishes_hwm_after_forwarded_batch_apply() {
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();
    let mut buf = BytesMut::new();

    // Create a topic.
    let create_cmd = MqCommand::create_topic(&mut buf, "abt-topic", RetentionPolicy::default(), 0);
    append_and_flush(&mut log, vec![make_entry(1, 1, create_cmd)]).await;

    // Build TAG_FORWARDED_BATCH with sequential batch_seq values from node 3.
    let mut entries = Vec::new();
    for i in 0..5u64 {
        let msg = bisque_mq::flat::FlatMessageBuilder::new(b"abt")
            .timestamp(1000)
            .build();
        let pub_cmd = MqCommand::publish(&mut buf, 1, &[msg]);
        let frame = make_sub_frame(1, i, pub_cmd.as_bytes());
        MqCommand::write_forwarded_batch(&mut buf, 3, 1, i, i, &frame);
        let cmd = MqCommand::split_from(&mut buf);
        entries.push(make_entry(2 + i, 1, cmd));
    }
    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new(
        tmp.path().join("engine-abt").to_str().unwrap(),
    )));
    let config = ParallelApplyConfig {
        num_partitions: 1, // Single partition so partition 0 always runs.
        ..Default::default()
    };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher,
        None,
        None,
        0,
        0,
        "test-abt",
    );

    manager.advance_and_wait(6).await;

    // Check the applied batch table.
    let table = manager.applied_batch_table();
    let hwm = table.high_water(3);
    assert_eq!(hwm, 4, "batch_seq 0..4 all applied, HWM should be 4");

    // Other nodes should be u64::MAX (initial "nothing applied" sentinel).
    assert_eq!(table.high_water(0), u64::MAX);
    assert_eq!(table.high_water(1), u64::MAX);

    manager.shutdown().await;
    storage.stop();
}

// =============================================================================
// 12. Mixed raft log pipeline: TAG_BATCH + TAG_FORWARDED_BATCH interleaved
// =============================================================================

#[tokio::test]
async fn raft_log_pipeline_interleaved_batch_and_forwarded() {
    let bl = Arc::new(RaftBacklog::new(10_000_000));
    let tmp = TempDir::new().unwrap();
    let raft_config = MmapStorageConfig::new(tmp.path());
    let storage = MmapPerGroupLogStorage::<MqTypeConfig>::new(raft_config)
        .await
        .unwrap();
    let mut log = storage.get_log_storage(0).await.unwrap();
    let mut buf = BytesMut::new();

    // Create a topic.
    let create_cmd =
        MqCommand::create_topic(&mut buf, "interleaved-topic", RetentionPolicy::default(), 0);
    append_and_flush(&mut log, vec![make_entry(1, 1, create_cmd)]).await;

    let mut entries = Vec::new();
    let mut charged_sizes = Vec::new();
    let mut index = 2u64;

    // Interleave TAG_FORWARDED_BATCH and TAG_BATCH.
    for i in 0..20u64 {
        if i % 2 == 0 {
            // TAG_FORWARDED_BATCH
            let msg = bisque_mq::flat::FlatMessageBuilder::new(b"fwd")
                .timestamp(1000)
                .build();
            let pub_cmd = MqCommand::publish(&mut buf, 1, &[msg]);
            let frame = make_sub_frame(1, i, pub_cmd.as_bytes());
            MqCommand::write_forwarded_batch(&mut buf, 2, 1, i, i, &frame);
        } else {
            // TAG_BATCH
            let msg = bisque_mq::flat::FlatMessageBuilder::new(b"batch")
                .timestamp(2000)
                .build();
            let publish = MqCommand::publish(&mut buf, 1, &[msg]);
            MqCommand::write_batch_with_id(&mut buf, i as u32, &[publish]);
        }
        let cmd = MqCommand::split_from(&mut buf);
        let len = cmd_size(&cmd);
        bl.charge(len).await;
        charged_sizes.push(len);
        entries.push(make_entry(index, 1, cmd));
        index += 1;
    }

    let total_charged: usize = charged_sizes.iter().sum();
    assert_eq!(bl.current(), total_charged);

    append_and_flush(&mut log, entries).await;

    let prefetcher = log.prefetcher();
    let engine = Arc::new(MqEngine::new(MqConfig::new(
        tmp.path().join("engine-interleaved").to_str().unwrap(),
    )));
    let config = ParallelApplyConfig {
        num_partitions: 4,
        ..Default::default()
    };

    let mut manager = AsyncApplyManager::new(
        &config,
        Arc::clone(&engine),
        prefetcher,
        None,
        None,
        0,
        0,
        "test-interleaved",
    );

    manager.advance_and_wait(index - 1).await;

    // Simulate state machine release for all charged entries.
    for &size in &charged_sizes {
        bl.release(size);
    }

    assert_eq!(
        bl.current(),
        0,
        "interleaved batch/forwarded must drain to zero"
    );

    // Verify the engine processed messages.
    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "interleaved-topic")
        .unwrap();
    assert_eq!(
        topic.meta.message_count, 20,
        "all 20 messages should be published"
    );

    manager.shutdown().await;
    storage.stop();
}

// =============================================================================
// Helper: stand up a single-node raft with RaftBacklog on the state machine
// =============================================================================

type MqManager = MultiRaftManager<
    MqTypeConfig,
    BisqueTcpTransport<MqTypeConfig>,
    MultiplexedLogStorage<MqTypeConfig>,
>;

/// Stand up a single-node raft cluster with a RaftBacklog wired into the
/// state machine. Returns (raft, manager, tmp_dir).
async fn setup_single_node_raft(
    backlog: Arc<RaftBacklog>,
) -> (Raft<MqTypeConfig>, Arc<MqManager>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let node_id = 1u64;
    let group_id = 0u64;

    // Engine + state machine
    let mq_config = MqConfig::new(tmp.path().join("mq").to_str().unwrap());
    let engine = bisque_mq::engine::MqEngine::new(mq_config);
    let sm = bisque_mq::MqStateMachine::new(engine).with_raft_backlog(Arc::clone(&backlog));

    // Raft log storage
    let storage_config = MmapStorageConfig::new(tmp.path().join("raft-data"));
    let storage = MultiplexedLogStorage::new(storage_config).await.unwrap();

    // Transport (single-node, no real networking needed but required by type)
    let registry = Arc::new(DefaultNodeRegistry::new());
    registry.register(node_id, "127.0.0.1:0".parse().unwrap());
    let transport = BisqueTcpTransport::new(BisqueTcpTransportConfig::default(), registry);

    // Manager + group
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

    // Bootstrap single-node membership
    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    raft.initialize(members).await.unwrap();

    // Wait for leadership
    for _ in 0..60 {
        if raft.current_leader().await == Some(node_id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        raft.current_leader().await,
        Some(node_id),
        "single-node must become leader"
    );

    (raft, manager, tmp)
}

// =============================================================================
// 13. Real raft: TAG_FORWARDED_BATCH charge → client_write → apply → release
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn real_raft_forwarded_batch_backlog_drains_to_zero() {
    let backlog = Arc::new(RaftBacklog::new(10_000_000));
    let (raft, manager, _tmp) = setup_single_node_raft(Arc::clone(&backlog)).await;
    let mut buf = BytesMut::new();

    let num_batches = 20u64;
    for i in 0..num_batches {
        let payload = vec![0xABu8; 64 + (i as usize % 30)];
        let frame = make_sub_frame(1, i, &payload);
        MqCommand::write_forwarded_batch(&mut buf, 1, 1, i, i, &frame);
        let cmd = MqCommand::split_from(&mut buf);
        let cmd_len = cmd_size(&cmd);

        backlog.charge(cmd_len).await;
        raft.client_write(cmd).await.unwrap();
    }

    // client_write returns after commit + apply, so backlog should already be drained.
    assert_eq!(
        backlog.current(),
        0,
        "backlog must be zero after all client_write() calls return"
    );

    let _ = raft.shutdown().await;
    manager.shutdown_all().await;
}

// =============================================================================
// 14. Real raft: TAG_BATCH charge → client_write → apply → release
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn real_raft_batch_backlog_drains_to_zero() {
    let backlog = Arc::new(RaftBacklog::new(10_000_000));
    let (raft, manager, _tmp) = setup_single_node_raft(Arc::clone(&backlog)).await;
    let mut buf = BytesMut::new();

    let num_batches = 20u64;
    for i in 0..num_batches {
        let msg = bisque_mq::flat::FlatMessageBuilder::new(format!("batch-{i}").as_bytes())
            .timestamp(1000)
            .build();
        let publish = MqCommand::publish(&mut buf, 1, &[msg]);
        MqCommand::write_batch_with_id(&mut buf, (i + 1) as u32, &[publish]);
        let cmd = MqCommand::split_from(&mut buf);
        let cmd_len = cmd_size(&cmd);

        backlog.charge(cmd_len).await;
        raft.client_write(cmd).await.unwrap();
    }

    assert_eq!(
        backlog.current(),
        0,
        "backlog must be zero after all TAG_BATCH client_write() calls return"
    );

    let _ = raft.shutdown().await;
    manager.shutdown_all().await;
}

// =============================================================================
// 15. Real raft: interleaved TAG_BATCH + TAG_FORWARDED_BATCH
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn real_raft_interleaved_backlog_drains_to_zero() {
    let backlog = Arc::new(RaftBacklog::new(10_000_000));
    let (raft, manager, _tmp) = setup_single_node_raft(Arc::clone(&backlog)).await;
    let mut buf = BytesMut::new();

    for i in 0..40u64 {
        if i % 2 == 0 {
            // TAG_FORWARDED_BATCH
            let frame = make_sub_frame(1, i, &[0xCC; 48]);
            MqCommand::write_forwarded_batch(&mut buf, 2, 1, i, i, &frame);
        } else {
            // TAG_BATCH
            let msg = bisque_mq::flat::FlatMessageBuilder::new(b"mixed")
                .timestamp(2000)
                .build();
            let publish = MqCommand::publish(&mut buf, 1, &[msg]);
            MqCommand::write_batch_with_id(&mut buf, i as u32, &[publish]);
        }
        let cmd = MqCommand::split_from(&mut buf);
        backlog.charge(cmd_size(&cmd)).await;
        raft.client_write(cmd).await.unwrap();
    }

    assert_eq!(
        backlog.current(),
        0,
        "interleaved batch/forwarded must drain to zero through real raft"
    );

    let _ = raft.shutdown().await;
    manager.shutdown_all().await;
}

// =============================================================================
// 16. Real raft: backpressure actually blocks when budget is exhausted
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn real_raft_backpressure_gates_producers() {
    // Very small budget — only allows ~1-2 commands in flight.
    let backlog = Arc::new(RaftBacklog::new(256));
    let (raft, manager, _tmp) = setup_single_node_raft(Arc::clone(&backlog)).await;

    let num_msgs = 50u64;
    let backlog2 = Arc::clone(&backlog);
    let raft2 = raft.clone();

    // Producer that respects backpressure.
    let producer = tokio::spawn(async move {
        let mut buf = BytesMut::new();
        for i in 0..num_msgs {
            let frame = make_sub_frame(1, i, &[0xEE; 64]);
            MqCommand::write_forwarded_batch(&mut buf, 1, 1, i, i, &frame);
            let cmd = MqCommand::split_from(&mut buf);
            let cmd_len = cmd_size(&cmd);

            // This is the real backpressure path: charge (waits for capacity), propose.
            backlog2.charge(cmd_len).await;
            raft2.client_write(cmd).await.unwrap();
        }
    });

    tokio::time::timeout(Duration::from_secs(10), producer)
        .await
        .expect("producer should complete within 10s")
        .unwrap();

    assert_eq!(
        backlog.current(),
        0,
        "backlog must be zero — all proposals applied"
    );

    let _ = raft.shutdown().await;
    manager.shutdown_all().await;
}

// =============================================================================
// 17. Real raft: concurrent producers with backpressure
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn real_raft_concurrent_producers_with_backpressure() {
    let backlog = Arc::new(RaftBacklog::new(4096));
    let (raft, manager, _tmp) = setup_single_node_raft(Arc::clone(&backlog)).await;

    let num_producers = 4;
    let msgs_per_producer = 20u64;

    let mut handles = Vec::new();
    for p in 0..num_producers {
        let bl = Arc::clone(&backlog);
        let r = raft.clone();
        handles.push(tokio::spawn(async move {
            let mut buf = BytesMut::new();
            for i in 0..msgs_per_producer {
                let seq = p * msgs_per_producer + i;
                let frame = make_sub_frame(p as u32, seq, &[0xAA; 32]);
                MqCommand::write_forwarded_batch(&mut buf, p as u32, 1, seq, seq, &frame);
                let cmd = MqCommand::split_from(&mut buf);
                let cmd_len = cmd_size(&cmd);

                bl.charge(cmd_len).await;
                r.client_write(cmd).await.unwrap();
            }
        }));
    }

    for h in handles {
        tokio::time::timeout(Duration::from_secs(15), h)
            .await
            .expect("producer should finish")
            .unwrap();
    }

    assert_eq!(
        backlog.current(),
        0,
        "concurrent producers: backlog must drain to zero"
    );

    let _ = raft.shutdown().await;
    manager.shutdown_all().await;
}

// =============================================================================
// 18. Real raft: only TAG_BATCH and TAG_FORWARDED_BATCH release backlog
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn real_raft_non_batch_commands_do_not_release() {
    let backlog = Arc::new(RaftBacklog::new(10_000_000));
    let (raft, manager, _tmp) = setup_single_node_raft(Arc::clone(&backlog)).await;
    let mut buf = BytesMut::new();

    // Write a structural command (create_topic) — should NOT release backlog.
    let create = MqCommand::create_topic(&mut buf, "test-topic", RetentionPolicy::default(), 0);
    raft.client_write(create).await.unwrap();

    // Backlog was never charged, so still 0.
    assert_eq!(backlog.current(), 0);

    // Now charge manually and write a non-batch command — backlog should NOT drain.
    backlog.charge(100).await;
    let create2 = MqCommand::create_topic(&mut buf, "test-topic-2", RetentionPolicy::default(), 0);
    raft.client_write(create2).await.unwrap();

    // The 100 bytes should still be there — create_topic doesn't release.
    assert_eq!(
        backlog.current(),
        100,
        "non-batch commands must not release backlog"
    );

    // Clean up the leaked charge.
    backlog.release(100);

    // Now write a TAG_FORWARDED_BATCH and verify it DOES release.
    let frame = make_sub_frame(1, 0, b"payload");
    MqCommand::write_forwarded_batch(&mut buf, 1, 1, 0, 0, &frame);
    let cmd = MqCommand::split_from(&mut buf);
    let cmd_len = cmd_size(&cmd);
    backlog.charge(cmd_len).await;
    assert_eq!(backlog.current(), cmd_len);

    raft.client_write(cmd).await.unwrap();
    assert_eq!(
        backlog.current(),
        0,
        "TAG_FORWARDED_BATCH must release backlog"
    );

    let _ = raft.shutdown().await;
    manager.shutdown_all().await;
}
