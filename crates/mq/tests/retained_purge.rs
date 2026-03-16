//! Comprehensive tests for RetainedValue mmap-backed lifecycle, detach/purge
//! promotion to heap, and purge sweep correctness for both exchanges and topics.
//!
//! Covers:
//!   - RetainedValue construction, detach, idempotent detach
//!   - Exchange retained messages with segment provenance via engine API
//!   - Topic retained messages with segment provenance
//!   - Purge sweep correctness via engine snapshot inspection
//!   - Edge cases: empty messages, detach preserves data, segment reuse

use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::exchange::RetainedValue;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::topic::{TopicMeta, TopicState};
use bisque_mq::types::*;
use bytes::Bytes;

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
    apply_engine_seg(engine, cmd, log_index, current_time, None)
}

fn apply_engine_seg(
    engine: &bisque_mq::engine::MqEngine,
    cmd: &MqCommand,
    log_index: u64,
    current_time: u64,
    segment_id: Option<u64>,
) -> ResponseEntry {
    let mut _buf = BytesMut::new();
    engine.apply_command(cmd, &mut _buf, log_index, current_time, segment_id);
    ResponseEntry::split_from(&mut _buf)
}

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-retained-purge-test"))
}

fn make_msg(value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(value).timestamp(1000).build()
}

fn create_exchange_in_engine(engine: &mut MqEngine, name: &str, log_index: u64) -> u64 {
    let mut buf = bytes::BytesMut::new();
    let __e = apply_engine(
        &engine,
        &MqCommand::create_exchange(&mut buf, name, ExchangeType::Topic),
        log_index,
        1000,
    );
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    __e.entity_id()
}

fn get_retained(
    engine: &mut MqEngine,
    exchange_id: u64,
    filter: Option<&str>,
    log_index: u64,
) -> Vec<RetainedEntry> {
    let mut buf = bytes::BytesMut::new();
    let __e = apply_engine(
        &engine,
        &MqCommand::get_retained(&mut buf, exchange_id, filter),
        log_index,
        9999,
    );
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_RETAINED_MESSAGES,
        "expected RetainedMessages"
    );
    __e.retained_messages().collect()
}

// =============================================================================
// RetainedValue unit tests — construction and detach
// =============================================================================

#[test]
fn retained_value_mmap_backed_stores_segment_id() {
    let rv = RetainedValue::mmap_backed(42, Bytes::from_static(b"hello"));
    assert_eq!(rv.segment_id, Some(42));
    assert_eq!(&rv.message[..], b"hello");
}

#[test]
fn retained_value_heap_has_no_segment_id() {
    let rv = RetainedValue::heap(Bytes::from_static(b"hello"));
    assert_eq!(rv.segment_id, None);
    assert_eq!(&rv.message[..], b"hello");
}

#[test]
fn retained_value_detach_clears_segment_and_copies_data() {
    let original = Bytes::from_static(b"mmap-data");
    let mut rv = RetainedValue::mmap_backed(7, original.clone());

    let old = rv.detach();
    assert_eq!(old, Some(7));
    assert_eq!(rv.segment_id, None);
    assert_eq!(&rv.message[..], b"mmap-data");
    assert_eq!(rv.message, original);
}

#[test]
fn retained_value_detach_idempotent() {
    let mut rv = RetainedValue::mmap_backed(10, Bytes::from_static(b"data"));

    let first = rv.detach();
    assert_eq!(first, Some(10));

    let second = rv.detach();
    assert_eq!(second, None, "second detach is no-op");

    let third = rv.detach();
    assert_eq!(third, None, "third detach is no-op");

    assert_eq!(&rv.message[..], b"data");
}

#[test]
fn retained_value_detach_heap_is_noop() {
    let mut rv = RetainedValue::heap(Bytes::from_static(b"heap"));
    let result = rv.detach();
    assert_eq!(result, None);
    assert_eq!(&rv.message[..], b"heap");
}

#[test]
fn retained_value_detach_empty_message() {
    let mut rv = RetainedValue::mmap_backed(5, Bytes::new());
    let old = rv.detach();
    assert_eq!(old, Some(5));
    assert_eq!(rv.segment_id, None);
    assert!(rv.message.is_empty());
}

#[test]
fn retained_value_detach_large_message() {
    let large = Bytes::from(vec![0xABu8; 1024 * 1024]);
    let mut rv = RetainedValue::mmap_backed(99, large.clone());

    rv.detach();
    assert_eq!(rv.segment_id, None);
    assert_eq!(rv.message.len(), 1024 * 1024);
    assert_eq!(rv.message, large);
}

#[test]
fn retained_value_detach_then_construct_new_mmap() {
    // Simulates the lifecycle: mmap → detach → new mmap-backed value
    let mut rv = RetainedValue::mmap_backed(1, Bytes::from_static(b"old"));
    rv.detach();
    assert_eq!(rv.segment_id, None);

    // Replace with new mmap-backed value (simulates new publish)
    rv = RetainedValue::mmap_backed(2, Bytes::from_static(b"new"));
    assert_eq!(rv.segment_id, Some(2));
    assert_eq!(&rv.message[..], b"new");
}

// =============================================================================
// Exchange retained — segment provenance via engine API
// =============================================================================

#[test]
fn exchange_set_retained_with_segment_id_preserves_data() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange_in_engine(&mut engine, "mqtt/ex", 1);

    let msg = make_msg(b"sensor-data");
    apply_engine_seg(
        &engine,
        &MqCommand::set_retained(&mut buf, exchange_id, "sensors/temp", &msg),
        2,
        1001,
        Some(42),
    );

    // Verify data accessible via GET_RETAINED
    let entries = get_retained(&mut engine, exchange_id, Some("sensors/temp"), 3);
    assert_eq!(entries.len(), 1);
    assert!(!entries[0].message.is_empty());
}

#[test]
fn exchange_retained_overwrite_on_different_segments() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let exchange_id = create_exchange_in_engine(&mut engine, "mqtt/ex", 1);

    let msg1 = make_msg(b"first");
    apply_engine_seg(
        &engine,
        &MqCommand::set_retained(&mut buf, exchange_id, "key", &msg1),
        2,
        1001,
        Some(10),
    );

    let msg2 = make_msg(b"second");
    apply_engine_seg(
        &engine,
        &MqCommand::set_retained(&mut buf, exchange_id, "key", &msg2),
        3,
        1002,
        Some(20),
    );

    // Only one entry
    let entries = get_retained(&mut engine, exchange_id, Some("key"), 4);
    assert_eq!(entries.len(), 1);
}

#[test]
fn exchange_retained_multiple_keys_different_segments() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange_in_engine(&mut engine, "mqtt/ex", 1);

    for (i, seg) in [10u64, 20, 30].iter().enumerate() {
        let msg = make_msg(format!("msg-{}", i).as_bytes());
        let key = format!("key-{}", i);
        apply_engine_seg(
            &engine,
            &MqCommand::set_retained(&mut buf, exchange_id, &key, &msg),
            i as u64 + 2,
            1001 + i as u64,
            Some(*seg),
        );
    }

    let entries = get_retained(&mut engine, exchange_id, None, 10);
    assert_eq!(entries.len(), 3);
}

#[test]
fn exchange_delete_then_reset_on_new_segment() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange_in_engine(&mut engine, "mqtt/ex", 1);

    let msg = make_msg(b"first");
    apply_engine_seg(
        &engine,
        &MqCommand::set_retained(&mut buf, exchange_id, "key", &msg),
        2,
        1001,
        Some(10),
    );

    apply_engine(
        &engine,
        &MqCommand::delete_retained(&mut buf, exchange_id, "key"),
        3,
        1002,
    );

    let msg2 = make_msg(b"second");
    apply_engine_seg(
        &engine,
        &MqCommand::set_retained(&mut buf, exchange_id, "key", &msg2),
        4,
        1003,
        Some(20),
    );

    let entries = get_retained(&mut engine, exchange_id, Some("key"), 5);
    assert_eq!(entries.len(), 1);
}

// =============================================================================
// Snapshot round-trip preserves data after purge sweep
// =============================================================================

#[test]
fn snapshot_preserves_retained_messages_after_segment_provenance() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let exchange_id = create_exchange_in_engine(&mut engine, "mqtt/ex", 1);

    // Set with mmap backing
    let msg = make_msg(b"will-survive-snapshot");
    apply_engine_seg(
        &engine,
        &MqCommand::set_retained(&mut buf, exchange_id, "key", &msg),
        2,
        1001,
        Some(42),
    );

    // Snapshot and restore — snapshot serialization doesn't preserve segment_id
    let snap = engine.snapshot();
    assert_eq!(snap.exchanges[0].retained.len(), 1);

    let mut engine2 = make_engine();
    engine2.restore(snap);

    let snap2 = engine2.snapshot();
    assert_eq!(snap2.exchanges[0].retained.len(), 1);
    assert_eq!(&snap2.exchanges[0].retained[0].routing_key[..], b"key");
}

#[test]
fn snapshot_with_many_retained_across_segments() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange_in_engine(&mut engine, "mqtt/ex", 1);

    for i in 0..20u64 {
        let msg = make_msg(format!("msg-{}", i).as_bytes());
        let key = format!("sensor/{}", i);
        let seg = (i / 5) + 1; // segments 1, 2, 3, 4
        apply_engine_seg(
            &engine,
            &MqCommand::set_retained(&mut buf, exchange_id, &key, &msg),
            i + 2,
            1001 + i,
            Some(seg),
        );
    }

    let snap = engine.snapshot();
    assert_eq!(snap.exchanges[0].retained.len(), 20);

    let engine2 = make_engine();
    engine2.restore(snap);

    let snap2 = engine2.snapshot();
    assert_eq!(snap2.exchanges[0].retained.len(), 20);
}

// =============================================================================
// Topic retained — via TopicState directly (public API)
// =============================================================================

fn make_retained_topic(name: &str) -> TopicState {
    let mut meta = TopicMeta::new(1, name.to_string(), 1000, RetentionPolicy::default());
    meta.retained = true;
    TopicState::new(meta, "test")
}

fn make_retained_topic_with_id(topic_id: u64, name: &str) -> TopicState {
    let mut meta = TopicMeta::new(topic_id, name.to_string(), 1000, RetentionPolicy::default());
    meta.retained = true;
    TopicState::new(meta, "test")
}

fn make_non_retained_topic(name: &str) -> TopicState {
    let meta = TopicMeta::new(1, name.to_string(), 1000, RetentionPolicy::default());
    TopicState::new(meta, "test")
}

#[test]
fn topic_publish_with_segment_creates_mmap_backed() {
    let topic = make_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(42));

    let rv = topic.get_retained().expect("should have retained");
    assert_eq!(rv.segment_id, Some(42));
}

#[test]
fn topic_publish_without_segment_creates_heap() {
    let topic = make_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), None);

    let rv = topic.get_retained().expect("should have retained");
    assert_eq!(rv.segment_id, None);
}

#[test]
fn topic_publish_overwrites_retained_with_new_segment() {
    let topic = make_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"v1")].into_iter(), Some(1));
    topic.apply_publish(11, vec![make_msg(b"v2")].into_iter(), Some(2));

    let rv = topic.get_retained().unwrap();
    assert_eq!(rv.segment_id, Some(2));
}

#[test]
fn topic_batch_retains_last_message() {
    let topic = make_retained_topic("t");
    let msgs = vec![make_msg(b"a"), make_msg(b"b"), make_msg(b"c")];
    topic.apply_publish(10, msgs.into_iter(), Some(5));

    let rv = topic.get_retained().unwrap();
    assert_eq!(rv.segment_id, Some(5));
}

#[test]
fn non_retained_topic_has_no_retained() {
    let topic = make_non_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(42));
    assert!(topic.get_retained().is_none());
}

#[test]
fn topic_detach_retained_promotes_to_heap() {
    let topic = make_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(7));

    assert!(topic.detach_retained_if_segment(7));

    let rv = topic.get_retained().unwrap();
    assert_eq!(rv.segment_id, None);
    assert!(!rv.message.is_empty());
}

#[test]
fn topic_detach_wrong_segment_is_noop() {
    let topic = make_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(42));

    assert!(!topic.detach_retained_if_segment(99));

    let rv = topic.get_retained().unwrap();
    assert_eq!(rv.segment_id, Some(42));
}

#[test]
fn topic_detach_idempotent() {
    let topic = make_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(7));

    assert!(topic.detach_retained_if_segment(7));
    assert!(!topic.detach_retained_if_segment(7)); // already detached

    let rv = topic.get_retained().unwrap();
    assert_eq!(rv.segment_id, None);
}

#[test]
fn topic_detach_then_new_publish_is_mmap_again() {
    let topic = make_retained_topic("t");

    topic.apply_publish(10, vec![make_msg(b"v1")].into_iter(), Some(1));
    topic.detach_retained_if_segment(1);
    assert_eq!(topic.get_retained().unwrap().segment_id, None);

    topic.apply_publish(11, vec![make_msg(b"v2")].into_iter(), Some(5));
    assert_eq!(topic.get_retained().unwrap().segment_id, Some(5));
}

#[test]
fn topic_multi_segment_lifecycle() {
    let topic = make_retained_topic("t");

    // Phase 1: segment 1
    topic.apply_publish(10, vec![make_msg(b"v1")].into_iter(), Some(1));
    assert_eq!(topic.get_retained().unwrap().segment_id, Some(1));

    // Phase 2: purge segment 1
    topic.detach_retained_if_segment(1);
    assert_eq!(topic.get_retained().unwrap().segment_id, None);

    // Phase 3: segment 2
    topic.apply_publish(11, vec![make_msg(b"v2")].into_iter(), Some(2));
    assert_eq!(topic.get_retained().unwrap().segment_id, Some(2));

    // Phase 4: purge segment 2
    topic.detach_retained_if_segment(2);
    assert_eq!(topic.get_retained().unwrap().segment_id, None);

    // Phase 5: segment 3
    topic.apply_publish(12, vec![make_msg(b"v3")].into_iter(), Some(3));
    assert_eq!(topic.get_retained().unwrap().segment_id, Some(3));
}

#[test]
fn topic_publish_transitions_segment() {
    let topic = make_retained_topic("t");

    topic.apply_publish(10, vec![make_msg(b"seg1")].into_iter(), Some(1));
    topic.apply_publish(11, vec![make_msg(b"seg2")].into_iter(), Some(2));

    // Purge segment 1 — no-op since retained is on segment 2
    assert!(!topic.detach_retained_if_segment(1));
    assert_eq!(topic.get_retained().unwrap().segment_id, Some(2));

    // Purge segment 2 — detaches
    assert!(topic.detach_retained_if_segment(2));
    assert_eq!(topic.get_retained().unwrap().segment_id, None);
}

// =============================================================================
// Partition-aware sweep simulation
// =============================================================================

#[test]
fn partition_sweep_topics_by_id_modulo() {
    let num_partitions = 3;
    let topics: Vec<TopicState> = (0..9)
        .map(|i| make_retained_topic_with_id(i + 1, &format!("t-{}", i)))
        .collect();

    // Publish on segment 10 to all topics
    for topic in &topics {
        topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(10));
    }

    // Worker 1 sweeps topics where topic_id % 3 == 1
    let purged_ids = [10u64];
    for topic in &topics {
        if (topic.meta.topic_id as usize) % num_partitions != 1 {
            continue;
        }
        for &seg in &purged_ids {
            topic.detach_retained_if_segment(seg);
        }
    }

    // Verify: only topics with id % 3 == 1 were swept (IDs: 1, 4, 7)
    for topic in &topics {
        let rv = topic.get_retained().unwrap();
        let expected_detached = (topic.meta.topic_id as usize) % num_partitions == 1;
        if expected_detached {
            assert_eq!(
                rv.segment_id, None,
                "topic {} should be detached",
                topic.meta.topic_id
            );
        } else {
            assert_eq!(
                rv.segment_id,
                Some(10),
                "topic {} should be untouched",
                topic.meta.topic_id
            );
        }
    }
}

#[test]
fn all_partitions_sweep_covers_all_topics() {
    let num_partitions = 4;
    let topics: Vec<TopicState> = (0..12)
        .map(|i| make_retained_topic_with_id(i + 1, &format!("t-{}", i)))
        .collect();

    for topic in &topics {
        topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(10));
    }

    // All workers sweep their partitions
    let purged_ids = [10u64];
    for partition_id in 0..num_partitions {
        for topic in &topics {
            if (topic.meta.topic_id as usize) % num_partitions != partition_id {
                continue;
            }
            for &seg in &purged_ids {
                topic.detach_retained_if_segment(seg);
            }
        }
    }

    // All topics should be detached
    for topic in &topics {
        assert_eq!(
            topic.get_retained().unwrap().segment_id,
            None,
            "topic {} should be detached",
            topic.meta.topic_id
        );
    }
}

// =============================================================================
// Double-sweep safety (simulates concurrent workers peeking same purge list)
// =============================================================================

#[test]
fn double_sweep_is_safe() {
    let topic = make_retained_topic("t");
    topic.apply_publish(10, vec![make_msg(b"data")].into_iter(), Some(10));

    // Two workers both sweep
    topic.detach_retained_if_segment(10);
    topic.detach_retained_if_segment(10); // idempotent

    let rv = topic.get_retained().unwrap();
    assert_eq!(rv.segment_id, None);
    assert!(!rv.message.is_empty());
}

#[test]
fn double_sweep_exchange_retained_value() {
    let mut rv = RetainedValue::mmap_backed(10, Bytes::from_static(b"data"));

    // First sweep
    let r1 = rv.detach();
    assert_eq!(r1, Some(10));

    // Second sweep (concurrent worker)
    let r2 = rv.detach();
    assert_eq!(r2, None);

    assert_eq!(rv.segment_id, None);
    assert_eq!(&rv.message[..], b"data");
}

// =============================================================================
// Edge: retained value with zero-length segment IDs
// =============================================================================

#[test]
fn retained_value_segment_id_zero_is_valid() {
    let mut rv = RetainedValue::mmap_backed(0, Bytes::from_static(b"seg-zero"));
    assert_eq!(rv.segment_id, Some(0));

    let old = rv.detach();
    assert_eq!(old, Some(0));
    assert_eq!(rv.segment_id, None);
}

#[test]
fn retained_value_max_segment_id() {
    let mut rv = RetainedValue::mmap_backed(u64::MAX, Bytes::from_static(b"max-seg"));
    assert_eq!(rv.segment_id, Some(u64::MAX));

    let old = rv.detach();
    assert_eq!(old, Some(u64::MAX));
    assert_eq!(rv.segment_id, None);
}
