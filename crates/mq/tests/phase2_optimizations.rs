//! Integration tests for Phase 2 optimizations (mq-optimizations-2.md).
//!
//! Tests cover:
//!   Opt 1: Event-driven delivery wiring (GroupNotifier integration)
//!   Opt 4: Will delay with cancellation on reconnect
//!   Opt 6: Shared subscription no-local validation
//!   Opt 7: Subscription metadata (group_id) in delivery response

use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::*;

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
    MqEngine::new(MqConfig::new("/tmp/mq-phase2-opt-test"))
}

fn make_flat_msg(value: &[u8]) -> bytes::Bytes {
    FlatMessageBuilder::new(value).timestamp(1000).build()
}

fn make_flat_msg_with_routing_key(value: &[u8], routing_key: &str) -> bytes::Bytes {
    FlatMessageBuilder::new(value)
        .routing_key(routing_key.as_bytes())
        .timestamp(1000)
        .build()
}

fn create_exchange(engine: &mut MqEngine, name: &str, log_index: u64, time: u64) -> u64 {
    let mut buf = bytes::BytesMut::new();
    let e = apply_engine(
        &engine,
        &MqCommand::create_exchange(&mut buf, name, ExchangeType::Topic),
        log_index,
        time,
    );
    assert_eq!(
        e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    e.entity_id()
}

fn create_queue(engine: &mut MqEngine, name: &str, log_index: u64, time: u64) -> u64 {
    let mut buf = bytes::BytesMut::new();
    let e = apply_engine(
        &engine,
        &MqCommand::create_queue(
            &mut buf,
            name,
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        log_index,
        time,
    );
    assert_eq!(
        e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    e.entity_id()
}

fn create_binding(
    engine: &mut MqEngine,
    exchange_id: u64,
    queue_id: u64,
    routing_key: &str,
    log_index: u64,
    time: u64,
) -> u64 {
    let mut buf = bytes::BytesMut::new();
    let e = apply_engine(
        &engine,
        &MqCommand::create_binding(&mut buf, exchange_id, queue_id, Some(routing_key)),
        log_index,
        time,
    );
    assert_eq!(
        e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    e.entity_id()
}

fn create_session(
    engine: &mut MqEngine,
    session_id: u64,
    client_id: &str,
    log_index: u64,
    time: u64,
) {
    let mut buf = bytes::BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::create_session(&mut buf, session_id, client_id, 30000, 0),
        log_index,
        time,
    );
}

fn set_will(
    engine: &mut MqEngine,
    session_id: u64,
    exchange_id: u64,
    delay_secs: u32,
    routing_key: &str,
    message: &bytes::Bytes,
    log_index: u64,
    time: u64,
) {
    let mut buf = bytes::BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            session_id,
            exchange_id,
            delay_secs,
            0,
            false,
            routing_key,
            message,
        ),
        log_index,
        time,
    );
}

/// For a queue (ack-variant consumer group with auto-created source topic),
/// the source topic ID is allocated immediately after the group ID.
fn source_topic_id(group_id: u64) -> u64 {
    group_id + 1
}

fn enqueue_messages(engine: &mut MqEngine, topic_id: u64, count: usize, log_index: u64, time: u64) {
    let mut buf = bytes::BytesMut::new();
    for i in 0..count {
        let msg = make_flat_msg(format!("msg-{}", i).as_bytes());
        apply_engine(
            &engine,
            &MqCommand::publish(&mut buf, topic_id, &[msg]),
            log_index + i as u64,
            time,
        );
    }
}

fn deliver_messages(
    engine: &mut MqEngine,
    group_id: u64,
    consumer_id: u64,
    max_count: u32,
    log_index: u64,
    time: u64,
) -> Vec<DeliveredMessage> {
    let mut buf = bytes::BytesMut::new();
    let e = apply_engine(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, consumer_id, max_count),
        log_index,
        time,
    );
    assert_eq!(e.tag(), ResponseEntry::TAG_MESSAGES, "expected Messages");
    e.messages().collect()
}

// =============================================================================
// Opt 1: Event-Driven Delivery Wiring (GroupNotifier)
// =============================================================================

#[tokio::test]
async fn test_opt1_notifier_triggered_on_enqueue() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "notify-q", 1, 1000);

    let notify = std::sync::Arc::new(tokio::sync::Notify::new());
    engine.metadata().group_notifier.watch(queue_id, &notify);

    // Spawn a waiter before triggering so notify_waiters() finds it.
    let n = notify.clone();
    let waiter = tokio::spawn(async move { n.notified().await });
    tokio::task::yield_now().await;

    let msg = make_flat_msg(b"hello");
    apply_engine(
        &engine,
        &MqCommand::publish(&mut buf, source_topic_id(queue_id), &[msg]),
        2,
        1001,
    );

    tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
        .await
        .expect("should receive notification after enqueue")
        .unwrap();
}

#[tokio::test]
async fn test_opt1_notifier_triggered_on_exchange_publish() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/ex", 1, 1000);
    let queue_id = create_queue(&mut engine, "sub-q", 2, 1001);
    create_binding(
        &mut engine,
        exchange_id,
        source_topic_id(queue_id),
        "sensors/#",
        3,
        1002,
    );

    let notify = std::sync::Arc::new(tokio::sync::Notify::new());
    engine.metadata().group_notifier.watch(queue_id, &notify);

    let n = notify.clone();
    let waiter = tokio::spawn(async move { n.notified().await });
    tokio::task::yield_now().await;

    let msg = make_flat_msg_with_routing_key(b"temp=22", "sensors/temp");
    apply_engine(
        &engine,
        &MqCommand::publish_to_exchange(&mut buf, exchange_id, &[msg]),
        4,
        1003,
    );

    tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
        .await
        .expect("should receive notification after exchange publish")
        .unwrap();
}

#[tokio::test]
async fn test_opt1_notifier_triggered_on_nack() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "nack-q", 1, 1000);
    create_session(&mut engine, 100, "consumer-1", 2, 1001);

    enqueue_messages(&mut engine, source_topic_id(queue_id), 1, 3, 1002);
    let msgs = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    assert_eq!(msgs.len(), 1);

    let notify = std::sync::Arc::new(tokio::sync::Notify::new());
    engine.metadata().group_notifier.watch(queue_id, &notify);

    let n = notify.clone();
    let waiter = tokio::spawn(async move { n.notified().await });
    tokio::task::yield_now().await;

    let msg_ids: Vec<u64> = msgs.iter().map(|m| m.message_id).collect();
    apply_engine(
        &engine,
        &MqCommand::group_nack(&mut buf, queue_id, &msg_ids),
        5,
        1004,
    );

    tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
        .await
        .expect("should receive notification after nack")
        .unwrap();
}

#[tokio::test]
async fn test_opt1_notifier_multiple_watchers() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "multi-watch-q", 1, 1000);

    let n1 = std::sync::Arc::new(tokio::sync::Notify::new());
    let n2 = std::sync::Arc::new(tokio::sync::Notify::new());
    engine.metadata().group_notifier.watch(queue_id, &n1);
    engine.metadata().group_notifier.watch(queue_id, &n2);

    let n1c = n1.clone();
    let n2c = n2.clone();
    let w1 = tokio::spawn(async move { n1c.notified().await });
    let w2 = tokio::spawn(async move { n2c.notified().await });
    tokio::task::yield_now().await;

    let msg = make_flat_msg(b"hello");
    apply_engine(
        &engine,
        &MqCommand::publish(&mut buf, source_topic_id(queue_id), &[msg]),
        2,
        1001,
    );

    tokio::time::timeout(std::time::Duration::from_millis(100), w1)
        .await
        .expect("watcher 1 should receive notification")
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), w2)
        .await
        .expect("watcher 2 should receive notification")
        .unwrap();
}

#[tokio::test]
async fn test_opt1_notifier_no_spurious_for_other_queues() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "q1", 1, 1000);
    let q2 = create_queue(&mut engine, "q2", 2, 1001);

    let n1 = std::sync::Arc::new(tokio::sync::Notify::new());
    let n2 = std::sync::Arc::new(tokio::sync::Notify::new());
    engine.metadata().group_notifier.watch(q1, &n1);
    engine.metadata().group_notifier.watch(q2, &n2);

    let n1c = n1.clone();
    let n2c = n2.clone();
    let w1 = tokio::spawn(async move { n1c.notified().await });
    let w2 = tokio::spawn(async move { n2c.notified().await });
    tokio::task::yield_now().await;

    let msg = make_flat_msg(b"only-q1");
    apply_engine(
        &engine,
        &MqCommand::publish(&mut buf, source_topic_id(q1), &[msg]),
        3,
        1002,
    );

    tokio::time::timeout(std::time::Duration::from_millis(100), w1)
        .await
        .expect("q1 watcher should get notification")
        .unwrap();
    // q2 should NOT be notified — expect timeout.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), w2)
            .await
            .is_err(),
        "q2 watcher should NOT get notification"
    );
}

// =============================================================================
// Opt 4: Will Delay with Cancellation on Reconnect
// =============================================================================

#[test]
fn test_opt4_will_delay_stores_pending() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Register session
    create_session(&mut engine, 100, "client-abc", 2, 1001);

    // Set will with delay_secs > 0
    let will_msg = make_flat_msg(b"offline");
    set_will(
        &mut engine,
        100,
        exchange_id,
        30,
        "clients/status",
        &will_msg,
        3,
        1002,
    );

    // Disconnect — will should be stored as pending
    let resp = apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        4,
        1003,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_WILL_PENDING,
        "expected WillPending"
    );
    assert_eq!(resp.will_pending_delay_ms(), 30_000);
}

#[test]
fn test_opt4_will_no_delay_fires_immediately() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-target-q", 2, 1001);
    create_binding(
        &mut engine,
        exchange_id,
        queue_id,
        "clients/status",
        3,
        1002,
    );

    create_session(&mut engine, 100, "client-xyz", 4, 1003);

    // Set will with delay_secs = 0
    // Embed routing key in the flat message so publish_will_payload can route it
    let will_msg = make_flat_msg_with_routing_key(b"offline", "clients/status");
    set_will(
        &mut engine,
        100,
        exchange_id,
        0,
        "clients/status",
        &will_msg,
        5,
        1004,
    );

    // Disconnect — will should fire immediately (returns Ok)
    let resp = apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        6,
        1005,
    );
    assert!(
        resp.is_ok(),
        "immediate will disconnect should return Ok, got tag={}",
        resp.tag()
    );

    // The will message should be in the target queue
    create_session(&mut engine, 200, "reader", 7, 1006);
    let msgs = deliver_messages(&mut engine, queue_id, 200, 10, 8, 1007);
    assert!(
        !msgs.is_empty(),
        "will message should have been routed to queue"
    );
}

#[test]
fn test_opt4_cancel_pending_will_on_reconnect() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    create_session(&mut engine, 100, "client-reconnect", 2, 1001);

    let will_msg = make_flat_msg(b"offline");
    set_will(
        &mut engine,
        100,
        exchange_id,
        60,
        "clients/status",
        &will_msg,
        3,
        1002,
    );

    // Disconnect — will becomes pending
    apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        4,
        1003,
    );

    // Reconnect with same client_id — should cancel pending will
    create_session(&mut engine, 200, "client-reconnect", 5, 1004);

    // Try to fire pending wills — nothing should fire (cancelled)
    let resp = apply_engine(
        &engine,
        &MqCommand::fire_pending_wills(&mut buf, 1003 + 61_000),
        6,
        1005,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_WILLS_FIRED,
        "expected WillsFired"
    );
    assert_eq!(
        resp.wills_fired_count(),
        0,
        "cancelled will should not fire"
    );
}

#[test]
fn test_opt4_fire_pending_wills_timing() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-q", 2, 1001);
    create_binding(&mut engine, exchange_id, queue_id, "clients/#", 3, 1002);

    create_session(&mut engine, 100, "timed-client", 4, 1003);

    let will_msg = make_flat_msg_with_routing_key(b"offline", "clients/status");
    set_will(
        &mut engine,
        100,
        exchange_id,
        10,
        "clients/status",
        &will_msg,
        5,
        1004,
    );

    apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        6,
        1005,
    );

    // Fire at 5 seconds after disconnect (before 10s delay)
    let resp = apply_engine(
        &engine,
        &MqCommand::fire_pending_wills(&mut buf, 1005 + 5_000),
        7,
        1006,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_WILLS_FIRED,
        "expected WillsFired"
    );
    assert_eq!(
        resp.wills_fired_count(),
        0,
        "will should not fire before delay expires"
    );

    // Fire at 11 seconds after disconnect (after 10s delay)
    let resp = apply_engine(
        &engine,
        &MqCommand::fire_pending_wills(&mut buf, 1005 + 11_000),
        8,
        1007,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_WILLS_FIRED,
        "expected WillsFired"
    );
    assert_eq!(
        resp.wills_fired_count(),
        1,
        "will should fire after delay expires"
    );
}

#[test]
fn test_opt4_pending_wills_survive_snapshot() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    // Create queue + binding before snapshot so the will can route after restore
    let queue_id = create_queue(&mut engine, "will-q", 2, 1001);
    create_binding(&mut engine, exchange_id, queue_id, "status", 3, 1002);

    create_session(&mut engine, 100, "snap-client", 4, 1003);

    // Embed routing key in flat message so publish_will_payload can route it
    let will_msg = make_flat_msg_with_routing_key(b"bye", "status");
    set_will(
        &mut engine,
        100,
        exchange_id,
        60,
        "status",
        &will_msg,
        5,
        1004,
    );

    apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        6,
        1005,
    );

    // Snapshot and restore
    let snap = engine.snapshot();
    assert!(
        !snap.pending_wills.is_empty(),
        "snapshot should contain pending wills"
    );
    assert_eq!(snap.pending_wills[0].client_id, "snap-client");

    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Fire pending wills — the restored will should fire via the restored exchange + binding
    let resp = apply_engine(
        &engine2,
        &MqCommand::fire_pending_wills(&mut buf, 1005 + 61_000),
        7,
        1006,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_WILLS_FIRED,
        "expected WillsFired"
    );
    assert_eq!(
        resp.wills_fired_count(),
        1,
        "pending will should survive snapshot/restore"
    );
}

// =============================================================================
// Opt 6: Shared Subscription No-Local Validation
// =============================================================================

#[test]
fn test_opt6_reject_no_local_on_shared_binding() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "shared-q", 2, 1001);

    // Create binding with no_local=true AND shared_group=Some("group1")
    // Should be rejected per MQTT 5.0 SS 3.8.3.1
    let resp = apply_engine(
        &engine,
        &MqCommand::create_binding_with_opts(
            &mut buf,
            exchange_id,
            queue_id,
            Some("sensors/#"),
            true,
            Some("group1"),
            None,
        ),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_ERROR,
        "expected Custom error"
    );
    let message = resp.error_message();
    assert!(
        message.contains("no_local") || message.contains("shared"),
        "error should mention no_local or shared: {}",
        message
    );
}

#[test]
fn test_opt6_allow_no_local_on_non_shared_binding() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "non-shared-q", 2, 1001);

    let resp = apply_engine(
        &engine,
        &MqCommand::create_binding_with_opts(
            &mut buf,
            exchange_id,
            queue_id,
            Some("sensors/#"),
            true,
            None,
            None,
        ),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "no_local without shared should succeed, got {:?}",
        resp.tag()
    );
}

#[test]
fn test_opt6_allow_shared_without_no_local() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "shared-ok-q", 2, 1001);

    let resp = apply_engine(
        &engine,
        &MqCommand::create_binding_with_opts(
            &mut buf,
            exchange_id,
            queue_id,
            Some("sensors/#"),
            false,
            Some("group1"),
            None,
        ),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "shared without no_local should succeed, got {:?}",
        resp.tag()
    );
}

// =============================================================================
// Opt 7: Subscription Metadata (group_id) in Delivery Response
// =============================================================================

#[test]
fn test_opt7_deliver_includes_group_id() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "delivery-q", 1, 1000);

    create_session(&mut engine, 100, "consumer-1", 2, 1001);

    enqueue_messages(&mut engine, source_topic_id(queue_id), 3, 3, 1002);

    let msgs = deliver_messages(&mut engine, queue_id, 100, 10, 6, 1005);
    assert_eq!(msgs.len(), 3);

    for msg in &msgs {
        assert_eq!(
            msg.group_id, queue_id,
            "delivered message should include group_id"
        );
    }
}

#[test]
fn test_opt7_group_id_codec_roundtrip() {
    let mut buf = BytesMut::new();

    use bisque_raft::codec::{Decode, Encode};
    use std::io::Cursor;

    let original = DeliveredMessage {
        message_id: 42,
        attempt: 3,
        original_timestamp: 99999,
        group_id: 12345,
    };

    let mut buf = Vec::new();
    original.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = DeliveredMessage::decode(&mut cursor).unwrap();

    assert_eq!(decoded.message_id, 42);
    assert_eq!(decoded.attempt, 3);
    assert_eq!(decoded.original_timestamp, 99999);
    assert_eq!(decoded.group_id, 12345);
}

// =============================================================================
// Codec tests for commands
// =============================================================================

#[test]
fn test_codec_fire_pending_wills() {
    let mut buf = bytes::BytesMut::new();
    let cmd = MqCommand::fire_pending_wills(&mut buf, 1234567890);
    assert_eq!(cmd.tag(), MqCommand::TAG_FIRE_PENDING_WILLS);
}

// =============================================================================
// Binding codec with opts
// =============================================================================

#[test]
fn test_create_binding_with_opts_codec() {
    let mut buf = bytes::BytesMut::new();
    let cmd = MqCommand::create_binding_with_opts(
        &mut buf,
        10,
        20,
        Some("sensors/+/temp"),
        true,
        Some("shared-group-1"),
        Some(42),
    );
    assert_eq!(cmd.tag(), MqCommand::TAG_CREATE_BINDING);

    let view = cmd.as_create_binding();
    assert_eq!(view.exchange_id(), 10);
    assert_eq!(view.topic_id(), 20);
    assert_eq!(view.routing_key(), Some("sensors/+/temp".to_string()));
    assert!(view.no_local(), "no_local should be true");
    assert_eq!(view.shared_group(), Some("shared-group-1".to_string()));
    assert_eq!(view.subscription_id(), Some(42));
}

#[test]
fn test_create_binding_backward_compat() {
    let mut buf = BytesMut::new();

    let cmd = MqCommand::create_binding(&mut buf, 10, 20, Some("topic/#"));
    let view = cmd.as_create_binding();
    assert!(
        !view.no_local(),
        "backward compat: no_local should be false"
    );
    assert!(
        view.shared_group().is_none(),
        "backward compat: shared_group should be None"
    );
}
