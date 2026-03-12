//! Integration tests for Phase 2 optimizations (mq-optimizations-2.md).
//!
//! Tests cover:
//!   Opt 1: Event-driven delivery wiring (QueueNotifier integration)
//!   Opt 3: Native topic alias support (tags 69-70)
//!   Opt 4: Will delay with cancellation on reconnect (tags 71-72)
//!   Opt 6: Shared subscription no-local validation
//!   Opt 7: Subscription metadata (queue_id) in delivery response
//!   Opt 8: Publisher session ID for dedup (tag 73)

use bytes::Bytes;

use bisque_mq::config::{MqConfig, QueueConfig};
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::*;

// =============================================================================
// Helpers
// =============================================================================

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-phase2-opt-test"))
}

fn make_flat_msg(value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(Bytes::from(value.to_vec()))
        .timestamp(1000)
        .build()
}

fn make_flat_msg_with_routing_key(value: &[u8], routing_key: &str) -> Bytes {
    FlatMessageBuilder::new(Bytes::from(value.to_vec()))
        .routing_key(Bytes::from(routing_key.to_owned()))
        .timestamp(1000)
        .build()
}

fn create_exchange(engine: &mut MqEngine, name: &str, log_index: u64, time: u64) -> u64 {
    match engine.apply_command(
        &MqCommand::create_exchange(name, ExchangeType::Topic),
        log_index,
        time,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

fn create_queue(engine: &mut MqEngine, name: &str, log_index: u64, time: u64) -> u64 {
    match engine.apply_command(
        &MqCommand::create_queue(name, &QueueConfig::default()),
        log_index,
        time,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

fn create_binding(
    engine: &mut MqEngine,
    exchange_id: u64,
    queue_id: u64,
    routing_key: &str,
    log_index: u64,
    time: u64,
) -> u64 {
    match engine.apply_command(
        &MqCommand::create_binding(exchange_id, queue_id, Some(routing_key)),
        log_index,
        time,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

fn register_consumer(
    engine: &mut MqEngine,
    consumer_id: u64,
    group_name: &str,
    subs: &[Subscription],
    log_index: u64,
    time: u64,
) {
    engine.apply_command(
        &MqCommand::register_consumer(consumer_id, group_name, subs),
        log_index,
        time,
    );
}

fn set_will(
    engine: &mut MqEngine,
    consumer_id: u64,
    exchange_id: u64,
    delay_secs: u32,
    routing_key: &str,
    message: &Bytes,
    log_index: u64,
    time: u64,
) {
    engine.apply_command(
        &MqCommand::set_will(
            consumer_id,
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

fn enqueue_messages(engine: &mut MqEngine, queue_id: u64, count: usize, log_index: u64, time: u64) {
    for i in 0..count {
        let msg = make_flat_msg(format!("msg-{}", i).as_bytes());
        engine.apply_command(
            &MqCommand::enqueue(queue_id, &[msg], &[None]),
            log_index + i as u64,
            time,
        );
    }
}

fn deliver_messages(
    engine: &mut MqEngine,
    queue_id: u64,
    consumer_id: u64,
    max_count: u32,
    log_index: u64,
    time: u64,
) -> Vec<DeliveredMessage> {
    match engine.apply_command(
        &MqCommand::deliver(queue_id, consumer_id, max_count),
        log_index,
        time,
    ) {
        MqResponse::Messages { messages } => messages.to_vec(),
        other => panic!("expected Messages, got {:?}", other),
    }
}

// =============================================================================
// Opt 1: Event-Driven Delivery Wiring (QueueNotifier)
// =============================================================================

#[test]
fn test_opt1_notifier_triggered_on_enqueue() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "notify-q", 1, 1000);

    let mut rx = engine.metadata().queue_notifier.watch(queue_id);

    let msg = make_flat_msg(b"hello");
    engine.apply_command(&MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 1001);

    assert!(
        rx.try_recv().is_ok(),
        "should receive notification after enqueue"
    );
}

#[test]
fn test_opt1_notifier_triggered_on_exchange_publish() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/ex", 1, 1000);
    let queue_id = create_queue(&mut engine, "sub-q", 2, 1001);
    create_binding(&mut engine, exchange_id, queue_id, "sensors/#", 3, 1002);

    let mut rx = engine.metadata().queue_notifier.watch(queue_id);

    let msg = make_flat_msg_with_routing_key(b"temp=22", "sensors/temp");
    engine.apply_command(
        &MqCommand::publish_to_exchange(exchange_id, &[msg]),
        4,
        1003,
    );

    assert!(
        rx.try_recv().is_ok(),
        "should receive notification after exchange publish"
    );
}

#[test]
fn test_opt1_notifier_triggered_on_nack() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "nack-q", 1, 1000);
    register_consumer(
        &mut engine,
        100,
        "consumer-1",
        &[Subscription {
            entity_type: EntityType::Queue,
            entity_id: queue_id,
        }],
        2,
        1001,
    );

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let msgs = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    assert_eq!(msgs.len(), 1);

    let mut rx = engine.metadata().queue_notifier.watch(queue_id);

    let msg_ids: Vec<u64> = msgs.iter().map(|m| m.message_id).collect();
    engine.apply_command(&MqCommand::nack(queue_id, &msg_ids), 5, 1004);

    assert!(
        rx.try_recv().is_ok(),
        "should receive notification after nack"
    );
}

#[test]
fn test_opt1_notifier_multiple_watchers() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "multi-watch-q", 1, 1000);

    let mut rx1 = engine.metadata().queue_notifier.watch(queue_id);
    let mut rx2 = engine.metadata().queue_notifier.watch(queue_id);

    let msg = make_flat_msg(b"hello");
    engine.apply_command(&MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 1001);

    assert!(
        rx1.try_recv().is_ok(),
        "watcher 1 should receive notification"
    );
    assert!(
        rx2.try_recv().is_ok(),
        "watcher 2 should receive notification"
    );
}

#[test]
fn test_opt1_notifier_no_spurious_for_other_queues() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "q1", 1, 1000);
    let q2 = create_queue(&mut engine, "q2", 2, 1001);

    let mut rx1 = engine.metadata().queue_notifier.watch(q1);
    let mut rx2 = engine.metadata().queue_notifier.watch(q2);

    let msg = make_flat_msg(b"only-q1");
    engine.apply_command(&MqCommand::enqueue(q1, &[msg], &[None]), 3, 1002);

    assert!(rx1.try_recv().is_ok(), "q1 watcher should get notification");
    assert!(
        rx2.try_recv().is_err(),
        "q2 watcher should NOT get notification"
    );
}

// =============================================================================
// Opt 3: Native Topic Alias Support (tags 69-70)
// =============================================================================

#[test]
fn test_opt3_set_topic_alias() {
    let mut engine = make_engine();

    let resp = engine.apply_command(&MqCommand::set_topic_alias(100, 1, "sensors/temp"), 1, 1000);
    assert!(
        matches!(resp, MqResponse::Ok),
        "set_topic_alias should return Ok, got {:?}",
        resp
    );
}

#[test]
fn test_opt3_set_topic_alias_overwrite() {
    let mut engine = make_engine();

    engine.apply_command(&MqCommand::set_topic_alias(100, 1, "sensors/temp"), 1, 1000);

    let resp = engine.apply_command(
        &MqCommand::set_topic_alias(100, 1, "sensors/humidity"),
        2,
        1001,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt3_set_multiple_aliases() {
    let mut engine = make_engine();

    engine.apply_command(&MqCommand::set_topic_alias(100, 1, "sensors/temp"), 1, 1000);
    engine.apply_command(
        &MqCommand::set_topic_alias(100, 2, "sensors/humidity"),
        2,
        1001,
    );
    engine.apply_command(
        &MqCommand::set_topic_alias(100, 3, "actuators/fan"),
        3,
        1002,
    );

    let resp = engine.apply_command(&MqCommand::clear_topic_aliases(100), 4, 1003);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt3_clear_aliases_different_consumers() {
    let mut engine = make_engine();

    engine.apply_command(&MqCommand::set_topic_alias(100, 1, "topic/a"), 1, 1000);
    engine.apply_command(&MqCommand::set_topic_alias(200, 1, "topic/b"), 2, 1001);

    // Clear only consumer 100's aliases
    engine.apply_command(&MqCommand::clear_topic_aliases(100), 3, 1002);

    // Consumer 200's aliases should still be there
    let resp = engine.apply_command(&MqCommand::clear_topic_aliases(200), 4, 1003);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt3_codec_set_topic_alias_roundtrip() {
    let cmd = MqCommand::set_topic_alias(42, 7, "devices/thermostat");
    assert_eq!(cmd.tag(), MqCommand::TAG_SET_TOPIC_ALIAS);

    let view = cmd.as_set_topic_alias();
    assert_eq!(view.consumer_id(), 42);
    assert_eq!(view.alias(), 7);
    assert_eq!(view.topic_name(), "devices/thermostat");
}

// =============================================================================
// Opt 4: Will Delay with Cancellation on Reconnect (tags 71-72)
// =============================================================================

#[test]
fn test_opt4_will_delay_stores_pending() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Register consumer
    register_consumer(&mut engine, 100, "client-abc", &[], 2, 1001);

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
    let resp = engine.apply_command(&MqCommand::disconnect_consumer(100), 4, 1003);
    match &resp {
        MqResponse::WillPending { delay_secs, .. } => {
            assert_eq!(*delay_secs, 30);
        }
        other => panic!("expected WillPending, got {:?}", other),
    }
}

#[test]
fn test_opt4_will_no_delay_fires_immediately() {
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

    register_consumer(&mut engine, 100, "client-xyz", &[], 4, 1003);

    // Set will with delay_secs = 0
    let will_msg = make_flat_msg(b"offline");
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
    let resp = engine.apply_command(&MqCommand::disconnect_consumer(100), 6, 1005);
    assert!(
        matches!(resp, MqResponse::Ok),
        "immediate will disconnect should return Ok, got {:?}",
        resp
    );

    // The will message should be in the target queue
    register_consumer(
        &mut engine,
        200,
        "reader",
        &[Subscription {
            entity_type: EntityType::Queue,
            entity_id: queue_id,
        }],
        7,
        1006,
    );
    let msgs = deliver_messages(&mut engine, queue_id, 200, 10, 8, 1007);
    assert!(
        !msgs.is_empty(),
        "will message should have been routed to queue"
    );
}

#[test]
fn test_opt4_cancel_pending_will_on_reconnect() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    register_consumer(&mut engine, 100, "client-reconnect", &[], 2, 1001);

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
    engine.apply_command(&MqCommand::disconnect_consumer(100), 4, 1003);

    // Reconnect with same group_name — should cancel pending will
    register_consumer(&mut engine, 200, "client-reconnect", &[], 5, 1004);

    // Try to fire pending wills — nothing should fire (cancelled)
    let resp = engine.apply_command(&MqCommand::fire_pending_wills(1003 + 61_000), 6, 1005);
    match &resp {
        MqResponse::WillsFired { count } => {
            assert_eq!(*count, 0, "cancelled will should not fire");
        }
        other => panic!("expected WillsFired, got {:?}", other),
    }
}

#[test]
fn test_opt4_cancel_pending_will_explicit() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    register_consumer(&mut engine, 100, "client-cancel", &[], 2, 1001);

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

    engine.apply_command(&MqCommand::disconnect_consumer(100), 4, 1003);

    // Explicit cancel
    let resp = engine.apply_command(&MqCommand::cancel_pending_will("client-cancel"), 5, 1004);
    assert!(
        matches!(resp, MqResponse::Ok),
        "cancel should return Ok, got {:?}",
        resp
    );
}

#[test]
fn test_opt4_fire_pending_wills_timing() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-q", 2, 1001);
    create_binding(&mut engine, exchange_id, queue_id, "clients/#", 3, 1002);

    register_consumer(&mut engine, 100, "timed-client", &[], 4, 1003);

    let will_msg = make_flat_msg(b"offline");
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

    engine.apply_command(&MqCommand::disconnect_consumer(100), 6, 1005);

    // Fire at 5 seconds after disconnect (before 10s delay)
    let resp = engine.apply_command(&MqCommand::fire_pending_wills(1005 + 5_000), 7, 1006);
    match &resp {
        MqResponse::WillsFired { count } => {
            assert_eq!(*count, 0, "will should not fire before delay expires");
        }
        other => panic!("expected WillsFired, got {:?}", other),
    }

    // Fire at 11 seconds after disconnect (after 10s delay)
    let resp = engine.apply_command(&MqCommand::fire_pending_wills(1005 + 11_000), 8, 1007);
    match &resp {
        MqResponse::WillsFired { count } => {
            assert_eq!(*count, 1, "will should fire after delay expires");
        }
        other => panic!("expected WillsFired, got {:?}", other),
    }
}

#[test]
fn test_opt4_pending_wills_survive_snapshot() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    register_consumer(&mut engine, 100, "snap-client", &[], 2, 1001);

    let will_msg = make_flat_msg(b"bye");
    set_will(
        &mut engine,
        100,
        exchange_id,
        60,
        "status",
        &will_msg,
        3,
        1002,
    );

    engine.apply_command(&MqCommand::disconnect_consumer(100), 4, 1003);

    // Snapshot and restore
    let snap = engine.snapshot();
    assert!(
        !snap.pending_wills.is_empty(),
        "snapshot should contain pending wills"
    );
    assert_eq!(snap.pending_wills[0].client_id, "snap-client");

    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Create exchange/queue in restored engine for will routing
    let exchange_id2 = create_exchange(&mut engine2, "mqtt/exchange2", 5, 1004);
    let queue_id2 = create_queue(&mut engine2, "will-q2", 6, 1005);
    create_binding(&mut engine2, exchange_id2, queue_id2, "status", 7, 1006);

    // Fire pending wills — the restored will should fire
    let resp = engine2.apply_command(&MqCommand::fire_pending_wills(1003 + 61_000), 8, 1007);
    match &resp {
        MqResponse::WillsFired { count } => {
            assert_eq!(*count, 1, "pending will should survive snapshot/restore");
        }
        other => panic!("expected WillsFired, got {:?}", other),
    }
}

// =============================================================================
// Opt 6: Shared Subscription No-Local Validation
// =============================================================================

#[test]
fn test_opt6_reject_no_local_on_shared_binding() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "shared-q", 2, 1001);

    // Create binding with no_local=true AND shared_group=Some("group1")
    // Should be rejected per MQTT 5.0 SS 3.8.3.1
    let resp = engine.apply_command(
        &MqCommand::create_binding_with_opts(
            exchange_id,
            queue_id,
            Some("sensors/#"),
            true,           // no_local
            Some("group1"), // shared_group
            None,           // subscription_id
        ),
        3,
        1002,
    );
    match &resp {
        MqResponse::Error(MqError::Custom(message)) => {
            assert!(
                message.contains("no_local") || message.contains("shared"),
                "error should mention no_local or shared: {}",
                message
            );
        }
        other => panic!("expected Custom error, got {:?}", other),
    }
}

#[test]
fn test_opt6_allow_no_local_on_non_shared_binding() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "non-shared-q", 2, 1001);

    let resp = engine.apply_command(
        &MqCommand::create_binding_with_opts(
            exchange_id,
            queue_id,
            Some("sensors/#"),
            true, // no_local
            None, // no shared_group
            None, // subscription_id
        ),
        3,
        1002,
    );
    assert!(
        matches!(resp, MqResponse::EntityCreated { .. }),
        "no_local without shared should succeed, got {:?}",
        resp
    );
}

#[test]
fn test_opt6_allow_shared_without_no_local() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "shared-ok-q", 2, 1001);

    let resp = engine.apply_command(
        &MqCommand::create_binding_with_opts(
            exchange_id,
            queue_id,
            Some("sensors/#"),
            false,          // no_local = false
            Some("group1"), // shared_group
            None,           // subscription_id
        ),
        3,
        1002,
    );
    assert!(
        matches!(resp, MqResponse::EntityCreated { .. }),
        "shared without no_local should succeed, got {:?}",
        resp
    );
}

// =============================================================================
// Opt 7: Subscription Metadata (queue_id) in Delivery Response
// =============================================================================

#[test]
fn test_opt7_deliver_includes_queue_id() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "delivery-q", 1, 1000);

    register_consumer(
        &mut engine,
        100,
        "consumer-1",
        &[Subscription {
            entity_type: EntityType::Queue,
            entity_id: queue_id,
        }],
        2,
        1001,
    );

    enqueue_messages(&mut engine, queue_id, 3, 3, 1002);

    let msgs = deliver_messages(&mut engine, queue_id, 100, 10, 6, 1005);
    assert_eq!(msgs.len(), 3);

    for msg in &msgs {
        assert_eq!(
            msg.queue_id, queue_id,
            "delivered message should include queue_id"
        );
    }
}

#[test]
fn test_opt7_queue_id_codec_roundtrip() {
    use bisque_raft::codec::{Decode, Encode};
    use std::io::Cursor;

    let original = DeliveredMessage {
        message_id: 42,
        attempt: 3,
        original_timestamp: 99999,
        queue_id: 12345,
    };

    let mut buf = Vec::new();
    original.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = DeliveredMessage::decode(&mut cursor).unwrap();

    assert_eq!(decoded.message_id, 42);
    assert_eq!(decoded.attempt, 3);
    assert_eq!(decoded.original_timestamp, 99999);
    assert_eq!(decoded.queue_id, 12345);
}

// =============================================================================
// Opt 8: Publisher Session ID for Dedup (tag 73)
// =============================================================================

#[test]
fn test_opt8_register_publisher_session() {
    let mut engine = make_engine();

    let resp = engine.apply_command(
        &MqCommand::register_publisher_session(100, "session-abc"),
        1,
        1000,
    );
    assert!(
        matches!(resp, MqResponse::Ok),
        "register_publisher_session should return Ok, got {:?}",
        resp
    );
}

#[test]
fn test_opt8_register_publisher_session_codec_roundtrip() {
    let cmd = MqCommand::register_publisher_session(42, "my-session-xyz");
    assert_eq!(cmd.tag(), MqCommand::TAG_REGISTER_PUBLISHER_SESSION);

    let view = cmd.as_register_publisher_session();
    assert_eq!(view.consumer_id(), 42);
    assert_eq!(view.session_id(), "my-session-xyz");
}

#[test]
fn test_opt8_cancel_pending_will_codec_roundtrip() {
    let cmd = MqCommand::cancel_pending_will("client-123");
    assert_eq!(cmd.tag(), MqCommand::TAG_CANCEL_PENDING_WILL);

    let view = cmd.as_cancel_pending_will();
    assert_eq!(view.client_id(), "client-123");
}

// =============================================================================
// Codec tests for new commands (tags 69-73)
// =============================================================================

#[test]
fn test_codec_fire_pending_wills() {
    let cmd = MqCommand::fire_pending_wills(1234567890);
    assert_eq!(cmd.tag(), MqCommand::TAG_FIRE_PENDING_WILLS);
}

#[test]
fn test_codec_tag_constants() {
    assert_eq!(MqCommand::TAG_SET_TOPIC_ALIAS, 69);
    assert_eq!(MqCommand::TAG_CLEAR_TOPIC_ALIASES, 70);
    assert_eq!(MqCommand::TAG_CANCEL_PENDING_WILL, 71);
    assert_eq!(MqCommand::TAG_FIRE_PENDING_WILLS, 72);
    assert_eq!(MqCommand::TAG_REGISTER_PUBLISHER_SESSION, 73);
}

// =============================================================================
// Binding codec with opts
// =============================================================================

#[test]
fn test_create_binding_with_opts_codec() {
    let cmd = MqCommand::create_binding_with_opts(
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
    assert_eq!(view.queue_id(), 20);
    assert_eq!(view.routing_key(), Some("sensors/+/temp".to_string()));
    assert!(view.no_local(), "no_local should be true");
    assert_eq!(view.shared_group(), Some("shared-group-1".to_string()));
    assert_eq!(view.subscription_id(), Some(42));
}

#[test]
fn test_create_binding_backward_compat() {
    let cmd = MqCommand::create_binding(10, 20, Some("topic/#"));
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

// =============================================================================
// MqResponse codec for new variants
// =============================================================================

#[test]
fn test_response_wills_fired_codec() {
    use bisque_raft::codec::{Decode, Encode};
    use std::io::Cursor;

    let resp = MqResponse::WillsFired { count: 5 };
    let mut buf = Vec::new();
    resp.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = MqResponse::decode(&mut cursor).unwrap();
    match decoded {
        MqResponse::WillsFired { count } => assert_eq!(count, 5),
        other => panic!("expected WillsFired, got {:?}", other),
    }
}

#[test]
fn test_response_topic_aliases_codec() {
    use bisque_raft::codec::{Decode, Encode};
    use std::io::Cursor;

    let aliases = vec![
        TopicAliasEntry {
            alias: 1,
            topic_name: "sensors/temp".to_string(),
        },
        TopicAliasEntry {
            alias: 2,
            topic_name: "sensors/humidity".to_string(),
        },
    ];
    let resp = MqResponse::TopicAliases { aliases };
    let mut buf = Vec::new();
    resp.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = MqResponse::decode(&mut cursor).unwrap();
    match decoded {
        MqResponse::TopicAliases {
            aliases: decoded_aliases,
        } => {
            assert_eq!(decoded_aliases.len(), 2);
            assert_eq!(decoded_aliases[0].alias, 1);
            assert_eq!(decoded_aliases[0].topic_name, "sensors/temp");
            assert_eq!(decoded_aliases[1].alias, 2);
            assert_eq!(decoded_aliases[1].topic_name, "sensors/humidity");
        }
        other => panic!("expected TopicAliases, got {:?}", other),
    }
}
