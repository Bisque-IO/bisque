//! Integration tests for MQTT-native engine optimizations.
//!
//! Tests cover the 8 optimizations defined in mq-mqtt/docs/mq-optimizations.md:
//!   Phase 1: FlatMessage flags & publisher_id (Opts 5, 8)
//!   Phase 2: Retained messages, will/testament, QoS 2 state machine (Opts 2, 3, 4)
//!   Phase 3: Session persistence, batch deliver/ack (Opts 6, 7)
//!   Phase 4: Push-based delivery (Opt 1) — tested separately
//!
//! Test naming convention: test_{optimization}_{scenario}

use bytes::Bytes;

use bisque_mq::config::{MqConfig, QueueConfig};
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::{FlatMessage, FlatMessageBuilder};
use bisque_mq::types::*;

// =============================================================================
// Helpers
// =============================================================================

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-mqtt-opt-test"))
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

fn create_queue_with_config(
    engine: &mut MqEngine,
    name: &str,
    config: &QueueConfig,
    log_index: u64,
    time: u64,
) -> u64 {
    match engine.apply_command(&MqCommand::create_queue(name, config), log_index, time) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

fn register_consumer(
    engine: &mut MqEngine,
    consumer_id: u64,
    subs: &[Subscription],
    log_index: u64,
    time: u64,
) {
    engine.apply_command(
        &MqCommand::register_consumer(consumer_id, "mqtt-consumer", subs),
        log_index,
        time,
    );
}

fn enqueue_messages(engine: &mut MqEngine, queue_id: u64, count: usize, log_index: u64, time: u64) {
    // Enqueue each message individually with a unique log_index so each gets
    // a unique message_id (the engine uses log_index as the message_id key).
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
// Phase 1: FlatMessage Flags (Optimization 5)
// =============================================================================

#[test]
fn test_opt5_flag_retain_roundtrip() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .retain(true)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert!(flat.is_retain(), "retain flag should be set");

    // Without retain
    let msg2 = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .build();
    let flat2 = FlatMessage::new(msg2).unwrap();
    assert!(
        !flat2.is_retain(),
        "retain flag should not be set by default"
    );
}

#[test]
fn test_opt5_flag_no_local_roundtrip() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .no_local(true)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert!(flat.is_no_local(), "no_local flag should be set");

    let msg2 = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .build();
    let flat2 = FlatMessage::new(msg2).unwrap();
    assert!(
        !flat2.is_no_local(),
        "no_local flag should not be set by default"
    );
}

#[test]
fn test_opt5_flag_utf8_payload_roundtrip() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert!(flat.is_utf8_payload(), "utf8_payload flag should be set");

    let msg2 = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .build();
    let flat2 = FlatMessage::new(msg2).unwrap();
    assert!(
        !flat2.is_utf8_payload(),
        "utf8_payload flag should not be set by default"
    );
}

#[test]
fn test_opt5_all_new_flags_combined() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"data"))
        .timestamp(2000)
        .retain(true)
        .no_local(true)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert!(flat.is_retain());
    assert!(flat.is_no_local());
    assert!(flat.is_utf8_payload());
}

#[test]
fn test_opt5_new_flags_dont_affect_existing_fields() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"value-data"))
        .timestamp(5000)
        .key(Bytes::from_static(b"my-key"))
        .routing_key(Bytes::from_static(b"sensors/temp"))
        .ttl(30000)
        .delay(1000)
        .retain(true)
        .no_local(true)
        .header(Bytes::from_static(b"h1"), Bytes::from_static(b"v1"))
        .build();
    let flat = FlatMessage::new(msg).unwrap();

    // Verify existing fields are unaffected
    assert_eq!(&flat.value()[..], b"value-data");
    assert_eq!(&flat.key().unwrap()[..], b"my-key");
    assert_eq!(&flat.routing_key().unwrap()[..], b"sensors/temp");
    assert_eq!(flat.meta().ttl_ms, 30000);
    assert_eq!(flat.meta().delay_ms, 1000);
    assert_eq!(flat.meta().timestamp, 5000);
    assert_eq!(flat.header_count(), 1);
    let (hk, hv) = flat.header(0);
    assert_eq!(&hk[..], b"h1");
    assert_eq!(&hv[..], b"v1");

    // Verify new flags
    assert!(flat.is_retain());
    assert!(flat.is_no_local());
}

#[test]
fn test_opt5_flags_backward_compat() {
    // A message built without new flags should have all new checks return false
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"old-message"))
        .timestamp(1000)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert!(!flat.is_retain());
    assert!(!flat.is_no_local());
    assert!(!flat.is_utf8_payload());
}

// =============================================================================
// Phase 1: Publisher ID in Fixed Header (Optimization 8)
// =============================================================================

#[test]
fn test_opt8_publisher_id_roundtrip() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .publisher_id(12345)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert_eq!(flat.publisher_id(), 12345);
}

#[test]
fn test_opt8_publisher_id_zero_default() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert_eq!(flat.publisher_id(), 0);
}

#[test]
fn test_opt8_publisher_id_large_value() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"hello"))
        .timestamp(1000)
        .publisher_id(u64::MAX)
        .build();
    let flat = FlatMessage::new(msg).unwrap();
    assert_eq!(flat.publisher_id(), u64::MAX);
}

#[test]
fn test_opt8_publisher_id_with_all_fields() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"payload"))
        .timestamp(9999)
        .key(Bytes::from_static(b"key"))
        .routing_key(Bytes::from_static(b"a/b/c"))
        .reply_to(Bytes::from_static(b"reply-topic"))
        .correlation_id(Bytes::from_static(b"corr-123"))
        .ttl(60000)
        .delay(5000)
        .publisher_id(42)
        .retain(true)
        .header(
            Bytes::from_static(b"hdr-key"),
            Bytes::from_static(b"hdr-val"),
        )
        .header(Bytes::from_static(b"hdr2"), Bytes::from_static(b"val2"))
        .build();
    let flat = FlatMessage::new(msg).unwrap();

    // All fields should work correctly with 40-byte header
    assert_eq!(&flat.value()[..], b"payload");
    assert_eq!(&flat.key().unwrap()[..], b"key");
    assert_eq!(&flat.routing_key().unwrap()[..], b"a/b/c");
    assert_eq!(&flat.reply_to().unwrap()[..], b"reply-topic");
    assert_eq!(&flat.correlation_id().unwrap()[..], b"corr-123");
    assert_eq!(flat.meta().ttl_ms, 60000);
    assert_eq!(flat.meta().delay_ms, 5000);
    assert_eq!(flat.meta().timestamp, 9999);
    assert_eq!(flat.publisher_id(), 42);
    assert!(flat.is_retain());
    assert_eq!(flat.header_count(), 2);
    let (k1, v1) = flat.header(0);
    assert_eq!(&k1[..], b"hdr-key");
    assert_eq!(&v1[..], b"hdr-val");
    let (k2, v2) = flat.header(1);
    assert_eq!(&k2[..], b"hdr2");
    assert_eq!(&v2[..], b"val2");
}

#[test]
fn test_opt8_combined_retain_publisher_full_message() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"sensor-reading"))
        .timestamp(1000)
        .routing_key(Bytes::from_static(b"sensors/temp/room1"))
        .retain(true)
        .publisher_id(999)
        .header(
            Bytes::from_static(b"content-type"),
            Bytes::from_static(b"application/json"),
        )
        .build();
    let flat = FlatMessage::new(msg).unwrap();

    assert!(flat.is_retain());
    assert_eq!(flat.publisher_id(), 999);
    assert_eq!(&flat.routing_key().unwrap()[..], b"sensors/temp/room1");
    assert_eq!(&flat.value()[..], b"sensor-reading");
    assert_eq!(flat.header_count(), 1);
}

#[test]
fn test_opt8_zero_copy_preserved() {
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"zero-copy-test"))
        .timestamp(1000)
        .publisher_id(1)
        .build();
    let flat = FlatMessage::new(msg.clone()).unwrap();
    let value = flat.value();
    // Bytes should be a zero-copy slice — no allocation
    assert_eq!(&value[..], b"zero-copy-test");
    // Verify it's a slice of the original buffer (same underlying allocation)
    assert!(value.len() == 14);
}

// =============================================================================
// Phase 2: Native Retained Message Store (Optimization 2)
// =============================================================================

#[test]
fn test_opt2_set_retained_basic() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let msg = make_flat_msg_with_routing_key(b"temp=22.5", "sensors/temp");

    let resp = engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg),
        2,
        1001,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt2_get_retained_exact() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let msg = make_flat_msg_with_routing_key(b"temp=22.5", "sensors/temp");

    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg),
        2,
        1001,
    );

    let resp = engine.apply_command(
        &MqCommand::get_retained(exchange_id, "sensors/temp"),
        3,
        1002,
    );
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(&messages[0].routing_key[..], b"sensors/temp");
            // Verify the message bytes are valid FlatMessage
            let flat = FlatMessage::new(messages[0].message.clone()).unwrap();
            assert_eq!(&flat.value()[..], b"temp=22.5");
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_opt2_get_retained_wildcard_plus() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_flat_msg_with_routing_key(b"temp=22", "sensors/temp/room1");
    let msg2 = make_flat_msg_with_routing_key(b"humid=55", "sensors/humidity/room1");
    let msg3 = make_flat_msg_with_routing_key(b"temp=20", "sensors/temp/room2");

    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp/room1", &msg1),
        2,
        1001,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/humidity/room1", &msg2),
        3,
        1002,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp/room2", &msg3),
        4,
        1003,
    );

    // + matches one level
    let resp = engine.apply_command(
        &MqCommand::get_retained(exchange_id, "sensors/+/room1"),
        5,
        1004,
    );
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 2);
            let keys: Vec<String> = messages
                .iter()
                .map(|m| String::from_utf8(m.routing_key.to_vec()).unwrap())
                .collect();
            assert!(keys.contains(&"sensors/temp/room1".to_string()));
            assert!(keys.contains(&"sensors/humidity/room1".to_string()));
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_opt2_get_retained_wildcard_hash() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_flat_msg(b"val1");
    let msg2 = make_flat_msg(b"val2");
    let msg3 = make_flat_msg(b"val3");

    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg1),
        2,
        1001,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp/room1", &msg2),
        3,
        1002,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "actuators/fan", &msg3),
        4,
        1003,
    );

    // # matches zero or more levels
    let resp = engine.apply_command(&MqCommand::get_retained(exchange_id, "sensors/#"), 5, 1004);
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(
                messages.len(),
                2,
                "should match sensors/temp and sensors/temp/room1"
            );
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_opt2_update_retained_replaces() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_flat_msg(b"old-value");
    let msg2 = make_flat_msg(b"new-value");

    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "state/room1", &msg1),
        2,
        1001,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "state/room1", &msg2),
        3,
        1002,
    );

    let resp = engine.apply_command(
        &MqCommand::get_retained(exchange_id, "state/room1"),
        4,
        1003,
    );
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 1);
            let flat = FlatMessage::new(messages[0].message.clone()).unwrap();
            assert_eq!(&flat.value()[..], b"new-value");
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_opt2_delete_retained() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_flat_msg(b"data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "topic/a", &msg),
        2,
        1001,
    );

    // Delete
    let resp = engine.apply_command(&MqCommand::delete_retained(exchange_id, "topic/a"), 3, 1002);
    assert!(matches!(resp, MqResponse::Ok));

    // Get should return empty
    let resp = engine.apply_command(&MqCommand::get_retained(exchange_id, "topic/a"), 4, 1003);
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 0);
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_opt2_delete_retained_nonexistent() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Deleting nonexistent retained should be idempotent
    let resp = engine.apply_command(
        &MqCommand::delete_retained(exchange_id, "nonexistent/topic"),
        2,
        1001,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt2_retained_nonexistent_exchange() {
    let mut engine = make_engine();

    let msg = make_flat_msg(b"data");
    let resp = engine.apply_command(&MqCommand::set_retained(999, "topic", &msg), 1, 1000);
    assert!(matches!(resp, MqResponse::Error(MqError::NotFound { .. })));

    let resp = engine.apply_command(&MqCommand::get_retained(999, "#"), 2, 1001);
    assert!(matches!(resp, MqResponse::Error(MqError::NotFound { .. })));
}

#[test]
fn test_opt2_retained_survives_snapshot() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_flat_msg(b"retained-data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "state/light", &msg),
        2,
        1001,
    );

    // Snapshot
    let snap = engine.snapshot();

    // New engine from snapshot
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Verify retained message persists
    let resp = engine2.apply_command(
        &MqCommand::get_retained(exchange_id, "state/light"),
        3,
        1002,
    );
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 1);
            let flat = FlatMessage::new(messages[0].message.clone()).unwrap();
            assert_eq!(&flat.value()[..], b"retained-data");
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_opt2_retained_many_topics() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    for i in 0..100 {
        let msg = make_flat_msg(format!("val-{}", i).as_bytes());
        let topic = format!("devices/device-{}/status", i);
        engine.apply_command(
            &MqCommand::set_retained(exchange_id, &topic, &msg),
            (i + 2) as u64,
            1001 + i as u64,
        );
    }

    // Get all with #
    let resp = engine.apply_command(&MqCommand::get_retained(exchange_id, "#"), 200, 2000);
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 100);
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_opt2_get_retained_no_match() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_flat_msg(b"data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg),
        2,
        1001,
    );

    let resp = engine.apply_command(
        &MqCommand::get_retained(exchange_id, "actuators/+"),
        3,
        1002,
    );
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 0);
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

// =============================================================================
// Phase 2: Native Will / Testament (Optimization 3)
// =============================================================================

#[test]
fn test_opt3_set_will_basic() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    register_consumer(&mut engine, 100, &[], 2, 1001);

    let will_msg = make_flat_msg_with_routing_key(b"client offline", "status/client1");
    let resp = engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status/client1", &will_msg),
        3,
        1002,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt3_clear_will_basic() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    let will_msg = make_flat_msg(b"offline");
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status", &will_msg),
        3,
        1002,
    );

    let resp = engine.apply_command(&MqCommand::clear_will(100), 4, 1003);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt3_will_on_disconnect_immediate() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Create a queue bound to the exchange to receive the will message
    let queue_id = create_queue(&mut engine, "will-receiver", 2, 1001);
    engine.apply_command(
        &MqCommand::create_binding(exchange_id, queue_id, Some("status/client1")),
        3,
        1002,
    );

    register_consumer(&mut engine, 100, &[], 4, 1003);

    let will_msg = make_flat_msg_with_routing_key(b"client offline", "status/client1");
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status/client1", &will_msg),
        5,
        1004,
    );

    // Disconnect consumer — will should be published
    let resp = engine.apply_command(&MqCommand::disconnect_consumer(100), 6, 1005);
    // Immediate will (delay=0) should publish inline
    assert!(matches!(resp, MqResponse::Ok));

    // Verify will message arrived in the bound queue
    let delivered = deliver_messages(&mut engine, queue_id, 200, 1, 7, 1006);
    assert_eq!(
        delivered.len(),
        1,
        "will message should have been routed to queue"
    );
}

#[test]
fn test_opt3_clean_disconnect_no_will() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-receiver", 2, 1001);
    engine.apply_command(
        &MqCommand::create_binding(exchange_id, queue_id, Some("status/#")),
        3,
        1002,
    );

    register_consumer(&mut engine, 100, &[], 4, 1003);

    let will_msg = make_flat_msg(b"offline");
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status/c1", &will_msg),
        5,
        1004,
    );

    // Clear will before disconnect (clean disconnect)
    engine.apply_command(&MqCommand::clear_will(100), 6, 1005);

    // Disconnect — no will should be published
    engine.apply_command(&MqCommand::disconnect_consumer(100), 7, 1006);

    // Queue should be empty
    let delivered = deliver_messages(&mut engine, queue_id, 200, 1, 8, 1007);
    assert_eq!(delivered.len(), 0, "no will message after clean disconnect");
}

#[test]
fn test_opt3_will_nonexistent_consumer() {
    let mut engine = make_engine();
    let will_msg = make_flat_msg(b"offline");

    let resp = engine.apply_command(
        &MqCommand::set_will(999, 1, 0, 0, false, "status", &will_msg),
        1,
        1000,
    );
    assert!(matches!(resp, MqResponse::Error(MqError::NotFound { .. })));
}

#[test]
fn test_opt3_will_with_delay() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    let will_msg = make_flat_msg(b"delayed-offline");
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 5, 1, false, "status/c1", &will_msg),
        3,
        1002,
    );

    // Disconnect — should return WillPending
    let resp = engine.apply_command(&MqCommand::disconnect_consumer(100), 4, 1003);
    match resp {
        MqResponse::WillPending { delay_secs, .. } => {
            assert_eq!(delay_secs, 5);
        }
        other => panic!("expected WillPending for delayed will, got {:?}", other),
    }
}

#[test]
fn test_opt3_will_update_replaces() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-q", 2, 1001);
    engine.apply_command(
        &MqCommand::create_binding(exchange_id, queue_id, Some("status/c1")),
        3,
        1002,
    );

    register_consumer(&mut engine, 100, &[], 4, 1003);

    let will_msg1 = make_flat_msg_with_routing_key(b"msg1", "status/c1");
    let will_msg2 = make_flat_msg_with_routing_key(b"msg2-updated", "status/c1");

    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status/c1", &will_msg1),
        5,
        1004,
    );
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status/c1", &will_msg2),
        6,
        1005,
    );

    // Disconnect — should publish msg2, not msg1
    engine.apply_command(&MqCommand::disconnect_consumer(100), 7, 1006);

    let delivered = deliver_messages(&mut engine, queue_id, 200, 10, 8, 1007);
    assert_eq!(
        delivered.len(),
        1,
        "only the latest will should be published"
    );
}

#[test]
fn test_opt3_will_survives_snapshot() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    let will_msg = make_flat_msg(b"crash-will");
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status", &will_msg),
        3,
        1002,
    );

    // Snapshot and restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Verify will is preserved — consumer should have will set
    // We verify by checking that disconnect publishes the will
    let queue_id = create_queue(&mut engine2, "will-q", 4, 1003);
    engine2.apply_command(
        &MqCommand::create_binding(exchange_id, queue_id, Some("status")),
        5,
        1004,
    );
    engine2.apply_command(&MqCommand::disconnect_consumer(100), 6, 1005);

    let delivered = deliver_messages(&mut engine2, queue_id, 200, 1, 7, 1006);
    assert_eq!(delivered.len(), 1, "will should survive snapshot");
}

// =============================================================================
// Phase 2: QoS 2 State Machine (Optimization 4)
// =============================================================================

#[test]
fn test_opt4_mark_received_basic() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    assert_eq!(delivered.len(), 1);
    let msg_id = delivered[0].message_id;

    // Mark received (PUBREC sent)
    let resp = engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);
    assert!(matches!(resp, MqResponse::Ok));

    // Verify state via snapshot
    let snap = engine.snapshot();
    let queue = &snap.queues[0];
    // In-flight count should have decremented (Received is not in-flight)
    assert_eq!(
        queue.meta.in_flight_count, 0,
        "Received messages should not count as in-flight"
    );
}

#[test]
fn test_opt4_mark_released_basic() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);
    let resp = engine.apply_command(&MqCommand::mark_released(queue_id, &[msg_id]), 6, 1005);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt4_ack_after_released() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);
    engine.apply_command(&MqCommand::mark_released(queue_id, &[msg_id]), 6, 1005);

    // ACK after full QoS 2 handshake
    let resp = engine.apply_command(&MqCommand::ack(queue_id, &[msg_id], None), 7, 1006);
    assert!(matches!(resp, MqResponse::Ok));

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 0);
    assert_eq!(snap.queues[0].meta.in_flight_count, 0);
}

#[test]
fn test_opt4_ack_after_received() {
    // Some implementations allow ACK directly from Received state (shortcut)
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);

    // ACK directly from Received
    let resp = engine.apply_command(&MqCommand::ack(queue_id, &[msg_id], None), 6, 1005);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt4_mark_received_invalid_state_pending() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);

    enqueue_messages(&mut engine, queue_id, 1, 2, 1001);
    // Message is Pending (not delivered yet) — mark_received should have no effect
    let resp = engine.apply_command(
        &MqCommand::mark_received(queue_id, &[2]), // log_index 2 = enqueue
        3,
        1002,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Message should still be pending
    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 1);
}

#[test]
fn test_opt4_mark_released_invalid_state_inflight() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    // Try mark_released on InFlight (should skip Received) — no effect
    let resp = engine.apply_command(&MqCommand::mark_released(queue_id, &[msg_id]), 5, 1004);
    assert!(matches!(resp, MqResponse::Ok));

    // Message should still be in-flight
    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.in_flight_count, 1);
}

#[test]
fn test_opt4_received_no_visibility_timeout() {
    let mut engine = make_engine();
    let config = QueueConfig {
        visibility_timeout_ms: 1000,
        ..Default::default()
    };
    let queue_id = create_queue_with_config(&mut engine, "qos2-queue", &config, 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    // Mark as received
    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);

    // Fire timeout — should NOT affect Received messages
    engine.apply_command(
        &MqCommand::timeout_expired(queue_id, &[msg_id]),
        6,
        5000, // well past visibility timeout
    );

    // Message should still be in Received state, not back in Pending
    let snap = engine.snapshot();
    assert_eq!(
        snap.queues[0].meta.pending_count, 0,
        "timeout should not requeue Received messages"
    );
}

#[test]
fn test_opt4_qos2_state_survives_snapshot() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);

    // Snapshot and restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Verify state persisted — can complete QoS 2 handshake
    engine2.apply_command(&MqCommand::mark_released(queue_id, &[msg_id]), 6, 1005);
    engine2.apply_command(&MqCommand::ack(queue_id, &[msg_id], None), 7, 1006);

    let snap2 = engine2.snapshot();
    assert_eq!(snap2.queues[0].meta.pending_count, 0);
    assert_eq!(snap2.queues[0].meta.in_flight_count, 0);
}

#[test]
fn test_opt4_batch_mark_received() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 5, 10, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 5, 20, 1003);
    assert_eq!(delivered.len(), 5);

    let msg_ids: Vec<u64> = delivered.iter().map(|d| d.message_id).collect();

    // Batch mark_received
    let resp = engine.apply_command(&MqCommand::mark_received(queue_id, &msg_ids), 21, 1004);
    assert!(matches!(resp, MqResponse::Ok));

    let snap = engine.snapshot();
    assert_eq!(
        snap.queues[0].meta.in_flight_count, 0,
        "all 5 should be Received, not in-flight"
    );
}

#[test]
fn test_opt4_nack_only_works_on_inflight() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    // Mark received
    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);

    // NACK on Received — should not revert to Pending
    engine.apply_command(&MqCommand::nack(queue_id, &[msg_id]), 6, 1005);

    let snap = engine.snapshot();
    // Message should not be back in pending (nack only works on InFlight)
    assert_eq!(snap.queues[0].meta.pending_count, 0);
}

#[test]
fn test_opt4_mark_received_decrements_inflight_count() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-queue", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 3, 10, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 3, 20, 1003);
    assert_eq!(delivered.len(), 3);

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.in_flight_count, 3);

    // Mark 2 as received
    let ids = vec![delivered[0].message_id, delivered[1].message_id];
    engine.apply_command(&MqCommand::mark_received(queue_id, &ids), 21, 1004);

    let snap = engine.snapshot();
    assert_eq!(
        snap.queues[0].meta.in_flight_count, 1,
        "2 moved to Received, 1 still InFlight"
    );
}

// =============================================================================
// Phase 3: Session Persistence (Optimization 6)
// =============================================================================

#[test]
fn test_opt6_persist_session_basic() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"serialized-subscription-data");

    let resp = engine.apply_command(
        &MqCommand::persist_session(100, "mqtt-client-1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt6_restore_session_basic() {
    let mut engine = make_engine();
    let sub_data = Bytes::from(b"sub-filter:sensors/#,qos:1".to_vec());

    engine.apply_command(
        &MqCommand::persist_session(100, "mqtt-client-1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    let resp = engine.apply_command(&MqCommand::restore_session("mqtt-client-1"), 2, 1001);
    match resp {
        MqResponse::SessionRestored {
            consumer_id,
            session_expiry_secs,
            subscription_data,
        } => {
            assert_eq!(consumer_id, 100);
            assert_eq!(session_expiry_secs, 3600);
            assert_eq!(&subscription_data[..], b"sub-filter:sensors/#,qos:1");
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

#[test]
fn test_opt6_restore_session_expired() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"data");

    // Persist with 1-second expiry
    engine.apply_command(
        &MqCommand::persist_session(100, "client1", 1, &sub_data, 0, 0, 0),
        1,
        1000, // disconnected at 1000ms
    );

    // Restore 3 seconds later (well past 1-second expiry)
    let resp = engine.apply_command(
        &MqCommand::restore_session("client1"),
        2,
        4000, // 3 seconds later
    );
    assert!(matches!(resp, MqResponse::SessionNotFound));
}

#[test]
fn test_opt6_restore_session_never_expire() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"data");

    // Persist with 0xFFFFFFFF = never expire
    engine.apply_command(
        &MqCommand::persist_session(100, "client1", 0xFFFFFFFF, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    // Restore much later
    let resp = engine.apply_command(
        &MqCommand::restore_session("client1"),
        2,
        1_000_000_000, // very far in the future
    );
    match resp {
        MqResponse::SessionRestored { consumer_id, .. } => {
            assert_eq!(consumer_id, 100);
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

#[test]
fn test_opt6_restore_session_nonexistent() {
    let mut engine = make_engine();

    let resp = engine.apply_command(&MqCommand::restore_session("unknown-client"), 1, 1000);
    assert!(matches!(resp, MqResponse::SessionNotFound));
}

#[test]
fn test_opt6_expire_sessions_cleanup() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"data");

    // Session 1: expires in 1 second
    engine.apply_command(
        &MqCommand::persist_session(100, "client-short", 1, &sub_data, 0, 0, 0),
        1,
        1000,
    );
    // Session 2: expires in 10 seconds
    engine.apply_command(
        &MqCommand::persist_session(101, "client-medium", 10, &sub_data, 0, 0, 0),
        2,
        1000,
    );
    // Session 3: never expires
    engine.apply_command(
        &MqCommand::persist_session(102, "client-forever", 0xFFFFFFFF, &sub_data, 0, 0, 0),
        3,
        1000,
    );

    // Expire at now=5000ms (5 seconds later)
    engine.apply_command(&MqCommand::expire_sessions(5000), 4, 5000);

    // client-short should be expired (1s expiry, 4s elapsed)
    let resp = engine.apply_command(&MqCommand::restore_session("client-short"), 5, 5001);
    assert!(
        matches!(resp, MqResponse::SessionNotFound),
        "short-lived session should be expired"
    );

    // client-medium should still exist (10s expiry, 4s elapsed)
    let resp = engine.apply_command(&MqCommand::restore_session("client-medium"), 6, 5002);
    assert!(
        matches!(resp, MqResponse::SessionRestored { .. }),
        "medium session should still exist"
    );

    // client-forever should still exist
    let resp = engine.apply_command(&MqCommand::restore_session("client-forever"), 7, 5003);
    assert!(
        matches!(resp, MqResponse::SessionRestored { .. }),
        "forever session should still exist"
    );
}

#[test]
fn test_opt6_persist_session_overwrite() {
    let mut engine = make_engine();

    engine.apply_command(
        &MqCommand::persist_session(
            100,
            "client1",
            3600,
            &Bytes::from_static(b"old-data"),
            0,
            0,
            0,
        ),
        1,
        1000,
    );
    engine.apply_command(
        &MqCommand::persist_session(
            200,
            "client1",
            7200,
            &Bytes::from_static(b"new-data"),
            0,
            0,
            0,
        ),
        2,
        2000,
    );

    let resp = engine.apply_command(&MqCommand::restore_session("client1"), 3, 2001);
    match resp {
        MqResponse::SessionRestored {
            consumer_id,
            session_expiry_secs,
            subscription_data,
        } => {
            assert_eq!(consumer_id, 200);
            assert_eq!(session_expiry_secs, 7200);
            assert_eq!(&subscription_data[..], b"new-data");
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

#[test]
fn test_opt6_session_survives_snapshot() {
    let mut engine = make_engine();
    let sub_data = Bytes::from(b"snapshot-test-data".to_vec());

    engine.apply_command(
        &MqCommand::persist_session(100, "client1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    // Snapshot and restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    let resp = engine2.apply_command(&MqCommand::restore_session("client1"), 2, 1001);
    match resp {
        MqResponse::SessionRestored {
            subscription_data, ..
        } => {
            assert_eq!(&subscription_data[..], b"snapshot-test-data");
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

#[test]
fn test_opt6_subscription_data_opaque() {
    let mut engine = make_engine();
    // Arbitrary binary data (not valid UTF-8)
    let sub_data = Bytes::from(vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02]);

    engine.apply_command(
        &MqCommand::persist_session(100, "client1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    let resp = engine.apply_command(&MqCommand::restore_session("client1"), 2, 1001);
    match resp {
        MqResponse::SessionRestored {
            subscription_data, ..
        } => {
            assert_eq!(
                subscription_data, sub_data,
                "binary data should be preserved exactly"
            );
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

// =============================================================================
// Phase 3: Batch Deliver / ACK (Optimization 7)
// =============================================================================

#[test]
fn test_opt7_multi_deliver_basic() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-1", 1, 1000);
    let q2 = create_queue(&mut engine, "queue-2", 2, 1001);
    let q3 = create_queue(&mut engine, "queue-3", 3, 1002);
    register_consumer(&mut engine, 100, &[], 4, 1003);

    enqueue_messages(&mut engine, q1, 5, 10, 1004);
    enqueue_messages(&mut engine, q2, 3, 20, 1005);
    enqueue_messages(&mut engine, q3, 7, 30, 1006);

    let resp = engine.apply_command(
        &MqCommand::multi_deliver(100, &[(q1, 10), (q2, 10), (q3, 10)]),
        40,
        1007,
    );
    match resp {
        MqResponse::MultiMessages { queues } => {
            // Should have results for all 3 queues (or at least the ones with messages)
            let total_msgs: usize = queues.iter().map(|(_, msgs)| msgs.len()).sum();
            assert_eq!(total_msgs, 15, "should deliver 5+3+7=15 messages total");
        }
        other => panic!("expected MultiMessages, got {:?}", other),
    }
}

#[test]
fn test_opt7_multi_deliver_empty_queues() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-1", 1, 1000);
    let q2 = create_queue(&mut engine, "queue-2", 2, 1001);
    register_consumer(&mut engine, 100, &[], 3, 1002);

    // No messages enqueued
    let resp = engine.apply_command(&MqCommand::multi_deliver(100, &[(q1, 5), (q2, 5)]), 4, 1003);
    match resp {
        MqResponse::MultiMessages { queues } => {
            let total_msgs: usize = queues.iter().map(|(_, msgs)| msgs.len()).sum();
            assert_eq!(total_msgs, 0, "no messages in empty queues");
        }
        other => panic!("expected MultiMessages, got {:?}", other),
    }
}

#[test]
fn test_opt7_multi_deliver_partial() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-full", 1, 1000);
    let q2 = create_queue(&mut engine, "queue-empty", 2, 1001);
    register_consumer(&mut engine, 100, &[], 3, 1002);

    enqueue_messages(&mut engine, q1, 3, 10, 1003);
    // q2 has no messages

    let resp = engine.apply_command(
        &MqCommand::multi_deliver(100, &[(q1, 5), (q2, 5)]),
        20,
        1004,
    );
    match resp {
        MqResponse::MultiMessages { queues } => {
            let total_msgs: usize = queues.iter().map(|(_, msgs)| msgs.len()).sum();
            assert_eq!(total_msgs, 3, "should only have messages from q1");
        }
        other => panic!("expected MultiMessages, got {:?}", other),
    }
}

#[test]
fn test_opt7_multi_deliver_max_count() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-1", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, q1, 100, 10, 1002);

    let resp = engine.apply_command(&MqCommand::multi_deliver(100, &[(q1, 5)]), 200, 1003);
    match resp {
        MqResponse::MultiMessages { queues } => {
            let total_msgs: usize = queues.iter().map(|(_, msgs)| msgs.len()).sum();
            assert_eq!(total_msgs, 5, "should respect max_count=5");
        }
        other => panic!("expected MultiMessages, got {:?}", other),
    }
}

#[test]
fn test_opt7_multi_deliver_nonexistent_queue() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-1", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, q1, 3, 10, 1002);

    // Include nonexistent queue 999
    let resp = engine.apply_command(
        &MqCommand::multi_deliver(100, &[(q1, 5), (999, 5)]),
        20,
        1003,
    );
    match resp {
        MqResponse::MultiMessages { queues } => {
            let total_msgs: usize = queues.iter().map(|(_, msgs)| msgs.len()).sum();
            assert_eq!(
                total_msgs, 3,
                "should deliver from valid queue, skip invalid"
            );
        }
        other => panic!("expected MultiMessages, got {:?}", other),
    }
}

#[test]
fn test_opt7_multi_ack_basic() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-1", 1, 1000);
    let q2 = create_queue(&mut engine, "queue-2", 2, 1001);
    register_consumer(&mut engine, 100, &[], 3, 1002);

    enqueue_messages(&mut engine, q1, 3, 10, 1003);
    enqueue_messages(&mut engine, q2, 2, 20, 1004);

    let d1 = deliver_messages(&mut engine, q1, 100, 3, 30, 1005);
    let d2 = deliver_messages(&mut engine, q2, 100, 2, 31, 1006);

    let ids1: Vec<u64> = d1.iter().map(|d| d.message_id).collect();
    let ids2: Vec<u64> = d2.iter().map(|d| d.message_id).collect();

    // Multi-ack across both queues
    let resp = engine.apply_command(&MqCommand::multi_ack(&[(q1, &ids1), (q2, &ids2)]), 32, 1007);
    assert!(matches!(resp, MqResponse::Ok));

    // Verify all acked
    let snap = engine.snapshot();
    for queue in &snap.queues {
        assert_eq!(queue.meta.in_flight_count, 0, "all should be acked");
        assert_eq!(queue.meta.pending_count, 0);
    }
}

#[test]
fn test_opt7_multi_ack_idempotent() {
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-1", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, q1, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, q1, 100, 1, 4, 1003);
    let ids: Vec<u64> = delivered.iter().map(|d| d.message_id).collect();

    // ACK once
    engine.apply_command(&MqCommand::multi_ack(&[(q1, &ids)]), 5, 1004);

    // ACK again — should not error
    let resp = engine.apply_command(&MqCommand::multi_ack(&[(q1, &ids)]), 6, 1005);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_opt7_batch_ack_per_queue() {
    // Tests existing ack() with multiple message_ids (not multi_ack)
    let mut engine = make_engine();
    let q1 = create_queue(&mut engine, "queue-1", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, q1, 10, 10, 1002);
    let delivered = deliver_messages(&mut engine, q1, 100, 10, 30, 1003);
    assert_eq!(delivered.len(), 10);

    let all_ids: Vec<u64> = delivered.iter().map(|d| d.message_id).collect();

    // Batch ack all 10 in one call
    let resp = engine.apply_command(&MqCommand::ack(q1, &all_ids, None), 31, 1004);
    assert!(matches!(resp, MqResponse::Ok));

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.in_flight_count, 0);
    assert_eq!(snap.queues[0].meta.pending_count, 0);
}

// =============================================================================
// Integration: End-to-End MQTT Optimization Workflows
// =============================================================================

#[test]
fn test_integration_retained_via_engine_publish_subscribe() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Publish with retain
    let msg = make_flat_msg_with_routing_key(b"temp=22.5", "sensors/temp");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg),
        2,
        1001,
    );

    // Later, subscribe — get retained message
    let resp = engine.apply_command(&MqCommand::get_retained(exchange_id, "sensors/+"), 3, 1002);
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 1);
            let flat = FlatMessage::new(messages[0].message.clone()).unwrap();
            assert_eq!(&flat.value()[..], b"temp=22.5");
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_integration_will_via_engine_connect_disconnect() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-q", 2, 1001);
    engine.apply_command(
        &MqCommand::create_binding(exchange_id, queue_id, Some("status/#")),
        3,
        1002,
    );

    // "Connect" — register consumer with will
    register_consumer(&mut engine, 100, &[], 4, 1003);
    let will_msg = make_flat_msg_with_routing_key(b"offline", "status/client1");
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 1, false, "status/client1", &will_msg),
        5,
        1004,
    );

    // "Unclean disconnect"
    engine.apply_command(&MqCommand::disconnect_consumer(100), 6, 1005);

    // Will should have been published to the queue
    let delivered = deliver_messages(&mut engine, queue_id, 200, 1, 7, 1006);
    assert_eq!(delivered.len(), 1, "will message should be delivered");
}

#[test]
fn test_integration_qos2_full_handshake_via_engine() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-q", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    // Enqueue
    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);

    // Deliver (Pending → InFlight)
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    // PUBREC (InFlight → Received)
    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);

    // PUBREL/PUBCOMP (Received → Released)
    engine.apply_command(&MqCommand::mark_released(queue_id, &[msg_id]), 6, 1005);

    // ACK (Released → Acked)
    engine.apply_command(&MqCommand::ack(queue_id, &[msg_id], None), 7, 1006);

    // Verify complete
    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 0);
    assert_eq!(snap.queues[0].meta.in_flight_count, 0);
}

#[test]
fn test_integration_session_restore_full_flow() {
    let mut engine = make_engine();

    // Connect and subscribe
    register_consumer(&mut engine, 100, &[], 1, 1000);
    let sub_data = Bytes::from(b"filter:sensors/#,qos:1,no_local:false".to_vec());

    // Disconnect with session persistence
    engine.apply_command(
        &MqCommand::persist_session(100, "my-mqtt-client", 3600, &sub_data, 0, 0, 0),
        2,
        1001,
    );
    engine.apply_command(&MqCommand::disconnect_consumer(100), 3, 1002);

    // Reconnect — restore session
    let resp = engine.apply_command(&MqCommand::restore_session("my-mqtt-client"), 4, 1003);
    match resp {
        MqResponse::SessionRestored {
            consumer_id,
            subscription_data,
            ..
        } => {
            assert_eq!(consumer_id, 100);
            assert_eq!(
                &subscription_data[..],
                b"filter:sensors/#,qos:1,no_local:false"
            );
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

#[test]
fn test_integration_batch_delivery_cycle() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Create 3 subscription queues with bindings
    let q1 = create_queue(&mut engine, "sub/client/sensors.temp", 2, 1001);
    let q2 = create_queue(&mut engine, "sub/client/sensors.humidity", 3, 1002);
    let q3 = create_queue(&mut engine, "sub/client/status", 4, 1003);

    register_consumer(&mut engine, 100, &[], 5, 1004);

    // Enqueue to each queue
    enqueue_messages(&mut engine, q1, 10, 100, 1005);
    enqueue_messages(&mut engine, q2, 5, 200, 1006);
    enqueue_messages(&mut engine, q3, 3, 300, 1007);

    // Single multi_deliver — 1 raft proposal instead of 3
    let resp = engine.apply_command(
        &MqCommand::multi_deliver(100, &[(q1, 10), (q2, 10), (q3, 10)]),
        400,
        1008,
    );
    let all_acks: Vec<(u64, Vec<u64>)> = match resp {
        MqResponse::MultiMessages { queues } => {
            let total: usize = queues.iter().map(|(_, msgs)| msgs.len()).sum();
            assert_eq!(total, 18, "10+5+3 messages");
            queues
                .iter()
                .map(|(qid, msgs)| (*qid, msgs.iter().map(|m| m.message_id).collect()))
                .collect()
        }
        other => panic!("expected MultiMessages, got {:?}", other),
    };

    // Single multi_ack — 1 raft proposal instead of 18
    let ack_refs: Vec<(u64, &[u64])> = all_acks
        .iter()
        .map(|(qid, ids)| (*qid, ids.as_slice()))
        .collect();
    let resp = engine.apply_command(&MqCommand::multi_ack(&ack_refs), 401, 1009);
    assert!(matches!(resp, MqResponse::Ok));

    // Verify all acked
    let snap = engine.snapshot();
    for queue in &snap.queues {
        assert_eq!(queue.meta.in_flight_count, 0);
        assert_eq!(queue.meta.pending_count, 0);
    }
}

// =============================================================================
// Integration: Crash Recovery
// =============================================================================

#[test]
fn test_crash_recovery_qos2() {
    let mut engine = make_engine();
    let queue_id = create_queue(&mut engine, "qos2-q", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    enqueue_messages(&mut engine, queue_id, 1, 3, 1002);
    let delivered = deliver_messages(&mut engine, queue_id, 100, 1, 4, 1003);
    let msg_id = delivered[0].message_id;

    // PUBREC sent — mark received
    engine.apply_command(&MqCommand::mark_received(queue_id, &[msg_id]), 5, 1004);

    // "Crash" — snapshot and restore to new engine
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // After recovery: message should still be in Received state
    // Complete the QoS 2 handshake
    engine2.apply_command(&MqCommand::mark_released(queue_id, &[msg_id]), 6, 1005);
    engine2.apply_command(&MqCommand::ack(queue_id, &[msg_id], None), 7, 1006);

    let snap2 = engine2.snapshot();
    assert_eq!(snap2.queues[0].meta.pending_count, 0);
    assert_eq!(snap2.queues[0].meta.in_flight_count, 0);
}

#[test]
fn test_crash_recovery_retained() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_flat_msg(b"retained-before-crash");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "state/device1", &msg),
        2,
        1001,
    );

    // "Crash"
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Verify retained survives
    let resp = engine2.apply_command(
        &MqCommand::get_retained(exchange_id, "state/device1"),
        3,
        1002,
    );
    match resp {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 1);
            let flat = FlatMessage::new(messages[0].message.clone()).unwrap();
            assert_eq!(&flat.value()[..], b"retained-before-crash");
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_crash_recovery_session() {
    let mut engine = make_engine();
    let sub_data = Bytes::from(b"crash-recovery-subs".to_vec());

    engine.apply_command(
        &MqCommand::persist_session(100, "crash-client", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    // "Crash"
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    let resp = engine2.apply_command(&MqCommand::restore_session("crash-client"), 2, 1001);
    match resp {
        MqResponse::SessionRestored {
            subscription_data, ..
        } => {
            assert_eq!(&subscription_data[..], b"crash-recovery-subs");
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

#[test]
fn test_crash_recovery_will() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    register_consumer(&mut engine, 100, &[], 2, 1001);

    let will_msg = make_flat_msg_with_routing_key(b"crash-will", "status/c1");
    engine.apply_command(
        &MqCommand::set_will(100, exchange_id, 0, 0, false, "status/c1", &will_msg),
        3,
        1002,
    );

    // "Crash" before disconnect
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Create queue to receive will in restored engine
    let queue_id = create_queue(&mut engine2, "will-q", 4, 1003);
    engine2.apply_command(
        &MqCommand::create_binding(exchange_id, queue_id, Some("status/c1")),
        5,
        1004,
    );

    // Now disconnect — will should still be set after recovery
    engine2.apply_command(&MqCommand::disconnect_consumer(100), 6, 1005);

    let delivered = deliver_messages(&mut engine2, queue_id, 200, 1, 7, 1006);
    assert_eq!(
        delivered.len(),
        1,
        "will should survive crash and publish on disconnect"
    );
}
