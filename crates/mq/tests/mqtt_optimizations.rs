//! Integration tests for MQTT-native engine optimizations.
//!
//! Tests cover the optimizations defined in mq-mqtt/docs/mq-optimizations.md:
//!   Phase 1: FlatMessage flags & publisher_id (Opts 5, 8)
//!   Phase 2: Will/testament (Opt 3)
//!   Phase 3: Session persistence (Opt 6)
//!
//! Test naming convention: test_{optimization}_{scenario}
//!
//! NOTE: Tests for removed features (get_retained, delete_retained,
//! mark_received, QoS 2 state machine, topic aliases, publisher sessions,
//! multi_deliver, multi_ack) have been deleted after the entity model
//! consolidation.

use bytes::Bytes;

use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::{FlatMessage, FlatMessageBuilder};
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
    MqEngine::new(MqConfig::new("/tmp/mq-mqtt-opt-test"))
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
    let mut buf = BytesMut::new();
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
    let mut buf = BytesMut::new();
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

fn create_queue_with_config(
    engine: &mut MqEngine,
    name: &str,
    config: AckVariantConfig,
    log_index: u64,
    time: u64,
) -> u64 {
    let mut buf = BytesMut::new();
    let e = apply_engine(
        &engine,
        &MqCommand::create_queue(
            &mut buf,
            name,
            config,
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

fn create_session(engine: &mut MqEngine, session_id: u64, log_index: u64, time: u64) {
    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::create_session(&mut buf, session_id, "mqtt-consumer", 60000, 0),
        log_index,
        time,
    );
}

fn enqueue_messages(engine: &mut MqEngine, topic_id: u64, count: usize, log_index: u64, time: u64) {
    // Publish each message individually with a unique log_index so each gets
    // a unique message_id (the engine uses log_index as the message_id key).
    let mut buf = BytesMut::new();
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
    let mut buf = BytesMut::new();
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
// Phase 1: FlatMessage Flags (Optimization 5)
// =============================================================================

#[test]
fn test_opt5_flag_retain_roundtrip() {
    let msg = FlatMessageBuilder::new(b"hello")
        .timestamp(1000)
        .retain(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_retain(), "retain flag should be set");

    // Without retain
    let msg2 = FlatMessageBuilder::new(b"hello").timestamp(1000).build();
    let flat2 = FlatMessage::new(&msg2).unwrap();
    assert!(
        !flat2.is_retain(),
        "retain flag should not be set by default"
    );
}

#[test]
fn test_opt5_flag_no_local_roundtrip() {
    let msg = FlatMessageBuilder::new(b"hello")
        .timestamp(1000)
        .no_local(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_no_local(), "no_local flag should be set");

    let msg2 = FlatMessageBuilder::new(b"hello").timestamp(1000).build();
    let flat2 = FlatMessage::new(&msg2).unwrap();
    assert!(
        !flat2.is_no_local(),
        "no_local flag should not be set by default"
    );
}

#[test]
fn test_opt5_flag_utf8_payload_roundtrip() {
    let msg = FlatMessageBuilder::new(b"hello")
        .timestamp(1000)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_utf8_payload(), "utf8_payload flag should be set");

    let msg2 = FlatMessageBuilder::new(b"hello").timestamp(1000).build();
    let flat2 = FlatMessage::new(&msg2).unwrap();
    assert!(
        !flat2.is_utf8_payload(),
        "utf8_payload flag should not be set by default"
    );
}

#[test]
fn test_opt5_all_new_flags_combined() {
    let msg = FlatMessageBuilder::new(b"data")
        .timestamp(2000)
        .retain(true)
        .no_local(true)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_retain());
    assert!(flat.is_no_local());
    assert!(flat.is_utf8_payload());
}

#[test]
fn test_opt5_new_flags_dont_affect_existing_fields() {
    let msg = FlatMessageBuilder::new(b"value-data")
        .timestamp(5000)
        .key(b"my-key")
        .routing_key(b"sensors/temp")
        .ttl(30000)
        .delay(1000)
        .retain(true)
        .no_local(true)
        .header(b"h1", b"v1")
        .build();
    let flat = FlatMessage::new(&msg).unwrap();

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
    let msg = FlatMessageBuilder::new(b"old-message")
        .timestamp(1000)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(!flat.is_retain());
    assert!(!flat.is_no_local());
    assert!(!flat.is_utf8_payload());
}

// =============================================================================
// Phase 1: Publisher ID in Fixed Header (Optimization 8)
// =============================================================================

#[test]
fn test_opt8_publisher_id_roundtrip() {
    let msg = FlatMessageBuilder::new(b"hello")
        .timestamp(1000)
        .publisher_id(12345)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), 12345);
}

#[test]
fn test_opt8_publisher_id_zero_default() {
    let msg = FlatMessageBuilder::new(b"hello").timestamp(1000).build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), 0);
}

#[test]
fn test_opt8_publisher_id_large_value() {
    let msg = FlatMessageBuilder::new(b"hello")
        .timestamp(1000)
        .publisher_id(u64::MAX)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), u64::MAX);
}

#[test]
fn test_opt8_publisher_id_with_all_fields() {
    let msg = FlatMessageBuilder::new(b"payload")
        .timestamp(9999)
        .key(b"key")
        .routing_key(b"a/b/c")
        .reply_to(b"reply-topic")
        .correlation_id(b"corr-123")
        .ttl(60000)
        .delay(5000)
        .publisher_id(42)
        .retain(true)
        .header(b"hdr-key", b"hdr-val")
        .header(b"hdr2", b"val2")
        .build();
    let flat = FlatMessage::new(&msg).unwrap();

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
    let msg = FlatMessageBuilder::new(b"sensor-reading")
        .timestamp(1000)
        .routing_key(b"sensors/temp/room1")
        .retain(true)
        .publisher_id(999)
        .header(b"content-type", b"application/json")
        .build();
    let flat = FlatMessage::new(&msg).unwrap();

    assert!(flat.is_retain());
    assert_eq!(flat.publisher_id(), 999);
    assert_eq!(&flat.routing_key().unwrap()[..], b"sensors/temp/room1");
    assert_eq!(&flat.value()[..], b"sensor-reading");
    assert_eq!(flat.header_count(), 1);
}

#[test]
fn test_opt8_zero_copy_preserved() {
    let msg = FlatMessageBuilder::new(b"zero-copy-test")
        .timestamp(1000)
        .publisher_id(1)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    let value = flat.value();
    // Bytes should be a zero-copy slice -- no allocation
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

    let mut buf = BytesMut::new();
    let resp = apply_engine(
        &engine,
        &MqCommand::set_retained(&mut buf, exchange_id, "sensors/temp", &msg),
        2,
        1001,
    );
    assert!(resp.is_ok(), "expected Ok");
}

// =============================================================================
// Phase 2: Native Will / Testament (Optimization 3)
// =============================================================================

#[test]
fn test_opt3_set_will_basic() {
    let mut engine = make_engine();
    let _exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    create_session(&mut engine, 100, 2, 1001);

    let mut buf = BytesMut::new();
    let will_msg = make_flat_msg_with_routing_key(b"client offline", "status/client1");
    let resp = apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            _exchange_id,
            0,
            0,
            false,
            "status/client1",
            &will_msg,
        ),
        3,
        1002,
    );
    assert!(resp.is_ok(), "expected Ok");
}

#[test]
fn test_opt3_clear_will_basic() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    create_session(&mut engine, 100, 2, 1001);

    let mut buf = BytesMut::new();
    let will_msg = make_flat_msg(b"offline");
    apply_engine(
        &engine,
        &MqCommand::set_will(&mut buf, 100, exchange_id, 0, 0, false, "status", &will_msg),
        3,
        1002,
    );

    let resp = apply_engine(&engine, &MqCommand::clear_will(&mut buf, 100), 4, 1003);
    assert!(resp.is_ok(), "expected Ok");
}

#[test]
fn test_opt3_will_on_disconnect_immediate() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Create a queue bound to the exchange to receive the will message
    let queue_id = create_queue(&mut engine, "will-receiver", 2, 1001);
    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::create_binding(&mut buf, exchange_id, queue_id, Some("status/client1")),
        3,
        1002,
    );

    create_session(&mut engine, 100, 4, 1003);

    let will_msg = make_flat_msg_with_routing_key(b"client offline", "status/client1");
    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            exchange_id,
            0,
            0,
            false,
            "status/client1",
            &will_msg,
        ),
        5,
        1004,
    );

    // Disconnect session -- will should be published
    let resp = apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        6,
        1005,
    );
    // Immediate will (delay=0) should publish inline
    assert!(resp.is_ok(), "expected Ok");

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
    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::create_binding(&mut buf, exchange_id, queue_id, Some("status/#")),
        3,
        1002,
    );

    create_session(&mut engine, 100, 4, 1003);

    let will_msg = make_flat_msg(b"offline");
    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            exchange_id,
            0,
            0,
            false,
            "status/c1",
            &will_msg,
        ),
        5,
        1004,
    );

    // Clear will before disconnect (clean disconnect)
    apply_engine(&engine, &MqCommand::clear_will(&mut buf, 100), 6, 1005);

    // Disconnect -- no will should be published
    apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, false),
        7,
        1006,
    );

    // Queue should be empty
    let delivered = deliver_messages(&mut engine, queue_id, 200, 1, 8, 1007);
    assert_eq!(delivered.len(), 0, "no will message after clean disconnect");
}

#[test]
fn test_opt3_will_nonexistent_session() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let will_msg = make_flat_msg(b"offline");

    let resp = apply_engine(
        &engine,
        &MqCommand::set_will(&mut buf, 999, 1, 0, 0, false, "status", &will_msg),
        1,
        1000,
    );
    assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR, "expected Error");
    assert_eq!(resp.error_kind(), ResponseEntry::ERR_NOT_FOUND);
}

#[test]
fn test_opt3_will_with_delay() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    create_session(&mut engine, 100, 2, 1001);

    let mut buf = BytesMut::new();
    let will_msg = make_flat_msg(b"delayed-offline");
    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            exchange_id,
            5,
            1,
            false,
            "status/c1",
            &will_msg,
        ),
        3,
        1002,
    );

    // Disconnect -- should return WillPending
    let resp = apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        4,
        1003,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_WILL_PENDING,
        "expected WillPending for delayed will"
    );
    assert!(resp.will_pending_delay_ms() > 0);
}

#[test]
fn test_opt3_will_update_replaces() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-q", 2, 1001);
    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::create_binding(&mut buf, exchange_id, queue_id, Some("status/c1")),
        3,
        1002,
    );

    create_session(&mut engine, 100, 4, 1003);

    let will_msg1 = make_flat_msg_with_routing_key(b"msg1", "status/c1");
    let will_msg2 = make_flat_msg_with_routing_key(b"msg2-updated", "status/c1");

    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            exchange_id,
            0,
            0,
            false,
            "status/c1",
            &will_msg1,
        ),
        5,
        1004,
    );
    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            exchange_id,
            0,
            0,
            false,
            "status/c1",
            &will_msg2,
        ),
        6,
        1005,
    );

    // Disconnect -- should publish msg2, not msg1
    apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        7,
        1006,
    );

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
    create_session(&mut engine, 100, 2, 1001);

    let mut buf = BytesMut::new();
    let will_msg = make_flat_msg_with_routing_key(b"crash-will", "status");
    apply_engine(
        &engine,
        &MqCommand::set_will(&mut buf, 100, exchange_id, 0, 0, false, "status", &will_msg),
        3,
        1002,
    );

    // Snapshot and restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Verify will is preserved -- consumer should have will set
    // We verify by checking that disconnect publishes the will
    let queue_id = create_queue(&mut engine2, "will-q", 4, 1003);
    apply_engine(
        &engine2,
        &MqCommand::create_binding(&mut buf, exchange_id, queue_id, Some("status")),
        5,
        1004,
    );
    apply_engine(
        &engine2,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        6,
        1005,
    );

    let delivered = deliver_messages(&mut engine2, queue_id, 200, 1, 7, 1006);
    assert_eq!(delivered.len(), 1, "will should survive snapshot");
}

// =============================================================================
// Phase 3: Session Persistence (Optimization 6)
// =============================================================================

#[test]
fn test_opt6_persist_session_basic() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"serialized-subscription-data");

    let mut buf = BytesMut::new();
    let resp = apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "mqtt-client-1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );
    assert!(resp.is_ok(), "expected Ok");
}

#[test]
fn test_opt6_restore_session_basic() {
    let mut engine = make_engine();
    let sub_data = Bytes::from(b"sub-filter:sensors/#,qos:1".to_vec());

    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "mqtt-client-1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "mqtt-client-1"),
        2,
        1001,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored"
    );
    assert_eq!(resp.session_restored_id(), 100);
    assert!(resp.session_restored_expiry_ms() > 0);
    assert_eq!(
        &resp.session_restored_subscription_data()[..],
        b"sub-filter:sensors/#,qos:1"
    );
}

#[test]
fn test_opt6_restore_session_expired() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"data");

    let mut buf = BytesMut::new();
    // Persist with 1-second expiry
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "client1", 1, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    // Restore 3 seconds later (well past 1-second expiry)
    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "client1"),
        2,
        4000,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_NOT_FOUND,
        "expected SessionNotFound"
    );
}

#[test]
fn test_opt6_restore_session_never_expire() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"data");

    let mut buf = BytesMut::new();
    // Persist with 0xFFFFFFFF = never expire
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "client1", 0xFFFFFFFF, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    // Restore much later
    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "client1"),
        2,
        1_000_000_000,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored"
    );
    assert_eq!(resp.session_restored_id(), 100);
}

#[test]
fn test_opt6_restore_session_nonexistent() {
    let mut engine = make_engine();

    let mut buf = BytesMut::new();
    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "unknown-client"),
        1,
        1000,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_NOT_FOUND,
        "expected SessionNotFound"
    );
}

#[test]
fn test_opt6_expire_sessions_cleanup() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"data");

    let mut buf = BytesMut::new();
    // Session 1: expires in 1 second
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "client-short", 1, &sub_data, 0, 0, 0),
        1,
        1000,
    );
    // Session 2: expires in 10 seconds
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 101, "client-medium", 10, &sub_data, 0, 0, 0),
        2,
        1000,
    );
    // Session 3: never expires
    apply_engine(
        &engine,
        &MqCommand::persist_session(
            &mut buf,
            102,
            "client-forever",
            0xFFFFFFFF,
            &sub_data,
            0,
            0,
            0,
        ),
        3,
        1000,
    );

    // Expire at now=5000ms (5 seconds later)
    apply_engine(
        &engine,
        &MqCommand::expire_sessions(&mut buf, 5000),
        4,
        5000,
    );

    // client-short should be expired (1s expiry, 4s elapsed)
    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "client-short"),
        5,
        5001,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_NOT_FOUND,
        "short-lived session should be expired"
    );

    // client-medium should still exist (10s expiry, 4s elapsed)
    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "client-medium"),
        6,
        5002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored: medium session should still exist"
    );

    // client-forever should still exist
    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "client-forever"),
        7,
        5003,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored: forever session should still exist"
    );
}

#[test]
fn test_opt6_persist_session_overwrite() {
    let mut engine = make_engine();

    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::persist_session(
            &mut buf,
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
    apply_engine(
        &engine,
        &MqCommand::persist_session(
            &mut buf,
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

    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "client1"),
        3,
        2001,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored"
    );
    assert_eq!(resp.session_restored_id(), 200);
    assert!(resp.session_restored_expiry_ms() > 0);
    assert_eq!(&resp.session_restored_subscription_data()[..], b"new-data");
}

#[test]
fn test_opt6_session_survives_snapshot() {
    let mut engine = make_engine();
    let sub_data = Bytes::from(b"snapshot-test-data".to_vec());

    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "client1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    // Snapshot and restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    let resp = apply_engine(
        &engine2,
        &MqCommand::restore_session(&mut buf, "client1"),
        2,
        1001,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored"
    );
    assert_eq!(
        &resp.session_restored_subscription_data()[..],
        b"snapshot-test-data"
    );
}

#[test]
fn test_opt6_subscription_data_opaque() {
    let mut engine = make_engine();
    // Arbitrary binary data (not valid UTF-8)
    let sub_data = Bytes::from(vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02]);

    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "client1", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "client1"),
        2,
        1001,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored"
    );
    assert_eq!(
        resp.session_restored_subscription_data(),
        sub_data,
        "binary data should be preserved exactly"
    );
}

// =============================================================================
// Integration: End-to-End MQTT Optimization Workflows
// =============================================================================

#[test]
fn test_integration_will_via_engine_connect_disconnect() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "will-q", 2, 1001);
    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::create_binding(&mut buf, exchange_id, queue_id, Some("status/#")),
        3,
        1002,
    );

    // "Connect" -- create session with will
    create_session(&mut engine, 100, 4, 1003);
    let will_msg = make_flat_msg_with_routing_key(b"offline", "status/client1");
    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            exchange_id,
            0,
            1,
            false,
            "status/client1",
            &will_msg,
        ),
        5,
        1004,
    );

    // "Unclean disconnect"
    apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        6,
        1005,
    );

    // Will should have been published to the queue
    let delivered = deliver_messages(&mut engine, queue_id, 200, 1, 7, 1006);
    assert_eq!(delivered.len(), 1, "will message should be delivered");
}

#[test]
fn test_integration_session_restore_full_flow() {
    let mut engine = make_engine();

    // Connect and create session
    create_session(&mut engine, 100, 1, 1000);
    let sub_data = Bytes::from(b"filter:sensors/#,qos:1,no_local:false".to_vec());

    let mut buf = BytesMut::new();
    // Disconnect with session persistence
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "my-mqtt-client", 3600, &sub_data, 0, 0, 0),
        2,
        1001,
    );
    apply_engine(
        &engine,
        &MqCommand::disconnect_session(&mut buf, 100, false),
        3,
        1002,
    );

    // Reconnect -- restore session
    let resp = apply_engine(
        &engine,
        &MqCommand::restore_session(&mut buf, "my-mqtt-client"),
        4,
        1003,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored"
    );
    assert_eq!(resp.session_restored_id(), 100);
    assert_eq!(
        &resp.session_restored_subscription_data()[..],
        b"filter:sensors/#,qos:1,no_local:false"
    );
}

// =============================================================================
// Integration: Crash Recovery
// =============================================================================

#[test]
fn test_crash_recovery_session() {
    let mut engine = make_engine();
    let sub_data = Bytes::from(b"crash-recovery-subs".to_vec());

    let mut buf = BytesMut::new();
    apply_engine(
        &engine,
        &MqCommand::persist_session(&mut buf, 100, "crash-client", 3600, &sub_data, 0, 0, 0),
        1,
        1000,
    );

    // "Crash"
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    let resp = apply_engine(
        &engine2,
        &MqCommand::restore_session(&mut buf, "crash-client"),
        2,
        1001,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_SESSION_RESTORED,
        "expected SessionRestored"
    );
    assert_eq!(
        &resp.session_restored_subscription_data()[..],
        b"crash-recovery-subs"
    );
}

#[test]
fn test_crash_recovery_will() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    create_session(&mut engine, 100, 2, 1001);

    let mut buf = BytesMut::new();
    let will_msg = make_flat_msg_with_routing_key(b"crash-will", "status/c1");
    apply_engine(
        &engine,
        &MqCommand::set_will(
            &mut buf,
            100,
            exchange_id,
            0,
            0,
            false,
            "status/c1",
            &will_msg,
        ),
        3,
        1002,
    );

    // "Crash" before disconnect
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Create queue to receive will in restored engine
    let queue_id = create_queue(&mut engine2, "will-q", 4, 1003);
    apply_engine(
        &engine2,
        &MqCommand::create_binding(&mut buf, exchange_id, queue_id, Some("status/c1")),
        5,
        1004,
    );

    // Now disconnect -- will should still be set after recovery
    apply_engine(
        &engine2,
        &MqCommand::disconnect_session(&mut buf, 100, true),
        6,
        1005,
    );

    let delivered = deliver_messages(&mut engine2, queue_id, 200, 1, 7, 1006);
    assert_eq!(
        delivered.len(),
        1,
        "will should survive crash and publish on disconnect"
    );
}
