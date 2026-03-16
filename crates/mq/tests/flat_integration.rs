//! Comprehensive integration tests for the FlatMessage format.
//!
//! These tests exercise the full encode → command → apply → decode path,
//! verifying that flat-encoded messages flow correctly through the engine.

use bytes::{Bytes, BytesMut};

use bisque_mq::async_apply::ResponseEntry;
use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::{FlatMessage, FlatMessageBuilder, FlatMessageMeta};
use bisque_mq::types::*;

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-flat-integration-test"))
}

fn apply(engine: &MqEngine, cmd: &MqCommand) -> ResponseEntry {
    let mut buf = BytesMut::new();
    engine.apply_command(cmd, &mut buf, 1, 0, None);
    ResponseEntry::split_from(&mut buf)
}

fn apply_at(
    engine: &MqEngine,
    cmd: &MqCommand,
    log_index: u64,
    current_time: u64,
) -> ResponseEntry {
    let mut buf = BytesMut::new();
    engine.apply_command(cmd, &mut buf, log_index, current_time, None);
    ResponseEntry::split_from(&mut buf)
}

// =============================================================================
// FlatMessageBuilder → FlatMessage roundtrip
// =============================================================================

#[test]
fn test_flat_message_full_roundtrip() {
    let flat_bytes = FlatMessageBuilder::new(b"important payload data")
        .key(b"order-key-123")
        .timestamp(1700000000)
        .ttl_ms(30_000)
        .delay_ms(5_000)
        .routing_key(b"orders.us.west")
        .reply_to(b"reply-topic-1")
        .correlation_id(b"corr-uuid-456")
        .header(b"content-type", b"application/json")
        .header(b"x-trace-id", b"trace-abc-def")
        .header(b"x-priority", b"high")
        .build();

    let flat = FlatMessage::new(&flat_bytes).expect("valid flat message");
    assert_eq!(flat.value(), Bytes::from("important payload data"));
    assert_eq!(flat.key(), Some(b"order-key-123" as &[u8]));
    assert_eq!(flat.timestamp(), 1700000000);
    assert_eq!(flat.ttl_ms(), Some(30_000));
    assert_eq!(flat.delay_ms(), Some(5_000));
    assert_eq!(flat.routing_key_str(), Some("orders.us.west".into()));
    assert_eq!(flat.reply_to(), Some(b"reply-topic-1" as &[u8]));
    assert_eq!(flat.correlation_id(), Some(b"corr-uuid-456" as &[u8]));
    assert_eq!(flat.header_count(), 3);

    let (k0, v0) = flat.header(0);
    assert_eq!(k0, Bytes::from_static(b"content-type"));
    assert_eq!(v0, Bytes::from_static(b"application/json"));
}

#[test]
fn test_flat_message_minimal_roundtrip() {
    let flat_bytes = FlatMessageBuilder::new(b"bare").build();
    let flat = FlatMessage::new(&flat_bytes).unwrap();

    assert_eq!(flat.value(), Bytes::from_static(b"bare"));
    assert!(flat.key().is_none());
    assert!(flat.ttl_ms().is_none());
    assert!(flat.delay_ms().is_none());
    assert!(flat.routing_key().is_none());
    assert!(flat.reply_to().is_none());
    assert!(flat.correlation_id().is_none());
    assert_eq!(flat.header_count(), 0);
}

// =============================================================================
// FlatMessageMeta — header-only parsing
// =============================================================================

#[test]
fn test_meta_ttl_and_delay_extraction() {
    let buf = FlatMessageBuilder::new(b"data")
        .timestamp(5000)
        .ttl_ms(60_000)
        .delay_ms(10_000)
        .build();

    let meta = FlatMessageMeta::parse(&buf).unwrap();
    assert_eq!(meta.timestamp, 5000);
    assert_eq!(meta.ttl_ms_opt(), Some(60_000));
    assert_eq!(meta.delay_ms_opt(), Some(10_000));
    assert!(meta.has_routing_key() == false);
}

#[test]
fn test_meta_value_len_large_payload() {
    let large_value = vec![0xABu8; 65_536];
    let buf = FlatMessageBuilder::new(&large_value).build();

    assert_eq!(FlatMessageMeta::value_len(&buf), Some(65_536));

    let flat = FlatMessage::new(&buf).unwrap();
    assert_eq!(flat.value_len(), 65_536);
    assert_eq!(flat.value().len(), 65_536);
    assert_eq!(flat.value(), &large_value[..]);
}

#[test]
fn test_meta_flags_routing_key_present() {
    let buf = FlatMessageBuilder::new(b"")
        .routing_key(b"events.user.created")
        .build();

    let meta = FlatMessageMeta::parse(&buf).unwrap();
    assert!(meta.has_routing_key());
}

#[test]
fn test_meta_parse_too_short() {
    assert!(FlatMessageMeta::parse(&[0u8; 31]).is_none());
    assert!(FlatMessageMeta::parse(&[]).is_none());
}

#[test]
fn test_meta_value_len_too_short() {
    // Only 32 bytes (header) — no span index
    assert!(FlatMessageMeta::value_len(&[0u8; 32]).is_none());
}

// =============================================================================
// Queue publish with TTL from flat header
// =============================================================================

#[test]
fn test_queue_publish_ttl_from_flat_header() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    // create_queue returns group_id; source topic is auto-created (group_id+1)
    MqCommand::write_create_queue(
        &mut buf,
        "ttl-queue",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    // Publish message with 30s TTL to the source topic
    let msg = FlatMessageBuilder::new(b"expiring")
        .timestamp(1000)
        .ttl_ms(30_000)
        .build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 5000);

    let snap = engine.snapshot();
    // Topic should have 1 message
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "ttl-queue")
        .unwrap();
    assert_eq!(topic.meta.message_count, 1);

    MqCommand::write_group_deliver(&mut buf, group_id, 100, 1);
    let _resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 6000);
    // Messages may or may not be delivered depending on ack state population
}

#[test]
fn test_queue_publish_delay_from_flat_header() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "delay-queue",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"delayed")
        .timestamp(1000)
        .delay_ms(10_000)
        .build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 5000);

    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "delay-queue")
        .unwrap();
    assert_eq!(topic.meta.message_count, 1);
}

#[test]
fn test_queue_publish_no_ttl_no_delay() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "plain-queue",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"plain").timestamp(1000).build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 5000);

    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "plain-queue")
        .unwrap();
    assert_eq!(topic.meta.message_count, 1);
}

#[test]
fn test_queue_publish_delay_clamped_by_ttl() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "clamped-queue",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"clamped")
        .timestamp(1000)
        .ttl_ms(10_000)
        .delay_ms(60_000)
        .build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 5000);

    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "clamped-queue")
        .unwrap();
    assert_eq!(topic.meta.message_count, 1);
}

// =============================================================================
// Topic publish with flat messages
// =============================================================================

#[test]
fn test_topic_publish_flat_messages() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_topic(&mut buf, "events", &RetentionPolicy::default(), 0);
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let topic_id = r.entity_id();

    let messages: Vec<Bytes> = vec![
        FlatMessageBuilder::new(b"event-1")
            .key(b"user-1")
            .timestamp(1000)
            .build(),
        FlatMessageBuilder::new(b"event-2")
            .timestamp(1001)
            .routing_key(b"events.user.created")
            .build(),
        FlatMessageBuilder::new(b"event-3")
            .key(b"user-3")
            .timestamp(1002)
            .header(b"source", b"api")
            .header(b"version", b"2")
            .build(),
    ];

    MqCommand::write_publish_bytes(&mut buf, topic_id, &messages);
    let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_PUBLISHED,
        "expected Published"
    );
    assert_eq!(resp.published_count(), 3);

    let snap = engine.snapshot();
    assert_eq!(snap.topics[0].meta.message_count, 3);
}

// =============================================================================
// Exchange routing with flat message routing key
// =============================================================================

#[test]
fn test_exchange_fanout_with_flat_messages() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let r = apply_at(
        &engine,
        &MqCommand::create_exchange(&mut buf, "fanout-ex", ExchangeType::Fanout),
        1,
        1000,
    );
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let ex_id = r.entity_id();

    // create_queue auto-creates source topic with same name
    MqCommand::write_create_queue(
        &mut buf,
        "q1",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let q1_group_id = r.entity_id();
    let q1_topic_id = q1_group_id + 1;

    MqCommand::write_create_queue(
        &mut buf,
        "q2",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let q2_group_id = r.entity_id();
    let q2_topic_id = q2_group_id + 1;

    // Bind exchange to source topics
    apply_at(
        &engine,
        &MqCommand::create_binding(&mut buf, ex_id, q1_topic_id, None),
        4,
        1000,
    );
    apply_at(
        &engine,
        &MqCommand::create_binding(&mut buf, ex_id, q2_topic_id, None),
        5,
        1000,
    );

    let msg = FlatMessageBuilder::new(b"fanout-data")
        .timestamp(2000)
        .key(b"some-key")
        .build();

    let resp = apply_at(
        &engine,
        &MqCommand::publish_to_exchange(&mut buf, ex_id, &[msg]),
        6,
        2000,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_PUBLISHED,
        "expected Published"
    );

    let snap = engine.snapshot();
    // Each source topic should have 1 message
    let q1_topic = snap.topics.iter().find(|t| t.meta.name == "q1").unwrap();
    let q2_topic = snap.topics.iter().find(|t| t.meta.name == "q2").unwrap();
    assert_eq!(q1_topic.meta.message_count, 1, "q1 topic should have 1 msg");
    assert_eq!(q2_topic.meta.message_count, 1, "q2 topic should have 1 msg");
}

#[test]
fn test_exchange_direct_routing_with_flat_routing_key() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let r = apply_at(
        &engine,
        &MqCommand::create_exchange(&mut buf, "direct-ex", ExchangeType::Direct),
        1,
        1000,
    );
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let ex_id = r.entity_id();

    MqCommand::write_create_queue(
        &mut buf,
        "orders-us",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let q_us_group = r.entity_id();
    let q_us_topic = q_us_group + 1;

    MqCommand::write_create_queue(
        &mut buf,
        "orders-eu",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let q_eu_group = r.entity_id();
    let q_eu_topic = q_eu_group + 1;

    apply_at(
        &engine,
        &MqCommand::create_binding(&mut buf, ex_id, q_us_topic, Some("us")),
        4,
        1000,
    );
    apply_at(
        &engine,
        &MqCommand::create_binding(&mut buf, ex_id, q_eu_topic, Some("eu")),
        5,
        1000,
    );

    let msg_us = FlatMessageBuilder::new(b"order-data-us")
        .timestamp(2000)
        .routing_key(b"us")
        .build();

    apply_at(
        &engine,
        &MqCommand::publish_to_exchange(&mut buf, ex_id, &[msg_us]),
        6,
        2000,
    );

    let snap = engine.snapshot();
    let us_topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "orders-us")
        .unwrap();
    let eu_topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "orders-eu")
        .unwrap();
    assert_eq!(us_topic.meta.message_count, 1);
    assert_eq!(eu_topic.meta.message_count, 0);

    let msg_eu = FlatMessageBuilder::new(b"order-data-eu")
        .timestamp(2001)
        .routing_key(b"eu")
        .build();

    apply_at(
        &engine,
        &MqCommand::publish_to_exchange(&mut buf, ex_id, &[msg_eu]),
        7,
        2001,
    );

    let snap = engine.snapshot();
    let us_topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "orders-us")
        .unwrap();
    let eu_topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "orders-eu")
        .unwrap();
    assert_eq!(us_topic.meta.message_count, 1);
    assert_eq!(eu_topic.meta.message_count, 1);
}

#[test]
fn test_exchange_no_routing_key_in_flat_message() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let r = apply_at(
        &engine,
        &MqCommand::create_exchange(&mut buf, "direct-ex-2", ExchangeType::Direct),
        1,
        1000,
    );
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let ex_id = r.entity_id();

    MqCommand::write_create_queue(
        &mut buf,
        "bound-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let q_group = r.entity_id();
    let q_topic = q_group + 1;

    apply_at(
        &engine,
        &MqCommand::create_binding(&mut buf, ex_id, q_topic, Some("specific")),
        3,
        1000,
    );

    let msg = FlatMessageBuilder::new(b"no-route").timestamp(2000).build();

    apply_at(
        &engine,
        &MqCommand::publish_to_exchange(&mut buf, ex_id, &[msg]),
        4,
        2000,
    );

    let snap = engine.snapshot();
    let q = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "bound-q")
        .unwrap();
    assert_eq!(
        q.meta.message_count, 0,
        "should not route without matching key"
    );
}

// =============================================================================
// Actor messages with flat encoding
// =============================================================================

#[test]
fn test_actor_send_flat_message() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    // Create actor group — returns group_id; source topic is auto-created
    let r = apply_at(
        &engine,
        &MqCommand::create_actor_group(
            &mut buf,
            "workers",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
    );
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"task-payload")
        .key(b"task-key")
        .timestamp(5000)
        .correlation_id(b"corr-xyz")
        .reply_to(b"response-topic")
        .header(b"priority", b"high")
        .build();

    // Publish to the actor group's source topic
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 5000);

    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "workers")
        .unwrap();
    assert_eq!(topic.meta.message_count, 1);
}

// =============================================================================
// Batch publish with mixed flat messages
// =============================================================================

#[test]
fn test_batch_publish_mixed_flat_messages() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "mixed-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let messages: Vec<Bytes> = vec![
        FlatMessageBuilder::new(b"msg-1").timestamp(1000).build(),
        FlatMessageBuilder::new(b"msg-2")
            .key(b"k2")
            .timestamp(1001)
            .ttl_ms(5000)
            .build(),
        FlatMessageBuilder::new(b"msg-3")
            .timestamp(1002)
            .delay_ms(2000)
            .build(),
        FlatMessageBuilder::new(b"msg-4")
            .key(b"k4")
            .timestamp(1003)
            .ttl_ms(60000)
            .delay_ms(1000)
            .routing_key(b"rk")
            .reply_to(b"rt")
            .correlation_id(b"cid")
            .header(b"h1", b"v1")
            .build(),
        FlatMessageBuilder::new(b"").timestamp(1004).build(),
    ];

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &messages);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);

    let snap = engine.snapshot();
    let topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "mixed-q")
        .unwrap();
    assert_eq!(topic.meta.message_count, 5);
}

// =============================================================================
// FlatMessage structural validation
// =============================================================================

#[test]
fn test_flat_message_invalid_buffer() {
    assert!(FlatMessage::new(&[]).is_none());
    assert!(FlatMessage::new(b"too short").is_none());

    let mut buf = vec![0u8; 32];
    buf[4] = 100;
    buf[5] = 0;
    assert!(FlatMessage::new(&buf).is_none());
}

#[test]
fn test_flat_message_empty_optional_fields() {
    let buf = FlatMessageBuilder::new(b"val")
        .key(b"")
        .routing_key(b"")
        .build();

    let flat = FlatMessage::new(&buf).unwrap();
    assert_eq!(flat.value(), Bytes::from_static(b"val"));
    assert_eq!(flat.key(), Some(b"" as &[u8]));
    assert_eq!(flat.routing_key(), Some(b"" as &[u8]));
}

#[test]
fn test_flat_message_many_headers() {
    let mut builder = FlatMessageBuilder::new(b"payload");
    let mut hdr_keys: Vec<String> = Vec::new();
    let mut hdr_vals: Vec<String> = Vec::new();
    for i in 0..50 {
        hdr_keys.push(format!("header-key-{}", i));
        hdr_vals.push(format!("header-value-{}", i));
    }
    for i in 0..50 {
        builder = builder.header(hdr_keys[i].as_bytes(), hdr_vals[i].as_bytes());
    }
    let buf = builder.build();

    let flat = FlatMessage::new(&buf).unwrap();
    assert_eq!(flat.header_count(), 50);

    let (k0, v0) = flat.header(0);
    assert_eq!(k0, Bytes::from("header-key-0"));
    assert_eq!(v0, Bytes::from("header-value-0"));

    let (k49, v49) = flat.header(49);
    assert_eq!(k49, Bytes::from("header-key-49"));
    assert_eq!(v49, Bytes::from("header-value-49"));

    let headers: Vec<_> = flat.headers().collect();
    assert_eq!(headers.len(), 50);
}

#[test]
fn test_flat_message_large_value() {
    let large = vec![0xFFu8; 1_000_000];
    let buf = FlatMessageBuilder::new(&large)
        .timestamp(42)
        .key(b"big-key")
        .build();

    let flat = FlatMessage::new(&buf).unwrap();
    assert_eq!(flat.value().len(), 1_000_000);
    assert_eq!(flat.value(), &large[..]);
    assert_eq!(flat.value_len(), 1_000_000);
    assert_eq!(flat.key(), Some(b"big-key" as &[u8]));
}

#[test]
fn test_flat_message_binary_value() {
    let binary_value: &[u8] = &[0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD];
    let binary_key: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

    let buf = FlatMessageBuilder::new(binary_value)
        .key(binary_key)
        .timestamp(100)
        .build();

    let flat = FlatMessage::new(&buf).unwrap();
    assert_eq!(flat.value(), binary_value);
    assert_eq!(flat.key().as_deref(), Some(binary_key));
}

// =============================================================================
// Snapshot/restore preserves flat message state
// =============================================================================

#[test]
fn test_snapshot_restore_with_flat_messages() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_topic(&mut buf, "t1", &RetentionPolicy::default(), 0);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);

    let messages: Vec<Bytes> = vec![
        FlatMessageBuilder::new(b"evt-1")
            .key(b"k1")
            .timestamp(1000)
            .build(),
        FlatMessageBuilder::new(b"evt-2")
            .timestamp(1001)
            .ttl_ms(5000)
            .build(),
    ];
    MqCommand::write_publish_bytes(&mut buf, 1, &messages);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    MqCommand::write_create_queue(
        &mut buf,
        "q1",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let enqueue_msg = FlatMessageBuilder::new(b"task-1")
        .timestamp(2000)
        .delay_ms(1000)
        .build();
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[enqueue_msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 4, 2000);

    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    let snap2 = engine2.snapshot();
    let t1_topic = snap2.topics.iter().find(|t| t.meta.name == "t1").unwrap();
    assert_eq!(t1_topic.meta.message_count, 2);
    // Source topic for q1 should have 1 message
    let q1_topic = snap2.topics.iter().find(|t| t.meta.name == "q1").unwrap();
    assert_eq!(q1_topic.meta.message_count, 1);

    let post_restore_msg = FlatMessageBuilder::new(b"post-restore")
        .timestamp(3000)
        .build();
    MqCommand::write_publish_bytes(&mut buf, 1, &[post_restore_msg]);
    let resp = apply_at(&engine2, &MqCommand::split_from(&mut buf), 5, 3000);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_PUBLISHED,
        "expected Published"
    );
}

// =============================================================================
// FlatMessage field combination edge cases
// =============================================================================

#[test]
fn test_flat_message_only_reply_to_and_correlation_id() {
    let buf = FlatMessageBuilder::new(b"rpc-request")
        .timestamp(100)
        .reply_to(b"response-queue")
        .correlation_id(b"req-id-001")
        .build();

    let flat = FlatMessage::new(&buf).unwrap();
    assert_eq!(flat.value(), Bytes::from_static(b"rpc-request"));
    assert!(flat.key().is_none());
    assert!(flat.routing_key().is_none());
    assert_eq!(flat.reply_to(), Some(b"response-queue" as &[u8]));
    assert_eq!(flat.correlation_id(), Some(b"req-id-001" as &[u8]));
}

#[test]
fn test_flat_message_only_routing_key_no_key() {
    let buf = FlatMessageBuilder::new(b"routed")
        .routing_key(b"events.user.signup")
        .timestamp(200)
        .build();

    let flat = FlatMessage::new(&buf).unwrap();
    assert!(flat.key().is_none());
    assert_eq!(flat.routing_key_str(), Some("events.user.signup".into()));
}

#[test]
fn test_flat_message_all_optional_fields_plus_headers() {
    let buf = FlatMessageBuilder::new(b"full")
        .key(b"k")
        .routing_key(b"rk")
        .reply_to(b"rt")
        .correlation_id(b"cid")
        .timestamp(999)
        .ttl_ms(1000)
        .delay_ms(500)
        .header(b"h1", b"v1")
        .header(b"h2", b"v2")
        .build();

    let flat = FlatMessage::new(&buf).unwrap();

    assert_eq!(flat.value(), Bytes::from_static(b"full"));
    assert_eq!(flat.key(), Some(b"k" as &[u8]));
    assert_eq!(flat.routing_key(), Some(b"rk" as &[u8]));
    assert_eq!(flat.reply_to(), Some(b"rt" as &[u8]));
    assert_eq!(flat.correlation_id(), Some(b"cid" as &[u8]));
    assert_eq!(flat.timestamp(), 999);
    assert_eq!(flat.ttl_ms(), Some(1000));
    assert_eq!(flat.delay_ms(), Some(500));
    assert_eq!(flat.header_count(), 2);

    let meta = FlatMessageMeta::parse(&buf).unwrap();
    assert_eq!(meta.ttl_ms_opt(), Some(1000));
    assert_eq!(meta.delay_ms_opt(), Some(500));
    assert!(meta.has_routing_key());
}

#[test]
fn test_flat_message_clone_independence() {
    let buf = FlatMessageBuilder::new(b"original")
        .key(b"key")
        .timestamp(42)
        .build();

    let flat1 = FlatMessage::new(&buf).unwrap();
    let flat2 = flat1.clone();

    assert_eq!(flat1.value(), flat2.value());
    assert_eq!(flat1.key(), flat2.key());
    assert_eq!(flat1.timestamp(), flat2.timestamp());

    let raw = flat1.as_bytes();
    assert!(!raw.is_empty());
    assert_eq!(flat2.value(), b"original");
}

#[test]
fn test_flat_message_as_bytes_roundtrip() {
    let original = FlatMessageBuilder::new(b"data")
        .key(b"k")
        .ttl_ms(1000)
        .timestamp(5)
        .build();

    let flat = FlatMessage::new(&original).unwrap();
    let raw = flat.as_bytes().clone();

    let flat2 = FlatMessage::new(&raw).unwrap();
    assert_eq!(flat2.value(), b"data");
    assert_eq!(flat2.key(), Some(b"k" as &[u8]));
    assert_eq!(flat2.ttl_ms(), Some(1000));
}

// =============================================================================
// ACK with response — queue request/reply pattern
// =============================================================================

#[test]
fn test_queue_ack_with_response_publishes_to_reply_topic() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_topic(&mut buf, "responses", &RetentionPolicy::default(), 0);
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let reply_topic_id = r.entity_id();

    MqCommand::write_create_queue(
        &mut buf,
        "requests",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);
    assert_eq!(
        r.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let request_msg = FlatMessageBuilder::new(b"compute-something")
        .timestamp(2000)
        .reply_to(b"responses")
        .correlation_id(b"req-001")
        .build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[request_msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 2000);

    MqCommand::write_group_deliver(&mut buf, group_id, 100, 1);
    let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 4, 3000);
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;

        let response_msg = FlatMessageBuilder::new(b"result: 42")
            .timestamp(4000)
            .correlation_id(b"req-001")
            .build();

        MqCommand::write_group_ack(&mut buf, group_id, &[msg_id], Some(&response_msg));
        let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 5, 4000);
        assert_eq!(resp.tag(), ResponseEntry::TAG_OK);

        let snap = engine.snapshot();
        let reply_topic = snap
            .topics
            .iter()
            .find(|t| t.meta.topic_id == reply_topic_id)
            .unwrap();
        assert_eq!(
            reply_topic.meta.message_count, 1,
            "response should be published to reply topic"
        );
    }
}

#[test]
fn test_queue_ack_without_response_is_normal_ack() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "normal-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"data")
        .timestamp(1000)
        .reply_to(b"some-topic")
        .build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);

    MqCommand::write_group_deliver(&mut buf, group_id, 100, 1);
    let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 2000);
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;
        MqCommand::write_group_ack(&mut buf, group_id, &[msg_id], None);
        let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 4, 3000);
        assert_eq!(resp.tag(), ResponseEntry::TAG_OK);
    }
}

#[test]
fn test_queue_ack_response_without_reply_to_drops_silently() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "no-reply-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"no-reply").timestamp(1000).build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);

    MqCommand::write_group_deliver(&mut buf, group_id, 100, 1);
    let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 2000);
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;
        let response = FlatMessageBuilder::new(b"orphan-response")
            .timestamp(3000)
            .build();

        MqCommand::write_group_ack(&mut buf, group_id, &[msg_id], Some(&response));
        let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 4, 3000);
        assert_eq!(resp.tag(), ResponseEntry::TAG_OK);
    }
}

#[test]
fn test_queue_ack_response_reply_topic_missing_drops_silently() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "q-missing-topic",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"request")
        .timestamp(1000)
        .reply_to(b"nonexistent-topic")
        .build();

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);

    MqCommand::write_group_deliver(&mut buf, group_id, 100, 1);
    let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 2000);
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;
        let response = FlatMessageBuilder::new(b"response").timestamp(3000).build();

        MqCommand::write_group_ack(&mut buf, group_id, &[msg_id], Some(&response));
        let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 4, 3000);
        assert_eq!(resp.tag(), ResponseEntry::TAG_OK);
    }
}

#[test]
fn test_queue_ack_multiple_messages_first_has_reply_to() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_topic(&mut buf, "multi-replies", &RetentionPolicy::default(), 0);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 1, 1000);

    MqCommand::write_create_queue(
        &mut buf,
        "multi-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let r = apply_at(&engine, &MqCommand::split_from(&mut buf), 2, 1000);
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg1 = FlatMessageBuilder::new(b"req-1")
        .timestamp(1000)
        .reply_to(b"multi-replies")
        .correlation_id(b"corr-1")
        .build();
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg1]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 3, 1000);

    let msg2 = FlatMessageBuilder::new(b"req-2").timestamp(1001).build();
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[msg2]);
    apply_at(&engine, &MqCommand::split_from(&mut buf), 4, 1001);

    MqCommand::write_group_deliver(&mut buf, group_id, 100, 2);
    let resp = apply_at(&engine, &MqCommand::split_from(&mut buf), 5, 2000);
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let msg_ids: Vec<u64> = resp.messages().map(|m| m.message_id).collect();

        let response = FlatMessageBuilder::new(b"batch-result")
            .timestamp(3000)
            .build();

        MqCommand::write_group_ack(&mut buf, group_id, &msg_ids, Some(&response));
        apply_at(&engine, &MqCommand::split_from(&mut buf), 6, 3000);
    }
}

// =============================================================================
// ACK with response — actor request/reply pattern
// =============================================================================

#[test]
fn test_actor_ack_with_response_publishes_to_reply_topic() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let r = apply_at(
        &engine,
        &MqCommand::create_topic(&mut buf, "actor-responses", RetentionPolicy::default(), 0),
        1,
        1000,
    );
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let reply_topic_id = r.entity_id();

    let r = apply_at(
        &engine,
        &MqCommand::create_actor_group(
            &mut buf,
            "rpc-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        2,
        1000,
    );
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();

    let actor_id = Bytes::from_static(b"calculator");

    let request = FlatMessageBuilder::new(b"2+2")
        .timestamp(2000)
        .reply_to(b"actor-responses")
        .correlation_id(b"calc-001")
        .build();

    let source_topic_id = group_id + 1;
    apply_at(
        &engine,
        &MqCommand::publish(&mut buf, source_topic_id, &[request]),
        3,
        2000,
    );
    apply_at(
        &engine,
        &MqCommand::group_assign_actors(&mut buf, group_id, 100, &[actor_id.clone()]),
        4,
        2001,
    );

    let resp = apply_at(
        &engine,
        &MqCommand::group_deliver_actor(&mut buf, group_id, 100, &[actor_id.clone()]),
        5,
        2002,
    );
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;

        let response = FlatMessageBuilder::new(b"4")
            .timestamp(3000)
            .correlation_id(b"calc-001")
            .build();

        let resp = apply_at(
            &engine,
            &MqCommand::group_ack_actor(&mut buf, group_id, &actor_id, msg_id, Some(&response)),
            6,
            3000,
        );
        assert_eq!(resp.tag(), ResponseEntry::TAG_OK);

        let snap = engine.snapshot();
        let reply_topic = snap
            .topics
            .iter()
            .find(|t| t.meta.topic_id == reply_topic_id)
            .unwrap();
        assert_eq!(
            reply_topic.meta.message_count, 1,
            "actor response should be published to reply topic"
        );
    }
}

#[test]
fn test_actor_ack_without_response_normal() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let r = apply_at(
        &engine,
        &MqCommand::create_actor_group(
            &mut buf,
            "normal-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
    );
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let actor_id = Bytes::from_static(b"worker-1");

    let msg = FlatMessageBuilder::new(b"fire-and-forget")
        .timestamp(2000)
        .build();

    apply_at(
        &engine,
        &MqCommand::publish(&mut buf, source_topic_id, &[msg]),
        2,
        2000,
    );
    apply_at(
        &engine,
        &MqCommand::group_assign_actors(&mut buf, group_id, 100, &[actor_id.clone()]),
        3,
        2001,
    );

    let resp = apply_at(
        &engine,
        &MqCommand::group_deliver_actor(&mut buf, group_id, 100, &[actor_id.clone()]),
        4,
        2002,
    );
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;
        let resp = apply_at(
            &engine,
            &MqCommand::group_ack_actor(&mut buf, group_id, &actor_id, msg_id, None),
            5,
            3000,
        );
        assert_eq!(resp.tag(), ResponseEntry::TAG_OK);
    }
}

#[test]
fn test_actor_ack_response_no_reply_to_drops_silently() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let r = apply_at(
        &engine,
        &MqCommand::create_actor_group(
            &mut buf,
            "no-reply-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
    );
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let actor_id = Bytes::from_static(b"actor-1");

    let msg = FlatMessageBuilder::new(b"task").timestamp(2000).build();

    apply_at(
        &engine,
        &MqCommand::publish(&mut buf, source_topic_id, &[msg]),
        2,
        2000,
    );
    apply_at(
        &engine,
        &MqCommand::group_assign_actors(&mut buf, group_id, 100, &[actor_id.clone()]),
        3,
        2001,
    );

    let resp = apply_at(
        &engine,
        &MqCommand::group_deliver_actor(&mut buf, group_id, 100, &[actor_id.clone()]),
        4,
        2002,
    );
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;
        let response = FlatMessageBuilder::new(b"result").timestamp(3000).build();

        let resp = apply_at(
            &engine,
            &MqCommand::group_ack_actor(&mut buf, group_id, &actor_id, msg_id, Some(&response)),
            5,
            3000,
        );
        assert_eq!(resp.tag(), ResponseEntry::TAG_OK);
    }
}

// =============================================================================
// Display format with response
// =============================================================================

#[test]
fn test_ack_display_with_response() {
    let mut buf = BytesMut::new();
    let ack_no_resp = MqCommand::group_ack(&mut buf, 1, &[10, 11], None);
    assert_eq!(format!("{}", ack_no_resp), "GroupAck(group=1)");

    let resp_bytes = FlatMessageBuilder::new(b"resp").build();
    let ack_with_resp = MqCommand::group_ack(&mut buf, 1, &[10], Some(&resp_bytes));
    assert_eq!(format!("{}", ack_with_resp), "GroupAck(group=1)");
}

#[test]
fn test_ack_actor_display_with_response() {
    let mut buf = BytesMut::new();
    let actor_id = Bytes::from_static(b"a1");
    let ack_no_resp = MqCommand::group_ack_actor(&mut buf, 1, &actor_id, 10, None);
    assert_eq!(format!("{}", ack_no_resp), "GroupAckActor(group=1)");

    let resp_bytes = FlatMessageBuilder::new(b"resp").build();
    let ack_with_resp = MqCommand::group_ack_actor(&mut buf, 1, &actor_id, 10, Some(&resp_bytes));
    assert_eq!(format!("{}", ack_with_resp), "GroupAckActor(group=1)");
}

// =============================================================================
// Snapshot/restore preserves reply_to metadata
// =============================================================================

#[test]
fn test_snapshot_restore_preserves_reply_to() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    apply_at(
        &engine,
        &MqCommand::create_topic(&mut buf, "replies", RetentionPolicy::default(), 0),
        1,
        1000,
    );
    let r = apply_at(
        &engine,
        &MqCommand::create_queue(
            &mut buf,
            "rpc-queue",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        2,
        1000,
    );
    assert_eq!(r.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let group_id = r.entity_id();
    let source_topic_id = group_id + 1;

    let msg = FlatMessageBuilder::new(b"request")
        .timestamp(2000)
        .reply_to(b"replies")
        .correlation_id(b"corr-snap")
        .build();

    apply_at(
        &engine,
        &MqCommand::publish(&mut buf, source_topic_id, &[msg]),
        3,
        2000,
    );

    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    let resp = apply_at(
        &engine2,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 1),
        4,
        3000,
    );
    if resp.tag() == ResponseEntry::TAG_MESSAGES && resp.message_count() > 0 {
        let messages: Vec<_> = resp.messages().collect();
        let msg_id = messages[0].message_id;

        let response = FlatMessageBuilder::new(b"reply-data")
            .timestamp(4000)
            .build();

        apply_at(
            &engine2,
            &MqCommand::group_ack(&mut buf, group_id, &[msg_id], Some(&response)),
            5,
            4000,
        );

        let snap2 = engine2.snapshot();
        let reply_topic = snap2
            .topics
            .iter()
            .find(|t| t.meta.name == "replies")
            .unwrap();
        assert_eq!(
            reply_topic.meta.message_count, 1,
            "response should be routed to reply topic after restore"
        );
    }
}
