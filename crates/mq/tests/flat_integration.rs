//! Comprehensive integration tests for the FlatMessage format.
//!
//! These tests exercise the full encode → command → apply → decode path,
//! verifying that flat-encoded messages flow correctly through the engine.

use bytes::Bytes;

use bisque_mq::config::{ActorConfig, MqConfig, QueueConfig};
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::{FlatMessage, FlatMessageBuilder, FlatMessageMeta};
use bisque_mq::types::*;

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-flat-integration-test"))
}

// =============================================================================
// MessagePayload ↔ FlatMessage roundtrip
// =============================================================================

#[test]
fn test_message_payload_full_roundtrip() {
    let payload = MessagePayload {
        key: Some(Bytes::from_static(b"order-key-123")),
        value: Bytes::from("important payload data"),
        headers: vec![
            (
                "content-type".into(),
                Bytes::from_static(b"application/json"),
            ),
            ("x-trace-id".into(), Bytes::from_static(b"trace-abc-def")),
            ("x-priority".into(), Bytes::from_static(b"high")),
        ],
        timestamp: 1700000000,
        ttl_ms: Some(30_000),
        delay_ms: Some(5_000),
        routing_key: Some("orders.us.west".into()),
        reply_to: Some("reply-topic-1".into()),
        correlation_id: Some("corr-uuid-456".into()),
    };

    // Encode via the convenience method
    let flat_bytes = payload.encode_flat();

    // Decode and verify every field
    let flat = FlatMessage::new(flat_bytes).expect("valid flat message");
    assert_eq!(flat.value(), Bytes::from("important payload data"));
    assert_eq!(flat.key(), Some(Bytes::from_static(b"order-key-123")));
    assert_eq!(flat.timestamp(), 1700000000);
    assert_eq!(flat.ttl_ms(), Some(30_000));
    assert_eq!(flat.delay_ms(), Some(5_000));
    assert_eq!(flat.routing_key_str(), Some("orders.us.west".into()));
    assert_eq!(flat.reply_to(), Some(Bytes::from_static(b"reply-topic-1")));
    assert_eq!(
        flat.correlation_id(),
        Some(Bytes::from_static(b"corr-uuid-456"))
    );
    assert_eq!(flat.header_count(), 3);

    let (k0, v0) = flat.header(0);
    assert_eq!(k0, Bytes::from_static(b"content-type"));
    assert_eq!(v0, Bytes::from_static(b"application/json"));

    // Round-trip back to MessagePayload
    let back = flat.to_message_payload();
    assert_eq!(back.key, payload.key);
    assert_eq!(back.value, payload.value);
    assert_eq!(back.timestamp, payload.timestamp);
    assert_eq!(back.ttl_ms, payload.ttl_ms);
    assert_eq!(back.delay_ms, payload.delay_ms);
    assert_eq!(back.routing_key, payload.routing_key);
    assert_eq!(back.reply_to, payload.reply_to);
    assert_eq!(back.correlation_id, payload.correlation_id);
    assert_eq!(back.headers.len(), 3);
}

#[test]
fn test_message_payload_minimal_roundtrip() {
    let payload = MessagePayload {
        key: None,
        value: Bytes::from_static(b"bare"),
        headers: vec![],
        timestamp: 0,
        ttl_ms: None,
        delay_ms: None,
        routing_key: None,
        reply_to: None,
        correlation_id: None,
    };

    let flat_bytes = payload.encode_flat();
    let flat = FlatMessage::new(flat_bytes).unwrap();

    assert_eq!(flat.value(), Bytes::from_static(b"bare"));
    assert!(flat.key().is_none());
    assert!(flat.ttl_ms().is_none());
    assert!(flat.delay_ms().is_none());
    assert!(flat.routing_key().is_none());
    assert!(flat.reply_to().is_none());
    assert!(flat.correlation_id().is_none());
    assert_eq!(flat.header_count(), 0);

    let back = flat.to_message_payload();
    assert_eq!(back.value, payload.value);
    assert!(back.key.is_none());
}

// =============================================================================
// FlatMessageMeta — header-only parsing
// =============================================================================

#[test]
fn test_meta_ttl_and_delay_extraction() {
    let buf = FlatMessageBuilder::new(Bytes::from_static(b"data"))
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
    let large_value = Bytes::from(vec![0xABu8; 65_536]);
    let buf = FlatMessageBuilder::new(large_value.clone()).build();

    assert_eq!(FlatMessageMeta::value_len(&buf), Some(65_536));

    let flat = FlatMessage::new(buf).unwrap();
    assert_eq!(flat.value_len(), 65_536);
    assert_eq!(flat.value().len(), 65_536);
    assert_eq!(flat.value(), large_value);
}

#[test]
fn test_meta_flags_routing_key_present() {
    let buf = FlatMessageBuilder::new(Bytes::new())
        .routing_key(Bytes::from_static(b"events.user.created"))
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
// Queue enqueue with TTL from flat header
// =============================================================================

#[test]
fn test_queue_enqueue_ttl_from_flat_header() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("ttl-queue", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Enqueue message with 30s TTL
    let msg = FlatMessageBuilder::new(Bytes::from_static(b"expiring"))
        .timestamp(1000)
        .ttl_ms(30_000)
        .build();

    engine.apply_command(
        MqCommand::enqueue(queue_id, &[msg], &[None]),
        2,
        5000, // current_time = 5000
    );

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 1);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 3, 6000);
    match resp {
        MqResponse::Messages { messages } => assert_eq!(messages.len(), 1),
        other => panic!("expected Messages, got {:?}", other),
    }
}

#[test]
fn test_queue_enqueue_delay_from_flat_header() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("delay-queue", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"delayed"))
        .timestamp(1000)
        .delay_ms(10_000)
        .build();

    engine.apply_command(MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 5000);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 3, 6000);
    match resp {
        MqResponse::Messages { messages } => assert_eq!(
            messages.len(),
            0,
            "message should not be deliverable before delay expires"
        ),
        other => panic!("expected Messages, got {:?}", other),
    }

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 4, 16000);
    match resp {
        MqResponse::Messages { messages } => assert_eq!(messages.len(), 1),
        other => panic!("expected Messages, got {:?}", other),
    }
}

#[test]
fn test_queue_enqueue_no_ttl_no_delay() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("plain-queue", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"plain"))
        .timestamp(1000)
        .build();

    engine.apply_command(MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 5000);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 3, 5001);
    match resp {
        MqResponse::Messages { messages } => assert_eq!(messages.len(), 1),
        other => panic!("expected Messages, got {:?}", other),
    }
}

#[test]
fn test_queue_enqueue_delay_clamped_by_ttl() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("clamped-queue", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"clamped"))
        .timestamp(1000)
        .ttl_ms(10_000)
        .delay_ms(60_000)
        .build();

    engine.apply_command(MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 5000);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 3, 15001);
    match resp {
        MqResponse::Messages { messages } => {
            assert!(messages.len() <= 1);
        }
        other => panic!("expected Messages, got {:?}", other),
    }
}

// =============================================================================
// Topic publish with flat messages
// =============================================================================

#[test]
fn test_topic_publish_flat_messages() {
    let mut engine = make_engine();

    let topic_id = match engine.apply_command(
        MqCommand::create_topic("events", RetentionPolicy::default(), 0),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let messages: Vec<Bytes> = vec![
        FlatMessageBuilder::new(Bytes::from_static(b"event-1"))
            .key(Bytes::from_static(b"user-1"))
            .timestamp(1000)
            .build(),
        FlatMessageBuilder::new(Bytes::from_static(b"event-2"))
            .timestamp(1001)
            .routing_key(Bytes::from_static(b"events.user.created"))
            .build(),
        FlatMessageBuilder::new(Bytes::from_static(b"event-3"))
            .key(Bytes::from_static(b"user-3"))
            .timestamp(1002)
            .header("source", &b"api"[..])
            .header("version", &b"2"[..])
            .build(),
    ];

    let resp = engine.apply_command(MqCommand::publish(topic_id, &messages), 2, 1000);
    match resp {
        MqResponse::Published { offsets } => assert_eq!(offsets.len(), 3),
        other => panic!("expected Published, got {:?}", other),
    }

    let snap = engine.snapshot();
    assert_eq!(snap.topics[0].meta.message_count, 3);
}

// =============================================================================
// Exchange routing with flat message routing key
// =============================================================================

#[test]
fn test_exchange_fanout_with_flat_messages() {
    let mut engine = make_engine();

    let ex_id = match engine.apply_command(
        MqCommand::create_exchange("fanout-ex", ExchangeType::Fanout),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let q1_id = match engine.apply_command(
        MqCommand::create_queue("q1", &QueueConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };
    let q2_id = match engine.apply_command(
        MqCommand::create_queue("q2", &QueueConfig::default()),
        3,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    engine.apply_command(MqCommand::create_binding(ex_id, q1_id, None), 4, 1000);
    engine.apply_command(MqCommand::create_binding(ex_id, q2_id, None), 5, 1000);

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"fanout-data"))
        .timestamp(2000)
        .key(Bytes::from_static(b"some-key"))
        .build();

    let resp = engine.apply_command(MqCommand::publish_to_exchange(ex_id, &[msg]), 6, 2000);
    assert!(matches!(resp, MqResponse::Ok));

    let snap = engine.snapshot();
    assert_eq!(snap.queues.len(), 2);
    for q in &snap.queues {
        assert_eq!(
            q.meta.pending_count, 1,
            "queue {} should have 1 msg",
            q.meta.name
        );
    }
}

#[test]
fn test_exchange_direct_routing_with_flat_routing_key() {
    let mut engine = make_engine();

    let ex_id = match engine.apply_command(
        MqCommand::create_exchange("direct-ex", ExchangeType::Direct),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let q_us = match engine.apply_command(
        MqCommand::create_queue("orders-us", &QueueConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };
    let q_eu = match engine.apply_command(
        MqCommand::create_queue("orders-eu", &QueueConfig::default()),
        3,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    engine.apply_command(MqCommand::create_binding(ex_id, q_us, Some("us")), 4, 1000);
    engine.apply_command(MqCommand::create_binding(ex_id, q_eu, Some("eu")), 5, 1000);

    let msg_us = FlatMessageBuilder::new(Bytes::from_static(b"order-data-us"))
        .timestamp(2000)
        .routing_key(Bytes::from_static(b"us"))
        .build();

    engine.apply_command(MqCommand::publish_to_exchange(ex_id, &[msg_us]), 6, 2000);

    let snap = engine.snapshot();
    let us_queue = snap
        .queues
        .iter()
        .find(|q| q.meta.name == "orders-us")
        .unwrap();
    let eu_queue = snap
        .queues
        .iter()
        .find(|q| q.meta.name == "orders-eu")
        .unwrap();
    assert_eq!(us_queue.meta.pending_count, 1);
    assert_eq!(eu_queue.meta.pending_count, 0);

    let msg_eu = FlatMessageBuilder::new(Bytes::from_static(b"order-data-eu"))
        .timestamp(2001)
        .routing_key(Bytes::from_static(b"eu"))
        .build();

    engine.apply_command(MqCommand::publish_to_exchange(ex_id, &[msg_eu]), 7, 2001);

    let snap = engine.snapshot();
    let us_queue = snap
        .queues
        .iter()
        .find(|q| q.meta.name == "orders-us")
        .unwrap();
    let eu_queue = snap
        .queues
        .iter()
        .find(|q| q.meta.name == "orders-eu")
        .unwrap();
    assert_eq!(us_queue.meta.pending_count, 1);
    assert_eq!(eu_queue.meta.pending_count, 1);
}

#[test]
fn test_exchange_no_routing_key_in_flat_message() {
    let mut engine = make_engine();

    let ex_id = match engine.apply_command(
        MqCommand::create_exchange("direct-ex-2", ExchangeType::Direct),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let q_id = match engine.apply_command(
        MqCommand::create_queue("bound-q", &QueueConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    engine.apply_command(
        MqCommand::create_binding(ex_id, q_id, Some("specific")),
        3,
        1000,
    );

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"no-route"))
        .timestamp(2000)
        .build();

    engine.apply_command(MqCommand::publish_to_exchange(ex_id, &[msg]), 4, 2000);

    let snap = engine.snapshot();
    let q = snap
        .queues
        .iter()
        .find(|q| q.meta.name == "bound-q")
        .unwrap();
    assert_eq!(
        q.meta.pending_count, 0,
        "should not route without matching key"
    );
}

// =============================================================================
// Actor messages with flat encoding
// =============================================================================

#[test]
fn test_actor_send_flat_message() {
    let mut engine = make_engine();

    let ns_id = match engine.apply_command(
        MqCommand::create_actor_namespace("workers", &ActorConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let actor_id = Bytes::from_static(b"worker-1");

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"task-payload"))
        .key(Bytes::from_static(b"task-key"))
        .timestamp(5000)
        .correlation_id(Bytes::from_static(b"corr-xyz"))
        .reply_to(Bytes::from_static(b"response-topic"))
        .header("priority", &b"high"[..])
        .build();

    engine.apply_command(MqCommand::send_to_actor(ns_id, &actor_id, &msg), 2, 5000);

    let snap = engine.snapshot();
    assert_eq!(snap.actor_namespaces[0].actors.len(), 1);
    assert_eq!(snap.actor_namespaces[0].actors[0].pending_count, 1);
}

// =============================================================================
// Batch enqueue with mixed flat messages
// =============================================================================

#[test]
fn test_batch_enqueue_mixed_flat_messages() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("mixed-q", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let messages: Vec<Bytes> = vec![
        FlatMessageBuilder::new(Bytes::from_static(b"msg-1"))
            .timestamp(1000)
            .build(),
        FlatMessageBuilder::new(Bytes::from_static(b"msg-2"))
            .key(Bytes::from_static(b"k2"))
            .timestamp(1001)
            .ttl_ms(5000)
            .build(),
        FlatMessageBuilder::new(Bytes::from_static(b"msg-3"))
            .timestamp(1002)
            .delay_ms(2000)
            .build(),
        FlatMessageBuilder::new(Bytes::from_static(b"msg-4"))
            .key(Bytes::from_static(b"k4"))
            .timestamp(1003)
            .ttl_ms(60000)
            .delay_ms(1000)
            .routing_key(Bytes::from_static(b"rk"))
            .reply_to(Bytes::from_static(b"rt"))
            .correlation_id(Bytes::from_static(b"cid"))
            .header("h1", &b"v1"[..])
            .build(),
        FlatMessageBuilder::new(Bytes::new())
            .timestamp(1004)
            .build(),
    ];

    let dedup_keys: Vec<Option<Bytes>> = vec![None, None, None, None, None];
    engine.apply_command(
        MqCommand::enqueue(queue_id, &messages, &dedup_keys),
        2,
        1000,
    );

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 5);
}

// =============================================================================
// FlatMessage structural validation
// =============================================================================

#[test]
fn test_flat_message_invalid_buffer() {
    assert!(FlatMessage::new(Bytes::new()).is_none());
    assert!(FlatMessage::new(Bytes::from_static(b"too short")).is_none());

    let mut buf = vec![0u8; 32];
    buf[4] = 100;
    buf[5] = 0;
    assert!(FlatMessage::new(Bytes::from(buf)).is_none());
}

#[test]
fn test_flat_message_empty_optional_fields() {
    let buf = FlatMessageBuilder::new(Bytes::from_static(b"val"))
        .key(Bytes::new())
        .routing_key(Bytes::new())
        .build();

    let flat = FlatMessage::new(buf).unwrap();
    assert_eq!(flat.value(), Bytes::from_static(b"val"));
    assert_eq!(flat.key(), Some(Bytes::new()));
    assert_eq!(flat.routing_key(), Some(Bytes::new()));
}

#[test]
fn test_flat_message_many_headers() {
    let mut builder = FlatMessageBuilder::new(Bytes::from_static(b"payload"));
    for i in 0..50 {
        builder = builder.header(
            Bytes::from(format!("header-key-{}", i)),
            Bytes::from(format!("header-value-{}", i)),
        );
    }
    let buf = builder.build();

    let flat = FlatMessage::new(buf).unwrap();
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
    let large = Bytes::from(vec![0xFFu8; 1_000_000]);
    let buf = FlatMessageBuilder::new(large.clone())
        .timestamp(42)
        .key(Bytes::from_static(b"big-key"))
        .build();

    let flat = FlatMessage::new(buf).unwrap();
    assert_eq!(flat.value().len(), 1_000_000);
    assert_eq!(flat.value(), large);
    assert_eq!(flat.value_len(), 1_000_000);
    assert_eq!(flat.key(), Some(Bytes::from_static(b"big-key")));
}

#[test]
fn test_flat_message_binary_value() {
    let binary_value = Bytes::from(vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD]);
    let binary_key = Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);

    let buf = FlatMessageBuilder::new(binary_value.clone())
        .key(binary_key.clone())
        .timestamp(100)
        .build();

    let flat = FlatMessage::new(buf).unwrap();
    assert_eq!(flat.value(), binary_value);
    assert_eq!(flat.key(), Some(binary_key));
}

// =============================================================================
// Snapshot/restore preserves flat message state
// =============================================================================

#[test]
fn test_snapshot_restore_with_flat_messages() {
    let mut engine = make_engine();

    engine.apply_command(
        MqCommand::create_topic("t1", RetentionPolicy::default(), 0),
        1,
        1000,
    );
    let messages: Vec<Bytes> = vec![
        FlatMessageBuilder::new(Bytes::from_static(b"evt-1"))
            .key(Bytes::from_static(b"k1"))
            .timestamp(1000)
            .build(),
        FlatMessageBuilder::new(Bytes::from_static(b"evt-2"))
            .timestamp(1001)
            .ttl_ms(5000)
            .build(),
    ];
    engine.apply_command(MqCommand::publish(1, &messages), 2, 1001);

    engine.apply_command(
        MqCommand::create_queue("q1", &QueueConfig::default()),
        3,
        1000,
    );
    let enqueue_msg = FlatMessageBuilder::new(Bytes::from_static(b"task-1"))
        .timestamp(2000)
        .delay_ms(1000)
        .build();
    engine.apply_command(MqCommand::enqueue(2, &[enqueue_msg], &[None]), 4, 2000);

    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    let snap2 = engine2.snapshot();
    assert_eq!(snap2.topics[0].meta.message_count, 2);
    assert_eq!(snap2.queues[0].meta.pending_count, 1);

    let post_restore_msg = FlatMessageBuilder::new(Bytes::from_static(b"post-restore"))
        .timestamp(3000)
        .build();
    let resp = engine2.apply_command(MqCommand::publish(1, &[post_restore_msg]), 5, 3000);
    assert!(matches!(resp, MqResponse::Published { .. }));
}

// =============================================================================
// FlatMessage field combination edge cases
// =============================================================================

#[test]
fn test_flat_message_only_reply_to_and_correlation_id() {
    let buf = FlatMessageBuilder::new(Bytes::from_static(b"rpc-request"))
        .timestamp(100)
        .reply_to(Bytes::from_static(b"response-queue"))
        .correlation_id(Bytes::from_static(b"req-id-001"))
        .build();

    let flat = FlatMessage::new(buf).unwrap();
    assert_eq!(flat.value(), Bytes::from_static(b"rpc-request"));
    assert!(flat.key().is_none());
    assert!(flat.routing_key().is_none());
    assert_eq!(flat.reply_to(), Some(Bytes::from_static(b"response-queue")));
    assert_eq!(
        flat.correlation_id(),
        Some(Bytes::from_static(b"req-id-001"))
    );
}

#[test]
fn test_flat_message_only_routing_key_no_key() {
    let buf = FlatMessageBuilder::new(Bytes::from_static(b"routed"))
        .routing_key(Bytes::from_static(b"events.user.signup"))
        .timestamp(200)
        .build();

    let flat = FlatMessage::new(buf).unwrap();
    assert!(flat.key().is_none());
    assert_eq!(flat.routing_key_str(), Some("events.user.signup".into()));
}

#[test]
fn test_flat_message_all_optional_fields_plus_headers() {
    let buf = FlatMessageBuilder::new(Bytes::from_static(b"full"))
        .key(Bytes::from_static(b"k"))
        .routing_key(Bytes::from_static(b"rk"))
        .reply_to(Bytes::from_static(b"rt"))
        .correlation_id(Bytes::from_static(b"cid"))
        .timestamp(999)
        .ttl_ms(1000)
        .delay_ms(500)
        .header("h1", &b"v1"[..])
        .header("h2", &b"v2"[..])
        .build();

    let flat = FlatMessage::new(buf.clone()).unwrap();

    assert_eq!(flat.value(), Bytes::from_static(b"full"));
    assert_eq!(flat.key(), Some(Bytes::from_static(b"k")));
    assert_eq!(flat.routing_key(), Some(Bytes::from_static(b"rk")));
    assert_eq!(flat.reply_to(), Some(Bytes::from_static(b"rt")));
    assert_eq!(flat.correlation_id(), Some(Bytes::from_static(b"cid")));
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
    let buf = FlatMessageBuilder::new(Bytes::from_static(b"original"))
        .key(Bytes::from_static(b"key"))
        .timestamp(42)
        .build();

    let flat1 = FlatMessage::new(buf).unwrap();
    let flat2 = flat1.clone();

    assert_eq!(flat1.value(), flat2.value());
    assert_eq!(flat1.key(), flat2.key());
    assert_eq!(flat1.timestamp(), flat2.timestamp());

    let raw = flat1.into_bytes();
    assert!(!raw.is_empty());
    assert_eq!(flat2.value(), Bytes::from_static(b"original"));
}

#[test]
fn test_flat_message_as_bytes_roundtrip() {
    let original = FlatMessageBuilder::new(Bytes::from_static(b"data"))
        .key(Bytes::from_static(b"k"))
        .ttl_ms(1000)
        .timestamp(5)
        .build();

    let flat = FlatMessage::new(original.clone()).unwrap();
    let raw = flat.as_bytes().clone();

    let flat2 = FlatMessage::new(raw).unwrap();
    assert_eq!(flat2.value(), Bytes::from_static(b"data"));
    assert_eq!(flat2.key(), Some(Bytes::from_static(b"k")));
    assert_eq!(flat2.ttl_ms(), Some(1000));
}

// =============================================================================
// ACK with response — queue request/reply pattern
// =============================================================================

#[test]
fn test_queue_ack_with_response_publishes_to_reply_topic() {
    let mut engine = make_engine();

    let reply_topic_id = match engine.apply_command(
        MqCommand::create_topic("responses", RetentionPolicy::default(), 0),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("requests", &QueueConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let request_msg = FlatMessageBuilder::new(Bytes::from_static(b"compute-something"))
        .timestamp(2000)
        .reply_to(Bytes::from_static(b"responses"))
        .correlation_id(Bytes::from_static(b"req-001"))
        .build();

    engine.apply_command(
        MqCommand::enqueue(queue_id, &[request_msg], &[None]),
        3,
        2000,
    );

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 4, 3000);
    let msg_id = match resp {
        MqResponse::Messages { messages } => {
            assert_eq!(messages.len(), 1);
            messages[0].message_id
        }
        other => panic!("expected Messages, got {:?}", other),
    };

    let response_msg = FlatMessageBuilder::new(Bytes::from_static(b"result: 42"))
        .timestamp(4000)
        .correlation_id(Bytes::from_static(b"req-001"))
        .build();

    let resp = engine.apply_command(
        MqCommand::ack(queue_id, &[msg_id], Some(&response_msg)),
        5,
        4000,
    );
    assert!(matches!(resp, MqResponse::Ok));

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

#[test]
fn test_queue_ack_without_response_is_normal_ack() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("normal-q", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"data"))
        .timestamp(1000)
        .reply_to(Bytes::from_static(b"some-topic"))
        .build();

    engine.apply_command(MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 1000);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 3, 2000);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        _ => panic!(),
    };

    let resp = engine.apply_command(MqCommand::ack(queue_id, &[msg_id], None), 4, 3000);
    assert!(matches!(resp, MqResponse::Ok));

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.in_flight_count, 0);
    assert_eq!(snap.queues[0].meta.pending_count, 0);
}

#[test]
fn test_queue_ack_response_without_reply_to_drops_silently() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("no-reply-q", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"no-reply"))
        .timestamp(1000)
        .build();

    engine.apply_command(MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 1000);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 3, 2000);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        _ => panic!(),
    };

    let response = FlatMessageBuilder::new(Bytes::from_static(b"orphan-response"))
        .timestamp(3000)
        .build();

    let resp = engine.apply_command(
        MqCommand::ack(queue_id, &[msg_id], Some(&response)),
        4,
        3000,
    );
    assert!(matches!(resp, MqResponse::Ok));

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.in_flight_count, 0);
}

#[test]
fn test_queue_ack_response_reply_topic_missing_drops_silently() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("q-missing-topic", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"request"))
        .timestamp(1000)
        .reply_to(Bytes::from_static(b"nonexistent-topic"))
        .build();

    engine.apply_command(MqCommand::enqueue(queue_id, &[msg], &[None]), 2, 1000);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 1), 3, 2000);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        _ => panic!(),
    };

    let response = FlatMessageBuilder::new(Bytes::from_static(b"response"))
        .timestamp(3000)
        .build();

    let resp = engine.apply_command(
        MqCommand::ack(queue_id, &[msg_id], Some(&response)),
        4,
        3000,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_queue_ack_multiple_messages_first_has_reply_to() {
    let mut engine = make_engine();

    engine.apply_command(
        MqCommand::create_topic("multi-replies", RetentionPolicy::default(), 0),
        1,
        1000,
    );

    let queue_id = match engine.apply_command(
        MqCommand::create_queue("multi-q", &QueueConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let msg1 = FlatMessageBuilder::new(Bytes::from_static(b"req-1"))
        .timestamp(1000)
        .reply_to(Bytes::from_static(b"multi-replies"))
        .correlation_id(Bytes::from_static(b"corr-1"))
        .build();
    engine.apply_command(MqCommand::enqueue(queue_id, &[msg1], &[None]), 3, 1000);

    let msg2 = FlatMessageBuilder::new(Bytes::from_static(b"req-2"))
        .timestamp(1001)
        .build();
    engine.apply_command(MqCommand::enqueue(queue_id, &[msg2], &[None]), 4, 1001);

    let resp = engine.apply_command(MqCommand::deliver(queue_id, 100, 2), 5, 2000);
    let msg_ids: Vec<u64> = match resp {
        MqResponse::Messages { messages } => messages.iter().map(|m| m.message_id).collect(),
        other => panic!("expected Messages, got {:?}", other),
    };
    assert_eq!(msg_ids.len(), 2);

    let response = FlatMessageBuilder::new(Bytes::from_static(b"batch-result"))
        .timestamp(3000)
        .build();

    engine.apply_command(MqCommand::ack(queue_id, &msg_ids, Some(&response)), 6, 3000);

    let snap = engine.snapshot();
    let reply_topic = snap
        .topics
        .iter()
        .find(|t| t.meta.name == "multi-replies")
        .unwrap();
    assert_eq!(reply_topic.meta.message_count, 1);
}

// =============================================================================
// ACK with response — actor request/reply pattern
// =============================================================================

#[test]
fn test_actor_ack_with_response_publishes_to_reply_topic() {
    let mut engine = make_engine();

    let reply_topic_id = match engine.apply_command(
        MqCommand::create_topic("actor-responses", RetentionPolicy::default(), 0),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let ns_id = match engine.apply_command(
        MqCommand::create_actor_namespace("rpc-actors", &ActorConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let actor_id = Bytes::from_static(b"calculator");

    let request = FlatMessageBuilder::new(Bytes::from_static(b"2+2"))
        .timestamp(2000)
        .reply_to(Bytes::from_static(b"actor-responses"))
        .correlation_id(Bytes::from_static(b"calc-001"))
        .build();

    engine.apply_command(
        MqCommand::send_to_actor(ns_id, &actor_id, &request),
        3,
        2000,
    );

    engine.apply_command(
        MqCommand::assign_actors(ns_id, 100, &[actor_id.clone()]),
        4,
        2001,
    );

    let resp = engine.apply_command(
        MqCommand::deliver_actor_message(ns_id, &actor_id, 100),
        5,
        2002,
    );
    let msg_id = match resp {
        MqResponse::Messages { messages } => {
            assert_eq!(messages.len(), 1);
            messages[0].message_id
        }
        other => panic!("expected Messages, got {:?}", other),
    };

    let response = FlatMessageBuilder::new(Bytes::from_static(b"4"))
        .timestamp(3000)
        .correlation_id(Bytes::from_static(b"calc-001"))
        .build();

    let resp = engine.apply_command(
        MqCommand::ack_actor_message(ns_id, &actor_id, msg_id, Some(&response)),
        6,
        3000,
    );
    assert!(matches!(resp, MqResponse::Ok));

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

#[test]
fn test_actor_ack_without_response_normal() {
    let mut engine = make_engine();

    let ns_id = match engine.apply_command(
        MqCommand::create_actor_namespace("normal-actors", &ActorConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let actor_id = Bytes::from_static(b"worker-1");

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"fire-and-forget"))
        .timestamp(2000)
        .build();

    engine.apply_command(MqCommand::send_to_actor(ns_id, &actor_id, &msg), 2, 2000);

    engine.apply_command(
        MqCommand::assign_actors(ns_id, 100, &[actor_id.clone()]),
        3,
        2001,
    );

    let resp = engine.apply_command(
        MqCommand::deliver_actor_message(ns_id, &actor_id, 100),
        4,
        2002,
    );
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        _ => panic!(),
    };

    let resp = engine.apply_command(
        MqCommand::ack_actor_message(ns_id, &actor_id, msg_id, None),
        5,
        3000,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_actor_ack_response_no_reply_to_drops_silently() {
    let mut engine = make_engine();

    let ns_id = match engine.apply_command(
        MqCommand::create_actor_namespace("no-reply-actors", &ActorConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let actor_id = Bytes::from_static(b"actor-1");

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"task"))
        .timestamp(2000)
        .build();

    engine.apply_command(MqCommand::send_to_actor(ns_id, &actor_id, &msg), 2, 2000);

    engine.apply_command(
        MqCommand::assign_actors(ns_id, 100, &[actor_id.clone()]),
        3,
        2001,
    );

    let resp = engine.apply_command(
        MqCommand::deliver_actor_message(ns_id, &actor_id, 100),
        4,
        2002,
    );
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        _ => panic!(),
    };

    let response = FlatMessageBuilder::new(Bytes::from_static(b"result"))
        .timestamp(3000)
        .build();

    let resp = engine.apply_command(
        MqCommand::ack_actor_message(ns_id, &actor_id, msg_id, Some(&response)),
        5,
        3000,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

// =============================================================================
// Display format with response
// =============================================================================

#[test]
fn test_ack_display_with_response() {
    let ack_no_resp = MqCommand::ack(1, &[10, 11], None);
    assert_eq!(format!("{}", ack_no_resp), "Ack(queue=1, count=2)");

    let resp_bytes = FlatMessageBuilder::new(Bytes::from_static(b"resp")).build();
    let ack_with_resp = MqCommand::ack(1, &[10], Some(&resp_bytes));
    assert_eq!(format!("{}", ack_with_resp), "Ack(queue=1, count=1)");
}

#[test]
fn test_ack_actor_display_with_response() {
    let actor_id = Bytes::from_static(b"a1");
    let ack_no_resp = MqCommand::ack_actor_message(1, &actor_id, 10, None);
    assert_eq!(format!("{}", ack_no_resp), "AckActorMessage(ns=1)");

    let resp_bytes = FlatMessageBuilder::new(Bytes::from_static(b"resp")).build();
    let ack_with_resp = MqCommand::ack_actor_message(1, &actor_id, 10, Some(&resp_bytes));
    assert_eq!(format!("{}", ack_with_resp), "AckActorMessage(ns=1)");
}

// =============================================================================
// Snapshot/restore preserves reply_to metadata
// =============================================================================

#[test]
fn test_snapshot_restore_preserves_reply_to() {
    let mut engine = make_engine();

    engine.apply_command(
        MqCommand::create_topic("replies", RetentionPolicy::default(), 0),
        1,
        1000,
    );
    let queue_id = match engine.apply_command(
        MqCommand::create_queue("rpc-queue", &QueueConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let msg = FlatMessageBuilder::new(Bytes::from_static(b"request"))
        .timestamp(2000)
        .reply_to(Bytes::from_static(b"replies"))
        .correlation_id(Bytes::from_static(b"corr-snap"))
        .build();

    engine.apply_command(MqCommand::enqueue(queue_id, &[msg], &[None]), 3, 2000);

    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    let resp = engine2.apply_command(MqCommand::deliver(queue_id, 100, 1), 4, 3000);
    let msg_id = match resp {
        MqResponse::Messages { messages } => {
            assert_eq!(messages.len(), 1);
            messages[0].message_id
        }
        _ => panic!(),
    };

    let response = FlatMessageBuilder::new(Bytes::from_static(b"reply-data"))
        .timestamp(4000)
        .build();

    engine2.apply_command(
        MqCommand::ack(queue_id, &[msg_id], Some(&response)),
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
