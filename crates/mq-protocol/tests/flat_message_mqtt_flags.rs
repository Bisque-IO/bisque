//! Tests for MQTT-native FlatMessage flag bits and publisher_id field.
//!
//! Covers Optimization 5 (message-level flags) and Optimization 8 (publisher ID
//! in fixed header) from the MQTT optimization plan.

use bisque_mq_protocol::flat::{FlatMessage, FlatMessageBuilder, FlatMessageMeta};

// =============================================================================
// Optimization 5: Message-Level Flag Bits
// =============================================================================

#[test]
fn test_retain_flag_set() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .retain(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_retain());
}

#[test]
fn test_retain_flag_not_set_by_default() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(!flat.is_retain());
}

#[test]
fn test_retain_false_explicit() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .retain(false)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(!flat.is_retain());
}

#[test]
fn test_no_local_flag_set() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .no_local(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_no_local());
}

#[test]
fn test_no_local_flag_not_set_by_default() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(!flat.is_no_local());
}

#[test]
fn test_utf8_payload_flag_set() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_utf8_payload());
}

#[test]
fn test_utf8_payload_flag_not_set_by_default() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(!flat.is_utf8_payload());
}

#[test]
fn test_all_mqtt_flags_combined() {
    let msg = FlatMessageBuilder::new(&b"data"[..])
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
fn test_mqtt_flags_dont_affect_value() {
    let msg = FlatMessageBuilder::new(&b"my-payload"[..])
        .timestamp(5000)
        .retain(true)
        .no_local(true)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.value(), b"my-payload");
}

#[test]
fn test_mqtt_flags_with_key() {
    let msg = FlatMessageBuilder::new(&b"value"[..])
        .timestamp(1000)
        .key(&b"partition-key"[..])
        .retain(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.key().unwrap(), b"partition-key");
    assert_eq!(flat.value(), b"value");
    assert!(flat.is_retain());
}

#[test]
fn test_mqtt_flags_with_routing_key() {
    let msg = FlatMessageBuilder::new(&b"sensor-data"[..])
        .timestamp(1000)
        .routing_key(&b"sensors/temp/room1"[..])
        .retain(true)
        .no_local(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.routing_key().unwrap(), b"sensors/temp/room1");
    assert!(flat.is_retain());
    assert!(flat.is_no_local());
}

#[test]
fn test_mqtt_flags_with_ttl_and_delay() {
    let msg = FlatMessageBuilder::new(&b"data"[..])
        .timestamp(1000)
        .ttl(30000)
        .delay(5000)
        .retain(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.meta().ttl_ms, 30000);
    assert_eq!(flat.meta().delay_ms, 5000);
    assert!(flat.is_retain());
}

#[test]
fn test_mqtt_flags_with_headers() {
    let msg = FlatMessageBuilder::new(&b"data"[..])
        .timestamp(1000)
        .retain(true)
        .header(&b"content-type"[..], &b"application/json"[..])
        .header(&b"x-custom"[..], &b"value"[..])
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_retain());
    assert_eq!(flat.header_count(), 2);
    let (k, v) = flat.header(0);
    assert_eq!(k, b"content-type");
    assert_eq!(v, b"application/json");
}

#[test]
fn test_mqtt_flags_with_reply_to_and_correlation_id() {
    let msg = FlatMessageBuilder::new(&b"request"[..])
        .timestamp(1000)
        .reply_to(&b"response/topic"[..])
        .correlation_id(&b"req-12345"[..])
        .no_local(true)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.reply_to().unwrap(), b"response/topic");
    assert_eq!(flat.correlation_id().unwrap(), b"req-12345");
    assert!(flat.is_no_local());
    assert!(flat.is_utf8_payload());
}

#[test]
fn test_existing_flags_preserved_with_mqtt_flags() {
    // Verify HAS_KEY, HAS_TTL, HAS_DELAY, HAS_ROUTING_KEY, HAS_REPLY_TO,
    // HAS_CORRELATION_ID all work when MQTT flags are also set.
    let msg = FlatMessageBuilder::new(&b"full"[..])
        .timestamp(9999)
        .key(&b"k"[..])
        .routing_key(&b"rk"[..])
        .reply_to(&b"rt"[..])
        .correlation_id(&b"ci"[..])
        .ttl(1000)
        .delay(2000)
        .retain(true)
        .no_local(true)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();

    assert_eq!(flat.value(), b"full");
    assert_eq!(flat.key().unwrap(), b"k");
    assert_eq!(flat.routing_key().unwrap(), b"rk");
    assert_eq!(flat.reply_to().unwrap(), b"rt");
    assert_eq!(flat.correlation_id().unwrap(), b"ci");
    assert_eq!(flat.meta().ttl_ms, 1000);
    assert_eq!(flat.meta().delay_ms, 2000);
    assert_eq!(flat.meta().timestamp, 9999);
    assert!(flat.is_retain());
    assert!(flat.is_no_local());
    assert!(flat.is_utf8_payload());
}

#[test]
fn test_meta_parse_flags() {
    let msg = FlatMessageBuilder::new(&b"data"[..])
        .timestamp(1000)
        .retain(true)
        .no_local(true)
        .build();
    let meta = FlatMessageMeta::parse(&msg).unwrap();
    // Verify the flags field has the correct bits set
    assert_ne!(meta.flags & (1 << 6), 0, "FLAG_RETAIN bit should be set");
    assert_ne!(meta.flags & (1 << 7), 0, "FLAG_NO_LOCAL bit should be set");
    assert_eq!(
        meta.flags & (1 << 8),
        0,
        "FLAG_UTF8_PAYLOAD bit should NOT be set"
    );
}

// =============================================================================
// Optimization 8: Publisher ID in Fixed Header
// =============================================================================

#[test]
fn test_publisher_id_basic() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .publisher_id(42)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), 42);
}

#[test]
fn test_publisher_id_zero_by_default() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), 0);
}

#[test]
fn test_publisher_id_max_u64() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .publisher_id(u64::MAX)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), u64::MAX);
}

#[test]
fn test_publisher_id_one() {
    let msg = FlatMessageBuilder::new(&b"hello"[..])
        .timestamp(1000)
        .publisher_id(1)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), 1);
}

#[test]
fn test_publisher_id_with_value_and_key() {
    let msg = FlatMessageBuilder::new(&b"payload"[..])
        .timestamp(1000)
        .key(&b"key"[..])
        .publisher_id(12345)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), 12345);
    assert_eq!(flat.value(), b"payload");
    assert_eq!(flat.key().unwrap(), b"key");
}

#[test]
fn test_publisher_id_with_all_optional_fields() {
    let msg = FlatMessageBuilder::new(&b"full-payload"[..])
        .timestamp(9999)
        .key(&b"pk"[..])
        .routing_key(&b"a/b/c"[..])
        .reply_to(&b"reply"[..])
        .correlation_id(&b"corr"[..])
        .ttl(60000)
        .delay(5000)
        .publisher_id(999)
        .header(&b"h1"[..], &b"v1"[..])
        .header(&b"h2"[..], &b"v2"[..])
        .header(&b"h3"[..], &b"v3"[..])
        .build();
    let flat = FlatMessage::new(&msg).unwrap();

    assert_eq!(flat.value(), b"full-payload");
    assert_eq!(flat.key().unwrap(), b"pk");
    assert_eq!(flat.routing_key().unwrap(), b"a/b/c");
    assert_eq!(flat.reply_to().unwrap(), b"reply");
    assert_eq!(flat.correlation_id().unwrap(), b"corr");
    assert_eq!(flat.meta().ttl_ms, 60000);
    assert_eq!(flat.meta().delay_ms, 5000);
    assert_eq!(flat.meta().timestamp, 9999);
    assert_eq!(flat.publisher_id(), 999);
    assert_eq!(flat.header_count(), 3);
}

#[test]
fn test_publisher_id_with_mqtt_flags() {
    let msg = FlatMessageBuilder::new(&b"data"[..])
        .timestamp(1000)
        .publisher_id(777)
        .retain(true)
        .no_local(true)
        .utf8_payload(true)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.publisher_id(), 777);
    assert!(flat.is_retain());
    assert!(flat.is_no_local());
    assert!(flat.is_utf8_payload());
}

#[test]
fn test_publisher_id_meta_parse() {
    let msg = FlatMessageBuilder::new(&b"data"[..])
        .timestamp(1000)
        .publisher_id(555)
        .build();
    let meta = FlatMessageMeta::parse(&msg).unwrap();
    assert_eq!(meta.publisher_id, 555);
    assert_ne!(
        meta.flags & (1 << 9),
        0,
        "FLAG_HAS_PUBLISHER_ID should be set"
    );
}

#[test]
fn test_publisher_id_zero_does_not_set_flag() {
    let msg = FlatMessageBuilder::new(&b"data"[..])
        .timestamp(1000)
        .publisher_id(0)
        .build();
    let meta = FlatMessageMeta::parse(&msg).unwrap();
    assert_eq!(meta.publisher_id, 0);
    // publisher_id(0) should NOT set FLAG_HAS_PUBLISHER_ID
    assert_eq!(
        meta.flags & (1 << 9),
        0,
        "FLAG_HAS_PUBLISHER_ID should NOT be set for 0"
    );
}

#[test]
fn test_publisher_id_zero_copy_value() {
    let msg = FlatMessageBuilder::new(&b"zero-copy-test"[..])
        .timestamp(1000)
        .publisher_id(1)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    let value = flat.value();
    assert_eq!(value, b"zero-copy-test");
    assert_eq!(value.len(), 14);
}

// =============================================================================
// Combined: Full MQTT Message Round-Trip
// =============================================================================

#[test]
fn test_full_mqtt_message_roundtrip() {
    // Simulate a complete MQTT PUBLISH message encoded as FlatMessage
    let msg = FlatMessageBuilder::new(&b"{\"temp\": 22.5}"[..])
        .timestamp(1709251200000) // 2024-03-01
        .routing_key(&b"sensors/temp/room1"[..])
        .ttl(300000) // 5 minutes
        .publisher_id(42)
        .retain(true)
        .utf8_payload(true)
        .header(&b"content-type"[..], &b"application/json"[..])
        .build();

    let flat = FlatMessage::new(&msg).unwrap();

    // Verify all MQTT-relevant fields
    assert_eq!(flat.value(), b"{\"temp\": 22.5}");
    assert_eq!(flat.routing_key().unwrap(), b"sensors/temp/room1");
    assert_eq!(flat.meta().ttl_ms, 300000);
    assert_eq!(flat.meta().timestamp, 1709251200000);
    assert_eq!(flat.publisher_id(), 42);
    assert!(flat.is_retain());
    assert!(flat.is_utf8_payload());
    assert!(!flat.is_no_local());
    assert_eq!(flat.header_count(), 1);
    let (k, v) = flat.header(0);
    assert_eq!(k, b"content-type");
    assert_eq!(v, b"application/json");

    // Verify via meta parse
    let meta = FlatMessageMeta::parse(&msg).unwrap();
    assert_eq!(meta.publisher_id, 42);
    assert_eq!(meta.ttl_ms, 300000);
    assert_eq!(meta.timestamp, 1709251200000);
}

#[test]
fn test_minimal_message_no_mqtt_fields() {
    // Ensure minimal messages still work with new header size
    let msg = FlatMessageBuilder::new(&b"x"[..]).build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.value(), b"x");
    assert_eq!(flat.publisher_id(), 0);
    assert!(!flat.is_retain());
    assert!(!flat.is_no_local());
    assert!(!flat.is_utf8_payload());
    assert!(flat.key().is_none());
    assert!(flat.routing_key().is_none());
    assert!(flat.reply_to().is_none());
    assert!(flat.correlation_id().is_none());
    assert_eq!(flat.header_count(), 0);
}

#[test]
fn test_empty_value_with_mqtt_flags() {
    let msg = FlatMessageBuilder::new(&b""[..])
        .timestamp(1000)
        .retain(true)
        .publisher_id(1)
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.value().len(), 0);
    assert!(flat.is_retain());
    assert_eq!(flat.publisher_id(), 1);
}

#[test]
fn test_large_payload_with_mqtt_flags() {
    let large_payload = vec![0xAB; 1024 * 1024]; // 1MB
    let msg = FlatMessageBuilder::new(&large_payload[..])
        .timestamp(1000)
        .retain(true)
        .publisher_id(u64::MAX)
        .routing_key(&b"large/topic"[..])
        .build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert_eq!(flat.value().len(), 1024 * 1024);
    assert!(flat.is_retain());
    assert_eq!(flat.publisher_id(), u64::MAX);
    assert_eq!(flat.routing_key().unwrap(), b"large/topic");
}

#[test]
fn test_many_headers_with_mqtt_flags() {
    let hdr_keys: Vec<Vec<u8>> = (0..50).map(|i| format!("hdr-{}", i).into_bytes()).collect();
    let hdr_vals: Vec<Vec<u8>> = (0..50).map(|i| format!("val-{}", i).into_bytes()).collect();

    let mut builder = FlatMessageBuilder::new(&b"data"[..])
        .timestamp(1000)
        .retain(true)
        .publisher_id(42);

    for i in 0..50 {
        builder = builder.header(&hdr_keys[i], &hdr_vals[i]);
    }

    let msg = builder.build();
    let flat = FlatMessage::new(&msg).unwrap();
    assert!(flat.is_retain());
    assert_eq!(flat.publisher_id(), 42);
    assert_eq!(flat.header_count(), 50);

    // Spot-check a few headers
    let (k0, v0) = flat.header(0);
    assert_eq!(k0, b"hdr-0");
    assert_eq!(v0, b"val-0");
    let (k49, v49) = flat.header(49);
    assert_eq!(k49, b"hdr-49");
    assert_eq!(v49, b"val-49");
}
