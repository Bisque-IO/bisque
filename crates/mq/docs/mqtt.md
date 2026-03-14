# Native MQTT Passthrough — MqttEnvelope Format

## Context

MQTT inbound currently converts wire bytes to FlatMessage format:
decode MQTT → extract fields → FlatMessageBuilder::build() → store Bytes.
FlatMessageBuilder::build() costs ~60ns/msg (BytesMut alloc + span index write + data copies).
This is the dominant cost after our optimizations — both native and MQTT pay it.

Current benchmarks:
- Native: ~10M msgs/sec (100ns/msg)
- MQTT 3.1.1 QoS0: ~6M msgs/sec (167ns/msg)
- Delta: ~67ns/msg (of which ~60ns is shared FlatMessageBuilder cost)

If MQTT bypasses FlatMessageBuilder, the gap closes to ~7ns (~MQTT decode overhead only).

## Approach: MqttEnvelope format

A compact 40-byte header + raw MQTT data sections, stored alongside FlatMessage in the same
log. A single discriminator bit (flags bit 15) tells the engine which format to parse.

### Wire layout

```
[flags: u16]           bit 15 = FORMAT_MQTT (always 0 in FlatMessage)
                       bit 0 = RETAIN, bit 1 = IS_V5, bit 2 = HAS_TTL
[_reserved: u16]
[topic_len: u16]       routing key length
[props_len: u16]       raw MQTT 5.0 property bytes length (0 for V3.1.1)
[timestamp: u64]
[ttl_ms: u64]
[publisher_id: u64]
[_padding: u64]        future use (delay_ms etc.)
--- 40 bytes (= HEADER_SIZE, same alignment as FlatMessage) ---
[topic: topic_len]     raw UTF-8 topic string (= routing key)
[properties: props_len] raw MQTT 5.0 property wire bytes (post varint-prefix)
[payload: remaining]   message payload
```

### Cost comparison (inbound, per message)

| Operation | FlatMessage | MqttEnvelope |
|-----------|------------|--------------|
| Allocate BytesMut | ~15ns | ~15ns (same) |
| Write fixed header | ~5ns | ~5ns (same) |
| Span index (2-pass) | ~20ns | **0** (no spans) |
| Property decomposition | ~15ns | **0** (raw passthrough) |
| Data copy | ~5ns | ~5ns (same, 3 put_slice) |
| **Total build** | **~60ns** | **~25ns** |
| **Savings** | — | **~35ns/msg** |

## Files to modify

### 1. `crates/mq-protocol/src/flat.rs` — Core types

- Add `pub const FLAG_FORMAT_MQTT: u16 = 1 << 15`
- Add `pub fn is_mqtt_envelope(buf: &[u8]) -> bool` (inline, checks bit 15)
- Add `MqttEnvelopeMeta` struct + `parse()` / `value_len()` / `ttl_ms()` / `publisher_id()` statics
- Add `MqttEnvelope` struct (zero-copy view over Bytes):
  - `topic()`, `properties_raw()`, `payload()`, `is_retain()`, `is_v5()`
  - `reply_to()` / `correlation_id()` — lazy scan of raw properties for IDs 0x08/0x09
- Add `MqttEnvelopeBuilder` struct with `build() -> Bytes`:
  - Writes 40-byte header + topic + properties + payload in single BytesMut alloc
  - No span index, no header key/value encoding

### 2. `crates/mq/src/engine.rs:378-385` — Exchange routing

Replace `FlatMessage::new(m).and_then(|f| f.routing_key())` with:
```rust
if is_mqtt_envelope(m) {
    MqttEnvelope::new(m.clone()).map(|e| e.topic())
} else {
    FlatMessage::new(m.clone()).and_then(|f| f.routing_key())
}
```

### 3. `crates/mq/src/consumer_group.rs:346-380` — Enqueue metadata

Gate `FlatMessageMeta::parse()` behind format check. For MqttEnvelope:
- Read publisher_id, ttl_ms, timestamp from envelope header
- Scan `properties_raw()` for reply_to (0x08) / correlation_id (0x09) only when props_len > 0

### 4. `crates/mq/src/topic.rs:681` — Value length accounting

Add envelope path: `buf.len() - 40 - topic_len - props_len`

### 5. `crates/mq-mqtt/src/session.rs:1363-1409` — Inbound hot path

Replace `FlatMessageBuilder` block with `MqttEnvelopeBuilder`:
```rust
let flat_message = MqttEnvelopeBuilder::new(topic, publish.payload.clone())
    .timestamp(now)
    .publisher_id(self.session_id)
    .retain(publish.retain)
    .is_v5(self.protocol_version == ProtocolVersion::V5)
    .properties_raw(if has_props { publish.properties.raw().clone() } else { Bytes::new() })
    .ttl_ms(ttl)
    .build();
```

All 7+ property accessor calls eliminated. No header key/value construction. No span index.

Will messages remain FlatMessage (rare, cross-protocol).

### 6. `crates/mq-mqtt/src/codec.rs` — Outbound encoder

Add `encode_publish_from_envelope()`:
- Write MQTT fixed header (new QoS/DUP/RETAIN per subscriber)
- Write topic from envelope (or alias)
- Write packet_id
- For V5: write properties — copy raw props from envelope, filtering out 0x02/0x0B/0x23,
  then append injected subscription_id + topic_alias + adjusted message_expiry_interval
- Write payload (zero-copy from envelope)

Add `copy_properties_filtered()` helper: single-pass scan of raw property bytes,
copies all properties except filtered IDs. For V3.1.1 (props_len=0): no-op.

### 7. `crates/mq-mqtt/src/server.rs:835-940` — Outbound delivery

Format-aware dispatch in deliver_outbound per-message loop:
```rust
if is_mqtt_envelope(&flat_bytes) {
    // MqttEnvelope path → encode_publish_from_envelope
} else {
    // FlatMessage path → encode_publish_from_flat_with_expiry (unchanged)
}
```

### 8. `crates/mq-mqtt/examples/mqtt_adapter_bench.rs` — Benchmark

Add `bench_inbound_envelope()` variant that uses MqttEnvelopeBuilder.
Compare against FlatMessageBuilder and native baselines.

## Key risks

1. **Outbound property filtering correctness**: The `copy_properties_filtered` helper must
   correctly skip variable-length property encodings. Mitigate with roundtrip tests.

2. **Non-MQTT consumers**: If a native consumer reads MqttEnvelope bytes,
   `FlatMessage::new()` returns `None` (bit 15 set). Existing code handles this with
   `if let Some(flat) = ...` patterns. Document that mixed-protocol topics need
   `MessageFormat::parse()` in the future.

3. **Rolling upgrades**: Old nodes won't recognize MqttEnvelope. Gate behind config flag
   until cluster-wide upgrade.

## Verification

1. `cargo test -p bisque-mq-protocol -p bisque-mq -p bisque-mq-mqtt` — all existing tests pass
2. New unit tests: MqttEnvelope roundtrip (build → parse → extract fields)
3. New unit tests: encode_publish_from_envelope produces identical wire output as encode_publish_from_flat for same message content
4. Benchmark: `cargo run --release -p bisque-mq-mqtt --example mqtt_adapter_bench` — envelope variant shows ~35ns improvement over FlatMessage path
