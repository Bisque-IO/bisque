# Bytes Removal: &[u8] Everywhere on the Write Path

## Problem

Every inbound message pays the Bytes tax twice:

1. **Builder allocates + freezes**: `BytesMut::with_capacity(N)` → write fields →
   `freeze()` → atomic Arc init (~15ns alloc + ~5ns atomic).
2. **MqCommand copies it anyway**: `publish_to_exchange(&[Bytes])` calls
   `extend_from_slice()` on each message, copying bytes into the command buffer.

The `Bytes` wrapper provides zero-copy shared ownership via Arc refcounting.
But on the **write path**, nobody shares — the data flows linearly:

```
build message → store in PublishPlan → copy into MqCommand → drop plan
```

Every `Bytes::clone()`, `Bytes::slice()`, and `freeze()` along this path is
pure waste. The data is temporary and owned by exactly one thing at a time.

**Where Bytes IS justified**: the **read path**. `FlatMessage` and `MqttEnvelope`
wrap mmap-backed raft log segments where multiple consumers slice into a shared
buffer. `RetainedValue` tracks segment provenance for deferred copy-on-unmap.
These use cases genuinely need Arc-backed shared ownership.

## Guiding Principle

```
Write path: &[u8] and &mut BytesMut — zero allocations, zero atomics
Read path:  Bytes (mmap slicing, retained messages, cross-async ownership)
```

## Current Flow (Write Path)

```
MQTT wire bytes
  → decode_publish_to_envelope() → BytesMut        [alloc #1: ~15ns]
  → .freeze()                    → Bytes            [atomic: ~5ns]
  → PublishPlan { flat_message: Bytes }
  → publish_to_exchange(&[Bytes])
     → set_vec_bytes()
        → extend_from_slice()   → copies into cmd   [memcpy]
  → drop PublishPlan             → Arc decrement     [atomic: ~5ns]
```

Total waste per message: **~25ns** (one allocation, two atomics, all pointless).

## Target Flow (Write Path)

```
MQTT wire bytes
  → decode_publish_to_envelope(&mut buf)             [writes into shared buf]
  → PublishPlan { msg_range: Range<usize> }          [indexes into buf]
  → publish_to_exchange_slices(&[&[u8]])
     → extend_from_slice()   → copies into cmd       [memcpy, same as before]
  → buf.clear()                                      [reset, no dealloc]
```

Total overhead per message: **0ns** (zero allocs, zero atomics after warmup).

## Changes

### Phase 1: Writers (no allocation in builders)

Replace `FlatMessageBuilder` and `MqttEnvelopeBuilder` with writers that
append directly into a caller-owned `&mut BytesMut`.

#### MqttEnvelopeWriter

No span index. Pure sequential writes into the caller's buffer.

```rust
pub struct MqttEnvelopeWriter<'a> {
    buf: &'a mut BytesMut,
    start: usize,     // offset where this message begins in buf
}

impl<'a> MqttEnvelopeWriter<'a> {
    pub fn new(buf: &'a mut BytesMut, topic: &[u8], payload: &[u8]) -> Self {
        let start = buf.len();
        let total = MQTT_ENVELOPE_HEADER_SIZE + topic.len() + payload.len();
        buf.reserve(total);

        // Write 40-byte header with FLAG_FORMAT_MQTT, topic_len, zeroed scalars
        let flags = FLAG_FORMAT_MQTT;
        buf.put_u16_le(flags);
        buf.put_u16_le(0);                         // reserved
        buf.put_u16_le(topic.len() as u16);        // topic_len
        buf.put_u16_le(0);                         // props_len
        buf.put_u64_le(0);                         // timestamp
        buf.put_u64_le(0);                         // ttl_ms
        buf.put_u64_le(0);                         // publisher_id
        buf.put_u64_le(0);                         // padding

        // Variable data
        buf.put_slice(topic);
        buf.put_slice(payload);

        Self { buf, start }
    }

    // All setters write directly into buf[start..start+40]. No struct fields.

    pub fn timestamp(self, ts: u64) -> Self {
        let s = self.start;
        self.buf[s+8..s+16].copy_from_slice(&ts.to_le_bytes());
        self
    }

    pub fn publisher_id(self, id: u64) -> Self {
        let s = self.start;
        self.buf[s+24..s+32].copy_from_slice(&id.to_le_bytes());
        self
    }

    pub fn ttl_ms(self, ttl: u64) -> Self {
        let s = self.start;
        if ttl != 0 {
            let flags = u16::from_le_bytes([self.buf[s], self.buf[s+1]]);
            self.buf[s..s+2].copy_from_slice(&(flags | MQTT_FLAG_HAS_TTL).to_le_bytes());
        }
        self.buf[s+16..s+24].copy_from_slice(&ttl.to_le_bytes());
        self
    }

    pub fn retain(self, r: bool) -> Self {
        let s = self.start;
        let flags = u16::from_le_bytes([self.buf[s], self.buf[s+1]]);
        let new = if r { flags | MQTT_FLAG_RETAIN } else { flags & !MQTT_FLAG_RETAIN };
        self.buf[s..s+2].copy_from_slice(&new.to_le_bytes());
        self
    }

    pub fn is_v5(self, v: bool) -> Self {
        let s = self.start;
        let flags = u16::from_le_bytes([self.buf[s], self.buf[s+1]]);
        let new = if v { flags | MQTT_FLAG_IS_V5 } else { flags & !MQTT_FLAG_IS_V5 };
        self.buf[s..s+2].copy_from_slice(&new.to_le_bytes());
        self
    }

    /// Insert raw MQTT 5.0 property bytes between topic and payload.
    pub fn properties_raw(self, props: &[u8]) -> Self {
        if props.is_empty() { return self; }
        let s = self.start;
        let topic_len = u16::from_le_bytes([self.buf[s+4], self.buf[s+5]]) as usize;
        let insert_at = s + MQTT_ENVELOPE_HEADER_SIZE + topic_len;
        let old_len = self.buf.len();
        self.buf.resize(old_len + props.len(), 0);
        self.buf.copy_within(insert_at..old_len, insert_at + props.len());
        self.buf[insert_at..insert_at + props.len()].copy_from_slice(props);
        self.buf[s+6..s+8].copy_from_slice(&(props.len() as u16).to_le_bytes());
        self
    }

    /// Returns start offset. Header is already fully written — no finalize step.
    pub fn finish(self) -> usize {
        self.start
    }
}
```

Caller usage:

```rust
let start = buf.len();
MqttEnvelopeWriter::new(&mut buf, &topic, &payload)
    .timestamp(now)
    .publisher_id(session_id)
    .finish();
let msg: &[u8] = &buf[start..];
```

#### FlatMessageWriter

Span index goes between header and data. We don't know the span count until
all fields are set. Data is written inline as each field is added.

In `finish()`, compute the span count from the recorded field lengths, shift
the data region forward with `copy_within`, and write span entries from the
known lengths. No tracking array needed for optional fields — their lengths
are scalar fields on the writer struct. Headers need per-pair lengths tracked
in a small stack array.

No builder struct needed. All field data is known at call time.
Same pattern as MqCommand: compute total size, single `resize`, then
write everything via direct slice indexing. One capacity check, one
length update, pure memory stores after that.

```rust
/// Write a FlatMessage directly into `buf`. Returns the start offset.
/// Single resize + direct slice writes. No per-field BytesMut overhead.
#[inline]
pub fn write_flat_message(
    buf: &mut BytesMut,
    value: &[u8],
    key: Option<&[u8]>,
    routing_key: Option<&[u8]>,
    reply_to: Option<&[u8]>,
    correlation_id: Option<&[u8]>,
    headers: &[(&[u8], &[u8])],
    timestamp: u64,
    ttl_ms: u64,
    delay_ms: u64,
    publisher_id: u64,
    priority: u8,
    retain: bool,
) -> usize {
    // --- Compute total size ---
    let mut flags: u16 = 0;
    let mut span_count: u16 = 1; // value
    let optional = [key, routing_key, reply_to, correlation_id];
    let opt_flags = [FLAG_HAS_KEY, FLAG_HAS_ROUTING_KEY, FLAG_HAS_REPLY_TO, FLAG_HAS_CORRELATION_ID];
    let mut data_size = value.len();
    for i in 0..4 {
        if let Some(b) = optional[i] {
            flags |= opt_flags[i];
            span_count += 1;
            data_size += b.len();
        }
    }
    if ttl_ms != 0       { flags |= FLAG_HAS_TTL; }
    if delay_ms != 0     { flags |= FLAG_HAS_DELAY; }
    if publisher_id != 0 { flags |= FLAG_HAS_PUBLISHER_ID; }
    if priority != 0     { flags |= FLAG_HAS_PRIORITY; }
    if retain            { flags |= FLAG_RETAIN; }
    let header_count = headers.len() as u16;
    span_count += header_count * 2;
    for (k, v) in headers { data_size += k.len() + v.len(); }

    let span_bytes = span_count as usize * SPAN_SIZE;
    let total = HEADER_SIZE + span_bytes + data_size;

    // --- Single resize, then direct slice writes ---
    let start = buf.len();
    buf.resize(start + total, 0);
    let out = &mut buf[start..start + total];

    // Fixed header (40 bytes)
    out[0..2].copy_from_slice(&flags.to_le_bytes());
    out[2..4].copy_from_slice(&header_count.to_le_bytes());
    out[4..6].copy_from_slice(&span_count.to_le_bytes());
    out[6] = priority;
    // out[7] already zero
    out[8..16].copy_from_slice(&timestamp.to_le_bytes());
    out[16..24].copy_from_slice(&ttl_ms.to_le_bytes());
    out[24..32].copy_from_slice(&delay_ms.to_le_bytes());
    out[32..40].copy_from_slice(&publisher_id.to_le_bytes());

    // Span index + data in single pass.
    // si = span index cursor, di = data cursor. Both advance independently
    // through the same output slice. Better cache locality than two passes.
    let mut si = HEADER_SIZE;
    let mut di = HEADER_SIZE + span_bytes;
    let mut data_off: u32 = 0;

    macro_rules! emit {
        ($d:expr) => {{
            let len = $d.len() as u32;
            out[si..si+4].copy_from_slice(&data_off.to_le_bytes());
            out[si+4..si+8].copy_from_slice(&len.to_le_bytes());
            si += 8;
            out[di..di + len as usize].copy_from_slice($d);
            di += len as usize;
            data_off += len;
        }}
    }

    emit!(value);
    for f in &optional { if let Some(b) = f { emit!(b); } }
    for (k, v) in headers { emit!(k); emit!(v); }

    debug_assert_eq!(si, HEADER_SIZE + span_bytes);
    debug_assert_eq!(di, total);
    start
}
```

One `resize` → one capacity check, one length update. All writes are
`copy_from_slice` into a pre-sized `&mut [u8]`. The `emit!` macro
writes the span entry AND the data together — span index cursor and
data cursor advance independently through the same output slice.
Same fixed+flex pattern as MqCommand's wire format.

### Phase 2: PublishPlan uses ranges instead of Bytes

```rust
pub struct PublishPlan {
    // Before: flat_message: Bytes
    pub msg_start: usize,    // offset into session's shared BytesMut
    pub msg_len: usize,

    pub exchange_id: u64,
    pub topic_id: u64,
    // ... other fields unchanged
}
```

The session owns a reusable `BytesMut` that accumulates messages:

```rust
// In MqttSession or server connection handler:
let mut msg_buf = BytesMut::with_capacity(8192);  // one-time alloc, reused
```

When the batch flushes:
```rust
let slices: SmallVec<[&[u8]; 32]> = plans.iter()
    .map(|p| &msg_buf[p.msg_start..p.msg_start + p.msg_len])
    .collect();
let cmd = MqCommand::publish_to_exchange_slices(exchange_id, &slices);
msg_buf.clear();  // reset for next batch, no dealloc
```

### Phase 3: MqCommand accepts &[&[u8]]

New methods alongside the existing `&[Bytes]` versions:

```rust
pub fn publish_to_exchange_slices(exchange_id: u64, messages: &[&[u8]]) -> Self {
    let table_size = messages.len() * 8;
    let data_size: usize = messages.iter().map(|m| m.len()).sum();
    CommandBuilder::with_capacity(
        Self::TAG_PUBLISH_TO_EXCHANGE, 0, 24,
        24 + table_size + data_size,
    )
    .set_u64(8, exchange_id)
    .set_vec_slices(16, messages)
    .finish()
}

// CommandBuilder:
pub fn set_vec_slices(mut self, offset: usize, values: &[&[u8]]) -> Self {
    // Identical logic to set_vec_bytes, just takes &[u8] instead of Bytes
    self.buf[offset..offset + 4]
        .copy_from_slice(&(values.len() as u32).to_le_bytes());
    let table_offset = self.buf.len();
    self.buf[offset + 4..offset + 8]
        .copy_from_slice(&(table_offset as u32).to_le_bytes());

    let table_size = values.len() * 8;
    self.buf.resize(self.buf.len() + table_size, 0);

    for (i, v) in values.iter().enumerate() {
        let data_offset = self.buf.len();
        self.buf.extend_from_slice(v);
        let desc = table_offset + i * 8;
        self.buf[desc..desc+4]
            .copy_from_slice(&(v.len() as u32).to_le_bytes());
        self.buf[desc+4..desc+8]
            .copy_from_slice(&(data_offset as u32).to_le_bytes());
    }
    self
}
```

The existing `&[Bytes]` methods (`set_vec_bytes`, `publish_to_exchange`) are
replaced entirely. `VecBytesIter` itself changes to yield `&[u8]` instead of
`Bytes` — it borrows from the MqCommand's `buf` which lives for the duration
of the apply call. No Arc refcount bumps needed:

```rust
// Before:
impl Iterator for VecBytesIter {
    type Item = Bytes;
    fn next(&mut self) -> Option<Bytes> {
        Some(self.buf.slice(offset..offset + size))  // Arc bump per message
    }
}

// After:
impl<'a> Iterator for VecSliceIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        Some(&self.buf[offset..offset + size])       // zero-cost borrow
    }
}
```

`apply_publish` changes from `Iterator<Item = Bytes>` to
`Iterator<Item = &[u8]>`. The only place that needs owned `Bytes` is
`RetainedValue` storage — that one call site does `Bytes::copy_from_slice(msg)`
to take ownership of the last message for retention.

### Phase 4: FlatMessage/MqttEnvelope become borrowed views

Both structs are temporary accessors over mmap segment data. The `buf`
field changes from owned `Bytes` to borrowed `&'a [u8]`:

```rust
// Before:
pub struct FlatMessage { buf: Bytes }
pub struct MqttEnvelope { buf: Bytes }

// After:
pub struct FlatMessage<'a> { buf: &'a [u8] }
pub struct MqttEnvelope<'a> { buf: &'a [u8] }
```

Constructors change from `new(Bytes)` to `new(&'a [u8])`. All accessors
return `&'a [u8]` (borrowing from the backing slice) instead of `Bytes`
(Arc refcount bump).

#### Accessor changes

```rust
// FlatMessage
span_bytes(index) -> Bytes     →  span_slice(index) -> &'a [u8]
value()           -> Bytes     →  value()           -> &'a [u8]
key()             -> Option<Bytes>  →  key()        -> Option<&'a [u8]>
routing_key()     -> Option<Bytes>  →  routing_key()-> Option<&'a [u8]>
routing_key_str() -> Option<String> →  routing_key_str() -> Option<&'a str>
reply_to()        -> Option<Bytes>  →  reply_to()   -> Option<&'a [u8]>
correlation_id()  -> Option<Bytes>  →  correlation_id() -> Option<&'a [u8]>
header(i)         -> (Bytes, Bytes) →  header(i)    -> (&'a [u8], &'a [u8])
headers()         -> Iterator<Item=(Bytes,Bytes)> → Iterator<Item=(&'a [u8],&'a [u8])>

// MqttEnvelope
topic()          -> Bytes      →  topic()          -> &'a [u8]
payload()        -> Bytes      →  payload()        -> &'a [u8]
properties_raw() -> Bytes      →  properties_raw() -> &'a [u8]
reply_to()       -> Option<Bytes>  →  reply_to()   -> Option<&'a [u8]>
correlation_id() -> Option<Bytes>  →  correlation_id() -> Option<&'a [u8]>

// Internal helpers
find_utf8_property()   -> Option<Bytes>  →  Option<&'a [u8]>
find_binary_property() -> Option<Bytes>  →  Option<&'a [u8]>
```

Zero-cost borrows from the backing `&'a [u8]`. No Arc, no atomics.

#### Call site changes by file

**engine.rs:379-384** — routing key extraction in `TAG_PUBLISH_TO_EXCHANGE`:
```rust
// Before: closure creates temporary FlatMessage/MqttEnvelope, returns Bytes
let routing_key_bytes = messages.first().and_then(|m| {
    if is_mqtt_envelope(m) {
        MqttEnvelope::new(m.clone()).map(|e| e.topic())    // Bytes
    } else {
        FlatMessage::new(m.clone()).and_then(|f| f.routing_key()) // Bytes
    }
});

// After: hoist wrapper to keep it alive for the borrow
let first = messages.first();
let envelope = first.and_then(|m| MqttEnvelope::new(m.clone()));
let flat = if envelope.is_none() {
    first.and_then(|m| FlatMessage::new(m.clone()))
} else { None };
let routing_key_bytes: Option<&[u8]> = envelope.as_ref().map(|e| e.topic())
    .or_else(|| flat.as_ref().and_then(|f| f.routing_key()));
```

**engine.rs:1823** — exchange-based routing (same pattern):
```rust
// Before:
let routing_key_bytes = FlatMessage::new(payload.clone()).and_then(|f| f.routing_key());
// After:
let flat = FlatMessage::new(payload.clone());
let routing_key_bytes = flat.as_ref().and_then(|f| f.routing_key());
```

**consumer_group.rs:378-380** — already keeps `env` alive, works as-is:
```rust
let env = MqttEnvelope::new(msg.clone());
let reply = env.as_ref().and_then(|e| e.reply_to());   // borrows from env
let corr = env.as_ref().and_then(|e| e.correlation_id());
```

**consumer_group.rs:388-395** — hoist FlatMessage:
```rust
// Before: creates FlatMessage inline in and_then
let reply = if m.has_reply_to() {
    FlatMessage::new(msg.clone()).and_then(|f| f.reply_to())
} else { None };

// After: hoist to keep alive
let flat = if m.has_reply_to() || m.has_correlation_id() {
    FlatMessage::new(msg.clone())
} else { None };
let reply = if m.has_reply_to() {
    flat.as_ref().and_then(|f| f.reply_to())
} else { None };
let corr = if m.has_correlation_id() {
    flat.as_ref().and_then(|f| f.correlation_id())
} else { None };
```

**consumer_group.rs:877-882** — same hoist pattern.

**server.rs:900-902** — topic extraction for alias resolution:
```rust
// Before: env.topic() returns Bytes, f.routing_key() returns Option<Bytes>
let topic = if let Some(ref env) = envelope {
    env.topic()          // was Bytes, now &[u8]
} else if let Some(ref f) = flat {
    f.routing_key().unwrap_or_default()  // was Bytes, now &[u8]
} else { unreachable!() };

// After: same code, but unwrap_or_default() → unwrap_or(&[])
let topic: &[u8] = if let Some(ref env) = envelope {
    env.topic()
} else if let Some(ref f) = flat {
    f.routing_key().unwrap_or(&[])
} else { unreachable!() };
```

**mq-mqtt/codec.rs:1886-1887** — outbound FlatMessage encode:
```rust
// topic and payload become &[u8], used with put_slice() — works as-is
let topic = flat_msg.routing_key().unwrap_or_default();  // → unwrap_or(&[])
let payload = flat_msg.value();  // &[u8], passed to put_slice
```

**mq-mqtt/codec.rs:1985-2095** — reply_to/correlation_id/headers encode:
```rust
// All use .len() and extend_from_slice/put_slice — works with &[u8]
if let Some(rt) = flat_msg.reply_to() {
    size += 1 + 2 + rt.len();      // .len() on &[u8] ✓
}
buf.extend_from_slice(rt);          // takes &[u8] ✓
if k == b"mqtt.content_type" { ... } // &[u8] == &[u8; N] via deref ✓
```

**mq-mqtt/codec.rs:2120-2121** — outbound MqttEnvelope encode:
```rust
let topic = envelope.topic();       // &[u8]
let payload = envelope.payload();   // &[u8]
// Both passed to put_slice — works as-is
```

**mq-mqtt/codec.rs:2210,2272** — properties_raw encode:
```rust
let raw_props = envelope.properties_raw();  // &[u8]
if !raw_props.is_empty() { ... }            // works on &[u8]
filtered_properties_size(raw_props);        // takes &[u8] already
copy_properties_filtered(raw_props, buf);   // takes &[u8] already
```

**mq-mqtt/session.rs:2505-2530** — will message reconstruction:
```rust
// topic: unwrap_or_else needs &[u8] not Bytes::from_static
let topic = flat_msg.routing_key().unwrap_or(b"unknown");
let payload = flat_msg.value();  // &[u8]
// Headers loop: k and v are &[u8], comparisons work
if k == b"mqtt.content_type" { ... }
```

**mq-sqs/handler.rs:566** — SQS body from value:
```rust
let val = flat.value();  // &[u8]
String::from_utf8(val.to_vec())  // .to_vec() works on &[u8]
```

**mq-kafka/codec.rs:1057-1110** — Kafka record encode:
```rust
let value = flat.value();           // &[u8]
write_varint(buf, value.len() as i32);
buf.put_slice(value);               // put_slice takes &[u8] ✓
match flat.key() {                   // Option<&[u8]>
    Some(k) => {
        write_varint(buf, k.len() as i32);
        buf.put_slice(k);           // ✓
    }
    None => write_varint(buf, -1),
};
let (hk, hv) = flat.header(i);     // (&[u8], &[u8])
write_varint_slice(buf, hk);        // takes &[u8] ✓
write_varint_slice(buf, hv);        // ✓
```

**Tests** — change `Bytes::from_static(b"...")` to `b"..."` or `.as_ref()`:
```rust
// Before:
assert_eq!(msg.value(), Bytes::from_static(b"payload"));
assert_eq!(msg.key(), Some(Bytes::from_static(b"k")));
// After:
assert_eq!(msg.value(), b"payload");
assert_eq!(msg.key(), Some(b"k".as_ref()));
```

Files with test changes:
- `crates/mq-protocol/src/flat.rs` (inline tests)
- `crates/mq/tests/flat_integration.rs`
- `crates/mq/tests/mqtt_optimizations.rs`
- `crates/mq-protocol/tests/flat_message_mqtt_flags.rs`
- `crates/mq-kafka/src/codec.rs` (inline tests)
- `crates/mq-mqtt/examples/mqtt_adapter_bench.rs`

### Phase 5: MQTT codec encode from &[u8]

`encode_publish_from_flat` and `encode_publish_from_envelope` call accessors
like `flat.routing_key()`. After Phase 4, these return `&[u8]` instead of
`Bytes` — the encode functions work unchanged since they only need the bytes
for `buf.put_slice()`.

### Phase 6: MQTT Publish fields as &[u8]

`Publish { topic: Bytes, payload: Bytes }` can become borrowed:

```rust
pub struct Publish<'a> {
    pub topic: &'a [u8],
    pub payload: &'a [u8],
    pub properties: PropertiesRef<'a>,  // borrows raw property bytes
    // ...
}
```

The `'a` borrows from the network buffer. `handle_publish` consumes
the Publish synchronously, so the borrow is valid.

**Exception**: `Connect` fields (`client_id`, `username`) are stored in
`MqttSession` and outlive the network buffer. These stay owned — but could
be `Box<[u8]>` instead of `Bytes` to avoid Arc overhead (single owner, never
shared).

## MqCommand Tag-Level Format Discrimination

Currently, `FlatMessage` and `MqttEnvelope` share the same MqCommand tags
(`TAG_PUBLISH_TO_EXCHANGE`, `TAG_SET_RETAINED`) and are distinguished at
runtime by checking bit 15 of the message flags (`is_mqtt_envelope()`).
This is a hack — every message in the hot path pays for this runtime check.

**New approach**: dedicated MqCommand tags for MQTT-native payloads.

```rust
// types.rs — new tags
pub const TAG_PUBLISH_TO_EXCHANGE_MQTT: u8 = 53;
pub const TAG_SET_RETAINED_MQTT: u8 = 54;
```

The command tag tells the engine which format the embedded messages use.
No per-message `is_mqtt_envelope()` check needed.

### Engine dispatch

```rust
// engine.rs apply()
MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
    // FlatMessage path (unchanged)
    let routing_key = FlatMessage::new(m.clone()).and_then(|f| f.routing_key());
    ...
}
MqCommand::TAG_PUBLISH_TO_EXCHANGE_MQTT => {
    // MqttEnvelope path — tag already tells us the format
    let routing_key = MqttEnvelope::new(m.clone()).map(|e| e.topic());
    ...
}
```

### consumer_group.rs

Same pattern — the caller passes a `is_mqtt: bool` (derived from the command
tag) into `apply_enqueue`, eliminating the per-message `is_mqtt_envelope()`.

### topic.rs

`apply_publish` gains a `is_mqtt: bool` parameter. Value length accounting
uses the right meta parser without runtime format checks.

### What goes away

- `FLAG_FORMAT_MQTT` (bit 15 hack in flags)
- `is_mqtt_envelope()` function
- Per-message format branching in engine, consumer_group, topic hot paths
- MqttEnvelope flags no longer need a discriminator bit

### MqCommand constructors

```rust
// codec.rs — new constructor
pub fn publish_to_exchange_mqtt(exchange_id: u64, messages: &[&[u8]]) -> Self {
    // Same wire layout as publish_to_exchange, just different tag
    CommandBuilder::with_capacity(
        Self::TAG_PUBLISH_TO_EXCHANGE_MQTT, 0, 24,
        24 + table_size + data_size,
    )
    .set_u64(8, exchange_id)
    .set_vec_slices(16, messages)
    .finish()
}

pub fn set_retained_mqtt(exchange_id: u64, routing_key: &str, message: &[u8]) -> Self {
    CommandBuilder::with_capacity(
        Self::TAG_SET_RETAINED_MQTT, ...
    )
    ...
}
```

### MQTT session usage

`session.rs` uses `TAG_PUBLISH_TO_EXCHANGE_MQTT` when building commands
from MQTT publishes. The MqttEnvelopeWriter writes into the shared buffer,
and the command carries the MQTT tag. No format bit needed in the envelope.

## Cross-Crate Scope

This refactor spans four crates:

### bisque-raft

The raft crate's Bytes usage is justified — mmap-backed zero-copy reads
via `bytes_from_segment()` which pins `Arc<Segment>` inside a `Bytes` handle.

**Changes:**

- **`Decode::decode_from_bytes(Bytes)`**: Stays as-is. This is the mmap → zero-copy
  decode path. Implementations like `Entry::decode_from_bytes` use `Bytes::slice()`
  for zero-cost sub-slicing of mmap-backed data.

- **`SegmentView::slice_bytes()` → `slice()`**: Returns `&[u8]` instead of
  `Bytes` where the caller only needs to read (not store). The `SegmentView`
  already holds the `Bytes`/`Arc<Segment>` for lifetime management.

- **`SegmentPrefetcher::read_normal_entry_data()`**: Stays `Bytes` — callers
  need ownership to keep mmap pinned while decoding entries that outlive
  the prefetcher borrow.

- **`BorrowPayload::payload_bytes()`**: Already returns `&[u8]`. No change.

- **Transport (`send_request`, `rpc_call`)**: Response payloads stay `Bytes` —
  they cross async boundaries and need owned lifetime. Could become `Vec<u8>`
  in the future since they're read-once.

### bisque-mq-protocol

- **`FlatMessage` / `MqttEnvelope` accessors** → return `&[u8]` (Phase 4)
- **`FlatMessageBuilder` / `MqttEnvelopeBuilder`** → replaced by `write_flat_message()`
  and `write_mqtt_envelope()` functions writing into `&mut BytesMut` (Phase 1)
- **`FlatMessage.buf: Bytes`** → `&'a [u8]` (Phase 4) — temporary accessor over mmap data
- **`MqttEnvelope.buf: Bytes`** → `&'a [u8]` (Phase 4) — same

### bisque-mq

- **`MqCommand` constructors** → `publish_to_exchange(&[&[u8]])` (Phase 3)
- **`CommandBuilder::set_vec_bytes`** → `set_vec_slices(&[&[u8]])` (Phase 3)
- **`VecBytesIter`** → `VecSliceIter<'a>` yielding `&'a [u8]` (Phase 3)
- **`Topic::apply_publish`** → `Iterator<Item = &[u8]>` (Phase 3)
- **`MqCommand.buf: Bytes`** → `Vec<u8>` (Phase 7)
- **`MqCommand.segments`** → removed (Phase 7)
- **`engine.apply_command`** → takes `&[u8]` instead of `&MqCommand` (Phase 7)
- **View structs** → borrow `&'a [u8]` directly, not `&'a MqCommand` (Phase 7)
- **Batch iteration** → yields `&'a [u8]` instead of `MqCommand` (Phase 7)
- **`RetainedValue.message: Bytes`** stays — segment tracking, copy-on-unmap

### bisque-mq-mqtt

- **`PublishPlan.flat_message`** → range into shared `BytesMut` (Phase 2)
- **`MqttEnvelopeWriter`** → writes into `&mut BytesMut` (Phase 1)
- **Encode functions** → natural cleanup from Phase 4 accessor changes (Phase 5)
- **`Publish` / `Properties`** → borrowed from network buffer (Phase 6)
- **`Connect` fields** → stay owned, could become `Box<[u8]>` (single owner)

## Phase 7: MqCommand `&[u8]` Migration

MqCommand currently serves dual roles:
1. **Write path**: owned builder for proposing entries to Raft
2. **Read path**: temporary accessor over mmap segment data

Both roles use `buf: Bytes`, which imposes Arc overhead that neither needs.
On the write path, the data is owned once and never shared. On the read path,
MqCommand is a temporary accessor — identical to FlatMessage/MqttEnvelope.

### Current architecture

```
Write: CommandBuilder (BytesMut) → freeze() → MqCommand { buf: Bytes }
       → Encode::encode() writes to raft log writer
       → drop (Arc decrement)

Read:  mmap segment → Bytes (via Arc<Segment>) → MqCommand::from_bytes(Bytes)
       → cmd.tag() / cmd.as_publish() / ... (view accessors)
       → drop (Arc decrement)
```

Both paths pay unnecessary Arc costs:
- Write: `freeze()` + `drop()` = ~10ns
- Read: `from_bytes()` stores `Bytes`, then every `as_batch().commands()`
  call does `self.buf.slice()` per sub-command = ~5ns × batch_size

### Target architecture

Split MqCommand into write-owned and read-borrowed:

```
Write: CommandBuilder (BytesMut) → MqCommand { buf: Vec<u8> }
       → Encode::encode() writes buf to raft log writer
       → drop (free, no atomics)

Read:  mmap data → &[u8] → engine.apply_command(&[u8])
       → tag = buf[0], dispatch
       → CmdPublish { buf: &'a [u8] } etc. (zero-copy view structs)
```

### MqCommand write path: `buf: Vec<u8>`

Replace `buf: Bytes` with `buf: Vec<u8>`. CommandBuilder already builds
into `BytesMut` — instead of `freeze()` to `Bytes`, call `.to_vec()` or
keep as `Vec<u8>` directly.

```rust
pub struct MqCommand {
    pub(crate) buf: Vec<u8>,
    // segments removed — see below
}
```

`Encode::encode()` writes `&self.buf` to the writer — works with `Vec<u8>`.
`BorrowPayload::payload_bytes()` returns `&self.buf` — works with `Vec<u8>`.

**Segments field removed**: The scatter-mode `segments: Option<Vec<Bytes>>`
was for zero-copy write encoding where payloads live in separate `Bytes`
references. With the `&[u8]` write path (Phase 3), all data is copied into
the command buffer via `set_vec_slices`. No scatter needed.

### MqCommand read path: `apply_command(&[u8])`

On the read path, MqCommand is never constructed. The raw `&[u8]` from
mmap data flows directly to the engine:

```rust
// Before (state_machine.rs):
EntryPayload::Normal(cmd) => {
    self.engine.apply_command(&cmd, log_index, ...);
}

// After:
EntryPayload::Normal(cmd) => {
    self.engine.apply_command(&cmd.buf, log_index, ...);
}
```

For async apply workers (which read from mmap directly):

```rust
// Before (cursor.rs / async_apply.rs):
let cmd = MqCommand::from_bytes(data);  // wraps Bytes
engine.apply_command(&cmd, ...);

// After:
let cmd_bytes: &[u8] = &segment_view[offset..offset + len];
engine.apply_command(cmd_bytes, ...);
```

### engine.rs: `apply_command(&[u8])`

```rust
// Before:
pub fn apply_command(&self, cmd: &MqCommand, ...) -> MqResponse {
    match cmd.tag() {
        MqCommand::TAG_PUBLISH => { ... }
        MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
            let publish = cmd.as_publish_to_exchange();
            ...
        }
    }
}

// After:
pub fn apply_command(&self, buf: &[u8], ...) -> MqResponse {
    let tag = buf[0];
    match tag {
        MqCommand::TAG_PUBLISH => { ... }
        MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
            let publish = CmdPublishToExchange::from_buf(buf);
            ...
        }
    }
}
```

### View structs: borrow `&'a [u8]`

View structs like `CmdPublish`, `CmdPublishToExchange`, `CmdBatch` etc.
already exist as thin wrappers. They currently borrow from `&MqCommand`
(which owns `Bytes`). Change to borrow `&'a [u8]` directly:

```rust
// Before:
pub struct CmdPublish<'a> { cmd: &'a MqCommand }
impl<'a> CmdPublish<'a> {
    fn topic_id(&self) -> u64 { self.cmd.field_u64(8) }
}

// After:
pub struct CmdPublish<'a> { buf: &'a [u8] }
impl<'a> CmdPublish<'a> {
    fn topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }
}
```

### Batch commands: zero-copy iteration

`CmdBatch::commands()` currently yields `MqCommand` (each doing
`self.buf.slice()` = Arc bump). Change to yield `&'a [u8]`:

```rust
// Before:
impl<'a> Iterator for BatchIter<'a> {
    type Item = MqCommand;
    fn next(&mut self) -> Option<MqCommand> {
        let sub = self.cmd.buf.slice(offset..offset + len);
        Some(MqCommand::from_bytes(sub))  // Arc bump
    }
}

// After:
impl<'a> Iterator for BatchIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        Some(&self.buf[offset..offset + len])  // zero-cost borrow
    }
}
```

`apply_batch` changes accordingly:
```rust
fn apply_batch(&self, buf: &[u8], ...) -> MqResponse {
    let batch = CmdBatch::from_buf(buf);
    let cmds: SmallVec<[&[u8]; 16]> = batch.commands().collect();
    // ...
}
```

### OpenRaft AppData boundary

OpenRaft requires `AppData: Serialize + Deserialize + Send + Sync + 'static`
(via the `serde` feature flag). These bounds exist on the type parameter `D`
in `BisqueRaftTypeConfig<D, R>`.

The Serialize/Deserialize impls are **never called on the hot path** —
the actual raft log codec uses our flat binary `Encode`/`Decode` traits.
The serde impls exist solely to satisfy the OpenRaft trait bound.

Two options:

**Option A: Disable OpenRaft `serde` feature.** If no other code relies on
serde-based raft serialization, drop `"serde"` from the openraft feature
list. Then `OptionalSerde` becomes an empty trait and MqCommand no longer
needs `Serialize + Deserialize`. This is the cleanest path.

**Option B: Keep serde impls as no-ops.** If the `serde` feature can't be
disabled, the custom `Serialize`/`Deserialize` impls on MqCommand stay
(they just write/read `Vec<u8>` instead of `Bytes`). They satisfy the
bound without affecting the hot path.

Either way, MqCommand's `buf` becomes `Vec<u8>` (not `Bytes`), and the
`Encode`/`Decode` traits work unchanged — `Encode::encode` writes
`&self.buf`, `Decode::decode` reads into `Vec<u8>`.

### Call site changes

**state_machine.rs:apply_batch_inline()** — `cmd` is `&MqCommand`, change
to `cmd.payload_bytes()` or `&cmd.buf`:
```rust
let response = self.engine.apply_command(&cmd.buf, log_index, ...);
```

**state_machine.rs:apply()** — same pattern for the inline apply path.

**async_apply.rs:process_range()** — workers read `SegmentRecord` which
currently contains `command: MqCommand`. Change to `command_bytes: &[u8]`
(borrowed from the segment view):
```rust
let response = self.engine.apply_command(rec.command_bytes, ...);
```

**engine.rs:apply_batch()** — `batch.commands()` yields `&[u8]` instead of
`MqCommand`. Sub-command dispatch uses `buf[0]` for tag.

**engine.rs:apply_publish_run()** — takes `&[&[u8]]` instead of `&[MqCommand]`.

**cursor.rs:MqSegmentCursor** — `next_record_raw()` returns `&[u8]` view
into the mmap data instead of constructing `MqCommand::from_bytes(Bytes)`.

### What stays owned

| Item | Type | Why |
|------|------|-----|
| MqCommand.buf | `Vec<u8>` | Write path: CommandBuilder output, proposed to Raft |
| Encode/Decode | trait impls | Raft log binary codec (not serde) |
| Serialize/Deserialize | trait impls | OpenRaft AppData bound (never hot path) |

### What goes away

- `MqCommand.buf: Bytes` → `Vec<u8>` (no Arc)
- `MqCommand.segments: Option<Vec<Bytes>>` → removed (scatter mode replaced by copy)
- `MqCommand::from_bytes(Bytes)` → removed (read path uses raw `&[u8]`)
- `MqCommand::decode_from_bytes(Bytes)` → reads into `Vec<u8>`
- Per-sub-command `Bytes::slice()` in batch iteration → zero-cost `&[u8]` borrows

## What Stays as Bytes

| Crate | Type | Field | Why |
|-------|------|-------|-----|
| mq | RetainedValue | message | segment tracking, copy-on-unmap |
| mq-mqtt | Connect | client_id etc. | stored in session, outlives network buffer |
| raft | SegmentView | data | mmap pin via Arc<Segment> |
| raft | Decode trait | decode_from_bytes param | mmap zero-copy decode entry point |
| raft | Transport | response payloads | cross-async ownership |

FlatMessage, MqttEnvelope, and MqCommand are all temporary accessors —
they borrow `&'a [u8]` from backing data (mmap segments, network buffers).
No Arc ownership needed. Everything else moves to `&[u8]`.

## Expected Impact

| Operation | Before | After | Savings |
|-----------|--------|-------|---------|
| BytesMut::with_capacity (per msg) | ~15ns | 0 (reuse) | 15ns |
| freeze() atomic init | ~5ns | 0 | 5ns |
| Bytes::clone in PublishPlan | ~5ns | 0 | 5ns |
| Arc drop after submit | ~5ns | 0 | 5ns |
| FlatMessage accessor (x3) | ~15ns | 0 | 15ns |
| VecBytesIter Bytes::slice (per msg) | ~5ns | 0 | 5ns |
| MqCommand::from_bytes on read path | ~5ns | 0 | 5ns |
| Batch sub-cmd Bytes::slice (×N) | ~5ns×N | 0 | 5ns×N |
| MqCommand freeze on write path | ~5ns | 0 | 5ns |
| **Total per message** | **~60ns+** | **0** | **~60ns+** |

At 10M msgs/sec, that's ~600ms/sec of pure atomic/alloc overhead eliminated.

## Implementation Order

1. **Phase 4** (bisque-mq-protocol) — accessor return types `&[u8]`. Smallest
   blast radius, biggest immediate win. Ripples into all consumers.
2. **Phase 1** (bisque-mq-protocol) — `write_flat_message()` and
   `write_mqtt_envelope()` into `&mut BytesMut`. Eliminates per-message alloc.
3. **Phase 3** (bisque-mq) — `VecSliceIter`, `set_vec_slices`,
   `publish_to_exchange` takes `&[&[u8]]`. Changes `apply_publish` signature.
   Add `TAG_PUBLISH_TO_EXCHANGE_MQTT` and `TAG_SET_RETAINED_MQTT` tags,
   constructors, and view structs. Engine/consumer_group/topic dispatch
   by tag instead of `is_mqtt_envelope()`.
4. **Phase 2** (bisque-mq-mqtt) — PublishPlan ranges + session shared buffer.
   Session uses `TAG_PUBLISH_TO_EXCHANGE_MQTT` for MQTT publishes.
   Ties phases 1+3 together.
5. **Phase 5** (bisque-mq-mqtt) — encode cleanup, falls out from phase 4.
6. **Phase 6** (bisque-mq-mqtt) — `Publish<'a>` with borrowed fields. Larger
   refactor, can be deferred.
7. **Phase 7** (bisque-mq) — MqCommand `&[u8]` migration. Write path:
   `buf: Bytes` → `Vec<u8>`, remove scatter segments. Read path:
   `apply_command(&MqCommand)` → `apply_command(&[u8])`, view structs
   borrow `&'a [u8]`, batch iteration yields `&'a [u8]`.
   Evaluate disabling OpenRaft `serde` feature to remove Serialize/Deserialize
   requirement.
8. **Cleanup** — remove `FLAG_FORMAT_MQTT`, `is_mqtt_envelope()`, and all
   per-message format branching. MqttEnvelope flags become a clean u8.
