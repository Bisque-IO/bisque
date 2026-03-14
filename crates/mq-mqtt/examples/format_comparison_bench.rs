//! Benchmark: FlatMessage vs MqttEnvelope vs PackedEnvelope format comparison.
//!
//! Compares build + read costs for three formats:
//!
//! 1. **FlatMessage**: Span-indexed format. 40-byte header + 8-byte span entries
//!    per variable field + data region. O(1) reads via span index.
//!
//! 2. **MqttEnvelope + MQTT props**: 40-byte header + topic + MQTT 5.0 property
//!    bytes + payload. Reply-to/correlation-id use MQTT property IDs (0x08/0x09).
//!    Key/headers use user properties (0x26). Sequential scan on read.
//!
//! 3. **PackedEnvelope**: 40-byte header + topic + packed extension header + payload.
//!    Extension header: 8-byte fixed prefix with u16 lengths for key, reply_to,
//!    correlation_id, header_count — then variable data. O(1) reads via cumulative
//!    offset computation. 2 bytes/field vs FlatMessage's 8 bytes/field.
//!
//!    ```text
//!    [40-byte envelope header (topic_len, ext_len in reserved fields)]
//!    [topic: topic_len]
//!    [extensions: ext_len]
//!      key_len:            u16  ─┐
//!      reply_to_len:       u16   │ 8 bytes fixed
//!      correlation_id_len: u16   │
//!      header_count:       u16  ─┘
//!      [key][reply_to][correlation_id]
//!      [hk_len:u16][hv_len:u16][hk][hv] × header_count
//!    [payload: remaining]
//!    ```
//!
//! Run with: cargo run --release -p bisque-mq-mqtt --example format_comparison_bench

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use std::hint::black_box;
use std::time::Instant;

use bytes::{Bytes, BytesMut};

use bisque_mq::flat::{
    FlatMessage, FlatMessageBuilder, MqttEnvelope, MqttEnvelopeBuilder, MqttEnvelopeWriter,
    write_flat_message,
};

// =============================================================================
// MQTT 5.0 property encoding helpers
// =============================================================================

/// Encode a UTF-8 string property: [id:1][len:2 BE][utf8]
fn encode_utf8_prop(buf: &mut Vec<u8>, id: u8, value: &[u8]) {
    buf.push(id);
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
}

/// Encode a binary data property: [id:1][len:2 BE][bytes]
fn encode_binary_prop(buf: &mut Vec<u8>, id: u8, value: &[u8]) {
    buf.push(id);
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
}

/// Encode a user property (string pair): [0x26][k_len:2 BE][key][v_len:2 BE][value]
fn encode_user_prop(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    buf.push(0x26);
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
}

/// Build raw MQTT 5.0 property bytes carrying FlatMessage-equivalent fields.
fn build_props(
    reply_to: Option<&[u8]>,
    correlation_id: Option<&[u8]>,
    key: Option<&[u8]>,
    headers: &[(&[u8], &[u8])],
) -> Vec<u8> {
    let mut props = Vec::with_capacity(128);
    if let Some(rt) = reply_to {
        encode_utf8_prop(&mut props, 0x08, rt); // Response Topic
    }
    if let Some(cid) = correlation_id {
        encode_binary_prop(&mut props, 0x09, cid); // Correlation Data
    }
    if let Some(k) = key {
        encode_user_prop(&mut props, b"k", k); // key as user property
    }
    for (hk, hv) in headers {
        encode_user_prop(&mut props, hk, hv);
    }
    props
}

// =============================================================================
// Scenarios
// =============================================================================

struct Scenario {
    name: &'static str,
    payload_size: usize,
    key: Option<&'static [u8]>,
    routing_key: Option<&'static [u8]>,
    reply_to: Option<&'static [u8]>,
    correlation_id: Option<&'static [u8]>,
    headers: &'static [(&'static [u8], &'static [u8])],
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "minimal",
        payload_size: 256,
        key: None,
        routing_key: Some(b"orders/us-east/created"),
        reply_to: None,
        correlation_id: None,
        headers: &[],
    },
    Scenario {
        name: "rpc",
        payload_size: 256,
        key: None,
        routing_key: Some(b"rpc/user-service/get"),
        reply_to: Some(b"reply/client-42/inbox"),
        correlation_id: Some(b"req-98765"),
        headers: &[],
    },
    Scenario {
        name: "kafka-like",
        payload_size: 256,
        key: Some(b"user-12345"),
        routing_key: Some(b"events/user/updated"),
        reply_to: None,
        correlation_id: None,
        headers: &[(b"source", b"user-svc"), (b"trace-id", b"abc123def456")],
    },
    Scenario {
        name: "full",
        payload_size: 256,
        key: Some(b"order-99999"),
        routing_key: Some(b"orders/eu-west/shipped"),
        reply_to: Some(b"reply/client-7/inbox"),
        correlation_id: Some(b"corr-abcdef"),
        headers: &[
            (b"source", b"order-svc"),
            (b"trace-id", b"t-0123456789abcdef"),
            (b"content-type", b"application/json"),
        ],
    },
    Scenario {
        name: "full-1024",
        payload_size: 1024,
        key: Some(b"order-99999"),
        routing_key: Some(b"orders/eu-west/shipped"),
        reply_to: Some(b"reply/client-7/inbox"),
        correlation_id: Some(b"corr-abcdef"),
        headers: &[
            (b"source", b"order-svc"),
            (b"trace-id", b"t-0123456789abcdef"),
            (b"content-type", b"application/json"),
        ],
    },
    Scenario {
        name: "full-64",
        payload_size: 64,
        key: Some(b"order-99999"),
        routing_key: Some(b"orders/eu-west/shipped"),
        reply_to: Some(b"reply/client-7/inbox"),
        correlation_id: Some(b"corr-abcdef"),
        headers: &[
            (b"source", b"order-svc"),
            (b"trace-id", b"t-0123456789abcdef"),
            (b"content-type", b"application/json"),
        ],
    },
];

// =============================================================================
// Benchmark runners
// =============================================================================

const NUM_MESSAGES: u64 = 2_000_000;

struct BenchResult {
    label: String,
    ns_per_msg: f64,
    msgs_per_sec: f64,
    bytes_per_msg: usize,
}

// -- Build benchmarks ---------------------------------------------------------

fn bench_flat_builder(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        let mut b = FlatMessageBuilder::new(&payload);
        if let Some(rk) = s.routing_key {
            b = b.routing_key(rk);
        }
        if let Some(k) = s.key {
            b = b.key(k);
        }
        if let Some(rt) = s.reply_to {
            b = b.reply_to(rt);
        }
        if let Some(cid) = s.correlation_id {
            b = b.correlation_id(cid);
        }
        for (hk, hv) in s.headers {
            b = b.header(*hk, *hv);
        }
        b = b.timestamp(1000).publisher_id(42);
        let result = b.build();
        black_box(&result);
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    // Get size from one build
    let mut b = FlatMessageBuilder::new(&payload);
    if let Some(rk) = s.routing_key {
        b = b.routing_key(rk);
    }
    if let Some(k) = s.key {
        b = b.key(k);
    }
    if let Some(rt) = s.reply_to {
        b = b.reply_to(rt);
    }
    if let Some(cid) = s.correlation_id {
        b = b.correlation_id(cid);
    }
    for (hk, hv) in s.headers {
        b = b.header(*hk, *hv);
    }
    let sample = b.timestamp(1000).publisher_id(42).build();
    BenchResult {
        label: format!("FlatBuilder {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: sample.len(),
    }
}

fn bench_flat_writer(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let mut buf = BytesMut::with_capacity(4096);
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        buf.clear();
        write_flat_message(
            &mut buf,
            &payload,
            s.key,
            s.routing_key,
            s.reply_to,
            s.correlation_id,
            s.headers,
            1000,
            0,
            0,
            42,
            0,
            false,
        );
        black_box(&buf);
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    buf.clear();
    write_flat_message(
        &mut buf,
        &payload,
        s.key,
        s.routing_key,
        s.reply_to,
        s.correlation_id,
        s.headers,
        1000,
        0,
        0,
        42,
        0,
        false,
    );
    let size = buf.len();
    BenchResult {
        label: format!("FlatWriter  {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: size,
    }
}

fn bench_envelope_builder(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let topic = s.routing_key.unwrap_or(b"");
    let props = build_props(s.reply_to, s.correlation_id, s.key, s.headers);
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        let result = MqttEnvelopeBuilder::new(topic, &payload)
            .timestamp(1000)
            .publisher_id(42)
            .properties_raw(&props)
            .build();
        black_box(&result);
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    let sample = MqttEnvelopeBuilder::new(topic, &payload)
        .timestamp(1000)
        .publisher_id(42)
        .properties_raw(&props)
        .build();
    BenchResult {
        label: format!("EnvBuilder  {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: sample.len(),
    }
}

fn bench_envelope_writer(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let topic = s.routing_key.unwrap_or(b"");
    let props = build_props(s.reply_to, s.correlation_id, s.key, s.headers);
    let mut buf = BytesMut::with_capacity(4096);
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        buf.clear();
        MqttEnvelopeWriter::new(&mut buf, topic, &payload)
            .timestamp(1000)
            .publisher_id(42)
            .properties_raw(&props)
            .finish();
        black_box(&buf);
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    buf.clear();
    MqttEnvelopeWriter::new(&mut buf, topic, &payload)
        .timestamp(1000)
        .publisher_id(42)
        .properties_raw(&props)
        .finish();
    let size = buf.len();
    BenchResult {
        label: format!("EnvWriter   {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: size,
    }
}

// -- Read benchmarks ----------------------------------------------------------

fn bench_flat_read(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let mut b = FlatMessageBuilder::new(&payload);
    if let Some(rk) = s.routing_key {
        b = b.routing_key(rk);
    }
    if let Some(k) = s.key {
        b = b.key(k);
    }
    if let Some(rt) = s.reply_to {
        b = b.reply_to(rt);
    }
    if let Some(cid) = s.correlation_id {
        b = b.correlation_id(cid);
    }
    for (hk, hv) in s.headers {
        b = b.header(*hk, *hv);
    }
    let data = b.timestamp(1000).publisher_id(42).build();
    let flat = FlatMessage::new(&data).unwrap();

    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        let _ = black_box(flat.value());
        let _ = black_box(flat.routing_key());
        let _ = black_box(flat.key());
        let _ = black_box(flat.reply_to());
        let _ = black_box(flat.correlation_id());
        let _ = black_box(flat.timestamp());
        let _ = black_box(flat.publisher_id());
        let _ = black_box(flat.ttl_ms());
        let hc = flat.header_count();
        for i in 0..hc {
            let _ = black_box(flat.header(i));
        }
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    BenchResult {
        label: format!("FlatRead    {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: data.len(),
    }
}

fn bench_envelope_read(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let topic = s.routing_key.unwrap_or(b"");
    let props = build_props(s.reply_to, s.correlation_id, s.key, s.headers);
    let data = MqttEnvelopeBuilder::new(topic, &payload)
        .timestamp(1000)
        .publisher_id(42)
        .properties_raw(&props)
        .build();
    let env = MqttEnvelope::new(&data).unwrap();

    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        let _ = black_box(env.payload());
        let _ = black_box(env.topic());
        let _ = black_box(env.reply_to()); // scans properties
        let _ = black_box(env.correlation_id()); // scans properties
        let _ = black_box(env.timestamp());
        let _ = black_box(env.publisher_id());
        let _ = black_box(env.ttl_ms());
        // Read user properties (key + headers) by scanning raw props
        let raw = env.properties_raw();
        let _ = black_box(scan_user_properties(raw));
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    BenchResult {
        label: format!("EnvRead     {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: data.len(),
    }
}

/// Scan all user properties (0x26) from raw MQTT property bytes.
/// Returns count of user properties found.
fn scan_user_properties(raw: &[u8]) -> usize {
    let mut pos = 0;
    let mut count = 0;
    while pos < raw.len() {
        let id = raw[pos];
        pos += 1;
        match id {
            // UTF-8 string pair (user property)
            0x26 => {
                if pos + 2 > raw.len() {
                    break;
                }
                let klen = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
                pos += 2 + klen;
                if pos + 2 > raw.len() {
                    break;
                }
                let vlen = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
                pos += 2 + vlen;
                count += 1;
            }
            // UTF-8 string (response topic etc.)
            0x03 | 0x08 | 0x12 | 0x15 | 0x1A | 0x1C | 0x1F => {
                if pos + 2 > raw.len() {
                    break;
                }
                let len = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
                pos += 2 + len;
            }
            // Binary data (correlation data etc.)
            0x09 | 0x16 => {
                if pos + 2 > raw.len() {
                    break;
                }
                let len = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
                pos += 2 + len;
            }
            // VarInt (u32)
            0x01 | 0x02 | 0x11 | 0x27 => {
                pos += 4;
            }
            // u16
            0x13 | 0x21 | 0x22 | 0x23 | 0x33 => {
                pos += 2;
            }
            // u8
            0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => {
                pos += 1;
            }
            _ => break,
        }
    }
    count
}

// =============================================================================
// PackedEnvelope: envelope header + packed extension fields
// =============================================================================

/// Header size for the packed envelope (same 40-byte layout as MqttEnvelope).
const PACKED_HEADER_SIZE: usize = 40;
/// Extension prefix: 4 × u16 = 8 bytes.
const EXT_PREFIX_SIZE: usize = 8;

/// Write a PackedEnvelope into `buf`. Returns start offset.
///
/// Layout:
/// ```text
/// [flags:u16][reserved:u16][topic_len:u16][ext_len:u16]
/// [timestamp:u64][ttl_ms:u64][publisher_id:u64][padding:u64]
/// [topic: topic_len]
/// [ext: ext_len]
///   key_len:u16  reply_to_len:u16  corr_id_len:u16  header_count:u16
///   [key][reply_to][correlation_id]
///   [hk_len:u16 hv_len:u16 hk hv] × header_count
/// [payload: remaining]
/// ```
#[inline]
fn write_packed_envelope(
    buf: &mut BytesMut,
    topic: &[u8],
    payload: &[u8],
    key: Option<&[u8]>,
    reply_to: Option<&[u8]>,
    correlation_id: Option<&[u8]>,
    headers: &[(&[u8], &[u8])],
    timestamp: u64,
    ttl_ms: u64,
    publisher_id: u64,
    retain: bool,
) -> usize {
    let key_b = key.unwrap_or(&[]);
    let rt_b = reply_to.unwrap_or(&[]);
    let cid_b = correlation_id.unwrap_or(&[]);

    let has_ext = !key_b.is_empty() || !rt_b.is_empty() || !cid_b.is_empty() || !headers.is_empty();
    let ext_var: usize = key_b.len()
        + rt_b.len()
        + cid_b.len()
        + headers
            .iter()
            .map(|(k, v)| 4 + k.len() + v.len())
            .sum::<usize>();
    let ext_len = if has_ext {
        EXT_PREFIX_SIZE + ext_var
    } else {
        0
    };

    let total = PACKED_HEADER_SIZE + topic.len() + ext_len + payload.len();
    let start = buf.len();
    buf.resize(start + total, 0);
    let out = &mut buf[start..start + total];

    // Header
    let flags: u16 = bisque_mq::flat::FLAG_FORMAT_MQTT
        | if retain { 1 } else { 0 }
        | if ttl_ms != 0 { 1 << 2 } else { 0 };
    out[0..2].copy_from_slice(&flags.to_le_bytes());
    // reserved = 0
    out[4..6].copy_from_slice(&(topic.len() as u16).to_le_bytes());
    out[6..8].copy_from_slice(&(ext_len as u16).to_le_bytes());
    out[8..16].copy_from_slice(&timestamp.to_le_bytes());
    out[16..24].copy_from_slice(&ttl_ms.to_le_bytes());
    out[24..32].copy_from_slice(&publisher_id.to_le_bytes());
    // padding = 0

    let mut pos = PACKED_HEADER_SIZE;

    // Topic
    out[pos..pos + topic.len()].copy_from_slice(topic);
    pos += topic.len();

    // Extensions
    if has_ext {
        out[pos..pos + 2].copy_from_slice(&(key_b.len() as u16).to_le_bytes());
        out[pos + 2..pos + 4].copy_from_slice(&(rt_b.len() as u16).to_le_bytes());
        out[pos + 4..pos + 6].copy_from_slice(&(cid_b.len() as u16).to_le_bytes());
        out[pos + 6..pos + 8].copy_from_slice(&(headers.len() as u16).to_le_bytes());
        pos += EXT_PREFIX_SIZE;

        out[pos..pos + key_b.len()].copy_from_slice(key_b);
        pos += key_b.len();
        out[pos..pos + rt_b.len()].copy_from_slice(rt_b);
        pos += rt_b.len();
        out[pos..pos + cid_b.len()].copy_from_slice(cid_b);
        pos += cid_b.len();

        for (hk, hv) in headers {
            out[pos..pos + 2].copy_from_slice(&(hk.len() as u16).to_le_bytes());
            out[pos + 2..pos + 4].copy_from_slice(&(hv.len() as u16).to_le_bytes());
            pos += 4;
            out[pos..pos + hk.len()].copy_from_slice(hk);
            pos += hk.len();
            out[pos..pos + hv.len()].copy_from_slice(hv);
            pos += hv.len();
        }
    }

    // Payload
    out[pos..pos + payload.len()].copy_from_slice(payload);

    start
}

/// Reader for PackedEnvelope — O(1) field access via cumulative u16 lengths.
struct PackedEnvelopeReader<'a> {
    buf: &'a [u8],
}

impl<'a> PackedEnvelopeReader<'a> {
    fn new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < PACKED_HEADER_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline]
    fn topic_len(&self) -> usize {
        u16::from_le_bytes([self.buf[4], self.buf[5]]) as usize
    }

    #[inline]
    fn ext_len(&self) -> usize {
        u16::from_le_bytes([self.buf[6], self.buf[7]]) as usize
    }

    #[inline]
    fn timestamp(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    #[inline]
    fn ttl_ms(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    #[inline]
    fn publisher_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[24..32].try_into().unwrap())
    }

    #[inline]
    fn topic(&self) -> &'a [u8] {
        let start = PACKED_HEADER_SIZE;
        &self.buf[start..start + self.topic_len()]
    }

    #[inline]
    fn ext_start(&self) -> usize {
        PACKED_HEADER_SIZE + self.topic_len()
    }

    #[inline]
    fn payload(&self) -> &'a [u8] {
        let start = self.ext_start() + self.ext_len();
        &self.buf[start..]
    }

    // Extension field readers — O(1) via cumulative u16 lengths

    #[inline]
    fn ext_key_len(&self) -> u16 {
        let s = self.ext_start();
        if self.ext_len() == 0 {
            return 0;
        }
        u16::from_le_bytes([self.buf[s], self.buf[s + 1]])
    }

    #[inline]
    fn ext_reply_to_len(&self) -> u16 {
        let s = self.ext_start();
        if self.ext_len() == 0 {
            return 0;
        }
        u16::from_le_bytes([self.buf[s + 2], self.buf[s + 3]])
    }

    #[inline]
    fn ext_correlation_id_len(&self) -> u16 {
        let s = self.ext_start();
        if self.ext_len() == 0 {
            return 0;
        }
        u16::from_le_bytes([self.buf[s + 4], self.buf[s + 5]])
    }

    #[inline]
    fn ext_header_count(&self) -> u16 {
        let s = self.ext_start();
        if self.ext_len() == 0 {
            return 0;
        }
        u16::from_le_bytes([self.buf[s + 6], self.buf[s + 7]])
    }

    #[inline]
    fn key(&self) -> Option<&'a [u8]> {
        let klen = self.ext_key_len() as usize;
        if klen == 0 {
            return None;
        }
        let start = self.ext_start() + EXT_PREFIX_SIZE;
        Some(&self.buf[start..start + klen])
    }

    #[inline]
    fn reply_to(&self) -> Option<&'a [u8]> {
        let rtlen = self.ext_reply_to_len() as usize;
        if rtlen == 0 {
            return None;
        }
        let start = self.ext_start() + EXT_PREFIX_SIZE + self.ext_key_len() as usize;
        Some(&self.buf[start..start + rtlen])
    }

    #[inline]
    fn correlation_id(&self) -> Option<&'a [u8]> {
        let cidlen = self.ext_correlation_id_len() as usize;
        if cidlen == 0 {
            return None;
        }
        let start = self.ext_start()
            + EXT_PREFIX_SIZE
            + self.ext_key_len() as usize
            + self.ext_reply_to_len() as usize;
        Some(&self.buf[start..start + cidlen])
    }

    fn header(&self, index: usize) -> Option<(&'a [u8], &'a [u8])> {
        let hc = self.ext_header_count() as usize;
        if index >= hc {
            return None;
        }
        let mut pos = self.ext_start()
            + EXT_PREFIX_SIZE
            + self.ext_key_len() as usize
            + self.ext_reply_to_len() as usize
            + self.ext_correlation_id_len() as usize;
        // Skip to the requested header
        for _ in 0..index {
            let hklen = u16::from_le_bytes([self.buf[pos], self.buf[pos + 1]]) as usize;
            let hvlen = u16::from_le_bytes([self.buf[pos + 2], self.buf[pos + 3]]) as usize;
            pos += 4 + hklen + hvlen;
        }
        let hklen = u16::from_le_bytes([self.buf[pos], self.buf[pos + 1]]) as usize;
        let hvlen = u16::from_le_bytes([self.buf[pos + 2], self.buf[pos + 3]]) as usize;
        pos += 4;
        let hk = &self.buf[pos..pos + hklen];
        let hv = &self.buf[pos + hklen..pos + hklen + hvlen];
        Some((hk, hv))
    }
}

// -- PackedEnvelope benchmarks ------------------------------------------------

fn bench_packed_writer(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let topic = s.routing_key.unwrap_or(b"");
    let mut buf = BytesMut::with_capacity(4096);
    let start_bench = Instant::now();
    for _ in 0..NUM_MESSAGES {
        buf.clear();
        write_packed_envelope(
            &mut buf,
            topic,
            &payload,
            s.key,
            s.reply_to,
            s.correlation_id,
            s.headers,
            1000,
            0,
            42,
            false,
        );
        black_box(&buf);
    }
    let elapsed = start_bench.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    buf.clear();
    write_packed_envelope(
        &mut buf,
        topic,
        &payload,
        s.key,
        s.reply_to,
        s.correlation_id,
        s.headers,
        1000,
        0,
        42,
        false,
    );
    let size = buf.len();
    BenchResult {
        label: format!("PackedWrite {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: size,
    }
}

fn bench_packed_read(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let topic = s.routing_key.unwrap_or(b"");
    let mut buf = BytesMut::with_capacity(4096);
    write_packed_envelope(
        &mut buf,
        topic,
        &payload,
        s.key,
        s.reply_to,
        s.correlation_id,
        s.headers,
        1000,
        0,
        42,
        false,
    );
    let reader = PackedEnvelopeReader::new(&buf).unwrap();
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        let _ = black_box(reader.payload());
        let _ = black_box(reader.topic());
        let _ = black_box(reader.key());
        let _ = black_box(reader.reply_to());
        let _ = black_box(reader.correlation_id());
        let _ = black_box(reader.timestamp());
        let _ = black_box(reader.publisher_id());
        let _ = black_box(reader.ttl_ms());
        let hc = reader.ext_header_count() as usize;
        for i in 0..hc {
            let _ = black_box(reader.header(i));
        }
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    BenchResult {
        label: format!("PackedRead  {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: buf.len(),
    }
}

fn bench_packed_roundtrip(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let topic = s.routing_key.unwrap_or(b"");
    let mut buf = BytesMut::with_capacity(4096);
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        buf.clear();
        write_packed_envelope(
            &mut buf,
            topic,
            &payload,
            s.key,
            s.reply_to,
            s.correlation_id,
            s.headers,
            1000,
            0,
            42,
            false,
        );
        let reader = PackedEnvelopeReader::new(&buf).unwrap();
        let _ = black_box(reader.payload());
        let _ = black_box(reader.topic());
        let _ = black_box(reader.key());
        let _ = black_box(reader.reply_to());
        let _ = black_box(reader.correlation_id());
        let _ = black_box(reader.timestamp());
        let _ = black_box(reader.publisher_id());
        let hc = reader.ext_header_count() as usize;
        for i in 0..hc {
            let _ = black_box(reader.header(i));
        }
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    BenchResult {
        label: format!("PackedRT    {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: 0,
    }
}

// -- Build+Read roundtrip benchmarks ------------------------------------------

fn bench_flat_roundtrip(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let mut buf = BytesMut::with_capacity(4096);

    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        buf.clear();
        write_flat_message(
            &mut buf,
            &payload,
            s.key,
            s.routing_key,
            s.reply_to,
            s.correlation_id,
            s.headers,
            1000,
            0,
            0,
            42,
            0,
            false,
        );
        let flat = FlatMessage::new(&buf).unwrap();
        let _ = black_box(flat.value());
        let _ = black_box(flat.routing_key());
        let _ = black_box(flat.key());
        let _ = black_box(flat.reply_to());
        let _ = black_box(flat.correlation_id());
        let _ = black_box(flat.timestamp());
        let _ = black_box(flat.publisher_id());
        let hc = flat.header_count();
        for i in 0..hc {
            let _ = black_box(flat.header(i));
        }
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    BenchResult {
        label: format!("FlatRT      {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: 0,
    }
}

fn bench_envelope_roundtrip(s: &Scenario) -> BenchResult {
    let payload = vec![0xABu8; s.payload_size];
    let topic = s.routing_key.unwrap_or(b"");
    let props = build_props(s.reply_to, s.correlation_id, s.key, s.headers);
    let mut buf = BytesMut::with_capacity(4096);

    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        buf.clear();
        MqttEnvelopeWriter::new(&mut buf, topic, &payload)
            .timestamp(1000)
            .publisher_id(42)
            .properties_raw(&props)
            .finish();
        let env = MqttEnvelope::new(&buf).unwrap();
        let _ = black_box(env.payload());
        let _ = black_box(env.topic());
        let _ = black_box(env.reply_to());
        let _ = black_box(env.correlation_id());
        let _ = black_box(env.timestamp());
        let _ = black_box(env.publisher_id());
        let raw = env.properties_raw();
        let _ = black_box(scan_user_properties(raw));
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    BenchResult {
        label: format!("EnvRT       {}", s.name),
        ns_per_msg: ns,
        msgs_per_sec: 1e9 / ns,
        bytes_per_msg: 0,
    }
}

// =============================================================================
// Output
// =============================================================================

fn print_header() {
    println!(
        "\n{:<30} {:>8} {:>14} {:>8}",
        "Benchmark", "ns/msg", "msgs/sec", "bytes"
    );
    println!("{:-<64}", "");
}

fn print_row(r: &BenchResult) {
    println!(
        "{:<30} {:>8.1} {:>14.0} {:>8}",
        r.label, r.ns_per_msg, r.msgs_per_sec, r.bytes_per_msg
    );
}

fn print_delta(flat: &BenchResult, env: &BenchResult) {
    let delta = flat.ns_per_msg - env.ns_per_msg;
    let pct = delta / flat.ns_per_msg * 100.0;
    let size_delta = flat.bytes_per_msg as isize - env.bytes_per_msg as isize;
    println!(
        "  → envelope saves {:.1}ns/msg ({:.1}%), {} bytes/msg",
        delta, pct, size_delta
    );
}

fn main() {
    println!("FlatMessage vs MqttEnvelope Format Comparison");
    println!("==============================================");
    println!("Messages per test: {}\n", NUM_MESSAGES);

    // --- Build cost ---
    println!("### Build cost (write path — zero-alloc writers only)");
    print_header();
    for s in SCENARIOS {
        let fw = bench_flat_writer(s);
        let ew = bench_envelope_writer(s);
        let pw = bench_packed_writer(s);
        print_row(&fw);
        print_row(&ew);
        print_row(&pw);
        let fw_pw = fw.ns_per_msg - pw.ns_per_msg;
        println!(
            "  → packed vs flat: {:.1}ns faster ({:.1}%), vs env: {:.1}ns",
            fw_pw,
            fw_pw / fw.ns_per_msg * 100.0,
            ew.ns_per_msg - pw.ns_per_msg,
        );
        println!();
    }

    // --- Read cost ---
    println!("### Read cost (all fields)");
    print_header();
    for s in SCENARIOS {
        let fr = bench_flat_read(s);
        let er = bench_envelope_read(s);
        let pr = bench_packed_read(s);
        print_row(&fr);
        print_row(&er);
        print_row(&pr);
        println!(
            "  → packed vs flat: {:.1}ns, packed vs env: {:.1}ns",
            fr.ns_per_msg - pr.ns_per_msg,
            er.ns_per_msg - pr.ns_per_msg,
        );
        println!();
    }

    // --- Build+Read roundtrip ---
    println!("### Build+Read roundtrip (write → parse → read all fields)");
    print_header();
    for s in SCENARIOS {
        let fr = bench_flat_roundtrip(s);
        let er = bench_envelope_roundtrip(s);
        let pr = bench_packed_roundtrip(s);
        print_row(&fr);
        print_row(&er);
        print_row(&pr);
        let fw_pr = fr.ns_per_msg - pr.ns_per_msg;
        println!(
            "  → packed vs flat: {:.1}ns ({:.1}%), packed vs env: {:.1}ns",
            fw_pr,
            fw_pr / fr.ns_per_msg * 100.0,
            er.ns_per_msg - pr.ns_per_msg,
        );
        println!();
    }

    // --- Wire size comparison ---
    println!("### Wire size comparison");
    println!(
        "\n{:<16} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Scenario", "FlatMsg", "Envelope", "Packed", "Flat-Env", "Flat-Pack"
    );
    println!("{:-<70}", "");
    for s in SCENARIOS {
        let payload = vec![0xABu8; s.payload_size];
        let mut b = FlatMessageBuilder::new(&payload);
        if let Some(rk) = s.routing_key {
            b = b.routing_key(rk);
        }
        if let Some(k) = s.key {
            b = b.key(k);
        }
        if let Some(rt) = s.reply_to {
            b = b.reply_to(rt);
        }
        if let Some(cid) = s.correlation_id {
            b = b.correlation_id(cid);
        }
        for (hk, hv) in s.headers {
            b = b.header(*hk, *hv);
        }
        let flat_bytes = b.timestamp(1000).publisher_id(42).build();

        let topic = s.routing_key.unwrap_or(b"");
        let props = build_props(s.reply_to, s.correlation_id, s.key, s.headers);
        let env_bytes = MqttEnvelopeBuilder::new(topic, &payload)
            .timestamp(1000)
            .publisher_id(42)
            .properties_raw(&props)
            .build();

        let mut packed_buf = BytesMut::with_capacity(4096);
        write_packed_envelope(
            &mut packed_buf,
            topic,
            &payload,
            s.key,
            s.reply_to,
            s.correlation_id,
            s.headers,
            1000,
            0,
            42,
            false,
        );

        let fe = flat_bytes.len() as isize - env_bytes.len() as isize;
        let fp = flat_bytes.len() as isize - packed_buf.len() as isize;
        println!(
            "{:<16} {:>10} {:>10} {:>10} {:>+10} {:>+10}",
            s.name,
            flat_bytes.len(),
            env_bytes.len(),
            packed_buf.len(),
            fe,
            fp,
        );
    }

    println!("\nDone.");
}
