//! Zero-copy flat message format backed by `Bytes`.
//!
//! # Wire layout
//!
//! ```text
//! [fixed header: 40 bytes]
//!   flags:            u16   (bitfield — see FLAGS_*)
//!   header_count:     u16   (number of key/value header pairs)
//!   span_count:       u16   (total variable-length spans in the index)
//!   priority:         u8    (0 = not set; message priority for ack groups)
//!   reserved:         u8    (alignment padding)
//!   timestamp:        u64
//!   ttl_ms:           u64   (0 = not set)
//!   delay_ms:         u64   (0 = not set)
//!   publisher_id:     u64   (0 = not set; MQTT client/producer ID)
//!
//! [span index: span_count × 8 bytes]
//!   Fixed order: value, [key], [routing_key], [reply_to], [correlation_id],
//!                hdr_key_0, hdr_val_0, hdr_key_1, hdr_val_1, …
//!   Each span: offset: u32 (relative to data-region start), length: u32
//!
//! [data region]
//!   Raw concatenated bytes — zero-copy sliceable from mmap.
//! ```

use bytes::{BufMut, Bytes, BytesMut};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const HEADER_SIZE: usize = 40;
const SPAN_SIZE: usize = 8; // u32 offset + u32 length

// Flag bits
const FLAG_HAS_KEY: u16 = 1 << 0;
const FLAG_HAS_TTL: u16 = 1 << 1;
const FLAG_HAS_DELAY: u16 = 1 << 2;
const FLAG_HAS_ROUTING_KEY: u16 = 1 << 3;
const FLAG_HAS_REPLY_TO: u16 = 1 << 4;
const FLAG_HAS_CORRELATION_ID: u16 = 1 << 5;
// MQTT-native flag bits
const FLAG_RETAIN: u16 = 1 << 6;
const FLAG_NO_LOCAL: u16 = 1 << 7;
const FLAG_UTF8_PAYLOAD: u16 = 1 << 8;
const FLAG_HAS_PUBLISHER_ID: u16 = 1 << 9;
const FLAG_HAS_PRIORITY: u16 = 1 << 10;

// Span index positions (value is always span 0)
const SPAN_VALUE: usize = 0;

// ---------------------------------------------------------------------------
// FlatMessageMeta — extracted from the fixed 32-byte header only
// ---------------------------------------------------------------------------

/// Lightweight metadata extracted from the first 40 bytes of a `FlatMessage`.
/// No variable-length deserialization required — suitable for the state machine
/// replay path where only timestamps and TTL/delay matter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlatMessageMeta {
    pub flags: u16,
    pub header_count: u16,
    pub span_count: u16,
    pub priority: u8,
    pub timestamp: u64,
    pub ttl_ms: u64,
    pub delay_ms: u64,
    pub publisher_id: u64,
}

impl FlatMessageMeta {
    /// Parse metadata from the first 40 bytes of `buf`.
    /// Returns `None` if the buffer is too short.
    #[inline]
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        Some(Self {
            flags: u16::from_le_bytes([buf[0], buf[1]]),
            header_count: u16::from_le_bytes([buf[2], buf[3]]),
            span_count: u16::from_le_bytes([buf[4], buf[5]]),
            priority: buf[6], // first byte of former reserved u16
            // buf[7] reserved
            timestamp: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            ttl_ms: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            delay_ms: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            publisher_id: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
        })
    }

    #[inline]
    pub fn ttl_ms_opt(&self) -> Option<u64> {
        if self.flags & FLAG_HAS_TTL != 0 {
            Some(self.ttl_ms)
        } else {
            None
        }
    }

    #[inline]
    pub fn delay_ms_opt(&self) -> Option<u64> {
        if self.flags & FLAG_HAS_DELAY != 0 {
            Some(self.delay_ms)
        } else {
            None
        }
    }

    #[inline]
    pub fn has_routing_key(&self) -> bool {
        self.flags & FLAG_HAS_ROUTING_KEY != 0
    }

    #[inline]
    pub fn has_reply_to(&self) -> bool {
        self.flags & FLAG_HAS_REPLY_TO != 0
    }

    #[inline]
    pub fn has_correlation_id(&self) -> bool {
        self.flags & FLAG_HAS_CORRELATION_ID != 0
    }

    #[inline]
    pub fn is_retain(&self) -> bool {
        self.flags & FLAG_RETAIN != 0
    }

    #[inline]
    pub fn is_no_local(&self) -> bool {
        self.flags & FLAG_NO_LOCAL != 0
    }

    #[inline]
    pub fn is_utf8_payload(&self) -> bool {
        self.flags & FLAG_UTF8_PAYLOAD != 0
    }

    /// Total byte length of the value field, read from the first span.
    /// Requires the span index to be present in `buf`.
    #[inline]
    pub fn value_len(buf: &[u8]) -> Option<u32> {
        let start = HEADER_SIZE + SPAN_VALUE * SPAN_SIZE + 4; // skip offset, read length
        if buf.len() < start + 4 {
            return None;
        }
        Some(u32::from_le_bytes(
            buf[start..start + 4].try_into().unwrap(),
        ))
    }

    /// Read priority directly from `buf` without full parse.
    /// Returns `None` if the buffer is too short or priority flag is not set.
    #[inline]
    pub fn priority(buf: &[u8]) -> Option<u8> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        let flags = u16::from_le_bytes([buf[0], buf[1]]);
        if flags & FLAG_HAS_PRIORITY != 0 {
            Some(buf[6])
        } else {
            None
        }
    }

    /// Read TTL directly from `buf` without full parse.
    /// Returns `None` if the buffer is too short or TTL flag is not set.
    #[inline]
    pub fn ttl_ms(buf: &[u8]) -> Option<u64> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        let flags = u16::from_le_bytes([buf[0], buf[1]]);
        if flags & FLAG_HAS_TTL != 0 {
            Some(u64::from_le_bytes(buf[16..24].try_into().unwrap()))
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// FlatMessage — validated zero-copy view over Bytes
// ---------------------------------------------------------------------------

/// Zero-copy view over a flat-encoded message buffer.
///
/// All accessors return `Bytes::slice()` — a refcount bump on the underlying
/// buffer (typically an mmap'd raft log segment), with zero heap allocations.
#[derive(Clone)]
pub struct FlatMessage {
    buf: Bytes,
    meta: FlatMessageMeta,
    data_offset: usize,
}

impl std::fmt::Debug for FlatMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatMessage")
            .field("len", &self.buf.len())
            .field("meta", &self.meta)
            .finish()
    }
}

impl FlatMessage {
    /// Wrap a `Bytes` buffer as a `FlatMessage`.
    /// Returns `None` if the buffer is too short or structurally invalid.
    pub fn new(buf: Bytes) -> Option<Self> {
        let meta = FlatMessageMeta::parse(&buf)?;
        let span_index_end = HEADER_SIZE + (meta.span_count as usize) * SPAN_SIZE;
        if buf.len() < span_index_end {
            return None;
        }
        Some(Self {
            meta,
            data_offset: span_index_end,
            buf,
        })
    }

    #[inline]
    pub fn meta(&self) -> &FlatMessageMeta {
        &self.meta
    }

    #[inline]
    pub fn as_bytes(&self) -> &Bytes {
        &self.buf
    }

    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.buf
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.meta.timestamp
    }

    #[inline]
    pub fn ttl_ms(&self) -> Option<u64> {
        self.meta.ttl_ms_opt()
    }

    #[inline]
    pub fn delay_ms(&self) -> Option<u64> {
        self.meta.delay_ms_opt()
    }

    #[inline]
    pub fn is_retain(&self) -> bool {
        self.meta.is_retain()
    }

    #[inline]
    pub fn is_no_local(&self) -> bool {
        self.meta.is_no_local()
    }

    #[inline]
    pub fn is_utf8_payload(&self) -> bool {
        self.meta.is_utf8_payload()
    }

    #[inline]
    pub fn publisher_id(&self) -> u64 {
        self.meta.publisher_id
    }

    // -- span accessor helpers ------------------------------------------------

    #[inline]
    fn span(&self, index: usize) -> (u32, u32) {
        let base = HEADER_SIZE + index * SPAN_SIZE;
        let offset = u32::from_le_bytes(self.buf[base..base + 4].try_into().unwrap());
        let length = u32::from_le_bytes(self.buf[base + 4..base + 8].try_into().unwrap());
        (offset, length)
    }

    #[inline]
    fn span_bytes(&self, index: usize) -> Bytes {
        let (offset, length) = self.span(index);
        let start = self.data_offset + offset as usize;
        let end = start + length as usize;
        self.buf.slice(start..end)
    }

    /// Returns the optional span index for a named field, accounting for
    /// which optional fields are present before it.
    #[inline]
    fn optional_span_index(&self, flag: u16) -> Option<usize> {
        if self.meta.flags & flag == 0 {
            return None;
        }
        // Span 0 is always value. Optional fields follow in flag-bit order.
        let mut idx = 1; // start after value
        let preceding_flags = [
            FLAG_HAS_KEY,
            FLAG_HAS_ROUTING_KEY,
            FLAG_HAS_REPLY_TO,
            FLAG_HAS_CORRELATION_ID,
        ];
        for &f in &preceding_flags {
            if f == flag {
                break;
            }
            if self.meta.flags & f != 0 {
                idx += 1;
            }
        }
        Some(idx)
    }

    // -- public field accessors -----------------------------------------------

    /// Message value (always present).
    #[inline]
    pub fn value(&self) -> Bytes {
        self.span_bytes(SPAN_VALUE)
    }

    /// Message key (optional).
    #[inline]
    pub fn key(&self) -> Option<Bytes> {
        self.optional_span_index(FLAG_HAS_KEY)
            .map(|i| self.span_bytes(i))
    }

    /// Routing key (optional).
    #[inline]
    pub fn routing_key(&self) -> Option<Bytes> {
        self.optional_span_index(FLAG_HAS_ROUTING_KEY)
            .map(|i| self.span_bytes(i))
    }

    /// Routing key as a UTF-8 string slice. Panics on invalid UTF-8.
    #[inline]
    pub fn routing_key_str(&self) -> Option<String> {
        self.routing_key()
            .map(|b| String::from_utf8(b.to_vec()).expect("routing_key must be valid UTF-8"))
    }

    /// Reply-to topic (optional).
    #[inline]
    pub fn reply_to(&self) -> Option<Bytes> {
        self.optional_span_index(FLAG_HAS_REPLY_TO)
            .map(|i| self.span_bytes(i))
    }

    /// Correlation ID (optional).
    #[inline]
    pub fn correlation_id(&self) -> Option<Bytes> {
        self.optional_span_index(FLAG_HAS_CORRELATION_ID)
            .map(|i| self.span_bytes(i))
    }

    /// Number of user headers.
    #[inline]
    pub fn header_count(&self) -> usize {
        self.meta.header_count as usize
    }

    /// Returns the (key, value) header pair at position `i`.
    /// Panics if `i >= header_count()`.
    #[inline]
    pub fn header(&self, i: usize) -> (Bytes, Bytes) {
        assert!(i < self.header_count(), "header index out of bounds");
        let base_span = self.headers_base_span();
        let key = self.span_bytes(base_span + i * 2);
        let val = self.span_bytes(base_span + i * 2 + 1);
        (key, val)
    }

    /// Iterator over all headers as (key_bytes, value_bytes) pairs.
    pub fn headers(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        let count = self.header_count();
        (0..count).map(move |i| self.header(i))
    }

    /// Compute the base span index where headers start.
    #[inline]
    fn headers_base_span(&self) -> usize {
        let mut idx = 1; // after value
        for &f in &[
            FLAG_HAS_KEY,
            FLAG_HAS_ROUTING_KEY,
            FLAG_HAS_REPLY_TO,
            FLAG_HAS_CORRELATION_ID,
        ] {
            if self.meta.flags & f != 0 {
                idx += 1;
            }
        }
        idx
    }

    /// Total size in bytes of the value field.
    #[inline]
    pub fn value_len(&self) -> u32 {
        self.span(SPAN_VALUE).1
    }
}

// ---------------------------------------------------------------------------
// FlatMessageBuilder — write-path encoder
// ---------------------------------------------------------------------------

/// Builds a flat-encoded message into a contiguous `Bytes` buffer.
///
/// Usage:
/// ```ignore
/// let msg = FlatMessageBuilder::new(value)
///     .key(some_key)
///     .timestamp(ts)
///     .ttl_ms(5000)
///     .header("content-type", b"application/json")
///     .build();
/// ```
pub struct FlatMessageBuilder {
    value: Bytes,
    key: Option<Bytes>,
    routing_key: Option<Bytes>,
    reply_to: Option<Bytes>,
    correlation_id: Option<Bytes>,
    headers: Vec<(Bytes, Bytes)>,
    timestamp: u64,
    ttl_ms: u64,
    delay_ms: u64,
    publisher_id: u64,
    priority: u8,
    flags: u16,
}

impl FlatMessageBuilder {
    pub fn new(value: Bytes) -> Self {
        Self {
            value,
            key: None,
            routing_key: None,
            reply_to: None,
            correlation_id: None,
            headers: Vec::new(),
            timestamp: 0,
            ttl_ms: 0,
            delay_ms: 0,
            publisher_id: 0,
            priority: 0,
            flags: 0,
        }
    }

    pub fn key(mut self, key: Bytes) -> Self {
        self.flags |= FLAG_HAS_KEY;
        self.key = Some(key);
        self
    }

    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        if priority != 0 {
            self.flags |= FLAG_HAS_PRIORITY;
        } else {
            self.flags &= !FLAG_HAS_PRIORITY;
        }
        self
    }

    pub fn routing_key(mut self, rk: Bytes) -> Self {
        self.flags |= FLAG_HAS_ROUTING_KEY;
        self.routing_key = Some(rk);
        self
    }

    pub fn reply_to(mut self, rt: Bytes) -> Self {
        self.flags |= FLAG_HAS_REPLY_TO;
        self.reply_to = Some(rt);
        self
    }

    pub fn correlation_id(mut self, cid: Bytes) -> Self {
        self.flags |= FLAG_HAS_CORRELATION_ID;
        self.correlation_id = Some(cid);
        self
    }

    pub fn timestamp(mut self, ts: u64) -> Self {
        self.timestamp = ts;
        self
    }

    pub fn ttl_ms(mut self, ttl: u64) -> Self {
        self.flags |= FLAG_HAS_TTL;
        self.ttl_ms = ttl;
        self
    }

    pub fn delay_ms(mut self, delay: u64) -> Self {
        self.flags |= FLAG_HAS_DELAY;
        self.delay_ms = delay;
        self
    }

    /// Alias for `ttl_ms`.
    pub fn ttl(self, ttl: u64) -> Self {
        self.ttl_ms(ttl)
    }

    /// Alias for `delay_ms`.
    pub fn delay(self, delay: u64) -> Self {
        self.delay_ms(delay)
    }

    pub fn retain(mut self, retain: bool) -> Self {
        if retain {
            self.flags |= FLAG_RETAIN;
        } else {
            self.flags &= !FLAG_RETAIN;
        }
        self
    }

    pub fn no_local(mut self, no_local: bool) -> Self {
        if no_local {
            self.flags |= FLAG_NO_LOCAL;
        } else {
            self.flags &= !FLAG_NO_LOCAL;
        }
        self
    }

    pub fn utf8_payload(mut self, utf8: bool) -> Self {
        if utf8 {
            self.flags |= FLAG_UTF8_PAYLOAD;
        } else {
            self.flags &= !FLAG_UTF8_PAYLOAD;
        }
        self
    }

    pub fn publisher_id(mut self, id: u64) -> Self {
        self.publisher_id = id;
        if id != 0 {
            self.flags |= FLAG_HAS_PUBLISHER_ID;
        } else {
            self.flags &= !FLAG_HAS_PUBLISHER_ID;
        }
        self
    }

    pub fn header(mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Encode the message into a flat `Bytes` buffer.
    pub fn build(self) -> Bytes {
        // Compute span count: 1 (value) + optional fields + 2 * header_count
        let mut span_count: u16 = 1; // value
        let optional_fields: &[Option<&Bytes>] = &[
            self.key.as_ref(),
            self.routing_key.as_ref(),
            self.reply_to.as_ref(),
            self.correlation_id.as_ref(),
        ];
        for f in optional_fields {
            if f.is_some() {
                span_count += 1;
            }
        }
        let header_count = self.headers.len() as u16;
        span_count += header_count * 2;

        // Compute data region size
        let mut data_size: usize = self.value.len();
        for f in optional_fields {
            if let Some(b) = f {
                data_size += b.len();
            }
        }
        for (k, v) in &self.headers {
            data_size += k.len() + v.len();
        }

        let total_size = HEADER_SIZE + (span_count as usize) * SPAN_SIZE + data_size;
        let mut buf = BytesMut::with_capacity(total_size);

        // -- Write fixed header (40 bytes) --
        buf.put_u16_le(self.flags);
        buf.put_u16_le(header_count);
        buf.put_u16_le(span_count);
        buf.put_u8(self.priority); // byte 6: priority
        buf.put_u8(0); // byte 7: reserved
        buf.put_u64_le(self.timestamp);
        buf.put_u64_le(self.ttl_ms);
        buf.put_u64_le(self.delay_ms);
        buf.put_u64_le(self.publisher_id);

        // -- Write span index --
        struct Span {
            offset: u32,
            length: u32,
            data: Bytes,
        }
        let mut spans: Vec<Span> = Vec::with_capacity(span_count as usize);
        let mut data_offset: u32 = 0;

        let push_span = |data: &Bytes, spans: &mut Vec<Span>, offset: &mut u32| {
            spans.push(Span {
                offset: *offset,
                length: data.len() as u32,
                data: data.clone(),
            });
            *offset += data.len() as u32;
        };

        // value (always first)
        push_span(&self.value, &mut spans, &mut data_offset);

        // optional fields in flag order
        if let Some(ref key) = self.key {
            push_span(key, &mut spans, &mut data_offset);
        }
        if let Some(ref rk) = self.routing_key {
            push_span(rk, &mut spans, &mut data_offset);
        }
        if let Some(ref rt) = self.reply_to {
            push_span(rt, &mut spans, &mut data_offset);
        }
        if let Some(ref cid) = self.correlation_id {
            push_span(cid, &mut spans, &mut data_offset);
        }

        // headers: key0, val0, key1, val1, ...
        for (k, v) in &self.headers {
            push_span(k, &mut spans, &mut data_offset);
            push_span(v, &mut spans, &mut data_offset);
        }

        // Write span index entries
        for span in &spans {
            buf.put_u32_le(span.offset);
            buf.put_u32_le(span.length);
        }

        // Write data region
        for span in &spans {
            buf.put_slice(&span.data);
        }

        debug_assert_eq!(buf.len(), total_size);
        buf.freeze()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_minimal() {
        let value = Bytes::from_static(b"hello world");
        let buf = FlatMessageBuilder::new(value.clone()).timestamp(42).build();

        let msg = FlatMessage::new(buf).unwrap();
        assert_eq!(msg.value(), value);
        assert_eq!(msg.timestamp(), 42);
        assert_eq!(msg.key(), None);
        assert_eq!(msg.routing_key(), None);
        assert_eq!(msg.reply_to(), None);
        assert_eq!(msg.correlation_id(), None);
        assert_eq!(msg.ttl_ms(), None);
        assert_eq!(msg.delay_ms(), None);
        assert_eq!(msg.header_count(), 0);
    }

    #[test]
    fn roundtrip_all_fields() {
        let buf = FlatMessageBuilder::new(Bytes::from_static(b"payload"))
            .key(Bytes::from_static(b"msg-key"))
            .timestamp(1000)
            .ttl_ms(5000)
            .delay_ms(2000)
            .routing_key(Bytes::from_static(b"orders.us"))
            .reply_to(Bytes::from_static(b"reply-topic"))
            .correlation_id(Bytes::from_static(b"corr-123"))
            .header("content-type", &b"application/json"[..])
            .header("x-trace-id", &b"abc-def"[..])
            .build();

        let msg = FlatMessage::new(buf).unwrap();
        assert_eq!(msg.value(), Bytes::from_static(b"payload"));
        assert_eq!(msg.key(), Some(Bytes::from_static(b"msg-key")));
        assert_eq!(msg.timestamp(), 1000);
        assert_eq!(msg.ttl_ms(), Some(5000));
        assert_eq!(msg.delay_ms(), Some(2000));
        assert_eq!(msg.routing_key(), Some(Bytes::from_static(b"orders.us")));
        assert_eq!(msg.reply_to(), Some(Bytes::from_static(b"reply-topic")));
        assert_eq!(msg.correlation_id(), Some(Bytes::from_static(b"corr-123")));
        assert_eq!(msg.header_count(), 2);
        let (k0, v0) = msg.header(0);
        assert_eq!(k0, Bytes::from_static(b"content-type"));
        assert_eq!(v0, Bytes::from_static(b"application/json"));
        let (k1, v1) = msg.header(1);
        assert_eq!(k1, Bytes::from_static(b"x-trace-id"));
        assert_eq!(v1, Bytes::from_static(b"abc-def"));
    }

    #[test]
    fn meta_extraction_from_header_only() {
        let buf = FlatMessageBuilder::new(Bytes::from_static(b"data"))
            .timestamp(999)
            .ttl_ms(3000)
            .delay_ms(1500)
            .build();

        let meta = FlatMessageMeta::parse(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(meta.timestamp, 999);
        assert_eq!(meta.ttl_ms_opt(), Some(3000));
        assert_eq!(meta.delay_ms_opt(), Some(1500));
    }

    #[test]
    fn meta_no_ttl_no_delay() {
        let buf = FlatMessageBuilder::new(Bytes::from_static(b"x"))
            .timestamp(1)
            .build();

        let meta = FlatMessageMeta::parse(&buf).unwrap();
        assert_eq!(meta.ttl_ms_opt(), None);
        assert_eq!(meta.delay_ms_opt(), None);
    }

    #[test]
    fn value_len_from_meta() {
        let buf = FlatMessageBuilder::new(Bytes::from(vec![0u8; 1024]))
            .timestamp(0)
            .build();

        assert_eq!(FlatMessageMeta::value_len(&buf), Some(1024));
    }

    #[test]
    fn subset_optional_fields() {
        // Only key and correlation_id — no routing_key or reply_to
        let buf = FlatMessageBuilder::new(Bytes::from_static(b"val"))
            .key(Bytes::from_static(b"k"))
            .correlation_id(Bytes::from_static(b"cid"))
            .timestamp(7)
            .build();

        let msg = FlatMessage::new(buf).unwrap();
        assert_eq!(msg.value(), Bytes::from_static(b"val"));
        assert_eq!(msg.key(), Some(Bytes::from_static(b"k")));
        assert_eq!(msg.routing_key(), None);
        assert_eq!(msg.reply_to(), None);
        assert_eq!(msg.correlation_id(), Some(Bytes::from_static(b"cid")));
    }

    #[test]
    fn empty_value() {
        let buf = FlatMessageBuilder::new(Bytes::new()).timestamp(0).build();
        let msg = FlatMessage::new(buf).unwrap();
        assert_eq!(msg.value(), Bytes::new());
        assert_eq!(msg.value_len(), 0);
    }

    #[test]
    fn headers_iterator() {
        let buf = FlatMessageBuilder::new(Bytes::from_static(b"v"))
            .header("a", &b"1"[..])
            .header("b", &b"2"[..])
            .header("c", &b"3"[..])
            .build();

        let msg = FlatMessage::new(buf).unwrap();
        let hdrs: Vec<_> = msg.headers().collect();
        assert_eq!(hdrs.len(), 3);
        assert_eq!(hdrs[0].0, Bytes::from_static(b"a"));
        assert_eq!(hdrs[2].1, Bytes::from_static(b"3"));
    }

    #[test]
    fn too_short_buffer_returns_none() {
        assert!(FlatMessage::new(Bytes::from_static(b"short")).is_none());
        assert!(FlatMessageMeta::parse(b"short").is_none());
    }
}
