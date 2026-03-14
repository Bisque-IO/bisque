//! Zero-copy flat message formats.
//!
//! # FlatMessage wire layout
//!
//! ```text
//! [fixed header: 40 bytes]
//!   flags:            u16   (bitfield — see FLAG_*)
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
//!
//! # MqttEnvelope wire layout
//!
//! ```text
//! [fixed header: 40 bytes]
//!   flags:            u16   (bit 15 = FORMAT_MQTT, bit 0 = RETAIN, bit 1 = IS_V5,
//!                            bit 2 = HAS_TTL, bit 7 = NO_LOCAL, bit 8 = UTF8_PAYLOAD)
//!   reserved:         u16
//!   topic_len:        u16   (routing key length)
//!   props_len:        u16   (raw MQTT 5.0 property bytes length; 0 for V3.1.1)
//!   timestamp:        u64
//!   ttl_ms:           u64
//!   publisher_id:     u64
//!   padding:          u64   (future use)
//!
//! [topic: topic_len]        raw UTF-8 topic string
//! [properties: props_len]   raw MQTT 5.0 property wire bytes
//! [payload: remaining]      message payload
//! ```

use bytes::{BufMut, Bytes, BytesMut};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const HEADER_SIZE: usize = 40;
const SPAN_SIZE: usize = 8; // u32 offset + u32 length

// FlatMessage flag bits
const FLAG_HAS_KEY: u16 = 1 << 0;
const FLAG_HAS_TTL: u16 = 1 << 1;
const FLAG_HAS_DELAY: u16 = 1 << 2;
const FLAG_HAS_ROUTING_KEY: u16 = 1 << 3;
const FLAG_HAS_REPLY_TO: u16 = 1 << 4;
const FLAG_HAS_CORRELATION_ID: u16 = 1 << 5;
const FLAG_RETAIN: u16 = 1 << 6;
const FLAG_NO_LOCAL: u16 = 1 << 7;
const FLAG_UTF8_PAYLOAD: u16 = 1 << 8;
const FLAG_HAS_PUBLISHER_ID: u16 = 1 << 9;
const FLAG_HAS_PRIORITY: u16 = 1 << 10;

// MqttEnvelope flag bits (shares some with FlatMessage)
pub const FLAG_FORMAT_MQTT: u16 = 1 << 15;
const MQTT_FLAG_RETAIN: u16 = 1 << 0;
const MQTT_FLAG_IS_V5: u16 = 1 << 1;
const MQTT_FLAG_HAS_TTL: u16 = 1 << 2;
const MQTT_FLAG_NO_LOCAL: u16 = 1 << 7;
const MQTT_FLAG_UTF8_PAYLOAD: u16 = 1 << 8;

// MqttEnvelope header size (same as FlatMessage for alignment)
pub const MQTT_ENVELOPE_HEADER_SIZE: usize = 40;

// Span index positions (value is always span 0)
const SPAN_VALUE: usize = 0;

// ---------------------------------------------------------------------------
// Format discrimination
// ---------------------------------------------------------------------------

/// Check if a message buffer is an MqttEnvelope (bit 15 of flags is set).
#[inline]
pub fn is_mqtt_envelope(buf: &[u8]) -> bool {
    buf.len() >= 2 && u16::from_le_bytes([buf[0], buf[1]]) & FLAG_FORMAT_MQTT != 0
}

// ---------------------------------------------------------------------------
// FlatMessageMeta — extracted from the fixed 40-byte header only
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
// FlatMessage — validated zero-copy view over &[u8]
// ---------------------------------------------------------------------------

/// Zero-copy view over a flat-encoded message buffer.
///
/// All accessors return `&[u8]` slices into the backing buffer — no refcount
/// bumps, no heap allocations.
#[derive(Clone, Copy)]
pub struct FlatMessage<'a> {
    buf: &'a [u8],
    meta: FlatMessageMeta,
    data_offset: usize,
}

impl<'a> std::fmt::Debug for FlatMessage<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatMessage")
            .field("len", &self.buf.len())
            .field("meta", &self.meta)
            .finish()
    }
}

impl<'a> FlatMessage<'a> {
    /// Wrap a byte slice as a `FlatMessage`.
    /// Returns `None` if the buffer is too short or structurally invalid.
    pub fn new(buf: &'a [u8]) -> Option<Self> {
        let meta = FlatMessageMeta::parse(buf)?;
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
    pub fn as_bytes(&self) -> &'a [u8] {
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
    fn span_slice(&self, index: usize) -> &'a [u8] {
        let (offset, length) = self.span(index);
        let start = self.data_offset + offset as usize;
        let end = start + length as usize;
        &self.buf[start..end]
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
    pub fn value(&self) -> &'a [u8] {
        self.span_slice(SPAN_VALUE)
    }

    /// Message key (optional).
    #[inline]
    pub fn key(&self) -> Option<&'a [u8]> {
        self.optional_span_index(FLAG_HAS_KEY)
            .map(|i| self.span_slice(i))
    }

    /// Routing key (optional).
    #[inline]
    pub fn routing_key(&self) -> Option<&'a [u8]> {
        self.optional_span_index(FLAG_HAS_ROUTING_KEY)
            .map(|i| self.span_slice(i))
    }

    /// Routing key as a UTF-8 string slice. Returns `None` if not present or invalid UTF-8.
    #[inline]
    pub fn routing_key_str(&self) -> Option<&'a str> {
        self.routing_key().and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Reply-to topic (optional).
    #[inline]
    pub fn reply_to(&self) -> Option<&'a [u8]> {
        self.optional_span_index(FLAG_HAS_REPLY_TO)
            .map(|i| self.span_slice(i))
    }

    /// Correlation ID (optional).
    #[inline]
    pub fn correlation_id(&self) -> Option<&'a [u8]> {
        self.optional_span_index(FLAG_HAS_CORRELATION_ID)
            .map(|i| self.span_slice(i))
    }

    /// Number of user headers.
    #[inline]
    pub fn header_count(&self) -> usize {
        self.meta.header_count as usize
    }

    /// Returns the (key, value) header pair at position `i`.
    /// Panics if `i >= header_count()`.
    #[inline]
    pub fn header(&self, i: usize) -> (&'a [u8], &'a [u8]) {
        assert!(i < self.header_count(), "header index out of bounds");
        let base_span = self.headers_base_span();
        let key = self.span_slice(base_span + i * 2);
        let val = self.span_slice(base_span + i * 2 + 1);
        (key, val)
    }

    /// Iterator over all headers as (key_bytes, value_bytes) pairs.
    pub fn headers(&self) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + '_ {
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

/// Builds a flat-encoded message into a caller-owned `BytesMut` — zero
/// intermediate allocations, zero atomics.
///
/// Usage:
/// ```ignore
/// let mut buf = BytesMut::with_capacity(4096);
/// let start = FlatMessageBuilder::new(value)
///     .key(some_key)
///     .timestamp(ts)
///     .ttl_ms(5000)
///     .header(b"content-type", b"application/json")
///     .build_into(&mut buf);
/// let bytes = buf.split().freeze(); // if you need Bytes
/// ```
pub struct FlatMessageBuilder<'a> {
    value: &'a [u8],
    key: Option<&'a [u8]>,
    routing_key: Option<&'a [u8]>,
    reply_to: Option<&'a [u8]>,
    correlation_id: Option<&'a [u8]>,
    headers: Vec<(&'a [u8], &'a [u8])>,
    timestamp: u64,
    ttl_ms: u64,
    delay_ms: u64,
    publisher_id: u64,
    priority: u8,
    flags: u16,
}

impl<'a> FlatMessageBuilder<'a> {
    pub fn new(value: &'a [u8]) -> Self {
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

    pub fn key(mut self, key: &'a [u8]) -> Self {
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

    pub fn routing_key(mut self, rk: &'a [u8]) -> Self {
        self.flags |= FLAG_HAS_ROUTING_KEY;
        self.routing_key = Some(rk);
        self
    }

    pub fn reply_to(mut self, rt: &'a [u8]) -> Self {
        self.flags |= FLAG_HAS_REPLY_TO;
        self.reply_to = Some(rt);
        self
    }

    pub fn correlation_id(mut self, cid: &'a [u8]) -> Self {
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

    pub fn header(mut self, key: &'a [u8], value: &'a [u8]) -> Self {
        self.headers.push((key, value));
        self
    }

    /// Write the flat message into a caller-owned `BytesMut`. Returns the
    /// start offset within `buf`. Zero intermediate allocations.
    #[inline]
    pub fn build_into(self, buf: &mut BytesMut) -> usize {
        let headers: Vec<(&[u8], &[u8])> = self.headers;
        // Extra flags set by the builder that write_flat_message doesn't know about
        let extra = self.flags & (FLAG_NO_LOCAL | FLAG_UTF8_PAYLOAD);
        let start = write_flat_message(
            buf,
            self.value,
            self.key,
            self.routing_key,
            self.reply_to,
            self.correlation_id,
            &headers,
            self.timestamp,
            self.ttl_ms,
            self.delay_ms,
            self.publisher_id,
            self.priority,
            self.flags & FLAG_RETAIN != 0,
        );
        if extra != 0 {
            let existing = u16::from_le_bytes([buf[start], buf[start + 1]]);
            let patched = existing | extra;
            buf[start..start + 2].copy_from_slice(&patched.to_le_bytes());
        }
        start
    }

    /// Convenience: allocate a new buffer and return frozen `Bytes`.
    /// Prefer `build_into()` on hot paths to avoid the allocation.
    pub fn build(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(256);
        self.build_into(&mut buf);
        buf.freeze()
    }
}

// ---------------------------------------------------------------------------
// write_flat_message — zero-alloc write into caller-owned buffer
// ---------------------------------------------------------------------------

/// Inner write logic — works on any `&mut [u8]` output region of exactly
/// the right size.
#[inline]
pub fn write_flat_message_inner(
    out: &mut [u8],
    value: &[u8],
    optional: &[Option<&[u8]>; 4],
    headers: &[(&[u8], &[u8])],
    flags: u16,
    header_count: u16,
    span_count: u16,
    span_bytes: usize,
    timestamp: u64,
    ttl_ms: u64,
    delay_ms: u64,
    publisher_id: u64,
    priority: u8,
) {
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

    let mut si = HEADER_SIZE;
    let mut di = HEADER_SIZE + span_bytes;
    let mut data_off: u32 = 0;

    macro_rules! emit {
        ($d:expr) => {{
            let len = $d.len() as u32;
            out[si..si + 4].copy_from_slice(&data_off.to_le_bytes());
            out[si + 4..si + 8].copy_from_slice(&len.to_le_bytes());
            si += 8;
            out[di..di + len as usize].copy_from_slice($d);
            di += len as usize;
            data_off += len;
        }};
    }

    emit!(value);
    for f in optional {
        if let Some(b) = f {
            emit!(b);
        }
    }
    for (k, v) in headers {
        emit!(k);
        emit!(v);
    }

    debug_assert_eq!(si, HEADER_SIZE + span_bytes);
}

/// Compute layout for a flat message. Returns `(flags, span_count, data_size, total_size)`.
#[inline]
pub fn flat_message_layout(
    value: &[u8],
    optional: &[Option<&[u8]>; 4],
    headers: &[(&[u8], &[u8])],
    ttl_ms: u64,
    delay_ms: u64,
    publisher_id: u64,
    priority: u8,
    retain: bool,
) -> (u16, u16, usize, usize) {
    let mut flags: u16 = 0;
    let mut span_count: u16 = 1; // value
    let opt_flags = [
        FLAG_HAS_KEY,
        FLAG_HAS_ROUTING_KEY,
        FLAG_HAS_REPLY_TO,
        FLAG_HAS_CORRELATION_ID,
    ];
    let mut data_size = value.len();
    for i in 0..4 {
        if let Some(b) = optional[i] {
            flags |= opt_flags[i];
            span_count += 1;
            data_size += b.len();
        }
    }
    if ttl_ms != 0 {
        flags |= FLAG_HAS_TTL;
    }
    if delay_ms != 0 {
        flags |= FLAG_HAS_DELAY;
    }
    if publisher_id != 0 {
        flags |= FLAG_HAS_PUBLISHER_ID;
    }
    if priority != 0 {
        flags |= FLAG_HAS_PRIORITY;
    }
    if retain {
        flags |= FLAG_RETAIN;
    }
    let header_count = headers.len() as u16;
    span_count += header_count * 2;
    for (k, v) in headers {
        data_size += k.len() + v.len();
    }

    let span_bytes = span_count as usize * SPAN_SIZE;
    let total = HEADER_SIZE + span_bytes + data_size;
    (flags, span_count, data_size, total)
}

/// Write a `FlatMessage` directly into a caller-owned `&mut BytesMut`, returning
/// the start offset within the buffer. Single resize + direct slice writes —
/// no intermediate allocations.
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
    let optional = [key, routing_key, reply_to, correlation_id];
    let (flags, span_count, _data_size, total) = flat_message_layout(
        value,
        &optional,
        headers,
        ttl_ms,
        delay_ms,
        publisher_id,
        priority,
        retain,
    );
    let header_count = headers.len() as u16;
    let span_bytes = span_count as usize * SPAN_SIZE;

    let start = buf.len();
    buf.resize(start + total, 0);
    write_flat_message_inner(
        &mut buf[start..start + total],
        value,
        &optional,
        headers,
        flags,
        header_count,
        span_count,
        span_bytes,
        timestamp,
        ttl_ms,
        delay_ms,
        publisher_id,
        priority,
    );
    start
}

/// Write a `FlatMessage` directly into a caller-owned `&mut Vec<u8>`, returning
/// the start offset within the buffer. Same zero-alloc logic as the `BytesMut`
/// variant — use this when writing into `CommandBuilder` or other `Vec`-backed buffers.
#[inline]
pub fn write_flat_message_vec(
    buf: &mut Vec<u8>,
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
    let optional = [key, routing_key, reply_to, correlation_id];
    let (flags, span_count, _data_size, total) = flat_message_layout(
        value,
        &optional,
        headers,
        ttl_ms,
        delay_ms,
        publisher_id,
        priority,
        retain,
    );
    let header_count = headers.len() as u16;
    let span_bytes = span_count as usize * SPAN_SIZE;

    let start = buf.len();
    buf.resize(start + total, 0);
    write_flat_message_inner(
        &mut buf[start..start + total],
        value,
        &optional,
        headers,
        flags,
        header_count,
        span_count,
        span_bytes,
        timestamp,
        ttl_ms,
        delay_ms,
        publisher_id,
        priority,
    );
    start
}

// ===========================================================================
// MqttEnvelope — zero-copy view for MQTT-native messages
// ===========================================================================

/// Lightweight metadata extracted from the first 40 bytes of an `MqttEnvelope`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MqttEnvelopeMeta {
    pub flags: u16,
    pub topic_len: u16,
    pub props_len: u16,
    pub timestamp: u64,
    pub ttl_ms: u64,
    pub publisher_id: u64,
}

impl MqttEnvelopeMeta {
    /// Parse metadata from the first 40 bytes of an MqttEnvelope buffer.
    /// Returns `None` if the buffer is too short or not an MqttEnvelope.
    #[inline]
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < MQTT_ENVELOPE_HEADER_SIZE {
            return None;
        }
        let flags = u16::from_le_bytes([buf[0], buf[1]]);
        if flags & FLAG_FORMAT_MQTT == 0 {
            return None;
        }
        Some(Self {
            flags,
            // buf[2..4] reserved
            topic_len: u16::from_le_bytes([buf[4], buf[5]]),
            props_len: u16::from_le_bytes([buf[6], buf[7]]),
            timestamp: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            ttl_ms: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            publisher_id: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            // buf[32..40] padding
        })
    }

    /// Compute the value (payload) length for an MqttEnvelope.
    #[inline]
    pub fn value_len(buf: &[u8]) -> Option<u32> {
        let meta = Self::parse(buf)?;
        let var_start =
            MQTT_ENVELOPE_HEADER_SIZE + meta.topic_len as usize + meta.props_len as usize;
        if buf.len() < var_start {
            return None;
        }
        Some((buf.len() - var_start) as u32)
    }
}

/// Zero-copy view over an MQTT-native envelope buffer.
///
/// All accessors return `&[u8]` slices into the backing buffer.
#[derive(Clone, Copy)]
pub struct MqttEnvelope<'a> {
    buf: &'a [u8],
    meta: MqttEnvelopeMeta,
}

impl<'a> std::fmt::Debug for MqttEnvelope<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttEnvelope")
            .field("len", &self.buf.len())
            .field("meta", &self.meta)
            .finish()
    }
}

impl<'a> MqttEnvelope<'a> {
    /// Wrap a byte slice as an `MqttEnvelope`.
    /// Returns `None` if the buffer is too short or not an MqttEnvelope.
    pub fn new(buf: &'a [u8]) -> Option<Self> {
        let meta = MqttEnvelopeMeta::parse(buf)?;
        let min_len = MQTT_ENVELOPE_HEADER_SIZE + meta.topic_len as usize + meta.props_len as usize;
        if buf.len() < min_len {
            return None;
        }
        Some(Self { buf, meta })
    }

    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.buf
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.meta.timestamp
    }

    #[inline]
    pub fn ttl_ms(&self) -> Option<u64> {
        if self.meta.flags & MQTT_FLAG_HAS_TTL != 0 {
            Some(self.meta.ttl_ms)
        } else {
            None
        }
    }

    #[inline]
    pub fn publisher_id(&self) -> u64 {
        self.meta.publisher_id
    }

    #[inline]
    pub fn is_retain(&self) -> bool {
        self.meta.flags & MQTT_FLAG_RETAIN != 0
    }

    #[inline]
    pub fn is_v5(&self) -> bool {
        self.meta.flags & MQTT_FLAG_IS_V5 != 0
    }

    #[inline]
    pub fn is_no_local(&self) -> bool {
        self.meta.flags & MQTT_FLAG_NO_LOCAL != 0
    }

    #[inline]
    pub fn is_utf8_payload(&self) -> bool {
        self.meta.flags & MQTT_FLAG_UTF8_PAYLOAD != 0
    }

    /// Topic bytes (routing key).
    #[inline]
    pub fn topic(&self) -> &'a [u8] {
        let start = MQTT_ENVELOPE_HEADER_SIZE;
        let end = start + self.meta.topic_len as usize;
        &self.buf[start..end]
    }

    /// Raw MQTT 5.0 property bytes (empty for V3.1.1).
    #[inline]
    pub fn properties_raw(&self) -> &'a [u8] {
        let start = MQTT_ENVELOPE_HEADER_SIZE + self.meta.topic_len as usize;
        let end = start + self.meta.props_len as usize;
        &self.buf[start..end]
    }

    /// Message payload.
    #[inline]
    pub fn payload(&self) -> &'a [u8] {
        let start =
            MQTT_ENVELOPE_HEADER_SIZE + self.meta.topic_len as usize + self.meta.props_len as usize;
        &self.buf[start..]
    }

    /// Reply-to topic from MQTT 5.0 properties (property ID 0x08 = response topic).
    #[inline]
    pub fn reply_to(&self) -> Option<&'a [u8]> {
        find_utf8_property(self.properties_raw(), 0x08)
    }

    /// Correlation data from MQTT 5.0 properties (property ID 0x09).
    #[inline]
    pub fn correlation_id(&self) -> Option<&'a [u8]> {
        find_binary_property(self.properties_raw(), 0x09)
    }
}

// ---------------------------------------------------------------------------
// MqttEnvelopeBuilder — write-path encoder (returns Bytes)
// ---------------------------------------------------------------------------

/// Builds an MqttEnvelope into a contiguous `Bytes` buffer.
pub struct MqttEnvelopeBuilder {
    topic: Bytes,
    payload: Bytes,
    props_raw: Bytes,
    timestamp: u64,
    ttl_ms: u64,
    publisher_id: u64,
    flags: u16,
}

impl MqttEnvelopeBuilder {
    pub fn new(topic: &[u8], payload: &[u8]) -> Self {
        Self {
            topic: Bytes::copy_from_slice(topic),
            payload: Bytes::copy_from_slice(payload),
            props_raw: Bytes::new(),
            timestamp: 0,
            ttl_ms: 0,
            publisher_id: 0,
            flags: FLAG_FORMAT_MQTT,
        }
    }

    pub fn timestamp(mut self, ts: u64) -> Self {
        self.timestamp = ts;
        self
    }

    pub fn publisher_id(mut self, id: u64) -> Self {
        self.publisher_id = id;
        self
    }

    pub fn ttl_ms(mut self, ttl: u64) -> Self {
        if ttl != 0 {
            self.flags |= MQTT_FLAG_HAS_TTL;
        }
        self.ttl_ms = ttl;
        self
    }

    pub fn retain(mut self, r: bool) -> Self {
        if r {
            self.flags |= MQTT_FLAG_RETAIN;
        } else {
            self.flags &= !MQTT_FLAG_RETAIN;
        }
        self
    }

    pub fn is_v5(mut self, v: bool) -> Self {
        if v {
            self.flags |= MQTT_FLAG_IS_V5;
        } else {
            self.flags &= !MQTT_FLAG_IS_V5;
        }
        self
    }

    pub fn no_local(mut self, n: bool) -> Self {
        if n {
            self.flags |= MQTT_FLAG_NO_LOCAL;
        } else {
            self.flags &= !MQTT_FLAG_NO_LOCAL;
        }
        self
    }

    pub fn utf8_payload(mut self, u: bool) -> Self {
        if u {
            self.flags |= MQTT_FLAG_UTF8_PAYLOAD;
        } else {
            self.flags &= !MQTT_FLAG_UTF8_PAYLOAD;
        }
        self
    }

    pub fn properties_raw(mut self, props: &[u8]) -> Self {
        self.props_raw = Bytes::copy_from_slice(props);
        self
    }

    /// Encode the envelope into a contiguous `Bytes` buffer.
    pub fn build(self) -> Bytes {
        let total = MQTT_ENVELOPE_HEADER_SIZE
            + self.topic.len()
            + self.props_raw.len()
            + self.payload.len();
        let mut buf = BytesMut::with_capacity(total);

        // Fixed header (40 bytes)
        buf.put_u16_le(self.flags);
        buf.put_u16_le(0); // reserved
        buf.put_u16_le(self.topic.len() as u16);
        buf.put_u16_le(self.props_raw.len() as u16);
        buf.put_u64_le(self.timestamp);
        buf.put_u64_le(self.ttl_ms);
        buf.put_u64_le(self.publisher_id);
        buf.put_u64_le(0); // padding

        // Variable data
        buf.put_slice(&self.topic);
        buf.put_slice(&self.props_raw);
        buf.put_slice(&self.payload);

        debug_assert_eq!(buf.len(), total);
        buf.freeze()
    }
}

// ---------------------------------------------------------------------------
// MqttEnvelopeWriter — zero-alloc write into caller-owned BytesMut
// ---------------------------------------------------------------------------

/// Writes an `MqttEnvelope` directly into a caller-owned `&mut BytesMut`.
///
/// The header is fully written on construction — setter methods mutate the
/// already-written bytes in place. Call [`finish`](Self::finish) to obtain
/// the start offset.
pub struct MqttEnvelopeWriter<'a> {
    buf: &'a mut BytesMut,
    start: usize,
}

impl<'a> MqttEnvelopeWriter<'a> {
    pub fn new(buf: &'a mut BytesMut, topic: &[u8], payload: &[u8]) -> Self {
        let start = buf.len();
        let total = MQTT_ENVELOPE_HEADER_SIZE + topic.len() + payload.len();
        buf.reserve(total);

        buf.put_u16_le(FLAG_FORMAT_MQTT); // flags with FORMAT_MQTT bit
        buf.put_u16_le(0); // reserved
        buf.put_u16_le(topic.len() as u16); // topic_len
        buf.put_u16_le(0); // props_len
        buf.put_u64_le(0); // timestamp
        buf.put_u64_le(0); // ttl_ms
        buf.put_u64_le(0); // publisher_id
        buf.put_u64_le(0); // padding

        buf.put_slice(topic);
        buf.put_slice(payload);

        Self { buf, start }
    }

    pub fn timestamp(self, ts: u64) -> Self {
        let s = self.start;
        self.buf[s + 8..s + 16].copy_from_slice(&ts.to_le_bytes());
        self
    }

    pub fn publisher_id(self, id: u64) -> Self {
        let s = self.start;
        self.buf[s + 24..s + 32].copy_from_slice(&id.to_le_bytes());
        self
    }

    pub fn ttl_ms(self, ttl: u64) -> Self {
        let s = self.start;
        if ttl != 0 {
            let flags = u16::from_le_bytes([self.buf[s], self.buf[s + 1]]);
            self.buf[s..s + 2].copy_from_slice(&(flags | MQTT_FLAG_HAS_TTL).to_le_bytes());
        }
        self.buf[s + 16..s + 24].copy_from_slice(&ttl.to_le_bytes());
        self
    }

    pub fn retain(self, r: bool) -> Self {
        let s = self.start;
        let flags = u16::from_le_bytes([self.buf[s], self.buf[s + 1]]);
        let new = if r {
            flags | MQTT_FLAG_RETAIN
        } else {
            flags & !MQTT_FLAG_RETAIN
        };
        self.buf[s..s + 2].copy_from_slice(&new.to_le_bytes());
        self
    }

    pub fn is_v5(self, v: bool) -> Self {
        let s = self.start;
        let flags = u16::from_le_bytes([self.buf[s], self.buf[s + 1]]);
        let new = if v {
            flags | MQTT_FLAG_IS_V5
        } else {
            flags & !MQTT_FLAG_IS_V5
        };
        self.buf[s..s + 2].copy_from_slice(&new.to_le_bytes());
        self
    }

    pub fn no_local(self, nl: bool) -> Self {
        let s = self.start;
        let flags = u16::from_le_bytes([self.buf[s], self.buf[s + 1]]);
        let new = if nl {
            flags | MQTT_FLAG_NO_LOCAL
        } else {
            flags & !MQTT_FLAG_NO_LOCAL
        };
        self.buf[s..s + 2].copy_from_slice(&new.to_le_bytes());
        self
    }

    pub fn utf8_payload(self, u: bool) -> Self {
        let s = self.start;
        let flags = u16::from_le_bytes([self.buf[s], self.buf[s + 1]]);
        let new = if u {
            flags | MQTT_FLAG_UTF8_PAYLOAD
        } else {
            flags & !MQTT_FLAG_UTF8_PAYLOAD
        };
        self.buf[s..s + 2].copy_from_slice(&new.to_le_bytes());
        self
    }

    /// Insert raw MQTT 5.0 property bytes between topic and payload.
    pub fn properties_raw(self, props: &[u8]) -> Self {
        if props.is_empty() {
            return self;
        }
        let s = self.start;
        let topic_len = u16::from_le_bytes([self.buf[s + 4], self.buf[s + 5]]) as usize;
        let insert_at = s + MQTT_ENVELOPE_HEADER_SIZE + topic_len;
        let old_len = self.buf.len();
        self.buf.resize(old_len + props.len(), 0);
        self.buf
            .copy_within(insert_at..old_len, insert_at + props.len());
        self.buf[insert_at..insert_at + props.len()].copy_from_slice(props);
        self.buf[s + 6..s + 8].copy_from_slice(&(props.len() as u16).to_le_bytes());
        self
    }

    /// Returns start offset. Header is already fully written — no finalize step.
    pub fn finish(self) -> usize {
        self.start
    }
}

// ---------------------------------------------------------------------------
// MQTT property helpers
// ---------------------------------------------------------------------------

/// Find a UTF-8 string property (2-byte length prefixed) by property ID.
fn find_utf8_property<'a>(raw: &'a [u8], target_id: u8) -> Option<&'a [u8]> {
    let mut pos = 0;
    while pos < raw.len() {
        let id = raw[pos];
        pos += 1;
        if id == target_id {
            // UTF-8 string: 2-byte length prefix
            if pos + 2 > raw.len() {
                return None;
            }
            let len = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
            pos += 2;
            if pos + len > raw.len() {
                return None;
            }
            return Some(&raw[pos..pos + len]);
        }
        // Skip this property
        if let Some(next) = skip_mqtt_property(raw, pos, id) {
            pos = next;
        } else {
            return None;
        }
    }
    None
}

/// Find a binary data property (2-byte length prefixed) by property ID.
fn find_binary_property<'a>(raw: &'a [u8], target_id: u8) -> Option<&'a [u8]> {
    let mut pos = 0;
    while pos < raw.len() {
        let id = raw[pos];
        pos += 1;
        if id == target_id {
            // Binary data: 2-byte length prefix
            if pos + 2 > raw.len() {
                return None;
            }
            let len = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
            pos += 2;
            if pos + len > raw.len() {
                return None;
            }
            return Some(&raw[pos..pos + len]);
        }
        if let Some(next) = skip_mqtt_property(raw, pos, id) {
            pos = next;
        } else {
            return None;
        }
    }
    None
}

/// Skip over an MQTT 5.0 property value starting at `pos` for the given `id`.
/// Returns the position after the property value, or `None` on malformed data.
///
/// MQTT 5.0 property value formats:
/// - Byte (1): IDs 0x01, 0x17, 0x19, 0x24, 0x25, 0x28, 0x29, 0x2A
/// - Two Byte Integer (2): IDs 0x13, 0x21, 0x23
/// - Four Byte Integer (4): IDs 0x02, 0x11, 0x18, 0x22, 0x27
/// - Variable Byte Integer: IDs 0x0B
/// - UTF-8 String (2+len): IDs 0x03, 0x08, 0x12, 0x15, 0x1A, 0x1C, 0x1F
/// - Binary Data (2+len): IDs 0x09, 0x16
/// - UTF-8 String Pair (2+k_len+2+v_len): ID 0x26
pub fn skip_mqtt_property(raw: &[u8], pos: usize, id: u8) -> Option<usize> {
    match id {
        // Byte
        0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => {
            if pos + 1 > raw.len() {
                return None;
            }
            Some(pos + 1)
        }
        // Two Byte Integer
        0x13 | 0x21 | 0x23 => {
            if pos + 2 > raw.len() {
                return None;
            }
            Some(pos + 2)
        }
        // Four Byte Integer
        0x02 | 0x11 | 0x18 | 0x22 | 0x27 => {
            if pos + 4 > raw.len() {
                return None;
            }
            Some(pos + 4)
        }
        // Variable Byte Integer
        0x0B => {
            let mut p = pos;
            loop {
                if p >= raw.len() {
                    return None;
                }
                let b = raw[p];
                p += 1;
                if b & 0x80 == 0 {
                    break;
                }
            }
            Some(p)
        }
        // UTF-8 String
        0x03 | 0x08 | 0x12 | 0x15 | 0x1A | 0x1C | 0x1F => {
            if pos + 2 > raw.len() {
                return None;
            }
            let len = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
            let end = pos + 2 + len;
            if end > raw.len() {
                return None;
            }
            Some(end)
        }
        // Binary Data
        0x09 | 0x16 => {
            if pos + 2 > raw.len() {
                return None;
            }
            let len = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
            let end = pos + 2 + len;
            if end > raw.len() {
                return None;
            }
            Some(end)
        }
        // UTF-8 String Pair (user property)
        0x26 => {
            if pos + 2 > raw.len() {
                return None;
            }
            let k_len = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
            let v_start = pos + 2 + k_len;
            if v_start + 2 > raw.len() {
                return None;
            }
            let v_len = u16::from_be_bytes([raw[v_start], raw[v_start + 1]]) as usize;
            let end = v_start + 2 + v_len;
            if end > raw.len() {
                return None;
            }
            Some(end)
        }
        _ => None,
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
        let buf = FlatMessageBuilder::new(b"hello world")
            .timestamp(42)
            .build();

        let msg = FlatMessage::new(&buf).unwrap();
        assert_eq!(msg.value(), b"hello world");
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
        let buf = FlatMessageBuilder::new(b"payload")
            .key(b"msg-key")
            .timestamp(1000)
            .ttl_ms(5000)
            .delay_ms(2000)
            .routing_key(b"orders.us")
            .reply_to(b"reply-topic")
            .correlation_id(b"corr-123")
            .header(b"content-type", b"application/json")
            .header(b"x-trace-id", b"abc-def")
            .build();

        let msg = FlatMessage::new(&buf).unwrap();
        assert_eq!(msg.value(), b"payload");
        assert_eq!(msg.key(), Some(b"msg-key".as_ref()));
        assert_eq!(msg.timestamp(), 1000);
        assert_eq!(msg.ttl_ms(), Some(5000));
        assert_eq!(msg.delay_ms(), Some(2000));
        assert_eq!(msg.routing_key(), Some(b"orders.us".as_ref()));
        assert_eq!(msg.reply_to(), Some(b"reply-topic".as_ref()));
        assert_eq!(msg.correlation_id(), Some(b"corr-123".as_ref()));
        assert_eq!(msg.header_count(), 2);
        let (k0, v0) = msg.header(0);
        assert_eq!(k0, b"content-type");
        assert_eq!(v0, b"application/json");
        let (k1, v1) = msg.header(1);
        assert_eq!(k1, b"x-trace-id");
        assert_eq!(v1, b"abc-def");
    }

    #[test]
    fn meta_extraction_from_header_only() {
        let buf = FlatMessageBuilder::new(b"data")
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
        let buf = FlatMessageBuilder::new(b"x").timestamp(1).build();

        let meta = FlatMessageMeta::parse(&buf).unwrap();
        assert_eq!(meta.ttl_ms_opt(), None);
        assert_eq!(meta.delay_ms_opt(), None);
    }

    #[test]
    fn value_len_from_meta() {
        let data = vec![0u8; 1024];
        let buf = FlatMessageBuilder::new(&data).timestamp(0).build();

        assert_eq!(FlatMessageMeta::value_len(&buf), Some(1024));
    }

    #[test]
    fn subset_optional_fields() {
        // Only key and correlation_id — no routing_key or reply_to
        let buf = FlatMessageBuilder::new(b"val")
            .key(b"k")
            .correlation_id(b"cid")
            .timestamp(7)
            .build();

        let msg = FlatMessage::new(&buf).unwrap();
        assert_eq!(msg.value(), b"val");
        assert_eq!(msg.key(), Some(b"k".as_ref()));
        assert_eq!(msg.routing_key(), None);
        assert_eq!(msg.reply_to(), None);
        assert_eq!(msg.correlation_id(), Some(b"cid".as_ref()));
    }

    #[test]
    fn empty_value() {
        let buf = FlatMessageBuilder::new(b"").timestamp(0).build();
        let msg = FlatMessage::new(&buf).unwrap();
        assert_eq!(msg.value(), b"");
        assert_eq!(msg.value_len(), 0);
    }

    #[test]
    fn headers_iterator() {
        let buf = FlatMessageBuilder::new(b"v")
            .header(b"a", b"1")
            .header(b"b", b"2")
            .header(b"c", b"3")
            .build();

        let msg = FlatMessage::new(&buf).unwrap();
        let hdrs: Vec<_> = msg.headers().collect();
        assert_eq!(hdrs.len(), 3);
        assert_eq!(hdrs[0].0, b"a");
        assert_eq!(hdrs[2].1, b"3");
    }

    #[test]
    fn too_short_buffer_returns_none() {
        assert!(FlatMessage::new(b"short" as &[u8]).is_none());
        assert!(FlatMessageMeta::parse(b"short").is_none());
    }

    // -- MqttEnvelope tests ---------------------------------------------------

    #[test]
    fn mqtt_envelope_roundtrip() {
        let buf = MqttEnvelopeBuilder::new(b"test/topic", b"hello")
            .timestamp(12345)
            .publisher_id(42)
            .retain(true)
            .is_v5(true)
            .ttl_ms(60000)
            .build();

        assert!(is_mqtt_envelope(&buf));
        let env = MqttEnvelope::new(&buf).unwrap();
        assert_eq!(env.topic(), b"test/topic");
        assert_eq!(env.payload(), b"hello");
        assert_eq!(env.timestamp(), 12345);
        assert_eq!(env.publisher_id(), 42);
        assert!(env.is_retain());
        assert!(env.is_v5());
        assert_eq!(env.ttl_ms(), Some(60000));
    }

    #[test]
    fn mqtt_envelope_no_ttl() {
        let buf = MqttEnvelopeBuilder::new(b"t", b"p").timestamp(1).build();

        let env = MqttEnvelope::new(&buf).unwrap();
        assert_eq!(env.ttl_ms(), None);
        assert!(!env.is_retain());
        assert!(!env.is_v5());
    }

    #[test]
    fn mqtt_envelope_with_properties() {
        // Build raw MQTT properties: response topic (0x08) + correlation data (0x09)
        let mut props = Vec::new();
        // Response topic: id=0x08, length=11 (BE), "reply/topic"
        props.push(0x08);
        props.extend_from_slice(&11u16.to_be_bytes());
        props.extend_from_slice(b"reply/topic");
        // Correlation data: id=0x09, length=8 (BE), "corr-123"
        props.push(0x09);
        props.extend_from_slice(&8u16.to_be_bytes());
        props.extend_from_slice(b"corr-123");

        let buf = MqttEnvelopeBuilder::new(b"test/topic", b"payload")
            .is_v5(true)
            .properties_raw(&props)
            .build();

        let env = MqttEnvelope::new(&buf).unwrap();
        assert_eq!(env.reply_to(), Some(b"reply/topic".as_ref()));
        assert_eq!(env.correlation_id(), Some(b"corr-123".as_ref()));
    }

    #[test]
    fn mqtt_envelope_value_len() {
        let buf = MqttEnvelopeBuilder::new(b"t", b"12345")
            .timestamp(0)
            .build();

        assert_eq!(MqttEnvelopeMeta::value_len(&buf), Some(5));
    }

    #[test]
    fn flat_message_not_mqtt_envelope() {
        let buf = FlatMessageBuilder::new(b"x").timestamp(0).build();

        assert!(!is_mqtt_envelope(&buf));
    }

    #[test]
    fn mqtt_envelope_not_flat_message() {
        let buf = MqttEnvelopeBuilder::new(b"t", b"p").timestamp(0).build();

        // MqttEnvelope has different layout (topic_len in span_count position),
        // so FlatMessage parse will typically fail (buffer too short for phantom spans).
        // The discriminator is the reliable way to distinguish formats.
        assert!(is_mqtt_envelope(&buf));
        // FlatMessage::new may or may not succeed depending on buffer sizes — the
        // important thing is that is_mqtt_envelope() correctly identifies the format.
    }

    #[test]
    fn skip_mqtt_property_byte() {
        // Property 0x01 (Payload Format Indicator) = 1 byte
        assert_eq!(skip_mqtt_property(&[0x01], 0, 0x01), Some(1));
    }

    #[test]
    fn skip_mqtt_property_four_byte() {
        // Property 0x02 (Message Expiry Interval) = 4 bytes
        let data = [0x00, 0x00, 0x00, 0x3C]; // 60 seconds
        assert_eq!(skip_mqtt_property(&data, 0, 0x02), Some(4));
    }

    #[test]
    fn skip_mqtt_property_utf8_string() {
        // Property 0x08 (Response Topic) = 2-byte length + string
        let mut data = Vec::new();
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(b"hello");
        assert_eq!(skip_mqtt_property(&data, 0, 0x08), Some(7));
    }

    // -- write_flat_message tests ---------------------------------------------

    #[test]
    fn write_flat_message_roundtrip() {
        let mut buf = BytesMut::new();
        let start = write_flat_message(
            &mut buf,
            b"hello world",
            Some(b"my-key"),
            Some(b"orders.us"),
            Some(b"reply-topic"),
            Some(b"corr-123"),
            &[],
            42,
            5000,
            2000,
            99,
            3,
            true,
        );
        assert_eq!(start, 0);

        let msg = FlatMessage::new(&buf[start..]).unwrap();
        assert_eq!(msg.value(), b"hello world");
        assert_eq!(msg.key(), Some(b"my-key".as_ref()));
        assert_eq!(msg.routing_key(), Some(b"orders.us".as_ref()));
        assert_eq!(msg.reply_to(), Some(b"reply-topic".as_ref()));
        assert_eq!(msg.correlation_id(), Some(b"corr-123".as_ref()));
        assert_eq!(msg.timestamp(), 42);
        assert_eq!(msg.ttl_ms(), Some(5000));
        assert_eq!(msg.delay_ms(), Some(2000));
        assert_eq!(msg.publisher_id(), 99);
        assert_eq!(msg.meta().priority, 3);
        assert!(msg.is_retain());
        assert_eq!(msg.header_count(), 0);
    }

    #[test]
    fn write_flat_message_with_headers_roundtrip() {
        let mut buf = BytesMut::new();
        let headers: &[(&[u8], &[u8])] = &[
            (b"content-type", b"application/json"),
            (b"x-trace-id", b"abc-def"),
        ];
        let start = write_flat_message(
            &mut buf,
            b"payload",
            Some(b"msg-key"),
            None,
            None,
            None,
            headers,
            1000,
            0,
            0,
            0,
            0,
            false,
        );

        let msg = FlatMessage::new(&buf[start..]).unwrap();
        assert_eq!(msg.value(), b"payload");
        assert_eq!(msg.key(), Some(b"msg-key".as_ref()));
        assert_eq!(msg.routing_key(), None);
        assert_eq!(msg.reply_to(), None);
        assert_eq!(msg.correlation_id(), None);
        assert_eq!(msg.timestamp(), 1000);
        assert_eq!(msg.ttl_ms(), None);
        assert_eq!(msg.delay_ms(), None);
        assert!(!msg.is_retain());
        assert_eq!(msg.header_count(), 2);
        let (k0, v0) = msg.header(0);
        assert_eq!(k0, b"content-type");
        assert_eq!(v0, b"application/json");
        let (k1, v1) = msg.header(1);
        assert_eq!(k1, b"x-trace-id");
        assert_eq!(v1, b"abc-def");
    }

    #[test]
    fn write_flat_message_matches_builder_byte_for_byte() {
        let builder_bytes = FlatMessageBuilder::new(b"payload")
            .key(b"msg-key")
            .timestamp(1000)
            .ttl_ms(5000)
            .delay_ms(2000)
            .routing_key(b"orders.us")
            .reply_to(b"reply-topic")
            .correlation_id(b"corr-123")
            .publisher_id(77)
            .priority(5)
            .retain(true)
            .header(b"content-type", b"application/json")
            .header(b"x-trace-id", b"abc-def")
            .build();

        let mut buf = BytesMut::new();
        let headers: &[(&[u8], &[u8])] = &[
            (b"content-type", b"application/json"),
            (b"x-trace-id", b"abc-def"),
        ];
        write_flat_message(
            &mut buf,
            b"payload",
            Some(b"msg-key"),
            Some(b"orders.us"),
            Some(b"reply-topic"),
            Some(b"corr-123"),
            headers,
            1000,
            5000,
            2000,
            77,
            5,
            true,
        );

        assert_eq!(buf.as_ref(), builder_bytes.as_ref());
    }

    // -- MqttEnvelopeWriter tests ---------------------------------------------

    #[test]
    fn mqtt_envelope_writer_roundtrip() {
        let mut buf = BytesMut::new();
        let start = MqttEnvelopeWriter::new(&mut buf, b"test/topic", b"hello")
            .timestamp(12345)
            .publisher_id(42)
            .retain(true)
            .is_v5(true)
            .ttl_ms(60000)
            .finish();

        assert!(is_mqtt_envelope(&buf[start..]));
        let env = MqttEnvelope::new(&buf[start..]).unwrap();
        assert_eq!(env.topic(), b"test/topic");
        assert_eq!(env.payload(), b"hello");
        assert_eq!(env.timestamp(), 12345);
        assert_eq!(env.publisher_id(), 42);
        assert!(env.is_retain());
        assert!(env.is_v5());
        assert_eq!(env.ttl_ms(), Some(60000));
    }

    #[test]
    fn mqtt_envelope_writer_with_properties_roundtrip() {
        // Build raw MQTT properties: response topic (0x08) + correlation data (0x09)
        let mut props = Vec::new();
        props.push(0x08);
        props.extend_from_slice(&11u16.to_be_bytes());
        props.extend_from_slice(b"reply/topic");
        props.push(0x09);
        props.extend_from_slice(&8u16.to_be_bytes());
        props.extend_from_slice(b"corr-123");

        let mut buf = BytesMut::new();
        let start = MqttEnvelopeWriter::new(&mut buf, b"test/topic", b"payload")
            .is_v5(true)
            .properties_raw(&props)
            .finish();

        let env = MqttEnvelope::new(&buf[start..]).unwrap();
        assert_eq!(env.topic(), b"test/topic");
        assert_eq!(env.payload(), b"payload");
        assert_eq!(env.reply_to(), Some(b"reply/topic".as_ref()));
        assert_eq!(env.correlation_id(), Some(b"corr-123".as_ref()));
    }
}
