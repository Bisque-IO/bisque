//! Flat binary codec for MqCommand.
//!
//! Hand-rolled binary encoding replaces serde/bincode for zero-copy reads
//! from mmap-backed raft log segments. Each type is encoded as:
//!
//! ```text
//! [tag: u8][fields...]
//! ```
//!
//! Variable-length fields use length-prefixed encoding:
//! - `String`:       [len: u32 LE][utf8 bytes]
//! - `Bytes`:        [len: u32 LE][raw bytes]
//! - `Vec<T>`:       [count: u32 LE][T₀][T₁]...
//! - `Option<T>`:    [0u8] if None, [1u8][T] if Some

use std::io::{Read, Write};

use bisque_raft::codec::{BorrowPayload, CodecError, Decode, Encode};
use bytes::{BufMut, Bytes, BytesMut};
use smallvec::SmallVec;

use crate::types::*;

// =============================================================================
// MqCommand tag aliases (delegate to MqCommand::TAG_*)
// =============================================================================

// =============================================================================
// Bytes encoding helpers
// =============================================================================

#[inline]
fn encode_bytes<W: Write>(w: &mut W, b: &[u8]) -> Result<(), CodecError> {
    (b.len() as u32).encode(w)?;
    w.write_all(b)?;
    Ok(())
}

#[inline]
fn decode_bytes_owned<R: Read>(r: &mut R) -> Result<Bytes, CodecError> {
    let len = u32::decode(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}

#[inline]
fn decode_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>, CodecError> {
    let len = u32::decode(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[inline]
fn encode_vec_u64<W: Write>(w: &mut W, v: &[u64]) -> Result<(), CodecError> {
    (v.len() as u32).encode(w)?;
    for &x in v {
        x.encode(w)?;
    }
    Ok(())
}

#[inline]
fn decode_vec_u64<R: Read>(r: &mut R) -> Result<SmallVec<[u64; 8]>, CodecError> {
    let count = u32::decode(r)? as usize;
    let mut v = SmallVec::with_capacity(count.min(4096));
    for _ in 0..count {
        v.push(u64::decode(r)?);
    }
    Ok(v)
}

/// Result of [`decode_u64s_at`]: either a zero-copy borrowed
/// `&[u64]` (aligned case) or an owned `SmallVec` (unaligned / big-endian).
pub enum DecodeU64s<'a> {
    Borrowed(&'a [u64]),
    Owned(SmallVec<[u64; 8]>),
}

impl std::ops::Deref for DecodeU64s<'_> {
    type Target = [u64];
    #[inline]
    fn deref(&self) -> &[u64] {
        match self {
            DecodeU64s::Borrowed(s) => s,
            DecodeU64s::Owned(v) => v,
        }
    }
}

/// Decode `count` u64s from `buf` starting at `data_offset`.
/// Zero-copy when the pointer is 8-byte aligned on LE targets.
#[inline]
pub fn decode_u64s_at(buf: &[u8], data_offset: usize, count: usize) -> DecodeU64s<'_> {
    let capped = count.min(4096);
    let end = data_offset + capped * 8;
    if end > buf.len() {
        return DecodeU64s::Owned(SmallVec::new());
    }
    #[cfg(target_endian = "little")]
    {
        let data = &buf[data_offset..end];
        let ptr = data.as_ptr();
        if ptr.align_offset(std::mem::align_of::<u64>()) == 0 {
            let slice = unsafe { std::slice::from_raw_parts(ptr as *const u64, capped) };
            return DecodeU64s::Borrowed(slice);
        }
        let mut v: SmallVec<[u64; 8]> = SmallVec::with_capacity(capped);
        unsafe {
            std::ptr::copy_nonoverlapping(ptr, v.as_mut_ptr() as *mut u8, capped * 8);
            v.set_len(capped);
        }
        DecodeU64s::Owned(v)
    }
    #[cfg(target_endian = "big")]
    {
        let mut v: SmallVec<[u64; 8]> = SmallVec::with_capacity(capped);
        for i in 0..capped {
            let off = data_offset + i * 8;
            v.push(u64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
        }
        DecodeU64s::Owned(v)
    }
}

// =============================================================================
// BytesMut write helpers — fixed/flex binary command construction
// =============================================================================

/// Initialize a command's fixed region in `buf`. Returns `base` (start offset).
/// Header layout: `[size:4][fixed_size:2][tag:1][flags:1]`
/// Size is patched in [`write_cmd_finish`].
#[inline]
fn write_cmd_begin(buf: &mut BytesMut, tag: u8, flags: u8, fixed_size: u16) -> usize {
    debug_assert!(fixed_size >= 8 && fixed_size % 8 == 0);
    let base = buf.len();
    buf.put_bytes(0, fixed_size as usize);
    buf[base + 4..base + 6].copy_from_slice(&fixed_size.to_le_bytes());
    buf[base + 6] = tag;
    buf[base + 7] = flags;
    base
}

/// Patch the size header (first 4 bytes of command at `base`) with total command length.
#[inline]
fn write_cmd_finish(buf: &mut BytesMut, base: usize) {
    let total = (buf.len() - base) as u32;
    buf[base..base + 4].copy_from_slice(&total.to_le_bytes());
}

/// Write a u64 at fixed slot `slot` (relative to `base`).
#[inline]
fn write_u64_field(buf: &mut BytesMut, base: usize, slot: usize, value: u64) {
    buf[base + slot..base + slot + 8].copy_from_slice(&value.to_le_bytes());
}

/// Write a u32 at fixed slot `slot` (relative to `base`).
#[inline]
fn write_u32_field(buf: &mut BytesMut, base: usize, slot: usize, value: u32) {
    buf[base + slot..base + slot + 4].copy_from_slice(&value.to_le_bytes());
}

/// Write an i32 at fixed slot `slot` (relative to `base`).
#[inline]
fn write_i32_field(buf: &mut BytesMut, base: usize, slot: usize, value: i32) {
    buf[base + slot..base + slot + 4].copy_from_slice(&value.to_le_bytes());
}

/// Write a u8 at fixed slot `slot` (relative to `base`).
#[inline]
fn write_u8_field(buf: &mut BytesMut, base: usize, slot: usize, value: u8) {
    buf[base + slot] = value;
}

/// Write a flex8 value at fixed slot `slot` (relative to `base`).
///
/// Small (≤7 bytes): inlined as `[(len << 1):u8][data:≤7]`.
/// Large (>8 bytes): `[(rel_offset << 1 | 1):u32][size:u32]` pointing into flex region.
/// `rel_offset` is the byte offset of the data from `base` (within the command).
fn write_flex8_field(buf: &mut BytesMut, base: usize, slot: usize, data: &[u8]) {
    if data.len() <= 7 {
        buf[base + slot] = (data.len() as u8) << 1;
        buf[base + slot + 1..base + slot + 1 + data.len()].copy_from_slice(data);
    } else {
        let rel_offset = (buf.len() - base) as u32;
        buf.extend_from_slice(data);
        let encoded = (rel_offset << 1) | 1;
        buf[base + slot..base + slot + 4].copy_from_slice(&encoded.to_le_bytes());
        buf[base + slot + 4..base + slot + 8].copy_from_slice(&(data.len() as u32).to_le_bytes());
    }
}

#[inline]
fn write_flex8_str_field(buf: &mut BytesMut, base: usize, slot: usize, s: &str) {
    write_flex8_field(buf, base, slot, s.as_bytes());
}

#[inline]
fn write_opt_flex8_str_field(buf: &mut BytesMut, base: usize, slot: usize, s: Option<&str>) {
    if let Some(s) = s {
        write_flex8_field(buf, base, slot, s.as_bytes());
    }
    // else: slot stays zeroed → length 0 → None
}

#[inline]
fn write_opt_flex8_bytes_field(buf: &mut BytesMut, base: usize, slot: usize, b: Option<&Bytes>) {
    if let Some(b) = b {
        write_flex8_field(buf, base, slot, b);
    }
}

/// Write a vec_u64 field at `slot`.
/// Fixed: `[count:4][data_offset:4]`. Data is 8-byte aligned in flex region.
fn write_vec_u64_field(buf: &mut BytesMut, base: usize, slot: usize, values: &[u64]) {
    buf[base + slot..base + slot + 4].copy_from_slice(&(values.len() as u32).to_le_bytes());
    if values.is_empty() {
        return;
    }
    let pad = (8 - (buf.len() % 8)) % 8;
    if pad > 0 {
        buf.put_bytes(0, pad);
    }
    let rel_offset = (buf.len() - base) as u32;
    buf[base + slot + 4..base + slot + 8].copy_from_slice(&rel_offset.to_le_bytes());
    for &v in values {
        buf.put_u64_le(v);
    }
}

/// Write a vec_bytes field at `slot` using sequential length-prefixed format.
/// Fixed: `[count:4][data_start:4]`.
/// Flex: `[len0:4][data0][len1:4][data1]...` — stream-oriented, no descriptor table.
fn write_vec_bytes_field(buf: &mut BytesMut, base: usize, slot: usize, values: &[&[u8]]) {
    buf[base + slot..base + slot + 4].copy_from_slice(&(values.len() as u32).to_le_bytes());
    if values.is_empty() {
        return;
    }
    let data_start = (buf.len() - base) as u32;
    buf[base + slot + 4..base + slot + 8].copy_from_slice(&data_start.to_le_bytes());
    for v in values {
        buf.put_u32_le(v.len() as u32);
        buf.extend_from_slice(v);
    }
}

/// Same as [`write_vec_bytes_field`] but takes `&[Bytes]`.
fn write_vec_bytes_owned_field(buf: &mut BytesMut, base: usize, slot: usize, values: &[Bytes]) {
    buf[base + slot..base + slot + 4].copy_from_slice(&(values.len() as u32).to_le_bytes());
    if values.is_empty() {
        return;
    }
    let data_start = (buf.len() - base) as u32;
    buf[base + slot + 4..base + slot + 8].copy_from_slice(&data_start.to_le_bytes());
    for v in values {
        buf.put_u32_le(v.len() as u32);
        buf.extend_from_slice(v);
    }
}

/// Write a blob field at `slot`: encode `value` using the [`Encode`] trait.
/// Fixed: `[data_offset:4][data_size:4]`. Data appended to flex region.
fn write_blob_field<T: Encode>(buf: &mut BytesMut, base: usize, slot: usize, value: &T) {
    let data_start = (buf.len() - base) as u32;
    let mut tmp = Vec::with_capacity(value.encoded_size());
    value.encode(&mut tmp).unwrap();
    let data_size = tmp.len() as u32;
    buf.extend_from_slice(&tmp);
    buf[base + slot..base + slot + 4].copy_from_slice(&data_start.to_le_bytes());
    buf[base + slot + 4..base + slot + 8].copy_from_slice(&data_size.to_le_bytes());
}

/// Write a vec_kv field at `slot`: key-value pairs with flex8 descriptors.
/// Fixed: `[count:4][table_offset:4]`. Table: 16 bytes per entry `[key:flex8][val:flex8]`.
fn write_vec_kv_field(buf: &mut BytesMut, base: usize, slot: usize, pairs: &[(&str, &[u8])]) {
    buf[base + slot..base + slot + 4].copy_from_slice(&(pairs.len() as u32).to_le_bytes());
    if pairs.is_empty() {
        return;
    }
    let table_rel = (buf.len() - base) as u32;
    buf[base + slot + 4..base + slot + 8].copy_from_slice(&table_rel.to_le_bytes());
    let table_size = pairs.len() * 16;
    let table_abs = buf.len();
    buf.put_bytes(0, table_size);
    for (i, (key, val)) in pairs.iter().enumerate() {
        let key_slot = table_abs + i * 16 - base;
        let val_slot = key_slot + 8;
        write_flex8_field(buf, base, key_slot, key.as_bytes());
        write_flex8_field(buf, base, val_slot, val);
    }
}

#[inline]
fn encode_opt_string<W: Write>(w: &mut W, v: &Option<String>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(s) => {
            1u8.encode(w)?;
            s.encode(w)
        }
    }
}

#[inline]
fn decode_opt_string<R: Read>(r: &mut R) -> Result<Option<String>, CodecError> {
    if u8::decode(r)? == 0 {
        Ok(None)
    } else {
        Ok(Some(String::decode(r)?))
    }
}

#[inline]
fn encode_opt_u64<W: Write>(w: &mut W, v: Option<u64>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(val) => {
            1u8.encode(w)?;
            val.encode(w)
        }
    }
}

#[inline]
fn decode_opt_u64<R: Read>(r: &mut R) -> Result<Option<u64>, CodecError> {
    if u8::decode(r)? == 0 {
        Ok(None)
    } else {
        Ok(Some(u64::decode(r)?))
    }
}

// =============================================================================
// Sub-type codecs
// =============================================================================

// -- EntityType --

impl Encode for EntityType {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            EntityType::Topic => 0,
            EntityType::Exchange => 1,
            EntityType::ConsumerGroup => 2,
            EntityType::Session => 3,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for EntityType {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(EntityType::Topic),
            1 => Ok(EntityType::Exchange),
            2 => Ok(EntityType::ConsumerGroup),
            3 => Ok(EntityType::Session),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- ExchangeType --

impl Encode for ExchangeType {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            ExchangeType::Fanout => 0,
            ExchangeType::Direct => 1,
            ExchangeType::Topic => 2,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for ExchangeType {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(ExchangeType::Fanout),
            1 => Ok(ExchangeType::Direct),
            2 => Ok(ExchangeType::Topic),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- RetentionPolicy --

impl Encode for RetentionPolicy {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.max_age_secs.encode(w)?;
        self.max_bytes.encode(w)?;
        self.max_messages.encode(w)
    }
    fn encoded_size(&self) -> usize {
        self.max_age_secs.encoded_size()
            + self.max_bytes.encoded_size()
            + self.max_messages.encoded_size()
    }
}

impl Decode for RetentionPolicy {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            max_age_secs: Option::decode(r)?,
            max_bytes: Option::decode(r)?,
            max_messages: Option::decode(r)?,
        })
    }
}

// -- EntityKind --

impl Encode for EntityKind {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            EntityKind::Topic => 0,
            EntityKind::Exchange => 1,
            EntityKind::Binding => 2,
            EntityKind::ConsumerGroup => 3,
            EntityKind::Session => 4,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for EntityKind {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(EntityKind::Topic),
            1 => Ok(EntityKind::Exchange),
            2 => Ok(EntityKind::Binding),
            3 => Ok(EntityKind::ConsumerGroup),
            4 => Ok(EntityKind::Session),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- MqError --

impl Encode for MqError {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            MqError::NotFound { entity, id } => {
                0u8.encode(w)?;
                entity.encode(w)?;
                id.encode(w)
            }
            MqError::AlreadyExists { entity, id } => {
                1u8.encode(w)?;
                entity.encode(w)?;
                id.encode(w)
            }
            MqError::MailboxFull { pending } => {
                2u8.encode(w)?;
                pending.encode(w)
            }
            MqError::IllegalGeneration => 4u8.encode(w),
            MqError::RebalanceInProgress => 5u8.encode(w),
            MqError::UnknownMemberId => 6u8.encode(w),
            MqError::Custom(msg) => {
                3u8.encode(w)?;
                msg.encode(w)
            }
            MqError::BackPressure { group_id } => {
                7u8.encode(w)?;
                group_id.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            MqError::NotFound { .. } | MqError::AlreadyExists { .. } => 1 + 8,
            MqError::MailboxFull { .. } => 4,
            MqError::IllegalGeneration
            | MqError::RebalanceInProgress
            | MqError::UnknownMemberId => 0,
            MqError::Custom(msg) => msg.encoded_size(),
            MqError::BackPressure { .. } => 8,
        }
    }
}

impl Decode for MqError {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(MqError::NotFound {
                entity: EntityKind::decode(r)?,
                id: u64::decode(r)?,
            }),
            1 => Ok(MqError::AlreadyExists {
                entity: EntityKind::decode(r)?,
                id: u64::decode(r)?,
            }),
            2 => Ok(MqError::MailboxFull {
                pending: u32::decode(r)?,
            }),
            3 => Ok(MqError::Custom(String::decode(r)?)),
            4 => Ok(MqError::IllegalGeneration),
            5 => Ok(MqError::RebalanceInProgress),
            6 => Ok(MqError::UnknownMemberId),
            7 => Ok(MqError::BackPressure {
                group_id: u64::decode(r)?,
            }),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- DeliveredMessage --

impl Encode for DeliveredMessage {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.message_id.encode(w)?;
        self.attempt.encode(w)?;
        self.original_timestamp.encode(w)?;
        self.group_id.encode(w)
    }
    fn encoded_size(&self) -> usize {
        8 + 4 + 8 + 8
    }
}

impl Decode for DeliveredMessage {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            message_id: u64::decode(r)?,
            attempt: u32::decode(r)?,
            original_timestamp: u64::decode(r)?,
            group_id: u64::decode(r)?,
        })
    }
}

// -- GroupVariant --

impl Encode for GroupVariant {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        (*self as u8).encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for GroupVariant {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(GroupVariant::Offset),
            1 => Ok(GroupVariant::Ack),
            2 => Ok(GroupVariant::Actor),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- TopicLifetimePolicy --

impl Encode for TopicLifetimePolicy {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            TopicLifetimePolicy::Permanent => 0,
            TopicLifetimePolicy::DeleteOnLastDetach => 1,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for TopicLifetimePolicy {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(TopicLifetimePolicy::Permanent),
            1 => Ok(TopicLifetimePolicy::DeleteOnLastDetach),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- TopicDedupConfig --

impl Encode for TopicDedupConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.window_secs.encode(w)?;
        self.max_entries.encode(w)
    }
    fn encoded_size(&self) -> usize {
        16
    }
}

impl Decode for TopicDedupConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let window_secs = u64::decode(r)?;
        // Backwards compat: old wire format only had window_secs (8 bytes).
        let max_entries = u64::decode(r).unwrap_or(100_000);
        Ok(Self {
            window_secs,
            max_entries,
        })
    }
}

// -- AckVariantConfig --

impl Encode for AckVariantConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.visibility_timeout_ms.encode(w)?;
        self.max_retries.encode(w)?;
        encode_opt_string(w, &self.dead_letter_topic)?;
        self.delay_default_ms.encode(w)?;
        self.max_in_flight_per_consumer.encode(w)?;
        encode_opt_u64(w, self.max_pending_messages)?;
        encode_opt_u64(w, self.max_pending_bytes)?;
        encode_opt_u64(w, self.max_delayed_messages)?;
        encode_opt_u64(w, self.max_delayed_bytes)
    }
    fn encoded_size(&self) -> usize {
        8 + 4
            + 1
            + self.dead_letter_topic.as_ref().map_or(0, |s| 4 + s.len())
            + 8
            + 4
            + 1
            + self.max_pending_messages.map_or(0, |_| 8)
            + 1
            + self.max_pending_bytes.map_or(0, |_| 8)
            + 1
            + self.max_delayed_messages.map_or(0, |_| 8)
            + 1
            + self.max_delayed_bytes.map_or(0, |_| 8)
    }
}

impl Decode for AckVariantConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            visibility_timeout_ms: u64::decode(r)?,
            max_retries: u32::decode(r)?,
            dead_letter_topic: decode_opt_string(r)?,
            delay_default_ms: u64::decode(r)?,
            max_in_flight_per_consumer: u32::decode(r)?,
            max_pending_messages: decode_opt_u64(r)?,
            max_pending_bytes: decode_opt_u64(r)?,
            max_delayed_messages: decode_opt_u64(r)?,
            max_delayed_bytes: decode_opt_u64(r)?,
        })
    }
}

// -- ActorVariantConfig --

impl Encode for ActorVariantConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.max_mailbox_depth.encode(w)?;
        self.idle_eviction_secs.encode(w)?;
        self.ack_timeout_ms.encode(w)?;
        self.max_retries.encode(w)?;
        encode_opt_u64(w, self.max_pending_messages)?;
        encode_opt_u64(w, self.max_pending_bytes)
    }
    fn encoded_size(&self) -> usize {
        4 + 8
            + 8
            + 4
            + 1
            + self.max_pending_messages.map_or(0, |_| 8)
            + 1
            + self.max_pending_bytes.map_or(0, |_| 8)
    }
}

impl Decode for ActorVariantConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            max_mailbox_depth: u32::decode(r)?,
            idle_eviction_secs: u64::decode(r)?,
            ack_timeout_ms: u64::decode(r)?,
            max_retries: u32::decode(r)?,
            max_pending_messages: decode_opt_u64(r)?,
            max_pending_bytes: decode_opt_u64(r)?,
        })
    }
}

// -- VariantConfig --

impl Encode for VariantConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            VariantConfig::Offset => 0u8.encode(w),
            VariantConfig::Ack(c) => {
                1u8.encode(w)?;
                c.encode(w)
            }
            VariantConfig::Actor(c) => {
                2u8.encode(w)?;
                c.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            VariantConfig::Offset => 0,
            VariantConfig::Ack(c) => c.encoded_size(),
            VariantConfig::Actor(c) => c.encoded_size(),
        }
    }
}

impl Decode for VariantConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(VariantConfig::Offset),
            1 => Ok(VariantConfig::Ack(AckVariantConfig::decode(r)?)),
            2 => Ok(VariantConfig::Actor(ActorVariantConfig::decode(r)?)),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- EntityStats --

impl Encode for EntityStats {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            EntityStats::Topic {
                topic_id,
                message_count,
                head_index,
                tail_index,
            } => {
                0u8.encode(w)?;
                topic_id.encode(w)?;
                message_count.encode(w)?;
                head_index.encode(w)?;
                tail_index.encode(w)
            }
            EntityStats::ConsumerGroup {
                group_id,
                variant,
                pending_count,
                in_flight_count,
                dlq_count,
                active_actor_count,
            } => {
                1u8.encode(w)?;
                group_id.encode(w)?;
                variant.encode(w)?;
                pending_count.encode(w)?;
                in_flight_count.encode(w)?;
                dlq_count.encode(w)?;
                active_actor_count.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            EntityStats::Topic { .. } => 4 * 8,
            EntityStats::ConsumerGroup { .. } => 8 + 1 + 4 * 8,
        }
    }
}

impl Decode for EntityStats {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(EntityStats::Topic {
                topic_id: u64::decode(r)?,
                message_count: u64::decode(r)?,
                head_index: u64::decode(r)?,
                tail_index: u64::decode(r)?,
            }),
            1 => Ok(EntityStats::ConsumerGroup {
                group_id: u64::decode(r)?,
                variant: GroupVariant::decode(r)?,
                pending_count: u64::decode(r)?,
                in_flight_count: u64::decode(r)?,
                dlq_count: u64::decode(r)?,
                active_actor_count: u64::decode(r)?,
            }),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// =============================================================================
// MqCommand — Encode (passthrough, buffer already contains encoded bytes)
// =============================================================================

impl Encode for MqCommand {
    #[inline]
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        w.write_all(&self.buf)?;
        Ok(())
    }

    #[inline]
    fn encoded_size(&self) -> usize {
        self.buf.len()
    }
}

// =============================================================================
// MqCommand — Decode (zero-copy wrap)
// =============================================================================

impl Decode for MqCommand {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(MqCommand {
            buf: Bytes::from(buf),
        })
    }

    fn decode_from_bytes(data: Bytes) -> Result<Self, CodecError> {
        Ok(MqCommand { buf: data })
    }
}

impl BorrowPayload for MqCommand {
    fn payload_bytes(&self) -> &[u8] {
        &self.buf
    }

    fn extra_payload_segments(&self) -> &[bytes::Bytes] {
        &[]
    }
}

// =============================================================================
// MqApplyResponse — Encode/Decode/BorrowPayload
// =============================================================================

impl Encode for crate::types::MqApplyResponse {
    #[inline]
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        w.write_all(&self.log_index.to_le_bytes())?;
        Ok(())
    }

    #[inline]
    fn encoded_size(&self) -> usize {
        8
    }
}

impl Decode for crate::types::MqApplyResponse {
    fn decode<R: Read>(_reader: &mut R) -> Result<Self, CodecError> {
        Err(CodecError::InvalidDiscriminant(0))
    }

    fn decode_from_bytes(bytes: Bytes) -> Result<Self, CodecError> {
        if bytes.len() < 8 {
            return Err(CodecError::InvalidDiscriminant(0));
        }
        let log_index = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        Ok(Self { log_index })
    }
}

impl BorrowPayload for crate::types::MqApplyResponse {
    fn payload_bytes(&self) -> &[u8] {
        &[]
    }

    fn extra_payload_segments(&self) -> &[bytes::Bytes] {
        &[]
    }
}

// =============================================================================
// MqCommand — write_* methods (write into caller-provided BytesMut slab)
// =============================================================================

impl MqCommand {
    // -- Topics (0-5) --

    pub fn write_create_topic(
        buf: &mut BytesMut,
        name: &str,
        retention: &RetentionPolicy,
        partition_count: u32,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_CREATE_TOPIC, 0, 32);
        write_flex8_str_field(buf, base, 8, name);
        write_blob_field(buf, base, 16, retention);
        write_u32_field(buf, base, 24, partition_count);
        write_cmd_finish(buf, base);
    }

    pub fn write_delete_topic(buf: &mut BytesMut, topic_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_DELETE_TOPIC, 0, 16);
        write_u64_field(buf, base, 8, topic_id);
        write_cmd_finish(buf, base);
    }

    /// @8 topic_id:8, @16 messages:vec_bytes (sequential format)
    pub fn write_publish(buf: &mut BytesMut, topic_id: u64, messages: &[&[u8]]) {
        let base = write_cmd_begin(buf, Self::TAG_PUBLISH, 0, 24);
        write_u64_field(buf, base, 8, topic_id);
        write_vec_bytes_field(buf, base, 16, messages);
        write_cmd_finish(buf, base);
    }

    pub fn write_publish_bytes(buf: &mut BytesMut, topic_id: u64, messages: &[Bytes]) {
        let base = write_cmd_begin(buf, Self::TAG_PUBLISH, 0, 24);
        write_u64_field(buf, base, 8, topic_id);
        write_vec_bytes_owned_field(buf, base, 16, messages);
        write_cmd_finish(buf, base);
    }

    pub fn write_commit_offset(buf: &mut BytesMut, topic_id: u64, consumer_id: u64, offset: u64) {
        let base = write_cmd_begin(buf, Self::TAG_COMMIT_OFFSET, 0, 32);
        write_u64_field(buf, base, 8, topic_id);
        write_u64_field(buf, base, 16, consumer_id);
        write_u64_field(buf, base, 24, offset);
        write_cmd_finish(buf, base);
    }

    pub fn write_purge_topic(buf: &mut BytesMut, topic_id: u64, before_index: u64) {
        let base = write_cmd_begin(buf, Self::TAG_PURGE_TOPIC, 0, 24);
        write_u64_field(buf, base, 8, topic_id);
        write_u64_field(buf, base, 16, before_index);
        write_cmd_finish(buf, base);
    }

    pub fn write_set_retained(
        buf: &mut BytesMut,
        exchange_id: u64,
        routing_key: &str,
        message: &[u8],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_SET_RETAINED, 0, 32);
        write_u64_field(buf, base, 8, exchange_id);
        write_flex8_str_field(buf, base, 16, routing_key);
        write_flex8_field(buf, base, 24, message);
        write_cmd_finish(buf, base);
    }

    pub fn write_get_retained(
        buf: &mut BytesMut,
        exchange_id: u64,
        routing_key_filter: Option<&str>,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GET_RETAINED, 0, 24);
        write_u64_field(buf, base, 8, exchange_id);
        write_opt_flex8_str_field(buf, base, 16, routing_key_filter);
        write_cmd_finish(buf, base);
    }

    pub fn write_delete_retained(buf: &mut BytesMut, exchange_id: u64, routing_key: &str) {
        let base = write_cmd_begin(buf, Self::TAG_DELETE_RETAINED, 0, 24);
        write_u64_field(buf, base, 8, exchange_id);
        write_flex8_str_field(buf, base, 16, routing_key);
        write_cmd_finish(buf, base);
    }

    // -- Exchanges (6-10) --

    pub fn write_create_exchange(buf: &mut BytesMut, name: &str, exchange_type: ExchangeType) {
        let et_byte = match exchange_type {
            ExchangeType::Direct => 0,
            ExchangeType::Fanout => 1,
            ExchangeType::Topic => 2,
        };
        let base = write_cmd_begin(buf, Self::TAG_CREATE_EXCHANGE, et_byte, 16);
        write_flex8_str_field(buf, base, 8, name);
        write_cmd_finish(buf, base);
    }

    pub fn write_delete_exchange(buf: &mut BytesMut, exchange_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_DELETE_EXCHANGE, 0, 16);
        write_u64_field(buf, base, 8, exchange_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_create_binding(
        buf: &mut BytesMut,
        exchange_id: u64,
        topic_id: u64,
        routing_key: Option<&str>,
    ) {
        Self::write_create_binding_with_opts(
            buf,
            exchange_id,
            topic_id,
            routing_key,
            false,
            None,
            None,
        );
    }

    pub fn write_create_binding_with_opts(
        buf: &mut BytesMut,
        exchange_id: u64,
        topic_id: u64,
        routing_key: Option<&str>,
        no_local: bool,
        shared_group: Option<&str>,
        subscription_id: Option<u32>,
    ) {
        let flags = if no_local { 1u8 } else { 0u8 };
        let base = write_cmd_begin(buf, Self::TAG_CREATE_BINDING, flags, 48);
        write_u64_field(buf, base, 8, exchange_id);
        write_u64_field(buf, base, 16, topic_id);
        write_opt_flex8_str_field(buf, base, 24, routing_key);
        write_opt_flex8_str_field(buf, base, 32, shared_group);
        if let Some(sid) = subscription_id {
            write_u32_field(buf, base, 40, sid);
            write_u8_field(buf, base, 44, 1);
        }
        write_cmd_finish(buf, base);
    }

    pub fn write_delete_binding(buf: &mut BytesMut, binding_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_DELETE_BINDING, 0, 16);
        write_u64_field(buf, base, 8, binding_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_publish_to_exchange(buf: &mut BytesMut, exchange_id: u64, messages: &[&[u8]]) {
        let base = write_cmd_begin(buf, Self::TAG_PUBLISH_TO_EXCHANGE, 0, 24);
        write_u64_field(buf, base, 8, exchange_id);
        write_vec_bytes_field(buf, base, 16, messages);
        write_cmd_finish(buf, base);
    }

    pub fn write_publish_to_exchange_bytes(
        buf: &mut BytesMut,
        exchange_id: u64,
        messages: &[Bytes],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_PUBLISH_TO_EXCHANGE, 0, 24);
        write_u64_field(buf, base, 8, exchange_id);
        write_vec_bytes_owned_field(buf, base, 16, messages);
        write_cmd_finish(buf, base);
    }

    pub fn write_publish_to_exchange_mqtt(
        buf: &mut BytesMut,
        exchange_id: u64,
        messages: &[&[u8]],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_PUBLISH_TO_EXCHANGE_MQTT, 0, 24);
        write_u64_field(buf, base, 8, exchange_id);
        write_vec_bytes_field(buf, base, 16, messages);
        write_cmd_finish(buf, base);
    }

    pub fn write_set_retained_mqtt(
        buf: &mut BytesMut,
        exchange_id: u64,
        routing_key: &str,
        message: &[u8],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_SET_RETAINED_MQTT, 0, 32);
        write_u64_field(buf, base, 8, exchange_id);
        write_flex8_str_field(buf, base, 16, routing_key);
        write_flex8_field(buf, base, 24, message);
        write_cmd_finish(buf, base);
    }

    // -- Consumer Groups (11-18) --

    pub fn write_create_consumer_group_full(
        buf: &mut BytesMut,
        name: &str,
        auto_offset_reset: u8,
        variant_config: &VariantConfig,
        auto_create_topic: bool,
        topic_retention: &RetentionPolicy,
        topic_dedup: Option<&TopicDedupConfig>,
        topic_lifetime: TopicLifetimePolicy,
        dlq_topic_name: Option<&str>,
        response_topic_name: Option<&str>,
    ) {
        let lifetime_byte = match topic_lifetime {
            TopicLifetimePolicy::Permanent => 0u8,
            TopicLifetimePolicy::DeleteOnLastDetach => 1u8,
        };
        let base = write_cmd_begin(buf, Self::TAG_CREATE_CONSUMER_GROUP, 0, 64);
        write_flex8_str_field(buf, base, 8, name);
        write_opt_flex8_str_field(buf, base, 16, dlq_topic_name);
        write_opt_flex8_str_field(buf, base, 24, response_topic_name);
        write_blob_field(buf, base, 32, variant_config);
        write_blob_field(buf, base, 40, topic_retention);
        if let Some(dedup) = topic_dedup {
            write_blob_field(buf, base, 48, dedup);
        }
        write_u8_field(buf, base, 56, auto_offset_reset);
        write_u8_field(buf, base, 57, auto_create_topic as u8);
        write_u8_field(buf, base, 58, lifetime_byte);
        write_cmd_finish(buf, base);
    }

    pub fn write_create_consumer_group(buf: &mut BytesMut, name: &str, auto_offset_reset: u8) {
        Self::write_create_consumer_group_full(
            buf,
            name,
            auto_offset_reset,
            &VariantConfig::Offset,
            false,
            &RetentionPolicy::default(),
            None,
            TopicLifetimePolicy::Permanent,
            None,
            None,
        );
    }

    pub fn write_create_queue(
        buf: &mut BytesMut,
        name: &str,
        config: AckVariantConfig,
        retention: RetentionPolicy,
        dedup: Option<&TopicDedupConfig>,
        enable_dlq: bool,
        dlq_name: Option<&str>,
        enable_response: bool,
        response_name: Option<&str>,
    ) {
        let dlq = enable_dlq.then(|| {
            dlq_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}:dlq", name))
        });
        let resp = enable_response.then(|| {
            response_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}:resp", name))
        });
        Self::write_create_consumer_group_full(
            buf,
            name,
            1,
            &VariantConfig::Ack(config),
            true,
            &retention,
            dedup,
            TopicLifetimePolicy::Permanent,
            dlq.as_deref(),
            resp.as_deref(),
        );
    }

    pub fn write_create_actor_group(
        buf: &mut BytesMut,
        name: &str,
        config: ActorVariantConfig,
        retention: RetentionPolicy,
        enable_response: bool,
        response_name: Option<&str>,
    ) {
        let resp = enable_response.then(|| {
            response_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}:resp", name))
        });
        Self::write_create_consumer_group_full(
            buf,
            name,
            1,
            &VariantConfig::Actor(config),
            true,
            &retention,
            None,
            TopicLifetimePolicy::Permanent,
            None,
            resp.as_deref(),
        );
    }

    pub fn write_create_job_cron(
        buf: &mut BytesMut,
        name: &str,
        ack_config: AckVariantConfig,
        retention: RetentionPolicy,
    ) {
        Self::write_create_consumer_group_full(
            buf,
            name,
            1,
            &VariantConfig::Ack(ack_config),
            true,
            &retention,
            None,
            TopicLifetimePolicy::Permanent,
            None,
            None,
        );
    }

    pub fn write_delete_consumer_group(buf: &mut BytesMut, group_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_DELETE_CONSUMER_GROUP, 0, 16);
        write_u64_field(buf, base, 8, group_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_commit_group_offset(
        buf: &mut BytesMut,
        group_id: u64,
        generation: i32,
        topic_id: u64,
        partition_index: u32,
        offset: u64,
        metadata: Option<&str>,
        timestamp: u64,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_COMMIT_GROUP_OFFSET, 0, 56);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, topic_id);
        write_u64_field(buf, base, 24, offset);
        write_u64_field(buf, base, 32, timestamp);
        write_opt_flex8_str_field(buf, base, 40, metadata);
        write_i32_field(buf, base, 48, generation);
        write_u32_field(buf, base, 52, partition_index);
        write_cmd_finish(buf, base);
    }

    pub fn write_join_consumer_group(
        buf: &mut BytesMut,
        group_id: u64,
        member_id: &str,
        client_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: &str,
        protocols: &[(&str, &[u8])],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_JOIN_CONSUMER_GROUP, 0, 56);
        write_u64_field(buf, base, 8, group_id);
        write_flex8_str_field(buf, base, 16, member_id);
        write_flex8_str_field(buf, base, 24, client_id);
        write_flex8_str_field(buf, base, 32, protocol_type);
        write_i32_field(buf, base, 40, session_timeout_ms);
        write_i32_field(buf, base, 44, rebalance_timeout_ms);
        write_vec_kv_field(buf, base, 48, protocols);
        write_cmd_finish(buf, base);
    }

    pub fn write_sync_consumer_group(
        buf: &mut BytesMut,
        group_id: u64,
        generation: i32,
        member_id: &str,
        assignments: &[(&str, &[u8])],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_SYNC_CONSUMER_GROUP, 0, 40);
        write_u64_field(buf, base, 8, group_id);
        write_flex8_str_field(buf, base, 16, member_id);
        write_i32_field(buf, base, 24, generation);
        write_vec_kv_field(buf, base, 32, assignments);
        write_cmd_finish(buf, base);
    }

    pub fn write_leave_consumer_group(buf: &mut BytesMut, group_id: u64, member_id: &str) {
        let base = write_cmd_begin(buf, Self::TAG_LEAVE_CONSUMER_GROUP, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_flex8_str_field(buf, base, 16, member_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_heartbeat_consumer_group(
        buf: &mut BytesMut,
        group_id: u64,
        member_id: &str,
        generation: i32,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_HEARTBEAT_CONSUMER_GROUP, 0, 32);
        write_u64_field(buf, base, 8, group_id);
        write_flex8_str_field(buf, base, 16, member_id);
        write_i32_field(buf, base, 24, generation);
        write_cmd_finish(buf, base);
    }

    pub fn write_expire_group_sessions(buf: &mut BytesMut, now_ms: u64) {
        let base = write_cmd_begin(buf, Self::TAG_EXPIRE_GROUP_SESSIONS, 0, 16);
        write_u64_field(buf, base, 8, now_ms);
        write_cmd_finish(buf, base);
    }

    // -- Ack Variant (19-29) --

    pub fn write_group_deliver(
        buf: &mut BytesMut,
        group_id: u64,
        consumer_id: u64,
        max_count: u32,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_DELIVER, 0, 48);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, consumer_id);
        write_u32_field(buf, base, 24, max_count);
        // @32 exclude_publisher_id, @40 current_time_ms — stay zeroed (disabled)
        write_cmd_finish(buf, base);
    }

    pub fn write_group_deliver_filtered(
        buf: &mut BytesMut,
        group_id: u64,
        consumer_id: u64,
        max_count: u32,
        exclude_publisher_id: u64,
        current_time_ms: u64,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_DELIVER, 0, 48);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, consumer_id);
        write_u32_field(buf, base, 24, max_count);
        write_u64_field(buf, base, 32, exclude_publisher_id);
        write_u64_field(buf, base, 40, current_time_ms);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_ack(
        buf: &mut BytesMut,
        group_id: u64,
        message_ids: &[u64],
        response: Option<&Bytes>,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_ACK, 0, 32);
        write_u64_field(buf, base, 8, group_id);
        write_vec_u64_field(buf, base, 16, message_ids);
        write_opt_flex8_bytes_field(buf, base, 24, response);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_nack(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_NACK, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_vec_u64_field(buf, base, 16, message_ids);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_release(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_RELEASE, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_vec_u64_field(buf, base, 16, message_ids);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_modify(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_MODIFY, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_vec_u64_field(buf, base, 16, message_ids);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_extend_visibility(
        buf: &mut BytesMut,
        group_id: u64,
        message_ids: &[u64],
        extension_ms: u64,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_EXTEND_VISIBILITY, 0, 32);
        write_u64_field(buf, base, 8, group_id);
        write_vec_u64_field(buf, base, 16, message_ids);
        write_u64_field(buf, base, 24, extension_ms);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_timeout_expired(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_TIMEOUT_EXPIRED, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_vec_u64_field(buf, base, 16, message_ids);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_publish_to_dlq(
        buf: &mut BytesMut,
        source_group_id: u64,
        dlq_topic_id: u64,
        dead_letter_ids: &[u64],
        messages: &[&[u8]],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_PUBLISH_TO_DLQ, 0, 40);
        write_u64_field(buf, base, 8, source_group_id);
        write_u64_field(buf, base, 16, dlq_topic_id);
        write_vec_u64_field(buf, base, 24, dead_letter_ids);
        write_vec_bytes_field(buf, base, 32, messages);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_expire_pending(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_EXPIRE_PENDING, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_vec_u64_field(buf, base, 16, message_ids);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_purge(buf: &mut BytesMut, group_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_PURGE, 0, 16);
        write_u64_field(buf, base, 8, group_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_get_attributes(buf: &mut BytesMut, group_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_GET_ATTRIBUTES, 0, 16);
        write_u64_field(buf, base, 8, group_id);
        write_cmd_finish(buf, base);
    }

    // -- Actor Variant (30-35) --

    pub fn write_group_deliver_actor(
        buf: &mut BytesMut,
        group_id: u64,
        consumer_id: u64,
        actor_ids: &[&[u8]],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_DELIVER_ACTOR, 0, 32);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, consumer_id);
        write_vec_bytes_field(buf, base, 24, actor_ids);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_ack_actor(
        buf: &mut BytesMut,
        group_id: u64,
        actor_id: &[u8],
        message_id: u64,
        response: Option<&Bytes>,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_ACK_ACTOR, 0, 40);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, message_id);
        write_flex8_field(buf, base, 24, actor_id);
        write_opt_flex8_bytes_field(buf, base, 32, response);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_nack_actor(
        buf: &mut BytesMut,
        group_id: u64,
        actor_id: &[u8],
        message_id: u64,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_NACK_ACTOR, 0, 32);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, message_id);
        write_flex8_field(buf, base, 24, actor_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_assign_actors(
        buf: &mut BytesMut,
        group_id: u64,
        consumer_id: u64,
        actor_ids: &[&[u8]],
    ) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_ASSIGN_ACTORS, 0, 32);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, consumer_id);
        write_vec_bytes_field(buf, base, 24, actor_ids);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_release_actors(buf: &mut BytesMut, group_id: u64, consumer_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_RELEASE_ACTORS, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, consumer_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_group_evict_idle(buf: &mut BytesMut, group_id: u64, before_timestamp: u64) {
        let base = write_cmd_begin(buf, Self::TAG_GROUP_EVICT_IDLE, 0, 24);
        write_u64_field(buf, base, 8, group_id);
        write_u64_field(buf, base, 16, before_timestamp);
        write_cmd_finish(buf, base);
    }

    // -- Cron (36-39) --

    pub fn write_cron_enable(buf: &mut BytesMut, topic_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_CRON_ENABLE, 0, 16);
        write_u64_field(buf, base, 8, topic_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_cron_disable(buf: &mut BytesMut, topic_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_CRON_DISABLE, 0, 16);
        write_u64_field(buf, base, 8, topic_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_cron_trigger(buf: &mut BytesMut, topic_id: u64, triggered_at: u64) {
        let base = write_cmd_begin(buf, Self::TAG_CRON_TRIGGER, 0, 24);
        write_u64_field(buf, base, 8, topic_id);
        write_u64_field(buf, base, 16, triggered_at);
        write_cmd_finish(buf, base);
    }

    pub fn write_cron_update(buf: &mut BytesMut, topic_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_CRON_UPDATE, 0, 16);
        write_u64_field(buf, base, 8, topic_id);
        write_cmd_finish(buf, base);
    }

    // -- Sessions (40-48) --

    pub fn write_create_session(
        buf: &mut BytesMut,
        session_id: u64,
        client_id: &str,
        keep_alive_ms: u64,
        session_expiry_ms: u64,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_CREATE_SESSION, 0, 40);
        write_u64_field(buf, base, 8, session_id);
        write_u64_field(buf, base, 16, keep_alive_ms);
        write_u64_field(buf, base, 24, session_expiry_ms);
        write_flex8_str_field(buf, base, 32, client_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_disconnect_session(buf: &mut BytesMut, session_id: u64, publish_will: bool) {
        let flags = if publish_will { 1u8 } else { 0u8 };
        let base = write_cmd_begin(buf, Self::TAG_DISCONNECT_SESSION, flags, 16);
        write_u64_field(buf, base, 8, session_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_heartbeat_session(buf: &mut BytesMut, session_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_HEARTBEAT_SESSION, 0, 16);
        write_u64_field(buf, base, 8, session_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_set_will(
        buf: &mut BytesMut,
        session_id: u64,
        topic_id: u64,
        delay_secs: u32,
        qos: u8,
        retain: bool,
        routing_key: &str,
        message: &[u8],
    ) {
        let flags = (qos & 0x03) | (if retain { 0x04 } else { 0 });
        let base = write_cmd_begin(buf, Self::TAG_SET_WILL, flags, 48);
        write_u64_field(buf, base, 8, session_id);
        write_u64_field(buf, base, 16, topic_id);
        write_flex8_str_field(buf, base, 24, routing_key);
        write_flex8_field(buf, base, 32, message);
        write_u32_field(buf, base, 40, delay_secs);
        write_cmd_finish(buf, base);
    }

    pub fn write_clear_will(buf: &mut BytesMut, session_id: u64) {
        let base = write_cmd_begin(buf, Self::TAG_CLEAR_WILL, 0, 16);
        write_u64_field(buf, base, 8, session_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_fire_pending_wills(buf: &mut BytesMut, now_ms: u64) {
        let base = write_cmd_begin(buf, Self::TAG_FIRE_PENDING_WILLS, 0, 16);
        write_u64_field(buf, base, 8, now_ms);
        write_cmd_finish(buf, base);
    }

    pub fn write_persist_session(
        buf: &mut BytesMut,
        session_id: u64,
        client_id: &str,
        session_expiry_secs: u32,
        subscription_data: &[u8],
        inbound_qos_inflight: u32,
        outbound_qos1_count: u32,
        remaining_quota: u64,
    ) {
        let base = write_cmd_begin(buf, Self::TAG_PERSIST_SESSION, 0, 56);
        write_u64_field(buf, base, 8, session_id);
        write_u64_field(buf, base, 16, remaining_quota);
        write_flex8_str_field(buf, base, 24, client_id);
        write_flex8_field(buf, base, 32, subscription_data);
        write_u32_field(buf, base, 40, session_expiry_secs);
        write_u32_field(buf, base, 44, inbound_qos_inflight);
        write_u32_field(buf, base, 48, outbound_qos1_count);
        write_cmd_finish(buf, base);
    }

    pub fn write_restore_session(buf: &mut BytesMut, client_id: &str) {
        let base = write_cmd_begin(buf, Self::TAG_RESTORE_SESSION, 0, 16);
        write_flex8_str_field(buf, base, 8, client_id);
        write_cmd_finish(buf, base);
    }

    pub fn write_expire_sessions(buf: &mut BytesMut, now_ms: u64) {
        let base = write_cmd_begin(buf, Self::TAG_EXPIRE_SESSIONS, 0, 16);
        write_u64_field(buf, base, 8, now_ms);
        write_cmd_finish(buf, base);
    }

    // -- Batch (49) --

    pub fn write_batch(buf: &mut BytesMut, commands: &[MqCommand]) {
        Self::write_batch_with_id(buf, 0, commands);
    }

    pub fn write_batch_with_id(buf: &mut BytesMut, batch_id: u32, commands: &[MqCommand]) {
        let base = write_cmd_begin(buf, Self::TAG_BATCH, 0, 16);
        write_u32_field(buf, base, 8, commands.len() as u32);
        write_u32_field(buf, base, 12, batch_id);
        for cmd in commands {
            buf.extend_from_slice(&cmd.buf);
            let pad = (8 - (cmd.buf.len() % 8)) % 8;
            if pad > 0 {
                buf.put_bytes(0, pad);
            }
        }
        write_cmd_finish(buf, base);
    }

    // -- Dedup (50) --

    pub fn write_prune_dedup_window(buf: &mut BytesMut, topic_id: u64, before_timestamp: u64) {
        let base = write_cmd_begin(buf, Self::TAG_PRUNE_DEDUP_WINDOW, 0, 24);
        write_u64_field(buf, base, 8, topic_id);
        write_u64_field(buf, base, 16, before_timestamp);
        write_cmd_finish(buf, base);
    }

    // -- ForwardedBatch (55) --

    /// Write TAG_FORWARDED_BATCH into `buf`.
    /// Layout: `@8 node_id:u32, @12 count:u32, @16 [sub-frames...]`
    pub fn write_forwarded_batch(buf: &mut BytesMut, node_id: u32, count: u32, frames: &[u8]) {
        let base = write_cmd_begin(buf, Self::TAG_FORWARDED_BATCH, 0, 16);
        write_u32_field(buf, base, 8, node_id);
        write_u32_field(buf, base, 12, count);
        buf.extend_from_slice(frames);
        write_cmd_finish(buf, base);
    }

    /// Patch the TAG_FORWARDED_BATCH header into the first 16 bytes of `buf`.
    /// `buf` must start with 16 zeroed bytes (via `buf.put_bytes(0, 16)`) followed by sub-frames.
    pub fn write_forwarded_batch_header(buf: &mut bytes::BytesMut, node_id: u32, count: u32) {
        let total = buf.len() as u32;
        buf[0..4].copy_from_slice(&total.to_le_bytes());
        buf[4..6].copy_from_slice(&16u16.to_le_bytes());
        buf[6] = Self::TAG_FORWARDED_BATCH;
        buf[7] = 0;
        buf[8..12].copy_from_slice(&node_id.to_le_bytes());
        buf[12..16].copy_from_slice(&count.to_le_bytes());
    }

    // -- Resume (56) --

    /// Write TAG_RESUME into `buf`. Wire layout: `[TAG_RESUME:1][pad:3][session_client_id:4][last_acked_seq:8]`
    pub fn write_resume(buf: &mut BytesMut, session_client_id: u32, last_acked_seq: u64) {
        buf.put_u8(Self::TAG_RESUME);
        buf.put_bytes(0, 3);
        buf.put_u32_le(session_client_id);
        buf.put_u64_le(last_acked_seq);
    }
}

// MqCommand — Convenience constructors (caller supplies &mut BytesMut scratch buffer)
// =============================================================================

impl MqCommand {
    pub fn create_topic(
        buf: &mut BytesMut,
        name: &str,
        retention: RetentionPolicy,
        partition_count: u32,
    ) -> Self {
        Self::write_create_topic(buf, name, &retention, partition_count);
        Self::split_from(buf)
    }
    pub fn delete_topic(buf: &mut BytesMut, topic_id: u64) -> Self {
        Self::write_delete_topic(buf, topic_id);
        Self::split_from(buf)
    }
    pub fn publish(buf: &mut BytesMut, topic_id: u64, messages: &[Bytes]) -> Self {
        Self::write_publish_bytes(buf, topic_id, messages);
        Self::split_from(buf)
    }
    pub fn publish_scatter(buf: &mut BytesMut, topic_id: u64, messages: Vec<Bytes>) -> Self {
        Self::write_publish_bytes(buf, topic_id, &messages);
        Self::split_from(buf)
    }
    pub fn commit_offset(buf: &mut BytesMut, topic_id: u64, consumer_id: u64, offset: u64) -> Self {
        Self::write_commit_offset(buf, topic_id, consumer_id, offset);
        Self::split_from(buf)
    }
    pub fn purge_topic(buf: &mut BytesMut, topic_id: u64, before_index: u64) -> Self {
        Self::write_purge_topic(buf, topic_id, before_index);
        Self::split_from(buf)
    }
    pub fn set_retained(
        buf: &mut BytesMut,
        exchange_id: u64,
        routing_key: &str,
        message: &[u8],
    ) -> Self {
        Self::write_set_retained(buf, exchange_id, routing_key, message);
        Self::split_from(buf)
    }
    pub fn get_retained(
        buf: &mut BytesMut,
        exchange_id: u64,
        routing_key_filter: Option<&str>,
    ) -> Self {
        Self::write_get_retained(buf, exchange_id, routing_key_filter);
        Self::split_from(buf)
    }
    pub fn delete_retained(buf: &mut BytesMut, exchange_id: u64, routing_key: &str) -> Self {
        Self::write_delete_retained(buf, exchange_id, routing_key);
        Self::split_from(buf)
    }
    pub fn create_exchange(buf: &mut BytesMut, name: &str, exchange_type: ExchangeType) -> Self {
        Self::write_create_exchange(buf, name, exchange_type);
        Self::split_from(buf)
    }
    pub fn delete_exchange(buf: &mut BytesMut, exchange_id: u64) -> Self {
        Self::write_delete_exchange(buf, exchange_id);
        Self::split_from(buf)
    }
    pub fn create_binding(
        buf: &mut BytesMut,
        exchange_id: u64,
        topic_id: u64,
        routing_key: Option<&str>,
    ) -> Self {
        Self::write_create_binding(buf, exchange_id, topic_id, routing_key);
        Self::split_from(buf)
    }
    pub fn create_binding_with_opts(
        buf: &mut BytesMut,
        exchange_id: u64,
        topic_id: u64,
        routing_key: Option<&str>,
        no_local: bool,
        shared_group: Option<&str>,
        subscription_id: Option<u32>,
    ) -> Self {
        Self::write_create_binding_with_opts(
            buf,
            exchange_id,
            topic_id,
            routing_key,
            no_local,
            shared_group,
            subscription_id,
        );
        Self::split_from(buf)
    }
    pub fn publish_to_exchange(buf: &mut BytesMut, exchange_id: u64, messages: &[Bytes]) -> Self {
        Self::write_publish_to_exchange_bytes(buf, exchange_id, messages);
        Self::split_from(buf)
    }
    pub fn create_consumer_group(buf: &mut BytesMut, name: &str, auto_offset_reset: u8) -> Self {
        Self::write_create_consumer_group(buf, name, auto_offset_reset);
        Self::split_from(buf)
    }
    pub fn create_queue(
        buf: &mut BytesMut,
        name: &str,
        config: AckVariantConfig,
        retention: RetentionPolicy,
        dedup: Option<&TopicDedupConfig>,
        enable_dlq: bool,
        dlq_name: Option<&str>,
        enable_response: bool,
        response_name: Option<&str>,
    ) -> Self {
        Self::write_create_queue(
            buf,
            name,
            config,
            retention,
            dedup,
            enable_dlq,
            dlq_name,
            enable_response,
            response_name,
        );
        Self::split_from(buf)
    }
    pub fn create_actor_group(
        buf: &mut BytesMut,
        name: &str,
        config: ActorVariantConfig,
        retention: RetentionPolicy,
        enable_response: bool,
        response_name: Option<&str>,
    ) -> Self {
        Self::write_create_actor_group(
            buf,
            name,
            config,
            retention,
            enable_response,
            response_name,
        );
        Self::split_from(buf)
    }
    pub fn create_job_cron(
        buf: &mut BytesMut,
        name: &str,
        ack_config: AckVariantConfig,
        retention: RetentionPolicy,
    ) -> Self {
        Self::write_create_job_cron(buf, name, ack_config, retention);
        Self::split_from(buf)
    }
    pub fn delete_consumer_group(buf: &mut BytesMut, group_id: u64) -> Self {
        Self::write_delete_consumer_group(buf, group_id);
        Self::split_from(buf)
    }
    pub fn commit_group_offset(
        buf: &mut BytesMut,
        group_id: u64,
        generation: i32,
        topic_id: u64,
        partition_index: u32,
        offset: u64,
        metadata: Option<&str>,
        timestamp: u64,
    ) -> Self {
        Self::write_commit_group_offset(
            buf,
            group_id,
            generation,
            topic_id,
            partition_index,
            offset,
            metadata,
            timestamp,
        );
        Self::split_from(buf)
    }
    pub fn join_consumer_group(
        buf: &mut BytesMut,
        group_id: u64,
        member_id: &str,
        client_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: &str,
        protocols: &[(&str, &[u8])],
    ) -> Self {
        Self::write_join_consumer_group(
            buf,
            group_id,
            member_id,
            client_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            protocols,
        );
        Self::split_from(buf)
    }
    pub fn sync_consumer_group(
        buf: &mut BytesMut,
        group_id: u64,
        generation: i32,
        member_id: &str,
        assignments: &[(&str, &[u8])],
    ) -> Self {
        Self::write_sync_consumer_group(buf, group_id, generation, member_id, assignments);
        Self::split_from(buf)
    }
    pub fn leave_consumer_group(buf: &mut BytesMut, group_id: u64, member_id: &str) -> Self {
        Self::write_leave_consumer_group(buf, group_id, member_id);
        Self::split_from(buf)
    }
    pub fn heartbeat_consumer_group(
        buf: &mut BytesMut,
        group_id: u64,
        member_id: &str,
        generation: i32,
    ) -> Self {
        Self::write_heartbeat_consumer_group(buf, group_id, member_id, generation);
        Self::split_from(buf)
    }
    pub fn expire_group_sessions(buf: &mut BytesMut, now_ms: u64) -> Self {
        Self::write_expire_group_sessions(buf, now_ms);
        Self::split_from(buf)
    }
    pub fn group_deliver(
        buf: &mut BytesMut,
        group_id: u64,
        consumer_id: u64,
        max_count: u32,
    ) -> Self {
        Self::write_group_deliver(buf, group_id, consumer_id, max_count);
        Self::split_from(buf)
    }
    pub fn group_ack(
        buf: &mut BytesMut,
        group_id: u64,
        message_ids: &[u64],
        response: Option<&Bytes>,
    ) -> Self {
        Self::write_group_ack(buf, group_id, message_ids, response);
        Self::split_from(buf)
    }
    pub fn group_nack(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) -> Self {
        Self::write_group_nack(buf, group_id, message_ids);
        Self::split_from(buf)
    }
    pub fn group_release(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) -> Self {
        Self::write_group_release(buf, group_id, message_ids);
        Self::split_from(buf)
    }
    pub fn group_modify(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) -> Self {
        Self::write_group_modify(buf, group_id, message_ids);
        Self::split_from(buf)
    }
    pub fn group_extend_visibility(
        buf: &mut BytesMut,
        group_id: u64,
        message_ids: &[u64],
        extension_ms: u64,
    ) -> Self {
        Self::write_group_extend_visibility(buf, group_id, message_ids, extension_ms);
        Self::split_from(buf)
    }
    pub fn group_timeout_expired(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) -> Self {
        Self::write_group_timeout_expired(buf, group_id, message_ids);
        Self::split_from(buf)
    }
    pub fn group_expire_pending(buf: &mut BytesMut, group_id: u64, message_ids: &[u64]) -> Self {
        Self::write_group_expire_pending(buf, group_id, message_ids);
        Self::split_from(buf)
    }
    pub fn group_publish_to_dlq(
        buf: &mut BytesMut,
        source_group_id: u64,
        dlq_topic_id: u64,
        dead_letter_ids: &[u64],
        messages: &[Bytes],
    ) -> Self {
        let msgs: Vec<&[u8]> = messages.iter().map(|b| b.as_ref()).collect();
        Self::write_group_publish_to_dlq(
            buf,
            source_group_id,
            dlq_topic_id,
            dead_letter_ids,
            &msgs,
        );
        Self::split_from(buf)
    }
    pub fn group_purge(buf: &mut BytesMut, group_id: u64) -> Self {
        Self::write_group_purge(buf, group_id);
        Self::split_from(buf)
    }
    pub fn group_get_attributes(buf: &mut BytesMut, group_id: u64) -> Self {
        Self::write_group_get_attributes(buf, group_id);
        Self::split_from(buf)
    }
    pub fn group_deliver_actor(
        buf: &mut BytesMut,
        group_id: u64,
        consumer_id: u64,
        actor_ids: &[Bytes],
    ) -> Self {
        let ids: Vec<&[u8]> = actor_ids.iter().map(|b| b.as_ref()).collect();
        Self::write_group_deliver_actor(buf, group_id, consumer_id, &ids);
        Self::split_from(buf)
    }
    pub fn group_ack_actor(
        buf: &mut BytesMut,
        group_id: u64,
        actor_id: &[u8],
        message_id: u64,
        response: Option<&Bytes>,
    ) -> Self {
        Self::write_group_ack_actor(buf, group_id, actor_id, message_id, response);
        Self::split_from(buf)
    }
    pub fn group_nack_actor(
        buf: &mut BytesMut,
        group_id: u64,
        actor_id: &[u8],
        message_id: u64,
    ) -> Self {
        Self::write_group_nack_actor(buf, group_id, actor_id, message_id);
        Self::split_from(buf)
    }
    pub fn group_assign_actors(
        buf: &mut BytesMut,
        group_id: u64,
        consumer_id: u64,
        actor_ids: &[Bytes],
    ) -> Self {
        let ids: Vec<&[u8]> = actor_ids.iter().map(|b| b.as_ref()).collect();
        Self::write_group_assign_actors(buf, group_id, consumer_id, &ids);
        Self::split_from(buf)
    }
    pub fn group_release_actors(buf: &mut BytesMut, group_id: u64, consumer_id: u64) -> Self {
        Self::write_group_release_actors(buf, group_id, consumer_id);
        Self::split_from(buf)
    }
    pub fn group_evict_idle(buf: &mut BytesMut, group_id: u64, before_timestamp: u64) -> Self {
        Self::write_group_evict_idle(buf, group_id, before_timestamp);
        Self::split_from(buf)
    }
    pub fn cron_enable(buf: &mut BytesMut, topic_id: u64) -> Self {
        Self::write_cron_enable(buf, topic_id);
        Self::split_from(buf)
    }
    pub fn cron_disable(buf: &mut BytesMut, topic_id: u64) -> Self {
        Self::write_cron_disable(buf, topic_id);
        Self::split_from(buf)
    }
    pub fn cron_trigger(buf: &mut BytesMut, topic_id: u64, triggered_at: u64) -> Self {
        Self::write_cron_trigger(buf, topic_id, triggered_at);
        Self::split_from(buf)
    }
    pub fn create_session(
        buf: &mut BytesMut,
        session_id: u64,
        client_id: &str,
        keep_alive_ms: u64,
        session_expiry_ms: u64,
    ) -> Self {
        Self::write_create_session(buf, session_id, client_id, keep_alive_ms, session_expiry_ms);
        Self::split_from(buf)
    }
    pub fn disconnect_session(buf: &mut BytesMut, session_id: u64, publish_will: bool) -> Self {
        Self::write_disconnect_session(buf, session_id, publish_will);
        Self::split_from(buf)
    }
    pub fn heartbeat_session(buf: &mut BytesMut, session_id: u64) -> Self {
        Self::write_heartbeat_session(buf, session_id);
        Self::split_from(buf)
    }
    pub fn set_will(
        buf: &mut BytesMut,
        session_id: u64,
        topic_id: u64,
        delay_secs: u32,
        qos: u8,
        retain: bool,
        routing_key: &str,
        message: &[u8],
    ) -> Self {
        Self::write_set_will(
            buf,
            session_id,
            topic_id,
            delay_secs,
            qos,
            retain,
            routing_key,
            message,
        );
        Self::split_from(buf)
    }
    pub fn clear_will(buf: &mut BytesMut, session_id: u64) -> Self {
        Self::write_clear_will(buf, session_id);
        Self::split_from(buf)
    }
    pub fn fire_pending_wills(buf: &mut BytesMut, now_ms: u64) -> Self {
        Self::write_fire_pending_wills(buf, now_ms);
        Self::split_from(buf)
    }
    pub fn persist_session(
        buf: &mut BytesMut,
        session_id: u64,
        client_id: &str,
        session_expiry_secs: u32,
        subscription_data: &[u8],
        inbound_qos_inflight: u32,
        outbound_qos1_count: u32,
        remaining_quota: u64,
    ) -> Self {
        Self::write_persist_session(
            buf,
            session_id,
            client_id,
            session_expiry_secs,
            subscription_data,
            inbound_qos_inflight,
            outbound_qos1_count,
            remaining_quota,
        );
        Self::split_from(buf)
    }
    pub fn restore_session(buf: &mut BytesMut, client_id: &str) -> Self {
        Self::write_restore_session(buf, client_id);
        Self::split_from(buf)
    }
    pub fn expire_sessions(buf: &mut BytesMut, now_ms: u64) -> Self {
        Self::write_expire_sessions(buf, now_ms);
        Self::split_from(buf)
    }
    pub fn batch(buf: &mut BytesMut, commands: &[MqCommand]) -> Self {
        Self::write_batch(buf, commands);
        Self::split_from(buf)
    }
}

// =============================================================================
// MqCommand — View accessor methods
// =============================================================================

impl MqCommand {
    pub fn as_create_topic(&self) -> CmdCreateTopic<'_> {
        CmdCreateTopic { buf: &self.buf }
    }

    pub fn as_publish(&self) -> CmdPublish<'_> {
        CmdPublish { buf: &self.buf }
    }

    pub fn collect_publish_messages(&self) -> Vec<Bytes> {
        self.as_publish().messages().collect()
    }

    pub fn take_publish_segments(&mut self) -> Vec<Bytes> {
        CmdPublish { buf: &self.buf }.messages().collect()
    }

    pub fn as_create_exchange(&self) -> CmdCreateExchange<'_> {
        CmdCreateExchange { buf: &self.buf }
    }

    pub fn as_create_binding(&self) -> CmdCreateBinding<'_> {
        CmdCreateBinding { buf: &self.buf }
    }

    pub fn as_publish_to_exchange(&self) -> CmdPublishToExchange<'_> {
        CmdPublishToExchange { buf: &self.buf }
    }

    pub fn as_batch(&self) -> CmdBatch<'_> {
        CmdBatch { buf: &self.buf }
    }

    pub fn as_forwarded_batch(&self) -> CmdForwardedBatch<'_> {
        CmdForwardedBatch { buf: &self.buf }
    }

    pub fn as_resume(&self) -> CmdResume<'_> {
        CmdResume { buf: &self.buf }
    }

    pub fn as_create_consumer_group(&self) -> CmdCreateConsumerGroup<'_> {
        CmdCreateConsumerGroup { buf: &self.buf }
    }

    pub fn as_commit_group_offset(&self) -> CmdCommitGroupOffset<'_> {
        CmdCommitGroupOffset { buf: &self.buf }
    }

    pub fn as_join_consumer_group(&self) -> CmdJoinConsumerGroup<'_> {
        CmdJoinConsumerGroup { buf: &self.buf }
    }

    pub fn as_sync_consumer_group(&self) -> CmdSyncConsumerGroup<'_> {
        CmdSyncConsumerGroup { buf: &self.buf }
    }

    pub fn as_leave_consumer_group(&self) -> CmdLeaveConsumerGroup<'_> {
        CmdLeaveConsumerGroup { buf: &self.buf }
    }

    pub fn as_heartbeat_consumer_group(&self) -> CmdHeartbeatConsumerGroup<'_> {
        CmdHeartbeatConsumerGroup { buf: &self.buf }
    }

    pub fn as_set_retained(&self) -> CmdSetRetained<'_> {
        CmdSetRetained { buf: &self.buf }
    }

    pub fn as_get_retained(&self) -> CmdGetRetained<'_> {
        CmdGetRetained { buf: &self.buf }
    }

    pub fn as_delete_retained(&self) -> CmdDeleteRetained<'_> {
        CmdDeleteRetained { buf: &self.buf }
    }

    pub fn as_set_will(&self) -> CmdSetWill<'_> {
        CmdSetWill { buf: &self.buf }
    }

    pub fn as_persist_session(&self) -> CmdPersistSession<'_> {
        CmdPersistSession { buf: &self.buf }
    }

    pub fn as_restore_session(&self) -> CmdRestoreSession<'_> {
        CmdRestoreSession { buf: &self.buf }
    }

    pub fn as_publish_to_dlq(&self) -> CmdPublishToDlq<'_> {
        CmdPublishToDlq { buf: &self.buf }
    }

    // -- Message extraction helpers --

    /// For `Publish` or `PublishToExchange`: iterate messages zero-copy via vec_bytes.
    /// Returns `None` for other command types.
    #[inline]
    pub fn publish_messages(&self) -> Option<VecBytesIter<'_>> {
        let tag = self.tag();
        if tag != Self::TAG_PUBLISH
            && tag != Self::TAG_PUBLISH_TO_EXCHANGE
            && tag != Self::TAG_PUBLISH_TO_EXCHANGE_MQTT
        {
            return None;
        }
        let count = self.field_vec_bytes_count(16);
        Some(VecBytesIter::new(&self.buf, 16, count))
    }

    #[inline]
    pub fn publish_messages_slice_iter(&self) -> Option<VecSliceIter<'_>> {
        let tag = self.tag();
        if tag != Self::TAG_PUBLISH
            && tag != Self::TAG_PUBLISH_TO_EXCHANGE
            && tag != Self::TAG_PUBLISH_TO_EXCHANGE_MQTT
        {
            return None;
        }
        Some(VecSliceIter::new(&self.buf, 16))
    }

    #[inline]
    pub fn publish_messages_for_topic(&self, topic_id: u64) -> Option<VecBytesIter<'_>> {
        if self.tag() != Self::TAG_PUBLISH {
            return None;
        }
        if self.field_u64(8) != topic_id {
            return None;
        }
        let count = self.field_vec_bytes_count(16);
        Some(VecBytesIter::new(&self.buf, 16, count))
    }
}

// =============================================================================
// View struct helpers — read flex8/blob/vec from raw buffer
// =============================================================================

/// Read a flex8 slot from a raw buffer at the given offset.
///
/// Bit-0 tag: `byte[0] & 1 == 0` → small inline, `byte[0] & 1 == 1` → large.
#[inline]
fn read_flex8(buf: &[u8], offset: usize) -> &[u8] {
    let first = buf[offset];
    if first & 1 == 0 {
        let len = (first >> 1) as usize;
        if len == 0 {
            return &[];
        }
        &buf[offset + 1..offset + 1 + len]
    } else {
        let raw = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
        let data_offset = (raw >> 1) as usize;
        let size = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap()) as usize;
        &buf[data_offset..data_offset + size]
    }
}

/// Read a flex8 slot as &str.
#[inline]
fn read_flex8_str(buf: &[u8], offset: usize) -> &str {
    std::str::from_utf8(read_flex8(buf, offset)).unwrap_or("")
}

/// Read a blob slot from a raw buffer: `[offset:4][size:4]`.
#[inline]
fn read_blob(buf: &[u8], offset: usize) -> &[u8] {
    let data_offset = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
    let size = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap()) as usize;
    if size == 0 {
        return &[];
    }
    &buf[data_offset..data_offset + size]
}

// =============================================================================
// View structs — zero-copy accessors over MqCommand buffers
// =============================================================================

/// Zero-copy view over a CreateTopic command.
/// Layout: @8 name:flex8, @16 retention:blob, @24 partition_count:u32
pub struct CmdCreateTopic<'a> {
    buf: &'a [u8],
}

impl<'a> CmdCreateTopic<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn name(&self) -> &str {
        std::str::from_utf8(read_flex8(self.buf, 8)).unwrap_or("")
    }

    pub fn retention(&self) -> RetentionPolicy {
        let blob = read_blob(self.buf, 16);
        if blob.is_empty() {
            return RetentionPolicy::default();
        }
        let mut cursor = std::io::Cursor::new(blob);
        RetentionPolicy::decode(&mut cursor).unwrap_or_default()
    }

    pub fn partition_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[24..28].try_into().unwrap())
    }
}

/// Zero-copy view over a Publish command.
/// Layout: @8 topic_id:u64, @16 messages:vec_bytes
pub struct CmdPublish<'a> {
    buf: &'a [u8],
}

impl<'a> CmdPublish<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn message_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[16..20].try_into().unwrap())
    }

    pub fn messages(&self) -> VecBytesIter<'a> {
        VecBytesIter::new(self.buf, 16, self.message_count())
    }

    pub fn messages_slice_iter(&self) -> VecSliceIter<'a> {
        VecSliceIter::new(self.buf, 16)
    }
}

/// Zero-copy view over a CreateExchange command.
/// Layout: flags=exchange_type, @8 name:flex8
pub struct CmdCreateExchange<'a> {
    buf: &'a [u8],
}

impl<'a> CmdCreateExchange<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn name(&self) -> &str {
        read_flex8_str(self.buf, 8)
    }

    pub fn exchange_type(&self) -> ExchangeType {
        match self.buf[7] {
            // flags byte
            0 => ExchangeType::Direct,
            1 => ExchangeType::Fanout,
            2 => ExchangeType::Topic,
            _ => ExchangeType::Fanout,
        }
    }
}

/// Zero-copy view over a CreateBinding command.
/// Layout: flags[0]=no_local, @8 exchange_id:u64, @16 topic_id:u64,
///         @24 routing_key:opt_flex8, @32 shared_group:opt_flex8
pub struct CmdCreateBinding<'a> {
    buf: &'a [u8],
}

impl<'a> CmdCreateBinding<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    pub fn routing_key(&self) -> Option<String> {
        let data = read_flex8(self.buf, 24);
        if data.is_empty() {
            None
        } else {
            Some(std::str::from_utf8(data).unwrap_or("").to_string())
        }
    }

    pub fn no_local(&self) -> bool {
        self.buf[7] & 1 != 0 // flags bit 0
    }

    pub fn shared_group(&self) -> Option<String> {
        let data = read_flex8(self.buf, 32);
        if data.is_empty() {
            None
        } else {
            Some(std::str::from_utf8(data).unwrap_or("").to_string())
        }
    }

    pub fn subscription_id(&self) -> Option<u32> {
        // @40 subscription_id:u32, @44 has_subscription_id:u8
        if self.buf.len() >= 45 && self.buf[44] == 1 {
            Some(u32::from_le_bytes(self.buf[40..44].try_into().unwrap()))
        } else {
            None
        }
    }
}

/// Zero-copy view over a PublishToExchange command.
/// Layout: @8 exchange_id:u64, @16 messages:vec_bytes
pub struct CmdPublishToExchange<'a> {
    buf: &'a [u8],
}

impl<'a> CmdPublishToExchange<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn messages(&self) -> VecBytesIter<'a> {
        let count = u32::from_le_bytes(self.buf[16..20].try_into().unwrap());
        VecBytesIter::new(self.buf, 16, count)
    }

    pub fn messages_slice_iter(&self) -> VecSliceIter<'a> {
        VecSliceIter::new(self.buf, 16)
    }
}

/// Zero-copy view over a PublishToDlq command.
/// Layout: @8 source_group_id:u64, @16 dlq_topic_id:u64,
///         @24 dead_letter_ids:vec_u64, @32 messages:vec_bytes
pub struct CmdPublishToDlq<'a> {
    buf: &'a [u8],
}

impl<'a> CmdPublishToDlq<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn source_group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn dlq_topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    pub fn dead_letter_ids(&self) -> SmallVec<[u64; 8]> {
        let count = u32::from_le_bytes(self.buf[24..28].try_into().unwrap()) as usize;
        if count == 0 {
            return SmallVec::new();
        }
        let data_offset = u32::from_le_bytes(self.buf[28..32].try_into().unwrap()) as usize;
        let mut v = SmallVec::with_capacity(count.min(4096));
        for i in 0..count.min(4096) {
            let off = data_offset + i * 8;
            v.push(u64::from_le_bytes(
                self.buf[off..off + 8].try_into().unwrap(),
            ));
        }
        v
    }

    pub fn messages(&self) -> VecBytesIter<'a> {
        let count = u32::from_le_bytes(self.buf[32..36].try_into().unwrap());
        VecBytesIter::new(self.buf, 32, count)
    }
}

/// Zero-copy view over a Batch command.
/// Layout: @8 count:u32, fixed=16, flex: self-sized sub-commands with 8-byte padding
pub struct CmdBatch<'a> {
    buf: &'a [u8],
}

impl<'a> CmdBatch<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn count(&self) -> u32 {
        u32::from_le_bytes(self.buf[8..12].try_into().unwrap())
    }

    /// Batch ID registered in `AsyncApplyManager::batch_registry`.
    /// 0 = no registry entry (legacy or no response needed).
    pub fn batch_id(&self) -> u32 {
        u32::from_le_bytes(self.buf[12..16].try_into().unwrap())
    }

    pub fn commands(&self) -> BatchIter<'a> {
        // Sub-commands start at offset 16 (fixed_size)
        BatchIter {
            buf: self.buf,
            offset: 16,
            remaining: self.count(),
        }
    }
}

// =============================================================================
// Consumer Group view structs
// =============================================================================

/// Zero-copy view over a CreateConsumerGroup command.
/// Layout: @8 name:flex8, @16 dlq_topic_name:flex8, @24 response_topic_name:flex8,
///         @32 variant_config:blob, @40 topic_retention:blob, @48 topic_dedup:blob,
///         @56 auto_offset_reset:u8, auto_create_topic:u8, topic_lifetime:u8
pub struct CmdCreateConsumerGroup<'a> {
    buf: &'a [u8],
}

impl<'a> CmdCreateConsumerGroup<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn name(&self) -> &str {
        read_flex8_str(self.buf, 8)
    }

    pub fn auto_offset_reset(&self) -> u8 {
        self.buf[56]
    }

    pub fn variant_config(&self) -> VariantConfig {
        let blob = read_blob(self.buf, 32);
        if blob.is_empty() {
            return VariantConfig::default();
        }
        let mut cursor = std::io::Cursor::new(blob);
        VariantConfig::decode(&mut cursor).unwrap_or_default()
    }

    pub fn auto_create_topic(&self) -> bool {
        self.buf[57] != 0
    }

    pub fn topic_retention(&self) -> RetentionPolicy {
        let blob = read_blob(self.buf, 40);
        if blob.is_empty() {
            return RetentionPolicy::default();
        }
        let mut cursor = std::io::Cursor::new(blob);
        RetentionPolicy::decode(&mut cursor).unwrap_or_default()
    }

    pub fn topic_dedup(&self) -> Option<TopicDedupConfig> {
        let blob = read_blob(self.buf, 48);
        if blob.is_empty() {
            return None;
        }
        let mut cursor = std::io::Cursor::new(blob);
        TopicDedupConfig::decode(&mut cursor).ok()
    }

    pub fn topic_lifetime(&self) -> TopicLifetimePolicy {
        match self.buf[58] {
            1 => TopicLifetimePolicy::DeleteOnLastDetach,
            _ => TopicLifetimePolicy::Permanent,
        }
    }

    pub fn dlq_topic_name(&self) -> Option<String> {
        let data = read_flex8(self.buf, 16);
        if data.is_empty() {
            None
        } else {
            Some(std::str::from_utf8(data).unwrap_or("").to_string())
        }
    }

    pub fn response_topic_name(&self) -> Option<String> {
        let data = read_flex8(self.buf, 24);
        if data.is_empty() {
            None
        } else {
            Some(std::str::from_utf8(data).unwrap_or("").to_string())
        }
    }
}

/// Zero-copy view over a CommitGroupOffset command.
/// Layout: @8 group_id:u64, @16 topic_id:u64, @24 offset:u64,
///         @32 timestamp:u64, @40 metadata:opt_flex8,
///         @48 generation:i32, partition_index:u32
pub struct CmdCommitGroupOffset<'a> {
    buf: &'a [u8],
}

impl<'a> CmdCommitGroupOffset<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn generation(&self) -> i32 {
        i32::from_le_bytes(self.buf[48..52].try_into().unwrap())
    }

    pub fn topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    pub fn partition_index(&self) -> u32 {
        u32::from_le_bytes(self.buf[52..56].try_into().unwrap())
    }

    pub fn offset(&self) -> u64 {
        u64::from_le_bytes(self.buf[24..32].try_into().unwrap())
    }

    pub fn metadata(&self) -> Option<&str> {
        let data = read_flex8(self.buf, 40);
        if data.is_empty() {
            None
        } else {
            Some(std::str::from_utf8(data).unwrap_or(""))
        }
    }

    /// Zero-copy metadata bytes from the command buffer.
    pub fn metadata_bytes(&self) -> Option<&'a [u8]> {
        let data = read_flex8(self.buf, 40);
        if data.is_empty() { None } else { Some(data) }
    }

    pub fn timestamp(&self) -> u64 {
        u64::from_le_bytes(self.buf[32..40].try_into().unwrap())
    }
}

/// Zero-copy view over a JoinConsumerGroup command.
/// Layout: @8 group_id:u64, @16 member_id:flex8, @24 client_id:flex8,
///         @32 protocol_type:flex8, @40 session_timeout_ms:i32|rebalance_timeout_ms:i32,
///         @48 protocols:vec_kv
pub struct CmdJoinConsumerGroup<'a> {
    buf: &'a [u8],
}

impl<'a> CmdJoinConsumerGroup<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn member_id(&self) -> &str {
        read_flex8_str(self.buf, 16)
    }

    pub fn client_id(&self) -> &str {
        read_flex8_str(self.buf, 24)
    }

    pub fn session_timeout_ms(&self) -> i32 {
        i32::from_le_bytes(self.buf[40..44].try_into().unwrap())
    }

    pub fn rebalance_timeout_ms(&self) -> i32 {
        i32::from_le_bytes(self.buf[44..48].try_into().unwrap())
    }

    pub fn protocol_type(&self) -> &str {
        read_flex8_str(self.buf, 32)
    }

    pub fn protocols_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[48..52].try_into().unwrap())
    }

    pub fn protocols(&self) -> Vec<(String, Bytes)> {
        let count = self.protocols_count() as usize;
        if count == 0 {
            return Vec::new();
        }
        let table_offset = u32::from_le_bytes(self.buf[52..56].try_into().unwrap()) as usize;
        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let entry_off = table_offset + i * 16;
            let name = read_flex8_str(self.buf, entry_off);
            let val = read_flex8(self.buf, entry_off + 8);
            result.push((name.to_string(), Bytes::copy_from_slice(val)));
        }
        result
    }
}

/// Zero-copy view over a SyncConsumerGroup command.
/// Layout: @8 group_id:u64, @16 member_id:flex8, @24 generation:i32,
///         @32 assignments:vec_kv
pub struct CmdSyncConsumerGroup<'a> {
    buf: &'a [u8],
}

impl<'a> CmdSyncConsumerGroup<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn generation(&self) -> i32 {
        i32::from_le_bytes(self.buf[24..28].try_into().unwrap())
    }

    pub fn member_id(&self) -> &str {
        read_flex8_str(self.buf, 16)
    }

    pub fn assignments_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[32..36].try_into().unwrap())
    }

    pub fn assignments(&self) -> Vec<(String, Vec<u8>)> {
        let count = self.assignments_count() as usize;
        if count == 0 {
            return Vec::new();
        }
        let table_offset = u32::from_le_bytes(self.buf[36..40].try_into().unwrap()) as usize;
        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let entry_off = table_offset + i * 16;
            let mid = read_flex8_str(self.buf, entry_off);
            let data = read_flex8(self.buf, entry_off + 8);
            result.push((mid.to_string(), data.to_vec()));
        }
        result
    }
}

/// Zero-copy view over a LeaveConsumerGroup command.
/// Layout: @8 group_id:u64, @16 member_id:flex8
pub struct CmdLeaveConsumerGroup<'a> {
    buf: &'a [u8],
}

impl<'a> CmdLeaveConsumerGroup<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn member_id(&self) -> &str {
        read_flex8_str(self.buf, 16)
    }
}

/// Zero-copy view over a HeartbeatConsumerGroup command.
/// Layout: @8 group_id:u64, @16 member_id:flex8, @24 generation:i32
pub struct CmdHeartbeatConsumerGroup<'a> {
    buf: &'a [u8],
}

impl<'a> CmdHeartbeatConsumerGroup<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn member_id(&self) -> &str {
        read_flex8_str(self.buf, 16)
    }

    pub fn generation(&self) -> i32 {
        i32::from_le_bytes(self.buf[24..28].try_into().unwrap())
    }
}

// =============================================================================
// Session / MQTT view structs
// =============================================================================

/// Layout: @8 exchange_id:u64, @16 routing_key:flex8, @24 message:flex8
pub struct CmdSetRetained<'a> {
    buf: &'a [u8],
}

impl<'a> CmdSetRetained<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn routing_key(&self) -> &str {
        read_flex8_str(self.buf, 16)
    }

    pub fn message(&self) -> Bytes {
        Bytes::copy_from_slice(read_flex8(self.buf, 24))
    }

    /// Zero-copy message slice from the command buffer.
    pub fn message_bytes(&self) -> &'a [u8] {
        read_flex8(self.buf, 24)
    }
}

/// Layout: @8 exchange_id:u64, @16 filter:opt_flex8
pub struct CmdGetRetained<'a> {
    buf: &'a [u8],
}

impl<'a> CmdGetRetained<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn routing_key_filter(&self) -> Option<String> {
        let data = read_flex8(self.buf, 16);
        if data.is_empty() {
            None
        } else {
            Some(std::str::from_utf8(data).unwrap_or("").to_string())
        }
    }
}

/// Layout: @8 exchange_id:u64, @16 routing_key:flex8
pub struct CmdDeleteRetained<'a> {
    buf: &'a [u8],
}

impl<'a> CmdDeleteRetained<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn routing_key(&self) -> &str {
        read_flex8_str(self.buf, 16)
    }
}

/// Layout: flags=[qos:2bits|retain:1bit], @8 session_id:u64, @16 topic_id:u64,
///         @24 routing_key:flex8, @32 message:flex8, @40 delay_secs:u32
pub struct CmdSetWill<'a> {
    buf: &'a [u8],
}

impl<'a> CmdSetWill<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn consumer_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    pub fn delay_secs(&self) -> u32 {
        u32::from_le_bytes(self.buf[40..44].try_into().unwrap())
    }

    pub fn qos(&self) -> u8 {
        self.buf[7] & 0x03 // flags bits 0-1
    }

    pub fn retain(&self) -> bool {
        self.buf[7] & 0x04 != 0 // flags bit 2
    }

    pub fn routing_key(&self) -> String {
        read_flex8_str(self.buf, 24).to_string()
    }

    pub fn message(&self) -> Bytes {
        Bytes::copy_from_slice(read_flex8(self.buf, 32))
    }
}

/// Layout: @8 session_id:u64, @16 remaining_quota:u64, @24 client_id:flex8,
///         @32 subscription_data:flex8, @40 session_expiry_secs:u32|inbound_qos_inflight:u32,
///         @48 outbound_qos1_count:u32
pub struct CmdPersistSession<'a> {
    buf: &'a [u8],
}

impl<'a> CmdPersistSession<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn consumer_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    pub fn client_id(&self) -> &str {
        read_flex8_str(self.buf, 24)
    }

    pub fn session_expiry_secs(&self) -> u32 {
        u32::from_le_bytes(self.buf[40..44].try_into().unwrap())
    }

    pub fn subscription_data(&self) -> Bytes {
        Bytes::copy_from_slice(read_flex8(self.buf, 32))
    }

    pub fn inbound_qos_inflight(&self) -> u32 {
        u32::from_le_bytes(self.buf[44..48].try_into().unwrap())
    }

    pub fn outbound_qos1_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[48..52].try_into().unwrap())
    }

    pub fn remaining_quota(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }
}

/// Layout: @8 client_id:flex8
pub struct CmdRestoreSession<'a> {
    buf: &'a [u8],
}

impl<'a> CmdRestoreSession<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn client_id(&self) -> &str {
        read_flex8_str(self.buf, 8)
    }
}

// =============================================================================
// Iterators
// =============================================================================

pub struct BatchIter<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: u32,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        // Each sub-command is self-sized: first 4 bytes = total size (including header)
        let size =
            u32::from_le_bytes(self.buf[self.offset..self.offset + 4].try_into().unwrap()) as usize;
        let slice = &self.buf[self.offset..self.offset + size];
        // Advance by size, padded to 8-byte boundary
        let padded = (size + 7) & !7;
        self.offset += padded;
        Some(slice)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

impl ExactSizeIterator for BatchIter<'_> {}

// =============================================================================
// CmdForwardedBatch / CmdForwardedBatchIter — TAG_FORWARDED_BATCH (55)
// =============================================================================

/// Zero-copy view over a TAG_FORWARDED_BATCH command.
///
/// Fixed region:
/// ```text
/// @8  node_id:   u32  — originating follower node ID (0 = local/leader)
/// @12 count:     u32  — number of sub-frames
/// @16 [sub-frames...]
/// ```
///
/// Each sub-frame:
/// ```text
/// [payload_len:4][client_id:4][request_seq:8][cmd_bytes + opt_pad]
/// ```
/// `payload_len` = `12 + cmd_bytes.len()` (client_id:4 + request_seq:8 + cmd, excludes len field).
/// Total sub-frame size: `4 + payload_len` bytes (may be padded to 8-byte boundary by write_batcher).
pub struct CmdForwardedBatch<'a> {
    buf: &'a [u8],
}

impl<'a> CmdForwardedBatch<'a> {
    /// Originating follower node ID (`0` means local/leader).
    #[inline]
    pub fn node_id(&self) -> u32 {
        u32::from_le_bytes(self.buf[8..12].try_into().unwrap())
    }

    /// Number of sub-frames in this batch.
    #[inline]
    pub fn count(&self) -> u32 {
        u32::from_le_bytes(self.buf[12..16].try_into().unwrap())
    }

    /// Iterate over `(client_id, request_seq, cmd_bytes)` sub-frames.
    #[inline]
    pub fn iter(&self) -> CmdForwardedBatchIter<'a> {
        CmdForwardedBatchIter {
            buf: self.buf,
            pos: 16, // sub-frames start at offset 16
        }
    }
}

/// Zero-copy iterator over sub-frames in a [`CmdForwardedBatch`].
///
/// Each call to [`next`](Iterator::next) parses one sub-frame header and returns
/// `(client_id: u32, request_seq: u64, cmd_bytes: &[u8])` with no allocation.
///
/// Sub-frame layout:
/// ```text
/// [payload_len:4][client_id:4][request_seq:8][cmd_bytes...]
/// payload_len = 12 + cmd_bytes.len()
/// advance     = 4 + payload_len = 16 + cmd_bytes.len()
/// ```
pub struct CmdForwardedBatchIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for CmdForwardedBatchIter<'a> {
    type Item = (u32, u64, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 4 > self.buf.len() {
            return None;
        }
        // payload_len = client_id(4) + request_seq(8) + cmd_bytes.len()
        // Excludes the 4-byte payload_len field itself.
        let payload_len =
            u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        // Minimum payload: client_id(4) + request_seq(8) = 12 bytes.
        if payload_len < 12 || self.pos + 4 + payload_len > self.buf.len() {
            return None;
        }
        let client_id =
            u32::from_le_bytes(self.buf[self.pos + 4..self.pos + 8].try_into().unwrap());
        let request_seq =
            u64::from_le_bytes(self.buf[self.pos + 8..self.pos + 16].try_into().unwrap());
        let cmd_bytes = &self.buf[self.pos + 16..self.pos + 4 + payload_len];
        self.pos += 4 + payload_len;
        Some((client_id, request_seq, cmd_bytes))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None) // count not tracked here; use CmdForwardedBatch::count()
    }
}

// =============================================================================
// CmdResume — TAG_RESUME (56)
// =============================================================================

/// Zero-copy view over a TAG_RESUME command.
///
/// Wire layout (16 bytes total):
/// ```text
/// [TAG_RESUME:1][pad:3][session_client_id:4][last_acked_seq:8]
/// ```
///
/// Sent by a client as the first sub-frame after reconnecting. The state machine
/// responds with its `last_applied_seq` for the session so the client can
/// discard already-applied commands and safely retry the rest.
pub struct CmdResume<'a> {
    buf: &'a [u8],
}

impl<'a> CmdResume<'a> {
    /// The `client_id` from the previous session to look up.
    #[inline]
    pub fn session_client_id(&self) -> u32 {
        u32::from_le_bytes(self.buf[4..8].try_into().unwrap())
    }

    /// The highest `request_seq` the client received a response for.
    #[inline]
    pub fn last_acked_seq(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }
}

/// Zero-copy iterator over a vec_bytes field in sequential format.
///
/// Fixed slot: `[count:4][data_start:4]` where `data_start` is the byte offset
/// (relative to command start) of the sequential data region.
/// Data region: `[len0:4][data0][len1:4][data1]...` — stream-oriented, no descriptor table.
pub struct VecBytesIter<'a> {
    buf: &'a [u8],
    pos: usize,
    count: usize,
    index: usize,
}

impl<'a> VecBytesIter<'a> {
    pub fn new(buf: &'a [u8], slot_offset: usize, count: u32) -> Self {
        let pos = if count == 0 {
            0
        } else {
            u32::from_le_bytes(buf[slot_offset + 4..slot_offset + 8].try_into().unwrap()) as usize
        };
        Self {
            buf,
            pos,
            count: count as usize,
            index: 0,
        }
    }

    #[inline]
    pub fn remaining(&self) -> u32 {
        (self.count - self.index) as u32
    }
}

impl<'a> Iterator for VecBytesIter<'a> {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Bytes> {
        if self.index >= self.count {
            return None;
        }
        let len = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        self.pos += 4;
        let data = Bytes::copy_from_slice(&self.buf[self.pos..self.pos + len]);
        self.pos += len;
        self.index += 1;
        Some(data)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.count - self.index;
        (r, Some(r))
    }
}

impl ExactSizeIterator for VecBytesIter<'_> {}

/// Iterator over a vec_bytes field yielding `&[u8]` slices instead of `Bytes`.
pub struct VecSliceIter<'a> {
    buf: &'a [u8],
    pos: usize,
    count: usize,
    index: usize,
}

impl<'a> VecSliceIter<'a> {
    pub fn new(buf: &'a [u8], slot_offset: usize) -> Self {
        let count =
            u32::from_le_bytes(buf[slot_offset..slot_offset + 4].try_into().unwrap()) as usize;
        let pos = if count == 0 {
            0
        } else {
            u32::from_le_bytes(buf[slot_offset + 4..slot_offset + 8].try_into().unwrap()) as usize
        };
        Self {
            buf,
            pos,
            count,
            index: 0,
        }
    }
}

impl<'a> Iterator for VecSliceIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.index >= self.count {
            return None;
        }
        let len = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        self.pos += 4;
        let data = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        self.index += 1;
        Some(data)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.count - self.index;
        (r, Some(r))
    }
}

impl ExactSizeIterator for VecSliceIter<'_> {
    fn len(&self) -> usize {
        self.count - self.index
    }
}

pub struct FlatOptBytes<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: u32,
}

impl<'a> Iterator for FlatOptBytes<'a> {
    type Item = Option<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let present = self.buf[self.offset];
        self.offset += 1;
        if present == 0 {
            Some(None)
        } else {
            let len = u32::from_le_bytes(self.buf[self.offset..self.offset + 4].try_into().unwrap())
                as usize;
            self.offset += 4;
            let val = Bytes::copy_from_slice(&self.buf[self.offset..self.offset + len]);
            self.offset += len;
            Some(Some(val))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

impl ExactSizeIterator for FlatOptBytes<'_> {}

/// Zero-copy iterator over length-prefixed messages in a flat command buffer.
pub struct FlatMessages<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: u32,
}

impl FlatMessages<'_> {
    #[inline]
    pub fn remaining(&self) -> u32 {
        self.remaining
    }
}

impl<'a> Iterator for FlatMessages<'a> {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Bytes> {
        if self.remaining == 0 {
            return None;
        }
        let buf_len = self.buf.len();
        if self.offset + 4 > buf_len {
            self.remaining = 0;
            return None;
        }
        let len =
            u32::from_le_bytes(self.buf[self.offset..self.offset + 4].try_into().unwrap()) as usize;
        self.offset += 4;
        if self.offset + len > buf_len {
            self.remaining = 0;
            return None;
        }
        let slice = Bytes::copy_from_slice(&self.buf[self.offset..self.offset + len]);
        self.offset += len;
        self.remaining -= 1;
        Some(slice)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

impl ExactSizeIterator for FlatMessages<'_> {}

// =============================================================================
// fmt_mq_command — Display formatter for MqCommand
// =============================================================================

pub fn fmt_mq_command(cmd: &MqCommand, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match cmd.tag() {
        MqCommand::TAG_CREATE_TOPIC => {
            let v = cmd.as_create_topic();
            write!(f, "CreateTopic({})", v.name())
        }
        MqCommand::TAG_DELETE_TOPIC => write!(f, "DeleteTopic({})", cmd.field_u64(8)),
        MqCommand::TAG_PUBLISH => {
            let v = cmd.as_publish();
            write!(
                f,
                "Publish(topic={}, count={})",
                v.topic_id(),
                v.message_count()
            )
        }
        MqCommand::TAG_COMMIT_OFFSET => {
            write!(
                f,
                "CommitOffset(topic={}, consumer={}, offset={})",
                cmd.field_u64(8),
                cmd.field_u64(16),
                cmd.field_u64(24)
            )
        }
        MqCommand::TAG_PURGE_TOPIC => {
            write!(
                f,
                "PurgeTopic(topic={}, before={})",
                cmd.field_u64(8),
                cmd.field_u64(16)
            )
        }
        MqCommand::TAG_SET_RETAINED | MqCommand::TAG_SET_RETAINED_MQTT => {
            let v = cmd.as_set_retained();
            write!(
                f,
                "SetRetained(exchange={}, rk={})",
                v.exchange_id(),
                v.routing_key()
            )
        }
        MqCommand::TAG_GET_RETAINED => {
            let v = cmd.as_get_retained();
            write!(
                f,
                "GetRetained(exchange={}, filter={:?})",
                v.exchange_id(),
                v.routing_key_filter()
            )
        }
        MqCommand::TAG_DELETE_RETAINED => {
            let v = cmd.as_delete_retained();
            write!(
                f,
                "DeleteRetained(exchange={}, rk={})",
                v.exchange_id(),
                v.routing_key()
            )
        }
        MqCommand::TAG_CREATE_EXCHANGE => {
            let v = cmd.as_create_exchange();
            write!(f, "CreateExchange({})", v.name())
        }
        MqCommand::TAG_DELETE_EXCHANGE => write!(f, "DeleteExchange({})", cmd.field_u64(8)),
        MqCommand::TAG_CREATE_BINDING => {
            let v = cmd.as_create_binding();
            write!(
                f,
                "CreateBinding(exchange={}, topic={})",
                v.exchange_id(),
                v.topic_id()
            )
        }
        MqCommand::TAG_DELETE_BINDING => write!(f, "DeleteBinding({})", cmd.field_u64(8)),
        MqCommand::TAG_PUBLISH_TO_EXCHANGE | MqCommand::TAG_PUBLISH_TO_EXCHANGE_MQTT => {
            let v = cmd.as_publish_to_exchange();
            write!(f, "PublishToExchange(exchange={})", v.exchange_id())
        }
        MqCommand::TAG_CREATE_CONSUMER_GROUP => {
            let v = cmd.as_create_consumer_group();
            write!(f, "CreateConsumerGroup(name={})", v.name())
        }
        MqCommand::TAG_DELETE_CONSUMER_GROUP => {
            write!(f, "DeleteConsumerGroup({})", cmd.field_u64(8))
        }
        MqCommand::TAG_JOIN_CONSUMER_GROUP => {
            let v = cmd.as_join_consumer_group();
            write!(
                f,
                "JoinConsumerGroup(group={}, member={}, client={})",
                v.group_id(),
                v.member_id(),
                v.client_id()
            )
        }
        MqCommand::TAG_SYNC_CONSUMER_GROUP => {
            let v = cmd.as_sync_consumer_group();
            write!(
                f,
                "SyncConsumerGroup(group={}, gen={}, member={})",
                v.group_id(),
                v.generation(),
                v.member_id()
            )
        }
        MqCommand::TAG_LEAVE_CONSUMER_GROUP => {
            let v = cmd.as_leave_consumer_group();
            write!(
                f,
                "LeaveConsumerGroup(group={}, member={})",
                v.group_id(),
                v.member_id()
            )
        }
        MqCommand::TAG_HEARTBEAT_CONSUMER_GROUP => {
            let v = cmd.as_heartbeat_consumer_group();
            write!(
                f,
                "HeartbeatConsumerGroup(group={}, member={}, gen={})",
                v.group_id(),
                v.member_id(),
                v.generation()
            )
        }
        MqCommand::TAG_COMMIT_GROUP_OFFSET => {
            let v = cmd.as_commit_group_offset();
            write!(
                f,
                "CommitGroupOffset(group={}, gen={}, topic={}, part={}, offset={})",
                v.group_id(),
                v.generation(),
                v.topic_id(),
                v.partition_index(),
                v.offset()
            )
        }
        MqCommand::TAG_EXPIRE_GROUP_SESSIONS => {
            write!(f, "ExpireGroupSessions(now={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_DELIVER => {
            write!(
                f,
                "GroupDeliver(group={}, consumer={}, max={})",
                cmd.field_u64(8),
                cmd.field_u64(16),
                cmd.field_u32(24)
            )
        }
        MqCommand::TAG_GROUP_ACK => {
            write!(f, "GroupAck(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_NACK => {
            write!(f, "GroupNack(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_RELEASE => {
            write!(f, "GroupRelease(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_MODIFY => {
            write!(f, "GroupModify(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_EXTEND_VISIBILITY => {
            write!(f, "GroupExtendVisibility(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_TIMEOUT_EXPIRED => {
            write!(f, "GroupTimeoutExpired(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_PUBLISH_TO_DLQ => {
            let v = cmd.as_publish_to_dlq();
            write!(
                f,
                "GroupPublishToDlq(group={}, dlq_topic={})",
                v.source_group_id(),
                v.dlq_topic_id()
            )
        }
        MqCommand::TAG_GROUP_EXPIRE_PENDING => {
            write!(f, "GroupExpirePending(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_PURGE => {
            write!(f, "GroupPurge(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_GET_ATTRIBUTES => {
            write!(f, "GroupGetAttributes(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_DELIVER_ACTOR => {
            write!(
                f,
                "GroupDeliverActor(group={}, consumer={})",
                cmd.field_u64(8),
                cmd.field_u64(16)
            )
        }
        MqCommand::TAG_GROUP_ACK_ACTOR => {
            write!(f, "GroupAckActor(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_NACK_ACTOR => {
            write!(f, "GroupNackActor(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_GROUP_ASSIGN_ACTORS => {
            write!(
                f,
                "GroupAssignActors(group={}, consumer={})",
                cmd.field_u64(8),
                cmd.field_u64(16)
            )
        }
        MqCommand::TAG_GROUP_RELEASE_ACTORS => {
            write!(
                f,
                "GroupReleaseActors(group={}, consumer={})",
                cmd.field_u64(8),
                cmd.field_u64(16)
            )
        }
        MqCommand::TAG_GROUP_EVICT_IDLE => {
            write!(f, "GroupEvictIdle(group={})", cmd.field_u64(8))
        }
        MqCommand::TAG_CRON_ENABLE => {
            write!(f, "CronEnable(topic={})", cmd.field_u64(8))
        }
        MqCommand::TAG_CRON_DISABLE => {
            write!(f, "CronDisable(topic={})", cmd.field_u64(8))
        }
        MqCommand::TAG_CRON_TRIGGER => {
            write!(
                f,
                "CronTrigger(topic={}, at={})",
                cmd.field_u64(8),
                cmd.field_u64(16)
            )
        }
        MqCommand::TAG_CRON_UPDATE => {
            write!(f, "CronUpdate(topic={})", cmd.field_u64(8))
        }
        MqCommand::TAG_CREATE_SESSION => {
            write!(f, "CreateSession(session={})", cmd.field_u64(8))
        }
        MqCommand::TAG_DISCONNECT_SESSION => {
            write!(f, "DisconnectSession(session={})", cmd.field_u64(8))
        }
        MqCommand::TAG_HEARTBEAT_SESSION => {
            write!(f, "HeartbeatSession(session={})", cmd.field_u64(8))
        }
        MqCommand::TAG_SET_WILL => {
            let v = cmd.as_set_will();
            write!(
                f,
                "SetWill(session={}, topic={})",
                v.consumer_id(),
                v.exchange_id()
            )
        }
        MqCommand::TAG_CLEAR_WILL => {
            write!(f, "ClearWill(session={})", cmd.field_u64(8))
        }
        MqCommand::TAG_FIRE_PENDING_WILLS => {
            write!(f, "FirePendingWills(now={})", cmd.field_u64(8))
        }
        MqCommand::TAG_PERSIST_SESSION => {
            let v = cmd.as_persist_session();
            write!(
                f,
                "PersistSession(session={}, client={})",
                v.consumer_id(),
                v.client_id()
            )
        }
        MqCommand::TAG_RESTORE_SESSION => {
            let v = cmd.as_restore_session();
            write!(f, "RestoreSession(client={})", v.client_id())
        }
        MqCommand::TAG_EXPIRE_SESSIONS => {
            write!(f, "ExpireSessions(now={})", cmd.field_u64(8))
        }
        MqCommand::TAG_BATCH => {
            let v = cmd.as_batch();
            write!(f, "Batch(count={})", v.count())
        }
        MqCommand::TAG_PRUNE_DEDUP_WINDOW => {
            write!(
                f,
                "PruneDedupWindow(topic={}, before={})",
                cmd.field_u64(8),
                cmd.field_u64(16)
            )
        }
        _ => write!(f, "MqCommand(tag={})", cmd.tag()),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn publish_roundtrip() {
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(
            &mut buf,
            42,
            &[Bytes::from_static(b"hello"), Bytes::from_static(b"world")],
        );
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_publish();
        assert_eq!(v.topic_id(), 42);
        let msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], Bytes::from_static(b"hello"));
        assert_eq!(msgs[1], Bytes::from_static(b"world"));
    }

    #[test]
    fn create_topic_roundtrip() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(
            &mut buf,
            "my-topic",
            &RetentionPolicy {
                max_age_secs: Some(3600),
                max_bytes: None,
                max_messages: Some(1_000_000),
            },
            8,
        );
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), "my-topic");
        let ret = v.retention();
        assert_eq!(ret.max_age_secs, Some(3600));
        assert_eq!(ret.max_bytes, None);
        assert_eq!(ret.max_messages, Some(1_000_000));
        assert_eq!(v.partition_count(), 8);
    }

    #[test]
    fn batch_roundtrip() {
        let mut buf = BytesMut::new();
        MqCommand::write_delete_topic(&mut buf, 1);
        let c1 = MqCommand::split_from(&mut buf);
        MqCommand::write_publish_bytes(&mut buf, 2, &[Bytes::from_static(b"msg")]);
        let c2 = MqCommand::split_from(&mut buf);
        MqCommand::write_heartbeat_session(&mut buf, 99);
        let c3 = MqCommand::split_from(&mut buf);
        let cmds = vec![c1, c2, c3];
        MqCommand::write_batch(&mut buf, &cmds);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_batch();
        assert_eq!(v.count(), 3);
        let sub_cmds: Vec<&[u8]> = v.commands().collect();
        assert_eq!(sub_cmds.len(), 3);
        assert_eq!(
            crate::types::buf_tag(sub_cmds[0]),
            MqCommand::TAG_DELETE_TOPIC
        );
        assert_eq!(crate::types::buf_tag(sub_cmds[1]), MqCommand::TAG_PUBLISH);
        assert_eq!(
            crate::types::buf_tag(sub_cmds[2]),
            MqCommand::TAG_HEARTBEAT_SESSION
        );
    }

    #[test]
    fn publish_messages_zero_copy() {
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(
            &mut buf,
            42,
            &[Bytes::from_static(b"aaa"), Bytes::from_static(b"bbbbb")],
        );
        let cmd = MqCommand::split_from(&mut buf);

        assert_eq!(cmd.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(cmd.as_publish().topic_id(), 42);

        let msgs: Vec<Bytes> = cmd.publish_messages().unwrap().collect();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], Bytes::from_static(b"aaa"));
        assert_eq!(msgs[1], Bytes::from_static(b"bbbbb"));
    }

    #[test]
    fn publish_messages_for_topic_filter() {
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 7, &[Bytes::from_static(b"x")]);
        let cmd = MqCommand::split_from(&mut buf);
        assert!(cmd.publish_messages_for_topic(7).is_some());
        assert!(cmd.publish_messages_for_topic(8).is_none());
    }

    #[test]
    fn non_publish_returns_none() {
        let mut buf = BytesMut::new();
        MqCommand::write_delete_topic(&mut buf, 1);
        let cmd = MqCommand::split_from(&mut buf);
        assert!(cmd.publish_messages().is_none());
        assert!(cmd.publish_messages_for_topic(1).is_none());
    }

    #[test]
    fn create_exchange_view() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_exchange(&mut buf, "my-exchange", ExchangeType::Topic);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_exchange();
        assert_eq!(v.name(), "my-exchange");
        assert_eq!(v.exchange_type(), ExchangeType::Topic);
    }

    #[test]
    fn create_binding_view() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_binding(&mut buf, 1, 2, Some("routing.key"));
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_binding();
        assert_eq!(v.exchange_id(), 1);
        assert_eq!(v.topic_id(), 2);
        assert_eq!(v.routing_key(), Some("routing.key".to_string()));
    }

    #[test]
    fn display_format() {
        let mut buf = BytesMut::new();

        MqCommand::write_create_topic(&mut buf, "events", &RetentionPolicy::default(), 0);
        let cmd_create = MqCommand::split_from(&mut buf);

        MqCommand::write_delete_topic(&mut buf, 42);
        let cmd_delete = MqCommand::split_from(&mut buf);

        MqCommand::write_publish_bytes(
            &mut buf,
            1,
            &[crate::flat::FlatMessageBuilder::new(b"x").build()],
        );
        let cmd_publish = MqCommand::split_from(&mut buf);

        MqCommand::write_heartbeat_session(&mut buf, 99);
        let cmd_heartbeat = MqCommand::split_from(&mut buf);

        let cases: Vec<(MqCommand, &str)> = vec![
            (cmd_create, "CreateTopic(events)"),
            (cmd_delete, "DeleteTopic(42)"),
            (cmd_publish, "Publish(topic=1, count=1)"),
            (cmd_heartbeat, "HeartbeatSession(session=99)"),
        ];

        for (cmd, expected) in cases {
            assert_eq!(format!("{}", cmd), expected);
        }
    }

    #[test]
    fn consumer_group_command_roundtrips() {
        let mut buf = BytesMut::new();

        MqCommand::write_create_consumer_group(&mut buf, "my-group", 1);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_consumer_group();
        assert_eq!(v.name(), "my-group");
        assert_eq!(v.auto_offset_reset(), 1);

        MqCommand::write_delete_consumer_group(&mut buf, 42);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.field_u64(8), 42);

        MqCommand::write_commit_group_offset(&mut buf, 10, 3, 20, 0, 100, Some("md"), 5000);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_commit_group_offset();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.generation(), 3);
        assert_eq!(v.topic_id(), 20);
        assert_eq!(v.partition_index(), 0);
        assert_eq!(v.offset(), 100);
        assert_eq!(v.metadata(), Some("md"));
        assert_eq!(v.timestamp(), 5000);

        MqCommand::write_commit_group_offset(&mut buf, 10, 3, 20, 0, 100, None, 5000);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_commit_group_offset();
        assert_eq!(v.metadata(), None);
        assert_eq!(v.timestamp(), 5000);

        MqCommand::write_join_consumer_group(
            &mut buf,
            10,
            "member-1",
            "client-1",
            30_000,
            60_000,
            "consumer",
            &[("range", b"\x01\x02"), ("roundrobin", b"\x03")],
        );
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_join_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");
        assert_eq!(v.client_id(), "client-1");
        assert_eq!(v.session_timeout_ms(), 30_000);
        assert_eq!(v.rebalance_timeout_ms(), 60_000);
        assert_eq!(v.protocol_type(), "consumer");
        let protocols = v.protocols();
        assert_eq!(protocols[0].0, "range");
        assert_eq!(protocols[1].0, "roundrobin");

        MqCommand::write_sync_consumer_group(
            &mut buf,
            10,
            5,
            "member-1",
            &[("member-1", b"assign-1"), ("member-2", b"assign-2")],
        );
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_sync_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.generation(), 5);
        assert_eq!(v.member_id(), "member-1");
        let assignments = v.assignments();
        assert_eq!(assignments.len(), 2);

        MqCommand::write_leave_consumer_group(&mut buf, 10, "member-1");
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_leave_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");

        MqCommand::write_heartbeat_consumer_group(&mut buf, 10, "member-1", 7);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_heartbeat_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");
        assert_eq!(v.generation(), 7);
    }

    #[test]
    fn group_ack_variant_roundtrips() {
        let mut buf = BytesMut::new();

        MqCommand::write_group_deliver(&mut buf, 5, 10, 100);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_DELIVER);
        assert_eq!(cmd.field_u64(8), 5);
        assert_eq!(cmd.field_u64(16), 10);
        assert_eq!(cmd.field_u32(24), 100);

        MqCommand::write_group_ack(&mut buf, 5, &[1, 2, 3], None);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_ACK);
        assert_eq!(cmd.field_u64(8), 5);
        let ids = cmd.field_vec_u64(16);
        assert_eq!(&*ids, &[1, 2, 3]);

        MqCommand::write_group_nack(&mut buf, 7, &[10, 20]);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_NACK);

        MqCommand::write_group_extend_visibility(&mut buf, 3, &[1], 5000);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_EXTEND_VISIBILITY);

        MqCommand::write_group_publish_to_dlq(&mut buf, 1, 2, &[10], &[b"dead".as_ref()]);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_PUBLISH_TO_DLQ);
        let v = cmd.as_publish_to_dlq();
        assert_eq!(v.source_group_id(), 1);
        assert_eq!(v.dlq_topic_id(), 2);
    }

    #[test]
    fn session_command_roundtrips() {
        let mut buf = BytesMut::new();

        MqCommand::write_create_session(&mut buf, 1, "client-1", 30000, 3600000);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_CREATE_SESSION);
        assert_eq!(cmd.field_u64(8), 1);

        MqCommand::write_disconnect_session(&mut buf, 1, true);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_DISCONNECT_SESSION);
        assert_eq!(cmd.field_u64(8), 1);

        MqCommand::write_heartbeat_session(&mut buf, 1);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_HEARTBEAT_SESSION);
        assert_eq!(cmd.field_u64(8), 1);
    }

    #[test]
    fn cron_command_roundtrips() {
        let mut buf = BytesMut::new();

        MqCommand::write_cron_enable(&mut buf, 5);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_ENABLE);
        assert_eq!(cmd.field_u64(8), 5);

        MqCommand::write_cron_disable(&mut buf, 5);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_DISABLE);

        MqCommand::write_cron_trigger(&mut buf, 5, 12345);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_TRIGGER);
        assert_eq!(cmd.field_u64(8), 5);
        assert_eq!(cmd.field_u64(16), 12345);
    }

    // =========================================================================
    // Wire format v2 comprehensive tests
    // =========================================================================

    #[test]
    fn v2_header_layout() {
        let mut buf = BytesMut::new();
        MqCommand::write_delete_topic(&mut buf, 42);
        let cmd = MqCommand::split_from(&mut buf);
        let buf = cmd.as_bytes();
        // [size:4][fixed:2][tag:1][flags:1]
        let size = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(size as usize, buf.len());
        let fixed = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        assert!(fixed >= 8);
        assert_eq!(fixed as usize % 8, 0);
        assert_eq!(buf[6], MqCommand::TAG_DELETE_TOPIC);
        assert_eq!(buf[7], 0); // no flags
    }

    #[test]
    fn v2_header_flags() {
        let mut buf = BytesMut::new();
        MqCommand::write_disconnect_session(&mut buf, 1, true);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.flags(), 1);
        MqCommand::write_disconnect_session(&mut buf, 1, false);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.flags(), 0);
    }

    #[test]
    fn v2_flex8_inline_small() {
        // Strings ≤ 7 bytes are stored inline
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "abc", &RetentionPolicy::default(), 1);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), "abc");
        // Verify it's inline: bit 0 = 0, len = byte >> 1
        assert_eq!(cmd.as_bytes()[8] & 1, 0);
        assert_eq!(cmd.as_bytes()[8] >> 1, 3);
    }

    #[test]
    fn v2_flex8_large_string() {
        // Strings > 7 bytes go to flex region
        let long_name = "this-is-a-very-long-topic-name";
        assert!(long_name.len() > 7);
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, long_name, &RetentionPolicy::default(), 1);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), long_name);
        // Verify it's in flex: bit 0 = 1
        assert_eq!(cmd.as_bytes()[8] & 1, 1);
    }

    #[test]
    fn v2_flex8_exactly_7_bytes() {
        let name = "1234567"; // exactly 7 bytes
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, name, &RetentionPolicy::default(), 1);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), name);
        // Should be inline: bit 0 = 0, len = 7
        assert_eq!(cmd.as_bytes()[8] & 1, 0);
        assert_eq!(cmd.as_bytes()[8] >> 1, 7);
    }

    #[test]
    fn v2_flex8_8_bytes_goes_to_flex() {
        let name = "12345678"; // 8 bytes — too large for inline
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, name, &RetentionPolicy::default(), 1);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), name);
        // Should be in flex region: bit 0 = 1
        assert_eq!(cmd.as_bytes()[8] & 1, 1);
    }

    #[test]
    fn v2_flex8_empty() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "", &RetentionPolicy::default(), 1);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), "");
    }

    #[test]
    fn v2_vec_bytes_empty() {
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &[]);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_publish();
        assert_eq!(v.topic_id(), 1);
        assert_eq!(v.message_count(), 0);
        let msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(msgs.len(), 0);
    }

    #[test]
    fn v2_vec_bytes_single() {
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &[Bytes::from_static(b"single")]);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_publish();
        assert_eq!(v.message_count(), 1);
        let msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"single"));
    }

    #[test]
    fn v2_vec_bytes_many() {
        let messages: Vec<Bytes> = (0..100)
            .map(|i| Bytes::from(format!("message-{i}")))
            .collect();
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 42, &messages);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_publish();
        assert_eq!(v.topic_id(), 42);
        assert_eq!(v.message_count(), 100);
        let decoded: Vec<Bytes> = v.messages().collect();
        assert_eq!(decoded.len(), 100);
        for (i, msg) in decoded.iter().enumerate() {
            assert_eq!(*msg, Bytes::from(format!("message-{i}")));
        }
    }

    #[test]
    fn v2_vec_bytes_random_access() {
        let messages = vec![
            Bytes::from_static(b"zero"),
            Bytes::from_static(b"one"),
            Bytes::from_static(b"two"),
        ];
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &messages);
        let cmd = MqCommand::split_from(&mut buf);
        // Random access via field_vec_bytes_get
        assert_eq!(cmd.field_vec_bytes_get(16, 0), b"zero");
        assert_eq!(cmd.field_vec_bytes_get(16, 1), b"one");
        assert_eq!(cmd.field_vec_bytes_get(16, 2), b"two");
    }

    #[test]
    fn v2_vec_bytes_iter_remaining() {
        let messages = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ];
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &messages);
        let cmd = MqCommand::split_from(&mut buf);
        let mut iter = cmd.as_publish().messages();
        assert_eq!(iter.remaining(), 3);
        iter.next();
        assert_eq!(iter.remaining(), 2);
        iter.next();
        assert_eq!(iter.remaining(), 1);
        iter.next();
        assert_eq!(iter.remaining(), 0);
        assert!(iter.next().is_none());
    }

    #[test]
    fn v2_vec_u64_roundtrip() {
        let ids = vec![1u64, 2, 3, 100, u64::MAX];
        let mut buf = BytesMut::new();
        MqCommand::write_group_ack(&mut buf, 42, &ids, None);
        let cmd = MqCommand::split_from(&mut buf);
        let decoded = cmd.field_vec_u64(16);
        assert_eq!(&*decoded, &ids);
    }

    #[test]
    fn v2_vec_u64_empty() {
        let mut buf = BytesMut::new();
        MqCommand::write_group_ack(&mut buf, 42, &[], None);
        let cmd = MqCommand::split_from(&mut buf);
        let decoded = cmd.field_vec_u64(16);
        assert!(decoded.is_empty());
    }

    #[test]
    fn v2_blob_roundtrip() {
        let retention = RetentionPolicy {
            max_age_secs: Some(86400),
            max_bytes: Some(1_000_000),
            max_messages: Some(100_000),
        };
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "t", &retention, 4);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        let decoded_ret = v.retention();
        assert_eq!(decoded_ret.max_age_secs, retention.max_age_secs);
        assert_eq!(decoded_ret.max_bytes, retention.max_bytes);
        assert_eq!(decoded_ret.max_messages, retention.max_messages);
    }

    #[test]
    fn v2_blob_default() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "t", &RetentionPolicy::default(), 1);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_topic();
        let ret = v.retention();
        assert_eq!(ret.max_age_secs, None);
        assert_eq!(ret.max_bytes, None);
        assert_eq!(ret.max_messages, None);
    }

    #[test]
    fn v2_batch_self_sized() {
        // Verify batch sub-commands are self-sized (size in first 4 bytes)
        let mut buf = BytesMut::new();
        MqCommand::write_delete_topic(&mut buf, 1);
        let c1 = MqCommand::split_from(&mut buf);
        MqCommand::write_delete_topic(&mut buf, 2);
        let c2 = MqCommand::split_from(&mut buf);
        let cmds = vec![c1, c2];
        MqCommand::write_batch(&mut buf, &cmds);
        let batch = MqCommand::split_from(&mut buf);
        let buf = batch.as_bytes();
        // batch header: [size:4][fixed:2][tag:1][flags:1][count:4][pad:4]
        let count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        assert_eq!(count, 2);
        // sub-commands start at offset 16
        let sub1_size = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
        assert!(sub1_size > 0);
        // First sub-command tag at offset 16+6
        assert_eq!(buf[16 + 6], MqCommand::TAG_DELETE_TOPIC);
    }

    #[test]
    fn v2_batch_alignment() {
        // Sub-commands in batch should be padded to 8-byte boundaries
        let mut buf = BytesMut::new();
        MqCommand::write_delete_topic(&mut buf, 1);
        let c1 = MqCommand::split_from(&mut buf);
        MqCommand::write_heartbeat_session(&mut buf, 2);
        let c2 = MqCommand::split_from(&mut buf);
        MqCommand::write_delete_topic(&mut buf, 3);
        let c3 = MqCommand::split_from(&mut buf);
        let cmds = vec![c1, c2, c3];
        MqCommand::write_batch(&mut buf, &cmds);
        let batch = MqCommand::split_from(&mut buf);
        let v = batch.as_batch();
        let sub_cmds: Vec<&[u8]> = v.commands().collect();
        assert_eq!(sub_cmds.len(), 3);
        assert_eq!(crate::types::buf_field_u64(sub_cmds[0], 8), 1);
        assert_eq!(crate::types::buf_field_u64(sub_cmds[1], 8), 2);
        assert_eq!(crate::types::buf_field_u64(sub_cmds[2], 8), 3);
    }

    #[test]
    fn v2_batch_with_variable_size_subcmds() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "short", &RetentionPolicy::default(), 1);
        let c1 = MqCommand::split_from(&mut buf);
        MqCommand::write_publish_bytes(&mut buf, 1, &[Bytes::from_static(b"hello world data")]);
        let c2 = MqCommand::split_from(&mut buf);
        MqCommand::write_delete_topic(&mut buf, 99);
        let c3 = MqCommand::split_from(&mut buf);
        let cmds = vec![c1, c2, c3];
        MqCommand::write_batch(&mut buf, &cmds);
        let batch = MqCommand::split_from(&mut buf);
        let sub_cmds: Vec<&[u8]> = batch.as_batch().commands().collect();
        assert_eq!(sub_cmds.len(), 3);
        assert_eq!(
            crate::types::buf_tag(sub_cmds[0]),
            MqCommand::TAG_CREATE_TOPIC
        );
        assert_eq!(CmdCreateTopic::from_buf(sub_cmds[0]).name(), "short");
        assert_eq!(crate::types::buf_tag(sub_cmds[1]), MqCommand::TAG_PUBLISH);
        assert_eq!(CmdPublish::from_buf(sub_cmds[1]).topic_id(), 1);
        let msgs: Vec<Bytes> = CmdPublish::from_buf(sub_cmds[1]).messages().collect();
        assert_eq!(msgs[0], Bytes::from_static(b"hello world data"));
        assert_eq!(
            crate::types::buf_tag(sub_cmds[2]),
            MqCommand::TAG_DELETE_TOPIC
        );
        assert_eq!(crate::types::buf_field_u64(sub_cmds[2], 8), 99);
    }

    #[test]
    fn v2_command_builder_size_field() {
        // The size field at offset 0 should equal total encoded size (buf + extra).
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &[Bytes::from_static(b"test")]);
        let cmd = MqCommand::split_from(&mut buf);
        let bytes = cmd.as_bytes();
        let size = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(size as usize, cmd.total_encoded_size());
    }

    #[test]
    fn v2_command_builder_fixed_field() {
        // The fixed field at offset 4 should be a multiple of 8
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "x", &RetentionPolicy::default(), 1);
        let cmd = MqCommand::split_from(&mut buf);
        let bytes = cmd.as_bytes();
        let fixed = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        assert_eq!(fixed as usize % 8, 0);
        assert!(fixed >= 8);
    }

    #[test]
    fn v2_all_simple_entity_commands() {
        // Test all commands that just have @8 entity_id:u64
        let mut buf = BytesMut::new();

        let cases: &[(fn(&mut BytesMut, u64), u8)] = &[
            (MqCommand::write_delete_topic, MqCommand::TAG_DELETE_TOPIC),
            (
                MqCommand::write_delete_exchange,
                MqCommand::TAG_DELETE_EXCHANGE,
            ),
            (
                MqCommand::write_delete_binding,
                MqCommand::TAG_DELETE_BINDING,
            ),
            (
                MqCommand::write_delete_consumer_group,
                MqCommand::TAG_DELETE_CONSUMER_GROUP,
            ),
            (
                MqCommand::write_heartbeat_session,
                MqCommand::TAG_HEARTBEAT_SESSION,
            ),
            (MqCommand::write_cron_enable, MqCommand::TAG_CRON_ENABLE),
            (MqCommand::write_cron_disable, MqCommand::TAG_CRON_DISABLE),
        ];
        for (write_fn, expected_tag) in cases {
            write_fn(&mut buf, 12345);
            let cmd = MqCommand::split_from(&mut buf);
            assert_eq!(cmd.tag(), *expected_tag);
            assert_eq!(cmd.field_u64(8), 12345);
        }
    }

    #[test]
    fn v2_create_session_full() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_session(&mut buf, 99, "my-client-id", 30_000, 3_600_000);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_CREATE_SESSION);
        assert_eq!(cmd.field_u64(8), 99);
        assert_eq!(cmd.field_u64(16), 30_000);
        assert_eq!(cmd.field_u64(24), 3_600_000);
        assert_eq!(cmd.field_flex8_str(32), "my-client-id");
    }

    #[test]
    fn v2_exchange_roundtrip() {
        let mut buf = BytesMut::new();
        for (name, etype) in [
            ("direct-ex", ExchangeType::Direct),
            ("fanout-ex", ExchangeType::Fanout),
            ("topic-ex", ExchangeType::Topic),
        ] {
            MqCommand::write_create_exchange(&mut buf, name, etype);
            let cmd = MqCommand::split_from(&mut buf);
            let v = cmd.as_create_exchange();
            assert_eq!(v.name(), name);
            assert_eq!(v.exchange_type(), etype);
        }
    }

    #[test]
    fn v2_binding_with_all_opts() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_binding_with_opts(
            &mut buf,
            10,
            20,
            Some("my.routing.key.pattern"),
            true,
            Some("shared-group-name"),
            Some(42),
        );
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_binding();
        assert_eq!(v.exchange_id(), 10);
        assert_eq!(v.topic_id(), 20);
        assert_eq!(v.routing_key(), Some("my.routing.key.pattern".to_string()));
        assert!(v.no_local());
        assert_eq!(v.shared_group(), Some("shared-group-name".to_string()));
        assert_eq!(v.subscription_id(), Some(42));
    }

    #[test]
    fn v2_binding_minimal() {
        let mut buf = BytesMut::new();
        MqCommand::write_create_binding(&mut buf, 1, 2, None);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_create_binding();
        assert_eq!(v.exchange_id(), 1);
        assert_eq!(v.topic_id(), 2);
        assert_eq!(v.routing_key(), None);
        assert!(!v.no_local());
        assert_eq!(v.shared_group(), None);
        assert_eq!(v.subscription_id(), None);
    }

    #[test]
    fn v2_publish_to_exchange_roundtrip() {
        let msgs = vec![Bytes::from_static(b"msg-a"), Bytes::from_static(b"msg-b")];
        let mut buf = BytesMut::new();
        MqCommand::write_publish_to_exchange_bytes(&mut buf, 55, &msgs);
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_publish_to_exchange();
        assert_eq!(v.exchange_id(), 55);
        let decoded: Vec<Bytes> = v.messages().collect();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], Bytes::from_static(b"msg-a"));
        assert_eq!(decoded[1], Bytes::from_static(b"msg-b"));
    }

    #[test]
    fn v2_group_deliver_roundtrip() {
        let mut buf = BytesMut::new();
        MqCommand::write_group_deliver(&mut buf, 100, 200, 50);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.field_u64(8), 100);
        assert_eq!(cmd.field_u64(16), 200);
        assert_eq!(cmd.field_u32(24), 50);
    }

    #[test]
    fn v2_group_extend_visibility_roundtrip() {
        let mut buf = BytesMut::new();
        MqCommand::write_group_extend_visibility(&mut buf, 7, &[10, 20, 30], 60000);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_EXTEND_VISIBILITY);
        assert_eq!(cmd.field_u64(8), 7);
        let ids = cmd.field_vec_u64(16);
        assert_eq!(&*ids, &[10, 20, 30]);
        assert_eq!(cmd.field_u64(24), 60000);
    }

    #[test]
    fn v2_publish_to_dlq_roundtrip() {
        let mut buf = BytesMut::new();
        MqCommand::write_group_publish_to_dlq(
            &mut buf,
            10,
            20,
            &[100, 200],
            &[b"dead-1".as_ref(), b"dead-2".as_ref()],
        );
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_publish_to_dlq();
        assert_eq!(v.source_group_id(), 10);
        assert_eq!(v.dlq_topic_id(), 20);
        let dl_ids = v.dead_letter_ids();
        assert_eq!(&*dl_ids, &[100, 200]);
        let bodies: Vec<Bytes> = v.messages().collect();
        assert_eq!(bodies.len(), 2);
        assert_eq!(&*bodies[0], b"dead-1");
        assert_eq!(&*bodies[1], b"dead-2");
    }

    #[test]
    fn v2_serde_roundtrip() {
        // MqCommand implements Serialize/Deserialize for Raft AppData
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 42, &[Bytes::from_static(b"serde-test")]);
        let cmd = MqCommand::split_from(&mut buf);
        let serialized = serde_json::to_vec(&cmd).unwrap();
        let deserialized: MqCommand = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(deserialized.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(deserialized.field_u64(8), 42);
        let msgs: Vec<Bytes> = deserialized.as_publish().messages().collect();
        assert_eq!(msgs[0], Bytes::from_static(b"serde-test"));
    }

    #[test]
    fn v2_encode_decode_roundtrip() {
        // Test Encode/Decode traits (for Raft)
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "raft-test", &RetentionPolicy::default(), 4);
        let cmd = MqCommand::split_from(&mut buf);
        let encoded = cmd.encode_to_vec().unwrap();
        let decoded = MqCommand::decode_from_slice(&encoded).unwrap();
        assert_eq!(decoded.tag(), MqCommand::TAG_CREATE_TOPIC);
        assert_eq!(decoded.as_create_topic().name(), "raft-test");
        assert_eq!(decoded.as_create_topic().partition_count(), 4);
    }

    #[test]
    fn v2_vec_kv_join_consumer_group() {
        let mut buf = BytesMut::new();
        MqCommand::write_join_consumer_group(
            &mut buf,
            1,
            "m1",
            "c1",
            10000,
            20000,
            "consumer",
            &[("range", b"\x01"), ("sticky", b"\x02\x03")],
        );
        let cmd = MqCommand::split_from(&mut buf);
        let v = cmd.as_join_consumer_group();
        assert_eq!(v.group_id(), 1);
        assert_eq!(v.member_id(), "m1");
        assert_eq!(v.client_id(), "c1");
        assert_eq!(v.session_timeout_ms(), 10000);
        assert_eq!(v.rebalance_timeout_ms(), 20000);
        assert_eq!(v.protocol_type(), "consumer");
        let protocols = v.protocols();
        assert_eq!(protocols.len(), 2);
        assert_eq!(protocols[0].0, "range");
        assert_eq!(&*protocols[0].1, &[0x01]);
        assert_eq!(protocols[1].0, "sticky");
        assert_eq!(&*protocols[1].1, &[0x02, 0x03]);
    }

    #[test]
    fn v2_large_batch() {
        // Test batch with many sub-commands
        let mut buf = BytesMut::new();
        let cmds: Vec<MqCommand> = (0..50)
            .map(|i| {
                MqCommand::write_delete_topic(&mut buf, i);
                MqCommand::split_from(&mut buf)
            })
            .collect();
        MqCommand::write_batch(&mut buf, &cmds);
        let batch = MqCommand::split_from(&mut buf);
        let v = batch.as_batch();
        assert_eq!(v.count(), 50);
        let sub_cmds: Vec<&[u8]> = v.commands().collect();
        assert_eq!(sub_cmds.len(), 50);
        for (i, sub) in sub_cmds.iter().enumerate() {
            assert_eq!(crate::types::buf_tag(sub), MqCommand::TAG_DELETE_TOPIC);
            assert_eq!(crate::types::buf_field_u64(sub, 8), i as u64);
        }
    }

    #[test]
    fn publish_scatter_wire_equivalence() {
        let msgs: Vec<Bytes> = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b"world"),
            Bytes::from_static(b"foo bar baz"),
        ];

        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 42, &msgs);
        let cmd1 = MqCommand::split_from(&mut buf);
        MqCommand::write_publish_bytes(&mut buf, 42, &msgs);
        let cmd2 = MqCommand::split_from(&mut buf);

        // Encoded sizes must match
        assert_eq!(cmd1.encoded_size(), cmd2.encoded_size());

        // Encoded bytes must be identical
        let b1 = cmd1.encode_to_vec().unwrap();
        let b2 = cmd2.encode_to_vec().unwrap();
        assert_eq!(b1, b2);

        // Decode the encoded bytes and verify field access works
        let decoded = MqCommand::decode_from_bytes(Bytes::from(b2)).unwrap();
        let v = decoded.as_publish();
        assert_eq!(v.topic_id(), 42);
        let decoded_msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(decoded_msgs.len(), 3);
        assert_eq!(decoded_msgs[0], Bytes::from_static(b"hello"));
        assert_eq!(decoded_msgs[1], Bytes::from_static(b"world"));
        assert_eq!(decoded_msgs[2], Bytes::from_static(b"foo bar baz"));
    }

    #[test]
    fn publish_scatter_empty() {
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 99, &[]);
        let cmd1 = MqCommand::split_from(&mut buf);
        MqCommand::write_publish_bytes(&mut buf, 99, &[]);
        let cmd2 = MqCommand::split_from(&mut buf);
        assert_eq!(cmd1.encode_to_vec().unwrap(), cmd2.encode_to_vec().unwrap());
    }

    #[test]
    fn batch_with_scatter_subcommands() {
        let msgs: Vec<Bytes> = vec![Bytes::from_static(b"aaa"), Bytes::from_static(b"bbb")];

        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &msgs);
        let pub1_cmd = MqCommand::split_from(&mut buf);
        MqCommand::write_publish_bytes(&mut buf, 2, &msgs);
        let pub2_cmd = MqCommand::split_from(&mut buf);
        MqCommand::write_delete_topic(&mut buf, 3);
        let delete = MqCommand::split_from(&mut buf);

        MqCommand::write_batch(&mut buf, &[pub1_cmd, pub2_cmd, delete]);
        let batch = MqCommand::split_from(&mut buf);
        let v = batch.as_batch();
        assert_eq!(v.count(), 3);

        let sub_cmds: Vec<&[u8]> = v.commands().collect();
        let pub1 = CmdPublish::from_buf(sub_cmds[0]);
        assert_eq!(pub1.topic_id(), 1);
        let m1: Vec<Bytes> = pub1.messages().collect();
        assert_eq!(m1, msgs);

        let pub2 = CmdPublish::from_buf(sub_cmds[1]);
        assert_eq!(pub2.topic_id(), 2);
        let m2: Vec<Bytes> = pub2.messages().collect();
        assert_eq!(m2, msgs);

        assert_eq!(
            crate::types::buf_tag(sub_cmds[2]),
            MqCommand::TAG_DELETE_TOPIC
        );
        assert_eq!(crate::types::buf_field_u64(sub_cmds[2], 8), 3);
    }

    #[test]
    fn collect_publish_messages_equivalence() {
        let msgs: Vec<Bytes> = vec![Bytes::from_static(b"x"), Bytes::from_static(b"yy")];
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &msgs);
        let cmd = MqCommand::split_from(&mut buf);
        let collected = cmd.collect_publish_messages();
        assert_eq!(collected, msgs);

        MqCommand::write_publish_bytes(&mut buf, 1, &msgs);
        let cmd = MqCommand::split_from(&mut buf);
        let collected = cmd.collect_publish_messages();
        assert_eq!(collected, msgs);
    }
}
