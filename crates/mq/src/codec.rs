//! Flat binary codec for MqCommand and MqResponse.
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
use bytes::Bytes;
use smallvec::SmallVec;

use crate::types::*;

// =============================================================================
// MqCommand tag aliases (delegate to MqCommand::TAG_*)
// =============================================================================

// =============================================================================
// MqResponse tag constants
// =============================================================================

const TAG_RESP_OK: u8 = 0;
const TAG_RESP_ERROR: u8 = 1;
const TAG_RESP_ENTITY_CREATED: u8 = 2;
const TAG_RESP_MESSAGES: u8 = 3;
const TAG_RESP_PUBLISHED: u8 = 4;
const TAG_RESP_STATS: u8 = 5;
const TAG_RESP_BATCH: u8 = 6;
const TAG_RESP_DEAD_LETTERED: u8 = 7;
const TAG_RESP_GROUP_JOINED: u8 = 8;
const TAG_RESP_GROUP_SYNCED: u8 = 9;
const TAG_RESP_RETAINED_MESSAGES: u8 = 10;
const TAG_RESP_WILL_PENDING: u8 = 11;
const TAG_RESP_SESSION_RESTORED: u8 = 12;
const TAG_RESP_SESSION_NOT_FOUND: u8 = 13;
const TAG_RESP_MULTI_MESSAGES: u8 = 14;
const TAG_RESP_TOPIC_ALIASES: u8 = 15;
const TAG_RESP_WILLS_FIRED: u8 = 16;
const TAG_RESP_COMMITTED: u8 = 17;

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
// CommandBuilder — fixed/flex binary command construction
// =============================================================================

/// Builder for MqCommand with fixed/flex layout.
///
/// The fixed region has compile-time-known field offsets (all 8-byte aligned).
/// The flex region holds variable-length data (strings, byte arrays, vectors).
///
/// Usage:
/// ```ignore
/// let cmd = CommandBuilder::new(TAG, 0, 24)  // 24-byte fixed region
///     .set_u64(8, topic_id)
///     .set_vec_bytes(16, &messages)
///     .finish();
/// ```
pub(crate) struct CommandBuilder {
    buf: SmallVec<[u8; 128]>,
}

impl CommandBuilder {
    /// Create a new builder with the given tag, flags, and fixed region size.
    /// `fixed_size` must be a multiple of 8 and >= 8 (header size).
    ///
    /// For fixed-only commands (≤128 bytes total), the buffer stays entirely
    /// on the stack via `SmallVec` — zero heap allocations until `finish()`.
    #[inline]
    pub fn new(tag: u8, flags: u8, fixed_size: usize) -> Self {
        debug_assert!(fixed_size >= 8 && fixed_size % 8 == 0);
        let mut buf = SmallVec::new();
        buf.resize(fixed_size, 0);
        // Header: [size:4][fixed:2][tag:1][flags:1]
        // size is patched in finish()
        buf[4..6].copy_from_slice(&(fixed_size as u16).to_le_bytes());
        buf[6] = tag;
        buf[7] = flags;
        Self { buf }
    }

    /// Create a new builder pre-sized for `total_capacity` bytes.
    ///
    /// Use this when the total size (fixed + flex) is known or estimable
    /// upfront.  Avoids reallocation when flex data is appended — one
    /// heap allocation total instead of two.
    #[inline]
    pub fn with_capacity(tag: u8, flags: u8, fixed_size: usize, total_capacity: usize) -> Self {
        debug_assert!(fixed_size >= 8 && fixed_size % 8 == 0);
        let mut buf = SmallVec::with_capacity(total_capacity);
        buf.resize(fixed_size, 0);
        buf[4..6].copy_from_slice(&(fixed_size as u16).to_le_bytes());
        buf[6] = tag;
        buf[7] = flags;
        Self { buf }
    }

    #[inline]
    pub fn set_u64(mut self, offset: usize, value: u64) -> Self {
        self.buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        self
    }

    #[inline]
    pub fn set_u32(mut self, offset: usize, value: u32) -> Self {
        self.buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        self
    }

    #[inline]
    pub fn set_i32(mut self, offset: usize, value: i32) -> Self {
        self.buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        self
    }

    #[inline]
    pub fn set_u8(mut self, offset: usize, value: u8) -> Self {
        self.buf[offset] = value;
        self
    }

    /// Set a flex8 string slot. Small strings (≤7 bytes) are inlined.
    pub fn set_str(mut self, offset: usize, s: &str) -> Self {
        self.write_flex8(offset, s.as_bytes());
        self
    }

    /// Set a flex8 bytes slot. Small values (≤7 bytes) are inlined.
    pub fn set_bytes(mut self, offset: usize, b: &[u8]) -> Self {
        self.write_flex8(offset, b);
        self
    }

    /// Set a flex8 bytes slot from a Bytes value.
    pub fn set_bytes_val(mut self, offset: usize, b: &Bytes) -> Self {
        self.write_flex8(offset, b);
        self
    }

    /// Set an optional flex8 string slot. None → all zeros (length 0).
    pub fn set_opt_str(self, offset: usize, s: Option<&str>) -> Self {
        match s {
            None => self, // zeros already
            Some(s) => self.set_str(offset, s),
        }
    }

    /// Set an optional flex8 bytes slot.
    pub fn set_opt_bytes(self, offset: usize, b: Option<&Bytes>) -> Self {
        match b {
            None => self,
            Some(b) => self.set_bytes(offset, b),
        }
    }

    /// Set a vec_u64 slot: `[count:4][offset:4]` in fixed, u64 data in flex.
    /// Ensures the u64 data in flex is 8-byte aligned.
    pub fn set_vec_u64(mut self, offset: usize, values: &[u64]) -> Self {
        self.buf[offset..offset + 4].copy_from_slice(&(values.len() as u32).to_le_bytes());
        if values.is_empty() {
            return self;
        }
        // Pad to 8-byte alignment
        let pad = (8 - (self.buf.len() % 8)) % 8;
        // Reserve once for pad + all u64 data
        self.buf.reserve(pad + values.len() * 8);
        self.buf.extend_from_slice(&[0u8; 8][..pad]);
        let data_offset = self.buf.len();
        self.buf[offset + 4..offset + 8].copy_from_slice(&(data_offset as u32).to_le_bytes());
        for &v in values {
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
        self
    }

    /// Set a vec_bytes slot with descriptor table for O(1) random access.
    ///
    /// Fixed: `[count:4][offset:4]`
    /// Flex: `[size0:4][offset0:4][size1:4][offset1:4]...][data0][data1]...`
    pub fn set_vec_bytes(mut self, offset: usize, values: &[Bytes]) -> Self {
        self.buf[offset..offset + 4].copy_from_slice(&(values.len() as u32).to_le_bytes());
        if values.is_empty() {
            return self;
        }
        let table_offset = self.buf.len();
        self.buf[offset + 4..offset + 8].copy_from_slice(&(table_offset as u32).to_le_bytes());
        // Pre-calculate total size: descriptor table + all data
        let table_size = values.len() * 8;
        let data_size: usize = values.iter().map(|v| v.len()).sum();
        self.buf.reserve(table_size + data_size);
        self.buf.resize(self.buf.len() + table_size, 0);
        // Write data and fill descriptors
        for (i, v) in values.iter().enumerate() {
            let data_offset = self.buf.len();
            self.buf.extend_from_slice(v);
            let desc_off = table_offset + i * 8;
            self.buf[desc_off..desc_off + 4].copy_from_slice(&(v.len() as u32).to_le_bytes());
            self.buf[desc_off + 4..desc_off + 8]
                .copy_from_slice(&(data_offset as u32).to_le_bytes());
        }
        self
    }

    /// Set a vec_bytes slot from borrowed slices. Identical wire format to
    /// `set_vec_bytes` but accepts `&[&[u8]]` — no `Bytes` allocation needed.
    pub fn set_vec_slices(mut self, offset: usize, values: &[&[u8]]) -> Self {
        self.buf[offset..offset + 4].copy_from_slice(&(values.len() as u32).to_le_bytes());
        if values.is_empty() {
            return self;
        }
        let table_offset = self.buf.len();
        self.buf[offset + 4..offset + 8].copy_from_slice(&(table_offset as u32).to_le_bytes());
        // Pre-calculate total size: descriptor table + all data
        let table_size = values.len() * 8;
        let data_size: usize = values.iter().map(|v| v.len()).sum();
        self.buf.reserve(table_size + data_size);
        self.buf.resize(self.buf.len() + table_size, 0);
        // Write data and fill descriptors
        for (i, v) in values.iter().enumerate() {
            let data_offset = self.buf.len();
            self.buf.extend_from_slice(v);
            let desc_off = table_offset + i * 8;
            self.buf[desc_off..desc_off + 4].copy_from_slice(&(v.len() as u32).to_le_bytes());
            self.buf[desc_off + 4..desc_off + 8]
                .copy_from_slice(&(data_offset as u32).to_le_bytes());
        }
        self
    }

    /// Set a blob slot by encoding a value using the Encode trait.
    pub fn set_blob_encode<T: Encode>(mut self, offset: usize, value: &T) -> Self {
        let data_offset = self.buf.len();
        value.encode(&mut self.buf).unwrap();
        let data_size = self.buf.len() - data_offset;
        self.buf[offset..offset + 4].copy_from_slice(&(data_offset as u32).to_le_bytes());
        self.buf[offset + 4..offset + 8].copy_from_slice(&(data_size as u32).to_le_bytes());
        self
    }

    /// Set a vec_kv slot: key-value pairs with flex8 descriptors.
    /// Each entry is 16 bytes of descriptors: `[key:flex8][val:flex8]`.
    pub fn set_vec_kv(mut self, offset: usize, pairs: &[(&str, &[u8])]) -> Self {
        self.buf[offset..offset + 4].copy_from_slice(&(pairs.len() as u32).to_le_bytes());
        if pairs.is_empty() {
            return self;
        }
        let table_offset = self.buf.len();
        self.buf[offset + 4..offset + 8].copy_from_slice(&(table_offset as u32).to_le_bytes());
        // Reserve descriptor table: 16 bytes per entry (2 × flex8)
        let table_size = pairs.len() * 16;
        self.buf.resize(self.buf.len() + table_size, 0);
        // Write data and fill descriptors
        for (i, (key, val)) in pairs.iter().enumerate() {
            let key_desc = table_offset + i * 16;
            let val_desc = key_desc + 8;
            self.write_flex8(key_desc, key.as_bytes());
            self.write_flex8(val_desc, val);
        }
        self
    }

    /// Internal: write a flex8 small/large value at the given fixed-region offset.
    ///
    /// Bit-0 tag encoding (all LE):
    /// - Small (≤7): `[(len << 1):u8][data:7]`     — bit 0 = 0
    /// - Large (>7): `[(offset << 1 | 1):u32_le][size:u32_le]` — bit 0 = 1, 31-bit offset
    fn write_flex8(&mut self, offset: usize, data: &[u8]) {
        if data.len() <= 7 {
            // Small inline: bit 0 = 0, len in bits 1..7
            self.buf[offset] = (data.len() as u8) << 1;
            self.buf[offset + 1..offset + 1 + data.len()].copy_from_slice(data);
        } else {
            // Large: bit 0 = 1, 31-bit offset via shift
            let data_offset = self.buf.len();
            self.buf.extend_from_slice(data);
            let encoded = ((data_offset as u32) << 1) | 1;
            self.buf[offset..offset + 4].copy_from_slice(&encoded.to_le_bytes());
            self.buf[offset + 4..offset + 8].copy_from_slice(&(data.len() as u32).to_le_bytes());
        }
    }

    /// Finalize the command: patch the size header and return MqCommand.
    #[inline]
    pub fn finish(mut self) -> MqCommand {
        let total_size = self.buf.len();
        self.buf[0..4].copy_from_slice(&(total_size as u32).to_le_bytes());
        MqCommand {
            buf: Bytes::from(self.buf.into_vec()),
            extra: SmallVec::new(),
        }
    }

    /// Begin building a `vec_bytes` field by writing the count and reserving
    /// the descriptor table. Returns a [`VecBytesWriter`] that lets callers
    /// write entries directly into the command buffer — zero intermediate copies.
    #[inline]
    pub fn begin_vec_bytes(mut self, offset: usize, count: usize) -> VecBytesWriter {
        self.buf[offset..offset + 4].copy_from_slice(&(count as u32).to_le_bytes());
        if count == 0 {
            return VecBytesWriter {
                builder: self,
                table_start: 0,
                count: 0,
                index: 0,
            };
        }
        let table_offset = self.buf.len();
        self.buf[offset + 4..offset + 8].copy_from_slice(&(table_offset as u32).to_le_bytes());
        let table_size = count * 8;
        self.buf.resize(self.buf.len() + table_size, 0);
        VecBytesWriter {
            builder: self,
            table_start: table_offset,
            count,
            index: 0,
        }
    }
}

/// Zero-copy writer for building `vec_bytes` entries directly inside a
/// [`CommandBuilder`]'s buffer. Each entry is written in-place — no
/// intermediate `BytesMut` or `Vec<Bytes>` needed.
///
/// Usage:
/// ```ignore
/// let mut w = CommandBuilder::new(tag, 0, 24)
///     .set_u64(8, exchange_id)
///     .begin_vec_bytes(16, batch_size);
/// for msg in &messages {
///     let start = w.position();
///     w.buf_mut().extend_from_slice(msg);
///     w.commit(start);
/// }
/// let cmd = w.finish();
/// ```
pub struct VecBytesWriter {
    builder: CommandBuilder,
    table_start: usize,
    count: usize,
    index: usize,
}

impl VecBytesWriter {
    /// Append one entry's bytes directly via a closure that writes into
    /// the command's internal `Vec<u8>`. The descriptor is filled automatically.
    ///
    /// For publish batches, use with [`write_flat_message_vec`] or
    /// [`MqttEnvelopeWriter::write_vec`] to write messages with zero
    /// intermediate copies.
    #[inline]
    pub fn push<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Vec<u8>),
    {
        debug_assert!(self.index < self.count);
        // For batch commands the SmallVec is always heap-spilled (>128 bytes),
        // so into_vec()/from_vec() are O(1) pointer moves — no copy.
        let mut vec = std::mem::take(&mut self.builder.buf).into_vec();
        let data_offset = vec.len();
        f(&mut vec);
        let len = vec.len() - data_offset;
        self.builder.buf = SmallVec::from_vec(vec);

        let desc_off = self.table_start + self.index * 8;
        self.builder.buf[desc_off..desc_off + 4].copy_from_slice(&(len as u32).to_le_bytes());
        self.builder.buf[desc_off + 4..desc_off + 8]
            .copy_from_slice(&(data_offset as u32).to_le_bytes());
        self.index += 1;
    }

    /// Append a pre-built entry (convenience for `push(|v| v.extend_from_slice(data))`).
    #[inline]
    pub fn push_slice(&mut self, data: &[u8]) {
        debug_assert!(self.index < self.count);
        let data_offset = self.builder.buf.len();
        self.builder.buf.extend_from_slice(data);
        let len = data.len();
        let desc_off = self.table_start + self.index * 8;
        self.builder.buf[desc_off..desc_off + 4].copy_from_slice(&(len as u32).to_le_bytes());
        self.builder.buf[desc_off + 4..desc_off + 8]
            .copy_from_slice(&(data_offset as u32).to_le_bytes());
        self.index += 1;
    }

    /// Finalize the command. Panics in debug mode if not all entries were written.
    #[inline]
    pub fn finish(self) -> MqCommand {
        debug_assert_eq!(self.index, self.count);
        self.builder.finish()
    }
}

// =============================================================================
// CommandWriter — zero-copy command builder that writes directly into &mut Vec<u8>
// =============================================================================

/// Builds a command directly into an external `Vec<u8>`. Unlike `CommandBuilder`
/// which uses an internal `SmallVec` then copies out, `CommandWriter` writes
/// every byte exactly once into the target buffer.
pub(crate) struct CommandWriter<'a> {
    buf: &'a mut Vec<u8>,
    /// Byte offset in `buf` where this command starts.
    cmd_start: usize,
}

#[allow(dead_code)]
impl<'a> CommandWriter<'a> {
    /// Begin a new command. Reserves `fixed_size` zeroed bytes in `buf`.
    #[inline]
    fn new(buf: &'a mut Vec<u8>, tag: u8, flags: u8, fixed_size: usize) -> Self {
        debug_assert!(fixed_size >= 8 && fixed_size % 8 == 0);
        let cmd_start = buf.len();
        buf.resize(cmd_start + fixed_size, 0);
        // Header: [size:4][fixed:2][tag:1][flags:1]
        buf[cmd_start + 4..cmd_start + 6].copy_from_slice(&(fixed_size as u16).to_le_bytes());
        buf[cmd_start + 6] = tag;
        buf[cmd_start + 7] = flags;
        Self { buf, cmd_start }
    }

    /// Begin a new command with pre-reserved capacity for the total size.
    #[inline]
    fn with_capacity(
        buf: &'a mut Vec<u8>,
        tag: u8,
        flags: u8,
        fixed_size: usize,
        total_capacity: usize,
    ) -> Self {
        debug_assert!(fixed_size >= 8 && fixed_size % 8 == 0);
        buf.reserve(total_capacity);
        Self::new(buf, tag, flags, fixed_size)
    }

    #[inline]
    fn set_u64(&mut self, offset: usize, value: u64) {
        let off = self.cmd_start + offset;
        self.buf[off..off + 8].copy_from_slice(&value.to_le_bytes());
    }

    #[inline]
    fn set_u32(&mut self, offset: usize, value: u32) {
        let off = self.cmd_start + offset;
        self.buf[off..off + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Write a flex8 field: small (≤7 bytes) inlined, large appended to flex region.
    #[inline]
    fn set_flex8(&mut self, offset: usize, data: &[u8]) {
        let off = self.cmd_start + offset;
        if data.len() <= 7 {
            self.buf[off] = (data.len() as u8) << 1;
            self.buf[off + 1..off + 1 + data.len()].copy_from_slice(data);
        } else {
            let data_offset = self.buf.len();
            self.buf.extend_from_slice(data);
            let encoded = ((data_offset as u32) << 1) | 1;
            self.buf[off..off + 4].copy_from_slice(&encoded.to_le_bytes());
            self.buf[off + 4..off + 8].copy_from_slice(&(data.len() as u32).to_le_bytes());
        }
    }

    #[inline]
    fn set_str(&mut self, offset: usize, s: &str) {
        self.set_flex8(offset, s.as_bytes());
    }

    /// Write a vec_u64 field: `[count:4][offset:4]` in fixed, u64 data in flex.
    #[inline]
    fn set_vec_u64(&mut self, offset: usize, values: &[u64]) {
        let off = self.cmd_start + offset;
        self.buf[off..off + 4].copy_from_slice(&(values.len() as u32).to_le_bytes());
        if values.is_empty() {
            return;
        }
        // Pad to 8-byte alignment
        let pad = (8 - (self.buf.len() % 8)) % 8;
        self.buf.extend_from_slice(&[0u8; 8][..pad]);
        let data_offset = self.buf.len();
        self.buf[off + 4..off + 8].copy_from_slice(&(data_offset as u32).to_le_bytes());
        for &v in values {
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
    }

    /// Write an optional flex8 bytes field.
    #[inline]
    fn set_opt_bytes(&mut self, offset: usize, b: Option<&Bytes>) {
        if let Some(b) = b {
            self.set_flex8(offset, b);
        }
    }

    /// Finalize: patch the size header. Returns `(start, len)`.
    #[inline]
    fn finish(self) -> (u32, u32) {
        let total_size = self.buf.len() - self.cmd_start;
        let start = self.cmd_start;
        self.buf[start..start + 4].copy_from_slice(&(total_size as u32).to_le_bytes());
        (start as u32, total_size as u32)
    }
}

// =============================================================================
// MqCommandBuffer — reusable buffer for zero-alloc command building
// =============================================================================

/// A reusable buffer for building multiple `MqCommand`s without per-command
/// heap allocation. Commands are serialized contiguously into a single `Vec<u8>`.
/// The buffer tracks command boundaries so callers can drain them individually.
///
/// All `write_*` methods build commands directly into the buffer — every byte
/// is written exactly once. No intermediate `SmallVec` or `CommandBuilder`.
///
/// The buffer persists across calls — `clear()` resets length but keeps the
/// allocation, so subsequent writes reuse the same memory.
pub struct MqCommandBuffer {
    buf: Vec<u8>,
    /// Command boundaries: (start, len) pairs.
    ranges: SmallVec<[(u32, u32); 8]>,
}

impl MqCommandBuffer {
    /// Create a new buffer with the given initial capacity.
    #[inline]
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            ranges: SmallVec::new(),
        }
    }

    /// Reset the buffer for reuse. Keeps the underlying allocation.
    #[inline]
    pub fn clear(&mut self) {
        self.buf.clear();
        self.ranges.clear();
    }

    /// Number of commands in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Whether the buffer has no commands.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Get the raw bytes for the command at `index`.
    #[inline]
    pub fn get(&self, index: usize) -> &[u8] {
        let (start, len) = self.ranges[index];
        &self.buf[start as usize..(start + len) as usize]
    }

    /// Create an `MqCommand` from the command at `index` (copies bytes).
    #[inline]
    pub fn to_command(&self, index: usize) -> MqCommand {
        MqCommand::from_vec(self.get(index).to_vec())
    }

    /// Raw buffer access.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Mutable raw buffer access.
    #[inline]
    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    /// Push a pre-built command range (for callers that write directly into buf).
    #[inline]
    pub fn push_range(&mut self, start: u32, len: u32) {
        self.ranges.push((start, len));
    }

    // -- Write methods: one per MqCommand constructor used by MQTT session --
    // Each writes directly into self.buf via CommandWriter. Zero intermediate copies.

    /// Write a `group_ack` command. @8 group_id:8, @16 message_ids:vec_u64, @24 response:opt_flex8
    #[inline]
    pub fn write_group_ack(
        &mut self,
        group_id: u64,
        message_ids: &[u64],
        response: Option<&Bytes>,
    ) {
        let resp_size = response.map_or(0, |b| if b.len() > 7 { b.len() } else { 0 });
        let mut w = CommandWriter::with_capacity(
            &mut self.buf,
            MqCommand::TAG_GROUP_ACK,
            0,
            32,
            32 + message_ids.len() * 8 + resp_size,
        );
        w.set_u64(8, group_id);
        w.set_vec_u64(16, message_ids);
        w.set_opt_bytes(24, response);
        let range = w.finish();
        self.ranges.push(range);
    }

    /// Write a `group_nack` command. @8 group_id:8, @16 message_ids:vec_u64
    #[inline]
    pub fn write_group_nack(&mut self, group_id: u64, message_ids: &[u64]) {
        let mut w = CommandWriter::with_capacity(
            &mut self.buf,
            MqCommand::TAG_GROUP_NACK,
            0,
            24,
            24 + message_ids.len() * 8,
        );
        w.set_u64(8, group_id);
        w.set_vec_u64(16, message_ids);
        let range = w.finish();
        self.ranges.push(range);
    }

    /// Write a `heartbeat_session` command. @8 session_id:8
    #[inline]
    pub fn write_heartbeat_session(&mut self, session_id: u64) {
        let mut w = CommandWriter::new(&mut self.buf, MqCommand::TAG_HEARTBEAT_SESSION, 0, 16);
        w.set_u64(8, session_id);
        let range = w.finish();
        self.ranges.push(range);
    }

    /// Write a `create_session` command. @8 session_id:8, @16 keep_alive_ms:8,
    /// @24 session_expiry_ms:8, @32 client_id:flex8
    #[inline]
    pub fn write_create_session(
        &mut self,
        session_id: u64,
        client_id: &str,
        keep_alive_ms: u64,
        session_expiry_ms: u64,
    ) {
        let mut w = CommandWriter::new(&mut self.buf, MqCommand::TAG_CREATE_SESSION, 0, 40);
        w.set_u64(8, session_id);
        w.set_u64(16, keep_alive_ms);
        w.set_u64(24, session_expiry_ms);
        w.set_str(32, client_id);
        let range = w.finish();
        self.ranges.push(range);
    }

    /// Write a `disconnect_session` command. flags[0]=publish_will, @8 session_id:8
    #[inline]
    pub fn write_disconnect_session(&mut self, session_id: u64, publish_will: bool) {
        let flags = if publish_will { 1u8 } else { 0u8 };
        let mut w = CommandWriter::new(&mut self.buf, MqCommand::TAG_DISCONNECT_SESSION, flags, 16);
        w.set_u64(8, session_id);
        let range = w.finish();
        self.ranges.push(range);
    }

    /// Write a `delete_binding` command. @8 binding_id:8
    #[inline]
    pub fn write_delete_binding(&mut self, binding_id: u64) {
        let mut w = CommandWriter::new(&mut self.buf, MqCommand::TAG_DELETE_BINDING, 0, 16);
        w.set_u64(8, binding_id);
        let range = w.finish();
        self.ranges.push(range);
    }

    /// Write a `delete_consumer_group` command. @8 group_id:8
    #[inline]
    pub fn write_delete_consumer_group(&mut self, group_id: u64) {
        let mut w = CommandWriter::new(&mut self.buf, MqCommand::TAG_DELETE_CONSUMER_GROUP, 0, 16);
        w.set_u64(8, group_id);
        let range = w.finish();
        self.ranges.push(range);
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
            extra: SmallVec::new(),
        })
    }

    fn decode_from_bytes(data: Bytes) -> Result<Self, CodecError> {
        Ok(MqCommand {
            buf: data,
            extra: SmallVec::new(),
        })
    }
}

impl BorrowPayload for MqCommand {
    fn payload_bytes(&self) -> &[u8] {
        &self.buf
    }

    fn extra_payload_segments(&self) -> &[bytes::Bytes] {
        &self.extra
    }
}

// =============================================================================
// MqCommand — Constructor methods
// =============================================================================

impl MqCommand {
    // -- Topics (0-5) --

    /// @8 name:flex8, @16 retention:blob, @24 partition_count:4
    pub fn create_topic(name: &str, retention: RetentionPolicy, partition_count: u32) -> Self {
        CommandBuilder::new(Self::TAG_CREATE_TOPIC, 0, 32)
            .set_str(8, name)
            .set_blob_encode(16, &retention)
            .set_u32(24, partition_count)
            .finish()
    }

    /// @8 topic_id:8
    pub fn delete_topic(topic_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_DELETE_TOPIC, 0, 16)
            .set_u64(8, topic_id)
            .finish()
    }

    /// @8 topic_id:8, @16 messages:vec_bytes
    pub fn publish(topic_id: u64, messages: &[Bytes]) -> Self {
        let table_size = messages.len() * 8;
        let data_size: usize = messages.iter().map(|m| m.len()).sum();
        CommandBuilder::with_capacity(Self::TAG_PUBLISH, 0, 24, 24 + table_size + data_size)
            .set_u64(8, topic_id)
            .set_vec_bytes(16, messages)
            .finish()
    }

    /// Publish with owned messages Vec. Produces identical wire format to `publish`.
    pub fn publish_scatter(topic_id: u64, messages: Vec<Bytes>) -> Self {
        CommandBuilder::new(Self::TAG_PUBLISH, 0, 24)
            .set_u64(8, topic_id)
            .set_vec_bytes(16, &messages)
            .finish()
    }

    /// Exchange publish with owned messages Vec.
    pub fn publish_to_exchange_scatter(exchange_id: u64, messages: Vec<Bytes>) -> Self {
        CommandBuilder::new(Self::TAG_PUBLISH_TO_EXCHANGE, 0, 24)
            .set_u64(8, exchange_id)
            .set_vec_bytes(16, &messages)
            .finish()
    }

    /// @8 topic_id:8, @16 consumer_id:8, @24 offset:8
    pub fn commit_offset(topic_id: u64, consumer_id: u64, offset: u64) -> Self {
        CommandBuilder::new(Self::TAG_COMMIT_OFFSET, 0, 32)
            .set_u64(8, topic_id)
            .set_u64(16, consumer_id)
            .set_u64(24, offset)
            .finish()
    }

    /// @8 topic_id:8, @16 before_index:8
    pub fn purge_topic(topic_id: u64, before_index: u64) -> Self {
        CommandBuilder::new(Self::TAG_PURGE_TOPIC, 0, 24)
            .set_u64(8, topic_id)
            .set_u64(16, before_index)
            .finish()
    }

    /// @8 exchange_id:8, @16 routing_key:flex8, @24 message:flex8
    pub fn set_retained(exchange_id: u64, routing_key: &str, message: &Bytes) -> Self {
        CommandBuilder::new(Self::TAG_SET_RETAINED, 0, 32)
            .set_u64(8, exchange_id)
            .set_str(16, routing_key)
            .set_bytes_val(24, message)
            .finish()
    }

    /// @8 exchange_id:8, @16 filter:opt_flex8
    pub fn get_retained(exchange_id: u64, routing_key_filter: Option<&str>) -> Self {
        CommandBuilder::new(Self::TAG_GET_RETAINED, 0, 24)
            .set_u64(8, exchange_id)
            .set_opt_str(16, routing_key_filter)
            .finish()
    }

    /// @8 exchange_id:8, @16 routing_key:flex8
    pub fn delete_retained(exchange_id: u64, routing_key: &str) -> Self {
        CommandBuilder::new(Self::TAG_DELETE_RETAINED, 0, 24)
            .set_u64(8, exchange_id)
            .set_str(16, routing_key)
            .finish()
    }

    // -- Exchanges (6-10) --

    /// flags=exchange_type, @8 name:flex8
    pub fn create_exchange(name: &str, exchange_type: ExchangeType) -> Self {
        let et_byte = match exchange_type {
            ExchangeType::Direct => 0,
            ExchangeType::Fanout => 1,
            ExchangeType::Topic => 2,
        };
        CommandBuilder::new(Self::TAG_CREATE_EXCHANGE, et_byte, 16)
            .set_str(8, name)
            .finish()
    }

    /// @8 exchange_id:8
    pub fn delete_exchange(exchange_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_DELETE_EXCHANGE, 0, 16)
            .set_u64(8, exchange_id)
            .finish()
    }

    /// flags[0]=no_local, @8 exchange_id:8, @16 topic_id:8,
    /// @24 routing_key:opt_flex8, @32 shared_group:opt_flex8,
    /// @40 subscription_id:4, @44 has_sub_id:1
    pub fn create_binding(exchange_id: u64, topic_id: u64, routing_key: Option<&str>) -> Self {
        Self::create_binding_with_opts(exchange_id, topic_id, routing_key, false, None, None)
    }

    pub fn create_binding_with_opts(
        exchange_id: u64,
        topic_id: u64,
        routing_key: Option<&str>,
        no_local: bool,
        shared_group: Option<&str>,
        subscription_id: Option<u32>,
    ) -> Self {
        let flags = if no_local { 1u8 } else { 0u8 };
        let mut b = CommandBuilder::new(Self::TAG_CREATE_BINDING, flags, 48)
            .set_u64(8, exchange_id)
            .set_u64(16, topic_id)
            .set_opt_str(24, routing_key)
            .set_opt_str(32, shared_group);
        if let Some(sid) = subscription_id {
            b = b.set_u32(40, sid).set_u8(44, 1);
        }
        b.finish()
    }

    /// @8 binding_id:8
    pub fn delete_binding(binding_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_DELETE_BINDING, 0, 16)
            .set_u64(8, binding_id)
            .finish()
    }

    /// @8 exchange_id:8, @16 messages:vec_bytes
    pub fn publish_to_exchange(exchange_id: u64, messages: &[Bytes]) -> Self {
        let table_size = messages.len() * 8;
        let data_size: usize = messages.iter().map(|m| m.len()).sum();
        CommandBuilder::with_capacity(
            Self::TAG_PUBLISH_TO_EXCHANGE,
            0,
            24,
            24 + table_size + data_size,
        )
        .set_u64(8, exchange_id)
        .set_vec_bytes(16, messages)
        .finish()
    }

    /// @8 exchange_id:8, @16 messages:vec_bytes (slice variant, same wire format)
    pub fn publish_to_exchange_slices(exchange_id: u64, messages: &[&[u8]]) -> Self {
        let table_size = messages.len() * 8;
        let data_size: usize = messages.iter().map(|m| m.len()).sum();
        CommandBuilder::with_capacity(
            Self::TAG_PUBLISH_TO_EXCHANGE,
            0,
            24,
            24 + table_size + data_size,
        )
        .set_u64(8, exchange_id)
        .set_vec_slices(16, messages)
        .finish()
    }

    /// Begin building a publish-to-exchange command where messages are written
    /// directly into the command buffer — zero intermediate copies.
    ///
    /// `capacity_hint` is the estimated total size of all message bytes
    /// (excluding the descriptor table). Pass `0` to let the buffer grow
    /// dynamically.
    ///
    /// Returns a [`VecBytesWriter`]; call `.push()` for each message,
    /// then `.finish()` to produce the `MqCommand`.
    pub fn begin_publish_to_exchange(
        tag: u8,
        exchange_id: u64,
        count: usize,
        capacity_hint: usize,
    ) -> VecBytesWriter {
        let table_size = count * 8;
        CommandBuilder::with_capacity(tag, 0, 24, 24 + table_size + capacity_hint)
            .set_u64(8, exchange_id)
            .begin_vec_bytes(16, count)
    }

    /// @8 exchange_id:8, @16 messages:vec_bytes — MQTT exchange publish variant
    pub fn publish_to_exchange_mqtt(exchange_id: u64, messages: &[&[u8]]) -> Self {
        let table_size = messages.len() * 8;
        let data_size: usize = messages.iter().map(|m| m.len()).sum();
        CommandBuilder::with_capacity(
            Self::TAG_PUBLISH_TO_EXCHANGE_MQTT,
            0,
            24,
            24 + table_size + data_size,
        )
        .set_u64(8, exchange_id)
        .set_vec_slices(16, messages)
        .finish()
    }

    /// @8 exchange_id:8, @16 routing_key:flex8, @24 message:flex8 — MQTT retained variant
    pub fn set_retained_mqtt(exchange_id: u64, routing_key: &str, message: &[u8]) -> Self {
        CommandBuilder::new(Self::TAG_SET_RETAINED_MQTT, 0, 32)
            .set_u64(8, exchange_id)
            .set_str(16, routing_key)
            .set_bytes(24, message)
            .finish()
    }

    // -- Consumer Groups (11-18) --

    /// @8 name:flex8, @16 dlq_topic_name:opt_flex8, @24 response_topic_name:opt_flex8,
    /// @32 variant_config:blob, @40 topic_retention:blob, @48 topic_dedup:blob,
    /// @56 auto_offset_reset:1|auto_create_topic:1|topic_lifetime:1
    pub fn create_consumer_group_full(
        name: &str,
        auto_offset_reset: u8,
        variant_config: &VariantConfig,
        auto_create_topic: bool,
        topic_retention: &RetentionPolicy,
        topic_dedup: Option<&TopicDedupConfig>,
        topic_lifetime: TopicLifetimePolicy,
        dlq_topic_name: Option<&str>,
        response_topic_name: Option<&str>,
    ) -> Self {
        let lifetime_byte = match topic_lifetime {
            TopicLifetimePolicy::Permanent => 0u8,
            TopicLifetimePolicy::DeleteOnLastDetach => 1u8,
        };
        let mut b = CommandBuilder::new(Self::TAG_CREATE_CONSUMER_GROUP, 0, 64)
            .set_str(8, name)
            .set_opt_str(16, dlq_topic_name)
            .set_opt_str(24, response_topic_name)
            .set_blob_encode(32, variant_config)
            .set_blob_encode(40, topic_retention)
            .set_u8(56, auto_offset_reset)
            .set_u8(57, auto_create_topic as u8)
            .set_u8(58, lifetime_byte);
        if let Some(dedup) = topic_dedup {
            b = b.set_blob_encode(48, dedup);
        }
        b.finish()
    }

    /// Simple create_consumer_group (Offset variant, no auto-create topic).
    pub fn create_consumer_group(name: &str, auto_offset_reset: u8) -> Self {
        Self::create_consumer_group_full(
            name,
            auto_offset_reset,
            &VariantConfig::Offset,
            false,
            &RetentionPolicy::default(),
            None,
            TopicLifetimePolicy::Permanent,
            None,
            None,
        )
    }

    // -- Compound create convenience APIs --

    /// Create an Ack-variant consumer group with an auto-created source topic.
    pub fn create_queue(
        name: &str,
        config: AckVariantConfig,
        retention: RetentionPolicy,
        dedup: Option<&TopicDedupConfig>,
        enable_dlq: bool,
        dlq_name: Option<&str>,
        enable_response: bool,
        response_name: Option<&str>,
    ) -> Self {
        let dlq = if enable_dlq {
            Some(
                dlq_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{}:dlq", name)),
            )
        } else {
            None
        };
        let resp = if enable_response {
            Some(
                response_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{}:resp", name)),
            )
        } else {
            None
        };
        Self::create_consumer_group_full(
            name,
            1,
            &VariantConfig::Ack(config),
            true,
            &retention,
            dedup,
            TopicLifetimePolicy::Permanent,
            dlq.as_deref(),
            resp.as_deref(),
        )
    }

    /// Create an Actor-variant consumer group with an auto-created source topic.
    pub fn create_actor_group(
        name: &str,
        config: ActorVariantConfig,
        retention: RetentionPolicy,
        enable_response: bool,
        response_name: Option<&str>,
    ) -> Self {
        let resp = if enable_response {
            Some(
                response_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{}:resp", name)),
            )
        } else {
            None
        };
        Self::create_consumer_group_full(
            name,
            1,
            &VariantConfig::Actor(config),
            true,
            &retention,
            None,
            TopicLifetimePolicy::Permanent,
            None,
            resp.as_deref(),
        )
    }

    /// Create an Ack-variant consumer group with cron auto-publish.
    pub fn create_job_cron(
        name: &str,
        ack_config: AckVariantConfig,
        retention: RetentionPolicy,
    ) -> Self {
        Self::create_consumer_group_full(
            name,
            1,
            &VariantConfig::Ack(ack_config),
            true,
            &retention,
            None,
            TopicLifetimePolicy::Permanent,
            None,
            None,
        )
    }

    /// @8 group_id:8
    pub fn delete_consumer_group(group_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_DELETE_CONSUMER_GROUP, 0, 16)
            .set_u64(8, group_id)
            .finish()
    }

    /// @8 group_id:8, @16 topic_id:8, @24 offset:8, @32 timestamp:8,
    /// @40 metadata:opt_flex8, @48 generation:i32|partition_index:u32
    pub fn commit_group_offset(
        group_id: u64,
        generation: i32,
        topic_id: u64,
        partition_index: u32,
        offset: u64,
        metadata: Option<&str>,
        timestamp: u64,
    ) -> Self {
        CommandBuilder::new(Self::TAG_COMMIT_GROUP_OFFSET, 0, 56)
            .set_u64(8, group_id)
            .set_u64(16, topic_id)
            .set_u64(24, offset)
            .set_u64(32, timestamp)
            .set_opt_str(40, metadata)
            .set_i32(48, generation)
            .set_u32(52, partition_index)
            .finish()
    }

    /// @8 group_id:8, @16 member_id:flex8, @24 client_id:flex8,
    /// @32 protocol_type:flex8, @40 session_timeout_ms:i32|rebalance_timeout_ms:i32,
    /// @48 protocols:vec_kv
    pub fn join_consumer_group(
        group_id: u64,
        member_id: &str,
        client_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: &str,
        protocols: &[(&str, &[u8])],
    ) -> Self {
        CommandBuilder::new(Self::TAG_JOIN_CONSUMER_GROUP, 0, 56)
            .set_u64(8, group_id)
            .set_str(16, member_id)
            .set_str(24, client_id)
            .set_str(32, protocol_type)
            .set_i32(40, session_timeout_ms)
            .set_i32(44, rebalance_timeout_ms)
            .set_vec_kv(48, protocols)
            .finish()
    }

    /// @8 group_id:8, @16 member_id:flex8, @24 generation:i32,
    /// @32 assignments:vec_kv
    pub fn sync_consumer_group(
        group_id: u64,
        generation: i32,
        member_id: &str,
        assignments: &[(&str, &[u8])],
    ) -> Self {
        CommandBuilder::new(Self::TAG_SYNC_CONSUMER_GROUP, 0, 40)
            .set_u64(8, group_id)
            .set_str(16, member_id)
            .set_i32(24, generation)
            .set_vec_kv(32, assignments)
            .finish()
    }

    /// @8 group_id:8, @16 member_id:flex8
    pub fn leave_consumer_group(group_id: u64, member_id: &str) -> Self {
        CommandBuilder::new(Self::TAG_LEAVE_CONSUMER_GROUP, 0, 24)
            .set_u64(8, group_id)
            .set_str(16, member_id)
            .finish()
    }

    /// @8 group_id:8, @16 member_id:flex8, @24 generation:i32
    pub fn heartbeat_consumer_group(group_id: u64, member_id: &str, generation: i32) -> Self {
        CommandBuilder::new(Self::TAG_HEARTBEAT_CONSUMER_GROUP, 0, 32)
            .set_u64(8, group_id)
            .set_str(16, member_id)
            .set_i32(24, generation)
            .finish()
    }

    /// @8 now_ms:8
    pub fn expire_group_sessions(now_ms: u64) -> Self {
        CommandBuilder::new(Self::TAG_EXPIRE_GROUP_SESSIONS, 0, 16)
            .set_u64(8, now_ms)
            .finish()
    }

    // -- Ack Variant (19-29) --

    /// @8 group_id:8, @16 consumer_id:8, @24 max_count:4
    /// @8 group_id:8, @16 consumer_id:8, @24 max_count:4, @32 exclude_publisher_id:8, @40 current_time_ms:8
    ///
    /// `exclude_publisher_id`: when non-zero, messages with this publisher_id are
    /// auto-ACKed at engine level (MQTT no-local filtering).
    /// `current_time_ms`: when non-zero, messages whose TTL has expired are
    /// auto-ACKed at engine level (MQTT message expiry filtering).
    pub fn group_deliver(group_id: u64, consumer_id: u64, max_count: u32) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_DELIVER, 0, 48)
            .set_u64(8, group_id)
            .set_u64(16, consumer_id)
            .set_u32(24, max_count)
            .set_u64(32, 0) // exclude_publisher_id (disabled)
            .set_u64(40, 0) // current_time_ms (disabled)
            .finish()
    }

    /// Extended group_deliver with no-local and expiry filtering support.
    pub fn group_deliver_filtered(
        group_id: u64,
        consumer_id: u64,
        max_count: u32,
        exclude_publisher_id: u64,
        current_time_ms: u64,
    ) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_DELIVER, 0, 48)
            .set_u64(8, group_id)
            .set_u64(16, consumer_id)
            .set_u32(24, max_count)
            .set_u64(32, exclude_publisher_id)
            .set_u64(40, current_time_ms)
            .finish()
    }

    /// @8 group_id:8, @16 message_ids:vec_u64, @24 response:opt_flex8
    pub fn group_ack(group_id: u64, message_ids: &[u64], response: Option<&Bytes>) -> Self {
        let resp_size = response.map_or(0, |b| if b.len() > 7 { b.len() } else { 0 });
        CommandBuilder::with_capacity(
            Self::TAG_GROUP_ACK,
            0,
            32,
            32 + message_ids.len() * 8 + resp_size,
        )
        .set_u64(8, group_id)
        .set_vec_u64(16, message_ids)
        .set_opt_bytes(24, response)
        .finish()
    }

    /// @8 group_id:8, @16 message_ids:vec_u64
    pub fn group_nack(group_id: u64, message_ids: &[u64]) -> Self {
        CommandBuilder::with_capacity(Self::TAG_GROUP_NACK, 0, 24, 24 + message_ids.len() * 8)
            .set_u64(8, group_id)
            .set_vec_u64(16, message_ids)
            .finish()
    }

    /// @8 group_id:8, @16 message_ids:vec_u64
    pub fn group_release(group_id: u64, message_ids: &[u64]) -> Self {
        CommandBuilder::with_capacity(Self::TAG_GROUP_RELEASE, 0, 24, 24 + message_ids.len() * 8)
            .set_u64(8, group_id)
            .set_vec_u64(16, message_ids)
            .finish()
    }

    /// @8 group_id:8, @16 message_ids:vec_u64
    pub fn group_modify(group_id: u64, message_ids: &[u64]) -> Self {
        CommandBuilder::with_capacity(Self::TAG_GROUP_MODIFY, 0, 24, 24 + message_ids.len() * 8)
            .set_u64(8, group_id)
            .set_vec_u64(16, message_ids)
            .finish()
    }

    /// @8 group_id:8, @16 message_ids:vec_u64, @24 extension_ms:8
    pub fn group_extend_visibility(group_id: u64, message_ids: &[u64], extension_ms: u64) -> Self {
        CommandBuilder::with_capacity(
            Self::TAG_GROUP_EXTEND_VISIBILITY,
            0,
            32,
            32 + message_ids.len() * 8,
        )
        .set_u64(8, group_id)
        .set_vec_u64(16, message_ids)
        .set_u64(24, extension_ms)
        .finish()
    }

    /// @8 group_id:8, @16 message_ids:vec_u64
    pub fn group_timeout_expired(group_id: u64, message_ids: &[u64]) -> Self {
        CommandBuilder::with_capacity(
            Self::TAG_GROUP_TIMEOUT_EXPIRED,
            0,
            24,
            24 + message_ids.len() * 8,
        )
        .set_u64(8, group_id)
        .set_vec_u64(16, message_ids)
        .finish()
    }

    /// @8 source_group_id:8, @16 dlq_topic_id:8,
    /// @24 dead_letter_ids:vec_u64, @32 messages:vec_bytes
    pub fn group_publish_to_dlq(
        source_group_id: u64,
        dlq_topic_id: u64,
        dead_letter_ids: &[u64],
        messages: &[Bytes],
    ) -> Self {
        let u64_size = dead_letter_ids.len() * 8;
        let table_size = messages.len() * 8;
        let data_size: usize = messages.iter().map(|m| m.len()).sum();
        CommandBuilder::with_capacity(
            Self::TAG_GROUP_PUBLISH_TO_DLQ,
            0,
            40,
            40 + u64_size + table_size + data_size,
        )
        .set_u64(8, source_group_id)
        .set_u64(16, dlq_topic_id)
        .set_vec_u64(24, dead_letter_ids)
        .set_vec_bytes(32, messages)
        .finish()
    }

    /// @8 group_id:8, @16 message_ids:vec_u64
    pub fn group_expire_pending(group_id: u64, message_ids: &[u64]) -> Self {
        CommandBuilder::with_capacity(
            Self::TAG_GROUP_EXPIRE_PENDING,
            0,
            24,
            24 + message_ids.len() * 8,
        )
        .set_u64(8, group_id)
        .set_vec_u64(16, message_ids)
        .finish()
    }

    /// @8 group_id:8
    pub fn group_purge(group_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_PURGE, 0, 16)
            .set_u64(8, group_id)
            .finish()
    }

    /// @8 group_id:8
    pub fn group_get_attributes(group_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_GET_ATTRIBUTES, 0, 16)
            .set_u64(8, group_id)
            .finish()
    }

    // -- Actor Variant (30-35) --

    /// @8 group_id:8, @16 consumer_id:8, @24 actor_ids:vec_bytes
    pub fn group_deliver_actor(group_id: u64, consumer_id: u64, actor_ids: &[Bytes]) -> Self {
        let table_size = actor_ids.len() * 8;
        let data_size: usize = actor_ids.iter().map(|a| a.len()).sum();
        CommandBuilder::with_capacity(
            Self::TAG_GROUP_DELIVER_ACTOR,
            0,
            32,
            32 + table_size + data_size,
        )
        .set_u64(8, group_id)
        .set_u64(16, consumer_id)
        .set_vec_bytes(24, actor_ids)
        .finish()
    }

    /// @8 group_id:8, @16 message_id:8, @24 actor_id:flex8, @32 response:opt_flex8
    pub fn group_ack_actor(
        group_id: u64,
        actor_id: &[u8],
        message_id: u64,
        response: Option<&Bytes>,
    ) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_ACK_ACTOR, 0, 40)
            .set_u64(8, group_id)
            .set_u64(16, message_id)
            .set_bytes(24, actor_id)
            .set_opt_bytes(32, response)
            .finish()
    }

    /// @8 group_id:8, @16 message_id:8, @24 actor_id:flex8
    pub fn group_nack_actor(group_id: u64, actor_id: &[u8], message_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_NACK_ACTOR, 0, 32)
            .set_u64(8, group_id)
            .set_u64(16, message_id)
            .set_bytes(24, actor_id)
            .finish()
    }

    /// @8 group_id:8, @16 consumer_id:8, @24 actor_ids:vec_bytes
    pub fn group_assign_actors(group_id: u64, consumer_id: u64, actor_ids: &[Bytes]) -> Self {
        let table_size = actor_ids.len() * 8;
        let data_size: usize = actor_ids.iter().map(|a| a.len()).sum();
        CommandBuilder::with_capacity(
            Self::TAG_GROUP_ASSIGN_ACTORS,
            0,
            32,
            32 + table_size + data_size,
        )
        .set_u64(8, group_id)
        .set_u64(16, consumer_id)
        .set_vec_bytes(24, actor_ids)
        .finish()
    }

    /// @8 group_id:8, @16 consumer_id:8
    pub fn group_release_actors(group_id: u64, consumer_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_RELEASE_ACTORS, 0, 24)
            .set_u64(8, group_id)
            .set_u64(16, consumer_id)
            .finish()
    }

    /// @8 group_id:8, @16 before_timestamp:8
    pub fn group_evict_idle(group_id: u64, before_timestamp: u64) -> Self {
        CommandBuilder::new(Self::TAG_GROUP_EVICT_IDLE, 0, 24)
            .set_u64(8, group_id)
            .set_u64(16, before_timestamp)
            .finish()
    }

    // -- Cron (36-39) --

    /// @8 topic_id:8
    pub fn cron_enable(topic_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_CRON_ENABLE, 0, 16)
            .set_u64(8, topic_id)
            .finish()
    }

    /// @8 topic_id:8
    pub fn cron_disable(topic_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_CRON_DISABLE, 0, 16)
            .set_u64(8, topic_id)
            .finish()
    }

    /// @8 topic_id:8, @16 triggered_at:8
    pub fn cron_trigger(topic_id: u64, triggered_at: u64) -> Self {
        CommandBuilder::new(Self::TAG_CRON_TRIGGER, 0, 24)
            .set_u64(8, topic_id)
            .set_u64(16, triggered_at)
            .finish()
    }

    /// @8 topic_id:8
    pub fn cron_update(topic_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_CRON_UPDATE, 0, 16)
            .set_u64(8, topic_id)
            .finish()
    }

    // -- Sessions (40-48) --

    /// @8 session_id:8, @16 keep_alive_ms:8, @24 session_expiry_ms:8, @32 client_id:flex8
    pub fn create_session(
        session_id: u64,
        client_id: &str,
        keep_alive_ms: u64,
        session_expiry_ms: u64,
    ) -> Self {
        CommandBuilder::new(Self::TAG_CREATE_SESSION, 0, 40)
            .set_u64(8, session_id)
            .set_u64(16, keep_alive_ms)
            .set_u64(24, session_expiry_ms)
            .set_str(32, client_id)
            .finish()
    }

    /// flags[0]=publish_will, @8 session_id:8
    pub fn disconnect_session(session_id: u64, publish_will: bool) -> Self {
        let flags = if publish_will { 1u8 } else { 0u8 };
        CommandBuilder::new(Self::TAG_DISCONNECT_SESSION, flags, 16)
            .set_u64(8, session_id)
            .finish()
    }

    /// @8 session_id:8
    pub fn heartbeat_session(session_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_HEARTBEAT_SESSION, 0, 16)
            .set_u64(8, session_id)
            .finish()
    }

    /// flags=qos(2bits)|retain(1bit), @8 session_id:8, @16 topic_id:8,
    /// @24 routing_key:flex8, @32 message:flex8, @40 delay_secs:4
    pub fn set_will(
        session_id: u64,
        topic_id: u64,
        delay_secs: u32,
        qos: u8,
        retain: bool,
        routing_key: &str,
        message: &Bytes,
    ) -> Self {
        let flags = (qos & 0x03) | (if retain { 0x04 } else { 0 });
        CommandBuilder::new(Self::TAG_SET_WILL, flags, 48)
            .set_u64(8, session_id)
            .set_u64(16, topic_id)
            .set_str(24, routing_key)
            .set_bytes_val(32, message)
            .set_u32(40, delay_secs)
            .finish()
    }

    /// @8 session_id:8
    pub fn clear_will(session_id: u64) -> Self {
        CommandBuilder::new(Self::TAG_CLEAR_WILL, 0, 16)
            .set_u64(8, session_id)
            .finish()
    }

    /// @8 now_ms:8
    pub fn fire_pending_wills(now_ms: u64) -> Self {
        CommandBuilder::new(Self::TAG_FIRE_PENDING_WILLS, 0, 16)
            .set_u64(8, now_ms)
            .finish()
    }

    /// @8 session_id:8, @16 remaining_quota:8, @24 client_id:flex8,
    /// @32 subscription_data:flex8, @40 session_expiry_secs:4|inbound_qos_inflight:4,
    /// @48 outbound_qos1_count:4
    pub fn persist_session(
        session_id: u64,
        client_id: &str,
        session_expiry_secs: u32,
        subscription_data: &Bytes,
        inbound_qos_inflight: u32,
        outbound_qos1_count: u32,
        remaining_quota: u64,
    ) -> Self {
        CommandBuilder::new(Self::TAG_PERSIST_SESSION, 0, 56)
            .set_u64(8, session_id)
            .set_u64(16, remaining_quota)
            .set_str(24, client_id)
            .set_bytes_val(32, subscription_data)
            .set_u32(40, session_expiry_secs)
            .set_u32(44, inbound_qos_inflight)
            .set_u32(48, outbound_qos1_count)
            .finish()
    }

    /// @8 client_id:flex8
    pub fn restore_session(client_id: &str) -> Self {
        CommandBuilder::new(Self::TAG_RESTORE_SESSION, 0, 16)
            .set_str(8, client_id)
            .finish()
    }

    /// @8 now_ms:8
    pub fn expire_sessions(now_ms: u64) -> Self {
        CommandBuilder::new(Self::TAG_EXPIRE_SESSIONS, 0, 16)
            .set_u64(8, now_ms)
            .finish()
    }

    // -- Batch (49) --

    /// @8 count:4, flex: sub-commands (self-sized, padded to 8-byte boundaries)
    pub fn batch(commands: &[MqCommand]) -> Self {
        // Pre-calculate total size for single allocation.
        let data_size: usize = commands
            .iter()
            .map(|cmd| {
                let len = cmd.total_encoded_size();
                len + (8 - (len % 8)) % 8
            })
            .sum();
        let mut b = CommandBuilder::with_capacity(Self::TAG_BATCH, 0, 16, 16 + data_size)
            .set_u32(8, commands.len() as u32);
        // Sub-commands go in flex region; each is self-sized via its header.
        // Pad each to 8-byte boundary for alignment.
        for cmd in commands {
            b.buf.extend_from_slice(&cmd.buf);
            let len = cmd.buf.len();
            let pad = (8 - (len % 8)) % 8;
            if pad > 0 {
                b.buf.extend_from_slice(&[0u8; 8][..pad]);
            }
        }
        b.finish()
    }

    // -- Dedup (50) --

    /// @8 topic_id:8, @16 before_timestamp:8
    pub fn prune_dedup_window(topic_id: u64, before_timestamp: u64) -> Self {
        CommandBuilder::new(Self::TAG_PRUNE_DEDUP_WINDOW, 0, 24)
            .set_u64(8, topic_id)
            .set_u64(16, before_timestamp)
            .finish()
    }

    // -- ForwardedBatch (55) --

    /// Build a TAG_FORWARDED_BATCH command from pre-encoded sub-frames.
    ///
    /// `frames` must already be in wire format: concatenated
    /// `[len:4][client_id:4][cmd_bytes]` sub-frames (no padding).
    ///
    /// Fixed region layout:
    /// ```text
    /// @8  node_id: u32
    /// @12 count:   u32
    /// @16 [sub-frames...]
    /// ```
    pub fn forwarded_batch(node_id: u32, count: u32, frames: &[u8]) -> Self {
        let total = 16 + frames.len(); // header(8) + fixed(8) + frames
        let mut buf = Vec::with_capacity(total);
        // header placeholder (patched below)
        buf.extend_from_slice(&[0u8; 8]);
        buf.extend_from_slice(&node_id.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(frames);
        // Patch size and fixed fields in header.
        buf[0..4].copy_from_slice(&(total as u32).to_le_bytes());
        buf[4..6].copy_from_slice(&16u16.to_le_bytes()); // fixed region = 16
        buf[6] = Self::TAG_FORWARDED_BATCH;
        buf[7] = 0; // flags
        MqCommand {
            buf: Bytes::from(buf),
            extra: SmallVec::new(),
        }
    }

    /// Write the TAG_FORWARDED_BATCH header into the first 16 bytes of `buf`.
    ///
    /// `buf` must have been pre-allocated with at least 16 bytes of zeroed
    /// space at the front (via `buf.put_bytes(0, 16)`), followed by sub-frames.
    /// Call this after all sub-frames have been written.
    pub fn write_forwarded_batch_header(buf: &mut bytes::BytesMut, node_id: u32, count: u32) {
        let total = buf.len() as u32;
        buf[0..4].copy_from_slice(&total.to_le_bytes());
        buf[4..6].copy_from_slice(&16u16.to_le_bytes()); // fixed region = 16
        buf[6] = Self::TAG_FORWARDED_BATCH;
        buf[7] = 0; // flags
        buf[8..12].copy_from_slice(&node_id.to_le_bytes());
        buf[12..16].copy_from_slice(&count.to_le_bytes());
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

    /// Collect publish/exchange-publish messages from the descriptor table.
    pub fn collect_publish_messages(&self) -> Vec<Bytes> {
        let v = self.as_publish();
        v.messages().collect()
    }

    /// Collect publish messages from the descriptor table.
    /// Used by the write batcher for merge operations.
    pub fn take_publish_segments(&mut self) -> Vec<Bytes> {
        let v = CmdPublish { buf: &self.buf };
        v.messages().collect()
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
        // vec_bytes slot at @16: [count:4][offset:4]
        let count = self.field_vec_bytes_count(16);
        Some(VecBytesIter::new(&self.buf, 16, count))
    }

    /// For `Publish`, `PublishToExchange`, or `PublishToExchangeMqtt`: iterate
    /// messages as borrowed slices — avoids `Bytes::slice()` overhead.
    /// Returns `None` for other command types.
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

    /// If this is a `Publish` for the given `topic_id`, return a zero-copy
    /// message iterator. Returns `None` otherwise.
    #[inline]
    pub fn publish_messages_for_topic(&self, topic_id: u64) -> Option<VecBytesIter<'_>> {
        if self.tag() != Self::TAG_PUBLISH {
            return None;
        }
        let tid = self.field_u64(8);
        if tid != topic_id {
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
        let count = self.message_count();
        VecBytesIter::new(self.buf, 16, count)
    }

    /// Borrowed-slice iterator over the messages.
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

    /// Borrowed-slice iterator over the messages — avoids `Bytes::slice()` overhead.
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
/// @8  node_id: u32  — originating follower node ID
/// @12 count:   u32  — number of sub-frames
/// @16 [sub-frames...]
/// ```
///
/// Each sub-frame:
/// ```text
/// [len:4][client_id:4][cmd_bytes + pad8]
/// ```
/// `len` = `4 + cmd_bytes.len()` (includes client_id, excludes the len field itself).
/// Total sub-frame size: `4 + align8(len)` bytes.
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

    /// Iterate over `(client_id, cmd_bytes)` sub-frames.
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
/// `(client_id: u32, cmd_bytes: &[u8])` with no allocation.
pub struct CmdForwardedBatchIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for CmdForwardedBatchIter<'a> {
    type Item = (u32, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 4 > self.buf.len() {
            return None;
        }
        // len = client_id (4) + cmd_bytes — excludes the 4-byte len field itself.
        let len = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        if len < 4 || self.pos + 4 + len > self.buf.len() {
            return None;
        }
        let client_id =
            u32::from_le_bytes(self.buf[self.pos + 4..self.pos + 8].try_into().unwrap());
        let cmd_bytes = &self.buf[self.pos + 8..self.pos + 4 + len];
        self.pos += 4 + len;
        Some((client_id, cmd_bytes))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None) // count not tracked here; use CmdForwardedBatch::count()
    }
}

/// Zero-copy iterator over vec_bytes descriptor table.
/// Uses O(1) random access per element via [size:4][offset:4] descriptors.
pub struct VecBytesIter<'a> {
    buf: &'a [u8],
    table_offset: usize,
    index: usize,
    count: usize,
}

impl<'a> VecBytesIter<'a> {
    /// Create from a raw buffer and slot offset. `count` is the element count.
    pub fn new(buf: &'a [u8], slot_offset: usize, count: u32) -> Self {
        let table_offset = if count == 0 {
            0
        } else {
            u32::from_le_bytes(buf[slot_offset + 4..slot_offset + 8].try_into().unwrap()) as usize
        };
        Self {
            buf,
            table_offset,
            index: 0,
            count: count as usize,
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
        let desc = self.table_offset + self.index * 8;
        let size = u32::from_le_bytes(self.buf[desc..desc + 4].try_into().unwrap()) as usize;
        let data_offset =
            u32::from_le_bytes(self.buf[desc + 4..desc + 8].try_into().unwrap()) as usize;
        self.index += 1;
        Some(Bytes::copy_from_slice(
            &self.buf[data_offset..data_offset + size],
        ))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.count - self.index;
        (r, Some(r))
    }
}

impl ExactSizeIterator for VecBytesIter<'_> {}

/// Iterator over vec_bytes descriptor table that yields `&[u8]` slices
/// instead of `Bytes`.
pub struct VecSliceIter<'a> {
    buf: &'a [u8],
    count: usize,
    table_offset: usize,
    index: usize,
}

impl<'a> VecSliceIter<'a> {
    /// Create from a raw buffer and the slot offset where `[count:4][table_offset:4]` lives.
    pub fn new(buf: &'a [u8], slot_offset: usize) -> Self {
        let count =
            u32::from_le_bytes(buf[slot_offset..slot_offset + 4].try_into().unwrap()) as usize;
        let table_offset = if count == 0 {
            0
        } else {
            u32::from_le_bytes(buf[slot_offset + 4..slot_offset + 8].try_into().unwrap()) as usize
        };
        Self {
            buf,
            count,
            table_offset,
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
        let desc = self.table_offset + self.index * 8;
        let len = u32::from_le_bytes(self.buf[desc..desc + 4].try_into().unwrap()) as usize;
        let offset = u32::from_le_bytes(self.buf[desc + 4..desc + 8].try_into().unwrap()) as usize;
        self.index += 1;
        Some(&self.buf[offset..offset + len])
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
// MqResponse — Encode / Decode
// =============================================================================

impl Encode for MqResponse {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            MqResponse::Ok => TAG_RESP_OK.encode(w),
            MqResponse::Error(err) => {
                TAG_RESP_ERROR.encode(w)?;
                err.encode(w)
            }
            MqResponse::EntityCreated { id } => {
                TAG_RESP_ENTITY_CREATED.encode(w)?;
                id.encode(w)
            }
            MqResponse::Messages { messages } => {
                TAG_RESP_MESSAGES.encode(w)?;
                (messages.len() as u32).encode(w)?;
                for msg in messages.iter() {
                    msg.encode(w)?;
                }
                Ok(())
            }
            MqResponse::Published { base_offset, count } => {
                TAG_RESP_PUBLISHED.encode(w)?;
                base_offset.encode(w)?;
                count.encode(w)
            }
            MqResponse::Stats(stats) => {
                TAG_RESP_STATS.encode(w)?;
                stats.encode(w)
            }
            MqResponse::BatchResponse(resps) => {
                TAG_RESP_BATCH.encode(w)?;
                (resps.len() as u32).encode(w)?;
                for resp in (*resps).iter() {
                    resp.encode(w)?;
                }
                Ok(())
            }
            MqResponse::GroupJoined {
                generation,
                leader,
                member_id,
                protocol_name,
                is_leader,
                members,
                phase_complete,
            } => {
                TAG_RESP_GROUP_JOINED.encode(w)?;
                generation.encode(w)?;
                leader.encode(w)?;
                member_id.encode(w)?;
                protocol_name.encode(w)?;
                (*is_leader as u8).encode(w)?;
                (members.len() as u32).encode(w)?;
                for (mid, meta) in members {
                    mid.encode(w)?;
                    encode_bytes(w, meta)?;
                }
                (*phase_complete as u8).encode(w)
            }
            MqResponse::GroupSynced {
                assignment,
                phase_complete,
            } => {
                TAG_RESP_GROUP_SYNCED.encode(w)?;
                encode_bytes(w, assignment)?;
                (*phase_complete as u8).encode(w)
            }
            MqResponse::DeadLettered {
                dead_letter_ids,
                dlq_topic_id,
            } => {
                TAG_RESP_DEAD_LETTERED.encode(w)?;
                encode_vec_u64(w, dead_letter_ids)?;
                dlq_topic_id.encode(w)
            }
            MqResponse::RetainedMessages { messages } => {
                TAG_RESP_RETAINED_MESSAGES.encode(w)?;
                (messages.len() as u32).encode(w)?;
                for entry in messages {
                    encode_bytes(w, &entry.routing_key)?;
                    encode_bytes(w, &entry.message)?;
                }
                Ok(())
            }
            MqResponse::WillPending {
                session_id,
                delay_ms,
            } => {
                TAG_RESP_WILL_PENDING.encode(w)?;
                session_id.encode(w)?;
                delay_ms.encode(w)
            }
            MqResponse::SessionRestored {
                session_id,
                session_expiry_ms,
                subscription_data,
            } => {
                TAG_RESP_SESSION_RESTORED.encode(w)?;
                session_id.encode(w)?;
                session_expiry_ms.encode(w)?;
                encode_bytes(w, subscription_data)
            }
            MqResponse::SessionNotFound => TAG_RESP_SESSION_NOT_FOUND.encode(w),
            MqResponse::MultiMessages { groups } => {
                TAG_RESP_MULTI_MESSAGES.encode(w)?;
                (groups.len() as u32).encode(w)?;
                for (group_id, messages) in groups {
                    group_id.encode(w)?;
                    (messages.len() as u32).encode(w)?;
                    for msg in messages.iter() {
                        msg.encode(w)?;
                    }
                }
                Ok(())
            }
            MqResponse::TopicAliases { aliases } => {
                TAG_RESP_TOPIC_ALIASES.encode(w)?;
                (aliases.len() as u32).encode(w)?;
                for a in aliases {
                    a.alias.encode(w)?;
                    a.topic_name.encode(w)?;
                }
                Ok(())
            }
            MqResponse::WillsFired { count } => {
                TAG_RESP_WILLS_FIRED.encode(w)?;
                count.encode(w)
            }
            MqResponse::Committed { log_index } => {
                TAG_RESP_COMMITTED.encode(w)?;
                log_index.encode(w)
            }
        }
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            MqResponse::Ok => 0,
            MqResponse::Error(err) => err.encoded_size(),
            MqResponse::EntityCreated { .. } => 8,
            MqResponse::Messages { messages } => {
                4 + messages.iter().map(|m| m.encoded_size()).sum::<usize>()
            }
            MqResponse::Published { .. } => 8 + 8,
            MqResponse::Stats(stats) => stats.encoded_size(),
            MqResponse::BatchResponse(resps) => {
                4 + resps.iter().map(|r| r.encoded_size()).sum::<usize>()
            }
            MqResponse::GroupJoined {
                generation: _,
                leader,
                member_id,
                protocol_name,
                members,
                ..
            } => {
                4 + leader.encoded_size()
                    + member_id.encoded_size()
                    + protocol_name.encoded_size()
                    + 1 // is_leader
                    + 4 // members count
                    + members.iter().map(|(mid, meta)| mid.encoded_size() + 4 + meta.len()).sum::<usize>()
                    + 1 // phase_complete
            }
            MqResponse::GroupSynced { assignment, .. } => 4 + assignment.len() + 1,
            MqResponse::DeadLettered {
                dead_letter_ids, ..
            } => 4 + dead_letter_ids.len() * 8 + 8,
            MqResponse::RetainedMessages { messages } => {
                4 + messages
                    .iter()
                    .map(|e| 4 + e.routing_key.len() + 4 + e.message.len())
                    .sum::<usize>()
            }
            MqResponse::WillPending { .. } => 8 + 8,
            MqResponse::SessionRestored {
                subscription_data, ..
            } => 8 + 8 + 4 + subscription_data.len(),
            MqResponse::SessionNotFound => 0,
            MqResponse::MultiMessages { groups } => {
                4 + groups
                    .iter()
                    .map(|(_, msgs)| 8 + 4 + msgs.iter().map(|m| m.encoded_size()).sum::<usize>())
                    .sum::<usize>()
            }
            MqResponse::TopicAliases { aliases } => {
                4 + aliases
                    .iter()
                    .map(|a| 2 + a.topic_name.encoded_size())
                    .sum::<usize>()
            }
            MqResponse::WillsFired { .. } => 4,
            MqResponse::Committed { .. } => 8,
        }
    }
}

impl Decode for MqResponse {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            TAG_RESP_OK => Ok(MqResponse::Ok),
            TAG_RESP_ERROR => Ok(MqResponse::Error(MqError::decode(r)?)),
            TAG_RESP_ENTITY_CREATED => Ok(MqResponse::EntityCreated {
                id: u64::decode(r)?,
            }),
            TAG_RESP_MESSAGES => {
                let count = u32::decode(r)? as usize;
                let mut messages = SmallVec::with_capacity(count.min(256));
                for _ in 0..count {
                    messages.push(DeliveredMessage::decode(r)?);
                }
                Ok(MqResponse::Messages { messages })
            }
            TAG_RESP_PUBLISHED => Ok(MqResponse::Published {
                base_offset: u64::decode(r)?,
                count: u64::decode(r)?,
            }),
            TAG_RESP_STATS => Ok(MqResponse::Stats(EntityStats::decode(r)?)),
            TAG_RESP_BATCH => {
                let count = u32::decode(r)? as usize;
                let mut resps = SmallVec::with_capacity(count.min(256));
                for _ in 0..count {
                    resps.push(MqResponse::decode(r)?);
                }
                Ok(MqResponse::BatchResponse(Box::new(resps)))
            }
            TAG_RESP_GROUP_JOINED => {
                let generation = i32::decode(r)?;
                let leader = String::decode(r)?;
                let member_id = String::decode(r)?;
                let protocol_name = String::decode(r)?;
                let is_leader = u8::decode(r)? != 0;
                let count = u32::decode(r)? as usize;
                let mut members = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    let mid = String::decode(r)?;
                    let meta = decode_bytes_owned(r)?;
                    members.push((mid, meta));
                }
                let phase_complete = u8::decode(r)? != 0;
                Ok(MqResponse::GroupJoined {
                    generation,
                    leader,
                    member_id,
                    protocol_name,
                    is_leader,
                    members,
                    phase_complete,
                })
            }
            TAG_RESP_GROUP_SYNCED => {
                let assignment = decode_bytes(r)?;
                let phase_complete = u8::decode(r)? != 0;
                Ok(MqResponse::GroupSynced {
                    assignment,
                    phase_complete,
                })
            }
            TAG_RESP_DEAD_LETTERED => Ok(MqResponse::DeadLettered {
                dead_letter_ids: decode_vec_u64(r)?,
                dlq_topic_id: u64::decode(r)?,
            }),
            TAG_RESP_RETAINED_MESSAGES => {
                let count = u32::decode(r)? as usize;
                let mut messages = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let routing_key = decode_bytes_owned(r)?;
                    let message = decode_bytes_owned(r)?;
                    messages.push(RetainedEntry {
                        routing_key,
                        message,
                    });
                }
                Ok(MqResponse::RetainedMessages { messages })
            }
            TAG_RESP_WILL_PENDING => Ok(MqResponse::WillPending {
                session_id: u64::decode(r)?,
                delay_ms: u64::decode(r)?,
            }),
            TAG_RESP_SESSION_RESTORED => Ok(MqResponse::SessionRestored {
                session_id: u64::decode(r)?,
                session_expiry_ms: u64::decode(r)?,
                subscription_data: decode_bytes_owned(r)?,
            }),
            TAG_RESP_SESSION_NOT_FOUND => Ok(MqResponse::SessionNotFound),
            TAG_RESP_MULTI_MESSAGES => {
                let count = u32::decode(r)? as usize;
                let mut groups = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    let group_id = u64::decode(r)?;
                    let msg_count = u32::decode(r)? as usize;
                    let mut messages = SmallVec::with_capacity(msg_count.min(256));
                    for _ in 0..msg_count {
                        messages.push(DeliveredMessage::decode(r)?);
                    }
                    groups.push((group_id, messages));
                }
                Ok(MqResponse::MultiMessages { groups })
            }
            TAG_RESP_TOPIC_ALIASES => {
                let count = u32::decode(r)? as usize;
                let mut aliases = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    aliases.push(TopicAliasEntry {
                        alias: u16::decode(r)?,
                        topic_name: String::decode(r)?,
                    });
                }
                Ok(MqResponse::TopicAliases { aliases })
            }
            TAG_RESP_WILLS_FIRED => Ok(MqResponse::WillsFired {
                count: u32::decode(r)?,
            }),
            TAG_RESP_COMMITTED => Ok(MqResponse::Committed {
                log_index: u64::decode(r)?,
            }),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

impl BorrowPayload for MqResponse {
    fn payload_bytes(&self) -> &[u8] {
        &[]
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_resp(resp: &MqResponse) -> MqResponse {
        let encoded = resp.encode_to_vec().unwrap();
        MqResponse::decode_from_slice(&encoded).unwrap()
    }

    #[test]
    fn publish_roundtrip() {
        let cmd = MqCommand::publish(
            42,
            &[Bytes::from_static(b"hello"), Bytes::from_static(b"world")],
        );
        let v = cmd.as_publish();
        assert_eq!(v.topic_id(), 42);
        let msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], Bytes::from_static(b"hello"));
        assert_eq!(msgs[1], Bytes::from_static(b"world"));
    }

    #[test]
    fn create_topic_roundtrip() {
        let cmd = MqCommand::create_topic(
            "my-topic",
            RetentionPolicy {
                max_age_secs: Some(3600),
                max_bytes: None,
                max_messages: Some(1_000_000),
            },
            8,
        );
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
        let cmds = vec![
            MqCommand::delete_topic(1),
            MqCommand::publish(2, &[Bytes::from_static(b"msg")]),
            MqCommand::heartbeat_session(99),
        ];
        let cmd = MqCommand::batch(&cmds);
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
    fn response_roundtrips() {
        let cases: Vec<MqResponse> = vec![
            MqResponse::Ok,
            MqResponse::Error(MqError::NotFound {
                entity: EntityKind::Topic,
                id: 42,
            }),
            MqResponse::EntityCreated { id: 7 },
            MqResponse::Published {
                base_offset: 1,
                count: 3,
            },
            MqResponse::DeadLettered {
                dead_letter_ids: smallvec::smallvec![10, 20],
                dlq_topic_id: 99,
            },
        ];
        for resp in &cases {
            let decoded = roundtrip_resp(resp);
            let enc1 = resp.encode_to_vec().unwrap();
            let enc2 = decoded.encode_to_vec().unwrap();
            assert_eq!(enc1, enc2, "roundtrip mismatch for {:?}", resp);
        }
    }

    #[test]
    fn publish_messages_zero_copy() {
        let cmd = MqCommand::publish(
            42,
            &[Bytes::from_static(b"aaa"), Bytes::from_static(b"bbbbb")],
        );

        assert_eq!(cmd.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(cmd.as_publish().topic_id(), 42);

        let msgs: Vec<Bytes> = cmd.publish_messages().unwrap().collect();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], Bytes::from_static(b"aaa"));
        assert_eq!(msgs[1], Bytes::from_static(b"bbbbb"));
    }

    #[test]
    fn publish_messages_for_topic_filter() {
        let cmd = MqCommand::publish(7, &[Bytes::from_static(b"x")]);
        assert!(cmd.publish_messages_for_topic(7).is_some());
        assert!(cmd.publish_messages_for_topic(8).is_none());
    }

    #[test]
    fn non_publish_returns_none() {
        let cmd = MqCommand::delete_topic(1);
        assert!(cmd.publish_messages().is_none());
        assert!(cmd.publish_messages_for_topic(1).is_none());
    }

    #[test]
    fn create_exchange_view() {
        let cmd = MqCommand::create_exchange("my-exchange", ExchangeType::Topic);
        let v = cmd.as_create_exchange();
        assert_eq!(v.name(), "my-exchange");
        assert_eq!(v.exchange_type(), ExchangeType::Topic);
    }

    #[test]
    fn create_binding_view() {
        let cmd = MqCommand::create_binding(1, 2, Some("routing.key"));
        let v = cmd.as_create_binding();
        assert_eq!(v.exchange_id(), 1);
        assert_eq!(v.topic_id(), 2);
        assert_eq!(v.routing_key(), Some("routing.key".to_string()));
    }

    #[test]
    fn display_format() {
        let cases: Vec<(MqCommand, &str)> = vec![
            (
                MqCommand::create_topic("events", RetentionPolicy::default(), 0),
                "CreateTopic(events)",
            ),
            (MqCommand::delete_topic(42), "DeleteTopic(42)"),
            (
                MqCommand::publish(1, &[crate::flat::FlatMessageBuilder::new(b"x").build()]),
                "Publish(topic=1, count=1)",
            ),
            (
                MqCommand::heartbeat_session(99),
                "HeartbeatSession(session=99)",
            ),
        ];

        for (cmd, expected) in cases {
            assert_eq!(format!("{}", cmd), expected);
        }
    }

    #[test]
    fn consumer_group_command_roundtrips() {
        let cmd = MqCommand::create_consumer_group("my-group", 1);
        let v = cmd.as_create_consumer_group();
        assert_eq!(v.name(), "my-group");
        assert_eq!(v.auto_offset_reset(), 1);

        let cmd = MqCommand::delete_consumer_group(42);
        assert_eq!(cmd.field_u64(8), 42);

        let cmd = MqCommand::commit_group_offset(10, 3, 20, 0, 100, Some("md"), 5000);
        let v = cmd.as_commit_group_offset();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.generation(), 3);
        assert_eq!(v.topic_id(), 20);
        assert_eq!(v.partition_index(), 0);
        assert_eq!(v.offset(), 100);
        assert_eq!(v.metadata(), Some("md"));
        assert_eq!(v.timestamp(), 5000);

        let cmd = MqCommand::commit_group_offset(10, 3, 20, 0, 100, None, 5000);
        let v = cmd.as_commit_group_offset();
        assert_eq!(v.metadata(), None);
        assert_eq!(v.timestamp(), 5000);

        let cmd = MqCommand::join_consumer_group(
            10,
            "member-1",
            "client-1",
            30_000,
            60_000,
            "consumer",
            &[("range", b"\x01\x02"), ("roundrobin", b"\x03")],
        );
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

        let cmd = MqCommand::sync_consumer_group(
            10,
            5,
            "member-1",
            &[("member-1", b"assign-1"), ("member-2", b"assign-2")],
        );
        let v = cmd.as_sync_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.generation(), 5);
        assert_eq!(v.member_id(), "member-1");
        let assignments = v.assignments();
        assert_eq!(assignments.len(), 2);

        let cmd = MqCommand::leave_consumer_group(10, "member-1");
        let v = cmd.as_leave_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");

        let cmd = MqCommand::heartbeat_consumer_group(10, "member-1", 7);
        let v = cmd.as_heartbeat_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");
        assert_eq!(v.generation(), 7);
    }

    #[test]
    fn consumer_group_response_roundtrips() {
        let resp = MqResponse::GroupJoined {
            generation: 3,
            leader: "m-1".to_string(),
            member_id: "m-2".to_string(),
            protocol_name: "range".to_string(),
            is_leader: false,
            members: vec![("m-1".to_string(), Bytes::from_static(&[1, 2, 3]))],
            phase_complete: true,
        };
        let decoded = roundtrip_resp(&resp);
        match decoded {
            MqResponse::GroupJoined {
                generation,
                leader,
                member_id,
                protocol_name,
                is_leader,
                members,
                phase_complete,
            } => {
                assert_eq!(generation, 3);
                assert_eq!(leader, "m-1");
                assert_eq!(member_id, "m-2");
                assert_eq!(protocol_name, "range");
                assert!(!is_leader);
                assert_eq!(members.len(), 1);
                assert!(phase_complete);
            }
            _ => panic!("wrong variant"),
        }

        let resp = MqResponse::GroupSynced {
            assignment: vec![4, 5, 6],
            phase_complete: false,
        };
        let decoded = roundtrip_resp(&resp);
        match decoded {
            MqResponse::GroupSynced {
                assignment,
                phase_complete,
            } => {
                assert_eq!(assignment, vec![4, 5, 6]);
                assert!(!phase_complete);
            }
            _ => panic!("wrong variant"),
        }

        let resp = MqResponse::Error(MqError::IllegalGeneration);
        match roundtrip_resp(&resp) {
            MqResponse::Error(MqError::IllegalGeneration) => {}
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn group_ack_variant_roundtrips() {
        let cmd = MqCommand::group_deliver(5, 10, 100);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_DELIVER);
        assert_eq!(cmd.field_u64(8), 5);
        assert_eq!(cmd.field_u64(16), 10);
        assert_eq!(cmd.field_u32(24), 100);

        let cmd = MqCommand::group_ack(5, &[1, 2, 3], None);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_ACK);
        assert_eq!(cmd.field_u64(8), 5);
        let ids = cmd.field_vec_u64(16);
        assert_eq!(&*ids, &[1, 2, 3]);

        let cmd = MqCommand::group_nack(7, &[10, 20]);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_NACK);

        let cmd = MqCommand::group_extend_visibility(3, &[1], 5000);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_EXTEND_VISIBILITY);

        let cmd = MqCommand::group_publish_to_dlq(1, 2, &[10], &[Bytes::from_static(b"dead")]);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_PUBLISH_TO_DLQ);
        let v = cmd.as_publish_to_dlq();
        assert_eq!(v.source_group_id(), 1);
        assert_eq!(v.dlq_topic_id(), 2);
    }

    #[test]
    fn session_command_roundtrips() {
        let cmd = MqCommand::create_session(1, "client-1", 30000, 3600000);
        assert_eq!(cmd.tag(), MqCommand::TAG_CREATE_SESSION);
        assert_eq!(cmd.field_u64(8), 1);

        let cmd = MqCommand::disconnect_session(1, true);
        assert_eq!(cmd.tag(), MqCommand::TAG_DISCONNECT_SESSION);
        assert_eq!(cmd.field_u64(8), 1);

        let cmd = MqCommand::heartbeat_session(1);
        assert_eq!(cmd.tag(), MqCommand::TAG_HEARTBEAT_SESSION);
        assert_eq!(cmd.field_u64(8), 1);
    }

    #[test]
    fn cron_command_roundtrips() {
        let cmd = MqCommand::cron_enable(5);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_ENABLE);
        assert_eq!(cmd.field_u64(8), 5);

        let cmd = MqCommand::cron_disable(5);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_DISABLE);

        let cmd = MqCommand::cron_trigger(5, 12345);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_TRIGGER);
        assert_eq!(cmd.field_u64(8), 5);
        assert_eq!(cmd.field_u64(16), 12345);
    }

    // =========================================================================
    // Wire format v2 comprehensive tests
    // =========================================================================

    #[test]
    fn v2_header_layout() {
        let cmd = MqCommand::delete_topic(42);
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
        let cmd = MqCommand::disconnect_session(1, true);
        assert_eq!(cmd.flags(), 1);
        let cmd = MqCommand::disconnect_session(1, false);
        assert_eq!(cmd.flags(), 0);
    }

    #[test]
    fn v2_flex8_inline_small() {
        // Strings ≤ 7 bytes are stored inline
        let cmd = MqCommand::create_topic("abc", RetentionPolicy::default(), 1);
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
        let cmd = MqCommand::create_topic(long_name, RetentionPolicy::default(), 1);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), long_name);
        // Verify it's in flex: bit 0 = 1
        assert_eq!(cmd.as_bytes()[8] & 1, 1);
    }

    #[test]
    fn v2_flex8_exactly_7_bytes() {
        let name = "1234567"; // exactly 7 bytes
        let cmd = MqCommand::create_topic(name, RetentionPolicy::default(), 1);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), name);
        // Should be inline: bit 0 = 0, len = 7
        assert_eq!(cmd.as_bytes()[8] & 1, 0);
        assert_eq!(cmd.as_bytes()[8] >> 1, 7);
    }

    #[test]
    fn v2_flex8_8_bytes_goes_to_flex() {
        let name = "12345678"; // 8 bytes — too large for inline
        let cmd = MqCommand::create_topic(name, RetentionPolicy::default(), 1);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), name);
        // Should be in flex region: bit 0 = 1
        assert_eq!(cmd.as_bytes()[8] & 1, 1);
    }

    #[test]
    fn v2_flex8_empty() {
        let cmd = MqCommand::create_topic("", RetentionPolicy::default(), 1);
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), "");
    }

    #[test]
    fn v2_vec_bytes_empty() {
        let cmd = MqCommand::publish(1, &[]);
        let v = cmd.as_publish();
        assert_eq!(v.topic_id(), 1);
        assert_eq!(v.message_count(), 0);
        let msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(msgs.len(), 0);
    }

    #[test]
    fn v2_vec_bytes_single() {
        let cmd = MqCommand::publish(1, &[Bytes::from_static(b"single")]);
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
        let cmd = MqCommand::publish(42, &messages);
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
        let cmd = MqCommand::publish(1, &messages);
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
        let cmd = MqCommand::publish(1, &messages);
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
        let cmd = MqCommand::group_ack(42, &ids, None);
        let decoded = cmd.field_vec_u64(16);
        assert_eq!(&*decoded, &ids);
    }

    #[test]
    fn v2_vec_u64_empty() {
        let cmd = MqCommand::group_ack(42, &[], None);
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
        let cmd = MqCommand::create_topic("t", retention.clone(), 4);
        let v = cmd.as_create_topic();
        let decoded_ret = v.retention();
        assert_eq!(decoded_ret.max_age_secs, retention.max_age_secs);
        assert_eq!(decoded_ret.max_bytes, retention.max_bytes);
        assert_eq!(decoded_ret.max_messages, retention.max_messages);
    }

    #[test]
    fn v2_blob_default() {
        let cmd = MqCommand::create_topic("t", RetentionPolicy::default(), 1);
        let v = cmd.as_create_topic();
        let ret = v.retention();
        assert_eq!(ret.max_age_secs, None);
        assert_eq!(ret.max_bytes, None);
        assert_eq!(ret.max_messages, None);
    }

    #[test]
    fn v2_batch_self_sized() {
        // Verify batch sub-commands are self-sized (size in first 4 bytes)
        let cmds = vec![MqCommand::delete_topic(1), MqCommand::delete_topic(2)];
        let batch = MqCommand::batch(&cmds);
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
        let cmds = vec![
            MqCommand::delete_topic(1),
            MqCommand::heartbeat_session(2),
            MqCommand::delete_topic(3),
        ];
        let batch = MqCommand::batch(&cmds);
        let v = batch.as_batch();
        let sub_cmds: Vec<&[u8]> = v.commands().collect();
        assert_eq!(sub_cmds.len(), 3);
        assert_eq!(crate::types::buf_field_u64(sub_cmds[0], 8), 1);
        assert_eq!(crate::types::buf_field_u64(sub_cmds[1], 8), 2);
        assert_eq!(crate::types::buf_field_u64(sub_cmds[2], 8), 3);
    }

    #[test]
    fn v2_batch_with_variable_size_subcmds() {
        let cmds = vec![
            MqCommand::create_topic("short", RetentionPolicy::default(), 1),
            MqCommand::publish(1, &[Bytes::from_static(b"hello world data")]),
            MqCommand::delete_topic(99),
        ];
        let batch = MqCommand::batch(&cmds);
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
        // The size field at offset 0 should equal total buffer length
        let cmd = MqCommand::publish(1, &[Bytes::from_static(b"test")]);
        let buf = cmd.as_bytes();
        let size = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(size as usize, buf.len());
    }

    #[test]
    fn v2_command_builder_fixed_field() {
        // The fixed field at offset 4 should be a multiple of 8
        let cmd = MqCommand::create_topic("x", RetentionPolicy::default(), 1);
        let buf = cmd.as_bytes();
        let fixed = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        assert_eq!(fixed as usize % 8, 0);
        assert!(fixed >= 8);
    }

    #[test]
    fn v2_all_simple_entity_commands() {
        // Test all commands that just have @8 entity_id:u64
        let test_cases: Vec<(fn(u64) -> MqCommand, u8)> = vec![
            (MqCommand::delete_topic, MqCommand::TAG_DELETE_TOPIC),
            (MqCommand::delete_exchange, MqCommand::TAG_DELETE_EXCHANGE),
            (MqCommand::delete_binding, MqCommand::TAG_DELETE_BINDING),
            (
                MqCommand::delete_consumer_group,
                MqCommand::TAG_DELETE_CONSUMER_GROUP,
            ),
            (
                MqCommand::heartbeat_session,
                MqCommand::TAG_HEARTBEAT_SESSION,
            ),
            (MqCommand::cron_enable, MqCommand::TAG_CRON_ENABLE),
            (MqCommand::cron_disable, MqCommand::TAG_CRON_DISABLE),
        ];
        for (ctor, expected_tag) in test_cases {
            let cmd = ctor(12345);
            assert_eq!(cmd.tag(), expected_tag);
            assert_eq!(cmd.field_u64(8), 12345);
        }
    }

    #[test]
    fn v2_create_session_full() {
        let cmd = MqCommand::create_session(99, "my-client-id", 30_000, 3_600_000);
        assert_eq!(cmd.tag(), MqCommand::TAG_CREATE_SESSION);
        assert_eq!(cmd.field_u64(8), 99);
        assert_eq!(cmd.field_u64(16), 30_000);
        assert_eq!(cmd.field_u64(24), 3_600_000);
        assert_eq!(cmd.field_flex8_str(32), "my-client-id");
    }

    #[test]
    fn v2_exchange_roundtrip() {
        for (name, etype) in [
            ("direct-ex", ExchangeType::Direct),
            ("fanout-ex", ExchangeType::Fanout),
            ("topic-ex", ExchangeType::Topic),
        ] {
            let cmd = MqCommand::create_exchange(name, etype);
            let v = cmd.as_create_exchange();
            assert_eq!(v.name(), name);
            assert_eq!(v.exchange_type(), etype);
        }
    }

    #[test]
    fn v2_binding_with_all_opts() {
        let cmd = MqCommand::create_binding_with_opts(
            10,
            20,
            Some("my.routing.key.pattern"),
            true,
            Some("shared-group-name"),
            Some(42),
        );
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
        let cmd = MqCommand::create_binding(1, 2, None);
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
        let cmd = MqCommand::publish_to_exchange(55, &msgs);
        let v = cmd.as_publish_to_exchange();
        assert_eq!(v.exchange_id(), 55);
        let decoded: Vec<Bytes> = v.messages().collect();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], Bytes::from_static(b"msg-a"));
        assert_eq!(decoded[1], Bytes::from_static(b"msg-b"));
    }

    #[test]
    fn v2_group_deliver_roundtrip() {
        let cmd = MqCommand::group_deliver(100, 200, 50);
        assert_eq!(cmd.field_u64(8), 100);
        assert_eq!(cmd.field_u64(16), 200);
        assert_eq!(cmd.field_u32(24), 50);
    }

    #[test]
    fn v2_group_extend_visibility_roundtrip() {
        let cmd = MqCommand::group_extend_visibility(7, &[10, 20, 30], 60000);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_EXTEND_VISIBILITY);
        assert_eq!(cmd.field_u64(8), 7);
        let ids = cmd.field_vec_u64(16);
        assert_eq!(&*ids, &[10, 20, 30]);
        assert_eq!(cmd.field_u64(24), 60000);
    }

    #[test]
    fn v2_publish_to_dlq_roundtrip() {
        let cmd = MqCommand::group_publish_to_dlq(
            10,
            20,
            &[100, 200],
            &[Bytes::from_static(b"dead-1"), Bytes::from_static(b"dead-2")],
        );
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
        let cmd = MqCommand::publish(42, &[Bytes::from_static(b"serde-test")]);
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
        let cmd = MqCommand::create_topic("raft-test", RetentionPolicy::default(), 4);
        let encoded = cmd.encode_to_vec().unwrap();
        let decoded = MqCommand::decode_from_slice(&encoded).unwrap();
        assert_eq!(decoded.tag(), MqCommand::TAG_CREATE_TOPIC);
        assert_eq!(decoded.as_create_topic().name(), "raft-test");
        assert_eq!(decoded.as_create_topic().partition_count(), 4);
    }

    #[test]
    fn v2_vec_kv_join_consumer_group() {
        let cmd = MqCommand::join_consumer_group(
            1,
            "m1",
            "c1",
            10000,
            20000,
            "consumer",
            &[("range", b"\x01"), ("sticky", b"\x02\x03")],
        );
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
        let cmds: Vec<MqCommand> = (0..50).map(|i| MqCommand::delete_topic(i)).collect();
        let batch = MqCommand::batch(&cmds);
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

        let contiguous = MqCommand::publish(42, &msgs);
        let scatter = MqCommand::publish_scatter(42, msgs.clone());

        // Encoded sizes must match
        assert_eq!(contiguous.encoded_size(), scatter.encoded_size());

        // Encoded bytes must be identical
        let c_bytes = contiguous.encode_to_vec().unwrap();
        let s_bytes = scatter.encode_to_vec().unwrap();
        assert_eq!(c_bytes, s_bytes);

        // Decode the encoded bytes and verify field access works
        let decoded = MqCommand::decode_from_bytes(Bytes::from(s_bytes)).unwrap();
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
        let contiguous = MqCommand::publish(99, &[]);
        let scatter = MqCommand::publish_scatter(99, vec![]);
        assert_eq!(
            contiguous.encode_to_vec().unwrap(),
            scatter.encode_to_vec().unwrap()
        );
    }

    #[test]
    fn batch_with_scatter_subcommands() {
        let msgs: Vec<Bytes> = vec![Bytes::from_static(b"aaa"), Bytes::from_static(b"bbb")];

        // Batch containing publish_scatter and contiguous commands
        let scatter_pub = MqCommand::publish_scatter(1, msgs.clone());
        let contiguous_pub = MqCommand::publish(2, &msgs);
        let delete = MqCommand::delete_topic(3);

        let batch = MqCommand::batch(&[scatter_pub, contiguous_pub, delete]);
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
        let scatter = MqCommand::publish_scatter(1, msgs.clone());
        let collected = scatter.collect_publish_messages();
        assert_eq!(collected, msgs);

        let contiguous = MqCommand::publish(1, &msgs);
        let collected = contiguous.collect_publish_messages();
        assert_eq!(collected, msgs);
    }
}
