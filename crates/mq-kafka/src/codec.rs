use std::io::Read as IoRead;

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::types::*;

// =============================================================================
// Codec Error
// =============================================================================

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("incomplete frame: need more data")]
    Incomplete,
    #[error("unsupported API key: {0}")]
    UnsupportedApiKey(i16),
    #[error("unsupported API version: key={api_key}, version={version}")]
    UnsupportedApiVersion { api_key: i16, version: i16 },
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("invalid utf-8 in string")]
    InvalidUtf8,
    #[error("unexpected end of frame")]
    UnexpectedEof,
    #[error("invalid record batch: {0}")]
    InvalidRecordBatch(String),
}

const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024; // 100 MB

// =============================================================================
// Zero-Copy Decode Helpers (using BytesCursor)
// =============================================================================

#[inline]
fn ensure_cursor(cur: &BytesCursor, n: usize) -> Result<(), CodecError> {
    if cur.remaining() < n {
        Err(CodecError::UnexpectedEof)
    } else {
        Ok(())
    }
}

fn cursor_read_i8(cur: &mut BytesCursor) -> Result<i8, CodecError> {
    ensure_cursor(cur, 1)?;
    Ok(cur.read_i8())
}

fn cursor_read_i16(cur: &mut BytesCursor) -> Result<i16, CodecError> {
    ensure_cursor(cur, 2)?;
    Ok(cur.read_i16())
}

fn cursor_read_i32(cur: &mut BytesCursor) -> Result<i32, CodecError> {
    ensure_cursor(cur, 4)?;
    Ok(cur.read_i32())
}

fn cursor_read_i64(cur: &mut BytesCursor) -> Result<i64, CodecError> {
    ensure_cursor(cur, 8)?;
    Ok(cur.read_i64())
}

/// Kafka nullable string: i16 length (-1 = null), then UTF-8 bytes.
/// Returns zero-copy WireString backed by the input Bytes.
fn cursor_read_nullable_string(cur: &mut BytesCursor) -> Result<Option<WireString>, CodecError> {
    let len = cursor_read_i16(cur)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    ensure_cursor(cur, len)?;
    // Validate UTF-8 on the slice (no copy)
    let slice = cur.as_slice();
    if std::str::from_utf8(&slice[..len]).is_err() {
        return Err(CodecError::InvalidUtf8);
    }
    // Zero-copy: slice into the underlying Bytes
    let bytes = cur.read_slice(len);
    Ok(Some(WireString::from_utf8_unchecked(bytes)))
}

/// Kafka non-nullable string: i16 length, then UTF-8 bytes.
fn cursor_read_string(cur: &mut BytesCursor) -> Result<WireString, CodecError> {
    cursor_read_nullable_string(cur)?.ok_or(CodecError::UnexpectedEof)
}

/// Kafka nullable bytes: i32 length (-1 = null), then bytes.
/// Returns zero-copy Bytes backed by the input buffer.
fn cursor_read_nullable_bytes(cur: &mut BytesCursor) -> Result<Option<Bytes>, CodecError> {
    let len = cursor_read_i32(cur)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    ensure_cursor(cur, len)?;
    Ok(Some(cur.read_slice(len)))
}

fn cursor_read_bytes(cur: &mut BytesCursor) -> Result<Bytes, CodecError> {
    cursor_read_nullable_bytes(cur)?.ok_or(CodecError::UnexpectedEof)
}

fn cursor_read_array_len(cur: &mut BytesCursor) -> Result<i32, CodecError> {
    cursor_read_i32(cur)
}

/// Zigzag-encoded varint (used inside RecordBatch).
fn cursor_read_varint(cur: &mut BytesCursor) -> Result<i32, CodecError> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    loop {
        ensure_cursor(cur, 1)?;
        let byte = cur.read_u8();
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(CodecError::InvalidRecordBatch("varint too long".into()));
        }
    }
    Ok(((result >> 1) as i32) ^ -((result & 1) as i32))
}

fn cursor_read_varlong(cur: &mut BytesCursor) -> Result<i64, CodecError> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        ensure_cursor(cur, 1)?;
        let byte = cur.read_u8();
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 70 {
            return Err(CodecError::InvalidRecordBatch("varlong too long".into()));
        }
    }
    Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
}

/// Read varint-prefixed nullable bytes (used in record body). Zero-copy.
fn cursor_read_varint_bytes(cur: &mut BytesCursor) -> Result<Option<Bytes>, CodecError> {
    let len = cursor_read_varint(cur)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    ensure_cursor(cur, len)?;
    Ok(Some(cur.read_slice(len)))
}

/// Read varint-prefixed string as Bytes (used in record headers). Zero-copy.
fn cursor_read_varint_bytes_raw(cur: &mut BytesCursor) -> Result<Bytes, CodecError> {
    let len = cursor_read_varint(cur)?;
    if len < 0 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = len as usize;
    ensure_cursor(cur, len)?;
    Ok(cur.read_slice(len))
}

// =============================================================================
// Legacy &[u8] Decode Helpers (for RecordBatch decode from arbitrary slices)
// =============================================================================

fn ensure(buf: &[u8], n: usize) -> Result<(), CodecError> {
    if buf.len() < n {
        Err(CodecError::UnexpectedEof)
    } else {
        Ok(())
    }
}

fn read_i8(buf: &mut &[u8]) -> Result<i8, CodecError> {
    ensure(buf, 1)?;
    let v = buf[0] as i8;
    *buf = &buf[1..];
    Ok(v)
}

fn read_i16(buf: &mut &[u8]) -> Result<i16, CodecError> {
    ensure(buf, 2)?;
    let v = i16::from_be_bytes([buf[0], buf[1]]);
    *buf = &buf[2..];
    Ok(v)
}

fn read_i32(buf: &mut &[u8]) -> Result<i32, CodecError> {
    ensure(buf, 4)?;
    let v = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    Ok(v)
}

fn read_i64(buf: &mut &[u8]) -> Result<i64, CodecError> {
    ensure(buf, 8)?;
    let v = i64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    *buf = &buf[8..];
    Ok(v)
}

fn read_varint(buf: &mut &[u8]) -> Result<i32, CodecError> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    loop {
        ensure(buf, 1)?;
        let byte = buf[0];
        *buf = &buf[1..];
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(CodecError::InvalidRecordBatch("varint too long".into()));
        }
    }
    Ok(((result >> 1) as i32) ^ -((result & 1) as i32))
}

fn read_varlong(buf: &mut &[u8]) -> Result<i64, CodecError> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        ensure(buf, 1)?;
        let byte = buf[0];
        *buf = &buf[1..];
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 70 {
            return Err(CodecError::InvalidRecordBatch("varlong too long".into()));
        }
    }
    Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
}

fn read_varint_bytes(buf: &mut &[u8]) -> Result<Option<Bytes>, CodecError> {
    let len = read_varint(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    ensure(buf, len)?;
    let data = Bytes::copy_from_slice(&buf[..len]);
    *buf = &buf[len..];
    Ok(Some(data))
}

fn read_varint_string(buf: &mut &[u8]) -> Result<Bytes, CodecError> {
    let len = read_varint(buf)?;
    if len < 0 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = len as usize;
    ensure(buf, len)?;
    let data = Bytes::copy_from_slice(&buf[..len]);
    *buf = &buf[len..];
    Ok(data)
}

// =============================================================================
// Encode Helpers
// =============================================================================

fn write_i8(buf: &mut BytesMut, v: i8) {
    buf.put_i8(v);
}

fn write_i16(buf: &mut BytesMut, v: i16) {
    buf.put_i16(v);
}

fn write_i32(buf: &mut BytesMut, v: i32) {
    buf.put_i32(v);
}

fn write_i64(buf: &mut BytesMut, v: i64) {
    buf.put_i64(v);
}

/// Write a Kafka nullable string from an Option<WireString>.
fn write_nullable_string(buf: &mut BytesMut, s: &Option<WireString>) {
    match s {
        None => write_i16(buf, -1),
        Some(s) => {
            write_i16(buf, s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
    }
}

/// Write a Kafka string from a WireString (or anything &[u8]).
fn write_string(buf: &mut BytesMut, s: &[u8]) {
    write_i16(buf, s.len() as i16);
    buf.put_slice(s);
}

fn write_nullable_bytes(buf: &mut BytesMut, data: &Option<Bytes>) {
    match data {
        None => write_i32(buf, -1),
        Some(data) => {
            write_i32(buf, data.len() as i32);
            buf.put_slice(data);
        }
    }
}

fn write_bytes(buf: &mut BytesMut, data: &[u8]) {
    write_i32(buf, data.len() as i32);
    buf.put_slice(data);
}

fn write_varint(buf: &mut BytesMut, val: i32) {
    let mut v = ((val << 1) ^ (val >> 31)) as u32;
    loop {
        if v & !0x7F == 0 {
            buf.put_u8(v as u8);
            break;
        }
        buf.put_u8((v & 0x7F | 0x80) as u8);
        v >>= 7;
    }
}

fn write_varlong(buf: &mut BytesMut, val: i64) {
    let mut v = ((val << 1) ^ (val >> 63)) as u64;
    loop {
        if v & !0x7F == 0 {
            buf.put_u8(v as u8);
            break;
        }
        buf.put_u8((v & 0x7F | 0x80) as u8);
        v >>= 7;
    }
}

fn write_varint_bytes(buf: &mut BytesMut, data: &Option<Bytes>) {
    match data {
        None => write_varint(buf, -1),
        Some(data) => {
            write_varint(buf, data.len() as i32);
            buf.put_slice(data);
        }
    }
}

fn write_varint_slice(buf: &mut BytesMut, data: &[u8]) {
    write_varint(buf, data.len() as i32);
    buf.put_slice(data);
}

// =============================================================================
// Compact (Flexible Version) Helpers
// =============================================================================

#[allow(dead_code)]
/// Unsigned varint for compact lengths (Kafka flexible versions).
fn write_unsigned_varint(buf: &mut BytesMut, val: u32) {
    let mut v = val;
    loop {
        if v & !0x7F == 0 {
            buf.put_u8(v as u8);
            break;
        }
        buf.put_u8((v & 0x7F | 0x80) as u8);
        v >>= 7;
    }
}

#[allow(dead_code)]
fn cursor_read_unsigned_varint(cur: &mut BytesCursor) -> Result<u32, CodecError> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    loop {
        ensure_cursor(cur, 1)?;
        let byte = cur.read_u8();
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(CodecError::InvalidRecordBatch(
                "unsigned varint too long".into(),
            ));
        }
    }
    Ok(result)
}

#[allow(dead_code)]
/// Compact string: unsigned_varint(len+1), then UTF-8 bytes. 0 = null.
fn cursor_read_compact_string(cur: &mut BytesCursor) -> Result<WireString, CodecError> {
    let len = cursor_read_unsigned_varint(cur)?;
    if len == 0 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = (len - 1) as usize;
    ensure_cursor(cur, len)?;
    let slice = cur.as_slice();
    if std::str::from_utf8(&slice[..len]).is_err() {
        return Err(CodecError::InvalidUtf8);
    }
    let bytes = cur.read_slice(len);
    Ok(WireString::from_utf8_unchecked(bytes))
}

#[allow(dead_code)]
fn cursor_read_compact_nullable_string(
    cur: &mut BytesCursor,
) -> Result<Option<WireString>, CodecError> {
    let len = cursor_read_unsigned_varint(cur)?;
    if len == 0 {
        return Ok(None);
    }
    let len = (len - 1) as usize;
    ensure_cursor(cur, len)?;
    let slice = cur.as_slice();
    if std::str::from_utf8(&slice[..len]).is_err() {
        return Err(CodecError::InvalidUtf8);
    }
    let bytes = cur.read_slice(len);
    Ok(Some(WireString::from_utf8_unchecked(bytes)))
}

#[allow(dead_code)]
/// Compact array length: unsigned_varint(count+1). 0 = null (-1 equivalent).
fn cursor_read_compact_array_len(cur: &mut BytesCursor) -> Result<i32, CodecError> {
    let n = cursor_read_unsigned_varint(cur)?;
    if n == 0 { Ok(-1) } else { Ok((n - 1) as i32) }
}

#[allow(dead_code)]
fn cursor_read_compact_bytes(cur: &mut BytesCursor) -> Result<Bytes, CodecError> {
    let len = cursor_read_unsigned_varint(cur)?;
    if len == 0 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = (len - 1) as usize;
    ensure_cursor(cur, len)?;
    Ok(cur.read_slice(len))
}

#[allow(dead_code)]
fn cursor_read_compact_nullable_bytes(cur: &mut BytesCursor) -> Result<Option<Bytes>, CodecError> {
    let len = cursor_read_unsigned_varint(cur)?;
    if len == 0 {
        return Ok(None);
    }
    let len = (len - 1) as usize;
    ensure_cursor(cur, len)?;
    Ok(Some(cur.read_slice(len)))
}

#[allow(dead_code)]
/// Skip tagged fields section (for flexible versions).
/// Format: unsigned_varint(num_fields), then for each: unsigned_varint(tag), unsigned_varint(size), bytes.
fn cursor_skip_tagged_fields(cur: &mut BytesCursor) -> Result<(), CodecError> {
    let num_fields = cursor_read_unsigned_varint(cur)?;
    for _ in 0..num_fields {
        let _tag = cursor_read_unsigned_varint(cur)?;
        let size = cursor_read_unsigned_varint(cur)? as usize;
        ensure_cursor(cur, size)?;
        cur.advance(size);
    }
    Ok(())
}

#[allow(dead_code)]
/// Write compact string: unsigned_varint(len+1) then bytes.
fn write_compact_string(buf: &mut BytesMut, s: &[u8]) {
    write_unsigned_varint(buf, (s.len() + 1) as u32);
    buf.put_slice(s);
}

#[allow(dead_code)]
fn write_compact_nullable_string(buf: &mut BytesMut, s: &Option<WireString>) {
    match s {
        None => write_unsigned_varint(buf, 0),
        Some(s) => {
            write_unsigned_varint(buf, (s.len() + 1) as u32);
            buf.put_slice(s.as_bytes());
        }
    }
}

#[allow(dead_code)]
fn write_compact_array_len(buf: &mut BytesMut, len: usize) {
    write_unsigned_varint(buf, (len + 1) as u32);
}

#[allow(dead_code)]
fn write_compact_bytes(buf: &mut BytesMut, data: &[u8]) {
    write_unsigned_varint(buf, (data.len() + 1) as u32);
    buf.put_slice(data);
}

#[allow(dead_code)]
fn write_compact_nullable_bytes(buf: &mut BytesMut, data: &Option<Bytes>) {
    match data {
        None => write_unsigned_varint(buf, 0),
        Some(data) => {
            write_unsigned_varint(buf, (data.len() + 1) as u32);
            buf.put_slice(data);
        }
    }
}

#[allow(dead_code)]
/// Write empty tagged fields section (0 fields).
fn write_empty_tagged_fields(buf: &mut BytesMut) {
    write_unsigned_varint(buf, 0);
}

// =============================================================================
// Flexible-version-aware encode/decode helpers
// =============================================================================

/// Write a string, using compact encoding if `flexible` is true.
#[inline]
fn enc_string(buf: &mut BytesMut, s: &[u8], flexible: bool) {
    if flexible {
        write_compact_string(buf, s)
    } else {
        write_string(buf, s)
    }
}

/// Write a nullable string, using compact encoding if `flexible` is true.
#[inline]
fn enc_nullable_string(buf: &mut BytesMut, s: &Option<WireString>, flexible: bool) {
    if flexible {
        write_compact_nullable_string(buf, s)
    } else {
        write_nullable_string(buf, s)
    }
}

/// Write an array length, using compact encoding if `flexible` is true.
#[inline]
fn enc_array_len(buf: &mut BytesMut, len: usize, flexible: bool) {
    if flexible {
        write_compact_array_len(buf, len)
    } else {
        write_i32(buf, len as i32)
    }
}

/// Write bytes, using compact encoding if `flexible` is true.
#[inline]
fn enc_bytes(buf: &mut BytesMut, b: &[u8], flexible: bool) {
    if flexible {
        write_compact_bytes(buf, b)
    } else {
        write_bytes(buf, b)
    }
}

/// Write a nullable bytes/record_set field. When empty, writes -1 (non-flexible) or 0 (flexible).
#[inline]
fn enc_nullable_records(buf: &mut BytesMut, b: &[u8], flexible: bool) {
    if b.is_empty() {
        if flexible {
            write_unsigned_varint(buf, 0)
        } else {
            write_i32(buf, -1)
        }
    } else {
        enc_bytes(buf, b, flexible);
    }
}

/// Write tagged fields (empty) if flexible.
#[inline]
fn enc_tagged(buf: &mut BytesMut, flexible: bool) {
    if flexible {
        write_empty_tagged_fields(buf)
    }
}

/// Read a string, using compact encoding if `flexible` is true.
#[inline]
fn dec_string(cur: &mut BytesCursor, flexible: bool) -> Result<WireString, CodecError> {
    if flexible {
        cursor_read_compact_string(cur)
    } else {
        cursor_read_string(cur)
    }
}

/// Read a nullable string, using compact encoding if `flexible` is true.
#[inline]
fn dec_nullable_string(
    cur: &mut BytesCursor,
    flexible: bool,
) -> Result<Option<WireString>, CodecError> {
    if flexible {
        cursor_read_compact_nullable_string(cur)
    } else {
        cursor_read_nullable_string(cur)
    }
}

/// Read an array length, using compact encoding if `flexible` is true.
#[inline]
fn dec_array_len(cur: &mut BytesCursor, flexible: bool) -> Result<i32, CodecError> {
    if flexible {
        cursor_read_compact_array_len(cur)
    } else {
        cursor_read_array_len(cur)
    }
}

/// Read bytes, using compact encoding if `flexible` is true.
#[inline]
fn dec_bytes(cur: &mut BytesCursor, flexible: bool) -> Result<Bytes, CodecError> {
    if flexible {
        cursor_read_compact_bytes(cur)
    } else {
        cursor_read_bytes(cur)
    }
}

/// Read nullable bytes, using compact encoding if `flexible` is true.
#[inline]
fn dec_nullable_bytes(cur: &mut BytesCursor, flexible: bool) -> Result<Option<Bytes>, CodecError> {
    if flexible {
        cursor_read_compact_nullable_bytes(cur)
    } else {
        cursor_read_nullable_bytes(cur)
    }
}

/// Skip tagged fields if flexible.
#[inline]
fn dec_tagged(cur: &mut BytesCursor, flexible: bool) -> Result<(), CodecError> {
    if flexible {
        cursor_skip_tagged_fields(cur)
    } else {
        Ok(())
    }
}

// =============================================================================
// Size computation helpers (for encode_record without double-buffering)
// =============================================================================

#[inline]
fn varint_size(val: i32) -> usize {
    let mut v = ((val << 1) ^ (val >> 31)) as u32;
    let mut size = 1;
    while v > 0x7F {
        v >>= 7;
        size += 1;
    }
    size
}

#[inline]
fn varlong_size(val: i64) -> usize {
    let mut v = ((val << 1) ^ (val >> 63)) as u64;
    let mut size = 1;
    while v > 0x7F {
        v >>= 7;
        size += 1;
    }
    size
}

#[inline]
fn varint_bytes_size(data: &Option<Bytes>) -> usize {
    match data {
        None => varint_size(-1),
        Some(d) => varint_size(d.len() as i32) + d.len(),
    }
}

/// Compute the encoded size of a record body (everything after the length varint).
fn record_body_size(record: &Record) -> usize {
    1 // attributes (i8)
    + varlong_size(record.timestamp_delta)
    + varint_size(record.offset_delta)
    + varint_bytes_size(&record.key)
    + varint_bytes_size(&record.value)
    + varint_size(record.headers.len() as i32)
    + record.headers.iter().map(|h| {
        varint_size(h.key.len() as i32) + h.key.len()
        + varint_size(h.value.len() as i32) + h.value.len()
    }).sum::<usize>()
}

// =============================================================================
// Compression
// =============================================================================

/// Kafka compression types (lower 3 bits of record batch attributes).
const COMPRESSION_NONE: i16 = 0;
const COMPRESSION_GZIP: i16 = 1;
const COMPRESSION_SNAPPY: i16 = 2;
const COMPRESSION_LZ4: i16 = 3;
const COMPRESSION_ZSTD: i16 = 4;

/// Decompress record batch payload according to compression type.
fn decompress_records(compression: i16, data: &[u8]) -> Result<Vec<u8>, CodecError> {
    // Pre-allocate output at 2x compressed size as a reasonable estimate
    let estimated = data.len() * 2;

    match compression {
        COMPRESSION_GZIP => {
            let mut decoder = flate2::read::GzDecoder::new(data);
            let mut out = Vec::with_capacity(estimated);
            decoder
                .read_to_end(&mut out)
                .map_err(|e| CodecError::InvalidRecordBatch(format!("gzip decompress: {e}")))?;
            Ok(out)
        }
        COMPRESSION_SNAPPY => {
            // Kafka uses Xerial Snappy framing: [magic:8][version:4][compat:4][chunks...]
            // Each chunk: [compressed_len:4][compressed_data]
            // Standard snappy raw block is also accepted (librdkafka sends raw).
            if data.len() >= 16 && data[..8] == [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00] {
                // Xerial framing — decode directly into output buffer
                let mut out = Vec::with_capacity(estimated);
                let mut decoder = snap::raw::Decoder::new();
                let mut pos = 16; // skip magic + version + compat
                while pos < data.len() {
                    if pos + 4 > data.len() {
                        return Err(CodecError::InvalidRecordBatch(
                            "snappy: truncated chunk header".into(),
                        ));
                    }
                    let chunk_len = u32::from_be_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + chunk_len > data.len() {
                        return Err(CodecError::InvalidRecordBatch(
                            "snappy: truncated chunk data".into(),
                        ));
                    }
                    let chunk_data = &data[pos..pos + chunk_len];
                    let decompressed_len = snap::raw::decompress_len(chunk_data).map_err(|e| {
                        CodecError::InvalidRecordBatch(format!("snappy decompress: {e}"))
                    })?;
                    let prev_len = out.len();
                    out.reserve(decompressed_len);
                    // SAFETY: we reserved exactly decompressed_len bytes above
                    unsafe {
                        out.set_len(prev_len + decompressed_len);
                    }
                    decoder
                        .decompress(chunk_data, &mut out[prev_len..])
                        .map_err(|e| {
                            CodecError::InvalidRecordBatch(format!("snappy decompress: {e}"))
                        })?;
                    pos += chunk_len;
                }
                Ok(out)
            } else {
                // Raw snappy (no framing) — decompress directly into pre-sized buffer
                let decompressed_len = snap::raw::decompress_len(data).map_err(|e| {
                    CodecError::InvalidRecordBatch(format!("snappy decompress: {e}"))
                })?;
                let mut out = vec![0u8; decompressed_len];
                snap::raw::Decoder::new()
                    .decompress(data, &mut out)
                    .map_err(|e| {
                        CodecError::InvalidRecordBatch(format!("snappy decompress: {e}"))
                    })?;
                Ok(out)
            }
        }
        COMPRESSION_LZ4 => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
            let mut out = Vec::with_capacity(estimated);
            decoder
                .read_to_end(&mut out)
                .map_err(|e| CodecError::InvalidRecordBatch(format!("lz4 decompress: {e}")))?;
            Ok(out)
        }
        COMPRESSION_ZSTD => zstd::stream::decode_all(data)
            .map_err(|e| CodecError::InvalidRecordBatch(format!("zstd decompress: {e}"))),
        other => Err(CodecError::InvalidRecordBatch(format!(
            "unknown compression type: {other}"
        ))),
    }
}

// =============================================================================
// RecordBatch Encode/Decode
// =============================================================================

/// Decode a single record from within a RecordBatch.
fn decode_record(buf: &mut &[u8]) -> Result<Record, CodecError> {
    let _length = read_varint(buf)?;
    let _attributes = read_i8(buf)?;
    let timestamp_delta = read_varlong(buf)?;
    let offset_delta = read_varint(buf)?;
    let key = read_varint_bytes(buf)?;
    let value = read_varint_bytes(buf)?;

    let header_count = read_varint(buf)?;
    let mut headers = Vec::with_capacity(header_count.max(0) as usize);
    for _ in 0..header_count {
        let hdr_key = read_varint_string(buf)?;
        let hdr_val = read_varint_bytes(buf)?.unwrap_or_default();
        headers.push(RecordHeader {
            key: hdr_key,
            value: hdr_val,
        });
    }

    Ok(Record {
        offset_delta,
        timestamp_delta,
        key,
        value,
        headers,
    })
}

/// Decode a single record from a BytesCursor (zero-copy).
fn decode_record_cursor(cur: &mut BytesCursor) -> Result<Record, CodecError> {
    let _length = cursor_read_varint(cur)?;
    let _attributes = cursor_read_i8(cur)?;
    let timestamp_delta = cursor_read_varlong(cur)?;
    let offset_delta = cursor_read_varint(cur)?;
    let key = cursor_read_varint_bytes(cur)?;
    let value = cursor_read_varint_bytes(cur)?;

    let header_count = cursor_read_varint(cur)?;
    let mut headers = Vec::with_capacity(header_count.max(0) as usize);
    for _ in 0..header_count {
        let hdr_key = cursor_read_varint_bytes_raw(cur)?;
        let hdr_val = cursor_read_varint_bytes(cur)?.unwrap_or_default();
        headers.push(RecordHeader {
            key: hdr_key,
            value: hdr_val,
        });
    }

    Ok(Record {
        offset_delta,
        timestamp_delta,
        key,
        value,
        headers,
    })
}

/// Decode a Kafka v2 RecordBatch from a byte slice.
pub fn decode_record_batch(data: &[u8]) -> Result<RecordBatch, CodecError> {
    let buf = &mut &data[..];

    ensure(buf, 8 + 4 + 4)?;
    let base_offset = read_i64(buf)?;
    let _batch_length = read_i32(buf)?;
    let partition_leader_epoch = read_i32(buf)?;

    ensure(buf, 1)?;
    let magic = buf[0];
    *buf = &buf[1..];
    if magic != 2 {
        return Err(CodecError::InvalidRecordBatch(format!(
            "unsupported magic: {}",
            magic
        )));
    }

    ensure(buf, 4)?;
    let _crc = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];

    let attributes = read_i16(buf)?;
    let last_offset_delta = read_i32(buf)?;
    let first_timestamp = read_i64(buf)?;
    let max_timestamp = read_i64(buf)?;
    let producer_id = read_i64(buf)?;
    let producer_epoch = read_i16(buf)?;
    let base_sequence = read_i32(buf)?;
    let record_count = read_i32(buf)?;

    let compression = attributes & 0x07;
    let mut records = Vec::with_capacity(record_count as usize);

    if compression != COMPRESSION_NONE {
        // Decompress remaining bytes, then decode records from decompressed data
        let decompressed = decompress_records(compression, buf)?;
        let buf2 = &mut &decompressed[..];
        for _ in 0..record_count {
            records.push(decode_record(buf2)?);
        }
    } else {
        for _ in 0..record_count {
            records.push(decode_record(buf)?);
        }
    }

    Ok(RecordBatch {
        base_offset,
        partition_leader_epoch,
        attributes,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    })
}

/// Decode a Kafka v2 RecordBatch from Bytes (zero-copy for record keys/values/headers).
pub fn decode_record_batch_bytes(data: Bytes) -> Result<RecordBatch, CodecError> {
    let mut cur = BytesCursor::new(data);

    ensure_cursor(&cur, 8 + 4 + 4)?;
    let base_offset = cur.read_i64();
    let _batch_length = cur.read_i32();
    let partition_leader_epoch = cur.read_i32();

    ensure_cursor(&cur, 1)?;
    let magic = cur.read_u8();
    if magic != 2 {
        return Err(CodecError::InvalidRecordBatch(format!(
            "unsupported magic: {}",
            magic
        )));
    }

    ensure_cursor(&cur, 4)?;
    let _crc = cur.read_u32();

    let attributes = cursor_read_i16(&mut cur)?;
    let last_offset_delta = cursor_read_i32(&mut cur)?;
    let first_timestamp = cursor_read_i64(&mut cur)?;
    let max_timestamp = cursor_read_i64(&mut cur)?;
    let producer_id = cursor_read_i64(&mut cur)?;
    let producer_epoch = cursor_read_i16(&mut cur)?;
    let base_sequence = cursor_read_i32(&mut cur)?;
    let record_count = cursor_read_i32(&mut cur)?;

    let compression = attributes & 0x07;
    let mut records = Vec::with_capacity(record_count as usize);

    if compression != COMPRESSION_NONE {
        // Decompress remaining bytes, then decode records from decompressed data
        let remaining = cur.as_slice();
        let decompressed = decompress_records(compression, remaining)?;
        let buf2 = &mut &decompressed[..];
        for _ in 0..record_count {
            records.push(decode_record(buf2)?);
        }
    } else {
        for _ in 0..record_count {
            records.push(decode_record_cursor(&mut cur)?);
        }
    }

    Ok(RecordBatch {
        base_offset,
        partition_leader_epoch,
        attributes,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    })
}

/// Encode a single record directly into buf (no intermediate buffer).
/// Pre-computes body size to write the length varint first.
fn encode_record(buf: &mut BytesMut, record: &Record) {
    let body_size = record_body_size(record);
    write_varint(buf, body_size as i32);

    // Write record body directly into buf
    write_i8(buf, 0); // attributes
    write_varlong(buf, record.timestamp_delta);
    write_varint(buf, record.offset_delta);
    write_varint_bytes(buf, &record.key);
    write_varint_bytes(buf, &record.value);
    write_varint(buf, record.headers.len() as i32);
    for hdr in &record.headers {
        write_varint_slice(buf, &hdr.key);
        write_varint_slice(buf, &hdr.value);
    }
}

/// Encode a Kafka v2 RecordBatch into the buffer.
pub fn encode_record_batch(batch: &RecordBatch, buf: &mut BytesMut) {
    write_i64(buf, batch.base_offset);

    let batch_len_pos = buf.len();
    write_i32(buf, 0);

    let batch_start = buf.len();

    write_i32(buf, batch.partition_leader_epoch);
    buf.put_u8(2); // magic

    let crc_pos = buf.len();
    buf.put_u32(0);

    let crc_start = buf.len();

    write_i16(buf, batch.attributes);
    write_i32(buf, batch.last_offset_delta);
    write_i64(buf, batch.first_timestamp);
    write_i64(buf, batch.max_timestamp);
    write_i64(buf, batch.producer_id);
    write_i16(buf, batch.producer_epoch);
    write_i32(buf, batch.base_sequence);
    write_i32(buf, batch.records.len() as i32);

    for record in &batch.records {
        encode_record(buf, record);
    }

    let batch_length = (buf.len() - batch_start) as i32;
    buf[batch_len_pos..batch_len_pos + 4].copy_from_slice(&batch_length.to_be_bytes());

    let crc = crc32fast::hash(&buf[crc_start..]);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
}

// =============================================================================
// Request Decode — zero-copy via BytesCursor
// =============================================================================

/// Peek at the frame size from the first 4 bytes. Returns total frame size
/// (including the 4-byte length prefix) or `Incomplete` if not enough data.
pub fn peek_frame_size(data: &[u8]) -> Result<usize, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::Incomplete);
    }
    let frame_size = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if frame_size > MAX_FRAME_SIZE {
        return Err(CodecError::FrameTooLarge(frame_size));
    }
    Ok(4 + frame_size)
}

/// Decode a Kafka request from a pre-extracted frame (Bytes).
/// The frame includes the 4-byte length prefix.
/// All string/bytes fields are zero-copy slices into the input Bytes.
pub fn decode_request_bytes(frame: Bytes) -> Result<(RequestHeader, KafkaRequest), CodecError> {
    let mut cur = BytesCursor::new(frame);

    // Skip the 4-byte frame length (already validated by peek_frame_size)
    ensure_cursor(&cur, 4)?;
    cur.advance(4);

    let api_key = cursor_read_i16(&mut cur)?;
    let api_version = cursor_read_i16(&mut cur)?;
    let correlation_id = cursor_read_i32(&mut cur)?;

    let api = ApiKey::from_i16(api_key).ok_or(CodecError::UnsupportedApiKey(api_key))?;
    let flexible = api.is_flexible(api_version);

    // Request header v2 (flexible versions): compact string + tagged fields
    // Request header v1 (non-flexible): standard nullable string
    let client_id = if flexible {
        cursor_read_compact_nullable_string(&mut cur)?
    } else {
        cursor_read_nullable_string(&mut cur)?
    };
    if flexible {
        cursor_skip_tagged_fields(&mut cur)?;
    }

    let header = RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    };

    let request = match api {
        ApiKey::ApiVersions => KafkaRequest::ApiVersions,

        ApiKey::Metadata => {
            let count = dec_array_len(&mut cur, flexible)?;
            let topics = if count < 0 {
                None
            } else {
                let mut v = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    v.push(dec_string(&mut cur, flexible)?);
                }
                Some(v)
            };
            if api_version >= 4 {
                let _allow_auto_topic_creation = cursor_read_i8(&mut cur)?;
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::Metadata(MetadataRequest { topics })
        }

        ApiKey::Produce => {
            let transactional_id = if api_version >= 3 {
                dec_nullable_string(&mut cur, flexible)?
            } else {
                None
            };
            let acks = cursor_read_i16(&mut cur)?;
            let timeout_ms = cursor_read_i32(&mut cur)?;
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let record_set = dec_nullable_bytes(&mut cur, flexible)?;
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(ProducePartitionData {
                        partition_index,
                        record_set,
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(ProduceTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::Produce(ProduceRequest {
                transactional_id,
                acks,
                timeout_ms,
                topics,
                ..Default::default()
            })
        }

        ApiKey::Fetch => {
            let replica_id = cursor_read_i32(&mut cur)?;
            let max_wait_ms = cursor_read_i32(&mut cur)?;
            let min_bytes = cursor_read_i32(&mut cur)?;
            let max_bytes = if api_version >= 3 {
                cursor_read_i32(&mut cur)?
            } else {
                i32::MAX
            };
            let isolation_level = if api_version >= 4 {
                cursor_read_i8(&mut cur)?
            } else {
                0
            };
            let session_id = if api_version >= 7 {
                cursor_read_i32(&mut cur)?
            } else {
                0
            };
            let session_epoch = if api_version >= 7 {
                cursor_read_i32(&mut cur)?
            } else {
                -1
            };
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let current_leader_epoch = if api_version >= 9 {
                        cursor_read_i32(&mut cur)?
                    } else {
                        -1
                    };
                    let fetch_offset = cursor_read_i64(&mut cur)?;
                    let log_start_offset = if api_version >= 5 {
                        cursor_read_i64(&mut cur)?
                    } else {
                        -1
                    };
                    let max_bytes = cursor_read_i32(&mut cur)?;
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(FetchPartitionData {
                        partition_index,
                        current_leader_epoch,
                        fetch_offset,
                        log_start_offset,
                        max_bytes,
                        ..Default::default()
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(FetchTopicData {
                    topic_name,
                    partitions,
                });
            }
            let mut forgotten_topics = Vec::new();
            if api_version >= 7 {
                let forgotten_count = dec_array_len(&mut cur, flexible)?;
                forgotten_topics = Vec::with_capacity(forgotten_count.max(0) as usize);
                for _ in 0..forgotten_count.max(0) {
                    let topic_name = dec_string(&mut cur, flexible)?;
                    let part_count = dec_array_len(&mut cur, flexible)?;
                    let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                    for _ in 0..part_count.max(0) {
                        partitions.push(cursor_read_i32(&mut cur)?);
                    }
                    dec_tagged(&mut cur, flexible)?;
                    forgotten_topics.push(FetchForgottenTopic {
                        topic_name,
                        partitions,
                    });
                }
            }
            let rack_id = if api_version >= 11 {
                Some(dec_string(&mut cur, flexible)?)
            } else {
                None
            };
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::Fetch(FetchRequest {
                replica_id,
                max_wait_ms,
                min_bytes,
                max_bytes,
                isolation_level,
                session_id,
                session_epoch,
                topics,
                forgotten_topics,
                rack_id,
                ..Default::default()
            })
        }

        ApiKey::ListOffsets => {
            let replica_id = cursor_read_i32(&mut cur)?;
            if api_version >= 2 {
                let _isolation_level = cursor_read_i8(&mut cur)?;
            }
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let timestamp = cursor_read_i64(&mut cur)?;
                    if api_version == 0 {
                        let _max_num_offsets = cursor_read_i32(&mut cur)?;
                    }
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(ListOffsetsPartitionData {
                        partition_index,
                        timestamp,
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(ListOffsetsTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::ListOffsets(ListOffsetsRequest { replica_id, topics })
        }

        ApiKey::FindCoordinator => {
            let key = dec_string(&mut cur, flexible)?;
            let key_type = if api_version >= 1 {
                cursor_read_i8(&mut cur)?
            } else {
                0
            };
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::FindCoordinator(FindCoordinatorRequest { key, key_type })
        }

        ApiKey::JoinGroup => {
            let group_id = dec_string(&mut cur, flexible)?;
            let session_timeout_ms = cursor_read_i32(&mut cur)?;
            let rebalance_timeout_ms = if api_version >= 1 {
                cursor_read_i32(&mut cur)?
            } else {
                session_timeout_ms
            };
            let member_id = dec_string(&mut cur, flexible)?;
            let group_instance_id = if api_version >= 5 {
                dec_nullable_string(&mut cur, flexible)?
            } else {
                None
            };
            let protocol_type = dec_string(&mut cur, flexible)?;
            let proto_count = dec_array_len(&mut cur, flexible)?;
            let mut protocols = Vec::with_capacity(proto_count.max(0) as usize);
            for _ in 0..proto_count {
                let name = dec_string(&mut cur, flexible)?;
                let metadata = dec_bytes(&mut cur, flexible)?;
                dec_tagged(&mut cur, flexible)?;
                protocols.push(JoinGroupProtocol { name, metadata });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::JoinGroup(JoinGroupRequest {
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                ..Default::default()
            })
        }

        ApiKey::SyncGroup => {
            let group_id = dec_string(&mut cur, flexible)?;
            let generation_id = cursor_read_i32(&mut cur)?;
            let member_id = dec_string(&mut cur, flexible)?;
            let group_instance_id = if api_version >= 3 {
                dec_nullable_string(&mut cur, flexible)?
            } else {
                None
            };
            let assign_count = dec_array_len(&mut cur, flexible)?;
            let mut assignments = Vec::with_capacity(assign_count.max(0) as usize);
            for _ in 0..assign_count {
                let mid = dec_string(&mut cur, flexible)?;
                let assignment = dec_bytes(&mut cur, flexible)?;
                dec_tagged(&mut cur, flexible)?;
                assignments.push(SyncGroupAssignment {
                    member_id: mid,
                    assignment,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::SyncGroup(SyncGroupRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                assignments,
                ..Default::default()
            })
        }

        ApiKey::Heartbeat => {
            let group_id = dec_string(&mut cur, flexible)?;
            let generation_id = cursor_read_i32(&mut cur)?;
            let member_id = dec_string(&mut cur, flexible)?;
            let group_instance_id = if api_version >= 3 {
                dec_nullable_string(&mut cur, flexible)?
            } else {
                None
            };
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::Heartbeat(HeartbeatRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                ..Default::default()
            })
        }

        ApiKey::LeaveGroup => {
            let group_id = dec_string(&mut cur, flexible)?;
            let member_id = if api_version <= 2 {
                dec_string(&mut cur, flexible)?
            } else {
                WireString::empty()
            };
            let members = if api_version >= 3 {
                let count = dec_array_len(&mut cur, flexible)?;
                let mut members = Vec::with_capacity(count.max(0) as usize);
                for _ in 0..count.max(0) {
                    let mid = dec_string(&mut cur, flexible)?;
                    let gii = dec_nullable_string(&mut cur, flexible)?;
                    dec_tagged(&mut cur, flexible)?;
                    members.push(LeaveGroupMember {
                        member_id: mid,
                        group_instance_id: gii,
                    });
                }
                members
            } else {
                Vec::new()
            };
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::LeaveGroup(LeaveGroupRequest {
                group_id,
                member_id,
                members,
                ..Default::default()
            })
        }

        ApiKey::OffsetCommit => {
            let group_id = dec_string(&mut cur, flexible)?;
            let generation_id = if api_version >= 1 {
                cursor_read_i32(&mut cur)?
            } else {
                -1
            };
            let member_id = if api_version >= 1 {
                dec_string(&mut cur, flexible)?
            } else {
                WireString::empty()
            };
            if api_version >= 2 && api_version <= 4 {
                let _retention_time_ms = cursor_read_i64(&mut cur)?;
            }
            let group_instance_id = if api_version >= 7 {
                dec_nullable_string(&mut cur, flexible)?
            } else {
                None
            };
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let offset = cursor_read_i64(&mut cur)?;
                    if api_version == 1 {
                        let _timestamp = cursor_read_i64(&mut cur)?;
                    }
                    let committed_leader_epoch = if api_version >= 6 {
                        cursor_read_i32(&mut cur)?
                    } else {
                        -1
                    };
                    let metadata = dec_nullable_string(&mut cur, flexible)?;
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(OffsetCommitPartitionData {
                        partition_index,
                        offset,
                        committed_leader_epoch,
                        metadata,
                        ..Default::default()
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(OffsetCommitTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::OffsetCommit(OffsetCommitRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                topics,
                ..Default::default()
            })
        }

        ApiKey::OffsetFetch => {
            let group_id = dec_string(&mut cur, flexible)?;
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    partitions.push(cursor_read_i32(&mut cur)?);
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(OffsetFetchTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::OffsetFetch(OffsetFetchRequest { group_id, topics })
        }

        ApiKey::CreateTopics => {
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let name = dec_string(&mut cur, flexible)?;
                let num_partitions = cursor_read_i32(&mut cur)?;
                let replication_factor = cursor_read_i16(&mut cur)?;
                let assign_count = dec_array_len(&mut cur, flexible)?;
                for _ in 0..assign_count.max(0) {
                    let _partition_index = cursor_read_i32(&mut cur)?;
                    let replica_count = dec_array_len(&mut cur, flexible)?;
                    for _ in 0..replica_count.max(0) {
                        let _broker_id = cursor_read_i32(&mut cur)?;
                    }
                }
                let config_count = dec_array_len(&mut cur, flexible)?;
                for _ in 0..config_count.max(0) {
                    let _key = dec_string(&mut cur, flexible)?;
                    let _val = dec_nullable_string(&mut cur, flexible)?;
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(CreateTopicRequest {
                    name,
                    num_partitions,
                    replication_factor,
                });
            }
            let timeout_ms = cursor_read_i32(&mut cur)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::CreateTopics(CreateTopicsRequest { topics, timeout_ms })
        }

        ApiKey::DeleteTopics => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut topic_names = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                topic_names.push(dec_string(&mut cur, flexible)?);
            }
            let timeout_ms = cursor_read_i32(&mut cur)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DeleteTopics(DeleteTopicsRequest {
                topic_names,
                timeout_ms,
            })
        }

        ApiKey::DescribeGroups => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut group_ids = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                group_ids.push(dec_string(&mut cur, flexible)?);
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DescribeGroups(DescribeGroupsRequest { group_ids })
        }

        ApiKey::ListGroups => KafkaRequest::ListGroups,

        ApiKey::SaslHandshake => {
            let mechanism = dec_string(&mut cur, flexible)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::SaslHandshake(SaslHandshakeRequest { mechanism })
        }

        ApiKey::SaslAuthenticate => {
            let auth_bytes = dec_bytes(&mut cur, flexible)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes })
        }

        ApiKey::DeleteRecords => {
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let offset = cursor_read_i64(&mut cur)?;
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(DeleteRecordsPartitionData {
                        partition_index,
                        offset,
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(DeleteRecordsTopicData {
                    topic_name,
                    partitions,
                });
            }
            let timeout_ms = cursor_read_i32(&mut cur)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DeleteRecords(DeleteRecordsRequest { topics, timeout_ms })
        }

        ApiKey::InitProducerId => {
            let transactional_id = dec_nullable_string(&mut cur, flexible)?;
            let transaction_timeout_ms = cursor_read_i32(&mut cur)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::InitProducerId(InitProducerIdRequest {
                transactional_id,
                transaction_timeout_ms,
            })
        }

        ApiKey::AddPartitionsToTxn => {
            let transactional_id = dec_string(&mut cur, flexible)?;
            let producer_id = cursor_read_i64(&mut cur)?;
            let producer_epoch = cursor_read_i16(&mut cur)?;
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    partitions.push(cursor_read_i32(&mut cur)?);
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(AddPartitionsToTxnTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::AddPartitionsToTxn(AddPartitionsToTxnRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                topics,
            })
        }

        ApiKey::AddOffsetsToTxn => {
            let transactional_id = dec_string(&mut cur, flexible)?;
            let producer_id = cursor_read_i64(&mut cur)?;
            let producer_epoch = cursor_read_i16(&mut cur)?;
            let group_id = dec_string(&mut cur, flexible)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::AddOffsetsToTxn(AddOffsetsToTxnRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                group_id,
            })
        }

        ApiKey::EndTxn => {
            let transactional_id = dec_string(&mut cur, flexible)?;
            let producer_id = cursor_read_i64(&mut cur)?;
            let producer_epoch = cursor_read_i16(&mut cur)?;
            let committed = cursor_read_i8(&mut cur)? != 0;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::EndTxn(EndTxnRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                committed,
            })
        }

        ApiKey::TxnOffsetCommit => {
            let transactional_id = dec_string(&mut cur, flexible)?;
            let group_id = dec_string(&mut cur, flexible)?;
            let producer_id = cursor_read_i64(&mut cur)?;
            let producer_epoch = cursor_read_i16(&mut cur)?;
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let offset = cursor_read_i64(&mut cur)?;
                    let metadata = dec_nullable_string(&mut cur, flexible)?;
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(TxnOffsetCommitPartitionData {
                        partition_index,
                        offset,
                        metadata,
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(TxnOffsetCommitTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::TxnOffsetCommit(TxnOffsetCommitRequest {
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                topics,
            })
        }

        ApiKey::DescribeConfigs => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut resources = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                let resource_type = cursor_read_i8(&mut cur)?;
                let resource_name = dec_string(&mut cur, flexible)?;
                let name_count = dec_array_len(&mut cur, flexible)?;
                let config_names = if name_count < 0 {
                    None
                } else {
                    let mut names = Vec::with_capacity(name_count as usize);
                    for _ in 0..name_count {
                        names.push(dec_string(&mut cur, flexible)?);
                    }
                    Some(names)
                };
                dec_tagged(&mut cur, flexible)?;
                resources.push(DescribeConfigsResource {
                    resource_type,
                    resource_name,
                    config_names,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DescribeConfigs(DescribeConfigsRequest { resources })
        }

        ApiKey::AlterConfigs => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut resources = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                let resource_type = cursor_read_i8(&mut cur)?;
                let resource_name = dec_string(&mut cur, flexible)?;
                let config_count = dec_array_len(&mut cur, flexible)?;
                let mut configs = Vec::with_capacity(config_count.max(0) as usize);
                for _ in 0..config_count {
                    let name = dec_string(&mut cur, flexible)?;
                    let value = dec_nullable_string(&mut cur, flexible)?;
                    dec_tagged(&mut cur, flexible)?;
                    configs.push(AlterConfigEntry { name, value });
                }
                dec_tagged(&mut cur, flexible)?;
                resources.push(AlterConfigsResource {
                    resource_type,
                    resource_name,
                    configs,
                });
            }
            let validate_only = cursor_read_i8(&mut cur)? != 0;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::AlterConfigs(AlterConfigsRequest {
                resources,
                validate_only,
            })
        }

        ApiKey::CreatePartitions => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                let name = dec_string(&mut cur, flexible)?;
                let new_count = cursor_read_i32(&mut cur)?;
                // Skip assignments array (nullable)
                let assign_count = dec_array_len(&mut cur, flexible)?;
                for _ in 0..assign_count.max(0) {
                    let inner = dec_array_len(&mut cur, flexible)?;
                    for _ in 0..inner.max(0) {
                        let _broker = cursor_read_i32(&mut cur)?;
                    }
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(CreatePartitionsTopic {
                    name,
                    count: new_count,
                });
            }
            let timeout_ms = cursor_read_i32(&mut cur)?;
            let validate_only = cursor_read_i8(&mut cur)? != 0;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::CreatePartitions(CreatePartitionsRequest {
                topics,
                timeout_ms,
                validate_only,
            })
        }

        ApiKey::DeleteGroups => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut group_ids = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                group_ids.push(dec_string(&mut cur, flexible)?);
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DeleteGroups(DeleteGroupsRequest { group_ids })
        }

        ApiKey::OffsetDelete => {
            let group_id = dec_string(&mut cur, flexible)?;
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(OffsetDeletePartitionData { partition_index });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(OffsetDeleteTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::OffsetDelete(OffsetDeleteRequest { group_id, topics })
        }

        ApiKey::OffsetForLeaderEpoch => {
            if api_version >= 3 {
                let _replica_id = cursor_read_i32(&mut cur)?;
            }
            let topic_count = dec_array_len(&mut cur, flexible)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = dec_string(&mut cur, flexible)?;
                let part_count = dec_array_len(&mut cur, flexible)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let current_leader_epoch = if api_version >= 2 {
                        cursor_read_i32(&mut cur)?
                    } else {
                        -1
                    };
                    let leader_epoch = cursor_read_i32(&mut cur)?;
                    dec_tagged(&mut cur, flexible)?;
                    partitions.push(OffsetForLeaderEpochPartitionData {
                        partition_index,
                        current_leader_epoch,
                        leader_epoch,
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                topics.push(OffsetForLeaderEpochTopicData {
                    topic_name,
                    partitions,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::OffsetForLeaderEpoch(OffsetForLeaderEpochRequest { topics })
        }

        ApiKey::IncrementalAlterConfigs => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut resources = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count.max(0) {
                let resource_type = cursor_read_i8(&mut cur)?;
                let resource_name = dec_string(&mut cur, flexible)?;
                let config_count = dec_array_len(&mut cur, flexible)?;
                let mut configs = Vec::with_capacity(config_count.max(0) as usize);
                for _ in 0..config_count.max(0) {
                    let name = dec_string(&mut cur, flexible)?;
                    let config_operation = cursor_read_i8(&mut cur)?;
                    let value = dec_nullable_string(&mut cur, flexible)?;
                    dec_tagged(&mut cur, flexible)?;
                    configs.push(IncrementalAlterConfigEntry {
                        name,
                        config_operation,
                        value,
                    });
                }
                dec_tagged(&mut cur, flexible)?;
                resources.push(IncrementalAlterConfigsResource {
                    resource_type,
                    resource_name,
                    configs,
                });
            }
            let validate_only = cursor_read_i8(&mut cur)? != 0;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::IncrementalAlterConfigs(IncrementalAlterConfigsRequest {
                resources,
                validate_only,
            })
        }

        ApiKey::DescribeAcls => {
            let resource_type_filter = cursor_read_i8(&mut cur)?;
            let resource_name_filter = dec_nullable_string(&mut cur, flexible)?;
            let pattern_type_filter = if api_version >= 1 {
                cursor_read_i8(&mut cur)?
            } else {
                3 // MATCH (default)
            };
            let principal_filter = dec_nullable_string(&mut cur, flexible)?;
            let host_filter = dec_nullable_string(&mut cur, flexible)?;
            let operation = cursor_read_i8(&mut cur)?;
            let permission_type = cursor_read_i8(&mut cur)?;
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DescribeAcls(DescribeAclsRequest {
                resource_type_filter,
                resource_name_filter,
                pattern_type_filter,
                principal_filter,
                host_filter,
                operation,
                permission_type,
            })
        }

        ApiKey::CreateAcls => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut creations = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count.max(0) {
                let resource_type = cursor_read_i8(&mut cur)?;
                let resource_name = dec_string(&mut cur, flexible)?;
                let resource_pattern_type = if api_version >= 1 {
                    cursor_read_i8(&mut cur)?
                } else {
                    3 // LITERAL default
                };
                let principal = dec_string(&mut cur, flexible)?;
                let host = dec_string(&mut cur, flexible)?;
                let operation = cursor_read_i8(&mut cur)?;
                let permission_type = cursor_read_i8(&mut cur)?;
                dec_tagged(&mut cur, flexible)?;
                creations.push(AclCreation {
                    resource_type,
                    resource_name,
                    resource_pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::CreateAcls(CreateAclsRequest { creations })
        }

        ApiKey::DeleteAcls => {
            let count = dec_array_len(&mut cur, flexible)?;
            let mut filters = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count.max(0) {
                let resource_type_filter = cursor_read_i8(&mut cur)?;
                let resource_name_filter = dec_nullable_string(&mut cur, flexible)?;
                let pattern_type_filter = if api_version >= 1 {
                    cursor_read_i8(&mut cur)?
                } else {
                    3 // MATCH default
                };
                let principal_filter = dec_nullable_string(&mut cur, flexible)?;
                let host_filter = dec_nullable_string(&mut cur, flexible)?;
                let operation = cursor_read_i8(&mut cur)?;
                let permission_type = cursor_read_i8(&mut cur)?;
                dec_tagged(&mut cur, flexible)?;
                filters.push(AclFilter {
                    resource_type_filter,
                    resource_name_filter,
                    pattern_type_filter,
                    principal_filter,
                    host_filter,
                    operation,
                    permission_type,
                });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DeleteAcls(DeleteAclsRequest { filters })
        }

        ApiKey::DescribeLogDirs => {
            let count = dec_array_len(&mut cur, flexible)?;
            let topics = if count < 0 {
                None
            } else {
                let mut topics = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let topic = dec_string(&mut cur, flexible)?;
                    let pcount = dec_array_len(&mut cur, flexible)?;
                    let mut partitions = Vec::with_capacity(pcount.max(0) as usize);
                    for _ in 0..pcount.max(0) {
                        partitions.push(cursor_read_i32(&mut cur)?);
                    }
                    dec_tagged(&mut cur, flexible)?;
                    topics.push(DescribeLogDirsTopic { topic, partitions });
                }
                Some(topics)
            };
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DescribeLogDirs(DescribeLogDirsRequest { topics })
        }

        ApiKey::DescribeUserScramCredentials => {
            let count = dec_array_len(&mut cur, flexible)?;
            let users = if count < 0 {
                None
            } else {
                let mut users = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    users.push(dec_string(&mut cur, flexible)?);
                    dec_tagged(&mut cur, flexible)?;
                }
                Some(users)
            };
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DescribeUserScramCredentials(DescribeUserScramCredentialsRequest {
                users,
            })
        }

        ApiKey::AlterUserScramCredentials => {
            let ucount = dec_array_len(&mut cur, flexible)?;
            let mut upsertions = Vec::with_capacity(ucount.max(0) as usize);
            for _ in 0..ucount.max(0) {
                let name = dec_string(&mut cur, flexible)?;
                let mechanism = cursor_read_i8(&mut cur)?;
                let iterations = cursor_read_i32(&mut cur)?;
                let salt = dec_bytes(&mut cur, flexible)?;
                let salted_password = dec_bytes(&mut cur, flexible)?;
                dec_tagged(&mut cur, flexible)?;
                upsertions.push(ScramCredentialUpsertion {
                    name,
                    mechanism,
                    iterations,
                    salt,
                    salted_password,
                });
            }
            let dcount = dec_array_len(&mut cur, flexible)?;
            let mut deletions = Vec::with_capacity(dcount.max(0) as usize);
            for _ in 0..dcount.max(0) {
                let name = dec_string(&mut cur, flexible)?;
                let mechanism = cursor_read_i8(&mut cur)?;
                dec_tagged(&mut cur, flexible)?;
                deletions.push(ScramCredentialDeletion { name, mechanism });
            }
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::AlterUserScramCredentials(AlterUserScramCredentialsRequest {
                upsertions,
                deletions,
            })
        }

        ApiKey::DescribeCluster => {
            let include_cluster_authorized_operations = if cur.remaining() > 0 {
                cursor_read_i8(&mut cur).unwrap_or(0) != 0
            } else {
                false
            };
            dec_tagged(&mut cur, flexible)?;
            KafkaRequest::DescribeCluster(DescribeClusterRequest {
                include_cluster_authorized_operations,
            })
        }

        // Stub decode: consume remaining body as raw bytes
        ApiKey::WriteTxnMarkers => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::WriteTxnMarkers(WriteTxnMarkersRequest { data })
        }
        ApiKey::AlterReplicaLogDirs => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::AlterReplicaLogDirs(AlterReplicaLogDirsRequest { data })
        }
        ApiKey::CreateDelegationToken => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::CreateDelegationToken(CreateDelegationTokenRequest { data })
        }
        ApiKey::RenewDelegationToken => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::RenewDelegationToken(RenewDelegationTokenRequest { data })
        }
        ApiKey::ExpireDelegationToken => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ExpireDelegationToken(ExpireDelegationTokenRequest { data })
        }
        ApiKey::DescribeDelegationToken => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DescribeDelegationToken(DescribeDelegationTokenRequest { data })
        }
        ApiKey::ElectLeaders => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ElectLeaders(ElectLeadersRequest { data })
        }
        ApiKey::AlterPartitionReassignments => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::AlterPartitionReassignments(AlterPartitionReassignmentsRequest { data })
        }
        ApiKey::ListPartitionReassignments => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ListPartitionReassignments(ListPartitionReassignmentsRequest { data })
        }
        ApiKey::DescribeClientQuotas => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DescribeClientQuotas(DescribeClientQuotasRequest { data })
        }
        ApiKey::AlterClientQuotas => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::AlterClientQuotas(AlterClientQuotasRequest { data })
        }
        ApiKey::DescribeQuorum => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DescribeQuorum(DescribeQuorumRequest { data })
        }
        ApiKey::UpdateFeatures => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::UpdateFeatures(UpdateFeaturesRequest { data })
        }
        ApiKey::DescribeProducers => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DescribeProducers(DescribeProducersRequest { data })
        }
        ApiKey::UnregisterBroker => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::UnregisterBroker(UnregisterBrokerRequest { data })
        }
        ApiKey::DescribeTransactions => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DescribeTransactions(DescribeTransactionsRequest { data })
        }
        ApiKey::ListTransactions => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ListTransactions(ListTransactionsRequest { data })
        }
        ApiKey::ConsumerGroupHeartbeat => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest { data })
        }
        ApiKey::ConsumerGroupDescribe => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ConsumerGroupDescribe(ConsumerGroupDescribeRequest { data })
        }
        ApiKey::GetTelemetrySubscriptions => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::GetTelemetrySubscriptions(GetTelemetrySubscriptionsRequest { data })
        }
        ApiKey::PushTelemetry => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::PushTelemetry(PushTelemetryRequest { data })
        }
        ApiKey::ListConfigResources => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ListConfigResources(ListConfigResourcesRequest { data })
        }
        ApiKey::DescribeTopicPartitions => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DescribeTopicPartitions(DescribeTopicPartitionsRequest { data })
        }
        ApiKey::ShareGroupHeartbeat => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ShareGroupHeartbeat(ShareGroupHeartbeatRequest { data })
        }
        ApiKey::ShareGroupDescribe => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ShareGroupDescribe(ShareGroupDescribeRequest { data })
        }
        ApiKey::ShareFetch => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ShareFetch(ShareFetchRequest { data })
        }
        ApiKey::ShareAcknowledge => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ShareAcknowledge(ShareAcknowledgeRequest { data })
        }
        ApiKey::AddRaftVoter => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::AddRaftVoter(AddRaftVoterRequest { data })
        }
        ApiKey::RemoveRaftVoter => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::RemoveRaftVoter(RemoveRaftVoterRequest { data })
        }
        ApiKey::InitializeShareGroupState => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::InitializeShareGroupState(InitializeShareGroupStateRequest { data })
        }
        ApiKey::ReadShareGroupState => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ReadShareGroupState(ReadShareGroupStateRequest { data })
        }
        ApiKey::WriteShareGroupState => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::WriteShareGroupState(WriteShareGroupStateRequest { data })
        }
        ApiKey::DeleteShareGroupState => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DeleteShareGroupState(DeleteShareGroupStateRequest { data })
        }
        ApiKey::ReadShareGroupStateSummary => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::ReadShareGroupStateSummary(ReadShareGroupStateSummaryRequest { data })
        }
        ApiKey::StreamsGroupHeartbeat => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::StreamsGroupHeartbeat(StreamsGroupHeartbeatRequest { data })
        }
        ApiKey::StreamsGroupDescribe => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::StreamsGroupDescribe(StreamsGroupDescribeRequest { data })
        }
        ApiKey::DescribeShareGroupOffsets => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DescribeShareGroupOffsets(DescribeShareGroupOffsetsRequest { data })
        }
        ApiKey::AlterShareGroupOffsets => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::AlterShareGroupOffsets(AlterShareGroupOffsetsRequest { data })
        }
        ApiKey::DeleteShareGroupOffsets => {
            let data = cur.read_slice(cur.remaining());
            KafkaRequest::DeleteShareGroupOffsets(DeleteShareGroupOffsetsRequest { data })
        }
    };

    Ok((header, request))
}

/// Legacy decode from &[u8] — used by tests that build frames from BytesMut.
/// Converts to Bytes internally for zero-copy decode.
pub fn decode_request(data: &[u8]) -> Result<(RequestHeader, KafkaRequest, usize), CodecError> {
    let total = peek_frame_size(data)?;
    if data.len() < total {
        return Err(CodecError::Incomplete);
    }
    let frame = Bytes::copy_from_slice(&data[..total]);
    let (header, request) = decode_request_bytes(frame)?;
    Ok((header, request, total))
}

// =============================================================================
// Response Encode
// =============================================================================

/// Encode a Kafka response into `buf`, including the 4-byte frame length prefix.
pub fn encode_response(
    correlation_id: i32,
    api_key: i16,
    api_version: i16,
    response: &KafkaResponse,
    buf: &mut BytesMut,
) {
    let len_pos = buf.len();
    write_i32(buf, 0);

    let payload_start = buf.len();

    write_i32(buf, correlation_id);

    // Response header v1 (flexible versions): add tagged fields after correlation_id
    let flexible = ApiKey::from_i16(api_key).map_or(false, |ak| ak.is_flexible(api_version));
    if flexible {
        write_empty_tagged_fields(buf);
    }

    match response {
        KafkaResponse::ApiVersions(r) => {
            write_i16(buf, r.error_code);
            enc_array_len(buf, r.api_keys.len(), flexible);
            for ak in &r.api_keys {
                write_i16(buf, ak.api_key);
                write_i16(buf, ak.min_version);
                write_i16(buf, ak.max_version);
                enc_tagged(buf, flexible);
            }
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::Metadata(r) => {
            if api_version >= 3 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.brokers.len(), flexible);
            for b in &r.brokers {
                write_i32(buf, b.node_id);
                enc_string(buf, b.host.as_bytes(), flexible);
                write_i32(buf, b.port);
                if api_version >= 1 {
                    enc_nullable_string(buf, &None, flexible); // rack
                }
                enc_tagged(buf, flexible);
            }
            if api_version >= 2 {
                enc_string(buf, r.cluster_id.as_bytes(), flexible); // cluster_id
            }
            if api_version >= 1 {
                write_i32(buf, r.controller_id); // controller_id
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                write_i16(buf, t.error_code);
                enc_string(buf, t.name.as_bytes(), flexible);
                if api_version >= 1 {
                    buf.put_u8(0); // is_internal = false
                }
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i16(buf, p.error_code);
                    write_i32(buf, p.partition_index);
                    write_i32(buf, p.leader);
                    enc_array_len(buf, p.replicas.len(), flexible);
                    for &r in &p.replicas {
                        write_i32(buf, r);
                    }
                    enc_array_len(buf, p.isr.len(), flexible);
                    for &i in &p.isr {
                        write_i32(buf, i);
                    }
                    if api_version >= 5 {
                        enc_array_len(buf, 0, flexible); // offline_replicas count = 0
                    }
                    enc_tagged(buf, flexible);
                }
                if api_version >= 8 {
                    write_i32(buf, 0); // topic_authorized_operations
                }
                enc_tagged(buf, flexible);
            }
            if api_version >= 8 {
                write_i32(buf, i32::MIN); // cluster_authorized_operations (not available)
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::Produce(r) => {
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    write_i64(buf, p.base_offset);
                    if api_version >= 2 {
                        write_i64(buf, p.log_append_time_ms);
                    }
                    if api_version >= 5 {
                        write_i64(buf, p.log_start_offset);
                    }
                    if api_version >= 8 {
                        enc_array_len(buf, 0, flexible); // record_errors count = 0
                        enc_nullable_string(buf, &None, flexible); // error_message
                    }
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::Fetch(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            if api_version >= 7 {
                write_i16(buf, 0); // error_code
                write_i32(buf, 0); // session_id
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    write_i64(buf, p.high_watermark);
                    if api_version >= 4 {
                        write_i64(buf, p.last_stable_offset);
                    }
                    if api_version >= 5 {
                        write_i64(buf, p.log_start_offset);
                    }
                    if api_version >= 4 {
                        enc_array_len(buf, 0, flexible); // aborted_transactions count = 0
                    }
                    if api_version >= 11 {
                        write_i32(buf, p.preferred_read_replica);
                    }
                    enc_nullable_records(buf, &p.record_set, flexible);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::ListOffsets(r) => {
            if api_version >= 2 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    if api_version >= 4 {
                        write_i32(buf, 0); // leader_epoch
                    }
                    write_i64(buf, p.timestamp);
                    write_i64(buf, p.offset);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::FindCoordinator(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            write_i16(buf, r.error_code);
            if api_version >= 1 {
                enc_nullable_string(buf, &None, flexible); // error_message
            }
            write_i32(buf, r.node_id);
            enc_string(buf, r.host.as_bytes(), flexible);
            write_i32(buf, r.port);
            enc_tagged(buf, flexible);
        }

        KafkaResponse::JoinGroup(r) => {
            if api_version >= 2 {
                write_i32(buf, 0); // throttle_time_ms
            }
            write_i16(buf, r.error_code);
            write_i32(buf, r.generation_id);
            enc_string(buf, r.protocol_name.as_bytes(), flexible);
            enc_string(buf, r.leader.as_bytes(), flexible);
            enc_string(buf, r.member_id.as_bytes(), flexible);
            enc_array_len(buf, r.members.len(), flexible);
            for m in &r.members {
                enc_string(buf, m.member_id.as_bytes(), flexible);
                if api_version >= 5 {
                    enc_nullable_string(buf, &m.group_instance_id, flexible);
                }
                enc_bytes(buf, &m.metadata, flexible);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::SyncGroup(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            write_i16(buf, r.error_code);
            enc_bytes(buf, &r.assignment, flexible);
            enc_tagged(buf, flexible);
        }

        KafkaResponse::Heartbeat(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }

        KafkaResponse::LeaveGroup(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            write_i16(buf, r.error_code);
            if api_version >= 3 {
                enc_array_len(buf, r.members.len(), flexible);
                for m in &r.members {
                    enc_string(buf, m.member_id.as_bytes(), flexible);
                    enc_nullable_string(buf, &m.group_instance_id, flexible);
                    write_i16(buf, m.error_code);
                    enc_tagged(buf, flexible);
                }
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::OffsetCommit(r) => {
            if api_version >= 3 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::OffsetFetch(r) => {
            if api_version >= 3 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i64(buf, p.offset);
                    if api_version >= 5 {
                        write_i32(buf, p.committed_leader_epoch);
                    }
                    enc_nullable_string(buf, &p.metadata, flexible);
                    write_i16(buf, p.error_code);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            if api_version >= 2 {
                write_i16(buf, 0); // top-level error_code
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::CreateTopics(r) => {
            if api_version >= 2 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.name.as_bytes(), flexible);
                write_i16(buf, t.error_code);
                if api_version >= 1 {
                    enc_nullable_string(buf, &None, flexible); // error_message
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DeleteTopics(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.name.as_bytes(), flexible);
                write_i16(buf, t.error_code);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DescribeGroups(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.groups.len(), flexible);
            for g in &r.groups {
                write_i16(buf, g.error_code);
                enc_string(buf, g.group_id.as_bytes(), flexible);
                enc_string(buf, g.state.as_bytes(), flexible);
                enc_string(buf, g.protocol_type.as_bytes(), flexible);
                enc_string(buf, g.protocol.as_bytes(), flexible);
                enc_array_len(buf, g.members.len(), flexible);
                for m in &g.members {
                    enc_string(buf, m.member_id.as_bytes(), flexible);
                    if api_version >= 4 {
                        enc_nullable_string(buf, &m.group_instance_id, flexible);
                    }
                    enc_string(buf, m.client_id.as_bytes(), flexible);
                    enc_string(buf, m.client_host.as_bytes(), flexible);
                    enc_bytes(buf, &m.metadata, flexible);
                    enc_bytes(buf, &m.assignment, flexible);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::ListGroups(r) => {
            if api_version >= 1 {
                write_i32(buf, 0); // throttle_time_ms
            }
            write_i16(buf, r.error_code);
            enc_array_len(buf, r.groups.len(), flexible);
            for g in &r.groups {
                enc_string(buf, g.group_id.as_bytes(), flexible);
                enc_string(buf, g.protocol_type.as_bytes(), flexible);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::SaslHandshake(r) => {
            write_i16(buf, r.error_code);
            enc_array_len(buf, r.mechanisms.len(), flexible);
            for m in &r.mechanisms {
                enc_string(buf, m.as_bytes(), flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::SaslAuthenticate(r) => {
            write_i16(buf, r.error_code);
            enc_nullable_string(buf, &r.error_message, flexible);
            enc_bytes(buf, &r.auth_bytes, flexible);
            if api_version >= 1 {
                write_i64(buf, r.session_lifetime_ms);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DeleteRecords(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i64(buf, p.low_watermark);
                    write_i16(buf, p.error_code);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::InitProducerId(r) => {
            write_i32(buf, 0); // throttle_time_ms (always present)
            write_i16(buf, r.error_code);
            write_i64(buf, r.producer_id);
            write_i16(buf, r.producer_epoch);
            enc_tagged(buf, flexible);
        }

        KafkaResponse::AddPartitionsToTxn(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::AddOffsetsToTxn(r) => {
            write_i32(buf, 0); // throttle_time_ms
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }

        KafkaResponse::EndTxn(r) => {
            write_i32(buf, 0); // throttle_time_ms
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }

        KafkaResponse::TxnOffsetCommit(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DescribeConfigs(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.resources.len(), flexible);
            for res in &r.resources {
                write_i16(buf, res.error_code);
                enc_nullable_string(buf, &res.error_message, flexible);
                write_i8(buf, res.resource_type);
                enc_string(buf, res.resource_name.as_bytes(), flexible);
                enc_array_len(buf, res.configs.len(), flexible);
                for c in &res.configs {
                    enc_string(buf, c.name.as_bytes(), flexible);
                    enc_nullable_string(buf, &c.value, flexible);
                    buf.put_u8(c.read_only as u8);
                    buf.put_u8(c.is_default as u8);
                    buf.put_u8(c.is_sensitive as u8);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::AlterConfigs(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.resources.len(), flexible);
            for res in &r.resources {
                write_i16(buf, res.error_code);
                enc_nullable_string(buf, &res.error_message, flexible);
                write_i8(buf, res.resource_type);
                enc_string(buf, res.resource_name.as_bytes(), flexible);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::CreatePartitions(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.name.as_bytes(), flexible);
                write_i16(buf, t.error_code);
                enc_nullable_string(buf, &t.error_message, flexible);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DeleteGroups(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.results.len(), flexible);
            for g in &r.results {
                enc_string(buf, g.group_id.as_bytes(), flexible);
                write_i16(buf, g.error_code);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::OffsetDelete(r) => {
            write_i16(buf, r.error_code);
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::OffsetForLeaderEpoch(r) => {
            if api_version >= 2 {
                write_i32(buf, 0); // throttle_time_ms
            }
            enc_array_len(buf, r.topics.len(), flexible);
            for t in &r.topics {
                enc_string(buf, t.topic_name.as_bytes(), flexible);
                enc_array_len(buf, t.partitions.len(), flexible);
                for p in &t.partitions {
                    write_i16(buf, p.error_code);
                    write_i32(buf, p.partition_index);
                    if api_version >= 1 {
                        write_i32(buf, p.leader_epoch);
                    }
                    write_i64(buf, p.end_offset);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::IncrementalAlterConfigs(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.resources.len(), flexible);
            for res in &r.resources {
                write_i16(buf, res.error_code);
                enc_nullable_string(buf, &res.error_message, flexible);
                write_i8(buf, res.resource_type);
                enc_string(buf, res.resource_name.as_bytes(), flexible);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DescribeAcls(r) => {
            write_i32(buf, 0); // throttle_time_ms
            write_i16(buf, r.error_code);
            enc_nullable_string(buf, &r.error_message, flexible);
            enc_array_len(buf, r.resources.len(), flexible);
            for res in &r.resources {
                write_i8(buf, res.resource_type);
                enc_string(buf, res.resource_name.as_bytes(), flexible);
                write_i8(buf, res.pattern_type);
                enc_array_len(buf, res.acls.len(), flexible);
                for acl in &res.acls {
                    enc_string(buf, acl.principal.as_bytes(), flexible);
                    enc_string(buf, acl.host.as_bytes(), flexible);
                    write_i8(buf, acl.operation);
                    write_i8(buf, acl.permission_type);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::CreateAcls(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.results.len(), flexible);
            for res in &r.results {
                write_i16(buf, res.error_code);
                enc_nullable_string(buf, &res.error_message, flexible);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DeleteAcls(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.filter_results.len(), flexible);
            for fr in &r.filter_results {
                write_i16(buf, fr.error_code);
                enc_nullable_string(buf, &fr.error_message, flexible);
                enc_array_len(buf, fr.matching_acls.len(), flexible);
                for acl in &fr.matching_acls {
                    write_i16(buf, acl.error_code);
                    enc_nullable_string(buf, &acl.error_message, flexible);
                    write_i8(buf, acl.resource_type);
                    enc_string(buf, acl.resource_name.as_bytes(), flexible);
                    write_i8(buf, acl.resource_pattern_type);
                    enc_string(buf, acl.principal.as_bytes(), flexible);
                    enc_string(buf, acl.host.as_bytes(), flexible);
                    write_i8(buf, acl.operation);
                    write_i8(buf, acl.permission_type);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DescribeLogDirs(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.results.len(), flexible);
            for res in &r.results {
                write_i16(buf, res.error_code);
                enc_string(buf, res.log_dir.as_bytes(), flexible);
                enc_array_len(buf, res.topics.len(), flexible);
                for t in &res.topics {
                    enc_string(buf, t.name.as_bytes(), flexible);
                    enc_array_len(buf, t.partitions.len(), flexible);
                    for p in &t.partitions {
                        write_i32(buf, p.partition_index);
                        write_i64(buf, p.partition_size);
                        write_i64(buf, p.offset_lag);
                        write_i8(buf, if p.is_future_key { 1 } else { 0 });
                        enc_tagged(buf, flexible);
                    }
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DescribeUserScramCredentials(r) => {
            write_i32(buf, 0); // throttle_time_ms
            write_i16(buf, r.error_code);
            enc_nullable_string(buf, &r.error_message, flexible);
            enc_array_len(buf, r.results.len(), flexible);
            for res in &r.results {
                enc_string(buf, res.user.as_bytes(), flexible);
                write_i16(buf, res.error_code);
                enc_nullable_string(buf, &res.error_message, flexible);
                enc_array_len(buf, res.credential_infos.len(), flexible);
                for ci in &res.credential_infos {
                    write_i8(buf, ci.mechanism);
                    write_i32(buf, ci.iterations);
                    enc_tagged(buf, flexible);
                }
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::AlterUserScramCredentials(r) => {
            write_i32(buf, 0); // throttle_time_ms
            enc_array_len(buf, r.results.len(), flexible);
            for res in &r.results {
                enc_string(buf, res.user.as_bytes(), flexible);
                write_i16(buf, res.error_code);
                enc_nullable_string(buf, &res.error_message, flexible);
                enc_tagged(buf, flexible);
            }
            enc_tagged(buf, flexible);
        }

        KafkaResponse::DescribeCluster(r) => {
            write_i32(buf, 0); // throttle_time_ms
            write_i16(buf, r.error_code);
            enc_string(buf, r.cluster_id.as_bytes(), flexible);
            write_i32(buf, r.controller_id);
            enc_array_len(buf, r.brokers.len(), flexible);
            for b in &r.brokers {
                write_i32(buf, b.node_id);
                enc_string(buf, b.host.as_bytes(), flexible);
                write_i32(buf, b.port);
                enc_nullable_string(buf, &None, flexible); // rack
                enc_tagged(buf, flexible);
            }
            write_i32(buf, r.cluster_authorized_operations);
            enc_tagged(buf, flexible);
        }

        // Stub responses: write throttle_time_ms + error_code + tagged fields
        KafkaResponse::WriteTxnMarkers(r) => {
            write_i32(buf, 0); // throttle_time_ms
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::AlterReplicaLogDirs(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::CreateDelegationToken(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, 0); // throttle_time_ms
            enc_tagged(buf, flexible);
        }
        KafkaResponse::RenewDelegationToken(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, 0);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ExpireDelegationToken(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, 0);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DescribeDelegationToken(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, 0);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ElectLeaders(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::AlterPartitionReassignments(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ListPartitionReassignments(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DescribeClientQuotas(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::AlterClientQuotas(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DescribeQuorum(r) => {
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::UpdateFeatures(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DescribeProducers(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::UnregisterBroker(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DescribeTransactions(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ListTransactions(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ConsumerGroupHeartbeat(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ConsumerGroupDescribe(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::GetTelemetrySubscriptions(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::PushTelemetry(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ListConfigResources(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DescribeTopicPartitions(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ShareGroupHeartbeat(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ShareGroupDescribe(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ShareFetch(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ShareAcknowledge(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::AddRaftVoter(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::RemoveRaftVoter(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::InitializeShareGroupState(r) => {
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ReadShareGroupState(r) => {
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::WriteShareGroupState(r) => {
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DeleteShareGroupState(r) => {
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::ReadShareGroupStateSummary(r) => {
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::StreamsGroupHeartbeat(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::StreamsGroupDescribe(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DescribeShareGroupOffsets(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::AlterShareGroupOffsets(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
        KafkaResponse::DeleteShareGroupOffsets(r) => {
            write_i32(buf, 0);
            write_i16(buf, r.error_code);
            enc_tagged(buf, flexible);
        }
    }

    let payload_len = (buf.len() - payload_start) as i32;
    buf[len_pos..len_pos + 4].copy_from_slice(&payload_len.to_be_bytes());
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn write_test_i16(buf: &mut BytesMut, v: i16) {
        write_i16(buf, v);
    }
    fn write_test_i32(buf: &mut BytesMut, v: i32) {
        write_i32(buf, v);
    }
    fn write_test_string(buf: &mut BytesMut, s: &str) {
        write_string(buf, s.as_bytes());
    }

    #[test]
    fn test_varint_roundtrip() {
        for val in [0i32, 1, -1, 127, -128, 300, -300, i32::MAX, i32::MIN] {
            let mut buf = BytesMut::new();
            write_varint(&mut buf, val);
            let mut slice: &[u8] = &buf;
            let decoded = read_varint(&mut slice).unwrap();
            assert_eq!(val, decoded, "failed for {val}");
        }
    }

    #[test]
    fn test_varlong_roundtrip() {
        for val in [0i64, 1, -1, i64::MAX, i64::MIN, 100_000] {
            let mut buf = BytesMut::new();
            write_varlong(&mut buf, val);
            let mut slice: &[u8] = &buf;
            let decoded = read_varlong(&mut slice).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_string_roundtrip() {
        let mut buf = BytesMut::new();
        write_test_string(&mut buf, "hello");
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let decoded = cursor_read_string(&mut cur).unwrap();
        assert_eq!(&*decoded, "hello");
    }

    #[test]
    fn test_nullable_string_roundtrip() {
        // Non-null
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, &Some(WireString::from("test")));
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let decoded = cursor_read_nullable_string(&mut cur).unwrap();
        assert_eq!(decoded.as_deref(), Some("test"));

        // Null
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, &None);
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let decoded = cursor_read_nullable_string(&mut cur).unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn test_record_batch_roundtrip() {
        let batch = RecordBatch {
            base_offset: 42,
            partition_leader_epoch: 1,
            attributes: 0,
            last_offset_delta: 1,
            first_timestamp: 1000,
            max_timestamp: 1001,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![
                Record {
                    offset_delta: 0,
                    timestamp_delta: 0,
                    key: Some(Bytes::from_static(b"key1")),
                    value: Some(Bytes::from_static(b"value1")),
                    headers: vec![RecordHeader {
                        key: Bytes::from_static(b"h1"),
                        value: Bytes::from_static(b"v1"),
                    }],
                },
                Record {
                    offset_delta: 1,
                    timestamp_delta: 1,
                    key: None,
                    value: Some(Bytes::from_static(b"value2")),
                    headers: vec![],
                },
            ],
        };

        let mut buf = BytesMut::new();
        encode_record_batch(&batch, &mut buf);

        let decoded = decode_record_batch(&buf).unwrap();
        assert_eq!(decoded.base_offset, 42);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].key, Some(Bytes::from_static(b"key1")));
        assert_eq!(
            decoded.records[0].value,
            Some(Bytes::from_static(b"value1"))
        );
        assert_eq!(decoded.records[0].headers.len(), 1);
        assert_eq!(&decoded.records[0].headers[0].key[..], b"h1");
        assert_eq!(decoded.records[1].key, None);
        assert_eq!(
            decoded.records[1].value,
            Some(Bytes::from_static(b"value2"))
        );
    }

    #[test]
    fn test_record_batch_roundtrip_zero_copy() {
        let batch = RecordBatch {
            base_offset: 10,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 500,
            max_timestamp: 500,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                offset_delta: 0,
                timestamp_delta: 0,
                key: Some(Bytes::from_static(b"zerokey")),
                value: Some(Bytes::from_static(b"zeroval")),
                headers: vec![],
            }],
        };

        let mut buf = BytesMut::new();
        encode_record_batch(&batch, &mut buf);
        let data = buf.freeze();

        let decoded = decode_record_batch_bytes(data).unwrap();
        assert_eq!(decoded.base_offset, 10);
        assert_eq!(decoded.records[0].key.as_deref(), Some(b"zerokey".as_ref()));
        assert_eq!(
            decoded.records[0].value.as_deref(),
            Some(b"zeroval".as_ref())
        );
    }

    #[test]
    fn test_api_versions_response_encode() {
        let resp = KafkaResponse::ApiVersions(ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersionRange {
                api_key: 0,
                min_version: 0,
                max_version: 3,
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);

        assert_eq!(buf.len(), 4 + 4 + 2 + 4 + 6);

        let frame_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(frame_len as usize, buf.len() - 4);

        let cid = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(cid, 1);
    }

    #[test]
    fn test_api_versions_request_decode() {
        let mut frame = BytesMut::new();
        let mut payload = BytesMut::new();
        write_test_i16(&mut payload, 18);
        write_test_i16(&mut payload, 0);
        write_test_i32(&mut payload, 7);
        write_test_i16(&mut payload, -1);

        write_test_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (header, req, consumed) = decode_request(&frame).unwrap();
        assert_eq!(header.api_key, 18);
        assert_eq!(header.correlation_id, 7);
        assert_eq!(consumed, frame.len());
        assert!(matches!(req, KafkaRequest::ApiVersions));
    }

    #[test]
    fn test_metadata_request_decode() {
        let mut payload = BytesMut::new();
        write_test_i16(&mut payload, 3);
        write_test_i16(&mut payload, 0);
        write_test_i32(&mut payload, 42);
        write_test_i16(&mut payload, -1);
        write_test_i32(&mut payload, 1);
        write_test_string(&mut payload, "test-topic");

        let mut frame = BytesMut::new();
        write_test_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (header, req, _) = decode_request(&frame).unwrap();
        assert_eq!(header.correlation_id, 42);
        match req {
            KafkaRequest::Metadata(m) => {
                assert_eq!(m.topics.as_ref().unwrap().len(), 1);
                assert_eq!(&*m.topics.as_ref().unwrap()[0], "test-topic");
            }
            _ => panic!("expected Metadata"),
        }
    }

    #[test]
    fn test_incomplete_frame() {
        let data = [0u8, 0, 0, 20];
        let result = decode_request(&data);
        assert!(matches!(result, Err(CodecError::Incomplete)));
    }

    #[test]
    fn test_produce_request_decode() {
        let mut payload = BytesMut::new();
        write_test_i16(&mut payload, 0);
        write_test_i16(&mut payload, 0);
        write_test_i32(&mut payload, 99);
        write_test_i16(&mut payload, -1);
        write_test_i16(&mut payload, -1);
        write_test_i32(&mut payload, 5000);
        write_test_i32(&mut payload, 1);
        write_test_string(&mut payload, "my-topic");
        write_test_i32(&mut payload, 1);
        write_test_i32(&mut payload, 0);
        let mut batch_buf = BytesMut::new();
        let batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 1000,
            max_timestamp: 1000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(b"hello")),
                headers: vec![],
            }],
        };
        encode_record_batch(&batch, &mut batch_buf);
        write_bytes(&mut payload, &batch_buf);

        let mut frame = BytesMut::new();
        write_test_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (header, req, _) = decode_request(&frame).unwrap();
        assert_eq!(header.correlation_id, 99);
        match req {
            KafkaRequest::Produce(p) => {
                assert_eq!(p.acks, -1);
                assert_eq!(p.topics.len(), 1);
                assert_eq!(&*p.topics[0].topic_name, "my-topic");
                assert!(p.topics[0].partitions[0].record_set.is_some());
            }
            _ => panic!("expected Produce"),
        }
    }

    #[test]
    fn test_metadata_response_roundtrip() {
        let resp = KafkaResponse::Metadata(MetadataResponse {
            brokers: vec![BrokerMeta {
                node_id: 1,
                host: WireString::from("localhost"),
                port: 9092,
            }],
            cluster_id: WireString::from_static("bisque-mq"),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: WireString::from("test"),
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader: 1,
                    replicas: smallvec::smallvec![1],
                    isr: smallvec::smallvec![1],
                }],
            }],
        });

        let mut buf = BytesMut::new();
        encode_response(5, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);

        let frame_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(frame_len as usize, buf.len() - 4);
        let cid = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(cid, 5);
    }

    #[test]
    fn test_varint_size_computation() {
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(1), 1);
        assert_eq!(varint_size(-1), 1);
        assert_eq!(varint_size(63), 1);
        assert_eq!(varint_size(64), 2);
        assert_eq!(varint_size(-65), 2);
    }

    #[test]
    fn test_encode_record_no_double_buffer() {
        let record = Record {
            offset_delta: 0,
            timestamp_delta: 0,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            headers: vec![RecordHeader {
                key: Bytes::from_static(b"hk"),
                value: Bytes::from_static(b"hv"),
            }],
        };

        let mut buf = BytesMut::new();
        encode_record(&mut buf, &record);

        // Verify it decodes correctly
        let mut slice: &[u8] = &buf;
        let decoded = decode_record(&mut slice).unwrap();
        assert_eq!(decoded.key, Some(Bytes::from_static(b"k")));
        assert_eq!(decoded.value, Some(Bytes::from_static(b"v")));
        assert_eq!(decoded.headers.len(), 1);
    }

    // =========================================================================
    // peek_frame_size tests
    // =========================================================================

    #[test]
    fn test_peek_frame_size_valid() {
        let data = 100i32.to_be_bytes();
        let size = peek_frame_size(&data).unwrap();
        assert_eq!(size, 104); // 4 + 100
    }

    #[test]
    fn test_peek_frame_size_incomplete() {
        assert!(matches!(
            peek_frame_size(&[0, 0]),
            Err(CodecError::Incomplete)
        ));
        assert!(matches!(peek_frame_size(&[]), Err(CodecError::Incomplete)));
        assert!(matches!(
            peek_frame_size(&[0, 0, 0]),
            Err(CodecError::Incomplete)
        ));
    }

    #[test]
    fn test_peek_frame_size_too_large() {
        // MAX_FRAME_SIZE is 100MB = 104857600
        let huge = (MAX_FRAME_SIZE as i32 + 1).to_be_bytes();
        assert!(matches!(
            peek_frame_size(&huge),
            Err(CodecError::FrameTooLarge(_))
        ));
    }

    #[test]
    fn test_peek_frame_size_zero() {
        let data = 0i32.to_be_bytes();
        let size = peek_frame_size(&data).unwrap();
        assert_eq!(size, 4); // 4 + 0
    }

    // =========================================================================
    // decode_request_bytes tests
    // =========================================================================

    #[test]
    fn test_decode_request_bytes_api_versions() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 18); // ApiVersions
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 42);
        write_i16(&mut payload, -1); // null client_id

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (header, req) = decode_request_bytes(frame.freeze()).unwrap();
        assert_eq!(header.correlation_id, 42);
        assert!(matches!(req, KafkaRequest::ApiVersions));
    }

    #[test]
    fn test_decode_request_bytes_unsupported_api_key() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 999); // invalid api key
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 1);
        write_i16(&mut payload, -1);

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let result = decode_request_bytes(frame.freeze());
        assert!(matches!(result, Err(CodecError::UnsupportedApiKey(999))));
    }

    #[test]
    fn test_decode_request_bytes_truncated() {
        let mut frame = BytesMut::new();
        write_i32(&mut frame, 2); // claims 2 bytes of payload
        write_i16(&mut frame, 18); // only api_key, missing rest

        let result = decode_request_bytes(frame.freeze());
        assert!(matches!(result, Err(CodecError::UnexpectedEof)));
    }

    #[test]
    fn test_decode_request_bytes_fetch() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 1); // Fetch
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 10);
        write_i16(&mut payload, -1); // null client_id
        write_i32(&mut payload, -1); // replica_id
        write_i32(&mut payload, 500); // max_wait_ms
        write_i32(&mut payload, 1); // min_bytes
        write_i32(&mut payload, 0); // topic count

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (header, req) = decode_request_bytes(frame.freeze()).unwrap();
        assert_eq!(header.correlation_id, 10);
        match req {
            KafkaRequest::Fetch(f) => {
                assert_eq!(f.max_wait_ms, 500);
                assert_eq!(f.min_bytes, 1);
                assert!(f.topics.is_empty());
            }
            _ => panic!("expected Fetch"),
        }
    }

    #[test]
    fn test_decode_request_bytes_heartbeat() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 12); // Heartbeat
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 5);
        write_i16(&mut payload, -1);
        write_string(&mut payload, b"my-group");
        write_i32(&mut payload, 3); // generation_id
        write_string(&mut payload, b"member-1");

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        match req {
            KafkaRequest::Heartbeat(h) => {
                assert_eq!(h.group_id.as_str(), "my-group");
                assert_eq!(h.generation_id, 3);
                assert_eq!(h.member_id.as_str(), "member-1");
            }
            _ => panic!("expected Heartbeat"),
        }
    }

    #[test]
    fn test_decode_request_bytes_find_coordinator() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 10); // FindCoordinator
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 8);
        write_i16(&mut payload, -1);
        write_string(&mut payload, b"my-group");

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        match req {
            KafkaRequest::FindCoordinator(f) => {
                assert_eq!(f.key.as_str(), "my-group");
            }
            _ => panic!("expected FindCoordinator"),
        }
    }

    #[test]
    fn test_decode_request_bytes_sasl_handshake() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 17); // SaslHandshake
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 1);
        write_i16(&mut payload, -1);
        write_string(&mut payload, b"PLAIN");

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        match req {
            KafkaRequest::SaslHandshake(s) => {
                assert_eq!(s.mechanism.as_str(), "PLAIN");
            }
            _ => panic!("expected SaslHandshake"),
        }
    }

    #[test]
    fn test_decode_request_bytes_init_producer_id() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 22); // InitProducerId
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 1);
        write_i16(&mut payload, -1); // null client_id
        write_i16(&mut payload, -1); // null transactional_id
        write_i32(&mut payload, 5000); // txn timeout

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        match req {
            KafkaRequest::InitProducerId(r) => {
                assert!(r.transactional_id.is_none());
                assert_eq!(r.transaction_timeout_ms, 5000);
            }
            _ => panic!("expected InitProducerId"),
        }
    }

    // =========================================================================
    // Response encode tests for various response types
    // =========================================================================

    #[test]
    fn test_encode_heartbeat_response() {
        let resp = KafkaResponse::Heartbeat(HeartbeatResponse { error_code: 0 });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        // frame_len(4) + correlation_id(4) + error_code(2) = 10
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn test_encode_find_coordinator_response() {
        let resp = KafkaResponse::FindCoordinator(FindCoordinatorResponse {
            error_code: 0,
            node_id: 1,
            host: WireString::from("localhost"),
            port: 9092,
        });
        let mut buf = BytesMut::new();
        encode_response(2, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
        let cid = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(cid, 2);
    }

    #[test]
    fn test_encode_produce_response() {
        let resp = KafkaResponse::Produce(ProduceResponse {
            topics: vec![ProduceTopicResponse {
                topic_name: WireString::from("t"),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    base_offset: 42,
                    ..Default::default()
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(3, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_fetch_response() {
        let resp = KafkaResponse::Fetch(FetchResponse {
            topics: vec![FetchTopicResponse {
                topic_name: WireString::from("t"),
                partitions: vec![FetchPartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    high_watermark: 10,
                    record_set: Bytes::new(),
                    ..Default::default()
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(4, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_list_offsets_response() {
        let resp = KafkaResponse::ListOffsets(ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                topic_name: WireString::from("t"),
                partitions: vec![ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    timestamp: 1000,
                    offset: 5,
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(5, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_join_group_response() {
        let resp = KafkaResponse::JoinGroup(JoinGroupResponse {
            error_code: 0,
            generation_id: 1,
            protocol_name: WireString::from("range"),
            leader: WireString::from("leader-1"),
            member_id: WireString::from("member-1"),
            members: vec![JoinGroupMember {
                member_id: WireString::from("member-1"),
                metadata: Bytes::from_static(b"meta"),
                ..Default::default()
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(6, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_sync_group_response() {
        let resp = KafkaResponse::SyncGroup(SyncGroupResponse {
            error_code: 0,
            assignment: Bytes::from_static(b"assignment-data"),
        });
        let mut buf = BytesMut::new();
        encode_response(7, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_leave_group_response() {
        let resp = KafkaResponse::LeaveGroup(LeaveGroupResponse {
            error_code: 0,
            members: Vec::new(),
        });
        let mut buf = BytesMut::new();
        encode_response(8, 0, 0, &resp, &mut buf);
        assert_eq!(buf.len(), 10); // frame_len(4) + cid(4) + error(2)
    }

    #[test]
    fn test_encode_create_topics_response() {
        let resp = KafkaResponse::CreateTopics(CreateTopicsResponse {
            topics: vec![CreateTopicResponse {
                name: WireString::from("new-topic"),
                error_code: 0,
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(9, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_delete_topics_response() {
        let resp = KafkaResponse::DeleteTopics(DeleteTopicsResponse {
            topics: vec![DeleteTopicResponse {
                name: WireString::from("del-topic"),
                error_code: 0,
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(10, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_list_groups_response() {
        let resp = KafkaResponse::ListGroups(ListGroupsResponse {
            error_code: 0,
            groups: vec![ListedGroup {
                group_id: WireString::from("g1"),
                protocol_type: WireString::from("consumer"),
                ..Default::default()
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(11, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_describe_groups_response() {
        let resp = KafkaResponse::DescribeGroups(DescribeGroupsResponse {
            groups: vec![DescribedGroup {
                error_code: 0,
                group_id: WireString::from("g1"),
                state: WireString::from_static("Stable"),
                protocol_type: WireString::from("consumer"),
                protocol: WireString::from("range"),
                members: vec![],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(12, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_sasl_handshake_response() {
        let resp = KafkaResponse::SaslHandshake(SaslHandshakeResponse {
            error_code: 0,
            mechanisms: vec![WireString::from_static("PLAIN")],
        });
        let mut buf = BytesMut::new();
        encode_response(13, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_sasl_authenticate_response() {
        let resp = KafkaResponse::SaslAuthenticate(SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: Bytes::new(),
            ..Default::default()
        });
        let mut buf = BytesMut::new();
        encode_response(14, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    #[test]
    fn test_encode_init_producer_id_response() {
        let resp = KafkaResponse::InitProducerId(InitProducerIdResponse {
            error_code: 0,
            producer_id: 42,
            producer_epoch: 0,
        });
        let mut buf = BytesMut::new();
        encode_response(15, 0, 0, &resp, &mut buf);
        // frame(4) + cid(4) + throttle(4) + error(2) + pid(8) + epoch(2) = 24
        assert_eq!(buf.len(), 24);
    }

    #[test]
    fn test_encode_end_txn_response() {
        let resp = KafkaResponse::EndTxn(EndTxnResponse { error_code: 0 });
        let mut buf = BytesMut::new();
        encode_response(16, 0, 0, &resp, &mut buf);
        // frame(4) + cid(4) + throttle(4) + error(2) = 14
        assert_eq!(buf.len(), 14);
    }

    #[test]
    fn test_encode_delete_groups_response() {
        let resp = KafkaResponse::DeleteGroups(DeleteGroupsResponse {
            results: vec![DeleteGroupResult {
                group_id: WireString::from("g1"),
                error_code: 0,
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(17, 0, 0, &resp, &mut buf);
        assert!(buf.len() > 8);
    }

    // =========================================================================
    // Varint/Varlong edge cases with cursor-based decode
    // =========================================================================

    #[test]
    fn test_cursor_varint_boundary_values() {
        for val in [
            0i32,
            1,
            -1,
            63,
            -64,
            64,
            -65,
            8191,
            -8192,
            i32::MAX,
            i32::MIN,
        ] {
            let mut buf = BytesMut::new();
            write_varint(&mut buf, val);
            let frame = buf.freeze();
            let mut cur = BytesCursor::new(frame);
            let decoded = cursor_read_varint(&mut cur).unwrap();
            assert_eq!(val, decoded, "cursor varint failed for {val}");
        }
    }

    #[test]
    fn test_cursor_varlong_boundary_values() {
        for val in [0i64, 1, -1, 127, -128, i64::MAX, i64::MIN, 100_000_000] {
            let mut buf = BytesMut::new();
            write_varlong(&mut buf, val);
            let frame = buf.freeze();
            let mut cur = BytesCursor::new(frame);
            let decoded = cursor_read_varlong(&mut cur).unwrap();
            assert_eq!(val, decoded, "cursor varlong failed for {val}");
        }
    }

    #[test]
    fn test_cursor_varint_empty_input() {
        let frame = Bytes::new();
        let mut cur = BytesCursor::new(frame);
        let result = cursor_read_varint(&mut cur);
        assert!(matches!(result, Err(CodecError::UnexpectedEof)));
    }

    #[test]
    fn test_cursor_read_nullable_string_valid() {
        let mut buf = BytesMut::new();
        write_string(&mut buf, b"hello");
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let s = cursor_read_nullable_string(&mut cur).unwrap();
        assert_eq!(s.as_deref(), Some("hello"));
    }

    #[test]
    fn test_cursor_read_nullable_string_null() {
        let mut buf = BytesMut::new();
        write_i16(&mut buf, -1); // null marker
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let s = cursor_read_nullable_string(&mut cur).unwrap();
        assert!(s.is_none());
    }

    #[test]
    fn test_cursor_read_nullable_bytes_null() {
        let mut buf = BytesMut::new();
        write_i32(&mut buf, -1); // null marker
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let b = cursor_read_nullable_bytes(&mut cur).unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_cursor_read_nullable_bytes_valid() {
        let mut buf = BytesMut::new();
        write_bytes(&mut buf, b"data");
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let b = cursor_read_nullable_bytes(&mut cur).unwrap();
        assert_eq!(b.as_deref(), Some(b"data".as_ref()));
    }

    #[test]
    fn test_cursor_read_varint_bytes_null() {
        let mut buf = BytesMut::new();
        write_varint(&mut buf, -1);
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let b = cursor_read_varint_bytes(&mut cur).unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_cursor_read_varint_bytes_valid() {
        let mut buf = BytesMut::new();
        write_varint(&mut buf, 3);
        buf.extend_from_slice(b"abc");
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let b = cursor_read_varint_bytes(&mut cur).unwrap();
        assert_eq!(b.as_deref(), Some(b"abc".as_ref()));
    }

    #[test]
    fn test_cursor_read_varint_bytes_raw_valid() {
        let mut buf = BytesMut::new();
        write_varint(&mut buf, 5);
        buf.extend_from_slice(b"hello");
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let b = cursor_read_varint_bytes_raw(&mut cur).unwrap();
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn test_cursor_read_varint_bytes_raw_negative_len() {
        let mut buf = BytesMut::new();
        write_varint(&mut buf, -1);
        let frame = buf.freeze();
        let mut cur = BytesCursor::new(frame);
        let result = cursor_read_varint_bytes_raw(&mut cur);
        assert!(matches!(result, Err(CodecError::UnexpectedEof)));
    }

    // =========================================================================
    // Record batch with multiple records
    // =========================================================================

    #[test]
    fn test_record_batch_roundtrip_no_records() {
        let batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![],
        };

        let mut buf = BytesMut::new();
        encode_record_batch(&batch, &mut buf);
        let data = buf.freeze();
        let decoded = decode_record_batch_bytes(data).unwrap();
        assert!(decoded.records.is_empty());
    }

    #[test]
    fn test_record_batch_roundtrip_null_key_value() {
        let batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: None,
                headers: vec![],
            }],
        };

        let mut buf = BytesMut::new();
        encode_record_batch(&batch, &mut buf);
        let data = buf.freeze();
        let decoded = decode_record_batch_bytes(data).unwrap();
        assert_eq!(decoded.records.len(), 1);
        assert!(decoded.records[0].key.is_none());
        assert!(decoded.records[0].value.is_none());
    }

    #[test]
    fn test_record_batch_roundtrip_with_headers() {
        let batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(b"val")),
                headers: vec![
                    RecordHeader {
                        key: Bytes::from_static(b"h1"),
                        value: Bytes::from_static(b"v1"),
                    },
                    RecordHeader {
                        key: Bytes::from_static(b"h2"),
                        value: Bytes::from_static(b"v2"),
                    },
                ],
            }],
        };

        let mut buf = BytesMut::new();
        encode_record_batch(&batch, &mut buf);
        let data = buf.freeze();
        let decoded = decode_record_batch_bytes(data).unwrap();
        assert_eq!(decoded.records[0].headers.len(), 2);
        assert_eq!(&decoded.records[0].headers[0].key[..], b"h1");
        assert_eq!(&decoded.records[0].headers[1].key[..], b"h2");
    }

    #[test]
    fn test_record_encode_decode_roundtrip() {
        let record = Record {
            offset_delta: 5,
            timestamp_delta: 100,
            key: Some(Bytes::from_static(b"mykey")),
            value: Some(Bytes::from_static(b"myvalue")),
            headers: vec![RecordHeader {
                key: Bytes::from_static(b"hdr"),
                value: Bytes::from_static(b"val"),
            }],
        };

        let mut buf = BytesMut::new();
        encode_record(&mut buf, &record);

        let mut slice: &[u8] = &buf;
        let decoded = decode_record(&mut slice).unwrap();
        assert_eq!(decoded.offset_delta, 5);
        assert_eq!(decoded.timestamp_delta, 100);
        assert_eq!(decoded.key.as_deref(), Some(b"mykey".as_ref()));
        assert_eq!(decoded.value.as_deref(), Some(b"myvalue".as_ref()));
        assert_eq!(decoded.headers.len(), 1);
    }

    // =========================================================================
    // CodecError display
    // =========================================================================

    #[test]
    fn test_codec_error_display() {
        assert_eq!(
            format!("{}", CodecError::Incomplete),
            "incomplete frame: need more data"
        );
        assert_eq!(
            format!("{}", CodecError::UnsupportedApiKey(99)),
            "unsupported API key: 99"
        );
        assert_eq!(
            format!("{}", CodecError::FrameTooLarge(999)),
            "frame too large: 999 bytes"
        );
        assert_eq!(
            format!("{}", CodecError::InvalidUtf8),
            "invalid utf-8 in string"
        );
        assert_eq!(
            format!("{}", CodecError::UnexpectedEof),
            "unexpected end of frame"
        );
    }

    // =========================================================================
    // Decode all request types via decode_request_bytes
    // =========================================================================

    #[test]
    fn test_decode_list_offsets_request() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 2); // ListOffsets
        write_i16(&mut payload, 1); // version 1 (no max_num_offsets field)
        write_i32(&mut payload, 1);
        write_i16(&mut payload, -1);
        write_i32(&mut payload, -1); // replica_id
        write_i32(&mut payload, 1); // topic count
        write_string(&mut payload, b"topic-a");
        write_i32(&mut payload, 1); // partition count
        write_i32(&mut payload, 0); // partition_index
        write_i64(&mut payload, -1); // timestamp (latest)

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        match req {
            KafkaRequest::ListOffsets(lo) => {
                assert_eq!(lo.topics.len(), 1);
                assert_eq!(lo.topics[0].topic_name.as_str(), "topic-a");
                assert_eq!(lo.topics[0].partitions[0].timestamp, -1);
            }
            _ => panic!("expected ListOffsets"),
        }
    }

    #[test]
    fn test_decode_leave_group_request() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 13); // LeaveGroup
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 1);
        write_i16(&mut payload, -1);
        write_string(&mut payload, b"my-group");
        write_string(&mut payload, b"member-1");

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        match req {
            KafkaRequest::LeaveGroup(l) => {
                assert_eq!(l.group_id.as_str(), "my-group");
                assert_eq!(l.member_id.as_str(), "member-1");
            }
            _ => panic!("expected LeaveGroup"),
        }
    }

    #[test]
    fn test_decode_describe_groups_request() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 15); // DescribeGroups
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 1);
        write_i16(&mut payload, -1);
        write_i32(&mut payload, 2); // 2 groups
        write_string(&mut payload, b"g1");
        write_string(&mut payload, b"g2");

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        match req {
            KafkaRequest::DescribeGroups(d) => {
                assert_eq!(d.group_ids.len(), 2);
                assert_eq!(d.group_ids[0].as_str(), "g1");
                assert_eq!(d.group_ids[1].as_str(), "g2");
            }
            _ => panic!("expected DescribeGroups"),
        }
    }

    #[test]
    fn test_decode_list_groups_request() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 16); // ListGroups
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 1);
        write_i16(&mut payload, -1);

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (_, req) = decode_request_bytes(frame.freeze()).unwrap();
        assert!(matches!(req, KafkaRequest::ListGroups));
    }

    // =========================================================================
    // Response encode tests for remaining types
    // =========================================================================

    #[test]
    fn test_encode_offset_commit_response() {
        let resp = KafkaResponse::OffsetCommit(OffsetCommitResponse {
            topics: vec![OffsetCommitTopicResponse {
                topic_name: WireString::from("t1"),
                partitions: vec![OffsetCommitPartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_offset_fetch_response() {
        let resp = KafkaResponse::OffsetFetch(OffsetFetchResponse {
            topics: vec![OffsetFetchTopicResponse {
                topic_name: WireString::from("t1"),
                partitions: vec![OffsetFetchPartitionResponse {
                    partition_index: 0,
                    offset: 42,
                    metadata: Some(WireString::from("meta")),
                    error_code: 0,
                    ..Default::default()
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_offset_fetch_response_null_metadata() {
        let resp = KafkaResponse::OffsetFetch(OffsetFetchResponse {
            topics: vec![OffsetFetchTopicResponse {
                topic_name: WireString::from("t1"),
                partitions: vec![OffsetFetchPartitionResponse {
                    partition_index: 0,
                    offset: -1,
                    metadata: None,
                    error_code: 0,
                    ..Default::default()
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_add_partitions_to_txn_response() {
        let resp = KafkaResponse::AddPartitionsToTxn(AddPartitionsToTxnResponse {
            topics: vec![AddPartitionsToTxnTopicResponse {
                topic_name: WireString::from("txn-topic"),
                partitions: vec![
                    AddPartitionsToTxnPartitionResponse {
                        partition_index: 0,
                        error_code: 0,
                    },
                    AddPartitionsToTxnPartitionResponse {
                        partition_index: 1,
                        error_code: 0,
                    },
                ],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_add_offsets_to_txn_response() {
        let resp = KafkaResponse::AddOffsetsToTxn(AddOffsetsToTxnResponse { error_code: 0 });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_txn_offset_commit_response() {
        let resp = KafkaResponse::TxnOffsetCommit(TxnOffsetCommitResponse {
            topics: vec![TxnOffsetCommitTopicResponse {
                topic_name: WireString::from("t1"),
                partitions: vec![TxnOffsetCommitPartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_delete_records_response() {
        let resp = KafkaResponse::DeleteRecords(DeleteRecordsResponse {
            topics: vec![DeleteRecordsTopicResponse {
                topic_name: WireString::from("t1"),
                partitions: vec![DeleteRecordsPartitionResponse {
                    partition_index: 0,
                    low_watermark: 42,
                    error_code: 0,
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_describe_configs_response() {
        let resp = KafkaResponse::DescribeConfigs(DescribeConfigsResponse {
            resources: vec![DescribeConfigsResourceResult {
                error_code: 0,
                error_message: None,
                resource_type: 2,
                resource_name: WireString::from("my-topic"),
                configs: vec![DescribeConfigEntry {
                    name: WireString::from_static("cleanup.policy"),
                    value: Some(WireString::from_static("delete")),
                    read_only: false,
                    is_default: true,
                    is_sensitive: false,
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_describe_configs_response_with_error() {
        let resp = KafkaResponse::DescribeConfigs(DescribeConfigsResponse {
            resources: vec![DescribeConfigsResourceResult {
                error_code: 3,
                error_message: Some(WireString::from("oops")),
                resource_type: 99,
                resource_name: WireString::from("x"),
                configs: vec![],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_alter_configs_response() {
        let resp = KafkaResponse::AlterConfigs(AlterConfigsResponse {
            resources: vec![AlterConfigsResourceResult {
                error_code: 0,
                error_message: None,
                resource_type: 2,
                resource_name: WireString::from("t1"),
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_create_partitions_response() {
        let resp = KafkaResponse::CreatePartitions(CreatePartitionsResponse {
            topics: vec![CreatePartitionsTopicResponse {
                name: WireString::from("t1"),
                error_code: 0,
                error_message: None,
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_create_partitions_response_with_error() {
        let resp = KafkaResponse::CreatePartitions(CreatePartitionsResponse {
            topics: vec![CreatePartitionsTopicResponse {
                name: WireString::from("t1"),
                error_code: 37, // InvalidPartitions
                error_message: Some(WireString::from("bad")),
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_offset_delete_response() {
        let resp = KafkaResponse::OffsetDelete(OffsetDeleteResponse {
            error_code: 0,
            topics: vec![OffsetDeleteTopicResponse {
                topic_name: WireString::from("t1"),
                partitions: vec![OffsetDeletePartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_metadata_response() {
        let resp = KafkaResponse::Metadata(MetadataResponse {
            brokers: vec![BrokerMeta {
                node_id: 1,
                host: WireString::from("localhost"),
                port: 9092,
            }],
            cluster_id: WireString::from_static("bisque-mq"),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: WireString::from("t1"),
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader: 1,
                    replicas: smallvec::smallvec![1],
                    isr: smallvec::smallvec![1],
                }],
            }],
        });
        let mut buf = BytesMut::new();
        encode_response(1, 0, 0, &resp, &mut buf);
        assert!(!buf.is_empty());
    }

    // =========================================================================
    // Decompression tests
    // =========================================================================

    #[test]
    fn test_decompress_none_passthrough() {
        // COMPRESSION_NONE is handled at the caller level (not by decompress_records).
        // decompress_records treats 0 as unknown.
        let data = b"hello";
        let result = decompress_records(COMPRESSION_NONE, data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_gzip_roundtrip() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let original = b"test data for gzip compression";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let decompressed = decompress_records(COMPRESSION_GZIP, &compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_snappy_raw_roundtrip() {
        let original = b"test data for snappy raw compression!!";
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(original).unwrap();

        let decompressed = decompress_records(COMPRESSION_SNAPPY, &compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_snappy_xerial_roundtrip() {
        let original = b"test data for snappy xerial framing!!";
        let mut encoder = snap::raw::Encoder::new();
        let compressed_chunk = encoder.compress_vec(original).unwrap();

        // Build Xerial framed snappy
        let mut framed = Vec::new();
        // Magic
        framed.extend_from_slice(&[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00]);
        // Version
        framed.extend_from_slice(&[0, 0, 0, 1]);
        // Compat
        framed.extend_from_slice(&[0, 0, 0, 1]);
        // Chunk: [len:4][data]
        framed.extend_from_slice(&(compressed_chunk.len() as u32).to_be_bytes());
        framed.extend_from_slice(&compressed_chunk);

        let decompressed = decompress_records(COMPRESSION_SNAPPY, &framed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_snappy_xerial_multiple_chunks() {
        let chunk1 = b"first chunk data";
        let chunk2 = b"second chunk data";
        let mut encoder = snap::raw::Encoder::new();
        let c1 = encoder.compress_vec(chunk1).unwrap();
        let c2 = encoder.compress_vec(chunk2).unwrap();

        let mut framed = Vec::new();
        framed.extend_from_slice(&[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00]);
        framed.extend_from_slice(&[0, 0, 0, 1]);
        framed.extend_from_slice(&[0, 0, 0, 1]);
        framed.extend_from_slice(&(c1.len() as u32).to_be_bytes());
        framed.extend_from_slice(&c1);
        framed.extend_from_slice(&(c2.len() as u32).to_be_bytes());
        framed.extend_from_slice(&c2);

        let decompressed = decompress_records(COMPRESSION_SNAPPY, &framed).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(chunk1);
        expected.extend_from_slice(chunk2);
        assert_eq!(decompressed, expected);
    }

    #[test]
    fn test_decompress_snappy_xerial_truncated_header() {
        let mut framed = Vec::new();
        framed.extend_from_slice(&[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00]);
        framed.extend_from_slice(&[0, 0, 0, 1]);
        framed.extend_from_slice(&[0, 0, 0, 1]);
        // Add partial chunk header (only 2 bytes instead of 4)
        framed.extend_from_slice(&[0, 0]);

        let result = decompress_records(COMPRESSION_SNAPPY, &framed);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_snappy_xerial_truncated_data() {
        let mut framed = Vec::new();
        framed.extend_from_slice(&[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00]);
        framed.extend_from_slice(&[0, 0, 0, 1]);
        framed.extend_from_slice(&[0, 0, 0, 1]);
        // Chunk header says 100 bytes but only 5 follow
        framed.extend_from_slice(&100u32.to_be_bytes());
        framed.extend_from_slice(&[1, 2, 3, 4, 5]);

        let result = decompress_records(COMPRESSION_SNAPPY, &framed);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_lz4_roundtrip() {
        use lz4_flex::frame::FrameEncoder;
        use std::io::Write;

        let original = b"test data for lz4 frame compression";
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let decompressed = decompress_records(COMPRESSION_LZ4, &compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_zstd_roundtrip() {
        let original = b"test data for zstd compression roundtrip!";
        let compressed = zstd::stream::encode_all(&original[..], 3).unwrap();

        let decompressed = decompress_records(COMPRESSION_ZSTD, &compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_unknown_compression() {
        let result = decompress_records(99, b"data");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{err}").contains("unknown compression"));
    }

    #[test]
    fn test_decompress_gzip_invalid_data() {
        let result = decompress_records(COMPRESSION_GZIP, b"not gzip data");
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_zstd_invalid_data() {
        let result = decompress_records(COMPRESSION_ZSTD, b"not zstd data");
        assert!(result.is_err());
    }

    // =========================================================================
    // varint_size / varlong_size tests
    // =========================================================================

    #[test]
    fn test_varint_size_all_ranges() {
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(1), 1);
        assert_eq!(varint_size(-1), 1);
        assert_eq!(varint_size(63), 1);
        assert_eq!(varint_size(-64), 1);
        assert_eq!(varint_size(64), 2);
        assert_eq!(varint_size(-65), 2);
        assert_eq!(varint_size(i32::MAX), 5);
        assert_eq!(varint_size(i32::MIN), 5);
    }

    #[test]
    fn test_varlong_size_all_ranges() {
        assert_eq!(varlong_size(0), 1);
        assert_eq!(varlong_size(1), 1);
        assert_eq!(varlong_size(-1), 1);
        assert_eq!(varlong_size(63), 1);
        assert_eq!(varlong_size(64), 2);
        assert_eq!(varlong_size(i64::MAX), 10);
        assert_eq!(varlong_size(i64::MIN), 10);
    }
}
