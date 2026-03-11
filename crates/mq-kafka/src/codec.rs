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
    if compression != 0 {
        return Err(CodecError::InvalidRecordBatch(format!(
            "compression type {} not yet supported",
            compression
        )));
    }

    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        records.push(decode_record(buf)?);
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
    if compression != 0 {
        return Err(CodecError::InvalidRecordBatch(format!(
            "compression type {} not yet supported",
            compression
        )));
    }

    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        records.push(decode_record_cursor(&mut cur)?);
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
    let client_id = cursor_read_nullable_string(&mut cur)?;

    let header = RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    };

    let api = ApiKey::from_i16(api_key).ok_or(CodecError::UnsupportedApiKey(api_key))?;

    let request = match api {
        ApiKey::ApiVersions => KafkaRequest::ApiVersions,

        ApiKey::Metadata => {
            let count = cursor_read_array_len(&mut cur)?;
            let topics = if count < 0 {
                None
            } else {
                let mut v = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    v.push(cursor_read_string(&mut cur)?);
                }
                Some(v)
            };
            KafkaRequest::Metadata(MetadataRequest { topics })
        }

        ApiKey::Produce => {
            let acks = cursor_read_i16(&mut cur)?;
            let timeout_ms = cursor_read_i32(&mut cur)?;
            let topic_count = cursor_read_array_len(&mut cur)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = cursor_read_string(&mut cur)?;
                let part_count = cursor_read_array_len(&mut cur)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let record_set = cursor_read_nullable_bytes(&mut cur)?;
                    partitions.push(ProducePartitionData {
                        partition_index,
                        record_set,
                    });
                }
                topics.push(ProduceTopicData {
                    topic_name,
                    partitions,
                });
            }
            KafkaRequest::Produce(ProduceRequest {
                acks,
                timeout_ms,
                topics,
            })
        }

        ApiKey::Fetch => {
            let _replica_id = cursor_read_i32(&mut cur)?;
            let max_wait_ms = cursor_read_i32(&mut cur)?;
            let min_bytes = cursor_read_i32(&mut cur)?;
            let topic_count = cursor_read_array_len(&mut cur)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = cursor_read_string(&mut cur)?;
                let part_count = cursor_read_array_len(&mut cur)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let fetch_offset = cursor_read_i64(&mut cur)?;
                    let max_bytes = cursor_read_i32(&mut cur)?;
                    partitions.push(FetchPartitionData {
                        partition_index,
                        fetch_offset,
                        max_bytes,
                    });
                }
                topics.push(FetchTopicData {
                    topic_name,
                    partitions,
                });
            }
            KafkaRequest::Fetch(FetchRequest {
                max_wait_ms,
                min_bytes,
                topics,
            })
        }

        ApiKey::ListOffsets => {
            let replica_id = cursor_read_i32(&mut cur)?;
            let topic_count = cursor_read_array_len(&mut cur)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = cursor_read_string(&mut cur)?;
                let part_count = cursor_read_array_len(&mut cur)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let timestamp = cursor_read_i64(&mut cur)?;
                    if api_version == 0 {
                        let _max_num_offsets = cursor_read_i32(&mut cur)?;
                    }
                    partitions.push(ListOffsetsPartitionData {
                        partition_index,
                        timestamp,
                    });
                }
                topics.push(ListOffsetsTopicData {
                    topic_name,
                    partitions,
                });
            }
            KafkaRequest::ListOffsets(ListOffsetsRequest { replica_id, topics })
        }

        ApiKey::FindCoordinator => {
            let key = cursor_read_string(&mut cur)?;
            let key_type = if api_version >= 1 {
                cursor_read_i8(&mut cur)?
            } else {
                0
            };
            KafkaRequest::FindCoordinator(FindCoordinatorRequest { key, key_type })
        }

        ApiKey::JoinGroup => {
            let group_id = cursor_read_string(&mut cur)?;
            let session_timeout_ms = cursor_read_i32(&mut cur)?;
            let rebalance_timeout_ms = if api_version >= 1 {
                cursor_read_i32(&mut cur)?
            } else {
                session_timeout_ms
            };
            let member_id = cursor_read_string(&mut cur)?;
            let protocol_type = cursor_read_string(&mut cur)?;
            let proto_count = cursor_read_array_len(&mut cur)?;
            let mut protocols = Vec::with_capacity(proto_count.max(0) as usize);
            for _ in 0..proto_count {
                let name = cursor_read_string(&mut cur)?;
                let metadata = cursor_read_bytes(&mut cur)?;
                protocols.push(JoinGroupProtocol { name, metadata });
            }
            KafkaRequest::JoinGroup(JoinGroupRequest {
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                protocol_type,
                protocols,
            })
        }

        ApiKey::SyncGroup => {
            let group_id = cursor_read_string(&mut cur)?;
            let generation_id = cursor_read_i32(&mut cur)?;
            let member_id = cursor_read_string(&mut cur)?;
            let assign_count = cursor_read_array_len(&mut cur)?;
            let mut assignments = Vec::with_capacity(assign_count.max(0) as usize);
            for _ in 0..assign_count {
                let mid = cursor_read_string(&mut cur)?;
                let assignment = cursor_read_bytes(&mut cur)?;
                assignments.push(SyncGroupAssignment {
                    member_id: mid,
                    assignment,
                });
            }
            KafkaRequest::SyncGroup(SyncGroupRequest {
                group_id,
                generation_id,
                member_id,
                assignments,
            })
        }

        ApiKey::Heartbeat => {
            let group_id = cursor_read_string(&mut cur)?;
            let generation_id = cursor_read_i32(&mut cur)?;
            let member_id = cursor_read_string(&mut cur)?;
            KafkaRequest::Heartbeat(HeartbeatRequest {
                group_id,
                generation_id,
                member_id,
            })
        }

        ApiKey::LeaveGroup => {
            let group_id = cursor_read_string(&mut cur)?;
            let member_id = cursor_read_string(&mut cur)?;
            KafkaRequest::LeaveGroup(LeaveGroupRequest {
                group_id,
                member_id,
            })
        }

        ApiKey::OffsetCommit => {
            let group_id = cursor_read_string(&mut cur)?;
            let generation_id = if api_version >= 1 {
                cursor_read_i32(&mut cur)?
            } else {
                -1
            };
            let member_id = if api_version >= 1 {
                cursor_read_string(&mut cur)?
            } else {
                WireString::empty()
            };
            if api_version >= 2 {
                let _retention_time_ms = cursor_read_i64(&mut cur)?;
            }
            let topic_count = cursor_read_array_len(&mut cur)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = cursor_read_string(&mut cur)?;
                let part_count = cursor_read_array_len(&mut cur)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = cursor_read_i32(&mut cur)?;
                    let offset = cursor_read_i64(&mut cur)?;
                    if api_version == 1 {
                        let _timestamp = cursor_read_i64(&mut cur)?;
                    }
                    let metadata = cursor_read_nullable_string(&mut cur)?;
                    partitions.push(OffsetCommitPartitionData {
                        partition_index,
                        offset,
                        metadata,
                    });
                }
                topics.push(OffsetCommitTopicData {
                    topic_name,
                    partitions,
                });
            }
            KafkaRequest::OffsetCommit(OffsetCommitRequest {
                group_id,
                generation_id,
                member_id,
                topics,
            })
        }

        ApiKey::OffsetFetch => {
            let group_id = cursor_read_string(&mut cur)?;
            let topic_count = cursor_read_array_len(&mut cur)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = cursor_read_string(&mut cur)?;
                let part_count = cursor_read_array_len(&mut cur)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    partitions.push(cursor_read_i32(&mut cur)?);
                }
                topics.push(OffsetFetchTopicData {
                    topic_name,
                    partitions,
                });
            }
            KafkaRequest::OffsetFetch(OffsetFetchRequest { group_id, topics })
        }

        ApiKey::CreateTopics => {
            let topic_count = cursor_read_array_len(&mut cur)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let name = cursor_read_string(&mut cur)?;
                let num_partitions = cursor_read_i32(&mut cur)?;
                let replication_factor = cursor_read_i16(&mut cur)?;
                let assign_count = cursor_read_array_len(&mut cur)?;
                for _ in 0..assign_count.max(0) {
                    let _partition_index = cursor_read_i32(&mut cur)?;
                    let replica_count = cursor_read_array_len(&mut cur)?;
                    for _ in 0..replica_count.max(0) {
                        let _broker_id = cursor_read_i32(&mut cur)?;
                    }
                }
                let config_count = cursor_read_array_len(&mut cur)?;
                for _ in 0..config_count.max(0) {
                    let _key = cursor_read_string(&mut cur)?;
                    let _val = cursor_read_nullable_string(&mut cur)?;
                }
                topics.push(CreateTopicRequest {
                    name,
                    num_partitions,
                    replication_factor,
                });
            }
            let timeout_ms = cursor_read_i32(&mut cur)?;
            KafkaRequest::CreateTopics(CreateTopicsRequest { topics, timeout_ms })
        }

        ApiKey::DeleteTopics => {
            let count = cursor_read_array_len(&mut cur)?;
            let mut topic_names = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                topic_names.push(cursor_read_string(&mut cur)?);
            }
            let timeout_ms = cursor_read_i32(&mut cur)?;
            KafkaRequest::DeleteTopics(DeleteTopicsRequest {
                topic_names,
                timeout_ms,
            })
        }

        ApiKey::DescribeGroups => {
            let count = cursor_read_array_len(&mut cur)?;
            let mut group_ids = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                group_ids.push(cursor_read_string(&mut cur)?);
            }
            KafkaRequest::DescribeGroups(DescribeGroupsRequest { group_ids })
        }

        ApiKey::ListGroups => KafkaRequest::ListGroups,
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
pub fn encode_response(correlation_id: i32, response: &KafkaResponse, buf: &mut BytesMut) {
    let len_pos = buf.len();
    write_i32(buf, 0);

    let payload_start = buf.len();

    write_i32(buf, correlation_id);

    match response {
        KafkaResponse::ApiVersions(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, r.api_keys.len() as i32);
            for ak in &r.api_keys {
                write_i16(buf, ak.api_key);
                write_i16(buf, ak.min_version);
                write_i16(buf, ak.max_version);
            }
        }

        KafkaResponse::Metadata(r) => {
            write_i32(buf, r.brokers.len() as i32);
            for b in &r.brokers {
                write_i32(buf, b.node_id);
                write_string(buf, b.host.as_bytes());
                write_i32(buf, b.port);
            }
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_i16(buf, t.error_code);
                write_string(buf, t.name.as_bytes());
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i16(buf, p.error_code);
                    write_i32(buf, p.partition_index);
                    write_i32(buf, p.leader);
                    write_i32(buf, p.replicas.len() as i32);
                    for &r in &p.replicas {
                        write_i32(buf, r);
                    }
                    write_i32(buf, p.isr.len() as i32);
                    for &i in &p.isr {
                        write_i32(buf, i);
                    }
                }
            }
        }

        KafkaResponse::Produce(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, t.topic_name.as_bytes());
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    write_i64(buf, p.base_offset);
                }
            }
        }

        KafkaResponse::Fetch(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, t.topic_name.as_bytes());
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    write_i64(buf, p.high_watermark);
                    if p.record_set.is_empty() {
                        write_i32(buf, -1);
                    } else {
                        write_bytes(buf, &p.record_set);
                    }
                }
            }
        }

        KafkaResponse::ListOffsets(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, t.topic_name.as_bytes());
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    write_i64(buf, p.timestamp);
                    write_i64(buf, p.offset);
                }
            }
        }

        KafkaResponse::FindCoordinator(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, r.node_id);
            write_string(buf, r.host.as_bytes());
            write_i32(buf, r.port);
        }

        KafkaResponse::JoinGroup(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, r.generation_id);
            write_string(buf, r.protocol_name.as_bytes());
            write_string(buf, r.leader.as_bytes());
            write_string(buf, r.member_id.as_bytes());
            write_i32(buf, r.members.len() as i32);
            for m in &r.members {
                write_string(buf, m.member_id.as_bytes());
                write_bytes(buf, &m.metadata);
            }
        }

        KafkaResponse::SyncGroup(r) => {
            write_i16(buf, r.error_code);
            write_bytes(buf, &r.assignment);
        }

        KafkaResponse::Heartbeat(r) => {
            write_i16(buf, r.error_code);
        }

        KafkaResponse::LeaveGroup(r) => {
            write_i16(buf, r.error_code);
        }

        KafkaResponse::OffsetCommit(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, t.topic_name.as_bytes());
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                }
            }
        }

        KafkaResponse::OffsetFetch(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, t.topic_name.as_bytes());
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i64(buf, p.offset);
                    write_nullable_string(buf, &p.metadata);
                    write_i16(buf, p.error_code);
                }
            }
        }

        KafkaResponse::CreateTopics(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, t.name.as_bytes());
                write_i16(buf, t.error_code);
            }
        }

        KafkaResponse::DeleteTopics(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, t.name.as_bytes());
                write_i16(buf, t.error_code);
            }
        }

        KafkaResponse::DescribeGroups(r) => {
            write_i32(buf, r.groups.len() as i32);
            for g in &r.groups {
                write_i16(buf, g.error_code);
                write_string(buf, g.group_id.as_bytes());
                write_string(buf, g.state.as_bytes());
                write_string(buf, g.protocol_type.as_bytes());
                write_string(buf, g.protocol.as_bytes());
                write_i32(buf, g.members.len() as i32);
                for m in &g.members {
                    write_string(buf, m.member_id.as_bytes());
                    write_string(buf, m.client_id.as_bytes());
                    write_string(buf, m.client_host.as_bytes());
                    write_bytes(buf, &m.metadata);
                    write_bytes(buf, &m.assignment);
                }
            }
        }

        KafkaResponse::ListGroups(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, r.groups.len() as i32);
            for g in &r.groups {
                write_string(buf, g.group_id.as_bytes());
                write_string(buf, g.protocol_type.as_bytes());
            }
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
        encode_response(1, &resp, &mut buf);

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
            topics: vec![TopicMetadata {
                error_code: 0,
                name: WireString::from("test"),
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader: 1,
                    replicas: vec![1],
                    isr: vec![1],
                }],
            }],
        });

        let mut buf = BytesMut::new();
        encode_response(5, &resp, &mut buf);
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
}
