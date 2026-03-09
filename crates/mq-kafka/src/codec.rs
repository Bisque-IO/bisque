use bytes::{Buf, BufMut, Bytes, BytesMut};
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
// Primitive Decode Helpers
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
    Ok(buf.get_i8())
}

fn read_i16(buf: &mut &[u8]) -> Result<i16, CodecError> {
    ensure(buf, 2)?;
    Ok(buf.get_i16())
}

fn read_i32(buf: &mut &[u8]) -> Result<i32, CodecError> {
    ensure(buf, 4)?;
    Ok(buf.get_i32())
}

fn read_i64(buf: &mut &[u8]) -> Result<i64, CodecError> {
    ensure(buf, 8)?;
    Ok(buf.get_i64())
}

/// Kafka nullable string: i16 length (-1 = null), then bytes.
fn read_nullable_string(buf: &mut &[u8]) -> Result<Option<String>, CodecError> {
    let len = read_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    ensure(buf, len)?;
    let s = std::str::from_utf8(&buf[..len]).map_err(|_| CodecError::InvalidUtf8)?;
    let s = s.to_string();
    buf.advance(len);
    Ok(Some(s))
}

/// Kafka non-nullable string: i16 length, then bytes.
fn read_string(buf: &mut &[u8]) -> Result<String, CodecError> {
    read_nullable_string(buf)?.ok_or(CodecError::UnexpectedEof)
}

/// Kafka nullable bytes: i32 length (-1 = null), then bytes.
fn read_nullable_bytes(buf: &mut &[u8]) -> Result<Option<Bytes>, CodecError> {
    let len = read_i32(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    ensure(buf, len)?;
    let data = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(Some(data))
}

fn read_bytes(buf: &mut &[u8]) -> Result<Bytes, CodecError> {
    read_nullable_bytes(buf)?.ok_or(CodecError::UnexpectedEof)
}

fn read_array_len(buf: &mut &[u8]) -> Result<i32, CodecError> {
    read_i32(buf)
}

/// Zigzag-encoded varint (used inside RecordBatch).
fn read_varint(buf: &mut &[u8]) -> Result<i32, CodecError> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    loop {
        ensure(buf, 1)?;
        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(CodecError::InvalidRecordBatch("varint too long".into()));
        }
    }
    // Zigzag decode
    Ok(((result >> 1) as i32) ^ -((result & 1) as i32))
}

fn read_varlong(buf: &mut &[u8]) -> Result<i64, CodecError> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        ensure(buf, 1)?;
        let byte = buf.get_u8();
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

/// Read varint-prefixed nullable bytes (used in record body).
fn read_varint_bytes(buf: &mut &[u8]) -> Result<Option<Bytes>, CodecError> {
    let len = read_varint(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    ensure(buf, len)?;
    let data = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(Some(data))
}

fn read_varint_string(buf: &mut &[u8]) -> Result<String, CodecError> {
    let len = read_varint(buf)?;
    if len < 0 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = len as usize;
    ensure(buf, len)?;
    let s = std::str::from_utf8(&buf[..len]).map_err(|_| CodecError::InvalidUtf8)?;
    let s = s.to_string();
    buf.advance(len);
    Ok(s)
}

// =============================================================================
// Primitive Encode Helpers
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

fn write_nullable_string(buf: &mut BytesMut, s: &Option<String>) {
    match s {
        None => write_i16(buf, -1),
        Some(s) => {
            write_i16(buf, s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
    }
}

fn write_string(buf: &mut BytesMut, s: &str) {
    write_i16(buf, s.len() as i16);
    buf.put_slice(s.as_bytes());
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
    // Zigzag encode
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

fn write_varint_string(buf: &mut BytesMut, s: &str) {
    write_varint(buf, s.len() as i32);
    buf.put_slice(s.as_bytes());
}

// =============================================================================
// RecordBatch Encode/Decode
// =============================================================================

/// Decode a single record from within a RecordBatch.
fn decode_record(buf: &mut &[u8]) -> Result<Record, CodecError> {
    let _length = read_varint(buf)?; // record length (we just consume it)
    let _attributes = read_i8(buf)?;
    let timestamp_delta = read_varlong(buf)?;
    let offset_delta = read_varint(buf)?;
    let key = read_varint_bytes(buf)?;
    let value = read_varint_bytes(buf)?;

    let header_count = read_varint(buf)?;
    let mut headers = Vec::new();
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

/// Decode a Kafka v2 RecordBatch from a byte slice.
pub fn decode_record_batch(data: &[u8]) -> Result<RecordBatch, CodecError> {
    let buf = &mut &data[..];

    ensure(buf, 8 + 4 + 4)?; // base_offset + batch_length + partition_leader_epoch
    let base_offset = read_i64(buf)?;
    let _batch_length = read_i32(buf)?;
    let partition_leader_epoch = read_i32(buf)?;

    // Magic byte
    ensure(buf, 1)?;
    let magic = buf.get_u8();
    if magic != 2 {
        return Err(CodecError::InvalidRecordBatch(format!(
            "unsupported magic: {}",
            magic
        )));
    }

    // CRC (skip validation for now)
    ensure(buf, 4)?;
    let _crc = buf.get_u32();

    let attributes = read_i16(buf)?;
    let last_offset_delta = read_i32(buf)?;
    let first_timestamp = read_i64(buf)?;
    let max_timestamp = read_i64(buf)?;
    let producer_id = read_i64(buf)?;
    let producer_epoch = read_i16(buf)?;
    let base_sequence = read_i32(buf)?;
    let record_count = read_i32(buf)?;

    // Check compression (attributes bits 0-2)
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

/// Encode a single record into the buffer.
fn encode_record(buf: &mut BytesMut, record: &Record) {
    // Build record body first to get length
    let mut body = BytesMut::new();
    write_i8(&mut body, 0); // attributes
    write_varlong(&mut body, record.timestamp_delta);
    write_varint(&mut body, record.offset_delta);
    write_varint_bytes(&mut body, &record.key);
    write_varint_bytes(&mut body, &record.value);
    write_varint(&mut body, record.headers.len() as i32);
    for hdr in &record.headers {
        write_varint_string(&mut body, &hdr.key);
        write_varint_bytes(&mut body, &Some(hdr.value.clone()));
    }

    write_varint(buf, body.len() as i32);
    buf.put_slice(&body);
}

/// Encode a Kafka v2 RecordBatch into the buffer.
pub fn encode_record_batch(batch: &RecordBatch, buf: &mut BytesMut) {
    write_i64(buf, batch.base_offset);

    // Placeholder for batch_length — fill in after encoding the rest
    let batch_len_pos = buf.len();
    write_i32(buf, 0);

    let batch_start = buf.len();

    write_i32(buf, batch.partition_leader_epoch);
    buf.put_u8(2); // magic

    // CRC placeholder
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

    // Fill in batch_length (everything after the batch_length field itself)
    let batch_length = (buf.len() - batch_start) as i32;
    buf[batch_len_pos..batch_len_pos + 4].copy_from_slice(&batch_length.to_be_bytes());

    // Compute CRC32C over everything after the CRC field
    let crc = crc32fast::hash(&buf[crc_start..]);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
}

// =============================================================================
// Request Decode
// =============================================================================

/// Try to decode a single Kafka request from `data`.
/// Returns `(header, request, bytes_consumed)` or `Incomplete` if more data needed.
pub fn decode_request(data: &[u8]) -> Result<(RequestHeader, KafkaRequest, usize), CodecError> {
    if data.len() < 4 {
        return Err(CodecError::Incomplete);
    }
    let frame_size = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if frame_size > MAX_FRAME_SIZE {
        return Err(CodecError::FrameTooLarge(frame_size));
    }
    let total = 4 + frame_size;
    if data.len() < total {
        return Err(CodecError::Incomplete);
    }

    let buf = &mut &data[4..total];

    // Request header
    let api_key = read_i16(buf)?;
    let api_version = read_i16(buf)?;
    let correlation_id = read_i32(buf)?;
    let client_id = read_nullable_string(buf)?;

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
            let count = read_array_len(buf)?;
            let topics = if count < 0 {
                None
            } else {
                let mut v = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    v.push(read_string(buf)?);
                }
                Some(v)
            };
            KafkaRequest::Metadata(MetadataRequest { topics })
        }

        ApiKey::Produce => {
            let acks = read_i16(buf)?;
            let timeout_ms = read_i32(buf)?;
            let topic_count = read_array_len(buf)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = read_string(buf)?;
                let part_count = read_array_len(buf)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = read_i32(buf)?;
                    let record_set = read_nullable_bytes(buf)?;
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
            let _replica_id = read_i32(buf)?;
            let max_wait_ms = read_i32(buf)?;
            let min_bytes = read_i32(buf)?;
            let topic_count = read_array_len(buf)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = read_string(buf)?;
                let part_count = read_array_len(buf)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = read_i32(buf)?;
                    let fetch_offset = read_i64(buf)?;
                    let max_bytes = read_i32(buf)?;
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
            let replica_id = read_i32(buf)?;
            let topic_count = read_array_len(buf)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = read_string(buf)?;
                let part_count = read_array_len(buf)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = read_i32(buf)?;
                    let timestamp = read_i64(buf)?;
                    // v0 has max_num_offsets but v1 does not
                    if api_version == 0 {
                        let _max_num_offsets = read_i32(buf)?;
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
            let key = read_string(buf)?;
            let key_type = if api_version >= 1 {
                read_i8(buf)?
            } else {
                0 // group coordinator
            };
            KafkaRequest::FindCoordinator(FindCoordinatorRequest { key, key_type })
        }

        ApiKey::JoinGroup => {
            let group_id = read_string(buf)?;
            let session_timeout_ms = read_i32(buf)?;
            let rebalance_timeout_ms = if api_version >= 1 {
                read_i32(buf)?
            } else {
                session_timeout_ms
            };
            let member_id = read_string(buf)?;
            let protocol_type = read_string(buf)?;
            let proto_count = read_array_len(buf)?;
            let mut protocols = Vec::with_capacity(proto_count.max(0) as usize);
            for _ in 0..proto_count {
                let name = read_string(buf)?;
                let metadata = read_bytes(buf)?;
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
            let group_id = read_string(buf)?;
            let generation_id = read_i32(buf)?;
            let member_id = read_string(buf)?;
            let assign_count = read_array_len(buf)?;
            let mut assignments = Vec::with_capacity(assign_count.max(0) as usize);
            for _ in 0..assign_count {
                let mid = read_string(buf)?;
                let assignment = read_bytes(buf)?;
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
            let group_id = read_string(buf)?;
            let generation_id = read_i32(buf)?;
            let member_id = read_string(buf)?;
            KafkaRequest::Heartbeat(HeartbeatRequest {
                group_id,
                generation_id,
                member_id,
            })
        }

        ApiKey::LeaveGroup => {
            let group_id = read_string(buf)?;
            let member_id = read_string(buf)?;
            KafkaRequest::LeaveGroup(LeaveGroupRequest {
                group_id,
                member_id,
            })
        }

        ApiKey::OffsetCommit => {
            let group_id = read_string(buf)?;
            let generation_id = if api_version >= 1 { read_i32(buf)? } else { -1 };
            let member_id = if api_version >= 1 {
                read_string(buf)?
            } else {
                String::new()
            };
            if api_version >= 2 {
                let _retention_time_ms = read_i64(buf)?;
            }
            let topic_count = read_array_len(buf)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = read_string(buf)?;
                let part_count = read_array_len(buf)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    let partition_index = read_i32(buf)?;
                    let offset = read_i64(buf)?;
                    if api_version == 1 {
                        let _timestamp = read_i64(buf)?;
                    }
                    let metadata = read_nullable_string(buf)?;
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
            let group_id = read_string(buf)?;
            let topic_count = read_array_len(buf)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let topic_name = read_string(buf)?;
                let part_count = read_array_len(buf)?;
                let mut partitions = Vec::with_capacity(part_count.max(0) as usize);
                for _ in 0..part_count {
                    partitions.push(read_i32(buf)?);
                }
                topics.push(OffsetFetchTopicData {
                    topic_name,
                    partitions,
                });
            }
            KafkaRequest::OffsetFetch(OffsetFetchRequest { group_id, topics })
        }

        ApiKey::CreateTopics => {
            let topic_count = read_array_len(buf)?;
            let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
            for _ in 0..topic_count {
                let name = read_string(buf)?;
                let num_partitions = read_i32(buf)?;
                let replication_factor = read_i16(buf)?;
                // Skip replica assignments
                let assign_count = read_array_len(buf)?;
                for _ in 0..assign_count.max(0) {
                    let _partition_index = read_i32(buf)?;
                    let replica_count = read_array_len(buf)?;
                    for _ in 0..replica_count.max(0) {
                        let _broker_id = read_i32(buf)?;
                    }
                }
                // Skip configs
                let config_count = read_array_len(buf)?;
                for _ in 0..config_count.max(0) {
                    let _key = read_string(buf)?;
                    let _val = read_nullable_string(buf)?;
                }
                topics.push(CreateTopicRequest {
                    name,
                    num_partitions,
                    replication_factor,
                });
            }
            let timeout_ms = read_i32(buf)?;
            KafkaRequest::CreateTopics(CreateTopicsRequest { topics, timeout_ms })
        }

        ApiKey::DeleteTopics => {
            let count = read_array_len(buf)?;
            let mut topic_names = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                topic_names.push(read_string(buf)?);
            }
            let timeout_ms = read_i32(buf)?;
            KafkaRequest::DeleteTopics(DeleteTopicsRequest {
                topic_names,
                timeout_ms,
            })
        }

        ApiKey::DescribeGroups => {
            let count = read_array_len(buf)?;
            let mut group_ids = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                group_ids.push(read_string(buf)?);
            }
            KafkaRequest::DescribeGroups(DescribeGroupsRequest { group_ids })
        }

        ApiKey::ListGroups => KafkaRequest::ListGroups,
    };

    Ok((header, request, total))
}

// =============================================================================
// Response Encode
// =============================================================================

/// Encode a Kafka response into `buf`, including the 4-byte frame length prefix.
pub fn encode_response(correlation_id: i32, response: &KafkaResponse, buf: &mut BytesMut) {
    // Reserve space for frame length
    let len_pos = buf.len();
    write_i32(buf, 0);

    let payload_start = buf.len();

    // Response header
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
                write_string(buf, &b.host);
                write_i32(buf, b.port);
            }
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_i16(buf, t.error_code);
                write_string(buf, &t.name);
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i16(buf, p.error_code);
                    write_i32(buf, p.partition_index);
                    write_i32(buf, p.leader);
                    // replicas
                    write_i32(buf, p.replicas.len() as i32);
                    for &r in &p.replicas {
                        write_i32(buf, r);
                    }
                    // isr
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
                write_string(buf, &t.topic_name);
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
                write_string(buf, &t.topic_name);
                write_i32(buf, t.partitions.len() as i32);
                for p in &t.partitions {
                    write_i32(buf, p.partition_index);
                    write_i16(buf, p.error_code);
                    write_i64(buf, p.high_watermark);
                    // record set as nullable bytes
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
                write_string(buf, &t.topic_name);
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
            write_string(buf, &r.host);
            write_i32(buf, r.port);
        }

        KafkaResponse::JoinGroup(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, r.generation_id);
            write_string(buf, &r.protocol_name);
            write_string(buf, &r.leader);
            write_string(buf, &r.member_id);
            write_i32(buf, r.members.len() as i32);
            for m in &r.members {
                write_string(buf, &m.member_id);
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
                write_string(buf, &t.topic_name);
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
                write_string(buf, &t.topic_name);
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
                write_string(buf, &t.name);
                write_i16(buf, t.error_code);
            }
        }

        KafkaResponse::DeleteTopics(r) => {
            write_i32(buf, r.topics.len() as i32);
            for t in &r.topics {
                write_string(buf, &t.name);
                write_i16(buf, t.error_code);
            }
        }

        KafkaResponse::DescribeGroups(r) => {
            write_i32(buf, r.groups.len() as i32);
            for g in &r.groups {
                write_i16(buf, g.error_code);
                write_string(buf, &g.group_id);
                write_string(buf, &g.state);
                write_string(buf, &g.protocol_type);
                write_string(buf, &g.protocol);
                write_i32(buf, g.members.len() as i32);
                for m in &g.members {
                    write_string(buf, &m.member_id);
                    write_string(buf, &m.client_id);
                    write_string(buf, &m.client_host);
                    write_bytes(buf, &m.metadata);
                    write_bytes(buf, &m.assignment);
                }
            }
        }

        KafkaResponse::ListGroups(r) => {
            write_i16(buf, r.error_code);
            write_i32(buf, r.groups.len() as i32);
            for g in &r.groups {
                write_string(buf, &g.group_id);
                write_string(buf, &g.protocol_type);
            }
        }
    }

    // Patch frame length
    let payload_len = (buf.len() - payload_start) as i32;
    buf[len_pos..len_pos + 4].copy_from_slice(&payload_len.to_be_bytes());
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
        write_string(&mut buf, "hello");
        let mut slice: &[u8] = &buf;
        let decoded = read_string(&mut slice).unwrap();
        assert_eq!(decoded, "hello");
    }

    #[test]
    fn test_nullable_string_roundtrip() {
        // Non-null
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, &Some("test".to_string()));
        let mut slice: &[u8] = &buf;
        let decoded = read_nullable_string(&mut slice).unwrap();
        assert_eq!(decoded, Some("test".to_string()));

        // Null
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, &None);
        let mut slice: &[u8] = &buf;
        let decoded = read_nullable_string(&mut slice).unwrap();
        assert_eq!(decoded, None);
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
                        key: "h1".into(),
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
        assert_eq!(decoded.records[0].headers[0].key, "h1");
        assert_eq!(decoded.records[1].key, None);
        assert_eq!(
            decoded.records[1].value,
            Some(Bytes::from_static(b"value2"))
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

        // Should have: 4 (frame_len) + 4 (correlation_id) + 2 (error) + 4 (array_len) + 6 (one entry)
        assert_eq!(buf.len(), 4 + 4 + 2 + 4 + 6);

        // Verify frame length
        let frame_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(frame_len as usize, buf.len() - 4);

        // Verify correlation_id
        let cid = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(cid, 1);
    }

    #[test]
    fn test_api_versions_request_decode() {
        // Build a minimal ApiVersions request
        let mut frame = BytesMut::new();
        // Header: api_key=18, api_version=0, correlation_id=7, client_id=null
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 18); // ApiVersions
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 7);
        write_i16(&mut payload, -1); // null client_id

        write_i32(&mut frame, payload.len() as i32);
        frame.put_slice(&payload);

        let (header, req, consumed) = decode_request(&frame).unwrap();
        assert_eq!(header.api_key, 18);
        assert_eq!(header.correlation_id, 7);
        assert_eq!(consumed, frame.len());
        assert!(matches!(req, KafkaRequest::ApiVersions));
    }

    #[test]
    fn test_metadata_request_decode() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 3); // Metadata
        write_i16(&mut payload, 0);
        write_i32(&mut payload, 42);
        write_i16(&mut payload, -1); // null client_id
        write_i32(&mut payload, 1); // 1 topic
        write_string(&mut payload, "test-topic");

        let mut frame = BytesMut::new();
        write_i32(&mut frame, payload.len() as i32);
        frame.put_slice(&payload);

        let (header, req, _) = decode_request(&frame).unwrap();
        assert_eq!(header.correlation_id, 42);
        match req {
            KafkaRequest::Metadata(m) => {
                assert_eq!(m.topics.as_ref().unwrap().len(), 1);
                assert_eq!(m.topics.as_ref().unwrap()[0], "test-topic");
            }
            _ => panic!("expected Metadata"),
        }
    }

    #[test]
    fn test_incomplete_frame() {
        let data = [0u8, 0, 0, 20]; // says 20 bytes but we only have 4
        let result = decode_request(&data);
        assert!(matches!(result, Err(CodecError::Incomplete)));
    }

    #[test]
    fn test_produce_request_decode() {
        let mut payload = BytesMut::new();
        write_i16(&mut payload, 0); // Produce api_key
        write_i16(&mut payload, 0); // api_version
        write_i32(&mut payload, 99); // correlation_id
        write_i16(&mut payload, -1); // null client_id
        write_i16(&mut payload, -1); // acks = all
        write_i32(&mut payload, 5000); // timeout_ms
        write_i32(&mut payload, 1); // 1 topic
        write_string(&mut payload, "my-topic");
        write_i32(&mut payload, 1); // 1 partition
        write_i32(&mut payload, 0); // partition_index
        // record_set: build a small record batch
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
        write_i32(&mut frame, payload.len() as i32);
        frame.put_slice(&payload);

        let (header, req, _) = decode_request(&frame).unwrap();
        assert_eq!(header.correlation_id, 99);
        match req {
            KafkaRequest::Produce(p) => {
                assert_eq!(p.acks, -1);
                assert_eq!(p.topics.len(), 1);
                assert_eq!(p.topics[0].topic_name, "my-topic");
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
                host: "localhost".into(),
                port: 9092,
            }],
            topics: vec![TopicMetadata {
                error_code: 0,
                name: "test".into(),
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

        // Verify frame length and correlation_id
        let frame_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(frame_len as usize, buf.len() - 4);
        let cid = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(cid, 5);
    }
}
