//! Encode/Decode implementations for LanceCommand.
//!
//! These are required for the bisque-raft multi-raft codec layer which
//! serializes log entries to the mmap storage and over the TCP transport.
//!
//! The `decode_from_bytes` override enables zero-copy decoding of large
//! payloads (IPC-encoded RecordBatches) from mmap-backed `Bytes` buffers.

use std::io::{Read, Write};
use std::sync::Arc;

use bisque_raft::codec::{BorrowPayload, CodecError, Decode, Encode};
use bytes::Bytes;

use crate::types::{LanceCommand, SealReason, TxnId, TxnOp};

// Discriminant bytes for LanceCommand variants
const CMD_APPEND_RECORDS: u8 = 0;
const CMD_SEAL_ACTIVE_SEGMENT: u8 = 1;
const CMD_BEGIN_FLUSH: u8 = 2;
const CMD_PROMOTE_TO_DEEP_STORAGE: u8 = 3;
const CMD_CREATE_TABLE: u8 = 4;
const CMD_DROP_TABLE: u8 = 5;
const CMD_REGISTER_SESSION: u8 = 6;
const CMD_PIN_VERSION: u8 = 7;
const CMD_UNPIN_VERSION: u8 = 8;
const CMD_EXPIRE_SESSION: u8 = 9;
const CMD_DELETE_RECORDS: u8 = 10;
const CMD_UPDATE_RECORDS: u8 = 11;
const CMD_TXN_CHUNK: u8 = 12;
const CMD_TXN_COMMIT: u8 = 13;
const CMD_TXN_ABORT: u8 = 14;

// Discriminant bytes for TxnOp variants
const TXN_OP_APPEND: u8 = 0;
const TXN_OP_DELETE: u8 = 1;
const TXN_OP_UPDATE: u8 = 2;
const TXN_OP_CREATE_TABLE: u8 = 3;
const TXN_OP_DROP_TABLE: u8 = 4;

// Discriminant bytes for SealReason
const SEAL_MAX_AGE: u8 = 0;
const SEAL_MAX_SIZE: u8 = 1;

/// Encode a table name as a length-prefixed (u16) UTF-8 string.
#[inline]
fn encode_table_name<W: Write>(name: &str, writer: &mut W) -> Result<(), CodecError> {
    (name.len() as u16).encode(writer)?;
    writer.write_all(name.as_bytes())?;
    Ok(())
}

/// Decode a table name from a length-prefixed (u16) UTF-8 string.
#[inline]
fn decode_table_name<R: Read>(reader: &mut R) -> Result<Arc<str>, CodecError> {
    let len = u16::decode(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    let s = String::from_utf8(buf).map_err(|_| {
        CodecError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "table name is not valid UTF-8",
        ))
    })?;
    Ok(Arc::from(s))
}

/// Decode a table name from a `Bytes` buffer at the given offset.
/// Returns `(table_name, bytes_consumed)`.
#[inline]
fn decode_table_name_from_bytes(
    data: &Bytes,
    offset: usize,
) -> Result<(Arc<str>, usize), CodecError> {
    if data.len() < offset + 2 {
        return Err(CodecError::BufferTooSmall {
            needed: offset + 2,
            have: data.len(),
        });
    }
    let len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
    let name_end = offset + 2 + len;
    if data.len() < name_end {
        return Err(CodecError::BufferTooSmall {
            needed: name_end,
            have: data.len(),
        });
    }
    let name = std::str::from_utf8(&data[offset + 2..name_end])
        .map_err(|e| CodecError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
    Ok((Arc::from(name), 2 + len))
}

/// Read a u64 from a `Bytes` buffer at the given offset.
#[inline]
fn read_u64_at(data: &[u8], offset: usize) -> Result<u64, CodecError> {
    if data.len() < offset + 8 {
        return Err(CodecError::BufferTooSmall {
            needed: offset + 8,
            have: data.len(),
        });
    }
    Ok(u64::from_le_bytes(
        data[offset..offset + 8].try_into().unwrap(),
    ))
}

/// Read a u32 from a `Bytes` buffer at the given offset.
#[inline]
fn read_u32_at(data: &[u8], offset: usize) -> Result<u32, CodecError> {
    if data.len() < offset + 4 {
        return Err(CodecError::BufferTooSmall {
            needed: offset + 4,
            have: data.len(),
        });
    }
    Ok(u32::from_le_bytes(
        data[offset..offset + 4].try_into().unwrap(),
    ))
}

/// Validate UTF-8 on a byte slice and convert to `String`.
///
/// Validates in-place on the borrowed slice; only allocates the `String`
/// after validation succeeds, avoiding allocation on malformed input.
#[inline]
fn validated_utf8_string(data: &[u8], offset: usize, len: usize) -> Result<String, CodecError> {
    let s = std::str::from_utf8(&data[offset..offset + len]).map_err(|_| {
        CodecError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "filter is not valid UTF-8",
        ))
    })?;
    Ok(s.to_owned())
}

/// Encoded size of a table name (2-byte length prefix + UTF-8 bytes).
#[inline]
fn table_name_size(name: &str) -> usize {
    2 + name.len()
}

// =============================================================================
// TxnOp Encode / Decode
// =============================================================================

impl Encode for TxnOp {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            TxnOp::Append { table, data } => {
                TXN_OP_APPEND.encode(writer)?;
                encode_table_name(table, writer)?;
                (data.len() as u32).encode(writer)?;
                writer.write_all(data)?;
            }
            TxnOp::Delete { table, filter } => {
                TXN_OP_DELETE.encode(writer)?;
                encode_table_name(table, writer)?;
                (filter.len() as u32).encode(writer)?;
                writer.write_all(filter.as_bytes())?;
            }
            TxnOp::Update {
                table,
                filter,
                data,
            } => {
                TXN_OP_UPDATE.encode(writer)?;
                encode_table_name(table, writer)?;
                (filter.len() as u32).encode(writer)?;
                writer.write_all(filter.as_bytes())?;
                (data.len() as u32).encode(writer)?;
                writer.write_all(data)?;
            }
            TxnOp::CreateTable { table, schema_ipc } => {
                TXN_OP_CREATE_TABLE.encode(writer)?;
                encode_table_name(table, writer)?;
                (schema_ipc.len() as u32).encode(writer)?;
                writer.write_all(schema_ipc)?;
            }
            TxnOp::DropTable { table } => {
                TXN_OP_DROP_TABLE.encode(writer)?;
                encode_table_name(table, writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            TxnOp::Append { table, data } => table_name_size(table) + 4 + data.len(),
            TxnOp::Delete { table, filter } => table_name_size(table) + 4 + filter.len(),
            TxnOp::Update {
                table,
                filter,
                data,
            } => table_name_size(table) + 4 + filter.len() + 4 + data.len(),
            TxnOp::CreateTable { table, schema_ipc } => {
                table_name_size(table) + 4 + schema_ipc.len()
            }
            TxnOp::DropTable { table } => table_name_size(table),
        }
    }
}

impl Decode for TxnOp {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        match u8::decode(reader)? {
            TXN_OP_APPEND => {
                let table = decode_table_name(reader)?;
                let len = u32::decode(reader)? as usize;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                Ok(TxnOp::Append {
                    table,
                    data: Bytes::from(buf),
                })
            }
            TXN_OP_DELETE => {
                let table = decode_table_name(reader)?;
                let filter_len = u32::decode(reader)? as usize;
                let mut filter_buf = vec![0u8; filter_len];
                reader.read_exact(&mut filter_buf)?;
                let filter = String::from_utf8(filter_buf).map_err(|_| {
                    CodecError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "TxnOp filter is not valid UTF-8",
                    ))
                })?;
                Ok(TxnOp::Delete { table, filter })
            }
            TXN_OP_UPDATE => {
                let table = decode_table_name(reader)?;
                let filter_len = u32::decode(reader)? as usize;
                let mut filter_buf = vec![0u8; filter_len];
                reader.read_exact(&mut filter_buf)?;
                let filter = String::from_utf8(filter_buf).map_err(|_| {
                    CodecError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "TxnOp filter is not valid UTF-8",
                    ))
                })?;
                let data_len = u32::decode(reader)? as usize;
                let mut data_buf = vec![0u8; data_len];
                reader.read_exact(&mut data_buf)?;
                Ok(TxnOp::Update {
                    table,
                    filter,
                    data: Bytes::from(data_buf),
                })
            }
            TXN_OP_CREATE_TABLE => {
                let table = decode_table_name(reader)?;
                let len = u32::decode(reader)? as usize;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                Ok(TxnOp::CreateTable {
                    table,
                    schema_ipc: Bytes::from(buf),
                })
            }
            TXN_OP_DROP_TABLE => {
                let table = decode_table_name(reader)?;
                Ok(TxnOp::DropTable { table })
            }
            d => Err(CodecError::InvalidDiscriminant(d)),
        }
    }
}

impl Encode for TxnId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        16
    }
}

impl Decode for TxnId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(TxnId(u128::from_le_bytes(buf)))
    }
}

/// Decode a TxnId from a `Bytes` buffer at the given offset.
#[inline]
fn read_txn_id_at(data: &[u8], offset: usize) -> Result<TxnId, CodecError> {
    if data.len() < offset + 16 {
        return Err(CodecError::BufferTooSmall {
            needed: offset + 16,
            have: data.len(),
        });
    }
    let id = u128::from_le_bytes(data[offset..offset + 16].try_into().unwrap());
    Ok(TxnId(id))
}

/// Decode a Vec<TxnOp> from a `Bytes` buffer at the given offset (zero-copy for IPC data).
/// Returns `(ops, bytes_consumed)`.
fn decode_txn_ops_from_bytes(
    data: &Bytes,
    mut offset: usize,
) -> Result<(Vec<TxnOp>, usize), CodecError> {
    let start = offset;
    let n_ops = read_u32_at(data, offset)? as usize;
    offset += 4;

    let mut ops = Vec::with_capacity(n_ops);
    for _ in 0..n_ops {
        if data.len() <= offset {
            return Err(CodecError::BufferTooSmall {
                needed: offset + 1,
                have: data.len(),
            });
        }
        let disc = data[offset];
        offset += 1;

        match disc {
            TXN_OP_APPEND => {
                let (table, consumed) = decode_table_name_from_bytes(data, offset)?;
                offset += consumed;
                let len = read_u32_at(data, offset)? as usize;
                offset += 4;
                if data.len() < offset + len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + len,
                        have: data.len(),
                    });
                }
                let payload = data.slice(offset..offset + len);
                offset += len;
                ops.push(TxnOp::Append {
                    table,
                    data: payload,
                });
            }
            TXN_OP_DELETE => {
                let (table, consumed) = decode_table_name_from_bytes(data, offset)?;
                offset += consumed;
                let filter_len = read_u32_at(data, offset)? as usize;
                offset += 4;
                if data.len() < offset + filter_len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + filter_len,
                        have: data.len(),
                    });
                }
                let filter = validated_utf8_string(data, offset, filter_len)?;
                offset += filter_len;
                ops.push(TxnOp::Delete { table, filter });
            }
            TXN_OP_UPDATE => {
                let (table, consumed) = decode_table_name_from_bytes(data, offset)?;
                offset += consumed;
                let filter_len = read_u32_at(data, offset)? as usize;
                offset += 4;
                if data.len() < offset + filter_len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + filter_len,
                        have: data.len(),
                    });
                }
                let filter = validated_utf8_string(data, offset, filter_len)?;
                offset += filter_len;
                let data_len = read_u32_at(data, offset)? as usize;
                offset += 4;
                if data.len() < offset + data_len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + data_len,
                        have: data.len(),
                    });
                }
                let payload = data.slice(offset..offset + data_len);
                offset += data_len;
                ops.push(TxnOp::Update {
                    table,
                    filter,
                    data: payload,
                });
            }
            TXN_OP_CREATE_TABLE => {
                let (table, consumed) = decode_table_name_from_bytes(data, offset)?;
                offset += consumed;
                let len = read_u32_at(data, offset)? as usize;
                offset += 4;
                if data.len() < offset + len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + len,
                        have: data.len(),
                    });
                }
                let schema_ipc = data.slice(offset..offset + len);
                offset += len;
                ops.push(TxnOp::CreateTable { table, schema_ipc });
            }
            TXN_OP_DROP_TABLE => {
                let (table, consumed) = decode_table_name_from_bytes(data, offset)?;
                offset += consumed;
                ops.push(TxnOp::DropTable { table });
            }
            d => return Err(CodecError::InvalidDiscriminant(d)),
        }
    }

    Ok((ops, offset - start))
}

// =============================================================================
// SealReason Encode / Decode
// =============================================================================

impl Encode for SealReason {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            SealReason::MaxAge => SEAL_MAX_AGE.encode(writer),
            SealReason::MaxSize => SEAL_MAX_SIZE.encode(writer),
        }
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for SealReason {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        match u8::decode(reader)? {
            SEAL_MAX_AGE => Ok(SealReason::MaxAge),
            SEAL_MAX_SIZE => Ok(SealReason::MaxSize),
            d => Err(CodecError::InvalidDiscriminant(d)),
        }
    }
}

impl Encode for LanceCommand {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            LanceCommand::CreateTable {
                table_name,
                schema_ipc,
            } => {
                CMD_CREATE_TABLE.encode(writer)?;
                encode_table_name(table_name, writer)?;
                (schema_ipc.len() as u32).encode(writer)?;
                writer.write_all(schema_ipc)?;
            }
            LanceCommand::DropTable { table_name } => {
                CMD_DROP_TABLE.encode(writer)?;
                encode_table_name(table_name, writer)?;
            }
            LanceCommand::AppendRecords { table_name, data } => {
                CMD_APPEND_RECORDS.encode(writer)?;
                encode_table_name(table_name, writer)?;
                (data.len() as u32).encode(writer)?;
                writer.write_all(data)?;
            }
            LanceCommand::SealActiveSegment {
                table_name,
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => {
                CMD_SEAL_ACTIVE_SEGMENT.encode(writer)?;
                encode_table_name(table_name, writer)?;
                sealed_segment_id.encode(writer)?;
                new_active_segment_id.encode(writer)?;
                reason.encode(writer)?;
            }
            LanceCommand::BeginFlush {
                table_name,
                segment_id,
            } => {
                CMD_BEGIN_FLUSH.encode(writer)?;
                encode_table_name(table_name, writer)?;
                segment_id.encode(writer)?;
            }
            LanceCommand::PromoteToDeepStorage {
                table_name,
                segment_id,
                s3_manifest_version,
            } => {
                CMD_PROMOTE_TO_DEEP_STORAGE.encode(writer)?;
                encode_table_name(table_name, writer)?;
                segment_id.encode(writer)?;
                s3_manifest_version.encode(writer)?;
            }
            LanceCommand::RegisterSession { session_id } => {
                CMD_REGISTER_SESSION.encode(writer)?;
                session_id.encode(writer)?;
            }
            LanceCommand::PinVersion {
                session_id,
                table_name,
                tier,
                version,
            } => {
                CMD_PIN_VERSION.encode(writer)?;
                session_id.encode(writer)?;
                encode_table_name(table_name, writer)?;
                encode_table_name(tier, writer)?;
                version.encode(writer)?;
            }
            LanceCommand::UnpinVersion {
                session_id,
                table_name,
                tier,
                version,
            } => {
                CMD_UNPIN_VERSION.encode(writer)?;
                session_id.encode(writer)?;
                encode_table_name(table_name, writer)?;
                encode_table_name(tier, writer)?;
                version.encode(writer)?;
            }
            LanceCommand::ExpireSession { session_id } => {
                CMD_EXPIRE_SESSION.encode(writer)?;
                session_id.encode(writer)?;
            }
            LanceCommand::DeleteRecords { table_name, filter } => {
                CMD_DELETE_RECORDS.encode(writer)?;
                encode_table_name(table_name, writer)?;
                (filter.len() as u32).encode(writer)?;
                writer.write_all(filter.as_bytes())?;
            }
            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => {
                CMD_UPDATE_RECORDS.encode(writer)?;
                encode_table_name(table_name, writer)?;
                (filter.len() as u32).encode(writer)?;
                writer.write_all(filter.as_bytes())?;
                (data.len() as u32).encode(writer)?;
                writer.write_all(data)?;
            }
            LanceCommand::TxnChunk { txn_id, seq, ops } => {
                CMD_TXN_CHUNK.encode(writer)?;
                txn_id.encode(writer)?;
                seq.encode(writer)?;
                (ops.len() as u32).encode(writer)?;
                for op in ops {
                    op.encode(writer)?;
                }
            }
            LanceCommand::TxnCommit {
                txn_id,
                total_chunks,
            } => {
                CMD_TXN_COMMIT.encode(writer)?;
                txn_id.encode(writer)?;
                total_chunks.encode(writer)?;
            }
            LanceCommand::TxnAbort { txn_id } => {
                CMD_TXN_ABORT.encode(writer)?;
                txn_id.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            LanceCommand::CreateTable {
                table_name,
                schema_ipc,
            } => table_name_size(table_name) + 4 + schema_ipc.len(),
            LanceCommand::DropTable { table_name } => table_name_size(table_name),
            LanceCommand::AppendRecords { table_name, data } => {
                table_name_size(table_name) + 4 + data.len()
            }
            LanceCommand::SealActiveSegment {
                table_name, reason, ..
            } => table_name_size(table_name) + 8 + 8 + reason.encoded_size(),
            LanceCommand::BeginFlush { table_name, .. } => table_name_size(table_name) + 8,
            LanceCommand::PromoteToDeepStorage { table_name, .. } => {
                table_name_size(table_name) + 8 + 8
            }
            LanceCommand::RegisterSession { .. } => 8,
            LanceCommand::PinVersion {
                table_name, tier, ..
            } => 8 + table_name_size(table_name) + table_name_size(tier) + 8,
            LanceCommand::UnpinVersion {
                table_name, tier, ..
            } => 8 + table_name_size(table_name) + table_name_size(tier) + 8,
            LanceCommand::ExpireSession { .. } => 8,
            LanceCommand::DeleteRecords { table_name, filter } => {
                table_name_size(table_name) + 4 + filter.len()
            }
            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => table_name_size(table_name) + 4 + filter.len() + 4 + data.len(),
            LanceCommand::TxnChunk { txn_id, seq, ops } => {
                txn_id.encoded_size()
                    + seq.encoded_size()
                    + 4 // n_ops u32
                    + ops.iter().map(|op| op.encoded_size()).sum::<usize>()
            }
            LanceCommand::TxnCommit {
                txn_id,
                total_chunks,
            } => txn_id.encoded_size() + total_chunks.encoded_size(),
            LanceCommand::TxnAbort { txn_id } => txn_id.encoded_size(),
        }
    }
}

impl Decode for LanceCommand {
    /// Decode from a `Read` stream. Allocates for table name and payload.
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        match u8::decode(reader)? {
            CMD_CREATE_TABLE => {
                let table_name = decode_table_name(reader)?;
                let len = u32::decode(reader)? as usize;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                Ok(LanceCommand::CreateTable {
                    table_name,
                    schema_ipc: Bytes::from(buf),
                })
            }
            CMD_DROP_TABLE => {
                let table_name = decode_table_name(reader)?;
                Ok(LanceCommand::DropTable { table_name })
            }
            CMD_APPEND_RECORDS => {
                let table_name = decode_table_name(reader)?;
                let len = u32::decode(reader)? as usize;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                Ok(LanceCommand::AppendRecords {
                    table_name,
                    data: Bytes::from(buf),
                })
            }
            CMD_SEAL_ACTIVE_SEGMENT => {
                let table_name = decode_table_name(reader)?;
                let sealed_segment_id = u64::decode(reader)?;
                let new_active_segment_id = u64::decode(reader)?;
                let reason = SealReason::decode(reader)?;
                Ok(LanceCommand::SealActiveSegment {
                    table_name,
                    sealed_segment_id,
                    new_active_segment_id,
                    reason,
                })
            }
            CMD_BEGIN_FLUSH => {
                let table_name = decode_table_name(reader)?;
                let segment_id = u64::decode(reader)?;
                Ok(LanceCommand::BeginFlush {
                    table_name,
                    segment_id,
                })
            }
            CMD_PROMOTE_TO_DEEP_STORAGE => {
                let table_name = decode_table_name(reader)?;
                let segment_id = u64::decode(reader)?;
                let s3_manifest_version = u64::decode(reader)?;
                Ok(LanceCommand::PromoteToDeepStorage {
                    table_name,
                    segment_id,
                    s3_manifest_version,
                })
            }
            CMD_REGISTER_SESSION => {
                let session_id = u64::decode(reader)?;
                Ok(LanceCommand::RegisterSession { session_id })
            }
            CMD_PIN_VERSION => {
                let session_id = u64::decode(reader)?;
                let table_name = decode_table_name(reader)?;
                let tier = decode_table_name(reader)?;
                let version = u64::decode(reader)?;
                Ok(LanceCommand::PinVersion {
                    session_id,
                    table_name,
                    tier,
                    version,
                })
            }
            CMD_UNPIN_VERSION => {
                let session_id = u64::decode(reader)?;
                let table_name = decode_table_name(reader)?;
                let tier = decode_table_name(reader)?;
                let version = u64::decode(reader)?;
                Ok(LanceCommand::UnpinVersion {
                    session_id,
                    table_name,
                    tier,
                    version,
                })
            }
            CMD_EXPIRE_SESSION => {
                let session_id = u64::decode(reader)?;
                Ok(LanceCommand::ExpireSession { session_id })
            }
            CMD_DELETE_RECORDS => {
                let table_name = decode_table_name(reader)?;
                let filter_len = u32::decode(reader)? as usize;
                let mut filter_buf = vec![0u8; filter_len];
                reader.read_exact(&mut filter_buf)?;
                let filter = String::from_utf8(filter_buf).map_err(|_| {
                    CodecError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "filter is not valid UTF-8",
                    ))
                })?;
                Ok(LanceCommand::DeleteRecords { table_name, filter })
            }
            CMD_UPDATE_RECORDS => {
                let table_name = decode_table_name(reader)?;
                let filter_len = u32::decode(reader)? as usize;
                let mut filter_buf = vec![0u8; filter_len];
                reader.read_exact(&mut filter_buf)?;
                let filter = String::from_utf8(filter_buf).map_err(|_| {
                    CodecError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "filter is not valid UTF-8",
                    ))
                })?;
                let data_len = u32::decode(reader)? as usize;
                let mut data_buf = vec![0u8; data_len];
                reader.read_exact(&mut data_buf)?;
                Ok(LanceCommand::UpdateRecords {
                    table_name,
                    filter,
                    data: Bytes::from(data_buf),
                })
            }
            CMD_TXN_CHUNK => {
                let txn_id = TxnId::decode(reader)?;
                let seq = u32::decode(reader)?;
                let n_ops = u32::decode(reader)? as usize;
                let mut ops = Vec::with_capacity(n_ops);
                for _ in 0..n_ops {
                    ops.push(TxnOp::decode(reader)?);
                }
                Ok(LanceCommand::TxnChunk { txn_id, seq, ops })
            }
            CMD_TXN_COMMIT => {
                let txn_id = TxnId::decode(reader)?;
                let total_chunks = u32::decode(reader)?;
                Ok(LanceCommand::TxnCommit {
                    txn_id,
                    total_chunks,
                })
            }
            CMD_TXN_ABORT => {
                let txn_id = TxnId::decode(reader)?;
                Ok(LanceCommand::TxnAbort { txn_id })
            }
            d => Err(CodecError::InvalidDiscriminant(d)),
        }
    }

    /// Zero-copy decode from a `Bytes` buffer.
    ///
    /// Large payloads (AppendRecords data, CreateTable schema_ipc) are
    /// obtained via `Bytes::slice()` — no memcpy for mmap-backed buffers.
    fn decode_from_bytes(data: Bytes) -> Result<Self, CodecError> {
        if data.is_empty() {
            return Err(CodecError::BufferTooSmall { needed: 1, have: 0 });
        }
        let disc = data[0];
        let mut offset = 1;

        match disc {
            CMD_CREATE_TABLE => {
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let len = read_u32_at(&data, offset)? as usize;
                offset += 4;
                if data.len() < offset + len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + len,
                        have: data.len(),
                    });
                }
                let schema_ipc = data.slice(offset..offset + len);
                Ok(LanceCommand::CreateTable {
                    table_name,
                    schema_ipc,
                })
            }
            CMD_DROP_TABLE => {
                let (table_name, _) = decode_table_name_from_bytes(&data, offset)?;
                Ok(LanceCommand::DropTable { table_name })
            }
            CMD_APPEND_RECORDS => {
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let len = read_u32_at(&data, offset)? as usize;
                offset += 4;
                if data.len() < offset + len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + len,
                        have: data.len(),
                    });
                }
                // Zero-copy slice — shares the underlying Bytes buffer.
                let payload = data.slice(offset..offset + len);
                Ok(LanceCommand::AppendRecords {
                    table_name,
                    data: payload,
                })
            }
            CMD_SEAL_ACTIVE_SEGMENT => {
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let sealed_segment_id = read_u64_at(&data, offset)?;
                offset += 8;
                let new_active_segment_id = read_u64_at(&data, offset)?;
                offset += 8;
                if data.len() <= offset {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + 1,
                        have: data.len(),
                    });
                }
                let reason = match data[offset] {
                    SEAL_MAX_AGE => SealReason::MaxAge,
                    SEAL_MAX_SIZE => SealReason::MaxSize,
                    d => return Err(CodecError::InvalidDiscriminant(d)),
                };
                Ok(LanceCommand::SealActiveSegment {
                    table_name,
                    sealed_segment_id,
                    new_active_segment_id,
                    reason,
                })
            }
            CMD_BEGIN_FLUSH => {
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let segment_id = read_u64_at(&data, offset)?;
                Ok(LanceCommand::BeginFlush {
                    table_name,
                    segment_id,
                })
            }
            CMD_PROMOTE_TO_DEEP_STORAGE => {
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let segment_id = read_u64_at(&data, offset)?;
                offset += 8;
                let s3_manifest_version = read_u64_at(&data, offset)?;
                Ok(LanceCommand::PromoteToDeepStorage {
                    table_name,
                    segment_id,
                    s3_manifest_version,
                })
            }
            CMD_REGISTER_SESSION => {
                let session_id = read_u64_at(&data, offset)?;
                Ok(LanceCommand::RegisterSession { session_id })
            }
            CMD_PIN_VERSION => {
                let session_id = read_u64_at(&data, offset)?;
                offset += 8;
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let (tier, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let version = read_u64_at(&data, offset)?;
                Ok(LanceCommand::PinVersion {
                    session_id,
                    table_name,
                    tier,
                    version,
                })
            }
            CMD_UNPIN_VERSION => {
                let session_id = read_u64_at(&data, offset)?;
                offset += 8;
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let (tier, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let version = read_u64_at(&data, offset)?;
                Ok(LanceCommand::UnpinVersion {
                    session_id,
                    table_name,
                    tier,
                    version,
                })
            }
            CMD_EXPIRE_SESSION => {
                let session_id = read_u64_at(&data, offset)?;
                Ok(LanceCommand::ExpireSession { session_id })
            }
            CMD_DELETE_RECORDS => {
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let filter_len = read_u32_at(&data, offset)? as usize;
                offset += 4;
                if data.len() < offset + filter_len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + filter_len,
                        have: data.len(),
                    });
                }
                // Validate UTF-8 on the slice, then create String from the
                // zero-copy Bytes slice to avoid a redundant memcpy.
                let filter = validated_utf8_string(&data, offset, filter_len)?;
                Ok(LanceCommand::DeleteRecords { table_name, filter })
            }
            CMD_UPDATE_RECORDS => {
                let (table_name, consumed) = decode_table_name_from_bytes(&data, offset)?;
                offset += consumed;
                let filter_len = read_u32_at(&data, offset)? as usize;
                offset += 4;
                if data.len() < offset + filter_len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + filter_len,
                        have: data.len(),
                    });
                }
                let filter = validated_utf8_string(&data, offset, filter_len)?;
                offset += filter_len;
                let data_len = read_u32_at(&data, offset)? as usize;
                offset += 4;
                if data.len() < offset + data_len {
                    return Err(CodecError::BufferTooSmall {
                        needed: offset + data_len,
                        have: data.len(),
                    });
                }
                let payload = data.slice(offset..offset + data_len);
                Ok(LanceCommand::UpdateRecords {
                    table_name,
                    filter,
                    data: payload,
                })
            }
            CMD_TXN_CHUNK => {
                let txn_id = read_txn_id_at(&data, offset)?;
                offset += 16;
                let seq = read_u32_at(&data, offset)?;
                offset += 4;
                let (ops, consumed) = decode_txn_ops_from_bytes(&data, offset)?;
                let _ = consumed;
                Ok(LanceCommand::TxnChunk { txn_id, seq, ops })
            }
            CMD_TXN_COMMIT => {
                let txn_id = read_txn_id_at(&data, offset)?;
                offset += 16;
                let total_chunks = read_u32_at(&data, offset)?;
                Ok(LanceCommand::TxnCommit {
                    txn_id,
                    total_chunks,
                })
            }
            CMD_TXN_ABORT => {
                let txn_id = read_txn_id_at(&data, offset)?;
                Ok(LanceCommand::TxnAbort { txn_id })
            }
            d => Err(CodecError::InvalidDiscriminant(d)),
        }
    }
}

impl BorrowPayload for LanceCommand {
    fn payload_bytes(&self) -> &[u8] {
        match self {
            LanceCommand::AppendRecords { data, .. } => data,
            _ => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_create_table() {
        let cmd = LanceCommand::CreateTable {
            table_name: "logs".into(),
            schema_ipc: Bytes::from_static(&[1, 2, 3, 4]),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::CreateTable {
                table_name,
                schema_ipc,
            } => {
                assert_eq!(&*table_name, "logs");
                assert_eq!(&schema_ipc[..], &[1, 2, 3, 4]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_drop_table() {
        let cmd = LanceCommand::DropTable {
            table_name: "old_logs".into(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DropTable { table_name } => {
                assert_eq!(&*table_name, "old_logs");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_append_records() {
        let cmd = LanceCommand::AppendRecords {
            table_name: "metrics".into(),
            data: Bytes::from_static(&[1, 2, 3, 4, 5]),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::AppendRecords { table_name, data } => {
                assert_eq!(&*table_name, "metrics");
                assert_eq!(&data[..], &[1, 2, 3, 4, 5]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_seal() {
        let cmd = LanceCommand::SealActiveSegment {
            table_name: "events".into(),
            sealed_segment_id: 42,
            new_active_segment_id: 43,
            reason: SealReason::MaxAge,
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::SealActiveSegment {
                table_name,
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => {
                assert_eq!(&*table_name, "events");
                assert_eq!(sealed_segment_id, 42);
                assert_eq!(new_active_segment_id, 43);
                assert_eq!(reason, SealReason::MaxAge);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_begin_flush() {
        let cmd = LanceCommand::BeginFlush {
            table_name: "logs".into(),
            segment_id: 10,
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::BeginFlush {
                table_name,
                segment_id,
            } => {
                assert_eq!(&*table_name, "logs");
                assert_eq!(segment_id, 10);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_promote() {
        let cmd = LanceCommand::PromoteToDeepStorage {
            table_name: "logs".into(),
            segment_id: 7,
            s3_manifest_version: 99,
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::PromoteToDeepStorage {
                table_name,
                segment_id,
                s3_manifest_version,
            } => {
                assert_eq!(&*table_name, "logs");
                assert_eq!(segment_id, 7);
                assert_eq!(s3_manifest_version, 99);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn borrow_payload_append() {
        let data = Bytes::from_static(&[10, 20, 30]);
        let cmd = LanceCommand::AppendRecords {
            table_name: "t".into(),
            data: data.clone(),
        };
        assert_eq!(cmd.payload_bytes(), &[10, 20, 30]);
    }

    #[test]
    fn borrow_payload_non_append() {
        let cmd = LanceCommand::BeginFlush {
            table_name: "t".into(),
            segment_id: 1,
        };
        assert!(cmd.payload_bytes().is_empty());
    }

    #[test]
    fn zero_copy_decode_from_bytes() {
        let cmd = LanceCommand::AppendRecords {
            table_name: "metrics".into(),
            data: Bytes::from(vec![1u8, 2, 3, 4, 5]),
        };
        let encoded = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded = LanceCommand::decode_from_bytes(encoded).unwrap();
        match decoded {
            LanceCommand::AppendRecords { table_name, data } => {
                assert_eq!(&*table_name, "metrics");
                assert_eq!(&data[..], &[1, 2, 3, 4, 5]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_decode_create_table() {
        let cmd = LanceCommand::CreateTable {
            table_name: "logs".into(),
            schema_ipc: Bytes::from(vec![10u8, 20, 30]),
        };
        let encoded = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded = LanceCommand::decode_from_bytes(encoded).unwrap();
        match decoded {
            LanceCommand::CreateTable {
                table_name,
                schema_ipc,
            } => {
                assert_eq!(&*table_name, "logs");
                assert_eq!(&schema_ipc[..], &[10, 20, 30]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_delete_records() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "events".into(),
            filter: "id > 100 AND status = 'inactive'".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(&*table_name, "events");
                assert_eq!(filter, "id > 100 AND status = 'inactive'");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_delete_records_empty_filter() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "t".into(),
            filter: "".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(&*table_name, "t");
                assert_eq!(filter, "");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_update_records() {
        let cmd = LanceCommand::UpdateRecords {
            table_name: "metrics".into(),
            filter: "ts < '2024-01-01'".to_string(),
            data: Bytes::from_static(&[10, 20, 30, 40, 50]),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => {
                assert_eq!(&*table_name, "metrics");
                assert_eq!(filter, "ts < '2024-01-01'");
                assert_eq!(&data[..], &[10, 20, 30, 40, 50]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_update_records_empty_data() {
        let cmd = LanceCommand::UpdateRecords {
            table_name: "t".into(),
            filter: "id = 1".to_string(),
            data: Bytes::new(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => {
                assert_eq!(&*table_name, "t");
                assert_eq!(filter, "id = 1");
                assert!(data.is_empty());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_decode_delete_records() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "logs".into(),
            filter: "level = 'DEBUG'".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        let bytes = Bytes::from(encoded);
        let decoded = LanceCommand::decode_from_bytes(bytes).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(&*table_name, "logs");
                assert_eq!(filter, "level = 'DEBUG'");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_decode_update_records() {
        let cmd = LanceCommand::UpdateRecords {
            table_name: "data".into(),
            filter: "x > 0".to_string(),
            data: Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        let bytes = Bytes::from(encoded);
        let decoded = LanceCommand::decode_from_bytes(bytes).unwrap();
        match decoded {
            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => {
                assert_eq!(&*table_name, "data");
                assert_eq!(filter, "x > 0");
                assert_eq!(&data[..], &[1, 2, 3, 4, 5, 6, 7, 8]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn borrow_payload_delete_returns_empty() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "t".into(),
            filter: "id = 1".to_string(),
        };
        assert!(cmd.payload_bytes().is_empty());
    }

    #[test]
    fn roundtrip_delete_records_long_table_name() {
        // Table name is u16 length-prefixed, so 65535 bytes is the max.
        let long_name: Arc<str> = Arc::from("x".repeat(65535));
        let cmd = LanceCommand::DeleteRecords {
            table_name: long_name.clone(),
            filter: "id = 1".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(table_name.len(), 65535);
                assert_eq!(table_name, long_name);
                assert_eq!(filter, "id = 1");
            }
            _ => panic!("wrong variant"),
        }

        // Also verify zero-copy decode path.
        let bytes = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded2 = LanceCommand::decode_from_bytes(bytes).unwrap();
        match decoded2 {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(table_name.len(), 65535);
                assert_eq!(filter, "id = 1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_delete_records_max_filter() {
        // Filter is u32 length-prefixed, so very long filters should work fine.
        let long_filter = "id = 1 OR ".repeat(10_000); // 100,000 chars
        let cmd = LanceCommand::DeleteRecords {
            table_name: "t".into(),
            filter: long_filter.clone(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(&*table_name, "t");
                assert_eq!(filter.len(), long_filter.len());
                assert_eq!(filter, long_filter);
            }
            _ => panic!("wrong variant"),
        }

        // Also verify zero-copy decode path.
        let bytes = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded2 = LanceCommand::decode_from_bytes(bytes).unwrap();
        match decoded2 {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(&*table_name, "t");
                assert_eq!(filter, long_filter);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_delete_records_unicode_filter() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "\u{65e5}\u{672c}\u{8a9e}\u{30c6}\u{30fc}\u{30d6}\u{30eb}".into(),
            filter: "name = '\u{1f3af}'".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(
                    &*table_name,
                    "\u{65e5}\u{672c}\u{8a9e}\u{30c6}\u{30fc}\u{30d6}\u{30eb}"
                );
                assert_eq!(filter, "name = '\u{1f3af}'");
            }
            _ => panic!("wrong variant"),
        }
    }

    // =========================================================================
    // Transaction codec tests
    // =========================================================================

    fn sample_txn_id() -> TxnId {
        TxnId(0x0123456789abcdef_fedcba9876543210)
    }

    #[test]
    fn roundtrip_txn_chunk_single_append() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Append {
                table: "orders".into(),
                data: Bytes::from_static(&[1, 2, 3, 4, 5]),
            }],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::TxnChunk { txn_id, seq, ops } => {
                assert_eq!(txn_id, sample_txn_id());
                assert_eq!(seq, 0);
                assert_eq!(ops.len(), 1);
                match &ops[0] {
                    TxnOp::Append { table, data } => {
                        assert_eq!(&**table, "orders");
                        assert_eq!(&data[..], &[1, 2, 3, 4, 5]);
                    }
                    _ => panic!("wrong TxnOp variant"),
                }
            }
            _ => panic!("wrong LanceCommand variant"),
        }
    }

    #[test]
    fn roundtrip_txn_chunk_multiple_ops() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 3,
            ops: vec![
                TxnOp::Append {
                    table: "orders".into(),
                    data: Bytes::from_static(&[10, 20]),
                },
                TxnOp::Delete {
                    table: "inventory".into(),
                    filter: "product_id = 42".to_string(),
                },
                TxnOp::Update {
                    table: "metrics".into(),
                    filter: "ts < '2024-01-01'".to_string(),
                    data: Bytes::from_static(&[30, 40, 50]),
                },
                TxnOp::CreateTable {
                    table: "new_table".into(),
                    schema_ipc: Bytes::from_static(&[0xDE, 0xAD]),
                },
                TxnOp::DropTable {
                    table: "old_table".into(),
                },
            ],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::TxnChunk { txn_id, seq, ops } => {
                assert_eq!(txn_id, sample_txn_id());
                assert_eq!(seq, 3);
                assert_eq!(ops.len(), 5);

                match &ops[0] {
                    TxnOp::Append { table, data } => {
                        assert_eq!(&**table, "orders");
                        assert_eq!(&data[..], &[10, 20]);
                    }
                    _ => panic!("expected Append"),
                }
                match &ops[1] {
                    TxnOp::Delete { table, filter } => {
                        assert_eq!(&**table, "inventory");
                        assert_eq!(filter, "product_id = 42");
                    }
                    _ => panic!("expected Delete"),
                }
                match &ops[2] {
                    TxnOp::Update {
                        table,
                        filter,
                        data,
                    } => {
                        assert_eq!(&**table, "metrics");
                        assert_eq!(filter, "ts < '2024-01-01'");
                        assert_eq!(&data[..], &[30, 40, 50]);
                    }
                    _ => panic!("expected Update"),
                }
                match &ops[3] {
                    TxnOp::CreateTable { table, schema_ipc } => {
                        assert_eq!(&**table, "new_table");
                        assert_eq!(&schema_ipc[..], &[0xDE, 0xAD]);
                    }
                    _ => panic!("expected CreateTable"),
                }
                match &ops[4] {
                    TxnOp::DropTable { table } => {
                        assert_eq!(&**table, "old_table");
                    }
                    _ => panic!("expected DropTable"),
                }
            }
            _ => panic!("wrong LanceCommand variant"),
        }
    }

    #[test]
    fn roundtrip_txn_chunk_empty_ops() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::TxnChunk { txn_id, seq, ops } => {
                assert_eq!(txn_id, sample_txn_id());
                assert_eq!(seq, 0);
                assert!(ops.is_empty());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_txn_commit() {
        let cmd = LanceCommand::TxnCommit {
            txn_id: sample_txn_id(),
            total_chunks: 5,
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::TxnCommit {
                txn_id,
                total_chunks,
            } => {
                assert_eq!(txn_id, sample_txn_id());
                assert_eq!(total_chunks, 5);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_txn_abort() {
        let cmd = LanceCommand::TxnAbort {
            txn_id: sample_txn_id(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::TxnAbort { txn_id } => {
                assert_eq!(txn_id, sample_txn_id());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_txn_chunk() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 7,
            ops: vec![
                TxnOp::Append {
                    table: "t1".into(),
                    data: Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8]),
                },
                TxnOp::Update {
                    table: "t2".into(),
                    filter: "x > 0".to_string(),
                    data: Bytes::from(vec![9u8, 10, 11]),
                },
                TxnOp::CreateTable {
                    table: "t3".into(),
                    schema_ipc: Bytes::from(vec![0xAA, 0xBB]),
                },
            ],
        };
        let encoded = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded = LanceCommand::decode_from_bytes(encoded).unwrap();
        match decoded {
            LanceCommand::TxnChunk { txn_id, seq, ops } => {
                assert_eq!(txn_id, sample_txn_id());
                assert_eq!(seq, 7);
                assert_eq!(ops.len(), 3);
                match &ops[0] {
                    TxnOp::Append { table, data } => {
                        assert_eq!(&**table, "t1");
                        assert_eq!(&data[..], &[1, 2, 3, 4, 5, 6, 7, 8]);
                    }
                    _ => panic!("expected Append"),
                }
                match &ops[1] {
                    TxnOp::Update {
                        table,
                        filter,
                        data,
                    } => {
                        assert_eq!(&**table, "t2");
                        assert_eq!(filter, "x > 0");
                        assert_eq!(&data[..], &[9, 10, 11]);
                    }
                    _ => panic!("expected Update"),
                }
                match &ops[2] {
                    TxnOp::CreateTable { table, schema_ipc } => {
                        assert_eq!(&**table, "t3");
                        assert_eq!(&schema_ipc[..], &[0xAA, 0xBB]);
                    }
                    _ => panic!("expected CreateTable"),
                }
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_txn_commit() {
        let cmd = LanceCommand::TxnCommit {
            txn_id: sample_txn_id(),
            total_chunks: 42,
        };
        let encoded = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded = LanceCommand::decode_from_bytes(encoded).unwrap();
        match decoded {
            LanceCommand::TxnCommit {
                txn_id,
                total_chunks,
            } => {
                assert_eq!(txn_id, sample_txn_id());
                assert_eq!(total_chunks, 42);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_txn_abort() {
        let cmd = LanceCommand::TxnAbort {
            txn_id: sample_txn_id(),
        };
        let encoded = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded = LanceCommand::decode_from_bytes(encoded).unwrap();
        match decoded {
            LanceCommand::TxnAbort { txn_id } => {
                assert_eq!(txn_id, sample_txn_id());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_txn_chunk_large_payload() {
        // Verify large IPC payloads survive encode/decode.
        let big_data = Bytes::from(vec![0xABu8; 1_000_000]);
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Append {
                table: "big".into(),
                data: big_data.clone(),
            }],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::TxnChunk { ops, .. } => {
                assert_eq!(ops.len(), 1);
                match &ops[0] {
                    TxnOp::Append { data, .. } => assert_eq!(data.len(), 1_000_000),
                    _ => panic!("expected Append"),
                }
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_txn_chunk_unicode_table_names() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![
                TxnOp::Delete {
                    table: "\u{65e5}\u{672c}\u{8a9e}".into(),
                    filter: "id > 0".to_string(),
                },
                TxnOp::DropTable {
                    table: "\u{1f680}rockets".into(),
                },
            ],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::TxnChunk { ops, .. } => {
                assert_eq!(ops.len(), 2);
                match &ops[0] {
                    TxnOp::Delete { table, filter } => {
                        assert_eq!(&**table, "\u{65e5}\u{672c}\u{8a9e}");
                        assert_eq!(filter, "id > 0");
                    }
                    _ => panic!("expected Delete"),
                }
                match &ops[1] {
                    TxnOp::DropTable { table } => {
                        assert_eq!(&**table, "\u{1f680}rockets");
                    }
                    _ => panic!("expected DropTable"),
                }
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn txn_chunk_payload_bytes_empty() {
        // Transaction commands have no single payload.
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from_static(&[1, 2, 3]),
            }],
        };
        assert!(cmd.payload_bytes().is_empty());
    }

    #[test]
    fn txn_id_encode_deterministic() {
        // Ensure TxnId encodes to exactly 16 bytes in little-endian.
        let id = TxnId(0x0102030405060708_090a0b0c0d0e0f10);
        let mut buf = Vec::new();
        id.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 16);
        assert_eq!(
            buf,
            [
                0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03,
                0x02, 0x01
            ]
        );
    }

    #[test]
    fn stream_vs_zero_copy_consistency() {
        // Verify that stream decode and zero-copy decode produce identical results.
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 99,
            ops: vec![
                TxnOp::Append {
                    table: "a".into(),
                    data: Bytes::from_static(&[1, 2, 3]),
                },
                TxnOp::Delete {
                    table: "b".into(),
                    filter: "x = 1".to_string(),
                },
                TxnOp::Update {
                    table: "c".into(),
                    filter: "y > 0".to_string(),
                    data: Bytes::from_static(&[4, 5]),
                },
                TxnOp::CreateTable {
                    table: "d".into(),
                    schema_ipc: Bytes::from_static(&[6]),
                },
                TxnOp::DropTable { table: "e".into() },
            ],
        };
        let encoded_vec = cmd.encode_to_vec().unwrap();
        let stream_decoded = LanceCommand::decode_from_slice(&encoded_vec).unwrap();
        let zc_decoded = LanceCommand::decode_from_bytes(Bytes::from(encoded_vec.clone())).unwrap();

        // Compare by re-encoding both and checking byte equality.
        let re_encoded_stream = stream_decoded.encode_to_vec().unwrap();
        let re_encoded_zc = zc_decoded.encode_to_vec().unwrap();
        assert_eq!(re_encoded_stream, re_encoded_zc);
        assert_eq!(re_encoded_stream, encoded_vec);
    }

    // =========================================================================
    // Codec error path tests
    // =========================================================================

    #[test]
    fn decode_invalid_command_discriminant() {
        let buf = [0xFF];
        assert!(LanceCommand::decode_from_slice(&buf).is_err());
    }

    #[test]
    fn decode_empty_buffer() {
        let buf: &[u8] = &[];
        assert!(LanceCommand::decode_from_slice(buf).is_err());
    }

    #[test]
    fn decode_truncated_txn_chunk_missing_txn_id() {
        // Discriminant present but txn_id truncated.
        let buf = [CMD_TXN_CHUNK, 0x01, 0x02];
        assert!(LanceCommand::decode_from_slice(&buf).is_err());
    }

    #[test]
    fn decode_truncated_txn_commit_missing_total_chunks() {
        // Discriminant + full txn_id but missing total_chunks.
        let mut buf = vec![CMD_TXN_COMMIT];
        buf.extend_from_slice(&42u128.to_le_bytes());
        assert!(LanceCommand::decode_from_slice(&buf).is_err());
    }

    #[test]
    fn decode_truncated_txn_chunk_mid_op() {
        // Encode a valid TxnChunk then truncate mid-op.
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Append {
                table: "table_name".into(),
                data: Bytes::from(vec![1u8; 100]),
            }],
        };
        let full = cmd.encode_to_vec().unwrap();
        // Truncate: remove last 50 bytes (mid-payload).
        let truncated = &full[..full.len() - 50];
        assert!(LanceCommand::decode_from_slice(truncated).is_err());
    }

    #[test]
    fn zero_copy_decode_truncated_txn_chunk() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 64]),
            }],
        };
        let full = cmd.encode_to_vec().unwrap();
        let truncated = Bytes::from(full[..full.len() / 2].to_vec());
        assert!(LanceCommand::decode_from_bytes(truncated).is_err());
    }

    #[test]
    fn zero_copy_decode_invalid_discriminant() {
        let buf = Bytes::from_static(&[0xFF]);
        assert!(LanceCommand::decode_from_bytes(buf).is_err());
    }

    #[test]
    fn zero_copy_decode_empty_buffer() {
        let buf = Bytes::new();
        assert!(LanceCommand::decode_from_bytes(buf).is_err());
    }

    #[test]
    fn decode_txn_chunk_invalid_op_discriminant() {
        // Build a TxnChunk with 1 op but replace the op discriminant with 0xFF.
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::DropTable { table: "t".into() }],
        };
        let mut encoded = cmd.encode_to_vec().unwrap();
        // The op discriminant is the byte right after:
        //   1B cmd disc + 16B txn_id + 4B seq + 4B n_ops = offset 25
        encoded[25] = 0xFF;
        assert!(LanceCommand::decode_from_slice(&encoded).is_err());
    }

    #[test]
    fn zero_copy_decode_txn_chunk_invalid_op_discriminant() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::DropTable { table: "t".into() }],
        };
        let mut encoded = cmd.encode_to_vec().unwrap();
        encoded[25] = 0xFF;
        let bytes = Bytes::from(encoded);
        assert!(LanceCommand::decode_from_bytes(bytes).is_err());
    }

    #[test]
    fn decode_txn_op_delete_invalid_utf8_filter() {
        // Encode a valid TxnChunk with Delete op, then corrupt the filter bytes.
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Delete {
                table: "t".into(),
                filter: "id > 1".to_string(),
            }],
        };
        let mut encoded = cmd.encode_to_vec().unwrap();

        // Layout after cmd disc(1) + txn_id(16) + seq(4) + n_ops(4) + op_disc(1) +
        // table_name_len(4) + table_name("t"=1) + filter_len(4) = offset 35, then filter bytes.
        // Replace first byte of filter with invalid UTF-8.
        let filter_offset = 1 + 16 + 4 + 4 + 1 + 4 + 1 + 4; // = 35
        encoded[filter_offset] = 0xFF;
        encoded[filter_offset + 1] = 0xFE;

        assert!(
            LanceCommand::decode_from_slice(&encoded).is_err(),
            "should reject invalid UTF-8 in Delete filter"
        );
    }

    #[test]
    fn decode_txn_op_update_invalid_utf8_filter() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Update {
                table: "t".into(),
                filter: "id > 1".to_string(),
                data: Bytes::from_static(&[1, 2, 3]),
            }],
        };
        let mut encoded = cmd.encode_to_vec().unwrap();

        // Same layout up to filter bytes.
        let filter_offset = 1 + 16 + 4 + 4 + 1 + 4 + 1 + 4; // = 35
        encoded[filter_offset] = 0xFF;
        encoded[filter_offset + 1] = 0xFE;

        assert!(
            LanceCommand::decode_from_slice(&encoded).is_err(),
            "should reject invalid UTF-8 in Update filter"
        );
    }

    #[test]
    fn zero_copy_decode_txn_op_delete_invalid_utf8_filter() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Delete {
                table: "t".into(),
                filter: "id > 1".to_string(),
            }],
        };
        let mut encoded = cmd.encode_to_vec().unwrap();
        let filter_offset = 1 + 16 + 4 + 4 + 1 + 4 + 1 + 4;
        encoded[filter_offset] = 0xFF;
        encoded[filter_offset + 1] = 0xFE;

        let bytes = Bytes::from(encoded);
        assert!(
            LanceCommand::decode_from_bytes(bytes).is_err(),
            "zero-copy should reject invalid UTF-8 in Delete filter"
        );
    }

    #[test]
    fn zero_copy_decode_txn_op_update_invalid_utf8_filter() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: sample_txn_id(),
            seq: 0,
            ops: vec![TxnOp::Update {
                table: "t".into(),
                filter: "id > 1".to_string(),
                data: Bytes::from_static(&[1, 2, 3]),
            }],
        };
        let mut encoded = cmd.encode_to_vec().unwrap();
        let filter_offset = 1 + 16 + 4 + 4 + 1 + 4 + 1 + 4;
        encoded[filter_offset] = 0xFF;
        encoded[filter_offset + 1] = 0xFE;

        let bytes = Bytes::from(encoded);
        assert!(
            LanceCommand::decode_from_bytes(bytes).is_err(),
            "zero-copy should reject invalid UTF-8 in Update filter"
        );
    }

    #[test]
    fn zero_copy_decode_truncated_txn_commit() {
        // TxnCommit needs 1 (disc) + 16 (txn_id) + 4 (total_chunks) = 21 bytes.
        // Truncate after txn_id but before total_chunks.
        let cmd = LanceCommand::TxnCommit {
            txn_id: sample_txn_id(),
            total_chunks: 5,
        };
        let full = cmd.encode_to_vec().unwrap();
        let truncated = Bytes::from(full[..17].to_vec()); // 1 + 16, missing total_chunks
        assert!(
            LanceCommand::decode_from_bytes(truncated).is_err(),
            "should fail on truncated TxnCommit"
        );
    }

    #[test]
    fn zero_copy_decode_truncated_txn_abort() {
        // TxnAbort needs 1 (disc) + 16 (txn_id) = 17 bytes.
        // Truncate mid-txn_id.
        let cmd = LanceCommand::TxnAbort {
            txn_id: sample_txn_id(),
        };
        let full = cmd.encode_to_vec().unwrap();
        let truncated = Bytes::from(full[..9].to_vec()); // only 9 bytes, need 17
        assert!(
            LanceCommand::decode_from_bytes(truncated).is_err(),
            "should fail on truncated TxnAbort"
        );
    }
}
