//! Encode/Decode implementations for LanceCommand.
//!
//! These are required for the bisque-raft multi-raft codec layer which
//! serializes log entries to the mmap storage and over the TCP transport.
//!
//! The `decode_from_bytes` override enables zero-copy decoding of large
//! payloads (IPC-encoded RecordBatches) from mmap-backed `Bytes` buffers.

use bisque_raft::multi::codec::{BorrowPayload, CodecError, Decode, Encode};
use bytes::Bytes;
use std::io::{Read, Write};

use crate::types::{LanceCommand, SealReason};

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
fn decode_table_name<R: Read>(reader: &mut R) -> Result<String, CodecError> {
    let len = u16::decode(reader)? as usize;
    // Allocate directly as a String buffer — avoids intermediate Vec + from_utf8.
    let mut s = String::with_capacity(len);
    // SAFETY: We fill exactly `len` bytes via read_exact, then validate UTF-8.
    unsafe {
        let buf = s.as_mut_vec();
        buf.set_len(len);
        reader.read_exact(buf)?;
    }
    // Validate UTF-8. On error, `s` is dropped cleanly since it's valid memory.
    if std::str::from_utf8(s.as_bytes()).is_err() {
        return Err(CodecError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "table name is not valid UTF-8",
        )));
    }
    Ok(s)
}

/// Decode a table name from a `Bytes` buffer at the given offset.
/// Returns `(table_name, bytes_consumed)`.
#[inline]
fn decode_table_name_from_bytes(
    data: &Bytes,
    offset: usize,
) -> Result<(String, usize), CodecError> {
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
        .map_err(|e| CodecError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?
        .to_owned();
    Ok((name, 2 + len))
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

/// Encoded size of a table name (2-byte length prefix + UTF-8 bytes).
#[inline]
fn table_name_size(name: &str) -> usize {
    2 + name.len()
}

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
            LanceCommand::DeleteRecords {
                table_name,
                filter,
            } => {
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
            LanceCommand::DeleteRecords {
                table_name,
                filter,
            } => table_name_size(table_name) + 4 + filter.len(),
            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => table_name_size(table_name) + 4 + filter.len() + 4 + data.len(),
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
                let filter =
                    String::from_utf8(data[offset..offset + filter_len].to_vec()).map_err(
                        |_| {
                            CodecError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "filter is not valid UTF-8",
                            ))
                        },
                    )?;
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
                let filter =
                    String::from_utf8(data[offset..offset + filter_len].to_vec()).map_err(
                        |_| {
                            CodecError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "filter is not valid UTF-8",
                            ))
                        },
                    )?;
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
            table_name: "logs".to_string(),
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
                assert_eq!(table_name, "logs");
                assert_eq!(&schema_ipc[..], &[1, 2, 3, 4]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_drop_table() {
        let cmd = LanceCommand::DropTable {
            table_name: "old_logs".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DropTable { table_name } => {
                assert_eq!(table_name, "old_logs");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_append_records() {
        let cmd = LanceCommand::AppendRecords {
            table_name: "metrics".to_string(),
            data: Bytes::from_static(&[1, 2, 3, 4, 5]),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::AppendRecords { table_name, data } => {
                assert_eq!(table_name, "metrics");
                assert_eq!(&data[..], &[1, 2, 3, 4, 5]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_seal() {
        let cmd = LanceCommand::SealActiveSegment {
            table_name: "events".to_string(),
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
                assert_eq!(table_name, "events");
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
            table_name: "logs".to_string(),
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
                assert_eq!(table_name, "logs");
                assert_eq!(segment_id, 10);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_promote() {
        let cmd = LanceCommand::PromoteToDeepStorage {
            table_name: "logs".to_string(),
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
                assert_eq!(table_name, "logs");
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
            table_name: "t".to_string(),
            data: data.clone(),
        };
        assert_eq!(cmd.payload_bytes(), &[10, 20, 30]);
    }

    #[test]
    fn borrow_payload_non_append() {
        let cmd = LanceCommand::BeginFlush {
            table_name: "t".to_string(),
            segment_id: 1,
        };
        assert!(cmd.payload_bytes().is_empty());
    }

    #[test]
    fn zero_copy_decode_from_bytes() {
        let cmd = LanceCommand::AppendRecords {
            table_name: "metrics".to_string(),
            data: Bytes::from(vec![1u8, 2, 3, 4, 5]),
        };
        let encoded = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded = LanceCommand::decode_from_bytes(encoded).unwrap();
        match decoded {
            LanceCommand::AppendRecords { table_name, data } => {
                assert_eq!(table_name, "metrics");
                assert_eq!(&data[..], &[1, 2, 3, 4, 5]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_decode_create_table() {
        let cmd = LanceCommand::CreateTable {
            table_name: "logs".to_string(),
            schema_ipc: Bytes::from(vec![10u8, 20, 30]),
        };
        let encoded = Bytes::from(cmd.encode_to_vec().unwrap());
        let decoded = LanceCommand::decode_from_bytes(encoded).unwrap();
        match decoded {
            LanceCommand::CreateTable {
                table_name,
                schema_ipc,
            } => {
                assert_eq!(table_name, "logs");
                assert_eq!(&schema_ipc[..], &[10, 20, 30]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_delete_records() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "events".to_string(),
            filter: "id > 100 AND status = 'inactive'".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(table_name, "events");
                assert_eq!(filter, "id > 100 AND status = 'inactive'");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_delete_records_empty_filter() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "t".to_string(),
            filter: "".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(table_name, "t");
                assert_eq!(filter, "");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_update_records() {
        let cmd = LanceCommand::UpdateRecords {
            table_name: "metrics".to_string(),
            filter: "ts < '2024-01-01'".to_string(),
            data: Bytes::from_static(&[10, 20, 30, 40, 50]),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::UpdateRecords { table_name, filter, data } => {
                assert_eq!(table_name, "metrics");
                assert_eq!(filter, "ts < '2024-01-01'");
                assert_eq!(&data[..], &[10, 20, 30, 40, 50]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_update_records_empty_data() {
        let cmd = LanceCommand::UpdateRecords {
            table_name: "t".to_string(),
            filter: "id = 1".to_string(),
            data: Bytes::new(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::UpdateRecords { table_name, filter, data } => {
                assert_eq!(table_name, "t");
                assert_eq!(filter, "id = 1");
                assert!(data.is_empty());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_decode_delete_records() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "logs".to_string(),
            filter: "level = 'DEBUG'".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        let bytes = Bytes::from(encoded);
        let decoded = LanceCommand::decode_from_bytes(bytes).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(table_name, "logs");
                assert_eq!(filter, "level = 'DEBUG'");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn zero_copy_decode_update_records() {
        let cmd = LanceCommand::UpdateRecords {
            table_name: "data".to_string(),
            filter: "x > 0".to_string(),
            data: Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        let bytes = Bytes::from(encoded);
        let decoded = LanceCommand::decode_from_bytes(bytes).unwrap();
        match decoded {
            LanceCommand::UpdateRecords { table_name, filter, data } => {
                assert_eq!(table_name, "data");
                assert_eq!(filter, "x > 0");
                assert_eq!(&data[..], &[1, 2, 3, 4, 5, 6, 7, 8]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn borrow_payload_delete_returns_empty() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "t".to_string(),
            filter: "id = 1".to_string(),
        };
        assert!(cmd.payload_bytes().is_empty());
    }

    #[test]
    fn roundtrip_delete_records_long_table_name() {
        // Table name is u16 length-prefixed, so 65535 bytes is the max.
        let long_name = "x".repeat(65535);
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
            table_name: "t".to_string(),
            filter: long_filter.clone(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(table_name, "t");
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
                assert_eq!(table_name, "t");
                assert_eq!(filter, long_filter);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_delete_records_unicode_filter() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "\u{65e5}\u{672c}\u{8a9e}\u{30c6}\u{30fc}\u{30d6}\u{30eb}".to_string(),
            filter: "name = '\u{1f3af}'".to_string(),
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::DeleteRecords { table_name, filter } => {
                assert_eq!(table_name, "\u{65e5}\u{672c}\u{8a9e}\u{30c6}\u{30fc}\u{30d6}\u{30eb}");
                assert_eq!(filter, "name = '\u{1f3af}'");
            }
            _ => panic!("wrong variant"),
        }
    }
}
