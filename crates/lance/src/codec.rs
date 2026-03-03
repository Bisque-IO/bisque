//! Encode/Decode implementations for LanceCommand.
//!
//! These are required for the bisque-raft multi-raft codec layer which
//! serializes log entries to the mmap storage and over the TCP transport.

use bisque_raft::multi::codec::{BorrowPayload, CodecError, Decode, Encode};
use std::io::{Read, Write};

use crate::types::{LanceCommand, SealReason};

// Discriminant bytes for LanceCommand variants
const CMD_APPEND_RECORDS: u8 = 0;
const CMD_SEAL_ACTIVE_SEGMENT: u8 = 1;
const CMD_BEGIN_FLUSH: u8 = 2;
const CMD_PROMOTE_TO_DEEP_STORAGE: u8 = 3;
const CMD_CREATE_TABLE: u8 = 4;
const CMD_DROP_TABLE: u8 = 5;

// Discriminant bytes for SealReason
const SEAL_MAX_AGE: u8 = 0;
const SEAL_MAX_SIZE: u8 = 1;

/// Encode a table name as a length-prefixed (u16) UTF-8 string.
fn encode_table_name<W: Write>(name: &str, writer: &mut W) -> Result<(), CodecError> {
    (name.len() as u16).encode(writer)?;
    writer.write_all(name.as_bytes())?;
    Ok(())
}

/// Decode a table name from a length-prefixed (u16) UTF-8 string.
fn decode_table_name<R: Read>(reader: &mut R) -> Result<String, CodecError> {
    let len = u16::decode(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| CodecError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
}

/// Encoded size of a table name (2-byte length prefix + UTF-8 bytes).
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
            LanceCommand::CreateTable { table_name, schema_ipc } => {
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
            LanceCommand::BeginFlush { table_name, segment_id } => {
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
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            LanceCommand::CreateTable { table_name, schema_ipc } => {
                table_name_size(table_name) + 4 + schema_ipc.len()
            }
            LanceCommand::DropTable { table_name } => {
                table_name_size(table_name)
            }
            LanceCommand::AppendRecords { table_name, data } => {
                table_name_size(table_name) + 4 + data.len()
            }
            LanceCommand::SealActiveSegment { table_name, reason, .. } => {
                table_name_size(table_name) + 8 + 8 + reason.encoded_size()
            }
            LanceCommand::BeginFlush { table_name, .. } => {
                table_name_size(table_name) + 8
            }
            LanceCommand::PromoteToDeepStorage { table_name, .. } => {
                table_name_size(table_name) + 8 + 8
            }
        }
    }
}

impl Decode for LanceCommand {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        match u8::decode(reader)? {
            CMD_CREATE_TABLE => {
                let table_name = decode_table_name(reader)?;
                let len = u32::decode(reader)? as usize;
                let mut schema_ipc = vec![0u8; len];
                reader.read_exact(&mut schema_ipc)?;
                Ok(LanceCommand::CreateTable { table_name, schema_ipc })
            }
            CMD_DROP_TABLE => {
                let table_name = decode_table_name(reader)?;
                Ok(LanceCommand::DropTable { table_name })
            }
            CMD_APPEND_RECORDS => {
                let table_name = decode_table_name(reader)?;
                let len = u32::decode(reader)? as usize;
                let mut data = vec![0u8; len];
                reader.read_exact(&mut data)?;
                Ok(LanceCommand::AppendRecords { table_name, data })
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
                Ok(LanceCommand::BeginFlush { table_name, segment_id })
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
            schema_ipc: vec![1, 2, 3, 4],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::CreateTable { table_name, schema_ipc } => {
                assert_eq!(table_name, "logs");
                assert_eq!(schema_ipc, vec![1, 2, 3, 4]);
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
            data: vec![1, 2, 3, 4, 5],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::AppendRecords { table_name, data } => {
                assert_eq!(table_name, "metrics");
                assert_eq!(data, vec![1, 2, 3, 4, 5]);
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
            LanceCommand::BeginFlush { table_name, segment_id } => {
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
        let data = vec![10, 20, 30];
        let cmd = LanceCommand::AppendRecords {
            table_name: "t".to_string(),
            data: data.clone(),
        };
        assert_eq!(cmd.payload_bytes(), &data);
    }

    #[test]
    fn borrow_payload_non_append() {
        let cmd = LanceCommand::BeginFlush {
            table_name: "t".to_string(),
            segment_id: 1,
        };
        assert!(cmd.payload_bytes().is_empty());
    }
}
