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

// Discriminant bytes for SealReason
const SEAL_MAX_AGE: u8 = 0;
const SEAL_MAX_SIZE: u8 = 1;

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
            LanceCommand::AppendRecords { data } => {
                CMD_APPEND_RECORDS.encode(writer)?;
                (data.len() as u32).encode(writer)?;
                writer.write_all(data)?;
            }
            LanceCommand::SealActiveSegment {
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => {
                CMD_SEAL_ACTIVE_SEGMENT.encode(writer)?;
                sealed_segment_id.encode(writer)?;
                new_active_segment_id.encode(writer)?;
                reason.encode(writer)?;
            }
            LanceCommand::BeginFlush { segment_id } => {
                CMD_BEGIN_FLUSH.encode(writer)?;
                segment_id.encode(writer)?;
            }
            LanceCommand::PromoteToDeepStorage {
                segment_id,
                s3_manifest_version,
            } => {
                CMD_PROMOTE_TO_DEEP_STORAGE.encode(writer)?;
                segment_id.encode(writer)?;
                s3_manifest_version.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            LanceCommand::AppendRecords { data } => 4 + data.len(),
            LanceCommand::SealActiveSegment { reason, .. } => 8 + 8 + reason.encoded_size(),
            LanceCommand::BeginFlush { .. } => 8,
            LanceCommand::PromoteToDeepStorage { .. } => 8 + 8,
        }
    }
}

impl Decode for LanceCommand {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        match u8::decode(reader)? {
            CMD_APPEND_RECORDS => {
                let len = u32::decode(reader)? as usize;
                let mut data = vec![0u8; len];
                reader.read_exact(&mut data)?;
                Ok(LanceCommand::AppendRecords { data })
            }
            CMD_SEAL_ACTIVE_SEGMENT => {
                let sealed_segment_id = u64::decode(reader)?;
                let new_active_segment_id = u64::decode(reader)?;
                let reason = SealReason::decode(reader)?;
                Ok(LanceCommand::SealActiveSegment {
                    sealed_segment_id,
                    new_active_segment_id,
                    reason,
                })
            }
            CMD_BEGIN_FLUSH => {
                let segment_id = u64::decode(reader)?;
                Ok(LanceCommand::BeginFlush { segment_id })
            }
            CMD_PROMOTE_TO_DEEP_STORAGE => {
                let segment_id = u64::decode(reader)?;
                let s3_manifest_version = u64::decode(reader)?;
                Ok(LanceCommand::PromoteToDeepStorage {
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
        // For AppendRecords, return the IPC data for size hints.
        // For other variants, return empty — the mmap storage uses
        // this for sizing; actual encoding goes through Encode.
        match self {
            LanceCommand::AppendRecords { data } => data,
            _ => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_append_records() {
        let cmd = LanceCommand::AppendRecords {
            data: vec![1, 2, 3, 4, 5],
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::AppendRecords { data } => assert_eq!(data, vec![1, 2, 3, 4, 5]),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_seal() {
        let cmd = LanceCommand::SealActiveSegment {
            sealed_segment_id: 42,
            new_active_segment_id: 43,
            reason: SealReason::MaxAge,
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::SealActiveSegment {
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => {
                assert_eq!(sealed_segment_id, 42);
                assert_eq!(new_active_segment_id, 43);
                assert_eq!(reason, SealReason::MaxAge);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_begin_flush() {
        let cmd = LanceCommand::BeginFlush { segment_id: 10 };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::BeginFlush { segment_id } => assert_eq!(segment_id, 10),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_promote() {
        let cmd = LanceCommand::PromoteToDeepStorage {
            segment_id: 7,
            s3_manifest_version: 99,
        };
        let encoded = cmd.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), cmd.encoded_size());
        let decoded = LanceCommand::decode_from_slice(&encoded).unwrap();
        match decoded {
            LanceCommand::PromoteToDeepStorage {
                segment_id,
                s3_manifest_version,
            } => {
                assert_eq!(segment_id, 7);
                assert_eq!(s3_manifest_version, 99);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn borrow_payload_append() {
        let data = vec![10, 20, 30];
        let cmd = LanceCommand::AppendRecords { data: data.clone() };
        assert_eq!(cmd.payload_bytes(), &data);
    }

    #[test]
    fn borrow_payload_non_append() {
        let cmd = LanceCommand::BeginFlush { segment_id: 1 };
        assert!(cmd.payload_bytes().is_empty());
    }
}
