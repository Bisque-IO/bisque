//! Raft codec implementations for MqCommand and MqResponse.
//!
//! Uses bincode serialization as the wire format for raft log entries.

use std::io::{Read, Write};

use bisque_raft::codec::{BorrowPayload, CodecError, Decode, Encode};

use crate::types::{MqCommand, MqResponse};

impl Encode for MqCommand {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let data =
            bincode::serde::encode_to_vec(self, bincode::config::standard()).map_err(|e| {
                CodecError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ))
            })?;
        writer.write_all(&data)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

impl Decode for MqCommand {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        let (cmd, _) = bincode::serde::decode_from_slice(&data, bincode::config::standard())
            .map_err(|e| {
                CodecError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ))
            })?;
        Ok(cmd)
    }
}

impl BorrowPayload for MqCommand {
    fn payload_bytes(&self) -> &[u8] {
        // MqCommand cannot provide a zero-copy borrow — fall back to empty.
        // The storage layer will use Encode instead.
        &[]
    }
}

impl Encode for MqResponse {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let data =
            bincode::serde::encode_to_vec(self, bincode::config::standard()).map_err(|e| {
                CodecError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ))
            })?;
        writer.write_all(&data)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

impl Decode for MqResponse {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        let (resp, _) = bincode::serde::decode_from_slice(&data, bincode::config::standard())
            .map_err(|e| {
                CodecError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ))
            })?;
        Ok(resp)
    }
}
