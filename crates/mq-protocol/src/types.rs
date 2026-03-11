use std::io::{Cursor, Read, Write};

use bytes::{Buf, Bytes};

use crate::error::ProtocolError;
use crate::flat::FlatMessage;
use crate::tag::{ClientTag, ServerTag};

// =============================================================================
// Encode / Decode traits
// =============================================================================

pub trait Encode {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError>;
    fn encoded_size(&self) -> usize;

    fn encode_to_vec(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        self.encode(&mut buf)?;
        Ok(buf)
    }

    /// Encode into a reusable buffer, clearing it first.
    /// Avoids per-frame allocation when the caller retains the buffer.
    fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), ProtocolError> {
        buf.clear();
        buf.reserve(self.encoded_size());
        self.encode(buf)
    }
}

pub trait Decode: Sized {
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError>;

    fn decode_from_slice(data: &[u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(data);
        Self::decode(&mut cursor)
    }
}

// =============================================================================
// Primitive impls
// =============================================================================

impl Encode for u8 {
    #[inline]
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        w.write_all(std::slice::from_ref(self))?;
        Ok(())
    }
    #[inline]
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for u8 {
    #[inline]
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}

impl Encode for u16 {
    #[inline]
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        w.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    #[inline]
    fn encoded_size(&self) -> usize {
        2
    }
}

impl Decode for u16 {
    #[inline]
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let mut buf = [0u8; 2];
        r.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }
}

impl Encode for u32 {
    #[inline]
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        w.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    #[inline]
    fn encoded_size(&self) -> usize {
        4
    }
}

impl Decode for u32 {
    #[inline]
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl Encode for u64 {
    #[inline]
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        w.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    #[inline]
    fn encoded_size(&self) -> usize {
        8
    }
}

impl Decode for u64 {
    #[inline]
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

// Option<u64>: u8 tag (0=None, 1=Some) + u64 if Some.
impl Encode for Option<u64> {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        match self {
            None => 0u8.encode(w),
            Some(v) => {
                1u8.encode(w)?;
                v.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        match self {
            None => 1,
            Some(_) => 1 + 8,
        }
    }
}

impl Decode for Option<u64> {
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let tag = u8::decode(r)?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(u64::decode(r)?)),
            _ => Err(ProtocolError::UnknownTag(tag)),
        }
    }
}

// =============================================================================
// Byte buffer helpers (io::Read/Write based)
// =============================================================================

/// Encode a byte slice with u16 length prefix.
#[inline]
fn encode_bytes_u16<W: Write>(w: &mut W, data: &[u8]) -> Result<(), ProtocolError> {
    (data.len() as u16).encode(w)?;
    w.write_all(data)?;
    Ok(())
}

#[inline]
fn decode_bytes_u16<R: Read>(r: &mut R) -> Result<Vec<u8>, ProtocolError> {
    let len = u16::decode(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[inline]
fn encoded_size_bytes_u16(data: &[u8]) -> usize {
    2 + data.len()
}

/// Encode a byte slice with u32 length prefix.
#[inline]
fn encode_bytes_u32<W: Write>(w: &mut W, data: &[u8]) -> Result<(), ProtocolError> {
    (data.len() as u32).encode(w)?;
    w.write_all(data)?;
    Ok(())
}

#[inline]
fn decode_bytes_u32<R: Read>(r: &mut R) -> Result<Vec<u8>, ProtocolError> {
    let len = u32::decode(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[inline]
fn encoded_size_bytes_u32(data: &[u8]) -> usize {
    4 + data.len()
}

/// Encode a u64 vec with u16 count prefix.
#[inline]
fn encode_u64_vec<W: Write>(w: &mut W, ids: &[u64]) -> Result<(), ProtocolError> {
    (ids.len() as u16).encode(w)?;
    for id in ids {
        id.encode(w)?;
    }
    Ok(())
}

#[inline]
fn decode_u64_vec<R: Read>(r: &mut R) -> Result<Vec<u64>, ProtocolError> {
    let count = u16::decode(r)? as usize;
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(u64::decode(r)?);
    }
    Ok(ids)
}

#[inline]
fn encoded_size_u64_vec(ids: &[u64]) -> usize {
    2 + ids.len() * 8
}

// =============================================================================
// Zero-copy Bytes buffer helpers
// =============================================================================

#[inline]
fn read_u8_buf(buf: &mut Bytes) -> Result<u8, ProtocolError> {
    if buf.remaining() < 1 {
        return Err(ProtocolError::Truncated {
            need: 1,
            have: buf.remaining(),
        });
    }
    Ok(buf.get_u8())
}

#[inline]
fn read_u16_le_buf(buf: &mut Bytes) -> Result<u16, ProtocolError> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::Truncated {
            need: 2,
            have: buf.remaining(),
        });
    }
    Ok(buf.get_u16_le())
}

#[inline]
fn read_u32_le_buf(buf: &mut Bytes) -> Result<u32, ProtocolError> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::Truncated {
            need: 4,
            have: buf.remaining(),
        });
    }
    Ok(buf.get_u32_le())
}

#[inline]
fn read_u64_le_buf(buf: &mut Bytes) -> Result<u64, ProtocolError> {
    if buf.remaining() < 8 {
        return Err(ProtocolError::Truncated {
            need: 8,
            have: buf.remaining(),
        });
    }
    Ok(buf.get_u64_le())
}

/// Zero-copy: split off `len` bytes as a Bytes slice (u16 length prefix).
#[inline]
fn split_bytes_u16(buf: &mut Bytes) -> Result<Bytes, ProtocolError> {
    let len = read_u16_le_buf(buf)? as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Truncated {
            need: len,
            have: buf.remaining(),
        });
    }
    Ok(buf.split_to(len))
}

/// Zero-copy: split off `len` bytes as a Bytes slice (u32 length prefix).
#[inline]
fn split_bytes_u32(buf: &mut Bytes) -> Result<Bytes, ProtocolError> {
    let len = read_u32_le_buf(buf)? as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Truncated {
            need: len,
            have: buf.remaining(),
        });
    }
    Ok(buf.split_to(len))
}

#[inline]
fn read_u64_vec_buf(buf: &mut Bytes) -> Result<Vec<u64>, ProtocolError> {
    let count = read_u16_le_buf(buf)? as usize;
    if buf.remaining() < count * 8 {
        return Err(ProtocolError::Truncated {
            need: count * 8,
            have: buf.remaining(),
        });
    }
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(buf.get_u64_le());
    }
    Ok(ids)
}

#[inline]
fn read_option_u64_buf(buf: &mut Bytes) -> Result<Option<u64>, ProtocolError> {
    let tag = read_u8_buf(buf)?;
    match tag {
        0 => Ok(None),
        1 => Ok(Some(read_u64_le_buf(buf)?)),
        _ => Err(ProtocolError::UnknownTag(tag)),
    }
}

// =============================================================================
// ClientFrame
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum ClientFrame {
    Handshake {
        token: Bytes,
        consumer_id: Option<u64>,
    },
    Subscribe {
        sub_id: u32,
        group_id: u64,
        entity_type: u8,
        name_hash: u64,
        start_offset: u64,
        max_in_flight: u32,
    },
    Unsubscribe {
        sub_id: u32,
    },
    SetMaxInFlight {
        sub_id: u32,
        max_in_flight: u32,
    },
    Ack {
        sub_id: u32,
        message_ids: Vec<u64>,
    },
    Nack {
        sub_id: u32,
        message_ids: Vec<u64>,
    },
    CommitOffset {
        sub_id: u32,
        offset: u64,
    },
    Heartbeat,
    Close,
    SetByteBudget {
        budget_bytes: u64,
    },
    /// Publish pre-encoded flat messages to a topic (by name hash).
    /// Enables native high-throughput producers without going through
    /// SQS/Kafka/MQTT adapters.
    Publish {
        group_id: u64,
        topic_name_hash: u64,
        /// Pre-encoded flat message bytes (see `flat::FlatMessageBuilder`).
        messages: Vec<Bytes>,
    },

    // ── Consumer group operations ──────────────────────────────────────────
    /// Create a new consumer group.
    CreateGroup {
        group_id: u64,
        name: Bytes,
        auto_offset_reset: u8,
    },
    /// Delete a consumer group.
    DeleteGroup {
        group_id: u64,
        name_hash: u64,
    },
    /// Join a consumer group (triggers rebalance).
    JoinGroup {
        group_id: u64,
        name_hash: u64,
        member_id: Bytes,
        client_id: Bytes,
        session_timeout_ms: u32,
        rebalance_timeout_ms: u32,
        protocol_type: Bytes,
        /// `(protocol_name, metadata)` pairs.
        protocols: Vec<(Bytes, Bytes)>,
    },
    /// Sync partition assignments after a rebalance.
    SyncGroup {
        group_id: u64,
        name_hash: u64,
        generation: i32,
        member_id: Bytes,
        /// `(member_id, assignment)` pairs — only populated by the leader.
        assignments: Vec<(Bytes, Bytes)>,
    },
    /// Leave a consumer group.
    LeaveGroup {
        group_id: u64,
        name_hash: u64,
        member_id: Bytes,
    },
    /// Consumer group heartbeat (separate from connection heartbeat).
    GroupHeartbeat {
        group_id: u64,
        name_hash: u64,
        member_id: Bytes,
        generation: i32,
    },
    /// Commit offsets for a consumer group.
    CommitGroupOffset {
        group_id: u64,
        name_hash: u64,
        generation: i32,
        /// `(topic_id, partition_index, offset, metadata)` tuples.
        offsets: Vec<(u64, u32, u64, Bytes)>,
    },
    /// Fetch committed offsets for a consumer group.
    FetchGroupOffsets {
        group_id: u64,
        name_hash: u64,
        /// `(topic_id, partition_index)` pairs to fetch.
        partitions: Vec<(u64, u32)>,
    },
    /// List all consumer groups in a raft group.
    ListGroups {
        group_id: u64,
    },
    /// Describe a specific consumer group.
    DescribeGroup {
        group_id: u64,
        name_hash: u64,
    },
}

impl Encode for ClientFrame {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        match self {
            Self::Handshake { token, consumer_id } => {
                (ClientTag::Handshake as u8).encode(w)?;
                encode_bytes_u16(w, token)?;
                consumer_id.encode(w)?;
            }
            Self::Subscribe {
                sub_id,
                group_id,
                entity_type,
                name_hash,
                start_offset,
                max_in_flight,
            } => {
                (ClientTag::Subscribe as u8).encode(w)?;
                sub_id.encode(w)?;
                group_id.encode(w)?;
                entity_type.encode(w)?;
                name_hash.encode(w)?;
                start_offset.encode(w)?;
                max_in_flight.encode(w)?;
            }
            Self::Unsubscribe { sub_id } => {
                (ClientTag::Unsubscribe as u8).encode(w)?;
                sub_id.encode(w)?;
            }
            Self::SetMaxInFlight {
                sub_id,
                max_in_flight,
            } => {
                (ClientTag::SetMaxInFlight as u8).encode(w)?;
                sub_id.encode(w)?;
                max_in_flight.encode(w)?;
            }
            Self::Ack {
                sub_id,
                message_ids,
            } => {
                (ClientTag::Ack as u8).encode(w)?;
                sub_id.encode(w)?;
                encode_u64_vec(w, message_ids)?;
            }
            Self::Nack {
                sub_id,
                message_ids,
            } => {
                (ClientTag::Nack as u8).encode(w)?;
                sub_id.encode(w)?;
                encode_u64_vec(w, message_ids)?;
            }
            Self::CommitOffset { sub_id, offset } => {
                (ClientTag::CommitOffset as u8).encode(w)?;
                sub_id.encode(w)?;
                offset.encode(w)?;
            }
            Self::Heartbeat => {
                (ClientTag::Heartbeat as u8).encode(w)?;
            }
            Self::Close => {
                (ClientTag::Close as u8).encode(w)?;
            }
            Self::SetByteBudget { budget_bytes } => {
                (ClientTag::SetByteBudget as u8).encode(w)?;
                budget_bytes.encode(w)?;
            }
            Self::Publish {
                group_id,
                topic_name_hash,
                messages,
            } => {
                (ClientTag::Publish as u8).encode(w)?;
                group_id.encode(w)?;
                topic_name_hash.encode(w)?;
                (messages.len() as u16).encode(w)?;
                for msg in messages {
                    encode_bytes_u32(w, msg)?;
                }
            }
            Self::CreateGroup {
                group_id,
                name,
                auto_offset_reset,
            } => {
                (ClientTag::CreateGroup as u8).encode(w)?;
                group_id.encode(w)?;
                encode_bytes_u16(w, name)?;
                auto_offset_reset.encode(w)?;
            }
            Self::DeleteGroup {
                group_id,
                name_hash,
            } => {
                (ClientTag::DeleteGroup as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
            }
            Self::JoinGroup {
                group_id,
                name_hash,
                member_id,
                client_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                protocol_type,
                protocols,
            } => {
                (ClientTag::JoinGroup as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
                encode_bytes_u16(w, member_id)?;
                encode_bytes_u16(w, client_id)?;
                session_timeout_ms.encode(w)?;
                rebalance_timeout_ms.encode(w)?;
                encode_bytes_u16(w, protocol_type)?;
                (protocols.len() as u16).encode(w)?;
                for (name, metadata) in protocols {
                    encode_bytes_u16(w, name)?;
                    encode_bytes_u32(w, metadata)?;
                }
            }
            Self::SyncGroup {
                group_id,
                name_hash,
                generation,
                member_id,
                assignments,
            } => {
                (ClientTag::SyncGroup as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
                (*generation as u32).encode(w)?;
                encode_bytes_u16(w, member_id)?;
                (assignments.len() as u16).encode(w)?;
                for (mid, assignment) in assignments {
                    encode_bytes_u16(w, mid)?;
                    encode_bytes_u32(w, assignment)?;
                }
            }
            Self::LeaveGroup {
                group_id,
                name_hash,
                member_id,
            } => {
                (ClientTag::LeaveGroup as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
                encode_bytes_u16(w, member_id)?;
            }
            Self::GroupHeartbeat {
                group_id,
                name_hash,
                member_id,
                generation,
            } => {
                (ClientTag::GroupHeartbeat as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
                encode_bytes_u16(w, member_id)?;
                (*generation as u32).encode(w)?;
            }
            Self::CommitGroupOffset {
                group_id,
                name_hash,
                generation,
                offsets,
            } => {
                (ClientTag::CommitGroupOffset as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
                (*generation as u32).encode(w)?;
                (offsets.len() as u16).encode(w)?;
                for (topic_id, partition_index, offset, metadata) in offsets {
                    topic_id.encode(w)?;
                    partition_index.encode(w)?;
                    offset.encode(w)?;
                    encode_bytes_u16(w, metadata)?;
                }
            }
            Self::FetchGroupOffsets {
                group_id,
                name_hash,
                partitions,
            } => {
                (ClientTag::FetchGroupOffsets as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
                (partitions.len() as u16).encode(w)?;
                for (topic_id, partition_index) in partitions {
                    topic_id.encode(w)?;
                    partition_index.encode(w)?;
                }
            }
            Self::ListGroups { group_id } => {
                (ClientTag::ListGroups as u8).encode(w)?;
                group_id.encode(w)?;
            }
            Self::DescribeGroup {
                group_id,
                name_hash,
            } => {
                (ClientTag::DescribeGroup as u8).encode(w)?;
                group_id.encode(w)?;
                name_hash.encode(w)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            Self::Handshake { token, consumer_id } => {
                encoded_size_bytes_u16(token) + consumer_id.encoded_size()
            }
            Self::Subscribe { .. } => 4 + 8 + 1 + 8 + 8 + 4, // 33
            Self::Unsubscribe { .. } => 4,
            Self::SetMaxInFlight { .. } => 4 + 4,
            Self::Ack { message_ids, .. } => 4 + encoded_size_u64_vec(message_ids),
            Self::Nack { message_ids, .. } => 4 + encoded_size_u64_vec(message_ids),
            Self::CommitOffset { .. } => 4 + 8,
            Self::Heartbeat => 0,
            Self::Close => 0,
            Self::SetByteBudget { .. } => 8,
            Self::Publish { messages, .. } => {
                8 + 8
                    + 2
                    + messages
                        .iter()
                        .map(|m| encoded_size_bytes_u32(m))
                        .sum::<usize>()
            }
            Self::CreateGroup { name, .. } => 8 + encoded_size_bytes_u16(name) + 1,
            Self::DeleteGroup { .. } => 8 + 8,
            Self::JoinGroup {
                member_id,
                client_id,
                protocol_type,
                protocols,
                ..
            } => {
                8 + 8
                    + encoded_size_bytes_u16(member_id)
                    + encoded_size_bytes_u16(client_id)
                    + 4
                    + 4
                    + encoded_size_bytes_u16(protocol_type)
                    + 2
                    + protocols
                        .iter()
                        .map(|(n, m)| encoded_size_bytes_u16(n) + encoded_size_bytes_u32(m))
                        .sum::<usize>()
            }
            Self::SyncGroup {
                member_id,
                assignments,
                ..
            } => {
                8 + 8
                    + 4
                    + encoded_size_bytes_u16(member_id)
                    + 2
                    + assignments
                        .iter()
                        .map(|(mid, a)| encoded_size_bytes_u16(mid) + encoded_size_bytes_u32(a))
                        .sum::<usize>()
            }
            Self::LeaveGroup { member_id, .. } => 8 + 8 + encoded_size_bytes_u16(member_id),
            Self::GroupHeartbeat { member_id, .. } => 8 + 8 + encoded_size_bytes_u16(member_id) + 4,
            Self::CommitGroupOffset { offsets, .. } => {
                8 + 8
                    + 4
                    + 2
                    + offsets
                        .iter()
                        .map(|(_, _, _, meta)| 8 + 4 + 8 + encoded_size_bytes_u16(meta))
                        .sum::<usize>()
            }
            Self::FetchGroupOffsets { partitions, .. } => 8 + 8 + 2 + partitions.len() * (8 + 4),
            Self::ListGroups { .. } => 8,
            Self::DescribeGroup { .. } => 8 + 8,
        }
    }
}

impl Decode for ClientFrame {
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let tag = ClientTag::try_from(u8::decode(r)?)?;
        match tag {
            ClientTag::Handshake => Ok(Self::Handshake {
                token: Bytes::from(decode_bytes_u16(r)?),
                consumer_id: Option::<u64>::decode(r)?,
            }),
            ClientTag::Subscribe => Ok(Self::Subscribe {
                sub_id: u32::decode(r)?,
                group_id: u64::decode(r)?,
                entity_type: u8::decode(r)?,
                name_hash: u64::decode(r)?,
                start_offset: u64::decode(r)?,
                max_in_flight: u32::decode(r)?,
            }),
            ClientTag::Unsubscribe => Ok(Self::Unsubscribe {
                sub_id: u32::decode(r)?,
            }),
            ClientTag::SetMaxInFlight => Ok(Self::SetMaxInFlight {
                sub_id: u32::decode(r)?,
                max_in_flight: u32::decode(r)?,
            }),
            ClientTag::Ack => Ok(Self::Ack {
                sub_id: u32::decode(r)?,
                message_ids: decode_u64_vec(r)?,
            }),
            ClientTag::Nack => Ok(Self::Nack {
                sub_id: u32::decode(r)?,
                message_ids: decode_u64_vec(r)?,
            }),
            ClientTag::CommitOffset => Ok(Self::CommitOffset {
                sub_id: u32::decode(r)?,
                offset: u64::decode(r)?,
            }),
            ClientTag::Heartbeat => Ok(Self::Heartbeat),
            ClientTag::Close => Ok(Self::Close),
            ClientTag::SetByteBudget => Ok(Self::SetByteBudget {
                budget_bytes: u64::decode(r)?,
            }),
            ClientTag::Publish => {
                let group_id = u64::decode(r)?;
                let topic_name_hash = u64::decode(r)?;
                let count = u16::decode(r)? as usize;
                let mut messages = Vec::with_capacity(count);
                for _ in 0..count {
                    messages.push(Bytes::from(decode_bytes_u32(r)?));
                }
                Ok(Self::Publish {
                    group_id,
                    topic_name_hash,
                    messages,
                })
            }
            ClientTag::CreateGroup => Ok(Self::CreateGroup {
                group_id: u64::decode(r)?,
                name: Bytes::from(decode_bytes_u16(r)?),
                auto_offset_reset: u8::decode(r)?,
            }),
            ClientTag::DeleteGroup => Ok(Self::DeleteGroup {
                group_id: u64::decode(r)?,
                name_hash: u64::decode(r)?,
            }),
            ClientTag::JoinGroup => {
                let group_id = u64::decode(r)?;
                let name_hash = u64::decode(r)?;
                let member_id = Bytes::from(decode_bytes_u16(r)?);
                let client_id = Bytes::from(decode_bytes_u16(r)?);
                let session_timeout_ms = u32::decode(r)?;
                let rebalance_timeout_ms = u32::decode(r)?;
                let protocol_type = Bytes::from(decode_bytes_u16(r)?);
                let count = u16::decode(r)? as usize;
                let mut protocols = Vec::with_capacity(count);
                for _ in 0..count {
                    let name = Bytes::from(decode_bytes_u16(r)?);
                    let metadata = Bytes::from(decode_bytes_u32(r)?);
                    protocols.push((name, metadata));
                }
                Ok(Self::JoinGroup {
                    group_id,
                    name_hash,
                    member_id,
                    client_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    protocol_type,
                    protocols,
                })
            }
            ClientTag::SyncGroup => {
                let group_id = u64::decode(r)?;
                let name_hash = u64::decode(r)?;
                let generation = u32::decode(r)? as i32;
                let member_id = Bytes::from(decode_bytes_u16(r)?);
                let count = u16::decode(r)? as usize;
                let mut assignments = Vec::with_capacity(count);
                for _ in 0..count {
                    let mid = Bytes::from(decode_bytes_u16(r)?);
                    let assignment = Bytes::from(decode_bytes_u32(r)?);
                    assignments.push((mid, assignment));
                }
                Ok(Self::SyncGroup {
                    group_id,
                    name_hash,
                    generation,
                    member_id,
                    assignments,
                })
            }
            ClientTag::LeaveGroup => Ok(Self::LeaveGroup {
                group_id: u64::decode(r)?,
                name_hash: u64::decode(r)?,
                member_id: Bytes::from(decode_bytes_u16(r)?),
            }),
            ClientTag::GroupHeartbeat => Ok(Self::GroupHeartbeat {
                group_id: u64::decode(r)?,
                name_hash: u64::decode(r)?,
                member_id: Bytes::from(decode_bytes_u16(r)?),
                generation: u32::decode(r)? as i32,
            }),
            ClientTag::CommitGroupOffset => {
                let group_id = u64::decode(r)?;
                let name_hash = u64::decode(r)?;
                let generation = u32::decode(r)? as i32;
                let count = u16::decode(r)? as usize;
                let mut offsets = Vec::with_capacity(count);
                for _ in 0..count {
                    let topic_id = u64::decode(r)?;
                    let partition_index = u32::decode(r)?;
                    let offset = u64::decode(r)?;
                    let metadata = Bytes::from(decode_bytes_u16(r)?);
                    offsets.push((topic_id, partition_index, offset, metadata));
                }
                Ok(Self::CommitGroupOffset {
                    group_id,
                    name_hash,
                    generation,
                    offsets,
                })
            }
            ClientTag::FetchGroupOffsets => {
                let group_id = u64::decode(r)?;
                let name_hash = u64::decode(r)?;
                let count = u16::decode(r)? as usize;
                let mut partitions = Vec::with_capacity(count);
                for _ in 0..count {
                    let topic_id = u64::decode(r)?;
                    let partition_index = u32::decode(r)?;
                    partitions.push((topic_id, partition_index));
                }
                Ok(Self::FetchGroupOffsets {
                    group_id,
                    name_hash,
                    partitions,
                })
            }
            ClientTag::ListGroups => Ok(Self::ListGroups {
                group_id: u64::decode(r)?,
            }),
            ClientTag::DescribeGroup => Ok(Self::DescribeGroup {
                group_id: u64::decode(r)?,
                name_hash: u64::decode(r)?,
            }),
        }
    }
}

impl ClientFrame {
    /// Zero-copy decode from a `Bytes` buffer.
    /// Variable-length fields are slices into the original buffer — no heap allocation.
    pub fn decode_from_bytes(mut buf: Bytes) -> Result<Self, ProtocolError> {
        let tag = ClientTag::try_from(read_u8_buf(&mut buf)?)?;
        match tag {
            ClientTag::Handshake => Ok(Self::Handshake {
                token: split_bytes_u16(&mut buf)?,
                consumer_id: read_option_u64_buf(&mut buf)?,
            }),
            ClientTag::Subscribe => Ok(Self::Subscribe {
                sub_id: read_u32_le_buf(&mut buf)?,
                group_id: read_u64_le_buf(&mut buf)?,
                entity_type: read_u8_buf(&mut buf)?,
                name_hash: read_u64_le_buf(&mut buf)?,
                start_offset: read_u64_le_buf(&mut buf)?,
                max_in_flight: read_u32_le_buf(&mut buf)?,
            }),
            ClientTag::Unsubscribe => Ok(Self::Unsubscribe {
                sub_id: read_u32_le_buf(&mut buf)?,
            }),
            ClientTag::SetMaxInFlight => Ok(Self::SetMaxInFlight {
                sub_id: read_u32_le_buf(&mut buf)?,
                max_in_flight: read_u32_le_buf(&mut buf)?,
            }),
            ClientTag::Ack => Ok(Self::Ack {
                sub_id: read_u32_le_buf(&mut buf)?,
                message_ids: read_u64_vec_buf(&mut buf)?,
            }),
            ClientTag::Nack => Ok(Self::Nack {
                sub_id: read_u32_le_buf(&mut buf)?,
                message_ids: read_u64_vec_buf(&mut buf)?,
            }),
            ClientTag::CommitOffset => Ok(Self::CommitOffset {
                sub_id: read_u32_le_buf(&mut buf)?,
                offset: read_u64_le_buf(&mut buf)?,
            }),
            ClientTag::Heartbeat => Ok(Self::Heartbeat),
            ClientTag::Close => Ok(Self::Close),
            ClientTag::SetByteBudget => Ok(Self::SetByteBudget {
                budget_bytes: read_u64_le_buf(&mut buf)?,
            }),
            ClientTag::Publish => {
                let group_id = read_u64_le_buf(&mut buf)?;
                let topic_name_hash = read_u64_le_buf(&mut buf)?;
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut messages = Vec::with_capacity(count);
                for _ in 0..count {
                    messages.push(split_bytes_u32(&mut buf)?);
                }
                Ok(Self::Publish {
                    group_id,
                    topic_name_hash,
                    messages,
                })
            }
            ClientTag::CreateGroup => Ok(Self::CreateGroup {
                group_id: read_u64_le_buf(&mut buf)?,
                name: split_bytes_u16(&mut buf)?,
                auto_offset_reset: read_u8_buf(&mut buf)?,
            }),
            ClientTag::DeleteGroup => Ok(Self::DeleteGroup {
                group_id: read_u64_le_buf(&mut buf)?,
                name_hash: read_u64_le_buf(&mut buf)?,
            }),
            ClientTag::JoinGroup => {
                let group_id = read_u64_le_buf(&mut buf)?;
                let name_hash = read_u64_le_buf(&mut buf)?;
                let member_id = split_bytes_u16(&mut buf)?;
                let client_id = split_bytes_u16(&mut buf)?;
                let session_timeout_ms = read_u32_le_buf(&mut buf)?;
                let rebalance_timeout_ms = read_u32_le_buf(&mut buf)?;
                let protocol_type = split_bytes_u16(&mut buf)?;
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut protocols = Vec::with_capacity(count);
                for _ in 0..count {
                    let name = split_bytes_u16(&mut buf)?;
                    let metadata = split_bytes_u32(&mut buf)?;
                    protocols.push((name, metadata));
                }
                Ok(Self::JoinGroup {
                    group_id,
                    name_hash,
                    member_id,
                    client_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    protocol_type,
                    protocols,
                })
            }
            ClientTag::SyncGroup => {
                let group_id = read_u64_le_buf(&mut buf)?;
                let name_hash = read_u64_le_buf(&mut buf)?;
                let generation = read_u32_le_buf(&mut buf)? as i32;
                let member_id = split_bytes_u16(&mut buf)?;
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut assignments = Vec::with_capacity(count);
                for _ in 0..count {
                    let mid = split_bytes_u16(&mut buf)?;
                    let assignment = split_bytes_u32(&mut buf)?;
                    assignments.push((mid, assignment));
                }
                Ok(Self::SyncGroup {
                    group_id,
                    name_hash,
                    generation,
                    member_id,
                    assignments,
                })
            }
            ClientTag::LeaveGroup => Ok(Self::LeaveGroup {
                group_id: read_u64_le_buf(&mut buf)?,
                name_hash: read_u64_le_buf(&mut buf)?,
                member_id: split_bytes_u16(&mut buf)?,
            }),
            ClientTag::GroupHeartbeat => Ok(Self::GroupHeartbeat {
                group_id: read_u64_le_buf(&mut buf)?,
                name_hash: read_u64_le_buf(&mut buf)?,
                member_id: split_bytes_u16(&mut buf)?,
                generation: read_u32_le_buf(&mut buf)? as i32,
            }),
            ClientTag::CommitGroupOffset => {
                let group_id = read_u64_le_buf(&mut buf)?;
                let name_hash = read_u64_le_buf(&mut buf)?;
                let generation = read_u32_le_buf(&mut buf)? as i32;
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut offsets = Vec::with_capacity(count);
                for _ in 0..count {
                    let topic_id = read_u64_le_buf(&mut buf)?;
                    let partition_index = read_u32_le_buf(&mut buf)?;
                    let offset = read_u64_le_buf(&mut buf)?;
                    let metadata = split_bytes_u16(&mut buf)?;
                    offsets.push((topic_id, partition_index, offset, metadata));
                }
                Ok(Self::CommitGroupOffset {
                    group_id,
                    name_hash,
                    generation,
                    offsets,
                })
            }
            ClientTag::FetchGroupOffsets => {
                let group_id = read_u64_le_buf(&mut buf)?;
                let name_hash = read_u64_le_buf(&mut buf)?;
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut partitions = Vec::with_capacity(count);
                for _ in 0..count {
                    let topic_id = read_u64_le_buf(&mut buf)?;
                    let partition_index = read_u32_le_buf(&mut buf)?;
                    partitions.push((topic_id, partition_index));
                }
                Ok(Self::FetchGroupOffsets {
                    group_id,
                    name_hash,
                    partitions,
                })
            }
            ClientTag::ListGroups => Ok(Self::ListGroups {
                group_id: read_u64_le_buf(&mut buf)?,
            }),
            ClientTag::DescribeGroup => Ok(Self::DescribeGroup {
                group_id: read_u64_le_buf(&mut buf)?,
                name_hash: read_u64_le_buf(&mut buf)?,
            }),
        }
    }
}

// =============================================================================
// ServerFrame
// =============================================================================

/// Wire message wrapping a zero-copy `FlatMessage` with protocol-level routing
/// fields (`sub_id`, `message_id`).
///
/// Wire format: `[sub_id:4][message_id:8][flat_len:4][flat_bytes...]`
///
/// The client accesses message fields via `flat.value()`, `flat.key()`,
/// `flat.timestamp()`, `flat.headers()`, etc. — all zero-copy.
#[derive(Debug, Clone)]
pub struct WireMessage {
    pub sub_id: u32,
    pub message_id: u64,
    pub flat: FlatMessage,
}

impl PartialEq for WireMessage {
    fn eq(&self, other: &Self) -> bool {
        self.sub_id == other.sub_id
            && self.message_id == other.message_id
            && self.flat.as_bytes() == other.flat.as_bytes()
    }
}

impl WireMessage {
    /// Total byte size of message payload (flat message buffer).
    #[inline]
    pub fn payload_bytes(&self) -> u64 {
        self.flat.as_bytes().len() as u64
    }

    /// Encode the message body (without frame tag) into a writer.
    #[inline]
    fn encode_body<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        self.sub_id.encode(w)?;
        self.message_id.encode(w)?;
        encode_bytes_u32(w, self.flat.as_bytes())?;
        Ok(())
    }

    /// Encoded size of the message body (without frame tag).
    #[inline]
    fn body_encoded_size(&self) -> usize {
        4 + 8 + encoded_size_bytes_u32(self.flat.as_bytes())
    }

    /// Decode message body from an `io::Read` (without tag byte).
    #[inline]
    fn decode_body<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let sub_id = u32::decode(r)?;
        let message_id = u64::decode(r)?;
        let flat_bytes = Bytes::from(decode_bytes_u32(r)?);
        let flat = FlatMessage::new(flat_bytes).ok_or_else(|| ProtocolError::Truncated {
            need: crate::flat::HEADER_SIZE,
            have: 0,
        })?;
        Ok(Self {
            sub_id,
            message_id,
            flat,
        })
    }

    /// Zero-copy decode message body from a `Bytes` buffer (without tag byte).
    #[inline]
    fn decode_body_bytes(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        let sub_id = read_u32_le_buf(buf)?;
        let message_id = read_u64_le_buf(buf)?;
        let flat_bytes = split_bytes_u32(buf)?;
        let flat = FlatMessage::new(flat_bytes).ok_or_else(|| ProtocolError::Truncated {
            need: crate::flat::HEADER_SIZE,
            have: 0,
        })?;
        Ok(Self {
            sub_id,
            message_id,
            flat,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServerFrame {
    HandshakeOk {
        consumer_id: u64,
        session_token: Bytes,
    },
    HandshakeErr {
        code: u16,
        message: Bytes,
    },
    Subscribed {
        sub_id: u32,
        entity_id: u64,
    },
    Message(WireMessage),
    /// Batch of messages sharing one TCP frame. Reduces per-message framing
    /// overhead and enables single-syscall delivery of multiple messages.
    MessageBatch(Vec<WireMessage>),
    SubscriptionErr {
        sub_id: u32,
        code: u16,
        message: Bytes,
    },
    Heartbeat {
        server_time_ms: u64,
    },
    Close {
        reason: Bytes,
    },
    /// Pre-encoded frame bytes (tag + payload). Used by the server to send
    /// wire-format data that was encoded directly from the storage layer
    /// (e.g. FlatMessage → wire bytes) without materializing intermediate structs.
    /// The Vec contains the complete frame body including the tag byte.
    PreEncoded(Vec<u8>),

    // ── Consumer group responses ───────────────────────────────────────────
    /// Consumer group created successfully.
    GroupCreated {
        consumer_group_id: u64,
    },
    /// Consumer group deleted successfully.
    GroupDeleted,
    /// JoinGroup response with group membership info.
    GroupJoined {
        generation: i32,
        leader: Bytes,
        member_id: Bytes,
        protocol_name: Bytes,
        is_leader: u8,
        /// `(member_id, protocol_metadata)` — only populated for the leader.
        members: Vec<(Bytes, Bytes)>,
    },
    /// SyncGroup response with the member's partition assignment.
    GroupSynced {
        assignment: Bytes,
    },
    /// LeaveGroup completed.
    GroupLeft,
    /// Consumer group heartbeat acknowledged.
    GroupHeartbeatOk,
    /// Consumer group offset commit acknowledged.
    GroupOffsetCommitted,
    /// Fetched committed offsets for requested partitions.
    /// `(topic_id, partition_index, committed_offset, metadata)`.
    GroupOffsetsFetched {
        offsets: Vec<(u64, u32, u64, Bytes)>,
    },
    /// List of consumer groups.
    /// `(consumer_group_id, name, phase, protocol_type)`.
    GroupList {
        groups: Vec<(u64, Bytes, u8, Bytes)>,
    },
    /// Detailed consumer group description.
    GroupDescription {
        consumer_group_id: u64,
        name: Bytes,
        phase: u8,
        protocol_type: Bytes,
        protocol_name: Bytes,
        leader: Bytes,
        generation: i32,
        /// `(member_id, client_id, assignment)`.
        members: Vec<(Bytes, Bytes, Bytes)>,
    },
    /// Consumer group operation error.
    GroupError {
        code: u16,
        message: Bytes,
    },
}

impl Encode for ServerFrame {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), ProtocolError> {
        match self {
            Self::HandshakeOk {
                consumer_id,
                session_token,
            } => {
                (ServerTag::HandshakeOk as u8).encode(w)?;
                consumer_id.encode(w)?;
                encode_bytes_u16(w, session_token)?;
            }
            Self::HandshakeErr { code, message } => {
                (ServerTag::HandshakeErr as u8).encode(w)?;
                code.encode(w)?;
                encode_bytes_u16(w, message)?;
            }
            Self::Subscribed { sub_id, entity_id } => {
                (ServerTag::Subscribed as u8).encode(w)?;
                sub_id.encode(w)?;
                entity_id.encode(w)?;
            }
            Self::Message(msg) => {
                (ServerTag::Message as u8).encode(w)?;
                msg.encode_body(w)?;
            }
            Self::MessageBatch(msgs) => {
                (ServerTag::MessageBatch as u8).encode(w)?;
                (msgs.len() as u16).encode(w)?;
                for msg in msgs {
                    msg.encode_body(w)?;
                }
            }
            Self::SubscriptionErr {
                sub_id,
                code,
                message,
            } => {
                (ServerTag::SubscriptionErr as u8).encode(w)?;
                sub_id.encode(w)?;
                code.encode(w)?;
                encode_bytes_u16(w, message)?;
            }
            Self::Heartbeat { server_time_ms } => {
                (ServerTag::Heartbeat as u8).encode(w)?;
                server_time_ms.encode(w)?;
            }
            Self::Close { reason } => {
                (ServerTag::Close as u8).encode(w)?;
                encode_bytes_u16(w, reason)?;
            }
            Self::PreEncoded(data) => {
                w.write_all(data)?;
            }
            Self::GroupCreated { consumer_group_id } => {
                (ServerTag::GroupCreated as u8).encode(w)?;
                consumer_group_id.encode(w)?;
            }
            Self::GroupDeleted => {
                (ServerTag::GroupDeleted as u8).encode(w)?;
            }
            Self::GroupJoined {
                generation,
                leader,
                member_id,
                protocol_name,
                is_leader,
                members,
            } => {
                (ServerTag::GroupJoined as u8).encode(w)?;
                (*generation as u32).encode(w)?;
                encode_bytes_u16(w, leader)?;
                encode_bytes_u16(w, member_id)?;
                encode_bytes_u16(w, protocol_name)?;
                is_leader.encode(w)?;
                (members.len() as u16).encode(w)?;
                for (mid, metadata) in members {
                    encode_bytes_u16(w, mid)?;
                    encode_bytes_u32(w, metadata)?;
                }
            }
            Self::GroupSynced { assignment } => {
                (ServerTag::GroupSynced as u8).encode(w)?;
                encode_bytes_u32(w, assignment)?;
            }
            Self::GroupLeft => {
                (ServerTag::GroupLeft as u8).encode(w)?;
            }
            Self::GroupHeartbeatOk => {
                (ServerTag::GroupHeartbeatOk as u8).encode(w)?;
            }
            Self::GroupOffsetCommitted => {
                (ServerTag::GroupOffsetCommitted as u8).encode(w)?;
            }
            Self::GroupOffsetsFetched { offsets } => {
                (ServerTag::GroupOffsetsFetched as u8).encode(w)?;
                (offsets.len() as u16).encode(w)?;
                for (topic_id, partition_index, offset, metadata) in offsets {
                    topic_id.encode(w)?;
                    partition_index.encode(w)?;
                    offset.encode(w)?;
                    encode_bytes_u16(w, metadata)?;
                }
            }
            Self::GroupList { groups } => {
                (ServerTag::GroupList as u8).encode(w)?;
                (groups.len() as u16).encode(w)?;
                for (group_id, name, phase, protocol_type) in groups {
                    group_id.encode(w)?;
                    encode_bytes_u16(w, name)?;
                    phase.encode(w)?;
                    encode_bytes_u16(w, protocol_type)?;
                }
            }
            Self::GroupDescription {
                consumer_group_id,
                name,
                phase,
                protocol_type,
                protocol_name,
                leader,
                generation,
                members,
            } => {
                (ServerTag::GroupDescription as u8).encode(w)?;
                consumer_group_id.encode(w)?;
                encode_bytes_u16(w, name)?;
                phase.encode(w)?;
                encode_bytes_u16(w, protocol_type)?;
                encode_bytes_u16(w, protocol_name)?;
                encode_bytes_u16(w, leader)?;
                (*generation as u32).encode(w)?;
                (members.len() as u16).encode(w)?;
                for (mid, cid, assignment) in members {
                    encode_bytes_u16(w, mid)?;
                    encode_bytes_u16(w, cid)?;
                    encode_bytes_u32(w, assignment)?;
                }
            }
            Self::GroupError { code, message } => {
                (ServerTag::GroupError as u8).encode(w)?;
                code.encode(w)?;
                encode_bytes_u16(w, message)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::PreEncoded(data) => return data.len(),
            _ => {}
        }
        1 + match self {
            Self::HandshakeOk { session_token, .. } => 8 + encoded_size_bytes_u16(session_token),
            Self::HandshakeErr { message, .. } => 2 + encoded_size_bytes_u16(message),
            Self::Subscribed { .. } => 4 + 8,
            Self::Message(msg) => msg.body_encoded_size(),
            Self::MessageBatch(msgs) => {
                2 + msgs.iter().map(|m| m.body_encoded_size()).sum::<usize>()
            }
            Self::SubscriptionErr { message, .. } => 4 + 2 + encoded_size_bytes_u16(message),
            Self::Heartbeat { .. } => 8,
            Self::Close { reason } => encoded_size_bytes_u16(reason),
            Self::PreEncoded(_) => unreachable!(),
            Self::GroupCreated { .. } => 8,
            Self::GroupDeleted => 0,
            Self::GroupJoined {
                leader,
                member_id,
                protocol_name,
                members,
                ..
            } => {
                4 + encoded_size_bytes_u16(leader)
                    + encoded_size_bytes_u16(member_id)
                    + encoded_size_bytes_u16(protocol_name)
                    + 1
                    + 2
                    + members
                        .iter()
                        .map(|(mid, meta)| {
                            encoded_size_bytes_u16(mid) + encoded_size_bytes_u32(meta)
                        })
                        .sum::<usize>()
            }
            Self::GroupSynced { assignment } => encoded_size_bytes_u32(assignment),
            Self::GroupLeft => 0,
            Self::GroupHeartbeatOk => 0,
            Self::GroupOffsetCommitted => 0,
            Self::GroupOffsetsFetched { offsets } => {
                2 + offsets
                    .iter()
                    .map(|(_, _, _, meta)| 8 + 4 + 8 + encoded_size_bytes_u16(meta))
                    .sum::<usize>()
            }
            Self::GroupList { groups } => {
                2 + groups
                    .iter()
                    .map(|(_, name, _, pt)| {
                        8 + encoded_size_bytes_u16(name) + 1 + encoded_size_bytes_u16(pt)
                    })
                    .sum::<usize>()
            }
            Self::GroupDescription {
                name,
                protocol_type,
                protocol_name,
                leader,
                members,
                ..
            } => {
                8 + encoded_size_bytes_u16(name)
                    + 1
                    + encoded_size_bytes_u16(protocol_type)
                    + encoded_size_bytes_u16(protocol_name)
                    + encoded_size_bytes_u16(leader)
                    + 4
                    + 2
                    + members
                        .iter()
                        .map(|(mid, cid, a)| {
                            encoded_size_bytes_u16(mid)
                                + encoded_size_bytes_u16(cid)
                                + encoded_size_bytes_u32(a)
                        })
                        .sum::<usize>()
            }
            Self::GroupError { message, .. } => 2 + encoded_size_bytes_u16(message),
        }
    }
}

impl Decode for ServerFrame {
    fn decode<R: Read>(r: &mut R) -> Result<Self, ProtocolError> {
        let tag = ServerTag::try_from(u8::decode(r)?)?;
        match tag {
            ServerTag::HandshakeOk => Ok(Self::HandshakeOk {
                consumer_id: u64::decode(r)?,
                session_token: Bytes::from(decode_bytes_u16(r)?),
            }),
            ServerTag::HandshakeErr => Ok(Self::HandshakeErr {
                code: u16::decode(r)?,
                message: Bytes::from(decode_bytes_u16(r)?),
            }),
            ServerTag::Subscribed => Ok(Self::Subscribed {
                sub_id: u32::decode(r)?,
                entity_id: u64::decode(r)?,
            }),
            ServerTag::Message => Ok(Self::Message(WireMessage::decode_body(r)?)),
            ServerTag::MessageBatch => {
                let count = u16::decode(r)? as usize;
                let mut msgs = Vec::with_capacity(count);
                for _ in 0..count {
                    msgs.push(WireMessage::decode_body(r)?);
                }
                Ok(Self::MessageBatch(msgs))
            }
            ServerTag::SubscriptionErr => Ok(Self::SubscriptionErr {
                sub_id: u32::decode(r)?,
                code: u16::decode(r)?,
                message: Bytes::from(decode_bytes_u16(r)?),
            }),
            ServerTag::Heartbeat => Ok(Self::Heartbeat {
                server_time_ms: u64::decode(r)?,
            }),
            ServerTag::Close => Ok(Self::Close {
                reason: Bytes::from(decode_bytes_u16(r)?),
            }),
            ServerTag::GroupCreated => Ok(Self::GroupCreated {
                consumer_group_id: u64::decode(r)?,
            }),
            ServerTag::GroupDeleted => Ok(Self::GroupDeleted),
            ServerTag::GroupJoined => {
                let generation = u32::decode(r)? as i32;
                let leader = Bytes::from(decode_bytes_u16(r)?);
                let member_id = Bytes::from(decode_bytes_u16(r)?);
                let protocol_name = Bytes::from(decode_bytes_u16(r)?);
                let is_leader = u8::decode(r)?;
                let count = u16::decode(r)? as usize;
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let mid = Bytes::from(decode_bytes_u16(r)?);
                    let metadata = Bytes::from(decode_bytes_u32(r)?);
                    members.push((mid, metadata));
                }
                Ok(Self::GroupJoined {
                    generation,
                    leader,
                    member_id,
                    protocol_name,
                    is_leader,
                    members,
                })
            }
            ServerTag::GroupSynced => Ok(Self::GroupSynced {
                assignment: Bytes::from(decode_bytes_u32(r)?),
            }),
            ServerTag::GroupLeft => Ok(Self::GroupLeft),
            ServerTag::GroupHeartbeatOk => Ok(Self::GroupHeartbeatOk),
            ServerTag::GroupOffsetCommitted => Ok(Self::GroupOffsetCommitted),
            ServerTag::GroupOffsetsFetched => {
                let count = u16::decode(r)? as usize;
                let mut offsets = Vec::with_capacity(count);
                for _ in 0..count {
                    let topic_id = u64::decode(r)?;
                    let partition_index = u32::decode(r)?;
                    let offset = u64::decode(r)?;
                    let metadata = Bytes::from(decode_bytes_u16(r)?);
                    offsets.push((topic_id, partition_index, offset, metadata));
                }
                Ok(Self::GroupOffsetsFetched { offsets })
            }
            ServerTag::GroupList => {
                let count = u16::decode(r)? as usize;
                let mut groups = Vec::with_capacity(count);
                for _ in 0..count {
                    let group_id = u64::decode(r)?;
                    let name = Bytes::from(decode_bytes_u16(r)?);
                    let phase = u8::decode(r)?;
                    let protocol_type = Bytes::from(decode_bytes_u16(r)?);
                    groups.push((group_id, name, phase, protocol_type));
                }
                Ok(Self::GroupList { groups })
            }
            ServerTag::GroupDescription => {
                let consumer_group_id = u64::decode(r)?;
                let name = Bytes::from(decode_bytes_u16(r)?);
                let phase = u8::decode(r)?;
                let protocol_type = Bytes::from(decode_bytes_u16(r)?);
                let protocol_name = Bytes::from(decode_bytes_u16(r)?);
                let leader = Bytes::from(decode_bytes_u16(r)?);
                let generation = u32::decode(r)? as i32;
                let count = u16::decode(r)? as usize;
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let mid = Bytes::from(decode_bytes_u16(r)?);
                    let cid = Bytes::from(decode_bytes_u16(r)?);
                    let assignment = Bytes::from(decode_bytes_u32(r)?);
                    members.push((mid, cid, assignment));
                }
                Ok(Self::GroupDescription {
                    consumer_group_id,
                    name,
                    phase,
                    protocol_type,
                    protocol_name,
                    leader,
                    generation,
                    members,
                })
            }
            ServerTag::GroupError => Ok(Self::GroupError {
                code: u16::decode(r)?,
                message: Bytes::from(decode_bytes_u16(r)?),
            }),
        }
    }
}

impl ServerFrame {
    /// Zero-copy decode from a `Bytes` buffer.
    /// All variable-length fields are slices into the original buffer — zero heap allocation.
    pub fn decode_from_bytes(mut buf: Bytes) -> Result<Self, ProtocolError> {
        let tag = ServerTag::try_from(read_u8_buf(&mut buf)?)?;
        match tag {
            ServerTag::HandshakeOk => Ok(Self::HandshakeOk {
                consumer_id: read_u64_le_buf(&mut buf)?,
                session_token: split_bytes_u16(&mut buf)?,
            }),
            ServerTag::HandshakeErr => Ok(Self::HandshakeErr {
                code: read_u16_le_buf(&mut buf)?,
                message: split_bytes_u16(&mut buf)?,
            }),
            ServerTag::Subscribed => Ok(Self::Subscribed {
                sub_id: read_u32_le_buf(&mut buf)?,
                entity_id: read_u64_le_buf(&mut buf)?,
            }),
            ServerTag::Message => Ok(Self::Message(WireMessage::decode_body_bytes(&mut buf)?)),
            ServerTag::MessageBatch => {
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut msgs = Vec::with_capacity(count);
                for _ in 0..count {
                    msgs.push(WireMessage::decode_body_bytes(&mut buf)?);
                }
                Ok(Self::MessageBatch(msgs))
            }
            ServerTag::SubscriptionErr => Ok(Self::SubscriptionErr {
                sub_id: read_u32_le_buf(&mut buf)?,
                code: read_u16_le_buf(&mut buf)?,
                message: split_bytes_u16(&mut buf)?,
            }),
            ServerTag::Heartbeat => Ok(Self::Heartbeat {
                server_time_ms: read_u64_le_buf(&mut buf)?,
            }),
            ServerTag::Close => Ok(Self::Close {
                reason: split_bytes_u16(&mut buf)?,
            }),
            ServerTag::GroupCreated => Ok(Self::GroupCreated {
                consumer_group_id: read_u64_le_buf(&mut buf)?,
            }),
            ServerTag::GroupDeleted => Ok(Self::GroupDeleted),
            ServerTag::GroupJoined => {
                let generation = read_u32_le_buf(&mut buf)? as i32;
                let leader = split_bytes_u16(&mut buf)?;
                let member_id = split_bytes_u16(&mut buf)?;
                let protocol_name = split_bytes_u16(&mut buf)?;
                let is_leader = read_u8_buf(&mut buf)?;
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let mid = split_bytes_u16(&mut buf)?;
                    let metadata = split_bytes_u32(&mut buf)?;
                    members.push((mid, metadata));
                }
                Ok(Self::GroupJoined {
                    generation,
                    leader,
                    member_id,
                    protocol_name,
                    is_leader,
                    members,
                })
            }
            ServerTag::GroupSynced => Ok(Self::GroupSynced {
                assignment: split_bytes_u32(&mut buf)?,
            }),
            ServerTag::GroupLeft => Ok(Self::GroupLeft),
            ServerTag::GroupHeartbeatOk => Ok(Self::GroupHeartbeatOk),
            ServerTag::GroupOffsetCommitted => Ok(Self::GroupOffsetCommitted),
            ServerTag::GroupOffsetsFetched => {
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut offsets = Vec::with_capacity(count);
                for _ in 0..count {
                    let topic_id = read_u64_le_buf(&mut buf)?;
                    let partition_index = read_u32_le_buf(&mut buf)?;
                    let offset = read_u64_le_buf(&mut buf)?;
                    let metadata = split_bytes_u16(&mut buf)?;
                    offsets.push((topic_id, partition_index, offset, metadata));
                }
                Ok(Self::GroupOffsetsFetched { offsets })
            }
            ServerTag::GroupList => {
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut groups = Vec::with_capacity(count);
                for _ in 0..count {
                    let group_id = read_u64_le_buf(&mut buf)?;
                    let name = split_bytes_u16(&mut buf)?;
                    let phase = read_u8_buf(&mut buf)?;
                    let protocol_type = split_bytes_u16(&mut buf)?;
                    groups.push((group_id, name, phase, protocol_type));
                }
                Ok(Self::GroupList { groups })
            }
            ServerTag::GroupDescription => {
                let consumer_group_id = read_u64_le_buf(&mut buf)?;
                let name = split_bytes_u16(&mut buf)?;
                let phase = read_u8_buf(&mut buf)?;
                let protocol_type = split_bytes_u16(&mut buf)?;
                let protocol_name = split_bytes_u16(&mut buf)?;
                let leader = split_bytes_u16(&mut buf)?;
                let generation = read_u32_le_buf(&mut buf)? as i32;
                let count = read_u16_le_buf(&mut buf)? as usize;
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let mid = split_bytes_u16(&mut buf)?;
                    let cid = split_bytes_u16(&mut buf)?;
                    let assignment = split_bytes_u32(&mut buf)?;
                    members.push((mid, cid, assignment));
                }
                Ok(Self::GroupDescription {
                    consumer_group_id,
                    name,
                    phase,
                    protocol_type,
                    protocol_name,
                    leader,
                    generation,
                    members,
                })
            }
            ServerTag::GroupError => Ok(Self::GroupError {
                code: read_u16_le_buf(&mut buf)?,
                message: split_bytes_u16(&mut buf)?,
            }),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: encode → decode roundtrip for ClientFrame
    fn roundtrip_client(frame: &ClientFrame) -> ClientFrame {
        let encoded = frame.encode_to_vec().unwrap();
        assert_eq!(
            encoded.len(),
            frame.encoded_size(),
            "encoded_size mismatch for {:?}",
            frame
        );
        ClientFrame::decode_from_slice(&encoded).unwrap()
    }

    // Helper: encode → decode roundtrip for ServerFrame
    fn roundtrip_server(frame: &ServerFrame) -> ServerFrame {
        let encoded = frame.encode_to_vec().unwrap();
        assert_eq!(
            encoded.len(),
            frame.encoded_size(),
            "encoded_size mismatch for {:?}",
            frame
        );
        ServerFrame::decode_from_slice(&encoded).unwrap()
    }

    // Helper: zero-copy roundtrip for ClientFrame
    fn roundtrip_client_zc(frame: &ClientFrame) -> ClientFrame {
        let encoded = frame.encode_to_vec().unwrap();
        ClientFrame::decode_from_bytes(Bytes::from(encoded)).unwrap()
    }

    // Helper: zero-copy roundtrip for ServerFrame
    fn roundtrip_server_zc(frame: &ServerFrame) -> ServerFrame {
        let encoded = frame.encode_to_vec().unwrap();
        ServerFrame::decode_from_bytes(Bytes::from(encoded)).unwrap()
    }

    // =========================================================================
    // ClientFrame roundtrips
    // =========================================================================

    #[test]
    fn test_client_handshake_roundtrip() {
        let frame = ClientFrame::Handshake {
            token: Bytes::from_static(b"my-secret-token"),
            consumer_id: Some(42),
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_handshake_no_consumer_id() {
        let frame = ClientFrame::Handshake {
            token: Bytes::new(),
            consumer_id: None,
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_subscribe_roundtrip() {
        let frame = ClientFrame::Subscribe {
            sub_id: 1,
            group_id: 7,
            entity_type: 0,
            name_hash: 0xDEADBEEF_CAFEBABE,
            start_offset: 100,
            max_in_flight: 256,
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_unsubscribe_roundtrip() {
        let frame = ClientFrame::Unsubscribe { sub_id: 99 };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_client_set_max_in_flight_roundtrip() {
        let frame = ClientFrame::SetMaxInFlight {
            sub_id: 5,
            max_in_flight: 0,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_client_ack_roundtrip() {
        let frame = ClientFrame::Ack {
            sub_id: 3,
            message_ids: vec![10, 20, 30],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_ack_empty() {
        let frame = ClientFrame::Ack {
            sub_id: 3,
            message_ids: vec![],
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_client_nack_roundtrip() {
        let frame = ClientFrame::Nack {
            sub_id: 4,
            message_ids: vec![100, 200],
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_client_commit_offset_roundtrip() {
        let frame = ClientFrame::CommitOffset {
            sub_id: 2,
            offset: 999_999,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_client_heartbeat_roundtrip() {
        assert_eq!(
            roundtrip_client(&ClientFrame::Heartbeat),
            ClientFrame::Heartbeat
        );
    }

    #[test]
    fn test_client_close_roundtrip() {
        assert_eq!(roundtrip_client(&ClientFrame::Close), ClientFrame::Close);
    }

    #[test]
    fn test_client_set_byte_budget_roundtrip() {
        let frame = ClientFrame::SetByteBudget {
            budget_bytes: 16_000_000,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    // =========================================================================
    // ServerFrame roundtrips
    // =========================================================================

    #[test]
    fn test_server_handshake_ok_roundtrip() {
        let frame = ServerFrame::HandshakeOk {
            consumer_id: 42,
            session_token: Bytes::from_static(b"session-abc"),
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_handshake_err_roundtrip() {
        let frame = ServerFrame::HandshakeErr {
            code: 401,
            message: Bytes::from_static(b"unauthorized"),
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_subscribed_roundtrip() {
        let frame = ServerFrame::Subscribed {
            sub_id: 1,
            entity_id: 77,
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    /// Helper to build a WireMessage from common test fields.
    fn make_wire_msg(
        sub_id: u32,
        message_id: u64,
        timestamp: u64,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(&[u8], &[u8])],
    ) -> WireMessage {
        use crate::flat::FlatMessageBuilder;
        let mut builder = FlatMessageBuilder::new(Bytes::from(value.to_vec())).timestamp(timestamp);
        if let Some(k) = key {
            builder = builder.key(Bytes::from(k.to_vec()));
        }
        for (name, val) in headers {
            builder = builder.header(Bytes::from(name.to_vec()), Bytes::from(val.to_vec()));
        }
        WireMessage {
            sub_id,
            message_id,
            flat: FlatMessage::new(builder.build()).unwrap(),
        }
    }

    #[test]
    fn test_server_message_roundtrip() {
        let frame = ServerFrame::Message(make_wire_msg(
            1,
            500,
            1_700_000_000_000,
            Some(b"user:123"),
            b"hello world",
            &[(b"content-type", b"text/plain"), (b"trace-id", b"abc-def")],
        ));
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_message_no_key() {
        let frame = ServerFrame::Message(make_wire_msg(2, 1, 0, None, b"data", &[]));
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_message_empty_key() {
        let frame = ServerFrame::Message(make_wire_msg(2, 1, 0, Some(b""), b"", &[]));
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_server_subscription_err_roundtrip() {
        let frame = ServerFrame::SubscriptionErr {
            sub_id: 5,
            code: 404,
            message: Bytes::from_static(b"topic not found"),
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_server_heartbeat_roundtrip() {
        let frame = ServerFrame::Heartbeat {
            server_time_ms: 1_700_000_000_000,
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_server_close_roundtrip() {
        let frame = ServerFrame::Close {
            reason: Bytes::from_static(b"connection TTL expired"),
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    // =========================================================================
    // Error path tests
    // =========================================================================

    #[test]
    fn test_unknown_client_tag() {
        let data = [0xFF];
        let err = ClientFrame::decode_from_slice(&data).unwrap_err();
        assert!(matches!(err, ProtocolError::UnknownTag(0xFF)));
    }

    #[test]
    fn test_unknown_server_tag() {
        let data = [0x00];
        let err = ServerFrame::decode_from_slice(&data).unwrap_err();
        assert!(matches!(err, ProtocolError::UnknownTag(0x00)));
    }

    #[test]
    fn test_truncated_subscribe() {
        let data = [0x02, 0x01, 0x00];
        let err = ClientFrame::decode_from_slice(&data).unwrap_err();
        assert!(matches!(err, ProtocolError::Io(_)));
    }

    #[test]
    fn test_empty_frame() {
        let data: [u8; 0] = [];
        let err = ClientFrame::decode_from_slice(&data).unwrap_err();
        assert!(matches!(err, ProtocolError::Io(_)));
    }

    #[test]
    fn test_invalid_flat_message_in_wire() {
        // Build a valid WireMessage, then corrupt the flat_bytes to be too short
        let frame = ServerFrame::Message(make_wire_msg(1, 1, 1, None, b"x", &[]));
        let mut encoded = frame.encode_to_vec().unwrap();
        // Corrupt the flat_len field to claim more bytes than available
        // flat_len is at offset: 1(tag) + 4(sub_id) + 8(msg_id) = 13
        encoded[13..17].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        let err = ServerFrame::decode_from_slice(&encoded).unwrap_err();
        assert!(matches!(
            err,
            ProtocolError::Io(_) | ProtocolError::Truncated { .. }
        ));
    }

    // =========================================================================
    // Boundary & edge-case tests
    // =========================================================================

    #[test]
    fn test_max_u64_values() {
        let frame = ClientFrame::Subscribe {
            sub_id: u32::MAX,
            group_id: u64::MAX,
            entity_type: u8::MAX,
            name_hash: u64::MAX,
            start_offset: u64::MAX,
            max_in_flight: u32::MAX,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_zero_values_subscribe() {
        let frame = ClientFrame::Subscribe {
            sub_id: 0,
            group_id: 0,
            entity_type: 0,
            name_hash: 0,
            start_offset: 0,
            max_in_flight: 0,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_large_token_handshake() {
        let frame = ClientFrame::Handshake {
            token: Bytes::from(vec![0xAB; 65535]),
            consumer_id: Some(u64::MAX),
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_large_ack_batch() {
        let ids: Vec<u64> = (0..1000).collect();
        let frame = ClientFrame::Ack {
            sub_id: 1,
            message_ids: ids,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_large_nack_batch() {
        let ids: Vec<u64> = (0..500).map(|i| i * 7).collect();
        let frame = ClientFrame::Nack {
            sub_id: 42,
            message_ids: ids,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_message_large_value() {
        let value = vec![0xCC; 1_000_000];
        let frame = ServerFrame::Message(make_wire_msg(1, 1, 1, None, &value, &[]));
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_message_large_key() {
        let key = vec![0xDD; 100_000];
        let frame = ServerFrame::Message(make_wire_msg(1, 1, 1, Some(&key), b"v", &[]));
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_message_many_headers() {
        use crate::flat::FlatMessageBuilder;
        let mut builder = FlatMessageBuilder::new(Bytes::from_static(b"v"))
            .key(Bytes::from_static(b"k"))
            .timestamp(1_700_000_000);
        for i in 0..200 {
            builder = builder.header(
                Bytes::from(format!("header-{}", i).into_bytes()),
                Bytes::from(format!("value-{}", i).into_bytes()),
            );
        }
        let frame = ServerFrame::Message(WireMessage {
            sub_id: 10,
            message_id: 999,
            flat: FlatMessage::new(builder.build()).unwrap(),
        });
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_message_empty_header_name_and_value() {
        let frame =
            ServerFrame::Message(make_wire_msg(1, 1, 0, None, b"", &[(b"", b""), (b"", b"")]));
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_handshake_err_empty_message() {
        let frame = ServerFrame::HandshakeErr {
            code: 0,
            message: Bytes::new(),
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_subscription_err_long_message() {
        let frame = ServerFrame::SubscriptionErr {
            sub_id: u32::MAX,
            code: u16::MAX,
            message: Bytes::from(vec![0xFF; 10_000]),
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_close_with_reason() {
        let frame = ServerFrame::Close {
            reason: Bytes::from_static(b"connection TTL expired, please reconnect"),
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_close_empty_reason() {
        let frame = ServerFrame::Close {
            reason: Bytes::new(),
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_heartbeat_zero_time() {
        let frame = ServerFrame::Heartbeat { server_time_ms: 0 };
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_byte_budget_zero() {
        let frame = ClientFrame::SetByteBudget { budget_bytes: 0 };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_byte_budget_max() {
        let frame = ClientFrame::SetByteBudget {
            budget_bytes: u64::MAX,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_commit_offset_zero() {
        let frame = ClientFrame::CommitOffset {
            sub_id: 0,
            offset: 0,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_set_max_in_flight_max() {
        let frame = ClientFrame::SetMaxInFlight {
            sub_id: u32::MAX,
            max_in_flight: u32::MAX,
        };
        assert_eq!(roundtrip_client(&frame), frame);
    }

    #[test]
    fn test_subscribe_all_entity_types() {
        for entity_type in 0..=3 {
            let frame = ClientFrame::Subscribe {
                sub_id: entity_type as u32,
                group_id: 1,
                entity_type,
                name_hash: 0x1234,
                start_offset: 0,
                max_in_flight: 10,
            };
            assert_eq!(roundtrip_client(&frame), frame);
        }
    }

    #[test]
    fn test_encode_into_reusable_buffer() {
        let mut buf = Vec::new();
        let frame = ClientFrame::Heartbeat;
        frame.encode_into(&mut buf).unwrap();
        assert_eq!(buf.len(), 1);

        let frame2 = ClientFrame::Ack {
            sub_id: 1,
            message_ids: vec![10, 20],
        };
        frame2.encode_into(&mut buf).unwrap();
        assert_eq!(buf.len(), frame2.encoded_size());
        let decoded = ClientFrame::decode_from_slice(&buf).unwrap();
        assert_eq!(decoded, frame2);
    }

    #[test]
    fn test_zero_copy_server_message() {
        let frame = ServerFrame::Message(make_wire_msg(
            1,
            42,
            1000,
            Some(b"key"),
            b"value",
            &[(b"h1", b"v1")],
        ));
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_zero_copy_truncation_error() {
        let buf = Bytes::from_static(&[ServerTag::Message as u8, 0x01]);
        let err = ServerFrame::decode_from_bytes(buf).unwrap_err();
        assert!(matches!(err, ProtocolError::Truncated { .. }));
    }

    #[test]
    fn test_wire_message_payload_bytes() {
        let msg = make_wire_msg(1, 1, 0, Some(b"key"), b"value", &[(b"h", b"v")]);
        // payload_bytes returns the entire flat buffer size
        assert!(msg.payload_bytes() > 0);
    }

    // =========================================================================
    // MessageBatch roundtrips
    // =========================================================================

    #[test]
    fn test_server_message_batch_roundtrip() {
        let frame = ServerFrame::MessageBatch(vec![
            make_wire_msg(1, 100, 1000, Some(b"k1"), b"v1", &[]),
            make_wire_msg(1, 101, 1001, None, b"v2", &[(b"h", b"v")]),
            make_wire_msg(2, 200, 2000, Some(b"k3"), b"v3", &[]),
        ]);
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_message_batch_empty() {
        let frame = ServerFrame::MessageBatch(vec![]);
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_message_batch_single() {
        let frame = ServerFrame::MessageBatch(vec![make_wire_msg(5, 42, 999, None, b"hello", &[])]);
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    // =========================================================================
    // ClientFrame::Publish roundtrips
    // =========================================================================

    #[test]
    fn test_client_publish_roundtrip() {
        let frame = ClientFrame::Publish {
            group_id: 7,
            topic_name_hash: 0xDEADBEEF,
            messages: vec![
                Bytes::from_static(b"msg1"),
                Bytes::from_static(b"msg2"),
                Bytes::from_static(b"msg3"),
            ],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_publish_empty_messages() {
        let frame = ClientFrame::Publish {
            group_id: 1,
            topic_name_hash: 42,
            messages: vec![],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_publish_single_large_message() {
        let frame = ClientFrame::Publish {
            group_id: 1,
            topic_name_hash: 42,
            messages: vec![Bytes::from(vec![0xAB; 100_000])],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    // =========================================================================
    // Consumer group ClientFrame roundtrips
    // =========================================================================

    #[test]
    fn test_client_create_group_roundtrip() {
        let frame = ClientFrame::CreateGroup {
            group_id: 1,
            name: Bytes::from_static(b"my-group"),
            auto_offset_reset: 1,
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_delete_group_roundtrip() {
        let frame = ClientFrame::DeleteGroup {
            group_id: 1,
            name_hash: 0xCAFEBABE,
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_join_group_roundtrip() {
        let frame = ClientFrame::JoinGroup {
            group_id: 1,
            name_hash: 0xDEAD,
            member_id: Bytes::from_static(b""),
            client_id: Bytes::from_static(b"client-1"),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            protocol_type: Bytes::from_static(b"consumer"),
            protocols: vec![
                (
                    Bytes::from_static(b"range"),
                    Bytes::from_static(b"\x00\x01\x02"),
                ),
                (
                    Bytes::from_static(b"roundrobin"),
                    Bytes::from_static(b"\x03\x04"),
                ),
            ],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_join_group_empty_protocols() {
        let frame = ClientFrame::JoinGroup {
            group_id: 1,
            name_hash: 0,
            member_id: Bytes::from_static(b"member-1"),
            client_id: Bytes::from_static(b"client-1"),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 30000,
            protocol_type: Bytes::from_static(b"consumer"),
            protocols: vec![],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_sync_group_roundtrip() {
        let frame = ClientFrame::SyncGroup {
            group_id: 1,
            name_hash: 0xBEEF,
            generation: 5,
            member_id: Bytes::from_static(b"member-1"),
            assignments: vec![
                (
                    Bytes::from_static(b"member-1"),
                    Bytes::from_static(b"\x00\x01"),
                ),
                (
                    Bytes::from_static(b"member-2"),
                    Bytes::from_static(b"\x02\x03"),
                ),
            ],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_leave_group_roundtrip() {
        let frame = ClientFrame::LeaveGroup {
            group_id: 1,
            name_hash: 0x1234,
            member_id: Bytes::from_static(b"member-1"),
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_group_heartbeat_roundtrip() {
        let frame = ClientFrame::GroupHeartbeat {
            group_id: 1,
            name_hash: 0x5678,
            member_id: Bytes::from_static(b"member-1"),
            generation: 3,
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_commit_group_offset_roundtrip() {
        let frame = ClientFrame::CommitGroupOffset {
            group_id: 1,
            name_hash: 0xABCD,
            generation: 2,
            offsets: vec![
                (100, 0, 500, Bytes::from_static(b"")),
                (100, 1, 300, Bytes::from_static(b"meta")),
                (200, 0, 1000, Bytes::from_static(b"")),
            ],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_fetch_group_offsets_roundtrip() {
        let frame = ClientFrame::FetchGroupOffsets {
            group_id: 1,
            name_hash: 0x9999,
            partitions: vec![(100, 0), (100, 1), (200, 0)],
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_list_groups_roundtrip() {
        let frame = ClientFrame::ListGroups { group_id: 7 };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    #[test]
    fn test_client_describe_group_roundtrip() {
        let frame = ClientFrame::DescribeGroup {
            group_id: 1,
            name_hash: 0xAAAA,
        };
        assert_eq!(roundtrip_client(&frame), frame);
        assert_eq!(roundtrip_client_zc(&frame), frame);
    }

    // =========================================================================
    // Consumer group ServerFrame roundtrips
    // =========================================================================

    #[test]
    fn test_server_group_created_roundtrip() {
        let frame = ServerFrame::GroupCreated {
            consumer_group_id: 42,
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_deleted_roundtrip() {
        assert_eq!(
            roundtrip_server(&ServerFrame::GroupDeleted),
            ServerFrame::GroupDeleted
        );
        assert_eq!(
            roundtrip_server_zc(&ServerFrame::GroupDeleted),
            ServerFrame::GroupDeleted
        );
    }

    #[test]
    fn test_server_group_joined_roundtrip() {
        let frame = ServerFrame::GroupJoined {
            generation: 3,
            leader: Bytes::from_static(b"member-1"),
            member_id: Bytes::from_static(b"member-2"),
            protocol_name: Bytes::from_static(b"range"),
            is_leader: 0,
            members: vec![
                (
                    Bytes::from_static(b"member-1"),
                    Bytes::from_static(b"\x00\x01"),
                ),
                (
                    Bytes::from_static(b"member-2"),
                    Bytes::from_static(b"\x02\x03"),
                ),
            ],
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_joined_leader() {
        let frame = ServerFrame::GroupJoined {
            generation: 1,
            leader: Bytes::from_static(b"member-1"),
            member_id: Bytes::from_static(b"member-1"),
            protocol_name: Bytes::from_static(b"roundrobin"),
            is_leader: 1,
            members: vec![(Bytes::from_static(b"member-1"), Bytes::from_static(b"\x00"))],
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_synced_roundtrip() {
        let frame = ServerFrame::GroupSynced {
            assignment: Bytes::from_static(b"\x00\x01\x02\x03"),
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_left_roundtrip() {
        assert_eq!(
            roundtrip_server(&ServerFrame::GroupLeft),
            ServerFrame::GroupLeft
        );
    }

    #[test]
    fn test_server_group_heartbeat_ok_roundtrip() {
        assert_eq!(
            roundtrip_server(&ServerFrame::GroupHeartbeatOk),
            ServerFrame::GroupHeartbeatOk
        );
    }

    #[test]
    fn test_server_group_offset_committed_roundtrip() {
        assert_eq!(
            roundtrip_server(&ServerFrame::GroupOffsetCommitted),
            ServerFrame::GroupOffsetCommitted
        );
    }

    #[test]
    fn test_server_group_offsets_fetched_roundtrip() {
        let frame = ServerFrame::GroupOffsetsFetched {
            offsets: vec![
                (100, 0, 500, Bytes::from_static(b"")),
                (100, 1, 300, Bytes::from_static(b"meta")),
                (200, 0, 1000, Bytes::from_static(b"")),
            ],
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_list_roundtrip() {
        let frame = ServerFrame::GroupList {
            groups: vec![
                (
                    1,
                    Bytes::from_static(b"group-a"),
                    3,
                    Bytes::from_static(b"consumer"),
                ),
                (
                    2,
                    Bytes::from_static(b"group-b"),
                    0,
                    Bytes::from_static(b""),
                ),
            ],
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_description_roundtrip() {
        let frame = ServerFrame::GroupDescription {
            consumer_group_id: 42,
            name: Bytes::from_static(b"my-group"),
            phase: 3,
            protocol_type: Bytes::from_static(b"consumer"),
            protocol_name: Bytes::from_static(b"range"),
            leader: Bytes::from_static(b"member-1"),
            generation: 5,
            members: vec![
                (
                    Bytes::from_static(b"member-1"),
                    Bytes::from_static(b"client-1"),
                    Bytes::from_static(b"\x00\x01"),
                ),
                (
                    Bytes::from_static(b"member-2"),
                    Bytes::from_static(b"client-2"),
                    Bytes::from_static(b"\x02\x03"),
                ),
            ],
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_error_roundtrip() {
        let frame = ServerFrame::GroupError {
            code: 404,
            message: Bytes::from_static(b"consumer group not found"),
        };
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_group_error_empty_message() {
        let frame = ServerFrame::GroupError {
            code: 0,
            message: Bytes::new(),
        };
        assert_eq!(roundtrip_server(&frame), frame);
    }
}
