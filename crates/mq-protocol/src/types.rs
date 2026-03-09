use std::io::{Cursor, Read, Write};

use bytes::{Buf, Bytes};

use crate::error::ProtocolError;
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
fn encode_u64_vec<W: Write>(w: &mut W, ids: &[u64]) -> Result<(), ProtocolError> {
    (ids.len() as u16).encode(w)?;
    for id in ids {
        id.encode(w)?;
    }
    Ok(())
}

fn decode_u64_vec<R: Read>(r: &mut R) -> Result<Vec<u64>, ProtocolError> {
    let count = u16::decode(r)? as usize;
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(u64::decode(r)?);
    }
    Ok(ids)
}

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
        }
    }
}

// =============================================================================
// ServerFrame
// =============================================================================

/// Message payload extracted from ServerFrame::Message.
/// Boxed inside the enum to keep the enum small (~8 bytes for Message variant)
/// — better for channel buffers where non-Message variants are common.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerMessage {
    pub sub_id: u32,
    pub message_id: u64,
    pub timestamp: u64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(Bytes, Bytes)>,
}

impl ServerMessage {
    /// Total byte size of message payload (value + key + headers).
    pub fn payload_bytes(&self) -> u64 {
        let mut total = self.value.len() as u64;
        if let Some(ref k) = self.key {
            total += k.len() as u64;
        }
        for (name, val) in &self.headers {
            total += name.len() as u64 + val.len() as u64;
        }
        total
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
    Message(Box<ServerMessage>),
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
                msg.sub_id.encode(w)?;
                msg.message_id.encode(w)?;
                msg.timestamp.encode(w)?;
                match &msg.key {
                    None => 0u8.encode(w)?,
                    Some(k) => {
                        1u8.encode(w)?;
                        encode_bytes_u32(w, k)?;
                    }
                }
                encode_bytes_u32(w, &msg.value)?;
                (msg.headers.len() as u16).encode(w)?;
                for (name, val) in &msg.headers {
                    encode_bytes_u16(w, name)?;
                    encode_bytes_u32(w, val)?;
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
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            Self::HandshakeOk { session_token, .. } => 8 + encoded_size_bytes_u16(session_token),
            Self::HandshakeErr { message, .. } => 2 + encoded_size_bytes_u16(message),
            Self::Subscribed { .. } => 4 + 8,
            Self::Message(msg) => {
                let key_size = match &msg.key {
                    None => 1,
                    Some(k) => 1 + encoded_size_bytes_u32(k),
                };
                let headers_size: usize = msg
                    .headers
                    .iter()
                    .map(|(name, val)| encoded_size_bytes_u16(name) + encoded_size_bytes_u32(val))
                    .sum();
                4 + 8 + 8 + key_size + encoded_size_bytes_u32(&msg.value) + 2 + headers_size
            }
            Self::SubscriptionErr { message, .. } => 4 + 2 + encoded_size_bytes_u16(message),
            Self::Heartbeat { .. } => 8,
            Self::Close { reason } => encoded_size_bytes_u16(reason),
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
            ServerTag::Message => {
                let sub_id = u32::decode(r)?;
                let message_id = u64::decode(r)?;
                let timestamp = u64::decode(r)?;
                let key_tag = u8::decode(r)?;
                let key = match key_tag {
                    0 => None,
                    1 => Some(Bytes::from(decode_bytes_u32(r)?)),
                    _ => return Err(ProtocolError::UnknownTag(key_tag)),
                };
                let value = Bytes::from(decode_bytes_u32(r)?);
                let header_count = u16::decode(r)? as usize;
                let mut headers = Vec::with_capacity(header_count);
                for _ in 0..header_count {
                    let name = Bytes::from(decode_bytes_u16(r)?);
                    let val = Bytes::from(decode_bytes_u32(r)?);
                    headers.push((name, val));
                }
                Ok(Self::Message(Box::new(ServerMessage {
                    sub_id,
                    message_id,
                    timestamp,
                    key,
                    value,
                    headers,
                })))
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
            ServerTag::Message => {
                let sub_id = read_u32_le_buf(&mut buf)?;
                let message_id = read_u64_le_buf(&mut buf)?;
                let timestamp = read_u64_le_buf(&mut buf)?;
                let key_tag = read_u8_buf(&mut buf)?;
                let key = match key_tag {
                    0 => None,
                    1 => Some(split_bytes_u32(&mut buf)?),
                    _ => return Err(ProtocolError::UnknownTag(key_tag)),
                };
                let value = split_bytes_u32(&mut buf)?;
                let header_count = read_u16_le_buf(&mut buf)? as usize;
                let mut headers = Vec::with_capacity(header_count);
                for _ in 0..header_count {
                    let name = split_bytes_u16(&mut buf)?;
                    let val = split_bytes_u32(&mut buf)?;
                    headers.push((name, val));
                }
                Ok(Self::Message(Box::new(ServerMessage {
                    sub_id,
                    message_id,
                    timestamp,
                    key,
                    value,
                    headers,
                })))
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

    #[test]
    fn test_server_message_roundtrip() {
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 1,
            message_id: 500,
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"user:123")),
            value: Bytes::from_static(b"hello world"),
            headers: vec![
                (
                    Bytes::from_static(b"content-type"),
                    Bytes::from_static(b"text/plain"),
                ),
                (
                    Bytes::from_static(b"trace-id"),
                    Bytes::from_static(b"abc-def"),
                ),
            ],
        }));
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_message_no_key() {
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 2,
            message_id: 1,
            timestamp: 0,
            key: None,
            value: Bytes::from_static(b"data"),
            headers: vec![],
        }));
        assert_eq!(roundtrip_server(&frame), frame);
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_server_message_empty_key() {
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 2,
            message_id: 1,
            timestamp: 0,
            key: Some(Bytes::new()),
            value: Bytes::new(),
            headers: vec![],
        }));
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
    fn test_invalid_key_tag_in_message() {
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 1,
            message_id: 1,
            timestamp: 1,
            key: None,
            value: Bytes::from_static(b"x"),
            headers: vec![],
        }));
        let mut encoded = frame.encode_to_vec().unwrap();
        // key tag is at offset: 1(tag) + 4(sub_id) + 8(msg_id) + 8(timestamp) = 21
        encoded[21] = 0x99;
        let err = ServerFrame::decode_from_slice(&encoded).unwrap_err();
        assert!(matches!(err, ProtocolError::UnknownTag(0x99)));
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
        let value = Bytes::from(vec![0xCC; 1_000_000]);
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 1,
            message_id: 1,
            timestamp: 1,
            key: None,
            value,
            headers: vec![],
        }));
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_message_large_key() {
        let key = Bytes::from(vec![0xDD; 100_000]);
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 1,
            message_id: 1,
            timestamp: 1,
            key: Some(key),
            value: Bytes::from_static(b"v"),
            headers: vec![],
        }));
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_message_many_headers() {
        let headers: Vec<(Bytes, Bytes)> = (0..200)
            .map(|i| {
                (
                    Bytes::from(format!("header-{}", i).into_bytes()),
                    Bytes::from(format!("value-{}", i).into_bytes()),
                )
            })
            .collect();
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 10,
            message_id: 999,
            timestamp: 1_700_000_000,
            key: Some(Bytes::from_static(b"k")),
            value: Bytes::from_static(b"v"),
            headers,
        }));
        assert_eq!(roundtrip_server(&frame), frame);
    }

    #[test]
    fn test_message_empty_header_name_and_value() {
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 1,
            message_id: 1,
            timestamp: 0,
            key: None,
            value: Bytes::new(),
            headers: vec![(Bytes::new(), Bytes::new()), (Bytes::new(), Bytes::new())],
        }));
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
        let frame = ServerFrame::Message(Box::new(ServerMessage {
            sub_id: 1,
            message_id: 42,
            timestamp: 1000,
            key: Some(Bytes::from_static(b"key")),
            value: Bytes::from_static(b"value"),
            headers: vec![(Bytes::from_static(b"h1"), Bytes::from_static(b"v1"))],
        }));
        assert_eq!(roundtrip_server_zc(&frame), frame);
    }

    #[test]
    fn test_zero_copy_truncation_error() {
        let buf = Bytes::from_static(&[ServerTag::Message as u8, 0x01]);
        let err = ServerFrame::decode_from_bytes(buf).unwrap_err();
        assert!(matches!(err, ProtocolError::Truncated { .. }));
    }

    #[test]
    fn test_server_message_payload_bytes() {
        let msg = ServerMessage {
            sub_id: 1,
            message_id: 1,
            timestamp: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Bytes::from_static(b"value"),
            headers: vec![(Bytes::from_static(b"h"), Bytes::from_static(b"v"))],
        };
        // key(3) + value(5) + header_name(1) + header_val(1) = 10
        assert_eq!(msg.payload_bytes(), 10);
    }
}
