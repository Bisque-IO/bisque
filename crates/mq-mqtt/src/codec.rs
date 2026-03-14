//! MQTT binary codec — encode and decode MQTT packets from bytes.
//!
//! Implements the MQTT 3.1.1 / 5.0 binary wire protocol including:
//! - Fixed header parsing with packet type and flags
//! - Variable-length remaining length encoding/decoding (up to 4 bytes)
//! - Per-packet-type serialization and deserialization
//! - MQTT 5.0 property parsing

use bisque_mq::flat::{self, FlatMessage, MqttEnvelope};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use smallvec::SmallVec;

use crate::types::{
    Auth, ConnAck, Connect, ConnectFlags, Disconnect, PROP_SIZE_U32, Properties, ProtocolVersion,
    PubAck, PubComp, PubRec, PubRel, Publish, QoS, SubAck, Subscribe, TopicFilter, UnsubAck,
    Unsubscribe, WillMessage, prop_size_str, prop_size_varint,
};

// =============================================================================
// Errors
// =============================================================================

/// Codec error type.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("incomplete packet: need more data")]
    Incomplete,
    #[error("malformed remaining length encoding")]
    MalformedRemainingLength,
    #[error("unknown packet type: {0}")]
    UnknownPacketType(u8),
    #[error("invalid protocol name")]
    InvalidProtocolName(Bytes),
    #[error("unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(u8),
    #[error("invalid connect flags")]
    InvalidConnectFlags,
    #[error("invalid QoS value: {0}")]
    InvalidQoS(u8),
    #[error("packet too large: {0} bytes")]
    PacketTooLarge(usize),
    #[error("invalid UTF-8 string in packet")]
    InvalidUtf8,
    #[error("unexpected end of data")]
    UnexpectedEof,
    #[error("invalid property id: {0}")]
    InvalidPropertyId(u8),
    #[error("invalid fixed header flags for packet type {0}")]
    InvalidFixedHeaderFlags(u8),
    #[error("invalid topic name")]
    InvalidTopicName,
    #[error("invalid topic filter")]
    InvalidTopicFilter,
    #[error("duplicate property id: {0}")]
    DuplicateProperty(u8),
    #[error("invalid property value for property id: {0}")]
    InvalidPropertyValue(u8),
    #[error("malformed properties")]
    MalformedProperties,
    #[error("protocol error")]
    ProtocolError,
}

// =============================================================================
// Constants
// =============================================================================

/// Maximum allowed packet size (256 MB per MQTT spec).
pub const MAX_PACKET_SIZE: usize = 256 * 1024 * 1024;

/// Default maximum packet size for this server (1 MB).
pub const DEFAULT_MAX_PACKET_SIZE: usize = 1024 * 1024;

// =============================================================================
// Fixed Header Flag Validation (M2)
// =============================================================================

/// Validate that the fixed header flags are correct for the given packet type.
///
/// Per MQTT spec, most packet types require specific flag values:
/// - PUBLISH (3): flags are variable (DUP/QoS/Retain), no validation needed
/// - PUBREL (6), SUBSCRIBE (8), UNSUBSCRIBE (10): flags MUST be 0x02
/// - All others: flags MUST be 0x00
#[inline]
pub fn validate_fixed_header_flags(packet_type: u8, flags: u8) -> Result<(), CodecError> {
    match packet_type {
        3 => Ok(()), // PUBLISH: variable flags
        6 | 8 | 10 => {
            // PUBREL, SUBSCRIBE, UNSUBSCRIBE: must be 0x02
            if flags != 0x02 {
                Err(CodecError::InvalidFixedHeaderFlags(packet_type))
            } else {
                Ok(())
            }
        }
        1 | 2 | 4 | 5 | 7 | 9 | 11 | 12 | 13 | 14 | 15 => {
            // CONNECT, CONNACK, PUBACK, PUBREC, PUBCOMP, SUBACK, UNSUBACK,
            // PINGREQ, PINGRESP, DISCONNECT, AUTH: must be 0x00
            if flags != 0x00 {
                Err(CodecError::InvalidFixedHeaderFlags(packet_type))
            } else {
                Ok(())
            }
        }
        _ => Err(CodecError::UnknownPacketType(packet_type)),
    }
}

// =============================================================================
// Topic Name Validation (M3)
// =============================================================================

/// Validate an MQTT topic name (used in PUBLISH).
///
/// - Must not be empty
/// - Must not contain wildcard characters `+` or `#`
/// - Must not contain null byte 0x00
#[inline]
pub fn validate_topic_name(topic: &[u8]) -> Result<(), CodecError> {
    if topic.is_empty() {
        return Err(CodecError::InvalidTopicName);
    }
    // Use memchr3 for SIMD-accelerated scan of forbidden bytes.
    if memchr::memchr3(b'+', b'#', 0x00, topic).is_some() {
        return Err(CodecError::InvalidTopicName);
    }
    Ok(())
}

// =============================================================================
// Topic Filter Validation (M4)
// =============================================================================

/// Validate an MQTT topic filter (used in SUBSCRIBE).
///
/// - Must not be empty
/// - `+` must occupy an entire level (surrounded by `/` or at start/end)
/// - `#` must be last character and preceded by `/` (or be the only character)
/// - Must not contain null byte 0x00
pub fn validate_topic_filter(filter: &str) -> Result<(), CodecError> {
    if filter.is_empty() {
        return Err(CodecError::InvalidTopicFilter);
    }
    // Zero-allocation: iterate levels without collecting into Vec.
    // Use memchr-style byte scan instead of char iteration for null check.
    let bytes = filter.as_bytes();
    if memchr::memchr(0, bytes).is_some() {
        return Err(CodecError::InvalidTopicFilter);
    }
    let mut found_multi_level = false;
    for level in filter.split('/') {
        if found_multi_level {
            // '#' was not the last level — invalid.
            return Err(CodecError::InvalidTopicFilter);
        }
        if level.contains('#') {
            if level != "#" {
                // '#' mixed with other characters in the same level.
                return Err(CodecError::InvalidTopicFilter);
            }
            found_multi_level = true;
        }
        if level.contains('+') && level != "+" {
            // '+' must occupy an entire level
            return Err(CodecError::InvalidTopicFilter);
        }
    }
    Ok(())
}

// =============================================================================
// Remaining Length Encoding/Decoding
// =============================================================================

/// Decode the MQTT variable-length remaining length from a byte buffer.
///
/// Returns `(remaining_length, bytes_consumed)` on success.
/// Returns `Err(Incomplete)` if more bytes are needed.
/// Returns `Err(MalformedRemainingLength)` if encoding is invalid.
#[inline]
pub fn decode_remaining_length(buf: &[u8]) -> Result<(usize, usize), CodecError> {
    let mut multiplier: usize = 1;
    let mut value: usize = 0;
    let mut index = 0;

    loop {
        if index >= buf.len() {
            return Err(CodecError::Incomplete);
        }
        if index >= 4 {
            return Err(CodecError::MalformedRemainingLength);
        }

        let encoded_byte = buf[index];
        value += (encoded_byte as usize & 0x7F) * multiplier;
        multiplier *= 128;
        index += 1;

        if encoded_byte & 0x80 == 0 {
            break;
        }
    }

    if value > MAX_PACKET_SIZE {
        return Err(CodecError::PacketTooLarge(value));
    }

    Ok((value, index))
}

/// Encode a remaining length value into the MQTT variable-length format.
#[inline]
pub fn encode_remaining_length(mut value: usize, buf: &mut BytesMut) {
    loop {
        let mut encoded_byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded_byte |= 0x80;
        }
        buf.put_u8(encoded_byte);
        if value == 0 {
            break;
        }
    }
}

// =============================================================================
// UTF-8 String Helpers
// =============================================================================

/// Read an MQTT UTF-8 encoded string as zero-copy Bytes slice from the input buffer.
fn read_mqtt_string_zc(buf: &[u8], pos: &mut usize) -> Result<Bytes, CodecError> {
    if buf.len() - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]) as usize;
    *pos += 2;
    if buf.len() - *pos < len {
        return Err(CodecError::UnexpectedEof);
    }
    let data = &buf[*pos..*pos + len];
    let s = std::str::from_utf8(data).map_err(|_| CodecError::InvalidUtf8)?;
    validate_mqtt_utf8(s)?;
    let result = Bytes::copy_from_slice(data);
    *pos += len;
    Ok(result)
}

/// Read MQTT binary data from the input buffer.
fn read_mqtt_bytes_zc(buf: &[u8], pos: &mut usize) -> Result<Bytes, CodecError> {
    if buf.len() - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]) as usize;
    *pos += 2;
    if buf.len() - *pos < len {
        return Err(CodecError::UnexpectedEof);
    }
    let result = Bytes::copy_from_slice(&buf[*pos..*pos + len]);
    *pos += len;
    Ok(result)
}

/// Read a u8 from a byte buffer.
#[inline]
fn read_u8_zc(buf: &[u8], pos: &mut usize) -> Result<u8, CodecError> {
    if *pos >= buf.len() {
        return Err(CodecError::UnexpectedEof);
    }
    let v = buf[*pos];
    *pos += 1;
    Ok(v)
}

/// Read a u16 from a byte buffer.
#[inline]
fn read_u16_zc(buf: &[u8], pos: &mut usize) -> Result<u16, CodecError> {
    if buf.len() - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let v = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

/// Validate MQTT UTF-8 string constraints.
///
/// MQTT 3.1.1 SS 1.5.3, MQTT 5.0 SS 1.5.4: UTF-8 strings MUST NOT contain:
/// - U+0000 (null character)
/// - U+0001 to U+001F (C0 control characters, except allowed in payloads)
/// - U+007F (DEL)
/// - U+0080 to U+009F (C1 control characters)
/// - U+D800 to U+DFFF (surrogates — Rust's str guarantees no surrogates, but check defensively)
/// - U+FFFE and U+FFFF (non-characters)
fn validate_mqtt_utf8(s: &str) -> Result<(), CodecError> {
    for c in s.chars() {
        match c {
            '\0' => return Err(CodecError::InvalidUtf8),
            '\u{0001}'..='\u{001F}' | '\u{007F}'..='\u{009F}' => {
                return Err(CodecError::InvalidUtf8);
            }
            '\u{FFFE}' | '\u{FFFF}' => return Err(CodecError::InvalidUtf8),
            // Surrogates U+D800-U+DFFF cannot appear in valid Rust str,
            // so no explicit check needed.
            _ => {}
        }
    }
    Ok(())
}

/// Write an MQTT UTF-8 encoded string.
fn write_mqtt_string(s: &str, buf: &mut BytesMut) {
    buf.put_u16(s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
}

/// Write MQTT binary data with 2-byte length prefix.
fn write_mqtt_bytes(data: &[u8], buf: &mut BytesMut) {
    buf.put_u16(data.len() as u16);
    buf.extend_from_slice(data);
}

// =============================================================================
// Variable Byte Integer (MQTT 5.0)
// =============================================================================

/// Read a variable byte integer (used for property lengths in MQTT 5.0).
///
/// MQTT 5.0 SS 1.5.5-1: The encoded value MUST use the minimum number of bytes
/// necessary. Non-minimal encodings are treated as malformed.
fn read_variable_int(buf: &mut &[u8]) -> Result<u32, CodecError> {
    let mut multiplier: u32 = 1;
    let mut value: u32 = 0;
    let mut bytes_used: u32 = 0;

    for _ in 0..4 {
        if !buf.has_remaining() {
            return Err(CodecError::UnexpectedEof);
        }
        let byte = buf.get_u8();
        value += (byte as u32 & 0x7F) * multiplier;
        bytes_used += 1;
        if byte & 0x80 == 0 {
            // Validate minimum encoding: check that the value could not fit in fewer bytes.
            let min_bytes = if value < 128 {
                1
            } else if value < 16_384 {
                2
            } else if value < 2_097_152 {
                3
            } else {
                4
            };
            if bytes_used > min_bytes {
                return Err(CodecError::MalformedRemainingLength);
            }
            return Ok(value);
        }
        multiplier *= 128;
    }
    Err(CodecError::MalformedRemainingLength)
}

/// Write a variable byte integer.
fn write_variable_int(mut value: u32, buf: &mut BytesMut) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Compute the encoded size of a variable-length integer.
#[inline]
fn variable_int_size(value: u32) -> usize {
    if value < 128 {
        1
    } else if value < 16384 {
        2
    } else if value < 2_097_152 {
        3
    } else {
        4
    }
}

// =============================================================================
// Properties (MQTT 5.0)
// =============================================================================

/// Validate MQTT 5.0 property bytes without allocating.
///
/// Walks the raw property bytes checking: valid property IDs, no duplicates
/// (except User Property 0x26), value constraints (non-zero where required),
/// and UTF-8 validity for string properties.
fn validate_property_bytes(data: &[u8]) -> Result<(), CodecError> {
    let mut pos = 0;
    let mut seen: u64 = 0;

    while pos < data.len() {
        let id = data[pos];
        pos += 1;

        // Duplicate check (User Property 0x26 exempt).
        if id != 0x26 && id < 64 {
            let bit = 1u64 << id;
            if seen & bit != 0 {
                return Err(CodecError::DuplicateProperty(id));
            }
            seen |= bit;
        }

        match id {
            // u8 properties
            0x01 | 0x17 | 0x19 | 0x24 => {
                if pos >= data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                pos += 1;
            }
            // bool properties (u8 wire encoding)
            0x25 | 0x28 | 0x29 | 0x2A => {
                if pos >= data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                pos += 1;
            }
            // u16 properties
            0x13 | 0x22 => {
                if pos + 2 > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                pos += 2;
            }
            // u16 properties with non-zero constraint
            0x21 | 0x23 => {
                if pos + 2 > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let val = u16::from_be_bytes([data[pos], data[pos + 1]]);
                if val == 0 {
                    return Err(CodecError::InvalidPropertyValue(id));
                }
                pos += 2;
            }
            // u32 properties
            0x02 | 0x11 | 0x18 | 0x27 => {
                if pos + 4 > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                pos += 4;
            }
            // UTF-8 string properties
            0x03 | 0x08 | 0x12 | 0x15 | 0x1A | 0x1C | 0x1F => {
                if pos + 2 > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                if pos + 2 + len > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let s = std::str::from_utf8(&data[pos + 2..pos + 2 + len])
                    .map_err(|_| CodecError::InvalidUtf8)?;
                validate_mqtt_utf8(s)?;
                pos += 2 + len;
            }
            // Binary data properties
            0x09 | 0x16 => {
                if pos + 2 > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                if pos + 2 + len > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                pos += 2 + len;
            }
            // Subscription Identifier (Variable Byte Integer, must be non-zero)
            0x0B => {
                let mut val: u32 = 0;
                let mut mult: u32 = 1;
                let mut consumed = 0;
                loop {
                    if pos + consumed >= data.len() || consumed >= 4 {
                        return Err(CodecError::UnexpectedEof);
                    }
                    let byte = data[pos + consumed];
                    val += (byte as u32 & 0x7F) * mult;
                    consumed += 1;
                    if byte & 0x80 == 0 {
                        break;
                    }
                    mult *= 128;
                }
                if val == 0 {
                    return Err(CodecError::InvalidPropertyValue(0x0B));
                }
                pos += consumed;
            }
            // User Property (two UTF-8 strings)
            0x26 => {
                if pos + 2 > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let key_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                if pos + 2 + key_len > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let key_s = std::str::from_utf8(&data[pos + 2..pos + 2 + key_len])
                    .map_err(|_| CodecError::InvalidUtf8)?;
                validate_mqtt_utf8(key_s)?;
                let off = pos + 2 + key_len;
                if off + 2 > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let val_len = u16::from_be_bytes([data[off], data[off + 1]]) as usize;
                if off + 2 + val_len > data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                let val_s = std::str::from_utf8(&data[off + 2..off + 2 + val_len])
                    .map_err(|_| CodecError::InvalidUtf8)?;
                validate_mqtt_utf8(val_s)?;
                pos = off + 2 + val_len;
            }
            _ => {
                return Err(CodecError::InvalidPropertyId(id));
            }
        }
    }

    Ok(())
}

/// Read MQTT 5.0 properties from a byte buffer using zero-copy borrowed slices.
fn read_properties_zc<'a>(buf: &'a [u8], pos: &mut usize) -> Result<Properties<'a>, CodecError> {
    let remaining = &buf[*pos..];
    let mut cursor: &[u8] = remaining;
    let prop_len = read_variable_int(&mut cursor)? as usize;
    let varint_consumed = remaining.len() - cursor.len();
    if cursor.len() < prop_len {
        return Err(CodecError::UnexpectedEof);
    }
    if prop_len > 0 {
        validate_property_bytes(&cursor[..prop_len])?;
    }
    let raw_start = *pos + varint_consumed;
    let props = Properties::from_raw(&buf[raw_start..raw_start + prop_len]);
    *pos += varint_consumed + prop_len;
    Ok(props)
}

/// Content size of properties (just the raw bytes length).
#[inline]
fn compute_properties_size(props: &Properties<'_>) -> usize {
    props.raw().len()
}

/// Total encoded size of properties including the variable-length prefix.
#[inline]
fn properties_wire_size(props: &Properties<'_>) -> usize {
    let content_size = props.raw().len();
    variable_int_size(content_size as u32) + content_size
}

/// Write MQTT 5.0 properties to the output buffer.
/// Since Properties stores raw wire-format bytes, this is a direct copy.
#[inline]
fn write_properties(props: &Properties<'_>, buf: &mut BytesMut) {
    let raw = props.raw();
    write_variable_int(raw.len() as u32, buf);
    buf.extend_from_slice(raw);
}

/// Total encoded size of a properties section (varint prefix + content bytes).
#[inline]
fn props_wire_size(content_size: usize) -> usize {
    variable_int_size(content_size as u32) + content_size
}

// --- Property write helpers: write a single property directly into buf ---

#[inline]
fn write_prop_u8(buf: &mut BytesMut, id: u8, val: u8) {
    buf.put_u8(id);
    buf.put_u8(val);
}

#[inline]
fn write_prop_u16(buf: &mut BytesMut, id: u8, val: u16) {
    buf.put_u8(id);
    buf.put_u16(val);
}

#[inline]
fn write_prop_u32(buf: &mut BytesMut, id: u8, val: u32) {
    buf.put_u8(id);
    buf.put_u32(val);
}

#[inline]
fn write_prop_bytes(buf: &mut BytesMut, id: u8, val: &[u8]) {
    buf.put_u8(id);
    buf.put_u16(val.len() as u16);
    buf.extend_from_slice(val);
}

#[inline]
fn write_prop_varint(buf: &mut BytesMut, id: u8, mut val: u32) {
    buf.put_u8(id);
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if val == 0 {
            break;
        }
    }
}

// =============================================================================
// Packet Decoding
// =============================================================================

/// Parse the fixed header and extract the packet type, flags, and remaining length.
/// Returns `(packet_type_nibble, flags_nibble, remaining_length, header_size)`.
#[inline]
pub fn parse_fixed_header(buf: &[u8]) -> Result<(u8, u8, usize, usize), CodecError> {
    if buf.is_empty() {
        return Err(CodecError::Incomplete);
    }

    let first_byte = buf[0];
    let packet_type = first_byte >> 4;
    let flags = first_byte & 0x0F;

    let (remaining_length, rl_bytes) = decode_remaining_length(&buf[1..])?;

    Ok((packet_type, flags, remaining_length, 1 + rl_bytes))
}

/// Decode the first packet from a buffer, expected to be CONNECT.
///
/// Returns `(Connect, usize)` on success. If the packet is not CONNECT,
/// returns `Err(ProtocolError)`.
pub fn decode_connect_packet(buf: &[u8]) -> Result<(Connect, usize), CodecError> {
    let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;
    validate_fixed_header_flags(type_nibble, flags)?;

    let total_size = header_size + remaining_length;
    if buf.len() < total_size {
        return Err(CodecError::Incomplete);
    }

    if type_nibble != 1 {
        return Err(CodecError::ProtocolError);
    }

    let mut pos = header_size;
    let connect = decode_connect(buf, &mut pos)?;
    Ok((connect, total_size))
}

/// PUBLISH decode from raw `&[u8]` — topic and payload borrow from the input slice.
///
/// Used by `decode_packet(&[u8])` so the returned Publish borrows from the caller's
/// buffer, not from a temporary `Bytes` value.
/// Decode a PUBLISH packet from frozen Bytes, returning the Publish struct directly.
/// Zero-copy: topic and payload borrow from the input buffer.
pub fn decode_publish_from_frozen<'a>(
    buf: &'a [u8],
    start: usize,
    end: usize,
    flags: u8,
    is_v5: bool,
) -> Result<Publish<'a>, CodecError> {
    decode_publish_zero_copy_v(buf, start, end, flags, is_v5)
}

pub fn decode_publish_from_slice<'a>(
    buf: &'a [u8],
    start: usize,
    end: usize,
    flags: u8,
    is_v5: bool,
) -> Result<Publish<'a>, CodecError> {
    let dup = flags & 0x08 != 0;
    let qos_val = (flags >> 1) & 0x03;
    let qos = QoS::from_u8(qos_val).ok_or(CodecError::InvalidQoS(qos_val))?;
    let retain = flags & 0x01 != 0;

    let data = &buf[start..end];
    let mut pos = 0;

    // Topic: 2-byte length + UTF-8 data
    if data.len() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let topic_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;
    if data.len() < pos + topic_len {
        return Err(CodecError::UnexpectedEof);
    }
    std::str::from_utf8(&data[pos..pos + topic_len]).map_err(|_| CodecError::InvalidUtf8)?;
    let topic: &'a [u8] = &buf[start + pos..start + pos + topic_len];
    pos += topic_len;

    // Packet ID
    let packet_id = if qos != QoS::AtMostOnce {
        if data.len() < pos + 2 {
            return Err(CodecError::UnexpectedEof);
        }
        let id = u16::from_be_bytes([data[pos], data[pos + 1]]);
        if id == 0 {
            return Err(CodecError::ProtocolError);
        }
        pos += 2;
        Some(id)
    } else {
        None
    };

    // MQTT 5.0: read properties between packet_id and payload.
    let properties = if is_v5 {
        let mut abs_pos = start + pos;
        let props = read_properties_zc(buf, &mut abs_pos)?;
        pos = abs_pos - start;
        props
    } else {
        Properties::default()
    };

    // Payload: borrow directly from the input buffer
    let payload: &'a [u8] = if pos < data.len() {
        &buf[start + pos..end]
    } else {
        &[]
    };

    Ok(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
        properties,
    })
}

/// Zero-copy PUBLISH decode: topic and payload borrow from the input buffer.
pub fn decode_publish_zero_copy<'a>(
    buf: &'a [u8],
    start: usize,
    end: usize,
    flags: u8,
) -> Result<Publish<'a>, CodecError> {
    decode_publish_zero_copy_v(buf, start, end, flags, false)
}

pub fn decode_publish_zero_copy_v<'a>(
    buf: &'a [u8],
    start: usize,
    end: usize,
    flags: u8,
    is_v5: bool,
) -> Result<Publish<'a>, CodecError> {
    let dup = flags & 0x08 != 0;
    let qos_val = (flags >> 1) & 0x03;
    let qos = QoS::from_u8(qos_val).ok_or(CodecError::InvalidQoS(qos_val))?;
    let retain = flags & 0x01 != 0;

    let data = &buf[start..end];
    let mut pos = 0;

    // Topic: 2-byte length + UTF-8 data
    if data.len() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let topic_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;
    if data.len() < pos + topic_len {
        return Err(CodecError::UnexpectedEof);
    }
    // Validate UTF-8
    std::str::from_utf8(&data[pos..pos + topic_len]).map_err(|_| CodecError::InvalidUtf8)?;
    // Borrow directly from the input buffer (no Arc refcount bump)
    let topic: &'a [u8] = &buf[start + pos..start + pos + topic_len];
    pos += topic_len;

    // Packet ID
    let packet_id = if qos != QoS::AtMostOnce {
        if data.len() < pos + 2 {
            return Err(CodecError::UnexpectedEof);
        }
        let id = u16::from_be_bytes([data[pos], data[pos + 1]]);
        // GAP-1: MQTT 3.1.1 SS 2.3.1 — packet identifier must be non-zero.
        if id == 0 {
            return Err(CodecError::ProtocolError);
        }
        pos += 2;
        Some(id)
    } else {
        None
    };

    // MQTT 5.0: read properties between packet_id and payload.
    // Zero-copy: validate then slice from the input buffer.
    let properties = if is_v5 {
        let mut abs_pos = start + pos;
        let props = read_properties_zc(buf, &mut abs_pos)?;
        pos = abs_pos - start;
        props
    } else {
        Properties::default()
    };

    // Payload: borrow directly from the input buffer
    let payload: &'a [u8] = if pos < data.len() {
        &buf[start + pos..end]
    } else {
        &[]
    };

    Ok(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
        properties,
    })
}

/// Decoded PUBLISH metadata returned by `decode_publish_to_envelope`.
pub struct PublishMeta {
    pub qos: QoS,
    pub retain: bool,
    pub dup: bool,
    pub packet_id: Option<u16>,
    /// Topic bytes range within the envelope (for alias resolution).
    pub topic_offset: usize,
    pub topic_len: usize,
    /// Properties byte range within the envelope (for property extraction).
    pub props_len: usize,
}

/// Fused MQTT PUBLISH decode + MqttEnvelope build in one pass.
///
/// Parses the MQTT PUBLISH wire format and writes the MqttEnvelope directly,
/// avoiding the intermediate Publish struct, Bytes::slice refcount bumps,
/// and the separate MqttEnvelopeBuilder allocation.
///
/// Returns `(envelope_buf, meta, consumed_bytes)`.
/// The envelope is returned as `BytesMut` so it can be patched in-place
/// (e.g., TTL, flags) before freezing.
pub fn decode_publish_to_envelope(
    buf: &[u8],
    is_v5: bool,
    timestamp: u64,
    publisher_id: u64,
) -> Result<(BytesMut, PublishMeta, usize), CodecError> {
    // Parse fixed header.
    if buf.is_empty() {
        return Err(CodecError::Incomplete);
    }
    let first_byte = buf[0];
    let type_nibble = first_byte >> 4;
    if type_nibble != 3 {
        return Err(CodecError::UnknownPacketType(type_nibble));
    }
    let flags = first_byte & 0x0F;
    let dup = flags & 0x08 != 0;
    let qos_val = (flags >> 1) & 0x03;
    let qos = QoS::from_u8(qos_val).ok_or(CodecError::InvalidQoS(qos_val))?;
    let retain = flags & 0x01 != 0;

    let (remaining_length, rl_bytes) = decode_remaining_length(&buf[1..])?;
    let header_size = 1 + rl_bytes;
    let total_size = header_size + remaining_length;
    if buf.len() < total_size {
        return Err(CodecError::Incomplete);
    }

    let data = &buf[header_size..total_size];
    let mut pos = 0;

    // Topic length + topic bytes.
    if data.len() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let topic_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;
    if data.len() < pos + topic_len {
        return Err(CodecError::UnexpectedEof);
    }
    let topic_start = pos;
    pos += topic_len;

    // Packet ID (QoS 1/2 only).
    let packet_id = if qos != QoS::AtMostOnce {
        if data.len() < pos + 2 {
            return Err(CodecError::UnexpectedEof);
        }
        let id = u16::from_be_bytes([data[pos], data[pos + 1]]);
        if id == 0 {
            return Err(CodecError::ProtocolError);
        }
        pos += 2;
        Some(id)
    } else {
        None
    };

    // Properties (V5 only) — just extract the byte range, no validation.
    let (props_start, props_len) = if is_v5 {
        let mut cursor: &[u8] = &data[pos..];
        let prop_len = read_variable_int(&mut cursor)? as usize;
        let varint_consumed = (data.len() - pos) - cursor.len();
        if cursor.remaining() < prop_len {
            return Err(CodecError::UnexpectedEof);
        }
        let ps = pos + varint_consumed;
        pos = ps + prop_len;
        (ps, prop_len)
    } else {
        (pos, 0)
    };

    // Payload: remaining bytes.
    let payload_start = pos;
    let payload_len = data.len() - pos;

    // Build the envelope directly — single allocation, single pass of copies.
    let env_flags = {
        let mut f = flat::FLAG_FORMAT_MQTT;
        if retain {
            f |= 1 << 0; // MQTT_FLAG_RETAIN
        }
        if is_v5 {
            f |= 1 << 1; // MQTT_FLAG_IS_V5
        }
        f
    };

    let env_total = 40 + topic_len + props_len + payload_len;
    let mut env_buf = BytesMut::with_capacity(env_total);

    // 40-byte envelope header.
    env_buf.put_u16_le(env_flags);
    env_buf.put_u16_le(0); // reserved
    env_buf.put_u16_le(topic_len as u16);
    env_buf.put_u16_le(props_len as u16);
    env_buf.put_u64_le(timestamp);
    env_buf.put_u64_le(0); // ttl_ms (set later if needed)
    env_buf.put_u64_le(publisher_id);
    env_buf.put_u64_le(0); // padding

    // Copy topic + properties + payload from wire in order.
    env_buf.put_slice(&data[topic_start..topic_start + topic_len]);
    if props_len > 0 {
        env_buf.put_slice(&data[props_start..props_start + props_len]);
    }
    if payload_len > 0 {
        env_buf.put_slice(&data[payload_start..payload_start + payload_len]);
    }

    debug_assert_eq!(env_buf.len(), env_total);

    let meta = PublishMeta {
        qos,
        retain,
        dup,
        packet_id,
        topic_offset: 40, // always at byte 40 in the envelope
        topic_len,
        props_len,
    };

    Ok((env_buf, meta, total_size))
}

/// Set TTL and additional flags in an already-built envelope buffer.
/// This is used after `decode_publish_to_envelope` to patch in session-level metadata.
#[inline]
pub fn patch_envelope_ttl_flags(envelope: &mut [u8], ttl_ms: u64, extra_flags: u16) {
    if envelope.len() < 40 {
        return;
    }
    // Patch flags (OR in extra bits).
    if extra_flags != 0 {
        let current = u16::from_le_bytes([envelope[0], envelope[1]]);
        let updated = current | extra_flags;
        envelope[0..2].copy_from_slice(&updated.to_le_bytes());
    }
    // Patch ttl_ms at offset 16.
    if ttl_ms != 0 {
        // Set HAS_TTL flag (bit 2).
        let current = u16::from_le_bytes([envelope[0], envelope[1]]);
        let updated = current | (1 << 2); // MQTT_FLAG_HAS_TTL
        envelope[0..2].copy_from_slice(&updated.to_le_bytes());
        envelope[16..24].copy_from_slice(&ttl_ms.to_le_bytes());
    }
}

pub fn decode_connect(buf: &[u8], pos: &mut usize) -> Result<Connect, CodecError> {
    // Protocol Name
    let protocol_name = read_mqtt_string_zc(buf, pos)?;
    if &protocol_name[..] != b"MQTT" {
        return Err(CodecError::InvalidProtocolName(protocol_name));
    }

    // Protocol Level
    let level = read_u8_zc(buf, pos)?;
    let protocol_version =
        ProtocolVersion::from_level(level).ok_or(CodecError::UnsupportedProtocolVersion(level))?;

    // Connect Flags
    let flags_byte = read_u8_zc(buf, pos)?;
    let flags = ConnectFlags::from_byte(flags_byte).ok_or(CodecError::InvalidConnectFlags)?;

    // V3.1.1: password flag set without username flag is invalid.
    if protocol_version == ProtocolVersion::V311 && flags.password && !flags.username {
        return Err(CodecError::InvalidConnectFlags);
    }

    // Keep Alive
    let keep_alive = read_u16_zc(buf, pos)?;

    // MQTT 5.0 properties
    let properties_raw = if protocol_version == ProtocolVersion::V5 {
        let props = read_properties_zc(buf, pos)?;
        props.raw().to_vec()
    } else {
        Vec::new()
    };

    // Payload: Client Identifier
    let client_id = read_mqtt_string_zc(buf, pos)?;

    // Will message (if will flag set)
    let will = if flags.will {
        // MQTT 5.0 will properties
        let will_properties_raw = if protocol_version == ProtocolVersion::V5 {
            let props = read_properties_zc(buf, pos)?;
            props.raw().to_vec()
        } else {
            Vec::new()
        };
        let will_topic = read_mqtt_string_zc(buf, pos)?;
        let will_payload = read_mqtt_bytes_zc(buf, pos)?;
        Some(WillMessage {
            topic: will_topic,
            payload: will_payload,
            qos: flags.will_qos,
            retain: flags.will_retain,
            properties_raw: will_properties_raw,
        })
    } else {
        None
    };

    // Username
    let username = if flags.username {
        Some(read_mqtt_string_zc(buf, pos)?)
    } else {
        None
    };

    // Password
    let password = if flags.password {
        Some(read_mqtt_bytes_zc(buf, pos)?)
    } else {
        None
    };

    Ok(Connect {
        protocol_name,
        protocol_version,
        flags,
        keep_alive,
        client_id,
        will,
        username,
        password,
        properties_raw,
    })
}

pub fn decode_connack(buf: &[u8], pos: &mut usize, end: usize) -> Result<ConnAck, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let ack_flags = read_u8_zc(buf, pos)?;
    // MQTT 3.1.1 SS 3.2.2.1 / 5.0 SS 3.2.2.1: bits 7-1 are reserved and MUST be 0.
    if ack_flags & 0xFE != 0 {
        return Err(CodecError::InvalidFixedHeaderFlags(2)); // CONNACK packet type
    }
    let session_present = ack_flags & 0x01 != 0;
    let return_code = read_u8_zc(buf, pos)?;

    let mut connack = ConnAck {
        session_present,
        return_code,
        ..ConnAck::default()
    };

    // MQTT 5.0 properties (if data remains)
    if *pos < end {
        let props = read_properties_zc(buf, pos)?;
        if !props.is_empty() {
            parse_connack_props(props.raw(), &mut connack)?;
        }
    }

    Ok(connack)
}

/// Parse ConnAck property bytes into individual ConnAck struct fields.
fn parse_connack_props(data: &[u8], connack: &mut ConnAck) -> Result<(), CodecError> {
    let mut p = 0;
    while p < data.len() {
        let id = data[p];
        p += 1;
        match id {
            0x11 => {
                // session_expiry_interval
                if data.len() - p < 4 {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.session_expiry_interval = Some(u32::from_be_bytes([
                    data[p],
                    data[p + 1],
                    data[p + 2],
                    data[p + 3],
                ]));
                p += 4;
            }
            0x21 => {
                // receive_maximum
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.receive_maximum = Some(u16::from_be_bytes([data[p], data[p + 1]]));
                p += 2;
            }
            0x24 => {
                // maximum_qos
                if p >= data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.maximum_qos = Some(data[p]);
                p += 1;
            }
            0x25 => {
                // retain_available
                if p >= data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.retain_available = Some(data[p] != 0);
                p += 1;
            }
            0x27 => {
                // maximum_packet_size
                if data.len() - p < 4 {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.maximum_packet_size = Some(u32::from_be_bytes([
                    data[p],
                    data[p + 1],
                    data[p + 2],
                    data[p + 3],
                ]));
                p += 4;
            }
            0x12 => {
                // assigned_client_identifier (UTF-8 string)
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                if data.len() - p < 2 + len {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.assigned_client_identifier =
                    Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                p += 2 + len;
            }
            0x22 => {
                // topic_alias_maximum
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.topic_alias_maximum = Some(u16::from_be_bytes([data[p], data[p + 1]]));
                p += 2;
            }
            0x1F => {
                // reason_string (UTF-8 string)
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                if data.len() - p < 2 + len {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.reason_string = Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                p += 2 + len;
            }
            0x1A => {
                // response_information (UTF-8 string)
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                if data.len() - p < 2 + len {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.response_information =
                    Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                p += 2 + len;
            }
            0x28 => {
                // wildcard_subscription_available
                if p >= data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.wildcard_subscription_available = Some(data[p] != 0);
                p += 1;
            }
            0x29 => {
                // subscription_identifier_available
                if p >= data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.subscription_identifier_available = Some(data[p] != 0);
                p += 1;
            }
            0x2A => {
                // shared_subscription_available
                if p >= data.len() {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.shared_subscription_available = Some(data[p] != 0);
                p += 1;
            }
            0x13 => {
                // server_keep_alive
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.server_keep_alive = Some(u16::from_be_bytes([data[p], data[p + 1]]));
                p += 2;
            }
            0x15 => {
                // authentication_method (UTF-8 string)
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                if data.len() - p < 2 + len {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.authentication_method =
                    Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                p += 2 + len;
            }
            0x16 => {
                // authentication_data (binary)
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                if data.len() - p < 2 + len {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.authentication_data =
                    Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                p += 2 + len;
            }
            0x1C => {
                // server_reference (UTF-8 string)
                if data.len() - p < 2 {
                    return Err(CodecError::UnexpectedEof);
                }
                let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                if data.len() - p < 2 + len {
                    return Err(CodecError::UnexpectedEof);
                }
                connack.server_reference = Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                p += 2 + len;
            }
            _ => {
                p += Properties::skip_value(id, &data[p..])
                    .ok_or(CodecError::MalformedProperties)?;
            }
        }
    }
    Ok(())
}

/// Extract optional reason_string from a Properties section as a borrowed slice.
fn extract_reason_string_ref<'a>(props: &Properties<'a>) -> Option<&'a [u8]> {
    props.find_str_ref(0x1F)
}

fn extract_reason_string(props: &Properties<'_>) -> Option<Bytes> {
    props.find_str_ref(0x1F).map(Bytes::copy_from_slice)
}

pub fn decode_puback<'a>(
    buf: &'a [u8],
    pos: &mut usize,
    end: usize,
) -> Result<PubAck<'a>, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;
    let reason_code = if *pos < end {
        Some(read_u8_zc(buf, pos)?)
    } else {
        None
    };
    let reason_string = if *pos < end {
        let props = read_properties_zc(buf, pos)?;
        extract_reason_string_ref(&props)
    } else {
        None
    };

    Ok(PubAck {
        packet_id,
        reason_code,
        reason_string,
    })
}

pub fn decode_pubrec<'a>(
    buf: &'a [u8],
    pos: &mut usize,
    end: usize,
) -> Result<PubRec<'a>, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;
    let reason_code = if *pos < end {
        Some(read_u8_zc(buf, pos)?)
    } else {
        None
    };
    let reason_string = if *pos < end {
        let props = read_properties_zc(buf, pos)?;
        extract_reason_string_ref(&props)
    } else {
        None
    };

    Ok(PubRec {
        packet_id,
        reason_code,
        reason_string,
    })
}

pub fn decode_pubrel<'a>(
    buf: &'a [u8],
    pos: &mut usize,
    end: usize,
) -> Result<PubRel<'a>, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;
    let reason_code = if *pos < end {
        Some(read_u8_zc(buf, pos)?)
    } else {
        None
    };
    let reason_string = if *pos < end {
        let props = read_properties_zc(buf, pos)?;
        extract_reason_string_ref(&props)
    } else {
        None
    };

    Ok(PubRel {
        packet_id,
        reason_code,
        reason_string,
    })
}

pub fn decode_pubcomp<'a>(
    buf: &'a [u8],
    pos: &mut usize,
    end: usize,
) -> Result<PubComp<'a>, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;
    let reason_code = if *pos < end {
        Some(read_u8_zc(buf, pos)?)
    } else {
        None
    };
    let reason_string = if *pos < end {
        let props = read_properties_zc(buf, pos)?;
        extract_reason_string_ref(&props)
    } else {
        None
    };

    Ok(PubComp {
        packet_id,
        reason_code,
        reason_string,
    })
}

pub fn decode_subscribe(buf: &[u8], pos: &mut usize, end: usize) -> Result<Subscribe, CodecError> {
    decode_subscribe_v(buf, pos, end, false)
}

pub fn decode_subscribe_v(
    buf: &[u8],
    pos: &mut usize,
    end: usize,
    is_v5: bool,
) -> Result<Subscribe, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;
    // GAP-1: MQTT 3.1.1 §2.3.1 — packet identifier must be non-zero.
    if packet_id == 0 {
        return Err(CodecError::ProtocolError);
    }

    // MQTT 5.0 properties sit between packet_id and topic filters.
    let subscription_identifier = if is_v5 {
        let props = read_properties_zc(buf, pos)?;
        props.subscription_identifier()
    } else {
        None
    };

    // GAP-5: MQTT 3.1.1 §3.8.3-3 — SUBSCRIBE must contain at least one topic filter.
    if *pos >= end {
        return Err(CodecError::ProtocolError);
    }

    let mut filters = SmallVec::new();
    while *pos < end {
        let filter = read_mqtt_string_zc(buf, pos)?;
        if *pos >= end {
            return Err(CodecError::UnexpectedEof);
        }
        let options_byte = read_u8_zc(buf, pos)?;
        // GAP-6: MQTT 3.1.1 §3.8.3-4 — reserved bits [7:2] of options byte must be zero in v3.1.1.
        if !is_v5 && (options_byte & 0xFC) != 0 {
            return Err(CodecError::ProtocolError);
        }
        let qos = QoS::from_u8(options_byte & 0x03).ok_or(CodecError::InvalidQoS(options_byte))?;
        let no_local = options_byte & 0x04 != 0;
        let retain_as_published = options_byte & 0x08 != 0;
        let retain_handling = (options_byte >> 4) & 0x03;

        filters.push(TopicFilter {
            filter,
            qos,
            no_local,
            retain_as_published,
            retain_handling,
        });
    }

    Ok(Subscribe {
        packet_id,
        filters,
        subscription_identifier,
    })
}

pub fn decode_suback(buf: &[u8], pos: &mut usize, end: usize) -> Result<SubAck, CodecError> {
    decode_suback_v(buf, pos, end, false)
}

pub fn decode_suback_v(
    buf: &[u8],
    pos: &mut usize,
    end: usize,
    is_v5: bool,
) -> Result<SubAck, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;

    let reason_string = if is_v5 {
        let props = read_properties_zc(buf, pos)?;
        extract_reason_string(&props)
    } else {
        None
    };

    let mut return_codes = SmallVec::new();
    while *pos < end {
        return_codes.push(read_u8_zc(buf, pos)?);
    }

    Ok(SubAck {
        packet_id,
        return_codes,
        reason_string,
    })
}

pub fn decode_unsubscribe(
    buf: &[u8],
    pos: &mut usize,
    end: usize,
) -> Result<Unsubscribe, CodecError> {
    decode_unsubscribe_v(buf, pos, end, false)
}

pub fn decode_unsubscribe_v(
    buf: &[u8],
    pos: &mut usize,
    end: usize,
    is_v5: bool,
) -> Result<Unsubscribe, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;
    // GAP-1: MQTT 3.1.1 §2.3.1 — packet identifier must be non-zero.
    if packet_id == 0 {
        return Err(CodecError::ProtocolError);
    }

    // MQTT 5.0 properties — skip for unsubscribe (no fields extracted).
    if is_v5 {
        let _props = read_properties_zc(buf, pos)?;
    }

    // GAP-7: MQTT 3.1.1 §3.10.3-2 — UNSUBSCRIBE must contain at least one topic filter.
    if *pos >= end {
        return Err(CodecError::ProtocolError);
    }

    let mut filters = SmallVec::new();
    while *pos < end {
        filters.push(read_mqtt_string_zc(buf, pos)?);
    }

    Ok(Unsubscribe { packet_id, filters })
}

pub fn decode_unsuback(buf: &[u8], pos: &mut usize, end: usize) -> Result<UnsubAck, CodecError> {
    decode_unsuback_v(buf, pos, end, false)
}

pub fn decode_unsuback_v(
    buf: &[u8],
    pos: &mut usize,
    end: usize,
    is_v5: bool,
) -> Result<UnsubAck, CodecError> {
    if end - *pos < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = read_u16_zc(buf, pos)?;

    let reason_string = if is_v5 {
        let props = read_properties_zc(buf, pos)?;
        extract_reason_string(&props)
    } else {
        None
    };

    let mut reason_codes = SmallVec::new();
    while *pos < end {
        reason_codes.push(read_u8_zc(buf, pos)?);
    }

    Ok(UnsubAck {
        packet_id,
        reason_codes,
        reason_string,
    })
}

pub fn decode_disconnect(
    buf: &[u8],
    pos: &mut usize,
    end: usize,
    remaining_length: usize,
) -> Result<Disconnect, CodecError> {
    if remaining_length == 0 {
        return Ok(Disconnect {
            reason_code: None,
            session_expiry_interval: None,
            reason_string: None,
            server_reference: None,
        });
    }

    let reason_code = if *pos < end {
        Some(read_u8_zc(buf, pos)?)
    } else {
        None
    };

    let mut session_expiry_interval = None;
    let mut reason_string = None;
    let mut server_reference = None;

    if *pos < end {
        let props = read_properties_zc(buf, pos)?;
        if !props.is_empty() {
            let data = props.raw();
            let mut p = 0;
            while p < data.len() {
                let id = data[p];
                p += 1;
                match id {
                    0x11 => {
                        // session_expiry_interval
                        if data.len() - p < 4 {
                            return Err(CodecError::UnexpectedEof);
                        }
                        session_expiry_interval = Some(u32::from_be_bytes([
                            data[p],
                            data[p + 1],
                            data[p + 2],
                            data[p + 3],
                        ]));
                        p += 4;
                    }
                    0x1F => {
                        // reason_string
                        if data.len() - p < 2 {
                            return Err(CodecError::UnexpectedEof);
                        }
                        let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                        if data.len() - p < 2 + len {
                            return Err(CodecError::UnexpectedEof);
                        }
                        reason_string = Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                        p += 2 + len;
                    }
                    0x1C => {
                        // server_reference
                        if data.len() - p < 2 {
                            return Err(CodecError::UnexpectedEof);
                        }
                        let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                        if data.len() - p < 2 + len {
                            return Err(CodecError::UnexpectedEof);
                        }
                        server_reference = Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                        p += 2 + len;
                    }
                    _ => {
                        p += Properties::skip_value(id, &data[p..])
                            .ok_or(CodecError::MalformedProperties)?;
                    }
                }
            }
        }
    }

    Ok(Disconnect {
        reason_code,
        session_expiry_interval,
        reason_string,
        server_reference,
    })
}

/// Decode an AUTH packet (MQTT 5.0 only, SS 3.15).
pub fn decode_auth(
    buf: &[u8],
    pos: &mut usize,
    end: usize,
    remaining_length: usize,
) -> Result<Auth, CodecError> {
    if remaining_length == 0 {
        // Remaining length 0 ⇒ reason code 0x00 (Success), no properties.
        return Ok(Auth {
            reason_code: Auth::SUCCESS,
            authentication_method: None,
            authentication_data: None,
            reason_string: None,
        });
    }

    let reason_code = read_u8_zc(buf, pos)?;

    let mut authentication_method = None;
    let mut authentication_data = None;
    let mut reason_string = None;

    if remaining_length > 1 && *pos < end {
        let props = read_properties_zc(buf, pos)?;
        if !props.is_empty() {
            let data = props.raw();
            let mut p = 0;
            while p < data.len() {
                let id = data[p];
                p += 1;
                match id {
                    0x15 => {
                        // authentication_method
                        if data.len() - p < 2 {
                            return Err(CodecError::UnexpectedEof);
                        }
                        let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                        if data.len() - p < 2 + len {
                            return Err(CodecError::UnexpectedEof);
                        }
                        authentication_method =
                            Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                        p += 2 + len;
                    }
                    0x16 => {
                        // authentication_data
                        if data.len() - p < 2 {
                            return Err(CodecError::UnexpectedEof);
                        }
                        let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                        if data.len() - p < 2 + len {
                            return Err(CodecError::UnexpectedEof);
                        }
                        authentication_data =
                            Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                        p += 2 + len;
                    }
                    0x1F => {
                        // reason_string
                        if data.len() - p < 2 {
                            return Err(CodecError::UnexpectedEof);
                        }
                        let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
                        if data.len() - p < 2 + len {
                            return Err(CodecError::UnexpectedEof);
                        }
                        reason_string = Some(Bytes::copy_from_slice(&data[p + 2..p + 2 + len]));
                        p += 2 + len;
                    }
                    _ => {
                        p += Properties::skip_value(id, &data[p..])
                            .ok_or(CodecError::MalformedProperties)?;
                    }
                }
            }
        }
    }

    Ok(Auth {
        reason_code,
        authentication_method,
        authentication_data,
        reason_string,
    })
}

// =============================================================================
// Packet Encoding
// =============================================================================

/// Encode an MQTT PUBLISH packet directly from a `FlatMessage`, bypassing the
/// intermediate `Publish` and `Properties` structs entirely.
///
/// This is the zero-allocation outbound hot path. The topic, payload, and all
/// MQTT 5.0 properties are read directly from the FlatMessage's zero-copy spans
/// and written into the wire buffer in a single pass.
///
/// # Arguments
/// - `flat_msg`: The flat message containing the payload, routing key (topic), and headers.
/// - `qos`: Effective QoS for delivery.
/// - `retain`: Whether the retain flag should be set.
/// - `dup`: Whether the DUP flag should be set.
/// - `packet_id`: Packet identifier (required for QoS 1/2).
/// - `is_v5`: Whether to encode MQTT 5.0 properties.
/// - `subscription_id`: MQTT 5.0 subscription identifier (if any).
/// - `topic_alias`: MQTT 5.0 topic alias (if any).
/// - `buf`: Output buffer to write the encoded packet into.
/// Encode a PUBLISH directly from a FlatMessage (zero-copy delivery path).
///
/// `topic_alias_info`: `Some((alias, is_new))` where `is_new` indicates whether
/// this is a newly assigned alias (full topic sent) or an existing one (empty topic
/// per MQTT 5.0 SS 3.3.2.3.4).
pub fn encode_publish_from_flat(
    flat_msg: &FlatMessage,
    qos: QoS,
    retain: bool,
    dup: bool,
    packet_id: Option<u16>,
    is_v5: bool,
    subscription_id: Option<u32>,
    topic_alias_info: Option<(u16, bool)>,
    buf: &mut BytesMut,
) {
    encode_publish_from_flat_with_expiry(
        flat_msg,
        qos,
        retain,
        dup,
        packet_id,
        is_v5,
        subscription_id,
        topic_alias_info,
        None,
        buf,
    )
}

/// Like `encode_publish_from_flat` but with an optional adjusted message expiry
/// interval (in seconds). When provided, this overrides the FlatMessage's ttl_ms
/// so the subscriber receives the **remaining** lifetime per MQTT 5.0 §3.3.2.3.3.
pub fn encode_publish_from_flat_with_expiry(
    flat_msg: &FlatMessage,
    qos: QoS,
    retain: bool,
    dup: bool,
    packet_id: Option<u16>,
    is_v5: bool,
    subscription_id: Option<u32>,
    topic_alias_info: Option<(u16, bool)>,
    adjusted_expiry_secs: Option<u32>,
    buf: &mut BytesMut,
) {
    let topic = flat_msg.routing_key().unwrap_or_default();
    let payload = flat_msg.value();

    // For existing topic aliases, send empty topic (m7 optimization).
    let send_empty_topic = matches!(topic_alias_info, Some((_, false)));
    let topic_alias = topic_alias_info.map(|(alias, _)| alias);

    // Compute MQTT 5.0 properties size.
    let props_content_size = if is_v5 {
        compute_flat_properties_size(flat_msg, subscription_id, topic_alias, adjusted_expiry_secs)
    } else {
        0
    };
    let props_total_size = if is_v5 {
        variable_int_size(props_content_size as u32) + props_content_size
    } else {
        0
    };

    // Variable header: topic (2 + len) + optional packet_id (2) + properties
    let topic_wire_len = if send_empty_topic { 0 } else { topic.len() };
    let var_header_size =
        2 + topic_wire_len + if qos != QoS::AtMostOnce { 2 } else { 0 } + props_total_size;

    let remaining = var_header_size + payload.len();

    // Reserve capacity for the entire packet.
    buf.reserve(1 + 4 + remaining);

    // Fixed header byte.
    let mut first_byte = 0x30u8;
    if dup {
        first_byte |= 0x08;
    }
    first_byte |= (qos.as_u8() & 0x03) << 1;
    if retain {
        first_byte |= 0x01;
    }
    buf.put_u8(first_byte);
    encode_remaining_length(remaining, buf);

    // Topic (MQTT string: 2-byte length + raw bytes).
    // For existing topic aliases, send empty topic per MQTT 5.0 SS 3.3.2.3.4.
    if send_empty_topic {
        buf.put_u16(0);
    } else {
        buf.put_u16(topic.len() as u16);
        buf.extend_from_slice(&topic);
    }

    // Packet ID (QoS 1/2 only).
    if qos != QoS::AtMostOnce {
        if let Some(id) = packet_id {
            buf.put_u16(id);
        }
    }

    // MQTT 5.0 properties.
    if is_v5 {
        write_variable_int(props_content_size as u32, buf);
        write_flat_properties(
            flat_msg,
            subscription_id,
            topic_alias,
            adjusted_expiry_secs,
            buf,
        );
    }

    // Payload (from FlatMessage value span — zero-copy from mmap).
    buf.extend_from_slice(&payload);
}

/// Compute the encoded byte size of MQTT 5.0 properties extracted from a FlatMessage.
fn compute_flat_properties_size(
    flat_msg: &FlatMessage,
    subscription_id: Option<u32>,
    topic_alias: Option<u16>,
    adjusted_expiry_secs: Option<u32>,
) -> usize {
    let mut size = 0;

    // message_expiry_interval (0x02): 1 + 4
    // Use adjusted value if provided (remaining lifetime), else fall back to stored TTL.
    match adjusted_expiry_secs {
        Some(secs) if secs > 0 => {
            size += 1 + 4;
        }
        Some(_) => {} // zero or expired — don't include
        None => {
            if let Some(ttl_ms) = flat_msg.ttl_ms() {
                if ttl_ms / 1000 > 0 {
                    size += 1 + 4;
                }
            }
        }
    }

    // response_topic (0x08): 1 + 2 + len
    if let Some(rt) = flat_msg.reply_to() {
        size += 1 + 2 + rt.len();
    }

    // correlation_data (0x09): 1 + 2 + len
    if let Some(cd) = flat_msg.correlation_id() {
        size += 1 + 2 + cd.len();
    }

    // subscription_identifier (0x0B): 1 + variable_int
    if let Some(sub_id) = subscription_id {
        size += 1 + variable_int_size(sub_id);
    }

    // topic_alias (0x23): 1 + 2
    if topic_alias.is_some() {
        size += 1 + 2;
    }

    // FlatMessage headers → MQTT properties
    for i in 0..flat_msg.header_count() {
        let (k, v) = flat_msg.header(i);
        if &k[..] == b"mqtt.content_type" {
            // content_type (0x03): 1 + 2 + len
            size += 1 + 2 + v.len();
        } else if &k[..] == b"mqtt.payload_format" && !v.is_empty() {
            // payload_format_indicator (0x01): 1 + 1
            size += 1 + 1;
        } else if k.starts_with(b"mqtt.user.") {
            // user_property (0x26): 1 + (2 + key_len) + (2 + val_len)
            let user_key_len = k.len() - 10; // skip "mqtt.user."
            size += 1 + 2 + user_key_len + 2 + v.len();
        }
    }

    size
}

/// Write MQTT 5.0 properties extracted from a FlatMessage into the buffer.
fn write_flat_properties(
    flat_msg: &FlatMessage,
    subscription_id: Option<u32>,
    topic_alias: Option<u16>,
    adjusted_expiry_secs: Option<u32>,
    buf: &mut BytesMut,
) {
    // payload_format_indicator (0x01) — write early per MQTT convention
    // (handled below in header loop to avoid double iteration)

    // message_expiry_interval (0x02)
    // Use adjusted value (remaining lifetime) if provided, else fall back to stored TTL.
    match adjusted_expiry_secs {
        Some(secs) if secs > 0 => {
            buf.put_u8(0x02);
            buf.put_u32(secs);
        }
        Some(_) => {} // zero or expired — omit
        None => {
            if let Some(ttl_ms) = flat_msg.ttl_ms() {
                let secs = (ttl_ms / 1000) as u32;
                if secs > 0 {
                    buf.put_u8(0x02);
                    buf.put_u32(secs);
                }
            }
        }
    }

    // response_topic (0x08)
    if let Some(rt) = flat_msg.reply_to() {
        buf.put_u8(0x08);
        buf.put_u16(rt.len() as u16);
        buf.extend_from_slice(&rt);
    }

    // correlation_data (0x09)
    if let Some(cd) = flat_msg.correlation_id() {
        buf.put_u8(0x09);
        buf.put_u16(cd.len() as u16);
        buf.extend_from_slice(&cd);
    }

    // subscription_identifier (0x0B)
    if let Some(sub_id) = subscription_id {
        buf.put_u8(0x0B);
        write_variable_int(sub_id, buf);
    }

    // topic_alias (0x23)
    if let Some(alias) = topic_alias {
        buf.put_u8(0x23);
        buf.put_u16(alias);
    }

    // FlatMessage headers → MQTT properties
    for i in 0..flat_msg.header_count() {
        let (k, v) = flat_msg.header(i);
        if &k[..] == b"mqtt.content_type" {
            buf.put_u8(0x03);
            buf.put_u16(v.len() as u16);
            buf.extend_from_slice(&v);
        } else if &k[..] == b"mqtt.payload_format" && !v.is_empty() {
            buf.put_u8(0x01);
            buf.put_u8(v[0]);
        } else if k.starts_with(b"mqtt.user.") {
            let user_key = &k[10..]; // skip "mqtt.user."
            buf.put_u8(0x26);
            buf.put_u16(user_key.len() as u16);
            buf.extend_from_slice(user_key);
            buf.put_u16(v.len() as u16);
            buf.extend_from_slice(&v);
        }
    }
}

/// Encode an MQTT PUBLISH packet from an MqttEnvelope.
///
/// For V5 subscribers: copies raw properties from the envelope, filtering out
/// properties that must be per-subscriber (message_expiry_interval 0x02,
/// subscription_identifier 0x0B, topic_alias 0x23), then appends the injected
/// subscription_id, topic_alias, and adjusted_expiry as needed.
///
/// For V3.1.1 subscribers: no properties are written (props_len is 0).
pub fn encode_publish_from_envelope(
    envelope: &MqttEnvelope,
    qos: QoS,
    retain: bool,
    dup: bool,
    packet_id: Option<u16>,
    is_v5: bool,
    subscription_id: Option<u32>,
    topic_alias_info: Option<(u16, bool)>,
    adjusted_expiry_secs: Option<u32>,
    buf: &mut BytesMut,
) {
    let topic = envelope.topic();
    let payload = envelope.payload();

    // For existing topic aliases, send empty topic (m7 optimization).
    let send_empty_topic = matches!(topic_alias_info, Some((_, false)));
    let topic_alias = topic_alias_info.map(|(alias, _)| alias);

    // Compute MQTT 5.0 properties size.
    let props_content_size = if is_v5 {
        compute_envelope_properties_size(
            envelope,
            subscription_id,
            topic_alias,
            adjusted_expiry_secs,
        )
    } else {
        0
    };
    let props_total_size = if is_v5 {
        variable_int_size(props_content_size as u32) + props_content_size
    } else {
        0
    };

    // Variable header: topic (2 + len) + optional packet_id (2) + properties
    let topic_wire_len = if send_empty_topic { 0 } else { topic.len() };
    let var_header_size =
        2 + topic_wire_len + if qos != QoS::AtMostOnce { 2 } else { 0 } + props_total_size;

    let remaining = var_header_size + payload.len();

    buf.reserve(1 + 4 + remaining);

    // Fixed header byte.
    let mut first_byte = 0x30u8;
    if dup {
        first_byte |= 0x08;
    }
    first_byte |= (qos.as_u8() & 0x03) << 1;
    if retain {
        first_byte |= 0x01;
    }
    buf.put_u8(first_byte);
    encode_remaining_length(remaining, buf);

    // Topic.
    if send_empty_topic {
        buf.put_u16(0);
    } else {
        buf.put_u16(topic.len() as u16);
        buf.extend_from_slice(&topic);
    }

    // Packet ID (QoS 1/2 only).
    if qos != QoS::AtMostOnce {
        if let Some(id) = packet_id {
            buf.put_u16(id);
        }
    }

    // MQTT 5.0 properties.
    if is_v5 {
        write_variable_int(props_content_size as u32, buf);
        write_envelope_properties(
            envelope,
            subscription_id,
            topic_alias,
            adjusted_expiry_secs,
            buf,
        );
    }

    // Payload (zero-copy from envelope).
    buf.extend_from_slice(&payload);
}

/// Property IDs that must be filtered from raw envelope properties because they
/// are per-subscriber (injected separately).
const FILTERED_PROP_IDS: [u8; 3] = [0x02, 0x0B, 0x23]; // message_expiry, sub_id, topic_alias

/// Compute the encoded size of MQTT 5.0 properties for an envelope-based PUBLISH.
fn compute_envelope_properties_size(
    envelope: &MqttEnvelope,
    subscription_id: Option<u32>,
    topic_alias: Option<u16>,
    adjusted_expiry_secs: Option<u32>,
) -> usize {
    let mut size = 0;

    // Passthrough properties from envelope (filtering per-subscriber ones)
    let raw_props = envelope.properties_raw();
    if !raw_props.is_empty() {
        size += filtered_properties_size(&raw_props);
    }

    // Injected: message_expiry_interval (0x02)
    match adjusted_expiry_secs {
        Some(secs) if secs > 0 => {
            size += 1 + 4;
        }
        Some(_) => {}
        None => {
            if let Some(ttl) = envelope.ttl_ms() {
                if ttl / 1000 > 0 {
                    size += 1 + 4;
                }
            }
        }
    }

    // Injected: subscription_identifier (0x0B)
    if let Some(sub_id) = subscription_id {
        size += 1 + variable_int_size(sub_id);
    }

    // Injected: topic_alias (0x23)
    if topic_alias.is_some() {
        size += 1 + 2;
    }

    size
}

/// Compute total byte size of raw properties after filtering out FILTERED_PROP_IDS.
fn filtered_properties_size(raw: &[u8]) -> usize {
    let mut size = 0;
    let mut pos = 0;
    while pos < raw.len() {
        let id = raw[pos];
        let start = pos;
        pos += 1;
        if let Some(next) = bisque_mq::flat::skip_mqtt_property(raw, pos, id) {
            if !FILTERED_PROP_IDS.contains(&id) {
                size += next - start; // id byte + value bytes
            }
            pos = next;
        } else {
            break;
        }
    }
    size
}

/// Write MQTT 5.0 properties from an MqttEnvelope into the buffer.
fn write_envelope_properties(
    envelope: &MqttEnvelope,
    subscription_id: Option<u32>,
    topic_alias: Option<u16>,
    adjusted_expiry_secs: Option<u32>,
    buf: &mut BytesMut,
) {
    // Copy passthrough properties (filtering per-subscriber ones)
    let raw_props = envelope.properties_raw();
    if !raw_props.is_empty() {
        copy_properties_filtered(&raw_props, buf);
    }

    // Injected: message_expiry_interval (0x02)
    match adjusted_expiry_secs {
        Some(secs) if secs > 0 => {
            buf.put_u8(0x02);
            buf.put_u32(secs);
        }
        Some(_) => {}
        None => {
            if let Some(ttl) = envelope.ttl_ms() {
                let secs = (ttl / 1000) as u32;
                if secs > 0 {
                    buf.put_u8(0x02);
                    buf.put_u32(secs);
                }
            }
        }
    }

    // Injected: subscription_identifier (0x0B)
    if let Some(sub_id) = subscription_id {
        buf.put_u8(0x0B);
        write_variable_int(sub_id, buf);
    }

    // Injected: topic_alias (0x23)
    if let Some(alias) = topic_alias {
        buf.put_u8(0x23);
        buf.put_u16(alias);
    }
}

/// Single-pass copy of raw MQTT property bytes, skipping properties in FILTERED_PROP_IDS.
fn copy_properties_filtered(raw: &[u8], buf: &mut BytesMut) {
    let mut pos = 0;
    while pos < raw.len() {
        let id = raw[pos];
        let start = pos;
        pos += 1;
        if let Some(next) = bisque_mq::flat::skip_mqtt_property(raw, pos, id) {
            if !FILTERED_PROP_IDS.contains(&id) {
                buf.extend_from_slice(&raw[start..next]);
            }
            pos = next;
        } else {
            break;
        }
    }
}

pub fn encode_connect(connect: &Connect, buf: &mut BytesMut) {
    let connect_props = connect.properties();
    // Pre-compute remaining length to avoid intermediate BytesMut.
    let mut remaining = 0usize;

    // Variable header: protocol name + level + flags + keep_alive
    remaining += 2 + connect.protocol_name.len() + 1 + 1 + 2;

    // MQTT 5.0 connect properties
    let connect_props_size = if connect.protocol_version == ProtocolVersion::V5 {
        properties_wire_size(&connect_props)
    } else {
        0
    };
    remaining += connect_props_size;

    // Payload: client_id
    remaining += 2 + connect.client_id.len();

    // Will
    if let Some(ref will) = connect.will {
        if connect.protocol_version == ProtocolVersion::V5 {
            let will_props = will.properties();
            remaining += properties_wire_size(&will_props);
        }
        remaining += 2 + will.topic.len();
        remaining += 2 + will.payload.len();
    }

    // Username / password
    if let Some(ref username) = connect.username {
        remaining += 2 + username.len();
    }
    if let Some(ref password) = connect.password {
        remaining += 2 + password.len();
    }

    buf.reserve(1 + 4 + remaining);

    // Fixed header: CONNECT = 0x10
    buf.put_u8(0x10);
    encode_remaining_length(remaining, buf);

    // Variable header
    write_mqtt_bytes(&connect.protocol_name, buf);
    buf.put_u8(connect.protocol_version.level());
    buf.put_u8(connect.flags.to_byte());
    buf.put_u16(connect.keep_alive);

    if connect.protocol_version == ProtocolVersion::V5 {
        write_properties(&connect_props, buf);
    }

    // Payload
    write_mqtt_bytes(&connect.client_id, buf);

    if let Some(ref will) = connect.will {
        if connect.protocol_version == ProtocolVersion::V5 {
            let will_props = will.properties();
            write_properties(&will_props, buf);
        }
        write_mqtt_bytes(&will.topic, buf);
        write_mqtt_bytes(&will.payload, buf);
    }

    if let Some(ref username) = connect.username {
        write_mqtt_bytes(username, buf);
    }

    if let Some(ref password) = connect.password {
        write_mqtt_bytes(password, buf);
    }
}

pub fn encode_connack(connack: &ConnAck, buf: &mut BytesMut) {
    encode_connack_v(connack, buf, false)
}

pub(crate) fn encode_connack_v(connack: &ConnAck, buf: &mut BytesMut, is_v5: bool) {
    let props_content = connack.properties_size();
    let has_props = props_content > 0;
    let include_props = is_v5 || has_props;
    let remaining = 2 + if include_props {
        props_wire_size(props_content)
    } else {
        0
    };

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0x20); // CONNACK = 2 << 4
    encode_remaining_length(remaining, buf);
    buf.put_u8(if connack.session_present { 0x01 } else { 0x00 });
    buf.put_u8(connack.return_code);

    if include_props {
        write_variable_int(props_content as u32, buf);
        if let Some(v) = connack.session_expiry_interval {
            write_prop_u32(buf, 0x11, v);
        }
        if let Some(v) = connack.receive_maximum {
            write_prop_u16(buf, 0x21, v);
        }
        if let Some(v) = connack.maximum_qos {
            write_prop_u8(buf, 0x24, v);
        }
        if let Some(v) = connack.retain_available {
            write_prop_u8(buf, 0x25, v as u8);
        }
        if let Some(v) = connack.maximum_packet_size {
            write_prop_u32(buf, 0x27, v);
        }
        if let Some(ref v) = connack.assigned_client_identifier {
            write_prop_bytes(buf, 0x12, v);
        }
        if let Some(v) = connack.topic_alias_maximum {
            write_prop_u16(buf, 0x22, v);
        }
        if let Some(ref v) = connack.reason_string {
            write_prop_bytes(buf, 0x1F, v);
        }
        if let Some(ref v) = connack.response_information {
            write_prop_bytes(buf, 0x1A, v);
        }
        if let Some(v) = connack.wildcard_subscription_available {
            write_prop_u8(buf, 0x28, v as u8);
        }
        if let Some(v) = connack.subscription_identifier_available {
            write_prop_u8(buf, 0x29, v as u8);
        }
        if let Some(v) = connack.shared_subscription_available {
            write_prop_u8(buf, 0x2A, v as u8);
        }
        if let Some(v) = connack.server_keep_alive {
            write_prop_u16(buf, 0x13, v);
        }
        if let Some(ref v) = connack.authentication_method {
            write_prop_bytes(buf, 0x15, v);
        }
        if let Some(ref v) = connack.authentication_data {
            write_prop_bytes(buf, 0x16, v);
        }
        if let Some(ref v) = connack.server_reference {
            write_prop_bytes(buf, 0x1C, v);
        }
    }
}

pub fn encode_publish(publish: &Publish<'_>, buf: &mut BytesMut) {
    encode_publish_v(publish, buf, false)
}

pub fn encode_publish_v(publish: &Publish<'_>, buf: &mut BytesMut, is_v5: bool) {
    // Pre-compute remaining length to avoid intermediate BytesMut allocation.
    let topic_len = publish.topic.len();
    let props_total_size = if is_v5 {
        properties_wire_size(&publish.properties)
    } else {
        0
    };
    let var_header_size =
        2 + topic_len + if publish.qos != QoS::AtMostOnce { 2 } else { 0 } + props_total_size;
    let remaining = var_header_size + publish.payload.len();

    buf.reserve(1 + 4 + remaining); // 1 fixed header + up to 4 remaining length bytes + payload

    // Fixed header
    let mut first_byte = 0x30u8; // PUBLISH = 3 << 4
    if publish.dup {
        first_byte |= 0x08;
    }
    first_byte |= (publish.qos.as_u8() & 0x03) << 1;
    if publish.retain {
        first_byte |= 0x01;
    }
    buf.put_u8(first_byte);
    encode_remaining_length(remaining, buf);

    // Topic (write as MQTT string: 2-byte length + raw bytes)
    buf.put_u16(topic_len as u16);
    buf.extend_from_slice(publish.topic);

    // Packet ID
    if publish.qos != QoS::AtMostOnce {
        if let Some(packet_id) = publish.packet_id {
            buf.put_u16(packet_id);
        }
    }

    // MQTT 5.0 properties
    if is_v5 {
        write_properties(&publish.properties, buf);
    }

    // Payload
    buf.extend_from_slice(publish.payload);
}

pub fn encode_puback(puback: &PubAck<'_>, buf: &mut BytesMut) {
    encode_puback_v(puback, buf, false)
}

pub fn encode_puback_v(puback: &PubAck<'_>, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x40,
        puback.packet_id,
        puback.reason_code,
        puback.reason_string,
        buf,
        is_v5,
    );
}

pub fn encode_pubrec(pubrec: &PubRec<'_>, buf: &mut BytesMut) {
    encode_pubrec_v(pubrec, buf, false)
}

pub fn encode_pubrec_v(pubrec: &PubRec<'_>, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x50,
        pubrec.packet_id,
        pubrec.reason_code,
        pubrec.reason_string,
        buf,
        is_v5,
    );
}

pub fn encode_pubrel(pubrel: &PubRel<'_>, buf: &mut BytesMut) {
    encode_pubrel_v(pubrel, buf, false)
}

pub(crate) fn encode_pubrel_v(pubrel: &PubRel<'_>, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x62,
        pubrel.packet_id,
        pubrel.reason_code,
        pubrel.reason_string,
        buf,
        is_v5,
    );
}

pub fn encode_pubcomp(pubcomp: &PubComp<'_>, buf: &mut BytesMut) {
    encode_pubcomp_v(pubcomp, buf, false)
}

pub(crate) fn encode_pubcomp_v(pubcomp: &PubComp<'_>, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x70,
        pubcomp.packet_id,
        pubcomp.reason_code,
        pubcomp.reason_string,
        buf,
        is_v5,
    );
}

/// Common encoder for PUBACK/PUBREC/PUBREL/PUBCOMP.
/// For V3.1.1 (is_v5=false): writes only packet_id (2 bytes).
/// For V5: writes packet_id + optional reason_code + optional properties.
/// Per MQTT 5.0 spec: if reason_code is 0x00 and no properties, can omit reason_code.
fn encode_pub_ack_common(
    first_byte: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    reason_string: Option<&[u8]>,
    buf: &mut BytesMut,
    is_v5: bool,
) {
    if !is_v5 {
        buf.put_u8(first_byte);
        encode_remaining_length(2, buf);
        buf.put_u16(packet_id);
        return;
    }

    let rc = reason_code.unwrap_or(0x00);
    let props_content_size = reason_string.map_or(0, |s| prop_size_str(s.len()));
    let has_props = props_content_size > 0;

    if rc == 0x00 && !has_props {
        buf.put_u8(first_byte);
        encode_remaining_length(2, buf);
        buf.put_u16(packet_id);
    } else if !has_props {
        buf.put_u8(first_byte);
        encode_remaining_length(3, buf);
        buf.put_u16(packet_id);
        buf.put_u8(rc);
    } else {
        let remaining = 2 + 1 + props_wire_size(props_content_size);
        buf.put_u8(first_byte);
        encode_remaining_length(remaining, buf);
        buf.put_u16(packet_id);
        buf.put_u8(rc);
        write_variable_int(props_content_size as u32, buf);
        if let Some(s) = reason_string {
            write_prop_bytes(buf, 0x1F, s);
        }
    }
}

/// Encode PUBACK directly from scalars — no PubAck struct needed.
#[inline]
pub(crate) fn encode_puback_raw(
    packet_id: u16,
    reason_code: Option<u8>,
    buf: &mut BytesMut,
    is_v5: bool,
) {
    encode_pub_ack_common(0x40, packet_id, reason_code, None, buf, is_v5);
}

/// Encode PUBREC directly from scalars — no PubRec struct needed.
#[inline]
pub(crate) fn encode_pubrec_raw(
    packet_id: u16,
    reason_code: Option<u8>,
    buf: &mut BytesMut,
    is_v5: bool,
) {
    encode_pub_ack_common(0x50, packet_id, reason_code, None, buf, is_v5);
}

/// Encode DISCONNECT from scalars — no Disconnect struct needed.
/// For V5 only (V3.1.1 has no server-initiated DISCONNECT).
#[inline]
pub(crate) fn encode_disconnect_reason(
    reason_code: u8,
    reason_string: Option<&[u8]>,
    buf: &mut BytesMut,
) {
    let props_content = reason_string.map_or(0, |s| prop_size_str(s.len()));
    let has_props = props_content > 0;
    let remaining = 1 + if has_props {
        props_wire_size(props_content)
    } else {
        0
    };

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0xE0);
    encode_remaining_length(remaining, buf);
    buf.put_u8(reason_code);
    if has_props {
        write_variable_int(props_content as u32, buf);
        if let Some(s) = reason_string {
            write_prop_bytes(buf, 0x1F, s);
        }
    }
}

/// Encode SUBACK directly from scalars — no SubAck struct needed.
#[inline]
pub(crate) fn encode_suback_raw(
    packet_id: u16,
    return_codes: &[u8],
    buf: &mut BytesMut,
    is_v5: bool,
) {
    let props_total = if is_v5 { props_wire_size(0) } else { 0 };
    let remaining = 2 + props_total + return_codes.len();
    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0x90);
    encode_remaining_length(remaining, buf);
    buf.put_u16(packet_id);
    if is_v5 {
        write_variable_int(0, buf); // empty properties
    }
    buf.extend_from_slice(return_codes);
}

/// Read just the packet_id from an ack packet (PubAck/PubRec/PubRel/PubComp).
/// These all start with packet_id at position `header_size`.
#[inline]
pub(crate) fn read_ack_packet_id(buf: &[u8], header_size: usize) -> Result<u16, CodecError> {
    if buf.len() < header_size + 2 {
        return Err(CodecError::UnexpectedEof);
    }
    Ok(u16::from_be_bytes([buf[header_size], buf[header_size + 1]]))
}

/// Read reason_code and session_expiry_interval from a DISCONNECT packet.
/// Returns (reason_code, session_expiry_interval).
#[inline]
pub(crate) fn read_disconnect_fields(
    buf: &[u8],
    header_size: usize,
    remaining_length: usize,
) -> Result<(Option<u8>, Option<u32>), CodecError> {
    if remaining_length == 0 {
        return Ok((None, None));
    }
    let mut pos = header_size;
    let end = header_size + remaining_length;
    let reason_code = Some(read_u8_zc(buf, &mut pos)?);
    let session_expiry_interval = if pos < end {
        let props = read_properties_zc(buf, &mut pos)?;
        props.session_expiry_interval()
    } else {
        None
    };
    Ok((reason_code, session_expiry_interval))
}

/// Decode a Subscribe packet from raw bytes (pub(crate) version returning Subscribe struct).
#[inline]
pub(crate) fn decode_subscribe_raw(
    buf: &[u8],
    header_size: usize,
    total_size: usize,
    is_v5: bool,
) -> Result<Subscribe, CodecError> {
    let mut pos = header_size;
    let end = total_size;
    if is_v5 {
        decode_subscribe_v(buf, &mut pos, end, true)
    } else {
        decode_subscribe(buf, &mut pos, end)
    }
}

/// Decode an Unsubscribe packet from raw bytes.
#[inline]
pub(crate) fn decode_unsubscribe_raw(
    buf: &[u8],
    header_size: usize,
    total_size: usize,
    is_v5: bool,
) -> Result<Unsubscribe, CodecError> {
    let mut pos = header_size;
    let end = total_size;
    if is_v5 {
        decode_unsubscribe_v(buf, &mut pos, end, true)
    } else {
        decode_unsubscribe(buf, &mut pos, end)
    }
}

/// Decode an Auth packet from raw bytes.
#[inline]
pub(crate) fn decode_auth_raw(
    buf: &[u8],
    header_size: usize,
    total_size: usize,
    remaining_length: usize,
) -> Result<Auth, CodecError> {
    let mut pos = header_size;
    let end = total_size;
    decode_auth(buf, &mut pos, end, remaining_length)
}

pub fn encode_subscribe(subscribe: &Subscribe, buf: &mut BytesMut) {
    encode_subscribe_v(subscribe, buf, false)
}

pub fn encode_subscribe_v(subscribe: &Subscribe, buf: &mut BytesMut, is_v5: bool) {
    let props_content = if is_v5 {
        subscribe
            .subscription_identifier
            .map_or(0, |v| prop_size_varint(v))
    } else {
        0
    };
    let props_total = if props_content > 0 || is_v5 {
        props_wire_size(props_content)
    } else {
        0
    };
    let mut remaining = 2usize + props_total;
    for filter in &subscribe.filters {
        remaining += 2 + filter.filter.len() + 1;
    }

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0x82); // SUBSCRIBE = 8 << 4 | 0x02
    encode_remaining_length(remaining, buf);
    buf.put_u16(subscribe.packet_id);

    if is_v5 {
        write_variable_int(props_content as u32, buf);
        if let Some(v) = subscribe.subscription_identifier {
            write_prop_varint(buf, 0x0B, v);
        }
    }

    for filter in &subscribe.filters {
        write_mqtt_bytes(&filter.filter, buf);
        let mut options: u8 = filter.qos.as_u8() & 0x03;
        if filter.no_local {
            options |= 0x04;
        }
        if filter.retain_as_published {
            options |= 0x08;
        }
        options |= (filter.retain_handling & 0x03) << 4;
        buf.put_u8(options);
    }
}

pub fn encode_suback(suback: &SubAck, buf: &mut BytesMut) {
    encode_suback_v(suback, buf, false)
}

pub fn encode_suback_v(suback: &SubAck, buf: &mut BytesMut, is_v5: bool) {
    let props_content = if is_v5 {
        suback
            .reason_string
            .as_ref()
            .map_or(0, |s| prop_size_str(s.len()))
    } else {
        0
    };
    let props_total = if is_v5 {
        props_wire_size(props_content)
    } else {
        0
    };
    let remaining = 2 + props_total + suback.return_codes.len();

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0x90); // SUBACK = 9 << 4
    encode_remaining_length(remaining, buf);
    buf.put_u16(suback.packet_id);
    if is_v5 {
        write_variable_int(props_content as u32, buf);
        if let Some(ref s) = suback.reason_string {
            write_prop_bytes(buf, 0x1F, s);
        }
    }
    buf.extend_from_slice(&suback.return_codes);
}

pub fn encode_unsubscribe(unsubscribe: &Unsubscribe, buf: &mut BytesMut) {
    encode_unsubscribe_v(unsubscribe, buf, false)
}

pub fn encode_unsubscribe_v(unsubscribe: &Unsubscribe, buf: &mut BytesMut, is_v5: bool) {
    let props_total = if is_v5 { props_wire_size(0) } else { 0 }; // empty properties
    let mut remaining = 2usize + props_total;
    for filter in &unsubscribe.filters {
        remaining += 2 + filter.len();
    }

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0xA2); // UNSUBSCRIBE = 10 << 4 | 0x02
    encode_remaining_length(remaining, buf);
    buf.put_u16(unsubscribe.packet_id);
    if is_v5 {
        write_variable_int(0, buf); // empty properties
    }
    for filter in &unsubscribe.filters {
        write_mqtt_bytes(filter, buf);
    }
}

pub fn encode_unsuback(unsuback: &UnsubAck, buf: &mut BytesMut) {
    encode_unsuback_v(unsuback, buf, false)
}

pub(crate) fn encode_unsuback_v(unsuback: &UnsubAck, buf: &mut BytesMut, is_v5: bool) {
    let props_content = if is_v5 {
        unsuback
            .reason_string
            .as_ref()
            .map_or(0, |s| prop_size_str(s.len()))
    } else {
        0
    };
    let props_total = if is_v5 {
        props_wire_size(props_content)
    } else {
        0
    };
    let remaining = 2 + props_total + unsuback.reason_codes.len();

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0xB0); // UNSUBACK = 11 << 4
    encode_remaining_length(remaining, buf);
    buf.put_u16(unsuback.packet_id);
    if is_v5 {
        write_variable_int(props_content as u32, buf);
        if let Some(ref s) = unsuback.reason_string {
            write_prop_bytes(buf, 0x1F, s);
        }
    }
    buf.extend_from_slice(&unsuback.reason_codes);
}

pub fn encode_ping_req(buf: &mut BytesMut) {
    buf.put_u8(0xC0); // PINGREQ = 12 << 4
    buf.put_u8(0x00);
}

pub(crate) fn encode_ping_resp(buf: &mut BytesMut) {
    buf.put_u8(0xD0); // PINGRESP = 13 << 4
    buf.put_u8(0x00);
}

pub(crate) fn encode_disconnect(disconnect: &Disconnect, buf: &mut BytesMut) {
    if disconnect.reason_code.is_none() {
        // MQTT 3.1.1 DISCONNECT: no variable header.
        buf.put_u8(0xE0);
        buf.put_u8(0x00);
    } else {
        // MQTT 5.0: reason_code(1) + optional properties.
        let mut props_content = 0usize;
        if disconnect.session_expiry_interval.is_some() {
            props_content += PROP_SIZE_U32;
        }
        if let Some(ref s) = disconnect.reason_string {
            props_content += prop_size_str(s.len());
        }
        if let Some(ref s) = disconnect.server_reference {
            props_content += prop_size_str(s.len());
        }
        let has_props = props_content > 0;
        let remaining = 1 + if has_props {
            props_wire_size(props_content)
        } else {
            0
        };

        buf.reserve(1 + 4 + remaining);
        buf.put_u8(0xE0);
        encode_remaining_length(remaining, buf);
        buf.put_u8(disconnect.reason_code.unwrap());
        if has_props {
            write_variable_int(props_content as u32, buf);
            if let Some(v) = disconnect.session_expiry_interval {
                write_prop_u32(buf, 0x11, v);
            }
            if let Some(ref s) = disconnect.reason_string {
                write_prop_bytes(buf, 0x1F, s);
            }
            if let Some(ref s) = disconnect.server_reference {
                write_prop_bytes(buf, 0x1C, s);
            }
        }
    }
}

/// Encode an AUTH packet (MQTT 5.0 only).
pub(crate) fn encode_auth(auth: &Auth, buf: &mut BytesMut) {
    let mut props_content = 0usize;
    if let Some(ref v) = auth.authentication_method {
        props_content += prop_size_str(v.len());
    }
    if let Some(ref v) = auth.authentication_data {
        props_content += prop_size_str(v.len());
    }
    if let Some(ref s) = auth.reason_string {
        props_content += prop_size_str(s.len());
    }
    let has_props = props_content > 0;

    if auth.reason_code == Auth::SUCCESS && !has_props {
        buf.put_u8(0xF0);
        buf.put_u8(0x00);
    } else {
        let remaining = 1 + if has_props {
            props_wire_size(props_content)
        } else {
            0
        };

        buf.reserve(1 + 4 + remaining);
        buf.put_u8(0xF0);
        encode_remaining_length(remaining, buf);
        buf.put_u8(auth.reason_code);
        if has_props {
            write_variable_int(props_content as u32, buf);
            if let Some(ref v) = auth.authentication_method {
                write_prop_bytes(buf, 0x15, v);
            }
            if let Some(ref v) = auth.authentication_data {
                write_prop_bytes(buf, 0x16, v);
            }
            if let Some(ref s) = auth.reason_string {
                write_prop_bytes(buf, 0x1F, s);
            }
        }
    }
}

// =============================================================================
// Per-type decode from wire bytes (test + benchmark helpers)
// =============================================================================

/// Decode a specific packet type from wire bytes.
///
/// Each function parses the fixed header, validates it, then calls the
/// per-type decoder. Returns `(decoded_struct, total_bytes_consumed)`.
pub mod decode_buf {
    use super::*;

    /// Decode a ConnAck from wire bytes.
    pub fn connack(buf: &[u8]) -> Result<(ConnAck, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        validate_fixed_header_flags(2, 0)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_connack(buf, &mut pos, total)?, total))
    }

    /// Decode a PubAck from wire bytes.
    pub fn puback(buf: &[u8]) -> Result<(PubAck<'_>, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_puback(buf, &mut pos, total)?, total))
    }

    /// Decode a PubRec from wire bytes.
    pub fn pubrec(buf: &[u8]) -> Result<(PubRec<'_>, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_pubrec(buf, &mut pos, total)?, total))
    }

    /// Decode a PubRel from wire bytes.
    pub fn pubrel(buf: &[u8]) -> Result<(PubRel<'_>, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_pubrel(buf, &mut pos, total)?, total))
    }

    /// Decode a PubComp from wire bytes.
    pub fn pubcomp(buf: &[u8]) -> Result<(PubComp<'_>, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_pubcomp(buf, &mut pos, total)?, total))
    }

    /// Decode a SubAck from wire bytes.
    pub fn suback(buf: &[u8]) -> Result<(SubAck, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_suback(buf, &mut pos, total)?, total))
    }

    /// Decode a SubAck from wire bytes (V5).
    pub fn suback_v5(buf: &[u8]) -> Result<(SubAck, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_suback_v(buf, &mut pos, total, true)?, total))
    }

    /// Decode an UnsubAck from wire bytes.
    pub fn unsuback(buf: &[u8]) -> Result<(UnsubAck, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_unsuback(buf, &mut pos, total)?, total))
    }

    /// Decode an UnsubAck from wire bytes (V5).
    pub fn unsuback_v5(buf: &[u8]) -> Result<(UnsubAck, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_unsuback_v(buf, &mut pos, total, true)?, total))
    }

    /// Decode a Disconnect from wire bytes.
    pub fn disconnect(buf: &[u8]) -> Result<(Disconnect, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((
            decode_disconnect(buf, &mut pos, total, remaining_length)?,
            total,
        ))
    }

    /// Decode a Connect from wire bytes.
    pub fn connect(buf: &[u8]) -> Result<(Connect, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_connect(buf, &mut pos)?, total))
    }

    /// Decode a Publish from wire bytes (borrows topic/payload).
    pub fn publish(buf: &[u8]) -> Result<(Publish<'_>, usize), CodecError> {
        let (_, flags, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        Ok((
            decode_publish_from_slice(buf, header_size, total, flags, false)?,
            total,
        ))
    }

    /// Decode a Publish from wire bytes with V5 properties.
    pub fn publish_v5(buf: &[u8]) -> Result<(Publish<'_>, usize), CodecError> {
        let (_, flags, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        Ok((
            decode_publish_from_slice(buf, header_size, total, flags, true)?,
            total,
        ))
    }

    /// Decode a Subscribe from wire bytes.
    pub fn subscribe(buf: &[u8]) -> Result<(Subscribe, usize), CodecError> {
        let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;
        validate_fixed_header_flags(type_nibble, flags)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_subscribe(buf, &mut pos, total)?, total))
    }

    /// Decode a Subscribe from wire bytes (V5).
    pub fn subscribe_v5(buf: &[u8]) -> Result<(Subscribe, usize), CodecError> {
        let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;
        validate_fixed_header_flags(type_nibble, flags)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_subscribe_v(buf, &mut pos, total, true)?, total))
    }

    /// Decode an Unsubscribe from wire bytes.
    pub fn unsubscribe(buf: &[u8]) -> Result<(Unsubscribe, usize), CodecError> {
        let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;
        validate_fixed_header_flags(type_nibble, flags)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_unsubscribe(buf, &mut pos, total)?, total))
    }

    /// Decode an Unsubscribe from wire bytes (V5).
    pub fn unsubscribe_v5(buf: &[u8]) -> Result<(Unsubscribe, usize), CodecError> {
        let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;
        validate_fixed_header_flags(type_nibble, flags)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_unsubscribe_v(buf, &mut pos, total, true)?, total))
    }

    /// Decode an Auth from wire bytes.
    pub fn auth(buf: &[u8]) -> Result<(Auth, usize), CodecError> {
        let (_, _, remaining_length, header_size) = parse_fixed_header(buf)?;
        let total = header_size + remaining_length;
        if buf.len() < total {
            return Err(CodecError::Incomplete);
        }
        let mut pos = header_size;
        Ok((decode_auth(buf, &mut pos, total, remaining_length)?, total))
    }

    /// Return the packet type nibble from wire bytes (1=CONNECT, 2=CONNACK, etc.)
    /// Also validates fixed header flags per MQTT spec.
    pub fn packet_type(buf: &[u8]) -> Result<u8, CodecError> {
        let (type_nibble, flags, _, _) = parse_fixed_header(buf)?;
        validate_fixed_header_flags(type_nibble, flags)?;
        Ok(type_nibble)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PropertiesBuilder;
    use smallvec::smallvec;

    #[test]
    fn test_remaining_length_single_byte() {
        assert_eq!(decode_remaining_length(&[0x00]).unwrap(), (0, 1));
        assert_eq!(decode_remaining_length(&[0x7F]).unwrap(), (127, 1));
    }

    #[test]
    fn test_remaining_length_two_bytes() {
        // 128 = 0x00 | 0x80, 0x01
        assert_eq!(decode_remaining_length(&[0x80, 0x01]).unwrap(), (128, 2));
        // 16383 = 0xFF, 0x7F
        assert_eq!(decode_remaining_length(&[0xFF, 0x7F]).unwrap(), (16383, 2));
    }

    #[test]
    fn test_remaining_length_four_bytes() {
        // 2,097,152 = 0x80, 0x80, 0x80, 0x01
        assert_eq!(
            decode_remaining_length(&[0x80, 0x80, 0x80, 0x01]).unwrap(),
            (2_097_152, 4)
        );
    }

    #[test]
    fn test_remaining_length_incomplete() {
        assert!(matches!(
            decode_remaining_length(&[0x80]),
            Err(CodecError::Incomplete)
        ));
    }

    #[test]
    fn test_remaining_length_roundtrip() {
        let test_values = [0, 1, 127, 128, 16383, 16384, 2_097_151, 2_097_152];
        for &val in &test_values {
            let mut buf = BytesMut::new();
            encode_remaining_length(val, &mut buf);
            let (decoded, _) = decode_remaining_length(&buf).unwrap();
            assert_eq!(decoded, val, "failed roundtrip for {}", val);
        }
    }

    #[test]
    fn test_encode_decode_pingreq() {
        let mut buf = BytesMut::new();
        encode_ping_req(&mut buf);
        assert_eq!(&buf[..], &[0xC0, 0x00]);

        assert_eq!(decode_buf::packet_type(&buf).unwrap(), 12);
    }

    #[test]
    fn test_encode_decode_pingresp() {
        let mut buf = BytesMut::new();
        encode_ping_resp(&mut buf);
        assert_eq!(&buf[..], &[0xD0, 0x00]);

        assert_eq!(decode_buf::packet_type(&buf).unwrap(), 13);
    }

    #[test]
    fn test_encode_decode_connack() {
        let connack = ConnAck {
            session_present: false,
            return_code: 0x00,
            ..ConnAck::default()
        };

        let mut buf = BytesMut::new();
        encode_connack(&connack, &mut buf);

        let (c, consumed) = decode_buf::connack(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert!(!c.session_present);
        assert_eq!(c.return_code, 0x00);
    }

    #[test]
    fn test_encode_decode_publish_qos0() {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: None,
            payload: b"hello",
            properties: Properties::default(),
        };

        let mut buf = BytesMut::new();
        encode_publish(&publish, &mut buf);

        let (p, consumed) = decode_buf::publish(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert!(!p.dup);
        assert_eq!(p.qos, QoS::AtMostOnce);
        assert!(!p.retain);
        assert_eq!(p.topic, b"test/topic");
        assert!(p.packet_id.is_none());
        assert_eq!(p.payload, b"hello");
    }

    #[test]
    fn test_encode_decode_publish_qos1() {
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: b"sensor/1/temp",
            packet_id: Some(42),
            payload: b"22.5",
            properties: Properties::default(),
        };

        let mut buf = BytesMut::new();
        encode_publish(&publish, &mut buf);

        let (p, _) = decode_buf::publish(&buf).unwrap();
        assert_eq!(p.qos, QoS::AtLeastOnce);
        assert!(p.retain);
        assert_eq!(p.topic, b"sensor/1/temp");
        assert_eq!(p.packet_id, Some(42));
        assert_eq!(p.payload, b"22.5");
    }

    #[test]
    fn test_decode_publish_zero_copy() {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"zero/copy/topic",
            packet_id: None,
            payload: b"payload data",
            properties: Properties::default(),
        };

        let mut buf = BytesMut::new();
        encode_publish(&publish, &mut buf);

        let (p, consumed) = decode_buf::publish(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(&p.topic[..], b"zero/copy/topic");
        assert_eq!(&p.payload[..], b"payload data");
    }

    #[test]
    fn test_encode_publish_from_flat_no_v5() {
        use bisque_mq::flat::FlatMessageBuilder;

        let flat_bytes = FlatMessageBuilder::new(b"sensor reading")
            .routing_key(b"sensor/1/temp")
            .build();

        let flat = FlatMessage::new(&flat_bytes).unwrap();
        let mut buf = BytesMut::new();

        // Encode without V5 properties.
        encode_publish_from_flat(
            &flat,
            QoS::AtLeastOnce,
            false,
            false,
            Some(42),
            false,
            None,
            None,
            &mut buf,
        );

        // Decode and verify topic, QoS, packet_id, payload.
        let (p, _) = decode_buf::publish(&buf).unwrap();
        assert_eq!(&p.topic[..], b"sensor/1/temp");
        assert_eq!(p.qos, QoS::AtLeastOnce);
        assert_eq!(p.packet_id, Some(42));
        assert_eq!(&p.payload[..], b"sensor reading");
    }

    #[test]
    fn test_encode_publish_from_flat_v5_properties() {
        use bisque_mq::flat::FlatMessageBuilder;

        let flat_bytes = FlatMessageBuilder::new(b"data")
            .routing_key(b"t")
            .reply_to(b"reply/topic")
            .correlation_id(b"corr-123")
            .ttl_ms(60_000)
            .header(b"mqtt.content_type", b"application/json")
            .header(b"mqtt.payload_format", b"\x01")
            .header(b"mqtt.user.custom", b"value")
            .build();

        let flat = FlatMessage::new(&flat_bytes).unwrap();
        let mut buf = BytesMut::new();

        encode_publish_from_flat(
            &flat,
            QoS::AtMostOnce,
            false,
            false,
            None,
            true,
            Some(7),
            Some((3, true)), // new alias
            &mut buf,
        );

        // Verify the encoded packet is valid (can be parsed by fixed header).
        let (_, _, remaining_length, header_size) = parse_fixed_header(&buf).unwrap();
        assert_eq!(buf.len(), header_size + remaining_length);

        // Verify the topic is at the right position (new alias: full topic sent).
        // Fixed header = first_byte + remaining_length_bytes
        let topic_len = u16::from_be_bytes([buf[header_size], buf[header_size + 1]]) as usize;
        assert_eq!(topic_len, 1); // "t"
        assert_eq!(buf[header_size + 2], b't');

        // The payload "data" should be at the end.
        assert_eq!(&buf[buf.len() - 4..], b"data");
    }

    #[test]
    fn test_encode_decode_subscribe() {
        let subscribe = Subscribe {
            packet_id: 1,
            filters: smallvec![
                TopicFilter {
                    filter: Bytes::from_static(b"sensor/+/data"),
                    qos: QoS::AtLeastOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: 0,
                },
                TopicFilter {
                    filter: Bytes::from_static(b"control/#"),
                    qos: QoS::ExactlyOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: 0,
                },
            ],
            subscription_identifier: None,
        };

        let mut buf = BytesMut::new();
        encode_subscribe(&subscribe, &mut buf);

        let (s, _) = decode_buf::subscribe(&buf).unwrap();
        assert_eq!(s.packet_id, 1);
        assert_eq!(s.filters.len(), 2);
        assert_eq!(&s.filters[0].filter[..], b"sensor/+/data");
        assert_eq!(s.filters[0].qos, QoS::AtLeastOnce);
        assert_eq!(&s.filters[1].filter[..], b"control/#");
        assert_eq!(s.filters[1].qos, QoS::ExactlyOnce);
    }

    #[test]
    fn test_encode_decode_suback() {
        let suback = SubAck {
            packet_id: 1,
            return_codes: smallvec![0x01, 0x02, 0x80],
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_suback(&suback, &mut buf);

        let (s, _) = decode_buf::suback(&buf).unwrap();
        assert_eq!(s.packet_id, 1);
        assert_eq!(s.return_codes.as_slice(), &[0x01, 0x02, 0x80]);
    }

    #[test]
    fn test_encode_decode_unsubscribe() {
        let unsub = Unsubscribe {
            packet_id: 5,
            filters: smallvec![
                Bytes::from_static(b"sensor/+/data"),
                Bytes::from_static(b"control/#")
            ],
        };

        let mut buf = BytesMut::new();
        encode_unsubscribe(&unsub, &mut buf);

        let (u, _) = decode_buf::unsubscribe(&buf).unwrap();
        assert_eq!(u.packet_id, 5);
        assert_eq!(u.filters.len(), 2);
        assert_eq!(&u.filters[0][..], b"sensor/+/data");
    }

    #[test]
    fn test_encode_decode_disconnect() {
        // MQTT 3.1.1 style (no payload)
        let disconnect = Disconnect {
            reason_code: None,
            reason_string: None,
            session_expiry_interval: None,
            server_reference: None,
        };

        let mut buf = BytesMut::new();
        encode_disconnect(&disconnect, &mut buf);
        assert_eq!(&buf[..], &[0xE0, 0x00]);

        let (d, _) = decode_buf::disconnect(&buf).unwrap();
        assert!(d.reason_code.is_none());
    }

    #[test]
    fn test_encode_decode_connect_v311() {
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: Bytes::from_static(b"test-client"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };

        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);

        let (c, consumed) = decode_buf::connect(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(&c.protocol_name[..], b"MQTT");
        assert_eq!(c.protocol_version, ProtocolVersion::V311);
        assert!(c.flags.clean_session);
        assert_eq!(c.keep_alive, 60);
        assert_eq!(&c.client_id[..], b"test-client");
        assert!(c.will.is_none());
        assert!(c.username.is_none());
        assert!(c.password.is_none());
    }

    #[test]
    fn test_encode_decode_connect_with_will() {
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: true,
                password: true,
                will_retain: true,
                will_qos: QoS::AtLeastOnce,
                will: true,
                clean_session: true,
            },
            keep_alive: 120,
            client_id: Bytes::from_static(b"will-client"),
            will: Some(WillMessage {
                topic: Bytes::from_static(b"last/will"),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties_raw: Vec::new(),
            }),
            username: Some(Bytes::from_static(b"user")),
            password: Some(Bytes::from_static(b"pass")),
            properties_raw: Vec::new(),
        };

        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);

        let (c, _) = decode_buf::connect(&buf).unwrap();
        assert_eq!(&c.client_id[..], b"will-client");
        assert!(c.flags.will);
        let will = c.will.unwrap();
        assert_eq!(&will.topic[..], b"last/will");
        assert_eq!(will.payload.as_ref(), b"offline");
        assert_eq!(&c.username.unwrap()[..], b"user");
        assert_eq!(&c.password.unwrap()[..], b"pass");
    }

    #[test]
    fn test_encode_decode_puback() {
        let puback = PubAck {
            packet_id: 100,
            reason_code: None,
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_puback(&puback, &mut buf);

        let (p, _) = decode_buf::puback(&buf).unwrap();
        assert_eq!(p.packet_id, 100);
    }

    #[test]
    fn test_incomplete_data() {
        // Just one byte - not enough for a full packet
        let buf = [0x30]; // PUBLISH type byte only
        assert!(matches!(
            decode_buf::publish(&buf),
            Err(CodecError::Incomplete)
        ));

        // Fixed header complete but payload missing
        let buf = [0x30, 0x05]; // PUBLISH with 5 bytes remaining, but no payload
        assert!(matches!(
            decode_buf::publish(&buf),
            Err(CodecError::Incomplete)
        ));
    }

    #[test]
    fn test_properties_roundtrip() {
        let props_raw = PropertiesBuilder::new()
            .message_expiry_interval(3600)
            .content_type("application/json")
            .response_topic("reply/to")
            .user_property("key1", "val1")
            .user_property("key2", "val2")
            .build();
        let props = Properties::from_raw(&props_raw);

        let mut buf = BytesMut::new();
        write_properties(&props, &mut buf);

        let frozen = buf.freeze();
        let mut pos = 0;
        let decoded = read_properties_zc(&frozen, &mut pos).unwrap();
        assert_eq!(decoded.message_expiry_interval(), Some(3600));
        assert_eq!(decoded.content_type(), Some("application/json"));
        assert_eq!(decoded.response_topic(), Some("reply/to"));
        let up: Vec<_> = decoded.user_properties().collect();
        assert_eq!(up.len(), 2);
        assert_eq!(up[0].0, "key1");
        assert_eq!(up[0].1, "val1");
    }

    // =========================================================================
    // V5 versioned roundtrip tests
    // =========================================================================

    #[test]
    fn test_v5_publish_with_properties_roundtrip() {
        let props_raw = PropertiesBuilder::new()
            .message_expiry_interval(300)
            .content_type("application/json")
            .response_topic("reply/topic")
            .correlation_data(b"corr-id-1")
            .payload_format_indicator(1)
            .build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"test/v5/topic",
            packet_id: Some(101),
            payload: b"v5 payload",
            properties: Properties::from_raw(&props_raw),
        };

        let mut buf = BytesMut::new();
        encode_publish_v(&publish, &mut buf, true);

        let (p, consumed) = decode_buf::publish_v5(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(&p.topic[..], b"test/v5/topic");
        assert_eq!(p.packet_id, Some(101));
        assert_eq!(&p.payload[..], b"v5 payload");
        assert_eq!(p.properties.message_expiry_interval(), Some(300));
        assert_eq!(p.properties.content_type(), Some("application/json"));
        assert_eq!(p.properties.response_topic(), Some("reply/topic"));
        assert_eq!(
            p.properties.correlation_data(),
            Some(b"corr-id-1".as_slice())
        );
        assert_eq!(p.properties.payload_format_indicator(), Some(1));
    }

    #[test]
    fn test_v5_publish_zero_copy_with_properties() {
        let props_raw = PropertiesBuilder::new()
            .topic_alias(5)
            .subscription_identifier(42)
            .build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: b"zc/topic",
            packet_id: None,
            payload: b"zc payload",
            properties: Properties::from_raw(&props_raw),
        };

        let mut buf = BytesMut::new();
        encode_publish_v(&publish, &mut buf, true);

        let (p, consumed) = decode_buf::publish_v5(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(&p.topic[..], b"zc/topic");
        assert!(p.retain);
        assert_eq!(&p.payload[..], b"zc payload");
        assert_eq!(p.properties.topic_alias(), Some(5));
        assert_eq!(p.properties.subscription_identifier(), Some(42));
    }

    #[test]
    fn test_v5_subscribe_with_subscription_identifier() {
        let subscribe = Subscribe {
            packet_id: 10,
            filters: smallvec![TopicFilter {
                filter: Bytes::from_static(b"sensor/#"),
                qos: QoS::AtLeastOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: 1,
            }],
            subscription_identifier: Some(99),
        };

        let mut buf = BytesMut::new();
        encode_subscribe_v(&subscribe, &mut buf, true);

        let (s, consumed) = decode_buf::subscribe_v5(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(s.packet_id, 10);
        assert_eq!(s.subscription_identifier, Some(99));
        assert_eq!(s.filters.len(), 1);
        assert_eq!(&s.filters[0].filter[..], b"sensor/#");
        assert_eq!(s.filters[0].qos, QoS::AtLeastOnce);
        assert!(s.filters[0].no_local);
        assert!(s.filters[0].retain_as_published);
        assert_eq!(s.filters[0].retain_handling, 1);
    }

    #[test]
    fn test_v5_suback_with_properties() {
        let suback = SubAck {
            packet_id: 20,
            return_codes: smallvec![0x00, 0x01, 0x02],
            reason_string: Some(Bytes::from_static(b"all good")),
        };

        let mut buf = BytesMut::new();
        encode_suback_v(&suback, &mut buf, true);

        let (s, consumed) = decode_buf::suback_v5(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(s.packet_id, 20);
        assert_eq!(s.return_codes.as_slice(), &[0x00, 0x01, 0x02]);
        assert_eq!(s.reason_string.as_deref(), Some(b"all good".as_ref()));
    }

    #[test]
    fn test_v5_unsubscribe_with_properties() {
        let unsub = Unsubscribe {
            packet_id: 30,
            filters: smallvec![
                Bytes::from_static(b"sensor/#"),
                Bytes::from_static(b"cmd/+")
            ],
        };

        let mut buf = BytesMut::new();
        encode_unsubscribe_v(&unsub, &mut buf, true);

        let (u, consumed) = decode_buf::unsubscribe_v5(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(u.packet_id, 30);
        assert_eq!(u.filters.len(), 2);
        assert_eq!(&u.filters[0][..], b"sensor/#");
        assert_eq!(&u.filters[1][..], b"cmd/+");
    }

    #[test]
    fn test_v5_unsuback_with_properties() {
        let unsuback = UnsubAck {
            packet_id: 40,
            reason_codes: smallvec![0x00, 0x11],
            reason_string: Some(Bytes::from_static(b"done")),
        };

        let mut buf = BytesMut::new();
        encode_unsuback_v(&unsuback, &mut buf, true);

        let (u, consumed) = decode_buf::unsuback_v5(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(u.packet_id, 40);
        assert_eq!(u.reason_codes.as_slice(), &[0x00, 0x11]);
        assert_eq!(u.reason_string.as_deref(), Some(b"done".as_ref()));
    }

    #[test]
    fn test_v5_connack_empty_properties_has_property_length() {
        let connack = ConnAck {
            session_present: false,
            return_code: 0x00,
            ..ConnAck::default()
        };

        let mut buf = BytesMut::new();
        encode_connack_v(&connack, &mut buf, true);

        // V5 CONNACK must include property length byte even if 0.
        // Fixed header: 0x20 + remaining_length + ack_flags(1) + return_code(1) + prop_len(1=0x00)
        // So remaining_length = 3.
        assert_eq!(buf[0], 0x20); // CONNACK type
        assert_eq!(buf[1], 3); // remaining length = 3
        assert_eq!(buf[2], 0x00); // ack_flags
        assert_eq!(buf[3], 0x00); // return_code
        assert_eq!(buf[4], 0x00); // property length = 0

        // Verify it decodes correctly.
        let (c, consumed) = decode_buf::connack(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert!(!c.session_present);
        assert_eq!(c.return_code, 0x00);
    }

    #[test]
    fn test_v5_puback_with_reason_code() {
        let puback = PubAck {
            packet_id: 200,
            reason_code: Some(0x10), // No matching subscribers
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_puback_v(&puback, &mut buf, true);

        let (p, consumed) = decode_buf::puback(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(p.packet_id, 200);
        assert_eq!(p.reason_code, Some(0x10));
    }

    #[test]
    fn test_v5_puback_success_no_properties_compact() {
        // When reason_code is 0x00 and no properties, V5 encoder can omit them.
        let puback = PubAck {
            packet_id: 300,
            reason_code: Some(0x00),
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_puback_v(&puback, &mut buf, true);

        // Should be compact: type(1) + remaining_len(1) + packet_id(2) = 4 bytes.
        assert_eq!(buf.len(), 4);

        let (p, _) = decode_buf::puback(&buf).unwrap();
        assert_eq!(p.packet_id, 300);
    }

    #[test]
    fn test_v5_puback_with_properties() {
        let puback = PubAck {
            packet_id: 400,
            reason_code: Some(0x97), // Quota exceeded
            reason_string: Some(b"quota exceeded"),
        };

        let mut buf = BytesMut::new();
        encode_puback_v(&puback, &mut buf, true);

        let (p, consumed) = decode_buf::puback(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(p.packet_id, 400);
        assert_eq!(p.reason_code, Some(0x97));
        assert_eq!(p.reason_string.as_deref(), Some(b"quota exceeded".as_ref()));
    }

    #[test]
    fn test_v5_disconnect_roundtrip() {
        let disconnect = Disconnect {
            reason_code: Some(0x8B), // Server shutting down
            session_expiry_interval: Some(0),
            reason_string: Some(Bytes::from_static(b"server shutting down")),
            server_reference: None,
        };

        let mut buf = BytesMut::new();
        encode_disconnect(&disconnect, &mut buf);

        let (d, consumed) = decode_buf::disconnect(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(d.reason_code, Some(0x8B));
        assert_eq!(
            d.reason_string.as_deref(),
            Some(b"server shutting down".as_ref())
        );
        assert_eq!(d.session_expiry_interval, Some(0));
    }

    #[test]
    fn test_v5_pubrel_pubcomp_roundtrip() {
        // PUBREL with reason code
        let pubrel = PubRel {
            packet_id: 500,
            reason_code: Some(0x92), // Packet Identifier not found
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_pubrel_v(&pubrel, &mut buf, true);

        let (p, _) = decode_buf::pubrel(&buf).unwrap();
        assert_eq!(p.packet_id, 500);
        assert_eq!(p.reason_code, Some(0x92));

        // PUBCOMP with success (compact)
        let pubcomp = PubComp {
            packet_id: 501,
            reason_code: Some(0x00),
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_pubcomp_v(&pubcomp, &mut buf, true);
        assert_eq!(buf.len(), 4); // compact form

        let (p, _) = decode_buf::pubcomp(&buf).unwrap();
        assert_eq!(p.packet_id, 501);
    }

    #[test]
    fn test_v311_backward_compat_through_versioned_api() {
        // Ensure V3.1.1 (is_v5=false) produces the same output as the non-versioned encode.
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"compat/test",
            packet_id: Some(77),
            payload: b"hello",
            properties: Properties::default(),
        };

        let mut buf_v311 = BytesMut::new();
        encode_publish_v(&publish, &mut buf_v311, false);

        let mut buf_legacy = BytesMut::new();
        encode_publish(&publish, &mut buf_legacy);

        assert_eq!(buf_v311, buf_legacy);
    }

    // =========================================================================
    // Phase 2: Protocol Validation Tests
    // =========================================================================

    // ---- Fixed header flags (M2) ----

    #[test]
    fn test_fixed_header_flags_valid() {
        // CONNECT: flags must be 0x00
        assert!(validate_fixed_header_flags(1, 0x00).is_ok());
        // PUBLISH: any flags are ok
        assert!(validate_fixed_header_flags(3, 0x0B).is_ok());
        // SUBSCRIBE: flags must be 0x02
        assert!(validate_fixed_header_flags(8, 0x02).is_ok());
        // PUBREL: flags must be 0x02
        assert!(validate_fixed_header_flags(6, 0x02).is_ok());
        // PINGREQ: flags must be 0x00
        assert!(validate_fixed_header_flags(12, 0x00).is_ok());
    }

    #[test]
    fn test_fixed_header_flags_invalid() {
        // CONNECT with non-zero flags
        assert!(validate_fixed_header_flags(1, 0x01).is_err());
        // SUBSCRIBE with wrong flags
        assert!(validate_fixed_header_flags(8, 0x00).is_err());
        assert!(validate_fixed_header_flags(8, 0x04).is_err());
        // PUBREL with wrong flags
        assert!(validate_fixed_header_flags(6, 0x00).is_err());
        // UNSUBSCRIBE with wrong flags
        assert!(validate_fixed_header_flags(10, 0x00).is_err());
        // CONNACK with non-zero flags
        assert!(validate_fixed_header_flags(2, 0x01).is_err());
        // PINGRESP with non-zero flags
        assert!(validate_fixed_header_flags(13, 0x01).is_err());
    }

    #[test]
    fn test_fixed_header_flags_rejected_on_decode() {
        // Encode a PINGREQ with wrong flags (should be 0xC0, try 0xC1)
        let buf = [0xC1, 0x00]; // PINGREQ type=12, flags=1
        assert!(matches!(
            decode_buf::packet_type(&buf),
            Err(CodecError::InvalidFixedHeaderFlags(12))
        ));
    }

    // ---- Topic name validation (M3) ----

    #[test]
    fn test_topic_name_valid() {
        assert!(validate_topic_name(b"sensor/1/temp").is_ok());
        assert!(validate_topic_name(b"a").is_ok());
        assert!(validate_topic_name(b"/leading/slash").is_ok());
    }

    #[test]
    fn test_topic_name_empty_rejected() {
        assert!(matches!(
            validate_topic_name(b""),
            Err(CodecError::InvalidTopicName)
        ));
    }

    #[test]
    fn test_topic_name_wildcards_rejected() {
        assert!(validate_topic_name(b"sensor/+/data").is_err());
        assert!(validate_topic_name(b"sensor/#").is_err());
        assert!(validate_topic_name(b"+").is_err());
        assert!(validate_topic_name(b"#").is_err());
    }

    #[test]
    fn test_topic_name_null_byte_rejected() {
        assert!(validate_topic_name(b"sensor/\x00/data").is_err());
    }

    // ---- Topic filter validation (M4) ----

    #[test]
    fn test_topic_filter_valid() {
        assert!(validate_topic_filter("sensor/+/data").is_ok());
        assert!(validate_topic_filter("sensor/#").is_ok());
        assert!(validate_topic_filter("#").is_ok());
        assert!(validate_topic_filter("+").is_ok());
        assert!(validate_topic_filter("+/+/+").is_ok());
        assert!(validate_topic_filter("a/b/c").is_ok());
    }

    #[test]
    fn test_topic_filter_empty_rejected() {
        assert!(matches!(
            validate_topic_filter(""),
            Err(CodecError::InvalidTopicFilter)
        ));
    }

    #[test]
    fn test_topic_filter_malformed_wildcards_rejected() {
        // '+' not occupying entire level
        assert!(validate_topic_filter("sensor/+data").is_err());
        assert!(validate_topic_filter("sensor/da+ta").is_err());
        // '#' not at the end
        assert!(validate_topic_filter("#/sensor").is_err());
        // '#' not occupying entire level
        assert!(validate_topic_filter("sensor/#data").is_err());
    }

    #[test]
    fn test_topic_filter_null_byte_rejected() {
        assert!(validate_topic_filter("sensor/\x00/data").is_err());
    }

    // ---- Password without Username (M14) ----

    #[test]
    fn test_v311_password_without_username_rejected() {
        // Build a CONNECT packet for V3.1.1 with password=true, username=false.
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: false,
                password: true,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: Bytes::from_static(b"test"),
            will: None,
            username: None,
            password: Some(Bytes::from_static(b"secret")),
            properties_raw: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);

        let result = decode_buf::connect(&buf);
        assert!(matches!(result, Err(CodecError::InvalidConnectFlags)));
    }

    // ---- UTF-8 null byte rejection (m1) ----

    #[test]
    fn test_utf8_null_byte_rejected_in_string() {
        // Construct a raw MQTT string with a null byte inside.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x05]); // length=5
        buf.extend_from_slice(b"he\x00lo"); // contains null byte
        let bytes = Bytes::from(buf);
        let mut pos = 0;
        let result = read_mqtt_string_zc(&bytes, &mut pos);
        assert!(matches!(result, Err(CodecError::InvalidUtf8)));
    }

    #[test]
    fn test_utf8_null_byte_rejected_in_string_as_bytes() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x05]);
        buf.extend_from_slice(b"he\x00lo");
        let bytes = Bytes::from(buf);
        let mut pos = 0;
        let result = read_mqtt_string_zc(&bytes, &mut pos);
        assert!(matches!(result, Err(CodecError::InvalidUtf8)));
    }

    // ---- Duplicate property detection (m2) ----

    #[test]
    fn test_duplicate_property_rejected() {
        // Encode two Payload Format Indicator properties (0x01) in one property set.
        let mut prop_bytes = BytesMut::new();
        prop_bytes.put_u8(0x01);
        prop_bytes.put_u8(0x00); // first PFI
        prop_bytes.put_u8(0x01);
        prop_bytes.put_u8(0x01); // duplicate PFI

        let mut buf = BytesMut::new();
        write_variable_int(prop_bytes.len() as u32, &mut buf);
        buf.extend_from_slice(&prop_bytes);

        let frozen = buf.freeze();
        let mut pos = 0;
        let result = read_properties_zc(&frozen, &mut pos);
        assert!(matches!(result, Err(CodecError::DuplicateProperty(0x01))));
    }

    #[test]
    fn test_duplicate_user_property_allowed() {
        // User properties (0x26) can appear multiple times.
        let mut prop_bytes = BytesMut::new();
        prop_bytes.put_u8(0x26);
        write_mqtt_string("k1", &mut prop_bytes);
        write_mqtt_string("v1", &mut prop_bytes);
        prop_bytes.put_u8(0x26);
        write_mqtt_string("k2", &mut prop_bytes);
        write_mqtt_string("v2", &mut prop_bytes);

        let mut buf = BytesMut::new();
        write_variable_int(prop_bytes.len() as u32, &mut buf);
        buf.extend_from_slice(&prop_bytes);

        let frozen = buf.freeze();
        let mut pos = 0;
        let result = read_properties_zc(&frozen, &mut pos);
        assert!(result.is_ok());
        let props = result.unwrap();
        assert_eq!(props.user_properties().count(), 2);
    }

    // ---- Property value validation (m3, m4, m5) ----

    #[test]
    fn test_subscription_identifier_zero_rejected() {
        let mut prop_bytes = BytesMut::new();
        prop_bytes.put_u8(0x0B); // Subscription Identifier
        write_variable_int(0, &mut prop_bytes); // value=0 is invalid

        let mut buf = BytesMut::new();
        write_variable_int(prop_bytes.len() as u32, &mut buf);
        buf.extend_from_slice(&prop_bytes);

        let frozen = buf.freeze();
        let mut pos = 0;
        assert!(matches!(
            read_properties_zc(&frozen, &mut pos),
            Err(CodecError::InvalidPropertyValue(0x0B))
        ));
    }

    #[test]
    fn test_topic_alias_zero_rejected() {
        let mut prop_bytes = BytesMut::new();
        prop_bytes.put_u8(0x23); // Topic Alias
        prop_bytes.put_u16(0); // value=0 is invalid

        let mut buf = BytesMut::new();
        write_variable_int(prop_bytes.len() as u32, &mut buf);
        buf.extend_from_slice(&prop_bytes);

        let frozen = buf.freeze();
        let mut pos = 0;
        assert!(matches!(
            read_properties_zc(&frozen, &mut pos),
            Err(CodecError::InvalidPropertyValue(0x23))
        ));
    }

    #[test]
    fn test_receive_maximum_zero_rejected() {
        let mut prop_bytes = BytesMut::new();
        prop_bytes.put_u8(0x21); // Receive Maximum
        prop_bytes.put_u16(0); // value=0 is invalid

        let mut buf = BytesMut::new();
        write_variable_int(prop_bytes.len() as u32, &mut buf);
        buf.extend_from_slice(&prop_bytes);

        let frozen = buf.freeze();
        let mut pos = 0;
        assert!(matches!(
            read_properties_zc(&frozen, &mut pos),
            Err(CodecError::InvalidPropertyValue(0x21))
        ));
    }

    // ---- MQIsdp rejected (m6) ----

    #[test]
    fn test_mqisdp_rejected() {
        // Manually build a CONNECT with "MQIsdp" protocol name.
        let mut buf = BytesMut::new();
        buf.put_u8(0x10); // CONNECT fixed header
        // We'll fill in remaining length after computing the payload.
        let mut payload = BytesMut::new();
        write_mqtt_string("MQIsdp", &mut payload);
        payload.put_u8(4); // protocol level 4
        payload.put_u8(0x02); // flags: clean_session=true
        payload.put_u16(60); // keep_alive
        write_mqtt_string("test-client", &mut payload);

        encode_remaining_length(payload.len(), &mut buf);
        buf.extend_from_slice(&payload);

        let result = decode_buf::connect(&buf);
        assert!(matches!(result, Err(CodecError::InvalidProtocolName(_))));
    }

    // ---- AUTH packet codec tests ----

    #[test]
    fn test_auth_encode_decode_success_no_props() {
        let auth = Auth {
            reason_code: Auth::SUCCESS,
            authentication_method: None,
            authentication_data: None,
            reason_string: None,
        };
        let mut buf = BytesMut::new();
        encode_auth(&auth, &mut buf);
        // Success with no props should be 2 bytes: 0xF0, 0x00
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 0xF0);
        assert_eq!(buf[1], 0x00);

        let (decoded, _) = decode_buf::auth(&buf).unwrap();
        assert_eq!(decoded.reason_code, Auth::SUCCESS);
    }

    #[test]
    fn test_auth_encode_decode_continue_with_method() {
        let auth = Auth {
            reason_code: Auth::CONTINUE_AUTHENTICATION,
            authentication_method: Some(Bytes::from_static(b"SCRAM-SHA-256")),
            authentication_data: Some(Bytes::from_static(b"challenge-data")),
            reason_string: None,
        };
        let mut buf = BytesMut::new();
        encode_auth(&auth, &mut buf);
        assert!(buf.len() > 2);

        let (decoded, _) = decode_buf::auth(&buf).unwrap();
        assert_eq!(decoded.reason_code, Auth::CONTINUE_AUTHENTICATION);
        assert_eq!(
            decoded.authentication_method.as_deref(),
            Some(b"SCRAM-SHA-256".as_ref())
        );
        assert_eq!(
            decoded.authentication_data.as_deref(),
            Some(b"challenge-data".as_ref())
        );
    }

    #[test]
    fn test_auth_encode_decode_reauthenticate() {
        let auth = Auth {
            reason_code: Auth::RE_AUTHENTICATE,
            authentication_method: Some(Bytes::from_static(b"PLAIN")),
            authentication_data: None,
            reason_string: None,
        };
        let mut buf = BytesMut::new();
        encode_auth(&auth, &mut buf);

        let (decoded, _) = decode_buf::auth(&buf).unwrap();
        assert_eq!(decoded.reason_code, Auth::RE_AUTHENTICATE);
        assert_eq!(
            decoded.authentication_method.as_deref(),
            Some(b"PLAIN".as_ref())
        );
    }

    // ---- UTF-8 control character validation ----

    #[test]
    fn test_utf8_control_chars_rejected() {
        // U+0001 (SOH control character) should be rejected.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x04]);
        buf.extend_from_slice(b"ab\x01c");
        let bytes = Bytes::from(buf);
        let mut pos = 0;
        assert!(matches!(
            read_mqtt_string_zc(&bytes, &mut pos),
            Err(CodecError::InvalidUtf8)
        ));
    }

    #[test]
    fn test_utf8_del_rejected() {
        // U+007F (DEL) should be rejected.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x03]);
        buf.extend_from_slice(b"a\x7Fb");
        let bytes = Bytes::from(buf);
        let mut pos = 0;
        assert!(matches!(
            read_mqtt_string_zc(&bytes, &mut pos),
            Err(CodecError::InvalidUtf8)
        ));
    }

    #[test]
    fn test_utf8_c1_control_rejected() {
        // U+0080 (C1 control character) — encoded as 0xC2 0x80 in UTF-8.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x03]);
        buf.push(b'a');
        buf.push(0xC2);
        buf.push(0x80); // U+0080
        let bytes = Bytes::from(buf);
        let mut pos = 0;
        assert!(matches!(
            read_mqtt_string_zc(&bytes, &mut pos),
            Err(CodecError::InvalidUtf8)
        ));
    }

    #[test]
    fn test_utf8_nonchar_fffe_rejected() {
        // U+FFFE (non-character) — encoded as 0xEF 0xBF 0xBE in UTF-8.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x04]);
        buf.push(b'a');
        buf.push(0xEF);
        buf.push(0xBF);
        buf.push(0xBE); // U+FFFE
        let bytes = Bytes::from(buf);
        let mut pos = 0;
        assert!(matches!(
            read_mqtt_string_zc(&bytes, &mut pos),
            Err(CodecError::InvalidUtf8)
        ));
    }

    #[test]
    fn test_utf8_valid_string_passes() {
        // Normal ASCII + multibyte UTF-8 should pass.
        let text = "hello Wörld 你好";
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&(text.len() as u16).to_be_bytes());
        buf.extend_from_slice(text.as_bytes());
        let bytes = Bytes::from(buf);
        let mut pos = 0;
        let result = read_mqtt_string_zc(&bytes, &mut pos).unwrap();
        assert_eq!(&result[..], text.as_bytes());
    }

    // ---- CONNACK ack_flags validation ----

    #[test]
    fn test_connack_reserved_ack_flags_rejected() {
        // ack_flags with bit 1 set (reserved) should be rejected.
        let mut buf = BytesMut::new();
        buf.put_u8(0x20); // CONNACK fixed header
        buf.put_u8(0x02); // remaining length
        buf.put_u8(0x02); // ack_flags = 0x02 (reserved bit set)
        buf.put_u8(0x00); // return code
        let result = decode_buf::connack(&buf);
        assert!(
            result.is_err(),
            "CONNACK with reserved ack_flags should be rejected"
        );
    }

    #[test]
    fn test_connack_valid_ack_flags_accepted() {
        // ack_flags = 0x01 (session_present=true) should be accepted.
        let mut buf = BytesMut::new();
        buf.put_u8(0x20); // CONNACK fixed header
        buf.put_u8(0x02); // remaining length
        buf.put_u8(0x01); // ack_flags = 0x01 (session_present)
        buf.put_u8(0x00); // return code
        let (ca, _) = decode_buf::connack(&buf).unwrap();
        assert!(ca.session_present);
        assert_eq!(ca.return_code, 0);
    }

    // =========================================================================
    // Additional coverage tests
    // =========================================================================

    #[test]
    fn test_encode_decode_publish_qos2() {
        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: b"qos2/topic",
            packet_id: Some(999),
            payload: b"exactly-once",
            properties: Properties::default(),
        };

        let mut buf = BytesMut::new();
        encode_publish(&publish, &mut buf);
        let (p, consumed) = decode_buf::publish(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(p.qos, QoS::ExactlyOnce);
        assert!(p.retain);
        assert_eq!(p.packet_id, Some(999));
        assert_eq!(&p.topic[..], b"qos2/topic");
        assert_eq!(&p.payload[..], b"exactly-once");
    }

    #[test]
    fn test_remaining_length_three_bytes() {
        // 16384 = 0x80, 0x80, 0x01
        assert_eq!(
            decode_remaining_length(&[0x80, 0x80, 0x01]).unwrap(),
            (16384, 3)
        );
    }

    #[test]
    fn test_remaining_length_malformed_five_bytes() {
        // 5 continuation bytes = malformed
        let buf = [0x80, 0x80, 0x80, 0x80, 0x01];
        let result = decode_remaining_length(&buf);
        assert!(matches!(result, Err(CodecError::MalformedRemainingLength)));
    }

    #[test]
    fn test_remaining_length_overflow_packet_too_large() {
        // Encode a value > MAX_PACKET_SIZE (268,435,456). The max valid value in
        // 4-byte variable length is 268,435,455 which is exactly MAX_PACKET_SIZE - 1.
        // 268,435,455 = 0xFF, 0xFF, 0xFF, 0x7F — this should pass.
        assert_eq!(
            decode_remaining_length(&[0xFF, 0xFF, 0xFF, 0x7F]).unwrap(),
            (268_435_455, 4)
        );
    }

    #[test]
    fn test_encode_decode_connect_v5() {
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags {
                username: true,
                password: true,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 120,
            client_id: Bytes::from_static(b"v5-client"),
            will: None,
            username: Some(Bytes::from_static(b"user")),
            password: Some(Bytes::from_static(b"pass")),
            properties_raw: PropertiesBuilder::new()
                .session_expiry_interval(3600)
                .receive_maximum(100)
                .topic_alias_maximum(10)
                .build(),
        };

        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);
        let (c, consumed) = decode_buf::connect(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(c.protocol_version, ProtocolVersion::V5);
        assert_eq!(&c.client_id[..], b"v5-client");
        assert_eq!(c.username.as_deref(), Some(b"user".as_ref()));
        assert_eq!(c.password.as_deref(), Some(b"pass".as_ref()));
        assert_eq!(c.properties().session_expiry_interval(), Some(3600));
        assert_eq!(c.properties().receive_maximum(), Some(100));
        assert_eq!(c.properties().topic_alias_maximum(), Some(10));
    }

    #[test]
    fn test_encode_decode_connect_v5_with_will() {
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: true,
                will_qos: QoS::AtLeastOnce,
                will: true,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: Bytes::from_static(b"v5-will"),
            will: Some(WillMessage {
                topic: Bytes::from_static(b"last/will"),
                payload: Bytes::from_static(b"goodbye"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties_raw: PropertiesBuilder::new()
                    .will_delay_interval(30)
                    .content_type("text/plain")
                    .build(),
            }),
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };

        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);
        let (c, _) = decode_buf::connect(&buf).unwrap();
        assert_eq!(c.protocol_version, ProtocolVersion::V5);
        let will = c.will.unwrap();
        assert_eq!(&will.topic[..], b"last/will");
        assert_eq!(&will.payload[..], b"goodbye");
        assert_eq!(will.properties().will_delay_interval(), Some(30));
        assert_eq!(will.properties().content_type(), Some("text/plain"));
    }

    #[test]
    fn test_v5_connack_with_properties() {
        let connack = ConnAck {
            session_present: true,
            return_code: 0x00,
            maximum_qos: Some(1),
            retain_available: Some(true),
            maximum_packet_size: Some(1048576),
            server_keep_alive: Some(300),
            reason_string: Some(Bytes::from_static(b"welcome")),
            ..ConnAck::default()
        };

        let mut buf = BytesMut::new();
        encode_connack_v(&connack, &mut buf, true);
        let (ca, _) = decode_buf::connack(&buf).unwrap();
        assert!(ca.session_present);
        assert_eq!(ca.return_code, 0x00);
        assert_eq!(ca.maximum_qos, Some(1));
        assert_eq!(ca.retain_available, Some(true));
        assert_eq!(ca.maximum_packet_size, Some(1048576));
        assert_eq!(ca.server_keep_alive, Some(300));
        assert_eq!(ca.reason_string.as_deref(), Some(b"welcome".as_ref()));
    }

    #[test]
    fn test_v5_pubrec_roundtrip() {
        let pubrec = PubRec {
            packet_id: 42,
            reason_code: Some(0x10), // No Matching Subscribers
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_pubrec_v(&pubrec, &mut buf, true);
        let (pr, _) = decode_buf::pubrec(&buf).unwrap();
        assert_eq!(pr.packet_id, 42);
        assert_eq!(pr.reason_code, Some(0x10));
    }

    #[test]
    fn test_v311_pubrec_roundtrip() {
        let pubrec = PubRec {
            packet_id: 55,
            reason_code: None,
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_pubrec(&pubrec, &mut buf);
        let (pr, _) = decode_buf::pubrec(&buf).unwrap();
        assert_eq!(pr.packet_id, 55);
    }

    #[test]
    fn test_v5_pubcomp_with_reason_code() {
        let pubcomp = PubComp {
            packet_id: 77,
            reason_code: Some(0x92), // Packet Identifier Not Found
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_pubcomp_v(&pubcomp, &mut buf, true);
        let (pc, _) = decode_buf::pubcomp(&buf).unwrap();
        assert_eq!(pc.packet_id, 77);
        assert_eq!(pc.reason_code, Some(0x92));
    }

    #[test]
    fn test_v5_pubcomp_with_reason_and_properties() {
        let pubcomp = PubComp {
            packet_id: 88,
            reason_code: Some(0x00),
            reason_string: Some(b"ok"),
        };

        let mut buf = BytesMut::new();
        encode_pubcomp_v(&pubcomp, &mut buf, true);
        let (pc, _) = decode_buf::pubcomp(&buf).unwrap();
        assert_eq!(pc.packet_id, 88);
        assert_eq!(pc.reason_string.as_deref(), Some(b"ok".as_ref()));
    }

    #[test]
    fn test_v5_disconnect_reason_code_only_no_props() {
        let disconnect = Disconnect {
            reason_code: Some(0x04), // Disconnect with Will Message
            session_expiry_interval: None,
            reason_string: None,
            server_reference: None,
        };

        let mut buf = BytesMut::new();
        encode_disconnect(&disconnect, &mut buf);
        let (d, _) = decode_buf::disconnect(&buf).unwrap();
        assert_eq!(d.reason_code, Some(0x04));
    }

    #[test]
    fn test_v5_auth_reason_code_only_no_props() {
        // AUTH with non-success reason code but no properties.
        let auth = Auth {
            reason_code: Auth::CONTINUE_AUTHENTICATION,
            authentication_method: None,
            authentication_data: None,
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_auth(&auth, &mut buf);
        let (a, _) = decode_buf::auth(&buf).unwrap();
        assert_eq!(a.reason_code, Auth::CONTINUE_AUTHENTICATION);
    }

    #[test]
    fn test_v5_publish_qos2_with_properties_roundtrip() {
        let props_raw = PropertiesBuilder::new()
            .message_expiry_interval(60)
            .correlation_data(b"req-1")
            .build();
        let publish = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: b"test/qos2",
            packet_id: Some(500),
            payload: b"hello",
            properties: Properties::from_raw(&props_raw),
        };

        let mut buf = BytesMut::new();
        encode_publish_v(&publish, &mut buf, true);
        let (p, _) = decode_buf::publish_v5(&buf).unwrap();
        assert!(p.dup);
        assert_eq!(p.qos, QoS::ExactlyOnce);
        assert!(!p.retain);
        assert_eq!(p.packet_id, Some(500));
        assert_eq!(p.properties.message_expiry_interval(), Some(60));
        assert_eq!(p.properties.correlation_data(), Some(b"req-1".as_ref()));
    }

    #[test]
    fn test_v5_subscribe_without_subscription_id() {
        // V5 subscribe with empty properties (no subscription identifier).
        let subscribe = Subscribe {
            packet_id: 10,
            filters: smallvec![TopicFilter {
                filter: Bytes::from_static(b"a/b"),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            }],
            subscription_identifier: None,
        };

        let mut buf = BytesMut::new();
        encode_subscribe_v(&subscribe, &mut buf, true);
        let (s, _) = decode_buf::subscribe_v5(&buf).unwrap();
        assert_eq!(s.packet_id, 10);
        assert_eq!(s.filters.len(), 1);
        assert_eq!(&s.filters[0].filter[..], b"a/b");
        assert_eq!(s.subscription_identifier, None);
    }

    #[test]
    fn test_v5_suback_empty_properties() {
        let suback = SubAck {
            packet_id: 20,
            return_codes: smallvec![0x00, 0x01],
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_suback_v(&suback, &mut buf, true);
        let (sa, _) = decode_buf::suback_v5(&buf).unwrap();
        assert_eq!(sa.packet_id, 20);
        assert_eq!(sa.return_codes.as_slice(), &[0x00, 0x01]);
    }

    #[test]
    fn test_v5_unsubscribe_empty_properties() {
        let unsubscribe = Unsubscribe {
            packet_id: 30,
            filters: smallvec![Bytes::from_static(b"x/y")],
        };

        let mut buf = BytesMut::new();
        encode_unsubscribe_v(&unsubscribe, &mut buf, true);
        let (u, _) = decode_buf::unsubscribe_v5(&buf).unwrap();
        assert_eq!(u.packet_id, 30);
        assert_eq!(&u.filters[0][..], b"x/y");
    }

    #[test]
    fn test_v5_unsuback_empty_properties() {
        let unsuback = UnsubAck {
            packet_id: 40,
            reason_codes: smallvec![0x00, 0x11],
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_unsuback_v(&unsuback, &mut buf, true);
        let (ua, _) = decode_buf::unsuback_v5(&buf).unwrap();
        assert_eq!(ua.packet_id, 40);
        assert_eq!(ua.reason_codes.as_slice(), &[0x00, 0x11]);
    }

    #[test]
    fn test_encode_remaining_length_roundtrip_boundaries() {
        // Test all boundary values for remaining length encoding.
        for &value in &[
            0,
            1,
            127,
            128,
            16383,
            16384,
            2_097_151,
            2_097_152,
            268_435_455,
        ] {
            let mut buf = BytesMut::new();
            encode_remaining_length(value, &mut buf);
            let (decoded, _) = decode_remaining_length(&buf).unwrap();
            assert_eq!(decoded, value, "roundtrip failed for {}", value);
        }
    }

    #[test]
    fn test_v5_pubrec_compact_success() {
        // PUBREC V5 with reason_code=0x00 and no properties should use compact form.
        let pubrec = PubRec {
            packet_id: 100,
            reason_code: Some(0x00),
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_pubrec_v(&pubrec, &mut buf, true);
        // Compact form: fixed_header(1) + remaining_length(1) + packet_id(2) = 4 bytes
        assert_eq!(buf.len(), 4);

        let (pr, _) = decode_buf::pubrec(&buf).unwrap();
        assert_eq!(pr.packet_id, 100);
    }

    #[test]
    fn test_v5_pubrec_with_properties() {
        let pubrec = PubRec {
            packet_id: 101,
            reason_code: Some(0x00),
            reason_string: Some(b"ack"),
        };

        let mut buf = BytesMut::new();
        encode_pubrec_v(&pubrec, &mut buf, true);
        let (pr, _) = decode_buf::pubrec(&buf).unwrap();
        assert_eq!(pr.packet_id, 101);
        assert_eq!(pr.reason_string.as_deref(), Some(b"ack".as_ref()));
    }

    #[test]
    fn test_v5_pubrel_reason_code_only() {
        let pubrel = PubRel {
            packet_id: 200,
            reason_code: Some(0x92), // Packet Identifier Not Found
            reason_string: None,
        };

        let mut buf = BytesMut::new();
        encode_pubrel_v(&pubrel, &mut buf, true);
        let (pr, _) = decode_buf::pubrel(&buf).unwrap();
        assert_eq!(pr.packet_id, 200);
        assert_eq!(pr.reason_code, Some(0x92));
    }

    #[test]
    fn test_unknown_packet_type_rejected() {
        // Packet type 0 is invalid.
        let mut buf = BytesMut::new();
        buf.put_u8(0x00); // type 0
        buf.put_u8(0x00); // remaining length
        assert!(matches!(
            decode_buf::packet_type(&buf),
            Err(CodecError::UnknownPacketType(0))
        ));
    }

    #[test]
    fn test_v5_connect_with_auth_method() {
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: Bytes::from_static(b"auth-client"),
            will: None,
            username: None,
            password: None,
            properties_raw: PropertiesBuilder::new()
                .authentication_method("SCRAM-SHA-256")
                .authentication_data(b"client-first")
                .build(),
        };

        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);
        let (c, _) = decode_buf::connect(&buf).unwrap();
        assert_eq!(
            c.properties().authentication_method(),
            Some("SCRAM-SHA-256")
        );
        assert_eq!(
            c.properties().authentication_data(),
            Some(b"client-first".as_ref())
        );
    }

    #[test]
    fn test_publish_dup_flag_roundtrip() {
        let publish = Publish {
            dup: true,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"t",
            packet_id: Some(1),
            payload: b"",
            properties: Properties::default(),
        };

        let mut buf = BytesMut::new();
        encode_publish(&publish, &mut buf);
        let (p, _) = decode_buf::publish(&buf).unwrap();
        assert!(p.dup);
    }

    #[test]
    fn test_v5_properties_user_properties_multiple() {
        let props_raw = PropertiesBuilder::new()
            .user_property("key1", "val1")
            .user_property("key2", "val2")
            .user_property("key1", "val3") // duplicate key allowed
            .build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"props",
            packet_id: None,
            payload: b"",
            properties: Properties::from_raw(&props_raw),
        };

        let mut buf = BytesMut::new();
        encode_publish_v(&publish, &mut buf, true);
        let (p, _) = decode_buf::publish_v5(&buf).unwrap();
        let up: Vec<_> = p.properties.user_properties().collect();
        assert_eq!(up.len(), 3);
        assert_eq!(up[0], ("key1", "val1"));
        assert_eq!(up[2], ("key1", "val3"));
    }

    #[test]
    fn test_v5_connack_assigned_client_id() {
        let connack = ConnAck {
            session_present: false,
            return_code: 0x00,
            assigned_client_identifier: Some(Bytes::from_static(b"server-assigned-id")),
            ..ConnAck::default()
        };

        let mut buf = BytesMut::new();
        encode_connack_v(&connack, &mut buf, true);
        let (ca, _) = decode_buf::connack(&buf).unwrap();
        assert_eq!(
            ca.assigned_client_identifier.as_deref(),
            Some(b"server-assigned-id".as_ref())
        );
    }

    // =========================================================================
    // MQTT 3.1.1 Conformance Tests (GAP fixes)
    // =========================================================================

    /// Helper: build a raw MQTT packet from a type/flags nibble and payload.
    fn build_raw_packet(type_nibble: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push((type_nibble << 4) | (flags & 0x0F));
        // Encode remaining length.
        let mut len = payload.len();
        loop {
            let mut byte = (len & 0x7F) as u8;
            len >>= 7;
            if len > 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if len == 0 {
                break;
            }
        }
        buf.extend_from_slice(payload);
        buf
    }

    // GAP-1: Zero packet ID must be rejected.

    #[test]
    fn test_gap1_publish_qos1_zero_packet_id_rejected() {
        // PUBLISH QoS 1, topic "a", packet_id=0
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // topic len + topic
        payload.extend_from_slice(&[0x00, 0x00]); // packet_id = 0
        // flags: QoS 1 = 0x02
        let raw = build_raw_packet(3, 0x02, &payload);
        let result = decode_buf::publish(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "zero packet ID in QoS 1 PUBLISH should be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn test_gap1_subscribe_zero_packet_id_rejected() {
        // SUBSCRIBE: packet_id=0, one filter "a" with QoS 0
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x00]); // packet_id = 0
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // filter "a"
        payload.push(0x00); // QoS 0
        // SUBSCRIBE type=8, flags=0x02 (required)
        let raw = build_raw_packet(8, 0x02, &payload);
        let result = decode_buf::subscribe(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "zero packet ID in SUBSCRIBE should be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn test_gap1_unsubscribe_zero_packet_id_rejected() {
        // UNSUBSCRIBE: packet_id=0, one filter "a"
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x00]); // packet_id = 0
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // filter "a"
        // UNSUBSCRIBE type=10, flags=0x02 (required)
        let raw = build_raw_packet(10, 0x02, &payload);
        let result = decode_buf::unsubscribe(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "zero packet ID in UNSUBSCRIBE should be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn test_gap1_valid_nonzero_packet_ids_accepted() {
        // SUBSCRIBE with packet_id=1 should decode fine.
        let subscribe = Subscribe {
            packet_id: 1,
            filters: smallvec![TopicFilter {
                filter: Bytes::from_static(b"test/topic"),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            }],
            subscription_identifier: None,
        };
        let mut buf = BytesMut::new();
        encode_subscribe(&subscribe, &mut buf);
        let (s, _) = decode_buf::subscribe(&buf).unwrap();
        assert_eq!(s.packet_id, 1);
    }

    // GAP-5: Empty SUBSCRIBE payload must be rejected.

    #[test]
    fn test_gap5_subscribe_empty_payload_rejected() {
        // SUBSCRIBE with packet_id=1 but no topic filters.
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01]); // packet_id = 1
        // No filters follow.
        let raw = build_raw_packet(8, 0x02, &payload);
        let result = decode_buf::subscribe(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "empty SUBSCRIBE payload should be rejected, got {:?}",
            result
        );
    }

    // GAP-6: V3.1.1 SUBSCRIBE options byte reserved bits must be zero.

    #[test]
    fn test_gap6_subscribe_v311_reserved_bits_rejected() {
        // SUBSCRIBE with reserved bits set in options byte (0x04 = no_local, v5 only).
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01]); // packet_id = 1
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // filter "a"
        payload.push(0x04); // no_local bit set — invalid in v3.1.1
        let raw = build_raw_packet(8, 0x02, &payload);
        // decode_buf::subscribe uses the v3.1.1 path (is_v5=false).
        let result = decode_buf::subscribe(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "v3.1.1 SUBSCRIBE with reserved bits should be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn test_gap6_subscribe_v311_valid_qos_accepted() {
        // SUBSCRIBE with valid QoS 2 (options byte 0x02) should succeed.
        let subscribe = Subscribe {
            packet_id: 1,
            filters: smallvec![TopicFilter {
                filter: Bytes::from_static(b"a"),
                qos: QoS::ExactlyOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            }],
            subscription_identifier: None,
        };
        let mut buf = BytesMut::new();
        encode_subscribe(&subscribe, &mut buf);
        assert!(decode_buf::subscribe(&buf).is_ok());
    }

    // GAP-7: Empty UNSUBSCRIBE payload must be rejected.

    #[test]
    fn test_gap7_unsubscribe_empty_payload_rejected() {
        // UNSUBSCRIBE with packet_id=1 but no topic filters.
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01]); // packet_id = 1
        // No filters follow.
        let raw = build_raw_packet(10, 0x02, &payload);
        let result = decode_buf::unsubscribe(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "empty UNSUBSCRIBE payload should be rejected, got {:?}",
            result
        );
    }

    // GAP-1: Zero-copy PUBLISH path also rejects zero packet IDs.

    #[test]
    fn test_gap1_publish_zero_copy_qos1_zero_packet_id_rejected() {
        // PUBLISH QoS 1, topic "a", packet_id=0 — via the Bytes path.
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // topic
        payload.extend_from_slice(&[0x00, 0x00]); // packet_id = 0
        let raw = build_raw_packet(3, 0x02, &payload);
        let result = decode_buf::publish(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "zero-copy path: zero packet ID in QoS 1 PUBLISH should be rejected, got {:?}",
            result
        );
    }

    // Versioned path: zero packet ID in QoS 2 PUBLISH.

    #[test]
    fn test_gap1_publish_qos2_zero_packet_id_rejected() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // topic
        payload.extend_from_slice(&[0x00, 0x00]); // packet_id = 0
        // flags: QoS 2 = 0x04
        let raw = build_raw_packet(3, 0x04, &payload);
        let result = decode_buf::publish(&raw);
        assert!(
            matches!(result, Err(CodecError::ProtocolError)),
            "zero packet ID in QoS 2 PUBLISH should be rejected, got {:?}",
            result
        );
    }

    // =========================================================================
    // MQTT 3.1.1 Compliance Tests
    // =========================================================================

    // ---- BOM preservation [MQTT-1.5.3-3] ----

    #[test]
    fn test_utf8_bom_preserved_in_string() {
        // MQTT spec says BOM (U+FEFF = 0xEF 0xBB 0xBF) SHOULD NOT be stripped.
        // Verify that a string containing BOM survives encode/decode roundtrip.
        let mut buf = BytesMut::new();
        // Write a string that starts with BOM: "\u{FEFF}hello"
        let bom_str = "\u{FEFF}hello";
        write_mqtt_string(bom_str, &mut buf);

        let bytes = buf.freeze();
        let mut pos = 0;
        let decoded = read_mqtt_string_zc(&bytes, &mut pos).unwrap();
        assert_eq!(
            &decoded[..],
            bom_str.as_bytes(),
            "BOM must be preserved, not stripped"
        );
    }

    // ---- V5 password without username is allowed ----

    #[test]
    fn test_v5_password_without_username_allowed() {
        // MQTT 5.0 relaxes this constraint — password without username is valid.
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags {
                username: false,
                password: true,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: Bytes::from_static(b"v5-test"),
            will: None,
            username: None,
            password: Some(Bytes::from_static(b"token")),
            properties_raw: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);

        let result = decode_buf::connect(&buf);
        assert!(result.is_ok(), "V5 should allow password without username");
    }

    // ---- QoS 3 rejected [MQTT-3.3.1-4] ----

    #[test]
    fn test_publish_qos3_rejected() {
        // QoS bits both set to 1 (QoS=3) is a protocol violation.
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x04]); // topic length
        payload.extend_from_slice(b"test");
        payload.extend_from_slice(&[0x00, 0x01]); // packet_id (required for QoS>0)
        payload.extend_from_slice(b"data");

        // flags: 0x06 = QoS 3 (both bits set)
        let raw = build_raw_packet(3, 0x06, &payload);
        let result = decode_buf::publish(&raw);
        assert!(result.is_err(), "QoS 3 (0x06) must be rejected");
    }

    // ---- Fixed header flag validation [MQTT-2.2.2-1] ----

    #[test]
    fn test_subscribe_wrong_flags_rejected() {
        // SUBSCRIBE fixed header flags must be 0x02.
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01]); // packet_id
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // filter
        payload.push(0x00); // QoS 0

        // flags=0x00 instead of required 0x02
        let raw = build_raw_packet(8, 0x00, &payload);
        let result = decode_buf::subscribe(&raw);
        assert!(
            matches!(result, Err(CodecError::InvalidFixedHeaderFlags(8))),
            "SUBSCRIBE with flags!=0x02 must be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn test_unsubscribe_wrong_flags_rejected() {
        // UNSUBSCRIBE fixed header flags must be 0x02.
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01]); // packet_id
        payload.extend_from_slice(&[0x00, 0x01, b'a']); // filter

        // flags=0x00 instead of required 0x02
        let raw = build_raw_packet(10, 0x00, &payload);
        let result = decode_buf::unsubscribe(&raw);
        assert!(
            matches!(result, Err(CodecError::InvalidFixedHeaderFlags(10))),
            "UNSUBSCRIBE with flags!=0x02 must be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn test_pingreq_wrong_flags_rejected() {
        // PINGREQ flags must be 0x00.
        let raw = build_raw_packet(12, 0x01, &[]);
        let result = decode_buf::packet_type(&raw);
        assert!(
            matches!(result, Err(CodecError::InvalidFixedHeaderFlags(12))),
            "PINGREQ with flags!=0x00 must be rejected, got {:?}",
            result
        );
    }

    // ---- Topic name validation [MQTT-3.3.2-2] ----

    #[test]
    fn test_topic_name_with_hash_wildcard_rejected() {
        assert!(validate_topic_name(b"sensor/#").is_err());
    }

    #[test]
    fn test_topic_name_with_plus_wildcard_rejected() {
        assert!(validate_topic_name(b"sensor/+/data").is_err());
    }

    #[test]
    fn test_topic_name_empty_is_invalid() {
        assert!(validate_topic_name(b"").is_err());
    }

    // ---- Topic filter validation edge cases [MQTT-4.7.1-2, MQTT-4.7.1-3] ----

    #[test]
    fn test_topic_filter_hash_not_last_rejected() {
        assert!(validate_topic_filter("a/#/b").is_err());
    }

    #[test]
    fn test_topic_filter_hash_mixed_in_level_rejected() {
        assert!(validate_topic_filter("a/b#").is_err());
    }

    #[test]
    fn test_topic_filter_plus_mixed_in_level_rejected() {
        assert!(validate_topic_filter("a/b+c").is_err());
    }

    #[test]
    fn test_topic_filter_valid_multilevel() {
        assert!(validate_topic_filter("#").is_ok());
        assert!(validate_topic_filter("a/b/#").is_ok());
    }

    #[test]
    fn test_topic_filter_valid_single_level() {
        assert!(validate_topic_filter("+").is_ok());
        assert!(validate_topic_filter("+/a/+").is_ok());
        assert!(validate_topic_filter("a/+/b").is_ok());
    }

    // ---- Remaining length encoding edge cases ----

    #[test]
    fn test_remaining_length_max_value() {
        // Maximum remaining length: 268,435,455
        let max_val = 268_435_455;
        let mut buf = BytesMut::new();
        encode_remaining_length(max_val, &mut buf);
        let (decoded, consumed) = decode_remaining_length(&buf).unwrap();
        assert_eq!(decoded, max_val);
        assert_eq!(consumed, 4);
    }

    // ---- Connect with all fields roundtrip ----

    #[test]
    fn test_connect_v311_with_credentials_and_will_roundtrip() {
        let connect = Connect {
            protocol_name: Bytes::from_static(b"MQTT"),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: true,
                password: true,
                will_retain: true,
                will_qos: QoS::ExactlyOnce,
                will: true,
                clean_session: false,
            },
            keep_alive: 300,
            client_id: Bytes::from_static(b"full-featured-client"),
            will: Some(WillMessage {
                topic: Bytes::from_static(b"clients/status"),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::ExactlyOnce,
                retain: true,
                properties_raw: Vec::new(),
            }),
            username: Some(Bytes::from_static(b"admin")),
            password: Some(Bytes::from_static(b"secret123")),
            properties_raw: Vec::new(),
        };

        let mut buf = BytesMut::new();
        encode_connect(&connect, &mut buf);

        let (c, consumed) = decode_buf::connect(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(c.protocol_version, ProtocolVersion::V311);
        assert!(!c.flags.clean_session);
        assert!(c.flags.will);
        assert!(c.flags.will_retain);
        assert_eq!(c.flags.will_qos, QoS::ExactlyOnce);
        assert_eq!(c.keep_alive, 300);
        assert_eq!(&c.client_id[..], b"full-featured-client");
        let will = c.will.unwrap();
        assert_eq!(&will.topic[..], b"clients/status");
        assert_eq!(will.payload.as_ref(), b"offline");
        assert_eq!(&c.username.unwrap()[..], b"admin");
        assert_eq!(c.password.unwrap().as_ref(), b"secret123");
    }

    // ---- PUBREL/PUBCOMP roundtrip ----

    #[test]
    fn test_encode_decode_pubrel() {
        let pubrel = PubRel {
            packet_id: 42,
            reason_code: None,
            reason_string: None,
        };
        let mut buf = BytesMut::new();
        encode_pubrel(&pubrel, &mut buf);

        // PUBREL fixed header must have flags=0x02
        assert_eq!(buf[0] & 0x0F, 0x02);

        let (pr, _) = decode_buf::pubrel(&buf).unwrap();
        assert_eq!(pr.packet_id, 42);
    }

    #[test]
    fn test_encode_decode_pubrec() {
        let pubrec = PubRec {
            packet_id: 77,
            reason_code: None,
            reason_string: None,
        };
        let mut buf = BytesMut::new();
        encode_pubrec(&pubrec, &mut buf);

        let (pr, _) = decode_buf::pubrec(&buf).unwrap();
        assert_eq!(pr.packet_id, 77);
    }

    #[test]
    fn test_encode_decode_pubcomp() {
        let pubcomp = PubComp {
            packet_id: 99,
            reason_code: None,
            reason_string: None,
        };
        let mut buf = BytesMut::new();
        encode_pubcomp(&pubcomp, &mut buf);

        let (pc, _) = decode_buf::pubcomp(&buf).unwrap();
        assert_eq!(pc.packet_id, 99);
    }

    // ---- Publish QoS 2 with retain roundtrip ----

    #[test]
    fn test_publish_qos2_retain_roundtrip() {
        let publish = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: b"status/device/1",
            packet_id: Some(65535),
            payload: b"retained qos2 data",
            properties: Properties::default(),
        };

        let mut buf = BytesMut::new();
        encode_publish(&publish, &mut buf);

        let (p, _) = decode_buf::publish(&buf).unwrap();
        assert!(p.dup);
        assert_eq!(p.qos, QoS::ExactlyOnce);
        assert!(p.retain);
        assert_eq!(&p.topic[..], b"status/device/1");
        assert_eq!(p.packet_id, Some(65535));
        assert_eq!(&p.payload[..], b"retained qos2 data");
    }

    // ---- ConnectFlags reserved bit validation ----

    #[test]
    fn test_connect_flags_reserved_bit_set_rejected() {
        // Reserved bit 0 must be 0.
        assert!(
            ConnectFlags::from_byte(0x01).is_none(),
            "reserved bit 0 must be 0"
        );
        assert!(
            ConnectFlags::from_byte(0x03).is_none(),
            "reserved bit must cause rejection"
        );
    }

    // ---- Will flag constraints [MQTT-3.1.2-11 through 3.1.2-15] ----

    #[test]
    fn test_connect_flags_will_false_constraints() {
        // will=false: will_qos must be 0 and will_retain must be false.
        // will=false, will_qos=1: byte = 0b0000_1000 = 0x08
        assert!(
            ConnectFlags::from_byte(0x08).is_none(),
            "will=false with will_qos>0 must fail"
        );
        // will=false, will_retain=true: byte = 0b0010_0000 = 0x20
        assert!(
            ConnectFlags::from_byte(0x20).is_none(),
            "will=false with will_retain must fail"
        );
    }

    // ---- Message Expiry Adjustment tests (GAP-5) ----

    #[test]
    fn test_encode_publish_from_flat_with_adjusted_expiry() {
        use bisque_mq::flat::FlatMessageBuilder;

        // Build a FlatMessage with original TTL of 300s.
        let flat_bytes = FlatMessageBuilder::new(b"payload")
            .routing_key(b"test/expiry")
            .ttl_ms(300_000)
            .timestamp(1000)
            .build();

        let flat = FlatMessage::new(&flat_bytes).unwrap();
        let mut buf = BytesMut::new();

        // Encode with adjusted expiry of 120 seconds (remaining lifetime).
        encode_publish_from_flat_with_expiry(
            &flat,
            QoS::AtLeastOnce,
            false,
            false,
            Some(1),
            true, // V5
            None,
            None,
            Some(120), // adjusted expiry
            &mut buf,
        );

        // Decode with V5 to get properties.
        let (p, _) = decode_buf::publish_v5(&buf).unwrap();
        assert_eq!(p.properties.message_expiry_interval(), Some(120));
        assert_eq!(&p.topic[..], b"test/expiry");
        assert_eq!(&p.payload[..], b"payload");
    }

    #[test]
    fn test_encode_publish_from_flat_with_zero_adjusted_expiry_omits_property() {
        use bisque_mq::flat::FlatMessageBuilder;

        let flat_bytes = FlatMessageBuilder::new(b"data")
            .routing_key(b"t")
            .ttl_ms(60_000)
            .timestamp(1000)
            .build();

        let flat = FlatMessage::new(&flat_bytes).unwrap();
        let mut buf = BytesMut::new();

        // Zero adjusted expiry should omit the property entirely.
        encode_publish_from_flat_with_expiry(
            &flat,
            QoS::AtMostOnce,
            false,
            false,
            None,
            true,
            None,
            None,
            Some(0),
            &mut buf,
        );

        let (p, _) = decode_buf::publish_v5(&buf).unwrap();
        assert!(p.properties.message_expiry_interval().is_none());
    }

    #[test]
    fn test_encode_publish_from_flat_no_adjusted_expiry_uses_original() {
        use bisque_mq::flat::FlatMessageBuilder;

        let flat_bytes = FlatMessageBuilder::new(b"data")
            .routing_key(b"t")
            .ttl_ms(60_000)
            .timestamp(1000)
            .build();

        let flat = FlatMessage::new(&flat_bytes).unwrap();
        let mut buf = BytesMut::new();

        // No adjusted expiry → should use the FlatMessage's ttl_ms (60s).
        encode_publish_from_flat_with_expiry(
            &flat,
            QoS::AtMostOnce,
            false,
            false,
            None,
            true,
            None,
            None,
            None, // no override
            &mut buf,
        );

        let (p, _) = decode_buf::publish_v5(&buf).unwrap();
        assert_eq!(p.properties.message_expiry_interval(), Some(60));
    }

    // ---- Empty subscribe/unsubscribe payload rejection ----

    #[test]
    fn test_subscribe_empty_filters_rejected() {
        // SUBSCRIBE with no topic filter/QoS pairs is a protocol violation.
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x00, 0x01]); // packet_id only, no filters

        let raw = build_raw_packet(8, 0x02, &payload);
        let result = decode_buf::subscribe(&raw);
        // Should fail due to no filters (payload exhausted after packet_id).
        assert!(
            result.is_err(),
            "SUBSCRIBE with no filters must be rejected"
        );
    }
}
