//! MQTT binary codec — encode and decode MQTT packets from bytes.
//!
//! Implements the MQTT 3.1.1 / 5.0 binary wire protocol including:
//! - Fixed header parsing with packet type and flags
//! - Variable-length remaining length encoding/decoding (up to 4 bytes)
//! - Per-packet-type serialization and deserialization
//! - MQTT 5.0 property parsing

use bisque_mq::flat::FlatMessage;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use smallvec::SmallVec;

use crate::types::{
    Auth, ConnAck, Connect, ConnectFlags, Disconnect, MqttPacket, PacketType, Properties,
    ProtocolVersion, PubAck, PubComp, PubRec, PubRel, Publish, QoS, SubAck, Subscribe, TopicFilter,
    UnsubAck, Unsubscribe, WillMessage,
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
    #[error("invalid protocol name: {0}")]
    InvalidProtocolName(String),
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
        _ => Ok(()), // Unknown types handled elsewhere
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

/// Read an MQTT UTF-8 encoded string (2-byte length prefix + UTF-8 data).
fn read_mqtt_string(buf: &mut &[u8]) -> Result<String, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(CodecError::UnexpectedEof);
    }
    let data = &buf[..len];
    let s = std::str::from_utf8(data).map_err(|_| CodecError::InvalidUtf8)?;
    validate_mqtt_utf8(s)?;
    let result = s.to_string();
    buf.advance(len);
    Ok(result)
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

/// Read MQTT binary data (2-byte length prefix + raw bytes).
fn read_mqtt_bytes(buf: &mut &[u8]) -> Result<Bytes, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(CodecError::UnexpectedEof);
    }
    let data = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(data)
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
fn read_variable_int(buf: &mut &[u8]) -> Result<u32, CodecError> {
    let mut multiplier: u32 = 1;
    let mut value: u32 = 0;

    for _ in 0..4 {
        if !buf.has_remaining() {
            return Err(CodecError::UnexpectedEof);
        }
        let byte = buf.get_u8();
        value += (byte as u32 & 0x7F) * multiplier;
        if byte & 0x80 == 0 {
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

/// Read MQTT 5.0 properties from a buffer.
fn read_properties(buf: &mut &[u8]) -> Result<Properties, CodecError> {
    let prop_len = read_variable_int(buf)? as usize;
    if buf.remaining() < prop_len {
        return Err(CodecError::UnexpectedEof);
    }

    let mut props = Properties::default();
    let mut prop_buf = &buf[..prop_len];
    *buf = &buf[prop_len..];

    // Bitmask for duplicate property detection.
    // Property IDs range from 0x01 to 0x2A (42), all fit in u64.
    // User Property (0x26) is exempt — can appear multiple times.
    let mut seen: u64 = 0;

    while prop_buf.has_remaining() {
        let id = prop_buf.get_u8();

        // Check for duplicate properties (User Property 0x26 is exempt).
        if id != 0x26 && id < 64 {
            let bit = 1u64 << id;
            if seen & bit != 0 {
                return Err(CodecError::DuplicateProperty(id));
            }
            seen |= bit;
        }

        match id {
            0x01 => {
                // Payload Format Indicator
                props.payload_format_indicator = Some(prop_buf.get_u8());
            }
            0x02 => {
                // Message Expiry Interval
                props.message_expiry_interval = Some(prop_buf.get_u32());
            }
            0x03 => {
                // Content Type
                props.content_type = Some(read_mqtt_string(&mut prop_buf)?);
            }
            0x08 => {
                // Response Topic
                props.response_topic = Some(read_mqtt_string(&mut prop_buf)?);
            }
            0x09 => {
                // Correlation Data
                props.correlation_data = Some(read_mqtt_bytes(&mut prop_buf)?);
            }
            0x0B => {
                // Subscription Identifier
                let val = read_variable_int(&mut prop_buf)?;
                if val == 0 {
                    return Err(CodecError::InvalidPropertyValue(0x0B));
                }
                props.subscription_identifier = Some(val);
            }
            0x11 => {
                // Session Expiry Interval
                props.session_expiry_interval = Some(prop_buf.get_u32());
            }
            0x12 => {
                // Assigned Client Identifier
                props.assigned_client_identifier = Some(read_mqtt_string(&mut prop_buf)?);
            }
            0x13 => {
                // Server Keep Alive
                props.server_keep_alive = Some(prop_buf.get_u16());
            }
            0x15 => {
                // Authentication Method
                props.authentication_method = Some(read_mqtt_string(&mut prop_buf)?);
            }
            0x16 => {
                // Authentication Data
                props.authentication_data = Some(read_mqtt_bytes(&mut prop_buf)?);
            }
            0x17 => {
                // Request Problem Information
                props.request_problem_information = Some(prop_buf.get_u8());
            }
            0x18 => {
                // Will Delay Interval
                props.will_delay_interval = Some(prop_buf.get_u32());
            }
            0x19 => {
                // Request Response Information
                props.request_response_information = Some(prop_buf.get_u8());
            }
            0x1A => {
                // Response Information
                props.response_information = Some(read_mqtt_string(&mut prop_buf)?);
            }
            0x1C => {
                // Server Reference
                props.server_reference = Some(read_mqtt_string(&mut prop_buf)?);
            }
            0x1F => {
                // Reason String
                props.reason_string = Some(read_mqtt_string(&mut prop_buf)?);
            }
            0x21 => {
                // Receive Maximum
                let val = prop_buf.get_u16();
                if val == 0 {
                    return Err(CodecError::InvalidPropertyValue(0x21));
                }
                props.receive_maximum = Some(val);
            }
            0x22 => {
                // Topic Alias Maximum
                props.topic_alias_maximum = Some(prop_buf.get_u16());
            }
            0x23 => {
                // Topic Alias
                let val = prop_buf.get_u16();
                if val == 0 {
                    return Err(CodecError::InvalidPropertyValue(0x23));
                }
                props.topic_alias = Some(val);
            }
            0x24 => {
                // Maximum QoS
                props.maximum_qos = Some(prop_buf.get_u8());
            }
            0x25 => {
                // Retain Available
                props.retain_available = Some(prop_buf.get_u8() != 0);
            }
            0x26 => {
                // User Property (key-value pair)
                let key = read_mqtt_string(&mut prop_buf)?;
                let val = read_mqtt_string(&mut prop_buf)?;
                props.user_properties.push((key, val));
            }
            0x27 => {
                // Maximum Packet Size
                props.maximum_packet_size = Some(prop_buf.get_u32());
            }
            0x28 => {
                // Wildcard Subscription Available
                props.wildcard_subscription_available = Some(prop_buf.get_u8() != 0);
            }
            0x29 => {
                // Subscription Identifier Available
                props.subscription_identifier_available = Some(prop_buf.get_u8() != 0);
            }
            0x2A => {
                // Shared Subscription Available
                props.shared_subscription_available = Some(prop_buf.get_u8() != 0);
            }
            _ => {
                return Err(CodecError::InvalidPropertyId(id));
            }
        }
    }

    Ok(props)
}

/// Compute the encoded byte size of MQTT 5.0 properties (content only, excluding
/// the variable-length integer prefix).
fn compute_properties_size(props: &Properties) -> usize {
    let mut size = 0;
    if props.payload_format_indicator.is_some() {
        size += 1 + 1;
    }
    if props.message_expiry_interval.is_some() {
        size += 1 + 4;
    }
    if let Some(ref v) = props.content_type {
        size += 1 + 2 + v.len();
    }
    if let Some(ref v) = props.response_topic {
        size += 1 + 2 + v.len();
    }
    if let Some(ref v) = props.correlation_data {
        size += 1 + 2 + v.len();
    }
    if let Some(v) = props.subscription_identifier {
        size += 1 + variable_int_size(v);
    }
    if props.session_expiry_interval.is_some() {
        size += 1 + 4;
    }
    if let Some(ref v) = props.assigned_client_identifier {
        size += 1 + 2 + v.len();
    }
    if props.server_keep_alive.is_some() {
        size += 1 + 2;
    }
    if let Some(ref v) = props.authentication_method {
        size += 1 + 2 + v.len();
    }
    if let Some(ref v) = props.authentication_data {
        size += 1 + 2 + v.len();
    }
    if props.request_problem_information.is_some() {
        size += 1 + 1;
    }
    if props.will_delay_interval.is_some() {
        size += 1 + 4;
    }
    if props.request_response_information.is_some() {
        size += 1 + 1;
    }
    if let Some(ref v) = props.response_information {
        size += 1 + 2 + v.len();
    }
    if let Some(ref v) = props.server_reference {
        size += 1 + 2 + v.len();
    }
    if let Some(ref v) = props.reason_string {
        size += 1 + 2 + v.len();
    }
    if props.receive_maximum.is_some() {
        size += 1 + 2;
    }
    if props.topic_alias_maximum.is_some() {
        size += 1 + 2;
    }
    if props.topic_alias.is_some() {
        size += 1 + 2;
    }
    if props.maximum_qos.is_some() {
        size += 1 + 1;
    }
    if props.retain_available.is_some() {
        size += 1 + 1;
    }
    for (key, val) in &props.user_properties {
        size += 1 + 2 + key.len() + 2 + val.len();
    }
    if props.maximum_packet_size.is_some() {
        size += 1 + 4;
    }
    if props.wildcard_subscription_available.is_some() {
        size += 1 + 1;
    }
    if props.subscription_identifier_available.is_some() {
        size += 1 + 1;
    }
    if props.shared_subscription_available.is_some() {
        size += 1 + 1;
    }
    size
}

/// Total encoded size of properties including the variable-length prefix.
#[inline]
fn properties_wire_size(props: &Properties) -> usize {
    let content_size = compute_properties_size(props);
    variable_int_size(content_size as u32) + content_size
}

/// Write MQTT 5.0 properties directly into the output buffer (no intermediate allocation).
fn write_properties(props: &Properties, buf: &mut BytesMut) {
    let content_size = compute_properties_size(props);
    write_variable_int(content_size as u32, buf);

    if let Some(v) = props.payload_format_indicator {
        buf.put_u8(0x01);
        buf.put_u8(v);
    }
    if let Some(v) = props.message_expiry_interval {
        buf.put_u8(0x02);
        buf.put_u32(v);
    }
    if let Some(ref v) = props.content_type {
        buf.put_u8(0x03);
        write_mqtt_string(v, buf);
    }
    if let Some(ref v) = props.response_topic {
        buf.put_u8(0x08);
        write_mqtt_string(v, buf);
    }
    if let Some(ref v) = props.correlation_data {
        buf.put_u8(0x09);
        write_mqtt_bytes(v, buf);
    }
    if let Some(v) = props.subscription_identifier {
        buf.put_u8(0x0B);
        write_variable_int(v, buf);
    }
    if let Some(v) = props.session_expiry_interval {
        buf.put_u8(0x11);
        buf.put_u32(v);
    }
    if let Some(ref v) = props.assigned_client_identifier {
        buf.put_u8(0x12);
        write_mqtt_string(v, buf);
    }
    if let Some(v) = props.server_keep_alive {
        buf.put_u8(0x13);
        buf.put_u16(v);
    }
    if let Some(ref v) = props.authentication_method {
        buf.put_u8(0x15);
        write_mqtt_string(v, buf);
    }
    if let Some(ref v) = props.authentication_data {
        buf.put_u8(0x16);
        write_mqtt_bytes(v, buf);
    }
    if let Some(v) = props.request_problem_information {
        buf.put_u8(0x17);
        buf.put_u8(v);
    }
    if let Some(v) = props.will_delay_interval {
        buf.put_u8(0x18);
        buf.put_u32(v);
    }
    if let Some(v) = props.request_response_information {
        buf.put_u8(0x19);
        buf.put_u8(v);
    }
    if let Some(ref v) = props.response_information {
        buf.put_u8(0x1A);
        write_mqtt_string(v, buf);
    }
    if let Some(ref v) = props.server_reference {
        buf.put_u8(0x1C);
        write_mqtt_string(v, buf);
    }
    if let Some(ref v) = props.reason_string {
        buf.put_u8(0x1F);
        write_mqtt_string(v, buf);
    }
    if let Some(v) = props.receive_maximum {
        buf.put_u8(0x21);
        buf.put_u16(v);
    }
    if let Some(v) = props.topic_alias_maximum {
        buf.put_u8(0x22);
        buf.put_u16(v);
    }
    if let Some(v) = props.topic_alias {
        buf.put_u8(0x23);
        buf.put_u16(v);
    }
    if let Some(v) = props.maximum_qos {
        buf.put_u8(0x24);
        buf.put_u8(v);
    }
    if let Some(v) = props.retain_available {
        buf.put_u8(0x25);
        buf.put_u8(v as u8);
    }
    for (key, val) in &props.user_properties {
        buf.put_u8(0x26);
        write_mqtt_string(key, buf);
        write_mqtt_string(val, buf);
    }
    if let Some(v) = props.maximum_packet_size {
        buf.put_u8(0x27);
        buf.put_u32(v);
    }
    if let Some(v) = props.wildcard_subscription_available {
        buf.put_u8(0x28);
        buf.put_u8(v as u8);
    }
    if let Some(v) = props.subscription_identifier_available {
        buf.put_u8(0x29);
        buf.put_u8(v as u8);
    }
    if let Some(v) = props.shared_subscription_available {
        buf.put_u8(0x2A);
        buf.put_u8(v as u8);
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

/// Attempt to decode a single MQTT packet from the buffer.
///
/// Returns the decoded packet and the total number of bytes consumed.
/// Returns `Err(Incomplete)` if the buffer does not contain a full packet.
pub fn decode_packet(buf: &[u8]) -> Result<(MqttPacket, usize), CodecError> {
    let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;

    // Validate fixed header flags per MQTT spec.
    validate_fixed_header_flags(type_nibble, flags)?;

    let total_size = header_size + remaining_length;
    if buf.len() < total_size {
        return Err(CodecError::Incomplete);
    }

    let payload = &buf[header_size..total_size];
    let mut cursor = payload;

    let packet_type =
        PacketType::from_u8(type_nibble).ok_or(CodecError::UnknownPacketType(type_nibble))?;

    let packet = match packet_type {
        PacketType::Connect => decode_connect(&mut cursor)?,
        PacketType::ConnAck => decode_connack(&mut cursor)?,
        PacketType::Publish => decode_publish(&mut cursor, flags)?,
        PacketType::PubAck => decode_puback(&mut cursor)?,
        PacketType::PubRec => decode_pubrec(&mut cursor)?,
        PacketType::PubRel => decode_pubrel(&mut cursor)?,
        PacketType::PubComp => decode_pubcomp(&mut cursor)?,
        PacketType::Subscribe => decode_subscribe(&mut cursor)?,
        PacketType::SubAck => decode_suback(&mut cursor)?,
        PacketType::Unsubscribe => decode_unsubscribe(&mut cursor)?,
        PacketType::UnsubAck => decode_unsuback(&mut cursor)?,
        PacketType::PingReq => MqttPacket::PingReq,
        PacketType::PingResp => MqttPacket::PingResp,
        PacketType::Disconnect => decode_disconnect(&mut cursor, remaining_length)?,
        PacketType::Auth => decode_auth(&mut cursor, remaining_length)?,
    };

    Ok((packet, total_size))
}

/// Decode a single MQTT packet from a frozen `Bytes` buffer.
///
/// For PUBLISH packets, topic and payload are zero-copy `Bytes::slice()` from
/// the input buffer (refcount bump only, no heap allocation). All other packet
/// types use the standard decode path.
///
/// Returns the decoded packet and the total number of bytes consumed.
pub fn decode_packet_from_bytes(buf: &Bytes) -> Result<(MqttPacket, usize), CodecError> {
    let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;

    // Validate fixed header flags per MQTT spec.
    validate_fixed_header_flags(type_nibble, flags)?;

    let total_size = header_size + remaining_length;
    if buf.len() < total_size {
        return Err(CodecError::Incomplete);
    }

    let packet_type =
        PacketType::from_u8(type_nibble).ok_or(CodecError::UnknownPacketType(type_nibble))?;

    let packet = match packet_type {
        PacketType::Publish => decode_publish_zero_copy(buf, header_size, total_size, flags)?,
        _ => {
            // Non-PUBLISH packets use the standard &[u8] decode path.
            let payload = &buf[header_size..total_size];
            let mut cursor = payload;
            match packet_type {
                PacketType::Connect => decode_connect(&mut cursor)?,
                PacketType::ConnAck => decode_connack(&mut cursor)?,
                PacketType::PubAck => decode_puback(&mut cursor)?,
                PacketType::PubRec => decode_pubrec(&mut cursor)?,
                PacketType::PubRel => decode_pubrel(&mut cursor)?,
                PacketType::PubComp => decode_pubcomp(&mut cursor)?,
                PacketType::Subscribe => decode_subscribe(&mut cursor)?,
                PacketType::SubAck => decode_suback(&mut cursor)?,
                PacketType::Unsubscribe => decode_unsubscribe(&mut cursor)?,
                PacketType::UnsubAck => decode_unsuback(&mut cursor)?,
                PacketType::PingReq => MqttPacket::PingReq,
                PacketType::PingResp => MqttPacket::PingResp,
                PacketType::Disconnect => decode_disconnect(&mut cursor, remaining_length)?,
                PacketType::Auth => decode_auth(&mut cursor, remaining_length)?,
                PacketType::Publish => unreachable!(),
            }
        }
    };

    Ok((packet, total_size))
}

/// Zero-copy PUBLISH decode: topic and payload are `Bytes::slice()` from the input buffer.
fn decode_publish_zero_copy(
    buf: &Bytes,
    start: usize,
    end: usize,
    flags: u8,
) -> Result<MqttPacket, CodecError> {
    decode_publish_zero_copy_v(buf, start, end, flags, false)
}

fn decode_publish_zero_copy_v(
    buf: &Bytes,
    start: usize,
    end: usize,
    flags: u8,
    is_v5: bool,
) -> Result<MqttPacket, CodecError> {
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
    // Zero-copy slice from the input buffer
    let topic = buf.slice(start + pos..start + pos + topic_len);
    pos += topic_len;

    // Packet ID
    let packet_id = if qos != QoS::AtMostOnce {
        if data.len() < pos + 2 {
            return Err(CodecError::UnexpectedEof);
        }
        let id = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;
        Some(id)
    } else {
        None
    };

    // MQTT 5.0: read properties between packet_id and payload.
    let properties = if is_v5 {
        let mut cursor: &[u8] = &data[pos..];
        let props = read_properties(&mut cursor)?;
        // Advance pos by how many bytes read_properties consumed.
        let consumed = (data.len() - pos) - cursor.len();
        pos += consumed;
        props
    } else {
        Properties::default()
    };

    // Payload: zero-copy slice of remaining bytes
    let payload = if pos < data.len() {
        buf.slice(start + pos..end)
    } else {
        Bytes::new()
    };

    Ok(MqttPacket::Publish(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
        properties,
    }))
}

fn decode_connect(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    // Protocol Name
    let protocol_name = read_mqtt_string(buf)?;
    if protocol_name != "MQTT" {
        return Err(CodecError::InvalidProtocolName(protocol_name));
    }

    // Protocol Level
    if !buf.has_remaining() {
        return Err(CodecError::UnexpectedEof);
    }
    let level = buf.get_u8();
    let protocol_version =
        ProtocolVersion::from_level(level).ok_or(CodecError::UnsupportedProtocolVersion(level))?;

    // Connect Flags
    if !buf.has_remaining() {
        return Err(CodecError::UnexpectedEof);
    }
    let flags_byte = buf.get_u8();
    let flags = ConnectFlags::from_byte(flags_byte).ok_or(CodecError::InvalidConnectFlags)?;

    // V3.1.1: password flag set without username flag is invalid.
    if protocol_version == ProtocolVersion::V311 && flags.password && !flags.username {
        return Err(CodecError::InvalidConnectFlags);
    }

    // Keep Alive
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let keep_alive = buf.get_u16();

    // MQTT 5.0 properties
    let properties = if protocol_version == ProtocolVersion::V5 {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    // Payload: Client Identifier
    let client_id = read_mqtt_string(buf)?;

    // Will message (if will flag set)
    let will = if flags.will {
        // MQTT 5.0 will properties
        let will_properties = if protocol_version == ProtocolVersion::V5 {
            read_properties(buf)?
        } else {
            Properties::default()
        };
        let will_topic = read_mqtt_string(buf)?;
        let will_payload = read_mqtt_bytes(buf)?;
        Some(WillMessage {
            topic: will_topic,
            payload: will_payload,
            qos: flags.will_qos,
            retain: flags.will_retain,
            properties: will_properties,
        })
    } else {
        None
    };

    // Username
    let username = if flags.username {
        Some(read_mqtt_string(buf)?)
    } else {
        None
    };

    // Password
    let password = if flags.password {
        Some(read_mqtt_bytes(buf)?)
    } else {
        None
    };

    Ok(MqttPacket::Connect(Connect {
        protocol_name,
        protocol_version,
        flags,
        keep_alive,
        client_id,
        will,
        username,
        password,
        properties,
    }))
}

fn decode_connack(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let ack_flags = buf.get_u8();
    // MQTT 3.1.1 SS 3.2.2.1 / 5.0 SS 3.2.2.1: bits 7-1 are reserved and MUST be 0.
    if ack_flags & 0xFE != 0 {
        return Err(CodecError::InvalidFixedHeaderFlags(2)); // CONNACK packet type
    }
    let session_present = ack_flags & 0x01 != 0;
    let return_code = buf.get_u8();

    // MQTT 5.0 properties (if data remains)
    let properties = if buf.has_remaining() {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    Ok(MqttPacket::ConnAck(ConnAck {
        session_present,
        return_code,
        properties,
    }))
}

fn decode_publish(buf: &mut &[u8], flags: u8) -> Result<MqttPacket, CodecError> {
    decode_publish_v(buf, flags, false)
}

fn decode_publish_v(buf: &mut &[u8], flags: u8, is_v5: bool) -> Result<MqttPacket, CodecError> {
    let dup = flags & 0x08 != 0;
    let qos_val = (flags >> 1) & 0x03;
    let qos = QoS::from_u8(qos_val).ok_or(CodecError::InvalidQoS(qos_val))?;
    let retain = flags & 0x01 != 0;

    // Read topic as Bytes (validate UTF-8 but keep as Bytes).
    let topic = read_mqtt_string_as_bytes(buf)?;

    let packet_id = if qos != QoS::AtMostOnce {
        if buf.remaining() < 2 {
            return Err(CodecError::UnexpectedEof);
        }
        Some(buf.get_u16())
    } else {
        None
    };

    // MQTT 5.0: read properties between packet_id and payload.
    let properties = if is_v5 {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    // Remaining bytes are the payload.
    let payload = Bytes::copy_from_slice(buf.chunk());
    buf.advance(buf.remaining());

    Ok(MqttPacket::Publish(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
        properties,
    }))
}

/// Read an MQTT UTF-8 string and return as `Bytes` (validates UTF-8 but avoids `String` alloc).
fn read_mqtt_string_as_bytes(buf: &mut &[u8]) -> Result<Bytes, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(CodecError::UnexpectedEof);
    }
    let data = &buf[..len];
    let s = std::str::from_utf8(data).map_err(|_| CodecError::InvalidUtf8)?;
    validate_mqtt_utf8(s)?;
    let result = Bytes::copy_from_slice(data);
    buf.advance(len);
    Ok(result)
}

fn decode_puback(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();
    let reason_code = if buf.has_remaining() {
        Some(buf.get_u8())
    } else {
        None
    };
    let properties = if buf.has_remaining() {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    Ok(MqttPacket::PubAck(PubAck {
        packet_id,
        reason_code,
        properties,
    }))
}

fn decode_pubrec(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();
    let reason_code = if buf.has_remaining() {
        Some(buf.get_u8())
    } else {
        None
    };
    let properties = if buf.has_remaining() {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    Ok(MqttPacket::PubRec(PubRec {
        packet_id,
        reason_code,
        properties,
    }))
}

fn decode_pubrel(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();
    let reason_code = if buf.has_remaining() {
        Some(buf.get_u8())
    } else {
        None
    };
    let properties = if buf.has_remaining() {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    Ok(MqttPacket::PubRel(PubRel {
        packet_id,
        reason_code,
        properties,
    }))
}

fn decode_pubcomp(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();
    let reason_code = if buf.has_remaining() {
        Some(buf.get_u8())
    } else {
        None
    };
    let properties = if buf.has_remaining() {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    Ok(MqttPacket::PubComp(PubComp {
        packet_id,
        reason_code,
        properties,
    }))
}

fn decode_subscribe(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    decode_subscribe_v(buf, false)
}

fn decode_subscribe_v(buf: &mut &[u8], is_v5: bool) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    // MQTT 5.0 properties sit between packet_id and topic filters.
    let properties = if is_v5 {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    let mut filters = SmallVec::new();
    while buf.has_remaining() {
        let filter = read_mqtt_string(buf)?;
        if !buf.has_remaining() {
            return Err(CodecError::UnexpectedEof);
        }
        let options_byte = buf.get_u8();
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

    Ok(MqttPacket::Subscribe(Subscribe {
        packet_id,
        filters,
        properties,
    }))
}

fn decode_suback(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    decode_suback_v(buf, false)
}

fn decode_suback_v(buf: &mut &[u8], is_v5: bool) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    let properties = if is_v5 {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    let mut return_codes = SmallVec::new();
    while buf.has_remaining() {
        return_codes.push(buf.get_u8());
    }

    Ok(MqttPacket::SubAck(SubAck {
        packet_id,
        return_codes,
        properties,
    }))
}

fn decode_unsubscribe(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    decode_unsubscribe_v(buf, false)
}

fn decode_unsubscribe_v(buf: &mut &[u8], is_v5: bool) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    let properties = if is_v5 {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    let mut filters = SmallVec::new();
    while buf.has_remaining() {
        filters.push(read_mqtt_string(buf)?);
    }

    Ok(MqttPacket::Unsubscribe(Unsubscribe {
        packet_id,
        filters,
        properties,
    }))
}

fn decode_unsuback(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    decode_unsuback_v(buf, false)
}

fn decode_unsuback_v(buf: &mut &[u8], is_v5: bool) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    let properties = if is_v5 {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    let mut reason_codes = SmallVec::new();
    while buf.has_remaining() {
        reason_codes.push(buf.get_u8());
    }

    Ok(MqttPacket::UnsubAck(UnsubAck {
        packet_id,
        reason_codes,
        properties,
    }))
}

fn decode_disconnect(buf: &mut &[u8], remaining: usize) -> Result<MqttPacket, CodecError> {
    if remaining == 0 {
        return Ok(MqttPacket::Disconnect(Disconnect {
            reason_code: None,
            properties: Properties::default(),
        }));
    }

    let reason_code = if buf.has_remaining() {
        Some(buf.get_u8())
    } else {
        None
    };

    let properties = if buf.has_remaining() {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    Ok(MqttPacket::Disconnect(Disconnect {
        reason_code,
        properties,
    }))
}

/// Decode an AUTH packet (MQTT 5.0 only, SS 3.15).
fn decode_auth(buf: &mut &[u8], remaining: usize) -> Result<MqttPacket, CodecError> {
    if remaining == 0 {
        // Remaining length 0 ⇒ reason code 0x00 (Success), no properties.
        return Ok(MqttPacket::Auth(Auth {
            reason_code: Auth::SUCCESS,
            properties: Properties::default(),
        }));
    }

    let reason_code = buf.get_u8();

    let properties = if remaining > 1 && buf.has_remaining() {
        read_properties(buf)?
    } else {
        Properties::default()
    };

    Ok(MqttPacket::Auth(Auth {
        reason_code,
        properties,
    }))
}

// =============================================================================
// Packet Encoding
// =============================================================================

/// Encode an MQTT packet into a byte buffer.
pub fn encode_packet(packet: &MqttPacket, buf: &mut BytesMut) {
    match packet {
        MqttPacket::Connect(connect) => encode_connect(connect, buf),
        MqttPacket::ConnAck(connack) => encode_connack(connack, buf),
        MqttPacket::Publish(publish) => encode_publish(publish, buf),
        MqttPacket::PubAck(puback) => encode_puback(puback, buf),
        MqttPacket::PubRec(pubrec) => encode_pubrec(pubrec, buf),
        MqttPacket::PubRel(pubrel) => encode_pubrel(pubrel, buf),
        MqttPacket::PubComp(pubcomp) => encode_pubcomp(pubcomp, buf),
        MqttPacket::Subscribe(subscribe) => encode_subscribe(subscribe, buf),
        MqttPacket::SubAck(suback) => encode_suback(suback, buf),
        MqttPacket::Unsubscribe(unsubscribe) => encode_unsubscribe(unsubscribe, buf),
        MqttPacket::UnsubAck(unsuback) => encode_unsuback(unsuback, buf),
        MqttPacket::PingReq => encode_ping_req(buf),
        MqttPacket::PingResp => encode_ping_resp(buf),
        MqttPacket::Disconnect(disconnect) => encode_disconnect(disconnect, buf),
        MqttPacket::Auth(auth) => encode_auth(auth, buf),
    }
}

/// Decode a single MQTT packet with version awareness.
///
/// For V5, this correctly parses properties in PUBLISH, SUBSCRIBE, SUBACK,
/// UNSUBSCRIBE, and UNSUBACK packets. For V3.1.1/V3.1, behaves identically
/// to `decode_packet`.
pub fn decode_packet_versioned(
    buf: &[u8],
    version: ProtocolVersion,
) -> Result<(MqttPacket, usize), CodecError> {
    let is_v5 = version == ProtocolVersion::V5;
    if !is_v5 {
        return decode_packet(buf);
    }

    let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;

    // Validate fixed header flags per MQTT spec.
    validate_fixed_header_flags(type_nibble, flags)?;

    let total_size = header_size + remaining_length;
    if buf.len() < total_size {
        return Err(CodecError::Incomplete);
    }

    let payload = &buf[header_size..total_size];
    let mut cursor = payload;

    let packet_type =
        PacketType::from_u8(type_nibble).ok_or(CodecError::UnknownPacketType(type_nibble))?;

    let packet = match packet_type {
        PacketType::Connect => decode_connect(&mut cursor)?,
        PacketType::ConnAck => decode_connack(&mut cursor)?,
        PacketType::Publish => decode_publish_v(&mut cursor, flags, true)?,
        PacketType::PubAck => decode_puback(&mut cursor)?,
        PacketType::PubRec => decode_pubrec(&mut cursor)?,
        PacketType::PubRel => decode_pubrel(&mut cursor)?,
        PacketType::PubComp => decode_pubcomp(&mut cursor)?,
        PacketType::Subscribe => decode_subscribe_v(&mut cursor, true)?,
        PacketType::SubAck => decode_suback_v(&mut cursor, true)?,
        PacketType::Unsubscribe => decode_unsubscribe_v(&mut cursor, true)?,
        PacketType::UnsubAck => decode_unsuback_v(&mut cursor, true)?,
        PacketType::PingReq => MqttPacket::PingReq,
        PacketType::PingResp => MqttPacket::PingResp,
        PacketType::Disconnect => decode_disconnect(&mut cursor, remaining_length)?,
        PacketType::Auth => decode_auth(&mut cursor, remaining_length)?,
    };

    Ok((packet, total_size))
}

/// Decode a single MQTT packet from a frozen `Bytes` buffer with version awareness.
///
/// For PUBLISH packets, topic and payload are zero-copy `Bytes::slice()` from
/// the input buffer. For V5, properties are correctly parsed.
pub fn decode_packet_from_bytes_versioned(
    buf: &Bytes,
    version: ProtocolVersion,
) -> Result<(MqttPacket, usize), CodecError> {
    let is_v5 = version == ProtocolVersion::V5;
    if !is_v5 {
        return decode_packet_from_bytes(buf);
    }

    let (type_nibble, flags, remaining_length, header_size) = parse_fixed_header(buf)?;

    // Validate fixed header flags per MQTT spec.
    validate_fixed_header_flags(type_nibble, flags)?;

    let total_size = header_size + remaining_length;
    if buf.len() < total_size {
        return Err(CodecError::Incomplete);
    }

    let packet_type =
        PacketType::from_u8(type_nibble).ok_or(CodecError::UnknownPacketType(type_nibble))?;

    let packet = match packet_type {
        PacketType::Publish => {
            decode_publish_zero_copy_v(buf, header_size, total_size, flags, true)?
        }
        _ => {
            let payload = &buf[header_size..total_size];
            let mut cursor = payload;
            match packet_type {
                PacketType::Connect => decode_connect(&mut cursor)?,
                PacketType::ConnAck => decode_connack(&mut cursor)?,
                PacketType::PubAck => decode_puback(&mut cursor)?,
                PacketType::PubRec => decode_pubrec(&mut cursor)?,
                PacketType::PubRel => decode_pubrel(&mut cursor)?,
                PacketType::PubComp => decode_pubcomp(&mut cursor)?,
                PacketType::Subscribe => decode_subscribe_v(&mut cursor, true)?,
                PacketType::SubAck => decode_suback_v(&mut cursor, true)?,
                PacketType::Unsubscribe => decode_unsubscribe_v(&mut cursor, true)?,
                PacketType::UnsubAck => decode_unsuback_v(&mut cursor, true)?,
                PacketType::PingReq => MqttPacket::PingReq,
                PacketType::PingResp => MqttPacket::PingResp,
                PacketType::Disconnect => decode_disconnect(&mut cursor, remaining_length)?,
                PacketType::Auth => decode_auth(&mut cursor, remaining_length)?,
                PacketType::Publish => unreachable!(),
            }
        }
    };

    Ok((packet, total_size))
}

/// Encode an MQTT packet with version awareness.
///
/// For V5, this correctly writes properties in CONNACK, PUBLISH, PUBACK/PUBREC/PUBREL/PUBCOMP,
/// SUBSCRIBE, SUBACK, UNSUBSCRIBE, and UNSUBACK packets. For V3.1.1, behaves identically
/// to `encode_packet`.
pub fn encode_packet_versioned(packet: &MqttPacket, version: ProtocolVersion, buf: &mut BytesMut) {
    let is_v5 = version == ProtocolVersion::V5;
    if !is_v5 {
        return encode_packet(packet, buf);
    }

    match packet {
        MqttPacket::Connect(connect) => encode_connect(connect, buf),
        MqttPacket::ConnAck(connack) => encode_connack_v(connack, buf, true),
        MqttPacket::Publish(publish) => encode_publish_v(publish, buf, true),
        MqttPacket::PubAck(puback) => encode_puback_v(puback, buf, true),
        MqttPacket::PubRec(pubrec) => encode_pubrec_v(pubrec, buf, true),
        MqttPacket::PubRel(pubrel) => encode_pubrel_v(pubrel, buf, true),
        MqttPacket::PubComp(pubcomp) => encode_pubcomp_v(pubcomp, buf, true),
        MqttPacket::Subscribe(subscribe) => encode_subscribe_v(subscribe, buf, true),
        MqttPacket::SubAck(suback) => encode_suback_v(suback, buf, true),
        MqttPacket::Unsubscribe(unsubscribe) => encode_unsubscribe_v(unsubscribe, buf, true),
        MqttPacket::UnsubAck(unsuback) => encode_unsuback_v(unsuback, buf, true),
        MqttPacket::PingReq => encode_ping_req(buf),
        MqttPacket::PingResp => encode_ping_resp(buf),
        MqttPacket::Disconnect(disconnect) => encode_disconnect(disconnect, buf),
        MqttPacket::Auth(auth) => encode_auth(auth, buf),
    }
}

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
    let topic = flat_msg.routing_key().unwrap_or_default();
    let payload = flat_msg.value();

    // For existing topic aliases, send empty topic (m7 optimization).
    let send_empty_topic = matches!(topic_alias_info, Some((_, false)));
    let topic_alias = topic_alias_info.map(|(alias, _)| alias);

    // Compute MQTT 5.0 properties size.
    let props_content_size = if is_v5 {
        compute_flat_properties_size(flat_msg, subscription_id, topic_alias)
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
        write_flat_properties(flat_msg, subscription_id, topic_alias, buf);
    }

    // Payload (from FlatMessage value span — zero-copy from mmap).
    buf.extend_from_slice(&payload);
}

/// Compute the encoded byte size of MQTT 5.0 properties extracted from a FlatMessage.
fn compute_flat_properties_size(
    flat_msg: &FlatMessage,
    subscription_id: Option<u32>,
    topic_alias: Option<u16>,
) -> usize {
    let mut size = 0;

    // message_expiry_interval (0x02): 1 + 4
    if let Some(ttl_ms) = flat_msg.ttl_ms() {
        if ttl_ms / 1000 > 0 {
            size += 1 + 4;
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
    buf: &mut BytesMut,
) {
    // payload_format_indicator (0x01) — write early per MQTT convention
    // (handled below in header loop to avoid double iteration)

    // message_expiry_interval (0x02)
    if let Some(ttl_ms) = flat_msg.ttl_ms() {
        let secs = (ttl_ms / 1000) as u32;
        if secs > 0 {
            buf.put_u8(0x02);
            buf.put_u32(secs);
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

fn encode_connect(connect: &Connect, buf: &mut BytesMut) {
    // Pre-compute remaining length to avoid intermediate BytesMut.
    let mut remaining = 0usize;

    // Variable header: protocol name + level + flags + keep_alive
    remaining += 2 + connect.protocol_name.len() + 1 + 1 + 2;

    // MQTT 5.0 connect properties
    let connect_props_size = if connect.protocol_version == ProtocolVersion::V5 {
        properties_wire_size(&connect.properties)
    } else {
        0
    };
    remaining += connect_props_size;

    // Payload: client_id
    remaining += 2 + connect.client_id.len();

    // Will
    if let Some(ref will) = connect.will {
        if connect.protocol_version == ProtocolVersion::V5 {
            remaining += properties_wire_size(&will.properties);
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
    write_mqtt_string(&connect.protocol_name, buf);
    buf.put_u8(connect.protocol_version.level());
    buf.put_u8(connect.flags.to_byte());
    buf.put_u16(connect.keep_alive);

    if connect.protocol_version == ProtocolVersion::V5 {
        write_properties(&connect.properties, buf);
    }

    // Payload
    write_mqtt_string(&connect.client_id, buf);

    if let Some(ref will) = connect.will {
        if connect.protocol_version == ProtocolVersion::V5 {
            write_properties(&will.properties, buf);
        }
        write_mqtt_string(&will.topic, buf);
        write_mqtt_bytes(&will.payload, buf);
    }

    if let Some(ref username) = connect.username {
        write_mqtt_string(username, buf);
    }

    if let Some(ref password) = connect.password {
        write_mqtt_bytes(password, buf);
    }
}

fn encode_connack(connack: &ConnAck, buf: &mut BytesMut) {
    encode_connack_v(connack, buf, false)
}

fn encode_connack_v(connack: &ConnAck, buf: &mut BytesMut, is_v5: bool) {
    // Pre-compute remaining length: ack_flags(1) + return_code(1) + optional V5 properties.
    let props_size = properties_wire_size(&connack.properties);
    let has_props = compute_properties_size(&connack.properties) > 0;
    // For V5, ALWAYS include property length (even if 0). For V3, only if non-empty.
    let include_props = is_v5 || has_props;
    let remaining = 2 + if include_props { props_size } else { 0 };

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0x20); // CONNACK = 2 << 4
    encode_remaining_length(remaining, buf);
    buf.put_u8(if connack.session_present { 0x01 } else { 0x00 });
    buf.put_u8(connack.return_code);

    if include_props {
        write_properties(&connack.properties, buf);
    }
}

fn encode_publish(publish: &Publish, buf: &mut BytesMut) {
    encode_publish_v(publish, buf, false)
}

fn encode_publish_v(publish: &Publish, buf: &mut BytesMut, is_v5: bool) {
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
    buf.extend_from_slice(&publish.topic);

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
    buf.extend_from_slice(&publish.payload);
}

fn encode_puback(puback: &PubAck, buf: &mut BytesMut) {
    encode_puback_v(puback, buf, false)
}

fn encode_puback_v(puback: &PubAck, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x40,
        puback.packet_id,
        puback.reason_code,
        &puback.properties,
        buf,
        is_v5,
    );
}

fn encode_pubrec(pubrec: &PubRec, buf: &mut BytesMut) {
    encode_pubrec_v(pubrec, buf, false)
}

fn encode_pubrec_v(pubrec: &PubRec, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x50,
        pubrec.packet_id,
        pubrec.reason_code,
        &pubrec.properties,
        buf,
        is_v5,
    );
}

fn encode_pubrel(pubrel: &PubRel, buf: &mut BytesMut) {
    encode_pubrel_v(pubrel, buf, false)
}

fn encode_pubrel_v(pubrel: &PubRel, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x62,
        pubrel.packet_id,
        pubrel.reason_code,
        &pubrel.properties,
        buf,
        is_v5,
    );
}

fn encode_pubcomp(pubcomp: &PubComp, buf: &mut BytesMut) {
    encode_pubcomp_v(pubcomp, buf, false)
}

fn encode_pubcomp_v(pubcomp: &PubComp, buf: &mut BytesMut, is_v5: bool) {
    encode_pub_ack_common(
        0x70,
        pubcomp.packet_id,
        pubcomp.reason_code,
        &pubcomp.properties,
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
    properties: &Properties,
    buf: &mut BytesMut,
    is_v5: bool,
) {
    if !is_v5 {
        buf.put_u8(first_byte);
        encode_remaining_length(2, buf);
        buf.put_u16(packet_id);
        return;
    }

    // V5 path: determine what to include.
    let props_content_size = compute_properties_size(properties);
    let has_props = props_content_size > 0;
    let rc = reason_code.unwrap_or(0x00);

    if rc == 0x00 && !has_props {
        // Can omit reason_code and properties — just packet_id.
        buf.put_u8(first_byte);
        encode_remaining_length(2, buf);
        buf.put_u16(packet_id);
    } else if !has_props {
        // Reason code only, no properties.
        buf.put_u8(first_byte);
        encode_remaining_length(3, buf);
        buf.put_u16(packet_id);
        buf.put_u8(rc);
    } else {
        // Reason code + properties.
        let props_wire = variable_int_size(props_content_size as u32) + props_content_size;
        let remaining = 2 + 1 + props_wire;
        buf.put_u8(first_byte);
        encode_remaining_length(remaining, buf);
        buf.put_u16(packet_id);
        buf.put_u8(rc);
        write_properties(properties, buf);
    }
}

fn encode_subscribe(subscribe: &Subscribe, buf: &mut BytesMut) {
    encode_subscribe_v(subscribe, buf, false)
}

fn encode_subscribe_v(subscribe: &Subscribe, buf: &mut BytesMut, is_v5: bool) {
    // Pre-compute remaining length: packet_id(2) + [V5: properties] + sum(2 + filter_len + 1) per filter.
    let props_total_size = if is_v5 {
        properties_wire_size(&subscribe.properties)
    } else {
        0
    };
    let mut remaining = 2usize + props_total_size;
    for filter in &subscribe.filters {
        remaining += 2 + filter.filter.len() + 1;
    }

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0x82); // SUBSCRIBE = 8 << 4 | 0x02
    encode_remaining_length(remaining, buf);
    buf.put_u16(subscribe.packet_id);

    if is_v5 {
        write_properties(&subscribe.properties, buf);
    }

    for filter in &subscribe.filters {
        write_mqtt_string(&filter.filter, buf);
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

fn encode_suback(suback: &SubAck, buf: &mut BytesMut) {
    encode_suback_v(suback, buf, false)
}

fn encode_suback_v(suback: &SubAck, buf: &mut BytesMut, is_v5: bool) {
    // Pre-compute: packet_id(2) + [V5: properties] + return_codes.
    let props_total_size = if is_v5 {
        properties_wire_size(&suback.properties)
    } else {
        0
    };
    let remaining = 2 + props_total_size + suback.return_codes.len();

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0x90); // SUBACK = 9 << 4
    encode_remaining_length(remaining, buf);
    buf.put_u16(suback.packet_id);
    if is_v5 {
        write_properties(&suback.properties, buf);
    }
    buf.extend_from_slice(&suback.return_codes);
}

fn encode_unsubscribe(unsubscribe: &Unsubscribe, buf: &mut BytesMut) {
    encode_unsubscribe_v(unsubscribe, buf, false)
}

fn encode_unsubscribe_v(unsubscribe: &Unsubscribe, buf: &mut BytesMut, is_v5: bool) {
    // Pre-compute: packet_id(2) + [V5: properties] + sum(2 + filter_len) per filter.
    let props_total_size = if is_v5 {
        properties_wire_size(&unsubscribe.properties)
    } else {
        0
    };
    let mut remaining = 2usize + props_total_size;
    for filter in &unsubscribe.filters {
        remaining += 2 + filter.len();
    }

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0xA2); // UNSUBSCRIBE = 10 << 4 | 0x02
    encode_remaining_length(remaining, buf);
    buf.put_u16(unsubscribe.packet_id);
    if is_v5 {
        write_properties(&unsubscribe.properties, buf);
    }
    for filter in &unsubscribe.filters {
        write_mqtt_string(filter, buf);
    }
}

fn encode_unsuback(unsuback: &UnsubAck, buf: &mut BytesMut) {
    encode_unsuback_v(unsuback, buf, false)
}

fn encode_unsuback_v(unsuback: &UnsubAck, buf: &mut BytesMut, is_v5: bool) {
    // Pre-compute: packet_id(2) + [V5: properties] + reason_codes.
    let props_total_size = if is_v5 {
        properties_wire_size(&unsuback.properties)
    } else {
        0
    };
    let remaining = 2 + props_total_size + unsuback.reason_codes.len();

    buf.reserve(1 + 4 + remaining);
    buf.put_u8(0xB0); // UNSUBACK = 11 << 4
    encode_remaining_length(remaining, buf);
    buf.put_u16(unsuback.packet_id);
    if is_v5 {
        write_properties(&unsuback.properties, buf);
    }
    buf.extend_from_slice(&unsuback.reason_codes);
}

fn encode_ping_req(buf: &mut BytesMut) {
    buf.put_u8(0xC0); // PINGREQ = 12 << 4
    buf.put_u8(0x00);
}

fn encode_ping_resp(buf: &mut BytesMut) {
    buf.put_u8(0xD0); // PINGRESP = 13 << 4
    buf.put_u8(0x00);
}

fn encode_disconnect(disconnect: &Disconnect, buf: &mut BytesMut) {
    if disconnect.reason_code.is_none() {
        // MQTT 3.1.1 DISCONNECT: no variable header.
        buf.put_u8(0xE0);
        buf.put_u8(0x00);
    } else {
        // MQTT 5.0: reason_code(1) + optional properties.
        let props_content_size = compute_properties_size(&disconnect.properties);
        let has_props = props_content_size > 0;
        let remaining = 1 + if has_props {
            variable_int_size(props_content_size as u32) + props_content_size
        } else {
            0
        };

        buf.reserve(1 + 4 + remaining);
        buf.put_u8(0xE0);
        encode_remaining_length(remaining, buf);
        buf.put_u8(disconnect.reason_code.unwrap());
        if has_props {
            write_properties(&disconnect.properties, buf);
        }
    }
}

/// Encode an AUTH packet (MQTT 5.0 only).
fn encode_auth(auth: &Auth, buf: &mut BytesMut) {
    let props_content_size = compute_properties_size(&auth.properties);
    let has_props = props_content_size > 0;

    if auth.reason_code == Auth::SUCCESS && !has_props {
        // Optimisation: reason 0x00 with no properties ⇒ remaining length 0.
        buf.put_u8(0xF0);
        buf.put_u8(0x00);
    } else {
        let remaining = 1 + if has_props {
            variable_int_size(props_content_size as u32) + props_content_size
        } else {
            0
        };

        buf.reserve(1 + 4 + remaining);
        buf.put_u8(0xF0);
        encode_remaining_length(remaining, buf);
        buf.put_u8(auth.reason_code);
        if has_props {
            write_properties(&auth.properties, buf);
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
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
        encode_packet(&MqttPacket::PingReq, &mut buf);
        assert_eq!(&buf[..], &[0xC0, 0x00]);

        let (packet, consumed) = decode_packet(&buf).unwrap();
        assert!(matches!(packet, MqttPacket::PingReq));
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_encode_decode_pingresp() {
        let mut buf = BytesMut::new();
        encode_packet(&MqttPacket::PingResp, &mut buf);
        assert_eq!(&buf[..], &[0xD0, 0x00]);

        let (packet, consumed) = decode_packet(&buf).unwrap();
        assert!(matches!(packet, MqttPacket::PingResp));
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_encode_decode_connack() {
        let connack = MqttPacket::ConnAck(ConnAck {
            session_present: false,
            return_code: 0x00,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&connack, &mut buf);

        let (decoded, consumed) = decode_packet(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::ConnAck(c) => {
                assert!(!c.session_present);
                assert_eq!(c.return_code, 0x00);
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_encode_decode_publish_qos0() {
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic"),
            packet_id: None,
            payload: Bytes::from_static(b"hello"),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&publish, &mut buf);

        let (decoded, consumed) = decode_packet(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Publish(p) => {
                assert!(!p.dup);
                assert_eq!(p.qos, QoS::AtMostOnce);
                assert!(!p.retain);
                assert_eq!(&p.topic[..], b"test/topic");
                assert!(p.packet_id.is_none());
                assert_eq!(p.payload.as_ref(), b"hello");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_encode_decode_publish_qos1() {
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: Bytes::from_static(b"sensor/1/temp"),
            packet_id: Some(42),
            payload: Bytes::from_static(b"22.5"),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&publish, &mut buf);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::Publish(p) => {
                assert_eq!(p.qos, QoS::AtLeastOnce);
                assert!(p.retain);
                assert_eq!(&p.topic[..], b"sensor/1/temp");
                assert_eq!(p.packet_id, Some(42));
                assert_eq!(p.payload.as_ref(), b"22.5");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_decode_packet_from_bytes_zero_copy() {
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"zero/copy/topic"),
            packet_id: None,
            payload: Bytes::from_static(b"payload data"),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&publish, &mut buf);

        // Freeze and use zero-copy decode.
        let frozen = buf.freeze();
        let (decoded, consumed) = decode_packet_from_bytes(&frozen).unwrap();
        assert_eq!(consumed, frozen.len());

        match decoded {
            MqttPacket::Publish(p) => {
                assert_eq!(&p.topic[..], b"zero/copy/topic");
                assert_eq!(&p.payload[..], b"payload data");
                // Verify the topic and payload share the same backing buffer
                // (they are slices of frozen, not independent allocations).
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_encode_publish_from_flat_no_v5() {
        use bisque_mq::flat::FlatMessageBuilder;

        let flat_bytes = FlatMessageBuilder::new(Bytes::from_static(b"sensor reading"))
            .routing_key(Bytes::from_static(b"sensor/1/temp"))
            .build();

        let flat = FlatMessage::new(flat_bytes).unwrap();
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
        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::Publish(p) => {
                assert_eq!(&p.topic[..], b"sensor/1/temp");
                assert_eq!(p.qos, QoS::AtLeastOnce);
                assert_eq!(p.packet_id, Some(42));
                assert_eq!(&p.payload[..], b"sensor reading");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_encode_publish_from_flat_v5_properties() {
        use bisque_mq::flat::FlatMessageBuilder;

        let flat_bytes = FlatMessageBuilder::new(Bytes::from_static(b"data"))
            .routing_key(Bytes::from_static(b"t"))
            .reply_to(Bytes::from_static(b"reply/topic"))
            .correlation_id(Bytes::from_static(b"corr-123"))
            .ttl_ms(60_000)
            .header("mqtt.content_type", &b"application/json"[..])
            .header("mqtt.payload_format", &b"\x01"[..])
            .header("mqtt.user.custom", &b"value"[..])
            .build();

        let flat = FlatMessage::new(flat_bytes).unwrap();
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
        let subscribe = MqttPacket::Subscribe(Subscribe {
            packet_id: 1,
            filters: smallvec![
                TopicFilter {
                    filter: "sensor/+/data".to_string(),
                    qos: QoS::AtLeastOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: 0,
                },
                TopicFilter {
                    filter: "control/#".to_string(),
                    qos: QoS::ExactlyOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: 0,
                },
            ],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&subscribe, &mut buf);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::Subscribe(s) => {
                assert_eq!(s.packet_id, 1);
                assert_eq!(s.filters.len(), 2);
                assert_eq!(s.filters[0].filter, "sensor/+/data");
                assert_eq!(s.filters[0].qos, QoS::AtLeastOnce);
                assert_eq!(s.filters[1].filter, "control/#");
                assert_eq!(s.filters[1].qos, QoS::ExactlyOnce);
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_encode_decode_suback() {
        let suback = MqttPacket::SubAck(SubAck {
            packet_id: 1,
            return_codes: smallvec![0x01, 0x02, 0x80],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&suback, &mut buf);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::SubAck(s) => {
                assert_eq!(s.packet_id, 1);
                assert_eq!(s.return_codes.as_slice(), &[0x01, 0x02, 0x80]);
            }
            _ => panic!("expected SubAck"),
        }
    }

    #[test]
    fn test_encode_decode_unsubscribe() {
        let unsub = MqttPacket::Unsubscribe(Unsubscribe {
            packet_id: 5,
            filters: smallvec!["sensor/+/data".to_string(), "control/#".to_string()],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&unsub, &mut buf);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::Unsubscribe(u) => {
                assert_eq!(u.packet_id, 5);
                assert_eq!(u.filters.len(), 2);
                assert_eq!(u.filters[0], "sensor/+/data");
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[test]
    fn test_encode_decode_disconnect() {
        // MQTT 3.1.1 style (no payload)
        let disconnect = MqttPacket::Disconnect(Disconnect {
            reason_code: None,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&disconnect, &mut buf);
        assert_eq!(&buf[..], &[0xE0, 0x00]);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::Disconnect(d) => {
                assert!(d.reason_code.is_none());
            }
            _ => panic!("expected Disconnect"),
        }
    }

    #[test]
    fn test_encode_decode_connect_v311() {
        let connect = MqttPacket::Connect(Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: "test-client".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&connect, &mut buf);

        let (decoded, consumed) = decode_packet(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Connect(c) => {
                assert_eq!(c.protocol_name, "MQTT");
                assert_eq!(c.protocol_version, ProtocolVersion::V311);
                assert!(c.flags.clean_session);
                assert_eq!(c.keep_alive, 60);
                assert_eq!(c.client_id, "test-client");
                assert!(c.will.is_none());
                assert!(c.username.is_none());
                assert!(c.password.is_none());
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn test_encode_decode_connect_with_will() {
        let connect = MqttPacket::Connect(Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: "will-client".to_string(),
            will: Some(WillMessage {
                topic: "last/will".to_string(),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: Properties::default(),
            }),
            username: Some("user".to_string()),
            password: Some(Bytes::from_static(b"pass")),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&connect, &mut buf);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::Connect(c) => {
                assert_eq!(c.client_id, "will-client");
                assert!(c.flags.will);
                let will = c.will.unwrap();
                assert_eq!(will.topic, "last/will");
                assert_eq!(will.payload.as_ref(), b"offline");
                assert_eq!(c.username.unwrap(), "user");
                assert_eq!(c.password.unwrap().as_ref(), b"pass");
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn test_encode_decode_puback() {
        let puback = MqttPacket::PubAck(PubAck {
            packet_id: 100,
            reason_code: None,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&puback, &mut buf);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::PubAck(p) => {
                assert_eq!(p.packet_id, 100);
            }
            _ => panic!("expected PubAck"),
        }
    }

    #[test]
    fn test_incomplete_data() {
        // Just one byte - not enough for a full packet
        let buf = [0x30]; // PUBLISH type byte only
        assert!(matches!(decode_packet(&buf), Err(CodecError::Incomplete)));

        // Fixed header complete but payload missing
        let buf = [0x30, 0x05]; // PUBLISH with 5 bytes remaining, but no payload
        assert!(matches!(decode_packet(&buf), Err(CodecError::Incomplete)));
    }

    #[test]
    fn test_properties_roundtrip() {
        let props = Properties {
            message_expiry_interval: Some(3600),
            content_type: Some("application/json".to_string()),
            response_topic: Some("reply/to".to_string()),
            user_properties: smallvec![
                ("key1".to_string(), "val1".to_string()),
                ("key2".to_string(), "val2".to_string()),
            ],
            ..Properties::default()
        };

        let mut buf = BytesMut::new();
        write_properties(&props, &mut buf);

        let mut cursor: &[u8] = &buf;
        let decoded = read_properties(&mut cursor).unwrap();
        assert_eq!(decoded.message_expiry_interval, Some(3600));
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.response_topic.as_deref(), Some("reply/to"));
        assert_eq!(decoded.user_properties.len(), 2);
        assert_eq!(decoded.user_properties[0].0, "key1");
        assert_eq!(decoded.user_properties[0].1, "val1");
    }

    // =========================================================================
    // V5 versioned roundtrip tests
    // =========================================================================

    #[test]
    fn test_v5_publish_with_properties_roundtrip() {
        let props = Properties {
            message_expiry_interval: Some(300),
            content_type: Some("application/json".to_string()),
            response_topic: Some("reply/topic".to_string()),
            correlation_data: Some(Bytes::from_static(b"corr-id-1")),
            payload_format_indicator: Some(1),
            ..Properties::default()
        };
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: Bytes::from_static(b"test/v5/topic"),
            packet_id: Some(101),
            payload: Bytes::from_static(b"v5 payload"),
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&publish, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Publish(p) => {
                assert_eq!(&p.topic[..], b"test/v5/topic");
                assert_eq!(p.packet_id, Some(101));
                assert_eq!(&p.payload[..], b"v5 payload");
                assert_eq!(p.properties.message_expiry_interval, Some(300));
                assert_eq!(
                    p.properties.content_type.as_deref(),
                    Some("application/json")
                );
                assert_eq!(p.properties.response_topic.as_deref(), Some("reply/topic"));
                assert_eq!(
                    p.properties.correlation_data.as_deref(),
                    Some(b"corr-id-1".as_slice())
                );
                assert_eq!(p.properties.payload_format_indicator, Some(1));
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_v5_publish_zero_copy_with_properties() {
        let props = Properties {
            topic_alias: Some(5),
            subscription_identifier: Some(42),
            ..Properties::default()
        };
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: Bytes::from_static(b"zc/topic"),
            packet_id: None,
            payload: Bytes::from_static(b"zc payload"),
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&publish, ProtocolVersion::V5, &mut buf);
        let frozen = buf.freeze();

        let (decoded, consumed) =
            decode_packet_from_bytes_versioned(&frozen, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, frozen.len());

        match decoded {
            MqttPacket::Publish(p) => {
                assert_eq!(&p.topic[..], b"zc/topic");
                assert!(p.retain);
                assert_eq!(&p.payload[..], b"zc payload");
                assert_eq!(p.properties.topic_alias, Some(5));
                assert_eq!(p.properties.subscription_identifier, Some(42));
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_v5_subscribe_with_subscription_identifier() {
        let props = Properties {
            subscription_identifier: Some(99),
            ..Properties::default()
        };
        let subscribe = MqttPacket::Subscribe(Subscribe {
            packet_id: 10,
            filters: smallvec![TopicFilter {
                filter: "sensor/#".to_string(),
                qos: QoS::AtLeastOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: 1,
            }],
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&subscribe, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Subscribe(s) => {
                assert_eq!(s.packet_id, 10);
                assert_eq!(s.properties.subscription_identifier, Some(99));
                assert_eq!(s.filters.len(), 1);
                assert_eq!(s.filters[0].filter, "sensor/#");
                assert_eq!(s.filters[0].qos, QoS::AtLeastOnce);
                assert!(s.filters[0].no_local);
                assert!(s.filters[0].retain_as_published);
                assert_eq!(s.filters[0].retain_handling, 1);
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_v5_suback_with_properties() {
        let props = Properties {
            reason_string: Some("all good".to_string()),
            user_properties: smallvec![("key".to_string(), "val".to_string())],
            ..Properties::default()
        };
        let suback = MqttPacket::SubAck(SubAck {
            packet_id: 20,
            return_codes: smallvec![0x00, 0x01, 0x02],
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&suback, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::SubAck(s) => {
                assert_eq!(s.packet_id, 20);
                assert_eq!(s.return_codes.as_slice(), &[0x00, 0x01, 0x02]);
                assert_eq!(s.properties.reason_string.as_deref(), Some("all good"));
                assert_eq!(s.properties.user_properties.len(), 1);
                assert_eq!(s.properties.user_properties[0].0, "key");
            }
            _ => panic!("expected SubAck"),
        }
    }

    #[test]
    fn test_v5_unsubscribe_with_properties() {
        let props = Properties {
            user_properties: smallvec![("trace".to_string(), "abc".to_string())],
            ..Properties::default()
        };
        let unsub = MqttPacket::Unsubscribe(Unsubscribe {
            packet_id: 30,
            filters: smallvec!["sensor/#".to_string(), "cmd/+".to_string()],
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&unsub, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Unsubscribe(u) => {
                assert_eq!(u.packet_id, 30);
                assert_eq!(u.filters.len(), 2);
                assert_eq!(u.filters[0], "sensor/#");
                assert_eq!(u.filters[1], "cmd/+");
                assert_eq!(u.properties.user_properties.len(), 1);
                assert_eq!(u.properties.user_properties[0].0, "trace");
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[test]
    fn test_v5_unsuback_with_properties() {
        let props = Properties {
            reason_string: Some("done".to_string()),
            ..Properties::default()
        };
        let unsuback = MqttPacket::UnsubAck(UnsubAck {
            packet_id: 40,
            reason_codes: smallvec![0x00, 0x11],
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&unsuback, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::UnsubAck(u) => {
                assert_eq!(u.packet_id, 40);
                assert_eq!(u.reason_codes.as_slice(), &[0x00, 0x11]);
                assert_eq!(u.properties.reason_string.as_deref(), Some("done"));
            }
            _ => panic!("expected UnsubAck"),
        }
    }

    #[test]
    fn test_v5_connack_empty_properties_has_property_length() {
        let connack = MqttPacket::ConnAck(ConnAck {
            session_present: false,
            return_code: 0x00,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&connack, ProtocolVersion::V5, &mut buf);

        // V5 CONNACK must include property length byte even if 0.
        // Fixed header: 0x20 + remaining_length + ack_flags(1) + return_code(1) + prop_len(1=0x00)
        // So remaining_length = 3.
        assert_eq!(buf[0], 0x20); // CONNACK type
        assert_eq!(buf[1], 3); // remaining length = 3
        assert_eq!(buf[2], 0x00); // ack_flags
        assert_eq!(buf[3], 0x00); // return_code
        assert_eq!(buf[4], 0x00); // property length = 0

        // Verify it decodes correctly.
        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());
        match decoded {
            MqttPacket::ConnAck(c) => {
                assert!(!c.session_present);
                assert_eq!(c.return_code, 0x00);
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_v5_puback_with_reason_code() {
        let puback = MqttPacket::PubAck(PubAck {
            packet_id: 200,
            reason_code: Some(0x10), // No matching subscribers
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&puback, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::PubAck(p) => {
                assert_eq!(p.packet_id, 200);
                assert_eq!(p.reason_code, Some(0x10));
            }
            _ => panic!("expected PubAck"),
        }
    }

    #[test]
    fn test_v5_puback_success_no_properties_compact() {
        // When reason_code is 0x00 and no properties, V5 encoder can omit them.
        let puback = MqttPacket::PubAck(PubAck {
            packet_id: 300,
            reason_code: Some(0x00),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&puback, ProtocolVersion::V5, &mut buf);

        // Should be compact: type(1) + remaining_len(1) + packet_id(2) = 4 bytes.
        assert_eq!(buf.len(), 4);

        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        match decoded {
            MqttPacket::PubAck(p) => {
                assert_eq!(p.packet_id, 300);
            }
            _ => panic!("expected PubAck"),
        }
    }

    #[test]
    fn test_v5_puback_with_properties() {
        let props = Properties {
            reason_string: Some("quota exceeded".to_string()),
            ..Properties::default()
        };
        let puback = MqttPacket::PubAck(PubAck {
            packet_id: 400,
            reason_code: Some(0x97), // Quota exceeded
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&puback, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::PubAck(p) => {
                assert_eq!(p.packet_id, 400);
                assert_eq!(p.reason_code, Some(0x97));
                assert_eq!(
                    p.properties.reason_string.as_deref(),
                    Some("quota exceeded")
                );
            }
            _ => panic!("expected PubAck"),
        }
    }

    #[test]
    fn test_v5_disconnect_roundtrip() {
        let props = Properties {
            reason_string: Some("server shutting down".to_string()),
            session_expiry_interval: Some(0),
            ..Properties::default()
        };
        let disconnect = MqttPacket::Disconnect(Disconnect {
            reason_code: Some(0x8B), // Server shutting down
            properties: props,
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&disconnect, ProtocolVersion::V5, &mut buf);

        let (decoded, consumed) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Disconnect(d) => {
                assert_eq!(d.reason_code, Some(0x8B));
                assert_eq!(
                    d.properties.reason_string.as_deref(),
                    Some("server shutting down")
                );
                assert_eq!(d.properties.session_expiry_interval, Some(0));
            }
            _ => panic!("expected Disconnect"),
        }
    }

    #[test]
    fn test_v5_pubrel_pubcomp_roundtrip() {
        // PUBREL with reason code
        let pubrel = MqttPacket::PubRel(PubRel {
            packet_id: 500,
            reason_code: Some(0x92), // Packet Identifier not found
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubrel, ProtocolVersion::V5, &mut buf);

        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        match decoded {
            MqttPacket::PubRel(p) => {
                assert_eq!(p.packet_id, 500);
                assert_eq!(p.reason_code, Some(0x92));
            }
            _ => panic!("expected PubRel"),
        }

        // PUBCOMP with success (compact)
        let pubcomp = MqttPacket::PubComp(PubComp {
            packet_id: 501,
            reason_code: Some(0x00),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubcomp, ProtocolVersion::V5, &mut buf);
        assert_eq!(buf.len(), 4); // compact form

        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();
        match decoded {
            MqttPacket::PubComp(p) => {
                assert_eq!(p.packet_id, 501);
            }
            _ => panic!("expected PubComp"),
        }
    }

    #[test]
    fn test_v311_backward_compat_through_versioned_api() {
        // Ensure V3.1.1 through versioned API produces the same output as unversioned.
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: Bytes::from_static(b"compat/test"),
            packet_id: Some(77),
            payload: Bytes::from_static(b"hello"),
            properties: Properties::default(),
        });

        let mut buf_v311 = BytesMut::new();
        encode_packet_versioned(&publish, ProtocolVersion::V311, &mut buf_v311);

        let mut buf_legacy = BytesMut::new();
        encode_packet(&publish, &mut buf_legacy);

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
            decode_packet(&buf),
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
            protocol_name: "MQTT".to_string(),
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
            client_id: "test".to_string(),
            will: None,
            username: None,
            password: Some(Bytes::from_static(b"secret")),
            properties: Properties::default(),
        };
        let mut buf = BytesMut::new();
        encode_packet(&MqttPacket::Connect(connect), &mut buf);

        let result = decode_packet(&buf);
        assert!(matches!(result, Err(CodecError::InvalidConnectFlags)));
    }

    // ---- UTF-8 null byte rejection (m1) ----

    #[test]
    fn test_utf8_null_byte_rejected_in_string() {
        // Construct a raw MQTT string with a null byte inside.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x05]); // length=5
        buf.extend_from_slice(b"he\x00lo"); // contains null byte
        let mut cursor: &[u8] = &buf;
        let result = read_mqtt_string(&mut cursor);
        assert!(matches!(result, Err(CodecError::InvalidUtf8)));
    }

    #[test]
    fn test_utf8_null_byte_rejected_in_string_as_bytes() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x05]);
        buf.extend_from_slice(b"he\x00lo");
        let mut cursor: &[u8] = &buf;
        let result = read_mqtt_string_as_bytes(&mut cursor);
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

        let mut cursor: &[u8] = &buf;
        let result = read_properties(&mut cursor);
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

        let mut cursor: &[u8] = &buf;
        let result = read_properties(&mut cursor);
        assert!(result.is_ok());
        let props = result.unwrap();
        assert_eq!(props.user_properties.len(), 2);
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

        let mut cursor: &[u8] = &buf;
        assert!(matches!(
            read_properties(&mut cursor),
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

        let mut cursor: &[u8] = &buf;
        assert!(matches!(
            read_properties(&mut cursor),
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

        let mut cursor: &[u8] = &buf;
        assert!(matches!(
            read_properties(&mut cursor),
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

        let result = decode_packet(&buf);
        assert!(matches!(result, Err(CodecError::InvalidProtocolName(_))));
    }

    // ---- AUTH packet codec tests ----

    #[test]
    fn test_auth_encode_decode_success_no_props() {
        let auth = Auth {
            reason_code: Auth::SUCCESS,
            properties: Properties::default(),
        };
        let mut buf = BytesMut::new();
        encode_auth(&auth, &mut buf);
        // Success with no props should be 2 bytes: 0xF0, 0x00
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 0xF0);
        assert_eq!(buf[1], 0x00);

        let (pkt, _) = decode_packet(&buf).unwrap();
        match pkt {
            MqttPacket::Auth(decoded) => {
                assert_eq!(decoded.reason_code, Auth::SUCCESS);
            }
            other => panic!("expected Auth, got {:?}", other),
        }
    }

    #[test]
    fn test_auth_encode_decode_continue_with_method() {
        let auth = Auth {
            reason_code: Auth::CONTINUE_AUTHENTICATION,
            properties: Properties {
                authentication_method: Some("SCRAM-SHA-256".to_string()),
                authentication_data: Some(Bytes::from_static(b"challenge-data")),
                ..Properties::default()
            },
        };
        let mut buf = BytesMut::new();
        encode_auth(&auth, &mut buf);
        assert!(buf.len() > 2);

        let (pkt, _) = decode_packet(&buf).unwrap();
        match pkt {
            MqttPacket::Auth(decoded) => {
                assert_eq!(decoded.reason_code, Auth::CONTINUE_AUTHENTICATION);
                assert_eq!(
                    decoded.properties.authentication_method.as_deref(),
                    Some("SCRAM-SHA-256")
                );
                assert_eq!(
                    decoded.properties.authentication_data.as_deref(),
                    Some(b"challenge-data".as_slice())
                );
            }
            other => panic!("expected Auth, got {:?}", other),
        }
    }

    #[test]
    fn test_auth_encode_decode_reauthenticate() {
        let auth = Auth {
            reason_code: Auth::RE_AUTHENTICATE,
            properties: Properties {
                authentication_method: Some("PLAIN".to_string()),
                ..Properties::default()
            },
        };
        let mut buf = BytesMut::new();
        encode_auth(&auth, &mut buf);

        let (pkt, _) = decode_packet(&buf).unwrap();
        match pkt {
            MqttPacket::Auth(decoded) => {
                assert_eq!(decoded.reason_code, Auth::RE_AUTHENTICATE);
                assert_eq!(
                    decoded.properties.authentication_method.as_deref(),
                    Some("PLAIN")
                );
            }
            other => panic!("expected Auth, got {:?}", other),
        }
    }

    // ---- UTF-8 control character validation ----

    #[test]
    fn test_utf8_control_chars_rejected() {
        // U+0001 (SOH control character) should be rejected.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x04]);
        buf.extend_from_slice(b"ab\x01c");
        let mut cursor: &[u8] = &buf;
        assert!(matches!(
            read_mqtt_string(&mut cursor),
            Err(CodecError::InvalidUtf8)
        ));
    }

    #[test]
    fn test_utf8_del_rejected() {
        // U+007F (DEL) should be rejected.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0x00, 0x03]);
        buf.extend_from_slice(b"a\x7Fb");
        let mut cursor: &[u8] = &buf;
        assert!(matches!(
            read_mqtt_string(&mut cursor),
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
        let mut cursor: &[u8] = &buf;
        assert!(matches!(
            read_mqtt_string(&mut cursor),
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
        let mut cursor: &[u8] = &buf;
        assert!(matches!(
            read_mqtt_string(&mut cursor),
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
        let mut cursor: &[u8] = &buf;
        let result = read_mqtt_string(&mut cursor).unwrap();
        assert_eq!(result, text);
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
        let result = decode_packet(&buf);
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
        let (pkt, _) = decode_packet(&buf).unwrap();
        match pkt {
            MqttPacket::ConnAck(ca) => {
                assert!(ca.session_present);
                assert_eq!(ca.return_code, 0);
            }
            other => panic!("expected ConnAck, got {:?}", other),
        }
    }

    // =========================================================================
    // Additional coverage tests
    // =========================================================================

    #[test]
    fn test_encode_decode_publish_qos2() {
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: Bytes::from_static(b"qos2/topic"),
            packet_id: Some(999),
            payload: Bytes::from_static(b"exactly-once"),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&publish, &mut buf);
        let (decoded, consumed) = decode_packet(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Publish(p) => {
                assert_eq!(p.qos, QoS::ExactlyOnce);
                assert!(p.retain);
                assert_eq!(p.packet_id, Some(999));
                assert_eq!(&p.topic[..], b"qos2/topic");
                assert_eq!(&p.payload[..], b"exactly-once");
            }
            other => panic!("expected Publish, got {:?}", other),
        }
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
        let connect = MqttPacket::Connect(Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: "v5-client".to_string(),
            will: None,
            username: Some("user".to_string()),
            password: Some(Bytes::from_static(b"pass")),
            properties: Properties {
                session_expiry_interval: Some(3600),
                receive_maximum: Some(100),
                topic_alias_maximum: Some(10),
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet(&connect, &mut buf);
        let (decoded, consumed) = decode_packet(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        match decoded {
            MqttPacket::Connect(c) => {
                assert_eq!(c.protocol_version, ProtocolVersion::V5);
                assert_eq!(c.client_id, "v5-client");
                assert_eq!(c.username.as_deref(), Some("user"));
                assert_eq!(c.password.as_deref(), Some(b"pass".as_ref()));
                assert_eq!(c.properties.session_expiry_interval, Some(3600));
                assert_eq!(c.properties.receive_maximum, Some(100));
                assert_eq!(c.properties.topic_alias_maximum, Some(10));
            }
            other => panic!("expected Connect, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_connect_v5_with_will() {
        let connect = MqttPacket::Connect(Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: "v5-will".to_string(),
            will: Some(WillMessage {
                topic: "last/will".to_string(),
                payload: Bytes::from_static(b"goodbye"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: Properties {
                    will_delay_interval: Some(30),
                    content_type: Some("text/plain".to_string()),
                    ..Properties::default()
                },
            }),
            username: None,
            password: None,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&connect, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::Connect(c) => {
                assert_eq!(c.protocol_version, ProtocolVersion::V5);
                let will = c.will.unwrap();
                assert_eq!(will.topic, "last/will");
                assert_eq!(&will.payload[..], b"goodbye");
                assert_eq!(will.properties.will_delay_interval, Some(30));
                assert_eq!(will.properties.content_type.as_deref(), Some("text/plain"));
            }
            other => panic!("expected Connect, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_connack_with_properties() {
        let connack = MqttPacket::ConnAck(ConnAck {
            session_present: true,
            return_code: 0x00,
            properties: Properties {
                maximum_qos: Some(1),
                retain_available: Some(true),
                maximum_packet_size: Some(1048576),
                server_keep_alive: Some(300),
                reason_string: Some("welcome".to_string()),
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&connack, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::ConnAck(ca) => {
                assert!(ca.session_present);
                assert_eq!(ca.return_code, 0x00);
                assert_eq!(ca.properties.maximum_qos, Some(1));
                assert_eq!(ca.properties.retain_available, Some(true));
                assert_eq!(ca.properties.maximum_packet_size, Some(1048576));
                assert_eq!(ca.properties.server_keep_alive, Some(300));
                assert_eq!(ca.properties.reason_string.as_deref(), Some("welcome"));
            }
            other => panic!("expected ConnAck, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_pubrec_roundtrip() {
        let pubrec = MqttPacket::PubRec(PubRec {
            packet_id: 42,
            reason_code: Some(0x10), // No Matching Subscribers
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubrec, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::PubRec(pr) => {
                assert_eq!(pr.packet_id, 42);
                assert_eq!(pr.reason_code, Some(0x10));
            }
            other => panic!("expected PubRec, got {:?}", other),
        }
    }

    #[test]
    fn test_v311_pubrec_roundtrip() {
        let pubrec = MqttPacket::PubRec(PubRec {
            packet_id: 55,
            reason_code: None,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&pubrec, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::PubRec(pr) => {
                assert_eq!(pr.packet_id, 55);
            }
            other => panic!("expected PubRec, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_pubcomp_with_reason_code() {
        let pubcomp = MqttPacket::PubComp(PubComp {
            packet_id: 77,
            reason_code: Some(0x92), // Packet Identifier Not Found
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubcomp, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::PubComp(pc) => {
                assert_eq!(pc.packet_id, 77);
                assert_eq!(pc.reason_code, Some(0x92));
            }
            other => panic!("expected PubComp, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_pubcomp_with_reason_and_properties() {
        let pubcomp = MqttPacket::PubComp(PubComp {
            packet_id: 88,
            reason_code: Some(0x00),
            properties: Properties {
                reason_string: Some("ok".to_string()),
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubcomp, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::PubComp(pc) => {
                assert_eq!(pc.packet_id, 88);
                assert_eq!(pc.properties.reason_string.as_deref(), Some("ok"));
            }
            other => panic!("expected PubComp, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_disconnect_reason_code_only_no_props() {
        let disconnect = MqttPacket::Disconnect(Disconnect {
            reason_code: Some(0x04), // Disconnect with Will Message
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&disconnect, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::Disconnect(d) => {
                assert_eq!(d.reason_code, Some(0x04));
            }
            other => panic!("expected Disconnect, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_auth_reason_code_only_no_props() {
        // AUTH with non-success reason code but no properties.
        let auth = MqttPacket::Auth(Auth {
            reason_code: Auth::CONTINUE_AUTHENTICATION,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_auth(
            match &auth {
                MqttPacket::Auth(a) => a,
                _ => unreachable!(),
            },
            &mut buf,
        );
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::Auth(a) => {
                assert_eq!(a.reason_code, Auth::CONTINUE_AUTHENTICATION);
            }
            other => panic!("expected Auth, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_publish_qos2_with_properties_roundtrip() {
        let publish = MqttPacket::Publish(Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: Bytes::from_static(b"test/qos2"),
            packet_id: Some(500),
            payload: Bytes::from_static(b"hello"),
            properties: Properties {
                message_expiry_interval: Some(60),
                correlation_data: Some(Bytes::from_static(b"req-1")),
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&publish, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();

        match decoded {
            MqttPacket::Publish(p) => {
                assert!(p.dup);
                assert_eq!(p.qos, QoS::ExactlyOnce);
                assert!(!p.retain);
                assert_eq!(p.packet_id, Some(500));
                assert_eq!(p.properties.message_expiry_interval, Some(60));
                assert_eq!(
                    p.properties.correlation_data.as_deref(),
                    Some(b"req-1".as_ref())
                );
            }
            other => panic!("expected Publish, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_subscribe_without_subscription_id() {
        // V5 subscribe with empty properties (no subscription identifier).
        let subscribe = MqttPacket::Subscribe(Subscribe {
            packet_id: 10,
            filters: smallvec![TopicFilter {
                filter: "a/b".to_string(),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            }],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&subscribe, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();

        match decoded {
            MqttPacket::Subscribe(s) => {
                assert_eq!(s.packet_id, 10);
                assert_eq!(s.filters.len(), 1);
                assert_eq!(s.filters[0].filter, "a/b");
                assert_eq!(s.properties.subscription_identifier, None);
            }
            other => panic!("expected Subscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_suback_empty_properties() {
        let suback = MqttPacket::SubAck(SubAck {
            packet_id: 20,
            return_codes: smallvec![0x00, 0x01],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&suback, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();

        match decoded {
            MqttPacket::SubAck(sa) => {
                assert_eq!(sa.packet_id, 20);
                assert_eq!(sa.return_codes.as_slice(), &[0x00, 0x01]);
            }
            other => panic!("expected SubAck, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_unsubscribe_empty_properties() {
        let unsubscribe = MqttPacket::Unsubscribe(Unsubscribe {
            packet_id: 30,
            filters: smallvec!["x/y".to_string()],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&unsubscribe, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();

        match decoded {
            MqttPacket::Unsubscribe(u) => {
                assert_eq!(u.packet_id, 30);
                assert_eq!(u.filters[0], "x/y");
            }
            other => panic!("expected Unsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_unsuback_empty_properties() {
        let unsuback = MqttPacket::UnsubAck(UnsubAck {
            packet_id: 40,
            reason_codes: smallvec![0x00, 0x11],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&unsuback, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();

        match decoded {
            MqttPacket::UnsubAck(ua) => {
                assert_eq!(ua.packet_id, 40);
                assert_eq!(ua.reason_codes.as_slice(), &[0x00, 0x11]);
            }
            other => panic!("expected UnsubAck, got {:?}", other),
        }
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
        let pubrec = MqttPacket::PubRec(PubRec {
            packet_id: 100,
            reason_code: Some(0x00),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubrec, ProtocolVersion::V5, &mut buf);
        // Compact form: fixed_header(1) + remaining_length(1) + packet_id(2) = 4 bytes
        assert_eq!(buf.len(), 4);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::PubRec(pr) => assert_eq!(pr.packet_id, 100),
            other => panic!("expected PubRec, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_pubrec_with_properties() {
        let pubrec = MqttPacket::PubRec(PubRec {
            packet_id: 101,
            reason_code: Some(0x00),
            properties: Properties {
                reason_string: Some("ack".to_string()),
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubrec, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::PubRec(pr) => {
                assert_eq!(pr.packet_id, 101);
                assert_eq!(pr.properties.reason_string.as_deref(), Some("ack"));
            }
            other => panic!("expected PubRec, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_pubrel_reason_code_only() {
        let pubrel = MqttPacket::PubRel(PubRel {
            packet_id: 200,
            reason_code: Some(0x92), // Packet Identifier Not Found
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&pubrel, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::PubRel(pr) => {
                assert_eq!(pr.packet_id, 200);
                assert_eq!(pr.reason_code, Some(0x92));
            }
            other => panic!("expected PubRel, got {:?}", other),
        }
    }

    #[test]
    fn test_unknown_packet_type_rejected() {
        // Packet type 0 is invalid.
        let mut buf = BytesMut::new();
        buf.put_u8(0x00); // type 0
        buf.put_u8(0x00); // remaining length
        assert!(matches!(
            decode_packet(&buf),
            Err(CodecError::UnknownPacketType(0))
        ));
    }

    #[test]
    fn test_v5_connect_with_auth_method() {
        let connect = MqttPacket::Connect(Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: "auth-client".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties {
                authentication_method: Some("SCRAM-SHA-256".to_string()),
                authentication_data: Some(Bytes::from_static(b"client-first")),
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet(&connect, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::Connect(c) => {
                assert_eq!(
                    c.properties.authentication_method.as_deref(),
                    Some("SCRAM-SHA-256")
                );
                assert_eq!(
                    c.properties.authentication_data.as_deref(),
                    Some(b"client-first".as_ref())
                );
            }
            other => panic!("expected Connect, got {:?}", other),
        }
    }

    #[test]
    fn test_publish_dup_flag_roundtrip() {
        let publish = MqttPacket::Publish(Publish {
            dup: true,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: Bytes::from_static(b"t"),
            packet_id: Some(1),
            payload: Bytes::new(),
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&publish, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::Publish(p) => assert!(p.dup),
            other => panic!("expected Publish, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_properties_user_properties_multiple() {
        let publish = MqttPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"props"),
            packet_id: None,
            payload: Bytes::new(),
            properties: Properties {
                user_properties: smallvec![
                    ("key1".to_string(), "val1".to_string()),
                    ("key2".to_string(), "val2".to_string()),
                    ("key1".to_string(), "val3".to_string()), // duplicate key allowed
                ],
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&publish, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet_versioned(&buf, ProtocolVersion::V5).unwrap();

        match decoded {
            MqttPacket::Publish(p) => {
                assert_eq!(p.properties.user_properties.len(), 3);
                assert_eq!(
                    p.properties.user_properties[0],
                    ("key1".to_string(), "val1".to_string())
                );
                assert_eq!(
                    p.properties.user_properties[2],
                    ("key1".to_string(), "val3".to_string())
                );
            }
            other => panic!("expected Publish, got {:?}", other),
        }
    }

    #[test]
    fn test_v5_connack_assigned_client_id() {
        let connack = MqttPacket::ConnAck(ConnAck {
            session_present: false,
            return_code: 0x00,
            properties: Properties {
                assigned_client_identifier: Some("server-assigned-id".to_string()),
                ..Properties::default()
            },
        });

        let mut buf = BytesMut::new();
        encode_packet_versioned(&connack, ProtocolVersion::V5, &mut buf);
        let (decoded, _) = decode_packet(&buf).unwrap();

        match decoded {
            MqttPacket::ConnAck(ca) => {
                assert_eq!(
                    ca.properties.assigned_client_identifier.as_deref(),
                    Some("server-assigned-id")
                );
            }
            other => panic!("expected ConnAck, got {:?}", other),
        }
    }
}
