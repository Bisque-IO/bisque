//! MQTT binary codec — encode and decode MQTT packets from bytes.
//!
//! Implements the MQTT 3.1.1 / 5.0 binary wire protocol including:
//! - Fixed header parsing with packet type and flags
//! - Variable-length remaining length encoding/decoding (up to 4 bytes)
//! - Per-packet-type serialization and deserialization
//! - MQTT 5.0 property parsing

use bisque_mq::flat::FlatMessage;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::types::{
    ConnAck, Connect, ConnectFlags, Disconnect, MqttPacket, PacketType, Properties,
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
}

// =============================================================================
// Constants
// =============================================================================

/// Maximum allowed packet size (256 MB per MQTT spec).
pub const MAX_PACKET_SIZE: usize = 256 * 1024 * 1024;

/// Default maximum packet size for this server (1 MB).
pub const DEFAULT_MAX_PACKET_SIZE: usize = 1024 * 1024;

// =============================================================================
// Remaining Length Encoding/Decoding
// =============================================================================

/// Decode the MQTT variable-length remaining length from a byte buffer.
///
/// Returns `(remaining_length, bytes_consumed)` on success.
/// Returns `Err(Incomplete)` if more bytes are needed.
/// Returns `Err(MalformedRemainingLength)` if encoding is invalid.
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
    let result = s.to_string();
    buf.advance(len);
    Ok(result)
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

    while prop_buf.has_remaining() {
        let id = prop_buf.get_u8();
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
                props.subscription_identifier = Some(read_variable_int(&mut prop_buf)?);
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
                props.receive_maximum = Some(prop_buf.get_u16());
            }
            0x22 => {
                // Topic Alias Maximum
                props.topic_alias_maximum = Some(prop_buf.get_u16());
            }
            0x23 => {
                // Topic Alias
                props.topic_alias = Some(prop_buf.get_u16());
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

/// Write MQTT 5.0 properties into a buffer.
fn write_properties(props: &Properties, buf: &mut BytesMut) {
    let mut prop_buf = BytesMut::new();

    if let Some(v) = props.payload_format_indicator {
        prop_buf.put_u8(0x01);
        prop_buf.put_u8(v);
    }
    if let Some(v) = props.message_expiry_interval {
        prop_buf.put_u8(0x02);
        prop_buf.put_u32(v);
    }
    if let Some(ref v) = props.content_type {
        prop_buf.put_u8(0x03);
        write_mqtt_string(v, &mut prop_buf);
    }
    if let Some(ref v) = props.response_topic {
        prop_buf.put_u8(0x08);
        write_mqtt_string(v, &mut prop_buf);
    }
    if let Some(ref v) = props.correlation_data {
        prop_buf.put_u8(0x09);
        write_mqtt_bytes(v, &mut prop_buf);
    }
    if let Some(v) = props.subscription_identifier {
        prop_buf.put_u8(0x0B);
        write_variable_int(v, &mut prop_buf);
    }
    if let Some(v) = props.session_expiry_interval {
        prop_buf.put_u8(0x11);
        prop_buf.put_u32(v);
    }
    if let Some(ref v) = props.assigned_client_identifier {
        prop_buf.put_u8(0x12);
        write_mqtt_string(v, &mut prop_buf);
    }
    if let Some(v) = props.server_keep_alive {
        prop_buf.put_u8(0x13);
        prop_buf.put_u16(v);
    }
    if let Some(ref v) = props.authentication_method {
        prop_buf.put_u8(0x15);
        write_mqtt_string(v, &mut prop_buf);
    }
    if let Some(ref v) = props.authentication_data {
        prop_buf.put_u8(0x16);
        write_mqtt_bytes(v, &mut prop_buf);
    }
    if let Some(v) = props.request_problem_information {
        prop_buf.put_u8(0x17);
        prop_buf.put_u8(v);
    }
    if let Some(v) = props.will_delay_interval {
        prop_buf.put_u8(0x18);
        prop_buf.put_u32(v);
    }
    if let Some(v) = props.request_response_information {
        prop_buf.put_u8(0x19);
        prop_buf.put_u8(v);
    }
    if let Some(ref v) = props.response_information {
        prop_buf.put_u8(0x1A);
        write_mqtt_string(v, &mut prop_buf);
    }
    if let Some(ref v) = props.server_reference {
        prop_buf.put_u8(0x1C);
        write_mqtt_string(v, &mut prop_buf);
    }
    if let Some(ref v) = props.reason_string {
        prop_buf.put_u8(0x1F);
        write_mqtt_string(v, &mut prop_buf);
    }
    if let Some(v) = props.receive_maximum {
        prop_buf.put_u8(0x21);
        prop_buf.put_u16(v);
    }
    if let Some(v) = props.topic_alias_maximum {
        prop_buf.put_u8(0x22);
        prop_buf.put_u16(v);
    }
    if let Some(v) = props.topic_alias {
        prop_buf.put_u8(0x23);
        prop_buf.put_u16(v);
    }
    if let Some(v) = props.maximum_qos {
        prop_buf.put_u8(0x24);
        prop_buf.put_u8(v);
    }
    if let Some(v) = props.retain_available {
        prop_buf.put_u8(0x25);
        prop_buf.put_u8(v as u8);
    }
    for (key, val) in &props.user_properties {
        prop_buf.put_u8(0x26);
        write_mqtt_string(key, &mut prop_buf);
        write_mqtt_string(val, &mut prop_buf);
    }
    if let Some(v) = props.maximum_packet_size {
        prop_buf.put_u8(0x27);
        prop_buf.put_u32(v);
    }
    if let Some(v) = props.wildcard_subscription_available {
        prop_buf.put_u8(0x28);
        prop_buf.put_u8(v as u8);
    }
    if let Some(v) = props.subscription_identifier_available {
        prop_buf.put_u8(0x29);
        prop_buf.put_u8(v as u8);
    }
    if let Some(v) = props.shared_subscription_available {
        prop_buf.put_u8(0x2A);
        prop_buf.put_u8(v as u8);
    }

    write_variable_int(prop_buf.len() as u32, buf);
    buf.extend_from_slice(&prop_buf);
}

// =============================================================================
// Packet Decoding
// =============================================================================

/// Parse the fixed header and extract the packet type, flags, and remaining length.
/// Returns `(packet_type_nibble, flags_nibble, remaining_length, header_size)`.
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
        PacketType::Auth => MqttPacket::Auth,
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
                PacketType::Auth => MqttPacket::Auth,
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
        properties: Properties::default(),
    }))
}

fn decode_connect(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    // Protocol Name
    let protocol_name = read_mqtt_string(buf)?;
    if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
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

    // Remaining bytes are the payload. We cannot distinguish MQTT 5.0 properties
    // here without knowing the protocol version, so for now we consume
    // everything as payload. The session layer handles version-aware parsing
    // if needed.
    let payload = Bytes::copy_from_slice(buf.chunk());
    buf.advance(buf.remaining());

    Ok(MqttPacket::Publish(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
        properties: Properties::default(),
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
    std::str::from_utf8(data).map_err(|_| CodecError::InvalidUtf8)?;
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
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    // MQTT 5.0 properties (check if remaining data suggests properties)
    // For simplicity, we always try to parse subscription options.
    let properties = Properties::default();

    let mut filters = Vec::new();
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
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    let mut return_codes = Vec::new();
    while buf.has_remaining() {
        return_codes.push(buf.get_u8());
    }

    Ok(MqttPacket::SubAck(SubAck {
        packet_id,
        return_codes,
        properties: Properties::default(),
    }))
}

fn decode_unsubscribe(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    let mut filters = Vec::new();
    while buf.has_remaining() {
        filters.push(read_mqtt_string(buf)?);
    }

    Ok(MqttPacket::Unsubscribe(Unsubscribe {
        packet_id,
        filters,
        properties: Properties::default(),
    }))
}

fn decode_unsuback(buf: &mut &[u8]) -> Result<MqttPacket, CodecError> {
    if buf.remaining() < 2 {
        return Err(CodecError::UnexpectedEof);
    }
    let packet_id = buf.get_u16();

    let mut reason_codes = Vec::new();
    while buf.has_remaining() {
        reason_codes.push(buf.get_u8());
    }

    Ok(MqttPacket::UnsubAck(UnsubAck {
        packet_id,
        reason_codes,
        properties: Properties::default(),
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
        MqttPacket::Auth => {
            // AUTH packet: fixed header only (type 15, no payload for basic stub)
            buf.put_u8(0xF0);
            buf.put_u8(0x00);
        }
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
pub fn encode_publish_from_flat(
    flat_msg: &FlatMessage,
    qos: QoS,
    retain: bool,
    dup: bool,
    packet_id: Option<u16>,
    is_v5: bool,
    subscription_id: Option<u32>,
    topic_alias: Option<u16>,
    buf: &mut BytesMut,
) {
    let topic = flat_msg.routing_key().unwrap_or_default();
    let payload = flat_msg.value();

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
    let var_header_size =
        2 + topic.len() + if qos != QoS::AtMostOnce { 2 } else { 0 } + props_total_size;

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
    buf.put_u16(topic.len() as u16);
    buf.extend_from_slice(&topic);

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
    let mut payload = BytesMut::new();

    // Variable header
    write_mqtt_string(&connect.protocol_name, &mut payload);
    payload.put_u8(connect.protocol_version.level());
    payload.put_u8(connect.flags.to_byte());
    payload.put_u16(connect.keep_alive);

    // MQTT 5.0 properties
    if connect.protocol_version == ProtocolVersion::V5 {
        write_properties(&connect.properties, &mut payload);
    }

    // Payload
    write_mqtt_string(&connect.client_id, &mut payload);

    if let Some(ref will) = connect.will {
        if connect.protocol_version == ProtocolVersion::V5 {
            write_properties(&will.properties, &mut payload);
        }
        write_mqtt_string(&will.topic, &mut payload);
        write_mqtt_bytes(&will.payload, &mut payload);
    }

    if let Some(ref username) = connect.username {
        write_mqtt_string(username, &mut payload);
    }

    if let Some(ref password) = connect.password {
        write_mqtt_bytes(password, &mut payload);
    }

    // Fixed header: CONNECT = 0x10
    buf.put_u8(0x10);
    encode_remaining_length(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

fn encode_connack(connack: &ConnAck, buf: &mut BytesMut) {
    let mut payload = BytesMut::new();
    let ack_flags: u8 = if connack.session_present { 0x01 } else { 0x00 };
    payload.put_u8(ack_flags);
    payload.put_u8(connack.return_code);

    // Fixed header: CONNACK = 0x20
    buf.put_u8(0x20);
    encode_remaining_length(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

fn encode_publish(publish: &Publish, buf: &mut BytesMut) {
    // Pre-compute remaining length to avoid intermediate BytesMut allocation.
    let topic_len = publish.topic.len();
    let var_header_size = 2 + topic_len + if publish.qos != QoS::AtMostOnce { 2 } else { 0 };
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

    // Payload
    buf.extend_from_slice(&publish.payload);
}

fn encode_puback(puback: &PubAck, buf: &mut BytesMut) {
    buf.put_u8(0x40); // PUBACK = 4 << 4
    encode_remaining_length(2, buf);
    buf.put_u16(puback.packet_id);
}

fn encode_pubrec(pubrec: &PubRec, buf: &mut BytesMut) {
    buf.put_u8(0x50); // PUBREC = 5 << 4
    encode_remaining_length(2, buf);
    buf.put_u16(pubrec.packet_id);
}

fn encode_pubrel(pubrel: &PubRel, buf: &mut BytesMut) {
    buf.put_u8(0x62); // PUBREL = 6 << 4 | 0x02 (fixed flags)
    encode_remaining_length(2, buf);
    buf.put_u16(pubrel.packet_id);
}

fn encode_pubcomp(pubcomp: &PubComp, buf: &mut BytesMut) {
    buf.put_u8(0x70); // PUBCOMP = 7 << 4
    encode_remaining_length(2, buf);
    buf.put_u16(pubcomp.packet_id);
}

fn encode_subscribe(subscribe: &Subscribe, buf: &mut BytesMut) {
    let mut payload = BytesMut::new();
    payload.put_u16(subscribe.packet_id);

    for filter in &subscribe.filters {
        write_mqtt_string(&filter.filter, &mut payload);
        let mut options: u8 = filter.qos.as_u8() & 0x03;
        if filter.no_local {
            options |= 0x04;
        }
        if filter.retain_as_published {
            options |= 0x08;
        }
        options |= (filter.retain_handling & 0x03) << 4;
        payload.put_u8(options);
    }

    buf.put_u8(0x82); // SUBSCRIBE = 8 << 4 | 0x02
    encode_remaining_length(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

fn encode_suback(suback: &SubAck, buf: &mut BytesMut) {
    let mut payload = BytesMut::new();
    payload.put_u16(suback.packet_id);
    for &code in &suback.return_codes {
        payload.put_u8(code);
    }

    buf.put_u8(0x90); // SUBACK = 9 << 4
    encode_remaining_length(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

fn encode_unsubscribe(unsubscribe: &Unsubscribe, buf: &mut BytesMut) {
    let mut payload = BytesMut::new();
    payload.put_u16(unsubscribe.packet_id);
    for filter in &unsubscribe.filters {
        write_mqtt_string(filter, &mut payload);
    }

    buf.put_u8(0xA2); // UNSUBSCRIBE = 10 << 4 | 0x02
    encode_remaining_length(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

fn encode_unsuback(unsuback: &UnsubAck, buf: &mut BytesMut) {
    let mut payload = BytesMut::new();
    payload.put_u16(unsuback.packet_id);
    for &code in &unsuback.reason_codes {
        payload.put_u8(code);
    }

    buf.put_u8(0xB0); // UNSUBACK = 11 << 4
    encode_remaining_length(payload.len(), buf);
    buf.extend_from_slice(&payload);
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
        // MQTT 3.1.1 DISCONNECT: no variable header
        buf.put_u8(0xE0); // DISCONNECT = 14 << 4
        buf.put_u8(0x00);
    } else {
        let mut payload = BytesMut::new();
        if let Some(rc) = disconnect.reason_code {
            payload.put_u8(rc);
        }
        buf.put_u8(0xE0);
        encode_remaining_length(payload.len(), buf);
        buf.extend_from_slice(&payload);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
            Some(3),
            &mut buf,
        );

        // Verify the encoded packet is valid (can be parsed by fixed header).
        let (_, _, remaining_length, header_size) = parse_fixed_header(&buf).unwrap();
        assert_eq!(buf.len(), header_size + remaining_length);

        // Verify the topic is at the right position.
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
            filters: vec![
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
            return_codes: vec![0x01, 0x02, 0x80],
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        encode_packet(&suback, &mut buf);

        let (decoded, _) = decode_packet(&buf).unwrap();
        match decoded {
            MqttPacket::SubAck(s) => {
                assert_eq!(s.packet_id, 1);
                assert_eq!(s.return_codes, vec![0x01, 0x02, 0x80]);
            }
            _ => panic!("expected SubAck"),
        }
    }

    #[test]
    fn test_encode_decode_unsubscribe() {
        let unsub = MqttPacket::Unsubscribe(Unsubscribe {
            packet_id: 5,
            filters: vec!["sensor/+/data".to_string(), "control/#".to_string()],
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
            user_properties: vec![
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
}
