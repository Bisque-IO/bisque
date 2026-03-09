//! AMQP 1.0 binary codec.
//!
//! Handles the AMQP 1.0 type system encoding/decoding and frame parsing.
//! Frame structure: [4:size][1:doff][1:type][2:channel][payload]

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::types::*;

// =============================================================================
// Codec Error
// =============================================================================

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("incomplete frame: need more data")]
    Incomplete,
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(u32),
    #[error("invalid format code: 0x{0:02x}")]
    InvalidFormatCode(u8),
    #[error("invalid utf-8 in string")]
    InvalidUtf8,
    #[error("unexpected end of data")]
    UnexpectedEof,
    #[error("invalid frame header")]
    InvalidFrameHeader,
    #[error("unknown descriptor: {0}")]
    UnknownDescriptor(u64),
    #[error("invalid performative: {0}")]
    InvalidPerformative(String),
    #[error("invalid protocol header")]
    InvalidProtocolHeader,
}

const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024; // 16 MB

// =============================================================================
// Format Codes
// =============================================================================

mod format_code {
    pub const NULL: u8 = 0x40;
    pub const BOOLEAN_TRUE: u8 = 0x41;
    pub const BOOLEAN_FALSE: u8 = 0x42;
    pub const BOOLEAN: u8 = 0x56;
    pub const UBYTE: u8 = 0x50;
    pub const USHORT: u8 = 0x60;
    pub const UINT: u8 = 0x70;
    pub const UINT_SMALL: u8 = 0x52;
    pub const UINT_ZERO: u8 = 0x43;
    pub const ULONG: u8 = 0x80;
    pub const ULONG_SMALL: u8 = 0x53;
    pub const ULONG_ZERO: u8 = 0x44;
    pub const BYTE: u8 = 0x51;
    pub const SHORT: u8 = 0x61;
    pub const INT: u8 = 0x71;
    pub const INT_SMALL: u8 = 0x54;
    pub const LONG: u8 = 0x81;
    pub const LONG_SMALL: u8 = 0x55;
    pub const FLOAT: u8 = 0x72;
    pub const DOUBLE: u8 = 0x82;
    pub const TIMESTAMP: u8 = 0x83;
    pub const UUID: u8 = 0x98;
    pub const BINARY8: u8 = 0xa0;
    pub const BINARY32: u8 = 0xb0;
    pub const STRING8: u8 = 0xa1;
    pub const STRING32: u8 = 0xb1;
    pub const SYMBOL8: u8 = 0xa3;
    pub const SYMBOL32: u8 = 0xb3;
    pub const LIST0: u8 = 0x45;
    pub const LIST8: u8 = 0xc0;
    pub const LIST32: u8 = 0xd0;
    pub const MAP8: u8 = 0xc1;
    pub const MAP32: u8 = 0xd1;
    pub const ARRAY8: u8 = 0xe0;
    pub const ARRAY32: u8 = 0xf0;
    pub const DESCRIBED: u8 = 0x00;
}

// =============================================================================
// Decode Helpers
// =============================================================================

fn ensure(buf: &[u8], n: usize) -> Result<(), CodecError> {
    if buf.len() < n {
        Err(CodecError::UnexpectedEof)
    } else {
        Ok(())
    }
}

/// Decode an AMQP 1.0 typed value from the buffer.
pub fn decode_value(buf: &mut &[u8]) -> Result<AmqpValue, CodecError> {
    ensure(buf, 1)?;
    let code = buf[0];
    buf.advance(1);

    match code {
        format_code::NULL => Ok(AmqpValue::Null),
        format_code::BOOLEAN_TRUE => Ok(AmqpValue::Boolean(true)),
        format_code::BOOLEAN_FALSE => Ok(AmqpValue::Boolean(false)),
        format_code::BOOLEAN => {
            ensure(buf, 1)?;
            let v = buf[0] != 0;
            buf.advance(1);
            Ok(AmqpValue::Boolean(v))
        }
        format_code::UBYTE => {
            ensure(buf, 1)?;
            let v = buf[0];
            buf.advance(1);
            Ok(AmqpValue::Ubyte(v))
        }
        format_code::USHORT => {
            ensure(buf, 2)?;
            let v = u16::from_be_bytes([buf[0], buf[1]]);
            buf.advance(2);
            Ok(AmqpValue::Ushort(v))
        }
        format_code::UINT => {
            ensure(buf, 4)?;
            let v = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            buf.advance(4);
            Ok(AmqpValue::Uint(v))
        }
        format_code::UINT_SMALL => {
            ensure(buf, 1)?;
            let v = buf[0] as u32;
            buf.advance(1);
            Ok(AmqpValue::Uint(v))
        }
        format_code::UINT_ZERO => Ok(AmqpValue::Uint(0)),
        format_code::ULONG => {
            ensure(buf, 8)?;
            let v = u64::from_be_bytes(buf[..8].try_into().unwrap());
            buf.advance(8);
            Ok(AmqpValue::Ulong(v))
        }
        format_code::ULONG_SMALL => {
            ensure(buf, 1)?;
            let v = buf[0] as u64;
            buf.advance(1);
            Ok(AmqpValue::Ulong(v))
        }
        format_code::ULONG_ZERO => Ok(AmqpValue::Ulong(0)),
        format_code::BYTE => {
            ensure(buf, 1)?;
            let v = buf[0] as i8;
            buf.advance(1);
            Ok(AmqpValue::Byte(v))
        }
        format_code::SHORT => {
            ensure(buf, 2)?;
            let v = i16::from_be_bytes([buf[0], buf[1]]);
            buf.advance(2);
            Ok(AmqpValue::Short(v))
        }
        format_code::INT => {
            ensure(buf, 4)?;
            let v = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            buf.advance(4);
            Ok(AmqpValue::Int(v))
        }
        format_code::INT_SMALL => {
            ensure(buf, 1)?;
            let v = buf[0] as i8 as i32;
            buf.advance(1);
            Ok(AmqpValue::Int(v))
        }
        format_code::LONG => {
            ensure(buf, 8)?;
            let v = i64::from_be_bytes(buf[..8].try_into().unwrap());
            buf.advance(8);
            Ok(AmqpValue::Long(v))
        }
        format_code::LONG_SMALL => {
            ensure(buf, 1)?;
            let v = buf[0] as i8 as i64;
            buf.advance(1);
            Ok(AmqpValue::Long(v))
        }
        format_code::FLOAT => {
            ensure(buf, 4)?;
            let v = f32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            buf.advance(4);
            Ok(AmqpValue::Float(v))
        }
        format_code::DOUBLE => {
            ensure(buf, 8)?;
            let v = f64::from_be_bytes(buf[..8].try_into().unwrap());
            buf.advance(8);
            Ok(AmqpValue::Double(v))
        }
        format_code::TIMESTAMP => {
            ensure(buf, 8)?;
            let v = i64::from_be_bytes(buf[..8].try_into().unwrap());
            buf.advance(8);
            Ok(AmqpValue::Timestamp(v))
        }
        format_code::UUID => {
            ensure(buf, 16)?;
            let mut v = [0u8; 16];
            v.copy_from_slice(&buf[..16]);
            buf.advance(16);
            Ok(AmqpValue::Uuid(v))
        }
        format_code::BINARY8 => {
            ensure(buf, 1)?;
            let len = buf[0] as usize;
            buf.advance(1);
            ensure(buf, len)?;
            let data = Bytes::copy_from_slice(&buf[..len]);
            buf.advance(len);
            Ok(AmqpValue::Binary(data))
        }
        format_code::BINARY32 => {
            ensure(buf, 4)?;
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            buf.advance(4);
            ensure(buf, len)?;
            let data = Bytes::copy_from_slice(&buf[..len]);
            buf.advance(len);
            Ok(AmqpValue::Binary(data))
        }
        format_code::STRING8 => {
            ensure(buf, 1)?;
            let len = buf[0] as usize;
            buf.advance(1);
            ensure(buf, len)?;
            let s = std::str::from_utf8(&buf[..len])
                .map_err(|_| CodecError::InvalidUtf8)?
                .to_string();
            buf.advance(len);
            Ok(AmqpValue::String(s))
        }
        format_code::STRING32 => {
            ensure(buf, 4)?;
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            buf.advance(4);
            ensure(buf, len)?;
            let s = std::str::from_utf8(&buf[..len])
                .map_err(|_| CodecError::InvalidUtf8)?
                .to_string();
            buf.advance(len);
            Ok(AmqpValue::String(s))
        }
        format_code::SYMBOL8 => {
            ensure(buf, 1)?;
            let len = buf[0] as usize;
            buf.advance(1);
            ensure(buf, len)?;
            let s = std::str::from_utf8(&buf[..len])
                .map_err(|_| CodecError::InvalidUtf8)?
                .to_string();
            buf.advance(len);
            Ok(AmqpValue::Symbol(s))
        }
        format_code::SYMBOL32 => {
            ensure(buf, 4)?;
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            buf.advance(4);
            ensure(buf, len)?;
            let s = std::str::from_utf8(&buf[..len])
                .map_err(|_| CodecError::InvalidUtf8)?
                .to_string();
            buf.advance(len);
            Ok(AmqpValue::Symbol(s))
        }
        format_code::LIST0 => Ok(AmqpValue::List(Vec::new())),
        format_code::LIST8 => {
            ensure(buf, 2)?;
            let _size = buf[0] as usize;
            let count = buf[1] as usize;
            buf.advance(2);
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_value(buf)?);
            }
            Ok(AmqpValue::List(items))
        }
        format_code::LIST32 => {
            ensure(buf, 8)?;
            let _size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let count = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
            buf.advance(8);
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_value(buf)?);
            }
            Ok(AmqpValue::List(items))
        }
        format_code::MAP8 => {
            ensure(buf, 2)?;
            let _size = buf[0] as usize;
            let count = buf[1] as usize; // count is number of key-value pairs * 2
            buf.advance(2);
            let pairs = count / 2;
            let mut items = Vec::with_capacity(pairs);
            for _ in 0..pairs {
                let k = decode_value(buf)?;
                let v = decode_value(buf)?;
                items.push((k, v));
            }
            Ok(AmqpValue::Map(items))
        }
        format_code::MAP32 => {
            ensure(buf, 8)?;
            let _size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let count = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
            buf.advance(8);
            let pairs = count / 2;
            let mut items = Vec::with_capacity(pairs);
            for _ in 0..pairs {
                let k = decode_value(buf)?;
                let v = decode_value(buf)?;
                items.push((k, v));
            }
            Ok(AmqpValue::Map(items))
        }
        format_code::ARRAY8 | format_code::ARRAY32 => {
            let (count, _size) = if code == format_code::ARRAY8 {
                ensure(buf, 2)?;
                let s = buf[0] as usize;
                let c = buf[1] as usize;
                buf.advance(2);
                (c, s)
            } else {
                ensure(buf, 8)?;
                let s = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                let c = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
                buf.advance(8);
                (c, s)
            };
            // Array elements share a constructor (format code)
            let mut items = Vec::with_capacity(count);
            if count > 0 {
                // Read constructor
                ensure(buf, 1)?;
                let elem_code = buf[0];
                buf.advance(1);
                for _ in 0..count {
                    // Decode each element using the shared constructor
                    // Put the format code back for decode_value
                    let mut temp = Vec::with_capacity(1 + buf.len());
                    temp.push(elem_code);
                    temp.extend_from_slice(buf);
                    let mut temp_slice: &[u8] = &temp;
                    let val = decode_value(&mut temp_slice)?;
                    let consumed = temp.len() - temp_slice.len();
                    buf.advance(consumed - 1); // -1 for the format code we prepended
                    items.push(val);
                }
            }
            Ok(AmqpValue::Array(items))
        }
        format_code::DESCRIBED => {
            let descriptor = decode_value(buf)?;
            let value = decode_value(buf)?;
            Ok(AmqpValue::Described(Box::new(descriptor), Box::new(value)))
        }
        _ => Err(CodecError::InvalidFormatCode(code)),
    }
}

// =============================================================================
// Encode Helpers
// =============================================================================

/// Encode an AMQP 1.0 typed value into the buffer.
pub fn encode_value(buf: &mut BytesMut, value: &AmqpValue) {
    match value {
        AmqpValue::Null => buf.put_u8(format_code::NULL),
        AmqpValue::Boolean(true) => buf.put_u8(format_code::BOOLEAN_TRUE),
        AmqpValue::Boolean(false) => buf.put_u8(format_code::BOOLEAN_FALSE),
        AmqpValue::Ubyte(v) => {
            buf.put_u8(format_code::UBYTE);
            buf.put_u8(*v);
        }
        AmqpValue::Ushort(v) => {
            buf.put_u8(format_code::USHORT);
            buf.put_u16(*v);
        }
        AmqpValue::Uint(v) => {
            if *v == 0 {
                buf.put_u8(format_code::UINT_ZERO);
            } else if *v <= 255 {
                buf.put_u8(format_code::UINT_SMALL);
                buf.put_u8(*v as u8);
            } else {
                buf.put_u8(format_code::UINT);
                buf.put_u32(*v);
            }
        }
        AmqpValue::Ulong(v) => {
            if *v == 0 {
                buf.put_u8(format_code::ULONG_ZERO);
            } else if *v <= 255 {
                buf.put_u8(format_code::ULONG_SMALL);
                buf.put_u8(*v as u8);
            } else {
                buf.put_u8(format_code::ULONG);
                buf.put_u64(*v);
            }
        }
        AmqpValue::Byte(v) => {
            buf.put_u8(format_code::BYTE);
            buf.put_i8(*v);
        }
        AmqpValue::Short(v) => {
            buf.put_u8(format_code::SHORT);
            buf.put_i16(*v);
        }
        AmqpValue::Int(v) => {
            if *v >= -128 && *v <= 127 {
                buf.put_u8(format_code::INT_SMALL);
                buf.put_i8(*v as i8);
            } else {
                buf.put_u8(format_code::INT);
                buf.put_i32(*v);
            }
        }
        AmqpValue::Long(v) => {
            if *v >= -128 && *v <= 127 {
                buf.put_u8(format_code::LONG_SMALL);
                buf.put_i8(*v as i8);
            } else {
                buf.put_u8(format_code::LONG);
                buf.put_i64(*v);
            }
        }
        AmqpValue::Float(v) => {
            buf.put_u8(format_code::FLOAT);
            buf.put_f32(*v);
        }
        AmqpValue::Double(v) => {
            buf.put_u8(format_code::DOUBLE);
            buf.put_f64(*v);
        }
        AmqpValue::Timestamp(v) => {
            buf.put_u8(format_code::TIMESTAMP);
            buf.put_i64(*v);
        }
        AmqpValue::Uuid(v) => {
            buf.put_u8(format_code::UUID);
            buf.put_slice(v);
        }
        AmqpValue::Binary(v) => {
            if v.len() <= 255 {
                buf.put_u8(format_code::BINARY8);
                buf.put_u8(v.len() as u8);
            } else {
                buf.put_u8(format_code::BINARY32);
                buf.put_u32(v.len() as u32);
            }
            buf.put_slice(v);
        }
        AmqpValue::String(v) => {
            if v.len() <= 255 {
                buf.put_u8(format_code::STRING8);
                buf.put_u8(v.len() as u8);
            } else {
                buf.put_u8(format_code::STRING32);
                buf.put_u32(v.len() as u32);
            }
            buf.put_slice(v.as_bytes());
        }
        AmqpValue::Symbol(v) => {
            if v.len() <= 255 {
                buf.put_u8(format_code::SYMBOL8);
                buf.put_u8(v.len() as u8);
            } else {
                buf.put_u8(format_code::SYMBOL32);
                buf.put_u32(v.len() as u32);
            }
            buf.put_slice(v.as_bytes());
        }
        AmqpValue::Described(d, v) => {
            buf.put_u8(format_code::DESCRIBED);
            encode_value(buf, d);
            encode_value(buf, v);
        }
        AmqpValue::List(items) => {
            if items.is_empty() {
                buf.put_u8(format_code::LIST0);
            } else {
                // Encode items to a temp buffer to get size
                let mut body = BytesMut::new();
                for item in items {
                    encode_value(&mut body, item);
                }
                let count = items.len();
                if body.len() <= 255 && count <= 255 {
                    buf.put_u8(format_code::LIST8);
                    buf.put_u8((body.len() + 1) as u8); // +1 for count byte
                    buf.put_u8(count as u8);
                } else {
                    buf.put_u8(format_code::LIST32);
                    buf.put_u32((body.len() + 4) as u32); // +4 for count
                    buf.put_u32(count as u32);
                }
                buf.put_slice(&body);
            }
        }
        AmqpValue::Map(pairs) => {
            let mut body = BytesMut::new();
            for (k, v) in pairs {
                encode_value(&mut body, k);
                encode_value(&mut body, v);
            }
            let count = pairs.len() * 2;
            if body.len() <= 255 && count <= 255 {
                buf.put_u8(format_code::MAP8);
                buf.put_u8((body.len() + 1) as u8);
                buf.put_u8(count as u8);
            } else {
                buf.put_u8(format_code::MAP32);
                buf.put_u32((body.len() + 4) as u32);
                buf.put_u32(count as u32);
            }
            buf.put_slice(&body);
        }
        AmqpValue::Array(items) => {
            if items.is_empty() {
                buf.put_u8(format_code::ARRAY8);
                buf.put_u8(1); // size = 1 (just the constructor)
                buf.put_u8(0); // count = 0
                buf.put_u8(format_code::NULL); // constructor (arbitrary for empty)
            } else {
                // For simplicity, encode as list (arrays require shared constructor)
                let mut body = BytesMut::new();
                for item in items {
                    encode_value(&mut body, item);
                }
                buf.put_u8(format_code::LIST32);
                buf.put_u32((body.len() + 4) as u32);
                buf.put_u32(items.len() as u32);
                buf.put_slice(&body);
            }
        }
    }
}

/// Encode a described list (used for performatives).
pub fn encode_described_list(buf: &mut BytesMut, descriptor: u64, fields: &[AmqpValue]) {
    buf.put_u8(format_code::DESCRIBED);
    encode_value(buf, &AmqpValue::Ulong(descriptor));
    // Trim trailing nulls
    let mut last_non_null = 0;
    for (i, f) in fields.iter().enumerate() {
        if !f.is_null() {
            last_non_null = i + 1;
        }
    }
    let trimmed = &fields[..last_non_null];
    encode_value(buf, &AmqpValue::List(trimmed.to_vec()));
}

// =============================================================================
// Frame Decode/Encode
// =============================================================================

/// Try to decode a frame from the buffer.
/// Returns `(frame, bytes_consumed)` or `Incomplete` if more data needed.
pub fn decode_frame(data: &[u8]) -> Result<(AmqpFrame, usize), CodecError> {
    if data.len() < 8 {
        return Err(CodecError::Incomplete);
    }
    let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if size < 8 {
        return Err(CodecError::InvalidFrameHeader);
    }
    if size > MAX_FRAME_SIZE {
        return Err(CodecError::FrameTooLarge(size));
    }
    if data.len() < size as usize {
        return Err(CodecError::Incomplete);
    }

    let doff = data[4];
    let frame_type = data[5];
    let channel = u16::from_be_bytes([data[6], data[7]]);

    let header_size = (doff as usize) * 4;
    if header_size < 8 || header_size > size as usize {
        return Err(CodecError::InvalidFrameHeader);
    }

    let payload = &data[header_size..size as usize];

    let body = if payload.is_empty() {
        FrameBody::Empty
    } else if frame_type == FRAME_TYPE_SASL {
        let mut buf = payload;
        let value = decode_value(&mut buf)?;
        let sasl = decode_sasl_performative(value)?;
        FrameBody::Sasl(sasl)
    } else {
        let mut buf = payload;
        let value = decode_value(&mut buf)?;
        let remaining = Bytes::copy_from_slice(buf);
        let perf = decode_performative(value, remaining)?;
        FrameBody::Amqp(perf)
    };

    Ok((
        AmqpFrame {
            channel,
            frame_type,
            body,
        },
        size as usize,
    ))
}

/// Encode a frame into the buffer.
pub fn encode_frame(buf: &mut BytesMut, channel: u16, frame_type: u8, payload: &[u8]) {
    let size = 8 + payload.len();
    buf.put_u32(size as u32);
    buf.put_u8(2); // doff = 2 (8 bytes header)
    buf.put_u8(frame_type);
    buf.put_u16(channel);
    buf.put_slice(payload);
}

/// Encode an empty heartbeat frame.
pub fn encode_empty_frame(buf: &mut BytesMut) {
    encode_frame(buf, 0, FRAME_TYPE_AMQP, &[]);
}

// =============================================================================
// Performative Decode
// =============================================================================

fn list_get(list: &[AmqpValue], idx: usize) -> &AmqpValue {
    list.get(idx).unwrap_or(&AmqpValue::Null)
}

fn decode_performative(value: AmqpValue, payload: Bytes) -> Result<Performative, CodecError> {
    match value {
        AmqpValue::Described(desc, inner) => {
            let descriptor = desc.as_u64().ok_or(CodecError::InvalidPerformative(
                "non-numeric descriptor".into(),
            ))?;
            let fields = match *inner {
                AmqpValue::List(l) => l,
                _ => Vec::new(),
            };
            match descriptor {
                descriptor::OPEN => Ok(Performative::Open(decode_open(&fields)?)),
                descriptor::BEGIN => Ok(Performative::Begin(decode_begin(&fields)?)),
                descriptor::ATTACH => Ok(Performative::Attach(decode_attach(&fields)?)),
                descriptor::FLOW => Ok(Performative::Flow(decode_flow(&fields)?)),
                descriptor::TRANSFER => {
                    Ok(Performative::Transfer(decode_transfer(&fields, payload)?))
                }
                descriptor::DISPOSITION => {
                    Ok(Performative::Disposition(decode_disposition(&fields)?))
                }
                descriptor::DETACH => Ok(Performative::Detach(decode_detach(&fields)?)),
                descriptor::END => Ok(Performative::End(decode_end(&fields)?)),
                descriptor::CLOSE => Ok(Performative::Close(decode_close(&fields)?)),
                _ => Err(CodecError::UnknownDescriptor(descriptor)),
            }
        }
        _ => Err(CodecError::InvalidPerformative(
            "not a described type".into(),
        )),
    }
}

fn decode_open(fields: &[AmqpValue]) -> Result<Open, CodecError> {
    Ok(Open {
        container_id: list_get(fields, 0).as_str().unwrap_or("").to_string(),
        hostname: list_get(fields, 1).as_str().map(|s| s.to_string()),
        max_frame_size: list_get(fields, 2)
            .as_u32()
            .unwrap_or(DEFAULT_MAX_FRAME_SIZE),
        channel_max: list_get(fields, 3)
            .as_u32()
            .unwrap_or(DEFAULT_CHANNEL_MAX as u32) as u16,
        idle_timeout: list_get(fields, 4).as_u32(),
        ..Default::default()
    })
}

fn decode_begin(fields: &[AmqpValue]) -> Result<Begin, CodecError> {
    Ok(Begin {
        remote_channel: list_get(fields, 0).as_u32().map(|v| v as u16),
        next_outgoing_id: list_get(fields, 1).as_u32().unwrap_or(0),
        incoming_window: list_get(fields, 2).as_u32().unwrap_or(2048),
        outgoing_window: list_get(fields, 3).as_u32().unwrap_or(2048),
        handle_max: list_get(fields, 4).as_u32().unwrap_or(u32::MAX),
        ..Default::default()
    })
}

fn decode_source(value: &AmqpValue) -> Option<Source> {
    match value {
        AmqpValue::Described(_, inner) => {
            let fields = inner.as_list().unwrap_or(&[]);
            Some(Source {
                address: list_get(fields, 0).as_str().map(|s| s.to_string()),
                durable: list_get(fields, 1).as_u32().unwrap_or(0),
                dynamic: list_get(fields, 4).as_bool().unwrap_or(false),
                distribution_mode: list_get(fields, 6).as_str().map(|s| s.to_string()),
                ..Default::default()
            })
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

fn decode_target(value: &AmqpValue) -> Option<Target> {
    match value {
        AmqpValue::Described(_, inner) => {
            let fields = inner.as_list().unwrap_or(&[]);
            Some(Target {
                address: list_get(fields, 0).as_str().map(|s| s.to_string()),
                durable: list_get(fields, 1).as_u32().unwrap_or(0),
                dynamic: list_get(fields, 4).as_bool().unwrap_or(false),
                ..Default::default()
            })
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

fn decode_attach(fields: &[AmqpValue]) -> Result<Attach, CodecError> {
    Ok(Attach {
        name: list_get(fields, 0).as_str().unwrap_or("").to_string(),
        handle: list_get(fields, 1).as_u32().unwrap_or(0),
        role: Role::from_bool(list_get(fields, 2).as_bool().unwrap_or(false)),
        snd_settle_mode: SndSettleMode::from_u8(list_get(fields, 3).as_u32().unwrap_or(2) as u8),
        rcv_settle_mode: RcvSettleMode::from_u8(list_get(fields, 4).as_u32().unwrap_or(0) as u8),
        source: decode_source(list_get(fields, 5)),
        target: decode_target(list_get(fields, 6)),
        initial_delivery_count: list_get(fields, 10).as_u32(),
        max_message_size: list_get(fields, 11).as_u64(),
        ..Default::default()
    })
}

fn decode_flow(fields: &[AmqpValue]) -> Result<Flow, CodecError> {
    Ok(Flow {
        next_incoming_id: list_get(fields, 0).as_u32(),
        incoming_window: list_get(fields, 1).as_u32().unwrap_or(2048),
        next_outgoing_id: list_get(fields, 2).as_u32().unwrap_or(0),
        outgoing_window: list_get(fields, 3).as_u32().unwrap_or(2048),
        handle: list_get(fields, 4).as_u32(),
        delivery_count: list_get(fields, 5).as_u32(),
        link_credit: list_get(fields, 6).as_u32(),
        available: list_get(fields, 7).as_u32(),
        drain: list_get(fields, 8).as_bool().unwrap_or(false),
        echo: list_get(fields, 9).as_bool().unwrap_or(false),
        properties: match list_get(fields, 10) {
            AmqpValue::Null => None,
            v => Some(v.clone()),
        },
    })
}

fn decode_transfer(fields: &[AmqpValue], payload: Bytes) -> Result<Transfer, CodecError> {
    Ok(Transfer {
        handle: list_get(fields, 0).as_u32().unwrap_or(0),
        delivery_id: list_get(fields, 1).as_u32(),
        delivery_tag: list_get(fields, 2).as_binary().cloned(),
        message_format: list_get(fields, 3).as_u32(),
        settled: list_get(fields, 4).as_bool(),
        more: list_get(fields, 5).as_bool().unwrap_or(false),
        rcv_settle_mode: list_get(fields, 6)
            .as_u32()
            .map(|v| RcvSettleMode::from_u8(v as u8)),
        resume: list_get(fields, 8).as_bool().unwrap_or(false),
        aborted: list_get(fields, 9).as_bool().unwrap_or(false),
        batchable: list_get(fields, 10).as_bool().unwrap_or(false),
        payload,
        state: None,
    })
}

fn decode_delivery_state(value: &AmqpValue) -> Option<DeliveryState> {
    match value {
        AmqpValue::Described(desc, _inner) => {
            let d = desc.as_u64()?;
            match d {
                descriptor::ACCEPTED => Some(DeliveryState::Accepted),
                descriptor::REJECTED => Some(DeliveryState::Rejected { error: None }),
                descriptor::RELEASED => Some(DeliveryState::Released),
                descriptor::MODIFIED => Some(DeliveryState::Modified {
                    delivery_failed: false,
                    undeliverable_here: false,
                    message_annotations: None,
                }),
                _ => None,
            }
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

fn decode_disposition(fields: &[AmqpValue]) -> Result<Disposition, CodecError> {
    Ok(Disposition {
        role: Role::from_bool(list_get(fields, 0).as_bool().unwrap_or(false)),
        first: list_get(fields, 1).as_u32().unwrap_or(0),
        last: list_get(fields, 2).as_u32(),
        settled: list_get(fields, 3).as_bool().unwrap_or(false),
        state: decode_delivery_state(list_get(fields, 4)),
        batchable: list_get(fields, 5).as_bool().unwrap_or(false),
    })
}

fn decode_error(value: &AmqpValue) -> Option<AmqpError> {
    match value {
        AmqpValue::Described(_, inner) => {
            let fields = inner.as_list().unwrap_or(&[]);
            Some(AmqpError {
                condition: list_get(fields, 0).as_str().unwrap_or("").to_string(),
                description: list_get(fields, 1).as_str().map(|s| s.to_string()),
                info: None,
            })
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

fn decode_detach(fields: &[AmqpValue]) -> Result<Detach, CodecError> {
    Ok(Detach {
        handle: list_get(fields, 0).as_u32().unwrap_or(0),
        closed: list_get(fields, 1).as_bool().unwrap_or(false),
        error: decode_error(list_get(fields, 2)),
    })
}

fn decode_end(fields: &[AmqpValue]) -> Result<End, CodecError> {
    Ok(End {
        error: decode_error(list_get(fields, 0)),
    })
}

fn decode_close(fields: &[AmqpValue]) -> Result<Close, CodecError> {
    Ok(Close {
        error: decode_error(list_get(fields, 0)),
    })
}

// =============================================================================
// SASL Decode
// =============================================================================

fn decode_sasl_performative(value: AmqpValue) -> Result<SaslPerformative, CodecError> {
    match value {
        AmqpValue::Described(desc, inner) => {
            let d = desc.as_u64().ok_or(CodecError::InvalidPerformative(
                "non-numeric SASL descriptor".into(),
            ))?;
            let fields = match *inner {
                AmqpValue::List(l) => l,
                _ => Vec::new(),
            };
            match d {
                descriptor::SASL_MECHANISMS => {
                    let mechs = match list_get(&fields, 0) {
                        AmqpValue::Symbol(s) => vec![s.clone()],
                        AmqpValue::Array(arr) => arr
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect(),
                        _ => Vec::new(),
                    };
                    Ok(SaslPerformative::Mechanisms(SaslMechanisms {
                        mechanisms: mechs,
                    }))
                }
                descriptor::SASL_INIT => Ok(SaslPerformative::Init(SaslInit {
                    mechanism: list_get(&fields, 0).as_str().unwrap_or("").to_string(),
                    initial_response: list_get(&fields, 1).as_binary().cloned(),
                    hostname: list_get(&fields, 2).as_str().map(|s| s.to_string()),
                })),
                descriptor::SASL_OUTCOME => {
                    let code = list_get(&fields, 0).as_u32().unwrap_or(0) as u8;
                    Ok(SaslPerformative::Outcome(SaslOutcome {
                        code: SaslCode::from_u8(code),
                        additional_data: list_get(&fields, 1).as_binary().cloned(),
                    }))
                }
                _ => Err(CodecError::UnknownDescriptor(d)),
            }
        }
        _ => Err(CodecError::InvalidPerformative(
            "SASL: not described".into(),
        )),
    }
}

// =============================================================================
// Performative Encode
// =============================================================================

/// Encode an AMQP performative into a frame payload (without frame header).
pub fn encode_performative(buf: &mut BytesMut, perf: &Performative) {
    match perf {
        Performative::Open(p) => encode_open(buf, p),
        Performative::Begin(p) => encode_begin(buf, p),
        Performative::Attach(p) => encode_attach(buf, p),
        Performative::Flow(p) => encode_flow(buf, p),
        Performative::Transfer(p) => encode_transfer(buf, p),
        Performative::Disposition(p) => encode_disposition(buf, p),
        Performative::Detach(p) => encode_detach(buf, p),
        Performative::End(p) => encode_end(buf, p),
        Performative::Close(p) => encode_close(buf, p),
    }
}

fn encode_open(buf: &mut BytesMut, p: &Open) {
    let fields = vec![
        AmqpValue::String(p.container_id.clone()),
        p.hostname
            .as_ref()
            .map(|s| AmqpValue::String(s.clone()))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Uint(p.max_frame_size),
        AmqpValue::Ushort(p.channel_max),
        p.idle_timeout
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
    ];
    encode_described_list(buf, descriptor::OPEN, &fields);
}

fn encode_begin(buf: &mut BytesMut, p: &Begin) {
    let fields = vec![
        p.remote_channel
            .map(|v| AmqpValue::Ushort(v))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Uint(p.next_outgoing_id),
        AmqpValue::Uint(p.incoming_window),
        AmqpValue::Uint(p.outgoing_window),
        AmqpValue::Uint(p.handle_max),
    ];
    encode_described_list(buf, descriptor::BEGIN, &fields);
}

fn encode_source(buf: &mut BytesMut, s: &Source) {
    let fields = vec![
        s.address
            .as_ref()
            .map(|a| AmqpValue::String(a.clone()))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Uint(s.durable),
        AmqpValue::Null, // expiry-policy
        AmqpValue::Uint(s.timeout),
        AmqpValue::Boolean(s.dynamic),
    ];
    encode_described_list(buf, descriptor::SOURCE, &fields);
}

fn encode_target(buf: &mut BytesMut, t: &Target) {
    let fields = vec![
        t.address
            .as_ref()
            .map(|a| AmqpValue::String(a.clone()))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Uint(t.durable),
        AmqpValue::Null, // expiry-policy
        AmqpValue::Uint(t.timeout),
        AmqpValue::Boolean(t.dynamic),
    ];
    encode_described_list(buf, descriptor::TARGET, &fields);
}

fn encode_attach(buf: &mut BytesMut, p: &Attach) {
    // Encode source and target as described types
    let mut source_buf = BytesMut::new();
    if let Some(s) = &p.source {
        encode_source(&mut source_buf, s);
    }
    let mut target_buf = BytesMut::new();
    if let Some(t) = &p.target {
        encode_target(&mut target_buf, t);
    }

    // Build the attach fields list manually
    buf.put_u8(format_code::DESCRIBED);
    encode_value(buf, &AmqpValue::Ulong(descriptor::ATTACH));

    // Encode as list
    let mut body = BytesMut::new();
    encode_value(&mut body, &AmqpValue::String(p.name.clone())); // 0: name
    encode_value(&mut body, &AmqpValue::Uint(p.handle)); // 1: handle
    encode_value(&mut body, &AmqpValue::Boolean(p.role.as_bool())); // 2: role
    encode_value(&mut body, &AmqpValue::Ubyte(p.snd_settle_mode as u8)); // 3: snd-settle-mode
    encode_value(&mut body, &AmqpValue::Ubyte(p.rcv_settle_mode as u8)); // 4: rcv-settle-mode
    if p.source.is_some() {
        body.put_slice(&source_buf); // 5: source
    } else {
        encode_value(&mut body, &AmqpValue::Null);
    }
    if p.target.is_some() {
        body.put_slice(&target_buf); // 6: target
    } else {
        encode_value(&mut body, &AmqpValue::Null);
    }
    encode_value(&mut body, &AmqpValue::Null); // 7: unsettled
    encode_value(&mut body, &AmqpValue::Boolean(p.incomplete_unsettled)); // 8: incomplete-unsettled
    encode_value(&mut body, &AmqpValue::Null); // 9: (reserved)
    // 10: initial-delivery-count
    if let Some(c) = p.initial_delivery_count {
        encode_value(&mut body, &AmqpValue::Uint(c));
    } else {
        encode_value(&mut body, &AmqpValue::Null);
    }

    let count = 11u32;
    buf.put_u8(format_code::LIST32);
    buf.put_u32(body.len() as u32 + 4);
    buf.put_u32(count);
    buf.put_slice(&body);
}

fn encode_flow(buf: &mut BytesMut, p: &Flow) {
    let fields = vec![
        p.next_incoming_id
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Uint(p.incoming_window),
        AmqpValue::Uint(p.next_outgoing_id),
        AmqpValue::Uint(p.outgoing_window),
        p.handle
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        p.delivery_count
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        p.link_credit
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        p.available
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Boolean(p.drain),
        AmqpValue::Boolean(p.echo),
    ];
    encode_described_list(buf, descriptor::FLOW, &fields);
}

fn encode_transfer(buf: &mut BytesMut, p: &Transfer) {
    let fields = vec![
        AmqpValue::Uint(p.handle),
        p.delivery_id
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        p.delivery_tag
            .as_ref()
            .map(|v| AmqpValue::Binary(v.clone()))
            .unwrap_or(AmqpValue::Null),
        p.message_format
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        p.settled
            .map(|v| AmqpValue::Boolean(v))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Boolean(p.more),
    ];
    encode_described_list(buf, descriptor::TRANSFER, &fields);
    // Payload follows the performative in the same frame
    buf.put_slice(&p.payload);
}

fn encode_delivery_state(buf: &mut BytesMut, state: &DeliveryState) {
    match state {
        DeliveryState::Accepted => {
            encode_described_list(buf, descriptor::ACCEPTED, &[]);
        }
        DeliveryState::Rejected { .. } => {
            encode_described_list(buf, descriptor::REJECTED, &[]);
        }
        DeliveryState::Released => {
            encode_described_list(buf, descriptor::RELEASED, &[]);
        }
        DeliveryState::Modified {
            delivery_failed,
            undeliverable_here,
            ..
        } => {
            let fields = vec![
                AmqpValue::Boolean(*delivery_failed),
                AmqpValue::Boolean(*undeliverable_here),
            ];
            encode_described_list(buf, descriptor::MODIFIED, &fields);
        }
        DeliveryState::Received {
            section_number,
            section_offset,
        } => {
            let fields = vec![
                AmqpValue::Uint(*section_number),
                AmqpValue::Ulong(*section_offset),
            ];
            encode_described_list(buf, descriptor::RECEIVED, &fields);
        }
    }
}

fn encode_disposition(buf: &mut BytesMut, p: &Disposition) {
    let mut fields = vec![
        AmqpValue::Boolean(p.role.as_bool()),
        AmqpValue::Uint(p.first),
        p.last
            .map(|v| AmqpValue::Uint(v))
            .unwrap_or(AmqpValue::Null),
        AmqpValue::Boolean(p.settled),
    ];
    // State is a described type — encode inline
    if p.state.is_some() {
        // Encode described list inline via a temp buffer
        let mut state_buf = BytesMut::new();
        encode_delivery_state(&mut state_buf, p.state.as_ref().unwrap());
        // We need to write the described_list output as an AmqpValue
        // For simplicity, add null and handle state separately
        fields.push(AmqpValue::Null); // placeholder
    }
    encode_described_list(buf, descriptor::DISPOSITION, &fields[..4]); // Encode without state

    // If state present, we need to re-encode with state inline.
    // This is a simplification — proper encoding would embed the state in the list.
    // For now, encode the full frame manually if state is present.
    if p.state.is_some() {
        buf.clear();
        buf.put_u8(format_code::DESCRIBED);
        encode_value(buf, &AmqpValue::Ulong(descriptor::DISPOSITION));

        let mut body = BytesMut::new();
        encode_value(&mut body, &AmqpValue::Boolean(p.role.as_bool()));
        encode_value(&mut body, &AmqpValue::Uint(p.first));
        if let Some(last) = p.last {
            encode_value(&mut body, &AmqpValue::Uint(last));
        } else {
            encode_value(&mut body, &AmqpValue::Null);
        }
        encode_value(&mut body, &AmqpValue::Boolean(p.settled));
        encode_delivery_state(&mut body, p.state.as_ref().unwrap());
        encode_value(&mut body, &AmqpValue::Boolean(p.batchable));

        buf.put_u8(format_code::LIST32);
        buf.put_u32(body.len() as u32 + 4);
        buf.put_u32(6);
        buf.put_slice(&body);
    }
}

fn encode_error(buf: &mut BytesMut, err: &AmqpError) {
    let fields = vec![
        AmqpValue::Symbol(err.condition.clone()),
        err.description
            .as_ref()
            .map(|s| AmqpValue::String(s.clone()))
            .unwrap_or(AmqpValue::Null),
    ];
    encode_described_list(buf, descriptor::ERROR, &fields);
}

fn encode_detach(buf: &mut BytesMut, p: &Detach) {
    if let Some(err) = &p.error {
        buf.put_u8(format_code::DESCRIBED);
        encode_value(buf, &AmqpValue::Ulong(descriptor::DETACH));
        let mut body = BytesMut::new();
        encode_value(&mut body, &AmqpValue::Uint(p.handle));
        encode_value(&mut body, &AmqpValue::Boolean(p.closed));
        encode_error(&mut body, err);
        buf.put_u8(format_code::LIST32);
        buf.put_u32(body.len() as u32 + 4);
        buf.put_u32(3);
        buf.put_slice(&body);
    } else {
        let fields = vec![AmqpValue::Uint(p.handle), AmqpValue::Boolean(p.closed)];
        encode_described_list(buf, descriptor::DETACH, &fields);
    }
}

fn encode_end(buf: &mut BytesMut, p: &End) {
    if let Some(err) = &p.error {
        buf.put_u8(format_code::DESCRIBED);
        encode_value(buf, &AmqpValue::Ulong(descriptor::END));
        let mut body = BytesMut::new();
        encode_error(&mut body, err);
        buf.put_u8(format_code::LIST32);
        buf.put_u32(body.len() as u32 + 4);
        buf.put_u32(1);
        buf.put_slice(&body);
    } else {
        encode_described_list(buf, descriptor::END, &[]);
    }
}

fn encode_close(buf: &mut BytesMut, p: &Close) {
    if let Some(err) = &p.error {
        buf.put_u8(format_code::DESCRIBED);
        encode_value(buf, &AmqpValue::Ulong(descriptor::CLOSE));
        let mut body = BytesMut::new();
        encode_error(&mut body, err);
        buf.put_u8(format_code::LIST32);
        buf.put_u32(body.len() as u32 + 4);
        buf.put_u32(1);
        buf.put_slice(&body);
    } else {
        encode_described_list(buf, descriptor::CLOSE, &[]);
    }
}

// =============================================================================
// SASL Encode
// =============================================================================

pub fn encode_sasl_mechanisms(buf: &mut BytesMut, mechanisms: &[&str]) {
    let mechs: Vec<AmqpValue> = mechanisms
        .iter()
        .map(|m| AmqpValue::Symbol(m.to_string()))
        .collect();
    let fields = vec![AmqpValue::Array(mechs)];
    encode_described_list(buf, descriptor::SASL_MECHANISMS, &fields);
}

pub fn encode_sasl_outcome(buf: &mut BytesMut, code: SaslCode) {
    let fields = vec![AmqpValue::Ubyte(code as u8)];
    encode_described_list(buf, descriptor::SASL_OUTCOME, &fields);
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_roundtrip() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Null);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        assert_eq!(decoded, AmqpValue::Null);
    }

    #[test]
    fn test_boolean_roundtrip() {
        for v in [true, false] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Boolean(v));
            let mut slice: &[u8] = &buf;
            let decoded = decode_value(&mut slice).unwrap();
            assert_eq!(decoded, AmqpValue::Boolean(v));
        }
    }

    #[test]
    fn test_uint_roundtrip() {
        for v in [0u32, 1, 255, 256, 65535, u32::MAX] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Uint(v));
            let mut slice: &[u8] = &buf;
            let decoded = decode_value(&mut slice).unwrap();
            assert_eq!(decoded, AmqpValue::Uint(v));
        }
    }

    #[test]
    fn test_ulong_roundtrip() {
        for v in [0u64, 1, 255, 256, u64::MAX] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Ulong(v));
            let mut slice: &[u8] = &buf;
            let decoded = decode_value(&mut slice).unwrap();
            assert_eq!(decoded, AmqpValue::Ulong(v));
        }
    }

    #[test]
    fn test_int_roundtrip() {
        for v in [0i32, 1, -1, 127, -128, 1000, -1000, i32::MAX, i32::MIN] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Int(v));
            let mut slice: &[u8] = &buf;
            let decoded = decode_value(&mut slice).unwrap();
            assert_eq!(decoded, AmqpValue::Int(v));
        }
    }

    #[test]
    fn test_string_roundtrip() {
        for s in ["", "hello", "a".repeat(300).as_str()] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::String(s.to_string()));
            let mut slice: &[u8] = &buf;
            let decoded = decode_value(&mut slice).unwrap();
            assert_eq!(decoded, AmqpValue::String(s.to_string()));
        }
    }

    #[test]
    fn test_symbol_roundtrip() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Symbol("amqp:accepted:list".into()));
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        assert_eq!(decoded, AmqpValue::Symbol("amqp:accepted:list".into()));
    }

    #[test]
    fn test_binary_roundtrip() {
        let data = Bytes::from_static(b"hello world");
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Binary(data.clone()));
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        assert_eq!(decoded, AmqpValue::Binary(data));
    }

    #[test]
    fn test_list_roundtrip() {
        let list = AmqpValue::List(vec![
            AmqpValue::Uint(1),
            AmqpValue::String("test".into()),
            AmqpValue::Null,
        ]);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &list);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        assert_eq!(decoded, list);
    }

    #[test]
    fn test_empty_list_roundtrip() {
        let list = AmqpValue::List(Vec::new());
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &list);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        assert_eq!(decoded, list);
    }

    #[test]
    fn test_map_roundtrip() {
        let map = AmqpValue::Map(vec![(AmqpValue::String("key".into()), AmqpValue::Uint(42))]);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &map);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_described_type_roundtrip() {
        let val = AmqpValue::Described(
            Box::new(AmqpValue::Ulong(0x70)),
            Box::new(AmqpValue::List(vec![
                AmqpValue::Boolean(true),
                AmqpValue::Ubyte(4),
            ])),
        );
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        // Verify it decoded to a described type
        match decoded {
            AmqpValue::Described(d, v) => {
                assert_eq!(*d, AmqpValue::Ulong(0x70));
                match *v {
                    AmqpValue::List(l) => assert_eq!(l.len(), 2),
                    _ => panic!("expected list"),
                }
            }
            _ => panic!("expected described"),
        }
    }

    #[test]
    fn test_open_encode_decode() {
        let open = Open {
            container_id: "test-container".into(),
            hostname: Some("localhost".into()),
            max_frame_size: 65536,
            channel_max: 255,
            idle_timeout: Some(30000),
            ..Default::default()
        };

        let mut payload = BytesMut::new();
        encode_open(&mut payload, &open);

        // Wrap in a frame
        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let (frame, consumed) = decode_frame(&frame_buf).unwrap();
        assert_eq!(consumed, frame_buf.len());
        assert_eq!(frame.channel, 0);
        match frame.body {
            FrameBody::Amqp(Performative::Open(decoded)) => {
                assert_eq!(decoded.container_id, "test-container");
                assert_eq!(decoded.hostname.as_deref(), Some("localhost"));
                assert_eq!(decoded.max_frame_size, 65536);
                assert_eq!(decoded.idle_timeout, Some(30000));
            }
            _ => panic!("expected Open"),
        }
    }

    #[test]
    fn test_begin_encode_decode() {
        let begin = Begin {
            remote_channel: Some(0),
            next_outgoing_id: 1,
            incoming_window: 2048,
            outgoing_window: 2048,
            handle_max: 255,
            ..Default::default()
        };

        let mut payload = BytesMut::new();
        encode_begin(&mut payload, &begin);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 1, FRAME_TYPE_AMQP, &payload);

        let (frame, _) = decode_frame(&frame_buf).unwrap();
        assert_eq!(frame.channel, 1);
        match frame.body {
            FrameBody::Amqp(Performative::Begin(decoded)) => {
                assert_eq!(decoded.remote_channel, Some(0));
                assert_eq!(decoded.incoming_window, 2048);
            }
            _ => panic!("expected Begin"),
        }
    }

    #[test]
    fn test_empty_frame() {
        let mut buf = BytesMut::new();
        encode_empty_frame(&mut buf);
        assert_eq!(buf.len(), 8);

        let (frame, consumed) = decode_frame(&buf).unwrap();
        assert_eq!(consumed, 8);
        assert!(matches!(frame.body, FrameBody::Empty));
    }

    #[test]
    fn test_incomplete_frame() {
        let data = [0u8, 0, 0, 20, 2, 0, 0, 0]; // says 20 bytes but only 8
        assert!(matches!(decode_frame(&data), Err(CodecError::Incomplete)));
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let ts = AmqpValue::Timestamp(1709654400000);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &ts);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        assert_eq!(decoded, ts);
    }

    #[test]
    fn test_float_double_roundtrip() {
        let f = AmqpValue::Float(3.14);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &f);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        match decoded {
            AmqpValue::Float(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!("expected float"),
        }

        let d = AmqpValue::Double(2.718281828);
        buf.clear();
        encode_value(&mut buf, &d);
        let mut slice: &[u8] = &buf;
        let decoded = decode_value(&mut slice).unwrap();
        match decoded {
            AmqpValue::Double(v) => assert!((v - 2.718281828).abs() < 0.000001),
            _ => panic!("expected double"),
        }
    }

    #[test]
    fn test_sasl_mechanisms_encode() {
        let mut buf = BytesMut::new();
        encode_sasl_mechanisms(&mut buf, &["PLAIN", "ANONYMOUS"]);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_flow_encode_decode() {
        let flow = Flow {
            next_incoming_id: Some(5),
            incoming_window: 2048,
            next_outgoing_id: 10,
            outgoing_window: 2048,
            handle: Some(0),
            delivery_count: Some(3),
            link_credit: Some(100),
            ..Default::default()
        };

        let mut payload = BytesMut::new();
        encode_flow(&mut payload, &flow);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let (frame, _) = decode_frame(&frame_buf).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Flow(decoded)) => {
                assert_eq!(decoded.next_incoming_id, Some(5));
                assert_eq!(decoded.handle, Some(0));
                assert_eq!(decoded.link_credit, Some(100));
            }
            _ => panic!("expected Flow"),
        }
    }
}
