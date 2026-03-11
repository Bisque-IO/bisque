//! AMQP 1.0 binary codec.
//!
//! Handles the AMQP 1.0 type system encoding/decoding and frame parsing.
//! Frame structure: [4:size][1:doff][1:type][2:channel][payload]
//!
//! Zero-copy decode via `BytesCursor`: strings and binary data are sliced
//! directly from the input `Bytes` buffer with no intermediate allocations.

use bytes::{BufMut, Bytes, BytesMut};
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
// BytesCursor — zero-copy reader over Bytes
// =============================================================================

/// Position-tracked reader over a `Bytes` buffer that yields zero-copy slices.
pub struct BytesCursor {
    data: Bytes,
    pos: usize,
}

impl BytesCursor {
    /// Create a new cursor over the given `Bytes`.
    #[inline]
    pub fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    /// Remaining bytes available.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    /// Peek at the remaining data as a slice (no advance).
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.pos..]
    }

    /// Advance the position by `n` bytes.
    #[inline]
    fn advance(&mut self, n: usize) {
        self.pos += n;
    }

    /// Ensure at least `n` bytes remain.
    #[inline]
    fn ensure(&self, n: usize) -> Result<(), CodecError> {
        if self.remaining() < n {
            Err(CodecError::UnexpectedEof)
        } else {
            Ok(())
        }
    }

    /// Read a single byte.
    #[inline]
    fn read_u8(&mut self) -> Result<u8, CodecError> {
        self.ensure(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    /// Read 2 bytes as big-endian u16.
    #[inline]
    fn read_u16(&mut self) -> Result<u16, CodecError> {
        self.ensure(2)?;
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    /// Read 4 bytes as big-endian u32.
    #[inline]
    fn read_u32(&mut self) -> Result<u32, CodecError> {
        self.ensure(4)?;
        let s = &self.data[self.pos..self.pos + 4];
        let v = u32::from_be_bytes([s[0], s[1], s[2], s[3]]);
        self.pos += 4;
        Ok(v)
    }

    /// Read 8 bytes as big-endian u64.
    #[inline]
    fn read_u64(&mut self) -> Result<u64, CodecError> {
        self.ensure(8)?;
        let s = &self.data[self.pos..self.pos + 8];
        let v = u64::from_be_bytes(s.try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    /// Read 8 bytes as big-endian i64.
    #[inline]
    fn read_i64(&mut self) -> Result<i64, CodecError> {
        self.ensure(8)?;
        let s = &self.data[self.pos..self.pos + 8];
        let v = i64::from_be_bytes(s.try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    /// Zero-copy slice of `len` bytes from the underlying `Bytes`.
    #[inline]
    fn read_bytes(&mut self, len: usize) -> Result<Bytes, CodecError> {
        self.ensure(len)?;
        let slice = self.data.slice(self.pos..self.pos + len);
        self.pos += len;
        Ok(slice)
    }

    /// Zero-copy UTF-8 validated WireString.
    #[inline]
    fn read_wire_string(&mut self, len: usize) -> Result<WireString, CodecError> {
        self.ensure(len)?;
        // Validate UTF-8 on the slice without copying
        std::str::from_utf8(&self.data[self.pos..self.pos + len])
            .map_err(|_| CodecError::InvalidUtf8)?;
        let b = self.data.slice(self.pos..self.pos + len);
        self.pos += len;
        Ok(WireString::from_utf8_unchecked(b))
    }

    /// Read a raw slice reference (no copy, for fixed-size reads).
    #[inline]
    fn read_slice(&self, len: usize) -> Result<&[u8], CodecError> {
        if self.pos + len > self.data.len() {
            return Err(CodecError::UnexpectedEof);
        }
        Ok(&self.data[self.pos..self.pos + len])
    }

    /// Consume all remaining data as zero-copy Bytes.
    #[inline]
    fn read_remaining(&mut self) -> Bytes {
        if self.pos >= self.data.len() {
            Bytes::new()
        } else {
            let b = self.data.slice(self.pos..);
            self.pos = self.data.len();
            b
        }
    }
}

// =============================================================================
// Decode — zero-copy via BytesCursor
// =============================================================================

/// Decode an AMQP 1.0 typed value from the cursor (zero-copy for strings/binary).
pub fn decode_value(cur: &mut BytesCursor) -> Result<AmqpValue, CodecError> {
    let code = cur.read_u8()?;

    match code {
        format_code::NULL => Ok(AmqpValue::Null),
        format_code::BOOLEAN_TRUE => Ok(AmqpValue::Boolean(true)),
        format_code::BOOLEAN_FALSE => Ok(AmqpValue::Boolean(false)),
        format_code::BOOLEAN => {
            let v = cur.read_u8()? != 0;
            Ok(AmqpValue::Boolean(v))
        }
        format_code::UBYTE => Ok(AmqpValue::Ubyte(cur.read_u8()?)),
        format_code::USHORT => Ok(AmqpValue::Ushort(cur.read_u16()?)),
        format_code::UINT => Ok(AmqpValue::Uint(cur.read_u32()?)),
        format_code::UINT_SMALL => Ok(AmqpValue::Uint(cur.read_u8()? as u32)),
        format_code::UINT_ZERO => Ok(AmqpValue::Uint(0)),
        format_code::ULONG => Ok(AmqpValue::Ulong(cur.read_u64()?)),
        format_code::ULONG_SMALL => Ok(AmqpValue::Ulong(cur.read_u8()? as u64)),
        format_code::ULONG_ZERO => Ok(AmqpValue::Ulong(0)),
        format_code::BYTE => Ok(AmqpValue::Byte(cur.read_u8()? as i8)),
        format_code::SHORT => {
            cur.ensure(2)?;
            let v = i16::from_be_bytes([cur.as_slice()[0], cur.as_slice()[1]]);
            cur.advance(2);
            Ok(AmqpValue::Short(v))
        }
        format_code::INT => {
            let v = cur.read_u32()? as i32;
            Ok(AmqpValue::Int(v))
        }
        format_code::INT_SMALL => Ok(AmqpValue::Int(cur.read_u8()? as i8 as i32)),
        format_code::LONG => Ok(AmqpValue::Long(cur.read_i64()?)),
        format_code::LONG_SMALL => Ok(AmqpValue::Long(cur.read_u8()? as i8 as i64)),
        format_code::FLOAT => {
            let bits = cur.read_u32()?;
            Ok(AmqpValue::Float(f32::from_bits(bits)))
        }
        format_code::DOUBLE => {
            let bits = cur.read_u64()?;
            Ok(AmqpValue::Double(f64::from_bits(bits)))
        }
        format_code::TIMESTAMP => Ok(AmqpValue::Timestamp(cur.read_i64()?)),
        format_code::UUID => {
            cur.ensure(16)?;
            let mut v = [0u8; 16];
            v.copy_from_slice(cur.read_slice(16)?);
            cur.advance(16);
            Ok(AmqpValue::Uuid(v))
        }
        // Zero-copy binary
        format_code::BINARY8 => {
            let len = cur.read_u8()? as usize;
            Ok(AmqpValue::Binary(cur.read_bytes(len)?))
        }
        format_code::BINARY32 => {
            let len = cur.read_u32()? as usize;
            Ok(AmqpValue::Binary(cur.read_bytes(len)?))
        }
        // Zero-copy string
        format_code::STRING8 => {
            let len = cur.read_u8()? as usize;
            Ok(AmqpValue::String(cur.read_wire_string(len)?))
        }
        format_code::STRING32 => {
            let len = cur.read_u32()? as usize;
            Ok(AmqpValue::String(cur.read_wire_string(len)?))
        }
        // Zero-copy symbol
        format_code::SYMBOL8 => {
            let len = cur.read_u8()? as usize;
            Ok(AmqpValue::Symbol(cur.read_wire_string(len)?))
        }
        format_code::SYMBOL32 => {
            let len = cur.read_u32()? as usize;
            Ok(AmqpValue::Symbol(cur.read_wire_string(len)?))
        }
        format_code::LIST0 => Ok(AmqpValue::List(Vec::new())),
        format_code::LIST8 => {
            cur.ensure(2)?;
            let _size = cur.as_slice()[0] as usize;
            let count = cur.as_slice()[1] as usize;
            cur.advance(2);
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_value(cur)?);
            }
            Ok(AmqpValue::List(items))
        }
        format_code::LIST32 => {
            let _size = cur.read_u32()? as usize;
            let count = cur.read_u32()? as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_value(cur)?);
            }
            Ok(AmqpValue::List(items))
        }
        format_code::MAP8 => {
            cur.ensure(2)?;
            let _size = cur.as_slice()[0] as usize;
            let count = cur.as_slice()[1] as usize; // count is number of key-value pairs * 2
            cur.advance(2);
            let pairs = count / 2;
            let mut items = Vec::with_capacity(pairs);
            for _ in 0..pairs {
                let k = decode_value(cur)?;
                let v = decode_value(cur)?;
                items.push((k, v));
            }
            Ok(AmqpValue::Map(items))
        }
        format_code::MAP32 => {
            let _size = cur.read_u32()? as usize;
            let count = cur.read_u32()? as usize;
            let pairs = count / 2;
            let mut items = Vec::with_capacity(pairs);
            for _ in 0..pairs {
                let k = decode_value(cur)?;
                let v = decode_value(cur)?;
                items.push((k, v));
            }
            Ok(AmqpValue::Map(items))
        }
        // Fixed: O(n) array decode instead of O(n²)
        format_code::ARRAY8 | format_code::ARRAY32 => {
            let (count, _size) = if code == format_code::ARRAY8 {
                cur.ensure(2)?;
                let s = cur.as_slice()[0] as usize;
                let c = cur.as_slice()[1] as usize;
                cur.advance(2);
                (c, s)
            } else {
                let s = cur.read_u32()? as usize;
                let c = cur.read_u32()? as usize;
                (c, s)
            };
            let mut items = Vec::with_capacity(count);
            if count > 0 {
                // Read the shared constructor (format code)
                let elem_code = cur.read_u8()?;
                for _ in 0..count {
                    // Decode element by type directly, avoiding the per-element
                    // temp-buffer copy that existed before.
                    let val = decode_array_element(cur, elem_code)?;
                    items.push(val);
                }
            }
            Ok(AmqpValue::Array(items))
        }
        format_code::DESCRIBED => {
            let descriptor = decode_value(cur)?;
            let value = decode_value(cur)?;
            Ok(AmqpValue::Described(Box::new(descriptor), Box::new(value)))
        }
        _ => Err(CodecError::InvalidFormatCode(code)),
    }
}

/// Decode a single array element using the shared format code.
/// This avoids the O(n²) copy that the old implementation had.
fn decode_array_element(cur: &mut BytesCursor, code: u8) -> Result<AmqpValue, CodecError> {
    match code {
        format_code::NULL => Ok(AmqpValue::Null),
        format_code::BOOLEAN_TRUE => Ok(AmqpValue::Boolean(true)),
        format_code::BOOLEAN_FALSE => Ok(AmqpValue::Boolean(false)),
        format_code::BOOLEAN => Ok(AmqpValue::Boolean(cur.read_u8()? != 0)),
        format_code::UBYTE => Ok(AmqpValue::Ubyte(cur.read_u8()?)),
        format_code::USHORT => Ok(AmqpValue::Ushort(cur.read_u16()?)),
        format_code::UINT => Ok(AmqpValue::Uint(cur.read_u32()?)),
        format_code::UINT_SMALL => Ok(AmqpValue::Uint(cur.read_u8()? as u32)),
        format_code::UINT_ZERO => Ok(AmqpValue::Uint(0)),
        format_code::ULONG => Ok(AmqpValue::Ulong(cur.read_u64()?)),
        format_code::ULONG_SMALL => Ok(AmqpValue::Ulong(cur.read_u8()? as u64)),
        format_code::ULONG_ZERO => Ok(AmqpValue::Ulong(0)),
        format_code::BYTE => Ok(AmqpValue::Byte(cur.read_u8()? as i8)),
        format_code::SHORT => {
            cur.ensure(2)?;
            let v = i16::from_be_bytes([cur.as_slice()[0], cur.as_slice()[1]]);
            cur.advance(2);
            Ok(AmqpValue::Short(v))
        }
        format_code::INT => Ok(AmqpValue::Int(cur.read_u32()? as i32)),
        format_code::INT_SMALL => Ok(AmqpValue::Int(cur.read_u8()? as i8 as i32)),
        format_code::LONG => Ok(AmqpValue::Long(cur.read_i64()?)),
        format_code::LONG_SMALL => Ok(AmqpValue::Long(cur.read_u8()? as i8 as i64)),
        format_code::FLOAT => Ok(AmqpValue::Float(f32::from_bits(cur.read_u32()?))),
        format_code::DOUBLE => Ok(AmqpValue::Double(f64::from_bits(cur.read_u64()?))),
        format_code::TIMESTAMP => Ok(AmqpValue::Timestamp(cur.read_i64()?)),
        format_code::UUID => {
            cur.ensure(16)?;
            let mut v = [0u8; 16];
            v.copy_from_slice(cur.read_slice(16)?);
            cur.advance(16);
            Ok(AmqpValue::Uuid(v))
        }
        format_code::BINARY8 => {
            let len = cur.read_u8()? as usize;
            Ok(AmqpValue::Binary(cur.read_bytes(len)?))
        }
        format_code::BINARY32 => {
            let len = cur.read_u32()? as usize;
            Ok(AmqpValue::Binary(cur.read_bytes(len)?))
        }
        format_code::STRING8 => {
            let len = cur.read_u8()? as usize;
            Ok(AmqpValue::String(cur.read_wire_string(len)?))
        }
        format_code::STRING32 => {
            let len = cur.read_u32()? as usize;
            Ok(AmqpValue::String(cur.read_wire_string(len)?))
        }
        format_code::SYMBOL8 => {
            let len = cur.read_u8()? as usize;
            Ok(AmqpValue::Symbol(cur.read_wire_string(len)?))
        }
        format_code::SYMBOL32 => {
            let len = cur.read_u32()? as usize;
            Ok(AmqpValue::Symbol(cur.read_wire_string(len)?))
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
            encode_wire_string(buf, format_code::STRING8, format_code::STRING32, v);
        }
        AmqpValue::Symbol(v) => {
            encode_wire_string(buf, format_code::SYMBOL8, format_code::SYMBOL32, v);
        }
        AmqpValue::Described(d, v) => {
            buf.put_u8(format_code::DESCRIBED);
            encode_value(buf, d);
            encode_value(buf, v);
        }
        AmqpValue::List(items) => {
            encode_list(buf, items);
        }
        AmqpValue::Map(pairs) => {
            encode_map(buf, pairs);
        }
        AmqpValue::Array(items) => {
            if items.is_empty() {
                buf.put_u8(format_code::ARRAY8);
                buf.put_u8(1); // size = 1 (just the constructor)
                buf.put_u8(0); // count = 0
                buf.put_u8(format_code::NULL); // constructor (arbitrary for empty)
            } else {
                // For simplicity, encode as list (arrays require shared constructor)
                encode_list(buf, items);
            }
        }
    }
}

/// Encode a WireString with the appropriate short/long format code.
#[inline]
fn encode_wire_string(buf: &mut BytesMut, code8: u8, code32: u8, s: &WireString) {
    let bytes = s.as_bytes();
    if bytes.len() <= 255 {
        buf.put_u8(code8);
        buf.put_u8(bytes.len() as u8);
    } else {
        buf.put_u8(code32);
        buf.put_u32(bytes.len() as u32);
    }
    buf.put_slice(bytes);
}

// =============================================================================
// Direct Encode Helpers — bypass AmqpValue enum construction
// =============================================================================

// These write AMQP 1.0 type encodings directly into the buffer, avoiding the
// overhead of constructing intermediate AmqpValue instances on the stack and
// dispatching through the encode_value match. Used by performative encoders.

#[inline(always)]
fn put_null(buf: &mut BytesMut) {
    buf.put_u8(format_code::NULL);
}

#[inline(always)]
fn put_bool(buf: &mut BytesMut, v: bool) {
    buf.put_u8(if v {
        format_code::BOOLEAN_TRUE
    } else {
        format_code::BOOLEAN_FALSE
    });
}

#[inline(always)]
fn put_ubyte(buf: &mut BytesMut, v: u8) {
    buf.put_u8(format_code::UBYTE);
    buf.put_u8(v);
}

#[inline(always)]
fn put_ushort(buf: &mut BytesMut, v: u16) {
    buf.put_u8(format_code::USHORT);
    buf.put_u16(v);
}

#[inline(always)]
fn put_uint(buf: &mut BytesMut, v: u32) {
    if v == 0 {
        buf.put_u8(format_code::UINT_ZERO);
    } else if v <= 255 {
        buf.put_u8(format_code::UINT_SMALL);
        buf.put_u8(v as u8);
    } else {
        buf.put_u8(format_code::UINT);
        buf.put_u32(v);
    }
}

#[inline(always)]
fn put_ulong(buf: &mut BytesMut, v: u64) {
    if v == 0 {
        buf.put_u8(format_code::ULONG_ZERO);
    } else if v <= 255 {
        buf.put_u8(format_code::ULONG_SMALL);
        buf.put_u8(v as u8);
    } else {
        buf.put_u8(format_code::ULONG);
        buf.put_u64(v);
    }
}

#[inline(always)]
fn put_string(buf: &mut BytesMut, s: &WireString) {
    encode_wire_string(buf, format_code::STRING8, format_code::STRING32, s);
}

#[inline(always)]
fn put_symbol(buf: &mut BytesMut, s: &WireString) {
    encode_wire_string(buf, format_code::SYMBOL8, format_code::SYMBOL32, s);
}

#[inline(always)]
fn put_opt_uint(buf: &mut BytesMut, v: Option<u32>) {
    match v {
        Some(v) => put_uint(buf, v),
        None => buf.put_u8(format_code::NULL),
    }
}

#[inline(always)]
#[allow(dead_code)]
fn put_opt_ulong(buf: &mut BytesMut, v: Option<u64>) {
    match v {
        Some(v) => put_ulong(buf, v),
        None => buf.put_u8(format_code::NULL),
    }
}

#[inline(always)]
fn put_opt_ushort(buf: &mut BytesMut, v: Option<u16>) {
    match v {
        Some(v) => put_ushort(buf, v),
        None => buf.put_u8(format_code::NULL),
    }
}

#[inline(always)]
fn put_opt_bool(buf: &mut BytesMut, v: Option<bool>) {
    match v {
        Some(v) => put_bool(buf, v),
        None => buf.put_u8(format_code::NULL),
    }
}

#[inline(always)]
fn put_opt_string(buf: &mut BytesMut, v: Option<&WireString>) {
    match v {
        Some(s) => put_string(buf, s),
        None => buf.put_u8(format_code::NULL),
    }
}

#[inline(always)]
fn put_opt_binary(buf: &mut BytesMut, v: Option<&Bytes>) {
    match v {
        Some(b) => {
            if b.len() <= 255 {
                buf.put_u8(format_code::BINARY8);
                buf.put_u8(b.len() as u8);
            } else {
                buf.put_u8(format_code::BINARY32);
                buf.put_u32(b.len() as u32);
            }
            buf.put_slice(b);
        }
        None => buf.put_u8(format_code::NULL),
    }
}

/// Write an empty described list: `[DESCRIBED][descriptor][LIST0]`.
#[inline(always)]
fn put_empty_described_list(buf: &mut BytesMut, descriptor_val: u64) {
    buf.put_u8(format_code::DESCRIBED);
    put_ulong(buf, descriptor_val);
    buf.put_u8(format_code::LIST0);
}

/// Begin a non-empty described list with LIST32 header. Returns `(size_pos, body_start)`.
#[inline]
fn begin_described_list(buf: &mut BytesMut, descriptor_val: u64, count: u32) -> (usize, usize) {
    buf.put_u8(format_code::DESCRIBED);
    put_ulong(buf, descriptor_val);
    buf.put_u8(format_code::LIST32);
    let size_pos = buf.len();
    buf.put_u32(0); // size placeholder
    buf.put_u32(count);
    (size_pos, buf.len())
}

/// Patch the size field of a list started with `begin_described_list`.
#[inline]
fn finish_described_list(buf: &mut BytesMut, size_pos: usize, body_start: usize) {
    let body_len = buf.len() - body_start;
    let size = (body_len + 4) as u32; // +4 for the count field
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
}

/// Encode an array of `&str` as AMQP symbols directly (no heap allocation).
fn encode_str_symbol_array(buf: &mut BytesMut, symbols: &[&str]) {
    if symbols.is_empty() {
        buf.put_u8(format_code::ARRAY8);
        buf.put_u8(1); // size = 1 (just constructor)
        buf.put_u8(0); // count = 0
        buf.put_u8(format_code::SYMBOL8);
        return;
    }
    buf.put_u8(format_code::ARRAY32);
    let size_pos = buf.len();
    buf.put_u32(0); // size placeholder
    let after_size = buf.len();
    buf.put_u32(symbols.len() as u32);
    buf.put_u8(format_code::SYMBOL8); // shared constructor
    for s in symbols {
        buf.put_u8(s.len() as u8);
        buf.put_slice(s.as_bytes());
    }
    let size = (buf.len() - after_size) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
}

/// Encode a list using a two-pass approach: reserve header, encode body, patch size.
/// Avoids allocating a temporary BytesMut.
fn encode_list(buf: &mut BytesMut, items: &[AmqpValue]) {
    if items.is_empty() {
        buf.put_u8(format_code::LIST0);
        return;
    }
    // Always use LIST32 to avoid needing to know size upfront.
    // The overhead is 4 extra bytes vs LIST8, but we avoid a temp buffer allocation.
    buf.put_u8(format_code::LIST32);
    let size_pos = buf.len();
    buf.put_u32(0); // placeholder for size
    buf.put_u32(items.len() as u32);
    let body_start = buf.len();
    for item in items {
        encode_value(buf, item);
    }
    let body_len = buf.len() - body_start;
    let size = (body_len + 4) as u32; // +4 for count field
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
}

/// Encode a map using two-pass: reserve header, encode body, patch size.
fn encode_map(buf: &mut BytesMut, pairs: &[(AmqpValue, AmqpValue)]) {
    buf.put_u8(format_code::MAP32);
    let size_pos = buf.len();
    buf.put_u32(0); // placeholder for size
    let count = (pairs.len() * 2) as u32;
    buf.put_u32(count);
    let body_start = buf.len();
    for (k, v) in pairs {
        encode_value(buf, k);
        encode_value(buf, v);
    }
    let body_len = buf.len() - body_start;
    let size = (body_len + 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
}

/// Encode a described list (used for performatives).
/// Trims trailing nulls and writes directly — no intermediate Vec allocation.
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
    encode_list(buf, trimmed);
}

// =============================================================================
// Frame Decode/Encode
// =============================================================================

/// Try to decode a frame from the buffer.
/// Returns `(frame, bytes_consumed)` or `Incomplete` if more data needed.
///
/// Takes `Bytes` for zero-copy slicing of payloads and string fields.
pub fn decode_frame(data: &Bytes) -> Result<(AmqpFrame, usize), CodecError> {
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

    let payload_bytes = data.slice(header_size..size as usize);

    let body = if payload_bytes.is_empty() {
        FrameBody::Empty
    } else if frame_type == FRAME_TYPE_SASL {
        let mut cur = BytesCursor::new(payload_bytes);
        let value = decode_value(&mut cur)?;
        let sasl = decode_sasl_performative(value)?;
        FrameBody::Sasl(sasl)
    } else {
        let mut cur = BytesCursor::new(payload_bytes);
        let value = decode_value(&mut cur)?;
        // Zero-copy: remaining bytes become Transfer payload
        let remaining = cur.read_remaining();
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

/// Backwards-compatible: decode from a byte slice (copies into Bytes first).
pub fn decode_frame_slice(data: &[u8]) -> Result<(AmqpFrame, usize), CodecError> {
    // For BytesMut read buffers, we freeze first for zero-copy
    let bytes = Bytes::copy_from_slice(data);
    decode_frame(&bytes)
}

/// Encode a frame into the buffer.
#[inline]
pub fn encode_frame(buf: &mut BytesMut, channel: u16, frame_type: u8, payload: &[u8]) {
    let size = 8 + payload.len();
    buf.put_u32(size as u32);
    buf.put_u8(2); // doff = 2 (8 bytes header)
    buf.put_u8(frame_type);
    buf.put_u16(channel);
    buf.put_slice(payload);
}

/// Encode a performative directly into the frame buffer (single allocation).
/// Writes the frame header placeholder, encodes the performative body,
/// then patches the frame size. Avoids the intermediate payload BytesMut.
pub fn encode_framed_performative(
    buf: &mut BytesMut,
    channel: u16,
    frame_type: u8,
    perf: &Performative,
) {
    let frame_start = buf.len();
    // Write frame header placeholder
    buf.put_u32(0); // size placeholder
    buf.put_u8(2); // doff
    buf.put_u8(frame_type);
    buf.put_u16(channel);
    // Encode performative directly into frame body
    encode_performative(buf, perf);
    // Patch frame size
    let frame_size = (buf.len() - frame_start) as u32;
    buf[frame_start..frame_start + 4].copy_from_slice(&frame_size.to_be_bytes());
}

/// Encode an empty heartbeat frame.
#[inline]
pub fn encode_empty_frame(buf: &mut BytesMut) {
    buf.put_u32(8); // size = 8
    buf.put_u8(2); // doff
    buf.put_u8(FRAME_TYPE_AMQP);
    buf.put_u16(0); // channel
}

// =============================================================================
// Performative Decode
// =============================================================================

#[inline]
fn list_get(list: &[AmqpValue], idx: usize) -> &AmqpValue {
    list.get(idx).unwrap_or(&AmqpValue::Null)
}

/// Extract a WireString from a list field, or return an empty WireString.
#[inline]
fn list_get_wire_string(list: &[AmqpValue], idx: usize) -> WireString {
    list.get(idx)
        .and_then(|v| v.as_wire_string())
        .cloned()
        .unwrap_or_default()
}

/// Extract an optional WireString from a list field.
#[inline]
fn list_get_opt_wire_string(list: &[AmqpValue], idx: usize) -> Option<WireString> {
    list.get(idx).and_then(|v| v.as_wire_string()).cloned()
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
        container_id: list_get_wire_string(fields, 0),
        hostname: list_get_opt_wire_string(fields, 1),
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
                address: list_get_opt_wire_string(fields, 0),
                durable: list_get(fields, 1).as_u32().unwrap_or(0),
                dynamic: list_get(fields, 4).as_bool().unwrap_or(false),
                distribution_mode: list_get_opt_wire_string(fields, 6),
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
                address: list_get_opt_wire_string(fields, 0),
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
        name: list_get_wire_string(fields, 0),
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
                condition: list_get_wire_string(fields, 0),
                description: list_get_opt_wire_string(fields, 1),
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
                        AmqpValue::Symbol(s) => {
                            let mut v = smallvec::SmallVec::new();
                            v.push(s.clone());
                            v
                        }
                        AmqpValue::Array(arr) => arr
                            .iter()
                            .filter_map(|v| v.as_wire_string().cloned())
                            .collect(),
                        _ => smallvec::SmallVec::new(),
                    };
                    Ok(SaslPerformative::Mechanisms(SaslMechanisms {
                        mechanisms: mechs,
                    }))
                }
                descriptor::SASL_INIT => Ok(SaslPerformative::Init(SaslInit {
                    mechanism: list_get_wire_string(&fields, 0),
                    initial_response: list_get(&fields, 1).as_binary().cloned(),
                    hostname: list_get_opt_wire_string(&fields, 2),
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
    let count = if p.idle_timeout.is_some() { 5u32 } else { 4 };
    let (sp, bs) = begin_described_list(buf, descriptor::OPEN, count);
    put_string(buf, &p.container_id);
    put_opt_string(buf, p.hostname.as_ref());
    put_uint(buf, p.max_frame_size);
    put_ushort(buf, p.channel_max);
    if let Some(timeout) = p.idle_timeout {
        put_uint(buf, timeout);
    }
    finish_described_list(buf, sp, bs);
}

fn encode_begin(buf: &mut BytesMut, p: &Begin) {
    let (sp, bs) = begin_described_list(buf, descriptor::BEGIN, 5);
    put_opt_ushort(buf, p.remote_channel);
    put_uint(buf, p.next_outgoing_id);
    put_uint(buf, p.incoming_window);
    put_uint(buf, p.outgoing_window);
    put_uint(buf, p.handle_max);
    finish_described_list(buf, sp, bs);
}

fn encode_source(buf: &mut BytesMut, s: &Source) {
    let (sp, bs) = begin_described_list(buf, descriptor::SOURCE, 5);
    put_opt_string(buf, s.address.as_ref());
    put_uint(buf, s.durable);
    put_null(buf); // expiry-policy
    put_uint(buf, s.timeout);
    put_bool(buf, s.dynamic);
    finish_described_list(buf, sp, bs);
}

fn encode_target(buf: &mut BytesMut, t: &Target) {
    let (sp, bs) = begin_described_list(buf, descriptor::TARGET, 5);
    put_opt_string(buf, t.address.as_ref());
    put_uint(buf, t.durable);
    put_null(buf); // expiry-policy
    put_uint(buf, t.timeout);
    put_bool(buf, t.dynamic);
    finish_described_list(buf, sp, bs);
}

fn encode_attach(buf: &mut BytesMut, p: &Attach) {
    let (sp, bs) = begin_described_list(buf, descriptor::ATTACH, 11);
    put_string(buf, &p.name); // 0: name
    put_uint(buf, p.handle); // 1: handle
    put_bool(buf, p.role.as_bool()); // 2: role
    put_ubyte(buf, p.snd_settle_mode as u8); // 3: snd-settle-mode
    put_ubyte(buf, p.rcv_settle_mode as u8); // 4: rcv-settle-mode
    if let Some(s) = &p.source {
        // 5: source
        encode_source(buf, s);
    } else {
        put_null(buf);
    }
    if let Some(t) = &p.target {
        // 6: target
        encode_target(buf, t);
    } else {
        put_null(buf);
    }
    put_null(buf); // 7: unsettled
    put_bool(buf, p.incomplete_unsettled); // 8: incomplete-unsettled
    put_null(buf); // 9: reserved
    put_opt_uint(buf, p.initial_delivery_count); // 10: initial-delivery-count
    finish_described_list(buf, sp, bs);
}

fn encode_flow(buf: &mut BytesMut, p: &Flow) {
    let (sp, bs) = begin_described_list(buf, descriptor::FLOW, 10);
    put_opt_uint(buf, p.next_incoming_id);
    put_uint(buf, p.incoming_window);
    put_uint(buf, p.next_outgoing_id);
    put_uint(buf, p.outgoing_window);
    put_opt_uint(buf, p.handle);
    put_opt_uint(buf, p.delivery_count);
    put_opt_uint(buf, p.link_credit);
    put_opt_uint(buf, p.available);
    put_bool(buf, p.drain);
    put_bool(buf, p.echo);
    finish_described_list(buf, sp, bs);
}

fn encode_transfer(buf: &mut BytesMut, p: &Transfer) {
    let (sp, bs) = begin_described_list(buf, descriptor::TRANSFER, 6);
    put_uint(buf, p.handle);
    put_opt_uint(buf, p.delivery_id);
    put_opt_binary(buf, p.delivery_tag.as_ref());
    put_opt_uint(buf, p.message_format);
    put_opt_bool(buf, p.settled);
    put_bool(buf, p.more);
    finish_described_list(buf, sp, bs);
    // Payload follows the performative in the same frame
    buf.put_slice(&p.payload);
}

fn encode_delivery_state(buf: &mut BytesMut, state: &DeliveryState) {
    match state {
        DeliveryState::Accepted => put_empty_described_list(buf, descriptor::ACCEPTED),
        DeliveryState::Rejected { .. } => put_empty_described_list(buf, descriptor::REJECTED),
        DeliveryState::Released => put_empty_described_list(buf, descriptor::RELEASED),
        DeliveryState::Modified {
            delivery_failed,
            undeliverable_here,
            ..
        } => {
            let (sp, bs) = begin_described_list(buf, descriptor::MODIFIED, 2);
            put_bool(buf, *delivery_failed);
            put_bool(buf, *undeliverable_here);
            finish_described_list(buf, sp, bs);
        }
        DeliveryState::Received {
            section_number,
            section_offset,
        } => {
            let (sp, bs) = begin_described_list(buf, descriptor::RECEIVED, 2);
            put_uint(buf, *section_number);
            put_ulong(buf, *section_offset);
            finish_described_list(buf, sp, bs);
        }
    }
}

fn encode_disposition(buf: &mut BytesMut, p: &Disposition) {
    let count = if p.state.is_some() { 6u32 } else { 4 };
    let (sp, bs) = begin_described_list(buf, descriptor::DISPOSITION, count);
    put_bool(buf, p.role.as_bool());
    put_uint(buf, p.first);
    put_opt_uint(buf, p.last);
    put_bool(buf, p.settled);
    if let Some(state) = &p.state {
        encode_delivery_state(buf, state);
        put_bool(buf, p.batchable);
    }
    finish_described_list(buf, sp, bs);
}

fn encode_error(buf: &mut BytesMut, err: &AmqpError) {
    let count = if err.description.is_some() { 2u32 } else { 1 };
    let (sp, bs) = begin_described_list(buf, descriptor::ERROR, count);
    put_symbol(buf, &err.condition);
    if let Some(desc) = &err.description {
        put_string(buf, desc);
    }
    finish_described_list(buf, sp, bs);
}

fn encode_detach(buf: &mut BytesMut, p: &Detach) {
    let count = if p.error.is_some() { 3u32 } else { 2 };
    let (sp, bs) = begin_described_list(buf, descriptor::DETACH, count);
    put_uint(buf, p.handle);
    put_bool(buf, p.closed);
    if let Some(err) = &p.error {
        encode_error(buf, err);
    }
    finish_described_list(buf, sp, bs);
}

fn encode_end(buf: &mut BytesMut, p: &End) {
    if let Some(err) = &p.error {
        let (sp, bs) = begin_described_list(buf, descriptor::END, 1);
        encode_error(buf, err);
        finish_described_list(buf, sp, bs);
    } else {
        put_empty_described_list(buf, descriptor::END);
    }
}

fn encode_close(buf: &mut BytesMut, p: &Close) {
    if let Some(err) = &p.error {
        let (sp, bs) = begin_described_list(buf, descriptor::CLOSE, 1);
        encode_error(buf, err);
        finish_described_list(buf, sp, bs);
    } else {
        put_empty_described_list(buf, descriptor::CLOSE);
    }
}

// =============================================================================
// SASL Encode
// =============================================================================

/// Encode SASL mechanisms body (no frame header). No heap allocation.
pub fn encode_sasl_mechanisms(buf: &mut BytesMut, mechanisms: &[&str]) {
    let (sp, bs) = begin_described_list(buf, descriptor::SASL_MECHANISMS, 1);
    encode_str_symbol_array(buf, mechanisms);
    finish_described_list(buf, sp, bs);
}

/// Encode SASL outcome body (no frame header).
pub fn encode_sasl_outcome(buf: &mut BytesMut, code: SaslCode) {
    let (sp, bs) = begin_described_list(buf, descriptor::SASL_OUTCOME, 1);
    put_ubyte(buf, code as u8);
    finish_described_list(buf, sp, bs);
}

/// Encode SASL mechanisms as a complete framed SASL frame (no intermediate allocation).
pub fn encode_framed_sasl_mechanisms(buf: &mut BytesMut, mechanisms: &[&str]) {
    let frame_start = buf.len();
    buf.put_u32(0); // frame size placeholder
    buf.put_u8(2); // doff
    buf.put_u8(FRAME_TYPE_SASL);
    buf.put_u16(0); // channel
    encode_sasl_mechanisms(buf, mechanisms);
    let frame_size = (buf.len() - frame_start) as u32;
    buf[frame_start..frame_start + 4].copy_from_slice(&frame_size.to_be_bytes());
}

/// Encode SASL outcome as a complete framed SASL frame (no intermediate allocation).
pub fn encode_framed_sasl_outcome(buf: &mut BytesMut, code: SaslCode) {
    let frame_start = buf.len();
    buf.put_u32(0);
    buf.put_u8(2);
    buf.put_u8(FRAME_TYPE_SASL);
    buf.put_u16(0);
    encode_sasl_outcome(buf, code);
    let frame_size = (buf.len() - frame_start) as u32;
    buf[frame_start..frame_start + 4].copy_from_slice(&frame_size.to_be_bytes());
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
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert_eq!(decoded, AmqpValue::Null);
    }

    #[test]
    fn test_boolean_roundtrip() {
        for v in [true, false] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Boolean(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Boolean(v));
        }
    }

    #[test]
    fn test_uint_roundtrip() {
        for v in [0u32, 1, 255, 256, 65535, u32::MAX] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Uint(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Uint(v));
        }
    }

    #[test]
    fn test_ulong_roundtrip() {
        for v in [0u64, 1, 255, 256, u64::MAX] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Ulong(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Ulong(v));
        }
    }

    #[test]
    fn test_int_roundtrip() {
        for v in [0i32, 1, -1, 127, -128, 1000, -1000, i32::MAX, i32::MIN] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Int(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Int(v));
        }
    }

    #[test]
    fn test_string_roundtrip() {
        for s in ["", "hello", &"a".repeat(300)] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::String(WireString::from(s)));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::String(WireString::from(s)));
        }
    }

    #[test]
    fn test_symbol_roundtrip() {
        let mut buf = BytesMut::new();
        encode_value(
            &mut buf,
            &AmqpValue::Symbol(WireString::from("amqp:accepted:list")),
        );
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert_eq!(
            decoded,
            AmqpValue::Symbol(WireString::from("amqp:accepted:list"))
        );
    }

    #[test]
    fn test_binary_roundtrip() {
        let data = Bytes::from_static(b"hello world");
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Binary(data.clone()));
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert_eq!(decoded, AmqpValue::Binary(data));
    }

    #[test]
    fn test_list_roundtrip() {
        let list = AmqpValue::List(vec![
            AmqpValue::Uint(1),
            AmqpValue::String(WireString::from("test")),
            AmqpValue::Null,
        ]);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &list);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert_eq!(decoded, list);
    }

    #[test]
    fn test_empty_list_roundtrip() {
        let list = AmqpValue::List(Vec::new());
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &list);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert_eq!(decoded, list);
    }

    #[test]
    fn test_map_roundtrip() {
        let map = AmqpValue::Map(vec![(
            AmqpValue::String(WireString::from("key")),
            AmqpValue::Uint(42),
        )]);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &map);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
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
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
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
            container_id: WireString::from("test-container"),
            hostname: Some(WireString::from("localhost")),
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

        let frame_bytes = frame_buf.freeze();
        let (frame, consumed) = decode_frame(&frame_bytes).unwrap();
        assert_eq!(consumed, frame_bytes.len());
        assert_eq!(frame.channel, 0);
        match frame.body {
            FrameBody::Amqp(Performative::Open(decoded)) => {
                assert_eq!(&*decoded.container_id, "test-container");
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

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
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

        let bytes = buf.freeze();
        let (frame, consumed) = decode_frame(&bytes).unwrap();
        assert_eq!(consumed, 8);
        assert!(matches!(frame.body, FrameBody::Empty));
    }

    #[test]
    fn test_incomplete_frame() {
        let data = Bytes::from_static(&[0u8, 0, 0, 20, 2, 0, 0, 0]); // says 20 bytes but only 8
        assert!(matches!(decode_frame(&data), Err(CodecError::Incomplete)));
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let ts = AmqpValue::Timestamp(1709654400000);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &ts);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert_eq!(decoded, ts);
    }

    #[test]
    fn test_float_double_roundtrip() {
        let f = AmqpValue::Float(3.14);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &f);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Float(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!("expected float"),
        }

        let d = AmqpValue::Double(2.718281828);
        buf = BytesMut::new();
        encode_value(&mut buf, &d);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
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

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Flow(decoded)) => {
                assert_eq!(decoded.next_incoming_id, Some(5));
                assert_eq!(decoded.handle, Some(0));
                assert_eq!(decoded.link_credit, Some(100));
            }
            _ => panic!("expected Flow"),
        }
    }

    #[test]
    fn test_framed_performative_encode() {
        let open = Performative::Open(Open {
            container_id: WireString::from("test"),
            ..Default::default()
        });

        let mut buf = BytesMut::new();
        encode_framed_performative(&mut buf, 0, FRAME_TYPE_AMQP, &open);

        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Open(decoded)) => {
                assert_eq!(&*decoded.container_id, "test");
            }
            _ => panic!("expected Open"),
        }
    }

    #[test]
    fn test_disposition_with_state_roundtrip() {
        let disp = Disposition {
            role: Role::Receiver,
            first: 5,
            last: Some(10),
            settled: true,
            state: Some(DeliveryState::Accepted),
            batchable: false,
        };

        let mut buf = BytesMut::new();
        encode_disposition(&mut buf, &disp);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &buf);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Disposition(decoded)) => {
                assert_eq!(decoded.first, 5);
                assert_eq!(decoded.last, Some(10));
                assert!(decoded.settled);
                assert!(matches!(decoded.state, Some(DeliveryState::Accepted)));
            }
            _ => panic!("expected Disposition"),
        }
    }

    #[test]
    fn test_zero_copy_string_decode() {
        // Verify that decoded strings share memory with the input buffer
        let mut buf = BytesMut::new();
        encode_value(
            &mut buf,
            &AmqpValue::String(WireString::from("zero-copy-test")),
        );
        let bytes = buf.freeze();
        let original_ptr = bytes.as_ptr();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::String(ws) => {
                assert_eq!(&*ws, "zero-copy-test");
                // The WireString's data should point into the original buffer
                // (offset by the format code + length byte)
                let ws_ptr = ws.as_bytes().as_ptr();
                assert!(ws_ptr >= original_ptr);
            }
            _ => panic!("expected String"),
        }
    }
}
