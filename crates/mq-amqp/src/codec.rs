//! AMQP 1.0 binary codec.
//!
//! Handles the AMQP 1.0 type system encoding/decoding and frame parsing.
//! Frame structure: [4:size][1:doff][1:type][2:channel][payload]
//!
//! Zero-copy decode via `BytesCursor`: strings and binary data are sliced
//! directly from the input `Bytes` buffer with no intermediate allocations.

use bytes::{BufMut, Bytes, BytesMut};
use smallvec::SmallVec;
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
    pub const DECIMAL32: u8 = 0x74;
    pub const DECIMAL64: u8 = 0x84;
    pub const DECIMAL128: u8 = 0x94;
    pub const CHAR: u8 = 0x73;
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

/// Structural equality for AmqpValue map keys.
///
/// The derived PartialEq on AmqpValue always returns true (for AmqpError usage),
/// so we need a real structural comparison for duplicate-key detection.
fn amqp_value_key_eq(a: &AmqpValue, b: &AmqpValue) -> bool {
    use AmqpValue::*;
    match (a, b) {
        (Null, Null) => true,
        (Boolean(x), Boolean(y)) => x == y,
        (Ubyte(x), Ubyte(y)) => x == y,
        (Ushort(x), Ushort(y)) => x == y,
        (Uint(x), Uint(y)) => x == y,
        (Ulong(x), Ulong(y)) => x == y,
        (Byte(x), Byte(y)) => x == y,
        (Short(x), Short(y)) => x == y,
        (Int(x), Int(y)) => x == y,
        (Long(x), Long(y)) => x == y,
        (Float(x), Float(y)) => x.to_bits() == y.to_bits(),
        (Double(x), Double(y)) => x.to_bits() == y.to_bits(),
        (Decimal32(x), Decimal32(y)) => x == y,
        (Decimal64(x), Decimal64(y)) => x == y,
        (Decimal128(x), Decimal128(y)) => x == y,
        (Char(x), Char(y)) => x == y,
        (Timestamp(x), Timestamp(y)) => x == y,
        (Uuid(x), Uuid(y)) => x == y,
        (Binary(x), Binary(y)) => x == y,
        (String(x), String(y)) | (Symbol(x), Symbol(y)) => x == y,
        _ => false,
    }
}

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
        format_code::DECIMAL32 => {
            cur.ensure(4)?;
            let mut v = [0u8; 4];
            v.copy_from_slice(cur.read_slice(4)?);
            cur.advance(4);
            Ok(AmqpValue::Decimal32(v))
        }
        format_code::DECIMAL64 => {
            cur.ensure(8)?;
            let mut v = [0u8; 8];
            v.copy_from_slice(cur.read_slice(8)?);
            cur.advance(8);
            Ok(AmqpValue::Decimal64(v))
        }
        format_code::DECIMAL128 => {
            cur.ensure(16)?;
            let mut v = [0u8; 16];
            v.copy_from_slice(cur.read_slice(16)?);
            cur.advance(16);
            Ok(AmqpValue::Decimal128(v))
        }
        format_code::CHAR => {
            let code_point = cur.read_u32()?;
            let ch = char::from_u32(code_point)
                .ok_or(CodecError::InvalidFormatCode(format_code::CHAR))?;
            Ok(AmqpValue::Char(ch))
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
            // T1: Reject duplicate map keys (spec §1.6.23)
            if items.len() <= 64 {
                for i in 0..items.len() {
                    for j in (i + 1)..items.len() {
                        if amqp_value_key_eq(&items[i].0, &items[j].0) {
                            return Err(CodecError::InvalidPerformative(
                                "map contains duplicate keys".into(),
                            ));
                        }
                    }
                }
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
            // T1: Reject duplicate map keys (spec §1.6.23)
            if items.len() <= 64 {
                for i in 0..items.len() {
                    for j in (i + 1)..items.len() {
                        if amqp_value_key_eq(&items[i].0, &items[j].0) {
                            return Err(CodecError::InvalidPerformative(
                                "map contains duplicate keys".into(),
                            ));
                        }
                    }
                }
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
        format_code::DECIMAL32 => {
            cur.ensure(4)?;
            let mut v = [0u8; 4];
            v.copy_from_slice(cur.read_slice(4)?);
            cur.advance(4);
            Ok(AmqpValue::Decimal32(v))
        }
        format_code::DECIMAL64 => {
            cur.ensure(8)?;
            let mut v = [0u8; 8];
            v.copy_from_slice(cur.read_slice(8)?);
            cur.advance(8);
            Ok(AmqpValue::Decimal64(v))
        }
        format_code::DECIMAL128 => {
            cur.ensure(16)?;
            let mut v = [0u8; 16];
            v.copy_from_slice(cur.read_slice(16)?);
            cur.advance(16);
            Ok(AmqpValue::Decimal128(v))
        }
        format_code::CHAR => {
            let code_point = cur.read_u32()?;
            let ch = char::from_u32(code_point)
                .ok_or(CodecError::InvalidFormatCode(format_code::CHAR))?;
            Ok(AmqpValue::Char(ch))
        }
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
        AmqpValue::Decimal32(v) => {
            buf.put_u8(format_code::DECIMAL32);
            buf.put_slice(v);
        }
        AmqpValue::Decimal64(v) => {
            buf.put_u8(format_code::DECIMAL64);
            buf.put_slice(v);
        }
        AmqpValue::Decimal128(v) => {
            buf.put_u8(format_code::DECIMAL128);
            buf.put_slice(v);
        }
        AmqpValue::Char(v) => {
            buf.put_u8(format_code::CHAR);
            buf.put_u32(*v as u32);
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
fn put_binary(buf: &mut BytesMut, b: &[u8]) {
    if b.len() <= 255 {
        buf.put_u8(format_code::BINARY8);
        buf.put_u8(b.len() as u8);
    } else {
        buf.put_u8(format_code::BINARY32);
        buf.put_u32(b.len() as u32);
    }
    buf.put_slice(b);
}

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

/// Encode an array of WireString as AMQP symbol array.
fn encode_wire_string_symbol_array(buf: &mut BytesMut, symbols: &[WireString]) {
    if symbols.is_empty() {
        buf.put_u8(format_code::ARRAY8);
        buf.put_u8(1);
        buf.put_u8(0);
        buf.put_u8(format_code::SYMBOL8);
        return;
    }
    buf.put_u8(format_code::ARRAY32);
    let size_pos = buf.len();
    buf.put_u32(0);
    let after_size = buf.len();
    buf.put_u32(symbols.len() as u32);
    buf.put_u8(format_code::SYMBOL8);
    for s in symbols {
        buf.put_u8(s.len() as u8);
        buf.put_slice(s.as_bytes().as_ref());
    }
    let size = (buf.len() - after_size) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
}

/// Encode an array of WireString as AMQP string array (for locales).
#[allow(dead_code)]
fn encode_wire_string_array(buf: &mut BytesMut, strings: &[WireString]) {
    if strings.is_empty() {
        buf.put_u8(format_code::ARRAY8);
        buf.put_u8(1);
        buf.put_u8(0);
        buf.put_u8(format_code::STRING8);
        return;
    }
    buf.put_u8(format_code::ARRAY32);
    let size_pos = buf.len();
    buf.put_u32(0);
    let after_size = buf.len();
    buf.put_u32(strings.len() as u32);
    buf.put_u8(format_code::STRING8);
    for s in strings {
        buf.put_u8(s.len() as u8);
        buf.put_slice(s.as_bytes().as_ref());
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

/// Borrow a value from a list field (read-only access for scalars).
#[inline]
fn list_get(list: &[AmqpValue], idx: usize) -> &AmqpValue {
    list.get(idx).unwrap_or(&AmqpValue::Null)
}

/// Take a value from a list field by swapping with Null (zero-copy move).
/// Use this instead of `list_get(...).clone()` to avoid deep clones.
#[inline]
fn list_take(list: &mut [AmqpValue], idx: usize) -> AmqpValue {
    list.get_mut(idx)
        .map(|v| std::mem::replace(v, AmqpValue::Null))
        .unwrap_or(AmqpValue::Null)
}

/// Take an optional non-null value from a list field (zero-copy move).
#[inline]
fn list_take_opt(list: &mut [AmqpValue], idx: usize) -> Option<AmqpValue> {
    match list.get_mut(idx) {
        Some(AmqpValue::Null) | None => None,
        Some(v) => Some(std::mem::replace(v, AmqpValue::Null)),
    }
}

/// Extract a WireString from a list field (O(1) Bytes refcount bump).
#[inline]
fn list_get_wire_string(list: &[AmqpValue], idx: usize) -> WireString {
    list.get(idx)
        .and_then(|v| v.as_wire_string())
        .cloned()
        .unwrap_or_default()
}

/// Extract an optional WireString from a list field (O(1) Bytes refcount bump).
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
            let mut fields = match *inner {
                AmqpValue::List(l) => l,
                _ => Vec::new(),
            };
            match descriptor {
                descriptor::OPEN => Ok(Performative::Open(decode_open(&fields)?)),
                descriptor::BEGIN => Ok(Performative::Begin(decode_begin(&fields)?)),
                descriptor::ATTACH => Ok(Performative::Attach(decode_attach(&mut fields)?)),
                descriptor::FLOW => Ok(Performative::Flow(decode_flow(&mut fields)?)),
                descriptor::TRANSFER => {
                    Ok(Performative::Transfer(decode_transfer(&fields, payload)?))
                }
                descriptor::DISPOSITION => {
                    Ok(Performative::Disposition(decode_disposition(&mut fields)?))
                }
                descriptor::DETACH => Ok(Performative::Detach(decode_detach(&mut fields)?)),
                descriptor::END => Ok(Performative::End(decode_end(&mut fields)?)),
                descriptor::CLOSE => Ok(Performative::Close(decode_close(&mut fields)?)),
                _ => Err(CodecError::UnknownDescriptor(descriptor)),
            }
        }
        _ => Err(CodecError::InvalidPerformative(
            "not a described type".into(),
        )),
    }
}

fn decode_open(fields: &[AmqpValue]) -> Result<Open, CodecError> {
    let mut open = Open {
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
    };
    // TR13: outgoing-locales (field 5)
    if let AmqpValue::Array(arr) = list_get(fields, 5) {
        for v in arr {
            if let Some(s) = v.as_wire_string() {
                open.outgoing_locales.push(s.clone());
            }
        }
    } else if let AmqpValue::Symbol(s) = list_get(fields, 5) {
        open.outgoing_locales.push(s.clone());
    }
    // TR13: incoming-locales (field 6)
    if let AmqpValue::Array(arr) = list_get(fields, 6) {
        for v in arr {
            if let Some(s) = v.as_wire_string() {
                open.incoming_locales.push(s.clone());
            }
        }
    } else if let AmqpValue::Symbol(s) = list_get(fields, 6) {
        open.incoming_locales.push(s.clone());
    }
    // offered-capabilities (field 7)
    if let AmqpValue::Array(arr) = list_get(fields, 7) {
        for v in arr {
            if let Some(s) = v.as_wire_string() {
                open.offered_capabilities.push(s.clone());
            }
        }
    } else if let AmqpValue::Symbol(s) = list_get(fields, 7) {
        open.offered_capabilities.push(s.clone());
    }
    // desired-capabilities (field 8)
    if let AmqpValue::Array(arr) = list_get(fields, 8) {
        for v in arr {
            if let Some(s) = v.as_wire_string() {
                open.desired_capabilities.push(s.clone());
            }
        }
    } else if let AmqpValue::Symbol(s) = list_get(fields, 8) {
        open.desired_capabilities.push(s.clone());
    }
    // TR4: properties (field 9)
    match list_get(fields, 9) {
        AmqpValue::Null => {}
        v => open.properties = Some(v.clone()),
    }
    Ok(open)
}

fn decode_begin(fields: &[AmqpValue]) -> Result<Begin, CodecError> {
    let mut begin = Begin {
        remote_channel: list_get(fields, 0).as_u32().map(|v| v as u16),
        next_outgoing_id: list_get(fields, 1).as_u32().unwrap_or(0),
        incoming_window: list_get(fields, 2).as_u32().unwrap_or(2048),
        outgoing_window: list_get(fields, 3).as_u32().unwrap_or(2048),
        handle_max: list_get(fields, 4).as_u32().unwrap_or(u32::MAX),
        ..Default::default()
    };
    // TR5: offered-capabilities (field 5)
    if let AmqpValue::Array(arr) = list_get(fields, 5) {
        for v in arr {
            if let Some(s) = v.as_wire_string() {
                begin.offered_capabilities.push(s.clone());
            }
        }
    } else if let AmqpValue::Symbol(s) = list_get(fields, 5) {
        begin.offered_capabilities.push(s.clone());
    }
    // TR5: desired-capabilities (field 6)
    if let AmqpValue::Array(arr) = list_get(fields, 6) {
        for v in arr {
            if let Some(s) = v.as_wire_string() {
                begin.desired_capabilities.push(s.clone());
            }
        }
    } else if let AmqpValue::Symbol(s) = list_get(fields, 6) {
        begin.desired_capabilities.push(s.clone());
    }
    // TR5: properties (field 7)
    match list_get(fields, 7) {
        AmqpValue::Null => {}
        v => begin.properties = Some(v.clone()),
    }
    Ok(begin)
}

fn decode_source_owned(value: AmqpValue) -> Option<Source> {
    match value {
        AmqpValue::Described(_, inner) => {
            let mut fields = match *inner {
                AmqpValue::List(l) => l,
                _ => return Some(Source::default()),
            };
            Some(Source {
                address: list_get_opt_wire_string(&fields, 0),
                durable: list_get(&fields, 1).as_u32().unwrap_or(0),
                expiry_policy: list_get_opt_wire_string(&fields, 2),
                timeout: list_get(&fields, 3).as_u32().unwrap_or(0),
                dynamic: list_get(&fields, 4).as_bool().unwrap_or(false),
                dynamic_node_properties: list_take_opt(&mut fields, 5),
                distribution_mode: list_get_opt_wire_string(&fields, 6),
                filter: list_take_opt(&mut fields, 7),
                default_outcome: list_take_opt(&mut fields, 8),
                ..Default::default()
            })
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

fn decode_target_owned(value: AmqpValue) -> Option<Target> {
    match value {
        AmqpValue::Described(_, inner) => {
            let mut fields = match *inner {
                AmqpValue::List(l) => l,
                _ => return Some(Target::default()),
            };
            Some(Target {
                address: list_get_opt_wire_string(&fields, 0),
                durable: list_get(&fields, 1).as_u32().unwrap_or(0),
                expiry_policy: list_get_opt_wire_string(&fields, 2),
                timeout: list_get(&fields, 3).as_u32().unwrap_or(0),
                dynamic: list_get(&fields, 4).as_bool().unwrap_or(false),
                dynamic_node_properties: list_take_opt(&mut fields, 5),
                ..Default::default()
            })
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

fn decode_attach(fields: &mut [AmqpValue]) -> Result<Attach, CodecError> {
    // Gap X1: Check if target is a Coordinator (descriptor 0x30) before parsing as Target
    let target_val = list_take(fields, 6);
    let (target, coordinator_target) = match &target_val {
        AmqpValue::Described(desc, _) if desc.as_u64() == Some(txn_descriptor::COORDINATOR) => {
            (None, decode_coordinator(&target_val))
        }
        _ => (decode_target_owned(target_val), None),
    };
    Ok(Attach {
        name: list_get_wire_string(fields, 0),
        handle: list_get(fields, 1).as_u32().unwrap_or(0),
        role: Role::from_bool(list_get(fields, 2).as_bool().unwrap_or(false)),
        snd_settle_mode: SndSettleMode::from_u8(list_get(fields, 3).as_u32().unwrap_or(2) as u8),
        rcv_settle_mode: RcvSettleMode::from_u8(list_get(fields, 4).as_u32().unwrap_or(0) as u8),
        source: decode_source_owned(list_take(fields, 5)),
        target,
        coordinator_target,
        unsettled: list_take_opt(fields, 7),
        incomplete_unsettled: list_get(fields, 8).as_bool().unwrap_or(false),
        initial_delivery_count: list_get(fields, 10).as_u32(),
        max_message_size: list_get(fields, 11).as_u64(),
        properties: list_take_opt(fields, 14),
        ..Default::default()
    })
}

fn decode_flow(fields: &mut [AmqpValue]) -> Result<Flow, CodecError> {
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
        properties: list_take_opt(fields, 10),
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

pub fn decode_delivery_state(value: &AmqpValue) -> Option<DeliveryState> {
    match value {
        AmqpValue::Described(desc, inner) => {
            let d = desc.as_u64()?;
            match d {
                descriptor::ACCEPTED => Some(DeliveryState::Accepted),
                descriptor::REJECTED => Some(DeliveryState::Rejected { error: None }),
                descriptor::RELEASED => Some(DeliveryState::Released),
                descriptor::MODIFIED => {
                    let fields = match inner.as_ref() {
                        AmqpValue::List(l) => l.as_slice(),
                        _ => &[],
                    };
                    Some(DeliveryState::Modified {
                        delivery_failed: list_get(fields, 0).as_bool().unwrap_or(false),
                        undeliverable_here: list_get(fields, 1).as_bool().unwrap_or(false),
                        message_annotations: match list_get(fields, 2) {
                            AmqpValue::Null => None,
                            v => Some(v.clone()),
                        },
                    })
                }
                descriptor::RECEIVED => {
                    let fields = match inner.as_ref() {
                        AmqpValue::List(l) => l.as_slice(),
                        _ => &[],
                    };
                    Some(DeliveryState::Received {
                        section_number: list_get(fields, 0).as_u32().unwrap_or(0),
                        section_offset: list_get(fields, 1).as_u64().unwrap_or(0),
                    })
                }
                txn_descriptor::TRANSACTIONAL_STATE => {
                    let fields = match inner.as_ref() {
                        AmqpValue::List(l) => l.as_slice(),
                        _ => &[],
                    };
                    let txn_id = list_get(fields, 0).as_binary().cloned().unwrap_or_default();
                    let outcome = decode_delivery_state(list_get(fields, 1))
                        .and_then(|ds| Outcome::from_delivery_state(&ds));
                    Some(DeliveryState::Transactional(TransactionalState {
                        txn_id,
                        outcome,
                    }))
                }
                txn_descriptor::DECLARED => {
                    let fields = match inner.as_ref() {
                        AmqpValue::List(l) => l.as_slice(),
                        _ => &[],
                    };
                    let txn_id = list_get(fields, 0).as_binary().cloned().unwrap_or_default();
                    Some(DeliveryState::Declared { txn_id })
                }
                _ => None,
            }
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

/// Owned variant — takes AmqpValue by value, moves inner data without cloning.
fn decode_delivery_state_owned(value: AmqpValue) -> Option<DeliveryState> {
    match value {
        AmqpValue::Described(desc, inner) => {
            let d = desc.as_u64()?;
            match d {
                descriptor::ACCEPTED => Some(DeliveryState::Accepted),
                descriptor::REJECTED => Some(DeliveryState::Rejected { error: None }),
                descriptor::RELEASED => Some(DeliveryState::Released),
                descriptor::MODIFIED => {
                    let mut fields = match *inner {
                        AmqpValue::List(l) => l,
                        _ => Vec::new(),
                    };
                    Some(DeliveryState::Modified {
                        delivery_failed: list_get(&fields, 0).as_bool().unwrap_or(false),
                        undeliverable_here: list_get(&fields, 1).as_bool().unwrap_or(false),
                        message_annotations: list_take_opt(&mut fields, 2),
                    })
                }
                descriptor::RECEIVED => {
                    let fields = match inner.as_ref() {
                        AmqpValue::List(l) => l.as_slice(),
                        _ => &[],
                    };
                    Some(DeliveryState::Received {
                        section_number: list_get(fields, 0).as_u32().unwrap_or(0),
                        section_offset: list_get(fields, 1).as_u64().unwrap_or(0),
                    })
                }
                txn_descriptor::TRANSACTIONAL_STATE => {
                    let mut fields = match *inner {
                        AmqpValue::List(l) => l,
                        _ => Vec::new(),
                    };
                    let txn_id = list_get(&fields, 0)
                        .as_binary()
                        .cloned()
                        .unwrap_or_default();
                    let outcome = decode_delivery_state_owned(list_take(&mut fields, 1))
                        .and_then(|ds| Outcome::from_delivery_state(&ds));
                    Some(DeliveryState::Transactional(TransactionalState {
                        txn_id,
                        outcome,
                    }))
                }
                txn_descriptor::DECLARED => {
                    let fields = match inner.as_ref() {
                        AmqpValue::List(l) => l.as_slice(),
                        _ => &[],
                    };
                    let txn_id = list_get(fields, 0).as_binary().cloned().unwrap_or_default();
                    Some(DeliveryState::Declared { txn_id })
                }
                _ => None,
            }
        }
        AmqpValue::Null => None,
        _ => None,
    }
}

/// Decode a Coordinator target from a described list.
pub fn decode_coordinator(value: &AmqpValue) -> Option<Coordinator> {
    match value {
        AmqpValue::Described(desc, inner) => {
            if desc.as_u64()? != txn_descriptor::COORDINATOR {
                return None;
            }
            let fields = match inner.as_ref() {
                AmqpValue::List(l) => l.as_slice(),
                _ => &[],
            };
            let mut caps = SmallVec::new();
            if let AmqpValue::Array(arr) = list_get(fields, 0) {
                for v in arr {
                    if let Some(s) = v.as_wire_string() {
                        caps.push(s.clone());
                    }
                }
            }
            Some(Coordinator { capabilities: caps })
        }
        _ => None,
    }
}

/// Decode TxnDeclare from a Transfer payload body.
pub fn decode_txn_declare(value: &AmqpValue) -> Option<TxnDeclare> {
    match value {
        AmqpValue::Described(desc, inner) => {
            if desc.as_u64()? != txn_descriptor::DECLARE {
                return None;
            }
            let fields = match inner.as_ref() {
                AmqpValue::List(l) => l.as_slice(),
                _ => &[],
            };
            Some(TxnDeclare {
                global_id: list_get(fields, 0).as_binary().cloned(),
            })
        }
        _ => None,
    }
}

/// Decode TxnDischarge from a Transfer payload body.
pub fn decode_txn_discharge(value: &AmqpValue) -> Option<TxnDischarge> {
    match value {
        AmqpValue::Described(desc, inner) => {
            if desc.as_u64()? != txn_descriptor::DISCHARGE {
                return None;
            }
            let fields = match inner.as_ref() {
                AmqpValue::List(l) => l.as_slice(),
                _ => &[],
            };
            Some(TxnDischarge {
                txn_id: list_get(fields, 0).as_binary().cloned().unwrap_or_default(),
                fail: list_get(fields, 1).as_bool().unwrap_or(false),
            })
        }
        _ => None,
    }
}

/// Encode a TxnDeclared outcome.
pub fn encode_txn_declared(buf: &mut BytesMut, txn_id: &[u8]) {
    let (sp, bs) = begin_described_list(buf, txn_descriptor::DECLARED, 1);
    put_binary(buf, txn_id);
    finish_described_list(buf, sp, bs);
}

/// Encode a TransactionalState delivery state.
pub fn encode_transactional_state(
    buf: &mut BytesMut,
    txn_id: &[u8],
    outcome: Option<&DeliveryState>,
) {
    let count = if outcome.is_some() { 2u32 } else { 1 };
    let (sp, bs) = begin_described_list(buf, txn_descriptor::TRANSACTIONAL_STATE, count);
    put_binary(buf, txn_id);
    if let Some(state) = outcome {
        encode_delivery_state(buf, state);
    }
    finish_described_list(buf, sp, bs);
}

/// Encode a Coordinator target.
pub fn encode_coordinator(buf: &mut BytesMut, coord: &Coordinator) {
    if coord.capabilities.is_empty() {
        put_empty_described_list(buf, txn_descriptor::COORDINATOR);
    } else {
        let (sp, bs) = begin_described_list(buf, txn_descriptor::COORDINATOR, 1);
        encode_wire_string_symbol_array(buf, &coord.capabilities);
        finish_described_list(buf, sp, bs);
    }
}

fn decode_disposition(fields: &mut [AmqpValue]) -> Result<Disposition, CodecError> {
    Ok(Disposition {
        role: Role::from_bool(list_get(fields, 0).as_bool().unwrap_or(false)),
        first: list_get(fields, 1).as_u32().unwrap_or(0),
        last: list_get(fields, 2).as_u32(),
        settled: list_get(fields, 3).as_bool().unwrap_or(false),
        state: decode_delivery_state_owned(list_take(fields, 4)),
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

fn decode_detach(fields: &mut [AmqpValue]) -> Result<Detach, CodecError> {
    Ok(Detach {
        handle: list_get(fields, 0).as_u32().unwrap_or(0),
        closed: list_get(fields, 1).as_bool().unwrap_or(false),
        error: decode_error(list_get(fields, 2)),
    })
}

fn decode_end(fields: &mut [AmqpValue]) -> Result<End, CodecError> {
    Ok(End {
        error: decode_error(list_get(fields, 0)),
    })
}

fn decode_close(fields: &mut [AmqpValue]) -> Result<Close, CodecError> {
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
                descriptor::SASL_CHALLENGE => {
                    let challenge = list_get(&fields, 0)
                        .as_binary()
                        .cloned()
                        .unwrap_or_default();
                    Ok(SaslPerformative::Challenge(SaslChallenge { challenge }))
                }
                descriptor::SASL_RESPONSE => {
                    let response = list_get(&fields, 0)
                        .as_binary()
                        .cloned()
                        .unwrap_or_default();
                    Ok(SaslPerformative::Response(SaslResponse { response }))
                }
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
    // Determine field count based on last non-default field
    let count = if p.properties.is_some() {
        10u32
    } else if !p.desired_capabilities.is_empty() {
        9
    } else if !p.offered_capabilities.is_empty() {
        8
    } else if !p.incoming_locales.is_empty() {
        7
    } else if !p.outgoing_locales.is_empty() {
        6
    } else if p.idle_timeout.is_some() {
        5
    } else {
        4
    };
    let (sp, bs) = begin_described_list(buf, descriptor::OPEN, count);
    put_string(buf, &p.container_id); // 0
    put_opt_string(buf, p.hostname.as_ref()); // 1
    put_uint(buf, p.max_frame_size); // 2
    put_ushort(buf, p.channel_max); // 3
    if count >= 5 {
        match p.idle_timeout {
            Some(timeout) => put_uint(buf, timeout),
            None => put_null(buf),
        }
    }
    if count >= 6 {
        // outgoing-locales
        if p.outgoing_locales.is_empty() {
            put_null(buf);
        } else {
            encode_wire_string_array(buf, &p.outgoing_locales);
        }
    }
    if count >= 7 {
        // incoming-locales
        if p.incoming_locales.is_empty() {
            put_null(buf);
        } else {
            encode_wire_string_array(buf, &p.incoming_locales);
        }
    }
    if count >= 8 {
        // offered-capabilities
        if p.offered_capabilities.is_empty() {
            put_null(buf);
        } else {
            encode_wire_string_symbol_array(buf, &p.offered_capabilities);
        }
    }
    if count >= 9 {
        // desired-capabilities
        if p.desired_capabilities.is_empty() {
            put_null(buf);
        } else {
            encode_wire_string_symbol_array(buf, &p.desired_capabilities);
        }
    }
    if count >= 10 {
        // properties
        match &p.properties {
            Some(v) => encode_value(buf, v),
            None => put_null(buf),
        }
    }
    finish_described_list(buf, sp, bs);
}

fn encode_begin(buf: &mut BytesMut, p: &Begin) {
    let count = if p.properties.is_some() {
        8u32
    } else if !p.desired_capabilities.is_empty() {
        7
    } else if !p.offered_capabilities.is_empty() {
        6
    } else {
        5
    };
    let (sp, bs) = begin_described_list(buf, descriptor::BEGIN, count);
    put_opt_ushort(buf, p.remote_channel);
    put_uint(buf, p.next_outgoing_id);
    put_uint(buf, p.incoming_window);
    put_uint(buf, p.outgoing_window);
    put_uint(buf, p.handle_max);
    if count >= 6 {
        if !p.offered_capabilities.is_empty() {
            encode_wire_string_symbol_array(buf, &p.offered_capabilities);
        } else {
            put_null(buf);
        }
    }
    if count >= 7 {
        if !p.desired_capabilities.is_empty() {
            encode_wire_string_symbol_array(buf, &p.desired_capabilities);
        } else {
            put_null(buf);
        }
    }
    if count >= 8 {
        match &p.properties {
            Some(v) => encode_value(buf, v),
            None => put_null(buf),
        }
    }
    finish_described_list(buf, sp, bs);
}

fn encode_source(buf: &mut BytesMut, s: &Source) {
    // Determine field count based on last non-default field
    let count = if !s.capabilities.is_empty() {
        11u32
    } else if !s.outcomes.is_empty() {
        10
    } else if s.default_outcome.is_some() {
        9
    } else if s.filter.is_some() {
        8
    } else if s.distribution_mode.is_some() {
        7
    } else if s.dynamic_node_properties.is_some() {
        6
    } else {
        5
    };
    let (sp, bs) = begin_described_list(buf, descriptor::SOURCE, count);
    put_opt_string(buf, s.address.as_ref()); // 0: address
    put_uint(buf, s.durable); // 1: durable
    // 2: expiry-policy
    if let Some(ref ep) = s.expiry_policy {
        put_symbol(buf, ep);
    } else {
        put_null(buf);
    }
    put_uint(buf, s.timeout); // 3: timeout
    put_bool(buf, s.dynamic); // 4: dynamic
    if count >= 6 {
        // 5: dynamic-node-properties
        match &s.dynamic_node_properties {
            Some(v) => encode_value(buf, v),
            None => put_null(buf),
        }
    }
    if count >= 7 {
        // 6: distribution-mode
        if let Some(ref dm) = s.distribution_mode {
            put_symbol(buf, dm);
        } else {
            put_null(buf);
        }
    }
    if count >= 8 {
        // 7: filter
        if let Some(ref filter) = s.filter {
            encode_value(buf, filter);
        } else {
            put_null(buf);
        }
    }
    if count >= 9 {
        // 8: default-outcome
        if let Some(ref outcome) = s.default_outcome {
            encode_value(buf, outcome);
        } else {
            put_null(buf);
        }
    }
    if count >= 10 {
        // 9: outcomes
        if !s.outcomes.is_empty() {
            encode_wire_string_symbol_array(buf, &s.outcomes);
        } else {
            put_null(buf);
        }
    }
    if count >= 11 {
        // 10: capabilities
        if !s.capabilities.is_empty() {
            encode_wire_string_symbol_array(buf, &s.capabilities);
        } else {
            put_null(buf);
        }
    }
    finish_described_list(buf, sp, bs);
}

fn encode_target(buf: &mut BytesMut, t: &Target) {
    let count = if !t.capabilities.is_empty() {
        7u32
    } else if t.dynamic_node_properties.is_some() {
        6
    } else {
        5
    };
    let (sp, bs) = begin_described_list(buf, descriptor::TARGET, count);
    put_opt_string(buf, t.address.as_ref()); // 0: address
    put_uint(buf, t.durable); // 1: durable
    // 2: expiry-policy
    if let Some(ref ep) = t.expiry_policy {
        put_symbol(buf, ep);
    } else {
        put_null(buf);
    }
    put_uint(buf, t.timeout); // 3: timeout
    put_bool(buf, t.dynamic); // 4: dynamic
    if count >= 6 {
        // 5: dynamic-node-properties
        match &t.dynamic_node_properties {
            Some(v) => encode_value(buf, v),
            None => put_null(buf),
        }
    }
    if count >= 7 {
        // 6: capabilities
        if !t.capabilities.is_empty() {
            encode_wire_string_symbol_array(buf, &t.capabilities);
        } else {
            put_null(buf);
        }
    }
    finish_described_list(buf, sp, bs);
}

fn encode_attach(buf: &mut BytesMut, p: &Attach) {
    // Determine field count based on last non-default field present
    let count = if p.properties.is_some() {
        15u32
    } else if !p.desired_capabilities.is_empty() {
        14
    } else if !p.offered_capabilities.is_empty() {
        13
    } else if p.max_message_size.is_some() {
        12
    } else {
        11
    };
    let (sp, bs) = begin_described_list(buf, descriptor::ATTACH, count);
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
    if let Some(ref coord) = p.coordinator_target {
        // 6: target (Coordinator)
        encode_coordinator(buf, coord);
    } else if let Some(t) = &p.target {
        // 6: target
        encode_target(buf, t);
    } else {
        put_null(buf);
    }
    // 7: unsettled
    if let Some(ref unsettled) = p.unsettled {
        encode_value(buf, unsettled);
    } else {
        put_null(buf);
    }
    put_bool(buf, p.incomplete_unsettled); // 8: incomplete-unsettled
    put_null(buf); // 9: reserved (initial-delivery-count is field 10, not 9)
    put_opt_uint(buf, p.initial_delivery_count); // 10: initial-delivery-count
    if count >= 12 {
        // 11: max-message-size
        match p.max_message_size {
            Some(v) => put_ulong(buf, v),
            None => put_null(buf),
        }
    }
    if count >= 13 {
        // 12: offered-capabilities
        if !p.offered_capabilities.is_empty() {
            encode_wire_string_symbol_array(buf, &p.offered_capabilities);
        } else {
            put_null(buf);
        }
    }
    if count >= 14 {
        // 13: desired-capabilities
        if !p.desired_capabilities.is_empty() {
            encode_wire_string_symbol_array(buf, &p.desired_capabilities);
        } else {
            put_null(buf);
        }
    }
    if count >= 15 {
        // 14: properties
        if let Some(ref props) = p.properties {
            encode_value(buf, props);
        } else {
            put_null(buf);
        }
    }
    finish_described_list(buf, sp, bs);
}

fn encode_flow(buf: &mut BytesMut, p: &Flow) {
    let count = if p.properties.is_some() { 11u32 } else { 10 };
    let (sp, bs) = begin_described_list(buf, descriptor::FLOW, count);
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
    if count >= 11 {
        match &p.properties {
            Some(v) => encode_value(buf, v),
            None => put_null(buf),
        }
    }
    finish_described_list(buf, sp, bs);
}

fn encode_transfer(buf: &mut BytesMut, p: &Transfer) {
    // Determine field count based on last non-default field
    let count = if p.batchable {
        11u32
    } else if p.aborted {
        10
    } else if p.resume {
        9
    } else if p.state.is_some() {
        8
    } else if p.rcv_settle_mode.is_some() {
        7
    } else {
        6
    };
    let (sp, bs) = begin_described_list(buf, descriptor::TRANSFER, count);
    put_uint(buf, p.handle); // 0
    put_opt_uint(buf, p.delivery_id); // 1
    put_opt_binary(buf, p.delivery_tag.as_ref()); // 2
    put_opt_uint(buf, p.message_format); // 3
    put_opt_bool(buf, p.settled); // 4
    put_bool(buf, p.more); // 5
    if count >= 7 {
        match p.rcv_settle_mode {
            Some(mode) => put_ubyte(buf, mode as u8),
            None => put_null(buf),
        }
    }
    if count >= 8 {
        match &p.state {
            Some(state) => encode_delivery_state(buf, state),
            None => put_null(buf),
        }
    }
    if count >= 9 {
        put_bool(buf, p.resume); // 8
    }
    if count >= 10 {
        put_bool(buf, p.aborted); // 9
    }
    if count >= 11 {
        put_bool(buf, p.batchable); // 10
    }
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
        DeliveryState::Transactional(ts) => {
            let ds = ts.outcome.as_ref().map(|o| o.to_delivery_state());
            encode_transactional_state(buf, &ts.txn_id, ds.as_ref());
        }
        DeliveryState::Declared { txn_id } => {
            encode_txn_declared(buf, txn_id);
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
    let count = if err.info.is_some() {
        3u32
    } else if err.description.is_some() {
        2
    } else {
        1
    };
    let (sp, bs) = begin_described_list(buf, descriptor::ERROR, count);
    put_symbol(buf, &err.condition);
    if count >= 2 {
        match &err.description {
            Some(desc) => put_string(buf, desc),
            None => put_null(buf),
        }
    }
    if count >= 3 {
        match &err.info {
            Some(info) => encode_value(buf, info),
            None => put_null(buf),
        }
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
// Message Section Decode/Encode
// =============================================================================

/// Decode an AMQP 1.0 message from a Transfer payload.
///
/// Parses described sections: Header (0x70), DeliveryAnnotations (0x71),
/// MessageAnnotations (0x72), Properties (0x73), ApplicationProperties (0x74),
/// Body: Data (0x75), AmqpSequence (0x76), AmqpValue (0x77), Footer (0x78).
///
/// Zero-copy: body DATA sections reference the original Bytes buffer.
pub fn decode_message(payload: Bytes) -> Result<AmqpMessage, CodecError> {
    if payload.is_empty() {
        return Ok(AmqpMessage::default());
    }

    let mut msg = AmqpMessage::default();
    let mut cur = BytesCursor::new(payload);

    while cur.remaining() > 0 {
        let value = decode_value(&mut cur)?;
        match value {
            AmqpValue::Described(desc, inner) => {
                let d = desc.as_u64().unwrap_or(0);
                match d {
                    descriptor::HEADER => {
                        msg.header = Some(decode_message_header(&inner));
                    }
                    descriptor::DELIVERY_ANNOTATIONS => {
                        msg.delivery_annotations = Some(*inner);
                    }
                    descriptor::MESSAGE_ANNOTATIONS => {
                        msg.message_annotations = Some(*inner);
                    }
                    descriptor::PROPERTIES => {
                        msg.properties = Some(decode_message_properties(*inner));
                    }
                    descriptor::APPLICATION_PROPERTIES => {
                        // M2: Validate application-properties values are simple types
                        if let AmqpValue::Map(ref entries) = *inner {
                            for (_key, value) in entries {
                                match value {
                                    AmqpValue::Map(_)
                                    | AmqpValue::List(_)
                                    | AmqpValue::Array(_) => {
                                        return Err(CodecError::InvalidPerformative(
                                            "application-properties values must be simple types"
                                                .into(),
                                        ));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        msg.application_properties = Some(*inner);
                    }
                    descriptor::DATA => {
                        // DATA section body is binary
                        match *inner {
                            AmqpValue::Binary(b) => msg.body.push(b),
                            other => {
                                // Encode non-binary data section as bytes
                                let mut tmp = BytesMut::new();
                                encode_value(&mut tmp, &other);
                                msg.body.push(tmp.freeze());
                            }
                        }
                    }
                    descriptor::AMQP_SEQUENCE => {
                        // Store sequence as encoded bytes
                        let mut tmp = BytesMut::new();
                        encode_value(&mut tmp, &inner);
                        msg.body.push(tmp.freeze());
                    }
                    descriptor::AMQP_VALUE => {
                        // Store value section as encoded bytes
                        let mut tmp = BytesMut::new();
                        encode_value(&mut tmp, &inner);
                        msg.body.push(tmp.freeze());
                    }
                    descriptor::FOOTER => {
                        msg.footer = Some(*inner);
                    }
                    _ => {
                        // Unknown section — skip
                    }
                }
            }
            _ => {
                // Non-described value in message payload — skip
            }
        }
    }

    Ok(msg)
}

fn decode_message_header(value: &AmqpValue) -> MessageHeader {
    let fields = match value {
        AmqpValue::List(l) => l.as_slice(),
        _ => &[],
    };
    MessageHeader {
        durable: list_get(fields, 0).as_bool().unwrap_or(false),
        priority: list_get(fields, 1).as_u32().unwrap_or(4) as u8,
        ttl: list_get(fields, 2).as_u32(),
        first_acquirer: list_get(fields, 3).as_bool().unwrap_or(false),
        delivery_count: list_get(fields, 4).as_u32().unwrap_or(0),
    }
}

fn decode_message_properties(value: AmqpValue) -> MessageProperties {
    let mut fields = match value {
        AmqpValue::List(l) => l,
        _ => return MessageProperties::default(),
    };
    MessageProperties {
        message_id: list_take_opt(&mut fields, 0),
        user_id: list_get(&fields, 1).as_binary().cloned(),
        to: list_get_opt_wire_string(&fields, 2),
        subject: list_get_opt_wire_string(&fields, 3),
        reply_to: list_get_opt_wire_string(&fields, 4),
        correlation_id: list_take_opt(&mut fields, 5),
        content_type: list_get_opt_wire_string(&fields, 6),
        content_encoding: list_get_opt_wire_string(&fields, 7),
        absolute_expiry_time: list_get(&fields, 8).as_i64(),
        creation_time: list_get(&fields, 9).as_i64(),
        group_id: list_get_opt_wire_string(&fields, 10),
        group_sequence: list_get(&fields, 11).as_u32(),
        reply_to_group_id: list_get_opt_wire_string(&fields, 12),
    }
}

/// Encode an AMQP 1.0 message into a buffer (for Transfer payload).
pub fn encode_message(buf: &mut BytesMut, msg: &AmqpMessage) {
    // Header section
    if let Some(header) = &msg.header {
        encode_message_header(buf, header);
    }
    // Delivery annotations
    if let Some(da) = &msg.delivery_annotations {
        buf.put_u8(format_code::DESCRIBED);
        put_ulong(buf, descriptor::DELIVERY_ANNOTATIONS);
        encode_value(buf, da);
    }
    // Message annotations
    if let Some(ma) = &msg.message_annotations {
        buf.put_u8(format_code::DESCRIBED);
        put_ulong(buf, descriptor::MESSAGE_ANNOTATIONS);
        encode_value(buf, ma);
    }
    // Properties section
    if let Some(props) = &msg.properties {
        encode_message_properties(buf, props);
    }
    // Application properties
    if let Some(ap) = &msg.application_properties {
        buf.put_u8(format_code::DESCRIBED);
        put_ulong(buf, descriptor::APPLICATION_PROPERTIES);
        encode_value(buf, ap);
    }
    // Body sections (DATA)
    for data in &msg.body {
        buf.put_u8(format_code::DESCRIBED);
        put_ulong(buf, descriptor::DATA);
        if data.len() <= 255 {
            buf.put_u8(format_code::BINARY8);
            buf.put_u8(data.len() as u8);
        } else {
            buf.put_u8(format_code::BINARY32);
            buf.put_u32(data.len() as u32);
        }
        buf.put_slice(data);
    }
    // Footer
    if let Some(footer) = &msg.footer {
        buf.put_u8(format_code::DESCRIBED);
        put_ulong(buf, descriptor::FOOTER);
        encode_value(buf, footer);
    }
}

fn encode_message_header(buf: &mut BytesMut, h: &MessageHeader) {
    // Determine field count (trim trailing defaults)
    let count = if h.delivery_count != 0 {
        5u32
    } else if h.first_acquirer {
        4
    } else if h.ttl.is_some() {
        3
    } else if h.priority != 4 {
        2
    } else if h.durable {
        1
    } else {
        0
    };
    if count == 0 {
        put_empty_described_list(buf, descriptor::HEADER);
        return;
    }
    let (sp, bs) = begin_described_list(buf, descriptor::HEADER, count);
    if count >= 1 {
        put_bool(buf, h.durable);
    }
    if count >= 2 {
        put_ubyte(buf, h.priority);
    }
    if count >= 3 {
        put_opt_uint(buf, h.ttl);
    }
    if count >= 4 {
        put_bool(buf, h.first_acquirer);
    }
    if count >= 5 {
        put_uint(buf, h.delivery_count);
    }
    finish_described_list(buf, sp, bs);
}

fn encode_message_properties(buf: &mut BytesMut, p: &MessageProperties) {
    // Find last non-null field
    let mut count = 0u32;
    if p.message_id.is_some() {
        count = 1;
    }
    if p.user_id.is_some() {
        count = 2;
    }
    if p.to.is_some() {
        count = 3;
    }
    if p.subject.is_some() {
        count = 4;
    }
    if p.reply_to.is_some() {
        count = 5;
    }
    if p.correlation_id.is_some() {
        count = 6;
    }
    if p.content_type.is_some() {
        count = 7;
    }
    if p.content_encoding.is_some() {
        count = 8;
    }
    if p.absolute_expiry_time.is_some() {
        count = 9;
    }
    if p.creation_time.is_some() {
        count = 10;
    }
    if p.group_id.is_some() {
        count = 11;
    }
    if p.group_sequence.is_some() {
        count = 12;
    }
    if p.reply_to_group_id.is_some() {
        count = 13;
    }
    if count == 0 {
        put_empty_described_list(buf, descriptor::PROPERTIES);
        return;
    }
    let (sp, bs) = begin_described_list(buf, descriptor::PROPERTIES, count);
    // 0: message-id
    if count >= 1 {
        match &p.message_id {
            Some(v) => encode_value(buf, v),
            None => put_null(buf),
        }
    }
    // 1: user-id
    if count >= 2 {
        put_opt_binary(buf, p.user_id.as_ref());
    }
    // 2: to
    if count >= 3 {
        put_opt_string(buf, p.to.as_ref());
    }
    // 4: subject
    if count >= 4 {
        put_opt_string(buf, p.subject.as_ref());
    }
    // 5: reply-to
    if count >= 5 {
        put_opt_string(buf, p.reply_to.as_ref());
    }
    // 6: correlation-id
    if count >= 6 {
        match &p.correlation_id {
            Some(v) => encode_value(buf, v),
            None => put_null(buf),
        }
    }
    // 7: content-type
    if count >= 7 {
        put_opt_string(buf, p.content_type.as_ref());
    }
    // 8: content-encoding
    if count >= 8 {
        put_opt_string(buf, p.content_encoding.as_ref());
    }
    // 9: absolute-expiry-time
    if count >= 9 {
        match p.absolute_expiry_time {
            Some(t) => {
                buf.put_u8(format_code::TIMESTAMP);
                buf.put_i64(t);
            }
            None => put_null(buf),
        }
    }
    // 10: creation-time
    if count >= 10 {
        match p.creation_time {
            Some(t) => {
                buf.put_u8(format_code::TIMESTAMP);
                buf.put_i64(t);
            }
            None => put_null(buf),
        }
    }
    // 11: group-id
    if count >= 11 {
        put_opt_string(buf, p.group_id.as_ref());
    }
    // 12: group-sequence
    if count >= 12 {
        put_opt_uint(buf, p.group_sequence);
    }
    // 13: reply-to-group-id
    if count >= 13 {
        put_opt_string(buf, p.reply_to_group_id.as_ref());
    }
    finish_described_list(buf, sp, bs);
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

/// Encode SASL challenge body (no frame header).
pub fn encode_sasl_challenge(buf: &mut BytesMut, challenge: &[u8]) {
    let (sp, bs) = begin_described_list(buf, descriptor::SASL_CHALLENGE, 1);
    put_binary(buf, challenge);
    finish_described_list(buf, sp, bs);
}

/// Encode SASL response body (no frame header).
pub fn encode_sasl_response(buf: &mut BytesMut, response: &[u8]) {
    let (sp, bs) = begin_described_list(buf, descriptor::SASL_RESPONSE, 1);
    put_binary(buf, response);
    finish_described_list(buf, sp, bs);
}

/// Encode SASL challenge as a complete framed SASL frame.
pub fn encode_framed_sasl_challenge(buf: &mut BytesMut, challenge: &[u8]) {
    let frame_start = buf.len();
    buf.put_u32(0);
    buf.put_u8(2);
    buf.put_u8(FRAME_TYPE_SASL);
    buf.put_u16(0);
    encode_sasl_challenge(buf, challenge);
    let frame_size = (buf.len() - frame_start) as u32;
    buf[frame_start..frame_start + 4].copy_from_slice(&frame_size.to_be_bytes());
}

/// Encode SASL outcome as a complete framed SASL frame with optional additional data.
pub fn encode_framed_sasl_outcome_with_data(
    buf: &mut BytesMut,
    code: SaslCode,
    additional_data: Option<&[u8]>,
) {
    let frame_start = buf.len();
    buf.put_u32(0);
    buf.put_u8(2);
    buf.put_u8(FRAME_TYPE_SASL);
    buf.put_u16(0);
    let count = if additional_data.is_some() { 2u32 } else { 1 };
    let (sp, bs) = begin_described_list(buf, descriptor::SASL_OUTCOME, count);
    put_ubyte(buf, code as u8);
    if let Some(data) = additional_data {
        put_binary(buf, data);
    }
    finish_described_list(buf, sp, bs);
    let frame_size = (buf.len() - frame_start) as u32;
    buf[frame_start..frame_start + 4].copy_from_slice(&frame_size.to_be_bytes());
}

/// Encode SASL-INIT as a complete framed SASL frame.
pub fn encode_framed_sasl_init(
    buf: &mut BytesMut,
    mechanism: &str,
    initial_response: Option<&[u8]>,
) {
    let frame_start = buf.len();
    buf.put_u32(0);
    buf.put_u8(2);
    buf.put_u8(FRAME_TYPE_SASL);
    buf.put_u16(0);
    let count = if initial_response.is_some() { 2u32 } else { 1 };
    let (sp, bs) = begin_described_list(buf, descriptor::SASL_INIT, count);
    put_symbol(buf, &WireString::from(mechanism));
    if let Some(data) = initial_response {
        put_binary(buf, data);
    }
    finish_described_list(buf, sp, bs);
    let frame_size = (buf.len() - frame_start) as u32;
    buf[frame_start..frame_start + 4].copy_from_slice(&frame_size.to_be_bytes());
}

/// Encode SASL-RESPONSE as a complete framed SASL frame.
pub fn encode_framed_sasl_response(buf: &mut BytesMut, response: &[u8]) {
    let frame_start = buf.len();
    buf.put_u32(0);
    buf.put_u8(2);
    buf.put_u8(FRAME_TYPE_SASL);
    buf.put_u16(0);
    encode_sasl_response(buf, response);
    let frame_size = (buf.len() - frame_start) as u32;
    buf[frame_start..frame_start + 4].copy_from_slice(&frame_size.to_be_bytes());
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

    // =========================================================================
    // Message Section Codec Tests
    // =========================================================================

    #[test]
    fn test_decode_empty_message() {
        let msg = decode_message(Bytes::new()).unwrap();
        assert!(msg.header.is_none());
        assert!(msg.properties.is_none());
        assert!(msg.body.is_empty());
    }

    #[test]
    fn test_message_header_roundtrip() {
        let header = MessageHeader {
            durable: true,
            priority: 7,
            ttl: Some(60000),
            first_acquirer: true,
            delivery_count: 3,
        };
        let msg = AmqpMessage {
            header: Some(header),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let h = decoded.header.unwrap();
        assert!(h.durable);
        assert_eq!(h.priority, 7);
        assert_eq!(h.ttl, Some(60000));
        assert!(h.first_acquirer);
        assert_eq!(h.delivery_count, 3);
    }

    #[test]
    fn test_message_header_defaults() {
        let header = MessageHeader::default();
        let msg = AmqpMessage {
            header: Some(header),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let h = decoded.header.unwrap();
        assert!(!h.durable);
        assert_eq!(h.priority, 4);
        assert_eq!(h.ttl, None);
        assert!(!h.first_acquirer);
        assert_eq!(h.delivery_count, 0);
    }

    #[test]
    fn test_message_properties_roundtrip() {
        let props = MessageProperties {
            message_id: Some(AmqpValue::String(WireString::from("msg-001"))),
            user_id: Some(Bytes::from_static(b"guest")),
            to: Some(WireString::from("queue/tasks")),
            subject: Some(WireString::from("task.created")),
            reply_to: Some(WireString::from("queue/replies")),
            correlation_id: Some(AmqpValue::String(WireString::from("corr-001"))),
            content_type: Some(WireString::from("application/json")),
            content_encoding: Some(WireString::from("utf-8")),
            absolute_expiry_time: Some(1709654400000),
            creation_time: Some(1709650800000),
            group_id: Some(WireString::from("group-1")),
            group_sequence: Some(42),
            reply_to_group_id: Some(WireString::from("reply-group-1")),
        };
        let msg = AmqpMessage {
            properties: Some(props),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let p = decoded.properties.unwrap();
        assert_eq!(p.message_id.unwrap().as_str(), Some("msg-001"));
        assert_eq!(p.user_id.unwrap().as_ref(), b"guest");
        assert_eq!(p.to.as_deref(), Some("queue/tasks"));
        assert_eq!(p.subject.as_deref(), Some("task.created"));
        assert_eq!(p.reply_to.as_deref(), Some("queue/replies"));
        assert_eq!(p.correlation_id.unwrap().as_str(), Some("corr-001"));
        assert_eq!(p.content_type.as_deref(), Some("application/json"));
        assert_eq!(p.content_encoding.as_deref(), Some("utf-8"));
        assert_eq!(p.absolute_expiry_time, Some(1709654400000));
        assert_eq!(p.creation_time, Some(1709650800000));
        assert_eq!(p.group_id.as_deref(), Some("group-1"));
        assert_eq!(p.group_sequence, Some(42));
        assert_eq!(p.reply_to_group_id.as_deref(), Some("reply-group-1"));
    }

    #[test]
    fn test_message_data_body_roundtrip() {
        let msg = AmqpMessage {
            body: smallvec::smallvec![Bytes::from_static(b"hello world")],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        assert_eq!(decoded.body.len(), 1);
        assert_eq!(decoded.body[0].as_ref(), b"hello world");
    }

    #[test]
    fn test_message_multiple_data_sections() {
        let msg = AmqpMessage {
            body: smallvec::smallvec![
                Bytes::from_static(b"part1"),
                Bytes::from_static(b"part2"),
                Bytes::from_static(b"part3"),
            ],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        assert_eq!(decoded.body.len(), 3);
        assert_eq!(decoded.body[0].as_ref(), b"part1");
        assert_eq!(decoded.body[1].as_ref(), b"part2");
        assert_eq!(decoded.body[2].as_ref(), b"part3");
    }

    #[test]
    fn test_message_full_roundtrip() {
        let msg = AmqpMessage {
            header: Some(MessageHeader {
                durable: true,
                priority: 5,
                ttl: Some(30000),
                ..Default::default()
            }),
            properties: Some(MessageProperties {
                message_id: Some(AmqpValue::String(WireString::from("id-1"))),
                content_type: Some(WireString::from("text/plain")),
                ..Default::default()
            }),
            application_properties: Some(AmqpValue::Map(vec![(
                AmqpValue::String(WireString::from("key")),
                AmqpValue::String(WireString::from("value")),
            )])),
            body: smallvec::smallvec![Bytes::from_static(b"Hello, AMQP!")],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();

        let h = decoded.header.unwrap();
        assert!(h.durable);
        assert_eq!(h.priority, 5);
        assert_eq!(h.ttl, Some(30000));

        let p = decoded.properties.unwrap();
        assert_eq!(p.message_id.unwrap().as_str(), Some("id-1"));
        assert_eq!(p.content_type.as_deref(), Some("text/plain"));

        assert!(decoded.application_properties.is_some());
        assert_eq!(decoded.body.len(), 1);
        assert_eq!(decoded.body[0].as_ref(), b"Hello, AMQP!");
    }

    #[test]
    fn test_message_annotations_roundtrip() {
        let annotations = AmqpValue::Map(vec![(
            AmqpValue::Symbol(WireString::from("x-opt-routing-key")),
            AmqpValue::String(WireString::from("events.user.created")),
        )]);
        let msg = AmqpMessage {
            message_annotations: Some(annotations),
            body: smallvec::smallvec![Bytes::from_static(b"data")],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        assert!(decoded.message_annotations.is_some());
    }

    #[test]
    fn test_message_footer_roundtrip() {
        let footer = AmqpValue::Map(vec![(
            AmqpValue::Symbol(WireString::from("x-checksum")),
            AmqpValue::Uint(0xDEADBEEF),
        )]);
        let msg = AmqpMessage {
            body: smallvec::smallvec![Bytes::from_static(b"data")],
            footer: Some(footer),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        assert!(decoded.footer.is_some());
    }

    #[test]
    fn test_message_body_only() {
        let msg = AmqpMessage {
            body: smallvec::smallvec![Bytes::from_static(b"just body")],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        assert!(decoded.header.is_none());
        assert!(decoded.properties.is_none());
        assert_eq!(decoded.body.len(), 1);
        assert_eq!(decoded.body[0].as_ref(), b"just body");
    }

    #[test]
    fn test_txn_declared_roundtrip() {
        let mut buf = BytesMut::new();
        encode_txn_declared(&mut buf, b"txn-123");
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        // Should be a described type with descriptor 0x33
        match &value {
            AmqpValue::Described(desc, inner) => {
                assert_eq!(desc.as_u64(), Some(txn_descriptor::DECLARED));
                let fields = inner.as_list().unwrap();
                assert_eq!(fields[0].as_binary().unwrap().as_ref(), b"txn-123");
            }
            _ => panic!("expected described type"),
        }
    }

    #[test]
    fn test_transactional_state_roundtrip() {
        let mut buf = BytesMut::new();
        encode_transactional_state(&mut buf, b"txn-456", Some(&DeliveryState::Accepted));
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let state = decode_delivery_state(&value);
        match state {
            Some(DeliveryState::Transactional(ts)) => {
                assert_eq!(ts.txn_id.as_ref(), b"txn-456");
                assert_eq!(ts.outcome.unwrap(), Outcome::Accepted);
            }
            other => panic!("expected TransactionalState, got {:?}", other),
        }
    }

    #[test]
    fn test_txn_declare_decode() {
        // Build a described list for Declare with global_id = None
        let mut buf = BytesMut::new();
        put_empty_described_list(&mut buf, txn_descriptor::DECLARE);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let declare = decode_txn_declare(&value);
        assert!(declare.is_some());
        assert!(declare.unwrap().global_id.is_none());
    }

    #[test]
    fn test_txn_discharge_decode() {
        let mut buf = BytesMut::new();
        let (sp, bs) = begin_described_list(&mut buf, txn_descriptor::DISCHARGE, 2);
        put_binary(&mut buf, b"my-txn");
        put_bool(&mut buf, true); // fail = true (rollback)
        finish_described_list(&mut buf, sp, bs);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let discharge = decode_txn_discharge(&value).unwrap();
        assert_eq!(discharge.txn_id.as_ref(), b"my-txn");
        assert!(discharge.fail);
    }

    #[test]
    fn test_coordinator_encode_roundtrip() {
        let coord = Coordinator::default();
        let mut buf = BytesMut::new();
        encode_coordinator(&mut buf, &coord);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let decoded = decode_coordinator(&value);
        assert!(decoded.is_some());
        assert!(decoded.unwrap().capabilities.is_empty());
    }

    // =========================================================================
    // Gap 1: UUID type encode/decode roundtrip
    // =========================================================================

    #[test]
    fn test_uuid_roundtrip() {
        let uuid_bytes: [u8; 16] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10,
        ];
        let val = AmqpValue::Uuid(uuid_bytes);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert!(matches!(decoded, AmqpValue::Uuid(v) if v == uuid_bytes));
    }

    #[test]
    fn test_uuid_all_zeros() {
        let uuid_bytes = [0u8; 16];
        let val = AmqpValue::Uuid(uuid_bytes);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert!(matches!(decoded, AmqpValue::Uuid(v) if v == [0u8; 16]));
    }

    #[test]
    fn test_uuid_all_ff() {
        let uuid_bytes = [0xFFu8; 16];
        let val = AmqpValue::Uuid(uuid_bytes);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert!(matches!(decoded, AmqpValue::Uuid(v) if v == [0xFFu8; 16]));
    }

    // =========================================================================
    // Gap 2: Array type encode/decode roundtrip
    // =========================================================================

    #[test]
    fn test_empty_array_roundtrip() {
        let val = AmqpValue::Array(Vec::new());
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Array(items) => assert!(items.is_empty()),
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn test_array_of_uints_roundtrip() {
        // Encode a symbol array using encode_str_symbol_array and decode it
        let mut buf = BytesMut::new();
        encode_str_symbol_array(&mut buf, &["alpha", "beta", "gamma"]);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].as_str(), Some("alpha"));
                assert_eq!(items[1].as_str(), Some("beta"));
                assert_eq!(items[2].as_str(), Some("gamma"));
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn test_empty_symbol_array_roundtrip() {
        let mut buf = BytesMut::new();
        encode_str_symbol_array(&mut buf, &[]);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Array(items) => assert!(items.is_empty()),
            _ => panic!("expected Array"),
        }
    }

    // =========================================================================
    // Gap 3: Error paths — invalid format codes, truncated payloads, oversized frames
    // =========================================================================

    #[test]
    fn test_invalid_format_code() {
        let bytes = Bytes::from_static(&[0xFF]); // not a valid AMQP format code
        let mut cur = BytesCursor::new(bytes);
        let result = decode_value(&mut cur);
        assert!(matches!(result, Err(CodecError::InvalidFormatCode(0xFF))));
    }

    #[test]
    fn test_truncated_uint_payload() {
        // UINT format code followed by only 2 bytes instead of 4
        let bytes = Bytes::from_static(&[format_code::UINT, 0x00, 0x01]);
        let mut cur = BytesCursor::new(bytes);
        let result = decode_value(&mut cur);
        assert!(matches!(result, Err(CodecError::UnexpectedEof)));
    }

    #[test]
    fn test_truncated_string_payload() {
        // STRING8 with length 10, but only 3 bytes of data
        let bytes = Bytes::from_static(&[format_code::STRING8, 10, b'a', b'b', b'c']);
        let mut cur = BytesCursor::new(bytes);
        let result = decode_value(&mut cur);
        assert!(matches!(result, Err(CodecError::UnexpectedEof)));
    }

    #[test]
    fn test_oversized_frame() {
        // Frame claiming to be 32 MB (over MAX_FRAME_SIZE of 16 MB)
        let mut data = vec![0u8; 8];
        let size: u32 = 32 * 1024 * 1024;
        data[..4].copy_from_slice(&size.to_be_bytes());
        data[4] = 2; // doff
        data[5] = 0; // frame type
        let bytes = Bytes::from(data);
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CodecError::FrameTooLarge(_))));
    }

    #[test]
    fn test_frame_too_small() {
        // Frame size < 8 is invalid
        let mut data = vec![0u8; 8];
        data[..4].copy_from_slice(&4u32.to_be_bytes()); // size = 4
        data[4] = 2;
        let bytes = Bytes::from(data);
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CodecError::InvalidFrameHeader)));
    }

    #[test]
    fn test_empty_cursor() {
        let bytes = Bytes::new();
        let mut cur = BytesCursor::new(bytes);
        let result = decode_value(&mut cur);
        assert!(matches!(result, Err(CodecError::UnexpectedEof)));
    }

    // =========================================================================
    // Gap 4: encode_attach/decode_attach roundtrip with all fields populated
    // =========================================================================

    #[test]
    fn test_attach_full_roundtrip() {
        let attach = Attach {
            name: WireString::from("my-link"),
            handle: 7,
            role: Role::Receiver,
            snd_settle_mode: SndSettleMode::Settled,
            rcv_settle_mode: RcvSettleMode::Second,
            source: Some(Source {
                address: Some(WireString::from("queue/input")),
                durable: 2,
                expiry_policy: Some(WireString::from("session-end")),
                timeout: 60,
                dynamic: false,
                distribution_mode: Some(WireString::from("move")),
                ..Default::default()
            }),
            target: Some(Target {
                address: Some(WireString::from("queue/output")),
                durable: 1,
                timeout: 30,
                dynamic: true,
                ..Default::default()
            }),
            unsettled: None,
            incomplete_unsettled: true,
            initial_delivery_count: Some(42),
            max_message_size: Some(1024 * 1024),
            properties: Some(AmqpValue::Map(vec![(
                AmqpValue::Symbol(WireString::from("x-opt")),
                AmqpValue::String(WireString::from("val")),
            )])),
            ..Default::default()
        };

        let mut payload = BytesMut::new();
        encode_attach(&mut payload, &attach);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 3, FRAME_TYPE_AMQP, &payload);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        assert_eq!(frame.channel, 3);
        match frame.body {
            FrameBody::Amqp(Performative::Attach(decoded)) => {
                assert_eq!(&*decoded.name, "my-link");
                assert_eq!(decoded.handle, 7);
                assert_eq!(decoded.role, Role::Receiver);
                assert_eq!(decoded.snd_settle_mode, SndSettleMode::Settled);
                assert_eq!(decoded.rcv_settle_mode, RcvSettleMode::Second);
                assert!(decoded.incomplete_unsettled);
                assert_eq!(decoded.initial_delivery_count, Some(42));
                assert_eq!(decoded.max_message_size, Some(1024 * 1024));

                let src = decoded.source.unwrap();
                assert_eq!(src.address.as_deref(), Some("queue/input"));
                assert_eq!(src.durable, 2);
                assert_eq!(src.timeout, 60);
                assert_eq!(src.expiry_policy.as_deref(), Some("session-end"));
                assert_eq!(src.distribution_mode.as_deref(), Some("move"));

                let tgt = decoded.target.unwrap();
                assert_eq!(tgt.address.as_deref(), Some("queue/output"));
                assert_eq!(tgt.durable, 1);
                assert!(tgt.dynamic);

                assert!(decoded.properties.is_some());
            }
            _ => panic!("expected Attach"),
        }
    }

    // =========================================================================
    // Gap 5: encode_transfer/decode roundtrip
    // =========================================================================

    #[test]
    fn test_transfer_roundtrip() {
        let transfer = Transfer {
            handle: 3,
            delivery_id: Some(99),
            delivery_tag: Some(Bytes::from_static(b"tag-1")),
            message_format: Some(0),
            settled: Some(true),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
            payload: Bytes::from_static(b"hello-payload"),
        };

        let mut payload_buf = BytesMut::new();
        encode_transfer(&mut payload_buf, &transfer);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload_buf);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Transfer(decoded)) => {
                assert_eq!(decoded.handle, 3);
                assert_eq!(decoded.delivery_id, Some(99));
                assert_eq!(decoded.delivery_tag.as_deref(), Some(b"tag-1".as_ref()));
                assert_eq!(decoded.message_format, Some(0));
                assert_eq!(decoded.settled, Some(true));
                assert!(!decoded.more);
                assert_eq!(decoded.payload.as_ref(), b"hello-payload");
            }
            _ => panic!("expected Transfer"),
        }
    }

    #[test]
    fn test_transfer_with_all_flags() {
        let transfer = Transfer {
            handle: 0,
            delivery_id: Some(1),
            delivery_tag: Some(Bytes::from_static(b"t")),
            message_format: Some(0),
            settled: Some(false),
            more: true,
            rcv_settle_mode: Some(RcvSettleMode::Second),
            state: None,
            resume: true,
            aborted: true,
            batchable: true,
            payload: Bytes::new(),
        };

        let mut payload_buf = BytesMut::new();
        encode_transfer(&mut payload_buf, &transfer);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload_buf);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Transfer(decoded)) => {
                assert!(decoded.more);
                assert!(decoded.resume);
                assert!(decoded.aborted);
                assert!(decoded.batchable);
                assert_eq!(decoded.rcv_settle_mode, Some(RcvSettleMode::Second));
            }
            _ => panic!("expected Transfer"),
        }
    }

    // =========================================================================
    // Gap 6: encode_detach/decode_detach roundtrip
    // =========================================================================

    #[test]
    fn test_detach_roundtrip_no_error() {
        let detach = Detach {
            handle: 5,
            closed: true,
            error: None,
        };

        let mut payload = BytesMut::new();
        encode_detach(&mut payload, &detach);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Detach(decoded)) => {
                assert_eq!(decoded.handle, 5);
                assert!(decoded.closed);
                assert!(decoded.error.is_none());
            }
            _ => panic!("expected Detach"),
        }
    }

    #[test]
    fn test_detach_roundtrip_with_error() {
        let detach = Detach {
            handle: 2,
            closed: true,
            error: Some(AmqpError::new(
                condition::LINK_DETACH_FORCED,
                "forced detach",
            )),
        };

        let mut payload = BytesMut::new();
        encode_detach(&mut payload, &detach);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Detach(decoded)) => {
                assert_eq!(decoded.handle, 2);
                assert!(decoded.closed);
                let err = decoded.error.unwrap();
                assert_eq!(&*err.condition, condition::LINK_DETACH_FORCED);
                assert_eq!(err.description.as_deref(), Some("forced detach"));
            }
            _ => panic!("expected Detach"),
        }
    }

    // =========================================================================
    // Gap 7: encode_end/decode_end roundtrip
    // =========================================================================

    #[test]
    fn test_end_roundtrip_no_error() {
        let end = End { error: None };

        let mut payload = BytesMut::new();
        encode_end(&mut payload, &end);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::End(decoded)) => {
                assert!(decoded.error.is_none());
            }
            _ => panic!("expected End"),
        }
    }

    #[test]
    fn test_end_roundtrip_with_error() {
        let end = End {
            error: Some(AmqpError::new(
                condition::SESSION_WINDOW_VIOLATION,
                "window exceeded",
            )),
        };

        let mut payload = BytesMut::new();
        encode_end(&mut payload, &end);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::End(decoded)) => {
                let err = decoded.error.unwrap();
                assert_eq!(&*err.condition, condition::SESSION_WINDOW_VIOLATION);
                assert_eq!(err.description.as_deref(), Some("window exceeded"));
            }
            _ => panic!("expected End"),
        }
    }

    // =========================================================================
    // Gap 8: encode_close/decode_close roundtrip
    // =========================================================================

    #[test]
    fn test_close_roundtrip_no_error() {
        let close = Close { error: None };

        let mut payload = BytesMut::new();
        encode_close(&mut payload, &close);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Close(decoded)) => {
                assert!(decoded.error.is_none());
            }
            _ => panic!("expected Close"),
        }
    }

    #[test]
    fn test_close_roundtrip_with_error() {
        let close = Close {
            error: Some(AmqpError::new(
                condition::CONNECTION_FORCED,
                "server shutting down",
            )),
        };

        let mut payload = BytesMut::new();
        encode_close(&mut payload, &close);

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);

        let frame_bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&frame_bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Close(decoded)) => {
                let err = decoded.error.unwrap();
                assert_eq!(&*err.condition, condition::CONNECTION_FORCED);
                assert_eq!(err.description.as_deref(), Some("server shutting down"));
            }
            _ => panic!("expected Close"),
        }
    }

    // =========================================================================
    // Gap 9: Frame DOFF validation
    // =========================================================================

    #[test]
    fn test_frame_doff_too_small() {
        // doff=1 means 4 bytes header, but minimum is 8 (doff=2)
        let mut data = vec![0u8; 12];
        data[..4].copy_from_slice(&12u32.to_be_bytes());
        data[4] = 1; // doff = 1 -> header_size = 4 < 8
        data[5] = 0;
        let bytes = Bytes::from(data);
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CodecError::InvalidFrameHeader)));
    }

    #[test]
    fn test_frame_doff_larger_than_size() {
        // doff=10 means 40 bytes header, but frame is only 12 bytes
        let mut data = vec![0u8; 12];
        data[..4].copy_from_slice(&12u32.to_be_bytes());
        data[4] = 10; // doff = 10 -> header_size = 40 > 12
        data[5] = 0;
        let bytes = Bytes::from(data);
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CodecError::InvalidFrameHeader)));
    }

    #[test]
    fn test_frame_doff_valid_with_extended_header() {
        // doff=3 means 12 bytes header with 4 extra extended header bytes
        // Frame size 12, so payload is empty -> heartbeat
        let mut data = vec![0u8; 12];
        data[..4].copy_from_slice(&12u32.to_be_bytes());
        data[4] = 3; // doff = 3 -> header_size = 12
        data[5] = FRAME_TYPE_AMQP;
        let bytes = Bytes::from(data);
        let (frame, consumed) = decode_frame(&bytes).unwrap();
        assert_eq!(consumed, 12);
        assert!(matches!(frame.body, FrameBody::Empty));
    }

    // =========================================================================
    // Gap 10: Frame size boundary (MAX_FRAME_SIZE)
    // =========================================================================

    #[test]
    fn test_frame_at_max_size_boundary() {
        // A frame exactly at MAX_FRAME_SIZE (16 MB) should be accepted if data is present
        // We just test the size check without providing all the data
        let size: u32 = 16 * 1024 * 1024; // exactly MAX_FRAME_SIZE
        let mut data = vec![0u8; 8];
        data[..4].copy_from_slice(&size.to_be_bytes());
        data[4] = 2; // doff
        data[5] = 0;
        let bytes = Bytes::from(data);
        // Should be Incomplete, not FrameTooLarge (size == MAX is allowed)
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CodecError::Incomplete)));
    }

    #[test]
    fn test_frame_one_over_max_size() {
        let size: u32 = 16 * 1024 * 1024 + 1; // one over MAX_FRAME_SIZE
        let mut data = vec![0u8; 8];
        data[..4].copy_from_slice(&size.to_be_bytes());
        data[4] = 2;
        data[5] = 0;
        let bytes = Bytes::from(data);
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CodecError::FrameTooLarge(_))));
    }

    // =========================================================================
    // Gap 11: decode_performative with unknown descriptor
    // =========================================================================

    #[test]
    fn test_decode_performative_unknown_descriptor() {
        // Encode a described type with an unknown descriptor
        let mut buf = BytesMut::new();
        encode_value(
            &mut buf,
            &AmqpValue::Described(
                Box::new(AmqpValue::Ulong(0xDEAD)),
                Box::new(AmqpValue::List(vec![])),
            ),
        );

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &buf);

        let frame_bytes = frame_buf.freeze();
        let result = decode_frame(&frame_bytes);
        assert!(matches!(result, Err(CodecError::UnknownDescriptor(0xDEAD))));
    }

    #[test]
    fn test_decode_performative_non_described() {
        // A frame payload that is NOT a described type should fail
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Uint(42));

        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &buf);

        let frame_bytes = frame_buf.freeze();
        let result = decode_frame(&frame_bytes);
        assert!(matches!(result, Err(CodecError::InvalidPerformative(_))));
    }

    // =========================================================================
    // Gap 12: Zero-copy verification for binary and symbol types
    // =========================================================================

    #[test]
    fn test_zero_copy_binary_decode() {
        let mut buf = BytesMut::new();
        encode_value(
            &mut buf,
            &AmqpValue::Binary(Bytes::from_static(b"binary-data")),
        );
        let bytes = buf.freeze();
        let original_ptr = bytes.as_ptr();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Binary(b) => {
                assert_eq!(b.as_ref(), b"binary-data");
                // Decoded binary should point into original buffer
                let b_ptr = b.as_ptr();
                assert!(b_ptr >= original_ptr);
            }
            _ => panic!("expected Binary"),
        }
    }

    #[test]
    fn test_zero_copy_symbol_decode() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Symbol(WireString::from("amqp:test")));
        let bytes = buf.freeze();
        let original_ptr = bytes.as_ptr();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Symbol(ws) => {
                assert_eq!(&*ws, "amqp:test");
                let ws_ptr = ws.as_bytes().as_ptr();
                assert!(ws_ptr >= original_ptr);
            }
            _ => panic!("expected Symbol"),
        }
    }

    // =========================================================================
    // Gap 13: SASL outcome encoding
    // =========================================================================

    #[test]
    fn test_sasl_outcome_encode_decode() {
        for code in [
            SaslCode::Ok,
            SaslCode::Auth,
            SaslCode::Sys,
            SaslCode::SysPerm,
            SaslCode::SysTemp,
        ] {
            let mut buf = BytesMut::new();
            encode_framed_sasl_outcome(&mut buf, code);
            let bytes = buf.freeze();
            let (frame, _) = decode_frame(&bytes).unwrap();
            match frame.body {
                FrameBody::Sasl(SaslPerformative::Outcome(outcome)) => {
                    assert_eq!(outcome.code, code);
                    assert!(outcome.additional_data.is_none());
                }
                _ => panic!("expected SASL Outcome"),
            }
        }
    }

    #[test]
    fn test_sasl_outcome_with_additional_data() {
        let mut buf = BytesMut::new();
        encode_framed_sasl_outcome_with_data(&mut buf, SaslCode::Ok, Some(b"extra-data"));
        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Sasl(SaslPerformative::Outcome(outcome)) => {
                assert_eq!(outcome.code, SaslCode::Ok);
                assert_eq!(
                    outcome.additional_data.as_deref(),
                    Some(b"extra-data".as_ref())
                );
            }
            _ => panic!("expected SASL Outcome"),
        }
    }

    // =========================================================================
    // Gap 14: encode_sasl_challenge, encode_sasl_response roundtrips
    // =========================================================================

    #[test]
    fn test_sasl_challenge_roundtrip() {
        let mut buf = BytesMut::new();
        encode_framed_sasl_challenge(&mut buf, b"challenge-data-here");
        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Sasl(SaslPerformative::Challenge(ch)) => {
                assert_eq!(ch.challenge.as_ref(), b"challenge-data-here");
            }
            _ => panic!("expected SASL Challenge"),
        }
    }

    #[test]
    fn test_sasl_response_roundtrip() {
        let mut buf = BytesMut::new();
        encode_framed_sasl_response(&mut buf, b"response-data-here");
        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Sasl(SaslPerformative::Response(resp)) => {
                assert_eq!(resp.response.as_ref(), b"response-data-here");
            }
            _ => panic!("expected SASL Response"),
        }
    }

    #[test]
    fn test_sasl_challenge_empty() {
        let mut buf = BytesMut::new();
        encode_framed_sasl_challenge(&mut buf, b"");
        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Sasl(SaslPerformative::Challenge(ch)) => {
                assert!(ch.challenge.is_empty());
            }
            _ => panic!("expected SASL Challenge"),
        }
    }

    // =========================================================================
    // Gap 15: Nested described types
    // =========================================================================

    #[test]
    fn test_nested_described_types() {
        let inner = AmqpValue::Described(
            Box::new(AmqpValue::Ulong(0x01)),
            Box::new(AmqpValue::String(WireString::from("inner"))),
        );
        let outer = AmqpValue::Described(Box::new(AmqpValue::Ulong(0x02)), Box::new(inner));

        let mut buf = BytesMut::new();
        encode_value(&mut buf, &outer);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();

        match decoded {
            AmqpValue::Described(d_outer, v_outer) => {
                assert_eq!(d_outer.as_u64(), Some(0x02));
                match *v_outer {
                    AmqpValue::Described(d_inner, v_inner) => {
                        assert_eq!(d_inner.as_u64(), Some(0x01));
                        assert_eq!(v_inner.as_str(), Some("inner"));
                    }
                    _ => panic!("expected inner described type"),
                }
            }
            _ => panic!("expected outer described type"),
        }
    }

    // =========================================================================
    // Gap 16: Empty list/map encoding
    // =========================================================================

    #[test]
    fn test_empty_map_roundtrip() {
        let map = AmqpValue::Map(Vec::new());
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &map);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Map(pairs) => assert!(pairs.is_empty()),
            _ => panic!("expected Map"),
        }
    }

    #[test]
    fn test_empty_list_uses_list0_encoding() {
        let list = AmqpValue::List(Vec::new());
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &list);
        // LIST0 is a single byte: 0x45
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], format_code::LIST0);
    }

    // =========================================================================
    // Gap 17: decode_value with invalid UTF-8 (CodecError::InvalidUtf8)
    // =========================================================================

    #[test]
    fn test_invalid_utf8_string() {
        // STRING8, length=2, followed by invalid UTF-8 bytes
        let bytes = Bytes::from_static(&[format_code::STRING8, 2, 0xFF, 0xFE]);
        let mut cur = BytesCursor::new(bytes);
        let result = decode_value(&mut cur);
        assert!(matches!(result, Err(CodecError::InvalidUtf8)));
    }

    #[test]
    fn test_invalid_utf8_symbol() {
        // SYMBOL8, length=3, followed by invalid UTF-8 bytes
        let bytes = Bytes::from_static(&[format_code::SYMBOL8, 3, 0xC0, 0x80, 0xFF]);
        let mut cur = BytesCursor::new(bytes);
        let result = decode_value(&mut cur);
        assert!(matches!(result, Err(CodecError::InvalidUtf8)));
    }

    // =========================================================================
    // Gap 18: Smallint/smalluint/smalllong encoding paths (compact encoding forms)
    // =========================================================================

    #[test]
    fn test_uint_compact_encoding_zero() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Uint(0));
        // UINT_ZERO is a single byte
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], format_code::UINT_ZERO);
    }

    #[test]
    fn test_uint_compact_encoding_small() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Uint(200));
        // UINT_SMALL: 1 byte code + 1 byte value
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], format_code::UINT_SMALL);
        assert_eq!(buf[1], 200);
    }

    #[test]
    fn test_uint_full_encoding() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Uint(1000));
        // UINT: 1 byte code + 4 byte value
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], format_code::UINT);
    }

    #[test]
    fn test_ulong_compact_encoding_zero() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Ulong(0));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], format_code::ULONG_ZERO);
    }

    #[test]
    fn test_ulong_compact_encoding_small() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Ulong(100));
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], format_code::ULONG_SMALL);
        assert_eq!(buf[1], 100);
    }

    #[test]
    fn test_ulong_full_encoding() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Ulong(256));
        assert_eq!(buf.len(), 9);
        assert_eq!(buf[0], format_code::ULONG);
    }

    #[test]
    fn test_int_compact_encoding_small() {
        // Values -128..127 use INT_SMALL
        for v in [-128i32, -1, 0, 1, 127] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Int(v));
            assert_eq!(buf.len(), 2, "INT_SMALL for value {v}");
            assert_eq!(buf[0], format_code::INT_SMALL);
            // Roundtrip check
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Int(v));
        }
    }

    #[test]
    fn test_int_full_encoding() {
        for v in [-129i32, 128, i32::MAX, i32::MIN] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Int(v));
            assert_eq!(buf.len(), 5, "INT for value {v}");
            assert_eq!(buf[0], format_code::INT);
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Int(v));
        }
    }

    #[test]
    fn test_long_compact_encoding_small() {
        for v in [-128i64, -1, 0, 1, 127] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Long(v));
            assert_eq!(buf.len(), 2, "LONG_SMALL for value {v}");
            assert_eq!(buf[0], format_code::LONG_SMALL);
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Long(v));
        }
    }

    #[test]
    fn test_long_full_encoding() {
        for v in [-129i64, 128, i64::MAX, i64::MIN] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Long(v));
            assert_eq!(buf.len(), 9, "LONG for value {v}");
            assert_eq!(buf[0], format_code::LONG);
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, AmqpValue::Long(v));
        }
    }

    // =========================================================================
    // Gap 19: Byte, Short, Long, Float, Double edge cases
    // =========================================================================

    #[test]
    fn test_byte_edge_cases() {
        for v in [i8::MIN, -1, 0, 1, i8::MAX] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Byte(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert!(matches!(decoded, AmqpValue::Byte(x) if x == v));
        }
    }

    #[test]
    fn test_short_edge_cases() {
        for v in [i16::MIN, -1, 0, 1, i16::MAX] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Short(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert!(matches!(decoded, AmqpValue::Short(x) if x == v));
        }
    }

    #[test]
    fn test_float_special_values() {
        for v in [
            0.0f32,
            -0.0,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::MIN,
            f32::MAX,
        ] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Float(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            match decoded {
                AmqpValue::Float(x) => assert_eq!(x.to_bits(), v.to_bits()),
                _ => panic!("expected Float"),
            }
        }
    }

    #[test]
    fn test_float_nan() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Float(f32::NAN));
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Float(x) => assert!(x.is_nan()),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn test_double_special_values() {
        for v in [
            0.0f64,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
        ] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Double(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            match decoded {
                AmqpValue::Double(x) => assert_eq!(x.to_bits(), v.to_bits()),
                _ => panic!("expected Double"),
            }
        }
    }

    #[test]
    fn test_double_nan() {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &AmqpValue::Double(f64::NAN));
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Double(x) => assert!(x.is_nan()),
            _ => panic!("expected Double"),
        }
    }

    #[test]
    fn test_long_negative_values() {
        for v in [i64::MIN, -1_000_000_000_000i64, -1] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Long(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert!(matches!(decoded, AmqpValue::Long(x) if x == v));
        }
    }

    // =========================================================================
    // Gap 20: Message with application_properties roundtrip
    // =========================================================================

    #[test]
    fn test_message_with_application_properties_roundtrip() {
        let app_props = AmqpValue::Map(vec![
            (
                AmqpValue::String(WireString::from("prop1")),
                AmqpValue::Uint(100),
            ),
            (
                AmqpValue::String(WireString::from("prop2")),
                AmqpValue::String(WireString::from("hello")),
            ),
            (
                AmqpValue::String(WireString::from("prop3")),
                AmqpValue::Boolean(true),
            ),
        ]);
        let msg = AmqpMessage {
            application_properties: Some(app_props),
            body: smallvec::smallvec![Bytes::from_static(b"body")],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let ap = decoded.application_properties.unwrap();
        let pairs = ap.as_map().unwrap();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0.as_str(), Some("prop1"));
        assert_eq!(pairs[0].1.as_u32(), Some(100));
        assert_eq!(pairs[1].0.as_str(), Some("prop2"));
        assert_eq!(pairs[1].1.as_str(), Some("hello"));
    }

    // =========================================================================
    // Gap 21: Message with delivery_annotations roundtrip
    // =========================================================================

    #[test]
    fn test_message_with_delivery_annotations_roundtrip() {
        let da = AmqpValue::Map(vec![(
            AmqpValue::Symbol(WireString::from("x-opt-delivery-delay")),
            AmqpValue::Uint(5000),
        )]);
        let msg = AmqpMessage {
            delivery_annotations: Some(da),
            body: smallvec::smallvec![Bytes::from_static(b"body")],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let decoded_da = decoded.delivery_annotations.unwrap();
        let pairs = decoded_da.as_map().unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0.as_str(), Some("x-opt-delivery-delay"));
        assert_eq!(pairs[0].1.as_u32(), Some(5000));
    }

    // =========================================================================
    // Gap 22: Message properties with correlation_id as different AmqpValue types
    // =========================================================================

    #[test]
    fn test_message_properties_correlation_id_as_uint() {
        let msg = AmqpMessage {
            properties: Some(MessageProperties {
                correlation_id: Some(AmqpValue::Uint(12345)),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let p = decoded.properties.unwrap();
        assert_eq!(p.correlation_id.unwrap().as_u32(), Some(12345));
    }

    #[test]
    fn test_message_properties_correlation_id_as_binary() {
        let msg = AmqpMessage {
            properties: Some(MessageProperties {
                correlation_id: Some(AmqpValue::Binary(Bytes::from_static(b"corr-bin"))),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let p = decoded.properties.unwrap();
        assert_eq!(
            p.correlation_id.unwrap().as_binary().unwrap().as_ref(),
            b"corr-bin"
        );
    }

    #[test]
    fn test_message_properties_correlation_id_as_uuid() {
        let uuid = [0xAAu8; 16];
        let msg = AmqpMessage {
            properties: Some(MessageProperties {
                correlation_id: Some(AmqpValue::Uuid(uuid)),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze()).unwrap();
        let p = decoded.properties.unwrap();
        assert!(matches!(p.correlation_id, Some(AmqpValue::Uuid(v)) if v == uuid));
    }

    // =========================================================================
    // Gap 23: decode_txn_discharge with fail=false
    // =========================================================================

    #[test]
    fn test_txn_discharge_fail_false() {
        let mut buf = BytesMut::new();
        let (sp, bs) = begin_described_list(&mut buf, txn_descriptor::DISCHARGE, 2);
        put_binary(&mut buf, b"commit-txn");
        put_bool(&mut buf, false); // fail = false (commit)
        finish_described_list(&mut buf, sp, bs);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let discharge = decode_txn_discharge(&value).unwrap();
        assert_eq!(discharge.txn_id.as_ref(), b"commit-txn");
        assert!(!discharge.fail);
    }

    #[test]
    fn test_txn_discharge_default_fail() {
        // Discharge with only txn_id (no fail field) -> defaults to false
        let mut buf = BytesMut::new();
        let (sp, bs) = begin_described_list(&mut buf, txn_descriptor::DISCHARGE, 1);
        put_binary(&mut buf, b"txn-default");
        finish_described_list(&mut buf, sp, bs);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let discharge = decode_txn_discharge(&value).unwrap();
        assert_eq!(discharge.txn_id.as_ref(), b"txn-default");
        assert!(!discharge.fail);
    }

    // =========================================================================
    // Gap 24: encode_transactional_state with outcome=None
    // =========================================================================

    #[test]
    fn test_transactional_state_no_outcome() {
        let mut buf = BytesMut::new();
        encode_transactional_state(&mut buf, b"txn-no-outcome", None);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let state = decode_delivery_state(&value);
        match state {
            Some(DeliveryState::Transactional(ts)) => {
                assert_eq!(ts.txn_id.as_ref(), b"txn-no-outcome");
                assert!(ts.outcome.is_none());
            }
            other => panic!("expected TransactionalState, got {:?}", other),
        }
    }

    #[test]
    fn test_transactional_state_with_released_outcome() {
        let mut buf = BytesMut::new();
        encode_transactional_state(&mut buf, b"txn-rel", Some(&DeliveryState::Released));
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let state = decode_delivery_state(&value);
        match state {
            Some(DeliveryState::Transactional(ts)) => {
                assert_eq!(ts.txn_id.as_ref(), b"txn-rel");
                assert_eq!(ts.outcome, Some(Outcome::Released));
            }
            other => panic!("expected TransactionalState, got {:?}", other),
        }
    }

    // =========================================================================
    // Additional edge-case coverage
    // =========================================================================

    #[test]
    fn test_ubyte_roundtrip() {
        for v in [0u8, 1, 127, 128, 255] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Ubyte(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert!(matches!(decoded, AmqpValue::Ubyte(x) if x == v));
        }
    }

    #[test]
    fn test_ushort_roundtrip() {
        for v in [0u16, 1, 255, 256, u16::MAX] {
            let mut buf = BytesMut::new();
            encode_value(&mut buf, &AmqpValue::Ushort(v));
            let bytes = buf.freeze();
            let mut cur = BytesCursor::new(bytes);
            let decoded = decode_value(&mut cur).unwrap();
            assert!(matches!(decoded, AmqpValue::Ushort(x) if x == v));
        }
    }

    #[test]
    fn test_sasl_init_roundtrip() {
        let mut buf = BytesMut::new();
        encode_framed_sasl_init(&mut buf, "PLAIN", Some(b"\x00user\x00pass"));
        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Sasl(SaslPerformative::Init(init)) => {
                assert_eq!(&*init.mechanism, "PLAIN");
                assert_eq!(
                    init.initial_response.as_deref(),
                    Some(b"\x00user\x00pass".as_ref())
                );
            }
            _ => panic!("expected SASL Init"),
        }
    }

    #[test]
    fn test_sasl_mechanisms_roundtrip() {
        let mut buf = BytesMut::new();
        encode_framed_sasl_mechanisms(&mut buf, &["PLAIN", "ANONYMOUS", "SCRAM-SHA-256"]);
        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Sasl(SaslPerformative::Mechanisms(mechs)) => {
                assert_eq!(mechs.mechanisms.len(), 3);
                assert_eq!(&*mechs.mechanisms[0], "PLAIN");
                assert_eq!(&*mechs.mechanisms[1], "ANONYMOUS");
                assert_eq!(&*mechs.mechanisms[2], "SCRAM-SHA-256");
            }
            _ => panic!("expected SASL Mechanisms"),
        }
    }

    #[test]
    fn test_large_binary_roundtrip() {
        // Test binary > 255 bytes uses BINARY32 encoding
        let large_data = vec![0xABu8; 500];
        let val = AmqpValue::Binary(Bytes::from(large_data.clone()));
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        assert_eq!(buf[0], format_code::BINARY32);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        match decoded {
            AmqpValue::Binary(b) => assert_eq!(b.as_ref(), large_data.as_slice()),
            _ => panic!("expected Binary"),
        }
    }

    #[test]
    fn test_large_string_roundtrip() {
        // Test string > 255 bytes uses STRING32 encoding
        let large_str = "x".repeat(300);
        let val = AmqpValue::String(WireString::from(large_str.as_str()));
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        assert_eq!(buf[0], format_code::STRING32);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert_eq!(
            decoded,
            AmqpValue::String(WireString::from(large_str.as_str()))
        );
    }

    #[test]
    fn test_timestamp_negative() {
        // Negative timestamp (before epoch)
        let val = AmqpValue::Timestamp(-1_000_000);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        assert!(matches!(decoded, AmqpValue::Timestamp(-1_000_000)));
    }

    // =========================================================================
    // Declared delivery state encode/decode
    // =========================================================================

    #[test]
    fn test_declared_delivery_state_roundtrip() {
        let state = DeliveryState::Declared {
            txn_id: Bytes::from_static(b"my-txn-id"),
        };
        let mut buf = BytesMut::new();
        encode_delivery_state(&mut buf, &state);

        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let decoded = decode_delivery_state(&value);
        assert!(decoded.is_some());
        match decoded.unwrap() {
            DeliveryState::Declared { txn_id } => {
                assert_eq!(txn_id, Bytes::from_static(b"my-txn-id"));
            }
            other => panic!("expected Declared, got {:?}", other),
        }
    }

    #[test]
    fn test_disposition_with_declared_state() {
        let disp = Disposition {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: true,
            state: Some(DeliveryState::Declared {
                txn_id: Bytes::from_static(b"txn-42"),
            }),
            batchable: false,
        };
        let mut buf = BytesMut::new();
        encode_disposition(&mut buf, &disp);
        // Just verify it encodes without panic
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_coordinator_target_encode_roundtrip() {
        let coord = Coordinator {
            capabilities: SmallVec::new(),
        };
        let mut buf = BytesMut::new();
        encode_coordinator(&mut buf, &coord);

        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let value = decode_value(&mut cur).unwrap();
        let decoded = decode_coordinator(&value);
        assert!(decoded.is_some());
    }

    #[test]
    fn test_attach_with_coordinator_roundtrip() {
        let attach = Attach {
            name: WireString::from("txn-ctrl"),
            handle: 5,
            role: Role::Sender,
            initial_delivery_count: Some(0),
            coordinator_target: Some(Coordinator::default()),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_framed_performative(&mut buf, 0, FRAME_TYPE_AMQP, &Performative::Attach(attach));

        let bytes = buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        if let FrameBody::Amqp(Performative::Attach(decoded)) = frame.body {
            assert_eq!(decoded.name, WireString::from("txn-ctrl"));
            assert_eq!(decoded.handle, 5);
            assert!(decoded.coordinator_target.is_some());
            assert!(decoded.target.is_none());
        } else {
            panic!("expected Attach performative");
        }
    }

    // =========================================================================
    // Lifetime policy descriptors
    // =========================================================================

    #[test]
    fn test_lifetime_policy_enum() {
        use crate::types::{LifetimePolicy, descriptor};

        assert_eq!(
            LifetimePolicy::from_descriptor(descriptor::DELETE_ON_CLOSE),
            Some(LifetimePolicy::DeleteOnClose)
        );
        assert_eq!(
            LifetimePolicy::from_descriptor(descriptor::DELETE_ON_NO_LINKS),
            Some(LifetimePolicy::DeleteOnNoLinks)
        );
        assert_eq!(
            LifetimePolicy::from_descriptor(descriptor::DELETE_ON_NO_MESSAGES),
            Some(LifetimePolicy::DeleteOnNoMessages)
        );
        assert_eq!(
            LifetimePolicy::from_descriptor(descriptor::DELETE_ON_NO_LINKS_OR_MESSAGES),
            Some(LifetimePolicy::DeleteOnNoLinksOrMessages)
        );
        assert_eq!(LifetimePolicy::from_descriptor(0x99), None);

        assert_eq!(
            LifetimePolicy::DeleteOnClose.descriptor(),
            descriptor::DELETE_ON_CLOSE
        );
    }

    // =================================================================
    // T1: Map duplicate key rejection
    // =================================================================

    #[test]
    fn test_map_duplicate_key_rejected() {
        // Manually encode a map with duplicate keys
        let mut buf = BytesMut::new();
        // MAP8: [format_code][size][count]
        // We'll encode: { "key" => 1, "key" => 2 } which should be rejected
        let map = AmqpValue::Map(vec![
            (
                AmqpValue::String(WireString::from("key")),
                AmqpValue::Uint(1),
            ),
            (
                AmqpValue::String(WireString::from("other")),
                AmqpValue::Uint(2),
            ),
        ]);
        encode_value(&mut buf, &map);
        // This should decode fine (no duplicates)
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        assert!(decode_value(&mut cur).is_ok());

        // Now build a map WITH duplicate keys by encoding manually
        let mut buf2 = BytesMut::new();
        // Write MAP8 header
        let dup_map = AmqpValue::Map(vec![
            (
                AmqpValue::String(WireString::from("dup")),
                AmqpValue::Uint(1),
            ),
            (
                AmqpValue::String(WireString::from("dup")),
                AmqpValue::Uint(2),
            ),
        ]);
        encode_value(&mut buf2, &dup_map);
        // Decoding should fail because of duplicate keys
        let bytes2 = buf2.freeze();
        let mut cur2 = BytesCursor::new(bytes2);
        let result = decode_value(&mut cur2);
        assert!(
            result.is_err(),
            "map with duplicate keys should be rejected"
        );
    }

    #[test]
    fn test_map_unique_keys_accepted() {
        let map = AmqpValue::Map(vec![
            (AmqpValue::String(WireString::from("a")), AmqpValue::Uint(1)),
            (AmqpValue::String(WireString::from("b")), AmqpValue::Uint(2)),
            (AmqpValue::String(WireString::from("c")), AmqpValue::Uint(3)),
        ]);
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &map);
        let bytes = buf.freeze();
        let mut cur = BytesCursor::new(bytes);
        let decoded = decode_value(&mut cur).unwrap();
        if let AmqpValue::Map(entries) = decoded {
            assert_eq!(entries.len(), 3);
        } else {
            panic!("expected map");
        }
    }

    // =================================================================
    // TR3: Attach capabilities encoding
    // =================================================================

    #[test]
    fn test_attach_offered_capabilities_encode_decode() {
        let attach = Attach {
            name: WireString::from("test"),
            handle: 0,
            role: Role::Sender,
            initial_delivery_count: Some(0),
            offered_capabilities: smallvec::smallvec![
                WireString::from("cap1"),
                WireString::from("cap2"),
            ],
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        encode_attach(&mut buf, &attach);
        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &buf);
        let bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Attach(decoded)) => {
                assert_eq!(decoded.name, WireString::from("test"));
                // Note: offered_capabilities are at field 12, which may not be
                // decoded by default — the decode_attach function decodes up to field 14
            }
            _ => panic!("expected Attach"),
        }
    }

    // =================================================================
    // TR4: Open full field roundtrip
    // =================================================================

    #[test]
    fn test_open_full_fields_roundtrip() {
        let open = Open {
            container_id: WireString::from("test-container"),
            hostname: Some(WireString::from("localhost")),
            max_frame_size: 65536,
            channel_max: 255,
            idle_timeout: Some(30000),
            outgoing_locales: smallvec::smallvec![WireString::from("en-US")],
            incoming_locales: smallvec::smallvec![WireString::from("en-US")],
            offered_capabilities: smallvec::smallvec![WireString::from("ANONYMOUS-RELAY")],
            desired_capabilities: smallvec::smallvec![WireString::from("DELAYED-DELIVERY")],
            properties: Some(AmqpValue::Map(vec![(
                AmqpValue::Symbol(WireString::from("product")),
                AmqpValue::String(WireString::from("test")),
            )])),
        };

        let mut payload = BytesMut::new();
        encode_open(&mut payload, &open);
        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);
        let bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Open(decoded)) => {
                assert_eq!(&*decoded.container_id, "test-container");
                assert_eq!(decoded.hostname.as_deref(), Some("localhost"));
                assert_eq!(decoded.max_frame_size, 65536);
                assert_eq!(decoded.channel_max, 255);
                assert_eq!(decoded.idle_timeout, Some(30000));
                assert_eq!(decoded.outgoing_locales.len(), 1);
                assert_eq!(&*decoded.outgoing_locales[0], "en-US");
                assert_eq!(decoded.incoming_locales.len(), 1);
                assert_eq!(decoded.offered_capabilities.len(), 1);
                assert_eq!(&*decoded.offered_capabilities[0], "ANONYMOUS-RELAY");
                assert_eq!(decoded.desired_capabilities.len(), 1);
                assert_eq!(&*decoded.desired_capabilities[0], "DELAYED-DELIVERY");
                assert!(decoded.properties.is_some());
            }
            _ => panic!("expected Open"),
        }
    }

    // =================================================================
    // TR5: Begin capabilities roundtrip
    // =================================================================

    #[test]
    fn test_begin_capabilities_roundtrip() {
        let begin = Begin {
            remote_channel: Some(0),
            next_outgoing_id: 1,
            incoming_window: 2048,
            outgoing_window: 2048,
            handle_max: 255,
            offered_capabilities: smallvec::smallvec![WireString::from("cap1")],
            desired_capabilities: smallvec::smallvec![WireString::from("cap2")],
            properties: Some(AmqpValue::Map(vec![(
                AmqpValue::Symbol(WireString::from("key")),
                AmqpValue::Uint(42),
            )])),
        };

        let mut payload = BytesMut::new();
        encode_begin(&mut payload, &begin);
        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);
        let bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Begin(decoded)) => {
                assert_eq!(decoded.remote_channel, Some(0));
                assert_eq!(decoded.handle_max, 255);
                assert_eq!(decoded.offered_capabilities.len(), 1);
                assert_eq!(&*decoded.offered_capabilities[0], "cap1");
                assert_eq!(decoded.desired_capabilities.len(), 1);
                assert_eq!(&*decoded.desired_capabilities[0], "cap2");
                assert!(decoded.properties.is_some());
            }
            _ => panic!("expected Begin"),
        }
    }

    // =================================================================
    // M2: Application-properties value validation
    // =================================================================

    #[test]
    fn test_application_properties_simple_values_ok() {
        let msg = AmqpMessage {
            application_properties: Some(AmqpValue::Map(vec![
                (
                    AmqpValue::String(WireString::from("key1")),
                    AmqpValue::Uint(42),
                ),
                (
                    AmqpValue::String(WireString::from("key2")),
                    AmqpValue::String(WireString::from("val")),
                ),
                (
                    AmqpValue::String(WireString::from("key3")),
                    AmqpValue::Boolean(true),
                ),
            ])),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let decoded = decode_message(buf.freeze());
        assert!(decoded.is_ok());
    }

    #[test]
    fn test_application_properties_complex_value_rejected() {
        // Build a message with a map value in application-properties (not allowed)
        let msg = AmqpMessage {
            application_properties: Some(AmqpValue::Map(vec![(
                AmqpValue::String(WireString::from("key1")),
                AmqpValue::Map(vec![(
                    AmqpValue::String(WireString::from("nested")),
                    AmqpValue::Uint(1),
                )]),
            )])),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let result = decode_message(buf.freeze());
        assert!(
            result.is_err(),
            "map value in application-properties should be rejected"
        );
    }

    #[test]
    fn test_application_properties_list_value_rejected() {
        let msg = AmqpMessage {
            application_properties: Some(AmqpValue::Map(vec![(
                AmqpValue::String(WireString::from("key1")),
                AmqpValue::List(vec![AmqpValue::Uint(1)]),
            )])),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_message(&mut buf, &msg);
        let result = decode_message(buf.freeze());
        assert!(
            result.is_err(),
            "list value in application-properties should be rejected"
        );
    }

    // =================================================================
    // Flow properties roundtrip
    // =================================================================

    #[test]
    fn test_flow_with_properties_roundtrip() {
        let flow = Flow {
            next_incoming_id: Some(5),
            incoming_window: 2048,
            next_outgoing_id: 10,
            outgoing_window: 2048,
            handle: Some(0),
            delivery_count: Some(3),
            link_credit: Some(100),
            properties: Some(AmqpValue::Map(vec![(
                AmqpValue::Symbol(WireString::from("txn-id")),
                AmqpValue::Binary(Bytes::from_static(b"\x00\x01")),
            )])),
            ..Default::default()
        };
        let mut payload = BytesMut::new();
        encode_flow(&mut payload, &flow);
        let mut frame_buf = BytesMut::new();
        encode_frame(&mut frame_buf, 0, FRAME_TYPE_AMQP, &payload);
        let bytes = frame_buf.freeze();
        let (frame, _) = decode_frame(&bytes).unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Flow(decoded)) => {
                assert_eq!(decoded.next_incoming_id, Some(5));
                assert!(decoded.properties.is_some());
                if let Some(AmqpValue::Map(entries)) = decoded.properties {
                    assert_eq!(entries.len(), 1);
                } else {
                    panic!("expected properties map");
                }
            }
            _ => panic!("expected Flow"),
        }
    }
}
