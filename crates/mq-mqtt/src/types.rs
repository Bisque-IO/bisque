//! MQTT packet type definitions for MQTT 3.1.1 and 5.0.
//!
//! Provides a comprehensive type system for all MQTT control packets,
//! QoS levels, connect flags, and property types.

use bytes::Bytes;
use smallvec::SmallVec;

/// Property wire sizes for pre-computing remaining length.
pub const PROP_SIZE_U8: usize = 2; // id(1) + u8(1)
pub const PROP_SIZE_U16: usize = 3; // id(1) + u16(2)
pub const PROP_SIZE_U32: usize = 5; // id(1) + u32(4)

/// Wire size of a length-prefixed string/bytes property.
#[inline]
pub const fn prop_size_str(len: usize) -> usize {
    1 + 2 + len // id(1) + len_prefix(2) + data
}

/// Wire size of a variable-byte integer property (subscription identifier).
#[inline]
pub const fn prop_size_varint(v: u32) -> usize {
    1 + varint_size(v) // id(1) + varint
}

/// Size of a variable-byte integer encoding.
#[inline]
pub const fn varint_size(v: u32) -> usize {
    if v < 128 {
        1
    } else if v < 16384 {
        2
    } else if v < 2_097_152 {
        3
    } else {
        4
    }
}

// =============================================================================
// Protocol Version
// =============================================================================

/// MQTT protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    /// MQTT 3.1.1 (protocol level 4).
    V311,
    /// MQTT 5.0 (protocol level 5).
    V5,
}

impl ProtocolVersion {
    /// Wire protocol level byte.
    pub fn level(self) -> u8 {
        match self {
            Self::V311 => 4,
            Self::V5 => 5,
        }
    }

    /// Parse from wire protocol level byte.
    pub fn from_level(level: u8) -> Option<Self> {
        match level {
            4 => Some(Self::V311),
            5 => Some(Self::V5),
            _ => None,
        }
    }
}

// =============================================================================
// QoS
// =============================================================================

/// MQTT Quality of Service level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// At most once delivery.
    AtMostOnce = 0,
    /// At least once delivery.
    AtLeastOnce = 1,
    /// Exactly once delivery.
    ExactlyOnce = 2,
}

impl QoS {
    /// Parse from a 2-bit integer value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::AtMostOnce),
            1 => Some(Self::AtLeastOnce),
            2 => Some(Self::ExactlyOnce),
            _ => None,
        }
    }

    /// Convert to wire representation.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

// =============================================================================
// Packet Type (fixed header)
// =============================================================================

/// MQTT control packet type (4-bit value from fixed header byte 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5,
    PubRel = 6,
    PubComp = 7,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
    Auth = 15, // MQTT 5.0 only
}

impl PacketType {
    /// Parse from the upper 4 bits of the first fixed header byte.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Connect),
            2 => Some(Self::ConnAck),
            3 => Some(Self::Publish),
            4 => Some(Self::PubAck),
            5 => Some(Self::PubRec),
            6 => Some(Self::PubRel),
            7 => Some(Self::PubComp),
            8 => Some(Self::Subscribe),
            9 => Some(Self::SubAck),
            10 => Some(Self::Unsubscribe),
            11 => Some(Self::UnsubAck),
            12 => Some(Self::PingReq),
            13 => Some(Self::PingResp),
            14 => Some(Self::Disconnect),
            15 => Some(Self::Auth),
            _ => None,
        }
    }
}

// =============================================================================
// Connect Flags
// =============================================================================

/// Flags parsed from the CONNECT packet's Connect Flags byte.
#[derive(Debug, Clone)]
pub struct ConnectFlags {
    pub username: bool,
    pub password: bool,
    pub will_retain: bool,
    pub will_qos: QoS,
    pub will: bool,
    pub clean_session: bool,
}

impl ConnectFlags {
    /// Decode from the connect flags byte.
    pub fn from_byte(byte: u8) -> Option<Self> {
        // Bit 0 is reserved and must be 0.
        if byte & 0x01 != 0 {
            return None;
        }
        let will_qos = QoS::from_u8((byte >> 3) & 0x03)?;
        let will = byte & 0x04 != 0;
        let will_retain = byte & 0x20 != 0;

        // If will flag is false, will_qos must be 0 and will_retain must be false.
        if !will && (will_qos != QoS::AtMostOnce || will_retain) {
            return None;
        }

        Some(Self {
            username: byte & 0x80 != 0,
            password: byte & 0x40 != 0,
            will_retain,
            will_qos,
            will,
            clean_session: byte & 0x02 != 0,
        })
    }

    /// Encode to the connect flags byte.
    pub fn to_byte(&self) -> u8 {
        let mut byte = 0u8;
        if self.username {
            byte |= 0x80;
        }
        if self.password {
            byte |= 0x40;
        }
        if self.will_retain {
            byte |= 0x20;
        }
        byte |= (self.will_qos.as_u8() & 0x03) << 3;
        if self.will {
            byte |= 0x04;
        }
        if self.clean_session {
            byte |= 0x02;
        }
        byte
    }
}

// =============================================================================
// Connect Return Code (MQTT 3.1.1) / Reason Code (MQTT 5.0)
// =============================================================================

/// CONNACK return code for MQTT 3.1.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Accepted = 0x00,
    UnacceptableProtocolVersion = 0x01,
    IdentifierRejected = 0x02,
    ServerUnavailable = 0x03,
    BadUserNameOrPassword = 0x04,
    NotAuthorized = 0x05,
}

impl ConnectReturnCode {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Accepted),
            0x01 => Some(Self::UnacceptableProtocolVersion),
            0x02 => Some(Self::IdentifierRejected),
            0x03 => Some(Self::ServerUnavailable),
            0x04 => Some(Self::BadUserNameOrPassword),
            0x05 => Some(Self::NotAuthorized),
            _ => None,
        }
    }
}

/// MQTT 5.0 reason codes (subset used across various packet types).
///
/// Note: In MQTT 5.0, several reason codes share the same numeric value (0x00)
/// but are used in different packet contexts (CONNACK, SUBACK, DISCONNECT, etc.).
/// We represent them as constants rather than enum variants to avoid conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReasonCode(pub u8);

impl ReasonCode {
    pub const SUCCESS: Self = Self(0x00);
    pub const NORMAL_DISCONNECTION: Self = Self(0x00);
    pub const GRANTED_QOS0: Self = Self(0x00);
    pub const GRANTED_QOS1: Self = Self(0x01);
    pub const GRANTED_QOS2: Self = Self(0x02);
    pub const NO_MATCHING_SUBSCRIBERS: Self = Self(0x10);
    pub const NO_SUBSCRIPTION_EXISTED: Self = Self(0x11);
    pub const UNSPECIFIED_ERROR: Self = Self(0x80);
    pub const MALFORMED_PACKET: Self = Self(0x81);
    pub const PROTOCOL_ERROR: Self = Self(0x82);
    pub const IMPLEMENTATION_SPECIFIC_ERROR: Self = Self(0x83);
    pub const UNSUPPORTED_PROTOCOL_VERSION: Self = Self(0x84);
    pub const CLIENT_IDENTIFIER_NOT_VALID: Self = Self(0x85);
    pub const BAD_USER_NAME_OR_PASSWORD: Self = Self(0x86);
    pub const NOT_AUTHORIZED: Self = Self(0x87);
    pub const SERVER_UNAVAILABLE: Self = Self(0x88);
    pub const SERVER_BUSY: Self = Self(0x89);
    pub const BANNED: Self = Self(0x8A);
    pub const SERVER_SHUTTING_DOWN: Self = Self(0x8B);
    pub const BAD_AUTHENTICATION_METHOD: Self = Self(0x8C);
    pub const KEEP_ALIVE_TIMEOUT: Self = Self(0x8D);
    pub const SESSION_TAKEN_OVER: Self = Self(0x8E);
    pub const TOPIC_FILTER_INVALID: Self = Self(0x8F);
    pub const TOPIC_NAME_INVALID: Self = Self(0x90);
    pub const PACKET_IDENTIFIER_IN_USE: Self = Self(0x91);
    pub const PACKET_IDENTIFIER_NOT_FOUND: Self = Self(0x92);
    pub const RECEIVE_MAXIMUM_EXCEEDED: Self = Self(0x93);
    pub const TOPIC_ALIAS_INVALID: Self = Self(0x94);
    pub const PACKET_TOO_LARGE: Self = Self(0x95);
    pub const MESSAGE_RATE_TOO_HIGH: Self = Self(0x96);
    pub const QUOTA_EXCEEDED: Self = Self(0x97);
    pub const ADMINISTRATIVE_ACTION: Self = Self(0x98);
    pub const PAYLOAD_FORMAT_INVALID: Self = Self(0x99);
    pub const RETAIN_NOT_SUPPORTED: Self = Self(0x9A);
    pub const QOS_NOT_SUPPORTED: Self = Self(0x9B);
    pub const USE_ANOTHER_SERVER: Self = Self(0x9C);
    pub const SERVER_MOVED: Self = Self(0x9D);
    pub const SHARED_SUBSCRIPTIONS_NOT_SUPPORTED: Self = Self(0x9E);
    pub const DISCONNECT_WITH_WILL: Self = Self(0x04);
    pub const CONNECTION_RATE_EXCEEDED: Self = Self(0x9F);
    pub const MAXIMUM_CONNECT_TIME: Self = Self(0xA0);
    pub const SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED: Self = Self(0xA1);
    pub const WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED: Self = Self(0xA2);
}

// =============================================================================
// MQTT 5.0 Properties
// =============================================================================

/// MQTT 5.0 property identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PropertyId {
    PayloadFormatIndicator = 0x01,
    MessageExpiryInterval = 0x02,
    ContentType = 0x03,
    ResponseTopic = 0x08,
    CorrelationData = 0x09,
    SubscriptionIdentifier = 0x0B,
    SessionExpiryInterval = 0x11,
    AssignedClientIdentifier = 0x12,
    ServerKeepAlive = 0x13,
    AuthenticationMethod = 0x15,
    AuthenticationData = 0x16,
    RequestProblemInformation = 0x17,
    WillDelayInterval = 0x18,
    RequestResponseInformation = 0x19,
    ResponseInformation = 0x1A,
    ServerReference = 0x1C,
    ReasonString = 0x1F,
    ReceiveMaximum = 0x21,
    TopicAliasMaximum = 0x22,
    TopicAlias = 0x23,
    MaximumQoS = 0x24,
    RetainAvailable = 0x25,
    UserProperty = 0x26,
    MaximumPacketSize = 0x27,
    WildcardSubscriptionAvailable = 0x28,
    SubscriptionIdentifierAvailable = 0x29,
    SharedSubscriptionAvailable = 0x2A,
}

/// Zero-copy MQTT 5.0 properties, backed by raw wire-format bytes.
///
/// Property bytes are stored in MQTT wire format (tag-value pairs without the
/// variable-length integer prefix). Accessor methods scan the raw bytes on
/// demand, avoiding heap allocations during decode.
#[derive(Clone)]
pub struct Properties<'a> {
    /// Raw property bytes (content after the variable-length integer prefix).
    /// Empty for MQTT 3.1.1 or when no properties are present.
    raw: &'a [u8],
}

impl Default for Properties<'_> {
    fn default() -> Self {
        Self { raw: &[] }
    }
}

impl std::fmt::Debug for Properties<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Properties")
            .field("len", &self.raw.len())
            .finish()
    }
}

impl<'a> Properties<'a> {
    /// Create from raw property bytes (wire format, without varint prefix).
    pub fn from_raw(raw: &'a [u8]) -> Self {
        Self { raw }
    }

    /// Get the raw property bytes for re-encoding.
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// Whether no properties are present.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    // ---- Private scanning helpers ----

    /// Compute the size of a property value given its ID and the remaining data.
    /// Returns None if the data is truncated or the ID is unknown.
    #[inline]
    pub(crate) fn skip_value(id: u8, data: &[u8]) -> Option<usize> {
        match id {
            // u8 / bool
            0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => {
                if data.is_empty() {
                    return None;
                }
                Some(1)
            }
            // u16
            0x13 | 0x21 | 0x22 | 0x23 => {
                if data.len() < 2 {
                    return None;
                }
                Some(2)
            }
            // u32
            0x02 | 0x11 | 0x18 | 0x27 => {
                if data.len() < 4 {
                    return None;
                }
                Some(4)
            }
            // UTF-8 string or binary data (2-byte length prefix)
            0x03 | 0x08 | 0x09 | 0x12 | 0x15 | 0x16 | 0x1A | 0x1C | 0x1F => {
                if data.len() < 2 {
                    return None;
                }
                let len = u16::from_be_bytes([data[0], data[1]]) as usize;
                if data.len() < 2 + len {
                    return None;
                }
                Some(2 + len)
            }
            // Variable Byte Integer (subscription identifier)
            0x0B => {
                for i in 0..4.min(data.len()) {
                    if data[i] & 0x80 == 0 {
                        return Some(i + 1);
                    }
                }
                None
            }
            // User Property: two UTF-8 strings
            0x26 => {
                if data.len() < 2 {
                    return None;
                }
                let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                let off = 2 + key_len;
                if data.len() < off + 2 {
                    return None;
                }
                let val_len = u16::from_be_bytes([data[off], data[off + 1]]) as usize;
                let total = off + 2 + val_len;
                if data.len() < total {
                    return None;
                }
                Some(total)
            }
            _ => None,
        }
    }

    #[inline]
    fn find_u8_prop(&self, target: u8) -> Option<u8> {
        let data = self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let id = data[pos];
            pos += 1;
            if id == target {
                return data.get(pos).copied();
            }
            pos += Self::skip_value(id, &data[pos..])?;
        }
        None
    }

    #[inline]
    fn find_u16_prop(&self, target: u8) -> Option<u16> {
        let data = self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let id = data[pos];
            pos += 1;
            if id == target && data.len() >= pos + 2 {
                return Some(u16::from_be_bytes([data[pos], data[pos + 1]]));
            }
            pos += Self::skip_value(id, &data[pos..])?;
        }
        None
    }

    #[inline]
    fn find_u32_prop(&self, target: u8) -> Option<u32> {
        let data = self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let id = data[pos];
            pos += 1;
            if id == target && data.len() >= pos + 4 {
                return Some(u32::from_be_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                ]));
            }
            pos += Self::skip_value(id, &data[pos..])?;
        }
        None
    }

    #[inline]
    fn find_str_prop(&self, target: u8) -> Option<&'a str> {
        let data = self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let id = data[pos];
            pos += 1;
            if id == target && data.len() >= pos + 2 {
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                if data.len() >= pos + 2 + len {
                    // Safety: validated as UTF-8 during decode
                    return std::str::from_utf8(&data[pos + 2..pos + 2 + len]).ok();
                }
                return None;
            }
            pos += Self::skip_value(id, &data[pos..])?;
        }
        None
    }

    /// Find a string property and return it as a borrowed slice.
    #[inline]
    pub(crate) fn find_str_as_bytes(&self, target: u8) -> Option<&'a [u8]> {
        self.find_str_ref(target)
    }

    /// Find a string property and return it as a borrowed slice.
    #[inline]
    pub(crate) fn find_str_ref(&self, target: u8) -> Option<&'a [u8]> {
        let data = self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let id = data[pos];
            pos += 1;
            if id == target && data.len() >= pos + 2 {
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                if data.len() >= pos + 2 + len {
                    return Some(&data[pos + 2..pos + 2 + len]);
                }
                return None;
            }
            pos += Self::skip_value(id, &data[pos..])?;
        }
        None
    }

    /// Find a binary data property and return it as a zero-copy `Bytes` slice.
    #[inline]
    fn find_bytes_prop(&self, target: u8) -> Option<&'a [u8]> {
        let data = self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let id = data[pos];
            pos += 1;
            if id == target && data.len() >= pos + 2 {
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                if data.len() >= pos + 2 + len {
                    return Some(&data[pos + 2..pos + 2 + len]);
                }
                return None;
            }
            pos += Self::skip_value(id, &data[pos..])?;
        }
        None
    }

    /// Find a binary data property and return a borrowed slice.
    #[inline]
    fn find_bytes_as_bytes(&self, target: u8) -> Option<&'a [u8]> {
        self.find_bytes_prop(target)
    }

    #[inline]
    fn find_bool_prop(&self, target: u8) -> Option<bool> {
        self.find_u8_prop(target).map(|v| v != 0)
    }

    // ---- Public accessors ----

    pub fn payload_format_indicator(&self) -> Option<u8> {
        self.find_u8_prop(0x01)
    }

    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.find_u32_prop(0x02)
    }

    pub fn content_type(&self) -> Option<&'a str> {
        self.find_str_prop(0x03)
    }

    /// Borrowed slice of the content type string.
    pub fn content_type_bytes(&self) -> Option<&'a [u8]> {
        self.find_str_as_bytes(0x03)
    }

    pub fn response_topic(&self) -> Option<&'a str> {
        self.find_str_prop(0x08)
    }

    /// Borrowed slice of the response topic string.
    pub fn response_topic_bytes(&self) -> Option<&'a [u8]> {
        self.find_str_as_bytes(0x08)
    }

    pub fn correlation_data(&self) -> Option<&'a [u8]> {
        self.find_bytes_prop(0x09)
    }

    /// Borrowed slice of the correlation data.
    pub fn correlation_data_bytes(&self) -> Option<&'a [u8]> {
        self.find_bytes_as_bytes(0x09)
    }

    pub fn subscription_identifier(&self) -> Option<u32> {
        let data = self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let id = data[pos];
            pos += 1;
            if id == 0x0B {
                let mut val: u32 = 0;
                let mut mult: u32 = 1;
                for i in 0..4 {
                    if pos + i >= data.len() {
                        return None;
                    }
                    val += (data[pos + i] as u32 & 0x7F) * mult;
                    if data[pos + i] & 0x80 == 0 {
                        return Some(val);
                    }
                    mult *= 128;
                }
                return None;
            }
            pos += Self::skip_value(id, &data[pos..])?;
        }
        None
    }

    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.find_u32_prop(0x11)
    }

    pub fn assigned_client_identifier(&self) -> Option<&'a str> {
        self.find_str_prop(0x12)
    }

    pub fn server_keep_alive(&self) -> Option<u16> {
        self.find_u16_prop(0x13)
    }

    pub fn authentication_method(&self) -> Option<&'a str> {
        self.find_str_prop(0x15)
    }

    pub fn authentication_data(&self) -> Option<&'a [u8]> {
        self.find_bytes_prop(0x16)
    }

    /// Borrowed slice of the authentication data.
    pub fn authentication_data_bytes(&self) -> Option<&'a [u8]> {
        self.find_bytes_as_bytes(0x16)
    }

    pub fn request_problem_information(&self) -> Option<u8> {
        self.find_u8_prop(0x17)
    }

    pub fn will_delay_interval(&self) -> Option<u32> {
        self.find_u32_prop(0x18)
    }

    pub fn request_response_information(&self) -> Option<u8> {
        self.find_u8_prop(0x19)
    }

    pub fn response_information(&self) -> Option<&'a str> {
        self.find_str_prop(0x1A)
    }

    pub fn server_reference(&self) -> Option<&'a str> {
        self.find_str_prop(0x1C)
    }

    pub fn reason_string(&self) -> Option<&'a str> {
        self.find_str_prop(0x1F)
    }

    pub fn receive_maximum(&self) -> Option<u16> {
        self.find_u16_prop(0x21)
    }

    pub fn topic_alias_maximum(&self) -> Option<u16> {
        self.find_u16_prop(0x22)
    }

    pub fn topic_alias(&self) -> Option<u16> {
        self.find_u16_prop(0x23)
    }

    pub fn maximum_qos(&self) -> Option<u8> {
        self.find_u8_prop(0x24)
    }

    pub fn retain_available(&self) -> Option<bool> {
        self.find_bool_prop(0x25)
    }

    /// Iterate over user properties as `(&str, &str)` pairs.
    pub fn user_properties(&self) -> UserPropertiesIter<'a> {
        UserPropertiesIter {
            data: self.raw,
            pos: 0,
        }
    }

    pub fn maximum_packet_size(&self) -> Option<u32> {
        self.find_u32_prop(0x27)
    }

    pub fn wildcard_subscription_available(&self) -> Option<bool> {
        self.find_bool_prop(0x28)
    }

    pub fn subscription_identifier_available(&self) -> Option<bool> {
        self.find_bool_prop(0x29)
    }

    pub fn shared_subscription_available(&self) -> Option<bool> {
        self.find_bool_prop(0x2A)
    }
}

/// Iterator over MQTT 5.0 user properties (property ID 0x26).
pub struct UserPropertiesIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for UserPropertiesIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.data.len() {
            let id = self.data[self.pos];
            self.pos += 1;
            let vsize = Properties::skip_value(id, &self.data[self.pos..])?;
            if id == 0x26 {
                let d = &self.data[self.pos..];
                let key_len = u16::from_be_bytes([d[0], d[1]]) as usize;
                let key = std::str::from_utf8(&d[2..2 + key_len]).ok()?;
                let off = 2 + key_len;
                let val_len = u16::from_be_bytes([d[off], d[off + 1]]) as usize;
                let val = std::str::from_utf8(&d[off + 2..off + 2 + val_len]).ok()?;
                self.pos += vsize;
                return Some((key, val));
            }
            self.pos += vsize;
        }
        None
    }
}

/// Builder for MQTT 5.0 Properties (used in tests and for constructing Properties
/// for Publish and Connect packets).
pub struct PropertiesBuilder {
    buf: Vec<u8>,
}

impl PropertiesBuilder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn build(self) -> Vec<u8> {
        self.buf
    }

    fn put_u8(&mut self, id: u8, val: u8) -> &mut Self {
        self.buf.push(id);
        self.buf.push(val);
        self
    }

    fn put_u16(&mut self, id: u8, val: u16) -> &mut Self {
        self.buf.push(id);
        self.buf.extend_from_slice(&val.to_be_bytes());
        self
    }

    fn put_u32(&mut self, id: u8, val: u32) -> &mut Self {
        self.buf.push(id);
        self.buf.extend_from_slice(&val.to_be_bytes());
        self
    }

    fn put_str(&mut self, id: u8, val: &str) -> &mut Self {
        self.buf.push(id);
        self.buf
            .extend_from_slice(&(val.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(val.as_bytes());
        self
    }

    fn put_bytes(&mut self, id: u8, val: &[u8]) -> &mut Self {
        self.buf.push(id);
        self.buf
            .extend_from_slice(&(val.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(val);
        self
    }

    fn put_varint(&mut self, id: u8, mut val: u32) -> &mut Self {
        self.buf.push(id);
        loop {
            let mut byte = (val & 0x7F) as u8;
            val >>= 7;
            if val > 0 {
                byte |= 0x80;
            }
            self.buf.push(byte);
            if val == 0 {
                break;
            }
        }
        self
    }

    pub fn payload_format_indicator(mut self, v: u8) -> Self {
        self.put_u8(0x01, v);
        self
    }
    pub fn message_expiry_interval(mut self, v: u32) -> Self {
        self.put_u32(0x02, v);
        self
    }
    pub fn content_type(mut self, v: &str) -> Self {
        self.put_str(0x03, v);
        self
    }
    pub fn response_topic(mut self, v: &str) -> Self {
        self.put_str(0x08, v);
        self
    }
    pub fn correlation_data(mut self, v: &[u8]) -> Self {
        self.put_bytes(0x09, v);
        self
    }
    pub fn subscription_identifier(mut self, v: u32) -> Self {
        self.put_varint(0x0B, v);
        self
    }
    pub fn session_expiry_interval(mut self, v: u32) -> Self {
        self.put_u32(0x11, v);
        self
    }
    pub fn assigned_client_identifier(mut self, v: &str) -> Self {
        self.put_str(0x12, v);
        self
    }
    pub fn server_keep_alive(mut self, v: u16) -> Self {
        self.put_u16(0x13, v);
        self
    }
    pub fn authentication_method(mut self, v: &str) -> Self {
        self.put_str(0x15, v);
        self
    }
    pub fn authentication_data(mut self, v: &[u8]) -> Self {
        self.put_bytes(0x16, v);
        self
    }
    pub fn will_delay_interval(mut self, v: u32) -> Self {
        self.put_u32(0x18, v);
        self
    }
    pub fn reason_string(mut self, v: &str) -> Self {
        self.put_str(0x1F, v);
        self
    }
    pub fn receive_maximum(mut self, v: u16) -> Self {
        self.put_u16(0x21, v);
        self
    }
    pub fn topic_alias_maximum(mut self, v: u16) -> Self {
        self.put_u16(0x22, v);
        self
    }
    pub fn topic_alias(mut self, v: u16) -> Self {
        self.put_u16(0x23, v);
        self
    }
    pub fn maximum_qos(mut self, v: u8) -> Self {
        self.put_u8(0x24, v);
        self
    }
    pub fn retain_available(mut self, v: bool) -> Self {
        self.put_u8(0x25, v as u8);
        self
    }
    pub fn user_property(mut self, k: &str, v: &str) -> Self {
        self.buf.push(0x26);
        self.buf.extend_from_slice(&(k.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(k.as_bytes());
        self.buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(v.as_bytes());
        self
    }
    pub fn maximum_packet_size(mut self, v: u32) -> Self {
        self.put_u32(0x27, v);
        self
    }
    pub fn response_information(mut self, v: &str) -> Self {
        self.put_str(0x1A, v);
        self
    }
    pub fn server_reference(mut self, v: &str) -> Self {
        self.put_str(0x1C, v);
        self
    }
    pub fn wildcard_subscription_available(mut self, v: bool) -> Self {
        self.put_u8(0x28, v as u8);
        self
    }
    pub fn subscription_identifier_available(mut self, v: bool) -> Self {
        self.put_u8(0x29, v as u8);
        self
    }
    pub fn shared_subscription_available(mut self, v: bool) -> Self {
        self.put_u8(0x2A, v as u8);
        self
    }

    /// Wire size of the variable-int prefix + content.
    pub fn wire_size(&self) -> usize {
        let content = self.buf.len();
        varint_size(content as u32) + content
    }
}

// =============================================================================
// Will Message
// =============================================================================

/// Last Will and Testament message from CONNECT.
#[derive(Debug, Clone)]
pub struct WillMessage {
    pub topic: Bytes,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    pub properties_raw: Vec<u8>,
}

impl WillMessage {
    /// Get a borrowed Properties view over the raw property bytes.
    pub fn properties(&self) -> Properties<'_> {
        Properties::from_raw(&self.properties_raw)
    }
}

// =============================================================================
// Subscription
// =============================================================================

/// A single topic filter + options from a SUBSCRIBE packet.
#[derive(Debug, Clone)]
pub struct TopicFilter {
    pub filter: Bytes,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
}

impl TopicFilter {
    /// Returns the filter as a UTF-8 string slice (validated during decode).
    #[inline]
    pub fn filter_str(&self) -> &str {
        // SAFETY: validated as UTF-8 during decode.
        unsafe { std::str::from_utf8_unchecked(&self.filter) }
    }
}

// =============================================================================
// MQTT Packets
// =============================================================================

/// CONNECT packet payload.
#[derive(Debug, Clone)]
pub struct Connect {
    pub protocol_name: Bytes,
    pub protocol_version: ProtocolVersion,
    pub flags: ConnectFlags,
    pub keep_alive: u16,
    pub client_id: Bytes,
    pub will: Option<WillMessage>,
    pub username: Option<Bytes>,
    pub password: Option<Bytes>,
    pub properties_raw: Vec<u8>,
}

impl Connect {
    /// Get a borrowed Properties view over the raw property bytes.
    pub fn properties(&self) -> Properties<'_> {
        Properties::from_raw(&self.properties_raw)
    }

    /// Client ID as a UTF-8 string slice (validated during decode).
    #[inline]
    pub fn client_id_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.client_id) }
    }

    /// Username as a UTF-8 string slice (validated during decode).
    #[inline]
    pub fn username_str(&self) -> Option<&str> {
        self.username
            .as_ref()
            .map(|b| unsafe { std::str::from_utf8_unchecked(b) })
    }
}

/// CONNACK packet payload.
///
/// Properties are individual typed fields — encode writes them directly
/// into the output buffer with zero intermediate allocation.
#[derive(Debug, Clone, Default)]
pub struct ConnAck {
    pub session_present: bool,
    pub return_code: u8,
    // V5 properties
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<Bytes>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<Bytes>,
    pub response_information: Option<Bytes>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifier_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
    pub authentication_method: Option<Bytes>,
    pub authentication_data: Option<Bytes>,
    pub server_reference: Option<Bytes>,
}

impl ConnAck {
    /// Compute the wire size of the V5 properties section content.
    pub fn properties_size(&self) -> usize {
        let mut n = 0;
        if self.session_expiry_interval.is_some() {
            n += PROP_SIZE_U32;
        }
        if self.receive_maximum.is_some() {
            n += PROP_SIZE_U16;
        }
        if self.maximum_qos.is_some() {
            n += PROP_SIZE_U8;
        }
        if self.retain_available.is_some() {
            n += PROP_SIZE_U8;
        }
        if self.maximum_packet_size.is_some() {
            n += PROP_SIZE_U32;
        }
        if let Some(ref v) = self.assigned_client_identifier {
            n += prop_size_str(v.len());
        }
        if self.topic_alias_maximum.is_some() {
            n += PROP_SIZE_U16;
        }
        if let Some(ref v) = self.reason_string {
            n += prop_size_str(v.len());
        }
        if let Some(ref v) = self.response_information {
            n += prop_size_str(v.len());
        }
        if self.wildcard_subscription_available.is_some() {
            n += PROP_SIZE_U8;
        }
        if self.subscription_identifier_available.is_some() {
            n += PROP_SIZE_U8;
        }
        if self.shared_subscription_available.is_some() {
            n += PROP_SIZE_U8;
        }
        if self.server_keep_alive.is_some() {
            n += PROP_SIZE_U16;
        }
        if let Some(ref v) = self.authentication_method {
            n += prop_size_str(v.len());
        }
        if let Some(ref v) = self.authentication_data {
            n += prop_size_str(v.len());
        }
        if let Some(ref v) = self.server_reference {
            n += prop_size_str(v.len());
        }
        n
    }
}

/// PUBLISH packet payload.
///
/// The `'a` lifetime borrows topic and payload from the network buffer.
/// `handle_publish` consumes the Publish synchronously, so the borrow is valid.
#[derive(Debug, Clone)]
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a [u8],
    pub packet_id: Option<u16>,
    pub payload: &'a [u8],
    pub properties: Properties<'a>,
}

/// PUBACK packet (QoS 1 acknowledgment).
#[derive(Debug, Clone)]
pub struct PubAck<'a> {
    pub packet_id: u16,
    pub reason_code: Option<u8>,
    pub reason_string: Option<&'a [u8]>,
}

/// PUBREC packet (QoS 2, step 1 response).
#[derive(Debug, Clone)]
pub struct PubRec<'a> {
    pub packet_id: u16,
    pub reason_code: Option<u8>,
    pub reason_string: Option<&'a [u8]>,
}

/// PUBREL packet (QoS 2, step 2).
#[derive(Debug, Clone)]
pub struct PubRel<'a> {
    pub packet_id: u16,
    pub reason_code: Option<u8>,
    pub reason_string: Option<&'a [u8]>,
}

/// PUBCOMP packet (QoS 2, step 3 response).
#[derive(Debug, Clone)]
pub struct PubComp<'a> {
    pub packet_id: u16,
    pub reason_code: Option<u8>,
    pub reason_string: Option<&'a [u8]>,
}

/// SUBSCRIBE packet.
#[derive(Debug, Clone)]
pub struct Subscribe {
    pub packet_id: u16,
    pub filters: SmallVec<[TopicFilter; 4]>,
    /// Subscription Identifier from V5 properties (parsed eagerly).
    pub subscription_identifier: Option<u32>,
}

/// SUBACK packet.
#[derive(Debug, Clone)]
pub struct SubAck {
    pub packet_id: u16,
    pub return_codes: SmallVec<[u8; 8]>,
    pub reason_string: Option<Bytes>,
}

/// UNSUBSCRIBE packet.
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub filters: SmallVec<[Bytes; 4]>,
}

/// UNSUBACK packet.
#[derive(Debug, Clone)]
pub struct UnsubAck {
    pub packet_id: u16,
    pub reason_codes: SmallVec<[u8; 8]>,
    pub reason_string: Option<Bytes>,
}

/// DISCONNECT packet (MQTT 5.0 adds reason code + properties).
#[derive(Debug, Clone)]
pub struct Disconnect {
    pub reason_code: Option<u8>,
    // Inbound V5 property (parsed eagerly during decode)
    pub session_expiry_interval: Option<u32>,
    // Outbound V5 properties (written directly during encode)
    pub reason_string: Option<Bytes>,
    pub server_reference: Option<Bytes>,
}

/// AUTH packet (MQTT 5.0 only — Enhanced Authentication).
///
/// Used for multi-step SASL-like authentication flows (SCRAM, Kerberos, etc.).
/// Reason codes: 0x00 (Success), 0x18 (Continue Authentication), 0x19 (Re-authenticate).
#[derive(Debug, Clone)]
pub struct Auth {
    pub reason_code: u8,
    pub authentication_method: Option<Bytes>,
    pub authentication_data: Option<Bytes>,
    pub reason_string: Option<Bytes>,
}

impl Auth {
    /// Success — authentication complete.
    pub const SUCCESS: u8 = 0x00;
    /// Continue Authentication — server sends challenge, client sends response.
    pub const CONTINUE_AUTHENTICATION: u8 = 0x18;
    /// Re-authenticate — client initiates re-authentication on an existing connection.
    pub const RE_AUTHENTICATE: u8 = 0x19;
}

// =============================================================================
// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos_roundtrip() {
        for val in 0..=2u8 {
            let qos = QoS::from_u8(val).unwrap();
            assert_eq!(qos.as_u8(), val);
        }
        assert!(QoS::from_u8(3).is_none());
    }

    #[test]
    fn test_protocol_version_level() {
        assert_eq!(ProtocolVersion::V311.level(), 4);
        assert_eq!(ProtocolVersion::V5.level(), 5);
        assert_eq!(ProtocolVersion::from_level(4), Some(ProtocolVersion::V311));
        assert_eq!(ProtocolVersion::from_level(5), Some(ProtocolVersion::V5));
        assert_eq!(ProtocolVersion::from_level(3), None);
    }

    #[test]
    fn test_packet_type_roundtrip() {
        for val in 1..=15u8 {
            assert!(PacketType::from_u8(val).is_some());
        }
        assert!(PacketType::from_u8(0).is_none());
        assert!(PacketType::from_u8(16).is_none());
    }

    #[test]
    fn test_connect_flags_roundtrip() {
        let flags = ConnectFlags {
            username: true,
            password: true,
            will_retain: false,
            will_qos: QoS::AtLeastOnce,
            will: true,
            clean_session: true,
        };
        let byte = flags.to_byte();
        let decoded = ConnectFlags::from_byte(byte).unwrap();
        assert_eq!(decoded.username, true);
        assert_eq!(decoded.password, true);
        assert_eq!(decoded.will_retain, false);
        assert_eq!(decoded.will_qos, QoS::AtLeastOnce);
        assert_eq!(decoded.will, true);
        assert_eq!(decoded.clean_session, true);
    }

    #[test]
    fn test_connect_flags_reserved_bit() {
        // Bit 0 must be 0 per MQTT spec.
        assert!(ConnectFlags::from_byte(0x01).is_none());
        assert!(ConnectFlags::from_byte(0x03).is_none());
    }

    #[test]
    fn test_connect_flags_no_flags() {
        let flags = ConnectFlags::from_byte(0x00).unwrap();
        assert!(!flags.username);
        assert!(!flags.password);
        assert!(!flags.will_retain);
        assert_eq!(flags.will_qos, QoS::AtMostOnce);
        assert!(!flags.will);
        assert!(!flags.clean_session);
    }

    #[test]
    fn test_properties_default() {
        let props = Properties::default();
        assert!(props.is_empty());
        assert!(props.message_expiry_interval().is_none());
        assert!(props.topic_alias().is_none());
        assert_eq!(props.user_properties().count(), 0);
    }

    #[test]
    fn test_properties_scan_accessors() {
        // Build raw property bytes manually (wire format)
        let mut raw = Vec::new();
        // message_expiry_interval (0x02) = 300
        raw.push(0x02);
        raw.extend_from_slice(&300u32.to_be_bytes());
        // payload_format_indicator (0x01) = 1
        raw.push(0x01);
        raw.push(1);
        // topic_alias (0x23) = 42
        raw.push(0x23);
        raw.extend_from_slice(&42u16.to_be_bytes());

        let props = Properties::from_raw(&raw);
        assert!(!props.is_empty());
        assert_eq!(props.message_expiry_interval(), Some(300));
        assert_eq!(props.payload_format_indicator(), Some(1));
        assert_eq!(props.topic_alias(), Some(42));
        assert!(props.session_expiry_interval().is_none());
    }

    #[test]
    fn test_connack_properties_size() {
        let connack = ConnAck {
            session_present: false,
            return_code: 0,
            receive_maximum: Some(100),
            maximum_qos: Some(1),
            topic_alias_maximum: Some(50),
            ..Default::default()
        };
        // receive_maximum: 3 + maximum_qos: 2 + topic_alias_maximum: 3 = 8
        assert_eq!(connack.properties_size(), 8);
    }

    #[test]
    fn test_prop_size_constants() {
        assert_eq!(PROP_SIZE_U8, 2);
        assert_eq!(PROP_SIZE_U16, 3);
        assert_eq!(PROP_SIZE_U32, 5);
        assert_eq!(prop_size_str(5), 8); // id + 2-byte len + 5 data
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(varint_size(16384), 3);
    }

    // ---- Will flags validation (M15) ----

    #[test]
    fn test_will_false_with_qos_nonzero_rejected() {
        // will=false (bit 2 clear), will_qos=1 (bits 4:3 = 01), will_retain=false
        // Byte: 0b0000_1000 = 0x08 (will_qos=1 without will set)
        assert!(ConnectFlags::from_byte(0x08).is_none());
    }

    #[test]
    fn test_will_false_with_retain_rejected() {
        // will=false, will_qos=0, will_retain=true
        // Byte: 0b0010_0000 = 0x20 (will_retain without will set)
        assert!(ConnectFlags::from_byte(0x20).is_none());
    }

    #[test]
    fn test_will_false_with_qos_and_retain_rejected() {
        // will=false, will_qos=2, will_retain=true
        // Byte: 0b0011_0000 = 0x30
        assert!(ConnectFlags::from_byte(0x30).is_none());
    }

    #[test]
    fn test_will_true_with_qos_and_retain_accepted() {
        // will=true, will_qos=2, will_retain=true, clean_session=true
        // Byte: 0b0011_0110 = 0x36
        let flags = ConnectFlags::from_byte(0x36).unwrap();
        assert!(flags.will);
        assert_eq!(flags.will_qos, QoS::ExactlyOnce);
        assert!(flags.will_retain);
        assert!(flags.clean_session);
    }

    // ---- Additional coverage tests ----

    #[test]
    fn test_connect_return_code_all_variants() {
        assert_eq!(
            ConnectReturnCode::from_u8(0x00),
            Some(ConnectReturnCode::Accepted)
        );
        assert_eq!(
            ConnectReturnCode::from_u8(0x01),
            Some(ConnectReturnCode::UnacceptableProtocolVersion)
        );
        assert_eq!(
            ConnectReturnCode::from_u8(0x02),
            Some(ConnectReturnCode::IdentifierRejected)
        );
        assert_eq!(
            ConnectReturnCode::from_u8(0x03),
            Some(ConnectReturnCode::ServerUnavailable)
        );
        assert_eq!(
            ConnectReturnCode::from_u8(0x04),
            Some(ConnectReturnCode::BadUserNameOrPassword)
        );
        assert_eq!(
            ConnectReturnCode::from_u8(0x05),
            Some(ConnectReturnCode::NotAuthorized)
        );
        assert_eq!(ConnectReturnCode::from_u8(0x06), None);
        assert_eq!(ConnectReturnCode::from_u8(0xFF), None);
    }

    #[test]
    fn test_qos_from_u8_invalid() {
        assert_eq!(QoS::from_u8(3), None);
        assert_eq!(QoS::from_u8(4), None);
        assert_eq!(QoS::from_u8(255), None);
    }

    #[test]
    fn test_reason_code_constants() {
        assert_eq!(ReasonCode::SUCCESS.0, 0x00);
        assert_eq!(ReasonCode::NORMAL_DISCONNECTION.0, 0x00);
        assert_eq!(ReasonCode::GRANTED_QOS0.0, 0x00);
        assert_eq!(ReasonCode::GRANTED_QOS1.0, 0x01);
        assert_eq!(ReasonCode::GRANTED_QOS2.0, 0x02);
        assert_eq!(ReasonCode::NO_MATCHING_SUBSCRIBERS.0, 0x10);
        assert_eq!(ReasonCode::UNSPECIFIED_ERROR.0, 0x80);
        assert_eq!(ReasonCode::MALFORMED_PACKET.0, 0x81);
        assert_eq!(ReasonCode::PROTOCOL_ERROR.0, 0x82);
        assert_eq!(ReasonCode::BAD_USER_NAME_OR_PASSWORD.0, 0x86);
        assert_eq!(ReasonCode::NOT_AUTHORIZED.0, 0x87);
        assert_eq!(ReasonCode::TOPIC_FILTER_INVALID.0, 0x8F);
        assert_eq!(ReasonCode::TOPIC_NAME_INVALID.0, 0x90);
        assert_eq!(ReasonCode::PACKET_IDENTIFIER_IN_USE.0, 0x91);
        assert_eq!(ReasonCode::PACKET_IDENTIFIER_NOT_FOUND.0, 0x92);
        assert_eq!(ReasonCode::TOPIC_ALIAS_INVALID.0, 0x94);
        assert_eq!(ReasonCode::PACKET_TOO_LARGE.0, 0x95);
        assert_eq!(ReasonCode::PAYLOAD_FORMAT_INVALID.0, 0x99);
        assert_eq!(ReasonCode::BANNED.0, 0x8A);
        assert_eq!(ReasonCode::SERVER_SHUTTING_DOWN.0, 0x8B);
        assert_eq!(ReasonCode::BAD_AUTHENTICATION_METHOD.0, 0x8C);
        assert_eq!(ReasonCode::KEEP_ALIVE_TIMEOUT.0, 0x8D);
        assert_eq!(ReasonCode::SESSION_TAKEN_OVER.0, 0x8E);
        assert_eq!(ReasonCode::RECEIVE_MAXIMUM_EXCEEDED.0, 0x93);
        assert_eq!(ReasonCode::MESSAGE_RATE_TOO_HIGH.0, 0x96);
        assert_eq!(ReasonCode::ADMINISTRATIVE_ACTION.0, 0x98);
        assert_eq!(ReasonCode::USE_ANOTHER_SERVER.0, 0x9C);
        assert_eq!(ReasonCode::SERVER_MOVED.0, 0x9D);
        assert_eq!(ReasonCode::DISCONNECT_WITH_WILL.0, 0x04);
        assert_eq!(ReasonCode::CONNECTION_RATE_EXCEEDED.0, 0x9F);
        assert_eq!(ReasonCode::MAXIMUM_CONNECT_TIME.0, 0xA0);
    }

    #[test]
    fn test_protocol_version_from_level_invalid() {
        assert_eq!(ProtocolVersion::from_level(0), None);
        assert_eq!(ProtocolVersion::from_level(3), None);
        assert_eq!(ProtocolVersion::from_level(6), None);
    }

    #[test]
    fn test_packet_type_from_u8_boundaries() {
        assert_eq!(PacketType::from_u8(0), None);
        assert_eq!(PacketType::from_u8(1), Some(PacketType::Connect));
        assert_eq!(PacketType::from_u8(15), Some(PacketType::Auth));
        assert_eq!(PacketType::from_u8(16), None);
    }

    #[test]
    fn test_auth_constants() {
        assert_eq!(Auth::SUCCESS, 0x00);
        assert_eq!(Auth::CONTINUE_AUTHENTICATION, 0x18);
        assert_eq!(Auth::RE_AUTHENTICATE, 0x19);
    }

    #[test]
    fn test_connect_flags_with_username_password() {
        // username=true, password=true, clean_session=true
        let byte = 0xC2; // 1100_0010
        let flags = ConnectFlags::from_byte(byte).unwrap();
        assert!(flags.username);
        assert!(flags.password);
        assert!(flags.clean_session);
        assert!(!flags.will);
        assert_eq!(flags.to_byte(), byte);
    }

    #[test]
    fn test_connect_flags_invalid_qos3() {
        // will=true, will_qos=3 (invalid), Byte: 0b0001_1100 = 0x1C
        assert!(ConnectFlags::from_byte(0x1C).is_none());
    }
}
