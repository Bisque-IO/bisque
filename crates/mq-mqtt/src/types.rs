//! MQTT packet type definitions for MQTT 3.1.1 and 5.0.
//!
//! Provides a comprehensive type system for all MQTT control packets,
//! QoS levels, connect flags, and property types.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

// =============================================================================
// Protocol Version
// =============================================================================

/// MQTT protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    pub const USE_ANOTHER_SERVER: Self = Self(0x9D);
    pub const SHARED_SUBSCRIPTIONS_NOT_SUPPORTED: Self = Self(0x9E);
    pub const SERVER_MOVED: Self = Self(0x9E); // Same as SHARED_SUBSCRIPTIONS_NOT_SUPPORTED (context-dependent)
    pub const CONNECTION_RATE_EXCEEDED: Self = Self(0x9F);
    pub const MAXIMUM_CONNECT_TIME: Self = Self(0xA0);
    pub const SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED: Self = Self(0xA1);
    pub const WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED: Self = Self(0xA2);
}

// =============================================================================
// MQTT 5.0 Properties
// =============================================================================

/// MQTT 5.0 property identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Container for MQTT 5.0 properties attached to a packet.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Properties {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_format_indicator: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_expiry_interval: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_data: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_identifier: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_expiry_interval: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned_client_identifier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_keep_alive: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication_method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication_data: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_maximum: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_alias_maximum: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_alias: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_qos: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_available: Option<bool>,
    #[serde(default, skip_serializing_if = "SmallVec::is_empty")]
    pub user_properties: SmallVec<[(String, String); 4]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_packet_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wildcard_subscription_available: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_identifier_available: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_subscription_available: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_string: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub will_delay_interval: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_problem_information: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_response_information: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_information: Option<String>,
}

// =============================================================================
// Will Message
// =============================================================================

/// Last Will and Testament message from CONNECT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WillMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    /// MQTT 5.0 will properties.
    #[serde(default)]
    pub properties: Properties,
}

// =============================================================================
// Subscription
// =============================================================================

/// A single topic filter + options from a SUBSCRIBE packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFilter {
    pub filter: String,
    pub qos: QoS,
    /// MQTT 5.0: No Local option (don't receive own publishes).
    #[serde(default)]
    pub no_local: bool,
    /// MQTT 5.0: Retain As Published.
    #[serde(default)]
    pub retain_as_published: bool,
    /// MQTT 5.0: Retain handling (0=send on subscribe, 1=send if new, 2=don't send).
    #[serde(default)]
    pub retain_handling: u8,
}

// =============================================================================
// MQTT Packets
// =============================================================================

/// CONNECT packet payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connect {
    pub protocol_name: String,
    pub protocol_version: ProtocolVersion,
    pub flags: ConnectFlags,
    pub keep_alive: u16,
    pub client_id: String,
    #[serde(default)]
    pub will: Option<WillMessage>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<Bytes>,
    /// MQTT 5.0 properties.
    #[serde(default)]
    pub properties: Properties,
}

/// CONNACK packet payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnAck {
    pub session_present: bool,
    pub return_code: u8,
    /// MQTT 5.0 properties.
    #[serde(default)]
    pub properties: Properties,
}

/// PUBLISH packet payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    /// Topic name as raw bytes (validated UTF-8 on decode).
    /// Using `Bytes` instead of `String` enables zero-copy from the read buffer.
    pub topic: Bytes,
    /// Packet identifier (present for QoS 1 and 2).
    #[serde(default)]
    pub packet_id: Option<u16>,
    pub payload: Bytes,
    /// MQTT 5.0 properties.
    #[serde(default)]
    pub properties: Properties,
}

/// PUBACK packet (QoS 1 acknowledgment).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubAck {
    pub packet_id: u16,
    /// MQTT 5.0 reason code.
    #[serde(default)]
    pub reason_code: Option<u8>,
    #[serde(default)]
    pub properties: Properties,
}

/// PUBREC packet (QoS 2, step 1 response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubRec {
    pub packet_id: u16,
    #[serde(default)]
    pub reason_code: Option<u8>,
    #[serde(default)]
    pub properties: Properties,
}

/// PUBREL packet (QoS 2, step 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubRel {
    pub packet_id: u16,
    #[serde(default)]
    pub reason_code: Option<u8>,
    #[serde(default)]
    pub properties: Properties,
}

/// PUBCOMP packet (QoS 2, step 3 response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubComp {
    pub packet_id: u16,
    #[serde(default)]
    pub reason_code: Option<u8>,
    #[serde(default)]
    pub properties: Properties,
}

/// SUBSCRIBE packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscribe {
    pub packet_id: u16,
    pub filters: SmallVec<[TopicFilter; 4]>,
    #[serde(default)]
    pub properties: Properties,
}

/// SUBACK packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAck {
    pub packet_id: u16,
    /// Return codes / reason codes per subscription.
    pub return_codes: SmallVec<[u8; 8]>,
    #[serde(default)]
    pub properties: Properties,
}

/// UNSUBSCRIBE packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub filters: SmallVec<[String; 4]>,
    #[serde(default)]
    pub properties: Properties,
}

/// UNSUBACK packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubAck {
    pub packet_id: u16,
    /// MQTT 5.0 reason codes per filter.
    #[serde(default)]
    pub reason_codes: SmallVec<[u8; 8]>,
    #[serde(default)]
    pub properties: Properties,
}

/// DISCONNECT packet (MQTT 5.0 adds reason code + properties).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Disconnect {
    #[serde(default)]
    pub reason_code: Option<u8>,
    #[serde(default)]
    pub properties: Properties,
}

/// AUTH packet (MQTT 5.0 only — Enhanced Authentication).
///
/// Used for multi-step SASL-like authentication flows (SCRAM, Kerberos, etc.).
/// Reason codes: 0x00 (Success), 0x18 (Continue Authentication), 0x19 (Re-authenticate).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Auth {
    pub reason_code: u8,
    #[serde(default)]
    pub properties: Properties,
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
// Top-Level Packet Enum
// =============================================================================

/// A decoded MQTT control packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqttPacket {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    Auth(Auth),
}

impl MqttPacket {
    /// Return the packet type discriminant.
    pub fn packet_type(&self) -> PacketType {
        match self {
            Self::Connect(_) => PacketType::Connect,
            Self::ConnAck(_) => PacketType::ConnAck,
            Self::Publish(_) => PacketType::Publish,
            Self::PubAck(_) => PacketType::PubAck,
            Self::PubRec(_) => PacketType::PubRec,
            Self::PubRel(_) => PacketType::PubRel,
            Self::PubComp(_) => PacketType::PubComp,
            Self::Subscribe(_) => PacketType::Subscribe,
            Self::SubAck(_) => PacketType::SubAck,
            Self::Unsubscribe(_) => PacketType::Unsubscribe,
            Self::UnsubAck(_) => PacketType::UnsubAck,
            Self::PingReq => PacketType::PingReq,
            Self::PingResp => PacketType::PingResp,
            Self::Disconnect(_) => PacketType::Disconnect,
            Self::Auth(_) => PacketType::Auth,
        }
    }
}

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
    fn test_mqtt_packet_type_method() {
        let pkt = MqttPacket::PingReq;
        assert_eq!(pkt.packet_type(), PacketType::PingReq);

        let pkt = MqttPacket::Disconnect(Disconnect {
            reason_code: None,
            properties: Properties::default(),
        });
        assert_eq!(pkt.packet_type(), PacketType::Disconnect);
    }

    #[test]
    fn test_properties_default() {
        let props = Properties::default();
        assert!(props.message_expiry_interval.is_none());
        assert!(props.user_properties.is_empty());
        assert!(props.topic_alias.is_none());
    }

    #[test]
    fn test_will_message_serde() {
        let will = WillMessage {
            topic: "last/will".to_string(),
            payload: Bytes::from_static(b"goodbye"),
            qos: QoS::AtLeastOnce,
            retain: true,
            properties: Properties::default(),
        };
        let json = serde_json::to_string(&will).unwrap();
        let decoded: WillMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic, "last/will");
        assert_eq!(decoded.qos, QoS::AtLeastOnce);
        assert!(decoded.retain);
    }

    #[test]
    fn test_topic_filter_serde() {
        let filter = TopicFilter {
            filter: "sensor/+/data".to_string(),
            qos: QoS::ExactlyOnce,
            no_local: true,
            retain_as_published: false,
            retain_handling: 1,
        };
        let json = serde_json::to_string(&filter).unwrap();
        let decoded: TopicFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.filter, "sensor/+/data");
        assert_eq!(decoded.qos, QoS::ExactlyOnce);
        assert!(decoded.no_local);
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
        assert_eq!(ReasonCode::USE_ANOTHER_SERVER.0, 0x9D);
        assert_eq!(ReasonCode::CONNECTION_RATE_EXCEEDED.0, 0x9F);
        assert_eq!(ReasonCode::MAXIMUM_CONNECT_TIME.0, 0xA0);
    }

    #[test]
    fn test_mqtt_packet_type_all_variants() {
        use super::*;
        let packets: Vec<MqttPacket> = vec![
            MqttPacket::Connect(Connect {
                protocol_name: "MQTT".into(),
                protocol_version: ProtocolVersion::V311,
                flags: ConnectFlags::from_byte(0x02).unwrap(),
                keep_alive: 60,
                client_id: "c".into(),
                will: None,
                username: None,
                password: None,
                properties: Properties::default(),
            }),
            MqttPacket::ConnAck(ConnAck {
                session_present: false,
                return_code: 0,
                properties: Properties::default(),
            }),
            MqttPacket::Publish(Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: Bytes::new(),
                packet_id: None,
                payload: Bytes::new(),
                properties: Properties::default(),
            }),
            MqttPacket::PubAck(PubAck {
                packet_id: 1,
                reason_code: None,
                properties: Properties::default(),
            }),
            MqttPacket::PubRec(PubRec {
                packet_id: 1,
                reason_code: None,
                properties: Properties::default(),
            }),
            MqttPacket::PubRel(PubRel {
                packet_id: 1,
                reason_code: None,
                properties: Properties::default(),
            }),
            MqttPacket::PubComp(PubComp {
                packet_id: 1,
                reason_code: None,
                properties: Properties::default(),
            }),
            MqttPacket::Subscribe(Subscribe {
                packet_id: 1,
                filters: SmallVec::new(),
                properties: Properties::default(),
            }),
            MqttPacket::SubAck(SubAck {
                packet_id: 1,
                return_codes: SmallVec::new(),
                properties: Properties::default(),
            }),
            MqttPacket::Unsubscribe(Unsubscribe {
                packet_id: 1,
                filters: SmallVec::new(),
                properties: Properties::default(),
            }),
            MqttPacket::UnsubAck(UnsubAck {
                packet_id: 1,
                reason_codes: SmallVec::new(),
                properties: Properties::default(),
            }),
            MqttPacket::PingReq,
            MqttPacket::PingResp,
            MqttPacket::Disconnect(Disconnect {
                reason_code: None,
                properties: Properties::default(),
            }),
            MqttPacket::Auth(Auth {
                reason_code: 0,
                properties: Properties::default(),
            }),
        ];

        let expected_types = vec![
            PacketType::Connect,
            PacketType::ConnAck,
            PacketType::Publish,
            PacketType::PubAck,
            PacketType::PubRec,
            PacketType::PubRel,
            PacketType::PubComp,
            PacketType::Subscribe,
            PacketType::SubAck,
            PacketType::Unsubscribe,
            PacketType::UnsubAck,
            PacketType::PingReq,
            PacketType::PingResp,
            PacketType::Disconnect,
            PacketType::Auth,
        ];

        for (pkt, expected) in packets.iter().zip(expected_types.iter()) {
            assert_eq!(pkt.packet_type(), *expected);
        }
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
