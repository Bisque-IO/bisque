//! AMQP 1.0 type system, performatives, and error conditions.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use bytes::Bytes;
use smallvec::SmallVec;

// =============================================================================
// WireString — zero-copy UTF-8 string backed by Bytes
// =============================================================================

/// A zero-copy UTF-8 string backed by `Bytes`.
///
/// Cloning is O(1) (atomic refcount increment, no data copy).
/// Implements `Deref<Target=str>` for transparent string access.
#[derive(Debug, Clone, Eq)]
pub struct WireString(Bytes);

impl WireString {
    /// Create from a `Bytes` that has already been validated as UTF-8.
    ///
    /// # Safety (logical)
    /// Caller must ensure `b` contains valid UTF-8.
    #[inline]
    pub fn from_utf8_unchecked(b: Bytes) -> Self {
        Self(b)
    }

    /// Create from a `Bytes`, validating UTF-8.
    #[inline]
    pub fn from_utf8(b: Bytes) -> Result<Self, std::str::Utf8Error> {
        std::str::from_utf8(&b)?;
        Ok(Self(b))
    }

    /// Create from a static string.
    #[inline]
    pub fn from_static(s: &'static str) -> Self {
        Self(Bytes::from_static(s.as_bytes()))
    }

    /// Create from an owned String (takes ownership, no copy if already heap).
    #[inline]
    pub fn from_string(s: String) -> Self {
        Self(Bytes::from(s.into_bytes()))
    }

    /// Get the underlying Bytes.
    #[inline]
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }

    /// Convert to the underlying Bytes (zero-copy).
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for WireString {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        // SAFETY: We guarantee UTF-8 at construction time.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl PartialEq for WireString {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<str> for WireString {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq<&str> for WireString {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_ref() == *other
    }
}

impl Hash for WireString {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl AsRef<str> for WireString {
    #[inline]
    fn as_ref(&self) -> &str {
        self
    }
}

impl fmt::Display for WireString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self)
    }
}

impl From<&str> for WireString {
    #[inline]
    fn from(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for WireString {
    #[inline]
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl Default for WireString {
    #[inline]
    fn default() -> Self {
        Self(Bytes::new())
    }
}

// =============================================================================
// Protocol Constants
// =============================================================================

/// AMQP 1.0 protocol header: "AMQP" + protocol-id(0) + major(1) + minor(0) + revision(0)
pub const AMQP_HEADER: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 1, 0, 0];

/// SASL protocol header: "AMQP" + protocol-id(3) + major(1) + minor(0) + revision(0)
pub const SASL_HEADER: [u8; 8] = [b'A', b'M', b'Q', b'P', 3, 1, 0, 0];

/// Minimum frame size (8-byte header).
pub const MIN_FRAME_SIZE: u32 = 512;
/// Default max frame size.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 65536;
/// Default channel max.
pub const DEFAULT_CHANNEL_MAX: u16 = 255;
/// Default idle timeout in milliseconds (0 = disabled).
pub const DEFAULT_IDLE_TIMEOUT: u32 = 0;

/// Frame type: AMQP.
pub const FRAME_TYPE_AMQP: u8 = 0;
/// Frame type: SASL.
pub const FRAME_TYPE_SASL: u8 = 1;

// =============================================================================
// AMQP 1.0 Type System
// =============================================================================

/// AMQP 1.0 primitive and composite values.
#[derive(Debug, Clone)]
pub enum AmqpValue {
    Null,
    Boolean(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    /// Milliseconds since Unix epoch.
    Timestamp(i64),
    Uuid([u8; 16]),
    /// Zero-copy binary data.
    Binary(Bytes),
    /// Zero-copy UTF-8 string.
    String(WireString),
    /// Zero-copy UTF-8 symbol.
    Symbol(WireString),
    /// Described type: (descriptor, value).
    Described(Box<AmqpValue>, Box<AmqpValue>),
    /// Ordered list of values.
    List(Vec<AmqpValue>),
    /// Key-value map (keys are AmqpValue).
    Map(Vec<(AmqpValue, AmqpValue)>),
    /// Homogeneous array.
    Array(Vec<AmqpValue>),
}

impl AmqpValue {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) | Self::Symbol(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_wire_string(&self) -> Option<&WireString> {
        match self {
            Self::String(s) | Self::Symbol(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Self::Uint(v) => Some(*v),
            Self::Ubyte(v) => Some(*v as u32),
            Self::Ushort(v) => Some(*v as u32),
            Self::Ulong(v) => Some(*v as u32),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Ulong(v) => Some(*v),
            Self::Uint(v) => Some(*v as u64),
            Self::Ubyte(v) => Some(*v as u64),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Long(v) => Some(*v),
            Self::Int(v) => Some(*v as i64),
            Self::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_binary(&self) -> Option<&Bytes> {
        match self {
            Self::Binary(b) => Some(b),
            _ => None,
        }
    }

    #[inline]
    pub fn as_list(&self) -> Option<&[AmqpValue]> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    #[inline]
    pub fn as_map(&self) -> Option<&[(AmqpValue, AmqpValue)]> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Get a value from a map by string key.
    pub fn map_get(&self, key: &str) -> Option<&AmqpValue> {
        self.as_map().and_then(|m| {
            m.iter()
                .find(|(k, _)| k.as_str() == Some(key))
                .map(|(_, v)| v)
        })
    }
}

impl fmt::Display for AmqpValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Ubyte(v) => write!(f, "{v}"),
            Self::Ushort(v) => write!(f, "{v}"),
            Self::Uint(v) => write!(f, "{v}"),
            Self::Ulong(v) => write!(f, "{v}"),
            Self::Byte(v) => write!(f, "{v}"),
            Self::Short(v) => write!(f, "{v}"),
            Self::Int(v) => write!(f, "{v}"),
            Self::Long(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{v}"),
            Self::Timestamp(v) => write!(f, "timestamp({v})"),
            Self::Uuid(v) => write!(f, "uuid({v:02x?})"),
            Self::Binary(v) => write!(f, "binary({})", v.len()),
            Self::String(v) => write!(f, "\"{v}\""),
            Self::Symbol(v) => write!(f, ":{v}"),
            Self::Described(d, v) => write!(f, "described({d}, {v})"),
            Self::List(v) => write!(f, "list({})", v.len()),
            Self::Map(v) => write!(f, "map({})", v.len()),
            Self::Array(v) => write!(f, "array({})", v.len()),
        }
    }
}

// =============================================================================
// Performative Descriptors (numeric)
// =============================================================================

/// Performative descriptor codes (domain 0x00000000:0x00000053 etc.)
pub mod descriptor {
    // Connection performatives
    pub const OPEN: u64 = 0x0000_0000_0000_0010;
    pub const BEGIN: u64 = 0x0000_0000_0000_0011;
    pub const ATTACH: u64 = 0x0000_0000_0000_0012;
    pub const FLOW: u64 = 0x0000_0000_0000_0013;
    pub const TRANSFER: u64 = 0x0000_0000_0000_0014;
    pub const DISPOSITION: u64 = 0x0000_0000_0000_0015;
    pub const DETACH: u64 = 0x0000_0000_0000_0016;
    pub const END: u64 = 0x0000_0000_0000_0017;
    pub const CLOSE: u64 = 0x0000_0000_0000_0018;

    // SASL performatives
    pub const SASL_MECHANISMS: u64 = 0x0000_0000_0000_0040;
    pub const SASL_INIT: u64 = 0x0000_0000_0000_0041;
    pub const SASL_CHALLENGE: u64 = 0x0000_0000_0000_0042;
    pub const SASL_RESPONSE: u64 = 0x0000_0000_0000_0043;
    pub const SASL_OUTCOME: u64 = 0x0000_0000_0000_0044;

    // Delivery states
    pub const ACCEPTED: u64 = 0x0000_0000_0000_0024;
    pub const REJECTED: u64 = 0x0000_0000_0000_0025;
    pub const RELEASED: u64 = 0x0000_0000_0000_0026;
    pub const MODIFIED: u64 = 0x0000_0000_0000_0027;
    pub const RECEIVED: u64 = 0x0000_0000_0000_0023;

    // Message sections
    pub const HEADER: u64 = 0x0000_0000_0000_0070;
    pub const DELIVERY_ANNOTATIONS: u64 = 0x0000_0000_0000_0071;
    pub const MESSAGE_ANNOTATIONS: u64 = 0x0000_0000_0000_0072;
    pub const PROPERTIES: u64 = 0x0000_0000_0000_0073;
    pub const APPLICATION_PROPERTIES: u64 = 0x0000_0000_0000_0074;
    pub const DATA: u64 = 0x0000_0000_0000_0075;
    pub const AMQP_SEQUENCE: u64 = 0x0000_0000_0000_0076;
    pub const AMQP_VALUE: u64 = 0x0000_0000_0000_0077;
    pub const FOOTER: u64 = 0x0000_0000_0000_0078;

    // Terminus types
    pub const SOURCE: u64 = 0x0000_0000_0000_0028;
    pub const TARGET: u64 = 0x0000_0000_0000_0029;

    // Error
    pub const ERROR: u64 = 0x0000_0000_0000_001D;
}

// =============================================================================
// Performatives (parsed from described types)
// =============================================================================

/// AMQP 1.0 performatives — the "verbs" of the protocol.
#[derive(Debug, Clone)]
pub enum Performative {
    Open(Open),
    Begin(Begin),
    Attach(Attach),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
    Detach(Detach),
    End(End),
    Close(Close),
}

impl fmt::Display for Performative {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open(p) => write!(f, "Open(container={:?})", &*p.container_id),
            Self::Begin(p) => write!(f, "Begin(channel={:?})", p.remote_channel),
            Self::Attach(p) => write!(f, "Attach(name={}, role={:?})", &*p.name, p.role),
            Self::Flow(_) => write!(f, "Flow"),
            Self::Transfer(p) => write!(
                f,
                "Transfer(handle={}, delivery={})",
                p.handle,
                p.delivery_id.unwrap_or(0)
            ),
            Self::Disposition(p) => write!(f, "Disposition(role={:?}, first={})", p.role, p.first),
            Self::Detach(p) => write!(f, "Detach(handle={})", p.handle),
            Self::End(_) => write!(f, "End"),
            Self::Close(_) => write!(f, "Close"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Open {
    pub container_id: WireString,
    pub hostname: Option<WireString>,
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_timeout: Option<u32>,
    pub outgoing_locales: SmallVec<[WireString; 2]>,
    pub incoming_locales: SmallVec<[WireString; 2]>,
    pub offered_capabilities: SmallVec<[WireString; 4]>,
    pub desired_capabilities: SmallVec<[WireString; 4]>,
    pub properties: Option<AmqpValue>,
}

impl Default for Open {
    fn default() -> Self {
        Self {
            container_id: WireString::default(),
            hostname: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            channel_max: DEFAULT_CHANNEL_MAX,
            idle_timeout: None,
            outgoing_locales: SmallVec::new(),
            incoming_locales: SmallVec::new(),
            offered_capabilities: SmallVec::new(),
            desired_capabilities: SmallVec::new(),
            properties: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Begin {
    pub remote_channel: Option<u16>,
    pub next_outgoing_id: u32,
    pub incoming_window: u32,
    pub outgoing_window: u32,
    pub handle_max: u32,
    pub offered_capabilities: SmallVec<[WireString; 4]>,
    pub desired_capabilities: SmallVec<[WireString; 4]>,
    pub properties: Option<AmqpValue>,
}

impl Default for Begin {
    fn default() -> Self {
        Self {
            remote_channel: None,
            next_outgoing_id: 0,
            incoming_window: 2048,
            outgoing_window: 2048,
            handle_max: u32::MAX,
            offered_capabilities: SmallVec::new(),
            desired_capabilities: SmallVec::new(),
            properties: None,
        }
    }
}

/// Link role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Sender role (sends messages).
    Sender,
    /// Receiver role (receives messages).
    Receiver,
}

impl Role {
    #[inline]
    pub fn from_bool(v: bool) -> Self {
        if v { Self::Receiver } else { Self::Sender }
    }

    #[inline]
    pub fn as_bool(self) -> bool {
        match self {
            Self::Sender => false,
            Self::Receiver => true,
        }
    }
}

/// Sender settle mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SndSettleMode {
    Unsettled = 0,
    Settled = 1,
    Mixed = 2,
}

impl SndSettleMode {
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Unsettled,
            1 => Self::Settled,
            _ => Self::Mixed,
        }
    }
}

/// Receiver settle mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RcvSettleMode {
    First = 0,
    Second = 1,
}

impl RcvSettleMode {
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::First,
            _ => Self::Second,
        }
    }
}

/// Source terminus.
#[derive(Debug, Clone, Default)]
pub struct Source {
    pub address: Option<WireString>,
    pub durable: u32,
    pub expiry_policy: Option<WireString>,
    pub timeout: u32,
    pub dynamic: bool,
    pub distribution_mode: Option<WireString>,
    pub filter: Option<AmqpValue>,
    pub default_outcome: Option<AmqpValue>,
    pub outcomes: SmallVec<[WireString; 4]>,
    pub capabilities: SmallVec<[WireString; 4]>,
}

/// Target terminus.
#[derive(Debug, Clone, Default)]
pub struct Target {
    pub address: Option<WireString>,
    pub durable: u32,
    pub expiry_policy: Option<WireString>,
    pub timeout: u32,
    pub dynamic: bool,
    pub capabilities: SmallVec<[WireString; 4]>,
}

#[derive(Debug, Clone)]
pub struct Attach {
    pub name: WireString,
    pub handle: u32,
    pub role: Role,
    pub snd_settle_mode: SndSettleMode,
    pub rcv_settle_mode: RcvSettleMode,
    pub source: Option<Source>,
    pub target: Option<Target>,
    pub unsettled: Option<AmqpValue>,
    pub incomplete_unsettled: bool,
    pub initial_delivery_count: Option<u32>,
    pub max_message_size: Option<u64>,
    pub offered_capabilities: SmallVec<[WireString; 4]>,
    pub desired_capabilities: SmallVec<[WireString; 4]>,
    pub properties: Option<AmqpValue>,
}

impl Default for Attach {
    fn default() -> Self {
        Self {
            name: WireString::default(),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: SndSettleMode::Mixed,
            rcv_settle_mode: RcvSettleMode::First,
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: SmallVec::new(),
            desired_capabilities: SmallVec::new(),
            properties: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Flow {
    pub next_incoming_id: Option<u32>,
    pub incoming_window: u32,
    pub next_outgoing_id: u32,
    pub outgoing_window: u32,
    pub handle: Option<u32>,
    pub delivery_count: Option<u32>,
    pub link_credit: Option<u32>,
    pub available: Option<u32>,
    pub drain: bool,
    pub echo: bool,
    pub properties: Option<AmqpValue>,
}

#[derive(Debug, Clone)]
pub struct Transfer {
    pub handle: u32,
    pub delivery_id: Option<u32>,
    pub delivery_tag: Option<Bytes>,
    pub message_format: Option<u32>,
    pub settled: Option<bool>,
    pub more: bool,
    pub rcv_settle_mode: Option<RcvSettleMode>,
    pub state: Option<DeliveryState>,
    pub resume: bool,
    pub aborted: bool,
    pub batchable: bool,
    /// Message payload (after the performative in the frame).
    pub payload: Bytes,
}

impl Default for Transfer {
    fn default() -> Self {
        Self {
            handle: 0,
            delivery_id: None,
            delivery_tag: None,
            message_format: None,
            settled: None,
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
            payload: Bytes::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Disposition {
    pub role: Role,
    pub first: u32,
    pub last: Option<u32>,
    pub settled: bool,
    pub state: Option<DeliveryState>,
    pub batchable: bool,
}

#[derive(Debug, Clone)]
pub struct Detach {
    pub handle: u32,
    pub closed: bool,
    pub error: Option<AmqpError>,
}

#[derive(Debug, Clone, Default)]
pub struct End {
    pub error: Option<AmqpError>,
}

#[derive(Debug, Clone, Default)]
pub struct Close {
    pub error: Option<AmqpError>,
}

// =============================================================================
// Delivery States
// =============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryState {
    Accepted,
    Rejected {
        error: Option<AmqpError>,
    },
    Released,
    Modified {
        delivery_failed: bool,
        undeliverable_here: bool,
        message_annotations: Option<AmqpValue>,
    },
    Received {
        section_number: u32,
        section_offset: u64,
    },
}

// =============================================================================
// Error Conditions
// =============================================================================

/// AMQP 1.0 error condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpError {
    pub condition: WireString,
    pub description: Option<WireString>,
    pub info: Option<AmqpValue>,
}

impl AmqpError {
    pub fn new(condition: &str, description: &str) -> Self {
        Self {
            condition: WireString::from(condition),
            description: Some(WireString::from(description)),
            info: None,
        }
    }

    /// Create from static strings — zero-copy, no allocation.
    pub fn from_static(condition: &'static str, description: &'static str) -> Self {
        Self {
            condition: WireString::from_static(condition),
            description: Some(WireString::from_static(description)),
            info: None,
        }
    }
}

impl PartialEq for AmqpValue {
    fn eq(&self, _other: &Self) -> bool {
        // Used only for AmqpError comparison; info field is opaque
        true
    }
}

impl Eq for AmqpValue {}

/// Standard AMQP 1.0 error condition symbols.
pub mod condition {
    pub const INTERNAL_ERROR: &str = "amqp:internal-error";
    pub const NOT_FOUND: &str = "amqp:not-found";
    pub const UNAUTHORIZED_ACCESS: &str = "amqp:unauthorized-access";
    pub const DECODE_ERROR: &str = "amqp:decode-error";
    pub const RESOURCE_LIMIT_EXCEEDED: &str = "amqp:resource-limit-exceeded";
    pub const NOT_ALLOWED: &str = "amqp:not-allowed";
    pub const INVALID_FIELD: &str = "amqp:invalid-field";
    pub const NOT_IMPLEMENTED: &str = "amqp:not-implemented";
    pub const RESOURCE_LOCKED: &str = "amqp:resource-locked";
    pub const PRECONDITION_FAILED: &str = "amqp:precondition-failed";
    pub const RESOURCE_DELETED: &str = "amqp:resource-deleted";
    pub const CONNECTION_FORCED: &str = "amqp:connection:forced";
    pub const FRAMING_ERROR: &str = "amqp:connection:framing-error";
    pub const LINK_REDIRECT: &str = "amqp:link:redirect";
    pub const LINK_STOLEN: &str = "amqp:link:stolen";
    pub const LINK_DETACH_FORCED: &str = "amqp:link:detach-forced";
    pub const LINK_TRANSFER_LIMIT_EXCEEDED: &str = "amqp:link:transfer-limit-exceeded";
    pub const LINK_MESSAGE_SIZE_EXCEEDED: &str = "amqp:link:message-size-exceeded";
    pub const SESSION_WINDOW_VIOLATION: &str = "amqp:session:window-violation";
    pub const SESSION_ERRANT_LINK: &str = "amqp:session:errant-link";
    pub const SESSION_HANDLE_IN_USE: &str = "amqp:session:handle-in-use";
    pub const SESSION_UNATTACHED_HANDLE: &str = "amqp:session:unattached-handle";
}

// =============================================================================
// SASL
// =============================================================================

#[derive(Debug, Clone)]
pub enum SaslPerformative {
    Mechanisms(SaslMechanisms),
    Init(SaslInit),
    Outcome(SaslOutcome),
}

#[derive(Debug, Clone)]
pub struct SaslMechanisms {
    pub mechanisms: SmallVec<[WireString; 4]>,
}

#[derive(Debug, Clone)]
pub struct SaslInit {
    pub mechanism: WireString,
    pub initial_response: Option<Bytes>,
    pub hostname: Option<WireString>,
}

#[derive(Debug, Clone)]
pub struct SaslOutcome {
    pub code: SaslCode,
    pub additional_data: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SaslCode {
    Ok = 0,
    Auth = 1,
    Sys = 2,
    SysPerm = 3,
    SysTemp = 4,
}

impl SaslCode {
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Ok,
            1 => Self::Auth,
            2 => Self::Sys,
            3 => Self::SysPerm,
            _ => Self::SysTemp,
        }
    }
}

// =============================================================================
// AMQP 1.0 Frame
// =============================================================================

/// A parsed AMQP 1.0 frame.
#[derive(Debug, Clone)]
pub struct AmqpFrame {
    pub channel: u16,
    pub frame_type: u8,
    pub body: FrameBody,
}

#[derive(Debug, Clone)]
pub enum FrameBody {
    /// AMQP performative with optional trailing payload (for Transfer).
    Amqp(Performative),
    /// SASL performative.
    Sasl(SaslPerformative),
    /// Empty frame (heartbeat).
    Empty,
}

// =============================================================================
// Message Sections (parsed from described types in Transfer payload)
// =============================================================================

/// AMQP 1.0 message header section.
#[derive(Debug, Clone, Default)]
pub struct MessageHeader {
    pub durable: bool,
    pub priority: u8,
    pub ttl: Option<u32>,
    pub first_acquirer: bool,
    pub delivery_count: u32,
}

/// AMQP 1.0 message properties section.
#[derive(Debug, Clone, Default)]
pub struct MessageProperties {
    pub message_id: Option<AmqpValue>,
    pub user_id: Option<Bytes>,
    pub to: Option<WireString>,
    pub subject: Option<WireString>,
    pub reply_to: Option<WireString>,
    pub correlation_id: Option<AmqpValue>,
    pub content_type: Option<WireString>,
    pub content_encoding: Option<WireString>,
    pub absolute_expiry_time: Option<i64>,
    pub creation_time: Option<i64>,
    pub group_id: Option<WireString>,
    pub group_sequence: Option<u32>,
    pub reply_to_group_id: Option<WireString>,
}

/// A parsed AMQP 1.0 message (from Transfer payload).
#[derive(Debug, Clone, Default)]
pub struct AmqpMessage {
    pub header: Option<MessageHeader>,
    pub delivery_annotations: Option<AmqpValue>,
    pub message_annotations: Option<AmqpValue>,
    pub properties: Option<MessageProperties>,
    pub application_properties: Option<AmqpValue>,
    pub body: SmallVec<[Bytes; 1]>,
    pub footer: Option<AmqpValue>,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_string_basics() {
        let ws = WireString::from("hello");
        assert_eq!(&*ws, "hello");
        assert_eq!(ws.len(), 5);
        assert!(!ws.is_empty());
        assert_eq!(ws, "hello");

        let ws2 = ws.clone(); // O(1) clone
        assert_eq!(ws, ws2);

        let empty = WireString::default();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_wire_string_from_static() {
        let ws = WireString::from_static("static-str");
        assert_eq!(&*ws, "static-str");
    }

    #[test]
    fn test_wire_string_display() {
        let ws = WireString::from("display-test");
        assert_eq!(format!("{ws}"), "display-test");
    }

    #[test]
    fn test_amqp_value_display() {
        assert_eq!(AmqpValue::Null.to_string(), "null");
        assert_eq!(AmqpValue::Boolean(true).to_string(), "true");
        assert_eq!(
            AmqpValue::String(WireString::from("hello")).to_string(),
            "\"hello\""
        );
        assert_eq!(
            AmqpValue::Symbol(WireString::from("sym")).to_string(),
            ":sym"
        );
        assert_eq!(AmqpValue::Uint(42).to_string(), "42");
    }

    #[test]
    fn test_amqp_value_accessors() {
        assert_eq!(
            AmqpValue::String(WireString::from("test")).as_str(),
            Some("test")
        );
        assert_eq!(
            AmqpValue::Symbol(WireString::from("sym")).as_str(),
            Some("sym")
        );
        assert_eq!(AmqpValue::Uint(42).as_str(), None);

        assert_eq!(AmqpValue::Uint(42).as_u32(), Some(42));
        assert_eq!(AmqpValue::Ubyte(7).as_u32(), Some(7));
        assert_eq!(AmqpValue::Boolean(true).as_bool(), Some(true));
    }

    #[test]
    fn test_amqp_value_map_get() {
        let map = AmqpValue::Map(vec![
            (
                AmqpValue::String(WireString::from("key1")),
                AmqpValue::Uint(1),
            ),
            (
                AmqpValue::String(WireString::from("key2")),
                AmqpValue::Uint(2),
            ),
        ]);
        assert_eq!(map.map_get("key1").and_then(|v| v.as_u32()), Some(1));
        assert_eq!(map.map_get("key2").and_then(|v| v.as_u32()), Some(2));
        assert!(map.map_get("key3").is_none());
    }

    #[test]
    fn test_role_conversion() {
        assert_eq!(Role::from_bool(false), Role::Sender);
        assert_eq!(Role::from_bool(true), Role::Receiver);
        assert!(!Role::Sender.as_bool());
        assert!(Role::Receiver.as_bool());
    }

    #[test]
    fn test_sasl_code() {
        assert_eq!(SaslCode::from_u8(0), SaslCode::Ok);
        assert_eq!(SaslCode::from_u8(1), SaslCode::Auth);
        assert_eq!(SaslCode::from_u8(99), SaslCode::SysTemp);
    }

    #[test]
    fn test_settle_modes() {
        assert_eq!(SndSettleMode::from_u8(0), SndSettleMode::Unsettled);
        assert_eq!(SndSettleMode::from_u8(1), SndSettleMode::Settled);
        assert_eq!(SndSettleMode::from_u8(2), SndSettleMode::Mixed);

        assert_eq!(RcvSettleMode::from_u8(0), RcvSettleMode::First);
        assert_eq!(RcvSettleMode::from_u8(1), RcvSettleMode::Second);
    }

    #[test]
    fn test_protocol_headers() {
        assert_eq!(&AMQP_HEADER[..4], b"AMQP");
        assert_eq!(AMQP_HEADER[4], 0); // protocol-id
        assert_eq!(&SASL_HEADER[..4], b"AMQP");
        assert_eq!(SASL_HEADER[4], 3); // SASL protocol-id
    }

    #[test]
    fn test_amqp_error() {
        let err = AmqpError::new(condition::NOT_FOUND, "queue not found");
        assert_eq!(&*err.condition, "amqp:not-found");
        assert_eq!(err.description.as_deref(), Some("queue not found"));
    }

    #[test]
    fn test_performative_display() {
        let open = Performative::Open(Open {
            container_id: WireString::from("test"),
            ..Default::default()
        });
        assert!(open.to_string().contains("test"));

        let attach = Performative::Attach(Attach {
            name: WireString::from("link1"),
            role: Role::Sender,
            ..Default::default()
        });
        assert!(attach.to_string().contains("link1"));
    }

    #[test]
    fn test_delivery_state() {
        let state = DeliveryState::Accepted;
        assert_eq!(state, DeliveryState::Accepted);

        let state = DeliveryState::Rejected { error: None };
        assert!(matches!(state, DeliveryState::Rejected { .. }));
    }

    #[test]
    fn test_descriptor_constants() {
        assert_eq!(descriptor::OPEN, 0x10);
        assert_eq!(descriptor::CLOSE, 0x18);
        assert_eq!(descriptor::SASL_MECHANISMS, 0x40);
        assert_eq!(descriptor::HEADER, 0x70);
        assert_eq!(descriptor::DATA, 0x75);
    }
}
