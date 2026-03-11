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
    /// Transactional delivery state (descriptor 0x34).
    Transactional(TransactionalState),
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
    Challenge(SaslChallenge),
    Response(SaslResponse),
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

/// SASL challenge from server to client (descriptor 0x42).
#[derive(Debug, Clone)]
pub struct SaslChallenge {
    pub challenge: Bytes,
}

/// SASL response from client to server (descriptor 0x43).
#[derive(Debug, Clone)]
pub struct SaslResponse {
    pub response: Bytes,
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
#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub durable: bool,
    /// Default priority is 4 per AMQP 1.0 spec.
    pub priority: u8,
    pub ttl: Option<u32>,
    pub first_acquirer: bool,
    pub delivery_count: u32,
}

impl Default for MessageHeader {
    fn default() -> Self {
        Self {
            durable: false,
            priority: 4,
            ttl: None,
            first_acquirer: false,
            delivery_count: 0,
        }
    }
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
// Transaction Types (AMQP 1.0 Section 4)
// =============================================================================

/// Coordinator target type for transaction coordinator links (descriptor 0x30).
#[derive(Debug, Clone, Default)]
pub struct Coordinator {
    /// Capabilities required of the transaction coordinator.
    pub capabilities: SmallVec<[WireString; 4]>,
}

/// Declare performative — request to begin a transaction (descriptor 0x31).
#[derive(Debug, Clone, Default)]
pub struct TxnDeclare {
    /// Global transaction identifier (optional).
    pub global_id: Option<Bytes>,
}

/// Discharge performative — request to commit or rollback (descriptor 0x32).
#[derive(Debug, Clone)]
pub struct TxnDischarge {
    /// Transaction identifier.
    pub txn_id: Bytes,
    /// If true, rollback; if false, commit.
    pub fail: bool,
}

/// Declared outcome — response to a Declare (descriptor 0x33).
#[derive(Debug, Clone)]
pub struct TxnDeclared {
    /// Assigned transaction identifier.
    pub txn_id: Bytes,
}

/// Non-recursive outcome for use within transactional state.
/// Avoids `Box<DeliveryState>` since a transaction outcome is never itself transactional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
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
}

impl Outcome {
    /// Convert to the equivalent `DeliveryState`.
    pub fn to_delivery_state(&self) -> DeliveryState {
        match self {
            Outcome::Accepted => DeliveryState::Accepted,
            Outcome::Rejected { error } => DeliveryState::Rejected {
                error: error.clone(),
            },
            Outcome::Released => DeliveryState::Released,
            Outcome::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations,
            } => DeliveryState::Modified {
                delivery_failed: *delivery_failed,
                undeliverable_here: *undeliverable_here,
                message_annotations: message_annotations.clone(),
            },
        }
    }

    /// Try to convert from a `DeliveryState`.
    /// Returns `None` for `Received` and `Transactional` variants.
    pub fn from_delivery_state(state: &DeliveryState) -> Option<Self> {
        match state {
            DeliveryState::Accepted => Some(Outcome::Accepted),
            DeliveryState::Rejected { error } => Some(Outcome::Rejected {
                error: error.clone(),
            }),
            DeliveryState::Released => Some(Outcome::Released),
            DeliveryState::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations,
            } => Some(Outcome::Modified {
                delivery_failed: *delivery_failed,
                undeliverable_here: *undeliverable_here,
                message_annotations: message_annotations.clone(),
            }),
            _ => None,
        }
    }
}

/// Transactional state — delivery state within a transaction (descriptor 0x34).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionalState {
    /// Transaction identifier.
    pub txn_id: Bytes,
    /// Outcome to apply when the transaction is discharged.
    pub outcome: Option<Outcome>,
}

/// Descriptor codes for transaction types.
pub mod txn_descriptor {
    pub const COORDINATOR: u64 = 0x0000_0000_0000_0030;
    pub const DECLARE: u64 = 0x0000_0000_0000_0031;
    pub const DISCHARGE: u64 = 0x0000_0000_0000_0032;
    pub const DECLARED: u64 = 0x0000_0000_0000_0033;
    pub const TRANSACTIONAL_STATE: u64 = 0x0000_0000_0000_0034;
}

// =============================================================================
// SASL Authenticator Trait
// =============================================================================

/// Authenticated identity after successful SASL exchange.
#[derive(Debug, Clone)]
pub struct SaslIdentity {
    /// The authenticated user name.
    pub username: WireString,
    /// The authorization identity (may differ from authentication identity).
    pub authzid: Option<WireString>,
}

/// Result of a SASL authentication step.
#[derive(Debug)]
pub enum AuthStep {
    /// Authentication succeeded. Contains the authenticated identity.
    Success(SaslIdentity),
    /// Server needs to send a challenge to the client.
    Challenge(Bytes),
    /// Authentication failed with the given SASL code.
    Failure(SaslCode),
}

/// Opaque per-mechanism state for multi-round SASL exchanges.
pub type MechState = Vec<u8>;

/// Trait for pluggable SASL authentication backends.
///
/// Implementations must be `Send + Sync` for use across tokio tasks.
pub trait SaslAuthenticator: Send + Sync + 'static {
    /// Return the list of supported SASL mechanism names.
    fn mechanisms(&self) -> &[&str];

    /// Begin authentication for the given mechanism with optional initial response.
    /// Returns the first step result and any mechanism-specific state.
    fn start(&self, mechanism: &str, initial_response: Option<&[u8]>) -> (AuthStep, MechState);

    /// Continue authentication with a client response and mechanism state.
    fn step(&self, response: &[u8], state: &mut MechState) -> AuthStep;
}

/// Default authenticator that accepts PLAIN and ANONYMOUS without credential validation.
#[derive(Debug, Clone, Default)]
pub struct PermissiveAuthenticator;

impl SaslAuthenticator for PermissiveAuthenticator {
    fn mechanisms(&self) -> &[&str] {
        &["PLAIN", "ANONYMOUS"]
    }

    fn start(&self, mechanism: &str, initial_response: Option<&[u8]>) -> (AuthStep, MechState) {
        match mechanism {
            "PLAIN" => {
                if let Some(response) = initial_response {
                    let identity = parse_sasl_plain_identity(response);
                    (AuthStep::Success(identity), Vec::new())
                } else {
                    // Need initial response — send empty challenge
                    (AuthStep::Challenge(Bytes::new()), Vec::new())
                }
            }
            "ANONYMOUS" => {
                let identity = SaslIdentity {
                    username: WireString::from_static("anonymous"),
                    authzid: None,
                };
                (AuthStep::Success(identity), Vec::new())
            }
            _ => (AuthStep::Failure(SaslCode::Auth), Vec::new()),
        }
    }

    fn step(&self, response: &[u8], _state: &mut MechState) -> AuthStep {
        // For PLAIN: the response IS the initial-response
        let identity = parse_sasl_plain_identity(response);
        AuthStep::Success(identity)
    }
}

/// Parse SASL PLAIN response format: `[authzid]\0authcid\0password`
pub fn parse_sasl_plain_identity(response: &[u8]) -> SaslIdentity {
    let mut first_null = None;
    let mut second_null = None;
    for (i, &b) in response.iter().enumerate() {
        if b == 0 {
            if first_null.is_none() {
                first_null = Some(i);
            } else {
                second_null = Some(i);
                break;
            }
        }
    }
    if let (Some(n1), Some(n2)) = (first_null, second_null) {
        let authzid = if n1 > 0 {
            let bytes = Bytes::copy_from_slice(&response[..n1]);
            WireString::from_utf8(bytes).ok()
        } else {
            None
        };
        let username_bytes = Bytes::copy_from_slice(&response[n1 + 1..n2]);
        let username = WireString::from_utf8(username_bytes)
            .unwrap_or_else(|_| WireString::from_static("unknown"));
        SaslIdentity { username, authzid }
    } else if let Some(n1) = first_null {
        let username_bytes = Bytes::copy_from_slice(&response[..n1]);
        let username = WireString::from_utf8(username_bytes)
            .unwrap_or_else(|_| WireString::from_static("unknown"));
        SaslIdentity {
            username,
            authzid: None,
        }
    } else {
        SaslIdentity {
            username: WireString::from_static("unknown"),
            authzid: None,
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

    // =========================================================================
    // AmqpValue::Display for all variants
    // =========================================================================

    #[test]
    fn test_amqp_value_display_all_variants() {
        assert_eq!(
            AmqpValue::Timestamp(1234567890).to_string(),
            "timestamp(1234567890)"
        );
        assert_eq!(
            AmqpValue::Uuid([0u8; 16]).to_string(),
            "uuid([00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00])"
        );
        assert_eq!(
            AmqpValue::Described(Box::new(AmqpValue::Ulong(0x70)), Box::new(AmqpValue::Null))
                .to_string(),
            "described(112, null)"
        );
        assert_eq!(
            AmqpValue::List(vec![AmqpValue::Uint(1), AmqpValue::Uint(2)]).to_string(),
            "list(2)"
        );
        // Nested list
        assert_eq!(
            AmqpValue::List(vec![AmqpValue::List(vec![AmqpValue::Null])]).to_string(),
            "list(1)"
        );
        assert_eq!(
            AmqpValue::Map(vec![(AmqpValue::Uint(1), AmqpValue::Uint(2))]).to_string(),
            "map(1)"
        );
        assert_eq!(
            AmqpValue::Array(vec![
                AmqpValue::Uint(1),
                AmqpValue::Uint(2),
                AmqpValue::Uint(3)
            ])
            .to_string(),
            "array(3)"
        );
        assert_eq!(AmqpValue::Byte(-1).to_string(), "-1");
        assert_eq!(AmqpValue::Short(-100).to_string(), "-100");
        assert_eq!(AmqpValue::Float(1.5).to_string(), "1.5");
        assert_eq!(AmqpValue::Double(2.5).to_string(), "2.5");
        assert_eq!(AmqpValue::Ushort(65535).to_string(), "65535");
        assert_eq!(
            AmqpValue::Binary(Bytes::from_static(b"abc")).to_string(),
            "binary(3)"
        );
    }

    // =========================================================================
    // WireString edge cases
    // =========================================================================

    #[test]
    fn test_wire_string_from_utf8_error() {
        let invalid = Bytes::from_static(&[0xff, 0xfe]);
        let result = WireString::from_utf8(invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_wire_string_hash_identical() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = WireString::from("hello");
        let b = WireString::from_string("hello".to_string());

        let mut ha = DefaultHasher::new();
        a.hash(&mut ha);
        let mut hb = DefaultHasher::new();
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[test]
    fn test_wire_string_partial_eq_str_ref() {
        let ws = WireString::from("test");
        assert!(ws == "test");
        let s: &str = "test";
        assert!(ws == s);
        assert!(ws != "other");
    }

    #[test]
    fn test_wire_string_default_is_empty() {
        let ws = WireString::default();
        assert!(ws.is_empty());
        assert_eq!(ws.len(), 0);
        assert_eq!(&*ws, "");
    }

    // =========================================================================
    // AmqpValue accessors returning None for wrong types
    // =========================================================================

    #[test]
    fn test_amqp_value_as_i64_wrong_type() {
        assert_eq!(AmqpValue::Boolean(true).as_i64(), None);
        assert_eq!(AmqpValue::String(WireString::from("x")).as_i64(), None);
        assert_eq!(AmqpValue::Null.as_i64(), None);
    }

    #[test]
    fn test_amqp_value_as_u64_wrong_type() {
        assert_eq!(AmqpValue::Boolean(true).as_u64(), None);
        assert_eq!(AmqpValue::Long(-1).as_u64(), None);
        assert_eq!(AmqpValue::Null.as_u64(), None);
    }

    #[test]
    fn test_amqp_value_as_bool_wrong_type() {
        assert_eq!(AmqpValue::Uint(1).as_bool(), None);
        assert_eq!(AmqpValue::Null.as_bool(), None);
        assert_eq!(AmqpValue::String(WireString::from("true")).as_bool(), None);
    }

    #[test]
    fn test_amqp_value_as_list_wrong_type() {
        assert!(AmqpValue::Uint(1).as_list().is_none());
        assert!(AmqpValue::Null.as_list().is_none());
        assert!(AmqpValue::Map(vec![]).as_list().is_none());
    }

    #[test]
    fn test_amqp_value_as_binary_wrong_type() {
        assert!(AmqpValue::Uint(1).as_binary().is_none());
        assert!(AmqpValue::Null.as_binary().is_none());
        assert!(
            AmqpValue::String(WireString::from("x"))
                .as_binary()
                .is_none()
        );
    }

    #[test]
    fn test_amqp_value_as_map_wrong_type() {
        assert!(AmqpValue::Uint(1).as_map().is_none());
        assert!(AmqpValue::List(vec![]).as_map().is_none());
        assert!(AmqpValue::Null.as_map().is_none());
    }

    #[test]
    fn test_amqp_value_map_get_on_non_map() {
        assert!(AmqpValue::Uint(1).map_get("key").is_none());
        assert!(AmqpValue::List(vec![]).map_get("key").is_none());
        assert!(AmqpValue::Null.map_get("key").is_none());
    }

    #[test]
    fn test_amqp_value_is_null() {
        assert!(AmqpValue::Null.is_null());
        assert!(!AmqpValue::Uint(0).is_null());
        assert!(!AmqpValue::Boolean(false).is_null());
        assert!(!AmqpValue::String(WireString::from("")).is_null());
    }

    // =========================================================================
    // Outcome conversions
    // =========================================================================

    #[test]
    fn test_outcome_to_delivery_state_all_variants() {
        assert_eq!(
            Outcome::Accepted.to_delivery_state(),
            DeliveryState::Accepted
        );
        assert_eq!(
            Outcome::Released.to_delivery_state(),
            DeliveryState::Released
        );

        let rejected = Outcome::Rejected { error: None };
        match rejected.to_delivery_state() {
            DeliveryState::Rejected { error } => assert!(error.is_none()),
            _ => panic!("expected Rejected"),
        }

        let modified = Outcome::Modified {
            delivery_failed: true,
            undeliverable_here: true,
            message_annotations: None,
        };
        match modified.to_delivery_state() {
            DeliveryState::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations,
            } => {
                assert!(delivery_failed);
                assert!(undeliverable_here);
                assert!(message_annotations.is_none());
            }
            _ => panic!("expected Modified"),
        }
    }

    #[test]
    fn test_outcome_from_delivery_state_all() {
        assert_eq!(
            Outcome::from_delivery_state(&DeliveryState::Accepted),
            Some(Outcome::Accepted)
        );
        assert_eq!(
            Outcome::from_delivery_state(&DeliveryState::Released),
            Some(Outcome::Released)
        );
        assert_eq!(
            Outcome::from_delivery_state(&DeliveryState::Rejected { error: None }),
            Some(Outcome::Rejected { error: None })
        );
        assert_eq!(
            Outcome::from_delivery_state(&DeliveryState::Modified {
                delivery_failed: false,
                undeliverable_here: false,
                message_annotations: None,
            }),
            Some(Outcome::Modified {
                delivery_failed: false,
                undeliverable_here: false,
                message_annotations: None,
            })
        );
        // Received and Transactional return None
        assert_eq!(
            Outcome::from_delivery_state(&DeliveryState::Received {
                section_number: 0,
                section_offset: 0,
            }),
            None
        );
        assert_eq!(
            Outcome::from_delivery_state(&DeliveryState::Transactional(TransactionalState {
                txn_id: Bytes::from_static(b"txn"),
                outcome: None,
            })),
            None
        );
    }

    // =========================================================================
    // Default implementations
    // =========================================================================

    #[test]
    fn test_source_default() {
        let src = Source::default();
        assert!(src.address.is_none());
        assert_eq!(src.durable, 0);
        assert!(src.expiry_policy.is_none());
        assert_eq!(src.timeout, 0);
        assert!(!src.dynamic);
        assert!(src.distribution_mode.is_none());
        assert!(src.filter.is_none());
        assert!(src.default_outcome.is_none());
        assert!(src.outcomes.is_empty());
        assert!(src.capabilities.is_empty());
    }

    #[test]
    fn test_target_default() {
        let tgt = Target::default();
        assert!(tgt.address.is_none());
        assert_eq!(tgt.durable, 0);
        assert!(tgt.expiry_policy.is_none());
        assert_eq!(tgt.timeout, 0);
        assert!(!tgt.dynamic);
        assert!(tgt.capabilities.is_empty());
    }

    #[test]
    fn test_message_properties_default() {
        let props = MessageProperties::default();
        assert!(props.message_id.is_none());
        assert!(props.user_id.is_none());
        assert!(props.to.is_none());
        assert!(props.subject.is_none());
        assert!(props.reply_to.is_none());
        assert!(props.correlation_id.is_none());
        assert!(props.content_type.is_none());
        assert!(props.content_encoding.is_none());
        assert!(props.absolute_expiry_time.is_none());
        assert!(props.creation_time.is_none());
        assert!(props.group_id.is_none());
        assert!(props.group_sequence.is_none());
        assert!(props.reply_to_group_id.is_none());
    }

    #[test]
    fn test_message_header_default() {
        let hdr = MessageHeader::default();
        assert!(!hdr.durable);
        assert_eq!(hdr.priority, 4);
        assert!(hdr.ttl.is_none());
        assert!(!hdr.first_acquirer);
        assert_eq!(hdr.delivery_count, 0);
    }

    #[test]
    fn test_amqp_message_default() {
        let msg = AmqpMessage::default();
        assert!(msg.header.is_none());
        assert!(msg.delivery_annotations.is_none());
        assert!(msg.message_annotations.is_none());
        assert!(msg.properties.is_none());
        assert!(msg.application_properties.is_none());
        assert!(msg.body.is_empty());
        assert!(msg.footer.is_none());
    }

    #[test]
    fn test_open_default() {
        let open = Open::default();
        assert!(open.container_id.is_empty());
        assert!(open.hostname.is_none());
        assert_eq!(open.max_frame_size, DEFAULT_MAX_FRAME_SIZE);
        assert_eq!(open.channel_max, DEFAULT_CHANNEL_MAX);
        assert!(open.idle_timeout.is_none());
        assert!(open.outgoing_locales.is_empty());
        assert!(open.incoming_locales.is_empty());
        assert!(open.offered_capabilities.is_empty());
        assert!(open.desired_capabilities.is_empty());
        assert!(open.properties.is_none());
    }

    #[test]
    fn test_begin_default() {
        let begin = Begin::default();
        assert!(begin.remote_channel.is_none());
        assert_eq!(begin.next_outgoing_id, 0);
        assert_eq!(begin.incoming_window, 2048);
        assert_eq!(begin.outgoing_window, 2048);
        assert_eq!(begin.handle_max, u32::MAX);
        assert!(begin.offered_capabilities.is_empty());
        assert!(begin.desired_capabilities.is_empty());
        assert!(begin.properties.is_none());
    }

    #[test]
    fn test_attach_default() {
        let attach = Attach::default();
        assert!(attach.name.is_empty());
        assert_eq!(attach.handle, 0);
        assert_eq!(attach.role, Role::Sender);
        assert_eq!(attach.snd_settle_mode, SndSettleMode::Mixed);
        assert_eq!(attach.rcv_settle_mode, RcvSettleMode::First);
        assert!(attach.source.is_none());
        assert!(attach.target.is_none());
        assert!(attach.unsettled.is_none());
        assert!(!attach.incomplete_unsettled);
        assert!(attach.initial_delivery_count.is_none());
        assert!(attach.max_message_size.is_none());
        assert!(attach.offered_capabilities.is_empty());
        assert!(attach.desired_capabilities.is_empty());
        assert!(attach.properties.is_none());
    }

    #[test]
    fn test_flow_default() {
        let flow = Flow::default();
        assert!(flow.next_incoming_id.is_none());
        assert_eq!(flow.incoming_window, 0);
        assert_eq!(flow.next_outgoing_id, 0);
        assert_eq!(flow.outgoing_window, 0);
        assert!(flow.handle.is_none());
        assert!(flow.delivery_count.is_none());
        assert!(flow.link_credit.is_none());
        assert!(flow.available.is_none());
        assert!(!flow.drain);
        assert!(!flow.echo);
        assert!(flow.properties.is_none());
    }

    #[test]
    fn test_transfer_default() {
        let transfer = Transfer::default();
        assert_eq!(transfer.handle, 0);
        assert!(transfer.delivery_id.is_none());
        assert!(transfer.delivery_tag.is_none());
        assert!(transfer.message_format.is_none());
        assert!(transfer.settled.is_none());
        assert!(!transfer.more);
        assert!(transfer.rcv_settle_mode.is_none());
        assert!(transfer.state.is_none());
        assert!(!transfer.resume);
        assert!(!transfer.aborted);
        assert!(!transfer.batchable);
        assert!(transfer.payload.is_empty());
    }

    #[test]
    fn test_end_default() {
        let end = End::default();
        assert!(end.error.is_none());
    }

    #[test]
    fn test_close_default() {
        let close = Close::default();
        assert!(close.error.is_none());
    }

    #[test]
    fn test_disposition_no_default() {
        // Disposition has no Default (role, first are required), just verify construction
        let disp = Disposition {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: true,
            state: None,
            batchable: false,
        };
        assert_eq!(disp.role, Role::Receiver);
        assert_eq!(disp.first, 0);
    }

    // =========================================================================
    // Performative Display for all variants
    // =========================================================================

    #[test]
    fn test_performative_display_all_variants() {
        let open = Performative::Open(Open {
            container_id: WireString::from("container-1"),
            ..Default::default()
        });
        assert!(open.to_string().contains("container-1"));

        let begin = Performative::Begin(Begin {
            remote_channel: Some(5),
            ..Default::default()
        });
        let begin_str = begin.to_string();
        assert!(begin_str.contains("Begin"));
        assert!(begin_str.contains("5"));

        let attach = Performative::Attach(Attach {
            name: WireString::from("my-link"),
            role: Role::Receiver,
            ..Default::default()
        });
        let attach_str = attach.to_string();
        assert!(attach_str.contains("my-link"));
        assert!(attach_str.contains("Receiver"));

        let flow = Performative::Flow(Flow::default());
        assert_eq!(flow.to_string(), "Flow");

        let transfer = Performative::Transfer(Transfer {
            handle: 7,
            delivery_id: Some(42),
            ..Default::default()
        });
        let transfer_str = transfer.to_string();
        assert!(transfer_str.contains("7"));
        assert!(transfer_str.contains("42"));

        let disposition = Performative::Disposition(Disposition {
            role: Role::Sender,
            first: 10,
            last: None,
            settled: true,
            state: None,
            batchable: false,
        });
        let disp_str = disposition.to_string();
        assert!(disp_str.contains("Disposition"));
        assert!(disp_str.contains("Sender"));
        assert!(disp_str.contains("10"));

        let detach = Performative::Detach(Detach {
            handle: 3,
            closed: true,
            error: None,
        });
        let detach_str = detach.to_string();
        assert!(detach_str.contains("Detach"));
        assert!(detach_str.contains("3"));

        let end = Performative::End(End::default());
        assert_eq!(end.to_string(), "End");

        let close = Performative::Close(Close::default());
        assert_eq!(close.to_string(), "Close");
    }

    // =========================================================================
    // ConnectionError Display
    // =========================================================================

    #[test]
    fn test_connection_error_display_all_variants() {
        use crate::codec::CodecError;
        use crate::connection::ConnectionError;
        use crate::link::LinkError;

        let e = ConnectionError::ProtocolMismatch([b'H', b'T', b'T', b'P', b'/', b'1', b'.', b'1']);
        assert!(e.to_string().contains("protocol mismatch"));

        let e = ConnectionError::Codec(CodecError::UnexpectedEof);
        assert!(e.to_string().contains("codec error"));

        let e = ConnectionError::Link(LinkError::UnknownHandle(99));
        assert!(e.to_string().contains("link error"));

        let e = ConnectionError::UnexpectedPhase {
            expected: crate::connection::ConnectionPhase::AwaitingOpen,
            got: crate::connection::ConnectionPhase::Open,
        };
        assert!(e.to_string().contains("unexpected phase"));

        let e = ConnectionError::UnexpectedFrame("bad frame");
        assert!(e.to_string().contains("unexpected frame"));
        assert!(e.to_string().contains("bad frame"));

        let e = ConnectionError::NotOpen;
        assert_eq!(e.to_string(), "connection not open");

        let e = ConnectionError::AuthenticationFailed;
        assert_eq!(e.to_string(), "authentication failed");

        let e = ConnectionError::UnknownSession(7);
        assert!(e.to_string().contains("7"));

        let e = ConnectionError::SessionLimitExceeded {
            channel: 10,
            max: 5,
        };
        assert!(e.to_string().contains("10"));
        assert!(e.to_string().contains("5"));
    }

    // =========================================================================
    // SaslCode::from_u8 for all valid codes and invalid
    // =========================================================================

    #[test]
    fn test_sasl_code_from_u8_all() {
        assert_eq!(SaslCode::from_u8(0), SaslCode::Ok);
        assert_eq!(SaslCode::from_u8(1), SaslCode::Auth);
        assert_eq!(SaslCode::from_u8(2), SaslCode::Sys);
        assert_eq!(SaslCode::from_u8(3), SaslCode::SysPerm);
        assert_eq!(SaslCode::from_u8(4), SaslCode::SysTemp);
        // Invalid codes default to SysTemp
        assert_eq!(SaslCode::from_u8(5), SaslCode::SysTemp);
        assert_eq!(SaslCode::from_u8(255), SaslCode::SysTemp);
    }
}
