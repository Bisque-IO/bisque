//! MQTT session — per-connection state and MQTT-to-MqCommand translation.
//!
//! Each TCP connection gets an `MqttSession` that tracks:
//! - Client identity and protocol version
//! - Active subscriptions (topic filter -> bisque-mq entity mappings)
//! - Will message for unclean disconnect
//! - In-flight QoS 1/2 packet tracking (both inbound and outbound)
//! - Entity ID cache (exchange_name -> id, queue_name -> id, etc.)
//! - MQTT 5.0 topic alias mappings, flow control, session expiry

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bisque_mq::MqCommandBuffer;
use bisque_mq::flat::{self, FlatMessageBuilder};
use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use tracing::{debug, warn};

use crate::auth::{AuthProvider, AuthResult, AuthState};
use crate::codec::{
    encode_auth, encode_connack_v, encode_disconnect, encode_disconnect_reason, encode_ping_resp,
    encode_puback_raw, encode_pubcomp_v, encode_pubrec_raw, encode_pubrel_v, encode_suback_raw,
    encode_unsuback_v, validate_topic_filter, validate_topic_name,
};
use crate::types::{
    Auth, ConnAck, Connect, ConnectReturnCode, Properties, ProtocolVersion, PubComp, PubRel,
    Publish, QoS, UnsubAck, WillMessage,
};

// =============================================================================
// Constants
// =============================================================================

/// Single global MQTT exchange name. All MQTT publishes route through this
/// topic-type exchange, matching MQTT's single-namespace topic semantics.
pub(crate) const MQTT_EXCHANGE_NAME: &str = "mqtt/exchange";

/// Static header key constants to avoid per-message allocation.
static HDR_CONTENT_TYPE: Bytes = Bytes::from_static(b"mqtt.content_type");
static HDR_PAYLOAD_FORMAT: Bytes = Bytes::from_static(b"mqtt.payload_format");

// =============================================================================
// Session Configuration
// =============================================================================

/// Configuration for MQTT sessions.
#[derive(Debug, Clone)]
pub struct MqttSessionConfig {
    /// Maximum number of in-flight QoS 1/2 messages per session.
    pub max_inflight: usize,
    /// Maximum packet size the server will accept (bytes).
    pub max_packet_size: usize,
    /// Default keep-alive in seconds (used if client sends 0).
    pub default_keep_alive: u16,
    /// Prefix for bisque-mq topic names created from MQTT topics.
    /// Arc<str> avoids per-connection String cloning.
    pub topic_prefix: Arc<str>,
    /// Prefix for retained message topics.
    /// Arc<str> avoids per-connection String cloning.
    pub retained_prefix: Arc<str>,
    /// Maximum QoS the server supports (0, 1, or 2). Subscriptions requesting
    /// a higher QoS will be downgraded and the granted QoS returned in SUBACK.
    pub maximum_qos: QoS,
    /// Maximum PUBLISH messages per second per client (0 = unlimited).
    /// Exceeding this rate causes DISCONNECT with 0x96 (Message rate too high).
    pub max_publish_rate: u32,
    /// Maximum cumulative PUBLISH quota per client (0 = unlimited).
    /// When exhausted, further PUBLISHes receive 0x97 (Quota exceeded).
    pub max_publish_quota: u64,
    /// Skip MQTT topic name validation on inbound PUBLISH.
    /// Trades correctness for throughput — only use in trusted environments.
    pub skip_topic_validation: bool,
}

impl Default for MqttSessionConfig {
    fn default() -> Self {
        Self {
            max_inflight: 65535,
            max_packet_size: 1024 * 1024,
            default_keep_alive: 60,
            topic_prefix: Arc::from("mqtt/"),
            retained_prefix: Arc::from("$mqtt/retained/"),
            maximum_qos: QoS::ExactlyOnce,
            max_publish_rate: 0,
            max_publish_quota: 0,
            skip_topic_validation: false,
        }
    }
}

// =============================================================================
// Entity Cache
// =============================================================================

/// Caches bisque-mq entity IDs resolved from command responses, so we don't
/// re-create entities on every PUBLISH/SUBSCRIBE.
#[derive(Debug, Default)]
struct EntityCache {
    /// "mqtt/exchange" or other exchange names -> exchange_id
    exchanges: HashMap<String, u64>,
    /// queue name -> queue_id
    queues: HashMap<String, u64>,
    /// Two-level map: exchange_id -> (routing_key -> binding_id).
    /// O(1) lookup without allocating a `(u64, String)` key tuple.
    bindings: HashMap<u64, HashMap<String, u64>>,
    /// topic name -> topic_id
    topics: HashMap<String, u64>,
}

/// Append `s` to `buf`, replacing '/' with '.' at the byte level.
/// Both are single-byte ASCII, so UTF-8 validity is preserved.
#[inline]
fn push_dotted(buf: &mut String, s: &str) {
    let start = buf.len();
    buf.push_str(s);
    // Safety: we only replace b'/' (0x2F) with b'.' (0x2E), both ASCII.
    // The String's UTF-8 validity is preserved.
    unsafe {
        for b in &mut buf.as_mut_vec()[start..] {
            if *b == b'/' {
                *b = b'.';
            }
        }
    }
}

/// Insert into a `HashMap<String, u64>` without allocating when the key
/// already exists. Accepts `String` (zero-alloc move) or `&str`.
///
/// On cache hit (key exists): updates value in-place, zero allocation.
/// On cache miss: `key.into()` allocates for `&str`, zero-cost move for `String`.
#[inline]
fn cache_insert(map: &mut HashMap<String, u64>, key: impl AsRef<str> + Into<String>, value: u64) {
    if let Some(v) = map.get_mut(key.as_ref()) {
        *v = value;
    } else {
        map.insert(key.into(), value);
    }
}

// =============================================================================
// In-flight Message Tracking
// =============================================================================

/// State of a QoS 1 message in flight.
#[derive(Debug, Clone)]
struct QoS1InFlight {
    /// The bisque-mq message_id returned from Deliver, used for ACK.
    mq_message_id: Option<u64>,
    /// The bisque-mq queue_id this was delivered from.
    mq_queue_id: Option<u64>,
    /// Direction of the message.
    direction: Direction,
}

/// Direction of an in-flight message.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Direction {
    /// Client -> Server (client published, we enqueued).
    Inbound,
    /// Server -> Client (we delivered from queue to client).
    Outbound,
}

/// State of a QoS 2 inbound message (client -> server publish).
#[derive(Debug, Clone)]
enum QoS2InboundState {
    /// PUBLISH received, PUBREC sent, waiting for PUBREL.
    PubRecSent,
    /// PUBREL received, PUBCOMP sent, transaction complete.
    Complete,
}

/// State of a QoS 2 outbound message (server -> client delivery).
#[derive(Debug, Clone)]
enum QoS2OutboundState {
    /// PUBLISH sent, waiting for PUBREC from client.
    PublishSent {
        mq_queue_id: u64,
        mq_message_id: u64,
    },
    /// PUBREC received, PUBREL sent, waiting for PUBCOMP.
    PubRelSent {
        mq_queue_id: u64,
        mq_message_id: u64,
    },
}

// =============================================================================
// Subscription Mapping
// =============================================================================

/// Tracks a single MQTT subscription's mapping to bisque-mq entities.
#[derive(Debug, Clone)]
pub struct SubscriptionMapping {
    /// The original MQTT topic filter string.
    pub filter: String,
    /// The maximum QoS granted for this subscription.
    pub max_qos: QoS,
    /// The bisque-mq exchange ID used for routing.
    pub exchange_id: Option<u64>,
    /// The bisque-mq binding ID within the exchange.
    pub binding_id: Option<u64>,
    /// The bisque-mq queue ID for QoS 1/2 delivery.
    pub queue_id: Option<u64>,
    /// The bisque-mq topic ID for QoS 0 delivery.
    pub topic_id: Option<u64>,
    /// The shared subscription group name, if any.
    pub shared_group: Option<String>,
    /// MQTT 5.0 subscription identifier.
    pub subscription_id: Option<u32>,
    /// MQTT 5.0: No Local option — don't deliver messages published by this client.
    pub no_local: bool,
    /// MQTT 5.0: Retain As Published — preserve the original retain flag.
    pub retain_as_published: bool,
    /// MQTT 5.0: Retain Handling (0=send on subscribe, 1=only if new, 2=never).
    pub retain_handling: u8,
    /// Pre-computed: true if filter starts with '+' or '#' (GAP-8 §4.7.2).
    /// Cached here to avoid per-delivery-cycle string inspection.
    pub filter_starts_with_wildcard: bool,
}

/// Per-subscription delivery metadata, cached in session for O(1) lookup.
#[derive(Debug, Clone, Copy)]
pub struct SubDeliveryInfo {
    /// The bisque-mq queue ID for this subscription.
    pub queue_id: u64,
    /// Maximum QoS granted for this subscription.
    pub max_qos: QoS,
    /// MQTT 5.0: No Local option.
    pub no_local: bool,
    /// MQTT 5.0: Retain As Published option.
    pub retain_as_published: bool,
    /// GAP-8: True if the subscription filter starts with a wildcard (+ or #).
    /// Topics starting with $ must not be delivered to such subscriptions.
    pub filter_starts_with_wildcard: bool,
    /// True if this is a shared subscription ($share/group/...).
    /// MQTT 5.0 §4.8.2: Retained messages must not be sent for shared subscriptions.
    pub is_shared: bool,
}

// =============================================================================
// Structured Plans (returned to server for async orchestration)
// =============================================================================

/// Plan for publishing a message. The server resolves entity IDs and submits
/// commands in order.
///
/// ACK responses (PUBACK/PUBREC) and error DISCONNECTs are written directly
/// to `session.out_buf` during `handle_publish` — no MqttPacket intermediaries.
#[derive(Debug)]
pub struct PublishPlan {
    /// The exchange name to route through.
    pub exchange_name: &'static str,
    /// Cached exchange_id if already known.
    pub cached_exchange_id: Option<u64>,
    /// Start offset of the flat message in the shared `msg_buf`.
    /// Used together with `msg_len` to extract the message slice.
    /// When `msg_len == 0`, the publish is skipped (error/skip sentinel).
    pub msg_start: usize,
    /// Length of the flat message in the shared `msg_buf`.
    pub msg_len: usize,
    /// Owned flat message bytes for will messages and other cases that need
    /// owned storage (e.g., delayed will publish after session is gone).
    /// When `Some`, this takes precedence over `msg_start`/`msg_len`.
    pub flat_message: Option<Bytes>,
    /// Optional retained message plan.
    pub retained: Option<RetainedPlan>,
    /// If true, the server must disconnect the client after flushing out_buf.
    /// The DISCONNECT packet is already encoded in out_buf.
    pub need_disconnect: bool,
}

impl PublishPlan {
    /// Returns true if this plan has no message to publish (skip/error sentinel).
    #[inline]
    pub fn has_message(&self) -> bool {
        self.flat_message.is_some() || self.msg_len > 0
    }
}

/// Plan for storing a retained message.
#[derive(Debug)]
pub struct RetainedPlan {
    /// bisque-mq topic name for the retained message.
    pub topic_name: String,
    /// Cached topic_id if already known.
    pub cached_topic_id: Option<u64>,
    /// The flat message bytes. None means clear the retained message.
    pub flat_message: Option<Bytes>,
}

/// Plan for subscribing to a topic filter.
///
/// The SUBACK response is written directly to `session.out_buf` during
/// `handle_subscribe` — no MqttPacket intermediary.
#[derive(Debug)]
pub struct SubscribePlan {
    /// The exchange name (global MQTT exchange).
    pub exchange_name: &'static str,
    /// Cached exchange_id if already known.
    pub cached_exchange_id: Option<u64>,
    /// Per-filter subscription details.
    pub filters: SmallVec<[SubscribeFilterPlan; 4]>,
}

/// Per-filter subscription plan.
#[derive(Debug)]
pub struct SubscribeFilterPlan {
    /// Original MQTT filter string.
    pub filter: String,
    /// bisque-mq queue name for this subscription.
    pub queue_name: String,
    /// Cached queue_id if known.
    pub cached_queue_id: Option<u64>,
    /// Routing key for the binding (MQTT wildcards translated to bisque-mq).
    pub routing_key: String,
    /// Cached binding key (exchange_id, routing_key).
    pub cached_binding_id: Option<u64>,
    /// QoS for this subscription.
    pub qos: QoS,
    /// Whether this is a shared subscription.
    pub shared_group: Option<String>,
    /// MQTT 5.0 No Local option.
    pub no_local: bool,
    /// MQTT 5.0 subscription identifier.
    pub subscription_id: Option<u32>,
    /// MQTT 5.0 Retain Handling (0=send on subscribe, 1=only if new, 2=never).
    pub retain_handling: u8,
    /// Whether this filter already existed before this SUBSCRIBE.
    pub is_new_subscription: bool,
}

// =============================================================================
// MqttSession
// =============================================================================

/// Global session ID counter.
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Per-connection MQTT session state.
pub struct MqttSession {
    /// Unique session ID (used as bisque-mq consumer_id and producer_id).
    pub session_id: u64,
    /// MQTT client identifier.
    pub client_id: String,
    /// Negotiated protocol version.
    pub protocol_version: ProtocolVersion,
    /// Whether this is a clean session.
    pub clean_session: bool,
    /// Keep-alive interval in seconds.
    pub keep_alive: u16,
    /// Will message to publish on unclean disconnect.
    pub will: Option<WillMessage>,
    /// Session configuration.
    pub config: MqttSessionConfig,
    /// Whether the session has been connected (CONNECT received + CONNACK sent).
    pub connected: bool,
    /// GAP-4: Whether this session was resumed from a persisted session.
    /// When true, the first outbound deliveries should set DUP=1 for QoS 1/2.
    /// Cleared after the first delivery cycle.
    pub session_resumed: bool,

    // -- Entity cache --
    entity_cache: EntityCache,

    // -- Subscription state --
    /// topic_filter -> SubscriptionMapping
    subscriptions: HashMap<String, SubscriptionMapping>,

    // -- In-flight QoS tracking --
    /// QoS 1: packet_id -> in-flight state
    qos1_inflight: HashMap<u16, QoS1InFlight>,
    /// QoS 2 inbound (client -> server): packet_id -> state
    qos2_inbound: HashMap<u16, QoS2InboundState>,
    /// QoS 2 outbound (server -> client): packet_id -> state
    qos2_outbound: HashMap<u16, QoS2OutboundState>,

    // -- Packet ID generation --
    /// Next outbound packet ID (server -> client).
    next_packet_id: u16,
    /// Bitset of in-use packet IDs (1..=65535). Bit N set = ID N is in use.
    /// Avoids 3 HashMap lookups per alloc_packet_id call.
    /// 65536 bits = 8192 bytes = 8 KiB (fits in L1 cache).
    packet_id_bitset: Box<[u64; 1024]>,

    // -- MQTT 5.0 features --
    /// Inbound topic alias map (alias number -> topic bytes), set by client.
    inbound_topic_aliases: HashMap<u16, Bytes>,
    /// Outbound topic alias map (topic bytes -> alias number), set by server.
    /// Uses Bytes key to avoid String allocation on lookup/insert.
    outbound_topic_aliases: HashMap<Bytes, u16>,
    next_outbound_alias: u16,
    /// Client's max topic alias (from CONNECT properties).
    max_topic_alias: u16,
    /// Client's receive_maximum (MQTT 5.0 flow control).
    client_receive_maximum: u16,
    /// Client's maximum packet size (MQTT 5.0). 0 = no limit from client.
    pub client_maximum_packet_size: u32,
    /// Session expiry interval in seconds (0 = expire on disconnect).
    pub session_expiry_interval: u32,

    // -- Performance counters --
    /// O(1) counter for inbound QoS 1/2 in-flight messages (client -> server).
    /// Used to enforce the server's Receive Maximum (MQTT 5.0 SS 3.3.4).
    inbound_qos_inflight: usize,
    /// The server's receive maximum advertised in CONNACK.
    /// Inbound QoS 1/2 publishes exceeding this trigger disconnect with 0x93.
    server_receive_maximum: u16,
    /// O(1) counter for outbound QoS 1 in-flight messages.
    /// Maintained on insert/remove to avoid iterating qos1_inflight.
    outbound_qos1_count: usize,
    /// Cached first subscription identifier for O(1) outbound delivery lookup.
    cached_first_sub_id: Option<u32>,
    /// Reverse index: queue_id -> subscription_id for O(1) outbound lookup.
    /// Updated on subscribe/unsubscribe; avoids O(n) scan per delivered message.
    queue_to_sub_id: HashMap<u64, u32>,
    /// Cached delivery info, invalidated on subscribe/unsubscribe.
    cached_delivery_info: Vec<SubDeliveryInfo>,
    /// True when cached_delivery_info needs rebuild.
    delivery_info_dirty: bool,

    // -- MQTT 5.0 Problem Information --
    /// MQTT 5.0 SS 3.1.2.11.7: If false, the server MUST NOT send
    /// Reason String or User Properties in any packet other than CONNACK/DISCONNECT.
    request_problem_information: bool,

    // -- Enhanced Authentication (MQTT 5.0) --
    /// Current state of enhanced auth flow.
    auth_state: AuthState,
    /// Pluggable auth provider (shared across sessions).
    auth_provider: Option<Arc<dyn AuthProvider>>,

    // -- Rate / Quota Limiting --
    /// Publish count in the current rate-limit window.
    rate_window_count: u32,
    /// Start of the current rate-limit window (ms since epoch).
    rate_window_start_ms: u64,
    /// Remaining publish quota (decremented per PUBLISH). 0 = unlimited.
    remaining_quota: u64,

    // -- Command output buffer --
    /// Shared output buffer for MqCommands. Methods write commands directly
    /// here via `MqCommandBuffer::write_*`. Callers drain commands after
    /// each method call — zero per-command heap allocation.
    pub cmd_buf: MqCommandBuffer,

    // -- MQTT packet output buffer --
    /// Reusable output buffer for MQTT response packets. Session methods
    /// encode response bytes (ConnAck, PubRel, PubComp, PingResp, etc.)
    /// directly here instead of returning MqttPacket values.
    /// Callers flush this buffer to the TCP stream after each method call.
    pub out_buf: BytesMut,
}

// ---------------------------------------------------------------------------
// Raw property scanning helpers — used by handle_publish_fused to extract
// specific property values without constructing a Properties struct.
// ---------------------------------------------------------------------------

/// Scan raw MQTT property bytes for a u16 property by ID.
#[inline]
fn scan_property_u16(props: &[u8], target_id: u8) -> Option<u16> {
    let mut pos = 0;
    while pos < props.len() {
        let id = props[pos];
        pos += 1;
        if id == target_id {
            if pos + 2 > props.len() {
                return None;
            }
            return Some(u16::from_be_bytes([props[pos], props[pos + 1]]));
        }
        pos = flat::skip_mqtt_property(props, pos, id)?;
    }
    None
}

/// Scan raw MQTT property bytes for a u32 property by ID.
#[inline]
fn scan_property_u32(props: &[u8], target_id: u8) -> Option<u32> {
    let mut pos = 0;
    while pos < props.len() {
        let id = props[pos];
        pos += 1;
        if id == target_id {
            if pos + 4 > props.len() {
                return None;
            }
            return Some(u32::from_be_bytes([
                props[pos],
                props[pos + 1],
                props[pos + 2],
                props[pos + 3],
            ]));
        }
        pos = flat::skip_mqtt_property(props, pos, id)?;
    }
    None
}

/// Scan raw MQTT property bytes for a byte property by ID.
#[inline]
fn scan_property_byte(props: &[u8], target_id: u8) -> Option<u8> {
    let mut pos = 0;
    while pos < props.len() {
        let id = props[pos];
        pos += 1;
        if id == target_id {
            if pos >= props.len() {
                return None;
            }
            return Some(props[pos]);
        }
        pos = flat::skip_mqtt_property(props, pos, id)?;
    }
    None
}

impl MqttSession {
    /// Create a new unconnected session.
    pub fn new(config: MqttSessionConfig) -> Self {
        let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            session_id,
            client_id: String::new(),
            protocol_version: ProtocolVersion::V311,
            clean_session: true,
            keep_alive: config.default_keep_alive,
            will: None,
            config,
            connected: false,
            session_resumed: false,
            entity_cache: EntityCache::default(),
            subscriptions: HashMap::new(),
            qos1_inflight: HashMap::new(),
            qos2_inbound: HashMap::new(),
            qos2_outbound: HashMap::new(),
            next_packet_id: 1,
            packet_id_bitset: Box::new([0u64; 1024]),
            inbound_topic_aliases: HashMap::new(),
            outbound_topic_aliases: HashMap::new(),
            next_outbound_alias: 1,
            max_topic_alias: 0,
            client_receive_maximum: 65535,
            client_maximum_packet_size: 0,
            session_expiry_interval: 0,
            inbound_qos_inflight: 0,
            server_receive_maximum: 65535,
            outbound_qos1_count: 0,
            cached_first_sub_id: None,
            queue_to_sub_id: HashMap::new(),
            cached_delivery_info: Vec::new(),
            delivery_info_dirty: true,
            request_problem_information: true,
            auth_state: AuthState::None,
            auth_provider: None,
            rate_window_count: 0,
            rate_window_start_ms: 0,
            remaining_quota: 0,
            cmd_buf: MqCommandBuffer::new(256),
            out_buf: BytesMut::with_capacity(128),
        }
    }

    /// Create a new session with an auth provider.
    pub fn with_auth_provider(config: MqttSessionConfig, provider: Arc<dyn AuthProvider>) -> Self {
        let mut session = Self::new(config);
        session.auth_provider = Some(provider);
        session
    }

    /// Return the will delay interval in seconds, or 0 if no will or no delay.
    pub fn will_delay_interval(&self) -> u32 {
        self.will
            .as_ref()
            .and_then(|w| w.properties().will_delay_interval())
            .unwrap_or(0)
    }

    /// Strip reason_string and user_properties from properties if
    /// request_problem_information is false (MQTT 5.0 SS 3.1.2.11.7).
    /// Only applies to non-CONNACK/DISCONNECT packets.
    ///
    /// With zero-copy Properties, this is a no-op when called on
    /// `Properties::default()` (which has no reason_string or user_properties).
    /// For non-default properties, the caller should avoid adding those fields
    /// to the builder when `request_problem_information` is false.
    fn strip_problem_info(&self, _props: &Properties) {
        // Properties is now a zero-copy type — fields cannot be mutated.
        // This is a no-op since it is only called on Properties::default().
    }

    /// Check publish rate and quota limits. Returns `Ok(())` if allowed, or
    /// `Err(reason_code)` with 0x96 (Message rate too high) or 0x97 (Quota exceeded).
    pub fn check_publish_rate_quota(&mut self) -> Result<(), u8> {
        let max_rate = self.config.max_publish_rate;
        if max_rate > 0 {
            let now = Self::now_ms();
            // 1-second sliding window.
            if now - self.rate_window_start_ms >= 1000 {
                self.rate_window_start_ms = now;
                self.rate_window_count = 0;
            }
            self.rate_window_count += 1;
            if self.rate_window_count > max_rate {
                return Err(0x96); // Message rate too high
            }
        }

        let max_quota = self.config.max_publish_quota;
        if max_quota > 0 {
            if self.remaining_quota == 0 {
                // Initialize on first use.
                self.remaining_quota = max_quota;
            }
            if self.remaining_quota <= 1 {
                return Err(0x97); // Quota exceeded
            }
            self.remaining_quota -= 1;
        }

        Ok(())
    }

    /// Mark a packet ID as in-use in the bitset.
    #[inline]
    fn mark_packet_id(&mut self, id: u16) {
        let idx = id as usize;
        self.packet_id_bitset[idx / 64] |= 1u64 << (idx % 64);
    }

    /// Clear a packet ID from the bitset.
    #[inline]
    fn clear_packet_id(&mut self, id: u16) {
        let idx = id as usize;
        self.packet_id_bitset[idx / 64] &= !(1u64 << (idx % 64));
    }

    /// Allocate the next outbound packet identifier. O(1) via bitset lookup.
    fn alloc_packet_id(&mut self) -> u16 {
        for _ in 0..65535u32 {
            let id = self.next_packet_id;
            self.next_packet_id = self.next_packet_id.wrapping_add(1);
            if self.next_packet_id == 0 {
                self.next_packet_id = 1;
            }
            let idx = id as usize;
            if self.packet_id_bitset[idx / 64] & (1u64 << (idx % 64)) == 0 {
                // Mark as in-use and return.
                self.packet_id_bitset[idx / 64] |= 1u64 << (idx % 64);
                return id;
            }
        }
        warn!("all packet IDs exhausted");
        self.next_packet_id
    }

    /// Get the current timestamp in milliseconds.
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Map an MQTT topic string to a bisque-mq topic name.
    #[allow(dead_code)]
    fn mq_topic_name(&self, mqtt_topic: &str) -> String {
        format!("{}{}", self.config.topic_prefix, mqtt_topic)
    }

    /// Map an MQTT topic string to a retained message bisque-mq topic name.
    fn retained_topic_name(&self, mqtt_topic: &str) -> String {
        let prefix = &*self.config.retained_prefix;
        let mut s = String::with_capacity(prefix.len() + mqtt_topic.len());
        s.push_str(prefix);
        s.push_str(mqtt_topic);
        s
    }

    /// Map an MQTT topic filter to a bisque-mq queue name.
    fn subscription_queue_name(&self, topic_filter: &str, shared_group: Option<&str>) -> String {
        // Build the queue name with a single allocation using pre-sized String.
        // Append topic_filter bytes with '/' -> '.' replacement at byte level
        // (both are single-byte ASCII, so UTF-8 safety is preserved).
        if let Some(group) = shared_group {
            let mut s = String::with_capacity(12 + group.len() + 1 + topic_filter.len());
            s.push_str("mqtt/shared/");
            s.push_str(group);
            s.push('/');
            push_dotted(&mut s, topic_filter);
            s
        } else {
            let mut s = String::with_capacity(9 + self.client_id.len() + 1 + topic_filter.len());
            s.push_str("mqtt/sub/");
            s.push_str(&self.client_id);
            s.push('/');
            push_dotted(&mut s, topic_filter);
            s
        }
    }

    /// Translate MQTT topic filter wildcards to bisque-mq exchange pattern.
    /// MQTT uses `+` for single-level wildcard; bisque-mq uses `*`.
    /// Both use `#` for multi-level wildcard.
    /// Returns `Cow::Borrowed` when no transformation needed (zero alloc fast path).
    fn mqtt_filter_to_mq_pattern(filter: &str) -> std::borrow::Cow<'_, str> {
        // Fast path: if no '+' present, return borrowed — zero allocation.
        // Uses memchr for SIMD-accelerated scan.
        if memchr::memchr(b'+', filter.as_bytes()).is_none() {
            return std::borrow::Cow::Borrowed(filter);
        }
        // Only allocate and transform when '+' is actually present.
        // Byte-level replacement: '+' and '*' are both single-byte ASCII.
        let mut buf = filter.as_bytes().to_vec();
        for b in &mut buf {
            if *b == b'+' {
                *b = b'*';
            }
        }
        // Safety: we only replaced ASCII '+' with ASCII '*', preserving UTF-8 validity.
        std::borrow::Cow::Owned(unsafe { String::from_utf8_unchecked(buf) })
    }

    /// Parse a shared subscription filter: `$share/group/actual/filter`.
    /// Returns `(group_name, actual_filter)` or None if not a shared sub.
    fn parse_shared_subscription(filter: &str) -> Option<(&str, &str)> {
        if let Some(rest) = filter.strip_prefix("$share/") {
            if let Some(slash_pos) = rest.find('/') {
                let group = &rest[..slash_pos];
                let topic_filter = &rest[slash_pos + 1..];
                return Some((group, topic_filter));
            }
        }
        None
    }

    // =========================================================================
    // Entity cache helpers (called by server after command responses)
    // =========================================================================

    /// Cache an exchange name -> id mapping.
    /// Accepts owned `String` (zero-alloc move) or `&str` (allocates only on miss).
    pub fn cache_exchange_id(&mut self, name: impl AsRef<str> + Into<String>, id: u64) {
        cache_insert(&mut self.entity_cache.exchanges, name, id);
    }

    /// Cache a queue name -> id mapping.
    pub fn cache_queue_id(&mut self, name: impl AsRef<str> + Into<String>, id: u64) {
        cache_insert(&mut self.entity_cache.queues, name, id);
    }

    /// Cache a topic name -> id mapping.
    pub fn cache_topic_id(&mut self, name: impl AsRef<str> + Into<String>, id: u64) {
        cache_insert(&mut self.entity_cache.topics, name, id);
    }

    /// Cache a binding (exchange_id, routing_key) -> binding_id mapping.
    pub fn cache_binding_id(
        &mut self,
        exchange_id: u64,
        routing_key: impl AsRef<str> + Into<String>,
        binding_id: u64,
    ) {
        cache_insert(
            self.entity_cache.bindings.entry(exchange_id).or_default(),
            routing_key,
            binding_id,
        );
    }

    /// Look up a cached exchange ID.
    pub fn cached_exchange_id(&self, name: &str) -> Option<u64> {
        self.entity_cache.exchanges.get(name).copied()
    }

    /// Look up a cached queue ID.
    pub fn cached_queue_id(&self, name: &str) -> Option<u64> {
        self.entity_cache.queues.get(name).copied()
    }

    /// Look up a cached topic ID.
    pub fn cached_topic_id(&self, name: &str) -> Option<u64> {
        self.entity_cache.topics.get(name).copied()
    }

    /// Look up a cached binding ID. O(1) via two-level HashMap (no key allocation).
    #[inline]
    pub fn cached_binding_id(&self, exchange_id: u64, routing_key: &str) -> Option<u64> {
        self.entity_cache
            .bindings
            .get(&exchange_id)
            .and_then(|m| m.get(routing_key))
            .copied()
    }

    // =========================================================================
    // CONNECT handling
    // =========================================================================

    /// Handle an incoming CONNECT packet.
    ///
    /// Commands written to `cmd_buf`. Returns the CONNACK response packet.
    pub fn handle_connect(&mut self, connect: &Connect) {
        self.cmd_buf.clear();
        self.out_buf.clear();
        self.protocol_version = connect.protocol_version;
        self.clean_session = connect.flags.clean_session;
        self.will = connect.will.clone();

        // Client ID handling (M5):
        // - V3.1.1: empty client_id with clean_session=true generates an ID;
        //   empty client_id with clean_session=false returns IdentifierRejected.
        // - V5: empty client_id always generates an ID (assigned_client_identifier in CONNACK).
        let mut assigned_client_id = None;
        if connect.client_id.is_empty() {
            match connect.protocol_version {
                ProtocolVersion::V311 => {
                    if connect.flags.clean_session {
                        self.client_id = format!("auto-{:016x}", self.session_id);
                    } else {
                        // Return CONNACK with IdentifierRejected.
                        encode_connack_v(
                            &ConnAck {
                                session_present: false,
                                return_code: ConnectReturnCode::IdentifierRejected as u8,
                                reason_string: Some(Bytes::from(
                                    "empty client ID requires clean_session=true",
                                )),
                                ..ConnAck::default()
                            },
                            &mut self.out_buf,
                            false,
                        );
                        return;
                    }
                }
                ProtocolVersion::V5 => {
                    let generated = format!("auto-{:016x}", self.session_id);
                    assigned_client_id = Some(generated.clone());
                    self.client_id = generated;
                }
            }
        } else {
            self.client_id = connect.client_id_str().to_string();
        }

        // Negotiate keep-alive (M16): only override keep_alive=0 with default for V3.1.1.
        // V5 allows keep_alive=0 (means no keep-alive timeout).
        self.keep_alive =
            if connect.keep_alive == 0 && connect.protocol_version == ProtocolVersion::V311 {
                self.config.default_keep_alive
            } else {
                connect.keep_alive
            };

        // MQTT 5.0: extract properties
        if let Some(max) = connect.properties().topic_alias_maximum() {
            self.max_topic_alias = max;
        }
        if let Some(rm) = connect.properties().receive_maximum() {
            self.client_receive_maximum = rm;
        }
        if let Some(mps) = connect.properties().maximum_packet_size() {
            self.client_maximum_packet_size = mps;
        }
        if let Some(sei) = connect.properties().session_expiry_interval() {
            self.session_expiry_interval = sei;
        }
        // MQTT 5.0 SS 3.1.2.11.7: Request Problem Information.
        // Default is true (1). If 0, suppress reason_string and user_properties
        // on all packets except CONNACK and DISCONNECT.
        if let Some(rpi) = connect.properties().request_problem_information() {
            self.request_problem_information = rpi != 0;
        }

        // Enhanced Authentication (MQTT 5.0, SS 3.1.2.11.9):
        // If the CONNECT contains an authentication_method, initiate the enhanced auth flow.
        if connect.protocol_version == ProtocolVersion::V5 {
            if let Some(method) = connect.properties().authentication_method() {
                if let Some(ref provider) = self.auth_provider {
                    if !provider.supports_method(method) {
                        encode_connack_v(
                            &ConnAck {
                                session_present: false,
                                return_code: 0x8C, // Bad authentication method
                                reason_string: Some(Bytes::from(
                                    "unsupported authentication method",
                                )),
                                ..ConnAck::default()
                            },
                            &mut self.out_buf,
                            true,
                        );
                        return;
                    }

                    let auth_data = connect.properties().authentication_data();
                    let username = connect.username_str();
                    let password = connect.password.as_ref().map(|b| b.as_ref());
                    let result = provider.authenticate_connect(
                        method,
                        &self.client_id,
                        username,
                        password,
                        auth_data,
                    );

                    match result {
                        AuthResult::Success => {
                            // Auth succeeded immediately — fall through to normal CONNACK.
                        }
                        AuthResult::Continue {
                            authentication_data,
                        } => {
                            // Send AUTH challenge, wait for client response.
                            self.auth_state = AuthState::WaitingForResponse {
                                method: method.to_string(),
                                step: 1,
                            };
                            encode_auth(
                                &Auth {
                                    reason_code: Auth::CONTINUE_AUTHENTICATION,
                                    authentication_method: Some(Bytes::from(method.to_string())),
                                    authentication_data: Some(authentication_data),
                                    reason_string: None,
                                },
                                &mut self.out_buf,
                            );
                            return;
                        }
                        AuthResult::Failed {
                            reason_code,
                            reason_string,
                        } => {
                            encode_connack_v(
                                &ConnAck {
                                    session_present: false,
                                    return_code: reason_code,
                                    reason_string: reason_string.map(|s| Bytes::from(s)),
                                    ..ConnAck::default()
                                },
                                &mut self.out_buf,
                                true,
                            );
                            return;
                        }
                    }
                } else {
                    // No auth provider configured but client sent authentication_method.
                    encode_connack_v(
                        &ConnAck {
                            session_present: false,
                            return_code: 0x8C, // Bad authentication method
                            reason_string: Some(Bytes::from(
                                "server does not support enhanced auth",
                            )),
                            ..ConnAck::default()
                        },
                        &mut self.out_buf,
                        true,
                    );
                    return;
                }
            }
        }

        self.connected = true;
        // Set the server's receive maximum for inbound flow control enforcement.
        self.server_receive_maximum = self.config.max_inflight.min(65535) as u16;

        // Compute consumer/producer name once to avoid double format! allocation.
        let consumer_name = format!("mqtt/{}", self.client_id);

        // Register this session with bisque-mq (unified consumer+producer).
        self.cmd_buf.write_create_session(
            self.session_id,
            &consumer_name,
            (self.keep_alive as u64) * 1000,
            (self.session_expiry_interval as u64) * 1000,
        );

        // If clean session, clear any old subscription state.
        if self.clean_session {
            self.subscriptions.clear();
            self.qos1_inflight.clear();
            self.qos2_inbound.clear();
            self.qos2_outbound.clear();
            self.packet_id_bitset.fill(0);
            self.entity_cache = EntityCache::default();
            self.outbound_qos1_count = 0;
            self.cached_first_sub_id = None;
            self.queue_to_sub_id.clear();
            self.delivery_info_dirty = true;
            // Reset topic alias maps (correctness: old aliases invalid on reconnect).
            self.outbound_topic_aliases.clear();
            self.inbound_topic_aliases.clear();
            self.next_outbound_alias = 1;
        }

        // Build CONNACK with capabilities for MQTT 5.0.
        let is_v5 = connect.protocol_version == ProtocolVersion::V5;
        let response_info = if is_v5 {
            // MQTT 5.0 SS 3.1.2.11.7: If client sets request_response_information=1,
            // server MAY include response_information in CONNACK.
            if connect.properties().request_response_information() == Some(1) {
                Some(Bytes::from(format!("mqtt/response/{}", self.client_id)))
            } else {
                None
            }
        } else {
            None
        };

        encode_connack_v(
            &ConnAck {
                session_present: !self.clean_session,
                return_code: ConnectReturnCode::Accepted as u8,
                maximum_qos: if is_v5 {
                    Some(self.config.maximum_qos.as_u8())
                } else {
                    None
                },
                retain_available: if is_v5 { Some(true) } else { None },
                maximum_packet_size: if is_v5 {
                    Some(self.config.max_packet_size as u32)
                } else {
                    None
                },
                topic_alias_maximum: if is_v5 { Some(65535) } else { None },
                wildcard_subscription_available: if is_v5 { Some(true) } else { None },
                subscription_identifier_available: if is_v5 { Some(true) } else { None },
                shared_subscription_available: if is_v5 { Some(true) } else { None },
                receive_maximum: if is_v5 {
                    Some(self.config.max_inflight.min(65535) as u16)
                } else {
                    None
                },
                server_keep_alive: if is_v5 { Some(self.keep_alive) } else { None },
                assigned_client_identifier: assigned_client_id.map(|id| Bytes::from(id)),
                response_information: response_info,
                ..ConnAck::default()
            },
            &mut self.out_buf,
            is_v5,
        );

        debug!(
            client_id = %self.client_id,
            session_id = self.session_id,
            version = ?self.protocol_version,
            clean = self.clean_session,
            "MQTT session connected"
        );
    }

    /// Patch the session_present flag in an already-encoded ConnAck in `out_buf`.
    ///
    /// ConnAck wire layout: `[0x20][remaining_len(1-4 bytes)][session_present][return_code]...`
    /// This patches the session_present byte (first byte after remaining_length).
    pub fn patch_connack_session_present(&mut self, present: bool) {
        if self.out_buf.len() >= 3 && self.out_buf[0] == 0x20 {
            // Skip past fixed header byte (0x20) + remaining length varint.
            let mut offset = 1;
            while offset < self.out_buf.len() && (self.out_buf[offset] & 0x80) != 0 {
                offset += 1;
            }
            offset += 1; // skip final byte of remaining_length
            if offset < self.out_buf.len() {
                self.out_buf[offset] = if present { 0x01 } else { 0x00 };
            }
        }
    }

    // =========================================================================
    // Enhanced Authentication (AUTH packet handling)
    // =========================================================================

    /// Handle an incoming AUTH packet during an enhanced authentication flow.
    ///
    /// Commands written to `cmd_buf`. Response packets encoded to `out_buf`.
    pub fn handle_auth(&mut self, auth: &Auth) {
        self.cmd_buf.clear();
        self.out_buf.clear();

        let (method, step) = match &self.auth_state {
            AuthState::WaitingForResponse { method, step } => (method.clone(), *step),
            _ => {
                warn!("received AUTH packet but no auth flow in progress");
                encode_disconnect(
                    &crate::types::Disconnect {
                        reason_code: Some(0x82), // Protocol Error
                        reason_string: None,
                        session_expiry_interval: None,
                        server_reference: None,
                    },
                    &mut self.out_buf,
                );
                let _will = self.handle_disconnect(None);
                return;
            }
        };

        // Validate reason code
        if auth.reason_code != Auth::CONTINUE_AUTHENTICATION {
            warn!(
                reason_code = auth.reason_code,
                "unexpected AUTH reason code"
            );
            self.auth_state = AuthState::Complete;
            encode_connack_v(
                &ConnAck {
                    session_present: false,
                    return_code: 0x82, // Protocol Error
                    reason_string: Some(Bytes::from("unexpected AUTH reason code")),
                    ..ConnAck::default()
                },
                &mut self.out_buf,
                true,
            );
            return;
        }

        // Validate method matches
        if let Some(pkt_method) = auth
            .authentication_method
            .as_ref()
            .map(|b| unsafe { std::str::from_utf8_unchecked(b) })
        {
            if pkt_method != method {
                warn!(expected = %method, got = %pkt_method, "AUTH method mismatch");
                self.auth_state = AuthState::Complete;
                encode_connack_v(
                    &ConnAck {
                        session_present: false,
                        return_code: 0x8C, // Bad authentication method
                        reason_string: Some(Bytes::from("authentication method mismatch")),
                        ..ConnAck::default()
                    },
                    &mut self.out_buf,
                    true,
                );
                return;
            }
        }

        let provider = match &self.auth_provider {
            Some(p) => Arc::clone(p),
            None => {
                self.auth_state = AuthState::Complete;
                encode_connack_v(
                    &ConnAck {
                        session_present: false,
                        return_code: 0x8C,
                        reason_string: Some(Bytes::from("server does not support enhanced auth")),
                        ..ConnAck::default()
                    },
                    &mut self.out_buf,
                    true,
                );
                return;
            }
        };

        let auth_data = auth.authentication_data.as_deref();
        let result = provider.authenticate_continue(&method, &self.client_id, auth_data, step);

        match result {
            AuthResult::Success => {
                self.auth_state = AuthState::Complete;
                self.connected = true;

                // Registration commands written to cmd_buf (same as handle_connect completion).
                let consumer_name = format!("mqtt/{}", self.client_id);
                self.cmd_buf.write_create_session(
                    self.session_id,
                    &consumer_name,
                    (self.keep_alive as u64) * 1000,
                    (self.session_expiry_interval as u64) * 1000,
                );

                debug!(
                    client_id = %self.client_id,
                    "enhanced auth completed successfully"
                );

                encode_connack_v(
                    &ConnAck {
                        session_present: !self.clean_session,
                        return_code: ConnectReturnCode::Accepted as u8,
                        maximum_qos: Some(self.config.maximum_qos.as_u8()),
                        retain_available: Some(true),
                        maximum_packet_size: Some(self.config.max_packet_size as u32),
                        topic_alias_maximum: Some(65535),
                        wildcard_subscription_available: Some(true),
                        subscription_identifier_available: Some(true),
                        shared_subscription_available: Some(true),
                        receive_maximum: Some(self.config.max_inflight.min(65535) as u16),
                        server_keep_alive: Some(self.keep_alive),
                        authentication_method: Some(Bytes::from(method.clone())),
                        ..ConnAck::default()
                    },
                    &mut self.out_buf,
                    true,
                );
            }
            AuthResult::Continue {
                authentication_data,
            } => {
                self.auth_state = AuthState::WaitingForResponse {
                    method: method.clone(),
                    step: step + 1,
                };
                encode_auth(
                    &Auth {
                        reason_code: Auth::CONTINUE_AUTHENTICATION,
                        authentication_method: Some(Bytes::from(method.clone())),
                        authentication_data: Some(authentication_data),
                        reason_string: None,
                    },
                    &mut self.out_buf,
                );
            }
            AuthResult::Failed {
                reason_code,
                reason_string,
            } => {
                self.auth_state = AuthState::Complete;
                encode_connack_v(
                    &ConnAck {
                        session_present: false,
                        return_code: reason_code,
                        reason_string: reason_string.map(|s| Bytes::from(s)),
                        ..ConnAck::default()
                    },
                    &mut self.out_buf,
                    true,
                );
            }
        }
    }

    // =========================================================================
    // PUBLISH handling
    // =========================================================================

    /// Handle an incoming PUBLISH packet from the client.
    ///
    /// Returns a `PublishPlan` for the server to orchestrate (resolve entity IDs,
    /// submit commands, send responses).
    /// Create an empty publish plan (used for error/skip returns).
    fn empty_publish_plan() -> PublishPlan {
        PublishPlan {
            exchange_name: MQTT_EXCHANGE_NAME,
            cached_exchange_id: None,
            msg_start: 0,
            msg_len: 0,
            flat_message: None,
            retained: None,
            need_disconnect: false,
        }
    }

    pub fn handle_publish(&mut self, publish: &Publish, msg_buf: &mut BytesMut) -> PublishPlan {
        self.out_buf.clear();
        let is_v5 = self.protocol_version == ProtocolVersion::V5;

        // Max QoS enforcement: reject PUBLISH with QoS above server's maximum_qos.
        // MQTT 5.0 SS 3.2.2.3.4: server MUST NOT forward QoS > advertised maximum_qos.
        if publish.qos > self.config.maximum_qos {
            warn!(
                client_qos = publish.qos.as_u8(),
                max_qos = self.config.maximum_qos.as_u8(),
                "PUBLISH QoS exceeds server maximum_qos"
            );
            if let Some(packet_id) = publish.packet_id {
                let rc = if is_v5 { Some(0x9B) } else { None };
                match publish.qos {
                    QoS::AtLeastOnce => encode_puback_raw(packet_id, rc, &mut self.out_buf, is_v5),
                    QoS::ExactlyOnce => encode_pubrec_raw(packet_id, rc, &mut self.out_buf, is_v5),
                    _ => {}
                }
            }
            return Self::empty_publish_plan();
        }

        // MQTT 5.0 SS 3.3.4: Receive Maximum enforcement.
        // If the client sends more QoS 1/2 PUBLISH packets than the server's
        // Receive Maximum without receiving acknowledgments, disconnect with 0x93.
        if publish.qos != QoS::AtMostOnce && is_v5 {
            if self.inbound_qos_inflight >= self.server_receive_maximum as usize {
                warn!(
                    client_id = %self.client_id,
                    inflight = self.inbound_qos_inflight,
                    receive_maximum = self.server_receive_maximum,
                    "receive maximum exceeded"
                );
                encode_disconnect_reason(
                    0x93,
                    Some(b"receive maximum exceeded"),
                    &mut self.out_buf,
                );
                let mut plan = Self::empty_publish_plan();
                plan.need_disconnect = true;
                return plan;
            }
            self.inbound_qos_inflight += 1;
        }

        // Rate / Quota limiting: check before processing the PUBLISH.
        if let Err(reason_code) = self.check_publish_rate_quota() {
            warn!(
                client_id = %self.client_id,
                reason_code,
                "publish rate/quota limit exceeded"
            );
            let reason_str: &[u8] = if reason_code == 0x96 {
                b"message rate too high"
            } else {
                b"quota exceeded"
            };
            encode_disconnect_reason(reason_code, Some(reason_str), &mut self.out_buf);
            let mut plan = Self::empty_publish_plan();
            plan.need_disconnect = true;
            return plan;
        }

        // Resolve topic alias if present (MQTT 5.0).
        // Topic is Bytes (UTF-8 validated on decode).
        // Gate behind has_props: V3.1.1 has no properties, skip the scan entirely.
        let has_props = !publish.properties.is_empty();
        let topic: Bytes = if has_props && let Some(alias) = publish.properties.topic_alias() {
            // Topic alias validation: alias must be 1..=server's topic_alias_maximum (65535).
            // MQTT 5.0 SS 3.3.2.3.4: alias of 0 is a protocol error.
            if alias == 0 {
                warn!("topic alias 0 is invalid");
                encode_disconnect_reason(
                    0x82,
                    Some(b"topic alias must not be 0"),
                    &mut self.out_buf,
                );
                let mut plan = Self::empty_publish_plan();
                plan.need_disconnect = true;
                return plan;
            }
            if !publish.topic.is_empty() {
                let t = Bytes::copy_from_slice(publish.topic);
                self.inbound_topic_aliases.insert(alias, t.clone());
                t
            } else if let Some(resolved) = self.inbound_topic_aliases.get(&alias) {
                resolved.clone()
            } else {
                warn!(alias, "unknown inbound topic alias");
                return Self::empty_publish_plan();
            }
        } else {
            Bytes::copy_from_slice(publish.topic)
        };

        // Validate topic name (M3): reject wildcards, empty, and null bytes.
        if !self.config.skip_topic_validation && validate_topic_name(&topic).is_err() {
            warn!(topic = ?std::str::from_utf8(&topic).unwrap_or("<invalid>"), "invalid topic name in PUBLISH");
            return Self::empty_publish_plan();
        }

        // Topic-level authorization (GAP-8): check if the client is allowed to publish.
        if let Some(ref provider) = self.auth_provider {
            let topic_str = std::str::from_utf8(&topic).unwrap_or("");
            let username = None; // Username not stored in session after CONNECT
            if !provider.authorize_topic(
                &self.client_id,
                username,
                topic_str,
                crate::auth::TopicAction::Publish,
            ) {
                warn!(
                    client_id = %self.client_id,
                    topic = topic_str,
                    "publish not authorized"
                );
                if let Some(packet_id) = publish.packet_id {
                    match publish.qos {
                        QoS::AtLeastOnce => {
                            encode_puback_raw(packet_id, Some(0x87), &mut self.out_buf, is_v5);
                        }
                        QoS::ExactlyOnce => {
                            encode_pubrec_raw(packet_id, Some(0x87), &mut self.out_buf, is_v5);
                        }
                        _ => {}
                    }
                }
                return Self::empty_publish_plan();
            }
        }

        // Payload format UTF-8 validation (MQTT 5.0 SS 3.3.2.3.2):
        // If payload_format_indicator = 1, the payload MUST be valid UTF-8.
        // Read once and reuse below to avoid scanning properties twice.
        let pfi = if has_props {
            publish.properties.payload_format_indicator()
        } else {
            None
        };
        if pfi == Some(1) {
            if std::str::from_utf8(publish.payload).is_err() {
                warn!("payload_format_indicator=1 but payload is not valid UTF-8");
                if let Some(packet_id) = publish.packet_id {
                    match publish.qos {
                        QoS::AtLeastOnce => {
                            encode_puback_raw(packet_id, Some(0x99), &mut self.out_buf, is_v5);
                        }
                        QoS::ExactlyOnce => {
                            encode_pubrec_raw(packet_id, Some(0x99), &mut self.out_buf, is_v5);
                        }
                        _ => {}
                    }
                }
                return Self::empty_publish_plan();
            }
        }

        let now = Self::now_ms();

        // Build MqttEnvelope: 40-byte header + raw topic + raw properties + payload.
        // No span index, no header key/value encoding — raw MQTT passthrough.
        let ttl = if has_props {
            publish
                .properties
                .message_expiry_interval()
                .map(|secs| secs as u64 * 1000)
                .unwrap_or(0)
        } else {
            0
        };

        let props_raw_ref: &[u8] = if has_props {
            publish.properties.raw()
        } else {
            &[]
        };
        // Write MqttEnvelope directly into the shared msg_buf (zero alloc).
        let msg_start = msg_buf.len();
        flat::MqttEnvelopeWriter::new(msg_buf, &topic, publish.payload)
            .timestamp(now)
            .publisher_id(self.session_id)
            .retain(publish.retain)
            .is_v5(self.protocol_version == ProtocolVersion::V5)
            .no_local(false)
            .utf8_payload(pfi == Some(1))
            .properties_raw(props_raw_ref)
            .ttl_ms(ttl)
            .finish();
        let msg_len = msg_buf.len() - msg_start;

        // Track in-flight state and encode ACK response directly to out_buf.
        match publish.qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => {
                if let Some(packet_id) = publish.packet_id {
                    self.mark_packet_id(packet_id);
                    self.qos1_inflight.insert(
                        packet_id,
                        QoS1InFlight {
                            mq_message_id: None,
                            mq_queue_id: None,
                            direction: Direction::Inbound,
                        },
                    );
                    let rc = if is_v5 { Some(0x00) } else { None };
                    encode_puback_raw(packet_id, rc, &mut self.out_buf, is_v5);
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = publish.packet_id {
                    // QoS 2 duplicate detection (m12): if packet_id already exists
                    // in PubRecSent state, respond with PUBREC but skip the publish.
                    if let Some(QoS2InboundState::PubRecSent) = self.qos2_inbound.get(&packet_id) {
                        debug!(packet_id, "QoS 2 duplicate PUBLISH, resending PUBREC");
                        let rc = if is_v5 { Some(0x00) } else { None };
                        encode_pubrec_raw(packet_id, rc, &mut self.out_buf, is_v5);
                        return Self::empty_publish_plan();
                    }
                    self.mark_packet_id(packet_id);
                    self.qos2_inbound
                        .insert(packet_id, QoS2InboundState::PubRecSent);
                    let rc = if is_v5 { Some(0x00) } else { None };
                    encode_pubrec_raw(packet_id, rc, &mut self.out_buf, is_v5);
                }
            }
        }

        // Handle retained messages.
        let retained = if publish.retain {
            let topic_str = std::str::from_utf8(&topic).unwrap_or("");
            let retained_name = self.retained_topic_name(topic_str);
            let cached_topic_id = self.entity_cache.topics.get(&retained_name).copied();

            if publish.payload.is_empty() {
                // Empty payload clears the retained message.
                Some(RetainedPlan {
                    topic_name: retained_name,
                    cached_topic_id,
                    flat_message: None,
                })
            } else {
                // Retained messages need owned storage — copy from msg_buf.
                Some(RetainedPlan {
                    topic_name: retained_name,
                    cached_topic_id,
                    flat_message: Some(Bytes::copy_from_slice(
                        &msg_buf[msg_start..msg_start + msg_len],
                    )),
                })
            }
        } else {
            None
        };

        PublishPlan {
            exchange_name: MQTT_EXCHANGE_NAME,
            cached_exchange_id: self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied(),
            msg_start,
            msg_len,
            flat_message: None,
            retained,
            need_disconnect: false,
        }
    }

    /// Fast-path PUBLISH handler for pre-built MqttEnvelope from fused decode.
    ///
    /// Takes a mutable envelope buffer + metadata from `decode_publish_to_envelope`.
    /// Handles topic alias resolution, TTL patching, QoS tracking, and retained
    /// message planning — but skips the intermediate Publish struct, Properties
    /// construction, and separate MqttEnvelopeBuilder allocation.
    ///
    /// ACK responses and disconnect are encoded directly to `self.out_buf`.
    pub fn handle_publish_fused(
        &mut self,
        envelope: &mut BytesMut,
        meta: &crate::codec::PublishMeta,
        msg_buf: &mut BytesMut,
    ) -> PublishPlan {
        self.out_buf.clear();
        let is_v5 = self.protocol_version == ProtocolVersion::V5;

        // Max QoS enforcement.
        if meta.qos > self.config.maximum_qos {
            if let Some(packet_id) = meta.packet_id {
                match meta.qos {
                    QoS::AtLeastOnce => {
                        encode_puback_raw(packet_id, Some(0x9B), &mut self.out_buf, is_v5)
                    }
                    QoS::ExactlyOnce => {
                        encode_pubrec_raw(packet_id, Some(0x9B), &mut self.out_buf, is_v5)
                    }
                    _ => {}
                }
            }
            return Self::empty_publish_plan();
        }

        // Receive maximum enforcement (QoS 1/2 only).
        if meta.qos != QoS::AtMostOnce
            && is_v5
            && self.inbound_qos_inflight >= self.server_receive_maximum as usize
        {
            encode_disconnect_reason(0x93, None, &mut self.out_buf);
            let mut plan = Self::empty_publish_plan();
            plan.need_disconnect = true;
            return plan;
        }

        // Extract topic from envelope for alias resolution + validation.
        let topic_bytes = &envelope[meta.topic_offset..meta.topic_offset + meta.topic_len];

        // Topic alias resolution (V5 only, when props are present).
        let topic = if is_v5 && meta.props_len > 0 {
            let props_start = 40 + meta.topic_len;
            let props_end = props_start + meta.props_len;
            let props = &envelope[props_start..props_end];
            let alias = scan_property_u16(props, 0x23);

            if let Some(alias_val) = alias {
                if alias_val == 0 {
                    encode_disconnect_reason(0x82, None, &mut self.out_buf);
                    let mut plan = Self::empty_publish_plan();
                    plan.need_disconnect = true;
                    return plan;
                }
                if meta.topic_len > 0 {
                    let t = Bytes::copy_from_slice(topic_bytes);
                    self.inbound_topic_aliases.insert(alias_val, t.clone());
                    t
                } else if let Some(resolved) = self.inbound_topic_aliases.get(&alias_val) {
                    resolved.clone()
                } else {
                    encode_disconnect_reason(0x82, None, &mut self.out_buf);
                    let mut plan = Self::empty_publish_plan();
                    plan.need_disconnect = true;
                    return plan;
                }
            } else {
                Bytes::copy_from_slice(topic_bytes)
            }
        } else {
            Bytes::copy_from_slice(topic_bytes)
        };

        // Topic validation.
        if !self.config.skip_topic_validation && validate_topic_name(&topic).is_err() {
            if let Some(packet_id) = meta.packet_id {
                match meta.qos {
                    QoS::AtLeastOnce => {
                        encode_puback_raw(packet_id, Some(0x90), &mut self.out_buf, is_v5)
                    }
                    QoS::ExactlyOnce => {
                        encode_pubrec_raw(packet_id, Some(0x99), &mut self.out_buf, is_v5)
                    }
                    _ => {}
                }
            }
            return Self::empty_publish_plan();
        }

        // Extract TTL from properties (scan for message_expiry_interval 0x02).
        let ttl_ms = if meta.props_len > 0 {
            let props_start = 40 + meta.topic_len;
            let props = &envelope[props_start..props_start + meta.props_len];
            scan_property_u32(props, 0x02)
                .map(|secs| secs as u64 * 1000)
                .unwrap_or(0)
        } else {
            0
        };

        // Patch the envelope in-place: set TTL, utf8 flag, no_local flag.
        let mut extra_flags: u16 = 0;
        if meta.props_len > 0 {
            let props_start = 40 + meta.topic_len;
            let props = &envelope[props_start..props_start + meta.props_len];
            if scan_property_byte(props, 0x01) == Some(1) {
                extra_flags |= 1 << 4; // MQTT_FLAG_UTF8_PAYLOAD
            }
        }
        crate::codec::patch_envelope_ttl_flags(envelope, ttl_ms, extra_flags);

        // Write the envelope into the shared msg_buf (zero alloc).
        let msg_start = msg_buf.len();
        if meta.topic_len == 0 && !topic.is_empty() {
            let payload_start = 40 + meta.topic_len + meta.props_len;
            let props_slice = if meta.props_len > 0 {
                let ps = 40 + meta.topic_len;
                &envelope[ps..ps + meta.props_len]
            } else {
                &[]
            };
            flat::MqttEnvelopeWriter::new(msg_buf, &topic, &envelope[payload_start..])
                .timestamp(Self::now_ms())
                .publisher_id(self.session_id)
                .retain(meta.retain)
                .is_v5(is_v5)
                .properties_raw(props_slice)
                .ttl_ms(ttl_ms)
                .finish();
        } else {
            let env_bytes = envelope.split();
            msg_buf.extend_from_slice(&env_bytes);
        };
        let msg_len = msg_buf.len() - msg_start;

        // QoS tracking + ACK response encoded directly to out_buf.
        match meta.qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => {
                if let Some(packet_id) = meta.packet_id {
                    self.mark_packet_id(packet_id);
                    self.qos1_inflight.insert(
                        packet_id,
                        QoS1InFlight {
                            mq_message_id: None,
                            mq_queue_id: None,
                            direction: Direction::Inbound,
                        },
                    );
                    let rc = if is_v5 { Some(0x00) } else { None };
                    encode_puback_raw(packet_id, rc, &mut self.out_buf, is_v5);
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = meta.packet_id {
                    if let Some(QoS2InboundState::PubRecSent) = self.qos2_inbound.get(&packet_id) {
                        let rc = if is_v5 { Some(0x00) } else { None };
                        encode_pubrec_raw(packet_id, rc, &mut self.out_buf, is_v5);
                        return Self::empty_publish_plan();
                    }
                    self.mark_packet_id(packet_id);
                    self.inbound_qos_inflight += 1;
                    self.qos2_inbound
                        .insert(packet_id, QoS2InboundState::PubRecSent);
                    let rc = if is_v5 { Some(0x00) } else { None };
                    encode_pubrec_raw(packet_id, rc, &mut self.out_buf, is_v5);
                }
            }
        }

        // Retained message handling.
        let retained = if meta.retain {
            let topic_str = std::str::from_utf8(&topic).unwrap_or("");
            let retained_name = self.retained_topic_name(topic_str);
            let cached_topic_id = self.entity_cache.topics.get(&retained_name).copied();
            if msg_len == 0 {
                Some(RetainedPlan {
                    topic_name: retained_name,
                    cached_topic_id,
                    flat_message: None,
                })
            } else {
                Some(RetainedPlan {
                    topic_name: retained_name,
                    cached_topic_id,
                    flat_message: Some(Bytes::copy_from_slice(
                        &msg_buf[msg_start..msg_start + msg_len],
                    )),
                })
            }
        } else {
            None
        };

        PublishPlan {
            exchange_name: MQTT_EXCHANGE_NAME,
            cached_exchange_id: self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied(),
            msg_start,
            msg_len,
            flat_message: None,
            retained,
            need_disconnect: false,
        }
    }

    // =========================================================================
    // PUBACK / PUBREC / PUBREL / PUBCOMP handling
    // =========================================================================

    /// Handle PUBACK from client (QoS 1 acknowledgment for server -> client delivery).
    ///
    /// Returns an ACK command if this was an outbound delivery, None otherwise.
    pub fn handle_puback(&mut self, packet_id: u16) {
        self.cmd_buf.clear();
        if let Some(inflight) = self.qos1_inflight.remove(&packet_id) {
            self.clear_packet_id(packet_id);
            let is_outbound = inflight.direction == Direction::Outbound;
            if is_outbound {
                self.outbound_qos1_count -= 1;
            } else {
                // Inbound QoS 1 completed — release receive maximum slot.
                self.inbound_qos_inflight = self.inbound_qos_inflight.saturating_sub(1);
            }
            debug!(packet_id, direction = ?inflight.direction, "QoS 1 acknowledged");
            if is_outbound {
                if let (Some(queue_id), Some(message_id)) =
                    (inflight.mq_queue_id, inflight.mq_message_id)
                {
                    self.cmd_buf.write_group_ack(queue_id, &[message_id], None);
                }
            }
        } else {
            warn!(packet_id, "PUBACK for unknown packet ID");
        }
    }

    /// Handle PUBREC from client (QoS 2, client acknowledges our outbound PUBLISH).
    ///
    /// Returns the PUBREL response packet. No MqCommands needed at this stage.
    pub fn handle_pubrec(&mut self, packet_id: u16) {
        self.out_buf.clear();
        // Transition outbound QoS 2 from PublishSent -> PubRelSent.
        let mut found = true;
        if let Some(state) = self.qos2_outbound.remove(&packet_id) {
            match state {
                QoS2OutboundState::PublishSent {
                    mq_queue_id,
                    mq_message_id,
                } => {
                    self.qos2_outbound.insert(
                        packet_id,
                        QoS2OutboundState::PubRelSent {
                            mq_queue_id,
                            mq_message_id,
                        },
                    );
                }
                QoS2OutboundState::PubRelSent { .. } => {
                    debug!(packet_id, "duplicate PUBREC (already in PubRelSent)");
                    self.qos2_outbound.insert(packet_id, state);
                }
            }
        } else {
            warn!(packet_id, "PUBREC for unknown outbound packet ID");
            found = false;
        }

        let reason_code = if self.protocol_version == ProtocolVersion::V5 {
            if found { Some(0x00) } else { Some(0x92) } // 0x92 = Packet Identifier Not Found
        } else {
            None
        };
        let is_v5 = self.protocol_version == ProtocolVersion::V5;
        encode_pubrel_v(
            &PubRel {
                packet_id,
                reason_code,
                reason_string: None,
            },
            &mut self.out_buf,
            is_v5,
        );
    }

    /// Handle PUBREL from client (QoS 2 inbound, step 2).
    ///
    /// Returns the PUBCOMP response packet. No MqCommands needed at this stage.
    pub fn handle_pubrel(&mut self, packet_id: u16) {
        self.out_buf.clear();
        let mut found = false;
        if let Some(state) = self.qos2_inbound.get(&packet_id) {
            match state {
                QoS2InboundState::PubRecSent => {
                    // Message was already published on PUBLISH receive.
                    // Release receive maximum slot — inbound QoS 2 flow completing.
                    self.inbound_qos_inflight = self.inbound_qos_inflight.saturating_sub(1);
                    found = true;
                }
                QoS2InboundState::Complete => {
                    debug!(packet_id, "duplicate PUBREL (already complete)");
                    found = true;
                }
            }
        } else {
            warn!(packet_id, "PUBREL for unknown packet ID");
        }

        self.qos2_inbound
            .insert(packet_id, QoS2InboundState::Complete);

        let reason_code = if self.protocol_version == ProtocolVersion::V5 {
            if found { Some(0x00) } else { Some(0x92) } // 0x92 = Packet Identifier Not Found
        } else {
            None
        };
        let is_v5 = self.protocol_version == ProtocolVersion::V5;
        encode_pubcomp_v(
            &PubComp {
                packet_id,
                reason_code,
                reason_string: None,
            },
            &mut self.out_buf,
            is_v5,
        );
    }

    /// Handle PUBCOMP from client (QoS 2 outbound complete).
    ///
    /// Returns an ACK command if the QoS 2 flow completed successfully, None otherwise.
    pub fn handle_pubcomp(&mut self, packet_id: u16) {
        self.cmd_buf.clear();
        if let Some(state) = self.qos2_outbound.remove(&packet_id) {
            self.clear_packet_id(packet_id);
            match state {
                QoS2OutboundState::PubRelSent {
                    mq_queue_id,
                    mq_message_id,
                } => {
                    // QoS 2 delivery complete. ACK in bisque-mq.
                    debug!(packet_id, "QoS 2 outbound delivery complete");
                    self.cmd_buf
                        .write_group_ack(mq_queue_id, &[mq_message_id], None);
                }
                QoS2OutboundState::PublishSent { .. } => {
                    warn!(
                        packet_id,
                        "PUBCOMP received but PUBREL not yet sent (protocol violation)"
                    );
                }
            }
        } else {
            warn!(packet_id, "PUBCOMP for unknown packet ID");
        }
    }

    // =========================================================================
    // SUBSCRIBE handling
    // =========================================================================

    /// Handle a SUBSCRIBE packet.
    ///
    /// Returns a `SubscribePlan` for the server to orchestrate.
    pub fn handle_subscribe(
        &mut self,
        packet_id: u16,
        filters: &[crate::types::TopicFilter],
        subscription_id: Option<u32>,
    ) -> SubscribePlan {
        self.out_buf.clear();
        let mut filter_plans = SmallVec::new();
        let mut return_codes: SmallVec<[u8; 8]> = SmallVec::new();

        for filter in filters {
            // Validate topic filter (M4).
            if validate_topic_filter(filter.filter_str()).is_err() {
                warn!(filter = %filter.filter_str(), "invalid topic filter in SUBSCRIBE");
                return_codes.push(0x80); // Failure
                continue;
            }

            // Check for shared subscription. For non-shared, avoid cloning
            // filter.filter into actual_filter — use a borrow instead.
            let (shared_group, actual_filter_owned) =
                if let Some((group, f)) = Self::parse_shared_subscription(filter.filter_str()) {
                    (Some(group.to_string()), Some(f.to_string()))
                } else {
                    (None, None)
                };
            let actual_filter = actual_filter_owned
                .as_deref()
                .unwrap_or(filter.filter_str());

            // MQTT 5.0 SS 3.8.3.1: It is a Protocol Error to set the No Local
            // bit to 1 on a Shared Subscription.
            if shared_group.is_some() && filter.no_local {
                warn!(filter = %filter.filter_str(), "no_local=1 on shared subscription is a protocol error");
                return_codes.push(0xA2); // Shared Subscriptions not supported (closest reason code)
                continue;
            }

            // Topic-level authorization (GAP-8): check if the client is allowed to subscribe.
            if let Some(ref provider) = self.auth_provider {
                let username = None; // Username not stored in session after CONNECT
                if !provider.authorize_topic(
                    &self.client_id,
                    username,
                    &actual_filter,
                    crate::auth::TopicAction::Subscribe,
                ) {
                    warn!(
                        client_id = %self.client_id,
                        filter = %filter.filter_str(),
                        "subscribe not authorized"
                    );
                    return_codes.push(0x87); // Not Authorized
                    continue;
                }
            }

            // Translate MQTT wildcards to bisque-mq pattern.
            let routing_key = Self::mqtt_filter_to_mq_pattern(actual_filter).into_owned();

            // Queue name: shared or per-client.
            let queue_name = self.subscription_queue_name(&actual_filter, shared_group.as_deref());

            let cached_queue_id = self.entity_cache.queues.get(&queue_name).copied();
            let cached_exchange_id = self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied();
            let cached_binding_id = cached_exchange_id.and_then(|eid| {
                self.entity_cache
                    .bindings
                    .get(&eid)
                    .and_then(|m| m.get(&routing_key))
                    .copied()
            });

            // QoS downgrade (m8): clamp to server maximum_qos.
            let granted_qos = if filter.qos > self.config.maximum_qos {
                self.config.maximum_qos
            } else {
                filter.qos
            };

            // Check if this subscription already exists (for retain_handling=1).
            let is_new_subscription = !self.subscriptions.contains_key(filter.filter_str());

            filter_plans.push(SubscribeFilterPlan {
                filter: filter.filter_str().to_string(),
                queue_name,
                cached_queue_id,
                routing_key,
                cached_binding_id,
                qos: granted_qos,
                shared_group: shared_group.clone(),
                no_local: filter.no_local,
                subscription_id,
                retain_handling: filter.retain_handling,
                is_new_subscription,
            });

            // Track the subscription. Use the owned actual_filter when available
            // (shared subs) to avoid an extra clone; otherwise clone from filter.filter.
            let sub_filter = actual_filter_owned.unwrap_or_else(|| filter.filter_str().to_string());
            self.subscriptions.insert(
                filter.filter_str().to_string(),
                SubscriptionMapping {
                    filter_starts_with_wildcard: filter.filter_str().starts_with('+')
                        || filter.filter_str().starts_with('#'),
                    filter: sub_filter,
                    max_qos: granted_qos,
                    exchange_id: cached_exchange_id,
                    binding_id: cached_binding_id,
                    queue_id: cached_queue_id,
                    topic_id: None,
                    shared_group,
                    subscription_id,
                    no_local: filter.no_local,
                    retain_as_published: filter.retain_as_published,
                    retain_handling: filter.retain_handling,
                },
            );

            return_codes.push(granted_qos.as_u8());

            // Update cached subscription ID for O(1) outbound lookup.
            if subscription_id.is_some() && self.cached_first_sub_id.is_none() {
                self.cached_first_sub_id = subscription_id;
            }

            // Maintain queue_to_sub_id reverse index when queue_id is already cached.
            if let (Some(qid), Some(sid)) = (cached_queue_id, subscription_id) {
                self.queue_to_sub_id.insert(qid, sid);
            }
            self.delivery_info_dirty = true;

            debug!(
                client_id = %self.client_id,
                filter = %filter.filter_str(),
                qos = ?filter.qos,
                "subscription added"
            );
        }

        // Encode SUBACK directly to out_buf — no MqttPacket intermediary.
        let is_v5 = self.protocol_version == ProtocolVersion::V5;
        encode_suback_raw(packet_id, &return_codes, &mut self.out_buf, is_v5);

        SubscribePlan {
            exchange_name: MQTT_EXCHANGE_NAME,
            cached_exchange_id: self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied(),
            filters: filter_plans,
        }
    }

    // =========================================================================
    // UNSUBSCRIBE handling
    // =========================================================================

    /// Handle an UNSUBSCRIBE packet.
    ///
    /// Returns MqCommands to tear down bindings and an UNSUBACK response.
    /// Commands written to `cmd_buf`. Returns UNSUBACK response.
    pub fn handle_unsubscribe(&mut self, packet_id: u16, filters: &[String]) {
        self.cmd_buf.clear();
        self.out_buf.clear();
        let mut reason_codes = SmallVec::new();

        for filter in filters {
            if let Some(mapping) = self.subscriptions.remove(filter) {
                // Delete the exchange binding.
                if let Some(binding_id) = mapping.binding_id {
                    self.cmd_buf.write_delete_binding(binding_id);
                }
                // Delete subscription queue if clean session and not shared.
                if self.clean_session && mapping.shared_group.is_none() {
                    if let Some(queue_id) = mapping.queue_id {
                        self.cmd_buf.write_delete_consumer_group(queue_id);
                    }
                }
                // Remove from queue_to_sub_id reverse index.
                if let Some(queue_id) = mapping.queue_id {
                    self.queue_to_sub_id.remove(&queue_id);
                }
                self.delivery_info_dirty = true;
                reason_codes.push(0x00); // Success
                debug!(
                    client_id = %self.client_id,
                    filter = %filter,
                    "subscription removed"
                );
            } else {
                reason_codes.push(0x11); // No subscription existed
            }
        }

        // Recalculate cached subscription ID after removals.
        self.cached_first_sub_id = self.subscriptions.values().find_map(|m| m.subscription_id);

        let is_v5 = self.protocol_version == ProtocolVersion::V5;
        encode_unsuback_v(
            &UnsubAck {
                packet_id,
                reason_codes,
                reason_string: None,
            },
            &mut self.out_buf,
            is_v5,
        );
    }

    // =========================================================================
    // PINGREQ handling
    // =========================================================================

    /// Handle a PINGREQ packet. Command written to `cmd_buf`, PingResp to `out_buf`.
    pub fn handle_pingreq(&mut self) {
        self.cmd_buf.clear();
        self.out_buf.clear();
        self.cmd_buf.write_heartbeat_session(self.session_id);
        encode_ping_resp(&mut self.out_buf);
    }

    // =========================================================================
    // DISCONNECT handling
    // =========================================================================

    /// Build a FlatMessage from a WillMessage, preserving all MQTT 5.0 properties.
    fn build_will_flat_message(will: &WillMessage, now: u64, include_delay: bool) -> Bytes {
        // Hoist all property bytes into variables that outlive the builder.
        let reply_to = will.properties().response_topic_bytes();
        let correlation_id = will.properties().correlation_data_bytes();
        let content_type = will.properties().content_type_bytes();
        let pfi_byte = will.properties().payload_format_indicator().map(|v| [v]);
        let will_props = will.properties();
        let user_props: Vec<_> = will_props
            .user_properties()
            .map(|(k, v): (&str, &str)| {
                let mut hdr_key = Vec::with_capacity(10 + k.len());
                hdr_key.extend_from_slice(b"mqtt.user.");
                hdr_key.extend_from_slice(k.as_bytes());
                (hdr_key, v.to_string())
            })
            .collect();

        let mut builder = FlatMessageBuilder::new(&will.payload)
            .timestamp(now)
            .routing_key(&will.topic);

        // MQTT 5.0 will properties.
        if let Some(ref rt) = reply_to {
            builder = builder.reply_to(rt);
        }
        if let Some(ref cd) = correlation_id {
            builder = builder.correlation_id(cd);
        }
        if let Some(secs) = will.properties().message_expiry_interval() {
            builder = builder.ttl_ms(secs as u64 * 1000);
        }
        if let Some(ref ct) = content_type {
            builder = builder.header(&HDR_CONTENT_TYPE[..], ct);
        }
        if let Some(ref b) = pfi_byte {
            builder = builder.header(&HDR_PAYLOAD_FORMAT[..], b);
        }
        for (k, v) in &user_props {
            builder = builder.header(k, v.as_bytes());
        }
        if include_delay {
            if let Some(delay_secs) = will.properties().will_delay_interval() {
                if delay_secs > 0 {
                    builder = builder.delay_ms(delay_secs as u64 * 1000);
                }
            }
        }

        builder.build()
    }

    /// Handle a DISCONNECT packet (clean disconnect).
    ///
    /// For MQTT 5.0, if the reason code is 0x04 (Disconnect with Will Message),
    /// the will is NOT cleared — it will be published by the server.
    /// For all other reason codes (including 0x00 Normal Disconnection), the will
    /// is cleared per MQTT 5.0 SS 3.14.4.
    pub fn handle_disconnect(
        &mut self,
        disconnect: Option<&crate::types::Disconnect>,
    ) -> Option<PublishPlan> {
        self.cmd_buf.clear();

        // MQTT 5.0: Update session expiry interval from DISCONNECT properties (SS 3.14.2.2.2).
        // It is a Protocol Error to set a non-zero Session Expiry Interval if the
        // Session Expiry Interval in the CONNECT packet was zero.
        if self.protocol_version == ProtocolVersion::V5 {
            if let Some(d) = disconnect {
                if let Some(new_sei) = d.session_expiry_interval {
                    if self.session_expiry_interval == 0 && new_sei != 0 {
                        warn!(
                            client_id = %self.client_id,
                            "protocol error: cannot set non-zero session expiry on DISCONNECT when CONNECT had 0"
                        );
                    } else {
                        self.session_expiry_interval = new_sei;
                    }
                }
            }
        }

        // MQTT 5.0 reason code 0x04 = Disconnect with Will Message.
        let publish_will = if self.protocol_version == ProtocolVersion::V5 {
            disconnect
                .and_then(|d| d.reason_code)
                .map_or(false, |rc| rc == 0x04)
        } else {
            false
        };

        let will_plan = if publish_will {
            // Build will plan preserving all MQTT 5.0 properties (reason 0x04).
            self.will.take().map(|will| {
                let now = Self::now_ms();
                let flat_message = Self::build_will_flat_message(&will, now, false);

                PublishPlan {
                    exchange_name: MQTT_EXCHANGE_NAME,
                    cached_exchange_id: self
                        .entity_cache
                        .exchanges
                        .get(MQTT_EXCHANGE_NAME)
                        .copied(),
                    msg_start: 0,
                    msg_len: 0,
                    flat_message: Some(flat_message),
                    retained: if will.retain && !will.payload.is_empty() {
                        let retained_name = self.retained_topic_name(unsafe {
                            std::str::from_utf8_unchecked(&will.topic)
                        });
                        let cached_topic_id = self.entity_cache.topics.get(&retained_name).copied();
                        let ret_msg = Self::build_will_flat_message(&will, now, false);
                        Some(RetainedPlan {
                            topic_name: retained_name,
                            cached_topic_id,
                            flat_message: Some(ret_msg),
                        })
                    } else {
                        None
                    },
                    need_disconnect: false,
                }
            })
        } else {
            self.will = None;
            None
        };

        self.connected = false;

        self.cmd_buf
            .write_disconnect_session(self.session_id, publish_will);

        // If clean session, remove all subscription bindings and queues.
        if self.clean_session {
            for mapping in self.subscriptions.values() {
                if let Some(binding_id) = mapping.binding_id {
                    self.cmd_buf.write_delete_binding(binding_id);
                }
                if mapping.shared_group.is_none() {
                    if let Some(queue_id) = mapping.queue_id {
                        self.cmd_buf.write_delete_consumer_group(queue_id);
                    }
                }
            }
            self.subscriptions.clear();
            self.entity_cache = EntityCache::default();
        }

        debug!(
            client_id = %self.client_id,
            session_id = self.session_id,
            clean = self.clean_session,
            will_published = will_plan.is_some(),
            "MQTT session disconnected (clean)"
        );

        will_plan
    }

    // =========================================================================
    // Unclean disconnect (TCP drop)
    // =========================================================================

    /// Handle an unclean disconnect (TCP connection dropped without DISCONNECT).
    ///
    /// Returns a `PublishPlan` for the will message (if any) and cleanup commands.
    pub fn handle_unclean_disconnect(&mut self) -> Option<PublishPlan> {
        self.cmd_buf.clear();

        // Build will message plan if present.
        let will_plan = if let Some(ref will) = self.will {
            let now = Self::now_ms();
            let flat_message = Self::build_will_flat_message(will, now, true);

            Some(PublishPlan {
                exchange_name: MQTT_EXCHANGE_NAME,
                cached_exchange_id: self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied(),
                msg_start: 0,
                msg_len: 0,
                flat_message: Some(flat_message),
                retained: if will.retain && !will.payload.is_empty() {
                    let retained_name = self
                        .retained_topic_name(unsafe { std::str::from_utf8_unchecked(&will.topic) });
                    let cached_topic_id = self.entity_cache.topics.get(&retained_name).copied();
                    let ret_msg = Self::build_will_flat_message(will, now, false);
                    Some(RetainedPlan {
                        topic_name: retained_name,
                        cached_topic_id,
                        flat_message: Some(ret_msg),
                    })
                } else {
                    None
                },
                need_disconnect: false,
            })
        } else {
            None
        };

        self.will = None;
        self.connected = false;

        // Disconnect session. For unclean disconnect, publish will message.
        self.cmd_buf.write_disconnect_session(self.session_id, true);

        // Collect all outbound NACK message IDs, grouped by queue_id.
        // Uses SmallVec to avoid HashMap allocation for the common case.
        let mut nack_pairs: SmallVec<[(u64, u64); 16]> = SmallVec::new();
        for inflight in self.qos1_inflight.values() {
            if inflight.direction == Direction::Outbound {
                if let (Some(qid), Some(mid)) = (inflight.mq_queue_id, inflight.mq_message_id) {
                    nack_pairs.push((qid, mid));
                }
            }
        }
        for state in self.qos2_outbound.values() {
            let (qid, mid) = match state {
                QoS2OutboundState::PublishSent {
                    mq_queue_id,
                    mq_message_id,
                }
                | QoS2OutboundState::PubRelSent {
                    mq_queue_id,
                    mq_message_id,
                } => (*mq_queue_id, *mq_message_id),
            };
            nack_pairs.push((qid, mid));
        }

        // Sort by queue_id then batch into NACK commands.
        if !nack_pairs.is_empty() {
            nack_pairs.sort_unstable_by_key(|(qid, _)| *qid);
            let mut i = 0;
            while i < nack_pairs.len() {
                let cur_qid = nack_pairs[i].0;
                let start = i;
                while i < nack_pairs.len() && nack_pairs[i].0 == cur_qid {
                    i += 1;
                }
                let mids: SmallVec<[u64; 8]> =
                    nack_pairs[start..i].iter().map(|(_, mid)| *mid).collect();
                self.cmd_buf.write_group_nack(cur_qid, &mids);
            }
        }

        self.qos1_inflight.clear();
        self.qos2_inbound.clear();
        self.qos2_outbound.clear();
        self.packet_id_bitset.fill(0);
        self.outbound_qos1_count = 0;
        self.queue_to_sub_id.clear();
        self.delivery_info_dirty = true;

        debug!(
            client_id = %self.client_id,
            session_id = self.session_id,
            "MQTT session disconnected (unclean)"
        );

        will_plan
    }

    // =========================================================================
    // Outbound message delivery (bisque-mq -> MQTT client)
    // =========================================================================

    // =========================================================================
    // Outbound delivery tracking (zero-alloc hot path)
    // =========================================================================

    /// Track an outbound delivery without constructing a Publish packet.
    ///
    /// Used with `encode_publish_from_flat()` for zero-allocation delivery.
    /// Returns `None` if inflight is full, `Some(None)` for QoS 0 (no packet_id),
    /// `Some(Some(id))` for QoS 1/2 with the allocated packet_id.
    pub fn track_outbound_delivery(
        &mut self,
        effective_qos: QoS,
        mq_queue_id: u64,
        mq_message_id: u64,
    ) -> Option<Option<u16>> {
        if effective_qos != QoS::AtMostOnce && self.is_inflight_full() {
            return None;
        }

        if effective_qos == QoS::AtMostOnce {
            return Some(None);
        }

        let id = self.alloc_packet_id();

        match effective_qos {
            QoS::AtLeastOnce => {
                self.qos1_inflight.insert(
                    id,
                    QoS1InFlight {
                        mq_message_id: Some(mq_message_id),
                        mq_queue_id: Some(mq_queue_id),
                        direction: Direction::Outbound,
                    },
                );
                self.outbound_qos1_count += 1;
            }
            QoS::ExactlyOnce => {
                self.qos2_outbound.insert(
                    id,
                    QoS2OutboundState::PublishSent {
                        mq_queue_id,
                        mq_message_id,
                    },
                );
            }
            _ => {}
        }

        Some(Some(id))
    }

    /// Resolve outbound topic alias for a topic (V5 only).
    /// Zero-alloc lookup via Bytes key; only allocates on first-time insert.
    ///
    /// Returns `Some((alias, is_new))` where `is_new` indicates whether this is
    /// a newly assigned alias (topic must be sent) or an existing one (topic can
    /// be zero-length per MQTT 5.0 SS 3.3.2.3.4).
    pub fn resolve_outbound_topic_alias(&mut self, topic: &[u8]) -> Option<(u16, bool)> {
        if self.max_topic_alias == 0 {
            return None;
        }
        if let Some(&alias) = self.outbound_topic_aliases.get(topic) {
            Some((alias, false)) // Existing alias
        } else if self.next_outbound_alias <= self.max_topic_alias {
            let alias = self.next_outbound_alias;
            self.next_outbound_alias += 1;
            self.outbound_topic_aliases
                .insert(Bytes::copy_from_slice(topic), alias);
            Some((alias, true)) // Newly assigned
        } else {
            None
        }
    }

    /// Find a subscription identifier to include in outbound PUBLISH (V5).
    /// O(1) via cached value instead of iterating all subscriptions.
    pub fn find_subscription_id(&self) -> Option<u32> {
        self.cached_first_sub_id
    }

    /// Find the subscription identifier for a specific queue_id.
    /// O(1) via reverse index maintained on subscribe/unsubscribe.
    ///
    /// MQTT 5.0 SS 3.8.3.1: the subscription identifier from the matching
    /// subscription MUST be sent in the PUBLISH to the subscriber.
    #[inline]
    pub fn find_subscription_id_for_queue(&self, queue_id: u64) -> Option<u32> {
        self.queue_to_sub_id.get(&queue_id).copied()
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Return the number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Return the number of in-flight QoS 1 messages.
    pub fn qos1_inflight_count(&self) -> usize {
        self.qos1_inflight.len()
    }

    /// Return the number of in-flight QoS 2 messages (inbound + outbound).
    pub fn qos2_inflight_count(&self) -> usize {
        self.qos2_inbound.len() + self.qos2_outbound.len()
    }

    /// Total outbound in-flight count (for flow control).
    /// O(1) via maintained counters instead of iterating qos1_inflight.
    #[inline]
    pub fn outbound_inflight_count(&self) -> usize {
        self.outbound_qos1_count + self.qos2_outbound.len()
    }

    /// Check if the session has exceeded its inflight limit.
    #[inline]
    pub fn is_inflight_full(&self) -> bool {
        let outbound = self.outbound_inflight_count();
        outbound
            >= self
                .config
                .max_inflight
                .min(self.client_receive_maximum as usize)
    }

    /// Remaining capacity for outbound in-flight messages.
    #[inline]
    pub fn remaining_inflight(&self) -> usize {
        let max = self
            .config
            .max_inflight
            .min(self.client_receive_maximum as usize);
        max.saturating_sub(self.outbound_inflight_count())
    }

    /// Inbound QoS 1/2 inflight count (for session persistence).
    pub fn inbound_qos_inflight(&self) -> u32 {
        self.inbound_qos_inflight as u32
    }

    /// Outbound QoS 1 inflight count (for session persistence).
    pub fn outbound_qos1_count(&self) -> u32 {
        self.outbound_qos1_count as u32
    }

    /// Remaining rate-limit quota (for session persistence).
    pub fn remaining_quota(&self) -> u64 {
        self.remaining_quota
    }

    /// Restore flow control counters from a persisted session.
    pub fn restore_flow_control(&mut self, inbound: u32, outbound: u32, quota: u64) {
        self.inbound_qos_inflight = inbound as usize;
        self.outbound_qos1_count = outbound as usize;
        self.remaining_quota = quota;
    }

    /// Iterator over subscriptions for the delivery loop.
    pub fn subscriptions_iter(&self) -> impl Iterator<Item = (&str, &SubscriptionMapping)> {
        self.subscriptions.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Return cached delivery info, rebuilding if dirty.
    /// Avoids O(n) subscription scan on every delivery cycle.
    pub fn delivery_info(&mut self) -> &[SubDeliveryInfo] {
        if self.delivery_info_dirty {
            self.cached_delivery_info.clear();
            self.cached_delivery_info
                .extend(self.subscriptions.values().filter_map(|mapping| {
                    mapping.queue_id.map(|qid| SubDeliveryInfo {
                        queue_id: qid,
                        max_qos: mapping.max_qos,
                        no_local: mapping.no_local,
                        retain_as_published: mapping.retain_as_published,
                        filter_starts_with_wildcard: mapping.filter_starts_with_wildcard,
                        is_shared: mapping.shared_group.is_some(),
                    })
                }));
            self.delivery_info_dirty = false;
        }
        &self.cached_delivery_info
    }

    /// Check if any subscription with the given queue_id has no_local=true.
    /// O(1) via cached delivery info scan (small vec, typically < 10 entries).
    pub fn has_no_local_for_queue(&mut self, queue_id: u64) -> bool {
        self.delivery_info()
            .iter()
            .any(|d| d.queue_id == queue_id && d.no_local)
    }

    /// Check if any subscription with the given queue_id has retain_as_published=true.
    /// O(1) via cached delivery info scan (small vec, typically < 10 entries).
    pub fn has_retain_as_published_for_queue(&mut self, queue_id: u64) -> bool {
        self.delivery_info()
            .iter()
            .any(|d| d.queue_id == queue_id && d.retain_as_published)
    }

    /// Return outbound QoS 1 in-flight packet IDs (for session persistence).
    pub fn pending_qos1_packet_ids(&self) -> SmallVec<[u16; 16]> {
        self.qos1_inflight
            .iter()
            .filter(|(_, v)| v.direction == Direction::Outbound)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Return outbound QoS 2 in-flight packet IDs (for session persistence).
    pub fn pending_qos2_packet_ids(&self) -> SmallVec<[u16; 16]> {
        self.qos2_outbound.keys().copied().collect()
    }

    /// Restore subscriptions from a persisted session.
    ///
    /// Returns `SubscribePlan`s that the server must execute to re-create
    /// queues and bindings in bisque-mq. Called on reconnect when
    /// clean_session=false and a persisted session is found.
    pub fn restore_subscriptions(
        &mut self,
        persisted: &[crate::session_store::PersistedSubscription],
    ) -> SmallVec<[SubscribePlan; 4]> {
        let mut plans = SmallVec::new();
        for sub in persisted {
            let filter = crate::types::TopicFilter {
                filter: if let Some(ref group) = sub.shared_group {
                    Bytes::from(format!("$share/{}/{}", group, sub.filter))
                } else {
                    Bytes::from(sub.filter.clone())
                },
                qos: sub.max_qos,
                no_local: sub.no_local,
                retain_as_published: sub.retain_as_published,
                retain_handling: 2, // Don't send retained on restore (already received)
            };
            let filters: SmallVec<[crate::types::TopicFilter; 4]> = SmallVec::from_elem(filter, 1);
            // Use packet_id=0 since this is a server-internal restore, not a client SUBSCRIBE.
            let plan = self.handle_subscribe(0, &filters, sub.subscription_id);
            plans.push(plan);
        }
        plans
    }

    /// Encode retransmit packets to `out_buf` for session resumption (MQTT 5.0 SS 4.4).
    ///
    /// Encodes PUBREL for outbound QoS 2 in PubRelSent state.
    /// Returns the number of packets encoded.
    pub fn pending_retransmits(&mut self) -> usize {
        self.out_buf.clear();
        let is_v5 = self.protocol_version == ProtocolVersion::V5;
        let mut count = 0;

        // Retransmit PUBREL for outbound QoS 2 in PubRelSent state.
        for (&packet_id, state) in &self.qos2_outbound {
            if matches!(state, QoS2OutboundState::PubRelSent { .. }) {
                let rc = if is_v5 { Some(0x00) } else { None };
                encode_pubrel_v(
                    &PubRel {
                        packet_id,
                        reason_code: rc,
                        reason_string: None,
                    },
                    &mut self.out_buf,
                    is_v5,
                );
                count += 1;
            }
        }

        count
    }

    /// Update a subscription mapping with resolved bisque-mq entity IDs.
    pub fn update_subscription_ids(
        &mut self,
        filter: &str,
        exchange_id: Option<u64>,
        binding_id: Option<u64>,
        queue_id: Option<u64>,
        topic_id: Option<u64>,
    ) {
        if let Some(mapping) = self.subscriptions.get_mut(filter) {
            if let Some(id) = exchange_id {
                mapping.exchange_id = Some(id);
            }
            if let Some(id) = binding_id {
                mapping.binding_id = Some(id);
            }
            if let Some(id) = queue_id {
                mapping.queue_id = Some(id);
                // Maintain queue_id -> subscription_id reverse index.
                if let Some(sub_id) = mapping.subscription_id {
                    self.queue_to_sub_id.insert(id, sub_id);
                }
            }
            if let Some(id) = topic_id {
                mapping.topic_id = Some(id);
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;
    use crate::types::*;
    use bisque_mq::MqCommand;
    use bisque_mq::flat::FlatMessage;

    fn make_session() -> MqttSession {
        MqttSession::new(MqttSessionConfig::default())
    }

    fn make_connect(client_id: &str, clean: bool) -> Connect {
        Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: clean,
            },
            keep_alive: 60,
            client_id: Bytes::copy_from_slice(client_id.as_bytes()),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        }
    }

    fn make_connect_v5(client_id: &str, clean: bool) -> Connect {
        Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: clean,
            },
            keep_alive: 60,
            client_id: Bytes::copy_from_slice(client_id.as_bytes()),
            will: None,
            username: None,
            password: None,
            properties_raw: PropertiesBuilder::new()
                .topic_alias_maximum(10)
                .receive_maximum(100)
                .session_expiry_interval(3600)
                .build(),
        }
    }

    // ---- CONNECT tests ----

    #[test]
    fn test_session_connect() {
        let mut session = make_session();
        let connect = make_connect("test-client", true);

        session.handle_connect(&connect);

        assert!(session.connected);
        assert_eq!(session.client_id, "test-client");
        assert!(session.clean_session);
        assert_eq!(session.keep_alive, 60);

        assert_eq!(session.cmd_buf.len(), 1);
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_CREATE_SESSION
        );

        let (connack, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(connack.return_code, 0x00);
        assert!(!connack.session_present);
    }

    #[test]
    fn test_session_connect_v5_capabilities() {
        let mut session = make_session();
        let connect = make_connect_v5("v5-client", true);

        session.handle_connect(&connect);

        assert_eq!(session.max_topic_alias, 10);
        assert_eq!(session.client_receive_maximum, 100);
        assert_eq!(session.session_expiry_interval, 3600);

        let (connack, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert!(connack.maximum_qos.is_some());
        assert!(connack.retain_available.is_some());
        assert!(connack.shared_subscription_available.is_some());
        assert!(connack.wildcard_subscription_available.is_some());
        assert!(connack.receive_maximum.is_some());
    }

    // ---- PUBLISH tests ----

    #[test]
    fn test_publish_qos0_returns_plan() {
        let mut session = make_session();
        session.handle_connect(&make_connect("pub-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"sensor/1/temp",
            packet_id: None,
            payload: b"22.5",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);

        assert_eq!(plan.exchange_name, MQTT_EXCHANGE_NAME);
        assert!(plan.has_message());
        assert!(session.out_buf.is_empty()); // QoS 0: no ack
        assert!(plan.retained.is_none());
    }

    #[test]
    fn test_publish_qos1_returns_puback() {
        let mut session = make_session();
        session.handle_connect(&make_connect("pub-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"sensor/1/temp",
            packet_id: Some(1),
            payload: b"22.5",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);

        assert!(!session.out_buf.is_empty());
        let (puback, _) = codec::decode_buf::puback(&session.out_buf).unwrap();
        assert_eq!(puback.packet_id, 1);
    }

    #[test]
    fn test_publish_qos2_returns_pubrec() {
        let mut session = make_session();
        session.handle_connect(&make_connect("pub-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: b"important/data",
            packet_id: Some(10),
            payload: b"critical",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);

        assert!(!session.out_buf.is_empty());
        let (pubrec, _) = codec::decode_buf::pubrec(&session.out_buf).unwrap();
        assert_eq!(pubrec.packet_id, 10);
        assert_eq!(session.qos2_inflight_count(), 1);

        // PUBREL completes the inbound QoS 2.
        session.handle_pubrel(10);
        codec::decode_buf::pubcomp(&session.out_buf).unwrap();
    }

    #[test]
    fn test_publish_uses_native_reply_to_and_correlation_id() {
        let mut session = make_session();
        session.handle_connect(&make_connect("rr-client", true));

        let props_raw = PropertiesBuilder::new()
            .response_topic("reply/topic")
            .correlation_data(b"corr-123")
            .build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"request/topic",
            packet_id: None,
            payload: b"request body",
            properties: Properties::from_raw(&props_raw),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);

        // Verify the MqttEnvelope has reply_to and correlation_id from raw properties.
        let msg_slice = &msg_buf[plan.msg_start..plan.msg_start + plan.msg_len];
        let env = bisque_mq::flat::MqttEnvelope::new(msg_slice).expect("valid mqtt envelope");
        let reply_to = env.reply_to().expect("should have reply_to");
        assert_eq!(&reply_to[..], b"reply/topic");
        let corr_id = env.correlation_id().expect("should have correlation_id");
        assert_eq!(&corr_id[..], b"corr-123");
    }

    #[test]
    fn test_publish_with_ttl() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ttl-client", true));

        let props_raw = PropertiesBuilder::new().message_expiry_interval(60).build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"expiring",
            packet_id: None,
            payload: b"data",
            properties: Properties::from_raw(&props_raw),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        let msg_slice = &msg_buf[plan.msg_start..plan.msg_start + plan.msg_len];
        let env = bisque_mq::flat::MqttEnvelope::new(msg_slice).expect("valid mqtt envelope");
        assert_eq!(env.ttl_ms(), Some(60_000));
    }

    #[test]
    fn test_publish_retained() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ret-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: b"status/device1",
            packet_id: None,
            payload: b"online",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        let retained = plan.retained.as_ref().expect("should have retained plan");
        assert_eq!(retained.topic_name, "$mqtt/retained/status/device1");
        assert!(retained.flat_message.is_some());
    }

    #[test]
    fn test_publish_retained_clear() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ret-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: b"status/device1",
            packet_id: None,
            payload: b"", // Empty = clear
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        let retained = plan.retained.as_ref().expect("should have retained plan");
        assert!(retained.flat_message.is_none()); // Clear signal
    }

    // ---- SUBSCRIBE tests ----

    #[test]
    fn test_subscribe_returns_plan() {
        let mut session = make_session();
        session.handle_connect(&make_connect("sub-client", true));

        let filters = vec![
            TopicFilter {
                filter: Bytes::from("sensor/+/data"),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
            TopicFilter {
                filter: Bytes::from("control/#"),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
        ];

        let plan = session.handle_subscribe(1, &filters, None);

        assert_eq!(plan.exchange_name, MQTT_EXCHANGE_NAME);
        assert_eq!(plan.filters.len(), 2);

        // Verify wildcard translation: + -> *
        assert_eq!(plan.filters[0].routing_key, "sensor/*/data");
        assert_eq!(plan.filters[1].routing_key, "control/#");

        // Verify queue names.
        assert!(plan.filters[0].queue_name.contains("sub-client"));
        assert!(plan.filters[0].queue_name.contains("sensor"));

        assert_eq!(session.subscription_count(), 2);

        let (sa, _) = codec::decode_buf::suback(&session.out_buf).unwrap();
        assert_eq!(sa.packet_id, 1);
        assert_eq!(sa.return_codes.as_slice(), &[1, 0]);
    }

    #[test]
    fn test_subscribe_shared() {
        let mut session = make_session();
        session.handle_connect(&make_connect("shared-client", true));

        let filters = vec![TopicFilter {
            filter: Bytes::from("$share/mygroup/sensor/+/data"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];

        let plan = session.handle_subscribe(1, &filters, None);

        assert_eq!(plan.filters.len(), 1);
        assert_eq!(plan.filters[0].shared_group, Some("mygroup".to_string()));
        assert_eq!(plan.filters[0].routing_key, "sensor/*/data");
        // Shared queue is scoped by group, not client.
        assert!(plan.filters[0].queue_name.contains("shared/mygroup"));
        assert!(!plan.filters[0].queue_name.contains("shared-client"));
    }

    #[test]
    fn test_subscribe_with_subscription_id() {
        let mut session = make_session();
        session.handle_connect(&make_connect("subid-client", true));

        let filters = vec![TopicFilter {
            filter: Bytes::from("test/#"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];

        let plan = session.handle_subscribe(1, &filters, Some(42));

        assert_eq!(plan.filters[0].subscription_id, Some(42));
        let mapping = session.subscriptions.get("test/#").unwrap();
        assert_eq!(mapping.subscription_id, Some(42));
    }

    // ---- UNSUBSCRIBE tests ----

    #[test]
    fn test_unsubscribe() {
        let mut session = make_session();
        session.handle_connect(&make_connect("sub-client", true));

        let filters = vec![TopicFilter {
            filter: Bytes::from("sensor/+/data"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);
        assert_eq!(session.subscription_count(), 1);

        session.handle_unsubscribe(2, &["sensor/+/data".to_string()]);
        assert_eq!(session.subscription_count(), 0);

        let (ua, _) = codec::decode_buf::unsuback(&session.out_buf).unwrap();
        assert_eq!(ua.packet_id, 2);
        assert_eq!(ua.reason_codes.as_slice(), &[0x00]);
    }

    // ---- PING tests ----

    #[test]
    fn test_pingreq() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ping-client", true));

        session.handle_pingreq();
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_HEARTBEAT_SESSION
        );
        assert!(!session.out_buf.is_empty());
    }

    // ---- DISCONNECT tests ----

    #[test]
    fn test_disconnect_clean() {
        let mut session = make_session();
        session.handle_connect(&make_connect("dc-client", true));

        let filters = vec![TopicFilter {
            filter: Bytes::from("test/#"),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);

        let will_plan = session.handle_disconnect(None);
        assert!(!session.connected);
        assert!(session.will.is_none());
        assert!(will_plan.is_none());
        assert!(session.cmd_buf.len() >= 1);
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_DISCONNECT_SESSION
        );
        assert_eq!(session.subscription_count(), 0);
    }

    #[test]
    fn test_unclean_disconnect_with_will() {
        let mut session = make_session();

        let will = WillMessage {
            topic: Bytes::from_static(b"client/status"),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties_raw: Vec::new(),
        };

        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: true,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: Bytes::from("will-client"),
            will: Some(will),
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };

        session.handle_connect(&connect);
        assert!(session.will.is_some());

        let will_plan = session.handle_unclean_disconnect();
        assert!(!session.connected);
        assert!(session.will.is_none());
        assert!(will_plan.is_some());

        let plan = will_plan.unwrap();
        assert_eq!(plan.exchange_name, MQTT_EXCHANGE_NAME);
        assert!(plan.has_message());

        // Should have DisconnectSession.
        assert!(session.cmd_buf.len() >= 1);
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_DISCONNECT_SESSION
        );
    }

    #[test]
    fn test_unclean_disconnect_will_preserves_v5_properties() {
        let mut session = make_session();

        let will = WillMessage {
            topic: Bytes::from_static(b"status"),
            payload: Bytes::from_static(b"gone"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties_raw: PropertiesBuilder::new()
                .response_topic("reply/status")
                .correlation_data(b"will-corr")
                .content_type("text/plain")
                .message_expiry_interval(300)
                .will_delay_interval(10)
                .build(),
        };

        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: true,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: Bytes::from("v5-will"),
            will: Some(will),
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };

        session.handle_connect(&connect);
        let will_plan = session.handle_unclean_disconnect();

        let plan = will_plan.unwrap();
        let flat_bytes = plan
            .flat_message
            .as_ref()
            .expect("will plan should have owned flat_message");
        let flat = FlatMessage::new(flat_bytes).expect("valid flat message");

        // Native fields preserved.
        let reply_to = flat.reply_to().expect("should have reply_to");
        assert_eq!(&reply_to[..], b"reply/status");
        let corr_id = flat.correlation_id().expect("should have correlation_id");
        assert_eq!(&corr_id[..], b"will-corr");

        // TTL preserved.
        assert_eq!(flat.ttl_ms(), Some(300_000));

        // Delay preserved.
        assert_eq!(flat.delay_ms(), Some(10_000));
    }

    // ---- QoS 2 outbound state machine tests ----

    #[test]
    fn test_qos2_outbound_full_flow() {
        let mut session = make_session();
        session.handle_connect(&make_connect("q2-client", true));

        // Server delivers a message to client as QoS 2.
        let packet_id = session
            .track_outbound_delivery(QoS::ExactlyOnce, 100, 42)
            .unwrap()
            .unwrap();

        assert_eq!(session.qos2_outbound.len(), 1);

        // Client sends PUBREC.
        session.handle_pubrec(packet_id);
        codec::decode_buf::pubrel(&session.out_buf).unwrap();

        // State should be PubRelSent now.
        assert!(matches!(
            session.qos2_outbound.get(&packet_id),
            Some(QoS2OutboundState::PubRelSent { .. })
        ));

        // Client sends PUBCOMP.
        session.handle_pubcomp(packet_id);

        // Should ACK in bisque-mq.
        assert!(!session.cmd_buf.is_empty(), "should return ACK command");
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_GROUP_ACK
        );
        assert!(session.qos2_outbound.is_empty());
    }

    // ---- Outbound publish tests ----

    #[test]
    fn test_outbound_publish_qos1_tracks_inflight() {
        let mut session = make_session();
        session.handle_connect(&make_connect("out-client", true));

        let result = session.track_outbound_delivery(QoS::AtLeastOnce, 100, 42);
        assert!(result.is_some());
        assert!(result.unwrap().is_some()); // QoS 1 gets a packet_id

        assert_eq!(session.qos1_inflight_count(), 1);

        // PUBACK from client should ACK in bisque-mq.
        session.handle_puback(1);
        assert!(!session.cmd_buf.is_empty(), "should return ACK command");
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_GROUP_ACK
        );
    }

    // ---- Entity cache tests ----

    #[test]
    fn test_entity_cache() {
        let mut session = make_session();

        session.cache_exchange_id("mqtt/exchange", 1);
        assert_eq!(session.cached_exchange_id("mqtt/exchange"), Some(1));

        session.cache_queue_id("mqtt/sub/client/test", 2);
        assert_eq!(session.cached_queue_id("mqtt/sub/client/test"), Some(2));

        session.cache_topic_id("mqtt/test", 3);
        assert_eq!(session.cached_topic_id("mqtt/test"), Some(3));

        session.cache_binding_id(1, "test/#", 4);
        assert_eq!(session.cached_binding_id(1, "test/#"), Some(4));
    }

    // ---- Wildcard translation tests ----

    #[test]
    fn test_wildcard_translation() {
        assert_eq!(
            MqttSession::mqtt_filter_to_mq_pattern("sensor/+/data"),
            "sensor/*/data"
        );
        assert_eq!(
            MqttSession::mqtt_filter_to_mq_pattern("home/+/+/temperature"),
            "home/*/*/temperature"
        );
        assert_eq!(
            MqttSession::mqtt_filter_to_mq_pattern("devices/#"),
            "devices/#"
        );
        assert_eq!(
            MqttSession::mqtt_filter_to_mq_pattern("exact/match"),
            "exact/match"
        );
    }

    // ---- Shared subscription parsing tests ----

    #[test]
    fn test_shared_subscription_parsing() {
        assert_eq!(
            MqttSession::parse_shared_subscription("$share/mygroup/sensor/+/data"),
            Some(("mygroup", "sensor/+/data"))
        );
        assert_eq!(
            MqttSession::parse_shared_subscription("$share/g1/topic"),
            Some(("g1", "topic"))
        );
        assert_eq!(
            MqttSession::parse_shared_subscription("regular/topic"),
            None
        );
        assert_eq!(
            MqttSession::parse_shared_subscription("$share/nofilter"),
            None // No second slash
        );
    }

    // ---- Inflight management tests ----

    #[test]
    fn test_inflight_full_check() {
        let mut session = MqttSession::new(MqttSessionConfig {
            max_inflight: 2,
            ..MqttSessionConfig::default()
        });
        session.handle_connect(&make_connect("full-client", true));

        assert!(!session.is_inflight_full());

        // Add 2 outbound QoS 1 entries via track_outbound_delivery.
        session.track_outbound_delivery(QoS::AtLeastOnce, 0, 1);
        session.track_outbound_delivery(QoS::AtLeastOnce, 0, 2);

        assert!(session.is_inflight_full());
        assert_eq!(session.remaining_inflight(), 0);
    }

    #[test]
    fn test_inflight_respects_receive_maximum() {
        let mut session = MqttSession::new(MqttSessionConfig {
            max_inflight: 100,
            ..MqttSessionConfig::default()
        });
        session.handle_connect(&make_connect("rm-client", true));
        session.client_receive_maximum = 1;

        // Add 1 outbound QoS 1 entry via track_outbound_delivery.
        session.track_outbound_delivery(QoS::AtLeastOnce, 0, 1);

        assert!(session.is_inflight_full());
    }

    #[test]
    fn test_inbound_inflight_not_counted_for_flow_control() {
        let mut session = MqttSession::new(MqttSessionConfig {
            max_inflight: 1,
            ..MqttSessionConfig::default()
        });
        session.handle_connect(&make_connect("inb-client", true));

        // Add inbound QoS 1 — should NOT count toward outbound inflight limit.
        session.qos1_inflight.insert(
            1,
            QoS1InFlight {
                mq_message_id: None,
                mq_queue_id: None,
                direction: Direction::Inbound,
            },
        );

        assert!(!session.is_inflight_full());
    }

    // ---- Packet ID tests ----

    #[test]
    fn test_packet_id_allocation() {
        let mut session = make_session();
        let id1 = session.alloc_packet_id();
        let id2 = session.alloc_packet_id();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_ne!(id1, id2);
    }

    // ---- dispatch tests ----

    #[test]
    fn test_dispatch_connect_and_pingreq() {
        let mut session = make_session();

        session.handle_connect(&make_connect("dispatch-client", true));
        assert!(!session.cmd_buf.is_empty());
        assert!(!session.out_buf.is_empty());
        codec::decode_buf::connack(&session.out_buf).unwrap();

        session.out_buf.clear();
        session.cmd_buf.clear();
        session.handle_pingreq();
        assert_eq!(session.cmd_buf.len(), 1);
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_HEARTBEAT_SESSION
        );
        assert!(!session.out_buf.is_empty());
    }

    // =========================================================================
    // Phase 2: Protocol Validation Tests
    // =========================================================================

    // ---- Client ID handling (M5) ----

    #[test]
    fn test_empty_client_id_v311_clean_generates_id() {
        let mut session = make_session();
        let connect = make_connect("", true);
        session.handle_connect(&connect);

        assert!(session.client_id.starts_with("auto-"));
        assert!(session.connected);
        let (c, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(c.return_code, 0x00);
    }

    #[test]
    fn test_empty_client_id_v311_not_clean_rejected() {
        let mut session = make_session();
        let connect = make_connect("", false);
        session.handle_connect(&connect);

        // Should return IdentifierRejected and no commands
        assert!(session.cmd_buf.is_empty());
        let (c, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(c.return_code, ConnectReturnCode::IdentifierRejected as u8);
    }

    #[test]
    fn test_empty_client_id_v5_generates_id_and_assigns() {
        let mut session = make_session();
        let connect = make_connect_v5("", true);
        session.handle_connect(&connect);

        assert!(session.client_id.starts_with("auto-"));
        assert!(session.connected);
        let (c, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(c.return_code, 0x00);
        assert!(
            c.assigned_client_identifier
                .as_ref()
                .map(|b| unsafe { std::str::from_utf8_unchecked(b) })
                .is_some()
        );
        assert!(
            c.assigned_client_identifier
                .as_ref()
                .map(|b| unsafe { std::str::from_utf8_unchecked(b) })
                .unwrap()
                .starts_with("auto-")
        );
    }

    // ---- Keep-alive=0 for V5 (M16) ----

    #[test]
    fn test_v311_keepalive_zero_gets_default() {
        let mut session = make_session();
        let mut connect = make_connect("ka-client", true);
        connect.keep_alive = 0;
        session.handle_connect(&connect);
        assert_eq!(session.keep_alive, session.config.default_keep_alive);
    }

    #[test]
    fn test_v5_keepalive_zero_stays_zero() {
        let mut session = make_session();
        let mut connect = make_connect_v5("ka-v5", true);
        connect.keep_alive = 0;
        session.handle_connect(&connect);
        assert_eq!(session.keep_alive, 0);
    }

    // ---- QoS 2 duplicate detection (m12) ----

    #[test]
    fn test_qos2_duplicate_publish_skips_message() {
        let mut session = make_session();
        session.handle_connect(&make_connect("dup-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: b"test/dup",
            packet_id: Some(42),
            payload: b"first",
            properties: Properties::default(),
        };

        // First PUBLISH
        let mut msg_buf = BytesMut::new();
        let plan1 = session.handle_publish(&publish, &mut msg_buf);
        assert!(plan1.has_message()); // Message is published
        assert!(!session.out_buf.is_empty());

        // Second PUBLISH with same packet_id (duplicate)
        let dup_publish = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: b"test/dup",
            packet_id: Some(42),
            payload: b"duplicate",
            properties: Properties::default(),
        };

        let mut msg_buf2 = BytesMut::new();
        let plan2 = session.handle_publish(&dup_publish, &mut msg_buf2);
        assert!(!plan2.has_message()); // Message is NOT published
        assert!(!session.out_buf.is_empty()); // But PUBREC is still sent
        let (pubrec, _) = codec::decode_buf::pubrec(&session.out_buf).unwrap();
        assert_eq!(pubrec.packet_id, 42);
    }

    // ---- Topic validation in handlers (task 14) ----

    #[test]
    fn test_publish_invalid_topic_returns_empty_plan() {
        let mut session = make_session();
        session.handle_connect(&make_connect("val-client", true));

        // Topic with wildcard
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"sensor/+/data",
            packet_id: None,
            payload: b"data",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        assert!(!plan.has_message());
    }

    #[test]
    fn test_subscribe_invalid_filter_returns_failure() {
        let mut session = make_session();
        session.handle_connect(&make_connect("sub-val", true));

        let filters = vec![
            crate::types::TopicFilter {
                filter: Bytes::from("sensor/+data"), // Invalid: + not occupying entire level
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
            crate::types::TopicFilter {
                filter: Bytes::from("valid/topic"), // Valid
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
        ];

        let plan = session.handle_subscribe(1, &filters, None);
        let (suback, _) = codec::decode_buf::suback(&session.out_buf).unwrap();
        assert_eq!(suback.return_codes.len(), 2);
        assert_eq!(suback.return_codes[0], 0x80); // Failure for invalid filter
        assert_eq!(suback.return_codes[1], QoS::AtLeastOnce.as_u8()); // Success for valid
    }

    // ---- Phase 3: Packet ID reuse check (M9) ----

    #[test]
    fn test_alloc_packet_id_skips_in_use() {
        let mut session = make_session();
        session.handle_connect(&make_connect("pid-client", true));

        // Manually insert IDs 1, 2, 3 into qos1_inflight.
        for id in 1..=3u16 {
            session.qos1_inflight.insert(
                id,
                QoS1InFlight {
                    mq_message_id: None,
                    mq_queue_id: None,
                    direction: Direction::Outbound,
                },
            );
            session.mark_packet_id(id);
        }
        session.next_packet_id = 1;

        // alloc_packet_id should skip 1, 2, 3 and return 4.
        let id = session.alloc_packet_id();
        assert_eq!(id, 4);
    }

    // ---- Phase 3: QoS downgrade (m8) ----

    #[test]
    fn test_subscribe_qos_downgrade() {
        let mut session = make_session();
        // Set maximum_qos to QoS 1.
        session.config.maximum_qos = QoS::AtLeastOnce;
        session.handle_connect(&make_connect("qos-client", true));

        let filters = vec![TopicFilter {
            filter: Bytes::from("test/topic"),
            qos: QoS::ExactlyOnce, // Request QoS 2
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];

        let plan = session.handle_subscribe(1, &filters, None);
        let (suback, _) = codec::decode_buf::suback(&session.out_buf).unwrap();
        // Should be downgraded to QoS 1.
        assert_eq!(suback.return_codes[0], QoS::AtLeastOnce.as_u8());
        // Subscription mapping should also have the downgraded QoS.
        let mapping = session.subscriptions.get("test/topic").unwrap();
        assert_eq!(mapping.max_qos, QoS::AtLeastOnce);
    }

    // ---- Phase 3: Subscription options stored (M6, M7, M8) ----

    #[test]
    fn test_subscribe_stores_v5_options() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("opts-client", true));

        let filters = vec![TopicFilter {
            filter: Bytes::from("test/topic"),
            qos: QoS::AtLeastOnce,
            no_local: true,
            retain_as_published: true,
            retain_handling: 2,
        }];

        let plan = session.handle_subscribe(1, &filters, None);

        // Verify options stored in mapping.
        let mapping = session.subscriptions.get("test/topic").unwrap();
        assert!(mapping.no_local);
        assert!(mapping.retain_as_published);
        assert_eq!(mapping.retain_handling, 2);

        // Verify options in filter plan.
        assert_eq!(plan.filters[0].retain_handling, 2);
        assert!(plan.filters[0].is_new_subscription);
    }

    #[test]
    fn test_subscribe_existing_filter_not_new() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("exist-client", true));

        let filters = vec![TopicFilter {
            filter: Bytes::from("test/topic"),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 1,
        }];

        // First subscribe: is_new_subscription should be true.
        let plan1 = session.handle_subscribe(1, &filters, None);
        assert!(plan1.filters[0].is_new_subscription);

        // Second subscribe to same filter: is_new_subscription should be false.
        let plan2 = session.handle_subscribe(2, &filters, None);
        assert!(!plan2.filters[0].is_new_subscription);
    }

    // ---- Phase 3 Opt 2: Publisher session ID via FlatMessage.publisher_id ----

    #[test]
    fn test_publish_stores_session_id_in_flat_message() {
        let mut session = make_session();
        session.handle_connect(&make_connect("hdr-client", true));
        let session_id = session.session_id;

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: None,
            payload: b"data",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        let msg_slice = &msg_buf[plan.msg_start..plan.msg_start + plan.msg_len];
        let env = bisque_mq::flat::MqttEnvelope::new(msg_slice).unwrap();

        // Verify publisher_id is set via MqttEnvelope fixed header (O(1) access).
        assert_eq!(env.publisher_id(), session_id);
    }

    // ---- Phase 3 Opt 3: Retain flag via FlatMessage.FLAG_RETAIN ----

    #[test]
    fn test_publish_retained_stores_retain_flag() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ret-hdr", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: b"test/retained",
            packet_id: None,
            payload: b"data",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        let msg_slice = &msg_buf[plan.msg_start..plan.msg_start + plan.msg_len];
        let env = bisque_mq::flat::MqttEnvelope::new(msg_slice).unwrap();

        // Verify retain flag via MqttEnvelope FLAG_RETAIN bit (O(1) access).
        assert!(env.is_retain());
    }

    #[test]
    fn test_publish_non_retained_does_not_set_retain_flag() {
        let mut session = make_session();
        session.handle_connect(&make_connect("no-ret", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: None,
            payload: b"data",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        let msg_slice = &msg_buf[plan.msg_start..plan.msg_start + plan.msg_len];
        let env = bisque_mq::flat::MqttEnvelope::new(msg_slice).unwrap();
        assert!(!env.is_retain());
    }

    // ---- Phase 3: Topic alias new vs existing ----

    #[test]
    fn test_resolve_outbound_topic_alias_new_vs_existing() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("alias-client", true));
        // max_topic_alias is set from CONNECT properties (10).

        // First call: new alias.
        let result1 = session.resolve_outbound_topic_alias(b"sensor/1/temp");
        assert!(result1.is_some());
        let (alias1, is_new1) = result1.unwrap();
        assert!(is_new1);
        assert_eq!(alias1, 1);

        // Second call: existing alias.
        let result2 = session.resolve_outbound_topic_alias(b"sensor/1/temp");
        assert!(result2.is_some());
        let (alias2, is_new2) = result2.unwrap();
        assert!(!is_new2);
        assert_eq!(alias2, alias1);

        // Different topic: new alias.
        let result3 = session.resolve_outbound_topic_alias(b"sensor/2/temp");
        assert!(result3.is_some());
        let (alias3, is_new3) = result3.unwrap();
        assert!(is_new3);
        assert_eq!(alias3, 2);
    }

    // ---- Phase 4: Will message on V5 DISCONNECT (M11) ----

    #[test]
    fn test_v5_disconnect_normal_clears_will() {
        let mut session = make_session();
        let mut connect = make_connect_v5("will-client", true);
        connect.flags.will = true;
        connect.will = Some(WillMessage {
            topic: Bytes::from_static(b"will/topic"),
            payload: Bytes::from_static(b"goodbye"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties_raw: Vec::new(),
        });
        session.handle_connect(&connect);
        assert!(session.will.is_some());

        // Normal DISCONNECT (reason 0x00): will should be cleared.
        let disconnect = crate::types::Disconnect {
            reason_code: Some(0x00),
            reason_string: None,
            session_expiry_interval: None,
            server_reference: None,
        };
        let will_plan = session.handle_disconnect(Some(&disconnect));
        assert!(will_plan.is_none());
        assert!(session.will.is_none());
    }

    #[test]
    fn test_v5_disconnect_with_will_publishes_will() {
        let mut session = make_session();
        let mut connect = make_connect_v5("will-client", true);
        connect.flags.will = true;
        connect.will = Some(WillMessage {
            topic: Bytes::from_static(b"will/topic"),
            payload: Bytes::from_static(b"goodbye"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties_raw: Vec::new(),
        });
        session.handle_connect(&connect);
        assert!(session.will.is_some());

        // DISCONNECT with reason 0x04 (Disconnect with Will Message): will should be published.
        let disconnect = crate::types::Disconnect {
            reason_code: Some(0x04),
            reason_string: None,
            session_expiry_interval: None,
            server_reference: None,
        };
        let will_plan = session.handle_disconnect(Some(&disconnect));
        assert!(will_plan.is_some());
        let plan = will_plan.unwrap();
        assert!(plan.has_message());
    }

    #[test]
    fn test_v311_disconnect_always_clears_will() {
        let mut session = make_session();
        let mut connect = make_connect("will-v3", true);
        connect.flags.will = true;
        connect.will = Some(WillMessage {
            topic: Bytes::from_static(b"will/topic"),
            payload: Bytes::from_static(b"goodbye"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties_raw: Vec::new(),
        });
        session.handle_connect(&connect);
        assert!(session.will.is_some());

        // V3.1.1 has no reason code — will is always cleared on DISCONNECT.
        let will_plan = session.handle_disconnect(None);
        assert!(will_plan.is_none());
        assert!(session.will.is_none());
    }

    // ---- Enhanced Authentication (Phase 5) ----

    /// Test auth provider that requires one challenge-response round.
    struct TestChallengeAuthProvider;

    impl crate::auth::AuthProvider for TestChallengeAuthProvider {
        fn supports_method(&self, method: &str) -> bool {
            method == "TEST-CHALLENGE"
        }

        fn authenticate_connect(
            &self,
            _method: &str,
            _client_id: &str,
            _username: Option<&str>,
            _password: Option<&[u8]>,
            _auth_data: Option<&[u8]>,
        ) -> crate::auth::AuthResult {
            crate::auth::AuthResult::Continue {
                authentication_data: Bytes::from_static(b"server-challenge"),
            }
        }

        fn authenticate_continue(
            &self,
            _method: &str,
            _client_id: &str,
            auth_data: Option<&[u8]>,
            _step: u32,
        ) -> crate::auth::AuthResult {
            if auth_data == Some(b"correct-response") {
                crate::auth::AuthResult::Success
            } else {
                crate::auth::AuthResult::Failed {
                    reason_code: 0x86,
                    reason_string: Some("bad response".into()),
                }
            }
        }
    }

    #[test]
    fn test_enhanced_auth_connect_sends_challenge() {
        let provider: Arc<dyn crate::auth::AuthProvider> = Arc::new(TestChallengeAuthProvider);
        let mut session = MqttSession::with_auth_provider(Default::default(), provider);

        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("auth-client"),
            will: None,
            username: None,
            password: None,
            properties_raw: PropertiesBuilder::new()
                .authentication_method("TEST-CHALLENGE")
                .build(),
        };

        session.handle_connect(&connect);
        // Should return AUTH challenge, not CONNACK.
        assert!(
            session.cmd_buf.is_empty(),
            "no commands until auth completes"
        );
        let (auth, _) = codec::decode_buf::auth(&session.out_buf).unwrap();
        assert_eq!(
            auth.reason_code,
            crate::types::Auth::CONTINUE_AUTHENTICATION
        );
        assert_eq!(
            auth.authentication_data.as_deref(),
            Some(b"server-challenge".as_slice())
        );
        assert!(!session.connected);
    }

    #[test]
    fn test_enhanced_auth_success_after_response() {
        let provider: Arc<dyn crate::auth::AuthProvider> = Arc::new(TestChallengeAuthProvider);
        let mut session = MqttSession::with_auth_provider(Default::default(), provider);

        // Start auth flow.
        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("auth-client"),
            will: None,
            username: None,
            password: None,
            properties_raw: PropertiesBuilder::new()
                .authentication_method("TEST-CHALLENGE")
                .build(),
        };
        session.handle_connect(&connect);

        // Send correct response.
        let auth_pkt = crate::types::Auth {
            reason_code: crate::types::Auth::CONTINUE_AUTHENTICATION,
            authentication_method: Some(Bytes::from("TEST-CHALLENGE")),
            authentication_data: Some(Bytes::from_static(b"correct-response")),
            reason_string: None,
        };
        session.handle_auth(&auth_pkt);

        // Should get CONNACK success + registration commands.
        assert!(
            !session.cmd_buf.is_empty(),
            "should have registration commands"
        );
        assert!(!session.out_buf.is_empty());
        let (connack, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(connack.return_code, 0x00); // Accepted
        assert_eq!(
            connack
                .authentication_method
                .as_ref()
                .map(|b| unsafe { std::str::from_utf8_unchecked(b) }),
            Some("TEST-CHALLENGE")
        );
        assert!(session.connected);
    }

    #[test]
    fn test_enhanced_auth_failure_after_bad_response() {
        let provider: Arc<dyn crate::auth::AuthProvider> = Arc::new(TestChallengeAuthProvider);
        let mut session = MqttSession::with_auth_provider(Default::default(), provider);

        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("auth-client"),
            will: None,
            username: None,
            password: None,
            properties_raw: PropertiesBuilder::new()
                .authentication_method("TEST-CHALLENGE")
                .build(),
        };
        session.handle_connect(&connect);

        // Send wrong response.
        let auth_pkt = crate::types::Auth {
            reason_code: crate::types::Auth::CONTINUE_AUTHENTICATION,
            authentication_method: Some(Bytes::from("TEST-CHALLENGE")),
            authentication_data: Some(Bytes::from_static(b"wrong-response")),
            reason_string: None,
        };
        session.handle_auth(&auth_pkt);

        assert!(session.cmd_buf.is_empty());
        assert!(!session.out_buf.is_empty());
        let (connack, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(connack.return_code, 0x86); // Bad credentials
        assert!(!session.connected);
    }

    #[test]
    fn test_enhanced_auth_unsupported_method_rejected() {
        let provider: Arc<dyn crate::auth::AuthProvider> = Arc::new(TestChallengeAuthProvider);
        let mut session = MqttSession::with_auth_provider(Default::default(), provider);

        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("auth-client"),
            will: None,
            username: None,
            password: None,
            properties_raw: PropertiesBuilder::new()
                .authentication_method("UNKNOWN-METHOD")
                .build(),
        };

        session.handle_connect(&connect);
        assert!(session.cmd_buf.is_empty());
        let (connack, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(connack.return_code, 0x8C); // Bad authentication method
    }

    #[test]
    fn test_auth_without_flow_in_progress_disconnects() {
        let mut session = MqttSession::new(Default::default());
        let auth_pkt = crate::types::Auth {
            reason_code: crate::types::Auth::CONTINUE_AUTHENTICATION,
            authentication_method: None,
            authentication_data: None,
            reason_string: None,
        };
        session.handle_auth(&auth_pkt);
        assert!(!session.out_buf.is_empty());
        // Should get a DISCONNECT
        let (d, _) = codec::decode_buf::disconnect(&session.out_buf).unwrap();
        assert_eq!(d.reason_code, Some(0x82)); // Protocol Error
    }

    // ---- Gap fix tests ----

    #[test]
    fn test_publish_qos_exceeds_max_rejected() {
        let mut config = MqttSessionConfig::default();
        config.maximum_qos = QoS::AtLeastOnce; // Server only supports QoS 0 and 1
        let mut session = MqttSession::new(config);

        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        session.handle_connect(&connect);

        // Try publishing QoS 2 (server max is QoS 1)
        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: Some(1),
            payload: b"hello",
            properties: Properties::default(),
        };
        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        assert!(!plan.has_message(), "message should not be published");
        assert!(!session.out_buf.is_empty());
        // Should get PUBREC with reason code 0x9B (QoS not supported)
        let (pubrec, _) = codec::decode_buf::pubrec(&session.out_buf).unwrap();
        assert_eq!(pubrec.reason_code, Some(0x9B));
    }

    #[test]
    fn test_publish_payload_format_utf8_validation() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        session.handle_connect(&connect);

        // Publish with payload_format_indicator=1 but invalid UTF-8 payload
        let props_raw = PropertiesBuilder::new().payload_format_indicator(1).build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: Some(1),
            payload: &[0xFF, 0xFE, 0xFD], // invalid UTF-8
            properties: Properties::from_raw(&props_raw),
        };
        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        assert!(!plan.has_message());
        assert!(!session.out_buf.is_empty());
        let (puback, _) = codec::decode_buf::puback(&session.out_buf).unwrap();
        assert_eq!(puback.reason_code, Some(0x99)); // Payload Format Invalid
    }

    #[test]
    fn test_publish_payload_format_valid_utf8_passes() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        session.handle_connect(&connect);

        let props_raw = PropertiesBuilder::new().payload_format_indicator(1).build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: None,
            payload: b"valid utf-8 string",
            properties: Properties::from_raw(&props_raw),
        };
        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        assert!(plan.has_message(), "valid UTF-8 should be published");
    }

    #[test]
    fn test_topic_alias_zero_rejected_in_publish() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        session.handle_connect(&connect);

        let props_raw = PropertiesBuilder::new()
            .topic_alias(0) // Invalid: 0 is not allowed
            .build();
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: None,
            payload: b"hello",
            properties: Properties::from_raw(&props_raw),
        };
        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        assert!(!plan.has_message(), "topic alias 0 should be rejected");
        // MQTT 5.0: topic alias 0 should trigger a disconnect with Protocol Error.
        assert!(
            plan.need_disconnect,
            "topic alias 0 should trigger disconnect"
        );
    }

    #[test]
    fn test_find_subscription_id_for_queue() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        session.handle_connect(&connect);

        // Subscribe with subscription ID
        let filters: smallvec::SmallVec<[crate::types::TopicFilter; 4]> =
            smallvec::smallvec![crate::types::TopicFilter {
                filter: Bytes::from("test/+"),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            }];
        let _plan = session.handle_subscribe(1, &filters, Some(42));
        // Update subscription with a queue_id
        session.update_subscription_ids("test/+", None, None, Some(100), None);

        // Should find sub ID for queue 100
        assert_eq!(session.find_subscription_id_for_queue(100), Some(42));
        // Should not find for unknown queue
        assert_eq!(session.find_subscription_id_for_queue(999), None);
    }

    #[test]
    fn test_v5_pubrec_pubrel_pubcomp_include_reason_codes() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        session.handle_connect(&connect);

        // PUBREC for unknown ID should return reason code 0x92
        session.handle_pubrec(999);
        let (pr, _) = codec::decode_buf::pubrel(&session.out_buf).unwrap();
        assert_eq!(pr.reason_code, Some(0x92));

        // PUBREL for unknown ID should return PUBCOMP with 0x92
        session.out_buf.clear();
        session.handle_pubrel(888);
        let (pc, _) = codec::decode_buf::pubcomp(&session.out_buf).unwrap();
        assert_eq!(pc.reason_code, Some(0x92));
    }

    #[test]
    fn test_restore_subscriptions() {
        let mut session = MqttSession::new(Default::default());
        let connect = make_connect_v5("restore-test", false);
        session.handle_connect(&connect);

        let persisted = vec![
            crate::session_store::PersistedSubscription {
                filter: "sensor/+/data".to_string(),
                max_qos: QoS::AtLeastOnce,
                subscription_id: Some(10),
                no_local: true,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: None,
            },
            crate::session_store::PersistedSubscription {
                filter: "alerts/#".to_string(),
                max_qos: QoS::ExactlyOnce,
                subscription_id: None,
                no_local: false,
                retain_as_published: true,
                retain_handling: 1,
                shared_group: None,
            },
        ];

        let plans = session.restore_subscriptions(&persisted);
        assert_eq!(plans.len(), 2);
        assert_eq!(session.subscription_count(), 2);

        // Verify subscription properties were restored.
        let (_, mapping) = session
            .subscriptions_iter()
            .find(|(k, _)| *k == "sensor/+/data")
            .expect("subscription not found");
        assert_eq!(mapping.subscription_id, Some(10));
        assert!(mapping.no_local);
    }

    #[test]
    fn test_request_response_information() {
        let mut session = MqttSession::new(Default::default());
        let mut connect = make_connect_v5("rri-test", true);
        // Rebuild properties with request_response_information=1 included.
        {
            let mut raw = Vec::new();
            // Existing properties from make_connect_v5
            raw.extend_from_slice(connect.properties().raw());
            // Add request_response_information = 1 (property ID 0x19)
            raw.push(0x19);
            raw.push(1);
            connect.properties_raw = raw;
        }
        session.handle_connect(&connect);
        let (ca, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert!(
            ca.response_information.is_some(),
            "expected response_information in CONNACK"
        );
        let ri = ca.response_information.clone().unwrap();
        let ri_str = std::str::from_utf8(&ri).unwrap();
        assert!(
            ri_str.contains("rri-test"),
            "response_information should contain client_id"
        );
    }

    #[test]
    fn test_request_response_information_not_requested() {
        let mut session = MqttSession::new(Default::default());
        let connect = make_connect_v5("rri-test", true);
        session.handle_connect(&connect);
        let (ca, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert!(
            ca.response_information.is_none(),
            "should not include response_information unless requested"
        );
    }

    // =========================================================================
    // Additional coverage tests
    // =========================================================================

    #[test]
    fn test_puback_inbound_direction() {
        // When client publishes QoS 1, server tracks as Inbound.
        // PUBACK for an inbound entry should return None (no ACK command needed).
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Simulate an inbound QoS 1 publish (client -> server).
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: Some(1),
            payload: b"hi",
            properties: Properties::default(),
        };
        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        // handle_publish tracks QoS1 inbound in qos1_inflight and returns a PUBACK response.
        assert!(!session.out_buf.is_empty());
        codec::decode_buf::puback(&session.out_buf).unwrap();

        // Now handle the PUBACK for the inbound message (this is the server's own ack).
        // For inbound messages, handle_puback should not return an MqCommand.
        session.handle_puback(1);
        assert!(session.cmd_buf.is_empty());
    }

    #[test]
    fn test_puback_unknown_packet_id() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));
        // PUBACK for a packet_id that was never sent.
        session.handle_puback(999);
        assert!(session.cmd_buf.is_empty());
    }

    #[test]
    fn test_puback_outbound_returns_ack_command() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Simulate an outbound QoS 1 delivery.
        let packet_id = session
            .track_outbound_delivery(QoS::AtLeastOnce, 42, 100)
            .unwrap()
            .unwrap();

        session.handle_puback(packet_id);
        assert!(
            !session.cmd_buf.is_empty(),
            "outbound PUBACK should produce an ACK command"
        );
    }

    #[test]
    fn test_pubrec_duplicate_in_pubrel_sent_state() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Manually set up a QoS 2 outbound in PublishSent state.
        let packet_id = session
            .track_outbound_delivery(QoS::ExactlyOnce, 10, 20)
            .unwrap()
            .unwrap();

        // First PUBREC transitions to PubRelSent.
        session.handle_pubrec(packet_id);
        // Duplicate PUBREC while already in PubRelSent state.
        session.out_buf.clear();
        session.handle_pubrec(packet_id);
        // Should still return a PUBREL.
        codec::decode_buf::pubrel(&session.out_buf).unwrap();
    }

    #[test]
    fn test_pubrec_unknown_packet_id() {
        let mut session = make_session();
        let mut connect = make_connect_v5("c", true);
        connect.flags.clean_session = true;
        session.handle_connect(&connect);

        session.handle_pubrec(999);
        // V5 should return reason_code 0x92 (Packet Identifier Not Found).
        let (pr, _) = codec::decode_buf::pubrel(&session.out_buf).unwrap();
        assert_eq!(pr.reason_code, Some(0x92));
    }

    #[test]
    fn test_pubrel_unknown_packet_id() {
        let mut session = make_session();
        let connect = make_connect_v5("c", true);
        session.handle_connect(&connect);

        session.handle_pubrel(999);
        // V5 should return reason_code 0x92 (Packet Identifier Not Found).
        let (pc, _) = codec::decode_buf::pubcomp(&session.out_buf).unwrap();
        assert_eq!(pc.reason_code, Some(0x92));
    }

    #[test]
    fn test_pubrel_duplicate_in_complete_state() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Simulate inbound QoS 2 flow.
        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: b"qos2/in",
            packet_id: Some(50),
            payload: b"data",
            properties: Properties::default(),
        };
        let mut msg_buf = BytesMut::new();
        let _plan = session.handle_publish(&publish, &mut msg_buf);
        // PUBREL completes the transaction.
        session.handle_pubrel(50);
        // Duplicate PUBREL should still succeed (already Complete state).
        session.out_buf.clear();
        session.handle_pubrel(50);
        codec::decode_buf::pubcomp(&session.out_buf).unwrap();
    }

    #[test]
    fn test_pubcomp_protocol_violation_publish_sent_state() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Set up outbound QoS 2 in PublishSent state.
        let packet_id = session
            .track_outbound_delivery(QoS::ExactlyOnce, 1, 2)
            .unwrap()
            .unwrap();

        // Send PUBCOMP without PUBREC first (protocol violation).
        session.handle_pubcomp(packet_id);
        assert!(
            session.cmd_buf.is_empty(),
            "PUBCOMP in PublishSent should not produce ACK"
        );
    }

    #[test]
    fn test_pubcomp_unknown_packet_id() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));
        session.handle_pubcomp(999);
        assert!(session.cmd_buf.is_empty());
    }

    #[test]
    fn test_clean_session_false_preserves_subscriptions() {
        let mut session = make_session();
        // First connect with clean_session=false.
        let connect = make_connect("persist", false);
        session.handle_connect(&connect);

        // Subscribe to a topic.
        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("a/b"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);
        assert!(session.subscriptions.contains_key("a/b"));

        // Disconnect.
        session.handle_disconnect(None);

        // Reconnect with clean_session=false — subscriptions should persist.
        let connect2 = make_connect("persist", false);
        session.handle_connect(&connect2);
        assert!(
            session.subscriptions.contains_key("a/b"),
            "subscriptions should persist across reconnect with clean_session=false"
        );
    }

    #[test]
    fn test_clean_session_true_clears_subscriptions() {
        let mut session = make_session();
        let connect = make_connect("cleaner", false);
        session.handle_connect(&connect);

        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("x/y"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);
        assert!(session.subscriptions.contains_key("x/y"));

        // Reconnect with clean_session=true — subscriptions should be cleared.
        let connect2 = make_connect("cleaner", true);
        session.handle_connect(&connect2);
        assert!(
            !session.subscriptions.contains_key("x/y"),
            "subscriptions should be cleared on clean_session=true"
        );
    }

    #[test]
    fn test_disconnect_clean_session_false_preserves_state() {
        let mut session = make_session();
        let connect = make_connect("persist", false);
        session.handle_connect(&connect);

        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("keep/me"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);

        let will_plan = session.handle_disconnect(Some(&Disconnect {
            reason_code: None,
            reason_string: None,
            session_expiry_interval: None,
            server_reference: None,
        }));

        assert!(will_plan.is_none());
        // With clean_session=false, no delete_binding or delete_queue commands.
        let has_delete = (0..session.cmd_buf.len()).any(|i| {
            let cmd = session.cmd_buf.to_command(i);
            cmd.tag() == bisque_mq::types::MqCommand::TAG_DELETE_BINDING
                || cmd.tag() == bisque_mq::types::MqCommand::TAG_DELETE_CONSUMER_GROUP
        });
        assert!(
            !has_delete,
            "clean_session=false should not delete bindings/queues"
        );
        assert!(session.subscriptions.contains_key("keep/me"));
    }

    #[test]
    fn test_update_subscription_ids() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("upd/test"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, Some(7));

        // Update with resolved IDs.
        session.update_subscription_ids("upd/test", Some(10), Some(20), Some(30), Some(40));

        let mapping = session.subscriptions.get("upd/test").unwrap();
        assert_eq!(mapping.exchange_id, Some(10));
        assert_eq!(mapping.binding_id, Some(20));
        assert_eq!(mapping.queue_id, Some(30));
        assert_eq!(mapping.topic_id, Some(40));

        // queue_to_sub_id reverse index should be updated.
        assert_eq!(session.find_subscription_id_for_queue(30), Some(7));
    }

    #[test]
    fn test_update_subscription_ids_nonexistent_filter() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));
        // Updating a non-existent filter is a no-op.
        session.update_subscription_ids("no/such/filter", Some(1), Some(2), Some(3), Some(4));
        assert!(session.subscriptions.is_empty());
    }

    #[test]
    fn test_has_no_local_for_queue() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("c", true));

        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("local/test"),
            qos: QoS::AtLeastOnce,
            no_local: true,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);
        session.update_subscription_ids("local/test", None, None, Some(100), None);

        assert!(session.has_no_local_for_queue(100));
        assert!(!session.has_no_local_for_queue(999));
    }

    #[test]
    fn test_has_retain_as_published_for_queue() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("c", true));

        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("retain/test"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: true,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);
        session.update_subscription_ids("retain/test", None, None, Some(200), None);

        assert!(session.has_retain_as_published_for_queue(200));
        assert!(!session.has_retain_as_published_for_queue(999));
    }

    #[test]
    fn test_topic_alias_limit_exceeded() {
        let mut session = make_session();
        let mut connect = make_connect_v5("c", true);
        connect.properties_raw = PropertiesBuilder::new()
            .topic_alias_maximum(2)
            .receive_maximum(100)
            .session_expiry_interval(3600)
            .build();
        session.handle_connect(&connect);

        // First two aliases should succeed.
        let r1 = session.resolve_outbound_topic_alias(b"topic/1");
        assert!(r1.is_some());
        assert_eq!(r1.unwrap().1, true); // is_new

        let r2 = session.resolve_outbound_topic_alias(b"topic/2");
        assert!(r2.is_some());
        assert_eq!(r2.unwrap().1, true); // is_new

        // Third unique topic should fail (limit exceeded).
        let r3 = session.resolve_outbound_topic_alias(b"topic/3");
        assert!(r3.is_none(), "should return None when alias limit exceeded");

        // Existing alias should still work.
        let r1_again = session.resolve_outbound_topic_alias(b"topic/1");
        assert!(r1_again.is_some());
        assert_eq!(r1_again.unwrap().1, false); // is_new = false (existing)
    }

    #[test]
    fn test_session_present_flag() {
        let mut session = make_session();
        // First connect with clean_session=false.
        let connect = make_connect("sp", false);
        session.handle_connect(&connect);
        let (ca, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        // session_present is !clean_session, so true on first connect too.
        assert!(ca.session_present);
    }

    #[test]
    fn test_session_present_false_on_clean() {
        let mut session = make_session();
        let connect = make_connect("sp", true);
        session.handle_connect(&connect);
        let (ca, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert!(!ca.session_present);
    }

    #[test]
    fn test_unclean_disconnect_nack_batching() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Build multiple outbound QoS 1 deliveries.
        for i in 0..3u64 {
            session.track_outbound_delivery(QoS::AtLeastOnce, 42, 100 + i);
        }

        let _ = session.handle_unclean_disconnect();
        // Should have disconnect_consumer, disconnect_producer, and NACK commands.
        let nack_count = (0..session.cmd_buf.len())
            .filter(|&i| {
                session.cmd_buf.to_command(i).tag() == bisque_mq::types::MqCommand::TAG_GROUP_NACK
            })
            .count();
        assert!(nack_count >= 1, "should have at least one NACK command");
    }

    #[test]
    fn test_track_outbound_delivery_qos0() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        let result = session.track_outbound_delivery(QoS::AtMostOnce, 1, 1);
        assert_eq!(result, Some(None)); // QoS 0 = no packet_id
    }

    #[test]
    fn test_track_outbound_delivery_qos1() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        let result = session.track_outbound_delivery(QoS::AtLeastOnce, 10, 20);
        assert!(result.is_some());
        let packet_id = result.unwrap();
        assert!(packet_id.is_some());
        assert_eq!(session.outbound_inflight_count(), 1);
    }

    #[test]
    fn test_track_outbound_delivery_qos2() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        let result = session.track_outbound_delivery(QoS::ExactlyOnce, 10, 20);
        assert!(result.is_some());
        let packet_id = result.unwrap();
        assert!(packet_id.is_some());
    }

    #[test]
    fn test_track_outbound_delivery_inflight_full() {
        let config = MqttSessionConfig {
            max_inflight: 1,
            ..Default::default()
        };
        let mut session = MqttSession::new(config);
        session.handle_connect(&make_connect("c", true));

        // First delivery succeeds.
        let r1 = session.track_outbound_delivery(QoS::AtLeastOnce, 1, 1);
        assert!(r1.is_some());

        // Second delivery should be rejected (inflight full).
        let r2 = session.track_outbound_delivery(QoS::AtLeastOnce, 2, 2);
        assert!(r2.is_none());
    }

    #[test]
    fn test_pending_qos1_packet_ids() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        session.track_outbound_delivery(QoS::AtLeastOnce, 1, 2);
        let ids = session.pending_qos1_packet_ids();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn test_pending_qos2_packet_ids() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        session.track_outbound_delivery(QoS::ExactlyOnce, 1, 2);
        let ids = session.pending_qos2_packet_ids();
        assert_eq!(ids.len(), 1);
    }

    // test_process_packet_unexpected_server_packets removed: process_packet no longer exists.
    // Server-originated packets are handled at the codec/server layer.

    #[test]
    fn test_subscription_queue_name_format() {
        let session = {
            let mut s = make_session();
            s.handle_connect(&make_connect("my-client", true));
            s
        };
        let name = session.subscription_queue_name("a/b/c", None);
        assert_eq!(name, "mqtt/sub/my-client/a.b.c");
    }

    #[test]
    fn test_subscription_queue_name_shared() {
        let session = {
            let mut s = make_session();
            s.handle_connect(&make_connect("c", true));
            s
        };
        let name = session.subscription_queue_name("x/y", Some("grp1"));
        assert_eq!(name, "mqtt/shared/grp1/x.y");
    }

    #[test]
    fn test_retained_topic_name_format() {
        let session = {
            let mut s = make_session();
            s.handle_connect(&make_connect("c", true));
            s
        };
        let name = session.retained_topic_name("test/topic");
        assert_eq!(name, "$mqtt/retained/test/topic");
    }

    #[test]
    fn test_mqtt_filter_to_mq_pattern_no_plus() {
        // Fast path: no '+' present.
        assert_eq!(MqttSession::mqtt_filter_to_mq_pattern("a/b/c"), "a/b/c");
        assert_eq!(
            MqttSession::mqtt_filter_to_mq_pattern("devices/#"),
            "devices/#"
        );
    }

    #[test]
    fn test_session_expiry_interval_stored() {
        let mut session = make_session();
        let connect = make_connect_v5("c", true);
        session.handle_connect(&connect);
        assert_eq!(session.session_expiry_interval, 3600);
    }

    #[test]
    fn test_client_receive_maximum_stored() {
        let mut session = make_session();
        let mut connect = make_connect_v5("c", true);
        connect.properties_raw = PropertiesBuilder::new()
            .topic_alias_maximum(10)
            .receive_maximum(50)
            .session_expiry_interval(3600)
            .build();
        session.handle_connect(&connect);
        // remaining_inflight should respect client_receive_maximum.
        assert!(session.remaining_inflight() <= 50);
    }

    #[test]
    fn test_unclean_disconnect_without_will() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        let will_plan = session.handle_unclean_disconnect();
        assert!(will_plan.is_none());
        // Should have disconnect_session command.
        assert!(session.cmd_buf.len() >= 1);
        assert_eq!(
            session.cmd_buf.to_command(0).tag(),
            MqCommand::TAG_DISCONNECT_SESSION
        );
        assert!(!session.connected);
    }

    #[test]
    fn test_v5_pubrec_v311_no_reason_code() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true)); // V3.1.1

        let packet_id = session
            .track_outbound_delivery(QoS::ExactlyOnce, 1, 2)
            .unwrap()
            .unwrap();

        session.handle_pubrec(packet_id);
        let (pr, _) = codec::decode_buf::pubrel(&session.out_buf).unwrap();
        // V3.1.1 should not include reason_code.
        assert_eq!(pr.reason_code, None);
    }

    // =========================================================================
    // MQTT 3.1.1 Compliance Tests
    // =========================================================================

    // ---- Session Present flag [MQTT-3.2.2-1 through 3.2.2-4] ----

    #[test]
    fn test_session_present_zero_on_identifier_rejected() {
        // MQTT-3.2.2-4: Non-zero return code must have Session Present = 0.
        let mut session = make_session();
        let connect = make_connect("", false); // empty ClientId + clean=false
        session.handle_connect(&connect);
        let (ca, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(ca.return_code, ConnectReturnCode::IdentifierRejected as u8);
        assert!(
            !ca.session_present,
            "Session Present must be 0 on rejection"
        );
    }

    // ---- Pending retransmits on session resumption [MQTT-4.4.0-1] ----

    #[test]
    fn test_pending_retransmits_pubrel_for_qos2_in_pubrel_sent_state() {
        let mut session = make_session();
        session.handle_connect(&make_connect("retrans", false));

        // Build outbound QoS 2, then process PUBREC to get into PubRelSent state.
        let packet_id = session
            .track_outbound_delivery(QoS::ExactlyOnce, 100, 42)
            .unwrap()
            .unwrap();

        // PUBREC transitions to PubRelSent state.
        session.handle_pubrec(packet_id);
        session.out_buf.clear();

        // pending_retransmits should include the PUBREL.
        let count = session.pending_retransmits();
        assert_eq!(count, 1);
        let (pr, _) = codec::decode_buf::pubrel(&session.out_buf).unwrap();
        assert_eq!(pr.packet_id, packet_id);
        // V3.1.1: no reason code.
        assert_eq!(pr.reason_code, None);
    }

    #[test]
    fn test_pending_retransmits_empty_when_no_inflight() {
        let mut session = make_session();
        session.handle_connect(&make_connect("empty-retrans", true));
        let count = session.pending_retransmits();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_pending_retransmits_v5_includes_reason_code() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("retrans-v5", false));

        let packet_id = session
            .track_outbound_delivery(QoS::ExactlyOnce, 100, 42)
            .unwrap()
            .unwrap();

        session.handle_pubrec(packet_id);
        session.out_buf.clear();

        let count = session.pending_retransmits();
        assert_eq!(count, 1);
        let (pr, _) = codec::decode_buf::pubrel(&session.out_buf).unwrap();
        // V5: success (0x00) with no properties is omitted on wire per SS 3.6.2.1.
        // Decoded as None, semantically equivalent to 0x00.
        assert!(pr.reason_code.is_none() || pr.reason_code == Some(0x00));
    }

    // ---- In-flight QoS completes after unsubscribe [MQTT-3.10.4-3] ----

    #[test]
    fn test_puback_succeeds_after_unsubscribe() {
        let mut session = make_session();
        session.handle_connect(&make_connect("unsub-inflight", true));

        // Simulate outbound QoS 1 delivery.
        let packet_id = session
            .track_outbound_delivery(QoS::AtLeastOnce, 42, 100)
            .unwrap()
            .unwrap();

        // Subscribe and then unsubscribe.
        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("topic/a"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);

        let unsub_filters: smallvec::SmallVec<[String; 4]> =
            smallvec::smallvec!["topic/a".to_string()];
        session.handle_unsubscribe(2, &unsub_filters);

        // PUBACK should still work (in-flight must complete).
        session.handle_puback(packet_id);
        assert!(
            !session.cmd_buf.is_empty(),
            "PUBACK must succeed even after unsubscribe"
        );
    }

    // ---- QoS 2 full inbound flow [MQTT-4.3.3-2] ----

    #[test]
    fn test_qos2_inbound_full_flow() {
        let mut session = make_session();
        session.handle_connect(&make_connect("q2-in", true));

        // Client sends QoS 2 PUBLISH.
        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: b"qos2/inbound",
            packet_id: Some(10),
            payload: b"exactly once",
            properties: Properties::default(),
        };
        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);

        // Server responds with PUBREC.
        assert!(!session.out_buf.is_empty());
        let (pr, _) = codec::decode_buf::pubrec(&session.out_buf).unwrap();
        assert_eq!(pr.packet_id, 10);

        // Client sends PUBREL.
        session.out_buf.clear();
        session.handle_pubrel(10);
        let (pc, _) = codec::decode_buf::pubcomp(&session.out_buf).unwrap();
        assert_eq!(pc.packet_id, 10);
    }

    // ---- Will message handling [MQTT-3.1.2-8 through 3.1.2-12] ----

    #[test]
    fn test_will_not_published_on_clean_disconnect() {
        let mut session = make_session();
        let mut connect = make_connect("will-clean", true);
        connect.flags.will = true;
        connect.will = Some(WillMessage {
            topic: Bytes::from_static(b"status"),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties_raw: Vec::new(),
        });
        session.handle_connect(&connect);
        assert!(session.will.is_some());

        // Clean DISCONNECT should NOT publish will.
        let will_plan = session.handle_disconnect(None);
        assert!(
            will_plan.is_none(),
            "will should not be published on clean disconnect"
        );
        assert!(
            session.will.is_none(),
            "will should be cleared after disconnect"
        );
    }

    #[test]
    fn test_will_published_on_unclean_disconnect() {
        let mut session = make_session();
        let mut connect = make_connect("will-unclean", true);
        connect.flags.will = true;
        connect.flags.will_retain = true;
        connect.flags.will_qos = QoS::AtLeastOnce;
        connect.will = Some(WillMessage {
            topic: Bytes::from_static(b"clients/status"),
            payload: Bytes::from_static(b"gone"),
            qos: QoS::AtLeastOnce,
            retain: true,
            properties_raw: Vec::new(),
        });
        session.handle_connect(&connect);

        let will_plan = session.handle_unclean_disconnect();
        assert!(
            will_plan.is_some(),
            "will must be published on unclean disconnect"
        );

        let plan = will_plan.unwrap();
        assert!(plan.has_message());
        assert_eq!(plan.exchange_name, MQTT_EXCHANGE_NAME);
    }

    #[test]
    fn test_no_will_without_will_flag() {
        // MQTT-3.1.2-12: will flag=0 means no will message published.
        let mut session = make_session();
        let connect = make_connect("no-will", true);
        session.handle_connect(&connect);
        assert!(session.will.is_none());

        let will_plan = session.handle_unclean_disconnect();
        assert!(
            will_plan.is_none(),
            "no will should be published when will flag=0"
        );
    }

    // ---- Keep-alive validation [MQTT-3.1.2-24] ----

    #[test]
    fn test_v311_keepalive_sets_value() {
        let mut session = make_session();
        let mut connect = make_connect("ka-test", true);
        connect.keep_alive = 120;
        session.handle_connect(&connect);
        assert_eq!(session.keep_alive, 120);
    }

    // ---- CONNECT packet processing [MQTT-3.1.0-1] ----

    #[test]
    fn test_publish_before_connect_not_connected() {
        let mut session = make_session();
        // Before CONNECT, session.connected is false.
        assert!(!session.connected);
        // Server should check session.connected before calling handle_publish.
    }

    // ---- Subscription count tracking ----

    #[test]
    fn test_subscription_count_after_subscribe_and_unsubscribe() {
        let mut session = make_session();
        session.handle_connect(&make_connect("sub-count", true));

        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![
            TopicFilter {
                filter: Bytes::from("a/b"),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
            TopicFilter {
                filter: Bytes::from("c/d"),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
        ];
        session.handle_subscribe(1, &filters, None);
        assert_eq!(session.subscription_count(), 2);

        let unsub_filters: smallvec::SmallVec<[String; 4]> = smallvec::smallvec!["a/b".to_string()];
        session.handle_unsubscribe(2, &unsub_filters);
        assert_eq!(session.subscription_count(), 1);
    }

    // ---- Protocol version stored correctly ----

    #[test]
    fn test_protocol_version_stored_v311() {
        let mut session = make_session();
        session.handle_connect(&make_connect("v311", true));
        assert_eq!(session.protocol_version, ProtocolVersion::V311);
    }

    #[test]
    fn test_protocol_version_stored_v5() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("v5", true));
        assert_eq!(session.protocol_version, ProtocolVersion::V5);
    }

    // ---- QoS 1 inbound duplicate handling ----

    #[test]
    fn test_qos1_duplicate_publish_still_acked() {
        let mut session = make_session();
        session.handle_connect(&make_connect("dup-q1", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"test/dup",
            packet_id: Some(1),
            payload: b"first",
            properties: Properties::default(),
        };
        let mut msg_buf = BytesMut::new();
        let plan1 = session.handle_publish(&publish, &mut msg_buf);
        assert!(plan1.has_message(), "first publish should produce message");
        assert!(!session.out_buf.is_empty());
        codec::decode_buf::puback(&session.out_buf).unwrap();

        // PUBACK frees the packet ID.
        session.handle_puback(1);

        // New publish with same packet ID (reuse after ACK) should succeed.
        let mut msg_buf2 = BytesMut::new();
        let plan2 = session.handle_publish(
            &Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: b"test/dup",
                packet_id: Some(1),
                payload: b"second",
                properties: Properties::default(),
            },
            &mut msg_buf2,
        );
        assert!(plan2.has_message(), "reused packet ID should succeed");
    }

    // ---- Unsubscribe nonexistent topic is valid [MQTT-3.10.4-5] ----

    #[test]
    fn test_unsubscribe_nonexistent_filter_returns_unsuback() {
        let mut session = make_session();
        session.handle_connect(&make_connect("unsub-none", true));

        let unsub_filters: smallvec::SmallVec<[String; 4]> =
            smallvec::smallvec!["no/such/filter".to_string()];
        session.handle_unsubscribe(1, &unsub_filters);
        let (ua, _) = codec::decode_buf::unsuback(&session.out_buf).unwrap();
        assert_eq!(ua.packet_id, 1, "UNSUBACK must echo packet ID");
    }

    // ---- PUBLISH to retained topic stores correctly ----

    #[test]
    fn test_publish_retained_produces_retained_topic_name() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ret-pub", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: b"devices/sensor1/temp",
            packet_id: None,
            payload: b"22.5",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        // Retained publishes should produce a retained plan.
        assert!(
            plan.retained.is_some(),
            "retained publish should produce a retained plan"
        );
        let retained = plan.retained.unwrap();
        assert!(
            retained.topic_name.contains("retained"),
            "retained topic should contain 'retained' prefix"
        );
        assert!(retained.topic_name.contains("devices/sensor1/temp"));
    }

    // ---- Clean session=true on reconnect clears inflight ----

    #[test]
    fn test_clean_session_true_clears_inflight() {
        let mut session = make_session();
        let connect = make_connect("inflight-clear", false);
        session.handle_connect(&connect);

        // Build an outbound QoS 1 delivery.
        session.track_outbound_delivery(QoS::AtLeastOnce, 1, 2);
        assert!(session.outbound_inflight_count() > 0);

        // Reconnect with clean_session=true.
        let connect2 = make_connect("inflight-clear", true);
        session.handle_connect(&connect2);
        assert_eq!(
            session.outbound_inflight_count(),
            0,
            "clean_session=true must clear inflight"
        );
    }

    // ---- Rate / Quota Limiting tests (GAP-12) ----

    #[test]
    fn test_publish_rate_limit_not_exceeded() {
        let mut config = MqttSessionConfig::default();
        config.max_publish_rate = 100;
        let mut session = MqttSession::new(config);
        let connect = make_connect_v5("rate-test", true);
        session.handle_connect(&connect);

        // Under the limit — should pass.
        for _ in 0..100 {
            assert!(session.check_publish_rate_quota().is_ok());
        }
    }

    #[test]
    fn test_publish_rate_limit_exceeded() {
        let mut config = MqttSessionConfig::default();
        config.max_publish_rate = 5;
        let mut session = MqttSession::new(config);
        let connect = make_connect_v5("rate-test2", true);
        session.handle_connect(&connect);

        // Use up the rate limit.
        for _ in 0..5 {
            assert!(session.check_publish_rate_quota().is_ok());
        }
        // Next one should be rejected with 0x96 (Message rate too high).
        assert_eq!(session.check_publish_rate_quota(), Err(0x96));
    }

    #[test]
    fn test_publish_quota_exhausted() {
        let mut config = MqttSessionConfig::default();
        config.max_publish_quota = 3;
        let mut session = MqttSession::new(config);
        let connect = make_connect_v5("quota-test", true);
        session.handle_connect(&connect);

        // 2 publishes should pass (quota starts at 3, decrements to 2 then 1).
        assert!(session.check_publish_rate_quota().is_ok());
        assert!(session.check_publish_rate_quota().is_ok());
        // Third should be rejected with 0x97 (Quota exceeded).
        assert_eq!(session.check_publish_rate_quota(), Err(0x97));
    }

    #[test]
    fn test_publish_rate_limit_triggers_disconnect() {
        let mut config = MqttSessionConfig::default();
        config.max_publish_rate = 1;
        let mut session = MqttSession::new(config);
        let connect = make_connect_v5("rate-disconnect", true);
        session.handle_connect(&connect);

        // First publish OK.
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: b"test/topic",
            packet_id: None,
            payload: b"hello",
            properties: Properties::default(),
        };
        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        assert!(!plan.need_disconnect);

        // Second publish in same window should trigger disconnect.
        let mut msg_buf2 = BytesMut::new();
        let plan2 = session.handle_publish(&publish, &mut msg_buf2);
        assert!(plan2.need_disconnect);
        let (d, _) = codec::decode_buf::disconnect(&session.out_buf).unwrap();
        assert_eq!(d.reason_code, Some(0x96));
    }

    #[test]
    fn test_no_rate_limit_when_zero() {
        // Default config has max_publish_rate = 0 (unlimited).
        let mut session = make_session();
        let connect = make_connect_v5("no-limit", true);
        session.handle_connect(&connect);

        // Should never fail.
        for _ in 0..1000 {
            assert!(session.check_publish_rate_quota().is_ok());
        }
    }

    // ---- Topic Authorization tests (GAP-8) ----

    #[test]
    fn test_topic_authorization_publish_denied() {
        use crate::auth::{AuthProvider, AuthResult, TopicAction};
        use std::sync::Arc;

        struct DenyPublishProvider;
        impl AuthProvider for DenyPublishProvider {
            fn supports_method(&self, _method: &str) -> bool {
                false
            }
            fn authenticate_connect(
                &self,
                _method: &str,
                _client_id: &str,
                _username: Option<&str>,
                _password: Option<&[u8]>,
                _auth_data: Option<&[u8]>,
            ) -> AuthResult {
                AuthResult::Success
            }
            fn authenticate_continue(
                &self,
                _method: &str,
                _client_id: &str,
                _auth_data: Option<&[u8]>,
                _step: u32,
            ) -> AuthResult {
                AuthResult::Success
            }
            fn authorize_topic(
                &self,
                _client_id: &str,
                _username: Option<&str>,
                _topic: &str,
                action: TopicAction,
            ) -> bool {
                matches!(action, TopicAction::Subscribe) // deny publish, allow subscribe
            }
        }

        let config = MqttSessionConfig::default();
        let provider: Arc<dyn AuthProvider> = Arc::new(DenyPublishProvider);
        let mut session = MqttSession::with_auth_provider(config, provider);
        let connect = make_connect_v5("auth-test", true);
        session.handle_connect(&connect);

        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"secret/topic",
            packet_id: Some(1),
            payload: b"data",
            properties: Properties::default(),
        };

        let mut msg_buf = BytesMut::new();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        // Should get a PUBACK with 0x87 (Not Authorized).
        assert!(!session.out_buf.is_empty());
        let (ack, _) = codec::decode_buf::puback(&session.out_buf).unwrap();
        assert_eq!(ack.reason_code, Some(0x87));
        // flat_message should be empty (not processed).
        assert!(!plan.has_message());
    }

    #[test]
    fn test_topic_authorization_subscribe_denied() {
        use crate::auth::{AuthProvider, AuthResult, TopicAction};
        use std::sync::Arc;

        struct DenySubscribeProvider;
        impl AuthProvider for DenySubscribeProvider {
            fn supports_method(&self, _method: &str) -> bool {
                false
            }
            fn authenticate_connect(
                &self,
                _method: &str,
                _client_id: &str,
                _username: Option<&str>,
                _password: Option<&[u8]>,
                _auth_data: Option<&[u8]>,
            ) -> AuthResult {
                AuthResult::Success
            }
            fn authenticate_continue(
                &self,
                _method: &str,
                _client_id: &str,
                _auth_data: Option<&[u8]>,
                _step: u32,
            ) -> AuthResult {
                AuthResult::Success
            }
            fn authorize_topic(
                &self,
                _client_id: &str,
                _username: Option<&str>,
                _topic: &str,
                action: TopicAction,
            ) -> bool {
                matches!(action, TopicAction::Publish) // deny subscribe, allow publish
            }
        }

        let config = MqttSessionConfig::default();
        let provider: Arc<dyn AuthProvider> = Arc::new(DenySubscribeProvider);
        let mut session = MqttSession::with_auth_provider(config, provider);
        let connect = make_connect_v5("auth-sub-test", true);
        session.handle_connect(&connect);

        let filters = [TopicFilter {
            filter: Bytes::from("secret/topic"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];

        let plan = session.handle_subscribe(1, &filters, None);
        // The SUBACK should contain 0x87 (Not Authorized) for the filter.
        let (suback, _) = codec::decode_buf::suback_v5(&session.out_buf).unwrap();
        assert_eq!(suback.return_codes[0], 0x87);
        // No subscription plans should be generated.
        assert!(plan.filters.is_empty());
    }
}
