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

use bisque_mq::MqCommand;
use bisque_mq::flat::{FlatMessage, FlatMessageBuilder};
use bytes::Bytes;
use smallvec::SmallVec;
use tracing::{debug, warn};

use crate::auth::{AuthProvider, AuthResult, AuthState};
use crate::codec::{validate_topic_filter, validate_topic_name};
use crate::types::{
    Auth, ConnAck, Connect, ConnectReturnCode, Disconnect, MqttPacket, Properties, ProtocolVersion,
    PubAck, PubComp, PubRec, PubRel, Publish, QoS, SubAck, UnsubAck, WillMessage,
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
static HDR_PUBLISHER_SESSION_ID: Bytes = Bytes::from_static(b"mqtt.publisher_session_id");
static HDR_ORIGINAL_RETAIN: Bytes = Bytes::from_static(b"mqtt.original_retain");

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
}

// =============================================================================
// Structured Plans (returned to server for async orchestration)
// =============================================================================

/// Plan for publishing a message. The server resolves entity IDs and submits
/// commands in order.
#[derive(Debug)]
pub struct PublishPlan {
    /// The exchange name to route through.
    pub exchange_name: &'static str,
    /// Cached exchange_id if already known.
    pub cached_exchange_id: Option<u64>,
    /// The flat message bytes to publish through the exchange.
    pub flat_message: Bytes,
    /// MQTT response packets to send back to the client (PUBACK/PUBREC).
    pub responses: SmallVec<[MqttPacket; 2]>,
    /// Optional retained message plan.
    pub retained: Option<RetainedPlan>,
    /// If set, the server must disconnect the client with this DISCONNECT packet
    /// after sending any response packets. Used for protocol errors detected
    /// during PUBLISH handling (e.g., receive maximum exceeded, topic alias 0).
    pub disconnect: Option<MqttPacket>,
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
#[derive(Debug)]
pub struct SubscribePlan {
    /// The exchange name (global MQTT exchange).
    pub exchange_name: &'static str,
    /// Cached exchange_id if already known.
    pub cached_exchange_id: Option<u64>,
    /// Per-filter subscription details.
    pub filters: SmallVec<[SubscribeFilterPlan; 4]>,
    /// The SUBACK response packet.
    pub suback: MqttPacket,
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
    /// Queue config (with dedup if needed).
    pub queue_config: bisque_mq::config::QueueConfig,
    /// Whether this is a shared subscription.
    pub shared_group: Option<String>,
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
    /// Pre-computed session ID as big-endian Bytes (avoids per-publish allocation).
    session_id_bytes: Bytes,

    // -- MQTT 5.0 Problem Information --
    /// MQTT 5.0 SS 3.1.2.11.7: If false, the server MUST NOT send
    /// Reason String or User Properties in any packet other than CONNACK/DISCONNECT.
    request_problem_information: bool,

    // -- Enhanced Authentication (MQTT 5.0) --
    /// Current state of enhanced auth flow.
    auth_state: AuthState,
    /// Pluggable auth provider (shared across sessions).
    auth_provider: Option<Arc<dyn AuthProvider>>,
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
            session_id_bytes: Bytes::copy_from_slice(&session_id.to_be_bytes()),
            request_problem_information: true,
            auth_state: AuthState::None,
            auth_provider: None,
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
            .and_then(|w| w.properties.will_delay_interval)
            .unwrap_or(0)
    }

    /// Strip reason_string and user_properties from properties if
    /// request_problem_information is false (MQTT 5.0 SS 3.1.2.11.7).
    /// Only applies to non-CONNACK/DISCONNECT packets.
    fn strip_problem_info(&self, props: &mut Properties) {
        if !self.request_problem_information {
            props.reason_string = None;
            props.user_properties.clear();
        }
    }

    /// Allocate the next outbound packet identifier, skipping IDs currently in use.
    fn alloc_packet_id(&mut self) -> u16 {
        // Safety: loop at most 65535 times to find an unused ID.
        for _ in 0..65535u32 {
            let id = self.next_packet_id;
            self.next_packet_id = self.next_packet_id.wrapping_add(1);
            if self.next_packet_id == 0 {
                self.next_packet_id = 1;
            }
            // Skip IDs currently in use by any in-flight QoS handshake.
            if !self.qos1_inflight.contains_key(&id)
                && !self.qos2_inbound.contains_key(&id)
                && !self.qos2_outbound.contains_key(&id)
            {
                return id;
            }
        }
        // All 65535 IDs are in use (should never happen with proper flow control).
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
        // Replace '/' -> '.' inline during copy to avoid a separate `.replace()` allocation.
        if let Some(group) = shared_group {
            // "mqtt/shared/" + group + "/" + dotted_filter
            let mut s = String::with_capacity(12 + group.len() + 1 + topic_filter.len());
            s.push_str("mqtt/shared/");
            s.push_str(group);
            s.push('/');
            for ch in topic_filter.chars() {
                s.push(if ch == '/' { '.' } else { ch });
            }
            s
        } else {
            // "mqtt/sub/" + client_id + "/" + dotted_filter
            let mut s = String::with_capacity(9 + self.client_id.len() + 1 + topic_filter.len());
            s.push_str("mqtt/sub/");
            s.push_str(&self.client_id);
            s.push('/');
            for ch in topic_filter.chars() {
                s.push(if ch == '/' { '.' } else { ch });
            }
            s
        }
    }

    /// Translate MQTT topic filter wildcards to bisque-mq exchange pattern.
    /// MQTT uses `+` for single-level wildcard; bisque-mq uses `*`.
    /// Both use `#` for multi-level wildcard.
    fn mqtt_filter_to_mq_pattern(filter: &str) -> String {
        // Fast path: if no '+' present, return as-is without allocating a new string
        // via `.replace()`. Uses memchr for SIMD-accelerated scan.
        if memchr::memchr(b'+', filter.as_bytes()).is_none() {
            return filter.to_string();
        }
        // Only allocate and transform when '+' is actually present.
        let mut s = String::with_capacity(filter.len());
        for ch in filter.chars() {
            s.push(if ch == '+' { '*' } else { ch });
        }
        s
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
    pub fn cache_exchange_id(&mut self, name: &str, id: u64) {
        self.entity_cache.exchanges.insert(name.to_string(), id);
    }

    /// Cache a queue name -> id mapping.
    pub fn cache_queue_id(&mut self, name: &str, id: u64) {
        self.entity_cache.queues.insert(name.to_string(), id);
    }

    /// Cache a topic name -> id mapping.
    pub fn cache_topic_id(&mut self, name: &str, id: u64) {
        self.entity_cache.topics.insert(name.to_string(), id);
    }

    /// Cache a binding (exchange_id, routing_key) -> binding_id mapping.
    pub fn cache_binding_id(&mut self, exchange_id: u64, routing_key: &str, binding_id: u64) {
        self.entity_cache
            .bindings
            .entry(exchange_id)
            .or_default()
            .insert(routing_key.to_string(), binding_id);
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
    /// Returns a list of `MqCommand`s to execute and the CONNACK response packet.
    pub fn handle_connect(&mut self, connect: &Connect) -> (SmallVec<[MqCommand; 4]>, MqttPacket) {
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
                        return (
                            SmallVec::new(),
                            MqttPacket::ConnAck(ConnAck {
                                session_present: false,
                                return_code: ConnectReturnCode::IdentifierRejected as u8,
                                properties: Properties {
                                    reason_string: Some(
                                        "empty client ID requires clean_session=true".into(),
                                    ),
                                    ..Properties::default()
                                },
                            }),
                        );
                    }
                }
                ProtocolVersion::V5 => {
                    let generated = format!("auto-{:016x}", self.session_id);
                    assigned_client_id = Some(generated.clone());
                    self.client_id = generated;
                }
            }
        } else {
            self.client_id = connect.client_id.clone();
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
        if let Some(max) = connect.properties.topic_alias_maximum {
            self.max_topic_alias = max;
        }
        if let Some(rm) = connect.properties.receive_maximum {
            self.client_receive_maximum = rm;
        }
        if let Some(mps) = connect.properties.maximum_packet_size {
            self.client_maximum_packet_size = mps;
        }
        if let Some(sei) = connect.properties.session_expiry_interval {
            self.session_expiry_interval = sei;
        }
        // MQTT 5.0 SS 3.1.2.11.7: Request Problem Information.
        // Default is true (1). If 0, suppress reason_string and user_properties
        // on all packets except CONNACK and DISCONNECT.
        if let Some(rpi) = connect.properties.request_problem_information {
            self.request_problem_information = rpi != 0;
        }

        // Enhanced Authentication (MQTT 5.0, SS 3.1.2.11.9):
        // If the CONNECT contains an authentication_method, initiate the enhanced auth flow.
        if connect.protocol_version == ProtocolVersion::V5 {
            if let Some(ref method) = connect.properties.authentication_method {
                if let Some(ref provider) = self.auth_provider {
                    if !provider.supports_method(method) {
                        return (
                            SmallVec::new(),
                            MqttPacket::ConnAck(ConnAck {
                                session_present: false,
                                return_code: 0x8C, // Bad authentication method
                                properties: Properties {
                                    reason_string: Some("unsupported authentication method".into()),
                                    ..Properties::default()
                                },
                            }),
                        );
                    }

                    let auth_data = connect.properties.authentication_data.as_deref();
                    let username = connect.username.as_deref();
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
                                method: method.clone(),
                                step: 1,
                            };
                            return (
                                SmallVec::new(),
                                MqttPacket::Auth(Auth {
                                    reason_code: Auth::CONTINUE_AUTHENTICATION,
                                    properties: Properties {
                                        authentication_method: Some(method.clone()),
                                        authentication_data: Some(authentication_data),
                                        ..Properties::default()
                                    },
                                }),
                            );
                        }
                        AuthResult::Failed {
                            reason_code,
                            reason_string,
                        } => {
                            return (
                                SmallVec::new(),
                                MqttPacket::ConnAck(ConnAck {
                                    session_present: false,
                                    return_code: reason_code,
                                    properties: Properties {
                                        reason_string,
                                        ..Properties::default()
                                    },
                                }),
                            );
                        }
                    }
                } else {
                    // No auth provider configured but client sent authentication_method.
                    return (
                        SmallVec::new(),
                        MqttPacket::ConnAck(ConnAck {
                            session_present: false,
                            return_code: 0x8C, // Bad authentication method
                            properties: Properties {
                                reason_string: Some("server does not support enhanced auth".into()),
                                ..Properties::default()
                            },
                        }),
                    );
                }
            }
        }

        self.connected = true;
        // Set the server's receive maximum for inbound flow control enforcement.
        self.server_receive_maximum = self.config.max_inflight.min(65535) as u16;
        let mut commands = SmallVec::new();

        // Compute consumer/producer name once to avoid double format! allocation.
        let consumer_name = format!("mqtt/{}", self.client_id);

        // Register this session as a bisque-mq consumer.
        commands.push(MqCommand::register_consumer(
            self.session_id,
            &consumer_name,
            &[],
        ));

        // Register as a producer for publishing.
        commands.push(MqCommand::register_producer(
            self.session_id,
            Some(&consumer_name),
        ));

        // If clean session, clear any old subscription state.
        if self.clean_session {
            self.subscriptions.clear();
            self.qos1_inflight.clear();
            self.qos2_inbound.clear();
            self.qos2_outbound.clear();
            self.entity_cache = EntityCache::default();
            self.outbound_qos1_count = 0;
            self.cached_first_sub_id = None;
            self.queue_to_sub_id.clear();
            // Reset topic alias maps (correctness: old aliases invalid on reconnect).
            self.outbound_topic_aliases.clear();
            self.inbound_topic_aliases.clear();
            self.next_outbound_alias = 1;
        }

        // Build CONNACK with capabilities for MQTT 5.0.
        let properties = if connect.protocol_version == ProtocolVersion::V5 {
            // MQTT 5.0 SS 3.1.2.11.7: If client sets request_response_information=1,
            // server MAY include response_information in CONNACK.
            let response_information = if connect.properties.request_response_information == Some(1)
            {
                // Provide a response topic prefix the client can use for request/response.
                Some(format!("mqtt/response/{}", self.client_id))
            } else {
                None
            };
            Properties {
                maximum_qos: Some(self.config.maximum_qos.as_u8()),
                retain_available: Some(true),
                maximum_packet_size: Some(self.config.max_packet_size as u32),
                topic_alias_maximum: Some(65535),
                wildcard_subscription_available: Some(true),
                subscription_identifier_available: Some(true),
                shared_subscription_available: Some(true),
                receive_maximum: Some(self.config.max_inflight.min(65535) as u16),
                server_keep_alive: Some(self.keep_alive),
                assigned_client_identifier: assigned_client_id,
                response_information,
                ..Properties::default()
            }
        } else {
            Properties::default()
        };

        let connack = MqttPacket::ConnAck(ConnAck {
            session_present: !self.clean_session,
            return_code: ConnectReturnCode::Accepted as u8,
            properties,
        });

        debug!(
            client_id = %self.client_id,
            session_id = self.session_id,
            version = ?self.protocol_version,
            clean = self.clean_session,
            "MQTT session connected"
        );

        (commands, connack)
    }

    // =========================================================================
    // Enhanced Authentication (AUTH packet handling)
    // =========================================================================

    /// Handle an incoming AUTH packet during an enhanced authentication flow.
    ///
    /// Returns `(commands, response_packets)` — the response is either another AUTH
    /// challenge (continue), a CONNACK (success/failure), or a DISCONNECT + CONNACK (failure).
    pub fn handle_auth(
        &mut self,
        auth: &Auth,
    ) -> (SmallVec<[MqCommand; 4]>, SmallVec<[MqttPacket; 2]>) {
        let (method, step) = match &self.auth_state {
            AuthState::WaitingForResponse { method, step } => (method.clone(), *step),
            _ => {
                warn!("received AUTH packet but no auth flow in progress");
                let disconnect = MqttPacket::Disconnect(crate::types::Disconnect {
                    reason_code: Some(0x82), // Protocol Error
                    properties: Properties::default(),
                });
                let (_will, cmds) = self.handle_disconnect(None);
                let mut pkts = SmallVec::new();
                pkts.push(disconnect);
                return (SmallVec::from_iter(cmds), pkts);
            }
        };

        // Validate reason code
        if auth.reason_code != Auth::CONTINUE_AUTHENTICATION {
            warn!(
                reason_code = auth.reason_code,
                "unexpected AUTH reason code"
            );
            self.auth_state = AuthState::Complete;
            let connack = MqttPacket::ConnAck(ConnAck {
                session_present: false,
                return_code: 0x82, // Protocol Error
                properties: Properties {
                    reason_string: Some("unexpected AUTH reason code".into()),
                    ..Properties::default()
                },
            });
            return (SmallVec::new(), SmallVec::from_elem(connack, 1));
        }

        // Validate method matches
        if let Some(ref pkt_method) = auth.properties.authentication_method {
            if pkt_method != &method {
                warn!(expected = %method, got = %pkt_method, "AUTH method mismatch");
                self.auth_state = AuthState::Complete;
                let connack = MqttPacket::ConnAck(ConnAck {
                    session_present: false,
                    return_code: 0x8C, // Bad authentication method
                    properties: Properties {
                        reason_string: Some("authentication method mismatch".into()),
                        ..Properties::default()
                    },
                });
                return (SmallVec::new(), SmallVec::from_elem(connack, 1));
            }
        }

        let provider = match &self.auth_provider {
            Some(p) => Arc::clone(p),
            None => {
                self.auth_state = AuthState::Complete;
                let connack = MqttPacket::ConnAck(ConnAck {
                    session_present: false,
                    return_code: 0x8C,
                    properties: Properties {
                        reason_string: Some("server does not support enhanced auth".into()),
                        ..Properties::default()
                    },
                });
                return (SmallVec::new(), SmallVec::from_elem(connack, 1));
            }
        };

        let auth_data = auth.properties.authentication_data.as_deref();
        let result = provider.authenticate_continue(&method, &self.client_id, auth_data, step);

        match result {
            AuthResult::Success => {
                self.auth_state = AuthState::Complete;
                self.connected = true;

                // Build CONNACK + registration commands (same as handle_connect completion).
                let consumer_name = format!("mqtt/{}", self.client_id);
                let mut commands = SmallVec::new();
                commands.push(MqCommand::register_consumer(
                    self.session_id,
                    &consumer_name,
                    &[],
                ));
                commands.push(MqCommand::register_producer(
                    self.session_id,
                    Some(&consumer_name),
                ));

                let properties = Properties {
                    maximum_qos: Some(self.config.maximum_qos.as_u8()),
                    retain_available: Some(true),
                    maximum_packet_size: Some(self.config.max_packet_size as u32),
                    topic_alias_maximum: Some(65535),
                    wildcard_subscription_available: Some(true),
                    subscription_identifier_available: Some(true),
                    shared_subscription_available: Some(true),
                    receive_maximum: Some(self.config.max_inflight.min(65535) as u16),
                    server_keep_alive: Some(self.keep_alive),
                    authentication_method: Some(method),
                    ..Properties::default()
                };

                let connack = MqttPacket::ConnAck(ConnAck {
                    session_present: !self.clean_session,
                    return_code: ConnectReturnCode::Accepted as u8,
                    properties,
                });

                debug!(
                    client_id = %self.client_id,
                    "enhanced auth completed successfully"
                );

                (commands, SmallVec::from_elem(connack, 1))
            }
            AuthResult::Continue {
                authentication_data,
            } => {
                self.auth_state = AuthState::WaitingForResponse {
                    method: method.clone(),
                    step: step + 1,
                };
                let auth_pkt = MqttPacket::Auth(Auth {
                    reason_code: Auth::CONTINUE_AUTHENTICATION,
                    properties: Properties {
                        authentication_method: Some(method),
                        authentication_data: Some(authentication_data),
                        ..Properties::default()
                    },
                });
                (SmallVec::new(), SmallVec::from_elem(auth_pkt, 1))
            }
            AuthResult::Failed {
                reason_code,
                reason_string,
            } => {
                self.auth_state = AuthState::Complete;
                let connack = MqttPacket::ConnAck(ConnAck {
                    session_present: false,
                    return_code: reason_code,
                    properties: Properties {
                        reason_string,
                        ..Properties::default()
                    },
                });
                (SmallVec::new(), SmallVec::from_elem(connack, 1))
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
            flat_message: Bytes::new(),
            responses: SmallVec::new(),
            retained: None,
            disconnect: None,
        }
    }

    pub fn handle_publish(&mut self, publish: &Publish) -> PublishPlan {
        // Max QoS enforcement: reject PUBLISH with QoS above server's maximum_qos.
        // MQTT 5.0 SS 3.2.2.3.4: server MUST NOT forward QoS > advertised maximum_qos.
        if publish.qos > self.config.maximum_qos {
            warn!(
                client_qos = publish.qos.as_u8(),
                max_qos = self.config.maximum_qos.as_u8(),
                "PUBLISH QoS exceeds server maximum_qos"
            );
            // For QoS 1, respond with PUBACK + error reason code (0x9B = QoS not supported).
            // For QoS 2, respond with PUBREC + error reason code.
            let mut responses = SmallVec::new();
            if let Some(packet_id) = publish.packet_id {
                match publish.qos {
                    QoS::AtLeastOnce => {
                        responses.push(MqttPacket::PubAck(PubAck {
                            packet_id,
                            reason_code: if self.protocol_version == ProtocolVersion::V5 {
                                Some(0x9B)
                            } else {
                                None
                            },
                            properties: Properties::default(),
                        }));
                    }
                    QoS::ExactlyOnce => {
                        responses.push(MqttPacket::PubRec(PubRec {
                            packet_id,
                            reason_code: if self.protocol_version == ProtocolVersion::V5 {
                                Some(0x9B)
                            } else {
                                None
                            },
                            properties: Properties::default(),
                        }));
                    }
                    _ => {}
                }
            }
            return PublishPlan {
                exchange_name: MQTT_EXCHANGE_NAME,
                cached_exchange_id: None,
                flat_message: Bytes::new(),
                responses,
                retained: None,
                disconnect: None,
            };
        }

        // MQTT 5.0 SS 3.3.4: Receive Maximum enforcement.
        // If the client sends more QoS 1/2 PUBLISH packets than the server's
        // Receive Maximum without receiving acknowledgments, disconnect with 0x93.
        if publish.qos != QoS::AtMostOnce && self.protocol_version == ProtocolVersion::V5 {
            if self.inbound_qos_inflight >= self.server_receive_maximum as usize {
                warn!(
                    client_id = %self.client_id,
                    inflight = self.inbound_qos_inflight,
                    receive_maximum = self.server_receive_maximum,
                    "receive maximum exceeded"
                );
                return PublishPlan {
                    exchange_name: MQTT_EXCHANGE_NAME,
                    cached_exchange_id: None,
                    flat_message: Bytes::new(),
                    responses: SmallVec::new(),
                    retained: None,
                    disconnect: Some(MqttPacket::Disconnect(Disconnect {
                        reason_code: Some(0x93), // Receive Maximum Exceeded
                        properties: Properties {
                            reason_string: Some("receive maximum exceeded".into()),
                            ..Properties::default()
                        },
                    })),
                };
            }
            self.inbound_qos_inflight += 1;
        }

        // Resolve topic alias if present (MQTT 5.0).
        // Topic is Bytes (UTF-8 validated on decode).
        let topic: Bytes = if let Some(alias) = publish.properties.topic_alias {
            // Topic alias validation: alias must be 1..=server's topic_alias_maximum (65535).
            // MQTT 5.0 SS 3.3.2.3.4: alias of 0 is a protocol error.
            if alias == 0 {
                warn!("topic alias 0 is invalid");
                // Topic alias 0 is a Protocol Error (MQTT 5.0 SS 3.3.2.3.4).
                return PublishPlan {
                    exchange_name: MQTT_EXCHANGE_NAME,
                    cached_exchange_id: None,
                    flat_message: Bytes::new(),
                    responses: SmallVec::new(),
                    retained: None,
                    disconnect: Some(MqttPacket::Disconnect(Disconnect {
                        reason_code: Some(0x82), // Protocol Error
                        properties: Properties {
                            reason_string: Some("topic alias must not be 0".into()),
                            ..Properties::default()
                        },
                    })),
                };
            }
            if !publish.topic.is_empty() {
                self.inbound_topic_aliases
                    .insert(alias, publish.topic.clone());
                publish.topic.clone()
            } else if let Some(resolved) = self.inbound_topic_aliases.get(&alias) {
                resolved.clone()
            } else {
                warn!(alias, "unknown inbound topic alias");
                return Self::empty_publish_plan();
            }
        } else {
            publish.topic.clone()
        };

        // Validate topic name (M3): reject wildcards, empty, and null bytes.
        if validate_topic_name(&topic).is_err() {
            warn!(topic = ?std::str::from_utf8(&topic).unwrap_or("<invalid>"), "invalid topic name in PUBLISH");
            return Self::empty_publish_plan();
        }

        // Payload format UTF-8 validation (MQTT 5.0 SS 3.3.2.3.2):
        // If payload_format_indicator = 1, the payload MUST be valid UTF-8.
        if publish.properties.payload_format_indicator == Some(1) {
            if std::str::from_utf8(&publish.payload).is_err() {
                warn!("payload_format_indicator=1 but payload is not valid UTF-8");
                // Return PUBACK/PUBREC with reason code 0x99 (Payload Format Invalid).
                let mut responses = SmallVec::new();
                if let Some(packet_id) = publish.packet_id {
                    match publish.qos {
                        QoS::AtLeastOnce => {
                            responses.push(MqttPacket::PubAck(PubAck {
                                packet_id,
                                reason_code: Some(0x99),
                                properties: Properties::default(),
                            }));
                        }
                        QoS::ExactlyOnce => {
                            responses.push(MqttPacket::PubRec(PubRec {
                                packet_id,
                                reason_code: Some(0x99),
                                properties: Properties::default(),
                            }));
                        }
                        _ => {}
                    }
                }
                return PublishPlan {
                    exchange_name: MQTT_EXCHANGE_NAME,
                    cached_exchange_id: None,
                    flat_message: Bytes::new(),
                    responses,
                    retained: None,
                    disconnect: None,
                };
            }
        }

        let now = Self::now_ms();

        // Build the flat message with native fields (not headers) for reply_to
        // and correlation_id so the engine's ACK-with-response routing works.
        // Topic is already Bytes — no String→Bytes conversion needed.
        let mut builder = FlatMessageBuilder::new(publish.payload.clone())
            .timestamp(now)
            .routing_key(topic.clone());

        // MQTT 5.0 request/response -> FlatMessage native fields.
        if let Some(ref rt) = publish.properties.response_topic {
            builder = builder.reply_to(Bytes::copy_from_slice(rt.as_bytes()));
        }
        if let Some(ref cd) = publish.properties.correlation_data {
            builder = builder.correlation_id(cd.clone());
        }

        // Store remaining MQTT 5.0 properties as headers using static keys.
        if let Some(ref ct) = publish.properties.content_type {
            builder = builder.header(
                HDR_CONTENT_TYPE.clone(),
                Bytes::copy_from_slice(ct.as_bytes()),
            );
        }
        if let Some(pfi) = publish.properties.payload_format_indicator {
            builder = builder.header(HDR_PAYLOAD_FORMAT.clone(), Bytes::copy_from_slice(&[pfi]));
        }
        for (key, val) in &publish.properties.user_properties {
            // Build "mqtt.user.<key>" without intermediate String by writing to a BytesMut.
            let mut hdr_key = bytes::BytesMut::with_capacity(10 + key.len());
            hdr_key.extend_from_slice(b"mqtt.user.");
            hdr_key.extend_from_slice(key.as_bytes());
            builder = builder.header(hdr_key.freeze(), Bytes::copy_from_slice(val.as_bytes()));
        }
        if let Some(secs) = publish.properties.message_expiry_interval {
            builder = builder.ttl_ms(secs as u64 * 1000);
        }

        // Store publisher session ID for No Local enforcement (M6).
        // Uses pre-computed Bytes (refcount bump only, no allocation).
        builder = builder.header(
            HDR_PUBLISHER_SESSION_ID.clone(),
            self.session_id_bytes.clone(),
        );

        // Store original retain flag for Retain As Published enforcement (M7).
        if publish.retain {
            builder = builder.header(HDR_ORIGINAL_RETAIN.clone(), Bytes::from_static(&[1]));
        }

        let flat_message = builder.build();

        // Build response packets and track in-flight state.
        let mut responses = SmallVec::new();
        match publish.qos {
            QoS::AtMostOnce => {
                // QoS 0: no acknowledgment needed.
            }
            QoS::AtLeastOnce => {
                if let Some(packet_id) = publish.packet_id {
                    // Track as inbound QoS 1 (no mq IDs needed for inbound).
                    self.qos1_inflight.insert(
                        packet_id,
                        QoS1InFlight {
                            mq_message_id: None,
                            mq_queue_id: None,
                            direction: Direction::Inbound,
                        },
                    );
                    let rc = if self.protocol_version == ProtocolVersion::V5 {
                        Some(0x00) // Success
                    } else {
                        None
                    };
                    responses.push(MqttPacket::PubAck(PubAck {
                        packet_id,
                        reason_code: rc,
                        properties: Properties::default(),
                    }));
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = publish.packet_id {
                    // QoS 2 duplicate detection (m12): if packet_id already exists
                    // in PubRecSent state, respond with PUBREC but skip the publish.
                    if let Some(QoS2InboundState::PubRecSent) = self.qos2_inbound.get(&packet_id) {
                        debug!(packet_id, "QoS 2 duplicate PUBLISH, resending PUBREC");
                        let rc = if self.protocol_version == ProtocolVersion::V5 {
                            Some(0x00)
                        } else {
                            None
                        };
                        return PublishPlan {
                            exchange_name: MQTT_EXCHANGE_NAME,
                            cached_exchange_id: None,
                            flat_message: Bytes::new(), // skip actual publish
                            responses: smallvec::smallvec![MqttPacket::PubRec(PubRec {
                                packet_id,
                                reason_code: rc,
                                properties: Properties::default(),
                            })],
                            retained: None,
                            disconnect: None,
                        };
                    }
                    self.qos2_inbound
                        .insert(packet_id, QoS2InboundState::PubRecSent);
                    let rc = if self.protocol_version == ProtocolVersion::V5 {
                        Some(0x00) // Success
                    } else {
                        None
                    };
                    responses.push(MqttPacket::PubRec(PubRec {
                        packet_id,
                        reason_code: rc,
                        properties: Properties::default(),
                    }));
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
                Some(RetainedPlan {
                    topic_name: retained_name,
                    cached_topic_id,
                    flat_message: Some(flat_message.clone()),
                })
            }
        } else {
            None
        };

        PublishPlan {
            exchange_name: MQTT_EXCHANGE_NAME,
            cached_exchange_id: self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied(),
            flat_message,
            responses,
            retained,
            disconnect: None,
        }
    }

    // =========================================================================
    // PUBACK / PUBREC / PUBREL / PUBCOMP handling
    // =========================================================================

    /// Handle PUBACK from client (QoS 1 acknowledgment for server -> client delivery).
    ///
    /// Returns an ACK command if this was an outbound delivery, None otherwise.
    pub fn handle_puback(&mut self, packet_id: u16) -> Option<MqCommand> {
        if let Some(inflight) = self.qos1_inflight.remove(&packet_id) {
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
                    return Some(MqCommand::ack(queue_id, &[message_id], None));
                }
            }
        } else {
            warn!(packet_id, "PUBACK for unknown packet ID");
        }
        None
    }

    /// Handle PUBREC from client (QoS 2, client acknowledges our outbound PUBLISH).
    ///
    /// Returns the PUBREL response packet. No MqCommands needed at this stage.
    pub fn handle_pubrec(&mut self, packet_id: u16) -> MqttPacket {
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
        MqttPacket::PubRel(PubRel {
            packet_id,
            reason_code,
            properties: Properties::default(),
        })
    }

    /// Handle PUBREL from client (QoS 2 inbound, step 2).
    ///
    /// Returns the PUBCOMP response packet. No MqCommands needed at this stage.
    pub fn handle_pubrel(&mut self, packet_id: u16) -> MqttPacket {
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
        MqttPacket::PubComp(PubComp {
            packet_id,
            reason_code,
            properties: Properties::default(),
        })
    }

    /// Handle PUBCOMP from client (QoS 2 outbound complete).
    ///
    /// Returns an ACK command if the QoS 2 flow completed successfully, None otherwise.
    pub fn handle_pubcomp(&mut self, packet_id: u16) -> Option<MqCommand> {
        if let Some(state) = self.qos2_outbound.remove(&packet_id) {
            match state {
                QoS2OutboundState::PubRelSent {
                    mq_queue_id,
                    mq_message_id,
                } => {
                    // QoS 2 delivery complete. ACK in bisque-mq.
                    debug!(packet_id, "QoS 2 outbound delivery complete");
                    return Some(MqCommand::ack(mq_queue_id, &[mq_message_id], None));
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
        None
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
        let mut filter_plans = SmallVec::new();
        let mut return_codes = SmallVec::new();

        for filter in filters {
            // Validate topic filter (M4).
            if validate_topic_filter(&filter.filter).is_err() {
                warn!(filter = %filter.filter, "invalid topic filter in SUBSCRIBE");
                return_codes.push(0x80); // Failure
                continue;
            }

            // Check for shared subscription.
            let (shared_group, actual_filter) =
                if let Some((group, f)) = Self::parse_shared_subscription(&filter.filter) {
                    (Some(group.to_string()), f.to_string())
                } else {
                    (None, filter.filter.clone())
                };

            // MQTT 5.0 SS 3.8.3.1: It is a Protocol Error to set the No Local
            // bit to 1 on a Shared Subscription.
            if shared_group.is_some() && filter.no_local {
                warn!(filter = %filter.filter, "no_local=1 on shared subscription is a protocol error");
                return_codes.push(0xA2); // Shared Subscriptions not supported (closest reason code)
                continue;
            }

            // Translate MQTT wildcards to bisque-mq pattern.
            let routing_key = Self::mqtt_filter_to_mq_pattern(&actual_filter);

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

            let queue_config = bisque_mq::config::QueueConfig::default();

            // QoS downgrade (m8): clamp to server maximum_qos.
            let granted_qos = if filter.qos > self.config.maximum_qos {
                self.config.maximum_qos
            } else {
                filter.qos
            };

            // Check if this subscription already exists (for retain_handling=1).
            let is_new_subscription = !self.subscriptions.contains_key(&filter.filter);

            filter_plans.push(SubscribeFilterPlan {
                filter: filter.filter.clone(),
                queue_name,
                cached_queue_id,
                routing_key,
                cached_binding_id,
                qos: granted_qos,
                queue_config,
                shared_group: shared_group.clone(),
                subscription_id,
                retain_handling: filter.retain_handling,
                is_new_subscription,
            });

            // Track the subscription.
            self.subscriptions.insert(
                filter.filter.clone(),
                SubscriptionMapping {
                    filter: actual_filter,
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

            debug!(
                client_id = %self.client_id,
                filter = %filter.filter,
                qos = ?filter.qos,
                "subscription added"
            );
        }

        let mut suback_props = Properties::default();
        self.strip_problem_info(&mut suback_props);
        let suback = MqttPacket::SubAck(SubAck {
            packet_id,
            return_codes,
            properties: suback_props,
        });

        SubscribePlan {
            exchange_name: MQTT_EXCHANGE_NAME,
            cached_exchange_id: self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied(),
            filters: filter_plans,
            suback,
        }
    }

    // =========================================================================
    // UNSUBSCRIBE handling
    // =========================================================================

    /// Handle an UNSUBSCRIBE packet.
    ///
    /// Returns MqCommands to tear down bindings and an UNSUBACK response.
    pub fn handle_unsubscribe(
        &mut self,
        packet_id: u16,
        filters: &[String],
    ) -> (SmallVec<[MqCommand; 4]>, MqttPacket) {
        let mut commands = SmallVec::new();
        let mut reason_codes = SmallVec::new();

        for filter in filters {
            if let Some(mapping) = self.subscriptions.remove(filter) {
                // Delete the exchange binding.
                if let Some(binding_id) = mapping.binding_id {
                    commands.push(MqCommand::delete_binding(binding_id));
                }
                // Delete subscription queue if clean session and not shared.
                if self.clean_session && mapping.shared_group.is_none() {
                    if let Some(queue_id) = mapping.queue_id {
                        commands.push(MqCommand::delete_queue(queue_id));
                    }
                }
                // Remove from queue_to_sub_id reverse index.
                if let Some(queue_id) = mapping.queue_id {
                    self.queue_to_sub_id.remove(&queue_id);
                }
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

        let mut unsuback_props = Properties::default();
        self.strip_problem_info(&mut unsuback_props);
        let unsuback = MqttPacket::UnsubAck(UnsubAck {
            packet_id,
            reason_codes,
            properties: unsuback_props,
        });

        (commands, unsuback)
    }

    // =========================================================================
    // PINGREQ handling
    // =========================================================================

    /// Handle a PINGREQ packet.
    pub fn handle_pingreq(&self) -> (MqCommand, MqttPacket) {
        let cmd = MqCommand::heartbeat(self.session_id);
        (cmd, MqttPacket::PingResp)
    }

    // =========================================================================
    // DISCONNECT handling
    // =========================================================================

    /// Handle a DISCONNECT packet (clean disconnect).
    ///
    /// For MQTT 5.0, if the reason code is 0x04 (Disconnect with Will Message),
    /// the will is NOT cleared — it will be published by the server.
    /// For all other reason codes (including 0x00 Normal Disconnection), the will
    /// is cleared per MQTT 5.0 SS 3.14.4.
    pub fn handle_disconnect(
        &mut self,
        disconnect: Option<&crate::types::Disconnect>,
    ) -> (Option<PublishPlan>, SmallVec<[MqCommand; 8]>) {
        // MQTT 5.0: Update session expiry interval from DISCONNECT properties (SS 3.14.2.2.2).
        // It is a Protocol Error to set a non-zero Session Expiry Interval if the
        // Session Expiry Interval in the CONNECT packet was zero.
        if self.protocol_version == ProtocolVersion::V5 {
            if let Some(d) = disconnect {
                if let Some(new_sei) = d.properties.session_expiry_interval {
                    if self.session_expiry_interval == 0 && new_sei != 0 {
                        warn!(
                            client_id = %self.client_id,
                            "protocol error: cannot set non-zero session expiry on DISCONNECT when CONNECT had 0"
                        );
                        // Protocol Error — but we're already disconnecting, so just
                        // log and ignore the update (the session will expire immediately).
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
            // Build will plan similar to unclean disconnect.
            self.will.take().map(|will| {
                let now = Self::now_ms();
                let flat_message = FlatMessageBuilder::new(will.payload.clone())
                    .timestamp(now)
                    .routing_key(Bytes::from(will.topic.clone()))
                    .build();

                PublishPlan {
                    exchange_name: MQTT_EXCHANGE_NAME,
                    cached_exchange_id: self
                        .entity_cache
                        .exchanges
                        .get(MQTT_EXCHANGE_NAME)
                        .copied(),
                    flat_message,
                    responses: SmallVec::new(),
                    retained: if will.retain && !will.payload.is_empty() {
                        let retained_name = self.retained_topic_name(&will.topic);
                        let cached_topic_id = self.entity_cache.topics.get(&retained_name).copied();
                        let ret_msg = FlatMessageBuilder::new(will.payload.clone())
                            .timestamp(now)
                            .routing_key(Bytes::from(will.topic.clone()))
                            .build();
                        Some(RetainedPlan {
                            topic_name: retained_name,
                            cached_topic_id,
                            flat_message: Some(ret_msg),
                        })
                    } else {
                        None
                    },
                    disconnect: None,
                }
            })
        } else {
            self.will = None;
            None
        };

        self.connected = false;

        let mut commands = SmallVec::new();

        commands.push(MqCommand::disconnect_consumer(self.session_id));
        commands.push(MqCommand::disconnect_producer(self.session_id));

        // If clean session, remove all subscription bindings and queues.
        if self.clean_session {
            for mapping in self.subscriptions.values() {
                if let Some(binding_id) = mapping.binding_id {
                    commands.push(MqCommand::delete_binding(binding_id));
                }
                if mapping.shared_group.is_none() {
                    if let Some(queue_id) = mapping.queue_id {
                        commands.push(MqCommand::delete_queue(queue_id));
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

        (will_plan, commands)
    }

    // =========================================================================
    // Unclean disconnect (TCP drop)
    // =========================================================================

    /// Handle an unclean disconnect (TCP connection dropped without DISCONNECT).
    ///
    /// Returns a `PublishPlan` for the will message (if any) and cleanup commands.
    pub fn handle_unclean_disconnect(&mut self) -> (Option<PublishPlan>, SmallVec<[MqCommand; 8]>) {
        let mut commands = SmallVec::new();

        // Build will message plan if present.
        let will_plan = if let Some(ref will) = self.will {
            let now = Self::now_ms();
            let mut builder = FlatMessageBuilder::new(will.payload.clone())
                .timestamp(now)
                .routing_key(Bytes::from(will.topic.clone()));

            // Preserve MQTT 5.0 will properties.
            if let Some(ref rt) = will.properties.response_topic {
                builder = builder.reply_to(Bytes::copy_from_slice(rt.as_bytes()));
            }
            if let Some(ref cd) = will.properties.correlation_data {
                builder = builder.correlation_id(cd.clone());
            }
            if let Some(secs) = will.properties.message_expiry_interval {
                builder = builder.ttl_ms(secs as u64 * 1000);
            }
            if let Some(ref ct) = will.properties.content_type {
                builder = builder.header(
                    HDR_CONTENT_TYPE.clone(),
                    Bytes::copy_from_slice(ct.as_bytes()),
                );
            }
            if let Some(pfi) = will.properties.payload_format_indicator {
                builder =
                    builder.header(HDR_PAYLOAD_FORMAT.clone(), Bytes::copy_from_slice(&[pfi]));
            }
            for (k, v) in &will.properties.user_properties {
                let mut hdr_key = bytes::BytesMut::with_capacity(10 + k.len());
                hdr_key.extend_from_slice(b"mqtt.user.");
                hdr_key.extend_from_slice(k.as_bytes());
                builder = builder.header(hdr_key.freeze(), Bytes::copy_from_slice(v.as_bytes()));
            }

            // Will delay interval: use FlatMessage delay.
            if let Some(delay_secs) = will.properties.will_delay_interval {
                if delay_secs > 0 {
                    builder = builder.delay_ms(delay_secs as u64 * 1000);
                }
            }

            let flat_message = builder.build();

            Some(PublishPlan {
                exchange_name: MQTT_EXCHANGE_NAME,
                cached_exchange_id: self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied(),
                flat_message,
                responses: SmallVec::new(),
                retained: if will.retain && !will.payload.is_empty() {
                    let retained_name = self.retained_topic_name(&will.topic);
                    let cached_topic_id = self.entity_cache.topics.get(&retained_name).copied();
                    // Rebuild without delay for the retained copy.
                    let ret_builder = FlatMessageBuilder::new(will.payload.clone())
                        .timestamp(now)
                        .routing_key(Bytes::from(will.topic.clone()));
                    Some(RetainedPlan {
                        topic_name: retained_name,
                        cached_topic_id,
                        flat_message: Some(ret_builder.build()),
                    })
                } else {
                    None
                },
                disconnect: None,
            })
        } else {
            None
        };

        self.will = None;
        self.connected = false;

        // Disconnect consumer/producer. DisconnectConsumer handles returning
        // in-flight queue messages to pending automatically.
        commands.push(MqCommand::disconnect_consumer(self.session_id));
        commands.push(MqCommand::disconnect_producer(self.session_id));

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
                commands.push(MqCommand::nack(cur_qid, &mids));
            }
        }

        self.qos1_inflight.clear();
        self.qos2_inbound.clear();
        self.qos2_outbound.clear();
        self.outbound_qos1_count = 0;
        self.queue_to_sub_id.clear();

        debug!(
            client_id = %self.client_id,
            session_id = self.session_id,
            "MQTT session disconnected (unclean)"
        );

        (will_plan, commands)
    }

    // =========================================================================
    // Outbound message delivery (bisque-mq -> MQTT client)
    // =========================================================================

    /// Build an outbound PUBLISH packet for delivering a bisque-mq message to
    /// the MQTT client, decoding the FlatMessage to reconstruct MQTT properties.
    pub fn build_outbound_publish_from_flat(
        &mut self,
        flat_msg: &FlatMessage,
        effective_qos: QoS,
        retain: bool,
        mq_queue_id: u64,
        mq_message_id: u64,
    ) -> Option<MqttPacket> {
        if self.is_inflight_full() {
            return None;
        }

        // Zero-copy: routing_key() returns Bytes (refcount bump from mmap).
        let topic = flat_msg
            .routing_key()
            .unwrap_or_else(|| Bytes::from_static(b"unknown"));

        let payload = flat_msg.value();

        // Reconstruct MQTT 5.0 properties from FlatMessage.
        let mut properties = Properties::default();

        if self.protocol_version == ProtocolVersion::V5 {
            // Native FlatMessage fields -> MQTT properties.
            if let Some(reply_to) = flat_msg.reply_to() {
                properties.response_topic = Some(String::from_utf8_lossy(&reply_to).into_owned());
            }
            if let Some(corr_id) = flat_msg.correlation_id() {
                properties.correlation_data = Some(corr_id);
            }

            // Reconstruct from headers — compare raw bytes to avoid String alloc.
            for i in 0..flat_msg.header_count() {
                let (k, v) = flat_msg.header(i);
                if &k[..] == b"mqtt.content_type" {
                    properties.content_type = Some(String::from_utf8_lossy(&v).into_owned());
                } else if &k[..] == b"mqtt.payload_format" && !v.is_empty() {
                    properties.payload_format_indicator = Some(v[0]);
                } else if k.starts_with(b"mqtt.user.") {
                    let prop_key = String::from_utf8_lossy(&k[10..]).into_owned();
                    properties
                        .user_properties
                        .push((prop_key, String::from_utf8_lossy(&v).into_owned()));
                }
            }

            // Reconstruct message_expiry_interval from TTL.
            if let Some(ttl_ms) = flat_msg.ttl_ms() {
                let remaining_secs = ttl_ms / 1000;
                if remaining_secs > 0 {
                    properties.message_expiry_interval = Some(remaining_secs as u32);
                }
            }

            // Per-queue subscription identifier lookup (MQTT 5.0 SS 3.3.4).
            if let Some(sub_id) = self.find_subscription_id_for_queue(mq_queue_id) {
                properties.subscription_identifier = Some(sub_id);
            }

            // Outbound topic alias assignment (m7: empty topic on existing alias).
            if let Some((alias, _is_new)) = self.resolve_outbound_topic_alias(&topic) {
                properties.topic_alias = Some(alias);
                // Note: For the struct-based path, we keep the full topic in the
                // Publish struct. The encode_publish_versioned encoder uses
                // properties.topic_alias presence to determine alias usage, but
                // always writes the topic from the struct. The m7 optimization
                // (empty topic on existing alias) is primarily for the zero-alloc
                // encode_publish_from_flat path which handles it via the bool flag.
            }
        }

        let packet_id = if effective_qos != QoS::AtMostOnce {
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

            Some(id)
        } else {
            None
        };

        Some(MqttPacket::Publish(Publish {
            dup: false,
            qos: effective_qos,
            retain,
            topic,
            packet_id,
            payload,
            properties,
        }))
    }

    /// Legacy build_outbound_publish for raw payloads (kept for backward compat).
    pub fn build_outbound_publish(
        &mut self,
        topic: &str,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        mq_queue_id: Option<u64>,
        mq_message_id: Option<u64>,
    ) -> MqttPacket {
        let packet_id = if qos != QoS::AtMostOnce {
            let id = self.alloc_packet_id();

            match qos {
                QoS::AtLeastOnce => {
                    self.qos1_inflight.insert(
                        id,
                        QoS1InFlight {
                            mq_message_id,
                            mq_queue_id,
                            direction: Direction::Outbound,
                        },
                    );
                    self.outbound_qos1_count += 1;
                }
                QoS::ExactlyOnce => {
                    if let (Some(qid), Some(mid)) = (mq_queue_id, mq_message_id) {
                        self.qos2_outbound.insert(
                            id,
                            QoS2OutboundState::PublishSent {
                                mq_queue_id: qid,
                                mq_message_id: mid,
                            },
                        );
                    }
                }
                _ => {}
            }

            Some(id)
        } else {
            None
        };

        MqttPacket::Publish(Publish {
            dup: false,
            qos,
            retain,
            topic: Bytes::copy_from_slice(topic.as_bytes()),
            packet_id,
            payload,
            properties: Properties::default(),
        })
    }

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

    /// Iterator over subscriptions for the delivery loop.
    pub fn subscriptions_iter(&self) -> impl Iterator<Item = (&str, &SubscriptionMapping)> {
        self.subscriptions.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Check if any subscription with the given queue_id has no_local=true.
    pub fn has_no_local_for_queue(&self, queue_id: u64) -> bool {
        self.subscriptions
            .values()
            .any(|m| m.queue_id == Some(queue_id) && m.no_local)
    }

    /// Check if any subscription with the given queue_id has retain_as_published=true.
    pub fn has_retain_as_published_for_queue(&self, queue_id: u64) -> bool {
        self.subscriptions
            .values()
            .any(|m| m.queue_id == Some(queue_id) && m.retain_as_published)
    }

    /// Return outbound QoS 1 in-flight packet IDs (for session persistence).
    pub fn pending_qos1_packet_ids(&self) -> Vec<u16> {
        self.qos1_inflight
            .iter()
            .filter(|(_, v)| v.direction == Direction::Outbound)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Return outbound QoS 2 in-flight packet IDs (for session persistence).
    pub fn pending_qos2_packet_ids(&self) -> Vec<u16> {
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
                    format!("$share/{}/{}", group, sub.filter)
                } else {
                    sub.filter.clone()
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

    /// Generate packets to retransmit on session resumption (MQTT 5.0 SS 4.4).
    ///
    /// Returns PUBLISH (DUP=1) for unacked outbound QoS 1, and PUBREL for
    /// outbound QoS 2 in PubRelSent state.
    pub fn pending_retransmits(&self) -> SmallVec<[MqttPacket; 4]> {
        let mut packets = SmallVec::new();

        // Retransmit outbound QoS 1 PUBLISH (DUP=1).
        // Note: we don't have the original payload cached, so we send a placeholder
        // that the server must fill from the queue. For now, just retransmit PUBRELs
        // which are fully reconstructable.

        // Retransmit PUBREL for outbound QoS 2 in PubRelSent state.
        for (&packet_id, state) in &self.qos2_outbound {
            if matches!(state, QoS2OutboundState::PubRelSent { .. }) {
                let rc = if self.protocol_version == ProtocolVersion::V5 {
                    Some(0x00)
                } else {
                    None
                };
                packets.push(MqttPacket::PubRel(PubRel {
                    packet_id,
                    reason_code: rc,
                    properties: Properties::default(),
                }));
            }
        }

        packets
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

    /// Process incoming MQTT packet and return (commands, response_packets).
    /// For PUBLISH and SUBSCRIBE, use handle_publish / handle_subscribe instead
    /// (they return structured plans for the server to orchestrate).
    ///
    /// This dispatch method handles simple packets that don't need orchestration.
    pub fn process_packet(
        &mut self,
        packet: &MqttPacket,
    ) -> (SmallVec<[MqCommand; 4]>, SmallVec<[MqttPacket; 2]>) {
        match packet {
            MqttPacket::Connect(connect) => {
                let (cmds, connack) = self.handle_connect(connect);
                (cmds, smallvec::smallvec![connack])
            }
            MqttPacket::Publish(_publish) => {
                // PUBLISH requires orchestration via handle_publish + server logic.
                // The server should call handle_publish() directly.
                // For backward compatibility, we still handle it here.
                let plan = self.handle_publish(_publish);
                // Return the responses from the plan; the server must separately
                // orchestrate the exchange publish.
                (SmallVec::new(), plan.responses)
            }
            MqttPacket::PubAck(puback) => {
                let cmds: SmallVec<[MqCommand; 4]> =
                    self.handle_puback(puback.packet_id).into_iter().collect();
                (cmds, SmallVec::new())
            }
            MqttPacket::PubRec(pubrec) => {
                let pubrel = self.handle_pubrec(pubrec.packet_id);
                (SmallVec::new(), smallvec::smallvec![pubrel])
            }
            MqttPacket::PubRel(pubrel) => {
                let pubcomp = self.handle_pubrel(pubrel.packet_id);
                (SmallVec::new(), smallvec::smallvec![pubcomp])
            }
            MqttPacket::PubComp(pubcomp) => {
                let cmds: SmallVec<[MqCommand; 4]> =
                    self.handle_pubcomp(pubcomp.packet_id).into_iter().collect();
                (cmds, SmallVec::new())
            }
            MqttPacket::Subscribe(subscribe) => {
                // SUBSCRIBE requires orchestration via handle_subscribe + server.
                let plan = self.handle_subscribe(
                    subscribe.packet_id,
                    &subscribe.filters,
                    subscribe.properties.subscription_identifier,
                );
                (SmallVec::new(), smallvec::smallvec![plan.suback])
            }
            MqttPacket::Unsubscribe(unsubscribe) => {
                let (cmds, unsuback) =
                    self.handle_unsubscribe(unsubscribe.packet_id, &unsubscribe.filters);
                (cmds, smallvec::smallvec![unsuback])
            }
            MqttPacket::PingReq => {
                let (cmd, pong) = self.handle_pingreq();
                (smallvec::smallvec![cmd], smallvec::smallvec![pong])
            }
            MqttPacket::Disconnect(disconnect) => {
                let (_will_plan, cmds) = self.handle_disconnect(Some(disconnect));
                // Note: will_plan is returned but process_packet can't orchestrate it.
                // The server's process_inbound_packet handles this directly.
                (SmallVec::from_iter(cmds), SmallVec::new())
            }
            // AUTH is only valid in MQTT 5.0 enhanced authentication flows,
            // which we do not support. Respond with DISCONNECT reason code 0x8C
            // (Bad authentication method).
            MqttPacket::Auth(auth) => {
                let (cmds, pkts) = self.handle_auth(auth);
                (cmds, SmallVec::from_iter(pkts))
            }
            // Server-originated packets should not arrive from client.
            MqttPacket::ConnAck(_)
            | MqttPacket::SubAck(_)
            | MqttPacket::UnsubAck(_)
            | MqttPacket::PingResp => {
                warn!(
                    packet_type = ?packet.packet_type(),
                    "unexpected packet from client"
                );
                (SmallVec::new(), SmallVec::new())
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
    use crate::types::*;

    fn make_session() -> MqttSession {
        MqttSession::new(MqttSessionConfig::default())
    }

    fn make_connect(client_id: &str, clean: bool) -> Connect {
        Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: client_id.to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        }
    }

    fn make_connect_v5(client_id: &str, clean: bool) -> Connect {
        Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: client_id.to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties {
                topic_alias_maximum: Some(10),
                receive_maximum: Some(100),
                session_expiry_interval: Some(3600),
                ..Properties::default()
            },
        }
    }

    // ---- CONNECT tests ----

    #[test]
    fn test_session_connect() {
        let mut session = make_session();
        let connect = make_connect("test-client", true);

        let (commands, response) = session.handle_connect(&connect);

        assert!(session.connected);
        assert_eq!(session.client_id, "test-client");
        assert!(session.clean_session);
        assert_eq!(session.keep_alive, 60);

        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].tag(), MqCommand::TAG_REGISTER_CONSUMER);
        assert_eq!(commands[1].tag(), MqCommand::TAG_REGISTER_PRODUCER);

        match response {
            MqttPacket::ConnAck(connack) => {
                assert_eq!(connack.return_code, 0x00);
                assert!(!connack.session_present);
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_session_connect_v5_capabilities() {
        let mut session = make_session();
        let connect = make_connect_v5("v5-client", true);

        let (_commands, response) = session.handle_connect(&connect);

        assert_eq!(session.max_topic_alias, 10);
        assert_eq!(session.client_receive_maximum, 100);
        assert_eq!(session.session_expiry_interval, 3600);

        match response {
            MqttPacket::ConnAck(connack) => {
                assert!(connack.properties.maximum_qos.is_some());
                assert!(connack.properties.retain_available.is_some());
                assert!(connack.properties.shared_subscription_available.is_some());
                assert!(connack.properties.wildcard_subscription_available.is_some());
                assert!(connack.properties.receive_maximum.is_some());
            }
            _ => panic!("expected ConnAck"),
        }
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
            topic: Bytes::from_static(b"sensor/1/temp"),
            packet_id: None,
            payload: Bytes::from_static(b"22.5"),
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);

        assert_eq!(plan.exchange_name, MQTT_EXCHANGE_NAME);
        assert!(!plan.flat_message.is_empty());
        assert!(plan.responses.is_empty()); // QoS 0: no ack
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
            topic: Bytes::from_static(b"sensor/1/temp"),
            packet_id: Some(1),
            payload: Bytes::from_static(b"22.5"),
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);

        assert_eq!(plan.responses.len(), 1);
        match &plan.responses[0] {
            MqttPacket::PubAck(puback) => assert_eq!(puback.packet_id, 1),
            _ => panic!("expected PubAck"),
        }
    }

    #[test]
    fn test_publish_qos2_returns_pubrec() {
        let mut session = make_session();
        session.handle_connect(&make_connect("pub-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: Bytes::from_static(b"important/data"),
            packet_id: Some(10),
            payload: Bytes::from_static(b"critical"),
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);

        assert_eq!(plan.responses.len(), 1);
        match &plan.responses[0] {
            MqttPacket::PubRec(pubrec) => assert_eq!(pubrec.packet_id, 10),
            _ => panic!("expected PubRec"),
        }
        assert_eq!(session.qos2_inflight_count(), 1);

        // PUBREL completes the inbound QoS 2.
        let pubcomp = session.handle_pubrel(10);
        assert!(matches!(pubcomp, MqttPacket::PubComp(_)));
    }

    #[test]
    fn test_publish_uses_native_reply_to_and_correlation_id() {
        let mut session = make_session();
        session.handle_connect(&make_connect("rr-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"request/topic"),
            packet_id: None,
            payload: Bytes::from_static(b"request body"),
            properties: Properties {
                response_topic: Some("reply/topic".to_string()),
                correlation_data: Some(Bytes::from_static(b"corr-123")),
                ..Properties::default()
            },
        };

        let plan = session.handle_publish(&publish);

        // Verify the flat message has native reply_to and correlation_id.
        let flat = FlatMessage::new(plan.flat_message).expect("valid flat message");
        let reply_to = flat.reply_to().expect("should have reply_to");
        assert_eq!(&reply_to[..], b"reply/topic");
        let corr_id = flat.correlation_id().expect("should have correlation_id");
        assert_eq!(&corr_id[..], b"corr-123");
    }

    #[test]
    fn test_publish_with_ttl() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ttl-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"expiring"),
            packet_id: None,
            payload: Bytes::from_static(b"data"),
            properties: Properties {
                message_expiry_interval: Some(60),
                ..Properties::default()
            },
        };

        let plan = session.handle_publish(&publish);
        let flat = FlatMessage::new(plan.flat_message).expect("valid flat message");
        assert_eq!(flat.ttl_ms(), Some(60_000));
    }

    #[test]
    fn test_publish_retained() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ret-client", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: Bytes::from_static(b"status/device1"),
            packet_id: None,
            payload: Bytes::from_static(b"online"),
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);
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
            topic: Bytes::from_static(b"status/device1"),
            packet_id: None,
            payload: Bytes::new(), // Empty = clear
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);
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
                filter: "sensor/+/data".to_string(),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
            TopicFilter {
                filter: "control/#".to_string(),
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

        match plan.suback {
            MqttPacket::SubAck(sa) => {
                assert_eq!(sa.packet_id, 1);
                assert_eq!(sa.return_codes.as_slice(), &[1, 0]);
            }
            _ => panic!("expected SubAck"),
        }
    }

    #[test]
    fn test_subscribe_shared() {
        let mut session = make_session();
        session.handle_connect(&make_connect("shared-client", true));

        let filters = vec![TopicFilter {
            filter: "$share/mygroup/sensor/+/data".to_string(),
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
            filter: "test/#".to_string(),
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
            filter: "sensor/+/data".to_string(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);
        assert_eq!(session.subscription_count(), 1);

        let (_, unsuback) = session.handle_unsubscribe(2, &["sensor/+/data".to_string()]);
        assert_eq!(session.subscription_count(), 0);

        match unsuback {
            MqttPacket::UnsubAck(ua) => {
                assert_eq!(ua.packet_id, 2);
                assert_eq!(ua.reason_codes.as_slice(), &[0x00]);
            }
            _ => panic!("expected UnsubAck"),
        }
    }

    // ---- PING tests ----

    #[test]
    fn test_pingreq() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ping-client", true));

        let (cmd, response) = session.handle_pingreq();
        assert_eq!(cmd.tag(), MqCommand::TAG_HEARTBEAT);
        assert!(matches!(response, MqttPacket::PingResp));
    }

    // ---- DISCONNECT tests ----

    #[test]
    fn test_disconnect_clean() {
        let mut session = make_session();
        session.handle_connect(&make_connect("dc-client", true));

        let filters = vec![TopicFilter {
            filter: "test/#".to_string(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);

        let (will_plan, commands) = session.handle_disconnect(None);
        assert!(!session.connected);
        assert!(session.will.is_none());
        assert!(will_plan.is_none());
        assert!(commands.len() >= 2);
        assert_eq!(session.subscription_count(), 0);
    }

    #[test]
    fn test_unclean_disconnect_with_will() {
        let mut session = make_session();

        let will = WillMessage {
            topic: "client/status".to_string(),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::default(),
        };

        let connect = Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: "will-client".to_string(),
            will: Some(will),
            username: None,
            password: None,
            properties: Properties::default(),
        };

        session.handle_connect(&connect);
        assert!(session.will.is_some());

        let (will_plan, commands) = session.handle_unclean_disconnect();
        assert!(!session.connected);
        assert!(session.will.is_none());
        assert!(will_plan.is_some());

        let plan = will_plan.unwrap();
        assert_eq!(plan.exchange_name, MQTT_EXCHANGE_NAME);
        assert!(!plan.flat_message.is_empty());

        // Should have DisconnectConsumer + DisconnectProducer.
        assert!(commands.len() >= 2);
    }

    #[test]
    fn test_unclean_disconnect_will_preserves_v5_properties() {
        let mut session = make_session();

        let will = WillMessage {
            topic: "status".to_string(),
            payload: Bytes::from_static(b"gone"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties {
                response_topic: Some("reply/status".to_string()),
                correlation_data: Some(Bytes::from_static(b"will-corr")),
                content_type: Some("text/plain".to_string()),
                message_expiry_interval: Some(300),
                will_delay_interval: Some(10),
                ..Properties::default()
            },
        };

        let connect = Connect {
            protocol_name: "MQTT".to_string(),
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
            client_id: "v5-will".to_string(),
            will: Some(will),
            username: None,
            password: None,
            properties: Properties::default(),
        };

        session.handle_connect(&connect);
        let (will_plan, _commands) = session.handle_unclean_disconnect();

        let plan = will_plan.unwrap();
        let flat = FlatMessage::new(plan.flat_message).expect("valid flat message");

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
        let packet = session.build_outbound_publish(
            "test/topic",
            Bytes::from_static(b"data"),
            QoS::ExactlyOnce,
            false,
            Some(100),
            Some(42),
        );

        let packet_id = match &packet {
            MqttPacket::Publish(p) => p.packet_id.unwrap(),
            _ => panic!("expected Publish"),
        };

        assert_eq!(session.qos2_outbound.len(), 1);

        // Client sends PUBREC.
        let pubrel = session.handle_pubrec(packet_id);
        assert!(matches!(pubrel, MqttPacket::PubRel(_)));

        // State should be PubRelSent now.
        assert!(matches!(
            session.qos2_outbound.get(&packet_id),
            Some(QoS2OutboundState::PubRelSent { .. })
        ));

        // Client sends PUBCOMP.
        let cmd = session.handle_pubcomp(packet_id);

        // Should ACK in bisque-mq.
        let cmd = cmd.expect("should return ACK command");
        assert_eq!(cmd.tag(), MqCommand::TAG_ACK);
        assert!(session.qos2_outbound.is_empty());
    }

    // ---- Outbound publish tests ----

    #[test]
    fn test_outbound_publish_qos1_tracks_inflight() {
        let mut session = make_session();
        session.handle_connect(&make_connect("out-client", true));

        let packet = session.build_outbound_publish(
            "sensor/1/temp",
            Bytes::from_static(b"22.5"),
            QoS::AtLeastOnce,
            false,
            Some(100),
            Some(42),
        );

        match packet {
            MqttPacket::Publish(p) => {
                assert_eq!(&p.topic[..], b"sensor/1/temp");
                assert_eq!(p.qos, QoS::AtLeastOnce);
                assert!(p.packet_id.is_some());
            }
            _ => panic!("expected Publish"),
        }

        assert_eq!(session.qos1_inflight_count(), 1);

        // PUBACK from client should ACK in bisque-mq.
        let cmd = session.handle_puback(1);
        let cmd = cmd.expect("should return ACK command");
        assert_eq!(cmd.tag(), MqCommand::TAG_ACK);
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

    // ---- process_packet dispatch tests ----

    #[test]
    fn test_process_packet_dispatch() {
        let mut session = make_session();

        let connect = MqttPacket::Connect(make_connect("dispatch-client", true));
        let (cmds, responses) = session.process_packet(&connect);
        assert!(!cmds.is_empty());
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MqttPacket::ConnAck(_)));

        let (cmds, responses) = session.process_packet(&MqttPacket::PingReq);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].tag(), MqCommand::TAG_HEARTBEAT);
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MqttPacket::PingResp));
    }

    // ---- FlatMessage outbound decode tests ----

    #[test]
    fn test_build_outbound_publish_from_flat() {
        let mut session = make_session();
        session.handle_connect(&make_connect_v5("outf-client", true));

        // Build a flat message with all properties.
        let flat_bytes = FlatMessageBuilder::new(Bytes::from_static(b"sensor data"))
            .timestamp(1000)
            .routing_key(Bytes::from_static(b"sensor/1/temp"))
            .reply_to(Bytes::from_static(b"reply/sensor"))
            .correlation_id(Bytes::from_static(b"corr-abc"))
            .ttl_ms(60_000)
            .header("mqtt.content_type", Bytes::from_static(b"application/json"))
            .header("mqtt.user.custom-key", Bytes::from_static(b"custom-value"))
            .build();

        let flat = FlatMessage::new(flat_bytes).unwrap();

        let packet = session
            .build_outbound_publish_from_flat(&flat, QoS::AtLeastOnce, false, 100, 42)
            .expect("should produce packet");

        match packet {
            MqttPacket::Publish(p) => {
                assert_eq!(&p.topic[..], b"sensor/1/temp");
                assert_eq!(p.payload, Bytes::from_static(b"sensor data"));
                assert_eq!(p.qos, QoS::AtLeastOnce);

                // MQTT 5.0 properties reconstructed.
                assert_eq!(
                    p.properties.response_topic,
                    Some("reply/sensor".to_string())
                );
                assert_eq!(
                    p.properties.correlation_data,
                    Some(Bytes::from_static(b"corr-abc"))
                );
                assert_eq!(
                    p.properties.content_type,
                    Some("application/json".to_string())
                );
                assert_eq!(p.properties.message_expiry_interval, Some(60));
                assert_eq!(p.properties.user_properties.len(), 1);
                assert_eq!(p.properties.user_properties[0].0, "custom-key");
                assert_eq!(p.properties.user_properties[0].1, "custom-value");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_build_outbound_publish_from_flat_respects_inflight_limit() {
        let mut session = MqttSession::new(MqttSessionConfig {
            max_inflight: 0, // Full immediately
            ..MqttSessionConfig::default()
        });
        session.handle_connect(&make_connect("limit-client", true));

        let flat_bytes = FlatMessageBuilder::new(Bytes::from_static(b"data"))
            .routing_key(Bytes::from_static(b"test"))
            .build();
        let flat = FlatMessage::new(flat_bytes).unwrap();

        let result = session.build_outbound_publish_from_flat(&flat, QoS::AtLeastOnce, false, 1, 1);
        assert!(result.is_none());
    }

    // =========================================================================
    // Phase 2: Protocol Validation Tests
    // =========================================================================

    // ---- Client ID handling (M5) ----

    #[test]
    fn test_empty_client_id_v311_clean_generates_id() {
        let mut session = make_session();
        let connect = make_connect("", true);
        let (_cmds, connack) = session.handle_connect(&connect);

        assert!(session.client_id.starts_with("auto-"));
        assert!(session.connected);
        match connack {
            MqttPacket::ConnAck(c) => assert_eq!(c.return_code, 0x00),
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_empty_client_id_v311_not_clean_rejected() {
        let mut session = make_session();
        let connect = make_connect("", false);
        let (cmds, connack) = session.handle_connect(&connect);

        // Should return IdentifierRejected and no commands
        assert!(cmds.is_empty());
        match connack {
            MqttPacket::ConnAck(c) => {
                assert_eq!(c.return_code, ConnectReturnCode::IdentifierRejected as u8);
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_empty_client_id_v5_generates_id_and_assigns() {
        let mut session = make_session();
        let connect = make_connect_v5("", true);
        let (_cmds, connack) = session.handle_connect(&connect);

        assert!(session.client_id.starts_with("auto-"));
        assert!(session.connected);
        match connack {
            MqttPacket::ConnAck(c) => {
                assert_eq!(c.return_code, 0x00);
                assert!(c.properties.assigned_client_identifier.is_some());
                assert!(
                    c.properties
                        .assigned_client_identifier
                        .unwrap()
                        .starts_with("auto-")
                );
            }
            _ => panic!("expected ConnAck"),
        }
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
            topic: Bytes::from_static(b"test/dup"),
            packet_id: Some(42),
            payload: Bytes::from_static(b"first"),
            properties: Properties::default(),
        };

        // First PUBLISH
        let plan1 = session.handle_publish(&publish);
        assert!(!plan1.flat_message.is_empty()); // Message is published
        assert_eq!(plan1.responses.len(), 1);

        // Second PUBLISH with same packet_id (duplicate)
        let dup_publish = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: Bytes::from_static(b"test/dup"),
            packet_id: Some(42),
            payload: Bytes::from_static(b"duplicate"),
            properties: Properties::default(),
        };

        let plan2 = session.handle_publish(&dup_publish);
        assert!(plan2.flat_message.is_empty()); // Message is NOT published
        assert_eq!(plan2.responses.len(), 1); // But PUBREC is still sent
        match &plan2.responses[0] {
            MqttPacket::PubRec(pubrec) => assert_eq!(pubrec.packet_id, 42),
            _ => panic!("expected PubRec"),
        }
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
            topic: Bytes::from_static(b"sensor/+/data"),
            packet_id: None,
            payload: Bytes::from_static(b"data"),
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);
        assert!(plan.flat_message.is_empty());
    }

    #[test]
    fn test_subscribe_invalid_filter_returns_failure() {
        let mut session = make_session();
        session.handle_connect(&make_connect("sub-val", true));

        let filters = vec![
            crate::types::TopicFilter {
                filter: "sensor/+data".to_string(), // Invalid: + not occupying entire level
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
            crate::types::TopicFilter {
                filter: "valid/topic".to_string(), // Valid
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
        ];

        let plan = session.handle_subscribe(1, &filters, None);
        match plan.suback {
            MqttPacket::SubAck(ref suback) => {
                assert_eq!(suback.return_codes.len(), 2);
                assert_eq!(suback.return_codes[0], 0x80); // Failure for invalid filter
                assert_eq!(suback.return_codes[1], QoS::AtLeastOnce.as_u8()); // Success for valid
            }
            _ => panic!("expected SubAck"),
        }
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
            filter: "test/topic".to_string(),
            qos: QoS::ExactlyOnce, // Request QoS 2
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];

        let plan = session.handle_subscribe(1, &filters, None);
        match plan.suback {
            MqttPacket::SubAck(ref suback) => {
                // Should be downgraded to QoS 1.
                assert_eq!(suback.return_codes[0], QoS::AtLeastOnce.as_u8());
            }
            _ => panic!("expected SubAck"),
        }
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
            filter: "test/topic".to_string(),
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
            filter: "test/topic".to_string(),
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

    // ---- Phase 3: Publisher session ID in FlatMessage ----

    #[test]
    fn test_publish_stores_session_id_in_flat_message() {
        let mut session = make_session();
        session.handle_connect(&make_connect("hdr-client", true));
        let session_id = session.session_id;

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic"),
            packet_id: None,
            payload: Bytes::from_static(b"data"),
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);
        let flat = bisque_mq::flat::FlatMessage::new(plan.flat_message).unwrap();

        // Verify publisher session ID header.
        let mut found_session_id = false;
        for i in 0..flat.header_count() {
            let (k, v) = flat.header(i);
            if &k[..] == b"mqtt.publisher_session_id" {
                assert_eq!(v.len(), 8);
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                assert_eq!(u64::from_be_bytes(buf), session_id);
                found_session_id = true;
            }
        }
        assert!(found_session_id, "publisher session ID header not found");
    }

    #[test]
    fn test_publish_retained_stores_retain_flag() {
        let mut session = make_session();
        session.handle_connect(&make_connect("ret-hdr", true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: Bytes::from_static(b"test/retained"),
            packet_id: None,
            payload: Bytes::from_static(b"data"),
            properties: Properties::default(),
        };

        let plan = session.handle_publish(&publish);
        let flat = bisque_mq::flat::FlatMessage::new(plan.flat_message).unwrap();

        let mut found_retain = false;
        for i in 0..flat.header_count() {
            let (k, v) = flat.header(i);
            if &k[..] == b"mqtt.original_retain" {
                assert_eq!(v[0], 1);
                found_retain = true;
            }
        }
        assert!(found_retain, "original retain header not found");
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
            topic: "will/topic".to_string(),
            payload: Bytes::from_static(b"goodbye"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::default(),
        });
        session.handle_connect(&connect);
        assert!(session.will.is_some());

        // Normal DISCONNECT (reason 0x00): will should be cleared.
        let disconnect = crate::types::Disconnect {
            reason_code: Some(0x00),
            properties: Properties::default(),
        };
        let (will_plan, _cmds) = session.handle_disconnect(Some(&disconnect));
        assert!(will_plan.is_none());
        assert!(session.will.is_none());
    }

    #[test]
    fn test_v5_disconnect_with_will_publishes_will() {
        let mut session = make_session();
        let mut connect = make_connect_v5("will-client", true);
        connect.flags.will = true;
        connect.will = Some(WillMessage {
            topic: "will/topic".to_string(),
            payload: Bytes::from_static(b"goodbye"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::default(),
        });
        session.handle_connect(&connect);
        assert!(session.will.is_some());

        // DISCONNECT with reason 0x04 (Disconnect with Will Message): will should be published.
        let disconnect = crate::types::Disconnect {
            reason_code: Some(0x04),
            properties: Properties::default(),
        };
        let (will_plan, _cmds) = session.handle_disconnect(Some(&disconnect));
        assert!(will_plan.is_some());
        let plan = will_plan.unwrap();
        assert!(!plan.flat_message.is_empty());
    }

    #[test]
    fn test_v311_disconnect_always_clears_will() {
        let mut session = make_session();
        let mut connect = make_connect("will-v3", true);
        connect.flags.will = true;
        connect.will = Some(WillMessage {
            topic: "will/topic".to_string(),
            payload: Bytes::from_static(b"goodbye"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::default(),
        });
        session.handle_connect(&connect);
        assert!(session.will.is_some());

        // V3.1.1 has no reason code — will is always cleared on DISCONNECT.
        let (will_plan, _cmds) = session.handle_disconnect(None);
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
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "auth-client".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties {
                authentication_method: Some("TEST-CHALLENGE".to_string()),
                ..Properties::default()
            },
        };

        let (cmds, pkt) = session.handle_connect(&connect);
        // Should return AUTH challenge, not CONNACK.
        assert!(cmds.is_empty(), "no commands until auth completes");
        match &pkt {
            MqttPacket::Auth(auth) => {
                assert_eq!(
                    auth.reason_code,
                    crate::types::Auth::CONTINUE_AUTHENTICATION
                );
                assert_eq!(
                    auth.properties.authentication_data.as_deref(),
                    Some(b"server-challenge".as_slice())
                );
            }
            other => panic!("expected AUTH, got {:?}", other),
        }
        assert!(!session.connected);
    }

    #[test]
    fn test_enhanced_auth_success_after_response() {
        let provider: Arc<dyn crate::auth::AuthProvider> = Arc::new(TestChallengeAuthProvider);
        let mut session = MqttSession::with_auth_provider(Default::default(), provider);

        // Start auth flow.
        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "auth-client".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties {
                authentication_method: Some("TEST-CHALLENGE".to_string()),
                ..Properties::default()
            },
        };
        session.handle_connect(&connect);

        // Send correct response.
        let auth_pkt = crate::types::Auth {
            reason_code: crate::types::Auth::CONTINUE_AUTHENTICATION,
            properties: Properties {
                authentication_method: Some("TEST-CHALLENGE".to_string()),
                authentication_data: Some(Bytes::from_static(b"correct-response")),
                ..Properties::default()
            },
        };
        let (cmds, pkts) = session.handle_auth(&auth_pkt);

        // Should get CONNACK success + registration commands.
        assert!(!cmds.is_empty(), "should have registration commands");
        assert_eq!(pkts.len(), 1);
        match &pkts[0] {
            MqttPacket::ConnAck(connack) => {
                assert_eq!(connack.return_code, 0x00); // Accepted
                assert_eq!(
                    connack.properties.authentication_method.as_deref(),
                    Some("TEST-CHALLENGE")
                );
            }
            other => panic!("expected ConnAck, got {:?}", other),
        }
        assert!(session.connected);
    }

    #[test]
    fn test_enhanced_auth_failure_after_bad_response() {
        let provider: Arc<dyn crate::auth::AuthProvider> = Arc::new(TestChallengeAuthProvider);
        let mut session = MqttSession::with_auth_provider(Default::default(), provider);

        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "auth-client".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties {
                authentication_method: Some("TEST-CHALLENGE".to_string()),
                ..Properties::default()
            },
        };
        session.handle_connect(&connect);

        // Send wrong response.
        let auth_pkt = crate::types::Auth {
            reason_code: crate::types::Auth::CONTINUE_AUTHENTICATION,
            properties: Properties {
                authentication_method: Some("TEST-CHALLENGE".to_string()),
                authentication_data: Some(Bytes::from_static(b"wrong-response")),
                ..Properties::default()
            },
        };
        let (cmds, pkts) = session.handle_auth(&auth_pkt);

        assert!(cmds.is_empty());
        assert_eq!(pkts.len(), 1);
        match &pkts[0] {
            MqttPacket::ConnAck(connack) => {
                assert_eq!(connack.return_code, 0x86); // Bad credentials
            }
            other => panic!("expected ConnAck, got {:?}", other),
        }
        assert!(!session.connected);
    }

    #[test]
    fn test_enhanced_auth_unsupported_method_rejected() {
        let provider: Arc<dyn crate::auth::AuthProvider> = Arc::new(TestChallengeAuthProvider);
        let mut session = MqttSession::with_auth_provider(Default::default(), provider);

        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "auth-client".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties {
                authentication_method: Some("UNKNOWN-METHOD".to_string()),
                ..Properties::default()
            },
        };

        let (cmds, pkt) = session.handle_connect(&connect);
        assert!(cmds.is_empty());
        match &pkt {
            MqttPacket::ConnAck(connack) => {
                assert_eq!(connack.return_code, 0x8C); // Bad authentication method
            }
            other => panic!("expected ConnAck, got {:?}", other),
        }
    }

    #[test]
    fn test_auth_without_flow_in_progress_disconnects() {
        let mut session = MqttSession::new(Default::default());
        let auth_pkt = crate::types::Auth {
            reason_code: crate::types::Auth::CONTINUE_AUTHENTICATION,
            properties: Properties::default(),
        };
        let (_cmds, pkts) = session.handle_auth(&auth_pkt);
        assert!(!pkts.is_empty());
        // Should get a DISCONNECT
        match &pkts[0] {
            MqttPacket::Disconnect(d) => {
                assert_eq!(d.reason_code, Some(0x82)); // Protocol Error
            }
            other => panic!("expected Disconnect, got {:?}", other),
        }
    }

    // ---- Gap fix tests ----

    #[test]
    fn test_publish_qos_exceeds_max_rejected() {
        let mut config = MqttSessionConfig::default();
        config.maximum_qos = QoS::AtLeastOnce; // Server only supports QoS 0 and 1
        let mut session = MqttSession::new(config);

        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        session.handle_connect(&connect);

        // Try publishing QoS 2 (server max is QoS 1)
        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic"),
            packet_id: Some(1),
            payload: Bytes::from_static(b"hello"),
            properties: Properties::default(),
        };
        let plan = session.handle_publish(&publish);
        assert!(
            plan.flat_message.is_empty(),
            "message should not be published"
        );
        assert_eq!(plan.responses.len(), 1);
        // Should get PUBREC with reason code 0x9B (QoS not supported)
        match &plan.responses[0] {
            MqttPacket::PubRec(pubrec) => {
                assert_eq!(pubrec.reason_code, Some(0x9B));
            }
            other => panic!("expected PubRec, got {:?}", other),
        }
    }

    #[test]
    fn test_publish_payload_format_utf8_validation() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        session.handle_connect(&connect);

        // Publish with payload_format_indicator=1 but invalid UTF-8 payload
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic"),
            packet_id: Some(1),
            payload: Bytes::from_static(&[0xFF, 0xFE, 0xFD]), // invalid UTF-8
            properties: Properties {
                payload_format_indicator: Some(1),
                ..Properties::default()
            },
        };
        let plan = session.handle_publish(&publish);
        assert!(plan.flat_message.is_empty());
        assert_eq!(plan.responses.len(), 1);
        match &plan.responses[0] {
            MqttPacket::PubAck(puback) => {
                assert_eq!(puback.reason_code, Some(0x99)); // Payload Format Invalid
            }
            other => panic!("expected PubAck, got {:?}", other),
        }
    }

    #[test]
    fn test_publish_payload_format_valid_utf8_passes() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        session.handle_connect(&connect);

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic"),
            packet_id: None,
            payload: Bytes::from_static(b"valid utf-8 string"),
            properties: Properties {
                payload_format_indicator: Some(1),
                ..Properties::default()
            },
        };
        let plan = session.handle_publish(&publish);
        assert!(
            !plan.flat_message.is_empty(),
            "valid UTF-8 should be published"
        );
    }

    #[test]
    fn test_topic_alias_zero_rejected_in_publish() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        session.handle_connect(&connect);

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic"),
            packet_id: None,
            payload: Bytes::from_static(b"hello"),
            properties: Properties {
                topic_alias: Some(0), // Invalid: 0 is not allowed
                ..Properties::default()
            },
        };
        let plan = session.handle_publish(&publish);
        assert!(
            plan.flat_message.is_empty(),
            "topic alias 0 should be rejected"
        );
        // MQTT 5.0: topic alias 0 should trigger a disconnect with Protocol Error.
        assert!(
            plan.disconnect.is_some(),
            "topic alias 0 should trigger disconnect"
        );
    }

    #[test]
    fn test_find_subscription_id_for_queue() {
        let mut session = MqttSession::new(Default::default());
        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        session.handle_connect(&connect);

        // Subscribe with subscription ID
        let filters: smallvec::SmallVec<[crate::types::TopicFilter; 4]> =
            smallvec::smallvec![crate::types::TopicFilter {
                filter: "test/+".to_string(),
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
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        session.handle_connect(&connect);

        // PUBREC for unknown ID should return reason code 0x92
        let pubrel = session.handle_pubrec(999);
        match pubrel {
            MqttPacket::PubRel(pr) => {
                assert_eq!(pr.reason_code, Some(0x92));
            }
            _ => panic!("expected PubRel"),
        }

        // PUBREL for unknown ID should return PUBCOMP with 0x92
        let pubcomp = session.handle_pubrel(888);
        match pubcomp {
            MqttPacket::PubComp(pc) => {
                assert_eq!(pc.reason_code, Some(0x92));
            }
            _ => panic!("expected PubComp"),
        }
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
        connect.properties.request_response_information = Some(1);
        let (_, connack) = session.handle_connect(&connect);
        match connack {
            MqttPacket::ConnAck(ca) => {
                assert!(
                    ca.properties.response_information.is_some(),
                    "expected response_information in CONNACK"
                );
                let ri = ca.properties.response_information.unwrap();
                assert!(
                    ri.contains("rri-test"),
                    "response_information should contain client_id"
                );
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_request_response_information_not_requested() {
        let mut session = MqttSession::new(Default::default());
        let connect = make_connect_v5("rri-test", true);
        let (_, connack) = session.handle_connect(&connect);
        match connack {
            MqttPacket::ConnAck(ca) => {
                assert!(
                    ca.properties.response_information.is_none(),
                    "should not include response_information unless requested"
                );
            }
            _ => panic!("expected ConnAck"),
        }
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
            topic: Bytes::from_static(b"test/topic"),
            packet_id: Some(1),
            payload: Bytes::from_static(b"hi"),
            properties: Properties::default(),
        };
        let plan = session.handle_publish(&publish);
        // handle_publish tracks QoS1 inbound in qos1_inflight and returns a PUBACK response.
        assert!(
            plan.responses
                .iter()
                .any(|r| matches!(r, MqttPacket::PubAck(_)))
        );

        // Now handle the PUBACK for the inbound message (this is the server's own ack).
        // For inbound messages, handle_puback should not return an MqCommand.
        let cmd = session.handle_puback(1);
        assert!(cmd.is_none());
    }

    #[test]
    fn test_puback_unknown_packet_id() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));
        // PUBACK for a packet_id that was never sent.
        let cmd = session.handle_puback(999);
        assert!(cmd.is_none());
    }

    #[test]
    fn test_puback_outbound_returns_ack_command() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Simulate an outbound QoS 1 delivery.
        let pkt = session.build_outbound_publish(
            "out/topic",
            Bytes::from_static(b"data"),
            QoS::AtLeastOnce,
            false,
            Some(42),  // mq_queue_id
            Some(100), // mq_message_id
        );
        let packet_id = match pkt {
            MqttPacket::Publish(p) => p.packet_id.unwrap(),
            _ => panic!("expected Publish"),
        };

        let cmd = session.handle_puback(packet_id);
        assert!(
            cmd.is_some(),
            "outbound PUBACK should produce an ACK command"
        );
    }

    #[test]
    fn test_pubrec_duplicate_in_pubrel_sent_state() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Manually set up a QoS 2 outbound in PublishSent state.
        let pkt = session.build_outbound_publish(
            "qos2/topic",
            Bytes::from_static(b"data"),
            QoS::ExactlyOnce,
            false,
            Some(10),
            Some(20),
        );
        let packet_id = match pkt {
            MqttPacket::Publish(p) => p.packet_id.unwrap(),
            _ => panic!("expected Publish"),
        };

        // First PUBREC transitions to PubRelSent.
        let _pubrel = session.handle_pubrec(packet_id);
        // Duplicate PUBREC while already in PubRelSent state.
        let pubrel2 = session.handle_pubrec(packet_id);
        // Should still return a PUBREL.
        assert!(matches!(pubrel2, MqttPacket::PubRel(_)));
    }

    #[test]
    fn test_pubrec_unknown_packet_id() {
        let mut session = make_session();
        let mut connect = make_connect_v5("c", true);
        connect.flags.clean_session = true;
        session.handle_connect(&connect);

        let pubrel = session.handle_pubrec(999);
        // V5 should return reason_code 0x92 (Packet Identifier Not Found).
        match pubrel {
            MqttPacket::PubRel(pr) => {
                assert_eq!(pr.reason_code, Some(0x92));
            }
            _ => panic!("expected PubRel"),
        }
    }

    #[test]
    fn test_pubrel_unknown_packet_id() {
        let mut session = make_session();
        let connect = make_connect_v5("c", true);
        session.handle_connect(&connect);

        let pubcomp = session.handle_pubrel(999);
        // V5 should return reason_code 0x92 (Packet Identifier Not Found).
        match pubcomp {
            MqttPacket::PubComp(pc) => {
                assert_eq!(pc.reason_code, Some(0x92));
            }
            _ => panic!("expected PubComp"),
        }
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
            topic: Bytes::from_static(b"qos2/in"),
            packet_id: Some(50),
            payload: Bytes::from_static(b"data"),
            properties: Properties::default(),
        };
        let _plan = session.handle_publish(&publish);
        // PUBREL completes the transaction.
        let _pubcomp = session.handle_pubrel(50);
        // Duplicate PUBREL should still succeed (already Complete state).
        let pubcomp2 = session.handle_pubrel(50);
        assert!(matches!(pubcomp2, MqttPacket::PubComp(_)));
    }

    #[test]
    fn test_pubcomp_protocol_violation_publish_sent_state() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Set up outbound QoS 2 in PublishSent state.
        let pkt = session.build_outbound_publish(
            "t",
            Bytes::from_static(b"d"),
            QoS::ExactlyOnce,
            false,
            Some(1),
            Some(2),
        );
        let packet_id = match pkt {
            MqttPacket::Publish(p) => p.packet_id.unwrap(),
            _ => panic!("expected Publish"),
        };

        // Send PUBCOMP without PUBREC first (protocol violation).
        let cmd = session.handle_pubcomp(packet_id);
        assert!(
            cmd.is_none(),
            "PUBCOMP in PublishSent should not produce ACK"
        );
    }

    #[test]
    fn test_pubcomp_unknown_packet_id() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));
        let cmd = session.handle_pubcomp(999);
        assert!(cmd.is_none());
    }

    #[test]
    fn test_clean_session_false_preserves_subscriptions() {
        let mut session = make_session();
        // First connect with clean_session=false.
        let connect = make_connect("persist", false);
        session.handle_connect(&connect);

        // Subscribe to a topic.
        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: "a/b".to_string(),
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
            filter: "x/y".to_string(),
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
            filter: "keep/me".to_string(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters, None);

        let (will_plan, commands) = session.handle_disconnect(Some(&Disconnect {
            reason_code: None,
            properties: Properties::default(),
        }));

        assert!(will_plan.is_none());
        // With clean_session=false, no delete_binding or delete_queue commands.
        let has_delete = commands.iter().any(|cmd| {
            cmd.tag() == bisque_mq::types::MqCommand::TAG_DELETE_BINDING
                || cmd.tag() == bisque_mq::types::MqCommand::TAG_DELETE_QUEUE
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
            filter: "upd/test".to_string(),
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
            filter: "local/test".to_string(),
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
            filter: "retain/test".to_string(),
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
        connect.properties.topic_alias_maximum = Some(2);
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
        let (_, connack) = session.handle_connect(&connect);
        match connack {
            MqttPacket::ConnAck(ca) => {
                // session_present is !clean_session, so true on first connect too.
                assert!(ca.session_present);
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_session_present_false_on_clean() {
        let mut session = make_session();
        let connect = make_connect("sp", true);
        let (_, connack) = session.handle_connect(&connect);
        match connack {
            MqttPacket::ConnAck(ca) => {
                assert!(!ca.session_present);
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_unclean_disconnect_nack_batching() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Build multiple outbound QoS 1 deliveries.
        for i in 0..3 {
            session.build_outbound_publish(
                "topic",
                Bytes::from_static(b"d"),
                QoS::AtLeastOnce,
                false,
                Some(42), // same queue
                Some(100 + i),
            );
        }

        let (_, commands) = session.handle_unclean_disconnect();
        // Should have disconnect_consumer, disconnect_producer, and NACK commands.
        let nack_count = commands
            .iter()
            .filter(|cmd| cmd.tag() == bisque_mq::types::MqCommand::TAG_NACK)
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

        session.build_outbound_publish(
            "t",
            Bytes::new(),
            QoS::AtLeastOnce,
            false,
            Some(1),
            Some(2),
        );
        let ids = session.pending_qos1_packet_ids();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn test_pending_qos2_packet_ids() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        session.build_outbound_publish(
            "t",
            Bytes::new(),
            QoS::ExactlyOnce,
            false,
            Some(1),
            Some(2),
        );
        let ids = session.pending_qos2_packet_ids();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn test_process_packet_unexpected_server_packets() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        // Server-originated packets from client should be silently ignored.
        let (cmds, pkts) = session.process_packet(&MqttPacket::PingResp);
        assert!(cmds.is_empty());
        assert!(pkts.is_empty());

        let (cmds, pkts) = session.process_packet(&MqttPacket::ConnAck(ConnAck {
            session_present: false,
            return_code: 0,
            properties: Properties::default(),
        }));
        assert!(cmds.is_empty());
        assert!(pkts.is_empty());

        let (cmds, pkts) = session.process_packet(&MqttPacket::SubAck(SubAck {
            packet_id: 1,
            return_codes: smallvec::smallvec![0],
            properties: Properties::default(),
        }));
        assert!(cmds.is_empty());
        assert!(pkts.is_empty());

        let (cmds, pkts) = session.process_packet(&MqttPacket::UnsubAck(UnsubAck {
            packet_id: 1,
            reason_codes: smallvec::smallvec![0],
            properties: Properties::default(),
        }));
        assert!(cmds.is_empty());
        assert!(pkts.is_empty());
    }

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
        connect.properties.receive_maximum = Some(50);
        session.handle_connect(&connect);
        // remaining_inflight should respect client_receive_maximum.
        assert!(session.remaining_inflight() <= 50);
    }

    #[test]
    fn test_unclean_disconnect_without_will() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        let (will_plan, commands) = session.handle_unclean_disconnect();
        assert!(will_plan.is_none());
        // Should still have disconnect_consumer and disconnect_producer.
        assert!(commands.len() >= 2);
        assert!(!session.connected);
    }

    #[test]
    fn test_v5_pubrec_v311_no_reason_code() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true)); // V3.1.1

        let pkt = session.build_outbound_publish(
            "t",
            Bytes::new(),
            QoS::ExactlyOnce,
            false,
            Some(1),
            Some(2),
        );
        let packet_id = match pkt {
            MqttPacket::Publish(p) => p.packet_id.unwrap(),
            _ => panic!("expected Publish"),
        };

        let pubrel = session.handle_pubrec(packet_id);
        match pubrel {
            MqttPacket::PubRel(pr) => {
                // V3.1.1 should not include reason_code.
                assert_eq!(pr.reason_code, None);
            }
            _ => panic!("expected PubRel"),
        }
    }

    #[test]
    fn test_build_outbound_publish_legacy() {
        let mut session = make_session();
        session.handle_connect(&make_connect("c", true));

        let pkt = session.build_outbound_publish(
            "legacy/topic",
            Bytes::from_static(b"payload"),
            QoS::AtMostOnce,
            true,
            None,
            None,
        );
        match pkt {
            MqttPacket::Publish(p) => {
                assert_eq!(&p.topic[..], b"legacy/topic");
                assert_eq!(&p.payload[..], b"payload");
                assert!(p.retain);
                assert_eq!(p.qos, QoS::AtMostOnce);
                assert!(p.packet_id.is_none());
            }
            _ => panic!("expected Publish"),
        }
    }
}
