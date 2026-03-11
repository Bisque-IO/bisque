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

use crate::types::{
    ConnAck, Connect, ConnectReturnCode, MqttPacket, Properties, ProtocolVersion, PubAck, PubComp,
    PubRec, PubRel, Publish, QoS, SubAck, UnsubAck, WillMessage,
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
}

impl Default for MqttSessionConfig {
    fn default() -> Self {
        Self {
            max_inflight: 65535,
            max_packet_size: 1024 * 1024,
            default_keep_alive: 60,
            topic_prefix: Arc::from("mqtt/"),
            retained_prefix: Arc::from("$mqtt/retained/"),
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
    /// (exchange_id, routing_key) -> binding_id
    bindings: HashMap<(u64, String), u64>,
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
    /// Session expiry interval in seconds (0 = expire on disconnect).
    pub session_expiry_interval: u32,

    // -- Performance counters --
    /// O(1) counter for outbound QoS 1 in-flight messages.
    /// Maintained on insert/remove to avoid iterating qos1_inflight.
    outbound_qos1_count: usize,
    /// Cached first subscription identifier for O(1) outbound delivery lookup.
    cached_first_sub_id: Option<u32>,
}

impl MqttSession {
    /// Create a new unconnected session.
    pub fn new(config: MqttSessionConfig) -> Self {
        Self {
            session_id: NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed),
            client_id: String::new(),
            protocol_version: ProtocolVersion::V311,
            clean_session: true,
            keep_alive: config.default_keep_alive,
            will: None,
            config,
            connected: false,
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
            session_expiry_interval: 0,
            outbound_qos1_count: 0,
            cached_first_sub_id: None,
        }
    }

    /// Allocate the next outbound packet identifier.
    fn alloc_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
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
        format!("{}{}", self.config.retained_prefix, mqtt_topic)
    }

    /// Map an MQTT topic filter to a bisque-mq queue name.
    fn subscription_queue_name(&self, topic_filter: &str, shared_group: Option<&str>) -> String {
        if let Some(group) = shared_group {
            // Shared subscriptions use a group-scoped queue (shared across clients).
            format!("mqtt/shared/{}/{}", group, topic_filter.replace('/', "."))
        } else {
            // Per-client queue for individual subscriptions.
            format!(
                "mqtt/sub/{}/{}",
                self.client_id,
                topic_filter.replace('/', ".")
            )
        }
    }

    /// Translate MQTT topic filter wildcards to bisque-mq exchange pattern.
    /// MQTT uses `+` for single-level wildcard; bisque-mq uses `*`.
    /// Both use `#` for multi-level wildcard.
    fn mqtt_filter_to_mq_pattern(filter: &str) -> String {
        filter.replace('+', "*")
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
            .insert((exchange_id, routing_key.to_string()), binding_id);
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

    /// Look up a cached binding ID (zero-alloc: iterates instead of allocating key).
    pub fn cached_binding_id(&self, exchange_id: u64, routing_key: &str) -> Option<u64> {
        // Avoid allocating a (u64, String) key for the lookup by iterating.
        // The binding cache is small (typically <10 entries per session).
        for ((eid, rk), &bid) in &self.entity_cache.bindings {
            if *eid == exchange_id && rk == routing_key {
                return Some(bid);
            }
        }
        None
    }

    // =========================================================================
    // CONNECT handling
    // =========================================================================

    /// Handle an incoming CONNECT packet.
    ///
    /// Returns a list of `MqCommand`s to execute and the CONNACK response packet.
    pub fn handle_connect(&mut self, connect: &Connect) -> (SmallVec<[MqCommand; 4]>, MqttPacket) {
        self.client_id = connect.client_id.clone();
        self.protocol_version = connect.protocol_version;
        self.clean_session = connect.flags.clean_session;
        self.will = connect.will.clone();
        self.connected = true;

        // Negotiate keep-alive
        self.keep_alive = if connect.keep_alive == 0 {
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
        if let Some(sei) = connect.properties.session_expiry_interval {
            self.session_expiry_interval = sei;
        }

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
            // Reset topic alias maps (correctness: old aliases invalid on reconnect).
            self.outbound_topic_aliases.clear();
            self.inbound_topic_aliases.clear();
            self.next_outbound_alias = 1;
        }

        // Build CONNACK with capabilities for MQTT 5.0.
        let properties = if connect.protocol_version == ProtocolVersion::V5 {
            Properties {
                maximum_qos: Some(2),
                retain_available: Some(true),
                maximum_packet_size: Some(self.config.max_packet_size as u32),
                topic_alias_maximum: Some(65535),
                wildcard_subscription_available: Some(true),
                subscription_identifier_available: Some(true),
                shared_subscription_available: Some(true),
                receive_maximum: Some(self.config.max_inflight.min(65535) as u16),
                server_keep_alive: Some(self.keep_alive),
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
    // PUBLISH handling
    // =========================================================================

    /// Handle an incoming PUBLISH packet from the client.
    ///
    /// Returns a `PublishPlan` for the server to orchestrate (resolve entity IDs,
    /// submit commands, send responses).
    pub fn handle_publish(&mut self, publish: &Publish) -> PublishPlan {
        // Resolve topic alias if present (MQTT 5.0).
        // Topic is Bytes (UTF-8 validated on decode).
        let topic: Bytes = if let Some(alias) = publish.properties.topic_alias {
            if !publish.topic.is_empty() {
                self.inbound_topic_aliases
                    .insert(alias, publish.topic.clone());
                publish.topic.clone()
            } else if let Some(resolved) = self.inbound_topic_aliases.get(&alias) {
                resolved.clone()
            } else {
                warn!(alias, "unknown inbound topic alias");
                return PublishPlan {
                    exchange_name: MQTT_EXCHANGE_NAME,
                    cached_exchange_id: None,
                    flat_message: Bytes::new(),
                    responses: SmallVec::new(),
                    retained: None,
                };
            }
        } else {
            publish.topic.clone()
        };

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
                    responses.push(MqttPacket::PubAck(PubAck {
                        packet_id,
                        reason_code: None,
                        properties: Properties::default(),
                    }));
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = publish.packet_id {
                    self.qos2_inbound
                        .insert(packet_id, QoS2InboundState::PubRecSent);
                    responses.push(MqttPacket::PubRec(PubRec {
                        packet_id,
                        reason_code: None,
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
        }

        MqttPacket::PubRel(PubRel {
            packet_id,
            reason_code: None,
            properties: Properties::default(),
        })
    }

    /// Handle PUBREL from client (QoS 2 inbound, step 2).
    ///
    /// Returns the PUBCOMP response packet. No MqCommands needed at this stage.
    pub fn handle_pubrel(&mut self, packet_id: u16) -> MqttPacket {
        if let Some(state) = self.qos2_inbound.get(&packet_id) {
            match state {
                QoS2InboundState::PubRecSent => {
                    // Message was already published on PUBLISH receive.
                    // Mark complete.
                }
                QoS2InboundState::Complete => {
                    debug!(packet_id, "duplicate PUBREL (already complete)");
                }
            }
        } else {
            warn!(packet_id, "PUBREL for unknown packet ID");
        }

        self.qos2_inbound
            .insert(packet_id, QoS2InboundState::Complete);

        MqttPacket::PubComp(PubComp {
            packet_id,
            reason_code: None,
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
            // Check for shared subscription.
            let (shared_group, actual_filter) =
                if let Some((group, f)) = Self::parse_shared_subscription(&filter.filter) {
                    (Some(group.to_string()), f.to_string())
                } else {
                    (None, filter.filter.clone())
                };

            // Translate MQTT wildcards to bisque-mq pattern.
            let routing_key = Self::mqtt_filter_to_mq_pattern(&actual_filter);

            // Queue name: shared or per-client.
            let queue_name = self.subscription_queue_name(&actual_filter, shared_group.as_deref());

            let cached_queue_id = self.entity_cache.queues.get(&queue_name).copied();
            let cached_exchange_id = self.entity_cache.exchanges.get(MQTT_EXCHANGE_NAME).copied();
            let cached_binding_id = cached_exchange_id.and_then(|eid| {
                self.entity_cache
                    .bindings
                    .get(&(eid, routing_key.clone()))
                    .copied()
            });

            let queue_config = bisque_mq::config::QueueConfig::default();

            filter_plans.push(SubscribeFilterPlan {
                filter: filter.filter.clone(),
                queue_name,
                cached_queue_id,
                routing_key,
                cached_binding_id,
                qos: filter.qos,
                queue_config,
                shared_group: shared_group.clone(),
                subscription_id,
            });

            // Track the subscription.
            self.subscriptions.insert(
                filter.filter.clone(),
                SubscriptionMapping {
                    filter: actual_filter,
                    max_qos: filter.qos,
                    exchange_id: cached_exchange_id,
                    binding_id: cached_binding_id,
                    queue_id: cached_queue_id,
                    topic_id: None,
                    shared_group,
                    subscription_id,
                },
            );

            return_codes.push(filter.qos.as_u8());

            // Update cached subscription ID for O(1) outbound lookup.
            if subscription_id.is_some() && self.cached_first_sub_id.is_none() {
                self.cached_first_sub_id = subscription_id;
            }

            debug!(
                client_id = %self.client_id,
                filter = %filter.filter,
                qos = ?filter.qos,
                "subscription added"
            );
        }

        let suback = MqttPacket::SubAck(SubAck {
            packet_id,
            return_codes,
            properties: Properties::default(),
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

        let unsuback = MqttPacket::UnsubAck(UnsubAck {
            packet_id,
            reason_codes,
            properties: Properties::default(),
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
    pub fn handle_disconnect(&mut self) -> SmallVec<[MqCommand; 8]> {
        self.will = None;
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
            "MQTT session disconnected (clean)"
        );

        commands
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

        // Group NACKs for outbound QoS 1 messages by queue_id.
        let mut nack_groups: HashMap<u64, SmallVec<[u64; 4]>> = HashMap::new();
        for inflight in self.qos1_inflight.values() {
            if inflight.direction == Direction::Outbound {
                if let (Some(qid), Some(mid)) = (inflight.mq_queue_id, inflight.mq_message_id) {
                    nack_groups.entry(qid).or_default().push(mid);
                }
            }
        }
        for (queue_id, message_ids) in nack_groups {
            commands.push(MqCommand::nack(queue_id, &message_ids));
        }

        // NACK outbound QoS 2 messages.
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
            commands.push(MqCommand::nack(qid, &[mid]));
        }

        self.qos1_inflight.clear();
        self.qos2_inbound.clear();
        self.qos2_outbound.clear();
        self.outbound_qos1_count = 0;

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

            // Use cached subscription identifier (O(1) instead of iterating).
            if let Some(sub_id) = self.cached_first_sub_id {
                properties.subscription_identifier = Some(sub_id);
            }

            // Outbound topic alias assignment (zero-alloc lookup via Bytes key).
            if self.max_topic_alias > 0 {
                if let Some(&alias) = self.outbound_topic_aliases.get(&topic[..]) {
                    properties.topic_alias = Some(alias);
                } else if self.next_outbound_alias <= self.max_topic_alias {
                    let alias = self.next_outbound_alias;
                    self.next_outbound_alias += 1;
                    self.outbound_topic_aliases.insert(topic.clone(), alias);
                    properties.topic_alias = Some(alias);
                }
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
    pub fn resolve_outbound_topic_alias(&mut self, topic: &[u8]) -> Option<u16> {
        if self.max_topic_alias == 0 {
            return None;
        }
        if let Some(&alias) = self.outbound_topic_aliases.get(topic) {
            Some(alias)
        } else if self.next_outbound_alias <= self.max_topic_alias {
            let alias = self.next_outbound_alias;
            self.next_outbound_alias += 1;
            self.outbound_topic_aliases
                .insert(Bytes::copy_from_slice(topic), alias);
            Some(alias)
        } else {
            None
        }
    }

    /// Find a subscription identifier to include in outbound PUBLISH (V5).
    /// O(1) via cached value instead of iterating all subscriptions.
    pub fn find_subscription_id(&self) -> Option<u32> {
        self.cached_first_sub_id
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
    pub fn outbound_inflight_count(&self) -> usize {
        self.outbound_qos1_count + self.qos2_outbound.len()
    }

    /// Check if the session has exceeded its inflight limit.
    pub fn is_inflight_full(&self) -> bool {
        let outbound = self.outbound_inflight_count();
        outbound
            >= self
                .config
                .max_inflight
                .min(self.client_receive_maximum as usize)
    }

    /// Remaining capacity for outbound in-flight messages.
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
            MqttPacket::Disconnect(_disconnect) => {
                let cmds = self.handle_disconnect();
                (SmallVec::from_iter(cmds), SmallVec::new())
            }
            // AUTH is only valid in MQTT 5.0 enhanced authentication flows,
            // which we do not support. Respond with DISCONNECT reason code 0x8C
            // (Bad authentication method).
            MqttPacket::Auth => {
                warn!("received AUTH packet — enhanced auth not supported, disconnecting");
                let disconnect = MqttPacket::Disconnect(crate::types::Disconnect {
                    reason_code: Some(0x8C),
                    properties: Default::default(),
                });
                let cmds = self.handle_disconnect();
                (SmallVec::from_iter(cmds), smallvec::smallvec![disconnect])
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

        let commands = session.handle_disconnect();
        assert!(!session.connected);
        assert!(session.will.is_none());
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
}
