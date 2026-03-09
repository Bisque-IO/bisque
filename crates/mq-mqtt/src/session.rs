//! MQTT session — per-connection state and MQTT-to-MqCommand translation.
//!
//! Each TCP connection gets an `MqttSession` that tracks:
//! - Client identity and protocol version
//! - Active subscriptions (topic filter -> bisque-mq entity mappings)
//! - Will message for unclean disconnect
//! - In-flight QoS 1/2 packet tracking
//! - MQTT 5.0 topic alias mappings

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bisque_mq::MqCommand;
use bisque_mq::types::{ExchangeType, MessagePayload, RetentionPolicy};
use bytes::Bytes;
use tracing::{debug, warn};

use crate::types::{
    ConnAck, Connect, ConnectReturnCode, MqttPacket, Properties, ProtocolVersion, PubAck, PubComp,
    PubRec, Publish, QoS, SubAck, UnsubAck, WillMessage,
};

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
    pub topic_prefix: String,
    /// Prefix for retained message topics.
    pub retained_prefix: String,
}

impl Default for MqttSessionConfig {
    fn default() -> Self {
        Self {
            max_inflight: 65535,
            max_packet_size: 1024 * 1024,
            default_keep_alive: 60,
            topic_prefix: "mqtt/".to_string(),
            retained_prefix: "$mqtt/retained/".to_string(),
        }
    }
}

// =============================================================================
// In-flight Message Tracking
// =============================================================================

/// State of a QoS 1 message in flight (waiting for PUBACK).
#[derive(Debug, Clone)]
struct QoS1InFlight {
    /// The bisque-mq message_id returned from enqueue, used for ACK.
    mq_message_id: Option<u64>,
    /// The bisque-mq queue_id this was enqueued to.
    mq_queue_id: Option<u64>,
}

/// State of a QoS 2 message in the two-phase handshake.
#[derive(Debug, Clone)]
enum QoS2State {
    /// PUBLISH received, PUBREC sent, waiting for PUBREL.
    PubRecSent {
        mq_message_id: Option<u64>,
        mq_queue_id: Option<u64>,
    },
    /// PUBREL received, PUBCOMP sent, transaction complete.
    Complete,
}

// =============================================================================
// Subscription Mapping
// =============================================================================

/// Tracks a single MQTT subscription's mapping to bisque-mq entities.
#[derive(Debug, Clone)]
pub struct SubscriptionMapping {
    /// The MQTT topic filter string.
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
}

// =============================================================================
// MqttSession
// =============================================================================

/// Global session ID counter.
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Per-connection MQTT session state.
pub struct MqttSession {
    /// Unique session ID (used as bisque-mq consumer_id).
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

    // -- Subscription state --
    /// topic_filter -> SubscriptionMapping
    subscriptions: HashMap<String, SubscriptionMapping>,

    // -- In-flight QoS tracking --
    /// QoS 1: packet_id -> in-flight state
    qos1_inflight: HashMap<u16, QoS1InFlight>,
    /// QoS 2: packet_id -> handshake state
    qos2_inflight: HashMap<u16, QoS2State>,

    // -- Packet ID generation --
    /// Next outbound packet ID (server -> client).
    next_packet_id: u16,

    // -- MQTT 5.0 topic alias --
    /// Inbound topic alias map (alias number -> topic string), set by client.
    inbound_topic_aliases: HashMap<u16, String>,
    /// Outbound topic alias map (topic string -> alias number), set by server.
    outbound_topic_aliases: HashMap<String, u16>,
    next_outbound_alias: u16,
    max_topic_alias: u16,
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
            subscriptions: HashMap::new(),
            qos1_inflight: HashMap::new(),
            qos2_inflight: HashMap::new(),
            next_packet_id: 1,
            inbound_topic_aliases: HashMap::new(),
            outbound_topic_aliases: HashMap::new(),
            next_outbound_alias: 1,
            max_topic_alias: 0,
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
    fn mq_topic_name(&self, mqtt_topic: &str) -> String {
        format!("{}{}", self.config.topic_prefix, mqtt_topic)
    }

    /// Map an MQTT topic string to a retained message bisque-mq topic name.
    fn retained_topic_name(&self, mqtt_topic: &str) -> String {
        format!("{}{}", self.config.retained_prefix, mqtt_topic)
    }

    /// Map an MQTT topic filter to a bisque-mq queue name (for QoS 1/2 subscriptions).
    fn subscription_queue_name(&self, topic_filter: &str) -> String {
        format!(
            "mqtt/sub/{}/{}",
            self.client_id,
            topic_filter.replace('/', ".")
        )
    }

    // =========================================================================
    // CONNECT handling
    // =========================================================================

    /// Handle an incoming CONNECT packet.
    ///
    /// Returns a list of `MqCommand`s to execute and the CONNACK response packet.
    pub fn handle_connect(&mut self, connect: &Connect) -> (Vec<MqCommand>, MqttPacket) {
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

        // MQTT 5.0: extract topic alias maximum from connect properties
        if let Some(max) = connect.properties.topic_alias_maximum {
            self.max_topic_alias = max;
        }

        let mut commands = Vec::new();

        // Register this session as a bisque-mq consumer.
        commands.push(MqCommand::RegisterConsumer {
            consumer_id: self.session_id,
            group_name: format!("mqtt/{}", self.client_id),
            subscriptions: Vec::new(),
        });

        // Register as a producer for publishing.
        commands.push(MqCommand::RegisterProducer {
            producer_id: self.session_id,
            name: Some(format!("mqtt/{}", self.client_id)),
        });

        // If clean session, clear any old subscription state.
        if self.clean_session {
            self.subscriptions.clear();
            self.qos1_inflight.clear();
            self.qos2_inflight.clear();
        }

        let connack = MqttPacket::ConnAck(ConnAck {
            session_present: !self.clean_session,
            return_code: ConnectReturnCode::Accepted as u8,
            properties: Properties::default(),
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
    /// Returns MqCommands to execute and optional response packets.
    pub fn handle_publish(&mut self, publish: &Publish) -> (Vec<MqCommand>, Vec<MqttPacket>) {
        let mut commands = Vec::new();
        let mut responses = Vec::new();

        // Resolve topic alias if present (MQTT 5.0).
        let topic = if let Some(alias) = publish.properties.topic_alias {
            if !publish.topic.is_empty() {
                // Client is defining a new alias mapping.
                self.inbound_topic_aliases
                    .insert(alias, publish.topic.clone());
                publish.topic.clone()
            } else if let Some(resolved) = self.inbound_topic_aliases.get(&alias) {
                resolved.clone()
            } else {
                warn!(alias, "unknown inbound topic alias");
                return (commands, responses);
            }
        } else {
            publish.topic.clone()
        };

        let now = Self::now_ms();

        // Build the MessagePayload.
        let mut headers = Vec::new();
        if let Some(ref rt) = publish.properties.response_topic {
            headers.push(("mqtt.response_topic".to_string(), Bytes::from(rt.clone())));
        }
        if let Some(ref cd) = publish.properties.correlation_data {
            headers.push(("mqtt.correlation_data".to_string(), cd.clone()));
        }
        if let Some(ref ct) = publish.properties.content_type {
            headers.push(("mqtt.content_type".to_string(), Bytes::from(ct.clone())));
        }
        for (key, val) in &publish.properties.user_properties {
            headers.push((format!("mqtt.user.{}", key), Bytes::from(val.clone())));
        }

        let ttl_ms = publish
            .properties
            .message_expiry_interval
            .map(|secs| secs as u64 * 1000);

        let payload = MessagePayload {
            key: None,
            value: publish.payload.clone(),
            headers,
            timestamp: now,
            ttl_ms,
            routing_key: Some(topic.clone()),
        };

        let mq_topic_name = self.mq_topic_name(&topic);

        match publish.qos {
            QoS::AtMostOnce => {
                // QoS 0: fire-and-forget publish to bisque-mq topic.
                commands.push(MqCommand::CreateTopic {
                    name: mq_topic_name.clone(),
                    retention: RetentionPolicy::default(),
                });
                // We use a placeholder topic_id of 0 here. In practice, the
                // session would maintain a cache of topic_name -> topic_id
                // resolved from CreateTopic responses or a lookup command.
                // For now, we use PublishToExchange via the MQTT exchange.
                commands.push(MqCommand::CreateExchange {
                    name: format!("mqtt/exchange/{}", topic),
                    exchange_type: ExchangeType::Topic,
                });
            }
            QoS::AtLeastOnce => {
                // QoS 1: enqueue to a queue for reliable delivery.
                let queue_name = format!("mqtt/pub/{}", topic.replace('/', "."));
                commands.push(MqCommand::CreateQueue {
                    name: queue_name,
                    config: bisque_mq::config::QueueConfig::default(),
                });
                commands.push(MqCommand::Enqueue {
                    queue_id: 0, // Resolved after CreateQueue
                    messages: vec![payload.clone()],
                    dedup_keys: vec![None],
                });

                // Track in-flight and send PUBACK immediately.
                if let Some(packet_id) = publish.packet_id {
                    self.qos1_inflight.insert(
                        packet_id,
                        QoS1InFlight {
                            mq_message_id: None,
                            mq_queue_id: None,
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
                // QoS 2: enqueue with dedup key for exactly-once.
                if let Some(packet_id) = publish.packet_id {
                    let dedup_key = Bytes::from(format!("mqtt2:{}:{}", self.client_id, packet_id));

                    let queue_name = format!("mqtt/pub/{}", topic.replace('/', "."));
                    commands.push(MqCommand::CreateQueue {
                        name: queue_name,
                        config: bisque_mq::config::QueueConfig {
                            dedup_window_secs: Some(300), // 5-minute dedup window
                            ..bisque_mq::config::QueueConfig::default()
                        },
                    });
                    commands.push(MqCommand::Enqueue {
                        queue_id: 0, // Resolved after CreateQueue
                        messages: vec![payload.clone()],
                        dedup_keys: vec![Some(dedup_key)],
                    });

                    // Send PUBREC (step 1 of QoS 2).
                    self.qos2_inflight.insert(
                        packet_id,
                        QoS2State::PubRecSent {
                            mq_message_id: None,
                            mq_queue_id: None,
                        },
                    );
                    responses.push(MqttPacket::PubRec(PubRec {
                        packet_id,
                        reason_code: None,
                        properties: Properties::default(),
                    }));
                }
            }
        }

        // Handle retained messages.
        if publish.retain {
            if publish.payload.is_empty() {
                // Empty payload clears the retained message.
                debug!(topic = %topic, "clearing retained message");
            } else {
                let retained_name = self.retained_topic_name(&topic);
                commands.push(MqCommand::CreateTopic {
                    name: retained_name,
                    retention: RetentionPolicy {
                        max_messages: Some(1), // Keep only latest
                        ..RetentionPolicy::default()
                    },
                });
            }
        }

        (commands, responses)
    }

    // =========================================================================
    // PUBACK / PUBREC / PUBREL / PUBCOMP handling
    // =========================================================================

    /// Handle PUBACK from client (QoS 1 acknowledgment for server -> client delivery).
    pub fn handle_puback(&mut self, packet_id: u16) -> Vec<MqCommand> {
        let mut commands = Vec::new();

        if let Some(inflight) = self.qos1_inflight.remove(&packet_id) {
            // ACK the message in bisque-mq.
            if let (Some(queue_id), Some(message_id)) =
                (inflight.mq_queue_id, inflight.mq_message_id)
            {
                commands.push(MqCommand::Ack {
                    queue_id,
                    message_ids: vec![message_id],
                });
            }
            debug!(packet_id, "QoS 1 delivery acknowledged");
        } else {
            warn!(packet_id, "PUBACK for unknown packet ID");
        }

        commands
    }

    /// Handle PUBREL from client (QoS 2, step 2).
    ///
    /// Returns MqCommands and the PUBCOMP response.
    pub fn handle_pubrel(&mut self, packet_id: u16) -> (Vec<MqCommand>, MqttPacket) {
        let mut commands = Vec::new();

        if let Some(state) = self.qos2_inflight.get(&packet_id) {
            match state {
                QoS2State::PubRecSent {
                    mq_message_id,
                    mq_queue_id,
                } => {
                    // ACK the message in bisque-mq now that client confirmed receipt.
                    if let (Some(queue_id), Some(message_id)) = (mq_queue_id, mq_message_id) {
                        commands.push(MqCommand::Ack {
                            queue_id: *queue_id,
                            message_ids: vec![*message_id],
                        });
                    }
                }
                QoS2State::Complete => {
                    debug!(packet_id, "duplicate PUBREL (already complete)");
                }
            }
        } else {
            warn!(packet_id, "PUBREL for unknown packet ID");
        }

        // Mark as complete and send PUBCOMP.
        self.qos2_inflight.insert(packet_id, QoS2State::Complete);

        let pubcomp = MqttPacket::PubComp(PubComp {
            packet_id,
            reason_code: None,
            properties: Properties::default(),
        });

        (commands, pubcomp)
    }

    /// Handle PUBCOMP from client (QoS 2 complete, server -> client direction).
    pub fn handle_pubcomp(&mut self, packet_id: u16) {
        if self.qos2_inflight.remove(&packet_id).is_some() {
            debug!(packet_id, "QoS 2 outbound delivery complete");
        } else {
            warn!(packet_id, "PUBCOMP for unknown packet ID");
        }
    }

    // =========================================================================
    // SUBSCRIBE handling
    // =========================================================================

    /// Handle a SUBSCRIBE packet.
    ///
    /// Returns MqCommands to set up the subscriptions and a SUBACK response.
    pub fn handle_subscribe(
        &mut self,
        packet_id: u16,
        filters: &[crate::types::TopicFilter],
    ) -> (Vec<MqCommand>, MqttPacket) {
        let mut commands = Vec::new();
        let mut return_codes = Vec::new();

        for filter in filters {
            // Create a topic exchange for MQTT routing (if not exists).
            let exchange_name = format!("mqtt/exchange/{}", self.mq_topic_name(&filter.filter));
            commands.push(MqCommand::CreateExchange {
                name: exchange_name,
                exchange_type: ExchangeType::Topic,
            });

            // For QoS 1/2, create a per-client subscription queue.
            if filter.qos != QoS::AtMostOnce {
                let queue_name = self.subscription_queue_name(&filter.filter);
                commands.push(MqCommand::CreateQueue {
                    name: queue_name,
                    config: bisque_mq::config::QueueConfig::default(),
                });

                // Create a binding from the exchange to this queue.
                commands.push(MqCommand::CreateBinding {
                    exchange_id: 0, // Resolved after CreateExchange response
                    queue_id: 0,    // Resolved after CreateQueue response
                    routing_key: Some(filter.filter.clone()),
                });
            }

            // Track the subscription.
            self.subscriptions.insert(
                filter.filter.clone(),
                SubscriptionMapping {
                    filter: filter.filter.clone(),
                    max_qos: filter.qos,
                    exchange_id: None,
                    binding_id: None,
                    queue_id: None,
                    topic_id: None,
                },
            );

            // Return granted QoS.
            return_codes.push(filter.qos.as_u8());

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

        (commands, suback)
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
    ) -> (Vec<MqCommand>, MqttPacket) {
        let mut commands = Vec::new();
        let mut reason_codes = Vec::new();

        for filter in filters {
            if let Some(mapping) = self.subscriptions.remove(filter) {
                // Delete the exchange binding.
                if let Some(binding_id) = mapping.binding_id {
                    commands.push(MqCommand::DeleteBinding { binding_id });
                }
                // Optionally delete the subscription queue (only if clean session).
                if self.clean_session {
                    if let Some(queue_id) = mapping.queue_id {
                        commands.push(MqCommand::DeleteQueue { queue_id });
                    }
                }
                reason_codes.push(0x00); // Success
                debug!(
                    client_id = %self.client_id,
                    filter = %filter,
                    "subscription removed"
                );
            } else {
                // No subscription found — MQTT 5.0 says this is not an error.
                reason_codes.push(0x11); // No subscription existed
            }
        }

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
    ///
    /// Returns a Heartbeat MqCommand and PINGRESP.
    pub fn handle_pingreq(&self) -> (MqCommand, MqttPacket) {
        let cmd = MqCommand::Heartbeat {
            consumer_id: self.session_id,
        };
        (cmd, MqttPacket::PingResp)
    }

    // =========================================================================
    // DISCONNECT handling
    // =========================================================================

    /// Handle a DISCONNECT packet (clean disconnect).
    ///
    /// Returns MqCommands to clean up the session. Will message is discarded.
    pub fn handle_disconnect(&mut self) -> Vec<MqCommand> {
        // Clean disconnect — discard will message.
        self.will = None;
        self.connected = false;

        let mut commands = Vec::new();

        // Disconnect the consumer and producer.
        commands.push(MqCommand::DisconnectConsumer {
            consumer_id: self.session_id,
        });
        commands.push(MqCommand::DisconnectProducer {
            producer_id: self.session_id,
        });

        // If clean session, remove all subscription bindings and queues.
        if self.clean_session {
            for mapping in self.subscriptions.values() {
                if let Some(binding_id) = mapping.binding_id {
                    commands.push(MqCommand::DeleteBinding { binding_id });
                }
                if let Some(queue_id) = mapping.queue_id {
                    commands.push(MqCommand::DeleteQueue { queue_id });
                }
            }
            self.subscriptions.clear();
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
    /// Returns MqCommands including will message publication.
    pub fn handle_unclean_disconnect(&mut self) -> Vec<MqCommand> {
        let mut commands = Vec::new();

        // Publish will message if present.
        if let Some(ref will) = self.will {
            let now = Self::now_ms();
            let payload = MessagePayload {
                key: None,
                value: will.payload.clone(),
                headers: Vec::new(),
                timestamp: now,
                ttl_ms: None,
                routing_key: Some(will.topic.clone()),
            };

            let mq_topic_name = self.mq_topic_name(&will.topic);
            commands.push(MqCommand::CreateTopic {
                name: mq_topic_name,
                retention: RetentionPolicy::default(),
            });

            // For will messages, we use a topic publish via exchange for fan-out.
            commands.push(MqCommand::CreateExchange {
                name: format!("mqtt/exchange/{}", will.topic),
                exchange_type: ExchangeType::Topic,
            });

            // Publish the will message through the exchange.
            // exchange_id 0 is a placeholder; resolved after CreateExchange.
            commands.push(MqCommand::PublishToExchange {
                exchange_id: 0,
                messages: vec![payload],
            });

            debug!(
                client_id = %self.client_id,
                will_topic = %will.topic,
                "publishing will message"
            );
        }
        self.will = None;
        self.connected = false;

        // Disconnect consumer/producer.
        commands.push(MqCommand::DisconnectConsumer {
            consumer_id: self.session_id,
        });
        commands.push(MqCommand::DisconnectProducer {
            producer_id: self.session_id,
        });

        // NACK any in-flight QoS 1 messages so they return to pending.
        for inflight in self.qos1_inflight.values() {
            if let (Some(queue_id), Some(message_id)) =
                (inflight.mq_queue_id, inflight.mq_message_id)
            {
                commands.push(MqCommand::Nack {
                    queue_id,
                    message_ids: vec![message_id],
                });
            }
        }
        self.qos1_inflight.clear();
        self.qos2_inflight.clear();

        debug!(
            client_id = %self.client_id,
            session_id = self.session_id,
            "MQTT session disconnected (unclean)"
        );

        commands
    }

    // =========================================================================
    // Outbound message delivery (bisque-mq -> MQTT client)
    // =========================================================================

    /// Build an outbound PUBLISH packet for delivering a bisque-mq message to
    /// the MQTT client.
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
                        },
                    );
                }
                QoS::ExactlyOnce => {
                    self.qos2_inflight.insert(
                        id,
                        QoS2State::PubRecSent {
                            mq_message_id,
                            mq_queue_id,
                        },
                    );
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
            topic: topic.to_string(),
            packet_id,
            payload,
            properties: Properties::default(),
        })
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

    /// Return the number of in-flight QoS 2 messages.
    pub fn qos2_inflight_count(&self) -> usize {
        self.qos2_inflight.len()
    }

    /// Check if the session has exceeded its inflight limit.
    pub fn is_inflight_full(&self) -> bool {
        self.qos1_inflight.len() + self.qos2_inflight.len() >= self.config.max_inflight
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
    ///
    /// This is the main dispatch method that routes decoded packets to the
    /// appropriate handler.
    pub fn process_packet(&mut self, packet: &MqttPacket) -> (Vec<MqCommand>, Vec<MqttPacket>) {
        match packet {
            MqttPacket::Connect(connect) => {
                let (cmds, connack) = self.handle_connect(connect);
                (cmds, vec![connack])
            }
            MqttPacket::Publish(publish) => self.handle_publish(publish),
            MqttPacket::PubAck(puback) => {
                let cmds = self.handle_puback(puback.packet_id);
                (cmds, vec![])
            }
            MqttPacket::PubRel(pubrel) => {
                let (cmds, pubcomp) = self.handle_pubrel(pubrel.packet_id);
                (cmds, vec![pubcomp])
            }
            MqttPacket::PubComp(pubcomp) => {
                self.handle_pubcomp(pubcomp.packet_id);
                (vec![], vec![])
            }
            MqttPacket::Subscribe(subscribe) => {
                let (cmds, suback) = self.handle_subscribe(subscribe.packet_id, &subscribe.filters);
                (cmds, vec![suback])
            }
            MqttPacket::Unsubscribe(unsubscribe) => {
                let (cmds, unsuback) =
                    self.handle_unsubscribe(unsubscribe.packet_id, &unsubscribe.filters);
                (cmds, vec![unsuback])
            }
            MqttPacket::PingReq => {
                let (cmd, pong) = self.handle_pingreq();
                (vec![cmd], vec![pong])
            }
            MqttPacket::Disconnect(_disconnect) => {
                let cmds = self.handle_disconnect();
                (cmds, vec![])
            }
            // Server-originated packets should not arrive from client.
            MqttPacket::ConnAck(_)
            | MqttPacket::PubRec(_)
            | MqttPacket::SubAck(_)
            | MqttPacket::UnsubAck(_)
            | MqttPacket::PingResp
            | MqttPacket::Auth => {
                warn!(
                    packet_type = ?packet.packet_type(),
                    "unexpected packet from client"
                );
                (vec![], vec![])
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

    #[test]
    fn test_session_connect() {
        let mut session = make_session();
        let connect = make_connect("test-client", true);

        let (commands, response) = session.handle_connect(&connect);

        assert!(session.connected);
        assert_eq!(session.client_id, "test-client");
        assert!(session.clean_session);
        assert_eq!(session.keep_alive, 60);

        // Should have RegisterConsumer + RegisterProducer
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], MqCommand::RegisterConsumer { .. }));
        assert!(matches!(commands[1], MqCommand::RegisterProducer { .. }));

        match response {
            MqttPacket::ConnAck(connack) => {
                assert_eq!(connack.return_code, 0x00);
                assert!(!connack.session_present);
            }
            _ => panic!("expected ConnAck"),
        }
    }

    #[test]
    fn test_session_publish_qos0() {
        let mut session = make_session();
        let connect = make_connect("pub-client", true);
        session.handle_connect(&connect);

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "sensor/1/temp".to_string(),
            packet_id: None,
            payload: Bytes::from_static(b"22.5"),
            properties: Properties::default(),
        };

        let (commands, responses) = session.handle_publish(&publish);

        // QoS 0: should create topic + exchange, no response packets.
        assert!(!commands.is_empty());
        assert!(responses.is_empty());
    }

    #[test]
    fn test_session_publish_qos1() {
        let mut session = make_session();
        let connect = make_connect("pub-client", true);
        session.handle_connect(&connect);

        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "sensor/1/temp".to_string(),
            packet_id: Some(1),
            payload: Bytes::from_static(b"22.5"),
            properties: Properties::default(),
        };

        let (commands, responses) = session.handle_publish(&publish);

        assert!(!commands.is_empty());
        // Should get a PUBACK response.
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            MqttPacket::PubAck(puback) => {
                assert_eq!(puback.packet_id, 1);
            }
            _ => panic!("expected PubAck"),
        }
    }

    #[test]
    fn test_session_publish_qos2() {
        let mut session = make_session();
        let connect = make_connect("pub-client", true);
        session.handle_connect(&connect);

        let publish = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "important/data".to_string(),
            packet_id: Some(10),
            payload: Bytes::from_static(b"critical"),
            properties: Properties::default(),
        };

        let (_, responses) = session.handle_publish(&publish);

        // Should get PUBREC.
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            MqttPacket::PubRec(pubrec) => {
                assert_eq!(pubrec.packet_id, 10);
            }
            _ => panic!("expected PubRec"),
        }
        assert_eq!(session.qos2_inflight_count(), 1);

        // Client sends PUBREL.
        let (_, pubcomp) = session.handle_pubrel(10);
        match pubcomp {
            MqttPacket::PubComp(pc) => {
                assert_eq!(pc.packet_id, 10);
            }
            _ => panic!("expected PubComp"),
        }
    }

    #[test]
    fn test_session_subscribe() {
        let mut session = make_session();
        let connect = make_connect("sub-client", true);
        session.handle_connect(&connect);

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

        let (commands, suback) = session.handle_subscribe(1, &filters);

        assert!(!commands.is_empty());
        assert_eq!(session.subscription_count(), 2);

        match suback {
            MqttPacket::SubAck(sa) => {
                assert_eq!(sa.packet_id, 1);
                assert_eq!(sa.return_codes, vec![1, 0]); // QoS 1, QoS 0
            }
            _ => panic!("expected SubAck"),
        }
    }

    #[test]
    fn test_session_unsubscribe() {
        let mut session = make_session();
        let connect = make_connect("sub-client", true);
        session.handle_connect(&connect);

        // Subscribe first.
        let filters = vec![TopicFilter {
            filter: "sensor/+/data".to_string(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters);
        assert_eq!(session.subscription_count(), 1);

        // Unsubscribe.
        let (_, unsuback) = session.handle_unsubscribe(2, &["sensor/+/data".to_string()]);
        assert_eq!(session.subscription_count(), 0);

        match unsuback {
            MqttPacket::UnsubAck(ua) => {
                assert_eq!(ua.packet_id, 2);
                assert_eq!(ua.reason_codes, vec![0x00]);
            }
            _ => panic!("expected UnsubAck"),
        }
    }

    #[test]
    fn test_session_pingreq() {
        let mut session = make_session();
        let connect = make_connect("ping-client", true);
        session.handle_connect(&connect);

        let (cmd, response) = session.handle_pingreq();
        assert!(matches!(cmd, MqCommand::Heartbeat { .. }));
        assert!(matches!(response, MqttPacket::PingResp));
    }

    #[test]
    fn test_session_disconnect_clean() {
        let mut session = make_session();
        let connect = make_connect("dc-client", true);
        session.handle_connect(&connect);

        // Add a subscription.
        let filters = vec![TopicFilter {
            filter: "test/#".to_string(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        session.handle_subscribe(1, &filters);

        let commands = session.handle_disconnect();
        assert!(!session.connected);
        assert!(session.will.is_none());
        // Should have DisconnectConsumer + DisconnectProducer.
        assert!(commands.len() >= 2);
        // Clean session: subscriptions cleared.
        assert_eq!(session.subscription_count(), 0);
    }

    #[test]
    fn test_session_unclean_disconnect_with_will() {
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

        let commands = session.handle_unclean_disconnect();
        assert!(!session.connected);
        assert!(session.will.is_none());

        // Should have will-related commands + disconnect commands.
        assert!(commands.len() >= 3);
    }

    #[test]
    fn test_session_outbound_publish() {
        let mut session = make_session();
        let connect = make_connect("out-client", true);
        session.handle_connect(&connect);

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
                assert_eq!(p.topic, "sensor/1/temp");
                assert_eq!(p.qos, QoS::AtLeastOnce);
                assert!(p.packet_id.is_some());
            }
            _ => panic!("expected Publish"),
        }

        assert_eq!(session.qos1_inflight_count(), 1);
    }

    #[test]
    fn test_process_packet_dispatch() {
        let mut session = make_session();

        // Process CONNECT
        let connect = MqttPacket::Connect(make_connect("dispatch-client", true));
        let (cmds, responses) = session.process_packet(&connect);
        assert!(!cmds.is_empty());
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MqttPacket::ConnAck(_)));

        // Process PINGREQ
        let (cmds, responses) = session.process_packet(&MqttPacket::PingReq);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], MqCommand::Heartbeat { .. }));
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MqttPacket::PingResp));
    }

    #[test]
    fn test_packet_id_allocation() {
        let mut session = make_session();
        let id1 = session.alloc_packet_id();
        let id2 = session.alloc_packet_id();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_inflight_full_check() {
        let mut session = MqttSession::new(MqttSessionConfig {
            max_inflight: 2,
            ..MqttSessionConfig::default()
        });
        let connect = make_connect("full-client", true);
        session.handle_connect(&connect);

        assert!(!session.is_inflight_full());

        // Add 2 QoS 1 in-flight entries.
        session.qos1_inflight.insert(
            1,
            QoS1InFlight {
                mq_message_id: None,
                mq_queue_id: None,
            },
        );
        session.qos1_inflight.insert(
            2,
            QoS1InFlight {
                mq_message_id: None,
                mq_queue_id: None,
            },
        );

        assert!(session.is_inflight_full());
    }
}
