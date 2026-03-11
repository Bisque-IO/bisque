//! MQTT TCP server — accepts connections and spawns session handlers.
//!
//! The `MqttServer` binds to a TCP port, accepts incoming connections, and
//! spawns a Tokio task per connection that runs the MQTT codec + session loop.
//!
//! Each connection runs a dual-direction loop via `tokio::select!`:
//! - **Inbound**: reads packets from the TCP socket, translates to MqCommands.
//! - **Outbound**: polls bisque-mq subscription queues and delivers messages.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bisque_mq::flat::FlatMessage;
use bisque_mq::types::{ExchangeType, MqError, RetentionPolicy};
use bisque_mq::{MqCommand, MqReader, MqResponse, MqWriteBatcher};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::codec::{self, CodecError};
use crate::session::{MqttSession, MqttSessionConfig, PublishPlan, RetainedPlan, SubscribePlan};
use crate::session_store::{self, SessionStore};
use crate::types::{MqttPacket, ProtocolVersion, QoS};

// =============================================================================
// Pre-initialized Metrics (OnceLock pattern — zero cost after first init)
// =============================================================================

struct MqttMetrics {
    packets_received: metrics::Counter,
    packets_sent: metrics::Counter,
    connections_total: metrics::Counter,
    connections_active: metrics::Gauge,
    publishes_in: metrics::Counter,
    publishes_out: metrics::Counter,
    subscribes: metrics::Counter,
    bytes_in: metrics::Counter,
    bytes_out: metrics::Counter,
}

impl MqttMetrics {
    fn new(catalog_name: &str) -> Self {
        let labels = [("catalog", catalog_name.to_owned())];
        Self {
            packets_received: metrics::counter!("mqtt.packets.received", &labels),
            packets_sent: metrics::counter!("mqtt.packets.sent", &labels),
            connections_total: metrics::counter!("mqtt.connections.total", &labels),
            connections_active: metrics::gauge!("mqtt.connections.active", &labels),
            publishes_in: metrics::counter!("mqtt.publishes.in", &labels),
            publishes_out: metrics::counter!("mqtt.publishes.out", &labels),
            subscribes: metrics::counter!("mqtt.subscribes", &labels),
            bytes_in: metrics::counter!("mqtt.bytes.in", &labels),
            bytes_out: metrics::counter!("mqtt.bytes.out", &labels),
        }
    }
}

static MQTT_METRICS: std::sync::OnceLock<MqttMetrics> = std::sync::OnceLock::new();

fn shared_metrics() -> &'static MqttMetrics {
    MQTT_METRICS.get_or_init(|| MqttMetrics::new("default"))
}

// =============================================================================
// Server Configuration
// =============================================================================

/// Configuration for the MQTT server.
#[derive(Clone)]
pub struct MqttServerConfig {
    /// Address to bind the TCP listener.
    pub bind_addr: SocketAddr,
    /// Session configuration applied to all new connections.
    pub session_config: MqttSessionConfig,
    /// TCP read buffer size in bytes.
    pub read_buffer_size: usize,
    /// Maximum time to wait for CONNECT after TCP accept.
    pub connect_timeout: Duration,
    /// TCP keepalive interval.
    pub tcp_keepalive: Option<Duration>,
    /// How often to poll subscription queues for outbound delivery (ms).
    pub delivery_poll_ms: u64,
    /// Maximum messages to deliver per poll cycle per subscription.
    pub delivery_batch_size: u32,
    /// Optional session store for persistence across reconnects.
    pub session_store: Option<Arc<dyn SessionStore>>,
}

impl Default for MqttServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:1883".parse().unwrap(),
            session_config: MqttSessionConfig::default(),
            read_buffer_size: 8192,
            connect_timeout: Duration::from_secs(10),
            tcp_keepalive: Some(Duration::from_secs(60)),
            delivery_poll_ms: 50,
            delivery_batch_size: 10,
            session_store: None,
        }
    }
}

// =============================================================================
// Server Statistics
// =============================================================================

/// Atomic server-level statistics.
pub struct MqttServerStats {
    active_connections: std::sync::atomic::AtomicU64,
    total_connections: std::sync::atomic::AtomicU64,
    total_packets_received: std::sync::atomic::AtomicU64,
    total_packets_sent: std::sync::atomic::AtomicU64,
}

impl MqttServerStats {
    fn new() -> Self {
        Self {
            active_connections: std::sync::atomic::AtomicU64::new(0),
            total_connections: std::sync::atomic::AtomicU64::new(0),
            total_packets_received: std::sync::atomic::AtomicU64::new(0),
            total_packets_sent: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn active_connections(&self) -> u64 {
        self.active_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn total_connections(&self) -> u64 {
        self.total_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn total_packets_received(&self) -> u64 {
        self.total_packets_received
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn total_packets_sent(&self) -> u64 {
        self.total_packets_sent
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// =============================================================================
// MqttServer
// =============================================================================

/// MQTT TCP server that accepts connections and translates MQTT to bisque-mq.
pub struct MqttServer {
    config: MqttServerConfig,
    batcher: Arc<MqWriteBatcher>,
    log_reader: Arc<MqReader>,
    stats: Arc<MqttServerStats>,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl MqttServer {
    /// Create a new MQTT server.
    pub fn new(
        config: MqttServerConfig,
        batcher: Arc<MqWriteBatcher>,
        log_reader: Arc<MqReader>,
        catalog_name: &str,
    ) -> Self {
        // Initialize metrics with catalog label (first call wins via OnceLock).
        MQTT_METRICS.get_or_init(|| MqttMetrics::new(catalog_name));
        Self {
            config,
            batcher,
            log_reader,
            stats: Arc::new(MqttServerStats::new()),
            shutdown_tx: None,
        }
    }

    /// Get a reference to the server statistics.
    pub fn stats(&self) -> Arc<MqttServerStats> {
        Arc::clone(&self.stats)
    }

    /// Start the MQTT server. Returns a JoinHandle for the accept loop.
    pub async fn start(
        &mut self,
    ) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!(addr = %self.config.bind_addr, "MQTT server listening");

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let config = self.config.clone();
        let batcher = Arc::clone(&self.batcher);
        let log_reader = Arc::clone(&self.log_reader);
        let stats = Arc::clone(&self.stats);

        let handle = tokio::spawn(accept_loop(
            listener,
            config,
            batcher,
            log_reader,
            stats,
            shutdown_rx,
        ));

        Ok(handle)
    }

    /// Signal the server to shut down gracefully.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }
}

// =============================================================================
// Accept Loop
// =============================================================================

async fn accept_loop(
    listener: TcpListener,
    config: MqttServerConfig,
    batcher: Arc<MqWriteBatcher>,
    log_reader: Arc<MqReader>,
    stats: Arc<MqttServerStats>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    // Periodic session expiry sweep (every 60 seconds).
    let mut expiry_interval = tokio::time::interval(Duration::from_secs(60));
    expiry_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        stats.total_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        stats.active_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        debug!(peer = %peer_addr, "new MQTT connection");

                        let m = shared_metrics();
                        m.connections_total.increment(1);
                        m.connections_active.increment(1.0);

                        let config = config.clone();
                        let batcher = Arc::clone(&batcher);
                        let log_reader = Arc::clone(&log_reader);
                        let stats = Arc::clone(&stats);

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(
                                stream,
                                peer_addr,
                                &config,
                                &batcher,
                                &log_reader,
                                &stats,
                            ).await {
                                debug!(peer = %peer_addr, error = %e, "connection closed with error");
                            }
                            stats.active_connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            shared_metrics().connections_active.decrement(1.0);
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "failed to accept connection");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            _ = expiry_interval.tick() => {
                if let Some(ref store) = config.session_store {
                    let expired = store.expire();
                    if expired > 0 {
                        debug!(expired, "expired stale MQTT sessions");
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("MQTT server shutting down");
                    break;
                }
            }
        }
    }
}

// =============================================================================
// Connection Error
// =============================================================================

#[derive(Debug, thiserror::Error)]
enum ConnectionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("connect timeout")]
    ConnectTimeout,
    #[error("first packet was not CONNECT")]
    NotConnect,
    #[error("batcher error: {0}")]
    Batcher(#[from] bisque_mq::MqBatcherError),
    #[error("connection closed by client")]
    Closed,
}

// =============================================================================
// Entity ID Resolution Helpers
// =============================================================================

/// Resolve an entity ID from a Create command response, handling AlreadyExists.
/// Returns the entity ID on success.
fn extract_entity_id(resp: &MqResponse) -> Option<u64> {
    match resp {
        MqResponse::EntityCreated { id } => Some(*id),
        MqResponse::Error(MqError::AlreadyExists { id, .. }) => Some(*id),
        _ => None,
    }
}

/// Ensure an exchange exists and return its ID, using the session cache.
async fn ensure_exchange(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    exchange_name: &str,
    cached_id: Option<u64>,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_exchange_id(exchange_name) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_exchange(
            exchange_name,
            ExchangeType::Topic,
        ))
        .await?;

    if let Some(id) = extract_entity_id(&resp) {
        session.cache_exchange_id(exchange_name, id);
        Ok(id)
    } else {
        warn!(?resp, "failed to create/resolve exchange");
        Err(ConnectionError::Batcher(
            bisque_mq::MqBatcherError::ChannelClosed,
        ))
    }
}

/// Ensure a queue exists and return its ID, using the session cache.
async fn ensure_queue(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    queue_name: &str,
    cached_id: Option<u64>,
    config: bisque_mq::config::QueueConfig,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_queue_id(queue_name) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_queue(queue_name, &config))
        .await?;

    if let Some(id) = extract_entity_id(&resp) {
        session.cache_queue_id(queue_name, id);
        Ok(id)
    } else {
        warn!(?resp, "failed to create/resolve queue");
        Err(ConnectionError::Batcher(
            bisque_mq::MqBatcherError::ChannelClosed,
        ))
    }
}

/// Ensure a topic exists and return its ID, using the session cache.
async fn ensure_topic(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    topic_name: &str,
    cached_id: Option<u64>,
    retention: RetentionPolicy,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_topic_id(topic_name) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_topic(topic_name, retention, 0))
        .await?;

    if let Some(id) = extract_entity_id(&resp) {
        session.cache_topic_id(topic_name, id);
        Ok(id)
    } else {
        warn!(?resp, "failed to create/resolve topic");
        Err(ConnectionError::Batcher(
            bisque_mq::MqBatcherError::ChannelClosed,
        ))
    }
}

/// Ensure a binding exists and return its ID, using the session cache.
async fn ensure_binding(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    exchange_id: u64,
    queue_id: u64,
    routing_key: &str,
    cached_id: Option<u64>,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_binding_id(exchange_id, routing_key) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_binding(
            exchange_id,
            queue_id,
            Some(routing_key),
        ))
        .await?;

    if let Some(id) = extract_entity_id(&resp) {
        session.cache_binding_id(exchange_id, routing_key, id);
        Ok(id)
    } else {
        warn!(?resp, "failed to create/resolve binding");
        Err(ConnectionError::Batcher(
            bisque_mq::MqBatcherError::ChannelClosed,
        ))
    }
}

// =============================================================================
// Publish Orchestration
// =============================================================================

/// Execute a PublishPlan: ensure exchange, publish message, handle retained.
async fn orchestrate_publish(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    plan: PublishPlan,
) -> Result<(), ConnectionError> {
    if plan.flat_message.is_empty() {
        return Ok(()); // Invalid publish (e.g., unknown topic alias)
    }

    // 1. Ensure the MQTT exchange exists.
    let exchange_id = ensure_exchange(
        session,
        batcher,
        plan.exchange_name,
        plan.cached_exchange_id,
    )
    .await?;

    // 2. Publish through the exchange (routes to all matching subscription queues).
    let resp = batcher
        .submit(MqCommand::publish_to_exchange(
            exchange_id,
            &[plan.flat_message],
        ))
        .await?;

    match &resp {
        MqResponse::Ok | MqResponse::Published { .. } => {}
        MqResponse::Error(e) => {
            warn!(%e, "exchange publish failed");
        }
        _ => {}
    }

    // 3. Handle retained message.
    if let Some(retained) = plan.retained {
        orchestrate_retained(session, batcher, retained).await?;
    }

    Ok(())
}

/// Execute a RetainedPlan: create/publish to the retained topic.
async fn orchestrate_retained(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    plan: RetainedPlan,
) -> Result<(), ConnectionError> {
    let topic_id = ensure_topic(
        session,
        batcher,
        &plan.topic_name,
        plan.cached_topic_id,
        RetentionPolicy {
            max_messages: Some(1), // Keep only latest
            ..RetentionPolicy::default()
        },
    )
    .await?;

    match plan.flat_message {
        Some(msg) => {
            // Store the retained message.
            let _ = batcher.submit(MqCommand::publish(topic_id, &[msg])).await?;
        }
        None => {
            // Clear retained: purge the topic.
            let _ = batcher
                .submit(MqCommand::purge_topic(topic_id, u64::MAX))
                .await?;
        }
    }

    Ok(())
}

// =============================================================================
// Subscribe Orchestration
// =============================================================================

/// Execute a SubscribePlan: ensure exchange, queues, and bindings.
async fn orchestrate_subscribe(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    plan: &SubscribePlan,
) -> Result<(), ConnectionError> {
    // 1. Ensure the global MQTT exchange.
    let exchange_id = ensure_exchange(
        session,
        batcher,
        plan.exchange_name,
        plan.cached_exchange_id,
    )
    .await?;

    // 2. For each filter, create queue and binding.
    for filter_plan in &plan.filters {
        // Ensure queue.
        let queue_id = ensure_queue(
            session,
            batcher,
            &filter_plan.queue_name,
            filter_plan.cached_queue_id,
            filter_plan.queue_config.clone(),
        )
        .await?;

        // Ensure binding.
        let binding_id = ensure_binding(
            session,
            batcher,
            exchange_id,
            queue_id,
            &filter_plan.routing_key,
            filter_plan.cached_binding_id,
        )
        .await?;

        // Update the subscription mapping with resolved IDs.
        session.update_subscription_ids(
            &filter_plan.filter,
            Some(exchange_id),
            Some(binding_id),
            Some(queue_id),
            None,
        );
    }

    Ok(())
}

// =============================================================================
// Outbound Delivery
// =============================================================================

/// Poll subscription queues and deliver messages to the MQTT client.
///
/// The `sub_buf` and `flat_messages_buf` are caller-owned reusable buffers
/// to avoid per-cycle allocations.
/// Per-subscription delivery metadata, collected into a reusable buffer.
#[derive(Clone, Copy)]
struct SubDeliveryInfo {
    queue_id: u64,
    max_qos: QoS,
    no_local: bool,
    retain_as_published: bool,
}

/// Extract the publisher session ID from a FlatMessage header.
fn extract_publisher_session_id(flat: &FlatMessage) -> Option<u64> {
    for i in 0..flat.header_count() {
        let (k, v) = flat.header(i);
        if &k[..] == b"mqtt.publisher_session_id" && v.len() == 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&v);
            return Some(u64::from_be_bytes(buf));
        }
    }
    None
}

/// Check if the original retain flag was set in a FlatMessage header.
fn extract_original_retain(flat: &FlatMessage) -> bool {
    for i in 0..flat.header_count() {
        let (k, v) = flat.header(i);
        if &k[..] == b"mqtt.original_retain" && !v.is_empty() {
            return v[0] != 0;
        }
    }
    false
}

async fn deliver_outbound(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    log_reader: &MqReader,
    stream: &mut TcpStream,
    stats: &MqttServerStats,
    batch_size: u32,
    write_buf: &mut BytesMut,
    sub_buf: &mut Vec<SubDeliveryInfo>,
    flat_messages_buf: &mut Vec<Bytes>,
) -> Result<(), ConnectionError> {
    if session.is_inflight_full() {
        return Ok(());
    }

    let m = shared_metrics();

    // Collect subscription info into reusable buffer to avoid borrow conflict.
    sub_buf.clear();
    sub_buf.extend(
        session
            .subscriptions_iter()
            .filter_map(|(_filter, mapping)| {
                mapping.queue_id.map(|qid| SubDeliveryInfo {
                    queue_id: qid,
                    max_qos: mapping.max_qos,
                    no_local: mapping.no_local,
                    retain_as_published: mapping.retain_as_published,
                })
            }),
    );

    let is_v5 = session.protocol_version == ProtocolVersion::V5;
    let my_session_id = session.session_id;

    for &SubDeliveryInfo {
        queue_id,
        max_qos,
        no_local,
        retain_as_published,
    } in sub_buf.iter()
    {
        if session.is_inflight_full() {
            break;
        }

        let remaining = session.remaining_inflight().min(batch_size as usize) as u32;
        if remaining == 0 {
            break;
        }

        // Issue Deliver command to pull messages from the subscription queue.
        let resp = batcher
            .submit(MqCommand::deliver(queue_id, session.session_id, remaining))
            .await?;

        if let MqResponse::Messages { messages } = resp {
            for delivered in &messages {
                // Read flat message bytes from the raft log via log reader.
                flat_messages_buf.clear();
                log_reader.read_messages_at_into(delivered.message_id, flat_messages_buf);

                if flat_messages_buf.is_empty() {
                    // Message has been purged — NACK it so the queue can move on.
                    let _ = batcher
                        .submit(MqCommand::nack(queue_id, &[delivered.message_id]))
                        .await;
                    continue;
                }

                for flat_bytes in flat_messages_buf.iter() {
                    if let Some(flat) = FlatMessage::new(flat_bytes.clone()) {
                        // No Local enforcement (M6): skip messages published by this session.
                        if no_local {
                            if let Some(pub_session_id) = extract_publisher_session_id(&flat) {
                                if pub_session_id == my_session_id {
                                    // ACK the message so it's removed from the queue.
                                    let _ = batcher
                                        .submit(MqCommand::ack(
                                            queue_id,
                                            &[delivered.message_id],
                                            None,
                                        ))
                                        .await;
                                    continue;
                                }
                            }
                        }

                        // Retain As Published enforcement (M7): determine retain flag.
                        let retain_flag = if retain_as_published {
                            extract_original_retain(&flat)
                        } else {
                            false
                        };

                        // Zero-alloc outbound: track inflight + encode directly from FlatMessage.
                        let tracking = session.track_outbound_delivery(
                            max_qos,
                            queue_id,
                            delivered.message_id,
                        );
                        let packet_id = match tracking {
                            None => continue, // inflight full
                            Some(pid) => pid,
                        };

                        let topic_alias = if is_v5 {
                            let topic = flat.routing_key().unwrap_or_default();
                            session.resolve_outbound_topic_alias(&topic)
                        } else {
                            None
                        };
                        let subscription_id = if is_v5 {
                            session.find_subscription_id_for_queue(queue_id)
                        } else {
                            None
                        };

                        // Maximum Packet Size enforcement — outbound (M10).
                        let buf_before = write_buf.len();
                        codec::encode_publish_from_flat(
                            &flat,
                            max_qos,
                            retain_flag,
                            false,
                            packet_id,
                            is_v5,
                            subscription_id,
                            topic_alias,
                            write_buf,
                        );
                        let client_max = session.client_maximum_packet_size;
                        let encoded_size = write_buf.len() - buf_before;
                        if client_max > 0 && encoded_size > client_max as usize {
                            // Roll back the encoded packet — skip delivery.
                            write_buf.truncate(buf_before);
                            debug!(
                                encoded_size,
                                client_max,
                                "outbound PUBLISH exceeds client maximum_packet_size, skipping"
                            );
                            // ACK the message so the queue doesn't retry.
                            let _ = batcher
                                .submit(MqCommand::ack(queue_id, &[delivered.message_id], None))
                                .await;
                            continue;
                        }
                        stats
                            .total_packets_sent
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        m.publishes_out.increment(1);
                    }
                }
            }
        }
    }

    // Batch write: flush all encoded packets in a single write_all.
    if !write_buf.is_empty() {
        let len = write_buf.len();
        stream.write_all(write_buf).await?;
        m.bytes_out.increment(len as u64);
        m.packets_sent.increment(1);
        write_buf.clear();
    }

    Ok(())
}

// =============================================================================
// Connection Handler
// =============================================================================

async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    config: &MqttServerConfig,
    batcher: &Arc<MqWriteBatcher>,
    log_reader: &Arc<MqReader>,
    stats: &Arc<MqttServerStats>,
) -> Result<(), ConnectionError> {
    let mut read_buf = BytesMut::with_capacity(config.read_buffer_size);
    // Reusable write buffer — cleared between uses, never re-allocated.
    let mut write_buf = BytesMut::with_capacity(config.read_buffer_size);
    let mut session = MqttSession::new(config.session_config.clone());

    // Wait for CONNECT packet with timeout.
    let first_packet = tokio::time::timeout(config.connect_timeout, async {
        loop {
            let n = stream.read_buf(&mut read_buf).await?;
            if n == 0 {
                return Err(ConnectionError::Closed);
            }

            match codec::decode_packet(&read_buf) {
                Ok((packet, consumed)) => {
                    let _ = read_buf.split_to(consumed);
                    return Ok(packet);
                }
                Err(CodecError::Incomplete) => continue,
                Err(e) => return Err(ConnectionError::Codec(e)),
            }
        }
    })
    .await
    .map_err(|_| ConnectionError::ConnectTimeout)??;

    if !matches!(first_packet, MqttPacket::Connect(_)) {
        return Err(ConnectionError::NotConnect);
    }

    // Process CONNECT via session.
    let (commands, responses) = session.process_packet(&first_packet);

    // Check session store for persisted session (MQTT 5.0 session resumption).
    let mut session_resumed = false;
    let mut restore_plans = smallvec::SmallVec::<[SubscribePlan; 4]>::new();
    if !session.clean_session {
        if let Some(ref store) = config.session_store {
            if let Some(persisted) = store.load(&session.client_id) {
                session_resumed = true;
                debug!(
                    client_id = %session.client_id,
                    subscriptions = persisted.subscriptions.len(),
                    "restoring persisted session"
                );
                // Re-create subscriptions from persisted state.
                restore_plans = session.restore_subscriptions(&persisted.subscriptions);
            }
        }
    } else if let Some(ref store) = config.session_store {
        // Clean session: remove any persisted state.
        store.remove(&session.client_id);
    }

    // Submit registration commands.
    for cmd in commands {
        let _ = batcher.submit(cmd).await;
    }

    // Re-create queues and bindings for restored subscriptions.
    for plan in &restore_plans {
        if let Err(e) = orchestrate_subscribe(&mut session, batcher, plan).await {
            warn!(error = %e, "failed to restore subscription");
        }
    }

    // Send CONNACK using reusable write buffer (version-aware encoding).
    // Patch session_present if we actually found a persisted session.
    for response in responses {
        let response = if session_resumed {
            if let MqttPacket::ConnAck(mut connack) = response {
                connack.session_present = true;
                MqttPacket::ConnAck(connack)
            } else {
                response
            }
        } else {
            response
        };
        write_buf.clear();
        codec::encode_packet_versioned(&response, session.protocol_version, &mut write_buf);
        stream.write_all(&write_buf).await?;
        stats
            .total_packets_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    stats
        .total_packets_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    info!(
        peer = %peer_addr,
        client_id = %session.client_id,
        "MQTT client connected"
    );

    // Main bidirectional loop.
    let result = connection_loop(
        &mut stream,
        &mut session,
        batcher,
        log_reader,
        stats,
        &mut read_buf,
        &mut write_buf,
        config.delivery_poll_ms,
        config.delivery_batch_size,
    )
    .await;

    // Handle disconnect.
    match &result {
        Ok(()) => {
            // Clean disconnect — handle_disconnect was already called.
        }
        Err(_) => {
            // Unclean disconnect.
            let (will_plan, commands) = session.handle_unclean_disconnect();

            // Publish will message if present.
            if let Some(plan) = will_plan {
                if let Err(e) = orchestrate_publish(&mut session, batcher, plan).await {
                    warn!(error = %e, "failed to publish will message");
                }
            }

            // Submit cleanup commands.
            for cmd in commands {
                let _ = batcher.submit(cmd).await;
            }
        }
    }

    // Persist session state if applicable (MQTT 5.0 session expiry).
    if let Some(ref store) = config.session_store {
        if session_store::should_persist(&session) {
            store.save(session_store::extract_session_state(&session));
            debug!(client_id = %session.client_id, "session state persisted");
        } else if session.clean_session {
            store.remove(&session.client_id);
        }
    }

    info!(
        peer = %peer_addr,
        client_id = %session.client_id,
        clean = result.is_ok(),
        "MQTT client disconnected"
    );

    result
}

// =============================================================================
// Connection Loop (bidirectional)
// =============================================================================

async fn connection_loop(
    stream: &mut TcpStream,
    session: &mut MqttSession,
    batcher: &Arc<MqWriteBatcher>,
    log_reader: &Arc<MqReader>,
    stats: &Arc<MqttServerStats>,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    delivery_poll_ms: u64,
    delivery_batch_size: u32,
) -> Result<(), ConnectionError> {
    let keep_alive_timeout = if session.keep_alive > 0 {
        Duration::from_secs(session.keep_alive as u64 * 3 / 2)
    } else {
        Duration::from_secs(3600)
    };

    let mut delivery_interval = tokio::time::interval(Duration::from_millis(delivery_poll_ms));
    delivery_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Reusable buffers for deliver_outbound — hoisted to avoid per-cycle allocation.
    let mut sub_buf: Vec<SubDeliveryInfo> = Vec::new();
    let mut flat_messages_buf: Vec<Bytes> = Vec::new();

    let m = shared_metrics();

    loop {
        tokio::select! {
            // Inbound: read from TCP with keep-alive timeout.
            read_result = tokio::time::timeout(keep_alive_timeout, stream.read_buf(read_buf)) => {
                match read_result {
                    Ok(Ok(0)) => return Err(ConnectionError::Closed),
                    Ok(Ok(n)) => {
                        m.bytes_in.increment(n as u64);
                        // Process all complete packets in the buffer.
                        // Use zero-copy decode: freeze the packet bytes so PUBLISH
                        // topic/payload are Bytes::slice() from the frozen buffer.
                        loop {
                            match codec::parse_fixed_header(read_buf) {
                                Ok((_, _, remaining_length, header_size)) => {
                                    let total_size = header_size + remaining_length;
                                    if read_buf.len() < total_size {
                                        break; // incomplete
                                    }

                                    // Maximum Packet Size enforcement — inbound (M10).
                                    let max_size = session.config.max_packet_size;
                                    if total_size > max_size {
                                        warn!(
                                            client_id = %session.client_id,
                                            packet_size = total_size,
                                            max = max_size,
                                            "inbound packet exceeds maximum packet size"
                                        );
                                        return Err(send_disconnect_and_close(
                                            stream, stats, write_buf,
                                            session.protocol_version,
                                            crate::types::ReasonCode::PACKET_TOO_LARGE.0,
                                            Some("packet too large"),
                                        ).await);
                                    }

                                    // Freeze exactly the packet bytes for zero-copy slicing.
                                    let packet_bytes: Bytes = read_buf.split_to(total_size).freeze();
                                    match codec::decode_packet_from_bytes_versioned(&packet_bytes, session.protocol_version) {
                                        Ok((packet, _)) => {
                                            stats.total_packets_received
                                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            m.packets_received.increment(1);

                                            let is_disconnect = matches!(packet, MqttPacket::Disconnect(_));

                                            process_inbound_packet(
                                                &packet, session, batcher, log_reader, stream, stats, write_buf,
                                            ).await?;

                                            if is_disconnect {
                                                return Ok(());
                                            }
                                        }
                                        Err(e) => {
                                            // Send server-initiated DISCONNECT for protocol errors (M12).
                                            return Err(send_disconnect_and_close(
                                                stream, stats, write_buf,
                                                session.protocol_version,
                                                crate::types::ReasonCode::MALFORMED_PACKET.0,
                                                Some(&e.to_string()),
                                            ).await);
                                        }
                                    }
                                }
                                Err(CodecError::Incomplete) => break,
                                Err(e) => {
                                    return Err(send_disconnect_and_close(
                                        stream, stats, write_buf,
                                        session.protocol_version,
                                        crate::types::ReasonCode::MALFORMED_PACKET.0,
                                        Some(&e.to_string()),
                                    ).await);
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => return Err(ConnectionError::Io(e)),
                    Err(_) => {
                        warn!(
                            client_id = %session.client_id,
                            timeout_secs = keep_alive_timeout.as_secs(),
                            "keep-alive timeout, disconnecting"
                        );
                        // Send server-initiated DISCONNECT for V5 (M12).
                        return Err(send_disconnect_and_close(
                            stream,
                            stats,
                            write_buf,
                            session.protocol_version,
                            crate::types::ReasonCode::PROTOCOL_ERROR.0,
                            Some("keep-alive timeout"),
                        ).await);
                    }
                }
            }

            // Outbound: deliver messages from subscription queues.
            _ = delivery_interval.tick(), if !session.is_inflight_full() && session.subscription_count() > 0 => {
                deliver_outbound(session, batcher, log_reader.as_ref(), stream, stats, delivery_batch_size, write_buf, &mut sub_buf, &mut flat_messages_buf).await?;
            }
        }
    }
}

// =============================================================================
// Inbound Packet Processing
// =============================================================================

/// Process a single inbound MQTT packet with full orchestration.
async fn process_inbound_packet(
    packet: &MqttPacket,
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    log_reader: &Arc<MqReader>,
    stream: &mut TcpStream,
    stats: &MqttServerStats,
    write_buf: &mut BytesMut,
) -> Result<(), ConnectionError> {
    let m = shared_metrics();
    let version = session.protocol_version;

    match packet {
        // Second CONNECT rejection (m11): a connected session must not receive another CONNECT.
        MqttPacket::Connect(_) => {
            warn!(client_id = %session.client_id, "received second CONNECT, disconnecting");
            // Send DISCONNECT with PROTOCOL_ERROR reason code.
            let disconnect = MqttPacket::Disconnect(crate::types::Disconnect {
                reason_code: Some(crate::types::ReasonCode::PROTOCOL_ERROR.0),
                properties: crate::types::Properties {
                    reason_string: Some("second CONNECT not allowed".into()),
                    ..Default::default()
                },
            });
            send_packets(
                stream,
                std::slice::from_ref(&disconnect),
                stats,
                write_buf,
                version,
            )
            .await?;
            return Err(ConnectionError::Codec(CodecError::InvalidFixedHeaderFlags(
                1,
            )));
        }

        MqttPacket::Publish(publish) => {
            m.publishes_in.increment(1);
            // Full orchestration: session builds plan, server resolves IDs.
            let plan = session.handle_publish(publish);

            // Send immediate responses (PUBACK/PUBREC) before orchestrating.
            send_packets(stream, &plan.responses, stats, write_buf, version).await?;

            // Orchestrate the actual publish through the exchange.
            orchestrate_publish(session, batcher, plan).await?;
        }

        MqttPacket::Subscribe(subscribe) => {
            m.subscribes.increment(1);
            // Full orchestration: create exchange, queues, bindings.
            let plan = session.handle_subscribe(
                subscribe.packet_id,
                &subscribe.filters,
                subscribe.properties.subscription_identifier,
            );

            // Orchestrate entity creation.
            if let Err(e) = orchestrate_subscribe(session, batcher, &plan).await {
                warn!(error = %e, "subscribe orchestration failed");
            }

            // Send SUBACK after orchestration so IDs are resolved.
            send_packets(
                stream,
                std::slice::from_ref(&plan.suback),
                stats,
                write_buf,
                version,
            )
            .await?;

            // Deliver retained messages for the newly subscribed filters.
            if let Err(e) = deliver_retained_on_subscribe(
                session,
                log_reader.as_ref(),
                stream,
                stats,
                &plan,
                write_buf,
            )
            .await
            {
                warn!(error = %e, "retained message delivery failed");
            }
        }

        MqttPacket::Disconnect(disconnect) => {
            // Session handles state cleanup; M11: will on V5 DISCONNECT reason 0x04.
            let (will_plan, cmds) = session.handle_disconnect(Some(disconnect));
            for cmd in cmds {
                let _ = batcher.submit(cmd).await;
            }
            // Publish will message if DISCONNECT with Will Message (0x04).
            if let Some(plan) = will_plan {
                if let Err(e) = orchestrate_publish(session, batcher, plan).await {
                    warn!(error = %e, "failed to publish will on V5 disconnect");
                }
            }
        }

        // Inline dispatch for ack packets — avoids Vec allocations on hot path.
        MqttPacket::PubAck(puback) => {
            if let Some(cmd) = session.handle_puback(puback.packet_id) {
                let _ = batcher.submit(cmd).await;
            }
        }

        MqttPacket::PubRec(pubrec) => {
            let pubrel = session.handle_pubrec(pubrec.packet_id);
            send_packets(
                stream,
                std::slice::from_ref(&pubrel),
                stats,
                write_buf,
                version,
            )
            .await?;
        }

        MqttPacket::PubRel(pubrel) => {
            let pubcomp = session.handle_pubrel(pubrel.packet_id);
            send_packets(
                stream,
                std::slice::from_ref(&pubcomp),
                stats,
                write_buf,
                version,
            )
            .await?;
        }

        MqttPacket::PubComp(pubcomp) => {
            if let Some(cmd) = session.handle_pubcomp(pubcomp.packet_id) {
                let _ = batcher.submit(cmd).await;
            }
        }

        MqttPacket::PingReq => {
            let (cmd, pong) = session.handle_pingreq();
            let _ = batcher.submit(cmd).await;
            send_packets(
                stream,
                std::slice::from_ref(&pong),
                stats,
                write_buf,
                version,
            )
            .await?;
        }

        // Remaining packet types (ConnAck, SubAck, UnsubAck, PingResp, Auth)
        // are unexpected from clients or handled via process_packet fallback.
        _ => {
            let (commands, responses) = session.process_packet(packet);
            for cmd in commands {
                if let Err(e) = batcher.submit(cmd).await {
                    warn!(error = %e, "failed to submit command");
                }
            }
            send_packets(stream, &responses, stats, write_buf, version).await?;
        }
    }

    Ok(())
}

/// Send a slice of MQTT packets to the client using the reusable write buffer.
async fn send_packets(
    stream: &mut TcpStream,
    packets: &[MqttPacket],
    stats: &MqttServerStats,
    write_buf: &mut BytesMut,
    version: ProtocolVersion,
) -> Result<(), ConnectionError> {
    // Batch-encode all packets into the reusable buffer, then flush once.
    write_buf.clear();
    for packet in packets {
        codec::encode_packet_versioned(packet, version, write_buf);
    }
    if !write_buf.is_empty() {
        stream.write_all(write_buf).await?;
        stats
            .total_packets_sent
            .fetch_add(packets.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

/// Send a DISCONNECT packet with the given reason code and close (M12).
///
/// For MQTT 5.0, this sends a proper DISCONNECT before returning an error.
/// For MQTT 3.1.1, DISCONNECT from server is not part of the spec, so we
/// just return the error (the connection will be dropped).
async fn send_disconnect_and_close(
    stream: &mut TcpStream,
    stats: &MqttServerStats,
    write_buf: &mut BytesMut,
    version: ProtocolVersion,
    reason_code: u8,
    reason_string: Option<&str>,
) -> ConnectionError {
    if version == ProtocolVersion::V5 {
        let mut properties = crate::types::Properties::default();
        if let Some(rs) = reason_string {
            properties.reason_string = Some(rs.to_string());
        }
        let disconnect = MqttPacket::Disconnect(crate::types::Disconnect {
            reason_code: Some(reason_code),
            properties,
        });
        // Best-effort send; ignore errors since we're closing anyway.
        let _ = send_packets(
            stream,
            std::slice::from_ref(&disconnect),
            stats,
            write_buf,
            version,
        )
        .await;
    }
    ConnectionError::Codec(CodecError::InvalidFixedHeaderFlags(reason_code))
}

// =============================================================================
// Retained Message Delivery
// =============================================================================

/// Deliver retained messages to the client after a SUBSCRIBE completes.
///
/// For each subscription filter, we look up matching retained topics (stored
/// under the `$mqtt/retained/` prefix) and send the latest message with the
/// retain flag set, per MQTT spec.
async fn deliver_retained_on_subscribe(
    session: &mut MqttSession,
    log_reader: &MqReader,
    stream: &mut TcpStream,
    stats: &MqttServerStats,
    plan: &SubscribePlan,
    write_buf: &mut BytesMut,
) -> Result<(), ConnectionError> {
    // Clone once per subscribe (not per message) to avoid borrow conflict with `session`.
    let retained_prefix = session.config.retained_prefix.clone();

    for filter_plan in &plan.filters {
        // Retain Handling enforcement (M8):
        // 0 = send retained at subscribe time (default)
        // 1 = send only if this is a new subscription
        // 2 = do not send retained messages at subscribe time
        match filter_plan.retain_handling {
            2 => continue,                                     // Never send retained
            1 if !filter_plan.is_new_subscription => continue, // Only new subs
            _ => {}                                            // 0 or 1-with-new: proceed
        }

        let filter = &filter_plan.filter;

        if !filter.contains('+') && !filter.contains('#') {
            // Exact match: look up the specific retained topic.
            let retained_topic_name = format!("{}{}", retained_prefix, filter);
            let topics = log_reader.list_topics_with_prefix(&retained_topic_name);
            for (name, topic_id) in topics {
                if name.as_ref() == retained_topic_name.as_bytes() {
                    deliver_single_retained(
                        session,
                        log_reader,
                        stream,
                        stats,
                        topic_id,
                        filter_plan.qos,
                        write_buf,
                    )
                    .await?;
                }
            }
        } else {
            // Wildcard: scan all retained topics and match against the filter.
            let topics = log_reader.list_topics_with_prefix(&retained_prefix);
            for (name, topic_id) in topics {
                let name_str = std::str::from_utf8(&name).unwrap_or("");
                if let Some(mqtt_topic) = name_str.strip_prefix(&*retained_prefix) {
                    if mqtt_topic_matches_filter(mqtt_topic, filter) {
                        deliver_single_retained(
                            session,
                            log_reader,
                            stream,
                            stats,
                            topic_id,
                            filter_plan.qos,
                            write_buf,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Deliver a single retained message from the given topic to the client.
///
/// Uses `encode_publish_from_flat` for zero-allocation encoding.
async fn deliver_single_retained(
    session: &mut MqttSession,
    log_reader: &MqReader,
    stream: &mut TcpStream,
    stats: &MqttServerStats,
    topic_id: u64,
    max_qos: QoS,
    write_buf: &mut BytesMut,
) -> Result<(), ConnectionError> {
    if let Some(msg_bytes) = log_reader.read_latest_topic_message(topic_id) {
        if let Some(flat) = FlatMessage::new(msg_bytes) {
            let is_v5 = session.protocol_version == ProtocolVersion::V5;

            // Track inflight for retained delivery (queue_id=0, message_id=0).
            let tracking = session.track_outbound_delivery(max_qos, 0, 0);
            let packet_id = match tracking {
                None => return Ok(()), // inflight full
                Some(pid) => pid,
            };

            let topic_alias = if is_v5 {
                let topic = flat.routing_key().unwrap_or_default();
                session.resolve_outbound_topic_alias(&topic)
            } else {
                None
            };
            let subscription_id = if is_v5 {
                session.find_subscription_id()
            } else {
                None
            };

            write_buf.clear();
            codec::encode_publish_from_flat(
                &flat,
                max_qos,
                true, // retain flag
                false,
                packet_id,
                is_v5,
                subscription_id,
                topic_alias,
                write_buf,
            );
            stream.write_all(write_buf).await?;
            stats
                .total_packets_sent
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    Ok(())
}

/// Check if an MQTT topic matches a topic filter (with `+` and `#` wildcards).
///
/// Zero-allocation: uses split iterators instead of collecting into Vecs.
///
/// - `+` matches a single topic level
/// - `#` matches zero or more remaining levels (must be the last segment)
fn mqtt_topic_matches_filter(topic: &str, filter: &str) -> bool {
    // MQTT 3.1.1 SS 4.7.2 / MQTT 5.0 SS 4.7.2: Topics starting with '$' MUST NOT
    // be matched by subscriptions starting with '#' or '+'. A client must subscribe
    // to a filter that explicitly starts with '$' to receive such topics.
    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }

    let mut topic_iter = topic.split('/');
    let mut filter_iter = filter.split('/');

    loop {
        match (filter_iter.next(), topic_iter.next()) {
            (Some("#"), _) => return true, // # matches everything remaining
            (Some(f), Some(t)) => {
                if f != "+" && f != t {
                    return false;
                }
            }
            (None, None) => return true, // both exhausted, exact match
            (Some(_), None) | (None, Some(_)) => return false, // length mismatch
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
    fn test_server_config_defaults() {
        let config = MqttServerConfig::default();
        assert_eq!(config.bind_addr.port(), 1883);
        assert_eq!(config.read_buffer_size, 8192);
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.delivery_poll_ms, 50);
        assert_eq!(config.delivery_batch_size, 10);
    }

    #[test]
    fn test_server_stats() {
        let stats = MqttServerStats::new();
        assert_eq!(stats.active_connections(), 0);
        assert_eq!(stats.total_connections(), 0);
        assert_eq!(stats.total_packets_received(), 0);
        assert_eq!(stats.total_packets_sent(), 0);

        stats
            .active_connections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(stats.active_connections(), 1);
    }

    #[test]
    fn test_extract_entity_id() {
        assert_eq!(
            extract_entity_id(&MqResponse::EntityCreated { id: 42 }),
            Some(42)
        );
        assert_eq!(
            extract_entity_id(&MqResponse::Error(MqError::AlreadyExists {
                entity: bisque_mq::types::EntityKind::Exchange,
                id: 99
            })),
            Some(99)
        );
        assert_eq!(extract_entity_id(&MqResponse::Ok), None);
    }

    #[test]
    fn test_mqtt_topic_matches_filter_exact() {
        assert!(mqtt_topic_matches_filter("a/b/c", "a/b/c"));
        assert!(!mqtt_topic_matches_filter("a/b/c", "a/b/d"));
        assert!(!mqtt_topic_matches_filter("a/b", "a/b/c"));
        assert!(!mqtt_topic_matches_filter("a/b/c", "a/b"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_single_level_wildcard() {
        assert!(mqtt_topic_matches_filter("a/b/c", "a/+/c"));
        assert!(mqtt_topic_matches_filter("a/x/c", "a/+/c"));
        assert!(!mqtt_topic_matches_filter("a/b/c/d", "a/+/c"));
        assert!(mqtt_topic_matches_filter("a/b/c", "+/b/c"));
        assert!(mqtt_topic_matches_filter("a/b/c", "+/+/+"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_multi_level_wildcard() {
        assert!(mqtt_topic_matches_filter("a/b/c", "a/#"));
        assert!(mqtt_topic_matches_filter("a/b", "a/#"));
        assert!(mqtt_topic_matches_filter("a", "a/#"));
        assert!(mqtt_topic_matches_filter("a/b/c/d/e", "#"));
        assert!(!mqtt_topic_matches_filter("b/c", "a/#"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_dollar_prefix() {
        // $-prefixed topics MUST NOT match wildcard filters that don't start with $.
        assert!(!mqtt_topic_matches_filter("$SYS/broker/clients", "#"));
        assert!(!mqtt_topic_matches_filter(
            "$SYS/broker/clients",
            "+/broker/clients"
        ));
        assert!(!mqtt_topic_matches_filter("$SYS/info", "+/info"));
        // But explicit $SYS subscriptions must work.
        assert!(mqtt_topic_matches_filter("$SYS/broker/clients", "$SYS/#"));
        assert!(mqtt_topic_matches_filter(
            "$SYS/broker/clients",
            "$SYS/broker/clients"
        ));
        assert!(mqtt_topic_matches_filter("$SYS/info", "$SYS/+"));
        // Non-$ topics still match wildcards normally.
        assert!(mqtt_topic_matches_filter("a/b/c", "#"));
        assert!(mqtt_topic_matches_filter("a/b/c", "+/b/c"));
    }

    fn build_flat(builder: bisque_mq::flat::FlatMessageBuilder) -> FlatMessage {
        FlatMessage::new(builder.build()).expect("valid FlatMessage")
    }

    #[test]
    fn test_extract_publisher_session_id_present() {
        use bisque_mq::flat::FlatMessageBuilder;
        let msg = build_flat(
            FlatMessageBuilder::new(Bytes::from_static(b"payload")).header(
                Bytes::from_static(b"mqtt.publisher_session_id"),
                Bytes::copy_from_slice(&42u64.to_be_bytes()),
            ),
        );
        assert_eq!(extract_publisher_session_id(&msg), Some(42));
    }

    #[test]
    fn test_extract_publisher_session_id_absent() {
        use bisque_mq::flat::FlatMessageBuilder;
        let msg = build_flat(FlatMessageBuilder::new(Bytes::from_static(b"payload")));
        assert_eq!(extract_publisher_session_id(&msg), None);
    }

    #[test]
    fn test_extract_publisher_session_id_wrong_length() {
        use bisque_mq::flat::FlatMessageBuilder;
        let msg = build_flat(
            FlatMessageBuilder::new(Bytes::from_static(b"payload")).header(
                Bytes::from_static(b"mqtt.publisher_session_id"),
                Bytes::from_static(b"short"),
            ),
        );
        assert_eq!(extract_publisher_session_id(&msg), None);
    }

    #[test]
    fn test_extract_original_retain_true() {
        use bisque_mq::flat::FlatMessageBuilder;
        let msg = build_flat(
            FlatMessageBuilder::new(Bytes::from_static(b"payload")).header(
                Bytes::from_static(b"mqtt.original_retain"),
                Bytes::from_static(&[1]),
            ),
        );
        assert!(extract_original_retain(&msg));
    }

    #[test]
    fn test_extract_original_retain_false() {
        use bisque_mq::flat::FlatMessageBuilder;
        let msg = build_flat(
            FlatMessageBuilder::new(Bytes::from_static(b"payload")).header(
                Bytes::from_static(b"mqtt.original_retain"),
                Bytes::from_static(&[0]),
            ),
        );
        assert!(!extract_original_retain(&msg));
    }

    #[test]
    fn test_extract_original_retain_absent() {
        use bisque_mq::flat::FlatMessageBuilder;
        let msg = build_flat(FlatMessageBuilder::new(Bytes::from_static(b"payload")));
        assert!(!extract_original_retain(&msg));
    }

    #[test]
    fn test_extract_original_retain_empty_value() {
        use bisque_mq::flat::FlatMessageBuilder;
        let msg = build_flat(
            FlatMessageBuilder::new(Bytes::from_static(b"payload"))
                .header(Bytes::from_static(b"mqtt.original_retain"), Bytes::new()),
        );
        assert!(!extract_original_retain(&msg));
    }

    #[test]
    fn test_server_stats_all_getters() {
        let stats = MqttServerStats::new();

        // Initially all zero.
        assert_eq!(stats.active_connections(), 0);
        assert_eq!(stats.total_connections(), 0);
        assert_eq!(stats.total_packets_received(), 0);
        assert_eq!(stats.total_packets_sent(), 0);

        // Increment each counter.
        stats
            .active_connections
            .fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        stats
            .total_connections
            .fetch_add(10, std::sync::atomic::Ordering::Relaxed);
        stats
            .total_packets_received
            .fetch_add(100, std::sync::atomic::Ordering::Relaxed);
        stats
            .total_packets_sent
            .fetch_add(50, std::sync::atomic::Ordering::Relaxed);

        assert_eq!(stats.active_connections(), 5);
        assert_eq!(stats.total_connections(), 10);
        assert_eq!(stats.total_packets_received(), 100);
        assert_eq!(stats.total_packets_sent(), 50);
    }

    #[test]
    fn test_mqtt_topic_matches_filter_empty_levels() {
        // Empty level matches empty level.
        assert!(mqtt_topic_matches_filter("/a", "/a"));
        assert!(mqtt_topic_matches_filter("a/", "a/"));
        assert!(!mqtt_topic_matches_filter("a", "a/"));
        assert!(!mqtt_topic_matches_filter("a/", "a"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_hash_at_root() {
        assert!(mqtt_topic_matches_filter("anything", "#"));
        assert!(mqtt_topic_matches_filter("a/b/c/d/e/f", "#"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_plus_single_level() {
        assert!(mqtt_topic_matches_filter("a", "+"));
        assert!(!mqtt_topic_matches_filter("a/b", "+"));
    }

    #[test]
    fn test_extract_entity_id_other_responses() {
        assert_eq!(extract_entity_id(&MqResponse::Ok), None);
        assert_eq!(
            extract_entity_id(&MqResponse::Error(MqError::NotFound {
                entity: bisque_mq::types::EntityKind::Queue,
                id: 1,
            })),
            None
        );
    }

    #[tokio::test]
    async fn test_encode_decode_integration() {
        use crate::types::*;

        let connect = MqttPacket::Connect(Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 30,
            client_id: "integration-test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        });

        let mut buf = BytesMut::new();
        codec::encode_packet(&connect, &mut buf);

        let (decoded, consumed) = codec::decode_packet(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        let mut session = MqttSession::new(MqttSessionConfig::default());
        let (commands, responses) = session.process_packet(&decoded);

        assert!(session.connected);
        assert_eq!(session.client_id, "integration-test");
        assert_eq!(session.keep_alive, 30);
        assert!(!commands.is_empty());
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MqttPacket::ConnAck(_)));
    }
}
