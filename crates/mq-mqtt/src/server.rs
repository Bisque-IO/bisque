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

use arc_swap::ArcSwapOption;
use dashmap::DashMap;

use smallvec::SmallVec;

use crate::codec::{self, CodecError};
use crate::session::{
    MqttSession, MqttSessionConfig, PublishPlan, RetainedPlan, SubDeliveryInfo,
    SubscribeFilterPlan, SubscribePlan,
};
use crate::session_store::{self, SessionStore};
use crate::types::{MqttPacket, ProtocolVersion, QoS};

/// Registry of active MQTT sessions keyed by client_id.
/// Used for session takeover: when a new connection arrives with the same
/// client_id, we send a shutdown signal to the old connection (MQTT 5.0 SS 3.1.4).
type ActiveSessions = Arc<DashMap<String, tokio::sync::oneshot::Sender<()>>>;

/// Registry of pending delayed will messages keyed by client_id.
/// On reconnect, the pending will is cancelled (MQTT 5.0 SS 3.1.2.11.2).
type PendingWills = Arc<DashMap<String, tokio::task::JoinHandle<()>>>;

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
    /// Server redirection (MQTT 5.0 §4.13).
    /// When set, all CONNECT requests receive a CONNACK with the specified reason
    /// code and Server Reference, directing clients to another server.
    pub server_redirect: Option<ServerRedirect>,
}

/// Server redirection configuration per MQTT 5.0 §4.13.
#[derive(Clone, Debug)]
pub struct ServerRedirect {
    /// The server reference URI (e.g. "other-server:1883").
    pub server_reference: String,
    /// If `true`, uses reason code 0x9D (Server Moved) indicating a permanent redirect.
    /// If `false`, uses 0x9C (Use Another Server) indicating a temporary redirect.
    pub permanent: bool,
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
            server_redirect: None,
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
// Retained Delivery Info
// =============================================================================

/// Lightweight per-filter info for retained message delivery.
/// Owns the filter string (moved from the plan before orchestration
/// consumes queue_name/routing_key — the filter is not cached).
struct RetainedFilterInfo {
    filter: String,
    qos: QoS,
    shared: bool,
    retain_handling: u8,
    is_new_subscription: bool,
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
    active_sessions: ActiveSessions,
    pending_wills: PendingWills,
    /// Dynamic server redirect, checked at connection time (lock-free).
    /// Overrides config.server_redirect when Some.
    dynamic_redirect: Arc<ArcSwapOption<ServerRedirect>>,
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
            active_sessions: Arc::new(DashMap::new()),
            pending_wills: Arc::new(DashMap::new()),
            dynamic_redirect: Arc::new(ArcSwapOption::empty()),
        }
    }

    /// Get a reference to the server statistics.
    pub fn stats(&self) -> Arc<MqttServerStats> {
        Arc::clone(&self.stats)
    }

    /// Set a dynamic server redirect. When set, new connections receive a CONNACK
    /// with Server Reference, directing them to another server.
    /// Pass `None` to clear the redirect and resume normal operation.
    pub fn set_redirect(&self, redirect: Option<ServerRedirect>) {
        self.dynamic_redirect.store(redirect.map(Arc::new));
    }

    /// Get the current effective redirect (dynamic takes priority over config).
    /// Lock-free: uses `ArcSwapOption::load` (atomic pointer read).
    pub fn effective_redirect(&self) -> Option<ServerRedirect> {
        let guard = self.dynamic_redirect.load();
        if let Some(ref r) = *guard {
            Some((**r).clone())
        } else {
            self.config.server_redirect.clone()
        }
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
        let active_sessions = Arc::clone(&self.active_sessions);
        let pending_wills = Arc::clone(&self.pending_wills);
        let dynamic_redirect = Arc::clone(&self.dynamic_redirect);

        let handle = tokio::spawn(accept_loop(
            listener,
            config,
            batcher,
            log_reader,
            stats,
            shutdown_rx,
            active_sessions,
            pending_wills,
            dynamic_redirect,
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
    active_sessions: ActiveSessions,
    pending_wills: PendingWills,
    dynamic_redirect: Arc<ArcSwapOption<ServerRedirect>>,
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
                        let active_sessions = Arc::clone(&active_sessions);
                        let pending_wills = Arc::clone(&pending_wills);
                        let dynamic_redirect = Arc::clone(&dynamic_redirect);

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(
                                stream,
                                peer_addr,
                                &config,
                                &batcher,
                                &log_reader,
                                &stats,
                                &active_sessions,
                                &pending_wills,
                                &dynamic_redirect,
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
/// Accepts owned `String` (zero-alloc cache insert) or `&str`.
async fn ensure_exchange(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    exchange_name: impl AsRef<str> + Into<String>,
    cached_id: Option<u64>,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_exchange_id(exchange_name.as_ref()) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_exchange(
            exchange_name.as_ref(),
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
/// Accepts owned `String` (zero-alloc cache insert) or `&str`.
async fn ensure_queue(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    queue_name: impl AsRef<str> + Into<String>,
    cached_id: Option<u64>,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_queue_id(queue_name.as_ref()) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_consumer_group(queue_name.as_ref(), 0))
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
/// Accepts owned `String` (zero-alloc cache insert) or `&str`.
async fn ensure_topic(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    topic_name: impl AsRef<str> + Into<String>,
    cached_id: Option<u64>,
    retention: RetentionPolicy,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_topic_id(topic_name.as_ref()) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_topic(topic_name.as_ref(), retention, 0))
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
/// Accepts owned `String` (zero-alloc cache insert) or `&str` for routing_key.
async fn ensure_binding(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    exchange_id: u64,
    queue_id: u64,
    routing_key: impl AsRef<str> + Into<String>,
    cached_id: Option<u64>,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_binding_id(exchange_id, routing_key.as_ref()) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_binding(
            exchange_id,
            queue_id,
            Some(routing_key.as_ref()),
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

/// Ensure a binding exists with MQTT 5.0 options (no_local, shared_group, subscription_id).
/// Accepts owned `String` (zero-alloc cache insert) or `&str` for routing_key.
async fn ensure_binding_with_opts(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    exchange_id: u64,
    queue_id: u64,
    routing_key: impl AsRef<str> + Into<String>,
    cached_id: Option<u64>,
    no_local: bool,
    shared_group: Option<&str>,
    subscription_id: Option<u32>,
) -> Result<u64, ConnectionError> {
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_binding_id(exchange_id, routing_key.as_ref()) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_binding_with_opts(
            exchange_id,
            queue_id,
            Some(routing_key.as_ref()),
            no_local,
            shared_group,
            subscription_id,
        ))
        .await?;

    if let Some(id) = extract_entity_id(&resp) {
        session.cache_binding_id(exchange_id, routing_key, id);
        Ok(id)
    } else {
        warn!(?resp, "failed to create/resolve binding with opts");
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
    // Destructure to move topic_name into ensure_topic (zero-alloc cache insert).
    let RetainedPlan {
        topic_name,
        cached_topic_id,
        flat_message,
    } = plan;
    let topic_id = ensure_topic(
        session,
        batcher,
        topic_name,
        cached_topic_id,
        RetentionPolicy {
            max_messages: Some(1), // Keep only latest
            ..RetentionPolicy::default()
        },
    )
    .await?;

    match flat_message {
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
/// Takes `filters` by value to move owned strings into the cache (zero-alloc).
/// `filter_names` supplies the original MQTT filter strings (moved out earlier
/// for retained delivery) — used to update subscription mappings.
async fn orchestrate_subscribe(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    exchange_name: &'static str,
    cached_exchange_id: Option<u64>,
    filters: SmallVec<[SubscribeFilterPlan; 4]>,
    filter_names: &[RetainedFilterInfo],
) -> Result<(), ConnectionError> {
    // 1. Ensure the global MQTT exchange.
    let exchange_id = ensure_exchange(session, batcher, exchange_name, cached_exchange_id).await?;

    // 2. For each filter, create queue and binding — moves owned strings into cache.
    for (filter_plan, name_info) in filters.into_iter().zip(filter_names.iter()) {
        // Destructure to move owned strings instead of cloning.
        let SubscribeFilterPlan {
            filter: _, // already moved to RetainedFilterInfo
            queue_name,
            cached_queue_id,
            routing_key,
            cached_binding_id,
            qos: _,
            shared_group,
            no_local,
            subscription_id,
            retain_handling: _,
            is_new_subscription: _,
        } = filter_plan;

        // Ensure queue — moves queue_name into cache.
        let queue_id = ensure_queue(session, batcher, queue_name, cached_queue_id).await?;

        // Ensure binding (with MQTT 5.0 opts: no_local, shared_group, subscription_id).
        let binding_id = if no_local || shared_group.is_some() || subscription_id.is_some() {
            ensure_binding_with_opts(
                session,
                batcher,
                exchange_id,
                queue_id,
                routing_key,
                cached_binding_id,
                no_local,
                shared_group.as_deref(),
                subscription_id,
            )
            .await?
        } else {
            ensure_binding(
                session,
                batcher,
                exchange_id,
                queue_id,
                routing_key,
                cached_binding_id,
            )
            .await?
        };

        // Update the subscription mapping with resolved IDs.
        session.update_subscription_ids(
            &name_info.filter,
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
// SubDeliveryInfo is now defined in session.rs and cached per-session.

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

    // Use cached delivery info from session (rebuilt only on subscribe/unsubscribe).
    sub_buf.clear();
    sub_buf.extend_from_slice(session.delivery_info());

    let is_v5 = session.protocol_version == ProtocolVersion::V5;
    let my_session_id = session.session_id;
    // Cache current time once per delivery batch (avoids syscall per message).
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    for &SubDeliveryInfo {
        queue_id,
        max_qos,
        no_local,
        retain_as_published,
        filter_starts_with_wildcard,
        is_shared,
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
            .submit(MqCommand::group_deliver(
                queue_id,
                session.session_id,
                remaining,
            ))
            .await?;

        if let MqResponse::Messages { messages } = resp {
            for delivered in &messages {
                // Read flat message bytes from the raft log via log reader.
                flat_messages_buf.clear();
                log_reader.read_messages_at_into(delivered.message_id, flat_messages_buf);

                if flat_messages_buf.is_empty() {
                    // Message has been purged — NACK it so the queue can move on.
                    let _ = batcher
                        .submit(MqCommand::group_nack(queue_id, &[delivered.message_id]))
                        .await;
                    continue;
                }

                for flat_bytes in flat_messages_buf.iter() {
                    if let Some(flat) = FlatMessage::new(flat_bytes.clone()) {
                        // GAP-8: $-topic filtering (MQTT 3.1.1 SS 4.7.2).
                        // Topics starting with $ must not be delivered to subscriptions
                        // whose filter starts with a wildcard (+ or #).
                        if filter_starts_with_wildcard {
                            if let Some(topic) = flat.routing_key() {
                                if topic.first() == Some(&b'$') {
                                    let _ = batcher
                                        .submit(MqCommand::group_ack(
                                            queue_id,
                                            &[delivered.message_id],
                                            None,
                                        ))
                                        .await;
                                    continue;
                                }
                            }
                        }

                        // No Local enforcement (M6): skip messages published by this session.
                        if no_local {
                            if let Some(pub_session_id) = extract_publisher_session_id(&flat) {
                                if pub_session_id == my_session_id {
                                    // ACK the message so it's removed from the queue.
                                    let _ = batcher
                                        .submit(MqCommand::group_ack(
                                            queue_id,
                                            &[delivered.message_id],
                                            None,
                                        ))
                                        .await;
                                    continue;
                                }
                            }
                        }

                        // Message Expiry enforcement (MQTT 5.0 §3.3.2.3.3):
                        // 1. Drop messages that have fully expired.
                        // 2. Compute remaining lifetime so subscribers see the adjusted interval.
                        let adjusted_expiry_secs = if is_v5 {
                            if let Some(ttl_ms) = flat.ttl_ms() {
                                let ts = flat.timestamp();
                                let elapsed_ms = now_ms.saturating_sub(ts);
                                if elapsed_ms >= ttl_ms {
                                    // Message has expired — ACK and skip.
                                    let _ = batcher
                                        .submit(MqCommand::group_ack(
                                            queue_id,
                                            &[delivered.message_id],
                                            None,
                                        ))
                                        .await;
                                    continue;
                                }
                                let remaining_ms = ttl_ms - elapsed_ms;
                                let remaining_secs = (remaining_ms / 1000) as u32;
                                Some(remaining_secs.max(1)) // at least 1s if not yet expired
                            } else {
                                None // no expiry set
                            }
                        } else {
                            None
                        };

                        // Retain As Published enforcement (M7): determine retain flag.
                        // MQTT 5.0 §4.8.2: retain flag must be 0 for shared subscriptions.
                        let retain_flag = if is_shared {
                            false
                        } else if retain_as_published {
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

                        // GAP-4: Set DUP=1 on first delivery after session resume.
                        let dup = session.session_resumed && max_qos != QoS::AtMostOnce;

                        // Maximum Packet Size enforcement — outbound (M10).
                        let buf_before = write_buf.len();
                        codec::encode_publish_from_flat_with_expiry(
                            &flat,
                            max_qos,
                            retain_flag,
                            dup,
                            packet_id,
                            is_v5,
                            subscription_id,
                            topic_alias,
                            adjusted_expiry_secs,
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
                                .submit(MqCommand::group_ack(
                                    queue_id,
                                    &[delivered.message_id],
                                    None,
                                ))
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

    // GAP-4: Clear session_resumed after first delivery cycle so subsequent
    // deliveries don't set DUP=1.
    if session.session_resumed {
        session.session_resumed = false;
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
    active_sessions: &ActiveSessions,
    pending_wills: &PendingWills,
    dynamic_redirect: &Arc<ArcSwapOption<ServerRedirect>>,
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
                // GAP-2: Unsupported protocol version → send CONNACK 0x01 before disconnect.
                Err(CodecError::UnsupportedProtocolVersion(_)) => {
                    let connack = MqttPacket::ConnAck(crate::types::ConnAck {
                        session_present: false,
                        return_code: crate::types::ConnectReturnCode::UnacceptableProtocolVersion
                            as u8,
                        properties: crate::types::Properties::default(),
                    });
                    write_buf.clear();
                    codec::encode_packet(&connack, &mut write_buf);
                    let _ = stream.write_all(&write_buf).await;
                    return Err(ConnectionError::Codec(
                        CodecError::UnsupportedProtocolVersion(0),
                    ));
                }
                Err(e) => return Err(ConnectionError::Codec(e)),
            }
        }
    })
    .await
    .map_err(|_| ConnectionError::ConnectTimeout)??;

    if !matches!(first_packet, MqttPacket::Connect(_)) {
        return Err(ConnectionError::NotConnect);
    }

    // Server Redirection (MQTT 5.0 §4.13): lock-free read; dynamic takes priority over config.
    let effective_redirect = {
        let guard = dynamic_redirect.load();
        if let Some(ref r) = *guard {
            Some((**r).clone())
        } else {
            config.server_redirect.clone()
        }
    };
    if let Some(ref redirect) = effective_redirect {
        let reason_code = if redirect.permanent { 0x9D } else { 0x9C };
        let mut props = crate::types::Properties::default();
        props.server_reference = Some(redirect.server_reference.clone());
        let connack = MqttPacket::ConnAck(crate::types::ConnAck {
            session_present: false,
            return_code: reason_code,
            properties: props,
        });
        write_buf.clear();
        codec::encode_packet(&connack, &mut write_buf);
        let _ = stream.write_all(&write_buf).await;
        return Err(ConnectionError::Closed);
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
                // Restore flow control counters and rate limiting quota.
                session.restore_flow_control(
                    persisted.inbound_qos_inflight,
                    persisted.outbound_qos1_count,
                    persisted.remaining_quota,
                );
                // GAP-4: Mark session as resumed so outbound deliveries set DUP=1.
                session.session_resumed = true;
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
    for mut plan in restore_plans {
        let mut name_infos: SmallVec<[RetainedFilterInfo; 4]> = SmallVec::new();
        let mut orch_filters: SmallVec<[SubscribeFilterPlan; 4]> = SmallVec::new();
        for mut fp in std::mem::take(&mut plan.filters) {
            name_infos.push(RetainedFilterInfo {
                filter: std::mem::take(&mut fp.filter),
                qos: fp.qos,
                shared: fp.shared_group.is_some(),
                retain_handling: fp.retain_handling,
                is_new_subscription: fp.is_new_subscription,
            });
            orch_filters.push(fp);
        }
        if let Err(e) = orchestrate_subscribe(
            &mut session,
            batcher,
            plan.exchange_name,
            plan.cached_exchange_id,
            orch_filters,
            &name_infos,
        )
        .await
        {
            warn!(error = %e, "failed to restore subscription");
        }
    }

    // Collect pending retransmits before sending CONNACK (MQTT 5.0 SS 4.4).
    let retransmits = if session_resumed {
        session.pending_retransmits()
    } else {
        smallvec::SmallVec::new()
    };

    // Send CONNACK using reusable write buffer (version-aware encoding).
    // GAP-3: Patch session_present based on whether a stored session was actually found.
    // - session_resumed=true  → session_present=true  (stored session exists)
    // - session_resumed=false → session_present=false  (no stored session, even if CleanSession=0)
    for response in responses {
        let response = if let MqttPacket::ConnAck(mut connack) = response {
            connack.session_present = session_resumed;
            MqttPacket::ConnAck(connack)
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

    // Send pending retransmits after CONNACK (MQTT 5.0 SS 4.4).
    if !retransmits.is_empty() {
        debug!(
            client_id = %session.client_id,
            count = retransmits.len(),
            "retransmitting pending QoS messages on session resume"
        );
        send_packets(
            &mut stream,
            &retransmits,
            stats,
            &mut write_buf,
            session.protocol_version,
        )
        .await?;
    }

    info!(
        peer = %peer_addr,
        client_id = %session.client_id,
        "MQTT client connected"
    );

    // Session Takeover (MQTT 5.0 SS 3.1.4): if another connection with the same
    // ClientID exists, signal it to disconnect with reason 0x8E (Session Taken Over).
    if let Some((_, old_tx)) = active_sessions.remove(&session.client_id) {
        debug!(
            client_id = %session.client_id,
            "session takeover: signaling old connection to disconnect"
        );
        let _ = old_tx.send(());
    }

    // Cancel any pending delayed will from a previous connection (MQTT 5.0 SS 3.1.2.11.2).
    if let Some((_, handle)) = pending_wills.remove(&session.client_id) {
        debug!(
            client_id = %session.client_id,
            "cancelling pending delayed will on reconnect"
        );
        handle.abort();
    }

    // Register this connection in the active sessions registry.
    let (takeover_tx, takeover_rx) = tokio::sync::oneshot::channel::<()>();
    active_sessions.insert(session.client_id.clone(), takeover_tx);

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
        takeover_rx,
    )
    .await;

    // Remove from active sessions on disconnect.
    // The entry might already have been replaced by a new connection (takeover),
    // so this is a best-effort cleanup.
    active_sessions.remove(&session.client_id);

    // Handle disconnect.
    match &result {
        Ok(()) => {
            // Clean disconnect — handle_disconnect was already called.
        }
        Err(_) => {
            // Unclean disconnect.
            let will_delay = session.will_delay_interval();
            let (will_plan, commands) = session.handle_unclean_disconnect();

            // Publish will message if present.
            if let Some(plan) = will_plan {
                if will_delay > 0 {
                    // MQTT 5.0 SS 3.1.3.9.2: Delay will publication.
                    // Spawn a task that sleeps for the delay interval and then publishes.
                    // The task is registered in pending_wills so it can be cancelled
                    // if the client reconnects before the delay expires.
                    let batcher = Arc::clone(batcher);
                    let client_id = session.client_id.clone();
                    let pw = Arc::clone(pending_wills);
                    let handle = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(will_delay as u64)).await;
                        // Delay expired — publish the will.
                        // We can't use orchestrate_publish here (needs session),
                        // so we submit the flat_message directly if exchange is known.
                        if !plan.flat_message.is_empty() {
                            if let Some(eid) = plan.cached_exchange_id {
                                let cmd = MqCommand::publish_to_exchange(eid, &[plan.flat_message]);
                                let _ = batcher.submit(cmd).await;
                            }
                        }
                        pw.remove(&client_id);
                    });
                    pending_wills.insert(session.client_id.clone(), handle);
                } else {
                    if let Err(e) = orchestrate_publish(&mut session, batcher, plan).await {
                        warn!(error = %e, "failed to publish will message");
                    }
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
    mut takeover_rx: tokio::sync::oneshot::Receiver<()>,
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

            // Session takeover: another connection with the same ClientID connected.
            _ = &mut takeover_rx => {
                warn!(
                    client_id = %session.client_id,
                    "session taken over by new connection"
                );
                // Send DISCONNECT with reason 0x8E (Session Taken Over) for MQTT 5.0.
                return Err(send_disconnect_and_close(
                    stream,
                    stats,
                    write_buf,
                    session.protocol_version,
                    crate::types::ReasonCode::SESSION_TAKEN_OVER.0,
                    Some("session taken over"),
                ).await);
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

            // If the session flagged a disconnect (e.g., receive maximum exceeded,
            // topic alias 0), send the DISCONNECT and close.
            if let Some(ref disconnect_pkt) = plan.disconnect {
                send_packets(
                    stream,
                    std::slice::from_ref(disconnect_pkt),
                    stats,
                    write_buf,
                    version,
                )
                .await?;
                return Err(ConnectionError::Codec(CodecError::ProtocolError));
            }

            // Orchestrate the actual publish through the exchange.
            orchestrate_publish(session, batcher, plan).await?;
        }

        MqttPacket::Subscribe(subscribe) => {
            m.subscribes.increment(1);
            // Full orchestration: create exchange, queues, bindings.
            let mut plan = session.handle_subscribe(
                subscribe.packet_id,
                &subscribe.filters,
                subscribe.properties.subscription_identifier,
            );

            // Split filter plans: extract filter string + scalars for retained delivery,
            // leave queue_name/routing_key for orchestration to move into cache.
            let mut retained_infos: SmallVec<[RetainedFilterInfo; 4]> = SmallVec::new();
            let mut orch_filters: SmallVec<[SubscribeFilterPlan; 4]> = SmallVec::new();
            for mut fp in std::mem::take(&mut plan.filters) {
                retained_infos.push(RetainedFilterInfo {
                    filter: std::mem::take(&mut fp.filter),
                    qos: fp.qos,
                    shared: fp.shared_group.is_some(),
                    retain_handling: fp.retain_handling,
                    is_new_subscription: fp.is_new_subscription,
                });
                orch_filters.push(fp);
            }

            // Orchestrate entity creation — moves queue_name/routing_key into cache (zero alloc).
            if let Err(e) = orchestrate_subscribe(
                session,
                batcher,
                plan.exchange_name,
                plan.cached_exchange_id,
                orch_filters,
                &retained_infos,
            )
            .await
            {
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
                &retained_infos,
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
    retained_filters: &[RetainedFilterInfo],
    write_buf: &mut BytesMut,
) -> Result<(), ConnectionError> {
    // Clone once per subscribe (not per message) to avoid borrow conflict with `session`.
    let retained_prefix = session.config.retained_prefix.clone();

    for info in retained_filters {
        // MQTT 5.0 §4.8.2: Retained messages MUST NOT be sent for shared subscriptions.
        if info.shared {
            continue;
        }

        // Retain Handling enforcement (M8):
        // 0 = send retained at subscribe time (default)
        // 1 = send only if this is a new subscription
        // 2 = do not send retained messages at subscribe time
        match info.retain_handling {
            2 => continue,                              // Never send retained
            1 if !info.is_new_subscription => continue, // Only new subs
            _ => {}                                     // 0 or 1-with-new: proceed
        }

        let filter = &info.filter;

        if !filter.contains('+') && !filter.contains('#') {
            // Exact match: look up the specific retained topic.
            let mut retained_topic_name =
                String::with_capacity(retained_prefix.len() + filter.len());
            retained_topic_name.push_str(&retained_prefix);
            retained_topic_name.push_str(filter);
            let topics = log_reader.list_topics_with_prefix(&retained_topic_name);
            for (name, topic_id) in topics {
                if name.as_ref() == retained_topic_name.as_bytes() {
                    deliver_single_retained(
                        session, log_reader, stream, stats, topic_id, info.qos, write_buf,
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
                            session, log_reader, stream, stats, topic_id, info.qos, write_buf,
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

            // Message Expiry enforcement for retained messages (MQTT 5.0 §3.3.2.3.3).
            let adjusted_expiry_secs = if is_v5 {
                if let Some(ttl_ms) = flat.ttl_ms() {
                    let ts = flat.timestamp();
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let elapsed_ms = now_ms.saturating_sub(ts);
                    if elapsed_ms >= ttl_ms {
                        return Ok(()); // retained message has expired — skip
                    }
                    Some(((ttl_ms - elapsed_ms) / 1000).max(1) as u32)
                } else {
                    None
                }
            } else {
                None
            };

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
            codec::encode_publish_from_flat_with_expiry(
                &flat,
                max_qos,
                true, // retain flag
                false,
                packet_id,
                is_v5,
                subscription_id,
                topic_alias,
                adjusted_expiry_secs,
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

    // =========================================================================
    // MQTT 3.1.1 Compliance: Topic Matching Tests
    // =========================================================================

    // ---- $-topic filtering edge cases [MQTT-4.7.2-1] ----

    #[test]
    fn test_mqtt_topic_matches_filter_dollar_with_hash_only() {
        // "#" must NOT match any $-prefixed topic.
        assert!(!mqtt_topic_matches_filter("$SYS", "#"));
        assert!(!mqtt_topic_matches_filter("$SYS/broker/load", "#"));
        assert!(!mqtt_topic_matches_filter("$any/topic", "#"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_dollar_with_plus_at_start() {
        // "+/..." must NOT match $-prefixed topics.
        assert!(!mqtt_topic_matches_filter("$SYS/info", "+/info"));
        assert!(!mqtt_topic_matches_filter("$SYS/broker", "+/broker"));
        assert!(!mqtt_topic_matches_filter("$OTHER/data", "+/data"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_dollar_explicit_subscription() {
        // Explicit $-prefix subscriptions MUST work.
        assert!(mqtt_topic_matches_filter("$SYS/info", "$SYS/info"));
        assert!(mqtt_topic_matches_filter("$SYS/broker/load", "$SYS/#"));
        assert!(mqtt_topic_matches_filter("$SYS/broker/load", "$SYS/+/load"));
        assert!(mqtt_topic_matches_filter("$SYS/info", "$SYS/+"));
        assert!(mqtt_topic_matches_filter(
            "$share/group/topic",
            "$share/group/topic"
        ));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_dollar_non_dollar_normal() {
        // Non-$-prefixed topics should match normally.
        assert!(mqtt_topic_matches_filter("sys/info", "+/info"));
        assert!(mqtt_topic_matches_filter("sys/info", "#"));
    }

    // ---- Topic matching: separator edge cases ----

    #[test]
    fn test_mqtt_topic_matches_filter_trailing_separator() {
        assert!(mqtt_topic_matches_filter("a/b/", "a/b/"));
        assert!(!mqtt_topic_matches_filter("a/b/", "a/b"));
        assert!(!mqtt_topic_matches_filter("a/b", "a/b/"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_leading_separator() {
        assert!(mqtt_topic_matches_filter("/a/b", "/a/b"));
        assert!(mqtt_topic_matches_filter("/a/b", "/+/b"));
        assert!(mqtt_topic_matches_filter("/a/b", "/#"));
        assert!(!mqtt_topic_matches_filter("a/b", "/a/b"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_single_char() {
        assert!(mqtt_topic_matches_filter("a", "a"));
        assert!(mqtt_topic_matches_filter("a", "+"));
        assert!(mqtt_topic_matches_filter("a", "#"));
        assert!(!mqtt_topic_matches_filter("a", "b"));
    }

    #[test]
    fn test_mqtt_topic_matches_filter_deep_nesting() {
        let topic = "a/b/c/d/e/f/g/h/i/j";
        assert!(mqtt_topic_matches_filter(topic, "a/b/c/d/e/f/g/h/i/j"));
        assert!(mqtt_topic_matches_filter(topic, "a/b/c/d/e/f/g/h/i/+"));
        assert!(mqtt_topic_matches_filter(topic, "a/#"));
        assert!(mqtt_topic_matches_filter(topic, "+/+/+/+/+/+/+/+/+/+"));
        assert!(!mqtt_topic_matches_filter(topic, "a/b/c/d/e/f/g/h/i"));
    }

    // ---- Integration: CONNECT → SUBSCRIBE → PUBLISH flow ----

    #[tokio::test]
    async fn test_full_connect_subscribe_publish_flow() {
        use crate::types::*;

        // Connect.
        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: "flow-test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        let mut session = MqttSession::new(MqttSessionConfig::default());
        let (cmds, connack) = session.handle_connect(&connect);
        assert!(session.connected);
        assert!(!cmds.is_empty());
        match connack {
            MqttPacket::ConnAck(ca) => assert_eq!(ca.return_code, 0x00),
            _ => panic!("expected ConnAck"),
        }

        // Subscribe.
        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: "sensor/+/temp".to_string(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        let plan = session.handle_subscribe(1, &filters, None);
        match plan.suback {
            MqttPacket::SubAck(sa) => {
                assert_eq!(sa.packet_id, 1);
                assert_eq!(sa.return_codes[0], QoS::AtLeastOnce.as_u8());
            }
            _ => panic!("expected SubAck"),
        }

        // Publish QoS 0.
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from_static(b"sensor/1/temp"),
            packet_id: None,
            payload: Bytes::from_static(b"22.5"),
            properties: Properties::default(),
        };
        let pub_plan = session.handle_publish(&publish);
        assert!(
            !pub_plan.flat_message.is_empty(),
            "publish should produce a message"
        );
        assert!(
            pub_plan.responses.is_empty(),
            "QoS 0 should not produce a response"
        );

        // Publish QoS 1.
        let publish_q1 = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: Bytes::from_static(b"sensor/2/temp"),
            packet_id: Some(2),
            payload: Bytes::from_static(b"23.1"),
            properties: Properties::default(),
        };
        let pub_plan_q1 = session.handle_publish(&publish_q1);
        assert!(!pub_plan_q1.flat_message.is_empty());
        assert_eq!(pub_plan_q1.responses.len(), 1);
        match &pub_plan_q1.responses[0] {
            MqttPacket::PubAck(pa) => assert_eq!(pa.packet_id, 2),
            _ => panic!("expected PubAck"),
        }

        // Disconnect.
        let (will_plan, _) = session.handle_disconnect(None);
        assert!(will_plan.is_none());
        assert!(!session.connected);
    }

    // ---- Server config validation ----

    #[test]
    fn test_server_config_custom_values() {
        let config = MqttServerConfig {
            bind_addr: "127.0.0.1:8883".parse().unwrap(),
            read_buffer_size: 16384,
            connect_timeout: Duration::from_secs(30),
            delivery_poll_ms: 100,
            delivery_batch_size: 50,
            ..Default::default()
        };
        assert_eq!(config.bind_addr.port(), 8883);
        assert_eq!(config.read_buffer_size, 16384);
        assert_eq!(config.delivery_poll_ms, 100);
        assert_eq!(config.delivery_batch_size, 50);
    }

    // ---- Server Redirection tests (GAP-11) ----

    #[test]
    fn test_server_redirect_config_defaults_to_none() {
        let config = MqttServerConfig::default();
        assert!(config.server_redirect.is_none());
    }

    #[test]
    fn test_server_redirect_config_temporary() {
        let config = MqttServerConfig {
            server_redirect: Some(ServerRedirect {
                server_reference: "backup-server:1883".to_string(),
                permanent: false,
            }),
            ..Default::default()
        };
        let redirect = config.server_redirect.as_ref().unwrap();
        assert_eq!(redirect.server_reference, "backup-server:1883");
        assert!(!redirect.permanent);
    }

    #[test]
    fn test_server_redirect_config_permanent() {
        let config = MqttServerConfig {
            server_redirect: Some(ServerRedirect {
                server_reference: "new-server:1883".to_string(),
                permanent: true,
            }),
            ..Default::default()
        };
        let redirect = config.server_redirect.as_ref().unwrap();
        assert!(redirect.permanent);
    }

    // ---- Shared subscription retained suppression test (GAP-6) ----

    #[test]
    fn test_subscribe_plan_shared_group_field() {
        // Verify that SubscribeFilterPlan carries shared_group for retained suppression.
        use crate::session::{MqttSession, MqttSessionConfig};
        use crate::types::*;

        let mut session = MqttSession::new(MqttSessionConfig::default());
        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V5,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 60,
            client_id: "shared-test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        };
        session.handle_connect(&connect);

        let filters = [TopicFilter {
            filter: "$share/mygroup/topic/test".to_string(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];

        let plan = session.handle_subscribe(1, &filters, None);
        // Shared subscription plan should have shared_group set.
        assert_eq!(plan.filters.len(), 1);
        assert_eq!(plan.filters[0].shared_group, Some("mygroup".to_string()));
    }
}
