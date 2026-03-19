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

use bisque_mq::flat::{FlatMessage, MqttEnvelope, is_mqtt_envelope};
use bisque_mq::notifier::GroupNotifier;
use bisque_mq::types::{ExchangeType, RetentionPolicy};
use bisque_mq::{LocalSubmitter, MqCommand, MqReader, ResponseEntry};
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
use crate::types::{ProtocolVersion, QoS};

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
        let mut buf = BytesMut::new();
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
    batcher: Arc<LocalSubmitter>,
    log_reader: Arc<MqReader>,
    stats: Arc<MqttServerStats>,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    active_sessions: ActiveSessions,
    pending_wills: PendingWills,
    /// Dynamic server redirect, checked at connection time (lock-free).
    /// Overrides config.server_redirect when Some.
    dynamic_redirect: Arc<ArcSwapOption<ServerRedirect>>,
    /// Push-based delivery notifier from the engine. When set, the connection
    /// loop wakes on group notifications instead of polling at fixed intervals.
    group_notifier: Option<Arc<GroupNotifier>>,
}

impl MqttServer {
    /// Create a new MQTT server.
    pub fn new(
        config: MqttServerConfig,
        batcher: Arc<LocalSubmitter>,
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
            group_notifier: None,
        }
    }

    /// Set the push-based delivery notifier. When set, the connection loop
    /// wakes on group notifications (sub-millisecond delivery) instead of
    /// polling at fixed intervals (50ms default).
    pub fn with_group_notifier(mut self, notifier: Arc<GroupNotifier>) -> Self {
        self.group_notifier = Some(notifier);
        self
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
        let group_notifier = self.group_notifier.clone();

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
            group_notifier,
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
    batcher: Arc<LocalSubmitter>,
    log_reader: Arc<MqReader>,
    stats: Arc<MqttServerStats>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    active_sessions: ActiveSessions,
    pending_wills: PendingWills,
    dynamic_redirect: Arc<ArcSwapOption<ServerRedirect>>,
    group_notifier: Option<Arc<GroupNotifier>>,
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
                        let group_notifier = group_notifier.clone();

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
                                group_notifier.as_deref(),
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
fn extract_entity_id(resp: &ResponseEntry) -> Option<u64> {
    if resp.tag() == ResponseEntry::TAG_ENTITY_CREATED {
        Some(resp.entity_id())
    } else if resp.is_already_exists() {
        Some(resp.error_entity_id())
    } else {
        None
    }
}

/// Ensure an exchange exists and return its ID, using the session cache.
/// Accepts owned `String` (zero-alloc cache insert) or `&str`.
async fn ensure_exchange(
    session: &mut MqttSession,
    batcher: &LocalSubmitter,
    exchange_name: impl AsRef<str> + Into<String>,
    cached_id: Option<u64>,
) -> Result<u64, ConnectionError> {
    let mut buf = bytes::BytesMut::new();
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_exchange_id(exchange_name.as_ref()) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_exchange(
            &mut buf,
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

/// Ensure a topic exists and return its ID, using the session cache.
/// Accepts owned `String` (zero-alloc cache insert) or `&str`.
async fn ensure_topic(
    session: &mut MqttSession,
    batcher: &LocalSubmitter,
    topic_name: impl AsRef<str> + Into<String>,
    cached_id: Option<u64>,
    retention: RetentionPolicy,
) -> Result<u64, ConnectionError> {
    let mut buf = bytes::BytesMut::new();
    if let Some(id) = cached_id {
        return Ok(id);
    }
    if let Some(id) = session.cached_topic_id(topic_name.as_ref()) {
        return Ok(id);
    }

    let resp = batcher
        .submit(MqCommand::create_topic(
            &mut buf,
            topic_name.as_ref(),
            retention,
            0,
        ))
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

// =============================================================================
// Publish Orchestration
// =============================================================================

/// Execute a PublishPlan: ensure exchange, publish message, handle retained.
///
/// Used for will messages (which carry owned `flat_message: Some(Bytes)`).
/// Normal PUBLISH batching uses `flush_publish_batch` instead.
async fn orchestrate_publish(
    session: &mut MqttSession,
    batcher: &LocalSubmitter,
    plan: PublishPlan,
) -> Result<(), ConnectionError> {
    let mut buf = bytes::BytesMut::new();
    if !plan.has_message() {
        return Ok(()); // Invalid publish (e.g., unknown topic alias)
    }

    // Will plans carry owned Bytes in flat_message.
    let msg = match plan.flat_message {
        Some(ref b) => b.clone(),
        None => return Ok(()), // No owned message — should use flush_publish_batch instead
    };

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
            &mut buf,
            exchange_id,
            &[msg],
        ))
        .await?;

    match resp.tag() {
        ResponseEntry::TAG_OK | ResponseEntry::TAG_PUBLISHED => {}
        ResponseEntry::TAG_ERROR => {
            warn!(error_kind = resp.error_kind(), "exchange publish failed");
        }
        _ => {}
    }

    // 3. Handle retained message.
    if let Some(retained) = plan.retained {
        orchestrate_retained(session, batcher, retained).await?;
    }

    Ok(())
}

/// Flush a batch of accumulated PUBLISH messages as a single
/// `publish_to_exchange_slices` command. Message data lives in `msg_buf`;
/// `publish_ranges` stores (start, len) pairs indexing into `msg_buf`.
/// After flush, `msg_buf` is cleared for the next batch.
async fn flush_publish_batch(
    session: &mut MqttSession,
    batcher: &LocalSubmitter,
    publish_ranges: &mut SmallVec<[(usize, usize); 32]>,
    msg_buf: &mut BytesMut,
    retained_batch: &mut SmallVec<[RetainedPlan; 4]>,
) -> Result<(), ConnectionError> {
    // Ensure the MQTT exchange exists (cached after first call).
    let exchange_name = crate::session::MQTT_EXCHANGE_NAME;
    let cached_id = session.cached_exchange_id(exchange_name);
    let exchange_id = ensure_exchange(session, batcher, exchange_name, cached_id).await?;

    // Build the MqCommand from slices, then clear buffers.
    let cmd = {
        let slices: SmallVec<[&[u8]; 32]> = publish_ranges
            .iter()
            .map(|&(start, len)| &msg_buf[start..start + len])
            .collect();
        {
            let mut scratch = BytesMut::new();
            MqCommand::write_publish_to_exchange(&mut scratch, exchange_id, &slices);
            MqCommand::split_from(&mut scratch)
        }
    };

    publish_ranges.clear();
    msg_buf.clear();

    // Submit all messages in a single publish_to_exchange_slices command.
    let resp = batcher.submit(cmd).await?;

    match resp.tag() {
        ResponseEntry::TAG_OK | ResponseEntry::TAG_PUBLISHED => {}
        ResponseEntry::TAG_ERROR => {
            warn!(
                error_kind = resp.error_kind(),
                "batch exchange publish failed"
            );
        }
        _ => {}
    }

    // Handle retained messages individually (different topics).
    for retained in retained_batch.drain(..) {
        orchestrate_retained(session, batcher, retained).await?;
    }

    Ok(())
}

/// Execute a RetainedPlan: create/publish to the retained topic.
async fn orchestrate_retained(
    session: &mut MqttSession,
    batcher: &LocalSubmitter,
    plan: RetainedPlan,
) -> Result<(), ConnectionError> {
    let mut buf = bytes::BytesMut::new();
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
            let _ = batcher
                .submit(MqCommand::publish(&mut buf, topic_id, &[msg]))
                .await?;
        }
        None => {
            // Clear retained: purge the topic.
            let _ = batcher
                .submit(MqCommand::purge_topic(&mut buf, topic_id, u64::MAX))
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
    batcher: &LocalSubmitter,
    exchange_name: &'static str,
    cached_exchange_id: Option<u64>,
    filters: SmallVec<[SubscribeFilterPlan; 4]>,
    filter_names: &[RetainedFilterInfo],
) -> Result<(), ConnectionError> {
    let mut buf = bytes::BytesMut::new();
    // 1. Ensure the global MQTT exchange.
    let exchange_id = ensure_exchange(session, batcher, exchange_name, cached_exchange_id).await?;

    // 2. Batch queue creation: collect uncached queue creates into a single Raft proposal.
    let mut queue_cmds: SmallVec<[MqCommand; 4]> = SmallVec::new();
    // Track which filter index needs a queue create (index into queue_cmds).
    let mut uncached_queue_indices: SmallVec<[usize; 4]> = SmallVec::new();
    let mut resolved_queue_ids: SmallVec<[u64; 4]> = SmallVec::with_capacity(filters.len());

    for fp in &filters {
        if let Some(id) = fp.cached_queue_id {
            resolved_queue_ids.push(id);
        } else if let Some(id) = session.cached_queue_id(&fp.queue_name) {
            resolved_queue_ids.push(id);
        } else {
            uncached_queue_indices.push(resolved_queue_ids.len());
            resolved_queue_ids.push(0); // placeholder
            queue_cmds.push(MqCommand::create_consumer_group(
                &mut buf,
                &fp.queue_name,
                0,
            ));
        }
    }

    // Submit batched queue creates if needed.
    if !queue_cmds.is_empty() {
        let resp = if queue_cmds.len() == 1 {
            batcher.submit(queue_cmds.pop().unwrap()).await?
        } else {
            batcher
                .submit(MqCommand::batch(&mut buf, &queue_cmds))
                .await?
        };

        // Parse responses and fill in resolved IDs.
        let batch_entries_q: Vec<ResponseEntry>;
        let responses: Vec<&ResponseEntry> = if resp.tag() == ResponseEntry::TAG_BATCH {
            batch_entries_q = resp.batch_entries().collect();
            batch_entries_q.iter().collect()
        } else {
            batch_entries_q = vec![resp.clone()];
            batch_entries_q.iter().collect()
        };
        for (resp_idx, &filter_idx) in uncached_queue_indices.iter().enumerate() {
            if let Some(id) = responses.get(resp_idx).and_then(|r| extract_entity_id(r)) {
                resolved_queue_ids[filter_idx] = id;
                session.cache_queue_id(&filters[filter_idx].queue_name, id);
            } else {
                warn!(?resp, "failed to create/resolve queue in batch");
                return Err(ConnectionError::Batcher(
                    bisque_mq::MqBatcherError::ChannelClosed,
                ));
            }
        }
    }

    // 3. Batch binding creation: collect uncached binding creates into a single Raft proposal.
    let mut binding_cmds: SmallVec<[MqCommand; 4]> = SmallVec::new();
    let mut uncached_binding_indices: SmallVec<[usize; 4]> = SmallVec::new();
    let mut resolved_binding_ids: SmallVec<[u64; 4]> = SmallVec::with_capacity(filters.len());

    for (i, fp) in filters.iter().enumerate() {
        if let Some(id) = fp.cached_binding_id {
            resolved_binding_ids.push(id);
        } else if let Some(id) = session.cached_binding_id(exchange_id, &fp.routing_key) {
            resolved_binding_ids.push(id);
        } else {
            uncached_binding_indices.push(resolved_binding_ids.len());
            resolved_binding_ids.push(0); // placeholder
            let queue_id = resolved_queue_ids[i];
            binding_cmds.push(MqCommand::create_binding_with_opts(
                &mut buf,
                exchange_id,
                queue_id,
                Some(fp.routing_key.as_str()),
                fp.no_local,
                fp.shared_group.as_deref(),
                fp.subscription_id,
            ));
        }
    }

    // Submit batched binding creates if needed.
    if !binding_cmds.is_empty() {
        let resp = if binding_cmds.len() == 1 {
            batcher.submit(binding_cmds.pop().unwrap()).await?
        } else {
            batcher
                .submit(MqCommand::batch(&mut buf, &binding_cmds))
                .await?
        };

        let batch_entries_b: Vec<ResponseEntry>;
        let responses: Vec<&ResponseEntry> = if resp.tag() == ResponseEntry::TAG_BATCH {
            batch_entries_b = resp.batch_entries().collect();
            batch_entries_b.iter().collect()
        } else {
            batch_entries_b = vec![resp.clone()];
            batch_entries_b.iter().collect()
        };
        for (resp_idx, &filter_idx) in uncached_binding_indices.iter().enumerate() {
            if let Some(id) = responses.get(resp_idx).and_then(|r| extract_entity_id(r)) {
                resolved_binding_ids[filter_idx] = id;
                session.cache_binding_id(exchange_id, &filters[filter_idx].routing_key, id);
            } else {
                warn!(?resp, "failed to create/resolve binding in batch");
                return Err(ConnectionError::Batcher(
                    bisque_mq::MqBatcherError::ChannelClosed,
                ));
            }
        }
    }

    // 4. Update subscription mappings with all resolved IDs.
    for (i, (_, name_info)) in filters.iter().zip(filter_names.iter()).enumerate() {
        session.update_subscription_ids(
            &name_info.filter,
            Some(exchange_id),
            Some(resolved_binding_ids[i]),
            Some(resolved_queue_ids[i]),
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

async fn deliver_outbound(
    session: &mut MqttSession,
    batcher: &LocalSubmitter,
    log_reader: &MqReader,
    stream: &mut TcpStream,
    stats: &MqttServerStats,
    batch_size: u32,
    write_buf: &mut BytesMut,
    sub_buf: &mut Vec<SubDeliveryInfo>,
    flat_messages_buf: &mut Vec<Bytes>,
) -> Result<(), ConnectionError> {
    let mut buf = bytes::BytesMut::new();
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
        filter_starts_with_wildcard: _,
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
        // Engine-level filtering: pass exclude_publisher_id for no-local,
        // and current time for message expiry — both handled at Raft apply.
        let exclude_pub = if no_local { my_session_id } else { 0 };
        let resp = batcher
            .submit({
                let mut scratch = BytesMut::new();
                MqCommand::write_group_deliver_filtered(
                    &mut scratch,
                    queue_id,
                    session.session_id,
                    remaining,
                    exclude_pub,
                    now_ms,
                );
                MqCommand::split_from(&mut scratch)
            })
            .await?;

        if resp.tag() == ResponseEntry::TAG_MESSAGES {
            for delivered in resp.messages() {
                // Read flat message bytes from the raft log via log reader.
                flat_messages_buf.clear();
                log_reader.read_messages_at_into(delivered.message_id, flat_messages_buf);

                if flat_messages_buf.is_empty() {
                    // Message has been purged — NACK it so the queue can move on.
                    let _ = batcher
                        .submit(MqCommand::group_nack(
                            &mut buf,
                            queue_id,
                            &[delivered.message_id],
                        ))
                        .await;
                    continue;
                }

                for flat_bytes in flat_messages_buf.iter() {
                    // Format-aware dispatch: MqttEnvelope or FlatMessage
                    let is_envelope = is_mqtt_envelope(flat_bytes);
                    let envelope = if is_envelope {
                        MqttEnvelope::new(flat_bytes)
                    } else {
                        None
                    };
                    let flat = if !is_envelope {
                        FlatMessage::new(flat_bytes)
                    } else {
                        None
                    };

                    if envelope.is_none() && flat.is_none() {
                        continue;
                    }

                    // Extract common fields from either format.
                    let (msg_ttl_ms, msg_timestamp, msg_is_retain) = if let Some(ref env) = envelope
                    {
                        (env.ttl_ms(), env.timestamp(), env.is_retain())
                    } else if let Some(ref f) = flat {
                        (f.ttl_ms(), f.timestamp(), f.is_retain())
                    } else {
                        unreachable!()
                    };

                    // Compute adjusted expiry interval for non-expired messages
                    // (MQTT 5.0 §3.3.2.3.3: subscribers see remaining lifetime).
                    let adjusted_expiry_secs = if is_v5 {
                        if let Some(ttl_ms) = msg_ttl_ms {
                            let elapsed_ms = now_ms.saturating_sub(msg_timestamp);
                            let remaining_ms = ttl_ms.saturating_sub(elapsed_ms);
                            let remaining_secs = (remaining_ms / 1000) as u32;
                            Some(remaining_secs.max(1))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Retain As Published enforcement (M7).
                    let retain_flag = if is_shared {
                        false
                    } else if retain_as_published {
                        msg_is_retain
                    } else {
                        false
                    };

                    // Track inflight + encode directly.
                    let tracking =
                        session.track_outbound_delivery(max_qos, queue_id, delivered.message_id);
                    let packet_id = match tracking {
                        None => continue,
                        Some(pid) => pid,
                    };

                    let topic_alias = if is_v5 {
                        let topic = if let Some(ref env) = envelope {
                            env.topic()
                        } else if let Some(ref f) = flat {
                            f.routing_key().unwrap_or_default()
                        } else {
                            unreachable!()
                        };
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

                    if let Some(ref env) = envelope {
                        codec::encode_publish_from_envelope(
                            env,
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
                    } else if let Some(ref f) = flat {
                        codec::encode_publish_from_flat_with_expiry(
                            f,
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
                    }

                    let client_max = session.client_maximum_packet_size;
                    let encoded_size = write_buf.len() - buf_before;
                    if client_max > 0 && encoded_size > client_max as usize {
                        write_buf.truncate(buf_before);
                        debug!(
                            encoded_size,
                            client_max,
                            "outbound PUBLISH exceeds client maximum_packet_size, skipping"
                        );
                        let _ = batcher
                            .submit(MqCommand::group_ack(
                                &mut buf,
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
    batcher: &Arc<LocalSubmitter>,
    log_reader: &Arc<MqReader>,
    stats: &Arc<MqttServerStats>,
    active_sessions: &ActiveSessions,
    pending_wills: &PendingWills,
    dynamic_redirect: &Arc<ArcSwapOption<ServerRedirect>>,
    group_notifier: Option<&GroupNotifier>,
) -> Result<(), ConnectionError> {
    let mut buf = bytes::BytesMut::new();
    let mut read_buf = BytesMut::with_capacity(config.read_buffer_size);
    // Reusable write buffer — cleared between uses, never re-allocated.
    let mut write_buf = BytesMut::with_capacity(config.read_buffer_size);
    let mut session = MqttSession::new(config.session_config.clone());

    // Wait for CONNECT packet with timeout.
    let connect = tokio::time::timeout(config.connect_timeout, async {
        loop {
            let n = stream.read_buf(&mut read_buf).await?;
            if n == 0 {
                return Err(ConnectionError::Closed);
            }

            match codec::decode_connect_packet(&read_buf) {
                Ok((connect, consumed)) => {
                    let _ = read_buf.split_to(consumed);
                    return Ok(connect);
                }
                Err(CodecError::Incomplete) => continue,
                // GAP-2: Unsupported protocol version → send CONNACK 0x01 before disconnect.
                Err(CodecError::UnsupportedProtocolVersion(_)) => {
                    let connack = crate::types::ConnAck {
                        session_present: false,
                        return_code: crate::types::ConnectReturnCode::UnacceptableProtocolVersion
                            as u8,
                        ..crate::types::ConnAck::default()
                    };
                    write_buf.clear();
                    codec::encode_connack_v(&connack, &mut write_buf, false);
                    let _ = stream.write_all(&write_buf).await;
                    return Err(ConnectionError::Codec(
                        CodecError::UnsupportedProtocolVersion(0),
                    ));
                }
                // Not a CONNECT packet.
                Err(CodecError::ProtocolError) => {
                    return Err(ConnectionError::NotConnect);
                }
                Err(e) => return Err(ConnectionError::Codec(e)),
            }
        }
    })
    .await
    .map_err(|_| ConnectionError::ConnectTimeout)??;

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
        let connack = crate::types::ConnAck {
            session_present: false,
            return_code: reason_code,
            server_reference: Some(Bytes::from(redirect.server_reference.clone())),
            ..crate::types::ConnAck::default()
        };
        write_buf.clear();
        codec::encode_connack_v(&connack, &mut write_buf, false);
        let _ = stream.write_all(&write_buf).await;
        return Err(ConnectionError::Closed);
    }

    // Process CONNECT via session.
    session.handle_connect(&connect);

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
    for i in 0..session.cmd_buf.len() {
        let _ = batcher.submit(session.cmd_buf[i].clone()).await;
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

    // GAP-3: Patch session_present based on whether a stored session was actually found.
    session.patch_connack_session_present(session_resumed);

    // Send CONNACK (already encoded in session.out_buf by handle_connect).
    if !session.out_buf.is_empty() {
        stream.write_all(&session.out_buf).await?;
        stats
            .total_packets_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    stats
        .total_packets_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Send pending retransmits after CONNACK (MQTT 5.0 SS 4.4).
    if session_resumed {
        let retransmit_count = session.pending_retransmits();
        if retransmit_count > 0 {
            debug!(
                client_id = %session.client_id,
                count = retransmit_count,
                "retransmitting pending QoS messages on session resume"
            );
            stream.write_all(&session.out_buf).await?;
            stats.total_packets_sent.fetch_add(
                retransmit_count as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
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
        group_notifier,
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
            let will_plan = session.handle_unclean_disconnect();

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
                        if let Some(ref msg) = plan.flat_message {
                            if let Some(eid) = plan.cached_exchange_id {
                                let cmd =
                                    MqCommand::publish_to_exchange(&mut buf, eid, &[msg.clone()]);
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

            // Submit cleanup commands from cmd_buf.
            for i in 0..session.cmd_buf.len() {
                let _ = batcher.submit(session.cmd_buf[i].clone()).await;
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
    batcher: &Arc<LocalSubmitter>,
    log_reader: &Arc<MqReader>,
    stats: &Arc<MqttServerStats>,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    delivery_poll_ms: u64,
    delivery_batch_size: u32,
    mut takeover_rx: tokio::sync::oneshot::Receiver<()>,
    group_notifier: Option<&GroupNotifier>,
) -> Result<(), ConnectionError> {
    let keep_alive_timeout = if session.keep_alive > 0 {
        Duration::from_secs(session.keep_alive as u64 * 3 / 2)
    } else {
        Duration::from_secs(3600)
    };

    // Push-based delivery: single per-connection Notify registered with GroupNotifier
    // for each subscription queue. Engine notify() wakes this connection's Notify
    // directly — zero intermediate allocations, coalescing semantics.
    let has_push_delivery = group_notifier.is_some();
    let delivery_notify = std::sync::Arc::new(tokio::sync::Notify::new());

    let fallback_poll_ms = if has_push_delivery {
        500
    } else {
        delivery_poll_ms
    };
    let mut delivery_interval = tokio::time::interval(Duration::from_millis(fallback_poll_ms));
    delivery_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Reusable buffers for deliver_outbound — hoisted to avoid per-cycle allocation.
    let mut sub_buf: Vec<SubDeliveryInfo> = Vec::new();
    let mut flat_messages_buf: Vec<Bytes> = Vec::new();

    // Reusable buffers for inbound publish batching — accumulate message ranges
    // into msg_buf and flush as a single publish_to_exchange_slices.
    let mut msg_buf: BytesMut = BytesMut::with_capacity(8192);
    let mut publish_ranges: SmallVec<[(usize, usize); 32]> = SmallVec::new();
    let mut retained_batch: SmallVec<[RetainedPlan; 4]> = SmallVec::new();
    // Reusable buffer for batched ACK responses — encoded bytes accumulated from session.out_buf.
    let mut ack_buf: BytesMut = BytesMut::with_capacity(256);

    // Track which queue_ids have active notification watchers to avoid re-registration.
    let mut watched_queue_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();

    let m = shared_metrics();

    loop {
        // Register this connection's Notify for any NEW subscription queue_ids.
        if let Some(notifier) = group_notifier {
            for sub in session.delivery_info() {
                if watched_queue_ids.insert(sub.queue_id) {
                    notifier.watch(sub.queue_id, &delivery_notify);
                }
            }
        }

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
                        //
                        // PUBLISH batching: accumulate flat messages from consecutive
                        // PUBLISH packets and flush as a single publish_to_exchange
                        // after all packets in this read are processed.
                        publish_ranges.clear();
                        msg_buf.clear();
                        retained_batch.clear();
                        ack_buf.clear();
                        let mut need_disconnect = false;
                        loop {
                            match codec::parse_fixed_header(read_buf) {
                                Ok((packet_type, flags, remaining_length, header_size)) => {
                                    let total_size = header_size + remaining_length;
                                    if read_buf.len() < total_size {
                                        break; // incomplete
                                    }

                                    // Validate fixed header flags per MQTT spec.
                                    if let Err(e) = codec::validate_fixed_header_flags(packet_type, flags) {
                                        return Err(send_disconnect_and_close(
                                            stream, stats, write_buf,
                                            session.protocol_version,
                                            crate::types::ReasonCode::MALFORMED_PACKET.0,
                                            Some(&e.to_string()),
                                        ).await);
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

                                    stats.total_packets_received
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    m.packets_received.increment(1);

                                    // Dispatch directly on packet type u8 — no MqttPacket enum.
                                    let is_v5 = session.protocol_version == ProtocolVersion::V5;
                                    match packet_type {
                                        // PUBLISH (3): batch path — accumulate messages.
                                        3 => {
                                            m.publishes_in.increment(1);
                                            // Freeze for zero-copy Publish decode.
                                            let packet_bytes: Bytes = read_buf.split_to(total_size).freeze();
                                            match codec::decode_publish_from_frozen(
                                                &packet_bytes, header_size, total_size, flags, is_v5,
                                            ) {
                                                Ok(publish) => {
                                                    let plan = session.handle_publish(&publish, &mut msg_buf);
                                                    if !session.out_buf.is_empty() {
                                                        ack_buf.extend_from_slice(&session.out_buf);
                                                    }
                                                    if plan.need_disconnect {
                                                        need_disconnect = true;
                                                    }
                                                    if plan.has_message() {
                                                        publish_ranges.push((plan.msg_start, plan.msg_len));
                                                        if let Some(retained) = plan.retained {
                                                            retained_batch.push(retained);
                                                        }
                                                    }
                                                }
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
                                        // Non-PUBLISH: flush any accumulated PUBLISH batch first,
                                        // then dispatch per packet type.
                                        _ => {
                                            if !ack_buf.is_empty() {
                                                stream.write_all(&ack_buf).await?;
                                                stats.total_packets_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                ack_buf.clear();
                                            }
                                            if !publish_ranges.is_empty() {
                                                flush_publish_batch(
                                                    session, batcher, &mut publish_ranges,
                                                    &mut msg_buf, &mut retained_batch,
                                                ).await?;
                                            }

                                            // Freeze for zero-copy decode of types that need Bytes.
                                            let packet_bytes: Bytes = read_buf.split_to(total_size).freeze();

                                            if let Err(e) = dispatch_inbound_packet(
                                                packet_type, flags, header_size, total_size,
                                                remaining_length, &packet_bytes,
                                                session, batcher, log_reader, stream, stats,
                                                write_buf, m,
                                            ).await {
                                                return Err(e);
                                            }

                                            // DISCONNECT (14): clean close.
                                            if packet_type == 14 {
                                                return Ok(());
                                            }
                                        }
                                    }

                                    if need_disconnect {
                                        if !ack_buf.is_empty() {
                                            stream.write_all(&ack_buf).await?;
                                            stats.total_packets_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        return Err(ConnectionError::Codec(CodecError::ProtocolError));
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

                        // Flush accumulated PUBLISH batch after processing all
                        // complete packets from this read.
                        if !ack_buf.is_empty() {
                            stream.write_all(&ack_buf).await?;
                            stats.total_packets_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        if !publish_ranges.is_empty() {
                            flush_publish_batch(
                                session, batcher, &mut publish_ranges,
                                &mut msg_buf, &mut retained_batch,
                            ).await?;
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

            // Outbound (push-based): wake immediately when any subscription queue
            // has new messages. Single Notify per connection — coalescing semantics.
            _ = delivery_notify.notified(), if has_push_delivery && !watched_queue_ids.is_empty() && !session.is_inflight_full() && session.subscription_count() > 0 => {
                deliver_outbound(session, batcher, log_reader.as_ref(), stream, stats, delivery_batch_size, write_buf, &mut sub_buf, &mut flat_messages_buf).await?;
            }

            // Outbound (poll fallback): safety net for push-based, or primary for non-push.
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

/// Dispatch a single inbound MQTT packet by type, with full orchestration.
///
/// Dispatches on the raw packet type u8 from the fixed header — no MqttPacket enum.
/// For ack packets (PUBACK/PUBREC/PUBREL/PUBCOMP), reads packet_id directly from
/// the wire bytes without constructing intermediate structs.
#[allow(clippy::too_many_arguments)]
async fn dispatch_inbound_packet(
    packet_type: u8,
    _flags: u8,
    header_size: usize,
    total_size: usize,
    remaining_length: usize,
    packet_bytes: &Bytes,
    session: &mut MqttSession,
    batcher: &LocalSubmitter,
    log_reader: &Arc<MqReader>,
    stream: &mut TcpStream,
    stats: &MqttServerStats,
    write_buf: &mut BytesMut,
    m: &MqttMetrics,
) -> Result<(), ConnectionError> {
    let version = session.protocol_version;
    let is_v5 = version == ProtocolVersion::V5;

    match packet_type {
        // CONNECT (1): second CONNECT rejection.
        1 => {
            warn!(client_id = %session.client_id, "received second CONNECT, disconnecting");
            if is_v5 {
                write_buf.clear();
                codec::encode_disconnect_reason(
                    crate::types::ReasonCode::PROTOCOL_ERROR.0,
                    Some(b"second CONNECT not allowed"),
                    write_buf,
                );
                let _ = stream.write_all(write_buf).await;
                stats
                    .total_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            return Err(ConnectionError::Codec(CodecError::InvalidFixedHeaderFlags(
                1,
            )));
        }

        // PUBLISH (3): should not reach here — handled in batch loop.
        3 => unreachable!("PUBLISH handled in batch loop"),

        // PUBACK (4): read packet_id directly from wire bytes.
        4 => {
            let packet_id = codec::read_ack_packet_id(packet_bytes, header_size)?;
            session.handle_puback(packet_id);
            for i in 0..session.cmd_buf.len() {
                let _ = batcher.submit(session.cmd_buf[i].clone()).await;
            }
        }

        // PUBREC (5): read packet_id directly from wire bytes.
        5 => {
            let packet_id = codec::read_ack_packet_id(packet_bytes, header_size)?;
            session.handle_pubrec(packet_id);
            if !session.out_buf.is_empty() {
                stream.write_all(&session.out_buf).await?;
                stats
                    .total_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // PUBREL (6): read packet_id directly from wire bytes.
        6 => {
            let packet_id = codec::read_ack_packet_id(packet_bytes, header_size)?;
            session.handle_pubrel(packet_id);
            if !session.out_buf.is_empty() {
                stream.write_all(&session.out_buf).await?;
                stats
                    .total_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // PUBCOMP (7): read packet_id directly from wire bytes.
        7 => {
            let packet_id = codec::read_ack_packet_id(packet_bytes, header_size)?;
            session.handle_pubcomp(packet_id);
            for i in 0..session.cmd_buf.len() {
                let _ = batcher.submit(session.cmd_buf[i].clone()).await;
            }
        }

        // SUBSCRIBE (8): decode full Subscribe struct.
        8 => {
            m.subscribes.increment(1);
            let subscribe =
                codec::decode_subscribe_raw(packet_bytes, header_size, total_size, is_v5).map_err(
                    |e| {
                        warn!(error = %e, "failed to decode SUBSCRIBE");
                        ConnectionError::Codec(e)
                    },
                )?;

            let mut plan = session.handle_subscribe(
                subscribe.packet_id,
                &subscribe.filters,
                subscribe.subscription_identifier,
            );

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

            // SUBACK already encoded in session.out_buf by handle_subscribe.
            if !session.out_buf.is_empty() {
                stream.write_all(&session.out_buf).await?;
                stats
                    .total_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

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

        // UNSUBSCRIBE (10): decode full Unsubscribe struct.
        10 => {
            let unsubscribe =
                codec::decode_unsubscribe_raw(packet_bytes, header_size, total_size, is_v5)
                    .map_err(|e| ConnectionError::Codec(e))?;
            let string_filters: smallvec::SmallVec<[String; 4]> = unsubscribe
                .filters
                .iter()
                .map(|b| unsafe { std::str::from_utf8_unchecked(b) }.to_string())
                .collect();
            session.handle_unsubscribe(unsubscribe.packet_id, &string_filters);
            for i in 0..session.cmd_buf.len() {
                let _ = batcher.submit(session.cmd_buf[i].clone()).await;
            }
            if !session.out_buf.is_empty() {
                stream.write_all(&session.out_buf).await?;
                stats
                    .total_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // PINGREQ (12): no decode needed.
        12 => {
            session.handle_pingreq();
            for i in 0..session.cmd_buf.len() {
                let _ = batcher.submit(session.cmd_buf[i].clone()).await;
            }
            if !session.out_buf.is_empty() {
                stream.write_all(&session.out_buf).await?;
                stats
                    .total_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // DISCONNECT (14): extract fields directly from wire bytes.
        14 => {
            let (reason_code, session_expiry_interval) =
                codec::read_disconnect_fields(packet_bytes, header_size, remaining_length)?;
            let disconnect = crate::types::Disconnect {
                reason_code,
                session_expiry_interval,
                reason_string: None,
                server_reference: None,
            };
            let will_plan = session.handle_disconnect(Some(&disconnect));
            for i in 0..session.cmd_buf.len() {
                let _ = batcher.submit(session.cmd_buf[i].clone()).await;
            }
            if let Some(plan) = will_plan {
                if let Err(e) = orchestrate_publish(session, batcher, plan).await {
                    warn!(error = %e, "failed to publish will on V5 disconnect");
                }
            }
        }

        // AUTH (15): decode full Auth struct.
        15 => {
            let auth =
                codec::decode_auth_raw(packet_bytes, header_size, total_size, remaining_length)
                    .map_err(|e| ConnectionError::Codec(e))?;
            session.handle_auth(&auth);
            for i in 0..session.cmd_buf.len() {
                let _ = batcher.submit(session.cmd_buf[i].clone()).await;
            }
            if !session.out_buf.is_empty() {
                stream.write_all(&session.out_buf).await?;
                stats
                    .total_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // Server-originated packets should not arrive from client.
        _ => {
            warn!(packet_type, "unexpected packet from client");
        }
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
        write_buf.clear();
        codec::encode_disconnect_reason(
            reason_code,
            reason_string.map(|s| s.as_bytes()),
            write_buf,
        );
        let _ = stream.write_all(write_buf).await;
        stats
            .total_packets_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        if let Some(flat) = FlatMessage::new(&msg_bytes) {
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
        use bisque_mq::types::{EntityKind, MqError};
        assert_eq!(
            extract_entity_id(&ResponseEntry::entity_created(0, 42)),
            Some(42)
        );
        assert_eq!(
            {
                let mut b = BytesMut::new();
                ResponseEntry::write_mq_error(
                    &mut b,
                    0,
                    &MqError::AlreadyExists {
                        entity: EntityKind::Exchange,
                        id: 99,
                    },
                );
                extract_entity_id(&ResponseEntry::split_from(&mut b))
            },
            Some(99)
        );
        assert_eq!(extract_entity_id(&ResponseEntry::ok(0)), None);
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

    fn build_flat_bytes(builder: bisque_mq::flat::FlatMessageBuilder) -> bytes::Bytes {
        let buf = builder.build();
        FlatMessage::new(&buf).expect("valid FlatMessage");
        buf
    }

    // --- Phase 3 Opt 2: publisher_id via FlatMessage fixed header ---

    #[test]
    fn test_flat_publisher_id_present() {
        use bisque_mq::flat::FlatMessageBuilder;
        let buf = build_flat_bytes(FlatMessageBuilder::new(b"payload").publisher_id(42));
        let msg = FlatMessage::new(&buf).unwrap();
        assert_eq!(msg.publisher_id(), 42);
    }

    #[test]
    fn test_flat_publisher_id_absent() {
        use bisque_mq::flat::FlatMessageBuilder;
        let buf = build_flat_bytes(FlatMessageBuilder::new(b"payload"));
        let msg = FlatMessage::new(&buf).unwrap();
        assert_eq!(msg.publisher_id(), 0);
    }

    #[test]
    fn test_flat_publisher_id_no_local_match() {
        use bisque_mq::flat::FlatMessageBuilder;
        let session_id = 99u64;
        let buf = build_flat_bytes(FlatMessageBuilder::new(b"payload").publisher_id(session_id));
        let msg = FlatMessage::new(&buf).unwrap();
        // No-local: skip if publisher_id matches session_id
        let pub_id = msg.publisher_id();
        assert!(pub_id != 0 && pub_id == session_id);
    }

    #[test]
    fn test_flat_publisher_id_no_local_no_match() {
        use bisque_mq::flat::FlatMessageBuilder;
        let buf = build_flat_bytes(FlatMessageBuilder::new(b"payload").publisher_id(42));
        let msg = FlatMessage::new(&buf).unwrap();
        let my_session_id = 99u64;
        let pub_id = msg.publisher_id();
        assert!(pub_id == 0 || pub_id != my_session_id);
    }

    // --- Phase 3 Opt 3: retain flag via FlatMessage FLAG_RETAIN ---

    #[test]
    fn test_flat_retain_flag_true() {
        use bisque_mq::flat::FlatMessageBuilder;
        let buf = build_flat_bytes(FlatMessageBuilder::new(b"payload").retain(true));
        let msg = FlatMessage::new(&buf).unwrap();
        assert!(msg.is_retain());
    }

    #[test]
    fn test_flat_retain_flag_false() {
        use bisque_mq::flat::FlatMessageBuilder;
        let buf = build_flat_bytes(FlatMessageBuilder::new(b"payload").retain(false));
        let msg = FlatMessage::new(&buf).unwrap();
        assert!(!msg.is_retain());
    }

    #[test]
    fn test_flat_retain_flag_absent() {
        use bisque_mq::flat::FlatMessageBuilder;
        let buf = build_flat_bytes(FlatMessageBuilder::new(b"payload"));
        let msg = FlatMessage::new(&buf).unwrap();
        assert!(!msg.is_retain());
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
        use bisque_mq::types::{EntityKind, MqError};
        assert_eq!(extract_entity_id(&ResponseEntry::ok(0)), None);
        assert_eq!(
            {
                let mut b = BytesMut::new();
                ResponseEntry::write_mq_error(
                    &mut b,
                    0,
                    &MqError::NotFound {
                        entity: EntityKind::ConsumerGroup,
                        id: 1,
                    },
                );
                extract_entity_id(&ResponseEntry::split_from(&mut b))
            },
            None
        );
    }

    #[tokio::test]
    async fn test_encode_decode_integration() {
        use crate::types::*;

        let connect = Connect {
            protocol_name: Bytes::from("MQTT"),
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
            client_id: Bytes::from("integration-test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };

        let mut buf = BytesMut::new();
        codec::encode_connect(&connect, &mut buf);

        let (decoded, consumed) = codec::decode_buf::connect(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        let mut session = MqttSession::new(MqttSessionConfig::default());
        session.handle_connect(&decoded);

        assert!(session.connected);
        assert_eq!(session.client_id, "integration-test");
        assert_eq!(session.keep_alive, 30);
        assert!(!session.cmd_buf.is_empty());
        assert!(!session.out_buf.is_empty());
        let (_connack, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
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
            protocol_name: Bytes::from("MQTT"),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags::from_byte(0x02).unwrap(),
            keep_alive: 60,
            client_id: Bytes::from("flow-test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        let mut session = MqttSession::new(MqttSessionConfig::default());
        session.handle_connect(&connect);
        assert!(session.connected);
        assert!(!session.cmd_buf.is_empty());
        let (ca, _) = codec::decode_buf::connack(&session.out_buf).unwrap();
        assert_eq!(ca.return_code, 0x00);

        // Subscribe.
        let filters: smallvec::SmallVec<[TopicFilter; 4]> = smallvec::smallvec![TopicFilter {
            filter: Bytes::from("sensor/+/temp"),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }];
        let plan = session.handle_subscribe(1, &filters, None);
        let (sa, _) = crate::codec::decode_buf::suback(&session.out_buf).unwrap();
        assert_eq!(sa.packet_id, 1);
        assert_eq!(sa.return_codes[0], QoS::AtLeastOnce.as_u8());

        // Publish QoS 0.
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
        let pub_plan = session.handle_publish(&publish, &mut msg_buf);
        assert!(pub_plan.has_message(), "publish should produce a message");
        assert!(
            session.out_buf.is_empty(),
            "QoS 0 should not produce a response"
        );

        // Publish QoS 1.
        let publish_q1 = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: b"sensor/2/temp",
            packet_id: Some(2),
            payload: b"23.1",
            properties: Properties::default(),
        };
        let mut msg_buf_q1 = BytesMut::new();
        let pub_plan_q1 = session.handle_publish(&publish_q1, &mut msg_buf_q1);
        assert!(pub_plan_q1.has_message());
        assert!(!session.out_buf.is_empty());
        let (pa, _) = crate::codec::decode_buf::puback(&session.out_buf).unwrap();
        assert_eq!(pa.packet_id, 2);

        // Disconnect.
        let will_plan = session.handle_disconnect(None);
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
            protocol_name: Bytes::from("MQTT"),
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
            client_id: Bytes::from("shared-test"),
            will: None,
            username: None,
            password: None,
            properties_raw: Vec::new(),
        };
        session.handle_connect(&connect);

        let filters = [TopicFilter {
            filter: Bytes::from("$share/mygroup/topic/test"),
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

    // --- Phase 3 Opt 1: Push-based delivery via GroupNotifier (Notify-based) ---

    #[tokio::test]
    async fn test_group_notifier_watch_and_notify() {
        use bisque_mq::notifier::GroupNotifier;

        let notifier = GroupNotifier::new();
        let conn_notify = Arc::new(tokio::sync::Notify::new());
        notifier.watch(42, &conn_notify);

        // Register notified() BEFORE calling notify (matches HighWaterMark pattern).
        let notified = conn_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        notifier.notify(42);
        let result = tokio::time::timeout(Duration::from_millis(100), notified).await;
        assert!(result.is_ok(), "should receive notification within 100ms");
    }

    #[tokio::test]
    async fn test_group_notifier_multiple_watchers() {
        use bisque_mq::notifier::GroupNotifier;

        let notifier = GroupNotifier::new();
        let n1 = Arc::new(tokio::sync::Notify::new());
        let n2 = Arc::new(tokio::sync::Notify::new());
        notifier.watch(42, &n1);
        notifier.watch(42, &n2);

        let f1 = n1.notified();
        let f2 = n2.notified();
        tokio::pin!(f1, f2);
        f1.as_mut().enable();
        f2.as_mut().enable();

        notifier.notify(42);

        let r1 = tokio::time::timeout(Duration::from_millis(100), f1).await;
        let r2 = tokio::time::timeout(Duration::from_millis(100), f2).await;
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    #[tokio::test]
    async fn test_group_notifier_no_notification_for_other_group() {
        use bisque_mq::notifier::GroupNotifier;

        let notifier = GroupNotifier::new();
        let conn_notify = Arc::new(tokio::sync::Notify::new());
        notifier.watch(42, &conn_notify);

        let notified = conn_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Notify a different group — should NOT wake our watcher.
        notifier.notify(99);
        let result = tokio::time::timeout(Duration::from_millis(50), notified).await;
        assert!(
            result.is_err(),
            "should NOT receive notification for other group"
        );
    }

    #[tokio::test]
    async fn test_group_notifier_merged_connection_notify() {
        use bisque_mq::notifier::GroupNotifier;

        let notifier = GroupNotifier::new();
        // Single connection Notify registered for two groups.
        let conn_notify = Arc::new(tokio::sync::Notify::new());
        notifier.watch(10, &conn_notify);
        notifier.watch(20, &conn_notify);

        // Notify group 10 — connection should wake.
        let notified = conn_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        notifier.notify(10);
        let r = tokio::time::timeout(Duration::from_millis(100), notified).await;
        assert!(r.is_ok(), "should wake on group 10 notification");

        // Notify group 20 — connection should also wake.
        let notified = conn_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        notifier.notify(20);
        let r = tokio::time::timeout(Duration::from_millis(100), notified).await;
        assert!(r.is_ok(), "should wake on group 20 notification");
    }

    #[test]
    fn test_mqtt_server_with_group_notifier() {
        use bisque_mq::notifier::GroupNotifier;

        let notifier = Arc::new(GroupNotifier::new());
        let conn_notify = Arc::new(tokio::sync::Notify::new());
        notifier.watch(1, &conn_notify);
    }

    #[tokio::test]
    async fn test_group_notifier_unwatch_cleans_up() {
        use bisque_mq::notifier::GroupNotifier;

        let notifier = GroupNotifier::new();
        let conn_notify = Arc::new(tokio::sync::Notify::new());
        notifier.watch(42, &conn_notify);

        // Unwatch removes all watchers.
        notifier.unwatch(42);

        // Notify after unwatch should be a no-op (no panic).
        notifier.notify(42);
    }
}
