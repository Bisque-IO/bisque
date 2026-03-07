//! Cluster mesh — full-mesh push-based ephemeral state synchronization.
//!
//! Every node in the cluster maintains TCP connections to all peers and
//! broadcasts state changes (operations, health) so each node holds a
//! complete merged view of cluster-wide ephemeral state.
//!
//! Runs on a dedicated TCP port, separate from Raft consensus. No consensus
//! guarantees — eventually consistent, optimized for freshness.
//!
//! # Usage
//!
//! ```ignore
//! let mesh = ClusterMesh::new(node_id, MeshConfig::default());
//! mesh.start().await?;
//! mesh.add_peer(2, "10.0.0.2:3201".parse().unwrap());
//! mesh.add_peer(3, "10.0.0.3:3201".parse().unwrap());
//!
//! // Bridge OperationsManager changes into the mesh
//! let mut rx = operations_manager.subscribe();
//! let mesh2 = mesh.clone();
//! tokio::spawn(async move {
//!     while let Ok(op) = rx.recv().await {
//!         mesh2.broadcast_operation(OperationSnapshot::from(&op));
//!     }
//! });
//!
//! // Query cluster-wide state
//! let remote_ops = mesh.cluster_state().all_remote_operations();
//! ```

pub mod connection;
pub mod protocol;
pub mod state;
pub mod tls;

pub use protocol::{MeshMessage, NodeHealth, OperationSnapshot};
pub use state::ClusterState;
pub use tls::MeshTlsConfig;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Configuration for the cluster mesh.
#[derive(Debug, Clone)]
pub struct MeshConfig {
    /// TCP port for the mesh listener.
    pub listen_port: u16,
    /// How often to send heartbeats to peers.
    pub heartbeat_interval: Duration,
    /// Mark a peer unreachable if no message arrives within this window.
    pub peer_timeout: Duration,
    /// Initial reconnection delay after a connection failure.
    pub reconnect_base: Duration,
    /// Maximum reconnection delay (backoff cap).
    pub reconnect_max: Duration,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Read/write timeout for individual frames during steady-state.
    pub frame_timeout: Duration,
    /// Maximum lifetime for a mesh TCP connection before transparent refresh.
    pub connection_ttl: Duration,
    /// Timeout for the entire handshake + snapshot exchange phase.
    pub handshake_timeout: Duration,
    /// Maximum concurrent inbound connections being processed.
    pub max_inbound_connections: usize,
    /// How long to keep completed/failed operations in the cluster state
    /// before eviction. Set to `Duration::ZERO` to disable eviction.
    pub operation_retention: Duration,
    /// Optional TLS configuration. When `None`, connections are plaintext.
    pub tls: Option<MeshTlsConfig>,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            listen_port: 3201,
            heartbeat_interval: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(15),
            reconnect_base: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            frame_timeout: Duration::from_secs(30),
            connection_ttl: Duration::from_secs(600),
            handshake_timeout: Duration::from_secs(10),
            max_inbound_connections: 128,
            operation_retention: Duration::from_secs(300), // 5 minutes
            tls: None,
        }
    }
}

/// Handle to a single outbound peer connection task.
struct PeerHandle {
    task: JoinHandle<()>,
    /// Dropping signals the task to send a Close frame and exit.
    _shutdown_tx: watch::Sender<()>,
}

impl Drop for PeerHandle {
    fn drop(&mut self) {
        // Dropping _shutdown_tx signals the task via watch::changed().
        // The task will attempt a graceful Close frame before exiting.
        // Abort as backstop in case the task doesn't exit promptly.
        self.task.abort();
    }
}

/// Cluster mesh: full-mesh push-based ephemeral state sync.
///
/// Thread-safe and cheaply cloneable via `Arc`.
pub struct ClusterMesh {
    inner: Arc<MeshInner>,
}

struct MeshInner {
    node_id: u64,
    config: MeshConfig,
    /// Merged view of all peers' ephemeral state.
    state: Arc<ClusterState>,
    /// Broadcast channel for local state changes.
    /// All outbound peer writers subscribe to this.
    local_change_tx: broadcast::Sender<MeshMessage>,
    /// Active outbound peer connections, keyed by node_id.
    peers: DashMap<u64, PeerHandle>,
    /// Active inbound peer connections, keyed by node_id.
    /// The watch::Sender signals old handlers to shut down on reconnect.
    inbound_peers: DashMap<u64, watch::Sender<()>>,
    /// Monotonic sequence counter for outbound messages.
    seq: AtomicU64,
    /// Function that produces a local state snapshot for handshakes.
    snapshot_fn: parking_lot::RwLock<Option<Arc<dyn Fn() -> MeshMessage + Send + Sync>>>,
    /// Listener task handle.
    listener_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
    /// Heartbeat task handle.
    heartbeat_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
    /// Liveness checker task handle.
    liveness_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
    /// Semaphore limiting concurrent inbound connection handling.
    inbound_semaphore: Arc<Semaphore>,
    /// TLS acceptor for inbound connections (built from config at startup).
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    /// TLS client config for outbound connections (built from config at startup).
    tls_client_config: Option<Arc<rustls::ClientConfig>>,
}

impl Clone for ClusterMesh {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl ClusterMesh {
    /// Create a new mesh instance. Call [`start`] to begin networking.
    ///
    /// If TLS is configured, builds the TLS acceptor and client config
    /// eagerly so that certificate errors are caught early.
    pub fn new(node_id: u64, config: MeshConfig) -> Result<Self, std::io::Error> {
        let (tx, _) = broadcast::channel(256);
        let max_inbound = config.max_inbound_connections;

        let (tls_acceptor, tls_client_config) = match &config.tls {
            Some(tls_config) => {
                let server_config = tls::build_server_config(tls_config)?;
                let client_config = tls::build_client_config(tls_config)?;
                (
                    Some(tokio_rustls::TlsAcceptor::from(server_config)),
                    Some(client_config),
                )
            }
            None => (None, None),
        };

        Ok(Self {
            inner: Arc::new(MeshInner {
                node_id,
                config,
                state: Arc::new(ClusterState::new(node_id)),
                local_change_tx: tx,
                peers: DashMap::new(),
                inbound_peers: DashMap::new(),
                seq: AtomicU64::new(1),
                snapshot_fn: parking_lot::RwLock::new(None),
                listener_handle: parking_lot::Mutex::new(None),
                heartbeat_handle: parking_lot::Mutex::new(None),
                liveness_handle: parking_lot::Mutex::new(None),
                inbound_semaphore: Arc::new(Semaphore::new(max_inbound)),
                tls_acceptor,
                tls_client_config,
            }),
        })
    }

    /// Set the function that produces a local state snapshot.
    ///
    /// This must be called before `start()`. The function is invoked
    /// during handshakes with peers and when a peer requests a resync.
    pub fn set_snapshot_fn<F>(&self, f: F)
    where
        F: Fn() -> MeshMessage + Send + Sync + 'static,
    {
        *self.inner.snapshot_fn.write() = Some(Arc::new(f));
    }

    /// Start the mesh: bind TCP listener, launch heartbeat and liveness tasks.
    pub async fn start(&self) -> Result<(), std::io::Error> {
        let addr: SocketAddr = ([0, 0, 0, 0], self.inner.config.listen_port).into();
        let listener = TcpListener::bind(addr).await?;
        info!(
            node_id = self.inner.node_id,
            port = self.inner.config.listen_port,
            "Cluster mesh listening"
        );

        // Listener task
        {
            let inner = self.inner.clone();
            let handle = tokio::spawn(async move {
                listener_loop(inner, listener).await;
            });
            *self.inner.listener_handle.lock() = Some(handle);
        }

        // Heartbeat task
        {
            let inner = self.inner.clone();
            let handle = tokio::spawn(async move {
                heartbeat_loop(inner).await;
            });
            *self.inner.heartbeat_handle.lock() = Some(handle);
        }

        // Liveness checker task
        {
            let inner = self.inner.clone();
            let handle = tokio::spawn(async move {
                liveness_loop(inner).await;
            });
            *self.inner.liveness_handle.lock() = Some(handle);
        }

        Ok(())
    }

    /// Register a peer and initiate an outbound connection.
    ///
    /// Only the node with the lower `node_id` initiates. If our `node_id`
    /// is higher, we skip — the peer will connect to our listener instead.
    pub fn add_peer(&self, node_id: u64, addr: SocketAddr) {
        if node_id == self.inner.node_id {
            return; // Don't connect to ourselves
        }

        // Only the lower-ID node initiates the outbound connection
        if self.inner.node_id > node_id {
            return;
        }

        if self.inner.peers.contains_key(&node_id) {
            return; // Already connected
        }

        let snapshot_fn = match self.inner.snapshot_fn.read().clone() {
            Some(f) => f,
            None => {
                warn!("Cannot add peer: snapshot_fn not set");
                return;
            }
        };

        let rx = self.inner.local_change_tx.subscribe();
        let config = self.inner.config.clone();
        let state = self.inner.state.clone();
        let local_node_id = self.inner.node_id;
        let tls_client_config = self.inner.tls_client_config.clone();

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let task = tokio::spawn(async move {
            connection::peer_connection_loop(
                local_node_id,
                node_id,
                addr,
                config,
                state,
                rx,
                snapshot_fn,
                shutdown_rx,
                tls_client_config,
            )
            .await;
        });

        self.inner.peers.insert(
            node_id,
            PeerHandle {
                task,
                _shutdown_tx: shutdown_tx,
            },
        );
        info!(peer = node_id, addr = %addr, "Added mesh peer (outbound)");
    }

    /// Remove a peer connection.
    pub fn remove_peer(&self, node_id: u64) {
        // PeerHandle::drop will signal shutdown and abort
        self.inner.peers.remove(&node_id);
        self.inner.inbound_peers.remove(&node_id);
        self.inner.state.remove_peer(node_id);
    }

    /// Update peer list from a set of `(node_id, addr)` pairs.
    /// Adds new peers, removes stale ones. Idempotent.
    pub fn update_peers(&self, peers: Vec<(u64, SocketAddr)>) {
        let new_ids: std::collections::HashSet<u64> = peers.iter().map(|(id, _)| *id).collect();

        // Remove peers no longer in the list
        let current_ids: Vec<u64> = self.inner.peers.iter().map(|e| *e.key()).collect();
        for id in current_ids {
            if !new_ids.contains(&id) {
                self.remove_peer(id);
            }
        }

        // Add new peers
        for (id, addr) in peers {
            self.add_peer(id, addr);
        }
    }

    /// Get the shared cluster state for querying cluster-wide data.
    pub fn cluster_state(&self) -> &Arc<ClusterState> {
        &self.inner.state
    }

    /// Broadcast a local operation change to all connected peers.
    pub fn broadcast_operation(&self, op: OperationSnapshot) {
        let msg = MeshMessage::OperationUpdate {
            node_id: self.inner.node_id,
            operation: op,
            seq: self.next_seq(),
        };
        let _ = self.inner.local_change_tx.send(msg);
    }

    /// Get the local node ID.
    pub fn node_id(&self) -> u64 {
        self.inner.node_id
    }

    fn next_seq(&self) -> u64 {
        self.inner.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Graceful shutdown: stop all tasks and close connections.
    pub fn shutdown(&self) {
        if let Some(h) = self.inner.listener_handle.lock().take() {
            h.abort();
        }
        if let Some(h) = self.inner.heartbeat_handle.lock().take() {
            h.abort();
        }
        if let Some(h) = self.inner.liveness_handle.lock().take() {
            h.abort();
        }
        self.inner.peers.clear(); // PeerHandle::drop signals and aborts each task
        self.inner.inbound_peers.clear();
    }
}

impl Drop for ClusterMesh {
    fn drop(&mut self) {
        // Only shut down if we're the last reference
        if Arc::strong_count(&self.inner) == 1 {
            self.shutdown();
        }
    }
}

/// Accept loop for inbound peer connections.
async fn listener_loop(inner: Arc<MeshInner>, listener: TcpListener) {
    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("Mesh listener accept error: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        debug_assert!(inner.snapshot_fn.read().is_some());
        let snapshot_fn = match inner.snapshot_fn.read().clone() {
            Some(f) => f,
            None => continue,
        };

        let config = inner.config.clone();
        let state = inner.state.clone();
        let local_change_tx = inner.local_change_tx.clone();
        let node_id = inner.node_id;
        let inbound_peers = inner.inbound_peers.clone();
        let semaphore = inner.inbound_semaphore.clone();
        let tls_acceptor = inner.tls_acceptor.clone();

        tokio::spawn(async move {
            // Backpressure: limit concurrent inbound connections
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => return, // Semaphore closed (shutdown)
            };

            // Configure TCP before TLS wrapping
            stream.set_nodelay(true).ok();
            connection::configure_keepalive_tcp(&stream).ok();

            // TLS accept (if configured) or wrap as plain
            let mut mesh_stream = if let Some(ref acceptor) = tls_acceptor {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => tls::MeshStream::ServerTls(tls_stream),
                    Err(e) => {
                        warn!(addr = %peer_addr, "Inbound TLS handshake error: {e}");
                        return;
                    }
                }
            } else {
                tls::MeshStream::Plain(stream)
            };

            // Phase 1: Mesh handshake with timeout — identify the peer
            let peer_node_id = match tokio::time::timeout(
                config.handshake_timeout,
                connection::inbound_handshake(
                    node_id,
                    &mut mesh_stream,
                    &config,
                    &state,
                    &snapshot_fn,
                ),
            )
            .await
            {
                Ok(Ok(peer_id)) => peer_id,
                Ok(Err(e)) => {
                    warn!(addr = %peer_addr, "Inbound mesh handshake error: {e}");
                    return;
                }
                Err(_) => {
                    warn!(addr = %peer_addr, "Inbound mesh handshake timed out");
                    return;
                }
            };

            // Phase 2: Dedup — close any existing connection from this peer
            let (cancel_tx, cancel_rx) = watch::channel(());
            if let Some((_, old_tx)) = inbound_peers.remove(&peer_node_id) {
                // Dropping old_tx closes its watch channel, signaling the old handler
                drop(old_tx);
            }
            inbound_peers.insert(peer_node_id, cancel_tx);

            // Phase 3: Steady-state loop
            match connection::run_inbound_loop(
                peer_node_id,
                mesh_stream,
                &config,
                &state,
                &local_change_tx,
                &snapshot_fn,
                cancel_rx,
            )
            .await
            {
                Ok(()) => {
                    info!(
                        peer = peer_node_id,
                        addr = %peer_addr,
                        "Inbound mesh connection closed"
                    );
                }
                Err(e) => {
                    warn!(
                        peer = peer_node_id,
                        addr = %peer_addr,
                        "Inbound mesh connection error: {e}"
                    );
                }
            }

            // Clean up
            inbound_peers.remove(&peer_node_id);
            state.mark_unreachable(peer_node_id);
        });
    }
}

/// Periodic heartbeat broadcaster.
async fn heartbeat_loop(inner: Arc<MeshInner>) {
    let mut interval = tokio::time::interval(inner.config.heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let health = collect_node_health();
        let seq = inner.seq.fetch_add(1, Ordering::Relaxed);
        let msg = MeshMessage::Heartbeat {
            node_id: inner.node_id,
            health,
            seq,
        };
        let _ = inner.local_change_tx.send(msg);
    }
}

/// Periodic check for unreachable peers.
async fn liveness_loop(inner: Arc<MeshInner>) {
    let check_interval = inner.config.peer_timeout / 2;
    let mut interval = tokio::time::interval(check_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let peer_ids = inner.state.peer_node_ids();
        let now = std::time::Instant::now();

        for node_id in peer_ids {
            // Check last_seen via the DashMap
            let timed_out = inner
                .state
                .peek_last_seen(node_id)
                .map(|last_seen| now.duration_since(last_seen) > inner.config.peer_timeout)
                .unwrap_or(false);

            if timed_out {
                warn!(peer = node_id, "Mesh peer timed out");
                inner.state.mark_unreachable(node_id);
            }
        }

        // Evict stale completed/failed operations to prevent unbounded memory growth
        if !inner.config.operation_retention.is_zero() {
            inner
                .state
                .evict_stale_operations(inner.config.operation_retention);
        }
    }
}

/// Collect basic node health metrics.
fn collect_node_health() -> NodeHealth {
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    // Basic metrics — can be enriched with sysinfo crate later
    NodeHealth {
        uptime_secs: 0, // TODO: track process start time
        cpu_usage_pct: 0,
        memory_used_bytes: 0,
        memory_total_bytes: 0,
        active_connections: 0,
        timestamp_ms,
    }
}
