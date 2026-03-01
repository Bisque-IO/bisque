//! Tokio TCP Transport Implementation for Multi-Raft
//!
//! Provides a concrete implementation of the MultiplexedTransport trait using
//! tokio's TcpStream with true connection multiplexing.
//!
//! ## True Connection Multiplexing
//!
//! This transport implements proper multiplexing where:
//! - Multiple requests can be in-flight concurrently on a single TCP connection
//! - Responses can arrive out-of-order and are correlated by request ID
//! - Each connection has a dedicated reader task that demultiplexes responses
//! - Connection pools maintain multiple connections per peer for maximum throughput
//! - Connections have a TTL to prevent degradation from long-lived connections

use crate::multi::codec::{Decode, Encode, ResponseMessage, RpcMessage};
use crate::multi::network::MultiplexedTransport;
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use openraft::RaftTypeConfig;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::borrow::Cow;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Thread-local buffer pool for encoding to avoid repeated allocations.
// The pool maintains a single buffer per thread that grows to accommodate the largest message.
thread_local! {
    static ENCODE_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Return a buffer to the thread-local pool for reuse.
/// If the current pool buffer is smaller, replace it with this one.
/// The buffer should be cleared before being returned.
pub fn return_encode_buffer(mut buffer: Vec<u8>) {
    buffer.clear();
    ENCODE_BUFFER.with(|buf| {
        let mut borrowed = buf.borrow_mut();
        // Keep the larger buffer for better reuse
        if buffer.capacity() > borrowed.capacity() {
            *borrowed = buffer;
        }
    });
}

/// Network error types for transport
#[derive(Debug, thiserror::Error)]
pub enum ManiacTransportError {
    #[error("Connection failed: {0}")]
    ConnectionError(Cow<'static, str>),

    #[error("Serialization error: {0}")]
    SerializationError(Cow<'static, str>),

    #[error("Network error: {0}")]
    NetworkError(Cow<'static, str>),

    #[error("Invalid response")]
    InvalidResponse,

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Unknown node: {0}")]
    UnknownNode(u64),

    #[error("Channel error: {0}")]
    ChannelError(Cow<'static, str>),

    #[error("Io error: {0}")]
    IoError(#[from] io::Error),

    #[error("Remote error: {0}")]
    RemoteError(Cow<'static, str>),

    #[error("Codec error: {0}")]
    CodecError(Cow<'static, str>),
}

impl<C: RaftTypeConfig> From<ManiacTransportError> for RPCError<C> {
    fn from(error: ManiacTransportError) -> Self {
        RPCError::Network(openraft::error::NetworkError::new(&error))
    }
}

impl<C: RaftTypeConfig> From<ManiacTransportError>
    for RPCError<C, RaftError<C, InstallSnapshotError>>
{
    fn from(error: ManiacTransportError) -> Self {
        RPCError::Network(openraft::error::NetworkError::new(&error))
    }
}

impl<C: RaftTypeConfig> From<ManiacTransportError> for RPCError<C, RaftError<C>> {
    fn from(error: ManiacTransportError) -> Self {
        RPCError::Network(openraft::error::NetworkError::new(&error))
    }
}

/// Configuration for ManiacTcpTransport
#[derive(Debug, Clone)]
pub struct ManiacTcpTransportConfig {
    /// Connection timeout for establishing new connections
    pub connect_timeout: Duration,
    /// Request timeout for individual RPC calls
    pub request_timeout: Duration,
    /// Number of connections to maintain per peer address for multiplexing.
    /// Higher values allow more concurrent requests but use more resources.
    /// Default: 4
    pub connections_per_addr: usize,
    /// Maximum number of concurrent in-flight requests per connection.
    /// Default: 256
    pub max_concurrent_requests_per_conn: usize,
    /// Connection time-to-live. Connections older than this will be closed
    /// and replaced to prevent TCP connection degradation.
    /// Default: 5 minutes
    pub connection_ttl: Duration,
    /// TCP nodelay (disable Nagle's algorithm)
    pub tcp_nodelay: bool,
}

impl Default for ManiacTcpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            connections_per_addr: 4,
            max_concurrent_requests_per_conn: 256,
            connection_ttl: Duration::from_secs(300), // 5 minutes
            tcp_nodelay: true,
        }
    }
}

/// Frame format: length (u32) + payload
const FRAME_PREFIX_LEN: usize = 4;

/// Helper to read a frame from any AsyncRead stream
pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<Vec<u8>, ManiacTransportError> {
    // Read length prefix
    let mut len_buf = [0u8; FRAME_PREFIX_LEN];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(ManiacTransportError::IoError)?;

    let len = u32::from_le_bytes(len_buf) as usize;

    if len == 0 {
        return Ok(Vec::new());
    }

    // Read payload
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(ManiacTransportError::IoError)?;

    Ok(payload)
}

/// Helper to write a frame to any AsyncWrite stream using sequential writes.
/// Takes ownership of the data to avoid copying into a new buffer.
/// Returns the payload buffer (cleared) so it can be reused by a buffer pool.
pub async fn write_frame_vectored<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut W,
    data: Vec<u8>,
) -> Result<Vec<u8>, ManiacTransportError> {
    let len = data.len() as u32;
    let len_buf = len.to_le_bytes();

    // Write length prefix + payload sequentially
    stream
        .write_all(&len_buf)
        .await
        .map_err(ManiacTransportError::IoError)?;
    stream
        .write_all(&data)
        .await
        .map_err(ManiacTransportError::IoError)?;

    // Return the payload buffer for potential reuse
    let mut payload_buf = data;
    payload_buf.clear(); // Clear for reuse but keep capacity
    Ok(payload_buf)
}

/// Helper to write a frame to any AsyncWrite stream (legacy version for borrowed data)
pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut W,
    data: &[u8],
) -> Result<(), ManiacTransportError> {
    let len = data.len() as u32;
    let mut frame = Vec::with_capacity(4 + data.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(data);

    stream
        .write_all(&frame)
        .await
        .map_err(ManiacTransportError::IoError)?;

    Ok(())
}

/// Type alias for pending request channel
type PendingResponseSender = oneshot::Sender<Result<Vec<u8>, ManiacTransportError>>;

/// Message sent to the connection task for writing
struct WriteRequest {
    request_id: u64,
    data: Vec<u8>,
    response_tx: PendingResponseSender,
}

/// RAII Guard for in-flight request counter to ensure it is decremented
/// even if the future is cancelled/dropped (e.g. due to timeout).
struct InFlightGuard<'a> {
    counter: &'a std::sync::atomic::AtomicU64,
}

impl<'a> InFlightGuard<'a> {
    fn new(counter: &'a std::sync::atomic::AtomicU64) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A single multiplexed connection with its own IO task
struct MultiplexedConnection {
    /// Channel to send write requests to the IO task (using flume for reliability)
    write_tx: flume::Sender<WriteRequest>,
    /// Connection creation time for TTL tracking
    created_at: Instant,
    /// Number of in-flight requests
    in_flight: AtomicU64,
    /// Whether the connection is still alive
    alive: AtomicBool,
    /// Connection ID for logging
    conn_id: u64,
}

impl MultiplexedConnection {
    /// Create a new multiplexed connection and spawn the IO task
    fn new(stream: TcpStream, conn_id: u64) -> Arc<Self> {
        // Channel for write requests - buffer up to 256 pending writes (using flume for reliability)
        let (write_tx, write_rx) = flume::bounded::<WriteRequest>(256);

        let conn = Arc::new(Self {
            write_tx,
            created_at: Instant::now(),
            in_flight: AtomicU64::new(0),
            alive: AtomicBool::new(true),
            conn_id,
        });

        // Spawn IO task that handles both reading and writing
        let conn_clone = conn.clone();
        tokio::spawn(async move {
            conn_clone.io_loop(stream, write_rx).await;
        });

        conn
    }

    /// IO loop that handles both reading responses and writing requests
    /// Uses split stream with separate reader and writer tasks for true concurrency
    async fn io_loop(self: Arc<Self>, stream: TcpStream, write_rx: flume::Receiver<WriteRequest>) {
        // Map of pending requests awaiting responses
        let pending: Arc<DashMap<u64, PendingResponseSender>> = Arc::new(DashMap::new());

        // Split the stream into read and write halves
        let (read_half, write_half) = stream.into_split();

        // Spawn the writer task
        let conn_self = self.clone();
        let pending_clone = pending.clone();
        let _writer_handle = tokio::spawn(async move {
            conn_self
                .writer_loop(write_half, write_rx, pending_clone)
                .await;
        });

        // Run the reader in the current task
        let reader_self = self.clone();
        reader_self.reader_loop(read_half, pending.clone()).await;

        // When reader exits, mark connection as dead (writer will notice via channel closure)
        self.alive.store(false, Ordering::Release);
        self.notify_all_pending_error(&pending);
    }

    /// Writer loop - processes write requests from the channel
    async fn writer_loop(
        self: Arc<Self>,
        mut write_half: tokio::net::tcp::OwnedWriteHalf,
        write_rx: flume::Receiver<WriteRequest>,
        pending: Arc<DashMap<u64, PendingResponseSender>>,
    ) {
        loop {
            // Check if connection is still alive
            if !self.alive.load(Ordering::Acquire) {
                return;
            }

            // Wait for a write request (this blocks properly without busy-wait)
            match write_rx.recv_async().await {
                Ok(write_req) => {
                    // Register the pending request before writing
                    pending.insert(write_req.request_id, write_req.response_tx);

                    // Write the request using sequential writes
                    match write_frame_vectored(&mut write_half, write_req.data).await {
                        Ok(returned_buf) => {
                            // Return the buffer to the thread-local pool for reuse
                            return_encode_buffer(returned_buf);
                        }
                        Err(e) => {
                            tracing::debug!("Connection {} write error: {}", self.conn_id, e);
                            // Remove and notify this request
                            if let Some((_, tx)) = pending.remove(&write_req.request_id) {
                                let _ = tx.send(Err(e));
                            }
                            self.alive.store(false, Ordering::Release);
                            return;
                        }
                    }
                }
                Err(_) => {
                    // Channel closed, connection is shutting down
                    return;
                }
            }
        }
    }

    /// Reader loop - reads responses and dispatches to pending requests
    async fn reader_loop(
        self: Arc<Self>,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        pending: Arc<DashMap<u64, PendingResponseSender>>,
    ) {
        loop {
            // Check if connection is still alive
            if !self.alive.load(Ordering::Acquire) {
                return;
            }

            // Read a response (blocks properly without busy-wait)
            match read_frame(&mut read_half).await {
                Ok(data) => {
                    // Parse request_id from response
                    // Parse request_id from response
                    if data.len() < 9 {
                        tracing::error!("Response too short: {} bytes", data.len());
                        // Close connection on invalid data to prevent busy loop
                        return;
                    }

                    let request_id = u64::from_le_bytes(data[1..9].try_into().unwrap());

                    if let Some((_, sender)) = pending.remove(&request_id) {
                        let _ = sender.send(Ok(data));
                    } else {
                        tracing::warn!(
                            "Received response for unknown request ID: {} (len={}, disc={})",
                            request_id,
                            data.len(),
                            data[0]
                        );
                    }
                }
                Err(e) => {
                    // Check if it's an EOF (connection closed gracefully)
                    if let ManiacTransportError::IoError(ref io_err) = e {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            tracing::debug!("Connection {} closed by peer", self.conn_id);
                        } else {
                            tracing::debug!("Connection {} read error: {}", self.conn_id, e);
                        }
                    }
                    return;
                }
            }
        }
    }

    /// Notify all pending requests of connection error
    fn notify_all_pending_error(&self, pending: &DashMap<u64, PendingResponseSender>) {
        // Drain all entries one by one - avoids collecting all keys into a Vec upfront
        // which could be large under high load
        while let Some(entry) = pending.iter().next().map(|e| *e.key()) {
            if let Some((_, sender)) = pending.remove(&entry) {
                let _ = sender.send(Err(ManiacTransportError::ConnectionClosed));
            }
        }
    }

    /// Check if connection is still alive and not expired
    fn is_usable(&self, ttl: Duration) -> bool {
        self.alive.load(Ordering::Acquire) && self.created_at.elapsed() < ttl
    }

    /// Get current in-flight count
    fn in_flight_count(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Send a request and wait for the response
    async fn send_request(
        &self,
        request_id: u64,
        request_data: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, ManiacTransportError> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(ManiacTransportError::ConnectionClosed);
        }

        // Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // RAII guard for in-flight count
        // This increments on creation and decrements on Drop (even if cancelled)
        let _guard = InFlightGuard::new(&self.in_flight);

        // Send write request to the IO task
        let write_req = WriteRequest {
            request_id,
            data: request_data,
            response_tx,
        };

        if self.write_tx.send_async(write_req).await.is_err() {
            self.alive.store(false, Ordering::Release);
            return Err(ManiacTransportError::ConnectionClosed);
        }

        // Wait for response with timeout
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_recv_err)) => {
                // Sender dropped - connection likely closed
                self.alive.store(false, Ordering::Release);
                Err(ManiacTransportError::ConnectionClosed)
            }
            Err(_timeout) => {
                // Request timed out - mark connection as dead to prevent reuse of stalled connection
                tracing::warn!(
                    "Request {} timed out, marking connection {} as dead",
                    request_id,
                    self.conn_id
                );
                self.alive.store(false, Ordering::Release);
                Err(ManiacTransportError::RequestTimeout)
            }
        }
    }
}

/// Pool of multiplexed TCP connections per peer with TTL management
struct MultiplexedConnectionPool {
    /// Connections per address
    pools: DashMap<SocketAddr, Vec<Arc<MultiplexedConnection>>>,
    /// Target number of connections per peer
    connections_per_addr: usize,
    /// Max concurrent requests per connection
    max_per_conn: usize,
    /// Connection TTL
    connection_ttl: Duration,
    /// Global connection ID counter
    conn_id_counter: AtomicU64,
}

impl MultiplexedConnectionPool {
    fn new(connections_per_addr: usize, max_per_conn: usize, connection_ttl: Duration) -> Self {
        Self {
            pools: DashMap::new(),
            connections_per_addr,
            max_per_conn,
            connection_ttl,
            conn_id_counter: AtomicU64::new(0),
        }
    }

    fn next_conn_id(&self) -> u64 {
        self.conn_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create a connection to the given address.
    /// Returns the connection with the lowest in-flight count that is still usable.
    async fn get_or_create<F, Fut>(
        &self,
        addr: SocketAddr,
        factory: F,
    ) -> Result<Arc<MultiplexedConnection>, ManiacTransportError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<TcpStream, io::Error>>,
    {
        let ttl = self.connection_ttl;
        let max_per_conn = self.max_per_conn as u64;

        // First pass: try to find an existing usable connection
        // We scope this block to ensure the DashMap lock is dropped before any await
        {
            let mut pool = self.pools.entry(addr).or_insert_with(Vec::new);

            // Remove dead or expired connections
            pool.retain(|conn| conn.is_usable(ttl));

            // Find connection with lowest in-flight count in a single pass
            // Track indices to avoid cloning Arc until we know which one we need
            let mut best_idx: Option<usize> = None;
            let mut best_count = u64::MAX;
            let mut fallback_idx: Option<usize> = None;
            let mut fallback_count = u64::MAX;

            for (idx, conn) in pool.iter().enumerate() {
                let count = conn.in_flight_count();
                // Track best connection with capacity
                if count < best_count && count < max_per_conn {
                    best_count = count;
                    best_idx = Some(idx);
                }
                // Also track overall best for fallback
                if count < fallback_count {
                    fallback_count = count;
                    fallback_idx = Some(idx);
                }
            }

            if let Some(idx) = best_idx {
                return Ok(pool[idx].clone());
            }

            // If we are at capacity, reuse the one with lowest load even if full
            if pool.len() >= self.connections_per_addr {
                if let Some(idx) = fallback_idx {
                    return Ok(pool[idx].clone());
                }
            }
        } // Lock dropped here

        // No usable connection and we have capacity to create one.
        // Create new connection without holding the lock
        let stream = factory().await.map_err(|e| {
            ManiacTransportError::ConnectionError(
                format!("Failed to connect to {}: {}", addr, e).into(),
            )
        })?;

        let conn_id = self.next_conn_id();
        let conn = MultiplexedConnection::new(stream, conn_id);

        // Re-acquire lock to insert
        // Note: another task might have inserted a connection in the meantime,
        // but it's safe to add ours too or check limit again.
        {
            let mut pool = self.pools.entry(addr).or_insert_with(Vec::new);
            // Re-check limit to be safe, though slightly over-limit is fine
            if pool.len() < self.connections_per_addr {
                pool.push(conn.clone());
                tracing::debug!(
                    "Created new multiplexed connection {} to {} (pool size: {})",
                    conn_id,
                    addr,
                    pool.len()
                );
                Ok(conn)
            } else {
                // Race condition: someone filled the pool while we were connecting.
                // We can either return our new connection (detached from pool) or use one from pool.
                // Let's use our new connection this time but not add it to the pool to respect limit strictly?
                // Or just add it temporarily.
                // For simplicity/robustness, let's just add it if strictly needed, or just return it.
                // Let's return it but not add to pool if full, effectively making it a one-off?
                // Better: Add it anyway to avoid waste, slightly exceeding soft limit is acceptable.
                pool.push(conn.clone());
                Ok(conn)
            }
        }
    }

    /// Remove a specific connection from the pool
    fn remove_connection(&self, addr: SocketAddr, conn_id: u64) {
        if let Some(mut pool) = self.pools.get_mut(&addr) {
            let before = pool.len();
            pool.retain(|c| c.conn_id != conn_id);
            if pool.len() < before {
                tracing::debug!(
                    "Removed connection {} to {} (pool size: {})",
                    conn_id,
                    addr,
                    pool.len()
                );
            }
        }
    }
}

/// Trait for resolving node IDs to socket addresses
pub trait NodeAddressResolver<NodeId>: Send + Sync + 'static {
    /// Resolve a node ID to a socket address
    fn resolve(&self, node_id: &NodeId) -> Option<SocketAddr>;

    /// Register a node ID with its address
    fn register(&self, node_id: NodeId, addr: SocketAddr);

    /// Unregister a node ID
    fn unregister(&self, node_id: &NodeId);
}

/// Default implementation using DashMap
pub struct DefaultNodeRegistry<NodeId: Eq + std::hash::Hash + Clone + Send + Sync + 'static> {
    nodes: DashMap<NodeId, SocketAddr>,
}

impl<NodeId: Eq + std::hash::Hash + Clone + Send + Sync + 'static> DefaultNodeRegistry<NodeId> {
    pub fn new() -> Self {
        Self {
            nodes: DashMap::new(),
        }
    }
}

impl<NodeId: Eq + std::hash::Hash + Clone + Send + Sync + 'static> Default
    for DefaultNodeRegistry<NodeId>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<NodeId: Eq + std::hash::Hash + Clone + Send + Sync + 'static> NodeAddressResolver<NodeId>
    for DefaultNodeRegistry<NodeId>
{
    fn resolve(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.nodes.get(node_id).map(|r| *r.value())
    }

    fn register(&self, node_id: NodeId, addr: SocketAddr) {
        self.nodes.insert(node_id, addr);
    }

    fn unregister(&self, node_id: &NodeId) {
        self.nodes.remove(node_id);
    }
}

/// TCP transport implementation for Multi-Raft with true connection multiplexing
pub struct ManiacTcpTransport<C: RaftTypeConfig> {
    config: ManiacTcpTransportConfig,
    connection_pool: Arc<MultiplexedConnectionPool>,
    /// Node address resolver
    node_registry: Arc<dyn NodeAddressResolver<C::NodeId>>,
    /// Global request ID counter for correlation
    request_id_counter: AtomicU64,
    _phantom: PhantomData<C>,
}

impl<C> ManiacTcpTransport<C>
where
    C: RaftTypeConfig,
    C::NodeId: Eq + std::hash::Hash + Clone,
{
    /// Create a new ManiacTcpTransport with true multiplexing support
    pub fn new(
        config: ManiacTcpTransportConfig,
        node_registry: Arc<dyn NodeAddressResolver<C::NodeId>>,
    ) -> Self {
        Self {
            connection_pool: Arc::new(MultiplexedConnectionPool::new(
                config.connections_per_addr,
                config.max_concurrent_requests_per_conn,
                config.connection_ttl,
            )),
            config,
            node_registry,
            request_id_counter: AtomicU64::new(0),
            _phantom: PhantomData,
        }
    }

    /// Create a new transport with default configuration and a new registry
    pub fn with_defaults() -> Self
    where
        C::NodeId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    {
        Self::new(
            ManiacTcpTransportConfig::default(),
            Arc::new(DefaultNodeRegistry::new()),
        )
    }

    /// Get the node registry for registering node addresses
    pub fn node_registry(&self) -> &Arc<dyn NodeAddressResolver<C::NodeId>> {
        &self.node_registry
    }

    /// Get the next request ID
    fn next_request_id(&self) -> u64 {
        self.request_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Internal RPC call helper with true multiplexing
    async fn rpc_call<D: Encode + Send + 'static>(
        &self,
        target: &SocketAddr,
        request_id: u64,
        request_msg: &RpcMessage<D>,
    ) -> Result<Vec<u8>, ManiacTransportError> {
        let pool = self.connection_pool.clone();
        let addr = *target;
        let tcp_nodelay = self.config.tcp_nodelay;

        tracing::debug!("rpc_call: target={}, request_id={}", addr, request_id);

        // Serialize request using thread-local buffer pool to reduce allocations.
        // We encode into the pooled buffer, clone the data (fast memcpy), then return the buffer.
        // The buffer keeps its capacity for reuse, avoiding repeated Vec growth.
        let request_data = ENCODE_BUFFER.with(|buf| {
            let mut borrowed = buf.borrow_mut();
            borrowed.clear();
            request_msg
                .encode_into(&mut borrowed)
                .map_err(|e| ManiacTransportError::CodecError(e.to_string().into()))?;
            // Clone the data - this is a fast memcpy, the buffer keeps its capacity
            Ok::<_, ManiacTransportError>(borrowed.clone())
        })?;

        tracing::debug!("rpc_call: encoded {} bytes", request_data.len());

        // Get a connection from the pool
        tracing::debug!("rpc_call: getting connection from pool");
        let conn = pool
            .get_or_create(addr, || async move {
                tracing::debug!("rpc_call: creating new TCP connection to {}", addr);
                let stream = TcpStream::connect(addr).await?;
                stream.set_nodelay(tcp_nodelay)?;
                Ok(stream)
            })
            .await?;

        tracing::debug!("rpc_call: got connection conn_id={}", conn.conn_id);

        // Send request and wait for response
        let result = conn
            .send_request(request_id, request_data, self.config.request_timeout)
            .await;

        tracing::debug!(
            "rpc_call: send_request result={:?}",
            result
                .as_ref()
                .map(|v| v.len())
                .map_err(|e| format!("{}", e))
        );

        // If the connection failed, remove it from the pool
        if result.is_err() && !conn.alive.load(Ordering::Acquire) {
            pool.remove_connection(addr, conn.conn_id);
        }

        result
    }
}

impl<C> MultiplexedTransport<C> for ManiacTcpTransport<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            Vote = openraft::impls::Vote<C>,
            Node = openraft::impls::BasicNode,
            Entry = openraft::impls::Entry<C>,
        >,
    C::SnapshotData: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
    C::Entry: Clone,
    C::D: crate::multi::codec::ToCodec<crate::multi::codec::RawBytes>
        + crate::multi::codec::FromCodec<crate::multi::codec::RawBytes>,
{
    async fn send_append_entries(
        &self,
        target: C::NodeId,
        group_id: u64,
        rpc: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C, RaftError<C>>> {
        use crate::multi::codec::{
            AppendEntriesRequest as CodecAppendEntriesRequest, Entry as CodecEntry, FromCodec,
            RawBytes, ToCodec,
        };

        let addr = self
            .node_registry
            .resolve(&target)
            .ok_or_else(|| ManiacTransportError::UnknownNode(target))?;
        let request_id = self.next_request_id();

        // Convert entries using ToCodec trait - pre-allocate capacity
        let mut codec_entries: Vec<CodecEntry<RawBytes>> = Vec::with_capacity(rpc.entries.len());
        for entry in rpc.entries.iter() {
            codec_entries.push(entry.to_codec());
        }

        let codec_rpc = CodecAppendEntriesRequest {
            vote: rpc.vote.to_codec(),
            prev_log_id: rpc.prev_log_id.as_ref().map(|lid| lid.to_codec()),
            entries: codec_entries,
            leader_commit: rpc.leader_commit.as_ref().map(|lid| lid.to_codec()),
        };

        let request = RpcMessage::AppendEntries {
            request_id,
            group_id,
            rpc: codec_rpc,
        };

        let response_data = self
            .rpc_call(&addr, request_id, &request)
            .await
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        // Deserialize response
        let response: RpcMessage<RawBytes> = RpcMessage::decode_from_slice(&response_data)
            .map_err(|e| ManiacTransportError::CodecError(e.to_string().into()))
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        match response {
            RpcMessage::Response {
                message: ResponseMessage::AppendEntries(resp),
                ..
            } => {
                use crate::multi::codec::AppendEntriesResponse as CodecResp;
                match resp {
                    CodecResp::Success => Ok(AppendEntriesResponse::Success),
                    CodecResp::PartialSuccess(log_id) => {
                        let lid = log_id.map(|l| openraft::LogId::<C>::from_codec(l));
                        Ok(AppendEntriesResponse::PartialSuccess(lid))
                    }
                    CodecResp::Conflict => Ok(AppendEntriesResponse::Conflict),
                    CodecResp::HigherVote(v) => {
                        let vote = openraft::impls::Vote::<C>::from_codec(v);
                        Ok(AppendEntriesResponse::HigherVote(vote))
                    }
                }
            }
            RpcMessage::Error { error, .. } => {
                Err(ManiacTransportError::RemoteError(error.into()).into())
            }
            _ => Err(ManiacTransportError::InvalidResponse.into()),
        }
    }

    async fn send_vote(
        &self,
        target: C::NodeId,
        group_id: u64,
        rpc: VoteRequest<C>,
    ) -> Result<VoteResponse<C>, RPCError<C, RaftError<C>>> {
        use crate::multi::codec::{FromCodec, RawBytes, ToCodec, VoteRequest as CodecVoteRequest};

        tracing::debug!("send_vote: target={}, group_id={}", target, group_id);

        let addr = self.node_registry.resolve(&target).ok_or_else(|| {
            tracing::error!("send_vote: Unknown node: {}", target);
            ManiacTransportError::UnknownNode(target)
        })?;

        tracing::debug!("send_vote: resolved addr={}", addr);
        let request_id = self.next_request_id();

        let codec_rpc = CodecVoteRequest {
            vote: rpc.vote.to_codec(),
            last_log_id: rpc.last_log_id.as_ref().map(|lid| lid.to_codec()),
        };

        let request: RpcMessage<RawBytes> = RpcMessage::Vote {
            request_id,
            group_id,
            rpc: codec_rpc,
        };

        let response_data = self
            .rpc_call(&addr, request_id, &request)
            .await
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        // Deserialize response
        let response: RpcMessage<RawBytes> = RpcMessage::decode_from_slice(&response_data)
            .map_err(|e| ManiacTransportError::CodecError(e.to_string().into()))
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        match response {
            RpcMessage::Response {
                message: ResponseMessage::Vote(resp),
                ..
            } => Ok(VoteResponse {
                vote: openraft::impls::Vote::<C>::from_codec(resp.vote),
                vote_granted: resp.vote_granted,
                last_log_id: resp
                    .last_log_id
                    .map(|l| openraft::LogId::<C>::from_codec(l)),
            }),
            RpcMessage::Error { error, .. } => {
                Err(ManiacTransportError::RemoteError(error.into()).into())
            }
            _ => Err(ManiacTransportError::InvalidResponse.into()),
        }
    }

    async fn send_install_snapshot(
        &self,
        target: C::NodeId,
        group_id: u64,
        rpc: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse<C>, RPCError<C, RaftError<C, InstallSnapshotError>>> {
        use crate::multi::codec::{
            FromCodec, InstallSnapshotRequest as CodecInstallSnapshotRequest, RawBytes, ToCodec,
        };

        let addr = self
            .node_registry
            .resolve(&target)
            .ok_or_else(|| ManiacTransportError::UnknownNode(target))?;
        let request_id = self.next_request_id();

        let codec_rpc = CodecInstallSnapshotRequest {
            vote: rpc.vote.to_codec(),
            meta: rpc.meta.to_codec(),
            offset: rpc.offset,
            data: RawBytes(rpc.data.clone()),
            done: rpc.done,
        };

        let request: RpcMessage<RawBytes> = RpcMessage::InstallSnapshot {
            request_id,
            group_id,
            rpc: codec_rpc,
        };

        let response_data = self
            .rpc_call(&addr, request_id, &request)
            .await
            .map_err(RPCError::<C, RaftError<C, InstallSnapshotError>>::from)?;

        // Deserialize response
        let response: RpcMessage<RawBytes> = RpcMessage::decode_from_slice(&response_data)
            .map_err(|e| ManiacTransportError::CodecError(e.to_string().into()))
            .map_err(RPCError::<C, RaftError<C, InstallSnapshotError>>::from)?;

        match response {
            RpcMessage::Response {
                message: ResponseMessage::InstallSnapshot(resp),
                ..
            } => Ok(InstallSnapshotResponse {
                vote: openraft::impls::Vote::<C>::from_codec(resp.vote),
            }),
            RpcMessage::Error { error, .. } => {
                Err(ManiacTransportError::RemoteError(error.into()).into())
            }
            _ => Err(ManiacTransportError::InvalidResponse.into()),
        }
    }

    async fn send_heartbeat_batch(
        &self,
        target: C::NodeId,
        batch: &[(u64, AppendEntriesRequest<C>)],
    ) -> Result<Vec<(u64, AppendEntriesResponse<C>)>, RPCError<C, RaftError<C>>> {
        use crate::multi::codec::{
            AppendEntriesRequest as CodecAppendEntriesRequest, AppendEntriesResponse as CodecResp,
            Entry as CodecEntry, FromCodec, RawBytes, RpcMessage, ToCodec,
        };

        let addr = self
            .node_registry
            .resolve(&target)
            .ok_or_else(|| ManiacTransportError::UnknownNode(target))?;

        let request_id = self.next_request_id();

        // Build codec heartbeats: one per group_id (already deduped by the buffer).
        let mut heartbeats: Vec<(u64, CodecAppendEntriesRequest<RawBytes>)> =
            Vec::with_capacity(batch.len());

        for (gid, rpc) in batch {
            // Pre-allocate entry conversion vector
            let mut codec_entries: Vec<CodecEntry<RawBytes>> =
                Vec::with_capacity(rpc.entries.len());
            for entry in rpc.entries.iter() {
                codec_entries.push(entry.to_codec());
            }

            let codec_rpc = CodecAppendEntriesRequest {
                vote: rpc.vote.to_codec(),
                prev_log_id: rpc.prev_log_id.as_ref().map(|lid| lid.to_codec()),
                entries: codec_entries,
                leader_commit: rpc.leader_commit.as_ref().map(|lid| lid.to_codec()),
            };

            heartbeats.push((*gid, codec_rpc));
        }

        let request = RpcMessage::HeartbeatBatchMulti {
            request_id,
            heartbeats,
        };

        let response_data = self
            .rpc_call(&addr, request_id, &request)
            .await
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        let response: RpcMessage<RawBytes> = RpcMessage::decode_from_slice(&response_data)
            .map_err(|e| ManiacTransportError::CodecError(e.to_string().into()))
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        let convert_resp = |resp: CodecResp| -> AppendEntriesResponse<C> {
            match resp {
                CodecResp::Success => AppendEntriesResponse::Success,
                CodecResp::PartialSuccess(log_id) => {
                    let lid = log_id.map(|l| openraft::LogId::<C>::from_codec(l));
                    AppendEntriesResponse::PartialSuccess(lid)
                }
                CodecResp::Conflict => AppendEntriesResponse::Conflict,
                CodecResp::HigherVote(v) => {
                    let vote = openraft::impls::Vote::<C>::from_codec(v);
                    AppendEntriesResponse::HigherVote(vote)
                }
            }
        };

        match response {
            RpcMessage::HeartbeatBatchMultiResponse { responses, .. } => Ok(responses
                .into_iter()
                .map(|(gid, resp)| (gid, convert_resp(resp)))
                .collect()),
            RpcMessage::Error { error, .. } => {
                Err(ManiacTransportError::RemoteError(error.into()).into())
            }
            _ => Err(ManiacTransportError::InvalidResponse.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi::test_support::run_async;

    #[test]
    fn test_transport_config_default() {
        let config = ManiacTcpTransportConfig::default();
        assert_eq!(config.connections_per_addr, 4);
        assert_eq!(config.max_concurrent_requests_per_conn, 256);
        assert_eq!(config.connection_ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_read_write_frame_roundtrip() {
        run_async(async {
            let data = b"hello, world!";
            let mut buffer = Vec::new();
            let mut cursor = std::io::Cursor::new(&mut buffer);

            // Write frame
            write_frame(&mut cursor, data).await.unwrap();

            // Reset cursor to read
            cursor.set_position(0);

            // Read frame
            let read_data = read_frame(&mut cursor).await.unwrap();
            assert_eq!(read_data, data);
        });
    }

    #[test]
    fn test_read_frame_empty() {
        run_async(async {
            let mut buffer = vec![0u8; 4]; // length = 0
            let mut cursor = std::io::Cursor::new(&mut buffer);

            let result = read_frame(&mut cursor).await.unwrap();
            assert_eq!(result, Vec::<u8>::new());
        });
    }

    #[test]
    fn test_read_frame_truncated_length() {
        run_async(async {
            let mut buffer = vec![0u8; 2]; // Incomplete length prefix
            let mut cursor = std::io::Cursor::new(&mut buffer);

            let result = read_frame(&mut cursor).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_read_frame_truncated_payload() {
        run_async(async {
            let mut buffer = Vec::new();
            buffer.extend_from_slice(&10u32.to_le_bytes()); // length = 10
            buffer.extend_from_slice(b"abc"); // Only 3 bytes instead of 10
            let mut cursor = std::io::Cursor::new(&mut buffer);

            let result = read_frame(&mut cursor).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_write_frame_large_data() {
        run_async(async {
            let data = vec![0x42u8; 10000];
            let mut buffer = Vec::new();
            let mut cursor = std::io::Cursor::new(&mut buffer);

            write_frame(&mut cursor, &data).await.unwrap();

            cursor.set_position(0);
            let read_data = read_frame(&mut cursor).await.unwrap();
            assert_eq!(read_data, data);
        });
    }

    #[test]
    fn test_multiple_frames() {
        run_async(async {
            let data1 = b"frame1";
            let data2 = b"frame2";
            let mut buffer = Vec::new();
            let mut cursor = std::io::Cursor::new(&mut buffer);

            write_frame(&mut cursor, data1).await.unwrap();
            write_frame(&mut cursor, data2).await.unwrap();

            cursor.set_position(0);
            let read1 = read_frame(&mut cursor).await.unwrap();
            assert_eq!(read1, data1);

            let read2 = read_frame(&mut cursor).await.unwrap();
            assert_eq!(read2, data2);
        });
    }
}
