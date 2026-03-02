//! Group-Pinned TCP Transport for Multi-Raft
//!
//! Each raft group is pinned to a single logical TCP connection per target host.
//! A crossfire queue per connection enables natural write batching: the writer task
//! blocks for the first message, then drains all immediately available messages
//! into a single `write_all` syscall (up to a configurable batch size).
//!
//! Connections have a TTL and are transparently refreshed without dropping
//! in-flight requests — old and new readers share the same pending-response map.

use crate::multi::codec::{Decode, Encode, ResponseMessage, RpcMessage};
use crate::multi::network::MultiplexedTransport;
use bytes::{Buf, Bytes, BytesMut};
use dashmap::DashMap;
use openraft::RaftTypeConfig;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

/// Boxed async reader half — either OwnedReadHalf (TCP) or ReadHalf<TlsStream>
pub type BoxedReader = Box<dyn tokio::io::AsyncRead + Unpin + Send>;
/// Boxed async writer half
pub type BoxedWriter = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;

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
pub enum BisqueTransportError {
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

impl<C: RaftTypeConfig> From<BisqueTransportError> for RPCError<C> {
    fn from(error: BisqueTransportError) -> Self {
        RPCError::Network(openraft::error::NetworkError::new(&error))
    }
}

impl<C: RaftTypeConfig> From<BisqueTransportError>
    for RPCError<C, RaftError<C, InstallSnapshotError>>
{
    fn from(error: BisqueTransportError) -> Self {
        RPCError::Network(openraft::error::NetworkError::new(&error))
    }
}

impl<C: RaftTypeConfig> From<BisqueTransportError> for RPCError<C, RaftError<C>> {
    fn from(error: BisqueTransportError) -> Self {
        RPCError::Network(openraft::error::NetworkError::new(&error))
    }
}

/// Configuration for BisqueTcpTransport
#[derive(Debug, Clone)]
pub struct BisqueTcpTransportConfig {
    /// Connection timeout for establishing new connections
    pub connect_timeout: Duration,
    /// Request timeout for individual RPC calls
    pub request_timeout: Duration,
    /// Capacity of the crossfire write channel per connection
    pub write_channel_capacity: usize,
    /// Maximum bytes to accumulate before issuing write_all.
    /// The writer drains the queue until this limit or the queue is empty.
    pub max_batch_bytes: usize,
    /// Connection time-to-live. Connections older than this are refreshed.
    pub connection_ttl: Duration,
    /// TCP nodelay (disable Nagle's algorithm)
    pub tcp_nodelay: bool,
    /// TLS client configuration for outgoing connections.
    #[cfg(feature = "tls")]
    pub tls_client_config: Option<Arc<rustls::ClientConfig>>,
    /// Server name for TLS SNI verification on outgoing connections.
    #[cfg(feature = "tls")]
    pub tls_server_name: Option<rustls::pki_types::ServerName<'static>>,
}

impl Default for BisqueTcpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            write_channel_capacity: 256,
            max_batch_bytes: 64 * 1024, // 64KB
            connection_ttl: Duration::from_secs(300), // 5 minutes
            tcp_nodelay: true,
            #[cfg(feature = "tls")]
            tls_client_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        }
    }
}

/// Frame format: length (u32) + payload
pub(crate) const FRAME_PREFIX_LEN: usize = 4;

/// Helper to read a frame from any AsyncRead stream
pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<Vec<u8>, BisqueTransportError> {
    // Read length prefix
    let mut len_buf = [0u8; FRAME_PREFIX_LEN];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(BisqueTransportError::IoError)?;

    let len = u32::from_le_bytes(len_buf) as usize;

    if len == 0 {
        return Ok(Vec::new());
    }

    // Read payload
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(BisqueTransportError::IoError)?;

    Ok(payload)
}

/// Helper to write a frame to any AsyncWrite stream using sequential writes.
/// Takes ownership of the data to avoid copying into a new buffer.
/// Returns the payload buffer (cleared) so it can be reused by a buffer pool.
pub async fn write_frame_vectored<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut W,
    data: Vec<u8>,
) -> Result<Vec<u8>, BisqueTransportError> {
    let len = data.len() as u32;
    let len_buf = len.to_le_bytes();

    // Write length prefix + payload sequentially
    stream
        .write_all(&len_buf)
        .await
        .map_err(BisqueTransportError::IoError)?;
    stream
        .write_all(&data)
        .await
        .map_err(BisqueTransportError::IoError)?;

    // Return the payload buffer for potential reuse
    let mut payload_buf = data;
    payload_buf.clear(); // Clear for reuse but keep capacity
    Ok(payload_buf)
}

/// Helper to write a frame to any AsyncWrite stream (for borrowed data).
/// Uses two write_all calls to avoid allocating a combined buffer.
pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut W,
    data: &[u8],
) -> Result<(), BisqueTransportError> {
    let len = data.len() as u32;
    let len_buf = len.to_le_bytes();
    stream
        .write_all(&len_buf)
        .await
        .map_err(BisqueTransportError::IoError)?;
    stream
        .write_all(data)
        .await
        .map_err(BisqueTransportError::IoError)?;
    Ok(())
}

/// Read a frame into a reusable buffer, avoiding per-frame allocation.
/// Returns the number of payload bytes read. The buffer is resized to fit
/// the payload; if capacity is already sufficient, no allocation occurs.
pub async fn read_frame_into<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
    buf: &mut Vec<u8>,
) -> Result<usize, BisqueTransportError> {
    let mut len_buf = [0u8; FRAME_PREFIX_LEN];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(BisqueTransportError::IoError)?;

    let len = u32::from_le_bytes(len_buf) as usize;
    if len == 0 {
        buf.clear();
        return Ok(0);
    }

    // Grow the buffer if needed. If capacity >= len this is just a length update.
    // The zero-fill from resize is overwritten by read_exact immediately.
    buf.resize(len, 0);
    stream
        .read_exact(&mut buf[..len])
        .await
        .map_err(BisqueTransportError::IoError)?;

    Ok(len)
}

/// Write a buffer that already contains the frame header prepended to the payload.
/// This is a single write_all call — no header/payload split, no extra syscall.
/// Returns the cleared buffer for pool reuse.
pub async fn write_preframed<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut W,
    data: Vec<u8>,
) -> Result<Vec<u8>, BisqueTransportError> {
    stream
        .write_all(&data)
        .await
        .map_err(BisqueTransportError::IoError)?;
    let mut buf = data;
    buf.clear();
    Ok(buf)
}

/// Encode a message into the thread-local buffer with a frame length header prepended.
/// Uses `mem::take` to move the buffer out of TLS without copying.
/// Returns an owned buffer containing `[4-byte LE length | payload]`.
/// Caller should pass the buffer to `write_preframed` and then `return_encode_buffer`.
pub fn encode_framed<E: Encode>(msg: &E) -> Result<Vec<u8>, BisqueTransportError> {
    ENCODE_BUFFER.with(|buf| {
        let mut borrowed = buf.borrow_mut();
        borrowed.clear();
        // Reserve space for the 4-byte frame length prefix
        borrowed.extend_from_slice(&[0u8; FRAME_PREFIX_LEN]);
        // Encode the message after the prefix
        msg.encode(&mut *borrowed)
            .map_err(|e| BisqueTransportError::CodecError(e.to_string().into()))?;
        // Patch the length prefix (payload length = total - header)
        let payload_len = (borrowed.len() - FRAME_PREFIX_LEN) as u32;
        borrowed[..FRAME_PREFIX_LEN].copy_from_slice(&payload_len.to_le_bytes());
        // Move the buffer out of TLS (zero-copy). TLS gets an empty Vec.
        Ok(std::mem::take(&mut *borrowed))
    })
}

/// Encode a message with a frame length header, appending to an existing buffer.
///
/// Unlike `encode_framed`, this does NOT use thread-local storage and does NOT
/// allocate. The caller provides a reusable buffer. Multiple messages can be
/// appended for batch writing (each gets its own length prefix).
///
/// After warmup the buffer is large enough to hold any response — zero alloc.
pub fn encode_framed_append<E: Encode>(msg: &E, buf: &mut Vec<u8>) -> Result<(), BisqueTransportError> {
    let start = buf.len();
    // Reserve space for the 4-byte frame length prefix
    buf.extend_from_slice(&[0u8; FRAME_PREFIX_LEN]);
    // Encode the message after the prefix
    msg.encode(buf)
        .map_err(|e| BisqueTransportError::CodecError(e.to_string().into()))?;
    // Patch the length prefix (payload length = total - header)
    let payload_len = (buf.len() - start - FRAME_PREFIX_LEN) as u32;
    buf[start..start + FRAME_PREFIX_LEN].copy_from_slice(&payload_len.to_le_bytes());
    Ok(())
}

// ---------------------------------------------------------------------------
// Group-pinned connection internals
// ---------------------------------------------------------------------------

/// Type alias for pending request channel
type PendingResponseSender = oneshot::Sender<Result<Bytes, BisqueTransportError>>;

/// Message sent to the writer task for batched writing
struct WriteRequest {
    request_id: u64,
    /// Pre-encoded frame: [4-byte LE length | payload]
    data: Vec<u8>,
    response_tx: PendingResponseSender,
}

/// Key for the per-(target, group) connection map
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupConnectionKey {
    target_addr: SocketAddr,
    group_id: u64,
}

/// A single logical connection for a (target, group_id) pair.
///
/// The `pending` map is shared across connection refreshes so that in-flight
/// requests on the old connection still receive their responses.
struct GroupConnection {
    /// Channel to send write requests to the writer task
    write_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<WriteRequest>>,
    /// Shared pending-response map (survives connection refresh)
    pending: Arc<std::sync::Mutex<HashMap<u64, PendingResponseSender>>>,
    /// Whether this connection is alive
    alive: Arc<AtomicBool>,
    /// When this connection was created
    created_at: Instant,
    /// Monotonic generation for refresh ordering
    generation: u64,
}

impl GroupConnection {
    /// Create a new connection, spawning writer and reader tasks.
    fn new(
        reader: BoxedReader,
        writer: BoxedWriter,
        pending: Arc<std::sync::Mutex<HashMap<u64, PendingResponseSender>>>,
        generation: u64,
        max_batch_bytes: usize,
        channel_capacity: usize,
    ) -> Self {
        let (write_tx, write_rx) =
            crossfire::mpsc::bounded_async::<WriteRequest>(channel_capacity);
        let alive = Arc::new(AtomicBool::new(true));

        // Spawn writer task
        let writer_alive = alive.clone();
        let writer_pending = pending.clone();
        tokio::spawn(async move {
            Self::writer_loop(writer, write_rx, writer_pending, writer_alive, max_batch_bytes)
                .await;
        });

        // Spawn reader task
        let reader_alive = alive.clone();
        let reader_pending = pending.clone();
        tokio::spawn(async move {
            Self::reader_loop(reader, reader_pending, reader_alive).await;
        });

        Self {
            write_tx,
            pending,
            alive,
            created_at: Instant::now(),
            generation,
        }
    }

    /// Writer loop: block on first message, drain queue, batch write.
    async fn writer_loop(
        mut write_half: BoxedWriter,
        write_rx: crossfire::AsyncRx<crossfire::mpsc::Array<WriteRequest>>,
        pending: Arc<std::sync::Mutex<HashMap<u64, PendingResponseSender>>>,
        alive: Arc<AtomicBool>,
        max_batch_bytes: usize,
    ) {
        use crossfire::TryRecvError;

        let mut encode_buf: Vec<u8> = Vec::with_capacity(max_batch_bytes);

        loop {
            if !alive.load(Ordering::Acquire) {
                return;
            }

            // 1. Block on first write request
            let first = match write_rx.recv().await {
                Ok(req) => req,
                Err(_) => return, // channel closed
            };

            // 2. Register pending and copy pre-encoded frame into batch buffer
            {
                let mut map = pending.lock().unwrap();
                map.insert(first.request_id, first.response_tx);
            }
            encode_buf.clear();
            encode_buf.extend_from_slice(&first.data);
            return_encode_buffer(first.data);

            // 3. Drain immediately available requests up to max_batch_bytes
            loop {
                if encode_buf.len() >= max_batch_bytes {
                    break;
                }
                match write_rx.try_recv() {
                    Ok(req) => {
                        {
                            let mut map = pending.lock().unwrap();
                            map.insert(req.request_id, req.response_tx);
                        }
                        encode_buf.extend_from_slice(&req.data);
                        return_encode_buffer(req.data);
                    }
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                }
            }

            // 4. Single write_all + flush
            if let Err(e) = write_half.write_all(&encode_buf).await {
                tracing::error!("writer: write error: {}", e);
                alive.store(false, Ordering::Release);
                Self::notify_all_pending_error(&pending);
                return;
            }
            if let Err(e) = write_half.flush().await {
                tracing::error!("writer: flush error: {}", e);
                alive.store(false, Ordering::Release);
                Self::notify_all_pending_error(&pending);
                return;
            }
        }
    }

    /// Reader loop: BytesMut rolling buffer, zero-copy frame extraction,
    /// dispatch by request_id to the shared pending map.
    async fn reader_loop(
        mut read_half: BoxedReader,
        pending: Arc<std::sync::Mutex<HashMap<u64, PendingResponseSender>>>,
        alive: Arc<AtomicBool>,
    ) {
        let mut buf = BytesMut::with_capacity(64 * 1024);

        loop {
            if !alive.load(Ordering::Acquire) {
                return;
            }

            // Process all complete frames in the buffer
            loop {
                if buf.len() < FRAME_PREFIX_LEN {
                    break;
                }
                let payload_len =
                    u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;

                if payload_len == 0 {
                    buf.advance(FRAME_PREFIX_LEN);
                    continue;
                }

                if buf.len() < FRAME_PREFIX_LEN + payload_len {
                    break; // incomplete frame
                }

                // Extract frame zero-copy
                buf.advance(FRAME_PREFIX_LEN);
                let frame: Bytes = buf.split_to(payload_len).freeze();

                if frame.len() < 9 {
                    tracing::error!("reader: response too short: {} bytes", frame.len());
                    alive.store(false, Ordering::Release);
                    Self::notify_all_pending_error(&pending);
                    return;
                }

                let request_id = u64::from_le_bytes(frame[1..9].try_into().unwrap());

                let sender = {
                    let mut map = pending.lock().unwrap();
                    map.remove(&request_id)
                };
                if let Some(sender) = sender {
                    let _ = sender.send(Ok(frame));
                } else {
                    tracing::warn!(
                        "reader: response for unknown request_id: {} (len={}, disc={})",
                        request_id,
                        payload_len,
                        frame[0]
                    );
                }
            }

            // Compact/reserve if running low
            if buf.capacity() - buf.len() < 4096 {
                buf.reserve(64 * 1024);
            }

            // Read more data
            match read_half.read_buf(&mut buf).await {
                Ok(0) => {
                    tracing::trace!("reader: connection closed by peer");
                    alive.store(false, Ordering::Release);
                    Self::notify_all_pending_error(&pending);
                    return;
                }
                Ok(_) => {} // loop back to parse
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        tracing::trace!("reader: connection closed by peer");
                    } else {
                        tracing::error!("reader: read error: {}", e);
                    }
                    alive.store(false, Ordering::Release);
                    Self::notify_all_pending_error(&pending);
                    return;
                }
            }
        }
    }

    /// Notify all pending requests of connection error.
    fn notify_all_pending_error(
        pending: &std::sync::Mutex<HashMap<u64, PendingResponseSender>>,
    ) {
        let drained: Vec<(u64, PendingResponseSender)> = {
            let mut map = pending.lock().unwrap();
            map.drain().collect()
        };
        for (_, sender) in drained {
            let _ = sender.send(Err(BisqueTransportError::ConnectionClosed));
        }
    }

    /// Check if connection is alive and within TTL
    fn is_usable(&self, ttl: Duration) -> bool {
        self.alive.load(Ordering::Acquire) && self.created_at.elapsed() < ttl
    }

    /// Send a pre-encoded request and await the response
    async fn send_request(
        &self,
        request_id: u64,
        request_data: Vec<u8>,
        timeout: Duration,
    ) -> Result<Bytes, BisqueTransportError> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(BisqueTransportError::ConnectionClosed);
        }

        let (response_tx, response_rx) = oneshot::channel();

        let write_req = WriteRequest {
            request_id,
            data: request_data,
            response_tx,
        };

        if self.write_tx.send(write_req).await.is_err() {
            self.alive.store(false, Ordering::Release);
            return Err(BisqueTransportError::ConnectionClosed);
        }

        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.alive.store(false, Ordering::Release);
                Err(BisqueTransportError::ConnectionClosed)
            }
            Err(_) => {
                tracing::warn!("request {} timed out", request_id);
                self.alive.store(false, Ordering::Release);
                Err(BisqueTransportError::RequestTimeout)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Node address resolution
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// BisqueTcpTransport — group-pinned TCP transport
// ---------------------------------------------------------------------------

/// Group-pinned TCP transport for Multi-Raft.
///
/// Each (target_host, group_id) pair gets its own dedicated TCP connection with a
/// crossfire write queue that enables natural batching of frames into a single syscall.
pub struct BisqueTcpTransport<C: RaftTypeConfig> {
    config: BisqueTcpTransportConfig,
    connections: DashMap<GroupConnectionKey, Arc<GroupConnection>>,
    /// Per-key creation locks to prevent concurrent connection creation races
    creation_locks: DashMap<GroupConnectionKey, Arc<tokio::sync::Mutex<()>>>,
    node_registry: Arc<dyn NodeAddressResolver<C::NodeId>>,
    request_id_counter: AtomicU64,
    generation_counter: AtomicU64,
    _phantom: PhantomData<C>,
}

impl<C> BisqueTcpTransport<C>
where
    C: RaftTypeConfig,
    C::NodeId: Eq + std::hash::Hash + Clone,
{
    /// Create a new BisqueTcpTransport
    pub fn new(
        config: BisqueTcpTransportConfig,
        node_registry: Arc<dyn NodeAddressResolver<C::NodeId>>,
    ) -> Self {
        Self {
            config,
            connections: DashMap::new(),
            creation_locks: DashMap::new(),
            node_registry,
            request_id_counter: AtomicU64::new(0),
            generation_counter: AtomicU64::new(0),
            _phantom: PhantomData,
        }
    }

    /// Create with default config and a new registry
    pub fn with_defaults() -> Self
    where
        C::NodeId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    {
        Self::new(
            BisqueTcpTransportConfig::default(),
            Arc::new(DefaultNodeRegistry::new()),
        )
    }

    /// Get the node registry
    pub fn node_registry(&self) -> &Arc<dyn NodeAddressResolver<C::NodeId>> {
        &self.node_registry
    }

    fn next_request_id(&self) -> u64 {
        self.request_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn next_generation(&self) -> u64 {
        self.generation_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create a connection for the given (addr, group_id) pair.
    /// Transparently refreshes expired connections.
    /// Uses a per-key creation lock to prevent concurrent creation races.
    async fn get_or_create_connection(
        &self,
        addr: SocketAddr,
        group_id: u64,
    ) -> Result<Arc<GroupConnection>, BisqueTransportError> {
        let key = GroupConnectionKey {
            target_addr: addr,
            group_id,
        };

        // Fast path: check existing connection (no lock needed)
        if let Some(conn) = self.connections.get(&key) {
            if conn.is_usable(self.config.connection_ttl) {
                return Ok(conn.clone());
            }
        }

        // Slow path: acquire per-key creation lock to serialize connection creation
        let lock = self
            .creation_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Re-check after acquiring lock — another task may have created it
        if let Some(conn) = self.connections.get(&key) {
            if conn.is_usable(self.config.connection_ttl) {
                return Ok(conn.clone());
            }
        }

        // Create new TCP connection (no DashMap lock held)
        let (reader, writer) = self.connect(addr).await?;

        let generation = self.next_generation();

        // Share pending map from old connection if one exists (for refresh)
        let pending = if let Some(old_conn) = self.connections.get(&key) {
            old_conn.pending.clone()
        } else {
            Arc::new(std::sync::Mutex::new(HashMap::new()))
        };

        let conn = Arc::new(GroupConnection::new(
            reader,
            writer,
            pending,
            generation,
            self.config.max_batch_bytes,
            self.config.write_channel_capacity,
        ));

        self.connections.insert(key, conn.clone());

        Ok(conn)
    }

    /// Establish a TCP connection (with optional TLS)
    async fn connect(&self, addr: SocketAddr) -> Result<(BoxedReader, BoxedWriter), BisqueTransportError> {
        let stream = tokio::time::timeout(
            self.config.connect_timeout,
            TcpStream::connect(addr),
        )
        .await
        .map_err(|_| {
            BisqueTransportError::ConnectionError(
                format!("Connection to {} timed out", addr).into(),
            )
        })?
        .map_err(|e| {
            BisqueTransportError::ConnectionError(
                format!("Failed to connect to {}: {}", addr, e).into(),
            )
        })?;

        stream.set_nodelay(self.config.tcp_nodelay).map_err(BisqueTransportError::IoError)?;

        #[cfg(feature = "tls")]
        if let Some(ref tls_config) = self.config.tls_client_config {
            let connector = tokio_rustls::TlsConnector::from(tls_config.clone());
            let server_name = self
                .config
                .tls_server_name
                .clone()
                .unwrap_or_else(|| rustls::pki_types::ServerName::IpAddress(addr.ip().into()));
            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|e| {
                    BisqueTransportError::ConnectionError(
                        format!("TLS handshake failed: {}", e).into(),
                    )
                })?;
            let (reader, writer) = tokio::io::split(tls_stream);
            return Ok((
                Box::new(reader) as BoxedReader,
                Box::new(writer) as BoxedWriter,
            ));
        }

        let (read_half, write_half) = stream.into_split();
        Ok((
            Box::new(read_half) as BoxedReader,
            Box::new(write_half) as BoxedWriter,
        ))
    }

    /// Internal RPC call: encode, get connection, send, await response
    async fn rpc_call<D: Encode + Send + 'static>(
        &self,
        addr: &SocketAddr,
        group_id: u64,
        request_id: u64,
        request_msg: &RpcMessage<D>,
    ) -> Result<Bytes, BisqueTransportError> {
        let request_data = encode_framed(request_msg)?;

        tracing::trace!(
            target = %addr,
            group_id,
            request_id,
            bytes = request_data.len(),
            "rpc_call"
        );

        let conn = self.get_or_create_connection(*addr, group_id).await?;

        let result = conn
            .send_request(request_id, request_data, self.config.request_timeout)
            .await;

        // If connection died, remove it so next call creates a fresh one
        if result.is_err() && !conn.alive.load(Ordering::Acquire) {
            let key = GroupConnectionKey {
                target_addr: *addr,
                group_id,
            };
            // Only remove if it's the same generation (avoid removing a fresh replacement)
            self.connections.remove_if(&key, |_, existing| {
                existing.generation == conn.generation
            });
        }

        result
    }
}

impl<C> MultiplexedTransport<C> for BisqueTcpTransport<C>
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
            .ok_or_else(|| BisqueTransportError::UnknownNode(target))?;
        let request_id = self.next_request_id();

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
            .rpc_call(&addr, group_id, request_id, &request)
            .await
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        let response: RpcMessage<RawBytes> = RpcMessage::decode_from_slice(&response_data)
            .map_err(|e| BisqueTransportError::CodecError(e.to_string().into()))
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
                Err(BisqueTransportError::RemoteError(error.into()).into())
            }
            _ => Err(BisqueTransportError::InvalidResponse.into()),
        }
    }

    async fn send_vote(
        &self,
        target: C::NodeId,
        group_id: u64,
        rpc: VoteRequest<C>,
    ) -> Result<VoteResponse<C>, RPCError<C, RaftError<C>>> {
        use crate::multi::codec::{FromCodec, RawBytes, ToCodec, VoteRequest as CodecVoteRequest};

        let addr = self
            .node_registry
            .resolve(&target)
            .ok_or_else(|| BisqueTransportError::UnknownNode(target))?;
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
            .rpc_call(&addr, group_id, request_id, &request)
            .await
            .map_err(RPCError::<C, RaftError<C>>::from)?;

        let response: RpcMessage<RawBytes> = RpcMessage::decode_from_slice(&response_data)
            .map_err(|e| BisqueTransportError::CodecError(e.to_string().into()))
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
                Err(BisqueTransportError::RemoteError(error.into()).into())
            }
            _ => Err(BisqueTransportError::InvalidResponse.into()),
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
            .ok_or_else(|| BisqueTransportError::UnknownNode(target))?;
        let request_id = self.next_request_id();

        let codec_rpc = CodecInstallSnapshotRequest {
            vote: rpc.vote.to_codec(),
            meta: rpc.meta.to_codec(),
            offset: rpc.offset,
            data: RawBytes(rpc.data),
            done: rpc.done,
        };

        let request: RpcMessage<RawBytes> = RpcMessage::InstallSnapshot {
            request_id,
            group_id,
            rpc: codec_rpc,
        };

        let response_data = self
            .rpc_call(&addr, group_id, request_id, &request)
            .await
            .map_err(RPCError::<C, RaftError<C, InstallSnapshotError>>::from)?;

        let response: RpcMessage<RawBytes> = RpcMessage::decode_from_slice(&response_data)
            .map_err(|e| BisqueTransportError::CodecError(e.to_string().into()))
            .map_err(RPCError::<C, RaftError<C, InstallSnapshotError>>::from)?;

        match response {
            RpcMessage::Response {
                message: ResponseMessage::InstallSnapshot(resp),
                ..
            } => Ok(InstallSnapshotResponse {
                vote: openraft::impls::Vote::<C>::from_codec(resp.vote),
            }),
            RpcMessage::Error { error, .. } => {
                Err(BisqueTransportError::RemoteError(error.into()).into())
            }
            _ => Err(BisqueTransportError::InvalidResponse.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi::test_support::run_async;

    #[test]
    fn test_transport_config_default() {
        let config = BisqueTcpTransportConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.write_channel_capacity, 256);
        assert_eq!(config.max_batch_bytes, 64 * 1024);
        assert_eq!(config.connection_ttl, Duration::from_secs(300));
        assert!(config.tcp_nodelay);
    }

    #[test]
    fn test_group_connection_key_equality() {
        let k1 = GroupConnectionKey {
            target_addr: "127.0.0.1:8080".parse().unwrap(),
            group_id: 1,
        };
        let k2 = GroupConnectionKey {
            target_addr: "127.0.0.1:8080".parse().unwrap(),
            group_id: 1,
        };
        let k3 = GroupConnectionKey {
            target_addr: "127.0.0.1:8080".parse().unwrap(),
            group_id: 2,
        };
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
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
