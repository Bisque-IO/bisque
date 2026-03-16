//! Raft forwarding transport: duplex TCP for leader-forwarding.
//!
//! Clients may connect to any raft node. Followers forward commands to the
//! leader over a duplex TCP connection and receive responses back through
//! partitioned [`ClientPartition`](crate::async_apply) tasks driven by the
//! [`ClientRegistry`](crate::async_apply::ClientRegistry).
//!
//! ## Architecture
//!
//! **Follower side** ([`ForwardClient`]):
//! - Maintains a reconnecting duplex TCP connection to the current leader.
//! - Outbound: commands from local clients are framed and written to TCP.
//! - Inbound: response frames are bulk-read, bitmap-partitioned by `client_id`,
//!   and dispatched as [`Bytes`] chunks to [`ClientPartition`] tasks via
//!   [`ClientRegistry`](crate::async_apply::ClientRegistry).
//!
//! **Leader side** ([`ForwardAcceptor`]):
//! - Listens for follower connections on a TCP port.
//! - Per-follower: spawns a [`run_follower_responder`] task that drains
//!   encoded response bytes from a crossfire MPSC channel and writes them
//!   to the follower's TCP socket using vectored I/O.
//! - Inbound: commands are read from TCP and proposed to raft as
//!   `TAG_FORWARDED_BATCH` entries.
//!
//! ## Wire format
//!
//! **Handshake** (first frame, follower → leader):
//! ```text
//! [len=4][node_id:u32 LE]
//! ```
//!
//! **Batch frame** (follower → leader, wraps one drain event):
//! ```text
//! [raft_group_id:4 LE][batch_payload_len:4 LE][sub-frames...]
//! ```
//! Each sub-frame:
//! ```text
//! [payload_len:4 LE][client_id:4 LE][request_seq:8 LE][cmd_bytes...]
//! payload_len = 12 + cmd_bytes.len()
//! ```
//!
//! **Response** (leader → follower): length-prefixed frames:
//! ```text
//! [len:4 LE][client_id:4 LE][request_seq:8 LE][log_index:8 LE][response_bytes]
//! len = 20 + response_bytes.len()
//! ```

use std::io;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use openraft::Raft;

use crate::MqTypeConfig;
use crate::async_apply::{ClientRegistry, ResponderBroadcast, ResponderUpdateMsg};
use crate::types::MqCommand;

// =============================================================================
// IO abstractions
// =============================================================================

/// Boxed async reader — either plain TCP or TLS.
type BoxedReader = Box<dyn tokio::io::AsyncRead + Unpin + Send>;
/// Boxed async writer — either plain TCP or TLS.
type BoxedWriter = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;

// =============================================================================
// Wire format
// =============================================================================

const FRAME_PREFIX_LEN: usize = 4;

// ---------------------------------------------------------------------------
// Channel-based outbound buffer — two-level drainer architecture
// ---------------------------------------------------------------------------

/// Drain-batch header size on the wire: [raft_group_id:4][batch_payload_len:4].
const BATCH_HEADER_LEN: usize = 8;

/// A batch of outbound sub-frames for a single raft group, ready for TCP writing.
///
/// Built by the per-group drainer task and consumed by the TCP connection drainer.
/// Contains the batch header and a SmallVec of sub-frame `Bytes` for vectored I/O.
struct OutboundBatch {
    /// Wire header: `[raft_group_id:4 LE][payload_len:4 LE]`.
    header: [u8; BATCH_HEADER_LEN],
    /// Sub-frame `Bytes` — each is `[payload_len:4][client_id:4][request_seq:8][cmd...]`.
    frames: SmallVec<[Bytes; 16]>,
}

/// Channel-based outbound buffer for sending sub-frames to a raft group.
///
/// Producers call [`try_write`](Self::try_write) or [`write`](Self::write) to
/// encode and send sub-frames into the crossfire MPSC channel. A per-group
/// drainer task collects frames into [`OutboundBatch`]es and feeds them to the
/// TCP connection drainer for vectored I/O.
#[derive(Clone)]
pub struct OutboundBuf {
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>,
}

impl OutboundBuf {
    /// Encode a sub-frame: `[payload_len:4 LE][client_id:4 LE][request_seq:8 LE][cmd_bytes...]`.
    #[inline]
    fn encode_sub_frame(client_id: u32, request_seq: u64, cmd: &[u8]) -> Bytes {
        let payload_len = (12 + cmd.len()) as u32;
        let mut buf = BytesMut::with_capacity(16 + cmd.len());
        buf.put_u32_le(payload_len);
        buf.put_u32_le(client_id);
        buf.put_u64_le(request_seq);
        buf.put_slice(cmd);
        buf.freeze()
    }

    /// Try to send one sub-frame without blocking.
    ///
    /// Returns `false` if the channel is full (backpressure).
    #[inline]
    pub fn try_write(&self, client_id: u32, request_seq: u64, cmd: &[u8]) -> bool {
        let frame = Self::encode_sub_frame(client_id, request_seq, cmd);
        self.tx.try_send(frame).is_ok()
    }

    /// Send one sub-frame, awaiting if the channel is full.
    #[inline]
    pub async fn write(&self, client_id: u32, request_seq: u64, cmd: &[u8]) {
        let frame = Self::encode_sub_frame(client_id, request_seq, cmd);
        let _ = self.tx.send(frame).await;
    }
}

/// Write all data from an `IoSlice` array, handling partial writes.
///
/// Attempts a single `write_vectored` call first (writev syscall). If partial,
/// falls back to `write_all` for each remaining slice.
async fn write_all_vectored(writer: &mut BoxedWriter, slices: &[IoSlice<'_>]) -> io::Result<()> {
    let total: usize = slices.iter().map(|s| s.len()).sum();
    if total == 0 {
        return Ok(());
    }

    let n = writer.write_vectored(slices).await?;
    if n == total {
        return Ok(());
    }
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "write_vectored returned 0",
        ));
    }

    // Partial write — fall back to write_all for remaining data.
    let mut skip = n;
    for slice in slices {
        if skip >= slice.len() {
            skip -= slice.len();
            continue;
        }
        writer.write_all(&slice[skip..]).await?;
        skip = 0;
    }
    Ok(())
}

/// Per-raft-group drainer task (level 1).
///
/// Receives sub-frame `Bytes` from producers via the group channel, collects
/// them into an [`OutboundBatch`] (SmallVec), and sends the batch to the TCP
/// connection drainer channel.
///
/// Returns the group rx on exit for reuse across reconnections.
async fn run_group_drainer(
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<Bytes>>,
    raft_group_id: u32,
    batch_tx: crossfire::MAsyncTx<crossfire::mpmc::Array<OutboundBatch>>,
    stop: CancellationToken,
) -> crossfire::AsyncRx<crossfire::mpsc::Array<Bytes>> {
    loop {
        let first = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            result = rx.recv() => match result {
                Ok(frame) => frame,
                Err(_) => break,
            },
        };

        // Yield so producers can accumulate more frames.
        tokio::task::yield_now().await;

        let mut frames = SmallVec::<[Bytes; 16]>::new();
        frames.push(first);
        while let Ok(frame) = rx.try_recv() {
            frames.push(frame);
        }

        let payload_len: usize = frames.iter().map(|f| f.len()).sum();
        let mut header = [0u8; BATCH_HEADER_LEN];
        header[..4].copy_from_slice(&raft_group_id.to_le_bytes());
        header[4..8].copy_from_slice(&(payload_len as u32).to_le_bytes());

        if batch_tx
            .send(OutboundBatch { header, frames })
            .await
            .is_err()
        {
            break; // TCP drainer gone
        }
    }
    rx
}

/// TCP connection drainer task (level 2).
///
/// Receives [`OutboundBatch`]es from per-group drainer tasks via the MPMC
/// channel, builds a flat `IoSlice` array across all batches, and writes them
/// to TCP with vectored I/O. Multiple TCP drainer tasks can consume from the
/// same MPMC channel for connection pooling.
async fn run_tcp_drainer(
    batch_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<OutboundBatch>>,
    writer: &mut BoxedWriter,
    stop: &CancellationToken,
) -> io::Result<()> {
    loop {
        let first = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            result = batch_rx.recv() => match result {
                Ok(batch) => batch,
                Err(_) => return Ok(()),
            },
        };

        // Drain all immediately available batches for one coalesced write.
        let mut batches = SmallVec::<[OutboundBatch; 8]>::new();
        batches.push(first);
        while let Ok(batch) = batch_rx.try_recv() {
            batches.push(batch);
        }

        // Build IoSlice array: [header][frame0][frame1]...[header][frame0]...
        let mut io_slices = SmallVec::<[IoSlice<'_>; 64]>::new();
        for batch in &batches {
            io_slices.push(IoSlice::new(&batch.header));
            for frame in &batch.frames {
                io_slices.push(IoSlice::new(frame));
            }
        }

        write_all_vectored(writer, &io_slices).await?;
        writer.flush().await?;
    }
}

/// Write a length-prefixed frame.
async fn write_frame(writer: &mut BoxedWriter, data: &[u8]) -> io::Result<()> {
    let len = data.len() as u32;
    writer.write_all(&len.to_le_bytes()).await?;
    if !data.is_empty() {
        writer.write_all(data).await?;
    }
    Ok(())
}

/// Write a command/response frame: `[4-byte len][client_id:u32][payload]`.
async fn write_client_frame(
    writer: &mut BoxedWriter,
    client_id: u32,
    payload: &[u8],
) -> io::Result<()> {
    let total_len = (4 + payload.len()) as u32;
    writer.write_all(&total_len.to_le_bytes()).await?;
    writer.write_all(&client_id.to_le_bytes()).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    Ok(())
}

/// Flush and write multiple client frames batched.
async fn write_client_frames_batched(
    writer: &mut BoxedWriter,
    buf: &mut BytesMut,
    frames: &[(u32, Bytes)],
) -> io::Result<()> {
    buf.clear();
    for (client_id, payload) in frames {
        let total_len = (4 + payload.len()) as u32;
        buf.put_u32_le(total_len);
        buf.put_u32_le(*client_id);
        buf.put_slice(payload);
    }
    writer.write_all(buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a length-prefixed frame into `buf`. Returns the payload length.
/// Returns `Ok(0)` on clean EOF.
async fn read_frame_into(reader: &mut BoxedReader, buf: &mut BytesMut) -> io::Result<usize> {
    let mut len_buf = [0u8; FRAME_PREFIX_LEN];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
        Err(e) => return Err(e),
    }
    let payload_len = u32::from_le_bytes(len_buf) as usize;
    if payload_len == 0 {
        return Ok(0);
    }
    buf.clear();
    buf.reserve(payload_len);
    // Safety: we just reserved enough space.
    unsafe { buf.set_len(payload_len) };
    reader.read_exact(&mut buf[..payload_len]).await?;
    Ok(payload_len)
}

/// Parse a client frame payload: `[client_id:u32][data...]`.
fn parse_client_frame(buf: &BytesMut) -> Option<(u32, &[u8])> {
    if buf.len() < 4 {
        return None;
    }
    let client_id = u32::from_le_bytes(buf[..4].try_into().unwrap());
    Some((client_id, &buf[4..]))
}

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the forwarding transport.
#[derive(Debug, Clone)]
pub struct ForwardConfig {
    /// Capacity of each per-follower [`run_follower_responder`] crossfire MPSC
    /// channel (in `Bytes` chunks).  Higher values allow more in-flight chunks
    /// before backpressure kicks in.  Default: 256.
    pub responder_channel_capacity: usize,
    /// Capacity of the per-raft-group outbound crossfire MPSC channel (in
    /// sub-frame `Bytes` items).  Higher values buffer more commands during
    /// reconnections.  Default: 4096.
    pub outbound_channel_capacity: usize,
    /// Number of concurrent TCP connections to the leader for outbound
    /// command batches.  All connections consume from a shared MPMC batch
    /// queue, providing connection-level parallelism.  Default: 1.
    pub num_outbound_connections: usize,
    /// Time-to-live for each outbound TCP connection.  When a connection's
    /// TTL expires it spawns a replacement before closing, ensuring seamless
    /// rotation via the shared MPMC batch queue.  `None` disables TTL
    /// rotation.  Default: `None`.
    pub connection_ttl: Option<Duration>,
    /// Inbound command buffer capacity (leader side). Default: 4096.
    pub inbound_buffer_capacity: usize,
    /// TCP connect timeout. Default: 10s.
    pub connect_timeout: Duration,
    /// Base reconnect backoff. Default: 100ms.
    pub reconnect_base: Duration,
    /// Max reconnect backoff. Default: 5s.
    pub reconnect_max: Duration,
    /// TCP_NODELAY. Default: true.
    pub tcp_nodelay: bool,
    /// TLS client config for outbound connections (follower → leader).
    #[cfg(feature = "tls")]
    pub tls_client_config: Option<Arc<rustls::ClientConfig>>,
    /// TLS server config for inbound connections (leader accepts followers).
    #[cfg(feature = "tls")]
    pub tls_server_config: Option<Arc<rustls::ServerConfig>>,
    /// TLS server name for client connections. If None, uses IP address.
    #[cfg(feature = "tls")]
    pub tls_server_name: Option<rustls::pki_types::ServerName<'static>>,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        Self {
            responder_channel_capacity: 256,
            outbound_channel_capacity: 4096,
            num_outbound_connections: 1,
            connection_ttl: None,
            inbound_buffer_capacity: 4096,
            connect_timeout: Duration::from_secs(10),
            reconnect_base: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
            tcp_nodelay: true,
            #[cfg(feature = "tls")]
            tls_client_config: None,
            #[cfg(feature = "tls")]
            tls_server_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        }
    }
}

// =============================================================================
// TCP + TLS connection helpers
// =============================================================================

/// Establish a TCP connection, optionally upgrading to TLS.
async fn connect_tcp(
    addr: SocketAddr,
    config: &ForwardConfig,
) -> io::Result<(BoxedReader, BoxedWriter)> {
    let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;

    if config.tcp_nodelay {
        stream.set_nodelay(true)?;
    }

    #[cfg(feature = "tls")]
    if let Some(ref tls_config) = config.tls_client_config {
        let connector = tokio_rustls::TlsConnector::from(tls_config.clone());
        let server_name = config
            .tls_server_name
            .clone()
            .unwrap_or_else(|| rustls::pki_types::ServerName::IpAddress(addr.ip().into()));
        let tls_stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
        let (r, w) = tokio::io::split(tls_stream);
        return Ok((Box::new(r), Box::new(w)));
    }

    let (r, w) = stream.into_split();
    Ok((Box::new(r), Box::new(w)))
}

/// Accept a TCP connection, optionally upgrading to TLS.
async fn accept_tcp(
    stream: TcpStream,
    _config: &ForwardConfig,
) -> io::Result<(BoxedReader, BoxedWriter)> {
    #[cfg(feature = "tls")]
    if let Some(ref tls_config) = _config.tls_server_config {
        let acceptor = tokio_rustls::TlsAcceptor::from(tls_config.clone());
        let tls_stream = acceptor
            .accept(stream)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
        let (r, w) = tokio::io::split(tls_stream);
        return Ok((Box::new(r), Box::new(w)));
    }

    let (r, w) = stream.into_split();
    Ok((Box::new(r), Box::new(w)))
}

// =============================================================================
// FollowerResponder task
// =============================================================================

/// Drains pre-encoded response chunks from a crossfire MPSC channel and writes
/// them to a follower's TCP socket in vectored batches — zero per-record allocation.
///
/// Each `Bytes` chunk contains tight-packed `[client_id:4 LE][log_index:8 LE]`
/// records (12 bytes each) produced by [`PartitionWorker`] flush.
async fn run_follower_responder(
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<Bytes>>,
    mut writer: BoxedWriter,
    cancel: CancellationToken,
) {
    let mut slabs: Vec<Bytes> = Vec::with_capacity(16);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            result = rx.recv() => {
                let slab = match result {
                    Ok(b) => b,
                    Err(_) => return,
                };
                slabs.push(slab);
                // Drain all immediately available chunks for one coalesced write.
                while let Ok(more) = rx.try_recv() {
                    slabs.push(more);
                }
                for slab in &slabs {
                    if let Err(e) = writer.write_all(slab).await {
                        debug!(error = %e, "follower responder write failed");
                        return;
                    }
                }
                if let Err(e) = writer.flush().await {
                    debug!(error = %e, "follower responder flush failed");
                    return;
                }
                slabs.clear();
            }
        }
    }
}

// =============================================================================
// Responder table helpers
// =============================================================================

fn responder_txs_insert(
    update_tx: &tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
    broadcast: &ResponderBroadcast,
    node_id: u32,
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>,
) {
    broadcast.insert(node_id, tx.clone());
    let _ = update_tx.send((node_id, Some(tx)));
}

// =============================================================================
// ForwardedBatch
// =============================================================================

/// A batch of commands forwarded from a single follower TCP batch frame.
///
/// Holds one reference-counted `Bytes` buffer containing sub-frames parsed
/// from the wire format. Individual commands are parsed on-the-fly via
/// [`iter()`](Self::iter), yielding `(client_id, request_seq, cmd_bytes)`
/// triples with zero per-command allocation.
///
/// Sub-frame wire format: `[payload_len:4][client_id:4][request_seq:8][cmd_bytes...]`
/// where `payload_len = 12 + cmd_bytes.len()`.
pub struct ForwardedBatch {
    /// The originating node ID.
    pub node_id: u32,
    /// The raft group this batch belongs to.
    pub raft_group_id: u32,
    /// Raw sub-frame bytes (without the 8-byte batch header).
    buf: Bytes,
    /// Number of complete sub-frames in this batch.
    count: u32,
}

impl ForwardedBatch {
    /// Number of commands in this batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Whether the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Iterate over `(client_id, command_bytes)` pairs.
    ///
    /// Parses frame boundaries on-the-fly from the wire format — no allocation.
    #[inline]
    pub fn iter(&self) -> ForwardedBatchIter<'_> {
        ForwardedBatchIter {
            buf: &self.buf,
            pos: 0,
        }
    }
}

/// Iterator over commands in a [`ForwardedBatch`].
///
/// Yields `(client_id, request_seq, cmd_bytes)` triples parsed from
/// sub-frames: `[payload_len:4][client_id:4][request_seq:8][cmd_bytes...]`.
pub struct ForwardedBatchIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for ForwardedBatchIter<'a> {
    /// `(client_id, request_seq, cmd_bytes)`
    type Item = (u32, u64, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 4 > self.buf.len() {
            return None;
        }
        // payload_len = 12 + cmd_bytes.len()  (client_id:4 + request_seq:8 + cmd)
        let payload_len =
            u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        if payload_len < 12 || self.pos + 4 + payload_len > self.buf.len() {
            return None;
        }
        let base = self.pos + 4;
        let client_id = u32::from_le_bytes(self.buf[base..base + 4].try_into().unwrap());
        let request_seq = u64::from_le_bytes(self.buf[base + 4..base + 12].try_into().unwrap());
        let cmd = &self.buf[base + 12..base + payload_len];
        self.pos += 4 + payload_len;
        Some((client_id, request_seq, cmd))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None) // can't know without parsing
    }
}

// =============================================================================
// ForwardClient (follower side)
// =============================================================================

/// Follower-side forwarding client.
///
/// Maintains a duplex TCP connection to the leader. Commands are sent into
/// a crossfire MPSC channel and survive reconnections — data accumulated
/// during disconnect is drained on reconnect.
pub struct ForwardClient {
    /// Channel-based outbound buffer.
    outbound: OutboundBuf,
    /// Watch channel for target leader address.
    target_tx: tokio::sync::watch::Sender<Option<SocketAddr>>,
    /// Cancellation token for shutdown.
    cancel: CancellationToken,
    /// Background task handle.
    handle: Option<JoinHandle<()>>,
    /// Connection generation — bumped on each reconnect.
    pub generation: Arc<AtomicU64>,
}

/// Cheap, cloneable handle for sending commands through a [`ForwardClient`].
#[derive(Clone)]
pub struct ForwardHandle {
    outbound: OutboundBuf,
}

impl ForwardHandle {
    /// Try to forward a command to the leader without blocking.
    ///
    /// Returns `false` if the channel is full (backpressure).
    #[inline]
    pub fn try_forward(&self, client_id: u32, request_seq: u64, cmd: &[u8]) -> bool {
        self.outbound.try_write(client_id, request_seq, cmd)
    }

    /// Forward a command, yielding to the runtime on backpressure.
    pub async fn forward(&self, client_id: u32, request_seq: u64, cmd: &[u8]) {
        self.outbound.write(client_id, request_seq, cmd).await;
    }
}

impl ForwardClient {
    /// Create and start a forwarding client.
    ///
    /// - `config`: transport configuration.
    /// - `node_id`: this node's ID (sent in handshake).
    /// - `raft_group_id`: raft group ID for multiplexed TCP connections.
    /// - `initial_target`: leader address (or `None` if unknown).
    /// - `client_registry`: local client registry for response dispatch.
    pub fn start(
        config: ForwardConfig,
        node_id: u32,
        raft_group_id: u32,
        initial_target: Option<SocketAddr>,
        client_registry: Arc<ClientRegistry>,
    ) -> Self {
        let (tx, rx) = crossfire::mpsc::bounded_async::<Bytes>(config.outbound_channel_capacity);
        let outbound = OutboundBuf { tx };
        let (target_tx, target_rx) = tokio::sync::watch::channel(initial_target);
        let cancel = CancellationToken::new();
        let generation = Arc::new(AtomicU64::new(0));

        let handle = tokio::spawn(forward_client_loop(
            config,
            node_id,
            raft_group_id,
            rx,
            target_rx,
            cancel.clone(),
            generation.clone(),
            client_registry,
        ));

        Self {
            outbound,
            target_tx,
            cancel,
            handle: Some(handle),
            generation,
        }
    }

    /// Get a cheap, cloneable handle for sending commands.
    pub fn clone_handle(&self) -> ForwardHandle {
        ForwardHandle {
            outbound: self.outbound.clone(),
        }
    }

    /// Try to forward a command to the leader without blocking.
    ///
    /// Returns `false` if the channel is full (backpressure).
    #[inline]
    pub fn try_forward(&self, client_id: u32, request_seq: u64, cmd: &[u8]) -> bool {
        self.outbound.try_write(client_id, request_seq, cmd)
    }

    /// Forward a command, yielding to the runtime on backpressure.
    pub async fn forward(&self, client_id: u32, request_seq: u64, cmd: &[u8]) {
        self.outbound.write(client_id, request_seq, cmd).await;
    }

    /// Update the leader address. The client will reconnect to the new target.
    pub fn set_target(&self, addr: Option<SocketAddr>) {
        let _ = self.target_tx.send(addr);
    }

    /// Current connection generation (incremented on each reconnect).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Shut down the forwarding client.
    pub async fn shutdown(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

/// Why a connection exited — used by the connection pool to decide whether to
/// respawn, back off, or shut down.
enum ConnectionExit {
    /// The connection's TTL expired — spawn a replacement immediately.
    TtlExpired,
    /// IO or protocol error.
    Error(io::Error),
    /// Global shutdown requested.
    Shutdown,
}

/// Main connection-pool loop for the forward client.
///
/// Creates the MPMC batch channel and group drainer once, then manages a
/// [`JoinSet`](tokio::task::JoinSet) of TCP connections. Each connection
/// consumes from the shared MPMC batch queue, so TTL rotation and error
/// recovery are seamless — batches simply flow to whichever connection is
/// ready.
async fn forward_client_loop(
    config: ForwardConfig,
    node_id: u32,
    raft_group_id: u32,
    group_rx: crossfire::AsyncRx<crossfire::mpsc::Array<Bytes>>,
    mut target_rx: tokio::sync::watch::Receiver<Option<SocketAddr>>,
    cancel: CancellationToken,
    generation: Arc<AtomicU64>,
    client_registry: Arc<ClientRegistry>,
) {
    // Create the MPMC batch channel (shared across all TCP connections).
    let (batch_tx, batch_rx) = crossfire::mpmc::bounded_async::<OutboundBatch>(256);

    // Spawn the group drainer — lives for the entire ForwardClient lifetime.
    let drainer_stop = cancel.child_token();
    let _group_handle = tokio::spawn(run_group_drainer(
        group_rx,
        raft_group_id,
        batch_tx,
        drainer_stop.clone(),
    ));

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // Wait for a valid target address.
        let addr = loop {
            let current = *target_rx.borrow_and_update();
            if let Some(addr) = current {
                break addr;
            }
            tokio::select! {
                _ = target_rx.changed() => continue,
                _ = cancel.cancelled() => return,
            }
        };

        // Spawn the initial connection pool.
        let conn_cancel = cancel.child_token();
        let mut conn_set = tokio::task::JoinSet::new();
        for _ in 0..config.num_outbound_connections {
            conn_set.spawn(run_single_connection(
                batch_rx.clone(),
                addr,
                node_id,
                config.clone(),
                conn_cancel.clone(),
                Arc::clone(&client_registry),
                generation.clone(),
            ));
        }

        // Monitor the connection pool: respawn on TTL/error, stop on
        // leader change or shutdown.
        let pool_exit = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break ConnectionExit::Shutdown,
                _ = target_rx.changed() => break ConnectionExit::TtlExpired,
                join_result = conn_set.join_next() => {
                    match join_result {
                        // All connections gone (shouldn't happen — we respawn).
                        None => break ConnectionExit::Error(
                            io::Error::new(io::ErrorKind::Other, "all connections exited"),
                        ),
                        Some(Ok(ConnectionExit::TtlExpired)) => {
                            // Seamless rotation — spawn replacement immediately.
                            conn_set.spawn(run_single_connection(
                                batch_rx.clone(),
                                addr,
                                node_id,
                                config.clone(),
                                conn_cancel.clone(),
                                Arc::clone(&client_registry),
                                generation.clone(),
                            ));
                        }
                        Some(Ok(ConnectionExit::Error(e))) => {
                            debug!(error = %e, "outbound connection error, respawning");
                            // Small delay before respawn to avoid spin on persistent errors.
                            let batch_rx = batch_rx.clone();
                            let config = config.clone();
                            let conn_cancel = conn_cancel.clone();
                            let client_registry = Arc::clone(&client_registry);
                            let generation = generation.clone();
                            conn_set.spawn(async move {
                                tokio::time::sleep(config.reconnect_base).await;
                                run_single_connection(
                                    batch_rx,
                                    addr,
                                    node_id,
                                    config,
                                    conn_cancel,
                                    client_registry,
                                    generation,
                                )
                                .await
                            });
                        }
                        Some(Ok(ConnectionExit::Shutdown)) | Some(Err(_)) => {
                            // Shutdown or panic — don't respawn.
                        }
                    }
                }
            }
        };

        // Cancel all remaining connections and drain the JoinSet.
        conn_cancel.cancel();
        while conn_set.join_next().await.is_some() {}

        match pool_exit {
            ConnectionExit::Shutdown => return,
            ConnectionExit::TtlExpired => {
                // Leader changed — reconnect to new target.
                debug!("leader changed, reconnecting");
            }
            ConnectionExit::Error(e) => {
                debug!(error = %e, "connection pool error");
            }
        }
    }
}

/// Run a single TCP connection to the leader: connect, handshake, then run
/// the outbound TCP drainer (from MPMC batch queue) and inbound response
/// reader concurrently. Exits on TTL expiry, error, or cancellation.
async fn run_single_connection(
    batch_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<OutboundBatch>>,
    addr: SocketAddr,
    node_id: u32,
    config: ForwardConfig,
    cancel: CancellationToken,
    client_registry: Arc<ClientRegistry>,
    generation: Arc<AtomicU64>,
) -> ConnectionExit {
    // Connect.
    let conn = tokio::select! {
        c = connect_tcp(addr, &config) => c,
        _ = cancel.cancelled() => return ConnectionExit::Shutdown,
    };
    let (reader, writer) = match conn {
        Ok(c) => c,
        Err(e) => return ConnectionExit::Error(e),
    };

    // Handshake: send our node_id.
    let (reader, mut writer) = (reader, writer);
    if let Err(e) = write_frame(&mut writer, &node_id.to_le_bytes()).await {
        return ConnectionExit::Error(e);
    }
    if let Err(e) = writer.flush().await {
        return ConnectionExit::Error(e);
    }

    generation.fetch_add(1, Ordering::Release);
    debug!(target = %addr, "forward connection established");

    // Spawn the outbound TCP drainer.
    let stop = cancel.child_token();
    let outbound_failed = CancellationToken::new();

    let outbound_stop = stop.clone();
    let outbound_failed_clone = outbound_failed.clone();
    let outbound_handle = tokio::spawn(async move {
        let result = run_tcp_drainer(batch_rx, &mut writer, &outbound_stop).await;
        if result.is_err() {
            outbound_failed_clone.cancel();
        }
        result
    });

    // Run inbound + TTL concurrently.
    let exit =
        run_connection_inbound(reader, &client_registry, &cancel, &outbound_failed, &config).await;

    // Stop the outbound drainer and wait for it to finish its current write.
    stop.cancel();
    let _ = outbound_handle.await;

    exit
}

/// Inbound response reader for a single connection.
///
/// Bulk-reads `[len:4 LE][payload...]` response frames from the leader and
/// dispatches them to [`ClientPartition`]s via [`ClientRegistry`]. Also
/// monitors TTL expiry, cancellation, and outbound failures.
async fn run_connection_inbound(
    mut reader: BoxedReader,
    client_registry: &ClientRegistry,
    cancel: &CancellationToken,
    outbound_failed: &CancellationToken,
    config: &ForwardConfig,
) -> ConnectionExit {
    let mut read_buf = BytesMut::with_capacity(1024 * 1024);
    let n = client_registry.num_partitions();
    let ttl_sleep = async {
        match config.connection_ttl {
            Some(d) => tokio::time::sleep(d).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(ttl_sleep);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return ConnectionExit::Shutdown,
            _ = &mut ttl_sleep => return ConnectionExit::TtlExpired,
            _ = outbound_failed.cancelled() => {
                return ConnectionExit::Error(
                    io::Error::new(io::ErrorKind::BrokenPipe, "outbound task failed"),
                );
            }
            bytes_read = reader.read_buf(&mut read_buf) => {
                let bytes_read = match bytes_read {
                    Ok(n) => n,
                    Err(e) => return ConnectionExit::Error(e),
                };
                if bytes_read == 0 {
                    return ConnectionExit::Error(
                        io::Error::new(io::ErrorKind::ConnectionReset, "EOF"),
                    );
                }

                // Single-pass scan: find complete frame boundaries.
                let buf = read_buf.as_ref();
                let mut complete = 0;
                let mut sent_mask = 0u64;
                let mut sent_count = 0usize;
                let mut scan = 0;
                while scan + 8 <= buf.len() {
                    let frame_len =
                        u32::from_le_bytes(buf[scan..scan + 4].try_into().unwrap()) as usize;
                    let next = scan + 4 + frame_len;
                    if frame_len < 4 || next > buf.len() {
                        break;
                    }
                    scan = next;
                    complete = next;
                }
                while scan + 4 <= buf.len() {
                    let frame_len =
                        u32::from_le_bytes(buf[scan..scan + 4].try_into().unwrap()) as usize;
                    let next = scan + 4 + frame_len;
                    if next > buf.len() {
                        break;
                    }
                    scan = next;
                    complete = next;
                }

                if complete > 0 {
                    let slab = read_buf.split_to(complete).freeze();

                    if n == 1 {
                        client_registry.send_to_partition_async(0, slab).await;
                    } else {
                        let mut pos = 0;
                        while pos + 8 <= complete && sent_count < n {
                            let frame_len = u32::from_le_bytes(
                                slab[pos..pos + 4].try_into().unwrap(),
                            ) as usize;
                            if frame_len < 4 || pos + 4 + frame_len > complete {
                                break;
                            }
                            let client_id = u32::from_le_bytes(
                                slab[pos + 4..pos + 8].try_into().unwrap(),
                            );
                            let p = (client_id as usize) % n;
                            if (sent_mask >> p) & 1 == 0 {
                                sent_mask |= 1 << p;
                                sent_count += 1;
                                client_registry
                                    .send_to_partition_async(p, slab.slice(pos..complete))
                                    .await;
                            }
                            pos += 4 + frame_len;
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================
// ForwardAcceptor (leader side)
// =============================================================================

/// Leader-side acceptor for forwarded connections from followers.
///
/// Accepts TCP connections, spawns per-follower [`run_follower_responder`] tasks,
/// and surfaces forwarded commands by proposing `TAG_FORWARDED_BATCH` entries
/// to raft (or sending them to a channel in test/bench mode).
pub struct ForwardAcceptor {
    /// Live responder channel table — directly owned, drained from `update_rx`.
    responder_txs: Box<[Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>; 64]>,
    /// Receives (node_id, Option<tx>) updates from accept loop as followers connect/disconnect.
    update_rx: tokio::sync::mpsc::UnboundedReceiver<ResponderUpdateMsg>,
    /// Cancellation token for shutdown.
    cancel: CancellationToken,
    /// Accept loop handle.
    handle: Option<JoinHandle<()>>,
    /// The actual bound address.
    local_addr: SocketAddr,
}

impl ForwardAcceptor {
    /// Bind and start accepting forwarded connections.
    ///
    /// - `responder_broadcast`: broadcast handle from [`AsyncApplyManager::responder_broadcast`].
    ///   Used to notify partition workers when followers connect and disconnect.
    pub async fn start(
        config: ForwardConfig,
        bind_addr: SocketAddr,
        raft: Raft<MqTypeConfig>,
        responder_broadcast: ResponderBroadcast,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let (update_tx, update_rx) = tokio::sync::mpsc::unbounded_channel();

        let handle = tokio::spawn(accept_loop(
            config,
            listener,
            raft,
            update_tx,
            responder_broadcast,
            cancel.clone(),
        ));

        Ok(Self {
            responder_txs: Box::new(std::array::from_fn(|_| None)),
            update_rx,
            cancel,
            handle: Some(handle),
            local_addr,
        })
    }

    /// The actual bound local address (useful when binding to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Drain pending connect/disconnect updates into the local responder table.
    fn drain_updates(&mut self) {
        while let Ok((node_id, tx)) = self.update_rx.try_recv() {
            self.responder_txs[node_id as usize] = tx;
        }
    }

    /// Push pre-encoded response bytes to a connected follower's responder channel.
    ///
    /// `bytes` must contain tight-packed `[client_id:4 LE][log_index:8 LE]` records.
    /// Non-blocking — returns `false` if the follower is not connected or the channel
    /// is full.
    pub fn push_response(&mut self, node_id: u32, bytes: Bytes) -> bool {
        self.drain_updates();
        if let Some(tx) = &self.responder_txs[node_id as usize] {
            tx.try_send(bytes).is_ok()
        } else {
            false
        }
    }

    /// Async variant of [`push_response`] — awaits if the channel is full (backpressure).
    pub async fn push_response_async(&mut self, node_id: u32, bytes: Bytes) {
        self.drain_updates();
        if let Some(tx) = &self.responder_txs[node_id as usize] {
            let _ = tx.send(bytes).await;
        }
    }

    /// Number of connected follower nodes.
    pub fn connected_nodes(&mut self) -> usize {
        self.drain_updates();
        self.responder_txs.iter().filter(|x| x.is_some()).count()
    }

    /// Shut down the acceptor and all connections.
    pub async fn shutdown(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Testing/benchmarking constructor: captures forwarded batches in a channel
    /// instead of proposing to raft, making the acceptor usable without a real
    /// Raft node. In production, use [`start`](Self::start) instead.
    pub async fn start_with_channel(
        config: ForwardConfig,
        bind_addr: SocketAddr,
    ) -> io::Result<(Self, tokio::sync::mpsc::Receiver<ForwardedBatch>)> {
        let (tx, rx) = tokio::sync::mpsc::channel(config.inbound_buffer_capacity);
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let responder_broadcast = ResponderBroadcast::new_empty();
        let cancel = CancellationToken::new();
        let (update_tx, update_rx) = tokio::sync::mpsc::unbounded_channel();

        let handle = tokio::spawn(accept_loop_with_channel(
            config,
            listener,
            tx,
            update_tx,
            responder_broadcast,
            cancel.clone(),
        ));

        Ok((
            Self {
                responder_txs: Box::new(std::array::from_fn(|_| None)),
                update_rx,
                cancel,
                handle: Some(handle),
                local_addr,
            },
            rx,
        ))
    }
}

/// Accept loop (channel variant): captures batches to a channel instead of proposing to raft.
async fn accept_loop_with_channel(
    config: ForwardConfig,
    listener: TcpListener,
    tx: tokio::sync::mpsc::Sender<ForwardedBatch>,
    update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
    responder_broadcast: ResponderBroadcast,
    cancel: CancellationToken,
) {
    loop {
        let accept = tokio::select! {
            a = listener.accept() => a,
            _ = cancel.cancelled() => return,
        };
        let (stream, peer_addr) = match accept {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "forward accept error (channel)");
                continue;
            }
        };
        if config.tcp_nodelay {
            let _ = stream.set_nodelay(true);
        }
        let config = config.clone();
        let tx = tx.clone();
        let update_tx = update_tx.clone();
        let responder_broadcast = responder_broadcast.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_follower_connection_with_channel(
                config,
                stream,
                peer_addr,
                tx,
                update_tx,
                responder_broadcast,
                cancel,
            )
            .await
            {
                debug!(peer = %peer_addr, error = %e, "follower connection ended (channel)");
            }
        });
    }
}

/// Handle a single follower connection (channel variant): parses frames into ForwardedBatch
/// and sends them to the channel instead of proposing to raft.
async fn handle_follower_connection_with_channel(
    config: ForwardConfig,
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    tx: tokio::sync::mpsc::Sender<ForwardedBatch>,
    update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
    responder_broadcast: ResponderBroadcast,
    cancel: CancellationToken,
) -> io::Result<()> {
    let (mut reader, writer) = accept_tcp(stream, &config).await?;

    let mut handshake_buf = BytesMut::with_capacity(8);
    let len = read_frame_into(&mut reader, &mut handshake_buf).await?;
    if len != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid handshake length: {len}"),
        ));
    }
    let node_id = u32::from_le_bytes(handshake_buf[..4].try_into().unwrap());

    let (resp_tx, resp_rx) =
        crossfire::mpsc::bounded_async::<Bytes>(config.responder_channel_capacity);
    responder_txs_insert(&update_tx, &responder_broadcast, node_id, resp_tx);

    let writer_cancel = cancel.clone();
    let writer_handle = tokio::spawn(async move {
        run_follower_responder(resp_rx, writer, writer_cancel).await;
    });

    // Parse batch wire format: [raft_group_id:4][batch_payload_len:4][sub-frames...]
    let mut read_buf = BytesMut::with_capacity(128 * 1024);
    let result = loop {
        let n = tokio::select! {
            r = reader.read_buf(&mut read_buf) => r?,
            _ = cancel.cancelled() => break Ok(()),
        };
        if n == 0 {
            break Ok(());
        }
        // Process all complete batches.
        while read_buf.len() >= BATCH_HEADER_LEN {
            let raft_group_id = u32::from_le_bytes(read_buf[..4].try_into().unwrap());
            let batch_payload_len = u32::from_le_bytes(read_buf[4..8].try_into().unwrap()) as usize;
            let needed = BATCH_HEADER_LEN + batch_payload_len;
            if read_buf.len() < needed {
                break;
            }
            let _ = read_buf.split_to(BATCH_HEADER_LEN);
            let payload = read_buf.split_to(batch_payload_len).freeze();

            // Count sub-frames.
            let mut sub_pos = 0;
            let mut count = 0u32;
            while sub_pos + 4 <= payload.len() {
                let plen =
                    u32::from_le_bytes(payload[sub_pos..sub_pos + 4].try_into().unwrap()) as usize;
                if plen < 12 || sub_pos + 4 + plen > payload.len() {
                    break;
                }
                sub_pos += 4 + plen;
                count += 1;
            }

            if count > 0 {
                let batch = ForwardedBatch {
                    node_id,
                    raft_group_id,
                    buf: payload,
                    count,
                };
                if tx.send(batch).await.is_err() {
                    break; // receiver dropped (shutdown)
                }
            }
        }
    };

    // We do NOT remove the responder entry for this node_id.  When multiple
    // connections from the same follower overlap (e.g. TTL rotation), the
    // newer connection has already overwritten the responder.  Removing it
    // here would delete the *new* connection's responder.  Stale entries are
    // harmless — sends to a closed channel simply fail.
    writer_handle.abort();
    result
}

/// Accept loop: listens for follower connections and spawns per-node handlers.
async fn accept_loop(
    config: ForwardConfig,
    listener: TcpListener,
    raft: Raft<MqTypeConfig>,
    update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
    responder_broadcast: ResponderBroadcast,
    cancel: CancellationToken,
) {
    loop {
        let accept = tokio::select! {
            a = listener.accept() => a,
            _ = cancel.cancelled() => return,
        };

        let (stream, peer_addr) = match accept {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "forward accept error");
                continue;
            }
        };

        if config.tcp_nodelay {
            let _ = stream.set_nodelay(true);
        }

        let config = config.clone();
        let raft = raft.clone();
        let update_tx = update_tx.clone();
        let responder_broadcast = responder_broadcast.clone();
        let cancel = cancel.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_follower_connection(
                config,
                stream,
                peer_addr,
                raft,
                update_tx,
                responder_broadcast,
                cancel,
            )
            .await
            {
                debug!(peer = %peer_addr, error = %e, "follower connection ended");
            }
        });
    }
}

/// Handle a single follower connection: handshake, then duplex I/O.
async fn handle_follower_connection(
    config: ForwardConfig,
    stream: TcpStream,
    peer_addr: SocketAddr,
    raft: Raft<MqTypeConfig>,
    update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
    responder_broadcast: ResponderBroadcast,
    cancel: CancellationToken,
) -> io::Result<()> {
    let (mut reader, writer) = accept_tcp(stream, &config).await?;

    // Read handshake: node_id.
    let mut handshake_buf = BytesMut::with_capacity(8);
    let len = read_frame_into(&mut reader, &mut handshake_buf).await?;
    if len != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid handshake length: {len}"),
        ));
    }
    let node_id = u32::from_le_bytes(handshake_buf[..4].try_into().unwrap());
    info!(node_id, peer = %peer_addr, "follower connected for forwarding");

    // Create responder channel and register in the shared table.
    let (resp_tx, resp_rx) =
        crossfire::mpsc::bounded_async::<Bytes>(config.responder_channel_capacity);
    responder_txs_insert(&update_tx, &responder_broadcast, node_id, resp_tx);

    // Spawn the FollowerResponder task: drains encoded bytes to TCP.
    let writer_cancel = cancel.clone();
    let writer_handle = tokio::spawn(async move {
        run_follower_responder(resp_rx, writer, writer_cancel).await;
    });

    // TCP reader: bulk-read from follower, parse batch frame headers and sub-frames.
    // Wire format from follower: [raft_group_id:4][batch_payload_len:4][sub-frames...]
    // Sub-frame: [payload_len:4][client_id:4][request_seq:8][cmd_bytes...]
    let mut read_buf = BytesMut::with_capacity(128 * 1024);
    let result = loop {
        // Bulk-read whatever TCP gives us, appending after any partial frame.
        let n = tokio::select! {
            r = reader.read_buf(&mut read_buf) => r?,
            _ = cancel.cancelled() => break Ok(()),
        };
        if n == 0 {
            break Ok(()); // EOF
        }

        // Process all complete batches in the buffer.
        // A complete batch is: BATCH_HEADER_LEN(8) + batch_payload_len bytes.
        while read_buf.len() >= BATCH_HEADER_LEN {
            let _raft_group_id = u32::from_le_bytes(read_buf[..4].try_into().unwrap());
            let batch_payload_len = u32::from_le_bytes(read_buf[4..8].try_into().unwrap()) as usize;
            let needed = BATCH_HEADER_LEN + batch_payload_len;
            if read_buf.len() < needed {
                break; // incomplete batch — wait for more data
            }

            // Consume the 8-byte header and extract the sub-frames payload.
            let _ = read_buf.split_to(BATCH_HEADER_LEN);
            let payload = read_buf.split_to(batch_payload_len).freeze();

            // Count sub-frames: [payload_len:4][client_id:4][request_seq:8][cmd...]
            let mut sub_pos = 0;
            let mut count = 0u32;
            while sub_pos + 4 <= payload.len() {
                let plen =
                    u32::from_le_bytes(payload[sub_pos..sub_pos + 4].try_into().unwrap()) as usize;
                if plen < 12 || sub_pos + 4 + plen > payload.len() {
                    break;
                }
                sub_pos += 4 + plen;
                count += 1;
            }

            if count > 0 {
                // Wrap into TAG_FORWARDED_BATCH and propose to raft.
                // Fire-and-forget: spawned task so TCP reader is never blocked.
                let mut scratch = BytesMut::new();
                MqCommand::write_forwarded_batch(&mut scratch, node_id, count, &payload);
                let cmd = MqCommand::split_from(&mut scratch);
                let raft2 = raft.clone();
                tokio::spawn(async move {
                    if let Err(e) = raft2.client_write(cmd).await {
                        debug!(error = %e, "forwarded batch proposal failed");
                    }
                });
            }
        }
        // Any partial batch remains in read_buf for the next read.
    };

    // We do NOT remove the responder — see comment in
    // handle_follower_connection_with_channel for rationale.
    writer_handle.abort();

    info!(node_id, peer = %peer_addr, "follower disconnected");
    result
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Wire format tests ----

    #[tokio::test]
    async fn write_and_read_frame() {
        let (client, server) = tokio::io::duplex(4096);
        let (r, w) = tokio::io::split(client);
        let mut writer: BoxedWriter = Box::new(w);
        let mut reader: BoxedReader = Box::new(r);
        drop(server);

        // Can't test with duplex since both halves are needed.
        // Use a full loopback instead.
        let (client_stream, server_stream) = tokio::io::duplex(8192);
        let (cr, cw) = tokio::io::split(client_stream);
        let (sr, sw) = tokio::io::split(server_stream);
        let mut c_writer: BoxedWriter = Box::new(cw);
        let mut s_reader: BoxedReader = Box::new(sr);

        let payload = b"hello world";
        write_frame(&mut c_writer, payload).await.unwrap();
        c_writer.flush().await.unwrap();

        let mut buf = BytesMut::new();
        let len = read_frame_into(&mut s_reader, &mut buf).await.unwrap();
        assert_eq!(len, payload.len());
        assert_eq!(&buf[..], payload);
    }

    #[tokio::test]
    async fn write_and_read_client_frame() {
        let (c, s) = tokio::io::duplex(8192);
        let (cr, cw) = tokio::io::split(c);
        let (sr, sw) = tokio::io::split(s);
        let mut writer: BoxedWriter = Box::new(cw);
        let mut reader: BoxedReader = Box::new(sr);

        let client_id = 42u32;
        let payload = b"test payload";
        write_client_frame(&mut writer, client_id, payload)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut buf = BytesMut::new();
        let len = read_frame_into(&mut reader, &mut buf).await.unwrap();
        assert_eq!(len, 4 + payload.len());

        let (cid, data) = parse_client_frame(&buf).unwrap();
        assert_eq!(cid, 42);
        assert_eq!(data, payload);
    }

    #[tokio::test]
    async fn multiple_frames_in_sequence() {
        let (c, s) = tokio::io::duplex(8192);
        let (_, cw) = tokio::io::split(c);
        let (sr, _) = tokio::io::split(s);
        let mut writer: BoxedWriter = Box::new(cw);
        let mut reader: BoxedReader = Box::new(sr);

        for i in 0u32..10 {
            write_client_frame(&mut writer, i, &i.to_le_bytes())
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();

        let mut buf = BytesMut::new();
        for i in 0u32..10 {
            let len = read_frame_into(&mut reader, &mut buf).await.unwrap();
            assert!(len > 0);
            let (cid, data) = parse_client_frame(&buf).unwrap();
            assert_eq!(cid, i);
            assert_eq!(data, &i.to_le_bytes());
        }
    }

    #[tokio::test]
    async fn empty_payload_frame() {
        let (c, s) = tokio::io::duplex(8192);
        let (_, cw) = tokio::io::split(c);
        let (sr, _) = tokio::io::split(s);
        let mut writer: BoxedWriter = Box::new(cw);
        let mut reader: BoxedReader = Box::new(sr);

        write_client_frame(&mut writer, 99u32, b"").await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = BytesMut::new();
        let len = read_frame_into(&mut reader, &mut buf).await.unwrap();
        assert_eq!(len, 4); // just client_id, no payload
        let (cid, data) = parse_client_frame(&buf).unwrap();
        assert_eq!(cid, 99);
        assert!(data.is_empty());
    }

    // ---- ForwardClient + ForwardAcceptor integration ----

    #[tokio::test]
    async fn client_server_roundtrip() {
        let config = ForwardConfig {
            inbound_buffer_capacity: 64,
            ..Default::default()
        };
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (mut acceptor, mut batch_rx) =
            ForwardAcceptor::start_with_channel(config.clone(), bind_addr)
                .await
                .unwrap();
        let addr = acceptor.local_addr();

        let client_registry = ClientRegistry::new(4, 256);

        // Register a callback that records the log_index from the response frame.
        let log_idx = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let done = std::sync::Arc::new(tokio::sync::Notify::new());
        let log_idx_cb = std::sync::Arc::clone(&log_idx);
        let done_cb = std::sync::Arc::clone(&done);
        let cb: crate::async_apply::ResponseCallback =
            std::sync::Arc::new(move |_slab, _offset, msg, is_done| {
                if !is_done && msg.len() >= 16 {
                    // Frame payload after client_id: [request_seq:8][log_index:8]
                    let li = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                    log_idx_cb.store(li, std::sync::atomic::Ordering::Relaxed);
                    done_cb.notify_one();
                }
            });
        let local_cid = client_registry.register(cb);

        let mut client = ForwardClient::start(
            config.clone(),
            1,
            0,
            Some(addr),
            Arc::clone(&client_registry),
        );

        // Give connection time to establish.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Forward a command (request_seq=0 means no session tracking).
        client.forward(local_cid, 0, b"test cmd").await;

        // Acceptor should receive it.
        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.node_id, 1);
        let (cid, _req_seq, cmd_bytes) = batch.iter().next().unwrap();
        assert_eq!(cid, local_cid);
        assert_eq!(cmd_bytes, b"test cmd");

        // Send response back via push_response_async.
        // New format: [len=20:4][cid:4][request_seq:8][log_index:8]
        let mut resp = BytesMut::with_capacity(24);
        resp.put_u32_le(20u32);
        resp.put_u32_le(local_cid);
        resp.put_u64_le(0u64); // request_seq
        resp.put_u64_le(42u64); // log_index
        acceptor.push_response_async(1, resp.freeze()).await;

        // Client should dispatch response to the callback.
        tokio::time::timeout(Duration::from_secs(1), done.notified())
            .await
            .expect("timeout waiting for response callback");
        assert_eq!(log_idx.load(std::sync::atomic::Ordering::Relaxed), 42);

        client.shutdown().await;
        acceptor.shutdown().await;
        client_registry.unregister(local_cid);
    }

    #[tokio::test]
    async fn client_reconnects_on_disconnect() {
        let config = ForwardConfig {
            reconnect_base: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(50),
            ..Default::default()
        };

        let (mut acceptor, _batch_rx) =
            ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let addr = acceptor.local_addr();

        let client_registry = ClientRegistry::new(4, 256);
        let mut client = ForwardClient::start(
            config.clone(),
            2,
            0,
            Some(addr),
            Arc::clone(&client_registry),
        );

        // Wait for initial connection.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let gen1 = client.generation();

        // Kill the acceptor to simulate disconnect.
        acceptor.shutdown().await;

        // Wait for disconnect detection.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Restart acceptor on same port.
        let (mut acceptor, mut batch_rx) =
            ForwardAcceptor::start_with_channel(config.clone(), addr)
                .await
                .unwrap();

        // Wait for reconnect.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Forward should work after reconnect.
        client.forward(1, 0, b"after reconnect").await;

        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (_, _req_seq, cmd_bytes) = batch.iter().next().unwrap();
        assert_eq!(cmd_bytes, b"after reconnect");

        // Generation should have increased.
        assert!(client.generation() > gen1);

        client.shutdown().await;
        acceptor.shutdown().await;
    }

    #[tokio::test]
    async fn client_buffers_during_disconnect() {
        let config = ForwardConfig {
            reconnect_base: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(50),
            ..Default::default()
        };

        // Bind a listener to get a port, then drop it so the client has
        // somewhere to try (and fail) to connect.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let client_registry = ClientRegistry::new(4, 256);
        let mut client = ForwardClient::start(
            config.clone(),
            3,
            0,
            Some(addr),
            Arc::clone(&client_registry),
        );

        // No acceptor yet — buffer commands (client_id is u32 now).
        for i in 0u32..5 {
            client.try_forward(i, 0, &i.to_le_bytes());
        }

        // Start acceptor on the same port.
        let (mut acceptor, mut batch_rx) =
            ForwardAcceptor::start_with_channel(config.clone(), addr)
                .await
                .unwrap();

        // Wait for connection + buffer drain.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Should receive all buffered commands (possibly across multiple batches).
        let mut total_commands = 0usize;
        while let Ok(Some(batch)) =
            tokio::time::timeout(Duration::from_millis(200), batch_rx.recv()).await
        {
            total_commands += batch.len();
        }

        assert_eq!(total_commands, 5, "expected all 5 buffered commands");

        client.shutdown().await;
        acceptor.shutdown().await;
    }

    #[tokio::test]
    async fn leader_change_triggers_reconnect() {
        let config = ForwardConfig {
            reconnect_base: Duration::from_millis(10),
            ..Default::default()
        };

        let (mut acceptor1, mut batch_rx1) =
            ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let addr1 = acceptor1.local_addr();

        let client_registry = ClientRegistry::new(4, 256);
        let mut client = ForwardClient::start(
            config.clone(),
            4,
            0,
            Some(addr1),
            Arc::clone(&client_registry),
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Forward to first leader.
        client.forward(1, 0, b"to leader 1").await;
        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx1.recv())
            .await
            .unwrap()
            .unwrap();
        let (_, _req_seq, cmd_bytes) = batch.iter().next().unwrap();
        assert_eq!(cmd_bytes, b"to leader 1");

        let (mut acceptor2, mut batch_rx2) =
            ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let addr2 = acceptor2.local_addr();

        // Change target to leader 2.
        client.set_target(Some(addr2));
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Forward to second leader.
        client.forward(1, 0, b"to leader 2").await;

        // Retransmitted unACKed batches from leader 1 may arrive first;
        // scan until we find "to leader 2".
        let mut found_cmd = false;
        for _ in 0..5 {
            let Ok(Some(batch)) =
                tokio::time::timeout(Duration::from_secs(1), batch_rx2.recv()).await
            else {
                break;
            };
            for (_, _, cmd_bytes) in batch.iter() {
                if cmd_bytes == b"to leader 2" {
                    found_cmd = true;
                }
            }
            if found_cmd {
                break;
            }
        }
        assert!(found_cmd, "expected 'to leader 2' to arrive on leader 2");

        client.shutdown().await;
        acceptor1.shutdown().await;
        acceptor2.shutdown().await;
    }

    #[tokio::test]
    async fn multiple_followers_concurrent() {
        let config = ForwardConfig::default();

        let (mut acceptor, mut batch_rx) =
            ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let addr = acceptor.local_addr();

        let num_followers: u32 = 3;
        let mut clients = Vec::new();
        for i in 0..num_followers {
            let reg = ClientRegistry::new(4, 256);
            let client = ForwardClient::start(config.clone(), 10 + i, 0, Some(addr), reg);
            clients.push(client);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Each follower sends a command.
        for (i, client) in clients.iter().enumerate() {
            let msg = format!("from node {}", 10 + i as u32);
            client.forward(1, 0, msg.as_bytes()).await;
        }

        // Collect all commands (may arrive as multiple batches).
        let mut received_nodes = Vec::new();
        while received_nodes.len() < num_followers as usize {
            let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv())
                .await
                .unwrap()
                .unwrap();
            for _ in 0..batch.len() {
                received_nodes.push(batch.node_id);
            }
        }
        received_nodes.sort();
        assert_eq!(received_nodes, vec![10u32, 11, 12]);

        // All followers should have routes.
        assert_eq!(acceptor.connected_nodes(), 3);

        for mut c in clients {
            c.shutdown().await;
        }
        acceptor.shutdown().await;
    }

    #[tokio::test]
    async fn node_route_response_delivery_through_tcp() {
        let config = ForwardConfig {
            inbound_buffer_capacity: 64,
            ..Default::default()
        };

        let (mut acceptor, mut batch_rx) =
            ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let addr = acceptor.local_addr();

        let client_registry = ClientRegistry::new(4, 256);

        // Two callbacks, one per client.
        let done1 = std::sync::Arc::new(tokio::sync::Notify::new());
        let done2 = std::sync::Arc::new(tokio::sync::Notify::new());
        let log1 = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let log2 = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        let (done1c, log1c) = (std::sync::Arc::clone(&done1), std::sync::Arc::clone(&log1));
        let cb1: crate::async_apply::ResponseCallback =
            std::sync::Arc::new(move |_slab, _offset, msg, is_done| {
                if !is_done && msg.len() >= 16 {
                    // Frame payload after client_id: [request_seq:8][log_index:8]
                    log1c.store(
                        u64::from_le_bytes(msg[8..16].try_into().unwrap()),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    done1c.notify_one();
                }
            });
        let (done2c, log2c) = (std::sync::Arc::clone(&done2), std::sync::Arc::clone(&log2));
        let cb2: crate::async_apply::ResponseCallback =
            std::sync::Arc::new(move |_slab, _offset, msg, is_done| {
                if !is_done && msg.len() >= 16 {
                    // Frame payload after client_id: [request_seq:8][log_index:8]
                    log2c.store(
                        u64::from_le_bytes(msg[8..16].try_into().unwrap()),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    done2c.notify_one();
                }
            });
        let cid1 = client_registry.register(cb1);
        let cid2 = client_registry.register(cb2);

        let mut client = ForwardClient::start(
            config.clone(),
            5,
            0,
            Some(addr),
            Arc::clone(&client_registry),
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Forward a command to establish the route.
        client.forward(cid1, 0, b"cmd1").await;
        let _ = tokio::time::timeout(Duration::from_millis(200), batch_rx.recv()).await;

        // Send responses via push_response_async.
        // New format: [len=20:4][cid:4][request_seq:8][log_index:8]
        let mut resp = BytesMut::with_capacity(48);
        resp.put_u32_le(20u32);
        resp.put_u32_le(cid1);
        resp.put_u64_le(0u64); // request_seq
        resp.put_u64_le(100u64); // log_index
        resp.put_u32_le(20u32);
        resp.put_u32_le(cid2);
        resp.put_u64_le(0u64); // request_seq
        resp.put_u64_le(200u64); // log_index
        acceptor.push_response_async(5, resp.freeze()).await;

        // Both callbacks should fire.
        tokio::time::timeout(Duration::from_secs(1), done1.notified())
            .await
            .expect("timeout waiting for cid1 response");
        tokio::time::timeout(Duration::from_secs(1), done2.notified())
            .await
            .expect("timeout waiting for cid2 response");
        assert_eq!(log1.load(std::sync::atomic::Ordering::Relaxed), 100);
        assert_eq!(log2.load(std::sync::atomic::Ordering::Relaxed), 200);

        client.shutdown().await;
        acceptor.shutdown().await;
        client_registry.unregister(cid1);
        client_registry.unregister(cid2);
    }

    // ---- ForwardConfig tests ----

    #[test]
    fn default_config() {
        let cfg = ForwardConfig::default();
        assert_eq!(cfg.responder_channel_capacity, 256);
        assert_eq!(cfg.outbound_channel_capacity, 4096);
        assert_eq!(cfg.connect_timeout, Duration::from_secs(10));
        assert!(cfg.tcp_nodelay);
    }

    // ---- Parse tests ----

    #[test]
    fn parse_client_frame_valid() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(123);
        buf.put_slice(b"data");

        let (cid, data) = parse_client_frame(&buf).unwrap();
        assert_eq!(cid, 123);
        assert_eq!(data, b"data");
    }

    #[test]
    fn parse_client_frame_too_short() {
        let buf = BytesMut::from(&[1u8, 2, 3][..]);
        assert!(parse_client_frame(&buf).is_none());
    }

    #[test]
    fn parse_client_frame_just_client_id() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(42);

        let (cid, data) = parse_client_frame(&buf).unwrap();
        assert_eq!(cid, 42);
        assert!(data.is_empty());
    }

    // ---- Batched write tests ----

    #[tokio::test]
    async fn write_batched_frames() {
        let (c, s) = tokio::io::duplex(8192);
        let (_, cw) = tokio::io::split(c);
        let (sr, _) = tokio::io::split(s);
        let mut writer: BoxedWriter = Box::new(cw);
        let mut reader: BoxedReader = Box::new(sr);

        let frames = vec![
            (1u32, Bytes::from_static(b"first")),
            (2u32, Bytes::from_static(b"second")),
            (3u32, Bytes::from_static(b"third")),
        ];

        let mut buf = BytesMut::new();
        write_client_frames_batched(&mut writer, &mut buf, &frames)
            .await
            .unwrap();

        let mut read_buf = BytesMut::new();
        for (expected_cid, expected_payload) in &frames {
            let len = read_frame_into(&mut reader, &mut read_buf).await.unwrap();
            assert!(len > 0);
            let (cid, data) = parse_client_frame(&read_buf).unwrap();
            assert_eq!(cid, *expected_cid);
            assert_eq!(data, expected_payload.as_ref());
        }
    }

    // ---- Batch framing tests ----

    /// Helper: build a raw batch frame (follower→leader wire format) manually.
    /// `[raft_group_id:4][batch_payload_len:4][sub-frames...]`
    /// Each sub-frame: `[payload_len:4][client_id:4][request_seq:8][cmd_bytes...]`
    fn build_batch_frame(commands: &[(u32, u64, &[u8])]) -> Bytes {
        build_batch_frame_with_group(0, commands)
    }

    fn build_batch_frame_with_group(raft_group_id: u32, commands: &[(u32, u64, &[u8])]) -> Bytes {
        let mut buf = BytesMut::new();

        // Compute total payload size first.
        let payload_len: usize = commands.iter().map(|(_, _, cmd)| 4 + 12 + cmd.len()).sum();

        buf.extend_from_slice(&raft_group_id.to_le_bytes()); // raft_group_id:4
        buf.extend_from_slice(&(payload_len as u32).to_le_bytes()); // batch_payload_len:4
        for (client_id, request_seq, cmd) in commands {
            let sub_payload_len = (12 + cmd.len()) as u32;
            buf.extend_from_slice(&sub_payload_len.to_le_bytes()); // payload_len:4
            buf.extend_from_slice(&client_id.to_le_bytes()); // client_id:4
            buf.extend_from_slice(&request_seq.to_le_bytes()); // request_seq:8
            buf.extend_from_slice(cmd); // cmd_bytes
        }
        buf.freeze()
    }

    #[test]
    fn batch_frame_wire_format_roundtrip() {
        // Build a batch frame with two commands and verify ForwardedBatchIter decodes it.
        let raw = build_batch_frame(&[(1, 100, b"hello"), (2, 200, b"world")]);

        // Skip batch header (4 bytes) to get sub-frames.
        let sub_frames = raw.slice(BATCH_HEADER_LEN..);
        let batch = ForwardedBatch {
            node_id: 7,
            raft_group_id: 0,
            buf: sub_frames,
            count: 2,
        };

        let items: Vec<_> = batch.iter().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (1, 100, b"hello".as_ref()));
        assert_eq!(items[1], (2, 200, b"world".as_ref()));
        assert_eq!(batch.node_id, 7);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn forwarded_batch_iter_single_cmd() {
        let raw = build_batch_frame(&[(99, 42, b"payload")]);
        let batch = ForwardedBatch {
            node_id: 1,
            raft_group_id: 0,
            buf: raw.slice(BATCH_HEADER_LEN..),
            count: 1,
        };
        let mut it = batch.iter();
        let (cid, rseq, cmd) = it.next().unwrap();
        assert_eq!(cid, 99);
        assert_eq!(rseq, 42);
        assert_eq!(cmd, b"payload");
        assert!(it.next().is_none());
    }

    #[test]
    fn forwarded_batch_iter_empty_cmd() {
        let raw = build_batch_frame(&[(5, 0, b"")]);
        let batch = ForwardedBatch {
            node_id: 1,
            raft_group_id: 0,
            buf: raw.slice(BATCH_HEADER_LEN..),
            count: 1,
        };
        let items: Vec<_> = batch.iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], (5u32, 0u64, b"".as_ref()));
    }

    #[test]
    fn sub_frame_encoding() {
        let frame = OutboundBuf::encode_sub_frame(42, 100, b"hello");
        assert_eq!(frame.len(), 16 + 5); // 4+4+8+5
        let payload_len = u32::from_le_bytes(frame[0..4].try_into().unwrap());
        assert_eq!(payload_len, 17); // 12 + 5
        let client_id = u32::from_le_bytes(frame[4..8].try_into().unwrap());
        assert_eq!(client_id, 42);
        let request_seq = u64::from_le_bytes(frame[8..16].try_into().unwrap());
        assert_eq!(request_seq, 100);
        assert_eq!(&frame[16..], b"hello");
    }

    #[tokio::test]
    async fn outbound_channel_try_write() {
        let (tx, rx) = crossfire::mpsc::bounded_async::<Bytes>(16);
        let outbound = OutboundBuf { tx };

        assert!(outbound.try_write(1, 10, b"cmd_a"));
        assert!(outbound.try_write(2, 11, b"cmd_b"));

        let frame1 = rx.try_recv().unwrap();
        let frame2 = rx.try_recv().unwrap();

        // Verify sub-frame wire format.
        let cid1 = u32::from_le_bytes(frame1[4..8].try_into().unwrap());
        let cid2 = u32::from_le_bytes(frame2[4..8].try_into().unwrap());
        assert_eq!(cid1, 1);
        assert_eq!(cid2, 2);
    }

    #[test]
    fn forwarded_batch_iter_request_seq_roundtrip() {
        // Verify request_seq is correctly encoded and decoded.
        let raw = build_batch_frame(&[
            (10, 0, b"zero_seq"),       // request_seq = 0 (write_batcher path)
            (20, u64::MAX, b"max_seq"), // request_seq = u64::MAX
            (30, 12345678, b"mid"),
        ]);
        let batch = ForwardedBatch {
            node_id: 1,
            raft_group_id: 0,
            buf: raw.slice(BATCH_HEADER_LEN..),
            count: 3,
        };
        let items: Vec<_> = batch.iter().collect();
        assert_eq!(items[0], (10, 0, b"zero_seq".as_ref()));
        assert_eq!(items[1], (20, u64::MAX, b"max_seq".as_ref()));
        assert_eq!(items[2], (30, 12345678, b"mid".as_ref()));
    }
}
