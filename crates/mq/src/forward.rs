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
//! **Command** (follower → leader): length-prefixed frames
//! ```text
//! [len:4 LE][client_id:u32 LE][command bytes...]
//! ```
//!
//! **Handshake** (first frame, follower → leader):
//! ```text
//! [len=4][node_id:u32 LE]
//! ```
//!
//! **Response** (leader → follower): tight-packed fixed-width records, no length prefix:
//! ```text
//! [client_id:u32 LE][log_index:u64 LE]  (12 bytes per record)
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use openraft::Raft;

use crate::MqTypeConfig;
use crate::async_apply::{
    ClientRegistry, ResponderBroadcast, ResponderTxVec, ResponseEntry, SharedResponderTxs,
};
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
// Outbound partition buffer — lock-free, partitioned write path
// ---------------------------------------------------------------------------

/// Capacity of each outbound segment. 1 MiB fits ~131K 8-byte frames.
const OUTBOUND_SEG_CAPACITY: u32 = 1 << 20; // 1 MiB

/// Bit 31 of `write_pos` is set by the drainer to seal the segment.
const OUTBOUND_SEALED_BIT: u32 = 1 << 31;

// Sub-frame format (no alignment padding, directly usable as TCP wire frames):
//   [payload_len: 4 LE][client_id: 4 LE][cmd_bytes...]
//   payload_len = 4 + cmd.len()
//   total frame  = 8 + cmd.len() bytes

/// One fixed-capacity buffer for a single outbound partition.
///
/// # Concurrency
/// - `write_pos` is claimed by producers via CAS; bit 31 is the seal flag.
/// - `committed` is incremented by producers (Release) after each write.
/// - Drainer spins (Acquire) until `committed == final_write_pos` before reading.
struct OutboundSeg {
    data: std::cell::UnsafeCell<BytesMut>,
    write_pos: AtomicU32, // bit 31 = OUTBOUND_SEALED_BIT
    committed: AtomicU32,
    capacity: u32,
}

// Safety: producers write to non-overlapping ranges claimed via CAS; the drainer
// reads only after all producers have committed (spin-wait on `committed`).
unsafe impl Send for OutboundSeg {}
unsafe impl Sync for OutboundSeg {}

impl OutboundSeg {
    fn new(capacity: u32) -> Box<Self> {
        Box::new(Self {
            data: std::cell::UnsafeCell::new(BytesMut::with_capacity(capacity as usize)),
            write_pos: AtomicU32::new(0),
            committed: AtomicU32::new(0),
            capacity,
        })
    }
}

/// Per-partition state: an atomically-swappable active segment.
struct OutboundPartition {
    active: AtomicPtr<OutboundSeg>,
}

unsafe impl Send for OutboundPartition {}
unsafe impl Sync for OutboundPartition {}

impl Drop for OutboundPartition {
    fn drop(&mut self) {
        let p = self.active.load(Ordering::Relaxed);
        if !p.is_null() {
            unsafe { drop(Box::from_raw(p)) };
        }
    }
}

/// Shared waker state for the drainer. Coalesces rapid writes into one wakeup.
struct OutboundWaker {
    pending: AtomicBool,
    notify: tokio::sync::Notify,
    shutdown: AtomicBool,
}

impl OutboundWaker {
    #[inline]
    fn wake(&self) {
        if !self.pending.swap(true, Ordering::Release) {
            self.notify.notify_waiters();
        }
    }
}

/// Shared outbound buffer — held by both the `ForwardClient`/`ForwardHandle` writers
/// and the background connection task that drains it.
///
/// Writers call [`try_write`](OutboundBuf::try_write) or
/// [`write`](OutboundBuf::write) to append sub-frames lock-free.
/// The background loop calls [`drain_to_tcp`](OutboundBuf::drain_to_tcp)
/// after [`wait_for_data`](OutboundBuf::wait_for_data) wakes it.
pub struct OutboundBuf {
    partitions: Vec<Arc<OutboundPartition>>,
    waker: OutboundWaker,
    mask: u32,
}

impl OutboundBuf {
    fn new(num_partitions: usize) -> Self {
        let n = num_partitions.next_power_of_two().max(1);
        let partitions = (0..n)
            .map(|_| {
                Arc::new(OutboundPartition {
                    active: AtomicPtr::new(Box::into_raw(OutboundSeg::new(OUTBOUND_SEG_CAPACITY))),
                })
            })
            .collect();
        Self {
            partitions,
            waker: OutboundWaker {
                pending: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
                shutdown: AtomicBool::new(false),
            },
            mask: (n - 1) as u32,
        }
    }

    /// Try to write one sub-frame to the partition for `client_id`.
    ///
    /// Returns `false` if the segment is sealed or full (backpressure).
    /// The caller is responsible for retrying via [`write`](Self::write).
    #[inline]
    pub fn try_write(&self, client_id: u32, cmd: &[u8]) -> bool {
        let entry_size = (8 + cmd.len()) as u32; // no alignment padding
        let payload_len = (4 + cmd.len()) as u32;
        let idx = (client_id & self.mask) as usize;
        let partition = &self.partitions[idx];

        loop {
            let seg_ptr = partition.active.load(Ordering::Acquire);
            let seg = unsafe { &*seg_ptr };

            let pos = seg.write_pos.load(Ordering::Relaxed);
            if pos & OUTBOUND_SEALED_BIT != 0 || pos + entry_size > seg.capacity {
                return false;
            }
            if seg
                .write_pos
                .compare_exchange(pos, pos + entry_size, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue; // lost slot race, retry
            }

            unsafe {
                let dst = (*seg.data.get()).as_ptr().add(pos as usize) as *mut u8;
                (dst as *mut u32).write_unaligned(payload_len.to_le());
                (dst.add(4) as *mut u32).write_unaligned(client_id.to_le());
                std::ptr::copy_nonoverlapping(cmd.as_ptr(), dst.add(8), cmd.len());
            }

            seg.committed.fetch_add(entry_size, Ordering::Release);
            self.waker.wake();
            return true;
        }
    }

    /// Async write — yields to runtime on backpressure until the frame is accepted.
    #[inline]
    pub async fn write(&self, client_id: u32, cmd: &[u8]) {
        while !self.try_write(client_id, cmd) {
            tokio::task::yield_now().await;
        }
    }

    /// Wait for data to be available (HighWaterMark pattern).
    async fn wait_for_data(&self) {
        let notified = self.waker.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.waker.pending.swap(false, Ordering::AcqRel) {
            notified.await;
            self.waker.pending.store(false, Ordering::Relaxed);
        }
    }

    /// Seal all partitions, spin-wait for in-flight writes, then write drained
    /// bytes to `writer`. Returns the number of bytes written (0 = nothing drained).
    async fn drain_to_tcp(&self, writer: &mut BoxedWriter) -> io::Result<usize> {
        let mut total = 0usize;
        for partition in self.partitions.iter() {
            let old_ptr = partition.active.load(Ordering::Acquire);
            let old_seg = unsafe { &*old_ptr };

            let sealed = old_seg
                .write_pos
                .fetch_or(OUTBOUND_SEALED_BIT, Ordering::AcqRel);
            let final_pos = sealed & !OUTBOUND_SEALED_BIT;

            if final_pos == 0 {
                // Empty segment — unseal and reuse.
                old_seg
                    .write_pos
                    .fetch_and(!OUTBOUND_SEALED_BIT, Ordering::Relaxed);
                continue;
            }

            // Install a fresh segment so writers can proceed immediately.
            let new_seg = Box::into_raw(OutboundSeg::new(OUTBOUND_SEG_CAPACITY));
            partition.active.store(new_seg, Ordering::Release);

            // Spin-wait for all in-flight producers to finish committing.
            while old_seg.committed.load(Ordering::Acquire) < final_pos {
                std::hint::spin_loop();
            }

            // Extract the committed bytes (zero-copy split).
            let data = unsafe {
                let bm = &mut *old_seg.data.get();
                bm.set_len(final_pos as usize);
                bm.split()
            };
            // Drop the old segment now that we own `data`.
            unsafe { drop(Box::from_raw(old_ptr)) };

            total += data.len();
            writer.write_all(&data).await?;
        }
        if total > 0 {
            writer.flush().await?;
        }
        Ok(total)
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
    /// Number of lock-free outbound partitions for the follower write buffer.
    /// Must be a power of two; defaults to 4.
    pub num_outbound_partitions: usize,
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
            num_outbound_partitions: 4,
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
                // Drain all immediately available chunks for one vectored write.
                while let Ok(more) = rx.try_recv() {
                    slabs.push(more);
                }
                // Write each slab sequentially (tokio doesn't expose true scatter I/O
                // via AsyncWriteExt, so we loop write_all which is equivalent for TCP).
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
    txs: &ArcSwap<ResponderTxVec>,
    broadcast: &ResponderBroadcast,
    node_id: u32,
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>,
) {
    broadcast.insert(node_id, tx.clone());
    txs.rcu(|old| {
        let mut new_arr: Box<[_; 64]> = Box::new(std::array::from_fn(|i| old[i].clone()));
        new_arr[node_id as usize] = Some(tx.clone());
        new_arr
    });
}

fn responder_txs_remove(
    txs: &ArcSwap<ResponderTxVec>,
    broadcast: &ResponderBroadcast,
    node_id: u32,
) {
    broadcast.remove(node_id);
    txs.rcu(|old| {
        let mut new_arr: Box<[_; 64]> = Box::new(std::array::from_fn(|i| old[i].clone()));
        new_arr[node_id as usize] = None;
        new_arr
    });
}

// =============================================================================
// ForwardedBatch
// =============================================================================

/// A batch of commands forwarded from a single follower TCP read.
///
/// Holds one reference-counted `Bytes` buffer for the entire read. Individual
/// commands are parsed on-the-fly from the wire format via [`iter()`](Self::iter),
/// yielding `(client_id, &[u8])` pairs with zero per-command allocation.
pub struct ForwardedBatch {
    /// The originating node ID.
    pub node_id: u32,
    /// Raw wire data: `[len:4][client_id:4][cmd]...` concatenated frames.
    buf: Bytes,
    /// Number of complete frames in this batch.
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
pub struct ForwardedBatchIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for ForwardedBatchIter<'a> {
    type Item = (u32, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 4 > self.buf.len() {
            return None;
        }
        let frame_len =
            u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        if frame_len < 4 || self.pos + 4 + frame_len > self.buf.len() {
            return None;
        }
        let payload_start = self.pos + 4;
        let client_id = u32::from_le_bytes(
            self.buf[payload_start..payload_start + 4]
                .try_into()
                .unwrap(),
        );
        let cmd = &self.buf[payload_start + 4..payload_start + frame_len];
        self.pos = payload_start + frame_len;
        Some((client_id, cmd))
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
/// Maintains a duplex TCP connection to the leader. Commands are written into
/// a partitioned lock-free [`OutboundBuf`] and survive reconnections —
/// data accumulated during disconnect is drained on reconnect.
pub struct ForwardClient {
    /// Shared lock-free outbound buffer.
    outbound: Arc<OutboundBuf>,
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
    outbound: Arc<OutboundBuf>,
}

impl ForwardHandle {
    /// Try to forward a command to the leader without blocking.
    ///
    /// Returns `false` if the active segment is sealed or full (backpressure).
    #[inline]
    pub fn try_forward(&self, client_id: u32, cmd: &[u8]) -> bool {
        self.outbound.try_write(client_id, cmd)
    }

    /// Forward a command, yielding to the runtime on backpressure.
    pub async fn forward(&self, client_id: u32, cmd: &[u8]) {
        self.outbound.write(client_id, cmd).await;
    }
}

impl ForwardClient {
    /// Create and start a forwarding client.
    ///
    /// - `config`: transport configuration.
    /// - `node_id`: this node's ID (sent in handshake).
    /// - `initial_target`: leader address (or `None` if unknown).
    /// - `client_registry`: local client registry for response dispatch.
    pub fn start(
        config: ForwardConfig,
        node_id: u32,
        initial_target: Option<SocketAddr>,
        client_registry: Arc<ClientRegistry>,
    ) -> Self {
        let outbound = Arc::new(OutboundBuf::new(config.num_outbound_partitions));
        let (target_tx, target_rx) = tokio::sync::watch::channel(initial_target);
        let cancel = CancellationToken::new();
        let generation = Arc::new(AtomicU64::new(0));

        let handle = tokio::spawn(forward_client_loop(
            config,
            node_id,
            Arc::clone(&outbound),
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
            outbound: Arc::clone(&self.outbound),
        }
    }

    /// Try to forward a command to the leader without blocking.
    ///
    /// Returns `false` if the active segment is sealed or full (backpressure).
    #[inline]
    pub fn try_forward(&self, client_id: u32, cmd: &[u8]) -> bool {
        self.outbound.try_write(client_id, cmd)
    }

    /// Forward a command, yielding to the runtime on backpressure.
    pub async fn forward(&self, client_id: u32, cmd: &[u8]) {
        self.outbound.write(client_id, cmd).await;
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

/// Main connection loop for the forward client.
async fn forward_client_loop(
    config: ForwardConfig,
    node_id: u32,
    outbound: Arc<OutboundBuf>,
    mut target_rx: tokio::sync::watch::Receiver<Option<SocketAddr>>,
    cancel: CancellationToken,
    generation: Arc<AtomicU64>,
    client_registry: Arc<ClientRegistry>,
) {
    let mut backoff = config.reconnect_base;

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
            // No target yet — wait for update or shutdown.
            tokio::select! {
                _ = target_rx.changed() => continue,
                _ = cancel.cancelled() => return,
            }
        };

        // Connect (with cancellation).
        let conn = tokio::select! {
            c = connect_tcp(addr, &config) => c,
            _ = cancel.cancelled() => return,
        };
        let (reader, writer) = match conn {
            Ok(c) => c,
            Err(e) => {
                warn!(target = %addr, error = %e, "forward connect failed");
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {},
                    _ = cancel.cancelled() => return,
                }
                backoff = (backoff * 2).min(config.reconnect_max);
                continue;
            }
        };

        // Reset backoff on successful connect.
        backoff = config.reconnect_base;
        generation.fetch_add(1, Ordering::Release);
        debug!(target = %addr, "forward client connected");

        // Handshake: send our node_id.
        let (mut reader, mut writer) = (reader, writer);
        if let Err(e) = write_frame(&mut writer, &node_id.to_le_bytes()).await {
            warn!(error = %e, "forward handshake write failed");
            continue;
        }
        if let Err(e) = writer.flush().await {
            warn!(error = %e, "forward handshake flush failed");
            continue;
        }

        // Run duplex: outbound and inbound tasks run concurrently.
        let result = run_forward_client_connection(
            Arc::clone(&outbound),
            reader,
            writer,
            &mut target_rx,
            &cancel,
            &client_registry,
        )
        .await;

        match result {
            Ok(Reconnect::LeaderChanged) => {
                debug!("leader changed, reconnecting");
                backoff = config.reconnect_base;
            }
            Ok(Reconnect::Shutdown) => return,
            Err(e) => {
                debug!(error = %e, "forward connection lost");
            }
        }
    }
}

enum Reconnect {
    LeaderChanged,
    Shutdown,
}

/// Outbound task: drains the [`OutboundBuf`] to TCP until cancelled.
///
/// The cancellation check happens only in the `select!` at `wait_for_data`,
/// never during `drain_to_tcp`, so a drain in progress always runs to
/// completion — no partial TCP writes.
///
/// On IO error, cancels `failed` so the inbound loop can detect the failure.
async fn run_outbound_loop(
    outbound: Arc<OutboundBuf>,
    mut writer: BoxedWriter,
    stop: CancellationToken,
    failed: CancellationToken,
) -> io::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            _ = outbound.wait_for_data() => {}
        }
        // Yield once so concurrent producers can accumulate more writes into
        // the active segment before we seal and drain it.
        tokio::task::yield_now().await;
        if let Err(e) = outbound.drain_to_tcp(&mut writer).await {
            failed.cancel();
            return Err(e);
        }
    }
}

/// Run a single connection session. Returns when the connection fails,
/// the leader changes, or shutdown is requested.
///
/// Outbound and inbound run as independent concurrent tasks so that:
/// - TCP writes and reads proceed in parallel (no head-of-line blocking).
/// - `drain_to_tcp` is never cancelled mid-write (cancel is only checked
///   between drains in [`run_outbound_loop`]).
async fn run_forward_client_connection(
    outbound: Arc<OutboundBuf>,
    reader: BoxedReader,
    writer: BoxedWriter,
    target_rx: &mut tokio::sync::watch::Receiver<Option<SocketAddr>>,
    cancel: &CancellationToken,
    client_registry: &ClientRegistry,
) -> Result<Reconnect, io::Error> {
    // stop_outbound fires on global shutdown or when we want to stop the connection.
    let stop_outbound = cancel.child_token();
    // outbound_failed is cancelled by the outbound task itself on IO error.
    let outbound_failed = CancellationToken::new();

    let outbound_handle = tokio::spawn(run_outbound_loop(
        outbound,
        writer,
        stop_outbound.clone(),
        outbound_failed.clone(),
    ));

    let result =
        run_inbound_loop(reader, target_rx, cancel, &outbound_failed, client_registry).await;

    // Signal the outbound task to stop, then wait for any in-progress drain
    // to finish before we return (and the connection is dropped).
    stop_outbound.cancel();
    let _ = outbound_handle.await;

    result
}

/// Inbound loop: bulk-reads fixed-width `[client_id:4 LE][log_index:8 LE]` response
/// records (12 bytes each) from the leader and dispatches them to the owning
/// [`ClientPartition`]s via [`ClientRegistry::send_to_partition_async`].
///
/// Records are batched per partition before sending — one channel send per partition
/// per read, instead of one per record. Provides backpressure — no silent drops.
async fn run_inbound_loop(
    mut reader: BoxedReader,
    target_rx: &mut tokio::sync::watch::Receiver<Option<SocketAddr>>,
    cancel: &CancellationToken,
    outbound_failed: &CancellationToken,
    client_registry: &ClientRegistry,
) -> Result<Reconnect, io::Error> {
    // Wire format: [len=12:4 LE][client_id:4 LE][log_index:8 LE] = 16 bytes per record.
    // The leader pre-frames records so the inbound loop can broadcast the raw slab directly
    // to all partition workers without any per-record decode, re-encode, or copy.
    const RECORD_SIZE: usize = 16; // [len:4][client_id:4][log_index:8]
    let mut read_buf = BytesMut::with_capacity(1024 * 1024);
    let n = client_registry.num_partitions();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(Reconnect::Shutdown),
            _ = target_rx.changed() => return Ok(Reconnect::LeaderChanged),
            _ = outbound_failed.cancelled() => {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "outbound task failed"));
            }
            bytes_read = reader.read_buf(&mut read_buf) => {
                let bytes_read = bytes_read?;
                if bytes_read == 0 {
                    return Err(io::Error::new(io::ErrorKind::ConnectionReset, "EOF"));
                }

                // Freeze all complete 16-byte records as a single zero-copy slab, then
                // broadcast to every partition worker.  Each worker's dispatch filters by
                // client_id % n == partition_id, skipping records it doesn't own.
                // The slab stays hot in L1/L2 cache from the TCP read, so scanning
                // past non-owned records is cheap — and we eliminate all copying.
                let complete = (read_buf.len() / RECORD_SIZE) * RECORD_SIZE;
                if complete > 0 {
                    let slab = read_buf.split_to(complete).freeze();
                    if n == 1 {
                        client_registry.send_to_partition_async(0, slab).await;
                    } else {
                        // Scan once: on the first record for partition p, send
                        // slab.slice(pos..complete) — starts at that record, skipping
                        // all preceding records.  dispatch() filters the remaining
                        // records by client_id % n == p.  Zero copies; each partition
                        // receives exactly one send, beginning no earlier than needed.
                        let mut sent_mask = 0u64;
                        let mut pos = 0;
                        while pos + RECORD_SIZE <= complete {
                            let client_id = u32::from_le_bytes(
                                slab[pos + 4..pos + 8].try_into().unwrap(),
                            );
                            let p = (client_id as usize) % n;
                            if (sent_mask >> p) & 1 == 0 {
                                sent_mask |= 1 << p;
                                client_registry
                                    .send_to_partition_async(p, slab.slice(pos..complete))
                                    .await;
                            }
                            pos += RECORD_SIZE;
                        }
                    }
                }
                // Any partial record (< 16 bytes) stays in read_buf for next read.
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
    /// Live responder channel table — updated as followers connect/disconnect.
    responder_txs: SharedResponderTxs,
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

        let responder_txs: SharedResponderTxs =
            Arc::new(ArcSwap::from_pointee(Box::new(std::array::from_fn(|_| {
                None
            }))));

        let handle = tokio::spawn(accept_loop(
            config,
            listener,
            raft,
            Arc::clone(&responder_txs),
            responder_broadcast,
            cancel.clone(),
        ));

        Ok(Self {
            responder_txs,
            cancel,
            handle: Some(handle),
            local_addr,
        })
    }

    /// The actual bound local address (useful when binding to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Push pre-encoded response bytes to a connected follower's responder channel.
    ///
    /// `bytes` must contain tight-packed `[client_id:4 LE][log_index:8 LE]` records.
    /// Non-blocking — returns `false` if the follower is not connected or the channel
    /// is full.
    pub fn push_response(&self, node_id: u32, bytes: Bytes) -> bool {
        if let Some(tx) = &self.responder_txs.load()[node_id as usize] {
            tx.try_send(bytes).is_ok()
        } else {
            false
        }
    }

    /// Async variant of [`push_response`] — awaits if the channel is full (backpressure).
    pub async fn push_response_async(&self, node_id: u32, bytes: Bytes) {
        let tx = self.responder_txs.load()[node_id as usize].clone();
        if let Some(tx) = tx {
            let _ = tx.send(bytes).await;
        }
    }

    /// Return a clone of the shared responder channel table handle.
    pub fn responder_txs_handle(&self) -> SharedResponderTxs {
        Arc::clone(&self.responder_txs)
    }

    /// Number of connected follower nodes.
    pub fn connected_nodes(&self) -> usize {
        self.responder_txs
            .load()
            .iter()
            .filter(|x| x.is_some())
            .count()
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
        let responder_txs: SharedResponderTxs =
            Arc::new(ArcSwap::from_pointee(Box::new(std::array::from_fn(|_| {
                None
            }))));
        let responder_broadcast = ResponderBroadcast::new_empty();
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(accept_loop_with_channel(
            config,
            listener,
            tx,
            Arc::clone(&responder_txs),
            responder_broadcast,
            cancel.clone(),
        ));

        Ok((
            Self {
                responder_txs,
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
    responder_txs: SharedResponderTxs,
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
        let responder_txs = Arc::clone(&responder_txs);
        let responder_broadcast = responder_broadcast.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_follower_connection_with_channel(
                config,
                stream,
                peer_addr,
                tx,
                responder_txs,
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
    responder_txs: SharedResponderTxs,
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
    responder_txs_insert(&responder_txs, &responder_broadcast, node_id, resp_tx);

    let writer_cancel = cancel.clone();
    let writer_handle = tokio::spawn(async move {
        run_follower_responder(resp_rx, writer, writer_cancel).await;
    });

    let mut read_buf = BytesMut::with_capacity(128 * 1024);
    let result = loop {
        let n = tokio::select! {
            r = reader.read_buf(&mut read_buf) => r?,
            _ = cancel.cancelled() => break Ok(()),
        };
        if n == 0 {
            break Ok(());
        }
        let mut pos = 0;
        let mut count = 0u32;
        while pos + 4 <= read_buf.len() {
            let frame_len = u32::from_le_bytes(read_buf[pos..pos + 4].try_into().unwrap()) as usize;
            if frame_len < 4 || pos + 4 + frame_len > read_buf.len() {
                break;
            }
            pos += 4 + frame_len;
            count += 1;
        }
        if count > 0 {
            let frames_bytes = read_buf.split_to(pos).freeze();
            let batch = ForwardedBatch {
                node_id,
                buf: frames_bytes,
                count,
            };
            if tx.send(batch).await.is_err() {
                break Ok(()); // receiver dropped (shutdown)
            }
        }
    };

    responder_txs_remove(&responder_txs, &responder_broadcast, node_id);
    writer_handle.abort();
    result
}

/// Accept loop: listens for follower connections and spawns per-node handlers.
async fn accept_loop(
    config: ForwardConfig,
    listener: TcpListener,
    raft: Raft<MqTypeConfig>,
    responder_txs: SharedResponderTxs,
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
        let responder_txs = Arc::clone(&responder_txs);
        let responder_broadcast = responder_broadcast.clone();
        let cancel = cancel.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_follower_connection(
                config,
                stream,
                peer_addr,
                raft,
                responder_txs,
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
    responder_txs: SharedResponderTxs,
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
    responder_txs_insert(&responder_txs, &responder_broadcast, node_id, resp_tx);

    // Spawn the FollowerResponder task: drains encoded bytes to TCP.
    let writer_cancel = cancel.clone();
    let writer_handle = tokio::spawn(async move {
        run_follower_responder(resp_rx, writer, writer_cancel).await;
    });

    // TCP reader: bulk-read from follower, parse frame boundaries, send batches.
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

        // Find the last complete frame boundary.
        let mut pos = 0;
        let mut count = 0u32;
        while pos + 4 <= read_buf.len() {
            let frame_len = u32::from_le_bytes(read_buf[pos..pos + 4].try_into().unwrap()) as usize;
            if frame_len < 4 || pos + 4 + frame_len > read_buf.len() {
                break; // short or partial frame
            }
            pos += 4 + frame_len;
            count += 1;
        }

        if count > 0 {
            // Freeze the complete portion once — single refcount for the batch.
            let frames_bytes = read_buf.split_to(pos).freeze();
            // Wrap into TAG_FORWARDED_BATCH and propose to raft directly.
            // Fire-and-forget: proposal happens in a spawned task so the TCP
            // reader is never blocked by raft commit latency.
            let cmd = MqCommand::forwarded_batch(node_id, count, &frames_bytes);
            let raft2 = raft.clone();
            tokio::spawn(async move {
                if let Err(e) = raft2.client_write(cmd).await {
                    debug!(error = %e, "forwarded batch proposal failed");
                }
            });
        }
        // Partial frame data remains in read_buf for next read.
    };

    // Cleanup: remove responder channel, abort writer task.
    responder_txs_remove(&responder_txs, &responder_broadcast, node_id);
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
                if !is_done && msg.len() >= 8 {
                    // NodeRoute frame payload: [log_index:8 LE]
                    let li = u64::from_le_bytes(msg[0..8].try_into().unwrap());
                    log_idx_cb.store(li, std::sync::atomic::Ordering::Relaxed);
                    done_cb.notify_one();
                }
            });
        let local_cid = client_registry.register(cb);

        let mut client =
            ForwardClient::start(config.clone(), 1, Some(addr), Arc::clone(&client_registry));

        // Give connection time to establish.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Forward a command.
        client.forward(local_cid, b"test cmd").await;

        // Acceptor should receive it.
        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.node_id, 1);
        let (cid, cmd_bytes) = batch.iter().next().unwrap();
        assert_eq!(cid, local_cid);
        assert_eq!(cmd_bytes, b"test cmd");

        // Send response back via push_response_async (pre-framed: [12u32][cid][log_index]).
        let mut resp = BytesMut::with_capacity(16);
        resp.put_u32_le(12u32);
        resp.put_u32_le(local_cid);
        resp.put_u64_le(42u64);
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
        let mut client =
            ForwardClient::start(config.clone(), 2, Some(addr), Arc::clone(&client_registry));

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
        client.forward(1, b"after reconnect").await;

        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (_, cmd_bytes) = batch.iter().next().unwrap();
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
        let mut client =
            ForwardClient::start(config.clone(), 3, Some(addr), Arc::clone(&client_registry));

        // No acceptor yet — buffer commands (client_id is u32 now).
        for i in 0u32..5 {
            client.try_forward(i, &i.to_le_bytes());
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
        let mut client =
            ForwardClient::start(config.clone(), 4, Some(addr1), Arc::clone(&client_registry));

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Forward to first leader.
        client.forward(1, b"to leader 1").await;
        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx1.recv())
            .await
            .unwrap()
            .unwrap();
        let (_, cmd_bytes) = batch.iter().next().unwrap();
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
        client.forward(1, b"to leader 2").await;
        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx2.recv())
            .await
            .unwrap()
            .unwrap();
        let (_, cmd_bytes) = batch.iter().next().unwrap();
        assert_eq!(cmd_bytes, b"to leader 2");

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
            let client = ForwardClient::start(config.clone(), 10 + i, Some(addr), reg);
            clients.push(client);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Each follower sends a command.
        for (i, client) in clients.iter().enumerate() {
            let msg = format!("from node {}", 10 + i as u32);
            client.forward(1, msg.as_bytes()).await;
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
                if !is_done && msg.len() >= 8 {
                    // NodeRoute frame payload: [log_index:8 LE]
                    log1c.store(
                        u64::from_le_bytes(msg[0..8].try_into().unwrap()),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    done1c.notify_one();
                }
            });
        let (done2c, log2c) = (std::sync::Arc::clone(&done2), std::sync::Arc::clone(&log2));
        let cb2: crate::async_apply::ResponseCallback =
            std::sync::Arc::new(move |_slab, _offset, msg, is_done| {
                if !is_done && msg.len() >= 8 {
                    log2c.store(
                        u64::from_le_bytes(msg[0..8].try_into().unwrap()),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    done2c.notify_one();
                }
            });
        let cid1 = client_registry.register(cb1);
        let cid2 = client_registry.register(cb2);

        let mut client =
            ForwardClient::start(config.clone(), 5, Some(addr), Arc::clone(&client_registry));

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Forward a command to establish the route.
        client.forward(cid1, b"cmd1").await;
        let _ = tokio::time::timeout(Duration::from_millis(200), batch_rx.recv()).await;

        // Send responses via push_response_async (pre-framed: [12u32][cid][log_index]).
        let mut resp = BytesMut::with_capacity(32);
        resp.put_u32_le(12u32);
        resp.put_u32_le(cid1);
        resp.put_u64_le(100u64);
        resp.put_u32_le(12u32);
        resp.put_u32_le(cid2);
        resp.put_u64_le(200u64);
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
}
