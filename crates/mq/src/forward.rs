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
//! - Outbound: commands from local clients are written lock-free into
//!   partitioned [`OutboundSeg`] buffers, then drained to TCP as batch frames.
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
//! [batch_seq:8 LE][raft_group_id:4 LE][batch_payload_len:4 LE][ack_seq:8 LE][sub-frames...]
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

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex as ParkingMutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::async_apply::{ClientRegistry, ResponderBroadcast, ResponderUpdateMsg};
use crate::raft_writer::RaftWriter;
use crate::types::{MqCommand, read_u32_le, read_u64_le};

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

/// Drain-batch header size on the wire:
/// `[batch_seq:8][raft_group_id:4][batch_payload_len:4][ack_seq:8]`
///
/// `ack_seq` is a cumulative acknowledgement piggybacked on every outbound
/// batch — the follower echoes the highest response-slab counter it has
/// received from the leader.  This rides the same TCP write as the data,
/// adding zero extra syscalls.
const BATCH_HEADER_LEN: usize = 24;

// ---------------------------------------------------------------------------
// Lock-free outbound partition buffer
// ---------------------------------------------------------------------------

/// Capacity of each outbound segment. 1 MiB fits many sub-frames.
const OUTBOUND_SEG_CAPACITY: u32 = 1 << 20; // 1 MiB

/// Bit 31 of `write_pos` is set by the drainer to seal the segment.
const OUTBOUND_SEALED_BIT: u32 = 1 << 31;

// Sub-frame format (directly usable as TCP wire frames):
//   [payload_len: 4 LE][client_id: 4 LE][request_seq: 8 LE][cmd_bytes...]
//   payload_len = 12 + cmd.len()
//   total frame  = 16 + cmd.len() bytes

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
}

impl OutboundWaker {
    #[inline]
    fn wake(&self) {
        if !self.pending.swap(true, Ordering::Release) {
            self.notify.notify_waiters();
        }
    }
}

/// Shared lock-free outbound buffer — held by `ForwardClient`/`ForwardHandle`
/// writers and the background drain task.
///
/// Writers call [`try_write`] or [`write`] to append sub-frames lock-free.
/// The drain loop calls [`drain_to_tcp`] after [`wait_for_data`] wakes it.
pub struct OutboundBuf {
    partitions: Vec<Arc<OutboundPartition>>,
    waker: OutboundWaker,
    /// Wakes writers blocked on a sealed/full segment after `collect_slabs()`
    /// installs fresh segments.
    writer_waker: tokio::sync::Notify,
    mask: u32,
    /// Raft group ID embedded in batch headers.
    raft_group_id: u32,
    /// Monotonically increasing batch sequence counter for dedup.
    next_batch_seq: AtomicU64,
    /// Cumulative ACK: highest response-slab counter received from the leader.
    /// Set by the inbound loop, read by `drain_to_tcp` and stamped into the
    /// batch header — zero extra syscalls.
    ack_seq: AtomicU64,
}

impl OutboundBuf {
    fn new(num_partitions: usize, raft_group_id: u32) -> Self {
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
            },
            writer_waker: tokio::sync::Notify::new(),
            mask: (n - 1) as u32,
            raft_group_id,
            next_batch_seq: AtomicU64::new(0),
            ack_seq: AtomicU64::new(0),
        }
    }

    /// Try to write one sub-frame to the partition for `client_id`.
    ///
    /// Returns `false` if the segment is sealed or full (backpressure).
    #[inline]
    pub fn try_write(&self, client_id: u32, request_seq: u64, cmd: &[u8]) -> bool {
        let entry_size = (16 + cmd.len()) as u32;
        let payload_len = (12 + cmd.len()) as u32;
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
                (dst.add(8) as *mut u64).write_unaligned(request_seq.to_le());
                std::ptr::copy_nonoverlapping(cmd.as_ptr(), dst.add(16), cmd.len());
            }

            seg.committed.fetch_add(entry_size, Ordering::Release);
            self.waker.wake();
            return true;
        }
    }

    /// Async write — spins briefly then waits on Notify for segment availability.
    #[inline]
    pub async fn write(&self, client_id: u32, request_seq: u64, cmd: &[u8]) {
        // Fast path: usually succeeds on the first try.
        if self.try_write(client_id, request_seq, cmd) {
            return;
        }
        // Brief spin: the drainer installs new segments very quickly after sealing.
        for _ in 0..8 {
            std::hint::spin_loop();
            if self.try_write(client_id, request_seq, cmd) {
                return;
            }
        }
        // Slow path: register for notification and wait.
        loop {
            let notified = self.writer_waker.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.try_write(client_id, request_seq, cmd) {
                return;
            }
            notified.await;
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

    /// Seal all partitions, install fresh segments (unblocking producers immediately),
    /// spin-wait for in-flight writes, and return the collected slabs with a pre-built
    /// batch header. Returns `None` if all partitions were empty.
    ///
    /// This is the fast, synchronous half of the drain: no I/O happens here.
    /// The caller is responsible for writing the returned [`DrainBatch`] to TCP.
    fn collect_slabs(&self) -> Option<DrainBatch> {
        let mut slabs: Vec<Bytes> = Vec::new();
        let mut total_payload = 0usize;

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
                bm.split().freeze()
            };
            // Drop the old segment now that we own `data`.
            unsafe { drop(Box::from_raw(old_ptr)) };

            total_payload += data.len();
            slabs.push(data);
        }

        // Wake writers that were blocked on sealed/full segments.
        self.writer_waker.notify_waiters();

        if total_payload == 0 {
            return None;
        }

        // Build batch header: [batch_seq:8][raft_group_id:4][payload_len:4][ack_seq:8]
        let batch_seq = self.next_batch_seq.fetch_add(1, Ordering::Relaxed);
        let ack = self.ack_seq.load(Ordering::Relaxed);
        let mut header = [0u8; BATCH_HEADER_LEN];
        header[0..8].copy_from_slice(&batch_seq.to_le_bytes());
        header[8..12].copy_from_slice(&self.raft_group_id.to_le_bytes());
        header[12..16].copy_from_slice(&(total_payload as u32).to_le_bytes());
        header[16..24].copy_from_slice(&ack.to_le_bytes());

        Some(DrainBatch { header, slabs })
    }
}

/// A batch of sealed outbound slabs with a pre-built wire header, ready to write to TCP.
struct DrainBatch {
    header: [u8; BATCH_HEADER_LEN],
    slabs: Vec<Bytes>,
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
    let client_id = read_u32_le(buf, 0).ok()?;
    Some((client_id, &buf[4..]))
}

// =============================================================================
// Raft backlog budget
// =============================================================================

/// Shared budget counter for bytes queued for raft apply.
///
/// The leader creates one `RaftBacklog` and shares it with:
/// - [`ForwardAcceptor`] / follower connection handlers (charge on TCP read)
/// - [`AsyncApplyManager`](crate::async_apply::AsyncApplyManager) workers (release after apply)
/// - [`MqWriteBatcher`](crate::write_batcher::MqWriteBatcher) / local batcher (charge on propose)
///
/// Built on [`tokio::sync::Semaphore`] for fair (FIFO) and atomic
/// wait-and-charge semantics.  When the budget is exhausted, producers
/// block in FIFO order until capacity is released.
pub struct RaftBacklog {
    semaphore: tokio::sync::Semaphore,
    notify: tokio::sync::Notify,
    max: usize,
}

impl RaftBacklog {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::new(max_bytes),
            notify: tokio::sync::Notify::new(),
            max: max_bytes,
        }
    }

    /// Atomically wait for and claim `bytes` of budget.
    ///
    /// Fair: waiters are served in FIFO order.  The call blocks until
    /// enough budget has been released, then atomically reserves `bytes`.
    pub async fn charge(&self, bytes: usize) {
        let permit = self.semaphore.acquire_many(bytes as u32).await.unwrap();
        permit.forget();
    }

    /// Try to charge `bytes` without blocking.
    ///
    /// Returns `true` if the budget was available and claimed.
    /// Returns `false` if insufficient capacity (no bytes charged).
    #[inline]
    pub fn try_charge(&self, bytes: usize) -> bool {
        match self.semaphore.try_acquire_many(bytes as u32) {
            Ok(permit) => {
                permit.forget();
                true
            }
            Err(_) => false,
        }
    }

    /// Remove bytes from the backlog (after raft apply or on proposal failure).
    #[inline]
    pub fn release(&self, bytes: usize) {
        self.semaphore.add_permits(bytes);
        self.notify.notify_waiters();
    }

    /// Current backlog in bytes.
    #[inline]
    pub fn current(&self) -> usize {
        self.max - self.semaphore.available_permits()
    }

    /// Maximum budget in bytes.
    #[inline]
    pub fn max(&self) -> usize {
        self.max
    }

    /// Wait until the backlog drains to zero.
    ///
    /// Woken by [`release()`](Self::release) — no polling or sleeping.
    pub async fn wait_for_drain(&self) {
        loop {
            // Register interest *before* checking the condition so a
            // release() between the check and the await is not lost.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.current() == 0 {
                return;
            }
            notified.await;
        }
    }
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
    /// Maximum byte size of a single TCP batch (and consequently a single Raft
    /// log entry on the acceptor side).  The drain loop aggregates channel
    /// receives until this threshold, then cuts.  Default: 512 KiB.
    pub max_batch_bytes: usize,
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
            max_batch_bytes: crate::write_batcher::DEFAULT_MAX_RAFT_ENTRY_BYTES,
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
/// them to a follower's TCP socket — zero per-record allocation.
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

// ---------------------------------------------------------------------------
// FollowerDedup — leader-side deduplication per follower
// ---------------------------------------------------------------------------

/// Leader-side deduplication state for a single follower node.
struct FollowerDedup {
    high_water: u64,
    above_hwm: HashSet<u64>,
}

impl FollowerDedup {
    fn new() -> Self {
        Self {
            high_water: u64::MAX,
            above_hwm: HashSet::new(),
        }
    }

    fn check_and_record(&mut self, batch_seq: u64) -> bool {
        if self.high_water == u64::MAX {
            self.high_water = batch_seq;
            self.advance_hwm();
            return true;
        }
        if batch_seq <= self.high_water {
            return false;
        }
        if self.above_hwm.contains(&batch_seq) {
            return false;
        }
        self.above_hwm.insert(batch_seq);
        self.advance_hwm();
        true
    }

    fn advance_hwm(&mut self) {
        loop {
            let next = self.high_water.wrapping_add(1);
            if self.above_hwm.remove(&next) {
                self.high_water = next;
            } else {
                break;
            }
        }
    }
}

// =============================================================================
// ForwardClient (follower side)
// =============================================================================

/// Pre-packed sub-frames ready for the drain loop.
///
/// Writers pack sub-frames into a `BytesMut` using the standard sub-frame
/// wire format (`[payload_len:4][client_id:4][request_seq:8][cmd_bytes...]`)
/// and send the frozen `Bytes` over a channel.  The drain loop aggregates
/// one or more of these into a single TCP batch frame — zero copies of
/// command bytes.
pub struct ForwardFrameBatch {
    pub bytes: Bytes,
    pub count: u32,
}

/// Type alias for the crossfire multi-producer async sender.
type FrameTx = crossfire::MAsyncTx<crossfire::mpsc::Array<ForwardFrameBatch>>;
/// Type alias for the crossfire async receiver.
type FrameRx = crossfire::AsyncRx<crossfire::mpsc::Array<ForwardFrameBatch>>;

/// Clone-able writer handle for submitting [`ForwardFrameBatch`]es to a
/// [`ForwardClient`].
///
/// Obtained via [`ForwardClient::writer`].  Multiple connection tasks may
/// hold independent clones; the underlying lock-free crossfire MPSC sender
/// handles concurrent sends without contention.
#[derive(Clone)]
pub struct ForwardWriter {
    tx: FrameTx,
}

impl ForwardWriter {
    /// Send a batch of pre-framed sub-frames to the forward drain loop.
    pub async fn send(
        &self,
        batch: ForwardFrameBatch,
    ) -> Result<(), crossfire::SendError<ForwardFrameBatch>> {
        self.tx.send(batch).await
    }
}

/// Follower-side forwarding client.
pub struct ForwardClient {
    frame_tx: FrameTx,
    target_tx: tokio::sync::watch::Sender<Option<SocketAddr>>,
    cancel: CancellationToken,
    handle: Option<JoinHandle<()>>,
    pub generation: Arc<AtomicU64>,
}

/// Cheap, cloneable handle for sending commands through a [`ForwardClient`].
#[derive(Clone)]
pub struct ForwardHandle {
    frame_tx: FrameTx,
}

impl ForwardHandle {
    /// Pack and send a single sub-frame.
    ///
    /// For high throughput, prefer [`ForwardClient::writer`] which lets callers
    /// pre-pack batches and amortise channel overhead.
    pub async fn forward(&self, client_id: u32, request_seq: u64, cmd: &[u8]) {
        let payload_len = (12 + cmd.len()) as u32;
        let entry_size = 16 + cmd.len();
        let mut buf = BytesMut::with_capacity(entry_size);
        buf.put_u32_le(payload_len);
        buf.put_u32_le(client_id);
        buf.put_u64_le(request_seq);
        buf.put_slice(cmd);
        let _ = self
            .frame_tx
            .send(ForwardFrameBatch {
                bytes: buf.freeze(),
                count: 1,
            })
            .await;
    }
}

/// Default capacity for the crossfire frame channel between writers and the drain loop.
const FORWARD_FRAME_CHANNEL_CAP: usize = 1024;

impl ForwardClient {
    pub fn start(
        config: ForwardConfig,
        node_id: u32,
        raft_group_id: u32,
        initial_target: Option<SocketAddr>,
        client_registry: Arc<ClientRegistry>,
    ) -> Self {
        let (frame_tx, frame_rx) =
            crossfire::mpsc::bounded_async::<ForwardFrameBatch>(FORWARD_FRAME_CHANNEL_CAP);
        let (target_tx, target_rx) = tokio::sync::watch::channel(initial_target);
        let cancel = CancellationToken::new();
        let generation = Arc::new(AtomicU64::new(0));

        let handle = tokio::spawn(forward_client_loop(
            config,
            node_id,
            raft_group_id,
            frame_rx,
            target_rx,
            cancel.clone(),
            generation.clone(),
            client_registry,
        ));

        Self {
            frame_tx,
            target_tx,
            cancel,
            handle: Some(handle),
            generation,
        }
    }

    /// Return a clone-able [`ForwardWriter`] for batch sends.
    pub fn writer(&self) -> ForwardWriter {
        ForwardWriter {
            tx: self.frame_tx.clone(),
        }
    }

    /// Return a clone-able [`ForwardHandle`] for single-frame sends.
    pub fn clone_handle(&self) -> ForwardHandle {
        ForwardHandle {
            frame_tx: self.frame_tx.clone(),
        }
    }

    /// Pack and send a single sub-frame (convenience wrapper).
    pub async fn forward(&self, client_id: u32, request_seq: u64, cmd: &[u8]) {
        let payload_len = (12 + cmd.len()) as u32;
        let entry_size = 16 + cmd.len();
        let mut buf = BytesMut::with_capacity(entry_size);
        buf.put_u32_le(payload_len);
        buf.put_u32_le(client_id);
        buf.put_u64_le(request_seq);
        buf.put_slice(cmd);
        let _ = self
            .frame_tx
            .send(ForwardFrameBatch {
                bytes: buf.freeze(),
                count: 1,
            })
            .await;
    }

    pub fn set_target(&self, addr: Option<SocketAddr>) {
        let _ = self.target_tx.send(addr);
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Graceful drain: close the frame channel so the drain loop processes
    /// all queued frames, flushes TCP, and the connection closes naturally.
    /// The acceptor reads all remaining data and charges backlog before this
    /// returns.
    ///
    /// After this, `shutdown()` is a no-op (handle already joined).
    pub async fn drain(&mut self) {
        // Replace frame_tx with a dummy — drops the original sender,
        // closing the channel.  The drain loop processes remaining frames
        // until recv() returns Err, then exits.  TCP write loop flushes
        // and closes.  Acceptor reads remaining data + EOF.  Inbound loop
        // sees EOF and exits.  forward_client_loop returns.
        let (_dummy_tx, _dummy_rx) = crossfire::mpsc::bounded_async::<ForwardFrameBatch>(1);
        self.frame_tx = _dummy_tx;
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

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
    raft_group_id: u32,
    mut frame_rx: FrameRx,
    mut target_rx: tokio::sync::watch::Receiver<Option<SocketAddr>>,
    cancel: CancellationToken,
    generation: Arc<AtomicU64>,
    client_registry: Arc<ClientRegistry>,
) {
    let mut backoff = config.reconnect_base;
    let ack_seq = Arc::new(AtomicU64::new(0));

    loop {
        if cancel.is_cancelled() {
            return;
        }

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

        backoff = config.reconnect_base;
        generation.fetch_add(1, Ordering::Release);
        debug!(target = %addr, "forward client connected");

        let (reader, mut writer) = (reader, writer);
        if let Err(e) = write_frame(&mut writer, &node_id.to_le_bytes()).await {
            warn!(error = %e, "forward handshake write failed");
            continue;
        }
        if let Err(e) = writer.flush().await {
            warn!(error = %e, "forward handshake flush failed");
            continue;
        }

        let (result, rx_back) = run_forward_client_connection(
            raft_group_id,
            config.max_batch_bytes,
            frame_rx,
            Arc::clone(&ack_seq),
            reader,
            writer,
            &mut target_rx,
            &cancel,
            &client_registry,
        )
        .await;
        frame_rx = rx_back;

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

/// Capacity of the channel between the drain task and the TCP write task.
/// Each slot holds ~1 MB of sealed slab data; 64 slots ≈ 64 MB of in-flight buffer,
/// enough to absorb TCP write latency spikes without stalling producers.
const OUTBOUND_WRITE_CHANNEL_CAP: usize = 64;

/// Drain task: receives [`ForwardFrameBatch`]es from writers and aggregates
/// them into [`DrainBatch`]es for the TCP write task.
///
/// Writers are completely independent — each packs sub-frames into its own
/// buffer and sends over the shared MPSC channel.  The drain loop does
/// `recv()` + `try_recv()` to batch multiple sends into one TCP write.
///
/// Returns the `frame_rx` receiver so the connection loop can reuse it
/// across reconnections.
async fn run_drain_loop(
    frame_rx: FrameRx,
    batch_tx: tokio::sync::mpsc::Sender<DrainBatch>,
    raft_group_id: u32,
    max_batch_bytes: usize,
    ack_seq: Arc<AtomicU64>,
    stop: CancellationToken,
    failed: CancellationToken,
) -> FrameRx {
    let mut next_batch_seq: u64 = 0;
    loop {
        let first = tokio::select! {
            biased;
            _ = stop.cancelled() => return frame_rx,
            _ = failed.cancelled() => return frame_rx,
            frame = frame_rx.recv() => match frame {
                Ok(f) => f,
                Err(_) => return frame_rx, // all senders dropped
            },
        };

        let mut slabs: Vec<Bytes> = vec![first.bytes];
        let mut total_payload: usize = slabs[0].len();

        // Non-blockingly drain all immediately available frames up to byte limit.
        while total_payload < max_batch_bytes {
            match frame_rx.try_recv() {
                Ok(more) => {
                    total_payload += more.bytes.len();
                    slabs.push(more.bytes);
                }
                Err(_) => break,
            }
        }

        // Build batch header: [batch_seq:8][raft_group_id:4][payload_len:4][ack_seq:8]
        let ack = ack_seq.load(Ordering::Relaxed);
        let mut header = [0u8; BATCH_HEADER_LEN];
        header[0..8].copy_from_slice(&next_batch_seq.to_le_bytes());
        header[8..12].copy_from_slice(&raft_group_id.to_le_bytes());
        header[12..16].copy_from_slice(&(total_payload as u32).to_le_bytes());
        header[16..24].copy_from_slice(&ack.to_le_bytes());
        next_batch_seq += 1;

        if batch_tx.send(DrainBatch { header, slabs }).await.is_err() {
            failed.cancel();
            return frame_rx;
        }
    }
}

/// TCP write task: receives [`DrainBatch`]es and writes them to the wire.
///
/// Runs independently of the drain loop so that slow TCP writes never delay
/// segment installation and producer progress.
async fn run_tcp_write_loop(
    mut batch_rx: tokio::sync::mpsc::Receiver<DrainBatch>,
    mut writer: BoxedWriter,
    failed: CancellationToken,
) {
    while let Some(batch) = batch_rx.recv().await {
        if let Err(e) = write_drain_batch(&mut writer, &batch).await {
            debug!(error = %e, "outbound TCP write failed");
            failed.cancel();
            return;
        }
    }
}

async fn write_drain_batch(writer: &mut BoxedWriter, batch: &DrainBatch) -> io::Result<()> {
    writer.write_all(&batch.header).await?;
    for slab in &batch.slabs {
        writer.write_all(slab).await?;
    }
    writer.flush().await
}

/// Run a single connection session.
///
/// Spawns two outbound tasks:
/// - **drain task**: receives frame batches from writers and aggregates into TCP batches
/// - **TCP write task**: drains the channel and writes batches to the wire
///
/// Returns the connection result and the `frame_rx` receiver (for reuse across reconnects).
async fn run_forward_client_connection(
    raft_group_id: u32,
    max_batch_bytes: usize,
    frame_rx: FrameRx,
    ack_seq: Arc<AtomicU64>,
    reader: BoxedReader,
    writer: BoxedWriter,
    target_rx: &mut tokio::sync::watch::Receiver<Option<SocketAddr>>,
    cancel: &CancellationToken,
    client_registry: &ClientRegistry,
) -> (Result<Reconnect, io::Error>, FrameRx) {
    let stop_drain = cancel.child_token();
    let outbound_failed = CancellationToken::new();

    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<DrainBatch>(OUTBOUND_WRITE_CHANNEL_CAP);

    let drain_handle = tokio::spawn(run_drain_loop(
        frame_rx,
        batch_tx,
        raft_group_id,
        max_batch_bytes,
        Arc::clone(&ack_seq),
        stop_drain.clone(),
        outbound_failed.clone(),
    ));

    let tcp_write_handle = tokio::spawn(run_tcp_write_loop(
        batch_rx,
        writer,
        outbound_failed.clone(),
    ));

    let result = run_inbound_loop(
        reader,
        target_rx,
        cancel,
        &outbound_failed,
        client_registry,
        &ack_seq,
    )
    .await;

    stop_drain.cancel();
    // Drain task exits, returns frame_rx, drops batch_tx → tcp_write_loop exits cleanly.
    let frame_rx = drain_handle.await.unwrap();
    let _ = tcp_write_handle.await;

    (result, frame_rx)
}

/// Inbound loop: reads length-prefixed response frames from the leader.
/// Bumps `ack_seq` after each successful dispatch so the next
/// outbound batch header piggybacks the cumulative ACK.
async fn run_inbound_loop(
    mut reader: BoxedReader,
    target_rx: &mut tokio::sync::watch::Receiver<Option<SocketAddr>>,
    cancel: &CancellationToken,
    outbound_failed: &CancellationToken,
    client_registry: &ClientRegistry,
    ack_seq: &AtomicU64,
) -> Result<Reconnect, io::Error> {
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

                let buf = read_buf.as_ref();
                let mut complete = 0;
                let mut sent_mask = 0u64;
                let mut sent_count = 0usize;

                let mut scan = 0;
                while scan + 4 <= buf.len() {
                    let frame_len =
                        read_u32_le(buf, scan).unwrap_or(0) as usize;
                    let next = scan + 4 + frame_len;
                    if frame_len == 0 || next > buf.len() {
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
                            let frame_len = read_u32_le(&slab, pos)
                                .unwrap_or(0) as usize;
                            if frame_len < 4 || pos + 4 + frame_len > complete {
                                break;
                            }
                            let client_id = read_u32_le(&slab, pos + 4)
                                .unwrap_or(0);
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

                    // Bump the cumulative ACK — next outbound batch will
                    // piggyback this in its header at zero extra cost.
                    ack_seq.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

// =============================================================================
// ForwardAcceptor (leader side)
// =============================================================================

/// Leader-side acceptor for forwarded connections from followers.
pub struct ForwardAcceptor {
    responder_txs: Box<[Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>; 64]>,
    update_rx: tokio::sync::mpsc::UnboundedReceiver<ResponderUpdateMsg>,
    cancel: CancellationToken,
    handle: Option<JoinHandle<()>>,
    local_addr: SocketAddr,
    /// Shared Raft write pool. `None` for the bench/channel path.
    raft_writer: Option<Arc<RaftWriter>>,
}

impl ForwardAcceptor {
    pub async fn start(
        config: ForwardConfig,
        bind_addr: SocketAddr,
        raft_writer: Arc<RaftWriter>,
        responder_broadcast: ResponderBroadcast,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let (update_tx, update_rx) = tokio::sync::mpsc::unbounded_channel();

        let handle = tokio::spawn(accept_loop(
            config,
            listener,
            Arc::clone(&raft_writer),
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
            raft_writer: Some(raft_writer),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Shared raft backlog budget from the underlying [`RaftWriter`], if present.
    pub fn backlog(&self) -> Option<&Arc<RaftBacklog>> {
        self.raft_writer.as_ref().map(|rw| rw.backlog())
    }

    fn drain_updates(&mut self) {
        while let Ok((node_id, tx)) = self.update_rx.try_recv() {
            self.responder_txs[node_id as usize] = tx;
        }
    }

    pub fn push_response(&mut self, node_id: u32, bytes: Bytes) -> bool {
        self.drain_updates();
        if let Some(tx) = &self.responder_txs[node_id as usize] {
            tx.try_send(bytes).is_ok()
        } else {
            false
        }
    }

    pub async fn push_response_async(&mut self, node_id: u32, bytes: Bytes) {
        self.drain_updates();
        if let Some(tx) = &self.responder_txs[node_id as usize] {
            let _ = tx.send(bytes).await;
        }
    }

    pub fn connected_nodes(&mut self) -> usize {
        self.drain_updates();
        self.responder_txs.iter().filter(|x| x.is_some()).count()
    }

    pub async fn shutdown(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

async fn accept_loop(
    config: ForwardConfig,
    listener: TcpListener,
    raft_writer: Arc<RaftWriter>,
    update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
    responder_broadcast: ResponderBroadcast,
    cancel: CancellationToken,
) {
    let dedup_table: Arc<ParkingMutex<HashMap<u32, FollowerDedup>>> =
        Arc::new(ParkingMutex::new(HashMap::new()));
    let leader_seq = Arc::new(AtomicU64::new(0));
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
        let raft_writer = Arc::clone(&raft_writer);
        let update_tx = update_tx.clone();
        let responder_broadcast = responder_broadcast.clone();
        let cancel = cancel.clone();
        let dedup_table = Arc::clone(&dedup_table);
        let leader_seq = Arc::clone(&leader_seq);
        tokio::spawn(async move {
            if let Err(e) = handle_follower_connection(
                config,
                stream,
                peer_addr,
                raft_writer,
                update_tx,
                responder_broadcast,
                cancel,
                dedup_table,
                leader_seq,
            )
            .await
            {
                debug!(peer = %peer_addr, error = %e, "follower connection ended");
            }
        });
    }
}

async fn handle_follower_connection(
    config: ForwardConfig,
    stream: TcpStream,
    peer_addr: SocketAddr,
    raft_writer: Arc<RaftWriter>,
    update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
    responder_broadcast: ResponderBroadcast,
    cancel: CancellationToken,
    dedup_table: Arc<ParkingMutex<HashMap<u32, FollowerDedup>>>,
    leader_seq: Arc<AtomicU64>,
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
    let node_id = read_u32_le(&handshake_buf, 0)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    info!(node_id, peer = %peer_addr, "follower connected for forwarding");

    let (resp_tx, resp_rx) =
        crossfire::mpsc::bounded_async::<Bytes>(config.responder_channel_capacity);
    responder_txs_insert(&update_tx, &responder_broadcast, node_id, resp_tx);

    let writer_cancel = cancel.clone();
    let writer_handle = tokio::spawn(async move {
        run_follower_responder(resp_rx, writer, writer_cancel).await;
    });

    let backlog = raft_writer.backlog();
    let tx = raft_writer.clone_tx();
    let mut read_buf = BytesMut::with_capacity(128 * 1024);
    let result = loop {
        let n = tokio::select! {
            r = reader.read_buf(&mut read_buf) => r?,
            _ = cancel.cancelled() => break Ok(()),
        };
        if n == 0 {
            break Ok(());
        }

        while read_buf.len() >= BATCH_HEADER_LEN {
            let batch_seq = read_u64_le(&read_buf, 0)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            let _raft_group_id = read_u32_le(&read_buf, 8)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            let batch_payload_len = read_u32_le(&read_buf, 12)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
                as usize;
            let _ack_seq = read_u64_le(&read_buf, 16)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            let needed = BATCH_HEADER_LEN + batch_payload_len;
            if read_buf.len() < needed {
                break;
            }

            let _ = read_buf.split_to(BATCH_HEADER_LEN);
            let payload = read_buf.split_to(batch_payload_len).freeze();

            let is_new = dedup_table
                .lock()
                .entry(node_id)
                .or_insert_with(FollowerDedup::new)
                .check_and_record(batch_seq);
            if !is_new {
                continue;
            }

            let mut sub_pos = 0;
            let mut count = 0u32;
            while sub_pos + 4 <= payload.len() {
                let plen = read_u32_le(&payload, sub_pos)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
                    as usize;
                if plen < 12 || sub_pos + 4 + plen > payload.len() {
                    break;
                }
                sub_pos += 4 + plen;
                count += 1;
            }

            if count > 0 {
                let seq = leader_seq.fetch_add(1, Ordering::Relaxed);
                let mut scratch = BytesMut::new();
                MqCommand::write_forwarded_batch(
                    &mut scratch,
                    node_id,
                    count,
                    batch_seq,
                    seq,
                    &payload,
                );
                let cmd = MqCommand::split_from(&mut scratch);
                let cmd_len = cmd.total_encoded_size();
                // Fair atomic charge — blocks in FIFO order when over budget.
                tokio::select! {
                    _ = backlog.charge(cmd_len) => {}
                    _ = cancel.cancelled() => break,
                }
                if let Some(ref tx) = tx {
                    let _ = tx.send((cmd, cmd_len)).await;
                }
            }
        }
    };

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

    // =========================================================================
    // Test-only: channel-based acceptor for unit tests that need a
    // controllable server side without a real Raft instance.
    // =========================================================================

    struct ForwardedBatch {
        node_id: u32,
        #[allow(dead_code)]
        raft_group_id: u32,
        buf: Bytes,
        count: u32,
    }

    impl ForwardedBatch {
        fn len(&self) -> usize {
            self.count as usize
        }

        fn iter(&self) -> ForwardedBatchIter<'_> {
            ForwardedBatchIter {
                buf: &self.buf,
                pos: 0,
            }
        }
    }

    struct ForwardedBatchIter<'a> {
        buf: &'a [u8],
        pos: usize,
    }

    impl<'a> Iterator for ForwardedBatchIter<'a> {
        type Item = (u32, u64, &'a [u8]);

        fn next(&mut self) -> Option<Self::Item> {
            if self.pos + 4 > self.buf.len() {
                return None;
            }
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
    }

    impl ForwardAcceptor {
        async fn start_with_channel(
            config: ForwardConfig,
            bind_addr: SocketAddr,
        ) -> io::Result<(Self, tokio::sync::mpsc::Receiver<ForwardedBatch>)> {
            let (tx, rx) = tokio::sync::mpsc::channel(256);
            let listener = TcpListener::bind(bind_addr).await?;
            let local_addr = listener.local_addr()?;
            let responder_broadcast = ResponderBroadcast::new_empty();
            let cancel = CancellationToken::new();
            let (update_tx, update_rx) = tokio::sync::mpsc::unbounded_channel();

            let handle = tokio::spawn(test_accept_loop(
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
                    raft_writer: None,
                },
                rx,
            ))
        }
    }

    async fn test_accept_loop(
        config: ForwardConfig,
        listener: TcpListener,
        tx: tokio::sync::mpsc::Sender<ForwardedBatch>,
        update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
        responder_broadcast: ResponderBroadcast,
        cancel: CancellationToken,
    ) {
        let dedup_table: Arc<ParkingMutex<HashMap<u32, FollowerDedup>>> =
            Arc::new(ParkingMutex::new(HashMap::new()));
        loop {
            let accept = tokio::select! {
                a = listener.accept() => a,
                _ = cancel.cancelled() => return,
            };
            let (stream, peer_addr) = match accept {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, "test accept error");
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
            let dedup_table = Arc::clone(&dedup_table);
            tokio::spawn(async move {
                if let Err(e) = test_handle_connection(
                    config,
                    stream,
                    tx,
                    update_tx,
                    responder_broadcast,
                    cancel,
                    dedup_table,
                )
                .await
                {
                    debug!(peer = %peer_addr, error = %e, "test connection ended");
                }
            });
        }
    }

    async fn test_handle_connection(
        config: ForwardConfig,
        stream: tokio::net::TcpStream,
        tx: tokio::sync::mpsc::Sender<ForwardedBatch>,
        update_tx: tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>,
        responder_broadcast: ResponderBroadcast,
        cancel: CancellationToken,
        dedup_table: Arc<ParkingMutex<HashMap<u32, FollowerDedup>>>,
    ) -> io::Result<()> {
        let (mut reader, writer) = accept_tcp(stream, &config).await?;
        let mut hs = BytesMut::with_capacity(8);
        let len = read_frame_into(&mut reader, &mut hs).await?;
        if len != 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad handshake"));
        }
        let node_id = u32::from_le_bytes(hs[..4].try_into().unwrap());
        let (resp_tx, resp_rx) =
            crossfire::mpsc::bounded_async::<Bytes>(config.responder_channel_capacity);
        responder_txs_insert(&update_tx, &responder_broadcast, node_id, resp_tx);
        let writer_cancel = cancel.clone();
        let writer_handle =
            tokio::spawn(
                async move { run_follower_responder(resp_rx, writer, writer_cancel).await },
            );
        let mut read_buf = BytesMut::with_capacity(128 * 1024);
        let result = loop {
            let n = tokio::select! {
                r = reader.read_buf(&mut read_buf) => r?,
                _ = cancel.cancelled() => break Ok(()),
            };
            if n == 0 {
                break Ok(());
            }
            while read_buf.len() >= BATCH_HEADER_LEN {
                let batch_seq = u64::from_le_bytes(read_buf[..8].try_into().unwrap());
                let raft_group_id = u32::from_le_bytes(read_buf[8..12].try_into().unwrap());
                let payload_len = u32::from_le_bytes(read_buf[12..16].try_into().unwrap()) as usize;
                let needed = BATCH_HEADER_LEN + payload_len;
                if read_buf.len() < needed {
                    break;
                }
                let _ = read_buf.split_to(BATCH_HEADER_LEN);
                let payload = read_buf.split_to(payload_len).freeze();
                let is_new = dedup_table
                    .lock()
                    .entry(node_id)
                    .or_insert_with(FollowerDedup::new)
                    .check_and_record(batch_seq);
                if !is_new {
                    continue;
                }
                let mut sub = 0;
                let mut count = 0u32;
                while sub + 4 <= payload.len() {
                    let plen =
                        u32::from_le_bytes(payload[sub..sub + 4].try_into().unwrap()) as usize;
                    if plen < 12 || sub + 4 + plen > payload.len() {
                        break;
                    }
                    sub += 4 + plen;
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
                        break;
                    }
                }
            }
        };
        writer_handle.abort();
        result
    }

    #[tokio::test]
    async fn write_and_read_frame() {
        let (c, s) = tokio::io::duplex(8192);
        let (sr, _sw) = tokio::io::split(s);
        let (_cr, cw) = tokio::io::split(c);
        let mut writer: BoxedWriter = Box::new(cw);
        let mut reader: BoxedReader = Box::new(sr);

        let payload = b"hello world";
        write_frame(&mut writer, payload).await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);

        let mut buf = BytesMut::new();
        let len = read_frame_into(&mut reader, &mut buf).await.unwrap();
        assert_eq!(len, payload.len());
        assert_eq!(&buf[..], payload);
    }

    #[tokio::test]
    async fn write_and_read_client_frame() {
        let (c, s) = tokio::io::duplex(8192);
        let (_cr, cw) = tokio::io::split(c);
        let (sr, _sw) = tokio::io::split(s);
        let mut writer: BoxedWriter = Box::new(cw);
        let mut reader: BoxedReader = Box::new(sr);

        write_client_frame(&mut writer, 42, b"test payload")
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut buf = BytesMut::new();
        let len = read_frame_into(&mut reader, &mut buf).await.unwrap();
        assert_eq!(len, 4 + b"test payload".len());
        let (cid, data) = parse_client_frame(&buf).unwrap();
        assert_eq!(cid, 42);
        assert_eq!(data, b"test payload");
    }

    #[tokio::test]
    async fn client_server_roundtrip() {
        let config = ForwardConfig::default();
        let (mut acceptor, mut batch_rx) =
            ForwardAcceptor::start_with_channel(config.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let addr = acceptor.local_addr();

        let client_registry = ClientRegistry::new(4, 256);
        let log_idx = Arc::new(AtomicU64::new(0));
        let done = Arc::new(tokio::sync::Notify::new());
        let log_idx_cb = Arc::clone(&log_idx);
        let done_cb = Arc::clone(&done);
        let cb: crate::async_apply::ResponseCallback =
            Arc::new(move |_slab, _offset, msg, is_done| {
                if !is_done && msg.len() >= 16 {
                    let li = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                    log_idx_cb.store(li, Ordering::Relaxed);
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

        tokio::time::sleep(Duration::from_millis(50)).await;
        client.forward(local_cid, 0, b"test cmd").await;

        let batch = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.node_id, 1);
        let (cid, _req_seq, cmd_bytes) = batch.iter().next().unwrap();
        assert_eq!(cid, local_cid);
        assert_eq!(cmd_bytes, b"test cmd");

        let mut resp = BytesMut::with_capacity(24);
        resp.put_u32_le(20u32);
        resp.put_u32_le(local_cid);
        resp.put_u64_le(0u64);
        resp.put_u64_le(42u64);
        acceptor.push_response_async(1, resp.freeze()).await;

        tokio::time::timeout(Duration::from_secs(1), done.notified())
            .await
            .expect("timeout waiting for response callback");
        assert_eq!(log_idx.load(Ordering::Relaxed), 42);

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

        let (mut acceptor, _rx) =
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

        tokio::time::sleep(Duration::from_millis(50)).await;
        let gen1 = client.generation();
        assert!(gen1 >= 1);

        acceptor.shutdown().await;

        let (mut acceptor2, mut rx2) = ForwardAcceptor::start_with_channel(config.clone(), addr)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        let gen2 = client.generation();
        assert!(gen2 > gen1, "should have reconnected (gen {gen1} → {gen2})");

        client.forward(1u32, 0u64, b"after reconnect").await;
        let batch = tokio::time::timeout(Duration::from_secs(1), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.node_id, 2);

        client.shutdown().await;
        acceptor2.shutdown().await;
    }

    #[test]
    fn follower_dedup_in_order() {
        let mut dedup = FollowerDedup::new();
        assert!(dedup.check_and_record(0));
        assert!(dedup.check_and_record(1));
        assert!(dedup.check_and_record(2));
        assert_eq!(dedup.high_water, 2);
    }

    #[test]
    fn follower_dedup_duplicate() {
        let mut dedup = FollowerDedup::new();
        assert!(dedup.check_and_record(0));
        assert!(!dedup.check_and_record(0));
        assert!(dedup.check_and_record(1));
        assert!(!dedup.check_and_record(1));
    }

    #[test]
    fn follower_dedup_out_of_order() {
        let mut dedup = FollowerDedup::new();
        assert!(dedup.check_and_record(0));
        assert!(dedup.check_and_record(2));
        assert_eq!(dedup.high_water, 0);
        assert!(dedup.check_and_record(1));
        assert_eq!(dedup.high_water, 2);
    }

    #[test]
    fn batch_header_encode_decode() {
        let mut header = [0u8; BATCH_HEADER_LEN];
        header[0..8].copy_from_slice(&42u64.to_le_bytes());
        header[8..12].copy_from_slice(&7u32.to_le_bytes());
        header[12..16].copy_from_slice(&1024u32.to_le_bytes());
        header[16..24].copy_from_slice(&99u64.to_le_bytes());
        assert_eq!(u64::from_le_bytes(header[0..8].try_into().unwrap()), 42);
        assert_eq!(u32::from_le_bytes(header[8..12].try_into().unwrap()), 7);
        assert_eq!(u32::from_le_bytes(header[12..16].try_into().unwrap()), 1024);
        assert_eq!(u64::from_le_bytes(header[16..24].try_into().unwrap()), 99);
    }

    #[test]
    fn default_config() {
        let config = ForwardConfig::default();
        assert_eq!(config.responder_channel_capacity, 256);
        assert_eq!(config.num_outbound_partitions, 4);
        assert!(config.tcp_nodelay);
    }

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

    // =========================================================================
    // RaftBacklog unit tests
    // =========================================================================

    #[test]
    fn backlog_try_charge_and_release() {
        let bl = RaftBacklog::new(1024);
        assert_eq!(bl.current(), 0);
        assert!(bl.try_charge(100));
        assert_eq!(bl.current(), 100);
        assert!(bl.try_charge(200));
        assert_eq!(bl.current(), 300);
        bl.release(50);
        assert_eq!(bl.current(), 250);
        bl.release(250);
        assert_eq!(bl.current(), 0);
    }

    #[test]
    fn backlog_max() {
        let bl = RaftBacklog::new(512);
        assert_eq!(bl.max(), 512);
    }

    #[tokio::test]
    async fn backlog_charge_returns_immediately_when_under_budget() {
        let bl = RaftBacklog::new(1024);
        // Should return immediately — under budget.
        tokio::time::timeout(Duration::from_millis(50), bl.charge(500))
            .await
            .expect("should not block when under budget");
        assert_eq!(bl.current(), 500);
    }

    #[tokio::test]
    async fn backlog_charge_blocks_when_over_budget() {
        let bl = Arc::new(RaftBacklog::new(100));
        // Exhaust the budget.
        assert!(bl.try_charge(100));
        let bl2 = Arc::clone(&bl);
        let unblocked = Arc::new(AtomicBool::new(false));
        let unblocked2 = Arc::clone(&unblocked);

        // Try to charge 1 more byte — should block.
        let handle = tokio::spawn(async move {
            bl2.charge(1).await;
            unblocked2.store(true, Ordering::Release);
        });

        // Give the waiter a chance to park.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !unblocked.load(Ordering::Acquire),
            "should still be blocked"
        );

        // Release enough to allow the pending charge.
        bl.release(50);

        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("should unblock after release")
            .unwrap();
        assert!(unblocked.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn backlog_multiple_waiters_all_unblocked() {
        let bl = Arc::new(RaftBacklog::new(100));
        // Exhaust the budget.
        assert!(bl.try_charge(100));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let bl2 = Arc::clone(&bl);
            handles.push(tokio::spawn(async move {
                bl2.charge(1).await;
            }));
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        // Release enough for all 4 waiters (each needs 1 byte).
        bl.release(50);

        for h in handles {
            tokio::time::timeout(Duration::from_millis(100), h)
                .await
                .expect("all waiters should unblock")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn backlog_release_on_failure_prevents_leak() {
        let bl = RaftBacklog::new(1024);
        let charged = 300;
        bl.charge(charged).await;
        assert_eq!(bl.current(), charged);
        // Simulate proposal failure — release.
        bl.release(charged);
        assert_eq!(bl.current(), 0);
    }

    #[test]
    fn backlog_try_charge_fails_over_budget() {
        let bl = RaftBacklog::new(100);
        assert!(bl.try_charge(100));
        assert!(!bl.try_charge(1), "should fail — budget exhausted");
        bl.release(50);
        assert!(bl.try_charge(50));
        assert_eq!(bl.current(), 100);
    }

    #[test]
    fn backlog_concurrent_try_charge_release() {
        use std::thread;
        // Use a very large budget so all try_charges succeed.
        let budget = 8 * 10_000 * 100;
        let bl = Arc::new(RaftBacklog::new(budget));
        let n = 8;
        let per_thread = 10_000usize;
        let charge_amount = 100usize;

        let mut handles = Vec::new();
        for _ in 0..n {
            let bl2 = Arc::clone(&bl);
            handles.push(thread::spawn(move || {
                for _ in 0..per_thread {
                    assert!(bl2.try_charge(charge_amount));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(bl.current(), n * per_thread * charge_amount);

        let mut handles = Vec::new();
        for _ in 0..n {
            let bl2 = Arc::clone(&bl);
            handles.push(thread::spawn(move || {
                for _ in 0..per_thread {
                    bl2.release(charge_amount);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(bl.current(), 0);
    }
}
