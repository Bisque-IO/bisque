//! Async apply — pull-based worker architecture for the MQ state machine.
//!
//! Workers read committed entries directly from the mmap raft log,
//! filter by partition (`primary_id % N`), and apply matching commands.
//! The state machine's `apply()` drains the stream, advances a
//! high-water mark, and returns. Workers process asynchronously.
//!
//! Batch entries (`TAG_BATCH`) are processed synchronously in `apply()`
//! with a barrier to preserve ordering. Workers skip them when scanning.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex as ParkingMutex;
use tracing::debug;

use bisque_raft::SegmentPrefetcher;

use crate::config::ParallelApplyConfig;
use crate::cursor::MqSegmentScanner;
use crate::engine::MqEngine;
use crate::manifest::MqManifestManager;
use crate::state_machine::{classify_structural, collect_structural_writes};
use crate::types::{MqCommand, MqResponse};

// =============================================================================
// High-Water Mark
// =============================================================================

/// Coalescing high-water mark with single-permit notification.
///
/// The writer (state machine) advances the HWM and notifies. Multiple
/// advances before a worker wakes coalesce into a single wakeup. The
/// worker reads the HWM, processes everything since its cursor, then
/// waits again.
pub struct HighWaterMark {
    value: AtomicU64,
    notify: tokio::sync::Notify,
}

impl HighWaterMark {
    pub fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Advance the HWM and notify all waiting workers.
    #[inline]
    pub fn advance(&self, new_hwm: u64) {
        self.value.store(new_hwm, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait for the HWM to advance past `cursor`. Returns the new HWM.
    ///
    /// Fast path: if HWM > cursor, returns immediately.
    /// Slow path: registers a single waker, sleeps until notified.
    #[inline]
    pub async fn wait_for(&self, cursor: u64) -> u64 {
        loop {
            // Register interest BEFORE checking the value to avoid missed wakeups.
            // notify_waiters() does not store permits, so we must call enable()
            // to ensure the notification is captured even if it fires between the
            // check and the await.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let hwm = self.value.load(Ordering::Acquire);
            if hwm > cursor {
                return hwm;
            }
            notified.await;
        }
    }

    #[inline]
    pub fn current(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }
}

// =============================================================================
// Response Entry (flat wire format)
// =============================================================================

/// Minimal response entry for per-client delivery channels.
///
/// Uses flat binary encoding with 8-byte aligned header:
/// `[tag:1][status:1][pad:6][log_index:8][...fields]`.
///
/// All field offsets are multiples of their natural alignment,
/// enabling zero-copy access via direct reads.
pub struct ResponseEntry {
    pub buf: Bytes,
}

impl ResponseEntry {
    pub const STATUS_OK: u8 = 0;
    pub const STATUS_ERROR: u8 = 1;

    pub const TAG_PUBLISHED: u8 = 0x01;
    pub const TAG_ENTITY_CREATED: u8 = 0x02;
    pub const TAG_OK: u8 = 0x03;
    pub const TAG_ERROR: u8 = 0x04;
    pub const TAG_MESSAGES: u8 = 0x05;
    pub const TAG_BATCH: u8 = 0x06;

    /// Encode a Published response (32 bytes).
    pub fn published(log_index: u64, base_offset: u64, count: u64) -> Self {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u8(Self::TAG_PUBLISHED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6); // pad to 8
        buf.put_u64_le(log_index);
        buf.put_u64_le(base_offset);
        buf.put_u64_le(count);
        Self { buf: buf.freeze() }
    }

    /// Encode an EntityCreated response (24 bytes).
    pub fn entity_created(log_index: u64, entity_id: u64) -> Self {
        let mut buf = BytesMut::with_capacity(24);
        buf.put_u8(Self::TAG_ENTITY_CREATED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6);
        buf.put_u64_le(log_index);
        buf.put_u64_le(entity_id);
        Self { buf: buf.freeze() }
    }

    /// Encode an Ok response (16 bytes).
    pub fn ok(log_index: u64) -> Self {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u8(Self::TAG_OK);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6);
        buf.put_u64_le(log_index);
        Self { buf: buf.freeze() }
    }

    /// Encode an Error response (24+ bytes).
    pub fn error(log_index: u64, error_code: u32, msg: &str) -> Self {
        let msg_bytes = msg.as_bytes();
        let padded_msg_len = (msg_bytes.len() + 7) & !7;
        let total = 24 + padded_msg_len;
        let mut buf = BytesMut::with_capacity(total);
        buf.put_u8(Self::TAG_ERROR);
        buf.put_u8(Self::STATUS_ERROR);
        buf.put_bytes(0, 6);
        buf.put_u64_le(log_index);
        buf.put_u32_le(error_code);
        buf.put_u32_le(msg_bytes.len() as u32);
        buf.put_slice(msg_bytes);
        let pad = padded_msg_len - msg_bytes.len();
        if pad > 0 {
            buf.put_bytes(0, pad);
        }
        Self { buf: buf.freeze() }
    }

    /// Construct from an MqResponse.
    pub fn from_response(log_index: u64, response: &MqResponse) -> Self {
        match response {
            MqResponse::Ok => Self::ok(log_index),
            MqResponse::Published { base_offset, count } => {
                Self::published(log_index, *base_offset, *count)
            }
            MqResponse::EntityCreated { id } => Self::entity_created(log_index, *id),
            MqResponse::Error(e) => Self::error(log_index, 1, &e.to_string()),
            _ => Self::ok(log_index), // fallback for complex responses
        }
    }

    // Zero-copy accessors.
    #[inline]
    pub fn tag(&self) -> u8 {
        self.buf[0]
    }
    #[inline]
    pub fn status(&self) -> u8 {
        self.buf[1]
    }
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.buf[1] == Self::STATUS_OK
    }
    #[inline]
    pub fn log_index(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    /// Write a complete dispatch frame into `buf` without a separate allocation.
    ///
    /// Frame layout: `[len:4 LE][client_id:4 LE][ResponseEntry bytes...]`
    /// where `len` = `4 (client_id field) + ResponseEntry byte count`.
    pub fn encode_frame_into(
        buf: &mut BytesMut,
        client_id: u32,
        log_index: u64,
        response: &MqResponse,
    ) {
        let len_pos = buf.len();
        buf.put_u32_le(0); // placeholder — filled after encoding the entry
        buf.put_u32_le(client_id);
        let entry_start = buf.len();
        match response {
            MqResponse::Ok => {
                buf.put_u8(Self::TAG_OK);
                buf.put_u8(Self::STATUS_OK);
                buf.put_bytes(0, 6);
                buf.put_u64_le(log_index);
            }
            MqResponse::Published { base_offset, count } => {
                buf.put_u8(Self::TAG_PUBLISHED);
                buf.put_u8(Self::STATUS_OK);
                buf.put_bytes(0, 6);
                buf.put_u64_le(log_index);
                buf.put_u64_le(*base_offset);
                buf.put_u64_le(*count);
            }
            MqResponse::EntityCreated { id } => {
                buf.put_u8(Self::TAG_ENTITY_CREATED);
                buf.put_u8(Self::STATUS_OK);
                buf.put_bytes(0, 6);
                buf.put_u64_le(log_index);
                buf.put_u64_le(*id);
            }
            MqResponse::Error(e) => {
                let msg = e.to_string();
                let msg_bytes = msg.as_bytes();
                let padded_msg_len = (msg_bytes.len() + 7) & !7;
                buf.put_u8(Self::TAG_ERROR);
                buf.put_u8(Self::STATUS_ERROR);
                buf.put_bytes(0, 6);
                buf.put_u64_le(log_index);
                buf.put_u32_le(1u32); // error_code
                buf.put_u32_le(msg_bytes.len() as u32);
                buf.put_slice(msg_bytes);
                let pad = padded_msg_len - msg_bytes.len();
                if pad > 0 {
                    buf.put_bytes(0, pad);
                }
            }
            _ => {
                buf.put_u8(Self::TAG_OK);
                buf.put_u8(Self::STATUS_OK);
                buf.put_bytes(0, 6);
                buf.put_u64_le(log_index);
            }
        }
        let payload_len = (buf.len() - len_pos - 4) as u32; // client_id(4) + entry bytes
        buf[len_pos..len_pos + 4].copy_from_slice(&payload_len.to_le_bytes());
        let _ = entry_start; // used only to document the split point
    }

    /// Write a log-index-only dispatch frame into `buf` without a separate allocation.
    ///
    /// Frame layout: `[len:4=12 LE][client_id:4 LE][log_index:8 LE]`
    #[inline]
    pub fn encode_log_index_frame(buf: &mut BytesMut, client_id: u32, log_index: u64) {
        buf.put_u32_le(12u32); // len = client_id(4) + log_index(8)
        buf.put_u32_le(client_id);
        buf.put_u64_le(log_index);
    }
}

// =============================================================================
// Shared responder channel type
// =============================================================================

/// Per-follower responder tx table, indexed directly by node_id (< 64).
pub type ResponderTxVec = Box<[Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>; 64]>;

/// Shared, atomically-swappable reference to the live responder channel table.
/// Used by [`ForwardAcceptor`] for its own `push_response` calls.
pub type SharedResponderTxs = Arc<ArcSwap<ResponderTxVec>>;

/// Message type for responder update notifications sent to [`PartitionWorker`]s.
pub type ResponderUpdateMsg = (
    u32,
    Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>,
);

/// Broadcasts follower connect/disconnect events to all [`PartitionWorker`]s.
///
/// Each worker owns its local responder table and drains updates from its
/// `UnboundedReceiver` at the top of each apply loop iteration — no shared
/// state, no locking on the hot path.
#[derive(Clone)]
pub struct ResponderBroadcast {
    txs: Vec<tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>>,
}

impl ResponderBroadcast {
    /// Create a broadcast handle with no registered workers (useful for tests/benches).
    pub fn new_empty() -> Self {
        Self { txs: vec![] }
    }

    pub fn insert(&self, node_id: u32, tx: crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>) {
        for sender in &self.txs {
            let _ = sender.send((node_id, Some(tx.clone())));
        }
    }

    pub fn remove(&self, node_id: u32) {
        for sender in &self.txs {
            let _ = sender.send((node_id, None));
        }
    }
}

// =============================================================================
// ResponseCallback / ClientPartition
// =============================================================================

/// Callback invoked by a [`ClientPartition`] when a response frame arrives.
///
/// - `slab`: the owning `Bytes` buffer — zero-copy clone via
///   `slab.slice(offset..offset + message.len())` if you need to retain it.
/// - `offset`: byte offset of `message` within `slab`.
/// - `message`: response payload (`[payload...]` after `[len:4][client_id:4]`).
/// - `is_done`: `true` after the partition has exhausted the current drain cycle;
///   the callback can use this signal to flush or batch its own output.
pub type ResponseCallback = Arc<dyn Fn(&Bytes, usize, &[u8], bool) + Send + Sync + 'static>;

enum ClientPartitionMsg {
    Register(u32, ResponseCallback),
    Unregister(u32),
}

/// Per-partition task that dispatches response frames to registered client callbacks.
///
/// Receives [`Bytes`] chunks via a crossfire MPSC channel. Each chunk contains
/// one or more wire frames: `[len:4 LE][client_id:4 LE][payload...]`.
/// The task scans linearly, invokes the matching callback for frames belonging
/// to its partition (`client_id % num_partitions == partition_id`), and signals
/// `is_done = true` to all touched callbacks after draining the channel.
///
/// The callback map is owned exclusively by this task — no mutex required.
struct ClientPartition {
    partition_id: usize,
    num_partitions: usize,
    bytes_rx: crossfire::AsyncRx<crossfire::mpsc::Array<Bytes>>,
    reg_rx: tokio::sync::mpsc::UnboundedReceiver<ClientPartitionMsg>,
    callbacks: HashMap<u32, ResponseCallback>,
    seen_ids: HashSet<u32>,
}

impl ClientPartition {
    /// Scan `slab` for complete frames belonging to this partition and invoke callbacks.
    ///
    /// When `num_partitions > 1` the slab is the full read buffer broadcast to all workers;
    /// each worker skips records whose `client_id % num_partitions != partition_id`.  The slab
    /// is already in L1/L2 cache from the TCP read so the extra linear scan is cheap.
    fn dispatch(&mut self, slab: &Bytes) {
        if self.callbacks.is_empty() {
            return;
        }
        let n = self.num_partitions;
        let p = self.partition_id;
        let buf = slab.as_ref();
        let mut pos = 0;
        while pos + 4 <= buf.len() {
            let frame_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            if frame_len < 4 || pos + 4 + frame_len > buf.len() {
                break;
            }
            let payload_start = pos + 4;
            let client_id =
                u32::from_le_bytes(buf[payload_start..payload_start + 4].try_into().unwrap());
            if n == 1 || (client_id as usize) % n == p {
                if let Some(cb) = self.callbacks.get(&client_id) {
                    let msg_offset = payload_start + 4;
                    let msg_end = payload_start + frame_len;
                    cb(slab, msg_offset, &buf[msg_offset..msg_end], false);
                    self.seen_ids.insert(client_id);
                }
            }
            pos = payload_start + frame_len;
        }
    }

    fn handle_reg(&mut self, msg: ClientPartitionMsg) {
        match msg {
            ClientPartitionMsg::Register(id, cb) => {
                self.callbacks.insert(id, cb);
            }
            ClientPartitionMsg::Unregister(id) => {
                self.callbacks.remove(&id);
            }
        }
    }

    async fn run(mut self) {
        let empty = Bytes::new();
        loop {
            // Drain pending registrations without blocking.
            while let Ok(msg) = self.reg_rx.try_recv() {
                self.handle_reg(msg);
            }

            tokio::select! {
                biased;
                msg = self.reg_rx.recv() => {
                    match msg {
                        Some(m) => self.handle_reg(m),
                        None => return,
                    }
                }
                result = self.bytes_rx.recv() => {
                    let slab = match result {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    self.dispatch(&slab);
                    while let Ok(more) = self.bytes_rx.try_recv() {
                        self.dispatch(&more);
                    }
                    // Signal all touched callbacks: drain cycle complete.
                    for &id in &self.seen_ids {
                        if let Some(cb) = self.callbacks.get(&id) {
                            cb(&empty, 0, &[], true);
                        }
                    }
                    self.seen_ids.clear();
                }
            }
        }
    }
}

// =============================================================================
// Client Registry
// =============================================================================

/// Registry of active client response partitions.
///
/// Each client is assigned to a [`ClientPartition`] by `client_id % num_partitions`.
/// The partition task owns the callback map for its shard — no mutex required.
/// Response frames are pushed via crossfire MPSC channels; registrations via
/// unbounded tokio channels.
pub struct ClientRegistry {
    next_id: AtomicU32,
    num_partitions: usize,
    bytes_txs: Vec<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>,
    reg_txs: Vec<tokio::sync::mpsc::UnboundedSender<ClientPartitionMsg>>,
}

impl ClientRegistry {
    /// Create a new `ClientRegistry` with `num_partitions` client partitions,
    /// each backed by a live [`ClientPartition`] task with the given channel `capacity`.
    ///
    /// Spawns `num_partitions` tokio tasks — must be called from within a tokio runtime.
    pub fn new(num_partitions: usize, capacity: usize) -> Arc<Self> {
        let n = num_partitions.max(1);
        let mut bytes_txs = Vec::with_capacity(n);
        let mut reg_txs = Vec::with_capacity(n);

        for i in 0..n {
            let (bytes_tx, bytes_rx) = crossfire::mpsc::bounded_async::<Bytes>(capacity);
            let (reg_tx, reg_rx) = tokio::sync::mpsc::unbounded_channel::<ClientPartitionMsg>();
            bytes_txs.push(bytes_tx);
            reg_txs.push(reg_tx);

            let partition = ClientPartition {
                partition_id: i,
                num_partitions: n,
                bytes_rx,
                reg_rx,
                callbacks: HashMap::new(),
                seen_ids: HashSet::new(),
            };
            tokio::spawn(partition.run());
        }

        Arc::new(Self {
            next_id: AtomicU32::new(1),
            num_partitions: n,
            bytes_txs,
            reg_txs,
        })
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Register a client with a response callback. Returns the assigned `client_id`.
    pub fn register(&self, callback: ResponseCallback) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let partition = (id as usize) % self.num_partitions;
        let _ = self.reg_txs[partition].send(ClientPartitionMsg::Register(id, callback));
        id
    }

    /// Unregister a client. The partition task drops the callback asynchronously.
    pub fn unregister(&self, client_id: u32) {
        let partition = (client_id as usize) % self.num_partitions;
        let _ = self.reg_txs[partition].send(ClientPartitionMsg::Unregister(client_id));
    }

    /// Push a raw [`Bytes`] chunk (complete wire frames) to a specific partition.
    ///
    /// Used by the TCP inbound loop after bitmap-based partition selection.
    /// Non-blocking — drops if full.
    pub fn send_to_partition(&self, partition: usize, bytes: Bytes) {
        debug_assert!(partition < self.num_partitions);
        let _ = self.bytes_txs[partition].try_send(bytes);
    }

    /// Async variant of [`send_to_partition`] — try_send first (fast path, no future
    /// machinery when the channel has space), falling back to a blocking await only if the
    /// channel is momentarily full (rare with the default 4096-chunk capacity).
    pub async fn send_to_partition_async(&self, partition: usize, bytes: Bytes) {
        if partition < self.num_partitions {
            let tx = &self.bytes_txs[partition];
            if let Err(e) = tx.try_send(bytes) {
                let _ = tx.send(e.into_inner()).await;
            }
        }
    }
}

// =============================================================================
// Client ID Table
// =============================================================================

/// Maps log_index → client_id for response routing.
///
/// Populated by `apply()` when client_id is available. Workers read
/// and remove after applying.
pub(crate) struct ClientIdTable {
    entries: papaya::HashMap<u64, u32>,
}

impl ClientIdTable {
    pub fn new() -> Self {
        Self {
            entries: papaya::HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn insert(&self, log_index: u64, client_id: u32) {
        self.entries.pin().insert(log_index, client_id);
    }

    pub fn take(&self, log_index: u64) -> Option<u32> {
        self.entries.pin().remove(&log_index).copied()
    }

    /// Acquire an owned (Send) epoch guard for reuse across multiple `take` calls.
    pub fn guard(
        &self,
    ) -> papaya::HashMapRef<
        '_,
        u64,
        u32,
        std::collections::hash_map::RandomState,
        papaya::OwnedGuard<'_>,
    > {
        self.entries.pin_owned()
    }
}

// =============================================================================
// Partition Worker
// =============================================================================

/// Per-worker state for the pull-based log consumer.
#[allow(dead_code)]
struct PartitionWorker {
    partition_id: usize,
    num_partitions: usize,
    cursor: Arc<AtomicU64>,
    hwm: Arc<HighWaterMark>,
    prefetcher: SegmentPrefetcher,
    engine: Arc<MqEngine>,
    client_registry: Arc<ClientRegistry>,
    client_id_table: Arc<ClientIdTable>,
    /// Owned per-follower responder tx table, indexed by node_id.
    /// Updated via `responder_update_rx` when followers connect/disconnect.
    responder_txs: Box<[Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>; 64]>,
    /// Receives (node_id, Option<tx>) updates from [`ResponderBroadcast`].
    responder_update_rx: tokio::sync::mpsc::UnboundedReceiver<ResponderUpdateMsg>,
    /// Per-follower output buffers indexed directly by node_id (< 64).
    /// Tight-packed `[client_id:4 LE][log_index:8 LE]` records, 12 bytes each.
    /// Direct indexing: O(1) access, no search required.
    response_buf: Box<[BytesMut; 64]>,
    /// Bitmask of non-empty slots in `response_buf`. Bit `i` is set when
    /// `response_buf[i]` contains data. Enables O(popcount) flush iteration.
    response_buf_mask: u64,
    /// Number of forwarded sub-command entries accumulated since last flush.
    entries_since_flush: usize,
    /// Number of bytes accumulated across all `response_buf` entries since last flush.
    bytes_since_flush: usize,
    /// Flush after this many accumulated entries (from config).
    config_flush_entries: usize,
    /// Flush after this many accumulated bytes (from config).
    config_flush_bytes: usize,
    manifest: Option<Arc<MqManifestManager>>,
    /// Purged segment IDs shared with log storage. Workers peek (without
    /// draining) after each range to sweep retained messages on their own
    /// exchanges. Detach is idempotent so overlapping sweeps are safe.
    purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
    group_id: u64,
    cursor_notify: Arc<tokio::sync::Notify>,
    m_apply_count: metrics::Counter,
    m_skip_count: metrics::Counter,
}

impl PartitionWorker {
    async fn run(mut self) {
        debug!(
            partition = self.partition_id,
            "async partition worker started"
        );

        // Persistent scanner — kept alive across ranges. The active segment
        // uses a live SegmentView that reads logical_size atomically, so the
        // cursor always sees newly appended data without re-acquiring.
        let mut scanner: Option<MqSegmentScanner> = None;

        loop {
            self.drain_responder_updates();
            let cursor_val = self.cursor.load(Ordering::Acquire);
            let hwm = self.hwm.wait_for(cursor_val).await;

            // Shutdown sentinel.
            if hwm == u64::MAX {
                debug!(
                    partition = self.partition_id,
                    "async partition worker shutting down"
                );
                return;
            }

            let from = cursor_val + 1;
            self.process_range(from, hwm, &mut scanner).await;
            self.sweep_purged_retained();
            self.cursor.store(hwm, Ordering::Release);
            self.cursor_notify.notify_waiters();
        }
    }

    fn drain_responder_updates(&mut self) {
        while let Ok((node_id, tx)) = self.responder_update_rx.try_recv() {
            self.responder_txs[node_id as usize] = tx;
        }
    }

    /// Flush accumulated forwarded-response bytes to the appropriate follower
    /// responder channels.  Called at end-of-range (and optionally mid-range).
    async fn flush_response_buf(&mut self) {
        if self.response_buf_mask == 0 {
            return;
        }

        let mut mask = self.response_buf_mask;
        self.response_buf_mask = 0;
        self.entries_since_flush = 0;
        self.bytes_since_flush = 0;

        while mask != 0 {
            let node_id = mask.trailing_zeros();
            mask &= mask - 1;
            let buf = &mut self.response_buf[node_id as usize];
            if buf.is_empty() {
                continue;
            }
            let bytes = buf.split().freeze();
            if let Some(tx) = &self.responder_txs[node_id as usize] {
                let _ = tx.send(bytes).await;
            }
        }
    }

    async fn process_range(&mut self, from: u64, to: u64, scanner: &mut Option<MqSegmentScanner>) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // One epoch guard for the entire range instead of one per applied entry.
        // Clone the Arc so the guard's lifetime is independent of `self`, allowing
        // `&mut self` borrows (e.g. flush_response_buf) to coexist.
        let cid_table = Arc::clone(&self.client_id_table);
        let cid_guard = cid_table.guard();

        // Reuse persistent scanner if available, otherwise create a new one.
        // The scanner's active segment uses a live SegmentView, so it always
        // sees newly appended data — no stale snapshot issues.
        if scanner.is_none() {
            let start_seg = match self.prefetcher.segment_id_for(from) {
                Some(seg) => seg,
                None => return, // entry purged or not yet written
            };
            *scanner = Some(MqSegmentScanner::new(self.prefetcher.clone(), start_seg));
        }
        let scan = scanner.as_mut().unwrap();

        let mut apply_count = 0u64;
        let mut skip_count = 0u64;

        loop {
            let Some(rec) = scan.next_record_raw() else {
                break; // segment(s) exhausted
            };

            // Skip entries before our range (mid-segment start).
            if rec.log_index < from {
                continue;
            }
            // Stop when past our range — put back for the next call.
            if rec.log_index > to {
                scan.put_back(rec);
                break;
            }

            let cmd = rec.command;

            // TAG_BATCH is applied synchronously by the state machine. Skip.
            if cmd.tag() == MqCommand::TAG_BATCH {
                continue;
            }

            // TAG_FORWARDED_BATCH: state machine already applied the sub-commands.
            // Accumulate [client_id:4 LE][log_index:8 LE] records into the
            // per-follower output buffer for all sub-commands owned by this
            // partition.  Flush to the FollowerResponder channel when thresholds
            // are met or at end-of-range.
            if cmd.tag() == MqCommand::TAG_FORWARDED_BATCH {
                let view = cmd.as_forwarded_batch();
                let node_id = view.node_id();
                debug_assert!(
                    (node_id as usize) < 64,
                    "node_id {node_id} exceeds response_buf capacity"
                );
                for (client_id, _) in view.iter() {
                    if (client_id as usize) % self.num_partitions == self.partition_id {
                        let buf = &mut self.response_buf[node_id as usize];
                        buf.put_u32_le(12u32); // length prefix: 4 (client_id) + 8 (log_index)
                        buf.put_u32_le(client_id);
                        buf.put_u64_le(rec.log_index);
                        self.response_buf_mask |= 1u64 << node_id;
                        self.entries_since_flush += 1;
                        self.bytes_since_flush += 16;
                    }
                }
                if self.entries_since_flush >= self.config_flush_entries
                    || self.bytes_since_flush >= self.config_flush_bytes
                {
                    self.flush_response_buf().await;
                }
                continue;
            }

            // Route check: does this entry belong to my partition?
            if !self.should_process(&cmd, rec.log_index) {
                skip_count += 1;
                continue;
            }

            // Segment location from the cursor — no index lookup needed.
            let segment_id = scan.current_segment_id().unwrap();

            // Apply command.
            let response =
                self.engine
                    .apply_command(&cmd, rec.log_index, current_time, Some(segment_id));

            // Post-apply: structural writes to MDBX.
            self.handle_structural_writes(&cmd, &response, rec.log_index);

            // Deliver response to the owning ClientPartition.
            if let Some(client_id) = cid_guard.remove(&rec.log_index).copied() {
                let partition = (client_id as usize) % self.num_partitions;
                let mut frame = BytesMut::with_capacity(4 + 4 + 32);
                ResponseEntry::encode_frame_into(&mut frame, client_id, rec.log_index, &response);
                self.client_registry
                    .send_to_partition(partition, frame.freeze());
            }

            apply_count += 1;
        }

        if apply_count > 0 {
            self.m_apply_count.increment(apply_count);
        }
        if skip_count > 0 {
            self.m_skip_count.increment(skip_count);
        }

        // End-of-range flush: always send any remaining response bytes.
        self.flush_response_buf().await;
    }

    /// Determine if this worker should process the given command.
    #[inline]
    fn should_process(&self, cmd: &MqCommand, log_index: u64) -> bool {
        match cmd.tag() {
            // Structural: assigned by log_index for even distribution.
            MqCommand::TAG_CREATE_TOPIC
            | MqCommand::TAG_DELETE_TOPIC
            | MqCommand::TAG_CREATE_EXCHANGE
            | MqCommand::TAG_DELETE_EXCHANGE
            | MqCommand::TAG_CREATE_CONSUMER_GROUP
            | MqCommand::TAG_DELETE_CONSUMER_GROUP
            | MqCommand::TAG_CREATE_BINDING
            | MqCommand::TAG_DELETE_BINDING
            | MqCommand::TAG_CREATE_SESSION
            | MqCommand::TAG_DISCONNECT_SESSION => {
                (log_index as usize) % self.num_partitions == self.partition_id
            }

            // Data-plane: assigned by primary_id for entity co-location.
            _ => (cmd.primary_id() as usize) % self.num_partitions == self.partition_id,
        }
    }

    /// Sweep retained messages on this worker's exchanges and topics that
    /// reference purged mmap segments. Copies the message bytes to heap so
    /// the segment can be freed. Only sweeps entities owned by this partition
    /// (`entity_id % num_partitions == partition_id`).
    fn sweep_purged_retained(&self) {
        let purged = match self.purged_segments {
            Some(ref p) => p,
            None => return,
        };

        // Peek — don't drain. The state machine drains after all workers
        // have caught up. Detach is idempotent so double-sweeps are harmless.
        let purged_set: HashSet<u64> = {
            let guard = purged.lock();
            if guard.is_empty() {
                return;
            }
            guard.iter().copied().collect()
        };

        let meta = self.engine.metadata();

        // Sweep exchange retained messages.
        let exchanges_guard = meta.exchanges.pin();
        for (&exchange_id, exchange) in exchanges_guard.iter() {
            if (exchange_id as usize) % self.num_partitions != self.partition_id {
                continue;
            }

            let mut retained = exchange.retained.write();
            let mut detached = false;
            for rv in retained.values_mut() {
                if let Some(seg_id) = rv.segment_id {
                    if purged_set.contains(&seg_id) {
                        rv.detach();
                        detached = true;
                    }
                }
            }

            if detached {
                if let Some(ref manifest) = self.manifest {
                    let entries: Vec<(String, Vec<u8>)> = retained
                        .iter()
                        .map(|(k, v)| (k.clone(), v.message.to_vec()))
                        .collect();
                    manifest.persist_retained_fire_and_forget(self.group_id, exchange_id, entries);
                }
            }
        }

        // Sweep topic retained messages.
        let topics_guard = meta.topics.pin();
        for (&topic_id, topic) in topics_guard.iter() {
            if !topic.meta.retained {
                continue;
            }
            if (topic_id as usize) % self.num_partitions != self.partition_id {
                continue;
            }

            let mut retained = topic.retained_message.write();
            if let Some(ref mut rv) = *retained {
                if let Some(seg_id) = rv.segment_id {
                    if purged_set.contains(&seg_id) {
                        rv.detach();
                    }
                }
            }
        }
    }

    fn handle_structural_writes(&self, cmd: &MqCommand, response: &MqResponse, log_index: u64) {
        let manifest = match self.manifest {
            Some(ref m) => m,
            None => return,
        };

        let kind = classify_structural(cmd);
        if let Some(writes) = collect_structural_writes(self.engine.metadata(), response, kind) {
            let next_id = self.engine.metadata().next_id.load(Ordering::Relaxed);
            for w in writes {
                manifest.structural_update_fire_and_forget(self.group_id, log_index, next_id, w);
            }
        }
    }
}

// =============================================================================
// Async Apply Manager
// =============================================================================

/// Coordinates pull-based partition workers and the high-water mark.
///
/// Created during state machine initialization. The state machine's
/// `apply()` advances the HWM; workers wake and pull from the log.
#[allow(dead_code)]
pub struct AsyncApplyManager {
    pub(crate) hwm: Arc<HighWaterMark>,
    pub(crate) client_registry: Arc<ClientRegistry>,
    pub(crate) client_id_table: Arc<ClientIdTable>,
    worker_cursors: Vec<Arc<AtomicU64>>,
    worker_handles: Vec<tokio::task::JoinHandle<()>>,
    cursor_notify: Arc<tokio::sync::Notify>,
    num_partitions: usize,
    /// Shared with log storage — workers peek after each range to sweep
    /// retained messages on their own exchanges. The state machine drains
    /// after all workers have caught up.
    purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
    /// Broadcasts follower connect/disconnect events to all partition workers.
    responder_broadcast: ResponderBroadcast,
}

impl AsyncApplyManager {
    /// Create the manager and spawn partition workers + client partitions.
    pub fn new(
        config: &ParallelApplyConfig,
        engine: Arc<MqEngine>,
        prefetcher: SegmentPrefetcher,
        manifest: Option<Arc<MqManifestManager>>,
        purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
        group_id: u64,
        initial_cursor: u64,
        catalog_name: &str,
    ) -> Self {
        let n = config.num_partitions;
        let hwm = Arc::new(HighWaterMark::new(initial_cursor));
        let client_id_table = Arc::new(ClientIdTable::new());
        let cursor_notify = Arc::new(tokio::sync::Notify::new());
        let mut responder_update_txs = Vec::with_capacity(n);
        let mut responder_update_rxs: Vec<
            tokio::sync::mpsc::UnboundedReceiver<ResponderUpdateMsg>,
        > = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            responder_update_txs.push(tx);
            responder_update_rxs.push(rx);
        }
        let responder_broadcast = ResponderBroadcast {
            txs: responder_update_txs,
        };

        // Build ClientRegistry channels and spawn ClientPartition tasks.
        let mut bytes_txs = Vec::with_capacity(n);
        let mut reg_txs = Vec::with_capacity(n);

        for i in 0..n {
            let (bytes_tx, bytes_rx) =
                crossfire::mpsc::bounded_async::<Bytes>(config.response_partition_capacity);
            let (reg_tx, reg_rx) = tokio::sync::mpsc::unbounded_channel::<ClientPartitionMsg>();
            bytes_txs.push(bytes_tx);
            reg_txs.push(reg_tx);

            let partition = ClientPartition {
                partition_id: i,
                num_partitions: n,
                bytes_rx,
                reg_rx,
                callbacks: HashMap::new(),
                seen_ids: HashSet::new(),
            };
            tokio::spawn(partition.run());
        }

        let client_registry = Arc::new(ClientRegistry {
            next_id: AtomicU32::new(1),
            num_partitions: n,
            bytes_txs,
            reg_txs,
        });

        let mut worker_cursors = Vec::with_capacity(n);
        let mut worker_handles = Vec::with_capacity(n);

        for i in 0..n {
            let cursor = Arc::new(AtomicU64::new(initial_cursor));
            let partition_label = i.to_string();
            let catalog = catalog_name.to_owned();
            let m_apply_count = metrics::counter!(
                "mq.async_worker.apply_count",
                "catalog" => catalog.clone(),
                "partition" => partition_label.clone()
            );
            let m_skip_count = metrics::counter!(
                "mq.async_worker.skip_count",
                "catalog" => catalog,
                "partition" => partition_label
            );

            let worker = PartitionWorker {
                partition_id: i,
                num_partitions: n,
                cursor: Arc::clone(&cursor),
                hwm: Arc::clone(&hwm),
                prefetcher: prefetcher.clone(),
                engine: Arc::clone(&engine),
                client_registry: Arc::clone(&client_registry),
                client_id_table: Arc::clone(&client_id_table),
                responder_txs: Box::new(std::array::from_fn(|_| None)),
                responder_update_rx: responder_update_rxs.remove(0),
                response_buf: Box::new(std::array::from_fn(|_| BytesMut::new())),
                response_buf_mask: 0,
                entries_since_flush: 0,
                bytes_since_flush: 0,
                config_flush_entries: config.response_flush_entries,
                config_flush_bytes: config.response_flush_bytes,
                manifest: manifest.clone(),
                purged_segments: purged_segments.clone(),
                group_id,
                cursor_notify: Arc::clone(&cursor_notify),
                m_apply_count,
                m_skip_count,
            };

            let handle = tokio::spawn(worker.run());
            worker_cursors.push(cursor);
            worker_handles.push(handle);
        }

        Self {
            hwm,
            client_registry,
            client_id_table,
            worker_cursors,
            worker_handles,
            cursor_notify,
            num_partitions: n,
            purged_segments,
            responder_broadcast,
        }
    }

    pub fn responder_broadcast(&self) -> ResponderBroadcast {
        self.responder_broadcast.clone()
    }

    /// Advance the HWM to the given index.
    #[inline]
    pub fn advance_hwm(&self, new_hwm: u64) {
        self.hwm.advance(new_hwm);
    }

    /// Advance the HWM and wait for all workers to reach it.
    pub async fn advance_and_wait(&self, target: u64) {
        self.hwm.advance(target);
        self.wait_for_workers(target).await;
    }

    /// Wait for all workers to reach at least `target`.
    pub async fn wait_for_workers(&self, target: u64) {
        loop {
            let notified = self.cursor_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.min_worker_cursor() >= target {
                return;
            }
            notified.await;
        }
    }

    /// Barrier: ensure all workers have processed up to `target`.
    pub async fn barrier(&self, target: u64) {
        self.advance_and_wait(target).await;
    }

    /// Returns the minimum cursor across all workers.
    pub fn min_worker_cursor(&self) -> u64 {
        self.worker_cursors
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }

    /// Drain the purged segments vec. Call after all workers have had a
    /// chance to sweep (i.e., after their cursors have advanced past the
    /// HWM at which the segments were purged).
    pub fn drain_purged_segments(&self) {
        if let Some(ref purged) = self.purged_segments {
            let mut guard = purged.lock();
            if !guard.is_empty() {
                guard.clear();
            }
        }
    }

    /// Number of partition workers.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Get a reference to the client registry.
    pub fn client_registry(&self) -> &Arc<ClientRegistry> {
        &self.client_registry
    }

    /// Shutdown all workers.
    pub async fn shutdown(&mut self) {
        self.hwm.advance(u64::MAX);
        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }
}

// =============================================================================
// Adapter Integration (Phase 4)
// =============================================================================

/// Tracks in-flight requests for a single client connection.
///
/// Each adapter connection maintains one of these. After proposing a command
/// through Raft, the adapter stores the request context here keyed by the
/// returned `log_index`. When a `ResponseEntry` arrives on the client channel
/// matching that `log_index`, the adapter removes the pending request and
/// constructs the protocol-specific response.
pub struct PendingRequests {
    requests: std::collections::HashMap<u64, PendingRequest>,
}

/// Protocol-specific request context stored while awaiting a response.
pub enum PendingRequest {
    /// MQTT publish awaiting PUBACK.
    Publish { packet_id: u16, qos: u8 },
    /// AMQP disposition awaiting settlement.
    Disposition { delivery_id: u32 },
    /// Kafka produce awaiting partition response.
    ProducePartition { topic: Arc<str>, partition: i32 },
    /// Entity creation (topic, exchange, consumer group, etc.).
    CreateEntity { name: Arc<str> },
    /// Generic request with no protocol-specific metadata.
    Generic,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            requests: std::collections::HashMap::new(),
        }
    }

    /// Register a pending request. Called after `raft.client_write()` returns
    /// the `log_index`.
    pub fn insert(&mut self, log_index: u64, request: PendingRequest) {
        self.requests.insert(log_index, request);
    }

    /// Remove and return a pending request. Called when a `ResponseEntry`
    /// arrives matching the `log_index`.
    pub fn remove(&mut self, log_index: u64) -> Option<PendingRequest> {
        self.requests.remove(&log_index)
    }

    /// Number of in-flight requests.
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    /// Whether there are no in-flight requests.
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- HighWaterMark tests ----

    #[tokio::test]
    async fn hwm_immediate_return_when_ahead() {
        let hwm = HighWaterMark::new(10);
        // cursor=5, HWM=10 → should return immediately
        let val = hwm.wait_for(5).await;
        assert_eq!(val, 10);
    }

    #[tokio::test]
    async fn hwm_waits_then_wakes() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        let handle = tokio::spawn(async move {
            // cursor=0, HWM=0 → should block
            hwm2.wait_for(0).await
        });

        // Give the task time to register the waiter.
        tokio::task::yield_now().await;

        // Advance HWM — should wake the waiter.
        hwm.advance(5);

        let val = handle.await.unwrap();
        assert_eq!(val, 5);
    }

    #[tokio::test]
    async fn hwm_coalescing() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        let handle = tokio::spawn(async move { hwm2.wait_for(0).await });

        tokio::task::yield_now().await;

        // Multiple advances before wake — should coalesce.
        hwm.advance(1);
        hwm.advance(2);
        hwm.advance(5);
        hwm.advance(10);

        let val = handle.await.unwrap();
        assert_eq!(val, 10);
    }

    #[test]
    fn hwm_current() {
        let hwm = HighWaterMark::new(42);
        assert_eq!(hwm.current(), 42);
        hwm.advance(100);
        assert_eq!(hwm.current(), 100);
    }

    // ---- ResponseEntry tests ----

    #[test]
    fn response_entry_ok() {
        let entry = ResponseEntry::ok(42);
        assert_eq!(entry.tag(), ResponseEntry::TAG_OK);
        assert!(entry.is_ok());
        assert_eq!(entry.log_index(), 42);
        assert_eq!(entry.buf.len(), 16);
    }

    #[test]
    fn response_entry_published() {
        let entry = ResponseEntry::published(100, 50, 10);
        assert_eq!(entry.tag(), ResponseEntry::TAG_PUBLISHED);
        assert!(entry.is_ok());
        assert_eq!(entry.log_index(), 100);
        assert_eq!(entry.buf.len(), 32);

        // Read base_offset and count.
        let base_offset = u64::from_le_bytes(entry.buf[16..24].try_into().unwrap());
        let count = u64::from_le_bytes(entry.buf[24..32].try_into().unwrap());
        assert_eq!(base_offset, 50);
        assert_eq!(count, 10);
    }

    #[test]
    fn response_entry_entity_created() {
        let entry = ResponseEntry::entity_created(200, 999);
        assert_eq!(entry.tag(), ResponseEntry::TAG_ENTITY_CREATED);
        assert!(entry.is_ok());
        assert_eq!(entry.log_index(), 200);

        let entity_id = u64::from_le_bytes(entry.buf[16..24].try_into().unwrap());
        assert_eq!(entity_id, 999);
    }

    #[test]
    fn response_entry_error() {
        let entry = ResponseEntry::error(300, 42, "not found");
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert_eq!(entry.status(), ResponseEntry::STATUS_ERROR);
        assert!(!entry.is_ok());
        assert_eq!(entry.log_index(), 300);

        let error_code = u32::from_le_bytes(entry.buf[16..20].try_into().unwrap());
        let msg_len = u32::from_le_bytes(entry.buf[20..24].try_into().unwrap()) as usize;
        assert_eq!(error_code, 42);
        assert_eq!(msg_len, 9);
        assert_eq!(&entry.buf[24..24 + msg_len], b"not found");
        // Verify 8-byte alignment.
        assert_eq!(entry.buf.len() % 8, 0);
    }

    #[test]
    fn response_entry_from_mq_response() {
        let entry = ResponseEntry::from_response(1, &MqResponse::Ok);
        assert_eq!(entry.tag(), ResponseEntry::TAG_OK);

        let entry = ResponseEntry::from_response(
            2,
            &MqResponse::Published {
                base_offset: 10,
                count: 5,
            },
        );
        assert_eq!(entry.tag(), ResponseEntry::TAG_PUBLISHED);
        assert_eq!(entry.log_index(), 2);

        let entry = ResponseEntry::from_response(3, &MqResponse::EntityCreated { id: 77 });
        assert_eq!(entry.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    }

    #[test]
    fn response_entry_8byte_alignment() {
        // All response sizes should be 8-byte aligned.
        assert_eq!(ResponseEntry::ok(0).buf.len() % 8, 0);
        assert_eq!(ResponseEntry::published(0, 0, 0).buf.len() % 8, 0);
        assert_eq!(ResponseEntry::entity_created(0, 0).buf.len() % 8, 0);
        assert_eq!(ResponseEntry::error(0, 0, "").buf.len() % 8, 0);
        assert_eq!(ResponseEntry::error(0, 0, "x").buf.len() % 8, 0);
        assert_eq!(ResponseEntry::error(0, 0, "exactly8!").buf.len() % 8, 0);
    }

    // ---- ClientRegistry tests ----

    #[tokio::test]
    async fn client_registry_register_and_dispatch() {
        use std::sync::atomic::AtomicU64;

        let registry = ClientRegistry::new(2, 64);

        let log_idx = Arc::new(AtomicU64::new(0));
        let done = Arc::new(tokio::sync::Notify::new());
        let log_idx_cb = Arc::clone(&log_idx);
        let done_cb = Arc::clone(&done);

        let cb: ResponseCallback = Arc::new(move |_slab, _offset, msg, is_done| {
            if !is_done && msg.len() >= 16 {
                // ResponseEntry::ok layout: [tag:1][status:1][pad:6][log_index:8]
                let li = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                log_idx_cb.store(li, Ordering::Relaxed);
                done_cb.notify_one();
            }
        });
        let id = registry.register(cb);
        assert!(id >= 1);

        let p = (id as usize) % registry.num_partitions();
        let mut frame = BytesMut::with_capacity(24);
        ResponseEntry::encode_frame_into(&mut frame, id, 42, &MqResponse::Ok);
        registry.send_to_partition(p, frame.freeze());
        done.notified().await;
        assert_eq!(log_idx.load(Ordering::Relaxed), 42);

        registry.unregister(id);
    }

    #[tokio::test]
    async fn client_registry_multiple_clients() {
        let registry = ClientRegistry::new(2, 64);
        let total = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let done = Arc::new(tokio::sync::Notify::new());

        let mut ids = Vec::new();
        for _ in 0..4 {
            let total_cb = Arc::clone(&total);
            let done_cb = Arc::clone(&done);
            let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
                if !is_done {
                    if total_cb.fetch_add(1, Ordering::Relaxed) + 1 >= 4 {
                        done_cb.notify_one();
                    }
                }
            });
            ids.push(registry.register(cb));
        }
        // IDs should be unique.
        let id_set: std::collections::HashSet<u32> = ids.iter().copied().collect();
        assert_eq!(id_set.len(), 4);

        for &id in &ids {
            let p = (id as usize) % registry.num_partitions();
            let mut frame = BytesMut::with_capacity(24);
            ResponseEntry::encode_frame_into(&mut frame, id, 1, &MqResponse::Ok);
            registry.send_to_partition(p, frame.freeze());
        }
        done.notified().await;
        assert_eq!(total.load(Ordering::Relaxed), 4);

        for id in ids {
            registry.unregister(id);
        }
    }

    // ---- ClientIdTable tests ----

    #[test]
    fn client_id_table_insert_take() {
        let table = ClientIdTable::new();
        table.insert(100, 42);
        table.insert(200, 43);

        assert_eq!(table.take(100), Some(42));
        assert_eq!(table.take(100), None); // already taken
        assert_eq!(table.take(200), Some(43));
        assert_eq!(table.take(300), None);
    }

    #[test]
    fn client_id_table_overwrite() {
        let table = ClientIdTable::new();
        table.insert(100, 1);
        table.insert(100, 2); // overwrite
        assert_eq!(table.take(100), Some(2));
    }

    // ---- PendingRequests tests ----

    #[test]
    fn pending_requests_lifecycle() {
        let mut pending = PendingRequests::new();
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        pending.insert(
            10,
            PendingRequest::Publish {
                packet_id: 1,
                qos: 1,
            },
        );
        pending.insert(11, PendingRequest::Disposition { delivery_id: 42 });
        pending.insert(12, PendingRequest::Generic);
        assert_eq!(pending.len(), 3);
        assert!(!pending.is_empty());

        match pending.remove(10) {
            Some(PendingRequest::Publish { packet_id, qos }) => {
                assert_eq!(packet_id, 1);
                assert_eq!(qos, 1);
            }
            _ => panic!("expected Publish"),
        }

        match pending.remove(11) {
            Some(PendingRequest::Disposition { delivery_id }) => {
                assert_eq!(delivery_id, 42);
            }
            _ => panic!("expected Disposition"),
        }

        assert!(pending.remove(10).is_none()); // already removed
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn pending_requests_all_variants() {
        let mut pending = PendingRequests::new();
        pending.insert(
            1,
            PendingRequest::Publish {
                packet_id: 100,
                qos: 2,
            },
        );
        pending.insert(2, PendingRequest::Disposition { delivery_id: 200 });
        pending.insert(
            3,
            PendingRequest::ProducePartition {
                topic: Arc::from("test-topic"),
                partition: 3,
            },
        );
        pending.insert(
            4,
            PendingRequest::CreateEntity {
                name: Arc::from("my-queue"),
            },
        );
        pending.insert(5, PendingRequest::Generic);
        assert_eq!(pending.len(), 5);

        match pending.remove(3) {
            Some(PendingRequest::ProducePartition { topic, partition }) => {
                assert_eq!(&*topic, "test-topic");
                assert_eq!(partition, 3);
            }
            _ => panic!("expected ProducePartition"),
        }

        match pending.remove(4) {
            Some(PendingRequest::CreateEntity { name }) => {
                assert_eq!(&*name, "my-queue");
            }
            _ => panic!("expected CreateEntity"),
        }
    }

    #[test]
    fn pending_requests_default() {
        let pending = PendingRequests::default();
        assert!(pending.is_empty());
    }

    // ---- ResponseEntry extended tests ----

    #[test]
    fn response_entry_from_error() {
        use crate::types::{EntityKind, MqError};
        let err = MqResponse::Error(MqError::NotFound {
            entity: EntityKind::Topic,
            id: 42,
        });
        let entry = ResponseEntry::from_response(99, &err);
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert!(!entry.is_ok());
        assert_eq!(entry.log_index(), 99);
        assert_eq!(entry.buf.len() % 8, 0);
    }

    #[test]
    fn response_entry_from_messages() {
        // Messages variant falls through to Ok (fallback).
        let resp = MqResponse::Messages {
            messages: Default::default(),
        };
        let entry = ResponseEntry::from_response(50, &resp);
        // Complex responses fall back to TAG_OK.
        assert_eq!(entry.tag(), ResponseEntry::TAG_OK);
        assert_eq!(entry.log_index(), 50);
    }

    #[test]
    fn response_entry_from_batch_response() {
        let resp = MqResponse::BatchResponse(Box::new(smallvec::smallvec![
            MqResponse::Ok,
            MqResponse::Published {
                base_offset: 0,
                count: 1
            },
        ]));
        let entry = ResponseEntry::from_response(60, &resp);
        // BatchResponse falls back to TAG_OK.
        assert_eq!(entry.tag(), ResponseEntry::TAG_OK);
    }

    #[test]
    fn response_entry_error_empty_message() {
        let entry = ResponseEntry::error(1, 0, "");
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert_eq!(entry.log_index(), 1);
        let msg_len = u32::from_le_bytes(entry.buf[20..24].try_into().unwrap()) as usize;
        assert_eq!(msg_len, 0);
        assert_eq!(entry.buf.len() % 8, 0);
    }

    #[test]
    fn response_entry_error_long_message() {
        let long_msg = "a]".repeat(500);
        let entry = ResponseEntry::error(2, 99, &long_msg);
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert_eq!(entry.log_index(), 2);
        let error_code = u32::from_le_bytes(entry.buf[16..20].try_into().unwrap());
        assert_eq!(error_code, 99);
        let msg_len = u32::from_le_bytes(entry.buf[20..24].try_into().unwrap()) as usize;
        assert_eq!(msg_len, long_msg.len());
        assert_eq!(&entry.buf[24..24 + msg_len], long_msg.as_bytes());
        assert_eq!(entry.buf.len() % 8, 0);
    }

    // ---- HighWaterMark extended tests ----

    #[tokio::test]
    async fn hwm_shutdown_sentinel() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        let handle = tokio::spawn(async move { hwm2.wait_for(0).await });

        tokio::task::yield_now().await;

        // Advance with shutdown sentinel.
        hwm.advance(u64::MAX);

        let val = handle.await.unwrap();
        assert_eq!(val, u64::MAX);
    }

    #[tokio::test]
    async fn hwm_multiple_waiters() {
        let hwm = Arc::new(HighWaterMark::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let hwm2 = Arc::clone(&hwm);
            handles.push(tokio::spawn(async move { hwm2.wait_for(0).await }));
        }

        tokio::task::yield_now().await;
        hwm.advance(42);

        for h in handles {
            assert_eq!(h.await.unwrap(), 42);
        }
    }

    #[tokio::test]
    async fn hwm_sequential_advances() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        // First wait.
        hwm.advance(5);
        let val = hwm2.wait_for(0).await;
        assert_eq!(val, 5);

        // Second wait from new cursor.
        let hwm3 = Arc::clone(&hwm);
        let handle = tokio::spawn(async move { hwm3.wait_for(5).await });
        tokio::task::yield_now().await;
        hwm.advance(10);
        assert_eq!(handle.await.unwrap(), 10);
    }

    // ---- ClientRegistry extended tests ----

    #[tokio::test]
    async fn client_registry_ids_are_unique() {
        let registry = ClientRegistry::new(2, 64);
        let noop: ResponseCallback = Arc::new(|_, _, _, _| {});
        let id1 = registry.register(Arc::clone(&noop));
        let id2 = registry.register(Arc::clone(&noop));
        let id3 = registry.register(Arc::clone(&noop));
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn client_registry_send_multiple_responses() {
        use std::sync::atomic::AtomicUsize;
        let registry = ClientRegistry::new(2, 64);
        let count = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(tokio::sync::Notify::new());
        let count_cb = Arc::clone(&count);
        let done_cb = Arc::clone(&done);
        let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
            if !is_done {
                if count_cb.fetch_add(1, Ordering::Relaxed) + 1 >= 10 {
                    done_cb.notify_one();
                }
            }
        });
        let id = registry.register(cb);
        let p = (id as usize) % registry.num_partitions();
        for i in 0..10u64 {
            let mut frame = BytesMut::with_capacity(24);
            ResponseEntry::encode_frame_into(&mut frame, id, i, &MqResponse::Ok);
            registry.send_to_partition(p, frame.freeze());
        }
        done.notified().await;
        assert_eq!(count.load(Ordering::Relaxed), 10);
        registry.unregister(id);
    }

    // ---- Worker routing tests ----

    /// Helper to test routing logic without constructing a PartitionWorker.
    fn route_partition(cmd: &MqCommand, log_index: u64, num_partitions: usize) -> usize {
        match cmd.tag() {
            MqCommand::TAG_CREATE_TOPIC
            | MqCommand::TAG_DELETE_TOPIC
            | MqCommand::TAG_CREATE_EXCHANGE
            | MqCommand::TAG_DELETE_EXCHANGE
            | MqCommand::TAG_CREATE_CONSUMER_GROUP
            | MqCommand::TAG_DELETE_CONSUMER_GROUP
            | MqCommand::TAG_CREATE_BINDING
            | MqCommand::TAG_DELETE_BINDING
            | MqCommand::TAG_CREATE_SESSION
            | MqCommand::TAG_DISCONNECT_SESSION => (log_index as usize) % num_partitions,
            _ => (cmd.primary_id() as usize) % num_partitions,
        }
    }

    #[test]
    fn worker_routing_structural_by_log_index() {
        let num_partitions = 4;

        // CREATE_TOPIC at log_index=9 → 9 % 4 = 1
        let cmd = MqCommand::create_topic("test", crate::types::RetentionPolicy::default(), 1);
        assert_eq!(route_partition(&cmd, 9, num_partitions), 1);
    }

    #[test]
    fn worker_routing_data_plane_by_primary_id() {
        let num_partitions = 4;

        let cmd = MqCommand::publish(7, &[]);
        assert_eq!(route_partition(&cmd, 0, num_partitions), 3); // 7 % 4 = 3

        let cmd = MqCommand::publish(8, &[]);
        assert_eq!(route_partition(&cmd, 0, num_partitions), 0); // 8 % 4 = 0
    }

    #[test]
    fn worker_routing_all_structural_tags() {
        let n = 4;
        let structural_cmds = vec![
            MqCommand::create_topic("t", crate::types::RetentionPolicy::default(), 0),
            MqCommand::delete_topic(1),
            MqCommand::create_exchange("e", crate::types::ExchangeType::Fanout),
            MqCommand::delete_exchange(1),
            MqCommand::create_consumer_group("g", 1),
            MqCommand::delete_consumer_group(1),
            MqCommand::create_session(1, "c", 30000, 0),
            MqCommand::disconnect_session(1, false),
        ];

        for cmd in &structural_cmds {
            // Structural commands at different log indices should spread across partitions.
            let mut seen = vec![false; n];
            for log_index in 0..n as u64 {
                seen[route_partition(cmd, log_index, n)] = true;
            }
            assert!(
                seen.iter().all(|&s| s),
                "tag {} should reach all partitions via log_index routing",
                cmd.tag()
            );
        }
    }

    #[test]
    fn worker_routing_data_plane_deterministic() {
        let n = 8;
        for topic_id in 0..100u64 {
            let cmd = MqCommand::publish(topic_id, &[]);
            let p1 = route_partition(&cmd, 1, n);
            let p2 = route_partition(&cmd, 999, n);
            // Same primary_id always maps to same partition regardless of log_index.
            assert_eq!(
                p1, p2,
                "data-plane routing should be independent of log_index"
            );
        }
    }

    #[test]
    fn worker_routing_batch_not_structural() {
        // TAG_BATCH should NOT match any structural tag, so it falls through
        // to data-plane routing (which workers skip entirely for batch).
        let batch = MqCommand::batch(&[MqCommand::publish(1, &[])]);
        assert_eq!(batch.tag(), MqCommand::TAG_BATCH);

        // Verify TAG_BATCH is not in the structural set.
        let p = route_partition(&batch, 100, 4);
        // Should use primary_id routing, not log_index.
        let expected = (batch.primary_id() as usize) % 4;
        assert_eq!(p, expected);
    }

    #[test]
    fn worker_routing_commit_offset_by_primary_id() {
        let cmd = MqCommand::commit_offset(42, 1, 100);
        assert_eq!(route_partition(&cmd, 0, 8), (42usize) % 8);
    }

    #[test]
    fn worker_routing_single_partition() {
        // With 1 partition, everything goes to partition 0.
        let cmd = MqCommand::publish(999, &[]);
        assert_eq!(route_partition(&cmd, 123, 1), 0);

        let cmd = MqCommand::create_topic("t", crate::types::RetentionPolicy::default(), 0);
        assert_eq!(route_partition(&cmd, 456, 1), 0);
    }
}
