//! Write batcher — coalesces individual MqCommand submissions into batched
//! Raft proposals.
//!
//! When ingesting lots of small writes, each individual command would produce
//! a separate Raft proposal. The write batcher accumulates commands over a
//! short linger window (or until a count threshold is reached) and submits
//! them as a single coalesced `MqCommand::Batch`.
//!
//! Uses [`crossfire`] lock-free bounded channels for the ingestion queue.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use openraft::Raft;
use smallvec::SmallVec;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::MqTypeConfig;
use crate::types::{MqCommand, MqError, MqResponse};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the MQ write batcher.
#[derive(Debug, Clone)]
pub struct MqWriteBatcherConfig {
    /// How long to wait after the first request before flushing a batch.
    pub linger: Duration,
    /// Maximum number of commands to accumulate before flushing early.
    pub max_batch_count: usize,
    /// Crossfire channel capacity.
    pub channel_capacity: usize,
}

impl Default for MqWriteBatcherConfig {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(5),
            max_batch_count: 256,
            channel_capacity: 1024,
        }
    }
}

impl MqWriteBatcherConfig {
    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    pub fn with_max_batch_count(mut self, max: usize) -> Self {
        self.max_batch_count = max;
        self
    }

    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }
}

// ---------------------------------------------------------------------------
// SubmitBuffer — partitioned lock-free ingestion buffer
// ---------------------------------------------------------------------------
//
// Each partition contains one active `PartitionSeg`: a raw `BytesMut` buffer
// that producers write into using atomic `fetch_add` to claim byte ranges.
// The batcher seals the segment (sets bit 31 of `write_pos` atomically),
// installs a fresh segment, then spins briefly until all in-flight producer
// writes have committed. The stolen buffer is frozen to a `Bytes` zero-copy.
//
// Multi-partition drains build a vectored `MqCommand` (buf + extra segments)
// to avoid any concatenation copy. The raft encoder writes all segments in
// order with a single CRC pass.
//
// ## Sub-frame wire format (8-byte aligned)
//
// ```text
// [payload_len:4][client_id:4][cmd_bytes...][zero_padding to align8]
// ```
// `payload_len` = `4 + cmd_bytes.len()` (client_id field + cmd, not counting len itself).
// Each sub-frame is padded with zero bytes to the next 8-byte boundary.
// The first 16 bytes of each partition buffer are reserved for the
// `TAG_FORWARDED_BATCH` header, filled by the batcher after sealing.

/// Partition buffer capacity. Must be large enough to absorb one drain cycle
/// at maximum throughput. 1 MiB fits ~16K sub-frames of 64 B each.
const PARTITION_CAPACITY: u32 = 1 << 20; // 1 MiB

/// Bit 31 of `write_pos` is set by the batcher to seal the segment.
const SEALED_BIT: u32 = 1 << 31;

/// Round `n` up to the next multiple of 8 (u32 variant).
#[inline(always)]
const fn align8_u32(n: u32) -> u32 {
    (n + 7) & !7
}

/// One fixed-capacity buffer for a single partition.
///
/// # Layout
/// `data[0..16]`   — TAG_FORWARDED_BATCH header space (filled by batcher).
/// `data[16..]`    — sub-frames written by the producer.
///
/// # Concurrency
/// - `write_pos` is claimed by producers via CAS; bit 31 is the seal flag.
/// - `committed` is incremented by producers (Release) after each write.
/// - Batcher spins (Acquire) until `committed == final_write_pos` before reading.
struct PartitionSeg {
    /// Raw byte buffer. `BytesMut` so batcher can `split().freeze()` zero-copy.
    /// Producers write via raw pointer into the claimed range; `UnsafeCell` for
    /// interior mutability without a Mutex (single-producer per partition).
    data: UnsafeCell<BytesMut>,
    /// Claimed byte cursor. Producers CAS to reserve a range. Bit 31 = sealed.
    write_pos: AtomicU32,
    /// Bytes fully written by producers (Release). Equals write_pos when all done.
    committed: AtomicU32,
    /// Number of sub-frames written (for TAG_FORWARDED_BATCH count field).
    entry_count: AtomicU32,
    /// Buffer capacity (excludes the sealed bit).
    capacity: u32,
}

// Safety: producers write to non-overlapping ranges claimed via CAS; the batcher
// reads only after all producers have committed (spin-wait on `committed`).
unsafe impl Send for PartitionSeg {}
unsafe impl Sync for PartitionSeg {}

impl PartitionSeg {
    /// Allocate a new segment with the 16-byte header region pre-initialized.
    fn new(capacity: u32) -> Box<Self> {
        let mut data = BytesMut::with_capacity(capacity as usize);
        data.put_bytes(0, 16); // mark header region as initialized (len = 16)
        Box::new(Self {
            data: UnsafeCell::new(data),
            write_pos: AtomicU32::new(16), // producers start after the 16-byte header
            committed: AtomicU32::new(16),
            entry_count: AtomicU32::new(0),
            capacity,
        })
    }
}

/// Per-partition state: an atomically-swappable active segment plus an optional
/// oversized slot for writes that exceed `PARTITION_CAPACITY`.
struct Partition {
    /// Pointer to the currently active `PartitionSeg`. The batcher swaps this
    /// to a fresh segment during drain; the retired segment is exclusively owned
    /// by the batcher once `committed == final_write_pos`.
    active: AtomicPtr<PartitionSeg>,
    /// Single-slot queue for oversized sub-frames (entry_size > PARTITION_CAPACITY - 16).
    ///
    /// The writer allocates a segment sized exactly for the entry, writes it, and
    /// stores the pointer here. The batcher drains it alongside the normal active
    /// segment. Null when empty. Because `SubmitWriter` is `!Clone` (single
    /// producer per partition), only one oversized write can be in-flight at a
    /// time; `try_write` returns `false` if the slot is occupied.
    oversized: AtomicPtr<PartitionSeg>,
}

// Safety: PartitionSeg is heap-allocated (Box::into_raw) and never aliased after
// the batcher swaps `active` / `oversized`. The batcher waits for committed before
// reading any retired segment.
unsafe impl Send for Partition {}
unsafe impl Sync for Partition {}

impl Drop for Partition {
    fn drop(&mut self) {
        let seg = self.active.load(Ordering::Relaxed);
        if !seg.is_null() {
            unsafe { drop(Box::from_raw(seg)) };
        }
        let seg = self.oversized.load(Ordering::Relaxed);
        if !seg.is_null() {
            unsafe { drop(Box::from_raw(seg)) };
        }
    }
}

/// Shared waker state for the batcher. Coalesces rapid writes into one wakeup,
/// matching the `HighWaterMark` pattern: `pending` is set by producers and
/// cleared by the batcher; `Notify::notify_waiters()` wakes the sleeping batcher.
struct BatcherWaker {
    pending: AtomicBool,
    notify: tokio::sync::Notify,
    shutdown: AtomicBool,
}

impl BatcherWaker {
    /// Signal the batcher that new data is available (coalescing: N signals → 1 wakeup).
    #[inline]
    fn wake(&self) {
        // Only call notify_waiters() on the first signal since the last drain.
        // Additional signals while batcher is still draining are no-ops.
        if !self.pending.swap(true, Ordering::Release) {
            self.notify.notify_waiters();
        }
    }
}

/// Non-cloneable writer handle pinned to a single partition.
///
/// Each local client gets its own `SubmitWriter` via [`LocalBatcher::writer_for`].
/// Subsequent writes go directly to the pinned partition without any per-write
/// routing computation. The `client_id` is embedded at construction time.
///
/// `SubmitWriter` is `!Clone` intentionally — one writer per partition enforces
/// the single-producer invariant that makes lock-free writes safe.
pub struct SubmitWriter {
    partition: Arc<Partition>,
    waker: Arc<BatcherWaker>,
    client_id: u32,
}

impl SubmitWriter {
    /// Try to append a command sub-frame without blocking or spinning.
    ///
    /// Returns `true` if the entry was written. Returns `false` immediately —
    /// without any spin or `await` — if the active segment is sealed or full
    /// (i.e., the batcher has not yet drained). The caller is responsible for
    /// retrying, typically via the async [`write`](Self::write) wrapper.
    ///
    /// CAS contention from concurrent writers on the same partition is handled
    /// with a tight retry (no backpressure involved, purely a slot race), so
    /// the function remains O(1) in the common case.
    #[inline]
    pub fn try_write(&self, cmd: &[u8]) -> bool {
        let payload_len = (4 + cmd.len()) as u32;
        let entry_size = align8_u32(8 + cmd.len() as u32);

        // Oversized path: entry won't fit in a standard PARTITION_CAPACITY segment.
        // Allocate a dedicated segment sized exactly for this one entry and park
        // it in the partition's oversized slot. Returns false if the slot is
        // already occupied (previous oversized write not yet drained by batcher).
        if entry_size > PARTITION_CAPACITY - 16 {
            return self.try_write_oversized(cmd, payload_len, entry_size);
        }

        loop {
            let seg_ptr = self.partition.active.load(Ordering::Acquire);
            let seg = unsafe { &*seg_ptr };

            let pos = seg.write_pos.load(Ordering::Relaxed);
            if pos & SEALED_BIT != 0 || pos + entry_size > seg.capacity {
                return false;
            }
            if seg
                .write_pos
                .compare_exchange(pos, pos + entry_size, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue; // lost slot race to another writer, retry (no backpressure)
            }

            unsafe {
                let buf_ptr = (*seg.data.get()).as_ptr() as *mut u8;
                let dst = buf_ptr.add(pos as usize);
                (dst as *mut u32).write_unaligned(payload_len.to_le());
                (dst.add(4) as *mut u32).write_unaligned(self.client_id.to_le());
                ptr::copy_nonoverlapping(cmd.as_ptr(), dst.add(8), cmd.len());
                let pad = entry_size as usize - 8 - cmd.len();
                if pad > 0 {
                    ptr::write_bytes(dst.add(8 + cmd.len()), 0, pad);
                }
            }

            seg.committed.fetch_add(entry_size, Ordering::Release);
            seg.entry_count.fetch_add(1, Ordering::Relaxed);
            self.waker.wake();
            return true;
        }
    }

    /// Write a single entry into a freshly allocated oversized segment and park
    /// it in `partition.oversized`. No CAS needed: `SubmitWriter` is `!Clone`
    /// so only one producer touches this partition at a time.
    ///
    /// Returns `false` if the oversized slot is already occupied.
    #[cold]
    fn try_write_oversized(&self, cmd: &[u8], payload_len: u32, entry_size: u32) -> bool {
        // Bail if the previous oversized write hasn't been drained yet.
        if !self.partition.oversized.load(Ordering::Acquire).is_null() {
            return false;
        }

        // Allocate a segment sized for exactly this one entry (header + frame).
        let seg_capacity = 16 + entry_size;
        let mut seg = PartitionSeg::new(seg_capacity);

        // Write the sub-frame directly at offset 16 (header region is reserved).
        unsafe {
            let buf_ptr = (*seg.data.get()).as_ptr() as *mut u8;
            let dst = buf_ptr.add(16);
            (dst as *mut u32).write_unaligned(payload_len.to_le());
            (dst.add(4) as *mut u32).write_unaligned(self.client_id.to_le());
            ptr::copy_nonoverlapping(cmd.as_ptr(), dst.add(8), cmd.len());
            let pad = entry_size as usize - 8 - cmd.len();
            if pad > 0 {
                ptr::write_bytes(dst.add(8 + cmd.len()), 0, pad);
            }
        }

        // Mark the write as complete before publishing the pointer.
        seg.write_pos.store(seg_capacity, Ordering::Relaxed);
        seg.committed.store(seg_capacity, Ordering::Release);
        seg.entry_count.store(1, Ordering::Relaxed);

        self.partition
            .oversized
            .store(Box::into_raw(seg), Ordering::Release);
        self.waker.wake();
        true
    }

    /// Append a command sub-frame, yielding to the async runtime whenever the
    /// active segment is sealed or full (backpressure from the batcher).
    ///
    /// Implemented as a `try_write` loop: the fast path (segment available,
    /// CAS succeeds) completes without any `await`. Backpressure is signalled
    /// by `try_write` returning `false`; the loop yields and retries.
    #[inline]
    pub async fn write(&self, cmd: &[u8]) {
        while !self.try_write(cmd) {
            tokio::task::yield_now().await;
        }
    }
}

// ---------------------------------------------------------------------------
// LocalBatcher — pressure-based TAG_FORWARDED_BATCH proposer for local clients
// ---------------------------------------------------------------------------

/// Pressure-based batcher for local client commands.
///
/// Local clients are assigned dedicated [`SubmitWriter`]s via
/// [`writer_for`](LocalBatcher::writer_for). Each writer is pinned to one
/// partition (power-of-2 count, selected by `client_id & (N-1)`), ensuring a
/// single-producer per partition and eliminating write-side locking entirely.
///
/// The batcher loop wakes on coalesced notifications, seals each active
/// partition's segment atomically, installs a fresh segment, spins (nanoseconds)
/// until all in-flight writes commit, then proposes a single
/// `TAG_FORWARDED_BATCH` raft entry. For single-partition drains the stolen
/// `Bytes` becomes the command buffer directly (zero copy). For multi-partition
/// drains a vectored `MqCommand` is built — the raft encoder writes all
/// segments in order without a concatenation buffer.
///
/// Use `node_id = 0` to indicate responses should be dispatched to local
/// clients via [`ClientRegistry`](crate::async_apply::ClientRegistry).
pub struct LocalBatcher {
    partitions: Arc<Vec<Arc<Partition>>>,
    waker: Arc<BatcherWaker>,
    /// Power-of-2 partition count; use `client_id & mask` to select.
    mask: u32,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl LocalBatcher {
    /// Create a new `LocalBatcher` with `num_partitions` partitions (rounded up
    /// to the next power of two) and spawn the drain loop.
    pub fn new(raft: Raft<MqTypeConfig>, node_id: u32, num_partitions: usize) -> Self {
        let n = num_partitions.next_power_of_two().max(1);
        let partitions: Arc<Vec<Arc<Partition>>> = Arc::new(
            (0..n)
                .map(|_| {
                    Arc::new(Partition {
                        active: AtomicPtr::new(Box::into_raw(PartitionSeg::new(
                            PARTITION_CAPACITY,
                        ))),
                        oversized: AtomicPtr::new(ptr::null_mut()),
                    })
                })
                .collect(),
        );
        let waker = Arc::new(BatcherWaker {
            pending: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
            shutdown: AtomicBool::new(false),
        });
        let task = tokio::spawn(local_batcher_loop(
            Arc::clone(&partitions),
            Arc::clone(&waker),
            raft,
            node_id,
        ));
        Self {
            partitions,
            waker,
            mask: (n - 1) as u32,
            task: parking_lot::Mutex::new(Some(task)),
        }
    }

    /// Get a `SubmitWriter` pinned to the partition for `client_id`.
    ///
    /// The partition is selected by `client_id & mask` (power-of-2 modulo).
    /// Multiple clients may share a partition if there are more clients than
    /// partitions; the CAS in `SubmitWriter::write` handles this safely.
    pub fn writer_for(&self, client_id: u32) -> SubmitWriter {
        let idx = (client_id & self.mask) as usize;
        SubmitWriter {
            partition: Arc::clone(&self.partitions[idx]),
            waker: Arc::clone(&self.waker),
            client_id,
        }
    }

    /// Shut down the batcher and wait for the task to exit.
    pub async fn shutdown(self) {
        self.waker.shutdown.store(true, Ordering::Release);
        // Set pending=true so the loop skips its await even if notify_waiters()
        // fires while it is mid-drain (not yet sleeping on the Notify).
        self.waker.pending.store(true, Ordering::Release);
        self.waker.notify.notify_waiters();
        if let Some(task) = self.task.lock().take() {
            let _ = task.await;
        }
    }
}

/// Seal all partitions, spin-wait for in-flight writers to commit, steal the
/// buffers, and assemble a `TAG_FORWARDED_BATCH` `MqCommand`. Returns `None`
/// if all partitions were empty this cycle.
fn drain_partitions_to_cmd(partitions: &[Arc<Partition>], node_id: u32) -> Option<MqCommand> {
    // Keep buffers as BytesMut so we can write the header after summing total_count.
    // SmallVec inline for ≤8 partitions: no heap allocation.
    // Up to active + oversized per partition; inline capacity for 8 partitions × 2.
    let mut stolen: SmallVec<[(BytesMut, u32); 16]> = SmallVec::new();
    let mut total_count = 0u32;

    for partition in partitions.iter() {
        let old_ptr = partition.active.load(Ordering::Acquire);
        let old_seg = unsafe { &*old_ptr };

        // Seal: set bit 31 of write_pos atomically.
        // Returns the value *before* sealing; mask off the bit to get final position.
        let sealed_val = old_seg.write_pos.fetch_or(SEALED_BIT, Ordering::AcqRel);
        let final_write_pos = sealed_val & !SEALED_BIT;

        if final_write_pos == 16 {
            // Nothing written this cycle. Unseal so the partition stays usable
            // without allocating a new segment.
            old_seg.write_pos.fetch_and(!SEALED_BIT, Ordering::Relaxed);
            continue;
        }

        // Install a fresh segment so producers resume writing immediately.
        let new_seg = Box::into_raw(PartitionSeg::new(PARTITION_CAPACITY));
        partition.active.store(new_seg, Ordering::Release);

        // Spin until all in-flight producers on `old_seg` have committed.
        // Bounded: producers are between CAS-claim and committed.fetch_add (~ns).
        while old_seg.committed.load(Ordering::Acquire) < final_write_pos {
            std::hint::spin_loop();
        }

        let entry_count = old_seg.entry_count.load(Ordering::Relaxed);
        total_count += entry_count;

        // Steal the data as a mutable BytesMut (zero-copy split).
        // Safety: exclusive access guaranteed by committed == final_write_pos.
        let buf = unsafe {
            let bm = &mut *old_seg.data.get();
            // Extend visible len to cover all written bytes (producers wrote past len=16).
            bm.set_len(final_write_pos as usize);
            bm.split() // BytesMut: takes [0..final_write_pos], leaves bm empty
        };

        // Drop the retired PartitionSeg (its BytesMut is now empty after split()).
        // Safety: no producers reference old_seg after committed == final_write_pos.
        unsafe { drop(Box::from_raw(old_ptr)) };

        stolen.push((buf, entry_count));

        // Also drain the oversized slot for this partition, if populated.
        // The writer stores committed/entry_count before the pointer (Release),
        // so an Acquire load of a non-null pointer sees fully committed data.
        let ov_ptr = partition.oversized.swap(ptr::null_mut(), Ordering::AcqRel);
        if !ov_ptr.is_null() {
            let ov_seg = unsafe { &*ov_ptr };
            let ov_end = ov_seg.committed.load(Ordering::Relaxed); // already committed
            let ov_count = ov_seg.entry_count.load(Ordering::Relaxed);
            total_count += ov_count;
            let buf = unsafe {
                let bm = &mut *ov_seg.data.get();
                bm.set_len(ov_end as usize);
                bm.split()
            };
            unsafe { drop(Box::from_raw(ov_ptr)) };
            stolen.push((buf, ov_count));
        }
    }

    // Drain oversized slots on partitions whose active segment was empty this
    // cycle (skipped by `continue` above, so their oversized wasn't touched yet).
    // Partitions that already ran the oversized drain in the first loop will
    // have null here and are skipped cheaply.
    for partition in partitions.iter() {
        let ov_ptr = partition.oversized.swap(ptr::null_mut(), Ordering::AcqRel);
        if ov_ptr.is_null() {
            continue;
        }
        let ov_seg = unsafe { &*ov_ptr };
        let ov_end = ov_seg.committed.load(Ordering::Relaxed);
        let ov_count = ov_seg.entry_count.load(Ordering::Relaxed);
        total_count += ov_count;
        let buf = unsafe {
            let bm = &mut *ov_seg.data.get();
            bm.set_len(ov_end as usize);
            bm.split()
        };
        unsafe { drop(Box::from_raw(ov_ptr)) };
        stolen.push((buf, ov_count));
    }

    if total_count == 0 {
        return None;
    }

    // ── Build MqCommand (zero-copy single, or vectored multi-partition) ──
    let cmd = if stolen.len() == 1 {
        // Fast path: single partition — write header and freeze in place.
        let (mut buf, _) = stolen.into_iter().next().unwrap();
        MqCommand::write_forwarded_batch_header(&mut buf, node_id, total_count);
        MqCommand {
            buf: buf.freeze(),
            extra: SmallVec::new(),
        }
    } else {
        // Vectored path: multi-partition — no concatenation copy.
        // Partition 0's buffer carries the TAG_FORWARDED_BATCH header.
        // Partitions 1..N contribute only their sub-frames (skip the 16-byte
        // header placeholder at the front of each buffer).
        let mut iter = stolen.into_iter();
        let (mut first, _) = iter.next().unwrap();
        MqCommand::write_forwarded_batch_header(&mut first, node_id, total_count);
        let buf = first.freeze();
        let extra: SmallVec<[bytes::Bytes; 1]> =
            iter.map(|(b, _)| b.freeze().slice(16..)).collect();
        MqCommand { buf, extra }
    };

    Some(cmd)
}

async fn local_batcher_loop(
    partitions: Arc<Vec<Arc<Partition>>>,
    waker: Arc<BatcherWaker>,
    raft: Raft<MqTypeConfig>,
    node_id: u32,
) {
    loop {
        // ── Wait for data (coalescing, HighWaterMark style) ──────────────────
        {
            let notified = waker.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !waker.pending.swap(false, Ordering::AcqRel) {
                notified.await;
                waker.pending.store(false, Ordering::Relaxed);
            }
        }

        if waker.shutdown.load(Ordering::Relaxed) {
            return;
        }

        if let Some(cmd) = drain_partitions_to_cmd(&partitions, node_id) {
            if let Err(e) = raft.client_write(cmd).await {
                debug!(error = %e, "local batcher raft proposal failed");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum MqBatcherError {
    #[error("batcher channel closed")]
    ChannelClosed,
    #[error("response channel dropped")]
    ResponseDropped,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct BatchedRequest {
    command: MqCommand,
    response_tx: oneshot::Sender<MqResponse>,
}

/// Tracks how to distribute a merged response back to original callers.
enum ResponseSlot {
    /// Single original caller — forward response directly.
    Single(oneshot::Sender<MqResponse>),
    /// Merged Publish callers — split `Published` by message count per caller.
    MergedPublish(Vec<(oneshot::Sender<MqResponse>, usize)>),
}

// ---------------------------------------------------------------------------
// MqWriteBatcher
// ---------------------------------------------------------------------------

/// Coalesces individual `MqCommand` submissions into batched Raft proposals.
///
/// Thread-safe and designed to be shared via `Arc`.
pub struct MqWriteBatcher {
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<BatchedRequest>>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
    // Pre-initialized metrics handles.
    m_flush_count: metrics::Counter,
    m_flush_linger: metrics::Counter,
    m_commands_batched: metrics::Histogram,
}

impl MqWriteBatcher {
    /// Create a new `MqWriteBatcher` and spawn the batcher loop.
    pub fn new(
        config: MqWriteBatcherConfig,
        raft: Raft<MqTypeConfig>,
        group_id: u64,
        catalog_name: &str,
    ) -> Self {
        let (tx, rx) = crossfire::mpsc::bounded_async::<BatchedRequest>(config.channel_capacity);

        let catalog = catalog_name.to_owned();
        let group_label = group_id.to_string();
        let m_flush_count = metrics::counter!(
            "mq.batcher.flushes",
            "catalog" => catalog.clone(),
            "group" => group_label.clone(),
            "reason" => "count"
        );
        let m_flush_linger = metrics::counter!(
            "mq.batcher.flushes",
            "catalog" => catalog.clone(),
            "group" => group_label.clone(),
            "reason" => "linger"
        );
        let m_commands_batched = metrics::histogram!(
            "mq.batcher.commands_per_flush",
            "catalog" => catalog,
            "group" => group_label
        );

        let task = tokio::spawn(batcher_loop(
            rx,
            raft,
            config,
            m_flush_count.clone(),
            m_flush_linger.clone(),
            m_commands_batched.clone(),
        ));

        Self {
            tx,
            task: parking_lot::Mutex::new(Some(task)),
            m_flush_count,
            m_flush_linger,
            m_commands_batched,
        }
    }

    /// Create a test batcher that routes commands through the given engine.
    ///
    /// Each submitted command is applied directly (no batching, no Raft).
    /// Engine uses interior mutability (papaya + atomics), no Mutex needed.
    #[cfg(any(test, feature = "test-util"))]
    pub fn new_test(engine: std::sync::Arc<crate::engine::MqEngine>) -> Self {
        let (tx, rx) = crossfire::mpsc::bounded_async::<BatchedRequest>(1024);
        let task = tokio::spawn(async move {
            let mut log_index = 1u64;
            while let Ok(req) = rx.recv().await {
                let resp = engine.apply_command(&req.command, log_index, log_index * 1000, None);
                log_index += 1;
                let _ = req.response_tx.send(resp);
            }
        });
        Self {
            tx,
            task: parking_lot::Mutex::new(Some(task)),
            m_flush_count: metrics::counter!("test.flush_count"),
            m_flush_linger: metrics::counter!("test.flush_linger"),
            m_commands_batched: metrics::histogram!("test.commands_batched"),
        }
    }

    /// Submit a single command. Blocks (async) until the coalesced Raft
    /// proposal completes and the response is available.
    pub async fn submit(&self, command: MqCommand) -> Result<MqResponse, MqBatcherError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = BatchedRequest {
            command,
            response_tx,
        };

        self.tx
            .send(request)
            .await
            .map_err(|_| MqBatcherError::ChannelClosed)?;

        response_rx
            .await
            .map_err(|_| MqBatcherError::ResponseDropped)
    }

    /// Shut down the batcher. Drops the sender so the loop exits, then
    /// awaits task completion.
    pub async fn shutdown(self) {
        drop(self.tx);
        if let Some(task) = self.task.lock().take() {
            let _ = task.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Batcher loop
// ---------------------------------------------------------------------------

/// Merge same-topic `Publish` commands in `pending` to reduce per-command overhead.
///
/// Returns `(merged_commands, response_slots)` where each slot knows how to
/// distribute the raft response back to original callers.
///
/// `publish_idx` is a caller-owned scratch map, reused across flushes to avoid
/// per-flush HashMap allocation.
fn merge_pending(
    pending: &mut Vec<BatchedRequest>,
    publish_idx: &mut HashMap<u64, usize>,
) -> (Vec<MqCommand>, Vec<ResponseSlot>) {
    let mut commands: Vec<MqCommand> = Vec::with_capacity(pending.len());
    let mut slots = Vec::with_capacity(pending.len());

    publish_idx.clear();

    for mut req in pending.drain(..) {
        match req.command.tag() {
            MqCommand::TAG_PUBLISH => {
                let topic_id = req.command.as_publish().topic_id();
                let msg_count = req.command.as_publish().message_count() as usize;
                if let Some(&idx) = publish_idx.get(&topic_id) {
                    // Merge: combine payload segments from both commands
                    // into a single scatter publish (zero-copy).
                    let mut all_msgs = commands[idx].take_publish_segments();
                    all_msgs.extend(req.command.take_publish_segments());
                    commands[idx] = MqCommand::publish_scatter(topic_id, all_msgs);
                    if let ResponseSlot::MergedPublish(ref mut callers) = slots[idx] {
                        callers.push((req.response_tx, msg_count));
                    }
                } else {
                    let idx = commands.len();
                    publish_idx.insert(topic_id, idx);
                    commands.push(req.command);
                    slots.push(ResponseSlot::MergedPublish(vec![(
                        req.response_tx,
                        msg_count,
                    )]));
                }
            }
            _ => {
                commands.push(req.command);
                slots.push(ResponseSlot::Single(req.response_tx));
            }
        }
    }

    (commands, slots)
}

/// Distribute a response to its original caller(s) via the slot.
fn dispatch_response(slot: ResponseSlot, response: MqResponse) {
    match slot {
        ResponseSlot::Single(tx) => {
            let _ = tx.send(response);
        }
        ResponseSlot::MergedPublish(callers) => {
            if callers.len() == 1 {
                let _ = callers.into_iter().next().unwrap().0.send(response);
                return;
            }
            match response {
                MqResponse::Published {
                    base_offset,
                    count: _,
                } => {
                    let mut consumed = 0u64;
                    for (tx, caller_count) in callers {
                        let _ = tx.send(MqResponse::Published {
                            base_offset: base_offset + consumed,
                            count: caller_count as u64,
                        });
                        consumed += caller_count as u64;
                    }
                }
                MqResponse::Error(_) => {
                    for (tx, _) in callers {
                        let _ = tx.send(response.clone());
                    }
                }
                other => {
                    for (tx, _) in callers {
                        let _ = tx.send(other.clone());
                    }
                }
            }
        }
    }
}

async fn batcher_loop(
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<BatchedRequest>>,
    raft: Raft<MqTypeConfig>,
    config: MqWriteBatcherConfig,
    m_flush_count: metrics::Counter,
    m_flush_linger: metrics::Counter,
    m_commands_batched: metrics::Histogram,
) {
    let mut pending: Vec<BatchedRequest> = Vec::with_capacity(config.max_batch_count);
    // Reusable scratch map for merge_pending — avoids per-flush HashMap allocation.
    let mut publish_idx: HashMap<u64, usize> = HashMap::new();

    loop {
        // Step 1: Block until the first request arrives.
        let first = match rx.recv().await {
            Ok(req) => req,
            Err(_) => {
                debug!("mq batcher_loop: channel closed, exiting");
                return;
            }
        };

        pending.clear();
        pending.push(first);

        // Step 2: If below threshold, wait linger then drain.
        if pending.len() < config.max_batch_count {
            tokio::time::sleep(config.linger).await;

            loop {
                match rx.try_recv() {
                    Ok(req) => {
                        pending.push(req);
                        if pending.len() >= config.max_batch_count {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        let num_commands = pending.len();
        let flushed_by_count = num_commands >= config.max_batch_count;

        // Step 3: Merge same-topic Publishes.
        let (commands, slots) = merge_pending(&mut pending, &mut publish_idx);

        // Step 4: Build command and propose through Raft.
        if commands.len() == 1 {
            // Single-command fast path: no Batch wrapper.
            let cmd = commands.into_iter().next().unwrap();
            let slot = slots.into_iter().next().unwrap();
            match raft.client_write(cmd).await {
                Ok(resp) => {
                    dispatch_response(slot, resp.response().clone());
                }
                Err(e) => {
                    warn!("mq batcher: raft error: {}", e);
                    dispatch_response(
                        slot,
                        MqResponse::Error(MqError::Custom(format!("raft error: {}", e))),
                    );
                }
            }
        } else {
            let batch_cmd = MqCommand::batch(&commands);

            match raft.client_write(batch_cmd).await {
                Ok(resp) => {
                    let response = resp.response().clone();
                    match response {
                        MqResponse::BatchResponse(responses) => {
                            for (slot, individual_response) in
                                slots.into_iter().zip(responses.into_iter())
                            {
                                dispatch_response(slot, individual_response);
                            }
                        }
                        other => {
                            for slot in slots {
                                dispatch_response(slot, other.clone());
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("mq batcher: raft batch error: {}", e);
                    let error_resp =
                        MqResponse::Error(MqError::Custom(format!("raft error: {}", e)));
                    for slot in slots {
                        dispatch_response(slot, error_resp.clone());
                    }
                }
            }
        }

        if flushed_by_count {
            m_flush_count.increment(1);
        } else {
            m_flush_linger.increment(1);
        }
        m_commands_batched.record(num_commands as f64);

        debug!(commands = num_commands, "mq batcher_loop: flushed batch");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = MqWriteBatcherConfig::default();
        assert_eq!(config.linger, Duration::from_millis(5));
        assert_eq!(config.max_batch_count, 256);
        assert_eq!(config.channel_capacity, 1024);
    }

    #[test]
    fn test_config_builder() {
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_millis(10))
            .with_max_batch_count(512)
            .with_channel_capacity(2048);
        assert_eq!(config.linger, Duration::from_millis(10));
        assert_eq!(config.max_batch_count, 512);
        assert_eq!(config.channel_capacity, 2048);
    }

    #[test]
    fn test_error_display() {
        let e = MqBatcherError::ChannelClosed;
        assert_eq!(e.to_string(), "batcher channel closed");
        let e = MqBatcherError::ResponseDropped;
        assert_eq!(e.to_string(), "response channel dropped");
    }
}
