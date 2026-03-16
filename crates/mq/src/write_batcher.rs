//! Write batcher — coalesces individual MqCommand submissions into batched
//! Raft proposals.
//!
//! ## MqWriteBatcher
//!
//! External clients submit commands via [`MqWriteBatcher::submit`]. The batcher
//! accumulates requests over a short linger window (or until a count threshold),
//! merges same-topic publishes, and proposes a single `MqCommand::Batch` to Raft.
//!
//! ## LocalBatcher
//!
//! Local connection tasks submit pre-framed sub-frame bytes via [`LocalWriter`].
//! The connection layer owns framing: each [`LocalFrameBatch`] is already in the
//! sub-frame wire format expected by `TAG_FORWARDED_BATCH`:
//!
//! ```text
//! [payload_len:4][client_id:4][request_seq:8][cmd_bytes...][opt_pad]
//! ```
//!
//! The [`LocalBatcher`] drain loop collects frames from a crossfire MPSC channel,
//! accumulates them until the linger deadline or count threshold fires, then
//! builds a vectored `MqCommand` (header buf + extra frame chunks) and proposes
//! it to Raft — zero copies of command bytes anywhere in the pipeline.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use openraft::Raft;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::MqTypeConfig;
use crate::async_apply::{AsyncApplyManager, ResponseEntry};
use crate::types::{MqCommand, MqError};

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

    pub fn with_max_batch_count(mut self, max_batch_count: usize) -> Self {
        self.max_batch_count = max_batch_count;
        self
    }

    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity;
        self
    }
}

// ---------------------------------------------------------------------------
// LocalBatcher — zero-copy TAG_FORWARDED_BATCH proposer for local connections
// ---------------------------------------------------------------------------

/// A batch of pre-framed sub-frames ready to hand to the [`LocalBatcher`].
///
/// Each `bytes` value contains one or more contiguous wire-format sub-frames:
/// ```text
/// [payload_len:4][client_id:4][request_seq:8][cmd_bytes...]...
/// ```
/// `count` is the number of sub-frames packed into `bytes`. Connection tasks
/// that write multiple sub-frames into one allocation set `count > 1` to
/// amortize refcount overhead; the batcher uses `count` to enforce the
/// `max_batch_count` threshold without scanning bytes.
pub struct LocalFrameBatch {
    pub bytes: Bytes,
    pub count: u32,
}

/// Clone-able writer handle for submitting [`LocalFrameBatch`]es to a
/// [`LocalBatcher`].
///
/// Obtained via [`LocalBatcher::writer`]. Multiple connection tasks may hold
/// independent clones; the underlying crossfire multi-producer sender handles
/// concurrent sends safely without locking.
#[derive(Clone)]
pub struct LocalWriter {
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<LocalFrameBatch>>,
}

impl LocalWriter {
    /// Send a batch of pre-framed sub-frames to the batcher.
    ///
    /// Awaits if the channel is at capacity (backpressure from the batcher not
    /// keeping up with Raft proposal throughput).
    pub async fn send(&self, batch: LocalFrameBatch) -> Result<(), MqBatcherError> {
        self.tx
            .send(batch)
            .await
            .map_err(|_| MqBatcherError::ChannelClosed)
    }
}

/// Pressure-based batcher for local connection commands.
///
/// Connections submit pre-framed sub-frame bytes via [`LocalWriter`]s obtained
/// from [`LocalBatcher::writer`]. The batcher drain loop accumulates frames
/// until the linger deadline or `max_batch_count` fires, then builds a vectored
/// `TAG_FORWARDED_BATCH` [`MqCommand`] (24-byte header buf + frame chunks as
/// extra) and proposes it through Raft — zero copies of command bytes.
///
/// Use `node_id = 0` for locally-originated batches; the state machine routes
/// responses to local clients via the in-process [`ClientRegistry`].
///
/// [`ClientRegistry`]: crate::async_apply::ClientRegistry
pub struct LocalBatcher {
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<LocalFrameBatch>>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl LocalBatcher {
    /// Create a new `LocalBatcher` and spawn the drain loop.
    pub fn new(raft: Raft<MqTypeConfig>, node_id: u32, config: MqWriteBatcherConfig) -> Self {
        let (tx, rx) = crossfire::mpsc::bounded_async::<LocalFrameBatch>(config.channel_capacity);
        let task = tokio::spawn(local_batcher_loop(rx, raft, config, node_id));
        Self {
            tx,
            task: parking_lot::Mutex::new(Some(task)),
        }
    }

    /// Return a clone-able [`LocalWriter`] that sends frames into this batcher.
    ///
    /// Each connection task should hold its own clone; all clones share the same
    /// bounded channel and experience backpressure together.
    pub fn writer(&self) -> LocalWriter {
        LocalWriter {
            tx: self.tx.clone(),
        }
    }

    /// Shut down the batcher: drop the sender so the drain loop exits, then
    /// await task completion.
    pub async fn shutdown(self) {
        drop(self.tx);
        if let Some(task) = self.task.lock().take() {
            let _ = task.await;
        }
    }

    /// Create a test batcher that captures built [`MqCommand`]s into `sink`
    /// rather than proposing through Raft.
    #[cfg(any(test, feature = "test-util"))]
    pub fn new_test(
        config: MqWriteBatcherConfig,
        node_id: u32,
        sink: tokio::sync::mpsc::UnboundedSender<MqCommand>,
    ) -> Self {
        let (tx, rx) = crossfire::mpsc::bounded_async::<LocalFrameBatch>(config.channel_capacity);
        let task = tokio::spawn(local_batcher_loop_test(rx, sink, config, node_id));
        Self {
            tx,
            task: parking_lot::Mutex::new(Some(task)),
        }
    }
}

/// Write a `TAG_FORWARDED_BATCH` header placeholder into `scratch` (16 zero bytes),
/// then patch `total_size` and `total_count` after all frame bytes have been appended.
#[inline]
fn patch_forwarded_batch_header(scratch: &mut BytesMut, total_count: u32, node_id: u32) {
    let total_size = scratch.len() as u32;
    scratch[0..4].copy_from_slice(&total_size.to_le_bytes());
    scratch[4..6].copy_from_slice(&16u16.to_le_bytes()); // fixed region size
    scratch[6] = MqCommand::TAG_FORWARDED_BATCH;
    scratch[7] = 0u8; // flags
    scratch[8..12].copy_from_slice(&node_id.to_le_bytes());
    scratch[12..16].copy_from_slice(&total_count.to_le_bytes());
}

async fn local_batcher_loop(
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<LocalFrameBatch>>,
    raft: Raft<MqTypeConfig>,
    config: MqWriteBatcherConfig,
    node_id: u32,
) {
    let mut scratch = BytesMut::new();
    loop {
        // Block until the first frame batch arrives.
        let first = match rx.recv().await {
            Ok(fb) => fb,
            Err(_) => return, // channel closed, all senders dropped
        };

        scratch.reserve(16 + first.bytes.len());
        scratch.put_bytes(0, 16); // header placeholder
        scratch.put_slice(&first.bytes);
        let mut total_count = first.count;

        // Drain more frames within the linger window or until max_batch_count.
        if total_count < config.max_batch_count as u32 {
            let deadline = Instant::now() + config.linger;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Ok(fb)) => {
                        total_count += fb.count;
                        scratch.put_slice(&fb.bytes);
                        if total_count >= config.max_batch_count as u32 {
                            break;
                        }
                    }
                    // Timeout expired or channel closed — flush what we have.
                    _ => break,
                }
            }
        }

        patch_forwarded_batch_header(&mut scratch, total_count, node_id);
        let cmd = MqCommand::split_from(&mut scratch);
        if let Err(e) = raft.client_write(cmd).await {
            debug!(error = %e, "local batcher raft proposal failed");
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
async fn local_batcher_loop_test(
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<LocalFrameBatch>>,
    sink: tokio::sync::mpsc::UnboundedSender<MqCommand>,
    config: MqWriteBatcherConfig,
    node_id: u32,
) {
    let mut scratch = BytesMut::new();
    loop {
        let first = match rx.recv().await {
            Ok(fb) => fb,
            Err(_) => return,
        };

        scratch.reserve(16 + first.bytes.len());
        scratch.put_bytes(0, 16); // header placeholder
        scratch.put_slice(&first.bytes);
        let mut total_count = first.count;

        if total_count < config.max_batch_count as u32 {
            let deadline = Instant::now() + config.linger;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Ok(fb)) => {
                        total_count += fb.count;
                        scratch.put_slice(&fb.bytes);
                        if total_count >= config.max_batch_count as u32 {
                            break;
                        }
                    }
                    _ => break,
                }
            }
        }

        patch_forwarded_batch_header(&mut scratch, total_count, node_id);
        let cmd = MqCommand::split_from(&mut scratch);
        if sink.send(cmd).is_err() {
            return; // sink closed
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
    response_tx: oneshot::Sender<ResponseEntry>,
}

/// Tracks how to distribute a merged response back to original callers.
enum ResponseSlot {
    /// Single original caller — forward response directly.
    Single(oneshot::Sender<ResponseEntry>),
    /// Merged Publish callers — split `Published` by message count per caller.
    MergedPublish(Vec<(oneshot::Sender<ResponseEntry>, usize)>),
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
    ///
    /// `async_apply` — when `Some`, the batcher uses the batch registry for response delivery
    /// (TAG_BATCH applied by designated worker). When `None`, all TAG_BATCH commands are
    /// applied synchronously inside the batcher loop (legacy/fallback path).
    pub fn new(
        config: MqWriteBatcherConfig,
        raft: Raft<MqTypeConfig>,
        async_apply: Option<Arc<AsyncApplyManager>>,
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
            async_apply,
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
                let mut buf = BytesMut::new();
                engine.apply_command(&req.command, &mut buf, log_index, log_index * 1000, None);
                let resp = ResponseEntry::split_from(&mut buf);
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
    pub async fn submit(&self, command: MqCommand) -> Result<ResponseEntry, MqBatcherError> {
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
    let mut scratch = BytesMut::new();
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
                    MqCommand::write_publish_bytes(&mut scratch, topic_id, &all_msgs);
                    commands[idx] = MqCommand::split_from(&mut scratch);
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
fn dispatch_response(slot: ResponseSlot, response: ResponseEntry) {
    match slot {
        ResponseSlot::Single(tx) => {
            let _ = tx.send(response);
        }
        ResponseSlot::MergedPublish(callers) => {
            if callers.len() == 1 {
                let _ = callers.into_iter().next().unwrap().0.send(response);
                return;
            }
            if response.tag() == ResponseEntry::TAG_PUBLISHED {
                let base_offset = response.base_offset();
                let log_index = response.log_index();
                let mut consumed = 0u64;
                for (tx, caller_count) in callers {
                    let mut buf = BytesMut::with_capacity(32);
                    ResponseEntry::write_published(
                        &mut buf,
                        log_index,
                        base_offset + consumed,
                        caller_count as u64,
                    );
                    let _ = tx.send(ResponseEntry::split_from(&mut buf));
                    consumed += caller_count as u64;
                }
            } else {
                for (tx, _) in callers {
                    let _ = tx.send(response.clone());
                }
            }
        }
    }
}

async fn batcher_loop(
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<BatchedRequest>>,
    raft: Raft<MqTypeConfig>,
    async_apply: Option<Arc<AsyncApplyManager>>,
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

        // Step 4: Build TAG_BATCH and propose through Raft.
        // When async_apply is present, allocate a batch_id and register an oneshot so
        // the designated worker can deliver the ResponseEntry after applying the batch.
        // When async_apply is absent (legacy/test path), the raft response is not used
        // for response delivery (callers should use new_test or arrange delivery otherwise).
        {
            let (batch_id, resp_rx) = if let Some(ref am) = async_apply {
                let (tx, rx) = oneshot::channel::<ResponseEntry>();
                let id = am.alloc_batch(tx);
                (id, Some(rx))
            } else {
                (0u32, None)
            };

            let mut scratch = BytesMut::new();
            MqCommand::write_batch_with_id(&mut scratch, batch_id, &commands);
            let batch_cmd = MqCommand::split_from(&mut scratch);

            match raft.client_write(batch_cmd).await {
                Ok(_resp) => {
                    if let Some(rx) = resp_rx {
                        // Raft response is MqApplyResponse (log_index only). Wait for worker
                        // to deliver the actual ResponseEntry via the batch_registry oneshot.
                        match rx.await {
                            Ok(response) => {
                                if response.tag() == ResponseEntry::TAG_BATCH {
                                    for (slot, individual_response) in
                                        slots.into_iter().zip(response.batch_entries())
                                    {
                                        dispatch_response(slot, individual_response);
                                    }
                                } else {
                                    for slot in slots {
                                        dispatch_response(slot, response.clone());
                                    }
                                }
                            }
                            Err(_) => {
                                // Worker dropped the sender (e.g. shutdown).
                                let mut buf = BytesMut::new();
                                ResponseEntry::write_mq_error(
                                    &mut buf,
                                    0,
                                    &MqError::Custom("batch response dropped".to_string()),
                                );
                                let error_resp = ResponseEntry::split_from(&mut buf);
                                for slot in slots {
                                    dispatch_response(slot, error_resp.clone());
                                }
                            }
                        }
                    } else {
                        // No async_apply: no response delivery from this path.
                        // Callers using new_test or non-async paths handle responses separately.
                        let mut buf = BytesMut::new();
                        ResponseEntry::write_ok(&mut buf, 0);
                        let ok_resp = ResponseEntry::split_from(&mut buf);
                        for slot in slots {
                            dispatch_response(slot, ok_resp.clone());
                        }
                    }
                }
                Err(e) => {
                    // Remove the batch_id from the registry since we won't get a worker response.
                    if let Some(ref am) = async_apply {
                        am.batch_registry.lock().remove(&batch_id);
                    }
                    warn!("mq batcher: raft batch error: {}", e);
                    let mut buf = BytesMut::new();
                    ResponseEntry::write_mq_error(
                        &mut buf,
                        0,
                        &MqError::Custom(format!("raft error: {}", e)),
                    );
                    let error_resp = ResponseEntry::split_from(&mut buf);
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
    use bisque_raft::codec::Encode;

    use super::*;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// Build a `TAG_FORWARDED_BATCH` `MqCommand` from pre-built sub-frames.
    ///
    /// Used in unit tests to construct commands for `as_forwarded_batch()` inspection
    /// without going through the async batcher loop.
    fn build_forwarded_batch_cmd(frames: Vec<Bytes>, count: u32, node_id: u32) -> MqCommand {
        let total_frame_bytes: usize = frames.iter().map(|f| f.len()).sum();
        let mut scratch = BytesMut::with_capacity(16 + total_frame_bytes);
        scratch.put_bytes(0, 16); // header placeholder
        for frame in frames {
            scratch.put_slice(&frame);
        }
        patch_forwarded_batch_header(&mut scratch, count, node_id);
        MqCommand::split_from(&mut scratch)
    }

    /// Build a minimal sub-frame Bytes in the TAG_FORWARDED_BATCH sub-frame wire format:
    /// `[payload_len:4][client_id:4][request_seq:8][cmd_bytes...]`
    fn make_sub_frame(client_id: u32, request_seq: u64, cmd: &[u8]) -> Bytes {
        let payload_len = (12 + cmd.len()) as u32;
        let mut buf = BytesMut::with_capacity(4 + payload_len as usize);
        buf.put_u32_le(payload_len);
        buf.put_u32_le(client_id);
        buf.put_u64_le(request_seq);
        buf.extend_from_slice(cmd);
        buf.freeze()
    }

    /// Encode a vectored MqCommand to a flat buffer and decode it back so that
    /// `as_forwarded_batch()` (which only reads `buf`) can inspect sub-frames.
    fn flatten(cmd: MqCommand) -> MqCommand {
        let mut encoded = Vec::with_capacity(cmd.total_encoded_size());
        cmd.encode(&mut encoded).unwrap();
        MqCommand::from_vec(encoded)
    }

    /// Create a `LocalBatcher` backed by a test sink and return the captured command channel.
    fn make_test_batcher(
        config: MqWriteBatcherConfig,
        node_id: u32,
    ) -> (
        LocalBatcher,
        tokio::sync::mpsc::UnboundedReceiver<MqCommand>,
    ) {
        let (sink_tx, sink_rx) = tokio::sync::mpsc::unbounded_channel();
        let batcher = LocalBatcher::new_test(config, node_id, sink_tx);
        (batcher, sink_rx)
    }

    // -------------------------------------------------------------------------
    // build_forwarded_batch_cmd — unit tests (no async, no Raft)
    // -------------------------------------------------------------------------

    #[test]
    fn build_cmd_tag_and_header_fields() {
        let frame = make_sub_frame(42, 7, b"hello");
        let cmd = build_forwarded_batch_cmd(vec![frame], 1, 99);

        assert_eq!(cmd.tag(), MqCommand::TAG_FORWARDED_BATCH);
        // node_id at buf[8..12]
        let node_id = u32::from_le_bytes(cmd.buf[8..12].try_into().unwrap());
        assert_eq!(node_id, 99);
        // count at buf[12..16]
        let count = u32::from_le_bytes(cmd.buf[12..16].try_into().unwrap());
        assert_eq!(count, 1);
        // Sub-frames start at offset 16 (no batch_seq field).
    }

    #[test]
    fn build_cmd_total_size_header() {
        let frame = make_sub_frame(1, 0, b"payload");
        let frame_len = frame.len();
        let cmd = build_forwarded_batch_cmd(vec![frame], 1, 0);

        // Size field at buf[0..4] must equal 16 (header) + frame bytes
        let size = u32::from_le_bytes(cmd.buf[0..4].try_into().unwrap()) as usize;
        assert_eq!(size, 16 + frame_len);
        assert_eq!(size, cmd.total_encoded_size());
    }

    #[test]
    fn build_cmd_two_frames_contiguous_in_buf() {
        let frame_a = make_sub_frame(1, 1, b"aaa");
        let frame_b = make_sub_frame(2, 2, b"bbb");
        let total = frame_a.len() + frame_b.len();

        let cmd = build_forwarded_batch_cmd(vec![frame_a, frame_b], 2, 0);

        // All frame bytes are concatenated after the 16-byte header.
        assert_eq!(cmd.buf.len(), 16 + total);
    }

    #[test]
    fn build_cmd_encode_decode_roundtrip_single_frame() {
        let frame = make_sub_frame(10, 20, b"cmd-data");
        let cmd = build_forwarded_batch_cmd(vec![frame], 1, 5);
        let flat = flatten(cmd);

        let view = flat.as_forwarded_batch();
        assert_eq!(view.node_id(), 5);
        assert_eq!(view.count(), 1);
        let entries: Vec<_> = view.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 10); // client_id
        assert_eq!(entries[0].1, 20); // request_seq
        assert_eq!(entries[0].2, b"cmd-data");
    }

    #[test]
    fn build_cmd_encode_decode_roundtrip_multi_frame() {
        let frames = vec![
            make_sub_frame(1, 100, b"alpha"),
            make_sub_frame(2, 200, b"beta"),
            make_sub_frame(3, 300, b"gamma"),
        ];
        let cmd = build_forwarded_batch_cmd(frames, 3, 7);
        let flat = flatten(cmd);

        let view = flat.as_forwarded_batch();
        assert_eq!(view.node_id(), 7);
        assert_eq!(view.count(), 3);

        let entries: Vec<_> = view.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            (entries[0].0, entries[0].1, entries[0].2),
            (1, 100, b"alpha".as_ref())
        );
        assert_eq!(
            (entries[1].0, entries[1].1, entries[1].2),
            (2, 200, b"beta".as_ref())
        );
        assert_eq!(
            (entries[2].0, entries[2].1, entries[2].2),
            (3, 300, b"gamma".as_ref())
        );
    }

    #[test]
    fn build_cmd_multi_subframe_single_bytes() {
        // Connection layer can pack multiple sub-frames into one Bytes allocation.
        // Build two sub-frames manually into one Bytes and set count=2.
        let sf1 = make_sub_frame(1, 1, b"x");
        let sf2 = make_sub_frame(2, 2, b"yy");
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&sf1);
        combined.extend_from_slice(&sf2);
        let combined = combined.freeze();

        let cmd = build_forwarded_batch_cmd(vec![combined], 2, 0);
        let flat = flatten(cmd);

        let view = flat.as_forwarded_batch();
        assert_eq!(view.count(), 2); // count reflects both sub-frames
        let entries: Vec<_> = view.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].2, b"x");
        assert_eq!(entries[1].2, b"yy");
    }

    #[test]
    fn build_cmd_empty_frames_still_valid() {
        // Zero sub-frames edge case: count=0, no frames.
        let cmd = build_forwarded_batch_cmd(vec![], 0, 0);
        assert_eq!(cmd.tag(), MqCommand::TAG_FORWARDED_BATCH);
        let size = u32::from_le_bytes(cmd.buf[0..4].try_into().unwrap()) as usize;
        assert_eq!(size, 16);
        assert_eq!(cmd.total_encoded_size(), 16);
    }

    // -------------------------------------------------------------------------
    // LocalWriter / LocalBatcher — async integration tests
    // -------------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_writer_send_single_frame_fires_on_linger() {
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_millis(10))
            .with_max_batch_count(256);
        let (batcher, mut sink) = make_test_batcher(config, 1);
        let writer = batcher.writer();

        let frame = make_sub_frame(5, 1, b"hello");
        writer
            .send(LocalFrameBatch {
                bytes: frame,
                count: 1,
            })
            .await
            .unwrap();

        // Wait long enough for linger to fire
        let cmd = tokio::time::timeout(Duration::from_millis(100), sink.recv())
            .await
            .expect("timed out waiting for batch")
            .expect("sink closed");

        assert_eq!(cmd.tag(), MqCommand::TAG_FORWARDED_BATCH);
        let count = u32::from_le_bytes(cmd.buf[12..16].try_into().unwrap());
        assert_eq!(count, 1);

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_fires_at_count_threshold_before_linger() {
        // max_batch_count=3, send 3 frames rapidly — should fire before linger expires.
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_secs(60)) // huge linger — must fire on count
            .with_max_batch_count(3)
            .with_channel_capacity(16);
        let (batcher, mut sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

        for i in 0u32..3 {
            writer
                .send(LocalFrameBatch {
                    bytes: make_sub_frame(i, i as u64, b"x"),
                    count: 1,
                })
                .await
                .unwrap();
        }

        let cmd = tokio::time::timeout(Duration::from_millis(500), sink.recv())
            .await
            .expect("timed out — count threshold did not fire")
            .unwrap();

        let flat = flatten(cmd);
        assert_eq!(flat.as_forwarded_batch().count(), 3);

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_count_accumulates_across_multi_subframe_batches() {
        // Send two LocalFrameBatches each carrying count=2; total=4 → fires at threshold=4.
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_secs(60))
            .with_max_batch_count(4)
            .with_channel_capacity(16);
        let (batcher, mut sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

        for _ in 0..2 {
            let sf1 = make_sub_frame(1, 1, b"a");
            let sf2 = make_sub_frame(2, 2, b"b");
            let mut combined = BytesMut::new();
            combined.extend_from_slice(&sf1);
            combined.extend_from_slice(&sf2);
            writer
                .send(LocalFrameBatch {
                    bytes: combined.freeze(),
                    count: 2,
                })
                .await
                .unwrap();
        }

        let cmd = tokio::time::timeout(Duration::from_millis(500), sink.recv())
            .await
            .expect("timed out")
            .unwrap();

        let flat = flatten(cmd);
        let view = flat.as_forwarded_batch();
        assert_eq!(view.count(), 4);
        // Iterator should yield 4 sub-frames
        assert_eq!(view.iter().count(), 4);

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_multiple_writers_same_channel() {
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_millis(20))
            .with_max_batch_count(256)
            .with_channel_capacity(64);
        let (batcher, mut sink) = make_test_batcher(config, 0);

        let writer_a = batcher.writer();
        let writer_b = batcher.writer();

        // Both writers send concurrently
        let send_a = async {
            writer_a
                .send(LocalFrameBatch {
                    bytes: make_sub_frame(1, 1, b"from-a"),
                    count: 1,
                })
                .await
                .unwrap();
        };
        let send_b = async {
            writer_b
                .send(LocalFrameBatch {
                    bytes: make_sub_frame(2, 2, b"from-b"),
                    count: 1,
                })
                .await
                .unwrap();
        };
        tokio::join!(send_a, send_b);

        let cmd = tokio::time::timeout(Duration::from_millis(200), sink.recv())
            .await
            .expect("timed out")
            .unwrap();

        let flat = flatten(cmd);
        let view = flat.as_forwarded_batch();
        assert_eq!(view.count(), 2);

        let entries: Vec<_> = view.iter().collect();
        assert_eq!(entries.len(), 2);
        // Both client_ids present (order may vary)
        let cids: std::collections::HashSet<u32> = entries.iter().map(|e| e.0).collect();
        assert!(cids.contains(&1));
        assert!(cids.contains(&2));

        drop(writer_a);
        drop(writer_b);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_node_id_propagated() {
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_millis(10))
            .with_max_batch_count(256);
        let (batcher, mut sink) = make_test_batcher(config, 42);
        let writer = batcher.writer();

        writer
            .send(LocalFrameBatch {
                bytes: make_sub_frame(0, 0, b"x"),
                count: 1,
            })
            .await
            .unwrap();

        let cmd = tokio::time::timeout(Duration::from_millis(100), sink.recv())
            .await
            .unwrap()
            .unwrap();

        let flat = flatten(cmd);
        assert_eq!(flat.as_forwarded_batch().node_id(), 42);

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_shutdown_drains_cleanly() {
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_millis(50))
            .with_max_batch_count(256);
        let (batcher, mut sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

        writer
            .send(LocalFrameBatch {
                bytes: make_sub_frame(1, 1, b"last"),
                count: 1,
            })
            .await
            .unwrap();

        // Drop the writer first so the channel drains, then shutdown the batcher.
        drop(writer);
        batcher.shutdown().await;

        // The command should have been flushed (by linger) before or during shutdown.
        // Channel may or may not have the command depending on timing; just verify
        // no panic and sink is eventually closed.
        let _ = sink.recv().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_writer_channel_closed_returns_error() {
        let config = MqWriteBatcherConfig::default();
        let (batcher, _sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

        // Abort the task to drop the receiver without waiting for senders to
        // close first (writer still holds a sender clone, so shutdown().await
        // would block forever).
        let task = batcher.task.lock().take().unwrap();
        task.abort();
        let _ = task.await; // receiver is now dropped

        // Sending after receiver dropped should return ChannelClosed.
        let result = writer
            .send(LocalFrameBatch {
                bytes: make_sub_frame(0, 0, b"x"),
                count: 1,
            })
            .await;
        assert!(matches!(result, Err(MqBatcherError::ChannelClosed)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_multiple_flushes_sequential() {
        // Verify the batcher loops correctly and handles multiple flush cycles.
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_millis(15))
            .with_max_batch_count(256)
            .with_channel_capacity(64);
        let (batcher, mut sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

        for cycle in 0u64..3 {
            writer
                .send(LocalFrameBatch {
                    bytes: make_sub_frame(0, cycle, b"data"),
                    count: 1,
                })
                .await
                .unwrap();

            let cmd = tokio::time::timeout(Duration::from_millis(200), sink.recv())
                .await
                .expect("timed out on cycle")
                .unwrap();
            let flat = flatten(cmd);
            assert_eq!(flat.as_forwarded_batch().count(), 1);
        }

        drop(writer);
        batcher.shutdown().await;
    }

    // -------------------------------------------------------------------------
    // MqWriteBatcherConfig tests (unchanged)
    // -------------------------------------------------------------------------

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

/// Isolated tests for crossfire MPSC channel behaviour under different Tokio runtimes.
///
/// Crossfire's async waker does not integrate with Tokio's `current_thread`
/// (single-threaded) runtime — `recv().await` never wakes up after a `send()`
/// from the same thread, causing tests to hang forever.  Using
/// `flavor = "multi_thread"` gives crossfire a thread-pool to park wakers on
/// and the channel works correctly.
///
/// These tests exist to document and verify this limitation so that future
/// maintainers understand why all crossfire-backed async tests in this crate
/// must use `#[tokio::test(flavor = "multi_thread")]`.
#[cfg(test)]
mod crossfire_runtime_tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use std::time::Duration;

    fn make_bytes(val: u8, len: usize) -> Bytes {
        let mut b = BytesMut::with_capacity(len);
        for _ in 0..len {
            b.put_u8(val);
        }
        b.freeze()
    }

    /// Verifies that crossfire bounded_async send+recv works on the multi-thread runtime.
    /// This is the known-good configuration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crossfire_send_recv_multi_thread_ok() {
        let (tx, rx) = crossfire::mpsc::bounded_async::<Bytes>(8);

        let payload = make_bytes(0xAB, 16);
        tx.send(payload.clone()).await.unwrap();

        let received = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timed out — crossfire recv did not wake on multi_thread runtime")
            .expect("channel closed unexpectedly");

        assert_eq!(received, payload);
        drop(tx);
    }

    /// Verifies that crossfire bounded_async send+recv works on the current_thread runtime.
    ///
    /// NOTE: This test is expected to FAIL (hang / timeout) if crossfire's
    /// waker is incompatible with current_thread.  It is marked
    /// `#[ignore]` so it does not block CI, but can be run manually with
    /// `cargo test -- --ignored crossfire_send_recv_current_thread_known_hang`
    /// to reproduce the hang.
    #[tokio::test] // intentionally current_thread
    #[ignore = "crossfire waker is incompatible with current_thread runtime — hangs without multi_thread"]
    async fn crossfire_send_recv_current_thread_known_hang() {
        let (tx, rx) = crossfire::mpsc::bounded_async::<Bytes>(8);

        let payload = make_bytes(0xCD, 8);
        tx.send(payload.clone()).await.unwrap();

        // On current_thread this recv will never wake — the test hangs here.
        // With a timeout the test surfaces the waker incompatibility as a panic.
        let received = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("EXPECTED FAILURE: crossfire recv hung on current_thread runtime")
            .expect("channel closed unexpectedly");

        assert_eq!(received, payload);
        drop(tx);
    }

    /// Verify that the channel correctly signals closure on multi_thread runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crossfire_channel_closed_on_drop_multi_thread() {
        let (tx, rx) = crossfire::mpsc::bounded_async::<Bytes>(4);
        drop(tx);

        let result = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timed out waiting for channel close signal");

        assert!(result.is_err(), "expected Err after sender dropped, got Ok");
    }

    /// Verify backpressure: a full channel blocks the sender until capacity opens up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crossfire_backpressure_multi_thread() {
        let capacity = 4usize;
        let (tx, rx) = crossfire::mpsc::bounded_async::<Bytes>(capacity);

        // Fill the channel.
        for i in 0u8..capacity as u8 {
            tx.send(make_bytes(i, 1)).await.unwrap();
        }

        // Spawn a task that drains one slot after a short delay.
        let drain_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            rx.recv().await.unwrap(); // open one slot
            rx // keep rx alive so channel doesn't close
        });

        // This send should block until the drain opens a slot, then succeed.
        let result = tokio::time::timeout(Duration::from_millis(500), tx.send(make_bytes(0xFF, 1)))
            .await
            .expect("timed out — backpressure send never unblocked");

        assert!(result.is_ok());
        drop(drain_task);
    }
}
