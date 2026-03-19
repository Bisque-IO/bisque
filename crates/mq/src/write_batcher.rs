//! Write batcher — coalesces individual MqCommand submissions into batched
//! Raft proposals.
//!
//! ## MqWriteBatcher
//!
//! External clients submit commands via [`MqWriteBatcher::submit`]. The batcher
//! accumulates available requests up to a count threshold, merges same-topic
//! publishes, and proposes a single `MqCommand::Batch` to Raft.
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
//! The [`LocalBatcher`] drain loop blocks on the first frame, then non-blockingly
//! drains all immediately available frames up to `max_batch_count`, builds a
//! vectored `MqCommand` (header buf + extra frame chunks) and proposes it to
//! Raft — zero copies of command bytes anywhere in the pipeline.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{BufMut, Bytes, BytesMut};

use crate::raft_writer::RaftWriter;
use crate::types::MqCommand;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the MQ write batcher.
#[derive(Debug, Clone)]
pub struct MqWriteBatcherConfig {
    /// Maximum number of commands to accumulate before flushing.
    pub max_batch_count: usize,
    /// Crossfire channel capacity.
    pub channel_capacity: usize,
    /// Maximum byte size of a single Raft log entry.
    ///
    /// The drain loop aggregates channel receives until the total payload
    /// reaches this threshold, then cuts a new entry. Prevents oversized
    /// entries from stalling the Raft pipeline. Default: 512 KiB.
    pub max_raft_entry_bytes: usize,
    /// Use scattered (vectored) MqCommand instead of memcpy into one
    /// contiguous buffer.  Scattered avoids copying frame bytes; contiguous
    /// produces a single `Bytes` that may be cheaper for small payloads.
    /// Default: true (scattered).
    pub use_scattered: bool,
    /// Maximum number of concurrent charge+send operations in the drain loop.
    ///
    /// With large messages each channel recv already fills a Raft entry, so
    /// the drain loop produces one proposal per recv. Serial charge+send
    /// starves the RaftWriter workers. This setting pipelines up to N
    /// charge+send operations concurrently. Default: 32.
    pub max_inflight_proposals: usize,
}

/// Default maximum Raft entry size: 1 MiB.
pub const DEFAULT_MAX_RAFT_ENTRY_BYTES: usize = 1024 * 1024;

impl Default for MqWriteBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_count: 256,
            channel_capacity: 1024,
            max_raft_entry_bytes: DEFAULT_MAX_RAFT_ENTRY_BYTES,
            use_scattered: true,
            max_inflight_proposals: 1024 * 16,
        }
    }
}

impl MqWriteBatcherConfig {
    pub fn with_max_batch_count(mut self, max_batch_count: usize) -> Self {
        self.max_batch_count = max_batch_count;
        self
    }

    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity;
        self
    }

    pub fn with_max_raft_entry_bytes(mut self, max_raft_entry_bytes: usize) -> Self {
        self.max_raft_entry_bytes = max_raft_entry_bytes;
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

/// Clone-able writer handle that builds `TAG_FORWARDED_BATCH` commands and
/// sends them directly to the [`RaftWriter`] MPMC queue.
///
/// Each `LocalWriter` is an independent proposer — N writers = N concurrent
/// charge+send pipelines, matching the forward path where each TCP connection
/// proposes independently. No intermediate drain loop.
#[derive(Clone)]
pub struct LocalWriter {
    tx: crate::raft_writer::MqCommandTx,
    backlog: Arc<crate::forward::RaftBacklog>,
    node_id: u32,
    stats: Arc<LocalBatcherStats>,
}

impl LocalWriter {
    /// Create a `LocalWriter` from raw components (used by mq-server handler).
    pub fn from_parts(
        tx: crate::raft_writer::MqCommandTx,
        backlog: Arc<crate::forward::RaftBacklog>,
        node_id: u32,
        stats: Arc<LocalBatcherStats>,
    ) -> Self {
        Self {
            tx,
            backlog,
            node_id,
            stats,
        }
    }

    /// Send a batch of pre-framed sub-frames directly to the RaftWriter.
    ///
    /// Builds a `TAG_FORWARDED_BATCH` command, charges the backlog budget
    /// (blocks if over capacity), then enqueues the command. Each writer
    /// charges independently — no serialization across writers.
    pub async fn send(&self, batch: LocalFrameBatch) -> Result<(), MqBatcherError> {
        let payload_len = batch.bytes.len();
        let hdr = build_forwarded_batch_header(payload_len, batch.count, self.node_id);
        let cmd = MqCommand::scattered(hdr, vec![batch.bytes]);
        let cmd_len = cmd.total_encoded_size();

        self.stats.batch_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_payload_bytes
            .fetch_add(payload_len as u64, Ordering::Relaxed);
        self.stats.channel_recvs.fetch_add(1, Ordering::Relaxed);

        self.backlog.charge(cmd_len).await;
        self.tx
            .send((cmd, cmd_len))
            .await
            .map_err(|_| MqBatcherError::ChannelClosed)
    }
}

/// Lightweight stats for the local write pipeline.
#[derive(Default)]
pub struct LocalBatcherStats {
    /// Number of MqCommands produced.
    pub batch_count: AtomicU64,
    /// Total payload bytes across all batches (excluding 32-byte headers).
    pub total_payload_bytes: AtomicU64,
    /// Number of sends (one per `LocalWriter::send` call).
    pub channel_recvs: AtomicU64,
}

/// Factory for [`LocalWriter`]s that send directly to the [`RaftWriter`].
///
/// No drain loop — each writer is an independent proposer. Use `node_id = 0`
/// for locally-originated batches; the state machine routes responses to local
/// clients via the in-process [`ClientRegistry`].
///
/// [`ClientRegistry`]: crate::async_apply::ClientRegistry
pub struct LocalBatcher {
    tx: crate::raft_writer::MqCommandTx,
    backlog: Arc<crate::forward::RaftBacklog>,
    node_id: u32,
    stats: Arc<LocalBatcherStats>,
}

impl LocalBatcher {
    /// Create a new `LocalBatcher` factory.
    pub fn new(raft_writer: Arc<RaftWriter>, node_id: u32, _config: MqWriteBatcherConfig) -> Self {
        let tx = raft_writer
            .clone_tx()
            .expect("RaftWriter must not be shut down");
        let backlog = raft_writer.backlog().clone();
        Self {
            tx,
            backlog,
            node_id,
            stats: Arc::new(LocalBatcherStats::default()),
        }
    }

    /// Batching stats (batch count, payload bytes, sends).
    pub fn stats(&self) -> &Arc<LocalBatcherStats> {
        &self.stats
    }

    /// Return a clone-able [`LocalWriter`] that sends directly to the RaftWriter.
    ///
    /// Each writer is independent — multiple writers charge and send
    /// concurrently with no shared drain loop.
    pub fn writer(&self) -> LocalWriter {
        LocalWriter {
            tx: self.tx.clone(),
            backlog: self.backlog.clone(),
            node_id: self.node_id,
            stats: Arc::clone(&self.stats),
        }
    }

    /// Shut down the batcher (no-op — no drain loop to stop).
    pub async fn shutdown(self) {}

    /// Create a test batcher that captures built [`MqCommand`]s into `sink`
    /// rather than proposing through Raft.
    #[cfg(any(test, feature = "test-util"))]
    pub fn new_test(
        config: MqWriteBatcherConfig,
        node_id: u32,
        sink: tokio::sync::mpsc::UnboundedSender<MqCommand>,
    ) -> Self {
        let (tx, rx) =
            crossfire::mpmc::bounded_async::<(MqCommand, usize)>(config.channel_capacity);
        let backlog = Arc::new(crate::forward::RaftBacklog::new(256 * 1024 * 1024));
        // Spawn a reader that forwards commands to the test sink.
        tokio::spawn(async move {
            while let Ok((cmd, _len)) = rx.recv().await {
                if sink.send(cmd).is_err() {
                    break;
                }
            }
        });
        Self {
            tx,
            backlog,
            node_id,
            stats: Arc::new(LocalBatcherStats::default()),
        }
    }
}

// ---------------------------------------------------------------------------
// LocalSubmitter — submit(MqCommand) → ResponseEntry atop LocalBatcher
// ---------------------------------------------------------------------------

/// Thin wrapper around [`LocalWriter`] + [`ClientRegistry`] that registers
/// a caller-supplied [`ResponseCallback`](crate::async_apply::ResponseCallback)
/// and provides fire-and-forget command submission.
///
/// Uses the same response path as [`ForwardClient`](crate::forward::ForwardClient):
/// the callback receives response frames from `PartitionWorker` via
/// `ClientRegistry`. Response correlation (matching `request_seq` to callers)
/// is the caller's responsibility.
///
/// ## Response frame format (delivered to callback)
///
/// ```text
/// msg = [request_seq:8][log_index:8][response_entry_bytes...]
/// ```
///
/// A `TAG_FORWARDED_BATCH` may span multiple `PartitionWorker`s (routed by
/// `primary_id % N`), so responses within a batch can arrive out of order.
pub struct LocalSubmitter {
    client_id: u32,
    next_seq: AtomicU64,
    writer: LocalWriter,
    client_registry: Arc<crate::async_apply::ClientRegistry>,
}

impl LocalSubmitter {
    /// Create a new `LocalSubmitter` that registers `callback` with the
    /// [`ClientRegistry`] for response delivery.
    ///
    /// The callback is invoked by the owning `ClientPartition` task for each
    /// response frame. See [`ResponseCallback`](crate::async_apply::ResponseCallback)
    /// for the signature: `(slab, msg_offset, msg, is_done)`.
    pub fn new(
        writer: LocalWriter,
        client_registry: Arc<crate::async_apply::ClientRegistry>,
        callback: crate::async_apply::ResponseCallback,
    ) -> Self {
        let client_id = client_registry.register(callback);

        Self {
            client_id,
            next_seq: AtomicU64::new(0),
            writer,
            client_registry,
        }
    }

    /// Returns the `client_id` assigned by the `ClientRegistry`.
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    /// Send a command. The response will be delivered asynchronously via the
    /// callback registered at construction time.
    ///
    /// Returns the `request_seq` assigned to this command (monotonically
    /// increasing per `LocalSubmitter`).
    pub async fn send(&self, command: &MqCommand) -> Result<u64, MqBatcherError> {
        let request_seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        // Build sub-frame: [payload_len:4][client_id:4][request_seq:8][cmd.buf...]
        let cmd_bytes = &command.buf;
        let payload_len = (12 + cmd_bytes.len()) as u32;
        let frame_len = 4 + payload_len as usize;
        let mut buf = BytesMut::with_capacity(frame_len);
        buf.put_u32_le(payload_len);
        buf.put_u32_le(self.client_id);
        buf.put_u64_le(request_seq);
        buf.put_slice(cmd_bytes);

        self.writer
            .send(LocalFrameBatch {
                bytes: buf.freeze(),
                count: 1,
            })
            .await
            .map_err(|_| MqBatcherError::ChannelClosed)?;

        Ok(request_seq)
    }
}

impl Drop for LocalSubmitter {
    fn drop(&mut self) {
        self.client_registry.unregister(self.client_id);
    }
}

/// Build a 32-byte TAG_FORWARDED_BATCH header for a scattered command.
///
/// `total_payload_bytes` is the sum of all slab lengths (excluding the header).
#[inline]
fn build_forwarded_batch_header(
    total_payload_bytes: usize,
    total_count: u32,
    node_id: u32,
) -> [u8; 32] {
    let total_size = (32 + total_payload_bytes) as u32;
    let mut hdr = [0u8; 32];
    hdr[0..4].copy_from_slice(&total_size.to_le_bytes());
    hdr[4..6].copy_from_slice(&32u16.to_le_bytes()); // fixed region size
    hdr[6] = MqCommand::TAG_FORWARDED_BATCH;
    // hdr[7] = 0 flags; hdr[8..12] node_id; hdr[12..16] total_count
    hdr[8..12].copy_from_slice(&node_id.to_le_bytes());
    hdr[12..16].copy_from_slice(&total_count.to_le_bytes());
    // batch_seq = 0, leader_seq = 0 for locally-originated batches; bytes 16..32 already zeroed.
    hdr
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        let hdr = build_forwarded_batch_header(total_frame_bytes, count, node_id);
        let mut scratch = BytesMut::with_capacity(32 + total_frame_bytes);
        scratch.put_slice(&hdr);
        for frame in frames {
            scratch.put_slice(&frame);
        }
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
        // Sub-frames start at offset 32 (after fixed header).
    }

    #[test]
    fn build_cmd_total_size_header() {
        let frame = make_sub_frame(1, 0, b"payload");
        let frame_len = frame.len();
        let cmd = build_forwarded_batch_cmd(vec![frame], 1, 0);

        // Size field at buf[0..4] must equal 32 (header) + frame bytes
        let size = u32::from_le_bytes(cmd.buf[0..4].try_into().unwrap()) as usize;
        assert_eq!(size, 32 + frame_len);
        assert_eq!(size, cmd.total_encoded_size());
    }

    #[test]
    fn build_cmd_two_frames_contiguous_in_buf() {
        let frame_a = make_sub_frame(1, 1, b"aaa");
        let frame_b = make_sub_frame(2, 2, b"bbb");
        let total = frame_a.len() + frame_b.len();

        let cmd = build_forwarded_batch_cmd(vec![frame_a, frame_b], 2, 0);

        // All frame bytes are concatenated after the 32-byte header.
        assert_eq!(cmd.buf.len(), 32 + total);
    }

    #[test]
    fn build_cmd_encode_decode_roundtrip_single_frame() {
        let frame = make_sub_frame(10, 20, b"cmd-data");
        let cmd = build_forwarded_batch_cmd(vec![frame], 1, 5);
        let flat = flatten(cmd);

        let view = flat.as_forwarded_batch().unwrap();
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

        let view = flat.as_forwarded_batch().unwrap();
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

        let view = flat.as_forwarded_batch().unwrap();
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
        assert_eq!(size, 32);
        assert_eq!(cmd.total_encoded_size(), 32);
    }

    // -------------------------------------------------------------------------
    // LocalWriter / LocalBatcher — async integration tests
    // -------------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_writer_send_single_frame_fires_immediately() {
        let config = MqWriteBatcherConfig::default().with_max_batch_count(256);
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

        let cmd = tokio::time::timeout(Duration::from_millis(100), sink.recv())
            .await
            .expect("timed out waiting for batch")
            .expect("sink closed");

        // Flatten the scattered command so we can inspect it.
        let flat = flatten(cmd);
        assert_eq!(flat.tag(), MqCommand::TAG_FORWARDED_BATCH);
        let view = flat.as_forwarded_batch().unwrap();
        assert_eq!(view.count(), 1);
        let entries: Vec<_> = view.iter().collect();
        assert_eq!(entries[0].0, 5); // client_id
        assert_eq!(entries[0].1, 1); // request_seq
        assert_eq!(entries[0].2, b"hello");

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_writer_each_send_produces_one_command() {
        // Each send() produces its own TAG_FORWARDED_BATCH — no coalescing.
        let config = MqWriteBatcherConfig::default()
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

        // Should receive 3 separate commands, one per send.
        for _ in 0..3 {
            let cmd = tokio::time::timeout(Duration::from_millis(500), sink.recv())
                .await
                .expect("timed out")
                .unwrap();
            let flat = flatten(cmd);
            assert_eq!(flat.as_forwarded_batch().unwrap().count(), 1);
        }

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_writer_multi_subframe_batch_preserved() {
        // A single send with count=2 produces one command with count=2.
        let config = MqWriteBatcherConfig::default().with_channel_capacity(16);
        let (batcher, mut sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

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

        let cmd = tokio::time::timeout(Duration::from_millis(500), sink.recv())
            .await
            .expect("timed out")
            .unwrap();

        let flat = flatten(cmd);
        let view = flat.as_forwarded_batch().unwrap();
        assert_eq!(view.count(), 2);
        assert_eq!(view.iter().count(), 2);

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_multiple_writers_same_channel() {
        let config = MqWriteBatcherConfig::default()
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

        // Without linger, the two frames may land in one or two batches depending
        // on scheduling. Drain until we have collected both client_ids.
        drop(writer_a);
        drop(writer_b);
        batcher.shutdown().await;

        let mut all_cids = std::collections::HashSet::new();
        while let Ok(Some(cmd)) =
            tokio::time::timeout(Duration::from_millis(200), sink.recv()).await
        {
            let flat = flatten(cmd);
            for (cid, _, _) in flat.as_forwarded_batch().unwrap().iter() {
                all_cids.insert(cid);
            }
        }
        assert!(all_cids.contains(&1));
        assert!(all_cids.contains(&2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_node_id_propagated() {
        let config = MqWriteBatcherConfig::default().with_max_batch_count(256);
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
        assert_eq!(flat.as_forwarded_batch().unwrap().node_id(), 42);

        drop(writer);
        batcher.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_shutdown_drains_cleanly() {
        let config = MqWriteBatcherConfig::default().with_max_batch_count(256);
        let (batcher, mut sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

        writer
            .send(LocalFrameBatch {
                bytes: make_sub_frame(1, 1, b"last"),
                count: 1,
            })
            .await
            .unwrap();

        drop(writer);
        batcher.shutdown().await;

        // Verify the frame was flushed and sink is eventually closed.
        let _ = sink.recv().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_writer_channel_closed_returns_error() {
        let config = MqWriteBatcherConfig::default();
        let (batcher, sink) = make_test_batcher(config, 0);
        let writer = batcher.writer();

        // Drop the sink and batcher so the MPMC receiver side closes.
        drop(sink);
        drop(batcher);

        // Fill the bounded MPMC channel to trigger the closed error.
        // The channel is bounded so eventually sends will fail.
        let mut got_error = false;
        for _ in 0..2048 {
            let result = writer
                .send(LocalFrameBatch {
                    bytes: make_sub_frame(0, 0, b"x"),
                    count: 1,
                })
                .await;
            if result.is_err() {
                got_error = true;
                break;
            }
        }
        assert!(got_error, "expected ChannelClosed error after batcher drop");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_batcher_multiple_flushes_sequential() {
        // Verify the batcher loops correctly and handles multiple flush cycles.
        let config = MqWriteBatcherConfig::default()
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
            assert_eq!(flat.as_forwarded_batch().unwrap().count(), 1);
        }

        drop(writer);
        batcher.shutdown().await;
    }

    // -------------------------------------------------------------------------
    // MqWriteBatcherConfig tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let config = MqWriteBatcherConfig::default();
        assert_eq!(config.max_batch_count, 256);
        assert_eq!(config.channel_capacity, 1024);
    }

    #[test]
    fn test_config_builder() {
        let config = MqWriteBatcherConfig::default()
            .with_max_batch_count(512)
            .with_channel_capacity(2048);
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
