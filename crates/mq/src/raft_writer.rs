//! Fixed-concurrency Raft write pool with byte-based backpressure.
//!
//! [`RaftWriter`] maintains a bounded MPMC queue and a fixed number of worker
//! tasks. Each worker drains the queue greedily and submits all available
//! commands via [`client_write_many`] in one shot, so Raft sees a full batch
//! per proposal rather than one command at a time.
//!
//! Byte-based backpressure is provided by an internal [`RaftBacklog`] budget.
//! [`RaftWriter::submit`] charges the budget before enqueuing; workers release
//! on error; the state machine releases on successful apply.
//!
//! [`client_write_many`]: openraft::Raft::client_write_many

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt as _;
use openraft::Raft;
use tokio::task::JoinHandle;
use tracing::debug;

use crate::MqTypeConfig;
use crate::forward::RaftBacklog;
use crate::types::MqCommand;

/// Lightweight stats for benchmarking Raft write batching.
#[derive(Default)]
pub struct RaftWriterStats {
    /// Number of `client_write_many` calls.
    pub proposals: AtomicU64,
    /// Total MqCommands across all proposals.
    pub entries: AtomicU64,
    /// Total bytes across all proposals.
    pub bytes: AtomicU64,
}

impl RaftWriterStats {
    pub fn reset(&self) {
        self.proposals.store(0, Ordering::Relaxed);
        self.entries.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }
}

/// Cloneable MPMC sender for the RaftWriter command queue.
pub type MqCommandTx = crossfire::MAsyncTx<crossfire::mpmc::Array<(MqCommand, usize)>>;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for [`RaftWriter`].
#[derive(Debug, Clone)]
pub struct RaftWriterConfig {
    /// Number of concurrent worker tasks, each holding one in-flight
    /// `client_write_many` call. Default: 8.
    pub worker_count: usize,
    /// Capacity of the bounded MPMC command queue shared by all workers.
    /// Default: 256.
    pub queue_capacity: usize,
    /// Maximum bytes of in-flight (charged but not yet applied) Raft commands
    /// before [`submit`] starts blocking. Default: 256 MiB.
    ///
    /// [`submit`]: RaftWriter::submit
    pub max_backlog_bytes: usize,
}

impl Default for RaftWriterConfig {
    fn default() -> Self {
        Self {
            worker_count: 8,
            queue_capacity: 256,
            max_backlog_bytes: 256 * 1024 * 1024,
        }
    }
}

impl RaftWriterConfig {
    pub fn with_worker_count(mut self, n: usize) -> Self {
        self.worker_count = n;
        self
    }

    pub fn with_queue_capacity(mut self, n: usize) -> Self {
        self.queue_capacity = n;
        self
    }

    pub fn with_max_backlog_bytes(mut self, n: usize) -> Self {
        self.max_backlog_bytes = n;
        self
    }
}

// ---------------------------------------------------------------------------
// RaftWriter
// ---------------------------------------------------------------------------

/// Fixed-concurrency Raft write pool.
///
/// Shared via `Arc`; callers invoke [`submit`] to enqueue commands.
///
/// [`submit`]: RaftWriter::submit
pub struct RaftWriter {
    tx: parking_lot::Mutex<Option<MqCommandTx>>,
    backlog: Arc<RaftBacklog>,
    workers: parking_lot::Mutex<Vec<JoinHandle<()>>>,
    stats: Arc<RaftWriterStats>,
}

impl RaftWriter {
    /// Create a new `RaftWriter` with an internal backlog budget derived from
    /// `config.max_backlog_bytes`.
    pub fn new(raft: Raft<MqTypeConfig>, config: RaftWriterConfig) -> Arc<Self> {
        let backlog = Arc::new(RaftBacklog::new(config.max_backlog_bytes));
        Self::with_backlog(raft, config, backlog)
    }

    /// Create a new `RaftWriter` sharing an existing [`RaftBacklog`].
    ///
    /// Use this when the same backlog budget must be shared between multiple
    /// components (e.g. a local batcher and a forward acceptor on the same
    /// Raft group).
    pub fn with_backlog(
        raft: Raft<MqTypeConfig>,
        config: RaftWriterConfig,
        backlog: Arc<RaftBacklog>,
    ) -> Arc<Self> {
        let worker_count = config.worker_count.max(1);
        let (tx, rx) = crossfire::mpmc::bounded_async::<(MqCommand, usize)>(config.queue_capacity);
        let stats = Arc::new(RaftWriterStats::default());

        let workers = (0..worker_count)
            .map(|_| {
                let rx = rx.clone();
                let raft = raft.clone();
                let backlog = Arc::clone(&backlog);
                let stats = Arc::clone(&stats);
                tokio::spawn(raft_worker_loop(rx, raft, backlog, stats))
            })
            .collect();

        Arc::new(Self {
            tx: parking_lot::Mutex::new(Some(tx)),
            backlog,
            workers: parking_lot::Mutex::new(workers),
            stats,
        })
    }

    /// Clone the MPMC sender for caching by the caller.
    ///
    /// Returns `None` after shutdown. Callers should clone once and reuse
    /// the returned handle to avoid locking on every send.
    pub fn clone_tx(&self) -> Option<MqCommandTx> {
        self.tx.lock().clone()
    }

    /// Submit a command to the write pool.
    ///
    /// Atomically waits for byte-budget capacity (FIFO-fair), charges it,
    /// then enqueues the command. Returns once enqueued — the worker calls
    /// `client_write_many` asynchronously.
    pub async fn submit(&self, cmd: MqCommand) {
        let cmd_len = cmd.total_encoded_size();
        self.backlog.charge(cmd_len).await;
        // Clone the sender before awaiting so the MutexGuard is dropped
        // before the .await point (MutexGuard is not Send).
        let tx = self.tx.lock().clone();
        if let Some(tx) = tx {
            let _ = tx.send((cmd, cmd_len)).await;
        }
    }

    /// Byte-budget shared with the state machine for backpressure signalling.
    pub fn backlog(&self) -> &Arc<RaftBacklog> {
        &self.backlog
    }

    /// Batching stats (proposals, entries, bytes).
    pub fn stats(&self) -> &Arc<RaftWriterStats> {
        &self.stats
    }

    /// Drain all queued commands and shut down all worker tasks.
    ///
    /// Drops the sender half to close the queue; workers exit after draining
    /// their current batch.
    pub async fn shutdown(&self) {
        // Closing the channel causes workers to exit after finishing in-flight writes.
        drop(self.tx.lock().take());
        let workers = std::mem::take(&mut *self.workers.lock());
        for w in workers {
            let _ = w.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

async fn raft_worker_loop(
    rx: crossfire::MAsyncRx<crossfire::mpmc::Array<(MqCommand, usize)>>,
    raft: Raft<MqTypeConfig>,
    backlog: Arc<RaftBacklog>,
    stats: Arc<RaftWriterStats>,
) {
    loop {
        // Block until at least one command is available.
        let (first_cmd, first_len) = match rx.recv().await {
            Ok(item) => item,
            Err(_) => return, // channel closed — all senders dropped
        };

        let mut cmds = vec![first_cmd];
        let mut sizes = vec![first_len];

        // Greedily drain all immediately available commands so Raft sees
        // as large a batch as possible per client_write_many call.
        while let Ok((cmd, len)) = rx.try_recv() {
            cmds.push(cmd);
            sizes.push(len);
        }

        // Record stats.
        let total_bytes: usize = sizes.iter().sum();
        stats.proposals.fetch_add(1, Ordering::Relaxed);
        stats
            .entries
            .fetch_add(cmds.len() as u64, Ordering::Relaxed);
        stats.bytes.fetch_add(total_bytes as u64, Ordering::Relaxed);

        // Submit the batch; Raft pipelines the entries internally.
        let batch_size = cmds.len();
        match raft.client_write_many(cmds).await {
            Err(e) => {
                debug!(error = %e, "raft_writer: write_many fatal error");
                for &sz in &sizes {
                    backlog.release(sz);
                }
            }
            Ok(stream) => {
                let mut stream = std::pin::pin!(stream);
                let mut i = 0usize;
                while let Some(result) = stream.next().await {
                    if result.is_err() {
                        if i < sizes.len() {
                            backlog.release(sizes[i]);
                        }
                    }
                    i += 1;
                }
                // Safety: if the stream yields fewer results than entries
                // submitted (e.g. Raft shutdown mid-batch), release the
                // backlog for the missing entries to prevent permit leaks.
                if i < batch_size {
                    for &sz in &sizes[i..] {
                        backlog.release(sz);
                    }
                }
            }
        }
    }
}
