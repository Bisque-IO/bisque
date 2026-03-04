//! Async apply buffer — decouples Lance I/O from the Raft apply path.
//!
//! The Raft log serves as the durable WAL. Lance writes are materialized views
//! that can safely lag behind. This module provides an `AsyncApplyBuffer` that
//! enqueues `AppendRecords` work and processes it in a background task, allowing
//! the state machine's `apply()` to return immediately.
//!
//! # Backpressure
//!
//! When pending IPC bytes exceed [`AsyncApplyConfig::max_pending_bytes`], the
//! `enqueue()` call blocks, which blocks `apply()`, which tells openraft to
//! slow down. This provides graceful degradation to synchronous behavior.
//!
//! # Ordering
//!
//! Per-table ordering is maintained: appends for a given table are processed in
//! log order. Different tables may be processed in parallel.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bisque_raft::multi::record_format::AtomicLogId;
use bytes::Bytes;
use openraft::LogId;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::engine::BisqueLance;
use crate::ipc;
use crate::LanceTypeConfig;

/// Configuration for the async apply buffer.
#[derive(Debug, Clone)]
pub struct AsyncApplyConfig {
    /// Max pending IPC bytes before blocking the apply stream.
    /// Default: 64 MB.
    pub max_pending_bytes: usize,
}

impl Default for AsyncApplyConfig {
    fn default() -> Self {
        Self {
            max_pending_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Observable handle for the Lance materialization watermark.
///
/// Cloneable and cheap to hold. Used by query layers (Flight SQL) to
/// implement read-after-write consistency: a client passes the log index
/// from its write response, and the query handler waits until Lance has
/// materialized at least that far before executing the query.
#[derive(Clone)]
pub struct AppliedWatermark {
    rx: watch::Receiver<u64>,
}

impl AppliedWatermark {
    /// Wait until Lance has materialized at least `min_log_index`, or the
    /// timeout expires.
    ///
    /// Returns `true` if the watermark was reached, `false` on timeout.
    pub async fn wait_for(&self, min_log_index: u64, timeout: Duration) -> bool {
        if *self.rx.borrow() >= min_log_index {
            return true;
        }
        let mut rx = self.rx.clone();
        let result = tokio::time::timeout(timeout, async {
            loop {
                if *rx.borrow_and_update() >= min_log_index {
                    return;
                }
                if rx.changed().await.is_err() {
                    return; // sender dropped
                }
            }
        })
        .await;
        result.is_ok()
    }

    /// Current materialized log index (0 = nothing materialized yet).
    pub fn current_index(&self) -> u64 {
        *self.rx.borrow()
    }
}

/// Work item sent to the background writer task.
enum ApplyWork {
    /// Append IPC-encoded data to a table.
    Append {
        log_id: LogId<LanceTypeConfig>,
        table_name: String,
        data: Bytes,
        data_len: usize,
    },
    /// Drain all pending writes for a specific table, then signal completion.
    DrainTable {
        table_name: String,
        done: oneshot::Sender<()>,
    },
    /// Drain ALL pending writes, then signal completion.
    DrainAll { done: oneshot::Sender<()> },
}

/// Accumulated pending work for a single table.
struct TableWork {
    /// IPC-encoded data chunks in log order.
    entries: Vec<(LogId<LanceTypeConfig>, Bytes)>,
    /// Total bytes across all entries (for backpressure accounting).
    total_bytes: usize,
}

impl TableWork {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            total_bytes: 0,
        }
    }
}

/// Async apply buffer that decouples Lance I/O from the Raft apply path.
///
/// `AppendRecords` entries are enqueued and processed by a background writer
/// task. The `lance_applied` watermark tracks the last log_id fully written
/// to Lance, which `applied_state()` uses as the conservative watermark.
pub struct AsyncApplyBuffer {
    /// Channel to send work items to the background writer.
    tx: mpsc::UnboundedSender<ApplyWork>,
    /// Background writer task handle.
    handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
    /// Watch sender — background writer publishes the max applied log index here.
    lance_applied_tx: watch::Sender<u64>,
    /// Full LogId of the last materialized entry (for `applied_state()`).
    /// Uses a lock-free SeqLock-style atomic instead of a Mutex.
    lance_applied_log_id: Arc<AtomicLogId>,
    /// Current pending bytes (for backpressure).
    pending_bytes: Arc<AtomicUsize>,
    /// Config.
    config: AsyncApplyConfig,
}

impl AsyncApplyBuffer {
    /// Create a new async apply buffer and spawn its background writer task.
    ///
    /// Returns the buffer and a clonable [`AppliedWatermark`] handle that
    /// query layers can use for read-after-write fencing.
    pub fn new(engine: Arc<BisqueLance>, config: AsyncApplyConfig) -> (Self, AppliedWatermark) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (watch_tx, watch_rx) = watch::channel(0u64);
        let lance_applied_log_id = Arc::new(AtomicLogId::new());
        let pending_bytes = Arc::new(AtomicUsize::new(0));

        let handle = tokio::spawn(background_writer(
            rx,
            engine,
            watch_tx.clone(),
            lance_applied_log_id.clone(),
            pending_bytes.clone(),
        ));

        let buffer = Self {
            tx,
            handle: parking_lot::Mutex::new(Some(handle)),
            lance_applied_tx: watch_tx,
            lance_applied_log_id,
            pending_bytes,
            config,
        };
        let watermark = AppliedWatermark { rx: watch_rx };
        (buffer, watermark)
    }

    /// Enqueue an append operation for background processing.
    ///
    /// If pending bytes exceed `max_pending_bytes`, this method blocks until
    /// the background writer drains enough work.
    pub async fn enqueue(
        &self,
        log_id: LogId<LanceTypeConfig>,
        table_name: String,
        data: Bytes,
    ) {
        let data_len = data.len();

        // Backpressure: spin-wait if we're over the limit.
        // The background writer decrements pending_bytes as it processes work.
        while self.pending_bytes.load(Ordering::Acquire) > self.config.max_pending_bytes {
            tokio::task::yield_now().await;
        }

        self.pending_bytes.fetch_add(data_len, Ordering::Release);

        let _ = self.tx.send(ApplyWork::Append {
            log_id,
            table_name,
            data,
            data_len,
        });
    }

    /// Drain all pending writes for a specific table.
    ///
    /// Blocks until the background writer has flushed all pending appends
    /// for the named table. Used before `SealActiveSegment` and `DropTable`
    /// to maintain per-table ordering.
    pub async fn drain_table(&self, table_name: &str) {
        let (done_tx, done_rx) = oneshot::channel();
        let _ = self.tx.send(ApplyWork::DrainTable {
            table_name: table_name.to_string(),
            done: done_tx,
        });
        let _ = done_rx.await;
    }

    /// Drain ALL pending writes across all tables.
    ///
    /// Blocks until the background writer has flushed everything. Used
    /// before building snapshots to ensure Lance state is fully caught up.
    pub async fn drain_all(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        let _ = self.tx.send(ApplyWork::DrainAll { done: done_tx });
        let _ = done_rx.await;
    }

    /// Get the current materialized log index (0 = nothing materialized yet).
    pub fn lance_applied_index(&self) -> u64 {
        *self.lance_applied_tx.borrow()
    }

    /// Get the full LogId of the last materialized entry.
    ///
    /// Used by `applied_state()` to report the conservative watermark to openraft.
    /// Lock-free: uses SeqLock-style atomic reads.
    pub fn lance_applied_log_id(&self) -> Option<LogId<LanceTypeConfig>> {
        self.lance_applied_log_id.load()
    }

    /// Shut down the background writer task.
    pub async fn shutdown(&self) {
        // Drop the sender to signal the background task to exit.
        // (The sender is in self.tx — we can't drop it, but closing the
        // channel will cause recv to return None.)
        // Instead, drain everything first, then the task will exit when
        // the channel is dropped with the AsyncApplyBuffer.

        // Drain all pending work first.
        let (done_tx, done_rx) = oneshot::channel();
        let _ = self.tx.send(ApplyWork::DrainAll { done: done_tx });
        let _ = done_rx.await;

        // Abort the background task.
        if let Some(handle) = self.handle.lock().take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// Background writer task — receives work items and writes to Lance.
async fn background_writer(
    mut rx: mpsc::UnboundedReceiver<ApplyWork>,
    engine: Arc<BisqueLance>,
    lance_applied_tx: watch::Sender<u64>,
    lance_applied_log_id: Arc<AtomicLogId>,
    pending_bytes: Arc<AtomicUsize>,
) {
    let mut per_table: HashMap<String, TableWork> = HashMap::new();

    loop {
        // Wait for the first item.
        let first = match rx.recv().await {
            Some(item) => item,
            None => {
                // Channel closed — flush remaining work and exit.
                flush_all(&mut per_table, &engine, &lance_applied_tx, &lance_applied_log_id, &pending_bytes).await;
                return;
            }
        };

        // Process the first item.
        match first {
            ApplyWork::Append {
                log_id,
                table_name,
                data,
                data_len,
            } => {
                accumulate(&mut per_table, log_id, table_name, data, data_len);
            }
            ApplyWork::DrainTable { table_name, done } => {
                flush_table(&table_name, &mut per_table, &engine, &lance_applied_tx, &lance_applied_log_id, &pending_bytes).await;
                let _ = done.send(());
                continue;
            }
            ApplyWork::DrainAll { done } => {
                flush_all(&mut per_table, &engine, &lance_applied_tx, &lance_applied_log_id, &pending_bytes).await;
                let _ = done.send(());
                continue;
            }
        }

        // Eagerly drain any additional items already in the channel.
        loop {
            match rx.try_recv() {
                Ok(ApplyWork::Append {
                    log_id,
                    table_name,
                    data,
                    data_len,
                }) => {
                    accumulate(&mut per_table, log_id, table_name, data, data_len);
                }
                Ok(ApplyWork::DrainTable { table_name, done }) => {
                    flush_table(&table_name, &mut per_table, &engine, &lance_applied_tx, &lance_applied_log_id, &pending_bytes).await;
                    let _ = done.send(());
                }
                Ok(ApplyWork::DrainAll { done }) => {
                    flush_all(&mut per_table, &engine, &lance_applied_tx, &lance_applied_log_id, &pending_bytes).await;
                    let _ = done.send(());
                }
                Err(_) => break,
            }
        }

        // Flush all accumulated work.
        if !per_table.is_empty() {
            flush_all(&mut per_table, &engine, &lance_applied_tx, &lance_applied_log_id, &pending_bytes).await;
        }
    }
}

/// Accumulate an append into per-table work.
fn accumulate(
    per_table: &mut HashMap<String, TableWork>,
    log_id: LogId<LanceTypeConfig>,
    table_name: String,
    data: Bytes,
    data_len: usize,
) {
    let work = per_table.entry(table_name).or_insert_with(TableWork::new);
    work.entries.push((log_id, data));
    work.total_bytes += data_len;
}

/// Flush all pending work for a specific table.
async fn flush_table(
    table_name: &str,
    per_table: &mut HashMap<String, TableWork>,
    engine: &BisqueLance,
    lance_applied_tx: &watch::Sender<u64>,
    lance_applied_log_id: &AtomicLogId,
    pending_bytes: &AtomicUsize,
) {
    if let Some(work) = per_table.remove(table_name) {
        write_table_work(table_name, work, engine, lance_applied_tx, lance_applied_log_id, pending_bytes).await;
    }
}

/// Flush all pending work across all tables.
async fn flush_all(
    per_table: &mut HashMap<String, TableWork>,
    engine: &BisqueLance,
    lance_applied_tx: &watch::Sender<u64>,
    lance_applied_log_id: &AtomicLogId,
    pending_bytes: &AtomicUsize,
) {
    if per_table.is_empty() {
        return;
    }

    let table_names: Vec<String> = per_table.keys().cloned().collect();
    for name in table_names {
        if let Some(work) = per_table.remove(&name) {
            write_table_work(&name, work, engine, lance_applied_tx, lance_applied_log_id, pending_bytes).await;
        }
    }
}

/// Write accumulated work for a single table to Lance.
async fn write_table_work(
    table_name: &str,
    work: TableWork,
    engine: &BisqueLance,
    lance_applied_tx: &watch::Sender<u64>,
    lance_applied_log_id: &AtomicLogId,
    pending_bytes: &AtomicUsize,
) {
    let total_bytes = work.total_bytes;

    // Decode all IPC entries and collect batches.
    let mut all_batches = Vec::new();
    let mut max_log_id: Option<LogId<LanceTypeConfig>> = None;

    for (log_id, data) in work.entries {
        match ipc::decode_record_batches(&data) {
            Ok(batches) => {
                all_batches.extend(batches);
                let should_update = match &max_log_id {
                    Some(current) => log_id > *current,
                    None => true,
                };
                if should_update {
                    max_log_id = Some(log_id);
                }
            }
            Err(e) => {
                error!(table = %table_name, "Failed to decode IPC data in async apply: {}", e);
            }
        }
    }

    if !all_batches.is_empty() {
        let table = match engine.require_table(table_name) {
            Ok(t) => t,
            Err(e) => {
                error!(table = %table_name, "Table not found in async apply: {}", e);
                pending_bytes.fetch_sub(total_bytes, Ordering::Release);
                return;
            }
        };

        if let Err(e) = table.apply_append(all_batches).await {
            error!(table = %table_name, "apply_append failed in async apply: {}", e);
        }
    }

    // Subtract processed bytes from pending counter.
    pending_bytes.fetch_sub(total_bytes, Ordering::Release);

    // Advance the lance_applied watermark (monotonically increasing).
    if let Some(ref new_log_id) = max_log_id {
        let new_index = new_log_id.index;

        // Update the full LogId (for applied_state) — lock-free SeqLock store.
        let should_advance = match lance_applied_log_id.load::<LanceTypeConfig>() {
            Some(current) => *new_log_id > current,
            None => true,
        };
        if should_advance {
            lance_applied_log_id.store(Some(new_log_id));
        }

        // Update the watch channel index (for read-fence waiters).
        lance_applied_tx.send_modify(|current| {
            if new_index > *current {
                debug!(log_index = new_index, table = %table_name, "advancing lance_applied watermark");
                *current = new_index;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Float64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use crate::config::BisqueLanceConfig;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])
    }

    fn make_batch(names: &[&str], values: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(test_schema()),
            vec![
                Arc::new(StringArray::from(
                    names.iter().map(|s| *s).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_log_id(index: u64) -> LogId<LanceTypeConfig> {
        LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 1,
                node_id: 1,
            },
            index,
        }
    }

    async fn setup_engine() -> (tempfile::TempDir, Arc<BisqueLance>) {
        let dir = tempfile::tempdir().unwrap();
        let config = BisqueLanceConfig::new(dir.path());
        let engine = Arc::new(BisqueLance::open(config).await.unwrap());
        (dir, engine)
    }

    #[tokio::test]
    async fn enqueue_and_drain_all() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine.config().build_table_config("test_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, watermark) = AsyncApplyBuffer::new(engine.clone(), AsyncApplyConfig::default());

        // Enqueue some work.
        let batch = make_batch(&["a", "b"], &[1.0, 2.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap().into();
        buffer
            .enqueue(make_log_id(1), "test_table".to_string(), data)
            .await;

        let batch2 = make_batch(&["c"], &[3.0]);
        let data2: Bytes = ipc::encode_record_batches(&[batch2]).unwrap().into();
        buffer
            .enqueue(make_log_id(2), "test_table".to_string(), data2)
            .await;

        // Drain all — should write to Lance.
        buffer.drain_all().await;

        // Verify watermark advanced.
        assert_eq!(buffer.lance_applied_index(), 2);
        assert_eq!(watermark.current_index(), 2);

        // Verify data was written.
        let table = engine.require_table("test_table").unwrap();
        let ds = table.active_dataset_snapshot().await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);

        buffer.shutdown().await;
    }

    #[tokio::test]
    async fn drain_table_only_flushes_named_table() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();

        let config_a = engine.config().build_table_config("table_a", Arc::new(schema.clone()));
        engine.create_table(config_a, None).await.unwrap();
        let config_b = engine.config().build_table_config("table_b", Arc::new(schema));
        engine.create_table(config_b, None).await.unwrap();

        let (buffer, _watermark) = AsyncApplyBuffer::new(engine.clone(), AsyncApplyConfig::default());

        let batch_a = make_batch(&["a"], &[1.0]);
        let data_a: Bytes = ipc::encode_record_batches(&[batch_a]).unwrap().into();
        buffer
            .enqueue(make_log_id(1), "table_a".to_string(), data_a)
            .await;

        let batch_b = make_batch(&["b"], &[2.0]);
        let data_b: Bytes = ipc::encode_record_batches(&[batch_b]).unwrap().into();
        buffer
            .enqueue(make_log_id(2), "table_b".to_string(), data_b)
            .await;

        // Drain only table_a.
        buffer.drain_table("table_a").await;

        // table_a should have data.
        let table_a = engine.require_table("table_a").unwrap();
        let ds_a = table_a.active_dataset_snapshot().await.unwrap();
        assert_eq!(ds_a.count_rows(None).await.unwrap(), 1);

        // Drain all to clean up.
        buffer.drain_all().await;

        // Now table_b should also have data.
        let table_b = engine.require_table("table_b").unwrap();
        let ds_b = table_b.active_dataset_snapshot().await.unwrap();
        assert_eq!(ds_b.count_rows(None).await.unwrap(), 1);

        buffer.shutdown().await;
    }

    #[tokio::test]
    async fn empty_drain_is_noop() {
        let (_dir, engine) = setup_engine().await;
        let (buffer, watermark) = AsyncApplyBuffer::new(engine, AsyncApplyConfig::default());

        // Drain with no pending work should complete immediately.
        buffer.drain_all().await;
        buffer.drain_table("nonexistent").await;

        assert_eq!(watermark.current_index(), 0);

        buffer.shutdown().await;
    }

    #[tokio::test]
    async fn backpressure_blocks_until_drained() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine.config().build_table_config("bp_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        // Tiny limit to trigger backpressure easily.
        let config = AsyncApplyConfig {
            max_pending_bytes: 100,
        };
        let (buffer, _watermark) = AsyncApplyBuffer::new(engine.clone(), config);
        let buffer = Arc::new(buffer);

        // Enqueue enough data to exceed the limit.
        let batch = make_batch(&["x"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap().into();

        // First enqueue should succeed.
        buffer
            .enqueue(make_log_id(1), "bp_table".to_string(), data.clone())
            .await;

        // The background writer should eventually process it, allowing more enqueues.
        // This tests that the backpressure mechanism doesn't deadlock.
        buffer
            .enqueue(make_log_id(2), "bp_table".to_string(), data.clone())
            .await;
        buffer
            .enqueue(make_log_id(3), "bp_table".to_string(), data)
            .await;

        buffer.drain_all().await;

        let table = engine.require_table("bp_table").unwrap();
        let ds = table.active_dataset_snapshot().await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);

        buffer.shutdown().await;
    }

    #[tokio::test]
    async fn watermark_wait_for() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine.config().build_table_config("wm_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, watermark) = AsyncApplyBuffer::new(engine.clone(), AsyncApplyConfig::default());

        // wait_for should timeout when nothing is materialized.
        assert!(!watermark.wait_for(1, Duration::from_millis(10)).await);

        // Enqueue and drain.
        let batch = make_batch(&["a"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap().into();
        buffer
            .enqueue(make_log_id(5), "wm_table".to_string(), data)
            .await;
        buffer.drain_all().await;

        // wait_for should return immediately for index <= 5.
        assert!(watermark.wait_for(5, Duration::from_millis(10)).await);
        assert!(watermark.wait_for(3, Duration::from_millis(10)).await);

        // wait_for should timeout for index > 5.
        assert!(!watermark.wait_for(6, Duration::from_millis(10)).await);

        buffer.shutdown().await;
    }
}
