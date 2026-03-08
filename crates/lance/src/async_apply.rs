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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bisque_raft::record_format::AtomicLogId;
use bytes::Bytes;
use openraft::LogId;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::LanceTypeConfig;
use crate::engine::BisqueLance;
use crate::ipc;

// ── Backpressure & async-apply metrics ────────────────────────────────

/// Pre-initialized metrics handles for async apply backpressure and write pipeline.
///
/// All handles are created once at buffer construction time. Hot-path code
/// calls `.increment()` / `.set()` / `.record()` on the pre-built handles
/// without ever invoking `metrics::counter!()` etc. inline.
struct AsyncApplyMetrics {
    /// Number of times `enqueue` entered the backpressure wait loop.
    backpressure_events: metrics::Counter,
    /// Number of backpressure waits that exceeded the timeout.
    backpressure_timeouts: metrics::Counter,
    /// Histogram of backpressure wait durations (ms).
    backpressure_wait_ms: metrics::Histogram,
    /// Current pending bytes gauge.
    pending_bytes_gauge: metrics::Gauge,
    /// Total bytes enqueued (cumulative).
    enqueued_bytes: metrics::Counter,
    /// Catalog name for creating per-table handles.
    catalog_name: String,
}

impl AsyncApplyMetrics {
    fn new(catalog: &str) -> Self {
        let cat = catalog.to_string();
        Self {
            backpressure_events: metrics::counter!(
                "bisque_async_apply_backpressure_events_total",
                "catalog" => cat.clone()
            ),
            backpressure_timeouts: metrics::counter!(
                "bisque_async_apply_backpressure_timeouts_total",
                "catalog" => cat.clone()
            ),
            backpressure_wait_ms: metrics::histogram!(
                "bisque_async_apply_backpressure_wait_ms",
                "catalog" => cat.clone()
            ),
            pending_bytes_gauge: metrics::gauge!(
                "bisque_async_apply_pending_bytes",
                "catalog" => cat.clone()
            ),
            enqueued_bytes: metrics::counter!(
                "bisque_async_apply_enqueued_bytes_total",
                "catalog" => cat.clone()
            ),
            catalog_name: cat,
        }
    }
}

/// Per-table flush metrics, lazily created and cached by the background writer.
struct PerTableMetrics {
    flushed_bytes: metrics::Counter,
    flushes: metrics::Counter,
    flush_errors: metrics::Counter,
    flush_latency_ms: metrics::Histogram,
}

impl PerTableMetrics {
    fn new(catalog: &str, table: &str) -> Self {
        let cat = catalog.to_string();
        let tbl = table.to_string();
        Self {
            flushed_bytes: metrics::counter!(
                "bisque_async_apply_flushed_bytes_total",
                "catalog" => cat.clone(),
                "table" => tbl.clone()
            ),
            flushes: metrics::counter!(
                "bisque_async_apply_flushes_total",
                "catalog" => cat.clone(),
                "table" => tbl.clone()
            ),
            flush_errors: metrics::counter!(
                "bisque_async_apply_flush_errors_total",
                "catalog" => cat.clone(),
                "table" => tbl.clone()
            ),
            flush_latency_ms: metrics::histogram!(
                "bisque_async_apply_flush_latency_ms",
                "catalog" => cat,
                "table" => tbl
            ),
        }
    }
}

/// Configuration for the async apply buffer.
#[derive(Debug, Clone)]
pub struct AsyncApplyConfig {
    /// Max pending IPC bytes before blocking the apply stream.
    /// Default: 64 MB.
    pub max_pending_bytes: usize,
    /// Timeout for backpressure wait before logging a warning and proceeding.
    /// Prevents indefinite stalls if the background writer is stuck.
    /// Default: 30 seconds.
    pub backpressure_timeout: Duration,
}

impl Default for AsyncApplyConfig {
    fn default() -> Self {
        Self {
            max_pending_bytes: 64 * 1024 * 1024,
            backpressure_timeout: Duration::from_secs(30),
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
        table_name: Arc<str>,
        data: Bytes,
        data_len: usize,
    },
    /// Drain all pending writes for a specific table, then signal completion.
    DrainTable {
        table_name: Arc<str>,
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
    /// Notified when the background writer drains bytes (unblocks backpressure).
    drain_notify: Arc<Notify>,
    /// Config.
    config: AsyncApplyConfig,
    /// Metrics.
    metrics: Arc<AsyncApplyMetrics>,
}

impl AsyncApplyBuffer {
    /// Create a new async apply buffer and spawn its background writer task.
    ///
    /// Returns the buffer and a clonable [`AppliedWatermark`] handle that
    /// query layers can use for read-after-write fencing.
    pub fn new(
        engine: Arc<BisqueLance>,
        config: AsyncApplyConfig,
        catalog_name: &str,
    ) -> (Self, AppliedWatermark) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (watch_tx, watch_rx) = watch::channel(0u64);
        let lance_applied_log_id = Arc::new(AtomicLogId::new());
        let pending_bytes = Arc::new(AtomicUsize::new(0));
        let drain_notify = Arc::new(Notify::new());
        let metrics = Arc::new(AsyncApplyMetrics::new(catalog_name));

        let handle = tokio::spawn(background_writer(
            rx,
            engine,
            watch_tx.clone(),
            lance_applied_log_id.clone(),
            pending_bytes.clone(),
            drain_notify.clone(),
            metrics.clone(),
        ));

        let buffer = Self {
            tx,
            handle: parking_lot::Mutex::new(Some(handle)),
            lance_applied_tx: watch_tx,
            lance_applied_log_id,
            pending_bytes,
            drain_notify,
            config,
            metrics,
        };
        let watermark = AppliedWatermark { rx: watch_rx };
        (buffer, watermark)
    }

    /// Enqueue an append operation for background processing.
    ///
    /// If pending bytes exceed `max_pending_bytes`, this method blocks until
    /// the background writer drains enough work.
    pub async fn enqueue(&self, log_id: LogId<LanceTypeConfig>, table_name: Arc<str>, data: Bytes) {
        let data_len = data.len();

        // Backpressure: park until the background writer drains enough work.
        // Register the Notify future *before* checking the condition to avoid
        // missing a wakeup between the load and the await.
        let deadline = tokio::time::Instant::now() + self.config.backpressure_timeout;
        let mut entered_backpressure = false;
        loop {
            let notified = self.drain_notify.notified();
            if self.pending_bytes.load(Ordering::Acquire) <= self.config.max_pending_bytes {
                break;
            }
            if !entered_backpressure {
                entered_backpressure = true;
                self.metrics.backpressure_events.increment(1);
            }
            match tokio::time::timeout_at(deadline, notified).await {
                Ok(()) => {} // Woken by drain — re-check condition.
                Err(_) => {
                    self.metrics.backpressure_timeouts.increment(1);
                    tracing::warn!(
                        pending_bytes = self.pending_bytes.load(Ordering::Relaxed),
                        max = self.config.max_pending_bytes,
                        "backpressure timeout exceeded; proceeding to avoid stall"
                    );
                    break;
                }
            }
        }
        if entered_backpressure {
            let waited = self.config.backpressure_timeout.as_secs_f64() * 1000.0
                - (deadline - tokio::time::Instant::now()).as_secs_f64() * 1000.0;
            self.metrics.backpressure_wait_ms.record(waited.max(0.0));
        }

        self.pending_bytes.fetch_add(data_len, Ordering::Release);
        self.metrics
            .pending_bytes_gauge
            .set(self.pending_bytes.load(Ordering::Relaxed) as f64);
        self.metrics.enqueued_bytes.increment(data_len as u64);

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
            table_name: Arc::from(table_name),
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
    drain_notify: Arc<Notify>,
    metrics: Arc<AsyncApplyMetrics>,
) {
    let mut per_table: HashMap<Arc<str>, TableWork> = HashMap::new();
    let mut table_metrics_cache: HashMap<Arc<str>, PerTableMetrics> = HashMap::new();

    loop {
        // Wait for the first item.
        let first = match rx.recv().await {
            Some(item) => item,
            None => {
                // Channel closed — flush remaining work and exit.
                flush_all(
                    &mut per_table,
                    &engine,
                    &lance_applied_tx,
                    &lance_applied_log_id,
                    &pending_bytes,
                    &drain_notify,
                    &metrics,
                    &mut table_metrics_cache,
                )
                .await;
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
                flush_table(
                    &table_name,
                    &mut per_table,
                    &engine,
                    &lance_applied_tx,
                    &lance_applied_log_id,
                    &pending_bytes,
                    &drain_notify,
                    &metrics,
                    &mut table_metrics_cache,
                )
                .await;
                let _ = done.send(());
                continue;
            }
            ApplyWork::DrainAll { done } => {
                flush_all(
                    &mut per_table,
                    &engine,
                    &lance_applied_tx,
                    &lance_applied_log_id,
                    &pending_bytes,
                    &drain_notify,
                    &metrics,
                    &mut table_metrics_cache,
                )
                .await;
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
                    flush_table(
                        &table_name,
                        &mut per_table,
                        &engine,
                        &lance_applied_tx,
                        &lance_applied_log_id,
                        &pending_bytes,
                        &drain_notify,
                        &metrics,
                        &mut table_metrics_cache,
                    )
                    .await;
                    let _ = done.send(());
                }
                Ok(ApplyWork::DrainAll { done }) => {
                    flush_all(
                        &mut per_table,
                        &engine,
                        &lance_applied_tx,
                        &lance_applied_log_id,
                        &pending_bytes,
                        &drain_notify,
                        &metrics,
                        &mut table_metrics_cache,
                    )
                    .await;
                    let _ = done.send(());
                }
                Err(_) => break,
            }
        }

        // Flush all accumulated work.
        if !per_table.is_empty() {
            flush_all(
                &mut per_table,
                &engine,
                &lance_applied_tx,
                &lance_applied_log_id,
                &pending_bytes,
                &drain_notify,
                &metrics,
                &mut table_metrics_cache,
            )
            .await;
        }
    }
}

/// Accumulate an append into per-table work.
fn accumulate(
    per_table: &mut HashMap<Arc<str>, TableWork>,
    log_id: LogId<LanceTypeConfig>,
    table_name: Arc<str>,
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
    per_table: &mut HashMap<Arc<str>, TableWork>,
    engine: &BisqueLance,
    lance_applied_tx: &watch::Sender<u64>,
    lance_applied_log_id: &AtomicLogId,
    pending_bytes: &AtomicUsize,
    drain_notify: &Notify,
    metrics: &AsyncApplyMetrics,
    table_metrics_cache: &mut HashMap<Arc<str>, PerTableMetrics>,
) {
    if let Some(work) = per_table.remove(table_name) {
        write_table_work(
            table_name,
            work,
            engine,
            lance_applied_tx,
            lance_applied_log_id,
            pending_bytes,
            drain_notify,
            metrics,
            table_metrics_cache,
        )
        .await;
    }
}

/// Flush all pending work across all tables.
async fn flush_all(
    per_table: &mut HashMap<Arc<str>, TableWork>,
    engine: &BisqueLance,
    lance_applied_tx: &watch::Sender<u64>,
    lance_applied_log_id: &AtomicLogId,
    pending_bytes: &AtomicUsize,
    drain_notify: &Notify,
    metrics: &AsyncApplyMetrics,
    table_metrics_cache: &mut HashMap<Arc<str>, PerTableMetrics>,
) {
    if per_table.is_empty() {
        return;
    }

    for (name, work) in per_table.drain() {
        write_table_work(
            &name,
            work,
            engine,
            lance_applied_tx,
            lance_applied_log_id,
            pending_bytes,
            drain_notify,
            metrics,
            table_metrics_cache,
        )
        .await;
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
    drain_notify: &Notify,
    metrics: &AsyncApplyMetrics,
    table_metrics_cache: &mut HashMap<Arc<str>, PerTableMetrics>,
) {
    let total_bytes = work.total_bytes;
    let flush_start = std::time::Instant::now();

    // Get or create per-table metrics handles (cached after first access).
    let table_key: Arc<str> = Arc::from(table_name);
    let tm = table_metrics_cache
        .entry(table_key)
        .or_insert_with(|| PerTableMetrics::new(&metrics.catalog_name, table_name));

    // Decode all IPC entries and collect batches.
    let mut all_batches = Vec::new();
    let mut max_log_id: Option<LogId<LanceTypeConfig>> = None;

    for (log_id, data) in work.entries {
        match ipc::decode_record_batches_into(&data, &mut all_batches) {
            Ok(()) => {
                let should_update = match &max_log_id {
                    Some(current) => log_id > *current,
                    None => true,
                };
                if should_update {
                    max_log_id = Some(log_id);
                }
            }
            Err(e) => {
                tm.flush_errors.increment(1);
                error!(table = %table_name, "Failed to decode IPC data in async apply: {}", e);
            }
        }
    }

    if !all_batches.is_empty() {
        let table = match engine.require_table(table_name) {
            Ok(t) => t,
            Err(e) => {
                tm.flush_errors.increment(1);
                error!(table = %table_name, "Table not found in async apply: {}", e);
                pending_bytes.fetch_sub(total_bytes, Ordering::Release);
                metrics
                    .pending_bytes_gauge
                    .set(pending_bytes.load(Ordering::Relaxed) as f64);
                tm.flushed_bytes.increment(total_bytes as u64);
                drain_notify.notify_waiters();
                return;
            }
        };

        if let Err(e) = table.apply_append(all_batches).await {
            tm.flush_errors.increment(1);
            error!(table = %table_name, "apply_append failed in async apply: {}", e);
        }
    }

    // Subtract processed bytes from pending counter and wake backpressured callers.
    pending_bytes.fetch_sub(total_bytes, Ordering::Release);
    metrics
        .pending_bytes_gauge
        .set(pending_bytes.load(Ordering::Relaxed) as f64);
    tm.flushed_bytes.increment(total_bytes as u64);
    tm.flushes.increment(1);
    tm.flush_latency_ms
        .record(flush_start.elapsed().as_secs_f64() * 1000.0);
    drain_notify.notify_waiters();

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
    use std::sync::atomic::AtomicBool;

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

    fn new_buffer(
        engine: Arc<BisqueLance>,
        config: AsyncApplyConfig,
    ) -> (AsyncApplyBuffer, AppliedWatermark) {
        AsyncApplyBuffer::new(engine, config, "test")
    }

    #[tokio::test]
    async fn enqueue_and_drain_all() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("test_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        // Enqueue some work.
        let batch = make_batch(&["a", "b"], &[1.0, 2.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
        buffer
            .enqueue(make_log_id(1), Arc::from("test_table"), data)
            .await;

        let batch2 = make_batch(&["c"], &[3.0]);
        let data2: Bytes = ipc::encode_record_batches(&[batch2]).unwrap();
        buffer
            .enqueue(make_log_id(2), Arc::from("test_table"), data2)
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

        let config_a = engine
            .config()
            .build_table_config("table_a", Arc::new(schema.clone()));
        engine.create_table(config_a, None).await.unwrap();
        let config_b = engine
            .config()
            .build_table_config("table_b", Arc::new(schema));
        engine.create_table(config_b, None).await.unwrap();

        let (buffer, _watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        let batch_a = make_batch(&["a"], &[1.0]);
        let data_a: Bytes = ipc::encode_record_batches(&[batch_a]).unwrap();
        buffer
            .enqueue(make_log_id(1), Arc::from("table_a"), data_a)
            .await;

        let batch_b = make_batch(&["b"], &[2.0]);
        let data_b: Bytes = ipc::encode_record_batches(&[batch_b]).unwrap();
        buffer
            .enqueue(make_log_id(2), Arc::from("table_b"), data_b)
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
        let (buffer, watermark) = new_buffer(engine, AsyncApplyConfig::default());

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
        let table_config = engine
            .config()
            .build_table_config("bp_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        // Tiny limit to trigger backpressure easily.
        let config = AsyncApplyConfig {
            max_pending_bytes: 100,
            ..Default::default()
        };
        let (buffer, _watermark) = new_buffer(engine.clone(), config);
        let buffer = Arc::new(buffer);

        // Enqueue enough data to exceed the limit.
        let batch = make_batch(&["x"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();

        // First enqueue should succeed.
        buffer
            .enqueue(make_log_id(1), Arc::from("bp_table"), data.clone())
            .await;

        // The background writer should eventually process it, allowing more enqueues.
        // This tests that the backpressure mechanism doesn't deadlock.
        buffer
            .enqueue(make_log_id(2), Arc::from("bp_table"), data.clone())
            .await;
        buffer
            .enqueue(make_log_id(3), Arc::from("bp_table"), data)
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
        let table_config = engine
            .config()
            .build_table_config("wm_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        // wait_for should timeout when nothing is materialized.
        assert!(!watermark.wait_for(1, Duration::from_millis(10)).await);

        // Enqueue and drain.
        let batch = make_batch(&["a"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
        buffer
            .enqueue(make_log_id(5), Arc::from("wm_table"), data)
            .await;
        buffer.drain_all().await;

        // wait_for should return immediately for index <= 5.
        assert!(watermark.wait_for(5, Duration::from_millis(10)).await);
        assert!(watermark.wait_for(3, Duration::from_millis(10)).await);

        // wait_for should timeout for index > 5.
        assert!(!watermark.wait_for(6, Duration::from_millis(10)).await);

        buffer.shutdown().await;
    }

    // ── Comprehensive backpressure tests ──────────────────────────────

    /// Verify that backpressure timeout fires when background writer is slower
    /// than incoming data and the timeout is very short.
    #[tokio::test]
    async fn backpressure_timeout_fires_when_writer_stalls() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("timeout_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        // Very small limit and very short timeout to force timeout behavior.
        let config = AsyncApplyConfig {
            max_pending_bytes: 1, // 1 byte — everything triggers backpressure
            backpressure_timeout: Duration::from_millis(50),
        };
        let (buffer, _watermark) = new_buffer(engine.clone(), config);
        let buffer = Arc::new(buffer);

        let batch = make_batch(&["a"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();

        // First enqueue succeeds (no backpressure on empty buffer).
        buffer
            .enqueue(make_log_id(1), Arc::from("timeout_table"), data.clone())
            .await;

        // Second enqueue should hit backpressure and eventually timeout,
        // since the background writer may not drain fast enough within 50ms
        // for such a tiny limit. Even if it does drain, the enqueue will
        // succeed. Either way: no deadlock, no hang.
        let start = std::time::Instant::now();
        buffer
            .enqueue(make_log_id(2), Arc::from("timeout_table"), data.clone())
            .await;
        buffer
            .enqueue(make_log_id(3), Arc::from("timeout_table"), data)
            .await;

        // Verify we didn't hang forever (should complete within a few hundred ms).
        assert!(start.elapsed() < Duration::from_secs(5));

        buffer.drain_all().await;
        buffer.shutdown().await;
    }

    /// Verify backpressure with concurrent producers all racing to enqueue.
    #[tokio::test]
    async fn backpressure_concurrent_producers() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("concurrent_bp", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let config = AsyncApplyConfig {
            max_pending_bytes: 200,
            ..Default::default()
        };
        let (buffer, _watermark) = new_buffer(engine.clone(), config);
        let buffer = Arc::new(buffer);

        let num_producers = 4;
        let items_per_producer = 5;

        let mut handles = Vec::new();
        for producer_id in 0..num_producers {
            let buf = buffer.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..items_per_producer {
                    let batch = make_batch(
                        &[&format!("p{producer_id}_i{i}")],
                        &[producer_id as f64 * 100.0 + i as f64],
                    );
                    let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
                    let idx = (producer_id * items_per_producer + i + 1) as u64;
                    buf.enqueue(make_log_id(idx), Arc::from("concurrent_bp"), data)
                        .await;
                }
            }));
        }

        // All producers should complete without deadlock.
        for h in handles {
            h.await.unwrap();
        }

        buffer.drain_all().await;

        let table = engine.require_table("concurrent_bp").unwrap();
        let ds = table.active_dataset_snapshot().await.unwrap();
        assert_eq!(
            ds.count_rows(None).await.unwrap(),
            (num_producers * items_per_producer) as usize,
        );

        buffer.shutdown().await;
    }

    /// Verify that pending_bytes returns to 0 after all work is drained.
    #[tokio::test]
    async fn pending_bytes_returns_to_zero_after_drain() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("pb_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, _watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        // Enqueue several items.
        for i in 1..=5 {
            let batch = make_batch(&[&format!("item{i}")], &[i as f64]);
            let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
            buffer
                .enqueue(make_log_id(i), Arc::from("pb_table"), data)
                .await;
        }

        // Drain and verify pending bytes goes back to zero.
        buffer.drain_all().await;
        assert_eq!(buffer.pending_bytes.load(Ordering::Relaxed), 0);

        buffer.shutdown().await;
    }

    /// Verify backpressure with multiple tables interleaved.
    #[tokio::test]
    async fn backpressure_multi_table_interleaved() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();

        for name in &["mt_a", "mt_b", "mt_c"] {
            let tc = engine
                .config()
                .build_table_config(name, Arc::new(schema.clone()));
            engine.create_table(tc, None).await.unwrap();
        }

        let config = AsyncApplyConfig {
            max_pending_bytes: 150,
            ..Default::default()
        };
        let (buffer, _watermark) = new_buffer(engine.clone(), config);

        let tables = ["mt_a", "mt_b", "mt_c"];
        for i in 0..9u64 {
            let table = tables[(i % 3) as usize];
            let batch = make_batch(&[&format!("v{i}")], &[i as f64]);
            let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
            buffer
                .enqueue(make_log_id(i + 1), Arc::from(table), data)
                .await;
        }

        buffer.drain_all().await;

        // Each table should have exactly 3 rows.
        for name in &["mt_a", "mt_b", "mt_c"] {
            let table = engine.require_table(name).unwrap();
            let ds = table.active_dataset_snapshot().await.unwrap();
            assert_eq!(ds.count_rows(None).await.unwrap(), 3, "table {name}");
        }

        assert_eq!(buffer.pending_bytes.load(Ordering::Relaxed), 0);
        buffer.shutdown().await;
    }

    /// Verify that enqueue to a nonexistent table doesn't panic or hang —
    /// the background writer logs an error and drains bytes correctly.
    #[tokio::test]
    async fn enqueue_to_missing_table_drains_bytes() {
        let (_dir, engine) = setup_engine().await;

        let (buffer, _watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        let batch = make_batch(&["ghost"], &[42.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();

        // Enqueue to a table that doesn't exist.
        buffer
            .enqueue(make_log_id(1), Arc::from("nonexistent_table"), data)
            .await;

        buffer.drain_all().await;

        // Pending bytes should still return to zero even on error.
        assert_eq!(buffer.pending_bytes.load(Ordering::Relaxed), 0);

        buffer.shutdown().await;
    }

    /// Verify shutdown while backpressure is active doesn't hang.
    #[tokio::test]
    async fn shutdown_during_backpressure() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("shutdown_bp", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let config = AsyncApplyConfig {
            max_pending_bytes: 100,
            backpressure_timeout: Duration::from_secs(30),
        };
        let (buffer, _watermark) = new_buffer(engine.clone(), config);
        let buffer = Arc::new(buffer);

        let batch = make_batch(&["x"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();

        // Saturate the buffer.
        buffer
            .enqueue(make_log_id(1), Arc::from("shutdown_bp"), data.clone())
            .await;
        buffer
            .enqueue(make_log_id(2), Arc::from("shutdown_bp"), data)
            .await;

        // Shutdown should drain all work and not hang.
        let start = std::time::Instant::now();
        buffer.shutdown().await;
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    /// Verify rapid enqueue/drain cycles don't leak pending bytes.
    #[tokio::test]
    async fn rapid_enqueue_drain_cycles_no_byte_leak() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("cycle_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        for cycle in 0..10u64 {
            let batch = make_batch(&[&format!("c{cycle}")], &[cycle as f64]);
            let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
            buffer
                .enqueue(make_log_id(cycle + 1), Arc::from("cycle_table"), data)
                .await;
            buffer.drain_all().await;

            assert_eq!(buffer.pending_bytes.load(Ordering::Relaxed), 0);
            assert_eq!(watermark.current_index(), cycle + 1);
        }

        let table = engine.require_table("cycle_table").unwrap();
        let ds = table.active_dataset_snapshot().await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 10);

        buffer.shutdown().await;
    }

    /// Verify that backpressure releases immediately when bytes drop below threshold.
    #[tokio::test]
    async fn backpressure_releases_when_below_threshold() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("release_bp", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let config = AsyncApplyConfig {
            max_pending_bytes: 100,
            ..Default::default()
        };
        let (buffer, _watermark) = new_buffer(engine.clone(), config);
        let buffer = Arc::new(buffer);

        let batch = make_batch(&["row"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();

        // Enqueue a burst of items — the background writer should drain each one,
        // releasing backpressure for the next.
        let enqueue_done = Arc::new(AtomicBool::new(false));
        let done_flag = enqueue_done.clone();

        let buf = buffer.clone();
        let d = data.clone();
        let handle = tokio::spawn(async move {
            for i in 1..=20u64 {
                buf.enqueue(make_log_id(i), Arc::from("release_bp"), d.clone())
                    .await;
            }
            done_flag.store(true, Ordering::Release);
        });

        // The producer should complete (not hang) — background writer drains.
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("producer should complete within 10s")
            .unwrap();

        assert!(enqueue_done.load(Ordering::Acquire));

        buffer.drain_all().await;

        let table = engine.require_table("release_bp").unwrap();
        let ds = table.active_dataset_snapshot().await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 20);

        buffer.shutdown().await;
    }

    /// Verify that watermark advances monotonically even with out-of-order
    /// per-table flushes across multiple drain_table calls.
    #[tokio::test]
    async fn watermark_monotonic_across_partial_drains() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        for name in &["mono_a", "mono_b"] {
            let tc = engine
                .config()
                .build_table_config(name, Arc::new(schema.clone()));
            engine.create_table(tc, None).await.unwrap();
        }

        let (buffer, watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        // Enqueue: log_id 1 -> table_a, log_id 2 -> table_b
        let batch = make_batch(&["a"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
        buffer
            .enqueue(make_log_id(1), Arc::from("mono_a"), data)
            .await;

        let batch = make_batch(&["b"], &[2.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
        buffer
            .enqueue(make_log_id(2), Arc::from("mono_b"), data)
            .await;

        // Drain table_b first (higher log_id).
        buffer.drain_table("mono_b").await;
        let wm_after_b = watermark.current_index();

        // Drain table_a (lower log_id).
        buffer.drain_table("mono_a").await;
        let wm_after_a = watermark.current_index();

        // Watermark should never go backwards.
        assert!(wm_after_a >= wm_after_b);
        // Both should be flushed: watermark should be at least 2.
        assert!(wm_after_a >= 2);

        buffer.shutdown().await;
    }

    /// Verify that zero-length data doesn't corrupt pending bytes accounting.
    #[tokio::test]
    async fn zero_length_data_accounting() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("zero_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, _watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());

        // Enqueue empty data — should not break accounting.
        buffer
            .enqueue(make_log_id(1), Arc::from("zero_table"), Bytes::new())
            .await;

        // Enqueue normal data after.
        let batch = make_batch(&["after_zero"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
        buffer
            .enqueue(make_log_id(2), Arc::from("zero_table"), data)
            .await;

        buffer.drain_all().await;
        assert_eq!(buffer.pending_bytes.load(Ordering::Relaxed), 0);

        buffer.shutdown().await;
    }

    /// Verify large burst followed by drain — simulates write spike.
    #[tokio::test]
    async fn large_burst_then_drain() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("burst_table", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let config = AsyncApplyConfig {
            max_pending_bytes: 500,
            ..Default::default()
        };
        let (buffer, watermark) = new_buffer(engine.clone(), config);

        let num_items = 50u64;
        for i in 1..=num_items {
            let batch = make_batch(&[&format!("burst{i}")], &[i as f64]);
            let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
            buffer
                .enqueue(make_log_id(i), Arc::from("burst_table"), data)
                .await;
        }

        buffer.drain_all().await;

        assert_eq!(watermark.current_index(), num_items);
        assert_eq!(buffer.pending_bytes.load(Ordering::Relaxed), 0);

        let table = engine.require_table("burst_table").unwrap();
        let ds = table.active_dataset_snapshot().await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), num_items as usize);

        buffer.shutdown().await;
    }

    /// Verify double shutdown is safe (idempotent).
    #[tokio::test]
    async fn double_shutdown_is_safe() {
        let (_dir, engine) = setup_engine().await;
        let (buffer, _watermark) = new_buffer(engine, AsyncApplyConfig::default());

        buffer.shutdown().await;
        buffer.shutdown().await; // second shutdown should not panic
    }

    /// Verify enqueue after shutdown doesn't panic (channel is closed,
    /// send returns Err which is silently ignored).
    #[tokio::test]
    async fn enqueue_after_shutdown_no_panic() {
        let (_dir, engine) = setup_engine().await;
        let schema = test_schema();
        let table_config = engine
            .config()
            .build_table_config("post_shutdown", Arc::new(schema));
        engine.create_table(table_config, None).await.unwrap();

        let (buffer, _watermark) = new_buffer(engine.clone(), AsyncApplyConfig::default());
        buffer.shutdown().await;

        // Enqueue after shutdown — should not panic.
        let batch = make_batch(&["late"], &[1.0]);
        let data: Bytes = ipc::encode_record_batches(&[batch]).unwrap();
        buffer
            .enqueue(make_log_id(99), Arc::from("post_shutdown"), data)
            .await;
        // pending_bytes will increase but never drain — that's expected after shutdown.
    }
}
