//! Write batcher — coalesces per-table writes into larger Raft proposals.
//!
//! When ingesting lots of small writes, each individual `write_records()` call
//! would produce a separate Raft proposal. The write batcher accumulates
//! writes per table over a short linger window (or until a byte threshold is
//! reached) and submits them as a single coalesced `AppendRecords` command.
//!
//! An optional [`WriteProcessor`] can be configured per table to transform
//! batches between accumulation and IPC encoding — e.g., aggregating counter
//! metrics, keeping last-value for gauges, or emitting materialized writes
//! to derived tables.
//!
//! Uses [`crossfire`] lock-free bounded channels for the ingestion queue,
//! with a dedicated `batcher_loop` task spawned per table on first write.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use openraft::Raft;
use parking_lot::RwLock;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::LanceTypeConfig;
use crate::ipc;
use crate::raft::WriteError;
use crate::types::{
    LanceCommand, LanceResponse, PersistedBatcherConfig, ProcessorDescriptor, WriteResult,
    duration_to_ms,
};
use crate::write_processor::{MaterializedWrite, WriteProcessor};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the per-table write batcher.
///
/// Includes an optional [`WriteProcessor`] that transforms accumulated batches
/// before IPC encoding and Raft proposal.
pub struct WriteBatcherConfig {
    /// How long to wait after the first request before flushing a batch.
    /// Shorter = lower latency, longer = more coalescing.
    pub linger: Duration,
    /// Maximum accumulated IPC-encoded bytes before flushing early.
    pub max_batch_bytes: usize,
    /// Crossfire channel capacity per table.
    pub channel_capacity: usize,
    /// Optional write processor applied between batch accumulation and IPC encoding.
    pub processor: Option<Arc<dyn WriteProcessor>>,
}

impl Clone for WriteBatcherConfig {
    fn clone(&self) -> Self {
        Self {
            linger: self.linger,
            max_batch_bytes: self.max_batch_bytes,
            channel_capacity: self.channel_capacity,
            processor: self.processor.clone(),
        }
    }
}

impl std::fmt::Debug for WriteBatcherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBatcherConfig")
            .field("linger", &self.linger)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("channel_capacity", &self.channel_capacity)
            .field("has_processor", &self.processor.is_some())
            .finish()
    }
}

impl Default for WriteBatcherConfig {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(5),
            max_batch_bytes: 8 * 1024 * 1024, // 8 MB
            channel_capacity: 1024,
            processor: None,
        }
    }
}

impl WriteBatcherConfig {
    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    pub fn with_max_batch_bytes(mut self, max: usize) -> Self {
        self.max_batch_bytes = max;
        self
    }

    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    /// Set a write processor that transforms batches between accumulation
    /// and IPC encoding. See [`WriteProcessor`] for details.
    pub fn with_processor(mut self, processor: Arc<dyn WriteProcessor>) -> Self {
        self.processor = Some(processor);
        self
    }

    /// Convert the batcher-only settings to a serializable form.
    pub fn to_persisted_batcher(&self) -> PersistedBatcherConfig {
        PersistedBatcherConfig {
            linger_ms: duration_to_ms(self.linger),
            max_batch_bytes: self.max_batch_bytes,
            channel_capacity: self.channel_capacity,
        }
    }

    /// Get the processor descriptor, if a processor is configured.
    pub fn processor_descriptor(&self) -> Option<ProcessorDescriptor> {
        self.processor.as_ref().and_then(|p| p.descriptor())
    }

    /// Reconstruct from persisted batcher config and optional processor descriptor.
    pub fn from_persisted(
        batcher: &PersistedBatcherConfig,
        processor: Option<&ProcessorDescriptor>,
    ) -> Self {
        Self {
            linger: Duration::from_millis(batcher.linger_ms),
            max_batch_bytes: batcher.max_batch_bytes,
            channel_capacity: batcher.channel_capacity,
            processor: processor.map(|d| d.into_processor()),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A single write request queued in the batcher.
struct BatchWriteRequest {
    batches: Vec<RecordBatch>,
    estimated_bytes: usize,
    response_tx: oneshot::Sender<Result<WriteResult, WriteError>>,
}

/// Handle to a per-table batcher task.
struct TableBatcherHandle {
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<BatchWriteRequest>>,
    _task: JoinHandle<()>,
}

// ---------------------------------------------------------------------------
// WriteBatcher
// ---------------------------------------------------------------------------

/// Coalesces per-table writes into batched Raft proposals.
///
/// Thread-safe and designed to be shared via `Arc`. Lazily spawns a
/// `batcher_loop` task for each table on first write.
pub struct WriteBatcher {
    default_config: WriteBatcherConfig,
    table_configs: RwLock<HashMap<String, WriteBatcherConfig>>,
    raft: Raft<LanceTypeConfig>,
    catalog_name: String,
    tables: RwLock<HashMap<String, Arc<TableBatcherHandle>>>,
}

impl WriteBatcher {
    /// Create a new `WriteBatcher` with the given default configuration.
    ///
    /// Individual tables can be overridden via [`configure_table`].
    pub fn new(
        default_config: WriteBatcherConfig,
        raft: Raft<LanceTypeConfig>,
        catalog_name: String,
    ) -> Self {
        Self {
            default_config,
            table_configs: RwLock::new(HashMap::new()),
            raft,
            catalog_name,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Set a per-table batcher configuration, overriding the default.
    ///
    /// If a batcher loop is already running for this table, the new config
    /// takes effect on the next batcher creation (e.g. after shutdown/restart).
    pub fn configure_table(&self, table_name: impl Into<String>, config: WriteBatcherConfig) {
        self.table_configs.write().insert(table_name.into(), config);
    }

    /// Resolve the effective config for a table.
    fn config_for(&self, table_name: &str) -> WriteBatcherConfig {
        self.table_configs
            .read()
            .get(table_name)
            .cloned()
            .unwrap_or_else(|| self.default_config.clone())
    }

    /// Submit batches for a table. Blocks (async) until the coalesced Raft
    /// proposal completes.
    pub async fn submit(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<WriteResult, WriteError> {
        let estimated_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        let (response_tx, response_rx) = oneshot::channel();

        let request = BatchWriteRequest {
            batches,
            estimated_bytes,
            response_tx,
        };

        let handle = self.get_or_create_batcher(table_name);

        // Send into the crossfire channel. If the channel is full, this
        // awaits until space is available.
        handle
            .tx
            .send(request)
            .await
            .map_err(|_| WriteError::Fatal("batcher channel closed".to_string()))?;

        // Wait for the batcher loop to process our request and return the result.
        response_rx
            .await
            .map_err(|_| WriteError::Fatal("batcher response channel dropped".to_string()))?
    }

    /// Get the per-table batcher handle, creating one if it doesn't exist.
    fn get_or_create_batcher(&self, table_name: &str) -> Arc<TableBatcherHandle> {
        // Fast path: read lock
        {
            let tables = self.tables.read();
            if let Some(handle) = tables.get(table_name) {
                return handle.clone();
            }
        }

        // Slow path: write lock + create
        let mut tables = self.tables.write();
        // Double-check after acquiring write lock
        if let Some(handle) = tables.get(table_name) {
            return handle.clone();
        }

        let config = self.config_for(table_name);

        let (tx, rx) = crossfire::mpsc::bounded_async::<BatchWriteRequest>(config.channel_capacity);

        let task = tokio::spawn(batcher_loop(
            Arc::from(table_name),
            rx,
            self.raft.clone(),
            config,
            self.catalog_name.clone(),
        ));

        let handle = Arc::new(TableBatcherHandle { tx, _task: task });
        tables.insert(table_name.to_string(), handle.clone());
        handle
    }

    /// Shut down all per-table batchers. Drops senders so loops exit,
    /// then awaits task completion.
    pub async fn shutdown(&self) {
        let handles: Vec<Arc<TableBatcherHandle>> = {
            let mut tables = self.tables.write();
            tables.drain().map(|(_, h)| h).collect()
        };

        for handle in handles {
            // Drop the sender clone we hold — once all senders are dropped
            // the batcher_loop will see a disconnected channel and exit.
            // The Arc<TableBatcherHandle> going out of scope after this
            // function drops the tx. We just need to wait for the task.
            // Since _task is inside Arc, we can't take ownership easily,
            // but the loop will exit once all tx clones are dropped.
            drop(handle);
        }

        // Give loops a moment to drain and exit.
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------
// Batcher loop
// ---------------------------------------------------------------------------

/// Per-table batcher loop. Blocks on the channel, accumulates writes over
/// the linger window, then coalesces and proposes through Raft.
///
/// If a [`WriteProcessor`] is configured, it is applied between batch
/// accumulation and IPC encoding. The processor may reduce data (aggregate-only)
/// or produce additional materialized writes to other tables.
async fn batcher_loop(
    table_name: Arc<str>,
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<BatchWriteRequest>>,
    raft: Raft<LanceTypeConfig>,
    config: WriteBatcherConfig,
    catalog_name: String,
) {
    // Pre-build metric handles once per batcher lifetime.
    let tname_label = table_name.to_string();
    let m_flush_size = metrics::counter!("bisque_batcher_flushes_total", "catalog" => catalog_name.clone(), "table" => tname_label.clone(), "reason" => "size");
    let m_flush_linger = metrics::counter!("bisque_batcher_flushes_total", "catalog" => catalog_name, "table" => tname_label, "reason" => "linger");

    let mut pending: Vec<BatchWriteRequest> = Vec::with_capacity(128);

    loop {
        // Step 1: Block until the first request arrives.
        let first = match rx.recv().await {
            Ok(req) => req,
            Err(_) => {
                // Channel disconnected — drain the processor before exiting.
                if let Some(processor) = &config.processor {
                    let drain_writes = processor.drain();
                    propose_materialized_writes(drain_writes, &raft, &table_name).await;
                }
                debug!(table = %table_name, "batcher_loop: channel closed, exiting");
                return;
            }
        };

        pending.clear();
        let mut accumulated_bytes = first.estimated_bytes;
        pending.push(first);

        // Step 2: If we haven't hit the byte threshold, wait for the linger
        // window and drain any additional requests.
        if accumulated_bytes < config.max_batch_bytes {
            tokio::time::sleep(config.linger).await;

            // Drain everything available without blocking.
            loop {
                match rx.try_recv() {
                    Ok(req) => {
                        accumulated_bytes += req.estimated_bytes;
                        pending.push(req);
                        if accumulated_bytes >= config.max_batch_bytes {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        let num_requests = pending.len();

        // Step 3: Coalesce all RecordBatches (move, don't clone — pending is consumed).
        let all_batches: Vec<RecordBatch> = pending
            .iter_mut()
            .flat_map(|r| r.batches.drain(..))
            .collect();

        // Step 3a: Apply write processor if configured.
        let (primary_batches, materialized_writes) = match &config.processor {
            Some(processor) => {
                let output = processor.process(all_batches);
                (output.primary, output.materialized)
            }
            None => (all_batches, Vec::new()),
        };

        // Step 3b: IPC-encode and propose primary batches.
        let result: Result<WriteResult, WriteError> = if primary_batches.is_empty() {
            Ok(WriteResult {
                log_index: 0,
                response: LanceResponse::Ok,
            })
        } else {
            match ipc::encode_record_batches(&primary_batches) {
                Ok(data) => {
                    let cmd = LanceCommand::AppendRecords {
                        table_name: table_name.clone(),
                        data,
                    };
                    match raft.client_write(cmd).await {
                        Ok(resp) => {
                            let response = resp.response().clone();
                            if let LanceResponse::Error(e) = &response {
                                Err(WriteError::Raft(e.clone()))
                            } else {
                                Ok(WriteResult {
                                    log_index: resp.log_id().index,
                                    response,
                                })
                            }
                        }
                        Err(e) => Err(WriteError::Raft(e.to_string())),
                    }
                }
                Err(e) => Err(WriteError::Encode(e.to_string())),
            }
        };

        // Step 3c: Propose materialized writes to their respective tables.
        if !materialized_writes.is_empty() {
            propose_materialized_writes(materialized_writes, &raft, &table_name).await;
        }

        if accumulated_bytes >= config.max_batch_bytes {
            m_flush_size.increment(1);
        } else {
            m_flush_linger.increment(1);
        }
        debug!(
            table = %table_name,
            requests = num_requests,
            bytes = accumulated_bytes,
            "batcher_loop: flushed coalesced batch"
        );

        // Step 4: Broadcast the result to all waiting callers.
        for req in pending.drain(..) {
            let _ = req.response_tx.send(result.clone());
        }
    }
}

/// Propose materialized writes through Raft. Errors are logged but don't
/// fail the primary write — materialized tables are derived and can be rebuilt.
async fn propose_materialized_writes(
    writes: Vec<MaterializedWrite>,
    raft: &Raft<LanceTypeConfig>,
    source_table: &str,
) {
    for mw in writes {
        if mw.batches.is_empty() {
            continue;
        }
        match ipc::encode_record_batches(&mw.batches) {
            Ok(data) => {
                let cmd = LanceCommand::AppendRecords {
                    table_name: Arc::from(mw.table_name.as_str()),
                    data,
                };
                if let Err(e) = raft.client_write(cmd).await {
                    warn!(
                        source_table = %source_table,
                        target_table = %mw.table_name,
                        "Failed to propose materialized write: {}", e
                    );
                }
            }
            Err(e) => {
                warn!(
                    source_table = %source_table,
                    target_table = %mw.table_name,
                    "Failed to encode materialized write: {}", e
                );
            }
        }
    }
}
