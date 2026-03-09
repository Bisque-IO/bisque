//! Raft integration layer for bisque-lance.
//!
//! `LanceRaftNode` wires the `BisqueLance` engine and `LanceStateMachine` into a
//! `MultiRaftManager` Raft group, and runs background tasks for:
//! - Leader election detection via metrics watcher
//! - Periodic seal checks across all tables
//! - Flush orchestration per table (sealed segment → S3, leader only)
//! - Periodic compaction across all tables

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use openraft::{Raft, ServerState};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::LanceTypeConfig;
use crate::async_apply::AppliedWatermark;
use crate::engine::BisqueLance;
use crate::ipc;
use crate::manifest::{LanceManifestManager, TableUpdate};
use crate::types::{LanceCommand, LanceResponse, TxnId, TxnOp, WriteResult};
use crate::write_batcher::{WriteBatcher, WriteBatcherConfig};

/// Raft-integrated node for bisque-lance.
///
/// Wraps a `Raft<LanceTypeConfig>` handle and the underlying multi-table `BisqueLance`
/// engine, running background leader-duty tasks (seal monitoring, flush orchestration,
/// compaction) across all tables.
pub struct LanceRaftNode {
    /// The Raft group handle.
    raft: Raft<LanceTypeConfig>,
    /// The underlying multi-table storage engine.
    engine: Arc<BisqueLance>,
    /// This node's ID.
    node_id: u64,
    /// The Raft group ID (used for per-group MDBX manifest).
    group_id: u64,
    /// Catalog name (raft group name) for metric labeling.
    catalog_name: String,
    /// Interval between seal threshold checks.
    seal_check_interval: Duration,
    /// Interval between flush attempts when a sealed segment is pending.
    flush_check_interval: Duration,
    /// Interval between compaction checks for active and S3 tiers.
    compaction_check_interval: Duration,
    /// Optional write batcher for coalescing per-table writes.
    write_batcher: Option<Arc<WriteBatcher>>,
    /// Optional watermark for read-after-write fencing (set when async apply is enabled).
    applied_watermark: Option<AppliedWatermark>,
    /// Optional MDBX manifest manager for crash-consistent metadata.
    manifest: Option<Arc<LanceManifestManager>>,
    /// Notify handle to trigger shutdown of background tasks.
    shutdown: Arc<Notify>,
    /// Durable flag that survives missed Notify wakeups.
    shutdown_flag: Arc<AtomicBool>,
    /// Background task handles.
    task_handles: parking_lot::Mutex<Vec<JoinHandle<()>>>,
}

impl LanceRaftNode {
    /// Create a new `LanceRaftNode`.
    ///
    /// Does NOT start background tasks — call [`start`] after construction.
    pub fn new(raft: Raft<LanceTypeConfig>, engine: Arc<BisqueLance>, node_id: u64) -> Self {
        Self {
            raft,
            engine,
            node_id,
            group_id: 0,
            catalog_name: String::new(),
            seal_check_interval: Duration::from_secs(5),
            flush_check_interval: Duration::from_secs(10),
            compaction_check_interval: Duration::from_secs(60),
            write_batcher: None,
            applied_watermark: None,
            manifest: None,
            shutdown: Arc::new(Notify::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            task_handles: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Set the interval for seal threshold checks.
    pub fn with_seal_check_interval(mut self, interval: Duration) -> Self {
        self.seal_check_interval = interval;
        self
    }

    /// Set the interval for flush checks.
    pub fn with_flush_check_interval(mut self, interval: Duration) -> Self {
        self.flush_check_interval = interval;
        self
    }

    /// Set the interval for compaction checks.
    pub fn with_compaction_check_interval(mut self, interval: Duration) -> Self {
        self.compaction_check_interval = interval;
        self
    }

    /// Set the Raft group ID (default: 0).
    ///
    /// Used for per-group MDBX manifest storage and batcher config persistence.
    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = group_id;
        self
    }

    /// Set the catalog name (raft group name) used for metric labeling.
    pub fn with_catalog_name(mut self, name: String) -> Self {
        self.catalog_name = name;
        self
    }

    /// Get the catalog name.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Enable write batching with the given default configuration.
    ///
    /// When enabled, `write_records()` routes through a per-table batcher
    /// that coalesces writes over a short linger window before submitting
    /// a single Raft proposal. Individual tables can be configured with
    /// [`configure_table_batcher`].
    pub fn with_write_batcher(mut self, config: WriteBatcherConfig) -> Self {
        self.write_batcher = Some(Arc::new(WriteBatcher::new(
            config,
            self.raft.clone(),
            self.catalog_name.clone(),
        )));
        self
    }

    /// Set the applied watermark for read-after-write fencing.
    ///
    /// Should be set when async apply is enabled. The watermark is obtained
    /// from [`LanceStateMachine::with_async_apply`].
    pub fn with_applied_watermark(mut self, watermark: AppliedWatermark) -> Self {
        self.applied_watermark = Some(watermark);
        self
    }

    /// Set the MDBX manifest manager for crash-consistent metadata.
    ///
    /// The manifest will be stopped when [`shutdown`] is called.
    pub fn with_manifest(mut self, manifest: Arc<LanceManifestManager>) -> Self {
        self.manifest = Some(manifest);
        self
    }

    /// Get a reference to the applied watermark, if async apply is enabled.
    pub fn applied_watermark(&self) -> Option<&AppliedWatermark> {
        self.applied_watermark.as_ref()
    }

    /// Set a per-table batcher configuration, overriding the default.
    ///
    /// Requires that write batching was enabled via [`with_write_batcher`].
    /// Has no effect if batching is not enabled.
    ///
    /// When an MDBX manifest is configured, the batcher/processor config is
    /// persisted so it survives restarts (restored automatically in [`start`]).
    pub fn configure_table_batcher(
        &self,
        table_name: impl Into<String>,
        config: WriteBatcherConfig,
    ) {
        let table_name = table_name.into();
        if let Some(batcher) = &self.write_batcher {
            batcher.configure_table(&table_name, config.clone());
        }

        // Persist batcher config to MDBX (fire-and-forget).
        if let Some(manifest) = &self.manifest {
            let group_id = self.group_id;
            let batcher_config = config.to_persisted_batcher();
            let processor_desc = config.processor_descriptor();
            let manifest = manifest.clone();
            let tname = table_name;
            tokio::spawn(async move {
                if let Ok(Some(mut entry)) = manifest.read_table(group_id, &tname) {
                    entry.config.batcher = Some(batcher_config);
                    entry.config.processor = processor_desc;
                    let update = LanceManifestManager::build_table_only_update(
                        group_id,
                        TableUpdate::Set {
                            table_name: Arc::from(tname.as_str()),
                            entry,
                        },
                    );
                    manifest.send_update(update).await;
                }
            });
        }
    }

    /// Start background tasks (leader watcher, seal checker, flush orchestrator, compaction).
    ///
    /// Also restores persisted per-table batcher/processor configs from MDBX
    /// if both a manifest and write batcher are configured.
    pub fn start(&self) {
        // Restore persisted batcher configs from MDBX.
        if let (Some(manifest), Some(batcher)) = (&self.manifest, &self.write_batcher) {
            match manifest.read_all_tables(self.group_id) {
                Ok(entries) => {
                    for (table_name, entry) in entries {
                        if entry.config.batcher.is_some() || entry.config.processor.is_some() {
                            let batcher_cfg = entry.config.batcher.as_ref();
                            let processor_desc = entry.config.processor.as_ref();
                            let restored = WriteBatcherConfig::from_persisted(
                                batcher_cfg.unwrap_or(&crate::types::PersistedBatcherConfig {
                                    linger_ms: 5,
                                    max_batch_bytes: 8 * 1024 * 1024,
                                    channel_capacity: 1024,
                                }),
                                processor_desc,
                            );
                            debug!(
                                table = %table_name,
                                has_processor = processor_desc.is_some(),
                                "Restored persisted batcher config"
                            );
                            batcher.configure_table(&table_name, restored);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read persisted batcher configs: {}", e);
                }
            }
        }

        let mut handles = self.task_handles.lock();

        let catalog = self.catalog_name.clone();

        // 1. Leader election watcher
        handles.push(tokio::spawn(leader_watcher(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            catalog.clone(),
            self.shutdown.clone(),
            self.shutdown_flag.clone(),
        )));

        // 2. Seal check loop (all tables)
        handles.push(tokio::spawn(seal_check_loop(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            catalog.clone(),
            self.seal_check_interval,
            self.shutdown.clone(),
            self.shutdown_flag.clone(),
        )));

        // 3. Flush orchestration loop (all tables)
        handles.push(tokio::spawn(flush_orchestrator(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            self.flush_check_interval,
            self.shutdown.clone(),
            self.shutdown_flag.clone(),
        )));

        // 4. Compaction loop (all tables)
        handles.push(tokio::spawn(compaction_loop(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            self.compaction_check_interval,
            self.shutdown.clone(),
            self.shutdown_flag.clone(),
        )));

        info!(
            node_id = self.node_id,
            "LanceRaftNode background tasks started"
        );
    }

    /// Create a table through Raft consensus.
    pub async fn create_table(
        &self,
        table_name: &str,
        schema: &arrow_schema::Schema,
    ) -> Result<WriteResult, WriteError> {
        let schema_ipc =
            ipc::schema_to_ipc(schema).map_err(|e| WriteError::Encode(e.to_string()))?;

        let cmd = LanceCommand::CreateTable {
            table_name: Arc::from(table_name),
            schema_ipc,
        };
        self.propose(cmd).await
    }

    /// Drop a table through Raft consensus.
    pub async fn drop_table(&self, table_name: &str) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::DropTable {
            table_name: Arc::from(table_name),
        };
        self.propose(cmd).await
    }

    /// Write record batches to a specific table through Raft consensus.
    ///
    /// When a [`WriteBatcher`] is configured, writes are coalesced over the
    /// linger window before being proposed as a single Raft entry. Otherwise
    /// each call produces an individual Raft proposal.
    ///
    /// Returns a [`WriteResult`] containing the committed log index, which
    /// can be used for read-after-write fencing.
    pub async fn write_records(
        &self,
        table_name: &str,
        batches: &[arrow_array::RecordBatch],
    ) -> Result<WriteResult, WriteError> {
        if batches.is_empty() {
            return Ok(WriteResult {
                log_index: 0,
                response: LanceResponse::Ok,
            });
        }

        // Route through batcher if configured.
        if let Some(batcher) = &self.write_batcher {
            return batcher.submit(table_name, batches.to_vec()).await;
        }

        let data =
            ipc::encode_record_batches(batches).map_err(|e| WriteError::Encode(e.to_string()))?;

        let cmd = LanceCommand::AppendRecords {
            table_name: Arc::from(table_name),
            data,
        };
        self.propose(cmd).await
    }

    /// Delete rows matching a SQL filter predicate from all tiers of a table.
    ///
    /// Uses Lance deletion vectors (soft delete) — no data files are rewritten.
    /// The delete is applied to all tiers (active, sealed, S3) that contain data,
    /// ensuring a consistent view across the entire table.
    pub async fn delete_records(
        &self,
        table_name: &str,
        filter: &str,
    ) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::DeleteRecords {
            table_name: Arc::from(table_name),
            filter: filter.to_string(),
        };
        self.propose(cmd).await
    }

    /// Update rows matching a filter with replacement data.
    ///
    /// Implemented as a cross-tier soft-delete of matching rows followed by
    /// an append of the replacement batches to the active segment.
    pub async fn update_records(
        &self,
        table_name: &str,
        filter: &str,
        batches: &[arrow_array::RecordBatch],
    ) -> Result<WriteResult, WriteError> {
        let data =
            ipc::encode_record_batches(batches).map_err(|e| WriteError::Encode(e.to_string()))?;
        let cmd = LanceCommand::UpdateRecords {
            table_name: Arc::from(table_name),
            filter: filter.to_string(),
            data,
        };
        self.propose(cmd).await
    }

    // =========================================================================
    // Multi-table transaction API
    // =========================================================================

    /// Generate a new transaction ID. Local only — no Raft round-trip.
    pub fn begin_txn(&self) -> TxnId {
        TxnId::new()
    }

    /// Send a chunk of transaction operations through Raft.
    ///
    /// Chunks are buffered in the state machine and not applied until
    /// [`txn_commit`] is called.
    pub async fn txn_chunk(
        &self,
        txn_id: TxnId,
        seq: u32,
        ops: Vec<TxnOp>,
    ) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::TxnChunk { txn_id, seq, ops };
        self.propose(cmd).await
    }

    /// Commit a multi-table transaction. Triggers reassembly of all buffered
    /// chunks and atomic apply across all affected tables.
    pub async fn txn_commit(
        &self,
        txn_id: TxnId,
        total_chunks: u32,
    ) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::TxnCommit {
            txn_id,
            total_chunks,
        };
        self.propose(cmd).await
    }

    /// Abort a multi-table transaction. Discards all buffered chunks.
    pub async fn txn_abort(&self, txn_id: TxnId) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::TxnAbort { txn_id };
        self.propose(cmd).await
    }

    /// Propose a command through Raft consensus.
    ///
    /// Returns a [`WriteResult`] containing the committed log index.
    pub async fn propose(&self, cmd: LanceCommand) -> Result<WriteResult, WriteError> {
        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(resp) => {
                let response = resp.response().clone();
                if let LanceResponse::Error(e) = &response {
                    return Err(WriteError::Raft(e.clone()));
                }
                let log_index = resp.log_id().index;
                Ok(WriteResult {
                    log_index,
                    response,
                })
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                Err(WriteError::NotLeader {
                    leader_id: fwd.leader_id,
                    leader_node: fwd.leader_node,
                })
            }
            Err(RaftError::APIError(e)) => Err(WriteError::Raft(e.to_string())),
            Err(RaftError::Fatal(e)) => Err(WriteError::Fatal(e.to_string())),
        }
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get the Raft group ID.
    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    /// Check if this node is currently the leader.
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Get the current leader's node ID, if known.
    pub fn current_leader(&self) -> Option<u64> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.current_leader
    }

    /// Get a reference to the underlying Raft handle.
    pub fn raft(&self) -> &Raft<LanceTypeConfig> {
        &self.raft
    }

    /// Get a reference to the underlying engine.
    pub fn engine(&self) -> &Arc<BisqueLance> {
        &self.engine
    }

    /// Get a reference to the manifest manager (if configured).
    pub fn manifest(&self) -> Option<&Arc<LanceManifestManager>> {
        self.manifest.as_ref()
    }

    /// Shutdown background tasks and the engine.
    pub async fn shutdown(&self) {
        info!(node_id = self.node_id, "Shutting down LanceRaftNode");

        // Drain the write batcher first so any pending writes complete.
        if let Some(batcher) = &self.write_batcher {
            batcher.shutdown().await;
        }

        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown.notify_waiters();

        let handles: Vec<_> = {
            let mut locked = self.task_handles.lock();
            locked.drain(..).collect()
        };
        for handle in handles {
            let _ = handle.await;
        }

        // Shut down the Raft state machine (stops heartbeat, replication,
        // and drains in-flight proposals). Must happen after background tasks
        // are joined (they call client_write) but before engine shutdown
        // (raft may still apply entries during its shutdown).
        // Use a timeout because raft.shutdown() can block if peers are
        // unreachable (waiting for replication acknowledgments).
        match tokio::time::timeout(Duration::from_secs(5), self.raft.shutdown()).await {
            Ok(Err(e)) => error!("Raft shutdown error: {:?}", e),
            Err(_) => warn!("Raft shutdown timed out after 5s, proceeding with cleanup"),
            _ => {}
        }

        if let Err(e) = self.engine.shutdown().await {
            error!("Engine shutdown error: {}", e);
        }

        // Stop the manifest worker thread last, after all engine work is done.
        if let Some(manifest) = &self.manifest {
            manifest.stop();
        }
    }
}

// =============================================================================
// Write Error
// =============================================================================

/// Errors from [`LanceRaftNode::write_records`] and [`LanceRaftNode::propose`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum WriteError {
    /// This node is not the leader; the request should be forwarded.
    #[error("not leader (leader: {leader_id:?})")]
    NotLeader {
        leader_id: Option<u64>,
        leader_node: Option<openraft::impls::BasicNode>,
    },

    /// Arrow IPC encoding failed.
    #[error("encode error: {0}")]
    Encode(String),

    /// Raft error (non-fatal).
    #[error("raft error: {0}")]
    Raft(String),

    /// Fatal Raft error (node must be restarted).
    #[error("fatal raft error: {0}")]
    Fatal(String),
}

// =============================================================================
// Background Tasks
// =============================================================================

/// Watches Raft metrics for leadership changes.
async fn leader_watcher(
    raft: Raft<LanceTypeConfig>,
    engine: Arc<BisqueLance>,
    node_id: u64,
    catalog_name: String,
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
) {
    let m_is_leader = metrics::gauge!("bisque_raft_is_leader", "catalog" => catalog_name);
    let mut metrics_rx = raft.metrics();
    let mut was_leader = false;

    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        let is_leader = {
            let m = metrics_rx.borrow_watched();
            m.state == ServerState::Leader && m.current_leader == Some(node_id)
        };

        if is_leader && !was_leader {
            m_is_leader.set(1.0);
            info!(node_id, "This node became leader");
            on_become_leader(&engine).await;
        } else if !is_leader && was_leader {
            m_is_leader.set(0.0);
            info!(node_id, "This node lost leadership");
        }

        was_leader = is_leader;

        tokio::select! {
            result = metrics_rx.changed() => {
                if result.is_err() {
                    debug!("Metrics channel closed, leader watcher exiting");
                    return;
                }
            }
            _ = shutdown.notified() => {
                debug!("Leader watcher shutting down");
                return;
            }
        }
    }
}

/// Called when this node becomes the Raft leader.
/// Recovers any incomplete flushes across all tables.
async fn on_become_leader(engine: &BisqueLance) {
    for table_name in engine.list_tables() {
        if let Some(table) = engine.get_table(&table_name) {
            if let Err(e) = table.recover_flush().await {
                error!(table = %table_name, "Flush recovery on leader election failed: {}", e);
            }
        }
    }
}

/// Periodically checks if any table's active segment should be sealed.
async fn seal_check_loop(
    raft: Raft<LanceTypeConfig>,
    engine: Arc<BisqueLance>,
    node_id: u64,
    catalog_name: String,
    interval: Duration,
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
) {
    let m_applied_index = metrics::gauge!("bisque_raft_applied_index", "catalog" => catalog_name);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.notified() => {
                debug!("Seal check loop shutting down");
                return;
            }
        }

        if !is_leader(&raft, node_id) {
            continue;
        }

        // Emit applied index gauge on each seal check iteration (leader only).
        if let Some(log_id) = raft.metrics().borrow_watched().last_applied {
            m_applied_index.set(log_id.index as f64);
        }

        for table_name in engine.list_tables() {
            let table = match engine.get_table(&table_name) {
                Some(t) => t,
                None => continue,
            };

            if let Some(reason) = table.should_seal() {
                let sealed_id = table.catalog().active_segment;
                let new_id = table.next_segment_id();

                info!(
                    table = %table_name,
                    sealed = sealed_id,
                    new_active = new_id,
                    ?reason,
                    "Proposing seal of active segment"
                );

                let cmd = LanceCommand::SealActiveSegment {
                    table_name: Arc::from(table_name.as_str()),
                    sealed_segment_id: sealed_id,
                    new_active_segment_id: new_id,
                    reason,
                };

                if let Err(e) = propose_cmd(&raft, cmd).await {
                    warn!(table = %table_name, "Failed to propose seal: {}", e);
                }
            }
        }
    }
}

/// Flush orchestrator — detects sealed segments across all tables and drives flush pipelines.
async fn flush_orchestrator(
    raft: Raft<LanceTypeConfig>,
    engine: Arc<BisqueLance>,
    node_id: u64,
    interval: Duration,
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.notified() => {
                debug!("Flush orchestrator shutting down");
                return;
            }
        }

        if !is_leader(&raft, node_id) {
            continue;
        }

        for table_name in engine.list_tables() {
            let table = match engine.get_table(&table_name) {
                Some(t) => t,
                None => continue,
            };

            if table.is_flush_in_progress() {
                continue;
            }

            let handle = match table.begin_flush() {
                Ok(h) => h,
                Err(_) => continue,
            };

            info!(table = %table_name, segment_id = handle.segment_id, "Starting flush pipeline");

            // Step 1: Propose BeginFlush
            let tname: Arc<str> = Arc::from(table_name.as_str());
            let begin_cmd = LanceCommand::BeginFlush {
                table_name: tname.clone(),
                segment_id: handle.segment_id,
            };
            if let Err(e) = propose_cmd(&raft, begin_cmd).await {
                warn!(table = %table_name, "Failed to propose BeginFlush: {}", e);
                continue;
            }

            // Step 2: Execute the S3 upload
            let s3_version = match table.execute_flush(&handle).await {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        table = %table_name,
                        segment_id = handle.segment_id,
                        "S3 flush failed: {}. Will retry on next cycle.", e
                    );
                    continue;
                }
            };

            // Step 3: Propose PromoteToDeepStorage
            let promote_cmd = LanceCommand::PromoteToDeepStorage {
                table_name: tname.clone(),
                segment_id: handle.segment_id,
                s3_manifest_version: s3_version,
            };
            if let Err(e) = propose_cmd(&raft, promote_cmd).await {
                error!(
                    table = %table_name,
                    segment_id = handle.segment_id,
                    s3_version,
                    "Failed to propose PromoteToDeepStorage: {}. \
                     Data is in S3 but metadata not committed — will be recovered on next leader election.",
                    e
                );
            } else {
                info!(
                    table = %table_name,
                    segment_id = handle.segment_id,
                    s3_version,
                    "Flush pipeline completed successfully"
                );
            }
        }
    }
}

/// Periodic compaction across all tables.
async fn compaction_loop(
    raft: Raft<LanceTypeConfig>,
    engine: Arc<BisqueLance>,
    node_id: u64,
    interval: Duration,
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.notified() => {
                debug!("Compaction loop shutting down");
                return;
            }
        }

        if !is_leader(&raft, node_id) {
            continue;
        }

        for table_name in engine.list_tables() {
            let table = match engine.get_table(&table_name) {
                Some(t) => t,
                None => continue,
            };

            // Active tier compaction
            match table.compact_active().await {
                Ok(stats) if stats.fragments_removed > 0 => {
                    info!(
                        table = %table_name,
                        fragments_removed = stats.fragments_removed,
                        fragments_added = stats.fragments_added,
                        "Active segment compaction completed"
                    );
                }
                Err(e) => {
                    warn!(table = %table_name, "Active segment compaction failed: {}", e);
                }
                _ => {}
            }

            // S3 tier compaction
            if table.config().has_s3() && !table.is_flush_in_progress() {
                match table.compact_s3().await {
                    Ok(stats) if stats.fragments_removed > 0 => {
                        info!(
                            table = %table_name,
                            fragments_removed = stats.fragments_removed,
                            fragments_added = stats.fragments_added,
                            "S3 compaction completed"
                        );
                    }
                    Err(e) => {
                        warn!(table = %table_name, "S3 compaction failed: {}", e);
                    }
                    _ => {}
                }
            }
        }
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn is_leader(raft: &Raft<LanceTypeConfig>, node_id: u64) -> bool {
    let metrics_rx = raft.metrics();
    let m = metrics_rx.borrow_watched();
    m.state == ServerState::Leader && m.current_leader == Some(node_id)
}

async fn propose_cmd(
    raft: &Raft<LanceTypeConfig>,
    cmd: LanceCommand,
) -> std::result::Result<LanceResponse, String> {
    match raft.client_write(cmd).await {
        Ok(resp) => match resp.response().clone() {
            LanceResponse::Error(e) => Err(format!("state machine error: {}", e)),
            other => Ok(other),
        },
        Err(e) => Err(format!("raft write error: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_error_display_not_leader() {
        let err = WriteError::NotLeader {
            leader_id: Some(42),
            leader_node: None,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("not leader"), "got: {}", msg);
        assert!(msg.contains("42"), "got: {}", msg);

        // Also test with None leader_id
        let err_none = WriteError::NotLeader {
            leader_id: None,
            leader_node: None,
        };
        let msg_none = format!("{}", err_none);
        assert!(msg_none.contains("not leader"), "got: {}", msg_none);
        assert!(msg_none.contains("None"), "got: {}", msg_none);
    }

    #[test]
    fn test_write_error_display_encode() {
        let err = WriteError::Encode("bad IPC data".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("encode error"), "got: {}", msg);
        assert!(msg.contains("bad IPC data"), "got: {}", msg);
    }

    #[test]
    fn test_write_error_display_raft() {
        let err = WriteError::Raft("consensus timeout".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("raft error"), "got: {}", msg);
        assert!(msg.contains("consensus timeout"), "got: {}", msg);
    }

    #[test]
    fn test_write_error_display_fatal() {
        let err = WriteError::Fatal("storage corruption".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("fatal raft error"), "got: {}", msg);
        assert!(msg.contains("storage corruption"), "got: {}", msg);
    }

    #[test]
    fn test_write_error_clone() {
        let err = WriteError::NotLeader {
            leader_id: Some(7),
            leader_node: None,
        };
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));

        let err2 = WriteError::Encode("test".to_string());
        let cloned2 = err2.clone();
        assert_eq!(format!("{}", err2), format!("{}", cloned2));

        let err3 = WriteError::Raft("test".to_string());
        let cloned3 = err3.clone();
        assert_eq!(format!("{}", err3), format!("{}", cloned3));

        let err4 = WriteError::Fatal("test".to_string());
        let cloned4 = err4.clone();
        assert_eq!(format!("{}", err4), format!("{}", cloned4));
    }
}
