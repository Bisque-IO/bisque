//! Raft integration layer for bisque-lance.
//!
//! `LanceRaftNode` wires the `BisqueLance` engine and `LanceStateMachine` into a
//! `MultiRaftManager` Raft group, and runs background tasks for:
//! - Leader election detection via metrics watcher
//! - Periodic seal checks across all tables
//! - Flush orchestration per table (sealed segment → S3, leader only)
//! - Periodic compaction across all tables

use std::sync::Arc;
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use openraft::{Raft, ServerState};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::engine::BisqueLance;
use crate::ipc;
use crate::types::{LanceCommand, LanceResponse};
use crate::LanceTypeConfig;

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
    /// Interval between seal threshold checks.
    seal_check_interval: Duration,
    /// Interval between flush attempts when a sealed segment is pending.
    flush_check_interval: Duration,
    /// Interval between compaction checks for active and S3 tiers.
    compaction_check_interval: Duration,
    /// Notify handle to trigger shutdown of background tasks.
    shutdown: Arc<Notify>,
    /// Background task handles.
    task_handles: parking_lot::Mutex<Vec<JoinHandle<()>>>,
}

impl LanceRaftNode {
    /// Create a new `LanceRaftNode`.
    ///
    /// Does NOT start background tasks — call [`start`] after construction.
    pub fn new(
        raft: Raft<LanceTypeConfig>,
        engine: Arc<BisqueLance>,
        node_id: u64,
    ) -> Self {
        Self {
            raft,
            engine,
            node_id,
            seal_check_interval: Duration::from_secs(5),
            flush_check_interval: Duration::from_secs(10),
            compaction_check_interval: Duration::from_secs(60),
            shutdown: Arc::new(Notify::new()),
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

    /// Start background tasks (leader watcher, seal checker, flush orchestrator, compaction).
    pub fn start(&self) {
        let mut handles = self.task_handles.lock();

        // 1. Leader election watcher
        handles.push(tokio::spawn(leader_watcher(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            self.shutdown.clone(),
        )));

        // 2. Seal check loop (all tables)
        handles.push(tokio::spawn(seal_check_loop(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            self.seal_check_interval,
            self.shutdown.clone(),
        )));

        // 3. Flush orchestration loop (all tables)
        handles.push(tokio::spawn(flush_orchestrator(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            self.flush_check_interval,
            self.shutdown.clone(),
        )));

        // 4. Compaction loop (all tables)
        handles.push(tokio::spawn(compaction_loop(
            self.raft.clone(),
            self.engine.clone(),
            self.node_id,
            self.compaction_check_interval,
            self.shutdown.clone(),
        )));

        info!(node_id = self.node_id, "LanceRaftNode background tasks started");
    }

    /// Create a table through Raft consensus.
    pub async fn create_table(
        &self,
        table_name: &str,
        schema: &arrow_schema::Schema,
    ) -> Result<LanceResponse, WriteError> {
        let schema_ipc = ipc::schema_to_ipc(schema)
            .map_err(|e| WriteError::Encode(e.to_string()))?;

        let cmd = LanceCommand::CreateTable {
            table_name: table_name.to_string(),
            schema_ipc,
        };
        self.propose(cmd).await
    }

    /// Drop a table through Raft consensus.
    pub async fn drop_table(
        &self,
        table_name: &str,
    ) -> Result<LanceResponse, WriteError> {
        let cmd = LanceCommand::DropTable {
            table_name: table_name.to_string(),
        };
        self.propose(cmd).await
    }

    /// Write record batches to a specific table through Raft consensus.
    pub async fn write_records(
        &self,
        table_name: &str,
        batches: &[arrow_array::RecordBatch],
    ) -> Result<LanceResponse, WriteError> {
        if batches.is_empty() {
            return Ok(LanceResponse::Ok);
        }

        let data = ipc::encode_record_batches(batches)
            .map_err(|e| WriteError::Encode(e.to_string()))?;

        let cmd = LanceCommand::AppendRecords {
            table_name: table_name.to_string(),
            data,
        };
        self.propose(cmd).await
    }

    /// Propose a command through Raft consensus.
    pub async fn propose(&self, cmd: LanceCommand) -> Result<LanceResponse, WriteError> {
        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(resp) => Ok(resp.response().clone()),
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

    /// Shutdown background tasks and the engine.
    pub async fn shutdown(&self) {
        info!(node_id = self.node_id, "Shutting down LanceRaftNode");
        self.shutdown.notify_waiters();

        let handles: Vec<_> = {
            let mut locked = self.task_handles.lock();
            locked.drain(..).collect()
        };
        for handle in handles {
            let _ = handle.await;
        }

        if let Err(e) = self.engine.shutdown().await {
            error!("Engine shutdown error: {}", e);
        }
    }
}

// =============================================================================
// Write Error
// =============================================================================

/// Errors from [`LanceRaftNode::write_records`] and [`LanceRaftNode::propose`].
#[derive(Debug, thiserror::Error)]
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
    shutdown: Arc<Notify>,
) {
    let mut metrics_rx = raft.metrics();
    let mut was_leader = false;

    loop {
        let is_leader = {
            let m = metrics_rx.borrow_watched();
            m.state == ServerState::Leader && m.current_leader == Some(node_id)
        };

        if is_leader && !was_leader {
            info!(node_id, "This node became leader");
            on_become_leader(&engine).await;
        } else if !is_leader && was_leader {
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
    interval: Duration,
    shutdown: Arc<Notify>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
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
                    table_name: table_name.clone(),
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
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
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
            let begin_cmd = LanceCommand::BeginFlush {
                table_name: table_name.clone(),
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
                table_name: table_name.clone(),
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
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
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
            LanceResponse::Ok => Ok(LanceResponse::Ok),
            LanceResponse::Error(e) => Err(format!("state machine error: {}", e)),
        },
        Err(e) => Err(format!("raft write error: {}", e)),
    }
}
