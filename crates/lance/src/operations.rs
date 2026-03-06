//! Operations manager — tracks and limits concurrent background operations
//! across all storage tiers (hot, warm, cold).
//!
//! - **Hot/Warm operations** (compaction, seal indexing) are automatic and tracked
//!   for observability only — they run inline without queue/semaphore control.
//! - **Cold operations** (S3 reindex, S3 compaction, flush) are user-triggered and
//!   go through a global + per-catalog semaphore queue.
//!
//! Publishes operation state changes via a broadcast channel so WebSocket
//! subscribers can receive real-time updates.

use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, broadcast};
use tracing::{info, warn};
use uuid::Uuid;

use crate::engine::BisqueLance;

/// Type of background operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpType {
    Compact,
    Reindex,
    Flush,
}

impl std::fmt::Display for OpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpType::Compact => write!(f, "compact"),
            OpType::Reindex => write!(f, "reindex"),
            OpType::Flush => write!(f, "flush"),
        }
    }
}

/// Storage tier the operation targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpTier {
    Hot,
    Warm,
    Cold,
}

impl std::fmt::Display for OpTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpTier::Hot => write!(f, "hot"),
            OpTier::Warm => write!(f, "warm"),
            OpTier::Cold => write!(f, "cold"),
        }
    }
}

/// Current status of an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpStatus {
    Queued,
    Running,
    Done,
    Failed,
    Cancelled,
}

/// A tracked background operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub node_id: u64,
    pub op_type: OpType,
    pub tier: OpTier,
    pub tenant: String,
    pub catalog: String,
    pub catalog_type: String,
    pub table: String,
    pub status: OpStatus,
    /// Progress from 0.0 to 1.0.
    pub progress: f64,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragments_done: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragments_total: Option<u64>,
}

/// Internal mutable state for a running operation.
struct OpState {
    op: Operation,
    created_instant: Instant,
}

/// Handle returned by [`OperationsManager::track`] for inline (non-queued)
/// operations. The caller updates progress and completes/fails the operation.
pub struct TrackedOp {
    id: String,
    mgr: Arc<OperationsManager>,
}

impl TrackedOp {
    /// Get the operation ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Update progress (0.0 to 1.0) and optionally fragment counts.
    pub fn set_progress(&self, progress: f64, fragments_done: Option<u64>, fragments_total: Option<u64>) {
        if let Some(mut entry) = self.mgr.ops.get_mut(&self.id) {
            entry.op.progress = progress;
            if let Some(fd) = fragments_done {
                entry.op.fragments_done = Some(fd);
            }
            if let Some(ft) = fragments_total {
                entry.op.fragments_total = Some(ft);
            }
        }
        self.mgr.notify(&self.id);
    }

    /// Mark the operation as successfully completed.
    pub fn complete(self) {
        self.mgr.complete_op(&self.id);
    }

    /// Mark the operation as failed.
    pub fn fail(self, error: String) {
        self.mgr.fail_op(&self.id, error);
    }
}

/// Manages background operations with global and per-catalog concurrency limits.
///
/// State changes are broadcast via a channel that WebSocket handlers can subscribe to.
pub struct OperationsManager {
    node_id: u64,
    ops: DashMap<String, OpState>,
    global_semaphore: Arc<Semaphore>,
    catalog_semaphores: DashMap<String, Arc<Semaphore>>,
    max_per_catalog: usize,
    /// How long to retain completed operations (seconds).
    retention_secs: u64,
    /// Broadcast channel for operation state change notifications.
    tx: broadcast::Sender<Operation>,
}

impl OperationsManager {
    /// Create a new operations manager.
    ///
    /// - `max_global`: maximum concurrent **queued** (cold) operations across all catalogs
    /// - `max_per_catalog`: maximum concurrent **queued** (cold) operations per catalog
    pub fn new(node_id: u64, max_global: usize, max_per_catalog: usize) -> Arc<Self> {
        let (tx, _) = broadcast::channel(256);

        let mgr = Arc::new(Self {
            node_id,
            ops: DashMap::new(),
            global_semaphore: Arc::new(Semaphore::new(max_global)),
            catalog_semaphores: DashMap::new(),
            max_per_catalog,
            retention_secs: 3600, // 1 hour
            tx,
        });

        // Start reaper for completed operations
        let reaper = mgr.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                reaper.reap_completed();
            }
        });

        mgr
    }

    /// Subscribe to operation state change events.
    pub fn subscribe(&self) -> broadcast::Receiver<Operation> {
        self.tx.subscribe()
    }

    /// Broadcast the current state of an operation.
    fn notify(&self, op_id: &str) {
        if let Some(entry) = self.ops.get(op_id) {
            let _ = self.tx.send(entry.op.clone());
        }
    }

    /// Get or create a per-catalog semaphore.
    fn catalog_semaphore(&self, catalog: &str) -> Arc<Semaphore> {
        self.catalog_semaphores
            .entry(catalog.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.max_per_catalog)))
            .clone()
    }

    // =========================================================================
    // Tracked (inline) operations — hot/warm, no queue, no semaphore
    // =========================================================================

    /// Track an inline operation that is about to start immediately.
    ///
    /// Used for automatic hot/warm operations (compaction, seal indexing).
    /// The caller runs the operation and uses the returned [`TrackedOp`] handle
    /// to update progress and report completion/failure.
    pub fn track(
        self: &Arc<Self>,
        op_type: OpType,
        tier: OpTier,
        tenant: String,
        catalog: String,
        catalog_type: String,
        table: String,
    ) -> TrackedOp {
        let id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().to_rfc3339();

        let op = Operation {
            id: id.clone(),
            node_id: self.node_id,
            op_type,
            tier,
            tenant,
            catalog,
            catalog_type,
            table,
            status: OpStatus::Running,
            progress: 0.0,
            created_at: now.clone(),
            started_at: Some(now),
            finished_at: None,
            error: None,
            fragments_done: None,
            fragments_total: None,
        };

        self.ops.insert(
            id.clone(),
            OpState {
                op,
                created_instant: Instant::now(),
            },
        );
        self.notify(&id);

        TrackedOp {
            id,
            mgr: self.clone(),
        }
    }

    // =========================================================================
    // Queued (cold) operations — semaphore-controlled
    // =========================================================================

    /// Queue a cold reindex operation. Returns the operation ID immediately.
    pub fn submit_reindex(
        self: &Arc<Self>,
        tenant: String,
        catalog: String,
        catalog_type: String,
        table: String,
        engine: Arc<BisqueLance>,
    ) -> String {
        self.submit(OpType::Reindex, OpTier::Cold, tenant, catalog, catalog_type, table, engine)
    }

    /// Queue a cold compaction operation. Returns the operation ID immediately.
    pub fn submit_compact(
        self: &Arc<Self>,
        tenant: String,
        catalog: String,
        catalog_type: String,
        table: String,
        engine: Arc<BisqueLance>,
    ) -> String {
        self.submit(OpType::Compact, OpTier::Cold, tenant, catalog, catalog_type, table, engine)
    }

    /// Queue a flush (warm→cold promote) operation. Returns the operation ID immediately.
    pub fn submit_flush(
        self: &Arc<Self>,
        tenant: String,
        catalog: String,
        catalog_type: String,
        table: String,
        engine: Arc<BisqueLance>,
    ) -> String {
        self.submit(OpType::Flush, OpTier::Cold, tenant, catalog, catalog_type, table, engine)
    }

    fn submit(
        self: &Arc<Self>,
        op_type: OpType,
        tier: OpTier,
        tenant: String,
        catalog: String,
        catalog_type: String,
        table: String,
        engine: Arc<BisqueLance>,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().to_rfc3339();

        let op = Operation {
            id: id.clone(),
            node_id: self.node_id,
            op_type,
            tier,
            tenant,
            catalog: catalog.clone(),
            catalog_type,
            table: table.clone(),
            status: OpStatus::Queued,
            progress: 0.0,
            created_at: now,
            started_at: None,
            finished_at: None,
            error: None,
            fragments_done: None,
            fragments_total: None,
        };

        self.ops.insert(
            id.clone(),
            OpState {
                op,
                created_instant: Instant::now(),
            },
        );

        self.notify(&id);

        let mgr = self.clone();
        let op_id = id.clone();
        tokio::spawn(async move {
            mgr.run_queued(op_id, op_type, catalog, table, engine)
                .await;
        });

        id
    }

    /// Cancel a queued operation. Returns true if cancelled.
    pub fn cancel(&self, op_id: &str) -> bool {
        if let Some(mut entry) = self.ops.get_mut(op_id) {
            if entry.op.status == OpStatus::Queued {
                entry.op.status = OpStatus::Cancelled;
                entry.op.finished_at = Some(chrono::Utc::now().to_rfc3339());
                drop(entry);
                self.notify(op_id);
                return true;
            }
        }
        false
    }

    /// List all operations, optionally filtered.
    pub fn list(
        &self,
        op_type: Option<OpType>,
        tier: Option<OpTier>,
        status: Option<OpStatus>,
    ) -> Vec<Operation> {
        self.ops
            .iter()
            .filter(|entry| {
                if let Some(t) = op_type {
                    if entry.op.op_type != t {
                        return false;
                    }
                }
                if let Some(t) = tier {
                    if entry.op.tier != t {
                        return false;
                    }
                }
                if let Some(s) = status {
                    if entry.op.status != s {
                        return false;
                    }
                }
                true
            })
            .map(|entry| entry.op.clone())
            .collect()
    }

    /// Get a single operation by ID.
    pub fn get(&self, op_id: &str) -> Option<Operation> {
        self.ops.get(op_id).map(|entry| entry.op.clone())
    }

    // =========================================================================
    // Queued operation runner
    // =========================================================================

    async fn run_queued(
        &self,
        op_id: String,
        op_type: OpType,
        catalog: String,
        table: String,
        engine: Arc<BisqueLance>,
    ) {
        if self.is_cancelled(&op_id) {
            return;
        }

        // Acquire per-catalog then global semaphore
        let cat_sem = self.catalog_semaphore(&catalog);
        let _cat_permit = cat_sem.acquire().await.unwrap();

        if self.is_cancelled(&op_id) {
            return;
        }

        let _global_permit = self.global_semaphore.acquire().await.unwrap();

        if self.is_cancelled(&op_id) {
            return;
        }

        self.update_status(&op_id, OpStatus::Running);

        info!(op_id = %op_id, op = %op_type, catalog = %catalog, table = %table, "Starting queued operation");

        let table_engine = match engine.get_table(&table) {
            Some(t) => t,
            None => {
                self.fail_op(&op_id, format!("Table '{}' not found", table));
                return;
            }
        };

        // Set fragment count for progress tracking (from S3 dataset)
        if let Some(ds) = table_engine.s3_dataset_snapshot().await {
            let total = ds.get_fragments().len() as u64;
            if let Some(mut entry) = self.ops.get_mut(&op_id) {
                entry.op.fragments_total = Some(total);
            }
            self.notify(&op_id);
        }

        let result = match op_type {
            OpType::Reindex => table_engine.create_s3_indices().await.map(|_| None),
            OpType::Compact => table_engine.compact_s3().await.map(|s| Some(s)),
            OpType::Flush => {
                // Flush is a placeholder — actual flush is driven by the Raft state machine.
                // This exists so manual flush triggers can be tracked.
                Ok(None)
            }
        };

        match result {
            Ok(stats) => {
                if let Some(stats) = stats {
                    if let Some(mut entry) = self.ops.get_mut(&op_id) {
                        entry.op.fragments_done = Some(stats.fragments_removed as u64);
                    }
                }
                self.complete_op(&op_id);
                info!(op_id = %op_id, op = %op_type, catalog = %catalog, table = %table, "Queued operation complete");
            }
            Err(e) => {
                self.fail_op(&op_id, e.to_string());
                warn!(op_id = %op_id, op = %op_type, catalog = %catalog, table = %table, "Queued operation failed: {}", e);
            }
        }
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    fn is_cancelled(&self, op_id: &str) -> bool {
        self.ops
            .get(op_id)
            .map(|e| e.op.status == OpStatus::Cancelled)
            .unwrap_or(true)
    }

    fn update_status(&self, op_id: &str, status: OpStatus) {
        if let Some(mut entry) = self.ops.get_mut(op_id) {
            entry.op.status = status;
            if status == OpStatus::Running {
                entry.op.started_at = Some(chrono::Utc::now().to_rfc3339());
            }
        }
        self.notify(op_id);
    }

    fn complete_op(&self, op_id: &str) {
        if let Some(mut entry) = self.ops.get_mut(op_id) {
            entry.op.status = OpStatus::Done;
            entry.op.progress = 1.0;
            entry.op.finished_at = Some(chrono::Utc::now().to_rfc3339());
        }
        self.notify(op_id);
    }

    fn fail_op(&self, op_id: &str, error: String) {
        if let Some(mut entry) = self.ops.get_mut(op_id) {
            entry.op.status = OpStatus::Failed;
            entry.op.error = Some(error);
            entry.op.finished_at = Some(chrono::Utc::now().to_rfc3339());
        }
        self.notify(op_id);
    }

    /// Remove completed operations older than retention period.
    fn reap_completed(&self) {
        let cutoff = Instant::now() - std::time::Duration::from_secs(self.retention_secs);
        self.ops.retain(|_, state| {
            if matches!(
                state.op.status,
                OpStatus::Done | OpStatus::Failed | OpStatus::Cancelled
            ) {
                state.created_instant > cutoff
            } else {
                true
            }
        });
    }
}
