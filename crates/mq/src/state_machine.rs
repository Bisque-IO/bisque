//! Raft state machine implementation for bisque-mq.

use std::io::{self, Cursor};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex as ParkingMutex;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, LogId, OptionalSend};
use tracing::{info, warn};

use bisque_raft::{SegmentPrefetcher, SegmentSyncClient};

use crate::MqTypeConfig;
use crate::async_apply::AsyncApplyManager;
use crate::config::MqConfig;
use crate::engine::MqEngine;
use crate::forward::RaftBacklog;
use crate::metadata::MqMetadata;
use crate::retention::RetentionEvaluator;
use crate::segment_index::SegmentIndexMap;
use crate::types::{MqApplyResponse, MqCommand, MqSnapshotData};

/// Raft state machine that drives the MQ engine.
///
/// On every startup the state machine returns `(None, Default)` from
/// `applied_state()`, forcing openraft to replay the entire available raft
/// log. This rebuilds the in-memory `MqEngine` from scratch.
pub struct MqStateMachine {
    engine: Arc<MqEngine>,
    last_applied: Option<openraft::alias::LogIdOf<MqTypeConfig>>,
    last_membership: openraft::alias::StoredMembershipOf<MqTypeConfig>,
    purge_floor: Option<Arc<AtomicU64>>,
    pin_ceiling: Option<Arc<AtomicU64>>,
    prefetcher: Option<SegmentPrefetcher>,
    group_id: u64,
    /// Client for syncing segment files from the leader during snapshot install.
    sync_client: Option<SegmentSyncClient>,
    /// Address of this node's segment sync server (included in outgoing snapshots).
    sync_addr: Option<String>,
    /// Group directory path for listing segment files in snapshot builder.
    group_dir: Option<std::path::PathBuf>,
    /// Per-segment index builders — lives outside the engine for
    /// lock-free concurrent reads during applies.
    segment_indexes: Arc<SegmentIndexMap>,
    /// Shared with log storage — unpinned segment IDs are pushed here
    /// when segments are unpinned (Level 1). Drained after each apply batch.
    purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
    /// Pull-based async apply manager.
    async_apply: Option<Arc<AsyncApplyManager>>,
    /// Level 2 retention evaluator — deletes segment files based on
    /// per-entity retention policies.
    retention_evaluator: Option<Arc<RetentionEvaluator>>,
    /// Shared raft backlog budget. Released here in `apply()` after each
    /// TAG_BATCH / TAG_FORWARDED_BATCH entry is committed.
    raft_backlog: Option<Arc<RaftBacklog>>,
}

impl MqStateMachine {
    /// Returns a shared reference to the engine.
    pub fn engine(&self) -> &Arc<MqEngine> {
        &self.engine
    }

    pub fn new(engine: MqEngine) -> Self {
        Self {
            engine: Arc::new(engine),
            last_applied: None,
            last_membership: openraft::StoredMembership::default(),
            purge_floor: None,
            pin_ceiling: None,
            prefetcher: None,
            group_id: 0,
            sync_client: None,
            sync_addr: None,
            group_dir: None,
            segment_indexes: Arc::new(SegmentIndexMap::new()),
            purged_segments: None,
            async_apply: None,
            retention_evaluator: None,
            raft_backlog: None,
        }
    }

    /// Initialize the pull-based async apply system. Call after all builder
    /// methods are invoked, before the state machine is used.
    pub fn init_async_apply(&mut self, config: &MqConfig) -> Result<(), String> {
        let prefetcher = self
            .prefetcher
            .clone()
            .ok_or_else(|| "init_async_apply requires a prefetcher".to_string())?;
        let initial_cursor = self.last_applied.map(|la| la.index).unwrap_or(0);
        let manager = AsyncApplyManager::new(
            &config.parallel_apply,
            Arc::clone(&self.engine),
            prefetcher,
            self.purged_segments.clone(),
            self.group_id,
            initial_cursor,
            &config.catalog_name,
        );
        self.async_apply = Some(Arc::new(manager));
        Ok(())
    }

    /// Get a shared reference to the async apply manager.
    /// Returns `None` if `init_async_apply()` has not been called yet.
    pub fn async_apply(&self) -> Option<Arc<AsyncApplyManager>> {
        self.async_apply.clone()
    }

    /// Initialize the Level 2 retention evaluator. Call after all builder
    /// methods are invoked. Requires a purge_floor and group_dir.
    pub fn init_retention(&mut self, config: &MqConfig) {
        let purge_floor = match self.purge_floor.as_ref() {
            Some(f) => Arc::clone(f),
            None => return,
        };
        let group_dir = match self.group_dir.as_ref() {
            Some(d) => d.clone(),
            None => return,
        };

        let evaluator = RetentionEvaluator::new(
            group_dir,
            self.group_id,
            self.engine.shared_metadata(),
            purge_floor,
            config.retention_eval_interval,
            &config.catalog_name,
        );
        self.retention_evaluator = Some(Arc::new(evaluator));
    }

    pub fn with_purge_floor(mut self, floor: Arc<AtomicU64>) -> Self {
        self.purge_floor = Some(floor);
        self
    }

    pub fn with_pin_ceiling(mut self, ceiling: Arc<AtomicU64>) -> Self {
        self.pin_ceiling = Some(ceiling);
        self
    }

    pub fn with_purged_segments(mut self, purged: Arc<ParkingMutex<Vec<u64>>>) -> Self {
        self.purged_segments = Some(purged);
        self
    }

    pub fn with_prefetcher(mut self, prefetcher: SegmentPrefetcher) -> Self {
        self.prefetcher = Some(prefetcher);
        self
    }

    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = group_id;
        self
    }

    pub fn with_sync_client(mut self, client: SegmentSyncClient) -> Self {
        self.sync_client = Some(client);
        self
    }

    pub fn with_sync_addr(mut self, addr: String) -> Self {
        self.sync_addr = Some(addr);
        self
    }

    pub fn with_group_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.group_dir = Some(dir);
        self
    }

    pub fn with_raft_backlog(mut self, backlog: Arc<RaftBacklog>) -> Self {
        self.raft_backlog = Some(backlog);
        self
    }

    /// Get a shared reference to the lock-free metadata for leader tasks and routers.
    pub fn shared_metadata(&self) -> Arc<MqMetadata> {
        self.engine.shared_metadata()
    }

    /// Build a snapshot of all engine state.
    pub fn snapshot(&self) -> MqSnapshotData {
        self.engine.snapshot()
    }

    /// Get a shared reference to the segment index map.
    pub fn shared_segment_indexes(&self) -> Arc<SegmentIndexMap> {
        Arc::clone(&self.segment_indexes)
    }

    fn update_purge_floor(&mut self) {
        if let Some(ref floor) = self.purge_floor {
            let mut effective = self.engine.compute_purge_floor();
            if let Some(ref async_apply) = self.async_apply {
                let min_cursor = async_apply.min_worker_cursor();
                if min_cursor > 0 && (effective == 0 || min_cursor < effective) {
                    effective = min_cursor;
                }
            }
            if effective > 0 {
                floor.store(effective, Ordering::Release);
            }
        }
    }

    fn update_pin_ceiling(&self) {
        if let Some(ref la) = self.last_applied {
            if let Some(ref ceiling) = self.pin_ceiling {
                ceiling.store(la.index, Ordering::Release);
            }
            if let Some(ref prefetcher) = self.prefetcher {
                prefetcher.prefetch_next(la.index);
            }
        }
    }
}

impl RaftStateMachine<MqTypeConfig> for MqStateMachine {
    type SnapshotBuilder = MqSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::alias::LogIdOf<MqTypeConfig>>,
            openraft::alias::StoredMembershipOf<MqTypeConfig>,
        ),
        io::Error,
    > {
        // Always return (None, Default) — openraft replays the entire available
        // raft log on startup, rebuilding the in-memory MqEngine from scratch.
        Ok((None, openraft::StoredMembership::default()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<MqTypeConfig>, io::Error>,
            > + Unpin
            + OptionalSend,
    {
        let mut had_entries = false;

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            self.last_applied = Some(entry.log_id);
            had_entries = true;

            match &entry.payload {
                EntryPayload::Membership(membership) => {
                    self.last_membership =
                        openraft::StoredMembership::new(Some(entry.log_id), membership.clone());
                }
                EntryPayload::Normal(cmd) => {
                    // Release raft backlog budget for commands that were charged
                    // on submission (TAG_FORWARDED_BATCH, TAG_BATCH).
                    if let Some(ref backlog) = self.raft_backlog {
                        let tag = cmd.tag();
                        if tag == MqCommand::TAG_FORWARDED_BATCH || tag == MqCommand::TAG_BATCH {
                            backlog.release(cmd.total_encoded_size());
                        }
                    }
                }
                _ => {}
            }

            if let Some(r) = responder {
                r.send(MqApplyResponse {
                    log_index: entry.log_id.index,
                });
            }
        }

        if !had_entries {
            self.update_purge_floor();
            self.update_pin_ceiling();
            return Ok(());
        }

        if let Some(ref async_apply) = self.async_apply {
            async_apply.drain_purged_segments();
            if let Some(ref la) = self.last_applied {
                async_apply.advance_hwm(la.index);
            }
        }

        self.update_purge_floor();
        self.update_pin_ceiling();

        // Drain sealed segment indexes and write .sidx files on blocking pool.
        // The current segment_id is derived from the last applied entry's record location.
        if let Some(ref group_dir) = self.group_dir {
            let current_seg_id = self.last_applied.and_then(|la| {
                self.prefetcher.as_ref().and_then(|p| {
                    let loc = p.log_location(la.index)?;
                    Some(loc.segment_id as u32)
                })
            });

            let sealed = self.segment_indexes.take_sealed(current_seg_id);

            if !sealed.is_empty() {
                let dir = group_dir.clone();
                tokio::task::spawn_blocking(move || {
                    for (seg_id, idx) in sealed {
                        let path = dir.join(format!("{:020}.sidx", seg_id));
                        match std::fs::File::create(&path) {
                            Ok(mut file) => {
                                if let Err(e) = idx.write_to(&mut file) {
                                    warn!(
                                        segment_id = seg_id,
                                        error = %e,
                                        "failed to write .sidx file"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    segment_id = seg_id,
                                    error = %e,
                                    "failed to create .sidx file"
                                );
                            }
                        }
                    }
                });
            }
        }

        // Level 2 retention evaluation — periodically check if unpinned
        // segments can be permanently deleted based on retention policies.
        if let Some(ref evaluator) = self.retention_evaluator {
            // Update the active segment ID so it's never deleted.
            if let Some(ref la) = self.last_applied {
                if let Some(ref prefetcher) = self.prefetcher {
                    if let Some(loc) = prefetcher.log_location(la.index) {
                        evaluator.set_active_segment(loc.segment_id);
                    }
                }
            }

            if evaluator.should_evaluate() {
                let eval = Arc::clone(evaluator);
                tokio::task::spawn_blocking(move || {
                    eval.evaluate();
                });
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Barrier: ensure all workers have drained before snapshotting.
        if let Some(ref async_apply) = self.async_apply {
            if let Some(ref la) = self.last_applied {
                async_apply.barrier(la.index).await;
            }
        }

        let mut snapshot_data = self.engine.snapshot();

        // Include file manifest so followers can sync segment files.
        if let Some(ref group_dir) = self.group_dir {
            match bisque_raft::list_segment_files(group_dir) {
                Ok(entries) => {
                    snapshot_data.file_manifest = entries;
                }
                Err(e) => {
                    warn!(error = %e, "Failed to list segment files for snapshot");
                }
            }
        }
        snapshot_data.sync_addr = self.sync_addr.clone();

        MqSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_data,
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::alias::SnapshotMetaOf<MqTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        // Barrier: drain all workers before restoring snapshot.
        if let Some(ref async_apply) = self.async_apply {
            if let Some(ref la) = self.last_applied {
                async_apply.barrier(la.index).await;
            }
        }

        let data = snapshot.into_inner();
        let (snap, _): (MqSnapshotData, _) =
            bincode::serde::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        drop(data); // Free raw bytes — no longer needed.

        // Sync segment files from the leader before restoring engine state.
        if !snap.file_manifest.is_empty() {
            if let (Some(client), Some(sync_addr)) = (&self.sync_client, &snap.sync_addr) {
                info!(
                    files = snap.file_manifest.len(),
                    sync_addr = %sync_addr,
                    "Syncing segment files from leader"
                );
                match client.sync_files(sync_addr, &snap.file_manifest).await {
                    Ok(result) => {
                        info!(
                            transferred = result.files_transferred,
                            missing = result.files_missing,
                            bytes = result.bytes_transferred,
                            "Segment file sync complete"
                        );
                        if !result.verification_failures.is_empty() {
                            warn!(
                                failures = ?result.verification_failures,
                                "Segment file verification failures"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Segment file sync failed, continuing with snapshot");
                    }
                }
            } else {
                warn!(
                    files = snap.file_manifest.len(),
                    has_client = self.sync_client.is_some(),
                    has_addr = snap.sync_addr.is_some(),
                    "Cannot sync segment files: missing client or addr"
                );
            }
        }

        self.engine.restore(snap);
        self.update_purge_floor();
        self.segment_indexes.clear();

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        self.update_pin_ceiling();

        info!(
            last_applied = ?self.last_applied,
            "MQ snapshot installed"
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::alias::SnapshotOf<MqTypeConfig>>, io::Error> {
        let last_applied = match self.last_applied {
            Some(la) => la,
            None => return Ok(None),
        };

        let snap_data = self.engine.snapshot();

        let data = bincode::serde::encode_to_vec(&snap_data, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let snapshot_id = format!("mq-{}-{}", last_applied.leader_id.term, last_applied.index);

        Ok(Some(openraft::storage::Snapshot {
            meta: openraft::storage::SnapshotMeta {
                last_log_id: Some(last_applied),
                last_membership: self.last_membership.clone(),
                snapshot_id,
            },
            snapshot: Cursor::new(data),
        }))
    }
}

// =============================================================================
// Snapshot Builder
// =============================================================================

pub struct MqSnapshotBuilder {
    last_applied: Option<openraft::alias::LogIdOf<MqTypeConfig>>,
    last_membership: openraft::alias::StoredMembershipOf<MqTypeConfig>,
    snapshot_data: MqSnapshotData,
}

impl RaftSnapshotBuilder<MqTypeConfig> for MqSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::alias::SnapshotOf<MqTypeConfig>, io::Error> {
        let last_applied = self.last_applied.unwrap_or(LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 0,
                node_id: 0,
            },
            index: 0,
        });

        let data = bincode::serde::encode_to_vec(&self.snapshot_data, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let snapshot_id = format!("mq-{}-{}", last_applied.leader_id.term, last_applied.index);

        Ok(openraft::storage::Snapshot {
            meta: openraft::storage::SnapshotMeta {
                last_log_id: Some(last_applied),
                last_membership: self.last_membership.clone(),
                snapshot_id,
            },
            snapshot: Cursor::new(data),
        })
    }
}
