//! Raft state machine implementation for bisque-mq.

use std::io::{self, Cursor};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use parking_lot::RwLock;
use tracing::{info, warn};

use bisque_raft::{SegmentPrefetcher, SegmentSyncClient};

use crate::MqTypeConfig;
use crate::engine::MqEngine;
use crate::manifest::{GroupMeta, MqManifestManager, StructuralWrite};
use crate::types::{MqCommand, MqResponse, MqSnapshotData};

/// Raft state machine that drives the MQ engine.
///
/// On normal startup the state machine returns `(None, Default)` from
/// `applied_state()`, forcing openraft to replay the entire available raft
/// log. This rebuilds the in-memory `MqEngine` from scratch — no periodic
/// MDBX snapshots are needed.
///
/// The MDBX manifest is only used when a snapshot is *installed* from the
/// leader (new/lagging nodes). In that case the snapshot is persisted so
/// that `applied_state()` can load it on the next restart and replay only
/// the entries after the snapshot point.
pub struct MqStateMachine {
    engine: Arc<RwLock<MqEngine>>,
    last_applied: Option<LogId<MqTypeConfig>>,
    last_membership: StoredMembership<MqTypeConfig>,
    purge_floor: Option<Arc<AtomicU64>>,
    pin_ceiling: Option<Arc<AtomicU64>>,
    prefetcher: Option<SegmentPrefetcher>,
    manifest: Option<Arc<MqManifestManager>>,
    group_id: u64,
    /// Tracks the structural purge floor loaded from MDBX.
    /// Used in combination with the message purge floor to determine
    /// the actual raft log purge point.
    structural_purge_floor: u64,
    /// Client for syncing segment files from the leader during snapshot install.
    sync_client: Option<SegmentSyncClient>,
    /// Address of this node's segment sync server (included in outgoing snapshots).
    sync_addr: Option<String>,
    /// Group directory path for listing segment files in snapshot builder.
    group_dir: Option<std::path::PathBuf>,
}

impl MqStateMachine {
    pub fn new(engine: MqEngine) -> Self {
        Self {
            engine: Arc::new(RwLock::new(engine)),
            last_applied: None,
            last_membership: StoredMembership::default(),
            purge_floor: None,
            pin_ceiling: None,
            prefetcher: None,
            manifest: None,
            group_id: 0,
            structural_purge_floor: 0,
            sync_client: None,
            sync_addr: None,
            group_dir: None,
        }
    }

    pub fn with_purge_floor(mut self, floor: Arc<AtomicU64>) -> Self {
        self.purge_floor = Some(floor);
        self
    }

    pub fn with_pin_ceiling(mut self, ceiling: Arc<AtomicU64>) -> Self {
        self.pin_ceiling = Some(ceiling);
        self
    }

    pub fn with_prefetcher(mut self, prefetcher: SegmentPrefetcher) -> Self {
        self.prefetcher = Some(prefetcher);
        self
    }

    pub fn with_manifest(mut self, manifest: Arc<MqManifestManager>, group_id: u64) -> Self {
        self.manifest = Some(manifest);
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

    /// Get a shared reference to the engine for use by leader tasks.
    pub fn shared_engine(&self) -> Arc<RwLock<MqEngine>> {
        Arc::clone(&self.engine)
    }

    fn update_purge_floor(&self, engine: &mut MqEngine) {
        if let Some(ref floor) = self.purge_floor {
            let message_floor = engine.compute_purge_floor();
            // Actual purge point is min of message floor and structural floor.
            // We can't purge past either: structural commands before structural_purge_floor
            // may not be in MDBX yet, and messages before message_floor are still referenced.
            let effective = if self.structural_purge_floor > 0 && message_floor > 0 {
                message_floor.min(self.structural_purge_floor)
            } else if message_floor > 0 {
                message_floor
            } else if self.structural_purge_floor > 0 {
                self.structural_purge_floor
            } else {
                0
            };
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
    ) -> Result<(Option<LogId<MqTypeConfig>>, StoredMembership<MqTypeConfig>), io::Error> {
        if let Some(ref manifest) = self.manifest {
            // First check for a previously installed snapshot (new/lagging node).
            if let Some(snapshot_data) = manifest.read_snapshot_data(self.group_id)? {
                if let Some((last_applied, membership)) =
                    manifest.read_applied_state(self.group_id)?
                {
                    self.last_applied = last_applied;
                    self.last_membership = membership.clone();

                    let mut engine = self.engine.write();
                    engine.restore(snapshot_data);
                    info!(
                        group_id = self.group_id,
                        last_applied = ?self.last_applied,
                        "MQ state machine restored from installed snapshot"
                    );

                    return Ok((self.last_applied, membership));
                }
            }

            // No snapshot — check for structural state persisted by normal operation.
            // This loads entity metadata (topics, queues, actors, jobs) from MDBX
            // and returns the structural_purge_floor so openraft replays only
            // entries from that point forward.
            if let Some(structural) = manifest.read_structural_state(self.group_id)? {
                if structural.structural_purge_floor > 0 {
                    self.structural_purge_floor = structural.structural_purge_floor;

                    let mut engine = self.engine.write();
                    engine.restore_structural(structural);

                    // Return a synthetic last_applied at structural_purge_floor
                    // so openraft replays from there.
                    let last_applied = LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 0,
                            node_id: 0,
                        },
                        index: self.structural_purge_floor,
                    };
                    self.last_applied = Some(last_applied);

                    info!(
                        group_id = self.group_id,
                        structural_purge_floor = self.structural_purge_floor,
                        "MQ state machine restored structural state from MDBX"
                    );

                    return Ok((self.last_applied, StoredMembership::default()));
                }
            }
        }

        Ok((None, StoredMembership::default()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<MqTypeConfig>, io::Error>,
            > + Unpin
            + OptionalSend,
    {
        // Hoist timestamp outside the loop — sub-millisecond drift is acceptable.
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => MqResponse::Ok,
                EntryPayload::Normal(cmd) => {
                    let log_index = entry.log_id.index;

                    // Check if this is a structural command before applying
                    let structural_kind = classify_structural(&cmd);

                    let mut engine = self.engine.write();
                    let response = engine.apply_command(cmd, log_index, current_time);

                    // Fire-and-forget structural writes to MDBX after apply
                    if let Some(ref manifest) = self.manifest {
                        if let Some(writes) =
                            collect_structural_writes(&engine, &response, structural_kind)
                        {
                            let next_id = engine.next_id;
                            for w in writes {
                                manifest.structural_update_fire_and_forget(
                                    self.group_id,
                                    log_index,
                                    next_id,
                                    w,
                                );
                            }
                        }
                    }
                    drop(engine);

                    response
                }
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    MqResponse::Ok
                }
            };

            if let Some(r) = responder {
                r.send(response);
            }
        }

        // Update purge floor and pin ceiling
        {
            let mut engine = self.engine.write();
            self.update_purge_floor(&mut engine);
        }
        self.update_pin_ceiling();

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let engine = self.engine.read();
        let mut snapshot_data = engine.snapshot();

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
        meta: &SnapshotMeta<MqTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
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

        // Clone snapshot for MDBX persistence before engine.restore() consumes it.
        // Bytes fields are refcounted so this clone is cheap.
        let snap_for_mdbx = snap.clone();

        // Single write lock acquisition for restore + purge floor.
        {
            let mut engine = self.engine.write();
            engine.restore(snap);
            self.update_purge_floor(&mut engine);
        }

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        // Persist to MDBX as individual entity records (not one big blob).
        if let Some(ref manifest) = self.manifest {
            let group_meta = GroupMeta::from_raft(&self.last_applied, &self.last_membership);
            manifest
                .install_snapshot(self.group_id, group_meta, snap_for_mdbx)
                .await?;
        }

        self.update_pin_ceiling();

        info!(
            last_applied = ?self.last_applied,
            "MQ snapshot installed"
        );
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<MqTypeConfig>>, io::Error> {
        let last_applied = match self.last_applied {
            Some(la) => la,
            None => return Ok(None),
        };

        let engine = self.engine.read();
        let snap_data = engine.snapshot();
        drop(engine);

        let data = bincode::serde::encode_to_vec(&snap_data, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let snapshot_id = format!("mq-{}-{}", last_applied.leader_id.term, last_applied.index);

        Ok(Some(Snapshot {
            meta: SnapshotMeta {
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
    last_applied: Option<LogId<MqTypeConfig>>,
    last_membership: StoredMembership<MqTypeConfig>,
    snapshot_data: MqSnapshotData,
}

impl RaftSnapshotBuilder<MqTypeConfig> for MqSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MqTypeConfig>, io::Error> {
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

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: Some(last_applied),
                last_membership: self.last_membership.clone(),
                snapshot_id,
            },
            snapshot: Cursor::new(data),
        })
    }
}

// =============================================================================
// Structural command classification
// =============================================================================

/// Identifies what kind of structural command this is (if any).
#[derive(Debug, Clone)]
enum StructuralKind {
    None,
    CreateTopic,
    DeleteTopic(u64),
    CreateQueue,
    DeleteQueue(u64),
    CreateActorNamespace,
    DeleteActorNamespace(u64),
    CreateJob,
    DeleteJob(u64),
    Batch(Vec<StructuralKind>),
}

fn classify_structural(cmd: &MqCommand) -> StructuralKind {
    match cmd {
        MqCommand::CreateTopic { .. } => StructuralKind::CreateTopic,
        MqCommand::DeleteTopic { topic_id } => StructuralKind::DeleteTopic(*topic_id),
        MqCommand::CreateQueue { .. } => StructuralKind::CreateQueue,
        MqCommand::DeleteQueue { queue_id } => StructuralKind::DeleteQueue(*queue_id),
        MqCommand::CreateActorNamespace { .. } => StructuralKind::CreateActorNamespace,
        MqCommand::DeleteActorNamespace { namespace_id } => {
            StructuralKind::DeleteActorNamespace(*namespace_id)
        }
        MqCommand::CreateJob { .. } => StructuralKind::CreateJob,
        MqCommand::DeleteJob { job_id } => StructuralKind::DeleteJob(*job_id),
        MqCommand::Batch(cmds) => {
            // Quick check: skip Vec allocation if no sub-command is structural.
            if cmds
                .iter()
                .all(|c| matches!(classify_structural(c), StructuralKind::None))
            {
                StructuralKind::None
            } else {
                StructuralKind::Batch(cmds.iter().map(classify_structural).collect())
            }
        }
        _ => StructuralKind::None,
    }
}

/// After apply_command, collect the structural writes to send to MDBX.
/// For creates, reads the newly-created entity metadata from the engine.
/// For deletes, uses the ID captured before apply.
fn collect_structural_writes(
    engine: &MqEngine,
    response: &MqResponse,
    kind: StructuralKind,
) -> Option<Vec<StructuralWrite>> {
    match kind {
        StructuralKind::None => None,
        StructuralKind::CreateTopic => {
            if let MqResponse::EntityCreated { id } = response {
                engine
                    .topics
                    .get(id)
                    .map(|t| vec![StructuralWrite::CreateTopic(t.meta.clone())])
            } else {
                None
            }
        }
        StructuralKind::DeleteTopic(id) => Some(vec![StructuralWrite::DeleteTopic(id)]),
        StructuralKind::CreateQueue => {
            if let MqResponse::EntityCreated { id } = response {
                engine
                    .queues
                    .get(id)
                    .map(|q| vec![StructuralWrite::CreateQueue(q.meta.clone())])
            } else {
                None
            }
        }
        StructuralKind::DeleteQueue(id) => Some(vec![StructuralWrite::DeleteQueue(id)]),
        StructuralKind::CreateActorNamespace => {
            if let MqResponse::EntityCreated { id } = response {
                engine
                    .actor_namespaces
                    .get(id)
                    .map(|ns| vec![StructuralWrite::CreateActorNamespace(ns.meta.clone())])
            } else {
                None
            }
        }
        StructuralKind::DeleteActorNamespace(id) => {
            Some(vec![StructuralWrite::DeleteActorNamespace(id)])
        }
        StructuralKind::CreateJob => {
            if let MqResponse::EntityCreated { id } = response {
                engine
                    .jobs
                    .get(id)
                    .map(|j| vec![StructuralWrite::CreateJob(j.meta.clone())])
            } else {
                None
            }
        }
        StructuralKind::DeleteJob(id) => Some(vec![StructuralWrite::DeleteJob(id)]),
        StructuralKind::Batch(kinds) => {
            let responses = if let MqResponse::BatchResponse(resps) = response {
                resps.as_slice()
            } else {
                return None;
            };
            let mut writes = Vec::new();
            for (kind, resp) in kinds.into_iter().zip(responses.iter()) {
                if let Some(w) = collect_structural_writes(engine, resp, kind) {
                    writes.extend(w);
                }
            }
            if writes.is_empty() {
                None
            } else {
                Some(writes)
            }
        }
    }
}
