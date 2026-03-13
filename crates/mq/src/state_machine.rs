//! Raft state machine implementation for bisque-mq.

use std::io::{self, Cursor};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex as ParkingMutex;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use tracing::{info, warn};

use bisque_raft::{SegmentPrefetcher, SegmentSyncClient};

use crate::MqTypeConfig;
use crate::engine::MqEngine;
use crate::manifest::{GroupMeta, MqManifestManager, StructuralWrite};
use crate::metadata::MqMetadata;
use crate::segment_index::SegmentIndexMap;
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
    engine: MqEngine,
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
    /// Per-segment index builders — lives outside the engine for
    /// lock-free concurrent reads during applies.
    segment_indexes: Arc<SegmentIndexMap>,
    /// Shared with log storage — purged segment IDs are pushed here
    /// when segments are deleted. Drained after each apply batch.
    purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
}

impl MqStateMachine {
    pub fn new(engine: MqEngine) -> Self {
        Self {
            engine,
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
            segment_indexes: Arc::new(SegmentIndexMap::new()),
            purged_segments: None,
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

    pub fn with_purged_segments(mut self, purged: Arc<ParkingMutex<Vec<u64>>>) -> Self {
        self.purged_segments = Some(purged);
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

    /// Get a shared reference to the lock-free metadata for leader tasks and routers.
    pub fn shared_metadata(&self) -> Arc<MqMetadata> {
        self.engine.shared_metadata()
    }

    /// Build a snapshot of all engine state (delegates to `MqEngine::snapshot`).
    pub fn snapshot(&self) -> MqSnapshotData {
        self.engine.snapshot()
    }

    /// Get a shared reference to the segment index map.
    pub fn shared_segment_indexes(&self) -> Arc<SegmentIndexMap> {
        Arc::clone(&self.segment_indexes)
    }

    fn update_purge_floor(&mut self) {
        if let Some(ref floor) = self.purge_floor {
            let message_floor = self.engine.compute_purge_floor();
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

                    self.engine.restore(snapshot_data);
                    self.segment_indexes.clear();
                    info!(
                        group_id = self.group_id,
                        last_applied = ?self.last_applied,
                        "MQ state machine restored from installed snapshot"
                    );

                    return Ok((self.last_applied, membership));
                }
            }

            // No snapshot — check for structural state persisted by normal operation.
            // This loads entity metadata (topics, exchanges, consumer groups, sessions)
            // from MDBX and returns the structural_purge_floor so openraft replays
            // only entries from that point forward.
            if let Some(structural) = manifest.read_structural_state(self.group_id)? {
                if structural.structural_purge_floor > 0 {
                    self.structural_purge_floor = structural.structural_purge_floor;

                    // Convert StructuralState into MqSnapshotData for restore_structural.
                    let snap = MqSnapshotData {
                        topics: structural
                            .topics
                            .into_iter()
                            .map(|meta| crate::types::TopicSnapshot {
                                meta,
                                consumer_offsets: Vec::new(),
                            })
                            .collect(),
                        exchanges: structural
                            .exchanges
                            .into_iter()
                            .map(|meta| crate::types::ExchangeSnapshot {
                                meta,
                                bindings: Vec::new(),
                                retained: Vec::new(),
                            })
                            .collect(),
                        consumer_groups: structural
                            .consumer_groups
                            .into_iter()
                            .map(|meta| crate::consumer_group::ConsumerGroupSnapshot {
                                meta,
                                offsets: Vec::new(),
                                ack_state: None,
                                actor_state: None,
                            })
                            .collect(),
                        sessions: structural
                            .sessions
                            .into_iter()
                            .map(|meta| crate::types::SessionSnapshot { meta })
                            .collect(),
                        pending_wills: Vec::new(),
                        next_id: structural.next_id,
                        file_manifest: Vec::new(),
                        sync_addr: None,
                    };
                    self.engine.restore_structural(snap);
                    self.segment_indexes.clear();

                    // Populate retained messages from MDBX into exchange state.
                    // These were persisted during segment purge sweeps.
                    for (exchange_id, entries) in structural.retained {
                        if let Some(mut exchange) =
                            self.engine.metadata().exchanges.get_mut(&exchange_id)
                        {
                            for (key, msg_bytes) in entries {
                                exchange.retained.insert(
                                    key,
                                    crate::exchange::RetainedValue::heap(bytes::Bytes::from(
                                        msg_bytes,
                                    )),
                                );
                            }
                        }
                    }

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

                    // Look up the physical record location for segment index tracking
                    let record_location = self.prefetcher.as_ref().and_then(|p| {
                        let loc = p.log_location(log_index)?;
                        Some((loc.segment_id as u32, loc.offset as u32))
                    });

                    // Check if this is a structural command before applying
                    let structural_kind = classify_structural(&cmd);

                    let segment_id = record_location.map(|(seg, _)| seg as u64);
                    let response =
                        self.engine
                            .apply_command(&cmd, log_index, current_time, segment_id);

                    // Fire-and-forget structural writes to MDBX after apply
                    if let Some(ref manifest) = self.manifest {
                        if let Some(writes) = collect_structural_writes(
                            self.engine.metadata(),
                            &response,
                            structural_kind,
                        ) {
                            let next_id = self.engine.metadata().next_id.load(Ordering::Relaxed);
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

                    // Track segment index entries
                    if let Some(loc) = record_location {
                        self.segment_indexes.track_command(&cmd, loc);
                    }

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
        self.update_purge_floor();
        self.update_pin_ceiling();

        // Sweep retained messages referencing purged mmap segments.
        // Detach (copy to heap) so the segment can be freed, then batch-persist
        // the detached retained data to MDBX for fast recovery.
        if let Some(ref purged_segments) = self.purged_segments {
            let purged_ids: Vec<u64> = {
                let mut guard = purged_segments.lock();
                if guard.is_empty() {
                    Vec::new()
                } else {
                    std::mem::take(&mut *guard)
                }
            };

            if !purged_ids.is_empty() {
                let meta = self.engine.metadata();
                // Collect exchange IDs first to avoid holding DashMap refs across mutation
                let exchange_ids: Vec<u64> = meta.exchanges.iter().map(|e| *e.key()).collect();

                for exchange_id in exchange_ids {
                    if let Some(mut exchange) = meta.exchanges.get_mut(&exchange_id) {
                        let mut detached = false;
                        for rv in exchange.retained.values_mut() {
                            if let Some(seg_id) = rv.segment_id {
                                if purged_ids.contains(&seg_id) {
                                    rv.detach();
                                    detached = true;
                                }
                            }
                        }

                        // Batch-persist all retained messages for this exchange
                        if detached {
                            if let Some(ref manifest) = self.manifest {
                                let entries: Vec<(String, Vec<u8>)> = exchange
                                    .retained
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.message.to_vec()))
                                    .collect();
                                manifest.persist_retained_fire_and_forget(
                                    self.group_id,
                                    exchange_id,
                                    entries,
                                );
                            }
                        }
                    }
                }
            }
        }

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
                // Fire segment range updates to MDBX before writing .sidx files
                if let Some(ref manifest) = self.manifest {
                    for (seg_id, idx) in &sealed {
                        let summaries = idx.entity_summaries();
                        if !summaries.is_empty() {
                            manifest.sealed_segment_fire_and_forget(
                                self.group_id,
                                *seg_id as u64,
                                summaries,
                            );
                        }
                    }
                }

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

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
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

        self.engine.restore(snap);
        self.update_purge_floor();
        self.segment_indexes.clear();

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

        let snap_data = self.engine.snapshot();

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
    CreateExchange,
    DeleteExchange(u64),
    CreateConsumerGroup,
    DeleteConsumerGroup(u64),
    CreateSession,
    SetRetained {
        exchange_id: u64,
        routing_key: String,
        message: Vec<u8>,
    },
    DeleteRetained {
        exchange_id: u64,
        routing_key: String,
    },
    Batch(Vec<StructuralKind>),
}

fn classify_structural(cmd: &MqCommand) -> StructuralKind {
    match cmd.tag() {
        MqCommand::TAG_CREATE_TOPIC => StructuralKind::CreateTopic,
        MqCommand::TAG_DELETE_TOPIC => StructuralKind::DeleteTopic(cmd.field_u64(8)),
        MqCommand::TAG_CREATE_EXCHANGE => StructuralKind::CreateExchange,
        MqCommand::TAG_DELETE_EXCHANGE => StructuralKind::DeleteExchange(cmd.field_u64(8)),
        MqCommand::TAG_CREATE_CONSUMER_GROUP => StructuralKind::CreateConsumerGroup,
        MqCommand::TAG_DELETE_CONSUMER_GROUP => {
            StructuralKind::DeleteConsumerGroup(cmd.field_u64(8))
        }
        MqCommand::TAG_CREATE_SESSION => StructuralKind::CreateSession,
        MqCommand::TAG_SET_RETAINED => {
            let v = cmd.as_set_retained();
            StructuralKind::SetRetained {
                exchange_id: v.exchange_id(),
                routing_key: v.routing_key().to_owned(),
                message: v.message().to_vec(),
            }
        }
        MqCommand::TAG_DELETE_RETAINED => {
            let v = cmd.as_delete_retained();
            StructuralKind::DeleteRetained {
                exchange_id: v.exchange_id(),
                routing_key: v.routing_key().to_owned(),
            }
        }
        MqCommand::TAG_BATCH => {
            let batch = cmd.as_batch();
            let kinds: Vec<StructuralKind> =
                batch.commands().map(|c| classify_structural(&c)).collect();
            if kinds.iter().all(|k| matches!(k, StructuralKind::None)) {
                StructuralKind::None
            } else {
                StructuralKind::Batch(kinds)
            }
        }
        _ => StructuralKind::None,
    }
}

/// After apply_command, collect the structural writes to send to MDBX.
/// For creates, reads the newly-created entity metadata from the metadata store.
/// For deletes, uses the ID captured before apply.
fn collect_structural_writes(
    meta: &MqMetadata,
    response: &MqResponse,
    kind: StructuralKind,
) -> Option<Vec<StructuralWrite>> {
    match kind {
        StructuralKind::None => None,
        StructuralKind::CreateTopic => {
            if let MqResponse::EntityCreated { id } = response {
                meta.topics
                    .get(id)
                    .map(|t| vec![StructuralWrite::CreateTopic(t.snapshot_meta())])
            } else {
                None
            }
        }
        StructuralKind::DeleteTopic(id) => Some(vec![StructuralWrite::DeleteTopic(id)]),
        StructuralKind::CreateExchange => {
            if let MqResponse::EntityCreated { id } = response {
                meta.exchanges
                    .get(id)
                    .map(|e| vec![StructuralWrite::CreateExchange(e.meta.clone())])
            } else {
                None
            }
        }
        StructuralKind::DeleteExchange(id) => Some(vec![StructuralWrite::DeleteExchange(id)]),
        StructuralKind::CreateConsumerGroup => {
            if let MqResponse::EntityCreated { id } = response {
                meta.consumer_groups
                    .get(id)
                    .map(|g| vec![StructuralWrite::CreateConsumerGroup(g.snapshot_meta())])
            } else {
                None
            }
        }
        StructuralKind::DeleteConsumerGroup(id) => {
            Some(vec![StructuralWrite::DeleteConsumerGroup(id)])
        }
        StructuralKind::CreateSession => {
            if let MqResponse::EntityCreated { id } = response {
                meta.sessions
                    .get(id)
                    .map(|s| vec![StructuralWrite::CreateSession(s.snapshot_meta())])
            } else {
                None
            }
        }
        StructuralKind::SetRetained {
            exchange_id,
            routing_key,
            message,
        } => {
            if matches!(response, MqResponse::Ok) {
                Some(vec![StructuralWrite::SetRetained {
                    exchange_id,
                    routing_key,
                    message,
                }])
            } else {
                None
            }
        }
        StructuralKind::DeleteRetained {
            exchange_id,
            routing_key,
        } => {
            if matches!(response, MqResponse::Ok) {
                Some(vec![StructuralWrite::DeleteRetained {
                    exchange_id,
                    routing_key,
                }])
            } else {
                None
            }
        }
        StructuralKind::Batch(kinds) => {
            let responses = if let MqResponse::BatchResponse(resps) = response {
                resps.as_slice()
            } else {
                return None;
            };
            let mut writes = Vec::new();
            for (kind, resp) in kinds.into_iter().zip(responses.iter()) {
                if let Some(w) = collect_structural_writes(meta, resp, kind) {
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
