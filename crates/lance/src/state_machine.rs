//! Raft state machine implementation for bisque-lance.
//!
//! `LanceStateMachine` implements openraft's `RaftStateMachine` trait,
//! dispatching applied log entries to the appropriate `TableEngine` within
//! the multi-table `BisqueLance` engine.

use std::io::{self, Cursor};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use tracing::{debug, error, info, warn};

use crate::LanceTypeConfig;
use crate::async_apply::{AppliedWatermark, AsyncApplyBuffer, AsyncApplyConfig};
use crate::catalog_events::{CatalogEventBus, CatalogEventKind};
use crate::engine::{BisqueLance, SnapshotGuardHandle};
use crate::ipc;
use crate::manifest::{
    GroupMeta, LanceManifestManager, ManifestCommand, ManifestUpdate, TableUpdate,
};
use crate::segment_sync::SegmentSyncClient;
use crate::types::{LanceCommand, LanceResponse, PersistedTableEntry, SnapshotData};

/// Raft state machine that drives the BisqueLance multi-table storage engine.
pub struct LanceStateMachine {
    engine: Arc<BisqueLance>,
    last_applied: Option<LogId<LanceTypeConfig>>,
    last_membership: StoredMembership<LanceTypeConfig>,
    /// Optional async apply buffer for decoupling Lance I/O from apply.
    async_buffer: Option<AsyncApplyBuffer>,
    /// Shared purge floor with the log storage. When set, the state machine
    /// updates this to prevent log purging below the min safe log index.
    purge_floor: Option<Arc<AtomicU64>>,
    /// Optional MDBX manifest for crash-consistent metadata persistence.
    manifest: Option<Arc<LanceManifestManager>>,
    /// Raft group ID for manifest key scoping.
    group_id: u64,
    /// Optional catalog event bus for real-time push notifications.
    catalog_events: Option<Arc<CatalogEventBus>>,
    /// Catalog name (raft group name) used to tag catalog events.
    catalog_name: String,
    /// Optional version pin tracker for Raft-replicated version pins.
    version_pins: Option<Arc<crate::version_pins::VersionPinTracker>>,
    /// Client for streaming segment files from leader during snapshot install.
    sync_client: Option<SegmentSyncClient>,
    /// Address of this node's segment sync server (included in snapshots so
    /// followers know where to connect).
    sync_addr: Option<String>,
    /// Active snapshot transfer guards. Each handle keeps file deletions
    /// deferred until it is dropped (via timeout or explicit release).
    /// Supports multiple concurrent transfers (e.g., multiple nodes joining).
    snapshot_guards: Vec<SnapshotGuardHandle>,
}

impl LanceStateMachine {
    pub fn new(engine: Arc<BisqueLance>) -> Self {
        Self {
            engine,
            last_applied: None,
            last_membership: StoredMembership::default(),
            async_buffer: None,
            purge_floor: None,
            manifest: None,
            group_id: 0,
            catalog_events: None,
            catalog_name: String::new(),
            version_pins: None,
            sync_client: None,
            sync_addr: None,
            snapshot_guards: Vec::new(),
        }
    }

    /// Set the catalog event bus for real-time push notifications.
    pub fn with_catalog_events(mut self, bus: Arc<CatalogEventBus>) -> Self {
        self.catalog_events = Some(bus);
        self
    }

    /// Set the catalog name used to tag catalog events.
    pub fn with_catalog_name(mut self, name: String) -> Self {
        self.catalog_name = name;
        self
    }

    /// Set the version pin tracker for Raft-replicated version pins.
    pub fn with_version_pins(mut self, pins: Arc<crate::version_pins::VersionPinTracker>) -> Self {
        self.version_pins = Some(pins);
        self
    }

    /// Set the segment sync client for file transfer during snapshot install.
    pub fn with_sync_client(mut self, client: SegmentSyncClient) -> Self {
        self.sync_client = Some(client);
        self
    }

    /// Set this node's segment sync server address (included in snapshots).
    pub fn with_sync_addr(mut self, addr: String) -> Self {
        self.sync_addr = Some(addr);
        self
    }

    /// Set the purge floor handle shared with the log storage.
    /// Must be called before the Raft node starts processing.
    pub fn with_purge_floor(mut self, floor: Arc<AtomicU64>) -> Self {
        self.purge_floor = Some(floor);
        self
    }

    /// Set the MDBX manifest for crash-consistent metadata persistence.
    ///
    /// When set, `applied_state()` reads from MDBX on startup, and
    /// `apply()` sends metadata updates to the manifest worker thread.
    pub fn with_manifest(mut self, manifest: Arc<LanceManifestManager>, group_id: u64) -> Self {
        self.manifest = Some(manifest);
        self.group_id = group_id;
        self
    }

    /// Create a new state machine with async apply enabled.
    ///
    /// Returns the state machine and a clonable [`AppliedWatermark`] handle
    /// that query layers can use for read-after-write fencing.
    pub fn with_async_apply(
        engine: Arc<BisqueLance>,
        config: AsyncApplyConfig,
    ) -> (Self, AppliedWatermark) {
        let (async_buffer, watermark) = AsyncApplyBuffer::new(engine.clone(), config);
        let sm = Self {
            engine,
            last_applied: None,
            last_membership: StoredMembership::default(),
            async_buffer: Some(async_buffer),
            purge_floor: None,
            manifest: None,
            group_id: 0,
            catalog_events: None,
            catalog_name: String::new(),
            version_pins: None,
            sync_client: None,
            sync_addr: None,
            snapshot_guards: Vec::new(),
        };
        (sm, watermark)
    }

    /// Access the underlying engine.
    pub fn engine(&self) -> &Arc<BisqueLance> {
        &self.engine
    }

    /// Access the catalog event bus (if configured).
    pub fn catalog_events(&self) -> Option<&Arc<CatalogEventBus>> {
        self.catalog_events.as_ref()
    }
}

impl RaftStateMachine<LanceTypeConfig> for LanceStateMachine {
    type SnapshotBuilder = LanceSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<LanceTypeConfig>>,
            StoredMembership<LanceTypeConfig>,
        ),
        io::Error,
    > {
        // On startup, try reading crash-consistent state from MDBX manifest.
        if let Some(manifest) = &self.manifest {
            if let Ok(Some((last_applied, membership))) = manifest.read_group_meta(self.group_id) {
                self.last_applied = last_applied.clone();
                self.last_membership = membership.clone();

                // Restore all tables from persisted entries (full config + state).
                if let Ok(entries) = manifest.read_all_tables(self.group_id) {
                    if !entries.is_empty() {
                        if let Err(e) = self.engine.restore_from_persisted_entries(entries).await {
                            error!("Failed to restore tables from MDBX manifest: {}", e);
                        }
                    }
                }

                info!(?last_applied, "Recovered applied_state from MDBX manifest");
                return Ok((last_applied, membership));
            }
        }

        // Fallback: in-memory state (first boot or no manifest).
        let last_applied = if let Some(buf) = &self.async_buffer {
            // Conservative watermark: only report what Lance has fully written.
            buf.lance_applied_log_id()
        } else {
            self.last_applied.clone()
        };
        Ok((last_applied, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<LanceTypeConfig>, io::Error>,
            > + Unpin
            + OptionalSend,
    {
        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            self.last_applied = Some(entry.log_id.clone());

            // Track what table update to send to the manifest (if any).
            let mut manifest_table_update: Option<TableUpdate> = None;
            // Catalog events to emit after successful apply.
            let mut catalog_event: Option<CatalogEventKind> = None;

            let response = match entry.payload {
                EntryPayload::Blank => LanceResponse::Ok,
                EntryPayload::Normal(cmd) => {
                    match cmd {
                        LanceCommand::AppendRecords { table_name, data } => {
                            // Skip entries whose data has already been promoted to
                            // deep storage. During recovery replay, per-table
                            // min_safe_log_index tells us the earliest log entry
                            // still in hot/warm storage — anything before it is
                            // already in S3 and must not be re-applied.
                            let skip = self
                                .engine
                                .require_table(&table_name)
                                .ok()
                                .and_then(|t| {
                                    t.min_safe_log_index()
                                        .map(|min_idx| entry.log_id.index < min_idx)
                                })
                                .unwrap_or(false);

                            if skip {
                                debug!(
                                    table = %table_name,
                                    log_index = entry.log_id.index,
                                    "Skipping AppendRecords: data already in deep storage"
                                );
                                LanceResponse::Ok
                            } else {
                                // Track the first log index written to the active segment.
                                // Only update manifest when the value changes (first write).
                                let changed = self
                                    .engine
                                    .require_table(&table_name)
                                    .ok()
                                    .map(|t| t.record_log_index(entry.log_id.index))
                                    .unwrap_or(false);

                                if changed {
                                    if let Ok(table) = self.engine.require_table(&table_name) {
                                        manifest_table_update = Some(TableUpdate::Set {
                                            table_name: table_name.clone(),
                                            entry: LanceManifestManager::build_table_entry(&table),
                                        });
                                        catalog_event =
                                            Some(CatalogEventKind::ActiveVersionBumped {
                                                table: table_name.clone(),
                                                version: table.catalog().active_segment,
                                            });
                                    }
                                }

                                if let Some(buf) = &self.async_buffer {
                                    // Async path: enqueue for background processing.
                                    buf.enqueue(entry.log_id.clone(), table_name, data).await;
                                    LanceResponse::Ok
                                } else {
                                    // Sync path (original behavior).
                                    self.apply_command(LanceCommand::AppendRecords {
                                        table_name,
                                        data,
                                    })
                                    .await
                                }
                            }
                        }
                        LanceCommand::SealActiveSegment {
                            ref table_name,
                            ref sealed_segment_id,
                            ref new_active_segment_id,
                            ..
                        } => {
                            let tname = table_name.clone();
                            let sealed_id = *sealed_segment_id;
                            let new_active_id = *new_active_segment_id;
                            // Barrier: drain pending writes for this table before proceeding.
                            if let Some(buf) = &self.async_buffer {
                                buf.drain_table(&tname).await;
                            }
                            let response = self.apply_command(cmd).await;

                            if let Ok(table) = self.engine.require_table(&tname) {
                                manifest_table_update = Some(TableUpdate::Set {
                                    table_name: tname.clone(),
                                    entry: LanceManifestManager::build_table_entry(&table),
                                });
                                if matches!(response, LanceResponse::Ok) {
                                    catalog_event = Some(CatalogEventKind::SegmentSealed {
                                        table: tname,
                                        active_version: new_active_id,
                                        sealed_version: sealed_id,
                                    });
                                }
                            }

                            response
                        }
                        LanceCommand::DropTable { ref table_name } => {
                            let tname = table_name.clone();
                            if let Some(buf) = &self.async_buffer {
                                buf.drain_table(&tname).await;
                            }
                            let response = self.apply_command(cmd).await;

                            manifest_table_update = Some(TableUpdate::Remove {
                                table_name: tname.clone(),
                            });
                            if matches!(response, LanceResponse::Ok) {
                                catalog_event =
                                    Some(CatalogEventKind::TableDropped { table: tname });
                            }

                            response
                        }
                        LanceCommand::PromoteToDeepStorage {
                            ref table_name,
                            ref s3_manifest_version,
                            ..
                        } => {
                            let tname = table_name.clone();
                            let s3_ver = *s3_manifest_version;
                            let response = self.apply_command(cmd).await;
                            // Eagerly update purge floor since promote relaxes the constraint.
                            self.update_purge_floor();

                            if let Ok(table) = self.engine.require_table(&tname) {
                                manifest_table_update = Some(TableUpdate::Set {
                                    table_name: tname.clone(),
                                    entry: LanceManifestManager::build_table_entry(&table),
                                });
                                if matches!(response, LanceResponse::Ok) {
                                    catalog_event = Some(CatalogEventKind::SegmentPromoted {
                                        table: tname,
                                        s3_manifest_version: s3_ver,
                                    });
                                }
                            }

                            response
                        }
                        LanceCommand::DeleteRecords { ref table_name, .. } => {
                            let tname = table_name.clone();
                            // Drain pending writes for this table before deleting.
                            if let Some(buf) = &self.async_buffer {
                                buf.drain_table(&tname).await;
                            }
                            let response = self.apply_command(cmd).await;

                            if let Ok(table) = self.engine.require_table(&tname) {
                                manifest_table_update = Some(TableUpdate::Set {
                                    table_name: tname.clone(),
                                    entry: LanceManifestManager::build_table_entry(&table),
                                });
                                if matches!(response, LanceResponse::RowsAffected(_)) {
                                    let (av, sv, s3v) = table.tier_versions();
                                    catalog_event =
                                        Some(CatalogEventKind::DataMutated {
                                            table: tname,
                                            active_version: av,
                                            sealed_version: sv,
                                            s3_version: s3v,
                                        });
                                }
                            }
                            response
                        }
                        LanceCommand::UpdateRecords { ref table_name, .. } => {
                            let tname = table_name.clone();
                            // Drain pending writes for this table before updating.
                            if let Some(buf) = &self.async_buffer {
                                buf.drain_table(&tname).await;
                            }
                            let response = self.apply_command(cmd).await;

                            if let Ok(table) = self.engine.require_table(&tname) {
                                manifest_table_update = Some(TableUpdate::Set {
                                    table_name: tname.clone(),
                                    entry: LanceManifestManager::build_table_entry(&table),
                                });
                                if matches!(response, LanceResponse::RowsAffected(_)) {
                                    let (av, sv, s3v) = table.tier_versions();
                                    catalog_event =
                                        Some(CatalogEventKind::DataMutated {
                                            table: tname,
                                            active_version: av,
                                            sealed_version: sv,
                                            s3_version: s3v,
                                        });
                                }
                            }
                            response
                        }
                        other => {
                            // Extract table_name + schema_ipc before moving the command.
                            let (tname, schema_ipc) = match &other {
                                LanceCommand::CreateTable {
                                    table_name,
                                    schema_ipc,
                                } => (Some(table_name.clone()), Some(schema_ipc.clone())),
                                LanceCommand::BeginFlush { table_name, .. } => {
                                    (Some(table_name.clone()), None)
                                }
                                _ => (None, None),
                            };

                            let response = self.apply_command(other).await;

                            // Update manifest with table state after CreateTable / BeginFlush.
                            if let Some(tname) = tname {
                                if let Ok(table) = self.engine.require_table(&tname) {
                                    manifest_table_update = Some(TableUpdate::Set {
                                        table_name: tname.clone(),
                                        entry: LanceManifestManager::build_table_entry(&table),
                                    });
                                    // Emit TableCreated event for CreateTable commands.
                                    if let Some(schema_ipc) = schema_ipc {
                                        if matches!(response, LanceResponse::Ok) {
                                            catalog_event = Some(CatalogEventKind::TableCreated {
                                                table: tname,
                                                schema_ipc: schema_ipc.to_vec(),
                                            });
                                        }
                                    }
                                }
                            }

                            response
                        }
                    }
                }
                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id.clone()), m);
                    LanceResponse::Ok
                }
            };

            // Emit catalog event + persist to WAL.
            if let Some(event_kind) = catalog_event {
                if let Some(bus) = &self.catalog_events {
                    let event = bus.publish(self.catalog_name.clone(), event_kind);
                    // Persist to MDBX WAL (fire-and-forget).
                    if let Some(manifest) = &self.manifest {
                        manifest
                            .send_update(ManifestCommand::AppendWalEvents {
                                group_id: self.group_id,
                                events: vec![event],
                            })
                            .await;
                    }
                }
            }

            // Send manifest update (fire-and-forget).
            if let Some(manifest) = &self.manifest {
                let update = LanceManifestManager::build_apply_update(
                    self.group_id,
                    &self.last_applied,
                    &self.last_membership,
                    manifest_table_update,
                );
                manifest.send_update(update).await;
            }

            if let Some(r) = responder {
                r.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Drain async buffer to ensure Lance state is caught up before snapshot.
        if let Some(buf) = &self.async_buffer {
            buf.drain_all().await;
        }

        // Flush manifest durably before building snapshot so MDBX is consistent.
        if let Some(manifest) = &self.manifest {
            let update = LanceManifestManager::build_apply_update(
                self.group_id,
                &self.last_applied,
                &self.last_membership,
                None,
            );
            if let Err(e) = manifest.send_update_durable(update).await {
                warn!("Failed to flush manifest before snapshot build: {}", e);
            }
        }

        // Pin purge floor to last_applied (watermark) so logs from this point
        // are retained while a fresh node syncs files and catches up.
        let watermark = self.last_applied.as_ref().map(|l| l.index);
        self.update_purge_floor_with(watermark);
        if let Some(idx) = watermark {
            info!(
                watermark = idx,
                "Pinned purge floor for snapshot (protects logs during file sync)"
            );
        }

        // Build file manifest by walking all segment directories.
        let file_manifest = match self.engine.config().build_file_manifest().await {
            Ok(manifest) => {
                let total_bytes: u64 = manifest.iter().map(|e| e.size).sum();
                info!(
                    files = manifest.len(),
                    total_bytes, "Built snapshot file manifest"
                );
                manifest
            }
            Err(e) => {
                warn!("Failed to build file manifest: {}", e);
                Vec::new()
            }
        };

        // Acquire snapshot transfer guard to prevent file deletion until
        // the handle is dropped (or the 10-minute timeout fires).
        // The handle is stored on the state machine and dropped when the
        // timeout expires. Multiple guards can be active concurrently
        // for parallel node joins.
        //
        // The watermark is passed to the guard so that `min_watermark()`
        // prevents `update_purge_floor()` from raising the floor above
        // the earliest active snapshot point.
        if !file_manifest.is_empty() {
            // Prune any already-released guards from previous snapshots
            self.snapshot_guards.retain(|h| !h.is_released());

            let handle = self
                .engine
                .snapshot_guard()
                .acquire(std::time::Duration::from_secs(600), watermark);
            self.snapshot_guards.push(handle);
        }

        LanceSnapshotBuilder {
            table_entries: self.engine.table_entries(),
            last_applied: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
            min_safe_log_index: watermark,
            file_manifest,
            sync_addr: self.sync_addr.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<LanceTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let (data, _): (SnapshotData, _) =
            bincode::serde::decode_from_slice(snapshot.get_ref(), bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let has_files = !data.file_manifest.is_empty();
        let total_files = data.file_manifest.len();
        let total_bytes: u64 = data.file_manifest.iter().map(|e| e.size).sum();

        info!(
            ?meta.last_log_id,
            tables = data.tables.len(),
            manifest_files = total_files,
            manifest_bytes = total_bytes,
            has_sync_addr = data.sync_addr.is_some(),
            "Installing snapshot"
        );

        // Shut down existing tables, then restore from snapshot.
        if let Err(e) = self.engine.shutdown().await {
            warn!("Error shutting down engine during snapshot install: {}", e);
        }

        // Stream segment files from leader BEFORE restoring tables.
        // This ensures the Lance dataset directories exist on disk when
        // TableEngine::open tries to open them.
        if has_files {
            if let Some(ref sync_addr) = data.sync_addr {
                if let Some(ref client) = self.sync_client {
                    info!(
                        addr = sync_addr,
                        files = total_files,
                        bytes = total_bytes,
                        "Syncing segment files from leader"
                    );

                    match client.sync_files(sync_addr, &data.file_manifest).await {
                        Ok(result) => {
                            info!(
                                files_transferred = result.files_transferred,
                                files_missing = result.files_missing,
                                bytes_transferred = result.bytes_transferred,
                                verification_failures = result.verification_failures.len(),
                                "Segment file sync complete"
                            );

                            if !result.verification_failures.is_empty() {
                                error!(
                                    failures = ?result.verification_failures,
                                    "Post-sync verification failed for some files"
                                );
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!(
                                        "Post-sync verification failed for {} files",
                                        result.verification_failures.len()
                                    ),
                                ));
                            }

                            if result.files_missing > 0 {
                                warn!(
                                    missing = ?result.missing_paths,
                                    "Some files were unavailable during sync (may have been promoted to S3)"
                                );
                            }
                        }
                        Err(e) => {
                            error!("Segment file sync failed: {}", e);
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("Segment file sync failed: {}", e),
                            ));
                        }
                    }
                } else {
                    warn!(
                        "Snapshot has {} files to sync but no sync client configured",
                        total_files
                    );
                }
            } else {
                warn!(
                    "Snapshot has {} files to sync but no sync address provided",
                    total_files
                );
            }
        }

        // Now restore tables — segment files should be on disk.
        for (table_name, table_entry) in &data.tables {
            let snapshot = table_entry.to_snapshot();
            let schema = if let Some(latest) = table_entry.schema_history.last() {
                match ipc::schema_from_ipc(&latest.schema_ipc) {
                    Ok(s) => Some(Arc::new(s)),
                    Err(e) => {
                        warn!(table = %table_name, "Failed to decode schema from snapshot: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            let config = crate::config::TableOpenConfig::from_persisted(
                table_name,
                &table_entry.config,
                schema,
                self.engine.config(),
            );

            if let Err(e) = self.engine.restore_table(config, &snapshot).await {
                error!(table = %table_name, "Failed to restore table from snapshot: {}", e);
            }
        }

        self.last_applied = meta.last_log_id.clone();
        self.last_membership = meta.last_membership.clone();

        // Restore normal purge floor computation now that sync is complete.
        self.update_purge_floor();

        // Bulk-write snapshot state to MDBX manifest for crash consistency.
        if let Some(manifest) = &self.manifest {
            let update = ManifestUpdate::InstallSnapshot {
                group_id: self.group_id,
                meta: GroupMeta::from_raft(&self.last_applied, &self.last_membership),
                tables: data.tables,
                done: None,
            };
            if let Err(e) = manifest.send_update_durable(update).await {
                error!("Failed to write snapshot state to MDBX manifest: {}", e);
            }
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LanceTypeConfig>>, io::Error> {
        // No snapshot if nothing has been applied yet — returning a synthetic
        // snapshot with a fake LogId would trick openraft into thinking the node
        // is already initialized, preventing single-node bootstrap.
        if self.last_applied.is_none() {
            return Ok(None);
        }
        let mut builder = self.get_snapshot_builder().await;
        match builder.build_snapshot().await {
            Ok(snap) => Ok(Some(snap)),
            Err(e) => {
                warn!("Failed to build snapshot: {}", e);
                Ok(None)
            }
        }
    }
}

impl LanceStateMachine {
    /// Compute the minimum log index across all tables' hot/warm segments.
    fn compute_min_safe_log_index(&self) -> Option<u64> {
        let mut min_index: Option<u64> = None;
        for table_name in self.engine.list_tables() {
            if let Some(table) = self.engine.get_table(&table_name) {
                if let Some(table_min) = table.min_safe_log_index() {
                    min_index = Some(match min_index {
                        Some(current) => current.min(table_min),
                        None => table_min,
                    });
                }
            }
        }
        min_index
    }

    /// Update the purge floor by recomputing from current state.
    ///
    /// Takes the minimum of `compute_min_safe_log_index()` (per-table
    /// constraint) and `min_watermark()` (active snapshot transfer
    /// constraint). This ensures the floor never rises above the
    /// earliest snapshot point that still has an active file transfer.
    fn update_purge_floor(&self) {
        let min_safe = self.compute_min_safe_log_index();
        let min_guard = self.engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.update_purge_floor_with(effective);
    }

    /// Update the purge floor with a precomputed value, still capped
    /// by any active snapshot transfer watermarks.
    fn update_purge_floor_with(&self, min_safe: Option<u64>) {
        if let Some(floor) = &self.purge_floor {
            // Never raise the floor above active snapshot watermarks
            let min_guard = self.engine.snapshot_guard().min_watermark();
            let effective = match (min_safe, min_guard) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let value = effective.unwrap_or(0);
            floor.store(value, Ordering::Release);
            if value > 0 {
                info!(min_safe_log_index = value, "Updated purge floor");
            }
        }
    }

    async fn apply_command(&self, cmd: LanceCommand) -> LanceResponse {
        debug!(%cmd, "Applying command");

        match cmd {
            LanceCommand::CreateTable {
                table_name,
                schema_ipc,
            } => match ipc::schema_from_ipc(&schema_ipc) {
                Ok(schema) => {
                    let config = self
                        .engine
                        .config()
                        .build_table_config(&table_name, Arc::new(schema));
                    match self.engine.create_table(config, None).await {
                        Ok(_) => LanceResponse::Ok,
                        Err(e) => {
                            error!(table = %table_name, "create_table failed: {}", e);
                            LanceResponse::Error(e.to_string())
                        }
                    }
                }
                Err(e) => {
                    error!(table = %table_name, "Failed to decode schema IPC: {}", e);
                    LanceResponse::Error(e.to_string())
                }
            },

            LanceCommand::DropTable { table_name } => {
                match self.engine.drop_table(&table_name).await {
                    Ok(()) => LanceResponse::Ok,
                    Err(e) => {
                        error!(table = %table_name, "drop_table failed: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }

            LanceCommand::AppendRecords { table_name, data } => {
                let table = match self.engine.require_table(&table_name) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(table = %table_name, "Table not found: {}", e);
                        return LanceResponse::Error(e.to_string());
                    }
                };
                match ipc::decode_record_batches(&data) {
                    Ok(batches) => match table.apply_append(batches).await {
                        Ok(()) => LanceResponse::Ok,
                        Err(e) => {
                            error!(table = %table_name, "apply_append failed: {}", e);
                            LanceResponse::Error(e.to_string())
                        }
                    },
                    Err(e) => {
                        error!(table = %table_name, "Failed to decode IPC data: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }

            LanceCommand::SealActiveSegment {
                table_name,
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => {
                let table = match self.engine.require_table(&table_name) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(table = %table_name, "Table not found: {}", e);
                        return LanceResponse::Error(e.to_string());
                    }
                };
                match table
                    .apply_seal(sealed_segment_id, new_active_segment_id, reason)
                    .await
                {
                    Ok(()) => {
                        if let Err(e) = table.compact_sealed().await {
                            warn!(table = %table_name, "Sealed segment compaction failed: {}", e);
                        }
                        if let Err(e) = table.create_seal_indices().await {
                            warn!(table = %table_name, "Index creation on sealed segment failed: {}", e);
                        }
                        LanceResponse::Ok
                    }
                    Err(e) => {
                        error!(table = %table_name, "apply_seal failed: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }

            LanceCommand::BeginFlush {
                table_name,
                segment_id,
            } => {
                let table = match self.engine.require_table(&table_name) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(table = %table_name, "Table not found: {}", e);
                        return LanceResponse::Error(e.to_string());
                    }
                };
                table.apply_begin_flush(segment_id);
                LanceResponse::Ok
            }

            LanceCommand::PromoteToDeepStorage {
                table_name,
                segment_id,
                s3_manifest_version,
            } => {
                let table = match self.engine.require_table(&table_name) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(table = %table_name, "Table not found: {}", e);
                        return LanceResponse::Error(e.to_string());
                    }
                };
                match table.apply_promote(segment_id, s3_manifest_version).await {
                    Ok(()) => LanceResponse::Ok,
                    Err(e) => {
                        error!(table = %table_name, "apply_promote failed: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }

            LanceCommand::RegisterSession { session_id } => {
                if let Some(pins) = &self.version_pins {
                    // Session ID is pre-allocated by the server; just ensure it exists.
                    debug!(session_id, "Raft-replicated session registration");
                    let _ = pins.create_session();
                }
                LanceResponse::Ok
            }

            LanceCommand::PinVersion {
                session_id,
                table_name,
                tier,
                version,
            } => {
                if let Some(pins) = &self.version_pins {
                    if let Some(pin_tier) = crate::version_pins::PinTier::from_str(&tier) {
                        pins.pin(
                            session_id,
                            crate::version_pins::PinKey {
                                catalog: self.catalog_name.clone(),
                                table: table_name,
                                tier: pin_tier,
                                version,
                            },
                        );
                    }
                }
                LanceResponse::Ok
            }

            LanceCommand::UnpinVersion {
                session_id,
                table_name,
                tier,
                version,
            } => {
                if let Some(pins) = &self.version_pins {
                    if let Some(pin_tier) = crate::version_pins::PinTier::from_str(&tier) {
                        pins.unpin(
                            session_id,
                            crate::version_pins::PinKey {
                                catalog: self.catalog_name.clone(),
                                table: table_name,
                                tier: pin_tier,
                                version,
                            },
                        );
                    }
                }
                LanceResponse::Ok
            }

            LanceCommand::ExpireSession { session_id } => {
                if let Some(pins) = &self.version_pins {
                    pins.remove_session(session_id);
                }
                LanceResponse::Ok
            }

            LanceCommand::DeleteRecords {
                table_name,
                filter,
            } => {
                let table = match self.engine.require_table(&table_name) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(table = %table_name, "Table not found: {}", e);
                        return LanceResponse::Error(e.to_string());
                    }
                };
                match table.apply_delete(&filter).await {
                    Ok(count) => LanceResponse::RowsAffected(count),
                    Err(e) => {
                        error!(table = %table_name, "apply_delete failed: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }

            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => {
                let table = match self.engine.require_table(&table_name) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(table = %table_name, "Table not found: {}", e);
                        return LanceResponse::Error(e.to_string());
                    }
                };
                match ipc::decode_record_batches(&data) {
                    Ok(batches) => match table.apply_update(&filter, batches).await {
                        Ok(count) => LanceResponse::RowsAffected(count),
                        Err(e) => {
                            error!(table = %table_name, "apply_update failed: {}", e);
                            LanceResponse::Error(e.to_string())
                        }
                    },
                    Err(e) => {
                        error!(table = %table_name, "Failed to decode IPC data: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }
        }
    }
}

/// Builds snapshots from the current multi-table engine state.
pub struct LanceSnapshotBuilder {
    table_entries: std::collections::HashMap<String, PersistedTableEntry>,
    last_applied: Option<LogId<LanceTypeConfig>>,
    last_membership: StoredMembership<LanceTypeConfig>,
    min_safe_log_index: Option<u64>,
    file_manifest: Vec<crate::types::SnapshotFileEntry>,
    sync_addr: Option<String>,
}

impl RaftSnapshotBuilder<LanceTypeConfig> for LanceSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<LanceTypeConfig>, io::Error> {
        let data = SnapshotData {
            tables: self.table_entries.clone(),
            min_safe_log_index: self.min_safe_log_index,
            file_manifest: self.file_manifest.clone(),
            sync_addr: self.sync_addr.clone(),
        };

        let bytes = bincode::serde::encode_to_vec(&data, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let last_applied = self.last_applied.clone().unwrap_or(LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 0,
                node_id: 0,
            },
            index: 0,
        });

        let meta = SnapshotMeta {
            last_log_id: Some(last_applied),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "lance-snap-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            ),
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}
