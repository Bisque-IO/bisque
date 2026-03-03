//! Raft state machine implementation for bisque-lance.
//!
//! `LanceStateMachine` implements openraft's `RaftStateMachine` trait,
//! dispatching applied log entries to the `BisqueLance` storage engine.

use std::io::{self, Cursor};
use std::sync::Arc;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use tracing::{debug, error, info, warn};

use crate::engine::BisqueLance;
use crate::ipc;
use crate::types::{LanceCommand, LanceResponse, SnapshotData};
use crate::LanceTypeConfig;

/// Raft state machine that drives the BisqueLance storage engine.
pub struct LanceStateMachine {
    engine: Arc<BisqueLance>,
    last_applied: Option<LogId<LanceTypeConfig>>,
    last_membership: StoredMembership<LanceTypeConfig>,
}

impl LanceStateMachine {
    pub fn new(engine: Arc<BisqueLance>) -> Self {
        Self {
            engine,
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }

    /// Access the underlying engine.
    pub fn engine(&self) -> &Arc<BisqueLance> {
        &self.engine
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
        Ok((self.last_applied.clone(), self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<
                    openraft::storage::EntryResponder<LanceTypeConfig>,
                    io::Error,
                >,
            > + Unpin
            + OptionalSend,
    {
        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            self.last_applied = Some(entry.log_id.clone());

            let response = match entry.payload {
                EntryPayload::Blank => LanceResponse::Ok,
                EntryPayload::Normal(cmd) => self.apply_command(cmd).await,
                EntryPayload::Membership(m) => {
                    self.last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), m);
                    LanceResponse::Ok
                }
            };

            if let Some(r) = responder {
                r.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LanceSnapshotBuilder {
            catalog: self.engine.catalog(),
            flush_state: self.engine.flush_state(),
            last_applied: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
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

        info!(
            ?meta.last_log_id,
            active_segment = data.catalog.active_segment,
            sealed_segment = ?data.catalog.sealed_segment,
            "Installing snapshot"
        );

        // Rebuild engine state from snapshot catalog.
        // The actual Lance datasets will be rebuilt from Raft log replay
        // starting from the snapshot point.
        // For now we just update the metadata — a full implementation would
        // re-open datasets from the snapshot catalog state.

        self.last_applied = meta.last_log_id.clone();
        self.last_membership = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LanceTypeConfig>>, io::Error> {
        // Build a snapshot on-demand from current state
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
    async fn apply_command(&self, cmd: LanceCommand) -> LanceResponse {
        debug!(%cmd, "Applying command");

        match cmd {
            LanceCommand::AppendRecords { data } => {
                match ipc::decode_record_batches(&data) {
                    Ok(batches) => match self.engine.apply_append(batches).await {
                        Ok(()) => LanceResponse::Ok,
                        Err(e) => {
                            error!("apply_append failed: {}", e);
                            LanceResponse::Error(e.to_string())
                        }
                    },
                    Err(e) => {
                        error!("Failed to decode IPC data: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }
            LanceCommand::SealActiveSegment {
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => {
                match self
                    .engine
                    .apply_seal(sealed_segment_id, new_active_segment_id, reason)
                    .await
                {
                    Ok(()) => {
                        // Build indices on the newly sealed segment (non-blocking for Raft)
                        if let Err(e) = self.engine.create_seal_indices().await {
                            warn!("Index creation on sealed segment failed: {}", e);
                            // Non-fatal: indices are an optimization
                        }
                        LanceResponse::Ok
                    }
                    Err(e) => {
                        error!("apply_seal failed: {}", e);
                        LanceResponse::Error(e.to_string())
                    }
                }
            }
            LanceCommand::BeginFlush { segment_id } => {
                self.engine.apply_begin_flush(segment_id);
                LanceResponse::Ok
            }
            LanceCommand::PromoteToDeepStorage {
                segment_id,
                s3_manifest_version,
            } => match self.engine.apply_promote(segment_id, s3_manifest_version).await {
                Ok(()) => LanceResponse::Ok,
                Err(e) => {
                    error!("apply_promote failed: {}", e);
                    LanceResponse::Error(e.to_string())
                }
            },
        }
    }
}

/// Builds snapshots from the current engine state.
pub struct LanceSnapshotBuilder {
    catalog: crate::types::SegmentCatalog,
    flush_state: crate::types::FlushState,
    last_applied: Option<LogId<LanceTypeConfig>>,
    last_membership: StoredMembership<LanceTypeConfig>,
}

impl RaftSnapshotBuilder<LanceTypeConfig> for LanceSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<LanceTypeConfig>, io::Error> {
        let data = SnapshotData {
            catalog: self.catalog.clone(),
            flush_state: self.flush_state.clone(),
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
