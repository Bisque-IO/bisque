//! Raft state machine implementation for bisque-lance.
//!
//! `LanceStateMachine` implements openraft's `RaftStateMachine` trait,
//! dispatching applied log entries to the appropriate `TableEngine` within
//! the multi-table `BisqueLance` engine.

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

/// Raft state machine that drives the BisqueLance multi-table storage engine.
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
            table_snapshots: self.engine.table_snapshots(),
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
            tables = data.tables.len(),
            "Installing snapshot"
        );

        // Shut down existing tables, then restore from snapshot.
        if let Err(e) = self.engine.shutdown().await {
            warn!("Error shutting down engine during snapshot install: {}", e);
        }

        for (table_name, table_snapshot) in &data.tables {
            let schema = if let Some(latest) = table_snapshot.schema_history.last() {
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

            let mut config = self.engine.config().build_table_config(
                table_name,
                schema.unwrap_or_else(|| {
                    use arrow_schema::{DataType, Field, Schema};
                    Arc::new(Schema::new(vec![Field::new("_placeholder", DataType::Null, true)]))
                }),
            );

            if !table_snapshot.catalog.s3_dataset_uri.is_empty() {
                config.s3_uri = Some(table_snapshot.catalog.s3_dataset_uri.clone());
            }

            if let Err(e) = self.engine.restore_table(config, table_snapshot).await {
                error!(table = %table_name, "Failed to restore table from snapshot: {}", e);
            }
        }

        self.last_applied = meta.last_log_id.clone();
        self.last_membership = meta.last_membership.clone();

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
    async fn apply_command(&self, cmd: LanceCommand) -> LanceResponse {
        debug!(%cmd, "Applying command");

        match cmd {
            LanceCommand::CreateTable { table_name, schema_ipc } => {
                match ipc::schema_from_ipc(&schema_ipc) {
                    Ok(schema) => {
                        let config = self.engine.config().build_table_config(
                            &table_name,
                            Arc::new(schema),
                        );
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
                }
            }

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

            LanceCommand::BeginFlush { table_name, segment_id } => {
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
        }
    }
}

/// Builds snapshots from the current multi-table engine state.
pub struct LanceSnapshotBuilder {
    table_snapshots: std::collections::HashMap<String, crate::types::TableSnapshot>,
    last_applied: Option<LogId<LanceTypeConfig>>,
    last_membership: StoredMembership<LanceTypeConfig>,
}

impl RaftSnapshotBuilder<LanceTypeConfig> for LanceSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<LanceTypeConfig>, io::Error> {
        let data = SnapshotData {
            tables: self.table_snapshots.clone(),
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
