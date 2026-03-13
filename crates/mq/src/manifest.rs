//! MDBX-backed manifest for crash-consistent MQ metadata.
//!
//! Each Raft group gets its own MDBX database under `{base_dir}/.mq_groups/{group_id}/`.
//!
//! Two persistence paths:
//!
//! 1. **Structural persistence** — Create/Delete commands for entities (topics,
//!    consumer groups, sessions) are persisted to MDBX via fire-and-forget
//!    writes. A `structural_purge_floor` tracks the last log index whose
//!    structural writes are confirmed committed to MDBX.
//!
//! 2. **Snapshot install** — Full snapshot blobs for new/lagging nodes receiving
//!    a snapshot transfer from the leader.
//!
//! On recovery: load structural state (entities + next_id) from MDBX, then
//! replay raft log from `structural_purge_floor` to rebuild message state and
//! catch up any structural commands that didn't make it to MDBX before crash.
//!
//! A single dedicated `std::thread` owns all open MDBX environments and
//! handles writes via crossfire queues. This keeps fsync off tokio async threads.
//!
//! Reads use MDBX read-only transactions directly (concurrent readers supported)
//! via a shared `Arc<GroupMdbxEnv>` map.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossfire::{MAsyncTx, RecvTimeoutError, Rx, TryRecvError, mpsc::Array};
use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, Table, TableFlags, WriteFlags,
};
use openraft::{LogId, StoredMembership};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::MqTypeConfig;
use crate::types::MqSnapshotData;

// MDBX table names
const META_TABLE: &str = "mq_meta";
const ENTITIES_TABLE: &str = "mq_entities";
const SNAPSHOT_TABLE: &str = "mq_snapshot";
const SEGMENT_RANGES_TABLE: &str = "mq_segment_ranges";
const META_KEY: &[u8] = b"raft";
const STRUCTURAL_FLOOR_KEY: &[u8] = b"structural_floor";
const NEXT_ID_KEY: &[u8] = b"next_id";

// =============================================================================
// Types
// =============================================================================

/// Persisted per-group Raft metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GroupMeta {
    last_applied_term: u64,
    last_applied_node_id: u64,
    last_applied_index: u64,
    last_applied_present: bool,
    membership_bytes: Vec<u8>,
}

impl GroupMeta {
    pub(crate) fn from_raft(
        last_applied: &Option<LogId<MqTypeConfig>>,
        last_membership: &StoredMembership<MqTypeConfig>,
    ) -> Self {
        let (term, node_id, index, present) = match last_applied {
            Some(lid) => (lid.leader_id.term, lid.leader_id.node_id, lid.index, true),
            None => (0, 0, 0, false),
        };
        let membership_bytes =
            bincode::serde::encode_to_vec(last_membership, bincode::config::standard())
                .unwrap_or_default();
        Self {
            last_applied_term: term,
            last_applied_node_id: node_id,
            last_applied_index: index,
            last_applied_present: present,
            membership_bytes,
        }
    }

    pub(crate) fn to_raft(
        &self,
    ) -> io::Result<(Option<LogId<MqTypeConfig>>, StoredMembership<MqTypeConfig>)> {
        let last_applied = if self.last_applied_present {
            Some(LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: self.last_applied_term,
                    node_id: self.last_applied_node_id,
                },
                index: self.last_applied_index,
            })
        } else {
            None
        };
        let (membership, _): (StoredMembership<MqTypeConfig>, _) =
            bincode::serde::decode_from_slice(&self.membership_bytes, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok((last_applied, membership))
    }
}

/// A structural entity write to persist in MDBX.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StructuralWrite {
    CreateTopic(crate::topic::TopicMeta),
    DeleteTopic(u64),
    CreateExchange(crate::exchange::ExchangeMeta),
    DeleteExchange(u64),
    CreateConsumerGroup(crate::consumer_group::ConsumerGroupMeta),
    DeleteConsumerGroup(u64),
    CreateSession(crate::session::SessionMeta),
    /// Persist a retained message set on an exchange.
    SetRetained {
        exchange_id: u64,
        routing_key: String,
        message: Vec<u8>,
    },
    /// Persist removal of a retained message from an exchange.
    DeleteRetained {
        exchange_id: u64,
        routing_key: String,
    },
}

/// Command sent to the manifest worker thread.
pub(crate) enum ManifestCommand {
    /// Fire-and-forget structural entity write.
    StructuralUpdate {
        group_id: u64,
        log_index: u64,
        next_id: u64,
        write: StructuralWrite,
    },
    /// Bulk write for snapshot install — entities written individually.
    InstallSnapshot {
        group_id: u64,
        meta: GroupMeta,
        snapshot_data: MqSnapshotData,
        done: Option<tokio::sync::oneshot::Sender<()>>,
    },
    /// Fire-and-forget segment range write when a segment is sealed.
    SealedSegment {
        group_id: u64,
        segment_id: u64,
        /// `(entity_type, entity_id, record_count, total_bytes)`
        entity_summaries: Vec<(u8, u64, u64, u64)>,
    },
    /// Fire-and-forget segment range deletion when a segment is purged.
    PurgeSegment { group_id: u64, segment_id: u64 },
    /// Batch-persist detached retained messages for an exchange.
    /// Written when segments are purged and retained messages are detached
    /// from mmap, so recovery doesn't need to replay ancient log entries.
    PersistRetained {
        group_id: u64,
        exchange_id: u64,
        /// (routing_key, message_bytes) pairs
        entries: Vec<(String, Vec<u8>)>,
    },
}

/// Entity key prefixes for MDBX entities table.
fn entity_key(prefix: &[u8], id: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = prefix[0];
    key[1..9].copy_from_slice(&id.to_be_bytes());
    key
}

const TOPIC_PREFIX: &[u8] = b"T";
const EXCHANGE_PREFIX: &[u8] = b"E";
const CONSUMER_GROUP_PREFIX: &[u8] = b"G";
const RETAINED_PREFIX: &[u8] = b"R";
const SESSION_PREFIX: &[u8] = b"S";

/// Structural state loaded from MDBX on recovery.
pub(crate) struct StructuralState {
    pub topics: Vec<crate::topic::TopicMeta>,
    pub exchanges: Vec<crate::exchange::ExchangeMeta>,
    pub consumer_groups: Vec<crate::consumer_group::ConsumerGroupMeta>,
    pub sessions: Vec<crate::session::SessionMeta>,
    /// Retained messages per exchange: (exchange_id, Vec<(routing_key, message_bytes)>)
    pub retained: Vec<(u64, Vec<(String, Vec<u8>)>)>,
    pub next_id: u64,
    pub structural_purge_floor: u64,
}

// =============================================================================
// GroupMdbxEnv
// =============================================================================

pub(crate) struct GroupMdbxEnv {
    db: Arc<Database<NoWriteMap>>,
    meta_dbi: u32,
    entities_dbi: u32,
    snapshot_dbi: u32,
    segment_ranges_dbi: u32,
}

impl GroupMdbxEnv {
    fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut opts = DatabaseOptions::default();
        opts.max_tables = Some(8);
        opts.mode = Mode::ReadWrite(ReadWriteOptions {
            min_size: Some(64 * 1024),
            max_size: Some(256 * 1024 * 1024),
            growth_step: Some(64 * 1024),
            shrink_threshold: Some(256 * 1024),
            ..Default::default()
        });

        let db = Database::<NoWriteMap>::open_with_options(path, opts)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let db = Arc::new(db);

        let txn = db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let meta_table = txn
            .create_table(Some(META_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let meta_dbi = meta_table.dbi();
        drop(meta_table);

        let entities_table = txn
            .create_table(Some(ENTITIES_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let entities_dbi = entities_table.dbi();
        drop(entities_table);

        let snapshot_table = txn
            .create_table(Some(SNAPSHOT_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let snapshot_dbi = snapshot_table.dbi();
        drop(snapshot_table);

        // DUP_SORT | DUP_FIXED: entity key → multiple fixed-size segment range values.
        // Key: [entity_type:1][entity_id:8 BE] (9 bytes)
        // Value: [segment_id:8 LE][record_count:8 LE][total_bytes:8 LE] (24 bytes)
        // Values sorted by raw bytes (segment_id LE is the sort prefix).
        let segment_ranges_table = txn
            .create_table(
                Some(SEGMENT_RANGES_TABLE),
                TableFlags::DUP_SORT | TableFlags::DUP_FIXED,
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let segment_ranges_dbi = segment_ranges_table.dbi();
        drop(segment_ranges_table);

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(Self {
            db,
            meta_dbi,
            entities_dbi,
            snapshot_dbi,
            segment_ranges_dbi,
        })
    }

    unsafe fn meta_table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.meta_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn meta_table_rw<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RW, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.meta_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn entities_table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.entities_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn entities_table_rw<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RW, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.entities_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn snapshot_table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.snapshot_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn snapshot_table_rw<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RW, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.snapshot_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn segment_ranges_table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.segment_ranges_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn segment_ranges_table_rw<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RW, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.segment_ranges_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    // -- Read operations --

    fn read_group_meta(&self) -> io::Result<Option<GroupMeta>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.meta_table_ro(&txn) };
        match txn.get::<std::borrow::Cow<[u8]>>(&table, META_KEY) {
            Ok(Some(bytes)) => {
                let (meta, _): (GroupMeta, _) =
                    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                Ok(Some(meta))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        }
    }

    fn read_snapshot_data(&self) -> io::Result<Option<MqSnapshotData>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let meta_tbl = unsafe { self.meta_table_ro(&txn) };

        // Read next_id from meta table
        let next_id = match txn.get::<std::borrow::Cow<[u8]>>(&meta_tbl, NEXT_ID_KEY) {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                u64::from_le_bytes(bytes[..8].try_into().unwrap())
            }
            _ => 1,
        };

        let snap_tbl = unsafe { self.snapshot_table_ro(&txn) };

        let mut topics = Vec::new();
        let mut consumer_groups = Vec::new();
        let mut exchanges = Vec::new();
        let mut sessions = Vec::new();
        let mut found_any = false;

        let mut cursor = txn
            .cursor(&snap_tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
        for item in iter {
            let (key, value) = match item {
                Ok(pair) => pair,
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
            };
            if key.len() != 9 {
                continue;
            }
            found_any = true;
            let prefix = key[0];
            match prefix {
                b'T' => {
                    let (snap, _): (crate::types::TopicSnapshot, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    topics.push(snap);
                }
                b'E' => {
                    let (snap, _): (crate::types::ExchangeSnapshot, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    exchanges.push(snap);
                }
                b'G' => {
                    let (snap, _): (crate::consumer_group::ConsumerGroupSnapshot, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    consumer_groups.push(snap);
                }
                b'S' => {
                    let (snap, _): (crate::types::SessionSnapshot, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    sessions.push(snap);
                }
                _ => {}
            }
        }

        if !found_any {
            return Ok(None);
        }

        Ok(Some(MqSnapshotData {
            topics,
            consumer_groups,
            exchanges,
            sessions,
            pending_wills: Vec::new(),
            next_id,
            file_manifest: Vec::new(),
            sync_addr: None,
        }))
    }

    // -- Read structural state --

    fn read_structural_state(&self) -> io::Result<Option<StructuralState>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let meta_tbl = unsafe { self.meta_table_ro(&txn) };

        // Read structural purge floor
        let floor = match txn.get::<std::borrow::Cow<[u8]>>(&meta_tbl, STRUCTURAL_FLOOR_KEY) {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                u64::from_le_bytes(bytes[..8].try_into().unwrap())
            }
            Ok(_) => return Ok(None),
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        };

        // Read next_id
        let next_id = match txn.get::<std::borrow::Cow<[u8]>>(&meta_tbl, NEXT_ID_KEY) {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                u64::from_le_bytes(bytes[..8].try_into().unwrap())
            }
            Ok(_) => 1,
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        };

        let entities_tbl = unsafe { self.entities_table_ro(&txn) };

        let mut topics = Vec::new();
        let mut exchanges = Vec::new();
        let mut consumer_groups = Vec::new();
        let mut sessions = Vec::new();
        let mut retained = Vec::new();

        // Scan all entity keys
        let mut cursor = txn
            .cursor(&entities_tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
        for item in iter {
            let (key, value) = match item {
                Ok(pair) => pair,
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
            };
            if key.len() != 9 {
                continue; // skip non-entity keys (e.g. "snapshot")
            }
            let prefix = key[0];
            match prefix {
                b'T' => {
                    let (meta, _): (crate::topic::TopicMeta, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    topics.push(meta);
                }
                b'E' => {
                    let (meta, _): (crate::exchange::ExchangeMeta, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    exchanges.push(meta);
                }
                b'G' => {
                    let (meta, _): (crate::consumer_group::ConsumerGroupMeta, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    consumer_groups.push(meta);
                }
                b'S' => {
                    let (meta, _): (crate::session::SessionMeta, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    sessions.push(meta);
                }
                b'R' => {
                    let exchange_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                    let (entries, _): (Vec<(String, Vec<u8>)>, _) =
                        bincode::serde::decode_from_slice(&value, bincode::config::standard())
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            })?;
                    retained.push((exchange_id, entries));
                }
                _ => {} // unknown prefix, skip
            }
        }

        Ok(Some(StructuralState {
            topics,
            exchanges,
            consumer_groups,
            sessions,
            retained,
            next_id,
            structural_purge_floor: floor,
        }))
    }

    // -- Write operations (called by worker thread only) --

    fn apply_structural_write(
        &self,
        log_index: u64,
        next_id: u64,
        write: &StructuralWrite,
    ) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let meta_tbl = unsafe { self.meta_table_rw(&txn) };
        let entities_tbl = unsafe { self.entities_table_rw(&txn) };

        // Write the entity
        match write {
            StructuralWrite::CreateTopic(meta) => {
                let key = entity_key(TOPIC_PREFIX, meta.topic_id);
                let bytes = bincode::serde::encode_to_vec(meta, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                txn.put(&entities_tbl, &key[..], &bytes, WriteFlags::empty())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
            StructuralWrite::DeleteTopic(id) => {
                let key = entity_key(TOPIC_PREFIX, *id);
                let _ = txn.del(&entities_tbl, &key[..], None);
            }
            StructuralWrite::CreateExchange(meta) => {
                let key = entity_key(EXCHANGE_PREFIX, meta.exchange_id);
                let bytes = bincode::serde::encode_to_vec(meta, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                txn.put(&entities_tbl, &key[..], &bytes, WriteFlags::empty())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
            StructuralWrite::DeleteExchange(id) => {
                let key = entity_key(EXCHANGE_PREFIX, *id);
                let _ = txn.del(&entities_tbl, &key[..], None);
            }
            StructuralWrite::CreateConsumerGroup(meta) => {
                let key = entity_key(CONSUMER_GROUP_PREFIX, meta.group_id);
                let bytes = bincode::serde::encode_to_vec(meta, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                txn.put(&entities_tbl, &key[..], &bytes, WriteFlags::empty())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
            StructuralWrite::DeleteConsumerGroup(id) => {
                let key = entity_key(CONSUMER_GROUP_PREFIX, *id);
                let _ = txn.del(&entities_tbl, &key[..], None);
            }
            StructuralWrite::CreateSession(meta) => {
                let key = entity_key(SESSION_PREFIX, meta.session_id);
                let bytes = bincode::serde::encode_to_vec(meta, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                txn.put(&entities_tbl, &key[..], &bytes, WriteFlags::empty())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
            StructuralWrite::SetRetained {
                exchange_id,
                routing_key,
                message,
            } => {
                // Read existing retained entries for this exchange, update, and write back.
                let key = entity_key(RETAINED_PREFIX, *exchange_id);
                let mut entries: Vec<(String, Vec<u8>)> = match txn
                    .get::<Vec<u8>>(&entities_tbl, &key[..])
                {
                    Ok(Some(existing)) => {
                        bincode::serde::decode_from_slice(&existing, bincode::config::standard())
                            .map(|(v, _)| v)
                            .unwrap_or_default()
                    }
                    _ => Vec::new(),
                };
                // Upsert: replace existing or append.
                if let Some(pos) = entries.iter().position(|(k, _)| k == routing_key) {
                    entries[pos].1 = message.clone();
                } else {
                    entries.push((routing_key.clone(), message.clone()));
                }
                let bytes = bincode::serde::encode_to_vec(&entries, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                txn.put(&entities_tbl, &key[..], &bytes, WriteFlags::empty())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
            StructuralWrite::DeleteRetained {
                exchange_id,
                routing_key,
            } => {
                let key = entity_key(RETAINED_PREFIX, *exchange_id);
                if let Ok(Some(existing)) = txn.get::<Vec<u8>>(&entities_tbl, &key[..]) {
                    let mut entries: Vec<(String, Vec<u8>)> =
                        bincode::serde::decode_from_slice(&existing, bincode::config::standard())
                            .map(|(v, _)| v)
                            .unwrap_or_default();
                    entries.retain(|(k, _)| k != routing_key);
                    if entries.is_empty() {
                        let _ = txn.del(&entities_tbl, &key[..], None);
                    } else {
                        let bytes =
                            bincode::serde::encode_to_vec(&entries, bincode::config::standard())
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                        txn.put(&entities_tbl, &key[..], &bytes, WriteFlags::empty())
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    }
                }
            }
        }

        // Update structural purge floor
        txn.put(
            &meta_tbl,
            STRUCTURAL_FLOOR_KEY,
            &log_index.to_le_bytes(),
            WriteFlags::empty(),
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Update next_id
        txn.put(
            &meta_tbl,
            NEXT_ID_KEY,
            &next_id.to_le_bytes(),
            WriteFlags::empty(),
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    /// Persist detached retained messages for an exchange.
    /// Overwrites any previously stored retained data for this exchange.
    fn persist_retained(&self, exchange_id: u64, entries: &[(String, Vec<u8>)]) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let entities_tbl = unsafe { self.entities_table_rw(&txn) };
        let key = entity_key(RETAINED_PREFIX, exchange_id);

        if entries.is_empty() {
            // Clear retained data for this exchange
            let _ = txn.del(&entities_tbl, &key[..], None);
        } else {
            let bytes = bincode::serde::encode_to_vec(entries, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&entities_tbl, &key[..], &bytes, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    fn install_snapshot(&self, meta: &GroupMeta, snap: &MqSnapshotData) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let meta_tbl = unsafe { self.meta_table_rw(&txn) };
        let meta_bytes = bincode::serde::encode_to_vec(meta, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        txn.put(&meta_tbl, META_KEY, &meta_bytes, WriteFlags::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Write next_id
        txn.put(
            &meta_tbl,
            NEXT_ID_KEY,
            &snap.next_id.to_le_bytes(),
            WriteFlags::empty(),
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Clear structural entities table and floor (no longer relevant after snapshot install)
        let entities_tbl = unsafe { self.entities_table_rw(&txn) };
        txn.clear_table(&entities_tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let _ = txn.del(&meta_tbl, STRUCTURAL_FLOOR_KEY, None);

        // Clear and rebuild snapshot table with individual entity records
        let snap_tbl = unsafe { self.snapshot_table_rw(&txn) };
        txn.clear_table(&snap_tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        for topic in &snap.topics {
            let key = entity_key(TOPIC_PREFIX, topic.meta.topic_id);
            let bytes = bincode::serde::encode_to_vec(topic, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&snap_tbl, &key[..], &bytes, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        for exchange in &snap.exchanges {
            let key = entity_key(EXCHANGE_PREFIX, exchange.meta.exchange_id);
            let bytes = bincode::serde::encode_to_vec(exchange, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&snap_tbl, &key[..], &bytes, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        for cg in &snap.consumer_groups {
            let key = entity_key(CONSUMER_GROUP_PREFIX, cg.meta.group_id);
            let bytes = bincode::serde::encode_to_vec(cg, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&snap_tbl, &key[..], &bytes, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        for session in &snap.sessions {
            let key = entity_key(SESSION_PREFIX, session.meta.session_id);
            let bytes = bincode::serde::encode_to_vec(session, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&snap_tbl, &key[..], &bytes, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        // Clear segment ranges — snapshot replaces all state
        let sr_tbl = unsafe { self.segment_ranges_table_rw(&txn) };
        txn.clear_table(&sr_tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    // -- Segment range operations --

    /// Write segment range entries for a sealed segment.
    ///
    /// Each entry is `(entity_type, entity_id, record_count, total_bytes)` for
    /// the given `segment_id`. Uses the DUP_SORT multimap: one key per entity,
    /// multiple segment range values sorted by segment_id.
    fn write_segment_ranges(
        &self,
        segment_id: u64,
        entries: &[(u8, u64, u64, u64)],
    ) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let tbl = unsafe { self.segment_ranges_table_rw(&txn) };

        for &(entity_type, entity_id, record_count, total_bytes) in entries {
            let key = segment_range_key(entity_type, entity_id);
            let val = segment_range_value(segment_id, record_count, total_bytes);
            txn.put(&tbl, &key[..], &val[..], WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    /// Read all segment ranges for a given entity.
    ///
    /// Returns `Vec<(segment_id, record_count, total_bytes)>` sorted by segment_id.
    fn read_segment_ranges(
        &self,
        entity_type: u8,
        entity_id: u64,
    ) -> io::Result<Vec<(u64, u64, u64)>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let tbl = unsafe { self.segment_ranges_table_ro(&txn) };

        let key = segment_range_key(entity_type, entity_id);
        let mut cursor = txn
            .cursor(&tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut results = Vec::new();

        // Position at the first dup value for this key
        let first: Option<std::borrow::Cow<[u8]>> = match cursor.set(&key[..]) {
            Ok(v) => v,
            Err(_) => return Ok(results),
        };

        if let Some(val) = first {
            if val.len() == SEGMENT_RANGE_VALUE_SIZE {
                results.push(decode_segment_range_value(&val));
            }
        }

        // Iterate remaining dups
        loop {
            match cursor.next_dup::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>() {
                Ok(Some((_k, val))) => {
                    if val.len() == SEGMENT_RANGE_VALUE_SIZE {
                        results.push(decode_segment_range_value(&val));
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }

        Ok(results)
    }

    /// Delete all segment range entries for a specific segment_id across all entities.
    ///
    /// Used during segment purge/GC when a raft segment is removed.
    fn delete_segment_ranges_by_segment(&self, segment_id: u64) -> io::Result<usize> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let tbl = unsafe { self.segment_ranges_table_rw(&txn) };

        let mut cursor = txn
            .cursor(&tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // For DUP_FIXED tables, get_both_range requires a full-size value.
        // Build a 24-byte search value with segment_id and zeros for the rest,
        // which seeks to the first value with this segment_id.
        let target_val = segment_range_value(segment_id, 0, 0);
        let mut deleted = 0usize;

        // Collect distinct keys first to avoid cursor invalidation during delete
        let mut keys_to_check: Vec<[u8; 9]> = Vec::new();
        {
            let iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
            let mut last_key = [0u8; 9];
            for item in iter {
                let (k, _) = match item {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                if k.len() == 9 {
                    let mut key = [0u8; 9];
                    key.copy_from_slice(&k);
                    if key != last_key {
                        keys_to_check.push(key);
                        last_key = key;
                    }
                }
            }
        }

        // Re-open cursor for deletion pass
        drop(cursor);
        let mut cursor = txn
            .cursor(&tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        for key in &keys_to_check {
            // Seek to first value >= target_val for this key
            let found: Option<std::borrow::Cow<[u8]>> =
                match cursor.get_both_range(&key[..], &target_val) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

            if let Some(val) = found {
                if val.len() == SEGMENT_RANGE_VALUE_SIZE {
                    let found_seg_id = u64::from_le_bytes(val[..8].try_into().unwrap());
                    if found_seg_id == segment_id {
                        let _ = cursor.del(WriteFlags::empty());
                        deleted += 1;
                    }
                }
            }
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(deleted)
    }
}

/// Key for segment_ranges multimap: `[entity_type:1][entity_id:8 BE]`.
fn segment_range_key(entity_type: u8, entity_id: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = entity_type;
    key[1..9].copy_from_slice(&entity_id.to_be_bytes());
    key
}

const SEGMENT_RANGE_VALUE_SIZE: usize = 24;

/// Value for segment_ranges multimap: `[segment_id:8 LE][record_count:8 LE][total_bytes:8 LE]`.
fn segment_range_value(segment_id: u64, record_count: u64, total_bytes: u64) -> [u8; 24] {
    let mut val = [0u8; 24];
    val[0..8].copy_from_slice(&segment_id.to_le_bytes());
    val[8..16].copy_from_slice(&record_count.to_le_bytes());
    val[16..24].copy_from_slice(&total_bytes.to_le_bytes());
    val
}

/// Decode a segment_ranges value into `(segment_id, record_count, total_bytes)`.
fn decode_segment_range_value(val: &[u8]) -> (u64, u64, u64) {
    let segment_id = u64::from_le_bytes(val[0..8].try_into().unwrap());
    let record_count = u64::from_le_bytes(val[8..16].try_into().unwrap());
    let total_bytes = u64::from_le_bytes(val[16..24].try_into().unwrap());
    (segment_id, record_count, total_bytes)
}

// =============================================================================
// MqManifestManager
// =============================================================================

/// Thread-safe MDBX manifest manager with a dedicated write thread.
///
/// Used only for snapshot installs (new/lagging nodes). Normal recovery
/// replays the raft log from the beginning without touching MDBX.
pub struct MqManifestManager {
    tx: MAsyncTx<Array<ManifestCommand>>,
    read_envs: Arc<RwLock<HashMap<u64, Arc<GroupMdbxEnv>>>>,
    base_dir: PathBuf,
    shutdown: Arc<AtomicBool>,
    worker_thread: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl MqManifestManager {
    pub fn new(base_dir: &Path) -> io::Result<Self> {
        let base_dir = base_dir.to_path_buf();
        let (tx, rx) = crossfire::mpsc::bounded_async_blocking::<ManifestCommand>(4096);
        let shutdown = Arc::new(AtomicBool::new(false));
        let read_envs: Arc<RwLock<HashMap<u64, Arc<GroupMdbxEnv>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let shutdown_clone = shutdown.clone();
        let envs_clone = read_envs.clone();
        let handle = std::thread::Builder::new()
            .name("mq-manifest-mdbx".to_string())
            .spawn(move || {
                Self::worker_loop(rx, shutdown_clone, envs_clone);
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        info!(path = %base_dir.display(), "MQ manifest manager started");

        Ok(Self {
            tx,
            read_envs,
            base_dir,
            shutdown,
            worker_thread: Arc::new(std::sync::Mutex::new(Some(handle))),
        })
    }

    pub fn open_group(&self, group_id: u64) -> io::Result<()> {
        let path = self.base_dir.join(format!(".mq_groups/{}", group_id));
        let env = Arc::new(GroupMdbxEnv::open(&path)?);
        self.read_envs.write().insert(group_id, env);
        info!(group_id, "MQ manifest group opened");
        Ok(())
    }

    pub fn close_group(&self, group_id: u64) {
        self.read_envs.write().remove(&group_id);
    }

    /// Read structural state (entities + purge floor) for recovery.
    pub(crate) fn read_structural_state(
        &self,
        group_id: u64,
    ) -> io::Result<Option<StructuralState>> {
        let envs = self.read_envs.read();
        let env = match envs.get(&group_id) {
            Some(e) => e,
            None => return Ok(None),
        };
        env.read_structural_state()
    }

    /// Fire-and-forget structural write. Does not block on MDBX commit.
    pub(crate) fn structural_update_fire_and_forget(
        &self,
        group_id: u64,
        log_index: u64,
        next_id: u64,
        write: StructuralWrite,
    ) {
        let _ = self.tx.try_send(ManifestCommand::StructuralUpdate {
            group_id,
            log_index,
            next_id,
            write,
        });
    }

    /// Fire-and-forget write of segment ranges when a segment is sealed.
    pub(crate) fn sealed_segment_fire_and_forget(
        &self,
        group_id: u64,
        segment_id: u64,
        entity_summaries: Vec<(u8, u64, u64, u64)>,
    ) {
        let _ = self.tx.try_send(ManifestCommand::SealedSegment {
            group_id,
            segment_id,
            entity_summaries,
        });
    }

    /// Fire-and-forget deletion of segment ranges when a segment is purged.
    pub(crate) fn purge_segment_fire_and_forget(&self, group_id: u64, segment_id: u64) {
        let _ = self.tx.try_send(ManifestCommand::PurgeSegment {
            group_id,
            segment_id,
        });
    }

    /// Fire-and-forget persist of detached retained messages for an exchange.
    /// Called after sweep detaches retained messages from purged mmap segments.
    pub(crate) fn persist_retained_fire_and_forget(
        &self,
        group_id: u64,
        exchange_id: u64,
        entries: Vec<(String, Vec<u8>)>,
    ) {
        let _ = self.tx.try_send(ManifestCommand::PersistRetained {
            group_id,
            exchange_id,
            entries,
        });
    }

    /// Read all segment ranges for a given entity (concurrent reader-safe).
    ///
    /// Returns `Vec<(segment_id, record_count, total_bytes)>` sorted by segment_id.
    pub fn read_segment_ranges(
        &self,
        group_id: u64,
        entity_type: u8,
        entity_id: u64,
    ) -> io::Result<Vec<(u64, u64, u64)>> {
        let envs = self.read_envs.read();
        let env = match envs.get(&group_id) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };
        env.read_segment_ranges(entity_type, entity_id)
    }

    /// Read persisted raft state for a group.
    pub fn read_applied_state(
        &self,
        group_id: u64,
    ) -> io::Result<Option<(Option<LogId<MqTypeConfig>>, StoredMembership<MqTypeConfig>)>> {
        let envs = self.read_envs.read();
        let env = match envs.get(&group_id) {
            Some(e) => e,
            None => return Ok(None),
        };
        match env.read_group_meta()? {
            Some(meta) => Ok(Some(meta.to_raft()?)),
            None => Ok(None),
        }
    }

    /// Read persisted snapshot data for a group.
    /// Reads individual entity records from the snapshot table.
    pub fn read_snapshot_data(&self, group_id: u64) -> io::Result<Option<MqSnapshotData>> {
        let envs = self.read_envs.read();
        let env = match envs.get(&group_id) {
            Some(e) => e,
            None => return Ok(None),
        };
        env.read_snapshot_data()
    }

    pub(crate) async fn install_snapshot(
        &self,
        group_id: u64,
        meta: GroupMeta,
        snapshot_data: MqSnapshotData,
    ) -> io::Result<()> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ManifestCommand::InstallSnapshot {
                group_id,
                meta,
                snapshot_data,
                done: Some(done_tx),
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manifest worker gone"))?;
        done_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manifest done channel dropped"))
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(mut guard) = self.worker_thread.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
    }

    fn worker_loop(
        rx: Rx<Array<ManifestCommand>>,
        shutdown: Arc<AtomicBool>,
        envs: Arc<RwLock<HashMap<u64, Arc<GroupMdbxEnv>>>>,
    ) {
        while !shutdown.load(Ordering::Acquire) {
            let cmd = match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(cmd) => cmd,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            };

            // Process this command, then drain any others immediately available
            let mut batch = vec![cmd];
            loop {
                match rx.try_recv() {
                    Ok(cmd) => batch.push(cmd),
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                }
            }

            for cmd in batch {
                match cmd {
                    ManifestCommand::StructuralUpdate {
                        group_id,
                        log_index,
                        next_id,
                        write,
                    } => {
                        let envs_read = envs.read();
                        if let Some(env) = envs_read.get(&group_id) {
                            if let Err(e) = env.apply_structural_write(log_index, next_id, &write) {
                                error!(group_id, error = %e, "MQ manifest structural write failed");
                            }
                        }
                    }
                    ManifestCommand::InstallSnapshot {
                        group_id,
                        meta,
                        snapshot_data,
                        done,
                    } => {
                        let envs_read = envs.read();
                        if let Some(env) = envs_read.get(&group_id) {
                            if let Err(e) = env.install_snapshot(&meta, &snapshot_data) {
                                error!(group_id, error = %e, "MQ manifest install snapshot failed");
                            }
                        }
                        if let Some(done) = done {
                            let _ = done.send(());
                        }
                    }
                    ManifestCommand::SealedSegment {
                        group_id,
                        segment_id,
                        entity_summaries,
                    } => {
                        let envs_read = envs.read();
                        if let Some(env) = envs_read.get(&group_id) {
                            if let Err(e) = env.write_segment_ranges(segment_id, &entity_summaries)
                            {
                                error!(
                                    group_id,
                                    segment_id,
                                    error = %e,
                                    "MQ manifest segment range write failed"
                                );
                            }
                        }
                    }
                    ManifestCommand::PurgeSegment {
                        group_id,
                        segment_id,
                    } => {
                        let envs_read = envs.read();
                        if let Some(env) = envs_read.get(&group_id) {
                            if let Err(e) = env.delete_segment_ranges_by_segment(segment_id) {
                                error!(
                                    group_id,
                                    segment_id,
                                    error = %e,
                                    "MQ manifest segment range delete failed"
                                );
                            }
                        }
                    }
                    ManifestCommand::PersistRetained {
                        group_id,
                        exchange_id,
                        entries,
                    } => {
                        let envs_read = envs.read();
                        if let Some(env) = envs_read.get(&group_id) {
                            if let Err(e) = env.persist_retained(exchange_id, &entries) {
                                error!(
                                    group_id,
                                    exchange_id,
                                    error = %e,
                                    "MQ manifest persist retained failed"
                                );
                            }
                        }
                    }
                }
            }
        }

        debug!("MQ manifest worker thread exiting");
    }
}

impl Drop for MqManifestManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MqConfig;
    use crate::engine::MqEngine;
    use crate::flat::FlatMessageBuilder;
    use crate::types::{MqCommand, RetentionPolicy};
    use std::sync::atomic::Ordering;

    fn make_msg(value: &[u8]) -> bytes::Bytes {
        FlatMessageBuilder::new(bytes::Bytes::from(value.to_vec()))
            .timestamp(1000)
            .build()
    }

    #[test]
    fn test_group_mdbx_env_open_and_read_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        assert!(env.read_group_meta().unwrap().is_none());
        assert!(env.read_snapshot_data().unwrap().is_none());
    }

    #[test]
    fn test_group_mdbx_env_install_snapshot_and_read() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let meta = GroupMeta {
            last_applied_term: 2,
            last_applied_node_id: 1,
            last_applied_index: 50,
            last_applied_present: true,
            membership_bytes: Vec::new(),
        };

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"t1".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        let snap = engine.snapshot();
        env.install_snapshot(&meta, &snap).unwrap();

        let read_meta = env.read_group_meta().unwrap().unwrap();
        assert_eq!(read_meta.last_applied_index, 50);

        let read_snap = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read_snap.topics.len(), 1);
        assert_eq!(read_snap.topics[0].meta.name, "t1");
        assert_eq!(
            read_snap.next_id,
            engine.metadata().next_id.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn test_group_mdbx_env_install_snapshot_clears_old() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let meta1 = GroupMeta {
            last_applied_term: 1,
            last_applied_node_id: 1,
            last_applied_index: 10,
            last_applied_present: true,
            membership_bytes: Vec::new(),
        };
        let mut engine1 = MqEngine::new(MqConfig::new("/tmp/test"));
        engine1.apply_command(
            &MqCommand::create_topic(&"old_topic".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        env.install_snapshot(&meta1, &engine1.snapshot()).unwrap();

        let meta2 = GroupMeta {
            last_applied_term: 2,
            last_applied_node_id: 1,
            last_applied_index: 50,
            last_applied_present: true,
            membership_bytes: Vec::new(),
        };
        let mut engine2 = MqEngine::new(MqConfig::new("/tmp/test"));
        engine2.apply_command(
            &MqCommand::create_topic(&"new_topic".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        engine2.apply_command(
            &MqCommand::create_topic(&"t2".to_string(), RetentionPolicy::default(), 0),
            2,
            1001,
            None,
        );
        env.install_snapshot(&meta2, &engine2.snapshot()).unwrap();

        let read_meta = env.read_group_meta().unwrap().unwrap();
        assert_eq!(read_meta.last_applied_index, 50);

        let read_snap = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read_snap.topics.len(), 2);
        assert_eq!(read_snap.topics[0].meta.name, "new_topic");
    }

    #[test]
    fn test_group_meta_roundtrip_present() {
        let meta = GroupMeta {
            last_applied_term: 5,
            last_applied_node_id: 3,
            last_applied_index: 100,
            last_applied_present: true,
            membership_bytes: Vec::new(),
        };

        let bytes = bincode::serde::encode_to_vec(&meta, bincode::config::standard()).unwrap();
        let (decoded, _): (GroupMeta, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.last_applied_term, 5);
        assert_eq!(decoded.last_applied_index, 100);
        assert!(decoded.last_applied_present);
    }

    #[test]
    fn test_group_meta_roundtrip_absent() {
        let meta = GroupMeta {
            last_applied_term: 0,
            last_applied_node_id: 0,
            last_applied_index: 0,
            last_applied_present: false,
            membership_bytes: Vec::new(),
        };

        let bytes = bincode::serde::encode_to_vec(&meta, bincode::config::standard()).unwrap();
        let (decoded, _): (GroupMeta, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert!(!decoded.last_applied_present);
    }

    #[tokio::test]
    async fn test_manifest_manager_open_group_and_read() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();

        mgr.open_group(1).unwrap();

        assert!(mgr.read_applied_state(1).unwrap().is_none());
        assert!(mgr.read_snapshot_data(1).unwrap().is_none());

        mgr.shutdown();
    }

    #[tokio::test]
    async fn test_manifest_manager_snapshot_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();
        mgr.open_group(1).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"events".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        engine.apply_command(
            &MqCommand::publish(1, &vec![make_msg(b"hello")]),
            2,
            1001,
            None,
        );

        let snap = engine.snapshot();

        let meta = GroupMeta {
            last_applied_term: 1,
            last_applied_node_id: 1,
            last_applied_index: 2,
            last_applied_present: true,
            membership_bytes: Vec::new(),
        };

        mgr.install_snapshot(1, meta, snap).await.unwrap();

        let read_snap = mgr.read_snapshot_data(1).unwrap().unwrap();
        assert_eq!(read_snap.topics.len(), 1);
        assert_eq!(read_snap.topics[0].meta.name, "events");

        mgr.shutdown();
    }

    #[tokio::test]
    async fn test_manifest_manager_close_group() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();
        mgr.open_group(1).unwrap();
        mgr.close_group(1);

        assert!(mgr.read_applied_state(1).unwrap().is_none());
        mgr.shutdown();
    }

    #[tokio::test]
    async fn test_manifest_manager_nonexistent_group() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();

        assert!(mgr.read_applied_state(999).unwrap().is_none());
        assert!(mgr.read_snapshot_data(999).unwrap().is_none());

        mgr.shutdown();
    }

    // =========================================================================
    // Comprehensive MDBX snapshot streaming tests
    // =========================================================================

    fn make_meta(index: u64) -> GroupMeta {
        let membership = openraft::StoredMembership::<crate::MqTypeConfig>::default();
        let membership_bytes =
            bincode::serde::encode_to_vec(&membership, bincode::config::standard())
                .unwrap_or_default();
        GroupMeta {
            last_applied_term: 1,
            last_applied_node_id: 1,
            last_applied_index: index,
            last_applied_present: true,
            membership_bytes,
        }
    }

    /// Topic and consumer group entity types are correctly persisted and read back.
    #[test]
    fn test_snapshot_all_entity_types_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));

        // Topic
        engine.apply_command(
            &MqCommand::create_topic(&"events".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        engine.apply_command(
            &MqCommand::publish(1, &vec![make_msg(b"m1"), make_msg(b"m2")]),
            2,
            1001,
            None,
        );

        let snap = engine.snapshot();
        assert_eq!(snap.topics.len(), 1);

        env.install_snapshot(&make_meta(2), &snap).unwrap();

        let read = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read.topics.len(), 1);
        assert_eq!(read.topics[0].meta.name, "events");
        assert_eq!(read.topics[0].meta.message_count, 2);

        assert_eq!(
            read.next_id,
            engine.metadata().next_id.load(Ordering::Relaxed)
        );
    }

    /// Multiple topics survive the MDBX roundtrip.
    #[test]
    fn test_snapshot_multiple_topics_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"t1".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        engine.apply_command(
            &MqCommand::create_topic(&"t2".to_string(), RetentionPolicy::default(), 0),
            2,
            1001,
            None,
        );

        let snap = engine.snapshot();
        assert_eq!(snap.topics.len(), 2);

        env.install_snapshot(&make_meta(2), &snap).unwrap();

        let read = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read.topics.len(), 2);
    }

    /// Installing a snapshot clears previous structural writes.
    #[test]
    fn test_snapshot_install_clears_structural_state() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Write structural data first
        let mut tmp_engine = MqEngine::new(MqConfig::new("/tmp/test"));
        tmp_engine.apply_command(
            &MqCommand::create_topic(
                &"structural-topic".to_string(),
                RetentionPolicy::default(),
                0,
            ),
            1,
            1000,
            None,
        );
        let topic_meta = tmp_engine
            .metadata()
            .topics
            .pin()
            .get(&1)
            .unwrap()
            .snapshot_meta();
        env.apply_structural_write(1, 2, &StructuralWrite::CreateTopic(topic_meta))
            .unwrap();

        // Verify structural state exists
        let structural = env.read_structural_state().unwrap().unwrap();
        assert_eq!(structural.topics.len(), 1);

        // Install snapshot — should clear the entities table
        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"snap-topic".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        env.install_snapshot(&make_meta(5), &engine.snapshot())
            .unwrap();

        // Structural state should be gone (entities table was cleared)
        let structural = env.read_structural_state().unwrap();
        assert!(
            structural.is_none(),
            "structural state should be cleared after snapshot install"
        );

        // Snapshot data should be available
        let snap = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(snap.topics.len(), 1);
        assert_eq!(snap.topics[0].meta.name, "snap-topic");
    }

    /// Structural writes after snapshot don't corrupt the snapshot table.
    #[test]
    fn test_structural_writes_after_snapshot_dont_corrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"snap-topic".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        env.install_snapshot(&make_meta(1), &engine.snapshot())
            .unwrap();

        // Now apply structural writes (simulates post-snapshot operation)
        let mut tmp_engine2 = MqEngine::new(MqConfig::new("/tmp/test"));
        tmp_engine2.apply_command(
            &MqCommand::create_topic(&"new-topic".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        let topic_meta = tmp_engine2
            .metadata()
            .topics
            .pin()
            .get(&1)
            .unwrap()
            .snapshot_meta();
        env.apply_structural_write(2, 100, &StructuralWrite::CreateTopic(topic_meta))
            .unwrap();

        // Snapshot data should still be intact (separate table)
        let snap = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(snap.topics.len(), 1);
        assert_eq!(snap.topics[0].meta.name, "snap-topic");
    }

    /// Install empty snapshot (no entities at all).
    #[test]
    fn test_snapshot_install_empty_entities() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let engine = MqEngine::new(MqConfig::new("/tmp/test"));
        let snap = engine.snapshot();
        assert_eq!(snap.topics.len(), 0);

        env.install_snapshot(&make_meta(0), &snap).unwrap();

        // Empty snapshot should return None (no records found)
        let read = env.read_snapshot_data().unwrap();
        assert!(read.is_none(), "empty snapshot should return None");
    }

    /// next_id is preserved correctly across snapshot install/read.
    #[test]
    fn test_snapshot_next_id_preserved() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        // Create 5 entities to advance next_id
        for i in 0..5 {
            engine.apply_command(
                &MqCommand::create_topic(&format!("t{}", i), RetentionPolicy::default(), 0),
                i + 1,
                1000,
                None,
            );
        }
        let snap = engine.snapshot();
        let expected_next_id = snap.next_id;
        assert!(
            expected_next_id >= 6,
            "next_id should be at least 6 after 5 creates"
        );

        env.install_snapshot(&make_meta(5), &snap).unwrap();

        let read = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read.next_id, expected_next_id);
    }

    /// Multiple groups can each have independent snapshot data.
    #[tokio::test]
    async fn test_multiple_groups_independent_snapshots() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();
        mgr.open_group(1).unwrap();
        mgr.open_group(2).unwrap();

        let mut engine1 = MqEngine::new(MqConfig::new("/tmp/test"));
        engine1.apply_command(
            &MqCommand::create_topic(&"group1-topic".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        mgr.install_snapshot(1, make_meta(1), engine1.snapshot())
            .await
            .unwrap();

        let mut engine2 = MqEngine::new(MqConfig::new("/tmp/test"));
        engine2.apply_command(
            &MqCommand::create_topic(&"group2-topic".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        mgr.install_snapshot(2, make_meta(1), engine2.snapshot())
            .await
            .unwrap();

        // Each group has its own data
        let snap1 = mgr.read_snapshot_data(1).unwrap().unwrap();
        assert_eq!(snap1.topics.len(), 1);
        assert_eq!(snap1.topics[0].meta.name, "group1-topic");

        let snap2 = mgr.read_snapshot_data(2).unwrap().unwrap();
        assert_eq!(snap2.topics.len(), 1);
        assert_eq!(snap2.topics[0].meta.name, "group2-topic");

        mgr.shutdown();
    }

    /// Many entities of each type to verify cursor scan handles large datasets.
    #[test]
    fn test_snapshot_many_entities() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        for i in 0..50 {
            engine.apply_command(
                &MqCommand::create_topic(&format!("topic-{}", i), RetentionPolicy::default(), 0),
                i + 1,
                1000,
                None,
            );
        }
        let snap = engine.snapshot();
        env.install_snapshot(&make_meta(50), &snap).unwrap();

        let read = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read.topics.len(), 50);
    }

    /// Successive snapshot installs fully replace previous data.
    #[test]
    fn test_successive_snapshot_installs_replace_data() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // First snapshot: 3 topics
        let mut engine1 = MqEngine::new(MqConfig::new("/tmp/test"));
        for i in 0..3 {
            engine1.apply_command(
                &MqCommand::create_topic(&format!("old-{}", i), RetentionPolicy::default(), 0),
                i + 1,
                1000,
                None,
            );
        }
        env.install_snapshot(&make_meta(3), &engine1.snapshot())
            .unwrap();
        let read = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read.topics.len(), 3);

        // Second snapshot: 1 topic (different name)
        let mut engine2 = MqEngine::new(MqConfig::new("/tmp/test"));
        engine2.apply_command(
            &MqCommand::create_topic(&"replacement-t".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        env.install_snapshot(&make_meta(10), &engine2.snapshot())
            .unwrap();

        let read = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read.topics.len(), 1, "old topics should be replaced");
        assert_eq!(read.topics[0].meta.name, "replacement-t");
    }

    /// Topic consumer offsets survive the MDBX roundtrip.
    #[test]
    fn test_snapshot_topic_consumer_offsets() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"t".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        engine.apply_command(
            &MqCommand::publish(1, &vec![make_msg(b"a"), make_msg(b"b"), make_msg(b"c")]),
            2,
            1001,
            None,
        );
        // Commit an offset for consumer 42
        engine.apply_command(&MqCommand::commit_offset(1, 42, 2), 3, 1002, None);

        let snap = engine.snapshot();
        assert!(!snap.topics[0].consumer_offsets.is_empty());

        env.install_snapshot(&make_meta(3), &snap).unwrap();

        let read = env.read_snapshot_data().unwrap().unwrap();
        assert_eq!(read.topics[0].consumer_offsets.len(), 1);
        assert_eq!(read.topics[0].consumer_offsets[0].consumer_id, 42);
        assert_eq!(read.topics[0].consumer_offsets[0].committed_offset, 2);
    }

    /// Full engine restore from MDBX-persisted snapshot produces identical state.
    #[test]
    fn test_snapshot_mdbx_restore_matches_original() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"t".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        engine.apply_command(&MqCommand::publish(1, &vec![make_msg(b"a")]), 2, 1001, None);

        let original_snap = engine.snapshot();
        env.install_snapshot(&make_meta(2), &original_snap).unwrap();

        let read_snap = env.read_snapshot_data().unwrap().unwrap();

        // Restore into a fresh engine and verify state matches
        let mut restored = MqEngine::new(MqConfig::new("/tmp/test2"));
        restored.restore(read_snap);

        let restored_snap = restored.snapshot();
        assert_eq!(restored_snap.topics.len(), original_snap.topics.len());
        assert_eq!(
            restored_snap.topics[0].meta.name,
            original_snap.topics[0].meta.name
        );
        assert_eq!(
            restored_snap.topics[0].meta.message_count,
            original_snap.topics[0].meta.message_count
        );
        assert_eq!(restored_snap.next_id, original_snap.next_id);
    }

    /// Snapshot file_manifest and sync_addr are NOT persisted to MDBX
    /// (they're runtime-only, stripped on write).
    #[test]
    fn test_snapshot_file_manifest_not_persisted() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"t".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );
        let mut snap = engine.snapshot();
        snap.file_manifest = vec![bisque_raft::SnapshotFileEntry {
            relative_path: "seg_000001.log".to_string(),
            size: 4096,
        }];
        snap.sync_addr = Some("10.0.0.1:5555".to_string());

        env.install_snapshot(&make_meta(1), &snap).unwrap();

        let read = env.read_snapshot_data().unwrap().unwrap();
        assert!(
            read.file_manifest.is_empty(),
            "file_manifest should not be persisted"
        );
        assert!(
            read.sync_addr.is_none(),
            "sync_addr should not be persisted"
        );
        assert_eq!(read.topics.len(), 1, "entity data should still be there");
    }

    /// Concurrent manifest manager install + read across groups.
    #[tokio::test]
    async fn test_manifest_manager_concurrent_groups() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();

        for gid in 1..=5 {
            mgr.open_group(gid).unwrap();
        }

        // Install snapshots on all groups
        for gid in 1..=5u64 {
            let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
            engine.apply_command(
                &MqCommand::create_topic(
                    &format!("group-{}-topic", gid),
                    RetentionPolicy::default(),
                    0,
                ),
                1,
                1000,
                None,
            );
            // Must install synchronously since manifest requires &self
            mgr.install_snapshot(gid, make_meta(1), engine.snapshot())
                .await
                .unwrap();
        }

        // Read all back
        for gid in 1..=5u64 {
            let snap = mgr.read_snapshot_data(gid).unwrap().unwrap();
            assert_eq!(snap.topics.len(), 1);
            assert_eq!(snap.topics[0].meta.name, format!("group-{}-topic", gid));
        }

        mgr.shutdown();
    }

    /// Structural writes followed by snapshot install: the applied_state
    /// should return the snapshot metadata, not the structural floor.
    #[tokio::test]
    async fn test_snapshot_install_updates_applied_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();
        mgr.open_group(1).unwrap();

        let mut engine = MqEngine::new(MqConfig::new("/tmp/test"));
        engine.apply_command(
            &MqCommand::create_topic(&"t".to_string(), RetentionPolicy::default(), 0),
            1,
            1000,
            None,
        );

        let mut meta = make_meta(100);
        meta.last_applied_term = 3;
        meta.last_applied_node_id = 2;

        mgr.install_snapshot(1, meta, engine.snapshot())
            .await
            .unwrap();

        let (last_applied, _membership) = mgr.read_applied_state(1).unwrap().unwrap();
        let la = last_applied.unwrap();
        assert_eq!(la.index, 100);
        assert_eq!(la.leader_id.term, 3);
        assert_eq!(la.leader_id.node_id, 2);

        mgr.shutdown();
    }

    // =========================================================================
    // Segment range MDBX multimap tests
    // =========================================================================

    #[test]
    fn test_segment_range_key_encoding() {
        let key = segment_range_key(0, 42);
        assert_eq!(key[0], 0); // entity_type
        assert_eq!(u64::from_be_bytes(key[1..9].try_into().unwrap()), 42);
    }

    #[test]
    fn test_segment_range_value_roundtrip() {
        let val = segment_range_value(100, 500, 65536);
        let (seg, rec, bytes) = decode_segment_range_value(&val);
        assert_eq!(seg, 100);
        assert_eq!(rec, 500);
        assert_eq!(bytes, 65536);
    }

    #[test]
    fn test_segment_ranges_write_and_read() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Write ranges for segment 1: topic 10 has 100 records / 4096 bytes
        env.write_segment_ranges(1, &[(0, 10, 100, 4096)]).unwrap();

        // Write ranges for segment 2: topic 10 has 50 records / 2048 bytes
        env.write_segment_ranges(2, &[(0, 10, 50, 2048)]).unwrap();

        let ranges = env.read_segment_ranges(0, 10).unwrap();
        assert_eq!(ranges.len(), 2);
        // Values sorted by segment_id (LE bytes)
        assert_eq!(ranges[0], (1, 100, 4096));
        assert_eq!(ranges[1], (2, 50, 2048));
    }

    #[test]
    fn test_segment_ranges_multiple_entities() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Segment 1 has data for topic 1 and queue 2
        env.write_segment_ranges(1, &[(0, 1, 10, 1000), (1, 2, 5, 500)])
            .unwrap();

        let topic_ranges = env.read_segment_ranges(0, 1).unwrap();
        assert_eq!(topic_ranges.len(), 1);
        assert_eq!(topic_ranges[0], (1, 10, 1000));

        let queue_ranges = env.read_segment_ranges(1, 2).unwrap();
        assert_eq!(queue_ranges.len(), 1);
        assert_eq!(queue_ranges[0], (1, 5, 500));

        // Non-existent entity
        let empty = env.read_segment_ranges(0, 999).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_segment_ranges_delete_by_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Two segments with data for topic 1
        env.write_segment_ranges(1, &[(0, 1, 10, 1000)]).unwrap();
        env.write_segment_ranges(2, &[(0, 1, 20, 2000)]).unwrap();
        // Segment 1 also has data for queue 2
        env.write_segment_ranges(1, &[(1, 2, 5, 500)]).unwrap();

        // Delete segment 1
        let deleted = env.delete_segment_ranges_by_segment(1).unwrap();
        assert_eq!(deleted, 2); // topic 1 + queue 2

        // Topic 1 should only have segment 2 left
        let ranges = env.read_segment_ranges(0, 1).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (2, 20, 2000));

        // Queue 2 should be empty
        let ranges = env.read_segment_ranges(1, 2).unwrap();
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_segment_ranges_cleared_on_snapshot_install() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Write some segment ranges
        env.write_segment_ranges(1, &[(0, 1, 10, 1000)]).unwrap();
        env.write_segment_ranges(2, &[(0, 1, 20, 2000)]).unwrap();

        assert_eq!(env.read_segment_ranges(0, 1).unwrap().len(), 2);

        // Install snapshot — should clear segment ranges
        let engine = MqEngine::new(MqConfig::new("/tmp/test"));
        env.install_snapshot(&make_meta(0), &engine.snapshot())
            .unwrap();

        let ranges = env.read_segment_ranges(0, 1).unwrap();
        assert!(
            ranges.is_empty(),
            "segment ranges should be cleared after snapshot install"
        );
    }

    #[test]
    fn test_segment_ranges_many_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Write 100 segments worth of data for a single entity
        for seg in 1..=100u64 {
            env.write_segment_ranges(seg, &[(0, 1, seg * 10, seg * 100)])
                .unwrap();
        }

        let ranges = env.read_segment_ranges(0, 1).unwrap();
        assert_eq!(ranges.len(), 100);

        // Should be sorted by segment_id
        for (i, &(seg_id, rec_count, total_bytes)) in ranges.iter().enumerate() {
            let expected_seg = (i + 1) as u64;
            assert_eq!(seg_id, expected_seg);
            assert_eq!(rec_count, expected_seg * 10);
            assert_eq!(total_bytes, expected_seg * 100);
        }
    }

    #[test]
    fn test_segment_ranges_empty_write() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Empty write is a no-op
        env.write_segment_ranges(1, &[]).unwrap();
        let ranges = env.read_segment_ranges(0, 1).unwrap();
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_segment_ranges_delete_nonexistent() {
        let tmp = tempfile::tempdir().unwrap();
        let env = GroupMdbxEnv::open(tmp.path()).unwrap();

        // Delete from empty table
        let deleted = env.delete_segment_ranges_by_segment(999).unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn test_manifest_manager_sealed_segment_fire_and_forget() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();
        mgr.open_group(1).unwrap();

        // Fire sealed segment update
        mgr.sealed_segment_fire_and_forget(1, 100, vec![(0, 1, 50, 4096), (1, 2, 30, 2048)]);

        // Give worker thread time to process
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let topic_ranges = mgr.read_segment_ranges(1, 0, 1).unwrap();
        assert_eq!(topic_ranges.len(), 1);
        assert_eq!(topic_ranges[0], (100, 50, 4096));

        let queue_ranges = mgr.read_segment_ranges(1, 1, 2).unwrap();
        assert_eq!(queue_ranges.len(), 1);
        assert_eq!(queue_ranges[0], (100, 30, 2048));

        mgr.shutdown();
    }

    #[tokio::test]
    async fn test_manifest_manager_purge_segment_fire_and_forget() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = MqManifestManager::new(tmp.path()).unwrap();
        mgr.open_group(1).unwrap();

        // Write two segments
        mgr.sealed_segment_fire_and_forget(1, 1, vec![(0, 1, 10, 1000)]);
        mgr.sealed_segment_fire_and_forget(1, 2, vec![(0, 1, 20, 2000)]);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert_eq!(mgr.read_segment_ranges(1, 0, 1).unwrap().len(), 2);

        // Purge segment 1
        mgr.purge_segment_fire_and_forget(1, 1);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let ranges = mgr.read_segment_ranges(1, 0, 1).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (2, 20, 2000));

        mgr.shutdown();
    }
}
