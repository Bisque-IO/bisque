//! MDBX-backed state machine manifest for crash-consistent metadata.
//!
//! Each Raft group gets its own MDBX database under `{base_dir}/.lance_groups/{group_id}/`.
//! Table keys are plain table name bytes (no group_id prefix needed since each
//! group has its own MDBX env).
//!
//! A single dedicated `std::thread` owns all open MDBX environments and
//! handles writes via crossfire queues. This gives natural write batching
//! and keeps fsync off tokio async threads.
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
use tracing::{debug, error, info, warn};

use crate::LanceTypeConfig;
use crate::catalog_events::CatalogEvent;
use crate::types::PersistedTableEntry;

const META_TABLE: &str = "lance_meta";
const TABLES_TABLE: &str = "lance_tables";
const WAL_TABLE: &str = "catalog_wal";
/// Key for the group meta record within the per-group MDBX.
const META_KEY: &[u8] = b"raft";
/// Maximum WAL size in bytes before compaction trims oldest entries.
const WAL_MAX_BYTES: u64 = 16 * 1024 * 1024; // 16 MiB

// =============================================================================
// Types
// =============================================================================

/// Persisted per-group global Raft metadata.
///
/// Flattens `LogId` fields to avoid coupling to openraft's internal layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GroupMeta {
    last_applied_term: u64,
    last_applied_node_id: u64,
    last_applied_index: u64,
    last_applied_present: bool,
    /// Bincode-encoded `StoredMembership<LanceTypeConfig>`.
    membership_bytes: Vec<u8>,
}

impl GroupMeta {
    pub(crate) fn from_raft(
        last_applied: &Option<LogId<LanceTypeConfig>>,
        last_membership: &StoredMembership<LanceTypeConfig>,
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

    fn to_raft(
        &self,
    ) -> io::Result<(
        Option<LogId<LanceTypeConfig>>,
        StoredMembership<LanceTypeConfig>,
    )> {
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
        let (membership, _): (StoredMembership<LanceTypeConfig>, _) =
            bincode::serde::decode_from_slice(&self.membership_bytes, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok((last_applied, membership))
    }
}

/// A table metadata change bundled with a manifest update.
pub(crate) enum TableUpdate {
    /// Upsert table metadata.
    Set {
        table_name: String,
        entry: PersistedTableEntry,
    },
    /// Remove table metadata.
    Remove { table_name: String },
}

/// Command sent to the manifest worker thread.
pub(crate) enum ManifestCommand {
    /// Standard apply-path update: group meta + optional table change.
    /// `meta` is optional — when `None`, only the table update is applied
    /// (existing meta is preserved).
    Apply {
        group_id: u64,
        meta: Option<GroupMeta>,
        table_update: Option<TableUpdate>,
        done: Option<tokio::sync::oneshot::Sender<()>>,
    },
    /// Bulk write for snapshot install: group meta + all tables.
    /// Clears existing table data for this group before writing.
    InstallSnapshot {
        group_id: u64,
        meta: GroupMeta,
        tables: HashMap<String, PersistedTableEntry>,
        done: Option<tokio::sync::oneshot::Sender<()>>,
    },
    /// Append catalog WAL events for client sync.
    AppendWalEvents {
        group_id: u64,
        events: Vec<CatalogEvent>,
    },
}

// Keep the old name as a public alias for backward compat in state_machine.rs.
pub(crate) type ManifestUpdate = ManifestCommand;

// =============================================================================
// GroupMdbxEnv — Per-Group MDBX Wrapper
// =============================================================================

/// Per-group MDBX database wrapper with cached DBI handles.
///
/// Each group gets its own MDBX environment. Keys are plain table name bytes
/// (no group_id prefix needed).
pub(crate) struct GroupMdbxEnv {
    db: Arc<Database<NoWriteMap>>,
    meta_dbi: u32,
    tables_dbi: u32,
    wal_dbi: u32,
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

        let tables_table = txn
            .create_table(Some(TABLES_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let tables_dbi = tables_table.dbi();
        drop(tables_table);

        let wal_table = txn
            .create_table(Some(WAL_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let wal_dbi = wal_table.dbi();
        drop(wal_table);

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(Self {
            db,
            meta_dbi,
            tables_dbi,
            wal_dbi,
        })
    }

    // -- DBI handle reconstruction (same pattern as raft manifest_mdbx.rs) --

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

    unsafe fn tables_table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.tables_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn tables_table_rw<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RW, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.tables_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn wal_table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.wal_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn wal_table_rw<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RW, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.wal_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    // -- Read operations (concurrent, called from any thread via Arc) --

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

    fn read_all_tables(&self) -> io::Result<HashMap<String, PersistedTableEntry>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.tables_table_ro(&txn) };
        let mut out: HashMap<String, PersistedTableEntry> = HashMap::new();

        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
        while let Some(Ok((k, v))) = iter.next() {
            if let Ok(table_name) = String::from_utf8(k.to_vec()) {
                match bincode::serde::decode_from_slice::<PersistedTableEntry, _>(
                    v.as_ref(),
                    bincode::config::standard(),
                ) {
                    Ok((entry, _)) => {
                        out.insert(table_name, entry);
                    }
                    Err(e) => {
                        warn!("Failed to decode table entry: {}", e);
                    }
                }
            }
        }
        Ok(out)
    }

    fn read_table(&self, table_name: &str) -> io::Result<Option<PersistedTableEntry>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.tables_table_ro(&txn) };
        match txn.get::<std::borrow::Cow<[u8]>>(&table, table_name.as_bytes()) {
            Ok(Some(bytes)) => {
                let (entry, _): (PersistedTableEntry, _) =
                    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        }
    }

    // -- WAL operations --

    /// Read all WAL events with sequence number > `since_seq`.
    fn read_wal_since(&self, since_seq: u64) -> io::Result<Vec<CatalogEvent>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.wal_table_ro(&txn) };

        let mut events = Vec::new();
        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Position cursor at the first key > since_seq.
        let start_key = (since_seq + 1).to_be_bytes();
        let iter = cursor.iter_from::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>(&start_key);
        for result in iter {
            match result {
                Ok((_, v)) => {
                    match bincode::serde::decode_from_slice::<CatalogEvent, _>(
                        v.as_ref(),
                        bincode::config::standard(),
                    ) {
                        Ok((event, _)) => events.push(event),
                        Err(e) => {
                            warn!("Failed to decode WAL event: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("WAL cursor error: {}", e);
                    break;
                }
            }
        }

        Ok(events)
    }

    /// Get the latest WAL sequence number, or 0 if the WAL is empty.
    fn latest_wal_seq(&self) -> io::Result<u64> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.wal_table_ro(&txn) };

        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Seek to last entry
        match cursor.last::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>() {
            Ok(Some((k, _))) => {
                if k.len() == 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&k);
                    Ok(u64::from_be_bytes(buf))
                } else {
                    Ok(0)
                }
            }
            Ok(None) => Ok(0),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        }
    }

    /// Append a WAL event (called by worker thread in a write transaction).
    fn append_wal_events(
        &self,
        txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
        events: &[CatalogEvent],
    ) -> io::Result<()> {
        let table = unsafe { self.wal_table_rw(txn) };
        for event in events {
            let key = event.seq.to_be_bytes();
            let value = bincode::serde::encode_to_vec(event, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&table, &key[..], &value, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        Ok(())
    }

    /// Delete WAL entries with sequence number <= `up_to_seq`.
    fn compact_wal(&self, up_to_seq: u64) -> io::Result<usize> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.wal_table_rw(&txn) };

        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut deleted = 0usize;
        let mut to_delete = Vec::new();

        let iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
        for result in iter {
            match result {
                Ok((k, _)) => {
                    if k.len() == 8 {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&k);
                        let seq = u64::from_be_bytes(buf);
                        if seq <= up_to_seq {
                            to_delete.push(seq);
                        } else {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        drop(cursor);

        for seq in &to_delete {
            let key = seq.to_be_bytes();
            match txn.del(&table, &key[..], None) {
                Ok(_) | Err(libmdbx::Error::NotFound) => {}
                Err(e) => {
                    return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                }
            }
            deleted += 1;
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(deleted)
    }

    /// Calculate total size of WAL entries in bytes (keys + values).
    fn wal_size_bytes(&self) -> io::Result<u64> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.wal_table_ro(&txn) };

        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut total: u64 = 0;
        let iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
        for result in iter {
            match result {
                Ok((k, v)) => {
                    total += k.len() as u64 + v.len() as u64;
                }
                Err(_) => break,
            }
        }

        Ok(total)
    }

    /// Compact WAL entries to fit within `max_bytes`.
    ///
    /// Walks from oldest, accumulates bytes to remove, and deletes entries
    /// until the remaining WAL fits within the limit.
    fn compact_wal_to_size(&self, max_bytes: u64) -> io::Result<usize> {
        let total = self.wal_size_bytes()?;
        if total <= max_bytes {
            return Ok(0);
        }

        let excess = total - max_bytes;
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.wal_table_rw(&txn) };

        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut accumulated: u64 = 0;
        let mut to_delete = Vec::new();
        let iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
        for result in iter {
            match result {
                Ok((k, v)) => {
                    let entry_size = k.len() as u64 + v.len() as u64;
                    if k.len() == 8 {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&k);
                        to_delete.push(u64::from_be_bytes(buf));
                    }
                    accumulated += entry_size;
                    if accumulated >= excess {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        drop(cursor);

        let deleted = to_delete.len();
        for seq in &to_delete {
            let key = seq.to_be_bytes();
            match txn.del(&table, &key[..], None) {
                Ok(_) | Err(libmdbx::Error::NotFound) => {}
                Err(e) => {
                    return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                }
            }
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(deleted)
    }

    // -- Write operations (called by worker thread only) --

    fn apply_coalesced(
        &self,
        meta: Option<&GroupMeta>,
        table_updates: &HashMap<String, Option<PersistedTableEntry>>,
    ) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        if let Some(meta) = meta {
            let meta_tbl = unsafe { self.meta_table_rw(&txn) };
            let value = bincode::serde::encode_to_vec(meta, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&meta_tbl, META_KEY, &value, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        if !table_updates.is_empty() {
            let tables_tbl = unsafe { self.tables_table_rw(&txn) };
            for (table_name, entry_opt) in table_updates {
                let key = table_name.as_bytes();
                match entry_opt {
                    Some(entry) => {
                        let value =
                            bincode::serde::encode_to_vec(entry, bincode::config::standard())
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                        txn.put(&tables_tbl, key, &value, WriteFlags::empty())
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    }
                    None => match txn.del(&tables_tbl, key, None) {
                        Ok(_) | Err(libmdbx::Error::NotFound) => {}
                        Err(e) => {
                            return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                        }
                    },
                }
            }
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    /// Install a snapshot: clear all existing tables, then write new data.
    fn install_snapshot(
        &self,
        meta: &GroupMeta,
        entries: &HashMap<String, PersistedTableEntry>,
    ) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Write meta
        let meta_tbl = unsafe { self.meta_table_rw(&txn) };
        let meta_value = bincode::serde::encode_to_vec(meta, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        txn.put(&meta_tbl, META_KEY, &meta_value, WriteFlags::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Clear existing tables
        let tables_tbl = unsafe { self.tables_table_rw(&txn) };
        txn.clear_table(&tables_tbl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Write new entries
        for (table_name, entry) in entries {
            let value = bincode::serde::encode_to_vec(entry, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(
                &tables_tbl,
                table_name.as_bytes(),
                &value,
                WriteFlags::empty(),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }
}

// =============================================================================
// LanceManifestManager — Public API with Worker Thread
// =============================================================================

/// Thread-safe MDBX manifest manager with a dedicated write thread.
///
/// Each Raft group gets its own MDBX database. The manager provides:
/// - `open_group()` / `close_group()` for lifecycle management
/// - Read methods that go directly to MDBX (read-only transactions)
/// - Write methods that send commands to the dedicated write thread
///
/// Shared via `Arc` across the state machine, engine, and raft node.
pub struct LanceManifestManager {
    tx: MAsyncTx<Array<ManifestCommand>>,
    /// Read-path access to per-group MDBX envs (concurrent readers).
    read_envs: Arc<RwLock<HashMap<u64, Arc<GroupMdbxEnv>>>>,
    /// Base directory for group MDBX databases.
    base_dir: PathBuf,
    shutdown: Arc<AtomicBool>,
    worker_thread: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl LanceManifestManager {
    /// Create a new manifest manager.
    ///
    /// Spawns the dedicated write thread. No groups are open yet —
    /// call [`open_group`] to open a group's MDBX database.
    pub fn new(base_dir: &Path) -> io::Result<Self> {
        let base_dir = base_dir.to_path_buf();
        let (tx, rx) = crossfire::mpsc::bounded_async_blocking::<ManifestCommand>(4096);
        let shutdown = Arc::new(AtomicBool::new(false));
        let read_envs: Arc<RwLock<HashMap<u64, Arc<GroupMdbxEnv>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let shutdown_clone = shutdown.clone();
        let envs_clone = read_envs.clone();
        let handle = std::thread::Builder::new()
            .name("lance-manifest-mdbx".to_string())
            .spawn(move || {
                Self::worker_loop(rx, shutdown_clone, envs_clone);
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        info!(path = %base_dir.display(), "Lance manifest manager started");

        Ok(Self {
            tx,
            read_envs,
            base_dir,
            shutdown,
            worker_thread: Arc::new(std::sync::Mutex::new(Some(handle))),
        })
    }

    /// Open (or create) backward-compatible from the old `open()` API.
    ///
    /// Opens a single shared MDBX and auto-opens group 0.
    pub fn open(base_dir: &Path) -> io::Result<Self> {
        let mgr = Self::new(base_dir)?;
        // Auto-open default group 0 at the legacy path for backward compatibility.
        let path = base_dir.join(".lance_manifest");
        let env = Arc::new(GroupMdbxEnv::open(&path)?);
        mgr.read_envs.write().insert(0, env);
        Ok(mgr)
    }

    /// Open a group's MDBX database.
    ///
    /// The database is stored at `{base_dir}/.lance_groups/{group_id}/`.
    /// The env is shared between the read path and the worker thread via Arc.
    pub fn open_group(&self, group_id: u64) -> io::Result<()> {
        if self.read_envs.read().contains_key(&group_id) {
            return Ok(());
        }

        let path = self.group_path(group_id);
        let env = Arc::new(GroupMdbxEnv::open(&path)?);
        self.read_envs.write().insert(group_id, env);
        Ok(())
    }

    /// Close a group's MDBX database.
    pub fn close_group(&self, group_id: u64) {
        self.read_envs.write().remove(&group_id);
    }

    fn group_path(&self, group_id: u64) -> PathBuf {
        self.base_dir
            .join(".lance_groups")
            .join(group_id.to_string())
    }

    // -- Read operations (direct MDBX reads via Arc<GroupMdbxEnv>) --

    fn get_env(&self, group_id: u64) -> io::Result<Arc<GroupMdbxEnv>> {
        self.read_envs
            .read()
            .get(&group_id)
            .cloned()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("manifest group {} not open", group_id),
                )
            })
    }

    /// Read per-group Raft metadata (last_applied, last_membership).
    pub fn read_group_meta(
        &self,
        group_id: u64,
    ) -> io::Result<
        Option<(
            Option<LogId<LanceTypeConfig>>,
            StoredMembership<LanceTypeConfig>,
        )>,
    > {
        let env = self.get_env(group_id)?;
        match env.read_group_meta()? {
            Some(meta) => Ok(Some(meta.to_raft()?)),
            None => Ok(None),
        }
    }

    /// Read all table entries for a group.
    pub fn read_all_tables(
        &self,
        group_id: u64,
    ) -> io::Result<HashMap<String, PersistedTableEntry>> {
        let env = self.get_env(group_id)?;
        env.read_all_tables()
    }

    /// Read WAL events with sequence number > `since_seq`.
    pub fn read_wal_since(&self, group_id: u64, since_seq: u64) -> io::Result<Vec<CatalogEvent>> {
        let env = self.get_env(group_id)?;
        env.read_wal_since(since_seq)
    }

    /// Get the latest WAL sequence number for a group.
    pub fn latest_wal_seq(&self, group_id: u64) -> io::Result<u64> {
        let env = self.get_env(group_id)?;
        env.latest_wal_seq()
    }

    /// Compact WAL entries up to (and including) the given sequence number.
    pub fn compact_wal(&self, group_id: u64, up_to_seq: u64) -> io::Result<usize> {
        let env = self.get_env(group_id)?;
        env.compact_wal(up_to_seq)
    }

    /// Read a single table's persisted entry.
    pub fn read_table(
        &self,
        group_id: u64,
        table_name: &str,
    ) -> io::Result<Option<PersistedTableEntry>> {
        let env = self.get_env(group_id)?;
        env.read_table(table_name)
    }

    // -- Write operations (via worker thread) --

    /// Send a command to the worker thread (fire-and-forget).
    pub(crate) async fn send_update(&self, cmd: ManifestCommand) {
        if let Err(e) = self.tx.send(cmd).await {
            debug!("Manifest command send failed (shutdown?): {}", e);
        }
    }

    /// Send a command and wait for durable commit (fsync).
    pub(crate) async fn send_update_durable(&self, cmd: ManifestCommand) -> io::Result<()> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        let cmd = match cmd {
            ManifestCommand::Apply {
                group_id,
                meta,
                table_update,
                ..
            } => ManifestCommand::Apply {
                group_id,
                meta,
                table_update,
                done: Some(done_tx),
            },
            ManifestCommand::InstallSnapshot {
                group_id,
                meta,
                tables,
                ..
            } => ManifestCommand::InstallSnapshot {
                group_id,
                meta,
                tables,
                done: Some(done_tx),
            },
            // WAL events are fire-and-forget only; durable not needed.
            other @ ManifestCommand::AppendWalEvents { .. } => {
                let _ = done_tx.send(());
                other
            }
        };

        self.tx
            .send(cmd)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        done_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "manifest worker dropped sender"))
    }

    /// Build a manifest command for the apply path.
    pub(crate) fn build_apply_update(
        group_id: u64,
        last_applied: &Option<LogId<LanceTypeConfig>>,
        last_membership: &StoredMembership<LanceTypeConfig>,
        table_update: Option<TableUpdate>,
    ) -> ManifestCommand {
        ManifestCommand::Apply {
            group_id,
            meta: Some(GroupMeta::from_raft(last_applied, last_membership)),
            table_update,
            done: None,
        }
    }

    /// Build a manifest command that only updates a table entry (no meta change).
    pub(crate) fn build_table_only_update(
        group_id: u64,
        table_update: TableUpdate,
    ) -> ManifestCommand {
        ManifestCommand::Apply {
            group_id,
            meta: None,
            table_update: Some(table_update),
            done: None,
        }
    }

    /// Build a persisted table entry from a table engine's current state.
    pub(crate) fn build_table_entry(
        table: &crate::table_engine::TableEngine,
    ) -> PersistedTableEntry {
        PersistedTableEntry {
            config: table.config().to_persisted(),
            catalog: table.catalog(),
            flush_state: table.flush_state(),
            schema_history: table.schema_history(),
        }
    }

    /// Signal the worker thread to shut down and wait for it to exit.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.worker_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }

    // -- Worker thread --

    fn worker_loop(
        rx: Rx<Array<ManifestCommand>>,
        shutdown: Arc<AtomicBool>,
        envs: Arc<RwLock<HashMap<u64, Arc<GroupMdbxEnv>>>>,
    ) {
        let mut pending: Vec<ManifestCommand> = Vec::new();

        loop {
            // 1. Blocking recv with timeout
            let first = match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    if !pending.is_empty() {
                        Self::commit_batch(&envs, &mut pending);
                    }
                    return;
                }
            };

            if let Some(v) = first {
                pending.push(v);
            }

            // 2. Drain without blocking to batch
            for _ in 0..4096 {
                match rx.try_recv() {
                    Ok(v) => pending.push(v),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            // 3. Coalesce and commit
            if !pending.is_empty() {
                Self::commit_batch(&envs, &mut pending);
            }

            // 4. Check shutdown
            if shutdown.load(Ordering::Acquire) {
                return;
            }
        }
    }

    fn commit_batch(
        envs: &Arc<RwLock<HashMap<u64, Arc<GroupMdbxEnv>>>>,
        pending: &mut Vec<ManifestCommand>,
    ) {
        // Coalesce per group: latest meta wins, latest table entry wins per key.
        // WAL events are accumulated in order (not coalesced).
        struct GroupBatch {
            meta: Option<GroupMeta>,
            table_updates: HashMap<String, Option<PersistedTableEntry>>,
            wal_events: Vec<CatalogEvent>,
            is_snapshot: bool,
        }

        let mut per_group: HashMap<u64, GroupBatch> = HashMap::new();
        let mut done_senders: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();

        for cmd in pending.drain(..) {
            match cmd {
                ManifestCommand::Apply {
                    group_id,
                    meta,
                    table_update,
                    done,
                } => {
                    let batch = per_group.entry(group_id).or_insert_with(|| GroupBatch {
                        meta: None,
                        table_updates: HashMap::new(),
                        wal_events: Vec::new(),
                        is_snapshot: false,
                    });
                    if let Some(m) = meta {
                        batch.meta = Some(m);
                    }
                    if let Some(tu) = table_update {
                        match tu {
                            TableUpdate::Set { table_name, entry } => {
                                batch.table_updates.insert(table_name, Some(entry));
                            }
                            TableUpdate::Remove { table_name } => {
                                batch.table_updates.insert(table_name, None);
                            }
                        }
                    }
                    if let Some(d) = done {
                        done_senders.push(d);
                    }
                }
                ManifestCommand::InstallSnapshot {
                    group_id,
                    meta,
                    tables,
                    done,
                } => {
                    let batch = per_group.entry(group_id).or_insert_with(|| GroupBatch {
                        meta: None,
                        table_updates: HashMap::new(),
                        wal_events: Vec::new(),
                        is_snapshot: false,
                    });
                    batch.meta = Some(meta);
                    batch.table_updates.clear();
                    for (name, entry) in tables {
                        batch.table_updates.insert(name, Some(entry));
                    }
                    batch.is_snapshot = true;
                    if let Some(d) = done {
                        done_senders.push(d);
                    }
                }
                ManifestCommand::AppendWalEvents { group_id, events } => {
                    let batch = per_group.entry(group_id).or_insert_with(|| GroupBatch {
                        meta: None,
                        table_updates: HashMap::new(),
                        wal_events: Vec::new(),
                        is_snapshot: false,
                    });
                    batch.wal_events.extend(events);
                }
            }
        }

        // Commit each group's batch to its own MDBX env.
        let envs_guard = envs.read();
        for (group_id, batch) in &per_group {
            let env = match envs_guard.get(group_id) {
                Some(e) => e,
                None => {
                    warn!(group_id, "Manifest command for unopened group, skipping");
                    continue;
                }
            };

            let result = if batch.is_snapshot {
                env.install_snapshot(
                    batch.meta.as_ref().unwrap(),
                    &batch
                        .table_updates
                        .iter()
                        .filter_map(|(k, v)| v.as_ref().map(|e| (k.clone(), e.clone())))
                        .collect(),
                )
            } else {
                env.apply_coalesced(batch.meta.as_ref(), &batch.table_updates)
            };

            if let Err(e) = result {
                error!(group_id, "MDBX manifest commit failed: {}", e);
            }

            // Append WAL events in a separate transaction.
            if !batch.wal_events.is_empty() {
                let txn = match env.db.begin_rw_txn() {
                    Ok(t) => t,
                    Err(e) => {
                        error!(group_id, "Failed to open WAL write txn: {}", e);
                        continue;
                    }
                };
                if let Err(e) = env.append_wal_events(&txn, &batch.wal_events) {
                    error!(group_id, "Failed to append WAL events: {}", e);
                    continue;
                }
                if let Err(e) = txn.commit() {
                    error!(group_id, "Failed to commit WAL events: {}", e);
                }

                // Size-based WAL compaction: keep WAL under 16 MiB.
                match env.compact_wal_to_size(WAL_MAX_BYTES) {
                    Ok(0) => {}
                    Ok(n) => {
                        debug!(group_id, deleted = n, "Compacted WAL entries (size limit)");
                    }
                    Err(e) => {
                        warn!(group_id, "WAL compaction failed: {}", e);
                    }
                }
            }
        }

        for sender in done_senders {
            let _ = sender.send(());
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FlushState, PersistedTableConfig, SegmentCatalog};
    use tempfile::TempDir;

    fn make_group_meta(index: u64) -> GroupMeta {
        GroupMeta {
            last_applied_term: 1,
            last_applied_node_id: 1,
            last_applied_index: index,
            last_applied_present: true,
            membership_bytes: bincode::serde::encode_to_vec(
                &StoredMembership::<LanceTypeConfig>::default(),
                bincode::config::standard(),
            )
            .unwrap(),
        }
    }

    fn make_table_entry(active_segment: u64) -> PersistedTableEntry {
        PersistedTableEntry {
            config: PersistedTableConfig::default(),
            catalog: SegmentCatalog {
                active_segment,
                sealed_segment: None,
                s3_manifest_version: 0,
                s3_dataset_uri: String::new(),
                active_first_log_index: Some(100),
                sealed_first_log_index: None,
            },
            flush_state: FlushState::Idle,
            schema_history: vec![],
        }
    }

    #[test]
    fn test_group_meta_roundtrip() {
        let meta = make_group_meta(42);
        let (last_applied, _membership) = meta.to_raft().unwrap();
        assert!(last_applied.is_some());
        let lid = last_applied.unwrap();
        assert_eq!(lid.index, 42);
        assert_eq!(lid.leader_id.term, 1);
        assert_eq!(lid.leader_id.node_id, 1);
    }

    #[test]
    fn test_group_meta_none_roundtrip() {
        let meta = GroupMeta::from_raft(&None, &StoredMembership::<LanceTypeConfig>::default());
        let (last_applied, _) = meta.to_raft().unwrap();
        assert!(last_applied.is_none());
    }

    #[test]
    fn test_per_group_read_write() {
        let dir = TempDir::new().unwrap();
        let env = GroupMdbxEnv::open(dir.path()).unwrap();

        let meta = make_group_meta(10);
        let mut updates = HashMap::new();
        updates.insert("test_table".to_string(), Some(make_table_entry(1)));
        env.apply_coalesced(Some(&meta), &updates).unwrap();

        // Read back
        let read_meta = env.read_group_meta().unwrap().unwrap();
        assert_eq!(read_meta.last_applied_index, 10);

        let tables = env.read_all_tables().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables["test_table"].catalog.active_segment, 1);

        let single = env.read_table("test_table").unwrap().unwrap();
        assert_eq!(single.catalog.active_segment, 1);

        assert!(env.read_table("nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_per_group_multiple_tables() {
        let dir = TempDir::new().unwrap();
        let env = GroupMdbxEnv::open(dir.path()).unwrap();

        let meta = make_group_meta(20);
        let mut updates = HashMap::new();
        updates.insert("alpha".to_string(), Some(make_table_entry(1)));
        updates.insert("beta".to_string(), Some(make_table_entry(2)));
        updates.insert("gamma".to_string(), Some(make_table_entry(3)));
        env.apply_coalesced(Some(&meta), &updates).unwrap();

        let tables = env.read_all_tables().unwrap();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables["alpha"].catalog.active_segment, 1);
        assert_eq!(tables["beta"].catalog.active_segment, 2);
        assert_eq!(tables["gamma"].catalog.active_segment, 3);
    }

    #[test]
    fn test_per_group_remove_table() {
        let dir = TempDir::new().unwrap();
        let env = GroupMdbxEnv::open(dir.path()).unwrap();

        let mut updates = HashMap::new();
        updates.insert("to_remove".to_string(), Some(make_table_entry(1)));
        env.apply_coalesced(Some(&make_group_meta(10)), &updates)
            .unwrap();
        assert!(env.read_table("to_remove").unwrap().is_some());

        let mut updates2 = HashMap::new();
        updates2.insert("to_remove".to_string(), None);
        env.apply_coalesced(None, &updates2).unwrap();
        assert!(env.read_table("to_remove").unwrap().is_none());
        assert!(env.read_all_tables().unwrap().is_empty());
    }

    #[test]
    fn test_install_snapshot_clears_existing() {
        let dir = TempDir::new().unwrap();
        let env = GroupMdbxEnv::open(dir.path()).unwrap();

        // Write initial data
        let mut updates = HashMap::new();
        updates.insert("old_table".to_string(), Some(make_table_entry(1)));
        env.apply_coalesced(Some(&make_group_meta(10)), &updates)
            .unwrap();
        assert!(env.read_table("old_table").unwrap().is_some());

        // Install snapshot with different tables
        let mut snapshot_entries = HashMap::new();
        snapshot_entries.insert("new_table".to_string(), make_table_entry(5));
        env.install_snapshot(&make_group_meta(50), &snapshot_entries)
            .unwrap();

        // Old table should be gone, new table present
        assert!(env.read_table("old_table").unwrap().is_none());
        let tables = env.read_all_tables().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables["new_table"].catalog.active_segment, 5);

        let meta = env.read_group_meta().unwrap().unwrap();
        assert_eq!(meta.last_applied_index, 50);
    }

    #[test]
    fn test_coalescing() {
        let dir = TempDir::new().unwrap();
        let env = GroupMdbxEnv::open(dir.path()).unwrap();

        let mut updates = HashMap::new();
        updates.insert("table".to_string(), Some(make_table_entry(1)));
        env.apply_coalesced(Some(&make_group_meta(10)), &updates)
            .unwrap();

        // Overwrite
        updates.insert("table".to_string(), Some(make_table_entry(5)));
        env.apply_coalesced(Some(&make_group_meta(20)), &updates)
            .unwrap();

        let meta = env.read_group_meta().unwrap().unwrap();
        assert_eq!(meta.last_applied_index, 20);
        let table = env.read_table("table").unwrap().unwrap();
        assert_eq!(table.catalog.active_segment, 5);
    }

    #[tokio::test]
    async fn test_manager_lifecycle() {
        let dir = TempDir::new().unwrap();
        let manager = LanceManifestManager::open(dir.path()).unwrap();

        // The legacy `open()` auto-opens group 0. Send an update.
        let update = ManifestCommand::Apply {
            group_id: 0,
            meta: Some(make_group_meta(42)),
            table_update: Some(TableUpdate::Set {
                table_name: "test".to_string(),
                entry: make_table_entry(1),
            }),
            done: None,
        };
        manager.send_update(update).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let meta = manager.read_group_meta(0).unwrap();
        assert!(meta.is_some());
        let (last_applied, _) = meta.unwrap();
        assert_eq!(last_applied.unwrap().index, 42);

        let tables = manager.read_all_tables(0).unwrap();
        assert_eq!(tables.len(), 1);

        manager.stop();
    }

    #[tokio::test]
    async fn test_durable_write() {
        let dir = TempDir::new().unwrap();
        let manager = LanceManifestManager::open(dir.path()).unwrap();

        let update = ManifestCommand::Apply {
            group_id: 0,
            meta: Some(make_group_meta(100)),
            table_update: None,
            done: None,
        };

        manager.send_update_durable(update).await.unwrap();

        let meta = manager.read_group_meta(0).unwrap().unwrap();
        let (last_applied, _) = meta;
        assert_eq!(last_applied.unwrap().index, 100);

        manager.stop();
    }

    #[tokio::test]
    async fn test_multi_group_open_close() {
        let dir = TempDir::new().unwrap();
        let manager = LanceManifestManager::new(dir.path()).unwrap();

        // Open two groups
        manager.open_group(1).unwrap();
        manager.open_group(2).unwrap();

        // Write to each
        let u1 = ManifestCommand::Apply {
            group_id: 1,
            meta: Some(make_group_meta(10)),
            table_update: Some(TableUpdate::Set {
                table_name: "g1_table".to_string(),
                entry: make_table_entry(1),
            }),
            done: None,
        };
        let u2 = ManifestCommand::Apply {
            group_id: 2,
            meta: Some(make_group_meta(20)),
            table_update: Some(TableUpdate::Set {
                table_name: "g2_table".to_string(),
                entry: make_table_entry(2),
            }),
            done: None,
        };
        manager.send_update_durable(u1).await.unwrap();
        manager.send_update_durable(u2).await.unwrap();

        // Verify isolation
        let g1 = manager.read_all_tables(1).unwrap();
        assert_eq!(g1.len(), 1);
        assert!(g1.contains_key("g1_table"));

        let g2 = manager.read_all_tables(2).unwrap();
        assert_eq!(g2.len(), 1);
        assert!(g2.contains_key("g2_table"));

        // Close group 1
        manager.close_group(1);
        assert!(manager.read_group_meta(1).is_err());

        // Group 2 still works
        assert!(manager.read_group_meta(2).unwrap().is_some());

        manager.stop();
    }
}
