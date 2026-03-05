//! Client-side MDBX persistence for `BisqueClient` catalog state.
//!
//! Stores the last-seen event sequence number and per-table catalog entries
//! so that reconnecting clients can do delta sync via `?since=last_seq`
//! instead of a full catalog fetch.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, Table, TableFlags, WriteFlags,
};
use serde::{Deserialize, Serialize};

const CLIENT_META_TABLE: &str = "client_meta";
const CLIENT_TABLES_TABLE: &str = "client_tables";
const STATE_KEY: &[u8] = b"state";

/// Metadata about the client's connection state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClientMeta {
    pub last_seq: u64,
    pub cluster_url: String,
    pub bucket: String,
}

/// Persisted per-table catalog entry (mirrors server catalog info).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedCatalogEntry {
    pub active_segment: u64,
    pub sealed_segment: Option<u64>,
    pub s3_dataset_uri: String,
    pub s3_storage_options: HashMap<String, String>,
    pub active_version: Option<u64>,
    pub sealed_version: Option<u64>,
    /// Arrow IPC-encoded schema bytes.
    pub schema_ipc: Vec<u8>,
}

/// Client-side MDBX store for persisting catalog state across restarts.
pub(crate) struct ClientStore {
    db: Arc<Database<NoWriteMap>>,
    meta_dbi: u32,
    tables_dbi: u32,
}

#[repr(C)]
struct RawTable<'txn> {
    dbi: u32,
    _marker: std::marker::PhantomData<&'txn ()>,
}

impl ClientStore {
    /// Open or create the client store at the given path.
    pub fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut opts = DatabaseOptions::default();
        opts.max_tables = Some(4);
        opts.mode = Mode::ReadWrite(ReadWriteOptions {
            min_size: Some(64 * 1024),
            max_size: Some(64 * 1024 * 1024),
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
            .create_table(Some(CLIENT_META_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let meta_dbi = meta_table.dbi();
        drop(meta_table);

        let tables_table = txn
            .create_table(Some(CLIENT_TABLES_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let tables_dbi = tables_table.dbi();
        drop(tables_table);

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(Self {
            db,
            meta_dbi,
            tables_dbi,
        })
    }

    // -- DBI handle reconstruction (same pattern as manifest.rs) --

    unsafe fn meta_table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
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
        let raw = RawTable {
            dbi: self.tables_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    /// Load persisted state: connection metadata + all table entries.
    ///
    /// Returns `None` if no state has been persisted yet.
    pub fn load_state(
        &self,
    ) -> io::Result<Option<(ClientMeta, HashMap<String, PersistedCatalogEntry>)>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Load meta
        let meta_table = unsafe { self.meta_table_ro(&txn) };
        let meta_bytes: Option<std::borrow::Cow<[u8]>> = txn
            .get(&meta_table, STATE_KEY)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let meta = match meta_bytes {
            Some(bytes) => {
                let (meta, _) = bincode::serde::decode_from_slice::<ClientMeta, _>(
                    bytes.as_ref(),
                    bincode::config::standard(),
                )
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                meta
            }
            None => return Ok(None),
        };

        // Load tables
        let tables_table = unsafe { self.tables_table_ro(&txn) };
        let mut tables = HashMap::new();
        let mut cursor = txn
            .cursor(&tables_table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let iter = cursor.iter::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>();
        for result in iter {
            match result {
                Ok((k, v)) => {
                    let name = String::from_utf8_lossy(&k).to_string();
                    match bincode::serde::decode_from_slice::<PersistedCatalogEntry, _>(
                        v.as_ref(),
                        bincode::config::standard(),
                    ) {
                        Ok((entry, _)) => {
                            tables.insert(name, entry);
                        }
                        Err(e) => {
                            tracing::warn!(table = %name, "Failed to decode persisted catalog entry: {}", e);
                        }
                    }
                }
                Err(_) => break,
            }
        }

        Ok(Some((meta, tables)))
    }

    /// Save full state atomically (metadata + all table entries).
    pub fn save_state(
        &self,
        meta: &ClientMeta,
        tables: &HashMap<String, PersistedCatalogEntry>,
    ) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Write meta
        let meta_table = unsafe { self.meta_table_rw(&txn) };
        let meta_bytes = bincode::serde::encode_to_vec(meta, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        txn.put(&meta_table, STATE_KEY, &meta_bytes, WriteFlags::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Clear existing tables and write new
        let tables_table = unsafe { self.tables_table_rw(&txn) };
        txn.clear_table(&tables_table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        for (name, entry) in tables {
            let value = bincode::serde::encode_to_vec(entry, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            txn.put(&tables_table, name.as_bytes(), &value, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    /// Update a single table entry.
    pub fn update_table(&self, name: &str, entry: &PersistedCatalogEntry) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let table = unsafe { self.tables_table_rw(&txn) };
        let value = bincode::serde::encode_to_vec(entry, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        txn.put(&table, name.as_bytes(), &value, WriteFlags::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    /// Remove a table entry.
    pub fn remove_table(&self, name: &str) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let table = unsafe { self.tables_table_rw(&txn) };
        match txn.del(&table, name.as_bytes(), None) {
            Ok(_) | Err(libmdbx::Error::NotFound) => {}
            Err(e) => {
                return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
            }
        }

        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    /// Update the last event sequence number.
    pub fn update_seq(&self, seq: u64) -> io::Result<()> {
        // Read existing meta in a RO transaction, decode, then drop.
        let mut meta = {
            let txn = self
                .db
                .begin_ro_txn()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let meta_table = unsafe { self.meta_table_ro(&txn) };
            let meta_bytes: Option<std::borrow::Cow<[u8]>> = txn
                .get(&meta_table, STATE_KEY)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            match meta_bytes {
                Some(bytes) => {
                    let (meta, _) = bincode::serde::decode_from_slice::<ClientMeta, _>(
                        bytes.as_ref(),
                        bincode::config::standard(),
                    )
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    meta
                }
                None => return Ok(()),
            }
        };

        meta.last_seq = seq;

        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let meta_table = unsafe { self.meta_table_rw(&txn) };
        let value = bincode::serde::encode_to_vec(&meta, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        txn.put(&meta_table, STATE_KEY, &value, WriteFlags::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = ClientStore::open(dir.path()).unwrap();

        // Initially empty
        assert!(store.load_state().unwrap().is_none());

        let meta = ClientMeta {
            last_seq: 42,
            cluster_url: "http://localhost:3300".into(),
            bucket: "data".into(),
        };
        let mut tables = HashMap::new();
        tables.insert(
            "events".into(),
            PersistedCatalogEntry {
                active_segment: 1,
                sealed_segment: Some(2),
                s3_dataset_uri: "s3://bucket/events".into(),
                s3_storage_options: HashMap::from([("aws_region".into(), "us-east-1".into())]),
                active_version: Some(5),
                sealed_version: Some(3),
                schema_ipc: vec![1, 2, 3],
            },
        );

        store.save_state(&meta, &tables).unwrap();

        let (loaded_meta, loaded_tables) = store.load_state().unwrap().unwrap();
        assert_eq!(loaded_meta.last_seq, 42);
        assert_eq!(loaded_meta.cluster_url, "http://localhost:3300");
        assert_eq!(loaded_meta.bucket, "data");
        assert_eq!(loaded_tables.len(), 1);
        let entry = loaded_tables.get("events").unwrap();
        assert_eq!(entry.active_segment, 1);
        assert_eq!(entry.sealed_segment, Some(2));
        assert_eq!(entry.active_version, Some(5));
        assert_eq!(entry.schema_ipc, vec![1, 2, 3]);
    }

    #[test]
    fn test_update_table_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let store = ClientStore::open(dir.path()).unwrap();

        let meta = ClientMeta {
            last_seq: 0,
            cluster_url: "http://localhost:3300".into(),
            bucket: "data".into(),
        };
        store.save_state(&meta, &HashMap::new()).unwrap();

        let entry = PersistedCatalogEntry {
            active_segment: 1,
            sealed_segment: None,
            s3_dataset_uri: "s3://bucket/logs".into(),
            s3_storage_options: HashMap::new(),
            active_version: Some(1),
            sealed_version: None,
            schema_ipc: vec![10, 20],
        };
        store.update_table("logs", &entry).unwrap();

        let (_, tables) = store.load_state().unwrap().unwrap();
        assert!(tables.contains_key("logs"));

        store.remove_table("logs").unwrap();
        let (_, tables) = store.load_state().unwrap().unwrap();
        assert!(!tables.contains_key("logs"));
    }

    #[test]
    fn test_update_seq() {
        let dir = tempfile::tempdir().unwrap();
        let store = ClientStore::open(dir.path()).unwrap();

        let meta = ClientMeta {
            last_seq: 10,
            cluster_url: "http://localhost:3300".into(),
            bucket: "data".into(),
        };
        store.save_state(&meta, &HashMap::new()).unwrap();

        store.update_seq(99).unwrap();

        let (loaded_meta, _) = store.load_state().unwrap().unwrap();
        assert_eq!(loaded_meta.last_seq, 99);
    }

    #[test]
    fn test_reopen_persistence() {
        let dir = tempfile::tempdir().unwrap();

        // Write state
        {
            let store = ClientStore::open(dir.path()).unwrap();
            let meta = ClientMeta {
                last_seq: 55,
                cluster_url: "http://host:3300".into(),
                bucket: "mybucket".into(),
            };
            let mut tables = HashMap::new();
            tables.insert(
                "metrics".into(),
                PersistedCatalogEntry {
                    active_segment: 3,
                    sealed_segment: Some(2),
                    s3_dataset_uri: "s3://deep/metrics".into(),
                    s3_storage_options: HashMap::new(),
                    active_version: Some(10),
                    sealed_version: Some(7),
                    schema_ipc: vec![42],
                },
            );
            store.save_state(&meta, &tables).unwrap();
        }

        // Reopen and verify
        {
            let store = ClientStore::open(dir.path()).unwrap();
            let (meta, tables) = store.load_state().unwrap().unwrap();
            assert_eq!(meta.last_seq, 55);
            assert_eq!(meta.bucket, "mybucket");
            assert_eq!(tables.len(), 1);
            assert_eq!(tables.get("metrics").unwrap().active_version, Some(10));
        }
    }
}
