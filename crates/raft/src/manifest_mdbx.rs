use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use super::record_format::RecordTypeFlags;
use crossfire::{MAsyncTx, RecvTimeoutError, Rx, TryRecvError, mpsc::Array};
use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, Table, TableFlags, WriteFlags,
};

const RAFT_SEGMENTS_META_TABLE: &str = "raft_segments_meta_v3";

/// Where a segment's data resides in the tiered storage hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub(crate) enum SegmentLocation {
    /// Segment file exists on local disk only.
    #[default]
    Local = 0,
    /// Segment has been archived to remote (S3) storage; local file deleted.
    Remote = 1,
    /// Segment exists on both local disk and remote (S3) storage.
    Both = 2,
}

impl SegmentLocation {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Remote,
            2 => Self::Both,
            _ => Self::Local,
        }
    }

    /// Returns true if a local file is expected to exist.
    pub(crate) fn has_local(&self) -> bool {
        matches!(self, Self::Local | Self::Both)
    }

    /// Returns true if the segment has been archived remotely.
    pub(crate) fn has_remote(&self) -> bool {
        matches!(self, Self::Remote | Self::Both)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentMeta {
    pub(crate) group_id: u64,
    pub(crate) segment_id: u64,
    /// Logical end offset (last valid record boundary).
    pub(crate) valid_bytes: u64,
    /// Optional entry index range present in this segment.
    pub(crate) min_index: Option<u64>,
    pub(crate) max_index: Option<u64>,
    /// Optional write-time range (unix nanos) for entries in this segment.
    pub(crate) min_ts: Option<u64>,
    pub(crate) max_ts: Option<u64>,
    /// True if this segment is sealed (not the current head).
    pub(crate) sealed: bool,
    /// Number of records in this segment.
    pub(crate) record_count: u64,
    /// Record types present in this segment (entries, votes, truncates, purges).
    /// Used for fast-path recovery: entry-only segments skip full CRC decode.
    pub(crate) record_type_flags: RecordTypeFlags,
    /// Number of Entry records (vs total record_count which includes votes, etc.)
    pub(crate) entry_count: u64,
    /// Byte offset of the first Entry record in the segment.
    pub(crate) first_entry_offset: u64,
    /// Where this segment's data resides (local, remote, or both).
    pub(crate) location: SegmentLocation,
}

impl SegmentMeta {
    pub(crate) fn merge_from(&mut self, other: SegmentMeta) {
        debug_assert_eq!(self.group_id, other.group_id);
        debug_assert_eq!(self.segment_id, other.segment_id);

        self.valid_bytes = self.valid_bytes.max(other.valid_bytes);
        self.sealed |= other.sealed;
        self.record_count = self.record_count.max(other.record_count);
        self.record_type_flags = self.record_type_flags.merge(other.record_type_flags);
        self.entry_count = self.entry_count.max(other.entry_count);
        if other.first_entry_offset > 0
            && (self.first_entry_offset == 0 || other.first_entry_offset < self.first_entry_offset)
        {
            self.first_entry_offset = other.first_entry_offset;
        }

        // Location: upgrade toward "Both" if either side has remote
        if other.location.has_remote() && !self.location.has_remote() {
            self.location = if self.location.has_local() {
                SegmentLocation::Both
            } else {
                other.location
            };
        }

        match (self.min_index, other.min_index) {
            (None, x) => self.min_index = x,
            (Some(a), Some(b)) => self.min_index = Some(a.min(b)),
            _ => {}
        }
        match (self.max_index, other.max_index) {
            (None, x) => self.max_index = x,
            (Some(a), Some(b)) => self.max_index = Some(a.max(b)),
            _ => {}
        }
        match (self.min_ts, other.min_ts) {
            (None, x) => self.min_ts = x,
            (Some(a), Some(b)) => self.min_ts = Some(a.min(b)),
            _ => {}
        }
        match (self.max_ts, other.max_ts) {
            (None, x) => self.max_ts = x,
            (Some(a), Some(b)) => self.max_ts = Some(a.max(b)),
            _ => {}
        }
    }

    fn key_bytes(&self) -> [u8; 16] {
        let mut k = [0u8; 16];
        k[0..8].copy_from_slice(&self.group_id.to_be_bytes());
        k[8..16].copy_from_slice(&self.segment_id.to_be_bytes());
        k
    }

    pub(crate) fn encode_value(&self) -> [u8; 72] {
        // [flags:1][record_type_flags:1][location:1][pad:5][valid:8][min_i:8][max_i:8][min_ts:8][max_ts:8][record_count:8][entry_count:8][first_entry_offset:8]
        let mut v = [0u8; 72];
        let mut flags = 0u8;
        if self.min_index.is_some() && self.max_index.is_some() {
            flags |= 1 << 0;
        }
        if self.min_ts.is_some() && self.max_ts.is_some() {
            flags |= 1 << 1;
        }
        if self.sealed {
            flags |= 1 << 2;
        }
        v[0] = flags;
        v[1] = self.record_type_flags.to_u8();
        v[2] = self.location as u8;
        v[8..16].copy_from_slice(&self.valid_bytes.to_be_bytes());
        let min_i = self.min_index.unwrap_or(0);
        let max_i = self.max_index.unwrap_or(0);
        v[16..24].copy_from_slice(&min_i.to_be_bytes());
        v[24..32].copy_from_slice(&max_i.to_be_bytes());
        let min_ts = self.min_ts.unwrap_or(0);
        let max_ts = self.max_ts.unwrap_or(0);
        v[32..40].copy_from_slice(&min_ts.to_be_bytes());
        v[40..48].copy_from_slice(&max_ts.to_be_bytes());
        v[48..56].copy_from_slice(&self.record_count.to_be_bytes());
        v[56..64].copy_from_slice(&self.entry_count.to_be_bytes());
        v[64..72].copy_from_slice(&self.first_entry_offset.to_be_bytes());
        v
    }

    pub(crate) fn decode_value(group_id: u64, segment_id: u64, bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 72 {
            return None;
        }
        let flags = bytes[0];
        let record_type_flags = RecordTypeFlags::from_u8(bytes[1]);
        let location = SegmentLocation::from_u8(bytes[2]);
        let valid_bytes = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
        let min_i = u64::from_be_bytes(bytes[16..24].try_into().ok()?);
        let max_i = u64::from_be_bytes(bytes[24..32].try_into().ok()?);
        let min_ts = u64::from_be_bytes(bytes[32..40].try_into().ok()?);
        let max_ts = u64::from_be_bytes(bytes[40..48].try_into().ok()?);
        let record_count = u64::from_be_bytes(bytes[48..56].try_into().ok()?);
        let entry_count = u64::from_be_bytes(bytes[56..64].try_into().ok()?);
        let first_entry_offset = u64::from_be_bytes(bytes[64..72].try_into().ok()?);

        let has_idx = (flags & (1 << 0)) != 0;
        let has_ts = (flags & (1 << 1)) != 0;
        let sealed = (flags & (1 << 2)) != 0;

        Some(Self {
            group_id,
            segment_id,
            valid_bytes,
            min_index: if has_idx { Some(min_i) } else { None },
            max_index: if has_idx { Some(max_i) } else { None },
            min_ts: if has_ts { Some(min_ts) } else { None },
            max_ts: if has_ts { Some(max_ts) } else { None },
            sealed,
            record_count,
            record_type_flags,
            entry_count,
            first_entry_offset,
            location,
        })
    }
}

pub(crate) struct MdbxManifest {
    db: Arc<Database<NoWriteMap>>,
    segments_meta_dbi: u32,
}

impl MdbxManifest {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Configure explicit geometry to avoid "invalid arguments" errors on some systems.
        // MDBX requires valid geometry settings; using defaults (-1) can fail on certain
        // filesystems or configurations.
        let mut opts = DatabaseOptions::default();
        opts.max_tables = Some(8);
        opts.mode = Mode::ReadWrite(ReadWriteOptions {
            min_size: Some(64 * 1024),          // 64 KB minimum
            max_size: Some(256 * 1024 * 1024),  // 256 MB maximum (plenty for manifest metadata)
            growth_step: Some(64 * 1024),       // Grow by 64 KB at a time
            shrink_threshold: Some(256 * 1024), // Shrink when 256 KB can be reclaimed
            ..Default::default()
        });

        let db = Database::<NoWriteMap>::open_with_options(path, opts)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let db = Arc::new(db);

        let txn = db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let segments = txn
            .create_table(Some(RAFT_SEGMENTS_META_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let segments_dbi = segments.dbi();
        drop(segments);
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(Self {
            db,
            segments_meta_dbi: segments_dbi,
        })
    }

    /// Create an in-memory manifest for filesystems that don't support MDBX
    pub(crate) fn new_in_memory() -> io::Result<Self> {
        // Use a temp directory that should work
        let temp_dir = std::env::temp_dir().join(".raft_manifest_in_memory");
        // Try to use temp dir, if that fails too, try a fallback path
        match Self::open(&temp_dir) {
            Ok(manifest) => Ok(manifest),
            Err(_) => {
                // libmdbx requires a path, so we need to use a temp path.
                let fallback_path = std::env::temp_dir()
                    .join(format!(".raft_manifest_fallback_{}", std::process::id()));
                Self::open(&fallback_path)
            }
        }
    }

    unsafe fn table_ro<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RO, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.segments_meta_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    unsafe fn table_rw<'txn>(
        &self,
        _txn: &libmdbx::Transaction<'txn, libmdbx::RW, NoWriteMap>,
    ) -> Table<'txn> {
        #[repr(C)]
        struct RawTable<'txn> {
            dbi: u32,
            _marker: std::marker::PhantomData<&'txn ()>,
        }
        let raw = RawTable {
            dbi: self.segments_meta_dbi,
            _marker: std::marker::PhantomData,
        };
        unsafe { std::mem::transmute(raw) }
    }

    pub(crate) fn read_group_segments(
        &self,
        group_id: u64,
    ) -> io::Result<HashMap<u64, SegmentMeta>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.table_ro(&txn) };
        let mut out: HashMap<u64, SegmentMeta> = HashMap::new();

        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut start = [0u8; 16];
        start[0..8].copy_from_slice(&group_id.to_be_bytes());
        // segment_id = 0 for starting point
        let mut iter = cursor.iter_from::<std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>>(&start);
        while let Some(Ok((k, v))) = iter.next() {
            let kb = k.as_ref();
            if kb.len() != 16 {
                break;
            }
            let gid = u64::from_be_bytes(kb[0..8].try_into().unwrap());
            if gid != group_id {
                break;
            }
            let sid = u64::from_be_bytes(kb[8..16].try_into().unwrap());
            if let Some(meta) = SegmentMeta::decode_value(gid, sid, v.as_ref()) {
                out.insert(sid, meta);
            } else {
                break;
            }
        }
        Ok(out)
    }

    pub(crate) fn apply_segment_updates(
        &self,
        updates: impl Iterator<Item = SegmentMeta>,
    ) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let table = unsafe { self.table_rw(&txn) };
        for u in updates {
            let k = u.key_bytes();
            let v = u.encode_value();
            txn.put(&table, &k, &v, WriteFlags::empty())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct ManifestManager {
    tx: MAsyncTx<Array<SegmentMeta>>,
    manifest: Arc<MdbxManifest>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    worker_thread: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl ManifestManager {
    pub(crate) fn open(base_dir: &Path) -> io::Result<Self> {
        let path = base_dir.join(".raft_manifest");
        let manifest = Arc::new(MdbxManifest::open(&path)?);
        let (tx, rx) = crossfire::mpsc::bounded_async_blocking::<SegmentMeta>(4096);
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let manifest_clone = manifest.clone();
        let shutdown_clone = shutdown.clone();
        let handle = std::thread::Builder::new()
            .name("raft-manifest-mdbx".to_string())
            .spawn(move || {
                Self::worker_loop(manifest_clone, rx, shutdown_clone);
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(Self {
            tx,
            manifest,
            shutdown,
            worker_thread: Arc::new(std::sync::Mutex::new(Some(handle))),
        })
    }

    /// Open an in-memory manifest for filesystems that don't support MDBX (e.g., tmpfs)
    /// This is best-effort metadata only - recovery will work but may be slower
    pub(crate) fn open_in_memory() -> io::Result<Self> {
        let manifest = Arc::new(MdbxManifest::new_in_memory()?);
        let (tx, rx) = crossfire::mpsc::bounded_async_blocking::<SegmentMeta>(4096);
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let manifest_clone = manifest.clone();
        let shutdown_clone = shutdown.clone();
        let handle = std::thread::Builder::new()
            .name("raft-manifest-memory".to_string())
            .spawn(move || {
                Self::worker_loop(manifest_clone, rx, shutdown_clone);
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(Self {
            tx,
            manifest,
            shutdown,
            worker_thread: Arc::new(std::sync::Mutex::new(Some(handle))),
        })
    }

    pub(crate) fn sender(&self) -> MAsyncTx<Array<SegmentMeta>> {
        self.tx.clone()
    }

    /// Signal the manifest worker thread to shut down and wait for it to exit.
    pub(crate) fn stop(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(handle) = self
            .worker_thread
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }

    pub(crate) fn read_group_segments(
        &self,
        group_id: u64,
    ) -> io::Result<HashMap<u64, SegmentMeta>> {
        self.manifest.read_group_segments(group_id)
    }

    fn worker_loop(
        manifest: Arc<MdbxManifest>,
        rx: Rx<Array<SegmentMeta>>,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
    ) {
        use std::time::Duration;

        // Coalesce updates by (group_id, segment_id) and commit in batches.
        let mut pending: HashMap<(u64, u64), SegmentMeta> = HashMap::new();
        loop {
            // Use recv_timeout so we can periodically check the shutdown flag
            // even when no messages arrive.
            let first = match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    // All senders dropped — flush remaining and exit.
                    if !pending.is_empty() {
                        let drained = pending.drain().map(|(_, v)| v).collect::<Vec<_>>();
                        let _ = manifest.apply_segment_updates(drained.into_iter());
                    }
                    return;
                }
            };

            if let Some(v) = first {
                let key = (v.group_id, v.segment_id);
                pending
                    .entry(key)
                    .and_modify(|m| m.merge_from(v))
                    .or_insert(v);
            }

            // Drain without blocking to batch.
            for _ in 0..4096 {
                match rx.try_recv() {
                    Ok(v) => {
                        let key = (v.group_id, v.segment_id);
                        pending
                            .entry(key)
                            .and_modify(|m| m.merge_from(v))
                            .or_insert(v);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            if !pending.is_empty() {
                let drained = pending.drain().map(|(_, v)| v).collect::<Vec<_>>();
                // Best-effort: if manifest commit fails, we just lose acceleration.
                let _ = manifest.apply_segment_updates(drained.into_iter());
            }

            if shutdown.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestTempDir;
    use std::time::Duration;

    // =========================================================================
    // SegmentMeta tests
    // =========================================================================

    fn make_segment_meta(group_id: u64, segment_id: u64) -> SegmentMeta {
        SegmentMeta {
            group_id,
            segment_id,
            valid_bytes: 0,
            min_index: None,
            max_index: None,
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        }
    }

    #[test]
    fn test_segment_meta_key_bytes() {
        let meta = SegmentMeta {
            group_id: 0x0102030405060708,
            segment_id: 0x1112131415161718,
            valid_bytes: 0,
            min_index: None,
            max_index: None,
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let key = meta.key_bytes();
        assert_eq!(key.len(), 16);
        // Check big-endian encoding
        assert_eq!(
            &key[0..8],
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
        assert_eq!(
            &key[8..16],
            &[0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18]
        );
    }

    #[test]
    fn test_segment_meta_encode_decode_minimal() {
        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 2,
            valid_bytes: 1024,
            min_index: None,
            max_index: None,
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(1, 2, &encoded).unwrap();

        assert_eq!(decoded.group_id, 1);
        assert_eq!(decoded.segment_id, 2);
        assert_eq!(decoded.valid_bytes, 1024);
        assert_eq!(decoded.min_index, None);
        assert_eq!(decoded.max_index, None);
        assert_eq!(decoded.min_ts, None);
        assert_eq!(decoded.max_ts, None);
        assert!(!decoded.sealed);
    }

    #[test]
    fn test_segment_meta_encode_decode_full() {
        let meta = SegmentMeta {
            group_id: 100,
            segment_id: 200,
            valid_bytes: 65536,
            min_index: Some(1),
            max_index: Some(1000),
            min_ts: Some(1234567890),
            max_ts: Some(9876543210),
            sealed: true,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(100, 200, &encoded).unwrap();

        assert_eq!(decoded.group_id, 100);
        assert_eq!(decoded.segment_id, 200);
        assert_eq!(decoded.valid_bytes, 65536);
        assert_eq!(decoded.min_index, Some(1));
        assert_eq!(decoded.max_index, Some(1000));
        assert_eq!(decoded.min_ts, Some(1234567890));
        assert_eq!(decoded.max_ts, Some(9876543210));
        assert!(decoded.sealed);
    }

    #[test]
    fn test_segment_meta_encode_decode_partial_index() {
        // Only index range, no timestamp
        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 1,
            valid_bytes: 100,
            min_index: Some(10),
            max_index: Some(20),
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(1, 1, &encoded).unwrap();

        assert_eq!(decoded.min_index, Some(10));
        assert_eq!(decoded.max_index, Some(20));
        assert_eq!(decoded.min_ts, None);
        assert_eq!(decoded.max_ts, None);
    }

    #[test]
    fn test_segment_meta_encode_decode_partial_timestamp() {
        // Only timestamp range, no index
        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 1,
            valid_bytes: 100,
            min_index: None,
            max_index: None,
            min_ts: Some(1000),
            max_ts: Some(2000),
            sealed: true,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(1, 1, &encoded).unwrap();

        assert_eq!(decoded.min_index, None);
        assert_eq!(decoded.max_index, None);
        assert_eq!(decoded.min_ts, Some(1000));
        assert_eq!(decoded.max_ts, Some(2000));
        assert!(decoded.sealed);
    }

    #[test]
    fn test_segment_meta_decode_wrong_length() {
        let short = [0u8; 47];
        let long = [0u8; 49];

        assert!(SegmentMeta::decode_value(1, 1, &short).is_none());
        assert!(SegmentMeta::decode_value(1, 1, &long).is_none());
    }

    #[test]
    fn test_segment_meta_encode_decode_max_values() {
        let meta = SegmentMeta {
            group_id: u64::MAX,
            segment_id: u64::MAX,
            valid_bytes: u64::MAX,
            min_index: Some(u64::MAX),
            max_index: Some(u64::MAX),
            min_ts: Some(u64::MAX),
            max_ts: Some(u64::MAX),
            sealed: true,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(u64::MAX, u64::MAX, &encoded).unwrap();

        assert_eq!(decoded.valid_bytes, u64::MAX);
        assert_eq!(decoded.min_index, Some(u64::MAX));
        assert_eq!(decoded.max_index, Some(u64::MAX));
        assert_eq!(decoded.min_ts, Some(u64::MAX));
        assert_eq!(decoded.max_ts, Some(u64::MAX));
    }

    // =========================================================================
    // SegmentMeta merge tests
    // =========================================================================

    #[test]
    fn test_segment_meta_merge_valid_bytes() {
        let mut base = make_segment_meta(1, 1);
        base.valid_bytes = 100;

        let other = SegmentMeta {
            valid_bytes: 200,
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert_eq!(base.valid_bytes, 200); // Takes max
    }

    #[test]
    fn test_segment_meta_merge_valid_bytes_base_larger() {
        let mut base = make_segment_meta(1, 1);
        base.valid_bytes = 300;

        let other = SegmentMeta {
            valid_bytes: 100,
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert_eq!(base.valid_bytes, 300); // Keeps base (larger)
    }

    #[test]
    fn test_segment_meta_merge_sealed_flag() {
        let mut base = make_segment_meta(1, 1);
        base.sealed = false;

        let other = SegmentMeta {
            sealed: true,
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert!(base.sealed); // OR operation

        // Once sealed, stays sealed
        let not_sealed = make_segment_meta(1, 1);
        base.merge_from(not_sealed);
        assert!(base.sealed);
    }

    #[test]
    fn test_segment_meta_merge_index_from_none() {
        let mut base = make_segment_meta(1, 1);
        assert_eq!(base.min_index, None);
        assert_eq!(base.max_index, None);

        let other = SegmentMeta {
            min_index: Some(10),
            max_index: Some(20),
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert_eq!(base.min_index, Some(10));
        assert_eq!(base.max_index, Some(20));
    }

    #[test]
    fn test_segment_meta_merge_index_min_max() {
        let mut base = make_segment_meta(1, 1);
        base.min_index = Some(50);
        base.max_index = Some(100);

        let other = SegmentMeta {
            min_index: Some(10),
            max_index: Some(200),
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert_eq!(base.min_index, Some(10)); // Takes min
        assert_eq!(base.max_index, Some(200)); // Takes max
    }

    #[test]
    fn test_segment_meta_merge_index_keep_base() {
        let mut base = make_segment_meta(1, 1);
        base.min_index = Some(5);
        base.max_index = Some(500);

        let other = SegmentMeta {
            min_index: Some(10),
            max_index: Some(200),
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert_eq!(base.min_index, Some(5)); // Base had lower min
        assert_eq!(base.max_index, Some(500)); // Base had higher max
    }

    #[test]
    fn test_segment_meta_merge_timestamp_from_none() {
        let mut base = make_segment_meta(1, 1);

        let other = SegmentMeta {
            min_ts: Some(1000),
            max_ts: Some(2000),
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert_eq!(base.min_ts, Some(1000));
        assert_eq!(base.max_ts, Some(2000));
    }

    #[test]
    fn test_segment_meta_merge_timestamp_min_max() {
        let mut base = make_segment_meta(1, 1);
        base.min_ts = Some(500);
        base.max_ts = Some(1500);

        let other = SegmentMeta {
            min_ts: Some(100),
            max_ts: Some(3000),
            ..make_segment_meta(1, 1)
        };

        base.merge_from(other);
        assert_eq!(base.min_ts, Some(100));
        assert_eq!(base.max_ts, Some(3000));
    }

    #[test]
    fn test_segment_meta_merge_keeps_base_when_other_none() {
        let mut base = make_segment_meta(1, 1);
        base.min_index = Some(10);
        base.max_index = Some(20);
        base.min_ts = Some(1000);
        base.max_ts = Some(2000);

        let other = make_segment_meta(1, 1); // All None

        base.merge_from(other);
        // Base values should be preserved
        assert_eq!(base.min_index, Some(10));
        assert_eq!(base.max_index, Some(20));
        assert_eq!(base.min_ts, Some(1000));
        assert_eq!(base.max_ts, Some(2000));
    }

    // =========================================================================
    // MdbxManifest tests
    // =========================================================================

    #[test]
    fn test_mdbx_manifest_open() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        // Verify empty read
        let segments = manifest.read_group_segments(1).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn test_mdbx_manifest_reopen() {
        let tmp = TestTempDir::new();

        // Write some data
        {
            let manifest = MdbxManifest::open(tmp.path()).unwrap();
            let meta = SegmentMeta {
                group_id: 1,
                segment_id: 0,
                valid_bytes: 1024,
                min_index: Some(1),
                max_index: Some(100),
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            };
            manifest
                .apply_segment_updates(std::iter::once(meta))
                .unwrap();
        }

        // Reopen and verify data persisted
        {
            let manifest = MdbxManifest::open(tmp.path()).unwrap();
            let segments = manifest.read_group_segments(1).unwrap();
            assert_eq!(segments.len(), 1);
            let meta = segments.get(&0).unwrap();
            assert_eq!(meta.valid_bytes, 1024);
            assert_eq!(meta.min_index, Some(1));
            assert_eq!(meta.max_index, Some(100));
        }
    }

    #[test]
    fn test_mdbx_manifest_write_read_single_segment() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        let meta = SegmentMeta {
            group_id: 42,
            segment_id: 7,
            valid_bytes: 65536,
            min_index: Some(1),
            max_index: Some(1000),
            min_ts: Some(1234567890),
            max_ts: Some(9876543210),
            sealed: true,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        manifest
            .apply_segment_updates(std::iter::once(meta))
            .unwrap();

        let segments = manifest.read_group_segments(42).unwrap();
        assert_eq!(segments.len(), 1);

        let read_meta = segments.get(&7).unwrap();
        assert_eq!(read_meta.group_id, 42);
        assert_eq!(read_meta.segment_id, 7);
        assert_eq!(read_meta.valid_bytes, 65536);
        assert_eq!(read_meta.min_index, Some(1));
        assert_eq!(read_meta.max_index, Some(1000));
        assert_eq!(read_meta.min_ts, Some(1234567890));
        assert_eq!(read_meta.max_ts, Some(9876543210));
        assert!(read_meta.sealed);
    }

    #[test]
    fn test_mdbx_manifest_write_read_multiple_segments() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        let metas = vec![
            SegmentMeta {
                group_id: 1,
                segment_id: 0,
                valid_bytes: 100,
                min_index: Some(1),
                max_index: Some(10),
                min_ts: None,
                max_ts: None,
                sealed: true,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 1,
                segment_id: 1,
                valid_bytes: 200,
                min_index: Some(11),
                max_index: Some(20),
                min_ts: None,
                max_ts: None,
                sealed: true,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 1,
                segment_id: 2,
                valid_bytes: 300,
                min_index: Some(21),
                max_index: Some(30),
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
        ];

        manifest.apply_segment_updates(metas.into_iter()).unwrap();

        let segments = manifest.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 3);

        assert_eq!(segments.get(&0).unwrap().valid_bytes, 100);
        assert!(segments.get(&0).unwrap().sealed);

        assert_eq!(segments.get(&1).unwrap().valid_bytes, 200);
        assert!(segments.get(&1).unwrap().sealed);

        assert_eq!(segments.get(&2).unwrap().valid_bytes, 300);
        assert!(!segments.get(&2).unwrap().sealed);
    }

    #[test]
    fn test_mdbx_manifest_multiple_groups_isolation() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        let metas = vec![
            SegmentMeta {
                group_id: 1,
                segment_id: 0,
                valid_bytes: 100,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 2,
                segment_id: 0,
                valid_bytes: 200,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 3,
                segment_id: 0,
                valid_bytes: 300,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
        ];

        manifest.apply_segment_updates(metas.into_iter()).unwrap();

        // Each group only sees its own segments
        let g1 = manifest.read_group_segments(1).unwrap();
        assert_eq!(g1.len(), 1);
        assert_eq!(g1.get(&0).unwrap().valid_bytes, 100);

        let g2 = manifest.read_group_segments(2).unwrap();
        assert_eq!(g2.len(), 1);
        assert_eq!(g2.get(&0).unwrap().valid_bytes, 200);

        let g3 = manifest.read_group_segments(3).unwrap();
        assert_eq!(g3.len(), 1);
        assert_eq!(g3.get(&0).unwrap().valid_bytes, 300);

        // Non-existent group returns empty
        let g99 = manifest.read_group_segments(99).unwrap();
        assert!(g99.is_empty());
    }

    #[test]
    fn test_mdbx_manifest_update_existing_segment() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        // Initial write
        let meta1 = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 100,
            min_index: Some(1),
            max_index: Some(10),
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };
        manifest
            .apply_segment_updates(std::iter::once(meta1))
            .unwrap();

        // Update with new values
        let meta2 = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 500,
            min_index: Some(1),
            max_index: Some(50),
            min_ts: None,
            max_ts: None,
            sealed: true,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };
        manifest
            .apply_segment_updates(std::iter::once(meta2))
            .unwrap();

        let segments = manifest.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 1);

        let read_meta = segments.get(&0).unwrap();
        assert_eq!(read_meta.valid_bytes, 500); // Updated
        assert_eq!(read_meta.max_index, Some(50)); // Updated
        assert!(read_meta.sealed); // Updated
    }

    #[test]
    fn test_mdbx_manifest_large_segment_ids() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        let metas = vec![
            SegmentMeta {
                group_id: 1,
                segment_id: u64::MAX - 2,
                valid_bytes: 100,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 1,
                segment_id: u64::MAX - 1,
                valid_bytes: 200,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 1,
                segment_id: u64::MAX,
                valid_bytes: 300,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
        ];

        manifest.apply_segment_updates(metas.into_iter()).unwrap();

        let segments = manifest.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments.get(&(u64::MAX - 2)).unwrap().valid_bytes, 100);
        assert_eq!(segments.get(&(u64::MAX - 1)).unwrap().valid_bytes, 200);
        assert_eq!(segments.get(&u64::MAX).unwrap().valid_bytes, 300);
    }

    #[test]
    fn test_mdbx_manifest_segment_ordering() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        // Insert out of order
        let metas = vec![
            SegmentMeta {
                group_id: 1,
                segment_id: 5,
                valid_bytes: 500,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 1,
                segment_id: 2,
                valid_bytes: 200,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
            SegmentMeta {
                group_id: 1,
                segment_id: 8,
                valid_bytes: 800,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            },
        ];

        manifest.apply_segment_updates(metas.into_iter()).unwrap();

        let segments = manifest.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 3);

        // All segments should be accessible
        assert_eq!(segments.get(&2).unwrap().valid_bytes, 200);
        assert_eq!(segments.get(&5).unwrap().valid_bytes, 500);
        assert_eq!(segments.get(&8).unwrap().valid_bytes, 800);
    }

    // =========================================================================
    // ManifestManager tests
    // =========================================================================

    #[test]
    fn test_manifest_manager_open() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();

        // Verify we can get a sender
        let _sender = manager.sender();

        // Verify empty read
        let segments = manager.read_group_segments(1).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn test_manifest_manager_async_write() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 1024,
            min_index: Some(1),
            max_index: Some(100),
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        // Send async (try_send for test since send() is async)
        sender.try_send(meta).unwrap();

        // Give worker time to process
        std::thread::sleep(Duration::from_millis(100));

        let segments = manager.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments.get(&0).unwrap().valid_bytes, 1024);
    }

    #[test]
    fn test_manifest_manager_coalesces_updates() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        // Send multiple updates to the same segment rapidly
        for i in 1..=10u64 {
            let meta = SegmentMeta {
                group_id: 1,
                segment_id: 0,
                valid_bytes: i * 100,
                min_index: Some(1),
                max_index: Some(i * 10),
                min_ts: None,
                max_ts: None,
                sealed: i == 10,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            };
            sender.try_send(meta).unwrap();
        }

        // Give worker time to process
        std::thread::sleep(Duration::from_millis(200));

        let segments = manager.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 1);

        let meta = segments.get(&0).unwrap();
        // Should have the max valid_bytes (1000) due to merge
        assert_eq!(meta.valid_bytes, 1000);
        // Should have the max of max_index (100)
        assert_eq!(meta.max_index, Some(100));
        // Should be sealed (OR of all sealed flags)
        assert!(meta.sealed);
    }

    #[test]
    fn test_manifest_manager_multiple_groups() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        // Send updates for multiple groups
        for group_id in 1..=3u64 {
            let meta = SegmentMeta {
                group_id,
                segment_id: 0,
                valid_bytes: group_id * 100,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            };
            sender.try_send(meta).unwrap();
        }

        std::thread::sleep(Duration::from_millis(200));

        for group_id in 1..=3u64 {
            let segments = manager.read_group_segments(group_id).unwrap();
            assert_eq!(segments.len(), 1);
            assert_eq!(segments.get(&0).unwrap().valid_bytes, group_id * 100);
        }
    }

    #[test]
    fn test_manifest_manager_clone() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let manager2 = manager.clone();

        // Both should reference the same manifest
        let sender1 = manager.sender();
        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 1024,
            min_index: None,
            max_index: None,
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };
        sender1.try_send(meta).unwrap();

        std::thread::sleep(Duration::from_millis(100));

        // Both managers should see the same data
        let seg1 = manager.read_group_segments(1).unwrap();
        let seg2 = manager2.read_group_segments(1).unwrap();
        assert_eq!(seg1.len(), 1);
        assert_eq!(seg2.len(), 1);
        assert_eq!(
            seg1.get(&0).unwrap().valid_bytes,
            seg2.get(&0).unwrap().valid_bytes
        );
    }

    #[test]
    fn test_manifest_manager_batch_updates() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        // Send many updates rapidly to test batching
        for seg_id in 0..100u64 {
            let meta = SegmentMeta {
                group_id: 1,
                segment_id: seg_id,
                valid_bytes: seg_id * 1000,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            };
            sender.try_send(meta).unwrap();
        }

        // Give worker time to process all
        std::thread::sleep(Duration::from_millis(500));

        let segments = manager.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 100);

        for seg_id in 0..100u64 {
            assert_eq!(segments.get(&seg_id).unwrap().valid_bytes, seg_id * 1000);
        }
    }

    // ===================================================================
    // Reliability — additional coverage
    // ===================================================================

    #[test]
    fn test_manifest_manager_stop_idempotent() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();

        // Calling stop twice should not panic
        manager.stop();
        manager.stop();
    }

    #[test]
    fn test_manifest_manager_read_after_stop() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 512,
            min_index: Some(1),
            max_index: Some(10),
            min_ts: None,
            max_ts: None,
            sealed: true,
            record_count: 5,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };
        sender.try_send(meta).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        manager.stop();

        // Reads should still work after stop (direct manifest read)
        let segments = manager.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments.get(&0).unwrap().valid_bytes, 512);
    }

    #[test]
    fn test_manifest_open_reopen_persistence() {
        let tmp = TestTempDir::new();

        // Write via manager
        {
            let manager = ManifestManager::open(tmp.path()).unwrap();
            let sender = manager.sender();
            let meta = SegmentMeta {
                group_id: 5,
                segment_id: 3,
                valid_bytes: 9999,
                min_index: Some(100),
                max_index: Some(200),
                min_ts: Some(1000),
                max_ts: Some(2000),
                sealed: true,
                record_count: 50,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            };
            sender.try_send(meta).unwrap();
            std::thread::sleep(Duration::from_millis(200));
            manager.stop();
        }

        // Reopen and verify
        {
            let manager = ManifestManager::open(tmp.path()).unwrap();
            let segments = manager.read_group_segments(5).unwrap();
            assert_eq!(segments.len(), 1);
            let meta = segments.get(&3).unwrap();
            assert_eq!(meta.valid_bytes, 9999);
            assert_eq!(meta.min_index, Some(100));
            assert_eq!(meta.max_index, Some(200));
            assert!(meta.sealed);
            manager.stop();
        }
    }

    #[test]
    fn test_manifest_empty_group_read() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        // Non-existent groups return empty HashMap
        for gid in [0, 1, 42, u64::MAX] {
            let segments = manifest.read_group_segments(gid).unwrap();
            assert!(segments.is_empty());
        }
    }

    // ===================================================================
    // SegmentMeta — additional edge cases
    // ===================================================================

    #[test]
    fn test_segment_meta_boundary_values() {
        // group_id=0, segment_id=0
        let meta = SegmentMeta {
            group_id: 0,
            segment_id: 0,
            valid_bytes: 0,
            min_index: Some(0),
            max_index: Some(0),
            min_ts: Some(0),
            max_ts: Some(0),
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };
        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(0, 0, &encoded).unwrap();
        assert_eq!(decoded.group_id, 0);
        assert_eq!(decoded.segment_id, 0);
        assert_eq!(decoded.min_index, Some(0));
        assert_eq!(decoded.max_index, Some(0));
    }

    #[test]
    fn test_segment_meta_key_ordering() {
        // Keys should sort correctly for MDBX range scans
        let m1 = SegmentMeta {
            group_id: 1,
            segment_id: 1,
            ..make_segment_meta(1, 1)
        };
        let m2 = SegmentMeta {
            group_id: 1,
            segment_id: 2,
            ..make_segment_meta(1, 2)
        };
        let m3 = SegmentMeta {
            group_id: 2,
            segment_id: 0,
            ..make_segment_meta(2, 0)
        };

        let k1 = m1.key_bytes();
        let k2 = m2.key_bytes();
        let k3 = m3.key_bytes();

        // Big-endian ordering: group 1 < group 2, and within group 1: seg 1 < seg 2
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn test_segment_meta_decode_wrong_length_variants() {
        // Various wrong-length payloads
        for len in [0, 1, 15, 55, 57, 100, 255] {
            let buf = vec![0u8; len];
            assert!(SegmentMeta::decode_value(1, 1, &buf).is_none());
        }
    }

    #[test]
    fn test_segment_meta_merge_all_fields_comprehensive() {
        let mut base = SegmentMeta {
            group_id: 1,
            segment_id: 1,
            valid_bytes: 100,
            min_index: Some(10),
            max_index: Some(50),
            min_ts: Some(500),
            max_ts: Some(1500),
            sealed: false,
            record_count: 5,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let other = SegmentMeta {
            group_id: 1,
            segment_id: 1,
            valid_bytes: 200,
            min_index: Some(5),
            max_index: Some(100),
            min_ts: Some(100),
            max_ts: Some(3000),
            sealed: true,
            record_count: 10,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        base.merge_from(other);

        assert_eq!(base.valid_bytes, 200); // max
        assert_eq!(base.min_index, Some(5)); // min
        assert_eq!(base.max_index, Some(100)); // max
        assert_eq!(base.min_ts, Some(100)); // min
        assert_eq!(base.max_ts, Some(3000)); // max
        assert!(base.sealed); // OR
        assert_eq!(base.record_count, 10); // max
    }

    #[test]
    fn test_segment_meta_key_bytes_roundtrip() {
        // key_bytes() → extract group_id and segment_id from bytes
        let meta = SegmentMeta {
            group_id: 12345,
            segment_id: 67890,
            ..make_segment_meta(12345, 67890)
        };
        let key = meta.key_bytes();
        let gid = u64::from_be_bytes(key[0..8].try_into().unwrap());
        let sid = u64::from_be_bytes(key[8..16].try_into().unwrap());
        assert_eq!(gid, 12345);
        assert_eq!(sid, 67890);
    }

    // ===================================================================
    // MdbxManifest — additional coverage
    // ===================================================================

    #[test]
    fn test_manifest_many_segments() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        // 1000 segments in one group
        let metas: Vec<_> = (0..1000u64)
            .map(|i| SegmentMeta {
                group_id: 1,
                segment_id: i,
                valid_bytes: i * 100,
                min_index: Some(i * 10),
                max_index: Some((i + 1) * 10 - 1),
                min_ts: None,
                max_ts: None,
                sealed: true,
                record_count: 10,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            })
            .collect();

        manifest.apply_segment_updates(metas.into_iter()).unwrap();

        let segments = manifest.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 1000);
        assert_eq!(segments.get(&0).unwrap().valid_bytes, 0);
        assert_eq!(segments.get(&999).unwrap().valid_bytes, 99900);
    }

    #[test]
    fn test_manifest_many_groups() {
        let tmp = TestTempDir::new();
        let manifest = MdbxManifest::open(tmp.path()).unwrap();

        // 100 groups with 10 segments each
        let metas: Vec<_> = (0..100u64)
            .flat_map(|g| {
                (0..10u64).map(move |s| SegmentMeta {
                    group_id: g,
                    segment_id: s,
                    valid_bytes: g * 1000 + s * 100,
                    min_index: None,
                    max_index: None,
                    min_ts: None,
                    max_ts: None,
                    sealed: false,
                    record_count: 0,
                    record_type_flags: RecordTypeFlags::default(),
                    entry_count: 0,
                    first_entry_offset: 0,
                    location: SegmentLocation::default(),
                })
            })
            .collect();

        manifest.apply_segment_updates(metas.into_iter()).unwrap();

        for g in 0..100u64 {
            let segments = manifest.read_group_segments(g).unwrap();
            assert_eq!(segments.len(), 10);
            for s in 0..10u64 {
                assert_eq!(segments.get(&s).unwrap().valid_bytes, g * 1000 + s * 100);
            }
        }
    }

    // ===================================================================
    // ManifestManager — concurrency
    // ===================================================================

    #[test]
    fn test_manifest_concurrent_read_write() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        // Send updates from multiple threads while reading
        let mut handles = Vec::new();
        for group_id in 0..10u64 {
            let s = sender.clone();
            handles.push(std::thread::spawn(move || {
                for seg_id in 0..10u64 {
                    let meta = SegmentMeta {
                        group_id,
                        segment_id: seg_id,
                        valid_bytes: seg_id * 100,
                        min_index: None,
                        max_index: None,
                        min_ts: None,
                        max_ts: None,
                        sealed: false,
                        record_count: 0,
                        record_type_flags: RecordTypeFlags::default(),
                        entry_count: 0,
                        first_entry_offset: 0,
                        location: SegmentLocation::default(),
                    };
                    let _ = s.try_send(meta);
                }
            }));
        }

        // Concurrent reads should not corrupt
        let mgr_clone = manager.clone();
        let read_handle = std::thread::spawn(move || {
            for _ in 0..50 {
                for g in 0..10 {
                    let _ = mgr_clone.read_group_segments(g);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        for h in handles {
            h.join().unwrap();
        }
        read_handle.join().unwrap();

        // Wait for worker to flush
        std::thread::sleep(Duration::from_millis(300));

        // All groups should have their segments
        for g in 0..10u64 {
            let segments = manager.read_group_segments(g).unwrap();
            assert_eq!(segments.len(), 10, "group {} missing segments", g);
        }

        manager.stop();
    }

    #[test]
    fn test_manifest_worker_batch_timing() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        // Send 100 updates rapidly
        let start = std::time::Instant::now();
        for i in 0..100u64 {
            let meta = SegmentMeta {
                group_id: 1,
                segment_id: i,
                valid_bytes: i * 100,
                min_index: None,
                max_index: None,
                min_ts: None,
                max_ts: None,
                sealed: false,
                record_count: 0,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 0,
                first_entry_offset: 0,
                location: SegmentLocation::default(),
            };
            sender.try_send(meta).unwrap();
        }

        // Wait for all to be processed
        std::thread::sleep(Duration::from_millis(500));

        let segments = manager.read_group_segments(1).unwrap();
        assert_eq!(segments.len(), 100);

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "Processing took too long: {:?}",
            elapsed
        );

        manager.stop();
    }

    #[test]
    fn test_manifest_sender_dropped_before_stop() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 100,
            min_index: None,
            max_index: None,
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };
        sender.try_send(meta).unwrap();
        drop(sender);

        // Worker should handle sender being dropped gracefully
        std::thread::sleep(Duration::from_millis(200));
        manager.stop();
    }

    #[test]
    fn test_manifest_concurrent_stop_and_send() {
        let tmp = TestTempDir::new();
        let manager = ManifestManager::open(tmp.path()).unwrap();
        let sender = manager.sender();

        // Spawn a thread that sends while main thread stops
        let s = sender.clone();
        let send_handle = std::thread::spawn(move || {
            for i in 0..100u64 {
                let meta = SegmentMeta {
                    group_id: 1,
                    segment_id: i,
                    valid_bytes: i * 100,
                    min_index: None,
                    max_index: None,
                    min_ts: None,
                    max_ts: None,
                    sealed: false,
                    record_count: 0,
                    record_type_flags: RecordTypeFlags::default(),
                    entry_count: 0,
                    first_entry_offset: 0,
                    location: SegmentLocation::default(),
                };
                if s.try_send(meta).is_err() {
                    break; // Channel closed
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        std::thread::sleep(Duration::from_millis(10));
        manager.stop();
        send_handle.join().unwrap();
    }

    #[test]
    fn test_manifest_encode_decode_record_type_flags() {
        use crate::record_format::RecordTypeFlags;

        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 100,
            min_index: None,
            max_index: None,
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 0,
            record_type_flags: RecordTypeFlags::from_u8(0b1111), // all flags set
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(1, 0, &encoded).unwrap();
        assert_eq!(decoded.record_type_flags.to_u8(), 0b1111);
    }

    #[test]
    fn test_manifest_encode_decode_record_count() {
        let meta = SegmentMeta {
            group_id: 1,
            segment_id: 0,
            valid_bytes: 100,
            min_index: None,
            max_index: None,
            min_ts: None,
            max_ts: None,
            sealed: false,
            record_count: 999_999,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 0,
            first_entry_offset: 0,
            location: SegmentLocation::default(),
        };

        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(1, 0, &encoded).unwrap();
        assert_eq!(decoded.record_count, 999_999);
    }
}
