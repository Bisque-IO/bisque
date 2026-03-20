//! WAL-based manifest log for crash-consistent metadata persistence.
//!
//! Replaces MDBX with a simple append-only WAL and periodic snapshotting.
//! The WAL stores two kinds of entries:
//!   - **Segment metadata** (kind 0): fixed-size updates for raft log segments
//!   - **Engine blobs** (kind 1): opaque data from the engine (e.g., bisque-mq manifests)
//!
//! When the WAL exceeds a configured size threshold, the caller triggers a
//! snapshot rotation: the current in-memory state is serialized to a snapshot
//! file, a new WAL is started, and the old WAL is deleted.
//!
//! ## File layout
//!
//! ```text
//! {dir}/snapshot.bin       – latest snapshot (atomic rename from snapshot.tmp)
//! {dir}/wal_{seqno:06}.bin – WAL files (only the latest is active)
//! ```
//!
//! ## WAL file format
//!
//! ```text
//! [magic:8 "BQMFWAL\0"][seqno:u64 LE]   – 16-byte header
//! [entry]*                                – variable number of entries
//! ```
//!
//! ## WAL entry format
//!
//! ```text
//! [len:u32 LE][data:len bytes][crc64:u64 LE]
//! ```
//!
//! The `data` bytes are interpreted by the caller (first byte is the entry kind).
//!
//! ## Snapshot format
//!
//! ```text
//! [magic:8 "BQMFSNP\0"][wal_seqno:u64 LE]   – 16-byte header
//! [data_len:u64 LE][data bytes][crc64:u64 LE]
//! ```

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use crc_fast::{CrcAlgorithm, Digest};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::record_format::RecordTypeFlags;

const WAL_MAGIC: [u8; 8] = *b"BQMFWAL\0";
const SNAP_MAGIC: [u8; 8] = *b"BQMFSNP\0";
const FILE_HEADER_LEN: usize = 16;

/// Entry kind: raft segment metadata update.
pub const KIND_SEGMENT_META: u8 = 0;
/// Entry kind: opaque engine manifest blob.
pub const KIND_ENGINE_BLOB: u8 = 1;

// ---------------------------------------------------------------------------
// SegmentLocation & SegmentMeta (moved from manifest_mdbx.rs)
// ---------------------------------------------------------------------------

/// Where a segment's data resides in the tiered storage hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SegmentLocation {
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
    pub fn has_local(&self) -> bool {
        matches!(self, Self::Local | Self::Both)
    }

    /// Returns true if the segment has been archived remotely.
    pub fn has_remote(&self) -> bool {
        matches!(self, Self::Remote | Self::Both)
    }
}

/// Metadata for a single raft log segment.
#[derive(Debug, Clone, Copy)]
pub struct SegmentMeta {
    pub group_id: u64,
    pub segment_id: u64,
    /// Logical end offset (last valid record boundary).
    pub valid_bytes: u64,
    /// Optional entry index range present in this segment.
    pub min_index: Option<u64>,
    pub max_index: Option<u64>,
    /// Optional write-time range (unix nanos) for entries in this segment.
    pub min_ts: Option<u64>,
    pub max_ts: Option<u64>,
    /// True if this segment is sealed (not the current head).
    pub sealed: bool,
    /// Number of records in this segment.
    pub record_count: u64,
    /// Record types present in this segment (entries, votes, truncates, purges).
    /// Used for fast-path recovery: entry-only segments skip full CRC decode.
    pub record_type_flags: RecordTypeFlags,
    /// Number of Entry records (vs total record_count which includes votes, etc.)
    pub entry_count: u64,
    /// Byte offset of the first Entry record in the segment.
    pub first_entry_offset: u64,
    /// Where this segment's data resides (local, remote, or both).
    pub location: SegmentLocation,
}

impl SegmentMeta {
    /// Monotonically merge another meta into this one. All fields grow/expand.
    pub fn merge_from(&mut self, other: SegmentMeta) {
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

    /// Encode segment meta as a WAL entry payload: [KIND_SEGMENT_META:1][group_id:8 LE][segment_id:8 LE][value:72]
    pub fn encode_entry(&self) -> [u8; 89] {
        let mut buf = [0u8; 89];
        buf[0] = KIND_SEGMENT_META;
        buf[1..9].copy_from_slice(&self.group_id.to_le_bytes());
        buf[9..17].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[17..89].copy_from_slice(&self.encode_value());
        buf
    }

    /// Decode a segment meta from a WAL entry payload (89 bytes starting with KIND_SEGMENT_META).
    pub fn decode_entry(data: &[u8]) -> Option<Self> {
        if data.len() != 89 || data[0] != KIND_SEGMENT_META {
            return None;
        }
        let group_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let segment_id = u64::from_le_bytes(data[9..17].try_into().ok()?);
        Self::decode_value(group_id, segment_id, &data[17..89])
    }

    /// Encode the 72-byte value (matching the existing binary format).
    pub fn encode_value(&self) -> [u8; 72] {
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
        v[16..24].copy_from_slice(&self.min_index.unwrap_or(0).to_be_bytes());
        v[24..32].copy_from_slice(&self.max_index.unwrap_or(0).to_be_bytes());
        v[32..40].copy_from_slice(&self.min_ts.unwrap_or(0).to_be_bytes());
        v[40..48].copy_from_slice(&self.max_ts.unwrap_or(0).to_be_bytes());
        v[48..56].copy_from_slice(&self.record_count.to_be_bytes());
        v[56..64].copy_from_slice(&self.entry_count.to_be_bytes());
        v[64..72].copy_from_slice(&self.first_entry_offset.to_be_bytes());
        v
    }

    /// Decode from the 72-byte value encoding.
    pub fn decode_value(group_id: u64, segment_id: u64, bytes: &[u8]) -> Option<Self> {
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

    /// Encode for the snapshot: [group_id:8 LE][segment_id:8 LE][value:72] = 88 bytes.
    fn encode_snapshot_entry(&self) -> [u8; 88] {
        let mut buf = [0u8; 88];
        buf[0..8].copy_from_slice(&self.group_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[16..88].copy_from_slice(&self.encode_value());
        buf
    }

    /// Decode from the 88-byte snapshot entry.
    fn decode_snapshot_entry(data: &[u8]) -> Option<Self> {
        if data.len() != 88 {
            return None;
        }
        let group_id = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let segment_id = u64::from_le_bytes(data[8..16].try_into().ok()?);
        Self::decode_value(group_id, segment_id, &data[16..88])
    }
}

// ---------------------------------------------------------------------------
// ManifestLog — the core async WAL + snapshot manager
// ---------------------------------------------------------------------------

/// Recovered state from opening a manifest log.
pub struct ManifestRecovery {
    /// Recovered segment metadata, keyed by (group_id, segment_id).
    pub segments: HashMap<(u64, u64), SegmentMeta>,
    /// Recovered engine blobs in order, as (group_id, raft_log_index, blob).
    pub engine_blobs: Vec<(u64, u64, Vec<u8>)>,
}

/// A simple WAL-based manifest log for crash-consistent metadata persistence.
///
/// The caller appends entries (segment metadata updates or engine blobs) to the
/// WAL. When the WAL exceeds the configured size threshold, the caller should
/// call [`rotate`] with a serialized snapshot of the engine's manifest state.
pub struct ManifestLog {
    dir: PathBuf,
    wal: fs::File,
    wal_seqno: u64,
    wal_size: u64,
    max_wal_size: u64,
    /// In-memory segment metadata, maintained during appends for snapshot generation.
    segments: HashMap<(u64, u64), SegmentMeta>,
}

impl ManifestLog {
    /// Open or create a manifest log at `dir`.
    ///
    /// Recovers from existing snapshot + WAL files. Returns the manifest log
    /// and any recovered engine blobs that the caller must replay.
    pub async fn open(
        dir: impl AsRef<Path>,
        max_wal_size: u64,
    ) -> io::Result<(Self, ManifestRecovery)> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;

        // Clean up incomplete snapshot from a previous crash.
        let snap_tmp = dir.join("snapshot.tmp");
        if fs::try_exists(&snap_tmp).await.unwrap_or(false) {
            fs::remove_file(&snap_tmp).await?;
        }

        // Load snapshot.
        let snap_path = dir.join("snapshot.bin");
        let (snap_segments, snap_engine_blobs, snap_wal_seqno) = read_snapshot(&snap_path).await?;

        // Seed in-memory segments from snapshot.
        let mut segments: HashMap<(u64, u64), SegmentMeta> = snap_segments
            .into_iter()
            .map(|m| ((m.group_id, m.segment_id), m))
            .collect();

        let mut engine_blobs = snap_engine_blobs;

        // Find WAL files and replay post-snapshot entries.
        let mut wal_files = find_wal_files(&dir).await?;
        wal_files.sort_by_key(|(seqno, _)| *seqno);

        let mut last_valid_end = FILE_HEADER_LEN as u64;
        let mut active_wal_seqno = None;

        for (seqno, path) in &wal_files {
            if *seqno > snap_wal_seqno {
                last_valid_end = replay_wal_entries(path, &mut segments, &mut engine_blobs).await?;
                active_wal_seqno = Some(*seqno);
            }
        }

        // Clean up pre-snapshot WAL files.
        for (seqno, path) in &wal_files {
            if *seqno <= snap_wal_seqno {
                let _ = fs::remove_file(path).await;
            }
        }

        // Open or create the active WAL.
        let (wal, wal_seqno, wal_size) = match active_wal_seqno {
            Some(seqno) => {
                let path = wal_path(&dir, seqno);
                let file = fs::OpenOptions::new().write(true).open(&path).await?;
                // Truncate past the last valid entry (handles torn writes).
                file.set_len(last_valid_end).await?;
                // Seek to end for appending. tokio::fs::File supports seek via
                // spawn_blocking — we just need to position it correctly.
                // set_len doesn't move the cursor, so we seek explicitly.
                let file = file.into_std().await;
                use std::io::{Seek, SeekFrom};
                let mut file = file;
                file.seek(SeekFrom::End(0))?;
                let file = fs::File::from_std(file);
                (file, seqno, last_valid_end)
            }
            None => {
                let seqno = snap_wal_seqno + 1;
                let file = create_wal_file(&dir, seqno).await?;
                (file, seqno, FILE_HEADER_LEN as u64)
            }
        };

        let manifest = ManifestLog {
            dir,
            wal,
            wal_seqno,
            wal_size,
            max_wal_size,
            segments,
        };

        let recovery = ManifestRecovery {
            segments: manifest.segments.clone(),
            engine_blobs,
        };

        Ok((manifest, recovery))
    }

    /// Append a segment metadata update to the WAL.
    ///
    /// Merges the update into the in-memory state and writes to the WAL.
    /// Returns `true` if the WAL has exceeded the size threshold.
    pub async fn append_segment_meta(&mut self, meta: SegmentMeta) -> io::Result<bool> {
        let key = (meta.group_id, meta.segment_id);
        self.segments
            .entry(key)
            .and_modify(|m| m.merge_from(meta))
            .or_insert(meta);

        let entry = meta.encode_entry();
        self.append_raw(&entry).await
    }

    /// Append an engine manifest blob to the WAL.
    ///
    /// The blob is opaque to the manifest log. The engine interprets it during
    /// recovery via the returned [`ManifestRecovery::engine_blobs`].
    /// Returns `true` if the WAL has exceeded the size threshold.
    pub async fn append_engine_blob(
        &mut self,
        group_id: u64,
        raft_log_index: u64,
        blob: &[u8],
    ) -> io::Result<bool> {
        // [KIND_ENGINE_BLOB:1][group_id:8 LE][raft_log_index:8 LE][blob...]
        let mut entry = Vec::with_capacity(1 + 8 + 8 + blob.len());
        entry.push(KIND_ENGINE_BLOB);
        entry.extend_from_slice(&group_id.to_le_bytes());
        entry.extend_from_slice(&raft_log_index.to_le_bytes());
        entry.extend_from_slice(blob);
        self.append_raw(&entry).await
    }

    /// Remove all segment metadata for a given group.
    pub fn remove_group_segments(&mut self, group_id: u64) {
        self.segments.retain(|&(gid, _), _| gid != group_id);
    }

    /// Read segment metadata for a group from the in-memory state.
    pub fn read_group_segments(&self, group_id: u64) -> HashMap<u64, SegmentMeta> {
        self.segments
            .iter()
            .filter(|&(&(gid, _), _)| gid == group_id)
            .map(|(&(_, sid), meta)| (sid, *meta))
            .collect()
    }

    /// Get a reference to all segment metadata.
    pub fn segments(&self) -> &HashMap<(u64, u64), SegmentMeta> {
        &self.segments
    }

    /// Current WAL file size in bytes.
    pub fn wal_size(&self) -> u64 {
        self.wal_size
    }

    /// Returns true if the WAL has exceeded the configured size threshold.
    pub fn needs_rotation(&self) -> bool {
        self.wal_size >= self.max_wal_size
    }

    /// Sync the WAL to disk.
    pub async fn sync(&self) -> io::Result<()> {
        self.wal.sync_data().await
    }

    /// Rotate the WAL: write a snapshot, create a new WAL, delete the old one.
    ///
    /// `engine_snapshot` is the opaque serialized engine manifest state. Pass an
    /// empty slice if the engine has no state to snapshot.
    pub async fn rotate(&mut self, engine_snapshot: &[u8]) -> io::Result<()> {
        // Flush and sync current WAL.
        self.wal.flush().await?;
        self.wal.sync_data().await?;

        // Write snapshot atomically via tmp + rename.
        write_snapshot(&self.dir, self.wal_seqno, &self.segments, engine_snapshot).await?;

        // Create new WAL.
        let new_seqno = self.wal_seqno + 1;
        let new_wal = create_wal_file(&self.dir, new_seqno).await?;

        // Delete old WAL.
        let old_path = wal_path(&self.dir, self.wal_seqno);
        let _ = fs::remove_file(&old_path).await;

        self.wal = new_wal;
        self.wal_seqno = new_seqno;
        self.wal_size = FILE_HEADER_LEN as u64;

        Ok(())
    }

    /// Append raw entry bytes to the WAL with CRC64 framing.
    /// Builds the full frame in memory and writes in a single call.
    async fn append_raw(&mut self, data: &[u8]) -> io::Result<bool> {
        let len = data.len() as u32;
        let crc = compute_crc(data);

        // Single write to minimize spawn_blocking calls.
        let mut buf = Vec::with_capacity(4 + data.len() + 8);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(data);
        buf.extend_from_slice(&crc.to_le_bytes());

        self.wal.write_all(&buf).await?;
        self.wal_size += buf.len() as u64;
        Ok(self.wal_size >= self.max_wal_size)
    }
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

fn wal_path(dir: &Path, seqno: u64) -> PathBuf {
    dir.join(format!("wal_{seqno:06}.bin"))
}

async fn create_wal_file(dir: &Path, seqno: u64) -> io::Result<fs::File> {
    let path = wal_path(dir, seqno);
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await?;

    let mut header = [0u8; FILE_HEADER_LEN];
    header[..8].copy_from_slice(&WAL_MAGIC);
    header[8..16].copy_from_slice(&seqno.to_le_bytes());
    file.write_all(&header).await?;
    file.sync_data().await?;
    Ok(file)
}

async fn find_wal_files(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut result = Vec::new();
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("wal_") {
            if let Some(seqno_str) = rest.strip_suffix(".bin") {
                if let Ok(seqno) = seqno_str.parse::<u64>() {
                    result.push((seqno, entry.path()));
                }
            }
        }
    }
    Ok(result)
}

fn compute_crc(data: &[u8]) -> u64 {
    let mut digest = Digest::new(CrcAlgorithm::Crc64Nvme);
    digest.update(data);
    digest.finalize()
}

// ---------------------------------------------------------------------------
// Snapshot read/write
// ---------------------------------------------------------------------------

/// Snapshot format:
/// ```text
/// [SNAP_MAGIC:8][wal_seqno:8 LE]                        – 16-byte header
/// [segment_count:u32 LE][segments: segment_count * 88B]  – segment metadata
/// [engine_len:u64 LE][engine_data: engine_len bytes]     – engine snapshot
/// [crc64:8 LE]                                           – CRC over everything after header
/// ```
async fn write_snapshot(
    dir: &Path,
    wal_seqno: u64,
    segments: &HashMap<(u64, u64), SegmentMeta>,
    engine_data: &[u8],
) -> io::Result<()> {
    let tmp_path = dir.join("snapshot.tmp");
    let final_path = dir.join("snapshot.bin");

    // Build the entire snapshot in memory (small data).
    let mut buf = Vec::new();

    // Header.
    buf.extend_from_slice(&SNAP_MAGIC);
    buf.extend_from_slice(&wal_seqno.to_le_bytes());

    // Body starts here — CRC covers everything after the header.
    let body_start = buf.len();

    // Segment metadata section.
    let count = segments.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());
    for meta in segments.values() {
        buf.extend_from_slice(&meta.encode_snapshot_entry());
    }

    // Engine snapshot section.
    buf.extend_from_slice(&(engine_data.len() as u64).to_le_bytes());
    buf.extend_from_slice(engine_data);

    // CRC over body.
    let crc = compute_crc(&buf[body_start..]);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Write atomically: tmp + fsync + rename.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;
    file.write_all(&buf).await?;
    file.sync_data().await?;
    drop(file);

    fs::rename(&tmp_path, &final_path).await?;

    // fsync directory for rename durability.
    let dir_file = fs::File::open(dir).await?;
    dir_file.sync_all().await?;

    Ok(())
}

/// Read a snapshot file. Returns (segments, engine_blobs, wal_seqno).
async fn read_snapshot(
    path: &Path,
) -> io::Result<(Vec<SegmentMeta>, Vec<(u64, u64, Vec<u8>)>, u64)> {
    if !fs::try_exists(path).await.unwrap_or(false) {
        return Ok((Vec::new(), Vec::new(), 0));
    }

    let data = fs::read(path).await?;
    if data.len() < FILE_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot too small",
        ));
    }
    if data[..8] != SNAP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad snapshot magic",
        ));
    }

    let wal_seqno = u64::from_le_bytes(data[8..16].try_into().unwrap());

    // Body is everything after the 16-byte header except the trailing 8-byte CRC.
    if data.len() < FILE_HEADER_LEN + 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot truncated",
        ));
    }
    let body = &data[FILE_HEADER_LEN..data.len() - 8];
    let stored_crc = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
    let computed_crc = compute_crc(body);
    if stored_crc != computed_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot CRC mismatch",
        ));
    }

    let mut pos = 0;

    // Parse segment metadata.
    if body.len() < pos + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot: missing segment count",
        ));
    }
    let seg_count = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    let mut segments = Vec::with_capacity(seg_count);
    for _ in 0..seg_count {
        if body.len() < pos + 88 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot: truncated segment entry",
            ));
        }
        match SegmentMeta::decode_snapshot_entry(&body[pos..pos + 88]) {
            Some(meta) => segments.push(meta),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot: invalid segment entry",
                ));
            }
        }
        pos += 88;
    }

    // Parse engine snapshot.
    if body.len() < pos + 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot: missing engine data length",
        ));
    }
    let engine_len = u64::from_le_bytes(body[pos..pos + 8].try_into().unwrap()) as usize;
    pos += 8;

    if body.len() < pos + engine_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot: truncated engine data",
        ));
    }

    // Engine snapshot is returned as a single blob with group_id=0, raft_log_index=0
    // to distinguish it from WAL-based engine blobs. The caller knows how to interpret it.
    let engine_blobs = if engine_len > 0 {
        vec![(0, 0, body[pos..pos + engine_len].to_vec())]
    } else {
        Vec::new()
    };

    Ok((segments, engine_blobs, wal_seqno))
}

// ---------------------------------------------------------------------------
// WAL reading
// ---------------------------------------------------------------------------

/// Read and replay WAL entries into the in-memory state.
/// Returns the byte offset of the last valid entry end (for truncation).
async fn replay_wal_entries(
    path: &Path,
    segments: &mut HashMap<(u64, u64), SegmentMeta>,
    engine_blobs: &mut Vec<(u64, u64, Vec<u8>)>,
) -> io::Result<u64> {
    let data = fs::read(path).await?;
    if data.len() < FILE_HEADER_LEN {
        return Ok(FILE_HEADER_LEN as u64);
    }
    if data[..8] != WAL_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad WAL magic"));
    }

    let mut pos = FILE_HEADER_LEN;
    let mut last_valid_pos = pos;

    while pos + 4 <= data.len() {
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let entry_end = pos + 4 + len + 8;

        if entry_end > data.len() {
            break; // Truncated entry (crash during write).
        }

        let payload = &data[pos + 4..pos + 4 + len];
        let stored_crc = u64::from_le_bytes(data[pos + 4 + len..entry_end].try_into().unwrap());
        let computed_crc = compute_crc(payload);

        if stored_crc != computed_crc {
            break; // Corrupted entry (crash during write).
        }

        // Interpret the entry.
        if !payload.is_empty() {
            match payload[0] {
                KIND_SEGMENT_META => {
                    if let Some(meta) = SegmentMeta::decode_entry(payload) {
                        let key = (meta.group_id, meta.segment_id);
                        segments
                            .entry(key)
                            .and_modify(|m| m.merge_from(meta))
                            .or_insert(meta);
                    }
                }
                KIND_ENGINE_BLOB => {
                    if payload.len() >= 17 {
                        let group_id = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                        let raft_log_index = u64::from_le_bytes(payload[9..17].try_into().unwrap());
                        let blob = payload[17..].to_vec();
                        engine_blobs.push((group_id, raft_log_index, blob));
                    }
                }
                _ => {} // Unknown kind — skip.
            }
        }

        pos = entry_end;
        last_valid_pos = pos;
    }

    Ok(last_valid_pos as u64)
}

// ---------------------------------------------------------------------------
// ManifestWriter — async task-based wrapper
// ---------------------------------------------------------------------------

/// Commands sent to the manifest worker task.
pub enum ManifestCommand {
    SegmentUpdate(SegmentMeta),
    EngineBlob {
        group_id: u64,
        raft_log_index: u64,
        blob: Vec<u8>,
    },
    Rotate {
        engine_snapshot: Vec<u8>,
        done: tokio::sync::oneshot::Sender<io::Result<()>>,
    },
    Sync {
        done: tokio::sync::oneshot::Sender<io::Result<()>>,
    },
}

/// Async wrapper around [`ManifestLog`] with a dedicated tokio task.
///
/// All writes are dispatched to a background task via `tokio::sync::mpsc`.
/// Segment metadata updates are fire-and-forget; snapshots and syncs can
/// be awaited for completion.
#[derive(Clone)]
pub struct ManifestWriter {
    tx: tokio::sync::mpsc::Sender<ManifestCommand>,
}

impl ManifestWriter {
    /// Open a manifest log and start the background writer task.
    pub async fn open(
        dir: impl AsRef<Path>,
        max_wal_size: u64,
    ) -> io::Result<(Self, ManifestRecovery)> {
        let (manifest, recovery) = ManifestLog::open(dir, max_wal_size).await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<ManifestCommand>(4096);

        tokio::spawn(Self::worker_loop(manifest, rx));

        let writer = ManifestWriter { tx };

        Ok((writer, recovery))
    }

    /// Send a segment metadata update (fire-and-forget).
    pub fn send_segment_update(&self, meta: SegmentMeta) {
        let _ = self.tx.try_send(ManifestCommand::SegmentUpdate(meta));
    }

    /// Send an engine blob (fire-and-forget).
    pub fn send_engine_blob(&self, group_id: u64, raft_log_index: u64, blob: Vec<u8>) {
        let _ = self.tx.try_send(ManifestCommand::EngineBlob {
            group_id,
            raft_log_index,
            blob,
        });
    }

    /// Request a snapshot rotation and wait for completion.
    pub async fn rotate(&self, engine_snapshot: Vec<u8>) -> io::Result<()> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ManifestCommand::Rotate {
                engine_snapshot,
                done: done_tx,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manifest worker gone"))?;
        done_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manifest worker gone"))?
    }

    /// Request a WAL sync and wait for completion.
    pub async fn sync(&self) -> io::Result<()> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ManifestCommand::Sync { done: done_tx })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manifest worker gone"))?;
        done_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "manifest worker gone"))?
    }

    async fn worker_loop(
        mut manifest: ManifestLog,
        mut rx: tokio::sync::mpsc::Receiver<ManifestCommand>,
    ) {
        while let Some(cmd) = rx.recv().await {
            Self::apply_command(&mut manifest, cmd).await;

            // Drain buffered commands without waiting.
            while let Ok(cmd) = rx.try_recv() {
                Self::apply_command(&mut manifest, cmd).await;
            }
        }

        // Channel closed — flush.
        let _ = manifest.sync().await;
    }

    async fn apply_command(manifest: &mut ManifestLog, cmd: ManifestCommand) {
        match cmd {
            ManifestCommand::SegmentUpdate(meta) => {
                if let Err(e) = manifest.append_segment_meta(meta).await {
                    tracing::warn!(
                        group_id = meta.group_id,
                        segment_id = meta.segment_id,
                        "manifest WAL append failed: {e}"
                    );
                }
            }
            ManifestCommand::EngineBlob {
                group_id,
                raft_log_index,
                blob,
            } => {
                if let Err(e) = manifest
                    .append_engine_blob(group_id, raft_log_index, &blob)
                    .await
                {
                    tracing::warn!(
                        group_id,
                        raft_log_index,
                        "manifest engine blob append failed: {e}"
                    );
                }
            }
            ManifestCommand::Rotate {
                engine_snapshot,
                done,
            } => {
                let result = manifest.rotate(&engine_snapshot).await;
                let _ = done.send(result);
            }
            ManifestCommand::Sync { done } => {
                let result = manifest.sync().await;
                let _ = done.send(result);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestTempDir;

    fn make_segment_meta(group_id: u64, segment_id: u64) -> SegmentMeta {
        SegmentMeta {
            group_id,
            segment_id,
            valid_bytes: 4096,
            min_index: Some(1),
            max_index: Some(100),
            min_ts: Some(1000),
            max_ts: Some(2000),
            sealed: true,
            record_count: 50,
            record_type_flags: RecordTypeFlags {
                has_vote: false,
                has_entry: true,
                has_truncate: false,
                has_purge: false,
            },
            entry_count: 50,
            first_entry_offset: 16,
            location: SegmentLocation::Local,
        }
    }

    #[test]
    fn test_segment_meta_encode_decode_entry() {
        let meta = make_segment_meta(1, 42);
        let encoded = meta.encode_entry();
        let decoded = SegmentMeta::decode_entry(&encoded).unwrap();
        assert_eq!(decoded.group_id, 1);
        assert_eq!(decoded.segment_id, 42);
        assert_eq!(decoded.valid_bytes, 4096);
        assert_eq!(decoded.min_index, Some(1));
        assert_eq!(decoded.max_index, Some(100));
        assert_eq!(decoded.sealed, true);
        assert_eq!(decoded.entry_count, 50);
        assert_eq!(decoded.location, SegmentLocation::Local);
    }

    #[test]
    fn test_segment_meta_encode_decode_snapshot_entry() {
        let meta = make_segment_meta(5, 10);
        let encoded = meta.encode_snapshot_entry();
        let decoded = SegmentMeta::decode_snapshot_entry(&encoded).unwrap();
        assert_eq!(decoded.group_id, 5);
        assert_eq!(decoded.segment_id, 10);
        assert_eq!(decoded.valid_bytes, meta.valid_bytes);
    }

    #[tokio::test]
    async fn test_open_empty_directory() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");
        let (manifest, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
        assert!(recovery.segments.is_empty());
        assert!(recovery.engine_blobs.is_empty());
        assert_eq!(manifest.wal_size, FILE_HEADER_LEN as u64);
    }

    #[tokio::test]
    async fn test_append_and_recover_segment_meta() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        // Write some entries.
        {
            let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            manifest
                .append_segment_meta(make_segment_meta(1, 1))
                .await
                .unwrap();
            manifest
                .append_segment_meta(make_segment_meta(1, 2))
                .await
                .unwrap();
            manifest
                .append_segment_meta(make_segment_meta(2, 1))
                .await
                .unwrap();
            manifest.sync().await.unwrap();
        }

        // Reopen and verify recovery.
        {
            let (manifest, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            assert_eq!(recovery.segments.len(), 3);
            assert!(recovery.segments.contains_key(&(1, 1)));
            assert!(recovery.segments.contains_key(&(1, 2)));
            assert!(recovery.segments.contains_key(&(2, 1)));

            let group1 = manifest.read_group_segments(1);
            assert_eq!(group1.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_append_and_recover_engine_blob() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        {
            let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            manifest.append_engine_blob(1, 100, b"hello").await.unwrap();
            manifest.append_engine_blob(1, 200, b"world").await.unwrap();
            manifest.sync().await.unwrap();
        }

        {
            let (_, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            assert_eq!(recovery.engine_blobs.len(), 2);
            assert_eq!(recovery.engine_blobs[0], (1, 100, b"hello".to_vec()));
            assert_eq!(recovery.engine_blobs[1], (1, 200, b"world".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_rotate_and_recover() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        {
            let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            manifest
                .append_segment_meta(make_segment_meta(1, 1))
                .await
                .unwrap();
            manifest
                .append_engine_blob(1, 50, b"before_snap")
                .await
                .unwrap();

            // Rotate with an engine snapshot.
            manifest.rotate(b"engine_state_v1").await.unwrap();

            // Write more after rotation.
            manifest
                .append_segment_meta(make_segment_meta(1, 2))
                .await
                .unwrap();
            manifest
                .append_engine_blob(1, 100, b"after_snap")
                .await
                .unwrap();
            manifest.sync().await.unwrap();
        }

        // Recover — should see segments from snapshot + WAL, and engine blobs.
        {
            let (_, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            assert_eq!(recovery.segments.len(), 2);
            assert!(recovery.segments.contains_key(&(1, 1))); // from snapshot
            assert!(recovery.segments.contains_key(&(1, 2))); // from WAL

            // Engine blobs: snapshot blob + post-rotation WAL blob.
            assert_eq!(recovery.engine_blobs.len(), 2);
            assert_eq!(recovery.engine_blobs[0].2, b"engine_state_v1");
            assert_eq!(recovery.engine_blobs[1], (1, 100, b"after_snap".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_segment_meta_merge_during_recovery() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        {
            let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();

            let mut meta1 = make_segment_meta(1, 1);
            meta1.valid_bytes = 1000;
            meta1.sealed = false;
            manifest.append_segment_meta(meta1).await.unwrap();

            let mut meta2 = make_segment_meta(1, 1);
            meta2.valid_bytes = 2000;
            meta2.sealed = true;
            manifest.append_segment_meta(meta2).await.unwrap();

            manifest.sync().await.unwrap();
        }

        {
            let (_, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            let meta = recovery.segments.get(&(1, 1)).unwrap();
            assert_eq!(meta.valid_bytes, 2000); // max
            assert_eq!(meta.sealed, true); // OR
        }
    }

    #[tokio::test]
    async fn test_wal_size_threshold() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        // Very small threshold.
        let (mut manifest, _) = ManifestLog::open(&dir, 200).await.unwrap();
        let exceeded = manifest
            .append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();

        // 16 (header) + 4 + 89 + 8 = 117 bytes after first entry.
        // Second entry should push past 200.
        if !exceeded {
            let exceeded = manifest
                .append_segment_meta(make_segment_meta(1, 2))
                .await
                .unwrap();
            assert!(exceeded);
        }
    }

    #[tokio::test]
    async fn test_truncated_wal_entry_recovery() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        // Write a valid entry.
        {
            let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            manifest
                .append_segment_meta(make_segment_meta(1, 1))
                .await
                .unwrap();
            manifest.sync().await.unwrap();
        }

        // Corrupt the WAL by appending garbage (simulating a crash mid-write).
        {
            let wal_files = find_wal_files(&dir).await.unwrap();
            assert!(!wal_files.is_empty());
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&wal_files[0].1)
                .await
                .unwrap();
            file.write_all(&[0xFF; 20]).await.unwrap();
            file.sync_data().await.unwrap();
        }

        // Recovery should still find the valid entry.
        {
            let (_, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            assert_eq!(recovery.segments.len(), 1);
            assert!(recovery.segments.contains_key(&(1, 1)));
        }
    }

    #[tokio::test]
    async fn test_multiple_rotations() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();

        for i in 0..5 {
            manifest
                .append_segment_meta(make_segment_meta(1, i))
                .await
                .unwrap();
            manifest
                .rotate(format!("snap_{i}").as_bytes())
                .await
                .unwrap();
        }

        manifest
            .append_segment_meta(make_segment_meta(1, 100))
            .await
            .unwrap();
        manifest.sync().await.unwrap();

        drop(manifest);

        let (_, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
        // All 5 segments from rotations + 1 from final WAL.
        assert_eq!(recovery.segments.len(), 6);

        // Only the latest snapshot's engine data + no WAL engine blobs.
        assert_eq!(recovery.engine_blobs.len(), 1);
        assert_eq!(recovery.engine_blobs[0].2, b"snap_4");
    }

    #[tokio::test]
    async fn test_read_group_segments() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
        manifest
            .append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();
        manifest
            .append_segment_meta(make_segment_meta(1, 2))
            .await
            .unwrap();
        manifest
            .append_segment_meta(make_segment_meta(2, 1))
            .await
            .unwrap();

        let g1 = manifest.read_group_segments(1);
        assert_eq!(g1.len(), 2);
        assert!(g1.contains_key(&1));
        assert!(g1.contains_key(&2));

        let g2 = manifest.read_group_segments(2);
        assert_eq!(g2.len(), 1);

        let g3 = manifest.read_group_segments(3);
        assert_eq!(g3.len(), 0);
    }

    #[tokio::test]
    async fn test_remove_group_segments() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
        manifest
            .append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();
        manifest
            .append_segment_meta(make_segment_meta(2, 1))
            .await
            .unwrap();

        manifest.remove_group_segments(1);
        assert_eq!(manifest.read_group_segments(1).len(), 0);
        assert_eq!(manifest.read_group_segments(2).len(), 1);
    }

    #[tokio::test]
    async fn test_snapshot_with_no_segments() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        {
            let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            manifest.rotate(b"empty_state").await.unwrap();
        }

        {
            let (_, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            assert!(recovery.segments.is_empty());
            assert_eq!(recovery.engine_blobs.len(), 1);
            assert_eq!(recovery.engine_blobs[0].2, b"empty_state");
        }
    }

    #[tokio::test]
    async fn test_snapshot_with_empty_engine_data() {
        let tmp = TestTempDir::new();
        let dir = tmp.path().join("manifest");

        {
            let (mut manifest, _) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            manifest
                .append_segment_meta(make_segment_meta(1, 1))
                .await
                .unwrap();
            manifest.rotate(b"").await.unwrap();
        }

        {
            let (_, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
            assert_eq!(recovery.segments.len(), 1);
            assert!(recovery.engine_blobs.is_empty()); // empty engine data → no blob
        }
    }
}
