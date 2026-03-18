//! Tiered storage: segment archival to remote object stores (S3, R2, MinIO, etc.)
//!
//! This module provides:
//! - `SegmentArchive` trait: async interface for uploading/downloading segments
//! - `S3ArchiveConfig`: configuration for S3-compatible storage
//! - `ArchiveManager`: coordinates upload/download/eviction with optional zstd compression
//! - `InMemoryArchive`: in-memory implementation for testing

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for S3-compatible remote segment storage.
#[derive(Debug, Clone)]
pub struct S3ArchiveConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix for segment objects (e.g. "raft/segments/").
    pub key_prefix: String,
    /// Optional S3-compatible endpoint URL (for MinIO, R2, etc.).
    pub endpoint: Option<String>,
    /// AWS region.
    pub region: String,
    /// Minimum seconds a sealed segment must remain on local disk before deletion.
    /// Prevents thrashing when segments are archived then immediately needed.
    pub min_local_retention_secs: u64,
    /// When true, compress segments with zstd before uploading to remote storage.
    /// Archived objects use `.log.zst` extension; downloaded segments are decompressed
    /// back to the original `.log` format transparently.
    pub compress_archived: bool,
    /// Zstd compression level (1-22). Default: 3. Higher = better ratio but slower.
    pub compression_level: i32,
}

impl Default for S3ArchiveConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            key_prefix: "raft/segments/".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            min_local_retention_secs: 3600,
            compress_archived: false,
            compression_level: 3,
        }
    }
}

impl S3ArchiveConfig {
    /// Build the S3 object key for a given group/segment.
    /// Uses `.log.zst` extension when compression is enabled, `.log` otherwise.
    pub fn object_key(&self, group_id: u64, segment_id: u64) -> String {
        let ext = if self.compress_archived {
            "log.zst"
        } else {
            "log"
        };
        format!(
            "{}group_{}/seg_{:06}.{}",
            self.key_prefix, group_id, segment_id, ext
        )
    }
}

// ---------------------------------------------------------------------------
// SegmentArchive trait
// ---------------------------------------------------------------------------

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Async trait for uploading and downloading segment files to/from remote storage.
///
/// Uses boxed futures for dyn-compatibility. Implementations must be Send + Sync.
pub trait SegmentArchive: Send + Sync + 'static {
    /// Upload a local segment file to remote storage.
    fn upload(&self, local_path: &Path, key: &str) -> BoxFuture<'_, io::Result<()>>;

    /// Download a remote segment to a local path.
    fn download(&self, key: &str, local_path: &Path) -> BoxFuture<'_, io::Result<()>>;

    /// Check if a remote segment exists.
    fn exists(&self, key: &str) -> BoxFuture<'_, io::Result<bool>>;

    /// Delete a remote segment.
    fn delete(&self, key: &str) -> BoxFuture<'_, io::Result<()>>;
}

// ---------------------------------------------------------------------------
// InMemoryArchive — for testing
// ---------------------------------------------------------------------------

/// In-memory segment archive for unit and integration tests.
/// Stores segment data in a HashMap keyed by object key.
pub struct InMemoryArchive {
    store: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemoryArchive {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    /// Return the number of objects currently stored.
    pub fn object_count(&self) -> usize {
        self.store.lock().len()
    }

    /// Check if a key exists in the store.
    pub fn contains_key(&self, key: &str) -> bool {
        self.store.lock().contains_key(key)
    }

    /// Return all object keys currently stored.
    pub fn keys(&self) -> Vec<String> {
        self.store.lock().keys().cloned().collect()
    }

    /// Get a copy of the stored data for a given key.
    pub fn get_data(&self, key: &str) -> Option<Vec<u8>> {
        self.store.lock().get(key).cloned()
    }
}

impl Default for InMemoryArchive {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentArchive for InMemoryArchive {
    fn upload(&self, local_path: &Path, key: &str) -> BoxFuture<'_, io::Result<()>> {
        let local_path = local_path.to_path_buf();
        let key = key.to_string();
        Box::pin(async move {
            let data = tokio::fs::read(&local_path).await?;
            self.store.lock().insert(key, data);
            Ok(())
        })
    }

    fn download(&self, key: &str, local_path: &Path) -> BoxFuture<'_, io::Result<()>> {
        let key = key.to_string();
        let local_path = local_path.to_path_buf();
        Box::pin(async move {
            let data = self.store.lock().get(&key).cloned().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("key not found: {key}"))
            })?;
            if let Some(parent) = local_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&local_path, &data).await?;
            Ok(())
        })
    }

    fn exists(&self, key: &str) -> BoxFuture<'_, io::Result<bool>> {
        let result = self.store.lock().contains_key(key);
        Box::pin(async move { Ok(result) })
    }

    fn delete(&self, key: &str) -> BoxFuture<'_, io::Result<()>> {
        self.store.lock().remove(key);
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// Zstd compression helpers
// ---------------------------------------------------------------------------

/// Compress `src` to `dst` using zstd at the given level. Runs synchronously.
fn compress_file(src: &Path, dst: &Path, level: i32) -> io::Result<()> {
    let input = std::fs::read(src)?;
    let mut encoder = zstd::Encoder::new(std::fs::File::create(dst)?, level)?;
    encoder.write_all(&input)?;
    encoder.finish()?;
    Ok(())
}

/// Decompress a zstd-compressed `src` to `dst`. Runs synchronously.
fn decompress_file(src: &Path, dst: &Path) -> io::Result<()> {
    let mut decoder = zstd::Decoder::new(std::fs::File::open(src)?)?;
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    std::fs::write(dst, &output)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ArchiveManager — coordinates upload/download/eviction
// ---------------------------------------------------------------------------

/// Manages the lifecycle of segment archival:
/// - Uploads sealed segments to remote storage
/// - Downloads segments on cache miss (when local file was evicted)
/// - Tracks what has been archived for eviction decisions
pub struct ArchiveManager {
    archive: Arc<dyn SegmentArchive>,
    config: S3ArchiveConfig,
}

impl ArchiveManager {
    pub fn new(archive: Arc<dyn SegmentArchive>, config: S3ArchiveConfig) -> Self {
        Self { archive, config }
    }

    /// Upload a sealed segment to remote storage.
    /// When `compress_archived` is enabled, the segment is zstd-compressed into a
    /// temporary file before upload, then the temp file is removed.
    pub async fn upload_segment(
        &self,
        group_id: u64,
        segment_id: u64,
        local_path: &Path,
    ) -> io::Result<()> {
        let key = self.config.object_key(group_id, segment_id);
        if self.config.compress_archived {
            let level = self.config.compression_level;
            let src = local_path.to_path_buf();
            let tmp_path = local_path.with_extension("log.zst.tmp");
            let tmp_clone = tmp_path.clone();
            // Compress in a blocking task — segment files can be ~1MB
            tokio::task::spawn_blocking(move || compress_file(&src, &tmp_clone, level))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
            let result = self.archive.upload(&tmp_path, &key).await;
            let _ = tokio::fs::remove_file(&tmp_path).await;
            result
        } else {
            self.archive.upload(local_path, &key).await
        }
    }

    /// Download a segment from remote storage to a local path.
    /// When `compress_archived` is enabled, the downloaded data is zstd-decompressed
    /// transparently so the local file is always an uncompressed segment.
    pub async fn download_segment(
        &self,
        group_id: u64,
        segment_id: u64,
        local_path: &Path,
    ) -> io::Result<()> {
        let key = self.config.object_key(group_id, segment_id);
        if self.config.compress_archived {
            let tmp_path = local_path.with_extension("log.zst.tmp");
            self.archive.download(&key, &tmp_path).await?;
            let dst = local_path.to_path_buf();
            let tmp_clone = tmp_path.clone();
            let result = tokio::task::spawn_blocking(move || decompress_file(&tmp_clone, &dst))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let _ = tokio::fs::remove_file(&tmp_path).await;
            result
        } else {
            self.archive.download(&key, local_path).await
        }
    }

    /// Check if a segment exists in remote storage.
    pub async fn segment_exists(&self, group_id: u64, segment_id: u64) -> io::Result<bool> {
        let key = self.config.object_key(group_id, segment_id);
        self.archive.exists(&key).await
    }

    /// Delete a segment from remote storage.
    pub async fn delete_segment(&self, group_id: u64, segment_id: u64) -> io::Result<()> {
        let key = self.config.object_key(group_id, segment_id);
        self.archive.delete(&key).await
    }

    /// Get the underlying archive config.
    pub fn config(&self) -> &S3ArchiveConfig {
        &self.config
    }

    /// Evict local segment files to bring total local disk usage under `max_local_bytes`.
    ///
    /// Only segments with `SegmentLocation::Both` (archived remotely) are eligible.
    /// Segments are evicted oldest (lowest segment_id) first.
    /// Returns the number of local files deleted and the bytes freed.
    ///
    /// `segment_files` should be a list of `(segment_id, file_path, file_size, location)`
    /// sorted by segment_id ascending (oldest first).
    pub async fn evict_local_segments(
        &self,
        max_local_bytes: u64,
        mut segment_files: Vec<(u64, PathBuf, u64, bool)>, // (seg_id, path, size, is_archived)
    ) -> (usize, u64) {
        if max_local_bytes == 0 {
            return (0, 0); // unlimited
        }

        let total_bytes: u64 = segment_files.iter().map(|(_, _, sz, _)| *sz).sum();
        if total_bytes <= max_local_bytes {
            return (0, 0);
        }

        let mut bytes_to_free = total_bytes - max_local_bytes;
        let mut deleted = 0usize;
        let mut freed = 0u64;

        // Sort by segment_id (oldest first) to evict oldest segments
        segment_files.sort_by_key(|(seg_id, _, _, _)| *seg_id);

        let retention_cutoff = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.config.min_local_retention_secs);

        for (seg_id, path, size, is_archived) in &segment_files {
            if bytes_to_free == 0 {
                break;
            }

            // Only evict segments that have been archived to remote
            if !is_archived {
                continue;
            }

            // Check min_local_retention_secs — don't evict recently sealed segments
            if let Ok(metadata) = std::fs::metadata(&path) {
                if let Ok(modified) = metadata.modified() {
                    let mod_secs = modified
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if mod_secs > retention_cutoff {
                        continue; // Too recent to evict
                    }
                }
            }

            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    tracing::debug!(
                        segment_id = seg_id,
                        bytes = size,
                        "evicted local segment file"
                    );
                    deleted += 1;
                    freed += size;
                    bytes_to_free = bytes_to_free.saturating_sub(*size);
                }
                Err(e) => {
                    tracing::warn!(
                        segment_id = seg_id,
                        error = %e,
                        "failed to evict local segment file"
                    );
                }
            }
        }

        (deleted, freed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestTempDir;

    #[tokio::test]
    async fn test_in_memory_archive_upload_download() {
        let archive = InMemoryArchive::new();
        let tmp = TestTempDir::new();

        // Create a fake segment file
        let seg_path = tmp.path().join("seg_000001.log");
        tokio::fs::write(&seg_path, b"segment data here")
            .await
            .unwrap();

        // Upload
        archive
            .upload(&seg_path, "group_0/seg_000001.log")
            .await
            .unwrap();
        assert_eq!(archive.object_count(), 1);
        assert!(archive.contains_key("group_0/seg_000001.log"));

        // Download to new location
        let dl_path = tmp.path().join("downloaded.log");
        archive
            .download("group_0/seg_000001.log", &dl_path)
            .await
            .unwrap();
        let data = tokio::fs::read(&dl_path).await.unwrap();
        assert_eq!(data, b"segment data here");
    }

    #[tokio::test]
    async fn test_in_memory_archive_exists_delete() {
        let archive = InMemoryArchive::new();
        let tmp = TestTempDir::new();

        let seg_path = tmp.path().join("seg.log");
        tokio::fs::write(&seg_path, b"data").await.unwrap();

        assert!(!archive.exists("key").await.unwrap());
        archive.upload(&seg_path, "key").await.unwrap();
        assert!(archive.exists("key").await.unwrap());
        archive.delete("key").await.unwrap();
        assert!(!archive.exists("key").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_archive_download_not_found() {
        let archive = InMemoryArchive::new();
        let tmp = TestTempDir::new();
        let dl_path = tmp.path().join("missing.log");
        let result = archive.download("nonexistent", &dl_path).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn test_archive_manager_roundtrip() {
        let archive = Arc::new(InMemoryArchive::new());
        let config = S3ArchiveConfig {
            bucket: "test-bucket".to_string(),
            key_prefix: "raft/".to_string(),
            ..Default::default()
        };
        let mgr = ArchiveManager::new(archive.clone(), config);
        let tmp = TestTempDir::new();

        // Create and upload a segment
        let seg_path = tmp.path().join("seg_000042.log");
        tokio::fs::write(&seg_path, b"raft log segment bytes")
            .await
            .unwrap();
        mgr.upload_segment(0, 42, &seg_path).await.unwrap();

        // Verify it exists
        assert!(mgr.segment_exists(0, 42).await.unwrap());
        assert!(!mgr.segment_exists(0, 99).await.unwrap());

        // Download to a different path
        let dl_path = tmp.path().join("restored.log");
        mgr.download_segment(0, 42, &dl_path).await.unwrap();
        let data = tokio::fs::read(&dl_path).await.unwrap();
        assert_eq!(data, b"raft log segment bytes");

        // Delete
        mgr.delete_segment(0, 42).await.unwrap();
        assert!(!mgr.segment_exists(0, 42).await.unwrap());
    }

    #[test]
    fn test_s3_archive_config_object_key() {
        let config = S3ArchiveConfig {
            bucket: "my-bucket".to_string(),
            key_prefix: "data/raft/".to_string(),
            ..Default::default()
        };
        assert_eq!(config.object_key(5, 42), "data/raft/group_5/seg_000042.log");
    }

    #[test]
    fn test_s3_archive_config_defaults() {
        let config = S3ArchiveConfig::default();
        assert_eq!(config.min_local_retention_secs, 3600);
        assert_eq!(config.key_prefix, "raft/segments/");
        assert_eq!(config.region, "us-east-1");
        assert!(!config.compress_archived);
        assert_eq!(config.compression_level, 3);
    }

    #[test]
    fn test_s3_archive_config_object_key_compressed() {
        let config = S3ArchiveConfig {
            bucket: "my-bucket".to_string(),
            key_prefix: "data/raft/".to_string(),
            compress_archived: true,
            ..Default::default()
        };
        assert_eq!(
            config.object_key(5, 42),
            "data/raft/group_5/seg_000042.log.zst"
        );
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        let tmp = TestTempDir::new();
        let original = tmp.path().join("original.log");
        let compressed = tmp.path().join("compressed.log.zst");
        let restored = tmp.path().join("restored.log");

        let data = b"hello world, this is raft segment data that should compress well \
                      hello world, this is raft segment data that should compress well";
        std::fs::write(&original, data).unwrap();

        compress_file(&original, &compressed, 3).unwrap();
        // Compressed file should exist and be smaller (or at least different)
        let compressed_size = std::fs::metadata(&compressed).unwrap().len();
        assert!(compressed_size > 0);
        assert!(compressed_size < data.len() as u64);

        decompress_file(&compressed, &restored).unwrap();
        let restored_data = std::fs::read(&restored).unwrap();
        assert_eq!(restored_data, data);
    }

    #[tokio::test]
    async fn test_archive_manager_compressed_roundtrip() {
        let archive = Arc::new(InMemoryArchive::new());
        let config = S3ArchiveConfig {
            bucket: "test-bucket".to_string(),
            key_prefix: "raft/".to_string(),
            compress_archived: true,
            ..Default::default()
        };
        let mgr = ArchiveManager::new(archive.clone(), config);
        let tmp = TestTempDir::new();

        // Create a segment file with repetitive data (compresses well)
        let seg_path = tmp.path().join("seg_000010.log");
        let original_data = "raft log entry\n".repeat(1000);
        tokio::fs::write(&seg_path, original_data.as_bytes())
            .await
            .unwrap();

        // Upload with compression
        mgr.upload_segment(0, 10, &seg_path).await.unwrap();

        // Verify the archive stores compressed data (smaller than original)
        assert!(mgr.segment_exists(0, 10).await.unwrap());
        let stored_data = archive
            .get_data("raft/group_0/seg_000010.log.zst")
            .expect("compressed key should exist");
        assert!(stored_data.len() < original_data.len());

        // Download and verify decompression produces identical data
        let dl_path = tmp.path().join("restored.log");
        mgr.download_segment(0, 10, &dl_path).await.unwrap();
        let restored = tokio::fs::read(&dl_path).await.unwrap();
        assert_eq!(restored, original_data.as_bytes());

        // Temp files should be cleaned up
        assert!(!seg_path.with_extension("log.zst.tmp").exists());
        assert!(!dl_path.with_extension("log.zst.tmp").exists());
    }
}
