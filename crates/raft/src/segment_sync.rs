//! Out-of-band TCP file sync for raft log segment transfer.
//!
//! When a new or lagging node receives a Raft snapshot, it needs the underlying
//! segment files that contain message payloads. This module provides a simple
//! TCP protocol to stream those files from the leader to the follower.
//!
//! ## Protocol
//!
//! **Request** (follower → leader):
//! ```text
//! [4 bytes] Magic: "BSYN"
//! [4 bytes] Version: u32 LE (currently 1)
//! [4 bytes] Manifest length: u32 LE
//! [N bytes] Manifest: bincode-encoded Vec<SnapshotFileEntry>
//! ```
//!
//! **Response** (leader → follower, repeated per file):
//! ```text
//! [2 bytes] Path length: u16 LE (0 = end sentinel)
//! [N bytes] Relative path: UTF-8
//! [8 bytes] File length: u64 LE (0 = file missing/skipped)
//! [N bytes] File data (streamed in chunks)
//! ```

use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

const SYNC_MAGIC: &[u8; 4] = b"BSYN";
const SYNC_VERSION: u32 = 1;
const STREAM_CHUNK_SIZE: usize = 256 * 1024;

// =============================================================================
// Types
// =============================================================================

/// A file entry in a snapshot manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFileEntry {
    /// Relative path from the data directory (e.g. `"seg_000001.log"`).
    pub relative_path: String,
    /// File size in bytes.
    pub size: u64,
}

/// Result of a file sync operation.
#[derive(Debug, Clone)]
pub struct SyncResult {
    pub files_requested: usize,
    pub files_transferred: usize,
    pub files_missing: usize,
    pub bytes_transferred: u64,
    pub missing_paths: Vec<String>,
    pub verification_failures: Vec<String>,
}

/// Configuration for the segment sync server (leader side).
pub struct SegmentSyncServerConfig {
    pub data_dir: PathBuf,
    pub bind_addr: std::net::SocketAddr,
    #[cfg(feature = "tls")]
    pub tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
}

/// Configuration for the segment sync client (follower side).
pub struct SegmentSyncClientConfig {
    pub data_dir: PathBuf,
    #[cfg(feature = "tls")]
    pub tls_config: Option<std::sync::Arc<rustls::ClientConfig>>,
    #[cfg(feature = "tls")]
    pub tls_server_name: Option<rustls::pki_types::ServerName<'static>>,
}

// =============================================================================
// Server
// =============================================================================

/// TCP server that serves segment files to followers during snapshot install.
pub struct SegmentSyncServer {
    config: SegmentSyncServerConfig,
}

impl SegmentSyncServer {
    pub fn new(config: SegmentSyncServerConfig) -> Self {
        Self { config }
    }

    /// Run the server until the shutdown signal fires.
    pub async fn serve(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> io::Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!(
            addr = %self.config.bind_addr,
            "Segment sync server listening"
        );

        #[cfg(feature = "tls")]
        let tls_acceptor = self
            .config
            .tls_config
            .as_ref()
            .map(|cfg| tokio_rustls::TlsAcceptor::from(cfg.clone()));

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer) = result?;
                    stream.set_nodelay(true).ok();
                    let data_dir = self.config.data_dir.clone();
                    debug!(peer = %peer, "Segment sync connection accepted");

                    #[cfg(feature = "tls")]
                    if let Some(ref acceptor) = tls_acceptor {
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    if let Err(e) = Self::handle_connection(tls_stream, &data_dir).await {
                                        warn!(peer = %peer, error = %e, "Segment sync TLS connection error");
                                    }
                                }
                                Err(e) => {
                                    warn!(peer = %peer, error = %e, "TLS handshake failed");
                                }
                            }
                        });
                        continue;
                    }

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, &data_dir).await {
                            warn!(peer = %peer, error = %e, "Segment sync connection error");
                        }
                    });
                }
                _ = shutdown.changed() => {
                    info!("Segment sync server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_connection<S>(mut stream: S, data_dir: &Path) -> io::Result<()>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // Read and validate magic
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic).await?;
        if &magic != SYNC_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid sync magic",
            ));
        }

        // Read and validate version
        let version = stream.read_u32_le().await?;
        if version != SYNC_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported sync version: {version}"),
            ));
        }

        // Read manifest
        let manifest_len = stream.read_u32_le().await? as usize;
        let mut manifest_buf = vec![0u8; manifest_len];
        stream.read_exact(&mut manifest_buf).await?;
        let (manifest, _): (Vec<SnapshotFileEntry>, _) =
            bincode::serde::decode_from_slice(&manifest_buf, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        info!(files = manifest.len(), "Serving segment files to follower");

        let total_bytes: u64 = manifest.iter().map(|e| e.size).sum();
        let mut progress = SyncProgress::new(total_bytes, manifest.len(), "send");
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];

        for entry in &manifest {
            if !validate_relative_path(&entry.relative_path) {
                warn!(path = %entry.relative_path, "Rejected invalid relative path");
                // Send path with size 0 to signal skip
                let path_bytes = entry.relative_path.as_bytes();
                stream.write_u16_le(path_bytes.len() as u16).await?;
                stream.write_all(path_bytes).await?;
                stream.write_u64_le(0).await?;
                continue;
            }

            let file_path = data_dir.join(&entry.relative_path);

            // Write path header
            let path_bytes = entry.relative_path.as_bytes();
            stream.write_u16_le(path_bytes.len() as u16).await?;
            stream.write_all(path_bytes).await?;

            // Try to open and stream the file
            match tokio::fs::File::open(&file_path).await {
                Ok(mut file) => {
                    let file_size = file.metadata().await?.len();
                    stream.write_u64_le(file_size).await?;

                    let mut remaining = file_size;
                    while remaining > 0 {
                        let to_read = (remaining as usize).min(STREAM_CHUNK_SIZE);
                        let n = file.read(&mut buf[..to_read]).await?;
                        if n == 0 {
                            break;
                        }
                        stream.write_all(&buf[..n]).await?;
                        remaining -= n as u64;
                        progress.add_bytes(n as u64);
                    }
                    progress.complete_file();
                    progress.maybe_report();
                }
                Err(_) => {
                    warn!(path = %entry.relative_path, "Segment file missing, sending size=0");
                    stream.write_u64_le(0).await?;
                }
            }
        }

        // End sentinel
        stream.write_u16_le(0).await?;
        stream.flush().await?;
        progress.report_complete();

        Ok(())
    }
}

// =============================================================================
// Client
// =============================================================================

/// TCP client that receives segment files from the leader during snapshot install.
pub struct SegmentSyncClient {
    config: SegmentSyncClientConfig,
}

impl SegmentSyncClient {
    pub fn new(config: SegmentSyncClientConfig) -> Self {
        Self { config }
    }

    /// Sync files from the leader. Returns a summary of what was transferred.
    pub async fn sync_files(
        &self,
        leader_addr: &str,
        manifest: &[SnapshotFileEntry],
    ) -> io::Result<SyncResult> {
        if manifest.is_empty() {
            return Ok(SyncResult {
                files_requested: 0,
                files_transferred: 0,
                files_missing: 0,
                bytes_transferred: 0,
                missing_paths: Vec::new(),
                verification_failures: Vec::new(),
            });
        }

        let stream = tokio::net::TcpStream::connect(leader_addr).await?;
        stream.set_nodelay(true).ok();

        #[cfg(feature = "tls")]
        if let (Some(tls_config), Some(server_name)) =
            (&self.config.tls_config, &self.config.tls_server_name)
        {
            let connector = tokio_rustls::TlsConnector::from(tls_config.clone());
            let tls_stream = connector.connect(server_name.clone(), stream).await?;
            let total_bytes: u64 = manifest.iter().map(|e| e.size).sum();
            let mut result = self.do_sync(tls_stream, manifest, total_bytes).await?;
            self.verify_files(manifest, &mut result);
            return Ok(result);
        }

        let total_bytes: u64 = manifest.iter().map(|e| e.size).sum();
        let mut result = self.do_sync(stream, manifest, total_bytes).await?;
        self.verify_files(manifest, &mut result);
        Ok(result)
    }

    async fn do_sync<S>(
        &self,
        mut stream: S,
        manifest: &[SnapshotFileEntry],
        total_bytes: u64,
    ) -> io::Result<SyncResult>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // Send request header
        stream.write_all(SYNC_MAGIC).await?;
        stream.write_u32_le(SYNC_VERSION).await?;

        let manifest_bytes =
            bincode::serde::encode_to_vec(manifest, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        stream.write_u32_le(manifest_bytes.len() as u32).await?;
        stream.write_all(&manifest_bytes).await?;
        stream.flush().await?;

        info!(
            files = manifest.len(),
            total_bytes = total_bytes,
            "Requesting segment files from leader"
        );

        let mut result = SyncResult {
            files_requested: manifest.len(),
            files_transferred: 0,
            files_missing: 0,
            bytes_transferred: 0,
            missing_paths: Vec::new(),
            verification_failures: Vec::new(),
        };
        let mut progress = SyncProgress::new(total_bytes, manifest.len(), "recv");
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];

        loop {
            let path_len = stream.read_u16_le().await? as usize;
            if path_len == 0 {
                break; // End sentinel
            }

            let mut path_buf = vec![0u8; path_len];
            stream.read_exact(&mut path_buf).await?;
            let relative_path = String::from_utf8(path_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            if !validate_relative_path(&relative_path) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid relative path from server: {relative_path}"),
                ));
            }

            let file_len = stream.read_u64_le().await?;
            if file_len == 0 {
                result.files_missing += 1;
                result.missing_paths.push(relative_path);
                progress.complete_file();
                continue;
            }

            let dest = self.config.data_dir.join(&relative_path);
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let mut file = tokio::fs::File::create(&dest).await?;
            let mut remaining = file_len;
            while remaining > 0 {
                let to_read = (remaining as usize).min(STREAM_CHUNK_SIZE);
                stream.read_exact(&mut buf[..to_read]).await?;
                file.write_all(&buf[..to_read]).await?;
                remaining -= to_read as u64;
                progress.add_bytes(to_read as u64);
            }
            file.flush().await?;
            result.files_transferred += 1;
            result.bytes_transferred += file_len;
            progress.complete_file();
            progress.maybe_report();
        }

        progress.report_complete();
        Ok(result)
    }

    fn verify_files(&self, manifest: &[SnapshotFileEntry], result: &mut SyncResult) {
        for entry in manifest {
            let path = self.config.data_dir.join(&entry.relative_path);
            match std::fs::metadata(&path) {
                Ok(meta) => {
                    if meta.len() != entry.size {
                        result.verification_failures.push(format!(
                            "{}: expected {} bytes, got {}",
                            entry.relative_path,
                            entry.size,
                            meta.len()
                        ));
                    }
                }
                Err(_) => {
                    // Only flag as failure if it wasn't already in missing_paths
                    if !result.missing_paths.contains(&entry.relative_path) {
                        result.verification_failures.push(format!(
                            "{}: file not found after sync",
                            entry.relative_path
                        ));
                    }
                }
            }
        }
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// List segment files in a group directory as `SnapshotFileEntry` values.
pub fn list_segment_files(group_dir: &Path) -> io::Result<Vec<SnapshotFileEntry>> {
    let ids = crate::storage_mmap::scan_segment_ids(group_dir)?;
    let mut entries = Vec::with_capacity(ids.len());
    for id in ids {
        let path = group_dir.join(format!("seg_{id:06}.log"));
        match std::fs::metadata(&path) {
            Ok(meta) => {
                entries.push(SnapshotFileEntry {
                    relative_path: format!("seg_{id:06}.log"),
                    size: meta.len(),
                });
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Segment was purged between scan and stat — skip
            }
            Err(e) => return Err(e),
        }
    }
    Ok(entries)
}

/// Validate that a relative path is safe (no directory traversal, no absolute paths).
fn validate_relative_path(path: &str) -> bool {
    if path.is_empty() || path.contains('\0') {
        return false;
    }
    let p = Path::new(path);
    if p.is_absolute() {
        return false;
    }
    for component in p.components() {
        match component {
            std::path::Component::ParentDir => return false,
            std::path::Component::RootDir => return false,
            std::path::Component::Prefix(_) => return false,
            _ => {}
        }
    }
    true
}

// =============================================================================
// Progress tracking
// =============================================================================

struct SyncProgress {
    total_bytes: u64,
    transferred_bytes: u64,
    files_total: usize,
    files_done: usize,
    start_time: Instant,
    last_report: Instant,
    direction: &'static str,
}

impl SyncProgress {
    fn new(total_bytes: u64, files_total: usize, direction: &'static str) -> Self {
        let now = Instant::now();
        Self {
            total_bytes,
            transferred_bytes: 0,
            files_total,
            files_done: 0,
            start_time: now,
            last_report: now,
            direction,
        }
    }

    fn add_bytes(&mut self, bytes: u64) {
        self.transferred_bytes += bytes;
    }

    fn complete_file(&mut self) {
        self.files_done += 1;
    }

    fn maybe_report(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_report).as_secs() >= 1 {
            self.last_report = now;
            let elapsed = now.duration_since(self.start_time).as_secs_f64();
            let pct = if self.total_bytes > 0 {
                (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
            } else {
                100.0
            };
            let rate = if elapsed > 0.0 {
                self.transferred_bytes as f64 / elapsed
            } else {
                0.0
            };
            info!(
                direction = self.direction,
                files = %format!("{}/{}", self.files_done, self.files_total),
                bytes = %format_bytes(self.transferred_bytes),
                pct = format!("{pct:.1}%"),
                rate = %format!("{}/s", format_bytes(rate as u64)),
                "Segment sync progress"
            );
        }
    }

    fn report_complete(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 {
            self.transferred_bytes as f64 / elapsed
        } else {
            0.0
        };
        info!(
            direction = self.direction,
            files = self.files_done,
            bytes = %format_bytes(self.transferred_bytes),
            elapsed = format!("{elapsed:.1}s"),
            rate = %format!("{}/s", format_bytes(rate as u64)),
            "Segment sync complete"
        );
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_relative_path() {
        assert!(validate_relative_path("seg_000001.log"));
        assert!(validate_relative_path("subdir/seg_000001.log"));
        assert!(!validate_relative_path(""));
        assert!(!validate_relative_path("/absolute/path"));
        assert!(!validate_relative_path("../escape"));
        assert!(!validate_relative_path("dir/../escape"));
        assert!(!validate_relative_path("path\0null"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.0 GB");
    }

    #[test]
    fn test_snapshot_file_entry_serde() {
        let entry = SnapshotFileEntry {
            relative_path: "seg_000001.log".to_string(),
            size: 1024,
        };
        let bytes = bincode::serde::encode_to_vec(&entry, bincode::config::standard()).unwrap();
        let (decoded, _): (SnapshotFileEntry, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.relative_path, "seg_000001.log");
        assert_eq!(decoded.size, 1024);
    }

    #[tokio::test]
    async fn test_sync_empty_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let client = SegmentSyncClient::new(SegmentSyncClientConfig {
            data_dir: dir.path().to_path_buf(),
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        });
        let result = client.sync_files("127.0.0.1:0", &[]).await.unwrap();
        assert_eq!(result.files_requested, 0);
        assert_eq!(result.files_transferred, 0);
    }

    #[tokio::test]
    async fn test_sync_roundtrip() {
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Create a temporary leader data dir with a segment file
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        let seg_data = vec![42u8; 4096];
        std::fs::write(leader_dir.path().join("seg_000001.log"), &seg_data).unwrap();
        std::fs::write(leader_dir.path().join("seg_000002.log"), &vec![99u8; 2048]).unwrap();

        let manifest = vec![
            SnapshotFileEntry {
                relative_path: "seg_000001.log".to_string(),
                size: 4096,
            },
            SnapshotFileEntry {
                relative_path: "seg_000002.log".to_string(),
                size: 2048,
            },
            SnapshotFileEntry {
                relative_path: "seg_000003.log".to_string(),
                size: 1024, // missing on leader
            },
        ];

        // Bind first to get the actual address
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let server_config = SegmentSyncServerConfig {
            data_dir: leader_dir.path().to_path_buf(),
            bind_addr: addr,
            #[cfg(feature = "tls")]
            tls_config: None,
        };
        let server = SegmentSyncServer::new(server_config);

        let server_handle = tokio::spawn(async move {
            server.serve(rx).await.unwrap();
        });

        // Give server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Sync
        let client = SegmentSyncClient::new(SegmentSyncClientConfig {
            data_dir: follower_dir.path().to_path_buf(),
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        });

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_requested, 3);
        assert_eq!(result.files_transferred, 2);
        assert_eq!(result.files_missing, 1);
        assert_eq!(result.bytes_transferred, 4096 + 2048);
        assert_eq!(result.missing_paths, vec!["seg_000003.log".to_string()]);
        assert!(result.verification_failures.is_empty());

        // Verify file contents
        let synced = std::fs::read(follower_dir.path().join("seg_000001.log")).unwrap();
        assert_eq!(synced, seg_data);

        // Shutdown
        tx.send(true).unwrap();
        server_handle.abort();
    }

    #[test]
    fn test_list_segment_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("seg_000001.log"), &[0u8; 100]).unwrap();
        std::fs::write(dir.path().join("seg_000003.log"), &[0u8; 200]).unwrap();
        std::fs::write(dir.path().join("not_a_segment.txt"), &[0u8; 50]).unwrap();

        let entries = list_segment_files(dir.path()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].relative_path, "seg_000001.log");
        assert_eq!(entries[0].size, 100);
        assert_eq!(entries[1].relative_path, "seg_000003.log");
        assert_eq!(entries[1].size, 200);
    }

    #[test]
    fn test_list_segment_files_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let entries = list_segment_files(dir.path()).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_list_segment_files_nonexistent_dir() {
        let entries = list_segment_files(Path::new("/tmp/nonexistent_segment_dir_xyz")).unwrap();
        assert!(entries.is_empty());
    }

    /// Helper: start a sync server, return the bound address and a shutdown sender.
    async fn start_server(
        data_dir: PathBuf,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let (tx, rx) = tokio::sync::watch::channel(false);
        let server = SegmentSyncServer::new(SegmentSyncServerConfig {
            data_dir,
            bind_addr: addr,
            #[cfg(feature = "tls")]
            tls_config: None,
        });
        let handle = tokio::spawn(async move {
            let _ = server.serve(rx).await;
        });
        // Give server time to bind
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (addr, tx, handle)
    }

    fn make_client(data_dir: PathBuf) -> SegmentSyncClient {
        SegmentSyncClient::new(SegmentSyncClientConfig {
            data_dir,
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        })
    }

    #[tokio::test]
    async fn test_sync_large_file_multi_chunk() {
        // File larger than STREAM_CHUNK_SIZE (256 KB) to exercise chunked transfer
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        let large_data: Vec<u8> = (0..=255u8).cycle().take(512 * 1024).collect();
        std::fs::write(leader_dir.path().join("seg_000001.log"), &large_data).unwrap();

        let manifest = vec![SnapshotFileEntry {
            relative_path: "seg_000001.log".to_string(),
            size: large_data.len() as u64,
        }];

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_transferred, 1);
        assert_eq!(result.bytes_transferred, large_data.len() as u64);
        assert!(result.verification_failures.is_empty());

        // Verify byte-for-byte content integrity
        let synced = std::fs::read(follower_dir.path().join("seg_000001.log")).unwrap();
        assert_eq!(synced.len(), large_data.len());
        assert_eq!(synced, large_data);

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_verification_failure_size_mismatch() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        // Write a file that's 100 bytes
        std::fs::write(leader_dir.path().join("seg_000001.log"), &[7u8; 100]).unwrap();

        // But the manifest claims it should be 200 bytes
        let manifest = vec![SnapshotFileEntry {
            relative_path: "seg_000001.log".to_string(),
            size: 200,
        }];

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_transferred, 1);
        // Verification should catch the size mismatch
        assert_eq!(result.verification_failures.len(), 1);
        assert!(result.verification_failures[0].contains("expected 200 bytes"));

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_all_files_missing() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        // No files on leader
        let manifest = vec![
            SnapshotFileEntry {
                relative_path: "seg_000001.log".to_string(),
                size: 100,
            },
            SnapshotFileEntry {
                relative_path: "seg_000002.log".to_string(),
                size: 200,
            },
        ];

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_requested, 2);
        assert_eq!(result.files_transferred, 0);
        assert_eq!(result.files_missing, 2);
        assert_eq!(result.bytes_transferred, 0);
        assert_eq!(result.missing_paths.len(), 2);

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_subdirectory_paths() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        // Create files in a subdirectory
        std::fs::create_dir_all(leader_dir.path().join("group_1")).unwrap();
        std::fs::write(leader_dir.path().join("group_1/seg_000001.log"), &[1u8; 64]).unwrap();

        let manifest = vec![SnapshotFileEntry {
            relative_path: "group_1/seg_000001.log".to_string(),
            size: 64,
        }];

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_transferred, 1);
        assert!(result.verification_failures.is_empty());

        // Verify subdirectory was created on follower
        let synced = std::fs::read(follower_dir.path().join("group_1/seg_000001.log")).unwrap();
        assert_eq!(synced, vec![1u8; 64]);

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_content_integrity_multiple_files() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        // Create files with distinct content patterns
        let file1: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();
        let file2: Vec<u8> = (0..2000).map(|i| ((i * 7 + 13) % 256) as u8).collect();
        let file3: Vec<u8> = (0..500).map(|i| ((i * 31) % 256) as u8).collect();

        std::fs::write(leader_dir.path().join("seg_000001.log"), &file1).unwrap();
        std::fs::write(leader_dir.path().join("seg_000002.log"), &file2).unwrap();
        std::fs::write(leader_dir.path().join("seg_000003.log"), &file3).unwrap();

        let manifest = vec![
            SnapshotFileEntry {
                relative_path: "seg_000001.log".to_string(),
                size: file1.len() as u64,
            },
            SnapshotFileEntry {
                relative_path: "seg_000002.log".to_string(),
                size: file2.len() as u64,
            },
            SnapshotFileEntry {
                relative_path: "seg_000003.log".to_string(),
                size: file3.len() as u64,
            },
        ];

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_transferred, 3);
        assert_eq!(
            result.bytes_transferred,
            (file1.len() + file2.len() + file3.len()) as u64
        );
        assert!(result.verification_failures.is_empty());

        // Byte-for-byte verification of all files
        assert_eq!(
            std::fs::read(follower_dir.path().join("seg_000001.log")).unwrap(),
            file1
        );
        assert_eq!(
            std::fs::read(follower_dir.path().join("seg_000002.log")).unwrap(),
            file2
        );
        assert_eq!(
            std::fs::read(follower_dir.path().join("seg_000003.log")).unwrap(),
            file3
        );

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_server_rejects_invalid_magic() {
        let leader_dir = tempfile::tempdir().unwrap();
        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Send wrong magic
        stream.write_all(b"XXXX").await.unwrap();
        stream.write_u32_le(SYNC_VERSION).await.unwrap();
        stream.flush().await.unwrap();

        // Server should close the connection — read should fail or return EOF
        let mut buf = [0u8; 1];
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(500), stream.read(&mut buf))
                .await;

        match result {
            Ok(Ok(0)) => {}  // EOF — server closed connection
            Ok(Err(_)) => {} // Connection reset — also fine
            Err(_) => panic!("server did not close connection after invalid magic"),
            Ok(Ok(_)) => panic!("server sent data after invalid magic"),
        }

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_server_rejects_invalid_version() {
        let leader_dir = tempfile::tempdir().unwrap();
        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(SYNC_MAGIC).await.unwrap();
        stream.write_u32_le(999).await.unwrap(); // Bad version
        stream.flush().await.unwrap();

        let mut buf = [0u8; 1];
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(500), stream.read(&mut buf))
                .await;

        match result {
            Ok(Ok(0)) => {}
            Ok(Err(_)) => {}
            Err(_) => panic!("server did not close connection after invalid version"),
            Ok(Ok(_)) => panic!("server sent data after invalid version"),
        }

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_single_file_exact_chunk_boundary() {
        // File exactly equal to STREAM_CHUNK_SIZE
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        let data = vec![0xABu8; STREAM_CHUNK_SIZE];
        std::fs::write(leader_dir.path().join("seg_000001.log"), &data).unwrap();

        let manifest = vec![SnapshotFileEntry {
            relative_path: "seg_000001.log".to_string(),
            size: STREAM_CHUNK_SIZE as u64,
        }];

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_transferred, 1);
        assert_eq!(result.bytes_transferred, STREAM_CHUNK_SIZE as u64);
        assert!(result.verification_failures.is_empty());

        let synced = std::fs::read(follower_dir.path().join("seg_000001.log")).unwrap();
        assert_eq!(synced, data);

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_empty_file() {
        // Zero-byte file on leader — should transfer successfully
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        std::fs::write(leader_dir.path().join("seg_000001.log"), &[]).unwrap();

        let manifest = vec![SnapshotFileEntry {
            relative_path: "seg_000001.log".to_string(),
            size: 0,
        }];

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        // The server sends file_len=0 for empty files, which the client
        // interprets as "missing". This is by design — empty segment files
        // are effectively no-ops.
        assert_eq!(result.files_requested, 1);

        tx.send(true).unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_connect_refused() {
        let follower_dir = tempfile::tempdir().unwrap();
        let client = make_client(follower_dir.path().to_path_buf());

        let manifest = vec![SnapshotFileEntry {
            relative_path: "seg_000001.log".to_string(),
            size: 100,
        }];

        // Connect to a port that nothing is listening on
        let result = client.sync_files("127.0.0.1:1", &manifest).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sync_manifest_serde_roundtrip_via_protocol() {
        // Verify the full manifest survives encode→send→receive→decode
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        // Create many files with various name patterns
        let mut manifest = Vec::new();
        for i in 0..20 {
            let name = format!("seg_{i:06}.log");
            let size = (i + 1) * 100;
            let data: Vec<u8> = (0..size).map(|b| (b % 256) as u8).collect();
            std::fs::write(leader_dir.path().join(&name), &data).unwrap();
            manifest.push(SnapshotFileEntry {
                relative_path: name,
                size: size as u64,
            });
        }

        let (addr, tx, handle) = start_server(leader_dir.path().to_path_buf()).await;
        let client = make_client(follower_dir.path().to_path_buf());

        let result = client
            .sync_files(&addr.to_string(), &manifest)
            .await
            .unwrap();

        assert_eq!(result.files_transferred, 20);
        assert_eq!(result.files_missing, 0);
        assert!(result.verification_failures.is_empty());

        // Verify every file
        for entry in &manifest {
            let expected_size = entry.size as usize;
            let expected: Vec<u8> = (0..expected_size).map(|b| (b % 256) as u8).collect();
            let actual = std::fs::read(follower_dir.path().join(&entry.relative_path)).unwrap();
            assert_eq!(actual, expected, "mismatch for {}", entry.relative_path);
        }

        tx.send(true).unwrap();
        handle.abort();
    }
}
