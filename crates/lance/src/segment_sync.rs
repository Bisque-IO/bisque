//! Out-of-band segment file streaming for Raft snapshot recovery.
//!
//! When a fresh node receives a Raft snapshot, it gets table metadata but not
//! the actual Lance dataset files. This module provides a dedicated TCP server
//! and client to stream those files directly from the leader's disk to the
//! follower's disk with zero intermediate copies.
//!
//! ## Protocol
//!
//! **Request** (follower → leader):
//! ```text
//! [4B magic: "BSYN"]
//! [4B version: u32 LE]
//! [4B manifest_len: u32 LE]
//! [manifest_len bytes: bincode-encoded Vec<SnapshotFileEntry>]
//! ```
//!
//! **Response** (leader → follower, per file):
//! ```text
//! [2B path_len: u16 LE]
//! [path_len bytes: UTF-8 relative path]
//! [8B file_len: u64 LE]
//! [file_len bytes: raw file data streamed in chunks]
//! ```
//!
//! End sentinel: `[2B path_len: 0]`

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::types::SnapshotFileEntry;

/// Magic bytes identifying a segment sync request.
const SYNC_MAGIC: &[u8; 4] = b"BSYN";
/// Protocol version.
const SYNC_VERSION: u32 = 1;
/// Chunk size for file streaming (256 KB).
const STREAM_CHUNK_SIZE: usize = 256 * 1024;

// =============================================================================
// Sync Result
// =============================================================================

/// Result of a segment sync operation with verification details.
#[derive(Debug, Clone)]
pub struct SyncResult {
    /// Total files requested.
    pub files_requested: usize,
    /// Files successfully transferred.
    pub files_transferred: usize,
    /// Files that were missing on the leader (e.g., promoted to S3 during transfer).
    pub files_missing: usize,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Paths of missing files (for diagnostics).
    pub missing_paths: Vec<String>,
    /// Files that failed post-sync verification (size mismatch).
    pub verification_failures: Vec<String>,
}

// =============================================================================
// Progress Tracking
// =============================================================================

/// Tracks and reports transfer progress.
pub struct SyncProgress {
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

    /// Report progress if at least 1 second has elapsed since last report.
    fn maybe_report(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_report) < Duration::from_secs(1) {
            return;
        }
        self.last_report = now;

        let elapsed = now.duration_since(self.start_time).as_secs_f64();
        let rate = if elapsed > 0.0 {
            self.transferred_bytes as f64 / elapsed
        } else {
            0.0
        };

        let pct = if self.total_bytes > 0 {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        } else {
            100.0
        };

        let remaining_bytes = self.total_bytes.saturating_sub(self.transferred_bytes);
        let eta_secs = if rate > 0.0 {
            (remaining_bytes as f64 / rate) as u64
        } else {
            0
        };

        info!(
            "{}: {}/{} files, {} / {} ({:.1}%), {}/s, ETA {}s",
            self.direction,
            self.files_done,
            self.files_total,
            format_bytes(self.transferred_bytes),
            format_bytes(self.total_bytes),
            pct,
            format_bytes(rate as u64),
            eta_secs,
        );
    }

    fn report_complete(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 {
            self.transferred_bytes as f64 / elapsed
        } else {
            0.0
        };

        info!(
            "{} complete: {} files, {} in {:.1}s ({}/s avg)",
            self.direction,
            self.files_done,
            format_bytes(self.transferred_bytes),
            elapsed,
            format_bytes(rate as u64),
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
        format!("{} B", bytes)
    }
}

// =============================================================================
// Server (leader side)
// =============================================================================

/// Configuration for the segment sync server.
pub struct SegmentSyncServerConfig {
    /// Base data directory (files are read relative to this).
    pub data_dir: PathBuf,
    /// Address to bind the sync server to.
    pub bind_addr: std::net::SocketAddr,
    /// Optional TLS config for encrypted transfers.
    #[cfg(feature = "tls")]
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
}

/// Serves segment files to fresh follower nodes.
pub struct SegmentSyncServer {
    config: SegmentSyncServerConfig,
}

impl SegmentSyncServer {
    pub fn new(config: SegmentSyncServerConfig) -> Self {
        Self { config }
    }

    /// Start serving. Returns when the shutdown signal is received.
    /// This is designed to be spawned as a background task.
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
                    stream.set_nodelay(true)?;
                    info!(peer = %peer, "Segment sync connection accepted");

                    let data_dir = self.config.data_dir.clone();

                    #[cfg(feature = "tls")]
                    let tls_acceptor = tls_acceptor.clone();

                    tokio::spawn(async move {
                        let result = {
                            #[cfg(feature = "tls")]
                            {
                                if let Some(ref acceptor) = tls_acceptor {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            Self::handle_connection(tls_stream, &data_dir).await
                                        }
                                        Err(e) => {
                                            error!(peer = %peer, "TLS handshake failed: {}", e);
                                            return;
                                        }
                                    }
                                } else {
                                    Self::handle_connection(stream, &data_dir).await
                                }
                            }
                            #[cfg(not(feature = "tls"))]
                            {
                                Self::handle_connection(stream, &data_dir).await
                            }
                        };

                        if let Err(e) = result {
                            error!(peer = %peer, "Segment sync connection error: {}", e);
                        }
                    });
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Segment sync server shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle a single sync connection.
    async fn handle_connection<S>(mut stream: S, data_dir: &Path) -> io::Result<()>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // Read request header
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic).await?;
        if &magic != SYNC_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid sync magic bytes",
            ));
        }

        let version = stream.read_u32_le().await?;
        if version != SYNC_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported sync protocol version: {}", version),
            ));
        }

        // Read requested file manifest
        let manifest_len = stream.read_u32_le().await? as usize;
        let mut manifest_bytes = vec![0u8; manifest_len];
        stream.read_exact(&mut manifest_bytes).await?;

        let (manifest, _): (Vec<SnapshotFileEntry>, _) =
            bincode::serde::decode_from_slice(&manifest_bytes, bincode::config::standard())
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Bad manifest: {}", e))
                })?;

        let total_bytes: u64 = manifest.iter().map(|e| e.size).sum();
        let files_total = manifest.len();
        info!(files = files_total, total_bytes, "Serving segment files");

        let mut progress = SyncProgress::new(total_bytes, files_total, "Segment sync send");
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];

        for entry in &manifest {
            // Validate path to prevent directory traversal
            validate_relative_path(&entry.relative_path)?;
            let file_path = data_dir.join(&entry.relative_path);

            // Send path header
            let path_bytes = entry.relative_path.as_bytes();
            stream.write_u16_le(path_bytes.len() as u16).await?;
            stream.write_all(path_bytes).await?;

            // Open and stream file
            let mut file = match tokio::fs::File::open(&file_path).await {
                Ok(f) => f,
                Err(e) => {
                    error!(path = %entry.relative_path, "Failed to open file: {}", e);
                    // Send file_len = 0 to signal skip, then continue
                    // (follower will handle missing file)
                    stream.write_u64_le(0).await?;
                    continue;
                }
            };

            // Get actual size (might differ from manifest if file changed)
            let metadata = file.metadata().await?;
            let actual_size = metadata.len();
            stream.write_u64_le(actual_size).await?;

            // Stream file data
            let mut remaining = actual_size;
            while remaining > 0 {
                let to_read = (remaining as usize).min(STREAM_CHUNK_SIZE);
                let n = file.read(&mut buf[..to_read]).await?;
                if n == 0 {
                    break;
                }
                stream.write_all(&buf[..n]).await?;
                remaining -= n as u64;
                progress.add_bytes(n as u64);
                progress.maybe_report();
            }

            progress.complete_file();
        }

        // Send end sentinel
        stream.write_u16_le(0).await?;
        stream.flush().await?;

        progress.report_complete();
        Ok(())
    }
}

// =============================================================================
// Client (follower side)
// =============================================================================

/// Configuration for the segment sync client.
pub struct SegmentSyncClientConfig {
    /// Base data directory (files are written relative to this).
    pub data_dir: PathBuf,
    /// Optional TLS config for encrypted transfers.
    #[cfg(feature = "tls")]
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
    /// Optional TLS server name for certificate verification.
    #[cfg(feature = "tls")]
    pub tls_server_name: Option<rustls::pki_types::ServerName<'static>>,
}

/// Streams segment files from the leader.
pub struct SegmentSyncClient {
    config: SegmentSyncClientConfig,
}

impl SegmentSyncClient {
    pub fn new(config: SegmentSyncClientConfig) -> Self {
        Self { config }
    }

    /// Connect to the leader's sync server and download all files in the manifest.
    ///
    /// Returns a [`SyncResult`] with details about what was transferred,
    /// what was missing, and whether post-sync verification passed.
    pub async fn sync_files(
        &self,
        leader_addr: &str,
        manifest: &[SnapshotFileEntry],
    ) -> crate::Result<SyncResult> {
        if manifest.is_empty() {
            debug!("Empty file manifest, nothing to sync");
            return Ok(SyncResult {
                files_requested: 0,
                files_transferred: 0,
                files_missing: 0,
                bytes_transferred: 0,
                missing_paths: Vec::new(),
                verification_failures: Vec::new(),
            });
        }

        let total_bytes: u64 = manifest.iter().map(|e| e.size).sum();
        info!(
            addr = leader_addr,
            files = manifest.len(),
            total_bytes,
            "Connecting to leader for segment sync"
        );

        let stream = TcpStream::connect(leader_addr)
            .await
            .map_err(|e| crate::error::Error::SegmentSync(format!("Connect failed: {}", e)))?;
        stream
            .set_nodelay(true)
            .map_err(|e| crate::error::Error::SegmentSync(format!("Set nodelay: {}", e)))?;

        // Apply TLS if configured
        #[cfg(feature = "tls")]
        let result = {
            if let (Some(tls_config), Some(server_name)) =
                (&self.config.tls_config, &self.config.tls_server_name)
            {
                let connector = tokio_rustls::TlsConnector::from(tls_config.clone());
                let tls_stream = connector
                    .connect(server_name.clone(), stream)
                    .await
                    .map_err(|e| {
                        crate::error::Error::SegmentSync(format!("TLS handshake failed: {}", e))
                    })?;
                self.do_sync(tls_stream, manifest, total_bytes).await
            } else {
                self.do_sync(stream, manifest, total_bytes).await
            }
        };

        #[cfg(not(feature = "tls"))]
        let result = self.do_sync(stream, manifest, total_bytes).await;

        let sync_result = result?;

        // Post-sync verification: check all transferred files exist with correct sizes
        let mut verification_failures = Vec::new();
        for entry in manifest {
            let path = self.config.data_dir.join(&entry.relative_path);
            match tokio::fs::metadata(&path).await {
                Ok(meta) => {
                    if meta.len() != entry.size && entry.size > 0 {
                        // Size mismatch — file may have been modified during transfer
                        warn!(
                            path = %entry.relative_path,
                            expected = entry.size,
                            actual = meta.len(),
                            "Post-sync size mismatch"
                        );
                        verification_failures.push(entry.relative_path.clone());
                    }
                }
                Err(_) if sync_result.missing_paths.contains(&entry.relative_path) => {
                    // Expected missing — leader reported it during transfer
                }
                Err(e) => {
                    warn!(
                        path = %entry.relative_path,
                        error = %e,
                        "Post-sync verification: file not found"
                    );
                    verification_failures.push(entry.relative_path.clone());
                }
            }
        }

        if !verification_failures.is_empty() {
            warn!(
                failures = verification_failures.len(),
                "Post-sync verification found issues"
            );
        }

        Ok(SyncResult {
            verification_failures,
            ..sync_result
        })
    }

    /// Perform the actual sync over an established connection.
    async fn do_sync<S>(
        &self,
        mut stream: S,
        manifest: &[SnapshotFileEntry],
        total_bytes: u64,
    ) -> crate::Result<SyncResult>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // Send request header
        stream.write_all(SYNC_MAGIC).await.map_err(io_to_sync)?;
        stream
            .write_u32_le(SYNC_VERSION)
            .await
            .map_err(io_to_sync)?;

        // Encode and send manifest
        let manifest_bytes =
            bincode::serde::encode_to_vec(manifest, bincode::config::standard())
                .map_err(|e| crate::error::Error::SegmentSync(format!("Encode manifest: {}", e)))?;
        stream
            .write_u32_le(manifest_bytes.len() as u32)
            .await
            .map_err(io_to_sync)?;
        stream
            .write_all(&manifest_bytes)
            .await
            .map_err(io_to_sync)?;
        stream.flush().await.map_err(io_to_sync)?;

        // Receive files
        let mut progress = SyncProgress::new(total_bytes, manifest.len(), "Segment sync receive");
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
        let mut files_transferred = 0usize;
        let mut files_missing = 0usize;
        let mut missing_paths = Vec::new();
        let mut bytes_transferred = 0u64;

        loop {
            let path_len = stream.read_u16_le().await.map_err(io_to_sync)?;
            if path_len == 0 {
                // End sentinel
                break;
            }

            // Read relative path
            let mut path_bytes = vec![0u8; path_len as usize];
            stream
                .read_exact(&mut path_bytes)
                .await
                .map_err(io_to_sync)?;
            let relative_path = String::from_utf8(path_bytes).map_err(|e| {
                crate::error::Error::SegmentSync(format!("Invalid UTF-8 path: {}", e))
            })?;

            // Validate path to prevent directory traversal
            validate_relative_path(&relative_path).map_err(io_to_sync)?;

            // Read file size
            let file_len = stream.read_u64_le().await.map_err(io_to_sync)?;

            if file_len == 0 {
                warn!(
                    path = %relative_path,
                    "Leader reported missing file (likely promoted to S3 during transfer)"
                );
                files_missing += 1;
                missing_paths.push(relative_path);
                progress.complete_file();
                continue;
            }

            // Create parent directories and open output file
            let output_path = self.config.data_dir.join(&relative_path);
            if let Some(parent) = output_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(io_to_sync)?;
            }

            let mut file = tokio::fs::File::create(&output_path)
                .await
                .map_err(io_to_sync)?;

            // Stream file data to disk
            let mut remaining = file_len;
            while remaining > 0 {
                let to_read = (remaining as usize).min(STREAM_CHUNK_SIZE);
                stream
                    .read_exact(&mut buf[..to_read])
                    .await
                    .map_err(io_to_sync)?;
                file.write_all(&buf[..to_read]).await.map_err(io_to_sync)?;
                remaining -= to_read as u64;
                progress.add_bytes(to_read as u64);
                bytes_transferred += to_read as u64;
                progress.maybe_report();
            }

            file.flush().await.map_err(io_to_sync)?;
            files_transferred += 1;
            progress.complete_file();

            debug!(
                path = %relative_path,
                size = file_len,
                "File synced"
            );
        }

        progress.report_complete();

        if files_missing > 0 {
            warn!(
                files_missing,
                files_transferred,
                "Some files were unavailable on leader (promoted to S3 during transfer)"
            );
        }

        Ok(SyncResult {
            files_requested: manifest.len(),
            files_transferred,
            files_missing,
            bytes_transferred,
            missing_paths,
            verification_failures: Vec::new(),
        })
    }
}

fn io_to_sync(e: io::Error) -> crate::error::Error {
    crate::error::Error::SegmentSync(e.to_string())
}

/// Validate that a relative path does not escape the base directory.
///
/// Rejects paths containing `..`, absolute paths, and paths with null bytes.
/// This prevents directory traversal attacks where a malicious manifest
/// could read/write files outside the data directory.
fn validate_relative_path(relative_path: &str) -> io::Result<()> {
    // Reject null bytes
    if relative_path.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path contains null byte",
        ));
    }

    // Reject absolute paths
    if relative_path.starts_with('/') || relative_path.starts_with('\\') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Absolute path rejected: {}", relative_path),
        ));
    }

    // Reject path traversal components
    for component in Path::new(relative_path).components() {
        match component {
            std::path::Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Path traversal rejected: {}", relative_path),
                ));
            }
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Absolute path component rejected: {}", relative_path),
                ));
            }
            _ => {}
        }
    }

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_sync_roundtrip() {
        // Create temp dirs for server and client
        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();

        // Create some test files on the "leader" side
        let tables_dir = server_dir
            .path()
            .join("tables")
            .join("test_table")
            .join("segments")
            .join("1.lance");
        std::fs::create_dir_all(&tables_dir).unwrap();

        let file1_path = tables_dir.join("data.lance");
        let file1_data = vec![42u8; 1024 * 100]; // 100KB
        std::fs::write(&file1_path, &file1_data).unwrap();

        let file2_path = tables_dir.join("_metadata");
        let file2_data = b"lance metadata content";
        std::fs::write(&file2_path, file2_data).unwrap();

        // Build manifest
        let manifest = vec![
            SnapshotFileEntry {
                relative_path: "tables/test_table/segments/1.lance/data.lance".to_string(),
                size: file1_data.len() as u64,
            },
            SnapshotFileEntry {
                relative_path: "tables/test_table/segments/1.lance/_metadata".to_string(),
                size: file2_data.len() as u64,
            },
        ];

        // Start server
        let server_config = SegmentSyncServerConfig {
            data_dir: server_dir.path().to_path_buf(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            #[cfg(feature = "tls")]
            tls_config: None,
        };

        let listener = TcpListener::bind(server_config.bind_addr).await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let data_dir = server_config.data_dir.clone();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            SegmentSyncServer::handle_connection(stream, &data_dir)
                .await
                .unwrap();
        });

        // Run client
        let client_config = SegmentSyncClientConfig {
            data_dir: client_dir.path().to_path_buf(),
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        };
        let client = SegmentSyncClient::new(client_config);

        client
            .sync_files(&server_addr.to_string(), &manifest)
            .await
            .unwrap();

        server_handle.await.unwrap();

        // Verify files were transferred correctly
        let received1 = std::fs::read(
            client_dir
                .path()
                .join("tables/test_table/segments/1.lance/data.lance"),
        )
        .unwrap();
        assert_eq!(received1, file1_data);

        let received2 = std::fs::read(
            client_dir
                .path()
                .join("tables/test_table/segments/1.lance/_metadata"),
        )
        .unwrap();
        assert_eq!(received2, file2_data);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(10 * 1024 * 1024), "10.0 MB");
        assert_eq!(format_bytes(2 * 1024 * 1024 * 1024), "2.0 GB");
    }

    #[tokio::test]
    async fn test_empty_manifest_noop() {
        let client_dir = tempfile::tempdir().unwrap();
        let client_config = SegmentSyncClientConfig {
            data_dir: client_dir.path().to_path_buf(),
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        };
        let client = SegmentSyncClient::new(client_config);

        // Empty manifest should succeed immediately without connecting
        client.sync_files("127.0.0.1:12345", &[]).await.unwrap();
    }
}
