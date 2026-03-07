//! Integration tests for segment sync (snapshot file streaming).
//!
//! Tests cover:
//! - Full roundtrip file transfer with verification
//! - Missing files during transfer (S3 promotion scenario)
//! - Large file streaming
//! - Post-sync verification (size mismatch detection)
//! - File manifest building from disk
//! - Connection refused / leader unavailable
//! - Concurrent transfers
//! - Protocol error handling

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use bisque_lance::{
    BisqueLanceConfig, SegmentSyncClient, SegmentSyncClientConfig, SegmentSyncServer,
    SegmentSyncServerConfig, SnapshotFileEntry, SnapshotTransferGuard,
};

// =============================================================================
// Test helpers
// =============================================================================

/// Create a directory tree with test files under `base_dir`.
/// Returns the manifest entries for the created files.
fn create_test_files(base_dir: &Path, files: &[(&str, &[u8])]) -> Vec<SnapshotFileEntry> {
    let mut manifest = Vec::new();
    for (relative_path, data) in files {
        let full_path = base_dir.join(relative_path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&full_path, data).unwrap();
        manifest.push(SnapshotFileEntry {
            relative_path: relative_path.to_string(),
            size: data.len() as u64,
        });
    }
    manifest
}

/// Start a sync server on an ephemeral port, returns the bound address and shutdown sender.
async fn start_server(
    data_dir: &Path,
) -> (
    String,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    // Bind to get the actual port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let config = SegmentSyncServerConfig {
        data_dir: data_dir.to_path_buf(),
        bind_addr: addr.parse().unwrap(),
        #[cfg(feature = "tls")]
        tls_config: None,
    };
    let server = SegmentSyncServer::new(config);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let handle = tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });

    // Give the server a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, shutdown_tx, handle)
}

fn make_client(data_dir: &Path) -> SegmentSyncClient {
    SegmentSyncClient::new(SegmentSyncClientConfig {
        data_dir: data_dir.to_path_buf(),
        #[cfg(feature = "tls")]
        tls_config: None,
        #[cfg(feature = "tls")]
        tls_server_name: None,
    })
}

// =============================================================================
// Basic transfer tests
// =============================================================================

#[tokio::test]
async fn test_sync_multiple_files_roundtrip() {
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    // Create varied file sizes
    let small_data = b"hello world";
    let medium_data = vec![0xABu8; 64 * 1024]; // 64 KB
    let large_data = vec![0xCDu8; 512 * 1024]; // 512 KB

    let manifest = create_test_files(
        server_dir.path(),
        &[
            (
                "tables/orders/segments/1.lance/_metadata",
                small_data.as_slice(),
            ),
            (
                "tables/orders/segments/1.lance/data/0.lance",
                &medium_data,
            ),
            (
                "tables/users/segments/2.lance/data/0.lance",
                &large_data,
            ),
        ],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_requested, 3);
    assert_eq!(result.files_transferred, 3);
    assert_eq!(result.files_missing, 0);
    assert!(result.missing_paths.is_empty());
    assert!(result.verification_failures.is_empty());

    // Verify each file matches
    let r1 = std::fs::read(
        client_dir
            .path()
            .join("tables/orders/segments/1.lance/_metadata"),
    )
    .unwrap();
    assert_eq!(r1, small_data);

    let r2 = std::fs::read(
        client_dir
            .path()
            .join("tables/orders/segments/1.lance/data/0.lance"),
    )
    .unwrap();
    assert_eq!(r2, medium_data);

    let r3 = std::fs::read(
        client_dir
            .path()
            .join("tables/users/segments/2.lance/data/0.lance"),
    )
    .unwrap();
    assert_eq!(r3, large_data);

    shutdown_tx.send(true).unwrap();
    // Drop client so server connection closes
    drop(client);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_large_file_streaming() {
    // Test streaming a file larger than the chunk buffer (256 KB chunks)
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    // 2 MB file — requires multiple chunks
    let large_data: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let manifest = create_test_files(
        server_dir.path(),
        &[("tables/big/segments/1.lance/data/0.lance", &large_data)],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_transferred, 1);
    assert_eq!(result.bytes_transferred, large_data.len() as u64);
    assert!(result.verification_failures.is_empty());

    let received = std::fs::read(
        client_dir
            .path()
            .join("tables/big/segments/1.lance/data/0.lance"),
    )
    .unwrap();
    assert_eq!(received.len(), large_data.len());
    assert_eq!(received, large_data);

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Missing file / S3 promotion tests
// =============================================================================

#[tokio::test]
async fn test_sync_missing_file_on_leader() {
    // Simulate a file that was promoted to S3 between manifest build and transfer.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let existing_data = b"existing file data";
    create_test_files(
        server_dir.path(),
        &[("tables/t1/segments/1.lance/data/0.lance", existing_data)],
    );

    // Manifest includes both an existing file AND a missing file
    let manifest = vec![
        SnapshotFileEntry {
            relative_path: "tables/t1/segments/1.lance/data/0.lance".to_string(),
            size: existing_data.len() as u64,
        },
        SnapshotFileEntry {
            relative_path: "tables/t1/segments/2.lance/data/0.lance".to_string(),
            size: 999, // file doesn't exist on leader
        },
    ];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_requested, 2);
    assert_eq!(result.files_transferred, 1);
    assert_eq!(result.files_missing, 1);
    assert_eq!(
        result.missing_paths,
        vec!["tables/t1/segments/2.lance/data/0.lance"]
    );

    // The existing file should have been transferred correctly
    let received = std::fs::read(
        client_dir
            .path()
            .join("tables/t1/segments/1.lance/data/0.lance"),
    )
    .unwrap();
    assert_eq!(received, existing_data);

    // The missing file should not exist on client
    assert!(!client_dir
        .path()
        .join("tables/t1/segments/2.lance/data/0.lance")
        .exists());

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_all_files_missing() {
    // All files were promoted to S3 — should succeed with all missing
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let manifest = vec![
        SnapshotFileEntry {
            relative_path: "tables/gone/segments/1.lance/data/0.lance".to_string(),
            size: 100,
        },
        SnapshotFileEntry {
            relative_path: "tables/gone/segments/2.lance/data/0.lance".to_string(),
            size: 200,
        },
    ];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_requested, 2);
    assert_eq!(result.files_transferred, 0);
    assert_eq!(result.files_missing, 2);
    assert_eq!(result.bytes_transferred, 0);

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Connection error tests
// =============================================================================

#[tokio::test]
async fn test_sync_connection_refused() {
    let client_dir = tempfile::tempdir().unwrap();
    let client = make_client(client_dir.path());

    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t1/segments/1.lance/data.lance".to_string(),
        size: 100,
    }];

    // Connect to a port that nobody is listening on
    let result = client.sync_files("127.0.0.1:1", &manifest).await;
    assert!(result.is_err(), "Should fail when connection is refused");
}

#[tokio::test]
async fn test_sync_empty_manifest_skips_connection() {
    let client_dir = tempfile::tempdir().unwrap();
    let client = make_client(client_dir.path());

    // Empty manifest should succeed without connecting at all
    let result = client.sync_files("127.0.0.1:1", &[]).await.unwrap();
    assert_eq!(result.files_requested, 0);
    assert_eq!(result.files_transferred, 0);
}

// =============================================================================
// File manifest building
// =============================================================================

#[tokio::test]
async fn test_build_file_manifest_from_disk() {
    let tmp = tempfile::tempdir().unwrap();
    let config = BisqueLanceConfig::new(tmp.path());

    // Create some segment files
    let segments_dir = tmp
        .path()
        .join("tables")
        .join("events")
        .join("segments")
        .join("1.lance");
    std::fs::create_dir_all(segments_dir.join("data")).unwrap();
    std::fs::create_dir_all(segments_dir.join("_versions")).unwrap();

    std::fs::write(segments_dir.join("data/0.lance"), vec![0u8; 4096]).unwrap();
    std::fs::write(segments_dir.join("_versions/1.manifest"), b"manifest").unwrap();
    std::fs::write(segments_dir.join("_metadata"), b"meta").unwrap();

    let manifest = config.build_file_manifest().await.unwrap();

    assert_eq!(manifest.len(), 3);

    // All paths should be relative to local_data_dir
    for entry in &manifest {
        assert!(
            entry.relative_path.starts_with("tables/events/segments/"),
            "Path should be relative: {}",
            entry.relative_path
        );
        assert!(entry.size > 0);
    }

    // Check specific entries exist
    let paths: Vec<&str> = manifest.iter().map(|e| e.relative_path.as_str()).collect();
    assert!(paths.contains(&"tables/events/segments/1.lance/data/0.lance"));
    assert!(paths.contains(&"tables/events/segments/1.lance/_versions/1.manifest"));
    assert!(paths.contains(&"tables/events/segments/1.lance/_metadata"));
}

#[tokio::test]
async fn test_build_file_manifest_empty_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let config = BisqueLanceConfig::new(tmp.path());

    // No tables directory at all
    let manifest = config.build_file_manifest().await.unwrap();
    assert!(manifest.is_empty());
}

#[tokio::test]
async fn test_build_file_manifest_multiple_tables() {
    let tmp = tempfile::tempdir().unwrap();
    let config = BisqueLanceConfig::new(tmp.path());

    // Create files for two different tables
    for table_name in &["orders", "users"] {
        let seg_dir = tmp
            .path()
            .join("tables")
            .join(table_name)
            .join("segments")
            .join("1.lance")
            .join("data");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::write(seg_dir.join("0.lance"), vec![0u8; 1024]).unwrap();
    }

    let manifest = config.build_file_manifest().await.unwrap();
    assert_eq!(manifest.len(), 2);

    let has_orders = manifest
        .iter()
        .any(|e| e.relative_path.contains("orders"));
    let has_users = manifest.iter().any(|e| e.relative_path.contains("users"));
    assert!(has_orders);
    assert!(has_users);
}

// =============================================================================
// Concurrent transfer tests
// =============================================================================

#[tokio::test]
async fn test_sync_concurrent_clients() {
    // Multiple clients syncing from the same server simultaneously.
    let server_dir = tempfile::tempdir().unwrap();

    let data_len = 128 * 1024usize;
    let data = vec![0xFFu8; data_len];
    let manifest = create_test_files(
        server_dir.path(),
        &[("tables/t1/segments/1.lance/data/0.lance", &data)],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    // Launch 3 concurrent clients
    let mut handles = Vec::new();
    for i in 0..3 {
        let addr = addr.clone();
        let manifest = manifest.clone();
        handles.push(tokio::spawn(async move {
            let client_dir = tempfile::tempdir().unwrap();
            let client = make_client(client_dir.path());
            let result = client.sync_files(&addr, &manifest).await.unwrap();
            assert_eq!(result.files_transferred, 1, "Client {i} should transfer 1 file");
            assert!(
                result.verification_failures.is_empty(),
                "Client {i} should have no verification failures"
            );

            // Verify data
            let received = std::fs::read(
                client_dir
                    .path()
                    .join("tables/t1/segments/1.lance/data/0.lance"),
            )
            .unwrap();
            assert_eq!(received.len(), data_len, "Client {i} data size mismatch");
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Post-sync verification tests
// =============================================================================

#[tokio::test]
async fn test_sync_detects_size_mismatch() {
    // If a file on disk has the wrong size after transfer,
    // verification should catch it.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let actual_data = b"short";
    create_test_files(
        server_dir.path(),
        &[("tables/t1/segments/1.lance/data/0.lance", actual_data)],
    );

    // Manifest claims a larger size than what's actually on disk
    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t1/segments/1.lance/data/0.lance".to_string(),
        size: 99999, // claimed size differs from actual
    }];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    // Transfer succeeds but verification should flag the mismatch
    assert_eq!(result.files_transferred, 1);
    assert_eq!(
        result.verification_failures.len(),
        1,
        "Should detect size mismatch"
    );
    assert_eq!(
        result.verification_failures[0],
        "tables/t1/segments/1.lance/data/0.lance"
    );

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// SnapshotData serialization backward compatibility
// =============================================================================

#[tokio::test]
async fn test_snapshot_data_roundtrip_serialization() {
    use bisque_lance::SnapshotData;
    use std::collections::HashMap;

    // SnapshotData with file manifest and sync addr should round-trip cleanly
    let data = SnapshotData {
        tables: HashMap::new(),
        min_safe_log_index: Some(42),
        file_manifest: vec![
            SnapshotFileEntry {
                relative_path: "tables/t1/segments/1.lance/data/0.lance".to_string(),
                size: 1024,
            },
            SnapshotFileEntry {
                relative_path: "tables/t1/segments/1.lance/_metadata".to_string(),
                size: 256,
            },
        ],
        sync_addr: Some("10.0.0.1:9999".to_string()),
    };

    let bytes = bincode::serde::encode_to_vec(&data, bincode::config::standard()).unwrap();
    let (decoded, _): (SnapshotData, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

    assert_eq!(decoded.min_safe_log_index, Some(42));
    assert_eq!(decoded.file_manifest.len(), 2);
    assert_eq!(decoded.file_manifest[0].relative_path, "tables/t1/segments/1.lance/data/0.lance");
    assert_eq!(decoded.file_manifest[0].size, 1024);
    assert_eq!(decoded.sync_addr, Some("10.0.0.1:9999".to_string()));

    // Empty manifest/sync_addr also round-trips
    let data2 = SnapshotData {
        tables: HashMap::new(),
        min_safe_log_index: None,
        file_manifest: Vec::new(),
        sync_addr: None,
    };

    let bytes2 = bincode::serde::encode_to_vec(&data2, bincode::config::standard()).unwrap();
    let (decoded2, _): (SnapshotData, _) =
        bincode::serde::decode_from_slice(&bytes2, bincode::config::standard()).unwrap();

    assert!(decoded2.file_manifest.is_empty());
    assert!(decoded2.sync_addr.is_none());
    assert!(decoded2.min_safe_log_index.is_none());
}

// =============================================================================
// File overwrite behavior
// =============================================================================

#[tokio::test]
async fn test_sync_overwrites_existing_files() {
    // If a partial/stale file exists on the follower, sync should overwrite it.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let new_data = b"correct data from leader";
    let manifest = create_test_files(
        server_dir.path(),
        &[("tables/t1/segments/1.lance/data/0.lance", new_data)],
    );

    // Pre-populate client with stale data
    let stale_path = client_dir
        .path()
        .join("tables/t1/segments/1.lance/data/0.lance");
    std::fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    std::fs::write(&stale_path, b"stale old data").unwrap();

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();
    assert_eq!(result.files_transferred, 1);
    assert!(result.verification_failures.is_empty());

    // File should contain the new data, not the stale data
    let received = std::fs::read(&stale_path).unwrap();
    assert_eq!(received, new_data);

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// SyncResult correctness
// =============================================================================

#[tokio::test]
async fn test_sync_result_byte_count_accuracy() {
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let data1 = vec![1u8; 1000];
    let data2 = vec![2u8; 2000];
    let data3 = vec![3u8; 3000];

    let manifest = create_test_files(
        server_dir.path(),
        &[
            ("tables/t/segments/1.lance/a", &data1),
            ("tables/t/segments/1.lance/b", &data2),
            ("tables/t/segments/1.lance/c", &data3),
        ],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_requested, 3);
    assert_eq!(result.files_transferred, 3);
    assert_eq!(result.bytes_transferred, 6000); // 1000 + 2000 + 3000
    assert!(result.verification_failures.is_empty());

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Guard + Sync: simulated apply_promote during transfer
// =============================================================================

#[tokio::test]
async fn test_sync_guard_defers_promote_during_transfer() {
    // Simulates the scenario where apply_promote runs while a snapshot
    // transfer is streaming files — the guard defers file deletion until
    // the transfer completes, ensuring the follower gets complete data.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();
    let guard = Arc::new(SnapshotTransferGuard::new());

    // Create segment files that would be included in a manifest
    let segment_data = vec![0xAAu8; 64 * 1024]; // 64 KB
    let manifest = create_test_files(
        server_dir.path(),
        &[(
            "tables/orders/segments/1.lance/data/0.lance",
            &segment_data,
        )],
    );

    // Step 1: Acquire guard (simulates get_snapshot_builder)
    let handle = guard.acquire(Duration::from_secs(60), None);

    // Step 2: Start sync server and transfer
    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    // Step 3: Simulate apply_promote arriving during transfer
    // The sealed segment directory would be deleted, but guard defers it
    let sealed_dir = server_dir
        .path()
        .join("tables/orders/segments/1.lance");
    assert!(guard.defer_or_delete(sealed_dir.clone()));
    assert!(
        sealed_dir.exists(),
        "Sealed segment should NOT be deleted during active transfer"
    );

    // Step 4: Transfer completes successfully (files still exist because deferred)
    let client = make_client(client_dir.path());
    let result = client.sync_files(&addr, &manifest).await.unwrap();
    assert_eq!(result.files_transferred, 1);
    assert!(result.verification_failures.is_empty());

    // Verify follower has correct data
    let received = std::fs::read(
        client_dir
            .path()
            .join("tables/orders/segments/1.lance/data/0.lance"),
    )
    .unwrap();
    assert_eq!(received, segment_data);

    // Step 5: Release guard — deferred deletion now happens
    drop(handle);
    assert!(
        !sealed_dir.exists(),
        "Sealed segment should be deleted after transfer completes"
    );

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_guard_defers_drop_table_during_transfer() {
    // Simulates drop_table during active snapshot transfer.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();
    let guard = Arc::new(SnapshotTransferGuard::new());

    let manifest = create_test_files(
        server_dir.path(),
        &[
            ("tables/doomed/segments/1.lance/data/0.lance", b"data1"),
            (
                "tables/doomed/segments/1.lance/_metadata",
                b"meta",
            ),
        ],
    );

    let handle = guard.acquire(Duration::from_secs(60), None);

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    // drop_table defers the entire table directory
    let table_dir = server_dir.path().join("tables/doomed");
    assert!(guard.defer_or_delete(table_dir.clone()));

    // Transfer still works because files exist (deletion was deferred)
    let client = make_client(client_dir.path());
    let result = client.sync_files(&addr, &manifest).await.unwrap();
    assert_eq!(result.files_transferred, 2);
    assert!(result.verification_failures.is_empty());

    // Release → deferred deletion
    drop(handle);
    assert!(!table_dir.exists());

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Concurrent snapshot transfers with guard
// =============================================================================

#[tokio::test]
async fn test_sync_concurrent_transfers_with_guard() {
    // Two nodes join simultaneously — both transfers must complete before
    // deferred deletions are flushed.
    let server_dir = tempfile::tempdir().unwrap();
    let guard = Arc::new(SnapshotTransferGuard::new());

    let data = vec![0xBBu8; 32 * 1024];
    let data_len = data.len();
    let manifest = create_test_files(
        server_dir.path(),
        &[("tables/t1/segments/1.lance/data/0.lance", &data)],
    );

    // Two snapshot builds = two guard handles
    let handle_a = guard.acquire(Duration::from_secs(60), None);
    let handle_b = guard.acquire(Duration::from_secs(60), None);

    // Promote arrives — deferred
    let seg_dir = server_dir.path().join("tables/t1/segments/1.lance");
    assert!(guard.defer_or_delete(seg_dir.clone()));

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    // Both clients transfer concurrently
    let mut join_handles = Vec::new();
    for i in 0..2 {
        let addr = addr.clone();
        let manifest = manifest.clone();
        join_handles.push(tokio::spawn(async move {
            let client_dir = tempfile::tempdir().unwrap();
            let client = make_client(client_dir.path());
            let result = client.sync_files(&addr, &manifest).await.unwrap();
            assert_eq!(
                result.files_transferred, 1,
                "Client {i} should transfer 1 file"
            );
            assert_eq!(result.bytes_transferred, data_len as u64);
            assert!(result.verification_failures.is_empty());
        }));
    }

    for jh in join_handles {
        jh.await.unwrap();
    }

    // First handle done — segment dir still exists
    drop(handle_a);
    assert_eq!(guard.active_count(), 1);
    assert!(
        seg_dir.exists(),
        "Should not delete until ALL transfers done"
    );

    // Second handle done — now flush
    drop(handle_b);
    assert_eq!(guard.active_count(), 0);
    assert!(
        !seg_dir.exists(),
        "Should delete after all transfers complete"
    );

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Guard timeout during actual transfer
// =============================================================================

#[tokio::test]
async fn test_sync_guard_timeout_during_idle() {
    // Guard acquired but no follower connects. After timeout, deferred
    // deletions are flushed and the guard releases.
    let tmp = tempfile::tempdir().unwrap();
    let guard = Arc::new(SnapshotTransferGuard::new());

    let dir = tmp.path().join("tables/orphan/segments/1.lance");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("data"), b"orphan data").unwrap();

    // Short timeout to simulate follower never connecting
    let _handle = guard.acquire(Duration::from_millis(200), None);
    assert!(guard.defer_or_delete(dir.clone()));
    assert!(dir.exists());

    // Wait for timeout
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(guard.active_count(), 0);
    assert!(
        !dir.exists(),
        "Timeout should flush deferred deletions even if no follower connected"
    );
}

// =============================================================================
// Transfer with no guard (normal operation, no snapshot in flight)
// =============================================================================

#[tokio::test]
async fn test_sync_without_guard_deletes_immediately() {
    // When no snapshot is in flight, defer_or_delete returns false
    // and the caller deletes immediately.
    let tmp = tempfile::tempdir().unwrap();
    let guard = Arc::new(SnapshotTransferGuard::new());

    let dir = tmp.path().join("immediate_delete");
    std::fs::create_dir_all(&dir).unwrap();

    assert!(
        !guard.defer_or_delete(dir.clone()),
        "Should return false when no transfer is active"
    );

    // Caller is responsible for deletion — simulate that
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(!dir.exists());
}

// =============================================================================
// Multiple promotes during single transfer
// =============================================================================

#[tokio::test]
async fn test_sync_multiple_promotes_during_single_transfer() {
    // Multiple segments get promoted while a single transfer is active.
    // All deferred deletions should flush at once when transfer completes.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();
    let guard = Arc::new(SnapshotTransferGuard::new());

    // Create files for multiple segments across multiple tables
    let manifest = create_test_files(
        server_dir.path(),
        &[
            ("tables/t1/segments/1.lance/data/0.lance", b"seg1"),
            ("tables/t1/segments/2.lance/data/0.lance", b"seg2"),
            ("tables/t2/segments/1.lance/data/0.lance", b"seg3"),
        ],
    );

    let handle = guard.acquire(Duration::from_secs(60), None);

    // Simulate 3 promote events during the transfer
    let seg_dirs: Vec<PathBuf> = vec![
        server_dir.path().join("tables/t1/segments/1.lance"),
        server_dir.path().join("tables/t1/segments/2.lance"),
        server_dir.path().join("tables/t2/segments/1.lance"),
    ];
    for d in &seg_dirs {
        assert!(guard.defer_or_delete(d.clone()));
        assert!(d.exists());
    }

    // Transfer should succeed (files still on disk)
    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());
    let result = client.sync_files(&addr, &manifest).await.unwrap();
    assert_eq!(result.files_transferred, 3);
    assert!(result.verification_failures.is_empty());

    // Release — all 3 deferred dirs should be cleaned up
    drop(handle);
    for d in &seg_dirs {
        assert!(!d.exists(), "All deferred segments should be deleted");
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Path traversal / security tests
// =============================================================================

#[tokio::test]
async fn test_sync_rejects_path_traversal_in_manifest() {
    // A malicious manifest entry with ".." should be rejected by the server
    // to prevent reading files outside the data directory.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    create_test_files(
        server_dir.path(),
        &[("tables/t1/segments/1.lance/data", b"legit data")],
    );

    // Manifest with path traversal
    let manifest = vec![SnapshotFileEntry {
        relative_path: "../secret.txt".to_string(),
        size: 10,
    }];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await;
    assert!(result.is_err(), "Path traversal should be rejected");
    // Server rejects the path and closes the connection, so the client
    // sees either "traversal rejected" (if the error propagates through
    // the protocol) or "unexpected end of file" (connection close).

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_rejects_absolute_path_in_manifest() {
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let manifest = vec![SnapshotFileEntry {
        relative_path: "/etc/passwd".to_string(),
        size: 100,
    }];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await;
    assert!(result.is_err(), "Absolute path should be rejected");

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_rejects_nested_path_traversal() {
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/../../secret".to_string(),
        size: 10,
    }];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await;
    assert!(
        result.is_err(),
        "Nested path traversal should be rejected"
    );

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Zero-byte file edge case
// =============================================================================

#[tokio::test]
async fn test_sync_zero_byte_file() {
    // A zero-byte file on the leader is indistinguishable from "missing"
    // in the current protocol (file_len=0 means "skip"). This documents
    // the behavior: zero-byte files are reported as missing.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let empty_path = server_dir
        .path()
        .join("tables/t1/segments/1.lance/empty");
    std::fs::create_dir_all(empty_path.parent().unwrap()).unwrap();
    std::fs::write(&empty_path, b"").unwrap();
    assert_eq!(std::fs::metadata(&empty_path).unwrap().len(), 0);

    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t1/segments/1.lance/empty".to_string(),
        size: 0,
    }];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    // Zero-byte files: server sends file_len=0, client treats as "missing".
    // Known protocol limitation — real Lance datasets never have zero-byte files.
    assert_eq!(result.files_missing, 1);
    assert_eq!(result.files_transferred, 0);

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Malformed protocol tests (raw TCP)
// =============================================================================

#[tokio::test]
async fn test_sync_server_rejects_invalid_magic() {
    let server_dir = tempfile::tempdir().unwrap();
    create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data", b"x")],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    // Send invalid magic bytes
    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stream.write_all(b"BAAD").await.unwrap();
    stream.write_u32_le(1).await.unwrap();
    stream.flush().await.unwrap();

    // Server should close the connection
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut buf = [0u8; 1];
    let result = stream.read(&mut buf).await;
    match result {
        Ok(0) => {} // connection closed cleanly
        Err(_) => {} // connection reset
        Ok(_) => panic!("Server should have closed connection on bad magic"),
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_server_rejects_unsupported_version() {
    let server_dir = tempfile::tempdir().unwrap();
    create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data", b"x")],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stream.write_all(b"BSYN").await.unwrap();
    stream.write_u32_le(999).await.unwrap(); // unsupported version
    stream.flush().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut buf = [0u8; 1];
    let result = stream.read(&mut buf).await;
    match result {
        Ok(0) | Err(_) => {} // expected: server closes
        Ok(_) => panic!("Server should reject unsupported version"),
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_server_rejects_corrupt_manifest() {
    let server_dir = tempfile::tempdir().unwrap();
    create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data", b"x")],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stream.write_all(b"BSYN").await.unwrap();
    stream.write_u32_le(1).await.unwrap();

    // Send garbage manifest data
    let garbage = b"this is not valid bincode data";
    stream.write_u32_le(garbage.len() as u32).await.unwrap();
    stream.write_all(garbage).await.unwrap();
    stream.flush().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut buf = [0u8; 1];
    let result = stream.read(&mut buf).await;
    match result {
        Ok(0) | Err(_) => {} // expected: server closes
        Ok(_) => panic!("Server should reject corrupt manifest"),
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Connection drop mid-transfer
// =============================================================================

#[tokio::test]
async fn test_sync_client_handles_server_disconnect_mid_transfer() {
    // Server sends one file then disconnects WITHOUT end sentinel.
    // Client should error, not hang or silently succeed with partial data.
    let client_dir = tempfile::tempdir().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Read client header
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic).await.unwrap();
        let _version = stream.read_u32_le().await.unwrap();
        let manifest_len = stream.read_u32_le().await.unwrap();
        let mut manifest_bytes = vec![0u8; manifest_len as usize];
        stream.read_exact(&mut manifest_bytes).await.unwrap();

        // Send one complete file
        let path = b"tables/t/segments/1.lance/data";
        stream.write_u16_le(path.len() as u16).await.unwrap();
        stream.write_all(path).await.unwrap();
        stream.write_u64_le(100).await.unwrap();
        stream.write_all(&vec![0xAA; 100]).await.unwrap();

        // Disconnect without end sentinel (simulating crash)
        drop(stream);
    });

    let manifest = vec![
        SnapshotFileEntry {
            relative_path: "tables/t/segments/1.lance/data".to_string(),
            size: 100,
        },
        SnapshotFileEntry {
            relative_path: "tables/t/segments/2.lance/data".to_string(),
            size: 200,
        },
    ];

    let client = make_client(client_dir.path());
    let result = client.sync_files(&addr, &manifest).await;

    assert!(
        result.is_err(),
        "Client should fail when server disconnects mid-transfer"
    );

    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_sync_client_handles_partial_file_data() {
    // Server claims file_len=1000 but only sends 500 bytes, then disconnects.
    // Client should error on the truncated read.
    let client_dir = tempfile::tempdir().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Read client header
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic).await.unwrap();
        let _version = stream.read_u32_le().await.unwrap();
        let manifest_len = stream.read_u32_le().await.unwrap();
        let mut manifest_bytes = vec![0u8; manifest_len as usize];
        stream.read_exact(&mut manifest_bytes).await.unwrap();

        // Send file header claiming 1000 bytes
        let path = b"tables/t/segments/1.lance/data";
        stream.write_u16_le(path.len() as u16).await.unwrap();
        stream.write_all(path).await.unwrap();
        stream.write_u64_le(1000).await.unwrap();

        // Only send 500 bytes, then disconnect
        stream.write_all(&vec![0xBB; 500]).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);
    });

    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t/segments/1.lance/data".to_string(),
        size: 1000,
    }];

    let client = make_client(client_dir.path());
    let result = client.sync_files(&addr, &manifest).await;

    assert!(
        result.is_err(),
        "Client should error on truncated file data"
    );

    server_handle.await.unwrap();
}

// =============================================================================
// Server shutdown during active transfer
// =============================================================================

#[tokio::test]
async fn test_sync_server_shutdown_during_transfer() {
    // Shutdown signal sent while a transfer is in progress.
    // Spawned connection handler should complete; new connections should fail.
    let server_dir = tempfile::tempdir().unwrap();

    let data = vec![0xCCu8; 1024 * 1024]; // 1 MB
    let manifest = create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data/0.lance", &data)],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    let client_dir = tempfile::tempdir().unwrap();
    let client = make_client(client_dir.path());
    let addr_clone = addr.clone();
    let manifest_clone = manifest.clone();

    let transfer = tokio::spawn(async move {
        client.sync_files(&addr_clone, &manifest_clone).await
    });

    // Send shutdown while transfer is likely still running
    tokio::time::sleep(Duration::from_millis(10)).await;
    shutdown_tx.send(true).unwrap();

    // In-flight transfer should complete successfully
    let result = transfer.await.unwrap();
    assert!(
        result.is_ok(),
        "In-flight transfer should complete despite server shutdown"
    );
    let sync_result = result.unwrap();
    assert_eq!(sync_result.files_transferred, 1);
    assert!(sync_result.verification_failures.is_empty());

    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;

    // New connections should fail after server shutdown
    let client_dir2 = tempfile::tempdir().unwrap();
    let client2 = make_client(client_dir2.path());
    let result2 = client2.sync_files(&addr, &manifest).await;
    assert!(
        result2.is_err(),
        "New connections should fail after server shutdown"
    );
}

// =============================================================================
// Many small files stress test
// =============================================================================

#[tokio::test]
async fn test_sync_many_small_files() {
    // Stress test: 500 small files with per-file protocol overhead.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let file_count = 500;
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..file_count {
        let table = i / 100;
        let segment = (i / 10) % 10;
        let file_idx = i % 10;
        let path = format!(
            "tables/t{}/segments/{}.lance/data/{}.lance",
            table, segment, file_idx
        );
        let data = format!("file-{}-content-{}", i, "x".repeat(i % 128)).into_bytes();
        files.push((path, data));
    }

    let manifest: Vec<SnapshotFileEntry> = files
        .iter()
        .map(|(path, data)| {
            let full_path = server_dir.path().join(path);
            std::fs::create_dir_all(full_path.parent().unwrap()).unwrap();
            std::fs::write(&full_path, data).unwrap();
            SnapshotFileEntry {
                relative_path: path.clone(),
                size: data.len() as u64,
            }
        })
        .collect();

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_requested, file_count);
    assert_eq!(result.files_transferred, file_count);
    assert_eq!(result.files_missing, 0);
    assert!(result.verification_failures.is_empty());

    // Verify a random sample
    for i in [0, 42, 123, 256, 499] {
        let (path, expected_data) = &files[i];
        let received = std::fs::read(client_dir.path().join(path)).unwrap();
        assert_eq!(&received, expected_data, "File {i} content mismatch");
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;
}

// =============================================================================
// File changes during transfer
// =============================================================================

#[tokio::test]
async fn test_sync_file_grows_during_transfer() {
    // If a file grows between manifest build and transfer, the server sends
    // the actual (larger) size. Post-sync verification detects the mismatch.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let original_data = vec![0xAAu8; 1000];
    create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data/0.lance", &original_data)],
    );

    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t/segments/1.lance/data/0.lance".to_string(),
        size: 1000,
    }];

    // Grow the file before the transfer
    let file_path = server_dir
        .path()
        .join("tables/t/segments/1.lance/data/0.lance");
    std::fs::write(&file_path, &vec![0xBBu8; 2000]).unwrap();

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_transferred, 1);
    // Verification detects: manifest says 1000, actual file is 2000
    assert_eq!(
        result.verification_failures.len(),
        1,
        "Should detect size mismatch when file grew during transfer"
    );

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

#[tokio::test]
async fn test_sync_file_shrinks_during_transfer() {
    // If a file shrinks between manifest build and transfer,
    // verification detects the mismatch.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let original_data = vec![0xAAu8; 5000];
    create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data/0.lance", &original_data)],
    );

    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t/segments/1.lance/data/0.lance".to_string(),
        size: 5000,
    }];

    // Shrink the file before transfer
    let file_path = server_dir
        .path()
        .join("tables/t/segments/1.lance/data/0.lance");
    std::fs::write(&file_path, &vec![0xBBu8; 500]).unwrap();

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_transferred, 1);
    assert_eq!(
        result.verification_failures.len(),
        1,
        "Should detect size mismatch when file shrunk during transfer"
    );

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Client receives path traversal from malicious server
// =============================================================================

#[tokio::test]
async fn test_sync_client_rejects_traversal_path_from_server() {
    // A malicious server sends a response with a path traversal attack.
    // The client should reject it.
    let client_dir = tempfile::tempdir().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Read client header
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic).await.unwrap();
        let _version = stream.read_u32_le().await.unwrap();
        let manifest_len = stream.read_u32_le().await.unwrap();
        let mut manifest_bytes = vec![0u8; manifest_len as usize];
        stream.read_exact(&mut manifest_bytes).await.unwrap();

        // Send a file with path traversal attack
        let evil_path = b"../../etc/evil";
        stream.write_u16_le(evil_path.len() as u16).await.unwrap();
        stream.write_all(evil_path).await.unwrap();
        stream.write_u64_le(10).await.unwrap();
        stream.write_all(b"evil data!").await.unwrap();

        // End sentinel
        stream.write_u16_le(0).await.unwrap();
        let _ = stream.flush().await;
    });

    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t/segments/1.lance/data".to_string(),
        size: 100,
    }];

    let client = make_client(client_dir.path());
    let result = client.sync_files(&addr, &manifest).await;

    assert!(
        result.is_err(),
        "Client should reject path traversal from server"
    );

    server_handle.await.unwrap();
}

// =============================================================================
// Deeply nested path
// =============================================================================

#[tokio::test]
async fn test_sync_deeply_nested_path() {
    // Test with a deeply nested path to verify protocol and filesystem handle it.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    let mut components: Vec<String> = vec!["tables".to_string(), "t".to_string()];
    for i in 0..50 {
        components.push(format!("deep_{:04}", i));
    }
    components.push("data.lance".to_string());
    let deep_path = components.join("/");

    let full_path = server_dir.path().join(&deep_path);
    std::fs::create_dir_all(full_path.parent().unwrap()).unwrap();
    std::fs::write(&full_path, b"deep data").unwrap();

    let manifest = vec![SnapshotFileEntry {
        relative_path: deep_path.clone(),
        size: 9,
    }];

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();
    assert_eq!(result.files_transferred, 1);
    assert!(result.verification_failures.is_empty());

    let received = std::fs::read(client_dir.path().join(&deep_path)).unwrap();
    assert_eq!(received, b"deep data");

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Client disconnects mid-transfer (server resilience)
// =============================================================================

#[tokio::test]
async fn test_sync_server_handles_client_disconnect() {
    // Client connects, sends valid header, but drops connection before
    // server finishes sending files. Server should not panic.
    let server_dir = tempfile::tempdir().unwrap();

    let data = vec![0xDDu8; 512 * 1024]; // 512 KB — enough for slow streaming
    create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data/0.lance", &data)],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    // Build a valid manifest
    let manifest = vec![SnapshotFileEntry {
        relative_path: "tables/t/segments/1.lance/data/0.lance".to_string(),
        size: data.len() as u64,
    }];

    // Connect, send valid request, then disconnect immediately
    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stream.write_all(b"BSYN").await.unwrap();
    stream.write_u32_le(1).await.unwrap();

    let manifest_bytes =
        bincode::serde::encode_to_vec(&manifest, bincode::config::standard()).unwrap();
    stream
        .write_u32_le(manifest_bytes.len() as u32)
        .await
        .unwrap();
    stream.write_all(&manifest_bytes).await.unwrap();
    stream.flush().await.unwrap();

    // Drop connection before receiving response
    drop(stream);

    // Give server time to handle the error
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Server should still be alive and accepting new connections
    let client_dir = tempfile::tempdir().unwrap();
    let client = make_client(client_dir.path());
    let result = client.sync_files(&addr, &manifest).await.unwrap();
    assert_eq!(result.files_transferred, 1, "Server should still work after client disconnect");

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Rapid sequential connections
// =============================================================================

#[tokio::test]
async fn test_sync_rapid_sequential_transfers() {
    // 10 sequential transfers to verify server handles rapid reconnections.
    let server_dir = tempfile::tempdir().unwrap();

    let data = b"rapid test data payload";
    let manifest = create_test_files(
        server_dir.path(),
        &[("tables/t/segments/1.lance/data", data.as_slice())],
    );

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;

    for i in 0..10 {
        let client_dir = tempfile::tempdir().unwrap();
        let client = make_client(client_dir.path());
        let result = client.sync_files(&addr, &manifest).await.unwrap();
        assert_eq!(
            result.files_transferred, 1,
            "Transfer {i} should succeed"
        );
        assert!(result.verification_failures.is_empty());
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

// =============================================================================
// Mixed success and failure manifest
// =============================================================================

#[tokio::test]
async fn test_sync_mix_of_existing_and_missing_preserves_order() {
    // Manifest with alternating existing and missing files.
    // Verify that existing files are all transferred correctly and
    // missing files are all tracked.
    let server_dir = tempfile::tempdir().unwrap();
    let client_dir = tempfile::tempdir().unwrap();

    // Create only odd-numbered files
    for i in (1..10).step_by(2) {
        let path = format!("tables/t/segments/{}.lance/data", i);
        let full = server_dir.path().join(&path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(&full, format!("data-{}", i).as_bytes()).unwrap();
    }

    // Manifest includes all 10 files (even ones don't exist)
    let manifest: Vec<SnapshotFileEntry> = (0..10)
        .map(|i| SnapshotFileEntry {
            relative_path: format!("tables/t/segments/{}.lance/data", i),
            size: if i % 2 == 1 {
                format!("data-{}", i).len() as u64
            } else {
                100 // doesn't exist
            },
        })
        .collect();

    let (addr, shutdown_tx, server_handle) = start_server(server_dir.path()).await;
    let client = make_client(client_dir.path());

    let result = client.sync_files(&addr, &manifest).await.unwrap();

    assert_eq!(result.files_transferred, 5, "Should transfer 5 existing files");
    assert_eq!(result.files_missing, 5, "Should report 5 missing files");

    // Verify each existing file was transferred correctly
    for i in (1..10).step_by(2) {
        let received = std::fs::read(
            client_dir
                .path()
                .join(format!("tables/t/segments/{}.lance/data", i)),
        )
        .unwrap();
        assert_eq!(
            received,
            format!("data-{}", i).as_bytes(),
            "File {i} content mismatch"
        );
    }

    // Verify missing files don't exist on client
    for i in (0..10).step_by(2) {
        let path = client_dir
            .path()
            .join(format!("tables/t/segments/{}.lance/data", i));
        assert!(!path.exists(), "Missing file {i} should not exist on client");
    }

    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}
