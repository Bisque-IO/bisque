//! Integration test: full snapshot-install-with-segment-sync flow.
//!
//! Exercises the end-to-end path:
//!   1. Leader builds engine state + creates segment files on disk
//!   2. Leader's snapshot builder includes file manifest
//!   3. Snapshot is serialized
//!   4. Follower's install_snapshot syncs segment files via TCP
//!   5. Follower restores engine state
//!   6. Verify segment files exist on follower and engine is restored

use std::io::Cursor;
use std::path::PathBuf;

use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::state_machine::MqStateMachine;
use bisque_mq::types::*;
use bisque_raft::{
    SegmentSyncClient, SegmentSyncClientConfig, SegmentSyncServer, SegmentSyncServerConfig,
    SnapshotFileEntry,
};
use bytes::Bytes;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{LogId, SnapshotMeta, StoredMembership};
use tokio::net::TcpListener;

type MqTypeConfig = bisque_raft::BisqueRaftTypeConfig<MqCommand, MqResponse>;

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-segment-sync-test"))
}

fn make_msg(value: &[u8]) -> MessagePayload {
    MessagePayload {
        key: None,
        value: Bytes::from(value.to_vec()),
        headers: Vec::new(),
        timestamp: 1000,
        ttl_ms: None,
        routing_key: None,
    }
}

fn leader_id() -> openraft::impls::leader_id_adv::LeaderId<MqTypeConfig> {
    openraft::impls::leader_id_adv::LeaderId {
        term: 1,
        node_id: 1,
    }
}

fn make_snapshot_meta(index: u64, id: &str) -> SnapshotMeta<MqTypeConfig> {
    SnapshotMeta {
        last_log_id: Some(LogId {
            leader_id: leader_id(),
            index,
        }),
        last_membership: StoredMembership::default(),
        snapshot_id: id.to_string(),
    }
}

fn make_server_config(
    data_dir: PathBuf,
    bind_addr: std::net::SocketAddr,
) -> SegmentSyncServerConfig {
    SegmentSyncServerConfig {
        data_dir,
        bind_addr,
        tls_config: None,
    }
}

fn make_client_config(data_dir: PathBuf) -> SegmentSyncClientConfig {
    SegmentSyncClientConfig {
        data_dir,
        tls_config: None,
        tls_server_name: None,
    }
}

/// Start a segment sync server, return address and shutdown handle.
async fn start_sync_server(
    data_dir: PathBuf,
) -> (
    String,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = SegmentSyncServer::new(make_server_config(data_dir, addr));
    let handle = tokio::spawn(async move {
        let _ = server.serve(rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr.to_string(), tx, handle)
}

/// Full end-to-end test: leader snapshot builder → serialize → follower
/// install_snapshot with segment file sync.
#[tokio::test]
async fn test_snapshot_install_with_segment_sync() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    // Create fake segment files in leader's group directory
    let leader_group_dir = leader_dir.path().to_path_buf();
    let seg1_data: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let seg2_data: Vec<u8> = (0..8192).map(|i| ((i * 7 + 3) % 256) as u8).collect();
    std::fs::write(leader_group_dir.join("seg_000001.log"), &seg1_data).unwrap();
    std::fs::write(leader_group_dir.join("seg_000002.log"), &seg2_data).unwrap();

    // Start sync server pointed at leader's data dir
    let (sync_addr, shutdown_tx, server_handle) = start_sync_server(leader_group_dir.clone()).await;

    // --- Leader side: build snapshot with file manifest ---
    let mut leader_engine = make_engine();
    leader_engine.apply_command(
        MqCommand::CreateTopic {
            name: "events".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    leader_engine.apply_command(
        MqCommand::Publish {
            topic_id: 1,
            messages: vec![make_msg(b"msg1"), make_msg(b"msg2")],
        },
        2,
        1001,
    );
    leader_engine.apply_command(
        MqCommand::CreateQueue {
            name: "tasks".to_string(),
            config: bisque_mq::config::QueueConfig::default(),
        },
        3,
        1002,
    );

    // Create leader state machine with group_dir and sync_addr
    let mut leader_sm = MqStateMachine::new(leader_engine)
        .with_group_dir(leader_group_dir.clone())
        .with_sync_addr(sync_addr.clone());

    // Build snapshot via the builder
    let mut builder = leader_sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    // Verify the snapshot includes file manifest
    let snapshot_bytes = snapshot.snapshot.into_inner();
    let (snap_data, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snapshot_bytes, bincode::config::standard()).unwrap();

    assert_eq!(snap_data.file_manifest.len(), 2);
    assert_eq!(snap_data.sync_addr, Some(sync_addr.clone()));
    assert_eq!(snap_data.topics.len(), 1);
    assert_eq!(snap_data.queues.len(), 1);

    // --- Follower side: install snapshot with segment sync ---
    let follower_group_dir = follower_dir.path().to_path_buf();
    std::fs::create_dir_all(&follower_group_dir).unwrap();

    let sync_client = SegmentSyncClient::new(make_client_config(follower_group_dir.clone()));

    let follower_engine = make_engine();
    let mut follower_sm = MqStateMachine::new(follower_engine).with_sync_client(sync_client);

    // Install snapshot on follower
    follower_sm
        .install_snapshot(
            &make_snapshot_meta(3, "test-snap-1"),
            Cursor::new(snapshot_bytes),
        )
        .await
        .unwrap();

    // Verify segment files were synced to follower
    let synced_seg1 = std::fs::read(follower_group_dir.join("seg_000001.log")).unwrap();
    assert_eq!(synced_seg1, seg1_data, "seg_000001.log content mismatch");

    let synced_seg2 = std::fs::read(follower_group_dir.join("seg_000002.log")).unwrap();
    assert_eq!(synced_seg2, seg2_data, "seg_000002.log content mismatch");

    // Verify engine state was restored
    let follower_engine = follower_sm.shared_engine();
    let engine = follower_engine.read();
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 1);
    assert_eq!(snap.topics[0].meta.name, "events");
    assert_eq!(snap.topics[0].meta.message_count, 2);
    assert_eq!(snap.queues.len(), 1);
    assert_eq!(snap.queues[0].meta.name, "tasks");

    // Cleanup
    shutdown_tx.send(true).unwrap();
    server_handle.abort();
}

/// Test that install_snapshot works gracefully when no sync_client is configured
/// (file manifest present but no client to sync with).
#[tokio::test]
async fn test_snapshot_install_without_sync_client() {
    let mut snap_data = MqSnapshotData::default();
    snap_data.file_manifest = vec![SnapshotFileEntry {
        relative_path: "seg_000001.log".to_string(),
        size: 100,
    }];
    snap_data.sync_addr = Some("127.0.0.1:9999".to_string());

    let snapshot_bytes =
        bincode::serde::encode_to_vec(&snap_data, bincode::config::standard()).unwrap();

    let mut follower_sm = MqStateMachine::new(make_engine());

    // Should succeed (warn but not fail)
    follower_sm
        .install_snapshot(
            &make_snapshot_meta(1, "no-client"),
            Cursor::new(snapshot_bytes),
        )
        .await
        .unwrap();
}

/// Test that install_snapshot works when file_manifest is empty
/// (no segment files to sync — fresh cluster).
#[tokio::test]
async fn test_snapshot_install_empty_manifest() {
    let mut engine = make_engine();
    engine.apply_command(
        MqCommand::CreateTopic {
            name: "t".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );

    let snap_data = engine.snapshot();
    assert!(snap_data.file_manifest.is_empty());

    let snapshot_bytes =
        bincode::serde::encode_to_vec(&snap_data, bincode::config::standard()).unwrap();

    let mut follower_sm = MqStateMachine::new(make_engine());

    follower_sm
        .install_snapshot(
            &make_snapshot_meta(1, "empty-manifest"),
            Cursor::new(snapshot_bytes),
        )
        .await
        .unwrap();

    // Engine state should still be restored
    let follower_engine = follower_sm.shared_engine();
    let engine = follower_engine.read();
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 1);
    assert_eq!(snap.topics[0].meta.name, "t");
}

/// Test that snapshot builder includes segment files from group_dir.
#[tokio::test]
async fn test_snapshot_builder_includes_file_manifest() {
    let group_dir = tempfile::tempdir().unwrap();

    // Create some segment files
    std::fs::write(group_dir.path().join("seg_000001.log"), &[0u8; 100]).unwrap();
    std::fs::write(group_dir.path().join("seg_000005.log"), &[0u8; 500]).unwrap();
    std::fs::write(group_dir.path().join("not_a_segment.dat"), &[0u8; 50]).unwrap();

    let mut sm = MqStateMachine::new(make_engine())
        .with_group_dir(group_dir.path().to_path_buf())
        .with_sync_addr("10.0.0.1:5555".to_string());

    let mut builder = sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    let data = snapshot.snapshot.into_inner();
    let (snap, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&data, bincode::config::standard()).unwrap();

    assert_eq!(snap.file_manifest.len(), 2);
    assert_eq!(snap.file_manifest[0].relative_path, "seg_000001.log");
    assert_eq!(snap.file_manifest[0].size, 100);
    assert_eq!(snap.file_manifest[1].relative_path, "seg_000005.log");
    assert_eq!(snap.file_manifest[1].size, 500);
    assert_eq!(snap.sync_addr, Some("10.0.0.1:5555".to_string()));
}

/// Test that snapshot builder without group_dir produces empty manifest.
#[tokio::test]
async fn test_snapshot_builder_no_group_dir() {
    let mut sm = MqStateMachine::new(make_engine());

    let mut builder = sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    let data = snapshot.snapshot.into_inner();
    let (snap, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&data, bincode::config::standard()).unwrap();

    assert!(snap.file_manifest.is_empty());
    assert!(snap.sync_addr.is_none());
}

/// Test snapshot install with sync client but unreachable server (sync fails gracefully).
#[tokio::test]
async fn test_snapshot_install_sync_fails_gracefully() {
    let follower_dir = tempfile::tempdir().unwrap();

    let sync_client = SegmentSyncClient::new(make_client_config(follower_dir.path().to_path_buf()));

    let mut snap_data = MqSnapshotData::default();
    snap_data.file_manifest = vec![SnapshotFileEntry {
        relative_path: "seg_000001.log".to_string(),
        size: 100,
    }];
    // Point to a port nothing is listening on
    snap_data.sync_addr = Some("127.0.0.1:1".to_string());

    let snapshot_bytes =
        bincode::serde::encode_to_vec(&snap_data, bincode::config::standard()).unwrap();

    let mut follower_sm = MqStateMachine::new(make_engine()).with_sync_client(sync_client);

    // Should succeed — sync failure is logged but doesn't block snapshot install
    follower_sm
        .install_snapshot(
            &make_snapshot_meta(1, "sync-fail"),
            Cursor::new(snapshot_bytes),
        )
        .await
        .unwrap();
}
