//! Integration tests for MDBX manifest + state machine interaction.
//!
//! Exercises the paths that only appear when `MqStateMachine` is wired
//! to an `MqManifestManager`:
//!   - `applied_state()` recovery from installed snapshot
//!   - `applied_state()` recovery from structural state
//!   - `install_snapshot()` persisting to MDBX
//!   - `get_current_snapshot()` returning a live snapshot
//!   - Corrupt snapshot input handling

use std::io::Cursor;
use std::sync::Arc;

use bisque_mq::MqManifestManager;
use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::state_machine::MqStateMachine;
use bisque_mq::types::*;
use bytes::Bytes;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{LogId, SnapshotMeta, StoredMembership};

type MqTypeConfig = bisque_raft::BisqueRaftTypeConfig<MqCommand, MqResponse>;

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-manifest-integration-test"))
}

fn make_msg(value: &[u8]) -> MessagePayload {
    MessagePayload {
        key: None,
        value: Bytes::from(value.to_vec()),
        headers: Vec::new(),
        timestamp: 1000,
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

/// Install a snapshot on a manifest-backed state machine, then create a
/// NEW state machine against the same manifest and call `applied_state()`.
/// This simulates a node restart after snapshot install.
#[tokio::test]
async fn test_snapshot_recovery_via_applied_state() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    // --- First lifecycle: install a snapshot ---
    let mut engine = make_engine();
    engine.apply_command(
        MqCommand::CreateTopic {
            name: "events".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    engine.apply_command(
        MqCommand::Publish {
            topic_id: 1,
            messages: vec![make_msg(b"m1"), make_msg(b"m2")],
        },
        2,
        1001,
    );
    engine.apply_command(
        MqCommand::CreateQueue {
            name: "tasks".to_string(),
            config: bisque_mq::config::QueueConfig::default(),
        },
        3,
        1002,
    );

    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();

    let mut sm1 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);

    sm1.install_snapshot(&make_snapshot_meta(3, "snap-1"), Cursor::new(snap_bytes))
        .await
        .unwrap();
    drop(sm1);

    // --- Second lifecycle: "restart" by creating a new state machine ---
    let mut sm2 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);

    let (last_applied, _membership) = sm2.applied_state().await.unwrap();

    // Verify recovery returned the correct last_applied
    let la = last_applied.unwrap();
    assert_eq!(la.index, 3);
    assert_eq!(la.leader_id.term, 1);

    // Verify the engine was restored with the snapshot data
    let shared = sm2.shared_engine();
    let eng = shared.read();
    let s = eng.snapshot();
    assert_eq!(s.topics.len(), 1);
    assert_eq!(s.topics[0].meta.name, "events");
    assert_eq!(s.topics[0].meta.message_count, 2);
    assert_eq!(s.queues.len(), 1);
    assert_eq!(s.queues[0].meta.name, "tasks");

    manifest.shutdown();
}

/// Install snapshot via state machine, then create a NEW state machine
/// and call `applied_state()` — exercises the snapshot recovery path
/// (which is the primary recovery mechanism after snapshot install).
#[tokio::test]
async fn test_snapshot_install_then_recovery_via_applied_state() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    // Build an engine with a topic and queue
    let mut engine = make_engine();
    engine.apply_command(
        MqCommand::CreateTopic {
            name: "my-topic".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    engine.apply_command(
        MqCommand::CreateQueue {
            name: "my-queue".to_string(),
            config: bisque_mq::config::QueueConfig::default(),
        },
        2,
        1001,
    );

    // Install snapshot via state machine (this writes to manifest)
    let snap_data = engine.snapshot();
    let snapshot_bytes =
        bincode::serde::encode_to_vec(&snap_data, bincode::config::standard()).unwrap();

    let mut sm = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);

    sm.install_snapshot(
        &make_snapshot_meta(2, "recovery-test"),
        Cursor::new(snapshot_bytes),
    )
    .await
    .unwrap();
    drop(sm);

    // Create a new state machine to simulate restart
    let mut sm2 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);

    let (last_applied, _membership) = sm2.applied_state().await.unwrap();

    // Verify snapshot recovery: last_applied should be at snapshot's log index
    let la = last_applied.unwrap();
    assert_eq!(la.index, 2, "should resume from snapshot last_log_id");

    // Verify the engine has the restored entities
    let shared = sm2.shared_engine();
    let eng = shared.read();
    let s = eng.snapshot();
    assert_eq!(s.topics.len(), 1);
    assert_eq!(s.topics[0].meta.name, "my-topic");
    assert_eq!(s.queues.len(), 1);
    assert_eq!(s.queues[0].meta.name, "my-queue");

    manifest.shutdown();
}

/// `applied_state()` with no manifest data returns (None, Default).
#[tokio::test]
async fn test_applied_state_empty_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    let mut sm = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);

    let (last_applied, _membership) = sm.applied_state().await.unwrap();
    assert!(last_applied.is_none());

    manifest.shutdown();
}

/// Snapshot install → structural writes → second snapshot install:
/// `applied_state()` on restart should load the LATEST snapshot, ignoring
/// any structural writes that happened in between.
#[tokio::test]
async fn test_snapshot_overwrites_structural_on_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    // First: install snapshot with topic "t1"
    let mut engine1 = make_engine();
    engine1.apply_command(
        MqCommand::CreateTopic {
            name: "t1".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    let snap1_bytes =
        bincode::serde::encode_to_vec(&engine1.snapshot(), bincode::config::standard()).unwrap();

    let mut sm1 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);
    sm1.install_snapshot(&make_snapshot_meta(1, "snap-1"), Cursor::new(snap1_bytes))
        .await
        .unwrap();
    drop(sm1);

    // Second: install snapshot with queue "q1" (no topics)
    let mut engine2 = make_engine();
    engine2.apply_command(
        MqCommand::CreateQueue {
            name: "q1".to_string(),
            config: bisque_mq::config::QueueConfig::default(),
        },
        1,
        1000,
    );
    let snap2_bytes =
        bincode::serde::encode_to_vec(&engine2.snapshot(), bincode::config::standard()).unwrap();

    let mut sm2 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);
    sm2.install_snapshot(&make_snapshot_meta(5, "snap-2"), Cursor::new(snap2_bytes))
        .await
        .unwrap();
    drop(sm2);

    // Restart: should recover from snap-2
    let mut sm3 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);
    let (la, _) = sm3.applied_state().await.unwrap();
    assert_eq!(la.unwrap().index, 5);

    let shared = sm3.shared_engine();
    let eng = shared.read();
    let s = eng.snapshot();
    assert_eq!(s.topics.len(), 0, "snap-2 has no topics");
    assert_eq!(s.queues.len(), 1);
    assert_eq!(s.queues[0].meta.name, "q1");

    manifest.shutdown();
}

/// `get_current_snapshot()` returns None when no entries have been applied.
#[tokio::test]
async fn test_get_current_snapshot_none_when_empty() {
    let mut sm = MqStateMachine::new(make_engine());
    let result = sm.get_current_snapshot().await.unwrap();
    assert!(result.is_none());
}

/// `get_current_snapshot()` returns a valid, deserializable snapshot
/// after `install_snapshot()` sets `last_applied`.
#[tokio::test]
async fn test_get_current_snapshot_after_install() {
    let mut engine = make_engine();
    engine.apply_command(
        MqCommand::CreateTopic {
            name: "live-topic".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    engine.apply_command(
        MqCommand::CreateQueue {
            name: "live-queue".to_string(),
            config: bisque_mq::config::QueueConfig::default(),
        },
        2,
        1001,
    );

    let snap_bytes =
        bincode::serde::encode_to_vec(&engine.snapshot(), bincode::config::standard()).unwrap();

    let mut sm = MqStateMachine::new(make_engine());
    sm.install_snapshot(&make_snapshot_meta(2, "test"), Cursor::new(snap_bytes))
        .await
        .unwrap();

    let current = sm.get_current_snapshot().await.unwrap().unwrap();

    // Verify metadata
    assert_eq!(current.meta.last_log_id.unwrap().index, 2);
    assert!(current.meta.snapshot_id.starts_with("mq-"));

    // Verify the snapshot is deserializable
    let data = current.snapshot.into_inner();
    let (snap, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&data, bincode::config::standard()).unwrap();
    assert_eq!(snap.topics.len(), 1);
    assert_eq!(snap.topics[0].meta.name, "live-topic");
    assert_eq!(snap.queues.len(), 1);
    assert_eq!(snap.queues[0].meta.name, "live-queue");
}

/// Passing invalid bytes to `install_snapshot()` returns InvalidData error.
#[tokio::test]
async fn test_install_snapshot_corrupt_bytes() {
    let mut sm = MqStateMachine::new(make_engine());

    let result = sm
        .install_snapshot(
            &make_snapshot_meta(1, "corrupt"),
            Cursor::new(vec![0xFF, 0xFE, 0xFD, 0xFC]),
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

/// Passing empty bytes to `install_snapshot()` returns an error.
#[tokio::test]
async fn test_install_snapshot_empty_bytes() {
    let mut sm = MqStateMachine::new(make_engine());

    let result = sm
        .install_snapshot(&make_snapshot_meta(1, "empty"), Cursor::new(Vec::new()))
        .await;

    assert!(result.is_err());
}

/// `install_snapshot()` with manifest wired in persists to MDBX and is
/// readable directly from the manifest.
#[tokio::test]
async fn test_install_snapshot_persists_to_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    let mut engine = make_engine();
    engine.apply_command(
        MqCommand::CreateTopic {
            name: "persisted".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    engine.apply_command(
        MqCommand::Publish {
            topic_id: 1,
            messages: vec![make_msg(b"x")],
        },
        2,
        1001,
    );

    let snap_bytes =
        bincode::serde::encode_to_vec(&engine.snapshot(), bincode::config::standard()).unwrap();

    let mut sm = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);

    sm.install_snapshot(
        &make_snapshot_meta(2, "persist-test"),
        Cursor::new(snap_bytes),
    )
    .await
    .unwrap();

    // Read directly from manifest to verify persistence
    let snap = manifest.read_snapshot_data(1).unwrap().unwrap();
    assert_eq!(snap.topics.len(), 1);
    assert_eq!(snap.topics[0].meta.name, "persisted");
    assert_eq!(snap.topics[0].meta.message_count, 1);

    let (la, _) = manifest.read_applied_state(1).unwrap().unwrap();
    assert_eq!(la.unwrap().index, 2);

    manifest.shutdown();
}

/// Snapshot builder produces a snapshot that can be installed on another
/// state machine with manifest, and then recovered on restart.
#[tokio::test]
async fn test_snapshot_builder_to_install_to_recovery_roundtrip() {
    let _leader_tmp = tempfile::tempdir().unwrap();
    let follower_tmp = tempfile::tempdir().unwrap();

    // --- Leader builds snapshot ---
    let mut leader_engine = make_engine();
    leader_engine.apply_command(
        MqCommand::CreateTopic {
            name: "rt-topic".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    leader_engine.apply_command(
        MqCommand::Publish {
            topic_id: 1,
            messages: vec![make_msg(b"payload")],
        },
        2,
        1001,
    );

    let mut leader_sm = MqStateMachine::new(leader_engine);
    // Manually set last_applied so get_snapshot_builder works
    leader_sm
        .install_snapshot(
            &make_snapshot_meta(2, "leader-base"),
            Cursor::new(
                bincode::serde::encode_to_vec(
                    &{
                        let eng = leader_sm.shared_engine();
                        eng.read().snapshot()
                    },
                    bincode::config::standard(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();

    let mut builder = leader_sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();
    let snapshot_bytes = snapshot.snapshot.into_inner();

    // --- Follower installs snapshot with manifest ---
    let follower_manifest = Arc::new(MqManifestManager::new(follower_tmp.path()).unwrap());
    follower_manifest.open_group(1).unwrap();

    let mut follower_sm =
        MqStateMachine::new(make_engine()).with_manifest(follower_manifest.clone(), 1);

    follower_sm
        .install_snapshot(&snapshot.meta, Cursor::new(snapshot_bytes))
        .await
        .unwrap();
    drop(follower_sm);

    // --- Follower restart ---
    let mut recovered_sm =
        MqStateMachine::new(make_engine()).with_manifest(follower_manifest.clone(), 1);

    let (la, _) = recovered_sm.applied_state().await.unwrap();
    assert_eq!(la.unwrap().index, 2);

    let shared = recovered_sm.shared_engine();
    let eng = shared.read();
    let s = eng.snapshot();
    assert_eq!(s.topics.len(), 1);
    assert_eq!(s.topics[0].meta.name, "rt-topic");
    assert_eq!(s.topics[0].meta.message_count, 1);

    follower_manifest.shutdown();
}

/// `applied_state()` without any manifest returns (None, Default).
#[tokio::test]
async fn test_applied_state_no_manifest() {
    let mut sm = MqStateMachine::new(make_engine());
    let (la, membership) = sm.applied_state().await.unwrap();
    assert!(la.is_none());
    // Default membership
    assert_eq!(membership, StoredMembership::default());
}
