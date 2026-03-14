//! Integration tests for the MQ engine.
//!
//! These tests exercise end-to-end workflows across multiple entity types,
//! snapshot/restore cycles, and complex multi-step operations.
//!
//! Since engine fields are `pub(crate)`, integration tests verify state via
//! `engine.snapshot()` and `engine.apply_command(, None)` responses.

use bytes::Bytes;

use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::*;

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-integration-test"))
}

fn make_flat_msg(value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(value).timestamp(1000).build()
}

fn make_flat_msg_with_key(key: &[u8], value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(value)
        .key(key)
        .timestamp(1000)
        .build()
}

// =============================================================================
// End-to-end topic workflow
// =============================================================================

#[test]
fn test_topic_publish_consume_commit_purge() {
    let mut engine = make_engine();

    // Create topic
    let topic_id = match engine.apply_command(
        &MqCommand::create_topic(
            "orders",
            RetentionPolicy {
                max_age_secs: Some(3600),
                ..Default::default()
            },
            0,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Create session instead of register_consumer
    engine.apply_command(
        &MqCommand::create_session(100, "order-processor", 30000, 60000),
        2,
        1001,
        None,
    );

    // Publish batch
    let messages: Vec<Bytes> = (0..10)
        .map(|i| make_flat_msg_with_key(format!("order-{}", i).as_bytes(), b"order-data"))
        .collect();
    let resp = engine.apply_command(&MqCommand::publish(topic_id, &messages), 3, 1002, None);
    match resp {
        MqResponse::Published { count, .. } => assert_eq!(count, 10),
        other => panic!("expected Published, got {:?}", other),
    }

    // Commit offset
    let resp = engine.apply_command(&MqCommand::commit_offset(topic_id, 100, 3), 4, 1003, None);
    assert!(matches!(resp, MqResponse::Ok));

    // Purge old messages
    let resp = engine.apply_command(&MqCommand::purge_topic(topic_id, 3), 5, 1004, None);
    assert!(matches!(resp, MqResponse::Ok));

    // Verify via snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 1);
    assert_eq!(snap.topics[0].meta.tail_index, 3);
    assert_eq!(snap.topics[0].consumer_offsets[0].committed_offset, 3);
}

// =============================================================================
// Full snapshot/restore roundtrip
// =============================================================================

#[test]
fn test_full_snapshot_restore_roundtrip() {
    let mut engine = make_engine();

    // Build diverse state
    engine.apply_command(
        &MqCommand::create_topic("events", RetentionPolicy::default(), 0),
        1,
        1000,
        None,
    );
    engine.apply_command(
        &MqCommand::publish(1, &[make_flat_msg(b"e1"), make_flat_msg(b"e2")]),
        2,
        1001,
        None,
    );
    engine.apply_command(&MqCommand::commit_offset(1, 100, 2), 3, 1002, None);

    engine.apply_command(
        &MqCommand::create_queue(
            "tasks",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            Some(&TopicDedupConfig {
                window_secs: 60,
                max_entries: 100_000,
            }),
            false,
            None,
            false,
            None,
        ),
        4,
        1003,
        None,
    );

    engine.apply_command(
        &MqCommand::create_actor_group(
            "actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        6,
        1005,
        None,
    );

    engine.apply_command(
        &MqCommand::create_job_cron(
            "cron-job",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
        ),
        8,
        1007,
        None,
    );

    engine.apply_command(
        &MqCommand::create_session(100, "workers", 30000, 60000),
        9,
        1008,
        None,
    );

    // Snapshot
    let snap = engine.snapshot();
    // 1 explicit topic + 3 auto-created source topics (queue, actor, job)
    assert!(snap.topics.len() >= 4);
    // Queue, actor group, and job cron are all consumer groups now
    assert!(snap.consumer_groups.len() >= 3);
    assert_eq!(snap.sessions.len(), 1);

    // Serialize/deserialize roundtrip
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    // Restore into fresh engine
    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    // Verify via second snapshot
    let snap2 = engine2.snapshot();
    assert!(snap2.topics.len() >= 4);
    let events_topic = snap2
        .topics
        .iter()
        .find(|t| t.meta.name == "events")
        .unwrap();
    assert_eq!(events_topic.meta.message_count, 2);
    assert_eq!(events_topic.consumer_offsets[0].committed_offset, 2);

    assert!(snap2.consumer_groups.len() >= 3);

    assert_eq!(snap2.sessions.len(), 1);
    assert_eq!(snap2.next_id, snap.next_id);

    // Engine continues to work after restore
    let resp = engine2.apply_command(
        &MqCommand::publish(1, &[make_flat_msg(b"post-restore")]),
        11,
        2000,
        None,
    );
    assert!(matches!(resp, MqResponse::Published { .. }));
}

// =============================================================================
// ID allocation uniqueness
// =============================================================================

#[test]
fn test_id_allocation_across_entity_types() {
    let mut engine = make_engine();

    let mut ids = Vec::new();
    for (i, name) in ["t1", "q1", "ns1", "j1"].iter().enumerate() {
        let resp = match i {
            0 => engine.apply_command(
                &MqCommand::create_topic(name, RetentionPolicy::default(), 0),
                (i + 1) as u64,
                1000,
                None,
            ),
            1 => engine.apply_command(
                &MqCommand::create_queue(
                    name,
                    AckVariantConfig::default(),
                    RetentionPolicy::default(),
                    None,
                    false,
                    None,
                    false,
                    None,
                ),
                (i + 1) as u64,
                1001,
                None,
            ),
            2 => engine.apply_command(
                &MqCommand::create_actor_group(
                    name,
                    ActorVariantConfig::default(),
                    RetentionPolicy::default(),
                    false,
                    None,
                ),
                (i + 1) as u64,
                1002,
                None,
            ),
            3 => engine.apply_command(
                &MqCommand::create_job_cron(
                    name,
                    AckVariantConfig::default(),
                    RetentionPolicy::default(),
                ),
                (i + 1) as u64,
                1003,
                None,
            ),
            _ => unreachable!(),
        };
        match resp {
            MqResponse::EntityCreated { id } => ids.push(id),
            other => panic!("expected EntityCreated, got {:?}", other),
        }
    }

    // All unique and monotonically increasing
    for i in 1..ids.len() {
        assert!(
            ids[i] > ids[i - 1],
            "IDs should be monotonically increasing: {:?}",
            ids
        );
    }
}

// =============================================================================
// Error handling
// =============================================================================

#[test]
fn test_operations_on_nonexistent_entities() {
    let mut engine = make_engine();

    let cases = vec![
        engine.apply_command(&MqCommand::delete_topic(999), 1, 1000, None),
        engine.apply_command(&MqCommand::delete_consumer_group(999), 2, 1000, None),
        engine.apply_command(&MqCommand::publish(999, &[]), 3, 1000, None),
        engine.apply_command(&MqCommand::group_deliver(999, 1, 1), 4, 1000, None),
    ];

    for resp in cases {
        assert!(
            matches!(resp, MqResponse::Error(_)),
            "expected Error, got {:?}",
            resp
        );
    }
}

#[test]
fn test_duplicate_entity_names() {
    let mut engine = make_engine();

    engine.apply_command(
        &MqCommand::create_topic("dup", RetentionPolicy::default(), 0),
        1,
        1000,
        None,
    );
    let resp = engine.apply_command(
        &MqCommand::create_topic("dup", RetentionPolicy::default(), 0),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Error(_)));

    engine.apply_command(
        &MqCommand::create_queue(
            "dup",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        3,
        1002,
        None,
    );
    let resp = engine.apply_command(
        &MqCommand::create_queue(
            "dup",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        4,
        1003,
        None,
    );
    assert!(matches!(resp, MqResponse::Error(_)));
}

// =============================================================================
// Delete and recreate entities
// =============================================================================

#[test]
fn test_delete_and_recreate_entity() {
    let mut engine = make_engine();

    // Create and delete topic
    engine.apply_command(
        &MqCommand::create_topic("ephemeral", RetentionPolicy::default(), 0),
        1,
        1000,
        None,
    );
    engine.apply_command(&MqCommand::delete_topic(1), 2, 1001, None);

    // Recreate with same name
    let resp = engine.apply_command(
        &MqCommand::create_topic("ephemeral", RetentionPolicy::default(), 0),
        3,
        1002,
        None,
    );
    match resp {
        MqResponse::EntityCreated { id } => assert_eq!(id, 2), // new ID
        other => panic!("expected EntityCreated, got {:?}", other),
    }

    // New entity works
    let resp = engine.apply_command(
        &MqCommand::publish(2, &[make_flat_msg(b"new-msg")]),
        4,
        1003,
        None,
    );
    assert!(matches!(resp, MqResponse::Published { .. }));

    // Old ID doesn't work
    let resp = engine.apply_command(
        &MqCommand::publish(1, &[make_flat_msg(b"x")]),
        5,
        1004,
        None,
    );
    assert!(matches!(resp, MqResponse::Error(_)));
}

// =============================================================================
// Batch command tests
// =============================================================================

#[test]
fn test_batch_mixed_creates_and_publishes() {
    let mut engine = make_engine();

    let batch = MqCommand::batch(&[
        MqCommand::create_topic("t1", RetentionPolicy::default(), 0),
        MqCommand::create_topic("t2", RetentionPolicy::default(), 0),
        MqCommand::publish(1, &[make_flat_msg(b"hello")]),
    ]);

    let resp = engine.apply_command(&batch, 1, 1000, None);
    match resp {
        MqResponse::BatchResponse(resps) => {
            assert_eq!(resps.len(), 3);
            assert!(
                matches!(resps[0], MqResponse::EntityCreated { id: 1 }),
                "First should be EntityCreated(1), got {:?}",
                resps[0]
            );
            assert!(
                matches!(resps[1], MqResponse::EntityCreated { id: 2 }),
                "Second should be EntityCreated(2), got {:?}",
                resps[1]
            );
            assert!(
                matches!(resps[2], MqResponse::Published { .. }),
                "Third should be Published, got {:?}",
                resps[2]
            );
        }
        other => panic!("expected BatchResponse, got {:?}", other),
    }

    // Verify both topics exist
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 2);
}

#[test]
fn test_batch_partial_errors() {
    let mut engine = make_engine();

    // First command will fail (nonexistent topic), second should still succeed
    let batch = MqCommand::batch(&[
        MqCommand::publish(999, &[make_flat_msg(b"fail")]),
        MqCommand::create_topic("ok", RetentionPolicy::default(), 0),
    ]);

    let resp = engine.apply_command(&batch, 1, 1000, None);
    match resp {
        MqResponse::BatchResponse(resps) => {
            assert_eq!(resps.len(), 2);
            assert!(
                matches!(resps[0], MqResponse::Error(_)),
                "First should be Error, got {:?}",
                resps[0]
            );
            assert!(
                matches!(resps[1], MqResponse::EntityCreated { .. }),
                "Second should be EntityCreated, got {:?}",
                resps[1]
            );
        }
        other => panic!("expected BatchResponse, got {:?}", other),
    }

    // Topic was created despite earlier error (no rollback)
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 1);
}

#[test]
fn test_batch_empty() {
    let mut engine = make_engine();

    let resp = engine.apply_command(&MqCommand::batch(&[]), 1, 1000, None);
    match resp {
        MqResponse::BatchResponse(resps) => {
            assert_eq!(resps.len(), 0);
        }
        other => panic!("expected BatchResponse, got {:?}", other),
    }
}

// =============================================================================
// Consumer group lifecycle
// =============================================================================

#[test]
fn test_consumer_group_create_and_delete() {
    let mut engine = make_engine();

    // Create
    let group_id = match engine.apply_command(
        &MqCommand::create_consumer_group("my-group", 1),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {other}"),
    };
    assert!(group_id > 0);

    // Duplicate create returns AlreadyExists
    match engine.apply_command(
        &MqCommand::create_consumer_group("my-group", 1),
        2,
        1001,
        None,
    ) {
        MqResponse::Error(MqError::AlreadyExists { .. }) => {}
        other => panic!("expected AlreadyExists, got {other}"),
    }

    // Delete
    match engine.apply_command(&MqCommand::delete_consumer_group(group_id), 3, 1002, None) {
        MqResponse::Ok => {}
        other => panic!("expected Ok, got {other}"),
    }

    // Can recreate after delete
    match engine.apply_command(
        &MqCommand::create_consumer_group("my-group", 0),
        4,
        1003,
        None,
    ) {
        MqResponse::EntityCreated { .. } => {}
        other => panic!("expected EntityCreated, got {other}"),
    }
}

#[test]
fn test_consumer_group_single_member_join_sync() {
    let mut engine = make_engine();

    // Create group
    let group_id =
        match engine.apply_command(&MqCommand::create_consumer_group("g1", 1), 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {other}"),
        };

    // Join with empty member_id -> auto-assigned
    let (member_id, generation) = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "test-client",
            30000,
            60000,
            "consumer",
            &[("range", b"\x00\x01")],
        ),
        2,
        1001,
        None,
    ) {
        MqResponse::GroupJoined {
            member_id,
            generation,
            is_leader,
            phase_complete,
            ..
        } => {
            assert!(phase_complete, "single member should complete immediately");
            assert!(is_leader, "sole member should be leader");
            assert_eq!(generation, 1);
            assert!(!member_id.is_empty());
            (member_id, generation)
        }
        other => panic!("expected GroupJoined, got {other}"),
    };

    // Sync as leader with assignments
    match engine.apply_command(
        &MqCommand::sync_consumer_group(
            group_id,
            generation,
            &member_id,
            &[(&member_id, b"assignment-data")],
        ),
        3,
        1002,
        None,
    ) {
        MqResponse::GroupSynced {
            assignment,
            phase_complete,
        } => {
            assert!(phase_complete);
            assert_eq!(assignment, b"assignment-data");
        }
        other => panic!("expected GroupSynced, got {other}"),
    }

    // Heartbeat
    match engine.apply_command(
        &MqCommand::heartbeat_consumer_group(group_id, &member_id, generation),
        4,
        1003,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("expected Ok, got {other}"),
    }
}

#[test]
fn test_consumer_group_two_members_join() {
    let mut engine = make_engine();

    let group_id =
        match engine.apply_command(&MqCommand::create_consumer_group("g2", 1), 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {other}"),
        };

    // First member joins -> phase NOT complete (waiting for more)
    let m1_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client-1",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        2,
        1001,
        None,
    ) {
        MqResponse::GroupJoined {
            member_id,
            phase_complete,
            ..
        } => {
            // With only one member, phase completes immediately
            assert!(phase_complete);
            member_id
        }
        other => panic!("expected GroupJoined, got {other}"),
    };

    // Complete sync for gen 1
    engine.apply_command(
        &MqCommand::sync_consumer_group(group_id, 1, &m1_id, &[(&m1_id, b"a1")]),
        3,
        1002,
        None,
    );

    // Second member joins -> triggers rebalance
    let m2_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client-2",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        4,
        1003,
        None,
    ) {
        MqResponse::GroupJoined {
            member_id,
            phase_complete,
            ..
        } => {
            // Phase NOT complete -- waiting for m1 to re-join
            assert!(!phase_complete);
            member_id
        }
        other => panic!("expected GroupJoined, got {other}"),
    };

    // m1 re-joins -> now all members joined -> phase completes
    match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            &m1_id,
            "client-1",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        5,
        1004,
        None,
    ) {
        MqResponse::GroupJoined {
            generation,
            phase_complete,
            ..
        } => {
            assert!(phase_complete);
            assert_eq!(generation, 2);
        }
        other => panic!("expected GroupJoined, got {other}"),
    }

    // Both sync in gen 2
    engine.apply_command(
        &MqCommand::sync_consumer_group(
            group_id,
            2,
            &m1_id,
            &[(&m1_id, b"a1-v2"), (&m2_id, b"a2-v2")],
        ),
        6,
        1005,
        None,
    );
    match engine.apply_command(
        &MqCommand::sync_consumer_group(group_id, 2, &m2_id, &[]),
        7,
        1006,
        None,
    ) {
        MqResponse::GroupSynced {
            assignment,
            phase_complete,
        } => {
            assert!(phase_complete);
            assert_eq!(assignment, b"a2-v2");
        }
        other => panic!("expected GroupSynced, got {other}"),
    }
}

#[test]
fn test_consumer_group_leave() {
    let mut engine = make_engine();

    let group_id =
        match engine.apply_command(&MqCommand::create_consumer_group("g3", 1), 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {other}"),
        };

    // Join
    let member_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        2,
        1001,
        None,
    ) {
        MqResponse::GroupJoined { member_id, .. } => member_id,
        other => panic!("expected GroupJoined, got {other}"),
    };

    // Leave
    match engine.apply_command(
        &MqCommand::leave_consumer_group(group_id, &member_id),
        3,
        1002,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("expected Ok, got {other}"),
    }

    // Leave again (idempotent)
    match engine.apply_command(
        &MqCommand::leave_consumer_group(group_id, &member_id),
        4,
        1003,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("expected Ok, got {other}"),
    }
}

#[test]
fn test_consumer_group_heartbeat_fencing() {
    let mut engine = make_engine();

    let group_id =
        match engine.apply_command(&MqCommand::create_consumer_group("g4", 1), 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {other}"),
        };

    let member_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        2,
        1001,
        None,
    ) {
        MqResponse::GroupJoined { member_id, .. } => member_id,
        other => panic!("expected GroupJoined, got {other}"),
    };

    // Correct generation
    match engine.apply_command(
        &MqCommand::heartbeat_consumer_group(group_id, &member_id, 1),
        3,
        1002,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("expected Ok, got {other}"),
    }

    // Wrong generation
    match engine.apply_command(
        &MqCommand::heartbeat_consumer_group(group_id, &member_id, 99),
        4,
        1003,
        None,
    ) {
        MqResponse::Error(MqError::IllegalGeneration) => {}
        other => panic!("expected IllegalGeneration, got {other}"),
    }

    // Unknown member
    match engine.apply_command(
        &MqCommand::heartbeat_consumer_group(group_id, "unknown-member", 1),
        5,
        1004,
        None,
    ) {
        MqResponse::Error(MqError::UnknownMemberId) => {}
        other => panic!("expected UnknownMemberId, got {other}"),
    }

    // Nonexistent group
    match engine.apply_command(
        &MqCommand::heartbeat_consumer_group(99999, &member_id, 1),
        6,
        1005,
        None,
    ) {
        MqResponse::Error(MqError::NotFound { .. }) => {}
        other => panic!("expected NotFound, got {other}"),
    }
}

#[test]
fn test_consumer_group_offset_commit_and_fetch() {
    let mut engine = make_engine();

    // Create topic + group
    let topic_id = match engine.apply_command(
        &MqCommand::create_topic("events", RetentionPolicy::default(), 0),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {other}"),
    };

    let group_id = match engine.apply_command(
        &MqCommand::create_consumer_group("cg-offsets", 1),
        2,
        1001,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {other}"),
    };

    // Join + sync to get a generation
    let member_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        3,
        1002,
        None,
    ) {
        MqResponse::GroupJoined { member_id, .. } => member_id,
        other => panic!("expected GroupJoined, got {other}"),
    };

    engine.apply_command(
        &MqCommand::sync_consumer_group(group_id, 1, &member_id, &[(&member_id, b"a")]),
        4,
        1003,
        None,
    );

    // Commit offset
    match engine.apply_command(
        &MqCommand::commit_group_offset(group_id, 1, topic_id, 0, 42, None, 2000),
        5,
        2000,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("expected Ok, got {other}"),
    }

    // Read offset via metadata
    let offset = engine
        .metadata()
        .get_consumer_group(group_id)
        .and_then(|g| g.get_offset(topic_id, 0));
    assert_eq!(offset, Some(42));

    // Commit higher offset
    engine.apply_command(
        &MqCommand::commit_group_offset(group_id, 1, topic_id, 0, 100, None, 3000),
        6,
        3000,
        None,
    );
    let offset = engine
        .metadata()
        .get_consumer_group(group_id)
        .and_then(|g| g.get_offset(topic_id, 0));
    assert_eq!(offset, Some(100));

    // Wrong generation should fail
    match engine.apply_command(
        &MqCommand::commit_group_offset(group_id, 99, topic_id, 0, 200, None, 4000),
        7,
        4000,
        None,
    ) {
        MqResponse::Error(MqError::IllegalGeneration) => {}
        other => panic!("expected IllegalGeneration, got {other}"),
    }
}

#[test]
fn test_consumer_group_session_expiry() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_consumer_group("g-expire", 1),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {other}"),
    };

    // Join with 5s session timeout
    let member_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client",
            5000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        2,
        1000,
        None,
    ) {
        MqResponse::GroupJoined { member_id, .. } => member_id,
        other => panic!("expected GroupJoined, got {other}"),
    };

    // Member heartbeat at t=1000, session_timeout=5000
    // Expire at t=5999 -> no expiry yet
    engine.apply_command(&MqCommand::expire_group_sessions(5999), 3, 5999, None);
    assert!(
        engine
            .metadata()
            .get_consumer_group(group_id)
            .unwrap()
            .has_member(&member_id)
    );

    // Expire at t=6001 -> should expire
    engine.apply_command(&MqCommand::expire_group_sessions(6001), 4, 6001, None);
    assert!(
        !engine
            .metadata()
            .get_consumer_group(group_id)
            .unwrap()
            .has_member(&member_id)
    );
}

#[test]
fn test_consumer_group_snapshot_restore() {
    let mut engine = make_engine();

    // Create group
    let group_id = match engine.apply_command(
        &MqCommand::create_consumer_group("snap-group", 1),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {other}"),
    };

    // Join member
    let member_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client",
            30000,
            60000,
            "consumer",
            &[("range", b"\x01")],
        ),
        2,
        1001,
        None,
    ) {
        MqResponse::GroupJoined { member_id, .. } => member_id,
        other => panic!("expected GroupJoined, got {other}"),
    };

    // Sync
    engine.apply_command(
        &MqCommand::sync_consumer_group(group_id, 1, &member_id, &[(&member_id, b"asn")]),
        3,
        1002,
        None,
    );

    // Commit offset
    engine.apply_command(
        &MqCommand::commit_group_offset(group_id, 1, 42, 0, 99, None, 1003),
        4,
        1003,
        None,
    );

    // Snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.consumer_groups.len(), 1);
    assert_eq!(snap.consumer_groups[0].meta.name, "snap-group");
    assert_eq!(snap.consumer_groups[0].meta.generation, 1);
    assert_eq!(snap.consumer_groups[0].meta.members.len(), 1);
    assert_eq!(snap.consumer_groups[0].offsets.len(), 1);
    assert_eq!(snap.consumer_groups[0].offsets[0].committed_offset, 99);

    // Serialize/deserialize roundtrip
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    // Restore into fresh engine
    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    // Verify via metadata
    {
        let group = engine2.metadata().get_consumer_group(group_id).unwrap();
        assert_eq!(group.generation(), 1);
        assert!(group.has_member(&member_id));
        assert_eq!(group.get_offset(42, 0), Some(99));
        assert_eq!(
            group.get_member_assignment(&member_id).unwrap().as_ref(),
            b"asn"
        );
    }

    // Verify via snapshot
    let snap2 = engine2.snapshot();
    assert_eq!(snap2.consumer_groups.len(), 1);
    assert_eq!(snap2.consumer_groups[0].meta.generation, 1);

    // Engine continues to work
    match engine2.apply_command(
        &MqCommand::heartbeat_consumer_group(group_id, &member_id, 1),
        5,
        2000,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("expected Ok after restore, got {other}"),
    }
}

#[test]
fn test_consumer_group_offset_expiry() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_consumer_group("g-offset-exp", 1),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {other}"),
    };

    // Join + sync
    let member_id = match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            "",
            "client",
            5000, // 5s session timeout
            60000,
            "consumer",
            &[("range", b"")],
        ),
        2,
        1001,
        None,
    ) {
        MqResponse::GroupJoined { member_id, .. } => member_id,
        other => panic!("expected GroupJoined, got {other}"),
    };

    engine.apply_command(
        &MqCommand::sync_consumer_group(group_id, 1, &member_id, &[(&member_id, b"a")]),
        3,
        1002,
        None,
    );

    // Group has member
    assert!(
        engine
            .metadata()
            .get_consumer_group(group_id)
            .unwrap()
            .has_member(&member_id)
    );

    // Expire before session timeout -> member still present
    engine.apply_command(&MqCommand::expire_group_sessions(5999), 4, 5999, None);
    assert!(
        engine
            .metadata()
            .get_consumer_group(group_id)
            .unwrap()
            .has_member(&member_id)
    );

    // Expire after session timeout -> member removed, group becomes empty
    engine.apply_command(&MqCommand::expire_group_sessions(6002), 5, 6002, None);
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    assert!(!group.has_member(&member_id));
    assert_eq!(group.member_count(), 0);
    assert_eq!(group.phase(), bisque_mq::consumer_group::GroupPhase::Empty);
}

// =============================================================================
// Batch 1: Engine Apply Handler Edge Cases
// =============================================================================

/// Helper: create a group and return its id.
fn create_group(engine: &mut MqEngine, name: &str, log_idx: u64, ts: u64) -> u64 {
    match engine.apply_command(
        &MqCommand::create_consumer_group(name, 1),
        log_idx,
        ts,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {other}"),
    }
}

/// Helper: join a group, return (member_id, generation, phase_complete).
fn join_group(
    engine: &mut MqEngine,
    group_id: u64,
    member_id: &str,
    log_idx: u64,
    ts: u64,
) -> (String, i32, bool) {
    match engine.apply_command(
        &MqCommand::join_consumer_group(
            group_id,
            member_id,
            "client",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        log_idx,
        ts,
        None,
    ) {
        MqResponse::GroupJoined {
            member_id,
            generation,
            phase_complete,
            ..
        } => (member_id, generation, phase_complete),
        other => panic!("expected GroupJoined, got {other}"),
    }
}

/// Helper: sync a group as leader with assignments.
fn sync_group(
    engine: &mut MqEngine,
    group_id: u64,
    generation: i32,
    member_id: &str,
    assignments: &[(&str, &[u8])],
    log_idx: u64,
    ts: u64,
) {
    match engine.apply_command(
        &MqCommand::sync_consumer_group(group_id, generation, member_id, assignments),
        log_idx,
        ts,
        None,
    ) {
        MqResponse::GroupSynced { .. } => {}
        other => panic!("expected GroupSynced, got {other}"),
    }
}

#[test]
fn test_cg_join_nonexistent_group() {
    let mut engine = make_engine();
    match engine.apply_command(
        &MqCommand::join_consumer_group(
            99999,
            "",
            "c",
            30000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::ConsumerGroup,
            id: 99999,
        }) => {}
        other => panic!("expected NotFound, got {other}"),
    }
}

#[test]
fn test_cg_sync_nonexistent_group() {
    let mut engine = make_engine();
    match engine.apply_command(
        &MqCommand::sync_consumer_group(99999, 1, "m1", &[]),
        1,
        1000,
        None,
    ) {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::ConsumerGroup,
            id: 99999,
        }) => {}
        other => panic!("expected NotFound, got {other}"),
    }
}

#[test]
fn test_cg_sync_wrong_generation() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-sync-g", 1, 1000);
    let (mid, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    assert_eq!(g, 1);

    // Sync with wrong generation
    match engine.apply_command(
        &MqCommand::sync_consumer_group(gid, 999, &mid, &[]),
        3,
        1002,
        None,
    ) {
        MqResponse::Error(MqError::IllegalGeneration) => {}
        other => panic!("expected IllegalGeneration, got {other}"),
    }
}

#[test]
fn test_cg_join_during_completing_rebalance() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-join-cr", 1, 1000);

    // Member A joins alone -> phase completes (CompletingRebalance)
    let (ma, gen1, complete) = join_group(&mut engine, gid, "", 2, 1001);
    assert!(complete);
    assert_eq!(gen1, 1);

    // Sync to go to Stable
    sync_group(&mut engine, gid, gen1, &ma, &[(&ma, b"a1")], 3, 1002);

    // Member B joins -> triggers PreparingRebalance from Stable
    let (_mb, _gen, complete) = join_group(&mut engine, gid, "", 4, 1003);
    assert!(!complete);

    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(
        group.phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );
}

#[test]
fn test_cg_leave_triggers_rebalance_in_stable() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-leave-stable", 1, 1000);

    // Two members join
    let (ma, _, _) = join_group(&mut engine, gid, "", 2, 1001);
    let (_mb, _, _) = join_group(&mut engine, gid, "", 3, 1002);
    // A rejoins to complete
    let (_, g, complete) = join_group(&mut engine, gid, &ma, 4, 1003);
    assert!(complete);

    // Sync both
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 5, 1004);

    // Verify Stable
    assert_eq!(
        engine.metadata().get_consumer_group(gid).unwrap().phase(),
        bisque_mq::consumer_group::GroupPhase::Stable
    );

    // Leave member A -> should trigger PreparingRebalance
    engine.apply_command(&MqCommand::leave_consumer_group(gid, &ma), 6, 1005, None);
    assert_eq!(
        engine.metadata().get_consumer_group(gid).unwrap().phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );
}

#[test]
fn test_cg_leave_during_rebalance() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-leave-rebal", 1, 1000);

    // Two members join, A completes
    let (ma, _, _) = join_group(&mut engine, gid, "", 2, 1001);
    let (mb, _, _) = join_group(&mut engine, gid, "", 3, 1002);
    // A rejoins
    join_group(&mut engine, gid, &ma, 4, 1003);

    // Sync to Stable
    let group = engine.metadata().get_consumer_group(gid).unwrap();
    let g = group.generation();
    drop(group);
    sync_group(
        &mut engine,
        gid,
        g,
        &ma,
        &[(&ma, b"a"), (&mb, b"b")],
        5,
        1004,
    );

    // B joins -> triggers PreparingRebalance
    join_group(&mut engine, gid, "", 6, 1005);

    // B leaves during rebalance
    engine.apply_command(&MqCommand::leave_consumer_group(gid, &mb), 7, 1006, None);

    // Should still have members, phase should be determined by remaining count
    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert!(group.member_count() >= 1);
}

#[test]
fn test_cg_leave_last_member() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-leave-last", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    engine.apply_command(&MqCommand::leave_consumer_group(gid, &ma), 4, 1003, None);

    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.phase(), bisque_mq::consumer_group::GroupPhase::Empty);
    assert!(group.leader().is_none());
    assert_eq!(group.member_count(), 0);
}

#[test]
fn test_cg_heartbeat_during_rebalance() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-hb-rebal", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // New member joins -> PreparingRebalance
    join_group(&mut engine, gid, "", 4, 1003);
    assert_eq!(
        engine.metadata().get_consumer_group(gid).unwrap().phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );

    // Heartbeat during rebalance -> RebalanceInProgress
    match engine.apply_command(
        &MqCommand::heartbeat_consumer_group(gid, &ma, g),
        5,
        1004,
        None,
    ) {
        MqResponse::Error(MqError::RebalanceInProgress) => {}
        other => panic!("expected RebalanceInProgress, got {other}"),
    }
}

#[test]
fn test_cg_double_join_same_member() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-double-join", 1, 1000);
    let (ma, gen1, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, gen1, &ma, &[(&ma, b"a")], 3, 1002);

    // Same member joins again
    let (ma2, gen2, complete) = join_group(&mut engine, gid, &ma, 4, 1003);
    assert_eq!(ma2, ma); // Same member_id
    assert!(complete); // Single member -> completes immediately
    assert_eq!(gen2, 2); // Generation bumped

    // Verify only 1 member
    assert_eq!(
        engine
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .member_count(),
        1
    );
}

#[test]
fn test_cg_offset_overwrite() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-off-over", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit offset 10
    engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 42, 0, 10, None, 1003),
        4,
        1003,
        None,
    );
    assert_eq!(
        engine
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .get_offset(42, 0),
        Some(10)
    );

    // Overwrite with offset 20
    engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 42, 0, 20, None, 1004),
        5,
        1004,
        None,
    );
    assert_eq!(
        engine
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .get_offset(42, 0),
        Some(20)
    );
}

#[test]
fn test_cg_offset_multiple_partitions() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-off-multi", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit to different topic/partition combos
    engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 10, 0, 100, None, 1003),
        4,
        1003,
        None,
    );
    engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 10, 1, 200, None, 1004),
        5,
        1004,
        None,
    );
    engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 20, 0, 300, None, 1005),
        6,
        1005,
        None,
    );

    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.get_offset(10, 0), Some(100));
    assert_eq!(group.get_offset(10, 1), Some(200));
    assert_eq!(group.get_offset(20, 0), Some(300));
    assert_eq!(group.get_offset(20, 1), None); // Not committed
}

#[test]
fn test_cg_offset_boundary_zero() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-off-zero", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit offset 0 -- valid boundary
    match engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 42, 0, 0, None, 1003),
        4,
        1003,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("expected Ok, got {other}"),
    }
    assert_eq!(
        engine
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .get_offset(42, 0),
        Some(0)
    );
}

#[test]
fn test_cg_offset_commit_nonexistent_group() {
    let mut engine = make_engine();
    match engine.apply_command(
        &MqCommand::commit_group_offset(99999, 1, 42, 0, 10, None, 1000),
        1,
        1000,
        None,
    ) {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::ConsumerGroup,
            ..
        }) => {}
        other => panic!("expected NotFound, got {other}"),
    }
}

#[test]
fn test_cg_session_expiry_triggers_rebalance() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-exp-rebal", 1, 1000);

    // Two members join with 5s timeout
    let (ma, _, _) = join_group(&mut engine, gid, "", 2, 1001);
    let (mb, _, _) = join_group(&mut engine, gid, "", 3, 1002);
    let (_, g, _) = join_group(&mut engine, gid, &ma, 4, 1003);
    sync_group(
        &mut engine,
        gid,
        g,
        &ma,
        &[(&ma, b"a"), (&mb, b"b")],
        5,
        1004,
    );

    // Heartbeat only member A at t=5000
    engine.apply_command(
        &MqCommand::heartbeat_consumer_group(gid, &ma, g),
        6,
        5000,
        None,
    );

    // Expire at t=31003 -> member B's last heartbeat was at join time 1002, timeout 30000
    // So B expires at 1002 + 30000 = 31002
    engine.apply_command(&MqCommand::expire_group_sessions(31003), 7, 31003, None);

    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.member_count(), 1); // Only A remains
    assert!(group.has_member(&ma));
    assert!(!group.has_member(&mb));
    // Should trigger rebalance
    assert_eq!(
        group.phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );
}

#[test]
fn test_cg_session_expiry_multiple_timeouts() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-exp-multi", 1, 1000);

    // Member A with 10s timeout
    match engine.apply_command(
        &MqCommand::join_consumer_group(
            gid,
            "",
            "c-a",
            10000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        2,
        1000,
        None,
    ) {
        MqResponse::GroupJoined { member_id, .. } => {
            // Member B with 60s timeout
            match engine.apply_command(
                &MqCommand::join_consumer_group(
                    gid,
                    "",
                    "c-b",
                    60000,
                    60000,
                    "consumer",
                    &[("range", b"")],
                ),
                3,
                1000,
                None,
            ) {
                MqResponse::GroupJoined { member_id: mb, .. } => {
                    // A rejoins (preserving 10s timeout) to complete
                    engine.apply_command(
                        &MqCommand::join_consumer_group(
                            gid,
                            &member_id,
                            "c-a",
                            10000,
                            60000,
                            "consumer",
                            &[("range", b"")],
                        ),
                        4,
                        1000,
                        None,
                    );
                    let group = engine.metadata().get_consumer_group(gid).unwrap();
                    let g = group.generation();
                    drop(group);
                    sync_group(
                        &mut engine,
                        gid,
                        g,
                        &member_id,
                        &[(&member_id, b"a"), (&mb, b"b")],
                        5,
                        1001,
                    );

                    // Expire at t=11002 -> A (10s timeout, heartbeat at 1000) expired, B (60s) still alive
                    engine.apply_command(&MqCommand::expire_group_sessions(11002), 6, 11002, None);
                    let group = engine.metadata().get_consumer_group(gid).unwrap();
                    assert_eq!(group.member_count(), 1);
                    assert!(group.has_member(&mb));
                    assert!(!group.has_member(&member_id));
                }
                other => panic!("expected GroupJoined, got {other}"),
            }
        }
        other => panic!("expected GroupJoined, got {other}"),
    }
}

#[test]
fn test_cg_session_expiry_boundary() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-exp-bound", 1, 1000);
    // Join with 10s timeout, heartbeat at t=1000
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1000);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1001);

    // Expire at exact boundary: 1000 + 30000 = 31000 (default session timeout)
    // Member heartbeat was at 1000, timeout is 30000
    // At t=31000: last_heartbeat(1000) + timeout(30000) = 31000, not < 31000 -> NOT expired
    engine.apply_command(&MqCommand::expire_group_sessions(31000), 4, 31000, None);
    assert_eq!(
        engine
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .member_count(),
        1
    );

    // At t=31001: 1000 + 30000 = 31000 < 31001 -> expired
    engine.apply_command(&MqCommand::expire_group_sessions(31001), 5, 31001, None);
    assert_eq!(
        engine
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .member_count(),
        0
    );
}

#[test]
fn test_cg_batch_with_consumer_group_commands() {
    let mut engine = make_engine();

    let create_cmd = MqCommand::create_consumer_group("g-batch", 1);
    let batch = MqCommand::batch(&[create_cmd]);

    match engine.apply_command(&batch, 1, 1000, None) {
        MqResponse::BatchResponse(responses) => {
            assert_eq!(responses.len(), 1);
            match &responses[0] {
                MqResponse::EntityCreated { id } => {
                    assert!(engine.metadata().get_consumer_group(*id).is_some());
                }
                other => panic!("expected EntityCreated, got {other}"),
            }
        }
        other => panic!("expected BatchResponse, got {other}"),
    }
}

#[test]
fn test_cg_batch_mixed_types() {
    let mut engine = make_engine();

    let topic_cmd = MqCommand::create_topic("t-batch", RetentionPolicy::default(), 0);
    let group_cmd = MqCommand::create_consumer_group("g-batch-mix", 1);
    let batch = MqCommand::batch(&[topic_cmd, group_cmd]);

    match engine.apply_command(&batch, 1, 1000, None) {
        MqResponse::BatchResponse(responses) => {
            assert_eq!(responses.len(), 2);
            // Both should be EntityCreated
            for resp in responses.iter() {
                match resp {
                    MqResponse::EntityCreated { .. } => {}
                    other => panic!("expected EntityCreated, got {other}"),
                }
            }
        }
        other => panic!("expected BatchResponse, got {other}"),
    }
}

#[test]
fn test_cg_protocol_selection_tiebreak() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-proto-tie", 1, 1000);

    // Member A: ["range", "roundrobin"]
    match engine.apply_command(
        &MqCommand::join_consumer_group(
            gid,
            "",
            "c-a",
            30000,
            60000,
            "consumer",
            &[("range", b""), ("roundrobin", b"")],
        ),
        2,
        1001,
        None,
    ) {
        MqResponse::GroupJoined { member_id: ma, .. } => {
            // Member B: ["roundrobin", "range"]
            match engine.apply_command(
                &MqCommand::join_consumer_group(
                    gid,
                    "",
                    "c-b",
                    30000,
                    60000,
                    "consumer",
                    &[("roundrobin", b""), ("range", b"")],
                ),
                3,
                1002,
                None,
            ) {
                MqResponse::GroupJoined { .. } => {
                    // A rejoins to complete phase
                    match engine.apply_command(
                        &MqCommand::join_consumer_group(
                            gid,
                            &ma,
                            "c-a",
                            30000,
                            60000,
                            "consumer",
                            &[("range", b""), ("roundrobin", b"")],
                        ),
                        4,
                        1003,
                        None,
                    ) {
                        MqResponse::GroupJoined { protocol_name, .. } => {
                            // Both protocols supported by 2 members -- tiebreak by position
                            assert!(protocol_name == "range" || protocol_name == "roundrobin");
                        }
                        other => panic!("expected GroupJoined, got {other}"),
                    }
                }
                other => panic!("expected GroupJoined, got {other}"),
            }
        }
        other => panic!("expected GroupJoined, got {other}"),
    }
}

#[test]
fn test_cg_three_member_rebalance() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-three", 1, 1000);

    // A joins alone -> g 1
    let (ma, gen1, c) = join_group(&mut engine, gid, "", 2, 1001);
    assert!(c);
    assert_eq!(gen1, 1);
    sync_group(&mut engine, gid, gen1, &ma, &[(&ma, b"a1")], 3, 1002);

    // B joins -> rebalance
    let (mb, _, c) = join_group(&mut engine, gid, "", 4, 1003);
    assert!(!c);

    // A rejoins -> g 2
    let (_, gen2, c) = join_group(&mut engine, gid, &ma, 5, 1004);
    assert!(c);
    assert_eq!(gen2, 2);
    sync_group(
        &mut engine,
        gid,
        gen2,
        &ma,
        &[(&ma, b"a2"), (&mb, b"b2")],
        6,
        1005,
    );

    // C joins -> rebalance
    let (mc, _, c) = join_group(&mut engine, gid, "", 7, 1006);
    assert!(!c);

    // A + B rejoin -> g 3
    join_group(&mut engine, gid, &ma, 8, 1007);
    let (_, gen3, c) = join_group(&mut engine, gid, &mb, 9, 1008);
    assert!(c);
    assert_eq!(gen3, 3);

    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.member_count(), 3);
    assert!(group.has_member(&ma));
    assert!(group.has_member(&mb));
    assert!(group.has_member(&mc));
}

#[test]
fn test_cg_leader_leaves_new_leader_elected() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-leader-leave", 1, 1000);

    // A + B join
    let (ma, _, _) = join_group(&mut engine, gid, "", 2, 1001);
    let (mb, _, _) = join_group(&mut engine, gid, "", 3, 1002);
    let (_, gen1, _) = join_group(&mut engine, gid, &ma, 4, 1003);
    sync_group(
        &mut engine,
        gid,
        gen1,
        &ma,
        &[(&ma, b"a"), (&mb, b"b")],
        5,
        1004,
    );

    let leader1 = engine
        .metadata()
        .get_consumer_group(gid)
        .unwrap()
        .leader()
        .unwrap();

    // Leader leaves
    engine.apply_command(
        &MqCommand::leave_consumer_group(gid, &leader1),
        6,
        1005,
        None,
    );

    // Remaining member rejoins -> new leader elected
    let remaining = if leader1.as_str() == ma { &mb } else { &ma };
    let (_, gen2, c) = join_group(&mut engine, gid, remaining, 7, 1006);
    assert!(c);
    assert_eq!(gen2, gen1 + 1);

    let leader2 = engine
        .metadata()
        .get_consumer_group(gid)
        .unwrap()
        .leader()
        .unwrap();
    assert_eq!(leader2.as_str(), remaining.as_str());
}

#[test]
fn test_cg_rejoin_with_different_protocols() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-proto-change", 1, 1000);

    // Join with "range"
    let (ma, gen1, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, gen1, &ma, &[(&ma, b"a")], 3, 1002);

    // Rejoin with "roundrobin"
    match engine.apply_command(
        &MqCommand::join_consumer_group(
            gid,
            &ma,
            "client",
            30000,
            60000,
            "consumer",
            &[("roundrobin", b"rr")],
        ),
        4,
        1003,
        None,
    ) {
        MqResponse::GroupJoined {
            protocol_name,
            generation,
            ..
        } => {
            assert_eq!(generation, 2);
            assert_eq!(protocol_name, "roundrobin");
        }
        other => panic!("expected GroupJoined, got {other}"),
    }
}

#[test]
fn test_cg_snapshot_mid_rebalance() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-snap-rebal", 1, 1000);

    // Join A, then B joins -> PreparingRebalance
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);
    join_group(&mut engine, gid, "", 4, 1003); // B joins -> rebalance

    assert_eq!(
        engine.metadata().get_consumer_group(gid).unwrap().phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance,
    );

    // Snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.consumer_groups.len(), 1);
    assert_eq!(
        snap.consumer_groups[0].meta.phase,
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );

    // Restore
    let mut engine2 = make_engine();
    engine2.restore(snap);
    let group = engine2.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(
        group.phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );
    assert_eq!(group.member_count(), 2);
}

// =============================================================================
// Batch 6: Snapshot/Restore Edge Cases
// =============================================================================

#[test]
fn test_cg_snapshot_empty_group_with_offsets() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-snap-empty", 1, 1000);

    // Join, commit offset, then leave (group becomes empty but offsets remain)
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);
    engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 42, 0, 100, Some("meta"), 1003),
        4,
        1003,
        None,
    );
    engine.apply_command(&MqCommand::leave_consumer_group(gid, &ma), 5, 1004, None);

    // Verify: empty group with offsets
    {
        let group = engine.metadata().get_consumer_group(gid).unwrap();
        assert_eq!(group.phase(), bisque_mq::consumer_group::GroupPhase::Empty);
        assert_eq!(group.member_count(), 0);
        assert_eq!(group.get_offset(42, 0), Some(100));
    }

    // Snapshot + restore
    let snap = engine.snapshot();
    assert_eq!(snap.consumer_groups.len(), 1);
    assert_eq!(snap.consumer_groups[0].offsets.len(), 1);
    assert_eq!(snap.consumer_groups[0].meta.members.len(), 0);

    let mut engine2 = make_engine();
    engine2.restore(snap);
    let group = engine2.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.phase(), bisque_mq::consumer_group::GroupPhase::Empty);
    assert_eq!(group.get_offset(42, 0), Some(100));
}

#[test]
fn test_cg_snapshot_multiple_groups() {
    let mut engine = make_engine();

    // Group A: Stable with members
    let ga = create_group(&mut engine, "g-snap-a", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, ga, "", 2, 1001);
    sync_group(&mut engine, ga, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Group B: Empty (just created)
    let gb = create_group(&mut engine, "g-snap-b", 4, 1003);

    // Group C: PreparingRebalance
    let gc = create_group(&mut engine, "g-snap-c", 5, 1004);
    let (mc, g, _) = join_group(&mut engine, gc, "", 6, 1005);
    sync_group(&mut engine, gc, g, &mc, &[(&mc, b"c")], 7, 1006);
    join_group(&mut engine, gc, "", 8, 1007); // Triggers rebalance

    // Snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.consumer_groups.len(), 3);

    // Restore
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Verify each group's state
    let ga_state = engine2.metadata().get_consumer_group(ga).unwrap();
    assert_eq!(
        ga_state.phase(),
        bisque_mq::consumer_group::GroupPhase::Stable
    );
    assert_eq!(ga_state.member_count(), 1);

    let gb_state = engine2.metadata().get_consumer_group(gb).unwrap();
    assert_eq!(
        gb_state.phase(),
        bisque_mq::consumer_group::GroupPhase::Empty
    );
    assert_eq!(gb_state.member_count(), 0);

    let gc_state = engine2.metadata().get_consumer_group(gc).unwrap();
    assert_eq!(
        gc_state.phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );
    assert_eq!(gc_state.member_count(), 2);
}

#[test]
fn test_cg_snapshot_dead_phase() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-snap-dead", 1, 1000);

    // Manually set phase to Dead via internal access
    // Since we can't directly set Dead through commands, we verify through snapshot data
    let snap = engine.snapshot();
    let mut modified_snap = snap;
    modified_snap.consumer_groups[0].meta.phase = bisque_mq::consumer_group::GroupPhase::Dead;

    let mut engine2 = make_engine();
    engine2.restore(modified_snap);

    let group = engine2.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.phase(), bisque_mq::consumer_group::GroupPhase::Dead);
}

#[test]
fn test_cg_snapshot_preserves_generation() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-snap-gen", 1, 1000);

    // Multiple rebalances to bump generation
    let (ma, _, _) = join_group(&mut engine, gid, "", 2, 1001); // gen 1
    join_group(&mut engine, gid, "", 3, 1002); // B joins
    let (_, g2, _) = join_group(&mut engine, gid, &ma, 4, 1003); // gen 2
    assert_eq!(g2, 2);

    let pre_gen = engine
        .metadata()
        .get_consumer_group(gid)
        .unwrap()
        .generation();

    // Snapshot + restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    let post_gen = engine2
        .metadata()
        .get_consumer_group(gid)
        .unwrap()
        .generation();
    assert_eq!(post_gen, pre_gen);
}

#[test]
fn test_cg_snapshot_operations_continue() {
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-snap-ops", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit an offset
    engine.apply_command(
        &MqCommand::commit_group_offset(gid, g, 42, 0, 50, None, 1003),
        4,
        1003,
        None,
    );

    // Snapshot + restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // All operations should continue working:

    // 1. Heartbeat
    match engine2.apply_command(
        &MqCommand::heartbeat_consumer_group(gid, &ma, g),
        10,
        2000,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("heartbeat after restore: {other}"),
    }

    // 2. Offset commit
    match engine2.apply_command(
        &MqCommand::commit_group_offset(gid, g, 42, 1, 75, None, 2001),
        11,
        2001,
        None,
    ) {
        MqResponse::Ok => {}
        other => panic!("offset commit after restore: {other}"),
    }
    assert_eq!(
        engine2
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .get_offset(42, 1),
        Some(75)
    );

    // 3. New member join -> rebalance
    let (mb, _, complete) = join_group(&mut engine2, gid, "", 12, 2002);
    assert!(!complete);

    // 4. Original member rejoins -> completes
    let (_, g2, complete) = join_group(&mut engine2, gid, &ma, 13, 2003);
    assert!(complete);
    assert!(g2 > g);

    // 5. Sync
    sync_group(
        &mut engine2,
        gid,
        g2,
        &ma,
        &[(&ma, b"a2"), (&mb, b"b2")],
        14,
        2004,
    );

    let group = engine2.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.phase(), bisque_mq::consumer_group::GroupPhase::Stable);
    assert_eq!(group.member_count(), 2);
}

// =============================================================================
// Ack Variant Integration Tests (via engine apply_command)
// =============================================================================

#[test]
fn test_ack_queue_create_publish_deliver_ack() {
    let mut engine = make_engine();

    // Create an ack-variant queue (auto-creates source topic)
    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "test-queue",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Find the source topic
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    assert!(source_topic_id > 0);
    drop(group);

    // Publish messages to source topic → auto-enqueues into ack group
    let msgs: Vec<Bytes> = (0..3)
        .map(|i| make_flat_msg(format!("msg-{i}").as_bytes()))
        .collect();
    let resp = engine.apply_command(&MqCommand::publish(source_topic_id, &msgs), 2, 1001, None);
    assert!(matches!(resp, MqResponse::Published { count: 3, .. }));

    // Verify ack state has pending messages
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 3);
    drop(group);

    // Create session for consumer
    engine.apply_command(
        &MqCommand::create_session(100, "consumer-1", 30000, 60000),
        3,
        1002,
        None,
    );

    // Deliver
    let resp = engine.apply_command(&MqCommand::group_deliver(group_id, 100, 10), 4, 1003, None);
    let delivered_ids = match resp {
        MqResponse::Messages { messages } => {
            assert_eq!(messages.len(), 3);
            messages.iter().map(|m| m.message_id).collect::<Vec<_>>()
        }
        other => panic!("expected Messages, got {:?}", other),
    };

    // Verify in-flight
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 0);
    assert_eq!(ack.in_flight_count(), 3);
    drop(group);

    // Ack all
    let resp = engine.apply_command(
        &MqCommand::group_ack(group_id, &delivered_ids, None),
        5,
        1004,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Verify empty
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 0);
    assert_eq!(ack.in_flight_count(), 0);
}

#[test]
fn test_ack_nack_returns_to_pending() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "nack-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    // Publish
    engine.apply_command(
        &MqCommand::publish(source_topic_id, &[make_flat_msg(b"msg")]),
        2,
        1001,
        None,
    );

    // Deliver
    let resp = engine.apply_command(&MqCommand::group_deliver(group_id, 100, 1), 3, 1002, None);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        other => panic!("expected Messages, got {:?}", other),
    };

    // Nack
    engine.apply_command(&MqCommand::group_nack(group_id, &[msg_id]), 4, 1003, None);

    // Verify returned to pending
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 1);
    assert_eq!(ack.in_flight_count(), 0);
}

#[test]
fn test_ack_release_returns_without_attempt_increment() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "release-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    engine.apply_command(
        &MqCommand::publish(source_topic_id, &[make_flat_msg(b"msg")]),
        2,
        1001,
        None,
    );

    let resp = engine.apply_command(&MqCommand::group_deliver(group_id, 100, 1), 3, 1002, None);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        other => panic!("expected Messages, got {:?}", other),
    };

    // Release
    engine.apply_command(
        &MqCommand::group_release(group_id, &[msg_id]),
        4,
        1003,
        None,
    );

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 1);
    assert_eq!(ack.in_flight_count(), 0);
    // Attempt count should be back to 0 (release decrements)
    let msgs_guard = ack.messages.pin();
    let meta = msgs_guard.get(&msg_id).unwrap();
    assert_eq!(meta.attempts, 0);
}

#[test]
fn test_ack_timeout_and_dead_letter() {
    let mut engine = make_engine();

    let mut config = AckVariantConfig::default();
    config.max_retries = 1;
    config.dead_letter_topic = Some("dlq-topic".to_string());

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "timeout-q",
            config,
            RetentionPolicy::default(),
            None,
            true,
            Some("dlq-topic"),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    engine.apply_command(
        &MqCommand::publish(source_topic_id, &[make_flat_msg(b"dlq-msg")]),
        2,
        1001,
        None,
    );

    // Deliver (attempts = 1 after deliver)
    let resp = engine.apply_command(&MqCommand::group_deliver(group_id, 100, 1), 3, 1002, None);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        other => panic!("expected Messages, got {:?}", other),
    };

    // Timeout with max_retries=1 → should dead letter
    let resp = engine.apply_command(
        &MqCommand::group_timeout_expired(group_id, &[msg_id]),
        4,
        1003,
        None,
    );
    match resp {
        MqResponse::DeadLettered {
            dead_letter_ids, ..
        } => {
            assert_eq!(dead_letter_ids.len(), 1);
        }
        other => panic!("expected DeadLettered, got {:?}", other),
    }
}

#[test]
fn test_ack_extend_visibility_via_engine() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "vis-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    engine.apply_command(
        &MqCommand::publish(source_topic_id, &[make_flat_msg(b"msg")]),
        2,
        1001,
        None,
    );

    let resp = engine.apply_command(&MqCommand::group_deliver(group_id, 100, 1), 3, 1002, None);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        other => panic!("expected Messages, got {:?}", other),
    };

    // Extend visibility
    let resp = engine.apply_command(
        &MqCommand::group_extend_visibility(group_id, &[msg_id], 60_000),
        4,
        1003,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Verify deadline extended
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    let msgs_guard = ack.messages.pin();
    let meta = msgs_guard.get(&msg_id).unwrap();
    // Original deadline was 1002 + 30000 = 31002, extended by 60000 = 91002
    assert!(meta.visibility_deadline.unwrap() > 31002);
}

#[test]
fn test_ack_purge_via_engine() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "purge-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    let msgs: Vec<Bytes> = (0..5).map(|_| make_flat_msg(b"x")).collect();
    engine.apply_command(&MqCommand::publish(source_topic_id, &msgs), 2, 1001, None);

    // Verify pending
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    assert_eq!(group.ack_state().unwrap().pending_count(), 5);
    drop(group);

    // Purge
    let resp = engine.apply_command(&MqCommand::group_purge(group_id), 3, 1002, None);
    assert!(matches!(resp, MqResponse::Ok));

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    assert_eq!(group.ack_state().unwrap().pending_count(), 0);
}

#[test]
fn test_ack_get_attributes() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "attrs-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    engine.apply_command(
        &MqCommand::publish(source_topic_id, &[make_flat_msg(b"a"), make_flat_msg(b"b")]),
        2,
        1001,
        None,
    );

    engine.apply_command(&MqCommand::group_deliver(group_id, 100, 1), 3, 1002, None);

    let resp = engine.apply_command(&MqCommand::group_get_attributes(group_id), 4, 1003, None);
    match resp {
        MqResponse::Stats(EntityStats::ConsumerGroup {
            pending_count,
            in_flight_count,
            variant,
            ..
        }) => {
            assert_eq!(pending_count, 1);
            assert_eq!(in_flight_count, 1);
            assert_eq!(variant, GroupVariant::Ack);
        }
        other => panic!("expected Stats(ConsumerGroup), got {:?}", other),
    }
}

// =============================================================================
// Actor Variant Integration Tests (via engine apply_command)
// =============================================================================

#[test]
fn test_actor_group_assign_deliver_ack() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "test-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Directly send a message to the actor (like engine test does)
    let actor_id = Bytes::from_static(b"actor-1");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 1000, None, 50)
            .unwrap();
    }

    // Assign actor to consumer via engine command
    let resp = engine.apply_command(
        &MqCommand::group_assign_actors(group_id, 42, &[actor_id.clone()]),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Deliver via engine command
    let resp = engine.apply_command(
        &MqCommand::group_deliver_actor(group_id, 42, &[actor_id.clone()]),
        3,
        1002,
        None,
    );
    match resp {
        MqResponse::Messages { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].message_id, 100);
        }
        other => panic!("expected Messages, got {:?}", other),
    }

    // Ack via engine command
    let resp = engine.apply_command(
        &MqCommand::group_ack_actor(group_id, b"actor-1", 100, None),
        4,
        1003,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Verify actor state
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    let actors_guard = actor_state.actors.pin();
    let actor = actors_guard.get(&actor_id).unwrap();
    assert_eq!(actor.in_flight_index(), None);
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn test_actor_nack_returns_to_mailbox() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "nack-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let actor_id = Bytes::from_static(b"actor-1");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 1000, None, 50)
            .unwrap();
    }

    engine.apply_command(
        &MqCommand::group_assign_actors(group_id, 42, &[actor_id.clone()]),
        2,
        1001,
        None,
    );

    let resp = engine.apply_command(
        &MqCommand::group_deliver_actor(group_id, 42, &[actor_id.clone()]),
        3,
        1002,
        None,
    );
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        other => panic!("expected Messages, got {:?}", other),
    };

    // Nack
    let resp = engine.apply_command(
        &MqCommand::group_nack_actor(group_id, b"actor-1", msg_id),
        4,
        1003,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Verify returned to mailbox
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    let actors_guard = actor_state.actors.pin();
    let actor = actors_guard.get(&actor_id).unwrap();
    assert_eq!(actor.in_flight_index(), None);
    assert_eq!(actor.pending_count(), 1);
}

#[test]
fn test_actor_release_unassigns() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "release-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let actor_id = Bytes::from_static(b"actor-1");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 1000, None, 50)
            .unwrap();
    }

    engine.apply_command(
        &MqCommand::group_assign_actors(group_id, 42, &[actor_id.clone()]),
        2,
        1001,
        None,
    );

    engine.apply_command(
        &MqCommand::group_deliver_actor(group_id, 42, &[actor_id.clone()]),
        3,
        1002,
        None,
    );

    // Release consumer → actor should be unassigned, in-flight returned to pending
    let resp = engine.apply_command(
        &MqCommand::group_release_actors(group_id, 42),
        4,
        1003,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    let actors_guard = actor_state.actors.pin();
    let actor = actors_guard.get(&actor_id).unwrap();
    assert_eq!(actor.assigned_consumer_id(), None);
    assert_eq!(actor.in_flight_index(), None);
    assert_eq!(actor.pending_count(), 1);
}

#[test]
fn test_actor_evict_idle_via_engine() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "evict-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let actor_id = Bytes::from_static(b"idle-actor");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 500, None, 50)
            .unwrap();
        // Assign, deliver, ack to empty the mailbox
        actor_state.apply_assign(42, &[actor_id.clone()]);
        actor_state.apply_deliver(&actor_id, 42, 50);
        actor_state.apply_ack(&actor_id, 100, 50);
    }

    // Evict actors idle before timestamp 1000
    let resp = engine.apply_command(&MqCommand::group_evict_idle(group_id, 1000), 2, 1001, None);
    assert!(matches!(resp, MqResponse::Ok));

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    assert_eq!(actor_state.active_count(), 0);
}

#[test]
fn test_actor_get_attributes() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_actor_group(
            "attr-actors",
            ActorVariantConfig::default(),
            RetentionPolicy::default(),
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let actor_id = Bytes::from_static(b"actor-1");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 1000, None, 50)
            .unwrap();
    }

    let resp = engine.apply_command(&MqCommand::group_get_attributes(group_id), 2, 1001, None);
    match resp {
        MqResponse::Stats(EntityStats::ConsumerGroup {
            variant,
            active_actor_count,
            ..
        }) => {
            assert_eq!(variant, GroupVariant::Actor);
            assert_eq!(active_actor_count, 1);
        }
        other => panic!("expected Stats(ConsumerGroup), got {:?}", other),
    }
}

// =============================================================================
// Ack Variant Snapshot/Restore Integration
// =============================================================================

#[test]
fn test_ack_snapshot_restore_via_engine() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "snap-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    // Publish and deliver some messages
    let msgs: Vec<Bytes> = (0..5).map(|_| make_flat_msg(b"snap-data")).collect();
    engine.apply_command(&MqCommand::publish(source_topic_id, &msgs), 2, 1001, None);
    engine.apply_command(&MqCommand::group_deliver(group_id, 100, 2), 3, 1002, None);

    // Take snapshot
    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    // Restore
    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    // Verify ack state restored
    let group = engine2.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 3);
    assert_eq!(ack.in_flight_count(), 2);
    drop(group);

    // Continue delivering
    let resp = engine2.apply_command(&MqCommand::group_deliver(group_id, 100, 10), 10, 2000, None);
    match resp {
        MqResponse::Messages { messages } => {
            assert_eq!(messages.len(), 3);
        }
        other => panic!("expected Messages, got {:?}", other),
    }
}

// =============================================================================
// Delete group cleans up
// =============================================================================

#[test]
fn test_delete_consumer_group_cleans_up() {
    let mut engine = make_engine();

    let group_id = match engine.apply_command(
        &MqCommand::create_queue(
            "del-q",
            AckVariantConfig::default(),
            RetentionPolicy::default(),
            None,
            false,
            None,
            false,
            None,
        ),
        1,
        1000,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Delete
    let resp = engine.apply_command(&MqCommand::delete_consumer_group(group_id), 2, 1001, None);
    assert!(matches!(resp, MqResponse::Ok));

    // Verify deleted
    assert!(engine.metadata().get_consumer_group(group_id).is_none());

    // Operations on deleted group fail
    let resp = engine.apply_command(&MqCommand::group_deliver(group_id, 100, 1), 3, 1002, None);
    assert!(matches!(resp, MqResponse::Error(_)));
}
