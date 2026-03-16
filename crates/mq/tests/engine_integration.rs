//! Integration tests for the MQ engine.
//!
//! These tests exercise end-to-end workflows across multiple entity types,
//! snapshot/restore cycles, and complex multi-step operations.
//!
//! Since engine fields are `pub(crate)`, integration tests verify state via
//! `engine.snapshot()` and `engine.apply_command(, None)` responses.

use bytes::{Bytes, BytesMut};

use bisque_mq::async_apply::ResponseEntry;
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

// Test-only helper: call apply_command and return ResponseEntry
fn apply(engine: &MqEngine, cmd: &MqCommand, log_index: u64, current_time: u64) -> ResponseEntry {
    let mut _buf = BytesMut::new();
    engine.apply_command(cmd, &mut _buf, log_index, current_time, None);
    ResponseEntry::split_from(&mut _buf)
}

// =============================================================================
// End-to-end topic workflow
// =============================================================================

#[test]
fn test_topic_publish_consume_commit_purge() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    // Create topic
    MqCommand::write_create_topic(
        &mut buf,
        "orders",
        &RetentionPolicy {
            max_age_secs: Some(3600),
            ..Default::default()
        },
        0,
    );
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(resp.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let topic_id = resp.entity_id();

    // Create session instead of register_consumer
    MqCommand::write_create_session(&mut buf, 100, "order-processor", 30000, 60000);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    // Publish batch
    let messages: Vec<Bytes> = (0..10)
        .map(|i| make_flat_msg_with_key(format!("order-{}", i).as_bytes(), b"order-data"))
        .collect();
    MqCommand::write_publish_bytes(&mut buf, topic_id, &messages);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 3, 1002);
    assert_eq!(resp.tag(), ResponseEntry::TAG_PUBLISHED);
    assert_eq!(resp.published_count(), 10);

    // Commit offset
    MqCommand::write_commit_offset(&mut buf, topic_id, 100, 3);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 4, 1003);
    assert!(resp.is_ok());

    // Purge old messages
    MqCommand::write_purge_topic(&mut buf, topic_id, 3);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 5, 1004);
    assert!(resp.is_ok());

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
    let mut buf = BytesMut::new();

    // Build diverse state
    MqCommand::write_create_topic(&mut buf, "events", &RetentionPolicy::default(), 0);
    apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    MqCommand::write_publish_bytes(&mut buf, 1, &[make_flat_msg(b"e1"), make_flat_msg(b"e2")]);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);
    MqCommand::write_commit_offset(&mut buf, 1, 100, 2);
    apply(&engine, &MqCommand::split_from(&mut buf), 3, 1002);

    MqCommand::write_create_queue(
        &mut buf,
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
    );
    apply(&engine, &MqCommand::split_from(&mut buf), 4, 1003);

    MqCommand::write_create_actor_group(
        &mut buf,
        "actors",
        ActorVariantConfig::default(),
        RetentionPolicy::default(),
        false,
        None,
    );
    apply(&engine, &MqCommand::split_from(&mut buf), 6, 1005);

    MqCommand::write_create_job_cron(
        &mut buf,
        "cron-job",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
    );
    apply(&engine, &MqCommand::split_from(&mut buf), 8, 1007);

    MqCommand::write_create_session(&mut buf, 100, "workers", 30000, 60000);
    apply(&engine, &MqCommand::split_from(&mut buf), 9, 1008);

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
    MqCommand::write_publish_bytes(&mut buf, 1, &[make_flat_msg(b"post-restore")]);
    let resp = apply(&engine2, &MqCommand::split_from(&mut buf), 11, 2000);
    assert_eq!(resp.tag(), ResponseEntry::TAG_PUBLISHED);
}

// =============================================================================
// ID allocation uniqueness
// =============================================================================

#[test]
fn test_id_allocation_across_entity_types() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let mut ids = Vec::new();
    for (i, name) in ["t1", "q1", "ns1", "j1"].iter().enumerate() {
        let resp = match i {
            0 => {
                MqCommand::write_create_topic(&mut buf, name, &RetentionPolicy::default(), 0);
                apply(
                    &engine,
                    &MqCommand::split_from(&mut buf),
                    (i + 1) as u64,
                    1000,
                )
            }
            1 => {
                MqCommand::write_create_queue(
                    &mut buf,
                    name,
                    AckVariantConfig::default(),
                    RetentionPolicy::default(),
                    None,
                    false,
                    None,
                    false,
                    None,
                );
                apply(
                    &engine,
                    &MqCommand::split_from(&mut buf),
                    (i + 1) as u64,
                    1001,
                )
            }
            2 => {
                MqCommand::write_create_actor_group(
                    &mut buf,
                    name,
                    ActorVariantConfig::default(),
                    RetentionPolicy::default(),
                    false,
                    None,
                );
                apply(
                    &engine,
                    &MqCommand::split_from(&mut buf),
                    (i + 1) as u64,
                    1002,
                )
            }
            3 => {
                MqCommand::write_create_job_cron(
                    &mut buf,
                    name,
                    AckVariantConfig::default(),
                    RetentionPolicy::default(),
                );
                apply(
                    &engine,
                    &MqCommand::split_from(&mut buf),
                    (i + 1) as u64,
                    1003,
                )
            }
            _ => unreachable!(),
        };
        assert_eq!(
            resp.tag(),
            ResponseEntry::TAG_ENTITY_CREATED,
            "expected EntityCreated, got tag={}",
            resp.tag()
        );
        ids.push(resp.entity_id());
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
    let mut buf = BytesMut::new();

    MqCommand::write_delete_topic(&mut buf, 999);
    let c0 = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    MqCommand::write_delete_consumer_group(&mut buf, 999);
    let c1 = apply(&engine, &MqCommand::split_from(&mut buf), 2, 1000);
    MqCommand::write_publish_bytes(&mut buf, 999, &[]);
    let c2 = apply(&engine, &MqCommand::split_from(&mut buf), 3, 1000);
    let cases = vec![
        c0,
        c1,
        c2,
        apply(
            &engine,
            &MqCommand::group_deliver(&mut buf, 999, 1, 1),
            4,
            1000,
        ),
    ];

    for resp in cases {
        assert_eq!(
            resp.tag(),
            ResponseEntry::TAG_ERROR,
            "expected Error, got tag={}",
            resp.tag()
        );
    }
}

#[test]
fn test_duplicate_entity_names() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_topic(&mut buf, "dup", &RetentionPolicy::default(), 0);
    apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    MqCommand::write_create_topic(&mut buf, "dup", &RetentionPolicy::default(), 0);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);
    assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR);

    MqCommand::write_create_queue(
        &mut buf,
        "dup",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    apply(&engine, &MqCommand::split_from(&mut buf), 3, 1002);
    MqCommand::write_create_queue(
        &mut buf,
        "dup",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 4, 1003);
    assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR);
}

// =============================================================================
// Delete and recreate entities
// =============================================================================

#[test]
fn test_delete_and_recreate_entity() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    // Create and delete topic
    MqCommand::write_create_topic(&mut buf, "ephemeral", &RetentionPolicy::default(), 0);
    apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    MqCommand::write_delete_topic(&mut buf, 1);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    // Recreate with same name
    MqCommand::write_create_topic(&mut buf, "ephemeral", &RetentionPolicy::default(), 0);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 3, 1002);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        resp.tag()
    );
    assert_eq!(resp.entity_id(), 2); // new ID

    // New entity works
    MqCommand::write_publish_bytes(&mut buf, 2, &[make_flat_msg(b"new-msg")]);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 4, 1003);
    assert_eq!(resp.tag(), ResponseEntry::TAG_PUBLISHED);

    // Old ID doesn't work
    MqCommand::write_publish_bytes(&mut buf, 1, &[make_flat_msg(b"x")]);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 5, 1004);
    assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR);
}

// =============================================================================
// Batch command tests
// =============================================================================

#[test]
fn test_batch_mixed_creates_and_publishes() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_topic(&mut buf, "t1", &RetentionPolicy::default(), 0);
    let cmd1 = MqCommand::split_from(&mut buf);
    MqCommand::write_create_topic(&mut buf, "t2", &RetentionPolicy::default(), 0);
    let cmd2 = MqCommand::split_from(&mut buf);
    MqCommand::write_publish_bytes(&mut buf, 1, &[make_flat_msg(b"hello")]);
    let cmd3 = MqCommand::split_from(&mut buf);
    MqCommand::write_batch(&mut buf, &[cmd1, cmd2, cmd3]);
    let batch = MqCommand::split_from(&mut buf);

    let resp = apply(&engine, &batch, 1, 1000);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_BATCH,
        "expected BatchResponse, got tag={}",
        resp.tag()
    );
    {
        let entries: Vec<_> = resp.batch_entries().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0].tag(),
            ResponseEntry::TAG_ENTITY_CREATED,
            "First should be EntityCreated(1), got tag={}",
            entries[0].tag()
        );
        assert_eq!(entries[0].entity_id(), 1, "First entity_id should be 1");
        assert_eq!(
            entries[1].tag(),
            ResponseEntry::TAG_ENTITY_CREATED,
            "Second should be EntityCreated(2), got tag={}",
            entries[1].tag()
        );
        assert_eq!(entries[1].entity_id(), 2, "Second entity_id should be 2");
        assert_eq!(
            entries[2].tag(),
            ResponseEntry::TAG_PUBLISHED,
            "Third should be Published, got tag={}",
            entries[2].tag()
        );
    }

    // Verify both topics exist
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 2);
}

#[test]
fn test_batch_partial_errors() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    // First command will fail (nonexistent topic), second should still succeed
    MqCommand::write_publish_bytes(&mut buf, 999, &[make_flat_msg(b"fail")]);
    let cmd1 = MqCommand::split_from(&mut buf);
    MqCommand::write_create_topic(&mut buf, "ok", &RetentionPolicy::default(), 0);
    let cmd2 = MqCommand::split_from(&mut buf);
    MqCommand::write_batch(&mut buf, &[cmd1, cmd2]);
    let batch = MqCommand::split_from(&mut buf);

    let resp = apply(&engine, &batch, 1, 1000);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_BATCH,
        "expected BatchResponse, got tag={}",
        resp.tag()
    );
    {
        let entries: Vec<_> = resp.batch_entries().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].tag(),
            ResponseEntry::TAG_ERROR,
            "First should be Error, got tag={}",
            entries[0].tag()
        );
        assert_eq!(
            entries[1].tag(),
            ResponseEntry::TAG_ENTITY_CREATED,
            "Second should be EntityCreated, got tag={}",
            entries[1].tag()
        );
    }

    // Topic was created despite earlier error (no rollback)
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 1);
}

#[test]
fn test_batch_empty() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_batch(&mut buf, &[]);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_BATCH,
        "expected BatchResponse, got tag={}",
        resp.tag()
    );
    assert_eq!(resp.batch_count(), 0);
}

// =============================================================================
// Consumer group lifecycle
// =============================================================================

#[test]
fn test_consumer_group_create_and_delete() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    // Create
    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "my-group", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };
    assert!(group_id > 0);

    // Duplicate create returns AlreadyExists
    match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "my-group", 1),
        2,
        1001,
    ) {
        e => assert!(
            e.is_already_exists(),
            "expected AlreadyExists, got tag={}",
            e.tag()
        ),
    }

    // Delete
    MqCommand::write_delete_consumer_group(&mut buf, group_id);
    match apply(&engine, &MqCommand::split_from(&mut buf), 3, 1002) {
        e => assert!(e.is_ok(), "expected Ok, got tag={}", e.tag()),
    }

    // Can recreate after delete
    match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "my-group", 0),
        4,
        1003,
    ) {
        e => assert_eq!(
            e.tag(),
            ResponseEntry::TAG_ENTITY_CREATED,
            "expected EntityCreated, got tag={}",
            e.tag()
        ),
    }
}

#[test]
fn test_consumer_group_single_member_join_sync() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();

    // Create group
    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "g1", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    // Join with empty member_id -> auto-assigned
    let (member_id, generation) = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            assert!(
                e.group_joined_phase_complete(),
                "single member should complete immediately"
            );
            assert!(e.group_joined_is_leader(), "sole member should be leader");
            assert_eq!(e.group_joined_generation(), 1);
            let member_id = e.group_joined_fields().1;
            assert!(!member_id.is_empty());
            let generation = e.group_joined_generation();
            (member_id, generation)
        }
    };

    // Sync as leader with assignments
    match apply(
        &engine,
        &MqCommand::sync_consumer_group(
            &mut buf,
            group_id,
            generation,
            &member_id,
            &[(&member_id, b"assignment-data")],
        ),
        3,
        1002,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_SYNCED,
                "expected GroupSynced, got tag={}",
                e.tag()
            );
            assert!(e.group_synced_phase_complete());
            assert_eq!(e.group_synced_assignment(), b"assignment-data");
        }
    }

    // Heartbeat
    match apply(
        &engine,
        &MqCommand::heartbeat_consumer_group(&mut buf, group_id, &member_id, generation),
        4,
        1003,
    ) {
        e => assert!(e.is_ok(), "expected Ok, got tag={}", e.tag()),
    }
}

#[test]
fn test_consumer_group_two_members_join() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();

    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "g2", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    // First member joins -> phase NOT complete (waiting for more)
    let m1_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            // With only one member, phase completes immediately
            assert!(e.group_joined_phase_complete());
            e.group_joined_fields().1
        }
    };

    // Complete sync for gen 1
    apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, group_id, 1, &m1_id, &[(&m1_id, b"a1")]),
        3,
        1002,
    );

    // Second member joins -> triggers rebalance
    let m2_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            // Phase NOT complete -- waiting for m1 to re-join
            assert!(!e.group_joined_phase_complete());
            e.group_joined_fields().1
        }
    };

    // m1 re-joins -> now all members joined -> phase completes
    match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            assert!(e.group_joined_phase_complete());
            assert_eq!(e.group_joined_generation(), 2);
        }
    }

    // Both sync in gen 2
    apply(
        &engine,
        &MqCommand::sync_consumer_group(
            &mut buf,
            group_id,
            2,
            &m1_id,
            &[(&m1_id, b"a1-v2"), (&m2_id, b"a2-v2")],
        ),
        6,
        1005,
    );
    match apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, group_id, 2, &m2_id, &[]),
        7,
        1006,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_SYNCED,
                "expected GroupSynced, got tag={}",
                e.tag()
            );
            assert!(e.group_synced_phase_complete());
            assert_eq!(e.group_synced_assignment(), b"a2-v2");
        }
    }
}

#[test]
fn test_consumer_group_leave() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "g3", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    // Join
    let member_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            e.group_joined_fields().1
        }
    };

    // Leave
    match apply(
        &engine,
        &MqCommand::leave_consumer_group(&mut buf, group_id, &member_id),
        3,
        1002,
    ) {
        e => assert!(e.is_ok(), "expected Ok, got tag={}", e.tag()),
    }

    // Leave again (idempotent)
    match apply(
        &engine,
        &MqCommand::leave_consumer_group(&mut buf, group_id, &member_id),
        4,
        1003,
    ) {
        e => assert!(e.is_ok(), "expected Ok, got tag={}", e.tag()),
    }
}

#[test]
fn test_consumer_group_heartbeat_fencing() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();

    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "g4", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    let member_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            e.group_joined_fields().1
        }
    };

    // Correct generation
    match apply(
        &engine,
        &MqCommand::heartbeat_consumer_group(&mut buf, group_id, &member_id, 1),
        3,
        1002,
    ) {
        e => assert!(e.is_ok(), "expected Ok, got tag={}", e.tag()),
    }

    // Wrong generation
    match apply(
        &engine,
        &MqCommand::heartbeat_consumer_group(&mut buf, group_id, &member_id, 99),
        4,
        1003,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(
                e.error_kind(),
                ResponseEntry::ERR_ILLEGAL_GENERATION,
                "expected IllegalGeneration"
            );
        }
    }

    // Unknown member
    match apply(
        &engine,
        &MqCommand::heartbeat_consumer_group(&mut buf, group_id, "unknown-member", 1),
        5,
        1004,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(
                e.error_kind(),
                ResponseEntry::ERR_UNKNOWN_MEMBER_ID,
                "expected UnknownMemberId"
            );
        }
    }

    // Nonexistent group
    match apply(
        &engine,
        &MqCommand::heartbeat_consumer_group(&mut buf, 99999, &member_id, 1),
        6,
        1005,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(
                e.error_kind(),
                ResponseEntry::ERR_NOT_FOUND,
                "expected NotFound"
            );
        }
    }
}

#[test]
fn test_consumer_group_offset_commit_and_fetch() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    // Create topic + group
    MqCommand::write_create_topic(&mut buf, "events", &RetentionPolicy::default(), 0);
    let topic_id = match apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "cg-offsets", 1),
        2,
        1001,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    // Join + sync to get a generation
    let member_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            e.group_joined_fields().1
        }
    };

    apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, group_id, 1, &member_id, &[(&member_id, b"a")]),
        4,
        1003,
    );

    // Commit offset
    match apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, group_id, 1, topic_id, 0, 42, None, 2000),
        5,
        2000,
    ) {
        e => assert!(e.is_ok(), "expected Ok, got tag={}", e.tag()),
    }

    // Read offset via metadata
    let offset = engine
        .metadata()
        .get_consumer_group(group_id)
        .and_then(|g| g.get_offset(topic_id, 0));
    assert_eq!(offset, Some(42));

    // Commit higher offset
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, group_id, 1, topic_id, 0, 100, None, 3000),
        6,
        3000,
    );
    let offset = engine
        .metadata()
        .get_consumer_group(group_id)
        .and_then(|g| g.get_offset(topic_id, 0));
    assert_eq!(offset, Some(100));

    // Wrong generation should fail
    match apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, group_id, 99, topic_id, 0, 200, None, 4000),
        7,
        4000,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(
                e.error_kind(),
                ResponseEntry::ERR_ILLEGAL_GENERATION,
                "expected IllegalGeneration"
            );
        }
    }
}

#[test]
fn test_consumer_group_session_expiry() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "g-expire", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    // Join with 5s session timeout
    let member_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            e.group_joined_fields().1
        }
    };

    // Member heartbeat at t=1000, session_timeout=5000
    // Expire at t=5999 -> no expiry yet
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 5999),
        3,
        5999,
    );
    assert!(
        engine
            .metadata()
            .get_consumer_group(group_id)
            .unwrap()
            .has_member(&member_id)
    );

    // Expire at t=6001 -> should expire
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 6001),
        4,
        6001,
    );
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
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();

    // Create group
    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "snap-group", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    // Join member
    let member_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            e.group_joined_fields().1
        }
    };

    // Sync
    apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, group_id, 1, &member_id, &[(&member_id, b"asn")]),
        3,
        1002,
    );

    // Commit offset
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, group_id, 1, 42, 0, 99, None, 1003),
        4,
        1003,
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
    match apply(
        &engine2,
        &MqCommand::heartbeat_consumer_group(&mut buf, group_id, &member_id, 1),
        5,
        2000,
    ) {
        e => assert!(e.is_ok(), "expected Ok after restore, got tag={}", e.tag()),
    }
}

#[test]
fn test_consumer_group_offset_expiry() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();

    let group_id = match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, "g-offset-exp", 1),
        1,
        1000,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
    };

    // Join + sync
    let member_id = match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
            group_id,
            "",
            "client",
            5000,
            60000,
            "consumer",
            &[("range", b"")],
        ),
        2,
        1001,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_GROUP_JOINED,
                "expected GroupJoined, got tag={}",
                e.tag()
            );
            e.group_joined_fields().1
        }
    };

    apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, group_id, 1, &member_id, &[(&member_id, b"a")]),
        3,
        1002,
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
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 5999),
        4,
        5999,
    );
    assert!(
        engine
            .metadata()
            .get_consumer_group(group_id)
            .unwrap()
            .has_member(&member_id)
    );

    // Expire after session timeout -> member removed, group becomes empty
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 6002),
        5,
        6002,
    );
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
    let mut buf = bytes::BytesMut::new();
    match apply(
        &engine,
        &MqCommand::create_consumer_group(&mut buf, name, 1),
        log_idx,
        ts,
    ) {
        e => {
            assert_eq!(
                e.tag(),
                ResponseEntry::TAG_ENTITY_CREATED,
                "expected EntityCreated, got tag={}",
                e.tag()
            );
            e.entity_id()
        }
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
    let mut buf = bytes::BytesMut::new();
    let e = apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    );
    assert_eq!(
        e.tag(),
        ResponseEntry::TAG_GROUP_JOINED,
        "expected GroupJoined, got tag={}",
        e.tag()
    );
    let member_id = e.group_joined_fields().1;
    let generation = e.group_joined_generation();
    let phase_complete = e.group_joined_phase_complete();
    (member_id, generation, phase_complete)
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
    let mut buf = bytes::BytesMut::new();
    match apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, group_id, generation, member_id, assignments),
        log_idx,
        ts,
    ) {
        e => assert_eq!(
            e.tag(),
            ResponseEntry::TAG_GROUP_SYNCED,
            "expected GroupSynced, got tag={}",
            e.tag()
        ),
    }
}

#[test]
fn test_cg_join_nonexistent_group() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    match apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(e.error_kind(), ResponseEntry::ERR_NOT_FOUND);
            assert_eq!(e.error_entity_id(), 99999, "expected NotFound(99999)");
        }
    }
}

#[test]
fn test_cg_sync_nonexistent_group() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    match apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, 99999, 1, "m1", &[]),
        1,
        1000,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(e.error_kind(), ResponseEntry::ERR_NOT_FOUND);
            assert_eq!(e.error_entity_id(), 99999, "expected NotFound(99999)");
        }
    }
}

#[test]
fn test_cg_sync_wrong_generation() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-sync-g", 1, 1000);
    let (mid, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    assert_eq!(g, 1);

    // Sync with wrong generation
    match apply(
        &engine,
        &MqCommand::sync_consumer_group(&mut buf, gid, 999, &mid, &[]),
        3,
        1002,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(
                e.error_kind(),
                ResponseEntry::ERR_ILLEGAL_GENERATION,
                "expected IllegalGeneration"
            );
        }
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
    let mut buf = bytes::BytesMut::new();
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
    apply(
        &engine,
        &MqCommand::leave_consumer_group(&mut buf, gid, &ma),
        6,
        1005,
    );
    assert_eq!(
        engine.metadata().get_consumer_group(gid).unwrap().phase(),
        bisque_mq::consumer_group::GroupPhase::PreparingRebalance
    );
}

#[test]
fn test_cg_leave_during_rebalance() {
    let mut buf = bytes::BytesMut::new();
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
    apply(
        &engine,
        &MqCommand::leave_consumer_group(&mut buf, gid, &mb),
        7,
        1006,
    );

    // Should still have members, phase should be determined by remaining count
    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert!(group.member_count() >= 1);
}

#[test]
fn test_cg_leave_last_member() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let gid = create_group(&mut engine, "g-leave-last", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    apply(
        &engine,
        &MqCommand::leave_consumer_group(&mut buf, gid, &ma),
        4,
        1003,
    );

    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.phase(), bisque_mq::consumer_group::GroupPhase::Empty);
    assert!(group.leader().is_none());
    assert_eq!(group.member_count(), 0);
}

#[test]
fn test_cg_heartbeat_during_rebalance() {
    let mut buf = bytes::BytesMut::new();
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
    match apply(
        &engine,
        &MqCommand::heartbeat_consumer_group(&mut buf, gid, &ma, g),
        5,
        1004,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(
                e.error_kind(),
                ResponseEntry::ERR_REBALANCE_IN_PROGRESS,
                "expected RebalanceInProgress"
            );
        }
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
    let mut buf = BytesMut::new();
    let gid = create_group(&mut engine, "g-off-over", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit offset 10
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 42, 0, 10, None, 1003),
        4,
        1003,
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
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 42, 0, 20, None, 1004),
        5,
        1004,
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
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-off-multi", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit to different topic/partition combos
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 10, 0, 100, None, 1003),
        4,
        1003,
    );
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 10, 1, 200, None, 1004),
        5,
        1004,
    );
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 20, 0, 300, None, 1005),
        6,
        1005,
    );

    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.get_offset(10, 0), Some(100));
    assert_eq!(group.get_offset(10, 1), Some(200));
    assert_eq!(group.get_offset(20, 0), Some(300));
    assert_eq!(group.get_offset(20, 1), None); // Not committed
}

#[test]
fn test_cg_offset_boundary_zero() {
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-off-zero", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit offset 0 -- valid boundary
    match apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 42, 0, 0, None, 1003),
        4,
        1003,
    ) {
        e => assert!(e.is_ok(), "expected Ok, got tag={}", e.tag()),
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
    let mut buf = BytesMut::new();
    match apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, 99999, 1, 42, 0, 10, None, 1000),
        1,
        1000,
    ) {
        e => {
            assert_eq!(e.tag(), ResponseEntry::TAG_ERROR);
            assert_eq!(e.error_kind(), ResponseEntry::ERR_NOT_FOUND);
            assert_eq!(e.error_entity_id(), 99999, "expected NotFound(99999)");
        }
    }
}

#[test]
fn test_cg_session_expiry_triggers_rebalance() {
    let mut buf = bytes::BytesMut::new();
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
    apply(
        &engine,
        &MqCommand::heartbeat_consumer_group(&mut buf, gid, &ma, g),
        6,
        5000,
    );

    // Expire at t=31003 -> member B's last heartbeat was at join time 1002, timeout 30000
    // So B expires at 1002 + 30000 = 31002
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 31003),
        7,
        31003,
    );

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
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-exp-multi", 1, 1000);

    // Member A with 10s timeout
    let ea = apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    );
    assert_eq!(
        ea.tag(),
        ResponseEntry::TAG_GROUP_JOINED,
        "expected GroupJoined for A"
    );
    let member_id = ea.group_joined_fields().1;
    // Member B with 60s timeout
    let eb = apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    );
    assert_eq!(
        eb.tag(),
        ResponseEntry::TAG_GROUP_JOINED,
        "expected GroupJoined for B"
    );
    let mb = eb.group_joined_fields().1;
    // A rejoins (preserving 10s timeout) to complete
    apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 11002),
        6,
        11002,
    );
    let group = engine.metadata().get_consumer_group(gid).unwrap();
    assert_eq!(group.member_count(), 1);
    assert!(group.has_member(&mb));
    assert!(!group.has_member(&member_id));
}

#[test]
fn test_cg_session_expiry_boundary() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let gid = create_group(&mut engine, "g-exp-bound", 1, 1000);
    // Join with 10s timeout, heartbeat at t=1000
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1000);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1001);

    // Expire at exact boundary: 1000 + 30000 = 31000 (default session timeout)
    // Member heartbeat was at 1000, timeout is 30000
    // At t=31000: last_heartbeat(1000) + timeout(30000) = 31000, not < 31000 -> NOT expired
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 31000),
        4,
        31000,
    );
    assert_eq!(
        engine
            .metadata()
            .get_consumer_group(gid)
            .unwrap()
            .member_count(),
        1
    );

    // At t=31001: 1000 + 30000 = 31000 < 31001 -> expired
    apply(
        &engine,
        &MqCommand::expire_group_sessions(&mut buf, 31001),
        5,
        31001,
    );
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
    let mut buf = BytesMut::new();

    MqCommand::write_create_consumer_group(&mut buf, "g-batch", 1);
    let create_cmd = MqCommand::split_from(&mut buf);
    MqCommand::write_batch(&mut buf, &[create_cmd]);
    let batch = MqCommand::split_from(&mut buf);

    let resp = apply(&engine, &batch, 1, 1000);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_BATCH,
        "expected BatchResponse"
    );
    let entries: Vec<_> = resp.batch_entries().collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated"
    );
    assert!(
        engine
            .metadata()
            .get_consumer_group(entries[0].entity_id())
            .is_some()
    );
}

#[test]
fn test_cg_batch_mixed_types() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_topic(&mut buf, "t-batch", &RetentionPolicy::default(), 0);
    let topic_cmd = MqCommand::split_from(&mut buf);
    MqCommand::write_create_consumer_group(&mut buf, "g-batch-mix", 1);
    let group_cmd = MqCommand::split_from(&mut buf);
    MqCommand::write_batch(&mut buf, &[topic_cmd, group_cmd]);
    let batch = MqCommand::split_from(&mut buf);

    let resp = apply(&engine, &batch, 1, 1000);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_BATCH,
        "expected BatchResponse"
    );
    let entries: Vec<_> = resp.batch_entries().collect();
    assert_eq!(entries.len(), 2);
    // Both should be EntityCreated
    for e in entries.iter() {
        assert_eq!(
            e.tag(),
            ResponseEntry::TAG_ENTITY_CREATED,
            "expected EntityCreated"
        );
    }
}

#[test]
fn test_cg_protocol_selection_tiebreak() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();
    let gid = create_group(&mut engine, "g-proto-tie", 1, 1000);

    // Member A: ["range", "roundrobin"]
    let ea = apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    );
    assert_eq!(
        ea.tag(),
        ResponseEntry::TAG_GROUP_JOINED,
        "expected GroupJoined for A"
    );
    let ma = ea.group_joined_fields().1;
    // Member B: ["roundrobin", "range"]
    let eb = apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    );
    assert_eq!(
        eb.tag(),
        ResponseEntry::TAG_GROUP_JOINED,
        "expected GroupJoined for B"
    );
    // A rejoins to complete phase
    let ec = apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    );
    assert_eq!(
        ec.tag(),
        ResponseEntry::TAG_GROUP_JOINED,
        "expected GroupJoined for A rejoin"
    );
    let protocol_name = ec.group_joined_fields().2;
    // Both protocols supported by 2 members -- tiebreak by position
    assert!(protocol_name == "range" || protocol_name == "roundrobin");
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
    let mut buf = bytes::BytesMut::new();
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
    apply(
        &engine,
        &MqCommand::leave_consumer_group(&mut buf, gid, &leader1),
        6,
        1005,
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
    let mut buf = BytesMut::new();
    let gid = create_group(&mut engine, "g-proto-change", 1, 1000);

    // Join with "range"
    let (ma, gen1, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, gen1, &ma, &[(&ma, b"a")], 3, 1002);

    // Rejoin with "roundrobin"
    let e = apply(
        &engine,
        &MqCommand::join_consumer_group(
            &mut buf,
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
    );
    assert_eq!(
        e.tag(),
        ResponseEntry::TAG_GROUP_JOINED,
        "expected GroupJoined"
    );
    assert_eq!(e.group_joined_generation(), 2);
    assert_eq!(e.group_joined_fields().2, "roundrobin");
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
    let mut buf = bytes::BytesMut::new();
    let mut engine = make_engine();
    let gid = create_group(&mut engine, "g-snap-empty", 1, 1000);

    // Join, commit offset, then leave (group becomes empty but offsets remain)
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 42, 0, 100, Some("meta"), 1003),
        4,
        1003,
    );
    apply(
        &engine,
        &MqCommand::leave_consumer_group(&mut buf, gid, &ma),
        5,
        1004,
    );

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
    let mut buf = BytesMut::new();
    let gid = create_group(&mut engine, "g-snap-ops", 1, 1000);
    let (ma, g, _) = join_group(&mut engine, gid, "", 2, 1001);
    sync_group(&mut engine, gid, g, &ma, &[(&ma, b"a")], 3, 1002);

    // Commit an offset
    apply(
        &engine,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 42, 0, 50, None, 1003),
        4,
        1003,
    );

    // Snapshot + restore
    let snap = engine.snapshot();
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // All operations should continue working:

    // 1. Heartbeat
    match apply(
        &engine2,
        &MqCommand::heartbeat_consumer_group(&mut buf, gid, &ma, g),
        10,
        2000,
    ) {
        e => assert!(e.is_ok(), "heartbeat after restore, got tag={}", e.tag()),
    }

    // 2. Offset commit
    match apply(
        &engine2,
        &MqCommand::commit_group_offset(&mut buf, gid, g, 42, 1, 75, None, 2001),
        11,
        2001,
    ) {
        e => assert!(
            e.is_ok(),
            "offset commit after restore, got tag={}",
            e.tag()
        ),
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
    let mut buf = BytesMut::new();

    // Create an ack-variant queue (auto-creates source topic)
    MqCommand::write_create_queue(
        &mut buf,
        "test-queue",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    // Find the source topic
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    assert!(source_topic_id > 0);
    drop(group);

    // Publish messages to source topic → auto-enqueues into ack group
    let msgs: Vec<Bytes> = (0..3)
        .map(|i| make_flat_msg(format!("msg-{i}").as_bytes()))
        .collect();
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &msgs);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);
    assert_eq!(resp.tag(), ResponseEntry::TAG_PUBLISHED);
    assert_eq!(resp.published_count(), 3);

    // Verify ack state has pending messages
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 3);
    drop(group);

    // Create session for consumer
    MqCommand::write_create_session(&mut buf, 100, "consumer-1", 30000, 60000);
    apply(&engine, &MqCommand::split_from(&mut buf), 3, 1002);

    // Deliver
    let resp = apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 10),
        4,
        1003,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    let delivered_msgs: Vec<_> = resp.messages().collect();
    assert_eq!(delivered_msgs.len(), 3);
    let delivered_ids: Vec<_> = delivered_msgs.iter().map(|m| m.message_id).collect();

    // Verify in-flight
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 0);
    assert_eq!(ack.in_flight_count(), 3);
    drop(group);

    // Ack all
    let resp = apply(
        &engine,
        &MqCommand::group_ack(&mut buf, group_id, &delivered_ids, None),
        5,
        1004,
    );
    assert!(resp.is_ok());

    // Verify empty
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 0);
    assert_eq!(ack.in_flight_count(), 0);
}

#[test]
fn test_ack_nack_returns_to_pending() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "nack-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    // Publish
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[make_flat_msg(b"msg")]);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    // Deliver
    let resp = apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 1),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    let msg_id = resp.messages().next().unwrap().message_id;

    // Nack
    apply(
        &engine,
        &MqCommand::group_nack(&mut buf, group_id, &[msg_id]),
        4,
        1003,
    );

    // Verify returned to pending
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let ack = group.ack_state().unwrap();
    assert_eq!(ack.pending_count(), 1);
    assert_eq!(ack.in_flight_count(), 0);
}

#[test]
fn test_ack_release_returns_without_attempt_increment() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "release-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[make_flat_msg(b"msg")]);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    let resp = apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 1),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    let msg_id = resp.messages().next().unwrap().message_id;

    // Release
    apply(
        &engine,
        &MqCommand::group_release(&mut buf, group_id, &[msg_id]),
        4,
        1003,
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
    let mut buf = BytesMut::new();

    let mut config = AckVariantConfig::default();
    config.max_retries = 1;
    config.dead_letter_topic = Some("dlq-topic".to_string());

    MqCommand::write_create_queue(
        &mut buf,
        "timeout-q",
        config,
        RetentionPolicy::default(),
        None,
        true,
        Some("dlq-topic"),
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[make_flat_msg(b"dlq-msg")]);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    // Deliver (attempts = 1 after deliver)
    let resp = apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 1),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    let msg_id = resp.messages().next().unwrap().message_id;

    // Timeout with max_retries=1 → should dead letter
    let resp = apply(
        &engine,
        &MqCommand::group_timeout_expired(&mut buf, group_id, &[msg_id]),
        4,
        1003,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_DEAD_LETTERED,
        "expected DeadLettered, got tag={}",
        resp.tag()
    );
    assert_eq!(resp.dead_letter_ids().count(), 1);
}

#[test]
fn test_ack_extend_visibility_via_engine() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "vis-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &[make_flat_msg(b"msg")]);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    let resp = apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 1),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    let msg_id = resp.messages().next().unwrap().message_id;

    // Extend visibility
    let resp = apply(
        &engine,
        &MqCommand::group_extend_visibility(&mut buf, group_id, &[msg_id], 60_000),
        4,
        1003,
    );
    assert!(resp.is_ok());

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
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "purge-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    let msgs: Vec<Bytes> = (0..5).map(|_| make_flat_msg(b"x")).collect();
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &msgs);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    // Verify pending
    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    assert_eq!(group.ack_state().unwrap().pending_count(), 5);
    drop(group);

    // Purge
    let resp = apply(
        &engine,
        &MqCommand::group_purge(&mut buf, group_id),
        3,
        1002,
    );
    assert!(resp.is_ok());

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    assert_eq!(group.ack_state().unwrap().pending_count(), 0);
}

#[test]
fn test_ack_get_attributes() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "attrs-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    MqCommand::write_publish_bytes(
        &mut buf,
        source_topic_id,
        &[make_flat_msg(b"a"), make_flat_msg(b"b")],
    );
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);

    apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 1),
        3,
        1002,
    );

    let resp = apply(
        &engine,
        &MqCommand::group_get_attributes(&mut buf, group_id),
        4,
        1003,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_STATS,
        "expected Stats, got tag={}",
        resp.tag()
    );
    {
        // Stats binary layout: [tag:1][status:1][kind:1][pad:5][log_idx:8][group_id:8][variant:4][pad:4][pending:8][in_flight:8][dlq:8][active_actors:8]
        let kind = resp.buf[2];
        assert_eq!(kind, 1u8, "expected ConsumerGroup kind=1");
        let variant_u32 = u32::from_le_bytes(resp.buf[24..28].try_into().unwrap());
        let pending = u64::from_le_bytes(resp.buf[32..40].try_into().unwrap());
        let in_flight = u64::from_le_bytes(resp.buf[40..48].try_into().unwrap());
        assert_eq!(pending, 1, "expected pending_count=1");
        assert_eq!(in_flight, 1, "expected in_flight_count=1");
        assert_eq!(
            variant_u32,
            GroupVariant::Ack as u32,
            "expected Ack variant"
        );
    }
}

// =============================================================================
// Actor Variant Integration Tests (via engine apply_command)
// =============================================================================

#[test]
fn test_actor_group_assign_deliver_ack() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_actor_group(
        &mut buf,
        "test-actors",
        ActorVariantConfig::default(),
        RetentionPolicy::default(),
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

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
    let resp = apply(
        &engine,
        &MqCommand::group_assign_actors(&mut buf, group_id, 42, &[actor_id.clone()]),
        2,
        1001,
    );
    assert!(resp.is_ok());

    // Deliver via engine command
    let resp = apply(
        &engine,
        &MqCommand::group_deliver_actor(&mut buf, group_id, 42, &[actor_id.clone()]),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    let __msgs: Vec<_> = resp.messages().collect();
    assert_eq!(__msgs.len(), 1);
    assert_eq!(__msgs[0].message_id, 100);

    // Ack via engine command
    let resp = apply(
        &engine,
        &MqCommand::group_ack_actor(&mut buf, group_id, b"actor-1", 100, None),
        4,
        1003,
    );
    assert!(resp.is_ok());

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
    let mut buf = BytesMut::new();

    MqCommand::write_create_actor_group(
        &mut buf,
        "nack-actors",
        ActorVariantConfig::default(),
        RetentionPolicy::default(),
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let actor_id = Bytes::from_static(b"actor-1");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 1000, None, 50)
            .unwrap();
    }

    apply(
        &engine,
        &MqCommand::group_assign_actors(&mut buf, group_id, 42, &[actor_id.clone()]),
        2,
        1001,
    );

    let resp = apply(
        &engine,
        &MqCommand::group_deliver_actor(&mut buf, group_id, 42, &[actor_id.clone()]),
        3,
        1002,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    let msg_id = resp.messages().next().unwrap().message_id;

    // Nack
    let resp = apply(
        &engine,
        &MqCommand::group_nack_actor(&mut buf, group_id, b"actor-1", msg_id),
        4,
        1003,
    );
    assert!(resp.is_ok());

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
    let mut buf = BytesMut::new();

    MqCommand::write_create_actor_group(
        &mut buf,
        "release-actors",
        ActorVariantConfig::default(),
        RetentionPolicy::default(),
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let actor_id = Bytes::from_static(b"actor-1");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 1000, None, 50)
            .unwrap();
    }

    apply(
        &engine,
        &MqCommand::group_assign_actors(&mut buf, group_id, 42, &[actor_id.clone()]),
        2,
        1001,
    );

    apply(
        &engine,
        &MqCommand::group_deliver_actor(&mut buf, group_id, 42, &[actor_id.clone()]),
        3,
        1002,
    );

    // Release consumer → actor should be unassigned, in-flight returned to pending
    let resp = apply(
        &engine,
        &MqCommand::group_release_actors(&mut buf, group_id, 42),
        4,
        1003,
    );
    assert!(resp.is_ok());

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
    let mut buf = BytesMut::new();

    MqCommand::write_create_actor_group(
        &mut buf,
        "evict-actors",
        ActorVariantConfig::default(),
        RetentionPolicy::default(),
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

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
    let resp = apply(
        &engine,
        &MqCommand::group_evict_idle(&mut buf, group_id, 1000),
        2,
        1001,
    );
    assert!(resp.is_ok());

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let actor_state = group.actor_state().unwrap();
    assert_eq!(actor_state.active_count(), 0);
}

#[test]
fn test_actor_get_attributes() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_actor_group(
        &mut buf,
        "attr-actors",
        ActorVariantConfig::default(),
        RetentionPolicy::default(),
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let actor_id = Bytes::from_static(b"actor-1");
    {
        let group = engine.metadata().get_consumer_group(group_id).unwrap();
        let actor_state = group.actor_state().unwrap();
        let config = group.actor_config().unwrap();
        actor_state
            .apply_send(config, group_id, &actor_id, 100, 1000, None, 50)
            .unwrap();
    }

    let resp = apply(
        &engine,
        &MqCommand::group_get_attributes(&mut buf, group_id),
        2,
        1001,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_STATS,
        "expected Stats, got tag={}",
        resp.tag()
    );
    {
        // Stats binary layout: [tag:1][status:1][kind:1][pad:5][log_idx:8][group_id:8][variant:4][pad:4][pending:8][in_flight:8][dlq:8][active_actors:8]
        let kind = resp.buf[2];
        assert_eq!(kind, 1u8, "expected ConsumerGroup kind=1");
        let variant_u32 = u32::from_le_bytes(resp.buf[24..28].try_into().unwrap());
        let active_actors = u64::from_le_bytes(resp.buf[56..64].try_into().unwrap());
        assert_eq!(
            variant_u32,
            GroupVariant::Actor as u32,
            "expected Actor variant"
        );
        assert_eq!(active_actors, 1, "expected active_actor_count=1");
    }
}

// =============================================================================
// Ack Variant Snapshot/Restore Integration
// =============================================================================

#[test]
fn test_ack_snapshot_restore_via_engine() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "snap-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    let group = engine.metadata().get_consumer_group(group_id).unwrap();
    let source_topic_id = group.meta.source_topic_id;
    drop(group);

    // Publish and deliver some messages
    let msgs: Vec<Bytes> = (0..5).map(|_| make_flat_msg(b"snap-data")).collect();
    MqCommand::write_publish_bytes(&mut buf, source_topic_id, &msgs);
    apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);
    apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 2),
        3,
        1002,
    );

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
    let resp = apply(
        &engine2,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 10),
        10,
        2000,
    );
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_MESSAGES,
        "expected Messages, got tag={}",
        resp.tag()
    );
    assert_eq!(resp.messages().count(), 3);
}

// =============================================================================
// Delete group cleans up
// =============================================================================

#[test]
fn test_delete_consumer_group_cleans_up() {
    let mut engine = make_engine();
    let mut buf = BytesMut::new();

    MqCommand::write_create_queue(
        &mut buf,
        "del-q",
        AckVariantConfig::default(),
        RetentionPolicy::default(),
        None,
        false,
        None,
        false,
        None,
    );
    let __e = apply(&engine, &MqCommand::split_from(&mut buf), 1, 1000);
    assert_eq!(
        __e.tag(),
        ResponseEntry::TAG_ENTITY_CREATED,
        "expected EntityCreated, got tag={}",
        __e.tag()
    );
    let group_id = __e.entity_id();

    // Delete
    MqCommand::write_delete_consumer_group(&mut buf, group_id);
    let resp = apply(&engine, &MqCommand::split_from(&mut buf), 2, 1001);
    assert!(resp.is_ok());

    // Verify deleted
    assert!(engine.metadata().get_consumer_group(group_id).is_none());

    // Operations on deleted group fail
    let resp = apply(
        &engine,
        &MqCommand::group_deliver(&mut buf, group_id, 100, 1),
        3,
        1002,
    );
    assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR);
}
