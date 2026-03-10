//! Integration tests for the MQ engine.
//!
//! These tests exercise end-to-end workflows across multiple entity types,
//! snapshot/restore cycles, and complex multi-step operations.
//!
//! Since engine fields are `pub(crate)`, integration tests verify state via
//! `engine.snapshot()` and `engine.apply_command()` responses.

use bytes::Bytes;

use bisque_mq::config::{ActorConfig, JobConfig, MqConfig, QueueConfig};
use bisque_mq::engine::MqEngine;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::*;

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-integration-test"))
}

fn make_flat_msg(value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(Bytes::from(value.to_vec()))
        .timestamp(1000)
        .build()
}

fn make_flat_msg_with_key(key: &[u8], value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(Bytes::from(value.to_vec()))
        .key(Bytes::from(key.to_vec()))
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
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Register consumer
    engine.apply_command(
        &MqCommand::register_consumer(
            100,
            "order-processor",
            &[Subscription {
                entity_type: EntityType::Topic,
                entity_id: topic_id,
            }],
        ),
        2,
        1001,
    );

    // Publish batch
    let messages: Vec<Bytes> = (0..10)
        .map(|i| make_flat_msg_with_key(format!("order-{}", i).as_bytes(), b"order-data"))
        .collect();
    let resp = engine.apply_command(&MqCommand::publish(topic_id, &messages), 3, 1002);
    match resp {
        MqResponse::Published { offsets } => assert_eq!(offsets.len(), 10),
        other => panic!("expected Published, got {:?}", other),
    }

    // Commit offset
    let resp = engine.apply_command(&MqCommand::commit_offset(topic_id, 100, 3), 4, 1003);
    assert!(matches!(resp, MqResponse::Ok));

    // Purge old messages
    let resp = engine.apply_command(&MqCommand::purge_topic(topic_id, 3), 5, 1004);
    assert!(matches!(resp, MqResponse::Ok));

    // Verify via snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 1);
    assert_eq!(snap.topics[0].meta.tail_index, 3);
    assert_eq!(snap.topics[0].consumer_offsets[0].committed_offset, 3);
}

// =============================================================================
// Queue lifecycle with retry and dead-letter
// =============================================================================

#[test]
fn test_queue_message_lifecycle_with_retries() {
    let config = QueueConfig {
        visibility_timeout_ms: 5000,
        max_retries: 2,
        ..Default::default()
    };
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(&MqCommand::create_queue("tasks", &config), 1, 1000) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Enqueue
    engine.apply_command(
        &MqCommand::enqueue(queue_id, &[make_flat_msg(b"process-payment")], &[None]),
        2,
        1001,
    );

    // First delivery
    engine.apply_command(&MqCommand::deliver(queue_id, 100, 1), 3, 2000);

    // Timeout expired (attempt 1) -> re-enqueue
    engine.apply_command(&MqCommand::timeout_expired(queue_id, &[2]), 4, 8000);

    // Second delivery
    engine.apply_command(&MqCommand::deliver(queue_id, 100, 1), 5, 9000);

    // Timeout expired (attempt 2 >= max_retries) -> dead letter
    engine.apply_command(&MqCommand::timeout_expired(queue_id, &[2]), 6, 15000);

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.dlq_count, 1);
    assert_eq!(snap.queues[0].meta.pending_count, 0);
    assert_eq!(snap.queues[0].meta.in_flight_count, 0);
}

// =============================================================================
// Queue deduplication
// =============================================================================

#[test]
fn test_queue_deduplication() {
    let config = QueueConfig {
        dedup_window_secs: Some(60),
        ..Default::default()
    };
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        &MqCommand::create_queue("idempotent-queue", &config),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let dedup_key = Bytes::from_static(b"payment-abc-123");

    // First enqueue
    engine.apply_command(
        &MqCommand::enqueue(
            queue_id,
            &[make_flat_msg(b"payment")],
            &[Some(dedup_key.clone())],
        ),
        2,
        1000,
    );

    // Duplicate within window -- silent skip
    engine.apply_command(
        &MqCommand::enqueue(
            queue_id,
            &[make_flat_msg(b"payment-retry")],
            &[Some(dedup_key.clone())],
        ),
        3,
        1010,
    );

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 1);

    // Prune dedup window
    engine.apply_command(&MqCommand::prune_dedup_window(queue_id, 1005), 4, 1100);

    // After pruning, same key accepted
    engine.apply_command(
        &MqCommand::enqueue(
            queue_id,
            &[make_flat_msg(b"payment-new")],
            &[Some(dedup_key.clone())],
        ),
        5,
        1100,
    );
    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 2);
}

// =============================================================================
// Actor full lifecycle
// =============================================================================

#[test]
fn test_actor_full_lifecycle() {
    let mut engine = make_engine();

    let ns_id = match engine.apply_command(
        &MqCommand::create_actor_namespace("users", &ActorConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let actor_a = Bytes::from_static(b"user-alice");
    let actor_b = Bytes::from_static(b"user-bob");

    // Send messages to different actors
    for (i, actor_id) in [&actor_a, &actor_b, &actor_a, &actor_b].iter().enumerate() {
        let flat_msg = make_flat_msg(format!("msg-{}", i).as_bytes());
        engine.apply_command(
            &MqCommand::send_to_actor(ns_id, actor_id, &flat_msg),
            (i + 2) as u64,
            1000 + i as u64,
        );
    }

    // Verify via snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.actor_namespaces[0].actors.len(), 2);

    // Assign actors
    engine.apply_command(
        &MqCommand::assign_actors(ns_id, 100, &[actor_a.clone()]),
        10,
        2000,
    );

    // Deliver (serialized)
    let resp = engine.apply_command(
        &MqCommand::deliver_actor_message(ns_id, &actor_a, 100),
        12,
        2002,
    );
    let msg_id = match &resp {
        MqResponse::Messages { messages } => {
            assert_eq!(messages.len(), 1);
            messages[0].message_id
        }
        other => panic!("expected Messages, got {:?}", other),
    };

    // Second deliver blocked (serialized)
    let resp = engine.apply_command(
        &MqCommand::deliver_actor_message(ns_id, &actor_a, 100),
        13,
        2003,
    );
    match &resp {
        MqResponse::Messages { messages } => assert_eq!(messages.len(), 0),
        other => panic!("expected empty Messages, got {:?}", other),
    }

    // Ack
    engine.apply_command(
        &MqCommand::ack_actor_message(ns_id, &actor_a, msg_id, None),
        14,
        2004,
    );

    // Now can deliver next
    let resp = engine.apply_command(
        &MqCommand::deliver_actor_message(ns_id, &actor_a, 100),
        15,
        2005,
    );
    match &resp {
        MqResponse::Messages { messages } => assert_eq!(messages.len(), 1),
        other => panic!("expected Messages, got {:?}", other),
    }
}

// =============================================================================
// Job scheduling
// =============================================================================

#[test]
fn test_job_scheduling_workflow() {
    let mut engine = make_engine();

    let config = JobConfig {
        cron_expression: "0 * * * * *".to_string(),
        execution_timeout_ms: 10_000,
        ..Default::default()
    };

    let job_id =
        match engine.apply_command(&MqCommand::create_job("daily-report", &config), 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

    // Assign and trigger
    engine.apply_command(&MqCommand::assign_job(job_id, 100), 2, 1001);
    engine.apply_command(&MqCommand::trigger_job(job_id, 1000, 5000), 3, 5000);

    let snap = engine.snapshot();
    assert_eq!(snap.jobs[0].meta.state.current_execution_id, Some(1000));

    // Complete
    engine.apply_command(&MqCommand::complete_job(job_id, 1000), 4, 6000);

    let snap = engine.snapshot();
    assert!(snap.jobs[0].meta.state.current_execution_id.is_none());
    assert_eq!(snap.jobs[0].meta.state.last_completed_at, Some(6000));
}

// =============================================================================
// Job failure tracking
// =============================================================================

#[test]
fn test_job_failure_tracking() {
    let mut engine = make_engine();

    let job_id = match engine.apply_command(
        &MqCommand::create_job("flaky-job", &JobConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Trigger and fail 3 times
    for i in 0..3u64 {
        let exec_id = 100 + i;
        let t = 5000 + i * 1000;
        engine.apply_command(&MqCommand::trigger_job(job_id, exec_id, t), i + 2, t);
        engine.apply_command(
            &MqCommand::fail_job(job_id, exec_id, &format!("error {}", i)),
            i + 10,
            t + 500,
        );
    }

    let snap = engine.snapshot();
    assert_eq!(snap.jobs[0].meta.state.consecutive_failures, 3);

    // Success resets counter
    engine.apply_command(&MqCommand::trigger_job(job_id, 999, 10000), 20, 10000);
    engine.apply_command(&MqCommand::complete_job(job_id, 999), 21, 11000);
    let snap = engine.snapshot();
    assert_eq!(snap.jobs[0].meta.state.consecutive_failures, 0);
}

// =============================================================================
// Consumer disconnect cascading cleanup
// =============================================================================

#[test]
fn test_consumer_disconnect_cascading_cleanup() {
    let mut engine = make_engine();

    // Create all entity types
    let queue_id = match engine.apply_command(
        &MqCommand::create_queue("q", &QueueConfig::default()),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let ns_id = match engine.apply_command(
        &MqCommand::create_actor_namespace("ns", &ActorConfig::default()),
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let job_id =
        match engine.apply_command(&MqCommand::create_job("j", &JobConfig::default()), 3, 1000) {
            MqResponse::EntityCreated { id } => id,
            _ => panic!(),
        };

    // Register consumer
    engine.apply_command(&MqCommand::register_consumer(100, "g", &[]), 4, 1000);

    // Enqueue, deliver (queue in-flight)
    engine.apply_command(
        &MqCommand::enqueue(queue_id, &[make_flat_msg(b"task")], &[None]),
        5,
        1001,
    );
    engine.apply_command(&MqCommand::deliver(queue_id, 100, 1), 6, 1002);

    // Send to actor, assign
    let actor_id = Bytes::from_static(b"actor-1");
    let flat_msg = make_flat_msg(b"msg");
    engine.apply_command(
        &MqCommand::send_to_actor(ns_id, &actor_id, &flat_msg),
        7,
        1003,
    );
    engine.apply_command(
        &MqCommand::assign_actors(ns_id, 100, &[actor_id.clone()]),
        8,
        1004,
    );

    // Assign job
    engine.apply_command(&MqCommand::assign_job(job_id, 100), 9, 1005);

    // Verify assigned state via snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.in_flight_count, 1);
    assert_eq!(snap.jobs[0].meta.state.assigned_consumer_id, Some(100));

    // Disconnect consumer -- cascading cleanup
    engine.apply_command(&MqCommand::disconnect_consumer(100), 10, 2000);

    let snap = engine.snapshot();

    // Queue: in-flight -> pending
    assert_eq!(snap.queues[0].meta.in_flight_count, 0);
    assert_eq!(snap.queues[0].meta.pending_count, 1);

    // Actor: unassigned
    let actor = snap.actor_namespaces[0]
        .actors
        .iter()
        .find(|a| a.actor_id == actor_id)
        .unwrap();
    assert!(actor.assigned_consumer_id.is_none());

    // Job: unassigned
    assert!(snap.jobs[0].meta.state.assigned_consumer_id.is_none());
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
    );
    engine.apply_command(
        &MqCommand::publish(1, &[make_flat_msg(b"e1"), make_flat_msg(b"e2")]),
        2,
        1001,
    );
    engine.apply_command(&MqCommand::commit_offset(1, 100, 2), 3, 1002);

    engine.apply_command(
        &MqCommand::create_queue(
            "tasks",
            &QueueConfig {
                dedup_window_secs: Some(60),
                ..Default::default()
            },
        ),
        4,
        1003,
    );
    engine.apply_command(
        &MqCommand::enqueue(
            2,
            &[make_flat_msg(b"t1")],
            &[Some(Bytes::from_static(b"k1"))],
        ),
        5,
        1004,
    );

    engine.apply_command(
        &MqCommand::create_actor_namespace("actors", &ActorConfig::default()),
        6,
        1005,
    );
    let flat_hello = make_flat_msg(b"hello");
    engine.apply_command(
        &MqCommand::send_to_actor(3, b"actor-1", &flat_hello),
        7,
        1006,
    );

    engine.apply_command(
        &MqCommand::create_job(
            "cron-job",
            &JobConfig {
                cron_expression: "0 * * * * *".to_string(),
                ..Default::default()
            },
        ),
        8,
        1007,
    );

    engine.apply_command(&MqCommand::register_consumer(100, "workers", &[]), 9, 1008);

    engine.apply_command(
        &MqCommand::register_producer(200, Some("producer-1")),
        10,
        1009,
    );

    // Snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.topics.len(), 1);
    assert_eq!(snap.queues.len(), 1);
    assert_eq!(snap.actor_namespaces.len(), 1);
    assert_eq!(snap.jobs.len(), 1);
    assert_eq!(snap.consumers.len(), 1);
    assert_eq!(snap.producers.len(), 1);

    // Serialize/deserialize roundtrip
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
    let (snap_restored, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&snap_bytes, bincode::config::standard()).unwrap();

    // Restore into fresh engine
    let mut engine2 = make_engine();
    engine2.restore(snap_restored);

    // Verify via second snapshot
    let snap2 = engine2.snapshot();
    assert_eq!(snap2.topics.len(), 1);
    assert_eq!(snap2.topics[0].meta.name, "events");
    assert_eq!(snap2.topics[0].meta.message_count, 2);
    assert_eq!(snap2.topics[0].consumer_offsets[0].committed_offset, 2);

    assert_eq!(snap2.queues.len(), 1);
    assert_eq!(snap2.queues[0].meta.name, "tasks");

    assert_eq!(snap2.actor_namespaces.len(), 1);
    assert_eq!(snap2.actor_namespaces[0].meta.name, "actors");
    assert_eq!(snap2.actor_namespaces[0].actors.len(), 1);

    assert_eq!(snap2.jobs.len(), 1);
    assert_eq!(snap2.jobs[0].meta.name, "cron-job");

    assert_eq!(snap2.consumers.len(), 1);
    assert_eq!(snap2.producers.len(), 1);
    assert_eq!(snap2.next_id, snap.next_id);

    // Engine continues to work after restore
    let resp = engine2.apply_command(
        &MqCommand::publish(1, &[make_flat_msg(b"post-restore")]),
        11,
        2000,
    );
    assert!(matches!(resp, MqResponse::Published { .. }));
}

// =============================================================================
// Purge floor across entities
// =============================================================================

#[test]
fn test_purge_floor_across_entities() {
    let mut engine = make_engine();

    engine.apply_command(
        &MqCommand::create_topic("t", RetentionPolicy::default(), 0),
        1,
        1000,
    );
    engine.apply_command(&MqCommand::publish(1, &[make_flat_msg(b"msg")]), 20, 1001);

    engine.apply_command(
        &MqCommand::create_queue("q", &QueueConfig::default()),
        2,
        1000,
    );
    engine.apply_command(
        &MqCommand::enqueue(2, &[make_flat_msg(b"task")], &[None]),
        10,
        1001,
    );

    engine.apply_command(
        &MqCommand::create_actor_namespace("ns", &ActorConfig::default()),
        3,
        1000,
    );
    let flat_msg = make_flat_msg(b"msg");
    engine.apply_command(&MqCommand::send_to_actor(3, b"a1", &flat_msg), 5, 1001);

    // Purge floor = min of message indices only (creation indices tracked
    // separately via structural purge floor in MDBX).
    // Actor msg at 5, queue msg at 10, topic msg at 20 -> floor = 5.
    assert_eq!(engine.compute_purge_floor(), 5);
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
            ),
            1 => engine.apply_command(
                &MqCommand::create_queue(name, &QueueConfig::default()),
                (i + 1) as u64,
                1001,
            ),
            2 => engine.apply_command(
                &MqCommand::create_actor_namespace(name, &ActorConfig::default()),
                (i + 1) as u64,
                1002,
            ),
            3 => engine.apply_command(
                &MqCommand::create_job(name, &JobConfig::default()),
                (i + 1) as u64,
                1003,
            ),
            _ => unreachable!(),
        };
        match resp {
            MqResponse::EntityCreated { id } => ids.push(id),
            other => panic!("expected EntityCreated, got {:?}", other),
        }
    }

    // All unique and monotonically increasing
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

// =============================================================================
// Error handling
// =============================================================================

#[test]
fn test_operations_on_nonexistent_entities() {
    let mut engine = make_engine();

    let cases = vec![
        engine.apply_command(&MqCommand::delete_topic(999), 1, 1000),
        engine.apply_command(&MqCommand::delete_queue(999), 2, 1000),
        engine.apply_command(&MqCommand::publish(999, &[]), 3, 1000),
        engine.apply_command(&MqCommand::deliver(999, 1, 1), 4, 1000),
        engine.apply_command(&MqCommand::ack(999, &[1], None), 5, 1000),
        engine.apply_command(
            &{
                let flat_msg = make_flat_msg(b"x");
                MqCommand::send_to_actor(999, b"a", &flat_msg)
            },
            6,
            1000,
        ),
        engine.apply_command(&MqCommand::trigger_job(999, 1, 1000), 7, 1000),
        engine.apply_command(&MqCommand::heartbeat(999), 8, 1000),
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
    );
    let resp = engine.apply_command(
        &MqCommand::create_topic("dup", RetentionPolicy::default(), 0),
        2,
        1001,
    );
    assert!(matches!(resp, MqResponse::Error(_)));

    engine.apply_command(
        &MqCommand::create_queue("dup", &QueueConfig::default()),
        3,
        1002,
    );
    let resp = engine.apply_command(
        &MqCommand::create_queue("dup", &QueueConfig::default()),
        4,
        1003,
    );
    assert!(matches!(resp, MqResponse::Error(_)));
}

// =============================================================================
// Visibility extension
// =============================================================================

#[test]
fn test_extend_visibility_workflow() {
    let mut engine = make_engine();

    let queue_id = match engine.apply_command(
        &MqCommand::create_queue(
            "long-tasks",
            &QueueConfig {
                visibility_timeout_ms: 5000,
                ..Default::default()
            },
        ),
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    engine.apply_command(
        &MqCommand::enqueue(queue_id, &[make_flat_msg(b"big-task")], &[None]),
        2,
        1001,
    );

    // Deliver
    let resp = engine.apply_command(&MqCommand::deliver(queue_id, 100, 1), 3, 2000);
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        _ => panic!(),
    };

    // Extend visibility
    let resp = engine.apply_command(
        &MqCommand::extend_visibility(queue_id, &[msg_id], 10_000),
        4,
        4000,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Ack
    let resp = engine.apply_command(&MqCommand::ack(queue_id, &[msg_id], None), 5, 5000);
    assert!(matches!(resp, MqResponse::Ok));
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
    );
    engine.apply_command(&MqCommand::delete_topic(1), 2, 1001);

    // Recreate with same name
    let resp = engine.apply_command(
        &MqCommand::create_topic("ephemeral", RetentionPolicy::default(), 0),
        3,
        1002,
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
    );
    assert!(matches!(resp, MqResponse::Published { .. }));

    // Old ID doesn't work
    let resp = engine.apply_command(&MqCommand::publish(1, &[make_flat_msg(b"x")]), 5, 1004);
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

    let resp = engine.apply_command(&batch, 1, 1000);
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

    let resp = engine.apply_command(&batch, 1, 1000);
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

    let resp = engine.apply_command(&MqCommand::batch(&[]), 1, 1000);
    match resp {
        MqResponse::BatchResponse(resps) => {
            assert_eq!(resps.len(), 0);
        }
        other => panic!("expected BatchResponse, got {:?}", other),
    }
}
