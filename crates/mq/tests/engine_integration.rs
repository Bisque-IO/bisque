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
use bisque_mq::types::*;

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-integration-test"))
}

fn make_msg(value: &[u8]) -> MessagePayload {
    MessagePayload {
        key: None,
        value: Bytes::from(value.to_vec()),
        headers: Vec::new(),
        timestamp: 1000,
    }
}

fn make_msg_with_key(key: &[u8], value: &[u8]) -> MessagePayload {
    MessagePayload {
        key: Some(Bytes::from(key.to_vec())),
        value: Bytes::from(value.to_vec()),
        headers: Vec::new(),
        timestamp: 1000,
    }
}

// =============================================================================
// End-to-end topic workflow
// =============================================================================

#[test]
fn test_topic_publish_consume_commit_purge() {
    let mut engine = make_engine();

    // Create topic
    let topic_id = match engine.apply_command(
        MqCommand::CreateTopic {
            name: "orders".to_string(),
            retention: RetentionPolicy {
                max_age_secs: Some(3600),
                ..Default::default()
            },
        },
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Register consumer
    engine.apply_command(
        MqCommand::RegisterConsumer {
            consumer_id: 100,
            group_name: "order-processor".to_string(),
            subscriptions: vec![Subscription {
                entity_type: EntityType::Topic,
                entity_id: topic_id,
            }],
        },
        2,
        1001,
    );

    // Publish batch
    let messages: Vec<MessagePayload> = (0..10)
        .map(|i| make_msg_with_key(format!("order-{}", i).as_bytes(), b"order-data"))
        .collect();
    let resp = engine.apply_command(MqCommand::Publish { topic_id, messages }, 3, 1002);
    match resp {
        MqResponse::Published { offsets } => assert_eq!(offsets.len(), 10),
        other => panic!("expected Published, got {:?}", other),
    }

    // Commit offset
    let resp = engine.apply_command(
        MqCommand::CommitOffset {
            topic_id,
            consumer_id: 100,
            offset: 3,
        },
        4,
        1003,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Purge old messages
    let resp = engine.apply_command(
        MqCommand::PurgeTopic {
            topic_id,
            before_index: 3,
        },
        5,
        1004,
    );
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

    let queue_id = match engine.apply_command(
        MqCommand::CreateQueue {
            name: "tasks".to_string(),
            config,
        },
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Enqueue
    engine.apply_command(
        MqCommand::Enqueue {
            queue_id,
            messages: vec![make_msg(b"process-payment")],
            dedup_keys: vec![None],
        },
        2,
        1001,
    );

    // First delivery
    engine.apply_command(
        MqCommand::Deliver {
            queue_id,
            consumer_id: 100,
            max_count: 1,
        },
        3,
        2000,
    );

    // Timeout expired (attempt 1) → re-enqueue
    engine.apply_command(
        MqCommand::TimeoutExpired {
            queue_id,
            message_ids: vec![2],
        },
        4,
        8000,
    );

    // Second delivery
    engine.apply_command(
        MqCommand::Deliver {
            queue_id,
            consumer_id: 100,
            max_count: 1,
        },
        5,
        9000,
    );

    // Timeout expired (attempt 2 >= max_retries) → dead letter
    engine.apply_command(
        MqCommand::TimeoutExpired {
            queue_id,
            message_ids: vec![2],
        },
        6,
        15000,
    );

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
        MqCommand::CreateQueue {
            name: "idempotent-queue".to_string(),
            config,
        },
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    let dedup_key = Bytes::from_static(b"payment-abc-123");

    // First enqueue
    engine.apply_command(
        MqCommand::Enqueue {
            queue_id,
            messages: vec![make_msg(b"payment")],
            dedup_keys: vec![Some(dedup_key.clone())],
        },
        2,
        1000,
    );

    // Duplicate within window — silent skip
    engine.apply_command(
        MqCommand::Enqueue {
            queue_id,
            messages: vec![make_msg(b"payment-retry")],
            dedup_keys: vec![Some(dedup_key.clone())],
        },
        3,
        1010,
    );

    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.pending_count, 1);

    // Prune dedup window
    engine.apply_command(
        MqCommand::PruneDedupWindow {
            queue_id,
            before_timestamp: 1005,
        },
        4,
        1100,
    );

    // After pruning, same key accepted
    engine.apply_command(
        MqCommand::Enqueue {
            queue_id,
            messages: vec![make_msg(b"payment-new")],
            dedup_keys: vec![Some(dedup_key.clone())],
        },
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
        MqCommand::CreateActorNamespace {
            name: "users".to_string(),
            config: ActorConfig::default(),
        },
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
        engine.apply_command(
            MqCommand::SendToActor {
                namespace_id: ns_id,
                actor_id: (*actor_id).clone(),
                message: make_msg(format!("msg-{}", i).as_bytes()),
            },
            (i + 2) as u64,
            1000 + i as u64,
        );
    }

    // Verify via snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.actor_namespaces[0].actors.len(), 2);

    // Assign actors
    engine.apply_command(
        MqCommand::AssignActors {
            namespace_id: ns_id,
            consumer_id: 100,
            actor_ids: vec![actor_a.clone()],
        },
        10,
        2000,
    );

    // Deliver (serialized)
    let resp = engine.apply_command(
        MqCommand::DeliverActorMessage {
            namespace_id: ns_id,
            actor_id: actor_a.clone(),
            consumer_id: 100,
        },
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
        MqCommand::DeliverActorMessage {
            namespace_id: ns_id,
            actor_id: actor_a.clone(),
            consumer_id: 100,
        },
        13,
        2003,
    );
    match &resp {
        MqResponse::Messages { messages } => assert_eq!(messages.len(), 0),
        other => panic!("expected empty Messages, got {:?}", other),
    }

    // Ack
    engine.apply_command(
        MqCommand::AckActorMessage {
            namespace_id: ns_id,
            actor_id: actor_a.clone(),
            message_id: msg_id,
        },
        14,
        2004,
    );

    // Now can deliver next
    let resp = engine.apply_command(
        MqCommand::DeliverActorMessage {
            namespace_id: ns_id,
            actor_id: actor_a.clone(),
            consumer_id: 100,
        },
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

    let job_id = match engine.apply_command(
        MqCommand::CreateJob {
            name: "daily-report".to_string(),
            config,
        },
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    };

    // Assign and trigger
    engine.apply_command(
        MqCommand::AssignJob {
            job_id,
            consumer_id: 100,
        },
        2,
        1001,
    );
    engine.apply_command(
        MqCommand::TriggerJob {
            job_id,
            execution_id: 1000,
            triggered_at: 5000,
        },
        3,
        5000,
    );

    let snap = engine.snapshot();
    assert_eq!(snap.jobs[0].meta.state.current_execution_id, Some(1000));

    // Complete
    engine.apply_command(
        MqCommand::CompleteJob {
            job_id,
            execution_id: 1000,
        },
        4,
        6000,
    );

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
        MqCommand::CreateJob {
            name: "flaky-job".to_string(),
            config: JobConfig::default(),
        },
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
        engine.apply_command(
            MqCommand::TriggerJob {
                job_id,
                execution_id: exec_id,
                triggered_at: t,
            },
            i + 2,
            t,
        );
        engine.apply_command(
            MqCommand::FailJob {
                job_id,
                execution_id: exec_id,
                error: format!("error {}", i),
            },
            i + 10,
            t + 500,
        );
    }

    let snap = engine.snapshot();
    assert_eq!(snap.jobs[0].meta.state.consecutive_failures, 3);

    // Success resets counter
    engine.apply_command(
        MqCommand::TriggerJob {
            job_id,
            execution_id: 999,
            triggered_at: 10000,
        },
        20,
        10000,
    );
    engine.apply_command(
        MqCommand::CompleteJob {
            job_id,
            execution_id: 999,
        },
        21,
        11000,
    );
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
        MqCommand::CreateQueue {
            name: "q".to_string(),
            config: QueueConfig::default(),
        },
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let ns_id = match engine.apply_command(
        MqCommand::CreateActorNamespace {
            name: "ns".to_string(),
            config: ActorConfig::default(),
        },
        2,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    let job_id = match engine.apply_command(
        MqCommand::CreateJob {
            name: "j".to_string(),
            config: JobConfig::default(),
        },
        3,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    // Register consumer
    engine.apply_command(
        MqCommand::RegisterConsumer {
            consumer_id: 100,
            group_name: "g".to_string(),
            subscriptions: Vec::new(),
        },
        4,
        1000,
    );

    // Enqueue, deliver (queue in-flight)
    engine.apply_command(
        MqCommand::Enqueue {
            queue_id,
            messages: vec![make_msg(b"task")],
            dedup_keys: vec![None],
        },
        5,
        1001,
    );
    engine.apply_command(
        MqCommand::Deliver {
            queue_id,
            consumer_id: 100,
            max_count: 1,
        },
        6,
        1002,
    );

    // Send to actor, assign
    let actor_id = Bytes::from_static(b"actor-1");
    engine.apply_command(
        MqCommand::SendToActor {
            namespace_id: ns_id,
            actor_id: actor_id.clone(),
            message: make_msg(b"msg"),
        },
        7,
        1003,
    );
    engine.apply_command(
        MqCommand::AssignActors {
            namespace_id: ns_id,
            consumer_id: 100,
            actor_ids: vec![actor_id.clone()],
        },
        8,
        1004,
    );

    // Assign job
    engine.apply_command(
        MqCommand::AssignJob {
            job_id,
            consumer_id: 100,
        },
        9,
        1005,
    );

    // Verify assigned state via snapshot
    let snap = engine.snapshot();
    assert_eq!(snap.queues[0].meta.in_flight_count, 1);
    assert_eq!(snap.jobs[0].meta.state.assigned_consumer_id, Some(100));

    // Disconnect consumer — cascading cleanup
    engine.apply_command(MqCommand::DisconnectConsumer { consumer_id: 100 }, 10, 2000);

    let snap = engine.snapshot();

    // Queue: in-flight → pending
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
            messages: vec![make_msg(b"e1"), make_msg(b"e2")],
        },
        2,
        1001,
    );
    engine.apply_command(
        MqCommand::CommitOffset {
            topic_id: 1,
            consumer_id: 100,
            offset: 2,
        },
        3,
        1002,
    );

    engine.apply_command(
        MqCommand::CreateQueue {
            name: "tasks".to_string(),
            config: QueueConfig {
                dedup_window_secs: Some(60),
                ..Default::default()
            },
        },
        4,
        1003,
    );
    engine.apply_command(
        MqCommand::Enqueue {
            queue_id: 2,
            messages: vec![make_msg(b"t1")],
            dedup_keys: vec![Some(Bytes::from_static(b"k1"))],
        },
        5,
        1004,
    );

    engine.apply_command(
        MqCommand::CreateActorNamespace {
            name: "actors".to_string(),
            config: ActorConfig::default(),
        },
        6,
        1005,
    );
    engine.apply_command(
        MqCommand::SendToActor {
            namespace_id: 3,
            actor_id: Bytes::from_static(b"actor-1"),
            message: make_msg(b"hello"),
        },
        7,
        1006,
    );

    engine.apply_command(
        MqCommand::CreateJob {
            name: "cron-job".to_string(),
            config: JobConfig {
                cron_expression: "0 * * * * *".to_string(),
                ..Default::default()
            },
        },
        8,
        1007,
    );

    engine.apply_command(
        MqCommand::RegisterConsumer {
            consumer_id: 100,
            group_name: "workers".to_string(),
            subscriptions: Vec::new(),
        },
        9,
        1008,
    );

    engine.apply_command(
        MqCommand::RegisterProducer {
            producer_id: 200,
            name: Some("producer-1".to_string()),
        },
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
        MqCommand::Publish {
            topic_id: 1,
            messages: vec![make_msg(b"post-restore")],
        },
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
        MqCommand::CreateTopic {
            name: "t".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    engine.apply_command(
        MqCommand::Publish {
            topic_id: 1,
            messages: vec![make_msg(b"msg")],
        },
        20,
        1001,
    );

    engine.apply_command(
        MqCommand::CreateQueue {
            name: "q".to_string(),
            config: QueueConfig::default(),
        },
        2,
        1000,
    );
    engine.apply_command(
        MqCommand::Enqueue {
            queue_id: 2,
            messages: vec![make_msg(b"task")],
            dedup_keys: vec![None],
        },
        10,
        1001,
    );

    engine.apply_command(
        MqCommand::CreateActorNamespace {
            name: "ns".to_string(),
            config: ActorConfig::default(),
        },
        3,
        1000,
    );
    engine.apply_command(
        MqCommand::SendToActor {
            namespace_id: 3,
            actor_id: Bytes::from_static(b"a1"),
            message: make_msg(b"msg"),
        },
        5,
        1001,
    );

    // Purge floor = min of message indices only (creation indices tracked
    // separately via structural purge floor in MDBX).
    // Actor msg at 5, queue msg at 10, topic msg at 20 → floor = 5.
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
                MqCommand::CreateTopic {
                    name: name.to_string(),
                    retention: RetentionPolicy::default(),
                },
                (i + 1) as u64,
                1000,
            ),
            1 => engine.apply_command(
                MqCommand::CreateQueue {
                    name: name.to_string(),
                    config: QueueConfig::default(),
                },
                (i + 1) as u64,
                1001,
            ),
            2 => engine.apply_command(
                MqCommand::CreateActorNamespace {
                    name: name.to_string(),
                    config: ActorConfig::default(),
                },
                (i + 1) as u64,
                1002,
            ),
            3 => engine.apply_command(
                MqCommand::CreateJob {
                    name: name.to_string(),
                    config: JobConfig::default(),
                },
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
        engine.apply_command(MqCommand::DeleteTopic { topic_id: 999 }, 1, 1000),
        engine.apply_command(MqCommand::DeleteQueue { queue_id: 999 }, 2, 1000),
        engine.apply_command(
            MqCommand::Publish {
                topic_id: 999,
                messages: Vec::new(),
            },
            3,
            1000,
        ),
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 999,
                consumer_id: 1,
                max_count: 1,
            },
            4,
            1000,
        ),
        engine.apply_command(
            MqCommand::Ack {
                queue_id: 999,
                message_ids: vec![1],
            },
            5,
            1000,
        ),
        engine.apply_command(
            MqCommand::SendToActor {
                namespace_id: 999,
                actor_id: Bytes::from_static(b"a"),
                message: make_msg(b"x"),
            },
            6,
            1000,
        ),
        engine.apply_command(
            MqCommand::TriggerJob {
                job_id: 999,
                execution_id: 1,
                triggered_at: 1000,
            },
            7,
            1000,
        ),
        engine.apply_command(MqCommand::Heartbeat { consumer_id: 999 }, 8, 1000),
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
        MqCommand::CreateTopic {
            name: "dup".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    let resp = engine.apply_command(
        MqCommand::CreateTopic {
            name: "dup".to_string(),
            retention: RetentionPolicy::default(),
        },
        2,
        1001,
    );
    assert!(matches!(resp, MqResponse::Error(_)));

    engine.apply_command(
        MqCommand::CreateQueue {
            name: "dup".to_string(),
            config: QueueConfig::default(),
        },
        3,
        1002,
    );
    let resp = engine.apply_command(
        MqCommand::CreateQueue {
            name: "dup".to_string(),
            config: QueueConfig::default(),
        },
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
        MqCommand::CreateQueue {
            name: "long-tasks".to_string(),
            config: QueueConfig {
                visibility_timeout_ms: 5000,
                ..Default::default()
            },
        },
        1,
        1000,
    ) {
        MqResponse::EntityCreated { id } => id,
        _ => panic!(),
    };

    engine.apply_command(
        MqCommand::Enqueue {
            queue_id,
            messages: vec![make_msg(b"big-task")],
            dedup_keys: vec![None],
        },
        2,
        1001,
    );

    // Deliver
    let resp = engine.apply_command(
        MqCommand::Deliver {
            queue_id,
            consumer_id: 100,
            max_count: 1,
        },
        3,
        2000,
    );
    let msg_id = match resp {
        MqResponse::Messages { messages } => messages[0].message_id,
        _ => panic!(),
    };

    // Extend visibility
    let resp = engine.apply_command(
        MqCommand::ExtendVisibility {
            queue_id,
            message_ids: vec![msg_id],
            extension_ms: 10_000,
        },
        4,
        4000,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Ack
    let resp = engine.apply_command(
        MqCommand::Ack {
            queue_id,
            message_ids: vec![msg_id],
        },
        5,
        5000,
    );
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
        MqCommand::CreateTopic {
            name: "ephemeral".to_string(),
            retention: RetentionPolicy::default(),
        },
        1,
        1000,
    );
    engine.apply_command(MqCommand::DeleteTopic { topic_id: 1 }, 2, 1001);

    // Recreate with same name
    let resp = engine.apply_command(
        MqCommand::CreateTopic {
            name: "ephemeral".to_string(),
            retention: RetentionPolicy::default(),
        },
        3,
        1002,
    );
    match resp {
        MqResponse::EntityCreated { id } => assert_eq!(id, 2), // new ID
        other => panic!("expected EntityCreated, got {:?}", other),
    }

    // New entity works
    let resp = engine.apply_command(
        MqCommand::Publish {
            topic_id: 2,
            messages: vec![make_msg(b"new-msg")],
        },
        4,
        1003,
    );
    assert!(matches!(resp, MqResponse::Published { .. }));

    // Old ID doesn't work
    let resp = engine.apply_command(
        MqCommand::Publish {
            topic_id: 1,
            messages: vec![make_msg(b"x")],
        },
        5,
        1004,
    );
    assert!(matches!(resp, MqResponse::Error(_)));
}

// =============================================================================
// Batch command tests
// =============================================================================

#[test]
fn test_batch_mixed_creates_and_publishes() {
    let mut engine = make_engine();

    let batch = MqCommand::Batch(vec![
        MqCommand::CreateTopic {
            name: "t1".to_string(),
            retention: RetentionPolicy::default(),
        },
        MqCommand::CreateTopic {
            name: "t2".to_string(),
            retention: RetentionPolicy::default(),
        },
        MqCommand::Publish {
            topic_id: 1, // t1's ID (first alloc_id)
            messages: vec![make_msg(b"hello")],
        },
    ]);

    let resp = engine.apply_command(batch, 1, 1000);
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
    let batch = MqCommand::Batch(vec![
        MqCommand::Publish {
            topic_id: 999,
            messages: vec![make_msg(b"fail")],
        },
        MqCommand::CreateTopic {
            name: "ok".to_string(),
            retention: RetentionPolicy::default(),
        },
    ]);

    let resp = engine.apply_command(batch, 1, 1000);
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

    let resp = engine.apply_command(MqCommand::Batch(vec![]), 1, 1000);
    match resp {
        MqResponse::BatchResponse(resps) => {
            assert_eq!(resps.len(), 0);
        }
        other => panic!("expected BatchResponse, got {:?}", other),
    }
}
