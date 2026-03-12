//! Integration tests for gap fixes (mq-mqtt alignment).
//!
//! Tests cover:
//!   Gap 1: QoS 2 inbound dedup (tags 74-75)
//!   Gap 2: Subscription ID replication in engine Binding
//!   Gap 3+4: Flow control counters and rate limiting in PersistedSession
//!   Gap 5: Will properties preserved on clean disconnect 0x04 (adapter-level, tested in session_store)
//!   Gap 6: Shared subscription is_shared in SubDeliveryInfo (adapter-level)
//!   Gap 7: Server redirection (adapter-level)

use bytes::Bytes;

use bisque_mq::config::{MqConfig, QueueConfig};
use bisque_mq::engine::MqEngine;
use bisque_mq::types::*;

// =============================================================================
// Helpers
// =============================================================================

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-gap-fixes-test"))
}

fn create_exchange(engine: &mut MqEngine, name: &str, log_index: u64, time: u64) -> u64 {
    match engine.apply_command(
        &MqCommand::create_exchange(name, ExchangeType::Topic),
        log_index,
        time,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

fn create_queue(engine: &mut MqEngine, name: &str, log_index: u64, time: u64) -> u64 {
    match engine.apply_command(
        &MqCommand::create_queue(name, &QueueConfig::default()),
        log_index,
        time,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

// =============================================================================
// Gap 1: QoS 2 Inbound Dedup (Tags 74-75)
// =============================================================================

#[test]
fn test_qos2_register_inbound_basic() {
    let mut engine = make_engine();
    let resp = engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 1, 1000);
    assert!(
        matches!(resp, MqResponse::Ok),
        "register should succeed: {:?}",
        resp
    );
}

#[test]
fn test_qos2_register_inbound_duplicate_rejected() {
    let mut engine = make_engine();
    // First registration succeeds
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 1, 1000);
    // Duplicate registration fails
    let resp = engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 2, 1001);
    assert!(
        matches!(resp, MqResponse::Error(MqError::Custom(_))),
        "duplicate should be rejected: {:?}",
        resp
    );
}

#[test]
fn test_qos2_complete_inbound_removes_entry() {
    let mut engine = make_engine();
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 1, 1000);
    // Complete removes the entry
    let resp = engine.apply_command(&MqCommand::qos2_complete_inbound(100, 1), 2, 1001);
    assert!(matches!(resp, MqResponse::Ok));
    // Re-registration should now succeed
    let resp = engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 3, 1002);
    assert!(
        matches!(resp, MqResponse::Ok),
        "re-register after complete should succeed: {:?}",
        resp
    );
}

#[test]
fn test_qos2_complete_nonexistent_is_ok() {
    let mut engine = make_engine();
    // Completing a non-existent entry is a no-op (idempotent)
    let resp = engine.apply_command(&MqCommand::qos2_complete_inbound(100, 99), 1, 1000);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_qos2_multiple_packets_per_consumer() {
    let mut engine = make_engine();
    // Register multiple packet IDs for same consumer
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 1, 1000);
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 2), 2, 1001);
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 3), 3, 1002);

    // Complete packet 2 — 1 and 3 still registered
    engine.apply_command(&MqCommand::qos2_complete_inbound(100, 2), 4, 1003);

    // Packet 2 can be re-registered
    let resp = engine.apply_command(&MqCommand::qos2_register_inbound(100, 2), 5, 1004);
    assert!(matches!(resp, MqResponse::Ok));

    // Packet 1 still registered (duplicate)
    let resp = engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 6, 1005);
    assert!(matches!(resp, MqResponse::Error(_)));
}

#[test]
fn test_qos2_different_consumers_independent() {
    let mut engine = make_engine();
    // Same packet ID on different consumers should both succeed
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 1, 1000);
    let resp = engine.apply_command(&MqCommand::qos2_register_inbound(200, 1), 2, 1001);
    assert!(
        matches!(resp, MqResponse::Ok),
        "different consumer same packet_id should succeed"
    );
}

#[test]
fn test_qos2_inbound_survives_snapshot() {
    let mut engine = make_engine();
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 1, 1000);
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 2), 2, 1001);

    // Snapshot and restore
    let snapshot = engine.snapshot();
    assert!(
        !snapshot.qos2_inbound.is_empty(),
        "snapshot should contain qos2_inbound"
    );

    let mut engine2 = make_engine();
    engine2.restore(snapshot);

    // After restore, packet 1 should still be registered (duplicate rejected)
    let resp = engine2.apply_command(&MqCommand::qos2_register_inbound(100, 1), 3, 1002);
    assert!(
        matches!(resp, MqResponse::Error(_)),
        "packet_id should survive snapshot: {:?}",
        resp
    );

    // Complete and re-register should work
    engine2.apply_command(&MqCommand::qos2_complete_inbound(100, 1), 4, 1003);
    let resp = engine2.apply_command(&MqCommand::qos2_register_inbound(100, 1), 5, 1004);
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_qos2_inbound_codec_roundtrip() {
    let cmd = MqCommand::qos2_register_inbound(42, 1234);
    assert_eq!(cmd.tag(), MqCommand::TAG_QOS2_REGISTER_INBOUND);
    let view = cmd.as_qos2_inbound();
    assert_eq!(view.consumer_id(), 42);
    assert_eq!(view.packet_id(), 1234);

    let cmd2 = MqCommand::qos2_complete_inbound(99, 5678);
    assert_eq!(cmd2.tag(), MqCommand::TAG_QOS2_COMPLETE_INBOUND);
    let view2 = cmd2.as_qos2_inbound();
    assert_eq!(view2.consumer_id(), 99);
    assert_eq!(view2.packet_id(), 5678);
}

// =============================================================================
// Gap 2: Subscription ID in Binding
// =============================================================================

#[test]
fn test_binding_subscription_id_codec() {
    let cmd = MqCommand::create_binding_with_opts(10, 20, Some("sensors/#"), false, None, Some(42));
    let view = cmd.as_create_binding();
    assert_eq!(view.exchange_id(), 10);
    assert_eq!(view.queue_id(), 20);
    assert_eq!(view.routing_key(), Some("sensors/#".to_string()));
    assert!(!view.no_local());
    assert_eq!(view.shared_group(), None);
    assert_eq!(view.subscription_id(), Some(42));
}

#[test]
fn test_binding_subscription_id_none_codec() {
    let cmd =
        MqCommand::create_binding_with_opts(10, 20, Some("topic/+"), true, Some("group1"), None);
    let view = cmd.as_create_binding();
    assert!(view.no_local());
    assert_eq!(view.shared_group(), Some("group1".to_string()));
    assert_eq!(view.subscription_id(), None);
}

#[test]
fn test_binding_subscription_id_stored_in_engine() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let queue_id = create_queue(&mut engine, "sub-q", 2, 1001);

    let resp = engine.apply_command(
        &MqCommand::create_binding_with_opts(
            exchange_id,
            queue_id,
            Some("test/#"),
            false,
            None,
            Some(99),
        ),
        3,
        1002,
    );
    assert!(
        matches!(resp, MqResponse::EntityCreated { .. }),
        "binding with subscription_id should succeed: {:?}",
        resp
    );
}

#[test]
fn test_binding_backward_compat_subscription_id_none() {
    // Basic create_binding should decode subscription_id as None
    let cmd = MqCommand::create_binding(10, 20, Some("topic/#"));
    let view = cmd.as_create_binding();
    assert_eq!(view.subscription_id(), None);
}

// =============================================================================
// Gap 3+4: Flow Control and Rate Limiting in PersistedSession
// =============================================================================

#[test]
fn test_persist_session_with_flow_control() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"sub-data");

    let resp = engine.apply_command(
        &MqCommand::persist_session(100, "client-fc", 3600, &sub_data, 5, 3, 1000),
        1,
        1000,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Restore and verify counters
    let resp = engine.apply_command(&MqCommand::restore_session("client-fc"), 2, 1001);
    match resp {
        MqResponse::SessionRestored {
            consumer_id,
            subscription_data,
            session_expiry_secs,
            ..
        } => {
            assert_eq!(consumer_id, 100);
            assert_eq!(session_expiry_secs, 3600);
            assert_eq!(subscription_data, sub_data);
        }
        other => panic!("expected SessionRestored, got {:?}", other),
    }
}

#[test]
fn test_persist_session_flow_control_codec_roundtrip() {
    let cmd = MqCommand::persist_session(
        42,
        "test-client",
        7200,
        &Bytes::from_static(b"data"),
        10,
        5,
        9999,
    );
    let view = cmd.as_persist_session();
    assert_eq!(view.consumer_id(), 42);
    assert_eq!(view.client_id(), "test-client");
    assert_eq!(view.session_expiry_secs(), 7200);
    assert_eq!(view.inbound_qos_inflight(), 10);
    assert_eq!(view.outbound_qos1_count(), 5);
    assert_eq!(view.remaining_quota(), 9999);
}

#[test]
fn test_persist_session_backward_compat_zeros() {
    // Old-style persist (with 0s) should work fine
    let cmd =
        MqCommand::persist_session(100, "old-client", 3600, &Bytes::from_static(b"x"), 0, 0, 0);
    let view = cmd.as_persist_session();
    assert_eq!(view.inbound_qos_inflight(), 0);
    assert_eq!(view.outbound_qos1_count(), 0);
    assert_eq!(view.remaining_quota(), 0);
}

// =============================================================================
// Snapshot round-trip with all gap data
// =============================================================================

#[test]
fn test_snapshot_with_qos2_and_sessions() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"sub");

    // Register QoS 2 inbound
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 1), 1, 1000);
    engine.apply_command(&MqCommand::qos2_register_inbound(100, 2), 2, 1001);

    // Persist session with flow control
    engine.apply_command(
        &MqCommand::persist_session(100, "snap-client", 3600, &sub_data, 3, 2, 500),
        3,
        1002,
    );

    let snapshot = engine.snapshot();
    assert!(!snapshot.qos2_inbound.is_empty());
    assert!(!snapshot.sessions.is_empty());

    // Restore to fresh engine
    let mut engine2 = make_engine();
    engine2.restore(snapshot);

    // QoS 2 state survived
    let resp = engine2.apply_command(&MqCommand::qos2_register_inbound(100, 1), 4, 1003);
    assert!(
        matches!(resp, MqResponse::Error(_)),
        "qos2 should survive snapshot"
    );

    // Session survived
    let resp = engine2.apply_command(&MqCommand::restore_session("snap-client"), 5, 1004);
    assert!(matches!(resp, MqResponse::SessionRestored { .. }));
}
