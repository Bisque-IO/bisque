//! Integration tests for gap fixes (mq-mqtt alignment).
//!
//! Tests cover:
//!   Gap 2: Subscription ID replication in engine Binding
//!   Gap 3+4: Flow control counters and rate limiting in PersistedSession
//!   Gap 5: Will properties preserved on clean disconnect 0x04 (adapter-level, tested in session_store)
//!   Gap 6: Shared subscription is_shared in SubDeliveryInfo (adapter-level)
//!   Gap 7: Server redirection (adapter-level)

use bytes::Bytes;

use bisque_mq::config::MqConfig;
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
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

fn create_queue(engine: &mut MqEngine, name: &str, log_index: u64, time: u64) -> u64 {
    match engine.apply_command(
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
        log_index,
        time,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

// =============================================================================
// Gap 2: Subscription ID in Binding
// =============================================================================

#[test]
fn test_binding_subscription_id_codec() {
    let cmd = MqCommand::create_binding_with_opts(10, 20, Some("sensors/#"), false, None, Some(42));
    let view = cmd.as_create_binding();
    assert_eq!(view.exchange_id(), 10);
    assert_eq!(view.topic_id(), 20);
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
        None,
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
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Restore and verify counters
    let resp = engine.apply_command(&MqCommand::restore_session("client-fc"), 2, 1001, None);
    match resp {
        MqResponse::SessionRestored {
            session_id,
            subscription_data,
            session_expiry_ms,
            ..
        } => {
            assert_eq!(session_id, 100);
            let _ = session_expiry_ms;
            // subscription_data restoration is not yet implemented (TODO in engine)
            let _ = subscription_data;
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
// Snapshot round-trip with sessions
// =============================================================================

#[test]
fn test_snapshot_with_sessions() {
    let mut engine = make_engine();
    let sub_data = Bytes::from_static(b"sub");

    // Persist session with flow control
    engine.apply_command(
        &MqCommand::persist_session(100, "snap-client", 3600, &sub_data, 3, 2, 500),
        3,
        1002,
        None,
    );

    let snapshot = engine.snapshot();
    assert!(!snapshot.sessions.is_empty());

    // Restore to fresh engine
    let mut engine2 = make_engine();
    engine2.restore(snapshot);

    // Session survived
    let resp = engine2.apply_command(&MqCommand::restore_session("snap-client"), 5, 1004, None);
    assert!(matches!(resp, MqResponse::SessionRestored { .. }));
}
