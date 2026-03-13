//! Comprehensive tests for retained messages in bisque-mq.
//!
//! Covers:
//!   - SET_RETAINED / GET_RETAINED / DELETE_RETAINED engine commands
//!   - Wildcard pattern matching on GET_RETAINED
//!   - Overwrite semantics (last-write wins)
//!   - Segment provenance tracking (mmap-backed vs heap)
//!   - Detach sweep when segments are purged
//!   - Manifest persistence (structural writes)
//!   - Raft recovery with retained messages in manifest
//!   - Snapshot install/restore with retained messages
//!   - Exchange deletion cleans up retained messages
//!   - Edge cases: nonexistent exchange, empty routing key, large payloads, unicode

use std::io::Cursor;
use std::sync::Arc;

use bisque_mq::MqManifestManager;
use bisque_mq::config::MqConfig;
use bisque_mq::engine::MqEngine;
use bisque_mq::exchange::RetainedValue;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::state_machine::MqStateMachine;
use bisque_mq::types::*;
use bytes::Bytes;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{LogId, SnapshotMeta, StoredMembership};

type MqTypeConfig = bisque_raft::BisqueRaftTypeConfig<MqCommand, MqResponse>;

// =============================================================================
// Helpers
// =============================================================================

fn make_engine() -> MqEngine {
    MqEngine::new(MqConfig::new("/tmp/mq-retained-test"))
}

fn make_msg(value: &[u8]) -> Bytes {
    FlatMessageBuilder::new(Bytes::from(value.to_vec()))
        .timestamp(1000)
        .build()
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

fn create_exchange_typed(
    engine: &mut MqEngine,
    name: &str,
    exchange_type: ExchangeType,
    log_index: u64,
    time: u64,
) -> u64 {
    match engine.apply_command(
        &MqCommand::create_exchange(name, exchange_type),
        log_index,
        time,
        None,
    ) {
        MqResponse::EntityCreated { id } => id,
        other => panic!("expected EntityCreated, got {:?}", other),
    }
}

/// Get retained messages count via GET_RETAINED command.
fn get_retained_count(engine: &mut MqEngine, exchange_id: u64, log_index: u64) -> usize {
    match engine.apply_command(
        &MqCommand::get_retained(exchange_id, None),
        log_index,
        9999,
        None,
    ) {
        MqResponse::RetainedMessages { messages } => messages.len(),
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

/// Get retained messages via GET_RETAINED command.
fn get_retained(
    engine: &mut MqEngine,
    exchange_id: u64,
    filter: Option<&str>,
    log_index: u64,
) -> Vec<RetainedEntry> {
    match engine.apply_command(
        &MqCommand::get_retained(exchange_id, filter),
        log_index,
        9999,
        None,
    ) {
        MqResponse::RetainedMessages { messages } => messages,
        other => panic!("expected RetainedMessages, got {:?}", other),
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

// =============================================================================
// Basic SET_RETAINED
// =============================================================================

#[test]
fn test_set_retained_basic() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let msg = make_msg(b"temp=22.5");

    let resp = engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Verify stored via GET_RETAINED
    let entries = get_retained(&mut engine, exchange_id, Some("sensors/temp"), 3);
    assert_eq!(entries.len(), 1);
    assert_eq!(&entries[0].routing_key[..], b"sensors/temp");
}

#[test]
fn test_set_retained_nonexistent_exchange() {
    let mut engine = make_engine();
    let msg = make_msg(b"data");

    let resp = engine.apply_command(
        &MqCommand::set_retained(99999, "some/key", &msg),
        1,
        1000,
        None,
    );
    assert!(matches!(resp, MqResponse::Error(MqError::NotFound { .. })));
}

#[test]
fn test_set_retained_overwrite() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_msg(b"first");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key", &msg1),
        2,
        1001,
        None,
    );

    let msg2 = make_msg(b"second");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key", &msg2),
        3,
        1002,
        None,
    );

    // Should have exactly one entry (overwritten, not appended)
    assert_eq!(get_retained_count(&mut engine, exchange_id, 4), 1);
}

#[test]
fn test_set_retained_multiple_keys() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    for i in 0..10 {
        let msg = make_msg(format!("val-{}", i).as_bytes());
        let key = format!("sensors/sensor{}", i);
        engine.apply_command(
            &MqCommand::set_retained(exchange_id, &key, &msg),
            i as u64 + 2,
            1001 + i as u64,
            None,
        );
    }

    assert_eq!(get_retained_count(&mut engine, exchange_id, 20), 10);
}

#[test]
fn test_set_retained_empty_routing_key() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);
    let msg = make_msg(b"data");

    let resp = engine.apply_command(
        &MqCommand::set_retained(exchange_id, "", &msg),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Empty routing key should be retrievable
    assert_eq!(get_retained_count(&mut engine, exchange_id, 3), 1);
}

// =============================================================================
// GET_RETAINED
// =============================================================================

#[test]
fn test_get_retained_all() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_msg(b"temp=22.5");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg1),
        2,
        1001,
        None,
    );

    let msg2 = make_msg(b"humidity=60");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/humidity", &msg2),
        3,
        1002,
        None,
    );

    // Get all retained (no filter)
    let entries = get_retained(&mut engine, exchange_id, None, 4);
    assert_eq!(entries.len(), 2);
}

#[test]
fn test_get_retained_exact_match() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_msg(b"temp=22.5");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg1),
        2,
        1001,
        None,
    );
    let msg2 = make_msg(b"humidity=60");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/humidity", &msg2),
        3,
        1002,
        None,
    );

    let entries = get_retained(&mut engine, exchange_id, Some("sensors/temp"), 4);
    assert_eq!(entries.len(), 1);
    assert_eq!(&entries[0].routing_key[..], b"sensors/temp");
}

#[test]
fn test_get_retained_wildcard_plus() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    for (i, key) in ["sensors/temp", "sensors/humidity", "actuators/valve"]
        .iter()
        .enumerate()
    {
        let msg = make_msg(format!("val-{}", i).as_bytes());
        engine.apply_command(
            &MqCommand::set_retained(exchange_id, key, &msg),
            i as u64 + 2,
            1001 + i as u64,
            None,
        );
    }

    // "sensors/+" should match sensors/temp and sensors/humidity, not actuators/valve
    let entries = get_retained(&mut engine, exchange_id, Some("sensors/+"), 5);
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert!(
            entry.routing_key.starts_with(b"sensors/"),
            "unexpected key: {:?}",
            std::str::from_utf8(&entry.routing_key)
        );
    }
}

#[test]
fn test_get_retained_wildcard_hash() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    for (i, key) in [
        "sensors/temp",
        "sensors/humidity",
        "sensors/room1/temp",
        "actuators/valve",
    ]
    .iter()
    .enumerate()
    {
        let msg = make_msg(format!("val-{}", i).as_bytes());
        engine.apply_command(
            &MqCommand::set_retained(exchange_id, key, &msg),
            i as u64 + 2,
            1001 + i as u64,
            None,
        );
    }

    // "sensors/#" should match all sensors/* including nested
    let entries = get_retained(&mut engine, exchange_id, Some("sensors/#"), 6);
    assert_eq!(entries.len(), 3);

    // "#" matches everything
    let entries = get_retained(&mut engine, exchange_id, Some("#"), 7);
    assert_eq!(entries.len(), 4);
}

#[test]
fn test_get_retained_empty_exchange() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let entries = get_retained(&mut engine, exchange_id, None, 2);
    assert!(entries.is_empty());
}

#[test]
fn test_get_retained_nonexistent_exchange() {
    let mut engine = make_engine();
    let resp = engine.apply_command(&MqCommand::get_retained(99999, None), 1, 1000, None);
    assert!(matches!(resp, MqResponse::Error(MqError::NotFound { .. })));
}

#[test]
fn test_get_retained_no_match() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg),
        2,
        1001,
        None,
    );

    let entries = get_retained(&mut engine, exchange_id, Some("actuators/+"), 3);
    assert!(entries.is_empty());
}

// =============================================================================
// DELETE_RETAINED
// =============================================================================

#[test]
fn test_delete_retained_basic() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "sensors/temp", &msg),
        2,
        1001,
        None,
    );

    let resp = engine.apply_command(
        &MqCommand::delete_retained(exchange_id, "sensors/temp"),
        3,
        1002,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Verify removed
    assert_eq!(get_retained_count(&mut engine, exchange_id, 4), 0);
}

#[test]
fn test_delete_retained_nonexistent_key() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Deleting a key that doesn't exist should be Ok (idempotent)
    let resp = engine.apply_command(
        &MqCommand::delete_retained(exchange_id, "nonexistent"),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));
}

#[test]
fn test_delete_retained_nonexistent_exchange() {
    let mut engine = make_engine();
    let resp = engine.apply_command(&MqCommand::delete_retained(99999, "key"), 1, 1000, None);
    assert!(matches!(resp, MqResponse::Error(MqError::NotFound { .. })));
}

#[test]
fn test_delete_retained_selective() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "a", &msg),
        2,
        1001,
        None,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "b", &msg),
        3,
        1002,
        None,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "c", &msg),
        4,
        1003,
        None,
    );

    // Delete only "b"
    engine.apply_command(&MqCommand::delete_retained(exchange_id, "b"), 5, 1004, None);

    assert_eq!(get_retained_count(&mut engine, exchange_id, 6), 2);
    // Verify "a" and "c" remain, "b" is gone
    assert_eq!(
        get_retained(&mut engine, exchange_id, Some("a"), 7).len(),
        1
    );
    assert_eq!(
        get_retained(&mut engine, exchange_id, Some("b"), 8).len(),
        0
    );
    assert_eq!(
        get_retained(&mut engine, exchange_id, Some("c"), 9).len(),
        1
    );
}

// =============================================================================
// SET + GET + DELETE round-trip
// =============================================================================

#[test]
fn test_retained_full_lifecycle() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // 1. Set retained
    let msg = make_msg(b"initial");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "topic/a", &msg),
        2,
        1001,
        None,
    );

    // 2. Get retained - should find it
    assert_eq!(
        get_retained(&mut engine, exchange_id, Some("topic/a"), 3).len(),
        1
    );

    // 3. Overwrite
    let msg2 = make_msg(b"updated");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "topic/a", &msg2),
        4,
        1003,
        None,
    );

    // 4. Get again - still one message
    assert_eq!(
        get_retained(&mut engine, exchange_id, Some("topic/a"), 5).len(),
        1
    );

    // 5. Delete
    engine.apply_command(
        &MqCommand::delete_retained(exchange_id, "topic/a"),
        6,
        1005,
        None,
    );

    // 6. Get again - should be empty
    assert_eq!(
        get_retained(&mut engine, exchange_id, Some("topic/a"), 7).len(),
        0
    );
}

// =============================================================================
// RetainedValue unit tests
// =============================================================================

#[test]
fn test_retained_value_detach() {
    let msg = Bytes::from_static(b"test-message");
    let mut rv = RetainedValue::mmap_backed(42, msg.clone());

    assert_eq!(rv.segment_id, Some(42));

    let old_seg = rv.detach();
    assert_eq!(old_seg, Some(42));
    assert_eq!(rv.segment_id, None, "segment_id cleared after detach");
    assert_eq!(
        &rv.message[..],
        b"test-message",
        "message preserved after detach"
    );
}

#[test]
fn test_retained_value_detach_already_heap() {
    let msg = Bytes::from_static(b"heap-message");
    let mut rv = RetainedValue::heap(msg);

    let old_seg = rv.detach();
    assert_eq!(old_seg, None, "no segment to detach from heap");
    assert_eq!(&rv.message[..], b"heap-message");
}

#[test]
fn test_retained_value_detach_idempotent() {
    let msg = Bytes::from_static(b"data");
    let mut rv = RetainedValue::mmap_backed(7, msg);

    let first = rv.detach();
    assert_eq!(first, Some(7));

    let second = rv.detach();
    assert_eq!(second, None, "second detach should be no-op");
}

#[test]
fn test_retained_value_mmap_constructor() {
    let msg = Bytes::from_static(b"mmap-data");
    let rv = RetainedValue::mmap_backed(99, msg.clone());
    assert_eq!(rv.segment_id, Some(99));
    assert_eq!(&rv.message[..], b"mmap-data");
}

#[test]
fn test_retained_value_heap_constructor() {
    let msg = Bytes::from_static(b"heap-data");
    let rv = RetainedValue::heap(msg.clone());
    assert_eq!(rv.segment_id, None);
    assert_eq!(&rv.message[..], b"heap-data");
}

// =============================================================================
// Exchange deletion cleans up retained messages
// =============================================================================

#[test]
fn test_delete_exchange_clears_retained() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"retained-data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key1", &msg),
        2,
        1001,
        None,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key2", &msg),
        3,
        1002,
        None,
    );

    // Delete the exchange
    let resp = engine.apply_command(&MqCommand::delete_exchange(exchange_id), 4, 1003, None);
    assert!(matches!(resp, MqResponse::Ok));

    // GET_RETAINED on deleted exchange should fail
    let resp = engine.apply_command(&MqCommand::get_retained(exchange_id, None), 5, 1004, None);
    assert!(matches!(resp, MqResponse::Error(MqError::NotFound { .. })));
}

// =============================================================================
// Multiple exchanges with independent retained messages
// =============================================================================

#[test]
fn test_retained_multiple_exchanges() {
    let mut engine = make_engine();
    let ex1 = create_exchange(&mut engine, "exchange1", 1, 1000);
    let ex2 = create_exchange(&mut engine, "exchange2", 2, 1001);

    let msg = make_msg(b"data");
    engine.apply_command(
        &MqCommand::set_retained(ex1, "shared/key", &msg),
        3,
        1002,
        None,
    );
    engine.apply_command(
        &MqCommand::set_retained(ex2, "shared/key", &msg),
        4,
        1003,
        None,
    );

    // Each exchange should have its own copy
    assert_eq!(get_retained_count(&mut engine, ex1, 5), 1);
    assert_eq!(get_retained_count(&mut engine, ex2, 6), 1);

    // Delete from one doesn't affect the other
    engine.apply_command(
        &MqCommand::delete_retained(ex1, "shared/key"),
        7,
        1004,
        None,
    );

    assert_eq!(get_retained_count(&mut engine, ex1, 8), 0);
    assert_eq!(get_retained_count(&mut engine, ex2, 9), 1);
}

// =============================================================================
// Snapshot round-trip with retained messages
// =============================================================================

#[test]
fn test_snapshot_includes_retained_messages() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_msg(b"retained-1");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "topic/a", &msg1),
        2,
        1001,
        None,
    );

    let msg2 = make_msg(b"retained-2");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "topic/b", &msg2),
        3,
        1002,
        None,
    );

    let snap = engine.snapshot();
    assert_eq!(snap.exchanges.len(), 1);
    assert_eq!(snap.exchanges[0].retained.len(), 2);

    // Restore into a new engine and verify via GET_RETAINED
    let mut engine2 = make_engine();
    engine2.restore(snap);

    // Find the exchange ID from the restored engine
    let snap2 = engine2.snapshot();
    let restored_ex_id = snap2.exchanges[0].meta.exchange_id;
    assert_eq!(get_retained_count(&mut engine2, restored_ex_id, 10), 2);
}

#[tokio::test]
async fn test_snapshot_install_with_retained_messages() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"retained-via-snapshot");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key", &msg),
        2,
        1001,
        None,
    );

    let snap_bytes =
        bincode::serde::encode_to_vec(&engine.snapshot(), bincode::config::standard()).unwrap();

    let mut sm = MqStateMachine::new(make_engine());
    sm.install_snapshot(
        &make_snapshot_meta(2, "retained-snap"),
        Cursor::new(snap_bytes),
    )
    .await
    .unwrap();

    // Verify retained messages in restored state machine
    let s = sm.snapshot();
    assert_eq!(s.exchanges.len(), 1);
    assert_eq!(s.exchanges[0].retained.len(), 1);
    assert_eq!(&s.exchanges[0].retained[0].routing_key[..], b"key");
}

// =============================================================================
// Manifest persistence and recovery of retained messages
// =============================================================================

#[tokio::test]
async fn test_manifest_recovery_with_retained_messages() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    // First lifecycle: install snapshot with retained messages
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"persisted-retained");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "persist/key", &msg),
        2,
        1001,
        None,
    );

    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();

    let mut sm1 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);
    sm1.install_snapshot(
        &make_snapshot_meta(2, "retained-manifest"),
        Cursor::new(snap_bytes),
    )
    .await
    .unwrap();
    drop(sm1);

    // Second lifecycle: recover from manifest
    let mut sm2 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);
    let (la, _) = sm2.applied_state().await.unwrap();
    assert_eq!(la.unwrap().index, 2);

    // Check snapshot after recovery
    let s = sm2.snapshot();
    assert_eq!(s.exchanges.len(), 1);
    assert_eq!(s.exchanges[0].retained.len(), 1);
    assert_eq!(&s.exchanges[0].retained[0].routing_key[..], b"persist/key");

    manifest.shutdown();
}

#[tokio::test]
async fn test_manifest_recovery_multiple_retained() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    // Build engine with multiple retained messages on multiple exchanges
    let mut engine = make_engine();
    let ex1 = create_exchange(&mut engine, "exchange1", 1, 1000);
    let ex2 = create_exchange(&mut engine, "exchange2", 2, 1001);

    for i in 0..5 {
        let msg = make_msg(format!("msg-{}", i).as_bytes());
        engine.apply_command(
            &MqCommand::set_retained(ex1, &format!("key/{}", i), &msg),
            i as u64 + 3,
            1002 + i as u64,
            None,
        );
    }
    let msg = make_msg(b"ex2-msg");
    engine.apply_command(
        &MqCommand::set_retained(ex2, "other/key", &msg),
        8,
        1007,
        None,
    );

    let snap = engine.snapshot();
    let snap_bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();

    let mut sm1 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);
    sm1.install_snapshot(
        &make_snapshot_meta(8, "multi-retained"),
        Cursor::new(snap_bytes),
    )
    .await
    .unwrap();
    drop(sm1);

    // Recover
    let mut sm2 = MqStateMachine::new(make_engine()).with_manifest(manifest.clone(), 1);
    let (la, _) = sm2.applied_state().await.unwrap();
    assert_eq!(la.unwrap().index, 8);

    let s = sm2.snapshot();
    assert_eq!(s.exchanges.len(), 2);

    let total_retained: usize = s.exchanges.iter().map(|e| e.retained.len()).sum();
    assert_eq!(total_retained, 6, "5 on ex1 + 1 on ex2");

    manifest.shutdown();
}

// =============================================================================
// Codec round-trip tests
// =============================================================================

#[test]
fn test_set_retained_codec() {
    let msg = make_msg(b"codec-test");
    let cmd = MqCommand::set_retained(42, "sensors/temp", &msg);

    assert_eq!(cmd.tag(), MqCommand::TAG_SET_RETAINED);
    let v = cmd.as_set_retained();
    assert_eq!(v.exchange_id(), 42);
    assert_eq!(v.routing_key(), "sensors/temp");
    assert!(!v.message().is_empty());
}

#[test]
fn test_get_retained_codec_with_filter() {
    let cmd = MqCommand::get_retained(42, Some("sensors/+"));

    assert_eq!(cmd.tag(), MqCommand::TAG_GET_RETAINED);
    let v = cmd.as_get_retained();
    assert_eq!(v.exchange_id(), 42);
    assert_eq!(v.routing_key_filter(), Some("sensors/+".to_string()));
}

#[test]
fn test_get_retained_codec_no_filter() {
    let cmd = MqCommand::get_retained(42, None);

    assert_eq!(cmd.tag(), MqCommand::TAG_GET_RETAINED);
    let v = cmd.as_get_retained();
    assert_eq!(v.exchange_id(), 42);
    assert_eq!(v.routing_key_filter(), None);
}

#[test]
fn test_delete_retained_codec() {
    let cmd = MqCommand::delete_retained(42, "sensors/temp");

    assert_eq!(cmd.tag(), MqCommand::TAG_DELETE_RETAINED);
    let v = cmd.as_delete_retained();
    assert_eq!(v.exchange_id(), 42);
    assert_eq!(v.routing_key(), "sensors/temp");
}

// =============================================================================
// Display formatting
// =============================================================================

#[test]
fn test_set_retained_display() {
    let msg = make_msg(b"data");
    let cmd = MqCommand::set_retained(42, "sensors/temp", &msg);
    let s = format!("{}", cmd);
    assert!(s.contains("SetRetained"));
    assert!(s.contains("42"));
    assert!(s.contains("sensors/temp"));
}

#[test]
fn test_get_retained_display() {
    let cmd = MqCommand::get_retained(42, Some("sensors/+"));
    let s = format!("{}", cmd);
    assert!(s.contains("GetRetained"));
    assert!(s.contains("42"));
}

#[test]
fn test_delete_retained_display() {
    let cmd = MqCommand::delete_retained(42, "sensors/temp");
    let s = format!("{}", cmd);
    assert!(s.contains("DeleteRetained"));
    assert!(s.contains("42"));
    assert!(s.contains("sensors/temp"));
}

// =============================================================================
// Raft encode/decode round-trip
// =============================================================================

#[test]
fn test_set_retained_raft_encode_decode() {
    use bisque_raft::codec::{Decode, Encode};

    let msg = make_msg(b"raft-test");
    let cmd = MqCommand::set_retained(42, "key", &msg);

    let mut buf = Vec::new();
    cmd.encode(&mut buf).unwrap();

    let decoded = MqCommand::decode(&mut &buf[..]).unwrap();
    assert_eq!(decoded.tag(), MqCommand::TAG_SET_RETAINED);
    let v = decoded.as_set_retained();
    assert_eq!(v.exchange_id(), 42);
    assert_eq!(v.routing_key(), "key");
}

#[test]
fn test_get_retained_raft_encode_decode() {
    use bisque_raft::codec::{Decode, Encode};

    let cmd = MqCommand::get_retained(99, Some("pattern/#"));

    let mut buf = Vec::new();
    cmd.encode(&mut buf).unwrap();

    let decoded = MqCommand::decode(&mut &buf[..]).unwrap();
    assert_eq!(decoded.tag(), MqCommand::TAG_GET_RETAINED);
    let v = decoded.as_get_retained();
    assert_eq!(v.exchange_id(), 99);
    assert_eq!(v.routing_key_filter(), Some("pattern/#".to_string()));
}

#[test]
fn test_delete_retained_raft_encode_decode() {
    use bisque_raft::codec::{Decode, Encode};

    let cmd = MqCommand::delete_retained(99, "del/key");

    let mut buf = Vec::new();
    cmd.encode(&mut buf).unwrap();

    let decoded = MqCommand::decode(&mut &buf[..]).unwrap();
    assert_eq!(decoded.tag(), MqCommand::TAG_DELETE_RETAINED);
    let v = decoded.as_delete_retained();
    assert_eq!(v.exchange_id(), 99);
    assert_eq!(v.routing_key(), "del/key");
}

// =============================================================================
// MqResponse::RetainedMessages codec round-trip
// =============================================================================

#[test]
fn test_retained_messages_response_encode_decode() {
    use bisque_raft::codec::{Decode, Encode};

    let resp = MqResponse::RetainedMessages {
        messages: vec![
            RetainedEntry {
                routing_key: Bytes::from_static(b"sensors/temp"),
                message: Bytes::from_static(b"22.5"),
            },
            RetainedEntry {
                routing_key: Bytes::from_static(b"sensors/humidity"),
                message: Bytes::from_static(b"60"),
            },
        ],
    };

    let mut buf = Vec::new();
    resp.encode(&mut buf).unwrap();

    let decoded = MqResponse::decode(&mut &buf[..]).unwrap();
    match decoded {
        MqResponse::RetainedMessages { messages } => {
            assert_eq!(messages.len(), 2);
            assert_eq!(&messages[0].routing_key[..], b"sensors/temp");
            assert_eq!(&messages[0].message[..], b"22.5");
            assert_eq!(&messages[1].routing_key[..], b"sensors/humidity");
            assert_eq!(&messages[1].message[..], b"60");
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

#[test]
fn test_retained_messages_response_empty() {
    use bisque_raft::codec::{Decode, Encode};

    let resp = MqResponse::RetainedMessages { messages: vec![] };

    let mut buf = Vec::new();
    resp.encode(&mut buf).unwrap();

    let decoded = MqResponse::decode(&mut &buf[..]).unwrap();
    match decoded {
        MqResponse::RetainedMessages { messages } => {
            assert!(messages.is_empty());
        }
        other => panic!("expected RetainedMessages, got {:?}", other),
    }
}

// =============================================================================
// Batch commands with retained
// =============================================================================

#[test]
fn test_batch_with_retained_commands() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg1 = make_msg(b"batch-1");
    let msg2 = make_msg(b"batch-2");

    let batch = MqCommand::batch(&[
        MqCommand::set_retained(exchange_id, "a", &msg1),
        MqCommand::set_retained(exchange_id, "b", &msg2),
    ]);

    let resp = engine.apply_command(&batch, 2, 1001, None);
    match resp {
        MqResponse::BatchResponse(resps) => {
            assert_eq!(resps.len(), 2);
            assert!(matches!(resps[0], MqResponse::Ok));
            assert!(matches!(resps[1], MqResponse::Ok));
        }
        other => panic!("expected BatchResponse, got {:?}", other),
    }

    assert_eq!(get_retained_count(&mut engine, exchange_id, 3), 2);
}

#[test]
fn test_batch_set_and_delete_retained() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Pre-set a retained message
    let msg = make_msg(b"to-delete");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "old-key", &msg),
        2,
        1001,
        None,
    );

    // Batch: set new + delete old
    let new_msg = make_msg(b"new");
    let batch = MqCommand::batch(&[
        MqCommand::set_retained(exchange_id, "new-key", &new_msg),
        MqCommand::delete_retained(exchange_id, "old-key"),
    ]);

    let resp = engine.apply_command(&batch, 3, 1002, None);
    match resp {
        MqResponse::BatchResponse(resps) => {
            assert_eq!(resps.len(), 2);
            assert!(matches!(resps[0], MqResponse::Ok));
            assert!(matches!(resps[1], MqResponse::Ok));
        }
        other => panic!("expected BatchResponse, got {:?}", other),
    }

    // Only new-key should remain
    let entries = get_retained(&mut engine, exchange_id, None, 4);
    assert_eq!(entries.len(), 1);
    assert_eq!(&entries[0].routing_key[..], b"new-key");
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn test_retained_large_message() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // 1MB message
    let large_payload = vec![0xABu8; 1024 * 1024];
    let msg = make_msg(&large_payload);
    let resp = engine.apply_command(
        &MqCommand::set_retained(exchange_id, "large", &msg),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    // Retrieve it
    let entries = get_retained(&mut engine, exchange_id, Some("large"), 3);
    assert_eq!(entries.len(), 1);
    assert!(entries[0].message.len() > 1024 * 1024); // includes FlatMessage header
}

#[test]
fn test_retained_unicode_routing_key() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"unicode-data");
    let key = "传感器/温度/房间1";
    let resp = engine.apply_command(
        &MqCommand::set_retained(exchange_id, key, &msg),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    let entries = get_retained(&mut engine, exchange_id, Some(key), 3);
    assert_eq!(entries.len(), 1);
    assert_eq!(std::str::from_utf8(&entries[0].routing_key).unwrap(), key);
}

#[test]
fn test_retained_segment_provenance_via_snapshot() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Set with segment_id to simulate mmap-backed
    let msg = make_msg(b"mmap-data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key", &msg),
        2,
        1001,
        Some(42),
    );

    // After snapshot restore, retained values are heap-backed (segment_id=None)
    // because snapshot serialization doesn't preserve segment provenance.
    let snap = engine.snapshot();
    assert_eq!(snap.exchanges[0].retained.len(), 1);

    let mut engine2 = make_engine();
    engine2.restore(snap);

    // The data is still accessible after restore
    let snap2 = engine2.snapshot();
    let restored_ex_id = snap2.exchanges[0].meta.exchange_id;
    assert_eq!(get_retained_count(&mut engine2, restored_ex_id, 10), 1);
}

// =============================================================================
// Snapshot builder with retained
// =============================================================================

#[tokio::test]
async fn test_snapshot_builder_includes_retained() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"builder-retained");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key", &msg),
        2,
        1001,
        None,
    );

    let snap_bytes =
        bincode::serde::encode_to_vec(&engine.snapshot(), bincode::config::standard()).unwrap();

    let mut sm = MqStateMachine::new(make_engine());
    sm.install_snapshot(
        &make_snapshot_meta(2, "builder-test"),
        Cursor::new(snap_bytes),
    )
    .await
    .unwrap();

    // Build snapshot via builder
    let mut builder = sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    // Deserialize and verify retained messages are included
    let data = snapshot.snapshot.into_inner();
    let (snap, _): (MqSnapshotData, _) =
        bincode::serde::decode_from_slice(&data, bincode::config::standard()).unwrap();
    assert_eq!(snap.exchanges.len(), 1);
    assert_eq!(snap.exchanges[0].retained.len(), 1);
}

// =============================================================================
// Retained messages across exchange types
// =============================================================================

#[test]
fn test_retained_on_direct_exchange() {
    let mut engine = make_engine();
    let exchange_id =
        create_exchange_typed(&mut engine, "direct-ex", ExchangeType::Direct, 1, 1000);

    let msg = make_msg(b"direct-retained");
    let resp = engine.apply_command(
        &MqCommand::set_retained(exchange_id, "exact-key", &msg),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    let entries = get_retained(&mut engine, exchange_id, Some("exact-key"), 3);
    assert_eq!(entries.len(), 1);
}

#[test]
fn test_retained_on_fanout_exchange() {
    let mut engine = make_engine();
    let exchange_id =
        create_exchange_typed(&mut engine, "fanout-ex", ExchangeType::Fanout, 1, 1000);

    let msg = make_msg(b"fanout-retained");
    let resp = engine.apply_command(
        &MqCommand::set_retained(exchange_id, "any-key", &msg),
        2,
        1001,
        None,
    );
    assert!(matches!(resp, MqResponse::Ok));

    let entries = get_retained(&mut engine, exchange_id, None, 3);
    assert_eq!(entries.len(), 1);
}

// =============================================================================
// Stress / many retained messages
// =============================================================================

#[test]
fn test_retained_many_keys() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    // Set 100 retained messages
    for i in 0..100u64 {
        let msg = make_msg(format!("msg-{}", i).as_bytes());
        let key = format!("level1/level2/sensor{}", i);
        engine.apply_command(
            &MqCommand::set_retained(exchange_id, &key, &msg),
            i + 2,
            1001 + i,
            None,
        );
    }

    // Get all
    assert_eq!(get_retained_count(&mut engine, exchange_id, 200), 100);

    // Wildcard filter
    let entries = get_retained(&mut engine, exchange_id, Some("level1/level2/+"), 201);
    assert_eq!(entries.len(), 100);

    // Delete half
    for i in 0..50u64 {
        let key = format!("level1/level2/sensor{}", i);
        engine.apply_command(
            &MqCommand::delete_retained(exchange_id, &key),
            300 + i,
            2000 + i,
            None,
        );
    }

    assert_eq!(get_retained_count(&mut engine, exchange_id, 400), 50);
}

// =============================================================================
// Snapshot with retained after deletes
// =============================================================================

#[test]
fn test_snapshot_after_delete_retained() {
    let mut engine = make_engine();
    let exchange_id = create_exchange(&mut engine, "mqtt/exchange", 1, 1000);

    let msg = make_msg(b"data");
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key1", &msg),
        2,
        1001,
        None,
    );
    engine.apply_command(
        &MqCommand::set_retained(exchange_id, "key2", &msg),
        3,
        1002,
        None,
    );
    engine.apply_command(
        &MqCommand::delete_retained(exchange_id, "key1"),
        4,
        1003,
        None,
    );

    let snap = engine.snapshot();
    assert_eq!(snap.exchanges[0].retained.len(), 1);
    assert_eq!(&snap.exchanges[0].retained[0].routing_key[..], b"key2");

    // Restore and verify
    let mut engine2 = make_engine();
    engine2.restore(snap);
    let snap2 = engine2.snapshot();
    assert_eq!(snap2.exchanges[0].retained.len(), 1);
}
