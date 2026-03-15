//! Integration tests for the async apply system.
//!
//! Tests exercise the public API of the async apply types: HighWaterMark,
//! ResponseEntry, ClientRegistry, and PendingRequests. The full end-to-end
//! worker pipeline (AsyncApplyManager + SegmentPrefetcher) requires a running
//! raft log and is tested via the state machine integration tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{BufMut, BytesMut};

use bisque_mq::async_apply::{
    ClientRegistry, HighWaterMark, PendingRequest, PendingRequests, ResponseCallback, ResponseEntry,
};
use bisque_mq::types::MqResponse;

// =============================================================================
// HighWaterMark integration tests
// =============================================================================

#[tokio::test]
async fn hwm_producer_consumer_pattern() {
    let hwm = Arc::new(HighWaterMark::new(0));
    let hwm_consumer = Arc::clone(&hwm);

    // Simulate a consumer that reads sequentially.
    let consumer = tokio::spawn(async move {
        let mut cursor = 0u64;
        let mut values = Vec::new();
        loop {
            let new_hwm = hwm_consumer.wait_for(cursor).await;
            if new_hwm == u64::MAX {
                break;
            }
            values.push(new_hwm);
            cursor = new_hwm;
        }
        values
    });

    // Producer advances HWM in bursts.
    for i in 1..=5u64 {
        hwm.advance(i * 10);
        tokio::task::yield_now().await;
    }
    hwm.advance(u64::MAX); // shutdown

    let values = consumer.await.unwrap();
    // Consumer should have seen monotonically increasing values.
    for window in values.windows(2) {
        assert!(
            window[1] > window[0],
            "values must be monotonically increasing"
        );
    }
    // Last value before shutdown should be 50.
    assert_eq!(*values.last().unwrap(), 50);
}

#[tokio::test]
async fn hwm_concurrent_producers() {
    let hwm = Arc::new(HighWaterMark::new(0));
    let hwm_reader = Arc::clone(&hwm);

    // Multiple producers advancing concurrently.
    let mut handles = Vec::new();
    for i in 1..=4 {
        let hwm2 = Arc::clone(&hwm);
        handles.push(tokio::spawn(async move {
            for j in 1..=10u64 {
                hwm2.advance(i * 100 + j);
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Final HWM should be one of the last values set.
    let final_val = hwm_reader.current();
    assert!(final_val > 0, "HWM should have been advanced");
}

// =============================================================================
// ResponseEntry integration tests
// =============================================================================

#[test]
fn response_entry_roundtrip_all_variants() {
    // Ok
    let entry = ResponseEntry::from_response(1, &MqResponse::Ok);
    assert_eq!(entry.tag(), ResponseEntry::TAG_OK);
    assert!(entry.is_ok());
    assert_eq!(entry.log_index(), 1);

    // Published
    let entry = ResponseEntry::from_response(
        2,
        &MqResponse::Published {
            base_offset: 100,
            count: 50,
        },
    );
    assert_eq!(entry.tag(), ResponseEntry::TAG_PUBLISHED);
    assert_eq!(entry.log_index(), 2);
    let base_offset = u64::from_le_bytes(entry.buf[16..24].try_into().unwrap());
    let count = u64::from_le_bytes(entry.buf[24..32].try_into().unwrap());
    assert_eq!(base_offset, 100);
    assert_eq!(count, 50);

    // EntityCreated
    let entry = ResponseEntry::from_response(3, &MqResponse::EntityCreated { id: 42 });
    assert_eq!(entry.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    let entity_id = u64::from_le_bytes(entry.buf[16..24].try_into().unwrap());
    assert_eq!(entity_id, 42);
}

#[test]
fn response_entry_alignment_stress() {
    // Test many message lengths to verify 8-byte alignment invariant.
    for len in 0..64 {
        let msg: String = "x".repeat(len);
        let entry = ResponseEntry::error(len as u64, 0, &msg);
        assert_eq!(
            entry.buf.len() % 8,
            0,
            "alignment broken for msg len {}",
            len
        );
        assert_eq!(entry.log_index(), len as u64);
    }
}

#[test]
fn response_entry_large_log_index() {
    let entry = ResponseEntry::ok(u64::MAX - 1);
    assert_eq!(entry.log_index(), u64::MAX - 1);

    let entry = ResponseEntry::published(u64::MAX / 2, u64::MAX, u64::MAX);
    assert_eq!(entry.log_index(), u64::MAX / 2);
}

// =============================================================================
// ClientRegistry integration tests
// =============================================================================

#[tokio::test]
async fn client_registry_concurrent_register_unregister() {
    let registry = ClientRegistry::new(4, 256);

    let received_total = Arc::new(AtomicUsize::new(0));
    let all_done = Arc::new(tokio::sync::Notify::new());

    let mut ids = Vec::new();
    for _ in 0..10 {
        let count = Arc::clone(&received_total);
        let done = Arc::clone(&all_done);
        let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
            if !is_done {
                if count.fetch_add(1, Ordering::Relaxed) + 1 >= 10 {
                    done.notify_one();
                }
            }
        });
        ids.push(registry.register(cb));
    }

    // All IDs should be unique.
    let id_set: std::collections::HashSet<u32> = ids.iter().copied().collect();
    assert_eq!(id_set.len(), 10);

    for &id in &ids {
        let p = (id as usize) % registry.num_partitions();
        let mut frame = BytesMut::with_capacity(24);
        ResponseEntry::encode_frame_into(&mut frame, id, id as u64, &MqResponse::Ok);
        registry.send_to_partition(p, frame.freeze());
    }
    all_done.notified().await;

    for id in ids {
        registry.unregister(id);
    }
}

#[tokio::test]
async fn client_registry_high_throughput() {
    let registry = ClientRegistry::new(4, 4096);

    let received = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let received_cb = Arc::clone(&received);
    let done_cb = Arc::clone(&done);
    let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
        if !is_done {
            if received_cb.fetch_add(1, Ordering::Relaxed) + 1 >= 1000 {
                done_cb.notify_one();
            }
        }
    });
    let id = registry.register(cb);

    let p = (id as usize) % registry.num_partitions();
    for i in 0..1000u64 {
        let mut frame = BytesMut::with_capacity(24);
        ResponseEntry::encode_frame_into(&mut frame, id, i, &MqResponse::Ok);
        registry.send_to_partition(p, frame.freeze());
    }
    done.notified().await;
    assert_eq!(received.load(Ordering::Relaxed), 1000);
    registry.unregister(id);
}

// =============================================================================
// PendingRequests integration tests
// =============================================================================

#[test]
fn pending_requests_insert_overwrite() {
    let mut pending = PendingRequests::new();
    pending.insert(10, PendingRequest::Generic);
    pending.insert(
        10,
        PendingRequest::Publish {
            packet_id: 1,
            qos: 0,
        },
    );

    // The second insert should overwrite.
    match pending.remove(10) {
        Some(PendingRequest::Publish { packet_id, qos }) => {
            assert_eq!(packet_id, 1);
            assert_eq!(qos, 0);
        }
        _ => panic!("expected overwritten Publish"),
    }
}

#[test]
fn pending_requests_stress() {
    let mut pending = PendingRequests::new();

    // Insert many entries.
    for i in 0..1000u64 {
        pending.insert(i, PendingRequest::Generic);
    }
    assert_eq!(pending.len(), 1000);

    // Remove all.
    for i in 0..1000u64 {
        assert!(pending.remove(i).is_some());
    }
    assert!(pending.is_empty());
}
