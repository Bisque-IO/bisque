//! Comprehensive tests for the deduplication system.
//!
//! Covers: DedupWindow (papaya::HashMap<u128, u64>), DedupPreFilter (per-connection),
//! DedupIndex (global scc::TreeIndex), TopicState dedup integration,
//! bytes_to_dedup_key conversion, snapshot/restore, pruning, concurrency,
//! and local background GC.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bisque_mq::topic::{DedupPreFilter, DedupWindow, TopicMeta, TopicState, bytes_to_dedup_key};
use bisque_mq::types::{RetentionPolicy, TopicDedupConfig};

// =============================================================================
// Helper
// =============================================================================

fn default_dedup_config() -> TopicDedupConfig {
    TopicDedupConfig {
        window_secs: 60,
        max_entries: 100_000,
    }
}

fn make_dedup_window() -> DedupWindow {
    DedupWindow::new(&default_dedup_config())
}

fn make_dedup_topic(window_secs: u64) -> TopicState {
    let mut meta = TopicMeta::new(
        1,
        "dedup-test".to_string(),
        1000,
        RetentionPolicy::default(),
    );
    meta.dedup_config = Some(TopicDedupConfig {
        window_secs,
        max_entries: 100_000,
    });
    TopicState::new(meta, "test-catalog")
}

fn make_topic_no_dedup() -> TopicState {
    let meta = TopicMeta::new(2, "no-dedup".to_string(), 1000, RetentionPolicy::default());
    TopicState::new(meta, "test-catalog")
}

// =============================================================================
// bytes_to_dedup_key conversion
// =============================================================================

#[test]
fn bytes_to_dedup_key_empty() {
    let k = bytes_to_dedup_key(b"");
    assert_eq!(k, 0u128);
}

#[test]
fn bytes_to_dedup_key_single_byte() {
    let k = bytes_to_dedup_key(&[0xAB]);
    // 0xAB followed by 15 zero bytes, big-endian
    let expected = 0xAB_u128 << 120;
    assert_eq!(k, expected);
}

#[test]
fn bytes_to_dedup_key_exact_16_bytes() {
    let input: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let k = bytes_to_dedup_key(&input);
    assert_eq!(k, u128::from_be_bytes(input));
}

#[test]
fn bytes_to_dedup_key_truncates_longer_than_16() {
    let input = b"this is way longer than 16 bytes";
    let k = bytes_to_dedup_key(input);
    // Should equal first 16 bytes only
    let expected = bytes_to_dedup_key(&input[..16]);
    assert_eq!(k, expected);
}

#[test]
fn bytes_to_dedup_key_uuid_roundtrip() {
    // Simulate a UUID (16 bytes)
    let uuid_bytes: [u8; 16] = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];
    let k = bytes_to_dedup_key(&uuid_bytes);
    assert_eq!(k, u128::from_be_bytes(uuid_bytes));
}

#[test]
fn bytes_to_dedup_key_different_inputs_different_keys() {
    let k1 = bytes_to_dedup_key(b"alpha");
    let k2 = bytes_to_dedup_key(b"beta");
    assert_ne!(k1, k2);
}

#[test]
fn bytes_to_dedup_key_zero_padded_short_key() {
    let k_short = bytes_to_dedup_key(b"abc");
    let mut padded = [0u8; 16];
    padded[..3].copy_from_slice(b"abc");
    let k_padded = u128::from_be_bytes(padded);
    assert_eq!(k_short, k_padded);
}

// =============================================================================
// DedupWindow — basic operations
// =============================================================================

#[test]
fn dedup_window_first_insert_not_duplicate() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(1, 1000));
}

#[test]
fn dedup_window_second_insert_is_duplicate() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(42, 1000));
    assert!(dw.check_and_insert(42, 2000)); // same key, different ts — still dup
}

#[test]
fn dedup_window_different_keys_not_duplicates() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(1, 1000));
    assert!(!dw.check_and_insert(2, 1000));
    assert!(!dw.check_and_insert(3, 1000));
}

#[test]
fn dedup_window_same_key_same_timestamp() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(99, 5000));
    assert!(dw.check_and_insert(99, 5000));
}

#[test]
fn dedup_window_len() {
    let dw = make_dedup_window();
    assert_eq!(dw.len(), 0);
    dw.check_and_insert(1, 100);
    dw.check_and_insert(2, 200);
    dw.check_and_insert(3, 300);
    assert_eq!(dw.len(), 3);
    // Inserting duplicate doesn't increase len
    dw.check_and_insert(1, 400);
    assert_eq!(dw.len(), 3);
}

// =============================================================================
// DedupWindow — pruning
// =============================================================================

#[test]
fn prune_removes_old_entries() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 1000);
    dw.check_and_insert(2, 2000);
    dw.check_and_insert(3, 3000);

    let removed = dw.prune(2500);
    assert_eq!(removed, 2); // keys 1 (ts=1000) and 2 (ts=2000)
    assert_eq!(dw.len(), 1);

    // Key 3 still present
    assert!(dw.check_and_insert(3, 4000));
    // Keys 1 and 2 are gone — re-insertable
    assert!(!dw.check_and_insert(1, 4000));
    assert!(!dw.check_and_insert(2, 4000));
}

#[test]
fn prune_with_zero_threshold_removes_nothing() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 1000);
    let removed = dw.prune(0);
    assert_eq!(removed, 0);
    assert_eq!(dw.len(), 1);
}

#[test]
fn prune_all_entries() {
    let dw = make_dedup_window();
    for i in 0..100 {
        dw.check_and_insert(i, i as u64 * 10);
    }
    assert_eq!(dw.len(), 100);
    let removed = dw.prune(u64::MAX);
    assert_eq!(removed, 100);
    assert_eq!(dw.len(), 0);
}

#[test]
fn prune_empty_window() {
    let dw = make_dedup_window();
    let removed = dw.prune(5000);
    assert_eq!(removed, 0);
}

#[test]
fn prune_exact_boundary() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 100);
    dw.check_and_insert(2, 200);
    // prune(200) removes entries with ts < 200, so only key 1
    let removed = dw.prune(200);
    assert_eq!(removed, 1);
    assert!(dw.check_and_insert(2, 300)); // key 2 still exists
}

#[test]
fn prune_repeated_calls_idempotent() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 100);
    dw.check_and_insert(2, 200);
    dw.prune(150);
    assert_eq!(dw.len(), 1);
    // Second prune with same threshold — nothing more to remove
    let removed = dw.prune(150);
    assert_eq!(removed, 0);
    assert_eq!(dw.len(), 1);
}

#[test]
fn prune_then_reinsert_same_key() {
    let dw = make_dedup_window();
    dw.check_and_insert(42, 1000);
    assert!(dw.check_and_insert(42, 1500)); // duplicate

    dw.prune(2000); // remove key 42

    // Now key 42 can be inserted again
    assert!(!dw.check_and_insert(42, 3000));
    assert_eq!(dw.len(), 1);
}

// =============================================================================
// DedupWindow — snapshot / restore
// =============================================================================

#[test]
fn snapshot_empty() {
    let dw = make_dedup_window();
    let snap = dw.snapshot();
    assert!(snap.is_empty());
}

#[test]
fn snapshot_roundtrip() {
    let dw = make_dedup_window();
    dw.check_and_insert(10, 100);
    dw.check_and_insert(20, 200);
    dw.check_and_insert(30, 300);

    let snap = dw.snapshot();
    assert_eq!(snap.len(), 3);

    let dw2 = make_dedup_window();
    dw2.restore(snap);

    // All keys should be duplicates in the restored window
    assert!(dw2.check_and_insert(10, 400));
    assert!(dw2.check_and_insert(20, 400));
    assert!(dw2.check_and_insert(30, 400));
    assert_eq!(dw2.len(), 3);
}

#[test]
fn restore_merges_with_existing() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 100);

    let entries = vec![(2, 200), (3, 300)];
    dw.restore(entries);

    assert_eq!(dw.len(), 3);
    assert!(dw.check_and_insert(1, 500));
    assert!(dw.check_and_insert(2, 500));
    assert!(dw.check_and_insert(3, 500));
}

#[test]
fn snapshot_preserves_timestamps_for_pruning() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 100);
    dw.check_and_insert(2, 500);

    let snap = dw.snapshot();

    let dw2 = make_dedup_window();
    dw2.restore(snap);

    // Prune at 300 — should remove key 1 (ts=100) but keep key 2 (ts=500)
    let removed = dw2.prune(300);
    assert_eq!(removed, 1);
    assert!(!dw2.check_and_insert(1, 600)); // key 1 was pruned, can reinsert
    assert!(dw2.check_and_insert(2, 600)); // key 2 still present
}

// =============================================================================
// TopicState dedup integration
// =============================================================================

#[test]
fn topic_no_dedup_always_returns_false() {
    let topic = make_topic_no_dedup();
    let key = bytes_to_dedup_key(b"anything");
    assert!(!topic.check_dedup(key, 1000));
    assert!(!topic.check_dedup(key, 2000)); // same key — still not duplicate
}

#[test]
fn topic_no_dedup_prune_returns_zero() {
    let topic = make_topic_no_dedup();
    assert_eq!(topic.prune_dedup(u64::MAX), 0);
}

#[test]
fn topic_with_dedup_detects_duplicates() {
    let topic = make_dedup_topic(60);
    let key = bytes_to_dedup_key(b"msg-id-001");
    assert!(!topic.check_dedup(key, 10_000));
    assert!(topic.check_dedup(key, 10_500));
}

#[test]
fn topic_dedup_prune_removes_expired() {
    let topic = make_dedup_topic(60);
    let key = bytes_to_dedup_key(b"msg-001");

    topic.check_dedup(key, 10_000);
    assert_eq!(topic.prune_dedup(10_001), 1);
    // Key can now be re-inserted
    assert!(!topic.check_dedup(key, 20_000));
}

#[test]
fn topic_dedup_multiple_keys_independent() {
    let topic = make_dedup_topic(60);
    let k1 = bytes_to_dedup_key(b"aaa");
    let k2 = bytes_to_dedup_key(b"bbb");
    let k3 = bytes_to_dedup_key(b"ccc");

    assert!(!topic.check_dedup(k1, 1000));
    assert!(!topic.check_dedup(k2, 2000));
    assert!(!topic.check_dedup(k3, 3000));

    // All are duplicates
    assert!(topic.check_dedup(k1, 4000));
    assert!(topic.check_dedup(k2, 4000));
    assert!(topic.check_dedup(k3, 4000));

    // Prune only the oldest
    topic.prune_dedup(1500);
    assert!(!topic.check_dedup(k1, 5000)); // pruned
    assert!(topic.check_dedup(k2, 5000)); // still present
    assert!(topic.check_dedup(k3, 5000)); // still present
}

// =============================================================================
// Concurrency
// =============================================================================

#[test]
fn concurrent_check_and_insert_no_panic() {
    let dw = Arc::new(make_dedup_window());
    let mut handles = Vec::new();

    for t in 0..10 {
        let dw = Arc::clone(&dw);
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = (t * 10_000 + i) as u128;
                dw.check_and_insert(key, (i * 10) as u64);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 10 threads × 1000 unique keys = 10_000
    assert_eq!(dw.len(), 10_000);
}

#[test]
fn concurrent_insert_and_prune() {
    let dw = Arc::new(make_dedup_window());
    let mut handles = Vec::new();

    // Inserters
    for t in 0..5 {
        let dw = Arc::clone(&dw);
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                let key = (t * 1000 + i) as u128;
                let ts = (i * 100) as u64;
                dw.check_and_insert(key, ts);
            }
        }));
    }

    // Pruners
    for _ in 0..3 {
        let dw = Arc::clone(&dw);
        handles.push(thread::spawn(move || {
            for threshold in (0..50_000).step_by(1000) {
                dw.prune(threshold);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Just verify no panic and len is consistent
    let _ = dw.len();
}

#[test]
fn concurrent_duplicate_detection() {
    // All threads try to insert the SAME key — exactly one should see "not duplicate"
    let dw = Arc::new(make_dedup_window());
    let key: u128 = 0xDEAD_BEEF;
    let mut handles = Vec::new();

    for _ in 0..20 {
        let dw = Arc::clone(&dw);
        handles.push(thread::spawn(move || dw.check_and_insert(key, 1000)));
    }

    let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let non_dup_count = results.iter().filter(|&&is_dup| !is_dup).count();

    // Exactly one thread should have been the first inserter
    assert_eq!(
        non_dup_count, 1,
        "exactly one thread should see non-duplicate"
    );
}

#[test]
fn concurrent_snapshot_during_inserts() {
    let dw = Arc::new(make_dedup_window());
    let mut handles = Vec::new();

    // Background inserter
    let dw_insert = Arc::clone(&dw);
    handles.push(thread::spawn(move || {
        for i in 0..5000 {
            dw_insert.check_and_insert(i as u128, i as u64 * 10);
        }
    }));

    // Concurrent snapshots
    for _ in 0..5 {
        let dw_snap = Arc::clone(&dw);
        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                let snap = dw_snap.snapshot();
                // Snapshot should be internally consistent
                for &(key, ts) in &snap {
                    assert!(key < 5000);
                    assert!(ts < 50_000);
                }
                thread::sleep(Duration::from_micros(10));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn dedup_key_zero_is_valid() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(0, 1000));
    assert!(dw.check_and_insert(0, 2000));
}

#[test]
fn dedup_key_max_u128() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(u128::MAX, 1000));
    assert!(dw.check_and_insert(u128::MAX, 2000));
}

#[test]
fn dedup_timestamp_zero() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(1, 0));
    assert!(dw.check_and_insert(1, 0));
}

#[test]
fn dedup_timestamp_max_u64() {
    let dw = make_dedup_window();
    assert!(!dw.check_and_insert(1, u64::MAX));
    assert!(dw.check_and_insert(1, u64::MAX));
}

#[test]
fn prune_with_max_u64_removes_everything() {
    let dw = make_dedup_window();
    for i in 0..50 {
        dw.check_and_insert(i, i as u64 * 1000);
    }
    let removed = dw.prune(u64::MAX);
    assert_eq!(removed, 50);
    assert_eq!(dw.len(), 0);
}

#[test]
fn large_number_of_keys() {
    let dw = make_dedup_window();
    let count = 50_000;
    for i in 0..count {
        assert!(!dw.check_and_insert(i as u128, i as u64));
    }
    assert_eq!(dw.len(), count);

    // All should be duplicates
    for i in 0..count {
        assert!(dw.check_and_insert(i as u128, (i + count) as u64));
    }

    // Prune half
    let removed = dw.prune((count / 2) as u64);
    assert_eq!(removed, count / 2);
    assert_eq!(dw.len(), count - count / 2);
}

#[test]
fn check_does_not_update_timestamp() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 100); // insert at ts=100
    dw.check_and_insert(1, 500); // duplicate check — should NOT update ts to 500

    // Prune at 200 — should remove key 1 because its ts is still 100
    let removed = dw.prune(200);
    assert_eq!(removed, 1);
    assert_eq!(dw.len(), 0);
}

// =============================================================================
// Stress: interleaved operations
// =============================================================================

#[test]
fn stress_insert_prune_snapshot_restore() {
    let dw = Arc::new(make_dedup_window());
    let mut handles = Vec::new();

    // Writer thread: insert keys with increasing timestamps
    let dw1 = Arc::clone(&dw);
    handles.push(thread::spawn(move || {
        for i in 0u128..10_000 {
            dw1.check_and_insert(i, i as u64);
        }
    }));

    // Pruner thread: prune progressively
    let dw2 = Arc::clone(&dw);
    handles.push(thread::spawn(move || {
        for threshold in (0..10_000).step_by(100) {
            dw2.prune(threshold);
        }
    }));

    // Snapshot thread: take periodic snapshots
    let dw3 = Arc::clone(&dw);
    handles.push(thread::spawn(move || {
        let mut snapshots = Vec::new();
        for _ in 0..50 {
            snapshots.push(dw3.snapshot());
            thread::sleep(Duration::from_micros(1));
        }
        // Verify all snapshots are internally consistent (keys and timestamps are finite)
        for snap in &snapshots {
            for &(key, _ts) in snap {
                assert!(key < 10_000);
            }
        }
    }));

    // Duplicate checker thread
    let dw4 = Arc::clone(&dw);
    handles.push(thread::spawn(move || {
        for i in 0u128..5_000 {
            // May or may not be duplicate depending on timing
            let _ = dw4.check_and_insert(i, i as u64 + 10_000);
        }
    }));

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// prune_dedup_local (background GC function)
// =============================================================================

#[test]
fn prune_dedup_local_with_no_topics() {
    use bisque_mq::config::MqConfig;
    use bisque_mq::metadata::MqMetadata;
    use bisque_mq::raft::prune_dedup_local;

    let config = MqConfig::new("/tmp");
    let md = MqMetadata::new(config.catalog_name.clone());
    prune_dedup_local(&md);
    // No panic, no-op
}

#[test]
fn prune_dedup_local_with_dedup_topic() {
    use bisque_mq::config::MqConfig;
    use bisque_mq::metadata::MqMetadata;
    use bisque_mq::raft::prune_dedup_local;

    let config = MqConfig::new("/tmp");
    let md = MqMetadata::new(config.catalog_name.clone());

    let mut meta = TopicMeta::new(1, "t1".to_string(), 1000, RetentionPolicy::default());
    meta.dedup_config = Some(TopicDedupConfig {
        window_secs: 60,
        max_entries: 100_000,
    });
    let topic = TopicState::new(meta, "test");
    let key = bytes_to_dedup_key(b"old-key");
    // Insert with timestamp 0 — guaranteed to be expired
    topic.check_dedup(key, 0);
    let topic = Arc::new(topic);
    md.topics.pin().insert(1, Arc::clone(&topic));

    prune_dedup_local(&md);

    // The key should have been pruned (ts=0 is well before now_ms - 60_000)
    assert!(!topic.check_dedup(key, 999_999_999));
}

#[test]
fn prune_dedup_local_skips_topics_without_dedup() {
    use bisque_mq::config::MqConfig;
    use bisque_mq::metadata::MqMetadata;
    use bisque_mq::raft::prune_dedup_local;

    let config = MqConfig::new("/tmp");
    let md = MqMetadata::new(config.catalog_name.clone());

    let meta = TopicMeta::new(1, "no-dedup".to_string(), 1000, RetentionPolicy::default());
    let topic = TopicState::new(meta, "test");
    md.topics.pin().insert(1, Arc::new(topic));

    // Should not panic
    prune_dedup_local(&md);
}

// =============================================================================
// Multiple topics — independent dedup windows
// =============================================================================

#[test]
fn dedup_windows_are_per_topic() {
    let topic_a = make_dedup_topic(60);
    let topic_b = make_dedup_topic(60);

    let key = bytes_to_dedup_key(b"shared-key");

    assert!(!topic_a.check_dedup(key, 1000)); // first in A
    assert!(!topic_b.check_dedup(key, 1000)); // first in B (independent)

    assert!(topic_a.check_dedup(key, 2000)); // duplicate in A
    assert!(topic_b.check_dedup(key, 2000)); // duplicate in B

    // Prune only topic A
    topic_a.prune_dedup(1500);
    assert!(!topic_a.check_dedup(key, 3000)); // A: pruned, can reinsert
    assert!(topic_b.check_dedup(key, 3000)); // B: still duplicate
}

// =============================================================================
// TopicDedupConfig window_secs semantics
// =============================================================================

#[test]
fn window_secs_zero_means_dedup_enabled_but_instant_expiry() {
    let topic = make_dedup_topic(0);
    let key = bytes_to_dedup_key(b"k1");

    // Insert at ts=1000
    assert!(!topic.check_dedup(key, 1000));
    // Still duplicate (prune hasn't run)
    assert!(topic.check_dedup(key, 1001));

    // Prune with window_secs=0 → before_ms = now_ms - 0 = now_ms
    // Everything with ts < now_ms gets pruned
    topic.prune_dedup(1002);
    assert!(!topic.check_dedup(key, 1003)); // pruned, can reinsert
}

// =============================================================================
// Inline GC (max_entries threshold)
// =============================================================================

#[test]
fn inline_gc_triggers_when_over_max_entries() {
    let config = TopicDedupConfig {
        window_secs: 10, // 10_000 ms window
        max_entries: 50,
    };
    let dw = DedupWindow::new(&config);

    // Insert 50 entries at ts=1000 (all within window).
    for i in 0u128..50 {
        assert!(!dw.check_and_insert(i, 1000));
    }
    assert_eq!(dw.len(), 50);

    // Insert key 50 at ts=20_000. This crosses max_entries,
    // triggering inline GC with before_ms = 20_000 - 10_000 = 10_000.
    // All 50 entries (ts=1000 < 10_000) are pruned before insert.
    assert!(!dw.check_and_insert(50, 20_000));
    // 50 old entries pruned, 1 new entry inserted.
    assert_eq!(dw.len(), 1);
}

#[test]
fn inline_gc_only_prunes_expired_entries() {
    let config = TopicDedupConfig {
        window_secs: 5, // 5_000 ms window
        max_entries: 10,
    };
    let dw = DedupWindow::new(&config);

    // Insert 5 entries at ts=1000 (old).
    for i in 0u128..5 {
        assert!(!dw.check_and_insert(i, 1000));
    }
    // Insert 5 more at ts=8000 (recent).
    for i in 5u128..10 {
        assert!(!dw.check_and_insert(i, 8000));
    }
    assert_eq!(dw.len(), 10);

    // Insert key 10 at ts=9000. Over max_entries,
    // inline GC prunes where ts < 9000 - 5000 = 4000.
    // Only the 5 entries at ts=1000 are pruned.
    assert!(!dw.check_and_insert(10, 9000));
    assert_eq!(dw.len(), 6); // 5 recent + 1 new
}

#[test]
fn inline_gc_does_not_trigger_below_threshold() {
    let config = TopicDedupConfig {
        window_secs: 60,
        max_entries: 100,
    };
    let dw = DedupWindow::new(&config);

    // Insert 50 entries — below max_entries.
    for i in 0u128..50 {
        dw.check_and_insert(i, 1000);
    }
    assert_eq!(dw.len(), 50);

    // Insert one more — still below threshold, no GC.
    dw.check_and_insert(50, 1000);
    assert_eq!(dw.len(), 51);
}

#[test]
fn inline_gc_with_max_entries_zero_means_no_limit() {
    let config = TopicDedupConfig {
        window_secs: 60,
        max_entries: 0,
    };
    let dw = DedupWindow::new(&config);

    // Insert many entries — no inline GC should trigger.
    for i in 0u128..1000 {
        dw.check_and_insert(i, 1000);
    }
    assert_eq!(dw.len(), 1000);
}

#[test]
fn inline_gc_preserves_duplicates_of_surviving_keys() {
    let config = TopicDedupConfig {
        window_secs: 5,
        max_entries: 5,
    };
    let dw = DedupWindow::new(&config);

    // Insert 5 entries at ts=8000 (within window).
    for i in 0u128..5 {
        dw.check_and_insert(i, 8000);
    }

    // Insert key 5 at ts=9000 — triggers inline GC.
    // GC threshold = 9000 - 5000 = 4000. All entries at ts=8000 survive.
    dw.check_and_insert(5, 9000);
    assert_eq!(dw.len(), 6);

    // Original keys are still duplicates.
    assert!(dw.check_and_insert(0, 9000));
    assert!(dw.check_and_insert(4, 9000));
}

#[test]
fn inline_gc_repeated_triggers() {
    let config = TopicDedupConfig {
        window_secs: 1, // 1_000 ms
        max_entries: 10,
    };
    let dw = DedupWindow::new(&config);

    // Wave 1: 10 entries at ts=1000.
    for i in 0u128..10 {
        dw.check_and_insert(i, 1000);
    }
    assert_eq!(dw.len(), 10);

    // Wave 2: insert at ts=5000 — triggers GC, prunes wave 1.
    dw.check_and_insert(100, 5000);
    assert_eq!(dw.len(), 1);

    // Fill back up to 10.
    for i in 101u128..110 {
        dw.check_and_insert(i, 5000);
    }
    assert_eq!(dw.len(), 10);

    // Wave 3: insert at ts=10000 — triggers GC again, prunes wave 2.
    dw.check_and_insert(200, 10_000);
    assert_eq!(dw.len(), 1);
}

#[test]
fn topic_inline_gc_via_check_dedup() {
    let mut meta = TopicMeta::new(1, "gc-topic".to_string(), 1000, RetentionPolicy::default());
    meta.dedup_config = Some(TopicDedupConfig {
        window_secs: 5,
        max_entries: 20,
    });
    let topic = TopicState::new(meta, "test");

    // Fill to max_entries with old timestamps.
    for i in 0u128..20 {
        topic.check_dedup(i, 1000);
    }

    // Next insert at a much later time — should trigger inline GC.
    assert!(!topic.check_dedup(99, 20_000));

    // Old entries should have been pruned. Key 0 is re-insertable.
    assert!(!topic.check_dedup(0, 20_000));
}

// =============================================================================
// DedupIndex (global scc::TreeIndex reverse index)
// =============================================================================

use bisque_mq::topic::DedupIndex;

#[test]
fn dedup_index_empty() {
    let idx = DedupIndex::new();
    assert!(idx.is_empty());
    assert!(idx.prune_before(u64::MAX).is_empty());
}

#[test]
fn dedup_index_insert_and_prune() {
    let idx = DedupIndex::new();
    idx.insert(1000, 1, 100);
    idx.insert(2000, 1, 200);
    idx.insert(3000, 1, 300);
    assert!(!idx.is_empty());

    // Prune entries before ts=2500 — should remove (1000, 1, 100) and (2000, 1, 200).
    let expired = idx.prune_before(2500);
    assert_eq!(expired.len(), 2);
    assert!(expired.contains(&(1, 100)));
    assert!(expired.contains(&(1, 200)));

    // Only (3000, 1, 300) remains.
    assert!(!idx.is_empty());

    // Prune remaining.
    let expired = idx.prune_before(4000);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], (1, 300));
    assert!(idx.is_empty());
}

#[test]
fn dedup_index_multi_topic() {
    let idx = DedupIndex::new();
    // Topic 1 entries at ts=1000.
    idx.insert(1000, 1, 10);
    idx.insert(1000, 1, 20);
    // Topic 2 entries at ts=1000.
    idx.insert(1000, 2, 10);
    // Topic 2 entry at ts=3000.
    idx.insert(3000, 2, 30);

    // Prune before 2000 — removes all ts=1000 entries across both topics.
    let expired = idx.prune_before(2000);
    assert_eq!(expired.len(), 3);
    assert!(expired.contains(&(1, 10)));
    assert!(expired.contains(&(1, 20)));
    assert!(expired.contains(&(2, 10)));

    // Only topic 2's ts=3000 entry remains.
    let remaining = idx.prune_before(u64::MAX);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0], (2, 30));
}

#[test]
fn dedup_index_prune_empty_range() {
    let idx = DedupIndex::new();
    idx.insert(5000, 1, 1);

    // Prune before 1000 — nothing expired.
    let expired = idx.prune_before(1000);
    assert!(expired.is_empty());
    assert!(!idx.is_empty());
}

#[test]
fn dedup_index_remove_single() {
    let idx = DedupIndex::new();
    idx.insert(1000, 1, 100);
    idx.insert(2000, 1, 200);

    idx.remove(1000, 1, 100);
    // Only (2000, 1, 200) remains.
    let expired = idx.prune_before(u64::MAX);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], (1, 200));
}

#[test]
fn dedup_index_duplicate_insert_is_idempotent() {
    let idx = DedupIndex::new();
    idx.insert(1000, 1, 100);
    idx.insert(1000, 1, 100); // same key — TreeIndex rejects duplicates

    let expired = idx.prune_before(2000);
    assert_eq!(expired.len(), 1);
}

#[test]
fn dedup_index_ordering_preserves_topic_isolation() {
    let idx = DedupIndex::new();
    // Same timestamp, different topics.
    idx.insert(1000, 1, 1);
    idx.insert(1000, 2, 1);
    idx.insert(1000, 3, 1);

    let expired = idx.prune_before(1001);
    assert_eq!(expired.len(), 3);
    // All three topics' entries are pruned.
    let topic_ids: Vec<u64> = expired.iter().map(|(tid, _)| *tid).collect();
    assert!(topic_ids.contains(&1));
    assert!(topic_ids.contains(&2));
    assert!(topic_ids.contains(&3));
}

// =============================================================================
// DedupIndex + DedupWindow integration (indexed insert + prune)
// =============================================================================

#[test]
fn indexed_insert_populates_both_forward_and_reverse() {
    let idx = DedupIndex::new();
    let dw = make_dedup_window();

    // Non-indexed insert — only forward map.
    assert!(!dw.check_and_insert(1, 1000));
    assert!(idx.is_empty());

    // Indexed insert — both forward map and reverse index.
    assert!(!dw.check_and_insert_indexed(2, 2000, 42, &idx));
    assert!(!idx.is_empty());
    assert_eq!(dw.len(), 2);

    // Key 2 is a duplicate in forward map.
    assert!(dw.check_and_insert(2, 3000));
}

#[test]
fn indexed_insert_detects_duplicates() {
    let idx = DedupIndex::new();
    let dw = make_dedup_window();

    assert!(!dw.check_and_insert_indexed(1, 1000, 1, &idx));
    // Second insert of same key — duplicate, no new index entry.
    assert!(dw.check_and_insert_indexed(1, 2000, 1, &idx));

    // Only one entry in the reverse index.
    let expired = idx.prune_before(u64::MAX);
    assert_eq!(expired.len(), 1);
}

#[test]
fn topic_check_dedup_indexed() {
    let idx = DedupIndex::new();
    let topic = make_dedup_topic(60);
    let key = bytes_to_dedup_key(b"msg-1");

    assert!(!topic.check_dedup_indexed(key, 1000, &idx));
    assert!(topic.check_dedup_indexed(key, 2000, &idx));
    assert!(!idx.is_empty());
}

#[test]
fn prune_dedup_local_uses_global_index() {
    use bisque_mq::config::MqConfig;
    use bisque_mq::metadata::MqMetadata;
    use bisque_mq::raft::prune_dedup_local;

    let config = MqConfig::new("/tmp");
    let md = MqMetadata::new(config.catalog_name.clone());

    let mut meta = TopicMeta::new(1, "t1".to_string(), 1000, RetentionPolicy::default());
    meta.dedup_config = Some(TopicDedupConfig {
        window_secs: 60,
        max_entries: 100_000,
    });
    let topic = TopicState::new(meta, "test");
    let key = bytes_to_dedup_key(b"indexed-key");

    // Insert via indexed path — populates both forward map and global index.
    topic.check_dedup_indexed(key, 0, &md.dedup_index);

    let topic = Arc::new(topic);
    md.topics.pin().insert(1, Arc::clone(&topic));

    prune_dedup_local(&md);

    // Key should be pruned from both forward map and global index.
    assert!(!topic.check_dedup(key, 999_999_999));
    assert!(md.dedup_index.is_empty());
}

#[test]
fn prune_dedup_local_multi_topic_mixed_windows() {
    use bisque_mq::config::MqConfig;
    use bisque_mq::metadata::MqMetadata;
    use bisque_mq::raft::prune_dedup_local;

    let config = MqConfig::new("/tmp");
    let md = MqMetadata::new(config.catalog_name.clone());

    // Topic 1: 60s window.
    let mut meta1 = TopicMeta::new(1, "short".to_string(), 0, RetentionPolicy::default());
    meta1.dedup_config = Some(TopicDedupConfig {
        window_secs: 60,
        max_entries: 100_000,
    });
    let t1 = TopicState::new(meta1, "test");
    t1.check_dedup_indexed(1, 0, &md.dedup_index); // old entry
    let t1 = Arc::new(t1);
    md.topics.pin().insert(1, Arc::clone(&t1));

    // Topic 2: 3600s window.
    let mut meta2 = TopicMeta::new(2, "long".to_string(), 0, RetentionPolicy::default());
    meta2.dedup_config = Some(TopicDedupConfig {
        window_secs: 3600,
        max_entries: 100_000,
    });
    let t2 = TopicState::new(meta2, "test");
    t2.check_dedup_indexed(2, 0, &md.dedup_index); // old entry
    let t2 = Arc::new(t2);
    md.topics.pin().insert(2, Arc::clone(&t2));

    prune_dedup_local(&md);

    // Both entries at ts=0 should be pruned (well before any cutoff).
    assert!(!t1.check_dedup(1, u64::MAX));
    assert!(!t2.check_dedup(2, u64::MAX));
}

#[test]
fn remove_dedup_keys_batch() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 1000);
    dw.check_and_insert(2, 1000);
    dw.check_and_insert(3, 1000);
    assert_eq!(dw.len(), 3);

    let removed = dw.remove_keys(&[1, 3]);
    assert_eq!(removed, 2);
    assert_eq!(dw.len(), 1);

    // Key 2 is still a duplicate.
    assert!(dw.check_and_insert(2, 2000));
    // Keys 1 and 3 can be reinserted.
    assert!(!dw.check_and_insert(1, 2000));
    assert!(!dw.check_and_insert(3, 2000));
}

#[test]
fn remove_dedup_keys_nonexistent() {
    let dw = make_dedup_window();
    dw.check_and_insert(1, 1000);
    let removed = dw.remove_keys(&[99, 100]);
    assert_eq!(removed, 0);
    assert_eq!(dw.len(), 1);
}

#[test]
fn dedup_index_concurrent_insert_and_prune() {
    use std::sync::Arc;

    let idx = Arc::new(DedupIndex::new());
    let dw = Arc::new(make_dedup_window());

    let mut handles = vec![];

    // 5 threads inserting.
    for t in 0..5u64 {
        let idx = Arc::clone(&idx);
        let dw = Arc::clone(&dw);
        handles.push(thread::spawn(move || {
            for i in 0..1000u128 {
                let key = t as u128 * 10_000 + i;
                let ts = (t * 1000 + i as u64) % 5000;
                dw.check_and_insert_indexed(key, ts, t, &idx);
            }
        }));
    }

    // 2 threads pruning.
    for _ in 0..2 {
        let idx = Arc::clone(&idx);
        handles.push(thread::spawn(move || {
            for cutoff in (0..5000).step_by(100) {
                idx.prune_before(cutoff);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Should not panic — verify data structure integrity.
    let _ = idx.prune_before(u64::MAX);
    assert!(idx.is_empty());
}

#[test]
fn restore_indexed_populates_global_index() {
    let idx = DedupIndex::new();
    let dw = make_dedup_window();

    let entries = vec![(1u128, 1000u64), (2, 2000), (3, 3000)];
    dw.restore_indexed(entries, 42, &idx);

    assert_eq!(dw.len(), 3);
    assert!(!idx.is_empty());

    // All three entries should be in the reverse index.
    let expired = idx.prune_before(u64::MAX);
    assert_eq!(expired.len(), 3);
    // All belong to topic 42.
    assert!(expired.iter().all(|(tid, _)| *tid == 42));
}

// =============================================================================
// DedupPreFilter (per-connection single-threaded fast path)
// =============================================================================

#[test]
fn prefilter_empty() {
    let pf = DedupPreFilter::new(100, 60_000);
    assert!(pf.is_empty());
    assert_eq!(pf.len(), 0);
    assert!(!pf.contains(1));
}

#[test]
fn prefilter_first_insert_not_duplicate() {
    let mut pf = DedupPreFilter::new(100, 60_000);
    assert!(!pf.check_and_insert(1, 1000));
    assert_eq!(pf.len(), 1);
}

#[test]
fn prefilter_second_insert_is_duplicate() {
    let mut pf = DedupPreFilter::new(100, 60_000);
    assert!(!pf.check_and_insert(1, 1000));
    assert!(pf.check_and_insert(1, 2000));
    assert_eq!(pf.len(), 1); // no double-count
}

#[test]
fn prefilter_different_keys_not_duplicate() {
    let mut pf = DedupPreFilter::new(100, 60_000);
    assert!(!pf.check_and_insert(1, 1000));
    assert!(!pf.check_and_insert(2, 1000));
    assert!(!pf.check_and_insert(3, 1000));
    assert_eq!(pf.len(), 3);
}

#[test]
fn prefilter_contains() {
    let mut pf = DedupPreFilter::new(100, 60_000);
    pf.check_and_insert(42, 1000);
    assert!(pf.contains(42));
    assert!(!pf.contains(43));
}

#[test]
fn prefilter_window_expiry() {
    let mut pf = DedupPreFilter::new(100, 5_000); // 5s window

    // Insert at ts=1000.
    pf.check_and_insert(1, 1000);
    assert!(pf.contains(1));

    // Still within window at ts=5000.
    assert!(pf.check_and_insert(1, 5000));

    // At ts=7000, window cutoff = 7000 - 5000 = 2000. Entry at ts=1000 expires.
    assert!(!pf.check_and_insert(1, 7000)); // evicted, re-inserted as new
    assert_eq!(pf.len(), 1);
}

#[test]
fn prefilter_window_expiry_multiple() {
    let mut pf = DedupPreFilter::new(100, 5_000);

    // Insert 3 entries at different times.
    pf.check_and_insert(1, 1000);
    pf.check_and_insert(2, 2000);
    pf.check_and_insert(3, 4000);
    assert_eq!(pf.len(), 3);

    // At ts=8000, cutoff = 3000. Entries at ts=1000 and ts=2000 expire.
    pf.check_and_insert(99, 8000);
    assert_eq!(pf.len(), 2); // key 3 (ts=4000) + key 99 (ts=8000)
    assert!(!pf.contains(1));
    assert!(!pf.contains(2));
    assert!(pf.contains(3));
    assert!(pf.contains(99));
}

#[test]
fn prefilter_capacity_eviction() {
    let mut pf = DedupPreFilter::new(5, 60_000);

    // Fill to capacity.
    for i in 0..5 {
        pf.check_and_insert(i, 1000);
    }
    assert_eq!(pf.len(), 5);

    // Insert one more — oldest (key=0) is evicted.
    assert!(!pf.check_and_insert(10, 1000));
    assert_eq!(pf.len(), 5);
    assert!(!pf.contains(0)); // evicted
    assert!(pf.contains(1)); // still present
    assert!(pf.contains(10)); // new entry
}

#[test]
fn prefilter_capacity_eviction_fifo_order() {
    let mut pf = DedupPreFilter::new(3, 60_000);

    pf.check_and_insert(10, 1000);
    pf.check_and_insert(20, 2000);
    pf.check_and_insert(30, 3000);

    // Evict oldest (10), then insert 40.
    pf.check_and_insert(40, 4000);
    assert!(!pf.contains(10));
    assert!(pf.contains(20));
    assert!(pf.contains(30));
    assert!(pf.contains(40));

    // Evict 20, insert 50.
    pf.check_and_insert(50, 5000);
    assert!(!pf.contains(20));
    assert!(pf.contains(30));
    assert!(pf.contains(40));
    assert!(pf.contains(50));
}

#[test]
fn prefilter_clear() {
    let mut pf = DedupPreFilter::new(100, 60_000);
    pf.check_and_insert(1, 1000);
    pf.check_and_insert(2, 1000);
    assert_eq!(pf.len(), 2);

    pf.clear();
    assert!(pf.is_empty());
    assert!(!pf.contains(1));

    // Can re-insert after clear.
    assert!(!pf.check_and_insert(1, 2000));
}

#[test]
fn prefilter_zero_window_means_instant_expiry() {
    let mut pf = DedupPreFilter::new(100, 0);

    pf.check_and_insert(1, 1000);
    // With window_ms=0, cutoff = timestamp - 0 = timestamp.
    // Entry at ts=1000 has ts >= cutoff (1001), so NOT expired at ts=1001.
    // Actually cutoff = 1001 - 0 = 1001, and 1000 < 1001, so it IS expired.
    assert!(!pf.check_and_insert(1, 1001)); // expired, re-inserted
}

#[test]
fn prefilter_from_config() {
    let config = TopicDedupConfig {
        window_secs: 300,
        max_entries: 100_000,
    };
    let mut pf = DedupPreFilter::from_config(&config);
    // max should be clamped: 100_000 / 10 = 10_000
    // window_ms = 300 * 1000 = 300_000

    // Basic functionality.
    assert!(!pf.check_and_insert(1, 1000));
    assert!(pf.check_and_insert(1, 2000));
}

#[test]
fn prefilter_from_config_small() {
    let config = TopicDedupConfig {
        window_secs: 10,
        max_entries: 100, // 100 / 10 = 10, but clamped to min 256
    };
    let pf = DedupPreFilter::from_config(&config);

    // Fill to 256 without eviction.
    // (Can't easily test the exact max_entries, but verify it works.)
    assert!(pf.is_empty());
}

#[test]
fn prefilter_duplicate_detection_before_expiry() {
    let mut pf = DedupPreFilter::new(100, 10_000);

    // Insert key at ts=5000.
    pf.check_and_insert(1, 5000);

    // Check same key at ts=14000 (cutoff = 4000, entry at 5000 still valid).
    assert!(pf.check_and_insert(1, 14000));

    // Check at ts=16000 (cutoff = 6000, entry at 5000 expired).
    assert!(!pf.check_and_insert(1, 16000));
}

#[test]
fn prefilter_interleaved_insert_and_expiry() {
    let mut pf = DedupPreFilter::new(100, 5_000);

    // Wave 1: insert at ts=1000.
    for i in 0..10 {
        pf.check_and_insert(i, 1000);
    }
    assert_eq!(pf.len(), 10);

    // Wave 2: insert at ts=4000 (wave 1 still alive).
    for i in 10..20 {
        pf.check_and_insert(i, 4000);
    }
    assert_eq!(pf.len(), 20);

    // Wave 3: insert at ts=7000 (wave 1 expires, cutoff=2000).
    pf.check_and_insert(100, 7000);
    assert_eq!(pf.len(), 11); // 10 from wave 2 + 1 new
    assert!(!pf.contains(0)); // wave 1 expired
    assert!(pf.contains(10)); // wave 2 alive
}

#[test]
fn prefilter_key_zero_and_max() {
    let mut pf = DedupPreFilter::new(100, 60_000);
    assert!(!pf.check_and_insert(0, 1000));
    assert!(!pf.check_and_insert(u128::MAX, 1000));
    assert!(pf.check_and_insert(0, 2000));
    assert!(pf.check_and_insert(u128::MAX, 2000));
}

#[test]
fn prefilter_timestamp_zero() {
    let mut pf = DedupPreFilter::new(100, 60_000);
    assert!(!pf.check_and_insert(1, 0));
    assert!(pf.check_and_insert(1, 0));
}

#[test]
fn prefilter_large_capacity() {
    let mut pf = DedupPreFilter::new(50_000, 60_000);
    for i in 0..50_000u128 {
        assert!(!pf.check_and_insert(i, 1000));
    }
    assert_eq!(pf.len(), 50_000);

    // All are duplicates.
    for i in 0..100u128 {
        assert!(pf.check_and_insert(i, 2000));
    }
}

#[test]
fn prefilter_two_tier_with_dedup_window() {
    // Simulate the two-tier dedup pattern: pre-filter → DedupWindow.
    let mut pf = DedupPreFilter::new(100, 60_000);
    let dw = make_dedup_window();

    let key = bytes_to_dedup_key(b"msg-1");

    // First message: pre-filter miss → check DedupWindow → miss → insert both.
    let is_dup = pf.check_and_insert(key, 1000) || dw.check_and_insert(key, 1000);
    assert!(!is_dup);

    // Same key again: pre-filter hit → skip DedupWindow entirely.
    assert!(pf.check_and_insert(key, 2000));
    // DedupWindow was never consulted for this check.

    // Different key: pre-filter miss → DedupWindow miss.
    let key2 = bytes_to_dedup_key(b"msg-2");
    let is_dup = pf.check_and_insert(key2, 3000) || dw.check_and_insert(key2, 3000);
    assert!(!is_dup);
}

#[test]
fn prefilter_cross_connection_not_caught() {
    // Pre-filter is per-connection. Two connections don't see each other's keys.
    let mut pf_a = DedupPreFilter::new(100, 60_000);
    let mut pf_b = DedupPreFilter::new(100, 60_000);

    let key = bytes_to_dedup_key(b"shared-msg");

    assert!(!pf_a.check_and_insert(key, 1000)); // conn A: first time
    assert!(!pf_b.check_and_insert(key, 1000)); // conn B: first time (independent)

    // Both see it as duplicate on retry.
    assert!(pf_a.check_and_insert(key, 2000));
    assert!(pf_b.check_and_insert(key, 2000));
}

#[test]
fn prefilter_rapid_eviction_and_reinsert() {
    let mut pf = DedupPreFilter::new(3, 60_000);

    // Fill and cycle through capacity.
    pf.check_and_insert(1, 1000);
    pf.check_and_insert(2, 1000);
    pf.check_and_insert(3, 1000);

    // Key 1 evicted when 4 is inserted.
    pf.check_and_insert(4, 1000);
    assert!(!pf.contains(1));

    // Re-insert key 1 (not a duplicate anymore).
    assert!(!pf.check_and_insert(1, 1000));
    // Key 2 was evicted to make room.
    assert!(!pf.contains(2));
}

#[test]
fn prefilter_expiry_before_capacity_eviction() {
    let mut pf = DedupPreFilter::new(5, 2_000); // 2s window, 5 capacity

    // Insert 3 old entries at ts=1000.
    pf.check_and_insert(1, 1000);
    pf.check_and_insert(2, 1000);
    pf.check_and_insert(3, 1000);
    assert_eq!(pf.len(), 3);

    // Insert at ts=2500 (cutoff=500, old entries still valid).
    pf.check_and_insert(4, 2500);
    pf.check_and_insert(5, 2500);
    assert_eq!(pf.len(), 5);

    // Insert at ts=3500 (cutoff=1500). Old entries at ts=1000 expire,
    // so capacity eviction is not needed.
    assert!(!pf.check_and_insert(6, 3500));
    assert_eq!(pf.len(), 3); // 4, 5, 6
    assert!(!pf.contains(1));
    assert!(!pf.contains(2));
    assert!(!pf.contains(3));
    assert!(pf.contains(4));
    assert!(pf.contains(5));
    assert!(pf.contains(6));
}
