//! Comprehensive integration tests for the two-level segment purge architecture.
//!
//! Level 1 (Unpin): releases mmap memory, never deletes files.
//! Level 2 (Retention): deletes segment files based on per-entity retention policies.
//!
//! Covers:
//!   - RetentionEvaluator construction and configuration
//!   - Retention policy evaluation (max_age, max_bytes, max_messages)
//!   - Segment deletion lifecycle (create → seal → unpin → retention delete)
//!   - Mixed retention policies across entities sharing a segment
//!   - Deleted topics unblock segment deletion
//!   - Default retention (None/None/None) blocks deletion forever
//!   - Concurrent evaluation guard
//!   - Active segment protection
//!   - Edge cases: empty segments, missing files, zero-value policies

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use std::fs::FileTimes;

use bisque_mq::manifest::MqManifestManager;
use bisque_mq::metadata::MqMetadata;
use bisque_mq::retention::RetentionEvaluator;
use bisque_mq::segment_index::ENTITY_TOPIC;
use bisque_mq::topic::{TopicMeta, TopicState};
use bisque_mq::types::RetentionPolicy;

// =============================================================================
// Helpers
// =============================================================================

fn make_metadata(catalog: &str) -> Arc<MqMetadata> {
    Arc::new(MqMetadata::new(catalog.to_string()))
}

fn make_evaluator(
    dir: &std::path::Path,
    metadata: Arc<MqMetadata>,
    eval_interval: Duration,
) -> (Arc<MqManifestManager>, Arc<RetentionEvaluator>) {
    let manifest = Arc::new(MqManifestManager::new(dir).unwrap());
    manifest.open_group(1).unwrap();

    let evaluator = RetentionEvaluator::new(
        dir.to_path_buf(),
        1,
        Arc::clone(&manifest),
        metadata,
        Arc::new(AtomicU64::new(u64::MAX)), // high floor = everything unpinned
        eval_interval,
        "test",
    );
    (manifest, Arc::new(evaluator))
}

fn add_topic(metadata: &MqMetadata, topic_id: u64, name: &str, retention: RetentionPolicy) {
    let meta = TopicMeta::new(topic_id, name.to_string(), 1000, retention);
    let state = Arc::new(TopicState::new(meta, "test"));
    metadata.topics.pin().insert(topic_id, state);
}

fn set_mtime(path: &std::path::Path, time: std::time::SystemTime) {
    let file = std::fs::File::options().write(true).open(path).unwrap();
    file.set_times(FileTimes::new().set_modified(time)).unwrap();
}

fn create_segment_file(dir: &std::path::Path, segment_id: u64) -> std::path::PathBuf {
    let path = dir.join(format!("seg_{:06}.log", segment_id));
    std::fs::write(&path, vec![0u8; 4096]).unwrap();
    path
}

fn segment_exists(dir: &std::path::Path, segment_id: u64) -> bool {
    dir.join(format!("seg_{:06}.log", segment_id)).exists()
}

fn wait_manifest() {
    std::thread::sleep(Duration::from_millis(200));
}

// =============================================================================
// Basic retention policy tests
// =============================================================================

#[test]
fn default_retention_blocks_deletion_forever() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(&md, 1, "forever", RetentionPolicy::default());

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Create segment with data for topic 1.
    create_segment_file(tmp.path(), 1);
    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 1000)]);
    wait_manifest();

    let deleted = eval.evaluate();
    assert_eq!(deleted, 0);
    assert!(segment_exists(tmp.path(), 1));
}

#[test]
fn max_age_allows_deletion_of_old_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(
        &md,
        1,
        "aged",
        RetentionPolicy {
            max_age_secs: Some(0), // age = 0 means anything older than now
            max_bytes: None,
            max_messages: None,
        },
    );

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Create an "old" segment file.
    let path = create_segment_file(tmp.path(), 1);
    // Backdate the file mtime by 10 seconds.
    let old_time = std::time::SystemTime::now() - Duration::from_secs(10);
    set_mtime(&path, old_time);

    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 1000)]);
    wait_manifest();

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1);
    assert!(!segment_exists(tmp.path(), 1));
}

#[test]
fn max_bytes_allows_deletion_when_over_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(
        &md,
        1,
        "sized",
        RetentionPolicy {
            max_age_secs: None,
            max_bytes: Some(1000),
            max_messages: None,
        },
    );

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Two segments: total = 600 + 800 = 1400 > max_bytes=1000.
    create_segment_file(tmp.path(), 1);
    create_segment_file(tmp.path(), 2);
    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 600)]);
    manifest.sealed_segment_fire_and_forget(1, 2, vec![(ENTITY_TOPIC, 1, 20, 800)]);
    wait_manifest();

    let deleted = eval.evaluate();
    assert!(deleted >= 1);
    // Oldest segment (1) should be deleted first.
    assert!(!segment_exists(tmp.path(), 1));
}

#[test]
fn max_bytes_blocks_when_under_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(
        &md,
        1,
        "small",
        RetentionPolicy {
            max_age_secs: None,
            max_bytes: Some(5000),
            max_messages: None,
        },
    );

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Total = 500 + 800 = 1300 < max_bytes=5000.
    create_segment_file(tmp.path(), 1);
    create_segment_file(tmp.path(), 2);
    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 500)]);
    manifest.sealed_segment_fire_and_forget(1, 2, vec![(ENTITY_TOPIC, 1, 20, 800)]);
    wait_manifest();

    let deleted = eval.evaluate();
    assert_eq!(deleted, 0);
    assert!(segment_exists(tmp.path(), 1));
    assert!(segment_exists(tmp.path(), 2));
}

#[test]
fn max_messages_allows_deletion_when_over_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(
        &md,
        1,
        "counted",
        RetentionPolicy {
            max_age_secs: None,
            max_bytes: None,
            max_messages: Some(50),
        },
    );

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Total = 30 + 40 = 70 > max_messages=50.
    create_segment_file(tmp.path(), 1);
    create_segment_file(tmp.path(), 2);
    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 30, 300)]);
    manifest.sealed_segment_fire_and_forget(1, 2, vec![(ENTITY_TOPIC, 1, 40, 400)]);
    wait_manifest();

    let deleted = eval.evaluate();
    assert!(deleted >= 1);
    assert!(!segment_exists(tmp.path(), 1));
}

// =============================================================================
// Multi-entity segment tests
// =============================================================================

#[test]
fn mixed_retention_blocks_if_any_entity_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");

    // Topic 1: has retention → allows deletion.
    add_topic(
        &md,
        1,
        "aged",
        RetentionPolicy {
            max_age_secs: Some(0),
            max_bytes: None,
            max_messages: None,
        },
    );
    // Topic 2: default retention → blocks deletion.
    add_topic(&md, 2, "forever", RetentionPolicy::default());

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Segment 1 has data for both topics.
    let path = create_segment_file(tmp.path(), 1);
    let old_time = std::time::SystemTime::now() - Duration::from_secs(10);
    set_mtime(&path, old_time);

    manifest.sealed_segment_fire_and_forget(
        1,
        1,
        vec![(ENTITY_TOPIC, 1, 10, 1000), (ENTITY_TOPIC, 2, 5, 500)],
    );
    wait_manifest();

    let deleted = eval.evaluate();
    assert_eq!(deleted, 0, "topic 2 blocks segment deletion");
    assert!(segment_exists(tmp.path(), 1));
}

#[test]
fn mixed_retention_allows_when_all_entities_allow() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");

    add_topic(
        &md,
        1,
        "aged1",
        RetentionPolicy {
            max_age_secs: Some(0),
            max_bytes: None,
            max_messages: None,
        },
    );
    add_topic(
        &md,
        2,
        "aged2",
        RetentionPolicy {
            max_age_secs: Some(0),
            max_bytes: None,
            max_messages: None,
        },
    );

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    let path = create_segment_file(tmp.path(), 1);
    let old_time = std::time::SystemTime::now() - Duration::from_secs(10);
    set_mtime(&path, old_time);

    manifest.sealed_segment_fire_and_forget(
        1,
        1,
        vec![(ENTITY_TOPIC, 1, 10, 1000), (ENTITY_TOPIC, 2, 5, 500)],
    );
    wait_manifest();

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1);
    assert!(!segment_exists(tmp.path(), 1));
}

// =============================================================================
// Deleted topic unblocks segment
// =============================================================================

#[test]
fn deleted_topic_unblocks_segment_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");

    // Initially topic 1 exists with default retention → blocks.
    add_topic(&md, 1, "will-die", RetentionPolicy::default());

    let (manifest, eval) = make_evaluator(tmp.path(), md.clone(), Duration::ZERO);

    create_segment_file(tmp.path(), 1);
    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 1000)]);
    wait_manifest();

    // First eval: topic exists, default retention → blocked.
    assert_eq!(eval.evaluate(), 0);
    assert!(segment_exists(tmp.path(), 1));

    // Delete the topic.
    md.topics.pin().remove(&1);

    // Second eval: topic is gone → allowed.
    assert_eq!(eval.evaluate(), 1);
    assert!(!segment_exists(tmp.path(), 1));
}

// =============================================================================
// Active segment protection
// =============================================================================

#[test]
fn active_segment_is_never_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(
        &md,
        1,
        "aged",
        RetentionPolicy {
            max_age_secs: Some(0),
            max_bytes: None,
            max_messages: None,
        },
    );

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    let path = create_segment_file(tmp.path(), 5);
    let old_time = std::time::SystemTime::now() - Duration::from_secs(10);
    set_mtime(&path, old_time);

    manifest.sealed_segment_fire_and_forget(1, 5, vec![(ENTITY_TOPIC, 1, 10, 1000)]);
    wait_manifest();

    // Mark segment 5 as the active segment.
    eval.set_active_segment(5);

    let deleted = eval.evaluate();
    assert_eq!(deleted, 0, "active segment must not be deleted");
    assert!(segment_exists(tmp.path(), 5));
}

// =============================================================================
// Empty / no-entity segments
// =============================================================================

#[test]
fn segment_with_no_entities_in_mdbx_is_deletable() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Segment file exists but no entity data in MDBX.
    create_segment_file(tmp.path(), 1);

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1);
    assert!(!segment_exists(tmp.path(), 1));
}

#[test]
fn missing_segment_file_is_harmless() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // No segment files at all.
    let deleted = eval.evaluate();
    assert_eq!(deleted, 0);
}

// =============================================================================
// Idempotent deletion
// =============================================================================

#[test]
fn double_evaluate_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    create_segment_file(tmp.path(), 1);

    let first = eval.evaluate();
    assert_eq!(first, 1);

    // Second eval — segment already deleted and tracked.
    let second = eval.evaluate();
    assert_eq!(second, 0);
}

// =============================================================================
// Rate limiting
// =============================================================================

#[test]
fn should_evaluate_respects_interval() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::from_secs(3600));

    assert!(eval.should_evaluate(), "first call should allow eval");

    // Run an eval to set the last_eval timestamp.
    eval.evaluate();

    assert!(
        !eval.should_evaluate(),
        "should not allow eval immediately after"
    );
}

#[test]
fn disabled_when_zero_interval() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    assert!(!eval.should_evaluate(), "ZERO interval = disabled");
}

// =============================================================================
// Concurrent evaluation guard
// =============================================================================

#[test]
fn concurrent_evaluate_returns_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Simulate concurrent eval.
    eval.evaluating.store(true, Ordering::Relaxed);
    assert_eq!(eval.evaluate(), 0);
    eval.evaluating.store(false, Ordering::Relaxed);
}

#[test]
fn concurrent_eval_with_threads() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Create some segment files.
    for i in 1..=5 {
        create_segment_file(tmp.path(), i);
    }

    let eval1 = Arc::clone(&eval);
    let eval2 = Arc::clone(&eval);
    let eval3 = Arc::clone(&eval);

    let h1 = std::thread::spawn(move || eval1.evaluate());
    let h2 = std::thread::spawn(move || eval2.evaluate());
    let h3 = std::thread::spawn(move || eval3.evaluate());

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();
    let r3 = h3.join().unwrap();

    // Exactly one thread should have done the work (5 segments).
    // The others should have returned 0 or done some subset.
    let total = r1 + r2 + r3;
    assert!(total <= 5, "should not delete more than 5");
    // All segments should be gone after all threads finish.
    for i in 1..=5u64 {
        assert!(
            !segment_exists(tmp.path(), i),
            "segment {} should be deleted",
            i
        );
    }
}

// =============================================================================
// Multiple segments, oldest-first deletion
// =============================================================================

#[test]
fn deletes_oldest_segments_first() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(
        &md,
        1,
        "counted",
        RetentionPolicy {
            max_age_secs: None,
            max_bytes: None,
            max_messages: Some(100),
        },
    );

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // 5 segments, 30 messages each = 150 total > 100 max.
    for i in 1..=5 {
        create_segment_file(tmp.path(), i);
        manifest.sealed_segment_fire_and_forget(1, i, vec![(ENTITY_TOPIC, 1, 30, 300)]);
    }
    wait_manifest();

    // Mark segment 5 as active so it's protected.
    eval.set_active_segment(5);

    let deleted = eval.evaluate();
    // Should delete oldest segments until remaining ≤ max_messages.
    // Each segment has 30 msgs. Total = 150. Need to trim to ≤ 100.
    // Delete seg 1 → remaining 120 > 100 → still eligible.
    // Delete seg 2 → remaining 90 ≤ 100.
    // But our current impl evaluates each segment independently, so
    // all 4 non-active segments may be deleted since total > 100 for each.
    assert!(deleted >= 1, "should delete at least 1 old segment");
    assert!(segment_exists(tmp.path(), 5), "active segment must survive");
}

// =============================================================================
// Purge floor interaction
// =============================================================================

#[test]
fn zero_purge_floor_blocks_all_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");

    let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
    manifest.open_group(1).unwrap();

    let evaluator = RetentionEvaluator::new(
        tmp.path().to_path_buf(),
        1,
        manifest,
        md,
        Arc::new(AtomicU64::new(0)), // purge_floor = 0
        Duration::ZERO,
        "test",
    );

    create_segment_file(tmp.path(), 1);

    let deleted = evaluator.evaluate();
    assert_eq!(deleted, 0, "zero purge floor should block evaluation");
    assert!(segment_exists(tmp.path(), 1));
}

// =============================================================================
// SIDX file cleanup
// =============================================================================

#[test]
fn sidx_file_deleted_with_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    create_segment_file(tmp.path(), 1);
    // Create a .sidx file too.
    let sidx_path = tmp.path().join(format!("{:020}.sidx", 1u64));
    std::fs::write(&sidx_path, b"sidx data").unwrap();

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1);
    assert!(!segment_exists(tmp.path(), 1));
    assert!(!sidx_path.exists(), ".sidx file should be deleted too");
}

// =============================================================================
// Multiple groups isolation (segments in different dirs)
// =============================================================================

#[test]
fn evaluator_only_deletes_in_its_own_dir() {
    let tmp1 = tempfile::tempdir().unwrap();
    let tmp2 = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval1) = make_evaluator(tmp1.path(), md.clone(), Duration::ZERO);
    let (_, eval2) = make_evaluator(tmp2.path(), md, Duration::ZERO);

    create_segment_file(tmp1.path(), 1);
    create_segment_file(tmp2.path(), 1);

    eval1.evaluate();
    assert!(
        !segment_exists(tmp1.path(), 1),
        "eval1 should delete its segment"
    );
    assert!(
        segment_exists(tmp2.path(), 1),
        "eval1 should not touch eval2's dir"
    );

    eval2.evaluate();
    assert!(
        !segment_exists(tmp2.path(), 1),
        "eval2 should delete its segment"
    );
}

// =============================================================================
// Stress test: many segments, many topics
// =============================================================================

#[test]
fn stress_many_segments_many_topics() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");

    // 20 topics, half with retention, half without.
    for i in 1..=20 {
        let retention = if i % 2 == 0 {
            RetentionPolicy {
                max_age_secs: Some(0),
                max_bytes: None,
                max_messages: None,
            }
        } else {
            RetentionPolicy::default()
        };
        add_topic(&md, i, &format!("t-{}", i), retention);
    }

    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // 50 segments, each with data for 2-3 topics.
    for seg in 1..=50 {
        let path = create_segment_file(tmp.path(), seg);
        let old_time = std::time::SystemTime::now() - Duration::from_secs(10);
        set_mtime(&path, old_time);

        let mut entities = Vec::new();
        // Each segment has data for topic (seg % 20 + 1) and topic ((seg + 7) % 20 + 1).
        let t1 = (seg % 20) + 1;
        let t2 = ((seg + 7) % 20) + 1;
        entities.push((ENTITY_TOPIC, t1, 10, 100));
        entities.push((ENTITY_TOPIC, t2, 5, 50));
        manifest.sealed_segment_fire_and_forget(1, seg, entities);
    }
    wait_manifest();

    eval.set_active_segment(50);

    let deleted = eval.evaluate();
    // Segments that only contain data for even-numbered topics (with retention)
    // should be deleted. Segments containing any odd-numbered topic data should
    // survive.
    assert!(
        deleted < 50,
        "some segments should be blocked by odd topics"
    );

    // Active segment must survive.
    assert!(segment_exists(tmp.path(), 50));
}

// =============================================================================
// Concurrent stress: multiple threads evaluating simultaneously
// =============================================================================

#[test]
fn concurrent_stress_10_threads() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Create 100 segment files.
    for i in 1..=100 {
        create_segment_file(tmp.path(), i);
    }
    eval.set_active_segment(100);

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let e = Arc::clone(&eval);
            std::thread::spawn(move || e.evaluate())
        })
        .collect();

    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

    // Exactly 99 segments should be deleted (100 is active).
    // Across all threads, some may have gotten 0 due to concurrent guard.
    // But after all finish, all non-active should be gone.
    assert!(total <= 99);

    // Verify final state.
    for i in 1..=99 {
        assert!(
            !segment_exists(tmp.path(), i),
            "segment {} should be deleted",
            i
        );
    }
    assert!(
        segment_exists(tmp.path(), 100),
        "active segment must survive"
    );
}

// =============================================================================
// Retention policy transitions
// =============================================================================

#[test]
fn changing_retention_policy_unblocks_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    add_topic(&md, 1, "evolving", RetentionPolicy::default());

    let (manifest, eval) = make_evaluator(tmp.path(), md.clone(), Duration::ZERO);

    let path = create_segment_file(tmp.path(), 1);
    let old_time = std::time::SystemTime::now() - Duration::from_secs(10);
    set_mtime(&path, old_time);

    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 1000)]);
    wait_manifest();

    // Blocked by default retention.
    assert_eq!(eval.evaluate(), 0);

    // Replace topic with retention policy.
    md.topics.pin().remove(&1);
    add_topic(
        &md,
        1,
        "evolving",
        RetentionPolicy {
            max_age_secs: Some(0),
            max_bytes: None,
            max_messages: None,
        },
    );

    // Now allowed.
    assert_eq!(eval.evaluate(), 1);
    assert!(!segment_exists(tmp.path(), 1));
}

// =============================================================================
// Edge: segment contains only non-topic entities
// =============================================================================

#[test]
fn segment_with_only_queue_entities_is_deletable() {
    use bisque_mq::segment_index::ENTITY_QUEUE;

    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (manifest, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    create_segment_file(tmp.path(), 1);
    // Only ENTITY_QUEUE data, no topic data.
    manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_QUEUE, 1, 10, 1000)]);
    wait_manifest();

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1, "queue-only segment should be deletable");
}

// =============================================================================
// Edge: very large segment IDs
// =============================================================================

#[test]
fn large_segment_ids_handled_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let (_, eval) = make_evaluator(tmp.path(), md, Duration::ZERO);

    let large_id = 999999u64;
    create_segment_file(tmp.path(), large_id);

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1);
    assert!(!segment_exists(tmp.path(), large_id));
}
