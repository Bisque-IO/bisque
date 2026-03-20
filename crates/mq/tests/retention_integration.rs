//! Comprehensive integration tests for the two-level segment purge architecture.
//!
//! Level 1 (Unpin): releases mmap memory, never deletes files.
//! Level 2 (Retention): deletes segment files below the purge floor.
//!
//! Without per-entity segment tracking (MDBX removed), all segments below
//! the purge floor and not currently active are eligible for deletion.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bisque_mq::metadata::MqMetadata;
use bisque_mq::retention::RetentionEvaluator;

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
) -> Arc<RetentionEvaluator> {
    let evaluator = RetentionEvaluator::new(
        dir.to_path_buf(),
        1,
        metadata,
        Arc::new(AtomicU64::new(u64::MAX)), // high floor = everything unpinned
        eval_interval,
        "test",
    );
    Arc::new(evaluator)
}

fn create_segment_file(dir: &std::path::Path, segment_id: u64) -> std::path::PathBuf {
    let path = dir.join(format!("seg_{:06}.log", segment_id));
    std::fs::write(&path, vec![0u8; 4096]).unwrap();
    path
}

fn segment_exists(dir: &std::path::Path, segment_id: u64) -> bool {
    dir.join(format!("seg_{:06}.log", segment_id)).exists()
}

// =============================================================================
// Basic deletion tests
// =============================================================================

#[test]
fn all_segments_below_floor_are_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

    create_segment_file(tmp.path(), 1);
    create_segment_file(tmp.path(), 2);
    create_segment_file(tmp.path(), 3);

    let deleted = eval.evaluate();
    assert_eq!(deleted, 3);
    assert!(!segment_exists(tmp.path(), 1));
    assert!(!segment_exists(tmp.path(), 2));
    assert!(!segment_exists(tmp.path(), 3));
}

// =============================================================================
// Active segment protection
// =============================================================================

#[test]
fn active_segment_is_never_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

    create_segment_file(tmp.path(), 1);
    create_segment_file(tmp.path(), 5);

    // Mark segment 5 as the active segment.
    eval.set_active_segment(5);

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1);
    assert!(!segment_exists(tmp.path(), 1));
    assert!(segment_exists(tmp.path(), 5), "active segment must survive");
}

// =============================================================================
// Empty directory
// =============================================================================

#[test]
fn missing_segment_file_is_harmless() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

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
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

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
    let eval = make_evaluator(tmp.path(), md, Duration::from_secs(3600));

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
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

    assert!(!eval.should_evaluate(), "ZERO interval = disabled");
}

// =============================================================================
// Concurrent evaluation guard
// =============================================================================

#[test]
fn concurrent_evaluate_returns_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

    // Simulate concurrent eval.
    eval.evaluating.store(true, Ordering::Relaxed);
    assert_eq!(eval.evaluate(), 0);
    eval.evaluating.store(false, Ordering::Relaxed);
}

#[test]
fn concurrent_eval_with_threads() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

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
// Purge floor interaction
// =============================================================================

#[test]
fn zero_purge_floor_blocks_all_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");

    let evaluator = RetentionEvaluator::new(
        tmp.path().to_path_buf(),
        1,
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
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

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
    let eval1 = make_evaluator(tmp1.path(), md.clone(), Duration::ZERO);
    let eval2 = make_evaluator(tmp2.path(), md, Duration::ZERO);

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
// Concurrent stress: multiple threads evaluating simultaneously
// =============================================================================

#[test]
fn concurrent_stress_10_threads() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

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
// Edge: very large segment IDs
// =============================================================================

#[test]
fn large_segment_ids_handled_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let md = make_metadata("test");
    let eval = make_evaluator(tmp.path(), md, Duration::ZERO);

    let large_id = 999999u64;
    create_segment_file(tmp.path(), large_id);

    let deleted = eval.evaluate();
    assert_eq!(deleted, 1);
    assert!(!segment_exists(tmp.path(), large_id));
}
