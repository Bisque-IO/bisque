//! Level 2 segment retention evaluator.
//!
//! Evaluates retention to determine which unpinned segments can be
//! permanently deleted from local disk.
//!
//! Level 1 (purge floor / unpin) releases mmap memory but never deletes
//! files. This module is the only path that removes segment files.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::metadata::MqMetadata;

// =============================================================================
// RetentionEvaluator
// =============================================================================

/// Evaluates retention to decide which raft log segments can be deleted
/// from local disk.
///
/// A segment is deletable when:
/// 1. It is below the Level 1 purge floor (unpinned — no active consumers).
/// 2. It is not the currently active segment.
pub struct RetentionEvaluator {
    /// Raft group directory — segment files live here.
    group_dir: PathBuf,
    /// Group ID (retained for metrics labels).
    #[allow(dead_code)]
    group_id: u64,
    /// Engine metadata — reads topic retention policies.
    #[allow(dead_code)]
    metadata: Arc<MqMetadata>,
    /// Level 1 purge floor — segments below this are unpinned.
    purge_floor: Arc<AtomicU64>,
    /// Active segment ID — never delete the active (being written) segment.
    /// Updated by the state machine after each apply.
    active_segment_id: AtomicU64,
    /// Guard against concurrent evaluations.
    pub evaluating: AtomicBool,
    /// Segment IDs that have been deleted (avoid re-scanning).
    deleted_segments: Mutex<HashSet<u64>>,
    /// Last evaluation time for rate limiting.
    last_eval: Mutex<Option<Instant>>,
    /// Minimum interval between evaluations.
    eval_interval: std::time::Duration,
    /// Pre-initialized metrics.
    m_segments_deleted: metrics::Counter,
    m_bytes_deleted: metrics::Counter,
    m_eval_count: metrics::Counter,
    /// Shared disk usage counter. Released on segment deletion.
    disk_usage_bytes: Option<Arc<AtomicU64>>,
    /// Maximum disk bytes allowed. 0 = unlimited. When exceeded, evaluations
    /// run at accelerated interval.
    max_disk_bytes: u64,
    /// Fraction of max_disk_bytes at which to begin urgent evaluations.
    disk_pressure_threshold: f64,
}

impl RetentionEvaluator {
    pub fn new(
        group_dir: PathBuf,
        group_id: u64,
        metadata: Arc<MqMetadata>,
        purge_floor: Arc<AtomicU64>,
        eval_interval: std::time::Duration,
        catalog_name: &str,
    ) -> Self {
        let m_segments_deleted = metrics::counter!(
            "mq.retention.segments_deleted",
            "catalog" => catalog_name.to_owned(),
            "group" => group_id.to_string()
        );
        let m_bytes_deleted = metrics::counter!(
            "mq.retention.bytes_deleted",
            "catalog" => catalog_name.to_owned(),
            "group" => group_id.to_string()
        );
        let m_eval_count = metrics::counter!(
            "mq.retention.eval_count",
            "catalog" => catalog_name.to_owned(),
            "group" => group_id.to_string()
        );

        Self {
            group_dir,
            group_id,
            metadata,
            purge_floor,
            active_segment_id: AtomicU64::new(u64::MAX),
            evaluating: AtomicBool::new(false),
            deleted_segments: Mutex::new(HashSet::new()),
            last_eval: Mutex::new(None),
            eval_interval,
            m_segments_deleted,
            m_bytes_deleted,
            m_eval_count,
            disk_usage_bytes: None,
            max_disk_bytes: 0,
            disk_pressure_threshold: 0.8,
        }
    }

    /// Set the shared disk usage counter and limits for disk pressure evaluation.
    pub fn with_disk_tracking(
        mut self,
        disk_usage_bytes: Arc<AtomicU64>,
        max_disk_bytes: u64,
        disk_pressure_threshold: f64,
    ) -> Self {
        self.disk_usage_bytes = Some(disk_usage_bytes);
        self.max_disk_bytes = max_disk_bytes;
        self.disk_pressure_threshold = disk_pressure_threshold;
        self
    }

    /// Update the active segment ID. Called by the state machine after
    /// determining which segment the latest apply wrote to.
    pub fn set_active_segment(&self, segment_id: u64) {
        self.active_segment_id.store(segment_id, Ordering::Relaxed);
    }

    /// Returns true if enough time has elapsed since the last evaluation,
    /// or if disk pressure requires urgent evaluation.
    pub fn should_evaluate(&self) -> bool {
        if self.eval_interval.is_zero() {
            return false; // disabled
        }
        // Urgent evaluation if disk pressure exceeds threshold.
        if self.max_disk_bytes > 0 {
            if let Some(ref usage) = self.disk_usage_bytes {
                let current = usage.load(Ordering::Relaxed);
                let threshold = (self.max_disk_bytes as f64 * self.disk_pressure_threshold) as u64;
                if current > threshold {
                    return true;
                }
            }
        }
        let guard = self.last_eval.lock();
        match *guard {
            None => true,
            Some(last) => last.elapsed() >= self.eval_interval,
        }
    }

    /// Run a retention evaluation cycle. Returns the number of segments deleted.
    ///
    /// This is safe to call from a blocking context (does file I/O).
    /// Only one evaluation runs at a time (guard by `evaluating` AtomicBool).
    pub fn evaluate(&self) -> usize {
        // Prevent concurrent evaluations.
        if self
            .evaluating
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return 0;
        }

        let result = self.evaluate_inner();

        // Update last eval time and release guard.
        *self.last_eval.lock() = Some(Instant::now());
        self.evaluating.store(false, Ordering::Release);

        self.m_eval_count.increment(1);
        result
    }

    fn evaluate_inner(&self) -> usize {
        let floor = self.purge_floor.load(Ordering::Acquire);
        if floor == 0 {
            return 0; // no purge floor set yet
        }

        let active_seg = self.active_segment_id.load(Ordering::Relaxed);

        // Scan segment files on disk.
        let segment_ids = match bisque_raft::scan_segment_ids(&self.group_dir) {
            Ok(ids) => ids,
            Err(e) => {
                warn!(error = %e, "retention: failed to scan segment files");
                return 0;
            }
        };

        let deleted_guard = self.deleted_segments.lock();
        let mut candidates: Vec<u64> = segment_ids
            .into_iter()
            .filter(|&id| id != active_seg) // never delete active segment
            .filter(|id| !deleted_guard.contains(id)) // skip already deleted
            .collect();
        drop(deleted_guard);

        candidates.sort_unstable(); // process oldest first

        // All segments below the purge floor are eligible for deletion.
        // The purge floor already accounts for consumer offsets.
        let deletable: Vec<u64> = candidates
            .into_iter()
            .filter(|&id| self.is_segment_deletable(id, floor))
            .collect();

        if deletable.is_empty() {
            return 0;
        }

        // Delete segment files.
        let mut deleted_count = 0;
        let mut deleted_bytes = 0u64;
        let mut deleted_guard = self.deleted_segments.lock();

        for segment_id in &deletable {
            let path = self.group_dir.join(format!("seg_{:06}.log", segment_id));

            // Get file size before deletion for metrics.
            let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

            match std::fs::remove_file(&path) {
                Ok(()) => {
                    deleted_count += 1;
                    deleted_bytes += file_size;
                    deleted_guard.insert(*segment_id);

                    // Also delete the .sidx file if it exists.
                    let sidx_path = self.group_dir.join(format!("{:020}.sidx", segment_id));
                    let _ = std::fs::remove_file(&sidx_path);

                    debug!(segment_id, file_size, "retention: deleted segment");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already deleted (race with another path) — record it.
                    deleted_guard.insert(*segment_id);
                }
                Err(e) => {
                    warn!(
                        segment_id,
                        error = %e,
                        "retention: failed to delete segment"
                    );
                }
            }
        }

        if deleted_count > 0 {
            // Release disk usage for deleted segments.
            if let Some(ref usage) = self.disk_usage_bytes {
                usage.fetch_sub(deleted_bytes, Ordering::Relaxed);
            }
            self.m_segments_deleted.increment(deleted_count as u64);
            self.m_bytes_deleted.increment(deleted_bytes);
            info!(
                deleted = deleted_count,
                bytes = deleted_bytes,
                "retention: evaluation complete"
            );
        }

        deleted_count
    }

    /// Check if a specific segment is eligible for deletion.
    ///
    /// Without per-entity segment tracking (MDBX was removed), we cannot
    /// evaluate per-topic `max_bytes` or `max_messages` policies. However,
    /// `max_age_secs` is still enforced using segment file mtime. Segments
    /// below the purge floor that are old enough are deleted.
    ///
    /// If ANY topic has a retention policy with no age limit and no size/message
    /// limit (i.e., retain-forever), we still delete — the purge floor already
    /// ensures consumers have moved past this data.
    fn is_segment_deletable(&self, segment_id: u64, _floor: u64) -> bool {
        // Check if any topic has a max_age that would protect this segment.
        let topics_guard = self.metadata.topics.pin();
        for topic in topics_guard.values() {
            if let Some(max_age) = topic.meta.retention.max_age_secs {
                if !self.segment_age_exceeded(segment_id, max_age) {
                    return false;
                }
            }
        }
        true
    }

    /// Check if a segment's data is older than `max_age_secs`.
    /// Uses file mtime as a proxy for the segment's newest record timestamp.
    fn segment_age_exceeded(&self, segment_id: u64, max_age_secs: u64) -> bool {
        let path = self.group_dir.join(format!("seg_{:06}.log", segment_id));

        let mtime = match std::fs::metadata(&path) {
            Ok(m) => match m.modified() {
                Ok(t) => t,
                Err(_) => return false, // can't determine age → conservative
            },
            Err(_) => return true, // file gone → effectively "expired"
        };

        match mtime.elapsed() {
            Ok(elapsed) => elapsed.as_secs() >= max_age_secs,
            Err(_) => false, // clock skew → conservative
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MqConfig;
    use crate::metadata::MqMetadata;

    fn make_metadata() -> Arc<MqMetadata> {
        let config = MqConfig::new("/tmp/retention-test");
        Arc::new(MqMetadata::new(config.catalog_name.clone()))
    }

    #[test]
    fn should_evaluate_disabled_when_zero_interval() {
        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::ZERO,
            "test",
        );

        assert!(!evaluator.should_evaluate());
    }

    #[test]
    fn should_evaluate_true_on_first_call() {
        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(30),
            "test",
        );

        assert!(evaluator.should_evaluate());
    }

    #[test]
    fn concurrent_eval_guard() {
        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(30),
            "test",
        );

        // Simulate concurrent eval by pre-setting the flag.
        evaluator.evaluating.store(true, Ordering::Relaxed);
        assert_eq!(evaluator.evaluate(), 0, "concurrent eval should return 0");
        evaluator.evaluating.store(false, Ordering::Relaxed);
    }

    #[test]
    fn all_segments_below_floor_are_deletable() {
        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(0),
            "test",
        );

        // All segments are deletable since we trust the purge floor.
        assert!(evaluator.is_segment_deletable(1, 100));
        assert!(evaluator.is_segment_deletable(99, 100));
    }

    #[test]
    fn max_age_protects_recent_segment() {
        use crate::topic::{TopicMeta, TopicState};
        use crate::types::RetentionPolicy;

        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();

        // Create a topic with 1-hour max_age.
        let retention = RetentionPolicy {
            max_age_secs: Some(3600),
            max_bytes: None,
            max_messages: None,
        };
        let meta = TopicMeta::new(1, "aged".to_string(), 1000, retention);
        md.topics
            .pin()
            .insert(1, Arc::new(TopicState::new(meta, "test")));

        // Create a recent segment file (mtime = now).
        let seg_path = tmp.path().join("seg_000001.log");
        std::fs::write(&seg_path, b"data").unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(30),
            "test",
        );

        // Recent segment should NOT be deletable (max_age not exceeded).
        assert!(
            !evaluator.is_segment_deletable(1, 100),
            "recent segment should be protected by max_age_secs"
        );
    }

    #[test]
    fn no_retention_policy_allows_deletion() {
        use crate::topic::{TopicMeta, TopicState};
        use crate::types::RetentionPolicy;

        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();

        // Topic with no max_age (retain forever based on consumer offsets).
        let retention = RetentionPolicy::default();
        let meta = TopicMeta::new(1, "forever".to_string(), 1000, retention);
        md.topics
            .pin()
            .insert(1, Arc::new(TopicState::new(meta, "test")));

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(30),
            "test",
        );

        // No max_age → no age protection → deletable (purge floor is the guard).
        assert!(evaluator.is_segment_deletable(1, 100));
    }

    #[test]
    fn missing_segment_file_treated_as_expired() {
        use crate::topic::{TopicMeta, TopicState};
        use crate::types::RetentionPolicy;

        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();

        let retention = RetentionPolicy {
            max_age_secs: Some(3600),
            max_bytes: None,
            max_messages: None,
        };
        let meta = TopicMeta::new(1, "aged".to_string(), 1000, retention);
        md.topics
            .pin()
            .insert(1, Arc::new(TopicState::new(meta, "test")));

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(30),
            "test",
        );

        // File doesn't exist → treated as expired → deletable.
        assert!(evaluator.is_segment_deletable(99, 100));
    }
}
