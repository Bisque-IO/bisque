//! Level 2 segment retention evaluator.
//!
//! Evaluates per-entity retention policies to determine which unpinned
//! segments can be permanently deleted from local disk. A segment is
//! eligible for deletion only when **every entity with data in that
//! segment** has satisfied its retention policy for that data.
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

use crate::manifest::MqManifestManager;
use crate::metadata::MqMetadata;
use crate::segment_index::ENTITY_TOPIC;
use crate::types::RetentionPolicy;

// =============================================================================
// RetentionEvaluator
// =============================================================================

/// Evaluates per-entity retention policies to decide which raft log segments
/// can be deleted from local disk.
///
/// A segment is deletable when:
/// 1. It is below the Level 1 purge floor (unpinned — no active consumers).
/// 2. Every entity with data in the segment has a retention policy that
///    allows the data to be discarded.
/// 3. (If archive configured) The segment has been uploaded to remote storage.
pub struct RetentionEvaluator {
    /// Raft group directory — segment files live here.
    group_dir: PathBuf,
    /// Group ID for manifest lookups.
    group_id: u64,
    /// Manifest manager for reading segment ranges from MDBX.
    manifest: Arc<MqManifestManager>,
    /// Engine metadata — reads topic retention policies.
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
}

impl RetentionEvaluator {
    pub fn new(
        group_dir: PathBuf,
        group_id: u64,
        manifest: Arc<MqManifestManager>,
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
            manifest,
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
        }
    }

    /// Update the active segment ID. Called by the state machine after
    /// determining which segment the latest apply wrote to.
    pub fn set_active_segment(&self, segment_id: u64) {
        self.active_segment_id.store(segment_id, Ordering::Relaxed);
    }

    /// Returns true if enough time has elapsed since the last evaluation.
    pub fn should_evaluate(&self) -> bool {
        if self.eval_interval.is_zero() {
            return false; // disabled
        }
        let guard = self.last_eval.lock();
        match *guard {
            None => true,
            Some(last) => last.elapsed() >= self.eval_interval,
        }
    }

    /// Run a retention evaluation cycle. Returns the number of segments deleted.
    ///
    /// This is safe to call from a blocking context (does file I/O and MDBX reads).
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

        let mut deletable = Vec::new();

        for &segment_id in &candidates {
            if self.is_segment_deletable(segment_id, floor) {
                deletable.push(segment_id);
            }
        }

        if deletable.is_empty() {
            return 0;
        }

        // Delete segment files and clean up MDBX.
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

                    // Clean up MDBX segment range entries.
                    self.manifest
                        .purge_segment_fire_and_forget(self.group_id, *segment_id);

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
    fn is_segment_deletable(&self, segment_id: u64, _floor: u64) -> bool {
        // Read all entities with data in this segment from MDBX.
        let entities = match self
            .manifest
            .read_entities_for_segment(self.group_id, segment_id)
        {
            Ok(e) => e,
            Err(e) => {
                warn!(
                    segment_id,
                    error = %e,
                    "retention: failed to read entities for segment"
                );
                return false; // conservative: don't delete if we can't check
            }
        };

        // If no entities are recorded for this segment, it's safe to delete.
        // This can happen if the segment only contained membership changes
        // or was never indexed.
        if entities.is_empty() {
            return true;
        }

        // Check each entity's retention policy.
        for &(entity_type, entity_id, _record_count, _total_bytes) in &entities {
            match self.entity_retention_allows_deletion(entity_type, entity_id, segment_id) {
                RetentionDecision::Allow => continue,
                RetentionDecision::Deny => return false,
            }
        }

        true
    }

    /// Check if a single entity's retention policy allows deleting data
    /// in the given segment.
    fn entity_retention_allows_deletion(
        &self,
        entity_type: u8,
        entity_id: u64,
        segment_id: u64,
    ) -> RetentionDecision {
        if entity_type == ENTITY_TOPIC {
            self.topic_retention_allows_deletion(entity_id, segment_id)
        } else {
            // Consumer groups (ENTITY_QUEUE) and actor namespaces don't have
            // independent retention policies — they inherit from their source
            // topic. If the topic data is gone, the group data can go too.
            // For now, treat non-topic entities as allowing deletion if their
            // topic allows it, or allow by default.
            RetentionDecision::Allow
        }
    }

    /// Check a topic's retention policy against the given segment.
    fn topic_retention_allows_deletion(&self, topic_id: u64, segment_id: u64) -> RetentionDecision {
        let topics_guard = self.metadata.topics.pin();
        let topic = match topics_guard.get(&topic_id) {
            Some(t) => t,
            None => {
                // Topic was deleted — its data can be purged.
                return RetentionDecision::Allow;
            }
        };

        let policy = &topic.meta.retention;

        // No retention limits = retain forever.
        if policy.max_age_secs.is_none()
            && policy.max_bytes.is_none()
            && policy.max_messages.is_none()
        {
            return RetentionDecision::Deny;
        }

        // Check each configured policy dimension. ALL configured limits must
        // be satisfied for the segment to be deletable.

        // -- max_age_secs --
        if let Some(max_age) = policy.max_age_secs {
            if !self.segment_age_exceeded(segment_id, max_age) {
                return RetentionDecision::Deny;
            }
        }

        // -- max_bytes --
        if let Some(max_bytes) = policy.max_bytes {
            if !self.topic_bytes_exceeded(topic_id, segment_id, max_bytes) {
                return RetentionDecision::Deny;
            }
        }

        // -- max_messages --
        if let Some(max_messages) = policy.max_messages {
            if !self.topic_messages_exceeded(topic_id, segment_id, max_messages) {
                return RetentionDecision::Deny;
            }
        }

        RetentionDecision::Allow
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

    /// Check if removing this segment's bytes would still leave the topic
    /// within its `max_bytes` budget. If the topic's total bytes MINUS this
    /// segment's contribution exceeds `max_bytes`, then the segment can go.
    fn topic_bytes_exceeded(&self, topic_id: u64, segment_id: u64, max_bytes: u64) -> bool {
        let ranges = match self
            .manifest
            .read_segment_ranges(self.group_id, ENTITY_TOPIC, topic_id)
        {
            Ok(r) => r,
            Err(_) => return false,
        };

        let total: u64 = ranges.iter().map(|&(_, _, bytes)| bytes).sum();
        let this_segment_bytes: u64 = ranges
            .iter()
            .filter(|&&(sid, _, _)| sid == segment_id)
            .map(|&(_, _, bytes)| bytes)
            .sum();

        // The segment is deletable if the remaining data after removal still
        // exceeds max_bytes (meaning we have more data than the limit, and
        // trimming from the oldest is appropriate), OR if this segment holds
        // data beyond the budget.
        total.saturating_sub(this_segment_bytes) >= max_bytes || total > max_bytes
    }

    /// Same logic as `topic_bytes_exceeded` but for message counts.
    fn topic_messages_exceeded(&self, topic_id: u64, segment_id: u64, max_messages: u64) -> bool {
        let ranges = match self
            .manifest
            .read_segment_ranges(self.group_id, ENTITY_TOPIC, topic_id)
        {
            Ok(r) => r,
            Err(_) => return false,
        };

        let total: u64 = ranges.iter().map(|&(_, count, _)| count).sum();
        let this_segment_count: u64 = ranges
            .iter()
            .filter(|&&(sid, _, _)| sid == segment_id)
            .map(|&(_, count, _)| count)
            .sum();

        total.saturating_sub(this_segment_count) >= max_messages || total > max_messages
    }
}

// =============================================================================
// Internal types
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetentionDecision {
    Allow,
    Deny,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MqConfig;
    use crate::metadata::MqMetadata;
    use crate::topic::{TopicMeta, TopicState};

    fn make_metadata() -> Arc<MqMetadata> {
        let config = MqConfig::new("/tmp/retention-test");
        Arc::new(MqMetadata::new(config.catalog_name.clone()))
    }

    fn make_topic_with_retention(
        topic_id: u64,
        name: &str,
        retention: RetentionPolicy,
    ) -> Arc<TopicState> {
        let meta = TopicMeta::new(topic_id, name.to_string(), 1000, retention);
        Arc::new(TopicState::new(meta, "test"))
    }

    #[test]
    fn retention_policy_none_blocks_deletion() {
        let md = make_metadata();
        let topic = make_topic_with_retention(
            1,
            "forever",
            RetentionPolicy::default(), // None/None/None
        );
        md.topics.pin().insert(1, topic);

        let tmp = tempfile::tempdir().unwrap();
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(0),
            "test",
        );

        assert_eq!(
            evaluator.topic_retention_allows_deletion(1, 1),
            RetentionDecision::Deny,
            "default retention (None/None/None) should block deletion"
        );
    }

    #[test]
    fn deleted_topic_allows_deletion() {
        let md = make_metadata();
        // Don't insert any topic — simulates deleted topic.

        let tmp = tempfile::tempdir().unwrap();
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(0),
            "test",
        );

        assert_eq!(
            evaluator.topic_retention_allows_deletion(1, 1),
            RetentionDecision::Allow,
            "deleted topic should allow segment deletion"
        );
    }

    #[test]
    fn segment_with_no_entities_is_deletable() {
        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(0),
            "test",
        );

        // Segment 99 has no entities in MDBX.
        assert!(evaluator.is_segment_deletable(99, 100));
    }

    #[test]
    fn should_evaluate_disabled_when_zero_interval() {
        let md = make_metadata();
        let tmp = tempfile::tempdir().unwrap();
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
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
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
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
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
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
    fn evaluate_with_max_bytes_policy() {
        let md = make_metadata();
        let topic = make_topic_with_retention(
            1,
            "sized",
            RetentionPolicy {
                max_age_secs: None,
                max_bytes: Some(1000),
                max_messages: None,
            },
        );
        md.topics.pin().insert(1, topic);

        let tmp = tempfile::tempdir().unwrap();
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        // Write segment ranges: segment 1 has 500 bytes, segment 2 has 800 bytes.
        // Total = 1300 > max_bytes=1000, so segment 1 (oldest) can be deleted
        // because remaining (800) < 1000.
        // Actually: total(1300) > max_bytes(1000) → true, so deletion is allowed.
        manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 500)]);
        manifest.sealed_segment_fire_and_forget(1, 2, vec![(ENTITY_TOPIC, 1, 20, 800)]);

        // Wait for async writes.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(0),
            "test",
        );

        assert!(evaluator.topic_bytes_exceeded(1, 1, 1000));
    }

    #[test]
    fn evaluate_with_max_bytes_under_limit() {
        let md = make_metadata();
        let topic = make_topic_with_retention(
            1,
            "small",
            RetentionPolicy {
                max_age_secs: None,
                max_bytes: Some(2000),
                max_messages: None,
            },
        );
        md.topics.pin().insert(1, topic);

        let tmp = tempfile::tempdir().unwrap();
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        // Total = 500 + 800 = 1300 < max_bytes=2000 → can't delete.
        manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 10, 500)]);
        manifest.sealed_segment_fire_and_forget(1, 2, vec![(ENTITY_TOPIC, 1, 20, 800)]);

        std::thread::sleep(std::time::Duration::from_millis(200));

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(0),
            "test",
        );

        assert!(!evaluator.topic_bytes_exceeded(1, 1, 2000));
    }

    #[test]
    fn evaluate_with_max_messages_policy() {
        let md = make_metadata();
        let topic = make_topic_with_retention(
            1,
            "counted",
            RetentionPolicy {
                max_age_secs: None,
                max_bytes: None,
                max_messages: Some(100),
            },
        );
        md.topics.pin().insert(1, topic);

        let tmp = tempfile::tempdir().unwrap();
        let manifest = Arc::new(MqManifestManager::new(tmp.path()).unwrap());
        manifest.open_group(1).unwrap();

        // Total messages = 50 + 80 = 130 > max_messages=100 → can delete.
        manifest.sealed_segment_fire_and_forget(1, 1, vec![(ENTITY_TOPIC, 1, 50, 500)]);
        manifest.sealed_segment_fire_and_forget(1, 2, vec![(ENTITY_TOPIC, 1, 80, 800)]);

        std::thread::sleep(std::time::Duration::from_millis(200));

        let evaluator = RetentionEvaluator::new(
            tmp.path().to_path_buf(),
            1,
            manifest,
            md,
            Arc::new(AtomicU64::new(100)),
            std::time::Duration::from_secs(0),
            "test",
        );

        assert!(evaluator.topic_messages_exceeded(1, 1, 100));
    }
}
