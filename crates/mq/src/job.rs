use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::config::JobConfig;
use crate::types::{JobState, OverlapPolicy, name_hash};

// =============================================================================
// Job Metadata (persisted to MDBX)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMeta {
    pub job_id: u64,
    pub name: String,
    pub created_at: u64,
    pub config: JobConfig,
    pub state: JobState,
    pub enabled: bool,
    #[serde(default)]
    pub name_hash: u64,
}

impl JobMeta {
    pub fn new(job_id: u64, name: String, created_at: u64, config: JobConfig) -> Self {
        let hash = name_hash(&name);
        let next_trigger = compute_next_trigger(&config.cron_expression, created_at);
        Self {
            job_id,
            name,
            created_at,
            config,
            state: JobState {
                assigned_consumer_id: None,
                last_triggered_at: None,
                last_completed_at: None,
                next_trigger_at: next_trigger,
                current_execution_id: None,
                consecutive_failures: 0,
                queued_triggers: 0,
            },
            enabled: true,
            name_hash: hash,
        }
    }

    /// Ensure name_hash is populated (for deserialized data that may have default 0).
    pub fn ensure_name_hash(&mut self) {
        if self.name_hash == 0 {
            self.name_hash = name_hash(&self.name);
        }
    }
}

// =============================================================================
// In-memory Job State
// =============================================================================

/// Sentinel for Option<u64> atomics: 0 means None.
const NONE_U64: u64 = 0;

#[inline]
fn opt_to_atomic(v: Option<u64>) -> u64 {
    v.unwrap_or(NONE_U64)
}

#[inline]
fn atomic_to_opt(v: u64) -> Option<u64> {
    if v == NONE_U64 { None } else { Some(v) }
}

/// Lock-free job instance.
///
/// Immutable identity fields are stored directly in `meta`. Mutable state uses
/// atomics for safe concurrent reads (leader tasks checking triggers/timeouts)
/// with single-writer updates (Raft apply path). No `UnsafeCell` needed.
pub struct JobInstance {
    // -- Immutable identity (set once at creation, never changed) --
    pub meta: JobMeta,

    // -- Mutable state (atomics for concurrent readers + single writer) --
    enabled: AtomicBool,
    config: RwLock<JobConfig>,
    pub(crate) assigned_consumer_id: AtomicU64,
    pub(crate) last_triggered_at: AtomicU64,
    pub(crate) last_completed_at: AtomicU64,
    pub(crate) next_trigger_at: AtomicU64,
    pub(crate) current_execution_id: AtomicU64,
    pub(crate) consecutive_failures: AtomicU32,
    pub(crate) queued_triggers: AtomicU32,

    // Pre-initialized metrics
    m_trigger_count: metrics::Counter,
    m_complete_count: metrics::Counter,
    m_fail_count: metrics::Counter,
    m_timeout_count: metrics::Counter,
}

impl JobInstance {
    pub fn new(meta: JobMeta, catalog_name: &str) -> Self {
        let labels = [
            ("catalog", catalog_name.to_owned()),
            ("job", meta.name.clone()),
        ];
        let m_trigger_count = metrics::counter!("mq.job.trigger.count", &labels);
        let m_complete_count = metrics::counter!("mq.job.complete.count", &labels);
        let m_fail_count = metrics::counter!("mq.job.fail.count", &labels);
        let m_timeout_count = metrics::counter!("mq.job.timeout.count", &labels);

        let enabled = AtomicBool::new(meta.enabled);
        let config = RwLock::new(meta.config.clone());
        let assigned_consumer_id = AtomicU64::new(opt_to_atomic(meta.state.assigned_consumer_id));
        let last_triggered_at = AtomicU64::new(opt_to_atomic(meta.state.last_triggered_at));
        let last_completed_at = AtomicU64::new(opt_to_atomic(meta.state.last_completed_at));
        let next_trigger_at = AtomicU64::new(meta.state.next_trigger_at);
        let current_execution_id = AtomicU64::new(opt_to_atomic(meta.state.current_execution_id));
        let consecutive_failures = AtomicU32::new(meta.state.consecutive_failures);
        let queued_triggers = AtomicU32::new(meta.state.queued_triggers);

        Self {
            meta,
            enabled,
            config,
            assigned_consumer_id,
            last_triggered_at,
            last_completed_at,
            next_trigger_at,
            current_execution_id,
            consecutive_failures,
            queued_triggers,
            m_trigger_count,
            m_complete_count,
            m_fail_count,
            m_timeout_count,
        }
    }

    // -- Atomic accessors --

    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn assigned_consumer_id(&self) -> Option<u64> {
        atomic_to_opt(self.assigned_consumer_id.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn last_triggered_at(&self) -> Option<u64> {
        atomic_to_opt(self.last_triggered_at.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn last_completed_at(&self) -> Option<u64> {
        atomic_to_opt(self.last_completed_at.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn next_trigger_at(&self) -> u64 {
        self.next_trigger_at.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn current_execution_id(&self) -> Option<u64> {
        atomic_to_opt(self.current_execution_id.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn queued_triggers(&self) -> u32 {
        self.queued_triggers.load(Ordering::Relaxed)
    }

    /// Build a JobMeta snapshot by flushing atomics back to a cloned meta.
    pub fn snapshot_meta(&self) -> JobMeta {
        let mut m = self.meta.clone();
        m.enabled = self.enabled();
        m.config = self.config.read().clone();
        m.state.assigned_consumer_id = self.assigned_consumer_id();
        m.state.last_triggered_at = self.last_triggered_at();
        m.state.last_completed_at = self.last_completed_at();
        m.state.next_trigger_at = self.next_trigger_at();
        m.state.current_execution_id = self.current_execution_id();
        m.state.consecutive_failures = self.consecutive_failures();
        m.state.queued_triggers = self.queued_triggers();
        m
    }

    // -- Apply methods (single-writer Raft apply path) --

    pub fn apply_trigger(&self, execution_id: u64, triggered_at: u64) {
        self.current_execution_id
            .store(execution_id, Ordering::Relaxed);
        self.last_triggered_at
            .store(triggered_at, Ordering::Relaxed);
        let next = compute_next_trigger(&self.config.read().cron_expression, triggered_at);
        self.next_trigger_at.store(next, Ordering::Relaxed);
        self.m_trigger_count.increment(1);
    }

    pub fn apply_assign(&self, consumer_id: u64) {
        self.assigned_consumer_id
            .store(consumer_id, Ordering::Relaxed);
    }

    pub fn apply_unassign(&self) {
        self.assigned_consumer_id.store(NONE_U64, Ordering::Relaxed);
    }

    pub fn apply_complete(&self, execution_id: u64, current_time: u64) {
        if self.current_execution_id() == Some(execution_id) {
            self.current_execution_id.store(NONE_U64, Ordering::Relaxed);
            self.last_completed_at
                .store(current_time, Ordering::Relaxed);
            self.consecutive_failures.store(0, Ordering::Relaxed);
            self.m_complete_count.increment(1);

            // Dequeue a queued trigger if any
            let qt = self.queued_triggers.load(Ordering::Relaxed);
            if qt > 0 {
                self.queued_triggers.store(qt - 1, Ordering::Relaxed);
            }
        }
    }

    pub fn apply_fail(&self, execution_id: u64) {
        if self.current_execution_id() == Some(execution_id) {
            self.current_execution_id.store(NONE_U64, Ordering::Relaxed);
            self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
            self.m_fail_count.increment(1);
        }
    }

    pub fn apply_timeout(&self, execution_id: u64) {
        if self.current_execution_id() == Some(execution_id) {
            self.current_execution_id.store(NONE_U64, Ordering::Relaxed);
            self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
            self.assigned_consumer_id.store(NONE_U64, Ordering::Relaxed);
            self.m_timeout_count.increment(1);
        }
    }

    pub fn apply_set_enabled(&self, value: bool) {
        self.enabled.store(value, Ordering::Relaxed);
    }

    pub fn apply_set_config(&self, new_config: JobConfig) {
        *self.config.write() = new_config;
    }

    /// Check if this job should be triggered now.
    pub fn should_trigger(&self, current_time: u64) -> bool {
        if !self.enabled() {
            return false;
        }
        if current_time < self.next_trigger_at() {
            return false;
        }
        let config = self.config.read();
        match config.overlap_policy {
            OverlapPolicy::Skip => self.current_execution_id().is_none(),
            OverlapPolicy::Queue => self.queued_triggers() < config.max_queued,
        }
    }

    /// Check if the current execution has timed out.
    pub fn is_execution_timed_out(&self, current_time: u64) -> bool {
        if let Some(triggered_at) = self.last_triggered_at() {
            if self.current_execution_id().is_some() {
                return current_time > triggered_at + self.config.read().execution_timeout_ms;
            }
        }
        false
    }
}

/// Compute the next trigger time from a cron expression.
///
/// Returns `after + 60_000` as a fallback if parsing fails.
pub fn compute_next_trigger(cron_expr: &str, after: u64) -> u64 {
    use chrono::{TimeZone, Utc};

    let schedule = match cron::Schedule::from_str(cron_expr) {
        Ok(s) => s,
        Err(_) => return after + 60_000, // fallback: 1 minute
    };

    let dt = Utc
        .timestamp_millis_opt(after as i64)
        .single()
        .unwrap_or_else(Utc::now);

    schedule
        .after(&dt)
        .next()
        .map(|next| next.timestamp_millis() as u64)
        .unwrap_or(after + 60_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::JobConfig;

    fn make_job(name: &str) -> JobInstance {
        let config = JobConfig {
            cron_expression: "0 * * * * *".to_string(), // every minute (6-field cron)
            ..Default::default()
        };
        let meta = JobMeta::new(1, name.to_string(), 1000, config);
        JobInstance::new(meta, "test")
    }

    fn make_job_with_config(name: &str, config: JobConfig) -> JobInstance {
        let meta = JobMeta::new(1, name.to_string(), 1000, config);
        JobInstance::new(meta, "test")
    }

    #[test]
    fn test_job_meta_defaults() {
        let job = make_job("test-job");
        assert_eq!(job.meta.job_id, 1);
        assert_eq!(job.meta.name, "test-job");
        assert!(job.enabled());
        assert!(job.current_execution_id().is_none());
        assert_eq!(job.consecutive_failures(), 0);
    }

    #[test]
    fn test_trigger() {
        let job = make_job("test-job");
        job.apply_trigger(100, 5000);

        assert_eq!(job.current_execution_id(), Some(100));
        assert_eq!(job.last_triggered_at(), Some(5000));
        assert!(job.next_trigger_at() > 5000);
    }

    #[test]
    fn test_assign() {
        let job = make_job("test-job");
        job.apply_assign(42);
        assert_eq!(job.assigned_consumer_id(), Some(42));
    }

    #[test]
    fn test_complete() {
        let job = make_job("test-job");
        job.apply_trigger(100, 5000);
        job.apply_complete(100, 6000);

        assert!(job.current_execution_id().is_none());
        assert_eq!(job.last_completed_at(), Some(6000));
        assert_eq!(job.consecutive_failures(), 0);
    }

    #[test]
    fn test_complete_wrong_execution_id() {
        let job = make_job("test-job");
        job.apply_trigger(100, 5000);
        job.apply_complete(999, 6000); // wrong ID

        // Should not change state
        assert_eq!(job.current_execution_id(), Some(100));
        assert!(job.last_completed_at().is_none());
    }

    #[test]
    fn test_fail() {
        let job = make_job("test-job");
        job.apply_trigger(100, 5000);
        job.apply_fail(100);

        assert!(job.current_execution_id().is_none());
        assert_eq!(job.consecutive_failures(), 1);
    }

    #[test]
    fn test_consecutive_failures() {
        let job = make_job("test-job");

        job.apply_trigger(1, 5000);
        job.apply_fail(1);
        assert_eq!(job.consecutive_failures(), 1);

        job.apply_trigger(2, 6000);
        job.apply_fail(2);
        assert_eq!(job.consecutive_failures(), 2);

        // Successful completion resets
        job.apply_trigger(3, 7000);
        job.apply_complete(3, 8000);
        assert_eq!(job.consecutive_failures(), 0);
    }

    #[test]
    fn test_timeout() {
        let job = make_job("test-job");
        job.apply_assign(42);
        job.apply_trigger(100, 5000);
        job.apply_timeout(100);

        assert!(job.current_execution_id().is_none());
        assert_eq!(job.consecutive_failures(), 1);
        assert!(job.assigned_consumer_id().is_none());
    }

    #[test]
    fn test_should_trigger_disabled() {
        let job = make_job("test-job");
        job.apply_set_enabled(false);
        assert!(!job.should_trigger(999999));
    }

    #[test]
    fn test_should_trigger_not_yet_due() {
        let job = make_job("test-job");
        // next_trigger_at is computed from cron, should be > 1000
        assert!(!job.should_trigger(0));
    }

    #[test]
    fn test_should_trigger_skip_policy() {
        let job = make_job("test-job");
        job.next_trigger_at.store(5000, Ordering::Relaxed);

        assert!(job.should_trigger(6000));

        // With active execution, skip policy prevents trigger
        job.current_execution_id.store(1, Ordering::Relaxed);
        assert!(!job.should_trigger(6000));
    }

    #[test]
    fn test_should_trigger_queue_policy() {
        let config = JobConfig {
            cron_expression: "0 * * * * *".to_string(),
            overlap_policy: OverlapPolicy::Queue,
            max_queued: 2,
            ..Default::default()
        };
        let job = make_job_with_config("test-job", config);
        job.next_trigger_at.store(5000, Ordering::Relaxed);
        job.current_execution_id.store(1, Ordering::Relaxed);

        // Can queue up to max_queued
        assert!(job.should_trigger(6000));

        job.queued_triggers.store(2, Ordering::Relaxed);
        assert!(!job.should_trigger(6000));
    }

    #[test]
    fn test_is_execution_timed_out() {
        let job = make_job("test-job");
        assert!(!job.is_execution_timed_out(999999));

        job.apply_trigger(100, 5000);
        // execution_timeout_ms default = 300_000
        assert!(!job.is_execution_timed_out(5000 + 299_999));
        assert!(job.is_execution_timed_out(5000 + 300_001));
    }

    #[test]
    fn test_queued_triggers_decrement_on_complete() {
        let job = make_job("test-job");
        job.queued_triggers.store(3, Ordering::Relaxed);
        job.apply_trigger(100, 5000);
        job.apply_complete(100, 6000);

        assert_eq!(job.queued_triggers(), 2);
    }

    #[test]
    fn test_compute_next_trigger_invalid_cron() {
        let result = compute_next_trigger("invalid-cron", 1000);
        assert_eq!(result, 1000 + 60_000);
    }

    #[test]
    fn test_compute_next_trigger_valid_cron() {
        // Every minute
        let result = compute_next_trigger("0 * * * * *", 1000);
        assert!(result > 1000);
    }

    #[test]
    fn test_snapshot_meta_roundtrip() {
        let job = make_job("test-job");
        job.apply_trigger(100, 5000);
        job.apply_assign(42);

        let snap = job.snapshot_meta();
        assert_eq!(snap.state.current_execution_id, Some(100));
        assert_eq!(snap.state.last_triggered_at, Some(5000));
        assert_eq!(snap.state.assigned_consumer_id, Some(42));
        assert!(snap.enabled);
    }
}
