use std::str::FromStr;

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

pub struct JobInstance {
    pub meta: JobMeta,

    // Pre-initialized metrics
    m_trigger_count: metrics::Counter,
    m_complete_count: metrics::Counter,
    m_fail_count: metrics::Counter,
    m_timeout_count: metrics::Counter,
}

impl JobInstance {
    pub fn new(meta: JobMeta) -> Self {
        let labels = [("job", meta.name.clone())];
        let m_trigger_count = metrics::counter!("mq.job.trigger.count", &labels);
        let m_complete_count = metrics::counter!("mq.job.complete.count", &labels);
        let m_fail_count = metrics::counter!("mq.job.fail.count", &labels);
        let m_timeout_count = metrics::counter!("mq.job.timeout.count", &labels);

        Self {
            meta,
            m_trigger_count,
            m_complete_count,
            m_fail_count,
            m_timeout_count,
        }
    }

    pub fn apply_trigger(&mut self, execution_id: u64, triggered_at: u64) {
        self.meta.state.current_execution_id = Some(execution_id);
        self.meta.state.last_triggered_at = Some(triggered_at);
        // Compute next trigger time
        self.meta.state.next_trigger_at =
            compute_next_trigger(&self.meta.config.cron_expression, triggered_at);
        self.m_trigger_count.increment(1);
    }

    pub fn apply_assign(&mut self, consumer_id: u64) {
        self.meta.state.assigned_consumer_id = Some(consumer_id);
    }

    pub fn apply_complete(&mut self, execution_id: u64, current_time: u64) {
        if self.meta.state.current_execution_id == Some(execution_id) {
            self.meta.state.current_execution_id = None;
            self.meta.state.last_completed_at = Some(current_time);
            self.meta.state.consecutive_failures = 0;
            self.m_complete_count.increment(1);

            // Dequeue a queued trigger if any
            if self.meta.state.queued_triggers > 0 {
                self.meta.state.queued_triggers -= 1;
            }
        }
    }

    pub fn apply_fail(&mut self, execution_id: u64) {
        if self.meta.state.current_execution_id == Some(execution_id) {
            self.meta.state.current_execution_id = None;
            self.meta.state.consecutive_failures += 1;
            self.m_fail_count.increment(1);
        }
    }

    pub fn apply_timeout(&mut self, execution_id: u64) {
        if self.meta.state.current_execution_id == Some(execution_id) {
            self.meta.state.current_execution_id = None;
            self.meta.state.consecutive_failures += 1;
            self.meta.state.assigned_consumer_id = None;
            self.m_timeout_count.increment(1);
        }
    }

    /// Check if this job should be triggered now.
    pub fn should_trigger(&self, current_time: u64) -> bool {
        if !self.meta.enabled {
            return false;
        }
        if current_time < self.meta.state.next_trigger_at {
            return false;
        }
        match self.meta.config.overlap_policy {
            OverlapPolicy::Skip => self.meta.state.current_execution_id.is_none(),
            OverlapPolicy::Queue => self.meta.state.queued_triggers < self.meta.config.max_queued,
        }
    }

    /// Check if the current execution has timed out.
    pub fn is_execution_timed_out(&self, current_time: u64) -> bool {
        if let Some(triggered_at) = self.meta.state.last_triggered_at {
            if self.meta.state.current_execution_id.is_some() {
                return current_time > triggered_at + self.meta.config.execution_timeout_ms;
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
        JobInstance::new(meta)
    }

    fn make_job_with_config(name: &str, config: JobConfig) -> JobInstance {
        let meta = JobMeta::new(1, name.to_string(), 1000, config);
        JobInstance::new(meta)
    }

    #[test]
    fn test_job_meta_defaults() {
        let job = make_job("test-job");
        assert_eq!(job.meta.job_id, 1);
        assert_eq!(job.meta.name, "test-job");
        assert!(job.meta.enabled);
        assert!(job.meta.state.current_execution_id.is_none());
        assert_eq!(job.meta.state.consecutive_failures, 0);
    }

    #[test]
    fn test_trigger() {
        let mut job = make_job("test-job");
        job.apply_trigger(100, 5000);

        assert_eq!(job.meta.state.current_execution_id, Some(100));
        assert_eq!(job.meta.state.last_triggered_at, Some(5000));
        assert!(job.meta.state.next_trigger_at > 5000);
    }

    #[test]
    fn test_assign() {
        let mut job = make_job("test-job");
        job.apply_assign(42);
        assert_eq!(job.meta.state.assigned_consumer_id, Some(42));
    }

    #[test]
    fn test_complete() {
        let mut job = make_job("test-job");
        job.apply_trigger(100, 5000);
        job.apply_complete(100, 6000);

        assert!(job.meta.state.current_execution_id.is_none());
        assert_eq!(job.meta.state.last_completed_at, Some(6000));
        assert_eq!(job.meta.state.consecutive_failures, 0);
    }

    #[test]
    fn test_complete_wrong_execution_id() {
        let mut job = make_job("test-job");
        job.apply_trigger(100, 5000);
        job.apply_complete(999, 6000); // wrong ID

        // Should not change state
        assert_eq!(job.meta.state.current_execution_id, Some(100));
        assert!(job.meta.state.last_completed_at.is_none());
    }

    #[test]
    fn test_fail() {
        let mut job = make_job("test-job");
        job.apply_trigger(100, 5000);
        job.apply_fail(100);

        assert!(job.meta.state.current_execution_id.is_none());
        assert_eq!(job.meta.state.consecutive_failures, 1);
    }

    #[test]
    fn test_consecutive_failures() {
        let mut job = make_job("test-job");

        job.apply_trigger(1, 5000);
        job.apply_fail(1);
        assert_eq!(job.meta.state.consecutive_failures, 1);

        job.apply_trigger(2, 6000);
        job.apply_fail(2);
        assert_eq!(job.meta.state.consecutive_failures, 2);

        // Successful completion resets
        job.apply_trigger(3, 7000);
        job.apply_complete(3, 8000);
        assert_eq!(job.meta.state.consecutive_failures, 0);
    }

    #[test]
    fn test_timeout() {
        let mut job = make_job("test-job");
        job.apply_assign(42);
        job.apply_trigger(100, 5000);
        job.apply_timeout(100);

        assert!(job.meta.state.current_execution_id.is_none());
        assert_eq!(job.meta.state.consecutive_failures, 1);
        assert!(job.meta.state.assigned_consumer_id.is_none());
    }

    #[test]
    fn test_should_trigger_disabled() {
        let mut job = make_job("test-job");
        job.meta.enabled = false;
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
        let mut job = make_job("test-job");
        job.meta.state.next_trigger_at = 5000;

        assert!(job.should_trigger(6000));

        // With active execution, skip policy prevents trigger
        job.meta.state.current_execution_id = Some(1);
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
        let mut job = make_job_with_config("test-job", config);
        job.meta.state.next_trigger_at = 5000;
        job.meta.state.current_execution_id = Some(1);

        // Can queue up to max_queued
        assert!(job.should_trigger(6000));

        job.meta.state.queued_triggers = 2;
        assert!(!job.should_trigger(6000));
    }

    #[test]
    fn test_is_execution_timed_out() {
        let mut job = make_job("test-job");
        assert!(!job.is_execution_timed_out(999999));

        job.apply_trigger(100, 5000);
        // execution_timeout_ms default = 300_000
        assert!(!job.is_execution_timed_out(5000 + 299_999));
        assert!(job.is_execution_timed_out(5000 + 300_001));
    }

    #[test]
    fn test_queued_triggers_decrement_on_complete() {
        let mut job = make_job("test-job");
        job.meta.state.queued_triggers = 3;
        job.apply_trigger(100, 5000);
        job.apply_complete(100, 6000);

        assert_eq!(job.meta.state.queued_triggers, 2);
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
}
