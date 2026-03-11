use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::types::{InputSource, OverlapPolicy, RetryConfig};

/// Top-level configuration for the MQ engine.
#[derive(Debug, Clone)]
pub struct MqConfig {
    pub data_dir: PathBuf,
    /// Catalog identity for metrics isolation. Set via `with_catalog()`.
    pub catalog_id: u64,
    /// Catalog name for metrics labels. Set via `with_catalog()`.
    pub catalog_name: String,
    pub visibility_scan_interval: Duration,
    pub cron_eval_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub actor_eviction_interval: Duration,
    pub dedup_prune_interval: Duration,
    pub purge_floor_interval: Duration,
    pub actor_rebalance_interval: Duration,
    pub job_timeout_interval: Duration,
    /// How often to check for expired consumer group sessions (default: 5s).
    pub group_session_expiry_interval: Duration,
    /// How often to check for expired consumer group offsets (default: 10 min).
    pub group_offset_expiry_interval: Duration,
    /// How long to retain offsets for empty groups (default: 7 days).
    pub group_offset_retention_ms: u64,
}

impl MqConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            catalog_id: 0,
            catalog_name: "default".to_string(),
            visibility_scan_interval: Duration::from_secs(1),
            cron_eval_interval: Duration::from_secs(1),
            heartbeat_timeout: Duration::from_secs(30),
            actor_eviction_interval: Duration::from_secs(60),
            dedup_prune_interval: Duration::from_secs(30),
            purge_floor_interval: Duration::from_secs(30),
            actor_rebalance_interval: Duration::from_secs(10),
            job_timeout_interval: Duration::from_secs(5),
            group_session_expiry_interval: Duration::from_secs(5),
            group_offset_expiry_interval: Duration::from_secs(600),
            group_offset_retention_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
        }
    }

    pub fn with_visibility_scan_interval(mut self, interval: Duration) -> Self {
        self.visibility_scan_interval = interval;
        self
    }

    pub fn with_cron_eval_interval(mut self, interval: Duration) -> Self {
        self.cron_eval_interval = interval;
        self
    }

    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    pub fn with_catalog(mut self, catalog_id: u64, catalog_name: String) -> Self {
        self.catalog_id = catalog_id;
        self.catalog_name = catalog_name;
        self
    }
}

/// Configuration for a message queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    #[serde(default = "default_visibility_timeout_ms")]
    pub visibility_timeout_ms: u64,
    #[serde(default = "default_queue_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub dead_letter_topic_id: Option<u64>,
    #[serde(default)]
    pub dedup_window_secs: Option<u64>,
    #[serde(default)]
    pub delay_default_ms: u64,
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight_per_consumer: u32,
}

fn default_visibility_timeout_ms() -> u64 {
    30_000
}
fn default_queue_max_retries() -> u32 {
    3
}
fn default_max_in_flight() -> u32 {
    100
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            visibility_timeout_ms: default_visibility_timeout_ms(),
            max_retries: default_queue_max_retries(),
            dead_letter_topic_id: None,
            dedup_window_secs: None,
            delay_default_ms: 0,
            max_in_flight_per_consumer: default_max_in_flight(),
        }
    }
}

/// Configuration for an actor namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorConfig {
    #[serde(default = "default_max_mailbox_depth")]
    pub max_mailbox_depth: u32,
    #[serde(default = "default_idle_eviction_secs")]
    pub idle_eviction_secs: u64,
    #[serde(default = "default_actor_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
    #[serde(default = "default_actor_max_retries")]
    pub max_retries: u32,
}

fn default_max_mailbox_depth() -> u32 {
    10_000
}
fn default_idle_eviction_secs() -> u64 {
    3600
}
fn default_actor_ack_timeout_ms() -> u64 {
    30_000
}
fn default_actor_max_retries() -> u32 {
    3
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            max_mailbox_depth: default_max_mailbox_depth(),
            idle_eviction_secs: default_idle_eviction_secs(),
            ack_timeout_ms: default_actor_ack_timeout_ms(),
            max_retries: default_actor_max_retries(),
        }
    }
}

/// Configuration for a cron job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    pub cron_expression: String,
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default = "default_execution_timeout_ms")]
    pub execution_timeout_ms: u64,
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    #[serde(default = "default_max_queued")]
    pub max_queued: u32,
    #[serde(default)]
    pub input_source: Option<InputSource>,
    #[serde(default)]
    pub retry_config: RetryConfig,
}

fn default_timezone() -> String {
    "UTC".to_string()
}
fn default_execution_timeout_ms() -> u64 {
    300_000
}
fn default_max_queued() -> u32 {
    10
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            cron_expression: "0 * * * *".to_string(),
            timezone: default_timezone(),
            execution_timeout_ms: default_execution_timeout_ms(),
            overlap_policy: OverlapPolicy::default(),
            max_queued: default_max_queued(),
            input_source: None,
            retry_config: RetryConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_config_defaults() {
        let config = QueueConfig::default();
        assert_eq!(config.visibility_timeout_ms, 30_000);
        assert_eq!(config.max_retries, 3);
        assert!(config.dead_letter_topic_id.is_none());
        assert!(config.dedup_window_secs.is_none());
        assert_eq!(config.delay_default_ms, 0);
        assert_eq!(config.max_in_flight_per_consumer, 100);
    }

    #[test]
    fn test_actor_config_defaults() {
        let config = ActorConfig::default();
        assert_eq!(config.max_mailbox_depth, 10_000);
        assert_eq!(config.idle_eviction_secs, 3600);
        assert_eq!(config.ack_timeout_ms, 30_000);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_job_config_defaults() {
        let config = JobConfig::default();
        assert_eq!(config.timezone, "UTC");
        assert_eq!(config.execution_timeout_ms, 300_000);
        assert_eq!(config.max_queued, 10);
        assert!(config.input_source.is_none());
    }

    #[test]
    fn test_mq_config_builder() {
        let config = MqConfig::new("/data/mq")
            .with_visibility_scan_interval(Duration::from_secs(5))
            .with_cron_eval_interval(Duration::from_secs(10))
            .with_heartbeat_timeout(Duration::from_secs(60));

        assert_eq!(config.visibility_scan_interval, Duration::from_secs(5));
        assert_eq!(config.cron_eval_interval, Duration::from_secs(10));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_queue_config_serde_roundtrip() {
        let config = QueueConfig {
            visibility_timeout_ms: 60_000,
            max_retries: 5,
            dead_letter_topic_id: Some(42),
            dedup_window_secs: Some(120),
            delay_default_ms: 1000,
            max_in_flight_per_consumer: 50,
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: QueueConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.visibility_timeout_ms, 60_000);
        assert_eq!(decoded.dead_letter_topic_id, Some(42));
        assert_eq!(decoded.dedup_window_secs, Some(120));
    }

    #[test]
    fn test_queue_config_serde_defaults() {
        // Deserialize minimal JSON — defaults should fill in
        let json = "{}";
        let config: QueueConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.visibility_timeout_ms, 30_000);
        assert_eq!(config.max_retries, 3);
        assert!(config.dead_letter_topic_id.is_none());
    }

    #[test]
    fn test_actor_config_serde_roundtrip() {
        let config = ActorConfig {
            max_mailbox_depth: 500,
            idle_eviction_secs: 7200,
            ack_timeout_ms: 60_000,
            max_retries: 5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: ActorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.max_mailbox_depth, 500);
        assert_eq!(decoded.idle_eviction_secs, 7200);
    }

    #[test]
    fn test_job_config_serde_roundtrip() {
        let config = JobConfig {
            cron_expression: "0 0 * * *".to_string(),
            timezone: "US/Eastern".to_string(),
            execution_timeout_ms: 600_000,
            overlap_policy: OverlapPolicy::Queue,
            max_queued: 5,
            input_source: None,
            retry_config: RetryConfig {
                max_retries: 10,
                retry_delay_ms: 10_000,
            },
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: JobConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.cron_expression, "0 0 * * *");
        assert_eq!(decoded.timezone, "US/Eastern");
        assert_eq!(decoded.retry_config.max_retries, 10);
    }
}
