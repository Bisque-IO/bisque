use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the parallel (partitioned) apply workers.
#[derive(Debug, Clone)]
pub struct ParallelApplyConfig {
    /// Number of partition workers. Always pre-allocated at startup.
    /// Fixed for the lifetime of the node — no rebalancing, fully deterministic.
    pub num_partitions: usize,
    /// Capacity of each [`ClientPartition`]'s crossfire MPSC channel (in `Bytes` chunks).
    /// Higher values allow more in-flight response chunks before backpressure kicks in.
    pub response_partition_capacity: usize,
    /// Flush the forwarded-response output buffer after this many sub-command entries
    /// have been accumulated by a single [`PartitionWorker`]. Default: 64.
    pub response_flush_entries: usize,
    /// Flush the forwarded-response output buffer after this many bytes have been
    /// accumulated by a single [`PartitionWorker`]. Default: 8192.
    pub response_flush_bytes: usize,
    /// Capacity of the per-follower [`FollowerResponder`] crossfire MPSC channel
    /// (in `Bytes` chunks). Default: 256.
    pub responder_channel_capacity: usize,
}

impl Default for ParallelApplyConfig {
    fn default() -> Self {
        Self {
            num_partitions: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            response_partition_capacity: 4096,
            response_flush_entries: 64,
            response_flush_bytes: 8192,
            responder_channel_capacity: 256,
        }
    }
}

// =============================================================================
// Top-level MQ config
// =============================================================================

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
    pub session_timeout: Duration,
    pub actor_eviction_interval: Duration,
    pub dedup_prune_interval: Duration,
    pub purge_floor_interval: Duration,
    pub actor_rebalance_interval: Duration,
    /// How often to check for expired consumer group sessions (default: 5s).
    pub group_session_expiry_interval: Duration,
    /// How often to check for expired consumer group offsets (default: 10 min).
    pub group_offset_expiry_interval: Duration,
    /// How long to retain offsets for empty groups (default: 7 days).
    pub group_offset_retention_ms: u64,
    /// How often to check for dead sessions and fire wills (default: 5s).
    pub session_expiry_interval: Duration,
    /// Parallel apply configuration.
    pub parallel_apply: ParallelApplyConfig,
    /// How often to evaluate segment retention policies (default: 30s).
    /// Set to Duration::ZERO to disable retention evaluation.
    pub retention_eval_interval: Duration,
}

impl MqConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            catalog_id: 0,
            catalog_name: "default".to_string(),
            visibility_scan_interval: Duration::from_secs(1),
            cron_eval_interval: Duration::from_secs(1),
            session_timeout: Duration::from_secs(30),
            actor_eviction_interval: Duration::from_secs(60),
            dedup_prune_interval: Duration::from_secs(30),
            purge_floor_interval: Duration::from_secs(30),
            actor_rebalance_interval: Duration::from_secs(10),
            group_session_expiry_interval: Duration::from_secs(5),
            group_offset_expiry_interval: Duration::from_secs(600),
            group_offset_retention_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
            session_expiry_interval: Duration::from_secs(5),
            parallel_apply: ParallelApplyConfig::default(),
            retention_eval_interval: Duration::from_secs(30),
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

    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    pub fn with_catalog(mut self, catalog_id: u64, catalog_name: String) -> Self {
        self.catalog_id = catalog_id;
        self.catalog_name = catalog_name;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mq_config_builder() {
        let config = MqConfig::new("/data/mq")
            .with_visibility_scan_interval(Duration::from_secs(5))
            .with_cron_eval_interval(Duration::from_secs(10))
            .with_session_timeout(Duration::from_secs(60));

        assert_eq!(config.visibility_scan_interval, Duration::from_secs(5));
        assert_eq!(config.cron_eval_interval, Duration::from_secs(10));
        assert_eq!(config.session_timeout, Duration::from_secs(60));
    }
}
