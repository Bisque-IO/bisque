use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MultiRaftConfig {
    /// Interval at which coalesced heartbeats are sent.
    pub heartbeat_interval: Duration,
}

impl Default for MultiRaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(100),
        }
    }
}
