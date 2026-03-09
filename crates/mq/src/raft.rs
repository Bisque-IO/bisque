//! MQ Raft node wrapper with leader-driven background tasks.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use openraft::Raft;
use openraft::async_runtime::WatchReceiver;
use parking_lot::RwLock;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::MqTypeConfig;
use crate::config::MqConfig;
use crate::engine::MqEngine;
use crate::manifest::MqManifestManager;
use crate::types::MqCommand;

/// Raft node wrapper for bisque-mq.
///
/// Manages the Raft handle and leader-driven background tasks for:
/// - Visibility timeout scanning
/// - Cron job evaluation
/// - Consumer heartbeat monitoring
/// - Job execution timeout checking
/// - Actor idle eviction
/// - Dedup window pruning
/// - Actor rebalancing
pub struct MqRaftNode {
    raft: Raft<MqTypeConfig>,
    node_id: u64,
    group_id: u64,
    config: MqConfig,
    engine: Option<Arc<RwLock<MqEngine>>>,
    manifest: Option<Arc<MqManifestManager>>,
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
    task_handles: parking_lot::Mutex<Vec<JoinHandle<()>>>,
    execution_counter: Arc<AtomicU64>,
}

impl MqRaftNode {
    pub fn new(raft: Raft<MqTypeConfig>, node_id: u64, config: MqConfig) -> Self {
        Self {
            raft,
            node_id,
            group_id: 0,
            config,
            engine: None,
            manifest: None,
            shutdown: Arc::new(Notify::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            task_handles: parking_lot::Mutex::new(Vec::new()),
            execution_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = group_id;
        self
    }

    pub fn with_manifest(mut self, manifest: Arc<MqManifestManager>) -> Self {
        self.manifest = Some(manifest);
        self
    }

    pub fn with_engine(mut self, engine: Arc<RwLock<MqEngine>>) -> Self {
        self.engine = Some(engine);
        self
    }

    pub fn raft(&self) -> &Raft<MqTypeConfig> {
        &self.raft
    }

    /// Start leader-driven background tasks.
    pub fn start(&self) {
        let engine = match &self.engine {
            Some(e) => Arc::clone(e),
            None => {
                warn!("MqRaftNode::start() called without engine — leader tasks disabled");
                return;
            }
        };

        let mut handles = self.task_handles.lock();

        // 1. Visibility timeout scanner
        {
            let engine = Arc::clone(&engine);
            handles.push(self.spawn_leader_task(
                "mq-visibility-scan",
                self.config.visibility_scan_interval,
                move |raft, _node_id| {
                    let engine = Arc::clone(&engine);
                    Box::pin(async move {
                        Self::scan_visibility_timeouts(&raft, &engine).await;
                    })
                },
            ));
        }

        // 2. Cron job evaluator
        {
            let engine = Arc::clone(&engine);
            let counter = Arc::clone(&self.execution_counter);
            handles.push(self.spawn_leader_task(
                "mq-cron-eval",
                self.config.cron_eval_interval,
                move |raft, _node_id| {
                    let engine = Arc::clone(&engine);
                    let counter = Arc::clone(&counter);
                    Box::pin(async move {
                        Self::evaluate_cron_jobs(&raft, &engine, &counter).await;
                    })
                },
            ));
        }

        // 3. Consumer heartbeat monitor
        {
            let engine = Arc::clone(&engine);
            let timeout = self.config.heartbeat_timeout;
            handles.push(self.spawn_leader_task(
                "mq-heartbeat-monitor",
                Duration::from_secs(5),
                move |raft, _node_id| {
                    let engine = Arc::clone(&engine);
                    Box::pin(async move {
                        Self::check_consumer_heartbeats(&raft, &engine, timeout).await;
                    })
                },
            ));
        }

        // 4. Job execution timeout
        {
            let engine = Arc::clone(&engine);
            handles.push(self.spawn_leader_task(
                "mq-job-timeout",
                self.config.job_timeout_interval,
                move |raft, _node_id| {
                    let engine = Arc::clone(&engine);
                    Box::pin(async move {
                        Self::check_job_timeouts(&raft, &engine).await;
                    })
                },
            ));
        }

        // 5. Dedup window pruning
        {
            let engine = Arc::clone(&engine);
            handles.push(self.spawn_leader_task(
                "mq-dedup-prune",
                self.config.dedup_prune_interval,
                move |raft, _node_id| {
                    let engine = Arc::clone(&engine);
                    Box::pin(async move {
                        Self::prune_dedup_windows(&raft, &engine).await;
                    })
                },
            ));
        }

        // 6. Actor idle eviction
        {
            let engine = Arc::clone(&engine);
            handles.push(self.spawn_leader_task(
                "mq-actor-eviction",
                self.config.actor_eviction_interval,
                move |raft, _node_id| {
                    let engine = Arc::clone(&engine);
                    Box::pin(async move {
                        Self::evict_idle_actors(&raft, &engine).await;
                    })
                },
            ));
        }

        // 7. Actor rebalancing
        {
            let engine = Arc::clone(&engine);
            handles.push(self.spawn_leader_task(
                "mq-actor-rebalance",
                self.config.actor_rebalance_interval,
                move |raft, _node_id| {
                    let engine = Arc::clone(&engine);
                    Box::pin(async move {
                        Self::rebalance_actors(&raft, &engine).await;
                    })
                },
            ));
        }

        info!(
            node_id = self.node_id,
            group_id = self.group_id,
            "MQ raft node started with {} background tasks",
            handles.len()
        );
    }

    fn spawn_leader_task<F>(
        &self,
        name: &'static str,
        interval: Duration,
        task_fn: F,
    ) -> JoinHandle<()>
    where
        F: Fn(
                Raft<MqTypeConfig>,
                u64,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let raft = self.raft.clone();
        let node_id = self.node_id;
        let shutdown = self.shutdown.clone();
        let shutdown_flag = self.shutdown_flag.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if shutdown_flag.load(Ordering::Acquire) {
                            break;
                        }
                        // Only run on leader
                        let metrics = raft.metrics().borrow_watched().clone();
                        if metrics.current_leader != Some(node_id) {
                            continue;
                        }
                        task_fn(raft.clone(), node_id).await;
                    }
                    _ = shutdown.notified() => {
                        break;
                    }
                }
            }
            debug!(name, "leader task exiting");
        })
    }

    /// Write a command through raft consensus.
    pub async fn write(&self, cmd: MqCommand) -> Result<crate::types::MqResponse, String> {
        match self.raft.client_write(cmd).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => Err(format!("raft write error: {}", e)),
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        let handles: Vec<_> = {
            let mut guard = self.task_handles.lock();
            guard.drain(..).collect()
        };
        for handle in handles {
            let _ = handle.await;
        }
        info!(node_id = self.node_id, "MQ raft node shut down");
    }

    // =========================================================================
    // Leader task implementations
    // =========================================================================

    async fn scan_visibility_timeouts(raft: &Raft<MqTypeConfig>, engine: &Arc<RwLock<MqEngine>>) {
        let commands = {
            let eng = engine.read();
            collect_visibility_timeouts(&eng, unix_ms())
        };
        if !commands.is_empty() {
            debug!(
                count = commands.len(),
                "proposing visibility timeout commands"
            );
            let _ = raft.client_write(MqCommand::Batch(commands)).await;
        }
    }

    async fn evaluate_cron_jobs(
        raft: &Raft<MqTypeConfig>,
        engine: &Arc<RwLock<MqEngine>>,
        counter: &AtomicU64,
    ) {
        let commands = {
            let eng = engine.read();
            collect_due_jobs(&eng, unix_ms(), counter)
        };
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing cron trigger commands");
            let _ = raft.client_write(MqCommand::Batch(commands)).await;
        }
    }

    async fn check_consumer_heartbeats(
        raft: &Raft<MqTypeConfig>,
        engine: &Arc<RwLock<MqEngine>>,
        timeout: Duration,
    ) {
        let commands = {
            let eng = engine.read();
            collect_dead_consumers(&eng, unix_ms(), timeout.as_millis() as u64)
        };
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing dead consumer commands");
            let _ = raft.client_write(MqCommand::Batch(commands)).await;
        }
    }

    async fn check_job_timeouts(raft: &Raft<MqTypeConfig>, engine: &Arc<RwLock<MqEngine>>) {
        let commands = {
            let eng = engine.read();
            collect_timed_out_jobs(&eng, unix_ms())
        };
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing job timeout commands");
            let _ = raft.client_write(MqCommand::Batch(commands)).await;
        }
    }

    async fn prune_dedup_windows(raft: &Raft<MqTypeConfig>, engine: &Arc<RwLock<MqEngine>>) {
        let commands = {
            let eng = engine.read();
            collect_dedup_prune_commands(&eng, unix_ms())
        };
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing dedup prune commands");
            let _ = raft.client_write(MqCommand::Batch(commands)).await;
        }
    }

    async fn evict_idle_actors(raft: &Raft<MqTypeConfig>, engine: &Arc<RwLock<MqEngine>>) {
        let commands = {
            let eng = engine.read();
            collect_idle_actor_evictions(&eng, unix_ms())
        };
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing actor eviction commands");
            let _ = raft.client_write(MqCommand::Batch(commands)).await;
        }
    }

    async fn rebalance_actors(raft: &Raft<MqTypeConfig>, engine: &Arc<RwLock<MqEngine>>) {
        let commands = {
            let eng = engine.read();
            collect_actor_rebalance(&eng)
        };
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing actor rebalance commands");
            let _ = raft.client_write(MqCommand::Batch(commands)).await;
        }
    }
}

impl Drop for MqRaftNode {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
    }
}

// =============================================================================
// Testable command collectors
// =============================================================================

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Collect TimeoutExpired commands for all queues with expired in-flight messages.
pub(crate) fn collect_visibility_timeouts(engine: &MqEngine, now_ms: u64) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    for (&queue_id, queue) in &engine.queues {
        let expired = queue.find_expired_messages(now_ms);
        if !expired.is_empty() {
            commands.push(MqCommand::TimeoutExpired {
                queue_id,
                message_ids: expired.into_vec(),
            });
        }
    }
    commands
}

/// Collect TriggerJob commands for all due jobs.
pub(crate) fn collect_due_jobs(
    engine: &MqEngine,
    now_ms: u64,
    counter: &AtomicU64,
) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    for (&job_id, job) in &engine.jobs {
        if job.should_trigger(now_ms) {
            let execution_id = counter.fetch_add(1, Ordering::Relaxed);
            commands.push(MqCommand::TriggerJob {
                job_id,
                execution_id,
                triggered_at: now_ms,
            });
        }
    }
    commands
}

/// Collect commands for dead consumers: timeout their in-flight messages,
/// timeout their assigned jobs, then disconnect them.
pub(crate) fn collect_dead_consumers(
    engine: &MqEngine,
    now_ms: u64,
    timeout_ms: u64,
) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    for (&consumer_id, consumer) in &engine.consumers {
        if !consumer.is_dead(now_ms, timeout_ms) {
            continue;
        }

        // Timeout in-flight queue messages for this consumer
        for (&queue_id, queue) in &engine.queues {
            let in_flight = queue.consumer_in_flight_ids(consumer_id);
            if !in_flight.is_empty() {
                commands.push(MqCommand::TimeoutExpired {
                    queue_id,
                    message_ids: in_flight.into_vec(),
                });
            }
        }

        // Timeout assigned jobs
        for &job_id in &consumer.meta.assigned_jobs {
            if let Some(job) = engine.jobs.get(&job_id) {
                if let Some(execution_id) = job.meta.state.current_execution_id {
                    commands.push(MqCommand::TimeoutJob {
                        job_id,
                        execution_id,
                    });
                }
            }
        }

        // Release actors assigned to this consumer
        for (&namespace_id, ns) in &engine.actor_namespaces {
            if ns.consumer_assignments.contains_key(&consumer_id) {
                commands.push(MqCommand::ReleaseActors {
                    namespace_id,
                    consumer_id,
                });
            }
        }

        // Disconnect the consumer
        commands.push(MqCommand::DisconnectConsumer { consumer_id });
    }
    commands
}

/// Collect TimeoutJob commands for jobs whose execution has timed out.
pub(crate) fn collect_timed_out_jobs(engine: &MqEngine, now_ms: u64) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    for (&job_id, job) in &engine.jobs {
        if job.is_execution_timed_out(now_ms) {
            if let Some(execution_id) = job.meta.state.current_execution_id {
                commands.push(MqCommand::TimeoutJob {
                    job_id,
                    execution_id,
                });
            }
        }
    }
    commands
}

/// Collect PruneDedupWindow commands for queues with active dedup windows.
pub(crate) fn collect_dedup_prune_commands(engine: &MqEngine, now_ms: u64) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    for (&queue_id, queue) in &engine.queues {
        let window_secs = queue.dedup.window_secs;
        if window_secs > 0 && !queue.dedup.buckets.is_empty() {
            let before_timestamp = now_ms.saturating_sub(window_secs * 1000);
            commands.push(MqCommand::PruneDedupWindow {
                queue_id,
                before_timestamp,
            });
        }
    }
    commands
}

/// Collect EvictIdleActors commands for all actor namespaces.
pub(crate) fn collect_idle_actor_evictions(engine: &MqEngine, now_ms: u64) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    for (&namespace_id, ns) in &engine.actor_namespaces {
        let eviction_secs = ns.meta.config.idle_eviction_secs;
        if eviction_secs > 0 {
            let before_timestamp = now_ms.saturating_sub(eviction_secs * 1000);
            commands.push(MqCommand::EvictIdleActors {
                namespace_id,
                before_timestamp,
            });
        }
    }
    commands
}

/// Collect AssignActors commands to distribute unassigned actors among subscribed consumers.
pub(crate) fn collect_actor_rebalance(engine: &MqEngine) -> Vec<MqCommand> {
    use crate::types::EntityType;

    let mut commands = Vec::new();
    for (&namespace_id, ns) in &engine.actor_namespaces {
        let unassigned = ns.unassigned_actors_with_messages();
        if unassigned.is_empty() {
            continue;
        }

        // Find consumers subscribed to this actor namespace
        let subscribed_consumers: Vec<u64> = engine
            .consumers
            .iter()
            .filter(|(_, c)| {
                c.meta.subscriptions.iter().any(|s| {
                    s.entity_type == EntityType::ActorNamespace && s.entity_id == namespace_id
                })
            })
            .map(|(&id, _)| id)
            .collect();

        if subscribed_consumers.is_empty() {
            continue;
        }

        // Round-robin distribute unassigned actors among consumers
        let actor_ids: Vec<_> = unassigned.into_iter().cloned().collect();
        let num_consumers = subscribed_consumers.len();

        // Group by target consumer
        let mut per_consumer: std::collections::HashMap<u64, Vec<bytes::Bytes>> =
            std::collections::HashMap::new();
        for (i, actor_id) in actor_ids.into_iter().enumerate() {
            let consumer_id = subscribed_consumers[i % num_consumers];
            per_consumer.entry(consumer_id).or_default().push(actor_id);
        }

        for (consumer_id, actor_ids) in per_consumer {
            commands.push(MqCommand::AssignActors {
                namespace_id,
                consumer_id,
                actor_ids,
            });
        }
    }
    commands
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActorConfig, JobConfig, MqConfig, QueueConfig};
    use crate::types::{EntityType, MessagePayload, OverlapPolicy, Subscription};
    use bytes::Bytes;

    fn make_engine() -> MqEngine {
        MqEngine::new(MqConfig::new("/tmp/test-mq"))
    }

    fn make_msg() -> MessagePayload {
        MessagePayload {
            key: None,
            value: Bytes::from_static(b"test"),
            headers: Vec::new(),
            timestamp: 1000,
            ttl_ms: None,
            routing_key: None,
        }
    }

    // =========================================================================
    // Visibility timeout tests
    // =========================================================================

    #[test]
    fn test_visibility_scan_no_expired() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg()],
                dedup_keys: vec![None],
            },
            2,
            1000,
        );
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 10,
            },
            3,
            1000,
        );

        // Not expired yet (visibility_timeout_ms = 30_000)
        let cmds = collect_visibility_timeouts(&engine, 2000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_visibility_scan_with_expired() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg()],
                dedup_keys: vec![None],
            },
            2,
            1000,
        );
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 10,
            },
            3,
            1000,
        );

        // Expired (deadline = 1000 + 30000 = 31000)
        let cmds = collect_visibility_timeouts(&engine, 32000);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            MqCommand::TimeoutExpired {
                queue_id,
                message_ids,
            } => {
                assert_eq!(*queue_id, 1);
                assert_eq!(message_ids.len(), 1);
            }
            _ => panic!("expected TimeoutExpired"),
        }
    }

    #[test]
    fn test_visibility_scan_multiple_queues() {
        let mut engine = make_engine();
        // Create two queues
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q2".to_string(),
                config: QueueConfig::default(),
            },
            2,
            1000,
        );

        // Enqueue and deliver in both
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg()],
                dedup_keys: vec![None],
            },
            3,
            1000,
        );
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 10,
            },
            4,
            1000,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 2,
                messages: vec![make_msg()],
                dedup_keys: vec![None],
            },
            5,
            1000,
        );
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 2,
                consumer_id: 100,
                max_count: 10,
            },
            6,
            1000,
        );

        let cmds = collect_visibility_timeouts(&engine, 32000);
        assert_eq!(cmds.len(), 2);
    }

    // =========================================================================
    // Cron evaluation tests
    // =========================================================================

    #[test]
    fn test_cron_eval_not_due() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateJob {
                name: "job1".to_string(),
                config: JobConfig {
                    cron_expression: "0 * * * * *".to_string(),
                    ..Default::default()
                },
            },
            1,
            1000,
        );

        // not due yet (next_trigger_at computed from cron, far in the future from epoch 1000ms)
        let counter = AtomicU64::new(1);
        let cmds = collect_due_jobs(&engine, 1000, &counter);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_cron_eval_due() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateJob {
                name: "job1".to_string(),
                config: JobConfig {
                    cron_expression: "0 * * * * *".to_string(),
                    ..Default::default()
                },
            },
            1,
            1000,
        );

        // Force the job to be due by setting next_trigger_at
        if let Some(job) = engine.jobs.get_mut(&1) {
            job.meta.state.next_trigger_at = 5000;
        }

        let counter = AtomicU64::new(100);
        let cmds = collect_due_jobs(&engine, 6000, &counter);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            MqCommand::TriggerJob {
                job_id,
                execution_id,
                triggered_at,
            } => {
                assert_eq!(*job_id, 1);
                assert_eq!(*execution_id, 100);
                assert_eq!(*triggered_at, 6000);
            }
            _ => panic!("expected TriggerJob"),
        }
        // Counter should have been incremented
        assert_eq!(counter.load(Ordering::Relaxed), 101);
    }

    #[test]
    fn test_cron_eval_skip_policy_blocks() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateJob {
                name: "job1".to_string(),
                config: JobConfig {
                    cron_expression: "0 * * * * *".to_string(),
                    overlap_policy: OverlapPolicy::Skip,
                    ..Default::default()
                },
            },
            1,
            1000,
        );

        // Set due and currently executing
        if let Some(job) = engine.jobs.get_mut(&1) {
            job.meta.state.next_trigger_at = 5000;
            job.meta.state.current_execution_id = Some(42);
        }

        let counter = AtomicU64::new(1);
        let cmds = collect_due_jobs(&engine, 6000, &counter);
        assert!(cmds.is_empty());
    }

    // =========================================================================
    // Consumer heartbeat tests
    // =========================================================================

    #[test]
    fn test_heartbeat_alive() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "group1".to_string(),
                subscriptions: Vec::new(),
            },
            1,
            1000,
        );

        let cmds = collect_dead_consumers(&engine, 2000, 30_000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_heartbeat_dead_consumer() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "group1".to_string(),
                subscriptions: Vec::new(),
            },
            1,
            1000,
        );

        // Dead after timeout (connected_at=1000, timeout=30_000)
        let cmds = collect_dead_consumers(&engine, 32000, 30_000);
        assert!(!cmds.is_empty());
        // Last command should be DisconnectConsumer
        assert!(matches!(
            cmds.last().unwrap(),
            MqCommand::DisconnectConsumer { consumer_id: 100 }
        ));
    }

    #[test]
    fn test_heartbeat_dead_with_in_flight() {
        let mut engine = make_engine();
        // Create queue and register consumer
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "group1".to_string(),
                subscriptions: vec![Subscription {
                    entity_type: EntityType::Queue,
                    entity_id: 1,
                }],
            },
            2,
            1000,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg()],
                dedup_keys: vec![None],
            },
            3,
            1000,
        );
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 10,
            },
            4,
            1000,
        );

        let cmds = collect_dead_consumers(&engine, 32000, 30_000);
        // Should have: TimeoutExpired for queue messages + DisconnectConsumer
        assert!(cmds.len() >= 2);
        assert!(
            cmds.iter()
                .any(|c| matches!(c, MqCommand::TimeoutExpired { .. }))
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, MqCommand::DisconnectConsumer { .. }))
        );
    }

    #[test]
    fn test_heartbeat_dead_with_jobs() {
        let mut engine = make_engine();
        // Create job
        engine.apply_command(
            MqCommand::CreateJob {
                name: "job1".to_string(),
                config: JobConfig::default(),
            },
            1,
            1000,
        );
        // Register consumer with job assignment
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "group1".to_string(),
                subscriptions: vec![Subscription {
                    entity_type: EntityType::Job,
                    entity_id: 1,
                }],
            },
            2,
            1000,
        );
        // Assign and trigger job
        engine.apply_command(
            MqCommand::AssignJob {
                job_id: 1,
                consumer_id: 100,
            },
            3,
            1000,
        );
        engine.apply_command(
            MqCommand::TriggerJob {
                job_id: 1,
                execution_id: 42,
                triggered_at: 1000,
            },
            4,
            1000,
        );
        // Add assigned job to consumer
        if let Some(consumer) = engine.consumers.get_mut(&100) {
            consumer.meta.assigned_jobs.push(1);
        }

        let cmds = collect_dead_consumers(&engine, 32000, 30_000);
        assert!(
            cmds.iter()
                .any(|c| matches!(c, MqCommand::TimeoutJob { .. }))
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, MqCommand::DisconnectConsumer { .. }))
        );
    }

    // =========================================================================
    // Job timeout tests
    // =========================================================================

    #[test]
    fn test_job_timeout_not_expired() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateJob {
                name: "job1".to_string(),
                config: JobConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::TriggerJob {
                job_id: 1,
                execution_id: 42,
                triggered_at: 1000,
            },
            2,
            1000,
        );

        // execution_timeout_ms = 300_000, not expired
        let cmds = collect_timed_out_jobs(&engine, 100_000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_job_timeout_expired() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateJob {
                name: "job1".to_string(),
                config: JobConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::TriggerJob {
                job_id: 1,
                execution_id: 42,
                triggered_at: 1000,
            },
            2,
            1000,
        );

        // execution_timeout_ms = 300_000, triggered_at = 1000
        let cmds = collect_timed_out_jobs(&engine, 302_000);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            MqCommand::TimeoutJob {
                job_id,
                execution_id,
            } => {
                assert_eq!(*job_id, 1);
                assert_eq!(*execution_id, 42);
            }
            _ => panic!("expected TimeoutJob"),
        }
    }

    // =========================================================================
    // Dedup prune tests
    // =========================================================================

    #[test]
    fn test_dedup_prune_no_dedup() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(), // no dedup
            },
            1,
            1000,
        );

        let cmds = collect_dedup_prune_commands(&engine, 100_000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_dedup_prune() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig {
                    dedup_window_secs: Some(60),
                    ..Default::default()
                },
            },
            1,
            1000,
        );
        // Insert a dedup key
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg()],
                dedup_keys: vec![Some(Bytes::from_static(b"key1"))],
            },
            2,
            1000,
        );

        let cmds = collect_dedup_prune_commands(&engine, 100_000);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            MqCommand::PruneDedupWindow {
                queue_id,
                before_timestamp,
            } => {
                assert_eq!(*queue_id, 1);
                // before_timestamp = 100_000 - 60*1000 = 40_000
                assert_eq!(*before_timestamp, 40_000);
            }
            _ => panic!("expected PruneDedupWindow"),
        }
    }

    // =========================================================================
    // Actor eviction tests
    // =========================================================================

    #[test]
    fn test_actor_eviction() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns1".to_string(),
                config: ActorConfig {
                    idle_eviction_secs: 3600,
                    ..Default::default()
                },
            },
            1,
            1000,
        );

        let cmds = collect_idle_actor_evictions(&engine, 5_000_000);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            MqCommand::EvictIdleActors {
                namespace_id,
                before_timestamp,
            } => {
                assert_eq!(*namespace_id, 1);
                assert_eq!(*before_timestamp, 5_000_000 - 3600 * 1000);
            }
            _ => panic!("expected EvictIdleActors"),
        }
    }

    #[test]
    fn test_actor_eviction_zero_window() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns1".to_string(),
                config: ActorConfig {
                    idle_eviction_secs: 0,
                    ..Default::default()
                },
            },
            1,
            1000,
        );

        let cmds = collect_idle_actor_evictions(&engine, 5_000_000);
        assert!(cmds.is_empty());
    }

    // =========================================================================
    // Actor rebalance tests
    // =========================================================================

    #[test]
    fn test_actor_rebalance_no_consumers() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns1".to_string(),
                config: ActorConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::SendToActor {
                namespace_id: 1,
                actor_id: Bytes::from_static(b"actor1"),
                message: make_msg(),
            },
            2,
            1000,
        );

        let cmds = collect_actor_rebalance(&engine);
        // No consumers subscribed, so no rebalance
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_actor_rebalance_with_consumer() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns1".to_string(),
                config: ActorConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::SendToActor {
                namespace_id: 1,
                actor_id: Bytes::from_static(b"actor1"),
                message: make_msg(),
            },
            2,
            1000,
        );
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "workers".to_string(),
                subscriptions: vec![Subscription {
                    entity_type: EntityType::ActorNamespace,
                    entity_id: 1,
                }],
            },
            3,
            1000,
        );

        let cmds = collect_actor_rebalance(&engine);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            MqCommand::AssignActors {
                namespace_id,
                consumer_id,
                actor_ids,
            } => {
                assert_eq!(*namespace_id, 1);
                assert_eq!(*consumer_id, 100);
                assert_eq!(actor_ids.len(), 1);
                assert_eq!(actor_ids[0], Bytes::from_static(b"actor1"));
            }
            _ => panic!("expected AssignActors"),
        }
    }

    #[test]
    fn test_actor_rebalance_multiple_consumers() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns1".to_string(),
                config: ActorConfig::default(),
            },
            1,
            1000,
        );
        // Send to 4 actors
        for i in 0..4 {
            engine.apply_command(
                MqCommand::SendToActor {
                    namespace_id: 1,
                    actor_id: Bytes::from(format!("actor{}", i)),
                    message: make_msg(),
                },
                10 + i,
                1000,
            );
        }
        // Register 2 consumers
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "workers".to_string(),
                subscriptions: vec![Subscription {
                    entity_type: EntityType::ActorNamespace,
                    entity_id: 1,
                }],
            },
            20,
            1000,
        );
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 200,
                group_name: "workers".to_string(),
                subscriptions: vec![Subscription {
                    entity_type: EntityType::ActorNamespace,
                    entity_id: 1,
                }],
            },
            21,
            1000,
        );

        let cmds = collect_actor_rebalance(&engine);
        // Should produce AssignActors commands for both consumers
        assert!(cmds.len() >= 1);
        let total_actors: usize = cmds
            .iter()
            .map(|c| match c {
                MqCommand::AssignActors { actor_ids, .. } => actor_ids.len(),
                _ => 0,
            })
            .sum();
        assert_eq!(total_actors, 4);
    }

    #[test]
    fn test_actor_rebalance_no_unassigned() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns1".to_string(),
                config: ActorConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::SendToActor {
                namespace_id: 1,
                actor_id: Bytes::from_static(b"actor1"),
                message: make_msg(),
            },
            2,
            1000,
        );
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "workers".to_string(),
                subscriptions: vec![Subscription {
                    entity_type: EntityType::ActorNamespace,
                    entity_id: 1,
                }],
            },
            3,
            1000,
        );
        // Assign the actor
        engine.apply_command(
            MqCommand::AssignActors {
                namespace_id: 1,
                consumer_id: 100,
                actor_ids: vec![Bytes::from_static(b"actor1")],
            },
            4,
            1000,
        );

        let cmds = collect_actor_rebalance(&engine);
        assert!(cmds.is_empty());
    }
}
