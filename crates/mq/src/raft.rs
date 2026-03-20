//! MQ Raft node wrapper with leader-driven background tasks.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use openraft::Raft;
use openraft::async_runtime::WatchReceiver;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use bytes::BytesMut;

use crate::MqTypeConfig;
use crate::config::MqConfig;
use crate::metadata::MqMetadata;
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
    node_id: u32,
    group_id: u64,
    config: MqConfig,
    metadata: Option<Arc<MqMetadata>>,
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
    task_handles: parking_lot::Mutex<Vec<JoinHandle<()>>>,
    execution_counter: Arc<AtomicU64>,
}

impl MqRaftNode {
    pub fn new(raft: Raft<MqTypeConfig>, node_id: u32, config: MqConfig) -> Self {
        Self {
            raft,
            node_id,
            group_id: 0,
            config,
            metadata: None,
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

    pub fn with_metadata(mut self, metadata: Arc<MqMetadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn raft(&self) -> &Raft<MqTypeConfig> {
        &self.raft
    }

    /// Start leader-driven background tasks.
    pub fn start(&self) {
        let metadata = match &self.metadata {
            Some(m) => Arc::clone(m),
            None => {
                warn!("MqRaftNode::start() called without metadata — leader tasks disabled");
                return;
            }
        };

        let mut handles = self.task_handles.lock();

        // 1. Visibility timeout scanner
        {
            let meta = Arc::clone(&metadata);
            handles.push(self.spawn_leader_task(
                "mq-visibility-scan",
                self.config.visibility_scan_interval,
                move |raft, _node_id| {
                    let meta = Arc::clone(&meta);
                    Box::pin(async move {
                        Self::scan_visibility_timeouts(&raft, &meta).await;
                    })
                },
            ));
        }

        // 2. Cron job evaluator
        {
            let meta = Arc::clone(&metadata);
            let counter = Arc::clone(&self.execution_counter);
            handles.push(self.spawn_leader_task(
                "mq-cron-eval",
                self.config.cron_eval_interval,
                move |raft, _node_id| {
                    let meta = Arc::clone(&meta);
                    let counter = Arc::clone(&counter);
                    Box::pin(async move {
                        Self::evaluate_cron_triggers(&raft, &meta, &counter).await;
                    })
                },
            ));
        }

        // 3. Session heartbeat monitor
        {
            let meta = Arc::clone(&metadata);
            let timeout = self.config.session_timeout;
            handles.push(self.spawn_leader_task(
                "mq-heartbeat-monitor",
                self.config.session_expiry_interval,
                move |raft, _node_id| {
                    let meta = Arc::clone(&meta);
                    Box::pin(async move {
                        Self::check_session_heartbeats(&raft, &meta, timeout).await;
                    })
                },
            ));
        }

        // 5. Dedup window pruning (local — no Raft command needed, deterministic)
        {
            let meta = Arc::clone(&metadata);
            let interval = self.config.dedup_prune_interval;
            let shutdown = self.shutdown.clone();
            handles.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = tick.tick() => prune_dedup_local(&meta),
                        _ = shutdown.notified() => break,
                    }
                }
                debug!("mq-dedup-prune local task exiting");
            }));
        }

        // 6. Actor idle eviction
        {
            let meta = Arc::clone(&metadata);
            handles.push(self.spawn_leader_task(
                "mq-actor-eviction",
                self.config.actor_eviction_interval,
                move |raft, _node_id| {
                    let meta = Arc::clone(&meta);
                    Box::pin(async move {
                        Self::evict_idle_actors(&raft, &meta).await;
                    })
                },
            ));
        }

        // 7. Actor rebalancing
        {
            let meta = Arc::clone(&metadata);
            handles.push(self.spawn_leader_task(
                "mq-actor-rebalance",
                self.config.actor_rebalance_interval,
                move |raft, _node_id| {
                    let meta = Arc::clone(&meta);
                    Box::pin(async move {
                        Self::rebalance_actors(&raft, &meta).await;
                    })
                },
            ));
        }

        // 8. Consumer group session expiry
        {
            handles.push(self.spawn_leader_task(
                "mq-group-session-expiry",
                self.config.group_session_expiry_interval,
                move |raft, _node_id| {
                    Box::pin(async move {
                        let now = unix_ms();
                        let mut scratch = BytesMut::new();
                        MqCommand::write_expire_group_sessions(&mut scratch, now);
                        let _ = raft.client_write(MqCommand::split_from(&mut scratch)).await;
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
                u32,
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
    pub async fn write(&self, cmd: MqCommand) -> Result<crate::types::MqApplyResponse, String> {
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

    async fn scan_visibility_timeouts(raft: &Raft<MqTypeConfig>, meta: &Arc<MqMetadata>) {
        let now = unix_ms();
        let mut commands = collect_visibility_timeouts(meta, now);
        commands.extend(collect_expired_pending_messages(meta, now));
        if !commands.is_empty() {
            debug!(
                count = commands.len(),
                "proposing visibility timeout / expiry commands"
            );
            let mut scratch = BytesMut::new();
            MqCommand::write_batch(&mut scratch, &commands);
            let _ = raft.client_write(MqCommand::split_from(&mut scratch)).await;
        }
    }

    async fn evaluate_cron_triggers(
        raft: &Raft<MqTypeConfig>,
        meta: &Arc<MqMetadata>,
        counter: &AtomicU64,
    ) {
        let commands = collect_due_cron_triggers(meta, unix_ms(), counter);
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing cron trigger commands");
            let mut scratch = BytesMut::new();
            MqCommand::write_batch(&mut scratch, &commands);
            let _ = raft.client_write(MqCommand::split_from(&mut scratch)).await;
        }
    }

    async fn check_session_heartbeats(
        raft: &Raft<MqTypeConfig>,
        meta: &Arc<MqMetadata>,
        timeout: Duration,
    ) {
        let commands = collect_dead_sessions(meta, unix_ms(), timeout.as_millis() as u64);
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing dead session commands");
            let mut scratch = BytesMut::new();
            MqCommand::write_batch(&mut scratch, &commands);
            let _ = raft.client_write(MqCommand::split_from(&mut scratch)).await;
        }
    }

    // prune_dedup_windows removed — dedup GC is now local (see prune_dedup_local)

    async fn evict_idle_actors(raft: &Raft<MqTypeConfig>, meta: &Arc<MqMetadata>) {
        let commands = collect_idle_actor_evictions(meta, unix_ms());
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing actor eviction commands");
            let mut scratch = BytesMut::new();
            MqCommand::write_batch(&mut scratch, &commands);
            let _ = raft.client_write(MqCommand::split_from(&mut scratch)).await;
        }
    }

    async fn rebalance_actors(raft: &Raft<MqTypeConfig>, meta: &Arc<MqMetadata>) {
        let commands = collect_actor_rebalance(meta);
        if !commands.is_empty() {
            debug!(count = commands.len(), "proposing actor rebalance commands");
            let mut scratch = BytesMut::new();
            MqCommand::write_batch(&mut scratch, &commands);
            let _ = raft.client_write(MqCommand::split_from(&mut scratch)).await;
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

/// Collect GroupTimeoutExpired commands for all consumer groups (Ack variant) with
/// expired in-flight messages (visibility timeout exceeded).
pub(crate) fn collect_visibility_timeouts(meta: &MqMetadata, now_ms: u64) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    let mut scratch = BytesMut::new();
    let guard = meta.consumer_groups.pin();
    for (&group_id, group) in guard.iter() {
        if let Some(ack) = group.ack_state() {
            let deadlines = ack.in_flight_deadlines.lock();
            let expired: Vec<u64> = deadlines
                .range(..=now_ms)
                .flat_map(|(_deadline, ids)| ids.iter().copied())
                .collect();
            drop(deadlines);
            if !expired.is_empty() {
                MqCommand::write_group_timeout_expired(&mut scratch, group_id, &expired);
                commands.push(MqCommand::split_from(&mut scratch));
            }
        }
    }
    commands
}

/// Collect GroupExpirePending commands for consumer groups (Ack variant) with
/// pending messages past their TTL.
pub(crate) fn collect_expired_pending_messages(meta: &MqMetadata, now_ms: u64) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    let mut scratch = BytesMut::new();
    let guard = meta.consumer_groups.pin();
    for (&group_id, group) in guard.iter() {
        if let Some(ack) = group.ack_state() {
            let deadlines = ack.expires_at_deadlines.lock();
            let expired: Vec<u64> = deadlines
                .range(..=now_ms)
                .flat_map(|(_deadline, ids)| ids.iter().copied())
                .collect();
            drop(deadlines);
            if !expired.is_empty() {
                MqCommand::write_group_expire_pending(&mut scratch, group_id, &expired);
                commands.push(MqCommand::split_from(&mut scratch));
            }
        }
    }
    commands
}

/// Collect CronTrigger commands for all topics with a due cron schedule.
pub(crate) fn collect_due_cron_triggers(
    meta: &MqMetadata,
    now_ms: u64,
    _counter: &AtomicU64,
) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    let mut scratch = BytesMut::new();
    let guard = meta.topics.pin();
    for (&topic_id, topic) in guard.iter() {
        if topic.should_cron_trigger(now_ms) {
            MqCommand::write_cron_trigger(&mut scratch, topic_id, now_ms);
            commands.push(MqCommand::split_from(&mut scratch));
        }
    }
    commands
}

/// Collect commands for dead sessions: timeout their in-flight messages in
/// consumer groups, release actors, then disconnect the session.
pub(crate) fn collect_dead_sessions(
    meta: &MqMetadata,
    now_ms: u64,
    timeout_ms: u64,
) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    let mut scratch = BytesMut::new();
    let sessions_guard = meta.sessions.pin();
    for (&session_id, session) in sessions_guard.iter() {
        if !session.is_expired(now_ms) {
            continue;
        }
        // Also check against explicit timeout
        let last_activity = session.last_activity_at();
        if last_activity > 0 && now_ms.saturating_sub(last_activity) < timeout_ms {
            continue;
        }

        // Timeout in-flight messages in consumer groups for this session
        let cg_guard = meta.consumer_groups.pin();
        for (&group_id, group) in cg_guard.iter() {
            if let Some(ack) = group.ack_state() {
                let in_flight = ack.consumer_in_flight_ids(session_id);
                if !in_flight.is_empty() {
                    MqCommand::write_group_timeout_expired(
                        &mut scratch,
                        group_id,
                        &in_flight.into_vec(),
                    );
                    commands.push(MqCommand::split_from(&mut scratch));
                }
            }
            // Release actors assigned to this session
            if let Some(actor) = group.actor_state() {
                if actor.consumer_assignments.pin().contains_key(&session_id) {
                    MqCommand::write_group_release_actors(&mut scratch, group_id, session_id);
                    commands.push(MqCommand::split_from(&mut scratch));
                }
            }
        }

        // Disconnect the session
        MqCommand::write_disconnect_session(&mut scratch, session_id, true);
        commands.push(MqCommand::split_from(&mut scratch));
    }
    commands
}

/// Prune dedup windows locally on every node using the global `DedupIndex`.
///
/// Computes the minimum cutoff across all dedup-enabled topics, then does a
/// single range scan on the global `scc::TreeIndex` to collect expired entries.
/// Each expired key is dispatched to its topic's forward map for removal.
///
/// Also does a per-topic forward-map sweep for any entries not in the global
/// index (e.g. entries inserted via the non-indexed path or before the index
/// was enabled).
pub fn prune_dedup_local(meta: &MqMetadata) {
    let now_ms = unix_ms();

    // Find the minimum cutoff across all dedup-enabled topics.
    let mut min_before_ms = u64::MAX;
    let guard = meta.topics.pin();
    for (_, topic) in guard.iter() {
        if let Some(ref dedup_config) = topic.meta.dedup_config {
            if topic.dedup.is_some() {
                let window_ms = dedup_config.window_secs * 1000;
                let before_ms = now_ms.saturating_sub(window_ms);
                min_before_ms = min_before_ms.min(before_ms);
            }
        }
    }

    if min_before_ms == u64::MAX {
        return; // No dedup-enabled topics.
    }

    // Single range scan on the global reverse index.
    let expired = meta.dedup_index.prune_before(min_before_ms);

    if !expired.is_empty() {
        // Group expired keys by topic_id for batch removal from forward maps.
        let mut current_topic_id = expired[0].0;
        let mut keys_buf: Vec<u128> = Vec::new();

        for &(tid, dk) in &expired {
            if tid != current_topic_id {
                if let Some(topic) = guard.get(&current_topic_id) {
                    topic.remove_dedup_keys(&keys_buf);
                }
                keys_buf.clear();
                current_topic_id = tid;
            }
            keys_buf.push(dk);
        }
        if !keys_buf.is_empty() {
            if let Some(topic) = guard.get(&current_topic_id) {
                topic.remove_dedup_keys(&keys_buf);
            }
        }
    }

    // Per-topic forward-map sweep: catches entries not in the global index
    // and handles per-topic windows that differ from the minimum cutoff.
    for (_, topic) in guard.iter() {
        if let Some(ref dedup_config) = topic.meta.dedup_config {
            if topic.dedup.is_some() {
                let window_ms = dedup_config.window_secs * 1000;
                let topic_before_ms = now_ms.saturating_sub(window_ms);
                topic.prune_dedup(topic_before_ms);
            }
        }
    }
}

/// Collect GroupEvictIdle commands for all consumer groups (Actor variant).
pub(crate) fn collect_idle_actor_evictions(meta: &MqMetadata, now_ms: u64) -> Vec<MqCommand> {
    use crate::types::VariantConfig;
    let mut commands = Vec::new();
    let mut scratch = BytesMut::new();
    let guard = meta.consumer_groups.pin();
    for (&group_id, group) in guard.iter() {
        if group.actor_state().is_some() {
            if let VariantConfig::Actor(ref actor_config) = group.meta.variant_config {
                let eviction_secs = actor_config.idle_eviction_secs;
                if eviction_secs > 0 {
                    let before_timestamp = now_ms.saturating_sub(eviction_secs * 1000);
                    MqCommand::write_group_evict_idle(&mut scratch, group_id, before_timestamp);
                    commands.push(MqCommand::split_from(&mut scratch));
                }
            }
        }
    }
    commands
}

/// Collect GroupAssignActors commands to distribute unassigned actors among
/// sessions that are members of actor-variant consumer groups.
pub(crate) fn collect_actor_rebalance(meta: &MqMetadata) -> Vec<MqCommand> {
    let mut commands = Vec::new();
    let mut scratch = BytesMut::new();
    let cg_guard = meta.consumer_groups.pin();
    for (&group_id, group) in cg_guard.iter() {
        if let Some(actor) = group.actor_state() {
            let unassigned = actor.unassigned_actors_with_messages();
            if unassigned.is_empty() {
                continue;
            }

            // Find sessions that are associated with this consumer group
            let sgi_guard = meta.session_group_index.pin();
            let member_session_ids: Vec<u64> = sgi_guard
                .iter()
                .filter(|(_, groups)| groups.contains(&group_id))
                .map(|(&session_id, _)| session_id)
                .collect();
            if member_session_ids.is_empty() {
                continue;
            }

            // Round-robin distribute unassigned actors among members
            let actor_ids = unassigned;
            let num_members = member_session_ids.len();

            // Group by target session
            let mut per_session: std::collections::HashMap<u64, Vec<bytes::Bytes>> =
                std::collections::HashMap::new();
            for (i, actor_id) in actor_ids.into_iter().enumerate() {
                let session_id = member_session_ids[i % num_members];
                per_session.entry(session_id).or_default().push(actor_id);
            }

            for (session_id, actor_ids) in per_session {
                let ids: Vec<&[u8]> = actor_ids.iter().map(|b| b.as_ref()).collect();
                MqCommand::write_group_assign_actors(&mut scratch, group_id, session_id, &ids);
                commands.push(MqCommand::split_from(&mut scratch));
            }
        }
    }
    commands
}

// TODO: adapt tests to unified model (Topic + ConsumerGroup + Session)
// The old tests referenced Queue, Job, ActorNamespace, Consumer entities
// which have been removed. Tests should be rewritten to use:
// - MqCommand::create_topic / publish for topic operations
// - MqCommand::create_consumer_group for Ack/Actor variant groups
// - MqCommand::create_session / disconnect_session for session lifecycle
// - collect_visibility_timeouts now scans consumer_groups (Ack variant)
// - collect_due_cron_triggers now scans topics with cron config
// - collect_dead_sessions now scans sessions
// - collect_idle_actor_evictions / collect_actor_rebalance now scan consumer_groups (Actor variant)
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MqConfig;
    use crate::engine::MqEngine;

    fn make_engine() -> MqEngine {
        MqEngine::new(MqConfig::new("/tmp/test-mq"))
    }

    #[test]
    fn test_visibility_scan_empty() {
        let engine = make_engine();
        let cmds = collect_visibility_timeouts(engine.metadata(), 2000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_expired_pending_empty() {
        let engine = make_engine();
        let cmds = collect_expired_pending_messages(engine.metadata(), 2000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_cron_trigger_empty() {
        let engine = make_engine();
        let counter = AtomicU64::new(1);
        let cmds = collect_due_cron_triggers(engine.metadata(), 1000, &counter);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_dead_sessions_empty() {
        let engine = make_engine();
        let cmds = collect_dead_sessions(engine.metadata(), 32000, 30_000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_dedup_prune_local_empty() {
        let engine = make_engine();
        // Should not panic with no topics having dedup enabled.
        prune_dedup_local(engine.metadata());
    }

    #[test]
    fn test_actor_eviction_empty() {
        let engine = make_engine();
        let cmds = collect_idle_actor_evictions(engine.metadata(), 5_000_000);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_actor_rebalance_empty() {
        let engine = make_engine();
        let cmds = collect_actor_rebalance(engine.metadata());
        assert!(cmds.is_empty());
    }
}
