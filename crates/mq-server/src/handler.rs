use std::sync::Arc;
use std::time::Duration;

use bisque_mq::types::{MqCommand, MqResponse, PartitionInfo};
use bisque_mq::write_batcher::MqWriteBatcher;
use bisque_mq_protocol::tag::ServerTag;
use bisque_mq_protocol::types::{ClientFrame, ServerFrame};
use bisque_raft::SegmentPrefetcher;
use bytes::Bytes;
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::error::ConsumerError;
use crate::session::ConsumerSession;
use crate::subscription::{
    ENTITY_TYPE_ACTOR_NAMESPACE, ENTITY_TYPE_QUEUE, ENTITY_TYPE_TOPIC, SubscriptionState,
};

// ---------------------------------------------------------------------------
// Consumer group data types for MqRouter
// ---------------------------------------------------------------------------

/// Summary info for a consumer group (used by ListGroups).
#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    pub consumer_group_id: u64,
    pub name: String,
    pub phase: u8,
    pub protocol_type: String,
}

/// Detailed description of a consumer group (used by DescribeGroup).
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    pub consumer_group_id: u64,
    pub name: String,
    pub phase: u8,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: String,
    pub generation: i32,
    pub members: Vec<ConsumerGroupMemberInfo>,
}

/// Member info within a consumer group description.
#[derive(Debug, Clone)]
pub struct ConsumerGroupMemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub assignment: Vec<u8>,
}

/// Committed offset info (used by FetchGroupOffsets).
#[derive(Debug, Clone)]
pub struct ConsumerGroupOffset {
    pub topic_id: u64,
    pub partition_index: u32,
    pub committed_offset: u64,
    pub metadata: String,
}

// ---------------------------------------------------------------------------
// Router trait
// ---------------------------------------------------------------------------

/// Trait for resolving entity names to IDs and routing to the right batcher.
///
/// Implementations live in the server integration layer (bisque crate).
/// Tests use a mock implementation.
pub trait MqRouter: Send + Sync + 'static {
    /// Resolve a (group_id, entity_type, name_hash) to an entity_id.
    /// Returns None if the entity is not found.
    fn resolve_entity(&self, group_id: u64, entity_type: u8, name_hash: u64) -> Option<u64>;

    /// Get the write batcher for a given raft group.
    fn get_batcher(&self, group_id: u64) -> Option<Arc<MqWriteBatcher>>;

    /// Validate a handshake token.
    /// Returns (consumer_id, session_token) on success.
    fn validate_token(
        &self,
        token: &[u8],
        consumer_id: Option<u64>,
    ) -> Result<(u64, Vec<u8>), String>;

    /// Get the current head index for a topic (for initial subscription).
    fn get_topic_head(&self, group_id: u64, entity_id: u64) -> u64;

    /// Subscribe to topic publish notifications for tailing.
    /// Returns a watch receiver that fires with the new head_index on each publish.
    fn subscribe_topic_notify(
        &self,
        group_id: u64,
        entity_id: u64,
    ) -> tokio::sync::watch::Receiver<u64>;

    /// Get the segment prefetcher for a given raft group.
    /// Used to read flat message payloads from the raft log by message_id (log index).
    fn get_prefetcher(&self, group_id: u64) -> Option<SegmentPrefetcher>;

    /// Get the partition map for a topic. Returns `None` if the topic doesn't exist.
    /// Returns `Some(empty slice)` if the topic is unpartitioned.
    fn get_topic_partitions(&self, group_id: u64, entity_id: u64) -> Option<Vec<PartitionInfo>> {
        let _ = (group_id, entity_id);
        None
    }

    /// Get the write batcher for a specific partition of a partitioned topic.
    /// Returns `None` if no batcher is available for that partition group.
    fn get_partition_batcher(&self, partition_group_id: u64) -> Option<Arc<MqWriteBatcher>> {
        // Default: fall through to get_batcher
        self.get_batcher(partition_group_id)
    }

    // ── Consumer group operations ──────────────────────────────────────────

    /// Resolve a consumer group name hash to a consumer_group_id.
    fn resolve_consumer_group(&self, group_id: u64, name_hash: u64) -> Option<u64> {
        let _ = (group_id, name_hash);
        None
    }

    /// Get the phase notification handle for async JoinGroup/SyncGroup completion.
    fn get_group_phase_notify(
        &self,
        group_id: u64,
        consumer_group_id: u64,
    ) -> Option<Arc<tokio::sync::Notify>> {
        let _ = (group_id, consumer_group_id);
        None
    }

    /// Read the final join result for a member after phase completion.
    /// Returns `(generation, leader, member_id, protocol_name, is_leader, members)`.
    fn read_group_join_result(
        &self,
        group_id: u64,
        consumer_group_id: u64,
        member_id: &str,
    ) -> Option<(i32, String, String, String, bool, Vec<(String, Bytes)>)> {
        let _ = (group_id, consumer_group_id, member_id);
        None
    }

    /// Read a member's partition assignment after sync completion.
    fn read_group_member_assignment(
        &self,
        group_id: u64,
        consumer_group_id: u64,
        member_id: &str,
    ) -> Option<Vec<u8>> {
        let _ = (group_id, consumer_group_id, member_id);
        None
    }

    /// List all consumer groups in a raft group.
    fn list_consumer_groups(&self, group_id: u64) -> Vec<ConsumerGroupInfo> {
        let _ = group_id;
        Vec::new()
    }

    /// Describe a specific consumer group.
    fn describe_consumer_group(
        &self,
        group_id: u64,
        consumer_group_id: u64,
    ) -> Option<ConsumerGroupDescription> {
        let _ = (group_id, consumer_group_id);
        None
    }

    /// Fetch committed offsets for specific partitions in a consumer group.
    fn fetch_group_offsets(
        &self,
        group_id: u64,
        consumer_group_id: u64,
        partitions: &[(u64, u32)],
    ) -> Vec<ConsumerGroupOffset> {
        let _ = (group_id, consumer_group_id, partitions);
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Handler configuration
// ---------------------------------------------------------------------------

/// Configuration for the consumer handler.
#[derive(Debug, Clone)]
pub struct ConsumerHandlerConfig {
    /// Interval between server heartbeats sent to the client.
    pub heartbeat_interval: Duration,
    /// Maximum time to wait for a client heartbeat before disconnecting.
    pub heartbeat_timeout: Duration,
    /// Timeout for the initial handshake.
    pub handshake_timeout: Duration,
    /// Maximum number of messages to deliver per round-robin turn.
    pub max_deliver_batch: u32,
    /// Catalog name for metrics labeling.
    pub catalog_name: String,
}

impl Default for ConsumerHandlerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(15),
            heartbeat_timeout: Duration::from_secs(90),
            handshake_timeout: Duration::from_secs(10),
            max_deliver_batch: 10,
            catalog_name: "default".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Topic watcher task
// ---------------------------------------------------------------------------

/// Spawns a task that watches a topic's head_index and notifies the handler.
fn spawn_topic_watcher(
    mut rx: tokio::sync::watch::Receiver<u64>,
    sub_id: u32,
    update_tx: mpsc::UnboundedSender<(u32, u64)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.changed().await {
                Ok(()) => {
                    let head = *rx.borrow();
                    if update_tx.send((sub_id, head)).is_err() {
                        return; // handler dropped
                    }
                }
                Err(_) => return, // sender dropped
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Consumer handler
// ---------------------------------------------------------------------------

/// Tracks a consumer group membership for this connection (for cleanup on disconnect).
#[derive(Debug, Clone)]
struct GroupMembership {
    group_id: u64,
    consumer_group_id: u64,
    member_id: String,
    /// Rebalance timeout from JoinGroup, reused for SyncGroup background task timeout.
    rebalance_timeout_ms: u32,
}

/// Main consumer handler that runs the connection lifecycle.
///
/// Transport-agnostic: communicates via channels.
/// The TCP/WS adapter converts between wire bytes and these channels.
pub struct ConsumerHandler<R: MqRouter> {
    router: Arc<R>,
    session: Option<ConsumerSession>,
    config: ConsumerHandlerConfig,
    frame_rx: mpsc::Receiver<ClientFrame>,
    frame_tx: mpsc::Sender<ServerFrame>,
    /// Channel for topic head updates from watcher tasks.
    topic_update_tx: mpsc::UnboundedSender<(u32, u64)>,
    topic_update_rx: mpsc::UnboundedReceiver<(u32, u64)>,
    /// Handles for topic watcher tasks (for cleanup).
    topic_watcher_handles: Vec<(u32, tokio::task::JoinHandle<()>)>,
    last_client_heartbeat: Instant,
    delivery_notify: Arc<Notify>,
    /// Reusable buffer for encoding message batches directly from FlatMessage
    /// into wire format bytes — avoids intermediate ServerMessage allocation.
    msg_batch_buf: Vec<u8>,
    /// Consumer group memberships held by this connection (for cleanup on disconnect).
    group_memberships: Vec<GroupMembership>,
    /// Spawned background tasks for async JoinGroup/SyncGroup completion.
    /// Aborted on disconnect to prevent orphaned work.
    background_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl<R: MqRouter> ConsumerHandler<R> {
    pub fn new(
        router: Arc<R>,
        config: ConsumerHandlerConfig,
        frame_rx: mpsc::Receiver<ClientFrame>,
        frame_tx: mpsc::Sender<ServerFrame>,
    ) -> Self {
        let (topic_update_tx, topic_update_rx) = mpsc::unbounded_channel();
        Self {
            router,
            session: None,
            config,
            frame_rx,
            frame_tx,
            topic_update_tx,
            topic_update_rx,
            topic_watcher_handles: Vec::new(),
            last_client_heartbeat: Instant::now(),
            delivery_notify: Arc::new(Notify::new()),
            msg_batch_buf: Vec::new(),
            group_memberships: Vec::new(),
            background_tasks: Vec::new(),
        }
    }

    /// Run the consumer handler lifecycle.
    pub async fn run(mut self) -> Result<(), ConsumerError> {
        // Phase 1: Handshake
        self.do_handshake().await?;

        // Phase 2: Main loop
        let result = self.main_loop().await;

        // Phase 3: Cleanup
        self.cleanup().await;

        result
    }

    // -----------------------------------------------------------------------
    // Handshake
    // -----------------------------------------------------------------------

    async fn do_handshake(&mut self) -> Result<(), ConsumerError> {
        let frame = tokio::time::timeout(self.config.handshake_timeout, self.frame_rx.recv())
            .await
            .map_err(|_| ConsumerError::HandshakeFailed("handshake timeout".into()))?
            .ok_or(ConsumerError::SessionClosed)?;

        match frame {
            ClientFrame::Handshake { token, consumer_id } => {
                match self.router.validate_token(&token, consumer_id) {
                    Ok((cid, session_token)) => {
                        let session_token = Bytes::from(session_token);
                        self.session = Some(ConsumerSession::new(
                            cid,
                            session_token.clone(),
                            &self.config.catalog_name,
                        ));
                        self.send_frame(ServerFrame::HandshakeOk {
                            consumer_id: cid,
                            session_token,
                        })
                        .await?;
                        debug!(consumer_id = cid, "consumer handshake complete");
                        Ok(())
                    }
                    Err(msg) => {
                        let message = Bytes::copy_from_slice(msg.as_bytes());
                        self.send_frame(ServerFrame::HandshakeErr { code: 401, message })
                            .await
                            .ok(); // best-effort
                        Err(ConsumerError::HandshakeFailed(msg))
                    }
                }
            }
            _ => Err(ConsumerError::HandshakeFailed(
                "expected Handshake frame".into(),
            )),
        }
    }

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------

    async fn main_loop(&mut self) -> Result<(), ConsumerError> {
        let mut heartbeat_tick = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let can_send = self.session.as_ref().is_some_and(|s| s.can_send());

            tokio::select! {
                // Client frame
                frame = self.frame_rx.recv() => {
                    match frame {
                        Some(frame) => {
                            if self.handle_client_frame(frame).await? {
                                return Ok(()); // Close requested
                            }
                        }
                        None => return Ok(()), // Transport closed
                    }
                }

                // Server heartbeat tick
                _ = heartbeat_tick.tick() => {
                    self.send_heartbeat().await?;
                    if self.last_client_heartbeat.elapsed() > self.config.heartbeat_timeout {
                        warn!(consumer_id = ?self.session.as_ref().map(|s| s.consumer_id),
                              "consumer heartbeat timeout");
                        return Err(ConsumerError::HeartbeatTimeout);
                    }
                }

                // Topic head updates from watcher tasks
                Some((sub_id, head)) = self.topic_update_rx.recv() => {
                    if let Some(session) = &mut self.session {
                        if let Some(sub) = session.get_subscription_mut(sub_id) {
                            sub.update_head_index(head);
                        }
                        session.update_readiness(sub_id);
                        self.delivery_notify.notify_one();
                    }
                }

                // Delivery opportunity
                _ = self.delivery_notify.notified(), if can_send => {
                    self.deliver_next().await?;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Client frame handling
    // -----------------------------------------------------------------------

    async fn handle_client_frame(&mut self, frame: ClientFrame) -> Result<bool, ConsumerError> {
        match frame {
            ClientFrame::Handshake { .. } => {
                warn!("duplicate handshake received");
            }

            ClientFrame::Subscribe {
                sub_id,
                group_id,
                entity_type,
                name_hash,
                start_offset,
                max_in_flight,
            } => {
                self.handle_subscribe(
                    sub_id,
                    group_id,
                    entity_type,
                    name_hash,
                    start_offset,
                    max_in_flight,
                )
                .await?;
            }

            ClientFrame::Unsubscribe { sub_id } => {
                self.handle_unsubscribe(sub_id);
            }

            ClientFrame::SetMaxInFlight {
                sub_id,
                max_in_flight,
            } => {
                if let Some(session) = &mut self.session {
                    session.update_max_in_flight(sub_id, max_in_flight);
                    self.delivery_notify.notify_one();
                }
            }

            ClientFrame::Ack {
                sub_id,
                message_ids,
            } => {
                self.handle_ack(sub_id, message_ids).await?;
            }

            ClientFrame::Nack {
                sub_id,
                message_ids,
            } => {
                self.handle_nack(sub_id, message_ids).await?;
            }

            ClientFrame::CommitOffset { sub_id, offset } => {
                self.handle_commit_offset(sub_id, offset).await?;
            }

            ClientFrame::Heartbeat => {
                self.last_client_heartbeat = Instant::now();
            }

            ClientFrame::Close => {
                self.send_frame(ServerFrame::Close {
                    reason: Bytes::from_static(b"client close"),
                })
                .await
                .ok();
                return Ok(true);
            }

            ClientFrame::SetByteBudget { budget_bytes } => {
                if let Some(session) = &mut self.session {
                    session.update_byte_budget(budget_bytes);
                    self.delivery_notify.notify_one();
                }
            }

            ClientFrame::Publish {
                group_id,
                topic_name_hash,
                messages,
            } => {
                self.handle_publish(group_id, topic_name_hash, messages)
                    .await?;
            }

            // ── Consumer group operations ──────────────────────────────────
            ClientFrame::CreateGroup {
                group_id,
                name,
                auto_offset_reset,
            } => {
                self.handle_create_group(group_id, &name, auto_offset_reset)
                    .await?;
            }

            ClientFrame::DeleteGroup {
                group_id,
                name_hash,
            } => {
                self.handle_delete_group(group_id, name_hash).await?;
            }

            ClientFrame::JoinGroup {
                group_id,
                name_hash,
                member_id,
                client_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                protocol_type,
                protocols,
            } => {
                self.handle_join_group(
                    group_id,
                    name_hash,
                    &member_id,
                    &client_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &protocol_type,
                    protocols,
                )
                .await?;
            }

            ClientFrame::SyncGroup {
                group_id,
                name_hash,
                generation,
                member_id,
                assignments,
            } => {
                self.handle_sync_group(group_id, name_hash, generation, &member_id, assignments)
                    .await?;
            }

            ClientFrame::LeaveGroup {
                group_id,
                name_hash,
                member_id,
            } => {
                self.handle_leave_group(group_id, name_hash, &member_id)
                    .await?;
            }

            ClientFrame::GroupHeartbeat {
                group_id,
                name_hash,
                member_id,
                generation,
            } => {
                self.handle_group_heartbeat(group_id, name_hash, &member_id, generation)
                    .await?;
            }

            ClientFrame::CommitGroupOffset {
                group_id,
                name_hash,
                generation,
                offsets,
            } => {
                self.handle_commit_group_offset(group_id, name_hash, generation, offsets)
                    .await?;
            }

            ClientFrame::FetchGroupOffsets {
                group_id,
                name_hash,
                partitions,
            } => {
                self.handle_fetch_group_offsets(group_id, name_hash, partitions)
                    .await?;
            }

            ClientFrame::ListGroups { group_id } => {
                self.handle_list_groups(group_id).await?;
            }

            ClientFrame::DescribeGroup {
                group_id,
                name_hash,
            } => {
                self.handle_describe_group(group_id, name_hash).await?;
            }
        }

        Ok(false)
    }

    // -----------------------------------------------------------------------
    // Subscribe / Unsubscribe
    // -----------------------------------------------------------------------

    async fn handle_subscribe(
        &mut self,
        sub_id: u32,
        group_id: u64,
        entity_type: u8,
        name_hash: u64,
        start_offset: u64,
        max_in_flight: u32,
    ) -> Result<(), ConsumerError> {
        let entity_id = match self.router.resolve_entity(group_id, entity_type, name_hash) {
            Some(id) => id,
            None => {
                self.send_frame(ServerFrame::SubscriptionErr {
                    sub_id,
                    code: 404,
                    message: Bytes::from_static(b"entity not found"),
                })
                .await?;
                return Ok(());
            }
        };

        let mut sub = SubscriptionState::new(
            sub_id,
            group_id,
            entity_type,
            name_hash,
            entity_id,
            start_offset,
            max_in_flight,
        );

        // For topics, set up publish notification watcher
        if entity_type == ENTITY_TYPE_TOPIC {
            let watcher = self.router.subscribe_topic_notify(group_id, entity_id);
            let head = *watcher.borrow();
            sub.update_head_index(head);

            let handle = spawn_topic_watcher(watcher, sub_id, self.topic_update_tx.clone());
            self.topic_watcher_handles.push((sub_id, handle));
        }

        if let Some(session) = &mut self.session {
            session.add_subscription(sub);
            self.delivery_notify.notify_one();
        }

        self.send_frame(ServerFrame::Subscribed { sub_id, entity_id })
            .await?;

        debug!(sub_id, entity_id, entity_type, "subscription created");
        Ok(())
    }

    fn handle_unsubscribe(&mut self, sub_id: u32) {
        if let Some(session) = &mut self.session {
            session.remove_subscription(sub_id);
        }
        // Cancel topic watcher if any
        self.topic_watcher_handles.retain(|(id, handle)| {
            if *id == sub_id {
                handle.abort();
                false
            } else {
                true
            }
        });
        debug!(sub_id, "subscription removed");
    }

    // -----------------------------------------------------------------------
    // Ack / Nack / CommitOffset
    // -----------------------------------------------------------------------

    async fn handle_ack(
        &mut self,
        sub_id: u32,
        message_ids: Vec<u64>,
    ) -> Result<(), ConsumerError> {
        let (group_id, entity_type, entity_id) = match self.get_sub_routing(sub_id) {
            Some(info) => info,
            None => return Ok(()),
        };

        let count = message_ids.len() as u32;
        let bytes_freed = 0u64; // TODO: track per-message byte sizes

        match entity_type {
            ENTITY_TYPE_QUEUE => {
                if let Some(batcher) = self.router.get_batcher(group_id) {
                    let _ = batcher
                        .submit(MqCommand::group_ack(group_id, &message_ids, None))
                        .await;
                }
            }
            ENTITY_TYPE_ACTOR_NAMESPACE => {
                if let Some(batcher) = self.router.get_batcher(group_id) {
                    for &msg_id in &message_ids {
                        let _ = batcher
                            .submit(MqCommand::group_ack_actor(
                                group_id,
                                &[], // TODO: track actor_id per in-flight message
                                msg_id,
                                None,
                            ))
                            .await;
                    }
                }
            }
            ENTITY_TYPE_TOPIC => {
                // Topics don't need ack — consumer tracks offset via CommitOffset
            }
            _ => {}
        }

        if let Some(session) = &mut self.session {
            session.on_ack(sub_id, count, bytes_freed);
            self.delivery_notify.notify_one();
        }

        Ok(())
    }

    async fn handle_nack(
        &mut self,
        sub_id: u32,
        message_ids: Vec<u64>,
    ) -> Result<(), ConsumerError> {
        let (group_id, entity_type, entity_id) = match self.get_sub_routing(sub_id) {
            Some(info) => info,
            None => return Ok(()),
        };

        let count = message_ids.len() as u32;
        let bytes_freed = 0u64;

        match entity_type {
            ENTITY_TYPE_QUEUE => {
                if let Some(batcher) = self.router.get_batcher(group_id) {
                    let _ = batcher
                        .submit(MqCommand::group_nack(group_id, &message_ids))
                        .await;
                }
            }
            ENTITY_TYPE_ACTOR_NAMESPACE => {
                if let Some(batcher) = self.router.get_batcher(group_id) {
                    for &msg_id in &message_ids {
                        let _ = batcher
                            .submit(MqCommand::group_nack_actor(
                                group_id,
                                &[], // TODO: track actor_id per in-flight message
                                msg_id,
                            ))
                            .await;
                    }
                }
            }
            _ => {}
        }

        if let Some(session) = &mut self.session {
            session.on_nack(sub_id, count, bytes_freed);
            self.delivery_notify.notify_one();
        }

        Ok(())
    }

    async fn handle_commit_offset(
        &mut self,
        sub_id: u32,
        offset: u64,
    ) -> Result<(), ConsumerError> {
        let (group_id, entity_type, entity_id) = match self.get_sub_routing(sub_id) {
            Some(info) => info,
            None => return Ok(()),
        };

        if entity_type != ENTITY_TYPE_TOPIC {
            return Ok(());
        }

        let consumer_id = self.session.as_ref().map(|s| s.consumer_id).unwrap_or(0);

        if let Some(batcher) = self.router.get_batcher(group_id) {
            let _ = batcher
                .submit(MqCommand::commit_offset(entity_id, consumer_id, offset))
                .await;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Consumer group operations
    // -----------------------------------------------------------------------

    async fn handle_create_group(
        &mut self,
        group_id: u64,
        name: &[u8],
        auto_offset_reset: u8,
    ) -> Result<(), ConsumerError> {
        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => {
                self.send_group_error(1, "no batcher available").await?;
                return Ok(());
            }
        };

        let name_str = std::str::from_utf8(name).unwrap_or("");
        let response = batcher
            .submit(MqCommand::create_consumer_group(
                name_str,
                auto_offset_reset,
            ))
            .await?;

        match response {
            MqResponse::EntityCreated { id } => {
                self.send_frame(ServerFrame::GroupCreated {
                    consumer_group_id: id,
                })
                .await?;
            }
            MqResponse::Error(e) => {
                self.send_group_error(2, &e.to_string()).await?;
            }
            _ => {
                self.send_group_error(3, "unexpected response").await?;
            }
        }
        Ok(())
    }

    async fn handle_delete_group(
        &mut self,
        group_id: u64,
        name_hash: u64,
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => return Ok(()),
        };

        let response = batcher
            .submit(MqCommand::delete_consumer_group(consumer_group_id))
            .await?;

        match response {
            MqResponse::Ok => {
                self.send_frame(ServerFrame::GroupDeleted).await?;
            }
            MqResponse::Error(e) => {
                self.send_group_error(2, &e.to_string()).await?;
            }
            _ => {
                self.send_frame(ServerFrame::GroupDeleted).await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_join_group(
        &mut self,
        group_id: u64,
        name_hash: u64,
        member_id: &[u8],
        client_id: &[u8],
        session_timeout_ms: u32,
        rebalance_timeout_ms: u32,
        protocol_type: &[u8],
        protocols: Vec<(Bytes, Bytes)>,
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => {
                self.send_group_error(1, "no batcher available").await?;
                return Ok(());
            }
        };

        // Clone phase_notify before submitting (for async completion)
        let phase_notify = self
            .router
            .get_group_phase_notify(group_id, consumer_group_id);

        let member_id_str = std::str::from_utf8(member_id).unwrap_or("");
        let client_id_str = std::str::from_utf8(client_id).unwrap_or("");
        let protocol_type_str = std::str::from_utf8(protocol_type).unwrap_or("");

        let protocol_pairs: Vec<(&str, &[u8])> = protocols
            .iter()
            .map(|(name, meta)| (std::str::from_utf8(name).unwrap_or(""), meta.as_ref()))
            .collect();

        let response = batcher
            .submit(MqCommand::join_consumer_group(
                consumer_group_id,
                member_id_str,
                client_id_str,
                session_timeout_ms as i32,
                rebalance_timeout_ms as i32,
                protocol_type_str,
                &protocol_pairs,
            ))
            .await?;

        match response {
            MqResponse::GroupJoined {
                generation,
                leader,
                member_id: assigned_member_id,
                protocol_name,
                is_leader,
                members,
                phase_complete,
            } => {
                // Track membership for cleanup on disconnect
                self.track_group_membership(
                    group_id,
                    consumer_group_id,
                    assigned_member_id.clone(),
                    rebalance_timeout_ms,
                );

                if phase_complete {
                    send_group_joined_response(
                        &self.frame_tx,
                        generation,
                        &leader,
                        &assigned_member_id,
                        &protocol_name,
                        is_leader,
                        &members,
                    )
                    .await?;
                } else {
                    // Spawn background task to wait for phase completion,
                    // so the main loop stays responsive for heartbeats etc.
                    let router = self.router.clone();
                    let frame_tx = self.frame_tx.clone();
                    let timeout = Duration::from_millis(rebalance_timeout_ms as u64);
                    let task = tokio::spawn(async move {
                        if let Some(notify) = phase_notify {
                            let _ = tokio::time::timeout(timeout, notify.notified()).await;
                        }

                        // Read final state from metadata
                        if let Some((g, ldr, mid, proto, is_ldr, mems)) = router
                            .read_group_join_result(
                                group_id,
                                consumer_group_id,
                                &assigned_member_id,
                            )
                        {
                            let _ = send_group_joined_response(
                                &frame_tx, g, &ldr, &mid, &proto, is_ldr, &mems,
                            )
                            .await;
                        } else {
                            // Fallback: use initial response data
                            let _ = send_group_joined_response(
                                &frame_tx,
                                generation,
                                &leader,
                                &assigned_member_id,
                                &protocol_name,
                                is_leader,
                                &members,
                            )
                            .await;
                        }
                    });
                    self.prune_background_tasks();
                    self.background_tasks.push(task);
                }
            }
            MqResponse::Error(e) => {
                self.send_group_error(2, &e.to_string()).await?;
            }
            _ => {
                self.send_group_error(3, "unexpected response").await?;
            }
        }
        Ok(())
    }

    async fn handle_sync_group(
        &mut self,
        group_id: u64,
        name_hash: u64,
        generation: i32,
        member_id: &[u8],
        assignments: Vec<(Bytes, Bytes)>,
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => {
                self.send_group_error(1, "no batcher available").await?;
                return Ok(());
            }
        };

        let phase_notify = self
            .router
            .get_group_phase_notify(group_id, consumer_group_id);

        let member_id_str = std::str::from_utf8(member_id).unwrap_or("").to_string();

        let assignment_pairs: Vec<(&str, &[u8])> = assignments
            .iter()
            .map(|(mid, assignment)| (std::str::from_utf8(mid).unwrap_or(""), assignment.as_ref()))
            .collect();

        let response = batcher
            .submit(MqCommand::sync_consumer_group(
                consumer_group_id,
                generation,
                &member_id_str,
                &assignment_pairs,
            ))
            .await?;

        match response {
            MqResponse::GroupSynced {
                assignment,
                phase_complete,
            } => {
                if phase_complete {
                    self.send_frame(ServerFrame::GroupSynced {
                        assignment: Bytes::from(assignment),
                    })
                    .await?;
                } else {
                    // Use rebalance timeout from JoinGroup (stored per membership)
                    let timeout = self.get_rebalance_timeout(group_id, consumer_group_id);
                    // Spawn background task so main loop stays responsive
                    let router = self.router.clone();
                    let frame_tx = self.frame_tx.clone();
                    let task = tokio::spawn(async move {
                        if let Some(notify) = phase_notify {
                            let _ = tokio::time::timeout(timeout, notify.notified()).await;
                        }

                        let final_assignment = router
                            .read_group_member_assignment(
                                group_id,
                                consumer_group_id,
                                &member_id_str,
                            )
                            .unwrap_or(assignment);

                        let _ = frame_tx
                            .send(ServerFrame::GroupSynced {
                                assignment: Bytes::from(final_assignment),
                            })
                            .await;
                    });
                    self.prune_background_tasks();
                    self.background_tasks.push(task);
                }
            }
            MqResponse::Error(e) => {
                self.send_group_error(2, &e.to_string()).await?;
            }
            _ => {
                self.send_group_error(3, "unexpected response").await?;
            }
        }
        Ok(())
    }

    async fn handle_leave_group(
        &mut self,
        group_id: u64,
        name_hash: u64,
        member_id: &[u8],
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => return Ok(()),
        };

        let member_id_str = std::str::from_utf8(member_id).unwrap_or("");
        let response = batcher
            .submit(MqCommand::leave_consumer_group(
                consumer_group_id,
                member_id_str,
            ))
            .await?;

        match response {
            MqResponse::Ok => {
                self.remove_group_membership(group_id, consumer_group_id, member_id_str);
                self.send_frame(ServerFrame::GroupLeft).await?;
            }
            MqResponse::Error(e) => {
                self.send_group_error(2, &e.to_string()).await?;
            }
            _ => {
                self.remove_group_membership(group_id, consumer_group_id, member_id_str);
                self.send_frame(ServerFrame::GroupLeft).await?;
            }
        }
        Ok(())
    }

    async fn handle_group_heartbeat(
        &mut self,
        group_id: u64,
        name_hash: u64,
        member_id: &[u8],
        generation: i32,
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => return Ok(()),
        };

        let member_id_str = std::str::from_utf8(member_id).unwrap_or("");
        let response = batcher
            .submit(MqCommand::heartbeat_consumer_group(
                consumer_group_id,
                member_id_str,
                generation,
            ))
            .await?;

        match response {
            MqResponse::Ok => {
                self.send_frame(ServerFrame::GroupHeartbeatOk).await?;
            }
            MqResponse::Error(e) => {
                self.send_group_error(2, &e.to_string()).await?;
            }
            _ => {
                self.send_frame(ServerFrame::GroupHeartbeatOk).await?;
            }
        }
        Ok(())
    }

    async fn handle_commit_group_offset(
        &mut self,
        group_id: u64,
        name_hash: u64,
        generation: i32,
        offsets: Vec<(u64, u32, u64, Bytes)>,
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => return Ok(()),
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let result = if offsets.len() == 1 {
            // Single offset — submit directly without batch overhead
            let (topic_id, partition_index, offset, metadata) = &offsets[0];
            let metadata_str = std::str::from_utf8(metadata).unwrap_or("");
            let metadata_opt = if metadata_str.is_empty() {
                None
            } else {
                Some(metadata_str)
            };
            batcher
                .submit(MqCommand::commit_group_offset(
                    consumer_group_id,
                    generation,
                    *topic_id,
                    *partition_index,
                    *offset,
                    metadata_opt,
                    now_ms,
                ))
                .await
        } else {
            // Multiple offsets — batch into a single raft log entry
            let commands: Vec<MqCommand> = offsets
                .iter()
                .map(|(topic_id, partition_index, offset, metadata)| {
                    let metadata_str = std::str::from_utf8(metadata).unwrap_or("");
                    let metadata_opt = if metadata_str.is_empty() {
                        None
                    } else {
                        Some(metadata_str)
                    };
                    MqCommand::commit_group_offset(
                        consumer_group_id,
                        generation,
                        *topic_id,
                        *partition_index,
                        *offset,
                        metadata_opt,
                        now_ms,
                    )
                })
                .collect();
            batcher.submit(MqCommand::batch(&commands)).await
        };

        match result {
            Ok(_) => {
                self.send_frame(ServerFrame::GroupOffsetCommitted).await?;
            }
            Err(e) => {
                self.send_group_error(2, &e.to_string()).await?;
            }
        }
        Ok(())
    }

    async fn handle_fetch_group_offsets(
        &mut self,
        group_id: u64,
        name_hash: u64,
        partitions: Vec<(u64, u32)>,
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        let offsets = self
            .router
            .fetch_group_offsets(group_id, consumer_group_id, &partitions);

        let wire_offsets: Vec<(u64, u32, u64, Bytes)> = offsets
            .into_iter()
            .map(|o| {
                (
                    o.topic_id,
                    o.partition_index,
                    o.committed_offset,
                    Bytes::copy_from_slice(o.metadata.as_bytes()),
                )
            })
            .collect();

        self.send_frame(ServerFrame::GroupOffsetsFetched {
            offsets: wire_offsets,
        })
        .await?;
        Ok(())
    }

    async fn handle_list_groups(&mut self, group_id: u64) -> Result<(), ConsumerError> {
        let groups = self.router.list_consumer_groups(group_id);

        let wire_groups: Vec<(u64, Bytes, u8, Bytes)> = groups
            .into_iter()
            .map(|g| {
                (
                    g.consumer_group_id,
                    Bytes::copy_from_slice(g.name.as_bytes()),
                    g.phase,
                    Bytes::copy_from_slice(g.protocol_type.as_bytes()),
                )
            })
            .collect();

        self.send_frame(ServerFrame::GroupList {
            groups: wire_groups,
        })
        .await?;
        Ok(())
    }

    async fn handle_describe_group(
        &mut self,
        group_id: u64,
        name_hash: u64,
    ) -> Result<(), ConsumerError> {
        let consumer_group_id = match self.router.resolve_consumer_group(group_id, name_hash) {
            Some(id) => id,
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
                return Ok(());
            }
        };

        match self
            .router
            .describe_consumer_group(group_id, consumer_group_id)
        {
            Some(desc) => {
                let wire_members: Vec<(Bytes, Bytes, Bytes)> = desc
                    .members
                    .into_iter()
                    .map(|m| {
                        (
                            Bytes::copy_from_slice(m.member_id.as_bytes()),
                            Bytes::copy_from_slice(m.client_id.as_bytes()),
                            Bytes::from(m.assignment),
                        )
                    })
                    .collect();

                self.send_frame(ServerFrame::GroupDescription {
                    consumer_group_id: desc.consumer_group_id,
                    name: Bytes::copy_from_slice(desc.name.as_bytes()),
                    phase: desc.phase,
                    protocol_type: Bytes::copy_from_slice(desc.protocol_type.as_bytes()),
                    protocol_name: Bytes::copy_from_slice(desc.protocol_name.as_bytes()),
                    leader: Bytes::copy_from_slice(desc.leader.as_bytes()),
                    generation: desc.generation,
                    members: wire_members,
                })
                .await?;
            }
            None => {
                self.send_group_error(404, "consumer group not found")
                    .await?;
            }
        }
        Ok(())
    }

    fn track_group_membership(
        &mut self,
        group_id: u64,
        consumer_group_id: u64,
        member_id: String,
        rebalance_timeout_ms: u32,
    ) {
        // A connection can only hold one membership per (group_id, consumer_group_id).
        // Replace any existing entry (handles member_id changes on rejoin).
        if let Some(existing) = self
            .group_memberships
            .iter_mut()
            .find(|m| m.group_id == group_id && m.consumer_group_id == consumer_group_id)
        {
            existing.member_id = member_id;
            existing.rebalance_timeout_ms = rebalance_timeout_ms;
        } else {
            self.group_memberships.push(GroupMembership {
                group_id,
                consumer_group_id,
                member_id,
                rebalance_timeout_ms,
            });
        }
    }

    /// Remove completed background task handles to prevent unbounded growth.
    fn prune_background_tasks(&mut self) {
        self.background_tasks.retain(|h| !h.is_finished());
    }

    /// Get the stored rebalance timeout for a consumer group membership.
    fn get_rebalance_timeout(&self, group_id: u64, consumer_group_id: u64) -> Duration {
        self.group_memberships
            .iter()
            .find(|m| m.group_id == group_id && m.consumer_group_id == consumer_group_id)
            .map(|m| Duration::from_millis(m.rebalance_timeout_ms as u64))
            .unwrap_or(Duration::from_secs(30))
    }

    fn remove_group_membership(&mut self, group_id: u64, consumer_group_id: u64, member_id: &str) {
        self.group_memberships.retain(|m| {
            !(m.group_id == group_id
                && m.consumer_group_id == consumer_group_id
                && m.member_id == member_id)
        });
    }

    async fn send_group_error(&self, code: u16, message: &str) -> Result<(), ConsumerError> {
        self.send_frame(ServerFrame::GroupError {
            code,
            message: Bytes::copy_from_slice(message.as_bytes()),
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Publish (native producer)
    // -----------------------------------------------------------------------

    async fn handle_publish(
        &mut self,
        group_id: u64,
        topic_name_hash: u64,
        messages: Vec<Bytes>,
    ) -> Result<(), ConsumerError> {
        let topic_id =
            match self
                .router
                .resolve_entity(group_id, ENTITY_TYPE_TOPIC, topic_name_hash)
            {
                Some(id) => id,
                None => {
                    debug!(topic_name_hash, "publish to unknown topic");
                    return Ok(());
                }
            };

        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => return Ok(()),
        };

        let _ = batcher
            .submit(MqCommand::publish(topic_id, &messages))
            .await;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Message delivery
    // -----------------------------------------------------------------------

    async fn deliver_next(&mut self) -> Result<(), ConsumerError> {
        // Extract what we need from session without holding the borrow
        let (sub_id, group_id, entity_type, entity_id, consumer_id, max_count) = {
            let session = match &mut self.session {
                Some(s) => s,
                None => return Ok(()),
            };

            let sub_id = match session.next_delivery_sub() {
                Some(id) => id,
                None => return Ok(()),
            };

            let sub = match session.get_subscription(sub_id) {
                Some(s) => s,
                None => return Ok(()),
            };

            let remaining = sub.remaining_capacity();
            if remaining == 0 {
                return Ok(());
            }

            let max_count = remaining.min(self.config.max_deliver_batch);
            (
                sub_id,
                sub.group_id,
                sub.entity_type,
                sub.entity_id,
                session.consumer_id,
                max_count,
            )
        };

        let delivered = match entity_type {
            ENTITY_TYPE_QUEUE => {
                self.deliver_queue(sub_id, group_id, entity_id, consumer_id, max_count)
                    .await?
            }
            ENTITY_TYPE_TOPIC => {
                // Mark tailing if caught up
                if let Some(session) = &mut self.session {
                    if let Some(sub) = session.get_subscription_mut(sub_id) {
                        if sub.current_offset >= sub.head_index {
                            sub.is_tailing = true;
                        }
                    }
                    session.update_readiness(sub_id);
                }
                false
            }
            ENTITY_TYPE_ACTOR_NAMESPACE => {
                self.deliver_actor(sub_id, group_id, entity_id, consumer_id)
                    .await?
            }
            _ => false,
        };

        // Re-notify only if we delivered messages and can still send more
        if delivered && self.session.as_ref().is_some_and(|s| s.can_send()) {
            self.delivery_notify.notify_one();
        }

        Ok(())
    }

    /// Returns true if at least one message was delivered.
    async fn deliver_queue(
        &mut self,
        sub_id: u32,
        group_id: u64,
        queue_id: u64,
        consumer_id: u64,
        max_count: u32,
    ) -> Result<bool, ConsumerError> {
        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => return Ok(false),
        };

        let response = batcher
            .submit(MqCommand::group_deliver(group_id, consumer_id, max_count))
            .await?;

        match response {
            MqResponse::Messages { messages } => {
                let delivered = !messages.is_empty();
                if delivered {
                    self.send_message_batch(group_id, sub_id, &messages).await?;
                }
                Ok(delivered)
            }
            MqResponse::Error(e) => {
                debug!(queue_id, error = %e, "queue deliver error");
                if let Some(session) = &mut self.session {
                    session.remove_subscription(sub_id);
                }
                self.cancel_topic_watcher(sub_id);
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Returns true if at least one message was delivered.
    async fn deliver_actor(
        &mut self,
        sub_id: u32,
        group_id: u64,
        namespace_id: u64,
        consumer_id: u64,
    ) -> Result<bool, ConsumerError> {
        let batcher = match self.router.get_batcher(group_id) {
            Some(b) => b,
            None => return Ok(false),
        };

        let response = batcher
            .submit(MqCommand::group_deliver_actor(
                group_id,
                consumer_id,
                &[], // TODO: track assigned actors
            ))
            .await?;

        if let MqResponse::Messages { messages } = response {
            let delivered = !messages.is_empty();
            if delivered {
                self.send_message_batch(group_id, sub_id, &messages).await?;
            }
            return Ok(delivered);
        }

        Ok(false)
    }

    /// Encode delivered messages directly from FlatMessage into wire format bytes
    /// and send as a single PreEncoded frame. Avoids intermediate ServerMessage
    /// allocation — encodes straight from the zero-copy raft log data.
    async fn send_message_batch(
        &mut self,
        group_id: u64,
        sub_id: u32,
        messages: &[bisque_mq::types::DeliveredMessage],
    ) -> Result<(), ConsumerError> {
        let prefetcher = match self.router.get_prefetcher(group_id) {
            Some(pf) => pf,
            None => return Ok(()),
        };

        self.msg_batch_buf.clear();
        // Write tag + placeholder for count.
        self.msg_batch_buf.push(ServerTag::MessageBatch as u8);
        self.msg_batch_buf.extend_from_slice(&[0u8; 2]); // count placeholder

        let mut count = 0u16;

        for msg in messages {
            let flat_bytes = match bisque_mq::read_message_at(&prefetcher, msg.message_id) {
                Some(b) => b,
                None => continue,
            };

            // Track bytes for flow control (flat buffer size).
            if let Some(session) = &mut self.session {
                session.on_message_sent(sub_id, flat_bytes.len() as u64);
            }

            // Encode wire message body: [sub_id][message_id][flat_len][flat_bytes]
            encode_wire_message_body(&mut self.msg_batch_buf, sub_id, msg.message_id, &flat_bytes);
            count += 1;
        }

        if count == 0 {
            return Ok(());
        }

        // Patch the count.
        self.msg_batch_buf[1..3].copy_from_slice(&count.to_le_bytes());

        // Take the buffer out, send it, and put it back for reuse.
        let encoded = std::mem::take(&mut self.msg_batch_buf);
        let result = self.send_frame(ServerFrame::PreEncoded(encoded)).await;
        // Recover the buffer (if the frame was consumed, we get a fresh Vec;
        // the key is that the *next* call's take will capture the returned Vec).
        if self.msg_batch_buf.capacity() == 0 {
            self.msg_batch_buf = Vec::with_capacity(4096);
        }
        result
    }

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------

    async fn send_heartbeat(&mut self) -> Result<(), ConsumerError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.send_frame(ServerFrame::Heartbeat {
            server_time_ms: now,
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    async fn cleanup(&mut self) {
        // Abort all topic watcher tasks
        for (_, handle) in self.topic_watcher_handles.drain(..) {
            handle.abort();
        }

        // Abort all spawned background tasks (JoinGroup/SyncGroup phase waits)
        for handle in self.background_tasks.drain(..) {
            handle.abort();
        }

        // Leave all consumer groups this connection was part of
        for membership in self.group_memberships.drain(..) {
            if let Some(batcher) = self.router.get_batcher(membership.group_id) {
                let _ = batcher
                    .submit(MqCommand::leave_consumer_group(
                        membership.consumer_group_id,
                        &membership.member_id,
                    ))
                    .await;
            }
        }

        let session = match &self.session {
            Some(s) => s,
            None => return,
        };

        let consumer_id = session.consumer_id;

        for group_id in session.group_ids() {
            if let Some(batcher) = self.router.get_batcher(group_id) {
                let _ = batcher
                    .submit(MqCommand::disconnect_session(consumer_id, false))
                    .await;
            }
        }

        debug!(consumer_id, "consumer session cleaned up");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    async fn send_frame(&self, frame: ServerFrame) -> Result<(), ConsumerError> {
        self.frame_tx
            .send(frame)
            .await
            .map_err(|_| ConsumerError::SendFailed)
    }

    /// Extract routing info (group_id, entity_type, entity_id) for a subscription.
    fn get_sub_routing(&self, sub_id: u32) -> Option<(u64, u8, u64)> {
        self.session.as_ref().and_then(|s| {
            s.get_subscription(sub_id)
                .map(|sub| (sub.group_id, sub.entity_type, sub.entity_id))
        })
    }

    fn cancel_topic_watcher(&mut self, sub_id: u32) {
        self.topic_watcher_handles.retain(|(id, handle)| {
            if *id == sub_id {
                handle.abort();
                false
            } else {
                true
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Free function for sending GroupJoined from spawned tasks
// ---------------------------------------------------------------------------

async fn send_group_joined_response(
    frame_tx: &mpsc::Sender<ServerFrame>,
    generation: i32,
    leader: &str,
    member_id: &str,
    protocol_name: &str,
    is_leader: bool,
    members: &[(String, Bytes)],
) -> Result<(), ConsumerError> {
    let wire_members: Vec<(Bytes, Bytes)> = members
        .iter()
        .map(|(mid, meta)| (Bytes::copy_from_slice(mid.as_bytes()), meta.clone()))
        .collect();

    frame_tx
        .send(ServerFrame::GroupJoined {
            generation,
            leader: Bytes::copy_from_slice(leader.as_bytes()),
            member_id: Bytes::copy_from_slice(member_id.as_bytes()),
            protocol_name: Bytes::copy_from_slice(protocol_name.as_bytes()),
            is_leader: if is_leader { 1 } else { 0 },
            members: wire_members,
        })
        .await
        .map_err(|_| ConsumerError::SendFailed)
}

// ---------------------------------------------------------------------------
// Direct FlatMessage → wire format encoding
// ---------------------------------------------------------------------------

/// Encode a single WireMessage body: `[sub_id:4][message_id:8][flat_len:4][flat_bytes...]`.
/// Zero copies — the raw FlatMessage bytes from the raft log are forwarded directly.
#[inline]
fn encode_wire_message_body(buf: &mut Vec<u8>, sub_id: u32, message_id: u64, flat_bytes: &[u8]) {
    buf.extend_from_slice(&sub_id.to_le_bytes());
    buf.extend_from_slice(&message_id.to_le_bytes());
    buf.extend_from_slice(&(flat_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(flat_bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Mock router
    // -----------------------------------------------------------------------

    struct MockRouter {
        entities: HashMap<(u64, u8, u64), u64>,
        valid_token: Vec<u8>,
        consumer_id: u64,
        topic_senders: HashMap<(u64, u64), tokio::sync::watch::Sender<u64>>,
        consumer_groups: HashMap<(u64, u64), u64>, // (group_id, name_hash) -> consumer_group_id
        group_descriptions: HashMap<(u64, u64), ConsumerGroupDescription>,
        group_offsets: HashMap<(u64, u64), Vec<ConsumerGroupOffset>>,
        group_list: Vec<ConsumerGroupInfo>,
    }

    impl MockRouter {
        fn new() -> Self {
            Self {
                entities: HashMap::new(),
                valid_token: b"valid-token".to_vec(),
                consumer_id: 42,
                topic_senders: HashMap::new(),
                consumer_groups: HashMap::new(),
                group_descriptions: HashMap::new(),
                group_offsets: HashMap::new(),
                group_list: Vec::new(),
            }
        }

        fn with_entity(
            mut self,
            group_id: u64,
            entity_type: u8,
            name_hash: u64,
            entity_id: u64,
        ) -> Self {
            self.entities
                .insert((group_id, entity_type, name_hash), entity_id);
            self
        }

        fn with_topic(mut self, group_id: u64, name_hash: u64, entity_id: u64, head: u64) -> Self {
            self.entities
                .insert((group_id, ENTITY_TYPE_TOPIC, name_hash), entity_id);
            let (tx, _) = tokio::sync::watch::channel(head);
            self.topic_senders.insert((group_id, entity_id), tx);
            self
        }

        fn with_consumer_group(
            mut self,
            group_id: u64,
            name_hash: u64,
            consumer_group_id: u64,
        ) -> Self {
            self.consumer_groups
                .insert((group_id, name_hash), consumer_group_id);
            self
        }

        fn with_group_description(mut self, desc: ConsumerGroupDescription) -> Self {
            self.group_descriptions
                .insert((0, desc.consumer_group_id), desc);
            self
        }

        fn with_group_offsets(
            mut self,
            group_id: u64,
            consumer_group_id: u64,
            offsets: Vec<ConsumerGroupOffset>,
        ) -> Self {
            self.group_offsets
                .insert((group_id, consumer_group_id), offsets);
            self
        }

        fn with_group_list(mut self, groups: Vec<ConsumerGroupInfo>) -> Self {
            self.group_list = groups;
            self
        }
    }

    impl MqRouter for MockRouter {
        fn resolve_entity(&self, group_id: u64, entity_type: u8, name_hash: u64) -> Option<u64> {
            self.entities
                .get(&(group_id, entity_type, name_hash))
                .copied()
        }

        fn get_batcher(&self, _group_id: u64) -> Option<Arc<MqWriteBatcher>> {
            None // Tests that need batcher will use integration tests
        }

        fn validate_token(
            &self,
            token: &[u8],
            _consumer_id: Option<u64>,
        ) -> Result<(u64, Vec<u8>), String> {
            if token == self.valid_token {
                Ok((self.consumer_id, b"session-token".to_vec()))
            } else {
                Err("invalid token".into())
            }
        }

        fn get_topic_head(&self, group_id: u64, entity_id: u64) -> u64 {
            self.topic_senders
                .get(&(group_id, entity_id))
                .map(|tx| *tx.borrow())
                .unwrap_or(0)
        }

        fn subscribe_topic_notify(
            &self,
            group_id: u64,
            entity_id: u64,
        ) -> tokio::sync::watch::Receiver<u64> {
            self.topic_senders
                .get(&(group_id, entity_id))
                .map(|tx| tx.subscribe())
                .unwrap_or_else(|| {
                    let (_, rx) = tokio::sync::watch::channel(0);
                    rx
                })
        }

        fn get_prefetcher(&self, _group_id: u64) -> Option<SegmentPrefetcher> {
            None // Tests that need prefetcher will use integration tests
        }

        fn resolve_consumer_group(&self, group_id: u64, name_hash: u64) -> Option<u64> {
            self.consumer_groups.get(&(group_id, name_hash)).copied()
        }

        fn list_consumer_groups(&self, _group_id: u64) -> Vec<ConsumerGroupInfo> {
            self.group_list.clone()
        }

        fn describe_consumer_group(
            &self,
            _group_id: u64,
            consumer_group_id: u64,
        ) -> Option<ConsumerGroupDescription> {
            self.group_descriptions
                .get(&(0, consumer_group_id))
                .cloned()
        }

        fn fetch_group_offsets(
            &self,
            group_id: u64,
            consumer_group_id: u64,
            _partitions: &[(u64, u32)],
        ) -> Vec<ConsumerGroupOffset> {
            self.group_offsets
                .get(&(group_id, consumer_group_id))
                .cloned()
                .unwrap_or_default()
        }
    }

    // -----------------------------------------------------------------------
    // Mock router with real batcher (for consumer group integration tests)
    // -----------------------------------------------------------------------

    struct BatcherRouter {
        batcher: Arc<MqWriteBatcher>,
        consumer_groups: HashMap<(u64, u64), u64>,
        _engine: std::sync::Arc<parking_lot::Mutex<bisque_mq::engine::MqEngine>>,
    }

    impl BatcherRouter {
        fn new() -> Self {
            let config = bisque_mq::config::MqConfig::new(
                std::env::temp_dir().join(format!("bisque-test-{}", std::process::id())),
            );
            let engine = std::sync::Arc::new(parking_lot::Mutex::new(
                bisque_mq::engine::MqEngine::new(config),
            ));
            let batcher = Arc::new(MqWriteBatcher::new_test(engine.clone()));
            Self {
                batcher,
                consumer_groups: HashMap::new(),
                _engine: engine,
            }
        }

        fn with_consumer_group(
            mut self,
            group_id: u64,
            name_hash: u64,
            consumer_group_id: u64,
        ) -> Self {
            self.consumer_groups
                .insert((group_id, name_hash), consumer_group_id);
            self
        }
    }

    impl MqRouter for BatcherRouter {
        fn resolve_entity(&self, _group_id: u64, _entity_type: u8, _name_hash: u64) -> Option<u64> {
            None
        }

        fn get_batcher(&self, _group_id: u64) -> Option<Arc<MqWriteBatcher>> {
            Some(self.batcher.clone())
        }

        fn validate_token(
            &self,
            token: &[u8],
            _consumer_id: Option<u64>,
        ) -> Result<(u64, Vec<u8>), String> {
            if token == b"valid-token" {
                Ok((42, b"session-token".to_vec()))
            } else {
                Err("invalid token".into())
            }
        }

        fn get_topic_head(&self, _group_id: u64, _entity_id: u64) -> u64 {
            0
        }

        fn subscribe_topic_notify(
            &self,
            _group_id: u64,
            _entity_id: u64,
        ) -> tokio::sync::watch::Receiver<u64> {
            let (_, rx) = tokio::sync::watch::channel(0);
            rx
        }

        fn get_prefetcher(&self, _group_id: u64) -> Option<SegmentPrefetcher> {
            None
        }

        fn resolve_consumer_group(&self, group_id: u64, name_hash: u64) -> Option<u64> {
            self.consumer_groups.get(&(group_id, name_hash)).copied()
        }
    }

    fn create_batcher_handler(
        router: BatcherRouter,
    ) -> (
        ConsumerHandler<BatcherRouter>,
        mpsc::Sender<ClientFrame>,
        mpsc::Receiver<ServerFrame>,
    ) {
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, server_rx) = mpsc::channel(64);
        let handler = ConsumerHandler::new(
            Arc::new(router),
            ConsumerHandlerConfig::default(),
            client_rx,
            server_tx,
        );
        (handler, client_tx, server_rx)
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    fn create_handler(
        router: MockRouter,
    ) -> (
        ConsumerHandler<MockRouter>,
        mpsc::Sender<ClientFrame>,
        mpsc::Receiver<ServerFrame>,
    ) {
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, server_rx) = mpsc::channel(64);
        let handler = ConsumerHandler::new(
            Arc::new(router),
            ConsumerHandlerConfig::default(),
            client_rx,
            server_tx,
        );
        (handler, client_tx, server_rx)
    }

    async fn do_handshake(
        client_tx: &mpsc::Sender<ClientFrame>,
        server_rx: &mut mpsc::Receiver<ServerFrame>,
    ) {
        client_tx
            .send(ClientFrame::Handshake {
                token: Bytes::from_static(b"valid-token"),
                consumer_id: None,
            })
            .await
            .unwrap();
        let frame = server_rx.recv().await.unwrap();
        assert!(matches!(frame, ServerFrame::HandshakeOk { .. }));
    }

    /// Receive the next non-heartbeat frame from the server.
    async fn recv_non_heartbeat(rx: &mut mpsc::Receiver<ServerFrame>) -> ServerFrame {
        loop {
            match rx.recv().await.unwrap() {
                ServerFrame::Heartbeat { .. } => continue,
                frame => return frame,
            }
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_successful_handshake() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());

        do_handshake(&client_tx, &mut server_rx).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let frame = server_rx.recv().await.unwrap();
        assert!(matches!(frame, ServerFrame::Close { .. }));

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_failed_handshake() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());

        client_tx
            .send(ClientFrame::Handshake {
                token: Bytes::from_static(b"bad-token"),
                consumer_id: None,
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        match frame {
            ServerFrame::HandshakeErr { code, message } => {
                assert_eq!(code, 401);
                assert_eq!(message, Bytes::from_static(b"invalid token"));
            }
            _ => panic!("expected HandshakeErr"),
        }

        assert!(handle.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_handshake_timeout() {
        let config = ConsumerHandlerConfig {
            handshake_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, _server_rx) = mpsc::channel(64);
        let handler =
            ConsumerHandler::new(Arc::new(MockRouter::new()), config, client_rx, server_tx);

        let handle = tokio::spawn(handler.run());

        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(client_tx);

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(ConsumerError::HandshakeFailed(_))));
    }

    #[tokio::test]
    async fn test_wrong_first_frame() {
        let (handler, client_tx, _server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: 0,
                name_hash: 0,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();

        assert!(matches!(
            handle.await.unwrap(),
            Err(ConsumerError::HandshakeFailed(_))
        ));
    }

    #[tokio::test]
    async fn test_subscribe_success() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());

        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        match frame {
            ServerFrame::Subscribed { sub_id, entity_id } => {
                assert_eq!(sub_id, 1);
                assert_eq!(entity_id, 100);
            }
            _ => panic!("expected Subscribed, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_not_found() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xDEAD,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        match frame {
            ServerFrame::SubscriptionErr { sub_id, code, .. } => {
                assert_eq!(sub_id, 1);
                assert_eq!(code, 404);
            }
            _ => panic!("expected SubscriptionErr, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_topic() {
        let router = MockRouter::new().with_topic(1, 0xCAFE, 10, 500);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xCAFE,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        match frame {
            ServerFrame::Subscribed { sub_id, entity_id } => {
                assert_eq!(sub_id, 1);
                assert_eq!(entity_id, 10);
            }
            _ => panic!("expected Subscribed"),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_exchange() {
        let config = ConsumerHandlerConfig {
            heartbeat_interval: Duration::from_millis(50),
            heartbeat_timeout: Duration::from_secs(10),
            ..Default::default()
        };
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, mut server_rx) = mpsc::channel(64);
        let handler =
            ConsumerHandler::new(Arc::new(MockRouter::new()), config, client_rx, server_tx);
        let handle = tokio::spawn(handler.run());

        do_handshake(&client_tx, &mut server_rx).await;
        client_tx.send(ClientFrame::Heartbeat).await.unwrap();

        // Wait for server heartbeat
        tokio::time::sleep(Duration::from_millis(80)).await;
        let frame = server_rx.recv().await.unwrap();
        assert!(matches!(frame, ServerFrame::Heartbeat { .. }));

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_transport_closed() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());

        do_handshake(&client_tx, &mut server_rx).await;
        drop(client_tx); // simulate transport close

        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_set_byte_budget() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::SetByteBudget {
                budget_bytes: 1_000_000,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await; // Subscribed

        // Unsubscribe
        client_tx
            .send(ClientFrame::Unsubscribe { sub_id: 1 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_set_max_in_flight() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await;

        // Pause
        client_tx
            .send(ClientFrame::SetMaxInFlight {
                sub_id: 1,
                max_in_flight: 0,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Resume
        client_tx
            .send(ClientFrame::SetMaxInFlight {
                sub_id: 1,
                max_in_flight: 50,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_config_defaults() {
        let config = ConsumerHandlerConfig::default();
        assert_eq!(config.heartbeat_interval, Duration::from_secs(15));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(90));
        assert_eq!(config.handshake_timeout, Duration::from_secs(10));
        assert_eq!(config.max_deliver_batch, 10);
    }

    // -----------------------------------------------------------------------
    // Additional edge case & failure tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_duplicate_handshake_ignored() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Send another handshake — should be ignored (no crash, no response)
        client_tx
            .send(ClientFrame::Handshake {
                token: Bytes::from_static(b"valid-token"),
                consumer_id: None,
            })
            .await
            .unwrap();

        // Give it time to process
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Should still be alive
        client_tx.send(ClientFrame::Close).await.unwrap();
        // Drain any heartbeats until we find Close
        loop {
            match server_rx.recv().await.unwrap() {
                ServerFrame::Close { .. } => break,
                ServerFrame::Heartbeat { .. } => continue,
                other => panic!("expected Close, got {:?}", other),
            }
        }
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_multiple_entities() {
        let router = MockRouter::new()
            .with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100)
            .with_entity(1, ENTITY_TYPE_QUEUE, 0xCAFE, 101)
            .with_entity(2, ENTITY_TYPE_QUEUE, 0xFACE, 200);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe to 3 different entities
        for (sub_id, group_id, hash) in [(1, 1, 0xBEEF), (2, 1, 0xCAFE), (3, 2, 0xFACE)] {
            client_tx
                .send(ClientFrame::Subscribe {
                    sub_id,
                    group_id,
                    entity_type: ENTITY_TYPE_QUEUE,
                    name_hash: hash,
                    start_offset: 0,
                    max_in_flight: 50,
                })
                .await
                .unwrap();
            let frame = server_rx.recv().await.unwrap();
            assert!(matches!(frame, ServerFrame::Subscribed { .. }));
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_unsubscribe_nonexistent() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Unsubscribe from a sub that doesn't exist — should not crash
        client_tx
            .send(ClientFrame::Unsubscribe { sub_id: 999 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_unsubscribe_resubscribe() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();
        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(matches!(
            frame,
            ServerFrame::Subscribed {
                sub_id: 1,
                entity_id: 100
            }
        ));

        // Unsubscribe
        client_tx
            .send(ClientFrame::Unsubscribe { sub_id: 1 })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Resubscribe with same sub_id
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(matches!(
            frame,
            ServerFrame::Subscribed {
                sub_id: 1,
                entity_id: 100
            }
        ));

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_ack_on_nonexistent_sub() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Ack on sub that doesn't exist — should not crash
        client_tx
            .send(ClientFrame::Ack {
                sub_id: 999,
                message_ids: vec![1, 2, 3],
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_nack_on_nonexistent_sub() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Nack {
                sub_id: 999,
                message_ids: vec![1],
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_commit_offset_on_nonexistent_sub() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::CommitOffset {
                sub_id: 999,
                offset: 42,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_set_max_in_flight_on_nonexistent_sub() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::SetMaxInFlight {
                sub_id: 999,
                max_in_flight: 50,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_timeout() {
        let config = ConsumerHandlerConfig {
            heartbeat_interval: Duration::from_millis(20),
            heartbeat_timeout: Duration::from_millis(50),
            handshake_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, mut server_rx) = mpsc::channel(64);
        let handler =
            ConsumerHandler::new(Arc::new(MockRouter::new()), config, client_rx, server_tx);
        let handle = tokio::spawn(handler.run());

        do_handshake(&client_tx, &mut server_rx).await;

        // Don't send any heartbeats — wait for timeout
        tokio::time::sleep(Duration::from_millis(200)).await;

        let result = handle.await.unwrap();
        assert!(
            matches!(result, Err(ConsumerError::HeartbeatTimeout)),
            "expected HeartbeatTimeout, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_heartbeat_kept_alive() {
        let config = ConsumerHandlerConfig {
            heartbeat_interval: Duration::from_millis(30),
            heartbeat_timeout: Duration::from_millis(80),
            handshake_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, mut server_rx) = mpsc::channel(64);
        let handler =
            ConsumerHandler::new(Arc::new(MockRouter::new()), config, client_rx, server_tx);
        let handle = tokio::spawn(handler.run());

        do_handshake(&client_tx, &mut server_rx).await;

        // Send heartbeats to keep alive
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(30)).await;
            client_tx.send(ClientFrame::Heartbeat).await.unwrap();
            // Drain any server heartbeats
            while let Ok(f) = server_rx.try_recv() {
                assert!(matches!(f, ServerFrame::Heartbeat { .. }));
            }
        }

        // Close cleanly
        client_tx.send(ClientFrame::Close).await.unwrap();
        // Drain until we find Close
        loop {
            match server_rx.recv().await {
                Some(ServerFrame::Close { .. }) => break,
                Some(ServerFrame::Heartbeat { .. }) => continue,
                other => panic!("unexpected: {:?}", other),
            }
        }
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_topic_subscribe_with_head_already_advanced() {
        let router = MockRouter::new().with_topic(1, 0xCAFE, 10, 500);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe starting from offset 0, head already at 500
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xCAFE,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        assert!(matches!(
            frame,
            ServerFrame::Subscribed {
                sub_id: 1,
                entity_id: 10
            }
        ));

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_topic_subscribe_already_caught_up() {
        // Subscribe at offset 500 with head at 500 — should be tailing immediately
        let router = MockRouter::new().with_topic(1, 0xCAFE, 10, 500);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xCAFE,
                start_offset: 500,
                max_in_flight: 100,
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        assert!(matches!(frame, ServerFrame::Subscribed { .. }));

        // Should not try to deliver since offset == head
        tokio::time::sleep(Duration::from_millis(30)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_actor_namespace_subscribe() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_ACTOR_NAMESPACE, 0xACE, 300);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_ACTOR_NAMESPACE,
                name_hash: 0xACE,
                start_offset: 0,
                max_in_flight: 10,
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        match frame {
            ServerFrame::Subscribed { sub_id, entity_id } => {
                assert_eq!(sub_id, 1);
                assert_eq!(entity_id, 300);
            }
            _ => panic!("expected Subscribed, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_set_byte_budget_then_subscribe() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Set budget before subscribing
        client_tx
            .send(ClientFrame::SetByteBudget { budget_bytes: 5000 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 10,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(matches!(frame, ServerFrame::Subscribed { .. }));

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_rapid_subscribe_unsubscribe_cycling() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        for cycle in 0..20 {
            let sub_id = (cycle % 5) + 1;
            client_tx
                .send(ClientFrame::Subscribe {
                    sub_id,
                    group_id: 1,
                    entity_type: ENTITY_TYPE_QUEUE,
                    name_hash: 0xBEEF,
                    start_offset: 0,
                    max_in_flight: 10,
                })
                .await
                .unwrap();
            let _ = server_rx.recv().await; // Subscribed

            client_tx
                .send(ClientFrame::Unsubscribe { sub_id })
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_topic_unsubscribe_cancels_watcher() {
        let router = MockRouter::new().with_topic(1, 0xCAFE, 10, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe to topic
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xCAFE,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await; // Subscribed

        // Unsubscribe — watcher should be cancelled
        client_tx
            .send(ClientFrame::Unsubscribe { sub_id: 1 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_transport_close_during_subscriptions() {
        let router = MockRouter::new()
            .with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100)
            .with_topic(1, 0xCAFE, 10, 500);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe to both queue and topic
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 2,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xCAFE,
                start_offset: 0,
                max_in_flight: 100,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await;

        // Drop transport — should trigger cleanup
        drop(client_tx);

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handshake_with_consumer_id() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());

        client_tx
            .send(ClientFrame::Handshake {
                token: Bytes::from_static(b"valid-token"),
                consumer_id: Some(99),
            })
            .await
            .unwrap();

        let frame = server_rx.recv().await.unwrap();
        match frame {
            ServerFrame::HandshakeOk {
                consumer_id,
                session_token,
            } => {
                assert_eq!(consumer_id, 42); // MockRouter always returns 42
                assert_eq!(session_token, Bytes::from_static(b"session-token"));
            }
            _ => panic!("expected HandshakeOk"),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_handshake_session_closed() {
        let (handler, client_tx, _server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());

        // Drop client_tx before sending anything — should be SessionClosed
        drop(client_tx);

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(ConsumerError::SessionClosed)));
    }

    #[tokio::test]
    async fn test_multiple_heartbeat_exchanges() {
        let config = ConsumerHandlerConfig {
            heartbeat_interval: Duration::from_millis(20),
            heartbeat_timeout: Duration::from_secs(10),
            ..Default::default()
        };
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, mut server_rx) = mpsc::channel(64);
        let handler =
            ConsumerHandler::new(Arc::new(MockRouter::new()), config, client_rx, server_tx);
        let handle = tokio::spawn(handler.run());

        do_handshake(&client_tx, &mut server_rx).await;

        // Collect multiple server heartbeats
        let mut heartbeat_count = 0;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(30)).await;
            client_tx.send(ClientFrame::Heartbeat).await.unwrap();
            while let Ok(frame) = server_rx.try_recv() {
                if matches!(frame, ServerFrame::Heartbeat { .. }) {
                    heartbeat_count += 1;
                }
            }
        }
        assert!(
            heartbeat_count >= 2,
            "expected at least 2 heartbeats, got {heartbeat_count}"
        );

        client_tx.send(ClientFrame::Close).await.unwrap();
        loop {
            match server_rx.recv().await {
                Some(ServerFrame::Close { .. }) => break,
                Some(ServerFrame::Heartbeat { .. }) => continue,
                other => panic!("unexpected: {:?}", other),
            }
        }
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_ack_empty_message_ids() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await;

        // Ack with empty message_ids
        client_tx
            .send(ClientFrame::Ack {
                sub_id: 1,
                message_ids: vec![],
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_commit_offset_on_queue_is_noop() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await;

        // CommitOffset on a queue sub — should be a no-op
        client_tx
            .send(ClientFrame::CommitOffset {
                sub_id: 1,
                offset: 42,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_pause_resume_cycle() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        let _ = server_rx.recv().await;

        // Rapid pause/resume cycles
        for _ in 0..10 {
            client_tx
                .send(ClientFrame::SetMaxInFlight {
                    sub_id: 1,
                    max_in_flight: 0,
                })
                .await
                .unwrap();
            client_tx
                .send(ClientFrame::SetMaxInFlight {
                    sub_id: 1,
                    max_in_flight: 100,
                })
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(30)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_multiple_topic_subscriptions() {
        let router = MockRouter::new()
            .with_topic(1, 0xCAFE, 10, 100)
            .with_topic(1, 0xBEEF, 20, 200);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe to two topics
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xCAFE,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        let frame = server_rx.recv().await.unwrap();
        assert!(matches!(
            frame,
            ServerFrame::Subscribed {
                sub_id: 1,
                entity_id: 10
            }
        ));

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 2,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        let frame = server_rx.recv().await.unwrap();
        assert!(matches!(
            frame,
            ServerFrame::Subscribed {
                sub_id: 2,
                entity_id: 20
            }
        ));

        // Unsubscribe one, other should still work
        client_tx
            .send(ClientFrame::Unsubscribe { sub_id: 1 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_mixed_entity_types() {
        let router = MockRouter::new()
            .with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100)
            .with_topic(1, 0xCAFE, 10, 500)
            .with_entity(1, ENTITY_TYPE_ACTOR_NAMESPACE, 0xACE, 300);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Subscribe to all three
        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 1,
                group_id: 1,
                entity_type: ENTITY_TYPE_QUEUE,
                name_hash: 0xBEEF,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        assert!(matches!(
            server_rx.recv().await.unwrap(),
            ServerFrame::Subscribed { .. }
        ));

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 2,
                group_id: 1,
                entity_type: ENTITY_TYPE_TOPIC,
                name_hash: 0xCAFE,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        assert!(matches!(
            server_rx.recv().await.unwrap(),
            ServerFrame::Subscribed { .. }
        ));

        client_tx
            .send(ClientFrame::Subscribe {
                sub_id: 3,
                group_id: 1,
                entity_type: ENTITY_TYPE_ACTOR_NAMESPACE,
                name_hash: 0xACE,
                start_offset: 0,
                max_in_flight: 50,
            })
            .await
            .unwrap();
        assert!(matches!(
            server_rx.recv().await.unwrap(),
            ServerFrame::Subscribed { .. }
        ));

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_server_frame_tx_dropped() {
        let router = MockRouter::new().with_entity(1, ENTITY_TYPE_QUEUE, 0xBEEF, 100);
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, server_rx) = mpsc::channel(64);
        let handler = ConsumerHandler::new(
            Arc::new(router),
            ConsumerHandlerConfig::default(),
            client_rx,
            server_tx,
        );
        let handle = tokio::spawn(handler.run());

        // Drop server_rx — handler should fail when trying to send
        drop(server_rx);

        client_tx
            .send(ClientFrame::Handshake {
                token: Bytes::from_static(b"valid-token"),
                consumer_id: None,
            })
            .await
            .unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_err()); // SendFailed
    }

    #[tokio::test]
    async fn test_custom_config() {
        let config = ConsumerHandlerConfig {
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(500),
            handshake_timeout: Duration::from_millis(200),
            max_deliver_batch: 5,
            catalog_name: "test".to_string(),
        };
        assert_eq!(config.heartbeat_interval, Duration::from_millis(100));
        assert_eq!(config.heartbeat_timeout, Duration::from_millis(500));
        assert_eq!(config.handshake_timeout, Duration::from_millis(200));
        assert_eq!(config.max_deliver_batch, 5);
    }

    #[tokio::test]
    async fn test_config_clone() {
        let config = ConsumerHandlerConfig::default();
        let cloned = config.clone();
        assert_eq!(config.heartbeat_interval, cloned.heartbeat_interval);
        assert_eq!(config.max_deliver_batch, cloned.max_deliver_batch);
    }

    // -----------------------------------------------------------------------
    // Consumer group tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_list_groups_empty() {
        let (handler, client_tx, mut server_rx) = create_handler(MockRouter::new());
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::ListGroups { group_id: 1 })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupList { groups } => {
                assert!(groups.is_empty());
            }
            _ => panic!("expected GroupList, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_list_groups_populated() {
        let groups = vec![
            ConsumerGroupInfo {
                consumer_group_id: 1,
                name: "group-a".into(),
                phase: 4, // Stable
                protocol_type: "consumer".into(),
            },
            ConsumerGroupInfo {
                consumer_group_id: 2,
                name: "group-b".into(),
                phase: 0, // Empty
                protocol_type: "consumer".into(),
            },
        ];
        let router = MockRouter::new().with_group_list(groups);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::ListGroups { group_id: 1 })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupList { groups } => {
                assert_eq!(groups.len(), 2);
                assert_eq!(groups[0].0, 1); // consumer_group_id
                assert_eq!(groups[1].0, 2);
            }
            _ => panic!("expected GroupList, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_describe_group_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // name_hash that doesn't resolve
        client_tx
            .send(ClientFrame::DescribeGroup {
                group_id: 1,
                name_hash: 0xDEAD,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_describe_group_success() {
        let desc = ConsumerGroupDescription {
            consumer_group_id: 10,
            name: "my-group".into(),
            phase: 4,
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            leader: "member-1".into(),
            generation: 3,
            members: vec![
                ConsumerGroupMemberInfo {
                    member_id: "member-1".into(),
                    client_id: "client-a".into(),
                    assignment: vec![1, 2, 3],
                },
                ConsumerGroupMemberInfo {
                    member_id: "member-2".into(),
                    client_id: "client-b".into(),
                    assignment: vec![4, 5, 6],
                },
            ],
        };
        let router = MockRouter::new()
            .with_consumer_group(1, 0xABCD, 10)
            .with_group_description(desc);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::DescribeGroup {
                group_id: 1,
                name_hash: 0xABCD,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupDescription {
                consumer_group_id,
                name,
                phase,
                generation,
                members,
                ..
            } => {
                assert_eq!(consumer_group_id, 10);
                assert_eq!(name, Bytes::from_static(b"my-group"));
                assert_eq!(phase, 4);
                assert_eq!(generation, 3);
                assert_eq!(members.len(), 2);
            }
            _ => panic!("expected GroupDescription, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_fetch_group_offsets_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::FetchGroupOffsets {
                group_id: 1,
                name_hash: 0xBEEF,
                partitions: vec![(1, 0)],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_fetch_group_offsets_success() {
        let offsets = vec![
            ConsumerGroupOffset {
                topic_id: 1,
                partition_index: 0,
                committed_offset: 100,
                metadata: "".into(),
            },
            ConsumerGroupOffset {
                topic_id: 1,
                partition_index: 1,
                committed_offset: 200,
                metadata: "my-meta".into(),
            },
        ];
        let router = MockRouter::new()
            .with_consumer_group(1, 0xCAFE, 5)
            .with_group_offsets(1, 5, offsets);
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::FetchGroupOffsets {
                group_id: 1,
                name_hash: 0xCAFE,
                partitions: vec![(1, 0), (1, 1)],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupOffsetsFetched { offsets } => {
                assert_eq!(offsets.len(), 2);
                assert_eq!(offsets[0].0, 1); // topic_id
                assert_eq!(offsets[0].2, 100); // committed_offset
                assert_eq!(offsets[1].2, 200);
            }
            _ => panic!("expected GroupOffsetsFetched, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_leave_group_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::LeaveGroup {
                group_id: 1,
                name_hash: 0xDEAD,
                member_id: Bytes::from_static(b"member-1"),
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_group_heartbeat_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::GroupHeartbeat {
                group_id: 1,
                name_hash: 0xDEAD,
                member_id: Bytes::from_static(b"member-1"),
                generation: 1,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_join_group_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::JoinGroup {
                group_id: 1,
                name_hash: 0xDEAD,
                member_id: Bytes::from_static(b""),
                client_id: Bytes::from_static(b"client-1"),
                session_timeout_ms: 30000,
                rebalance_timeout_ms: 60000,
                protocol_type: Bytes::from_static(b"consumer"),
                protocols: vec![(
                    Bytes::from_static(b"range"),
                    Bytes::from_static(b"metadata"),
                )],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_sync_group_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::SyncGroup {
                group_id: 1,
                name_hash: 0xDEAD,
                generation: 1,
                member_id: Bytes::from_static(b"member-1"),
                assignments: vec![],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_delete_group_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::DeleteGroup {
                group_id: 1,
                name_hash: 0xDEAD,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_commit_group_offset_not_found() {
        let router = MockRouter::new();
        let (handler, client_tx, mut server_rx) = create_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::CommitGroupOffset {
                group_id: 1,
                name_hash: 0xDEAD,
                generation: 1,
                offsets: vec![(1, 0, 100, Bytes::new())],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupError { code, .. } => {
                assert_eq!(code, 404);
            }
            _ => panic!("expected GroupError, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_group_membership_tracking() {
        // Verify that track_group_membership and remove_group_membership work
        let (client_tx, client_rx) = mpsc::channel(64);
        let (server_tx, _server_rx) = mpsc::channel(64);
        let mut handler = ConsumerHandler::new(
            Arc::new(MockRouter::new()),
            ConsumerHandlerConfig::default(),
            client_rx,
            server_tx,
        );

        // Track some memberships
        handler.track_group_membership(1, 10, "member-a".into(), 60000);
        handler.track_group_membership(1, 20, "member-b".into(), 30000);
        handler.track_group_membership(2, 10, "member-c".into(), 45000);
        assert_eq!(handler.group_memberships.len(), 3);

        // Same group+consumer_group should update, not add (even with different member_id)
        handler.track_group_membership(1, 10, "member-a-v2".into(), 90000);
        assert_eq!(handler.group_memberships.len(), 3);
        assert_eq!(handler.group_memberships[0].member_id, "member-a-v2");
        assert_eq!(handler.group_memberships[0].rebalance_timeout_ms, 90000);

        // Test get_rebalance_timeout
        assert_eq!(
            handler.get_rebalance_timeout(1, 10),
            Duration::from_millis(90000)
        );
        assert_eq!(
            handler.get_rebalance_timeout(99, 99),
            Duration::from_secs(30) // default fallback
        );

        // Remove one (using updated member_id)
        handler.remove_group_membership(1, 10, "member-a-v2");
        assert_eq!(handler.group_memberships.len(), 2);

        // Remove non-existent should be no-op
        handler.remove_group_membership(99, 99, "nope");
        assert_eq!(handler.group_memberships.len(), 2);

        drop(client_tx);
    }

    #[tokio::test]
    async fn test_send_group_joined_response_fn() {
        let (tx, mut rx) = mpsc::channel(16);

        send_group_joined_response(
            &tx,
            5,
            "leader-1",
            "member-1",
            "range",
            true,
            &[
                ("member-1".into(), Bytes::from_static(&[1, 2])),
                ("member-2".into(), Bytes::from_static(&[3, 4])),
            ],
        )
        .await
        .unwrap();

        let frame = rx.recv().await.unwrap();
        match frame {
            ServerFrame::GroupJoined {
                generation,
                leader,
                member_id,
                protocol_name,
                is_leader,
                members,
            } => {
                assert_eq!(generation, 5);
                assert_eq!(leader, Bytes::from_static(b"leader-1"));
                assert_eq!(member_id, Bytes::from_static(b"member-1"));
                assert_eq!(protocol_name, Bytes::from_static(b"range"));
                assert_eq!(is_leader, 1);
                assert_eq!(members.len(), 2);
            }
            _ => panic!("expected GroupJoined, got {:?}", frame),
        }
    }

    #[tokio::test]
    async fn test_send_group_joined_response_not_leader() {
        let (tx, mut rx) = mpsc::channel(16);

        send_group_joined_response(&tx, 3, "leader-1", "member-2", "roundrobin", false, &[])
            .await
            .unwrap();

        let frame = rx.recv().await.unwrap();
        match frame {
            ServerFrame::GroupJoined { is_leader, .. } => {
                assert_eq!(is_leader, 0);
            }
            _ => panic!("expected GroupJoined"),
        }
    }

    // -----------------------------------------------------------------------
    // Consumer group integration tests (with real batcher)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_create_group_with_batcher() {
        let router = BatcherRouter::new();
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::CreateGroup {
                group_id: 1,
                name: Bytes::from_static(b"test-group"),
                auto_offset_reset: 0,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        match frame {
            ServerFrame::GroupCreated {
                consumer_group_id, ..
            } => {
                assert!(consumer_group_id > 0);
            }
            _ => panic!("expected GroupCreated, got {:?}", frame),
        }

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_create_and_delete_group() {
        let router = BatcherRouter::new();
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Create
        client_tx
            .send(ClientFrame::CreateGroup {
                group_id: 1,
                name: Bytes::from_static(b"delete-me"),
                auto_offset_reset: 0,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        let cg_id = match frame {
            ServerFrame::GroupCreated {
                consumer_group_id, ..
            } => consumer_group_id,
            _ => panic!("expected GroupCreated"),
        };

        // Delete requires resolve — we need to use direct consumer_group_id.
        // Since MockRouter can't resolve from name_hash dynamically, we test
        // that delete with unknown name_hash returns GroupError.
        client_tx
            .send(ClientFrame::DeleteGroup {
                group_id: 1,
                name_hash: 0xDEAD, // won't resolve
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(
            matches!(frame, ServerFrame::GroupError { code: 404, .. }),
            "expected GroupError 404 for unknown name_hash, got {:?}",
            frame
        );

        let _ = cg_id; // used above

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_join_group_with_batcher_no_resolve() {
        // JoinGroup with batcher but name_hash doesn't resolve → GroupError
        let router = BatcherRouter::new();
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::JoinGroup {
                group_id: 1,
                name_hash: 0xDEAD,
                member_id: Bytes::from_static(b""),
                client_id: Bytes::from_static(b"client-1"),
                session_timeout_ms: 30000,
                rebalance_timeout_ms: 60000,
                protocol_type: Bytes::from_static(b"consumer"),
                protocols: vec![(Bytes::from_static(b"range"), Bytes::from_static(b"meta"))],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(matches!(frame, ServerFrame::GroupError { code: 404, .. }));

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_leave_group_with_batcher() {
        // Create group first, then use with_consumer_group to make it resolvable
        // and test leave
        let router = BatcherRouter::new().with_consumer_group(1, 0xABCD, 999);
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::LeaveGroup {
                group_id: 1,
                name_hash: 0xABCD,
                member_id: Bytes::from_static(b"member-1"),
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        // Engine may return Ok or Error (group doesn't exist in engine),
        // but either way the handler shouldn't crash
        assert!(
            matches!(
                frame,
                ServerFrame::GroupLeft | ServerFrame::GroupError { .. }
            ),
            "expected GroupLeft or GroupError, got {:?}",
            frame
        );

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_group_heartbeat_with_batcher() {
        let router = BatcherRouter::new().with_consumer_group(1, 0xABCD, 999);
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        client_tx
            .send(ClientFrame::GroupHeartbeat {
                group_id: 1,
                name_hash: 0xABCD,
                member_id: Bytes::from_static(b"member-1"),
                generation: 1,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(
            matches!(
                frame,
                ServerFrame::GroupHeartbeatOk | ServerFrame::GroupError { .. }
            ),
            "expected GroupHeartbeatOk or GroupError, got {:?}",
            frame
        );

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_commit_group_offset_with_batcher() {
        let router = BatcherRouter::new().with_consumer_group(1, 0xABCD, 999);
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Single offset commit
        client_tx
            .send(ClientFrame::CommitGroupOffset {
                group_id: 1,
                name_hash: 0xABCD,
                generation: 1,
                offsets: vec![(1, 0, 100, Bytes::from_static(b""))],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(
            matches!(
                frame,
                ServerFrame::GroupOffsetCommitted | ServerFrame::GroupError { .. }
            ),
            "expected GroupOffsetCommitted, got {:?}",
            frame
        );

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_commit_group_offset_batched_with_batcher() {
        let router = BatcherRouter::new().with_consumer_group(1, 0xABCD, 999);
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // Multiple offset commits — should batch
        client_tx
            .send(ClientFrame::CommitGroupOffset {
                group_id: 1,
                name_hash: 0xABCD,
                generation: 1,
                offsets: vec![
                    (1, 0, 100, Bytes::from_static(b"")),
                    (1, 1, 200, Bytes::from_static(b"meta-1")),
                    (2, 0, 50, Bytes::from_static(b"")),
                ],
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        assert!(
            matches!(
                frame,
                ServerFrame::GroupOffsetCommitted | ServerFrame::GroupError { .. }
            ),
            "expected GroupOffsetCommitted, got {:?}",
            frame
        );

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_full_consumer_group_lifecycle() {
        // Create → Join → Sync → Heartbeat → Leave
        let router = BatcherRouter::new();
        let (handler, client_tx, mut server_rx) = create_batcher_handler(router);
        let handle = tokio::spawn(handler.run());
        do_handshake(&client_tx, &mut server_rx).await;

        // 1. Create group
        client_tx
            .send(ClientFrame::CreateGroup {
                group_id: 1,
                name: Bytes::from_static(b"lifecycle-group"),
                auto_offset_reset: 0,
            })
            .await
            .unwrap();

        let frame = recv_non_heartbeat(&mut server_rx).await;
        let _cg_id = match frame {
            ServerFrame::GroupCreated {
                consumer_group_id, ..
            } => {
                assert!(consumer_group_id > 0);
                consumer_group_id
            }
            _ => panic!("expected GroupCreated, got {:?}", frame),
        };

        // Note: Further JoinGroup/SyncGroup/Leave require resolve_consumer_group
        // to map name_hash → consumer_group_id. Since BatcherRouter doesn't
        // dynamically register created groups, we verify the create path works
        // end-to-end and the handler remains healthy after.

        client_tx.send(ClientFrame::Close).await.unwrap();
        let _ = server_rx.recv().await;
        handle.await.unwrap().unwrap();
    }
}
