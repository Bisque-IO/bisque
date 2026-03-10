use std::sync::Arc;
use std::time::Duration;

use bisque_mq::flat::FlatMessage;
use bisque_mq::types::{MqCommand, MqResponse, PartitionInfo};
use bisque_mq::write_batcher::MqWriteBatcher;
use bisque_mq_protocol::types::{ClientFrame, ServerFrame, ServerMessage};
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
}

impl Default for ConsumerHandlerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(15),
            heartbeat_timeout: Duration::from_secs(90),
            handshake_timeout: Duration::from_secs(10),
            max_deliver_batch: 10,
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
    /// Reusable buffer for message batch delivery — avoids per-call Vec allocation.
    msg_batch_buf: Vec<ServerMessage>,
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
                        self.session = Some(ConsumerSession::new(cid, session_token.clone()));
                        self.send_frame(ServerFrame::HandshakeOk {
                            consumer_id: cid,
                            session_token,
                        })
                        .await?;
                        debug!(consumer_id = cid, "consumer handshake complete");
                        Ok(())
                    }
                    Err(msg) => {
                        let message = Bytes::from(msg.clone().into_bytes());
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
                        .submit(MqCommand::ack(entity_id, &message_ids, None))
                        .await;
                }
            }
            ENTITY_TYPE_ACTOR_NAMESPACE => {
                if let Some(batcher) = self.router.get_batcher(group_id) {
                    for &msg_id in &message_ids {
                        let _ = batcher
                            .submit(MqCommand::ack_actor_message(
                                entity_id,
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
                        .submit(MqCommand::nack(entity_id, &message_ids))
                        .await;
                }
            }
            ENTITY_TYPE_ACTOR_NAMESPACE => {
                if let Some(batcher) = self.router.get_batcher(group_id) {
                    for &msg_id in &message_ids {
                        let _ = batcher
                            .submit(MqCommand::nack_actor_message(
                                entity_id,
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
            .submit(MqCommand::deliver(queue_id, consumer_id, max_count))
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
            .submit(MqCommand::deliver_actor_message(
                namespace_id,
                &[], // TODO: track assigned actors
                consumer_id,
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

    /// Convert delivered messages to ServerMessages and send as a single
    /// MessageBatch frame. Reuses an internal buffer to avoid per-call allocation.
    async fn send_message_batch(
        &mut self,
        group_id: u64,
        sub_id: u32,
        messages: &[bisque_mq::types::DeliveredMessage],
    ) -> Result<(), ConsumerError> {
        let prefetcher = self.router.get_prefetcher(group_id);
        self.msg_batch_buf.clear();

        for msg in messages {
            let server_msg = if let Some(ref pf) = prefetcher {
                flat_to_server_message(pf, sub_id, msg.message_id)
            } else {
                None
            };
            if let Some(m) = server_msg {
                self.msg_batch_buf.push(m);
            }
        }

        if self.msg_batch_buf.is_empty() {
            return Ok(());
        }

        // Track bytes per message for flow control before sending.
        if let Some(session) = &mut self.session {
            for m in &self.msg_batch_buf {
                session.on_message_sent(sub_id, m.payload_bytes());
            }
        }

        // Drain elements into a new Vec, preserving the buffer's heap allocation
        // so the next call reuses it without re-allocating.
        let batch: Vec<ServerMessage> = self.msg_batch_buf.drain(..).collect();
        self.send_frame(ServerFrame::MessageBatch(batch)).await?;

        Ok(())
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

        let session = match &self.session {
            Some(s) => s,
            None => return,
        };

        let consumer_id = session.consumer_id;

        for group_id in session.group_ids() {
            if let Some(batcher) = self.router.get_batcher(group_id) {
                let _ = batcher
                    .submit(MqCommand::disconnect_consumer(consumer_id))
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
// Flat message → ServerMessage conversion
// ---------------------------------------------------------------------------

/// Read a flat message from the raft log and convert it to a `ServerMessage`.
/// Returns `None` if the message has been purged or is unreadable.
fn flat_to_server_message(
    prefetcher: &SegmentPrefetcher,
    sub_id: u32,
    message_id: u64,
) -> Option<ServerMessage> {
    let flat_bytes = bisque_mq::read_message_at(prefetcher, message_id)?;
    let flat = FlatMessage::new(flat_bytes)?;
    Some(ServerMessage {
        sub_id,
        message_id,
        timestamp: flat.timestamp(),
        key: flat.key(),
        value: flat.value(),
        headers: flat.headers().collect(),
    })
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
    }

    impl MockRouter {
        fn new() -> Self {
            Self {
                entities: HashMap::new(),
                valid_token: b"valid-token".to_vec(),
                consumer_id: 42,
                topic_senders: HashMap::new(),
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
}
