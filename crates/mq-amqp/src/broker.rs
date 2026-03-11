//! Broker trait for AMQP 1.0 ↔ bisque-mq integration.
//!
//! The `MessageBroker` trait abstracts the message queue engine operations
//! needed by the AMQP protocol adapter. Implementations may go through Raft
//! consensus (production) or operate directly on an engine (testing).

use bytes::Bytes;
use smallvec::SmallVec;

use crate::types::{AmqpMessage, DeliveryState, Outcome, WireString};

/// Result of publishing a message to the broker.
#[derive(Debug, Clone)]
pub struct PublishResult {
    /// Offset or message ID assigned by the broker.
    pub offset: u64,
}

/// A message delivered from the broker to an AMQP consumer.
#[derive(Debug, Clone)]
pub struct BrokerMessage {
    /// Broker-assigned message ID (for ack/nack).
    pub message_id: u64,
    /// The AMQP message payload (encoded sections).
    pub payload: Bytes,
    /// Delivery attempt count.
    pub attempt: u32,
}

/// Outcome of settling a delivery with the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleAction {
    /// Message acknowledged and removed from queue.
    Accept,
    /// Message rejected (optionally dead-lettered).
    Reject,
    /// Message released back to the queue for redelivery.
    Release,
    /// Message modified (redelivery with updated annotations).
    Modify,
}

impl SettleAction {
    /// Convert from AMQP DeliveryState.
    pub fn from_delivery_state(state: &DeliveryState) -> Self {
        match state {
            DeliveryState::Accepted => Self::Accept,
            DeliveryState::Rejected { .. } => Self::Reject,
            DeliveryState::Released => Self::Release,
            DeliveryState::Modified { .. } => Self::Modify,
            DeliveryState::Received { .. } => Self::Accept,
            DeliveryState::Transactional(ts) => {
                // Use the inner outcome if present, otherwise default to Accept
                match ts.outcome.as_ref() {
                    Some(Outcome::Accepted) => Self::Accept,
                    Some(Outcome::Rejected { .. }) => Self::Reject,
                    Some(Outcome::Released) => Self::Release,
                    Some(Outcome::Modified { .. }) => Self::Modify,
                    None => Self::Accept,
                }
            }
            DeliveryState::Declared { .. } => Self::Accept,
        }
    }
}

/// Errors from broker operations.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("entity not found: {0}")]
    NotFound(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("broker unavailable: {0}")]
    Unavailable(String),
    #[error("internal error: {0}")]
    Internal(String),
}

/// Trait abstracting the message queue engine for the AMQP adapter.
///
/// All methods are async to support Raft consensus round-trips.
/// Implementations must be `Send + Sync` for use across tokio tasks.
pub trait MessageBroker: Send + Sync + 'static {
    /// Resolve an AMQP address to a broker entity ID.
    /// Returns `(entity_type, entity_id)` where entity_type is "topic" or "queue".
    fn resolve_address(
        &self,
        address: &str,
    ) -> impl std::future::Future<Output = Result<(&'static str, u64), BrokerError>> + Send;

    /// Publish a message to a topic.
    fn publish_to_topic(
        &self,
        topic_id: u64,
        message: &AmqpMessage,
    ) -> impl std::future::Future<Output = Result<PublishResult, BrokerError>> + Send;

    /// Enqueue a message to a queue.
    fn enqueue(
        &self,
        queue_id: u64,
        message: &AmqpMessage,
    ) -> impl std::future::Future<Output = Result<PublishResult, BrokerError>> + Send;

    /// Fetch up to `max_count` messages from a queue for a consumer.
    fn fetch_messages(
        &self,
        queue_id: u64,
        consumer_id: u64,
        max_count: u32,
    ) -> impl std::future::Future<Output = Result<Vec<BrokerMessage>, BrokerError>> + Send;

    /// Fetch messages from a topic starting at `offset`.
    fn fetch_topic_messages(
        &self,
        topic_id: u64,
        offset: u64,
        max_count: u32,
    ) -> impl std::future::Future<Output = Result<Vec<BrokerMessage>, BrokerError>> + Send;

    /// Settle a delivery (ack, nack, release, modify).
    fn settle(
        &self,
        queue_id: u64,
        message_ids: &SmallVec<[u64; 16]>,
        action: SettleAction,
    ) -> impl std::future::Future<Output = Result<(), BrokerError>> + Send;

    /// Register a consumer for a queue or topic.
    fn register_consumer(
        &self,
        consumer_id: u64,
        entity_type: &'static str,
        entity_id: u64,
    ) -> impl std::future::Future<Output = Result<(), BrokerError>> + Send;

    /// Disconnect a consumer.
    fn disconnect_consumer(
        &self,
        consumer_id: u64,
    ) -> impl std::future::Future<Output = Result<(), BrokerError>> + Send;

    /// Create a dynamic node (temporary queue/topic) with a lifetime policy.
    /// Returns `(entity_type, entity_id, address)` for the new node.
    fn create_dynamic_node(
        &self,
        lifetime_policy: Option<crate::types::LifetimePolicy>,
    ) -> impl std::future::Future<Output = Result<(&'static str, u64, String), BrokerError>> + Send;

    /// Destroy a dynamic node (called when lifetime policy triggers deletion).
    fn destroy_dynamic_node(
        &self,
        entity_id: u64,
    ) -> impl std::future::Future<Output = Result<(), BrokerError>> + Send;
}

// =============================================================================
// Broker Action Queue (for sync → async bridging in connection processing)
// =============================================================================

/// Actions queued by the synchronous connection processor for async execution
/// by the server event loop. This bridges the sync `AmqpConnection::process()`
/// with the async `MessageBroker` trait methods.
#[derive(Debug, Clone)]
pub enum BrokerAction {
    /// Resolve an AMQP address to a broker entity (on ATTACH).
    ResolveAddress {
        link_handle: u32,
        session_channel: u16,
        address: WireString,
    },
    /// Register a consumer for a sender-role link (on ATTACH).
    RegisterConsumer {
        link_handle: u32,
        session_channel: u16,
        consumer_id: u64,
        entity_type: &'static str,
        entity_id: u64,
    },
    /// Publish a message to a topic (on receiver-role Transfer).
    Publish {
        entity_id: u64,
        payload: Bytes,
        delivery_id: u32,
        link_handle: u32,
        session_channel: u16,
    },
    /// Enqueue a message to a queue (on receiver-role Transfer).
    Enqueue {
        entity_id: u64,
        payload: Bytes,
        delivery_id: u32,
        link_handle: u32,
        session_channel: u16,
    },
    /// Settle deliveries with the broker (on Disposition).
    Settle {
        entity_id: u64,
        message_ids: SmallVec<[u64; 16]>,
        action: SettleAction,
    },
    /// Fetch messages for a sender-role link (on FLOW with credit).
    FetchMessages {
        link_handle: u32,
        session_channel: u16,
        entity_type: &'static str,
        entity_id: u64,
        consumer_id: u64,
        max_count: u32,
    },
    /// Disconnect a consumer (on DETACH).
    DisconnectConsumer { consumer_id: u64 },
    /// Commit a transaction — apply all buffered work (Gap X1).
    CommitTransaction {
        txn_id: Bytes,
        session_channel: u16,
        coordinator_handle: u32,
    },
    /// Rollback a transaction — discard all buffered work (Gap X1).
    RollbackTransaction {
        txn_id: Bytes,
        session_channel: u16,
        coordinator_handle: u32,
    },
    /// Create a dynamic node (on ATTACH with dynamic=true) (Gap M5).
    CreateDynamicNode {
        link_handle: u32,
        session_channel: u16,
        lifetime_policy: Option<crate::types::LifetimePolicy>,
    },
    /// Destroy a dynamic node (on link close for delete-on-close policy) (Gap M5).
    DestroyDynamicNode { entity_id: u64 },
}

/// A no-op broker for protocol testing. Accepts all messages, returns empty results.
#[derive(Debug, Clone, Default)]
pub struct NoopBroker;

impl MessageBroker for NoopBroker {
    async fn resolve_address(&self, address: &str) -> Result<(&'static str, u64), BrokerError> {
        // Auto-resolve: "topic/X" → topic, "queue/X" → queue, else → queue
        if address.starts_with("topic/") {
            Ok(("topic", 1))
        } else {
            Ok(("queue", 1))
        }
    }

    async fn publish_to_topic(
        &self,
        _topic_id: u64,
        _message: &AmqpMessage,
    ) -> Result<PublishResult, BrokerError> {
        Ok(PublishResult { offset: 0 })
    }

    async fn enqueue(
        &self,
        _queue_id: u64,
        _message: &AmqpMessage,
    ) -> Result<PublishResult, BrokerError> {
        Ok(PublishResult { offset: 0 })
    }

    async fn fetch_messages(
        &self,
        _queue_id: u64,
        _consumer_id: u64,
        _max_count: u32,
    ) -> Result<Vec<BrokerMessage>, BrokerError> {
        Ok(Vec::new())
    }

    async fn fetch_topic_messages(
        &self,
        _topic_id: u64,
        _offset: u64,
        _max_count: u32,
    ) -> Result<Vec<BrokerMessage>, BrokerError> {
        Ok(Vec::new())
    }

    async fn settle(
        &self,
        _queue_id: u64,
        _message_ids: &SmallVec<[u64; 16]>,
        _action: SettleAction,
    ) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn register_consumer(
        &self,
        _consumer_id: u64,
        _entity_type: &'static str,
        _entity_id: u64,
    ) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn disconnect_consumer(&self, _consumer_id: u64) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn create_dynamic_node(
        &self,
        _lifetime_policy: Option<crate::types::LifetimePolicy>,
    ) -> Result<(&'static str, u64, String), BrokerError> {
        // Auto-create a queue with a generated name
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(("queue", id, format!("dynamic-{id}")))
    }

    async fn destroy_dynamic_node(&self, _entity_id: u64) -> Result<(), BrokerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TransactionalState;

    #[tokio::test]
    async fn test_noop_broker_resolve() {
        let broker = NoopBroker;
        let (ty, id) = broker.resolve_address("topic/events").await.unwrap();
        assert_eq!(ty, "topic");
        assert_eq!(id, 1);

        let (ty, _) = broker.resolve_address("queue/tasks").await.unwrap();
        assert_eq!(ty, "queue");

        let (ty, _) = broker.resolve_address("bare-name").await.unwrap();
        assert_eq!(ty, "queue");
    }

    #[tokio::test]
    async fn test_noop_broker_publish() {
        let broker = NoopBroker;
        let msg = AmqpMessage::default();
        let result = broker.publish_to_topic(1, &msg).await.unwrap();
        assert_eq!(result.offset, 0);
    }

    #[test]
    fn test_settle_action_from_delivery_state() {
        assert_eq!(
            SettleAction::from_delivery_state(&DeliveryState::Accepted),
            SettleAction::Accept
        );
        assert_eq!(
            SettleAction::from_delivery_state(&DeliveryState::Released),
            SettleAction::Release
        );
        assert_eq!(
            SettleAction::from_delivery_state(&DeliveryState::Rejected { error: None }),
            SettleAction::Reject
        );
    }

    #[test]
    fn test_settle_action_from_transactional_state() {
        let ts = DeliveryState::Transactional(TransactionalState {
            txn_id: Bytes::from_static(b"txn-1"),
            outcome: Some(Outcome::Accepted),
        });
        assert_eq!(SettleAction::from_delivery_state(&ts), SettleAction::Accept);

        let ts_no_outcome = DeliveryState::Transactional(TransactionalState {
            txn_id: Bytes::from_static(b"txn-2"),
            outcome: None,
        });
        assert_eq!(
            SettleAction::from_delivery_state(&ts_no_outcome),
            SettleAction::Accept
        );
    }

    #[tokio::test]
    async fn test_noop_broker_enqueue() {
        let broker = NoopBroker;
        let msg = AmqpMessage::default();
        let result = broker.enqueue(1, &msg).await.unwrap();
        assert_eq!(result.offset, 0);
    }

    #[tokio::test]
    async fn test_noop_broker_fetch_messages() {
        let broker = NoopBroker;
        let msgs = broker.fetch_messages(1, 1, 10).await.unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn test_noop_broker_fetch_topic_messages() {
        let broker = NoopBroker;
        let msgs = broker.fetch_topic_messages(1, 0, 10).await.unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn test_noop_broker_settle() {
        let broker = NoopBroker;
        let ids = SmallVec::from_vec(vec![1, 2, 3]);
        let result = broker.settle(1, &ids, SettleAction::Accept).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_broker_register_consumer() {
        let broker = NoopBroker;
        let result = broker.register_consumer(1, "queue", 1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_broker_disconnect_consumer() {
        let broker = NoopBroker;
        let result = broker.disconnect_consumer(1).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_broker_message_struct() {
        let msg = BrokerMessage {
            message_id: 42,
            payload: Bytes::from_static(b"hello"),
            attempt: 3,
        };
        assert_eq!(msg.message_id, 42);
        assert_eq!(msg.payload, Bytes::from_static(b"hello"));
        assert_eq!(msg.attempt, 3);
    }

    #[test]
    fn test_publish_result_struct() {
        let result = PublishResult { offset: 99 };
        assert_eq!(result.offset, 99);
        let cloned = result.clone();
        assert_eq!(cloned.offset, 99);
    }

    #[test]
    fn test_broker_error_display() {
        let e = BrokerError::NotFound("queue-1".into());
        assert_eq!(e.to_string(), "entity not found: queue-1");

        let e = BrokerError::PermissionDenied("no access".into());
        assert_eq!(e.to_string(), "permission denied: no access");

        let e = BrokerError::Unavailable("shutting down".into());
        assert_eq!(e.to_string(), "broker unavailable: shutting down");

        let e = BrokerError::Internal("oops".into());
        assert_eq!(e.to_string(), "internal error: oops");
    }

    #[test]
    fn test_settle_action_from_modified_and_received() {
        let modified = DeliveryState::Modified {
            delivery_failed: true,
            undeliverable_here: false,
            message_annotations: None,
        };
        assert_eq!(
            SettleAction::from_delivery_state(&modified),
            SettleAction::Modify
        );

        let received = DeliveryState::Received {
            section_number: 0,
            section_offset: 0,
        };
        assert_eq!(
            SettleAction::from_delivery_state(&received),
            SettleAction::Accept
        );
    }

    #[test]
    fn test_settle_action_from_transactional_rejected_released_modified() {
        let ts_rejected = DeliveryState::Transactional(TransactionalState {
            txn_id: Bytes::from_static(b"txn"),
            outcome: Some(Outcome::Rejected { error: None }),
        });
        assert_eq!(
            SettleAction::from_delivery_state(&ts_rejected),
            SettleAction::Reject
        );

        let ts_released = DeliveryState::Transactional(TransactionalState {
            txn_id: Bytes::from_static(b"txn"),
            outcome: Some(Outcome::Released),
        });
        assert_eq!(
            SettleAction::from_delivery_state(&ts_released),
            SettleAction::Release
        );

        let ts_modified = DeliveryState::Transactional(TransactionalState {
            txn_id: Bytes::from_static(b"txn"),
            outcome: Some(Outcome::Modified {
                delivery_failed: true,
                undeliverable_here: false,
                message_annotations: None,
            }),
        });
        assert_eq!(
            SettleAction::from_delivery_state(&ts_modified),
            SettleAction::Modify
        );
    }

    #[tokio::test]
    async fn test_noop_broker_resolve_edge_cases() {
        let broker = NoopBroker;

        // Empty string -> defaults to queue
        let (ty, _) = broker.resolve_address("").await.unwrap();
        assert_eq!(ty, "queue");

        // "topic/" with no name -> still starts with "topic/"
        let (ty, _) = broker.resolve_address("topic/").await.unwrap();
        assert_eq!(ty, "topic");

        // Just "/" -> defaults to queue
        let (ty, _) = broker.resolve_address("/").await.unwrap();
        assert_eq!(ty, "queue");
    }

    #[test]
    fn test_settle_action_from_declared() {
        let declared = DeliveryState::Declared {
            txn_id: Bytes::from_static(b"txn-1"),
        };
        assert_eq!(
            SettleAction::from_delivery_state(&declared),
            SettleAction::Accept
        );
    }

    #[tokio::test]
    async fn test_noop_broker_create_dynamic_node() {
        let broker = NoopBroker;
        let (ty, id, addr) = broker.create_dynamic_node(None).await.unwrap();
        assert_eq!(ty, "queue");
        assert!(id > 0);
        assert!(addr.starts_with("dynamic-"));
    }

    #[tokio::test]
    async fn test_noop_broker_destroy_dynamic_node() {
        let broker = NoopBroker;
        let result = broker.destroy_dynamic_node(1).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_broker_action_clone() {
        let action = BrokerAction::Settle {
            entity_id: 1,
            message_ids: SmallVec::from_vec(vec![1, 2, 3]),
            action: SettleAction::Accept,
        };
        let cloned = action.clone();
        match cloned {
            BrokerAction::Settle {
                entity_id,
                message_ids,
                action,
            } => {
                assert_eq!(entity_id, 1);
                assert_eq!(message_ids.len(), 3);
                assert_eq!(action, SettleAction::Accept);
            }
            _ => panic!("expected Settle"),
        }
    }
}
