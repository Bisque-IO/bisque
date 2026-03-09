use bisque_mq::write_batcher::MqBatcherError;
use bisque_mq_protocol::error::ProtocolError;

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("batcher error: {0}")]
    Batcher(#[from] MqBatcherError),
    #[error("handshake failed: {0}")]
    HandshakeFailed(String),
    #[error("subscription not found: {0}")]
    SubscriptionNotFound(u32),
    #[error("session closed")]
    SessionClosed,
    #[error("heartbeat timeout")]
    HeartbeatTimeout,
    #[error("send failed")]
    SendFailed,
}
