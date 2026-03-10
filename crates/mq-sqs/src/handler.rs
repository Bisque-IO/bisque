use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use base64::Engine as _;
use bisque_mq::config::QueueConfig;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::{DeliveredMessage, MqCommand, MqError, MqResponse, name_hash};
use bisque_mq::write_batcher::MqWriteBatcher;
use bytes::Bytes;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::error::SqsError;
use crate::types::*;

/// Shared state for the SQS handler.
pub struct SqsState {
    /// The write batcher for submitting commands.
    batcher: Arc<MqWriteBatcher>,
    /// Log reader for fetching message payloads from raft log segments.
    log_reader: bisque_raft::SegmentPrefetcher,
    /// Base URL for queue URL construction.
    base_url: String,
    /// Default group ID for queue operations.
    group_id: u64,
    /// Pre-initialized metrics handles.
    m_send_count: metrics::Counter,
    m_receive_count: metrics::Counter,
    m_delete_count: metrics::Counter,
}

impl SqsState {
    pub fn new(
        batcher: Arc<MqWriteBatcher>,
        log_reader: bisque_raft::SegmentPrefetcher,
        base_url: String,
        group_id: u64,
    ) -> Self {
        let labels = [("group", group_id.to_string())];
        let m_send_count = metrics::counter!("sqs.send_message.count", &labels);
        let m_receive_count = metrics::counter!("sqs.receive_message.count", &labels);
        let m_delete_count = metrics::counter!("sqs.delete_message.count", &labels);
        Self {
            batcher,
            log_reader,
            base_url,
            group_id,
            m_send_count,
            m_receive_count,
            m_delete_count,
        }
    }

    fn queue_url(&self, queue_name: &str) -> String {
        format!("{}/sqs/{}/{}", self.base_url, self.group_id, queue_name)
    }

    fn parse_queue_url(&self, queue_url: &str) -> Result<(u64, String), SqsError> {
        // Format: {base}/sqs/{group_id}/{queue_name}
        let parts: Vec<&str> = queue_url.rsplitn(3, '/').collect();
        if parts.len() < 2 {
            return Err(SqsError::InvalidParameter(format!(
                "invalid queue URL: {}",
                queue_url
            )));
        }
        let queue_name = parts[0].to_string();
        let group_id = parts
            .get(1)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(self.group_id);
        Ok((group_id, queue_name))
    }

    fn resolve_queue(&self, queue_url: &str) -> Result<(u64, u64), SqsError> {
        let (_group_id, queue_name) = self.parse_queue_url(queue_url)?;
        let hash = name_hash(&queue_name);
        // We encode queue_id in the receipt handle, but for URL resolution
        // we need to look up by name. Submit a GetQueueAttributes to verify existence.
        // For now, use the name hash as a proxy — the actual resolution happens
        // when the command is applied by the engine.
        Ok((self.group_id, hash))
    }

    fn encode_receipt_handle(queue_id: u64, message_id: u64) -> String {
        let data = format!("{}:{}", queue_id, message_id);
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data.as_bytes())
    }

    fn decode_receipt_handle(handle: &str) -> Result<(u64, u64), SqsError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(handle)
            .map_err(|_| SqsError::InvalidReceiptHandle)?;
        let s = String::from_utf8(bytes).map_err(|_| SqsError::InvalidReceiptHandle)?;
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(SqsError::InvalidReceiptHandle);
        }
        let queue_id = parts[0]
            .parse::<u64>()
            .map_err(|_| SqsError::InvalidReceiptHandle)?;
        let message_id = parts[1]
            .parse::<u64>()
            .map_err(|_| SqsError::InvalidReceiptHandle)?;
        Ok((queue_id, message_id))
    }

    fn md5_hex(data: &[u8]) -> String {
        // Simple placeholder — SQS returns MD5 of message body.
        // Use a fast hash for now since we don't have md5 dep.
        format!("{:016x}{:016x}", data.len() as u64, crc(data))
    }

    // =========================================================================
    // Action handlers
    // =========================================================================

    pub async fn create_queue(
        &self,
        req: CreateQueueRequest,
    ) -> Result<Json<CreateQueueResponse>, SqsError> {
        let mut config = QueueConfig::default();

        if let Some(ref attrs) = req.attributes {
            if let Some(ref vt) = attrs.visibility_timeout {
                if let Ok(secs) = vt.parse::<u64>() {
                    config.visibility_timeout_ms = secs * 1000;
                }
            }
            if let Some(ref ds) = attrs.delay_seconds {
                if let Ok(secs) = ds.parse::<u64>() {
                    config.delay_default_ms = secs * 1000;
                }
            }
            if let Some(ref fifo) = attrs.fifo_queue {
                if fifo == "true" {
                    config.dedup_window_secs = Some(300); // 5 min default
                }
            }
            if let Some(ref policy) = attrs.redrive_policy {
                if let Ok(rp) = serde_json::from_str::<RedrivePolicy>(policy) {
                    // TODO: Resolve DLQ ARN to topic_id and set config.dead_letter_topic_id
                    config.max_retries = rp.max_receive_count;
                }
            }
        }

        let cmd = MqCommand::create_queue(&req.queue_name, &config);

        let resp = self.batcher.submit(cmd).await?;
        match resp {
            MqResponse::EntityCreated { id: _ } => Ok(Json(CreateQueueResponse {
                queue_url: self.queue_url(&req.queue_name),
            })),
            MqResponse::Error(MqError::AlreadyExists { .. }) => {
                // SQS returns the existing queue URL if attributes match
                Ok(Json(CreateQueueResponse {
                    queue_url: self.queue_url(&req.queue_name),
                }))
            }
            MqResponse::Error(e) => Err(SqsError::Internal(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn delete_queue(&self, req: DeleteQueueRequest) -> Result<Json<()>, SqsError> {
        let (_group_id, queue_name) = self.parse_queue_url(&req.queue_url)?;
        let hash = name_hash(&queue_name);
        // We need to resolve name → ID. For now, assume the caller provides the right URL.
        // In production, this would go through MqRouter::resolve_entity.
        let cmd = MqCommand::delete_queue(hash);
        let resp = self.batcher.submit(cmd).await?;
        match resp {
            MqResponse::Ok => Ok(Json(())),
            MqResponse::Error(e) => Err(SqsError::QueueNotFound(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn get_queue_url(
        &self,
        req: GetQueueUrlRequest,
    ) -> Result<Json<GetQueueUrlResponse>, SqsError> {
        // Verify queue exists by getting attributes
        let hash = name_hash(&req.queue_name);
        let cmd = MqCommand::get_queue_attributes(hash);
        let resp = self.batcher.submit(cmd).await?;
        match resp {
            MqResponse::Stats(_) => Ok(Json(GetQueueUrlResponse {
                queue_url: self.queue_url(&req.queue_name),
            })),
            MqResponse::Error(_) => Err(SqsError::QueueNotFound(req.queue_name)),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn send_message(
        &self,
        req: SendMessageRequest,
    ) -> Result<Json<SendMessageResponse>, SqsError> {
        let (_group_id, queue_name) = self.parse_queue_url(&req.queue_url)?;
        let queue_id = name_hash(&queue_name);

        let value = Bytes::from(req.message_body.clone());
        let md5 = Self::md5_hex(&value);

        let key = req.message_group_id.map(|s| Bytes::from(s.into_bytes()));
        let dedup_key = req
            .message_deduplication_id
            .map(|s| Bytes::from(s.into_bytes()));

        let mut headers = Vec::new();
        if let Some(attrs) = req.message_attributes {
            for (k, v) in attrs {
                if let Some(sv) = v.get("StringValue").and_then(|v| v.as_str()) {
                    headers.push((k, Bytes::from(sv.to_string().into_bytes())));
                } else if let Some(bv) = v.get("BinaryValue").and_then(|v| v.as_str()) {
                    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(bv) {
                        headers.push((k, Bytes::from(decoded)));
                    }
                }
            }
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut builder = FlatMessageBuilder::new(value).timestamp(now_ms);
        if let Some(k) = key {
            builder = builder.key(k);
        }
        for (hk, hv) in headers {
            builder = builder.header(Bytes::from(hk), hv);
        }
        let flat_msg = builder.build();

        let cmd = MqCommand::enqueue(queue_id, &[flat_msg], &[dedup_key]);

        let resp = self.batcher.submit(cmd).await?;
        self.m_send_count.increment(1);

        match resp {
            MqResponse::Published { offsets } => {
                let msg_id = offsets.first().copied().unwrap_or(0);
                Ok(Json(SendMessageResponse {
                    message_id: msg_id.to_string(),
                    md5_of_message_body: md5,
                    sequence_number: Some(msg_id.to_string()),
                }))
            }
            MqResponse::Error(e) => Err(SqsError::Internal(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn send_message_batch(
        &self,
        req: SendMessageBatchRequest,
    ) -> Result<Json<SendMessageBatchResponse>, SqsError> {
        let (_group_id, queue_name) = self.parse_queue_url(&req.queue_url)?;
        let queue_id = name_hash(&queue_name);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut messages = Vec::with_capacity(req.entries.len());
        let mut dedup_keys = Vec::with_capacity(req.entries.len());
        let mut entry_ids = Vec::with_capacity(req.entries.len());
        let mut md5s = Vec::with_capacity(req.entries.len());

        for entry in &req.entries {
            let value = Bytes::from(entry.message_body.clone().into_bytes());
            md5s.push(Self::md5_hex(&value));
            let key = entry
                .message_group_id
                .as_ref()
                .map(|s| Bytes::from(s.clone().into_bytes()));
            let dedup = entry
                .message_deduplication_id
                .as_ref()
                .map(|s| Bytes::from(s.clone().into_bytes()));

            {
                let mut builder = FlatMessageBuilder::new(value).timestamp(now_ms);
                if let Some(k) = key {
                    builder = builder.key(k);
                }
                messages.push(builder.build());
            }
            dedup_keys.push(dedup);
            entry_ids.push(entry.id.clone());
        }

        let cmd = MqCommand::enqueue(queue_id, &messages, &dedup_keys);

        let resp = self.batcher.submit(cmd).await?;
        self.m_send_count.increment(entry_ids.len() as u64);

        match resp {
            MqResponse::Published { offsets } => {
                let successful = entry_ids
                    .into_iter()
                    .zip(offsets.iter())
                    .zip(md5s.iter())
                    .map(|((id, &offset), md5)| SendMessageBatchResultEntry {
                        id,
                        message_id: offset.to_string(),
                        md5_of_message_body: md5.clone(),
                    })
                    .collect();

                Ok(Json(SendMessageBatchResponse {
                    successful,
                    failed: Vec::new(),
                }))
            }
            MqResponse::Error(e) => Err(SqsError::Internal(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn receive_message(
        &self,
        req: ReceiveMessageRequest,
    ) -> Result<Json<ReceiveMessageResponse>, SqsError> {
        let (_group_id, queue_name) = self.parse_queue_url(&req.queue_url)?;
        let queue_id = name_hash(&queue_name);
        let max_count = req.max_number_of_messages.unwrap_or(1).min(10);
        let wait_time = req.wait_time_seconds.unwrap_or(0);

        // Generate a consumer ID for this receive operation.
        // In a real implementation, this would be tied to a persistent consumer session.
        let consumer_id = rand_consumer_id();

        let cmd = MqCommand::deliver(queue_id, consumer_id, max_count);

        // Long-poll: retry up to wait_time seconds
        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_time as u64);

        loop {
            let resp = self.batcher.submit(cmd.clone()).await?;

            match resp {
                MqResponse::Messages { messages } if !messages.is_empty() => {
                    self.m_receive_count.increment(messages.len() as u64);
                    let sqs_messages = messages
                        .into_iter()
                        .map(|m| to_sqs_message(queue_id, m, &self.log_reader))
                        .collect();
                    return Ok(Json(ReceiveMessageResponse {
                        messages: sqs_messages,
                    }));
                }
                MqResponse::Messages { .. } => {
                    // No messages — check if we should keep waiting
                    if wait_time == 0 || tokio::time::Instant::now() >= deadline {
                        return Ok(Json(ReceiveMessageResponse {
                            messages: Vec::new(),
                        }));
                    }
                    // Brief sleep before retrying
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                MqResponse::Error(e) => return Err(SqsError::Internal(e.to_string())),
                _ => return Err(SqsError::Internal("unexpected response".into())),
            }
        }
    }

    pub async fn delete_message(&self, req: DeleteMessageRequest) -> Result<Json<()>, SqsError> {
        let (queue_id, message_id) = Self::decode_receipt_handle(&req.receipt_handle)?;

        let cmd = MqCommand::ack(queue_id, &[message_id], None);

        let resp = self.batcher.submit(cmd).await?;
        self.m_delete_count.increment(1);

        match resp {
            MqResponse::Ok => Ok(Json(())),
            MqResponse::Error(e) => Err(SqsError::Internal(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn delete_message_batch(
        &self,
        req: DeleteMessageBatchRequest,
    ) -> Result<Json<DeleteMessageBatchResponse>, SqsError> {
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        // Group valid entries by queue_id to batch into a single Ack per queue.
        let mut by_queue: HashMap<u64, Vec<(String, u64)>> = HashMap::new();

        for entry in &req.entries {
            match Self::decode_receipt_handle(&entry.receipt_handle) {
                Ok((queue_id, message_id)) => {
                    by_queue
                        .entry(queue_id)
                        .or_default()
                        .push((entry.id.clone(), message_id));
                }
                Err(_) => {
                    failed.push(BatchResultErrorEntry {
                        id: entry.id.clone(),
                        code: "ReceiptHandleIsInvalid".to_string(),
                        message: "invalid receipt handle".to_string(),
                        sender_fault: true,
                    });
                }
            }
        }

        // Single Ack command per queue with all message_ids.
        for (queue_id, entries) in by_queue {
            let (ids, message_ids): (Vec<String>, Vec<u64>) = entries.into_iter().unzip();
            let cmd = MqCommand::ack(queue_id, &message_ids, None);
            match self.batcher.submit(cmd).await {
                Ok(MqResponse::Ok) => {
                    for id in ids {
                        successful.push(DeleteMessageBatchResultEntry { id });
                    }
                }
                Ok(MqResponse::Error(e)) => {
                    let msg = e.to_string();
                    for id in ids {
                        failed.push(BatchResultErrorEntry {
                            id,
                            code: "InternalError".to_string(),
                            message: msg.clone(),
                            sender_fault: false,
                        });
                    }
                }
                _ => {
                    for id in ids {
                        failed.push(BatchResultErrorEntry {
                            id,
                            code: "InternalError".to_string(),
                            message: "unexpected response".to_string(),
                            sender_fault: false,
                        });
                    }
                }
            }
        }

        self.m_delete_count.increment(successful.len() as u64);
        Ok(Json(DeleteMessageBatchResponse { successful, failed }))
    }

    pub async fn change_message_visibility(
        &self,
        req: ChangeMessageVisibilityRequest,
    ) -> Result<Json<()>, SqsError> {
        let (queue_id, message_id) = Self::decode_receipt_handle(&req.receipt_handle)?;

        let cmd = MqCommand::extend_visibility(
            queue_id,
            &[message_id],
            req.visibility_timeout as u64 * 1000,
        );

        let resp = self.batcher.submit(cmd).await?;
        match resp {
            MqResponse::Ok => Ok(Json(())),
            MqResponse::Error(e) => Err(SqsError::Internal(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn change_message_visibility_batch(
        &self,
        req: ChangeMessageVisibilityBatchRequest,
    ) -> Result<Json<ChangeMessageVisibilityBatchResponse>, SqsError> {
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        // Group by (queue_id, extension_ms) to batch into single ExtendVisibility commands.
        let mut by_key: HashMap<(u64, u64), Vec<(String, u64)>> = HashMap::new();

        for entry in &req.entries {
            match Self::decode_receipt_handle(&entry.receipt_handle) {
                Ok((queue_id, message_id)) => {
                    let extension_ms = entry.visibility_timeout as u64 * 1000;
                    by_key
                        .entry((queue_id, extension_ms))
                        .or_default()
                        .push((entry.id.clone(), message_id));
                }
                Err(_) => {
                    failed.push(BatchResultErrorEntry {
                        id: entry.id.clone(),
                        code: "ReceiptHandleIsInvalid".to_string(),
                        message: "invalid receipt handle".to_string(),
                        sender_fault: true,
                    });
                }
            }
        }

        for ((queue_id, extension_ms), entries) in by_key {
            let (ids, message_ids): (Vec<String>, Vec<u64>) = entries.into_iter().unzip();
            let cmd = MqCommand::extend_visibility(queue_id, &message_ids, extension_ms);
            match self.batcher.submit(cmd).await {
                Ok(MqResponse::Ok) => {
                    for id in ids {
                        successful.push(ChangeMessageVisibilityBatchResultEntry { id });
                    }
                }
                _ => {
                    for id in ids {
                        failed.push(BatchResultErrorEntry {
                            id,
                            code: "InternalError".to_string(),
                            message: "failed to change visibility".to_string(),
                            sender_fault: false,
                        });
                    }
                }
            }
        }

        Ok(Json(ChangeMessageVisibilityBatchResponse {
            successful,
            failed,
        }))
    }

    pub async fn purge_queue(&self, req: PurgeQueueRequest) -> Result<Json<()>, SqsError> {
        let (_group_id, queue_name) = self.parse_queue_url(&req.queue_url)?;
        let queue_id = name_hash(&queue_name);

        let cmd = MqCommand::purge_queue(queue_id);
        let resp = self.batcher.submit(cmd).await?;
        match resp {
            MqResponse::Ok => Ok(Json(())),
            MqResponse::Error(e) => Err(SqsError::QueueNotFound(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }

    pub async fn get_queue_attributes(
        &self,
        req: GetQueueAttributesRequest,
    ) -> Result<Json<GetQueueAttributesResponse>, SqsError> {
        let (_group_id, queue_name) = self.parse_queue_url(&req.queue_url)?;
        let queue_id = name_hash(&queue_name);

        let cmd = MqCommand::get_queue_attributes(queue_id);
        let resp = self.batcher.submit(cmd).await?;

        match resp {
            MqResponse::Stats(bisque_mq::types::EntityStats::Queue {
                queue_id: _,
                pending_count,
                in_flight_count,
                dlq_count,
            }) => {
                let mut attrs = serde_json::Map::new();
                attrs.insert(
                    "ApproximateNumberOfMessages".to_string(),
                    serde_json::Value::String(pending_count.to_string()),
                );
                attrs.insert(
                    "ApproximateNumberOfMessagesNotVisible".to_string(),
                    serde_json::Value::String(in_flight_count.to_string()),
                );
                attrs.insert(
                    "ApproximateNumberOfMessagesDelayed".to_string(),
                    serde_json::Value::String("0".to_string()),
                );
                attrs.insert(
                    "ApproximateNumberOfMessagesDead".to_string(),
                    serde_json::Value::String(dlq_count.to_string()),
                );
                Ok(Json(GetQueueAttributesResponse { attributes: attrs }))
            }
            MqResponse::Error(e) => Err(SqsError::QueueNotFound(e.to_string())),
            _ => Err(SqsError::Internal("unexpected response".into())),
        }
    }
}

// =============================================================================
// Helpers
// =============================================================================

#[derive(Deserialize)]
struct RedrivePolicy {
    #[serde(rename = "maxReceiveCount")]
    max_receive_count: u32,
    #[serde(rename = "deadLetterTargetArn", default)]
    _dead_letter_target_arn: Option<String>,
}

fn to_sqs_message(
    queue_id: u64,
    msg: DeliveredMessage,
    prefetcher: &bisque_raft::SegmentPrefetcher,
) -> SqsMessage {
    let body = if let Some(flat_bytes) = bisque_mq::read_message_at(prefetcher, msg.message_id) {
        if let Some(flat) = bisque_mq::flat::FlatMessage::new(flat_bytes) {
            String::from_utf8_lossy(&flat.value()).into_owned()
        } else {
            String::new()
        }
    } else {
        String::new()
    };
    let md5 = SqsState::md5_hex(body.as_bytes());
    let receipt_handle = SqsState::encode_receipt_handle(queue_id, msg.message_id);

    SqsMessage {
        message_id: msg.message_id.to_string(),
        receipt_handle,
        body,
        md5_of_body: md5,
        attributes: None,
    }
}

fn rand_consumer_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1_000_000);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn crc(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_receipt_handle_roundtrip() {
        let handle = SqsState::encode_receipt_handle(42, 100);
        let (qid, mid) = SqsState::decode_receipt_handle(&handle).unwrap();
        assert_eq!(qid, 42);
        assert_eq!(mid, 100);
    }

    #[test]
    fn test_invalid_receipt_handle() {
        assert!(SqsState::decode_receipt_handle("invalid").is_err());
        assert!(SqsState::decode_receipt_handle("").is_err());
    }

    #[test]
    fn test_md5_hex() {
        let result = SqsState::md5_hex(b"hello");
        assert_eq!(result.len(), 32);
    }
}
