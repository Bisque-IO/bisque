use bytes::BytesMut;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use crate::error::SqsError;
use crate::types::*;
use axum::Json;
use base64::Engine as _;
use bisque_mq::async_apply::ResponseEntry;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::{DeliveredMessage, MqCommand, name_hash};
use bisque_mq::write_batcher::LocalSubmitter;

/// Shared state for the SQS handler.
pub struct SqsState {
    /// The write batcher for submitting commands.
    batcher: Arc<LocalSubmitter>,
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

/// Receipt handle is 16 raw bytes: [queue_id:u64 LE][message_id:u64 LE], then base64-encoded.
const RECEIPT_HANDLE_RAW_LEN: usize = 16;

impl SqsState {
    pub fn new(
        batcher: Arc<LocalSubmitter>,
        log_reader: bisque_raft::SegmentPrefetcher,
        base_url: String,
        group_id: u64,
        catalog_name: &str,
    ) -> Self {
        let labels = [
            ("catalog", catalog_name.to_owned()),
            ("group", group_id.to_string()),
        ];
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

    /// Returns (&str slice into the URL) — zero allocation.
    fn parse_queue_name<'a>(&self, queue_url: &'a str) -> Result<&'a str, SqsError> {
        // Format: {base}/sqs/{group_id}/{queue_name}
        queue_url
            .rsplit('/')
            .next()
            .ok_or_else(|| SqsError::InvalidParameter(format!("invalid queue URL: {}", queue_url)))
    }

    /// Binary receipt handle: 16 raw bytes [queue_id LE | message_id LE] → base64url.
    fn encode_receipt_handle(queue_id: u64, message_id: u64) -> String {
        let mut buf = [0u8; RECEIPT_HANDLE_RAW_LEN];
        buf[..8].copy_from_slice(&queue_id.to_le_bytes());
        buf[8..].copy_from_slice(&message_id.to_le_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
    }

    /// Decode binary receipt handle back to (queue_id, message_id).
    /// Uses stack buffer — zero heap allocation.
    fn decode_receipt_handle(handle: &str) -> Result<(u64, u64), SqsError> {
        let mut buf = BytesMut::new();
        let mut buf = [0u8; RECEIPT_HANDLE_RAW_LEN];
        let len = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode_slice(handle, &mut buf)
            .map_err(|_| SqsError::InvalidReceiptHandle)?;
        if len != RECEIPT_HANDLE_RAW_LEN {
            return Err(SqsError::InvalidReceiptHandle);
        }
        let queue_id = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let message_id = u64::from_le_bytes(buf[8..].try_into().unwrap());
        Ok((queue_id, message_id))
    }

    /// Stack-based hex encoding — single allocation with exact capacity.
    fn md5_hex(data: &[u8]) -> String {
        let mut buf = BytesMut::new();
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let len_val = data.len() as u64;
        let crc_val = crc(data);
        let mut buf = String::with_capacity(32);
        for &byte in len_val
            .to_be_bytes()
            .iter()
            .chain(crc_val.to_be_bytes().iter())
        {
            buf.push(HEX[(byte >> 4) as usize] as char);
            buf.push(HEX[(byte & 0xf) as usize] as char);
        }
        buf
    }

    // =========================================================================
    // Action handlers
    // =========================================================================

    pub async fn create_queue(
        &self,
        req: CreateQueueRequest,
    ) -> Result<Json<CreateQueueResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        // TODO: Map SQS-specific attributes (visibility_timeout, delay, dedup, redrive)
        // to engine-level AckVariantConfig once the full integration is wired up.
        let cmd = MqCommand::create_consumer_group(&mut buf, &req.queue_name, 0);

        let resp = self.batcher.submit(cmd).await?;
        if resp.tag() == ResponseEntry::TAG_ENTITY_CREATED || resp.is_already_exists() {
            Ok(Json(CreateQueueResponse {
                queue_url: self.queue_url(&req.queue_name),
            }))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::Internal(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn delete_queue(&self, req: DeleteQueueRequest) -> Result<Json<()>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let queue_name = self.parse_queue_name(&req.queue_url)?;
        let hash = name_hash(queue_name);
        let cmd = MqCommand::delete_consumer_group(&mut buf, hash);
        let resp = self.batcher.submit(cmd).await?;
        if resp.is_ok() {
            Ok(Json(()))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::QueueNotFound(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn get_queue_url(
        &self,
        req: GetQueueUrlRequest,
    ) -> Result<Json<GetQueueUrlResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        // Verify queue exists by getting attributes
        let hash = name_hash(&req.queue_name);
        let cmd = MqCommand::group_get_attributes(&mut buf, hash);
        let resp = self.batcher.submit(cmd).await?;
        if resp.tag() == ResponseEntry::TAG_STATS {
            Ok(Json(GetQueueUrlResponse {
                queue_url: self.queue_url(&req.queue_name),
            }))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::QueueNotFound(req.queue_name))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn send_message(
        &self,
        req: SendMessageRequest,
    ) -> Result<Json<SendMessageResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let queue_name = self.parse_queue_name(&req.queue_url)?;
        let queue_id = name_hash(queue_name);

        // Compute MD5 from borrowed bytes, then take ownership — zero-copy.
        let md5 = Self::md5_hex(req.message_body.as_bytes());
        let value = req.message_body.into_bytes();

        // Take ownership directly — no clone.
        let key = req.message_group_id.map(|s| s.into_bytes());
        let mut headers: Vec<(String, Vec<u8>)> = Vec::new();
        if let Some(attrs) = req.message_attributes {
            for (k, v) in attrs {
                if let Some(sv) = v.get("StringValue").and_then(|v| v.as_str()) {
                    headers.push((k, sv.as_bytes().to_vec()));
                } else if let Some(bv) = v.get("BinaryValue").and_then(|v| v.as_str()) {
                    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(bv) {
                        headers.push((k, decoded));
                    }
                }
            }
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut builder = FlatMessageBuilder::new(&value).timestamp(now_ms);
        if let Some(ref k) = key {
            builder = builder.key(k);
        }
        for (hk, hv) in &headers {
            builder = builder.header(hk.as_bytes(), &hv[..]);
        }
        let flat_msg = builder.build();

        let cmd = MqCommand::publish(&mut buf, queue_id, &[flat_msg]);

        let resp = self.batcher.submit(cmd).await?;
        self.m_send_count.increment(1);

        if resp.tag() == ResponseEntry::TAG_PUBLISHED {
            let base_offset = resp.base_offset();
            Ok(Json(SendMessageResponse {
                message_id: base_offset.to_string(),
                md5_of_message_body: md5,
                sequence_number: Some(base_offset.to_string()),
            }))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::Internal(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn send_message_batch(
        &self,
        req: SendMessageBatchRequest,
    ) -> Result<Json<SendMessageBatchResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let queue_name = self.parse_queue_name(&req.queue_url)?;
        let queue_id = name_hash(queue_name);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let len = req.entries.len();
        let mut messages = Vec::with_capacity(len);
        let mut entry_ids = Vec::with_capacity(len);
        let mut md5s = Vec::with_capacity(len);

        // Consume entries by value — zero-copy, no clones.
        for entry in req.entries {
            md5s.push(Self::md5_hex(entry.message_body.as_bytes()));
            let value = entry.message_body.into_bytes();
            let key = entry.message_group_id.map(|s| s.into_bytes());
            let mut builder = FlatMessageBuilder::new(&value).timestamp(now_ms);
            if let Some(ref k) = key {
                builder = builder.key(k);
            }
            messages.push(builder.build());
            entry_ids.push(entry.id);
        }

        let cmd = MqCommand::publish(&mut buf, queue_id, &messages);

        let resp = self.batcher.submit(cmd).await?;
        self.m_send_count.increment(entry_ids.len() as u64);

        if resp.tag() == ResponseEntry::TAG_PUBLISHED {
            let base_offset = resp.base_offset();
            let successful = entry_ids
                .into_iter()
                .zip(md5s.into_iter())
                .enumerate()
                .map(|(i, (id, md5))| {
                    let offset = base_offset + i as u64;
                    SendMessageBatchResultEntry {
                        id,
                        message_id: offset.to_string(),
                        md5_of_message_body: md5,
                    }
                })
                .collect();
            Ok(Json(SendMessageBatchResponse {
                successful,
                failed: Vec::new(),
            }))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::Internal(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn receive_message(
        &self,
        req: ReceiveMessageRequest,
    ) -> Result<Json<ReceiveMessageResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let queue_name = self.parse_queue_name(&req.queue_url)?;
        let queue_id = name_hash(queue_name);
        let max_count = req.max_number_of_messages.unwrap_or(1).min(10);
        let wait_time = req.wait_time_seconds.unwrap_or(0);

        // Generate a consumer ID for this receive operation.
        let consumer_id = rand_consumer_id();

        let cmd = MqCommand::group_deliver(&mut buf, queue_id, consumer_id, max_count);

        // Long-poll: retry up to wait_time seconds
        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_time as u64);

        loop {
            let resp = self.batcher.submit(cmd.clone()).await?;

            if resp.tag() == ResponseEntry::TAG_MESSAGES {
                let messages: Vec<DeliveredMessage> = resp.messages().collect();
                if !messages.is_empty() {
                    self.m_receive_count.increment(messages.len() as u64);
                    let sqs_messages = messages
                        .into_iter()
                        .map(|m| to_sqs_message(queue_id, m, &self.log_reader))
                        .collect();
                    return Ok(Json(ReceiveMessageResponse {
                        messages: sqs_messages,
                    }));
                }
                // No messages — check if we should keep waiting
                if wait_time == 0 || tokio::time::Instant::now() >= deadline {
                    return Ok(Json(ReceiveMessageResponse {
                        messages: Vec::new(),
                    }));
                }
                // Brief sleep before retrying
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            } else if resp.tag() == ResponseEntry::TAG_ERROR {
                return Err(SqsError::Internal(resp.error_message().to_string()));
            } else {
                return Err(SqsError::Internal("unexpected response".into()));
            }
        }
    }

    pub async fn delete_message(&self, req: DeleteMessageRequest) -> Result<Json<()>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let (queue_id, message_id) = Self::decode_receipt_handle(&req.receipt_handle)?;

        let cmd = MqCommand::group_ack(&mut buf, queue_id, &[message_id], None);

        let resp = self.batcher.submit(cmd).await?;
        self.m_delete_count.increment(1);

        if resp.is_ok() {
            Ok(Json(()))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::Internal(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn delete_message_batch(
        &self,
        req: DeleteMessageBatchRequest,
    ) -> Result<Json<DeleteMessageBatchResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        // Vec-based grouping instead of HashMap — SQS batch max is 10 entries.
        let mut by_queue: Vec<(u64, Vec<(String, u64)>)> = Vec::new();

        for entry in req.entries {
            match Self::decode_receipt_handle(&entry.receipt_handle) {
                Ok((queue_id, message_id)) => {
                    if let Some(group) = by_queue.iter_mut().find(|(qid, _)| *qid == queue_id) {
                        group.1.push((entry.id, message_id));
                    } else {
                        by_queue.push((queue_id, vec![(entry.id, message_id)]));
                    }
                }
                Err(_) => {
                    failed.push(BatchResultErrorEntry {
                        id: entry.id,
                        code: Cow::Borrowed("ReceiptHandleIsInvalid"),
                        message: Cow::Borrowed("invalid receipt handle"),
                        sender_fault: true,
                    });
                }
            }
        }

        // Single Ack command per queue with all message_ids.
        for (queue_id, entries) in by_queue {
            let (ids, message_ids): (Vec<String>, Vec<u64>) = entries.into_iter().unzip();
            let cmd = MqCommand::group_ack(&mut buf, queue_id, &message_ids, None);
            match self.batcher.submit(cmd).await {
                Ok(resp) if resp.is_ok() => {
                    for id in ids {
                        successful.push(DeleteMessageBatchResultEntry { id });
                    }
                }
                Ok(resp) if resp.tag() == ResponseEntry::TAG_ERROR => {
                    let msg: Cow<'static, str> = Cow::Owned(resp.error_message().to_string());
                    for id in ids {
                        failed.push(BatchResultErrorEntry {
                            id,
                            code: Cow::Borrowed("InternalError"),
                            message: msg.clone(),
                            sender_fault: false,
                        });
                    }
                }
                _ => {
                    for id in ids {
                        failed.push(BatchResultErrorEntry {
                            id,
                            code: Cow::Borrowed("InternalError"),
                            message: Cow::Borrowed("unexpected response"),
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
        let mut buf = bytes::BytesMut::new();
        let (queue_id, message_id) = Self::decode_receipt_handle(&req.receipt_handle)?;

        let cmd = MqCommand::group_extend_visibility(
            &mut buf,
            queue_id,
            &[message_id],
            req.visibility_timeout as u64 * 1000,
        );

        let resp = self.batcher.submit(cmd).await?;
        if resp.is_ok() {
            Ok(Json(()))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::Internal(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn change_message_visibility_batch(
        &self,
        req: ChangeMessageVisibilityBatchRequest,
    ) -> Result<Json<ChangeMessageVisibilityBatchResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        // Vec-based grouping by (queue_id, extension_ms) — SQS batch max 10 entries.
        let mut by_key: Vec<((u64, u64), Vec<(String, u64)>)> = Vec::new();

        for entry in req.entries {
            match Self::decode_receipt_handle(&entry.receipt_handle) {
                Ok((queue_id, message_id)) => {
                    let extension_ms = entry.visibility_timeout as u64 * 1000;
                    let key = (queue_id, extension_ms);
                    if let Some(group) = by_key.iter_mut().find(|(k, _)| *k == key) {
                        group.1.push((entry.id, message_id));
                    } else {
                        by_key.push((key, vec![(entry.id, message_id)]));
                    }
                }
                Err(_) => {
                    failed.push(BatchResultErrorEntry {
                        id: entry.id,
                        code: Cow::Borrowed("ReceiptHandleIsInvalid"),
                        message: Cow::Borrowed("invalid receipt handle"),
                        sender_fault: true,
                    });
                }
            }
        }

        for ((queue_id, extension_ms), entries) in by_key {
            let (ids, message_ids): (Vec<String>, Vec<u64>) = entries.into_iter().unzip();
            let cmd =
                MqCommand::group_extend_visibility(&mut buf, queue_id, &message_ids, extension_ms);
            match self.batcher.submit(cmd).await {
                Ok(resp) if resp.is_ok() => {
                    for id in ids {
                        successful.push(ChangeMessageVisibilityBatchResultEntry { id });
                    }
                }
                _ => {
                    for id in ids {
                        failed.push(BatchResultErrorEntry {
                            id,
                            code: Cow::Borrowed("InternalError"),
                            message: Cow::Borrowed("failed to change visibility"),
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
        let mut buf = bytes::BytesMut::new();
        let queue_name = self.parse_queue_name(&req.queue_url)?;
        let queue_id = name_hash(queue_name);

        let cmd = MqCommand::group_purge(&mut buf, queue_id);
        let resp = self.batcher.submit(cmd).await?;
        if resp.is_ok() {
            Ok(Json(()))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::QueueNotFound(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }

    pub async fn get_queue_attributes(
        &self,
        req: GetQueueAttributesRequest,
    ) -> Result<Json<GetQueueAttributesResponse>, SqsError> {
        let mut buf = bytes::BytesMut::new();
        let queue_name = self.parse_queue_name(&req.queue_url)?;
        let queue_id = name_hash(queue_name);

        let cmd = MqCommand::group_get_attributes(&mut buf, queue_id);
        let resp = self.batcher.submit(cmd).await?;

        if resp.tag() == ResponseEntry::TAG_STATS && resp.stats_kind() == 1 {
            let mut attrs = serde_json::Map::with_capacity(4);
            attrs.insert(
                "ApproximateNumberOfMessages".into(),
                serde_json::Value::String(resp.stats_pending_count().to_string()),
            );
            attrs.insert(
                "ApproximateNumberOfMessagesNotVisible".into(),
                serde_json::Value::String(resp.stats_in_flight_count().to_string()),
            );
            attrs.insert(
                "ApproximateNumberOfMessagesDelayed".into(),
                serde_json::Value::String("0".into()),
            );
            attrs.insert(
                "ApproximateNumberOfMessagesDead".into(),
                serde_json::Value::String(resp.stats_dlq_count().to_string()),
            );
            Ok(Json(GetQueueAttributesResponse { attributes: attrs }))
        } else if resp.tag() == ResponseEntry::TAG_ERROR {
            Err(SqsError::QueueNotFound(resp.error_message().to_string()))
        } else {
            Err(SqsError::Internal("unexpected response".into()))
        }
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn to_sqs_message(
    queue_id: u64,
    msg: DeliveredMessage,
    prefetcher: &bisque_raft::SegmentPrefetcher,
) -> SqsMessage {
    let body = if let Some(flat_bytes) = bisque_mq::read_message_at(prefetcher, msg.message_id) {
        if let Some(flat) = bisque_mq::flat::FlatMessage::new(&flat_bytes) {
            let val = flat.value();
            // Try zero-copy String::from_utf8 first (common case: valid UTF-8).
            // Falls back to lossy only for invalid data.
            String::from_utf8(val.to_vec())
                .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
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
    fn test_receipt_handle_large_values() {
        let handle = SqsState::encode_receipt_handle(u64::MAX, u64::MAX - 1);
        let (qid, mid) = SqsState::decode_receipt_handle(&handle).unwrap();
        assert_eq!(qid, u64::MAX);
        assert_eq!(mid, u64::MAX - 1);
    }

    #[test]
    fn test_receipt_handle_zero() {
        let handle = SqsState::encode_receipt_handle(0, 0);
        let (qid, mid) = SqsState::decode_receipt_handle(&handle).unwrap();
        assert_eq!(qid, 0);
        assert_eq!(mid, 0);
    }

    #[test]
    fn test_invalid_receipt_handle() {
        assert!(SqsState::decode_receipt_handle("invalid").is_err());
        assert!(SqsState::decode_receipt_handle("").is_err());
        // Valid base64 but wrong length
        assert!(SqsState::decode_receipt_handle("AQID").is_err());
    }

    #[test]
    fn test_md5_hex() {
        let result = SqsState::md5_hex(b"hello");
        assert_eq!(result.len(), 32);
        // Verify consistent output
        assert_eq!(result, SqsState::md5_hex(b"hello"));
        // Different input → different output
        assert_ne!(result, SqsState::md5_hex(b"world"));
    }
}
