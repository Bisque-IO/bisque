use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;

use crate::codec::{self, CodecError};
use crate::types::*;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Shared (non-per-connection) metrics handles for Kafka connections.
/// Avoids creating high-cardinality metric series per connection.
pub struct KafkaConnMetrics {
    pub requests: metrics::Counter,
    pub bytes_in: metrics::Counter,
    pub bytes_out: metrics::Counter,
}

impl KafkaConnMetrics {
    pub fn new(catalog_name: &str) -> Self {
        let labels = [("catalog", catalog_name.to_owned())];
        Self {
            requests: metrics::counter!("kafka.conn.requests", &labels),
            bytes_in: metrics::counter!("kafka.conn.bytes_in", &labels),
            bytes_out: metrics::counter!("kafka.conn.bytes_out", &labels),
        }
    }
}

/// Per-TCP-connection state for a Kafka client.
pub struct KafkaConnection {
    pub id: u64,
    pub client_id: Option<WireString>,
    pub read_buf: BytesMut,
    pub write_buf: BytesMut,
    /// bisque-mq consumer IDs registered via this connection (for cleanup on disconnect).
    pub consumer_member_ids: Vec<WireString>,
    metrics: &'static KafkaConnMetrics,
    /// Exponential moving average of response sizes for adaptive buffer pre-sizing.
    avg_response_size: u32,
}

// Use a lazily-initialized global for shared metrics to avoid passing through constructors.
static CONN_METRICS: std::sync::OnceLock<KafkaConnMetrics> = std::sync::OnceLock::new();

fn shared_metrics() -> &'static KafkaConnMetrics {
    CONN_METRICS.get_or_init(|| KafkaConnMetrics::new("default"))
}

/// Initialize the shared Kafka connection metrics with a specific catalog name.
/// Must be called before `shared_metrics()` for the catalog label to take effect.
pub fn init_conn_metrics(catalog_name: &str) {
    CONN_METRICS.get_or_init(|| KafkaConnMetrics::new(catalog_name));
}

impl KafkaConnection {
    pub fn new() -> Self {
        let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
            client_id: None,
            read_buf: BytesMut::with_capacity(65536),
            write_buf: BytesMut::with_capacity(65536),
            consumer_member_ids: Vec::new(),
            metrics: shared_metrics(),
            avg_response_size: 4096,
        }
    }

    /// Append incoming bytes to the read buffer.
    pub fn feed_data(&mut self, data: &[u8]) {
        self.metrics.bytes_in.increment(data.len() as u64);
        self.read_buf.extend_from_slice(data);
    }

    /// Try to decode a complete Kafka request from the read buffer.
    /// Uses zero-copy decode: splits off the frame from BytesMut and freezes
    /// to get a `Bytes`, then all string/bytes fields are zero-copy slices.
    pub fn try_decode_request(
        &mut self,
    ) -> Result<Option<(RequestHeader, KafkaRequest)>, CodecError> {
        // Peek at frame size without consuming
        let total = match codec::peek_frame_size(&self.read_buf) {
            Ok(total) => total,
            Err(CodecError::Incomplete) => return Ok(None),
            Err(e) => return Err(e),
        };

        if self.read_buf.len() < total {
            return Ok(None);
        }

        // Split off the frame and freeze for zero-copy Bytes-based decode
        let frame = self.read_buf.split_to(total).freeze();
        let (header, request) = codec::decode_request_bytes(frame)?;

        // Update client_id from first request (zero-copy clone)
        if self.client_id.is_none() {
            self.client_id = header.client_id.clone();
        }
        self.metrics.requests.increment(1);
        Ok(Some((header, request)))
    }

    /// Encode a response into the write buffer.
    /// Pre-reserves capacity based on an adaptive moving average of response sizes.
    pub fn encode_response(
        &mut self,
        correlation_id: i32,
        api_key: i16,
        api_version: i16,
        response: &KafkaResponse,
    ) {
        // Pre-reserve capacity based on adaptive average to reduce reallocs
        self.write_buf.reserve(self.avg_response_size as usize);

        let before = self.write_buf.len();
        codec::encode_response(
            correlation_id,
            api_key,
            api_version,
            response,
            &mut self.write_buf,
        );
        let response_size = (self.write_buf.len() - before) as u32;

        // Update exponential moving average: avg = avg * 7/8 + new * 1/8
        // This adapts to the connection's typical response size pattern
        self.avg_response_size =
            (self.avg_response_size.saturating_mul(7) / 8) + (response_size / 8).max(128);

        self.metrics.bytes_out.increment(response_size as u64);
    }

    /// Take the write buffer contents for sending over TCP.
    pub fn take_write_buf(&mut self) -> BytesMut {
        self.write_buf.split()
    }

    /// Whether there are pending bytes to write.
    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.is_empty()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn test_connection_new() {
        let conn = KafkaConnection::new();
        assert!(conn.id > 0);
        assert!(conn.client_id.is_none());
        assert!(!conn.has_pending_writes());
    }

    #[test]
    fn test_feed_and_decode_incomplete() {
        let mut conn = KafkaConnection::new();
        conn.feed_data(&[0, 0, 0, 20]);
        let result = conn.try_decode_request().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_feed_and_decode_api_versions() {
        let mut conn = KafkaConnection::new();

        let mut payload = BytesMut::new();
        payload.put_i16(18);
        payload.put_i16(0);
        payload.put_i32(42);
        payload.put_i16(-1);

        let mut frame = BytesMut::new();
        frame.put_i32(payload.len() as i32);
        frame.extend_from_slice(&payload);

        conn.feed_data(&frame);
        let result = conn.try_decode_request().unwrap();
        assert!(result.is_some());
        let (header, req) = result.unwrap();
        assert_eq!(header.correlation_id, 42);
        assert!(matches!(req, KafkaRequest::ApiVersions));

        assert!(conn.read_buf.is_empty());
    }

    #[test]
    fn test_encode_response() {
        let mut conn = KafkaConnection::new();
        assert!(!conn.has_pending_writes());

        let resp = KafkaResponse::Heartbeat(HeartbeatResponse { error_code: 0 });
        conn.encode_response(7, 12, 0, &resp);

        assert!(conn.has_pending_writes());
        let buf = conn.take_write_buf();
        assert!(!buf.is_empty());
        assert!(!conn.has_pending_writes());
    }

    #[test]
    fn test_multiple_requests_in_buffer() {
        let mut conn = KafkaConnection::new();

        for cid in [1i32, 2] {
            let mut payload = BytesMut::new();
            payload.put_i16(18);
            payload.put_i16(0);
            payload.put_i32(cid);
            payload.put_i16(-1);

            let mut frame = BytesMut::new();
            frame.put_i32(payload.len() as i32);
            frame.extend_from_slice(&payload);
            conn.feed_data(&frame);
        }

        let (h1, _) = conn.try_decode_request().unwrap().unwrap();
        assert_eq!(h1.correlation_id, 1);

        let (h2, _) = conn.try_decode_request().unwrap().unwrap();
        assert_eq!(h2.correlation_id, 2);

        assert!(conn.try_decode_request().unwrap().is_none());
    }

    #[test]
    fn test_connection_unique_ids() {
        let c1 = KafkaConnection::new();
        let c2 = KafkaConnection::new();
        assert_ne!(c1.id, c2.id);
    }

    #[test]
    fn test_client_id_set_on_first_request() {
        let mut conn = KafkaConnection::new();
        assert!(conn.client_id.is_none());

        // Build a valid ApiVersions request with client_id "my-client"
        let mut payload = BytesMut::new();
        payload.put_i16(18); // api_key = ApiVersions
        payload.put_i16(0); // api_version
        payload.put_i32(1); // correlation_id
        // client_id: length-prefixed string
        let cid = b"my-client";
        payload.put_i16(cid.len() as i16);
        payload.extend_from_slice(cid);

        let mut frame = BytesMut::new();
        frame.put_i32(payload.len() as i32);
        frame.extend_from_slice(&payload);

        conn.feed_data(&frame);
        let result = conn.try_decode_request().unwrap();
        assert!(result.is_some());

        assert!(conn.client_id.is_some());
        assert_eq!(conn.client_id.as_ref().unwrap().as_str(), "my-client");
    }

    #[test]
    fn test_feed_data_accumulates() {
        let mut conn = KafkaConnection::new();
        conn.feed_data(&[0, 0]);
        conn.feed_data(&[0, 8]); // frame size = 8
        // Not enough data yet
        assert!(conn.try_decode_request().unwrap().is_none());
        assert_eq!(conn.read_buf.len(), 4);
    }

    #[test]
    fn test_take_write_buf_clears() {
        let mut conn = KafkaConnection::new();
        let resp = KafkaResponse::Heartbeat(HeartbeatResponse { error_code: 0 });
        conn.encode_response(1, 12, 0, &resp);
        assert!(conn.has_pending_writes());

        let buf = conn.take_write_buf();
        assert!(!buf.is_empty());
        assert!(!conn.has_pending_writes());

        // Second take returns empty
        let buf2 = conn.take_write_buf();
        assert!(buf2.is_empty());
    }

    #[test]
    fn test_consumer_member_ids_tracking() {
        let mut conn = KafkaConnection::new();
        assert!(conn.consumer_member_ids.is_empty());
        conn.consumer_member_ids.push(WireString::from("member-1"));
        conn.consumer_member_ids.push(WireString::from("member-2"));
        assert_eq!(conn.consumer_member_ids.len(), 2);
    }

    #[test]
    fn test_partial_frame_split_across_feeds() {
        let mut conn = KafkaConnection::new();

        // Build a complete ApiVersions frame
        let mut payload = BytesMut::new();
        payload.put_i16(18); // ApiVersions
        payload.put_i16(0);
        payload.put_i32(99);
        payload.put_i16(-1); // null client_id

        let mut frame = BytesMut::new();
        frame.put_i32(payload.len() as i32);
        frame.extend_from_slice(&payload);

        let mid = frame.len() / 2;
        let (first_half, second_half) = frame.split_at(mid);

        // Feed first half
        conn.feed_data(first_half);
        assert!(conn.try_decode_request().unwrap().is_none());

        // Feed second half
        conn.feed_data(second_half);
        let result = conn.try_decode_request().unwrap();
        assert!(result.is_some());
        let (header, _) = result.unwrap();
        assert_eq!(header.correlation_id, 99);
    }

    #[test]
    fn test_init_conn_metrics() {
        // Just verify it doesn't panic
        init_conn_metrics("test-catalog");
    }
}
