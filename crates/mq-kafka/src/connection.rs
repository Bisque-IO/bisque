use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;

use crate::codec::{self, CodecError};
use crate::types::*;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Per-TCP-connection state for a Kafka client.
pub struct KafkaConnection {
    pub id: u64,
    pub client_id: Option<String>,
    pub read_buf: BytesMut,
    pub write_buf: BytesMut,
    /// bisque-mq consumer IDs registered via this connection (for cleanup on disconnect).
    pub consumer_member_ids: Vec<String>,

    // Pre-initialized metrics handles
    m_requests: metrics::Counter,
    m_bytes_in: metrics::Counter,
    m_bytes_out: metrics::Counter,
}

impl KafkaConnection {
    pub fn new() -> Self {
        let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let labels = [("conn_id", format!("{}", id))];
        Self {
            id,
            client_id: None,
            read_buf: BytesMut::with_capacity(65536),
            write_buf: BytesMut::with_capacity(65536),
            consumer_member_ids: Vec::new(),
            m_requests: metrics::counter!("kafka.conn.requests", &labels),
            m_bytes_in: metrics::counter!("kafka.conn.bytes_in", &labels),
            m_bytes_out: metrics::counter!("kafka.conn.bytes_out", &labels),
        }
    }

    /// Append incoming bytes to the read buffer.
    pub fn feed_data(&mut self, data: &[u8]) {
        self.m_bytes_in.increment(data.len() as u64);
        self.read_buf.extend_from_slice(data);
    }

    /// Try to decode a complete Kafka request from the read buffer.
    /// Returns `Ok(None)` if more data is needed.
    pub fn try_decode_request(
        &mut self,
    ) -> Result<Option<(RequestHeader, KafkaRequest)>, CodecError> {
        match codec::decode_request(&self.read_buf) {
            Ok((header, request, consumed)) => {
                // Update client_id from first request
                if self.client_id.is_none() {
                    self.client_id = header.client_id.clone();
                }
                let _ = self.read_buf.split_to(consumed);
                self.m_requests.increment(1);
                Ok(Some((header, request)))
            }
            Err(CodecError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Encode a response into the write buffer.
    pub fn encode_response(&mut self, correlation_id: i32, response: &KafkaResponse) {
        let before = self.write_buf.len();
        codec::encode_response(correlation_id, response, &mut self.write_buf);
        self.m_bytes_out
            .increment((self.write_buf.len() - before) as u64);
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
        // Feed partial data
        conn.feed_data(&[0, 0, 0, 20]);
        let result = conn.try_decode_request().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_feed_and_decode_api_versions() {
        let mut conn = KafkaConnection::new();

        // Build a valid ApiVersions request frame
        let mut payload = BytesMut::new();
        payload.put_i16(18); // api_key = ApiVersions
        payload.put_i16(0); // api_version
        payload.put_i32(42); // correlation_id
        payload.put_i16(-1); // null client_id

        let mut frame = BytesMut::new();
        frame.put_i32(payload.len() as i32);
        frame.extend_from_slice(&payload);

        conn.feed_data(&frame);
        let result = conn.try_decode_request().unwrap();
        assert!(result.is_some());
        let (header, req) = result.unwrap();
        assert_eq!(header.correlation_id, 42);
        assert!(matches!(req, KafkaRequest::ApiVersions));

        // Buffer should be consumed
        assert!(conn.read_buf.is_empty());
    }

    #[test]
    fn test_encode_response() {
        let mut conn = KafkaConnection::new();
        assert!(!conn.has_pending_writes());

        let resp = KafkaResponse::Heartbeat(HeartbeatResponse { error_code: 0 });
        conn.encode_response(7, &resp);

        assert!(conn.has_pending_writes());
        let buf = conn.take_write_buf();
        assert!(!buf.is_empty());
        assert!(!conn.has_pending_writes());
    }

    #[test]
    fn test_multiple_requests_in_buffer() {
        let mut conn = KafkaConnection::new();

        // Write two ApiVersions requests back-to-back
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
}
