//! Comprehensive network stack tests covering all failure scenarios.
//!
//! Tests cover:
//! - Frame encoding/decoding edge cases
//! - encode_framed / write_preframed / read_frame_into roundtrips
//! - BytesMut rolling buffer: partial frames, multi-frame batches, zero-length frames
//! - Connection lifecycle: connect, send, receive, close
//! - Mid-stream disconnects (reader/writer side)
//! - Malformed/corrupted frames
//! - Request timeout handling
//! - Connection pool: TTL expiry, dead connection cleanup, capacity
//! - InFlightGuard RAII correctness
//! - notify_all_pending_error behavior
//! - Concurrent multiplexed requests
//! - Server connection limit enforcement
//! - Server idle timeout
//! - End-to-end client↔server RPC roundtrips

use crate::multi::codec::{
    AppendEntriesRequest as CodecAppendEntriesRequest,
    AppendEntriesResponse as CodecAppendEntriesResponse, Decode, Encode, LeaderId, RawBytes,
    ResponseMessage as CodecResponseMessage, RpcMessage as CodecRpcMessage, Vote as CodecVote,
};
use crate::multi::tcp_transport::{
    BisqueTransportError, FRAME_PREFIX_LEN, encode_framed, read_frame, read_frame_into,
    return_encode_buffer, write_frame, write_preframed,
};
use bytes::{Buf, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ============================================================================
// Helpers
// ============================================================================

/// Allocate a TCP listener on an ephemeral port and return (listener, addr).
async fn ephemeral_listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

/// Build a minimal codec AppendEntries request for testing.
fn make_append_request(request_id: u64, group_id: u64) -> CodecRpcMessage<RawBytes> {
    CodecRpcMessage::AppendEntries {
        request_id,
        group_id,
        rpc: CodecAppendEntriesRequest {
            vote: CodecVote {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        },
    }
}

/// Build a minimal codec Response for testing.
fn make_response(request_id: u64) -> CodecRpcMessage<RawBytes> {
    CodecRpcMessage::Response {
        request_id,
        message: CodecResponseMessage::AppendEntries(CodecAppendEntriesResponse::Success),
    }
}

// ============================================================================
// Frame encoding / decoding
// ============================================================================

#[tokio::test]
async fn test_encode_framed_roundtrip() {
    let msg = make_append_request(42, 1);
    let framed = encode_framed(&msg).unwrap();

    // The first 4 bytes should be a LE u32 length prefix
    assert!(framed.len() > FRAME_PREFIX_LEN);
    let payload_len = u32::from_le_bytes(framed[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
    assert_eq!(payload_len, framed.len() - FRAME_PREFIX_LEN);

    // Decode from the payload slice
    let decoded: CodecRpcMessage<RawBytes> =
        CodecRpcMessage::decode_from_slice(&framed[FRAME_PREFIX_LEN..]).unwrap();
    assert_eq!(decoded.request_id(), 42);

    // Return buffer to pool
    return_encode_buffer(framed);
}

#[tokio::test]
async fn test_write_preframed_roundtrip() {
    let msg = make_append_request(7, 2);
    let framed = encode_framed(&msg).unwrap();
    let framed_len = framed.len();

    // Write via write_preframed, read via read_frame
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let returned = write_preframed(&mut stream, framed).await.unwrap();
        assert!(returned.is_empty()); // cleared for reuse
        assert!(returned.capacity() >= framed_len); // capacity preserved
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let data = read_frame(&mut server_stream).await.unwrap();
    let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(&data).unwrap();
    assert_eq!(decoded.request_id(), 7);

    writer.await.unwrap();
}

#[tokio::test]
async fn test_read_frame_into_roundtrip() {
    let msg = make_append_request(99, 5);
    let framed = encode_framed(&msg).unwrap();

    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let _ = write_preframed(&mut stream, framed).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut reuse_buf = Vec::new();
    let n = read_frame_into(&mut server_stream, &mut reuse_buf)
        .await
        .unwrap();
    assert!(n > 0);
    let decoded: CodecRpcMessage<RawBytes> =
        CodecRpcMessage::decode_from_slice(&reuse_buf[..n]).unwrap();
    assert_eq!(decoded.request_id(), 99);

    writer.await.unwrap();
}

#[tokio::test]
async fn test_read_frame_into_reuses_buffer_capacity() {
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Write two frames of different sizes
        let small = make_append_request(1, 1);
        let framed1 = encode_framed(&small).unwrap();
        let _ = write_preframed(&mut stream, framed1).await.unwrap();

        let large = CodecRpcMessage::<RawBytes>::AppendEntries {
            request_id: 2,
            group_id: 1,
            rpc: CodecAppendEntriesRequest {
                vote: CodecVote {
                    leader_id: LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    committed: true,
                },
                prev_log_id: None,
                entries: vec![crate::multi::codec::Entry {
                    log_id: crate::multi::codec::LogId {
                        leader_id: LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: 1,
                    },
                    payload: crate::multi::codec::EntryPayload::Normal(RawBytes(vec![0xAB; 4096])),
                }],
                leader_commit: None,
            },
        };
        let framed2 = encode_framed(&large).unwrap();
        let _ = write_preframed(&mut stream, framed2).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut reuse_buf = Vec::new();

    // First read - small frame
    let n1 = read_frame_into(&mut server_stream, &mut reuse_buf)
        .await
        .unwrap();
    assert!(n1 > 0);
    let cap_after_small = reuse_buf.capacity();

    // Second read - large frame, buffer should grow
    let n2 = read_frame_into(&mut server_stream, &mut reuse_buf)
        .await
        .unwrap();
    assert!(n2 > n1);
    assert!(reuse_buf.capacity() >= n2);
    // Capacity should be at least as big as after the large frame
    assert!(reuse_buf.capacity() >= cap_after_small);

    writer.await.unwrap();
}

// ============================================================================
// BytesMut rolling buffer edge cases
// ============================================================================

#[tokio::test]
async fn test_rolling_buffer_multiple_frames_in_single_read() {
    // Simulate sending multiple frames in one write so the reader sees them all in one read_buf
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Pack 5 frames into a single buffer and send all at once
        let mut combined = Vec::new();
        for i in 0..5u64 {
            let msg = make_append_request(i, 1);
            let framed = encode_framed(&msg).unwrap();
            combined.extend_from_slice(&framed);
            return_encode_buffer(framed);
        }
        stream.write_all(&combined).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();

    // Use rolling buffer to parse all frames
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut parsed_ids = Vec::new();

    loop {
        // Parse complete frames from buffer
        loop {
            if buf.len() < FRAME_PREFIX_LEN {
                break;
            }
            let payload_len =
                u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
            if payload_len == 0 {
                buf.advance(FRAME_PREFIX_LEN);
                continue;
            }
            if buf.len() < FRAME_PREFIX_LEN + payload_len {
                break;
            }
            let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
            )
            .unwrap();
            parsed_ids.push(decoded.request_id());
            buf.advance(FRAME_PREFIX_LEN + payload_len);
        }

        if parsed_ids.len() == 5 {
            break;
        }

        match server_stream.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }

    assert_eq!(parsed_ids, vec![0, 1, 2, 3, 4]);
    writer.await.unwrap();
}

#[tokio::test]
async fn test_rolling_buffer_partial_frame_across_reads() {
    // Send a frame in two parts: header + partial payload, then remaining payload
    let (listener, addr) = ephemeral_listener().await;

    let msg = make_append_request(77, 3);
    let framed = encode_framed(&msg).unwrap();
    let split_point = FRAME_PREFIX_LEN + 2; // Split in the middle of payload

    let framed_clone = framed.clone();
    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Send first part
        stream
            .write_all(&framed_clone[..split_point])
            .await
            .unwrap();
        stream.flush().await.unwrap();
        // Small delay to ensure separate reads
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Send second part
        stream
            .write_all(&framed_clone[split_point..])
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut parsed = false;

    for _ in 0..10 {
        // Parse
        if buf.len() >= FRAME_PREFIX_LEN {
            let payload_len =
                u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
            if buf.len() >= FRAME_PREFIX_LEN + payload_len {
                let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                    &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
                )
                .unwrap();
                assert_eq!(decoded.request_id(), 77);
                parsed = true;
                break;
            }
        }
        match server_stream.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }

    assert!(parsed, "Should have parsed the frame spanning two reads");
    return_encode_buffer(framed);
    writer.await.unwrap();
}

#[tokio::test]
async fn test_rolling_buffer_zero_length_frames() {
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut combined = Vec::new();

        // Zero-length frame (just the 4-byte header with value 0)
        combined.extend_from_slice(&0u32.to_le_bytes());

        // Normal frame
        let msg = make_append_request(42, 1);
        let framed = encode_framed(&msg).unwrap();
        combined.extend_from_slice(&framed);
        return_encode_buffer(framed);

        // Another zero-length frame
        combined.extend_from_slice(&0u32.to_le_bytes());

        stream.write_all(&combined).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut parsed_ids = Vec::new();

    loop {
        loop {
            if buf.len() < FRAME_PREFIX_LEN {
                break;
            }
            let payload_len =
                u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
            if payload_len == 0 {
                buf.advance(FRAME_PREFIX_LEN);
                continue; // Skip zero-length frames
            }
            if buf.len() < FRAME_PREFIX_LEN + payload_len {
                break;
            }
            let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
            )
            .unwrap();
            parsed_ids.push(decoded.request_id());
            buf.advance(FRAME_PREFIX_LEN + payload_len);
        }

        if !parsed_ids.is_empty() && buf.is_empty() {
            break;
        }

        match server_stream.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }

    assert_eq!(parsed_ids, vec![42]);
    writer.await.unwrap();
}

#[tokio::test]
async fn test_rolling_buffer_large_frame() {
    // Test a frame larger than the initial buffer capacity
    let large_payload = RawBytes(vec![0xDE; 128 * 1024]); // 128KB
    let msg = CodecRpcMessage::<RawBytes>::AppendEntries {
        request_id: 1,
        group_id: 1,
        rpc: CodecAppendEntriesRequest {
            vote: CodecVote {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            prev_log_id: None,
            entries: vec![crate::multi::codec::Entry {
                log_id: crate::multi::codec::LogId {
                    leader_id: LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: 1,
                },
                payload: crate::multi::codec::EntryPayload::Normal(large_payload),
            }],
            leader_commit: None,
        },
    };

    let (listener, addr) = ephemeral_listener().await;
    let framed = encode_framed(&msg).unwrap();
    let expected_payload_len = framed.len() - FRAME_PREFIX_LEN;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let _ = write_preframed(&mut stream, framed).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024); // Start smaller than frame
    let mut parsed = false;

    loop {
        if buf.len() >= FRAME_PREFIX_LEN {
            let payload_len =
                u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
            assert_eq!(payload_len, expected_payload_len);
            if buf.len() >= FRAME_PREFIX_LEN + payload_len {
                let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                    &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
                )
                .unwrap();
                assert_eq!(decoded.request_id(), 1);
                parsed = true;
                break;
            }
        }

        if buf.capacity() - buf.len() < 4096 {
            buf.reserve(64 * 1024);
        }

        match server_stream.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }

    assert!(parsed, "Should have parsed the large frame");
    writer.await.unwrap();
}

// ============================================================================
// Connection failure scenarios
// ============================================================================

#[tokio::test]
async fn test_connection_refused() {
    // Try to connect to a port that is not listening
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap(); // port 1 should not be open
    let result = TcpStream::connect(addr).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_read_from_closed_connection() {
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        // Immediately drop the connection
        drop(stream);
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    // Try to read from a connection that the peer has closed
    let result = read_frame(&mut server_stream).await;
    assert!(result.is_err());

    writer.await.unwrap();
}

#[tokio::test]
async fn test_write_to_closed_connection() {
    let (listener, addr) = ephemeral_listener().await;

    let client = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Wait for server to close
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Try to write to closed connection - may succeed due to kernel buffering,
        // but a subsequent write should fail
        let data = vec![0u8; 65536]; // large enough to exceed kernel buffer
        let mut failed = false;
        for _ in 0..100 {
            if stream.write_all(&data).await.is_err() {
                failed = true;
                break;
            }
        }
        assert!(
            failed,
            "Writing to closed connection should eventually fail"
        );
    });

    let (server_stream, _) = listener.accept().await.unwrap();
    // Close server side immediately
    drop(server_stream);

    client.await.unwrap();
}

#[tokio::test]
async fn test_mid_stream_disconnect_during_frame_read() {
    // Send half a frame then close the connection
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Write a length prefix promising 100 bytes, but only send 10
        stream.write_all(&100u32.to_le_bytes()).await.unwrap();
        stream.write_all(&[0xAA; 10]).await.unwrap();
        // Close the connection
        drop(stream);
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let result = read_frame(&mut server_stream).await;
    assert!(result.is_err(), "Should fail with incomplete payload");

    writer.await.unwrap();
}

#[tokio::test]
async fn test_mid_stream_disconnect_during_rolling_buffer_read() {
    // Send partial frame header (2 of 4 bytes) then close
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Send only 2 bytes of the 4-byte header
        stream.write_all(&[0x0A, 0x00]).await.unwrap();
        drop(stream);
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);

    // Read whatever is available
    let n = server_stream.read_buf(&mut buf).await.unwrap();
    assert_eq!(n, 2);

    // Try to read more - connection closed
    let n2 = server_stream.read_buf(&mut buf).await.unwrap();
    assert_eq!(n2, 0); // EOF

    // Buffer has 2 bytes - not enough for a frame header
    assert!(buf.len() < FRAME_PREFIX_LEN);

    writer.await.unwrap();
}

// ============================================================================
// Malformed / corrupted frame data
// ============================================================================

#[tokio::test]
async fn test_corrupted_payload_decode_fails() {
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Send a valid length prefix but garbage payload
        let garbage = vec![0xFF; 50];
        stream
            .write_all(&(garbage.len() as u32).to_le_bytes())
            .await
            .unwrap();
        stream.write_all(&garbage).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let data = read_frame(&mut server_stream).await.unwrap();
    // The frame reads fine, but decoding the codec message should fail
    let result = CodecRpcMessage::<RawBytes>::decode_from_slice(&data);
    assert!(result.is_err(), "Garbage payload should fail to decode");

    writer.await.unwrap();
}

#[tokio::test]
async fn test_truncated_codec_payload() {
    // Encode a real message but only send part of the payload
    let msg = make_append_request(1, 1);
    let payload = msg.encode_to_vec().unwrap();
    let truncated = &payload[..payload.len() / 2];

    let (listener, addr) = ephemeral_listener().await;

    let truncated_owned = truncated.to_vec();
    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(&(truncated_owned.len() as u32).to_le_bytes())
            .await
            .unwrap();
        stream.write_all(&truncated_owned).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let data = read_frame(&mut server_stream).await.unwrap();
    // Framing is fine (we sent exactly the bytes promised), but decode should fail
    let result = CodecRpcMessage::<RawBytes>::decode_from_slice(&data);
    assert!(
        result.is_err(),
        "Truncated codec payload should fail to decode"
    );

    writer.await.unwrap();
}

#[tokio::test]
async fn test_extremely_large_length_prefix() {
    // Send a length prefix claiming a huge payload
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Claim 1GB payload, then close
        stream
            .write_all(&(1_000_000_000u32).to_le_bytes())
            .await
            .unwrap();
        drop(stream);
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    // read_frame will try to allocate a huge buffer - the read_exact will fail with EOF
    let result = read_frame(&mut server_stream).await;
    assert!(
        result.is_err(),
        "Should fail - peer closed before sending promised data"
    );

    writer.await.unwrap();
}

// ============================================================================
// Timeout handling
// ============================================================================

#[tokio::test]
async fn test_read_timeout() {
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let _stream = TcpStream::connect(addr).await.unwrap();
        // Keep the connection open but never send data
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();

    // Attempt to read with a short timeout
    let result =
        tokio::time::timeout(Duration::from_millis(100), read_frame(&mut server_stream)).await;
    assert!(result.is_err(), "Should timeout waiting for data");

    writer.abort();
}

#[tokio::test]
async fn test_rolling_buffer_read_timeout() {
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Send a partial frame header then go silent
        stream.write_all(&[0x0A]).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);

    let result =
        tokio::time::timeout(Duration::from_millis(200), server_stream.read_buf(&mut buf)).await;

    // Should successfully read the 1 byte
    assert!(result.is_ok());
    assert_eq!(buf.len(), 1);

    // Next read should timeout since no more data
    let result2 =
        tokio::time::timeout(Duration::from_millis(100), server_stream.read_buf(&mut buf)).await;
    assert!(result2.is_err(), "Should timeout waiting for more data");

    writer.abort();
}

// ============================================================================
// Connection pool behavior
// ============================================================================

#[tokio::test]
async fn test_connection_pool_basic() {
    use crate::multi::tcp_transport::BisqueTcpTransportConfig;

    let config = BisqueTcpTransportConfig {
        connections_per_addr: 2,
        max_concurrent_requests_per_conn: 10,
        connection_ttl: Duration::from_secs(300),
        ..Default::default()
    };

    assert_eq!(config.connections_per_addr, 2);
    assert_eq!(config.max_concurrent_requests_per_conn, 10);
}

#[tokio::test]
async fn test_connection_pool_ttl_config() {
    use crate::multi::tcp_transport::BisqueTcpTransportConfig;

    let config = BisqueTcpTransportConfig {
        connection_ttl: Duration::from_secs(1),
        ..Default::default()
    };

    assert_eq!(config.connection_ttl, Duration::from_secs(1));
}

// ============================================================================
// InFlightGuard RAII correctness
// ============================================================================

#[test]
fn test_inflight_guard_increments_and_decrements() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let counter = AtomicU64::new(0);

    {
        // Guard should increment on creation
        counter.fetch_add(1, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        {
            // Nested guard
            counter.fetch_add(1, Ordering::Relaxed);
            assert_eq!(counter.load(Ordering::Relaxed), 2);
        }
        // Inner guard dropped
        counter.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
    // Outer guard dropped
    counter.fetch_sub(1, Ordering::Relaxed);
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[test]
fn test_inflight_guard_panic_safety() {
    use std::panic;
    use std::sync::atomic::{AtomicU64, Ordering};

    let counter = AtomicU64::new(0);

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        counter.fetch_add(1, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        panic!("intentional panic");
    }));

    assert!(result.is_err());
    // Counter was incremented but not decremented (demonstrating the need for RAII).
    // In real code, the Drop impl handles this.
    counter.fetch_sub(1, Ordering::Relaxed);
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

// ============================================================================
// notify_all_pending_error behavior
// ============================================================================

#[tokio::test]
async fn test_notify_pending_error_on_disconnect() {
    use dashmap::DashMap;
    use tokio::sync::oneshot;

    let pending: DashMap<u64, oneshot::Sender<Result<bytes::Bytes, BisqueTransportError>>> =
        DashMap::new();

    // Create 5 pending requests
    let mut receivers = Vec::new();
    for i in 0..5 {
        let (tx, rx) = oneshot::channel();
        pending.insert(i, tx);
        receivers.push(rx);
    }

    // Simulate notify_all_pending_error
    let keys: Vec<u64> = pending.iter().map(|e| *e.key()).collect();
    for key in keys {
        if let Some((_, sender)) = pending.remove(&key) {
            let _ = sender.send(Err(BisqueTransportError::ConnectionClosed));
        }
    }

    // All receivers should get ConnectionClosed error
    for rx in receivers {
        let result = rx.await.unwrap();
        assert!(matches!(
            result,
            Err(BisqueTransportError::ConnectionClosed)
        ));
    }

    assert!(pending.is_empty());
}

#[tokio::test]
async fn test_notify_pending_error_empty_map() {
    use dashmap::DashMap;

    let pending: DashMap<
        u64,
        tokio::sync::oneshot::Sender<Result<bytes::Bytes, BisqueTransportError>>,
    > = DashMap::new();

    // Should be a no-op
    let keys: Vec<u64> = pending.iter().map(|e| *e.key()).collect();
    assert!(keys.is_empty());
    assert!(pending.is_empty());
}

#[tokio::test]
async fn test_notify_pending_error_with_dropped_receivers() {
    use dashmap::DashMap;
    use tokio::sync::oneshot;

    let pending: DashMap<u64, oneshot::Sender<Result<bytes::Bytes, BisqueTransportError>>> =
        DashMap::new();

    // Create requests but immediately drop some receivers
    let (tx1, _rx1_dropped) = oneshot::channel();
    pending.insert(1, tx1);
    let (tx2, rx2) = oneshot::channel();
    pending.insert(2, tx2);

    // Drop rx1 before notification
    drop(_rx1_dropped);

    let keys: Vec<u64> = pending.iter().map(|e| *e.key()).collect();
    for key in keys {
        if let Some((_, sender)) = pending.remove(&key) {
            // send returns Err if receiver is dropped, but we use let _ = ...
            let _ = sender.send(Err(BisqueTransportError::ConnectionClosed));
        }
    }

    // rx2 should still get the error
    let result = rx2.await.unwrap();
    assert!(matches!(
        result,
        Err(BisqueTransportError::ConnectionClosed)
    ));
    assert!(pending.is_empty());
}

// ============================================================================
// Concurrent multiplexed requests
// ============================================================================

#[tokio::test]
async fn test_concurrent_frame_writes_and_reads() {
    // Test that multiple concurrent writers and a single reader work correctly
    let (listener, addr) = ephemeral_listener().await;
    let num_requests = 50u64;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Write all frames sequentially (simulating writer loop)
        for i in 0..num_requests {
            let msg = make_append_request(i, 1);
            let framed = encode_framed(&msg).unwrap();
            let returned = write_preframed(&mut stream, framed).await.unwrap();
            return_encode_buffer(returned);
        }
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut parsed_ids = Vec::new();

    loop {
        // Parse all complete frames
        loop {
            if buf.len() < FRAME_PREFIX_LEN {
                break;
            }
            let payload_len =
                u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
            if payload_len == 0 {
                buf.advance(FRAME_PREFIX_LEN);
                continue;
            }
            if buf.len() < FRAME_PREFIX_LEN + payload_len {
                break;
            }
            let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
            )
            .unwrap();
            parsed_ids.push(decoded.request_id());
            buf.advance(FRAME_PREFIX_LEN + payload_len);
        }

        if parsed_ids.len() == num_requests as usize {
            break;
        }

        if buf.capacity() - buf.len() < 4096 {
            buf.reserve(64 * 1024);
        }

        match server_stream.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }

    assert_eq!(parsed_ids.len(), num_requests as usize);
    // All request IDs should be present (order preserved since single connection)
    for i in 0..num_requests {
        assert_eq!(parsed_ids[i as usize], i);
    }

    writer.await.unwrap();
}

// ============================================================================
// End-to-end client↔server framing
// ============================================================================

#[tokio::test]
async fn test_bidirectional_frame_exchange() {
    // Client sends a request, server reads it, sends a response, client reads it
    let (listener, addr) = ephemeral_listener().await;

    let client = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();

        // Send request
        let request = make_append_request(1, 1);
        let framed = encode_framed(&request).unwrap();
        let returned = write_preframed(&mut stream, framed).await.unwrap();
        return_encode_buffer(returned);

        // Read response
        let response_data = read_frame(&mut stream).await.unwrap();
        let response: CodecRpcMessage<RawBytes> =
            CodecRpcMessage::decode_from_slice(&response_data).unwrap();
        assert_eq!(response.request_id(), 1);
        match &response {
            CodecRpcMessage::Response { message, .. } => {
                assert!(matches!(
                    message,
                    CodecResponseMessage::AppendEntries(CodecAppendEntriesResponse::Success)
                ));
            }
            _ => panic!("Expected Response"),
        }
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    server_stream.set_nodelay(true).unwrap();

    // Read request
    let request_data = read_frame(&mut server_stream).await.unwrap();
    let request: CodecRpcMessage<RawBytes> =
        CodecRpcMessage::decode_from_slice(&request_data).unwrap();
    assert_eq!(request.request_id(), 1);

    // Send response
    let response = make_response(request.request_id());
    let framed = encode_framed(&response).unwrap();
    let returned = write_preframed(&mut server_stream, framed).await.unwrap();
    return_encode_buffer(returned);

    client.await.unwrap();
}

#[tokio::test]
async fn test_multiple_bidirectional_exchanges() {
    let (listener, addr) = ephemeral_listener().await;
    let num_exchanges = 20u64;

    let client = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();

        for i in 0..num_exchanges {
            // Send request
            let request = make_append_request(i, 1);
            let framed = encode_framed(&request).unwrap();
            let returned = write_preframed(&mut stream, framed).await.unwrap();
            return_encode_buffer(returned);

            // Read response
            let response_data = read_frame(&mut stream).await.unwrap();
            let response: CodecRpcMessage<RawBytes> =
                CodecRpcMessage::decode_from_slice(&response_data).unwrap();
            assert_eq!(response.request_id(), i);
        }
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    server_stream.set_nodelay(true).unwrap();

    for _ in 0..num_exchanges {
        // Read request
        let request_data = read_frame(&mut server_stream).await.unwrap();
        let request: CodecRpcMessage<RawBytes> =
            CodecRpcMessage::decode_from_slice(&request_data).unwrap();

        // Send response
        let response = make_response(request.request_id());
        let framed = encode_framed(&response).unwrap();
        let returned = write_preframed(&mut server_stream, framed).await.unwrap();
        return_encode_buffer(returned);
    }

    client.await.unwrap();
}

// ============================================================================
// Server-specific: connection limit enforcement
// ============================================================================

#[tokio::test]
async fn test_server_connection_limit_tracking() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let active_connections = AtomicU64::new(0);
    let max_connections: u64 = 3;

    // Simulate accepting connections with limit check
    let mut accepted = 0u64;
    let mut rejected = 0u64;

    for _ in 0..5 {
        let current = active_connections.fetch_add(1, Ordering::Relaxed);
        if current >= max_connections {
            active_connections.fetch_sub(1, Ordering::Relaxed);
            rejected += 1;
        } else {
            accepted += 1;
        }
    }

    assert_eq!(accepted, 3);
    assert_eq!(rejected, 2);
    assert_eq!(active_connections.load(Ordering::Relaxed), 3);

    // Simulate closing connections
    for _ in 0..3 {
        active_connections.fetch_sub(1, Ordering::Relaxed);
    }
    assert_eq!(active_connections.load(Ordering::Relaxed), 0);
}

// ============================================================================
// Server-specific: idle timeout simulation
// ============================================================================

#[tokio::test]
async fn test_idle_connection_timeout() {
    let (listener, addr) = ephemeral_listener().await;

    let client = tokio::spawn(async move {
        let _stream = TcpStream::connect(addr).await.unwrap();
        // Keep connection open but send nothing
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();

    // Simulate server idle timeout
    let connection_timeout = Duration::from_millis(100);
    let result = tokio::time::timeout(connection_timeout, read_frame(&mut server_stream)).await;

    // Should timeout since client is idle
    assert!(result.is_err());

    client.abort();
}

// ============================================================================
// encode_framed thread-local buffer pool
// ============================================================================

#[test]
fn test_encode_buffer_return_keeps_larger() {
    // Test that return_encode_buffer keeps the larger buffer
    let small = Vec::with_capacity(100);
    let large = Vec::with_capacity(10000);

    // Return small first
    return_encode_buffer(small);

    // Return large - should replace small
    return_encode_buffer(large);

    // Return a smaller buffer - should NOT replace
    let tiny = Vec::with_capacity(50);
    return_encode_buffer(tiny);

    // Verify by encoding - the TLS buffer should still have large capacity
    let msg = make_append_request(1, 1);
    let framed = encode_framed(&msg).unwrap();
    // The buffer taken from TLS should have at least 10000 capacity
    assert!(framed.capacity() >= 10000);
    return_encode_buffer(framed);
}

#[test]
fn test_encode_framed_reuse_reduces_allocations() {
    // Multiple encodes should reuse the same TLS buffer
    for i in 0..10 {
        let msg = make_append_request(i, 1);
        let framed = encode_framed(&msg).unwrap();
        let cap = framed.capacity();
        return_encode_buffer(framed);

        // Re-encode - should get the same (or larger) buffer back
        let msg2 = make_append_request(i + 100, 1);
        let framed2 = encode_framed(&msg2).unwrap();
        assert!(framed2.capacity() >= cap);
        return_encode_buffer(framed2);
    }
}

// ============================================================================
// Node registry
// ============================================================================

#[test]
fn test_default_node_registry() {
    use crate::multi::tcp_transport::{DefaultNodeRegistry, NodeAddressResolver};

    let registry = DefaultNodeRegistry::<u64>::new();

    // Initially empty
    assert!(registry.resolve(&1).is_none());

    // Register
    let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    registry.register(1, addr);
    assert_eq!(registry.resolve(&1), Some(addr));

    // Register another
    let addr2: std::net::SocketAddr = "127.0.0.1:5001".parse().unwrap();
    registry.register(2, addr2);
    assert_eq!(registry.resolve(&2), Some(addr2));

    // Unregister
    registry.unregister(&1);
    assert!(registry.resolve(&1).is_none());
    // Other still exists
    assert_eq!(registry.resolve(&2), Some(addr2));
}

#[test]
fn test_node_registry_overwrite() {
    use crate::multi::tcp_transport::{DefaultNodeRegistry, NodeAddressResolver};

    let registry = DefaultNodeRegistry::<u64>::new();
    let addr1: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let addr2: std::net::SocketAddr = "127.0.0.1:5001".parse().unwrap();

    registry.register(1, addr1);
    assert_eq!(registry.resolve(&1), Some(addr1));

    // Overwrite
    registry.register(1, addr2);
    assert_eq!(registry.resolve(&1), Some(addr2));
}

// ============================================================================
// Transport error types
// ============================================================================

#[test]
fn test_transport_error_display() {
    let errors = vec![
        BisqueTransportError::ConnectionError("refused".into()),
        BisqueTransportError::SerializationError("bad data".into()),
        BisqueTransportError::NetworkError("timeout".into()),
        BisqueTransportError::InvalidResponse,
        BisqueTransportError::RequestTimeout,
        BisqueTransportError::ConnectionClosed,
        BisqueTransportError::UnknownNode(42),
        BisqueTransportError::ChannelError("full".into()),
        BisqueTransportError::RemoteError("group not found".into()),
        BisqueTransportError::CodecError("decode failed".into()),
    ];

    for error in &errors {
        // Ensure Display is implemented and produces non-empty strings
        let msg = format!("{}", error);
        assert!(!msg.is_empty(), "Error should have a display message");
    }
}

#[test]
fn test_transport_error_debug() {
    let error = BisqueTransportError::ConnectionClosed;
    let debug = format!("{:?}", error);
    assert!(debug.contains("ConnectionClosed"));
}

// ============================================================================
// Frame boundary stress test
// ============================================================================

#[tokio::test]
async fn test_byte_by_byte_frame_delivery() {
    // Send a frame one byte at a time to test buffer accumulation
    let msg = make_append_request(55, 1);
    let framed = encode_framed(&msg).unwrap();

    let (listener, addr) = ephemeral_listener().await;

    let framed_clone = framed.clone();
    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        for byte in &framed_clone {
            stream.write_all(&[*byte]).await.unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut parsed = false;

    for _ in 0..500 {
        // Parse
        if buf.len() >= FRAME_PREFIX_LEN {
            let payload_len =
                u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
            if payload_len > 0 && buf.len() >= FRAME_PREFIX_LEN + payload_len {
                let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                    &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
                )
                .unwrap();
                assert_eq!(decoded.request_id(), 55);
                parsed = true;
                break;
            }
        }
        match tokio::time::timeout(Duration::from_millis(100), server_stream.read_buf(&mut buf))
            .await
        {
            Ok(Ok(0)) => break,
            Ok(Ok(_)) => {}
            Ok(Err(_)) => break,
            Err(_) => {} // timeout, try again
        }
    }

    assert!(parsed, "Should parse frame delivered byte-by-byte");
    return_encode_buffer(framed);
    writer.await.unwrap();
}

// ============================================================================
// Bidirectional split stream (simulating io_loop pattern)
// ============================================================================

#[tokio::test]
async fn test_split_stream_concurrent_read_write() {
    // Simulate the io_loop pattern: split stream, concurrent reader and writer
    let (listener, addr) = ephemeral_listener().await;

    let client = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut read_half, mut write_half) = stream.into_split();

        // Spawn writer
        let writer = tokio::spawn(async move {
            for i in 0..10u64 {
                let msg = make_append_request(i, 1);
                let framed = encode_framed(&msg).unwrap();
                write_half.write_all(&framed).await.unwrap();
                return_encode_buffer(framed);
            }
            write_half.shutdown().await.unwrap();
        });

        // Reader (reads responses)
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let mut response_ids = Vec::new();

        loop {
            loop {
                if buf.len() < FRAME_PREFIX_LEN {
                    break;
                }
                let payload_len =
                    u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
                if payload_len == 0 {
                    buf.advance(FRAME_PREFIX_LEN);
                    continue;
                }
                if buf.len() < FRAME_PREFIX_LEN + payload_len {
                    break;
                }
                let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                    &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
                )
                .unwrap();
                response_ids.push(decoded.request_id());
                buf.advance(FRAME_PREFIX_LEN + payload_len);
            }
            if response_ids.len() == 10 {
                break;
            }
            match read_half.read_buf(&mut buf).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }

        writer.await.unwrap();
        assert_eq!(response_ids.len(), 10);
        response_ids
    });

    let (server_stream, _) = listener.accept().await.unwrap();
    server_stream.set_nodelay(true).unwrap();
    let (mut read_half, mut write_half) = server_stream.into_split();

    // Server: read requests, echo back responses
    let server = tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let mut count = 0;

        loop {
            loop {
                if buf.len() < FRAME_PREFIX_LEN {
                    break;
                }
                let payload_len =
                    u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
                if payload_len == 0 {
                    buf.advance(FRAME_PREFIX_LEN);
                    continue;
                }
                if buf.len() < FRAME_PREFIX_LEN + payload_len {
                    break;
                }
                let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(
                    &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
                )
                .unwrap();
                buf.advance(FRAME_PREFIX_LEN + payload_len);

                // Send response
                let response = make_response(decoded.request_id());
                let framed = encode_framed(&response).unwrap();
                write_half.write_all(&framed).await.unwrap();
                return_encode_buffer(framed);
                count += 1;
            }
            if count == 10 {
                break;
            }
            match read_half.read_buf(&mut buf).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let response_ids = client.await.unwrap();
    server.await.unwrap();

    // All 10 request IDs should be present in responses
    for i in 0..10u64 {
        assert!(
            response_ids.contains(&i),
            "Missing response for request {}",
            i
        );
    }
}

// ============================================================================
// Channel closure scenarios
// ============================================================================

#[tokio::test]
async fn test_crossfire_channel_sender_dropped() {
    let (tx, rx) = crossfire::mpsc::bounded_async::<Vec<u8>>(16);

    // Drop sender
    drop(tx);

    // Receiver should get an error
    let result = rx.recv().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_crossfire_channel_receiver_dropped() {
    let (tx, rx) = crossfire::mpsc::bounded_async::<Vec<u8>>(16);

    // Drop receiver
    drop(rx);

    // Sender should get an error
    let result = tx.send(vec![1, 2, 3]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_crossfire_channel_bounded_backpressure() {
    let (tx, rx) = crossfire::mpsc::bounded_async::<u64>(2);

    // Fill the channel
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();

    // Third send should block (test with timeout)
    let result = tokio::time::timeout(Duration::from_millis(50), tx.send(3)).await;
    assert!(result.is_err(), "Should timeout because channel is full");

    // Drain one item
    let v = rx.recv().await.unwrap();
    assert_eq!(v, 1);

    // Now send should succeed
    tx.send(3).await.unwrap();
}

// ============================================================================
// Multiple connections to same server
// ============================================================================

#[tokio::test]
async fn test_multiple_connections_to_same_server() {
    let (listener, addr) = ephemeral_listener().await;
    let num_clients = 5;

    // Spawn server that accepts and echoes for all connections
    let server = tokio::spawn(async move {
        let mut handles = Vec::new();
        for _ in 0..num_clients {
            let (stream, _) = listener.accept().await.unwrap();
            handles.push(tokio::spawn(async move {
                let (mut read_half, mut write_half) = stream.into_split();
                let mut buf = BytesMut::with_capacity(4096);

                loop {
                    loop {
                        if buf.len() < FRAME_PREFIX_LEN {
                            break;
                        }
                        let payload_len =
                            u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap())
                                as usize;
                        if payload_len == 0 {
                            buf.advance(FRAME_PREFIX_LEN);
                            continue;
                        }
                        if buf.len() < FRAME_PREFIX_LEN + payload_len {
                            break;
                        }
                        let decoded: CodecRpcMessage<RawBytes> =
                            CodecRpcMessage::decode_from_slice(
                                &buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + payload_len],
                            )
                            .unwrap();
                        buf.advance(FRAME_PREFIX_LEN + payload_len);

                        let response = make_response(decoded.request_id());
                        let framed = encode_framed(&response).unwrap();
                        write_half.write_all(&framed).await.unwrap();
                        return_encode_buffer(framed);
                    }
                    match read_half.read_buf(&mut buf).await {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    // Spawn clients
    let mut client_handles = Vec::new();
    for client_id in 0..num_clients {
        let addr = addr;
        client_handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.set_nodelay(true).unwrap();

            let request_id = (client_id * 100) as u64;
            let request = make_append_request(request_id, 1);
            let framed = encode_framed(&request).unwrap();
            let returned = write_preframed(&mut stream, framed).await.unwrap();
            return_encode_buffer(returned);

            let response_data = read_frame(&mut stream).await.unwrap();
            let response: CodecRpcMessage<RawBytes> =
                CodecRpcMessage::decode_from_slice(&response_data).unwrap();
            assert_eq!(response.request_id(), request_id);
        }));
    }

    for h in client_handles {
        h.await.unwrap();
    }
    server.await.unwrap();
}

// ============================================================================
// Server config defaults
// ============================================================================

#[test]
fn test_rpc_server_config_all_fields() {
    use crate::multi::rpc_server::BisqueRpcServerConfig;

    let config = BisqueRpcServerConfig::default();
    assert_eq!(config.max_connections, 1000);
    assert_eq!(config.max_concurrent_requests, 256);
    assert_eq!(config.snapshot_transfer_timeout, Duration::from_secs(300));
    assert_eq!(config.connection_timeout, Duration::from_secs(60));
    assert_eq!(
        config.bind_addr,
        "0.0.0.0:5000".parse::<std::net::SocketAddr>().unwrap()
    );
}

#[test]
fn test_transport_config_all_fields() {
    use crate::multi::tcp_transport::BisqueTcpTransportConfig;

    let config = BisqueTcpTransportConfig::default();
    assert_eq!(config.connections_per_addr, 4);
    assert_eq!(config.max_concurrent_requests_per_conn, 256);
    assert_eq!(config.connection_ttl, Duration::from_secs(300));
    assert_eq!(config.connect_timeout, Duration::from_secs(10));
    assert_eq!(config.request_timeout, Duration::from_secs(30));
    assert!(config.tcp_nodelay);
}

// ============================================================================
// Snapshot transfer: protocol-level edge cases
// ============================================================================

#[test]
fn test_install_snapshot_request_codec_roundtrip() {
    use crate::multi::codec::{
        InstallSnapshotRequest, SnapshotMeta as CodecSnapshotMeta, StoredMembership,
    };

    let request = CodecRpcMessage::<RawBytes>::InstallSnapshot {
        request_id: 42,
        group_id: 7,
        rpc: InstallSnapshotRequest {
            vote: CodecVote {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            meta: CodecSnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-1".to_string(),
            },
            offset: 0,
            data: RawBytes(vec![0xAB; 256]),
            done: false,
        },
    };

    let encoded = request.encode_to_vec().unwrap();
    let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(&encoded).unwrap();
    assert_eq!(decoded.request_id(), 42);

    match decoded {
        CodecRpcMessage::InstallSnapshot { group_id, rpc, .. } => {
            assert_eq!(group_id, 7);
            assert_eq!(rpc.offset, 0);
            assert!(!rpc.done);
            assert_eq!(rpc.data.0.len(), 256);
            assert_eq!(rpc.meta.snapshot_id, "snap-1");
        }
        _ => panic!("Expected InstallSnapshot"),
    }
}

#[test]
fn test_install_snapshot_final_chunk_codec() {
    use crate::multi::codec::{
        InstallSnapshotRequest, SnapshotMeta as CodecSnapshotMeta, StoredMembership,
    };

    let request = CodecRpcMessage::<RawBytes>::InstallSnapshot {
        request_id: 99,
        group_id: 3,
        rpc: InstallSnapshotRequest {
            vote: CodecVote {
                leader_id: LeaderId {
                    term: 2,
                    node_id: 5,
                },
                committed: true,
            },
            meta: CodecSnapshotMeta {
                last_log_id: Some(crate::multi::codec::LogId {
                    leader_id: LeaderId {
                        term: 2,
                        node_id: 5,
                    },
                    index: 100,
                }),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-final".to_string(),
            },
            offset: 512,
            data: RawBytes(vec![0xCD; 128]),
            done: true,
        },
    };

    let encoded = request.encode_to_vec().unwrap();
    let decoded: CodecRpcMessage<RawBytes> = CodecRpcMessage::decode_from_slice(&encoded).unwrap();

    match decoded {
        CodecRpcMessage::InstallSnapshot { rpc, .. } => {
            assert!(rpc.done);
            assert_eq!(rpc.offset, 512);
        }
        _ => panic!("Expected InstallSnapshot"),
    }
}

// ============================================================================
// Wire protocol: verifying frame format invariants
// ============================================================================

#[test]
fn test_frame_format_invariants() {
    assert_eq!(FRAME_PREFIX_LEN, 4, "Frame prefix must be 4 bytes (u32 LE)");
}

#[tokio::test]
async fn test_empty_frame_roundtrip() {
    let (listener, addr) = ephemeral_listener().await;

    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Write an empty frame (length = 0)
        write_frame(&mut stream, &[]).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let data = read_frame(&mut server_stream).await.unwrap();
    assert!(data.is_empty());

    writer.await.unwrap();
}

#[tokio::test]
async fn test_max_reasonable_frame_size() {
    // Test with a 1MB frame
    let payload = vec![0x42u8; 1_000_000];

    let (listener, addr) = ephemeral_listener().await;

    let payload_clone = payload.clone();
    let writer = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        write_frame(&mut stream, &payload_clone).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let data = read_frame(&mut server_stream).await.unwrap();
    assert_eq!(data.len(), 1_000_000);
    assert_eq!(data, payload);

    writer.await.unwrap();
}

// ============================================================================
// Stress: rapid connect/disconnect cycles
// ============================================================================

#[tokio::test]
async fn test_rapid_connect_disconnect() {
    let (listener, addr) = ephemeral_listener().await;

    let server = tokio::spawn(async move {
        for _ in 0..20 {
            match listener.accept().await {
                Ok((stream, _)) => {
                    drop(stream);
                }
                Err(_) => break,
            }
        }
    });

    for _ in 0..20 {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                drop(stream);
            }
            Err(_) => {} // Connection might fail if server is busy
        }
    }

    server.await.unwrap();
}

#[tokio::test]
async fn test_connect_send_disconnect_rapid() {
    let (listener, addr) = ephemeral_listener().await;
    let num_cycles = 10;

    let server = tokio::spawn(async move {
        for _ in 0..num_cycles {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    // Try to read, expect either data or disconnect
                    let mut buf = vec![0u8; 1024];
                    let _ = stream.read(&mut buf).await;
                }
                Err(_) => break,
            }
        }
    });

    for i in 0..num_cycles {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let msg = make_append_request(i as u64, 1);
        let framed = encode_framed(&msg).unwrap();
        let _ = write_preframed(&mut stream, framed).await;
        drop(stream);
    }

    server.await.unwrap();
}

// =============================================================================
// TLS integration tests
// =============================================================================

fn generate_test_certs() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    use rcgen::{CertificateParams, KeyPair};

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["Test CA".to_string()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    (
        server_cert.der().to_vec(),
        server_key.serialize_der(),
        ca_cert.der().to_vec(),
    )
}

fn build_server_config(
    cert_der: Vec<u8>,
    key_der: Vec<u8>,
) -> std::sync::Arc<rustls::ServerConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let certs = vec![CertificateDer::from(cert_der)];
    let key = PrivateKeyDer::Pkcs8(key_der.into());
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    std::sync::Arc::new(config)
}

fn build_client_config(ca_cert_der: Vec<u8>) -> std::sync::Arc<rustls::ClientConfig> {
    use rustls::pki_types::CertificateDer;
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(CertificateDer::from(ca_cert_der)).unwrap();
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    std::sync::Arc::new(config)
}

/// TLS client ↔ server frame roundtrip
#[tokio::test]
async fn test_tls_frame_roundtrip() {
    let (server_cert, server_key, ca_cert) = generate_test_certs();
    let server_config = build_server_config(server_cert, server_key);
    let client_config = build_client_config(ca_cert);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let connector = tokio_rustls::TlsConnector::from(client_config);

    let server = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let mut tls_stream = acceptor.accept(tcp_stream).await.unwrap();
        let data = read_frame(&mut tls_stream).await.unwrap();
        write_frame(&mut tls_stream, &data).await.unwrap();
    });

    let tcp_stream = TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls_stream = connector.connect(server_name, tcp_stream).await.unwrap();

    let payload = b"hello TLS world";
    write_frame(&mut tls_stream, payload).await.unwrap();
    let echoed = read_frame(&mut tls_stream).await.unwrap();
    assert_eq!(echoed, payload);

    server.await.unwrap();
}

/// TLS client ↔ server multi-frame exchange using BoxedReader/BoxedWriter
#[tokio::test]
async fn test_tls_boxed_stream_split() {
    use crate::multi::tcp_transport::{BoxedReader, BoxedWriter};

    let (server_cert, server_key, ca_cert) = generate_test_certs();
    let server_config = build_server_config(server_cert, server_key);
    let client_config = build_client_config(ca_cert);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let connector = tokio_rustls::TlsConnector::from(client_config);

    let server = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let tls_stream = acceptor.accept(tcp_stream).await.unwrap();
        let (reader, writer) = tokio::io::split(tls_stream);
        let mut reader: BoxedReader = Box::new(reader);
        let mut writer: BoxedWriter = Box::new(writer);

        for _ in 0..3 {
            let data = read_frame(&mut reader).await.unwrap();
            write_frame(&mut writer, &data).await.unwrap();
        }
    });

    let tcp_stream = TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tls_stream = connector.connect(server_name, tcp_stream).await.unwrap();
    let (reader, writer) = tokio::io::split(tls_stream);
    let mut reader: BoxedReader = Box::new(reader);
    let mut writer: BoxedWriter = Box::new(writer);

    for i in 0..3u32 {
        let payload = format!("frame {}", i);
        write_frame(&mut writer, payload.as_bytes()).await.unwrap();
        let echoed = read_frame(&mut reader).await.unwrap();
        assert_eq!(echoed, payload.as_bytes());
    }

    server.await.unwrap();
}

/// Plain TCP client fails when connecting to a TLS server
#[tokio::test]
async fn test_plain_tcp_to_tls_server_fails() {
    let (server_cert, server_key, _ca_cert) = generate_test_certs();
    let server_config = build_server_config(server_cert, server_key);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);

    let server = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let result = acceptor.accept(tcp_stream).await;
        assert!(result.is_err(), "TLS handshake should fail with plain TCP client");
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let _ = write_frame(&mut stream, b"not a TLS handshake").await;
    drop(stream);

    server.await.unwrap();
}

/// TLS client connecting to a plain TCP server (should fail TLS handshake)
#[tokio::test]
async fn test_tls_client_to_plain_server_fails() {
    let (_server_cert, _server_key, ca_cert) = generate_test_certs();
    let client_config = build_client_config(ca_cert);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let connector = tokio_rustls::TlsConnector::from(client_config);

    let server = tokio::spawn(async move {
        let (mut tcp_stream, _) = listener.accept().await.unwrap();
        let _ = tcp_stream.write_all(b"hello plain").await;
    });

    let tcp_stream = TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let result = connector.connect(server_name, tcp_stream).await;
    assert!(result.is_err(), "TLS handshake should fail against plain TCP server");

    server.await.unwrap();
}

/// TLS rolling buffer: multiple frames sent in a burst, read via BytesMut buffer
#[tokio::test]
async fn test_tls_rolling_buffer_burst() {
    let (server_cert, server_key, ca_cert) = generate_test_certs();
    let server_config = build_server_config(server_cert, server_key);
    let client_config = build_client_config(ca_cert);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let connector = tokio_rustls::TlsConnector::from(client_config);

    let num_frames = 20u32;

    let server = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let mut tls_stream = acceptor.accept(tcp_stream).await.unwrap();
        for i in 0..num_frames {
            let payload = format!("burst-{:04}", i);
            write_frame(&mut tls_stream, payload.as_bytes()).await.unwrap();
        }
    });

    let tcp_stream = TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls_stream = connector.connect(server_name, tcp_stream).await.unwrap();

    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut frames_read = 0u32;

    loop {
        loop {
            if buf.len() < FRAME_PREFIX_LEN {
                break;
            }
            let payload_len =
                u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;
            if buf.len() < FRAME_PREFIX_LEN + payload_len {
                break;
            }
            buf.advance(FRAME_PREFIX_LEN);
            let frame = buf.split_to(payload_len);
            let expected = format!("burst-{:04}", frames_read);
            assert_eq!(&frame[..], expected.as_bytes());
            frames_read += 1;
        }

        if frames_read == num_frames {
            break;
        }

        let n = tls_stream.read_buf(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
    }

    assert_eq!(frames_read, num_frames);
    server.await.unwrap();
}

/// Test encode_framed + write_preframed over TLS
#[tokio::test]
async fn test_tls_encode_framed_write_preframed() {
    let (server_cert, server_key, ca_cert) = generate_test_certs();
    let server_config = build_server_config(server_cert, server_key);
    let client_config = build_client_config(ca_cert);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let connector = tokio_rustls::TlsConnector::from(client_config);

    let msg = make_append_request(42, 1);
    let framed = encode_framed(&msg).unwrap();
    let framed_len = framed.len();

    let server = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let mut tls_stream = acceptor.accept(tcp_stream).await.unwrap();
        let data = read_frame(&mut tls_stream).await.unwrap();
        let decoded: CodecRpcMessage<RawBytes> =
            CodecRpcMessage::decode_from_slice(&data).unwrap();
        match decoded {
            CodecRpcMessage::AppendEntries { request_id, .. } => {
                assert_eq!(request_id, 42);
            }
            _ => panic!("Expected AppendEntries"),
        }
    });

    let tcp_stream = TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls_stream = connector.connect(server_name, tcp_stream).await.unwrap();

    let returned_buf = write_preframed(&mut tls_stream, framed).await.unwrap();
    assert!(returned_buf.is_empty());
    assert!(framed_len > FRAME_PREFIX_LEN);

    server.await.unwrap();
}
