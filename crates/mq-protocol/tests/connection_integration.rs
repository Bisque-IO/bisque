//! Connection integration tests — simulate full client↔server exchanges over TCP.

use bytes::Bytes;
use tokio::io::{AsyncWriteExt, DuplexStream};

use bisque_mq_protocol::error::ProtocolError;
use bisque_mq_protocol::frame::*;
use bisque_mq_protocol::types::*;

// =============================================================================
// Helpers
// =============================================================================

/// Write a frame over TCP (length-prefixed).
async fn send_client(stream: &mut DuplexStream, frame: &ClientFrame) {
    let data = encode_client_frame(frame).unwrap();
    write_tcp_frame(stream, &data).await.unwrap();
}

async fn send_server(stream: &mut DuplexStream, frame: &ServerFrame) {
    let data = encode_server_frame(frame).unwrap();
    write_tcp_frame(stream, &data).await.unwrap();
}

async fn recv_server(stream: &mut DuplexStream, buf: &mut Vec<u8>) -> ServerFrame {
    read_tcp_frame(stream, buf).await.unwrap();
    decode_server_frame(buf).unwrap()
}

async fn recv_client(stream: &mut DuplexStream, buf: &mut Vec<u8>) -> ClientFrame {
    read_tcp_frame(stream, buf).await.unwrap();
    decode_client_frame(buf).unwrap()
}

// =============================================================================
// Full handshake flow
// =============================================================================

#[tokio::test]
async fn test_handshake_flow_success() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    // Client sends handshake
    send_client(
        &mut client,
        &ClientFrame::Handshake {
            token: Bytes::from_static(b"valid-token"),
            consumer_id: None,
        },
    )
    .await;

    // Server receives it
    let frame = recv_client(&mut server, &mut buf).await;
    match frame {
        ClientFrame::Handshake { token, consumer_id } => {
            assert_eq!(token, Bytes::from_static(b"valid-token"));
            assert_eq!(consumer_id, None);
        }
        _ => panic!("expected Handshake"),
    }

    // Server responds with HandshakeOk
    send_server(
        &mut server,
        &ServerFrame::HandshakeOk {
            consumer_id: 100,
            session_token: Bytes::from_static(b"session-xyz"),
        },
    )
    .await;

    // Client receives it
    let frame = recv_server(&mut client, &mut buf).await;
    match frame {
        ServerFrame::HandshakeOk {
            consumer_id,
            session_token,
        } => {
            assert_eq!(consumer_id, 100);
            assert_eq!(session_token, Bytes::from_static(b"session-xyz"));
        }
        _ => panic!("expected HandshakeOk"),
    }
}

#[tokio::test]
async fn test_handshake_flow_reconnect() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    // Client reconnects with existing consumer_id
    send_client(
        &mut client,
        &ClientFrame::Handshake {
            token: Bytes::from_static(b"refresh-token"),
            consumer_id: Some(42),
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    match frame {
        ClientFrame::Handshake { consumer_id, .. } => {
            assert_eq!(consumer_id, Some(42));
        }
        _ => panic!("expected Handshake"),
    }

    send_server(
        &mut server,
        &ServerFrame::HandshakeOk {
            consumer_id: 42,
            session_token: Bytes::from_static(b"new-session"),
        },
    )
    .await;

    let frame = recv_server(&mut client, &mut buf).await;
    assert!(matches!(
        frame,
        ServerFrame::HandshakeOk {
            consumer_id: 42,
            ..
        }
    ));
}

#[tokio::test]
async fn test_handshake_flow_rejected() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    send_client(
        &mut client,
        &ClientFrame::Handshake {
            token: Bytes::from_static(b"bad-token"),
            consumer_id: None,
        },
    )
    .await;

    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::HandshakeErr {
            code: 401,
            message: Bytes::from_static(b"invalid token"),
        },
    )
    .await;

    let frame = recv_server(&mut client, &mut buf).await;
    match frame {
        ServerFrame::HandshakeErr { code, message } => {
            assert_eq!(code, 401);
            assert_eq!(message, Bytes::from_static(b"invalid token"));
        }
        _ => panic!("expected HandshakeErr"),
    }
}

// =============================================================================
// Subscribe + message delivery flow
// =============================================================================

#[tokio::test]
async fn test_subscribe_and_receive_messages() {
    let (mut client, mut server) = tokio::io::duplex(65536);
    let mut buf = Vec::new();

    // Client subscribes to a topic
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 1,
            group_id: 5,
            entity_type: 0, // Topic
            name_hash: 0xABCD,
            start_offset: 0,
            max_in_flight: 10,
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(frame, ClientFrame::Subscribe { sub_id: 1, .. }));

    // Server confirms subscription
    send_server(
        &mut server,
        &ServerFrame::Subscribed {
            sub_id: 1,
            entity_id: 77,
        },
    )
    .await;

    let frame = recv_server(&mut client, &mut buf).await;
    assert!(matches!(
        frame,
        ServerFrame::Subscribed {
            sub_id: 1,
            entity_id: 77
        }
    ));

    // Server delivers 3 messages
    for i in 0..3u64 {
        send_server(
            &mut server,
            &ServerFrame::Message(ServerMessage {
                sub_id: 1,
                message_id: 100 + i,
                timestamp: 1_700_000_000 + i,
                key: Some(Bytes::from(format!("key-{}", i))),
                value: Bytes::from(format!("payload-{}", i)),
                headers: vec![(
                    Bytes::from_static(b"idx"),
                    Bytes::from(i.to_le_bytes().to_vec()),
                )],
            }),
        )
        .await;
    }

    // Client receives all 3
    for i in 0..3u64 {
        let frame = recv_server(&mut client, &mut buf).await;
        match frame {
            ServerFrame::Message(msg) => {
                assert_eq!(msg.sub_id, 1);
                assert_eq!(msg.message_id, 100 + i);
                assert_eq!(msg.key, Some(Bytes::from(format!("key-{}", i))));
                assert_eq!(msg.value, Bytes::from(format!("payload-{}", i)));
            }
            _ => panic!("expected Message"),
        }
    }

    // Client acks all 3
    send_client(
        &mut client,
        &ClientFrame::Ack {
            sub_id: 1,
            message_ids: vec![100, 101, 102],
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    match frame {
        ClientFrame::Ack {
            sub_id,
            message_ids,
        } => {
            assert_eq!(sub_id, 1);
            assert_eq!(message_ids, vec![100, 101, 102]);
        }
        _ => panic!("expected Ack"),
    }
}

#[tokio::test]
async fn test_subscribe_error() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 99,
            group_id: 1,
            entity_type: 0,
            name_hash: 0xDEAD,
            start_offset: 0,
            max_in_flight: 10,
        },
    )
    .await;
    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::SubscriptionErr {
            sub_id: 99,
            code: 404,
            message: Bytes::from_static(b"topic not found"),
        },
    )
    .await;

    let frame = recv_server(&mut client, &mut buf).await;
    match frame {
        ServerFrame::SubscriptionErr {
            sub_id,
            code,
            message,
        } => {
            assert_eq!(sub_id, 99);
            assert_eq!(code, 404);
            assert_eq!(message, Bytes::from_static(b"topic not found"));
        }
        _ => panic!("expected SubscriptionErr"),
    }
}

// =============================================================================
// Multi-subscription flow
// =============================================================================

#[tokio::test]
async fn test_multiple_subscriptions_interleaved() {
    let (mut client, mut server) = tokio::io::duplex(65536);
    let mut buf = Vec::new();

    // Subscribe to topic (sub_id=1) and queue (sub_id=2)
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 1,
            group_id: 1,
            entity_type: 0, // Topic
            name_hash: 0x1111,
            start_offset: 0,
            max_in_flight: 5,
        },
    )
    .await;
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 2,
            group_id: 1,
            entity_type: 1, // Queue
            name_hash: 0x2222,
            start_offset: 0,
            max_in_flight: 3,
        },
    )
    .await;

    recv_client(&mut server, &mut buf).await;
    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::Subscribed {
            sub_id: 1,
            entity_id: 10,
        },
    )
    .await;
    send_server(
        &mut server,
        &ServerFrame::Subscribed {
            sub_id: 2,
            entity_id: 20,
        },
    )
    .await;

    recv_server(&mut client, &mut buf).await;
    recv_server(&mut client, &mut buf).await;

    // Server interleaves messages from both subscriptions
    send_server(
        &mut server,
        &ServerFrame::Message(ServerMessage {
            sub_id: 1,
            message_id: 1000,
            timestamp: 1,
            key: None,
            value: Bytes::from_static(b"topic-msg-1"),
            headers: vec![],
        }),
    )
    .await;
    send_server(
        &mut server,
        &ServerFrame::Message(ServerMessage {
            sub_id: 2,
            message_id: 2000,
            timestamp: 2,
            key: None,
            value: Bytes::from_static(b"queue-msg-1"),
            headers: vec![],
        }),
    )
    .await;
    send_server(
        &mut server,
        &ServerFrame::Message(ServerMessage {
            sub_id: 1,
            message_id: 1001,
            timestamp: 3,
            key: None,
            value: Bytes::from_static(b"topic-msg-2"),
            headers: vec![],
        }),
    )
    .await;

    // Client receives interleaved
    let m1 = recv_server(&mut client, &mut buf).await;
    assert!(
        matches!(m1, ServerFrame::Message(ref msg) if msg.sub_id == 1 && msg.message_id == 1000)
    );

    let m2 = recv_server(&mut client, &mut buf).await;
    assert!(
        matches!(m2, ServerFrame::Message(ref msg) if msg.sub_id == 2 && msg.message_id == 2000)
    );

    let m3 = recv_server(&mut client, &mut buf).await;
    assert!(
        matches!(m3, ServerFrame::Message(ref msg) if msg.sub_id == 1 && msg.message_id == 1001)
    );

    // Client acks topic and nacks queue
    send_client(
        &mut client,
        &ClientFrame::Ack {
            sub_id: 1,
            message_ids: vec![1000, 1001],
        },
    )
    .await;
    send_client(
        &mut client,
        &ClientFrame::Nack {
            sub_id: 2,
            message_ids: vec![2000],
        },
    )
    .await;

    let f1 = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f1, ClientFrame::Ack { sub_id: 1, .. }));
    let f2 = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f2, ClientFrame::Nack { sub_id: 2, .. }));
}

// =============================================================================
// Flow control frames
// =============================================================================

#[tokio::test]
async fn test_set_max_in_flight_flow() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    // Client adjusts max_in_flight dynamically
    send_client(
        &mut client,
        &ClientFrame::SetMaxInFlight {
            sub_id: 1,
            max_in_flight: 100,
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(
        frame,
        ClientFrame::SetMaxInFlight {
            sub_id: 1,
            max_in_flight: 100
        }
    ));

    // Pause delivery
    send_client(
        &mut client,
        &ClientFrame::SetMaxInFlight {
            sub_id: 1,
            max_in_flight: 0,
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(
        frame,
        ClientFrame::SetMaxInFlight {
            sub_id: 1,
            max_in_flight: 0
        }
    ));
}

#[tokio::test]
async fn test_byte_budget_flow() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    // Client sets initial byte budget
    send_client(
        &mut client,
        &ClientFrame::SetByteBudget {
            budget_bytes: 16_000_000,
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(
        frame,
        ClientFrame::SetByteBudget {
            budget_bytes: 16_000_000
        }
    ));

    // Client reduces budget (backpressure)
    send_client(
        &mut client,
        &ClientFrame::SetByteBudget {
            budget_bytes: 1_000_000,
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(
        frame,
        ClientFrame::SetByteBudget {
            budget_bytes: 1_000_000
        }
    ));
}

// =============================================================================
// Topic offset commit flow
// =============================================================================

#[tokio::test]
async fn test_commit_offset_flow() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    // Client commits offset
    send_client(
        &mut client,
        &ClientFrame::CommitOffset {
            sub_id: 1,
            offset: 500,
        },
    )
    .await;

    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(
        frame,
        ClientFrame::CommitOffset {
            sub_id: 1,
            offset: 500
        }
    ));
}

// =============================================================================
// Unsubscribe flow
// =============================================================================

#[tokio::test]
async fn test_subscribe_unsubscribe_resubscribe() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    // Subscribe
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 1,
            group_id: 1,
            entity_type: 0,
            name_hash: 0xAAAA,
            start_offset: 0,
            max_in_flight: 10,
        },
    )
    .await;
    let f = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f, ClientFrame::Subscribe { sub_id: 1, .. }));

    // Unsubscribe
    send_client(&mut client, &ClientFrame::Unsubscribe { sub_id: 1 }).await;
    let f = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f, ClientFrame::Unsubscribe { sub_id: 1 }));

    // Re-subscribe with same sub_id but different topic
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 1,
            group_id: 2,
            entity_type: 1,
            name_hash: 0xBBBB,
            start_offset: 0,
            max_in_flight: 20,
        },
    )
    .await;
    let f = recv_client(&mut server, &mut buf).await;
    match f {
        ClientFrame::Subscribe {
            sub_id,
            group_id,
            entity_type,
            name_hash,
            max_in_flight,
            ..
        } => {
            assert_eq!(sub_id, 1);
            assert_eq!(group_id, 2);
            assert_eq!(entity_type, 1);
            assert_eq!(name_hash, 0xBBBB);
            assert_eq!(max_in_flight, 20);
        }
        _ => panic!("expected Subscribe"),
    }
}

// =============================================================================
// Heartbeat exchange
// =============================================================================

#[tokio::test]
async fn test_bidirectional_heartbeats() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    // Client heartbeat
    send_client(&mut client, &ClientFrame::Heartbeat).await;
    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(frame, ClientFrame::Heartbeat));

    // Server heartbeat
    send_server(
        &mut server,
        &ServerFrame::Heartbeat {
            server_time_ms: 1_700_000_000_000,
        },
    )
    .await;
    let frame = recv_server(&mut client, &mut buf).await;
    assert!(matches!(
        frame,
        ServerFrame::Heartbeat {
            server_time_ms: 1_700_000_000_000
        }
    ));
}

// =============================================================================
// Graceful close flow
// =============================================================================

#[tokio::test]
async fn test_client_initiated_close() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    send_client(&mut client, &ClientFrame::Close).await;
    let frame = recv_client(&mut server, &mut buf).await;
    assert!(matches!(frame, ClientFrame::Close));

    send_server(
        &mut server,
        &ServerFrame::Close {
            reason: Bytes::from_static(b"acknowledged"),
        },
    )
    .await;
    let frame = recv_server(&mut client, &mut buf).await;
    assert!(matches!(frame, ServerFrame::Close { .. }));
}

#[tokio::test]
async fn test_server_initiated_close() {
    let (mut client, mut server) = tokio::io::duplex(8192);
    let mut buf = Vec::new();

    send_server(
        &mut server,
        &ServerFrame::Close {
            reason: Bytes::from_static(b"connection TTL expired"),
        },
    )
    .await;

    let frame = recv_server(&mut client, &mut buf).await;
    match frame {
        ServerFrame::Close { reason } => {
            assert_eq!(reason, Bytes::from_static(b"connection TTL expired"));
        }
        _ => panic!("expected Close"),
    }
}

// =============================================================================
// Connection drop detection
// =============================================================================

#[tokio::test]
async fn test_detect_closed_connection_on_read() {
    let (client, mut server) = tokio::io::duplex(8192);
    drop(client); // simulate abrupt close

    let mut buf = Vec::new();
    let err = read_tcp_frame(&mut server, &mut buf).await.unwrap_err();
    assert!(matches!(err, ProtocolError::ConnectionClosed));
}

#[tokio::test]
async fn test_detect_closed_connection_on_write() {
    let (mut client, server) = tokio::io::duplex(8192);
    drop(server);

    let data = encode_client_frame(&ClientFrame::Heartbeat).unwrap();
    // Writing to a closed duplex may succeed buffered, but eventually fails
    // Write enough to overflow the buffer
    let mut failed = false;
    for _ in 0..1000 {
        if write_tcp_frame(&mut client, &data).await.is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "expected write to fail after peer close");
}

// =============================================================================
// Full session lifecycle
// =============================================================================

#[tokio::test]
async fn test_full_session_lifecycle() {
    let (mut client, mut server) = tokio::io::duplex(65536);
    let mut buf = Vec::new();

    // 1. Handshake
    send_client(
        &mut client,
        &ClientFrame::Handshake {
            token: Bytes::from_static(b"auth-token"),
            consumer_id: None,
        },
    )
    .await;
    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::HandshakeOk {
            consumer_id: 1,
            session_token: Bytes::from_static(b"sess"),
        },
    )
    .await;
    recv_server(&mut client, &mut buf).await;

    // 2. Set byte budget
    send_client(
        &mut client,
        &ClientFrame::SetByteBudget {
            budget_bytes: 10_000_000,
        },
    )
    .await;
    recv_client(&mut server, &mut buf).await;

    // 3. Subscribe to topic
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 1,
            group_id: 1,
            entity_type: 0,
            name_hash: 0x1234,
            start_offset: 0,
            max_in_flight: 50,
        },
    )
    .await;
    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::Subscribed {
            sub_id: 1,
            entity_id: 100,
        },
    )
    .await;
    recv_server(&mut client, &mut buf).await;

    // 4. Subscribe to queue
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 2,
            group_id: 1,
            entity_type: 1,
            name_hash: 0x5678,
            start_offset: 0,
            max_in_flight: 10,
        },
    )
    .await;
    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::Subscribed {
            sub_id: 2,
            entity_id: 200,
        },
    )
    .await;
    recv_server(&mut client, &mut buf).await;

    // 5. Subscribe to actor namespace
    send_client(
        &mut client,
        &ClientFrame::Subscribe {
            sub_id: 3,
            group_id: 2,
            entity_type: 2, // ActorNamespace
            name_hash: 0x9ABC,
            start_offset: 0,
            max_in_flight: 5,
        },
    )
    .await;
    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::Subscribed {
            sub_id: 3,
            entity_id: 300,
        },
    )
    .await;
    recv_server(&mut client, &mut buf).await;

    // 6. Receive interleaved messages
    for i in 0..5u64 {
        let sub_id = (i % 3) as u32 + 1;
        send_server(
            &mut server,
            &ServerFrame::Message(ServerMessage {
                sub_id,
                message_id: 1000 + i,
                timestamp: 1_700_000_000 + i,
                key: if i % 2 == 0 {
                    Some(Bytes::from(format!("k{}", i)))
                } else {
                    None
                },
                value: Bytes::from(format!("msg-{}", i)),
                headers: vec![],
            }),
        )
        .await;
    }

    for i in 0..5u64 {
        let frame = recv_server(&mut client, &mut buf).await;
        match &frame {
            ServerFrame::Message(msg) => {
                assert_eq!(msg.sub_id, (i % 3) as u32 + 1);
                assert_eq!(msg.message_id, 1000 + i);
            }
            _ => panic!("expected Message"),
        }
    }

    // 7. Ack some, nack one, commit offset
    send_client(
        &mut client,
        &ClientFrame::Ack {
            sub_id: 1,
            message_ids: vec![1000, 1003],
        },
    )
    .await;
    send_client(
        &mut client,
        &ClientFrame::Nack {
            sub_id: 2,
            message_ids: vec![1001],
        },
    )
    .await;
    send_client(
        &mut client,
        &ClientFrame::CommitOffset {
            sub_id: 1,
            offset: 1003,
        },
    )
    .await;

    let f1 = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f1, ClientFrame::Ack { sub_id: 1, .. }));
    let f2 = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f2, ClientFrame::Nack { sub_id: 2, .. }));
    let f3 = recv_client(&mut server, &mut buf).await;
    assert!(matches!(
        f3,
        ClientFrame::CommitOffset {
            sub_id: 1,
            offset: 1003
        }
    ));

    // 8. Adjust flow control
    send_client(
        &mut client,
        &ClientFrame::SetMaxInFlight {
            sub_id: 1,
            max_in_flight: 200,
        },
    )
    .await;
    send_client(
        &mut client,
        &ClientFrame::SetByteBudget {
            budget_bytes: 50_000_000,
        },
    )
    .await;

    recv_client(&mut server, &mut buf).await;
    recv_client(&mut server, &mut buf).await;

    // 9. Unsubscribe from actor namespace
    send_client(&mut client, &ClientFrame::Unsubscribe { sub_id: 3 }).await;
    let f = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f, ClientFrame::Unsubscribe { sub_id: 3 }));

    // 10. Heartbeat exchange
    send_client(&mut client, &ClientFrame::Heartbeat).await;
    recv_client(&mut server, &mut buf).await;

    send_server(
        &mut server,
        &ServerFrame::Heartbeat {
            server_time_ms: 1_700_000_100_000,
        },
    )
    .await;
    recv_server(&mut client, &mut buf).await;

    // 11. Graceful close
    send_client(&mut client, &ClientFrame::Close).await;
    let f = recv_client(&mut server, &mut buf).await;
    assert!(matches!(f, ClientFrame::Close));

    send_server(
        &mut server,
        &ServerFrame::Close {
            reason: Bytes::from_static(b"goodbye"),
        },
    )
    .await;
    let f = recv_server(&mut client, &mut buf).await;
    assert!(matches!(f, ServerFrame::Close { .. }));
}

// =============================================================================
// High-throughput streaming
// =============================================================================

#[tokio::test]
async fn test_high_throughput_message_stream() {
    let (mut client, mut server) = tokio::io::duplex(1024 * 1024);
    let msg_count = 1000u64;

    // Server sends 1000 messages
    let write_task = tokio::spawn(async move {
        for i in 0..msg_count {
            let frame = ServerFrame::Message(ServerMessage {
                sub_id: 1,
                message_id: i,
                timestamp: i,
                key: None,
                value: Bytes::from(vec![0xAA; 100]),
                headers: vec![],
            });
            let data = encode_server_frame(&frame).unwrap();
            write_tcp_frame(&mut server, &data).await.unwrap();
        }
        // Send close to signal end
        let close = encode_server_frame(&ServerFrame::Close {
            reason: Bytes::from_static(b"done"),
        })
        .unwrap();
        write_tcp_frame(&mut server, &close).await.unwrap();
    });

    // Client reads all
    let mut buf = Vec::new();
    let mut received = 0u64;
    loop {
        read_tcp_frame(&mut client, &mut buf).await.unwrap();
        let frame = decode_server_frame(&buf).unwrap();
        match frame {
            ServerFrame::Message(msg) => {
                assert_eq!(msg.message_id, received);
                received += 1;
            }
            ServerFrame::Close { .. } => break,
            _ => panic!("unexpected frame"),
        }
    }
    assert_eq!(received, msg_count);
    write_task.await.unwrap();
}

#[tokio::test]
async fn test_high_throughput_ack_stream() {
    let (mut client, mut server) = tokio::io::duplex(1024 * 1024);
    let batch_count = 100u32;

    // Client sends 100 ack batches with 50 IDs each
    let write_task = tokio::spawn(async move {
        for batch in 0..batch_count {
            let ids: Vec<u64> = (0..50).map(|i| (batch as u64) * 50 + i).collect();
            let frame = ClientFrame::Ack {
                sub_id: 1,
                message_ids: ids,
            };
            let data = encode_client_frame(&frame).unwrap();
            write_tcp_frame(&mut client, &data).await.unwrap();
        }
        drop(client);
    });

    let mut buf = Vec::new();
    let mut total_acked = 0usize;
    loop {
        match read_tcp_frame(&mut server, &mut buf).await {
            Ok(_) => {
                let frame = decode_client_frame(&buf).unwrap();
                match frame {
                    ClientFrame::Ack { message_ids, .. } => {
                        total_acked += message_ids.len();
                    }
                    _ => panic!("expected Ack"),
                }
            }
            Err(ProtocolError::ConnectionClosed) => break,
            Err(e) => panic!("unexpected error: {}", e),
        }
    }
    assert_eq!(total_acked, 5000);
    write_task.await.unwrap();
}

// =============================================================================
// Concurrent bidirectional traffic
// =============================================================================

#[tokio::test]
async fn test_concurrent_bidirectional_traffic() {
    let (mut client_read, mut server_write) = tokio::io::duplex(256 * 1024);
    let (mut server_read, mut client_write) = tokio::io::duplex(256 * 1024);

    let msg_count = 200u64;

    // Server writer: sends messages
    let server_tx = tokio::spawn(async move {
        for i in 0..msg_count {
            let frame = ServerFrame::Message(ServerMessage {
                sub_id: (i % 3) as u32 + 1,
                message_id: i,
                timestamp: i,
                key: None,
                value: Bytes::from(vec![0xBB; 50]),
                headers: vec![],
            });
            let data = encode_server_frame(&frame).unwrap();
            write_tcp_frame(&mut server_write, &data).await.unwrap();
        }
        drop(server_write);
    });

    // Client writer: sends acks and heartbeats
    let client_tx = tokio::spawn(async move {
        for i in 0..msg_count {
            let frame = if i % 10 == 0 {
                ClientFrame::Heartbeat
            } else {
                ClientFrame::Ack {
                    sub_id: (i % 3) as u32 + 1,
                    message_ids: vec![i],
                }
            };
            let data = encode_client_frame(&frame).unwrap();
            write_tcp_frame(&mut client_write, &data).await.unwrap();
        }
        drop(client_write);
    });

    // Client reader: receives all server messages
    let client_rx = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut count = 0u64;
        loop {
            match read_tcp_frame(&mut client_read, &mut buf).await {
                Ok(_) => {
                    let frame = decode_server_frame(&buf).unwrap();
                    assert!(matches!(frame, ServerFrame::Message { .. }));
                    count += 1;
                }
                Err(ProtocolError::ConnectionClosed) => break,
                Err(e) => panic!("client read error: {}", e),
            }
        }
        count
    });

    // Server reader: receives all client frames
    let server_rx = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut ack_count = 0u64;
        let mut hb_count = 0u64;
        loop {
            match read_tcp_frame(&mut server_read, &mut buf).await {
                Ok(_) => {
                    let frame = decode_client_frame(&buf).unwrap();
                    match frame {
                        ClientFrame::Ack { .. } => ack_count += 1,
                        ClientFrame::Heartbeat => hb_count += 1,
                        _ => panic!("unexpected frame"),
                    }
                }
                Err(ProtocolError::ConnectionClosed) => break,
                Err(e) => panic!("server read error: {}", e),
            }
        }
        (ack_count, hb_count)
    });

    server_tx.await.unwrap();
    client_tx.await.unwrap();

    let messages_received = client_rx.await.unwrap();
    assert_eq!(messages_received, msg_count);

    let (acks, heartbeats) = server_rx.await.unwrap();
    assert_eq!(acks + heartbeats, msg_count);
    assert_eq!(heartbeats, 20); // every 10th frame is heartbeat: 0,10,20,...190 = 20
}

// =============================================================================
// Large message payloads over TCP framing
// =============================================================================

#[tokio::test]
async fn test_large_message_over_tcp() {
    let (mut client, mut server) = tokio::io::duplex(2 * 1024 * 1024);
    let mut buf = Vec::new();

    // 1MB value
    let large_value = Bytes::from(vec![0xEE; 1_000_000]);
    let frame = ServerFrame::Message(ServerMessage {
        sub_id: 1,
        message_id: 42,
        timestamp: 999,
        key: Some(Bytes::from(vec![0xDD; 1000])),
        value: large_value.clone(),
        headers: vec![(
            Bytes::from_static(b"big-header"),
            Bytes::from(vec![0xCC; 10_000]),
        )],
    });

    send_server(&mut server, &frame).await;
    let decoded = recv_server(&mut client, &mut buf).await;
    assert_eq!(decoded, frame);
}

// =============================================================================
// Buffer reuse across frames
// =============================================================================

#[tokio::test]
async fn test_buffer_reuse() {
    let (mut client, mut server) = tokio::io::duplex(65536);

    // Send frames of varying sizes
    let frames = vec![
        ClientFrame::Heartbeat,
        ClientFrame::Ack {
            sub_id: 1,
            message_ids: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        ClientFrame::Heartbeat,
        ClientFrame::Subscribe {
            sub_id: 99,
            group_id: 7,
            entity_type: 2,
            name_hash: 0xFEED,
            start_offset: 12345,
            max_in_flight: 500,
        },
        ClientFrame::Close,
    ];

    for f in &frames {
        let data = encode_client_frame(f).unwrap();
        write_tcp_frame(&mut client, &data).await.unwrap();
    }
    drop(client);

    // Read all using the SAME buffer — verifies buffer reuse works correctly
    let mut buf = Vec::new();
    let mut decoded = Vec::new();
    loop {
        match read_tcp_frame(&mut server, &mut buf).await {
            Ok(_) => decoded.push(decode_client_frame(&buf).unwrap()),
            Err(ProtocolError::ConnectionClosed) => break,
            Err(e) => panic!("error: {}", e),
        }
    }
    assert_eq!(decoded, frames);
}

// =============================================================================
// Partial write / read (truncated TCP stream)
// =============================================================================

#[tokio::test]
async fn test_truncated_tcp_stream_mid_frame() {
    let (mut writer, mut reader) = tokio::io::duplex(64);

    // Write a valid length prefix but only partial payload
    let frame = ClientFrame::Subscribe {
        sub_id: 1,
        group_id: 1,
        entity_type: 0,
        name_hash: 0,
        start_offset: 0,
        max_in_flight: 0,
    };
    let data = encode_client_frame(&frame).unwrap();
    let len = data.len() as u32;

    // Write length prefix
    writer.write_all(&len.to_le_bytes()).await.unwrap();
    // Write only half the payload
    writer.write_all(&data[..data.len() / 2]).await.unwrap();
    drop(writer); // close mid-frame

    let mut buf = Vec::new();
    let err = read_tcp_frame(&mut reader, &mut buf).await.unwrap_err();
    // Should get IO error (UnexpectedEof) because stream closed before full frame
    assert!(matches!(err, ProtocolError::Io(_)));
}
