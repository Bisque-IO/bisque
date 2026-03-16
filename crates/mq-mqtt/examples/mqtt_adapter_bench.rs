//! Benchmark: MQTT 3.1.1 / 5.0 adapter throughput.
//!
//! Measures the MQTT protocol adapter overhead: session state machine,
//! FlatMessage construction, MQTT wire encode/decode — the full path
//! from raw MQTT PUBLISH bytes to MqCommand, and from FlatMessage back
//! to MQTT PUBLISH wire bytes.
//!
//! Each scenario includes a native bisque-mq baseline (no MQTT encode/decode)
//! to isolate the cost of the MQTT protocol layer.
//!
//! Scenarios:
//! 1. Inbound: MQTT wire bytes → decode → session.handle_publish → MqCommand
//! 2. Outbound: FlatMessage → encode_publish_from_flat → MQTT wire bytes
//! 3. Round-trip: encode PUBLISH → decode → handle_publish → encode outbound
//!
//! Run with: cargo run --release -p bisque-mq-mqtt --example mqtt_adapter_bench

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use std::hint::black_box;
use std::time::Instant;

use bytes::{Bytes, BytesMut};

use bisque_mq::flat::{
    FlatMessage, FlatMessageBuilder, MqttEnvelope, MqttEnvelopeBuilder, is_mqtt_envelope,
    write_flat_message_vec,
};
use bisque_mq::types::MqCommand;
use bisque_mq_mqtt::codec;
use bisque_mq_mqtt::session::{MqttSession, MqttSessionConfig};
use bisque_mq_mqtt::types::*;

// =============================================================================
// Helpers
// =============================================================================

fn make_connect(version: ProtocolVersion, client_id: &str) -> Connect {
    Connect {
        protocol_name: Bytes::from("MQTT"),
        protocol_version: version,
        flags: ConnectFlags {
            username: false,
            password: false,
            will_retain: false,
            will_qos: QoS::AtMostOnce,
            will: false,
            clean_session: true,
        },
        keep_alive: 60,
        client_id: Bytes::copy_from_slice(client_id.as_bytes()),
        will: None,
        username: None,
        password: None,
        properties_raw: Vec::new(),
    }
}

fn make_publish_packet(
    topic: &str,
    payload_size: usize,
    qos: QoS,
    version: ProtocolVersion,
) -> Bytes {
    let payload = vec![0xABu8; payload_size];
    let topic_bytes = topic.as_bytes();
    let publish = Publish {
        dup: false,
        qos,
        retain: false,
        topic: topic_bytes,
        packet_id: if qos != QoS::AtMostOnce {
            Some(1)
        } else {
            None
        },
        payload: &payload,
        properties: Properties::default(),
    };
    let mut buf = BytesMut::with_capacity(256 + payload_size);
    let is_v5 = version == ProtocolVersion::V5;
    codec::encode_publish_v(&publish, &mut buf, is_v5);
    buf.freeze()
}

fn make_flat_msg(topic: &str, payload_size: usize) -> Bytes {
    let value = vec![0xABu8; payload_size];
    FlatMessageBuilder::new(&value)
        .routing_key(topic.as_bytes())
        .timestamp(1000)
        .publisher_id(42)
        .build()
}

fn init_session(version: ProtocolVersion, client_id: &str) -> MqttSession {
    init_session_opts(version, client_id, false)
}

fn init_session_opts(
    version: ProtocolVersion,
    client_id: &str,
    skip_validation: bool,
) -> MqttSession {
    let mut config = MqttSessionConfig::default();
    config.skip_topic_validation = skip_validation;
    let mut session = MqttSession::new(config);
    let connect = make_connect(version, client_id);
    session.handle_connect(&connect);
    session
}

fn make_result(
    label: &str,
    version: &'static str,
    total_msgs: u64,
    elapsed: std::time::Duration,
    bytes_per_msg: usize,
) -> BenchResult {
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let msgs_per_sec = total_msgs as f64 / elapsed.as_secs_f64();
    let bandwidth_mbps =
        (total_msgs as f64 * bytes_per_msg as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    BenchResult {
        label: label.to_string(),
        version,
        total_msgs,
        elapsed_ms,
        msgs_per_sec,
        bandwidth_mbps,
    }
}

// =============================================================================
// Result type
// =============================================================================

struct BenchResult {
    label: String,
    version: &'static str,
    total_msgs: u64,
    elapsed_ms: f64,
    msgs_per_sec: f64,
    bandwidth_mbps: f64,
}

// =============================================================================
// Benchmark: Native baseline — FlatMessage build + MqCommand::publish_to_exchange
// =============================================================================

fn bench_native_inbound(payload_size: usize, num_messages: u64) -> BenchResult {
    let payload = vec![0xABu8; payload_size];
    let topic = b"bench/topic/test";
    let exchange_id = 1u64;

    let mut buf = BytesMut::new();
    let start = Instant::now();
    for _ in 0..num_messages {
        let flat = FlatMessageBuilder::new(&payload)
            .routing_key(topic)
            .timestamp(1000)
            .publisher_id(42)
            .build();
        buf.clear();
        let cmd = MqCommand::publish_to_exchange(&mut buf, exchange_id, &[flat]);
        black_box(&cmd);
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("native inbound {}B", payload_size),
        "mq",
        num_messages,
        elapsed,
        payload_size,
    )
}

// =============================================================================
// Benchmark: Native baseline — FlatMessage field reads (outbound equivalent)
// =============================================================================

fn bench_native_outbound(payload_size: usize, num_messages: u64) -> BenchResult {
    let flat_bytes = make_flat_msg("bench/topic/test", payload_size);
    let flat = FlatMessage::new(&flat_bytes).unwrap();
    let flat_len = flat_bytes.len();

    let start = Instant::now();
    for _ in 0..num_messages {
        // Read the fields a consumer would access — the work the engine does
        // before handing off to any protocol adapter.
        let _topic = black_box(flat.routing_key());
        let _payload = black_box(flat.value());
        let _ts = black_box(flat.timestamp());
        let _pub_id = black_box(flat.publisher_id());
        let _retain = black_box(flat.is_retain());
        let _ttl = black_box(flat.ttl_ms());
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("native outbound {}B", payload_size),
        "mq",
        num_messages,
        elapsed,
        flat_len,
    )
}

// =============================================================================
// Benchmark: Native baseline — FlatMessage build + field reads (round-trip)
// =============================================================================

fn bench_native_roundtrip(payload_size: usize, num_messages: u64) -> BenchResult {
    let payload = vec![0xABu8; payload_size];
    let topic = b"bench/topic/test";

    let mut buf = BytesMut::new();
    let start = Instant::now();
    for _ in 0..num_messages {
        // Inbound: build FlatMessage + MqCommand
        let flat_bytes = FlatMessageBuilder::new(&payload)
            .routing_key(topic)
            .timestamp(1000)
            .publisher_id(42)
            .build();
        buf.clear();
        let cmd = MqCommand::publish_to_exchange(&mut buf, 1, &[flat_bytes.clone()]);
        black_box(&cmd);

        // Outbound: read fields from the FlatMessage
        let flat = FlatMessage::new(&flat_bytes).unwrap();
        let _topic = black_box(flat.routing_key());
        let _payload = black_box(flat.value());
        let _ts = black_box(flat.timestamp());
        let _pub_id = black_box(flat.publisher_id());
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("native roundtrip {}B", payload_size),
        "mq",
        num_messages,
        elapsed,
        payload_size,
    )
}

// =============================================================================
// Benchmark: Inbound (decode + handle_publish)
// =============================================================================

fn bench_inbound(
    version: ProtocolVersion,
    qos: QoS,
    payload_size: usize,
    num_messages: u64,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };

    let mut session = init_session(version, "bench-inbound");

    // Pre-encode a PUBLISH packet.
    let wire_bytes = make_publish_packet("bench/topic/test", payload_size, qos, version);
    let wire_len = wire_bytes.len();

    let mut msg_buf = BytesMut::new();
    let start = Instant::now();
    for i in 0..num_messages {
        // Decode from wire bytes (zero-copy).
        let (_, flags, remaining_length, header_size) =
            codec::parse_fixed_header(&wire_bytes).unwrap();
        let total_size = header_size + remaining_length;
        let is_v5 = version == ProtocolVersion::V5;
        let mut publish =
            codec::decode_publish_from_frozen(&wire_bytes, header_size, total_size, flags, is_v5)
                .unwrap();

        // Assign unique packet_id for QoS 1/2 to avoid duplicate rejection.
        if qos != QoS::AtMostOnce {
            publish.packet_id = Some((i % 65534 + 1) as u16);
        }
        let plan = session.handle_publish(&publish, &mut msg_buf);

        // Simulate ACK for QoS 1 so inflight doesn't fill up.
        if qos == QoS::AtLeastOnce {
            if let Some(pid) = publish.packet_id {
                session.handle_puback(pid);
            }
        } else if qos == QoS::ExactlyOnce {
            if let Some(pid) = publish.packet_id {
                // Inbound QoS2 flow: PUBLISH→PUBREC, then PUBREL→PUBCOMP
                let _pubcomp = session.handle_pubrel(pid);
            }
        }

        // Ensure the plan produced a flat message.
        debug_assert!(plan.has_message());
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("inbound QoS{} {}B", qos.as_u8(), payload_size),
        version_str,
        num_messages,
        elapsed,
        wire_len,
    )
}

// =============================================================================
// Benchmark: Inbound with skip_topic_validation
// =============================================================================

fn bench_inbound_novalidation(
    version: ProtocolVersion,
    payload_size: usize,
    num_messages: u64,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };

    let mut session = init_session_opts(version, "bench-noval", true);
    let wire_bytes =
        make_publish_packet("bench/topic/test", payload_size, QoS::AtMostOnce, version);
    let wire_len = wire_bytes.len();

    let mut msg_buf = BytesMut::new();
    let start = Instant::now();
    for _ in 0..num_messages {
        let (_, flags, remaining_length, header_size) =
            codec::parse_fixed_header(&wire_bytes).unwrap();
        let total_size = header_size + remaining_length;
        let is_v5 = version == ProtocolVersion::V5;
        let publish =
            codec::decode_publish_from_frozen(&wire_bytes, header_size, total_size, flags, is_v5)
                .unwrap();
        let plan = session.handle_publish(&publish, &mut msg_buf);
        debug_assert!(plan.has_message());
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("inbound noval {}B", payload_size),
        version_str,
        num_messages,
        elapsed,
        wire_len,
    )
}

// =============================================================================
// Benchmark: Batched Inbound (decode batch + handle_publish batch + single MqCommand)
// =============================================================================

fn bench_inbound_batched(
    version: ProtocolVersion,
    payload_size: usize,
    num_messages: u64,
    batch_size: usize,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };

    let mut session = init_session(version, "bench-inbound-batch");

    // Pre-encode a PUBLISH packet (QoS 0 for max throughput).
    let wire_bytes =
        make_publish_packet("bench/topic/test", payload_size, QoS::AtMostOnce, version);
    let wire_len = wire_bytes.len();
    let exchange_id = 1u64;

    let num_batches = num_messages / batch_size as u64;
    let mut msg_buf = BytesMut::new();

    let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(batch_size);

    let start = Instant::now();
    for _ in 0..num_batches {
        msg_buf.clear();
        offsets.clear();

        // Decode + handle_publish for each message in the batch.
        for _ in 0..batch_size {
            let (_, flags, remaining_length, header_size) =
                codec::parse_fixed_header(&wire_bytes).unwrap();
            let total_size = header_size + remaining_length;
            let is_v5 = version == ProtocolVersion::V5;
            let publish = codec::decode_publish_from_frozen(
                &wire_bytes,
                header_size,
                total_size,
                flags,
                is_v5,
            )
            .unwrap();
            let plan = session.handle_publish(&publish, &mut msg_buf);
            debug_assert!(plan.has_message());
            offsets.push((plan.msg_start, plan.msg_len));
        }

        // Build slices from the single contiguous msg_buf.
        let slices: Vec<&[u8]> = offsets
            .iter()
            .map(|&(start, len)| &msg_buf[start..start + len])
            .collect();
        let mut scratch = BytesMut::new();
        MqCommand::write_publish_to_exchange(&mut scratch, exchange_id, &slices);
        let cmd = MqCommand::split_from(&mut scratch);
        black_box(&cmd);
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("inbound batch({}) {}B", batch_size, payload_size),
        version_str,
        num_batches * batch_size as u64,
        elapsed,
        wire_len,
    )
}

fn bench_native_inbound_batched(
    payload_size: usize,
    num_messages: u64,
    batch_size: usize,
) -> BenchResult {
    let payload = vec![0xABu8; payload_size];
    let topic = b"bench/topic/test";
    let exchange_id = 1u64;

    let num_batches = num_messages / batch_size as u64;

    let start = Instant::now();
    for _ in 0..num_batches {
        let mut msg_bufs: Vec<Vec<u8>> = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let mut mbuf = Vec::new();
            write_flat_message_vec(
                &mut mbuf,
                &payload,
                None,
                Some(topic),
                None,
                None,
                &[],
                1000,
                0,
                0,
                42,
                0,
                false,
            );
            msg_bufs.push(mbuf);
        }
        let slices: Vec<&[u8]> = msg_bufs.iter().map(|b| b.as_slice()).collect();
        let mut scratch = BytesMut::new();
        MqCommand::write_publish_to_exchange(&mut scratch, exchange_id, &slices);
        let cmd = MqCommand::split_from(&mut scratch);
        black_box(&cmd);
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("native batch({}) {}B", batch_size, payload_size),
        "mq",
        num_batches * batch_size as u64,
        elapsed,
        payload_size,
    )
}

// =============================================================================
// Benchmark: Native baseline — MqttEnvelopeBuilder (no MQTT decode overhead)
// =============================================================================

fn bench_native_inbound_envelope(payload_size: usize, num_messages: u64) -> BenchResult {
    let payload = vec![0xABu8; payload_size];
    let topic = b"bench/topic/test";
    let exchange_id = 1u64;

    let mut buf = BytesMut::new();
    let start = Instant::now();
    for _ in 0..num_messages {
        let envelope = MqttEnvelopeBuilder::new(topic, &payload)
            .timestamp(1000)
            .publisher_id(42)
            .build();
        buf.clear();
        let cmd = MqCommand::publish_to_exchange(&mut buf, exchange_id, &[envelope]);
        black_box(&cmd);
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("native envelope {}B", payload_size),
        "mq",
        num_messages,
        elapsed,
        payload_size,
    )
}

// =============================================================================
// Benchmark: Native fast-path — build_flat_message from &[u8] slices
// =============================================================================

fn bench_native_inbound_fast_flat(payload_size: usize, num_messages: u64) -> BenchResult {
    let payload = vec![0xABu8; payload_size];
    let topic = b"bench/topic/test";
    let exchange_id = 1u64;

    let mut buf = BytesMut::new();
    let start = Instant::now();
    for _ in 0..num_messages {
        let flat = FlatMessageBuilder::new(&payload)
            .routing_key(topic)
            .timestamp(1000)
            .publisher_id(42)
            .build();
        buf.clear();
        let cmd = MqCommand::publish_to_exchange(&mut buf, exchange_id, &[flat]);
        black_box(&cmd);
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("native fast flat {}B", payload_size),
        "mq",
        num_messages,
        elapsed,
        payload_size,
    )
}

// =============================================================================
// Benchmark: Native fast-path — build_envelope from &[u8] slices
// =============================================================================

fn bench_native_inbound_fast_env(payload_size: usize, num_messages: u64) -> BenchResult {
    let payload = vec![0xABu8; payload_size];
    let topic = b"bench/topic/test";
    let exchange_id = 1u64;

    let mut buf = BytesMut::new();
    let start = Instant::now();
    for _ in 0..num_messages {
        let env = MqttEnvelopeBuilder::new(topic, &payload)
            .timestamp(1000)
            .publisher_id(42)
            .build();
        buf.clear();
        let cmd = MqCommand::publish_to_exchange(&mut buf, exchange_id, &[env]);
        black_box(&cmd);
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("native fast env {}B", payload_size),
        "mq",
        num_messages,
        elapsed,
        payload_size,
    )
}

// =============================================================================
// Benchmark: Outbound — MqttEnvelope → MQTT PUBLISH wire bytes
// =============================================================================

fn bench_outbound_envelope(
    version: ProtocolVersion,
    payload_size: usize,
    num_messages: u64,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };
    let is_v5 = version == ProtocolVersion::V5;

    let payload = vec![0xABu8; payload_size];
    let envelope_bytes = MqttEnvelopeBuilder::new(b"bench/topic/test", &payload)
        .timestamp(1000)
        .publisher_id(42)
        .is_v5(is_v5)
        .build();

    let envelope = MqttEnvelope::new(&envelope_bytes).unwrap();
    let env_len = envelope_bytes.len();
    let mut buf = BytesMut::with_capacity(256 + payload_size);

    let start = Instant::now();
    for i in 0..num_messages {
        buf.clear();
        let packet_id = if i % 2 == 1 { Some(1u16) } else { None };
        codec::encode_publish_from_envelope(
            &envelope,
            QoS::AtMostOnce,
            false,
            false,
            packet_id,
            is_v5,
            None,
            None,
            None,
            &mut buf,
        );
        debug_assert!(!buf.is_empty());
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("outbound env QoS0 {}B", payload_size),
        version_str,
        num_messages,
        elapsed,
        env_len,
    )
}

// =============================================================================
// Benchmark: Outbound (FlatMessage → MQTT PUBLISH wire bytes)
// =============================================================================

fn bench_outbound(
    version: ProtocolVersion,
    qos: QoS,
    payload_size: usize,
    num_messages: u64,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };
    let is_v5 = version == ProtocolVersion::V5;

    // Pre-build a FlatMessage.
    let flat_bytes = make_flat_msg("bench/topic/test", payload_size);
    let flat = FlatMessage::new(&flat_bytes).unwrap();
    let flat_len = flat_bytes.len();

    let mut buf = BytesMut::with_capacity(256 + payload_size);

    let start = Instant::now();
    for i in 0..num_messages {
        buf.clear();
        let packet_id = if qos != QoS::AtMostOnce {
            Some((i % 65534 + 1) as u16)
        } else {
            None
        };
        codec::encode_publish_from_flat(
            &flat, qos, false, false, packet_id, is_v5, None, None, &mut buf,
        );
        debug_assert!(!buf.is_empty());
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("outbound QoS{} {}B", qos.as_u8(), payload_size),
        version_str,
        num_messages,
        elapsed,
        flat_len,
    )
}

// =============================================================================
// Benchmark: Round-trip (encode → decode → handle_publish → encode outbound)
// =============================================================================

fn bench_roundtrip(
    version: ProtocolVersion,
    qos: QoS,
    payload_size: usize,
    num_messages: u64,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };
    let is_v5 = version == ProtocolVersion::V5;

    let mut session = init_session(version, "bench-roundtrip");

    let wire_bytes = make_publish_packet("bench/topic/test", payload_size, qos, version);
    let wire_len = wire_bytes.len();

    let mut out_buf = BytesMut::with_capacity(256 + payload_size);
    let mut msg_buf = BytesMut::new();

    let start = Instant::now();
    for i in 0..num_messages {
        // 1. Decode inbound PUBLISH.
        let (_, flags, remaining_length, header_size) =
            codec::parse_fixed_header(&wire_bytes).unwrap();
        let total_size = header_size + remaining_length;
        let mut publish =
            codec::decode_publish_from_frozen(&wire_bytes, header_size, total_size, flags, is_v5)
                .unwrap();

        if qos != QoS::AtMostOnce {
            publish.packet_id = Some((i % 65534 + 1) as u16);
        }

        // 2. Session processes it → produces FlatMessage.
        let plan = session.handle_publish(&publish, &mut msg_buf);

        // 3. Simulate outbound: encode back to MQTT PUBLISH.
        out_buf.clear();
        let pid = if qos != QoS::AtMostOnce {
            Some((i % 65534 + 1) as u16)
        } else {
            None
        };
        // Get the flat message bytes from plan.
        let flat_slice: &[u8] = if let Some(ref owned) = plan.flat_message {
            owned
        } else {
            &msg_buf[plan.msg_start..plan.msg_start + plan.msg_len]
        };
        if is_mqtt_envelope(flat_slice) {
            if let Some(env) = MqttEnvelope::new(flat_slice) {
                codec::encode_publish_from_envelope(
                    &env,
                    qos,
                    false,
                    false,
                    pid,
                    is_v5,
                    None,
                    None,
                    None,
                    &mut out_buf,
                );
            }
        } else if let Some(flat) = FlatMessage::new(flat_slice) {
            codec::encode_publish_from_flat(
                &flat,
                qos,
                false,
                false,
                pid,
                is_v5,
                None,
                None,
                &mut out_buf,
            );
        }

        // ACK to prevent inflight buildup.
        if qos == QoS::AtLeastOnce {
            if let Some(pid) = publish.packet_id {
                session.handle_puback(pid);
            }
        } else if qos == QoS::ExactlyOnce {
            if let Some(pid) = publish.packet_id {
                // Inbound QoS2 flow: PUBLISH→PUBREC, then PUBREL→PUBCOMP
                let _pubcomp = session.handle_pubrel(pid);
            }
        }
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("roundtrip QoS{} {}B", qos.as_u8(), payload_size),
        version_str,
        num_messages,
        elapsed,
        wire_len,
    )
}

// =============================================================================
// Benchmark: Fused Inbound (decode_publish_to_envelope + handle_publish_fused)
// =============================================================================

fn bench_inbound_fused(
    version: ProtocolVersion,
    payload_size: usize,
    num_messages: u64,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };
    let is_v5 = version == ProtocolVersion::V5;

    let mut session = init_session(version, "bench-fused");

    // Pre-encode a PUBLISH packet (QoS 0 for max throughput).
    let wire_bytes =
        make_publish_packet("bench/topic/test", payload_size, QoS::AtMostOnce, version);
    let wire_len = wire_bytes.len();
    let now = 1000u64;

    let mut msg_buf = BytesMut::new();
    let start = Instant::now();
    for _ in 0..num_messages {
        let (mut envelope, meta, _consumed) =
            codec::decode_publish_to_envelope(&wire_bytes, is_v5, now, session.session_id).unwrap();
        let plan = session.handle_publish_fused(&mut envelope, &meta, &mut msg_buf);
        debug_assert!(plan.has_message());
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("inbound fused {}B", payload_size),
        version_str,
        num_messages,
        elapsed,
        wire_len,
    )
}

// =============================================================================
// Benchmark: Fused Round-trip (fused decode → handle_publish_fused → encode outbound)
// =============================================================================

fn bench_roundtrip_fused(
    version: ProtocolVersion,
    payload_size: usize,
    num_messages: u64,
) -> BenchResult {
    let version_str = match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5.0",
    };
    let is_v5 = version == ProtocolVersion::V5;

    let mut session = init_session(version, "bench-fused-rt");

    let wire_bytes =
        make_publish_packet("bench/topic/test", payload_size, QoS::AtMostOnce, version);
    let wire_len = wire_bytes.len();
    let now = 1000u64;

    let mut out_buf = BytesMut::with_capacity(256 + payload_size);
    let mut msg_buf = BytesMut::new();

    let start = Instant::now();
    for _ in 0..num_messages {
        // 1. Fused decode + envelope build.
        let (mut envelope, meta, _consumed) =
            codec::decode_publish_to_envelope(&wire_bytes, is_v5, now, session.session_id).unwrap();

        // 2. Session processes it.
        let plan = session.handle_publish_fused(&mut envelope, &meta, &mut msg_buf);

        // 3. Encode outbound.
        out_buf.clear();
        let pid = None;
        let flat_slice: &[u8] = if let Some(ref owned) = plan.flat_message {
            owned
        } else {
            &msg_buf[plan.msg_start..plan.msg_start + plan.msg_len]
        };
        if is_mqtt_envelope(flat_slice) {
            if let Some(env) = MqttEnvelope::new(flat_slice) {
                codec::encode_publish_from_envelope(
                    &env,
                    QoS::AtMostOnce,
                    false,
                    false,
                    pid,
                    is_v5,
                    None,
                    None,
                    None,
                    &mut out_buf,
                );
            }
        }
    }
    let elapsed = start.elapsed();

    make_result(
        &format!("roundtrip fused {}B", payload_size),
        version_str,
        num_messages,
        elapsed,
        wire_len,
    )
}

// =============================================================================
// Output
// =============================================================================

fn print_header() {
    println!(
        "\n{:<26} {:>6} {:>12} {:>12} {:>14} {:>10}",
        "Benchmark", "Proto", "Messages", "Time(ms)", "Msgs/sec", "MB/s"
    );
    println!("{:-<84}", "");
}

fn print_row(r: &BenchResult) {
    println!(
        "{:<26} {:>6} {:>12} {:>12.1} {:>14.0} {:>10.1}",
        r.label, r.version, r.total_msgs, r.elapsed_ms, r.msgs_per_sec, r.bandwidth_mbps
    );
}

fn print_overhead(label: &str, baseline: &BenchResult, mqtt: &BenchResult) {
    let overhead = (1.0 - baseline.msgs_per_sec / mqtt.msgs_per_sec) * 100.0;
    let cost_ns = (1e9 / baseline.msgs_per_sec) - (1e9 / mqtt.msgs_per_sec);
    println!(
        "  {}: MQTT adds {:.0}ns/msg ({:.1}% overhead vs native)",
        label,
        cost_ns.abs(),
        overhead.abs()
    );
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    println!("MQTT Adapter Benchmark (encode/decode + session state machine)");
    println!("==============================================================\n");

    let num_messages = 1_000_000u64;

    let versions = [
        (ProtocolVersion::V311, "MQTT 3.1.1"),
        (ProtocolVersion::V5, "MQTT 5.0"),
    ];

    let payload_sizes = [64, 256, 1024];
    let qos_levels = [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];

    // --- Inbound benchmarks ---
    println!("### Inbound: payload → FlatMessage + MqCommand");
    print_header();

    for &payload_size in &payload_sizes {
        let native = bench_native_inbound(payload_size, num_messages);
        print_row(&native);

        for &(version, _) in &versions {
            for &qos in &qos_levels {
                let r = bench_inbound(version, qos, payload_size, num_messages);
                print_row(&r);
            }
        }

        // Show overhead for QoS 0 (simplest comparison).
        let r311 = bench_inbound(
            ProtocolVersion::V311,
            QoS::AtMostOnce,
            payload_size,
            num_messages,
        );
        let r5 = bench_inbound(
            ProtocolVersion::V5,
            QoS::AtMostOnce,
            payload_size,
            num_messages,
        );
        // No-validation variants.
        let nv311 = bench_inbound_novalidation(ProtocolVersion::V311, payload_size, num_messages);
        print_row(&nv311);
        let nv5 = bench_inbound_novalidation(ProtocolVersion::V5, payload_size, num_messages);
        print_row(&nv5);

        print_overhead(&format!("3.1.1 QoS0 {}B", payload_size), &r311, &native);
        print_overhead(&format!("5.0   QoS0 {}B", payload_size), &r5, &native);
        print_overhead(&format!("3.1.1 noval {}B", payload_size), &nv311, &native);
        print_overhead(&format!("5.0   noval {}B", payload_size), &nv5, &native);
        println!();
    }

    // --- Batched inbound benchmarks ---
    println!("### Batched Inbound: decode batch → handle_publish batch → single MqCommand");
    print_header();

    let batch_sizes = [8, 32, 128];
    for &payload_size in &payload_sizes {
        for &batch_size in &batch_sizes {
            let native = bench_native_inbound_batched(payload_size, num_messages, batch_size);
            print_row(&native);

            for &(version, _) in &versions {
                let r = bench_inbound_batched(version, payload_size, num_messages, batch_size);
                print_row(&r);
            }

            print_overhead(
                &format!("batch({}) {}B 3.1.1", batch_size, payload_size),
                &bench_inbound_batched(
                    ProtocolVersion::V311,
                    payload_size,
                    num_messages,
                    batch_size,
                ),
                &native,
            );
            println!();
        }
    }

    // --- Outbound benchmarks ---
    println!("### Outbound: FlatMessage → protocol wire bytes");
    print_header();

    for &payload_size in &payload_sizes {
        let native = bench_native_outbound(payload_size, num_messages);
        print_row(&native);

        for &(version, _) in &versions {
            for &qos in &qos_levels {
                let r = bench_outbound(version, qos, payload_size, num_messages);
                print_row(&r);
            }
        }

        let r311 = bench_outbound(
            ProtocolVersion::V311,
            QoS::AtMostOnce,
            payload_size,
            num_messages,
        );
        let r5 = bench_outbound(
            ProtocolVersion::V5,
            QoS::AtMostOnce,
            payload_size,
            num_messages,
        );
        print_overhead(&format!("3.1.1 QoS0 {}B", payload_size), &r311, &native);
        print_overhead(&format!("5.0   QoS0 {}B", payload_size), &r5, &native);
        println!();
    }

    // --- Round-trip benchmarks ---
    println!("### Round-trip: inbound + outbound full path");
    print_header();

    for &payload_size in &payload_sizes {
        let native = bench_native_roundtrip(payload_size, num_messages);
        print_row(&native);

        for &(version, _) in &versions {
            for &qos in &qos_levels {
                let r = bench_roundtrip(version, qos, payload_size, num_messages);
                print_row(&r);
            }
        }

        let r311 = bench_roundtrip(
            ProtocolVersion::V311,
            QoS::AtMostOnce,
            payload_size,
            num_messages,
        );
        let r5 = bench_roundtrip(
            ProtocolVersion::V5,
            QoS::AtMostOnce,
            payload_size,
            num_messages,
        );
        print_overhead(&format!("3.1.1 QoS0 {}B", payload_size), &r311, &native);
        print_overhead(&format!("5.0   QoS0 {}B", payload_size), &r5, &native);
        println!();
    }

    // --- Build cost comparison: all builder variants ---
    println!("### Message build cost comparison (all variants)");
    print_header();

    for &payload_size in &payload_sizes {
        let native_flat = bench_native_inbound(payload_size, num_messages);
        let native_env = bench_native_inbound_envelope(payload_size, num_messages);
        let fast_flat = bench_native_inbound_fast_flat(payload_size, num_messages);
        let fast_env = bench_native_inbound_fast_env(payload_size, num_messages);
        print_row(&native_flat);
        print_row(&native_env);
        print_row(&fast_flat);
        print_row(&fast_env);

        let flat_ns = 1e9 / native_flat.msgs_per_sec;
        let env_ns = 1e9 / native_env.msgs_per_sec;
        let fast_flat_ns = 1e9 / fast_flat.msgs_per_sec;
        let fast_env_ns = 1e9 / fast_env.msgs_per_sec;
        println!("  FlatMessageBuilder:     {:.0}ns/msg", flat_ns,);
        println!(
            "  MqttEnvelopeBuilder:    {:.0}ns/msg (saves {:.0}ns)",
            env_ns,
            flat_ns - env_ns,
        );
        println!(
            "  build_flat_message:     {:.0}ns/msg (saves {:.0}ns vs FlatMessageBuilder)",
            fast_flat_ns,
            flat_ns - fast_flat_ns,
        );
        println!(
            "  build_envelope:         {:.0}ns/msg (saves {:.0}ns vs FlatMessageBuilder)",
            fast_env_ns,
            flat_ns - fast_env_ns,
        );
        println!();
    }

    // --- MqttEnvelope outbound comparison ---
    println!("### MqttEnvelope vs FlatMessage: outbound encode cost");
    print_header();

    for &payload_size in &payload_sizes {
        for &(version, _) in &versions {
            let flat_out = bench_outbound(version, QoS::AtMostOnce, payload_size, num_messages);
            let env_out = bench_outbound_envelope(version, payload_size, num_messages);
            print_row(&flat_out);
            print_row(&env_out);

            let delta = (1e9 / flat_out.msgs_per_sec) - (1e9 / env_out.msgs_per_sec);
            println!("  envelope outbound delta: {:.0}ns/msg", delta,);
        }
        println!();
    }

    // --- Fused decode benchmarks ---
    println!("### Fused decode: decode_publish_to_envelope + handle_publish_fused");
    print_header();

    for &payload_size in &payload_sizes {
        let native = bench_native_inbound_envelope(payload_size, num_messages);
        print_row(&native);

        for &(version, _) in &versions {
            let fused = bench_inbound_fused(version, payload_size, num_messages);
            print_row(&fused);

            // Compare with non-fused inbound.
            let non_fused = bench_inbound(version, QoS::AtMostOnce, payload_size, num_messages);
            let fused_ns = 1e9 / fused.msgs_per_sec;
            let non_fused_ns = 1e9 / non_fused.msgs_per_sec;
            println!(
                "  fused vs non-fused: {:.0}ns vs {:.0}ns (saves {:.0}ns/msg)",
                fused_ns,
                non_fused_ns,
                non_fused_ns - fused_ns,
            );
        }
        println!();
    }

    // --- Fused round-trip ---
    println!("### Fused round-trip: fused decode → handle_publish_fused → encode outbound");
    print_header();

    for &payload_size in &payload_sizes {
        let native = bench_native_roundtrip(payload_size, num_messages);
        print_row(&native);

        for &(version, _) in &versions {
            let fused_rt = bench_roundtrip_fused(version, payload_size, num_messages);
            print_row(&fused_rt);

            let non_fused_rt =
                bench_roundtrip(version, QoS::AtMostOnce, payload_size, num_messages);
            let fused_ns = 1e9 / fused_rt.msgs_per_sec;
            let non_fused_ns = 1e9 / non_fused_rt.msgs_per_sec;
            println!(
                "  fused vs non-fused roundtrip: {:.0}ns vs {:.0}ns (saves {:.0}ns/msg)",
                fused_ns,
                non_fused_ns,
                non_fused_ns - fused_ns,
            );
        }
        println!();
    }

    // --- Summary comparison ---
    println!("### Protocol overhead summary (QoS 0, 256B payload)");
    print_header();
    let native = bench_native_roundtrip(256, num_messages);
    print_row(&native);
    let r311 = bench_roundtrip(ProtocolVersion::V311, QoS::AtMostOnce, 256, num_messages);
    print_row(&r311);
    let r5 = bench_roundtrip(ProtocolVersion::V5, QoS::AtMostOnce, 256, num_messages);
    print_row(&r5);
    let f311 = bench_roundtrip_fused(ProtocolVersion::V311, 256, num_messages);
    print_row(&f311);
    let f5 = bench_roundtrip_fused(ProtocolVersion::V5, 256, num_messages);
    print_row(&f5);

    let mqtt_vs_native_311 = (1e9 / r311.msgs_per_sec) - (1e9 / native.msgs_per_sec);
    let mqtt_vs_native_5 = (1e9 / r5.msgs_per_sec) - (1e9 / native.msgs_per_sec);
    let fused_vs_native_311 = (1e9 / f311.msgs_per_sec) - (1e9 / native.msgs_per_sec);
    let fused_vs_native_5 = (1e9 / f5.msgs_per_sec) - (1e9 / native.msgs_per_sec);

    println!();
    println!(
        "  Native bisque-mq:             {:.0} msgs/sec ({:.0}ns/msg)",
        native.msgs_per_sec,
        1e9 / native.msgs_per_sec
    );
    println!(
        "  MQTT 3.1.1 round-trip:        {:.0} msgs/sec ({:.0}ns/msg, +{:.0}ns protocol cost)",
        r311.msgs_per_sec,
        1e9 / r311.msgs_per_sec,
        mqtt_vs_native_311
    );
    println!(
        "  MQTT 3.1.1 fused round-trip:  {:.0} msgs/sec ({:.0}ns/msg, +{:.0}ns protocol cost)",
        f311.msgs_per_sec,
        1e9 / f311.msgs_per_sec,
        fused_vs_native_311
    );
    println!(
        "  MQTT 5.0 round-trip:          {:.0} msgs/sec ({:.0}ns/msg, +{:.0}ns protocol cost)",
        r5.msgs_per_sec,
        1e9 / r5.msgs_per_sec,
        mqtt_vs_native_5
    );
    println!(
        "  MQTT 5.0 fused round-trip:    {:.0} msgs/sec ({:.0}ns/msg, +{:.0}ns protocol cost)",
        f5.msgs_per_sec,
        1e9 / f5.msgs_per_sec,
        fused_vs_native_5
    );

    println!("\nDone.");
}
