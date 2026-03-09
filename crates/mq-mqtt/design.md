# bisque-mq-mqtt Design

## Overview

MQTT 3.1.1/5.0 protocol adapter for bisque-mq. Accepts raw MQTT TCP connections, decodes binary MQTT packets, and translates them into `MqCommand`/`MqResponse` interactions via the `MqWriteBatcher`. This allows standard MQTT clients (Mosquitto, Paho, MQTT.js, etc.) to publish and subscribe through bisque-mq without any custom client libraries.

## Architecture

```
MQTT Client
    |
    v
TCP Listener (MqttServer)
    |
    v
MQTT Binary Codec (MqttCodec)
    |  - Fixed header parsing
    |  - Variable-length remaining length decoding
    |  - Per-packet-type deserialization
    v
MQTT Session (MqttSession)
    |  - Per-connection state (client_id, subscriptions, will, QoS tracking)
    |  - Translates MQTT operations -> MqCommand variants
    |  - Translates MqResponse -> MQTT response packets
    v
MqWriteBatcher
    |
    v
Raft -> MqEngine (topics, queues, exchanges)
```

## MQTT -> bisque-mq Command Mapping

| MQTT Packet     | bisque-mq Operation                                                  |
|-----------------|----------------------------------------------------------------------|
| CONNECT         | RegisterConsumer (client_id as consumer group), store will message    |
| PUBLISH QoS 0   | Publish to topic (fire-and-forget)                                   |
| PUBLISH QoS 1   | Enqueue to a queue (queue-backed delivery with ACK tracking)         |
| PUBLISH QoS 2   | Enqueue with dedup_key (exactly-once via dedup window)               |
| SUBSCRIBE       | CreateBinding on a topic exchange + create per-subscription queue    |
| UNSUBSCRIBE     | DeleteBinding + optionally delete subscription queue                 |
| PINGREQ         | Heartbeat                                                            |
| DISCONNECT      | DisconnectConsumer (clean), or publish will + disconnect (unclean)   |

## QoS Mapping

### QoS 0 (At Most Once)
- Maps directly to `MqCommand::Publish` on a bisque-mq topic.
- No acknowledgment tracking. Fire-and-forget semantics.
- Subscribers receive messages through topic subscription fan-out.

### QoS 1 (At Least Once)
- Maps to `MqCommand::Enqueue` on a per-subscription queue.
- The MQTT session tracks in-flight packet IDs.
- PUBACK from the client triggers `MqCommand::Ack` on the queue.
- If the client disconnects without ACK, the message re-enters the pending state after visibility timeout.

### QoS 2 (Exactly Once)
- Maps to `MqCommand::Enqueue` with a `dedup_key` derived from the MQTT packet identifier.
- Uses the bisque-mq dedup window to prevent duplicate processing.
- Full QoS 2 handshake: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP.
- The session tracks the two-phase state per packet ID.

## Topic Wildcard Mapping

MQTT topics use `/` as a delimiter with `+` (single-level) and `#` (multi-level) wildcards. The bisque-mq exchange module (`exchange.rs`) already implements `topic_pattern_matches()` supporting both AMQP-style (`.` delimiter, `*`/`#`) and MQTT-style (`/` delimiter, `+`/`#`). MQTT topic filters are passed directly as binding routing keys on a Topic exchange.

**Examples:**
- MQTT `sensor/+/temperature` -> exchange binding with routing_key `sensor/+/temperature`
- MQTT `home/#` -> exchange binding with routing_key `home/#`

## Retained Messages

Retained messages are stored in a special bisque-mq topic named `$mqtt/retained/<topic_name>`. When a PUBLISH arrives with the retain flag set:

1. The message is published to the main topic for immediate delivery to subscribers.
2. The message is also stored (replacing any previous value) in the retained topic.
3. When a new SUBSCRIBE arrives, the session checks for retained messages matching the subscription filter and delivers them immediately.

A retained message with an empty payload clears the retained message for that topic.

## Will Messages

The CONNECT packet may include a will message (topic + payload + QoS + retain flag). The session stores the will message in memory. On unclean disconnect (TCP drop without DISCONNECT packet), the session publishes the will message to the specified topic. On clean disconnect (DISCONNECT received), the will is discarded.

## Session State

### Clean Session (MQTT 3.1.1) / Clean Start (MQTT 5.0)
- `clean_session=true`: Discard any previous session state. All subscriptions are ephemeral and removed on disconnect.
- `clean_session=false`: Restore previous subscriptions and pending QoS 1/2 messages on reconnect. The session state (subscriptions, in-flight messages, pending messages) must be persisted.

### Session Storage
- Subscription bindings persist naturally through bisque-mq exchanges and queues.
- In-flight QoS tracking is stored per-session in the MqttSession struct.
- For persistent sessions, the session metadata (client_id -> subscription list, in-flight state) can be stored in a dedicated bisque-mq topic or MDBX table.

## MQTT 5.0 Features

### Shared Subscriptions
- MQTT 5.0 `$share/<group>/<filter>` maps naturally to bisque-mq queue consumers.
- All clients in the same `<group>` share a single queue, with messages distributed round-robin or by bisque-mq's delivery strategy.

### Message Expiry
- MQTT 5.0 message expiry interval maps to `MessagePayload::ttl_ms`.
- The bisque-mq engine already supports per-message TTL.

### Topic Aliases
- Topic aliases are a session-local optimization. The codec maintains a bidirectional alias map per connection.
- Outbound: assign numeric aliases to frequently used topic strings to reduce wire overhead.
- Inbound: resolve alias numbers back to full topic strings before processing.

### Response Topic / Correlation Data
- Stored as headers in `MessagePayload::headers`.
- `response_topic` -> header key `mqtt.response_topic`
- `correlation_data` -> header key `mqtt.correlation_data`

### User Properties
- Mapped to `MessagePayload::headers` entries.

## Open Questions

1. **Session persistence across reconnects**: Should persistent session state be stored in MDBX (fast local KV) or in a dedicated bisque-mq topic (replicated via Raft)? MDBX is simpler for single-node; a replicated topic is needed for cluster-wide session migration.

2. **Shared subscription load balancing strategy**: Round-robin vs. least-loaded vs. sticky? bisque-mq queue delivery already handles this, but the mapping from `$share` group to queue consumer needs to be defined.

3. **Maximum packet size / flow control configuration**: What defaults for max packet size (MQTT 5.0 property)? How does this interact with bisque-mq's message size limits? Need configurable per-listener and per-session limits.

4. **Authentication integration with bisque auth middleware**: CONNECT username/password should flow through bisque's auth middleware. How to hook into the existing auth pipeline? Should we support MQTT 5.0 enhanced authentication (SASL-like challenge/response)?

5. **MQTT topic -> bisque topic/queue naming conventions**: Direct mapping (`sensor/temperature` -> bisque topic `sensor/temperature`) or namespaced (`mqtt/<client_group>/sensor/temperature`)? Need to avoid collisions with internal bisque topics.

6. **MQTT over WebSocket**: Should the server support MQTT over WebSocket (RFC 6455) in addition to raw TCP? This is commonly needed for browser-based MQTT clients. Could potentially share bisque's existing WebSocket infrastructure.

7. **Cluster-wide session state for client ID uniqueness**: MQTT requires that only one connection per client ID exists at any time. In a cluster, this requires distributed coordination. Options: Raft-based client ID registry, or gossip-based conflict detection with last-writer-wins.
