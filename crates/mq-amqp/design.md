# bisque-mq-amqp Design

## Overview

AMQP 0-9-1 protocol adapter that translates AMQP frames into bisque-mq
`MqCommand` / `MqResponse` interactions via the `MqWriteBatcher`. This allows
any standard AMQP 0-9-1 client (e.g. RabbitMQ client libraries, Pika, amqplib)
to produce and consume messages through bisque-mq without modification.

## Architecture

```
AMQP Client
    │
    ▼
TCP Listener (AmqpServer)
    │
    ▼
AMQP Frame Codec (tokio codec: AmqpCodec)
    │  ┌──────────────────────────────┐
    ▼  │                              │
AmqpConnection                        │
    │  ├─ Channel 0: connection ctrl  │
    │  ├─ Channel 1: AmqpChannel      │
    │  ├─ Channel 2: AmqpChannel      │
    │  └─ ...                         │
    │                                 │
    ▼                                 │
MqWriteBatcher ────► Raft ────► MqEngine
```

1. **TCP Listener** — `AmqpServer` binds a socket and accepts connections.
2. **Frame Codec** — `AmqpCodec` implements a tokio-style `Encoder`/`Decoder`
   that reads/writes raw AMQP 0-9-1 frames (7-byte header + payload + frame-end
   marker).
3. **Connection** — `AmqpConnection` owns the TCP stream, manages the
   connection handshake, heartbeats, and a map of open channels.
4. **Channel** — `AmqpChannel` holds per-channel state (consumers, unacked
   deliveries, QoS prefetch, tx state) and dispatches decoded method frames to
   the appropriate `MqCommand`.
5. **MqWriteBatcher** — Each channel submits commands through the shared
   `MqWriteBatcher`, which coalesces them into Raft proposals.

## AMQP → bisque-mq Mapping

| AMQP Method | bisque-mq Command / Action |
|---|---|
| `connection.start` / `tune` / `open` | Protocol handshake + auth; negotiated params stored in `AmqpConnection` |
| `connection.close` | Tear down all channels, `DisconnectConsumer` for each active consumer |
| `channel.open` | Create `AmqpChannel` in the connection's channel map |
| `channel.close` | Destroy channel, clean up consumers/unacked state |
| `exchange.declare` | `MqCommand::CreateExchange { name, exchange_type }` |
| `exchange.delete` | `MqCommand::DeleteExchange { exchange_id }` |
| `queue.declare` | `MqCommand::CreateQueue { name, config }` |
| `queue.delete` | `MqCommand::DeleteQueue { queue_id }` |
| `queue.bind` | `MqCommand::CreateBinding { exchange_id, queue_id, routing_key }` |
| `queue.unbind` | `MqCommand::DeleteBinding { binding_id }` |
| `queue.purge` | `MqCommand::PurgeQueue { queue_id }` |
| `basic.publish` | `MqCommand::PublishToExchange { exchange_id, messages }` (when exchange is named) or `MqCommand::Enqueue { queue_id, messages }` (default exchange routes by queue name) |
| `basic.consume` | `MqCommand::RegisterConsumer { consumer_id, group_name, subscriptions }` |
| `basic.cancel` | `MqCommand::DisconnectConsumer { consumer_id }` |
| `basic.deliver` | Pushed from server to client (not a command — we generate these frames) |
| `basic.ack` | `MqCommand::Ack { queue_id, message_ids }` |
| `basic.nack` | `MqCommand::Nack { queue_id, message_ids }` with optional requeue |
| `basic.reject` | `MqCommand::Nack { queue_id, message_ids }` (single message) |
| `basic.get` | `MqCommand::Deliver { queue_id, consumer_id, max_count: 1 }` |
| `basic.qos` (prefetch) | Sets `QueueConfig::max_in_flight_per_consumer` for the channel's consumer |
| `tx.select` | Begin accumulating commands in a batch buffer |
| `tx.commit` | `MqCommand::Batch(accumulated_commands)` |
| `tx.rollback` | Discard accumulated batch buffer |

## Exchange Types

The three exchange types are already implemented in `bisque_mq::exchange`:

- **direct** — routes to queues whose binding key exactly matches the routing key
- **fanout** — routes to all bound queues regardless of routing key
- **topic** — routes using AMQP-style wildcard matching (`*` = one word, `#` = zero or more)

## Default Exchanges

Created automatically when a virtual host / namespace is initialized:

| Name | Type | Behaviour |
|---|---|---|
| `""` (empty string) | direct | Routes to the queue whose name equals the routing key |
| `amq.direct` | direct | Standard direct exchange |
| `amq.fanout` | fanout | Standard fanout exchange |
| `amq.topic` | topic | Standard topic exchange |

## Message Properties

AMQP `basic-properties` are mapped to/from `MessagePayload`:

| AMQP Property | bisque-mq Field |
|---|---|
| `content-type` | `headers[("content-type", ...)]` |
| `content-encoding` | `headers[("content-encoding", ...)]` |
| `delivery-mode` | `headers[("x-delivery-mode", ...)]` (1 = transient, 2 = persistent) |
| `priority` | `headers[("x-priority", ...)]` |
| `correlation-id` | `headers[("correlation-id", ...)]` |
| `reply-to` | `headers[("reply-to", ...)]` |
| `expiration` | `ttl_ms` (parsed from string milliseconds) |
| `message-id` | `headers[("message-id", ...)]` |
| `timestamp` | `timestamp` |
| `type` | `headers[("type", ...)]` |
| `user-id` | `headers[("user-id", ...)]` |
| `app-id` | `headers[("app-id", ...)]` |
| `headers` (field table) | Remaining entries merged into `headers` |

## Consumer Lifecycle

1. Client sends `basic.consume` with queue name and consumer tag.
2. Adapter calls `MqCommand::RegisterConsumer` and records the subscription.
3. Engine pushes delivered messages; adapter wraps each in a `basic.deliver`
   frame and sends it on the channel.
4. Client sends `basic.ack` / `basic.nack` / `basic.reject`.
5. Adapter translates to `MqCommand::Ack` or `MqCommand::Nack`.
6. On `basic.cancel` or channel close, adapter sends
   `MqCommand::DisconnectConsumer`.

## Channel Multiplexing

AMQP allows multiple logical channels over a single TCP connection. Each
channel has its own:

- Consumer set (consumer_tag → consumer_id mapping)
- Unacked delivery tracking (delivery_tag → (queue_id, message_id))
- QoS prefetch settings
- Transaction buffer (if tx.select has been issued)

Channel 0 is reserved for connection-level control frames.

## Open Questions / Things to Figure Out

- **AMQP frame max size negotiation**: During `connection.tune` we propose a
  frame-max. Need to decide the default (RabbitMQ uses 131072). Must enforce
  this when encoding content body frames — split large bodies into multiple
  frames at the negotiated boundary.

- **Heartbeat negotiation**: AMQP defines its own heartbeat mechanism in
  `connection.tune`. bisque-mq already has `MqCommand::Heartbeat`. Decide
  whether to use AMQP heartbeats at the TCP layer (to detect dead connections)
  and separately issue bisque `Heartbeat` commands for the engine's consumer
  liveness tracking.

- **Whether to support AMQP transactions (`tx.select`) or just batches**:
  AMQP transactions guarantee atomicity of publishes + acks within a commit.
  We can map `tx.commit` to `MqCommand::Batch`, but true rollback semantics
  for acks are not currently supported in bisque-mq. We may want to only
  support publisher confirms as an alternative.

- **Virtual host mapping to bisque namespaces/groups**: AMQP `connection.open`
  includes a virtual-host field (`/` by default). Decide whether to map each
  vhost to a separate bisque-mq Raft group, a namespace prefix, or ignore it
  initially.

- **Header exchange and consistent-hash exchange support**: These are RabbitMQ
  extensions not part of core AMQP 0-9-1. Defer or add new `ExchangeType`
  variants.

- **Dead letter exchange integration**: AMQP supports `x-dead-letter-exchange`
  and `x-dead-letter-routing-key` as queue arguments. bisque-mq has
  `dead_letter_queue_id` in `QueueConfig`. Need to bridge: on DLQ, resolve the
  DLX exchange name to an exchange_id and route through it.

- **Message TTL per-queue vs per-message**: AMQP supports both `x-message-ttl`
  (queue argument, applies to all messages) and per-message `expiration`
  property. bisque-mq has `MessagePayload::ttl_ms` for per-message. Need to
  implement queue-level TTL as well.

- **Queue mirroring / HA policy**: In RabbitMQ, `x-ha-policy` controls
  mirroring. In bisque-mq, all queues are already replicated via Raft, so
  this argument can be accepted and ignored (or used to control Raft group
  placement).

- **Exclusive / auto-delete queue semantics**: `exclusive` queues are visible
  only to the declaring connection and deleted when it closes. `auto-delete`
  queues are deleted when the last consumer disconnects. These require
  connection/channel lifecycle hooks.

- **Authentication**: AMQP uses SASL mechanisms in `connection.start-ok`.
  Support PLAIN (username/password) initially, mapped to bisque auth tokens.
  EXTERNAL (TLS client certs) can come later.

- **Publisher confirms vs AMQP transactions**: Publisher confirms
  (`confirm.select`) are a RabbitMQ extension that provides per-message
  acknowledgement from the broker. Simpler and more performant than
  `tx.select/commit`. Since `MqWriteBatcher::submit` already returns a
  response per command, publisher confirms are a natural fit.
