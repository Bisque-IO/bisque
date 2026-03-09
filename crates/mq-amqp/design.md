# bisque-mq-amqp — AMQP 1.0 Protocol Adapter

## Overview

Implements the AMQP 1.0 protocol (ISO/IEC 19464:2014) as a thin adapter over
bisque-mq's Raft-replicated topic/queue engine. AMQP 1.0 clients (Apache Qpid,
Azure Service Bus SDK, RabbitMQ AMQP 1.0 plugin clients, etc.) connect to
bisque-mq as a standard AMQP 1.0 broker.

## AMQP 1.0 vs 0-9-1

AMQP 1.0 is a fundamentally different protocol from AMQP 0-9-1:

| Aspect | AMQP 0-9-1 | AMQP 1.0 |
|--------|-----------|----------|
| Model | Exchange/Queue/Binding | Address-based (no protocol-level exchanges) |
| Multiplexing | Connection → Channel | Connection → Session → Link |
| Frame types | Method/Header/Body/Heartbeat | Performatives (open/begin/attach/transfer/etc.) |
| Type system | Field tables | Rich AMQP type system (composite types) |
| Flow control | Channel-level QoS | Link-level credit-based flow |
| Settlement | Ack/Nack per delivery tag | Disposition with delivery states |
| Wire format | Class/method dispatch | Described types with descriptors |

## Architecture

```
TCP listener  (port 5672 default)
  └─ SASL negotiation (optional)
       └─ AmqpCodec       AMQP 1.0 frame parse / serialize
            └─ AmqpConnection  connection-level performatives
                 └─ AmqpSession    session-level performatives + transfer numbering
                      └─ AmqpLink     per-link state (sender/receiver)
                           └─ MqWriteBatcher  → Raft → MqEngine
```

## Protocol Layers

### 1. Transport (Connection)
- **open**: Negotiate connection parameters (max-frame-size, channel-max, idle-timeout)
- **close**: Graceful shutdown with error condition
- Heartbeat via empty frames (idle-timeout / 2)

### 2. Session
- **begin**: Create a session (maps to a channel number)
- **end**: Terminate a session
- Transfer numbering: `next-outgoing-id`, `incoming-window`, `outgoing-window`
- Flow control at the session level

### 3. Link
- **attach**: Create a sender or receiver link to an address (topic/queue name)
- **detach**: Remove a link
- **flow**: Credit-based flow control (receiver grants credits to sender)
- **transfer**: Send a message over a link
- **disposition**: Settle deliveries (accepted, rejected, released, modified)

## Address Mapping

AMQP 1.0 uses string addresses to identify message sources and targets:

| Address Pattern | bisque-mq Entity | Role |
|----------------|-------------------|------|
| `topic/{name}` | Topic | Sender publishes, receiver subscribes |
| `queue/{name}` | Queue | Sender enqueues, receiver dequeues |
| `exchange/{name}` | Exchange | Sender publishes to exchange |
| `{name}` | Auto-detect | Resolve by trying topic, then queue |

## Message Mapping

| AMQP 1.0 Section | MessagePayload Field | Notes |
|-------------------|---------------------|-------|
| properties.message-id | message_id | Assigned on publish (raft log index) |
| properties.correlation-id | headers["correlation-id"] | Stored in headers |
| properties.subject | routing_key | For exchange routing |
| properties.creation-time | timestamp | Milliseconds |
| header.ttl | ttl_ms | Per-message TTL |
| application-properties | headers | Key-value pairs |
| data section | value (Bytes) | Message body |
| message-annotations.x-key | key | Message key for partitioning |

## Link → bisque-mq Mapping

### Sender Links (client sends to bisque-mq)
- **attach(role=sender, target=topic/X)** → Resolve topic ID, register producer
- **transfer** → `MqCommand::Publish { topic_id, messages }`
- **transfer(target=queue/X)** → `MqCommand::Enqueue { queue_id, messages }`

### Receiver Links (client receives from bisque-mq)
- **attach(role=receiver, source=queue/X)** → Register consumer, subscribe to queue
- **flow(link-credit=N)** → `MqCommand::Deliver { queue_id, consumer_id, max_count: N }`
- **disposition(accepted)** → `MqCommand::Ack { queue_id, message_ids }`
- **disposition(rejected)** → `MqCommand::Nack { queue_id, message_ids }`
- **disposition(released)** → `MqCommand::Nack` (redeliver)

### Receiver Links on Topics
- **attach(role=receiver, source=topic/X)** → Register consumer with topic subscription
- Messages delivered as they arrive (push model with credit)
- **disposition(accepted)** → `MqCommand::CommitOffset { topic_id, consumer_id, offset }`

## Settlement Modes

| AMQP Mode | Constant | bisque-mq Behavior |
|-----------|----------|-------------------|
| settled (snd-settle-mode=settled) | Pre-settled / fire-and-forget | Publish without ack |
| unsettled (snd-settle-mode=unsettled) | At-least-once | Track delivery, wait for disposition |
| mixed | Per-transfer choice | Check settled flag on each transfer |

## Flow Control

AMQP 1.0 uses **credit-based flow control** at the link level:

1. Receiver attaches with `link-credit = 0` (no messages yet)
2. Receiver sends `flow` with `link-credit = N` (grant N message credits)
3. Sender sends up to N transfers, decrementing credit
4. When credit exhausted, sender stops until more flow frames arrive

This maps naturally to `MqCommand::Deliver { max_count: link_credit }`.

## SASL Authentication

AMQP 1.0 defines SASL as a separate protocol layer before AMQP transport:

1. Client connects, sends AMQP header with protocol-id=3 (SASL)
2. Server sends `sasl-mechanisms` (PLAIN, ANONYMOUS)
3. Client sends `sasl-init` with chosen mechanism + credentials
4. Server sends `sasl-outcome` (ok/auth-error)
5. Client sends AMQP header with protocol-id=0 (AMQP transport)
6. Normal AMQP 1.0 connection proceeds

## Wire Format

### Frame Structure
```
[4 bytes: size]  [1 byte: doff]  [1 byte: type]  [2 bytes: channel]  [payload]
```
- **size**: Total frame size including header (minimum 8)
- **doff**: Data offset in 4-byte words (minimum 2 = 8 bytes header)
- **type**: 0 = AMQP frame, 1 = SASL frame
- **channel**: Session channel number (0 for connection-level)

### AMQP Type System Encoding
- **Primitive types**: null, boolean, ubyte, ushort, uint, ulong, byte, short, int,
  long, float, double, timestamp, uuid, binary, string, symbol
- **Described types**: descriptor + value (used for all performatives)
- **Composite types**: list with described type descriptor
- **Encoding**: Single-byte format codes, variable-length with size prefixes

## Open Questions

1. **Dynamic link addresses**: Should `attach` with no target/source address
   auto-create temporary queues?
2. **Durable subscriptions**: How to map AMQP 1.0 durable terminus to
   bisque-mq consumer persistence?
3. **Message annotations vs application-properties**: Which AMQP section
   carries bisque-mq headers?
4. **Multi-transfer messages**: Large messages split across multiple transfer
   frames — need frame assembly.
5. **Transaction coordinator**: AMQP 1.0 defines a transactional model with
   coordinator/controller links. Phase 2.
6. **Distribution modes**: `move` (competing consumers) vs `copy` (pub-sub)
   on source. Maps to queue vs topic.
7. **Filters on source**: AMQP 1.0 supports filter sets on receiver link
   source. Not implemented initially.
8. **Connection idle-timeout**: Send heartbeat frames to keep connection alive.
9. **SASL EXTERNAL**: Certificate-based auth integration with bisque-meta.
