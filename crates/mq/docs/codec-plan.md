# codec.rs Rewrite Plan

Complete rewrite of `crates/mq/src/codec.rs` for the unified storage architecture.
Removes Queue/Actor/Job/Consumer/Producer codecs. Adds Consumer Group variants (Ack, Actor), Cron, and unified Session codecs.

---

## Section 1: Module Header & Imports

```rust
use std::io::{Read, Write};
use bisque_raft::codec::{BorrowPayload, CodecError, Decode, Encode};
use bytes::Bytes;
use smallvec::SmallVec;
use crate::types::*;
```

**Removed import:** `use crate::config::{ActorConfig, JobConfig, QueueConfig};` (those types no longer exist).

---

## Section 2: Tag Aliases & Response Tag Constants

### Tag aliases (keep)
```
pub(crate) const TAG_PUBLISH: u8 = MqCommand::TAG_PUBLISH;
pub(crate) const TAG_PUBLISH_TO_EXCHANGE: u8 = MqCommand::TAG_PUBLISH_TO_EXCHANGE;
```

### Response tags (keep all 17, unchanged)
```
TAG_RESP_OK = 0 .. TAG_RESP_WILLS_FIRED = 16
```

---

## Section 3: Encoding Helpers (keep all, unchanged)

- `encode_bytes`, `decode_bytes_owned`, `decode_bytes`
- `encode_opt_bytes`, `decode_opt_bytes`, `encode_opt_bytes_ref`
- `encode_vec_bytes`, `decode_vec_bytes`
- `encode_vec_u64`, `decode_vec_u64`
- `encode_vec_opt_bytes`, `decode_vec_opt_bytes`
- `encode_opt_string`, `encode_opt_str`, `decode_opt_string`
- `encode_opt_u32`, `decode_opt_u32`

---

## Section 4: Sub-Type Codecs

### EntityType — **UPDATE** (was 5 variants, now 4)
| Tag | Old           | New             |
|-----|---------------|-----------------|
| 0   | Topic         | Topic           |
| 1   | Queue         | Exchange        |
| 2   | ActorNamespace| ConsumerGroup   |
| 3   | Job           | Session         |
| 4   | Exchange      | *(removed)*     |

### ExchangeType — **KEEP** (Fanout=0, Direct=1, Topic=2)

### RetentionPolicy — **KEEP**

### GroupVariant — **NEW**
```
Offset=0, Ack=1, Actor=2
```
Encode: `(*self as u8).encode(w)`. Decode: match on u8.

### AckVariantConfig — **NEW**
Wire: `[visibility_timeout_ms:u64][max_retries:u32][dead_letter_topic:opt_str][delay_default_ms:u64][max_in_flight_per_consumer:u32]`

### ActorVariantConfig — **NEW**
Wire: `[max_mailbox_depth:u32][idle_eviction_secs:u64][ack_timeout_ms:u64][max_retries:u32]`

### VariantConfig — **NEW**
Wire: `[tag:u8][config_fields...]`
- `0` → Offset (no additional fields)
- `1` → Ack(AckVariantConfig)
- `2` → Actor(ActorVariantConfig)

### EntityKind — **UPDATE** (was 8 variants, now 5)
| Tag | Old             | New             |
|-----|-----------------|-----------------|
| 0   | Topic           | Topic           |
| 1   | Queue           | Exchange        |
| 2   | ActorNamespace  | Binding         |
| 3   | Job             | ConsumerGroup   |
| 4   | Consumer        | Session         |
| 5   | Exchange        | *(removed)*     |
| 6   | Binding         | *(removed)*     |
| 7   | ConsumerGroup   | *(removed)*     |

### MqError — **KEEP** (unchanged, EntityKind update propagates automatically)

### DeliveredMessage — **UPDATE**
Field rename: `queue_id` → `group_id`. Wire format identical (u64 at same offset).

### EntityStats — **UPDATE** (was 4 variants, now 2)
| Tag | Old             | New             |
|-----|-----------------|-----------------|
| 0   | Topic{topic_id, message_count, head_index, tail_index} | *(same)* |
| 1   | ConsumerGroup{group_id, variant:u8, pending_count, in_flight_count, dlq_count, active_actor_count} | **NEW** |

Old Queue(1), ActorNamespace(2), Job(3) variants removed.

### REMOVED sub-type codecs
- OverlapPolicy, Subscription, RetryConfig, InputSource
- QueueConfig, ActorConfig, JobConfig, JobState

---

## Section 5: MqCommand Encode/Decode/BorrowPayload — **KEEP** (unchanged)

Passthrough encode (write buf), decode (read_to_end or zero-copy wrap), BorrowPayload returns &buf.

---

## Section 6: `build_cmd!` Macro — **KEEP** (unchanged)

---

## Section 7: MqCommand Constructors

### Topics (KEEP + UPDATE)

| Constructor | Tag | Wire Format | Notes |
|---|---|---|---|
| `create_topic(name, retention, partition_count)` | 0 | `[0][name:str][retention][partition_count:u32]` | keep |
| `delete_topic(topic_id)` | 1 | `[1][topic_id:u64]` | keep |
| `publish(topic_id, messages)` | 2 | `[2][topic_id:u64][messages:vec_bytes]` | keep (pre-sized) |
| `publish_append(existing, new_messages)` | 2 | *(merges into existing)* | keep |
| `commit_offset(topic_id, consumer_id, offset)` | 3 | `[3][topic_id:u64][consumer_id:u64][offset:u64]` | keep |
| `purge_topic(topic_id, before_index)` | 4 | `[4][topic_id:u64][before_index:u64]` | keep |
| `set_retained(topic_id, routing_key, message)` | 5 | `[5][topic_id:u64][routing_key:str][message:bytes]` | **UPDATE**: was exchange_id → topic_id |

### Exchanges (KEEP + UPDATE)

| Constructor | Tag | Wire Format | Notes |
|---|---|---|---|
| `create_exchange(name, exchange_type)` | 6 | `[6][name:str][exchange_type:u8]` | keep |
| `delete_exchange(exchange_id)` | 7 | `[7][exchange_id:u64]` | keep |
| `create_binding(exchange_id, target_topic_id, routing_key)` | 8 | `[8][exchange_id:u64][target_topic_id:u64][routing_key:opt_str][no_local:u8][shared_group:opt_str][subscription_id:opt_u32]` | **UPDATE**: param queue_id → target_topic_id |
| `create_binding_with_opts(exchange_id, target_topic_id, ...)` | 8 | *(same)* | **UPDATE**: param queue_id → target_topic_id |
| `delete_binding(binding_id)` | 9 | `[9][binding_id:u64]` | keep |
| `publish_to_exchange(exchange_id, messages)` | 10 | `[10][exchange_id:u64][messages:vec_bytes]` | keep |

### Consumer Groups (KEEP + UPDATE)

| Constructor | Tag | Wire Format | Notes |
|---|---|---|---|
| `create_consumer_group(name, auto_offset_reset, variant, source_topic_id, variant_config)` | 11 | `[11][name:str][auto_offset_reset:u8][variant:u8][source_topic_id:u64][variant_config:VariantConfig]` | **UPDATE**: added variant, source_topic_id, variant_config |
| `delete_consumer_group(group_id)` | 12 | `[12][group_id:u64]` | keep |
| `join_consumer_group(group_id, member_id, client_id, session_timeout_ms, rebalance_timeout_ms, protocol_type, protocols)` | 13 | `[13][group_id:u64][member_id:str][client_id:str][session_timeout_ms:i32][rebalance_timeout_ms:i32][protocol_type:str][count:u32][(name:str)(meta:bytes)]*` | keep |
| `sync_consumer_group(group_id, generation, member_id, assignments)` | 14 | `[14][group_id:u64][generation:i32][member_id:str][count:u32][(mid:str)(data:bytes)]*` | keep |
| `leave_consumer_group(group_id, member_id)` | 15 | `[15][group_id:u64][member_id:str]` | keep |
| `heartbeat_consumer_group(group_id, member_id, generation)` | 16 | `[16][group_id:u64][member_id:str][generation:i32]` | keep |
| `commit_group_offset(group_id, generation, topic_id, partition_index, offset, metadata, timestamp)` | 17 | `[17][group_id:u64][generation:i32][topic_id:u64][partition:u32][offset:u64][metadata:opt_str][timestamp:u64]` | keep |
| `expire_group_sessions(now_ms)` | 18 | `[18][now_ms:u64]` | keep |

### Ack Variant — **ALL NEW**

| Constructor | Tag | Wire Format |
|---|---|---|
| `group_deliver(group_id, consumer_id, max_count)` | 19 | `[19][group_id:u64][consumer_id:u64][max_count:u32]` |
| `group_ack(group_id, message_ids, response)` | 20 | `[20][group_id:u64][message_ids:vec_u64][response:opt_bytes]` |
| `group_nack(group_id, message_ids)` | 21 | `[21][group_id:u64][message_ids:vec_u64]` |
| `group_release(group_id, message_ids)` | 22 | `[22][group_id:u64][message_ids:vec_u64]` |
| `group_modify(group_id, message_ids, new_visibility_ms)` | 23 | `[23][group_id:u64][message_ids:vec_u64][new_visibility_ms:u64]` |
| `group_extend_visibility(group_id, message_ids, extension_ms)` | 24 | `[24][group_id:u64][message_ids:vec_u64][extension_ms:u64]` |
| `group_timeout_expired(group_id, message_ids)` | 25 | `[25][group_id:u64][message_ids:vec_u64]` |
| `group_publish_to_dlq(source_group_id, dlq_topic_id, dead_letter_ids, messages)` | 26 | `[26][source_group_id:u64][dlq_topic_id:u64][dead_letter_ids:vec_u64][messages:vec_bytes]` |
| `group_expire_pending(group_id, message_ids)` | 27 | `[27][group_id:u64][message_ids:vec_u64]` |
| `group_purge(group_id)` | 28 | `[28][group_id:u64]` |
| `group_get_attributes(group_id)` | 29 | `[29][group_id:u64]` |

### Actor Variant — **ALL NEW**

| Constructor | Tag | Wire Format |
|---|---|---|
| `group_deliver_actor(group_id, actor_id, consumer_id)` | 30 | `[30][group_id:u64][actor_id:bytes][consumer_id:u64]` |
| `group_ack_actor(group_id, actor_id, message_id, response)` | 31 | `[31][group_id:u64][actor_id:bytes][message_id:u64][response:opt_bytes]` |
| `group_nack_actor(group_id, actor_id, message_id)` | 32 | `[32][group_id:u64][actor_id:bytes][message_id:u64]` |
| `group_assign_actors(group_id, consumer_id, actor_ids)` | 33 | `[33][group_id:u64][consumer_id:u64][actor_ids:vec_bytes]` |
| `group_release_actors(group_id, consumer_id)` | 34 | `[34][group_id:u64][consumer_id:u64]` |
| `group_evict_idle(group_id, before_timestamp)` | 35 | `[35][group_id:u64][before_timestamp:u64]` |

### Cron — **ALL NEW**

| Constructor | Tag | Wire Format |
|---|---|---|
| `cron_enable(topic_id)` | 36 | `[36][topic_id:u64]` |
| `cron_disable(topic_id)` | 37 | `[37][topic_id:u64]` |
| `cron_trigger(topic_id, triggered_at)` | 38 | `[38][topic_id:u64][triggered_at:u64]` |
| `cron_update(topic_id, cron_expression, timezone, max_pending)` | 39 | `[39][topic_id:u64][cron_expression:str][timezone:str][max_pending:u32]` |

### Sessions — **NEW** (replaces old Consumer/Producer/MQTT commands)

| Constructor | Tag | Wire Format |
|---|---|---|
| `create_session(session_id, client_id, keep_alive_ms, session_expiry_ms)` | 40 | `[40][session_id:u64][client_id:str][keep_alive_ms:u64][session_expiry_ms:u64]` |
| `disconnect_session(session_id)` | 41 | `[41][session_id:u64]` |
| `heartbeat_session(session_id)` | 42 | `[42][session_id:u64]` |
| `set_will(session_id, topic_id, delay_ms, retained, payload)` | 43 | `[43][session_id:u64][topic_id:u64][delay_ms:u64][retained:u8][payload:bytes]` |
| `clear_will(session_id)` | 44 | `[44][session_id:u64]` |
| `fire_pending_wills(now_ms)` | 45 | `[45][now_ms:u64]` |
| `persist_session(session_id, client_id, session_expiry_ms, subscription_data)` | 46 | `[46][session_id:u64][client_id:str][session_expiry_ms:u64][subscription_data:bytes]` |
| `restore_session(client_id)` | 47 | `[47][client_id:str]` |
| `expire_sessions(now_ms)` | 48 | `[48][now_ms:u64]` |

### Batch — **KEEP**

| Constructor | Tag | Wire Format |
|---|---|---|
| `batch(commands)` | 49 | `[49][count:u32][(len:u32)(cmd_bytes)]*` |

### Topic Dedup — **UPDATE**

| Constructor | Tag | Wire Format | Notes |
|---|---|---|---|
| `prune_dedup_window(topic_id, before_timestamp)` | 50 | `[50][topic_id:u64][before_timestamp:u64]` | was queue_id → topic_id |

### REMOVED Constructors
All of: `create_queue`, `delete_queue`, `enqueue`, `deliver` (old queue), `ack` (old queue), `nack` (old queue), `extend_visibility` (old queue), `timeout_expired` (old queue), `publish_to_dlq` (old queue), `prune_dedup_window` (old queue), `expire_pending_messages`, `purge_queue`, `get_queue_attributes`, `create_actor_namespace`, `delete_actor_namespace`, `send_to_actor`, `deliver_actor_message`, `ack_actor_message`, `nack_actor_message`, `assign_actors` (old), `release_actors` (old), `evict_idle_actors`, `create_job`, `delete_job`, `update_job`, `enable_job`, `disable_job`, `trigger_job`, `assign_job`, `complete_job`, `fail_job`, `timeout_job`, `register_consumer`, `disconnect_consumer`, `heartbeat` (old consumer), `register_producer`, `disconnect_producer`, `delete_retained`, `get_retained`, `mark_received`, `mark_released`, `multi_deliver`, `multi_ack`, `set_topic_alias`, `clear_topic_aliases`, `cancel_pending_will`, `register_publisher_session`, `qos2_register_inbound`, `qos2_complete_inbound`, `expire_group_offsets`

---

## Section 8: View Accessor Methods on MqCommand

One `as_*(&self) -> CmdFoo` method per view struct. Each clones `self.buf` into the view.

### Keep (with updates)
- `as_create_topic` → `CmdCreateTopic`
- `as_publish` → `CmdPublish`
- `as_create_exchange` → `CmdCreateExchange`
- `as_create_binding` → `CmdCreateBinding` (accessor renamed: `queue_id()` → `target_topic_id()`)
- `as_publish_to_exchange` → `CmdPublishToExchange`
- `as_create_consumer_group` → `CmdCreateConsumerGroup` (new fields)
- `as_commit_group_offset` → `CmdCommitGroupOffset`
- `as_join_consumer_group` → `CmdJoinConsumerGroup`
- `as_sync_consumer_group` → `CmdSyncConsumerGroup`
- `as_leave_consumer_group` → `CmdLeaveConsumerGroup`
- `as_heartbeat_consumer_group` → `CmdHeartbeatConsumerGroup`
- `as_batch` → `CmdBatch`
- `as_set_retained` → `CmdSetRetained` (accessor renamed: `exchange_id()` → `topic_id()`)

### New
- `as_group_ack` → `CmdGroupAck`
- `as_group_nack` → `CmdGroupNack`
- `as_group_release` → `CmdGroupRelease`
- `as_group_modify` → `CmdGroupModify`
- `as_group_extend_visibility` → `CmdGroupExtendVisibility`
- `as_group_timeout_expired` → `CmdGroupTimeoutExpired`
- `as_group_publish_to_dlq` → `CmdGroupPublishToDlq`
- `as_group_expire_pending` → `CmdGroupExpirePending`
- `as_group_deliver_actor` → `CmdGroupDeliverActor`
- `as_group_ack_actor` → `CmdGroupAckActor`
- `as_group_nack_actor` → `CmdGroupNackActor`
- `as_group_assign_actors` → `CmdGroupAssignActors`
- `as_cron_update` → `CmdCronUpdate`
- `as_create_session` → `CmdCreateSession`
- `as_set_will` → `CmdSetWill` (new format)
- `as_persist_session` → `CmdPersistSession` (new format)
- `as_restore_session` → `CmdRestoreSession`

### Keep (no view struct needed — use `cmd.field_u64(1)` directly)
These commands have fixed-layout fields readable with `field_u64`/`field_u32`:
- `delete_topic`, `commit_offset`, `purge_topic`
- `delete_exchange`, `delete_binding`
- `delete_consumer_group`, `expire_group_sessions`
- `group_deliver` (group_id=field_u64(1), consumer_id=field_u64(9), max_count=field_u32(17))
- `group_purge`, `group_get_attributes`
- `group_release_actors`, `group_evict_idle`
- `cron_enable`, `cron_disable`, `cron_trigger`
- `disconnect_session`, `heartbeat_session`, `clear_will`
- `fire_pending_wills`, `expire_sessions`
- `prune_dedup_window`

### Removed
All old Queue/Actor/Job/Consumer/Producer view accessors and their structs:
`as_create_queue`, `as_enqueue`, `as_ack` (old), `as_nack` (old), `as_extend_visibility` (old), `as_timeout_expired` (old), `as_publish_to_dlq` (old), `as_expire_pending_messages`, `as_create_actor_namespace`, `as_send_to_actor`, `as_deliver_actor_message`, `as_ack_actor_message`, `as_nack_actor_message`, `as_assign_actors` (old), `as_create_job`, `as_update_job`, `as_fail_job`, `as_register_consumer`, `as_register_producer`, `as_delete_retained`, `as_get_retained`, `as_mark_received`, `as_mark_released`, `as_multi_deliver`, `as_multi_ack`, `as_set_topic_alias`, `as_cancel_pending_will`, `as_register_publisher_session`, `as_qos2_inbound`

### Keep (message helpers on MqCommand)
- `publish_messages()` → `Option<FlatMessages>`
- `publish_messages_for_topic(topic_id)` → `Option<FlatMessages>`

---

## Section 9: View Structs

### Kept (with updates noted)

| Struct | Layout | Changes |
|---|---|---|
| `CmdCreateTopic` | `[0][name:str][retention][partition_count:u32]` | none |
| `CmdPublish` | `[2][topic_id:u64][count:u32][msgs...]` | none |
| `CmdCreateExchange` | `[6][name:str][exchange_type:u8]` | none |
| `CmdCreateBinding` | `[8][exchange_id:u64][target_topic_id:u64][routing_key:opt_str][no_local:u8][shared_group:opt_str][subscription_id:opt_u32]` | **accessor rename**: `queue_id()` → `target_topic_id()` |
| `CmdPublishToExchange` | `[10][exchange_id:u64][count:u32][msgs...]` | none |
| `CmdCreateConsumerGroup` | `[11][name:str][auto_offset_reset:u8][variant:u8][source_topic_id:u64][variant_config:VariantConfig]` | **added**: `variant()`, `source_topic_id()`, `variant_config()` |
| `CmdCommitGroupOffset` | `[17][group_id:u64][generation:i32][topic_id:u64][partition:u32][offset:u64][metadata:opt_str][timestamp:u64]` | none |
| `CmdJoinConsumerGroup` | (complex, multi-string) | none |
| `CmdSyncConsumerGroup` | (complex, multi-string) | none |
| `CmdLeaveConsumerGroup` | `[15][group_id:u64][member_id:str]` | none |
| `CmdHeartbeatConsumerGroup` | `[16][group_id:u64][member_id:str][generation:i32]` | none |
| `CmdSetRetained` | `[5][topic_id:u64][routing_key:str][message:bytes]` | **accessor rename**: `exchange_id()` → `topic_id()` |
| `CmdRestoreSession` | `[47][client_id:str]` | none (same wire format) |
| `CmdBatch` | `[49][count:u32][(len:u32)(cmd)]*` | none |

### New view structs

| Struct | Layout | Accessors |
|---|---|---|
| `CmdGroupAck` | `[20][group_id:u64][message_ids:vec_u64][response:opt_bytes]` | `group_id()`, `message_ids()`, `response()` |
| `CmdGroupNack` | `[21][group_id:u64][message_ids:vec_u64]` | `group_id()`, `message_ids()` |
| `CmdGroupRelease` | `[22][group_id:u64][message_ids:vec_u64]` | `group_id()`, `message_ids()` |
| `CmdGroupModify` | `[23][group_id:u64][message_ids:vec_u64][new_visibility_ms:u64]` | `group_id()`, `message_ids()`, `new_visibility_ms()` |
| `CmdGroupExtendVisibility` | `[24][group_id:u64][message_ids:vec_u64][extension_ms:u64]` | `group_id()`, `message_ids()`, `extension_ms()` |
| `CmdGroupTimeoutExpired` | `[25][group_id:u64][message_ids:vec_u64]` | `group_id()`, `message_ids()` |
| `CmdGroupPublishToDlq` | `[26][source_group_id:u64][dlq_topic_id:u64][dead_letter_ids:vec_u64][messages:vec_bytes]` | `source_group_id()`, `dlq_topic_id()`, `dead_letter_ids()`, `messages()` |
| `CmdGroupExpirePending` | `[27][group_id:u64][message_ids:vec_u64]` | `group_id()`, `message_ids()` |
| `CmdGroupDeliverActor` | `[30][group_id:u64][actor_id:bytes][consumer_id:u64]` | `group_id()`, `actor_id()`, `consumer_id()` |
| `CmdGroupAckActor` | `[31][group_id:u64][actor_id:bytes][message_id:u64][response:opt_bytes]` | `group_id()`, `actor_id()`, `message_id()`, `response()` |
| `CmdGroupNackActor` | `[32][group_id:u64][actor_id:bytes][message_id:u64]` | `group_id()`, `actor_id()`, `message_id()` |
| `CmdGroupAssignActors` | `[33][group_id:u64][consumer_id:u64][actor_ids:vec_bytes]` | `group_id()`, `consumer_id()`, `actor_ids()` |
| `CmdCronUpdate` | `[39][topic_id:u64][cron_expression:str][timezone:str][max_pending:u32]` | `topic_id()`, `cron_expression()`, `timezone()`, `max_pending()` |
| `CmdCreateSession` | `[40][session_id:u64][client_id:str][keep_alive_ms:u64][session_expiry_ms:u64]` | `session_id()`, `client_id()`, `keep_alive_ms()`, `session_expiry_ms()` |
| `CmdSetWill` | `[43][session_id:u64][topic_id:u64][delay_ms:u64][retained:u8][payload:bytes]` | `session_id()`, `topic_id()`, `delay_ms()`, `retained()`, `payload()` |
| `CmdPersistSession` | `[46][session_id:u64][client_id:str][session_expiry_ms:u64][subscription_data:bytes]` | `session_id()`, `client_id()`, `session_expiry_ms()`, `subscription_data()` |

### Removed view structs
`CmdCreateQueue`, `CmdEnqueue`, `CmdAck` (old), `CmdNack` (old), `CmdExtendVisibility` (old), `CmdTimeoutExpired` (old), `CmdPublishToDlq` (old), `CmdExpirePendingMessages`, `CmdCreateActorNamespace`, `CmdSendToActor`, `CmdDeliverActorMessage`, `CmdAckActorMessage`, `CmdNackActorMessage`, `CmdAssignActors` (old), `CmdCreateJob`, `CmdUpdateJob`, `CmdFailJob`, `CmdRegisterConsumer`, `CmdRegisterProducer`, `CmdDeleteRetained`, `CmdGetRetained`, `CmdSetTopicAlias`, `CmdCancelPendingWill`, `CmdRegisterPublisherSession`, `CmdQos2Inbound`, `CmdMarkReceived`, `CmdMarkReleased`, `CmdMultiDeliver`, `CmdMultiAck`

---

## Section 10: Iterators — **KEEP** (unchanged)

- `FlatMessages` — zero-copy message iterator (update doc comment: "FlatMqCommand" → "MqCommand")
- `FlatOptBytes` — zero-copy Option<Bytes> iterator
- `BatchIter` — zero-copy sub-command iterator

---

## Section 11: `fmt_mq_command` — **REWRITE**

Display formatter for all 51 tags (0-50). Match on `cmd.tag()`:

### Topics (0-5)
- 0: `CreateTopic({name})`
- 1: `DeleteTopic({topic_id})`
- 2: `Publish(topic={topic_id}, count={count})`
- 3: `CommitOffset(topic={}, consumer={}, offset={})`
- 4: `PurgeTopic(topic={}, before={})`
- 5: `SetRetained(topic={}, rk={})`

### Exchanges (6-10)
- 6: `CreateExchange({name})`
- 7: `DeleteExchange({id})`
- 8: `CreateBinding(exchange={}, target_topic={})`
- 9: `DeleteBinding({id})`
- 10: `PublishToExchange(exchange={})`

### Consumer Groups (11-18)
- 11: `CreateConsumerGroup(name={}, variant={:?})`
- 12: `DeleteConsumerGroup({id})`
- 13: `JoinConsumerGroup(group={}, member={}, client={})`
- 14: `SyncConsumerGroup(group={}, gen={}, member={})`
- 15: `LeaveConsumerGroup(group={}, member={})`
- 16: `HeartbeatConsumerGroup(group={}, member={}, gen={})`
- 17: `CommitGroupOffset(group={}, gen={}, topic={}, part={}, offset={})`
- 18: `ExpireGroupSessions(now={})`

### Ack Variant (19-29)
- 19: `GroupDeliver(group={}, consumer={}, max={})`
- 20: `GroupAck(group={}, count={})`
- 21: `GroupNack(group={}, count={})`
- 22: `GroupRelease(group={}, count={})`
- 23: `GroupModify(group={}, count={})`
- 24: `GroupExtendVisibility(group={})`
- 25: `GroupTimeoutExpired(group={})`
- 26: `GroupPublishToDlq(src_group={}, dlq_topic={})`
- 27: `GroupExpirePending(group={})`
- 28: `GroupPurge(group={})`
- 29: `GroupGetAttributes(group={})`

### Actor Variant (30-35)
- 30: `GroupDeliverActor(group={})`
- 31: `GroupAckActor(group={})`
- 32: `GroupNackActor(group={})`
- 33: `GroupAssignActors(group={}, consumer={})`
- 34: `GroupReleaseActors(group={}, consumer={})`
- 35: `GroupEvictIdle(group={})`

### Cron (36-39)
- 36: `CronEnable(topic={})`
- 37: `CronDisable(topic={})`
- 38: `CronTrigger(topic={})`
- 39: `CronUpdate(topic={})`

### Sessions (40-48)
- 40: `CreateSession(session={}, client={})`
- 41: `DisconnectSession({})`
- 42: `HeartbeatSession({})`
- 43: `SetWill(session={}, topic={})`
- 44: `ClearWill(session={})`
- 45: `FirePendingWills(now={})`
- 46: `PersistSession(session={}, client={})`
- 47: `RestoreSession(client={})`
- 48: `ExpireSessions(now={})`

### Batch + Dedup (49-50)
- 49: `Batch(count={})`
- 50: `PruneDedupWindow(topic={})`

---

## Section 12: MqResponse Encode/Decode — **UPDATE**

### Field changes in MqResponse variants

| Variant | Old Wire | New Wire |
|---|---|---|
| `WillPending` | `[11][consumer_id:u64][delay_secs:u32]` | `[11][session_id:u64][delay_ms:u64]` |
| `SessionRestored` | `[12][consumer_id:u64][session_expiry_secs:u32][subscription_data:bytes]` | `[12][session_id:u64][session_expiry_ms:u64][subscription_data:bytes]` |
| `MultiMessages` | `queues` field name | `groups` field name (wire identical) |

### encoded_size changes
- `WillPending`: was `8 + 4 = 12` → now `8 + 8 = 16`
- `SessionRestored`: was `8 + 4 + 4 + data.len()` → now `8 + 8 + 4 + data.len()`

### EntityStats Decode change
Old decoded tags 0-3 (Topic, Queue, ActorNamespace, Job). New decodes tags 0-1 (Topic, ConsumerGroup).
ConsumerGroup variant wire: `[1][group_id:u64][variant:u8][pending_count:u64][in_flight_count:u64][dlq_count:u64][active_actor_count:u64]`

---

## Section 13: Tests — **REWRITE**

### Keep (unchanged or trivially updated)
- `publish_roundtrip`
- `create_topic_roundtrip`
- `publish_messages_zero_copy`
- `publish_messages_for_topic_filter`
- `non_publish_returns_none`
- `response_roundtrips` (update EntityKind::Topic, no Queue)
- `consumer_group_response_roundtrips`
- `test_cg_codec_join_many_protocols`
- `test_cg_codec_sync_many_assignments`
- `test_cg_codec_commit_with_long_metadata`
- `test_cg_codec_unicode_metadata`
- `test_cg_codec_empty_strings`
- `test_publish_presized_buffer`
- `test_publish_append_merges_messages`
- `test_publish_append_single_message`
- `test_publish_append_preserves_keys_and_headers`

### Update
- `batch_roundtrip` — replace `heartbeat(99)` with `heartbeat_session(99)`
- `create_binding_view` — `queue_id()` → `target_topic_id()`
- `consumer_group_command_roundtrips` — add variant/source_topic_id/variant_config to create_consumer_group
- `all_simple_variants_roundtrip` — rewrite with new command set
- `display_format` — rewrite with new commands
- `test_cg_codec_batch_roundtrip` — update with new commands

### Remove
- `enqueue_roundtrip`
- `ack_view`
- `nack_view`
- `send_to_actor_view`
- `fail_job_view`
- `register_consumer_view`

### Add
- `group_ack_roundtrip` — test CmdGroupAck view
- `group_deliver_actor_roundtrip` — test CmdGroupDeliverActor view
- `cron_update_roundtrip` — test CmdCronUpdate view
- `session_roundtrips` — test create_session, set_will, persist_session views
- `variant_config_roundtrip` — test VariantConfig Encode/Decode
- `entity_stats_consumer_group_roundtrip` — test new EntityStats::ConsumerGroup

---

## Execution Order

Write the file in one pass (Write tool), structured in this order:
1. Module doc + imports
2. Tag aliases + response tag constants
3. Encoding helpers
4. Sub-type codecs (EntityType, ExchangeType, RetentionPolicy, GroupVariant, AckVariantConfig, ActorVariantConfig, VariantConfig, EntityKind, MqError, DeliveredMessage, EntityStats)
5. MqCommand Encode/Decode/BorrowPayload + build_cmd!
6. MqCommand constructors (Topics → Exchanges → Consumer Groups → Ack → Actor → Cron → Sessions → Batch → Dedup)
7. View accessor methods
8. View structs
9. Iterators (BatchIter, FlatOptBytes, FlatMessages)
10. fmt_mq_command
11. MqResponse Encode/Decode
12. Tests
