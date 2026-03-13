# MqCommand Wire Format v2

All integers are **little-endian**. All fixed-region fields are 8-byte aligned.

## Command Header (8 bytes)

Every command starts with:

```
@0  [size:u32]        total message size in bytes (header + fixed + flex)
@4  [fixed:u16]       byte offset where flex region starts (= fixed region size)
@6  [tag:u8]          command tag (0-52)
@7  [flags:u8]        reserved / per-command flags
```

Fixed region = bytes `[0..fixed)`. Flex region = bytes `[fixed..size)`.

## Fixed-Region Field Types

All slots are 8 bytes, ensuring every field is 8-byte aligned.

### Scalar Slots (8 bytes)

| Type | Layout | Notes |
|------|--------|-------|
| `u64` | `[value:8]` | |
| `i64` | `[value:8]` | |
| `u32` | `[value:4][_pad:4]` | or pack two u32s |
| `u32+u32` | `[a:4][b:4]` | two u32s packed |
| `i32+u32` | `[a:4][b:4]` | i32 + u32 packed |
| `u8...` | `[a:1][b:1]...[_pad]` | pack small fields, pad to 8 |

### String / Bytes Slot — `flex8` (8 bytes)

Uses **bit-0 tagging** for inline vs flex discrimination. All values little-endian.

**Small (inline, ≤ 7 bytes):**
```
byte 0:    [sssssss_0]     bit 0 = 0, bits 1-7 = length (0-7), i.e. byte = len << 1
bytes 1-7: inline data     zero-padded
```

**Large (flex reference, > 7 bytes):**
```
bytes 0-3: [(offset << 1 | 1):u32_le]    bit 0 = 1, bits 1-31 = offset from @0
bytes 4-7: [size:u32_le]                  length of data in flex region
```

**Discriminator:** `byte[0] & 1`. If 0 → inline, length = `byte[0] >> 1`. If 1 → large,
read full u32 LE, offset = `u32 >> 1` (31-bit, up to 2 GB), size from next 4 bytes.

**Optional str/bytes:** same slot — length 0 (all zeros) = `None`.

### Vector Slots (8 bytes)

All vector headers in fixed are `[count:4][offset:4]`. `offset` is from byte 0 of the command. If `count = 0`, offset is ignored.

**`vec_u64`** — fixed-size elements, direct access:
```
Fixed:  [count:4][offset:4]
Flex:   [u64_0:8][u64_1:8]...                  count elements, element i at offset + i*8
```

**`vec_bytes`** — variable-size elements, descriptor table for random access:
```
Fixed:  [count:4][offset:4]                    offset points to descriptor table

Flex:   [size0:4][offset0:4]                   element 0 descriptor  ─┐
        [size1:4][offset1:4]                   element 1 descriptor   │ count × 8 bytes
        ...                                                          ─┘
        [data0 bytes][data1 bytes]...          contiguous element data
```
Element `i`: read descriptor at `offset + i*8` → `(size, data_offset)`, data at `data_offset` for `size` bytes. O(1) random access, no intermediate allocation.

**`vec_kv`** — key-value pairs, same descriptor approach:
```
Fixed:  [count:4][offset:4]

Flex:   [key0: flex8][val0: flex8]             entry 0 descriptors  ─┐
        [key1: flex8][val1: flex8]             entry 1 descriptors   │ count × 16 bytes
        ...                                                         ─┘
        [key0_data][val0_data]...              contiguous data
```
Entry `i`: read key/val descriptors at `offset + i*16`. Each is a standard flex8 str/bytes slot.

### Blob Slot (8 bytes)

For opaque encoded types (RetentionPolicy, VariantConfig, etc.):

```
[offset:4][size:4]    offset from @0, size in bytes. size=0 means empty/None.
```

---

## Topics (0-5)

### 0 CREATE_TOPIC

```
@0  [size:4][fixed=32:2][tag=0:1][flags:1]
@8  [name: flex8]
@16 [retention: blob]
@24 [partition_count:4][_pad:4]
fixed=32
flex: [name_data?] [retention_data]
```

### 1 DELETE_TOPIC

```
@0  [size:4][fixed=16:2][tag=1:1][flags:1]
@8  [topic_id:8]
fixed=16
```

### 2 PUBLISH

```
@0  [size:4][fixed=24:2][tag=2:1][flags:1]
@8  [topic_id:8]
@16 [messages: count:4 | offset:4]                     vec_bytes
fixed=24
flex: [desc0:8][desc1:8]... [data0][data1]...
```

### 3 COMMIT_OFFSET

```
@0  [size:4][fixed=32:2][tag=3:1][flags:1]
@8  [topic_id:8]
@16 [consumer_id:8]
@24 [offset:8]
fixed=32
```

### 4 PURGE_TOPIC

```
@0  [size:4][fixed=24:2][tag=4:1][flags:1]
@8  [topic_id:8]
@16 [before_index:8]
fixed=24
```

### 5 SET_RETAINED

```
@0  [size:4][fixed=32:2][tag=5:1][flags:1]
@8  [exchange_id:8]
@16 [routing_key: flex8]
@24 [message: flex8]
fixed=32
flex: [routing_key_data?] [message_data?]
```

---

## Exchanges (6-10)

### 6 CREATE_EXCHANGE

```
@0  [size:4][fixed=16:2][tag=6:1][exchange_type:1]     exchange_type in flags byte
@8  [name: flex8]
fixed=16
flex: [name_data?]
```

### 7 DELETE_EXCHANGE

```
@0  [size:4][fixed=16:2][tag=7:1][flags:1]
@8  [exchange_id:8]
fixed=16
```

### 8 CREATE_BINDING

```
@0  [size:4][fixed=48:2][tag=8:1][no_local:1]          no_local in flags byte
@8  [exchange_id:8]
@16 [topic_id:8]
@24 [routing_key: flex8]                                opt_str (len=0 → None)
@32 [shared_group: flex8]                               opt_str (len=0 → None)
@40 [subscription_id:4][has_subscription_id:1][_pad:3]  opt_u32 (has=1 → Some)
fixed=48
flex: [routing_key_data?] [shared_group_data?]
```

### 9 DELETE_BINDING

```
@0  [size:4][fixed=16:2][tag=9:1][flags:1]
@8  [binding_id:8]
fixed=16
```

### 10 PUBLISH_TO_EXCHANGE

```
@0  [size:4][fixed=24:2][tag=10:1][flags:1]
@8  [exchange_id:8]
@16 [messages: count:4 | offset:4]                     vec_bytes
fixed=24
flex: [desc0:8][desc1:8]... [data0][data1]...
```

---

## Consumer Groups — Management (11-18)

### 11 CREATE_CONSUMER_GROUP

```
@0  [size:4][fixed=64:2][tag=11:1][flags:1]
@8  [name: flex8]
@16 [dlq_topic_name: flex8]                             opt_str (len=0 → None)
@24 [response_topic_name: flex8]                        opt_str (len=0 → None)
@32 [variant_config: blob]
@40 [topic_retention: blob]
@48 [topic_dedup: blob]                                 size=0 → None
@56 [auto_offset_reset:1][auto_create_topic:1][topic_lifetime:1][_pad:5]
fixed=64
flex: [name_data?] [dlq_data?] [resp_data?] [variant_data] [retention_data] [dedup_data?]
```

### 12 DELETE_CONSUMER_GROUP

```
@0  [size:4][fixed=16:2][tag=12:1][flags:1]
@8  [group_id:8]
fixed=16
```

### 13 JOIN_CONSUMER_GROUP

```
@0  [size:4][fixed=56:2][tag=13:1][flags:1]
@8  [group_id:8]
@16 [member_id: flex8]
@24 [client_id: flex8]
@32 [protocol_type: flex8]
@40 [session_timeout_ms:i32 | rebalance_timeout_ms:i32]
@48 [protocols: count:4 | offset:4]                    vec_kv {name:str, meta:bytes}
fixed=56
flex: [member_id_data?] [client_id_data?] [protocol_type_data?]
      [{name:flex8}{meta:flex8}]... [name0_data][meta0_data]...
```

### 14 SYNC_CONSUMER_GROUP

```
@0  [size:4][fixed=40:2][tag=14:1][flags:1]
@8  [group_id:8]
@16 [member_id: flex8]
@24 [generation:i32][_pad:4]
@32 [assignments: count:4 | offset:4]                  vec_kv {member_id:str, data:bytes}
fixed=40
flex: [member_id_data?] [{mid:flex8}{data:flex8}]... [mid0_data][data0]...
```

### 15 LEAVE_CONSUMER_GROUP

```
@0  [size:4][fixed=24:2][tag=15:1][flags:1]
@8  [group_id:8]
@16 [member_id: flex8]
fixed=24
flex: [member_id_data?]
```

### 16 HEARTBEAT_CONSUMER_GROUP

```
@0  [size:4][fixed=32:2][tag=16:1][flags:1]
@8  [group_id:8]
@16 [member_id: flex8]
@24 [generation:i32][_pad:4]
fixed=32
flex: [member_id_data?]
```

### 17 COMMIT_GROUP_OFFSET

```
@0  [size:4][fixed=56:2][tag=17:1][flags:1]
@8  [group_id:8]
@16 [topic_id:8]
@24 [offset:8]
@32 [timestamp:8]
@40 [metadata: flex8]                                   opt_str (len=0 → None)
@48 [generation:i32 | partition_index:u32]
fixed=56
flex: [metadata_data?]
```

### 18 EXPIRE_GROUP_SESSIONS

```
@0  [size:4][fixed=16:2][tag=18:1][flags:1]
@8  [now_ms:8]
fixed=16
```

---

## Consumer Groups — Ack Variant (19-29)

### 19 GROUP_DELIVER

```
@0  [size:4][fixed=32:2][tag=19:1][flags:1]
@8  [group_id:8]
@16 [consumer_id:8]
@24 [max_count:4][_pad:4]
fixed=32
```

### 20 GROUP_ACK

```
@0  [size:4][fixed=32:2][tag=20:1][flags:1]
@8  [group_id:8]
@16 [message_ids: count:4 | offset:4]                  vec_u64
@24 [response: flex8]                                   opt_bytes (len=0 → None)
fixed=32
flex: [u64_1:8][u64_2:8]... [response_data?]
```

### 21 GROUP_NACK

```
@0  [size:4][fixed=24:2][tag=21:1][flags:1]
@8  [group_id:8]
@16 [message_ids: count:4 | offset:4]                  vec_u64
fixed=24
flex: [u64_1:8][u64_2:8]...
```

### 22 GROUP_RELEASE

```
@0  [size:4][fixed=24:2][tag=22:1][flags:1]
@8  [group_id:8]
@16 [message_ids: count:4 | offset:4]                  vec_u64
fixed=24
flex: [u64_1:8][u64_2:8]...
```

### 23 GROUP_MODIFY

```
@0  [size:4][fixed=24:2][tag=23:1][flags:1]
@8  [group_id:8]
@16 [message_ids: count:4 | offset:4]                  vec_u64
fixed=24
flex: [u64_1:8][u64_2:8]...
```

### 24 GROUP_EXTEND_VISIBILITY

```
@0  [size:4][fixed=32:2][tag=24:1][flags:1]
@8  [group_id:8]
@16 [message_ids: count:4 | offset:4]                  vec_u64
@24 [extension_ms:8]
fixed=32
flex: [u64_1:8][u64_2:8]...
```

### 25 GROUP_TIMEOUT_EXPIRED

```
@0  [size:4][fixed=24:2][tag=25:1][flags:1]
@8  [group_id:8]
@16 [message_ids: count:4 | offset:4]                  vec_u64
fixed=24
flex: [u64_1:8][u64_2:8]...
```

### 26 GROUP_PUBLISH_TO_DLQ

```
@0  [size:4][fixed=40:2][tag=26:1][flags:1]
@8  [source_group_id:8]
@16 [dlq_topic_id:8]
@24 [dead_letter_ids: count:4 | offset:4]              vec_u64
@32 [messages: count:4 | offset:4]                     vec_bytes
fixed=40
flex: [u64_0:8]... [msg_desc0:8][msg_desc1:8]... [msg_data0][msg_data1]...
```

### 27 GROUP_EXPIRE_PENDING

```
@0  [size:4][fixed=24:2][tag=27:1][flags:1]
@8  [group_id:8]
@16 [message_ids: count:4 | offset:4]                  vec_u64
fixed=24
flex: [u64_1:8][u64_2:8]...
```

### 28 GROUP_PURGE

```
@0  [size:4][fixed=16:2][tag=28:1][flags:1]
@8  [group_id:8]
fixed=16
```

### 29 GROUP_GET_ATTRIBUTES

```
@0  [size:4][fixed=16:2][tag=29:1][flags:1]
@8  [group_id:8]
fixed=16
```

---

## Consumer Groups — Actor Variant (30-35)

### 30 GROUP_DELIVER_ACTOR

```
@0  [size:4][fixed=32:2][tag=30:1][flags:1]
@8  [group_id:8]
@16 [consumer_id:8]
@24 [actor_ids: count:4 | offset:4]                    vec_bytes
fixed=32
flex: [desc0:8][desc1:8]... [actor_id0][actor_id1]...
```

### 31 GROUP_ACK_ACTOR

```
@0  [size:4][fixed=32:2][tag=31:1][flags:1]
@8  [group_id:8]
@16 [message_id:8]
@24 [actor_id: flex8]
fixed=32
flex: [actor_id_data?] [response_data?]
```

Note: `opt_bytes(response)` — if present, appended after actor_id in flex. Presence signaled via flags bit.

### 32 GROUP_NACK_ACTOR

```
@0  [size:4][fixed=32:2][tag=32:1][flags:1]
@8  [group_id:8]
@16 [message_id:8]
@24 [actor_id: flex8]
fixed=32
flex: [actor_id_data?]
```

### 33 GROUP_ASSIGN_ACTORS

```
@0  [size:4][fixed=32:2][tag=33:1][flags:1]
@8  [group_id:8]
@16 [consumer_id:8]
@24 [actor_ids: count:4 | offset:4]                    vec_bytes
fixed=32
flex: [desc0:8][desc1:8]... [actor_id0][actor_id1]...
```

### 34 GROUP_RELEASE_ACTORS

```
@0  [size:4][fixed=24:2][tag=34:1][flags:1]
@8  [group_id:8]
@16 [consumer_id:8]
fixed=24
```

### 35 GROUP_EVICT_IDLE

```
@0  [size:4][fixed=24:2][tag=35:1][flags:1]
@8  [group_id:8]
@16 [before_timestamp:8]
fixed=24
```

---

## Cron (36-39)

### 36 CRON_ENABLE

```
@0  [size:4][fixed=16:2][tag=36:1][flags:1]
@8  [topic_id:8]
fixed=16
```

### 37 CRON_DISABLE

```
@0  [size:4][fixed=16:2][tag=37:1][flags:1]
@8  [topic_id:8]
fixed=16
```

### 38 CRON_TRIGGER

```
@0  [size:4][fixed=24:2][tag=38:1][flags:1]
@8  [topic_id:8]
@16 [triggered_at:8]
fixed=24
```

### 39 CRON_UPDATE

```
@0  [size:4][fixed=16:2][tag=39:1][flags:1]
@8  [topic_id:8]
fixed=16
```

---

## Sessions (40-48)

### 40 CREATE_SESSION

```
@0  [size:4][fixed=40:2][tag=40:1][flags:1]
@8  [session_id:8]
@16 [keep_alive_ms:8]
@24 [session_expiry_ms:8]
@32 [client_id: flex8]
fixed=40
flex: [client_id_data?]
```

### 41 DISCONNECT_SESSION

```
@0  [size:4][fixed=16:2][tag=41:1][publish_will:1]     publish_will in flags byte
@8  [session_id:8]
fixed=16
```

### 42 HEARTBEAT_SESSION

```
@0  [size:4][fixed=16:2][tag=42:1][flags:1]
@8  [session_id:8]
fixed=16
```

### 43 SET_WILL

```
@0  [size:4][fixed=48:2][tag=43:1][qos:2 bits | retain:1 bit | _:5 bits]
@8  [session_id:8]
@16 [topic_id:8]
@24 [routing_key: flex8]
@32 [message: flex8]
@40 [delay_secs:4][_pad:4]
fixed=48
flex: [routing_key_data?] [message_data?]
```

### 44 CLEAR_WILL

```
@0  [size:4][fixed=16:2][tag=44:1][flags:1]
@8  [session_id:8]
fixed=16
```

### 45 FIRE_PENDING_WILLS

```
@0  [size:4][fixed=16:2][tag=45:1][flags:1]
@8  [now_ms:8]
fixed=16
```

### 46 PERSIST_SESSION

```
@0  [size:4][fixed=48:2][tag=46:1][flags:1]
@8  [session_id:8]
@16 [remaining_quota:8]
@24 [client_id: flex8]
@32 [subscription_data: flex8]
@40 [session_expiry_secs:4 | inbound_qos_inflight:4]
fixed=48
flex: [client_id_data?] [subscription_data?]
```

Note: `outbound_qos1_count` (u32) needs a slot. Revised:

```
@0  [size:4][fixed=56:2][tag=46:1][flags:1]
@8  [session_id:8]
@16 [remaining_quota:8]
@24 [client_id: flex8]
@32 [subscription_data: flex8]
@40 [session_expiry_secs:4 | inbound_qos_inflight:4]
@48 [outbound_qos1_count:4 | _pad:4]
fixed=56
flex: [client_id_data?] [subscription_data?]
```

### 47 RESTORE_SESSION

```
@0  [size:4][fixed=16:2][tag=47:1][flags:1]
@8  [client_id: flex8]
fixed=16
flex: [client_id_data?]
```

### 48 EXPIRE_SESSIONS

```
@0  [size:4][fixed=16:2][tag=48:1][flags:1]
@8  [now_ms:8]
fixed=16
```

---

## Batch (49)

```
@0  [size:4][fixed=16:2][tag=49:1][flags:1]
@8  [count:4][_pad:4]
fixed=16
flex: [cmd1][cmd2]...                                   each cmd is self-sized via its header
```

Each sub-command is a complete MqCommand with its own 8-byte header. The first 4 bytes of each sub-command are its `size`, so the reader advances by `size` bytes to reach the next. Sub-commands are padded to 8-byte boundaries for alignment.

---

## Dedup (50)

### 50 PRUNE_DEDUP_WINDOW

```
@0  [size:4][fixed=24:2][tag=50:1][flags:1]
@8  [topic_id:8]
@16 [before_timestamp:8]
fixed=24
```

---

## Retained Messages (51-52)

### 51 GET_RETAINED

```
@0  [size:4][fixed=24:2][tag=51:1][flags:1]
@8  [exchange_id:8]
@16 [filter: flex8]                                     opt_str (len=0 → None)
fixed=24
flex: [filter_data?]
```

### 52 DELETE_RETAINED

```
@0  [size:4][fixed=24:2][tag=52:1][flags:1]
@8  [exchange_id:8]
@16 [routing_key: flex8]
fixed=24
flex: [routing_key_data?]
```

---

## Encoding Summary

All integers are little-endian. The `flex8` slot uses bit-0 tagging:

| Encoding | Byte layout | Discriminator |
|----------|-------------|---------------|
| **flex8 small** | `[(len<<1):u8][data:≤7]` | `byte[0] & 1 == 0` |
| **flex8 large** | `[(off<<1\|1):u32_le][size:u32_le]` | `byte[0] & 1 == 1` |
| **vec header** | `[count:u32_le][offset:u32_le]` | — |
| **blob** | `[offset:u32_le][size:u32_le]` | size=0 → empty/None |
| **scalar** | native LE, 8-byte aligned | — |

## Alignment Guarantees

- The 8-byte header ensures all fixed fields start at offset 8 (8-byte aligned).
- All fixed-region slots are 8 bytes, so every field is 8-byte aligned.
- `vec_u64` flex data starts at `offset` (stored in fixed slot). Since `fixed` is always a multiple of 8, and flex data for the first vec starts at `fixed`, u64 arrays are 8-byte aligned when the command buffer base is 8-byte aligned.
- Standalone commands from `Vec`/heap are always ≥8-byte aligned → guaranteed zero-copy transmute for `vec_u64`.
- Batch sub-commands are padded to 8-byte boundaries → sub-command bases are 8-byte aligned when the batch buffer base is → guaranteed zero-copy transmute inside batches too.
