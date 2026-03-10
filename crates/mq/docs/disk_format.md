# Disk Format: Per-Topic Dense Offsets & Segment Indexing

## Motivation

Raft log indexes are global, sparse, and shared across all entity types. For a
message queue, consumers need **per-topic dense monotonic offsets** (0, 1, 2, …)
independent of the raft log. The raft log entry is just an atomic unit for
replication — the segment files themselves ARE the data store, byte-identical
across all replicas.

This design introduces:
1. **Per-topic dense offsets** — each topic has its own monotonic sequence
2. **`MessageLocation`** — packed `segment_id:32 | segment_offset:32` for O(1)
   message lookup (8 bytes per message)
3. **Per-segment index file (`.sidx`)** — written at seal time, mmap-able,
   zero-copy format for all entity types

## MessageLocation

A packed `u64` that encodes the physical location of a message within the
segment storage:

```
 63                32 31                 0
+-------------------+-------------------+
|    segment_id     |  segment_offset   |
|     (32 bits)     |    (32 bits)      |
+-------------------+-------------------+
```

- **segment_id** (u32): Identifies the segment file (max ~4 billion segments)
- **segment_offset** (u32): Byte offset within the segment (max 4 GiB per segment,
  well above the typical 1–64 MiB segment size)

This gives O(1) lookup: `mmap[segment_id][segment_offset..+len]`.

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct MessageLocation(u64);

impl MessageLocation {
    pub fn new(segment_id: u32, offset: u32) -> Self;
    pub fn segment_id(self) -> u32;  // (self.0 >> 32) as u32
    pub fn offset(self) -> u32;      // self.0 as u32
}
```

## Per-Topic Offset Index

Each `TopicState` maintains a dense offset → location map:

```rust
pub struct TopicState {
    pub meta: TopicMeta,
    // ...existing fields...

    /// Dense offset → physical location. Index into this vec is
    /// (offset - tail_offset). Offsets are topic-local, starting from 0.
    offset_index: Vec<MessageLocation>,
}
```

`TopicMeta` fields change meaning:
- `head_index` → **next_offset**: the next offset to assign (one past the last
  assigned offset). Renamed semantically but kept as the same field for serde
  compat.
- `tail_index` → **tail_offset**: the lowest available offset (purged offsets
  are below this).
- `message_count` stays the same.

## apply_publish Changes

Current (broken for batches — all messages get same raft log index as offset):

```rust
fn apply_publish(&mut self, log_index: u64, messages: &[Bytes]) -> SmallVec<[u64; 16]> {
    let offset = log_index;
    for _ in messages { offsets.push(offset); }  // ALL IDENTICAL
}
```

New — assigns dense per-topic offsets and records physical locations:

```rust
fn apply_publish(
    &mut self,
    segment_id: u32,
    segment_offset: u32,
    record_data_offset: u32,  // offset within the record's data region
    messages: &[Bytes],
) -> SmallVec<[u64; 16]> {
    let base_offset = self.meta.head_index; // next_offset
    for (i, msg) in messages.iter().enumerate() {
        let topic_offset = base_offset + i as u64;
        let msg_offset = record_data_offset + /* accumulated offset within batch */;
        let loc = MessageLocation::new(segment_id, msg_offset);
        self.offset_index.push(loc);
        offsets.push(topic_offset);
    }
    self.meta.head_index = base_offset + messages.len() as u64;
    if self.meta.message_count == 0 {
        self.meta.tail_index = base_offset;
    }
    self.meta.message_count += messages.len() as u64;
}
```

## Segment Index File (.sidx)

Written once when a segment is sealed. Mmap-able, zero-copy, flat format.

### File Layout

```
[Header: 32 bytes]
  magic:            [u8; 4]     b"SIDX"
  version:          u32 LE      1
  segment_id:       u32 LE
  entity_count:     u32 LE      number of entity sections
  reserved:         [u8; 16]    zeroed

[Entity Directory: entity_count × 20 bytes]
  entity_type:      u8          0=topic, 1=queue, 2=actor_ns, 3=exchange, 4=job
  _pad:             [u8; 3]     zeroed
  entity_id:        u64 LE
  entry_offset:     u32 LE      byte offset in this file where entries start
  entry_count:      u32 LE      number of entries for this entity

[Entry Arrays: back-to-back, each entry 8 bytes]
  record_offset:    u32 LE      byte offset in the segment file
  record_len:       u32 LE      length of the record/message
```

### Entity Types

```rust
pub const SIDX_ENTITY_TOPIC: u8 = 0;
pub const SIDX_ENTITY_QUEUE: u8 = 1;
pub const SIDX_ENTITY_ACTOR_NS: u8 = 2;
pub const SIDX_ENTITY_EXCHANGE: u8 = 3;
pub const SIDX_ENTITY_JOB: u8 = 4;
```

### Building the Index

During `apply_command`, the engine accumulates `(entity_type, entity_id,
segment_offset, record_len)` tuples. When a segment is sealed, these are
sorted by `(entity_type, entity_id)` and written as the `.sidx` file.

### Reading the Index

The `.sidx` file is mmap'd. Lookups:
1. Binary search the entity directory for `(entity_type, entity_id)`
2. Index into the entry array at `entry_offset + entry_index * 8`
3. Read `record_offset` and `record_len` directly — O(1)

## MqLogReader Changes

The `MmapMqLogReader` gains a direct-addressing fast path:

```rust
fn read_message_at(&self, topic_id: u64, offset: u64) -> Option<Bytes> {
    let engine = self.engine.read().unwrap();
    let topic = engine.topics.get(&topic_id)?;
    let loc = topic.get_location(offset)?;
    drop(engine);
    // Direct segment access — O(1), no scanning
    self.prefetcher.read_bytes_at(loc.segment_id(), loc.offset())
}
```

The scan-based `read_topic_messages_into` becomes a range lookup over the
offset index, fetching each message by its physical location.

## Raft Record Layout (Reference)

```
+----------+----------+----------+------+--------+-------+------+-----------+--------+
| len (4B) | type(1B) |group(3B) |term  |node_id |index  | tag  | data      |crc(8B) |
| u32 LE   | u8       | u24 LE   |u64 LE|u64 LE  |u64 LE | u8   | variable  |u64 LE  |
+----------+----------+----------+------+--------+-------+------+-----------+--------+
  HEADER_SIZE=8                    ----entry header (25B)----
                                   data_start = 33
```

For MQ Publish commands, the data region contains:
```
[tag:1=0x02][topic_id:8][count:4][msg1_len:4][msg1_data...][msg2_len:4][msg2_data...]...
```

The `segment_offset` in `MessageLocation` points to the start of an individual
flat message within this data region, enabling zero-copy access without parsing
the raft record framing.
