# Async Responder Redesign

Replace `ResponderTable` + `mpsc::UnboundedSender`-based `ClientRegistry` with inline raft responder completion and a segmented MPSC byte buffer for client response delivery.

---

## 1. Create `ResponseChannel` (segmented MPSC byte buffer)

New module: `crates/mq/src/response_channel.rs`

### Entry format (8-byte aligned)

```
[len:u32][ready:u8][pad:3][response bytes... padded to 8-byte alignment]
```

- `len` — byte length of the response payload (not including header or padding)
- `ready` — atomic flag; producer sets to `1` with `Release` after writing payload
- `pad` — 3 zero bytes to align header to 8 bytes
- Total entry size: `8 + align8(payload_len)`

### Segment

```rust
struct Segment {
    data: Box<[u8]>,          // fixed capacity (e.g. 64KB, configurable)
    write_pos: AtomicU32,     // CAS claim cursor for producers
    sealed_pos: AtomicU32,    // set to final write_pos when sealed (u32::MAX while open)
    next: AtomicPtr<Segment>, // linked list for consumer traversal
}
```

- Fixed size allocated once (or recycled)
- `write_pos` is the CAS target — producers atomically claim `[pos, pos + entry_size)`
- `sealed_pos` is set when a producer detects the segment is full and rotates
- `next` links to the successor segment for the consumer to follow

### Channel structure

```rust
pub struct ResponseChannel {
    inner: Arc<ResponseChannelInner>,
}

struct ResponseChannelInner {
    /// Active write segment. Producers load this to find where to write.
    /// Protected by rotate_lock only during segment transitions.
    active: AtomicPtr<Segment>,

    /// Lock taken only during segment rotation (rare, on segment boundary).
    rotate_lock: parking_lot::Mutex<()>,

    /// Recycle queue — bounded, excess segments are dropped.
    recycle: parking_lot::Mutex<Vec<Box<[u8]>>>,

    /// Consumer notification.
    notify: tokio::sync::Notify,

    /// Closed flag — set when reader is dropped.
    closed: AtomicBool,

    /// Configuration.
    segment_capacity: u32,
    max_recycle: usize,
}
```

Split into writer/reader handles:

```rust
pub struct ResponseWriter {
    inner: Arc<ResponseChannelInner>,
}

pub struct ResponseReader {
    inner: Arc<ResponseChannelInner>,
    /// Current read segment.
    read_segment: *const Segment,
    /// Read position within current segment.
    read_pos: u32,
}
```

### Producer flow (`ResponseWriter::write`)

```
1. Check closed flag — return false if set
2. Load active segment pointer (Acquire)
3. Compute entry_size = 8 + align8(payload_len)
4. CAS loop on segment.write_pos to claim [pos, pos + entry_size)
   - If pos + entry_size > capacity:
     a. Acquire rotate_lock
     b. Double-check active hasn't changed (another producer may have rotated)
     c. Allocate new segment (or pop from recycle queue)
     d. Set old_segment.sealed_pos = old_segment.write_pos (Release)
     e. Set old_segment.next = new_segment
     f. Store new active pointer (Release)
     g. Release rotate_lock
     h. Retry write on new segment
5. Write payload bytes at data[pos + 8 ..]
6. Write len (u32 LE) at data[pos .. pos+4]
7. Set ready flag: AtomicU8 at data[pos + 4] store 1 (Release)
8. notify.notify_one()
```

### Consumer flow (`ResponseReader::try_read`)

```
1. At read_pos in current read_segment:
   a. Load ready flag at data[read_pos + 4] (Acquire)
   b. If ready == 1:
      - Read len from data[read_pos .. read_pos+4]
      - Return slice data[read_pos+8 .. read_pos+8+len]
      - Advance read_pos by 8 + align8(len)
   c. If ready == 0:
      - Check if segment is sealed (sealed_pos != u32::MAX)
      - If sealed AND read_pos >= sealed_pos: advance to next segment
        - Push consumed segment's data buffer to recycle queue
          (if recycle.len() < max_recycle, else drop)
        - Follow next pointer to successor segment
        - Reset read_pos = 0
        - Retry
      - Otherwise: no data available, return None
```

### Async consumer (`ResponseReader::recv`)

```rust
pub async fn recv(&mut self) -> Option<&[u8]> {
    loop {
        if let Some(data) = self.try_read() {
            return Some(data);
        }
        // Channel closed and fully drained.
        if self.inner.closed.load(Ordering::Acquire) {
            return None;
        }
        self.inner.notify.notified().await;
    }
}
```

### Disconnect detection

When the `ResponseReader` is dropped, set `closed: AtomicBool` on the inner. `ResponseWriter::write` checks this and returns `false` (analogous to current `tx.send().is_ok()` check).

---

## 2. Redesign `ClientRegistry`

Replace `papaya::HashMap<u64, mpsc::UnboundedSender<ResponseEntry>>` with `papaya::HashMap<u64, ResponseWriter>`.

```rust
pub struct ClientRegistry {
    clients: papaya::HashMap<u64, ResponseWriter>,
    next_id: AtomicU64,
}

impl ClientRegistry {
    pub fn register(&self, config: ResponseChannelConfig) -> (u64, ResponseReader) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (writer, reader) = response_channel(config);
        self.clients.pin().insert(id, writer);
        (id, reader)
    }

    pub fn unregister(&self, client_id: u64) {
        self.clients.pin().remove(&client_id);
        // Dropping the writer signals the reader via closed flag.
    }

    /// Write response bytes directly into the client's buffer.
    /// Returns false if client disconnected or not found.
    #[inline]
    pub fn send(&self, client_id: u64, entry: &ResponseEntry) -> bool {
        let guard = self.clients.pin();
        if let Some(writer) = guard.get(&client_id) {
            writer.write(&entry.buf)
        } else {
            false
        }
    }
}
```

Key change: `send()` now takes `&ResponseEntry` (borrows the bytes) instead of consuming it. The writer copies bytes directly into the segment buffer — zero intermediate allocation.

---

## 3. Remove `ResponderTable`, complete raft responders inline

### In `state_machine.rs` apply():

**Before:**
```rust
EntryPayload::Normal(cmd) => {
    if cmd.tag() == MqCommand::TAG_BATCH {
        // ... barrier + inline batch ...
    } else {
        if let Some(r) = responder {
            async_apply.responder_table.insert(log_index, r);
        }
    }
}
```

**After:**
```rust
EntryPayload::Normal(cmd) => {
    if cmd.tag() == MqCommand::TAG_BATCH {
        // ... barrier + inline batch ...
    } else {
        // Complete raft responder immediately with log index.
        // Actual result delivered via client channel.
        if let Some(r) = responder {
            r.send(MqResponse::Committed { log_index });
        }
    }
}
```

This requires adding an `MqResponse::Committed { log_index: u64 }` variant (or reusing `MqResponse::Ok` if the log index isn't needed in the raft response path).

### Remove from codebase:

- Delete `ResponderTable` struct and impl
- Remove `responder_table` field from `PartitionWorker` and `AsyncApplyManager`
- Remove `responder_table` from worker construction in `AsyncApplyManager::new()`
- Delete `ResponderTable` tests

---

## 4. Update `PartitionWorker`

The worker response delivery simplifies to client channel only:

**Before:**
```rust
if let Some(responder) = self.responder_table.take(rec.log_index) {
    responder.send(response);
} else if let Some(client_id) = self.client_id_table.take(rec.log_index) {
    let entry = ResponseEntry::from_response(rec.log_index, &response);
    let _ = self.client_registry.send(client_id, entry);
}
```

**After:**
```rust
if let Some(client_id) = self.client_id_table.take(rec.log_index) {
    let entry = ResponseEntry::from_response(rec.log_index, &response);
    let _ = self.client_registry.send(client_id, &entry);
}
```

---

## 5. Update `lib.rs` exports

- Add `pub mod response_channel;`
- Export `ResponseWriter`, `ResponseReader`, `ResponseChannelConfig`
- Remove `ResponderTable` from any re-exports

---

## 6. Update tests

- Delete `ResponderTable` tests (`responder_table_insert_take`, `responder_table_take_nonexistent`)
- Rewrite `ClientRegistry` tests to use the new `ResponseReader` API instead of `mpsc::UnboundedReceiver`
- Add `ResponseChannel` unit tests:
  - Single producer write + read
  - Multi-producer concurrent writes
  - Segment rotation on fill
  - Segment recycling (within limit and overflow/drop)
  - Disconnect detection (reader drop → writer returns false)
  - 8-byte alignment verification
  - Zero-length payload
  - Payload larger than one segment (should span rotation)

---

## Configuration

```rust
pub struct ResponseChannelConfig {
    /// Segment capacity in bytes. Default 65536 (64KB).
    pub segment_capacity: u32,
    /// Maximum number of segments kept in the recycle queue.
    /// Excess segments are dropped. Default 4.
    pub max_recycle: usize,
}
```

---

## Dependencies

No new crate dependencies required. Uses:
- `std::sync::atomic::{AtomicU8, AtomicU32, AtomicPtr, AtomicBool}`
- `parking_lot::Mutex` (already in deps, for rotate_lock and recycle queue)
- `tokio::sync::Notify` (already in deps)

---

## Files changed

| File | Change |
|---|---|
| `crates/mq/src/response_channel.rs` | **New** — `ResponseWriter`, `ResponseReader`, `Segment`, config |
| `crates/mq/src/async_apply.rs` | Remove `ResponderTable`, update `ClientRegistry`, update `PartitionWorker`, update `AsyncApplyManager` |
| `crates/mq/src/state_machine.rs` | Complete raft responders inline in `apply()` |
| `crates/mq/src/types.rs` | Add `MqResponse::Committed { log_index }` variant (if needed) |
| `crates/mq/src/lib.rs` | Add `pub mod response_channel`, update exports |
