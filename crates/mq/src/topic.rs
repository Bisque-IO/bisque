use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::exchange::RetainedValue;
use crate::flat::{FlatMessageMeta, MqttEnvelopeMeta, is_mqtt_envelope};
use crate::types::{
    PartitionInfo, RetentionPolicy, TopicCronConfig, TopicDedupConfig, TopicLifetimePolicy,
    name_hash,
};

// =============================================================================
// Global Dedup Reverse Index (scc::TreeIndex)
// =============================================================================

/// Global reverse index for O(expired) dedup pruning across all topics.
///
/// The `TreeIndex` is keyed by `(timestamp_ms, topic_id, dedup_key)` which
/// gives natural time ordering. Pruning uses `remove_range_sync` to bulk-delete
/// all entries older than the cutoff, then removes those keys from each topic's
/// forward `papaya::HashMap`.
///
/// Shared across all dedup-enabled topics via `Arc` on `MqMetadata`,
/// so memory cost is fixed regardless of topic count.
pub struct DedupIndex {
    /// `(timestamp_ms, topic_id, dedup_key)` → `()`
    index: scc::TreeIndex<(u64, u64, u128), ()>,
}

impl DedupIndex {
    pub fn new() -> Self {
        Self {
            index: scc::TreeIndex::new(),
        }
    }

    /// Record a dedup entry in the reverse index.
    #[inline]
    pub fn insert(&self, timestamp_ms: u64, topic_id: u64, key: u128) {
        let _ = self.index.insert_sync((timestamp_ms, topic_id, key), ());
    }

    /// Prune ALL entries with `timestamp_ms < before_ms` across all topics.
    ///
    /// Collects `(topic_id, dedup_key)` pairs via range scan, then bulk-removes
    /// them from the tree with `remove_range_sync`. Returns the expired pairs
    /// for the caller to remove from per-topic forward maps.
    pub fn prune_before(&self, before_ms: u64) -> Vec<(u64, u128)> {
        let guard = scc::Guard::new();
        let mut expired = Vec::new();

        // Tuple ordering: (ts, tid, dk) < (before_ms, 0, 0) iff ts < before_ms.
        for (key, _) in self.index.range(..(before_ms, 0, 0), &guard) {
            expired.push((key.1, key.2));
        }

        // Bulk remove from tree — single O(log n + expired) operation.
        if !expired.is_empty() {
            self.index.remove_range_sync(..(before_ms, 0, 0));
        }

        expired
    }

    /// Remove a single entry (e.g. when a topic is deleted).
    #[inline]
    pub fn remove(&self, timestamp_ms: u64, topic_id: u64, key: u128) {
        self.index.remove_sync(&(timestamp_ms, topic_id, key));
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        let guard = scc::Guard::new();
        self.index.iter(&guard).next().is_none()
    }
}

// =============================================================================
// Per-Connection Dedup Pre-Filter (single-threaded fast path)
// =============================================================================

/// Per-connection dedup pre-filter for protocol adapters.
///
/// Single-threaded (no sync overhead) — owned by one connection handler.
/// Catches same-connection duplicate retransmissions before they hit the
/// shared concurrent `DedupWindow` / `DedupIndex`.
///
/// Uses a `HashSet<u128>` for O(1) lookup and a `VecDeque<(u128, u64)>` for
/// FIFO eviction by insertion order. Entries are naturally time-ordered since
/// each connection processes messages sequentially.
pub struct DedupPreFilter {
    keys: std::collections::HashSet<u128>,
    /// Insertion-ordered entries for FIFO eviction: `(dedup_key, timestamp_ms)`.
    order: std::collections::VecDeque<(u128, u64)>,
    /// Maximum entries before oldest are evicted.
    max_entries: usize,
    /// Window duration in milliseconds. Entries older than `now - window_ms`
    /// are evicted on the next check.
    window_ms: u64,
}

impl DedupPreFilter {
    /// Create a new pre-filter with the given capacity and window.
    pub fn new(max_entries: usize, window_ms: u64) -> Self {
        Self {
            keys: std::collections::HashSet::with_capacity(max_entries.min(4096)),
            order: std::collections::VecDeque::with_capacity(max_entries.min(4096)),
            max_entries,
            window_ms,
        }
    }

    /// Create from a `TopicDedupConfig`, using a fraction of the global
    /// max_entries as the per-connection cap.
    pub fn from_config(config: &crate::types::TopicDedupConfig) -> Self {
        // Per-connection filter is much smaller than global — cap at 10K or 10%.
        let max = (config.max_entries as usize / 10).clamp(256, 10_000);
        Self::new(max, config.window_secs * 1000)
    }

    /// Returns `true` if the key is a duplicate (already seen within this
    /// connection's window). If not a duplicate, records it.
    ///
    /// Evicts expired and over-capacity entries before checking.
    #[inline]
    pub fn check_and_insert(&mut self, key: u128, timestamp_ms: u64) -> bool {
        self.evict_expired(timestamp_ms);

        if self.keys.contains(&key) {
            return true;
        }

        // Evict oldest if over capacity.
        while self.order.len() >= self.max_entries {
            if let Some((old_key, _)) = self.order.pop_front() {
                self.keys.remove(&old_key);
            }
        }

        self.keys.insert(key);
        self.order.push_back((key, timestamp_ms));
        false
    }

    /// Check without inserting. Returns `true` if the key was previously seen.
    #[inline]
    pub fn contains(&self, key: u128) -> bool {
        self.keys.contains(&key)
    }

    /// Number of entries currently tracked.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the filter is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.order.clear();
    }

    /// Evict entries older than `timestamp_ms - window_ms`.
    #[inline]
    fn evict_expired(&mut self, timestamp_ms: u64) {
        let cutoff = timestamp_ms.saturating_sub(self.window_ms);
        while let Some(&(key, ts)) = self.order.front() {
            if ts >= cutoff {
                break;
            }
            self.order.pop_front();
            self.keys.remove(&key);
        }
    }
}

/// Compute the next cron trigger time after the given timestamp (ms since epoch).
pub fn compute_next_trigger(cron_expr: &str, after: u64) -> u64 {
    use chrono::{TimeZone, Utc};
    use std::str::FromStr;

    let schedule = match cron::Schedule::from_str(cron_expr) {
        Ok(s) => s,
        Err(_) => return after + 60_000, // fallback: 1 minute
    };

    let dt = Utc
        .timestamp_millis_opt(after as i64)
        .single()
        .unwrap_or_else(Utc::now);

    schedule
        .after(&dt)
        .next()
        .map(|next| next.timestamp_millis() as u64)
        .unwrap_or(after + 60_000)
}

/// Sentinel: cached min needs full recompute.
const MIN_DIRTY: u64 = u64::MAX;
/// Sentinel: computed result is u64::MAX (no consumers).
const MIN_NONE: u64 = u64::MAX - 1;

// =============================================================================
// Topic Metadata (persisted to MDBX)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMeta {
    pub topic_id: u64,
    pub name: String,
    pub created_at: u64,
    pub retention: RetentionPolicy,
    pub head_index: u64,
    pub tail_index: u64,
    pub message_count: u64,
    #[serde(default)]
    pub name_hash: u64,
    /// Number of partitions. 0 or 1 means unpartitioned (data in coordinator group).
    /// >1 means partitioned (data spread across partition raft groups).
    #[serde(default)]
    pub partition_count: u32,
    /// Partition map. Empty if unpartitioned.
    #[serde(default)]
    pub partitions: Vec<PartitionInfo>,
    /// Total byte size of all messages in the topic (value bytes only).
    #[serde(default)]
    pub total_bytes: u64,
    /// Raft log index of the most recent publish.
    #[serde(default)]
    pub latest_log_index: u64,
    /// Message position within the batch at `latest_log_index`.
    #[serde(default)]
    pub latest_msg_pos: usize,

    // -- Unified storage features --
    /// Topic lifetime policy.
    #[serde(default)]
    pub lifetime: TopicLifetimePolicy,
    /// Whether this topic stores a retained message (last published message).
    #[serde(default)]
    pub retained: bool,
    /// Dedup configuration (if enabled).
    #[serde(default)]
    pub dedup_config: Option<TopicDedupConfig>,
    /// Cron auto-publish configuration (replaces Job entity).
    #[serde(default)]
    pub cron_config: Option<TopicCronConfig>,
    /// Whether cron is currently enabled.
    #[serde(default)]
    pub cron_enabled: bool,
    /// Next cron trigger time (ms since epoch).
    #[serde(default)]
    pub next_trigger_at: u64,
}

impl TopicMeta {
    pub fn new(topic_id: u64, name: String, created_at: u64, retention: RetentionPolicy) -> Self {
        let hash = name_hash(&name);
        Self {
            topic_id,
            name,
            created_at,
            retention,
            head_index: 0,
            tail_index: 0,
            message_count: 0,
            name_hash: hash,
            partition_count: 0,
            partitions: Vec::new(),
            total_bytes: 0,
            latest_log_index: 0,
            latest_msg_pos: 0,
            lifetime: TopicLifetimePolicy::Permanent,
            retained: false,
            dedup_config: None,
            cron_config: None,
            cron_enabled: false,
            next_trigger_at: 0,
        }
    }

    /// Create a new partitioned topic metadata.
    pub fn new_partitioned(
        topic_id: u64,
        name: String,
        created_at: u64,
        retention: RetentionPolicy,
        partition_count: u32,
        partitions: Vec<PartitionInfo>,
    ) -> Self {
        let hash = name_hash(&name);
        Self {
            topic_id,
            name,
            created_at,
            retention,
            head_index: 0,
            tail_index: 0,
            message_count: 0,
            name_hash: hash,
            partition_count,
            partitions,
            total_bytes: 0,
            latest_log_index: 0,
            latest_msg_pos: 0,
            lifetime: TopicLifetimePolicy::Permanent,
            retained: false,
            dedup_config: None,
            cron_config: None,
            cron_enabled: false,
            next_trigger_at: 0,
        }
    }

    /// Returns true if this topic is partitioned.
    #[inline]
    pub fn is_partitioned(&self) -> bool {
        self.partition_count > 1
    }

    /// Ensure name_hash is populated (for deserialized data that may have default 0).
    pub fn ensure_name_hash(&mut self) {
        if self.name_hash == 0 {
            self.name_hash = name_hash(&self.name);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConsumerOffset {
    pub topic_id: u64,
    pub consumer_id: u64,
    pub committed_offset: u64,
    #[serde(default)]
    pub pending_offset: u64,
}

// =============================================================================
// Dedup Window
// =============================================================================

/// Convert a byte slice (up to 16 bytes) into a `u128` dedup key.
/// Shorter slices are zero-padded on the right; longer slices are truncated.
#[inline]
pub fn bytes_to_dedup_key(b: &[u8]) -> u128 {
    let mut buf = [0u8; 16];
    let len = b.len().min(16);
    buf[..len].copy_from_slice(&b[..len]);
    u128::from_be_bytes(buf)
}

/// Lock-free sliding-window dedup tracker.
///
/// Forward map: `dedup_key (u128) → timestamp_ms (u64)`.
/// Prune iterates the map and removes entries older than the threshold.
///
/// When the entry count exceeds `max_entries`, an inline GC is triggered
/// on the next `check_and_insert` to prevent unbounded growth.
pub struct DedupWindow {
    /// dedup_key → insertion timestamp in milliseconds.
    keys: papaya::HashMap<u128, u64>,
    /// Approximate entry count (may drift slightly under concurrency).
    count: AtomicUsize,
    /// Maximum entries before inline GC fires. 0 = no limit.
    max_entries: usize,
    /// Window duration in milliseconds (used for inline GC threshold).
    window_ms: u64,
    /// Reusable buffer for collecting expired keys during prune.
    prune_buf: parking_lot::Mutex<Vec<u128>>,
}

impl DedupWindow {
    pub fn new(config: &TopicDedupConfig) -> Self {
        Self {
            keys: papaya::HashMap::new(),
            count: AtomicUsize::new(0),
            max_entries: config.max_entries as usize,
            window_ms: config.window_secs * 1000,
            prune_buf: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Returns `true` if this key is a duplicate (already seen within the window).
    /// `timestamp_ms` is the current wall-clock time in milliseconds.
    ///
    /// If the entry count exceeds `max_entries`, an inline GC prune runs
    /// before the insert to cap memory usage.
    pub fn check_and_insert(&self, key: u128, timestamp_ms: u64) -> bool {
        // Inline GC: if over capacity, prune expired entries immediately.
        if self.max_entries > 0 && self.count.load(Ordering::Relaxed) >= self.max_entries {
            let before_ms = timestamp_ms.saturating_sub(self.window_ms);
            self.prune(before_ms);
        }

        let guard = self.keys.pin();
        if guard.get(&key).is_some() {
            return true; // duplicate
        }
        guard.insert(key, timestamp_ms);
        self.count.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Like `check_and_insert`, but also records the entry in the global
    /// `DedupIndex` reverse index for efficient cross-topic pruning.
    pub fn check_and_insert_indexed(
        &self,
        key: u128,
        timestamp_ms: u64,
        topic_id: u64,
        index: &DedupIndex,
    ) -> bool {
        // Inline GC: if over capacity, prune expired entries immediately.
        if self.max_entries > 0 && self.count.load(Ordering::Relaxed) >= self.max_entries {
            let before_ms = timestamp_ms.saturating_sub(self.window_ms);
            self.prune(before_ms);
        }

        let guard = self.keys.pin();
        if guard.get(&key).is_some() {
            return true; // duplicate
        }
        guard.insert(key, timestamp_ms);
        self.count.fetch_add(1, Ordering::Relaxed);
        index.insert(timestamp_ms, topic_id, key);
        false
    }

    /// Remove all entries with timestamp older than `before_ms` (full-scan fallback).
    pub fn prune(&self, before_ms: u64) -> usize {
        let guard = self.keys.pin();
        let mut buf = self.prune_buf.lock();
        buf.clear();
        buf.extend(
            guard
                .iter()
                .filter(|(_, ts)| **ts < before_ms)
                .map(|(k, _)| *k),
        );
        let removed = buf.len();
        for k in buf.iter() {
            guard.remove(k);
        }
        if removed > 0 {
            // Saturating sub — count is approximate under concurrency.
            let prev = self.count.fetch_sub(removed, Ordering::Relaxed);
            // Correct for underflow (fetch_sub wraps on usize).
            if prev < removed {
                self.count.store(0, Ordering::Relaxed);
            }
        }
        removed
    }

    /// Remove specific keys from the forward map (called by DedupIndex-driven prune).
    pub fn remove_keys(&self, keys: &[u128]) -> usize {
        if keys.is_empty() {
            return 0;
        }
        let guard = self.keys.pin();
        let mut removed = 0;
        for k in keys {
            if guard.remove(k).is_some() {
                removed += 1;
            }
        }
        if removed > 0 {
            let prev = self.count.fetch_sub(removed, Ordering::Relaxed);
            if prev < removed {
                self.count.store(0, Ordering::Relaxed);
            }
        }
        removed
    }

    /// Approximate number of entries currently tracked.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Snapshot for serialization: `Vec<(dedup_key, timestamp_ms)>`.
    pub fn snapshot(&self) -> Vec<(u128, u64)> {
        let guard = self.keys.pin();
        guard.iter().map(|(&k, &ts)| (k, ts)).collect()
    }

    /// Restore from snapshot.
    pub fn restore(&self, entries: Vec<(u128, u64)>) {
        let guard = self.keys.pin();
        let count = entries.len();
        for (k, ts) in entries {
            guard.insert(k, ts);
        }
        self.count.fetch_add(count, Ordering::Relaxed);
    }

    /// Restore from snapshot and populate the global reverse index.
    pub fn restore_indexed(&self, entries: Vec<(u128, u64)>, topic_id: u64, index: &DedupIndex) {
        let guard = self.keys.pin();
        let count = entries.len();
        for (k, ts) in entries {
            guard.insert(k, ts);
            index.insert(ts, topic_id, k);
        }
        self.count.fetch_add(count, Ordering::Relaxed);
    }
}

// =============================================================================
// In-memory Topic State
// =============================================================================

/// Lock-free topic state.
///
/// Immutable identity fields are stored directly. Mutable counters use atomics
/// for safe concurrent reads (leader tasks, protocol adapters) with single-writer
/// updates (Raft apply path).
pub struct TopicState {
    // -- Immutable identity (set once at creation, never changed) --
    pub meta: TopicMeta,

    // -- Mutable counters (CachePadded to avoid false sharing between cores) --
    head_index: AtomicU64,
    tail_index: AtomicU64,
    message_count: AtomicU64,
    total_bytes: AtomicU64,
    latest_log_index: AtomicU64,
    latest_msg_pos: AtomicUsize,

    pub(crate) consumer_offsets: papaya::HashMap<u64, TopicConsumerOffset>,
    /// Cached minimum consumer offset.
    cached_min_consumer_offset: AtomicU64,

    /// IDs of consumer groups attached to this topic.
    pub(crate) consumer_group_ids: papaya::HashMap<u64, ()>,

    /// Retained message (last published value). Only used when meta.retained == true.
    /// Mmap-backed when segment_id is available; heap-copied otherwise.
    pub(crate) retained_message: RwLock<Option<RetainedValue>>,

    /// Dedup window (only allocated when dedup_config is Some).
    pub(crate) dedup: Option<DedupWindow>,

    /// Cron state atomics.
    pub(crate) cron_enabled: AtomicBool,
    pub(crate) next_trigger_at: AtomicU64,
    // Pre-initialized metrics handles (commented out — hot-path contention)
    m_publish_count: metrics::Counter,
    m_publish_bytes: metrics::Counter,
}

impl TopicState {
    pub fn new(meta: TopicMeta, catalog_name: &str) -> Self {
        let head_index = AtomicU64::new(meta.head_index);
        let tail_index = AtomicU64::new(meta.tail_index);
        let message_count = AtomicU64::new(meta.message_count);
        let total_bytes = AtomicU64::new(meta.total_bytes);
        let latest_log_index = AtomicU64::new(meta.latest_log_index);
        let latest_msg_pos = AtomicUsize::new(meta.latest_msg_pos);
        let cron_enabled = AtomicBool::new(meta.cron_enabled);
        let next_trigger_at = AtomicU64::new(meta.next_trigger_at);
        let dedup = meta.dedup_config.as_ref().map(DedupWindow::new);
        let m_publish_count = metrics::counter!(
            "mq.publish_count",
            "catalog" => catalog_name.to_owned(),
            "topic" => meta.name.clone()
        );
        let m_publish_bytes = metrics::counter!(
            "mq.topic.publish_bytes",
            "catalog" => catalog_name.to_owned(),
            "topic" => meta.name.clone()
        );

        Self {
            meta,
            head_index,
            tail_index,
            message_count,
            total_bytes,
            latest_log_index,
            latest_msg_pos,
            consumer_offsets: papaya::HashMap::new(),
            cached_min_consumer_offset: AtomicU64::new(MIN_NONE),
            consumer_group_ids: papaya::HashMap::new(),
            retained_message: RwLock::new(None),
            dedup,
            cron_enabled,
            next_trigger_at,
            m_publish_count,
            m_publish_bytes,
        }
    }

    // -- Atomic accessors --

    #[inline]
    pub fn head_index(&self) -> u64 {
        self.head_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn tail_index(&self) -> u64 {
        self.tail_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn message_count(&self) -> u64 {
        self.message_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn latest_log_index(&self) -> u64 {
        self.latest_log_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn latest_msg_pos(&self) -> usize {
        self.latest_msg_pos.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn cron_enabled(&self) -> bool {
        self.cron_enabled.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn next_trigger_at(&self) -> u64 {
        self.next_trigger_at.load(Ordering::Relaxed)
    }

    /// Build a TopicMeta snapshot by flushing atomics back to a cloned meta.
    pub fn snapshot_meta(&self) -> TopicMeta {
        let mut m = self.meta.clone();
        m.head_index = self.head_index();
        m.tail_index = self.tail_index();
        m.message_count = self.message_count();
        m.total_bytes = self.total_bytes();
        m.latest_log_index = self.latest_log_index();
        m.latest_msg_pos = self.latest_msg_pos();
        m.cron_enabled = self.cron_enabled();
        m.next_trigger_at = self.next_trigger_at();
        m
    }

    /// Snapshot consumer offsets as a Vec (for serialization).
    pub fn consumer_offsets_snapshot(&self) -> Vec<TopicConsumerOffset> {
        let guard = self.consumer_offsets.pin();
        guard.iter().map(|(_, v)| v.clone()).collect()
    }

    /// Apply a batch of pre-encoded flat messages to this topic.
    ///
    /// Assigns dense per-topic offsets (0, 1, 2, …).
    /// Returns `base_offset` — the first dense topic offset assigned.
    pub fn apply_publish(
        &self,
        log_index: u64,
        messages: impl ExactSizeIterator<Item = Bytes>,
        segment_id: Option<u64>,
    ) -> u64 {
        let base_offset = self.head_index();
        let count = messages.len() as u64;
        let mut total_bytes: u64 = 0;
        let mut last_pos: usize = 0;
        let mut last_msg: Option<Bytes> = None;
        for (i, m) in messages.enumerate() {
            total_bytes += if is_mqtt_envelope(&m) {
                MqttEnvelopeMeta::value_len(&m).unwrap_or(0) as u64
            } else {
                FlatMessageMeta::value_len(&m).unwrap_or(0) as u64
            };
            last_pos = i;
            if self.meta.retained {
                last_msg = Some(m);
            }
        }

        // Update retained message if enabled — mmap-backed when possible
        if let Some(msg) = last_msg {
            let rv = match segment_id {
                Some(seg_id) => RetainedValue::mmap_backed(seg_id, msg),
                None => RetainedValue::heap(msg),
            };
            *self.retained_message.write() = Some(rv);
        }

        if self.message_count() == 0 {
            self.tail_index.store(base_offset, Ordering::Relaxed);
        }
        self.message_count.fetch_add(count, Ordering::Relaxed);
        self.total_bytes.fetch_add(total_bytes, Ordering::Relaxed);
        self.head_index
            .store(base_offset + count, Ordering::Relaxed);
        self.latest_log_index.store(log_index, Ordering::Relaxed);
        self.latest_msg_pos.store(last_pos, Ordering::Relaxed);

        self.m_publish_count.increment(count);
        self.m_publish_bytes.increment(total_bytes);

        base_offset
    }

    /// Check dedup before publishing. Returns true if the key is a duplicate.
    /// Uses the per-topic forward map only (no global index).
    pub fn check_dedup(&self, key: u128, timestamp_ms: u64) -> bool {
        if let Some(ref dedup) = self.dedup {
            dedup.check_and_insert(key, timestamp_ms)
        } else {
            false
        }
    }

    /// Check dedup and also record in the global `DedupIndex` reverse index.
    pub fn check_dedup_indexed(&self, key: u128, timestamp_ms: u64, index: &DedupIndex) -> bool {
        if let Some(ref dedup) = self.dedup {
            dedup.check_and_insert_indexed(key, timestamp_ms, self.meta.topic_id, index)
        } else {
            false
        }
    }

    /// Prune dedup window entries older than `before_ms` (full-scan fallback).
    pub fn prune_dedup(&self, before_ms: u64) -> usize {
        if let Some(ref dedup) = self.dedup {
            dedup.prune(before_ms)
        } else {
            0
        }
    }

    /// Remove specific dedup keys from the forward map (index-driven prune).
    pub fn remove_dedup_keys(&self, keys: &[u128]) -> usize {
        if let Some(ref dedup) = self.dedup {
            dedup.remove_keys(keys)
        } else {
            0
        }
    }

    pub fn apply_commit_offset(&self, consumer_id: u64, offset: u64) {
        let guard = self.consumer_offsets.pin();
        let mut dirty = false;
        if let Some(existing) = guard.get(&consumer_id) {
            if offset > existing.committed_offset {
                let mut updated = existing.clone();
                updated.committed_offset = offset;
                guard.insert(consumer_id, updated);
                dirty = true;
            }
        } else {
            guard.insert(
                consumer_id,
                TopicConsumerOffset {
                    topic_id: self.meta.topic_id,
                    consumer_id,
                    committed_offset: offset,
                    pending_offset: 0,
                },
            );
            dirty = true;
        }
        if dirty {
            self.cached_min_consumer_offset
                .store(MIN_DIRTY, Ordering::Relaxed);
        }
    }

    pub fn apply_purge(&self, before_index: u64) {
        let current_tail = self.tail_index();
        if before_index > current_tail {
            self.tail_index.store(before_index, Ordering::Relaxed);
            let current_count = self.message_count();
            if current_count > 0 {
                let purged = before_index.saturating_sub(current_tail);
                self.message_count
                    .store(current_count.saturating_sub(purged), Ordering::Relaxed);
            }
        }
    }

    /// Attach a consumer group to this topic.
    pub fn attach_group(&self, group_id: u64) {
        self.consumer_group_ids.pin().insert(group_id, ());
    }

    /// Detach a consumer group from this topic.
    pub fn detach_group(&self, group_id: u64) {
        self.consumer_group_ids.pin().remove(&group_id);
    }

    /// Returns true if this topic should be auto-deleted (DeleteOnLastDetach + no groups).
    pub fn should_auto_delete(&self) -> bool {
        self.meta.lifetime == TopicLifetimePolicy::DeleteOnLastDetach
            && self.consumer_group_ids.pin().len() == 0
    }

    /// Returns a clone of the current retained message, if any.
    pub fn get_retained(&self) -> Option<RetainedValue> {
        let guard = self.retained_message.read();
        guard.as_ref().map(|rv| RetainedValue {
            segment_id: rv.segment_id,
            message: rv.message.clone(),
        })
    }

    /// Detach the retained message from the given segment by copying to heap.
    /// Returns `true` if the retained message was backed by `segment_id` and was detached.
    /// Returns `false` if there's no retained message, it's already heap-backed,
    /// or it's backed by a different segment.
    pub fn detach_retained_if_segment(&self, segment_id: u64) -> bool {
        let mut guard = self.retained_message.write();
        if let Some(rv) = guard.as_mut() {
            if rv.segment_id == Some(segment_id) {
                rv.detach();
                return true;
            }
        }
        false
    }

    /// Check if cron should trigger now.
    pub fn should_cron_trigger(&self, current_time: u64) -> bool {
        self.cron_enabled()
            && self.meta.cron_config.is_some()
            && current_time >= self.next_trigger_at()
    }

    /// Apply cron trigger: update next trigger time.
    pub fn apply_cron_trigger(&self, triggered_at: u64) {
        if let Some(ref cron) = self.meta.cron_config {
            let next = compute_next_trigger(&cron.cron_expression, triggered_at);
            self.next_trigger_at.store(next, Ordering::Relaxed);
        }
    }

    /// Returns the minimum log index required by this topic (for purge floor).
    pub fn min_required_index(&self) -> u64 {
        let cached = self.cached_min_consumer_offset.load(Ordering::Relaxed);
        let consumer_min = if cached == MIN_NONE {
            u64::MAX
        } else if cached != MIN_DIRTY {
            cached
        } else {
            // Recompute
            let guard = self.consumer_offsets.pin();
            let min = guard
                .iter()
                .map(|(_, e)| e.committed_offset)
                .min()
                .unwrap_or(u64::MAX);
            if min == u64::MAX {
                self.cached_min_consumer_offset
                    .store(MIN_NONE, Ordering::Relaxed);
            } else {
                self.cached_min_consumer_offset
                    .store(min, Ordering::Relaxed);
            }
            min
        };
        self.tail_index().min(consumer_min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatMessageBuilder;

    fn make_topic(name: &str) -> TopicState {
        let meta = TopicMeta::new(1, name.to_string(), 1000, RetentionPolicy::default());
        TopicState::new(meta, "test")
    }

    fn make_msg(value: &[u8]) -> Bytes {
        FlatMessageBuilder::new(value).timestamp(1000).build()
    }

    #[test]
    fn test_topic_meta_defaults() {
        let meta = TopicMeta::new(42, "events".to_string(), 5000, RetentionPolicy::default());
        assert_eq!(meta.topic_id, 42);
        assert_eq!(meta.name, "events");
        assert_eq!(meta.head_index, 0);
        assert_eq!(meta.tail_index, 0);
        assert_eq!(meta.message_count, 0);
        assert_eq!(meta.lifetime, TopicLifetimePolicy::Permanent);
        assert!(!meta.retained);
        assert!(meta.dedup_config.is_none());
        assert!(meta.cron_config.is_none());
    }

    #[test]
    fn test_publish_single_message() {
        let topic = make_topic("test");
        let msgs = vec![make_msg(b"hello")];
        let base = topic.apply_publish(10, msgs.into_iter(), None);

        assert_eq!(base, 0);
        assert_eq!(topic.head_index(), 1);
        assert_eq!(topic.tail_index(), 0);
        assert_eq!(topic.message_count(), 1);
        assert_eq!(topic.latest_log_index(), 10);
        assert_eq!(topic.latest_msg_pos(), 0);
    }

    #[test]
    fn test_publish_batch() {
        let topic = make_topic("test");
        let msgs = vec![make_msg(b"a"), make_msg(b"b"), make_msg(b"c")];
        let base = topic.apply_publish(100, msgs.into_iter(), None);

        assert_eq!(base, 0);
        assert_eq!(topic.message_count(), 3);
        assert_eq!(topic.head_index(), 3);
    }

    #[test]
    fn test_retained_message() {
        let mut meta = TopicMeta::new(
            1,
            "retained-test".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        meta.retained = true;
        let topic = TopicState::new(meta, "test");

        let msg = make_msg(b"last-value");
        topic.apply_publish(10, vec![msg.clone()].into_iter(), None);

        let retained = topic.retained_message.read();
        assert!(retained.is_some());
        assert!(retained.as_ref().unwrap().segment_id.is_none()); // heap-backed in tests
    }

    #[test]
    fn test_dedup_window() {
        use super::bytes_to_dedup_key;
        let mut meta = TopicMeta::new(
            1,
            "dedup-test".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        meta.dedup_config = Some(TopicDedupConfig {
            window_secs: 60,
            max_entries: 100_000,
        });
        let topic = TopicState::new(meta, "test");

        let key = bytes_to_dedup_key(b"key1");
        assert!(!topic.check_dedup(key, 100_000)); // first time - not duplicate
        assert!(topic.check_dedup(key, 100_000)); // second time - duplicate

        let key2 = bytes_to_dedup_key(b"key2");
        assert!(!topic.check_dedup(key2, 100_000)); // different key - not duplicate
    }

    #[test]
    fn test_dedup_prune() {
        use super::bytes_to_dedup_key;
        let mut meta = TopicMeta::new(
            1,
            "dedup-prune".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        meta.dedup_config = Some(TopicDedupConfig {
            window_secs: 60,
            max_entries: 100_000,
        });
        let topic = TopicState::new(meta, "test");

        let key = bytes_to_dedup_key(b"key1");
        topic.check_dedup(key, 100_000);
        topic.prune_dedup(100_001); // prune entries before this timestamp
        assert!(!topic.check_dedup(key, 200_000)); // key is no longer in window
    }

    #[test]
    fn test_consumer_group_attach_detach() {
        let topic = make_topic("test");
        topic.attach_group(1);
        topic.attach_group(2);
        assert_eq!(topic.consumer_group_ids.pin().len(), 2);

        topic.detach_group(1);
        assert_eq!(topic.consumer_group_ids.pin().len(), 1);
        assert!(!topic.should_auto_delete()); // Permanent policy
    }

    #[test]
    fn test_auto_delete_policy() {
        let mut meta = TopicMeta::new(1, "temp".to_string(), 1000, RetentionPolicy::default());
        meta.lifetime = TopicLifetimePolicy::DeleteOnLastDetach;
        let topic = TopicState::new(meta, "test");

        topic.attach_group(1);
        assert!(!topic.should_auto_delete());

        topic.detach_group(1);
        assert!(topic.should_auto_delete());
    }

    #[test]
    fn test_commit_offset() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"msg")), None);

        topic.apply_commit_offset(100, 0);
        assert_eq!(
            topic
                .consumer_offsets
                .pin()
                .get(&100)
                .unwrap()
                .committed_offset,
            0
        );

        topic.apply_commit_offset(100, 15);
        assert_eq!(
            topic
                .consumer_offsets
                .pin()
                .get(&100)
                .unwrap()
                .committed_offset,
            15
        );

        // Cannot go backwards
        topic.apply_commit_offset(100, 5);
        assert_eq!(
            topic
                .consumer_offsets
                .pin()
                .get(&100)
                .unwrap()
                .committed_offset,
            15
        );
    }

    #[test]
    fn test_purge() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"a")), None);
        topic.apply_publish(20, std::iter::once(make_msg(b"b")), None);
        topic.apply_purge(1);
        assert_eq!(topic.tail_index(), 1);
    }

    #[test]
    fn test_min_required_index() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"a")), None);
        topic.apply_publish(20, std::iter::once(make_msg(b"b")), None);
        assert_eq!(topic.min_required_index(), 0);

        topic.apply_purge(1);
        topic.apply_commit_offset(1, 0);
        assert_eq!(topic.min_required_index(), 0);

        topic.apply_commit_offset(1, 1);
        assert_eq!(topic.min_required_index(), 1);
    }

    #[test]
    fn test_partitioned_topic_meta() {
        use crate::types::{PartitionInfo, PartitionStatus};

        let partitions = vec![
            PartitionInfo {
                partition_index: 0,
                group_id: 100,
                status: PartitionStatus::Active,
            },
            PartitionInfo {
                partition_index: 1,
                group_id: 101,
                status: PartitionStatus::Active,
            },
        ];

        let meta = TopicMeta::new_partitioned(
            1,
            "events".to_string(),
            1000,
            RetentionPolicy::default(),
            2,
            partitions,
        );

        assert!(meta.is_partitioned());
        assert_eq!(meta.partition_count, 2);
        assert_eq!(meta.partitions.len(), 2);
    }
}
