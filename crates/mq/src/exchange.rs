use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::types::{Binding, ExchangeType, name_hash};

// =============================================================================
// Exchange Metadata (persisted to MDBX)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeMeta {
    pub exchange_id: u64,
    pub name: String,
    pub created_at: u64,
    pub exchange_type: ExchangeType,
    #[serde(default)]
    pub name_hash: u64,
}

impl ExchangeMeta {
    pub fn new(
        exchange_id: u64,
        name: String,
        created_at: u64,
        exchange_type: ExchangeType,
    ) -> Self {
        let hash = name_hash(&name);
        Self {
            exchange_id,
            name,
            created_at,
            exchange_type,
            name_hash: hash,
        }
    }

    pub fn ensure_name_hash(&mut self) {
        if self.name_hash == 0 {
            self.name_hash = name_hash(&self.name);
        }
    }
}

// =============================================================================
// Retained Message Value — tracks segment provenance for mmap lifecycle
// =============================================================================

/// A retained message value that tracks its backing segment.
///
/// When `segment_id` is `Some`, the `message` `Bytes` is zero-copy and backed
/// by an mmap'd raft log segment. The segment `Arc` stays alive as long as
/// this `Bytes` exists. When the segment is purged, the sweep copies the
/// message to heap and sets `segment_id` to `None`.
pub struct RetainedValue {
    /// The mmap segment ID backing this message, or `None` if heap-allocated
    /// (e.g. from snapshot restore or after detach sweep).
    pub segment_id: Option<u64>,
    /// The retained message bytes.
    pub message: Bytes,
}

impl RetainedValue {
    /// Create a retained value backed by an mmap segment.
    pub fn mmap_backed(segment_id: u64, message: Bytes) -> Self {
        Self {
            segment_id: Some(segment_id),
            message,
        }
    }

    /// Create a heap-allocated retained value (no segment provenance).
    pub fn heap(message: Bytes) -> Self {
        Self {
            segment_id: None,
            message,
        }
    }

    /// Detach from the mmap segment by copying to a heap-allocated `Bytes`.
    /// Returns the old segment_id if it was mmap-backed.
    pub fn detach(&mut self) -> Option<u64> {
        if let Some(seg_id) = self.segment_id.take() {
            self.message = Bytes::copy_from_slice(&self.message);
            Some(seg_id)
        } else {
            None
        }
    }
}

// =============================================================================
// In-memory Exchange State
// =============================================================================

pub struct ExchangeState {
    pub meta: ExchangeMeta,
    /// binding_id → Binding
    pub bindings: HashMap<u64, Binding>,
    /// Routing key hash → binding IDs (for direct exchange fast path).
    pub direct_index: HashMap<u64, SmallVec<[u64; 4]>>,
    /// Retained messages: routing_key → retained value with segment provenance.
    pub retained: HashMap<String, RetainedValue>,
}

impl ExchangeState {
    pub fn new(meta: ExchangeMeta) -> Self {
        Self {
            meta,
            bindings: HashMap::new(),
            direct_index: HashMap::new(),
            retained: HashMap::new(),
        }
    }

    pub fn add_binding(&mut self, binding: Binding) {
        let binding_id = binding.binding_id;
        // Build direct index for fast lookup
        if self.meta.exchange_type == ExchangeType::Direct {
            if let Some(ref key) = binding.routing_key {
                let hash = name_hash(key);
                self.direct_index.entry(hash).or_default().push(binding_id);
            }
        }
        self.bindings.insert(binding_id, binding);
    }

    pub fn remove_binding(&mut self, binding_id: u64) {
        if let Some(binding) = self.bindings.remove(&binding_id) {
            if self.meta.exchange_type == ExchangeType::Direct {
                if let Some(ref key) = binding.routing_key {
                    let hash = name_hash(key);
                    if let Some(ids) = self.direct_index.get_mut(&hash) {
                        ids.retain(|id| *id != binding_id);
                        if ids.is_empty() {
                            self.direct_index.remove(&hash);
                        }
                    }
                }
            }
        }
    }

    /// Route a message to target topic IDs based on exchange type and routing key.
    pub fn route(&self, routing_key: Option<&str>) -> SmallVec<[u64; 8]> {
        match self.meta.exchange_type {
            ExchangeType::Fanout => {
                // Deliver to all bound topics
                self.bindings.values().map(|b| b.target_topic_id).collect()
            }
            ExchangeType::Direct => {
                // Deliver to topics with exact routing key match
                if let Some(key) = routing_key {
                    let hash = name_hash(key);
                    self.direct_index
                        .get(&hash)
                        .map(|ids| {
                            ids.iter()
                                .filter_map(|id| self.bindings.get(id).map(|b| b.target_topic_id))
                                .collect()
                        })
                        .unwrap_or_default()
                } else {
                    // Match bindings with no routing key
                    self.bindings
                        .values()
                        .filter(|b| b.routing_key.is_none())
                        .map(|b| b.target_topic_id)
                        .collect()
                }
            }
            ExchangeType::Topic => {
                // Deliver to topics whose binding pattern matches the routing key
                let key = routing_key.unwrap_or("");
                self.bindings
                    .values()
                    .filter(|b| {
                        let pattern = b.routing_key.as_deref().unwrap_or("");
                        topic_pattern_matches(pattern, key)
                    })
                    .map(|b| b.target_topic_id)
                    .collect()
            }
        }
    }
}

// =============================================================================
// Topic Pattern Matching (AMQP / MQTT style)
// =============================================================================

/// Match a routing key against a binding pattern.
///
/// Supports both AMQP and MQTT wildcard conventions:
/// - `*` or `+` matches exactly one word/level
/// - `#` matches zero or more words/levels
/// - Words are delimited by `.` (AMQP) or `/` (MQTT)
pub fn topic_pattern_matches(pattern: &str, key: &str) -> bool {
    let delim = if pattern.as_bytes().contains(&b'/') || key.as_bytes().contains(&b'/') {
        b'/'
    } else {
        b'.'
    };
    match_bytes(pattern.as_bytes(), key.as_bytes(), delim)
}

/// Zero-allocation recursive pattern matching on byte slices.
fn match_bytes(pattern: &[u8], key: &[u8], delim: u8) -> bool {
    let (p_seg, p_rest) = split_first_segment(pattern, delim);
    let (k_seg, k_rest) = split_first_segment(key, delim);

    match (p_seg, k_seg) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(b"#"), _) => {
            // # at end matches everything
            if p_rest.is_empty() {
                return true;
            }
            // Try matching rest of pattern starting from every possible position in key
            if match_bytes(p_rest, key, delim) {
                return true;
            }
            let mut remaining = key;
            loop {
                let (seg, rest) = split_first_segment(remaining, delim);
                if seg.is_none() {
                    break;
                }
                if match_bytes(p_rest, rest, delim) {
                    return true;
                }
                if rest.is_empty() {
                    break;
                }
                remaining = rest;
            }
            false
        }
        (Some(_), None) => p_seg == Some(b"#") && p_rest.is_empty(),
        (Some(p), Some(k)) => {
            if p == b"*" || p == b"+" {
                match_bytes(p_rest, k_rest, delim)
            } else if p == k {
                match_bytes(p_rest, k_rest, delim)
            } else {
                false
            }
        }
    }
}

/// Split on the first delimiter. Returns (first_segment, rest_after_delimiter).
#[inline]
fn split_first_segment<'a>(input: &'a [u8], delim: u8) -> (Option<&'a [u8]>, &'a [u8]) {
    if input.is_empty() {
        return (None, &[]);
    }
    match input.iter().position(|&b| b == delim) {
        Some(pos) => (Some(&input[..pos]), &input[pos + 1..]),
        None => (Some(input), &[]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, ExchangeType};

    #[test]
    fn test_topic_pattern_exact() {
        assert!(topic_pattern_matches("foo.bar", "foo.bar"));
        assert!(!topic_pattern_matches("foo.bar", "foo.baz"));
    }

    #[test]
    fn test_topic_pattern_star() {
        assert!(topic_pattern_matches("foo.*", "foo.bar"));
        assert!(!topic_pattern_matches("foo.*", "foo.bar.baz"));
        assert!(topic_pattern_matches("*.bar", "foo.bar"));
    }

    #[test]
    fn test_topic_pattern_hash() {
        assert!(topic_pattern_matches("foo.#", "foo.bar"));
        assert!(topic_pattern_matches("foo.#", "foo.bar.baz"));
        assert!(topic_pattern_matches("foo.#", "foo"));
        assert!(topic_pattern_matches("#", "foo.bar.baz"));
        assert!(topic_pattern_matches("#", ""));
    }

    #[test]
    fn test_topic_pattern_mixed() {
        assert!(topic_pattern_matches("foo.*.baz", "foo.bar.baz"));
        assert!(!topic_pattern_matches("foo.*.baz", "foo.bar.qux"));
        assert!(topic_pattern_matches("*.*.baz", "a.b.baz"));
        assert!(topic_pattern_matches("#.baz", "a.b.c.baz"));
    }

    #[test]
    fn test_mqtt_style() {
        assert!(topic_pattern_matches(
            "sensor/+/temperature",
            "sensor/1/temperature"
        ));
        assert!(!topic_pattern_matches(
            "sensor/+/temperature",
            "sensor/1/humidity"
        ));
        assert!(topic_pattern_matches("sensor/#", "sensor/1/temperature"));
        assert!(topic_pattern_matches("sensor/#", "sensor"));
    }

    #[test]
    fn test_fanout_exchange() {
        let meta = ExchangeMeta::new(1, "fanout".to_string(), 100, ExchangeType::Fanout);
        let mut ex = ExchangeState::new(meta);
        ex.add_binding(Binding {
            binding_id: 1,
            exchange_id: 1,
            target_topic_id: 10,
            routing_key: None,
            no_local: false,
            shared_group: None,
            subscription_id: None,
        });
        ex.add_binding(Binding {
            binding_id: 2,
            exchange_id: 1,
            target_topic_id: 20,
            routing_key: None,
            no_local: false,
            shared_group: None,
            subscription_id: None,
        });

        let targets = ex.route(None);
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&10));
        assert!(targets.contains(&20));
    }

    #[test]
    fn test_direct_exchange() {
        let meta = ExchangeMeta::new(1, "direct".to_string(), 100, ExchangeType::Direct);
        let mut ex = ExchangeState::new(meta);
        ex.add_binding(Binding {
            binding_id: 1,
            exchange_id: 1,
            target_topic_id: 10,
            routing_key: Some("error".to_string()),
            no_local: false,
            shared_group: None,
            subscription_id: None,
        });
        ex.add_binding(Binding {
            binding_id: 2,
            exchange_id: 1,
            target_topic_id: 20,
            routing_key: Some("info".to_string()),
            no_local: false,
            shared_group: None,
            subscription_id: None,
        });

        assert_eq!(ex.route(Some("error")).as_slice(), &[10]);
        assert_eq!(ex.route(Some("info")).as_slice(), &[20]);
        assert!(ex.route(Some("debug")).is_empty());
    }

    #[test]
    fn test_topic_exchange() {
        let meta = ExchangeMeta::new(1, "topic".to_string(), 100, ExchangeType::Topic);
        let mut ex = ExchangeState::new(meta);
        ex.add_binding(Binding {
            binding_id: 1,
            exchange_id: 1,
            target_topic_id: 10,
            routing_key: Some("logs.*".to_string()),
            no_local: false,
            shared_group: None,
            subscription_id: None,
        });
        ex.add_binding(Binding {
            binding_id: 2,
            exchange_id: 1,
            target_topic_id: 20,
            routing_key: Some("logs.#".to_string()),
            no_local: false,
            shared_group: None,
            subscription_id: None,
        });

        let targets = ex.route(Some("logs.error"));
        assert!(targets.contains(&10));
        assert!(targets.contains(&20));

        let targets = ex.route(Some("logs.error.critical"));
        assert!(!targets.contains(&10)); // * matches exactly one
        assert!(targets.contains(&20)); // # matches multiple
    }

    #[test]
    fn test_remove_binding() {
        let meta = ExchangeMeta::new(1, "direct".to_string(), 100, ExchangeType::Direct);
        let mut ex = ExchangeState::new(meta);
        ex.add_binding(Binding {
            binding_id: 1,
            exchange_id: 1,
            target_topic_id: 10,
            routing_key: Some("key".to_string()),
            no_local: false,
            shared_group: None,
            subscription_id: None,
        });

        assert_eq!(ex.route(Some("key")).as_slice(), &[10]);
        ex.remove_binding(1);
        assert!(ex.route(Some("key")).is_empty());
    }
}
