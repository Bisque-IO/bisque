use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
// In-memory Exchange State
// =============================================================================

pub struct ExchangeState {
    pub meta: ExchangeMeta,
    /// binding_id → Binding
    pub bindings: HashMap<u64, Binding>,
    /// Routing key hash → binding IDs (for direct exchange fast path).
    pub direct_index: HashMap<u64, Vec<u64>>,
}

impl ExchangeState {
    pub fn new(meta: ExchangeMeta) -> Self {
        Self {
            meta,
            bindings: HashMap::new(),
            direct_index: HashMap::new(),
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
                        ids.retain(|&id| id != binding_id);
                        if ids.is_empty() {
                            self.direct_index.remove(&hash);
                        }
                    }
                }
            }
        }
    }

    /// Route a message to target queue IDs based on exchange type and routing key.
    pub fn route(&self, routing_key: Option<&str>) -> Vec<u64> {
        match self.meta.exchange_type {
            ExchangeType::Fanout => {
                // Deliver to all bound queues
                self.bindings.values().map(|b| b.queue_id).collect()
            }
            ExchangeType::Direct => {
                // Deliver to queues with exact routing key match
                if let Some(key) = routing_key {
                    let hash = name_hash(key);
                    self.direct_index
                        .get(&hash)
                        .map(|ids| {
                            ids.iter()
                                .filter_map(|id| self.bindings.get(id).map(|b| b.queue_id))
                                .collect()
                        })
                        .unwrap_or_default()
                } else {
                    // Match bindings with no routing key
                    self.bindings
                        .values()
                        .filter(|b| b.routing_key.is_none())
                        .map(|b| b.queue_id)
                        .collect()
                }
            }
            ExchangeType::Topic => {
                // Deliver to queues whose binding pattern matches the routing key
                let key = routing_key.unwrap_or("");
                self.bindings
                    .values()
                    .filter(|b| {
                        let pattern = b.routing_key.as_deref().unwrap_or("");
                        topic_pattern_matches(pattern, key)
                    })
                    .map(|b| b.queue_id)
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
    // Determine delimiter: use `/` if either contains `/`, else `.`
    let delim = if pattern.contains('/') || key.contains('/') {
        '/'
    } else {
        '.'
    };

    let pattern_parts: Vec<&str> = pattern.split(delim).collect();
    let key_parts: Vec<&str> = key.split(delim).collect();

    match_parts(&pattern_parts, &key_parts)
}

fn match_parts(pattern: &[&str], key: &[&str]) -> bool {
    let mut pi = 0;
    let mut ki = 0;

    while pi < pattern.len() {
        let p = pattern[pi];
        if p == "#" {
            // # at end matches everything remaining
            if pi == pattern.len() - 1 {
                return true;
            }
            // Try matching rest of pattern against every suffix of key
            for skip in ki..=key.len() {
                if match_parts(&pattern[pi + 1..], &key[skip..]) {
                    return true;
                }
            }
            return false;
        } else if p == "*" || p == "+" {
            // Match exactly one word
            if ki >= key.len() {
                return false;
            }
            pi += 1;
            ki += 1;
        } else {
            // Literal match
            if ki >= key.len() || p != key[ki] {
                return false;
            }
            pi += 1;
            ki += 1;
        }
    }

    pi == pattern.len() && ki == key.len()
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
            queue_id: 10,
            routing_key: None,
        });
        ex.add_binding(Binding {
            binding_id: 2,
            exchange_id: 1,
            queue_id: 20,
            routing_key: None,
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
            queue_id: 10,
            routing_key: Some("error".to_string()),
        });
        ex.add_binding(Binding {
            binding_id: 2,
            exchange_id: 1,
            queue_id: 20,
            routing_key: Some("info".to_string()),
        });

        assert_eq!(ex.route(Some("error")), vec![10]);
        assert_eq!(ex.route(Some("info")), vec![20]);
        assert!(ex.route(Some("debug")).is_empty());
    }

    #[test]
    fn test_topic_exchange() {
        let meta = ExchangeMeta::new(1, "topic".to_string(), 100, ExchangeType::Topic);
        let mut ex = ExchangeState::new(meta);
        ex.add_binding(Binding {
            binding_id: 1,
            exchange_id: 1,
            queue_id: 10,
            routing_key: Some("logs.*".to_string()),
        });
        ex.add_binding(Binding {
            binding_id: 2,
            exchange_id: 1,
            queue_id: 20,
            routing_key: Some("logs.#".to_string()),
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
            queue_id: 10,
            routing_key: Some("key".to_string()),
        });

        assert_eq!(ex.route(Some("key")), vec![10]);
        ex.remove_binding(1);
        assert!(ex.route(Some("key")).is_empty());
    }
}
