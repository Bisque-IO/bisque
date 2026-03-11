use std::collections::HashMap;

/// Build the bisque-mq topic name for a Kafka `(topic, partition)` pair.
pub fn partition_topic_name(kafka_topic: &str, partition: i32) -> String {
    format!("{}-{}", kafka_topic, partition)
}

/// Parse a bisque-mq topic name back into `(kafka_topic, partition_index)`.
/// Returns `None` if the name doesn't match `{name}-{digits}`.
pub fn parse_partition_topic(mq_topic_name: &str) -> Option<(&str, i32)> {
    let dash_pos = mq_topic_name.rfind('-')?;
    if dash_pos == 0 || dash_pos == mq_topic_name.len() - 1 {
        return None;
    }
    let suffix = &mq_topic_name[dash_pos + 1..];
    let partition: i32 = suffix.parse().ok()?;
    let kafka_name = &mq_topic_name[..dash_pos];
    Some((kafka_name, partition))
}

/// Maps Kafka topic names to their partition metadata (bisque-mq topic IDs).
///
/// Each entry in `topics` maps a Kafka topic name to a sorted list of
/// `(partition_index, bisque_mq_topic_id)` pairs.
pub struct PartitionMap {
    topics: HashMap<String, Vec<(i32, u64)>>,
}

impl PartitionMap {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    /// Rebuild the partition map from a list of `(topic_name, topic_id)` pairs
    /// sourced from the bisque-mq engine.
    pub fn refresh(&mut self, mq_topics: &[(String, u64)]) {
        self.topics.clear();
        for (name, topic_id) in mq_topics {
            if let Some((kafka_name, partition)) = parse_partition_topic(name) {
                if let Some(parts) = self.topics.get_mut(kafka_name) {
                    parts.push((partition, *topic_id));
                } else {
                    self.topics
                        .insert(kafka_name.to_string(), vec![(partition, *topic_id)]);
                }
            }
        }
        // Sort each partition list by index
        for parts in self.topics.values_mut() {
            parts.sort_by_key(|&(idx, _)| idx);
        }
    }

    /// Get the Kafka topic names as a collected Vec.
    pub fn kafka_topics(&self) -> Vec<&str> {
        self.topics.keys().map(|s| s.as_str()).collect()
    }

    /// Iterate Kafka topic names without allocating a Vec.
    pub fn kafka_topic_names(&self) -> impl Iterator<Item = &str> {
        self.topics.keys().map(|s| s.as_str())
    }

    /// Get the partition list for a Kafka topic.
    /// Returns `None` if the topic doesn't exist.
    pub fn partitions(&self, kafka_topic: &str) -> Option<&[(i32, u64)]> {
        self.topics.get(kafka_topic).map(|v| v.as_slice())
    }

    /// Get the partition count for a Kafka topic.
    pub fn partition_count(&self, kafka_topic: &str) -> Option<usize> {
        self.topics.get(kafka_topic).map(|v| v.len())
    }

    /// Resolve a `(kafka_topic, partition_index)` to a bisque-mq topic ID.
    ///
    /// Fast path O(1): direct index when partitions are contiguous from 0.
    /// Fallback O(log n): binary search for sparse partition indices.
    pub fn resolve(&self, kafka_topic: &str, partition: i32) -> Option<u64> {
        self.topics.get(kafka_topic).and_then(|parts| {
            let idx = partition as usize;
            // Fast path: partitions are typically 0..N contiguous
            if idx < parts.len() && parts[idx].0 == partition {
                return Some(parts[idx].1);
            }
            // Fallback: binary search (sorted by partition index)
            parts
                .binary_search_by_key(&partition, |&(p, _)| p)
                .ok()
                .map(|pos| parts[pos].1)
        })
    }

    /// Check if a Kafka topic exists.
    pub fn contains(&self, kafka_topic: &str) -> bool {
        self.topics.contains_key(kafka_topic)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_topic_name() {
        assert_eq!(partition_topic_name("events", 0), "events-0");
        assert_eq!(partition_topic_name("events", 3), "events-3");
        assert_eq!(partition_topic_name("my-topic", 1), "my-topic-1");
    }

    #[test]
    fn test_parse_partition_topic() {
        assert_eq!(parse_partition_topic("events-0"), Some(("events", 0)));
        assert_eq!(parse_partition_topic("events-3"), Some(("events", 3)));
        assert_eq!(parse_partition_topic("my-topic-1"), Some(("my-topic", 1)));
        assert_eq!(parse_partition_topic("events"), None);
        assert_eq!(parse_partition_topic("-0"), None);
        assert_eq!(parse_partition_topic("events-"), None);
        assert_eq!(parse_partition_topic("events-abc"), None);
    }

    #[test]
    fn test_partition_map_refresh() {
        let mut map = PartitionMap::new();
        let mq_topics = vec![
            ("events-0".to_string(), 100),
            ("events-1".to_string(), 101),
            ("events-2".to_string(), 102),
            ("orders-0".to_string(), 200),
            ("orders-1".to_string(), 201),
            ("standalone".to_string(), 300),
        ];
        map.refresh(&mq_topics);

        assert_eq!(map.partition_count("events"), Some(3));
        assert_eq!(map.partition_count("orders"), Some(2));
        assert_eq!(map.partition_count("standalone"), None);

        assert_eq!(map.resolve("events", 0), Some(100));
        assert_eq!(map.resolve("events", 1), Some(101));
        assert_eq!(map.resolve("events", 2), Some(102));
        assert_eq!(map.resolve("events", 3), None);
        assert_eq!(map.resolve("orders", 0), Some(200));
    }

    #[test]
    fn test_partition_map_sorted() {
        let mut map = PartitionMap::new();
        let mq_topics = vec![
            ("t-2".to_string(), 12),
            ("t-0".to_string(), 10),
            ("t-1".to_string(), 11),
        ];
        map.refresh(&mq_topics);

        let parts = map.partitions("t").unwrap();
        assert_eq!(parts[0], (0, 10));
        assert_eq!(parts[1], (1, 11));
        assert_eq!(parts[2], (2, 12));
    }

    #[test]
    fn test_partition_map_kafka_topics() {
        let mut map = PartitionMap::new();
        let mq_topics = vec![("alpha-0".to_string(), 1), ("beta-0".to_string(), 2)];
        map.refresh(&mq_topics);

        let mut topics = map.kafka_topics();
        topics.sort();
        assert_eq!(topics, vec!["alpha", "beta"]);
    }

    #[test]
    fn test_resolve_fast_path() {
        let mut map = PartitionMap::new();
        let mq_topics: Vec<_> = (0..100)
            .map(|i| (format!("t-{i}"), i as u64 + 1000))
            .collect();
        map.refresh(&mq_topics);

        // All should hit the O(1) fast path
        for i in 0..100 {
            assert_eq!(map.resolve("t", i), Some(i as u64 + 1000));
        }
        assert_eq!(map.resolve("t", 100), None);
    }

    #[test]
    fn test_kafka_topic_names_iterator() {
        let mut map = PartitionMap::new();
        let mq_topics = vec![("alpha-0".to_string(), 1), ("beta-0".to_string(), 2)];
        map.refresh(&mq_topics);

        let mut names: Vec<&str> = map.kafka_topic_names().collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn test_contains() {
        let mut map = PartitionMap::new();
        let mq_topics = vec![("events-0".to_string(), 100)];
        map.refresh(&mq_topics);

        assert!(map.contains("events"));
        assert!(!map.contains("nonexistent"));
    }

    #[test]
    fn test_resolve_sparse_partitions() {
        let mut map = PartitionMap::new();
        // Sparse: partitions 0, 2, 5 (gaps at 1, 3, 4)
        let mq_topics = vec![
            ("sparse-0".to_string(), 10),
            ("sparse-2".to_string(), 12),
            ("sparse-5".to_string(), 15),
        ];
        map.refresh(&mq_topics);

        assert_eq!(map.resolve("sparse", 0), Some(10));
        assert_eq!(map.resolve("sparse", 2), Some(12));
        assert_eq!(map.resolve("sparse", 5), Some(15));
        // Gaps should return None
        assert_eq!(map.resolve("sparse", 1), None);
        assert_eq!(map.resolve("sparse", 3), None);
        assert_eq!(map.resolve("sparse", 4), None);
    }

    #[test]
    fn test_refresh_empty() {
        let mut map = PartitionMap::new();
        map.refresh(&[]);
        assert!(map.kafka_topics().is_empty());
        assert_eq!(map.partition_count("anything"), None);
    }

    #[test]
    fn test_refresh_clears_old_data() {
        let mut map = PartitionMap::new();
        let topics1 = vec![("old-0".to_string(), 1)];
        map.refresh(&topics1);
        assert!(map.contains("old"));

        let topics2 = vec![("new-0".to_string(), 2)];
        map.refresh(&topics2);
        assert!(!map.contains("old"));
        assert!(map.contains("new"));
    }

    #[test]
    fn test_refresh_no_partition_suffix() {
        let mut map = PartitionMap::new();
        // Topics without dash-digit suffix should be skipped
        let mq_topics = vec![
            ("standalone".to_string(), 100),
            ("no-suffix".to_string(), 200), // "no" is not a digit suffix either... wait "suffix" is not digits
        ];
        map.refresh(&mq_topics);
        // Neither should appear since "standalone" has no dash-digit and
        // "no-suffix" suffix is not digits
        assert!(map.kafka_topics().is_empty());
    }

    #[test]
    fn test_partition_count_none() {
        let map = PartitionMap::new();
        assert_eq!(map.partition_count("nonexistent"), None);
    }

    #[test]
    fn test_partitions_none() {
        let map = PartitionMap::new();
        assert!(map.partitions("nonexistent").is_none());
    }

    #[test]
    fn test_resolve_nonexistent_topic() {
        let map = PartitionMap::new();
        assert_eq!(map.resolve("nope", 0), None);
    }

    #[test]
    fn test_refresh_duplicate_partition_names() {
        let mut map = PartitionMap::new();
        // Same partition appears twice — later one should be appended
        let mq_topics = vec![("dup-0".to_string(), 10), ("dup-0".to_string(), 20)];
        map.refresh(&mq_topics);
        // Should have 2 entries for partition 0 (both get added)
        let parts = map.partitions("dup").unwrap();
        assert_eq!(parts.len(), 2);
    }
}
