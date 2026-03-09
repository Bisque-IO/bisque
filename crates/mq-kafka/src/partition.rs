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
                self.topics
                    .entry(kafka_name.to_string())
                    .or_default()
                    .push((partition, *topic_id));
            }
        }
        // Sort each partition list by index
        for parts in self.topics.values_mut() {
            parts.sort_by_key(|&(idx, _)| idx);
        }
    }

    /// Get the Kafka topic names.
    pub fn kafka_topics(&self) -> Vec<&str> {
        self.topics.keys().map(|s| s.as_str()).collect()
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
    pub fn resolve(&self, kafka_topic: &str, partition: i32) -> Option<u64> {
        self.topics.get(kafka_topic).and_then(|parts| {
            parts
                .iter()
                .find(|&&(idx, _)| idx == partition)
                .map(|&(_, id)| id)
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
            ("standalone".to_string(), 300), // Not a Kafka partition topic
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
        // Insert in reverse order
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
}
