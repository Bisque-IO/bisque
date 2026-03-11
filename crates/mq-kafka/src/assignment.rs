//! Partition assignment strategies for consumer groups.
//!
//! Supports both **client-side** (Kafka compatibility, assignments come via
//! SyncGroup from the leader) and **server-side** (bisque-native) assignment.
//!
//! Three built-in strategies:
//! - [`RangeAssignor`]: Kafka default — contiguous partition ranges per topic.
//! - [`RoundRobinAssignor`]: Even distribution across all members.
//! - [`StickyAssignor`]: Minimize partition movement on rebalance.

use std::collections::{BTreeMap, HashMap, HashSet};

// =============================================================================
// Trait + Types
// =============================================================================

/// Input member for the assignor.
#[derive(Debug, Clone)]
pub struct AssignorMember {
    pub member_id: String,
    pub subscribed_topics: Vec<String>,
}

/// Result of assignment: `member_id → [(topic, [partitions])]`.
pub type AssignmentResult = Vec<(String, Vec<(String, Vec<i32>)>)>;

/// Trait for partition assignment strategies.
pub trait PartitionAssignor: Send + Sync {
    /// Strategy name (e.g. "range", "roundrobin", "sticky").
    fn name(&self) -> &str;

    /// Compute assignments.
    ///
    /// - `members`: the group members and their subscriptions.
    /// - `topic_partitions`: `(topic_name, [partition_indices])`.
    ///
    /// Returns `member_id → [(topic, [partitions])]`.
    fn assign(
        &self,
        members: &[AssignorMember],
        topic_partitions: &[(String, Vec<i32>)],
    ) -> AssignmentResult;
}

// =============================================================================
// RangeAssignor — Kafka default
// =============================================================================

/// Assigns contiguous partition ranges per topic.
///
/// For each topic, partitions are divided evenly among subscribing members
/// (sorted by member_id). The first `N % M` members get one extra partition
/// (where N = partitions, M = members).
pub struct RangeAssignor;

impl PartitionAssignor for RangeAssignor {
    fn name(&self) -> &str {
        "range"
    }

    fn assign(
        &self,
        members: &[AssignorMember],
        topic_partitions: &[(String, Vec<i32>)],
    ) -> AssignmentResult {
        let mut result: BTreeMap<String, Vec<(String, Vec<i32>)>> = BTreeMap::new();

        // Initialize empty entries for all members
        for m in members {
            result.entry(m.member_id.clone()).or_default();
        }

        for (topic, partitions) in topic_partitions {
            // Find members subscribed to this topic, sorted by member_id
            let mut subscribed: Vec<&str> = members
                .iter()
                .filter(|m| m.subscribed_topics.contains(topic))
                .map(|m| m.member_id.as_str())
                .collect();
            subscribed.sort();

            if subscribed.is_empty() || partitions.is_empty() {
                continue;
            }

            let n = partitions.len();
            let m = subscribed.len();
            let base = n / m;
            let remainder = n % m;

            let mut pos = 0;
            for (i, member_id) in subscribed.iter().enumerate() {
                let count = base + if i < remainder { 1 } else { 0 };
                let assigned: Vec<i32> = partitions[pos..pos + count].to_vec();
                pos += count;

                if !assigned.is_empty() {
                    result
                        .entry(member_id.to_string())
                        .or_default()
                        .push((topic.clone(), assigned));
                }
            }
        }

        result.into_iter().collect()
    }
}

// =============================================================================
// RoundRobinAssignor
// =============================================================================

/// Assigns partitions in a round-robin fashion across all subscribed members.
///
/// All partitions from all subscribed topics are interleaved across members
/// for maximum evenness.
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &str {
        "roundrobin"
    }

    fn assign(
        &self,
        members: &[AssignorMember],
        topic_partitions: &[(String, Vec<i32>)],
    ) -> AssignmentResult {
        // Collect all (topic, partition) pairs, sorted for determinism
        let mut all_tp: Vec<(&str, i32)> = Vec::new();
        for (topic, partitions) in topic_partitions {
            for &p in partitions {
                all_tp.push((topic.as_str(), p));
            }
        }
        all_tp.sort();

        // Build per-topic subscription sets for quick lookup
        let member_subs: Vec<(&str, HashSet<&str>)> = {
            let mut v: Vec<_> = members
                .iter()
                .map(|m| {
                    let subs: HashSet<&str> =
                        m.subscribed_topics.iter().map(|s| s.as_str()).collect();
                    (m.member_id.as_str(), subs)
                })
                .collect();
            v.sort_by_key(|(id, _)| *id);
            v
        };

        // member_id → topic → partitions
        let mut assign: BTreeMap<&str, BTreeMap<&str, Vec<i32>>> = BTreeMap::new();
        for (id, _) in &member_subs {
            assign.insert(id, BTreeMap::new());
        }

        let mut idx = 0;
        for (topic, partition) in &all_tp {
            // Find next eligible member (round-robin, skipping non-subscribers)
            let n = member_subs.len();
            for offset in 0..n {
                let candidate = (idx + offset) % n;
                let (mid, subs) = &member_subs[candidate];
                if subs.contains(topic) {
                    assign
                        .entry(mid)
                        .or_default()
                        .entry(topic)
                        .or_default()
                        .push(*partition);
                    idx = candidate + 1;
                    break;
                }
            }
        }

        assign
            .into_iter()
            .map(|(mid, topics)| {
                let topic_parts: Vec<(String, Vec<i32>)> = topics
                    .into_iter()
                    .map(|(t, ps)| (t.to_string(), ps))
                    .collect();
                (mid.to_string(), topic_parts)
            })
            .collect()
    }
}

// =============================================================================
// StickyAssignor
// =============================================================================

/// Minimizes partition movement on rebalance by preserving existing assignments.
///
/// On first assignment (no previous state), falls back to round-robin.
/// On subsequent rebalances:
/// 1. Keep all partitions whose current owner is still subscribed.
/// 2. Redistribute orphaned partitions evenly.
pub struct StickyAssignor;

/// Previous assignment state for sticky assignment.
#[derive(Debug, Clone, Default)]
pub struct PreviousAssignment {
    /// `member_id → [(topic, [partitions])]`
    pub assignments: HashMap<String, Vec<(String, Vec<i32>)>>,
}

impl StickyAssignor {
    /// Assign with previous state for stickiness.
    pub fn assign_with_previous(
        &self,
        members: &[AssignorMember],
        topic_partitions: &[(String, Vec<i32>)],
        previous: &PreviousAssignment,
    ) -> AssignmentResult {
        if previous.assignments.is_empty() {
            // No previous state — fall back to round-robin
            return RoundRobinAssignor.assign(members, topic_partitions);
        }

        let member_set: HashSet<&str> = members.iter().map(|m| m.member_id.as_str()).collect();
        let member_subs: HashMap<&str, HashSet<&str>> = members
            .iter()
            .map(|m| {
                let subs: HashSet<&str> = m.subscribed_topics.iter().map(|s| s.as_str()).collect();
                (m.member_id.as_str(), subs)
            })
            .collect();

        // Build the full set of topic-partitions to assign
        let mut all_tp: HashSet<(&str, i32)> = HashSet::new();
        for (topic, partitions) in topic_partitions {
            for &p in partitions {
                all_tp.insert((topic.as_str(), p));
            }
        }

        // Phase 1: retain existing assignments where member still exists and is subscribed
        let mut current: BTreeMap<String, Vec<(String, Vec<i32>)>> = BTreeMap::new();
        let mut assigned_tp: HashSet<(&str, i32)> = HashSet::new();

        for (mid, topics) in &previous.assignments {
            if !member_set.contains(mid.as_str()) {
                continue;
            }
            let subs = match member_subs.get(mid.as_str()) {
                Some(s) => s,
                None => continue,
            };
            for (topic, partitions) in topics {
                if !subs.contains(topic.as_str()) {
                    continue;
                }
                let kept: Vec<i32> = partitions
                    .iter()
                    .copied()
                    .filter(|&p| all_tp.contains(&(topic.as_str(), p)))
                    .collect();
                for &p in &kept {
                    assigned_tp.insert((topic.as_str(), p));
                }
                if !kept.is_empty() {
                    current
                        .entry(mid.clone())
                        .or_default()
                        .push((topic.clone(), kept));
                }
            }
        }

        // Phase 2: collect orphaned partitions
        let mut orphaned: Vec<(&str, i32)> = all_tp.difference(&assigned_tp).copied().collect();
        orphaned.sort();

        // Phase 3: distribute orphans to least-loaded eligible members
        // Count current load per member
        let mut load: BTreeMap<&str, usize> = BTreeMap::new();
        for m in members {
            let count = current
                .get(&m.member_id)
                .map(|topics| topics.iter().map(|(_, ps)| ps.len()).sum())
                .unwrap_or(0);
            load.insert(m.member_id.as_str(), count);
        }

        for (topic, partition) in orphaned {
            // Find eligible member with lowest load
            let best = members
                .iter()
                .filter(|m| {
                    member_subs
                        .get(m.member_id.as_str())
                        .map(|s| s.contains(topic))
                        .unwrap_or(false)
                })
                .min_by_key(|m| load.get(m.member_id.as_str()).copied().unwrap_or(0))
                .map(|m| m.member_id.as_str());

            if let Some(mid) = best {
                *load.entry(mid).or_insert(0) += 1;
                let entry = current.entry(mid.to_string()).or_default();
                // Find existing topic entry or create new one
                if let Some(tp) = entry.iter_mut().find(|(t, _)| t == topic) {
                    tp.1.push(partition);
                } else {
                    entry.push((topic.to_string(), vec![partition]));
                }
            }
        }

        // Phase 4: rebalance if distribution is uneven (new member joined)
        let total: usize = all_tp.len();
        let n_members = members.len();
        if n_members > 0 {
            let max_per_member = (total + n_members - 1) / n_members; // ceil(total/n)

            // Collect overloaded partitions
            let mut surplus: Vec<(String, i32)> = Vec::new();
            for (mid, topics) in current.iter_mut() {
                let count: usize = topics.iter().map(|(_, ps)| ps.len()).sum();
                if count > max_per_member {
                    let mut to_remove = count - max_per_member;
                    // Remove from the end of the last topics
                    for (topic, parts) in topics.iter_mut().rev() {
                        while to_remove > 0 && !parts.is_empty() {
                            let p = parts.pop().unwrap();
                            surplus.push((topic.clone(), p));
                            to_remove -= 1;
                        }
                        if to_remove == 0 {
                            break;
                        }
                    }
                }
            }
            // Remove empty topic entries
            for topics in current.values_mut() {
                topics.retain(|(_, ps)| !ps.is_empty());
            }

            // Distribute surplus to underloaded members
            surplus.sort();
            for (topic, partition) in surplus {
                let best = members
                    .iter()
                    .filter(|m| {
                        member_subs
                            .get(m.member_id.as_str())
                            .map(|s| s.contains(topic.as_str()))
                            .unwrap_or(false)
                    })
                    .min_by_key(|m| {
                        current
                            .get(&m.member_id)
                            .map(|ts| ts.iter().map(|(_, ps)| ps.len()).sum::<usize>())
                            .unwrap_or(0)
                    })
                    .map(|m| m.member_id.clone());

                if let Some(mid) = best {
                    let entry = current.entry(mid).or_default();
                    if let Some(tp) = entry.iter_mut().find(|(t, _)| *t == topic) {
                        tp.1.push(partition);
                    } else {
                        entry.push((topic, vec![partition]));
                    }
                }
            }
        }

        // Ensure all members appear in result
        for m in members {
            current.entry(m.member_id.clone()).or_default();
        }

        // Sort partitions within each topic for determinism
        for topics in current.values_mut() {
            for (_, parts) in topics.iter_mut() {
                parts.sort();
            }
            topics.sort_by(|a, b| a.0.cmp(&b.0));
        }

        current.into_iter().collect()
    }
}

impl PartitionAssignor for StickyAssignor {
    fn name(&self) -> &str {
        "sticky"
    }

    fn assign(
        &self,
        members: &[AssignorMember],
        topic_partitions: &[(String, Vec<i32>)],
    ) -> AssignmentResult {
        // Without previous state, delegate to round-robin
        self.assign_with_previous(members, topic_partitions, &PreviousAssignment::default())
    }
}

// =============================================================================
// Lookup helper
// =============================================================================

/// Get a built-in assignor by name.
pub fn get_assignor(name: &str) -> Option<Box<dyn PartitionAssignor>> {
    match name {
        "range" => Some(Box::new(RangeAssignor)),
        "roundrobin" => Some(Box::new(RoundRobinAssignor)),
        "sticky" => Some(Box::new(StickyAssignor)),
        _ => None,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn members(ids: &[&str], topics: &[&str]) -> Vec<AssignorMember> {
        ids.iter()
            .map(|id| AssignorMember {
                member_id: id.to_string(),
                subscribed_topics: topics.iter().map(|t| t.to_string()).collect(),
            })
            .collect()
    }

    fn topic_partitions(topics: &[(&str, i32)]) -> Vec<(String, Vec<i32>)> {
        topics
            .iter()
            .map(|(name, count)| (name.to_string(), (0..*count).collect()))
            .collect()
    }

    fn flatten(result: &AssignmentResult) -> BTreeMap<String, Vec<(String, i32)>> {
        let mut out: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
        for (mid, topics) in result {
            let entry = out.entry(mid.clone()).or_default();
            for (topic, parts) in topics {
                for &p in parts {
                    entry.push((topic.clone(), p));
                }
            }
            entry.sort();
        }
        out
    }

    fn total_partitions(result: &AssignmentResult) -> usize {
        result
            .iter()
            .map(|(_, topics)| topics.iter().map(|(_, ps)| ps.len()).sum::<usize>())
            .sum()
    }

    // -------------------------------------------------------------------------
    // RangeAssignor
    // -------------------------------------------------------------------------

    #[test]
    fn range_single_member() {
        let assignor = RangeAssignor;
        let m = members(&["m1"], &["t"]);
        let tp = topic_partitions(&[("t", 3)]);
        let result = assignor.assign(&m, &tp);

        assert_eq!(result.len(), 1);
        let flat = flatten(&result);
        assert_eq!(
            flat["m1"],
            vec![("t".into(), 0), ("t".into(), 1), ("t".into(), 2)]
        );
    }

    #[test]
    fn range_even_distribution() {
        let assignor = RangeAssignor;
        let m = members(&["m1", "m2"], &["t"]);
        let tp = topic_partitions(&[("t", 4)]);
        let result = assignor.assign(&m, &tp);

        let flat = flatten(&result);
        assert_eq!(flat["m1"], vec![("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(flat["m2"], vec![("t".into(), 2), ("t".into(), 3)]);
    }

    #[test]
    fn range_uneven_distribution() {
        let assignor = RangeAssignor;
        let m = members(&["m1", "m2"], &["t"]);
        let tp = topic_partitions(&[("t", 3)]);
        let result = assignor.assign(&m, &tp);

        let flat = flatten(&result);
        // m1 gets 2 (base=1 + remainder), m2 gets 1
        assert_eq!(flat["m1"], vec![("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(flat["m2"], vec![("t".into(), 2)]);
    }

    #[test]
    fn range_multiple_topics() {
        let assignor = RangeAssignor;
        let m = members(&["m1", "m2"], &["a", "b"]);
        let tp = topic_partitions(&[("a", 2), ("b", 2)]);
        let result = assignor.assign(&m, &tp);

        let flat = flatten(&result);
        assert_eq!(flat["m1"], vec![("a".into(), 0), ("b".into(), 0)]);
        assert_eq!(flat["m2"], vec![("a".into(), 1), ("b".into(), 1)]);
    }

    #[test]
    fn range_no_members() {
        let assignor = RangeAssignor;
        let m: Vec<AssignorMember> = vec![];
        let tp = topic_partitions(&[("t", 3)]);
        let result = assignor.assign(&m, &tp);
        assert!(result.is_empty());
    }

    #[test]
    fn range_partial_subscription() {
        let assignor = RangeAssignor;
        let m = vec![
            AssignorMember {
                member_id: "m1".into(),
                subscribed_topics: vec!["a".into(), "b".into()],
            },
            AssignorMember {
                member_id: "m2".into(),
                subscribed_topics: vec!["b".into()],
            },
        ];
        let tp = topic_partitions(&[("a", 2), ("b", 2)]);
        let result = assignor.assign(&m, &tp);

        let flat = flatten(&result);
        // m1 gets all of "a" (sole subscriber) + half of "b"
        assert!(flat["m1"].contains(&("a".into(), 0)));
        assert!(flat["m1"].contains(&("a".into(), 1)));
        // Both share "b"
        assert_eq!(total_partitions(&result), 4);
    }

    // -------------------------------------------------------------------------
    // RoundRobinAssignor
    // -------------------------------------------------------------------------

    #[test]
    fn roundrobin_even() {
        let assignor = RoundRobinAssignor;
        let m = members(&["m1", "m2"], &["t"]);
        let tp = topic_partitions(&[("t", 4)]);
        let result = assignor.assign(&m, &tp);

        let flat = flatten(&result);
        assert_eq!(flat["m1"].len(), 2);
        assert_eq!(flat["m2"].len(), 2);
        assert_eq!(total_partitions(&result), 4);
    }

    #[test]
    fn roundrobin_three_members() {
        let assignor = RoundRobinAssignor;
        let m = members(&["m1", "m2", "m3"], &["t"]);
        let tp = topic_partitions(&[("t", 7)]);
        let result = assignor.assign(&m, &tp);

        // 7 partitions / 3 members → 3,2,2 or 2,3,2 etc.
        let flat = flatten(&result);
        let counts: Vec<usize> = flat.values().map(|v| v.len()).collect();
        assert_eq!(counts.iter().sum::<usize>(), 7);
        assert!(*counts.iter().max().unwrap() - *counts.iter().min().unwrap() <= 1);
    }

    #[test]
    fn roundrobin_multi_topic() {
        let assignor = RoundRobinAssignor;
        let m = members(&["m1", "m2"], &["a", "b"]);
        let tp = topic_partitions(&[("a", 2), ("b", 2)]);
        let result = assignor.assign(&m, &tp);

        assert_eq!(total_partitions(&result), 4);
        let flat = flatten(&result);
        assert_eq!(flat["m1"].len(), 2);
        assert_eq!(flat["m2"].len(), 2);
    }

    // -------------------------------------------------------------------------
    // StickyAssignor
    // -------------------------------------------------------------------------

    #[test]
    fn sticky_no_previous_falls_back_to_roundrobin() {
        let assignor = StickyAssignor;
        let m = members(&["m1", "m2"], &["t"]);
        let tp = topic_partitions(&[("t", 4)]);
        let result = assignor.assign(&m, &tp);

        assert_eq!(total_partitions(&result), 4);
        let flat = flatten(&result);
        assert_eq!(flat["m1"].len(), 2);
        assert_eq!(flat["m2"].len(), 2);
    }

    #[test]
    fn sticky_preserves_existing() {
        let m = members(&["m1", "m2"], &["t"]);
        let tp = topic_partitions(&[("t", 4)]);

        // Previous: m1 had [0,1], m2 had [2,3]
        let mut prev = PreviousAssignment::default();
        prev.assignments
            .insert("m1".into(), vec![("t".into(), vec![0, 1])]);
        prev.assignments
            .insert("m2".into(), vec![("t".into(), vec![2, 3])]);

        let result = StickyAssignor.assign_with_previous(&m, &tp, &prev);
        let flat = flatten(&result);

        // Should preserve exactly the same assignment
        assert_eq!(flat["m1"], vec![("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(flat["m2"], vec![("t".into(), 2), ("t".into(), 3)]);
    }

    #[test]
    fn sticky_new_member_joins() {
        let m = members(&["m1", "m2", "m3"], &["t"]);
        let tp = topic_partitions(&[("t", 6)]);

        // Previous: m1 had [0,1,2], m2 had [3,4,5]; m3 is new
        let mut prev = PreviousAssignment::default();
        prev.assignments
            .insert("m1".into(), vec![("t".into(), vec![0, 1, 2])]);
        prev.assignments
            .insert("m2".into(), vec![("t".into(), vec![3, 4, 5])]);

        let result = StickyAssignor.assign_with_previous(&m, &tp, &prev);
        let flat = flatten(&result);

        // All 6 partitions should be assigned
        assert_eq!(total_partitions(&result), 6);

        // Each member should get 2 (even distribution)
        assert_eq!(flat["m1"].len(), 2);
        assert_eq!(flat["m2"].len(), 2);
        assert_eq!(flat["m3"].len(), 2);
    }

    #[test]
    fn sticky_member_leaves() {
        let m = members(&["m1"], &["t"]);
        let tp = topic_partitions(&[("t", 4)]);

        // Previous: m1 had [0,1], m2 had [2,3]; m2 left
        let mut prev = PreviousAssignment::default();
        prev.assignments
            .insert("m1".into(), vec![("t".into(), vec![0, 1])]);
        prev.assignments
            .insert("m2".into(), vec![("t".into(), vec![2, 3])]);

        let result = StickyAssignor.assign_with_previous(&m, &tp, &prev);
        let flat = flatten(&result);

        // m1 should get all 4 (kept 0,1 + picked up 2,3)
        assert_eq!(
            flat["m1"],
            vec![
                ("t".into(), 0),
                ("t".into(), 1),
                ("t".into(), 2),
                ("t".into(), 3),
            ]
        );
    }

    #[test]
    fn sticky_new_partition_added() {
        let m = members(&["m1", "m2"], &["t"]);
        let tp = topic_partitions(&[("t", 5)]); // was 4, now 5

        let mut prev = PreviousAssignment::default();
        prev.assignments
            .insert("m1".into(), vec![("t".into(), vec![0, 1])]);
        prev.assignments
            .insert("m2".into(), vec![("t".into(), vec![2, 3])]);

        let result = StickyAssignor.assign_with_previous(&m, &tp, &prev);
        let flat = flatten(&result);

        assert_eq!(total_partitions(&result), 5);
        // Existing assignments should be preserved
        assert!(flat["m1"].contains(&("t".into(), 0)));
        assert!(flat["m1"].contains(&("t".into(), 1)));
        assert!(flat["m2"].contains(&("t".into(), 2)));
        assert!(flat["m2"].contains(&("t".into(), 3)));
        // Partition 4 goes to least-loaded (either, both have 2)
    }

    // -------------------------------------------------------------------------
    // get_assignor
    // -------------------------------------------------------------------------

    #[test]
    fn test_get_assignor() {
        assert_eq!(get_assignor("range").unwrap().name(), "range");
        assert_eq!(get_assignor("roundrobin").unwrap().name(), "roundrobin");
        assert_eq!(get_assignor("sticky").unwrap().name(), "sticky");
        assert!(get_assignor("unknown").is_none());
    }

    // -------------------------------------------------------------------------
    // Edge case tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_range_empty_members() {
        let a = RangeAssignor;
        let tp = vec![("t".into(), vec![0, 1, 2])];
        let result = a.assign(&[], &tp);
        assert!(result.is_empty());
    }

    #[test]
    fn test_roundrobin_empty_members() {
        let a = RoundRobinAssignor;
        let tp = vec![("t".into(), vec![0, 1, 2])];
        let result = a.assign(&[], &tp);
        assert!(result.is_empty());
    }

    #[test]
    fn test_sticky_empty_members() {
        let a = StickyAssignor;
        let tp = vec![("t".into(), vec![0, 1, 2])];
        let result = a.assign(&[], &tp);
        assert!(result.is_empty());
    }

    #[test]
    fn test_range_partial_subscription() {
        let a = RangeAssignor;
        let members = vec![
            AssignorMember {
                member_id: "m1".into(),
                subscribed_topics: vec!["t1".into()],
            },
            AssignorMember {
                member_id: "m2".into(),
                subscribed_topics: vec!["t2".into()],
            },
            AssignorMember {
                member_id: "m3".into(),
                subscribed_topics: vec!["t1".into(), "t2".into()],
            },
        ];
        let tp = vec![("t1".into(), vec![0, 1, 2, 3]), ("t2".into(), vec![0, 1])];
        let result = a.assign(&members, &tp);
        let flat = flatten(&result);

        // t1 subscribers: m1, m3 → 4 partitions / 2 members = 2 each
        // t2 subscribers: m2, m3 → 2 partitions / 2 members = 1 each
        let m1_t1: Vec<i32> = flat["m1"]
            .iter()
            .filter(|(t, _)| t == "t1")
            .map(|(_, p)| *p)
            .collect();
        let m3_t1: Vec<i32> = flat["m3"]
            .iter()
            .filter(|(t, _)| t == "t1")
            .map(|(_, p)| *p)
            .collect();
        assert_eq!(m1_t1.len(), 2);
        assert_eq!(m3_t1.len(), 2);

        let m2_t2: Vec<i32> = flat["m2"]
            .iter()
            .filter(|(t, _)| t == "t2")
            .map(|(_, p)| *p)
            .collect();
        let m3_t2: Vec<i32> = flat["m3"]
            .iter()
            .filter(|(t, _)| t == "t2")
            .map(|(_, p)| *p)
            .collect();
        assert_eq!(m2_t2.len(), 1);
        assert_eq!(m3_t2.len(), 1);
    }

    #[test]
    fn test_roundrobin_partial_subscription() {
        let a = RoundRobinAssignor;
        let members = vec![
            AssignorMember {
                member_id: "m1".into(),
                subscribed_topics: vec!["t1".into()],
            },
            AssignorMember {
                member_id: "m2".into(),
                subscribed_topics: vec!["t1".into(), "t2".into()],
            },
        ];
        let tp = vec![("t1".into(), vec![0, 1, 2]), ("t2".into(), vec![0, 1])];
        let result = a.assign(&members, &tp);
        let flat = flatten(&result);

        // m1 only subscribes to t1, so gets some t1 partitions
        let m1_topics: HashSet<&String> = flat["m1"].iter().map(|(t, _)| t).collect();
        assert!(!m1_topics.contains(&"t2".to_string()));

        // m2 subscribes to both, gets both
        // Total: t1 has 3, t2 has 2 = 5 partitions assigned
        assert_eq!(total_partitions(&result), 5);
    }

    #[test]
    fn test_sticky_partial_subscription() {
        let a = StickyAssignor;
        let members = vec![
            AssignorMember {
                member_id: "m1".into(),
                subscribed_topics: vec!["t1".into()],
            },
            AssignorMember {
                member_id: "m2".into(),
                subscribed_topics: vec!["t1".into(), "t2".into()],
            },
        ];
        let tp = vec![("t1".into(), vec![0, 1]), ("t2".into(), vec![0, 1])];
        let mut prev_map = HashMap::new();
        prev_map.insert("m1".to_string(), vec![("t1".into(), vec![0])]);
        prev_map.insert(
            "m2".to_string(),
            vec![("t1".into(), vec![1]), ("t2".into(), vec![0, 1])],
        );
        let prev = PreviousAssignment {
            assignments: prev_map,
        };
        let result = a.assign_with_previous(&members, &tp, &prev);
        let flat = flatten(&result);

        // m1 should still have t1:0 (preserved, still subscribed)
        assert!(flat["m1"].contains(&("t1".into(), 0)));
        // m1 should NOT have any t2 partitions (not subscribed)
        assert!(flat["m1"].iter().all(|(t, _)| t != "t2"));
        // All 4 partitions assigned
        assert_eq!(total_partitions(&result), 4);
    }

    #[test]
    fn test_range_many_partitions() {
        let a = RangeAssignor;
        let members: Vec<AssignorMember> = (0..7)
            .map(|i| AssignorMember {
                member_id: format!("m{i}"),
                subscribed_topics: vec!["t".into()],
            })
            .collect();
        let tp = vec![("t".into(), (0..100).collect())];
        let result = a.assign(&members, &tp);

        // 100 partitions / 7 members: first 2 get 15, remaining 5 get 14
        // 2*15 + 5*14 = 30 + 70 = 100
        assert_eq!(total_partitions(&result), 100);
        let flat = flatten(&result);
        let counts: Vec<usize> = (0..7).map(|i| flat[&format!("m{i}")].len()).collect();
        assert!(counts.iter().all(|&c| c == 14 || c == 15));
        assert_eq!(counts.iter().filter(|&&c| c == 15).count(), 2);
    }

    #[test]
    fn test_roundrobin_many_partitions() {
        let a = RoundRobinAssignor;
        let members: Vec<AssignorMember> = (0..7)
            .map(|i| AssignorMember {
                member_id: format!("m{i}"),
                subscribed_topics: vec!["t".into()],
            })
            .collect();
        let tp = vec![("t".into(), (0..100).collect())];
        let result = a.assign(&members, &tp);

        assert_eq!(total_partitions(&result), 100);
        let flat = flatten(&result);
        let counts: Vec<usize> = (0..7).map(|i| flat[&format!("m{i}")].len()).collect();
        // Should be 14 or 15 per member
        assert!(counts.iter().all(|&c| c == 14 || c == 15));
    }

    #[test]
    fn test_sticky_many_members() {
        let a = StickyAssignor;
        let n_members = 20;
        let n_partitions = 60;

        let members: Vec<AssignorMember> = (0..n_members)
            .map(|i| AssignorMember {
                member_id: format!("m{i}"),
                subscribed_topics: vec!["t".into()],
            })
            .collect();
        let tp = vec![("t".into(), (0..n_partitions).collect())];

        // Initial assignment
        let result1 = a.assign(&members, &tp);
        assert_eq!(total_partitions(&result1), n_partitions as usize);

        // Remove member 5, keep rest
        let mut prev_map = HashMap::new();
        for (mid, topics) in &result1 {
            prev_map.insert(mid.clone(), topics.clone());
        }
        let prev = PreviousAssignment {
            assignments: prev_map,
        };

        let members2: Vec<AssignorMember> = (0..n_members)
            .filter(|&i| i != 5)
            .map(|i| AssignorMember {
                member_id: format!("m{i}"),
                subscribed_topics: vec!["t".into()],
            })
            .collect();
        let result2 = a.assign_with_previous(&members2, &tp, &prev);
        assert_eq!(total_partitions(&result2), n_partitions as usize);

        // Count how many partitions moved — should be minimal (only m5's partitions)
        let flat1 = flatten(&result1);
        let flat2 = flatten(&result2);
        let mut moved = 0;
        for (mid, parts) in &flat2 {
            if let Some(old_parts) = flat1.get(mid) {
                for p in parts {
                    if !old_parts.contains(p) {
                        moved += 1;
                    }
                }
            }
        }
        // Only m5's partitions (3) should have moved
        let m5_count = flat1.get("m5").map(|v| v.len()).unwrap_or(0);
        assert_eq!(
            moved, m5_count,
            "only removed member's partitions should move"
        );
    }
}
