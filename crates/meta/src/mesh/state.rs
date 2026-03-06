//! Merged cluster-wide ephemeral state.
//!
//! Each node's mesh maintains a `ClusterState` that holds the latest
//! [`NodeState`] received from every peer. HTTP/WebSocket handlers query
//! this to return cluster-wide results (e.g. all operations across all nodes).

use std::collections::HashMap;
use std::time::Instant;

use dashmap::DashMap;

use super::protocol::{NodeHealth, OperationSnapshot};

/// Ephemeral state published by a single remote node.
#[derive(Debug, Clone)]
pub struct NodeState {
    pub node_id: u64,
    /// All operations currently tracked by this node, keyed by operation ID.
    pub operations: HashMap<String, OperationSnapshot>,
    /// Latest health metrics from this node.
    pub health: NodeHealth,
    /// Last sequence number received from this node.
    pub last_seq: u64,
    /// When we last received any message from this node.
    pub last_seen: Instant,
    /// Whether the node is considered reachable.
    pub reachable: bool,
}

impl NodeState {
    fn new(node_id: u64) -> Self {
        Self {
            node_id,
            operations: HashMap::new(),
            health: NodeHealth::default(),
            last_seq: 0,
            last_seen: Instant::now(),
            reachable: true,
        }
    }
}

/// The merged cluster-wide ephemeral state.
///
/// Thread-safe via `DashMap` — readers (HTTP handlers, WebSocket) never
/// block writers (mesh receiver tasks).
pub struct ClusterState {
    /// Per-node ephemeral state, keyed by node_id.
    /// Does NOT include the local node — local state is queried directly
    /// from `OperationsManager`.
    nodes: DashMap<u64, NodeState>,
    /// The local node's ID.
    local_node_id: u64,
}

impl ClusterState {
    pub fn new(local_node_id: u64) -> Self {
        Self {
            nodes: DashMap::new(),
            local_node_id,
        }
    }

    /// Apply a full state snapshot from a peer, replacing all its state.
    pub fn apply_snapshot(
        &self,
        node_id: u64,
        operations: Vec<OperationSnapshot>,
        health: NodeHealth,
        seq: u64,
    ) {
        let ops_map: HashMap<String, OperationSnapshot> =
            operations.into_iter().map(|op| (op.id.clone(), op)).collect();

        self.nodes.insert(
            node_id,
            NodeState {
                node_id,
                operations: ops_map,
                health,
                last_seq: seq,
                last_seen: Instant::now(),
                reachable: true,
            },
        );
    }

    /// Apply an incremental operation update from a peer.
    ///
    /// Returns `true` if a sequence gap was detected (messages were skipped).
    pub fn apply_operation_update(&self, node_id: u64, op: OperationSnapshot, seq: u64) -> bool {
        let mut entry = self
            .nodes
            .entry(node_id)
            .or_insert_with(|| NodeState::new(node_id));
        let gap = entry.last_seq > 0 && seq > entry.last_seq + 1;
        entry.operations.insert(op.id.clone(), op);
        entry.last_seq = seq;
        entry.last_seen = Instant::now();
        entry.reachable = true;
        gap
    }

    /// Apply a heartbeat, updating health and last_seen.
    ///
    /// Returns `true` if a sequence gap was detected (messages were skipped).
    pub fn apply_heartbeat(&self, node_id: u64, health: NodeHealth, seq: u64) -> bool {
        let mut entry = self
            .nodes
            .entry(node_id)
            .or_insert_with(|| NodeState::new(node_id));
        let gap = entry.last_seq > 0 && seq > entry.last_seq + 1;
        entry.health = health;
        entry.last_seq = seq;
        entry.last_seen = Instant::now();
        entry.reachable = true;
        gap
    }

    /// Mark a peer as unreachable (no heartbeat within timeout).
    pub fn mark_unreachable(&self, node_id: u64) {
        if let Some(mut entry) = self.nodes.get_mut(&node_id) {
            entry.reachable = false;
        }
    }

    /// Remove a peer entirely (node left the cluster).
    pub fn remove_peer(&self, node_id: u64) {
        self.nodes.remove(&node_id);
    }

    /// Get all remote operations as snapshots.
    /// The caller should merge these with local operations from the `OperationsManager`.
    pub fn all_remote_operations(&self) -> Vec<OperationSnapshot> {
        let mut ops = Vec::new();
        for entry in self.nodes.iter() {
            if entry.reachable {
                ops.extend(entry.operations.values().cloned());
            }
        }
        ops
    }

    /// Get health for all known remote nodes.
    pub fn all_node_health(&self) -> Vec<(u64, NodeHealth, bool)> {
        self.nodes
            .iter()
            .map(|entry| (entry.node_id, entry.health.clone(), entry.reachable))
            .collect()
    }

    /// Get the set of known peer node IDs.
    pub fn peer_node_ids(&self) -> Vec<u64> {
        self.nodes.iter().map(|e| e.node_id).collect()
    }

    /// Peek at a peer's last_seen timestamp (for liveness checks).
    pub fn peek_last_seen(&self, node_id: u64) -> Option<Instant> {
        self.nodes.get(&node_id).map(|e| e.last_seen)
    }

    /// Get the local node ID.
    pub fn local_node_id(&self) -> u64 {
        self.local_node_id
    }

    /// Evict completed/failed operations older than `max_age` from all peers.
    ///
    /// Operations with status 2 (Done), 3 (Failed), or 4 (Cancelled) that have
    /// a `finished_at` timestamp older than `max_age` are removed to prevent
    /// unbounded memory growth.
    pub fn evict_stale_operations(&self, max_age: std::time::Duration) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let cutoff_ms = now_ms - max_age.as_millis() as i64;

        for mut entry in self.nodes.iter_mut() {
            entry.operations.retain(|_, op| {
                // Only evict terminal states: Done(2), Failed(3), Cancelled(4)
                if op.status < 2 {
                    return true;
                }
                // Parse finished_at as RFC3339 and check against cutoff
                if let Some(ref finished) = op.finished_at {
                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(finished) {
                        return dt.timestamp_millis() > cutoff_ms;
                    }
                }
                // No finished_at on a terminal op — evict it
                false
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_op(id: &str, node_id: u64, status: u8) -> OperationSnapshot {
        OperationSnapshot {
            id: id.into(),
            node_id,
            op_type: 0,
            tier: 0,
            tenant: "T".into(),
            catalog: "C".into(),
            catalog_type: "Lance".into(),
            table: "tbl".into(),
            status,
            progress_bits: 0.0_f64.to_bits(),
            created_at: "now".into(),
            started_at: None,
            finished_at: None,
            error: None,
            fragments_done: None,
            fragments_total: None,
        }
    }

    #[test]
    fn test_apply_snapshot_replaces_state() {
        let state = ClusterState::new(1);

        // Initial snapshot from node 2
        state.apply_snapshot(
            2,
            vec![make_op("op-1", 2, 1), make_op("op-2", 2, 0)],
            NodeHealth::default(),
            1,
        );
        assert_eq!(state.all_remote_operations().len(), 2);

        // New snapshot replaces everything
        state.apply_snapshot(2, vec![make_op("op-3", 2, 2)], NodeHealth::default(), 5);
        let ops = state.all_remote_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].id, "op-3");
    }

    #[test]
    fn test_incremental_updates() {
        let state = ClusterState::new(1);

        state.apply_operation_update(2, make_op("op-1", 2, 1), 1);
        state.apply_operation_update(2, make_op("op-2", 2, 0), 2);
        assert_eq!(state.all_remote_operations().len(), 2);

        // Update existing op
        state.apply_operation_update(2, make_op("op-1", 2, 2), 3);
        let ops = state.all_remote_operations();
        assert_eq!(ops.len(), 2);
        let op1 = ops.iter().find(|o| o.id == "op-1").unwrap();
        assert_eq!(op1.status, 2); // Done
    }

    #[test]
    fn test_unreachable_excludes_from_results() {
        let state = ClusterState::new(1);
        state.apply_operation_update(2, make_op("op-1", 2, 1), 1);
        assert_eq!(state.all_remote_operations().len(), 1);

        state.mark_unreachable(2);
        assert_eq!(state.all_remote_operations().len(), 0);
    }

    #[test]
    fn test_multiple_nodes() {
        let state = ClusterState::new(1);
        state.apply_operation_update(2, make_op("op-a", 2, 1), 1);
        state.apply_operation_update(3, make_op("op-b", 3, 0), 1);

        assert_eq!(state.all_remote_operations().len(), 2);
        assert_eq!(state.peer_node_ids().len(), 2);
    }

    #[test]
    fn test_sequence_gap_detection() {
        let state = ClusterState::new(1);

        // Sequential updates: no gap
        assert!(!state.apply_operation_update(2, make_op("op-1", 2, 1), 1));
        assert!(!state.apply_operation_update(2, make_op("op-2", 2, 0), 2));
        assert!(!state.apply_operation_update(2, make_op("op-3", 2, 1), 3));

        // Gap: seq jumps from 3 to 5
        assert!(state.apply_operation_update(2, make_op("op-4", 2, 1), 5));

        // Heartbeat gap detection
        assert!(!state.apply_heartbeat(2, NodeHealth::default(), 6));
        assert!(state.apply_heartbeat(2, NodeHealth::default(), 9)); // gap: 6→9

        // First message from a new peer: no gap (last_seq is 0)
        assert!(!state.apply_operation_update(3, make_op("op-a", 3, 0), 5));
    }

    #[test]
    fn test_remove_peer() {
        let state = ClusterState::new(1);
        state.apply_operation_update(2, make_op("op-1", 2, 1), 1);
        assert_eq!(state.peer_node_ids().len(), 1);

        state.remove_peer(2);
        assert_eq!(state.peer_node_ids().len(), 0);
        assert_eq!(state.all_remote_operations().len(), 0);
    }

    fn make_op_with_finished(id: &str, node_id: u64, status: u8, finished_at: Option<&str>) -> OperationSnapshot {
        OperationSnapshot {
            id: id.into(),
            node_id,
            op_type: 0,
            tier: 0,
            tenant: "T".into(),
            catalog: "C".into(),
            catalog_type: "Lance".into(),
            table: "tbl".into(),
            status,
            progress_bits: 0.0_f64.to_bits(),
            created_at: "2026-01-01T00:00:00Z".into(),
            started_at: None,
            finished_at: finished_at.map(|s| s.into()),
            error: None,
            fragments_done: None,
            fragments_total: None,
        }
    }

    #[test]
    fn test_evict_stale_operations() {
        let state = ClusterState::new(1);

        // Running op — should never be evicted
        state.apply_operation_update(2, make_op("running", 2, 1), 1);

        // Done op with recent finish — should NOT be evicted
        let recent = chrono::Utc::now().to_rfc3339();
        state.apply_operation_update(2, make_op_with_finished("done-recent", 2, 2, Some(&recent)), 2);

        // Done op with old finish — should be evicted
        state.apply_operation_update(2, make_op_with_finished("done-old", 2, 2, Some("2020-01-01T00:00:00Z")), 3);

        // Failed op with no finished_at — should be evicted (terminal with no timestamp)
        state.apply_operation_update(2, make_op_with_finished("failed-no-ts", 2, 3, None), 4);

        assert_eq!(state.all_remote_operations().len(), 4);

        // Evict ops finished more than 60 seconds ago
        state.evict_stale_operations(std::time::Duration::from_secs(60));

        let ops = state.all_remote_operations();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().any(|o| o.id == "running"));
        assert!(ops.iter().any(|o| o.id == "done-recent"));
    }
}
