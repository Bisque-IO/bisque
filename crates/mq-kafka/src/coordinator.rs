use std::collections::HashMap;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::types::*;

// =============================================================================
// Group Phase State Machine
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupPhase {
    /// No members.
    Empty,
    /// Waiting for all members to (re)join.
    PreparingRebalance,
    /// Waiting for leader to submit assignments.
    CompletingRebalance,
    /// All members assigned, stable operation.
    Stable,
    /// Group is being removed.
    Dead,
}

impl GroupPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
        }
    }
}

// =============================================================================
// Member State
// =============================================================================

struct MemberState {
    member_id: String,
    client_id: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocol_type: String,
    protocols: Vec<(String, Bytes)>,
    assignment: Bytes,
    last_heartbeat: Instant,
}

// =============================================================================
// Pending Responses
// =============================================================================

struct PendingJoin {
    member_id: String,
    tx: oneshot::Sender<JoinGroupResponse>,
}

struct PendingSync {
    member_id: String,
    tx: oneshot::Sender<SyncGroupResponse>,
}

// =============================================================================
// Group State
// =============================================================================

struct GroupState {
    group_id: String,
    generation_id: i32,
    protocol_type: String,
    protocol_name: String,
    leader: Option<String>,
    members: HashMap<String, MemberState>,
    phase: GroupPhase,
    pending_joins: Vec<PendingJoin>,
    pending_syncs: Vec<PendingSync>,
}

impl GroupState {
    fn new(group_id: String) -> Self {
        Self {
            group_id,
            generation_id: 0,
            protocol_type: String::new(),
            protocol_name: String::new(),
            leader: None,
            members: HashMap::new(),
            phase: GroupPhase::Empty,
            pending_joins: Vec::new(),
            pending_syncs: Vec::new(),
        }
    }

    /// Select a common protocol supported by all members.
    fn select_protocol(&self) -> Option<String> {
        if self.members.is_empty() {
            return None;
        }
        // Find first protocol supported by all members
        let first = self.members.values().next().unwrap();
        for (name, _) in &first.protocols {
            if self
                .members
                .values()
                .all(|m| m.protocols.iter().any(|(n, _)| n == name))
            {
                return Some(name.clone());
            }
        }
        None
    }

    /// Complete the JoinGroup phase: send responses to all pending joins.
    fn complete_join(&mut self) {
        self.generation_id += 1;
        self.protocol_name = self.select_protocol().unwrap_or_default();
        self.phase = GroupPhase::CompletingRebalance;

        let leader_id = self
            .leader
            .clone()
            .unwrap_or_else(|| self.members.keys().next().cloned().unwrap_or_default());
        self.leader = Some(leader_id.clone());

        let members_list: Vec<JoinGroupMember> = self
            .members
            .values()
            .map(|m| {
                let metadata = m
                    .protocols
                    .iter()
                    .find(|(n, _)| *n == self.protocol_name)
                    .map(|(_, d)| d.clone())
                    .unwrap_or_default();
                JoinGroupMember {
                    member_id: m.member_id.clone(),
                    metadata,
                }
            })
            .collect();

        let pending = std::mem::take(&mut self.pending_joins);
        for pj in pending {
            let is_leader = pj.member_id == leader_id;
            let resp = JoinGroupResponse {
                error_code: ErrorCode::None.as_i16(),
                generation_id: self.generation_id,
                protocol_name: self.protocol_name.clone(),
                leader: leader_id.clone(),
                member_id: pj.member_id,
                members: if is_leader {
                    members_list.clone()
                } else {
                    Vec::new()
                },
            };
            let _ = pj.tx.send(resp);
        }
    }

    /// Complete the SyncGroup phase: distribute assignments to all pending syncs.
    fn complete_sync(&mut self, assignments: &[SyncGroupAssignment]) {
        // Store assignments on member state
        for a in assignments {
            if let Some(member) = self.members.get_mut(&a.member_id) {
                member.assignment = a.assignment.clone();
            }
        }

        self.phase = GroupPhase::Stable;

        let pending = std::mem::take(&mut self.pending_syncs);
        for ps in pending {
            let assignment = self
                .members
                .get(&ps.member_id)
                .map(|m| m.assignment.clone())
                .unwrap_or_default();
            let _ = ps.tx.send(SyncGroupResponse {
                error_code: ErrorCode::None.as_i16(),
                assignment,
            });
        }
    }

    /// Remove a member and trigger rebalance if needed.
    fn remove_member(&mut self, member_id: &str) {
        self.members.remove(member_id);
        if self.members.is_empty() {
            self.phase = GroupPhase::Empty;
            self.leader = None;
        } else if self.phase == GroupPhase::Stable {
            // Need to rebalance
            self.phase = GroupPhase::PreparingRebalance;
            // If the leader left, pick a new one
            if self.leader.as_deref() == Some(member_id) {
                self.leader = self.members.keys().next().cloned();
            }
        }
    }
}

// =============================================================================
// Group Coordinator
// =============================================================================

/// Server-side consumer group coordinator implementing Kafka's
/// JoinGroup/SyncGroup/Heartbeat/LeaveGroup protocol.
pub struct GroupCoordinator {
    groups: HashMap<String, GroupState>,
    /// Reverse index: member_id → group_id.
    member_index: HashMap<String, String>,
    next_member_id: u64,
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            member_index: HashMap::new(),
            next_member_id: 1,
        }
    }

    fn generate_member_id(&mut self, client_id: &str) -> String {
        let id = self.next_member_id;
        self.next_member_id += 1;
        format!("{}-{}", client_id, id)
    }

    /// Handle a JoinGroup request. Returns a oneshot receiver that will
    /// deliver the response when the join phase completes.
    pub fn join_group(
        &mut self,
        req: &JoinGroupRequest,
    ) -> Result<oneshot::Receiver<JoinGroupResponse>, JoinGroupResponse> {
        if req.group_id.is_empty() {
            return Err(JoinGroupResponse {
                error_code: ErrorCode::InvalidGroupId.as_i16(),
                generation_id: -1,
                protocol_name: String::new(),
                leader: String::new(),
                member_id: String::new(),
                members: Vec::new(),
            });
        }

        // Assign member_id if empty (before borrowing self.groups)
        let member_id = if req.member_id.is_empty() {
            self.generate_member_id(
                req.protocols
                    .first()
                    .map(|p| p.name.as_str())
                    .unwrap_or("consumer"),
            )
        } else {
            req.member_id.clone()
        };

        let group = self
            .groups
            .entry(req.group_id.clone())
            .or_insert_with(|| GroupState::new(req.group_id.clone()));

        // Register or update member
        let protocols: Vec<(String, Bytes)> = req
            .protocols
            .iter()
            .map(|p| (p.name.clone(), p.metadata.clone()))
            .collect();

        group.members.insert(
            member_id.clone(),
            MemberState {
                member_id: member_id.clone(),
                client_id: member_id.clone(),
                session_timeout_ms: req.session_timeout_ms,
                rebalance_timeout_ms: req.rebalance_timeout_ms,
                protocol_type: req.protocol_type.clone(),
                protocols,
                assignment: Bytes::new(),
                last_heartbeat: Instant::now(),
            },
        );

        self.member_index
            .insert(member_id.clone(), req.group_id.clone());

        if group.protocol_type.is_empty() {
            group.protocol_type = req.protocol_type.clone();
        }

        // Transition to PreparingRebalance
        if group.phase == GroupPhase::Empty || group.phase == GroupPhase::Stable {
            group.phase = GroupPhase::PreparingRebalance;
        }

        let (tx, rx) = oneshot::channel();
        group.pending_joins.push(PendingJoin { member_id, tx });

        // If all known members have joined, complete immediately
        if group.pending_joins.len() >= group.members.len() {
            group.complete_join();
        }

        Ok(rx)
    }

    /// Handle a SyncGroup request. The leader sends assignments; followers wait.
    pub fn sync_group(
        &mut self,
        req: &SyncGroupRequest,
    ) -> Result<oneshot::Receiver<SyncGroupResponse>, SyncGroupResponse> {
        let group = match self.groups.get_mut(&req.group_id) {
            Some(g) => g,
            None => {
                return Err(SyncGroupResponse {
                    error_code: ErrorCode::InvalidGroupId.as_i16(),
                    assignment: Bytes::new(),
                });
            }
        };

        if group.generation_id != req.generation_id {
            return Err(SyncGroupResponse {
                error_code: ErrorCode::IllegalGeneration.as_i16(),
                assignment: Bytes::new(),
            });
        }

        if !group.members.contains_key(&req.member_id) {
            return Err(SyncGroupResponse {
                error_code: ErrorCode::UnknownMemberId.as_i16(),
                assignment: Bytes::new(),
            });
        }

        let (tx, rx) = oneshot::channel();
        group.pending_syncs.push(PendingSync {
            member_id: req.member_id.clone(),
            tx,
        });

        // If this is the leader and they provided assignments, complete
        let is_leader = group.leader.as_deref() == Some(&req.member_id);
        if is_leader && !req.assignments.is_empty() {
            group.complete_sync(&req.assignments);
        } else if group.pending_syncs.len() >= group.members.len() {
            // All members synced but no leader assignments — use empty
            group.complete_sync(&[]);
        }

        Ok(rx)
    }

    /// Handle a Heartbeat request.
    pub fn heartbeat(&mut self, req: &HeartbeatRequest) -> HeartbeatResponse {
        let group = match self.groups.get_mut(&req.group_id) {
            Some(g) => g,
            None => {
                return HeartbeatResponse {
                    error_code: ErrorCode::InvalidGroupId.as_i16(),
                };
            }
        };

        if group.generation_id != req.generation_id {
            return HeartbeatResponse {
                error_code: ErrorCode::IllegalGeneration.as_i16(),
            };
        }

        match group.members.get_mut(&req.member_id) {
            Some(member) => {
                member.last_heartbeat = Instant::now();
            }
            None => {
                return HeartbeatResponse {
                    error_code: ErrorCode::UnknownMemberId.as_i16(),
                };
            }
        }

        if group.phase == GroupPhase::PreparingRebalance {
            HeartbeatResponse {
                error_code: ErrorCode::RebalanceInProgress.as_i16(),
            }
        } else {
            HeartbeatResponse {
                error_code: ErrorCode::None.as_i16(),
            }
        }
    }

    /// Handle a LeaveGroup request.
    pub fn leave_group(&mut self, req: &LeaveGroupRequest) -> LeaveGroupResponse {
        let group = match self.groups.get_mut(&req.group_id) {
            Some(g) => g,
            None => {
                return LeaveGroupResponse {
                    error_code: ErrorCode::InvalidGroupId.as_i16(),
                };
            }
        };

        self.member_index.remove(&req.member_id);
        group.remove_member(&req.member_id);

        if group.phase == GroupPhase::Empty && group.members.is_empty() {
            // Clean up dead group
            self.groups.remove(&req.group_id);
        }

        LeaveGroupResponse {
            error_code: ErrorCode::None.as_i16(),
        }
    }

    /// Check for expired sessions and remove dead members.
    /// Returns the list of group IDs that transitioned to PreparingRebalance.
    pub fn expire_sessions(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut rebalanced = Vec::new();

        let group_ids: Vec<String> = self.groups.keys().cloned().collect();
        for group_id in group_ids {
            let group = self.groups.get_mut(&group_id).unwrap();
            if group.phase == GroupPhase::Dead || group.phase == GroupPhase::Empty {
                continue;
            }

            let expired: Vec<String> = group
                .members
                .iter()
                .filter(|(_, m)| {
                    now.duration_since(m.last_heartbeat).as_millis() as i32 > m.session_timeout_ms
                })
                .map(|(id, _)| id.clone())
                .collect();

            if !expired.is_empty() {
                for mid in &expired {
                    self.member_index.remove(mid);
                    group.remove_member(mid);
                }
                if group.phase == GroupPhase::PreparingRebalance {
                    rebalanced.push(group_id.clone());
                }
            }
        }

        rebalanced
    }

    /// Describe a group (for DescribeGroups API).
    pub fn describe_group(&self, group_id: &str) -> DescribedGroup {
        match self.groups.get(group_id) {
            None => DescribedGroup {
                error_code: ErrorCode::InvalidGroupId.as_i16(),
                group_id: group_id.to_string(),
                state: "Dead".to_string(),
                protocol_type: String::new(),
                protocol: String::new(),
                members: Vec::new(),
            },
            Some(group) => DescribedGroup {
                error_code: ErrorCode::None.as_i16(),
                group_id: group.group_id.clone(),
                state: group.phase.as_str().to_string(),
                protocol_type: group.protocol_type.clone(),
                protocol: group.protocol_name.clone(),
                members: group
                    .members
                    .values()
                    .map(|m| {
                        let metadata = m
                            .protocols
                            .iter()
                            .find(|(n, _)| *n == group.protocol_name)
                            .map(|(_, d)| d.clone())
                            .unwrap_or_default();
                        DescribedGroupMember {
                            member_id: m.member_id.clone(),
                            client_id: m.client_id.clone(),
                            client_host: String::new(),
                            metadata,
                            assignment: m.assignment.clone(),
                        }
                    })
                    .collect(),
            },
        }
    }

    /// List all groups (for ListGroups API).
    pub fn list_groups(&self) -> Vec<ListedGroup> {
        self.groups
            .values()
            .map(|g| ListedGroup {
                group_id: g.group_id.clone(),
                protocol_type: g.protocol_type.clone(),
            })
            .collect()
    }

    /// Remove a member by member_id (used on connection disconnect).
    pub fn remove_member(&mut self, member_id: &str) {
        if let Some(group_id) = self.member_index.remove(member_id) {
            if let Some(group) = self.groups.get_mut(&group_id) {
                group.remove_member(member_id);
                if group.phase == GroupPhase::Empty && group.members.is_empty() {
                    self.groups.remove(&group_id);
                }
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_join_req(group: &str, member: &str, protocol: &str) -> JoinGroupRequest {
        JoinGroupRequest {
            group_id: group.to_string(),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            member_id: member.to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupProtocol {
                name: protocol.to_string(),
                metadata: Bytes::from_static(b"meta"),
            }],
        }
    }

    #[tokio::test]
    async fn test_single_member_join_sync() {
        let mut coord = GroupCoordinator::new();

        // First join with empty member_id -> gets assigned
        let rx = coord.join_group(&make_join_req("g1", "", "range")).unwrap();
        let resp = rx.await.unwrap();
        assert_eq!(resp.error_code, ErrorCode::None.as_i16());
        assert_eq!(resp.generation_id, 1);
        assert!(!resp.member_id.is_empty());
        assert_eq!(resp.leader, resp.member_id); // sole member is leader

        // SyncGroup as leader
        let member_id = resp.member_id.clone();
        let sync_req = SyncGroupRequest {
            group_id: "g1".to_string(),
            generation_id: 1,
            member_id: member_id.clone(),
            assignments: vec![SyncGroupAssignment {
                member_id: member_id.clone(),
                assignment: Bytes::from_static(b"assign1"),
            }],
        };
        let rx = coord.sync_group(&sync_req).unwrap();
        let resp = rx.await.unwrap();
        assert_eq!(resp.error_code, ErrorCode::None.as_i16());
        assert_eq!(resp.assignment, Bytes::from_static(b"assign1"));
    }

    #[tokio::test]
    async fn test_two_member_join() {
        let mut coord = GroupCoordinator::new();

        // First member joins
        let rx1 = coord.join_group(&make_join_req("g1", "", "range")).unwrap();

        // Response comes immediately for first (and only known) member
        let resp1 = rx1.await.unwrap();
        let m1 = resp1.member_id.clone();
        assert_eq!(resp1.generation_id, 1);

        // Complete sync for gen 1
        let sync_rx = coord
            .sync_group(&SyncGroupRequest {
                group_id: "g1".to_string(),
                generation_id: 1,
                member_id: m1.clone(),
                assignments: vec![SyncGroupAssignment {
                    member_id: m1.clone(),
                    assignment: Bytes::from_static(b"a1"),
                }],
            })
            .unwrap();
        let _ = sync_rx.await.unwrap();

        // Second member joins, triggering rebalance
        let rx2 = coord.join_group(&make_join_req("g1", "", "range")).unwrap();

        // First member must re-join
        let rx1 = coord
            .join_group(&make_join_req("g1", &m1, "range"))
            .unwrap();

        // Both should get responses now
        let resp1 = rx1.await.unwrap();
        let resp2 = rx2.await.unwrap();
        assert_eq!(resp1.generation_id, 2);
        assert_eq!(resp2.generation_id, 2);
        // Leader gets member list, follower doesn't
        let leader_resp = if resp1.leader == resp1.member_id {
            &resp1
        } else {
            &resp2
        };
        assert_eq!(leader_resp.members.len(), 2);
    }

    #[test]
    fn test_heartbeat_unknown_group() {
        let mut coord = GroupCoordinator::new();
        let resp = coord.heartbeat(&HeartbeatRequest {
            group_id: "nonexistent".to_string(),
            generation_id: 1,
            member_id: "m1".to_string(),
        });
        assert_eq!(resp.error_code, ErrorCode::InvalidGroupId.as_i16());
    }

    #[tokio::test]
    async fn test_leave_group() {
        let mut coord = GroupCoordinator::new();

        let rx = coord.join_group(&make_join_req("g1", "", "range")).unwrap();
        let resp = rx.await.unwrap();
        let member_id = resp.member_id;

        let leave_resp = coord.leave_group(&LeaveGroupRequest {
            group_id: "g1".to_string(),
            member_id: member_id.clone(),
        });
        assert_eq!(leave_resp.error_code, ErrorCode::None.as_i16());

        // Group should be cleaned up
        assert!(coord.list_groups().is_empty());
    }

    #[test]
    fn test_describe_nonexistent_group() {
        let coord = GroupCoordinator::new();
        let desc = coord.describe_group("nope");
        assert_eq!(desc.error_code, ErrorCode::InvalidGroupId.as_i16());
    }

    #[test]
    fn test_invalid_group_id() {
        let mut coord = GroupCoordinator::new();
        let result = coord.join_group(&make_join_req("", "", "range"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_code, ErrorCode::InvalidGroupId.as_i16());
    }
}
