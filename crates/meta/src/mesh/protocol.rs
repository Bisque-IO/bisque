//! Wire protocol for cluster mesh communication.
//!
//! Uses the same 4-byte LE length-prefixed framing as the Raft transport,
//! with manual `Encode`/`Decode` implementations matching the codec patterns
//! in `bisque_raft::multi::codec`.

use std::io::{Read, Write};

use bisque_raft::multi::codec::{CodecError, Decode, Encode};

// ============================================================================
// NodeHealth
// ============================================================================

/// Health metrics published by each node via heartbeats.
#[derive(Debug, Clone, Default)]
pub struct NodeHealth {
    pub uptime_secs: u64,
    /// CPU usage in hundredths of a percent (0–10000).
    pub cpu_usage_pct: u16,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub active_connections: u32,
    /// Milliseconds since Unix epoch.
    pub timestamp_ms: u64,
}

impl Encode for NodeHealth {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.uptime_secs.encode(w)?;
        self.cpu_usage_pct.encode(w)?;
        self.memory_used_bytes.encode(w)?;
        self.memory_total_bytes.encode(w)?;
        self.active_connections.encode(w)?;
        self.timestamp_ms.encode(w)
    }

    fn encoded_size(&self) -> usize {
        8 + 2 + 8 + 8 + 4 + 8 // 38 bytes fixed
    }
}

impl Decode for NodeHealth {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            uptime_secs: u64::decode(r)?,
            cpu_usage_pct: u16::decode(r)?,
            memory_used_bytes: u64::decode(r)?,
            memory_total_bytes: u64::decode(r)?,
            active_connections: u32::decode(r)?,
            timestamp_ms: u64::decode(r)?,
        })
    }
}

// ============================================================================
// OperationSnapshot
// ============================================================================

/// Wire-format mirror of `bisque_lance::operations::Operation`.
///
/// Uses codec-native types (u8 enums, u64 for f64 bits) to avoid
/// pulling serde into the mesh protocol.
#[derive(Debug, Clone)]
pub struct OperationSnapshot {
    pub id: String,
    pub node_id: u64,
    /// 0 = Compact, 1 = Reindex, 2 = Flush
    pub op_type: u8,
    /// 0 = Hot, 1 = Warm, 2 = Cold
    pub tier: u8,
    pub tenant: String,
    pub catalog: String,
    pub catalog_type: String,
    pub table: String,
    /// 0 = Queued, 1 = Running, 2 = Done, 3 = Failed, 4 = Cancelled
    pub status: u8,
    /// Progress 0.0–1.0 encoded as `f64::to_bits()`.
    pub progress_bits: u64,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub fragments_done: Option<u64>,
    pub fragments_total: Option<u64>,
}

impl Encode for OperationSnapshot {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.id.encode(w)?;
        self.node_id.encode(w)?;
        self.op_type.encode(w)?;
        self.tier.encode(w)?;
        self.tenant.encode(w)?;
        self.catalog.encode(w)?;
        self.catalog_type.encode(w)?;
        self.table.encode(w)?;
        self.status.encode(w)?;
        self.progress_bits.encode(w)?;
        self.created_at.encode(w)?;
        self.started_at.encode(w)?;
        self.finished_at.encode(w)?;
        self.error.encode(w)?;
        self.fragments_done.encode(w)?;
        self.fragments_total.encode(w)
    }

    fn encoded_size(&self) -> usize {
        self.id.encoded_size()
            + 8 // node_id
            + 1 // op_type
            + 1 // tier
            + self.tenant.encoded_size()
            + self.catalog.encoded_size()
            + self.catalog_type.encoded_size()
            + self.table.encoded_size()
            + 1 // status
            + 8 // progress_bits
            + self.created_at.encoded_size()
            + self.started_at.encoded_size()
            + self.finished_at.encoded_size()
            + self.error.encoded_size()
            + self.fragments_done.encoded_size()
            + self.fragments_total.encoded_size()
    }
}

impl Decode for OperationSnapshot {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            id: String::decode(r)?,
            node_id: u64::decode(r)?,
            op_type: u8::decode(r)?,
            tier: u8::decode(r)?,
            tenant: String::decode(r)?,
            catalog: String::decode(r)?,
            catalog_type: String::decode(r)?,
            table: String::decode(r)?,
            status: u8::decode(r)?,
            progress_bits: u64::decode(r)?,
            created_at: String::decode(r)?,
            started_at: Option::<String>::decode(r)?,
            finished_at: Option::<String>::decode(r)?,
            error: Option::<String>::decode(r)?,
            fragments_done: Option::<u64>::decode(r)?,
            fragments_total: Option::<u64>::decode(r)?,
        })
    }
}

// ============================================================================
// MeshMessage
// ============================================================================

/// Discriminant values for `MeshMessage` variants on the wire.
const MSG_HANDSHAKE: u8 = 1;
const MSG_STATE_SNAPSHOT: u8 = 2;
const MSG_OPERATION_UPDATE: u8 = 3;
const MSG_HEARTBEAT: u8 = 4;
const MSG_REQUEST_SNAPSHOT: u8 = 5;
const MSG_CLOSE: u8 = 6;

/// Wire protocol messages exchanged between mesh peers.
#[derive(Debug, Clone)]
pub enum MeshMessage {
    /// First message on any connection. Identifies the sender.
    Handshake {
        node_id: u64,
        mesh_port: u16,
        protocol_version: u8,
    },

    /// Full state snapshot. Sent after handshake and on resync requests.
    StateSnapshot {
        node_id: u64,
        operations: Vec<OperationSnapshot>,
        health: NodeHealth,
        seq: u64,
    },

    /// Incremental update: a single operation changed state.
    OperationUpdate {
        node_id: u64,
        operation: OperationSnapshot,
        seq: u64,
    },

    /// Periodic heartbeat with node health metrics.
    Heartbeat {
        node_id: u64,
        health: NodeHealth,
        seq: u64,
    },

    /// Request a full re-snapshot from the peer.
    RequestSnapshot,

    /// Graceful close notification. Peer should stop reading and close.
    Close,
}

impl Encode for MeshMessage {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            MeshMessage::Handshake {
                node_id,
                mesh_port,
                protocol_version,
            } => {
                MSG_HANDSHAKE.encode(w)?;
                node_id.encode(w)?;
                mesh_port.encode(w)?;
                protocol_version.encode(w)?;
            }
            MeshMessage::StateSnapshot {
                node_id,
                operations,
                health,
                seq,
            } => {
                MSG_STATE_SNAPSHOT.encode(w)?;
                node_id.encode(w)?;
                operations.encode(w)?;
                health.encode(w)?;
                seq.encode(w)?;
            }
            MeshMessage::OperationUpdate {
                node_id,
                operation,
                seq,
            } => {
                MSG_OPERATION_UPDATE.encode(w)?;
                node_id.encode(w)?;
                operation.encode(w)?;
                seq.encode(w)?;
            }
            MeshMessage::Heartbeat {
                node_id,
                health,
                seq,
            } => {
                MSG_HEARTBEAT.encode(w)?;
                node_id.encode(w)?;
                health.encode(w)?;
                seq.encode(w)?;
            }
            MeshMessage::RequestSnapshot => {
                MSG_REQUEST_SNAPSHOT.encode(w)?;
            }
            MeshMessage::Close => {
                MSG_CLOSE.encode(w)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            MeshMessage::Handshake {
                node_id,
                mesh_port,
                protocol_version,
            } => node_id.encoded_size() + mesh_port.encoded_size() + protocol_version.encoded_size(),
            MeshMessage::StateSnapshot {
                node_id,
                operations,
                health,
                seq,
            } => {
                node_id.encoded_size()
                    + operations.encoded_size()
                    + health.encoded_size()
                    + seq.encoded_size()
            }
            MeshMessage::OperationUpdate {
                node_id,
                operation,
                seq,
            } => node_id.encoded_size() + operation.encoded_size() + seq.encoded_size(),
            MeshMessage::Heartbeat {
                node_id,
                health,
                seq,
            } => node_id.encoded_size() + health.encoded_size() + seq.encoded_size(),
            MeshMessage::RequestSnapshot => 0,
            MeshMessage::Close => 0,
        }
    }
}

impl Decode for MeshMessage {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let tag = u8::decode(r)?;
        match tag {
            MSG_HANDSHAKE => Ok(MeshMessage::Handshake {
                node_id: u64::decode(r)?,
                mesh_port: u16::decode(r)?,
                protocol_version: u8::decode(r)?,
            }),
            MSG_STATE_SNAPSHOT => Ok(MeshMessage::StateSnapshot {
                node_id: u64::decode(r)?,
                operations: Vec::<OperationSnapshot>::decode(r)?,
                health: NodeHealth::decode(r)?,
                seq: u64::decode(r)?,
            }),
            MSG_OPERATION_UPDATE => Ok(MeshMessage::OperationUpdate {
                node_id: u64::decode(r)?,
                operation: OperationSnapshot::decode(r)?,
                seq: u64::decode(r)?,
            }),
            MSG_HEARTBEAT => Ok(MeshMessage::Heartbeat {
                node_id: u64::decode(r)?,
                health: NodeHealth::decode(r)?,
                seq: u64::decode(r)?,
            }),
            MSG_REQUEST_SNAPSHOT => Ok(MeshMessage::RequestSnapshot),
            MSG_CLOSE => Ok(MeshMessage::Close),
            _ => Err(CodecError::InvalidDiscriminant(tag)),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T: Encode + Decode + std::fmt::Debug>(msg: &T) -> T {
        let bytes = msg.encode_to_vec().unwrap();
        assert_eq!(bytes.len(), msg.encoded_size());
        T::decode_from_slice(&bytes).unwrap()
    }

    #[test]
    fn test_node_health_roundtrip() {
        let h = NodeHealth {
            uptime_secs: 3600,
            cpu_usage_pct: 4250,
            memory_used_bytes: 1024 * 1024 * 512,
            memory_total_bytes: 1024 * 1024 * 1024 * 8,
            active_connections: 42,
            timestamp_ms: 1709712000000,
        };
        let h2 = roundtrip(&h);
        assert_eq!(h.uptime_secs, h2.uptime_secs);
        assert_eq!(h.cpu_usage_pct, h2.cpu_usage_pct);
        assert_eq!(h.memory_used_bytes, h2.memory_used_bytes);
        assert_eq!(h.memory_total_bytes, h2.memory_total_bytes);
        assert_eq!(h.active_connections, h2.active_connections);
        assert_eq!(h.timestamp_ms, h2.timestamp_ms);
    }

    #[test]
    fn test_operation_snapshot_roundtrip() {
        let op = OperationSnapshot {
            id: "abc-123".into(),
            node_id: 1,
            op_type: 1, // Reindex
            tier: 2,    // Cold
            tenant: "Acme".into(),
            catalog: "analytics".into(),
            catalog_type: "Lance".into(),
            table: "page_views".into(),
            status: 1, // Running
            progress_bits: 0.65_f64.to_bits(),
            created_at: "2026-03-06T10:00:00Z".into(),
            started_at: Some("2026-03-06T10:00:02Z".into()),
            finished_at: None,
            error: None,
            fragments_done: Some(130),
            fragments_total: Some(200),
        };
        let op2 = roundtrip(&op);
        assert_eq!(op.id, op2.id);
        assert_eq!(op.node_id, op2.node_id);
        assert_eq!(op.op_type, op2.op_type);
        assert_eq!(op.tier, op2.tier);
        assert_eq!(op.tenant, op2.tenant);
        assert_eq!(op.catalog, op2.catalog);
        assert_eq!(op.table, op2.table);
        assert_eq!(op.status, op2.status);
        assert_eq!(
            f64::from_bits(op.progress_bits),
            f64::from_bits(op2.progress_bits)
        );
        assert_eq!(op.started_at, op2.started_at);
        assert_eq!(op.finished_at, op2.finished_at);
        assert_eq!(op.error, op2.error);
        assert_eq!(op.fragments_done, op2.fragments_done);
        assert_eq!(op.fragments_total, op2.fragments_total);
    }

    #[test]
    fn test_handshake_roundtrip() {
        let msg = MeshMessage::Handshake {
            node_id: 42,
            mesh_port: 3201,
            protocol_version: 1,
        };
        let msg2 = roundtrip(&msg);
        match msg2 {
            MeshMessage::Handshake {
                node_id,
                mesh_port,
                protocol_version,
            } => {
                assert_eq!(node_id, 42);
                assert_eq!(mesh_port, 3201);
                assert_eq!(protocol_version, 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_state_snapshot_roundtrip() {
        let msg = MeshMessage::StateSnapshot {
            node_id: 1,
            operations: vec![OperationSnapshot {
                id: "op-1".into(),
                node_id: 1,
                op_type: 0,
                tier: 0,
                tenant: "T".into(),
                catalog: "C".into(),
                catalog_type: "Lance".into(),
                table: "tbl".into(),
                status: 1,
                progress_bits: 0.5_f64.to_bits(),
                created_at: "now".into(),
                started_at: None,
                finished_at: None,
                error: None,
                fragments_done: None,
                fragments_total: None,
            }],
            health: NodeHealth::default(),
            seq: 99,
        };
        let msg2 = roundtrip(&msg);
        match msg2 {
            MeshMessage::StateSnapshot {
                node_id,
                operations,
                seq,
                ..
            } => {
                assert_eq!(node_id, 1);
                assert_eq!(operations.len(), 1);
                assert_eq!(operations[0].id, "op-1");
                assert_eq!(seq, 99);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_heartbeat_roundtrip() {
        let msg = MeshMessage::Heartbeat {
            node_id: 3,
            health: NodeHealth {
                uptime_secs: 100,
                ..Default::default()
            },
            seq: 7,
        };
        let msg2 = roundtrip(&msg);
        match msg2 {
            MeshMessage::Heartbeat {
                node_id, seq, health, ..
            } => {
                assert_eq!(node_id, 3);
                assert_eq!(seq, 7);
                assert_eq!(health.uptime_secs, 100);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_request_snapshot_roundtrip() {
        let msg = MeshMessage::RequestSnapshot;
        let msg2 = roundtrip(&msg);
        assert!(matches!(msg2, MeshMessage::RequestSnapshot));
    }

    #[test]
    fn test_close_roundtrip() {
        let msg = MeshMessage::Close;
        let msg2 = roundtrip(&msg);
        assert!(matches!(msg2, MeshMessage::Close));
    }

    #[test]
    fn test_operation_update_roundtrip() {
        let msg = MeshMessage::OperationUpdate {
            node_id: 2,
            operation: OperationSnapshot {
                id: "op-2".into(),
                node_id: 2,
                op_type: 2,
                tier: 1,
                tenant: "X".into(),
                catalog: "Y".into(),
                catalog_type: "Lance".into(),
                table: "Z".into(),
                status: 3,
                progress_bits: 0.45_f64.to_bits(),
                created_at: "ts".into(),
                started_at: Some("ts2".into()),
                finished_at: Some("ts3".into()),
                error: Some("timeout".into()),
                fragments_done: Some(36),
                fragments_total: Some(80),
            },
            seq: 15,
        };
        let msg2 = roundtrip(&msg);
        match msg2 {
            MeshMessage::OperationUpdate {
                node_id,
                operation,
                seq,
            } => {
                assert_eq!(node_id, 2);
                assert_eq!(seq, 15);
                assert_eq!(operation.id, "op-2");
                assert_eq!(operation.error, Some("timeout".into()));
                assert_eq!(operation.fragments_done, Some(36));
            }
            _ => panic!("wrong variant"),
        }
    }
}
