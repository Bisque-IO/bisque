//! bisque-lance — Distributed Search & Analytics Storage Engine
//!
//! A Raft-replicated "Hot-Cold" LSM storage pipeline built on the Lance columnar format.
//! Manages the full lifecycle of data from ingestion through local NVMe segments
//! to deep S3 archival.
//!
//! # Architecture
//!
//! Data flows through three tiers:
//! - **Active Segment**: Local NVMe, accepting writes via Raft-replicated AppendRecords
//! - **Sealed Segment**: Local NVMe, read-only, awaiting flush to deep storage
//! - **Deep Storage**: S3 Lance dataset, the long-term archive
//!
//! The Raft state machine tracks segment metadata and drives transitions between tiers.

pub mod codec;
pub mod config;
pub mod engine;
pub mod error;
pub mod ipc;
pub mod query;
pub mod raft;
pub mod state_machine;
pub mod types;

pub use config::{BisqueLanceConfig, IndexSpec};
pub use engine::BisqueLance;
pub use error::{Error, Result};
pub use query::BisqueLanceTableProvider;
pub use raft::{LanceRaftNode, WriteError};
pub use state_machine::LanceStateMachine;
pub use types::{
    CleanupStats, CompactionStats, FlushHandle, FlushState, LanceCommand, LanceResponse,
    SealReason, SegmentCatalog, SegmentId, SnapshotData,
};

/// Raft type configuration for bisque-lance.
///
/// Uses `LanceCommand` as the application request type (D) and
/// `LanceResponse` as the response type (R).
pub type LanceTypeConfig =
    bisque_raft::BisqueRaftTypeConfig<LanceCommand, LanceResponse>;
