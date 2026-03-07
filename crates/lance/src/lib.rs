//! bisque-lance — Distributed Search & Analytics Storage Engine
//!
//! A Raft-replicated "Hot-Cold" LSM storage pipeline built on the Lance columnar format.
//! Manages the full lifecycle of data from ingestion through local NVMe segments
//! to deep S3 archival, with support for multiple independent tables.
//!
//! # Architecture
//!
//! Each table's data flows through three tiers:
//! - **Active Segment**: Local NVMe, accepting writes via Raft-replicated AppendRecords
//! - **Sealed Segment**: Local NVMe, read-only, awaiting flush to deep storage
//! - **Deep Storage**: S3 Lance dataset, the long-term archive
//!
//! The Raft state machine tracks segment metadata per table and drives transitions
//! between tiers.

pub mod async_apply;
pub mod catalog_events;
pub mod client;
pub mod client_store;
pub mod codec;
pub mod cold_store;
pub mod config;
pub mod engine;
pub mod error;
pub mod flight;
pub mod internal_metrics;
pub mod ipc;
pub mod manifest;
pub mod operations;
pub mod otel;
pub mod postgres;
pub mod processors;
pub mod query;
pub mod raft;
pub mod s3_server;
pub mod s3_store;
pub mod segment_sync;
pub mod state_machine;
pub mod table_engine;
pub mod types;
pub mod version_pins;
pub mod write_batcher;
pub mod write_processor;

pub use async_apply::{AppliedWatermark, AsyncApplyConfig};
pub use catalog_events::{CatalogEvent, CatalogEventBus, CatalogEventKind};
pub use client::BisqueClient;
pub use cold_store::CredentialConfig;
pub use config::{BisqueLanceConfig, IndexSpec, TableOpenConfig};
pub use engine::{BisqueLance, SnapshotGuardHandle, SnapshotTransferGuard};
pub use error::{Error, Result};
pub use flight::BisqueFlightService;
pub use manifest::LanceManifestManager;
pub use operations::OperationsManager;
pub use otel::{OtlpReceiver, otel_http_router, serve_http, serve_otlp};
pub use postgres::{PostgresServerConfig, serve_postgres};
pub use processors::{CounterAggregator, GaugeAggregator, HistogramAggregator};
pub use query::BisqueLanceTableProvider;
pub use raft::{LanceRaftNode, WriteError};
pub use s3_server::{S3ServerState, s3_router, serve_s3};
pub use s3_store::BisqueRoutingStore;
pub use segment_sync::{
    SegmentSyncClient, SegmentSyncClientConfig, SegmentSyncServer, SegmentSyncServerConfig,
    SyncResult,
};
pub use state_machine::LanceStateMachine;
pub use table_engine::TableEngine;
pub use types::{
    CleanupStats, CompactionStats, FlushHandle, FlushState, LanceCommand, LanceResponse,
    PersistedBatcherConfig, PersistedIndexSpec, PersistedTableConfig, PersistedTableEntry,
    ProcessorDescriptor, SchemaVersion, SealReason, SegmentCatalog, SegmentId, SnapshotData,
    SnapshotFileEntry, TableSnapshot, WriteResult,
};
pub use version_pins::VersionPinTracker;
pub use write_batcher::WriteBatcherConfig;
pub use write_processor::{MaterializedWrite, ProcessorOutput, WriteProcessor};

/// Raft type configuration for bisque-lance.
///
/// Uses `LanceCommand` as the application request type (D) and
/// `LanceResponse` as the response type (R).
pub type LanceTypeConfig = bisque_raft::BisqueRaftTypeConfig<LanceCommand, LanceResponse>;
