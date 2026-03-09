//! bisque-meta — Multi-Tenant Control Plane
//!
//! Manages tenants, catalogs (raft groups), API keys, and routing for the
//! bisque distributed cluster. Each catalog runs exactly one engine module
//! (lance, mq, libsql, etc.) and is backed by its own raft group.
//!
//! bisque-meta is itself a raft group, using the same `MultiRaftManager`
//! infrastructure as engine modules.
//!
//! # Architecture
//!
//! ```text
//! Cluster
//!  └─ Tenant
//!      ├─ Catalog "analytics"  (bisque-lance)
//!      ├─ Catalog "events"     (bisque-mq)
//!      └─ Catalog "app_db"     (bisque-libsql)
//!  └─ Tenant "bisque" (system)
//!      └─ Catalog "sys"        (bisque-lance, per-node OTel)
//! ```
//!
//! Clients connect directly to engine nodes using scoped tokens issued
//! by bisque-meta. Each protocol (Flight SQL, Postgres, OTel HTTP, S3)
//! uses its native mechanism to identify the target catalog.

pub mod config;
pub mod engine;
pub mod error;
pub mod mesh;
pub mod node;
pub mod state_machine;
pub mod token;
pub mod types;

pub use config::MetaConfig;
pub use engine::MetaEngine;
pub use error::{Error, Result};
pub use node::{MetaRaftNode, WriteError};
pub use state_machine::MetaStateMachine;
pub use token::TokenManager;
pub use types::{
    Account, AccountMembership, AccountRole, ApiKey, CatalogAccess, CatalogEntry, CatalogGrant,
    CatalogStatus, EngineType, MetaCommand, MetaResponse, MetaSnapshotData, RoutingEntry, Scope,
    Tenant, TenantGrant, TenantLimits, UsageReport, User,
};

/// Raft type configuration for bisque-meta.
///
/// Uses `MetaCommand` as the application request type (D) and
/// `MetaResponse` as the response type (R).
pub type MetaTypeConfig = bisque_raft::BisqueRaftTypeConfig<MetaCommand, MetaResponse>;
