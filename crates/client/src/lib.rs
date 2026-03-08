//! bisque-client — Unified client for connecting to bisque clusters.
//!
//! Provides a single `BisqueClient` that connects to a bisque cluster over
//! WebSocket for real-time catalog sync and the S3-compatible API for data
//! access. Exposes a DataFusion `SessionContext` for SQL queries across
//! multiple catalogs and storage tiers.
//!
//! # Architecture
//!
//! - One WebSocket connection per client, multiplexing events from N catalogs.
//! - Each catalog maps to a DataFusion `CatalogProvider`, enabling cross-catalog
//!   queries like `SELECT * FROM catalog_a.public.t1 JOIN catalog_b.public.t2`.
//! - Tables are backed by remote Lance datasets accessed through the cluster's
//!   S3-compatible API for hot/warm data and real S3 for cold data.
//! - When the server pushes catalog events, the client atomically swaps in
//!   updated dataset handles for zero-downtime reads.

mod client;
pub mod client_store;
pub mod cold_store;
pub mod ipc;
pub mod s3_store;

pub use client::BisqueClient;
pub use cold_store::CredentialConfig;
pub use s3_store::BisqueRoutingStore;
