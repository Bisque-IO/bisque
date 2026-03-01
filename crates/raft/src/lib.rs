//! Tokio runtime adapter for OpenRaft.
//!
//! This crate uses OpenRaft's built-in `TokioRuntime` for async operations.
//!
//! # Features
//!
//! - `multi` (default): Enables the multi-raft module for running multiple Raft groups

#[cfg(feature = "multi")]
pub mod multi;
pub mod type_config;

pub use openraft::TokioRuntime;
pub use type_config::BisqueRaftTypeConfig;
