//! Re-export of BisqueRaftTypeConfig for multi-raft usage

pub use crate::type_config::BisqueRaftTypeConfig;

/// Backwards-compatible alias
pub type ManiacRaftTypeConfig<D, R> = BisqueRaftTypeConfig<D, R>;
