//! Epoch-based memory reclamation.
//!
//! Two systems:
//!
//! - [`heap`]: Lightweight per-tree epoch with thread-local pin slots,
//!   min-heap garbage, and per-Collector isolation. For heap-allocated
//!   concurrent structures (ART, etc).
//!
//! - Seize-based: General-purpose epoch reclamation for pointer-based
//!   concurrent structures ([`EpochBox`], [`EpochRef`], [`Guard`]).

// Seize-based epoch (vendored).
mod seize;
pub use seize::*;
// Re-export submodule for `crate::epoch::collector::Collector` paths.
pub use seize::collector;

// Heap-epoch reclamation (Collector + Epoch + EpochGuard).
pub mod heap;

// Counted-epoch reclamation — per-epoch reader counters instead of watermark scan.
pub mod counted;
