//! Built-in [`WriteProcessor`](crate::WriteProcessor) implementations for
//! common aggregation patterns.
//!
//! - [`CounterAggregator`] — sums values per group key (stateless)
//! - [`GaugeAggregator`] — keeps last value per group key (stateless)
//! - [`HistogramAggregator`] — merges histogram bucket data per group key (stateless)

pub mod counter;
pub mod gauge;
pub mod histogram;

pub use counter::CounterAggregator;
pub use gauge::GaugeAggregator;
pub use histogram::HistogramAggregator;
