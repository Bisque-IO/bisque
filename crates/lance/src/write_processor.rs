//! WriteProcessor trait — hooks into the write batcher between batch
//! accumulation and IPC encoding.
//!
//! Processors can reduce data (aggregate-only mode) or produce additional
//! writes to materialized tables (multi-write mode). They are optional and
//! stored in [`WriteBatcherConfig`](crate::WriteBatcherConfig).
//!
//! # Modes
//!
//! - **Stateless** (aggregate-only): `CounterAggregator`, `GaugeAggregator`,
//!   `HistogramAggregator` reduce batches in `process()` and return empty
//!   from `drain()`.
//! - **Stateful** (multi-write): Maintains internal window state using interior
//!   mutability. `drain()` flushes incomplete windows on shutdown.

use arrow_array::RecordBatch;

use crate::types::ProcessorDescriptor;

/// A write to a materialized (derived) table.
///
/// Produced by [`WriteProcessor::process`] when the processor emits additional
/// writes beyond the primary table — e.g., windowed aggregation tables.
#[derive(Debug, Clone)]
pub struct MaterializedWrite {
    /// Target table name for the materialized output.
    pub table_name: String,
    /// RecordBatches to append to the target table.
    pub batches: Vec<RecordBatch>,
}

/// Output of a [`WriteProcessor::process`] call.
#[derive(Debug)]
pub struct ProcessorOutput {
    /// Primary batches that replace the original input for the current table.
    /// If empty, no Raft proposal is made for the primary table.
    pub primary: Vec<RecordBatch>,
    /// Additional writes to materialized tables.
    /// Each entry becomes a separate `AppendRecords` Raft proposal.
    pub materialized: Vec<MaterializedWrite>,
}

impl ProcessorOutput {
    /// Create an output with only primary batches (no materialized writes).
    pub fn primary_only(batches: Vec<RecordBatch>) -> Self {
        Self {
            primary: batches,
            materialized: Vec::new(),
        }
    }

    /// Create an output with primary batches and materialized writes.
    pub fn with_materialized(
        primary: Vec<RecordBatch>,
        materialized: Vec<MaterializedWrite>,
    ) -> Self {
        Self {
            primary,
            materialized,
        }
    }
}

/// Trait for processing write batches between accumulation and IPC encoding.
///
/// # Thread Safety
///
/// Processors are `Send + Sync` because the batcher loop runs on a tokio task.
/// Stateful processors should use `parking_lot::Mutex` for interior mutability.
pub trait WriteProcessor: Send + Sync + 'static {
    /// Process accumulated batches before IPC encoding and Raft proposal.
    ///
    /// `batches` contains all `RecordBatch`es accumulated during the linger
    /// window for a single table. Returns a `ProcessorOutput` with:
    /// - `primary`: batches to propose for the current table (may be reduced)
    /// - `materialized`: additional writes to other tables
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput;

    /// Drain any internal state on shutdown.
    ///
    /// Called when the batcher loop's channel disconnects. Stateless processors
    /// return an empty vec (the default). Stateful processors should flush
    /// any incomplete window aggregations.
    fn drain(&self) -> Vec<MaterializedWrite> {
        Vec::new()
    }

    /// Return a serializable descriptor for this processor, if available.
    ///
    /// Built-in processors (Counter, Gauge, Histogram) return `Some(...)`.
    /// Custom or third-party processors return `None` (the default).
    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        None
    }
}
