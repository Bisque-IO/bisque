//! DataFusion table provider for bisque-lance.
//!
//! `BisqueLanceTableProvider` implements DataFusion's `TableProvider` trait,
//! presenting a unified query view across all three storage tiers:
//!
//! ```text
//! UNION ALL(active_segment, sealed_segment, s3_deep_storage)
//! ```
//!
//! Each tier is scanned via Lance's native `Scanner`, which handles filter,
//! projection, and limit pushdown automatically.

use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_plan::union::UnionExec;
use lance::dataset::scanner::Scanner;
use lance::Dataset;

use crate::engine::BisqueLance;

/// DataFusion table provider that presents a unified view across all storage tiers.
///
/// Query planning always produces a `UnionExec` over whichever tiers are present:
/// - Active segment (local NVMe, read-write)
/// - Sealed segment (local NVMe, read-only)
/// - S3 deep storage (remote)
///
/// Filter pushdown, projection pushdown, and limit pushdown are all delegated
/// to Lance's native `Scanner::create_plan()`.
pub struct BisqueLanceTableProvider {
    engine: Arc<BisqueLance>,
    schema: SchemaRef,
}

impl std::fmt::Debug for BisqueLanceTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BisqueLanceTableProvider")
            .field("schema", &self.schema)
            .finish()
    }
}

impl BisqueLanceTableProvider {
    /// Create a table provider from a BisqueLance engine.
    ///
    /// The schema must be provided (typically from the engine's config or the
    /// active dataset's schema).
    pub fn new(engine: Arc<BisqueLance>, schema: SchemaRef) -> Self {
        Self { engine, schema }
    }

    /// Get the underlying engine.
    pub fn engine(&self) -> &Arc<BisqueLance> {
        &self.engine
    }
}

#[async_trait]
impl TableProvider for BisqueLanceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(3);

        // 1. Active segment
        if let Some(ds) = self.engine.active_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        // 2. Sealed segment
        if let Some(ds) = self.engine.sealed_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        // 3. S3 deep storage
        if let Some(ds) = self.engine.s3_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        if plans.is_empty() {
            // No datasets available — return an empty plan
            let empty_ds = self.engine.active_dataset_snapshot().await;
            match empty_ds {
                Some(ds) => {
                    let mut scan = ds.scan();
                    scan.limit(Some(0), None)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    scan.create_plan()
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                }
                None => Err(DataFusionError::Plan(
                    "no datasets available for query".to_string(),
                )),
            }
        } else {
            UnionExec::try_new(plans)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // Lance handles filter pushdown natively (FTS, scalar, vector).
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

/// Build a DataFusion `ExecutionPlan` for a single Lance dataset.
///
/// Applies projection, filter, and limit pushdown via Lance's `Scanner`.
async fn build_scan_plan(
    dataset: &Dataset,
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let mut scan = dataset.scan();

    // Projection pushdown
    match projection {
        Some(proj) if proj.is_empty() => {
            scan.empty_project()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        Some(proj) => {
            let columns: Vec<&str> = proj
                .iter()
                .filter_map(|&idx| schema.fields().get(idx).map(|f| f.name().as_str()))
                .collect();
            if !columns.is_empty() {
                scan.project(&columns)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
        }
        None => {}
    }

    // Filter pushdown
    apply_filters(&mut scan, filters);

    // Limit pushdown
    scan.limit(limit.map(|l| l as i64), None)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    scan.create_plan()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Apply DataFusion filter expressions to a Lance scanner.
fn apply_filters(scan: &mut Scanner, filters: &[Expr]) {
    let combined = match filters.len() {
        0 => return,
        1 => filters[0].clone(),
        _ => {
            let mut expr = filters[0].clone();
            for filter in &filters[1..] {
                expr = Expr::and(expr, filter.clone());
            }
            expr
        }
    };
    scan.filter_expr(combined);
}
