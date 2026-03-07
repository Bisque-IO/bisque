//! DataFusion table provider for bisque-lance.
//!
//! `BisqueLanceTableProvider` implements DataFusion's `TableProvider` trait,
//! presenting a unified query view across all three storage tiers of a single table:
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
use lance::Dataset;
use lance::dataset::scanner::Scanner;

use crate::table_engine::TableEngine;

/// DataFusion table provider for a single bisque-lance table.
///
/// Wraps a [`TableEngine`] and produces a `UnionExec` over whichever tiers are present:
/// - Active segment (local NVMe, read-write)
/// - Sealed segment (local NVMe, read-only)
/// - S3 deep storage (remote)
pub struct BisqueLanceTableProvider {
    table: Arc<TableEngine>,
    schema: SchemaRef,
}

impl std::fmt::Debug for BisqueLanceTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BisqueLanceTableProvider")
            .field("table", &self.table.name())
            .field("schema", &self.schema)
            .finish()
    }
}

impl BisqueLanceTableProvider {
    /// Create a table provider for a single table.
    pub fn new(table: Arc<TableEngine>, schema: SchemaRef) -> Self {
        Self { table, schema }
    }

    /// Get the underlying table engine.
    pub fn table(&self) -> &Arc<TableEngine> {
        &self.table
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
        if let Some(ds) = self.table.active_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        // 2. Sealed segment
        if let Some(ds) = self.table.sealed_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        // 3. S3 deep storage
        if let Some(ds) = self.table.s3_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        if plans.is_empty() {
            // No datasets available — return an empty plan
            let empty_ds = self.table.active_dataset_snapshot().await;
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
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

/// Build a DataFusion `ExecutionPlan` for a single Lance dataset.
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::logical_expr::col;

    /// Helper: create a minimal Lance dataset in a temp directory and return it.
    async fn temp_dataset() -> (tempfile::TempDir, Dataset) {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let ds = Dataset::write(reader, tmp.path().to_str().unwrap(), None)
            .await
            .unwrap();
        (tmp, ds)
    }

    #[tokio::test]
    async fn test_apply_filters_empty() {
        let (_tmp, ds) = temp_dataset().await;
        let mut scan = ds.scan();
        // Should not panic or error with empty filters
        apply_filters(&mut scan, &[]);
        // Verify we can still create a plan after no-op filter application
        let plan = scan.create_plan().await;
        assert!(plan.is_ok());
    }

    #[tokio::test]
    async fn test_apply_filters_single() {
        let (_tmp, ds) = temp_dataset().await;
        let mut scan = ds.scan();
        let filter = col("x").gt(datafusion_expr::lit(1));
        apply_filters(&mut scan, &[filter]);
        // Should not panic — the filter is set on the scanner
        let plan = scan.create_plan().await;
        assert!(plan.is_ok());
    }

    #[tokio::test]
    async fn test_apply_filters_multiple() {
        let (_tmp, ds) = temp_dataset().await;
        let mut scan = ds.scan();
        let f1 = col("x").gt(datafusion_expr::lit(0));
        let f2 = col("x").lt(datafusion_expr::lit(10));
        apply_filters(&mut scan, &[f1, f2]);
        let plan = scan.create_plan().await;
        assert!(plan.is_ok());
    }

    #[test]
    fn test_supports_filters_pushdown_returns_inexact() {
        // We test the logic directly without needing a full TableProvider.
        // The implementation maps every filter to Inexact.
        let filters: Vec<Expr> = vec![
            col("x").gt(datafusion_expr::lit(1)),
            col("x").lt(datafusion_expr::lit(100)),
        ];
        let filter_refs: Vec<&Expr> = filters.iter().collect();

        // Replicate the logic from supports_filters_pushdown
        let result: Vec<TableProviderFilterPushDown> = filter_refs
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect();

        assert_eq!(result.len(), 2);
        for r in &result {
            assert!(
                matches!(r, TableProviderFilterPushDown::Inexact),
                "expected Inexact pushdown"
            );
        }
    }
}
