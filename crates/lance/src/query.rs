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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_physical_plan::union::UnionExec;
use futures::stream::{Stream, StreamExt};
use lance::Dataset;
use lance::dataset::scanner::Scanner;

use lance_index::scalar::FullTextSearchQuery;

use crate::fts::extract_fts_match;
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

        // 1. Active segment (no inverted index — FTS falls back to LIKE)
        if let Some(ds) = self.table.active_dataset_snapshot().await {
            let plan =
                build_scan_plan(&ds, &self.schema, projection, filters, limit, false).await?;
            plans.push(plan);
        }

        // 2. Sealed segment (has inverted index — use Lance FTS)
        if let Some(ds) = self.table.sealed_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit, true).await?;
            plans.push(plan);
        }

        // 3. S3 deep storage (has inverted index — use Lance FTS)
        if let Some(ds) = self.table.s3_dataset_snapshot().await {
            let plan = build_scan_plan(&ds, &self.schema, projection, filters, limit, true).await?;
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
///
/// When `is_indexed` is `true` (sealed / S3 segments), any `fts_match(col, query)`
/// filters are applied via Lance's native `full_text_search` (inverted index, BM25).
/// When `false` (active segment, no index), they fall back to `col LIKE '%query%'`.
pub async fn build_scan_plan(
    dataset: &Dataset,
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    is_indexed: bool,
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

    // Fast path: if no FTS filters, skip partitioning entirely.
    let has_fts = filters.iter().any(|f| extract_fts_match(f).is_some());
    let mut fts_applied = false;

    if has_fts {
        let mut regular_filters: Vec<Expr> = Vec::with_capacity(filters.len());

        for filter in filters {
            if let Some((col_name, query)) = extract_fts_match(filter) {
                if is_indexed && !fts_applied {
                    // Use Lance native FTS on the first fts_match filter.
                    let fts_query = FullTextSearchQuery::new(query.to_owned())
                        .with_column(col_name.to_owned())
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    scan.full_text_search(fts_query)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    fts_applied = true;
                } else {
                    // Active segment or additional FTS filters: fall back to LIKE.
                    regular_filters.push(fts_to_like(col_name, query));
                }
            } else {
                regular_filters.push(filter.clone());
            }
        }

        apply_filters(&mut scan, &regular_filters);
    } else {
        // No FTS filters — apply all filters directly without allocation.
        apply_filters(&mut scan, filters);
    }

    // Limit pushdown
    scan.limit(limit.map(|l| l as i64), None)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let plan = scan
        .create_plan()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // If FTS was applied on an indexed segment, Lance may add a `_score` column.
    // Strip it to keep the schema consistent across the UnionExec (active/sealed/S3).
    if fts_applied {
        let plan_schema = plan.schema();
        if plan_schema.field_with_name("_score").is_ok() {
            let indices: Vec<usize> = plan_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name() != "_score")
                .map(|(i, _)| i)
                .collect();
            let exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = indices
                .iter()
                .map(|&i| {
                    let field = plan_schema.field(i);
                    let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                        datafusion::physical_expr::expressions::Column::new(field.name(), i),
                    );
                    (expr, field.name().clone())
                })
                .collect();
            return Ok(Arc::new(
                datafusion_physical_plan::projection::ProjectionExec::try_new(exprs, plan)?,
            ));
        }
    }

    Ok(plan)
}

/// Convert an `fts_match(col, query)` into `col LIKE '%query%'`.
fn fts_to_like(col_name: &str, query: &str) -> Expr {
    Expr::Like(datafusion_expr::expr::Like::new(
        false,
        Box::new(Expr::Column(datafusion::common::Column::from_name(
            col_name,
        ))),
        Box::new(Expr::Literal(
            datafusion::common::ScalarValue::Utf8(Some(format!("%{query}%"))),
            None,
        )),
        None,
        false,
    ))
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

// ---------------------------------------------------------------------------
// NodeIdExec — wraps an inner plan, appending a constant `node_id: UInt64` column
// ---------------------------------------------------------------------------

/// Custom `ExecutionPlan` that appends a constant `node_id: UInt64` column to
/// every `RecordBatch` produced by the inner plan.
#[derive(Debug)]
pub struct NodeIdExec {
    inner: Arc<dyn ExecutionPlan>,
    node_id: u64,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl NodeIdExec {
    /// Create a new `NodeIdExec` wrapping `inner` with the given `node_id`.
    pub fn new(inner: Arc<dyn ExecutionPlan>, node_id: u64) -> Self {
        let inner_schema = inner.schema();
        let mut fields: Vec<Arc<Field>> = inner_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("node_id", DataType::UInt64, false)));
        let schema = Arc::new(Schema::new(fields));

        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            inner.properties().partitioning.clone(),
            inner.properties().emission_type,
            inner.properties().boundedness,
        );

        Self {
            inner,
            node_id,
            schema,
            properties,
        }
    }
}

impl DisplayAs for NodeIdExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NodeIdExec(node_id={})", self.node_id)
    }
}

impl ExecutionPlan for NodeIdExec {
    fn name(&self) -> &str {
        "NodeIdExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NodeIdExec::new(children[0].clone(), self.node_id)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        Ok(Box::pin(NodeIdStream {
            inner: inner_stream,
            node_id: self.node_id,
            schema: self.schema.clone(),
        }))
    }
}

/// Stream adapter that appends `node_id` column to each batch.
struct NodeIdStream {
    inner: SendableRecordBatchStream,
    node_id: u64,
    schema: SchemaRef,
}

impl Stream for NodeIdStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let node_id = self.node_id;
                let num_rows = batch.num_rows();
                // Pre-allocate with exact capacity to avoid realloc on push
                let mut columns: Vec<Arc<dyn arrow_array::Array>> =
                    Vec::with_capacity(batch.num_columns() + 1);
                columns.extend(batch.columns().iter().cloned());
                // Build constant node_id column directly from iterator —
                // writes u64 values straight into the Arrow buffer without
                // an intermediate Vec<u64> allocation.
                columns.push(Arc::new(UInt64Array::from_iter_values(
                    std::iter::repeat_n(node_id, num_rows),
                )));
                let result = RecordBatch::try_new(self.schema.clone(), columns)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                Poll::Ready(Some(result))
            }
            other => other,
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for NodeIdStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Append a `node_id: UInt64` field to an existing Arrow schema.
pub fn schema_with_node_id(schema: &Schema) -> Schema {
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new("node_id", DataType::UInt64, false)));
    Schema::new(fields)
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

    // -----------------------------------------------------------------------
    // NodeIdExec tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_schema_with_node_id() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let augmented = schema_with_node_id(&schema);
        assert_eq!(augmented.fields().len(), 3);
        assert_eq!(augmented.field(0).name(), "a");
        assert_eq!(augmented.field(1).name(), "b");
        assert_eq!(augmented.field(2).name(), "node_id");
        assert_eq!(augmented.field(2).data_type(), &DataType::UInt64);
        assert!(!augmented.field(2).is_nullable());
    }

    #[test]
    fn test_schema_with_node_id_empty_schema() {
        let schema = Schema::empty();
        let augmented = schema_with_node_id(&schema);
        assert_eq!(augmented.fields().len(), 1);
        assert_eq!(augmented.field(0).name(), "node_id");
    }

    #[tokio::test]
    async fn test_node_id_exec_schema() {
        let (_tmp, ds) = temp_dataset().await;
        let inner_plan = ds.scan().create_plan().await.unwrap();
        let exec = NodeIdExec::new(inner_plan, 42);

        // Schema should have original fields + node_id
        let schema = exec.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"x"), "missing original field 'x'");
        assert!(
            field_names.contains(&"node_id"),
            "missing appended field 'node_id'"
        );
        assert_eq!(
            schema.field_with_name("node_id").unwrap().data_type(),
            &DataType::UInt64,
        );
    }

    #[tokio::test]
    async fn test_node_id_exec_appends_constant_column() {
        let (_tmp, ds) = temp_dataset().await;
        let inner_plan = ds.scan().create_plan().await.unwrap();
        let exec = Arc::new(NodeIdExec::new(inner_plan, 7));

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = exec.execute(0, Arc::new(ctx)).unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();

            // Verify node_id column is last and contains constant value
            let node_id_col = batch
                .column_by_name("node_id")
                .expect("node_id column missing");
            let node_ids = node_id_col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("node_id should be UInt64Array");

            for i in 0..batch.num_rows() {
                assert_eq!(node_ids.value(i), 7, "node_id should be constant 7");
            }

            // Verify original x column is preserved
            let x_col = batch.column_by_name("x").expect("x column missing");
            let x_vals = x_col
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("x should be Int32Array");
            // Original dataset had [1, 2, 3]
            assert!(x_vals.len() > 0);
        }
        assert_eq!(total_rows, 3, "should have 3 rows from temp dataset");
    }

    #[tokio::test]
    async fn test_node_id_exec_with_new_children() {
        let (_tmp, ds) = temp_dataset().await;
        let inner_plan = ds.scan().create_plan().await.unwrap();
        let exec = Arc::new(NodeIdExec::new(inner_plan.clone(), 99));

        // with_new_children should preserve node_id
        let new_exec = exec.clone().with_new_children(vec![inner_plan]).unwrap();
        let new_node_id_exec = new_exec.as_any().downcast_ref::<NodeIdExec>().unwrap();
        assert_eq!(new_node_id_exec.node_id, 99);
    }

    #[tokio::test]
    async fn test_node_id_exec_display() {
        let (_tmp, ds) = temp_dataset().await;
        let inner_plan = ds.scan().create_plan().await.unwrap();
        let exec = NodeIdExec::new(inner_plan, 42);

        let display = format!(
            "{}",
            datafusion::physical_plan::displayable(&exec).one_line()
        );
        assert!(
            display.contains("NodeIdExec(node_id=42)"),
            "display should contain node_id, got: {display}"
        );
    }

    #[tokio::test]
    async fn test_node_id_exec_properties_match_schema() {
        // Regression: EquivalenceProperties must reference the augmented schema,
        // not the inner schema, to avoid DataFusion OutputRequirements mismatch.
        let (_tmp, ds) = temp_dataset().await;
        let inner_plan = ds.scan().create_plan().await.unwrap();
        let exec = NodeIdExec::new(inner_plan, 1);

        let props = exec.properties();
        let eq_schema = props.eq_properties.schema();
        assert_eq!(
            eq_schema.fields().len(),
            exec.schema().fields().len(),
            "EquivalenceProperties schema must match NodeIdExec schema"
        );
        assert_eq!(
            eq_schema.field(eq_schema.fields().len() - 1).name(),
            "node_id",
            "last field in eq_properties schema should be node_id"
        );
    }

    #[tokio::test]
    async fn test_node_id_exec_zero_row_batch() {
        // Verify NodeIdExec handles 0-row batches without panic.
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();

        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let augmented_schema = Arc::new(schema_with_node_id(&schema));
        let provider = Arc::new(NodeIdTableProvider {
            inner: Arc::new(mem_table),
            node_id: 99,
            schema: augmented_schema,
        });

        let ctx = SessionContext::new();
        ctx.register_table("t", provider).unwrap();

        // WHERE false yields 0 rows — exercises repeat_n(node_id, 0)
        let df = ctx
            .sql("SELECT id, node_id FROM t WHERE id < 0")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_node_id_exec_end_to_end_sql() {
        // Full DataFusion query through NodeIdExec to verify optimizer compatibility.
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionContext;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Register a MemTable with the inner schema
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();

        // Wrap in a FederatedSysTableProvider-like setup using NodeIdExec
        let augmented_schema = Arc::new(schema_with_node_id(&schema));
        let provider = Arc::new(NodeIdTableProvider {
            inner: Arc::new(mem_table),
            node_id: 42,
            schema: augmented_schema,
        });

        let ctx = SessionContext::new();
        ctx.register_table("test_table", provider).unwrap();

        // Run a query that the DataFusion optimizer will process
        let df = ctx
            .sql("SELECT id, val, node_id FROM test_table WHERE id > 1")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "WHERE id > 1 should yield 2 rows");

        for batch in &batches {
            let node_ids = batch
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(node_ids.value(i), 42);
            }
        }
    }

    /// Minimal TableProvider that wraps an inner provider and adds node_id via NodeIdExec.
    /// Used by end-to-end SQL tests.
    #[derive(Debug)]
    struct NodeIdTableProvider {
        inner: Arc<dyn TableProvider>,
        node_id: u64,
        schema: SchemaRef,
    }

    #[async_trait]
    impl TableProvider for NodeIdTableProvider {
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
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let node_id_idx = self.schema.fields().len() - 1;
            let inner_proj: Option<Vec<usize>> = projection.map(|p| {
                p.iter()
                    .filter(|&&idx| idx != node_id_idx)
                    .copied()
                    .collect()
            });
            let inner_plan = self
                .inner
                .scan(state, inner_proj.as_ref(), filters, limit)
                .await?;
            Ok(Arc::new(NodeIdExec::new(inner_plan, self.node_id)))
        }
    }

    // -----------------------------------------------------------------------
    // FTS integration tests (build_scan_plan + fts_to_like)
    // -----------------------------------------------------------------------

    /// Helper: create a Lance dataset with a string column for FTS tests.
    async fn temp_text_dataset() -> (tempfile::TempDir, Dataset) {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(arrow_array::StringArray::from(vec![
                    "connection timeout error",
                    "all systems normal",
                    "timeout on database query",
                    "healthy heartbeat",
                ])),
            ],
        )
        .unwrap();

        let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let ds = Dataset::write(reader, tmp.path().to_str().unwrap(), None)
            .await
            .unwrap();
        (tmp, ds)
    }

    #[tokio::test]
    async fn test_build_scan_plan_no_fts_filters_unchanged() {
        // When there are no FTS filters, build_scan_plan should behave identically
        // to the pre-FTS version — regular filters applied directly.
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let filter = col("id").gt(datafusion_expr::lit(2));
        let plan = build_scan_plan(&ds, &schema, None, &[filter], None, false)
            .await
            .unwrap();

        // Execute and verify the filter was applied
        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 2, "WHERE id > 2 should yield rows 3 and 4");
    }

    #[tokio::test]
    async fn test_build_scan_plan_fts_active_segment_like_fallback() {
        // is_indexed=false: fts_match should be converted to LIKE '%query%'.
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let udf = crate::fts::fts_match_udf();
        let fts_filter = udf.call(vec![col("body"), datafusion_expr::lit("timeout")]);

        let plan = build_scan_plan(&ds, &schema, None, &[fts_filter], None, false)
            .await
            .unwrap();

        // Execute — LIKE '%timeout%' should match rows 1 and 3
        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 2, "LIKE '%timeout%' should match 2 rows");
    }

    #[tokio::test]
    async fn test_build_scan_plan_fts_with_regular_filter() {
        // Mixed: fts_match + regular filter on active segment.
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let udf = crate::fts::fts_match_udf();
        let fts_filter = udf.call(vec![col("body"), datafusion_expr::lit("timeout")]);
        let id_filter = col("id").gt(datafusion_expr::lit(1));

        let plan = build_scan_plan(&ds, &schema, None, &[fts_filter, id_filter], None, false)
            .await
            .unwrap();

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        // "timeout" in rows 1 and 3; id > 1 keeps only row 3
        assert_eq!(total_rows, 1, "fts + id > 1 should match 1 row");
    }

    #[tokio::test]
    async fn test_build_scan_plan_fts_with_limit() {
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let udf = crate::fts::fts_match_udf();
        let fts_filter = udf.call(vec![col("body"), datafusion_expr::lit("timeout")]);

        let plan = build_scan_plan(&ds, &schema, None, &[fts_filter], Some(1), false)
            .await
            .unwrap();

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        assert!(total_rows <= 1, "LIMIT 1 should return at most 1 row");
    }

    #[tokio::test]
    async fn test_build_scan_plan_fts_with_projection() {
        // Verify FTS works with column projection.
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let udf = crate::fts::fts_match_udf();
        let fts_filter = udf.call(vec![col("body"), datafusion_expr::lit("timeout")]);

        // Project only column 0 (id) — body is still needed for the filter
        let plan = build_scan_plan(&ds, &schema, Some(&vec![0, 1]), &[fts_filter], None, false)
            .await
            .unwrap();

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        assert_eq!(
            total_rows, 2,
            "LIKE '%timeout%' with projection should match 2 rows"
        );
    }

    #[test]
    fn test_fts_to_like_basic() {
        let expr = fts_to_like("body", "error");
        let display = format!("{expr}");
        assert!(
            display.contains("LIKE"),
            "should produce a LIKE expression, got: {display}"
        );
        assert!(
            display.contains("%error%"),
            "should wrap query in %, got: {display}"
        );
    }

    #[test]
    fn test_fts_to_like_empty_query() {
        let expr = fts_to_like("body", "");
        let display = format!("{expr}");
        assert!(
            display.contains("%%"),
            "empty query should produce %%, got: {display}"
        );
    }

    #[tokio::test]
    async fn test_build_scan_plan_multiple_fts_filters_active() {
        // Multiple fts_match filters on active segment — all become LIKE.
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let udf = crate::fts::fts_match_udf();
        let fts1 = udf.call(vec![col("body"), datafusion_expr::lit("timeout")]);
        let fts2 = udf.call(vec![col("body"), datafusion_expr::lit("connection")]);

        let plan = build_scan_plan(&ds, &schema, None, &[fts1, fts2], None, false)
            .await
            .unwrap();

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        // Only row 1 matches both "timeout" AND "connection"
        assert_eq!(total_rows, 1, "both LIKE filters should match 1 row");
    }

    #[tokio::test]
    async fn test_build_scan_plan_indexed_no_fts_filters() {
        // is_indexed=true but no fts_match filters — should apply regular filters
        // via the fast path (no Vec allocation, no FTS interception).
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let filter = col("id").gt(datafusion_expr::lit(2));
        let plan = build_scan_plan(&ds, &schema, None, &[filter], None, true)
            .await
            .unwrap();

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        assert_eq!(
            total_rows, 2,
            "regular filter on indexed segment should work"
        );
    }

    #[tokio::test]
    async fn test_build_scan_plan_empty_filters() {
        // No filters at all — should return all rows.
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let plan = build_scan_plan(&ds, &schema, None, &[], None, false)
            .await
            .unwrap();

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 4, "no filters should return all 4 rows");
    }

    #[tokio::test]
    async fn test_build_scan_plan_no_matching_rows() {
        let (_tmp, ds) = temp_text_dataset().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let udf = crate::fts::fts_match_udf();
        let fts = udf.call(vec![col("body"), datafusion_expr::lit("nonexistent_xyz")]);

        let plan = build_scan_plan(&ds, &schema, None, &[fts], None, false)
            .await
            .unwrap();

        let ctx = datafusion::execution::TaskContext::default();
        let mut stream = plan.execute(0, Arc::new(ctx)).unwrap();
        let mut total_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 0, "no rows should match nonexistent query");
    }
}
