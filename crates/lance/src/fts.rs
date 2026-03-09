//! Full-text search UDF for DataFusion.
//!
//! Provides `fts_match(column, query)` — a **marker UDF** that is intercepted
//! during scan planning in [`crate::query::build_scan_plan`].  For sealed/S3
//! segments with an inverted index it becomes a Lance `full_text_search` call;
//! for active segments (no index) it falls back to SQL `LIKE '%query%'`.

use std::any::Any;

use arrow_schema::DataType;
use datafusion::logical_expr::Expr;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Create the `fts_match` scalar UDF.
pub fn fts_match_udf() -> ScalarUDF {
    ScalarUDF::from(FtsMatchUdf::new())
}

/// Marker UDF: `fts_match(column, query) → Boolean`.
///
/// Never evaluated at runtime — intercepted during scan planning.
/// If it *does* reach evaluation (e.g. DataFusion runs it on an
/// intermediate batch), it returns `true` for every row so the
/// scan result is not incorrectly filtered.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FtsMatchUdf {
    signature: Signature,
}

impl FtsMatchUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FtsMatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "fts_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        // Fallback: return true for all rows so results are not dropped.
        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Boolean(Some(true)),
        ))
    }
}

/// Extract `(column_name, query_string)` from an `fts_match(col, 'query')` expression.
///
/// Returns borrowed references to avoid allocation on the hot planning path.
/// Returns `None` if the expression is not an `fts_match` call or if the arguments
/// are not in the expected form `(Column, Literal<Utf8>)`.
pub fn extract_fts_match(expr: &Expr) -> Option<(&str, &str)> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return None;
    };
    if func.name() != "fts_match" || args.len() != 2 {
        return None;
    }

    // First arg: column reference
    let col_name = match &args[0] {
        Expr::Column(c) => c.name(),
        _ => return None,
    };

    // Second arg: literal string
    let query = match &args[1] {
        Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(s)), _) => s.as_str(),
        _ => return None,
    };

    Some((col_name, query))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::col;
    use datafusion_expr::lit;

    #[test]
    fn test_udf_name_and_signature() {
        let udf = fts_match_udf();
        assert_eq!(udf.name(), "fts_match");
        let sig = udf.signature();
        assert_eq!(sig.volatility, Volatility::Immutable);
    }

    #[test]
    fn test_extract_fts_match_valid() {
        let udf = fts_match_udf();
        let expr = udf.call(vec![col("body"), lit("connection timeout")]);
        let result = extract_fts_match(&expr);
        assert_eq!(result, Some(("body", "connection timeout")));
    }

    #[test]
    fn test_extract_fts_match_column_expr() {
        let udf = fts_match_udf();
        let expr = udf.call(vec![col("body"), lit("error")]);
        let (col_name, query) = extract_fts_match(&expr).unwrap();
        assert_eq!(col_name, "body");
        assert_eq!(query, "error");
    }

    #[test]
    fn test_extract_fts_match_returns_none_for_non_fts() {
        // Column expression
        assert_eq!(extract_fts_match(&col("body")), None);

        // Binary expression
        let binary = col("x").gt(lit(1));
        assert_eq!(extract_fts_match(&binary), None);
    }

    #[test]
    fn test_extract_fts_match_returns_none_for_wrong_arg_types() {
        let udf = fts_match_udf();
        // Two columns instead of column + literal
        let expr = udf.call(vec![col("a"), col("b")]);
        assert_eq!(extract_fts_match(&expr), None);
    }

    #[test]
    fn test_return_type_is_boolean() {
        let udf = FtsMatchUdf::new();
        let rt = udf.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap();
        assert_eq!(rt, DataType::Boolean);
    }

    #[test]
    fn test_extract_fts_match_empty_query() {
        let udf = fts_match_udf();
        let expr = udf.call(vec![col("body"), lit("")]);
        let result = extract_fts_match(&expr);
        assert_eq!(result, Some(("body", "")));
    }

    #[test]
    fn test_extract_fts_match_different_udf_name() {
        // A ScalarFunction with a different name should not match.
        use datafusion_expr::{ScalarUDF, ScalarUDFImpl, Signature, Volatility};

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct OtherUdf {
            sig: Signature,
        }
        impl ScalarUDFImpl for OtherUdf {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "not_fts"
            }
            fn signature(&self) -> &Signature {
                &self.sig
            }
            fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
                Ok(DataType::Boolean)
            }
            fn invoke_with_args(
                &self,
                _: ScalarFunctionArgs,
            ) -> datafusion::common::Result<ColumnarValue> {
                unimplemented!()
            }
        }

        let other = ScalarUDF::from(OtherUdf {
            sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        });
        let expr = other.call(vec![col("body"), lit("test")]);
        assert_eq!(extract_fts_match(&expr), None);
    }

    #[test]
    fn test_extract_fts_match_literal_first_arg() {
        // fts_match('literal', 'query') — first arg is not a column
        let udf = fts_match_udf();
        let expr = udf.call(vec![lit("not_a_column"), lit("query")]);
        assert_eq!(extract_fts_match(&expr), None);
    }

    #[test]
    fn test_extract_fts_match_non_utf8_literal() {
        // fts_match(col, 42) — second arg is not a string
        let udf = fts_match_udf();
        let expr = udf.call(vec![col("body"), lit(42i32)]);
        assert_eq!(extract_fts_match(&expr), None);
    }

    #[test]
    fn test_extract_fts_match_special_chars_in_query() {
        let udf = fts_match_udf();
        let expr = udf.call(vec![col("body"), lit("error: %special% 'chars' \"here\"")]);
        let (col_name, query) = extract_fts_match(&expr).unwrap();
        assert_eq!(col_name, "body");
        assert_eq!(query, "error: %special% 'chars' \"here\"");
    }

    #[test]
    fn test_invoke_fallback_returns_true() {
        let udf = FtsMatchUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 10,
            return_field: std::sync::Arc::new(arrow_schema::Field::new(
                "out",
                DataType::Boolean,
                false,
            )),
            config_options: std::sync::Arc::new(Default::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Boolean(Some(v))) => {
                assert!(v, "fallback should return true");
            }
            other => panic!("expected Boolean(Some(true)), got: {:?}", other),
        }
    }

    #[test]
    fn test_extract_fts_match_qualified_column() {
        // fts_match(table.body, 'query') — qualified column name
        let udf = fts_match_udf();
        let qualified_col = Expr::Column(datafusion::common::Column::new(
            Some("sys".to_string()),
            "body",
        ));
        let expr = udf.call(vec![qualified_col, lit("test")]);
        let (col_name, query) = extract_fts_match(&expr).unwrap();
        assert_eq!(col_name, "body");
        assert_eq!(query, "test");
    }

    #[tokio::test]
    async fn test_fts_match_udf_end_to_end_sql() {
        // Register fts_match UDF and run a SQL query through DataFusion.
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionContext;

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                std::sync::Arc::new(StringArray::from(vec![
                    "connection timeout error",
                    "all good",
                    "timeout on database",
                    "healthy",
                ])),
            ],
        )
        .unwrap();

        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("logs", std::sync::Arc::new(mem_table))
            .unwrap();
        ctx.register_udf(fts_match_udf());

        // fts_match is a marker UDF that always returns true at runtime
        // (it's meant to be intercepted at scan planning, not in MemTable).
        // So this returns ALL rows — verifying the UDF registers and executes
        // without error in a real DataFusion context.
        let df = ctx
            .sql("SELECT id, body FROM logs WHERE fts_match(body, 'timeout')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // All 4 rows returned because the marker UDF returns true
        assert_eq!(total, 4);
    }
}
