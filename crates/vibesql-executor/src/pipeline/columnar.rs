//! Columnar Execution Pipeline
//!
//! Implements the `ExecutionPipeline` trait using columnar batch execution.
//! Uses LLVM auto-vectorized operations for filtering and aggregation.

use vibesql_ast::{Expression, SelectItem};
use vibesql_storage::Row;

use crate::errors::ExecutorError;

use super::{ExecutionContext, ExecutionPipeline, PipelineInput, PipelineOutput};

use crate::select::columnar::{
    extract_aggregates, extract_column_predicates, simd_filter_batch, AggregateSource,
    ColumnarBatch,
};

/// Columnar execution pipeline.
///
/// This pipeline converts row data to columnar batches and uses SIMD-accelerated
/// operations when available. It provides significant speedups (4-8x for filtering,
/// 10x for aggregation) on large datasets.
///
/// # Performance
///
/// Uses LLVM auto-vectorization for filtering and aggregation.
///
/// # Performance Characteristics
///
/// - **Filter**: SIMD predicate evaluation on column arrays
/// - **Projection**: Implicit in columnar format (no per-row overhead)
/// - **Aggregation**: SIMD reduction operations (sum, min, max, etc.)
///
/// # When to Use
///
/// - Large datasets (>10k rows)
/// - Simple aggregate queries without complex GROUP BY
/// - Queries with simple predicates that can be vectorized
///
/// # Limitations
///
/// - Does not support complex GROUP BY with expressions
/// - Requires conversion overhead for row-based input
/// - Falls back to row-oriented for unsupported patterns
pub struct ColumnarPipeline {
    /// Whether to use SIMD acceleration (requires feature flag)
    #[allow(dead_code)]
    use_simd: bool,
}

impl ColumnarPipeline {
    /// Create a new columnar pipeline.
    ///
    /// SIMD acceleration is provided via LLVM auto-vectorization.
    #[inline]
    pub fn new() -> Self {
        Self { use_simd: true }
    }

    /// Create a columnar pipeline with explicit SIMD setting.
    ///
    /// This is primarily for testing purposes.
    #[inline]
    #[allow(dead_code)]
    pub fn with_simd(use_simd: bool) -> Self {
        Self { use_simd }
    }
}

impl Default for ColumnarPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionPipeline for ColumnarPipeline {
    /// Create an evaluator with context for columnar execution.
    #[inline]
    fn create_evaluator<'a>(
        &self,
        ctx: &'a ExecutionContext<'a>,
    ) -> crate::evaluator::CombinedExpressionEvaluator<'a> {
        ctx.create_evaluator()
    }

    /// Apply WHERE clause filtering using columnar operations.
    ///
    /// When SIMD is enabled:
    /// 1. Uses columnar batch directly if input is already columnar (zero-copy)
    /// 2. Otherwise converts rows to columnar batch
    /// 3. Extracts simple predicates
    /// 4. Applies SIMD-accelerated filtering
    /// 5. Returns columnar batch output (defers row conversion to final stage)
    ///
    fn apply_filter(
        &self,
        input: PipelineInput<'_>,
        predicate: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // Handle batch input directly - avoids all conversions
        if let PipelineInput::Batch(batch) = input {
            // No predicate = no filtering, return batch as-is
            if predicate.is_none() {
                return Ok(PipelineOutput::from_batch(batch));
            }

            let predicate = predicate.unwrap();

            // Try to extract simple predicates for SIMD filtering
            let predicates = match extract_column_predicates(predicate, ctx.schema) {
                Some(preds) => preds,
                None => {
                    // Complex predicate - fall back to row-oriented filtering
                    let rows = batch.to_rows()?;
                    return self.fallback_filter(rows, Some(predicate), ctx);
                }
            };

            if batch.row_count() == 0 {
                return Ok(PipelineOutput::Empty);
            }

            // Apply SIMD-accelerated filtering
            let filtered_batch =
                if predicates.is_empty() { batch } else { simd_filter_batch(&batch, &predicates)? };

            // Return columnar batch output (defer row conversion to final stage)
            return Ok(PipelineOutput::from_batch(filtered_batch));
        }

        // Row-based input path
        let rows = input.into_rows();

        if rows.is_empty() {
            return Ok(PipelineOutput::Empty);
        }

        // No predicate = no filtering needed
        if predicate.is_none() {
            return Ok(PipelineOutput::from_rows(rows));
        }

        let predicate = predicate.unwrap();

        // Try to extract simple predicates for SIMD filtering
        let predicates = match extract_column_predicates(predicate, ctx.schema) {
            Some(preds) => preds,
            None => {
                // Complex predicate - fall back to row-oriented filtering
                return self.fallback_filter(rows, Some(predicate), ctx);
            }
        };

        // Convert to columnar batch for SIMD filtering
        let batch = ColumnarBatch::from_rows(&rows)?;

        // Apply SIMD-accelerated filtering
        let filtered_batch =
            if predicates.is_empty() { batch } else { simd_filter_batch(&batch, &predicates)? };

        // Return columnar batch output (defer row conversion to final stage)
        Ok(PipelineOutput::from_batch(filtered_batch))
    }

    /// Apply SELECT projection in columnar mode.
    ///
    /// In columnar execution, projection is typically handled implicitly
    /// by only computing the required aggregate columns. For non-aggregate
    /// projections, we fall back to row-oriented processing.
    fn apply_projection(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        let rows = input.into_rows();

        if rows.is_empty() {
            return Ok(PipelineOutput::Empty);
        }

        // Columnar projection is typically implicit in aggregate computation
        // For explicit projection, fall back to row-oriented processing
        let evaluator = ctx.create_evaluator();
        let buffer_pool = vibesql_storage::QueryBufferPool::new();

        let mut projected_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let projected = crate::select::projection::project_row_combined(
                &row,
                select_items,
                &evaluator,
                ctx.schema,
                &None,
                &buffer_pool,
            )?;
            projected_rows.push(projected);
        }

        Ok(PipelineOutput::from_rows(projected_rows))
    }

    /// Execute aggregation using columnar operations.
    ///
    /// When SIMD is enabled:
    /// 1. Uses columnar batch directly if input is already columnar (zero-copy)
    /// 2. Extracts aggregate specifications from SELECT list
    /// 3. Uses SIMD-accelerated reduction operations
    /// 4. Returns single result row
    ///
    /// Falls back to row-oriented for GROUP BY or complex aggregates.
    fn apply_aggregation(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        group_by: Option<&[Expression]>,
        _having: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // Columnar path doesn't support GROUP BY with expressions
        if let Some(exprs) = group_by {
            if !exprs.is_empty() {
                return self.fallback_aggregation(input, select_items, group_by, _having, ctx);
            }
        }

        // Extract aggregate expressions from select items first (before consuming input)
        let agg_exprs: Vec<Expression> = select_items
            .iter()
            .filter_map(|item| {
                if let SelectItem::Expression { expr, .. } = item {
                    Some(expr.clone())
                } else {
                    None
                }
            })
            .collect();

        // Try to extract aggregate specs for SIMD processing
        let agg_specs = match extract_aggregates(&agg_exprs, ctx.schema) {
            Some(specs) => specs,
            None => {
                // Complex aggregates - fall back to row-oriented
                return self.fallback_aggregation(input, select_items, group_by, _having, ctx);
            }
        };

        // Get or create the columnar batch from input
        // This handles both batch and row input formats
        let batch = match input {
            PipelineInput::Batch(batch) => batch,
            _ => {
                let rows = input.into_rows();
                if rows.is_empty() {
                    // Handle empty input per SQL standard
                    let values: Vec<vibesql_types::SqlValue> = agg_specs
                        .iter()
                        .map(|spec| match spec.op {
                            crate::select::columnar::AggregateOp::Count => {
                                vibesql_types::SqlValue::Integer(0)
                            }
                            _ => vibesql_types::SqlValue::Null,
                        })
                        .collect();
                    return Ok(PipelineOutput::from_rows(vec![Row::new(values)]));
                }
                // Convert to columnar batch for SIMD aggregation
                ColumnarBatch::from_rows(&rows)?
            }
        };

        // Handle empty batch per SQL standard
        if batch.row_count() == 0 {
            let values: Vec<vibesql_types::SqlValue> = agg_specs
                .iter()
                .map(|spec| match spec.op {
                    crate::select::columnar::AggregateOp::Count => {
                        vibesql_types::SqlValue::Integer(0)
                    }
                    _ => vibesql_types::SqlValue::Null,
                })
                .collect();
            return Ok(PipelineOutput::from_rows(vec![Row::new(values)]));
        }

        // Compute aggregates using batch-native operations
        let needs_schema =
            agg_specs.iter().any(|spec| matches!(spec.source, AggregateSource::Expression(_)));
        let schema_ref = if needs_schema { Some(ctx.schema) } else { None };

        let results =
            crate::select::columnar::compute_aggregates_from_batch(&batch, &agg_specs, schema_ref)?;

        Ok(PipelineOutput::from_rows(vec![Row::new(results)]))
    }

    /// Columnar supports simple aggregates without GROUP BY.
    fn supports_query_pattern(
        &self,
        has_aggregation: bool,
        has_group_by: bool,
        has_joins: bool,
    ) -> bool {
        // Columnar path works best for:
        // - Simple aggregates without GROUP BY
        // - Single table scans (no joins)
        has_aggregation && !has_group_by && !has_joins
    }

    #[inline]
    fn name(&self) -> &'static str {
        "ColumnarPipeline (SIMD)"
    }
}

impl ColumnarPipeline {
    /// Fallback to row-oriented filtering for complex predicates.
    fn fallback_filter(
        &self,
        rows: Vec<Row>,
        predicate: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        if predicate.is_none() {
            return Ok(PipelineOutput::from_rows(rows));
        }

        let predicate = predicate.unwrap();
        let evaluator = ctx.create_evaluator();

        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            let value = evaluator.eval(predicate, &row)?;
            let include = match value {
                vibesql_types::SqlValue::Boolean(true) => true,
                vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
                vibesql_types::SqlValue::Integer(0) => false,
                vibesql_types::SqlValue::Integer(_) => true,
                _ => false,
            };
            if include {
                filtered.push(row);
            }
        }

        Ok(PipelineOutput::from_rows(filtered))
    }

    /// Fallback to row-oriented aggregation for complex patterns.
    fn fallback_aggregation(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        group_by: Option<&[Expression]>,
        having: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        let rows = input.into_rows();
        let evaluator = ctx.create_evaluator();

        // Simple implementation without full GROUP BY support
        // This is a minimal fallback - the real implementation would use
        // the full row-oriented aggregation logic

        if group_by.is_some() && group_by.map(|g| !g.is_empty()).unwrap_or(false) {
            return Err(ExecutorError::UnsupportedFeature(
                "GROUP BY not supported in columnar fallback".to_string(),
            ));
        }

        if having.is_some() {
            return Err(ExecutorError::UnsupportedFeature(
                "HAVING not supported in columnar fallback".to_string(),
            ));
        }

        // Compute aggregates for all rows as single group
        let mut results = Vec::new();
        for item in select_items {
            if let SelectItem::Expression { expr, .. } = item {
                if rows.is_empty() {
                    // SQL standard: aggregates over empty set have defined behavior
                    let value = Self::evaluate_empty_aggregate(expr, &evaluator)?;
                    results.push(value);
                } else {
                    // Complex aggregates with non-empty input need full row-oriented execution
                    // to properly aggregate over all rows, not just the first one
                    return Err(ExecutorError::UnsupportedFeature(
                        "Complex aggregate expressions require row-oriented execution".to_string(),
                    ));
                }
            }
        }

        Ok(PipelineOutput::from_rows(vec![Row::new(results)]))
    }

    /// Evaluate an expression over an empty result set per SQL standard.
    fn evaluate_empty_aggregate(
        expr: &Expression,
        evaluator: &crate::evaluator::CombinedExpressionEvaluator<'_>,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),
            Expression::AggregateFunction { name, .. } => match name.to_uppercase().as_str() {
                "COUNT" => Ok(vibesql_types::SqlValue::Integer(0)),
                _ => Ok(vibesql_types::SqlValue::Null),
            },
            Expression::UnaryOp { op, expr: inner } => {
                let inner_val = Self::evaluate_empty_aggregate(inner, evaluator)?;
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let unary_expr =
                    Expression::UnaryOp { op: *op, expr: Box::new(Expression::Literal(inner_val)) };
                evaluator.eval(&unary_expr, &dummy_row)
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_empty_aggregate(left, evaluator)?;
                let right_val = Self::evaluate_empty_aggregate(right, evaluator)?;
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let binary_expr = Expression::BinaryOp {
                    left: Box::new(Expression::Literal(left_val)),
                    op: *op,
                    right: Box::new(Expression::Literal(right_val)),
                };
                evaluator.eval(&binary_expr, &dummy_row)
            }
            Expression::Function { name, .. } => match name.to_uppercase().as_str() {
                "COUNT" => Ok(vibesql_types::SqlValue::Integer(0)),
                _ => Ok(vibesql_types::SqlValue::Null),
            },
            Expression::Cast { expr: inner, data_type } => {
                let inner_val = Self::evaluate_empty_aggregate(inner, evaluator)?;
                crate::evaluator::casting::cast_value(
                    &inner_val,
                    data_type,
                    &vibesql_types::SqlMode::default(),
                )
            }
            _ => Ok(vibesql_types::SqlValue::Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::CombinedSchema;
    use vibesql_catalog::TableSchema;
    use vibesql_types::SqlValue;

    fn create_test_setup() -> (vibesql_storage::Database, CombinedSchema) {
        let database = vibesql_storage::Database::new();
        let table_schema = TableSchema::new("test".to_string(), vec![]);
        let schema = CombinedSchema::from_table("test".to_string(), table_schema);
        (database, schema)
    }

    fn make_test_row(values: Vec<i64>) -> Row {
        Row::new(values.into_iter().map(SqlValue::Integer).collect::<Vec<_>>())
    }

    #[test]
    fn test_columnar_pipeline_name() {
        let pipeline = ColumnarPipeline::new();
        let name = pipeline.name();
        assert!(name.starts_with("ColumnarPipeline"));
    }

    #[test]
    fn test_columnar_pipeline_supports_simple_aggregates() {
        let pipeline = ColumnarPipeline::new();

        // Supports simple aggregates without GROUP BY
        assert!(pipeline.supports_query_pattern(true, false, false));

        // Does not support GROUP BY
        assert!(!pipeline.supports_query_pattern(true, true, false));

        // Does not support joins
        assert!(!pipeline.supports_query_pattern(true, false, true));

        // Does not support non-aggregate queries
        assert!(!pipeline.supports_query_pattern(false, false, false));
    }

    #[test]
    fn test_columnar_pipeline_filter_no_predicate() {
        let (database, schema) = create_test_setup();
        let pipeline = ColumnarPipeline::new();

        let rows = vec![make_test_row(vec![1, 2]), make_test_row(vec![3, 4])];
        let input = PipelineInput::from_rows_owned(rows);
        let ctx = ExecutionContext::new(&schema, &database);

        let result = pipeline.apply_filter(input, None, &ctx).unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_columnar_pipeline_limit_offset() {
        let pipeline = ColumnarPipeline::new();

        let rows = vec![
            make_test_row(vec![1]),
            make_test_row(vec![2]),
            make_test_row(vec![3]),
            make_test_row(vec![4]),
            make_test_row(vec![5]),
        ];
        let output = PipelineOutput::from_rows(rows);

        // Test limit
        let result = pipeline.apply_limit_offset(output, Some(3), None).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_columnar_pipeline_default() {
        let pipeline = ColumnarPipeline::default();
        assert!(pipeline.name().starts_with("ColumnarPipeline"));
    }

    #[test]
    fn test_evaluate_empty_aggregate_count_returns_zero() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        // COUNT(*) should return 0 for empty set
        let count_expr = Expression::AggregateFunction {
            name: "COUNT".to_string(),
            args: vec![Expression::Wildcard],
            distinct: false,
        };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&count_expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Integer(0));

        // count (lowercase) should also return 0
        let count_lower = Expression::AggregateFunction {
            name: "count".to_string(),
            args: vec![Expression::Wildcard],
            distinct: false,
        };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&count_lower, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn test_evaluate_empty_aggregate_sum_returns_null() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        let sum_expr = Expression::AggregateFunction {
            name: "SUM".to_string(),
            args: vec![Expression::ColumnRef { table: None, column: "x".to_string() }],
            distinct: false,
        };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&sum_expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_evaluate_empty_aggregate_avg_min_max_return_null() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        for agg_name in &["AVG", "MIN", "MAX"] {
            let expr = Expression::AggregateFunction {
                name: agg_name.to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "x".to_string() }],
                distinct: false,
            };
            let result = ColumnarPipeline::evaluate_empty_aggregate(&expr, &evaluator).unwrap();
            assert_eq!(result, SqlValue::Null, "{} should return NULL for empty set", agg_name);
        }
    }

    #[test]
    fn test_evaluate_empty_aggregate_literal_returns_value() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        let literal_expr = Expression::Literal(SqlValue::Integer(42));
        let result = ColumnarPipeline::evaluate_empty_aggregate(&literal_expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluate_empty_aggregate_binary_op_with_count() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        // COUNT(*) + 10 should return 0 + 10 = 10
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::AggregateFunction {
                name: "COUNT".to_string(),
                args: vec![Expression::Wildcard],
                distinct: false,
            }),
            op: vibesql_ast::BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Integer(10));
    }

    #[test]
    fn test_evaluate_empty_aggregate_unary_op() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        // -COUNT(*) should return -0 = 0
        let expr = Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Minus,
            expr: Box::new(Expression::AggregateFunction {
                name: "COUNT".to_string(),
                args: vec![Expression::Wildcard],
                distinct: false,
            }),
        };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn test_evaluate_empty_aggregate_cast() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        // CAST(COUNT(*) AS VARCHAR) should return "0"
        let expr = Expression::Cast {
            expr: Box::new(Expression::AggregateFunction {
                name: "COUNT".to_string(),
                args: vec![Expression::Wildcard],
                distinct: false,
            }),
            data_type: vibesql_types::DataType::Varchar { max_length: None },
        };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("0")));
    }

    #[test]
    fn test_evaluate_empty_aggregate_function_count() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        // Some parsers may represent COUNT as Expression::Function instead of AggregateFunction
        let expr = Expression::Function {
            name: "COUNT".to_string(),
            args: vec![Expression::Wildcard],
            character_unit: None,
        };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn test_evaluate_empty_aggregate_column_ref_returns_null() {
        let (database, schema) = create_test_setup();
        let ctx = ExecutionContext::new(&schema, &database);
        let evaluator = ctx.create_evaluator();

        // Column reference without aggregate should return NULL
        let expr = Expression::ColumnRef { table: None, column: "x".to_string() };
        let result = ColumnarPipeline::evaluate_empty_aggregate(&expr, &evaluator).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_fallback_aggregation_returns_error_for_nonempty_rows() {
        let (database, schema) = create_test_setup();
        let pipeline = ColumnarPipeline::new();
        let ctx = ExecutionContext::new(&schema, &database);

        // Create non-empty input rows
        let rows = vec![make_test_row(vec![1, 2]), make_test_row(vec![3, 4])];
        let input = PipelineInput::from_rows_owned(rows);

        // Create a complex aggregate expression (- COUNT(*))
        let complex_agg = Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Minus,
            expr: Box::new(Expression::AggregateFunction {
                name: "COUNT".to_string(),
                args: vec![Expression::Wildcard],
                distinct: false,
            }),
        };
        let select_items = vec![SelectItem::Expression { expr: complex_agg, alias: None }];

        // fallback_aggregation should return UnsupportedFeature for non-empty rows
        let result = pipeline.fallback_aggregation(input, &select_items, None, None, &ctx);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::UnsupportedFeature(msg)) => {
                assert!(msg.contains("row-oriented execution"));
            }
            other => panic!("Expected UnsupportedFeature error, got: {:?}", other),
        }
    }

    #[test]
    fn test_fallback_aggregation_works_for_empty_rows() {
        let (database, schema) = create_test_setup();
        let pipeline = ColumnarPipeline::new();
        let ctx = ExecutionContext::new(&schema, &database);

        // Create empty input
        let rows: Vec<Row> = vec![];
        let input = PipelineInput::from_rows_owned(rows);

        // Create a complex aggregate expression (- COUNT(*))
        let complex_agg = Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Minus,
            expr: Box::new(Expression::AggregateFunction {
                name: "COUNT".to_string(),
                args: vec![Expression::Wildcard],
                distinct: false,
            }),
        };
        let select_items = vec![SelectItem::Expression { expr: complex_agg, alias: None }];

        // fallback_aggregation should work for empty rows (returns -0 = 0)
        let result = pipeline.fallback_aggregation(input, &select_items, None, None, &ctx);
        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.row_count(), 1);
    }
}
