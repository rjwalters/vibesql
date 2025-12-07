//! Native Columnar Execution Pipeline
//!
//! Implements the `ExecutionPipeline` trait using zero-copy columnar execution
//! directly from storage. This pipeline keeps data in columnar format throughout
//! the execution to minimize materialization overhead.

use vibesql_ast::{Expression, SelectItem};
use vibesql_storage::Row;

use crate::errors::ExecutorError;

use super::{ExecutionContext, ExecutionPipeline, PipelineInput, PipelineOutput};

use crate::select::columnar::{
    extract_aggregates, extract_column_predicates, simd_filter_batch, AggregateOp, AggregateSource,
    AggregateSpec, ColumnarBatch,
};

/// Native columnar execution pipeline.
///
/// This pipeline operates on data in columnar format throughout the execution,
/// only converting to rows at the final output stage. This provides maximum
/// performance for large-scale analytical queries.
///
/// # Performance
///
/// Uses LLVM auto-vectorization for SIMD-accelerated operations.
///
/// # Performance Characteristics
///
/// - **Filter**: SIMD-accelerated predicate evaluation (4-8x speedup)
/// - **Projection**: Zero-copy column selection
/// - **Aggregation**: SIMD reduction operations (10x speedup)
/// - **GROUP BY**: Hash-based grouping with columnar aggregation
///
/// # When to Use
///
/// - Large datasets (>100k rows)
/// - TPC-H style analytical queries
/// - Simple GROUP BY with column references
/// - Single table scans without complex JOINs
///
/// # Limitations
///
/// - No JOIN support (single table only)
/// - GROUP BY limited to simple column references
/// - No ROLLUP/CUBE/GROUPING SETS support
/// - Requires columnar storage format
pub struct NativeColumnarPipeline {
    /// Whether the pipeline has access to columnar storage
    #[allow(dead_code)]
    has_columnar_storage: bool,
}

impl NativeColumnarPipeline {
    /// Create a new native columnar pipeline.
    #[inline]
    pub fn new() -> Self {
        Self { has_columnar_storage: true }
    }

    /// Create a native columnar pipeline with explicit columnar storage availability.
    #[inline]
    #[allow(dead_code)]
    pub fn with_storage(has_columnar_storage: bool) -> Self {
        Self { has_columnar_storage }
    }
}

impl Default for NativeColumnarPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionPipeline for NativeColumnarPipeline {
    /// Create an evaluator with context for native columnar execution.
    #[inline]
    fn create_evaluator<'a>(
        &self,
        ctx: &'a ExecutionContext<'a>,
    ) -> crate::evaluator::CombinedExpressionEvaluator<'a> {
        ctx.create_evaluator()
    }

    /// Apply WHERE clause filtering using SIMD-accelerated columnar operations.
    ///
    /// This is the most optimized path:
    /// 1. Extracts simple predicates from WHERE clause
    /// 2. Applies SIMD filtering directly on column arrays
    /// 3. Returns filtered columnar batch (converted to rows for output)
    fn apply_filter(
        &self,
        input: PipelineInput<'_>,
        predicate: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // Handle native columnar input specially
        if let PipelineInput::NativeColumnar { table_name, column_indices: _ } = &input {
            // Get columnar data directly from storage
            let columnar_table = match ctx.database.get_columnar(table_name) {
                Ok(Some(ct)) => ct,
                Ok(None) | Err(_) => {
                    // Fall back to row-based if columnar not available
                    return self.fallback_filter(input.into_rows(), predicate, ctx);
                }
            };

            // Skip empty tables
            if columnar_table.row_count() == 0 {
                return Ok(PipelineOutput::Empty);
            }

            // Convert to batch
            let batch = ColumnarBatch::from_storage_columnar(&columnar_table)?;

            // No predicate = return all rows
            if predicate.is_none() {
                let rows = batch.to_rows()?;
                return Ok(PipelineOutput::from_rows(rows));
            }

            let predicate = predicate.unwrap();

            // Extract simple predicates for SIMD filtering
            let predicates = match extract_column_predicates(predicate, ctx.schema) {
                Some(preds) => preds,
                None => {
                    // Complex predicate - fall back to row filtering
                    let rows = batch.to_rows()?;
                    return self.fallback_filter(rows, Some(predicate), ctx);
                }
            };

            // Apply SIMD-accelerated filtering
            let filtered_batch =
                if predicates.is_empty() { batch } else { simd_filter_batch(&batch, &predicates)? };

            // Return batch directly - avoid row conversion overhead
            // The batch will stay in columnar format through the pipeline
            return Ok(PipelineOutput::from_batch(filtered_batch));
        }

        // For row-based input, convert to columnar and filter
        let rows = input.into_rows();

        if predicate.is_none() {
            return Ok(PipelineOutput::from_rows(rows));
        }

        let predicate = predicate.unwrap();

        // Try to extract simple predicates
        let predicates = match extract_column_predicates(predicate, ctx.schema) {
            Some(preds) => preds,
            None => {
                return self.fallback_filter(rows, Some(predicate), ctx);
            }
        };

        if rows.is_empty() {
            return Ok(PipelineOutput::Empty);
        }

        // Convert to columnar batch
        let batch = ColumnarBatch::from_rows(&rows)?;

        // Apply SIMD filtering
        let filtered_batch =
            if predicates.is_empty() { batch } else { simd_filter_batch(&batch, &predicates)? };

        // Return batch directly - keep data in columnar format
        Ok(PipelineOutput::from_batch(filtered_batch))
    }

    /// Apply SELECT projection using columnar operations.
    ///
    /// In native columnar execution, projection is typically implicit -
    /// we only compute the required columns. For explicit projection,
    /// we fall back to row-oriented processing only when necessary.
    fn apply_projection(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // For batch input, convert to rows for projection then convert back
        // This is less efficient but ensures correctness for complex projections
        // TODO: Implement native columnar projection for simple column selections
        let rows = match input {
            PipelineInput::Batch(batch) => {
                if batch.row_count() == 0 {
                    return Ok(PipelineOutput::Empty);
                }
                batch.to_rows()?
            }
            PipelineInput::Rows(rows) => {
                if rows.is_empty() {
                    return Ok(PipelineOutput::Empty);
                }
                rows.to_vec()
            }
            PipelineInput::RowsOwned(rows) => {
                if rows.is_empty() {
                    return Ok(PipelineOutput::Empty);
                }
                rows
            }
            PipelineInput::Empty => return Ok(PipelineOutput::Empty),
            PipelineInput::NativeColumnar { .. } => {
                // Should not reach here - native columnar goes through filter first
                return Ok(PipelineOutput::Empty);
            }
        };

        // Use row-oriented projection for now
        // Native columnar projection would require computing column indices
        // and extracting only those columns from the batch
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

    /// Execute aggregation using SIMD-accelerated columnar operations.
    ///
    /// This is the most optimized aggregation path:
    /// 1. Extracts aggregate specifications from SELECT list
    /// 2. For simple aggregates without GROUP BY: Uses SIMD reductions
    /// 3. For GROUP BY: Uses hash-based grouping with columnar aggregation
    fn apply_aggregation(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        group_by: Option<&[Expression]>,
        _having: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // Extract aggregate expressions from select items
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

        // Try to extract aggregate specs
        let agg_specs = match extract_aggregates(&agg_exprs, ctx.schema) {
            Some(specs) => specs,
            None => {
                return Err(ExecutorError::UnsupportedFeature(
                    "Complex aggregates not supported in native columnar pipeline".to_string(),
                ));
            }
        };

        // Get batch directly if input is already columnar, otherwise convert
        // This is the key optimization: avoid row conversion if data is already in batch format
        let batch = match input {
            PipelineInput::Batch(batch) => batch,
            PipelineInput::Rows(rows) => {
                if rows.is_empty() {
                    // Handle empty input per SQL standard
                    let values: Vec<vibesql_types::SqlValue> = agg_specs
                        .iter()
                        .map(|spec| match spec.op {
                            AggregateOp::Count => vibesql_types::SqlValue::Integer(0),
                            _ => vibesql_types::SqlValue::Null,
                        })
                        .collect();
                    return Ok(PipelineOutput::from_rows(vec![Row::new(values)]));
                }
                ColumnarBatch::from_rows(rows)?
            }
            PipelineInput::RowsOwned(rows) => {
                if rows.is_empty() {
                    // Handle empty input per SQL standard
                    let values: Vec<vibesql_types::SqlValue> = agg_specs
                        .iter()
                        .map(|spec| match spec.op {
                            AggregateOp::Count => vibesql_types::SqlValue::Integer(0),
                            _ => vibesql_types::SqlValue::Null,
                        })
                        .collect();
                    return Ok(PipelineOutput::from_rows(vec![Row::new(values)]));
                }
                ColumnarBatch::from_rows(&rows)?
            }
            PipelineInput::Empty => {
                // Handle empty input per SQL standard
                let values: Vec<vibesql_types::SqlValue> = agg_specs
                    .iter()
                    .map(|spec| match spec.op {
                        AggregateOp::Count => vibesql_types::SqlValue::Integer(0),
                        _ => vibesql_types::SqlValue::Null,
                    })
                    .collect();
                return Ok(PipelineOutput::from_rows(vec![Row::new(values)]));
            }
            PipelineInput::NativeColumnar { table_name, .. } => {
                // Get columnar data directly from storage
                let columnar_table = match ctx.database.get_columnar(&table_name) {
                    Ok(Some(ct)) => ct,
                    Ok(None) | Err(_) => {
                        return Err(ExecutorError::Other(format!(
                            "Table '{}' not found for columnar aggregation",
                            table_name
                        )));
                    }
                };
                ColumnarBatch::from_storage_columnar(&columnar_table)?
            }
        };

        // Handle empty batch
        if batch.row_count() == 0 {
            let values: Vec<vibesql_types::SqlValue> = agg_specs
                .iter()
                .map(|spec| match spec.op {
                    AggregateOp::Count => vibesql_types::SqlValue::Integer(0),
                    _ => vibesql_types::SqlValue::Null,
                })
                .collect();
            return Ok(PipelineOutput::from_rows(vec![Row::new(values)]));
        }

        // Check if we have GROUP BY
        if let Some(group_exprs) = group_by {
            if !group_exprs.is_empty() {
                return self.execute_group_by(&batch, group_exprs, &agg_specs, ctx);
            }
        }

        // Simple aggregation without GROUP BY
        let needs_schema =
            agg_specs.iter().any(|spec| matches!(spec.source, AggregateSource::Expression(_)));
        let schema_ref = if needs_schema { Some(ctx.schema) } else { None };

        let results =
            crate::select::columnar::compute_aggregates_from_batch(&batch, &agg_specs, schema_ref)?;

        Ok(PipelineOutput::from_rows(vec![Row::new(results)]))
    }

    /// Native columnar supports simple aggregates with optional GROUP BY.
    fn supports_query_pattern(
        &self,
        has_aggregation: bool,
        _has_group_by: bool,
        has_joins: bool,
    ) -> bool {
        // Native columnar supports:
        // - Aggregates with or without GROUP BY (single table only)
        // - No JOINs (single table requirement)
        // GROUP BY is supported for simple column references
        has_aggregation && !has_joins
    }

    #[inline]
    fn name(&self) -> &'static str {
        "NativeColumnarPipeline (SIMD)"
    }
}

impl NativeColumnarPipeline {
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

    /// Execute GROUP BY using hash-based columnar aggregation.
    fn execute_group_by(
        &self,
        batch: &ColumnarBatch,
        group_exprs: &[Expression],
        agg_specs: &[AggregateSpec],
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // Extract group column indices (only simple column refs supported)
        let group_cols: Vec<usize> = group_exprs
            .iter()
            .filter_map(|expr| match expr {
                Expression::ColumnRef { table, column } => {
                    ctx.schema.get_column_index(table.as_deref(), column.as_str())
                }
                _ => None,
            })
            .collect();

        if group_cols.len() != group_exprs.len() {
            return Err(ExecutorError::UnsupportedFeature(
                "GROUP BY with non-column expressions not supported in native columnar".to_string(),
            ));
        }

        // Convert aggregates to (column_idx, op) format
        let agg_cols: Vec<(usize, AggregateOp)> = agg_specs
            .iter()
            .filter_map(|spec| match &spec.source {
                AggregateSource::Column(idx) => Some((*idx, spec.op)),
                AggregateSource::CountStar => Some((0, AggregateOp::Count)),
                AggregateSource::Expression(_) => None,
            })
            .collect();

        if agg_cols.len() != agg_specs.len() {
            return Err(ExecutorError::UnsupportedFeature(
                "Expression aggregates not supported in native columnar GROUP BY".to_string(),
            ));
        }

        // Execute SIMD-accelerated hash-based GROUP BY directly on batch
        let result =
            crate::select::columnar::columnar_group_by_batch(batch, &group_cols, &agg_cols)?;

        Ok(PipelineOutput::from_rows(result))
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
    fn test_native_columnar_pipeline_name() {
        let pipeline = NativeColumnarPipeline::new();
        let name = pipeline.name();
        assert!(name.starts_with("NativeColumnarPipeline"));
    }

    #[test]
    fn test_native_columnar_pipeline_supports_aggregates() {
        let pipeline = NativeColumnarPipeline::new();

        // Supports aggregates without joins (SIMD always enabled via auto-vectorization)
        let supports_simple = pipeline.supports_query_pattern(true, false, false);
        assert!(supports_simple);

        // Does not support joins
        assert!(!pipeline.supports_query_pattern(true, false, true));
    }

    #[test]
    fn test_native_columnar_pipeline_filter_no_predicate() {
        let (database, schema) = create_test_setup();
        let pipeline = NativeColumnarPipeline::new();

        let rows = vec![make_test_row(vec![1, 2]), make_test_row(vec![3, 4])];
        let input = PipelineInput::from_rows_owned(rows);
        let ctx = ExecutionContext::new(&schema, &database);

        let result = pipeline.apply_filter(input, None, &ctx).unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_native_columnar_pipeline_default() {
        let pipeline = NativeColumnarPipeline::default();
        assert!(pipeline.name().starts_with("NativeColumnarPipeline"));
    }

    #[test]
    fn test_native_columnar_pipeline_limit_offset() {
        let pipeline = NativeColumnarPipeline::new();

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
}
