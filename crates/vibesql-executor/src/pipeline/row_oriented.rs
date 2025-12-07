//! Row-Oriented Execution Pipeline
//!
//! Implements the `ExecutionPipeline` trait using traditional row-by-row execution.
//! This pipeline delegates to existing proven logic for filtering and projection
//! while providing a unified interface.

use std::collections::HashMap;

use vibesql_ast::{Expression, SelectItem};
use vibesql_storage::Row;

use crate::{
    errors::ExecutorError,
    select::{
        filter::apply_where_filter_combined_auto, projection::project_row_combined,
        window::WindowFunctionKey,
    },
    SelectExecutor,
};

use super::{ExecutionContext, ExecutionPipeline, PipelineInput, PipelineOutput};

/// Row-oriented execution pipeline.
///
/// This pipeline processes data row-by-row, using the existing proven execution
/// logic from the select module. It provides good compatibility with all query
/// patterns but may be slower than columnar approaches for large datasets.
///
/// # Performance Characteristics
///
/// - **Filter**: Row-by-row predicate evaluation with pattern detection for fast paths
/// - **Projection**: Per-row column extraction and expression evaluation
/// - **Aggregation**: Currently delegates to existing SelectExecutor aggregation path
///
/// # When to Use
///
/// - Complex expressions that can't be vectorized
/// - Queries with correlated subqueries
/// - Small to medium datasets where columnar overhead isn't justified
pub struct RowOrientedPipeline<'a> {
    /// The executor providing query infrastructure (timeout, memory tracking, buffer pools)
    executor: &'a SelectExecutor<'a>,
    /// Optional window function result mapping
    window_mapping: Option<HashMap<WindowFunctionKey, usize>>,
}

impl<'a> RowOrientedPipeline<'a> {
    /// Create a new row-oriented pipeline.
    ///
    /// # Arguments
    /// * `executor` - The SelectExecutor providing query infrastructure
    #[inline]
    pub fn new(executor: &'a SelectExecutor<'a>) -> Self {
        Self { executor, window_mapping: None }
    }

    /// Create a new row-oriented pipeline with window function mapping.
    ///
    /// # Arguments
    /// * `executor` - The SelectExecutor providing query infrastructure
    /// * `window_mapping` - Mapping of window functions to result column indices
    #[inline]
    pub fn with_window_mapping(
        executor: &'a SelectExecutor<'a>,
        window_mapping: HashMap<WindowFunctionKey, usize>,
    ) -> Self {
        Self { executor, window_mapping: Some(window_mapping) }
    }
}

impl ExecutionPipeline for RowOrientedPipeline<'_> {
    /// Create an evaluator with context for row-oriented execution.
    #[inline]
    fn create_evaluator<'a>(
        &self,
        ctx: &'a ExecutionContext<'a>,
    ) -> crate::evaluator::CombinedExpressionEvaluator<'a> {
        ctx.create_evaluator()
    }

    /// Apply WHERE clause filtering row-by-row.
    ///
    /// Uses `apply_where_filter_combined_auto` which automatically selects
    /// between sequential, vectorized, and parallel execution based on
    /// dataset size and hardware capabilities.
    #[inline]
    fn apply_filter(
        &self,
        input: PipelineInput<'_>,
        predicate: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // Convert input to owned rows
        let rows = input.into_rows();

        // No predicate = no filtering
        if predicate.is_none() {
            return Ok(PipelineOutput::from_rows(rows));
        }

        // Create evaluator from context
        let evaluator = ctx.create_evaluator();

        // Apply filter using the auto-selecting implementation
        let filtered =
            apply_where_filter_combined_auto(rows, predicate, &evaluator, self.executor)?;

        Ok(PipelineOutput::from_rows(filtered))
    }

    /// Apply SELECT projection row-by-row.
    ///
    /// Processes each row through `project_row_combined` which handles:
    /// - Wildcard expansion (`SELECT *`)
    /// - Qualified wildcards (`SELECT table.*`)
    /// - Expression evaluation
    /// - Window function result lookup
    #[inline]
    fn apply_projection(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // Convert input to rows for processing
        let rows = input.into_rows();

        if rows.is_empty() {
            return Ok(PipelineOutput::Empty);
        }

        // Create evaluator from context
        let evaluator = ctx.create_evaluator();
        let buffer_pool = self.executor.query_buffer_pool();

        // Project each row
        let mut projected_rows = Vec::with_capacity(rows.len());
        for row in rows {
            // Check timeout periodically
            self.executor.check_timeout()?;

            let projected = project_row_combined(
                &row,
                select_items,
                &evaluator,
                ctx.schema,
                &self.window_mapping,
                buffer_pool,
            )?;
            projected_rows.push(projected);
        }

        Ok(PipelineOutput::from_rows(projected_rows))
    }

    /// Execute aggregation with optional GROUP BY.
    ///
    /// Note: Full aggregation support is implemented via SelectExecutor's
    /// `execute_with_aggregation` method. This pipeline method provides
    /// basic expression evaluation for simple non-aggregate cases.
    ///
    /// For queries with aggregate functions, callers should use the
    /// SelectExecutor aggregation path directly.
    fn apply_aggregation(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        group_by: Option<&[Expression]>,
        _having: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError> {
        // For aggregation with GROUP BY, recommend using SelectExecutor directly
        // The full aggregation path requires internal SelectExecutor methods
        if let Some(exprs) = group_by {
            if !exprs.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "GROUP BY aggregation should use SelectExecutor.execute_with_aggregation()"
                        .to_string(),
                ));
            }
        }

        let rows = input.into_rows();

        // For simple non-aggregate queries, evaluate expressions directly
        // This handles SELECT 1+1 type queries
        if rows.is_empty() {
            // Create one result row with evaluated expressions
            let evaluator = ctx.create_evaluator();
            let empty_row = Row::new(vec![]);

            let mut values = Vec::new();
            for item in select_items {
                if let SelectItem::Expression { expr, .. } = item {
                    let value = evaluator.eval(expr, &empty_row)?;
                    values.push(value);
                }
            }

            return Ok(PipelineOutput::from_rows(vec![Row::new(values)]));
        }

        // For queries with rows but no GROUP BY, treat all rows as one group
        // This is a simplified aggregation that doesn't handle aggregate functions
        let evaluator = ctx.create_evaluator();
        let first_row = &rows[0];

        let mut values = Vec::new();
        for item in select_items {
            if let SelectItem::Expression { expr, .. } = item {
                let value = evaluator.eval(expr, first_row)?;
                values.push(value);
            }
        }

        Ok(PipelineOutput::from_rows(vec![Row::new(values)]))
    }

    /// Row-oriented pipeline supports all query patterns for filter/projection.
    /// Aggregation support is partial (simple expressions only).
    #[inline]
    fn supports_query_pattern(
        &self,
        has_aggregation: bool,
        has_group_by: bool,
        _has_joins: bool,
    ) -> bool {
        // Full support for filter and projection
        // Partial support for aggregation (simple cases without GROUP BY)
        !has_aggregation || !has_group_by
    }

    #[inline]
    fn name(&self) -> &'static str {
        "RowOrientedPipeline"
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
    fn test_row_pipeline_name() {
        let database = vibesql_storage::Database::new();
        let executor = SelectExecutor::new(&database);
        let pipeline = RowOrientedPipeline::new(&executor);
        assert_eq!(pipeline.name(), "RowOrientedPipeline");
    }

    #[test]
    fn test_row_pipeline_supports_patterns() {
        let database = vibesql_storage::Database::new();
        let executor = SelectExecutor::new(&database);
        let pipeline = RowOrientedPipeline::new(&executor);

        // Supports non-aggregate patterns
        assert!(pipeline.supports_query_pattern(false, false, false));
        assert!(pipeline.supports_query_pattern(false, false, true));

        // Supports simple aggregates without GROUP BY
        assert!(pipeline.supports_query_pattern(true, false, false));

        // Does not fully support aggregates with GROUP BY
        assert!(!pipeline.supports_query_pattern(true, true, false));
    }

    #[test]
    fn test_row_pipeline_filter_no_predicate() {
        let (database, schema) = create_test_setup();
        let executor = SelectExecutor::new(&database);
        let pipeline = RowOrientedPipeline::new(&executor);

        let rows = vec![make_test_row(vec![1, 2]), make_test_row(vec![3, 4])];
        let input = PipelineInput::from_rows_owned(rows);
        let ctx = ExecutionContext::new(&schema, &database);

        let result = pipeline.apply_filter(input, None, &ctx).unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_row_pipeline_limit_offset() {
        let (database, _schema) = create_test_setup();
        let executor = SelectExecutor::new(&database);
        let pipeline = RowOrientedPipeline::new(&executor);

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
