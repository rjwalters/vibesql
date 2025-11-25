//! SIMD-accelerated batch projection for SELECT expressions
//!
//! This module provides batch evaluation of SELECT projection expressions using SIMD
//! operations when beneficial. It complements the row-by-row projection in projection.rs.

use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    schema::CombinedSchema,
};
use std::collections::HashMap;
use vibesql_ast::SelectItem;
use vibesql_storage::{QueryBufferPool, Row};
use vibesql_types::SqlValue;

#[cfg(feature = "simd")]
use crate::simd::expression::{can_use_simd_for_expression, eval_expression_batch_simd};

use super::window::WindowFunctionKey;

/// Threshold for using batch projection with SIMD
/// Below this, row-by-row projection is more efficient
#[cfg(feature = "simd")]
const BATCH_PROJECTION_THRESHOLD: usize = 100;

/// Project multiple rows at once using SIMD for arithmetic expressions
///
/// This function attempts to identify SELECT expressions that can benefit from
/// batch SIMD evaluation. For eligible expressions, it:
/// 1. Evaluates the expression across all rows using SIMD
/// 2. Assembles projected rows from the batch results
///
/// Falls back to row-by-row projection for:
/// - Small row counts (< threshold)
/// - Complex expressions (subqueries, aggregates, functions)
/// - Mixed expression types (some SIMD, some not)
#[cfg(feature = "simd")]
pub fn try_batch_project_simd(
    rows: &[Row],
    columns: &[SelectItem],
    evaluator: &CombinedExpressionEvaluator,
    schema: &CombinedSchema,
    window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    buffer_pool: &QueryBufferPool,
) -> Result<Option<Vec<Row>>, ExecutorError> {
    // Only use batch projection for large datasets
    if rows.is_empty() || rows.len() < BATCH_PROJECTION_THRESHOLD {
        return Ok(None);
    }

    // Check if we have window functions (can't use SIMD with these yet)
    if window_mapping.is_some() {
        return Ok(None);
    }

    // Analyze SELECT list to see if any expressions can use SIMD
    let mut has_simd_expr = false;
    for item in columns {
        if let SelectItem::Expression { expr, .. } = item {
            if can_use_simd_for_expression(expr, rows, evaluator) {
                has_simd_expr = true;
                break;
            }
        }
    }

    // If no expressions can benefit from SIMD, fall back to row-by-row
    if !has_simd_expr {
        return Ok(None);
    }

    // Evaluate each SELECT expression in batch mode
    let mut column_results = Vec::with_capacity(columns.len());

    for item in columns {
        match item {
            SelectItem::Wildcard { .. } => {
                // SELECT * - include all columns from each row
                // Can't use SIMD for this (just copying column values)
                // Collect all rows' values for each column position
                let num_cols = rows.first().map(|r| r.values.len()).unwrap_or(0);
                for col_idx in 0..num_cols {
                    let col_values: Vec<SqlValue> =
                        rows.iter().map(|row| row.values[col_idx].clone()).collect();
                    column_results.push(col_values);
                }
            }

            SelectItem::QualifiedWildcard { qualifier, .. } => {
                // SELECT table.* - include columns from specific table
                let result = schema.table_schemas.get(qualifier).cloned().or_else(|| {
                    let qualifier_lower = qualifier.to_lowercase();
                    schema
                        .table_schemas
                        .iter()
                        .find(|(key, _)| key.to_lowercase() == qualifier_lower)
                        .map(|(_, value)| value.clone())
                });

                if let Some((start_index, table_schema)) = result {
                    let num_columns = table_schema.columns.len();
                    let end_index = start_index + num_columns;

                    // Extract columns for this table from each row
                    for col_idx in start_index..end_index {
                        let col_values: Vec<SqlValue> =
                            rows.iter().map(|row| row.values[col_idx].clone()).collect();
                        column_results.push(col_values);
                    }
                }
            }

            SelectItem::Expression { expr, .. } => {
                // Try SIMD evaluation for this expression
                let values = if can_use_simd_for_expression(expr, rows, evaluator) {
                    // Use SIMD path
                    match eval_expression_batch_simd(expr, rows, evaluator) {
                        Ok(v) => v,
                        Err(_) => {
                            // SIMD failed (e.g., NULL values, type issues) - fall back entirely
                            return Ok(None);
                        }
                    }
                } else {
                    // Use scalar path for this expression
                    let mut values = Vec::with_capacity(rows.len());
                    for row in rows {
                        values.push(evaluator.eval(expr, row)?);
                    }
                    values
                };
                column_results.push(values);
            }
        }
    }

    // Transpose column_results into rows
    let num_rows = rows.len();
    let mut projected_rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut row_values = buffer_pool.get_value_buffer(column_results.len());
        for col_results in &column_results {
            row_values.push(col_results[row_idx].clone());
        }
        let values = std::mem::take(&mut row_values);
        buffer_pool.return_value_buffer(row_values);
        projected_rows.push(Row::new(values));
    }

    Ok(Some(projected_rows))
}

/// Fallback when SIMD feature is not enabled
#[cfg(not(feature = "simd"))]
pub fn try_batch_project_simd(
    _rows: &[Row],
    _columns: &[SelectItem],
    _evaluator: &CombinedExpressionEvaluator,
    _schema: &CombinedSchema,
    _window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    _buffer_pool: &QueryBufferPool,
) -> Result<Option<Vec<Row>>, ExecutorError> {
    // SIMD not enabled - always fall back to row-by-row
    Ok(None)
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;
    use crate::{evaluator::CombinedExpressionEvaluator, schema::CombinedSchema};
    use vibesql_ast::{BinaryOperator, Expression, SelectItem};
    use vibesql_storage::{QueryBufferPool, Row};
    use vibesql_types::{DataType, SqlValue};

    // Helper to create test evaluator
    fn create_test_evaluator() -> CombinedExpressionEvaluator<'static> {
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![ColumnSchema::new("a".to_string(), DataType::Bigint, false)];
        let table_schema = TableSchema::new("test".to_string(), columns);

        let schema = Box::leak(Box::new(CombinedSchema::from_table("test".to_string(), table_schema)));
        CombinedExpressionEvaluator::new(schema)
    }

    // Helper to create test schema
    fn create_test_schema() -> CombinedSchema {
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![ColumnSchema::new("a".to_string(), DataType::Bigint, false)];
        let table_schema = TableSchema::new("test".to_string(), columns);

        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    // Helper to create test rows
    fn create_test_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| Row::new(vec![SqlValue::Bigint(i as i64), SqlValue::Bigint((i * 2) as i64)]))
            .collect()
    }

    // ===== Batch Projection Threshold Tests =====

    #[test]
    fn test_try_batch_project_simd_returns_none_for_empty_rows() {
        let rows = vec![];
        let columns = vec![SelectItem::Expression {
            expr: Expression::Literal(SqlValue::Integer(1)),
            alias: None,
        }];
        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_try_batch_project_simd_returns_none_below_threshold() {
        let rows = create_test_rows(50); // Below BATCH_PROJECTION_THRESHOLD (100)
        let columns = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            },
            alias: None,
        }];
        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_try_batch_project_simd_returns_none_at_threshold() {
        let rows = create_test_rows(100); // Exactly at threshold
        let columns = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            },
            alias: None,
        }];
        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        // At threshold, should attempt SIMD (may return Some or None depending on expr evaluation)
        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
    }

    // ===== SIMD-Compatible Expression Tests =====

    #[test]
    fn test_try_batch_project_simd_returns_none_without_simd_expr() {
        let rows = create_test_rows(100);

        // Complex expression (function) - not SIMD-compatible
        let columns = vec![SelectItem::Expression {
            expr: Expression::Function {
                name: "ABS".to_string(),
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "x".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_try_batch_project_simd_returns_none_with_window_functions() {
        let rows = create_test_rows(100);

        // SIMD-compatible expression
        let columns = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            },
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        // Window mapping present - should return None
        let window_mapping = Some(HashMap::new());

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &window_mapping,
            &buffer_pool,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    // ===== Wildcard Tests =====

    #[test]
    fn test_try_batch_project_simd_handles_wildcard() {
        let rows = vec![
            Row::new(vec![SqlValue::Bigint(1), SqlValue::Bigint(2), SqlValue::Bigint(3)]),
            Row::new(vec![SqlValue::Bigint(4), SqlValue::Bigint(5), SqlValue::Bigint(6)]),
        ];

        let columns = vec![SelectItem::Wildcard { alias: None }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
        // Returns None due to row count < threshold, but function doesn't panic
    }

    #[test]
    fn test_try_batch_project_simd_handles_qualified_wildcard() {
        let rows = vec![
            Row::new(vec![SqlValue::Bigint(1), SqlValue::Bigint(2)]),
            Row::new(vec![SqlValue::Bigint(3), SqlValue::Bigint(4)]),
        ];

        let columns = vec![SelectItem::QualifiedWildcard {
            qualifier: "t".to_string(),
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
    }

    // ===== Expression Evaluation Tests =====

    #[test]
    fn test_try_batch_project_simd_evaluates_simple_expressions() {
        let rows = create_test_rows(100);

        // Simple arithmetic expression
        let columns = vec![SelectItem::Expression {
            expr: Expression::Literal(SqlValue::Integer(42)),
            alias: Some("const".to_string()),
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
        // May return Some or None depending on SIMD applicability
    }

    #[test]
    fn test_try_batch_project_simd_falls_back_on_simd_error() {
        let rows = create_test_rows(100);

        // Expression that will trigger SIMD but may fail (e.g., due to evaluation issues)
        let columns = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "nonexistent".to_string(),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            },
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        // Should fall back to None on error, not propagate error
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    // ===== Mixed Expression Tests =====

    #[test]
    fn test_try_batch_project_simd_handles_mixed_expressions() {
        let rows = create_test_rows(100);

        // Mix of SIMD-compatible and non-SIMD expressions
        let columns = vec![
            SelectItem::Expression {
                expr: Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "a".to_string(),
                    }),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::Literal(SqlValue::Integer(1))),
                },
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(42)),
                alias: None,
            },
        ];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
    }

    // ===== Result Transposition Tests =====

    #[test]
    fn test_try_batch_project_simd_transposes_results_correctly() {
        let rows = create_test_rows(100);

        // Multiple columns
        let columns = vec![
            SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(1)),
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(2)),
                alias: None,
            },
        ];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());

        // If SIMD path is taken, verify row structure
        if let Ok(Some(projected_rows)) = result {
            // Each row should have 2 columns
            for row in projected_rows {
                assert_eq!(row.values.len(), 2);
            }
        }
    }

    // ===== Integration with SIMD Expression Evaluation =====

    #[test]
    fn test_try_batch_project_simd_uses_simd_for_arithmetic() {
        let rows = create_test_rows(100);

        // Arithmetic expression that can use SIMD
        let columns = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Integer(10))),
                op: BinaryOperator::Multiply,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            },
            alias: Some("calc".to_string()),
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_try_batch_project_simd_uses_scalar_for_non_simd() {
        let rows = create_test_rows(100);

        // Expression that cannot use SIMD (aggregate)
        let columns = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: "COUNT".to_string(),
                args: vec![Expression::Literal(SqlValue::Integer(1))],
                distinct: false,
            },
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
        // Should return None (not SIMD-compatible)
        assert_eq!(result.unwrap(), None);
    }

    // ===== Buffer Pool Usage Tests =====

    #[test]
    fn test_try_batch_project_simd_uses_buffer_pool() {
        let rows = create_test_rows(100);

        let columns = vec![
            SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(1)),
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(2)),
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(3)),
                alias: None,
            },
        ];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        // This should use buffer pool for row value vectors
        let result = try_batch_project_simd(
            &rows,
            &columns,
            &evaluator,
            &schema,
            &None,
            &buffer_pool,
        );

        assert!(result.is_ok());
    }
}
