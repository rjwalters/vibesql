//! Batch projection for SELECT expressions
//!
//! This module provides batch evaluation of SELECT projection expressions.
//! For simple column-only projections, uses optimized batch processing.
//! For complex expressions, falls back to row-by-row projection.

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema,
};
use std::collections::HashMap;
use vibesql_ast::{Expression, SelectItem};
use vibesql_storage::{QueryBufferPool, Row};

use super::window::WindowFunctionKey;

/// Minimum number of rows to trigger batch projection optimization
const BATCH_PROJECTION_THRESHOLD: usize = 100;

/// Attempt batch projection for SELECT expressions
///
/// Optimizes simple column-only projections (no expressions, no wildcards)
/// by computing column indices once and applying them to all rows in batch.
///
/// For complex projections (expressions, wildcards, window functions),
/// falls back to row-by-row projection.
#[allow(unused_variables)]
pub fn try_batch_project_simd(
    rows: &[Row],
    columns: &[SelectItem],
    evaluator: &CombinedExpressionEvaluator,
    schema: &CombinedSchema,
    window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    buffer_pool: &QueryBufferPool,
) -> Result<Option<Vec<Row>>, ExecutorError> {
    // Skip batch optimization for small result sets or when window functions exist
    if rows.len() < BATCH_PROJECTION_THRESHOLD || window_mapping.is_some() {
        return Ok(None);
    }

    // Try to extract column indices for simple column-only projections
    let column_indices = match extract_simple_column_indices(columns, schema) {
        Some(indices) => indices,
        None => return Ok(None), // Complex projection - fall back to row-by-row
    };

    // Batch project using pre-computed column indices
    let projected_rows = batch_project_by_indices(rows, &column_indices, buffer_pool);

    Ok(Some(projected_rows))
}

/// Extract column indices for simple column-only projections
///
/// Returns Some(indices) if all SELECT items are simple column references.
/// Returns None if any item requires expression evaluation.
fn extract_simple_column_indices(
    columns: &[SelectItem],
    schema: &CombinedSchema,
) -> Option<Vec<usize>> {
    let mut indices = Vec::with_capacity(columns.len());

    for item in columns {
        match item {
            // Simple column reference: expr is ColumnRef
            SelectItem::Expression { expr, alias: _ } => {
                if let Expression::ColumnRef { table, column } = expr {
                    // Resolve column index from schema
                    if let Some(idx) = resolve_column_index(table.as_deref(), column, schema) {
                        indices.push(idx);
                    } else {
                        // Column not found - fall back to row-by-row
                        return None;
                    }
                } else {
                    // Complex expression - fall back to row-by-row
                    return None;
                }
            }
            // Wildcards require expansion - fall back to row-by-row
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                return None;
            }
        }
    }

    Some(indices)
}

/// Resolve column index from schema
fn resolve_column_index(
    table: Option<&str>,
    column: &str,
    schema: &CombinedSchema,
) -> Option<usize> {
    // If table is specified, do qualified lookup
    if let Some(table_name) = table {
        if let Some((start_idx, table_schema)) = schema.table_schemas.get(table_name) {
            for (col_idx, col) in table_schema.columns.iter().enumerate() {
                if col.name.eq_ignore_ascii_case(column) {
                    return Some(start_idx + col_idx);
                }
            }
        }
        // Try case-insensitive table name match
        for (tbl_name, (start_idx, table_schema)) in &schema.table_schemas {
            if tbl_name.eq_ignore_ascii_case(table_name) {
                for (col_idx, col) in table_schema.columns.iter().enumerate() {
                    if col.name.eq_ignore_ascii_case(column) {
                        return Some(start_idx + col_idx);
                    }
                }
            }
        }
        return None;
    }

    // Unqualified column - search all tables
    for (start_idx, table_schema) in schema.table_schemas.values() {
        for (col_idx, col) in table_schema.columns.iter().enumerate() {
            if col.name.eq_ignore_ascii_case(column) {
                return Some(start_idx + col_idx);
            }
        }
    }

    None
}

/// Batch project rows using pre-computed column indices
///
/// This is significantly faster than row-by-row projection because:
/// 1. Column indices are computed once (not per-row)
/// 2. Result vector is pre-allocated with exact capacity
/// 3. Each row's value vector is pre-allocated with exact size
fn batch_project_by_indices(
    rows: &[Row],
    column_indices: &[usize],
    buffer_pool: &QueryBufferPool,
) -> Vec<Row> {
    let num_output_columns = column_indices.len();

    // Pre-allocate result with exact capacity
    let mut result = buffer_pool.get_row_buffer(rows.len());

    for row in rows {
        // Pre-allocate values vector with exact size
        let mut values = Vec::with_capacity(num_output_columns);

        for &idx in column_indices {
            // Clone value from source row at computed index
            if let Some(value) = row.values.get(idx) {
                values.push(value.clone());
            } else {
                // Index out of bounds - push NULL (shouldn't happen with valid schema)
                values.push(vibesql_types::SqlValue::Null);
            }
        }

        result.push(Row::new(values));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{evaluator::CombinedExpressionEvaluator, schema::CombinedSchema};
    use vibesql_ast::SelectItem;
    use vibesql_storage::{QueryBufferPool, Row};
    use vibesql_types::{DataType, SqlValue};

    fn create_test_evaluator() -> CombinedExpressionEvaluator<'static> {
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![
            ColumnSchema::new("a".to_string(), DataType::Bigint, false),
            ColumnSchema::new("b".to_string(), DataType::Bigint, false),
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);

        let schema =
            Box::leak(Box::new(CombinedSchema::from_table("test".to_string(), table_schema)));
        CombinedExpressionEvaluator::new(schema)
    }

    fn create_test_schema() -> CombinedSchema {
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![
            ColumnSchema::new("a".to_string(), DataType::Bigint, false),
            ColumnSchema::new("b".to_string(), DataType::Bigint, false),
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);

        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    #[test]
    fn test_try_batch_project_simd_returns_none_for_literals() {
        // Literal expressions should fall back to row-by-row
        let rows: Vec<Row> = (0..200)
            .map(|i| Row::new(vec![SqlValue::Bigint(i as i64), SqlValue::Bigint(i as i64 * 2)]))
            .collect();

        let columns = vec![SelectItem::Expression {
            expr: vibesql_ast::Expression::Literal(SqlValue::Integer(42)),
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result =
            try_batch_project_simd(&rows, &columns, &evaluator, &schema, &None, &buffer_pool);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_try_batch_project_simd_returns_none_for_small_rowset() {
        // Small row sets (< 100) should fall back to row-by-row
        let rows: Vec<Row> = (0..50)
            .map(|i| Row::new(vec![SqlValue::Bigint(i as i64), SqlValue::Bigint(i as i64 * 2)]))
            .collect();

        let columns = vec![SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            },
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result =
            try_batch_project_simd(&rows, &columns, &evaluator, &schema, &None, &buffer_pool);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_try_batch_project_simd_works_for_simple_columns() {
        // Simple column projection on large rowset should use batch projection
        let rows: Vec<Row> = (0..200)
            .map(|i| Row::new(vec![SqlValue::Bigint(i as i64), SqlValue::Bigint(i as i64 * 2)]))
            .collect();

        // SELECT b FROM test (just column b)
        let columns = vec![SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "b".to_string(),
            },
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result =
            try_batch_project_simd(&rows, &columns, &evaluator, &schema, &None, &buffer_pool);

        assert!(result.is_ok());
        let projected = result.unwrap();
        assert!(projected.is_some());

        let projected_rows = projected.unwrap();
        assert_eq!(projected_rows.len(), 200);

        // Check that we got column b values (which are i * 2)
        for (i, row) in projected_rows.iter().enumerate() {
            assert_eq!(row.values.len(), 1);
            assert_eq!(row.values[0], SqlValue::Bigint(i as i64 * 2));
        }
    }

    #[test]
    fn test_try_batch_project_simd_multiple_columns() {
        // Test reordering columns: SELECT b, a FROM test
        let rows: Vec<Row> = (0..200)
            .map(|i| Row::new(vec![SqlValue::Bigint(i as i64), SqlValue::Bigint(i as i64 * 10)]))
            .collect();

        let columns = vec![
            SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "b".to_string(),
                },
                alias: None,
            },
            SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                },
                alias: None,
            },
        ];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result =
            try_batch_project_simd(&rows, &columns, &evaluator, &schema, &None, &buffer_pool);

        assert!(result.is_ok());
        let projected = result.unwrap();
        assert!(projected.is_some());

        let projected_rows = projected.unwrap();
        assert_eq!(projected_rows.len(), 200);

        // Check that columns are reordered: [b, a] = [i*10, i]
        for (i, row) in projected_rows.iter().enumerate() {
            assert_eq!(row.values.len(), 2);
            assert_eq!(row.values[0], SqlValue::Bigint(i as i64 * 10)); // b
            assert_eq!(row.values[1], SqlValue::Bigint(i as i64)); // a
        }
    }
}
