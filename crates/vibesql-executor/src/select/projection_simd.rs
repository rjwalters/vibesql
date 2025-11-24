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
            if can_use_simd_for_expression(expr, rows.len()) {
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
                let values = if can_use_simd_for_expression(expr, rows.len()) {
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

// TODO: Add comprehensive unit tests for batch SIMD projection
// Basic functionality verified through integration tests
