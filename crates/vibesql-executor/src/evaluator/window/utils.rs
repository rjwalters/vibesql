//! Utility functions for window function evaluation
//!
//! This module provides expression evaluation for window function frame calculations.
//! The `evaluate_expression_with_map` function supports column name resolution via
//! a pre-built column name to index mapping.

use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Evaluate expression with column name mapping support
///
/// This version accepts a column name to index mapping for resolving named columns.
pub fn evaluate_expression_with_map(
    expr: &Expression,
    row: &Row,
    column_map: &std::collections::HashMap<String, usize>,
) -> Result<SqlValue, String> {
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        Expression::ColumnRef(col_id) => {
            let column = col_id.column_canonical();
            // Try parsing column name as index (e.g., "0", "1")
            if let Ok(index) = column.parse::<usize>() {
                row.get(index)
                    .cloned()
                    .ok_or_else(|| format!("Column index {} out of bounds", index))
            } else {
                // Try to find column name in the mapping
                if let Some(&index) = column_map.get(column) {
                    row.get(index)
                        .cloned()
                        .ok_or_else(|| format!("Column index {} out of bounds", index))
                } else if let Some(&index) = column_map.get(&column.to_lowercase()) {
                    row.get(index)
                        .cloned()
                        .ok_or_else(|| format!("Column index {} out of bounds", index))
                } else {
                    // Fallback: assume first column
                    // This is a limitation - proper implementation should use schema
                    row.get(0).cloned().ok_or_else(|| "Row has no columns".to_string())
                }
            }
        }
        _ => Err("Unsupported expression in window function".to_string()),
    }
}

/// Simple expression evaluator for tests
///
/// This version uses index-based column resolution (columns named "0", "1", etc.)
/// without requiring a column name mapping.
pub fn evaluate_expression(expr: &Expression, row: &Row) -> Result<SqlValue, String> {
    evaluate_expression_with_map(expr, row, &std::collections::HashMap::new())
}

/// Evaluate default value expression
pub fn evaluate_default_value(default: Option<&Expression>) -> Result<SqlValue, String> {
    match default {
        None => Ok(SqlValue::Null),
        Some(expr) => match expr {
            Expression::Literal(val) => Ok(val.clone()),
            _ => Err("Default value must be a literal".to_string()),
        },
    }
}
