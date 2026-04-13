//! Utility functions for window function evaluation
//!
//! This module provides helper functions for window function evaluation.

use vibesql_ast::Expression;
use vibesql_types::SqlValue;

/// Simple expression evaluator for tests
///
/// This version uses index-based column resolution (columns named "0", "1", etc.)
/// and only handles Literal and ColumnRef expressions. It is used as a test-only
/// eval_fn for unit tests where the full CombinedExpressionEvaluator is not available.
#[cfg(test)]
pub fn evaluate_expression(expr: &Expression, row: &vibesql_storage::Row) -> Result<SqlValue, String> {
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
                // Fallback: assume first column (for simple test cases)
                row.get(0).cloned().ok_or_else(|| "Row has no columns".to_string())
            }
        }
        _ => Err("Unsupported expression in test evaluator".to_string()),
    }
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
