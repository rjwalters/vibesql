//! Utility functions for window function evaluation
//!
//! This module provides simplified expression evaluation for LAG/LEAD window functions.
//!
//! # Limitations
//!
//! The current implementation uses a simplified expression evaluator that:
//! - Only supports literals and column references
//! - Resolves column references by parsing column names as numeric indices (e.g., "0", "1")
//! - Falls back to the first column for non-numeric column names
//!
//! # Future Enhancement
//!
//! For full LAG/LEAD integration into the main window function execution path,
//! this module should be refactored to:
//! 1. Accept a schema parameter for proper column name resolution
//! 2. Use the full ExpressionEvaluator instead of this simplified version
//! 3. Support all expression types (functions, operators, subqueries, etc.)
//! 4. Thread the evaluator through all window function implementations
//!
//! See `crates/executor/src/select/window/evaluation.rs` for how the main
//! window function evaluation (SUM, AVG, COUNT, etc.) uses CombinedExpressionEvaluator.

use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Simple expression evaluation for window functions
///
/// This is a simplified version that handles basic cases (literals and column references).
/// It does NOT use schema context and has limited expression support.
///
/// # Limitations
/// - Column references are resolved by numeric index only
/// - Does not support complex expressions (functions, operators, subqueries)
/// - No type checking or validation
///
/// # Note
/// This function is currently only used by LAG/LEAD window functions in tests.
/// The main window function evaluation path uses proper ExpressionEvaluator.
pub fn evaluate_expression(expr: &Expression, row: &Row) -> Result<SqlValue, String> {
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        Expression::ColumnRef { column, .. } => {
            // Try parsing column name as index (e.g., "0", "1")
            if let Ok(index) = column.parse::<usize>() {
                row.get(index)
                    .cloned()
                    .ok_or_else(|| format!("Column index {} out of bounds", index))
            } else {
                // Fallback: assume first column
                // This is a limitation - proper implementation should use schema
                row.get(0).cloned().ok_or_else(|| "Row has no columns".to_string())
            }
        }
        _ => Err("Unsupported expression in window function".to_string()),
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
