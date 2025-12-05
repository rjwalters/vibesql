//! Simple expression evaluation for single rows
//!
//! This module provides lightweight expression evaluation for the subset of
//! expressions we support in columnar aggregates (column references and
//! binary operations).

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Evaluate a simple arithmetic expression for a single row
///
/// This is a lightweight evaluator for the subset of expressions we support
/// in columnar aggregates (column references and binary operations).
pub fn eval_simple_expr(
    expr: &Expression,
    row: &Row,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    match expr {
        Expression::ColumnRef { table, column } => {
            let col_idx = schema.get_column_index(table.as_deref(), column).ok_or_else(|| {
                ExecutorError::UnsupportedExpression(format!("Column not found: {}", column))
            })?;
            Ok(row.get(col_idx).cloned().unwrap_or(SqlValue::Null))
        }
        Expression::Literal(val) => Ok(val.clone()),
        Expression::BinaryOp { left, op, right } => {
            let left_val = eval_simple_expr(left, row, schema)?;
            let right_val = eval_simple_expr(right, row, schema)?;

            // Use the evaluator's operators module
            use crate::evaluator::operators::OperatorRegistry;
            OperatorRegistry::eval_binary_op(
                &left_val,
                op,
                &right_val,
                vibesql_types::SqlMode::default(),
            )
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "Complex expressions not supported in columnar aggregates".to_string(),
        )),
    }
}

/// Convert SqlValue to f64 for arithmetic operations
pub fn sql_value_to_f64(val: &SqlValue) -> Option<f64> {
    match val {
        SqlValue::Integer(v) => Some(*v as f64),
        SqlValue::Bigint(v) => Some(*v as f64),
        SqlValue::Smallint(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v as f64),
        SqlValue::Double(v) => Some(*v),
        SqlValue::Numeric(v) => Some(*v),
        SqlValue::Null => None,
        _ => None,
    }
}
