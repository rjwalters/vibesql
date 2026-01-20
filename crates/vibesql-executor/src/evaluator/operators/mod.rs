//! Trait-based operator evaluation system
//!
//! This module provides a pluggable, testable operator system that replaces
//! the monolithic match statement in the core evaluator. Each operator category
//! (arithmetic, comparison, logical, string, vector) is implemented in its own module
//! with dedicated logic and tests.

mod arithmetic;
mod bitwise;
mod comparison;
mod logical;
mod string;
mod vector;

use arithmetic::ArithmeticOps;
use bitwise::BitwiseOps;
use comparison::ComparisonOps;
use logical::LogicalOps;
use string::StringOps;
use vector::VectorOps;

// Re-export string truthiness helper for use in CASE evaluation
pub use logical::is_truthy_string;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// JSON extraction helper
/// Extracts a value from a JSON string using a key or path
/// - as_text=false: returns JSON value (->)
/// - as_text=true: returns text value (->>)
fn json_extract(
    left: &SqlValue,
    right: &SqlValue,
    as_text: bool,
) -> Result<SqlValue, ExecutorError> {
    // Get the JSON string from left operand
    let json_str = match left {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => {
            return Err(ExecutorError::TypeError(format!(
                "JSON operator requires string, got {:?}",
                left
            )));
        }
    };

    // Get the key/path from right operand
    let key = match right {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str().to_string(),
        SqlValue::Integer(i) => i.to_string(), // Array index
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => {
            return Err(ExecutorError::TypeError(format!(
                "JSON key must be string or integer, got {:?}",
                right
            )));
        }
    };

    // Parse JSON
    let json_value: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return Ok(SqlValue::Null), // Invalid JSON returns NULL
    };

    // Extract the value
    let extracted = if let Ok(index) = key.parse::<usize>() {
        // Array index access
        json_value.get(index)
    } else {
        // Object key access
        json_value.get(&key)
    };

    match extracted {
        Some(value) => {
            if as_text {
                // ->> returns text (unquoted for strings, serialized for others)
                match value {
                    serde_json::Value::Null => Ok(SqlValue::Null),
                    serde_json::Value::String(s) => {
                        Ok(SqlValue::Varchar(arcstr::ArcStr::from(s.as_str())))
                    }
                    serde_json::Value::Number(n) => {
                        Ok(SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())))
                    }
                    serde_json::Value::Bool(b) => {
                        Ok(SqlValue::Varchar(arcstr::ArcStr::from(if *b {
                            "true"
                        } else {
                            "false"
                        })))
                    }
                    _ => Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string()))),
                }
            } else {
                // -> returns JSON (serialized)
                Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string())))
            }
        }
        None => Ok(SqlValue::Null), // Key not found returns NULL
    }
}

/// Trait for binary operator evaluation
#[allow(dead_code)]
pub(crate) trait BinaryOperator {
    /// Evaluate the operator on two SQL values
    fn evaluate(&self, left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError>;
}

/// Central registry for all binary operators
///
/// This provides a unified interface for evaluating binary operations,
/// dispatching to the appropriate specialized operator implementation.
pub(crate) struct OperatorRegistry;

impl OperatorRegistry {
    /// Evaluate a binary operation using the appropriate operator implementation
    #[inline]
    pub fn eval_binary_op(
        left: &SqlValue,
        op: &vibesql_ast::BinaryOperator,
        right: &SqlValue,
        sql_mode: vibesql_types::SqlMode,
    ) -> Result<SqlValue, ExecutorError> {
        use vibesql_ast::BinaryOperator::*;

        // Short-circuit NULL handling for most operators (SQL three-valued logic)
        // NULL compared/operated with anything yields NULL
        // EXCEPT for AND/OR which have special three-valued logic (e.g., NULL OR TRUE = TRUE)
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            // Don't short-circuit for logical operators - they handle NULL specially
            if !matches!(op, And | Or) {
                return Ok(SqlValue::Null);
            }
        }

        match op {
            // Arithmetic operators
            Plus => ArithmeticOps::add(left, right),
            Minus => ArithmeticOps::subtract(left, right),
            Multiply => ArithmeticOps::multiply(left, right),
            Divide => ArithmeticOps::divide(left, right, sql_mode),
            IntegerDivide => ArithmeticOps::integer_divide(left, right),
            Modulo => ArithmeticOps::modulo(left, right),

            // Comparison operators
            Equal => ComparisonOps::equal(left, right),
            NotEqual => ComparisonOps::not_equal(left, right),
            LessThan => ComparisonOps::less_than(left, right),
            LessThanOrEqual => ComparisonOps::less_than_or_equal(left, right),
            GreaterThan => ComparisonOps::greater_than(left, right),
            GreaterThanOrEqual => ComparisonOps::greater_than_or_equal(left, right),

            // Logical operators
            And => LogicalOps::and(left, right),
            Or => LogicalOps::or(left, right),

            // String operators
            Concat => StringOps::concat(left, right),

            // Bitwise operators
            BitwiseAnd => BitwiseOps::and(left, right),
            BitwiseOr => BitwiseOps::or(left, right),
            LeftShift => BitwiseOps::left_shift(left, right),
            RightShift => BitwiseOps::right_shift(left, right),

            // Vector distance operators (pgvector compatible)
            CosineDistance => VectorOps::cosine_distance(left, right),
            NegativeInnerProduct => VectorOps::negative_inner_product(left, right),
            L2Distance => VectorOps::l2_distance(left, right),

            // JSON operators
            JsonExtract => json_extract(left, right, false),
            JsonExtractText => json_extract(left, right, true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_handling() {
        use vibesql_ast::BinaryOperator::*;

        // NULL + anything = NULL
        assert!(matches!(
            OperatorRegistry::eval_binary_op(
                &SqlValue::Null,
                &Plus,
                &SqlValue::Integer(1),
                vibesql_types::SqlMode::default()
            )
            .unwrap(),
            SqlValue::Null
        ));

        // anything + NULL = NULL
        assert!(matches!(
            OperatorRegistry::eval_binary_op(
                &SqlValue::Integer(1),
                &Plus,
                &SqlValue::Null,
                vibesql_types::SqlMode::default()
            )
            .unwrap(),
            SqlValue::Null
        ));

        // NULL comparison NULL = NULL
        assert!(matches!(
            OperatorRegistry::eval_binary_op(
                &SqlValue::Null,
                &Equal,
                &SqlValue::Null,
                vibesql_types::SqlMode::default()
            )
            .unwrap(),
            SqlValue::Null
        ));
    }
}
