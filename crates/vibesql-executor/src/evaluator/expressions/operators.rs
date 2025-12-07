//! Operator evaluation
//!
//! This module implements evaluation of operators including unary operators (+, -)

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Evaluate a unary operation
///
/// This function is shared by both ExpressionEvaluator and CombinedExpressionEvaluator
/// to avoid code duplication.
pub(crate) fn eval_unary_op(
    op: &vibesql_ast::UnaryOperator,
    val: &SqlValue,
) -> Result<SqlValue, ExecutorError> {
    use vibesql_ast::UnaryOperator::*;

    match (op, val) {
        // Unary plus - identity operation (preserves type in both modes)
        (Plus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(*n)),
        (Plus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(*n)),
        (Plus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(*n)),
        (Plus, SqlValue::Float(n)) => Ok(SqlValue::Float(*n)),
        (Plus, SqlValue::Real(n)) => Ok(SqlValue::Real(*n)),
        (Plus, SqlValue::Double(n)) => Ok(SqlValue::Double(*n)),
        (Plus, SqlValue::Numeric(s)) => Ok(SqlValue::Numeric(*s)),

        // Unary minus - negation (preserves type in both modes)
        (Minus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(-n)),
        (Minus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(-n)),
        (Minus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(-n)),
        (Minus, SqlValue::Float(n)) => Ok(SqlValue::Float(-n)),
        (Minus, SqlValue::Real(n)) => Ok(SqlValue::Real(-n)),
        (Minus, SqlValue::Double(n)) => Ok(SqlValue::Double(-n)),
        (Minus, SqlValue::Numeric(f)) => Ok(SqlValue::Numeric(-*f)),

        // NULL propagation - unary operations on NULL return NULL
        (Plus | Minus, SqlValue::Null) => Ok(SqlValue::Null),

        // Unary plus on text types - identity operation (SQLite behavior)
        // In SQLite, unary + on text returns the text unchanged
        (Plus, SqlValue::Character(s)) => Ok(SqlValue::Character(s.clone())),
        (Plus, SqlValue::Varchar(s)) => Ok(SqlValue::Varchar(s.clone())),

        // Unary NOT - logical negation
        // Per SQL standard three-valued logic:
        // - NOT NULL returns NULL
        // - NOT TRUE returns FALSE
        // - NOT FALSE returns TRUE
        // - NOT non-boolean values are coerced to boolean first
        (Not, SqlValue::Null) => Ok(SqlValue::Null), // NULL propagation for NOT
        (Not, SqlValue::Boolean(b)) => Ok(SqlValue::Boolean(!b)),

        // For non-boolean values, coerce to boolean first
        // In SQL, any non-zero number is TRUE, zero is FALSE
        (Not, SqlValue::Integer(n)) => Ok(SqlValue::Boolean(*n == 0)),
        (Not, SqlValue::Smallint(n)) => Ok(SqlValue::Boolean(*n == 0)),
        (Not, SqlValue::Bigint(n)) => Ok(SqlValue::Boolean(*n == 0)),
        (Not, SqlValue::Unsigned(n)) => Ok(SqlValue::Boolean(*n == 0)),
        (Not, SqlValue::Float(f)) => Ok(SqlValue::Boolean(!(*f != 0.0))),
        (Not, SqlValue::Real(f)) => Ok(SqlValue::Boolean(!(*f != 0.0))),
        (Not, SqlValue::Double(f)) => Ok(SqlValue::Boolean(!(*f != 0.0))),
        (Not, SqlValue::Numeric(d)) => Ok(SqlValue::Boolean(!(*d != 0.0))),
        (Not, SqlValue::Character(_)) => Ok(SqlValue::Boolean(false)), /* Non-empty character is */
        // truthy, so NOT is
        // false
        (Not, SqlValue::Varchar(_)) => Ok(SqlValue::Boolean(false)), /* Non-empty varchar is truthy, so NOT is false */
        (Not, SqlValue::Date(_)) => Ok(SqlValue::Boolean(false)),    // Date values are truthy
        (Not, SqlValue::Time(_)) => Ok(SqlValue::Boolean(false)),    // Time values are truthy
        (Not, SqlValue::Timestamp(_)) => Ok(SqlValue::Boolean(false)), /* Timestamp values are */
        // truthy
        (Not, SqlValue::Interval(_)) => Ok(SqlValue::Boolean(false)), // Interval values are truthy

        // Type errors
        (Plus, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "unary +".to_string(),
            right: SqlValue::Null,
        }),
        (Minus, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "unary -".to_string(),
            right: SqlValue::Null,
        }),

        // Other unary operators are handled elsewhere
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unary operator {:?} not supported in this context",
            op
        ))),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::UnaryOperator;

    use super::*;

    #[test]
    fn test_not_boolean() {
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_not_null() {
        assert_eq!(eval_unary_op(&UnaryOperator::Not, &SqlValue::Null).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_not_integer() {
        // Non-zero values are true, so NOT should return false
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Integer(77)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Integer(-4931)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Zero is false, so NOT should return true
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_not_float() {
        // Non-zero values are true, so NOT should return false
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Float(3.5)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Zero is false, so NOT should return true
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_not_varchar() {
        // Non-empty varchar is truthy, so NOT should return false
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Varchar(arcstr::ArcStr::from("hello"))).unwrap(),
            SqlValue::Boolean(false)
        );
        // Empty varchar is also considered truthy in this implementation
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Varchar(arcstr::ArcStr::from(""))).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_not_numeric() {
        // Non-zero values are true, so NOT should return false
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Numeric(3.5)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Zero is false, so NOT should return true
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Numeric(0.0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_unary_plus_standard_mode() {
        // Standard mode: Integer stays Integer
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Integer(42)).unwrap(),
            SqlValue::Integer(42)
        );
    }

    #[test]
    fn test_unary_plus_mysql_mode() {
        // MySQL mode: Integer stays Integer (matches actual MySQL behavior)
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Integer(42)).unwrap(),
            SqlValue::Integer(42)
        );
    }

    #[test]
    fn test_unary_minus_standard_mode() {
        // Standard mode: Integer stays Integer
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Integer(42)).unwrap(),
            SqlValue::Integer(-42)
        );
    }

    #[test]
    fn test_unary_minus_mysql_mode() {
        // MySQL mode: Integer stays Integer (matches actual MySQL behavior)
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Integer(42)).unwrap(),
            SqlValue::Integer(-42)
        );
    }

    #[test]
    fn test_unary_plus_on_text() {
        // SQLite behavior: unary + on text returns text unchanged (identity operation)
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Varchar(arcstr::ArcStr::from("hello"))).unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("hello"))
        );
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Character(arcstr::ArcStr::from("world"))).unwrap(),
            SqlValue::Character(arcstr::ArcStr::from("world"))
        );
        // Empty strings
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Varchar(arcstr::ArcStr::from(""))).unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from(""))
        );
    }
}
