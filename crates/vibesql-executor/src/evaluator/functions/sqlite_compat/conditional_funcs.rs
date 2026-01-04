//! Conditional functions
//!
//! This module contains SQLite-compatible conditional functions:
//! - IIF(condition, true_val, false_val) - Inline if (ternary)
//! - IFNULL(x, y) - Return y if x is NULL

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// IIF(condition, true_value, false_value) - Inline if (SQLite ternary)
///
/// Equivalent to CASE WHEN condition THEN true_value ELSE false_value END
/// Also equivalent to IF(condition, true_value, false_value)
pub(crate) fn iif(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "IIF requires exactly 3 arguments, got {}",
            args.len()
        )));
    }

    // SQLite's IIF treats any non-zero, non-NULL value as true
    let condition = &args[0];
    let is_true = match condition {
        SqlValue::Null => false,
        SqlValue::Boolean(b) => *b,
        SqlValue::Integer(i) => *i != 0,
        SqlValue::Bigint(i) => *i != 0,
        SqlValue::Smallint(i) => *i != 0,
        SqlValue::Unsigned(u) => *u != 0,
        SqlValue::Real(r) => *r != 0.0,
        SqlValue::Double(d) => *d != 0.0,
        SqlValue::Numeric(n) => *n != 0.0,
        SqlValue::Float(f) => *f != 0.0,
        // Non-empty strings are truthy in SQLite
        SqlValue::Varchar(s) | SqlValue::Character(s) => !s.is_empty(),
        _ => true, // Other non-null values are truthy
    };

    if is_true {
        Ok(args[1].clone())
    } else {
        Ok(args[2].clone())
    }
}

/// IFNULL(x, y) - Return y if x is NULL, otherwise return x
///
/// This is an alias for COALESCE(x, y) with exactly 2 arguments.
pub(crate) fn ifnull(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "IFNULL requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    if matches!(args[0], SqlValue::Null) {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iif() {
        // True condition
        assert_eq!(
            iif(&[SqlValue::Boolean(true), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );

        // False condition
        assert_eq!(
            iif(&[SqlValue::Boolean(false), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );

        // NULL condition (treated as false)
        assert_eq!(
            iif(&[SqlValue::Null, SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );

        // Non-zero integer is truthy
        assert_eq!(
            iif(&[SqlValue::Integer(5), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );

        // Zero is falsy
        assert_eq!(
            iif(&[SqlValue::Integer(0), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_ifnull() {
        assert_eq!(
            ifnull(&[SqlValue::Null, SqlValue::Integer(42)]).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            ifnull(&[SqlValue::Integer(1), SqlValue::Integer(42)]).unwrap(),
            SqlValue::Integer(1)
        );
    }
}
