//! Operator evaluation
//!
//! This module implements evaluation of operators including unary operators (+, -)

use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    evaluator::casting::{string_to_number, to_i64},
    evaluator::operators::is_truthy_string,
};

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

        // Unary plus on BLOB - identity operation (SQLite behavior)
        // SQLite: SELECT typeof(+x'616263') → 'blob'; SELECT quote(+x'616263') → X'616263'
        (Plus, SqlValue::Blob(b)) => Ok(SqlValue::Blob(b.clone())),

        // Unary minus on text types - convert to number first (SQLite behavior)
        // SQLite converts strings to numbers before applying unary minus:
        // - Numeric strings: '-123' → Integer(-123), '-3.14' → Real(-3.14)
        // - Non-numeric strings: '.' → 0, 'abc' → 0
        (Minus, SqlValue::Character(s)) | (Minus, SqlValue::Varchar(s)) => {
            let (int_val, float_val, is_float) = string_to_number(s);
            if is_float {
                Ok(SqlValue::Real(-float_val))
            } else {
                Ok(SqlValue::Integer(-int_val))
            }
        }

        // Unary minus on BLOB - SQLite coerces BLOB bytes → text → number, then negates.
        // Mirrors the TEXT/Varchar branch above. Examples:
        //   -x'313233' → -123   (bytes "123" → 123 → -123)
        //   -x'2E35'   → -0.5   (bytes ".5"  → 0.5 → -0.5)
        //   -x'616263' → 0      (bytes "abc" → 0)
        //   -x''       → 0      (empty blob)
        // SQLite rule: -x'<bytes>' === -CAST(x'<bytes>' AS TEXT)
        (Minus, SqlValue::Blob(b)) => {
            let s = String::from_utf8_lossy(b);
            let (int_val, float_val, is_float) = string_to_number(&s);
            if is_float {
                Ok(SqlValue::Real(-float_val))
            } else {
                Ok(SqlValue::Integer(-int_val))
            }
        }

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
        // SQLite parses the leading numeric portion of strings for boolean context
        // e.g., '0x' → 0 (falsey), '1x' → 1 (truthy), 'abc' → 0 (falsey)
        (Not, SqlValue::Character(s)) | (Not, SqlValue::Varchar(s)) => {
            Ok(SqlValue::Boolean(!is_truthy_string(s)))
        }
        (Not, SqlValue::Date(_)) => Ok(SqlValue::Boolean(false)), // Date values are truthy
        (Not, SqlValue::Time(_)) => Ok(SqlValue::Boolean(false)), // Time values are truthy
        (Not, SqlValue::Timestamp(_)) => Ok(SqlValue::Boolean(false)), /* Timestamp values are */
        // truthy
        (Not, SqlValue::Interval(_)) => Ok(SqlValue::Boolean(false)), // Interval values are truthy

        // Bitwise NOT (~) - converts to integer and flips all bits
        (BitwiseNot, SqlValue::Null) => Ok(SqlValue::Null),
        (BitwiseNot, val) => Ok(SqlValue::Integer(!to_i64(val)?)),

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
        // SQLite parses leading numeric for string truthiness
        // 'hello' → 0 (no leading numeric) → falsey → NOT returns true
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Varchar(arcstr::ArcStr::from("hello")))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // Empty string → 0 → falsey → NOT returns true
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Varchar(arcstr::ArcStr::from("")))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // '0x' → 0 (leading 0) → falsey → NOT returns true
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Varchar(arcstr::ArcStr::from("0x")))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // '1x' → 1 (leading 1) → truthy → NOT returns false
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Varchar(arcstr::ArcStr::from("1x")))
                .unwrap(),
            SqlValue::Boolean(false)
        );
        // '42abc' → 42 → truthy → NOT returns false
        assert_eq!(
            eval_unary_op(&UnaryOperator::Not, &SqlValue::Varchar(arcstr::ArcStr::from("42abc")))
                .unwrap(),
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
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Varchar(arcstr::ArcStr::from("hello")))
                .unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("hello"))
        );
        assert_eq!(
            eval_unary_op(
                &UnaryOperator::Plus,
                &SqlValue::Character(arcstr::ArcStr::from("world"))
            )
            .unwrap(),
            SqlValue::Character(arcstr::ArcStr::from("world"))
        );
        // Empty strings
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Varchar(arcstr::ArcStr::from("")))
                .unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from(""))
        );
    }

    #[test]
    fn test_unary_minus_on_text() {
        // SQLite behavior: unary - on text converts to number first, then negates
        // Non-numeric strings become 0
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Varchar(arcstr::ArcStr::from(".")))
                .unwrap(),
            SqlValue::Integer(0)
        );
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Varchar(arcstr::ArcStr::from("abc")))
                .unwrap(),
            SqlValue::Integer(0)
        );
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Varchar(arcstr::ArcStr::from("")))
                .unwrap(),
            SqlValue::Integer(0)
        );

        // Numeric strings are converted and negated
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Varchar(arcstr::ArcStr::from("123")))
                .unwrap(),
            SqlValue::Integer(-123)
        );
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Varchar(arcstr::ArcStr::from("  5  ")))
                .unwrap(),
            SqlValue::Integer(-5)
        );

        // Float strings
        let result =
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Varchar(arcstr::ArcStr::from("3.14")))
                .unwrap();
        match result {
            SqlValue::Real(v) => assert!((v - (-3.14)).abs() < 0.001, "Expected -3.14, got {}", v),
            other => panic!("Expected Real, got {:?}", other),
        }

        // Character type works the same
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Character(arcstr::ArcStr::from("42")))
                .unwrap(),
            SqlValue::Integer(-42)
        );
    }

    #[test]
    fn test_unary_plus_on_blob() {
        // SQLite behavior: unary + on BLOB returns the BLOB unchanged (identity)
        // Verified: SELECT typeof(+x'616263') → 'blob'; SELECT quote(+x'616263') → X'616263'
        let bytes = vec![0x61u8, 0x62, 0x63];
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Blob(bytes.clone())).unwrap(),
            SqlValue::Blob(bytes)
        );
        // Empty BLOB is preserved
        assert_eq!(
            eval_unary_op(&UnaryOperator::Plus, &SqlValue::Blob(vec![])).unwrap(),
            SqlValue::Blob(vec![])
        );
    }

    #[test]
    fn test_unary_minus_on_blob() {
        // SQLite behavior: unary - on a non-numeric BLOB coerces to 0 then negates → Integer(0)
        // Verified: SELECT typeof(-x'616263') → 'integer'; SELECT -x'616263' → 0
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Blob(vec![0x61, 0x62, 0x63])).unwrap(),
            SqlValue::Integer(0)
        );
        assert_eq!(
            eval_unary_op(&UnaryOperator::Minus, &SqlValue::Blob(vec![])).unwrap(),
            SqlValue::Integer(0)
        );
    }

    // Issue #5098: Unary minus on BLOB must coerce numeric-string bytes via
    // String::from_utf8_lossy + string_to_number, mirroring SQLite's rule:
    //   -x'<bytes>' === -CAST(x'<bytes>' AS TEXT)

    #[test]
    fn unary_minus_on_numeric_blob_returns_integer() {
        // x'313233' → bytes "123" → 123 → -123
        let blob = SqlValue::Blob(b"123".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Integer(-123));
    }

    #[test]
    fn unary_minus_on_decimal_blob_returns_real() {
        // x'2E35' → bytes ".5" → 0.5 → -0.5
        let blob = SqlValue::Blob(b".5".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Real(-0.5));
    }

    #[test]
    fn unary_minus_on_exponent_blob_returns_real() {
        // x'31653530' → bytes "1e50" → 1e50 → -1e50
        let blob = SqlValue::Blob(b"1e50".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        match result {
            SqlValue::Real(f) => {
                assert!(
                    (f + 1e50).abs() / 1e50 < 1e-12,
                    "Expected ~-1e50, got {}",
                    f
                );
            }
            other => panic!("Expected Real, got {:?}", other),
        }
    }

    #[test]
    fn unary_minus_on_non_numeric_blob_returns_integer_zero() {
        // x'616263' → bytes "abc" → 0 → 0 (no regression on PR #5090's like3 fixes)
        let blob = SqlValue::Blob(b"abc".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn unary_minus_on_empty_blob_returns_integer_zero() {
        // x'' → empty bytes → 0 → 0
        let blob = SqlValue::Blob(vec![]);
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn unary_minus_on_blob_extracts_numeric_prefix() {
        // x'31323378' → bytes "123x" → prefix "123" → 123 → -123
        let blob = SqlValue::Blob(b"123abc".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Integer(-123));
    }

    // Edge cases requested in the issue Test Plan:

    #[test]
    fn unary_minus_on_blob_with_leading_whitespace() {
        // x'20313233' → bytes " 123" → 123 → -123 (string_to_number handles leading whitespace)
        let blob = SqlValue::Blob(b" 123".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Integer(-123));
    }

    #[test]
    fn unary_minus_on_blob_with_negative_prefix() {
        // x'2D313233' → bytes "-123" → -123, then unary minus → +123
        let blob = SqlValue::Blob(b"-123".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Integer(123));
    }

    #[test]
    fn unary_minus_on_blob_with_invalid_utf8() {
        // x'FF' → invalid UTF-8 byte; from_utf8_lossy substitutes U+FFFD,
        // which is non-numeric, so the result is Integer(0).
        let blob = SqlValue::Blob(vec![0xFFu8]);
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn unary_minus_on_blob_with_negative_decimal() {
        // x'2D2E35' → bytes "-.5" → -0.5, then unary minus → +0.5
        let blob = SqlValue::Blob(b"-.5".to_vec());
        let result = eval_unary_op(&UnaryOperator::Minus, &blob).unwrap();
        assert_eq!(result, SqlValue::Real(0.5));
    }
}
