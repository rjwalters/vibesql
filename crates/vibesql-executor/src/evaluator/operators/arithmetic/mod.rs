//! Arithmetic operator implementations
//!
//! Handles: +, -, *, /
//! Supports: Integer, Smallint, Bigint, Float, Real, Double, Numeric types
//! Includes: Type coercion, mixed-type arithmetic, division-by-zero handling
//!
//! SQLite compatibility: NaN results are converted to NULL

mod addition;
mod division;
mod modulo;
mod multiplication;
mod subtraction;

pub use addition::Addition;
pub use division::Division;
pub use modulo::Modulo;
pub use multiplication::Multiplication;
pub use subtraction::Subtraction;
use vibesql_types::SqlValue;

/// Convert NaN floating-point results to NULL for SQLite compatibility.
///
/// SQLite converts NaN (Not a Number) results to NULL. This happens when:
/// - Infinity * 0.0
/// - 0.0 / 0.0
/// - Infinity - Infinity
///
/// Example:
/// ```sql
/// -- SQLite behavior:
/// SELECT (1e300 * 1e300) * 0.0;  -- Returns NULL (not NaN)
/// SELECT coalesce((1e300 * 1e300) * 0.0, 99.0);  -- Returns 99.0
/// ```
#[inline]
pub(super) fn nan_to_null(value: SqlValue) -> SqlValue {
    match &value {
        SqlValue::Float(f) if f.is_nan() => SqlValue::Null,
        SqlValue::Double(f) if f.is_nan() => SqlValue::Null,
        SqlValue::Numeric(f) if f.is_nan() => SqlValue::Null,
        _ => value,
    }
}

use crate::{
    errors::ExecutorError,
    evaluator::casting::{
        boolean_to_i64, is_approximate_numeric, is_exact_numeric, string_to_number, to_f64, to_i64,
    },
};

/// Result of type coercion for arithmetic operations
pub(super) enum CoercedValues {
    ExactNumeric(i64, i64),
    ApproximateNumeric(f64, f64),
    Numeric(f64, f64),
}

/// Helper to convert a value to numeric, with string coercion support
/// Returns (value_as_i64, value_as_f64, is_float)
fn coerce_single_value(value: &SqlValue) -> Option<(i64, f64, bool)> {
    match value {
        SqlValue::Integer(n) => Some((*n, *n as f64, false)),
        SqlValue::Smallint(n) => Some((*n as i64, *n as f64, false)),
        SqlValue::Bigint(n) => Some((*n, *n as f64, false)),
        SqlValue::Unsigned(n) => Some((*n as i64, *n as f64, false)),
        SqlValue::Float(f) => Some((*f as i64, *f as f64, true)),
        SqlValue::Real(f) => Some((*f as i64, *f as f64, true)),
        SqlValue::Double(f) => Some((*f as i64, *f, true)),
        SqlValue::Numeric(f) => Some((*f as i64, *f, true)),
        SqlValue::Boolean(b) => Some((if *b { 1 } else { 0 }, if *b { 1.0 } else { 0.0 }, false)),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let (i, f, is_float) = string_to_number(s);
            Some((i, f, is_float))
        }
        _ => None,
    }
}

/// Helper function to coerce two values to a common numeric type
pub(super) fn coerce_numeric_values(
    left: &SqlValue,
    right: &SqlValue,
    op: &str,
) -> Result<CoercedValues, ExecutorError> {
    use SqlValue::*;

    // Handle Boolean coercion first
    if matches!(left, Boolean(_)) || matches!(right, Boolean(_)) {
        let left_i64 = boolean_to_i64(left).or_else(|| to_i64(left).ok()).ok_or_else(|| {
            ExecutorError::TypeMismatch {
                left: left.clone(),
                op: op.to_string(),
                right: right.clone(),
            }
        })?;

        let right_i64 = boolean_to_i64(right).or_else(|| to_i64(right).ok()).ok_or_else(|| {
            ExecutorError::TypeMismatch {
                left: left.clone(),
                op: op.to_string(),
                right: right.clone(),
            }
        })?;

        return Ok(CoercedValues::ExactNumeric(left_i64, right_i64));
    }

    // Handle string coercion (SQLite compatibility)
    // When either operand is a string, coerce it to a number
    if matches!(left, Varchar(_) | Character(_)) || matches!(right, Varchar(_) | Character(_)) {
        let left_coerced =
            coerce_single_value(left).ok_or_else(|| ExecutorError::TypeMismatch {
                left: left.clone(),
                op: op.to_string(),
                right: right.clone(),
            })?;

        let right_coerced =
            coerce_single_value(right).ok_or_else(|| ExecutorError::TypeMismatch {
                left: left.clone(),
                op: op.to_string(),
                right: right.clone(),
            })?;

        // If either value is a float, use ApproximateNumeric
        if left_coerced.2 || right_coerced.2 {
            return Ok(CoercedValues::ApproximateNumeric(left_coerced.1, right_coerced.1));
        } else {
            return Ok(CoercedValues::ExactNumeric(left_coerced.0, right_coerced.0));
        }
    }

    // Mixed exact numeric types - integer arithmetic returns Integer type in both modes
    // This matches MySQL behavior where integer arithmetic produces integers
    if is_exact_numeric(left) && is_exact_numeric(right) {
        let left_i64 = to_i64(left)?;
        let right_i64 = to_i64(right)?;
        return Ok(CoercedValues::ExactNumeric(left_i64, right_i64));
    }

    // Approximate numeric types - promote to f64
    if is_approximate_numeric(left) && is_approximate_numeric(right) {
        let left_f64 = to_f64(left)?;
        let right_f64 = to_f64(right)?;
        return Ok(CoercedValues::ApproximateNumeric(left_f64, right_f64));
    }

    // Mixed Float/Integer - promote to f64
    if (matches!(left, Float(_) | Real(_) | Double(_))
        && matches!(right, Integer(_) | Smallint(_) | Bigint(_)))
        || (matches!(left, Integer(_) | Smallint(_) | Bigint(_))
            && matches!(right, Float(_) | Real(_) | Double(_)))
    {
        let left_f64 = to_f64(left)?;
        let right_f64 = to_f64(right)?;
        return Ok(CoercedValues::ApproximateNumeric(left_f64, right_f64));
    }

    // NUMERIC with any numeric type - preserve Numeric type
    if (matches!(left, Numeric(_))
        && matches!(
            right,
            Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
        ))
        || (matches!(left, Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_))
            && matches!(right, Numeric(_)))
    {
        let left_f64 = to_f64(left)?;
        let right_f64 = to_f64(right)?;
        return Ok(CoercedValues::Numeric(left_f64, right_f64));
    }

    // Type mismatch
    Err(ExecutorError::TypeMismatch {
        left: left.clone(),
        op: op.to_string(),
        right: right.clone(),
    })
}

pub(crate) struct ArithmeticOps;

impl ArithmeticOps {
    /// Addition operator (+)
    #[inline]
    pub fn add(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Addition::add(left, right)
    }

    /// Subtraction operator (-)
    #[inline]
    pub fn subtract(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Subtraction::subtract(left, right)
    }

    /// Multiplication operator (*)
    #[inline]
    pub fn multiply(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Multiplication::multiply(left, right)
    }

    /// Division operator (/)
    #[inline]
    pub fn divide(
        left: &SqlValue,
        right: &SqlValue,
        sql_mode: vibesql_types::SqlMode,
    ) -> Result<SqlValue, ExecutorError> {
        Division::divide(left, right, sql_mode)
    }

    /// Integer division operator (DIV) - MySQL-specific
    /// Returns integer result, truncating fractional part (truncates toward zero)
    #[inline]
    pub fn integer_divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Division::integer_divide(left, right)
    }

    /// Modulo operator (%)
    /// Returns the remainder of division
    #[inline]
    pub fn modulo(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Modulo::modulo(left, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_addition() {
        let result = ArithmeticOps::add(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(8));
    }

    #[test]
    fn test_integer_subtraction() {
        let result = ArithmeticOps::subtract(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(2));
    }

    #[test]
    fn test_integer_multiplication() {
        let result = ArithmeticOps::multiply(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(15));
    }

    #[test]
    fn test_integer_division_sqlite() {
        // SQLite mode: Division returns Integer for integer operands (truncated)
        let sqlite_mode = vibesql_types::SqlMode::SQLite;
        let result = ArithmeticOps::divide(
            &SqlValue::Integer(15),
            &SqlValue::Integer(3),
            sqlite_mode.clone(),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(5));

        // Test truncation behavior
        let result =
            ArithmeticOps::divide(&SqlValue::Integer(15), &SqlValue::Integer(4), sqlite_mode)
                .unwrap();
        assert_eq!(result, SqlValue::Integer(3)); // 15/4 = 3.75 truncates to 3
    }

    #[test]
    fn test_integer_division_mysql() {
        // MySQL mode: Division returns Numeric for integer operands (exact decimal arithmetic)
        let mysql_mode = vibesql_types::SqlMode::MySQL { flags: Default::default() };
        let result = ArithmeticOps::divide(
            &SqlValue::Integer(15),
            &SqlValue::Integer(3),
            mysql_mode.clone(),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Numeric(5.0));

        let result =
            ArithmeticOps::divide(&SqlValue::Integer(15), &SqlValue::Integer(4), mysql_mode)
                .unwrap();
        assert_eq!(result, SqlValue::Numeric(3.75));
    }

    #[test]
    fn test_division_by_zero() {
        // Division by zero should return NULL (SQL standard behavior)
        let result = ArithmeticOps::divide(
            &SqlValue::Integer(5),
            &SqlValue::Integer(0),
            vibesql_types::SqlMode::default(),
        );
        assert_eq!(result.unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_mixed_exact_numeric() {
        let result = ArithmeticOps::add(&SqlValue::Smallint(5), &SqlValue::Bigint(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(8));
    }

    #[test]
    fn test_float_arithmetic() {
        let result = ArithmeticOps::add(&SqlValue::Float(5.5), &SqlValue::Float(3.2)).unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 8.7).abs() < 0.01),
            _ => panic!("Expected Float result"),
        }
    }

    #[test]
    fn test_mixed_float_integer() {
        let result = ArithmeticOps::add(&SqlValue::Float(5.5), &SqlValue::Integer(3)).unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 8.5).abs() < 0.01),
            _ => panic!("Expected Float result"),
        }
    }

    // Boolean coercion tests
    #[test]
    fn test_boolean_true_addition() {
        // TRUE + 40 = 41
        let result = ArithmeticOps::add(&SqlValue::Boolean(true), &SqlValue::Integer(40)).unwrap();
        assert_eq!(result, SqlValue::Integer(41));
    }

    #[test]
    fn test_boolean_false_addition() {
        // FALSE + 40 = 40
        let result = ArithmeticOps::add(&SqlValue::Boolean(false), &SqlValue::Integer(40)).unwrap();
        assert_eq!(result, SqlValue::Integer(40));
    }

    #[test]
    fn test_integer_plus_boolean() {
        // 40 + TRUE = 41
        let result = ArithmeticOps::add(&SqlValue::Integer(40), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(41));
    }

    #[test]
    fn test_boolean_multiplication() {
        // 97 * TRUE = 97
        let result =
            ArithmeticOps::multiply(&SqlValue::Integer(97), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(97));

        // 97 * FALSE = 0
        let result =
            ArithmeticOps::multiply(&SqlValue::Integer(97), &SqlValue::Boolean(false)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn test_boolean_subtraction() {
        // TRUE - FALSE = 1
        let result =
            ArithmeticOps::subtract(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap();
        assert_eq!(result, SqlValue::Integer(1));

        // 5 - TRUE = 4
        let result =
            ArithmeticOps::subtract(&SqlValue::Integer(5), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(4));
    }

    #[test]
    fn test_boolean_division() {
        // SQLite mode: 10 / TRUE = 10 (integer division)
        let sqlite_mode = vibesql_types::SqlMode::SQLite;
        let result = ArithmeticOps::divide(
            &SqlValue::Integer(10),
            &SqlValue::Boolean(true),
            sqlite_mode.clone(),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(10));

        // TRUE / TRUE = 1
        let result =
            ArithmeticOps::divide(&SqlValue::Boolean(true), &SqlValue::Boolean(true), sqlite_mode)
                .unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }

    #[test]
    fn test_boolean_division_mysql() {
        // MySQL mode: 10 / TRUE = 10.0 (exact decimal division)
        let mysql_mode = vibesql_types::SqlMode::MySQL { flags: Default::default() };
        let result = ArithmeticOps::divide(
            &SqlValue::Integer(10),
            &SqlValue::Boolean(true),
            mysql_mode.clone(),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Numeric(10.0));

        // TRUE / TRUE = 1.0
        let result =
            ArithmeticOps::divide(&SqlValue::Boolean(true), &SqlValue::Boolean(true), mysql_mode)
                .unwrap();
        assert_eq!(result, SqlValue::Numeric(1.0));
    }

    #[test]
    fn test_boolean_division_by_false() {
        // 10 / FALSE should return NULL (SQL standard behavior)
        let result = ArithmeticOps::divide(
            &SqlValue::Integer(10),
            &SqlValue::Boolean(false),
            vibesql_types::SqlMode::default(),
        );
        assert_eq!(result.unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_boolean_modulo() {
        // 10 % TRUE = 0 (10 % 1 = 0)
        let result =
            ArithmeticOps::modulo(&SqlValue::Integer(10), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));

        // TRUE % TRUE = 0 (1 % 1 = 0)
        let result =
            ArithmeticOps::modulo(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn test_boolean_integer_divide() {
        // 10 DIV TRUE = 10
        let result =
            ArithmeticOps::integer_divide(&SqlValue::Integer(10), &SqlValue::Boolean(true))
                .unwrap();
        assert_eq!(result, SqlValue::Integer(10));

        // TRUE DIV TRUE = 1
        let result =
            ArithmeticOps::integer_divide(&SqlValue::Boolean(true), &SqlValue::Boolean(true))
                .unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }

    // Tests for Numeric type preservation
    #[test]
    fn test_numeric_multiply_integer() {
        // Numeric * Integer should preserve Numeric type
        let result =
            ArithmeticOps::multiply(&SqlValue::Numeric(1.0), &SqlValue::Integer(85)).unwrap();
        assert!(matches!(result, SqlValue::Numeric(_)));
        if let SqlValue::Numeric(n) = result {
            assert_eq!(n, 85.0);
        }
    }

    #[test]
    fn test_numeric_add_integer() {
        // Numeric + Integer should preserve Numeric type
        let result = ArithmeticOps::add(&SqlValue::Numeric(10.0), &SqlValue::Integer(5)).unwrap();
        assert!(matches!(result, SqlValue::Numeric(_)));
        if let SqlValue::Numeric(n) = result {
            assert_eq!(n, 15.0);
        }
    }

    #[test]
    fn test_numeric_subtract_integer() {
        // Numeric - Integer should preserve Numeric type
        let result =
            ArithmeticOps::subtract(&SqlValue::Numeric(10.0), &SqlValue::Integer(3)).unwrap();
        assert!(matches!(result, SqlValue::Numeric(_)));
        if let SqlValue::Numeric(n) = result {
            assert_eq!(n, 7.0);
        }
    }

    #[test]
    fn test_numeric_divide_integer() {
        // Numeric / Integer should preserve Numeric type
        let result = ArithmeticOps::divide(
            &SqlValue::Numeric(10.0),
            &SqlValue::Integer(2),
            vibesql_types::SqlMode::default(),
        )
        .unwrap();
        assert!(matches!(result, SqlValue::Numeric(_)));
        if let SqlValue::Numeric(n) = result {
            assert_eq!(n, 5.0);
        }
    }

    #[test]
    fn test_numeric_chain_operations() {
        // Test complex expression: 1.0 * 85 * -28 * 83
        let step1 =
            ArithmeticOps::multiply(&SqlValue::Numeric(1.0), &SqlValue::Integer(85)).unwrap();
        assert!(matches!(step1, SqlValue::Numeric(_)));

        let step2 = ArithmeticOps::multiply(&step1, &SqlValue::Integer(-28)).unwrap();
        assert!(matches!(step2, SqlValue::Numeric(_)));

        let step3 = ArithmeticOps::multiply(&step2, &SqlValue::Integer(83)).unwrap();
        assert!(matches!(step3, SqlValue::Numeric(_)));

        if let SqlValue::Numeric(n) = step3 {
            assert_eq!(n, -197540.0);
        }
    }

    #[test]
    fn test_integer_add_numeric() {
        // Integer + Numeric should preserve Numeric type (commutative)
        let result = ArithmeticOps::add(&SqlValue::Integer(5), &SqlValue::Numeric(10.0)).unwrap();
        assert!(matches!(result, SqlValue::Numeric(_)));
        if let SqlValue::Numeric(n) = result {
            assert_eq!(n, 15.0);
        }
    }

    #[test]
    fn test_numeric_add_numeric() {
        // Numeric + Numeric should preserve Numeric type
        let result = ArithmeticOps::add(&SqlValue::Numeric(10.0), &SqlValue::Numeric(5.0)).unwrap();
        assert!(matches!(result, SqlValue::Numeric(_)));
        if let SqlValue::Numeric(n) = result {
            assert_eq!(n, 15.0);
        }
    }

    #[test]
    fn test_numeric_modulo_integer() {
        // Numeric % Integer should preserve Numeric type
        let result =
            ArithmeticOps::modulo(&SqlValue::Numeric(10.0), &SqlValue::Integer(3)).unwrap();
        assert!(matches!(result, SqlValue::Numeric(_)));
        if let SqlValue::Numeric(n) = result {
            assert_eq!(n, 1.0);
        }
    }

    // SQL Mode Tests
    #[test]
    fn test_sql_mode_standard_integer_arithmetic() {
        // Standard mode: Integer + Integer → Integer
        let result = ArithmeticOps::add(&SqlValue::Integer(1), &SqlValue::Integer(2)).unwrap();
        assert_eq!(result, SqlValue::Integer(3));

        // Standard mode: Integer - Integer → Integer
        let result =
            ArithmeticOps::subtract(&SqlValue::Integer(91), &SqlValue::Integer(0)).unwrap();
        assert_eq!(result, SqlValue::Integer(91));

        // Standard mode: Integer * Integer → Integer
        let result = ArithmeticOps::multiply(&SqlValue::Integer(5), &SqlValue::Integer(7)).unwrap();
        assert_eq!(result, SqlValue::Integer(35));
    }

    #[test]
    fn test_sql_mode_mysql_integer_arithmetic() {
        // MySQL mode: Integer + Integer → Integer (matches actual MySQL behavior)
        let result = ArithmeticOps::add(&SqlValue::Integer(1), &SqlValue::Integer(2)).unwrap();
        assert_eq!(result, SqlValue::Integer(3));

        // MySQL mode: Integer - Integer → Integer (unary negation case)
        let result =
            ArithmeticOps::subtract(&SqlValue::Integer(0), &SqlValue::Integer(-91)).unwrap();
        assert_eq!(result, SqlValue::Integer(91));

        // MySQL mode: Integer * Integer → Integer
        let result = ArithmeticOps::multiply(&SqlValue::Integer(5), &SqlValue::Integer(7)).unwrap();
        assert_eq!(result, SqlValue::Integer(35));
    }

    #[test]
    fn test_sql_mode_comparison() {
        // Both modes now return Integer for integer arithmetic
        let int1 = SqlValue::Integer(100);
        let int2 = SqlValue::Integer(50);

        // Standard mode returns Integer
        let standard_result = ArithmeticOps::add(&int1, &int2).unwrap();
        assert_eq!(standard_result, SqlValue::Integer(150));

        // MySQL mode also returns Integer (matches actual MySQL behavior)
        let mysql_result = ArithmeticOps::add(&int1, &int2).unwrap();
        assert_eq!(mysql_result, SqlValue::Integer(150));
    }

    // NULL propagation tests for issue #1728
    #[test]
    fn test_null_addition() {
        assert_eq!(
            ArithmeticOps::add(&SqlValue::Null, &SqlValue::Integer(23)).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            ArithmeticOps::add(&SqlValue::Integer(23), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
        assert_eq!(ArithmeticOps::add(&SqlValue::Null, &SqlValue::Null).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_null_subtraction() {
        assert_eq!(
            ArithmeticOps::subtract(&SqlValue::Null, &SqlValue::Integer(23)).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            ArithmeticOps::subtract(&SqlValue::Integer(23), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_null_multiplication() {
        assert_eq!(
            ArithmeticOps::multiply(&SqlValue::Null, &SqlValue::Integer(23)).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            ArithmeticOps::multiply(&SqlValue::Integer(23), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_null_division() {
        assert_eq!(
            ArithmeticOps::divide(
                &SqlValue::Null,
                &SqlValue::Integer(23),
                vibesql_types::SqlMode::default()
            )
            .unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            ArithmeticOps::divide(
                &SqlValue::Integer(23),
                &SqlValue::Null,
                vibesql_types::SqlMode::default()
            )
            .unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_null_modulo() {
        assert_eq!(
            ArithmeticOps::modulo(&SqlValue::Null, &SqlValue::Integer(23)).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            ArithmeticOps::modulo(&SqlValue::Integer(23), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_cast_null_arithmetic() {
        // Simulate CAST(NULL AS SIGNED) + 23
        let null_bigint = SqlValue::Null; // Result of CAST(NULL AS SIGNED)
        let result = ArithmeticOps::add(&null_bigint, &SqlValue::Integer(23)).unwrap();
        assert_eq!(result, SqlValue::Null);

        // Test chained operations with NULL
        let step1 = ArithmeticOps::multiply(&SqlValue::Null, &SqlValue::Integer(100)).unwrap();
        assert_eq!(step1, SqlValue::Null);

        let step2 = ArithmeticOps::add(&step1, &SqlValue::Integer(-23)).unwrap();
        assert_eq!(step2, SqlValue::Null);
    }

    // String-to-number coercion tests (SQLite compatibility)
    #[test]
    fn test_string_integer_addition() {
        // '123' + 1 = 124
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("123")),
            &SqlValue::Integer(1),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(124));
    }

    #[test]
    fn test_integer_string_addition() {
        // 1 + '2' = 3
        let result = ArithmeticOps::add(
            &SqlValue::Integer(1),
            &SqlValue::Varchar(arcstr::ArcStr::from("2")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn test_string_float_addition() {
        // 1 + '2.5' = 3.5
        let result = ArithmeticOps::add(
            &SqlValue::Integer(1),
            &SqlValue::Varchar(arcstr::ArcStr::from("2.5")),
        )
        .unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 3.5).abs() < 0.01),
            _ => panic!("Expected Float result, got {:?}", result),
        }
    }

    #[test]
    fn test_non_numeric_string() {
        // 'abc' + 1 = 1 (non-numeric string converts to 0)
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("abc")),
            &SqlValue::Integer(1),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }

    #[test]
    fn test_whitespace_string() {
        // '  42  ' + 0 = 42 (whitespace trimmed)
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("  42  ")),
            &SqlValue::Integer(0),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_numeric_prefix_string() {
        // '3.14abc' + 0 = 3.14 (numeric prefix extracted)
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("3.15abc")),
            &SqlValue::Integer(0),
        )
        .unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 3.15).abs() < 0.01),
            _ => panic!("Expected Float result, got {:?}", result),
        }
    }

    #[test]
    fn test_string_multiplication() {
        // '3' * 4 = 12
        let result = ArithmeticOps::multiply(
            &SqlValue::Varchar(arcstr::ArcStr::from("3")),
            &SqlValue::Integer(4),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(12));
    }

    #[test]
    fn test_string_subtraction() {
        // '10' - 3 = 7
        let result = ArithmeticOps::subtract(
            &SqlValue::Varchar(arcstr::ArcStr::from("10")),
            &SqlValue::Integer(3),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(7));
    }

    #[test]
    fn test_string_division() {
        // '20' / 4 = 5 (SQLite mode)
        let result = ArithmeticOps::divide(
            &SqlValue::Varchar(arcstr::ArcStr::from("20")),
            &SqlValue::Integer(4),
            vibesql_types::SqlMode::SQLite,
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(5));
    }

    #[test]
    fn test_string_modulo() {
        // '17' % 5 = 2
        let result = ArithmeticOps::modulo(
            &SqlValue::Varchar(arcstr::ArcStr::from("17")),
            &SqlValue::Integer(5),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(2));
    }

    #[test]
    fn test_empty_string() {
        // '' + 1 = 1 (empty string converts to 0)
        let result =
            ArithmeticOps::add(&SqlValue::Varchar(arcstr::ArcStr::from("")), &SqlValue::Integer(1))
                .unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }

    #[test]
    fn test_negative_string() {
        // '-5' + 10 = 5
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("-5")),
            &SqlValue::Integer(10),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(5));
    }

    #[test]
    fn test_string_with_exponent() {
        // '1e2' + 0 = 100.0
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("1e2")),
            &SqlValue::Integer(0),
        )
        .unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 100.0).abs() < 0.01),
            _ => panic!("Expected Float result, got {:?}", result),
        }
    }

    #[test]
    fn test_string_string_addition() {
        // '10' + '5' = 15 (both strings coerced)
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("10")),
            &SqlValue::Varchar(arcstr::ArcStr::from("5")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Integer(15));
    }

    #[test]
    fn test_float_string_addition() {
        // '1.5' + 0.5 = 2.0
        let result = ArithmeticOps::add(
            &SqlValue::Varchar(arcstr::ArcStr::from("1.5")),
            &SqlValue::Float(0.5),
        )
        .unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 2.0).abs() < 0.01),
            _ => panic!("Expected Float result, got {:?}", result),
        }
    }
}
