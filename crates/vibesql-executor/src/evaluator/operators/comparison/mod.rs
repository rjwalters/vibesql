//! Comparison operator implementations
//!
//! Handles: =, <>, <, <=, >, >=
//! Supports: All SQL types with proper type coercion
//! Includes: NULL handling (three-valued logic), cross-type comparisons

pub mod equality;
pub mod ordering;

use std::str::FromStr;

use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    evaluator::casting::{is_approximate_numeric, is_exact_numeric, to_f64, to_i64},
};

/// Public API for comparison operations
pub(crate) struct ComparisonOps;

impl ComparisonOps {
    /// Equality operator (=)
    #[inline]
    pub fn equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        equality::equal(left, right)
    }

    /// Inequality operator (<>)
    #[inline]
    pub fn not_equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        equality::not_equal(left, right)
    }

    /// Less than operator (<)
    #[inline]
    pub fn less_than(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        ordering::less_than(left, right)
    }

    /// Less than or equal operator (<=)
    #[inline]
    pub fn less_than_or_equal(
        left: &SqlValue,
        right: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        ordering::less_than_or_equal(left, right)
    }

    /// Greater than operator (>)
    #[inline]
    pub fn greater_than(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        ordering::greater_than(left, right)
    }

    /// Greater than or equal operator (>=)
    #[inline]
    pub fn greater_than_or_equal(
        left: &SqlValue,
        right: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        ordering::greater_than_or_equal(left, right)
    }
}

/// Generic comparison helper used by all comparison operators
#[inline]
pub(crate) fn compare<F>(
    left: &SqlValue,
    right: &SqlValue,
    predicate: F,
    op_str: &str,
) -> Result<SqlValue, ExecutorError>
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    use SqlValue::*;

    // NULL handling - SQL three-valued logic
    // Any comparison with NULL returns NULL
    if matches!(left, Null) || matches!(right, Null) {
        return Ok(Null);
    }

    // Boolean coercion for comparisons
    //
    // SQLite has no BOOLEAN storage class: EXISTS/IN/comparison results are
    // the integers 0/1. When a Boolean operand meets a non-Boolean operand
    // (numeric, text, blob, ...), normalize the Boolean to Integer 0/1 and
    // re-dispatch so the regular cross-type logic below applies (numeric
    // coercion, SQLite type ordering: numeric < text < blob).
    //
    // Examples (matching SQLite):
    //   EXISTS(SELECT 1) == 'experiments' → 1 = 'experiments' → 0
    //   EXISTS(SELECT 1) <  'a'           → 1 < 'a'           → 1
    //   1 IN (SELECT 1)  == 2             → 1 = 2             → 0
    match (left, right) {
        (Boolean(b), other) if !matches!(other, Boolean(_)) => {
            return compare(&Integer(i64::from(*b)), other, predicate, op_str);
        }
        (other, Boolean(b)) if !matches!(other, Boolean(_)) => {
            return compare(other, &Integer(i64::from(*b)), predicate, op_str);
        }
        _ => {} // Fall through to existing comparison logic
    }

    // String-to-date implicit conversion
    // Handle: DATE compared to VARCHAR/CHARACTER
    // Allows: WHERE date_column <= '1998-09-01'
    match (left, right) {
        // Date compared to Varchar - parse varchar as date
        (Date(date_val), Varchar(s)) => match vibesql_types::Date::from_str(s) {
            Ok(parsed_date) => {
                return Ok(Boolean(predicate(date_val.cmp(&parsed_date))));
            }
            Err(_) => {
                return Err(ExecutorError::TypeMismatch {
                    left: left.clone(),
                    op: op_str.to_string(),
                    right: right.clone(),
                });
            }
        },

        // Varchar compared to Date - parse varchar as date (symmetric case)
        (Varchar(s), Date(date_val)) => match vibesql_types::Date::from_str(s) {
            Ok(parsed_date) => {
                return Ok(Boolean(predicate(parsed_date.cmp(date_val))));
            }
            Err(_) => {
                return Err(ExecutorError::TypeMismatch {
                    left: left.clone(),
                    op: op_str.to_string(),
                    right: right.clone(),
                });
            }
        },

        // Date compared to Character - parse character as date
        (Date(date_val), Character(s)) => match vibesql_types::Date::from_str(s) {
            Ok(parsed_date) => {
                return Ok(Boolean(predicate(date_val.cmp(&parsed_date))));
            }
            Err(_) => {
                return Err(ExecutorError::TypeMismatch {
                    left: left.clone(),
                    op: op_str.to_string(),
                    right: right.clone(),
                });
            }
        },

        // Character compared to Date - parse character as date (symmetric case)
        (Character(s), Date(date_val)) => match vibesql_types::Date::from_str(s) {
            Ok(parsed_date) => {
                return Ok(Boolean(predicate(parsed_date.cmp(date_val))));
            }
            Err(_) => {
                return Err(ExecutorError::TypeMismatch {
                    left: left.clone(),
                    op: op_str.to_string(),
                    right: right.clone(),
                });
            }
        },

        // Timestamp compared to string - always compare the TEXT renderings
        // lexicographically. SQLite's datetime() returns TEXT, so expressions
        // like `datetime(x,'auto') == '2022-01-27 13:15:44'` (date3.test 2.40)
        // and `datetime(b) BETWEEN '2017-07-04' AND '2017-07-08'` (date2-331)
        // are plain text comparisons there. The Display rendering of Timestamp
        // matches SQLite's 'YYYY-MM-DD HH:MM:SS' format, so for full canonical
        // timestamp strings lexicographic ordering equals semantic ordering
        // (ISO-8601); behavior only diverges for date-only strings, where
        // SQLite is lexicographic: '2017-07-08 00:00:00' > '2017-07-08'
        // (longer string with equal prefix). Unparseable strings (e.g.
        // 'hello') also compare as text instead of raising a type mismatch,
        // like SQLite.
        (Timestamp(ts), Varchar(s)) | (Timestamp(ts), Character(s)) => {
            return Ok(Boolean(predicate(ts.to_string().as_str().cmp(s.as_str()))));
        }
        (Varchar(s), Timestamp(ts)) | (Character(s), Timestamp(ts)) => {
            return Ok(Boolean(predicate(s.as_str().cmp(ts.to_string().as_str()))));
        }

        // Time compared to string - same TEXT-rendering approach as Timestamp
        // (Time's Display matches SQLite's time() 'HH:MM:SS' output)
        (Time(t), Varchar(s)) | (Time(t), Character(s)) => {
            return Ok(Boolean(predicate(t.to_string().as_str().cmp(s.as_str()))));
        }
        (Varchar(s), Time(t)) | (Character(s), Time(t)) => {
            return Ok(Boolean(predicate(s.as_str().cmp(t.to_string().as_str()))));
        }

        _ => {} // Fall through to regular comparison logic
    }

    // SQLite type ordering for comparison:
    // When comparing values of different storage classes, SQLite uses type ordering:
    // NULL < INTEGER/REAL < TEXT < BLOB
    //
    // Type coercion (converting TEXT '99' to INTEGER 99) only happens when:
    // 1. One operand has NUMERIC/INTEGER/REAL affinity AND
    // 2. The other operand has NONE or BLOB affinity
    //
    // Since we can't distinguish expression affinity at the SqlValue level,
    // we use strict type ordering here. Affinity-based coercion should be
    // handled at a higher level (expression evaluation) where column types are known.
    //
    // This matches SQLite's behavior for the whereB.test cases where:
    // - INTEGER 99 compared to TEXT '99' returns NOT EQUAL
    // - Neither side has NUMERIC affinity, so no type conversion occurs

    let is_string = |v: &SqlValue| matches!(v, Varchar(_) | Character(_));
    let is_numeric = |v: &SqlValue| {
        matches!(
            v,
            Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
        )
    };
    let is_blob = |v: &SqlValue| matches!(v, Blob(_));

    // SQLite type ordering: INTEGER/REAL < TEXT
    // No coercion - different storage classes are NOT equal
    if is_numeric(left) && is_string(right) {
        // Numeric < TEXT in SQLite type ordering
        return Ok(Boolean(predicate(std::cmp::Ordering::Less)));
    }

    if is_string(left) && is_numeric(right) {
        // TEXT > Numeric in SQLite type ordering
        return Ok(Boolean(predicate(std::cmp::Ordering::Greater)));
    }

    // SQLite type ordering: NULL < INTEGER/REAL < TEXT < BLOB
    // BLOB is greater than any non-BLOB storage class.
    // Verified: SELECT 'abc' >= x'6162' → 0, SELECT x'616263' >= 'abc' → 1
    if is_string(left) && is_blob(right) {
        // TEXT < BLOB
        return Ok(Boolean(predicate(std::cmp::Ordering::Less)));
    }
    if is_blob(left) && is_string(right) {
        // BLOB > TEXT
        return Ok(Boolean(predicate(std::cmp::Ordering::Greater)));
    }
    if is_numeric(left) && is_blob(right) {
        // Numeric < BLOB
        return Ok(Boolean(predicate(std::cmp::Ordering::Less)));
    }
    if is_blob(left) && is_numeric(right) {
        // BLOB > Numeric
        return Ok(Boolean(predicate(std::cmp::Ordering::Greater)));
    }

    match (left, right) {
        // BLOB vs BLOB - bytewise comparison (SQLite memcmp behavior)
        (Blob(a), Blob(b)) => Ok(Boolean(predicate(a.cmp(b)))),

        // Integer comparisons
        (Integer(a), Integer(b)) => Ok(Boolean(predicate(a.cmp(b)))),

        // String comparisons (VARCHAR and CHAR are compatible)
        (Varchar(a), Varchar(b)) => Ok(Boolean(predicate(a.cmp(b)))),
        (Character(a), Character(b)) => Ok(Boolean(predicate(a.cmp(b)))),
        (Character(a), Varchar(b)) | (Varchar(b), Character(a)) => Ok(Boolean(predicate(a.cmp(b)))),

        // Temporal type comparisons (DATE, TIME, TIMESTAMP)
        (Date(a), Date(b)) => Ok(Boolean(predicate(a.cmp(b)))),
        (Time(a), Time(b)) => Ok(Boolean(predicate(a.cmp(b)))),
        (Timestamp(a), Timestamp(b)) => Ok(Boolean(predicate(a.cmp(b)))),

        // Boolean comparisons
        (Boolean(a), Boolean(b)) => Ok(Boolean(predicate(a.cmp(b)))),

        // Cross-type numeric comparisons - exact numeric types
        (left_val, right_val) if is_exact_numeric(left_val) && is_exact_numeric(right_val) => {
            let left_i64 = to_i64(left_val)?;
            let right_i64 = to_i64(right_val)?;
            Ok(Boolean(predicate(left_i64.cmp(&right_i64))))
        }

        // Approximate numeric types
        (left_val, right_val)
            if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
        {
            let left_f64 = to_f64(left_val)?;
            let right_f64 = to_f64(right_val)?;
            Ok(Boolean(predicate(
                left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
            )))
        }

        // Mixed Float/Integer comparisons - use precise comparison for large values
        // SQLite handles edge cases near i64::MAX/i64::MIN specially to avoid
        // precision loss when converting large integers to f64.
        (left_val @ (Float(_) | Real(_) | Double(_)), right_val) if is_exact_numeric(right_val) => {
            let left_f64 = to_f64(left_val)?;
            let right_i64 = to_i64(right_val)?;
            Ok(Boolean(predicate(compare_float_int(left_f64, right_i64))))
        }
        (left_val, right_val @ (Float(_) | Real(_) | Double(_))) if is_exact_numeric(left_val) => {
            let left_i64 = to_i64(left_val)?;
            let right_f64 = to_f64(right_val)?;
            Ok(Boolean(predicate(compare_int_float(left_i64, right_f64))))
        }

        // NUMERIC comparisons with any numeric type
        (left_val @ Numeric(_), right_val)
            if matches!(
                right_val,
                Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
            ) =>
        {
            let left_f64 = to_f64(left_val)?;
            let right_f64 = to_f64(right_val)?;
            Ok(Boolean(predicate(
                left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
            )))
        }
        (left_val, right_val @ Numeric(_))
            if matches!(
                left_val,
                Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
            ) =>
        {
            let left_f64 = to_f64(left_val)?;
            let right_f64 = to_f64(right_val)?;
            Ok(Boolean(predicate(
                left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
            )))
        }

        // SQLite type affinity: Temporal vs Numeric returns false (not an error)
        // In SQLite, comparing DATETIME with INTEGER returns 0/1 based on type ordering,
        // not a type mismatch error. Different types are considered unequal.
        // This enables queries like: WHERE datetime(x) = y where y might be an integer
        (
            Timestamp(_) | Date(_) | Time(_),
            Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_),
        ) => {
            // For = comparison, different types are never equal
            // For < / > comparisons, SQLite uses type ordering (TEXT < INTEGER)
            // We follow SQLite's behavior: temporal types != numeric types
            Ok(Boolean(false))
        }
        (
            Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_),
            Timestamp(_) | Date(_) | Time(_),
        ) => {
            // Symmetric case
            Ok(Boolean(false))
        }

        // Type mismatch - for other incompatible types, still raise error
        _ => Err(ExecutorError::TypeMismatch {
            left: left.clone(),
            op: op_str.to_string(),
            right: right.clone(),
        }),
    }
}

/// Compare an integer (i64) with a float (f64) precisely, handling edge cases
/// near i64::MAX and i64::MIN where f64 loses precision.
///
/// SQLite uses special handling for these cases because f64 cannot exactly
/// represent integers larger than 2^53. This function ensures correct comparison
/// results even when the float value is beyond the range of i64.
///
/// Key insight: When i64::MAX (9223372036854775807) is converted to f64, it
/// rounds UP to 9223372036854775808.0. So `float(i64::MAX)` and `float(i64::MAX+1)`
/// produce the same f64 value. We must handle this edge case carefully.
#[inline]
fn compare_int_float(int_val: i64, float_val: f64) -> std::cmp::Ordering {
    // Handle NaN: NaN comparisons return Equal (will be false for all predicates)
    if float_val.is_nan() {
        return std::cmp::Ordering::Equal;
    }

    // Handle infinity
    if float_val.is_infinite() {
        return if float_val.is_sign_positive() {
            std::cmp::Ordering::Less // int < +inf
        } else {
            std::cmp::Ordering::Greater // int > -inf
        };
    }

    // Threshold for exact representation: 2^53 = 9007199254740992
    const EXACT_INT_MAX: f64 = 9007199254740992.0;
    const EXACT_INT_MIN: f64 = -9007199254740992.0;

    // If float is within the exact representation range, convert to i64 and compare
    if float_val >= EXACT_INT_MIN && float_val <= EXACT_INT_MAX && float_val.fract() == 0.0 {
        let float_as_int = float_val as i64;
        return int_val.cmp(&float_as_int);
    }

    // For floats with fractional parts, convert int to float and compare
    if float_val.fract() != 0.0 {
        let int_as_float = int_val as f64;
        return int_as_float.partial_cmp(&float_val).unwrap_or(std::cmp::Ordering::Equal);
    }

    // Handle the imprecise range: floats between 2^53 and i64 bounds
    // Key edge case: float(i64::MAX) = 9223372036854775808.0 (rounds UP!)
    // This is actually i64::MAX + 1 as an exact integer value.

    // The float value 9223372036854775808.0 represents the integer 2^63 = i64::MAX + 1
    // Since this is beyond i64::MAX, any i64 value is strictly less than it.
    // Note: This constant MUST be written as the exact bit pattern, not derived from i64::MAX
    const I64_MAX_PLUS_ONE_AS_F64: f64 = 9223372036854775808.0_f64; // 2^63 exactly

    if float_val >= I64_MAX_PLUS_ONE_AS_F64 {
        return std::cmp::Ordering::Less; // Any i64 is less than 2^63 or greater
    }

    // Similarly for the minimum: float(i64::MIN) represents -2^63 exactly (no rounding)
    // Any float less than i64::MIN as f64 means int > float
    const I64_MIN_AS_F64: f64 = -9223372036854775808.0_f64; // -2^63 exactly

    if float_val < I64_MIN_AS_F64 {
        return std::cmp::Ordering::Greater; // Any i64 is greater than value < i64::MIN
    }

    // If float equals i64::MIN exactly, compare with int
    if float_val == I64_MIN_AS_F64 {
        return int_val.cmp(&i64::MIN);
    }

    // For values in the imprecise zone (2^53 < |value| < 2^63), we need to be careful.
    // Convert int to float and compare, but account for potential rounding.
    let int_as_float = int_val as f64;

    if int_as_float == float_val {
        // They compare equal as floats. In the imprecise range, this could mean
        // the integer was rounded during conversion. Check if the round-trip
        // preserves the original value.
        let round_trip = int_as_float as i64;
        if round_trip == int_val {
            // The integer survives round-trip, so they're truly equal
            return std::cmp::Ordering::Equal;
        }
        // The integer was rounded. Determine direction of rounding.
        if round_trip > int_val {
            // int_as_float rounded UP, so int < float
            return std::cmp::Ordering::Less;
        } else {
            // int_as_float rounded DOWN, so int > float
            return std::cmp::Ordering::Greater;
        }
    }

    // Standard float comparison
    int_as_float.partial_cmp(&float_val).unwrap_or(std::cmp::Ordering::Equal)
}

/// Compare a float (f64) with an integer (i64) - returns float.cmp(int)
#[inline]
fn compare_float_int(float_val: f64, int_val: i64) -> std::cmp::Ordering {
    compare_int_float(int_val, float_val).reverse()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(text: &str) -> SqlValue {
        SqlValue::Varchar(arcstr::ArcStr::from(text))
    }

    // Issue #5803: Boolean operands normalize to Integer 0/1 before
    // cross-type dispatch, so Boolean vs text/blob follows SQLite type
    // ordering (numeric < text < blob) instead of raising a type mismatch.

    #[test]
    fn boolean_vs_text_all_six_ops() {
        let t = SqlValue::Boolean(true);
        // SQLite: SELECT 1 == 'experiments' → 0
        assert_eq!(ComparisonOps::equal(&t, &s("experiments")).unwrap(), SqlValue::Boolean(false));
        assert_eq!(
            ComparisonOps::not_equal(&t, &s("experiments")).unwrap(),
            SqlValue::Boolean(true)
        );
        // SQLite: SELECT 1 < 'a' → 1 (numeric < text)
        assert_eq!(ComparisonOps::less_than(&t, &s("a")).unwrap(), SqlValue::Boolean(true));
        assert_eq!(
            ComparisonOps::less_than_or_equal(&t, &s("a")).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(ComparisonOps::greater_than(&t, &s("a")).unwrap(), SqlValue::Boolean(false));
        assert_eq!(
            ComparisonOps::greater_than_or_equal(&t, &s("a")).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn text_vs_boolean_symmetric_order() {
        let f = SqlValue::Boolean(false);
        // text > numeric in SQLite type ordering
        assert_eq!(ComparisonOps::greater_than(&s("a"), &f).unwrap(), SqlValue::Boolean(true));
        assert_eq!(ComparisonOps::less_than(&s("a"), &f).unwrap(), SqlValue::Boolean(false));
        assert_eq!(ComparisonOps::equal(&s("a"), &f).unwrap(), SqlValue::Boolean(false));
        assert_eq!(ComparisonOps::not_equal(&s("a"), &f).unwrap(), SqlValue::Boolean(true));
        assert_eq!(
            ComparisonOps::greater_than_or_equal(&s("a"), &f).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::less_than_or_equal(&s("a"), &f).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn boolean_vs_blob_type_ordering() {
        let t = SqlValue::Boolean(true);
        let blob = SqlValue::Blob(vec![0x00]);
        // numeric < blob
        assert_eq!(ComparisonOps::less_than(&t, &blob).unwrap(), SqlValue::Boolean(true));
        assert_eq!(ComparisonOps::equal(&t, &blob).unwrap(), SqlValue::Boolean(false));
        assert_eq!(ComparisonOps::greater_than(&blob, &t).unwrap(), SqlValue::Boolean(true));
    }

    #[test]
    fn boolean_vs_numeric_regression_guard() {
        // Pre-existing behavior (do not regress): 1 IN (SELECT 1) == 2 → 0
        let t = SqlValue::Boolean(true);
        assert_eq!(
            ComparisonOps::equal(&t, &SqlValue::Integer(2)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            ComparisonOps::equal(&t, &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::less_than(&t, &SqlValue::Double(1.5)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Double(0.0), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn boolean_vs_boolean_unchanged() {
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Boolean(false), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn boolean_vs_null_returns_null() {
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
    }
}
