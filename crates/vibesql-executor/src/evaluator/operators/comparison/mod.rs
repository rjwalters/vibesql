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
    evaluator::casting::{
        boolean_to_i64, is_approximate_numeric, is_exact_numeric, to_f64, to_i64,
    },
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
    // If either operand is Boolean and the other is numeric, coerce boolean to i64
    match (left, right) {
        // Boolean compared to any numeric type
        (Boolean(_), right_val)
            if is_exact_numeric(right_val)
                || is_approximate_numeric(right_val)
                || matches!(right_val, Numeric(_)) =>
        {
            let left_i64 = boolean_to_i64(left).unwrap(); // Safe: we know left is Boolean

            // For exact numeric, compare as i64
            if is_exact_numeric(right_val) {
                let right_i64 = to_i64(right_val)?;
                return Ok(Boolean(predicate(left_i64.cmp(&right_i64))));
            }

            // For approximate numeric or Numeric, compare as f64
            let left_f64 = left_i64 as f64;
            let right_f64 = to_f64(right_val)?;
            return Ok(Boolean(predicate(
                left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
            )));
        }

        // Numeric compared to Boolean (symmetric case)
        (left_val, Boolean(_))
            if is_exact_numeric(left_val)
                || is_approximate_numeric(left_val)
                || matches!(left_val, Numeric(_)) =>
        {
            let right_i64 = boolean_to_i64(right).unwrap(); // Safe: we know right is Boolean

            // For exact numeric, compare as i64
            if is_exact_numeric(left_val) {
                let left_i64 = to_i64(left_val)?;
                return Ok(Boolean(predicate(left_i64.cmp(&right_i64))));
            }

            // For approximate numeric or Numeric, compare as f64
            let left_f64 = to_f64(left_val)?;
            let right_f64 = right_i64 as f64;
            return Ok(Boolean(predicate(
                left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
            )));
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

    match (left, right) {
        // Integer comparisons
        (Integer(a), Integer(b)) => Ok(Boolean(predicate(a.cmp(b)))),

        // String comparisons (VARCHAR and CHAR are compatible)
        // SQLite behavior: If both strings look like numbers, compare numerically
        // Examples: '0' = '0.0' → true, '10' < '9' → true (10 < 9 is false, but '10' < '9' as strings)
        // We need numeric comparison: '10' > '9' → true
        (Varchar(a), Varchar(b)) => compare_strings(a, b, predicate),
        (Character(a), Character(b)) => compare_strings(a, b, predicate),
        (Character(a), Varchar(b)) | (Varchar(b), Character(a)) => compare_strings(a, b, predicate),

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

        // Mixed Float/Integer comparisons - promote Integer to Float
        (
            left_val @ (Float(_) | Real(_) | Double(_)),
            right_val @ (Integer(_) | Smallint(_) | Bigint(_)),
        )
        | (
            left_val @ (Integer(_) | Smallint(_) | Bigint(_)),
            right_val @ (Float(_) | Real(_) | Double(_)),
        ) => {
            let left_f64 = to_f64(left_val)?;
            let right_f64 = to_f64(right_val)?;
            Ok(Boolean(predicate(
                left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
            )))
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
        (Timestamp(_) | Date(_) | Time(_), Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)) => {
            // For = comparison, different types are never equal
            // For < / > comparisons, SQLite uses type ordering (TEXT < INTEGER)
            // We follow SQLite's behavior: temporal types != numeric types
            Ok(Boolean(false))
        }
        (Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_), Timestamp(_) | Date(_) | Time(_)) => {
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

/// Compare two strings with SQLite's numeric coercion rules
///
/// SQLite behavior: When comparing two strings, if both look like numbers,
/// compare them numerically. Otherwise, compare as strings.
///
/// Examples:
/// - '0' = '0.0' → true (both are numeric, 0.0 == 0.0)
/// - '10' > '9' → true (both are numeric, 10 > 9)
/// - 'abc' < 'def' → true (neither is numeric, string comparison)
/// - '10' < 'abc' → true (mixed: numeric < non-numeric in SQLite)
#[inline]
fn compare_strings<F>(a: &str, b: &str, predicate: F) -> Result<SqlValue, ExecutorError>
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    // Try to parse both strings as numbers
    let a_num = parse_as_number(a);
    let b_num = parse_as_number(b);

    match (a_num, b_num) {
        // Both are numeric - compare as numbers
        (Some(a_val), Some(b_val)) => {
            let ordering = a_val.partial_cmp(&b_val).unwrap_or(std::cmp::Ordering::Equal);
            Ok(SqlValue::Boolean(predicate(ordering)))
        }
        // One is numeric, one is not - SQLite orders numeric < text
        (Some(_), None) => Ok(SqlValue::Boolean(predicate(std::cmp::Ordering::Less))),
        (None, Some(_)) => Ok(SqlValue::Boolean(predicate(std::cmp::Ordering::Greater))),
        // Neither is numeric - compare as strings
        (None, None) => Ok(SqlValue::Boolean(predicate(a.cmp(b)))),
    }
}

/// Try to parse a string as a number (integer or float)
///
/// Returns Some(f64) if the string looks like a number, None otherwise.
/// Handles: integers, floats, negative numbers, leading/trailing whitespace
#[inline]
fn parse_as_number(s: &str) -> Option<f64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Try parsing as f64 (handles both integer and floating point)
    trimmed.parse::<f64>().ok()
}
