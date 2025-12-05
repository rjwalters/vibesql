//! Comparison operator implementations
//!
//! Handles: =, <>, <, <=, >, >=
//! Supports: All SQL types with proper type coercion
//! Includes: NULL handling (three-valued logic), cross-type comparisons

pub mod equality;
pub mod ordering;

use crate::{
    errors::ExecutorError,
    evaluator::casting::{
        boolean_to_i64, is_approximate_numeric, is_exact_numeric, to_f64, to_i64,
    },
};
use std::str::FromStr;
use vibesql_types::SqlValue;

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

    match (left, right) {
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

        // Type mismatch
        _ => Err(ExecutorError::TypeMismatch {
            left: left.clone(),
            op: op_str.to_string(),
            right: right.clone(),
        }),
    }
}
