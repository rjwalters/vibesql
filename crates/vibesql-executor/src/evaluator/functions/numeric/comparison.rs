//! Comparison functions
//!
//! Implements GREATEST and LEAST functions, plus SQLite-compatible scalar MIN/MAX.

use std::cmp::Ordering;

use vibesql_types::SqlValue;

use super::exponential::numeric_to_f64;
use crate::errors::ExecutorError;

/// SQLite-compatible scalar MIN(val1, val2, ...) - Returns minimum value
/// Returns NULL if ANY argument is NULL (SQLite semantics)
pub fn scalar_min(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    scalar_extreme(args, "MIN", Ordering::Less)
}

/// SQLite-compatible scalar MAX(val1, val2, ...) - Returns maximum value
/// Returns NULL if ANY argument is NULL (SQLite semantics)
pub fn scalar_max(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    scalar_extreme(args, "MAX", Ordering::Greater)
}

/// Shared body of scalar MIN/MAX: picks the argument that compares as `want`
/// (`Less` for MIN, `Greater` for MAX) against the current pick. The first
/// argument wins ties and incomparable (NaN) pairs.
fn scalar_extreme(
    args: &[SqlValue],
    name: &str,
    want: Ordering,
) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "{name} requires at least one argument"
        )));
    }

    // SQLite semantics: return NULL if any argument is NULL
    if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }

    let mut best = &args[0];
    for arg in &args[1..] {
        if sqlite_compare(arg, best)? == Some(want) {
            best = arg;
        }
    }

    Ok(best.clone())
}

/// Type-aware comparison of `b` against `a` for scalar MIN/MAX.
/// `None` means the values are incomparable (e.g. NaN).
fn sqlite_compare(b: &SqlValue, a: &SqlValue) -> Result<Option<Ordering>, ExecutorError> {
    Ok(match (a, b) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Some(b.cmp(a)),
        (SqlValue::Double(a), SqlValue::Double(b)) => b.partial_cmp(a),
        // String types (Character/Varchar)
        (SqlValue::Character(a), SqlValue::Character(b))
        | (SqlValue::Varchar(a), SqlValue::Varchar(b))
        | (SqlValue::Character(a), SqlValue::Varchar(b))
        | (SqlValue::Varchar(a), SqlValue::Character(b)) => b.partial_cmp(a),
        // Mixed numeric types - compare as f64
        (a, b) if is_numeric(a) && is_numeric(b) => {
            let a_f64 = numeric_to_f64(a)?;
            let b_f64 = numeric_to_f64(b)?;
            b_f64.partial_cmp(&a_f64)
        }
        // SQLite type affinity comparison order: NULL < INTEGER/REAL < TEXT < BLOB;
        // within the same type category, compare as strings
        (a, b) => match type_order(b).cmp(&type_order(a)) {
            Ordering::Equal => Some(b.to_string().cmp(&a.to_string())),
            ord => Some(ord),
        },
    })
}

/// Check if a value is numeric
fn is_numeric(val: &SqlValue) -> bool {
    matches!(
        val,
        SqlValue::Integer(_)
            | SqlValue::Smallint(_)
            | SqlValue::Bigint(_)
            | SqlValue::Unsigned(_)
            | SqlValue::Numeric(_)
            | SqlValue::Float(_)
            | SqlValue::Real(_)
            | SqlValue::Double(_)
    )
}

/// SQLite type ordering for comparison: NULL < INTEGER/REAL < TEXT
fn type_order(val: &SqlValue) -> u8 {
    match val {
        SqlValue::Null => 0,
        SqlValue::Integer(_)
        | SqlValue::Smallint(_)
        | SqlValue::Bigint(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Numeric(_)
        | SqlValue::Float(_)
        | SqlValue::Real(_)
        | SqlValue::Double(_) => 1,
        SqlValue::Character(_) | SqlValue::Varchar(_) => 2,
        _ => 3, // Other types (Date, Time, etc.)
    }
}

/// GREATEST(val1, val2, ...) - Returns greatest value
pub fn greatest(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    greatest_least(args, "GREATEST", Ordering::Greater)
}

/// LEAST(val1, val2, ...) - Returns smallest value
pub fn least(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    greatest_least(args, "LEAST", Ordering::Less)
}

/// Shared body of GREATEST/LEAST: NULL arguments are skipped, and the
/// argument that compares as `want` against the current pick replaces it.
fn greatest_least(
    args: &[SqlValue],
    name: &str,
    want: Ordering,
) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "{name} requires at least one argument"
        )));
    }

    let mut best = &args[0];
    for arg in &args[1..] {
        // Skip NULL values
        if matches!(arg, SqlValue::Null) {
            continue;
        }
        if matches!(best, SqlValue::Null) {
            best = arg;
            continue;
        }

        let ord = match (best, arg) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => Some(b.cmp(a)),
            (SqlValue::Double(a), SqlValue::Double(b)) => b.partial_cmp(a),
            (a, b) => {
                let a_f64 = numeric_to_f64(a)?;
                let b_f64 = numeric_to_f64(b)?;
                b_f64.partial_cmp(&a_f64)
            }
        };
        if ord == Some(want) {
            best = arg;
        }
    }

    Ok(best.clone())
}
