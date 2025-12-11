//! Comparison functions
//!
//! Implements GREATEST and LEAST functions, plus SQLite-compatible scalar MIN/MAX.

use vibesql_types::SqlValue;

use super::exponential::numeric_to_f64;
use crate::errors::ExecutorError;

/// SQLite-compatible scalar MIN(val1, val2, ...) - Returns minimum value
/// Returns NULL if ANY argument is NULL (SQLite semantics)
pub fn scalar_min(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MIN requires at least one argument".to_string(),
        ));
    }

    // SQLite semantics: return NULL if any argument is NULL
    for arg in args {
        if matches!(arg, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
    }

    let mut min_val = &args[0];
    for arg in &args[1..] {
        // Compare values - use type-aware comparison
        match (min_val, arg) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                if b < a {
                    min_val = arg;
                }
            }
            (SqlValue::Double(a), SqlValue::Double(b)) => {
                if b < a {
                    min_val = arg;
                }
            }
            // String types (Character/Varchar)
            (SqlValue::Character(a), SqlValue::Character(b))
            | (SqlValue::Varchar(a), SqlValue::Varchar(b))
            | (SqlValue::Character(a), SqlValue::Varchar(b))
            | (SqlValue::Varchar(a), SqlValue::Character(b)) => {
                if b < a {
                    min_val = arg;
                }
            }
            // Mixed numeric types - compare as f64
            (a, b) if is_numeric(a) && is_numeric(b) => {
                let a_f64 = numeric_to_f64(a)?;
                let b_f64 = numeric_to_f64(b)?;
                if b_f64 < a_f64 {
                    min_val = arg;
                }
            }
            // SQLite type affinity comparison order: NULL < INTEGER/REAL < TEXT < BLOB
            (a, b) => {
                if type_order(b) < type_order(a) {
                    min_val = arg;
                } else if type_order(b) == type_order(a) {
                    // Same type category, compare as strings
                    let a_str = a.to_string();
                    let b_str = b.to_string();
                    if b_str < a_str {
                        min_val = arg;
                    }
                }
            }
        }
    }

    Ok(min_val.clone())
}

/// SQLite-compatible scalar MAX(val1, val2, ...) - Returns maximum value
/// Returns NULL if ANY argument is NULL (SQLite semantics)
pub fn scalar_max(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MAX requires at least one argument".to_string(),
        ));
    }

    // SQLite semantics: return NULL if any argument is NULL
    for arg in args {
        if matches!(arg, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
    }

    let mut max_val = &args[0];
    for arg in &args[1..] {
        // Compare values - use type-aware comparison
        match (max_val, arg) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                if b > a {
                    max_val = arg;
                }
            }
            (SqlValue::Double(a), SqlValue::Double(b)) => {
                if b > a {
                    max_val = arg;
                }
            }
            // String types (Character/Varchar)
            (SqlValue::Character(a), SqlValue::Character(b))
            | (SqlValue::Varchar(a), SqlValue::Varchar(b))
            | (SqlValue::Character(a), SqlValue::Varchar(b))
            | (SqlValue::Varchar(a), SqlValue::Character(b)) => {
                if b > a {
                    max_val = arg;
                }
            }
            // Mixed numeric types - compare as f64
            (a, b) if is_numeric(a) && is_numeric(b) => {
                let a_f64 = numeric_to_f64(a)?;
                let b_f64 = numeric_to_f64(b)?;
                if b_f64 > a_f64 {
                    max_val = arg;
                }
            }
            // SQLite type affinity comparison order: NULL < INTEGER/REAL < TEXT < BLOB
            (a, b) => {
                if type_order(b) > type_order(a) {
                    max_val = arg;
                } else if type_order(b) == type_order(a) {
                    // Same type category, compare as strings
                    let a_str = a.to_string();
                    let b_str = b.to_string();
                    if b_str > a_str {
                        max_val = arg;
                    }
                }
            }
        }
    }

    Ok(max_val.clone())
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
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "GREATEST requires at least one argument".to_string(),
        ));
    }

    let mut max_val = &args[0];
    for arg in &args[1..] {
        // Skip NULL values
        if matches!(arg, SqlValue::Null) {
            continue;
        }
        if matches!(max_val, SqlValue::Null) {
            max_val = arg;
            continue;
        }

        // Compare values
        match (max_val, arg) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                if b > a {
                    max_val = arg;
                }
            }
            (SqlValue::Double(a), SqlValue::Double(b)) => {
                if b > a {
                    max_val = arg;
                }
            }
            (a, b) => {
                let a_f64 = numeric_to_f64(a)?;
                let b_f64 = numeric_to_f64(b)?;
                if b_f64 > a_f64 {
                    max_val = arg;
                }
            }
        }
    }

    Ok(max_val.clone())
}

/// LEAST(val1, val2, ...) - Returns smallest value
pub fn least(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "LEAST requires at least one argument".to_string(),
        ));
    }

    let mut min_val = &args[0];
    for arg in &args[1..] {
        // Skip NULL values
        if matches!(arg, SqlValue::Null) {
            continue;
        }
        if matches!(min_val, SqlValue::Null) {
            min_val = arg;
            continue;
        }

        // Compare values
        match (min_val, arg) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                if b < a {
                    min_val = arg;
                }
            }
            (SqlValue::Double(a), SqlValue::Double(b)) => {
                if b < a {
                    min_val = arg;
                }
            }
            (a, b) => {
                let a_f64 = numeric_to_f64(a)?;
                let b_f64 = numeric_to_f64(b)?;
                if b_f64 < a_f64 {
                    min_val = arg;
                }
            }
        }
    }

    Ok(min_val.clone())
}
