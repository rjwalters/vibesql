//! Rounding functions
//!
//! Implements ROUND, FLOOR, and CEIL/CEILING functions.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::evaluator::functions::coercion::{coerce_to_integer, coerce_to_number};

/// ROUND(x [, precision]) - Round to nearest integer or decimal places
/// SQL:1999 Section 6.27: Numeric value functions
///
/// SQLite compatibility: Automatically coerces string types to numbers.
pub fn round(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ROUND requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let value = &args[0];
    let precision = if args.len() == 2 {
        match coerce_to_integer(&args[1]) {
            Some(p) => p as i32,
            None => return Ok(SqlValue::Null),
        }
    } else {
        0
    };

    // Fast path for numeric types to preserve their original type
    match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(n) => return Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => {
            let multiplier = 10_f32.powi(precision);
            return Ok(SqlValue::Float((f * multiplier).round() / multiplier));
        }
        SqlValue::Double(f) => {
            let multiplier = 10_f64.powi(precision);
            return Ok(SqlValue::Double((f * multiplier).round() / multiplier));
        }
        SqlValue::Real(f) => {
            let multiplier = 10_f32.powi(precision);
            return Ok(SqlValue::Real((f * multiplier).round() / multiplier));
        }
        SqlValue::Numeric(n) => {
            let multiplier = 10_f64.powi(precision);
            return Ok(SqlValue::Numeric((n * multiplier).round() / multiplier));
        }
        _ => {}
    }

    // Slow path: coerce other types (like strings) to numbers
    match coerce_to_number(value) {
        None => Ok(SqlValue::Null),
        Some(n) => {
            let multiplier = 10_f64.powi(precision);
            Ok(SqlValue::Double((n * multiplier).round() / multiplier))
        }
    }
}

/// FLOOR(x) - Round down to nearest integer
/// SQL:1999 Section 6.27: Numeric value functions
///
/// SQLite compatibility: Automatically coerces string types to numbers.
pub fn floor(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "FLOOR requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // Fast path for numeric types
    match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(n) => return Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => return Ok(SqlValue::Float(f.floor())),
        SqlValue::Double(f) => return Ok(SqlValue::Double(f.floor())),
        SqlValue::Real(f) => return Ok(SqlValue::Real(f.floor())),
        SqlValue::Numeric(n) => return Ok(SqlValue::Numeric(n.floor())),
        _ => {}
    }

    // Slow path: coerce other types (like strings) to numbers
    match coerce_to_number(&args[0]) {
        None => Ok(SqlValue::Null),
        Some(n) => Ok(SqlValue::Double(n.floor())),
    }
}

/// CEIL/CEILING(x) - Round up to nearest integer
/// SQL:1999 Section 6.27: Numeric value functions
/// Note: CEILING is an alias for CEIL
///
/// SQLite compatibility: Automatically coerces string types to numbers.
pub fn ceil(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "CEIL requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // Fast path for numeric types
    match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(n) => return Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => return Ok(SqlValue::Float(f.ceil())),
        SqlValue::Double(f) => return Ok(SqlValue::Double(f.ceil())),
        SqlValue::Real(f) => return Ok(SqlValue::Real(f.ceil())),
        SqlValue::Numeric(n) => return Ok(SqlValue::Numeric(n.ceil())),
        _ => {}
    }

    // Slow path: coerce other types (like strings) to numbers
    match coerce_to_number(&args[0]) {
        None => Ok(SqlValue::Null),
        Some(n) => Ok(SqlValue::Double(n.ceil())),
    }
}

/// TRUNCATE(x [, precision]) - Truncate to specified decimal places (towards zero)
/// SQL:1999 Section 6.27: Numeric value functions
///
/// SQLite compatibility: Automatically coerces string types to numbers.
pub fn truncate(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TRUNCATE requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let value = &args[0];
    let precision = if args.len() == 2 {
        match coerce_to_integer(&args[1]) {
            Some(p) => p as i32,
            None => return Ok(SqlValue::Null),
        }
    } else {
        0
    };

    // Fast path for numeric types
    match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(n) => return Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => {
            let multiplier = 10_f32.powi(precision);
            return Ok(SqlValue::Float((f * multiplier).trunc() / multiplier));
        }
        SqlValue::Double(f) => {
            let multiplier = 10_f64.powi(precision);
            return Ok(SqlValue::Double((f * multiplier).trunc() / multiplier));
        }
        SqlValue::Real(f) => {
            let multiplier = 10_f32.powi(precision);
            return Ok(SqlValue::Real((f * multiplier).trunc() / multiplier));
        }
        SqlValue::Numeric(n) => {
            let multiplier = 10_f64.powi(precision);
            return Ok(SqlValue::Numeric((n * multiplier).trunc() / multiplier));
        }
        _ => {}
    }

    // Slow path: coerce other types (like strings) to numbers
    match coerce_to_number(value) {
        None => Ok(SqlValue::Null),
        Some(n) => {
            let multiplier = 10_f64.powi(precision);
            Ok(SqlValue::Double((n * multiplier).trunc() / multiplier))
        }
    }
}
