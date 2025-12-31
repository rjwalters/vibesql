//! Rounding functions
//!
//! Implements ROUND, FLOOR, and CEIL/CEILING functions.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::evaluator::functions::coercion::{coerce_to_integer, coerce_to_number};

/// ROUND(x [, precision]) - Round to nearest integer or decimal places
/// SQL:1999 Section 6.27: Numeric value functions
///
/// SQLite compatibility:
/// - Automatically coerces string types to numbers.
/// - Precision is clamped to [0, 30] range (values > 30 become 30, values < 0 become 0).
/// - Uses string formatting for non-zero precision to avoid floating-point precision loss.
pub fn round(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ROUND requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let value = &args[0];
    // SQLite clamps precision to [0, 30] range
    // This also handles i64 overflow cases (e.g., 4294967297 becomes 30)
    let precision = if args.len() == 2 {
        match coerce_to_integer(&args[1]) {
            Some(p) => {
                if p > 30 {
                    30usize
                } else if p < 0 {
                    0usize
                } else {
                    p as usize
                }
            }
            None => return Ok(SqlValue::Null),
        }
    } else {
        0usize
    };

    // Get the numeric value to round
    let num = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(n) => *n as f64,
        SqlValue::Float(f) => *f as f64,
        SqlValue::Double(f) => *f,
        SqlValue::Real(f) => *f as f64,
        SqlValue::Numeric(n) => *n,
        _ => {
            // Slow path: coerce other types (like strings) to numbers
            match coerce_to_number(value) {
                None => return Ok(SqlValue::Null),
                Some(n) => n,
            }
        }
    };

    // SQLite always returns REAL from round(), regardless of input type
    // This ensures whole numbers display with ".0" suffix (e.g., "2.0" not "2")
    let result = round_with_precision(num, precision);
    Ok(SqlValue::Double(result))
}

/// Rounds a floating-point number to the specified precision using SQLite's algorithm.
///
/// For precision == 0, uses direct integer conversion for efficiency.
/// For precision > 0, uses string formatting to avoid floating-point precision loss.
/// This matches SQLite's implementation which uses printf("%!.*f", n, r) internally.
fn round_with_precision(value: f64, precision: usize) -> f64 {
    // SQLite's special case: if value is outside i64 range, it has no fractional part
    // to round (this is an optimization from SQLite's source)
    const MAX_ROUNDABLE: f64 = 4503599627370496.0; // 2^52
    if value < -MAX_ROUNDABLE || value > MAX_ROUNDABLE {
        return value;
    }

    if precision == 0 {
        // Direct rounding for precision 0 (more efficient)
        if value < 0.0 {
            (value - 0.5) as i64 as f64
        } else {
            (value + 0.5) as i64 as f64
        }
    } else {
        // Use string formatting to avoid precision loss
        // This matches SQLite's approach: sqlite3_mprintf("%!.*f", n, r)
        let formatted = format!("{:.prec$}", value, prec = precision);
        formatted.parse::<f64>().unwrap_or(value)
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
            // Real is now f64
            let multiplier = 10_f64.powi(precision);
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
