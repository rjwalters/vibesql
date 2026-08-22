//! Basic numeric functions
//!
//! Implements ABS, SIGN, and MOD functions.

use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, evaluator::functions::coercion::coerce_to_number};

/// ABS(x) - Absolute value
/// SQL:1999 Section 6.27: Numeric value functions
///
/// SQLite compatibility: Automatically coerces string types to numbers.
/// - ABS('-5') returns 5.0
/// - ABS('free') returns 0.0
pub fn abs(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ABS requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // Fast path for numeric types to preserve their original type
    match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(n) => {
            return n.checked_abs().map(SqlValue::Integer).ok_or(ExecutorError::IntegerOverflow);
        }
        SqlValue::Bigint(n) => {
            return n.checked_abs().map(SqlValue::Bigint).ok_or(ExecutorError::IntegerOverflow);
        }
        SqlValue::Smallint(n) => {
            return n.checked_abs().map(SqlValue::Smallint).ok_or(ExecutorError::IntegerOverflow);
        }
        SqlValue::Float(n) => return Ok(SqlValue::Float(n.abs())),
        SqlValue::Double(n) => return Ok(SqlValue::Double(n.abs())),
        SqlValue::Real(n) => return Ok(SqlValue::Real(n.abs())),
        SqlValue::Numeric(n) => return Ok(SqlValue::Numeric(n.abs())),
        SqlValue::Unsigned(n) => return Ok(SqlValue::Unsigned(*n)), // Unsigned is always positive
        _ => {}
    }

    // Slow path: coerce other types (like strings) to numbers
    match coerce_to_number(&args[0]) {
        None => Ok(SqlValue::Null),
        Some(n) => Ok(SqlValue::Double(n.abs())),
    }
}

/// SIGN(x) - Sign of number (-1, 0, or 1)
/// SQL:1999 Section 6.27: Numeric value functions
///
/// SQLite compatibility: Automatically coerces string types to numbers.
pub fn sign(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "SIGN requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // Fast path for numeric types to preserve their original type
    match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(n) => {
            let sign = match n.cmp(&0) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Equal => 0,
            };
            return Ok(SqlValue::Integer(sign));
        }
        SqlValue::Bigint(n) => {
            let sign = match n.cmp(&0) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Equal => 0,
            };
            return Ok(SqlValue::Bigint(sign));
        }
        SqlValue::Smallint(n) => {
            let sign: i16 = match n.cmp(&0) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Equal => 0,
            };
            return Ok(SqlValue::Smallint(sign));
        }
        SqlValue::Float(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            return Ok(SqlValue::Float(sign));
        }
        SqlValue::Double(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            return Ok(SqlValue::Double(sign));
        }
        SqlValue::Real(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            return Ok(SqlValue::Real(sign));
        }
        SqlValue::Numeric(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            return Ok(SqlValue::Numeric(sign));
        }
        SqlValue::Unsigned(n) => {
            // Unsigned is always non-negative
            let sign: u64 = if *n > 0 { 1 } else { 0 };
            return Ok(SqlValue::Unsigned(sign));
        }
        _ => {}
    }

    // Slow path: coerce other types (like strings) to numbers
    match coerce_to_number(&args[0]) {
        None => Ok(SqlValue::Null),
        Some(n) => {
            let sign = if n < 0.0 {
                -1.0
            } else if n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Double(sign))
        }
    }
}

/// MOD(x, y) - Modulo (remainder)
/// SQL:1999 Section 6.27: Numeric value functions
///
/// SQLite compatibility: Automatically coerces string types to numbers.
pub fn mod_fn(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "MOD requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // Fast path: both arguments are the same numeric type
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => return Ok(SqlValue::Null),

        (SqlValue::Integer(x), SqlValue::Integer(y)) => {
            if *y == 0 {
                return Ok(SqlValue::Null);
            }
            return Ok(SqlValue::Integer(x % y));
        }

        (SqlValue::Float(x), SqlValue::Float(y)) => {
            if *y == 0.0 {
                return Ok(SqlValue::Null);
            }
            return Ok(SqlValue::Float(x % y));
        }
        (SqlValue::Real(x), SqlValue::Real(y)) => {
            // Real is now f64
            if *y == 0.0 {
                return Ok(SqlValue::Null);
            }
            return Ok(SqlValue::Real(x % y));
        }

        _ => {}
    }

    // Slow path: coerce both arguments to f64
    let x = match coerce_to_number(&args[0]) {
        Some(n) => n,
        None => return Ok(SqlValue::Null),
    };
    let y = match coerce_to_number(&args[1]) {
        Some(n) => n,
        None => return Ok(SqlValue::Null),
    };

    if y == 0.0 {
        return Ok(SqlValue::Null);
    }

    Ok(SqlValue::Double(x % y))
}

/// PI() - Mathematical constant π
pub fn pi(_args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    Ok(SqlValue::Double(std::f64::consts::PI))
}
