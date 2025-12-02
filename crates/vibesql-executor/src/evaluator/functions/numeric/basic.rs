//! Basic numeric functions
//!
//! Implements ABS, SIGN, and MOD functions.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// ABS(x) - Absolute value
/// SQL:1999 Section 6.27: Numeric value functions
pub fn abs(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ABS requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => Ok(SqlValue::Integer(n.abs())),
        SqlValue::Bigint(n) => Ok(SqlValue::Bigint(n.abs())),
        SqlValue::Smallint(n) => Ok(SqlValue::Smallint(n.abs())),
        SqlValue::Float(n) => Ok(SqlValue::Float(n.abs())),
        SqlValue::Double(n) => Ok(SqlValue::Double(n.abs())),
        SqlValue::Real(n) => Ok(SqlValue::Real(n.abs())),
        SqlValue::Numeric(n) => Ok(SqlValue::Numeric(n.abs())),
        SqlValue::Unsigned(n) => Ok(SqlValue::Unsigned(*n)), // Unsigned is always positive
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "ABS requires numeric argument, got {:?}",
            val
        ))),
    }
}

/// SIGN(x) - Sign of number (-1, 0, or 1)
/// SQL:1999 Section 6.27: Numeric value functions
pub fn sign(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "SIGN requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => {
            let sign = match n.cmp(&0) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Equal => 0,
            };
            Ok(SqlValue::Integer(sign))
        }
        SqlValue::Bigint(n) => {
            let sign = match n.cmp(&0) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Equal => 0,
            };
            Ok(SqlValue::Bigint(sign))
        }
        SqlValue::Smallint(n) => {
            let sign: i16 = match n.cmp(&0) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Equal => 0,
            };
            Ok(SqlValue::Smallint(sign))
        }
        SqlValue::Float(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Float(sign))
        }
        SqlValue::Double(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Double(sign))
        }
        SqlValue::Real(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Real(sign))
        }
        SqlValue::Numeric(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Numeric(sign))
        }
        SqlValue::Unsigned(n) => {
            // Unsigned is always non-negative
            let sign: u64 = if *n > 0 { 1 } else { 0 };
            Ok(SqlValue::Unsigned(sign))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "SIGN requires numeric argument, got {:?}",
            val
        ))),
    }
}

/// MOD(x, y) - Modulo (remainder)
/// SQL:1999 Section 6.27: Numeric value functions
pub fn mod_fn(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "MOD requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),

        (SqlValue::Integer(x), SqlValue::Integer(y)) => {
            if *y == 0 {
                return Ok(SqlValue::Null);
            }
            Ok(SqlValue::Integer(x % y))
        }

        (SqlValue::Float(x), SqlValue::Float(y)) | (SqlValue::Real(x), SqlValue::Real(y)) => {
            if *y == 0.0 {
                return Ok(SqlValue::Null);
            }
            Ok(SqlValue::Float(x % y))
        }

        _ => Err(ExecutorError::UnsupportedFeature(
            "MOD requires numeric arguments of the same type".to_string(),
        )),
    }
}

/// PI() - Mathematical constant π
pub fn pi(_args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    Ok(SqlValue::Double(std::f64::consts::PI))
}
