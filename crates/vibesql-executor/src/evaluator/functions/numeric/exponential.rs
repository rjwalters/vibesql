//! Exponential and logarithmic functions
//!
//! Implements POWER/POW, SQRT, EXP, LN/LOG, and LOG10 functions.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Helper: Convert numeric types to f64
pub fn numeric_to_f64(val: &SqlValue) -> Result<f64, ExecutorError> {
    match val {
        SqlValue::Integer(n) => Ok(*n as f64),
        SqlValue::Bigint(n) => Ok(*n as f64),
        SqlValue::Smallint(n) => Ok(*n as f64),
        SqlValue::Unsigned(n) => Ok(*n as f64),
        SqlValue::Numeric(f) => Ok(*f),
        SqlValue::Float(f) => Ok(*f as f64),
        SqlValue::Double(f) => Ok(*f),
        SqlValue::Real(f) => Ok(*f as f64),
        _ => Err(ExecutorError::UnsupportedFeature(format!("Cannot convert {:?} to numeric", val))),
    }
}

/// POWER(x, y) - x raised to power y
/// SQL:1999 Section 6.27: Numeric value functions
/// Note: POW is an alias for POWER
pub fn power(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "POWER requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Integer(base), SqlValue::Integer(exp)) => {
            if *exp >= 0 && *exp <= i32::MAX as i64 {
                Ok(SqlValue::Double((*base as f64).powi(*exp as i32)))
            } else {
                Ok(SqlValue::Double((*base as f64).powf(*exp as f64)))
            }
        }
        (SqlValue::Float(base), SqlValue::Float(exp)) => Ok(SqlValue::Float(base.powf(*exp))),
        (SqlValue::Double(base), SqlValue::Double(exp)) => Ok(SqlValue::Double(base.powf(*exp))),
        (base, exp) => {
            // Try to convert to f64
            let base_f64 = numeric_to_f64(base)?;
            let exp_f64 = numeric_to_f64(exp)?;
            Ok(SqlValue::Double(base_f64.powf(exp_f64)))
        }
    }
}

/// SQRT(x) - Square root
/// SQL:1999 Section 6.27: Numeric value functions
pub fn sqrt(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "SQRT requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => {
            if *n < 0 {
                Err(ExecutorError::UnsupportedFeature("SQRT of negative number".to_string()))
            } else {
                Ok(SqlValue::Double((*n as f64).sqrt()))
            }
        }
        SqlValue::Float(f) => {
            if *f < 0.0 {
                Err(ExecutorError::UnsupportedFeature("SQRT of negative number".to_string()))
            } else {
                Ok(SqlValue::Float(f.sqrt()))
            }
        }
        SqlValue::Double(f) => {
            if *f < 0.0 {
                Err(ExecutorError::UnsupportedFeature("SQRT of negative number".to_string()))
            } else {
                Ok(SqlValue::Double(f.sqrt()))
            }
        }
        val => {
            let f = numeric_to_f64(val)?;
            if f < 0.0 {
                Err(ExecutorError::UnsupportedFeature("SQRT of negative number".to_string()))
            } else {
                Ok(SqlValue::Double(f.sqrt()))
            }
        }
    }
}

/// EXP(x) - e raised to power x
/// SQL:1999 Section 6.27: Numeric value functions
pub fn exp(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "EXP requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            Ok(SqlValue::Double(x.exp()))
        }
    }
}

/// LN(x) - Natural logarithm
/// SQL:1999 Section 6.27: Numeric value functions
/// Note: LOG is an alias for LN
pub fn ln(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LN requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            if x <= 0.0 {
                Err(ExecutorError::UnsupportedFeature("LN of non-positive number".to_string()))
            } else {
                Ok(SqlValue::Double(x.ln()))
            }
        }
    }
}

/// LOG10(x) - Base-10 logarithm
pub fn log10(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LOG10 requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            if x <= 0.0 {
                Err(ExecutorError::UnsupportedFeature("LOG10 of non-positive number".to_string()))
            } else {
                Ok(SqlValue::Double(x.log10()))
            }
        }
    }
}
