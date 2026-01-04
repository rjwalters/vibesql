//! Type conversion and inspection functions
//!
//! This module contains SQLite-compatible type functions:
//! - TYPEOF(x) - Return type name of expression
//! - TOREAL(x) - Convert value to floating-point
//! - TOINTEGER(x) - Convert value to integer
//! - INTREAL(x) - SQLite internal test function

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// TYPEOF(x) - Return the type name of the expression
///
/// SQLite returns one of: "null", "integer", "real", "text", "blob"
/// We map VibeSQL types to these SQLite type names.
pub(crate) fn typeof_func(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TYPEOF requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let type_name = match &args[0] {
        SqlValue::Null => "null",
        SqlValue::Integer(_)
        | SqlValue::Bigint(_)
        | SqlValue::Smallint(_)
        | SqlValue::Unsigned(_) => "integer",
        SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Numeric(_) | SqlValue::Float(_) => {
            "real"
        }
        SqlValue::Varchar(_) | SqlValue::Character(_) => "text",
        // Map other types to text (safe default for SQLite compatibility)
        SqlValue::Boolean(_) => "integer", // SQLite stores booleans as integers
        SqlValue::Date(_) | SqlValue::Time(_) | SqlValue::Timestamp(_) => "text",
        SqlValue::Interval(_) => "text",
        SqlValue::Vector(_) => "blob",
        SqlValue::Blob(_) => "blob",
    };

    Ok(SqlValue::Varchar(type_name.into()))
}

/// TOREAL(x) - Convert value to floating-point number (SQLite REAL type)
///
/// Converts the argument to a floating-point number. This is used primarily in SQLite
/// test suites for explicit type conversion. NULL input returns NULL.
/// String inputs are parsed as floating-point numbers.
/// Integer inputs are converted to floating-point.
///
/// Reference: https://www.sqlite.org/lang_corefunc.html
pub(crate) fn toreal(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TOREAL requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // Real is now f64 (SQLite REAL is 8-byte IEEE float)
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Real(r) => Ok(SqlValue::Real(*r)),
        SqlValue::Double(d) => Ok(SqlValue::Real(*d)),
        SqlValue::Float(f) => Ok(SqlValue::Real(*f as f64)),
        SqlValue::Numeric(n) => Ok(SqlValue::Real(*n)),
        SqlValue::Integer(i) => Ok(SqlValue::Real(*i as f64)),
        SqlValue::Bigint(i) => Ok(SqlValue::Real(*i as f64)),
        SqlValue::Smallint(i) => Ok(SqlValue::Real(*i as f64)),
        SqlValue::Unsigned(u) => Ok(SqlValue::Real(*u as f64)),
        SqlValue::Boolean(b) => Ok(SqlValue::Real(if *b { 1.0 } else { 0.0 })),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Try to parse string as a number
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(SqlValue::Real(0.0));
            }
            match trimmed.parse::<f64>() {
                Ok(f) => Ok(SqlValue::Real(f)),
                Err(_) => Ok(SqlValue::Real(0.0)), // SQLite returns 0.0 for non-numeric strings
            }
        }
        // For other types (Date, Time, Timestamp, Interval, Vector), return 0.0
        _ => Ok(SqlValue::Real(0.0)),
    }
}

/// TOINTEGER(x) - Convert value to integer (SQLite INTEGER type)
///
/// Converts the argument to an integer. This is used primarily in SQLite
/// test suites for explicit type conversion. NULL input returns NULL.
/// String inputs are parsed as integers (truncating any decimal part).
/// Floating-point inputs are truncated towards zero.
///
/// Reference: https://www.sqlite.org/lang_corefunc.html
pub(crate) fn tointeger(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TOINTEGER requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(i) => Ok(SqlValue::Integer(*i)),
        SqlValue::Bigint(i) => Ok(SqlValue::Integer(*i)),
        SqlValue::Smallint(i) => Ok(SqlValue::Integer(*i as i64)),
        SqlValue::Unsigned(u) => Ok(SqlValue::Integer(*u as i64)),
        SqlValue::Real(r) => Ok(SqlValue::Integer(*r as i64)),
        SqlValue::Double(d) => Ok(SqlValue::Integer(*d as i64)),
        SqlValue::Float(f) => Ok(SqlValue::Integer(*f as i64)),
        SqlValue::Numeric(n) => Ok(SqlValue::Integer(*n as i64)),
        SqlValue::Boolean(b) => Ok(SqlValue::Integer(if *b { 1 } else { 0 })),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Try to parse string as a number
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(SqlValue::Integer(0));
            }
            // First try parsing as integer
            if let Ok(i) = trimmed.parse::<i64>() {
                return Ok(SqlValue::Integer(i));
            }
            // Then try parsing as float and truncating
            if let Ok(f) = trimmed.parse::<f64>() {
                return Ok(SqlValue::Integer(f as i64));
            }
            // SQLite returns 0 for non-numeric strings
            Ok(SqlValue::Integer(0))
        }
        // For other types (Date, Time, Timestamp, Interval, Vector), return 0
        _ => Ok(SqlValue::Integer(0)),
    }
}

/// INTREAL(x) - SQLite test function for integer/real type testing
///
/// This is a SQLite internal test function. It returns the value unchanged
/// but with type affinity information preserved. In VibeSQL, we simply
/// return the value as-is since we don't have the same type affinity system.
pub(crate) fn intreal(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "INTREAL requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    // Simply return the argument unchanged
    Ok(args[0].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typeof() {
        assert_eq!(typeof_func(&[SqlValue::Null]).unwrap(), SqlValue::Varchar("null".into()));
        assert_eq!(
            typeof_func(&[SqlValue::Integer(42)]).unwrap(),
            SqlValue::Varchar("integer".into())
        );
        assert_eq!(
            typeof_func(&[SqlValue::Numeric(3.5)]).unwrap(),
            SqlValue::Varchar("real".into())
        );
        assert_eq!(
            typeof_func(&[SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Varchar("text".into())
        );
        // Test vector type returns "blob"
        assert_eq!(
            typeof_func(&[SqlValue::Vector(vec![1.0, 2.0, 3.0])]).unwrap(),
            SqlValue::Varchar("blob".into())
        );
    }

    #[test]
    fn test_toreal() {
        // NULL returns NULL
        assert_eq!(toreal(&[SqlValue::Null]).unwrap(), SqlValue::Null);

        // Integer to real
        assert_eq!(toreal(&[SqlValue::Integer(123)]).unwrap(), SqlValue::Real(123.0));

        // Float passthrough
        assert_eq!(toreal(&[SqlValue::Real(2.5)]).unwrap(), SqlValue::Real(2.5));

        // String to real
        assert_eq!(
            toreal(&[SqlValue::Varchar("123.456".into())]).unwrap(),
            SqlValue::Real(123.456)
        );

        // Non-numeric string returns 0.0
        assert_eq!(toreal(&[SqlValue::Varchar("abc".into())]).unwrap(), SqlValue::Real(0.0));

        // Empty string returns 0.0
        assert_eq!(toreal(&[SqlValue::Varchar("".into())]).unwrap(), SqlValue::Real(0.0));

        // Boolean conversion
        assert_eq!(toreal(&[SqlValue::Boolean(true)]).unwrap(), SqlValue::Real(1.0));
        assert_eq!(toreal(&[SqlValue::Boolean(false)]).unwrap(), SqlValue::Real(0.0));

        // Negative number
        assert_eq!(toreal(&[SqlValue::Integer(-42)]).unwrap(), SqlValue::Real(-42.0));

        // String with whitespace
        assert_eq!(toreal(&[SqlValue::Varchar("  2.5  ".into())]).unwrap(), SqlValue::Real(2.5));
    }

    #[test]
    fn test_tointeger() {
        // NULL returns NULL
        assert_eq!(tointeger(&[SqlValue::Null]).unwrap(), SqlValue::Null);

        // Integer passthrough
        assert_eq!(tointeger(&[SqlValue::Integer(123)]).unwrap(), SqlValue::Integer(123));

        // Float to integer (truncation)
        assert_eq!(tointeger(&[SqlValue::Real(3.7)]).unwrap(), SqlValue::Integer(3));
        assert_eq!(tointeger(&[SqlValue::Real(-3.7)]).unwrap(), SqlValue::Integer(-3));

        // String to integer
        assert_eq!(tointeger(&[SqlValue::Varchar("456".into())]).unwrap(), SqlValue::Integer(456));

        // String with decimal (truncation)
        assert_eq!(
            tointeger(&[SqlValue::Varchar("123.789".into())]).unwrap(),
            SqlValue::Integer(123)
        );

        // Non-numeric string returns 0
        assert_eq!(tointeger(&[SqlValue::Varchar("abc".into())]).unwrap(), SqlValue::Integer(0));

        // Empty string returns 0
        assert_eq!(tointeger(&[SqlValue::Varchar("".into())]).unwrap(), SqlValue::Integer(0));

        // Boolean conversion
        assert_eq!(tointeger(&[SqlValue::Boolean(true)]).unwrap(), SqlValue::Integer(1));
        assert_eq!(tointeger(&[SqlValue::Boolean(false)]).unwrap(), SqlValue::Integer(0));

        // Negative number
        assert_eq!(tointeger(&[SqlValue::Integer(-42)]).unwrap(), SqlValue::Integer(-42));

        // String with whitespace
        assert_eq!(
            tointeger(&[SqlValue::Varchar("  42  ".into())]).unwrap(),
            SqlValue::Integer(42)
        );
    }

    #[test]
    fn test_intreal() {
        // Integer passes through
        assert_eq!(intreal(&[SqlValue::Integer(42)]).unwrap(), SqlValue::Integer(42));

        // Real passes through
        assert_eq!(intreal(&[SqlValue::Numeric(2.5)]).unwrap(), SqlValue::Numeric(2.5));

        // NULL passes through
        assert_eq!(intreal(&[SqlValue::Null]).unwrap(), SqlValue::Null);

        // String passes through
        assert_eq!(
            intreal(&[SqlValue::Varchar("test".into())]).unwrap(),
            SqlValue::Varchar("test".into())
        );

        // Wrong number of arguments
        assert!(intreal(&[]).is_err());
        assert!(intreal(&[SqlValue::Integer(1), SqlValue::Integer(2)]).is_err());
    }
}
