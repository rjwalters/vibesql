//! SQLite compatibility functions
//!
//! This module implements SQLite-specific scalar functions needed for TCL test compatibility.
//! These functions follow SQLite's exact semantics as documented at:
//! https://www.sqlite.org/lang_corefunc.html

use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

/// TYPEOF(x) - Return the type name of the expression
///
/// SQLite returns one of: "null", "integer", "real", "text", "blob"
/// We map VibeSQL types to these SQLite type names.
pub(super) fn typeof_func(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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
    };

    Ok(SqlValue::Varchar(type_name.into()))
}

/// LIKELY(x) - Query planner hint that x is usually true
///
/// This is a no-op that returns its argument unchanged.
/// The hint is used by SQLite's query planner but has no effect on the result.
pub(super) fn likely(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LIKELY requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    Ok(args[0].clone())
}

/// UNLIKELY(x) - Query planner hint that x is usually false
///
/// This is a no-op that returns its argument unchanged.
/// The hint is used by SQLite's query planner but has no effect on the result.
pub(super) fn unlikely(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UNLIKELY requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    Ok(args[0].clone())
}

/// LIKELIHOOD(x, p) - Query planner hint with probability
///
/// This is a no-op that returns the first argument unchanged.
/// The second argument p is a probability between 0.0 and 1.0 (ignored).
pub(super) fn likelihood(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LIKELIHOOD requires exactly 2 arguments, got {}",
            args.len()
        )));
    }
    // Just return the first argument, ignoring the probability hint
    Ok(args[0].clone())
}

/// IIF(condition, true_value, false_value) - Inline if (SQLite ternary)
///
/// Equivalent to CASE WHEN condition THEN true_value ELSE false_value END
/// Also equivalent to IF(condition, true_value, false_value)
pub(super) fn iif(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "IIF requires exactly 3 arguments, got {}",
            args.len()
        )));
    }

    // SQLite's IIF treats any non-zero, non-NULL value as true
    let condition = &args[0];
    let is_true = match condition {
        SqlValue::Null => false,
        SqlValue::Boolean(b) => *b,
        SqlValue::Integer(i) => *i != 0,
        SqlValue::Bigint(i) => *i != 0,
        SqlValue::Smallint(i) => *i != 0,
        SqlValue::Unsigned(u) => *u != 0,
        SqlValue::Real(r) => *r != 0.0,
        SqlValue::Double(d) => *d != 0.0,
        SqlValue::Numeric(n) => *n != 0.0,
        SqlValue::Float(f) => *f != 0.0,
        // Non-empty strings are truthy in SQLite
        SqlValue::Varchar(s) | SqlValue::Character(s) => !s.is_empty(),
        _ => true, // Other non-null values are truthy
    };

    if is_true {
        Ok(args[1].clone())
    } else {
        Ok(args[2].clone())
    }
}

/// IFNULL(x, y) - Return y if x is NULL, otherwise return x
///
/// This is an alias for COALESCE(x, y) with exactly 2 arguments.
pub(super) fn ifnull(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "IFNULL requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    if matches!(args[0], SqlValue::Null) {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

/// HEX(x) - Convert blob/string to hexadecimal
///
/// Returns an upper-case hexadecimal string representation of the argument.
/// For strings, it converts each byte to its hex representation.
/// For blobs, it converts the raw bytes.
pub(super) fn hex(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "HEX requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let hex_string: String = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Vector(floats) => {
            // Convert vector to bytes first
            floats
                .iter()
                .flat_map(|f| f.to_le_bytes())
                .map(|b| format!("{:02X}", b))
                .collect()
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            s.as_bytes().iter().map(|b| format!("{:02X}", b)).collect()
        }
        SqlValue::Integer(i) => format!("{:X}", i),
        SqlValue::Bigint(i) => format!("{:X}", i),
        SqlValue::Smallint(i) => format!("{:X}", i),
        SqlValue::Unsigned(u) => format!("{:X}", u),
        _ => {
            // For other types, convert to string first then to hex
            let s = args[0].to_string();
            s.as_bytes().iter().map(|b| format!("{:02X}", b)).collect()
        }
    };

    Ok(SqlValue::Varchar(hex_string.into()))
}

/// UNHEX(x) - Convert hexadecimal string to blob
///
/// Returns a blob containing the binary data represented by the hexadecimal string.
/// Returns NULL if the input is not a valid hexadecimal string.
/// Note: VibeSQL doesn't have a Blob type, so we return the result as a Vector of bytes.
pub(super) fn unhex(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UNHEX requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let hex_str = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "UNHEX argument must be a string".to_string(),
            ));
        }
    };

    // SQLite returns NULL for odd-length hex strings
    if hex_str.len() % 2 != 0 {
        return Ok(SqlValue::Null);
    }

    // Parse hex string to bytes and return as a string (since we don't have Blob type)
    let mut bytes = Vec::with_capacity(hex_str.len() / 2);
    let chars: Vec<char> = hex_str.chars().collect();

    for chunk in chars.chunks(2) {
        let high = match chunk[0].to_digit(16) {
            Some(d) => d as u8,
            None => return Ok(SqlValue::Null), // Invalid hex character
        };
        let low = match chunk[1].to_digit(16) {
            Some(d) => d as u8,
            None => return Ok(SqlValue::Null), // Invalid hex character
        };
        bytes.push((high << 4) | low);
    }

    // Return as Varchar since VibeSQL doesn't have a Blob type
    // Convert bytes to a string (this may contain non-UTF8 characters)
    let result = String::from_utf8_lossy(&bytes).to_string();
    Ok(SqlValue::Varchar(result.into()))
}

/// ZEROBLOB(n) - Create a blob of n zero bytes
///
/// Returns a blob consisting of n zero-valued bytes.
/// Note: VibeSQL doesn't have a Blob type, so we return as a string of null characters.
pub(super) fn zeroblob(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ZEROBLOB requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let n = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(i) => *i as usize,
        SqlValue::Bigint(i) => *i as usize,
        SqlValue::Smallint(i) => *i as usize,
        SqlValue::Unsigned(u) => *u as usize,
        SqlValue::Numeric(n) => *n as usize,
        SqlValue::Real(r) => *r as usize,
        SqlValue::Double(d) => *d as usize,
        SqlValue::Float(f) => *f as usize,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ZEROBLOB argument must be numeric".to_string(),
            ));
        }
    };

    // SQLite limits blob size, we'll use a reasonable limit
    const MAX_BLOB_SIZE: usize = 1_000_000_000; // 1GB
    if n > MAX_BLOB_SIZE {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ZEROBLOB size {} exceeds maximum {}",
            n, MAX_BLOB_SIZE
        )));
    }

    // Return as a string of null characters (since VibeSQL doesn't have Blob type)
    let result: String = std::iter::repeat('\0').take(n).collect();
    Ok(SqlValue::Varchar(result.into()))
}

/// UNICODE(x) - Return the Unicode code point of the first character
///
/// Returns the numeric unicode code point of the first character of string x.
/// Returns NULL if the argument is NULL or an empty string.
pub(super) fn unicode(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UNICODE requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let s = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "UNICODE argument must be a string".to_string(),
            ));
        }
    };

    match s.chars().next() {
        Some(c) => Ok(SqlValue::Integer(c as i64)),
        None => Ok(SqlValue::Null), // Empty string returns NULL
    }
}

/// CHAR(x1, x2, ...) - Return string from Unicode code points
///
/// Returns a string composed of characters having the unicode code point values
/// given by the arguments. NULL arguments are skipped.
pub(super) fn char_func(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Ok(SqlValue::Varchar("".into()));
    }

    let mut result = String::with_capacity(args.len());

    for arg in args {
        let code_point = match arg {
            SqlValue::Null => continue, // Skip NULL arguments
            SqlValue::Integer(i) => *i as u32,
            SqlValue::Bigint(i) => *i as u32,
            SqlValue::Smallint(i) => *i as u32,
            SqlValue::Unsigned(u) => *u as u32,
            SqlValue::Numeric(n) => *n as u32,
            SqlValue::Real(r) => *r as u32,
            SqlValue::Double(d) => *d as u32,
            SqlValue::Float(f) => *f as u32,
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "CHAR arguments must be numeric".to_string(),
                ));
            }
        };

        if let Some(c) = char::from_u32(code_point) {
            result.push(c);
        }
        // Invalid code points are silently skipped (SQLite behavior)
    }

    Ok(SqlValue::Varchar(result.into()))
}

/// PRINTF(format, ...) - Formatted string output
///
/// Returns a string formatted according to the format string, similar to C's printf.
/// Supports: %d, %i (integer), %f (float), %e, %E (scientific), %s (string),
/// %x, %X (hex), %o (octal), %c (character), %% (literal %)
pub(super) fn printf(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "PRINTF requires at least 1 argument (format string)".to_string(),
        ));
    }

    let format_str = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "PRINTF format must be a string".to_string(),
            ));
        }
    };

    let format_args = &args[1..];
    let mut result = String::new();
    let mut arg_index = 0;
    let mut chars = format_str.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '%' {
            result.push(c);
            continue;
        }

        // Parse format specifier
        let next = chars.next();
        match next {
            Some('%') => result.push('%'),
            Some('d') | Some('i') => {
                // Integer
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_int(&format_args[arg_index]);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('f') => {
                // Float (default precision 6)
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_float(&format_args[arg_index], 6);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('e') => {
                // Scientific notation (lowercase)
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_scientific(&format_args[arg_index], false);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('E') => {
                // Scientific notation (uppercase)
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_scientific(&format_args[arg_index], true);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('s') => {
                // String
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_string(&format_args[arg_index]);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('x') => {
                // Hex (lowercase)
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_hex(&format_args[arg_index], false);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('X') => {
                // Hex (uppercase)
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_hex(&format_args[arg_index], true);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('o') => {
                // Octal
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_octal(&format_args[arg_index]);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some('c') => {
                // Character
                if arg_index >= format_args.len() {
                    result.push_str("(null)");
                } else {
                    let val = format_char(&format_args[arg_index]);
                    result.push_str(&val);
                    arg_index += 1;
                }
            }
            Some(other) => {
                // Unknown format specifier - include as-is
                result.push('%');
                result.push(other);
            }
            None => {
                // Trailing % at end of string
                result.push('%');
            }
        }
    }

    Ok(SqlValue::Varchar(result.into()))
}

// Helper functions for PRINTF

fn format_int(val: &SqlValue) -> String {
    match val {
        SqlValue::Null => "(null)".to_string(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Bigint(i) => i.to_string(),
        SqlValue::Smallint(i) => i.to_string(),
        SqlValue::Numeric(n) => (*n as i64).to_string(),
        SqlValue::Real(r) => (*r as i64).to_string(),
        SqlValue::Double(d) => (*d as i64).to_string(),
        SqlValue::Boolean(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => "0".to_string(),
    }
}

fn format_float(val: &SqlValue, precision: usize) -> String {
    match val {
        SqlValue::Null => "(null)".to_string(),
        SqlValue::Integer(i) => format!("{:.prec$}", *i as f64, prec = precision),
        SqlValue::Bigint(i) => format!("{:.prec$}", *i as f64, prec = precision),
        SqlValue::Smallint(i) => format!("{:.prec$}", *i as f64, prec = precision),
        SqlValue::Numeric(n) => format!("{:.prec$}", n, prec = precision),
        SqlValue::Real(r) => format!("{:.prec$}", r, prec = precision),
        SqlValue::Double(d) => format!("{:.prec$}", d, prec = precision),
        SqlValue::Float(f) => format!("{:.prec$}", f, prec = precision),
        _ => "0.000000".to_string(),
    }
}

fn format_scientific(val: &SqlValue, uppercase: bool) -> String {
    let f = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i as f64,
        SqlValue::Bigint(i) => *i as f64,
        SqlValue::Smallint(i) => *i as f64,
        SqlValue::Numeric(n) => *n,
        SqlValue::Real(r) => *r as f64,
        SqlValue::Double(d) => *d,
        SqlValue::Float(f) => *f as f64,
        _ => 0.0,
    };

    if uppercase {
        format!("{:E}", f)
    } else {
        format!("{:e}", f)
    }
}

fn format_string(val: &SqlValue) -> String {
    match val {
        SqlValue::Null => "(null)".to_string(),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        _ => val.to_string(),
    }
}

fn format_hex(val: &SqlValue, uppercase: bool) -> String {
    let i = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Numeric(n) => *n as i64,
        SqlValue::Real(r) => *r as i64,
        SqlValue::Double(d) => *d as i64,
        _ => 0,
    };

    if uppercase {
        format!("{:X}", i)
    } else {
        format!("{:x}", i)
    }
}

fn format_octal(val: &SqlValue) -> String {
    let i = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Numeric(n) => *n as i64,
        SqlValue::Real(r) => *r as i64,
        SqlValue::Double(d) => *d as i64,
        _ => 0,
    };

    format!("{:o}", i)
}

fn format_char(val: &SqlValue) -> String {
    let code = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i as u32,
        SqlValue::Bigint(i) => *i as u32,
        SqlValue::Smallint(i) => *i as u32,
        SqlValue::Numeric(n) => *n as u32,
        _ => return "".to_string(),
    };

    char::from_u32(code).map(|c| c.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typeof() {
        assert_eq!(
            typeof_func(&[SqlValue::Null]).unwrap(),
            SqlValue::Varchar("null".into())
        );
        assert_eq!(
            typeof_func(&[SqlValue::Integer(42)]).unwrap(),
            SqlValue::Varchar("integer".into())
        );
        assert_eq!(
            typeof_func(&[SqlValue::Numeric(3.14)]).unwrap(),
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
    fn test_likely_unlikely_likelihood() {
        let val = SqlValue::Boolean(true);
        assert_eq!(likely(&[val.clone()]).unwrap(), val);
        assert_eq!(unlikely(&[val.clone()]).unwrap(), val);
        assert_eq!(likelihood(&[val.clone(), SqlValue::Numeric(0.9)]).unwrap(), val);
    }

    #[test]
    fn test_iif() {
        // True condition
        assert_eq!(
            iif(&[SqlValue::Boolean(true), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );

        // False condition
        assert_eq!(
            iif(&[SqlValue::Boolean(false), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );

        // NULL condition (treated as false)
        assert_eq!(
            iif(&[SqlValue::Null, SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );

        // Non-zero integer is truthy
        assert_eq!(
            iif(&[SqlValue::Integer(5), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );

        // Zero is falsy
        assert_eq!(
            iif(&[SqlValue::Integer(0), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_ifnull() {
        assert_eq!(
            ifnull(&[SqlValue::Null, SqlValue::Integer(42)]).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            ifnull(&[SqlValue::Integer(1), SqlValue::Integer(42)]).unwrap(),
            SqlValue::Integer(1)
        );
    }

    #[test]
    fn test_hex() {
        assert_eq!(
            hex(&[SqlValue::Varchar("abc".into())]).unwrap(),
            SqlValue::Varchar("616263".into())
        );
        assert_eq!(hex(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        // Test integer conversion
        assert_eq!(hex(&[SqlValue::Integer(255)]).unwrap(), SqlValue::Varchar("FF".into()));
    }

    #[test]
    fn test_unhex() {
        // Note: unhex returns Varchar since we don't have Blob type
        let result = unhex(&[SqlValue::Varchar("616263".into())]).unwrap();
        assert!(matches!(result, SqlValue::Varchar(_)));

        // Odd length returns NULL
        assert_eq!(unhex(&[SqlValue::Varchar("abc".into())]).unwrap(), SqlValue::Null);
        // Invalid hex returns NULL
        assert_eq!(unhex(&[SqlValue::Varchar("zz".into())]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_zeroblob() {
        // Note: zeroblob returns Varchar with null characters since we don't have Blob type
        let result = zeroblob(&[SqlValue::Integer(4)]).unwrap();
        match result {
            SqlValue::Varchar(s) => assert_eq!(s.len(), 4),
            _ => panic!("Expected Varchar"),
        }
        let result = zeroblob(&[SqlValue::Integer(0)]).unwrap();
        match result {
            SqlValue::Varchar(s) => assert_eq!(s.len(), 0),
            _ => panic!("Expected Varchar"),
        }
        assert_eq!(zeroblob(&[SqlValue::Null]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(
            unicode(&[SqlValue::Varchar("A".into())]).unwrap(),
            SqlValue::Integer(65)
        );
        assert_eq!(
            unicode(&[SqlValue::Varchar("😀".into())]).unwrap(),
            SqlValue::Integer(128512)
        );
        assert_eq!(unicode(&[SqlValue::Varchar("".into())]).unwrap(), SqlValue::Null);
        assert_eq!(unicode(&[SqlValue::Null]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_char_func() {
        assert_eq!(
            char_func(&[SqlValue::Integer(65), SqlValue::Integer(66), SqlValue::Integer(67)])
                .unwrap(),
            SqlValue::Varchar("ABC".into())
        );
        // NULL arguments are skipped
        assert_eq!(
            char_func(&[SqlValue::Integer(65), SqlValue::Null, SqlValue::Integer(67)]).unwrap(),
            SqlValue::Varchar("AC".into())
        );
        // Empty args
        assert_eq!(char_func(&[]).unwrap(), SqlValue::Varchar("".into()));
    }

    #[test]
    fn test_printf() {
        // Basic integer
        assert_eq!(
            printf(&[SqlValue::Varchar("Value: %d".into()), SqlValue::Integer(42)]).unwrap(),
            SqlValue::Varchar("Value: 42".into())
        );

        // Float
        assert_eq!(
            printf(&[SqlValue::Varchar("Pi: %f".into()), SqlValue::Numeric(3.14159)]).unwrap(),
            SqlValue::Varchar("Pi: 3.141590".into())
        );

        // String
        assert_eq!(
            printf(&[
                SqlValue::Varchar("Hello, %s!".into()),
                SqlValue::Varchar("World".into())
            ])
            .unwrap(),
            SqlValue::Varchar("Hello, World!".into())
        );

        // Hex
        assert_eq!(
            printf(&[SqlValue::Varchar("%x".into()), SqlValue::Integer(255)]).unwrap(),
            SqlValue::Varchar("ff".into())
        );
        assert_eq!(
            printf(&[SqlValue::Varchar("%X".into()), SqlValue::Integer(255)]).unwrap(),
            SqlValue::Varchar("FF".into())
        );

        // Escaped percent
        assert_eq!(
            printf(&[SqlValue::Varchar("100%%".into())]).unwrap(),
            SqlValue::Varchar("100%".into())
        );
    }
}
