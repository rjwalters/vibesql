//! SQLite compatibility functions
//!
//! This module implements SQLite-specific scalar functions needed for TCL test compatibility.
//! These functions follow SQLite's exact semantics as documented at:
//! https://www.sqlite.org/lang_corefunc.html

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

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
            floats.iter().flat_map(|f| f.to_le_bytes()).map(|b| format!("{:02X}", b)).collect()
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
    let result = "\0".repeat(n);
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

/// TOREAL(x) - Convert value to floating-point number (SQLite REAL type)
///
/// Converts the argument to a floating-point number. This is used primarily in SQLite
/// test suites for explicit type conversion. NULL input returns NULL.
/// String inputs are parsed as floating-point numbers.
/// Integer inputs are converted to floating-point.
///
/// Reference: https://www.sqlite.org/lang_corefunc.html
pub(super) fn toreal(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TOREAL requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Real(r) => Ok(SqlValue::Real(*r)),
        SqlValue::Double(d) => Ok(SqlValue::Real(*d as f32)),
        SqlValue::Float(f) => Ok(SqlValue::Real(*f)),
        SqlValue::Numeric(n) => Ok(SqlValue::Real(*n as f32)),
        SqlValue::Integer(i) => Ok(SqlValue::Real(*i as f32)),
        SqlValue::Bigint(i) => Ok(SqlValue::Real(*i as f32)),
        SqlValue::Smallint(i) => Ok(SqlValue::Real(*i as f32)),
        SqlValue::Unsigned(u) => Ok(SqlValue::Real(*u as f32)),
        SqlValue::Boolean(b) => Ok(SqlValue::Real(if *b { 1.0 } else { 0.0 })),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Try to parse string as a number
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(SqlValue::Real(0.0));
            }
            match trimmed.parse::<f64>() {
                Ok(f) => Ok(SqlValue::Real(f as f32)),
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
pub(super) fn tointeger(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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

/// CONCAT_WS(separator, str1, str2, ...) - Concatenate with separator
///
/// Concatenates strings with the first argument as separator.
/// NULL values are skipped (not included in result).
/// Returns NULL if the separator is NULL.
pub(super) fn concat_ws(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CONCAT_WS requires at least 1 argument (separator)".to_string(),
        ));
    }

    // First argument is the separator
    let separator = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => other.to_string(),
    };

    // Remaining arguments are the strings to concatenate
    let mut parts: Vec<String> = Vec::new();
    for arg in &args[1..] {
        match arg {
            SqlValue::Null => continue, // Skip NULL values
            SqlValue::Varchar(s) | SqlValue::Character(s) => parts.push(s.to_string()),
            other => parts.push(other.to_string()),
        }
    }

    Ok(SqlValue::Varchar(parts.join(&separator).into()))
}

/// QUOTE(x) - Return SQL literal representation of a value
///
/// Returns a string which is the value of its argument suitable for
/// inclusion in another SQL statement. Strings are surrounded by
/// single-quotes with escapes on interior quotes.
pub(super) fn quote(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "QUOTE requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Varchar("NULL".into())),
        SqlValue::Integer(i) => Ok(SqlValue::Varchar(i.to_string().into())),
        SqlValue::Bigint(i) => Ok(SqlValue::Varchar(i.to_string().into())),
        SqlValue::Smallint(i) => Ok(SqlValue::Varchar(i.to_string().into())),
        SqlValue::Unsigned(u) => Ok(SqlValue::Varchar(u.to_string().into())),
        SqlValue::Real(r) => Ok(SqlValue::Varchar(r.to_string().into())),
        SqlValue::Double(d) => Ok(SqlValue::Varchar(d.to_string().into())),
        SqlValue::Numeric(n) => Ok(SqlValue::Varchar(n.to_string().into())),
        SqlValue::Float(f) => Ok(SqlValue::Varchar(f.to_string().into())),
        SqlValue::Boolean(b) => Ok(SqlValue::Varchar(if *b { "1" } else { "0" }.into())),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Escape single quotes by doubling them
            let escaped = s.replace('\'', "''");
            Ok(SqlValue::Varchar(format!("'{}'", escaped).into()))
        }
        SqlValue::Vector(floats) => {
            // Convert blob to X'...' hex format
            let hex: String =
                floats.iter().flat_map(|f| f.to_le_bytes()).map(|b| format!("{:02X}", b)).collect();
            Ok(SqlValue::Varchar(format!("X'{}'", hex).into()))
        }
        SqlValue::Date(d) => Ok(SqlValue::Varchar(format!("'{}'", d).into())),
        SqlValue::Time(t) => Ok(SqlValue::Varchar(format!("'{}'", t).into())),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Varchar(format!("'{}'", ts).into())),
        SqlValue::Interval(i) => Ok(SqlValue::Varchar(format!("'{}'", i).into())),
    }
}

/// INTREAL(x) - SQLite test function for integer/real type testing
///
/// This is a SQLite internal test function. It returns the value unchanged
/// but with type affinity information preserved. In VibeSQL, we simply
/// return the value as-is since we don't have the same type affinity system.
pub(super) fn intreal(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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
    fn test_likely_unlikely_likelihood() {
        let val = SqlValue::Boolean(true);
        assert_eq!(likely(std::slice::from_ref(&val)).unwrap(), val);
        assert_eq!(unlikely(std::slice::from_ref(&val)).unwrap(), val);
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
        assert_eq!(unicode(&[SqlValue::Varchar("A".into())]).unwrap(), SqlValue::Integer(65));
        assert_eq!(unicode(&[SqlValue::Varchar("😀".into())]).unwrap(), SqlValue::Integer(128512));
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
            printf(&[SqlValue::Varchar("Value: %f".into()), SqlValue::Numeric(1.5)]).unwrap(),
            SqlValue::Varchar("Value: 1.500000".into())
        );

        // String
        assert_eq!(
            printf(&[SqlValue::Varchar("Hello, %s!".into()), SqlValue::Varchar("World".into())])
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
    fn test_concat_ws() {
        // Basic concatenation with comma separator
        assert_eq!(
            concat_ws(&[
                SqlValue::Varchar(",".into()),
                SqlValue::Varchar("a".into()),
                SqlValue::Varchar("b".into()),
                SqlValue::Varchar("c".into())
            ])
            .unwrap(),
            SqlValue::Varchar("a,b,c".into())
        );

        // NULL separator returns NULL
        assert_eq!(
            concat_ws(&[
                SqlValue::Null,
                SqlValue::Varchar("a".into()),
                SqlValue::Varchar("b".into())
            ])
            .unwrap(),
            SqlValue::Null
        );

        // NULL values in strings are skipped
        assert_eq!(
            concat_ws(&[
                SqlValue::Varchar(",".into()),
                SqlValue::Varchar("a".into()),
                SqlValue::Null,
                SqlValue::Varchar("c".into())
            ])
            .unwrap(),
            SqlValue::Varchar("a,c".into())
        );

        // Empty separator
        assert_eq!(
            concat_ws(&[
                SqlValue::Varchar("".into()),
                SqlValue::Varchar("a".into()),
                SqlValue::Varchar("b".into())
            ])
            .unwrap(),
            SqlValue::Varchar("ab".into())
        );

        // Single string (no separator used)
        assert_eq!(
            concat_ws(&[SqlValue::Varchar(",".into()), SqlValue::Varchar("only".into())]).unwrap(),
            SqlValue::Varchar("only".into())
        );

        // No strings (empty result)
        assert_eq!(
            concat_ws(&[SqlValue::Varchar(",".into())]).unwrap(),
            SqlValue::Varchar("".into())
        );

        // Integers are converted to strings
        assert_eq!(
            concat_ws(&[
                SqlValue::Varchar("-".into()),
                SqlValue::Integer(1),
                SqlValue::Integer(2),
                SqlValue::Integer(3)
            ])
            .unwrap(),
            SqlValue::Varchar("1-2-3".into())
        );
    }

    #[test]
    fn test_quote() {
        // NULL
        assert_eq!(quote(&[SqlValue::Null]).unwrap(), SqlValue::Varchar("NULL".into()));

        // Integer
        assert_eq!(quote(&[SqlValue::Integer(123)]).unwrap(), SqlValue::Varchar("123".into()));

        // String without quotes
        assert_eq!(
            quote(&[SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Varchar("'hello'".into())
        );

        // String with embedded single quote
        assert_eq!(
            quote(&[SqlValue::Varchar("it's".into())]).unwrap(),
            SqlValue::Varchar("'it''s'".into())
        );

        // Float
        assert_eq!(quote(&[SqlValue::Numeric(2.5)]).unwrap(), SqlValue::Varchar("2.5".into()));

        // Boolean
        assert_eq!(quote(&[SqlValue::Boolean(true)]).unwrap(), SqlValue::Varchar("1".into()));
        assert_eq!(quote(&[SqlValue::Boolean(false)]).unwrap(), SqlValue::Varchar("0".into()));

        // Empty string
        assert_eq!(quote(&[SqlValue::Varchar("".into())]).unwrap(), SqlValue::Varchar("''".into()));
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
