//! SQLite compatibility functions
//!
//! This module implements SQLite-specific scalar functions needed for TCL test compatibility.
//! These functions follow SQLite's exact semantics as documented at:
//! https://www.sqlite.org/lang_corefunc.html

use std::borrow::Cow;

use rand::Rng;
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
        SqlValue::Blob(_) => "blob",
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

/// RANDOM() - Return a pseudo-random 64-bit signed integer
///
/// Returns a pseudo-random integer between -9223372036854775808 and +9223372036854775807.
/// The value changes on each call.
pub(super) fn random(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RANDOM takes no arguments, got {}",
            args.len()
        )));
    }

    let value: i64 = rand::rng().random();
    Ok(SqlValue::Integer(value))
}

/// RANDOMBLOB(N) - Return N bytes of pseudo-random data
///
/// Returns a blob containing N bytes of pseudo-random data.
/// SQLite returns a 1-byte blob for negative sizes, empty blob for zero.
pub(super) fn randomblob(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RANDOMBLOB requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // SQLite returns 1-byte blob for negative sizes, empty blob for zero
    let n = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(i) => {
            if *i < 0 {
                1usize // SQLite returns 1 byte for negative sizes
            } else {
                *i as usize
            }
        }
        SqlValue::Bigint(i) => {
            if *i < 0 {
                1usize
            } else {
                *i as usize
            }
        }
        SqlValue::Smallint(i) => {
            if *i < 0 {
                1usize
            } else {
                *i as usize
            }
        }
        SqlValue::Unsigned(u) => *u as usize,
        SqlValue::Numeric(n) => {
            if *n < 0.0 {
                1usize
            } else {
                *n as usize
            }
        }
        SqlValue::Real(r) => {
            if *r < 0.0 {
                1usize
            } else {
                *r as usize
            }
        }
        SqlValue::Double(d) => {
            if *d < 0.0 {
                1usize
            } else {
                *d as usize
            }
        }
        SqlValue::Float(f) => {
            if *f < 0.0 {
                1usize
            } else {
                *f as usize
            }
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "RANDOMBLOB argument must be numeric".to_string(),
            ));
        }
    };

    // SQLite limits blob size
    const MAX_BLOB_SIZE: usize = 1_000_000_000; // 1GB
    if n > MAX_BLOB_SIZE {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RANDOMBLOB size {} exceeds maximum {}",
            n, MAX_BLOB_SIZE
        )));
    }

    // Generate N random bytes
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..n).map(|_| rng.random()).collect();

    Ok(SqlValue::Blob(bytes))
}

/// HEX(x) - Convert blob/string to hexadecimal
///
/// Returns an upper-case hexadecimal string representation of the argument.
/// SQLite coerces all values to their TEXT representation first, then converts
/// each byte to its hex representation. For example, hex(255) returns "323535"
/// because 255 is first coerced to the string "255", then each character is
/// converted to hex (0x32, 0x35, 0x35).
///
/// For blob data (from randomblob/unhex), we use Latin-1 encoding where each
/// character maps to exactly one byte value (0x00-0xFF).
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
            // Convert vector (blob) to bytes first
            floats.iter().flat_map(|f| f.to_le_bytes()).map(|b| format!("{:02X}", b)).collect()
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Check if this looks like blob data (contains Latin-1 high bytes)
            // Latin-1 encoding: each char value IS the byte value
            let has_latin1_high_bytes = s.chars().any(|c| {
                let code = c as u32;
                (0x80..=0xFF).contains(&code)
            });

            if has_latin1_high_bytes {
                // This is blob data using Latin-1 encoding - convert each char to its byte value
                s.chars()
                    .map(|c| {
                        let byte = (c as u32) as u8;
                        format!("{:02X}", byte)
                    })
                    .collect()
            } else {
                // Regular UTF-8 string - use bytes directly
                s.as_bytes().iter().map(|b| format!("{:02X}", b)).collect()
            }
        }
        SqlValue::Blob(bytes) => {
            // Convert blob bytes directly to hex representation
            bytes.iter().map(|b| format!("{:02X}", b)).collect()
        }
        // SQLite coerces all other types to TEXT first, then converts to hex
        _ => {
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

    // Parse hex string to bytes
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

    Ok(SqlValue::Blob(bytes))
}

/// ZEROBLOB(n) - Create a blob of n zero bytes
///
/// Returns a blob consisting of n zero-valued bytes.
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

    Ok(SqlValue::Blob(vec![0u8; n]))
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

/// Parsed printf format specifier
#[derive(Default)]
struct FormatSpec {
    /// '-' flag: left-justify within the given field width
    left_justify: bool,
    /// '+' flag: always show sign for numeric types
    show_sign: bool,
    /// ' ' flag: use space for positive sign
    space_sign: bool,
    /// '#' flag: alternative form (0x for hex, 0 for octal - but NOT for zero values)
    alternate: bool,
    /// '0' flag: pad with zeros instead of spaces
    zero_pad: bool,
    /// Minimum field width
    width: Option<usize>,
    /// Precision (for floats: decimal places; for strings: max length)
    precision: Option<usize>,
    /// The conversion specifier character
    specifier: char,
}

/// PRINTF(format, ...) - Formatted string output
///
/// Returns a string formatted according to the format string, similar to C's printf.
/// Supports: %d, %i (integer), %f (float), %e, %E (scientific), %s (string),
/// %x, %X (hex), %o (octal), %c (character), %% (literal %)
/// Flags: - (left-justify), + (show sign), space, # (alternate form), 0 (zero-pad)
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

        // Check for %%
        if chars.peek() == Some(&'%') {
            chars.next();
            result.push('%');
            continue;
        }

        // Parse format specifier: %[flags][width][.precision]specifier
        let spec = parse_format_spec(&mut chars);

        // Format the value according to the specifier
        let formatted = if arg_index >= format_args.len() {
            "(null)".to_string()
        } else {
            let val = &format_args[arg_index];
            arg_index += 1;
            format_value(val, &spec)
        };

        result.push_str(&formatted);
    }

    Ok(SqlValue::Varchar(result.into()))
}

/// Parse a format specifier from the character stream
fn parse_format_spec(chars: &mut std::iter::Peekable<std::str::Chars>) -> FormatSpec {
    let mut spec = FormatSpec::default();

    // Parse flags
    while let Some(&c) = chars.peek() {
        match c {
            '-' => {
                spec.left_justify = true;
                chars.next();
            }
            '+' => {
                spec.show_sign = true;
                chars.next();
            }
            ' ' => {
                spec.space_sign = true;
                chars.next();
            }
            '#' => {
                spec.alternate = true;
                chars.next();
            }
            '0' => {
                spec.zero_pad = true;
                chars.next();
            }
            _ => break,
        }
    }

    // Parse width
    let mut width_str = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            width_str.push(c);
            chars.next();
        } else {
            break;
        }
    }
    if !width_str.is_empty() {
        spec.width = width_str.parse().ok();
    }

    // Parse precision
    if chars.peek() == Some(&'.') {
        chars.next();
        let mut prec_str = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                prec_str.push(c);
                chars.next();
            } else {
                break;
            }
        }
        spec.precision = Some(prec_str.parse().unwrap_or(0));
    }

    // Parse specifier
    spec.specifier = chars.next().unwrap_or('s');

    spec
}

/// Format a value according to the format specifier
fn format_value(val: &SqlValue, spec: &FormatSpec) -> String {
    let raw = match spec.specifier {
        'd' | 'i' => format_int_with_spec(val, spec),
        'f' => format_float_with_spec(val, spec),
        'e' => format_scientific_with_spec(val, false, spec),
        'E' => format_scientific_with_spec(val, true, spec),
        's' => format_string_with_spec(val, spec),
        'x' => format_hex_with_spec(val, false, spec),
        'X' => format_hex_with_spec(val, true, spec),
        'o' => format_octal_with_spec(val, spec),
        'c' => format_char(val),
        other => format!("%{}", other),
    };

    // Apply width and justification
    apply_width(&raw, spec)
}

/// Apply width and justification to a formatted string
fn apply_width(s: &str, spec: &FormatSpec) -> String {
    let width = spec.width.unwrap_or(0);
    if s.len() >= width {
        return s.to_string();
    }

    let padding = width - s.len();
    let pad_char = if spec.zero_pad && !spec.left_justify {
        '0'
    } else {
        ' '
    };

    if spec.left_justify {
        format!("{}{}", s, " ".repeat(padding))
    } else if spec.zero_pad && (s.starts_with('-') || s.starts_with('+')) {
        // For zero-padding with sign, put sign before zeros
        let (sign, rest) = s.split_at(1);
        format!(
            "{}{}{}",
            sign,
            std::iter::repeat(pad_char).take(padding).collect::<String>(),
            rest
        )
    } else {
        format!(
            "{}{}",
            std::iter::repeat(pad_char).take(padding).collect::<String>(),
            s
        )
    }
}

fn format_int_with_spec(val: &SqlValue, spec: &FormatSpec) -> String {
    let i = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Numeric(n) => *n as i64,
        SqlValue::Real(r) => *r as i64,
        SqlValue::Double(d) => *d as i64,
        SqlValue::Boolean(b) => {
            if *b {
                1
            } else {
                0
            }
        }
        _ => 0,
    };

    let abs_str = i.unsigned_abs().to_string();
    let sign = if i < 0 {
        "-"
    } else if spec.show_sign {
        "+"
    } else if spec.space_sign {
        " "
    } else {
        ""
    };

    format!("{}{}", sign, abs_str)
}

fn format_float_with_spec(val: &SqlValue, spec: &FormatSpec) -> String {
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

    let precision = spec.precision.unwrap_or(6);
    format!("{:.prec$}", f, prec = precision)
}

fn format_scientific_with_spec(val: &SqlValue, uppercase: bool, _spec: &FormatSpec) -> String {
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

fn format_string_with_spec(val: &SqlValue, spec: &FormatSpec) -> String {
    let s = match val {
        SqlValue::Null => "(null)".to_string(),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        _ => val.to_string(),
    };

    // Apply precision as max length for strings
    if let Some(prec) = spec.precision {
        s.chars().take(prec).collect()
    } else {
        s
    }
}

fn format_hex_with_spec(val: &SqlValue, uppercase: bool, spec: &FormatSpec) -> String {
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

    let hex = if uppercase {
        format!("{:X}", i)
    } else {
        format!("{:x}", i)
    };

    // Per C standard: # flag adds 0x/0X prefix, but NOT for zero values
    if spec.alternate && i != 0 {
        let prefix = if uppercase { "0X" } else { "0x" };
        format!("{}{}", prefix, hex)
    } else {
        hex
    }
}

fn format_octal_with_spec(val: &SqlValue, spec: &FormatSpec) -> String {
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

    let oct = format!("{:o}", i);

    // Per C standard: # flag adds leading 0 for octal (but not if already 0)
    if spec.alternate && i != 0 && !oct.starts_with('0') {
        format!("0{}", oct)
    } else {
        oct
    }
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

/// Format a float for QUOTE() - uses 9.0e+999 for infinity (SQLite compatibility)
fn format_float_for_quote(n: f64) -> String {
    if n.is_nan() {
        "NaN".to_string()
    } else if n.is_infinite() {
        // SQLite's quote() represents infinity as 9.0e+999
        if n > 0.0 { "9.0e+999".to_string() } else { "-9.0e+999".to_string() }
    } else {
        n.to_string()
    }
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
        SqlValue::Real(r) => Ok(SqlValue::Varchar(format_float_for_quote(*r).into())),
        SqlValue::Double(d) => Ok(SqlValue::Varchar(format_float_for_quote(*d).into())),
        SqlValue::Numeric(n) => Ok(SqlValue::Varchar(format_float_for_quote(*n).into())),
        SqlValue::Float(f) => Ok(SqlValue::Varchar(format_float_for_quote(*f as f64).into())),
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
        SqlValue::Blob(b) => {
            // Convert blob to X'...' hex format
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            Ok(SqlValue::Varchar(format!("X'{}'", hex).into()))
        }
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

/// LIKE(pattern, string) - SQL LIKE pattern matching as a function
///
/// Returns 1 if string matches pattern, 0 otherwise.
/// Pattern syntax:
/// - % matches any sequence of characters (including empty)
/// - _ matches exactly one character
///
/// LIKE is case-insensitive for ASCII letters (SQLite default)
/// Optionally takes a 3rd argument for escape character.
pub(super) fn like(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LIKE requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    // NULL propagation - SQL standard semantics
    if matches!(args[0], SqlValue::Null) || matches!(args[1], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let pattern = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        other => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "LIKE pattern must be a string, got {:?}",
                other
            )));
        }
    };

    // SQLite coerces non-string types to strings for LIKE comparison
    let text: Cow<str> = match &args[1] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => Cow::Borrowed(s.as_str()),
        SqlValue::Integer(i) => Cow::Owned(i.to_string()),
        SqlValue::Bigint(i) => Cow::Owned(i.to_string()),
        SqlValue::Smallint(i) => Cow::Owned(i.to_string()),
        SqlValue::Unsigned(u) => Cow::Owned(u.to_string()),
        SqlValue::Real(r) => Cow::Owned(r.to_string()),
        SqlValue::Double(d) => Cow::Owned(d.to_string()),
        SqlValue::Numeric(n) => Cow::Owned(n.to_string()),
        SqlValue::Float(f) => Cow::Owned(f.to_string()),
        SqlValue::Boolean(b) => Cow::Owned(if *b { "1".to_string() } else { "0".to_string() }),
        other => Cow::Owned(other.to_string()),
    };

    // Optional escape character (3rd argument)
    let escape_char: Option<char> = if args.len() >= 3 {
        match &args[2] {
            SqlValue::Varchar(s) | SqlValue::Character(s) if s.len() == 1 => s.chars().next(),
            SqlValue::Integer(n) => {
                let s = n.to_string();
                if s.len() == 1 { s.chars().next() } else { None }
            }
            _ => None,
        }
    } else {
        None
    };
    // SQLite's like() function uses case-insensitive matching by default
    let matched = crate::evaluator::pattern::like_match(&text, pattern, false, escape_char);
    Ok(SqlValue::Integer(if matched { 1 } else { 0 }))
}

/// GLOB(pattern, string) - Unix-style pattern matching
///
/// Returns 1 if string matches pattern, 0 otherwise.
/// Pattern syntax:
/// - * matches any sequence of characters (including empty)
/// - ? matches exactly one character
/// - [...] matches any character in the brackets
/// - [^...] or [!...] matches any character NOT in the brackets
///
/// GLOB is case-sensitive (unlike LIKE which is case-insensitive in SQLite)
pub(super) fn glob(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "GLOB requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // NULL propagation - SQL standard semantics
    if matches!(args[0], SqlValue::Null) || matches!(args[1], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let pattern = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        other => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "GLOB pattern must be a string, got {:?}",
                other
            )));
        }
    };

    // SQLite coerces non-string types to strings for GLOB comparison
    let text: Cow<str> = match &args[1] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => Cow::Borrowed(s.as_str()),
        SqlValue::Integer(i) => Cow::Owned(i.to_string()),
        SqlValue::Bigint(i) => Cow::Owned(i.to_string()),
        SqlValue::Smallint(i) => Cow::Owned(i.to_string()),
        SqlValue::Unsigned(u) => Cow::Owned(u.to_string()),
        SqlValue::Real(r) => Cow::Owned(r.to_string()),
        SqlValue::Double(d) => Cow::Owned(d.to_string()),
        SqlValue::Numeric(n) => Cow::Owned(n.to_string()),
        SqlValue::Float(f) => Cow::Owned(f.to_string()),
        SqlValue::Boolean(b) => Cow::Owned(if *b { "1".to_string() } else { "0".to_string() }),
        other => Cow::Owned(other.to_string()),
    };

    // Use the pattern matching function from pattern.rs
    let matched = crate::evaluator::pattern::glob_match(&text, pattern);
    Ok(SqlValue::Integer(if matched { 1 } else { 0 }))
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
    fn test_random() {
        // random() should return an integer
        let result = random(&[]).unwrap();
        assert!(matches!(result, SqlValue::Integer(_)));

        // Two calls should (almost certainly) return different values
        let result1 = random(&[]).unwrap();
        let result2 = random(&[]).unwrap();
        // This could theoretically fail but is astronomically unlikely
        assert_ne!(result1, result2);

        // Wrong number of arguments
        assert!(random(&[SqlValue::Integer(1)]).is_err());
    }

    #[test]
    fn test_randomblob() {
        // randomblob returns a Blob of N random bytes
        let result = randomblob(&[SqlValue::Integer(10)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => {
                assert_eq!(bytes.len(), 10);
            }
            _ => panic!("Expected Blob"),
        }

        // randomblob(0) returns empty blob
        let result = randomblob(&[SqlValue::Integer(0)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes.len(), 0),
            _ => panic!("Expected Blob"),
        }

        // SQLite returns 1-byte blob for negative sizes
        let result = randomblob(&[SqlValue::Integer(-5)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes.len(), 1),
            _ => panic!("Expected Blob"),
        }

        // NULL returns NULL
        assert_eq!(randomblob(&[SqlValue::Null]).unwrap(), SqlValue::Null);

        // Wrong number of arguments
        assert!(randomblob(&[]).is_err());
    }

    #[test]
    fn test_hex() {
        // Basic string conversion
        assert_eq!(
            hex(&[SqlValue::Varchar("abc".into())]).unwrap(),
            SqlValue::Varchar("616263".into())
        );
        assert_eq!(hex(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        // SQLite coerces integers to text first: hex(255) = hex("255") = "323535"
        // "255" = bytes [0x32, 0x35, 0x35]
        assert_eq!(hex(&[SqlValue::Integer(255)]).unwrap(), SqlValue::Varchar("323535".into()));
        // Uppercase hex string as stated in SQLite docs
        assert_eq!(
            hex(&[SqlValue::Varchar("ABC".into())]).unwrap(),
            SqlValue::Varchar("414243".into())
        );
    }

    #[test]
    fn test_unhex() {
        // unhex returns a Blob
        let result = unhex(&[SqlValue::Varchar("616263".into())]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes, vec![0x61, 0x62, 0x63]),
            _ => panic!("Expected Blob"),
        }

        // Odd length returns NULL
        assert_eq!(unhex(&[SqlValue::Varchar("abc".into())]).unwrap(), SqlValue::Null);
        // Invalid hex returns NULL
        assert_eq!(unhex(&[SqlValue::Varchar("zz".into())]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_zeroblob() {
        // zeroblob returns a Blob of zero bytes
        let result = zeroblob(&[SqlValue::Integer(4)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes, vec![0, 0, 0, 0]),
            _ => panic!("Expected Blob"),
        }
        let result = zeroblob(&[SqlValue::Integer(0)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes.len(), 0),
            _ => panic!("Expected Blob"),
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

    #[test]
    fn test_like() {
        // Basic wildcard matches
        assert_eq!(
            like(&[SqlValue::Varchar("%ello".into()), SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            like(&[SqlValue::Varchar("h_llo".into()), SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            like(&[SqlValue::Varchar("h%o".into()), SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Integer(1)
        );

        // Case insensitivity (SQLite default)
        assert_eq!(
            like(&[SqlValue::Varchar("HELLO".into()), SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            like(&[SqlValue::Varchar("hello".into()), SqlValue::Varchar("HELLO".into())]).unwrap(),
            SqlValue::Integer(1)
        );

        // Exact match
        assert_eq!(
            like(&[SqlValue::Varchar("abc".into()), SqlValue::Varchar("abc".into())]).unwrap(),
            SqlValue::Integer(1)
        );

        // No match
        assert_eq!(
            like(&[SqlValue::Varchar("___".into()), SqlValue::Varchar("ab".into())]).unwrap(),
            SqlValue::Integer(0)
        );

        // NULL propagation
        assert_eq!(like(&[SqlValue::Null, SqlValue::Varchar("hello".into())]).unwrap(), SqlValue::Null);
        assert_eq!(like(&[SqlValue::Varchar("hello".into()), SqlValue::Null]).unwrap(), SqlValue::Null);

        // Wrong number of arguments
        assert!(like(&[]).is_err());
        assert!(like(&[SqlValue::Varchar("a".into())]).is_err());
    }

    #[test]
    fn test_glob() {
        // Basic wildcard matches
        assert_eq!(
            glob(&[SqlValue::Varchar("*ello".into()), SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            glob(&[SqlValue::Varchar("h?llo".into()), SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            glob(&[SqlValue::Varchar("h*o".into()), SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Integer(1)
        );

        // Exact match
        assert_eq!(
            glob(&[SqlValue::Varchar("abc".into()), SqlValue::Varchar("abc".into())]).unwrap(),
            SqlValue::Integer(1)
        );

        // Case sensitivity (glob is case-sensitive)
        assert_eq!(
            glob(&[SqlValue::Varchar("abc".into()), SqlValue::Varchar("ABC".into())]).unwrap(),
            SqlValue::Integer(0)
        );

        // Character class
        assert_eq!(
            glob(&[SqlValue::Varchar("[abc]".into()), SqlValue::Varchar("a".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            glob(&[SqlValue::Varchar("[abc]".into()), SqlValue::Varchar("d".into())]).unwrap(),
            SqlValue::Integer(0)
        );

        // Character range
        assert_eq!(
            glob(&[SqlValue::Varchar("[a-z]".into()), SqlValue::Varchar("m".into())]).unwrap(),
            SqlValue::Integer(1)
        );

        // Negated character class
        assert_eq!(
            glob(&[SqlValue::Varchar("[^abc]".into()), SqlValue::Varchar("d".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            glob(&[SqlValue::Varchar("[^abc]".into()), SqlValue::Varchar("a".into())]).unwrap(),
            SqlValue::Integer(0)
        );

        // No match
        assert_eq!(
            glob(&[SqlValue::Varchar("???".into()), SqlValue::Varchar("ab".into())]).unwrap(),
            SqlValue::Integer(0)
        );

        // NULL propagation
        assert_eq!(glob(&[SqlValue::Null, SqlValue::Varchar("hello".into())]).unwrap(), SqlValue::Null);
        assert_eq!(glob(&[SqlValue::Varchar("hello".into()), SqlValue::Null]).unwrap(), SqlValue::Null);

        // Wrong number of arguments
        assert!(glob(&[]).is_err());
        assert!(glob(&[SqlValue::Varchar("a".into())]).is_err());
    }
}
