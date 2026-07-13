use crate::errors::ExecutorError;

// ========================================================================
// Type Coercion Helper Functions
// ========================================================================

/// Check if a value is an exact numeric type (SMALLINT, INTEGER, BIGINT, UNSIGNED)
pub(crate) fn is_exact_numeric(value: &vibesql_types::SqlValue) -> bool {
    matches!(
        value,
        vibesql_types::SqlValue::Smallint(_)
            | vibesql_types::SqlValue::Integer(_)
            | vibesql_types::SqlValue::Bigint(_)
            | vibesql_types::SqlValue::Unsigned(_)
    )
}

/// Check if a value is an approximate numeric type (FLOAT, REAL, DOUBLE)
pub(crate) fn is_approximate_numeric(value: &vibesql_types::SqlValue) -> bool {
    matches!(
        value,
        vibesql_types::SqlValue::Float(_)
            | vibesql_types::SqlValue::Real(_)
            | vibesql_types::SqlValue::Double(_)
    )
}

/// Convert exact numeric types to i64 for comparison
pub(crate) fn to_i64(value: &vibesql_types::SqlValue) -> Result<i64, ExecutorError> {
    match value {
        vibesql_types::SqlValue::Smallint(n) => Ok(*n as i64),
        vibesql_types::SqlValue::Integer(n) => Ok(*n),
        vibesql_types::SqlValue::Bigint(n) => Ok(*n),
        vibesql_types::SqlValue::Unsigned(n) => Ok(*n as i64), /* Note: may overflow for large */
        // unsigned
        vibesql_types::SqlValue::Numeric(f) => Ok(*f as i64),
        vibesql_types::SqlValue::Float(f) => Ok(*f as i64),
        vibesql_types::SqlValue::Real(f) => Ok(*f as i64),
        vibesql_types::SqlValue::Double(f) => Ok(*f as i64),
        vibesql_types::SqlValue::Boolean(b) => Ok(if *b { 1 } else { 0 }),
        // values
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "numeric_conversion".to_string(),
            right: vibesql_types::SqlValue::Null,
        }),
    }
}

/// Convert approximate numeric types to f64 for comparison
pub(crate) fn to_f64(value: &vibesql_types::SqlValue) -> Result<f64, ExecutorError> {
    match value {
        vibesql_types::SqlValue::Float(n) => Ok(*n as f64),
        vibesql_types::SqlValue::Real(n) => Ok(*n as f64),
        vibesql_types::SqlValue::Double(n) => Ok(*n),
        vibesql_types::SqlValue::Integer(n) => Ok(*n as f64),
        vibesql_types::SqlValue::Smallint(n) => Ok(*n as f64),
        vibesql_types::SqlValue::Bigint(n) => Ok(*n as f64),
        vibesql_types::SqlValue::Unsigned(n) => Ok(*n as f64),
        vibesql_types::SqlValue::Numeric(f) => Ok(*f),
        vibesql_types::SqlValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "numeric_conversion".to_string(),
            right: vibesql_types::SqlValue::Null,
        }),
    }
}

/// SQLite-compatible string-to-number coercion for arithmetic operations
///
/// SQLite rules for implicit string-to-number conversion:
/// 1. Leading whitespace is ignored
/// 2. Optional sign (+/-)
/// 3. Numeric prefix is extracted: '123abc' → 123
/// 4. Non-numeric strings convert to 0: 'abc' → 0
/// 5. Empty string converts to 0
/// 6. If the string contains a decimal point or exponent, it's treated as float
///
/// Returns (integer_value, float_value, is_float)
pub(crate) fn string_to_number(s: &str) -> (i64, f64, bool) {
    let trimmed = s.trim_start();

    if trimmed.is_empty() {
        return (0, 0.0, false);
    }

    // Check for sign
    let (sign, rest) = match trimmed.chars().next() {
        Some('-') => (-1i64, &trimmed[1..]),
        Some('+') => (1i64, &trimmed[1..]),
        _ => (1i64, trimmed),
    };

    // Find the numeric prefix
    let mut has_decimal = false;
    let mut has_exponent = false;
    let mut end_idx = 0;

    let chars: Vec<char> = rest.chars().collect();
    let len = chars.len();

    while end_idx < len {
        let c = chars[end_idx];
        if c.is_ascii_digit() {
            end_idx += 1;
        } else if c == '.' && !has_decimal && !has_exponent {
            has_decimal = true;
            end_idx += 1;
        } else if (c == 'e' || c == 'E') && !has_exponent && end_idx > 0 {
            // Check if exponent is followed by optional sign and digits
            has_exponent = true;
            end_idx += 1;
            // Allow optional sign after exponent
            if end_idx < len && (chars[end_idx] == '+' || chars[end_idx] == '-') {
                end_idx += 1;
            }
            // Must have at least one digit after exponent
            if end_idx < len && chars[end_idx].is_ascii_digit() {
                while end_idx < len && chars[end_idx].is_ascii_digit() {
                    end_idx += 1;
                }
            } else {
                // Invalid exponent, backtrack
                has_exponent = false;
                end_idx -=
                    if end_idx > 0 && (chars[end_idx - 1] == '+' || chars[end_idx - 1] == '-') {
                        2
                    } else {
                        1
                    };
                break;
            }
        } else {
            break;
        }
    }

    if end_idx == 0 {
        // No numeric prefix found
        return (0, 0.0, false);
    }

    let numeric_str: String = chars[..end_idx].iter().collect();

    if has_decimal || has_exponent {
        // Parse as float
        match numeric_str.parse::<f64>() {
            Ok(f) => {
                let result = f * sign as f64;
                (result as i64, result, true)
            }
            Err(_) => (0, 0.0, false),
        }
    } else {
        // Parse as integer
        match numeric_str.parse::<i64>() {
            Ok(n) => {
                let result = n * sign;
                (result, result as f64, false)
            }
            Err(_) => {
                // Try parsing as float (for very large numbers)
                match numeric_str.parse::<f64>() {
                    Ok(f) => {
                        let result = f * sign as f64;
                        (result as i64, result, true)
                    }
                    Err(_) => (0, 0.0, false),
                }
            }
        }
    }
}

/// SQLite-compatible string-to-integer coercion for CAST to INTEGER
///
/// According to SQLite documentation (R-24225-46995):
/// "When casting to INTEGER, if the text looks like a floating point value
/// with an exponent, the exponent will be ignored because it is no part of
/// the integer prefix. For example, CAST('123e+5' AS INTEGER) results in 123."
///
/// For overflow handling (R-06028-16857):
/// "If the prefix integer is greater than +9223372036854775807 then the
/// result of the cast is exactly +9223372036854775807. Similarly, if the
/// prefix integer is less than -9223372036854775808 then the result is
/// exactly -9223372036854775808."
///
/// Rules:
/// 1. Leading whitespace is ignored
/// 2. Optional sign (+/-)
/// 3. Integer prefix is extracted (digits before any non-digit)
/// 4. Exponents are NOT part of the integer prefix and are ignored
/// 5. Decimal point terminates the integer prefix
/// 6. Overflow clamps to i64::MAX or i64::MIN
pub(crate) fn string_to_integer(s: &str) -> i64 {
    let trimmed = s.trim_start();

    if trimmed.is_empty() {
        return 0;
    }

    // Check for sign
    let (is_negative, rest) = match trimmed.chars().next() {
        Some('-') => (true, &trimmed[1..]),
        Some('+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };

    // Find the integer prefix - stops at any non-digit (including '.', 'e', 'E')
    let mut end_idx = 0;
    let chars: Vec<char> = rest.chars().collect();
    let len = chars.len();

    while end_idx < len && chars[end_idx].is_ascii_digit() {
        end_idx += 1;
    }

    if end_idx == 0 {
        return 0;
    }

    let int_str: String = chars[..end_idx].iter().collect();

    match int_str.parse::<i64>() {
        Ok(n) => {
            if is_negative {
                n.checked_neg().unwrap_or(i64::MIN)
            } else {
                n
            }
        }
        Err(_) => {
            // Overflow - clamp to bounds (SQLite behavior)
            if is_negative {
                i64::MIN
            } else {
                i64::MAX
            }
        }
    }
}

/// Helper function to convert float to integer based on SQL mode
/// MySQL mode: rounds to nearest integer
/// SQLite mode: truncates toward zero
#[inline]
fn float_to_int(f: f64, sql_mode: &vibesql_types::SqlMode) -> i64 {
    match sql_mode {
        vibesql_types::SqlMode::MySQL { .. } => f.round() as i64,
        vibesql_types::SqlMode::SQLite => f.trunc() as i64,
    }
}

/// Helper function to convert float to unsigned based on SQL mode
/// MySQL mode: rounds to nearest integer then converts to unsigned
/// SQLite mode: truncates toward zero then converts to unsigned
#[inline]
fn float_to_unsigned(f: f64, sql_mode: &vibesql_types::SqlMode) -> u64 {
    match sql_mode {
        vibesql_types::SqlMode::MySQL { .. } => f.round() as u64,
        vibesql_types::SqlMode::SQLite => f.trunc() as u64,
    }
}

/// Cast a value to the target data type
/// Implements SQL:1999 CAST semantics for explicit type conversion
///
/// Float-to-integer conversion behavior varies by SQL mode:
/// - MySQL mode: rounds to nearest integer (CAST(5.7 AS SIGNED) → 6)
/// - SQLite mode: truncates toward zero (CAST(5.7 AS INTEGER) → 5)
pub(crate) fn cast_value(
    value: &vibesql_types::SqlValue,
    target_type: &vibesql_types::DataType,
    sql_mode: &vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    use vibesql_types::{DataType::*, SqlValue};

    // NULL can be cast to any type and remains NULL
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    match target_type {
        // Cast to INTEGER
        Integer => match value {
            SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
            SqlValue::Smallint(n) => Ok(SqlValue::Integer(*n as i64)),
            SqlValue::Bigint(n) => Ok(SqlValue::Integer(*n)),
            SqlValue::Unsigned(n) => Ok(SqlValue::Integer(*n as i64)),
            SqlValue::Numeric(f) => Ok(SqlValue::Integer(float_to_int(*f, sql_mode))),
            SqlValue::Float(f) => Ok(SqlValue::Integer(float_to_int(*f as f64, sql_mode))),
            SqlValue::Real(f) => Ok(SqlValue::Integer(float_to_int(*f as f64, sql_mode))),
            SqlValue::Double(f) => Ok(SqlValue::Integer(float_to_int(*f, sql_mode))),
            SqlValue::Boolean(b) => Ok(SqlValue::Integer(if *b { 1 } else { 0 })),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: Extract integer prefix, ignoring exponent (R-24225-46995)
                // '123e+5' -> 123, not 12300000
                Ok(SqlValue::Integer(string_to_integer(s)))
            }
            SqlValue::Blob(bytes) => {
                // SQLite: CAST BLOB to INTEGER interprets bytes as UTF-8 text first
                let text = std::str::from_utf8(bytes).unwrap_or("");
                Ok(SqlValue::Integer(string_to_integer(text)))
            }
            _ => Ok(SqlValue::Integer(0)), // SQLite: Default to 0 for unknown types
        },

        // Cast to SMALLINT
        Smallint => match value {
            SqlValue::Smallint(n) => Ok(SqlValue::Smallint(*n)),
            SqlValue::Integer(n) => Ok(SqlValue::Smallint(*n as i16)),
            SqlValue::Bigint(n) => Ok(SqlValue::Smallint(*n as i16)),
            SqlValue::Unsigned(n) => Ok(SqlValue::Smallint(*n as i16)),
            SqlValue::Numeric(f) => Ok(SqlValue::Smallint(float_to_int(*f, sql_mode) as i16)),
            SqlValue::Float(f) => Ok(SqlValue::Smallint(float_to_int(*f as f64, sql_mode) as i16)),
            SqlValue::Real(f) => Ok(SqlValue::Smallint(float_to_int(*f as f64, sql_mode) as i16)),
            SqlValue::Double(f) => Ok(SqlValue::Smallint(float_to_int(*f, sql_mode) as i16)),
            SqlValue::Boolean(b) => Ok(SqlValue::Smallint(if *b { 1 } else { 0 })),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: Extract integer prefix, ignoring exponent
                Ok(SqlValue::Smallint(string_to_integer(s) as i16))
            }
            SqlValue::Blob(bytes) => {
                let text = std::str::from_utf8(bytes).unwrap_or("");
                Ok(SqlValue::Smallint(string_to_integer(text) as i16))
            }
            _ => Ok(SqlValue::Smallint(0)), // SQLite: Default to 0 for unknown types
        },

        // Cast to BIGINT
        Bigint => match value {
            SqlValue::Bigint(n) => Ok(SqlValue::Bigint(*n)),
            SqlValue::Integer(n) => Ok(SqlValue::Bigint(*n)),
            SqlValue::Smallint(n) => Ok(SqlValue::Bigint(*n as i64)),
            SqlValue::Unsigned(n) => Ok(SqlValue::Bigint(*n as i64)),
            SqlValue::Numeric(f) => Ok(SqlValue::Bigint(float_to_int(*f, sql_mode))),
            SqlValue::Float(f) => Ok(SqlValue::Bigint(float_to_int(*f as f64, sql_mode))),
            SqlValue::Real(f) => Ok(SqlValue::Bigint(float_to_int(*f as f64, sql_mode))),
            SqlValue::Double(f) => Ok(SqlValue::Bigint(float_to_int(*f, sql_mode))),
            SqlValue::Boolean(b) => Ok(SqlValue::Bigint(if *b { 1 } else { 0 })),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: Extract integer prefix, ignoring exponent
                Ok(SqlValue::Bigint(string_to_integer(s)))
            }
            SqlValue::Blob(bytes) => {
                let text = std::str::from_utf8(bytes).unwrap_or("");
                Ok(SqlValue::Bigint(string_to_integer(text)))
            }
            _ => Ok(SqlValue::Bigint(0)), // SQLite: Default to 0 for unknown types
        },

        // Cast to UNSIGNED
        Unsigned => match value {
            SqlValue::Unsigned(n) => Ok(SqlValue::Unsigned(*n)),
            SqlValue::Integer(n) => {
                // Convert with wrap-around for negative values (MySQL behavior)
                Ok(SqlValue::Unsigned(*n as u64))
            }
            SqlValue::Smallint(n) => {
                // Convert with wrap-around for negative values (MySQL behavior)
                Ok(SqlValue::Unsigned(*n as u64))
            }
            SqlValue::Bigint(n) => {
                // Convert with wrap-around for negative values (MySQL behavior)
                Ok(SqlValue::Unsigned(*n as u64))
            }
            SqlValue::Numeric(f) => {
                // Convert numeric to unsigned using sql_mode-aware conversion
                Ok(SqlValue::Unsigned(float_to_unsigned(*f, sql_mode)))
            }
            SqlValue::Float(f) => {
                // Convert float to unsigned using sql_mode-aware conversion
                Ok(SqlValue::Unsigned(float_to_unsigned(*f as f64, sql_mode)))
            }
            SqlValue::Real(f) => {
                // Convert real to unsigned using sql_mode-aware conversion
                Ok(SqlValue::Unsigned(float_to_unsigned(*f as f64, sql_mode)))
            }
            SqlValue::Double(f) => {
                // Convert double to unsigned using sql_mode-aware conversion
                Ok(SqlValue::Unsigned(float_to_unsigned(*f, sql_mode)))
            }
            SqlValue::Boolean(b) => {
                // Boolean to unsigned: true=1, false=0 (SQL standard)
                Ok(SqlValue::Unsigned(if *b { 1 } else { 0 }))
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: Extract integer prefix, ignoring exponent
                Ok(SqlValue::Unsigned(string_to_integer(s) as u64))
            }
            SqlValue::Blob(bytes) => {
                let text = std::str::from_utf8(bytes).unwrap_or("");
                Ok(SqlValue::Unsigned(string_to_integer(text) as u64))
            }
            _ => Ok(SqlValue::Unsigned(0)), // SQLite: Default to 0 for unknown types
        },

        // Cast to NUMERIC
        // SQLite NUMERIC affinity preserves storage class from source:
        // - Integer types stay INTEGER
        // - Floating-point types stay REAL (even if whole number like 1.0)
        // - String/blob: INTEGER if exact integer representation, else REAL
        Numeric { .. } => {
            match value {
                // Integer types stay as INTEGER
                SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
                SqlValue::Smallint(n) => Ok(SqlValue::Integer(*n as i64)),
                SqlValue::Bigint(n) => Ok(SqlValue::Integer(*n)),
                SqlValue::Unsigned(n) => Ok(SqlValue::Integer(*n as i64)),
                SqlValue::Boolean(b) => Ok(SqlValue::Integer(if *b { 1 } else { 0 })),

                // Floating-point types stay as REAL (Numeric)
                // SQLite: CAST(1.0 AS NUMERIC) returns 1.0 (type real), not 1 (type integer)
                SqlValue::Numeric(f) => Ok(SqlValue::Numeric(*f)),
                SqlValue::Float(n) => Ok(SqlValue::Numeric(*n as f64)),
                SqlValue::Real(n) => Ok(SqlValue::Numeric(*n)),
                SqlValue::Double(n) => Ok(SqlValue::Numeric(*n)),

                // String/blob: convert to most appropriate type
                // SQLite NUMERIC affinity: use INTEGER if value is exact integer
                // '1.0' -> 1 (integer), '1.5' -> 1.5 (real)
                SqlValue::Varchar(s) | SqlValue::Character(s) => {
                    let (int_val, float_val, _) = string_to_number(s);
                    if float_val.fract() == 0.0
                        && float_val >= i64::MIN as f64
                        && float_val <= i64::MAX as f64
                    {
                        Ok(SqlValue::Integer(int_val))
                    } else {
                        Ok(SqlValue::Numeric(float_val))
                    }
                }
                SqlValue::Blob(bytes) => {
                    let text = std::str::from_utf8(bytes).unwrap_or("");
                    let (int_val, float_val, _) = string_to_number(text);
                    if float_val.fract() == 0.0
                        && float_val >= i64::MIN as f64
                        && float_val <= i64::MAX as f64
                    {
                        Ok(SqlValue::Integer(int_val))
                    } else {
                        Ok(SqlValue::Numeric(float_val))
                    }
                }
                _ => Ok(SqlValue::Integer(0)), // SQLite: Default to 0 for unknown types
            }
        }

        // Cast to FLOAT (f32)
        Float { .. } => match value {
            SqlValue::Float(n) => Ok(SqlValue::Float(*n)),
            SqlValue::Real(n) => Ok(SqlValue::Float(*n as f32)), // Real is now f64
            SqlValue::Double(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Integer(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Smallint(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Bigint(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Unsigned(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Numeric(f) => Ok(SqlValue::Float(*f as f32)),
            SqlValue::Boolean(b) => Ok(SqlValue::Float(if *b { 1.0 } else { 0.0 })),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: Permissive conversion - extract leading numeric portion, default to 0.0
                let (_, float_val, _) = string_to_number(s);
                Ok(SqlValue::Float(float_val as f32))
            }
            SqlValue::Blob(bytes) => {
                // SQLite: CAST BLOB to FLOAT interprets bytes as UTF-8 text first
                let text = std::str::from_utf8(bytes).unwrap_or("");
                let (_, float_val, _) = string_to_number(text);
                Ok(SqlValue::Float(float_val as f32))
            }
            _ => Ok(SqlValue::Float(0.0)), // SQLite: Default to 0.0 for unknown types
        },

        // Cast to REAL (now f64 - SQLite REAL is 8-byte IEEE float)
        Real => match value {
            SqlValue::Real(n) => Ok(SqlValue::Real(*n)),
            SqlValue::Float(n) => Ok(SqlValue::Real(*n as f64)),
            SqlValue::Double(n) => Ok(SqlValue::Real(*n)),
            SqlValue::Integer(n) => Ok(SqlValue::Real(*n as f64)),
            SqlValue::Smallint(n) => Ok(SqlValue::Real(*n as f64)),
            SqlValue::Bigint(n) => Ok(SqlValue::Real(*n as f64)),
            SqlValue::Unsigned(n) => Ok(SqlValue::Real(*n as f64)),
            SqlValue::Numeric(f) => Ok(SqlValue::Real(*f)),
            SqlValue::Boolean(b) => Ok(SqlValue::Real(if *b { 1.0 } else { 0.0 })),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: Permissive conversion - extract leading numeric portion, default to 0.0
                let (_, float_val, _) = string_to_number(s);
                Ok(SqlValue::Real(float_val))
            }
            SqlValue::Blob(bytes) => {
                // SQLite: CAST BLOB to REAL interprets bytes as UTF-8 text first
                let text = std::str::from_utf8(bytes).unwrap_or("");
                let (_, float_val, _) = string_to_number(text);
                Ok(SqlValue::Real(float_val))
            }
            _ => Ok(SqlValue::Real(0.0)), // SQLite: Default to 0.0 for unknown types
        },

        // Cast to DOUBLE PRECISION
        DoublePrecision => match value {
            SqlValue::Double(n) => Ok(SqlValue::Double(*n)),
            SqlValue::Float(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Real(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Integer(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Smallint(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Bigint(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Unsigned(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Numeric(f) => Ok(SqlValue::Double(*f)),
            SqlValue::Boolean(b) => Ok(SqlValue::Double(if *b { 1.0 } else { 0.0 })),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: Permissive conversion - extract leading numeric portion, default to 0.0
                let (_, float_val, _) = string_to_number(s);
                Ok(SqlValue::Double(float_val))
            }
            SqlValue::Blob(bytes) => {
                // SQLite: CAST BLOB to DOUBLE interprets bytes as UTF-8 text first
                let text = std::str::from_utf8(bytes).unwrap_or("");
                let (_, float_val, _) = string_to_number(text);
                Ok(SqlValue::Double(float_val))
            }
            _ => Ok(SqlValue::Double(0.0)), // SQLite: Default to 0.0 for unknown types
        },

        // Cast to VARCHAR
        Varchar { max_length } => {
            let string_val: arcstr::ArcStr = match value {
                SqlValue::Varchar(s) => s.clone(),
                SqlValue::Integer(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                SqlValue::Smallint(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                SqlValue::Bigint(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                SqlValue::Unsigned(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                // Floating-point values must render through the SqlValue Display
                // impl, not the raw f64/f32 `to_string()`. Rust's `f64::to_string`
                // emits shortest-round-trip *fixed-point* text
                // (e.g. "18446744073709550000"), whereas SQLite's CAST(REAL AS TEXT)
                // uses %!.15g-style scientific notation for large/small magnitudes
                // ("1.84467440737095e+19"). Display (format_f64/format_f32) already
                // matches SQLite 3.51 here, so route floats through it for parity
                // (fixes atof-3.1 GLOB comparison against the scientific rendering).
                SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_) => arcstr::ArcStr::from(value.to_string().as_str()),
                SqlValue::Boolean(b) => arcstr::ArcStr::from(if *b { "TRUE" } else { "FALSE" }),
                SqlValue::Date(s) => arcstr::ArcStr::from(s.to_string().as_str()),
                SqlValue::Time(s) => arcstr::ArcStr::from(s.to_string().as_str()),
                SqlValue::Timestamp(s) => arcstr::ArcStr::from(s.to_string().as_str()),
                SqlValue::Character(s) => s.clone(),
                SqlValue::Blob(bytes) => {
                    // SQLite: CAST BLOB to TEXT interprets bytes as UTF-8
                    match std::str::from_utf8(bytes) {
                        Ok(s) => arcstr::ArcStr::from(s),
                        Err(_) => arcstr::ArcStr::from(String::from_utf8_lossy(bytes).as_ref()),
                    }
                }
                _ => arcstr::ArcStr::from(""), // SQLite: Default to empty string for unknown types
            };

            // Truncate if exceeds max_length (when specified)
            match max_length {
                Some(len) if string_val.len() > *len => {
                    Ok(SqlValue::Varchar(arcstr::ArcStr::from(&string_val[..*len])))
                }
                _ => Ok(SqlValue::Varchar(string_val)),
            }
        }

        // Cast to DATE
        Date => match value {
            SqlValue::Date(s) => Ok(SqlValue::Date(*s)),
            SqlValue::Timestamp(s) => {
                // Extract date part from timestamp
                Ok(SqlValue::Date(s.date))
            }
            SqlValue::Varchar(s) => {
                // Parse VARCHAR as DATE
                match s.parse::<vibesql_types::Date>() {
                    Ok(date) => Ok(SqlValue::Date(date)),
                    Err(_) => Err(ExecutorError::CastError {
                        from_type: format!("VARCHAR '{}'", s),
                        to_type: "DATE".to_string(),
                    }),
                }
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "DATE".to_string(),
            }),
        },

        // Cast to TIME
        Time { .. } => match value {
            SqlValue::Time(s) => Ok(SqlValue::Time(*s)),
            SqlValue::Timestamp(s) => {
                // Extract time part from timestamp
                Ok(SqlValue::Time(s.time))
            }
            SqlValue::Varchar(s) => {
                // Parse VARCHAR as TIME
                match s.parse::<vibesql_types::Time>() {
                    Ok(time) => Ok(SqlValue::Time(time)),
                    Err(_) => Err(ExecutorError::CastError {
                        from_type: format!("VARCHAR '{}'", s),
                        to_type: "TIME".to_string(),
                    }),
                }
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "TIME".to_string(),
            }),
        },

        // Cast to TIMESTAMP
        Timestamp { .. } => match value {
            SqlValue::Timestamp(s) => Ok(SqlValue::Timestamp(*s)),
            SqlValue::Date(date) => {
                // Create timestamp with date and midnight time
                let midnight = vibesql_types::Time::new(0, 0, 0, 0).unwrap();
                Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(*date, midnight)))
            }
            SqlValue::Time(time) => {
                // SQL:1999: CAST(TIME AS TIMESTAMP) uses current date + time value
                use chrono::Datelike;
                let now = chrono::Local::now();
                let current_date =
                    vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                        .map_err(|e| ExecutorError::CastError {
                            from_type: format!("TIME '{}'", time),
                            to_type: format!("TIMESTAMP (date construction failed: {})", e),
                        })?;
                Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(current_date, *time)))
            }
            SqlValue::Varchar(s) => {
                // Parse VARCHAR as TIMESTAMP
                match s.parse::<vibesql_types::Timestamp>() {
                    Ok(timestamp) => Ok(SqlValue::Timestamp(timestamp)),
                    Err(_) => Err(ExecutorError::CastError {
                        from_type: format!("VARCHAR '{}'", s),
                        to_type: "TIMESTAMP".to_string(),
                    }),
                }
            }
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "TIMESTAMP".to_string(),
            }),
        },

        // Cast to CHARACTER
        Character { length } => {
            let string_val: arcstr::ArcStr = match value {
                SqlValue::Character(s) => s.clone(),
                SqlValue::Varchar(s) => s.clone(),
                SqlValue::Integer(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                SqlValue::Smallint(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                SqlValue::Bigint(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                SqlValue::Unsigned(n) => arcstr::ArcStr::from(n.to_string().as_str()),
                // See the CAST-to-VARCHAR branch above: floats must render through
                // the SqlValue Display impl (SQLite %!.15g scientific), not raw
                // f64/f32 `to_string()` (fixed-point).
                SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_) => arcstr::ArcStr::from(value.to_string().as_str()),
                SqlValue::Boolean(b) => arcstr::ArcStr::from(if *b { "TRUE" } else { "FALSE" }),
                SqlValue::Blob(bytes) => {
                    // SQLite: CAST BLOB to TEXT interprets bytes as UTF-8
                    match std::str::from_utf8(bytes) {
                        Ok(s) => arcstr::ArcStr::from(s),
                        Err(_) => arcstr::ArcStr::from(String::from_utf8_lossy(bytes).as_ref()),
                    }
                }
                _ => arcstr::ArcStr::from(""),
            };
            // Truncate or pad to exact length
            if string_val.len() > *length {
                Ok(SqlValue::Character(arcstr::ArcStr::from(&string_val[..*length])))
            } else if string_val.len() < *length {
                // Pad with spaces to reach exact length
                let padded = format!("{:<width$}", string_val, width = *length);
                Ok(SqlValue::Character(arcstr::ArcStr::from(padded.as_str())))
            } else {
                Ok(SqlValue::Character(string_val))
            }
        }

        // Cast to BLOB
        BinaryLargeObject => match value {
            SqlValue::Blob(b) => Ok(SqlValue::Blob(b.clone())),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                // SQLite: CAST TEXT to BLOB returns the text bytes
                Ok(SqlValue::Blob(s.as_bytes().to_vec()))
            }
            SqlValue::Integer(n) => {
                // SQLite: CAST integer to BLOB returns the string representation as bytes
                Ok(SqlValue::Blob(n.to_string().into_bytes()))
            }
            SqlValue::Smallint(n) => Ok(SqlValue::Blob(n.to_string().into_bytes())),
            SqlValue::Bigint(n) => Ok(SqlValue::Blob(n.to_string().into_bytes())),
            SqlValue::Unsigned(n) => Ok(SqlValue::Blob(n.to_string().into_bytes())),
            SqlValue::Float(n) => Ok(SqlValue::Blob(n.to_string().into_bytes())),
            SqlValue::Real(n) => Ok(SqlValue::Blob(n.to_string().into_bytes())),
            SqlValue::Double(n) => Ok(SqlValue::Blob(n.to_string().into_bytes())),
            SqlValue::Numeric(n) => Ok(SqlValue::Blob(n.to_string().into_bytes())),
            SqlValue::Boolean(b) => {
                Ok(SqlValue::Blob(if *b { b"1".to_vec() } else { b"0".to_vec() }))
            }
            _ => Ok(SqlValue::Blob(vec![])), // SQLite: Default to empty blob
        },

        // CAST to a user-defined / unknown type name (SQLite compatibility):
        // SQLite accepts any type name in CAST and applies its type-affinity
        // rules derived from the name (e.g. CAST(x AS banana) → NUMERIC).
        UserDefined { .. } => match target_type.sqlite_affinity() {
            vibesql_types::TypeAffinity::Integer => cast_value(value, &Integer, sql_mode),
            vibesql_types::TypeAffinity::Text => {
                cast_value(value, &Varchar { max_length: None }, sql_mode)
            }
            vibesql_types::TypeAffinity::None => cast_value(value, &BinaryLargeObject, sql_mode),
            vibesql_types::TypeAffinity::Real => cast_value(value, &DoublePrecision, sql_mode),
            vibesql_types::TypeAffinity::Numeric => {
                // SQLite NUMERIC cast: text converts to INTEGER when the value
                // is integral and representable losslessly, otherwise REAL;
                // non-numeric text converts to 0.
                let numeric_from_text = |s: &str| -> vibesql_types::SqlValue {
                    let (i, f, is_float) = string_to_number(s);
                    if is_float {
                        if f == f.trunc() && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                            SqlValue::Integer(f as i64)
                        } else {
                            SqlValue::Double(f)
                        }
                    } else {
                        SqlValue::Integer(i)
                    }
                };
                match value {
                    SqlValue::Varchar(s) | SqlValue::Character(s) => Ok(numeric_from_text(s)),
                    SqlValue::Blob(bytes) => {
                        let text = std::str::from_utf8(bytes).unwrap_or("");
                        Ok(numeric_from_text(text))
                    }
                    SqlValue::Boolean(b) => Ok(SqlValue::Integer(if *b { 1 } else { 0 })),
                    // Values that already have a numeric storage class keep it
                    other => Ok(other.clone()),
                }
            }
        },

        // Unsupported target types
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "CAST to {:?} not yet implemented",
            target_type
        ))),
    }
}
