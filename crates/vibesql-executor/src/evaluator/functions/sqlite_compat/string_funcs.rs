//! String manipulation functions
//!
//! This module contains SQLite-compatible string functions:
//! - CHAR(x1, x2, ...) - Return string from Unicode code points
//! - UNICODE(x) - Return Unicode code point of first character
//! - CONCAT_WS(sep, ...) - Concatenate with separator
//! - PRINTF(format, ...) - Formatted string output

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// UNICODE(x) - Return the Unicode code point of the first character
///
/// Returns the numeric unicode code point of the first character of string x.
/// Returns NULL if the argument is NULL or an empty string.
pub(crate) fn unicode(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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
pub(crate) fn char_func(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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

/// CONCAT_WS(separator, str1, str2, ...) - Concatenate with separator
///
/// Concatenates strings with the first argument as separator.
/// NULL values are skipped (not included in result).
/// Returns NULL if the separator is NULL.
/// SQLite requires at least 2 arguments (separator + at least one value).
pub(crate) fn concat_ws(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // SQLite requires at least 2 arguments: separator and at least one value
    if args.len() < 2 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "concat_ws".to_string(),
        });
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
    /// Whether precision should be read from the next argument (for %.*s, %.*c, etc.)
    precision_from_arg: bool,
    /// The conversion specifier character
    specifier: char,
}

/// PRINTF(format, ...) - Formatted string output
///
/// Returns a string formatted according to the format string, similar to C's printf.
/// Supports: %d, %i (integer), %f (float), %e, %E (scientific), %s (string),
/// %x, %X (hex), %o (octal), %c (character), %% (literal %)
/// Flags: - (left-justify), + (show sign), space, # (alternate form), 0 (zero-pad)
pub(crate) fn printf(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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
        let mut spec = parse_format_spec(&mut chars);

        // If precision comes from argument, consume it first
        if spec.precision_from_arg {
            if arg_index < format_args.len() {
                let prec_val = &format_args[arg_index];
                arg_index += 1;
                // Convert the precision argument to usize
                spec.precision = match prec_val {
                    SqlValue::Integer(i) => Some((*i).max(0) as usize),
                    SqlValue::Bigint(i) => Some((*i).max(0) as usize),
                    SqlValue::Smallint(i) => Some((*i).max(0) as usize),
                    SqlValue::Numeric(n) => Some((*n).max(0.0) as usize),
                    SqlValue::Real(r) => Some((*r).max(0.0) as usize),
                    SqlValue::Double(d) => Some((*d).max(0.0) as usize),
                    _ => Some(0),
                };
            } else {
                spec.precision = Some(0);
            }
        }

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
        // Check for * (precision from argument)
        if chars.peek() == Some(&'*') {
            chars.next();
            spec.precision_from_arg = true;
        } else {
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
        'c' => format_char(val, spec.precision),
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
    let pad_char = if spec.zero_pad && !spec.left_justify { '0' } else { ' ' };

    if spec.left_justify {
        format!("{}{}", s, " ".repeat(padding))
    } else if spec.zero_pad && (s.starts_with('-') || s.starts_with('+')) {
        // For zero-padding with sign, put sign before zeros
        let (sign, rest) = s.split_at(1);
        format!("{}{}{}", sign, std::iter::repeat(pad_char).take(padding).collect::<String>(), rest)
    } else {
        format!("{}{}", std::iter::repeat(pad_char).take(padding).collect::<String>(), s)
    }
}

fn format_int_with_spec(val: &SqlValue, spec: &FormatSpec) -> String {
    let i64_val = match val {
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

    // SQLite's %d format treats values as 32-bit signed integers.
    // Cast to i32 to properly interpret values like 0xffffffff as -1.
    let i = i64_val as i32;

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
    let i64_val = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Numeric(n) => *n as i64,
        SqlValue::Real(r) => *r as i64,
        SqlValue::Double(d) => *d as i64,
        _ => 0,
    };

    // SQLite's %x format uses 32-bit representation
    let i = i64_val as u32;

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
    let i64_val = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Numeric(n) => *n as i64,
        SqlValue::Real(r) => *r as i64,
        SqlValue::Double(d) => *d as i64,
        _ => 0,
    };

    // SQLite's %o format uses 32-bit representation
    let i = i64_val as u32;

    let oct = format!("{:o}", i);

    // Per C standard: # flag adds leading 0 for octal (but not if already 0)
    if spec.alternate && i != 0 && !oct.starts_with('0') {
        format!("0{}", oct)
    } else {
        oct
    }
}

fn format_char(val: &SqlValue, precision: Option<usize>) -> String {
    let code = match val {
        SqlValue::Null => return "(null)".to_string(),
        SqlValue::Integer(i) => *i as u32,
        SqlValue::Bigint(i) => *i as u32,
        SqlValue::Smallint(i) => *i as u32,
        SqlValue::Numeric(n) => *n as u32,
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // For string input, use the first character's code point (SQLite behavior)
            match s.chars().next() {
                Some(c) => c as u32,
                None => return String::new(),
            }
        }
        _ => return "".to_string(),
    };

    match char::from_u32(code) {
        Some(c) => {
            // If precision is specified, repeat the character that many times
            // This implements %.*c behavior (e.g., printf('%.*c', 5, 65) -> "AAAAA")
            let repeat_count = precision.unwrap_or(1);
            c.to_string().repeat(repeat_count)
        }
        None => String::new(),
    }
}

/// UNISTR(x) - Interpret Unicode escape sequences in a string
///
/// Converts Unicode escape sequences like \uXXXX to actual Unicode characters.
/// SQLite compatibility: Recognizes \uXXXX (4 hex digits) and \UXXXXXXXX (8 hex digits).
/// A backslash followed by anything else is passed through unchanged.
/// Returns NULL if the argument is NULL.
pub(crate) fn unistr(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UNISTR requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let s = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        other => {
            // Convert non-string to string first
            let s = other.to_string();
            return Ok(SqlValue::Varchar(process_unistr(&s).into()));
        }
    };

    Ok(SqlValue::Varchar(process_unistr(s).into()))
}

/// Process Unicode escape sequences in a string
fn process_unistr(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '\\' {
            result.push(c);
            continue;
        }

        // Check for \u or \U escape sequence
        match chars.peek() {
            Some('u') => {
                chars.next(); // consume 'u'
                // Try to parse 4 hex digits
                let hex: String = chars.by_ref().take(4).collect();
                if hex.len() == 4 {
                    if let Ok(code_point) = u32::from_str_radix(&hex, 16) {
                        if let Some(ch) = char::from_u32(code_point) {
                            result.push(ch);
                            continue;
                        }
                    }
                }
                // Invalid escape - pass through as-is
                result.push('\\');
                result.push('u');
                result.push_str(&hex);
            }
            Some('U') => {
                chars.next(); // consume 'U'
                // Try to parse 8 hex digits (for surrogate pairs or extended Unicode)
                let hex: String = chars.by_ref().take(8).collect();
                if hex.len() == 8 {
                    if let Ok(code_point) = u32::from_str_radix(&hex, 16) {
                        if let Some(ch) = char::from_u32(code_point) {
                            result.push(ch);
                            continue;
                        }
                    }
                }
                // Invalid escape - pass through as-is
                result.push('\\');
                result.push('U');
                result.push_str(&hex);
            }
            Some('+') => {
                chars.next(); // consume '+'
                // Try to parse up to 6 hex digits (Unicode code point format \+XXXXXX)
                let mut hex = String::new();
                while hex.len() < 6 {
                    if let Some(&c) = chars.peek() {
                        if c.is_ascii_hexdigit() {
                            hex.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if !hex.is_empty() {
                    if let Ok(code_point) = u32::from_str_radix(&hex, 16) {
                        if let Some(ch) = char::from_u32(code_point) {
                            result.push(ch);
                            continue;
                        }
                    }
                }
                // Invalid escape - pass through as-is
                result.push('\\');
                result.push('+');
                result.push_str(&hex);
            }
            _ => {
                // Not a Unicode escape, pass through the backslash
                result.push('\\');
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // No strings (just separator) - SQLite requires at least 2 args
        assert!(
            concat_ws(&[SqlValue::Varchar(",".into())]).is_err()
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
    fn test_printf_char() {
        // Basic %c - single character from code point
        assert_eq!(
            printf(&[SqlValue::Varchar("%c".into()), SqlValue::Integer(65)]).unwrap(),
            SqlValue::Varchar("A".into())
        );

        // %c with asterisk - no precision specified should output single char
        assert_eq!(
            printf(&[SqlValue::Varchar("%c".into()), SqlValue::Integer(42)]).unwrap(),
            SqlValue::Varchar("*".into())
        );
    }

    #[test]
    fn test_printf_precision_from_arg() {
        // %.*c - repeat character N times
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%.*c".into()),
                SqlValue::Integer(5),  // precision: repeat 5 times
                SqlValue::Integer(65)  // 'A'
            ])
            .unwrap(),
            SqlValue::Varchar("AAAAA".into())
        );

        // %.*c with zero precision - empty string
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%.*c".into()),
                SqlValue::Integer(0),
                SqlValue::Integer(65)
            ])
            .unwrap(),
            SqlValue::Varchar("".into())
        );

        // %.*c with precision 1 - single character
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%.*c".into()),
                SqlValue::Integer(1),
                SqlValue::Integer(66)  // 'B'
            ])
            .unwrap(),
            SqlValue::Varchar("B".into())
        );

        // %.*c with asterisk character
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%.*c".into()),
                SqlValue::Integer(3),
                SqlValue::Integer(42)  // '*'
            ])
            .unwrap(),
            SqlValue::Varchar("***".into())
        );

        // %.*s - precision for string truncation
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%.*s".into()),
                SqlValue::Integer(5),
                SqlValue::Varchar("Hello, World!".into())
            ])
            .unwrap(),
            SqlValue::Varchar("Hello".into())
        );
    }

    #[test]
    fn test_printf_char_with_width() {
        // %10.*c with width and precision - right-padded with spaces
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%10.*c".into()),
                SqlValue::Integer(3),
                SqlValue::Integer(42)  // '*'
            ])
            .unwrap(),
            SqlValue::Varchar("       ***".into())
        );

        // %-10.*c with left justification
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%-10.*c".into()),
                SqlValue::Integer(3),
                SqlValue::Integer(42)  // '*'
            ])
            .unwrap(),
            SqlValue::Varchar("***       ".into())
        );
    }

    #[test]
    fn test_printf_char_with_string_arg() {
        // %c with string argument - uses first character's code point (SQLite behavior)
        assert_eq!(
            printf(&[SqlValue::Varchar("%c".into()), SqlValue::Varchar("A".into())]).unwrap(),
            SqlValue::Varchar("A".into())
        );

        // %.*c with string argument - repeat first character N times
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%.*c".into()),
                SqlValue::Integer(5),
                SqlValue::Varchar("m".into())  // 'm' as string
            ])
            .unwrap(),
            SqlValue::Varchar("mmmmm".into())
        );

        // %.*c with longer string - should only use first character
        assert_eq!(
            printf(&[
                SqlValue::Varchar("%.*c".into()),
                SqlValue::Integer(3),
                SqlValue::Varchar("hello".into())  // only 'h' is used
            ])
            .unwrap(),
            SqlValue::Varchar("hhh".into())
        );

        // Mixed usage matching TCL test func-9.14 pattern
        assert_eq!(
            printf(&[
                SqlValue::Varchar("abc%.*cxyz".into()),
                SqlValue::Integer(5),
                SqlValue::Varchar("m".into())
            ])
            .unwrap(),
            SqlValue::Varchar("abcmmmmmxyz".into())
        );
    }
}
