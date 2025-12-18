//! Type coercion helpers for SQLite compatibility
//!
//! SQLite automatically coerces arguments to expected types in function calls.
//! These helpers provide consistent coercion behavior across all functions.

use vibesql_types::SqlValue;

/// Coerce a SqlValue to a string for string functions.
///
/// SQLite coercion rules for string functions:
/// - NULL → None (propagates NULL)
/// - String types → return as-is
/// - Integer → decimal string representation (e.g., 123 → "123")
/// - Float → decimal string representation (e.g., 7.5 → "7.5")
/// - Boolean → "0" or "1"
/// - Blob → UTF-8 lossy conversion
/// - Date/Time → ISO format string
pub fn coerce_to_string(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Null => None,
        SqlValue::Varchar(s) | SqlValue::Character(s) => Some(s.to_string()),
        SqlValue::Integer(n) | SqlValue::Bigint(n) => Some(n.to_string()),
        SqlValue::Smallint(n) => Some(n.to_string()),
        SqlValue::Unsigned(n) => Some(n.to_string()),
        SqlValue::Double(n) | SqlValue::Numeric(n) => Some(format_sqlite_float(*n)),
        SqlValue::Float(n) | SqlValue::Real(n) => Some(format_sqlite_float(*n as f64)),
        SqlValue::Boolean(b) => Some(if *b { "1" } else { "0" }.to_string()),
        SqlValue::Vector(v) => {
            // Format vector as comma-separated values in brackets
            let inner: Vec<String> = v.iter().map(|f| f.to_string()).collect();
            Some(format!("[{}]", inner.join(",")))
        }
        SqlValue::Date(d) => Some(d.to_string()),
        SqlValue::Time(t) => Some(t.to_string()),
        SqlValue::Timestamp(ts) => Some(ts.to_string()),
        SqlValue::Interval(i) => Some(i.to_string()),
    }
}

/// Coerce a SqlValue to a number for numeric functions.
///
/// SQLite coercion rules for numeric functions:
/// - NULL → None (propagates NULL)
/// - Numeric types → return as f64
/// - String → parse as number, default to 0.0 on failure
/// - Boolean → 0.0 or 1.0
pub fn coerce_to_number(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Null => None,
        SqlValue::Integer(n) | SqlValue::Bigint(n) => Some(*n as f64),
        SqlValue::Smallint(n) => Some(*n as f64),
        SqlValue::Unsigned(n) => Some(*n as f64),
        SqlValue::Double(n) | SqlValue::Numeric(n) => Some(*n),
        SqlValue::Float(n) | SqlValue::Real(n) => Some(*n as f64),
        SqlValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // SQLite: parse string to number, default to 0.0
            Some(parse_sqlite_number(s))
        }
        // Other types coerce to 0.0
        _ => Some(0.0),
    }
}

/// Coerce a SqlValue to an integer for functions requiring integer arguments.
///
/// SQLite coercion rules:
/// - NULL → None
/// - Integer types → return as-is
/// - Float → truncate to integer
/// - String → parse as integer, default to 0
pub fn coerce_to_integer(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Null => None,
        SqlValue::Integer(n) | SqlValue::Bigint(n) => Some(*n),
        SqlValue::Smallint(n) => Some(*n as i64),
        SqlValue::Unsigned(n) => Some(*n as i64),
        SqlValue::Double(n) | SqlValue::Numeric(n) => Some(*n as i64),
        SqlValue::Float(n) | SqlValue::Real(n) => Some(*n as i64),
        SqlValue::Boolean(b) => Some(if *b { 1 } else { 0 }),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // SQLite: parse string to integer, default to 0
            Some(s.trim().parse::<i64>().unwrap_or_else(|_| {
                // Try parsing as float then truncating
                s.trim().parse::<f64>().map(|f| f as i64).unwrap_or(0)
            }))
        }
        _ => Some(0),
    }
}

/// Format a float value like SQLite does.
///
/// SQLite uses specific formatting for floats:
/// - Integers are displayed without decimal point (e.g., 5.0 → "5.0" but may vary)
/// - Scientific notation for very large/small numbers
fn format_sqlite_float(n: f64) -> String {
    if n.is_nan() {
        return "NaN".to_string();
    }
    if n.is_infinite() {
        return if n.is_sign_positive() {
            "Inf"
        } else {
            "-Inf"
        }
        .to_string();
    }

    // SQLite typically uses standard decimal notation
    // For whole numbers, it still includes the decimal (e.g., 5.0)
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{:.1}", n)
    } else {
        // Use default Display which handles precision appropriately
        n.to_string()
    }
}

/// Parse a string to a number like SQLite does.
///
/// SQLite parsing rules:
/// - Leading/trailing whitespace is trimmed
/// - Leading numeric portion is parsed (e.g., "123abc" → 123)
/// - Non-numeric strings return 0.0
fn parse_sqlite_number(s: &str) -> f64 {
    let s = s.trim();
    if s.is_empty() {
        return 0.0;
    }

    // Try direct parse first
    if let Ok(n) = s.parse::<f64>() {
        return n;
    }

    // SQLite extracts leading numeric portion
    // Find the longest valid numeric prefix
    let mut end = 0;
    let mut has_dot = false;
    let mut has_e = false;
    let chars: Vec<char> = s.chars().collect();

    // Handle optional leading sign
    if !chars.is_empty() && (chars[0] == '-' || chars[0] == '+') {
        end = 1;
    }

    while end < chars.len() {
        let c = chars[end];
        if c.is_ascii_digit() {
            end += 1;
        } else if c == '.' && !has_dot && !has_e {
            has_dot = true;
            end += 1;
        } else if (c == 'e' || c == 'E') && !has_e && end > 0 {
            has_e = true;
            end += 1;
            // Handle exponent sign
            if end < chars.len() && (chars[end] == '-' || chars[end] == '+') {
                end += 1;
            }
        } else {
            break;
        }
    }

    if end == 0 || (end == 1 && (chars[0] == '-' || chars[0] == '+')) {
        return 0.0;
    }

    let numeric_part: String = chars[..end].iter().collect();
    numeric_part.parse::<f64>().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coerce_to_string() {
        assert_eq!(coerce_to_string(&SqlValue::Null), None);
        assert_eq!(
            coerce_to_string(&SqlValue::Integer(123)),
            Some("123".to_string())
        );
        assert_eq!(
            coerce_to_string(&SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_coerce_to_number() {
        assert_eq!(coerce_to_number(&SqlValue::Null), None);
        assert_eq!(coerce_to_number(&SqlValue::Integer(123)), Some(123.0));
        assert_eq!(
            coerce_to_number(&SqlValue::Varchar(arcstr::ArcStr::from("456"))),
            Some(456.0)
        );
        assert_eq!(
            coerce_to_number(&SqlValue::Varchar(arcstr::ArcStr::from("free"))),
            Some(0.0)
        );
        assert_eq!(
            coerce_to_number(&SqlValue::Varchar(arcstr::ArcStr::from("-5"))),
            Some(-5.0)
        );
    }

    #[test]
    fn test_parse_sqlite_number() {
        assert_eq!(parse_sqlite_number("123"), 123.0);
        assert_eq!(parse_sqlite_number("  456  "), 456.0);
        assert_eq!(parse_sqlite_number("-5"), -5.0);
        assert_eq!(parse_sqlite_number("free"), 0.0);
        assert_eq!(parse_sqlite_number("123abc"), 123.0);
        assert_eq!(parse_sqlite_number(""), 0.0);
    }
}
