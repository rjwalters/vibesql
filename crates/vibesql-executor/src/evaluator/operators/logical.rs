//! Logical operator implementations
//!
//! Handles: AND, OR
//! Implements: SQL three-valued logic (TRUE, FALSE, NULL)
//! Supports: SQLite truthiness (numeric values in boolean context)

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Parse the leading numeric portion of a string, SQLite-style.
///
/// SQLite rules for string-to-numeric coercion:
/// - Leading whitespace is skipped
/// - Optional sign (+/-) is recognized
/// - Digits and decimal point are parsed
/// - Parsing stops at the first non-numeric character
/// - Returns 0.0 if no numeric prefix is found
///
/// Examples:
/// - "123" → 123.0
/// - "123abc" → 123.0
/// - "-45.6xyz" → -45.6
/// - "abc" → 0.0
/// - "" → 0.0
/// Check if a string is truthy in SQLite boolean context.
/// Parses leading numeric and returns true if non-zero.
pub fn is_truthy_string(s: &str) -> bool {
    parse_leading_numeric(s) != 0.0
}

/// Check if a SqlValue is truthy using SQLite truthiness rules.
///
/// SQLite rules:
/// - `Boolean(true)` → true
/// - `Boolean(false)` → false
/// - `Null` → false (NULL is not truthy for WHERE clause purposes)
/// - `0` and `0.0` → false
/// - Any other numeric value → true
/// - Strings → parse leading numeric, non-zero is true, 0 or non-numeric is false
///
/// Shared by row-selection paths (DELETE, UPDATE-on-view) so comparison
/// results that surface as `Integer(1)`/`Integer(0)` (SQLite-style) are
/// treated identically to `Boolean(true)`/`Boolean(false)`.
pub fn is_truthy(value: &SqlValue) -> bool {
    use SqlValue::*;
    match value {
        Boolean(b) => *b,
        Null => false, // NULL is not truthy for row selection
        // Integer types: 0 is false, non-zero is true
        Integer(n) => *n != 0,
        Smallint(n) => *n != 0,
        Bigint(n) => *n != 0,
        Unsigned(n) => *n != 0,
        // Floating point types: 0.0 is false, non-zero is true
        Float(f) => *f != 0.0,
        Real(f) => *f != 0.0,
        Double(f) => *f != 0.0,
        Numeric(f) => *f != 0.0,
        // String types: SQLite parses leading numeric portion
        Varchar(s) | Character(s) => is_truthy_string(s),
        // BLOB: SQLite reads the bytes as text and applies the same
        // leading-numeric coercion: X'31' ("1") is truthy, zeroblob(3)
        // (NUL bytes) is falsy (#5803)
        Blob(b) => is_truthy_string(&String::from_utf8_lossy(b)),
        // Other types are not truthy (conservative default)
        _ => false,
    }
}

fn parse_leading_numeric(s: &str) -> f64 {
    let s = s.trim_start();
    if s.is_empty() {
        return 0.0;
    }

    let mut chars = s.chars().peekable();
    let mut result = String::new();

    // Handle optional sign
    if let Some(&c) = chars.peek() {
        if c == '+' || c == '-' {
            result.push(c);
            chars.next();
        }
    }

    let mut has_digits = false;
    let mut has_dot = false;

    // Parse digits and decimal point
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            result.push(c);
            has_digits = true;
            chars.next();
        } else if c == '.' && !has_dot {
            result.push(c);
            has_dot = true;
            chars.next();
        } else {
            break;
        }
    }

    if !has_digits {
        return 0.0;
    }

    result.parse::<f64>().unwrap_or(0.0)
}

/// Convert a SqlValue to a boolean using SQLite truthiness rules.
///
/// SQLite rules:
/// - `0` and `0.0` → false
/// - Any other numeric value → true
/// - Boolean values → as-is
/// - NULL → None (three-valued logic)
/// - Strings → try to parse as number, non-numeric strings are treated as 0 (false)
/// - Other types → error
fn to_boolean(value: &SqlValue) -> Result<Option<bool>, ExecutorError> {
    use SqlValue::*;
    match value {
        Boolean(b) => Ok(Some(*b)),
        Null => Ok(None),
        // Integer types: 0 is false, non-zero is true
        Integer(n) => Ok(Some(*n != 0)),
        Smallint(n) => Ok(Some(*n != 0)),
        Bigint(n) => Ok(Some(*n != 0)),
        Unsigned(n) => Ok(Some(*n != 0)),
        // Floating point types: 0.0 is false, non-zero is true
        Float(f) => Ok(Some(*f != 0.0)),
        Real(f) => Ok(Some(*f != 0.0)),
        Double(f) => Ok(Some(*f != 0.0)),
        Numeric(f) => Ok(Some(*f != 0.0)),
        // String types: SQLite coerces strings to numeric for boolean context
        // SQLite parses the leading numeric portion of the string
        // e.g., '1x' → 1 (truthy), '0x' → 0 (falsy), 'abc' → 0 (falsy)
        Varchar(s) | Character(s) => {
            let trimmed = s.trim();
            // SQLite parses the leading numeric portion of a string
            // Use the parse_leading_numeric helper to match SQLite behavior
            let numeric_value = parse_leading_numeric(trimmed);
            Ok(Some(numeric_value != 0.0))
        }
        // BLOB: SQLite reads the bytes as text and applies the same
        // leading-numeric coercion (#5856):
        //   x'31' AND 1 → 1;  zeroblob(4) AND 1 → 0
        Blob(b) => Ok(Some(is_truthy_string(&String::from_utf8_lossy(b)))),
        // Other types are not supported in boolean context
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "boolean context".to_string(),
            right: SqlValue::Null,
        }),
    }
}

pub(crate) struct LogicalOps;

impl LogicalOps {
    /// AND operator - SQL three-valued logic with SQLite truthiness
    ///
    /// Truth table:
    /// - TRUE AND TRUE = TRUE
    /// - TRUE AND FALSE = FALSE
    /// - TRUE AND NULL = NULL
    /// - FALSE AND anything = FALSE
    /// - NULL AND TRUE = NULL
    /// - NULL AND FALSE = FALSE
    /// - NULL AND NULL = NULL
    ///
    /// SQLite truthiness:
    /// - Numeric 0/0.0 is treated as FALSE
    /// - Non-zero numeric values are treated as TRUE
    #[inline]
    pub fn and(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Convert both operands to boolean (None = NULL)
        let left_bool = to_boolean(left)?;
        let right_bool = to_boolean(right)?;

        match (left_bool, right_bool) {
            // Both have values
            (Some(a), Some(b)) => Ok(Boolean(a && b)),

            // FALSE AND anything = FALSE (even NULL)
            (Some(false), None) | (None, Some(false)) => Ok(Boolean(false)),

            // TRUE AND NULL = NULL
            (Some(true), None) | (None, Some(true)) => Ok(Null),

            // NULL AND NULL = NULL
            (None, None) => Ok(Null),
        }
    }

    /// OR operator - SQL three-valued logic with SQLite truthiness
    ///
    /// Truth table:
    /// - TRUE OR anything = TRUE
    /// - FALSE OR TRUE = TRUE
    /// - FALSE OR FALSE = FALSE
    /// - FALSE OR NULL = NULL
    /// - NULL OR TRUE = TRUE
    /// - NULL OR FALSE = NULL
    /// - NULL OR NULL = NULL
    ///
    /// SQLite truthiness:
    /// - Numeric 0/0.0 is treated as FALSE
    /// - Non-zero numeric values are treated as TRUE
    #[inline]
    pub fn or(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Convert both operands to boolean (None = NULL)
        let left_bool = to_boolean(left)?;
        let right_bool = to_boolean(right)?;

        match (left_bool, right_bool) {
            // Both have values
            (Some(a), Some(b)) => Ok(Boolean(a || b)),

            // TRUE OR anything = TRUE (even NULL)
            (Some(true), None) | (None, Some(true)) => Ok(Boolean(true)),

            // FALSE OR NULL = NULL
            (Some(false), None) | (None, Some(false)) => Ok(Null),

            // NULL OR NULL = NULL
            (None, None) => Ok(Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_and_operator() {
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(false), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_operator() {
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(false), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_strings_in_boolean_context() {
        // SQLite coerces strings to numeric for boolean context
        // Non-numeric strings like "hello" parse to 0.0, which is falsy
        let result = LogicalOps::and(
            &SqlValue::Varchar(arcstr::ArcStr::from("hello")),
            &SqlValue::Boolean(true),
        );
        // "hello" → 0 (falsy), so false AND true = false
        assert_eq!(result.unwrap(), SqlValue::Boolean(false));

        let result = LogicalOps::or(
            &SqlValue::Boolean(true),
            &SqlValue::Varchar(arcstr::ArcStr::from("world")),
        );
        // true OR "world" (falsy) = true
        assert_eq!(result.unwrap(), SqlValue::Boolean(true));

        // Numeric string prefix should work
        let result = LogicalOps::and(
            &SqlValue::Varchar(arcstr::ArcStr::from("1abc")),
            &SqlValue::Boolean(true),
        );
        // "1abc" → 1 (truthy), so true AND true = true
        assert_eq!(result.unwrap(), SqlValue::Boolean(true));

        let result = LogicalOps::and(
            &SqlValue::Varchar(arcstr::ArcStr::from("0")),
            &SqlValue::Boolean(true),
        );
        // "0" → 0 (falsy), so false AND true = false
        assert_eq!(result.unwrap(), SqlValue::Boolean(false));
    }

    #[test]
    fn test_and_with_integers() {
        // SQLite truthiness: 0 = false, non-zero = true
        // 1 AND 1 = true
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 1 AND 0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 0 AND 1 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 0 AND 0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Non-zero values: 42 AND -7 = true
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(42), &SqlValue::Integer(-7)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_or_with_integers() {
        // SQLite truthiness: 0 = false, non-zero = true
        // 1 OR 0 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(1), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 OR 1 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(0), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 OR 0 = false
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(0), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 1 OR 1 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(1), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_and_with_floats() {
        // 1.5 AND 0.0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Float(1.5), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 1.5 AND 2.5 = true
        assert_eq!(
            LogicalOps::and(&SqlValue::Float(1.5), &SqlValue::Float(2.5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0.0 AND 0.0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Float(0.0), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_with_floats() {
        // 1.5 OR 0.0 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(1.5), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0.0 OR 0.0 = false
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(0.0), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_and_with_numeric() {
        // Numeric type (f64)
        assert_eq!(
            LogicalOps::and(&SqlValue::Numeric(4.3), &SqlValue::Numeric(2.4)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Numeric(0.0), &SqlValue::Numeric(2.4)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_with_numeric() {
        // Numeric type (f64)
        assert_eq!(
            LogicalOps::or(&SqlValue::Numeric(4.3), &SqlValue::Numeric(0.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Numeric(0.0), &SqlValue::Numeric(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_mixed_numeric_and_boolean() {
        // Integer AND Boolean
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Float OR Boolean
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(0.0), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(1.5), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_numeric_with_null() {
        // Integer with NULL
        // 0 AND NULL = FALSE (0 is false, false AND anything = false)
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(false)
        );
        // 1 AND NULL = NULL (1 is true, true AND null = null)
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
        // 1 OR NULL = TRUE (1 is true, true OR anything = true)
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(1), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 OR NULL = NULL (0 is false, false OR null = null)
        assert_eq!(LogicalOps::or(&SqlValue::Integer(0), &SqlValue::Null).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_and_with_null() {
        // FALSE AND NULL = FALSE
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(false), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Null, &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );

        // TRUE AND NULL = NULL
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Null, &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Null
        );

        // NULL AND NULL = NULL
        assert_eq!(LogicalOps::and(&SqlValue::Null, &SqlValue::Null).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_or_with_null() {
        // TRUE OR NULL = TRUE
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Null, &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );

        // FALSE OR NULL = NULL
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(false), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Null, &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Null
        );

        // NULL OR NULL = NULL
        assert_eq!(LogicalOps::or(&SqlValue::Null, &SqlValue::Null).unwrap(), SqlValue::Null);
    }
}
