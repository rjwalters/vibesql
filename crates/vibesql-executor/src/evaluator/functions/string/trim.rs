//! String trimming functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions
//! SQLite compatibility: trim(X), trim(X,Y), ltrim(X), ltrim(X,Y), rtrim(X), rtrim(X,Y)

use crate::{errors::ExecutorError, evaluator::functions::coercion::coerce_to_string};

/// TRIM(X) or TRIM(X, Y) - Remove characters from both ends of a string
///
/// SQLite compatibility:
/// - TRIM(X) removes whitespace from both ends
/// - TRIM(X, Y) removes any character in Y from both ends
/// - Automatically coerces numeric types to strings
///
/// Examples:
/// - trim('  hello  ') returns 'hello'
/// - trim('xxxhelloxxx', 'x') returns 'hello'
/// - trim('abchelloabc', 'abc') returns 'hello' (removes any of a, b, or c)
pub(in crate::evaluator::functions) fn trim(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TRIM requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let s = match coerce_to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    if args.len() == 1 {
        // Single argument: trim whitespace
        Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(s.trim().to_string())))
    } else {
        // Two arguments: trim specified characters
        let chars_to_remove = match coerce_to_string(&args[1]) {
            Some(c) => c,
            None => return Ok(vibesql_types::SqlValue::Null),
        };

        let char_set: std::collections::HashSet<char> = chars_to_remove.chars().collect();
        let result = s.trim_matches(|c| char_set.contains(&c)).to_string();
        Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
    }
}

/// LTRIM(X) or LTRIM(X, Y) - Remove characters from the left (leading) side of a string
///
/// SQLite compatibility:
/// - LTRIM(X) removes whitespace from the left
/// - LTRIM(X, Y) removes any character in Y from the left
/// - Automatically coerces numeric types to strings
///
/// Examples:
/// - ltrim('  hello') returns 'hello'
/// - ltrim('xxxhello', 'x') returns 'hello'
/// - ltrim('abchello', 'abc') returns 'hello' (removes any of a, b, or c)
pub(in crate::evaluator::functions) fn ltrim(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LTRIM requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let s = match coerce_to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    if args.len() == 1 {
        // Single argument: trim leading whitespace
        Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(s.trim_start().to_string())))
    } else {
        // Two arguments: trim specified characters from left
        let chars_to_remove = match coerce_to_string(&args[1]) {
            Some(c) => c,
            None => return Ok(vibesql_types::SqlValue::Null),
        };

        let char_set: std::collections::HashSet<char> = chars_to_remove.chars().collect();
        let result = s.trim_start_matches(|c| char_set.contains(&c)).to_string();
        Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
    }
}

/// RTRIM(X) or RTRIM(X, Y) - Remove characters from the right (trailing) side of a string
///
/// SQLite compatibility:
/// - RTRIM(X) removes whitespace from the right
/// - RTRIM(X, Y) removes any character in Y from the right
/// - Automatically coerces numeric types to strings
///
/// Examples:
/// - rtrim('hello  ') returns 'hello'
/// - rtrim('helloxxx', 'x') returns 'hello'
/// - rtrim('helloabc', 'abc') returns 'hello' (removes any of a, b, or c)
pub(in crate::evaluator::functions) fn rtrim(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RTRIM requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let s = match coerce_to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    if args.len() == 1 {
        // Single argument: trim trailing whitespace
        Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end().to_string())))
    } else {
        // Two arguments: trim specified characters from right
        let chars_to_remove = match coerce_to_string(&args[1]) {
            Some(c) => c,
            None => return Ok(vibesql_types::SqlValue::Null),
        };

        let char_set: std::collections::HashSet<char> = chars_to_remove.chars().collect();
        let result = s.trim_end_matches(|c| char_set.contains(&c)).to_string();
        Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
    }
}

/// TRIM with position and custom character support
/// Supports TRIM(BOTH/LEADING/TRAILING 'x' FROM 'string')
/// SQL:1999 Section 6.29: String value functions
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
#[allow(dead_code)] // Currently called via eval_trim method, may be used directly in future
pub(crate) fn trim_advanced(
    string_val: vibesql_types::SqlValue,
    position: Option<vibesql_ast::TrimPosition>,
    removal_char: Option<vibesql_types::SqlValue>,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Extract string with coercion
    let s = match coerce_to_string(&string_val) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    // Determine character to remove (default: space)
    let char_to_remove = if let Some(removal) = removal_char {
        match removal {
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            vibesql_types::SqlValue::Varchar(c) | vibesql_types::SqlValue::Character(c) => {
                if c.len() != 1 {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "TRIM removal character must be single character, got '{}'",
                        c
                    )));
                }
                c.chars().next().unwrap()
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "TRIM removal character must be string, got {:?}",
                    removal
                )))
            }
        }
    } else {
        ' ' // Default to space
    };

    // Apply trimming based on position
    let result = match position.unwrap_or(vibesql_ast::TrimPosition::Both) {
        vibesql_ast::TrimPosition::Both => s.trim_matches(char_to_remove).to_string(),
        vibesql_ast::TrimPosition::Leading => s.trim_start_matches(char_to_remove).to_string(),
        vibesql_ast::TrimPosition::Trailing => s.trim_end_matches(char_to_remove).to_string(),
    };

    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_trim_whitespace() {
        let args = vec![SqlValue::Varchar(arcstr::ArcStr::from("  hello  "))];
        let result = trim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    }

    #[test]
    fn test_trim_with_chars() {
        let args = vec![
            SqlValue::Varchar(arcstr::ArcStr::from("xxxhelloxxx")),
            SqlValue::Varchar(arcstr::ArcStr::from("x")),
        ];
        let result = trim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    }

    #[test]
    fn test_trim_with_multiple_chars() {
        let args = vec![
            SqlValue::Varchar(arcstr::ArcStr::from("abchelloabc")),
            SqlValue::Varchar(arcstr::ArcStr::from("abc")),
        ];
        let result = trim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    }

    #[test]
    fn test_ltrim_whitespace() {
        let args = vec![SqlValue::Varchar(arcstr::ArcStr::from("  hello"))];
        let result = ltrim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    }

    #[test]
    fn test_ltrim_with_chars() {
        let args = vec![
            SqlValue::Varchar(arcstr::ArcStr::from("xxxhello")),
            SqlValue::Varchar(arcstr::ArcStr::from("x")),
        ];
        let result = ltrim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    }

    #[test]
    fn test_rtrim_whitespace() {
        let args = vec![SqlValue::Varchar(arcstr::ArcStr::from("hello  "))];
        let result = rtrim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    }

    #[test]
    fn test_rtrim_with_chars() {
        let args = vec![
            SqlValue::Varchar(arcstr::ArcStr::from("helloxxx")),
            SqlValue::Varchar(arcstr::ArcStr::from("x")),
        ];
        let result = rtrim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    }

    #[test]
    fn test_trim_null_input() {
        let args = vec![SqlValue::Null];
        let result = trim(&args).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_trim_null_chars() {
        let args = vec![SqlValue::Varchar(arcstr::ArcStr::from("hello")), SqlValue::Null];
        let result = trim(&args).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_trim_numeric_coercion() {
        // SQLite coerces numbers to strings
        let args = vec![SqlValue::Integer(123)];
        let result = trim(&args).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("123")));
    }
}
