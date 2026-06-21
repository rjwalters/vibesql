//! JSON functions (SQLite JSON1 extension compatibility)
//!
//! This module contains SQLite-compatible JSON functions:
//! - json(X) - Validate and minify JSON
//!
//! Reference: https://www.sqlite.org/json1.html

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// json(X) - Validate and minify JSON
///
/// The json(X) function verifies that its argument X is a valid JSON string
/// and returns a minified version of that JSON string (with all unnecessary
/// whitespace removed). If X is not a well-formed JSON string, then this
/// function throws an error.
///
/// If the argument is NULL, returns NULL.
///
/// Reference: https://www.sqlite.org/json1.html#the_json_function
pub(crate) fn json(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "json".to_string() });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Parse the JSON to validate it. SQLite's json() accepts a relaxed
            // JSON5-like superset (unquoted object keys, single-quoted strings,
            // trailing commas, comments, etc.). Try strict serde_json first for
            // speed and exact behavior on canonical JSON, then fall back to a
            // JSON5 parser so inputs like `{a:3}` are accepted, matching SQLite.
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(s.as_str())
                .or_else(|_| json5::from_str::<serde_json::Value>(s.as_str()));
            match parsed {
                Ok(value) => {
                    // Re-serialize to minified, strict JSON (compact format)
                    let minified = serde_json::to_string(&value).map_err(|e| {
                        ExecutorError::SqliteCompatError(format!("malformed JSON: {}", e))
                    })?;
                    Ok(SqlValue::Varchar(minified.into()))
                }
                Err(_) => {
                    // SQLite returns "malformed JSON" for invalid JSON
                    Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()))
                }
            }
        }
        // For non-string types, SQLite throws an error
        _ => Err(ExecutorError::SqliteCompatError(
            "JSON functions require string arguments".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_valid_array() {
        let result = json(&[SqlValue::Varchar("[1,2,3]".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("[1,2,3]".into()));
    }

    #[test]
    fn test_json_minifies_whitespace() {
        let result = json(&[SqlValue::Varchar("  { \"a\" : 1 }  ".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("{\"a\":1}".into()));
    }

    #[test]
    fn test_json_null_input() {
        let result = json(&[SqlValue::Null]).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_json_invalid_json() {
        let result = json(&[SqlValue::Varchar("{invalid}".into())]);
        assert!(result.is_err());
        if let Err(ExecutorError::SqliteCompatError(msg)) = result {
            assert_eq!(msg, "malformed JSON");
        } else {
            panic!("Expected SqliteCompatError");
        }
    }

    #[test]
    fn test_json_string_value() {
        let result = json(&[SqlValue::Varchar("\"hello\"".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("\"hello\"".into()));
    }

    #[test]
    fn test_json_number_value() {
        let result = json(&[SqlValue::Varchar("42".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("42".into()));
    }

    #[test]
    fn test_json_boolean_value() {
        let result = json(&[SqlValue::Varchar("true".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("true".into()));
    }

    #[test]
    fn test_json_null_json_value() {
        let result = json(&[SqlValue::Varchar("null".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("null".into()));
    }

    #[test]
    fn test_json_nested_object() {
        let input = r#"{"a": {"b": [1, 2, 3]}, "c": "test"}"#;
        let result = json(&[SqlValue::Varchar(input.into())]).unwrap();
        // serde_json preserves key order in minified output
        assert_eq!(result, SqlValue::Varchar(r#"{"a":{"b":[1,2,3]},"c":"test"}"#.into()));
    }

    #[test]
    fn test_json_wrong_arg_count() {
        // No arguments
        let result = json(&[]);
        assert!(result.is_err());

        // Too many arguments
        let result = json(&[SqlValue::Varchar("[]".into()), SqlValue::Varchar("[]".into())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_non_string_input() {
        let result = json(&[SqlValue::Integer(42)]);
        assert!(result.is_err());
    }

    // SQLite's json() accepts a relaxed JSON5-like syntax. These regression
    // tests cover the aggorderby-9.x cases (unquoted keys) plus the broader
    // JSON5 features, verifying we canonicalize back to strict minified JSON.
    #[test]
    fn test_json_json5_unquoted_key() {
        let result = json(&[SqlValue::Varchar("{a:3}".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar(r#"{"a":3}"#.into()));
    }

    #[test]
    fn test_json_json5_multiple_unquoted_keys() {
        let result = json(&[SqlValue::Varchar("{x:2, y:5}".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar(r#"{"x":2,"y":5}"#.into()));
    }

    #[test]
    fn test_json_json5_single_quoted_string() {
        let result = json(&[SqlValue::Varchar("{'k':'v'}".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar(r#"{"k":"v"}"#.into()));
    }

    #[test]
    fn test_json_json5_trailing_comma() {
        let result = json(&[SqlValue::Varchar("[1,2,3,]".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("[1,2,3]".into()));
    }

    #[test]
    fn test_json_strict_json_still_rejects_garbage() {
        // Genuinely malformed input must still error even with JSON5 fallback.
        let result = json(&[SqlValue::Varchar("{not valid at all".into())]);
        assert!(result.is_err());
    }
}
