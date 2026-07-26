//! Pattern matching functions
//!
//! This module contains SQLite-compatible pattern matching functions:
//! - LIKE(pattern, string) - SQL LIKE pattern matching
//! - GLOB(pattern, string) - Unix-style pattern matching

use std::borrow::Cow;

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// LIKE(pattern, string) - SQL LIKE pattern matching as a function
///
/// Returns 1 if string matches pattern, 0 otherwise.
/// Pattern syntax:
/// - % matches any sequence of characters (including empty)
/// - _ matches exactly one character
///
/// LIKE is case-insensitive for ASCII letters (SQLite default)
/// Optionally takes a 3rd argument for escape character.
pub(crate) fn like(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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
        // Floats coerce through the SqlValue Display impl (%!.15g), not the raw
        // f64/f32 `to_string()`. SQLite compares against the %!.15g rendering, so
        // `2.0 LIKE '2.0'` is true and `1e300 LIKE '1.0e+300'` is true.
        SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Numeric(_) | SqlValue::Float(_) => {
            Cow::Owned(args[1].to_string())
        }
        SqlValue::Boolean(b) => Cow::Owned(if *b { "1".to_string() } else { "0".to_string() }),
        other => Cow::Owned(other.to_string()),
    };

    // Optional escape character (3rd argument)
    let escape_char: Option<char> = if args.len() >= 3 {
        match &args[2] {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let mut chars = s.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) => Some(c), // Exactly one character
                    _ => {
                        // Empty string or multi-character string: error per SQLite
                        return Err(ExecutorError::SqliteCompatError(
                            "ESCAPE expression must be a single character".to_string(),
                        ));
                    }
                }
            }
            _ => {
                return Err(ExecutorError::SqliteCompatError(
                    "ESCAPE expression must be a single character".to_string(),
                ));
            }
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
pub(crate) fn glob(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
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
        // Floats coerce through the SqlValue Display impl (%!.15g), not the raw
        // f64/f32 `to_string()`, matching SQLite's GLOB coercion.
        SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Numeric(_) | SqlValue::Float(_) => {
            Cow::Owned(args[1].to_string())
        }
        SqlValue::Boolean(b) => Cow::Owned(if *b { "1".to_string() } else { "0".to_string() }),
        other => Cow::Owned(other.to_string()),
    };

    // Use the pattern matching function from pattern.rs
    let matched = crate::evaluator::pattern::glob_match(&text, pattern);
    Ok(SqlValue::Integer(if matched { 1 } else { 0 }))
}

/// match(pattern, string) - the default `match()` application-defined function
/// that backs the `X MATCH Y` infix operator (parsed as `match(Y, X)`).
///
/// SQLite ships a genuine default implementation of `match()` (R-42037-37826):
/// unlike `regexp()` (which simply doesn't exist unless an extension registers
/// it), `match()` is always present and its default behavior is to raise this
/// exact error — it is only useful once an extension (e.g. FTS3/4/5) or a
/// user-registered function overrides it with real matching logic. VibeSQL has
/// no virtual-table MATCH override mechanism, so this default is always what
/// callers get, matching SQLite's out-of-the-box behavior.
pub(crate) fn match_default(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "match".to_string() });
    }
    Err(ExecutorError::SqliteCompatError(
        "unable to use function MATCH in the requested context".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(
            like(&[SqlValue::Null, SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            like(&[SqlValue::Varchar("hello".into()), SqlValue::Null]).unwrap(),
            SqlValue::Null
        );

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
        assert_eq!(
            glob(&[SqlValue::Null, SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            glob(&[SqlValue::Varchar("hello".into()), SqlValue::Null]).unwrap(),
            SqlValue::Null
        );

        // Wrong number of arguments
        assert!(glob(&[]).is_err());
        assert!(glob(&[SqlValue::Varchar("a".into())]).is_err());
    }
}
