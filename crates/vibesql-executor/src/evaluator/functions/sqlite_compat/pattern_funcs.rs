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
                if s.len() == 1 {
                    s.chars().next()
                } else {
                    None
                }
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
