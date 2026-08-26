//! Pattern matching functions
//!
//! This module contains SQLite-compatible pattern matching functions:
//! - LIKE(pattern, string) - SQL LIKE pattern matching
//! - GLOB(pattern, string) - Unix-style pattern matching

use std::{borrow::Cow, sync::LazyLock};

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

/// SQLite's `regexp()`/`regexpi()` test-extension (ext/misc/regexp.c) caps
/// repeat-quantifier counts (`{n}` / `{n,m}`) to guard against NFA-size
/// blowup; exceeding the cap raises "REGEXP pattern too big"
/// (regexp2.test 5.1/5.3: `a{1,999}bc`/`a{999}bc` succeed,
/// `a{1,25000}bc`/`a{25000}bc` fail). 1000 sits between the known-passing
/// and known-failing boundary cases in the SQLite test suite (the extension's
/// own internal constant is not part of its documented public interface).
const MAX_REGEXP_REPEAT: u32 = 1000;

static REPEAT_QUANTIFIER_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\{(\d+)(,(\d*))?\}").expect("valid literal regex"));

fn check_regexp_repeat_bounds(pattern: &str) -> Result<(), ExecutorError> {
    for caps in REPEAT_QUANTIFIER_RE.captures_iter(pattern) {
        for group in [caps.get(1), caps.get(3)] {
            if let Some(n) = group.and_then(|m| m.as_str().parse::<u32>().ok()) {
                if n > MAX_REGEXP_REPEAT {
                    return Err(ExecutorError::SqliteCompatError(
                        "REGEXP pattern too big".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Shared implementation backing `regexp()`/`regexpi()`.
///
/// `regexp(pattern, string)` / `regexpi(pattern, string)` implement the
/// SQLite `regexp`-test-extension-compatible functions (ext/misc/regexp.c)
/// that back the `X REGEXP Y` infix operator (parsed as `regexp(Y, X)` per
/// R-33693-50180). Unlike `match()` (always present, see [`match_default`]),
/// stock SQLite ships NO default `regexp()` at all (R-41650-20872) — it only
/// exists once an extension registers it. VibeSQL therefore gates these
/// functions behind the `enable_regexp_functions` PRAGMA (default OFF, so
/// `X REGEXP Y` raises "no such function" exactly like stock SQLite unless a
/// caller opts in — dispatch happens in `functions/mod.rs`, this module only
/// implements the matching logic once dispatch has already gated on the
/// PRAGMA).
fn regexp_impl(
    args: &[SqlValue],
    function_name: &str,
    case_insensitive: bool,
) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: function_name.to_string(),
        });
    }

    // NULL propagation - SQL standard semantics
    if matches!(args[0], SqlValue::Null) || matches!(args[1], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let pattern = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        other => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "{function_name} pattern must be a string, got {other:?}"
            )));
        }
    };

    // SQLite coerces non-string types to strings for REGEXP comparison,
    // mirroring GLOB's coercion above.
    let text: Cow<str> = match &args[1] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => Cow::Borrowed(s.as_str()),
        SqlValue::Integer(i) => Cow::Owned(i.to_string()),
        SqlValue::Bigint(i) => Cow::Owned(i.to_string()),
        SqlValue::Smallint(i) => Cow::Owned(i.to_string()),
        SqlValue::Unsigned(u) => Cow::Owned(u.to_string()),
        SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Numeric(_) | SqlValue::Float(_) => {
            Cow::Owned(args[1].to_string())
        }
        SqlValue::Boolean(b) => Cow::Owned(if *b { "1".to_string() } else { "0".to_string() }),
        other => Cow::Owned(other.to_string()),
    };

    check_regexp_repeat_bounds(pattern)?;

    let re = regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| ExecutorError::SqliteCompatError(format!("REGEXP pattern error: {e}")))?;

    Ok(SqlValue::Integer(if re.is_match(&text) { 1 } else { 0 }))
}

/// regexp(pattern, string) - case-sensitive extended-regex match.
///
/// See [`regexp_impl`] for the shared implementation and PRAGMA-gating notes.
pub(crate) fn regexp(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    regexp_impl(args, "regexp", false)
}

/// regexpi(pattern, string) - case-insensitive extended-regex match.
///
/// See [`regexp_impl`] for the shared implementation and PRAGMA-gating notes.
pub(crate) fn regexpi(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    regexp_impl(args, "regexpi", true)
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

    // regexp()/regexpi() — ported from SQLite's regexp1.test/regexp2.test
    // (Part of #6172). These functions are dispatch-gated behind the
    // `enable_regexp_functions` PRAGMA (see `functions/mod.rs`); the matching
    // logic itself is unconditional here, matching the pre-gate call site.

    fn s(text: &str) -> SqlValue {
        SqlValue::Varchar(text.into())
    }

    #[test]
    fn test_regexp_case_sensitive() {
        // regexp1-1.3.2 / 1.5.2 / 1.5.3
        assert_eq!(
            regexp(&[s("by|christ"), s("For since by man came death,")]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[s("by|christ"), s("even so in Christ shall all be made alive.")]).unwrap(),
            SqlValue::Integer(0)
        );
        assert_eq!(
            regexp(&[s("shall x*y*z*all"), s("even so in Christ shall all be made alive.")])
                .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[s("SHALL x*y*z*all"), s("even so in Christ shall all be made alive.")])
                .unwrap(),
            SqlValue::Integer(0)
        );
    }

    #[test]
    fn test_regexpi_case_insensitive() {
        // regexp1-1.1.2..1.1.5
        assert_eq!(regexpi(&[s("abc"), s("ABC")]).unwrap(), SqlValue::Integer(1));
        assert_eq!(regexpi(&[s("ABC"), s("ABC")]).unwrap(), SqlValue::Integer(1));
        assert_eq!(regexpi(&[s("ABC"), s("abc")]).unwrap(), SqlValue::Integer(1));
        assert_eq!(regexpi(&[s("ABC."), s("ABC")]).unwrap(), SqlValue::Integer(0));
        // regexp1-1.3.3/1.3.4/1.5.4
        assert_eq!(
            regexpi(&[s("by|christ"), s("even so in Christ shall all be made alive.")]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexpi(&[s("BY|CHRIST"), s("even so in Christ shall all be made alive.")]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexpi(&[s("SHALL x*y*z*all"), s("even so in Christ shall all be made alive.")])
                .unwrap(),
            SqlValue::Integer(1)
        );
    }

    #[test]
    fn test_regexp_char_classes() {
        // regexp2.test 4.1-4.18
        assert_eq!(regexp(&[s(r"\W"), s("abc")]).unwrap(), SqlValue::Integer(0));
        assert_eq!(regexp(&[s(r"\W"), s("a c")]).unwrap(), SqlValue::Integer(1));
        assert_eq!(regexp(&[s(r"\w"), s("abc")]).unwrap(), SqlValue::Integer(1));
        assert_eq!(regexp(&[s(r"\w"), s("   ")]).unwrap(), SqlValue::Integer(0));
        assert_eq!(regexp(&[s("[^a-z]"), s("abc")]).unwrap(), SqlValue::Integer(0));
        assert_eq!(regexp(&[s("[^a-z]"), s("a c")]).unwrap(), SqlValue::Integer(1));
        assert_eq!(regexp(&[s("[a-z]"), s("abc")]).unwrap(), SqlValue::Integer(1));
        assert_eq!(regexp(&[s("[a-z]"), s("   ")]).unwrap(), SqlValue::Integer(0));
    }

    #[test]
    fn test_regexp_repeat_bounds() {
        // regexp2.test 5.0-5.3
        assert_eq!(regexp(&[s("a{1,999}bc"), s("abc")]).unwrap(), SqlValue::Integer(1));
        assert!(regexp(&[s("a{1,25000}bc"), s("abc")]).is_err());
        assert_eq!(regexp(&[s("a{999}bc"), s("abc")]).unwrap(), SqlValue::Integer(0));
        assert!(regexp(&[s("a{25000}bc"), s("abc")]).is_err());
    }

    #[test]
    fn test_regexp_null_propagation() {
        assert_eq!(regexp(&[SqlValue::Null, s("abc")]).unwrap(), SqlValue::Null);
        assert_eq!(regexp(&[s("abc"), SqlValue::Null]).unwrap(), SqlValue::Null);
        assert_eq!(regexpi(&[SqlValue::Null, s("abc")]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_regexp_wrong_arg_count() {
        assert!(regexp(&[]).is_err());
        assert!(regexp(&[s("a")]).is_err());
        assert!(regexpi(&[]).is_err());
    }
}
