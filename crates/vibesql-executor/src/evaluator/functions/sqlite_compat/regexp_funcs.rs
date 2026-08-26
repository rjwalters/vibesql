//! `regexp(pattern, text)` / `regexpi(pattern, text)` — VibeSQL's opt-in
//! emulation of SQLite's `ext/misc/regexp.c` static test extension
//! (`test_regexp.c`), backing the `X REGEXP Y` infix operator (parsed as
//! `regexp(Y, X)`, see `vibesql-parser`'s `build_match_regexp_call`) once a
//! session has explicitly opted in via `PRAGMA enable_regexp_extension = 1`.
//!
//! **Not registered by default.** Real SQLite ships no built-in `regexp()`
//! (R-41650-20872 / R-33693-50180); `X REGEXP Y` with no extension loaded
//! correctly raises "no such function: REGEXP" — an assertion `e_expr.test`
//! (`e_expr-18.1.1`/`e_expr-18.1.2`) depends on. These two functions are only
//! reachable from `evaluator::expressions::special::eval_function` /
//! `evaluator::combined::special::eval_function` / `evaluator::arena`'s
//! function-call dispatch, and only when the *current session's*
//! `Database::regexp_extension_enabled()` flag is set — see #6576.
//!
//! Patterns are compiled with the `regex` crate, a practical superset of the
//! subset SQLite's own hand-rolled NFA engine supports for these tests:
//! literal characters, `.`, `*`, `+`, `?`, `{n}`/`{n,m}`, `[...]`/`[^...]`,
//! `^`/`$` anchors, `|` alternation, `(...)` grouping, and the `\b`/`\w`/`\W`/
//! `\s`/`\S` Perl classes SQLite's engine also implements.
//! `regexp(pattern, text)` matches like SQLite's `re_match()` — a substring
//! search, NOT a full-string match — so a pattern with no `^`/`$` anchors
//! matches anywhere within `text` (exactly `Regex::is_match`'s behavior).

use std::borrow::Cow;

use regex::RegexBuilder;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Mirrors SQLite's `ext/misc/regexp.c` `re_maxlen()`: `75 +
/// SQLITE_LIMIT_LIKE_PATTERN_LENGTH/2`, using SQLite's compiled-in default of
/// 50000 for that limit (VibeSQL exposes no runtime `sqlite3_limit`
/// equivalent to lower it, so this is a fixed budget rather than a
/// configurable one). A `{m}`/`{m,n}` repeat count whose value*2 exceeds this
/// budget is rejected with the same "REGEXP pattern too big" text SQLite's
/// pattern compiler raises (regexp2.test 5.1/5.3: `a{1,999}bc` and
/// `a{999}bc` fit — 999*2 = 1998 <= 25075 — while `a{1,25000}bc` and
/// `a{25000}bc` do not — 25000*2 = 50000 > 25075).
const RE_MAX_ALLOC: u64 = 75 + 50000 / 2;

/// Scan `pattern` for `{m}` / `{m,n}` repeat quantifiers and reject any whose
/// bound exceeds [`RE_MAX_ALLOC`] once doubled, matching
/// `docs/reference/sqlite/ext/misc/regexp.c`'s `case '{':` compile-time
/// guard. Operates byte-wise: `\` escapes the next byte (so `\{` is never
/// treated as the start of a quantifier), and all other bytes — including
/// UTF-8 continuation bytes, which are always >= 0x80 and so never collide
/// with `\`, `{`, or ASCII digits — are otherwise passed through untouched.
fn check_repeat_limits(pattern: &str) -> Result<(), ExecutorError> {
    let bytes = pattern.as_bytes();
    let mut i = 0;
    let mut escaped = false;
    while i < bytes.len() {
        let b = bytes[i];
        if escaped {
            escaped = false;
            i += 1;
            continue;
        }
        match b {
            b'\\' => {
                escaped = true;
                i += 1;
            }
            b'{' => {
                let mut j = i + 1;
                let mstart = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > mstart {
                    check_count(&pattern[mstart..j])?;
                }
                if j < bytes.len() && bytes[j] == b',' {
                    j += 1;
                    let nstart = j;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    if j > nstart {
                        check_count(&pattern[nstart..j])?;
                    }
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    Ok(())
}

fn check_count(digits: &str) -> Result<(), ExecutorError> {
    if let Ok(n) = digits.parse::<u64>() {
        if n.saturating_mul(2) > RE_MAX_ALLOC {
            return Err(ExecutorError::SqliteCompatError("REGEXP pattern too big".to_string()));
        }
    }
    Ok(())
}

/// SQLite coerces both REGEXP arguments through `sqlite3_value_text()`
/// (implicit ANY -> TEXT affinity), the same rule already applied to LIKE's
/// and GLOB's subject argument in `pattern_funcs.rs`.
fn coerce_to_text(value: &SqlValue) -> Cow<'_, str> {
    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => Cow::Borrowed(s.as_str()),
        SqlValue::Integer(i) => Cow::Owned(i.to_string()),
        SqlValue::Bigint(i) => Cow::Owned(i.to_string()),
        SqlValue::Smallint(i) => Cow::Owned(i.to_string()),
        SqlValue::Unsigned(u) => Cow::Owned(u.to_string()),
        // Floats coerce through the SqlValue Display impl (%!.15g), matching
        // the LIKE/GLOB coercion above (and SQLite's own TEXT affinity cast).
        SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Numeric(_) | SqlValue::Float(_) => {
            Cow::Owned(value.to_string())
        }
        SqlValue::Boolean(b) => Cow::Owned(if *b { "1".to_string() } else { "0".to_string() }),
        other => Cow::Owned(other.to_string()),
    }
}

fn regexp_impl(
    fn_name: &str,
    args: &[SqlValue],
    case_insensitive: bool,
) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: fn_name.to_string() });
    }
    // NULL propagation, matching SQLite's re_sql_func: a NULL pattern
    // returns before ever setting a result (implicit NULL), and a NULL
    // subject also leaves the result unset (implicit NULL).
    if matches!(args[0], SqlValue::Null) || matches!(args[1], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let pattern = coerce_to_text(&args[0]);
    let text = coerce_to_text(&args[1]);

    check_repeat_limits(&pattern)?;

    let re =
        RegexBuilder::new(&pattern).case_insensitive(case_insensitive).build().map_err(|e| {
            ExecutorError::SqliteCompatError(format!("malformed REGEXP pattern: {}", e))
        })?;

    Ok(SqlValue::Integer(if re.is_match(&text) { 1 } else { 0 }))
}

/// `regexp(pattern, text)` — case-sensitive substring regex match. Backs the
/// default `X REGEXP Y` operator once `PRAGMA enable_regexp_extension` has
/// enabled it for the session (#6576).
pub(crate) fn regexp(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    regexp_impl("regexp", args, false)
}

/// `regexpi(pattern, text)` — case-insensitive counterpart. SQLite's
/// `ext/misc/regexp.c` registers this as a second, always-caseless function
/// alongside `regexp()` (`sqlite3_create_function(..., "regexpi", ...,
/// (void*)1, ...)`); it has no infix-operator form of its own and is only
/// ever called directly (regexp1.test regexp1-1.1.2..5).
pub(crate) fn regexpi(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    regexp_impl("regexpi", args, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regexp_basic_substring_search() {
        assert_eq!(
            regexp(&[
                SqlValue::Varchar("^For ".into()),
                SqlValue::Varchar("For since by man".into())
            ])
            .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[
                SqlValue::Varchar("by|Christ".into()),
                SqlValue::Varchar("even so in Christ shall".into())
            ])
            .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[SqlValue::Varchar("^xyz".into()), SqlValue::Varchar("abc xyz".into())])
                .unwrap(),
            SqlValue::Integer(0)
        );
    }

    #[test]
    fn test_regexpi_case_insensitive_vs_regexp_case_sensitive() {
        assert_eq!(
            regexpi(&[SqlValue::Varchar("abc".into()), SqlValue::Varchar("ABC".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[SqlValue::Varchar("abc".into()), SqlValue::Varchar("ABC".into())]).unwrap(),
            SqlValue::Integer(0)
        );
    }

    #[test]
    fn test_null_propagation() {
        assert_eq!(
            regexp(&[SqlValue::Null, SqlValue::Varchar("x".into())]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            regexp(&[SqlValue::Varchar("x".into()), SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_quantifiers_and_char_classes() {
        // `{n}` / `{n,m}` and character-class patterns (regexp1.test 1.7-1.13).
        assert_eq!(
            regexp(&[SqlValue::Varchar("r{2}".into()), SqlValue::Varchar("resurrection".into())])
                .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[SqlValue::Varchar("[Aa]dam".into()), SqlValue::Varchar("Adam".into())])
                .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[SqlValue::Varchar("[^Aa]dam".into()), SqlValue::Varchar("Adam".into())])
                .unwrap(),
            SqlValue::Integer(0)
        );
    }

    #[test]
    fn test_repeat_limit_matches_sqlite_boundary() {
        // Mirrors regexp2.test 5.0-5.3 exactly: 999 fits, 25000 does not.
        assert!(regexp(&[SqlValue::Varchar("a{1,999}bc".into()), SqlValue::Varchar("abc".into())])
            .is_ok());
        assert!(regexp(&[SqlValue::Varchar("a{999}bc".into()), SqlValue::Varchar("abc".into())])
            .is_ok());
        let too_big =
            regexp(&[SqlValue::Varchar("a{1,25000}bc".into()), SqlValue::Varchar("abc".into())]);
        assert!(too_big.is_err());
        assert_eq!(too_big.unwrap_err().to_string(), "REGEXP pattern too big");
        let too_big2 =
            regexp(&[SqlValue::Varchar("a{25000}bc".into()), SqlValue::Varchar("abc".into())]);
        assert!(too_big2.is_err());
    }

    #[test]
    fn test_wrong_arity() {
        assert!(regexp(&[SqlValue::Varchar("a".into())]).is_err());
        assert!(regexp(&[]).is_err());
    }

    #[test]
    fn test_word_boundary_and_classes() {
        // regexp1.test 1.20-1.25 (\b, \w, \W, \s, \S).
        assert_eq!(
            regexp(&[SqlValue::Varchar("\\bma[nd]".into()), SqlValue::Varchar("by man".into())])
                .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[SqlValue::Varchar("\\W".into()), SqlValue::Varchar("a c".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            regexp(&[SqlValue::Varchar("\\W".into()), SqlValue::Varchar("abc".into())]).unwrap(),
            SqlValue::Integer(0)
        );
    }
}
