//! Conditional functions
//!
//! This module contains SQLite-compatible conditional functions:
//! - IIF(condition, true_val, false_val) - Inline if (ternary)
//! - IFNULL(x, y) - Return y if x is NULL
//! - ERROR([msg]) - Always raises an error (test-helper function)

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// IIF(...) - Inline if (SQLite ternary / 3.48+ variadic)
///
/// Two supported forms:
/// - `IIF(condition, true_value, false_value)` — equivalent to `CASE WHEN condition THEN true_value
///   ELSE false_value END`.
/// - `IIF(c1, v1, c2, v2, ..., else)` — CASE-chain form (odd argument count, `>= 3`); returns the
///   value paired with the first truthy condition, or the trailing `else` argument if none match.
///
/// Conditions use SQLite truthiness rules
/// (`crate::evaluator::operators::is_truthy`): `NULL` and numeric zero are
/// false, non-zero numerics are true, and strings coerce via their leading
/// numeric portion. This shares the exact implementation used by `IF` and
/// CASE/WHERE so all conditional paths agree.
pub(crate) fn iif(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    crate::evaluator::functions::control::variadic_conditional("IIF", args)
}

/// IFNULL(x, y) - Return y if x is NULL, otherwise return x
///
/// This is an alias for COALESCE(x, y) with exactly 2 arguments.
pub(crate) fn ifnull(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "IFNULL requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    if matches!(args[0], SqlValue::Null) {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

/// ERROR([msg]) - Always raises an error; a narrow test-helper builtin.
///
/// This is not a genuine SQLite C-level builtin (SQLite's own test harness
/// only registers this name per-file via TCL's `db func` mechanism — the
/// general `db func` gap is tracked separately in #5720). VibeSQL implements
/// it directly to reproduce the exact combined semantics of the two SQLite
/// TCL test files that register it themselves (`regexp2.test`, `analyzeF.test`):
///
/// - `ERROR()` (0 args) raises the literal text `SQL error!`
/// - `ERROR(msg)` (1 arg) raises `msg` verbatim, with no added prefix
///
/// Uses `ExecutorError::SqliteCompatError`, whose `Display` impl writes the
/// message with no prefix — required for an exact-string match against
/// `do_catchsql_test`'s literal expected output. `UnsupportedFeature` would
/// NOT work here: it wraps its message in a template.
pub(crate) fn error_func(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    match args.len() {
        0 => Err(ExecutorError::SqliteCompatError("SQL error!".to_string())),
        1 => Err(ExecutorError::SqliteCompatError(args[0].to_string())),
        n => Err(ExecutorError::UnsupportedFeature(format!(
            "ERROR requires 0 or 1 arguments, got {}",
            n
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iif() {
        // True condition
        assert_eq!(
            iif(&[SqlValue::Boolean(true), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );

        // False condition
        assert_eq!(
            iif(&[SqlValue::Boolean(false), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );

        // NULL condition (treated as false)
        assert_eq!(
            iif(&[SqlValue::Null, SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );

        // Non-zero integer is truthy
        assert_eq!(
            iif(&[SqlValue::Integer(5), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );

        // Zero is falsy
        assert_eq!(
            iif(&[SqlValue::Integer(0), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );

        // String condition uses leading-numeric coercion (SQLite truthiness)
        assert_eq!(
            iif(&[
                SqlValue::Varchar(arcstr::ArcStr::from("1abc")),
                SqlValue::Integer(1),
                SqlValue::Integer(2)
            ])
            .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            iif(&[
                SqlValue::Varchar(arcstr::ArcStr::from("abc")),
                SqlValue::Integer(1),
                SqlValue::Integer(2)
            ])
            .unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_iif_variadic() {
        // First branch true -> first value
        assert_eq!(
            iif(&[
                SqlValue::Boolean(true),
                SqlValue::Integer(1),
                SqlValue::Boolean(true),
                SqlValue::Integer(2),
                SqlValue::Integer(99),
            ])
            .unwrap(),
            SqlValue::Integer(1)
        );

        // First branch false, second branch true -> second value
        assert_eq!(
            iif(&[
                SqlValue::Boolean(false),
                SqlValue::Integer(1),
                SqlValue::Boolean(true),
                SqlValue::Integer(2),
                SqlValue::Integer(99),
            ])
            .unwrap(),
            SqlValue::Integer(2)
        );

        // No branch true -> trailing else value
        assert_eq!(
            iif(&[
                SqlValue::Boolean(false),
                SqlValue::Integer(1),
                SqlValue::Boolean(false),
                SqlValue::Integer(2),
                SqlValue::Integer(99),
            ])
            .unwrap(),
            SqlValue::Integer(99)
        );

        // NULL condition is falsy and skips its branch
        assert_eq!(
            iif(&[
                SqlValue::Null,
                SqlValue::Integer(1),
                SqlValue::Integer(0),
                SqlValue::Integer(2),
                SqlValue::Integer(99),
            ])
            .unwrap(),
            SqlValue::Integer(99)
        );

        // 5-arg mixed-type CASE chain (mirrors strict1.test usage):
        // iif(0, 'a', 0.0, 'b', 'c') -> 'c'
        assert_eq!(
            iif(&[
                SqlValue::Integer(0),
                SqlValue::Varchar(arcstr::ArcStr::from("a")),
                SqlValue::Real(0.0),
                SqlValue::Varchar(arcstr::ArcStr::from("b")),
                SqlValue::Varchar(arcstr::ArcStr::from("c")),
            ])
            .unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("c"))
        );
    }

    #[test]
    fn test_iif_two_arg_form() {
        // SQLite `iif(X, Y)` — implicit ELSE NULL (2-arg form is valid).
        assert_eq!(
            iif(&[SqlValue::Boolean(true), SqlValue::Integer(1)]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(iif(&[SqlValue::Boolean(false), SqlValue::Integer(1)]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_iif_invalid_arity() {
        // Even argument counts >= 4 are rejected (CASE-chain must be odd).
        assert!(iif(&[
            SqlValue::Boolean(true),
            SqlValue::Integer(1),
            SqlValue::Boolean(true),
            SqlValue::Integer(2),
        ])
        .is_err());
        // One argument and zero arguments are rejected.
        assert!(iif(&[SqlValue::Boolean(true)]).is_err());
        assert!(iif(&[]).is_err());
    }

    #[test]
    fn test_ifnull() {
        assert_eq!(
            ifnull(&[SqlValue::Null, SqlValue::Integer(42)]).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            ifnull(&[SqlValue::Integer(1), SqlValue::Integer(42)]).unwrap(),
            SqlValue::Integer(1)
        );
    }

    #[test]
    fn test_error_zero_args_raises_exact_text() {
        // ERROR() (0 args) raises the literal text "SQL error!" (regexp2.test 2.1).
        let err = error_func(&[]).unwrap_err();
        assert_eq!(err.to_string(), "SQL error!");
        assert!(matches!(err, ExecutorError::SqliteCompatError(ref msg) if msg == "SQL error!"));
    }

    #[test]
    fn test_error_one_arg_raises_verbatim_message() {
        // ERROR(msg) (1 arg) raises msg verbatim, no added prefix (analyzeF.test 4.1).
        let err = error_func(&[SqlValue::Varchar(arcstr::ArcStr::from("error one"))]).unwrap_err();
        assert_eq!(err.to_string(), "error one");
        assert!(matches!(err, ExecutorError::SqliteCompatError(ref msg) if msg == "error one"));
    }

    #[test]
    fn test_error_invalid_arity_is_normal_error_not_panic() {
        // 2+ args returns a normal argument-count error rather than panicking.
        let err = error_func(&[SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap_err();
        assert!(matches!(err, ExecutorError::UnsupportedFeature(_)));
    }
}
