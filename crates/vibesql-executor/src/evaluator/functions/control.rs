//! Control flow functions (IF)

use crate::{errors::ExecutorError, evaluator::operators::is_truthy};

/// IF(...) - MySQL-style / SQLite variadic conditional
///
/// Supported forms:
/// - `IF(condition, true_value)` — 2-argument form (SQLite `if(X,Y)`): returns `true_value` if
///   `condition` is truthy, otherwise `NULL` (implicit ELSE).
/// - `IF(condition, true_value, false_value)` — ternary; returns `true_value` if `condition` is
///   truthy, otherwise `false_value`.
/// - `IF(c1, v1, c2, v2, ..., else)` — CASE-chain form (odd argument count, `>= 3`). Evaluates each
///   condition in order and returns the value paired with the first truthy condition; if none
///   match, returns the trailing `else` argument.
///
/// Conditions use SQLite truthiness rules (see
/// `crate::evaluator::operators::is_truthy`): `NULL` and numeric zero are
/// false, non-zero numerics are true, and strings coerce via their leading
/// numeric portion. This matches `IIF` and CASE/WHERE semantics.
pub(super) fn if_func(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    variadic_conditional("IF", args)
}

/// Shared CASE-chain evaluation for the variadic `IF`/`IIF` forms.
///
/// Accepts the 2-argument `if(X, Y)` form (implicit `ELSE NULL`, matching
/// SQLite) and any odd argument count `>= 3` (CASE-chain). Iterates over
/// `(condition, value)` pairs, returning the value for the first truthy
/// condition; if no condition is truthy, returns the trailing else argument
/// (or `NULL` when there is no trailing else, i.e. the 2-arg form).
pub(crate) fn variadic_conditional(
    fn_name: &str,
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Valid arities: exactly 2 (implicit ELSE NULL) or any odd count >= 3.
    if args.len() < 2 || (args.len() > 2 && args.len().is_multiple_of(2)) {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: fn_name.to_string() });
    }

    // Evaluate (condition, value) pairs in order.
    let mut i = 0;
    while i + 1 < args.len() {
        if is_truthy(&args[i]) {
            return Ok(args[i + 1].clone());
        }
        i += 2;
    }

    // Odd-arity forms carry a trailing else argument; the 2-arg form does not
    // (its implicit else is NULL).
    if !args.len().is_multiple_of(2) {
        Ok(args[args.len() - 1].clone())
    } else {
        Ok(vibesql_types::SqlValue::Null)
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_if_ternary_boolean() {
        assert_eq!(
            if_func(&[SqlValue::Boolean(true), SqlValue::Integer(1), SqlValue::Integer(2)])
                .unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            if_func(&[SqlValue::Boolean(false), SqlValue::Integer(1), SqlValue::Integer(2)])
                .unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_if_ternary_integer_condition() {
        // Non-boolean conditions previously errored; now use SQLite truthiness.
        assert_eq!(
            if_func(&[
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("yes")),
                SqlValue::Varchar(arcstr::ArcStr::from("no")),
            ])
            .unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("yes"))
        );
        assert_eq!(
            if_func(&[
                SqlValue::Integer(0),
                SqlValue::Varchar(arcstr::ArcStr::from("yes")),
                SqlValue::Varchar(arcstr::ArcStr::from("no")),
            ])
            .unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("no"))
        );
    }

    #[test]
    fn test_if_ternary_float_condition() {
        assert_eq!(
            if_func(&[SqlValue::Real(1.5), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            if_func(&[SqlValue::Real(0.0), SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_if_null_condition_is_false() {
        assert_eq!(
            if_func(&[
                SqlValue::Null,
                SqlValue::Varchar(arcstr::ArcStr::from("a")),
                SqlValue::Varchar(arcstr::ArcStr::from("b")),
            ])
            .unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("b"))
        );
    }

    #[test]
    fn test_if_variadic_case_chain() {
        // if(0, 'a', 0.0, 'b', 'c') -> 'c'
        assert_eq!(
            if_func(&[
                SqlValue::Integer(0),
                SqlValue::Varchar(arcstr::ArcStr::from("a")),
                SqlValue::Real(0.0),
                SqlValue::Varchar(arcstr::ArcStr::from("b")),
                SqlValue::Varchar(arcstr::ArcStr::from("c")),
            ])
            .unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("c"))
        );

        // Mirrors strict1.test: if(k=11,1.5, k=12,2, k=13,'x', 0.0)
        // with k=12 -> second condition true -> 2
        assert_eq!(
            if_func(&[
                SqlValue::Boolean(false),
                SqlValue::Real(1.5),
                SqlValue::Boolean(true),
                SqlValue::Integer(2),
                SqlValue::Boolean(false),
                SqlValue::Varchar(arcstr::ArcStr::from("x")),
                SqlValue::Real(0.0),
            ])
            .unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_if_two_arg_form() {
        // SQLite `if(X, Y)` — implicit ELSE NULL.
        // Truthy condition returns Y.
        assert_eq!(
            if_func(&[SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("y"))]).unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("y"))
        );
        // Falsy condition returns NULL.
        assert_eq!(
            if_func(&[SqlValue::Integer(0), SqlValue::Varchar(arcstr::ArcStr::from("y"))]).unwrap(),
            SqlValue::Null
        );
        // NULL condition is falsy -> NULL.
        assert_eq!(if_func(&[SqlValue::Null, SqlValue::Integer(5)]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_if_invalid_arity() {
        // Even argument counts >= 4 are rejected (CASE-chain must be odd).
        assert!(if_func(&[
            SqlValue::Boolean(true),
            SqlValue::Integer(1),
            SqlValue::Boolean(false),
            SqlValue::Integer(2),
        ])
        .is_err());
        // One argument and zero arguments are rejected.
        assert!(if_func(&[SqlValue::Integer(1)]).is_err());
        assert!(if_func(&[]).is_err());
    }
}
