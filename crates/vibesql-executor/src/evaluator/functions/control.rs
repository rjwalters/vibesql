//! Control flow functions (IF)

use crate::errors::ExecutorError;
use crate::evaluator::operators::is_truthy;

/// IF(...) - MySQL-style / SQLite 3.48+ variadic conditional
///
/// Two supported forms:
/// - `IF(condition, true_value, false_value)` — ternary; returns `true_value`
///   if `condition` is truthy, otherwise `false_value`.
/// - `IF(c1, v1, c2, v2, ..., else)` — CASE-chain form (odd argument count,
///   `>= 3`). Evaluates each condition in order and returns the value paired
///   with the first truthy condition; if none match, returns the trailing
///   `else` argument.
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
/// Requires an odd argument count `>= 3`. Iterates over `(condition, value)`
/// pairs, returning the value for the first truthy condition; if no condition
/// is truthy, returns the trailing else argument.
pub(crate) fn variadic_conditional(
    fn_name: &str,
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "{fn_name} requires an odd number of arguments (>= 3), got {}",
            args.len()
        )));
    }

    // Evaluate (condition, value) pairs in order; the final argument is the
    // else value returned when no condition matches.
    let mut i = 0;
    while i + 1 < args.len() {
        if is_truthy(&args[i]) {
            return Ok(args[i + 1].clone());
        }
        i += 2;
    }

    // No condition matched — return the trailing else argument.
    Ok(args[args.len() - 1].clone())
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
    fn test_if_invalid_arity() {
        // Even argument counts are rejected.
        assert!(if_func(&[SqlValue::Boolean(true), SqlValue::Integer(1)]).is_err());
        assert!(if_func(&[]).is_err());
    }
}
