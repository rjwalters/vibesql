//! String transformation functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// REPLACE(string, from, to) - Replace occurrences
/// SQL:1999 Section 6.29: String value functions
pub(in crate::evaluator::functions) fn replace(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "REPLACE requires exactly 3 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1], &args[2]) {
        (vibesql_types::SqlValue::Null, _, _)
        | (_, vibesql_types::SqlValue::Null, _)
        | (_, _, vibesql_types::SqlValue::Null) => Ok(vibesql_types::SqlValue::Null),
        (
            vibesql_types::SqlValue::Varchar(text) | vibesql_types::SqlValue::Character(text),
            vibesql_types::SqlValue::Varchar(from) | vibesql_types::SqlValue::Character(from),
            vibesql_types::SqlValue::Varchar(to) | vibesql_types::SqlValue::Character(to),
        ) => Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(text.replace(&**from, to)))),
        (a, b, c) => Err(ExecutorError::UnsupportedFeature(format!(
            "REPLACE requires string arguments, got {:?}, {:?}, {:?}",
            a, b, c
        ))),
    }
}

/// REVERSE(string) - Reverse a string
pub(in crate::evaluator::functions) fn reverse(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "REVERSE requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            let reversed: String = s.chars().rev().collect();
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(reversed)))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "REVERSE requires string argument, got {:?}",
            val
        ))),
    }
}
