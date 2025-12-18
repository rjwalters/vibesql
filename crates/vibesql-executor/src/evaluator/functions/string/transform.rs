//! String transformation functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;
use crate::evaluator::functions::coercion::coerce_to_string;

/// REPLACE(string, from, to) - Replace occurrences
/// SQL:1999 Section 6.29: String value functions
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
pub(in crate::evaluator::functions) fn replace(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "REPLACE requires exactly 3 arguments, got {}",
            args.len()
        )));
    }

    // Coerce all arguments to strings
    let text = match coerce_to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };
    let from = match coerce_to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };
    let to = match coerce_to_string(&args[2]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
        text.replace(&from, &to),
    )))
}

/// REVERSE(string) - Reverse a string
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
pub(in crate::evaluator::functions) fn reverse(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "REVERSE requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match coerce_to_string(&args[0]) {
        None => Ok(vibesql_types::SqlValue::Null),
        Some(s) => {
            let reversed: String = s.chars().rev().collect();
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                reversed,
            )))
        }
    }
}
