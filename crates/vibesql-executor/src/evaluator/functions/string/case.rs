//! Case conversion functions for SQL strings
//!
//! SQL:1999 Section 6.29: String value functions

use crate::{errors::ExecutorError, evaluator::functions::coercion::coerce_to_string};

/// UPPER(string) - Convert string to uppercase
/// SQL:1999 Section 6.29: String value functions
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
/// - UPPER(123) returns "123"
/// - UPPER(7.5) returns "7.5"
pub(in crate::evaluator::functions) fn upper(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UPPER requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match coerce_to_string(&args[0]) {
        None => Ok(vibesql_types::SqlValue::Null),
        Some(s) => Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase()))),
    }
}

/// LOWER(string) - Convert string to lowercase
/// SQL:1999 Section 6.29: String value functions
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
/// - LOWER(123) returns "123"
/// - LOWER(7.5) returns "7.5"
pub(in crate::evaluator::functions) fn lower(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LOWER requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match coerce_to_string(&args[0]) {
        None => Ok(vibesql_types::SqlValue::Null),
        Some(s) => Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(s.to_lowercase()))),
    }
}
