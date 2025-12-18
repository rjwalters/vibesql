//! String trimming functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;
use crate::evaluator::functions::coercion::coerce_to_string;

/// TRIM(string) - Remove leading and trailing spaces
/// SQL:1999 Section 6.29: String value functions
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
#[allow(dead_code)] // Reserved for future use when basic TRIM needs to be exposed
pub(in crate::evaluator::functions) fn trim(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TRIM requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match coerce_to_string(&args[0]) {
        None => Ok(vibesql_types::SqlValue::Null),
        Some(s) => Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
            s.trim().to_string(),
        ))),
    }
}

/// TRIM with position and custom character support
/// Supports TRIM(BOTH/LEADING/TRAILING 'x' FROM 'string')
/// SQL:1999 Section 6.29: String value functions
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
#[allow(dead_code)] // Currently called via eval_trim method, may be used directly in future
pub(crate) fn trim_advanced(
    string_val: vibesql_types::SqlValue,
    position: Option<vibesql_ast::TrimPosition>,
    removal_char: Option<vibesql_types::SqlValue>,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Extract string with coercion
    let s = match coerce_to_string(&string_val) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    // Determine character to remove (default: space)
    let char_to_remove = if let Some(removal) = removal_char {
        match removal {
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            vibesql_types::SqlValue::Varchar(c) | vibesql_types::SqlValue::Character(c) => {
                if c.len() != 1 {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "TRIM removal character must be single character, got '{}'",
                        c
                    )));
                }
                c.chars().next().unwrap()
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "TRIM removal character must be string, got {:?}",
                    removal
                )))
            }
        }
    } else {
        ' ' // Default to space
    };

    // Apply trimming based on position
    let result = match position.unwrap_or(vibesql_ast::TrimPosition::Both) {
        vibesql_ast::TrimPosition::Both => s.trim_matches(char_to_remove).to_string(),
        vibesql_ast::TrimPosition::Leading => s.trim_start_matches(char_to_remove).to_string(),
        vibesql_ast::TrimPosition::Trailing => s.trim_end_matches(char_to_remove).to_string(),
    };

    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
}
