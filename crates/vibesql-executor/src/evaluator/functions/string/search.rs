//! String search functions for SQL
//!
//! Functions for finding substrings within strings
//! Includes POSITION (SQL standard), INSTR and LOCATE (MySQL/Oracle)

use crate::errors::ExecutorError;

/// POSITION(substring IN string) - Find position (1-indexed)
/// SQL:1999 Section 6.29: String value functions
/// Note: This is called as POSITION('sub', 'string') in our implementation
pub(in crate::evaluator::functions) fn position(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "POSITION requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
            Ok(vibesql_types::SqlValue::Null)
        }
        (
            vibesql_types::SqlValue::Varchar(needle) | vibesql_types::SqlValue::Character(needle),
            vibesql_types::SqlValue::Varchar(haystack)
            | vibesql_types::SqlValue::Character(haystack),
        ) => {
            // Find returns 0-indexed position, SQL needs 1-indexed
            match haystack.find(&**needle) {
                Some(pos) => Ok(vibesql_types::SqlValue::Integer((pos + 1) as i64)),
                None => Ok(vibesql_types::SqlValue::Integer(0)),
            }
        }
        (a, b) => Err(ExecutorError::UnsupportedFeature(format!(
            "POSITION requires string arguments, got {:?} and {:?}",
            a, b
        ))),
    }
}

/// Convert SqlValue to string for INSTR/POSITION/LOCATE functions
/// SQLite converts numeric values to strings for these functions
fn sql_value_to_string(val: &vibesql_types::SqlValue) -> Option<String> {
    match val {
        vibesql_types::SqlValue::Null => None,
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            Some(s.to_string())
        }
        vibesql_types::SqlValue::Integer(n) | vibesql_types::SqlValue::Bigint(n) => {
            Some(n.to_string())
        }
        vibesql_types::SqlValue::Smallint(n) => Some(n.to_string()),
        vibesql_types::SqlValue::Unsigned(n) => Some(n.to_string()),
        vibesql_types::SqlValue::Double(n) | vibesql_types::SqlValue::Numeric(n) => {
            Some(n.to_string())
        }
        vibesql_types::SqlValue::Float(n) | vibesql_types::SqlValue::Real(n) => {
            Some(n.to_string())
        }
        vibesql_types::SqlValue::Boolean(b) => Some(if *b { "1" } else { "0" }.to_string()),
        _ => None,
    }
}

/// Find character position (1-indexed) of needle in haystack
/// Returns character position, not byte position, for proper Unicode handling
fn find_char_position(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1; // SQLite: empty needle returns 1
    }

    // Find byte position first
    if let Some(byte_pos) = haystack.find(needle) {
        // Convert byte position to character position
        let char_pos = haystack[..byte_pos].chars().count();
        (char_pos + 1) as i64 // 1-indexed
    } else {
        0 // Not found
    }
}

/// INSTR(string, substring) - Find position of substring (1-indexed, 0 if not found)
/// MySQL/Oracle function - returns first occurrence position
/// SQLite converts numeric arguments to strings automatically
pub(in crate::evaluator::functions) fn instr(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "INSTR requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // Handle NULL arguments
    if matches!(&args[0], vibesql_types::SqlValue::Null)
        || matches!(&args[1], vibesql_types::SqlValue::Null)
    {
        return Ok(vibesql_types::SqlValue::Null);
    }

    // Convert both arguments to strings (SQLite does this for numbers)
    let haystack = sql_value_to_string(&args[0]).ok_or_else(|| {
        ExecutorError::UnsupportedFeature(format!(
            "INSTR cannot convert first argument to string: {:?}",
            args[0]
        ))
    })?;

    let needle = sql_value_to_string(&args[1]).ok_or_else(|| {
        ExecutorError::UnsupportedFeature(format!(
            "INSTR cannot convert second argument to string: {:?}",
            args[1]
        ))
    })?;

    // Find character position (not byte position)
    let position = find_char_position(&haystack, &needle);
    Ok(vibesql_types::SqlValue::Integer(position))
}

/// LOCATE(substring, string, [start]) - Find position of substring with optional start
/// Note: Arguments reversed compared to INSTR (needle, haystack vs haystack, needle)
pub(in crate::evaluator::functions) fn locate(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LOCATE requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
            Ok(vibesql_types::SqlValue::Null)
        }
        (
            vibesql_types::SqlValue::Varchar(needle) | vibesql_types::SqlValue::Character(needle),
            vibesql_types::SqlValue::Varchar(haystack)
            | vibesql_types::SqlValue::Character(haystack),
        ) => {
            // Optional start position (1-indexed, default to 1)
            let start_pos = if args.len() == 3 {
                match &args[2] {
                    vibesql_types::SqlValue::Integer(s) => {
                        // Convert from 1-indexed to 0-indexed, clamp to 0
                        ((*s - 1).max(0) as usize).min(haystack.len())
                    }
                    vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                    val => {
                        return Err(ExecutorError::UnsupportedFeature(format!(
                            "LOCATE start position must be integer, got {:?}",
                            val
                        )))
                    }
                }
            } else {
                0
            };

            // Search from start position
            let position = if start_pos >= haystack.len() {
                0 // Start beyond string length -> not found
            } else {
                haystack[start_pos..].find(&**needle)
                    .map(|pos| (pos + start_pos + 1) as i64) // Convert to 1-indexed
                    .unwrap_or(0)
            };

            Ok(vibesql_types::SqlValue::Integer(position))
        }
        (needle, haystack) => Err(ExecutorError::UnsupportedFeature(format!(
            "LOCATE requires string arguments, got {:?} and {:?}",
            needle, haystack
        ))),
    }
}
