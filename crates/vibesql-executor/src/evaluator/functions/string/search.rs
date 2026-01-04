//! String search functions for SQL
//!
//! Functions for finding substrings within strings
//! Includes POSITION (SQL standard), INSTR and LOCATE (MySQL/Oracle)

use crate::errors::ExecutorError;
use crate::evaluator::functions::coercion::{coerce_to_integer, coerce_to_string};

/// POSITION(substring IN string) - Find position (1-indexed)
/// SQL:1999 Section 6.29: String value functions
/// Note: This is called as POSITION('sub', 'string') in our implementation
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
pub(in crate::evaluator::functions) fn position(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "POSITION requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // Coerce both arguments to strings
    let needle = match coerce_to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };
    let haystack = match coerce_to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    // Find character position (not byte position)
    let position = find_char_position(&haystack, &needle);
    Ok(vibesql_types::SqlValue::Integer(position))
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
///
/// SQLite compatibility:
/// - When BOTH arguments are BLOBs, returns byte position (1-indexed)
/// - Otherwise, returns character position for proper Unicode handling
/// - Automatically coerces numeric types to strings
pub(in crate::evaluator::functions) fn instr(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "INSTR requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // SQLite: If BOTH arguments are BLOBs, use byte-level matching
    if let (vibesql_types::SqlValue::Blob(haystack), vibesql_types::SqlValue::Blob(needle)) =
        (&args[0], &args[1])
    {
        let position = find_byte_position(haystack, needle);
        return Ok(vibesql_types::SqlValue::Integer(position));
    }

    // Otherwise, coerce both arguments to strings and use character positions
    let haystack = match coerce_to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };
    let needle = match coerce_to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    // Find character position (not byte position)
    let position = find_char_position(&haystack, &needle);
    Ok(vibesql_types::SqlValue::Integer(position))
}

/// Find byte position (1-indexed) of needle in haystack
/// Returns byte position for BLOB-to-BLOB matching
fn find_byte_position(haystack: &[u8], needle: &[u8]) -> i64 {
    if needle.is_empty() {
        return 1; // SQLite: empty needle returns 1
    }

    // Find the needle in haystack using byte-level comparison
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|pos| (pos + 1) as i64) // Convert to 1-indexed
        .unwrap_or(0) // Not found
}

/// LOCATE(substring, string, [start]) - Find position of substring with optional start
/// Note: Arguments reversed compared to INSTR (needle, haystack vs haystack, needle)
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
pub(in crate::evaluator::functions) fn locate(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LOCATE requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    // Coerce first two arguments to strings
    let needle = match coerce_to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };
    let haystack = match coerce_to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    // Optional start position (1-indexed, default to 1) with coercion
    let start_pos = if args.len() == 3 {
        match coerce_to_integer(&args[2]) {
            Some(s) => {
                // Convert from 1-indexed to 0-indexed, clamp to 0
                ((s - 1).max(0) as usize).min(haystack.len())
            }
            None => return Ok(vibesql_types::SqlValue::Null),
        }
    } else {
        0
    };

    // Search from start position
    let position = if start_pos >= haystack.len() {
        0 // Start beyond string length -> not found
    } else {
        haystack[start_pos..]
            .find(&needle)
            .map(|pos| (pos + start_pos + 1) as i64) // Convert to 1-indexed
            .unwrap_or(0)
    };

    Ok(vibesql_types::SqlValue::Integer(position))
}
