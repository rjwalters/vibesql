//! String length measurement functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;
use crate::evaluator::functions::coercion::coerce_to_string;

/// CHAR_LENGTH(string [USING unit]) / CHARACTER_LENGTH(string [USING unit])
/// Return string length in characters or octets
/// SQL:1999 Section 6.29: String value functions
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
pub(in crate::evaluator::functions) fn char_length(
    args: &[vibesql_types::SqlValue],
    name: &str,
    character_unit: &Option<vibesql_ast::CharacterUnit>,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "{} requires exactly 1 argument, got {}",
            name,
            args.len()
        )));
    }

    match coerce_to_string(&args[0]) {
        None => Ok(vibesql_types::SqlValue::Null),
        Some(s) => {
            // Determine unit: CHARACTERS (default) or OCTETS
            let length = match character_unit {
                Some(vibesql_ast::CharacterUnit::Octets) => {
                    // USING OCTETS - return byte count
                    s.len() as i64
                }
                Some(vibesql_ast::CharacterUnit::Characters) | None => {
                    // USING CHARACTERS or default - return character count
                    s.chars().count() as i64
                }
            };
            Ok(vibesql_types::SqlValue::Integer(length))
        }
    }
}

/// OCTET_LENGTH(string) - Return number of octets (bytes) in string
/// SQL:1999 Section 6.29: String value functions
/// Returns byte length, not character count. For UTF-8:
/// - ASCII characters: 1 byte each
/// - Multi-byte characters: 2-4 bytes each
///
/// SQLite compatibility: Automatically coerces numeric types to strings.
/// - OCTET_LENGTH(7.5) returns 3 (length of "7.5")
pub(in crate::evaluator::functions) fn octet_length(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "OCTET_LENGTH requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match coerce_to_string(&args[0]) {
        None => Ok(vibesql_types::SqlValue::Null),
        Some(s) => Ok(vibesql_types::SqlValue::Integer(s.len() as i64)),
    }
}

/// LENGTH(str) - Return string length (SQLite compatible)
/// SQLite's LENGTH() accepts any type, converting to string first.
/// Returns character count for strings (not byte count), digit count for integers, etc.
/// Use OCTET_LENGTH() for byte count.
pub(in crate::evaluator::functions) fn length(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "length".to_string(),
        });
    }

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            // Return character count, not byte count (SQLite behavior)
            Ok(vibesql_types::SqlValue::Integer(s.chars().count() as i64))
        }
        // SQLite converts non-string types to string first
        vibesql_types::SqlValue::Integer(n) => {
            Ok(vibesql_types::SqlValue::Integer(n.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Smallint(n) => {
            Ok(vibesql_types::SqlValue::Integer(n.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Bigint(n) => {
            Ok(vibesql_types::SqlValue::Integer(n.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Unsigned(n) => {
            Ok(vibesql_types::SqlValue::Integer(n.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Float(f) => {
            Ok(vibesql_types::SqlValue::Integer(f.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Real(f) => {
            Ok(vibesql_types::SqlValue::Integer(f.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Double(f) => {
            Ok(vibesql_types::SqlValue::Integer(f.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Numeric(f) => {
            Ok(vibesql_types::SqlValue::Integer(f.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Boolean(b) => {
            // SQLite represents booleans as 0/1
            let s = if *b { "1" } else { "0" };
            Ok(vibesql_types::SqlValue::Integer(s.len() as i64))
        }
        vibesql_types::SqlValue::Date(d) => {
            Ok(vibesql_types::SqlValue::Integer(d.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Timestamp(ts) => {
            Ok(vibesql_types::SqlValue::Integer(ts.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Time(t) => {
            Ok(vibesql_types::SqlValue::Integer(t.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Interval(i) => {
            Ok(vibesql_types::SqlValue::Integer(i.to_string().len() as i64))
        }
        vibesql_types::SqlValue::Vector(v) => {
            // For vectors, return the number of dimensions
            Ok(vibesql_types::SqlValue::Integer(v.len() as i64))
        }
        vibesql_types::SqlValue::Blob(b) => {
            // For blobs, return the number of bytes
            Ok(vibesql_types::SqlValue::Integer(b.len() as i64))
        }
    }
}
