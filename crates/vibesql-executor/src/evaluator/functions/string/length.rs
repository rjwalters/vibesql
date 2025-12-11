//! String length measurement functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// CHAR_LENGTH(string [USING unit]) / CHARACTER_LENGTH(string [USING unit])
/// Return string length in characters or octets
/// SQL:1999 Section 6.29: String value functions
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

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
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
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "{} requires string argument, got {:?}",
            name, val
        ))),
    }
}

/// OCTET_LENGTH(string) - Return number of octets (bytes) in string
/// SQL:1999 Section 6.29: String value functions
/// Returns byte length, not character count. For UTF-8:
/// - ASCII characters: 1 byte each
/// - Multi-byte characters: 2-4 bytes each
pub(in crate::evaluator::functions) fn octet_length(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "OCTET_LENGTH requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
        vibesql_types::SqlValue::Varchar(s) => Ok(vibesql_types::SqlValue::Integer(s.len() as i64)),
        vibesql_types::SqlValue::Character(s) => {
            Ok(vibesql_types::SqlValue::Integer(s.len() as i64))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "OCTET_LENGTH requires string argument, got {:?}",
            val
        ))),
    }
}

/// LENGTH(str) - Return string length (SQLite compatible)
/// SQLite's LENGTH() accepts any type, converting to string first.
/// Returns byte count for strings, digit count for integers, etc.
pub(in crate::evaluator::functions) fn length(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "wrong number of arguments to function length()".to_string(),
        ));
    }

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            Ok(vibesql_types::SqlValue::Integer(s.len() as i64))
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
    }
}
