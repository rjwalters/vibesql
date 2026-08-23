//! String length measurement functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::{errors::ExecutorError, evaluator::functions::coercion::coerce_to_string};

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
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "length".to_string() });
    }

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            // Return character count, not byte count (SQLite behavior).
            //
            // SQLite treats TEXT values as NUL-terminated C strings: LENGTH()
            // counts characters only up to (and not including) the first
            // embedded NUL byte. This is the historical MEM_Zero / OP_ToText
            // behavior, e.g. LENGTH(CAST(zeroblob(1000) AS text)) == 0 and
            // LENGTH(CAST(x'4142004344' AS text)) == 2 ("AB\0CD" -> "AB").
            let text = match s.as_bytes().iter().position(|&b| b == 0) {
                Some(nul_idx) => &s[..nul_idx],
                None => s.as_str(),
            };
            Ok(vibesql_types::SqlValue::Integer(text.chars().count() as i64))
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

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;

    fn len_of(v: SqlValue) -> i64 {
        match length(&[v]).unwrap() {
            SqlValue::Integer(n) => n,
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    fn text(s: &str) -> SqlValue {
        SqlValue::Varchar(arcstr::ArcStr::from(s))
    }

    #[test]
    fn test_length_plain_text() {
        assert_eq!(len_of(text("hello")), 5);
        assert_eq!(len_of(text("")), 0);
        // Character count, not byte count.
        assert_eq!(len_of(text("héllo")), 5);
    }

    #[test]
    fn test_length_stops_at_embedded_nul() {
        // SQLite TEXT length counts up to the first NUL byte.
        // CAST(x'4142004344' AS text) -> "AB\0CD" -> length 2.
        assert_eq!(len_of(text("AB\0CD")), 2);
        // "a\0b" -> length 1.
        assert_eq!(len_of(text("a\0b")), 1);
        // Leading NUL -> length 0.
        assert_eq!(len_of(text("\0AB")), 0);
        // "abc\0" -> length 3.
        assert_eq!(len_of(text("abc\0")), 3);
    }

    #[test]
    fn test_length_zeroblob_as_text_is_zero() {
        // The historical fuzz-1.8 / MEM_Zero case: CAST(zeroblob(N) AS text)
        // yields a string of N NUL bytes whose LENGTH() is 0.
        let zeros: String = std::iter::repeat('\0').take(1000).collect();
        assert_eq!(len_of(text(&zeros)), 0);
        // Multibyte char before a NUL is still counted once.
        assert_eq!(len_of(text("é\0x")), 1);
    }

    #[test]
    fn test_length_blob_counts_all_bytes() {
        // BLOB length is the full byte count, including NUL bytes.
        assert_eq!(len_of(SqlValue::Blob(vec![0, 0, 0, 0, 0])), 5);
        assert_eq!(len_of(SqlValue::Blob(vec![0x41, 0x42, 0x00, 0x43, 0x44])), 5);
        assert_eq!(len_of(SqlValue::Blob(vec![])), 0);
    }

    #[test]
    fn test_length_null_is_null() {
        assert!(matches!(length(&[SqlValue::Null]).unwrap(), SqlValue::Null));
    }

    #[test]
    fn test_length_numeric_coercion() {
        assert_eq!(len_of(SqlValue::Integer(12345)), 5);
        assert_eq!(len_of(SqlValue::Integer(-42)), 3);
    }
}
