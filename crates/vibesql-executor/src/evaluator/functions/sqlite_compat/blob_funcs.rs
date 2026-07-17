//! Blob and encoding functions
//!
//! This module contains SQLite-compatible blob and encoding functions:
//! - HEX(x) - Convert blob/string to hexadecimal
//! - UNHEX(x) - Convert hexadecimal string to blob
//! - ZEROBLOB(n) - Create blob of n zero bytes
//! - RANDOMBLOB(n) - Create blob of n random bytes
//! - QUOTE(x) - Return SQL literal representation

use rand::RngExt;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// HEX(x) - Convert blob/string to hexadecimal
///
/// Returns an upper-case hexadecimal string representation of the argument.
/// SQLite coerces all values to their TEXT representation first, then converts
/// each byte to its hex representation. For example, hex(255) returns "323535"
/// because 255 is first coerced to the string "255", then each character is
/// converted to hex (0x32, 0x35, 0x35).
///
/// For blob data (from randomblob/unhex), we use Latin-1 encoding where each
/// character maps to exactly one byte value (0x00-0xFF).
pub(crate) fn hex(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "HEX requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let hex_string: String = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Vector(floats) => {
            // Convert vector (blob) to bytes first
            floats.iter().flat_map(|f| f.to_le_bytes()).map(|b| format!("{:02X}", b)).collect()
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Check if this looks like blob data (contains Latin-1 high bytes)
            // Latin-1 encoding: each char value IS the byte value
            let has_latin1_high_bytes = s.chars().any(|c| {
                let code = c as u32;
                (0x80..=0xFF).contains(&code)
            });

            if has_latin1_high_bytes {
                // This is blob data using Latin-1 encoding - convert each char to its byte value
                s.chars()
                    .map(|c| {
                        let byte = (c as u32) as u8;
                        format!("{:02X}", byte)
                    })
                    .collect()
            } else {
                // Regular UTF-8 string - use bytes directly
                s.as_bytes().iter().map(|b| format!("{:02X}", b)).collect()
            }
        }
        SqlValue::Blob(bytes) => {
            // Convert blob bytes directly to hex representation
            bytes.iter().map(|b| format!("{:02X}", b)).collect()
        }
        // SQLite coerces all other types to TEXT first, then converts to hex
        _ => {
            let s = args[0].to_string();
            s.as_bytes().iter().map(|b| format!("{:02X}", b)).collect()
        }
    };

    Ok(SqlValue::Varchar(hex_string.into()))
}

/// Coerce a value to its TEXT representation for UNHEX, matching SQLite's
/// `sqlite3_value_text()`. Returns `None` for NULL (so the caller can return
/// NULL per SQLite's "NULL in, NULL out" contract).
fn unhex_text(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Null => None,
        SqlValue::Varchar(s) | SqlValue::Character(s) => Some(s.to_string()),
        // SQLite coerces a blob to text by reading its bytes directly.
        SqlValue::Blob(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        // All other types (integer, real, boolean, ...) coerce to their text form.
        other => Some(other.to_string()),
    }
}

/// UNHEX(X) / UNHEX(X, Y) - Convert a hexadecimal string to a blob.
///
/// Returns a blob containing the binary data decoded from the hexadecimal
/// digit pairs in `X`. Returns NULL if `X` is not a valid hexadecimal string.
///
/// The two-argument form treats every character that appears in `Y` as an
/// ignorable separator: such characters may appear before, between, or after
/// complete hexadecimal digit pairs and are skipped. An ignore character may
/// NOT split a digit pair (this mirrors SQLite's `unhexFunc`): once the high
/// nibble of a byte has been read, the very next character must be a hex digit.
/// If either argument is NULL the result is NULL.
pub(crate) fn unhex(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UNHEX requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    // First argument: NULL propagates to a NULL result.
    let hex_str = match unhex_text(&args[0]) {
        Some(s) => s,
        None => return Ok(SqlValue::Null),
    };

    // Second argument (optional): the set of ignorable separator characters.
    // A NULL separator argument makes the whole result NULL.
    let ignore: std::collections::HashSet<char> = if args.len() == 2 {
        match unhex_text(&args[1]) {
            Some(s) => s.chars().collect(),
            None => return Ok(SqlValue::Null),
        }
    } else {
        std::collections::HashSet::new()
    };

    let chars: Vec<char> = hex_str.chars().collect();
    let mut bytes = Vec::with_capacity(chars.len() / 2 + 1);
    let mut i = 0;

    while i < chars.len() {
        // Skip ignorable separator characters before the high nibble.
        while i < chars.len() && ignore.contains(&chars[i]) {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }

        // High nibble.
        let high = match chars[i].to_digit(16) {
            Some(d) => d as u8,
            None => return Ok(SqlValue::Null), // Invalid hex character
        };
        i += 1;

        // Low nibble: the immediately following character. Separator characters
        // are NOT skipped here, so a separator cannot split a digit pair.
        if i >= chars.len() {
            return Ok(SqlValue::Null); // Dangling high nibble (odd number of digits)
        }
        let low = match chars[i].to_digit(16) {
            Some(d) => d as u8,
            None => return Ok(SqlValue::Null), // Invalid hex character
        };
        i += 1;

        bytes.push((high << 4) | low);
    }

    Ok(SqlValue::Blob(bytes))
}

/// ZEROBLOB(n) - Create a blob of n zero bytes
///
/// Returns a blob consisting of n zero-valued bytes.
pub(crate) fn zeroblob(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ZEROBLOB requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let n = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(i) => *i as usize,
        SqlValue::Bigint(i) => *i as usize,
        SqlValue::Smallint(i) => *i as usize,
        SqlValue::Unsigned(u) => *u as usize,
        SqlValue::Numeric(n) => *n as usize,
        SqlValue::Real(r) => *r as usize,
        SqlValue::Double(d) => *d as usize,
        SqlValue::Float(f) => *f as usize,
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // SQLite accepts numeric strings
            s.trim().parse::<f64>().map(|v| v as usize).unwrap_or(0)
        }
        SqlValue::Blob(_) => {
            // SQLite treats blobs as 0 in numeric context for zeroblob
            0
        }
        SqlValue::Boolean(b) => {
            // Boolean converts to 0 or 1
            if *b {
                1
            } else {
                0
            }
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ZEROBLOB argument must be numeric".to_string(),
            ));
        }
    };

    // SQLite limits blob size, we'll use a reasonable limit
    const MAX_BLOB_SIZE: usize = 1_000_000_000; // 1GB
    if n > MAX_BLOB_SIZE {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ZEROBLOB size {} exceeds maximum {}",
            n, MAX_BLOB_SIZE
        )));
    }

    Ok(SqlValue::Blob(vec![0u8; n]))
}

/// RANDOMBLOB(N) - Return N bytes of pseudo-random data
///
/// Returns a blob containing N bytes of pseudo-random data.
/// SQLite returns a 1-byte blob for negative sizes, empty blob for zero.
pub(crate) fn randomblob(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RANDOMBLOB requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // SQLite returns 1-byte blob for negative sizes, empty blob for zero
    let n = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(i) => {
            if *i < 0 {
                1usize // SQLite returns 1 byte for negative sizes
            } else {
                *i as usize
            }
        }
        SqlValue::Bigint(i) => {
            if *i < 0 {
                1usize
            } else {
                *i as usize
            }
        }
        SqlValue::Smallint(i) => {
            if *i < 0 {
                1usize
            } else {
                *i as usize
            }
        }
        SqlValue::Unsigned(u) => *u as usize,
        SqlValue::Numeric(n) => {
            if *n < 0.0 {
                1usize
            } else {
                *n as usize
            }
        }
        SqlValue::Real(r) => {
            if *r < 0.0 {
                1usize
            } else {
                *r as usize
            }
        }
        SqlValue::Double(d) => {
            if *d < 0.0 {
                1usize
            } else {
                *d as usize
            }
        }
        SqlValue::Float(f) => {
            if *f < 0.0 {
                1usize
            } else {
                *f as usize
            }
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "RANDOMBLOB argument must be numeric".to_string(),
            ));
        }
    };

    // SQLite limits blob size
    const MAX_BLOB_SIZE: usize = 1_000_000_000; // 1GB
    if n > MAX_BLOB_SIZE {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RANDOMBLOB size {} exceeds maximum {}",
            n, MAX_BLOB_SIZE
        )));
    }

    // Generate N random bytes
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..n).map(|_| rng.random()).collect();

    Ok(SqlValue::Blob(bytes))
}

/// Format a float for QUOTE() - uses 9.0e+999 for infinity (SQLite compatibility)
fn format_float_for_quote(n: f64) -> String {
    if n.is_nan() {
        "NaN".to_string()
    } else if n.is_infinite() {
        // SQLite's quote() represents infinity as 9.0e+999
        if n > 0.0 {
            "9.0e+999".to_string()
        } else {
            "-9.0e+999".to_string()
        }
    } else {
        // Finite values must render through the SqlValue Display impl
        // (format_f64), not the raw f64 `to_string()`. Rust's `f64::to_string`
        // emits shortest-round-trip *fixed-point* text (e.g. a 300-digit
        // expansion for 1e300), whereas SQLite's quote() uses %!.15g
        // ("1.0e+300", "2.0"). Display already matches sqlite3 3.51, so route
        // finite reals through it for parity.
        SqlValue::Double(n).to_string()
    }
}

/// QUOTE(x) - Return SQL literal representation of a value
///
/// Returns a string which is the value of its argument suitable for
/// inclusion in another SQL statement. Strings are surrounded by
/// single-quotes with escapes on interior quotes.
pub(crate) fn quote(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "QUOTE requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Varchar("NULL".into())),
        SqlValue::Integer(i) => Ok(SqlValue::Varchar(i.to_string().into())),
        SqlValue::Bigint(i) => Ok(SqlValue::Varchar(i.to_string().into())),
        SqlValue::Smallint(i) => Ok(SqlValue::Varchar(i.to_string().into())),
        SqlValue::Unsigned(u) => Ok(SqlValue::Varchar(u.to_string().into())),
        SqlValue::Real(r) => Ok(SqlValue::Varchar(format_float_for_quote(*r).into())),
        SqlValue::Double(d) => Ok(SqlValue::Varchar(format_float_for_quote(*d).into())),
        SqlValue::Numeric(n) => Ok(SqlValue::Varchar(format_float_for_quote(*n).into())),
        SqlValue::Float(f) => Ok(SqlValue::Varchar(format_float_for_quote(*f as f64).into())),
        SqlValue::Boolean(b) => Ok(SqlValue::Varchar(if *b { "1" } else { "0" }.into())),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // SQLite observes TEXT values as NUL-terminated C strings: quote()
            // renders the observed string, stopping at the first embedded NUL
            // (consistent with LENGTH() in length.rs). e.g.
            // CAST(x'4142004344' AS text) is "AB\0CD" but quote() emits 'AB',
            // not a literal containing an embedded NUL. This also keeps CLI
            // rendering balanced (PR #6093's format_sql_value truncation no
            // longer drops the closing quote).
            let observed = match s.as_bytes().iter().position(|&b| b == 0) {
                Some(nul_idx) => &s[..nul_idx],
                None => s.as_str(),
            };
            // Escape single quotes by doubling them
            let escaped = observed.replace('\'', "''");
            Ok(SqlValue::Varchar(format!("'{}'", escaped).into()))
        }
        SqlValue::Vector(floats) => {
            // Convert blob to X'...' hex format
            let hex: String =
                floats.iter().flat_map(|f| f.to_le_bytes()).map(|b| format!("{:02X}", b)).collect();
            Ok(SqlValue::Varchar(format!("X'{}'", hex).into()))
        }
        SqlValue::Date(d) => Ok(SqlValue::Varchar(format!("'{}'", d).into())),
        SqlValue::Time(t) => Ok(SqlValue::Varchar(format!("'{}'", t).into())),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Varchar(format!("'{}'", ts).into())),
        SqlValue::Interval(i) => Ok(SqlValue::Varchar(format!("'{}'", i).into())),
        SqlValue::Blob(b) => {
            // Convert blob to X'...' hex format
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            Ok(SqlValue::Varchar(format!("X'{}'", hex).into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex() {
        // Basic string conversion
        assert_eq!(
            hex(&[SqlValue::Varchar("abc".into())]).unwrap(),
            SqlValue::Varchar("616263".into())
        );
        assert_eq!(hex(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        // SQLite coerces integers to text first: hex(255) = hex("255") = "323535"
        // "255" = bytes [0x32, 0x35, 0x35]
        assert_eq!(hex(&[SqlValue::Integer(255)]).unwrap(), SqlValue::Varchar("323535".into()));
        // Uppercase hex string as stated in SQLite docs
        assert_eq!(
            hex(&[SqlValue::Varchar("ABC".into())]).unwrap(),
            SqlValue::Varchar("414243".into())
        );
    }

    #[test]
    fn test_unhex() {
        // unhex returns a Blob
        let result = unhex(&[SqlValue::Varchar("616263".into())]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes, vec![0x61, 0x62, 0x63]),
            _ => panic!("Expected Blob"),
        }

        // Odd length returns NULL
        assert_eq!(unhex(&[SqlValue::Varchar("abc".into())]).unwrap(), SqlValue::Null);
        // Invalid hex returns NULL
        assert_eq!(unhex(&[SqlValue::Varchar("zz".into())]).unwrap(), SqlValue::Null);
        // Empty string returns an empty blob (not NULL)
        assert_eq!(unhex(&[SqlValue::Varchar("".into())]).unwrap(), SqlValue::Blob(vec![]));
        // NULL propagates
        assert_eq!(unhex(&[SqlValue::Null]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_unhex_two_arg() {
        // Separators may appear before, between, and after complete digit pairs.
        assert_eq!(
            unhex(&[SqlValue::Varchar("FFFF  ABCD".into()), SqlValue::Varchar(" -".into())])
                .unwrap(),
            SqlValue::Blob(vec![0xFF, 0xFF, 0xAB, 0xCD])
        );
        assert_eq!(
            unhex(&[SqlValue::Varchar("--FFFF AB- -CD- ".into()), SqlValue::Varchar(" -".into())])
                .unwrap(),
            SqlValue::Blob(vec![0xFF, 0xFF, 0xAB, 0xCD])
        );
        // A string of only separators decodes to an empty blob.
        assert_eq!(
            unhex(&[SqlValue::Varchar("--".into()), SqlValue::Varchar(" -".into())]).unwrap(),
            SqlValue::Blob(vec![])
        );
        // A separator may not split a digit pair: "F F" -> read 'F' then ' ' as
        // the low nibble, which is not a hex digit -> NULL.
        assert_eq!(
            unhex(&[SqlValue::Varchar("F F".into()), SqlValue::Varchar(" ".into())]).unwrap(),
            SqlValue::Null
        );
        // A non-separator, non-hex character -> NULL.
        assert_eq!(
            unhex(&[SqlValue::Varchar("GG".into()), SqlValue::Varchar(" -".into())]).unwrap(),
            SqlValue::Null
        );
        // Either argument NULL -> NULL.
        assert_eq!(
            unhex(&[SqlValue::Null, SqlValue::Varchar(" ".into())]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            unhex(&[SqlValue::Varchar("1234".into()), SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_unhex_arity() {
        // Zero or more-than-two arguments is an error.
        assert!(unhex(&[]).is_err());
        assert!(unhex(&[
            SqlValue::Varchar("AB".into()),
            SqlValue::Varchar("".into()),
            SqlValue::Varchar("".into())
        ])
        .is_err());
    }

    #[test]
    fn test_zeroblob() {
        // zeroblob returns a Blob of zero bytes
        let result = zeroblob(&[SqlValue::Integer(4)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes, vec![0, 0, 0, 0]),
            _ => panic!("Expected Blob"),
        }
        let result = zeroblob(&[SqlValue::Integer(0)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes.len(), 0),
            _ => panic!("Expected Blob"),
        }
        assert_eq!(zeroblob(&[SqlValue::Null]).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_randomblob() {
        // randomblob returns a Blob of N random bytes
        let result = randomblob(&[SqlValue::Integer(10)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => {
                assert_eq!(bytes.len(), 10);
            }
            _ => panic!("Expected Blob"),
        }

        // randomblob(0) returns empty blob
        let result = randomblob(&[SqlValue::Integer(0)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes.len(), 0),
            _ => panic!("Expected Blob"),
        }

        // SQLite returns 1-byte blob for negative sizes
        let result = randomblob(&[SqlValue::Integer(-5)]).unwrap();
        match result {
            SqlValue::Blob(bytes) => assert_eq!(bytes.len(), 1),
            _ => panic!("Expected Blob"),
        }

        // NULL returns NULL
        assert_eq!(randomblob(&[SqlValue::Null]).unwrap(), SqlValue::Null);

        // Wrong number of arguments
        assert!(randomblob(&[]).is_err());
    }

    #[test]
    fn test_quote() {
        // NULL
        assert_eq!(quote(&[SqlValue::Null]).unwrap(), SqlValue::Varchar("NULL".into()));

        // Integer
        assert_eq!(quote(&[SqlValue::Integer(123)]).unwrap(), SqlValue::Varchar("123".into()));

        // String without quotes
        assert_eq!(
            quote(&[SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Varchar("'hello'".into())
        );

        // String with embedded single quote
        assert_eq!(
            quote(&[SqlValue::Varchar("it's".into())]).unwrap(),
            SqlValue::Varchar("'it''s'".into())
        );

        // Float
        assert_eq!(quote(&[SqlValue::Numeric(2.5)]).unwrap(), SqlValue::Varchar("2.5".into()));

        // Boolean
        assert_eq!(quote(&[SqlValue::Boolean(true)]).unwrap(), SqlValue::Varchar("1".into()));
        assert_eq!(quote(&[SqlValue::Boolean(false)]).unwrap(), SqlValue::Varchar("0".into()));

        // Empty string
        assert_eq!(quote(&[SqlValue::Varchar("".into())]).unwrap(), SqlValue::Varchar("''".into()));
    }

    #[test]
    fn test_quote_observes_embedded_nul() {
        // SQLite observes TEXT as a NUL-terminated C string. CAST(x'4142004344'
        // AS text) is "AB\0CD" but quote() emits 'AB' (no embedded NUL), so
        // hex(quote(...)) is 27414227 rather than 27414200434427.
        assert_eq!(
            quote(&[SqlValue::Varchar("AB\0CD".into())]).unwrap(),
            SqlValue::Varchar("'AB'".into())
        );
        // Leading NUL -> observed empty string -> ''
        assert_eq!(
            quote(&[SqlValue::Varchar("\0AB".into())]).unwrap(),
            SqlValue::Varchar("''".into())
        );
        // Interior single quote before a NUL is still escaped.
        assert_eq!(
            quote(&[SqlValue::Varchar("it's\0X".into())]).unwrap(),
            SqlValue::Varchar("'it''s'".into())
        );
    }
}
