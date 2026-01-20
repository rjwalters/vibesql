//! Blob and encoding functions
//!
//! This module contains SQLite-compatible blob and encoding functions:
//! - HEX(x) - Convert blob/string to hexadecimal
//! - UNHEX(x) - Convert hexadecimal string to blob
//! - ZEROBLOB(n) - Create blob of n zero bytes
//! - RANDOMBLOB(n) - Create blob of n random bytes
//! - QUOTE(x) - Return SQL literal representation

use rand::Rng;
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

/// UNHEX(x) - Convert hexadecimal string to blob
///
/// Returns a blob containing the binary data represented by the hexadecimal string.
/// Returns NULL if the input is not a valid hexadecimal string.
pub(crate) fn unhex(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UNHEX requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    let hex_str = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "UNHEX argument must be a string".to_string(),
            ));
        }
    };

    // SQLite returns NULL for odd-length hex strings
    if hex_str.len() % 2 != 0 {
        return Ok(SqlValue::Null);
    }

    // Parse hex string to bytes
    let mut bytes = Vec::with_capacity(hex_str.len() / 2);
    let chars: Vec<char> = hex_str.chars().collect();

    for chunk in chars.chunks(2) {
        let high = match chunk[0].to_digit(16) {
            Some(d) => d as u8,
            None => return Ok(SqlValue::Null), // Invalid hex character
        };
        let low = match chunk[1].to_digit(16) {
            Some(d) => d as u8,
            None => return Ok(SqlValue::Null), // Invalid hex character
        };
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
        n.to_string()
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
            // Escape single quotes by doubling them
            let escaped = s.replace('\'', "''");
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
}
