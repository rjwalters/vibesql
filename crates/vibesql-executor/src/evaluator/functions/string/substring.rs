//! Substring extraction functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::{
    errors::ExecutorError,
    evaluator::functions::coercion::{coerce_to_integer, coerce_to_string},
};

/// SUBSTRING(string, start [, length]) - Extract substring (SQLite compatible)
///
/// SQLite's substr() behavior:
/// - 1-based indexing: position 1 = first character/byte
/// - Negative start: count from end (-1 = last char, -2 = 2nd from end)
/// - Zero start: treated as position before first character
/// - Negative length: extract leftward from start position
/// - Zero length: return empty string
///
/// SQLite compatibility:
/// - Automatically coerces numeric types to strings: SUBSTR(12345, 2, 3) returns "234"
/// - For BLOB inputs, operates on bytes and returns BLOB (preserves type)
pub(in crate::evaluator::functions) fn substring(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "substr".to_string() });
    }

    let string_val = &args[0];
    let start_val = &args[1];
    let length_val = args.get(2);

    // Handle NULL inputs
    if matches!(string_val, vibesql_types::SqlValue::Null)
        || matches!(start_val, vibesql_types::SqlValue::Null)
        || (length_val.is_some() && matches!(length_val, Some(vibesql_types::SqlValue::Null)))
    {
        return Ok(vibesql_types::SqlValue::Null);
    }

    // Extract start position (1-based in SQL) with coercion
    let start = match coerce_to_integer(start_val) {
        Some(n) => n,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    // Extract optional length with coercion
    let length = if let Some(len_val) = length_val { coerce_to_integer(len_val) } else { None };

    // Handle BLOB inputs: operate on bytes and return BLOB
    if let vibesql_types::SqlValue::Blob(bytes) = string_val {
        let byte_count = bytes.len() as i64;
        let result = sqlite_substr_bytes(bytes, byte_count, start, length);
        return Ok(vibesql_types::SqlValue::Blob(result));
    }

    // Extract string with coercion (supports numeric types)
    let s = match coerce_to_string(string_val) {
        Some(s) => s,
        None => return Ok(vibesql_types::SqlValue::Null),
    };

    // SQLite observes TEXT values as NUL-terminated C strings: substr() operates
    // on the observed string, stopping at the first embedded NUL byte (consistent
    // with LENGTH() in length.rs). e.g. CAST(x'4142004344' AS text) is "AB\0CD"
    // but substr sees only "AB", so substr(x,3,2) is empty and substr(x,1,5) is
    // "AB". Numeric coercions never contain NULs, so this is a no-op for them.
    let s = match s.as_bytes().iter().position(|&b| b == 0) {
        Some(nul_idx) => &s[..nul_idx],
        None => s.as_str(),
    };

    // Work with characters (not bytes) for proper UTF-8 handling
    let chars: Vec<char> = s.chars().collect();
    let char_count = chars.len() as i64;

    // Calculate the result using SQLite's algorithm
    let result = sqlite_substr(&chars, char_count, start, length);

    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
}

/// Implements SQLite's substr() algorithm for BLOB (byte-based)
///
/// Same algorithm as sqlite_substr but operates on bytes instead of characters.
fn sqlite_substr_bytes(bytes: &[u8], byte_count: i64, start: i64, length: Option<i64>) -> Vec<u8> {
    // Handle the case where no length is specified
    let len = match length {
        Some(l) => l,
        None => {
            // No length: extract from start to end
            if start > 0 {
                // Positive start: from position to end
                if start > byte_count {
                    return Vec::new();
                }
                let start_idx = (start - 1) as usize;
                return bytes[start_idx..].to_vec();
            } else if start < 0 {
                // Negative start: from position-from-end to end
                let start_from_end = (-start) as usize;
                if start_from_end > bytes.len() {
                    return bytes.to_vec();
                }
                let start_idx = bytes.len() - start_from_end;
                return bytes[start_idx..].to_vec();
            } else {
                // start == 0: position before first byte, return all bytes
                return bytes.to_vec();
            }
        }
    };

    // Zero length always returns empty
    if len == 0 {
        return Vec::new();
    }

    if len > 0 {
        // Positive length: extract rightward
        if start > 0 {
            if start > byte_count {
                return Vec::new();
            }
            let start_idx = (start - 1) as usize;
            let end_idx = std::cmp::min(start_idx + len as usize, bytes.len());
            bytes[start_idx..end_idx].to_vec()
        } else if start < 0 {
            let start_from_end = (-start) as usize;
            if start_from_end > bytes.len() {
                // Start is before beginning of data
                // Adjust the length to account for "virtual" positions before the data
                let bytes_before_data = start_from_end - bytes.len();
                if len as usize <= bytes_before_data {
                    // Length is not enough to reach the data
                    return Vec::new();
                }
                let effective_len = (len as usize) - bytes_before_data;
                return bytes[..std::cmp::min(effective_len, bytes.len())].to_vec();
            }
            let start_idx = bytes.len() - start_from_end;
            let end_idx = std::cmp::min(start_idx + len as usize, bytes.len());
            bytes[start_idx..end_idx].to_vec()
        } else {
            // start == 0, positive length
            let effective_len = (len - 1) as usize;
            if effective_len == 0 {
                return Vec::new();
            }
            bytes[..std::cmp::min(effective_len, bytes.len())].to_vec()
        }
    } else {
        // Negative length: extract leftward
        let abs_len = (-len) as usize;

        if start > 0 {
            // Positive start, negative length
            // Extracts bytes at positions (start - abs_len) to (start - 1) (1-based)
            let requested_end = (start - 1) as usize;
            let requested_start = (start as usize).saturating_sub(abs_len);

            if requested_end == 0 {
                return Vec::new();
            }

            // Clamp to valid positions (1 to byte_count, 1-based)
            let valid_start = std::cmp::max(requested_start, 1);
            let valid_end = std::cmp::min(requested_end, bytes.len());

            if valid_start > valid_end {
                // No valid positions to extract
                return Vec::new();
            }

            // Convert to 0-based indices
            let start_idx = valid_start - 1;
            let end_idx = valid_end;
            bytes[start_idx..end_idx].to_vec()
        } else if start < 0 {
            let start_from_end = (-start) as usize;
            if start_from_end > bytes.len() {
                return Vec::new();
            }
            let end_pos = bytes.len() - start_from_end;
            let start_pos = end_pos.saturating_sub(abs_len);
            bytes[start_pos..end_pos].to_vec()
        } else {
            // start == 0, negative length
            Vec::new()
        }
    }
}

/// Implements SQLite's substr() algorithm
///
/// SQLite's substr(string, start, length) works as follows:
/// - Positions are 1-based (position 1 = first char)
/// - Negative start: count from end (-1 = last char)
/// - Zero start: position before first char
/// - Negative length: extract leftward
/// - Zero length: empty string
fn sqlite_substr(chars: &[char], char_count: i64, start: i64, length: Option<i64>) -> String {
    // Handle the case where no length is specified
    let len = match length {
        Some(l) => l,
        None => {
            // No length: extract from start to end
            if start > 0 {
                // Positive start: from position to end
                if start > char_count {
                    return String::new();
                }
                let start_idx = (start - 1) as usize;
                return chars[start_idx..].iter().collect();
            } else if start < 0 {
                // Negative start: from position-from-end to end
                // -1 = last char, -2 = second-to-last, etc.
                let start_from_end = (-start) as usize;
                if start_from_end > chars.len() {
                    // Start is before beginning, return whole string
                    return chars.iter().collect();
                }
                let start_idx = chars.len() - start_from_end;
                return chars[start_idx..].iter().collect();
            } else {
                // start == 0: position before first char, return whole string
                return chars.iter().collect();
            }
        }
    };

    // Zero length always returns empty string
    if len == 0 {
        return String::new();
    }

    if len > 0 {
        // Positive length: extract rightward
        if start > 0 {
            // Positive start, positive length: normal case
            // substr('hello', 2, 3) -> 'ell'
            if start > char_count {
                return String::new();
            }
            let start_idx = (start - 1) as usize;
            let end_idx = std::cmp::min(start_idx + len as usize, chars.len());
            chars[start_idx..end_idx].iter().collect()
        } else if start < 0 {
            // Negative start, positive length
            // substr('hello', -2, 2) -> 'lo' (start at 2nd from end, take 2)
            let start_from_end = (-start) as usize;
            if start_from_end > chars.len() {
                // Start is before beginning of string
                // SQLite behavior: adjust the length to account for "virtual" positions before the
                // string Example: substr('Supercalifragilisticexpialidocious', -35,
                // 2) 34-char string, start -35 = position 0 (before first char)
                //   We want 2 chars, but we "use up" 1 char of length for the virtual position
                //   So we get 1 char from the start of the string
                let chars_before_string = start_from_end - chars.len();
                if len as usize <= chars_before_string {
                    // Length is not enough to reach the string
                    return String::new();
                }
                let effective_len = (len as usize) - chars_before_string;
                return chars[..std::cmp::min(effective_len, chars.len())].iter().collect();
            }
            let start_idx = chars.len() - start_from_end;
            let end_idx = std::cmp::min(start_idx + len as usize, chars.len());
            chars[start_idx..end_idx].iter().collect()
        } else {
            // start == 0, positive length
            // Position 0 is before position 1, so:
            // substr('hello', 0, 2) -> 'h' (one char less than length due to virtual position)
            let effective_len = (len - 1) as usize;
            if effective_len == 0 {
                return String::new();
            }
            chars[..std::cmp::min(effective_len, chars.len())].iter().collect()
        }
    } else {
        // Negative length: extract leftward from start position
        let abs_len = (-len) as usize;

        if start > 0 {
            // Positive start, negative length
            // substr('hello', 3, -2) -> 'he' (from pos 3, go left 2 chars)
            // Extracts characters at positions (start - abs_len) to (start - 1) (1-based)
            let requested_end = (start - 1) as usize;
            let requested_start = (start as usize).saturating_sub(abs_len);

            if requested_end == 0 {
                return String::new();
            }

            // Clamp to valid positions (1 to char_count, 1-based)
            let valid_start = std::cmp::max(requested_start, 1);
            let valid_end = std::cmp::min(requested_end, chars.len());

            if valid_start > valid_end {
                // No valid positions to extract
                return String::new();
            }

            // Convert to 0-based indices
            let start_idx = valid_start - 1;
            let end_idx = valid_end;
            chars[start_idx..end_idx].iter().collect()
        } else if start < 0 {
            // Negative start, negative length
            // This is an unusual case
            let start_from_end = (-start) as usize;
            if start_from_end > chars.len() {
                return String::new();
            }
            let end_pos = chars.len() - start_from_end;
            let start_pos = end_pos.saturating_sub(abs_len);
            chars[start_pos..end_pos].iter().collect()
        } else {
            // start == 0, negative length
            // Position before first char, extracting leftward = empty
            String::new()
        }
    }
}

/// LEFT(string, n) - Leftmost n characters
pub(in crate::evaluator::functions) fn left(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LEFT requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
            Ok(vibesql_types::SqlValue::Null)
        }
        (
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s),
            vibesql_types::SqlValue::Integer(n),
        ) => {
            if *n < 0 {
                Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")))
            } else {
                let n_usize = *n as usize;
                let result: String = s.chars().take(n_usize).collect();
                Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
            }
        }
        (a, b) => Err(ExecutorError::UnsupportedFeature(format!(
            "LEFT requires string and integer arguments, got {:?} and {:?}",
            a, b
        ))),
    }
}

/// RIGHT(string, n) - Rightmost n characters
pub(in crate::evaluator::functions) fn right(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RIGHT requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
            Ok(vibesql_types::SqlValue::Null)
        }
        (
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s),
            vibesql_types::SqlValue::Integer(n),
        ) => {
            if *n < 0 {
                Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")))
            } else {
                let n_usize = *n as usize;
                let char_count = s.chars().count();
                if n_usize >= char_count {
                    Ok(vibesql_types::SqlValue::Varchar(s.clone()))
                } else {
                    let skip_count = char_count - n_usize;
                    let result: String = s.chars().skip(skip_count).collect();
                    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
                }
            }
        }
        (a, b) => Err(ExecutorError::UnsupportedFeature(format!(
            "RIGHT requires string and integer arguments, got {:?} and {:?}",
            a, b
        ))),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;

    fn text(s: &str) -> SqlValue {
        SqlValue::Varchar(arcstr::ArcStr::from(s))
    }

    fn substr_text(v: SqlValue, start: i64, len: Option<i64>) -> String {
        let mut args = vec![v, SqlValue::Integer(start)];
        if let Some(l) = len {
            args.push(SqlValue::Integer(l));
        }
        match substring(&args).unwrap() {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected Varchar, got {other:?}"),
        }
    }

    #[test]
    fn test_substr_plain_text() {
        assert_eq!(substr_text(text("hello"), 2, Some(3)), "ell");
        assert_eq!(substr_text(text("hello"), 1, None), "hello");
        assert_eq!(substr_text(text("hello"), -2, Some(2)), "lo");
    }

    #[test]
    fn test_substr_observes_embedded_nul() {
        // SQLite observes TEXT as a NUL-terminated C string. CAST(x'4142004344'
        // AS text) is "AB\0CD" but substr() sees only the observed "AB".
        let v = text("AB\0CD");
        // hex(substr(x,1,5)) -> "4142" (only "AB"); we assert the observed chars.
        assert_eq!(substr_text(v.clone(), 1, Some(5)), "AB");
        assert_eq!(substr_text(v.clone(), 1, Some(2)), "AB");
        assert_eq!(substr_text(v.clone(), 1, Some(10)), "AB");
        // position 3 is past the observed 2-char string -> empty.
        assert_eq!(substr_text(v.clone(), 3, Some(2)), "");
        assert_eq!(substr_text(v.clone(), 2, None), "B");
        // negative start counts from the end of the observed string.
        assert_eq!(substr_text(v.clone(), -2, Some(2)), "AB");
    }

    #[test]
    fn test_substr_leading_nul_is_empty() {
        // "\0AB" observed as "" -> any substr is empty.
        assert_eq!(substr_text(text("\0AB"), 1, Some(5)), "");
        assert_eq!(substr_text(text("\0AB"), 1, None), "");
    }

    #[test]
    fn test_substr_numeric_coercion_unaffected() {
        // Numeric coercions never contain NULs; behavior is unchanged.
        assert_eq!(substr_text(SqlValue::Integer(12345), 2, Some(3)), "234");
    }
}
