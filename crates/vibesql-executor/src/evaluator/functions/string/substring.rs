//! Substring extraction functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;
use std::borrow::Cow;

/// SUBSTRING(string, start [, length]) - Extract substring
/// SQL:1999 Section 6.29: String value functions
/// start is 1-based (SQL standard), length is optional
pub(in crate::evaluator::functions) fn substring(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "SUBSTRING requires 2 or 3 arguments, got {}",
            args.len()
        )));
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

    // Extract string - supports implicit coercion from Date/Timestamp to string
    // This enables TPC-H Q9 pattern: SUBSTR(o_orderdate, 1, 4) to extract year
    let s: Cow<str> = match string_val {
        vibesql_types::SqlValue::Varchar(s) => Cow::Borrowed(&**s),
        vibesql_types::SqlValue::Character(s) => Cow::Borrowed(&**s),
        vibesql_types::SqlValue::Date(d) => Cow::Owned(d.to_string()),
        vibesql_types::SqlValue::Timestamp(ts) => Cow::Owned(ts.to_string()),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "SUBSTRING requires string or date/timestamp argument, got {:?}",
                string_val
            )))
        }
    };

    // Extract start position (1-based in SQL)
    let start = match start_val {
        vibesql_types::SqlValue::Integer(n) => *n,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "SUBSTRING start position must be integer, got {:?}",
                start_val
            )))
        }
    };

    // Extract optional length
    let length = if let Some(len_val) = length_val {
        match len_val {
            vibesql_types::SqlValue::Integer(n) => Some(*n),
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "SUBSTRING length must be integer, got {:?}",
                    len_val
                )))
            }
        }
    } else {
        None
    };

    // Convert 1-based SQL index to 0-based Rust index
    // SQL standard: SUBSTRING('hello', 1, 1) = 'h'
    let start_idx = if start > 0 {
        (start - 1) as usize
    } else {
        // SQL:1999 treats start <= 0 as starting from position 1
        0
    };

    // Calculate substring
    let result = if start_idx >= s.len() {
        // Start beyond string length - return empty string
        String::new()
    } else if let Some(len) = length {
        if len <= 0 {
            // Zero or negative length - return empty string
            String::new()
        } else {
            let len_usize = len as usize;
            let end_idx = std::cmp::min(start_idx + len_usize, s.len());
            s[start_idx..end_idx].to_string()
        }
    } else {
        // No length specified - extract to end of string
        s[start_idx..].to_string()
    };

    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
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
