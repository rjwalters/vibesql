//! String concatenation functions for SQL
//!
//! Dialect-specific CONCAT function behavior:
//!
//! MySQL mode:
//! - NULL arguments cause the result to be NULL (standard SQL behavior)
//! - CONCAT('Hello', NULL) returns NULL
//!
//! SQLite mode (SQLite 3.44.0+):
//! - NULL arguments are skipped (not included in result)
//! - Does NOT return NULL if any argument is NULL
//! - Returns empty string if all arguments are NULL
//! - CONCAT('Hello', NULL) returns 'Hello'

use crate::errors::ExecutorError;

/// CONCAT(str1, str2, ...) - Concatenate strings
///
/// Behavior depends on SQL mode:
/// - MySQL: NULL propagates (any NULL argument returns NULL)
/// - SQLite: NULL arguments are ignored (skipped in result)
pub(in crate::evaluator::functions) fn concat(
    args: &[vibesql_types::SqlValue],
    sql_mode: &vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "concat".to_string() });
    }

    // MySQL mode: NULL propagates - any NULL argument returns NULL
    if matches!(sql_mode, vibesql_types::SqlMode::MySQL { .. }) {
        for arg in args {
            if matches!(arg, vibesql_types::SqlValue::Null) {
                return Ok(vibesql_types::SqlValue::Null);
            }
        }
    }

    let mut result = String::new();
    for arg in args {
        match arg {
            vibesql_types::SqlValue::Null => {
                // SQLite mode: NULL is skipped (not included in result)
                continue;
            }
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                result.push_str(s);
            }
            vibesql_types::SqlValue::Integer(n) => result.push_str(&n.to_string()),
            vibesql_types::SqlValue::Bigint(n) => result.push_str(&n.to_string()),
            vibesql_types::SqlValue::Smallint(n) => result.push_str(&n.to_string()),
            vibesql_types::SqlValue::Numeric(n) => result.push_str(&format_number(*n)),
            vibesql_types::SqlValue::Double(n) => result.push_str(&format_number(*n)),
            vibesql_types::SqlValue::Real(n) => result.push_str(&format_number(*n)),
            vibesql_types::SqlValue::Float(n) => result.push_str(&format_number(*n as f64)),
            vibesql_types::SqlValue::Boolean(b) => {
                result.push_str(if *b { "1" } else { "0" });
            }
            vibesql_types::SqlValue::Blob(bytes) => {
                // SQLite treats blobs as text in concat
                result.push_str(&String::from_utf8_lossy(bytes));
            }
            _ => {
                // Other types: convert to string representation
                result.push_str(&arg.to_string());
            }
        }
    }
    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
}

/// Format a number for string concatenation
fn format_number(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        // Whole number - format as integer
        format!("{}", n as i64)
    } else {
        n.to_string()
    }
}
