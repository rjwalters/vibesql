//! String operator implementations
//!
//! Handles: || (concatenation)
//! Supports: All SQL types with automatic string coercion (SQLite-compatible)

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

pub(crate) struct StringOps;

impl StringOps {
    /// Convert a SqlValue to its string representation for concatenation.
    ///
    /// SQLite compatibility: uses integer format for integers (no decimals),
    /// and preserves float decimal notation as appropriate.
    #[inline]
    fn to_concat_string(value: &SqlValue) -> String {
        use SqlValue::*;
        match value {
            // String types - use as-is
            Varchar(s) | Character(s) => s.to_string(),

            // Integer types - format without decimals
            Integer(i) => i.to_string(),
            Smallint(i) => i.to_string(),
            Bigint(i) => i.to_string(),
            Unsigned(u) => u.to_string(),

            // Boolean - SQLite treats as integer 0/1
            Boolean(b) => if *b { "1" } else { "0" }.to_string(),

            // Float types - use Rust's default formatting which is SQLite-compatible
            Numeric(n) => format_float(*n),
            Float(f) => format_float(*f as f64),
            Real(r) => format_float(*r as f64),
            Double(d) => format_float(*d),

            // Temporal types
            Date(d) => d.to_string(),
            Time(t) => t.to_string(),
            Timestamp(ts) => ts.to_string(),
            Interval(i) => i.to_string(),

            // Vector - format as bracketed list
            Vector(v) => {
                let formatted: Vec<String> = v.iter().map(|x| x.to_string()).collect();
                format!("[{}]", formatted.join(", "))
            }

            // NULL - should not reach here as NULL is handled at the operator registry level
            Null => "NULL".to_string(),
        }
    }

    /// String concatenation operator (||)
    ///
    /// Concatenates two values, automatically converting non-string types
    /// to their string representation (SQLite-compatible behavior).
    /// Result is always VARCHAR.
    ///
    /// Note: NULL handling is done at the OperatorRegistry level before
    /// this function is called (NULL || x = NULL).
    #[inline]
    pub fn concat(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        let left_str = Self::to_concat_string(left);
        let right_str = Self::to_concat_string(right);
        Ok(SqlValue::Varchar(arcstr::ArcStr::from(format!(
            "{}{}",
            left_str, right_str
        ))))
    }
}

/// Format a float value for concatenation (SQLite-compatible).
///
/// Uses minimal representation: integers show without decimals,
/// floats show their natural decimal representation.
#[inline]
fn format_float(n: f64) -> String {
    if n.is_nan() {
        "NaN".to_string()
    } else if n.is_infinite() {
        if n > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else if n.fract() == 0.0 && n.abs() < 1e15 {
        // Whole number - format without decimals but add .0 for floats
        // SQLite shows "10.0" for float 10.0, not "10"
        format!("{}.0", n as i64)
    } else {
        // Fractional or very large - use default formatting
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varchar_concat() {
        let result = StringOps::concat(
            &SqlValue::Varchar(arcstr::ArcStr::from("Hello")),
            &SqlValue::Varchar(arcstr::ArcStr::from(" World")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Hello World")));
    }

    #[test]
    fn test_char_concat() {
        let result = StringOps::concat(
            &SqlValue::Character(arcstr::ArcStr::from("Hello")),
            &SqlValue::Character(arcstr::ArcStr::from(" World")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Hello World")));
    }

    #[test]
    fn test_mixed_string_concat() {
        let result = StringOps::concat(
            &SqlValue::Varchar(arcstr::ArcStr::from("Hello")),
            &SqlValue::Character(arcstr::ArcStr::from(" World")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Hello World")));

        let result = StringOps::concat(
            &SqlValue::Character(arcstr::ArcStr::from("Hello")),
            &SqlValue::Varchar(arcstr::ArcStr::from(" World")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Hello World")));
    }

    // SQLite-compatible type coercion tests

    #[test]
    fn test_integer_concat() {
        // Integer || Integer
        let result = StringOps::concat(&SqlValue::Integer(1), &SqlValue::Integer(2)).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("12")));

        // String || Integer
        let result = StringOps::concat(
            &SqlValue::Varchar(arcstr::ArcStr::from("a")),
            &SqlValue::Integer(1),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("a1")));

        // Integer || String
        let result = StringOps::concat(
            &SqlValue::Integer(1),
            &SqlValue::Varchar(arcstr::ArcStr::from("b")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("1b")));
    }

    #[test]
    fn test_float_concat() {
        // Float || String
        let result = StringOps::concat(
            &SqlValue::Double(3.15),
            &SqlValue::Varchar(arcstr::ArcStr::from("x")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("3.15x")));

        // Whole number float preserves .0
        let result = StringOps::concat(&SqlValue::Double(10.0), &SqlValue::Double(20.0)).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("10.020.0")));
    }

    #[test]
    fn test_boolean_concat() {
        // Boolean true = "1"
        let result = StringOps::concat(
            &SqlValue::Boolean(true),
            &SqlValue::Varchar(arcstr::ArcStr::from("x")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("1x")));

        // Boolean false = "0"
        let result = StringOps::concat(
            &SqlValue::Boolean(false),
            &SqlValue::Varchar(arcstr::ArcStr::from("y")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("0y")));
    }

    #[test]
    fn test_other_integer_types() {
        // Smallint
        let result = StringOps::concat(&SqlValue::Smallint(10), &SqlValue::Smallint(20)).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("1020")));

        // Bigint
        let result = StringOps::concat(&SqlValue::Bigint(100), &SqlValue::Bigint(200)).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("100200")));

        // Unsigned
        let result = StringOps::concat(&SqlValue::Unsigned(5), &SqlValue::Unsigned(6)).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("56")));
    }
}
