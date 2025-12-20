//! Logical operator implementations
//!
//! Handles: AND, OR
//! Implements: SQL three-valued logic (TRUE, FALSE, NULL)
//! Supports: SQLite truthiness (numeric values in boolean context)

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Convert a SqlValue to a boolean using SQLite truthiness rules.
///
/// SQLite rules:
/// - `0` and `0.0` → false
/// - Any other numeric value → true
/// - Boolean values → as-is
/// - NULL → None (three-valued logic)
/// - Other types → error
fn to_boolean(value: &SqlValue) -> Result<Option<bool>, ExecutorError> {
    use SqlValue::*;
    match value {
        Boolean(b) => Ok(Some(*b)),
        Null => Ok(None),
        // Integer types: 0 is false, non-zero is true
        Integer(n) => Ok(Some(*n != 0)),
        Smallint(n) => Ok(Some(*n != 0)),
        Bigint(n) => Ok(Some(*n != 0)),
        Unsigned(n) => Ok(Some(*n != 0)),
        // Floating point types: 0.0 is false, non-zero is true
        Float(f) => Ok(Some(*f != 0.0)),
        Real(f) => Ok(Some(*f != 0.0)),
        Double(f) => Ok(Some(*f != 0.0)),
        Numeric(f) => Ok(Some(*f != 0.0)),
        // Other types are not supported in boolean context
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "boolean context".to_string(),
            right: SqlValue::Null,
        }),
    }
}

pub(crate) struct LogicalOps;

impl LogicalOps {
    /// AND operator - SQL three-valued logic with SQLite truthiness
    ///
    /// Truth table:
    /// - TRUE AND TRUE = TRUE
    /// - TRUE AND FALSE = FALSE
    /// - TRUE AND NULL = NULL
    /// - FALSE AND anything = FALSE
    /// - NULL AND TRUE = NULL
    /// - NULL AND FALSE = FALSE
    /// - NULL AND NULL = NULL
    ///
    /// SQLite truthiness:
    /// - Numeric 0/0.0 is treated as FALSE
    /// - Non-zero numeric values are treated as TRUE
    #[inline]
    pub fn and(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Convert both operands to boolean (None = NULL)
        let left_bool = to_boolean(left)?;
        let right_bool = to_boolean(right)?;

        match (left_bool, right_bool) {
            // Both have values
            (Some(a), Some(b)) => Ok(Boolean(a && b)),

            // FALSE AND anything = FALSE (even NULL)
            (Some(false), None) | (None, Some(false)) => Ok(Boolean(false)),

            // TRUE AND NULL = NULL
            (Some(true), None) | (None, Some(true)) => Ok(Null),

            // NULL AND NULL = NULL
            (None, None) => Ok(Null),
        }
    }

    /// OR operator - SQL three-valued logic with SQLite truthiness
    ///
    /// Truth table:
    /// - TRUE OR anything = TRUE
    /// - FALSE OR TRUE = TRUE
    /// - FALSE OR FALSE = FALSE
    /// - FALSE OR NULL = NULL
    /// - NULL OR TRUE = TRUE
    /// - NULL OR FALSE = NULL
    /// - NULL OR NULL = NULL
    ///
    /// SQLite truthiness:
    /// - Numeric 0/0.0 is treated as FALSE
    /// - Non-zero numeric values are treated as TRUE
    #[inline]
    pub fn or(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Convert both operands to boolean (None = NULL)
        let left_bool = to_boolean(left)?;
        let right_bool = to_boolean(right)?;

        match (left_bool, right_bool) {
            // Both have values
            (Some(a), Some(b)) => Ok(Boolean(a || b)),

            // TRUE OR anything = TRUE (even NULL)
            (Some(true), None) | (None, Some(true)) => Ok(Boolean(true)),

            // FALSE OR NULL = NULL
            (Some(false), None) | (None, Some(false)) => Ok(Null),

            // NULL OR NULL = NULL
            (None, None) => Ok(Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_and_operator() {
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(false), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_operator() {
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(false), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_type_error_unsupported_types() {
        // Strings are not supported in boolean context
        let result = LogicalOps::and(
            &SqlValue::Varchar(arcstr::ArcStr::from("hello")),
            &SqlValue::Boolean(true),
        );
        assert!(result.is_err());

        let result = LogicalOps::or(
            &SqlValue::Boolean(true),
            &SqlValue::Varchar(arcstr::ArcStr::from("world")),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_and_with_integers() {
        // SQLite truthiness: 0 = false, non-zero = true
        // 1 AND 1 = true
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 1 AND 0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 0 AND 1 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 0 AND 0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Non-zero values: 42 AND -7 = true
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(42), &SqlValue::Integer(-7)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_or_with_integers() {
        // SQLite truthiness: 0 = false, non-zero = true
        // 1 OR 0 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(1), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 OR 1 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(0), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 OR 0 = false
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(0), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 1 OR 1 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(1), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_and_with_floats() {
        // 1.5 AND 0.0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Float(1.5), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 1.5 AND 2.5 = true
        assert_eq!(
            LogicalOps::and(&SqlValue::Float(1.5), &SqlValue::Float(2.5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0.0 AND 0.0 = false
        assert_eq!(
            LogicalOps::and(&SqlValue::Float(0.0), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_with_floats() {
        // 1.5 OR 0.0 = true
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(1.5), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0.0 OR 0.0 = false
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(0.0), &SqlValue::Float(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_and_with_numeric() {
        // Numeric type (f64)
        assert_eq!(
            LogicalOps::and(&SqlValue::Numeric(4.3), &SqlValue::Numeric(2.4)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Numeric(0.0), &SqlValue::Numeric(2.4)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_with_numeric() {
        // Numeric type (f64)
        assert_eq!(
            LogicalOps::or(&SqlValue::Numeric(4.3), &SqlValue::Numeric(0.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Numeric(0.0), &SqlValue::Numeric(0.0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_mixed_numeric_and_boolean() {
        // Integer AND Boolean
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Float OR Boolean
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(0.0), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Float(1.5), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_numeric_with_null() {
        // Integer with NULL
        // 0 AND NULL = FALSE (0 is false, false AND anything = false)
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(0), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(false)
        );
        // 1 AND NULL = NULL (1 is true, true AND null = null)
        assert_eq!(
            LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
        // 1 OR NULL = TRUE (1 is true, true OR anything = true)
        assert_eq!(
            LogicalOps::or(&SqlValue::Integer(1), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 OR NULL = NULL (0 is false, false OR null = null)
        assert_eq!(LogicalOps::or(&SqlValue::Integer(0), &SqlValue::Null).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_and_with_null() {
        // FALSE AND NULL = FALSE
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(false), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Null, &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );

        // TRUE AND NULL = NULL
        assert_eq!(
            LogicalOps::and(&SqlValue::Boolean(true), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            LogicalOps::and(&SqlValue::Null, &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Null
        );

        // NULL AND NULL = NULL
        assert_eq!(LogicalOps::and(&SqlValue::Null, &SqlValue::Null).unwrap(), SqlValue::Null);
    }

    #[test]
    fn test_or_with_null() {
        // TRUE OR NULL = TRUE
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(true), &SqlValue::Null).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Null, &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );

        // FALSE OR NULL = NULL
        assert_eq!(
            LogicalOps::or(&SqlValue::Boolean(false), &SqlValue::Null).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            LogicalOps::or(&SqlValue::Null, &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Null
        );

        // NULL OR NULL = NULL
        assert_eq!(LogicalOps::or(&SqlValue::Null, &SqlValue::Null).unwrap(), SqlValue::Null);
    }
}
