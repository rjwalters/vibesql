//! Logical operator implementations
//!
//! Handles: AND, OR
//! Implements: SQL three-valued logic (TRUE, FALSE, NULL)

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

pub(crate) struct LogicalOps;

impl LogicalOps {
    /// AND operator - SQL three-valued logic
    ///
    /// Truth table:
    /// - TRUE AND TRUE = TRUE
    /// - TRUE AND FALSE = FALSE
    /// - TRUE AND NULL = NULL
    /// - FALSE AND anything = FALSE
    /// - NULL AND TRUE = NULL
    /// - NULL AND FALSE = FALSE
    /// - NULL AND NULL = NULL
    #[inline]
    pub fn and(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Both boolean values
            (Boolean(a), Boolean(b)) => Ok(Boolean(*a && *b)),

            // FALSE AND anything = FALSE (even NULL)
            (Boolean(false), Null) | (Null, Boolean(false)) => Ok(Boolean(false)),

            // TRUE AND NULL = NULL
            (Boolean(true), Null) | (Null, Boolean(true)) => Ok(Null),

            // NULL AND NULL = NULL
            (Null, Null) => Ok(Null),

            // Type mismatch for non-boolean, non-null values
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "AND".to_string(),
                right: right.clone(),
            }),
        }
    }

    /// OR operator - SQL three-valued logic
    ///
    /// Truth table:
    /// - TRUE OR anything = TRUE
    /// - FALSE OR TRUE = TRUE
    /// - FALSE OR FALSE = FALSE
    /// - FALSE OR NULL = NULL
    /// - NULL OR TRUE = TRUE
    /// - NULL OR FALSE = NULL
    /// - NULL OR NULL = NULL
    #[inline]
    pub fn or(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Both boolean values
            (Boolean(a), Boolean(b)) => Ok(Boolean(*a || *b)),

            // TRUE OR anything = TRUE (even NULL)
            (Boolean(true), Null) | (Null, Boolean(true)) => Ok(Boolean(true)),

            // FALSE OR NULL = NULL
            (Boolean(false), Null) | (Null, Boolean(false)) => Ok(Null),

            // NULL OR NULL = NULL
            (Null, Null) => Ok(Null),

            // Type mismatch for non-boolean, non-null values
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "OR".to_string(),
                right: right.clone(),
            }),
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
    fn test_type_error() {
        let result = LogicalOps::and(&SqlValue::Integer(1), &SqlValue::Boolean(true));
        assert!(result.is_err());
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
