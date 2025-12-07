//! String operator implementations
//!
//! Handles: || (concatenation)
//! Supports: VARCHAR and CHAR types

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

pub(crate) struct StringOps;

impl StringOps {
    /// String concatenation operator (||)
    ///
    /// Concatenates two strings, supporting both VARCHAR and CHAR types.
    /// Result is always VARCHAR.
    #[inline]
    pub fn concat(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            (Varchar(a), Varchar(b)) => Ok(Varchar(arcstr::ArcStr::from(format!("{}{}", a, b)))),
            (Varchar(a), Character(b)) => Ok(Varchar(arcstr::ArcStr::from(format!("{}{}", a, b)))),
            (Character(a), Varchar(b)) => Ok(Varchar(arcstr::ArcStr::from(format!("{}{}", a, b)))),
            (Character(a), Character(b)) => Ok(Varchar(arcstr::ArcStr::from(format!("{}{}", a, b)))),
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "||".to_string(),
                right: right.clone(),
            }),
        }
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

    #[test]
    fn test_type_error() {
        let result =
            StringOps::concat(&SqlValue::Integer(1), &SqlValue::Varchar(arcstr::ArcStr::from("test")));
        assert!(result.is_err());
    }
}
