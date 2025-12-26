//! Equality and inequality operators (= and <>)

use vibesql_types::SqlValue;

use super::compare;
use crate::errors::ExecutorError;

/// Equality operator (=)
#[inline]
pub fn equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
    compare(left, right, |cmp| cmp == std::cmp::Ordering::Equal, "=")
}

/// Inequality operator (<>)
#[inline]
pub fn not_equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
    compare(left, right, |cmp| cmp != std::cmp::Ordering::Equal, "<>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_equality() {
        let result = equal(&SqlValue::Integer(5), &SqlValue::Integer(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));

        let result = equal(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Boolean(false));
    }

    #[test]
    fn test_string_equality() {
        let result = equal(
            &SqlValue::Varchar(arcstr::ArcStr::from("hello")),
            &SqlValue::Varchar(arcstr::ArcStr::from("hello")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_cross_type_string() {
        let result = equal(
            &SqlValue::Character(arcstr::ArcStr::from("hello")),
            &SqlValue::Varchar(arcstr::ArcStr::from("hello")),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_boolean_equality() {
        let result = equal(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_mixed_exact_numeric() {
        let result = equal(&SqlValue::Smallint(5), &SqlValue::Bigint(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_mixed_float_integer() {
        let result = equal(&SqlValue::Float(5.0), &SqlValue::Integer(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_integer_vs_numeric() {
        let result =
            equal(&SqlValue::Integer(200), &SqlValue::Numeric(174.36666666666667)).unwrap();
        assert_eq!(result, SqlValue::Boolean(false));
    }

    #[test]
    fn test_boolean_equals_integer() {
        // TRUE (1) = 1 → true
        assert_eq!(
            equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) = 0 → true
        assert_eq!(
            equal(&SqlValue::Boolean(false), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) = 0 → false
        assert_eq!(
            equal(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_boolean_comparison_with_float() {
        // TRUE (1.0) = 1.0 (as float)
        assert_eq!(
            equal(&SqlValue::Boolean(true), &SqlValue::Float(1.0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_comparison_with_numeric() {
        // TRUE (1) compared to NUMERIC
        assert_eq!(
            equal(&SqlValue::Boolean(true), &SqlValue::Numeric(1.0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_equality_symmetric() {
        // Test symmetry: a = b should equal b = a
        assert_eq!(
            equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            equal(&SqlValue::Integer(1), &SqlValue::Boolean(true)).unwrap()
        );
        assert_eq!(
            equal(&SqlValue::Boolean(false), &SqlValue::Integer(0)).unwrap(),
            equal(&SqlValue::Integer(0), &SqlValue::Boolean(false)).unwrap()
        );
    }

    #[test]
    fn test_boolean_comparison_all_numeric_types() {
        // Test with Smallint, Bigint, Float, Real, Double, Numeric
        let true_val = SqlValue::Boolean(true);

        assert_eq!(equal(&true_val, &SqlValue::Smallint(1)).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&true_val, &SqlValue::Bigint(1)).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&true_val, &SqlValue::Real(1.0)).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&true_val, &SqlValue::Double(1.0)).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&true_val, &SqlValue::Numeric(1.0)).unwrap(), SqlValue::Boolean(true));
    }

    #[test]
    fn test_boolean_not_equal() {
        // TRUE <> 0
        assert_eq!(
            not_equal(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE <> 1
        assert_eq!(
            not_equal(&SqlValue::Boolean(false), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE <> 1 = false
        assert_eq!(
            not_equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    // SQLite type affinity tests: TEXT and NUMERIC are never equal
    // Per SQLite docs: "An INTEGER or REAL value is less than any TEXT or BLOB value."

    #[test]
    fn test_text_integer_not_equal() {
        // '10' = 10 should be FALSE (different types)
        assert_eq!(
            equal(
                &SqlValue::Varchar(arcstr::ArcStr::from("10")),
                &SqlValue::Integer(10)
            )
            .unwrap(),
            SqlValue::Boolean(false)
        );
        // Symmetric: 10 = '10' should also be FALSE
        assert_eq!(
            equal(
                &SqlValue::Integer(10),
                &SqlValue::Varchar(arcstr::ArcStr::from("10"))
            )
            .unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_text_real_not_equal() {
        // '10' = 10.0 should be FALSE (different types)
        assert_eq!(
            equal(
                &SqlValue::Varchar(arcstr::ArcStr::from("10")),
                &SqlValue::Double(10.0)
            )
            .unwrap(),
            SqlValue::Boolean(false)
        );
        // Symmetric: 10.0 = '10' should also be FALSE
        assert_eq!(
            equal(
                &SqlValue::Double(10.0),
                &SqlValue::Varchar(arcstr::ArcStr::from("10"))
            )
            .unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_text_numeric_not_equal_all_types() {
        let text_10 = SqlValue::Varchar(arcstr::ArcStr::from("10"));

        // TEXT is never equal to any numeric type
        assert_eq!(
            equal(&text_10, &SqlValue::Integer(10)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&text_10, &SqlValue::Smallint(10)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&text_10, &SqlValue::Bigint(10)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&text_10, &SqlValue::Float(10.0)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&text_10, &SqlValue::Real(10.0)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&text_10, &SqlValue::Double(10.0)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&text_10, &SqlValue::Numeric(10.0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_text_numeric_not_equal_character_type() {
        // Character type should behave the same as Varchar
        assert_eq!(
            equal(
                &SqlValue::Character(arcstr::ArcStr::from("10")),
                &SqlValue::Integer(10)
            )
            .unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(
                &SqlValue::Integer(10),
                &SqlValue::Character(arcstr::ArcStr::from("10"))
            )
            .unwrap(),
            SqlValue::Boolean(false)
        );
    }
}
