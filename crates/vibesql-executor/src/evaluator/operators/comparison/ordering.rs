//! Ordering operators (<, <=, >, >=)

use vibesql_types::SqlValue;

use super::compare;
use crate::errors::ExecutorError;

/// Less than operator (<)
#[inline]
pub fn less_than(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
    compare(left, right, |cmp| cmp == std::cmp::Ordering::Less, "<")
}

/// Less than or equal operator (<=)
#[inline]
pub fn less_than_or_equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
    compare(left, right, |cmp| cmp != std::cmp::Ordering::Greater, "<=")
}

/// Greater than operator (>)
#[inline]
pub fn greater_than(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
    compare(left, right, |cmp| cmp == std::cmp::Ordering::Greater, ">")
}

/// Greater than or equal operator (>=)
#[inline]
pub fn greater_than_or_equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
    compare(left, right, |cmp| cmp != std::cmp::Ordering::Less, ">=")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_comparisons() {
        assert_eq!(
            less_than(&SqlValue::Integer(3), &SqlValue::Integer(5)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            greater_than(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            less_than_or_equal(&SqlValue::Integer(5), &SqlValue::Integer(5)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_temporal_comparisons() {
        let result = less_than(
            &SqlValue::Date("2024-01-01".parse().unwrap()),
            &SqlValue::Date("2024-12-31".parse().unwrap()),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_boolean_less_than_integer() {
        // FALSE (0) < 5 = true
        assert_eq!(
            less_than(&SqlValue::Boolean(false), &SqlValue::Integer(5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) < 5 = true
        assert_eq!(
            less_than(&SqlValue::Boolean(true), &SqlValue::Integer(5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) < 1 = false
        assert_eq!(
            less_than(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
        // TRUE (1) < 0 = false
        assert_eq!(
            less_than(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_integer_greater_than_boolean() {
        // 5 > TRUE (1) = true
        assert_eq!(
            greater_than(&SqlValue::Integer(5), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 > FALSE (0) = false
        assert_eq!(
            greater_than(&SqlValue::Integer(0), &SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
        // 1 > TRUE (1) = false
        assert_eq!(
            greater_than(&SqlValue::Integer(1), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_boolean_comparison_with_float() {
        // FALSE (0.0) < 3.5
        assert_eq!(
            less_than(&SqlValue::Boolean(false), &SqlValue::Float(3.5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1.0) > 0.5
        assert_eq!(
            greater_than(&SqlValue::Boolean(true), &SqlValue::Float(0.5)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_comparison_with_numeric() {
        assert_eq!(
            less_than(&SqlValue::Boolean(false), &SqlValue::Numeric(0.5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) >= NUMERIC(0)
        assert_eq!(
            greater_than_or_equal(&SqlValue::Boolean(false), &SqlValue::Numeric(0.0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_less_than_or_equal() {
        // TRUE (1) <= 1
        assert_eq!(
            less_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) <= 0
        assert_eq!(
            less_than_or_equal(&SqlValue::Boolean(false), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) <= 0 = false
        assert_eq!(
            less_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_boolean_greater_than_or_equal() {
        // TRUE (1) >= 1
        assert_eq!(
            greater_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) >= 0
        assert_eq!(
            greater_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) >= 1 = false
        assert_eq!(
            greater_than_or_equal(&SqlValue::Boolean(false), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_real_greater_than_bigint() {
        // Real(846.0) > Bigint(221) = true
        assert_eq!(
            greater_than(&SqlValue::Real(846.0), &SqlValue::Bigint(221)).unwrap(),
            SqlValue::Boolean(true)
        );
        // Real(100.0) > Bigint(221) = false
        assert_eq!(
            greater_than(&SqlValue::Real(100.0), &SqlValue::Bigint(221)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    // SQLite type ordering tests for ordering comparisons
    // Type ordering: NULL < INTEGER/REAL < TEXT < BLOB
    // No coercion between different storage classes

    #[test]
    fn test_text_greater_than_integer() {
        // '10' > 10 = TRUE (TEXT > INTEGER in type ordering, regardless of content)
        assert_eq!(
            greater_than(&SqlValue::Varchar(arcstr::ArcStr::from("10")), &SqlValue::Integer(10))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // 10 < '10' = TRUE (INTEGER < TEXT in type ordering)
        assert_eq!(
            less_than(&SqlValue::Integer(10), &SqlValue::Varchar(arcstr::ArcStr::from("10")))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // '20' > 10 = TRUE (TEXT > INTEGER regardless of content)
        assert_eq!(
            greater_than(&SqlValue::Varchar(arcstr::ArcStr::from("20")), &SqlValue::Integer(10))
                .unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_text_greater_than_real() {
        // '10' > 10.0 = TRUE (TEXT > REAL in type ordering)
        assert_eq!(
            greater_than(&SqlValue::Varchar(arcstr::ArcStr::from("10")), &SqlValue::Double(10.0))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // 10.0 < '10' = TRUE (REAL < TEXT in type ordering)
        assert_eq!(
            less_than(&SqlValue::Double(10.0), &SqlValue::Varchar(arcstr::ArcStr::from("10")))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // '10.5' > 10.0 = TRUE (TEXT > REAL regardless of content)
        assert_eq!(
            greater_than(&SqlValue::Varchar(arcstr::ArcStr::from("10.5")), &SqlValue::Double(10.0))
                .unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_numeric_text_type_ordering() {
        // '10' < 10 = FALSE (TEXT > INTEGER in type ordering)
        assert_eq!(
            less_than(&SqlValue::Varchar(arcstr::ArcStr::from("10")), &SqlValue::Integer(10))
                .unwrap(),
            SqlValue::Boolean(false)
        );
        // 10 > '10' = FALSE (INTEGER < TEXT in type ordering)
        assert_eq!(
            greater_than(&SqlValue::Integer(10), &SqlValue::Varchar(arcstr::ArcStr::from("10")))
                .unwrap(),
            SqlValue::Boolean(false)
        );
        // '5' < 10 = FALSE (TEXT > INTEGER, regardless of numeric value)
        assert_eq!(
            less_than(&SqlValue::Varchar(arcstr::ArcStr::from("5")), &SqlValue::Integer(10))
                .unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_text_ordering_with_all_numeric_types() {
        let text_val = SqlValue::Varchar(arcstr::ArcStr::from("test"));

        // TEXT is greater than all numeric types
        assert_eq!(
            greater_than(&text_val, &SqlValue::Integer(100)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            greater_than(&text_val, &SqlValue::Smallint(100)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            greater_than(&text_val, &SqlValue::Bigint(100)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            greater_than(&text_val, &SqlValue::Float(100.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            greater_than(&text_val, &SqlValue::Real(100.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            greater_than(&text_val, &SqlValue::Double(100.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            greater_than(&text_val, &SqlValue::Numeric(100.0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }
}
