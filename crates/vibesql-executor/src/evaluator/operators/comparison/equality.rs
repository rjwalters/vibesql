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

    // SQLite type ordering tests
    // SQLite does NOT coerce between TEXT and INTEGER/REAL at the comparison level.
    // Type coercion only happens based on column affinity during expression evaluation.
    // At the raw comparison level, different storage classes are NOT equal.
    // Type ordering: NULL < INTEGER/REAL < TEXT < BLOB

    #[test]
    fn test_text_integer_type_ordering() {
        // '10' = 10 should be FALSE (different storage classes, no coercion)
        // This matches SQLite's whereB.test behavior
        assert_eq!(
            equal(&SqlValue::Varchar(arcstr::ArcStr::from("10")), &SqlValue::Integer(10)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Symmetric: 10 = '10' should also be FALSE
        assert_eq!(
            equal(&SqlValue::Integer(10), &SqlValue::Varchar(arcstr::ArcStr::from("10"))).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_text_real_type_ordering() {
        // '10' = 10.0 should be FALSE (different storage classes)
        assert_eq!(
            equal(&SqlValue::Varchar(arcstr::ArcStr::from("10")), &SqlValue::Double(10.0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // Symmetric: 10.0 = '10' should also be FALSE
        assert_eq!(
            equal(&SqlValue::Double(10.0), &SqlValue::Varchar(arcstr::ArcStr::from("10"))).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_text_numeric_type_ordering_all_types() {
        let text_10 = SqlValue::Varchar(arcstr::ArcStr::from("10"));

        // TEXT vs any numeric type should be NOT equal (type ordering)
        assert_eq!(equal(&text_10, &SqlValue::Integer(10)).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&text_10, &SqlValue::Smallint(10)).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&text_10, &SqlValue::Bigint(10)).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&text_10, &SqlValue::Float(10.0)).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&text_10, &SqlValue::Real(10.0)).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&text_10, &SqlValue::Double(10.0)).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&text_10, &SqlValue::Numeric(10.0)).unwrap(), SqlValue::Boolean(false));
    }

    #[test]
    fn test_text_numeric_type_ordering_character_type() {
        // Character type should behave the same as Varchar (TEXT storage class)
        assert_eq!(
            equal(&SqlValue::Character(arcstr::ArcStr::from("10")), &SqlValue::Integer(10))
                .unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&SqlValue::Integer(10), &SqlValue::Character(arcstr::ArcStr::from("10")))
                .unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_non_numeric_text_not_equal() {
        // Non-numeric strings should also not equal numbers (type ordering)
        assert_eq!(
            equal(&SqlValue::Varchar(arcstr::ArcStr::from("hello")), &SqlValue::Integer(10))
                .unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            equal(&SqlValue::Integer(10), &SqlValue::Varchar(arcstr::ArcStr::from("hello")))
                .unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_blob_vs_text_type_ordering() {
        // SQLite type ordering: TEXT < BLOB
        // 'abc' < x'6162' (TEXT is less than any BLOB)
        // Equality across storage classes is always false.
        // Verified: SELECT 'abc' = x'616263' → 0
        let text = SqlValue::Varchar(arcstr::ArcStr::from("abc"));
        let blob = SqlValue::Blob(vec![0x61, 0x62, 0x63]);
        assert_eq!(equal(&text, &blob).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&blob, &text).unwrap(), SqlValue::Boolean(false));
        assert_eq!(not_equal(&text, &blob).unwrap(), SqlValue::Boolean(true));
        assert_eq!(not_equal(&blob, &text).unwrap(), SqlValue::Boolean(true));
    }

    #[test]
    fn test_blob_vs_numeric_type_ordering() {
        // SQLite type ordering: INTEGER/REAL < BLOB
        // Numeric storage class is never equal to BLOB storage class.
        let blob = SqlValue::Blob(vec![0x01]);
        assert_eq!(equal(&SqlValue::Integer(1), &blob).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&blob, &SqlValue::Integer(1)).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&SqlValue::Real(1.0), &blob).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&blob, &SqlValue::Real(1.0)).unwrap(), SqlValue::Boolean(false));
    }

    #[test]
    fn test_blob_vs_blob_bytewise() {
        // BLOB vs BLOB: SQLite uses memcmp/bytewise comparison
        // Verified: SELECT x'616263' = x'616263' → 1
        let a = SqlValue::Blob(vec![0x61, 0x62, 0x63]);
        let b = SqlValue::Blob(vec![0x61, 0x62, 0x63]);
        assert_eq!(equal(&a, &b).unwrap(), SqlValue::Boolean(true));

        let c = SqlValue::Blob(vec![0x61, 0x62]);
        assert_eq!(equal(&a, &c).unwrap(), SqlValue::Boolean(false));
        assert_eq!(not_equal(&a, &c).unwrap(), SqlValue::Boolean(true));
    }

    fn ts(s: &str) -> SqlValue {
        use std::str::FromStr;
        SqlValue::Timestamp(vibesql_types::Timestamp::from_str(s).unwrap())
    }

    #[test]
    fn test_timestamp_vs_string_equality() {
        // SQLite date3.test 2.40: datetime(x,'auto') == '<text>' must evaluate
        // to a boolean, not raise a type mismatch (SQLite's datetime() returns
        // TEXT, so the comparison is valid there)
        let timestamp = ts("2022-01-27 13:15:44");
        let matching = SqlValue::Varchar(arcstr::ArcStr::from("2022-01-27 13:15:44"));
        let differing = SqlValue::Varchar(arcstr::ArcStr::from("2022-01-27 13:15:45"));

        assert_eq!(equal(&timestamp, &matching).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&matching, &timestamp).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&timestamp, &differing).unwrap(), SqlValue::Boolean(false));
        assert_eq!(not_equal(&timestamp, &differing).unwrap(), SqlValue::Boolean(true));

        // Character storage class behaves like Varchar
        let matching_char = SqlValue::Character(arcstr::ArcStr::from("2022-01-27 13:15:44"));
        assert_eq!(equal(&timestamp, &matching_char).unwrap(), SqlValue::Boolean(true));
    }

    #[test]
    fn test_timestamp_vs_unparseable_string_is_false_not_error() {
        // SQLite: SELECT datetime('2022-01-27') == 'hello' → 0 (text
        // comparison of the renderings), never an error
        let timestamp = ts("2022-01-27 00:00:00");
        let junk = SqlValue::Varchar(arcstr::ArcStr::from("hello"));
        assert_eq!(equal(&timestamp, &junk).unwrap(), SqlValue::Boolean(false));
        assert_eq!(equal(&junk, &timestamp).unwrap(), SqlValue::Boolean(false));
        assert_eq!(not_equal(&timestamp, &junk).unwrap(), SqlValue::Boolean(true));
    }

    #[test]
    fn test_time_vs_string_equality() {
        let time = SqlValue::Time(vibesql_types::Time::new(13, 15, 44, 0).unwrap());
        let matching = SqlValue::Varchar(arcstr::ArcStr::from("13:15:44"));
        let differing = SqlValue::Varchar(arcstr::ArcStr::from("13:15:45"));
        assert_eq!(equal(&time, &matching).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&matching, &time).unwrap(), SqlValue::Boolean(true));
        assert_eq!(equal(&time, &differing).unwrap(), SqlValue::Boolean(false));
    }
}
