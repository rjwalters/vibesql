use vibesql_types::*;

// Helper to create StringValue from &str (works with both Arc<str> and ArcStr)
fn sv(s: &str) -> vibesql_types::StringValue {
    vibesql_types::StringValue::from(s)
}


// ============================================================================
// PartialOrd Tests for SqlValue
// ============================================================================

#[test]
fn test_integer_ordering() {
    assert!(SqlValue::Integer(1) < SqlValue::Integer(2));
    assert!(SqlValue::Integer(2) > SqlValue::Integer(1));
    assert_eq!(
        SqlValue::Integer(1).partial_cmp(&SqlValue::Integer(1)),
        Some(std::cmp::Ordering::Equal)
    );
}

#[test]
fn test_smallint_ordering() {
    assert!(SqlValue::Smallint(10) < SqlValue::Smallint(20));
    assert!(SqlValue::Smallint(20) > SqlValue::Smallint(10));
}

#[test]
fn test_bigint_ordering() {
    assert!(SqlValue::Bigint(1000) < SqlValue::Bigint(2000));
    assert!(SqlValue::Bigint(2000) > SqlValue::Bigint(1000));
}

#[test]
fn test_float_ordering() {
    assert!(SqlValue::Float(1.5) < SqlValue::Float(2.5));
    assert!(SqlValue::Float(2.5) > SqlValue::Float(1.5));
}

#[test]
fn test_float_nan_is_incomparable() {
    let nan = SqlValue::Float(f32::NAN);
    let one = SqlValue::Float(1.0);
    assert_eq!(nan.partial_cmp(&one), None);
    assert_eq!(one.partial_cmp(&nan), None);
    assert_eq!(nan.partial_cmp(&nan), None);
}

#[test]
fn test_double_ordering() {
    assert!(SqlValue::Double(1.5) < SqlValue::Double(2.5));
    assert!(SqlValue::Double(2.5) > SqlValue::Double(1.5));
}

#[test]
fn test_double_nan_is_incomparable() {
    let nan = SqlValue::Double(f64::NAN);
    let one = SqlValue::Double(1.0);
    assert_eq!(nan.partial_cmp(&one), None);
    assert_eq!(one.partial_cmp(&nan), None);
}

#[test]
fn test_varchar_ordering() {
    assert!(SqlValue::Varchar(sv("apple")) < SqlValue::Varchar(sv("banana")));
    assert!(SqlValue::Varchar(sv("zebra")) > SqlValue::Varchar(sv("aardvark")));
}

#[test]
fn test_character_ordering() {
    assert!(SqlValue::Character(sv("a")) < SqlValue::Character(sv("b")));
    assert!(SqlValue::Character(sv("z")) > SqlValue::Character(sv("a")));
}

#[test]
fn test_boolean_ordering() {
    // In SQL, FALSE < TRUE
    assert!(SqlValue::Boolean(false) < SqlValue::Boolean(true));
    assert!(SqlValue::Boolean(true) > SqlValue::Boolean(false));
    assert_eq!(
        SqlValue::Boolean(true).partial_cmp(&SqlValue::Boolean(true)),
        Some(std::cmp::Ordering::Equal)
    );
}

#[test]
fn test_numeric_ordering() {
    assert!(SqlValue::Numeric(1.5) < SqlValue::Numeric(2.5));
    assert!(SqlValue::Numeric(100.0) > SqlValue::Numeric(50.5));
}

#[test]
fn test_numeric_invalid_is_incomparable() {
    let invalid = SqlValue::Numeric(f64::NAN);
    let valid = SqlValue::Numeric(1.0);
    assert_eq!(invalid.partial_cmp(&valid), None);
    assert_eq!(valid.partial_cmp(&invalid), None);
}

#[test]
fn test_date_ordering() {
    let date1 = "2024-01-01".parse::<Date>().unwrap();
    let date2 = "2024-12-31".parse::<Date>().unwrap();
    assert!(SqlValue::Date(date1) < SqlValue::Date(date2));
    assert!(SqlValue::Date(date2) > SqlValue::Date(date1));
}

#[test]
fn test_time_ordering() {
    let time1 = "09:00:00".parse::<Time>().unwrap();
    let time2 = "17:00:00".parse::<Time>().unwrap();
    assert!(SqlValue::Time(time1) < SqlValue::Time(time2));
    assert!(SqlValue::Time(time2) > SqlValue::Time(time1));
}

#[test]
fn test_timestamp_ordering() {
    let ts1 = "2024-01-01 09:00:00".parse::<Timestamp>().unwrap();
    let ts2 = "2024-01-01 17:00:00".parse::<Timestamp>().unwrap();
    assert!(SqlValue::Timestamp(ts1) < SqlValue::Timestamp(ts2));
}

#[test]
fn test_null_is_incomparable() {
    // NULL compared to anything (including NULL) returns None
    assert_eq!(SqlValue::Null.partial_cmp(&SqlValue::Integer(1)), None);
    assert_eq!(SqlValue::Integer(1).partial_cmp(&SqlValue::Null), None);
    assert_eq!(SqlValue::Null.partial_cmp(&SqlValue::Null), None);
}

#[test]
fn test_type_mismatch_is_incomparable() {
    // Different types cannot be compared
    assert_eq!(SqlValue::Integer(1).partial_cmp(&SqlValue::Varchar(sv("1"))), None);
    assert_eq!(SqlValue::Float(1.0).partial_cmp(&SqlValue::Integer(1)), None);
    assert_eq!(SqlValue::Boolean(true).partial_cmp(&SqlValue::Integer(1)), None);
}

#[test]
fn test_can_use_comparison_operators() {
    // Test that Rust's comparison operators work with PartialOrd
    let a = SqlValue::Integer(1);
    let b = SqlValue::Integer(2);

    assert!(a < b);
    assert!(b > a);
    assert!(a <= b);
    assert!(b >= a);
    assert!(a == a);
    assert!(a != b);
}

#[test]
fn test_real_ordering() {
    assert!(SqlValue::Real(1.5) < SqlValue::Real(2.5));
    assert!(SqlValue::Real(2.5) > SqlValue::Real(1.5));
}

#[test]
fn test_real_nan_is_incomparable() {
    let nan = SqlValue::Real(f32::NAN);
    let one = SqlValue::Real(1.0);
    assert_eq!(nan.partial_cmp(&one), None);
    assert_eq!(one.partial_cmp(&nan), None);
}
