use vibesql_types::*;

// Helper to create StringValue from &str (works with both Arc<str> and ArcStr)
fn sv(s: &str) -> vibesql_types::StringValue {
    vibesql_types::StringValue::from(s)
}


// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_empty_string_varchar() {
    let value = SqlValue::Varchar(sv(""));
    assert_eq!(format!("{}", value), "");
    assert!(!value.is_null());
}

#[test]
fn test_negative_integer() {
    let value = SqlValue::Integer(-42);
    assert_eq!(format!("{}", value), "-42");
}

#[test]
fn test_very_large_bigint() {
    let value = SqlValue::Bigint(i64::MAX);
    assert_eq!(format!("{}", value), format!("{}", i64::MAX));
}

#[test]
fn test_very_small_bigint() {
    let value = SqlValue::Bigint(i64::MIN);
    assert_eq!(format!("{}", value), format!("{}", i64::MIN));
}

#[test]
fn test_special_characters_in_varchar() {
    let value = SqlValue::Varchar(sv("Hello, 世界! 🌍"));
    assert_eq!(format!("{}", value), "Hello, 世界! 🌍");
}
