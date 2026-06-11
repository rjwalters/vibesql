use vibesql_types::*;

// Helper to create StringValue from &str (works with both Arc<str> and ArcStr)
fn sv(s: &str) -> vibesql_types::StringValue {
    vibesql_types::StringValue::from(s)
}

// ============================================================================
// Display/Format Tests - How types are displayed
// ============================================================================

#[test]
fn test_integer_display() {
    let value = SqlValue::Integer(42);
    assert_eq!(format!("{}", value), "42");
}

#[test]
fn test_varchar_display() {
    let value = SqlValue::Varchar(sv("hello"));
    assert_eq!(format!("{}", value), "hello");
}

#[test]
fn test_boolean_true_display() {
    let value = SqlValue::Boolean(true);
    assert_eq!(format!("{}", value), "TRUE");
}

#[test]
fn test_boolean_false_display() {
    let value = SqlValue::Boolean(false);
    assert_eq!(format!("{}", value), "FALSE");
}

#[test]
fn test_null_display() {
    let value = SqlValue::Null;
    assert_eq!(format!("{}", value), "NULL");
}

#[test]
fn test_smallint_display() {
    let value = SqlValue::Smallint(100);
    assert_eq!(format!("{}", value), "100");
}

#[test]
fn test_bigint_display() {
    let value = SqlValue::Bigint(1000000);
    assert_eq!(format!("{}", value), "1000000");
}

#[test]
fn test_numeric_display() {
    // SQLite-style: minimal representation, no trailing zeros
    let value = SqlValue::Numeric(123.45);
    assert_eq!(format!("{}", value), "123.45");
}

#[test]
fn test_numeric_whole_number_display() {
    // SQLite-style: whole numbers display with .0 suffix
    let value = SqlValue::Numeric(223.0);
    assert_eq!(format!("{}", value), "223.0");

    // Negative whole numbers
    let value = SqlValue::Numeric(-42.0);
    assert_eq!(format!("{}", value), "-42.0");
}

#[test]
fn test_float_display() {
    // SQLite-style: minimal representation
    let value = SqlValue::Float(2.5);
    assert_eq!(format!("{}", value), "2.5");
}

#[test]
fn test_real_display() {
    // Note: f32 to f64 conversion may introduce precision noise
    // 2.71f32 is not exactly representable
    let value = SqlValue::Real(2.5);
    assert_eq!(format!("{}", value), "2.5");
}

#[test]
fn test_double_display() {
    let value = SqlValue::Double(123.456);
    assert_eq!(format!("{}", value), "123.456");
}

#[test]
fn test_character_display() {
    let value = SqlValue::Character(sv("test"));
    assert_eq!(format!("{}", value), "test");
}

#[test]
fn test_date_display() {
    let date = "2024-01-01".parse::<Date>().unwrap();
    let value = SqlValue::Date(date);
    assert_eq!(format!("{}", value), "2024-01-01");
}

#[test]
fn test_negative_year_date_display() {
    // SQLite zero-pads negative (astronomical) years to 4 digits after the
    // sign, e.g. date(1392399.5) renders '-0900-02-28'
    let date = Date::new(-900, 2, 28).unwrap();
    let value = SqlValue::Date(date);
    assert_eq!(format!("{}", value), "-0900-02-28");

    let date = Date::new(900, 2, 28).unwrap();
    assert_eq!(format!("{}", SqlValue::Date(date)), "0900-02-28");
}

#[test]
fn test_time_display() {
    let time = "12:30:00".parse::<Time>().unwrap();
    let value = SqlValue::Time(time);
    assert_eq!(format!("{}", value), "12:30:00");
}

#[test]
fn test_timestamp_display() {
    let timestamp = "2024-01-01 12:30:00".parse::<Timestamp>().unwrap();
    let value = SqlValue::Timestamp(timestamp);
    assert_eq!(format!("{}", value), "2024-01-01 12:30:00");
}
