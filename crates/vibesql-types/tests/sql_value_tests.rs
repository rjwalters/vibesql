use vibesql_types::*;

// Helper to create StringValue from &str (works with both Arc<str> and ArcStr)
fn sv(s: &str) -> StringValue {
    StringValue::from(s)
}

// ============================================================================
// SqlValue Tests - Test SQL value representation
// ============================================================================

#[test]
fn test_integer_value_creation() {
    let value = SqlValue::Integer(42);
    assert_eq!(format!("{:?}", value), "Integer(42)");
}

#[test]
fn test_varchar_value_creation() {
    let value = SqlValue::Varchar(sv("hello"));
    assert_eq!(format!("{:?}", value), "Varchar(\"hello\")");
}

#[test]
fn test_boolean_true_value() {
    let value = SqlValue::Boolean(true);
    assert_eq!(format!("{:?}", value), "Boolean(true)");
}

#[test]
fn test_boolean_false_value() {
    let value = SqlValue::Boolean(false);
    assert_eq!(format!("{:?}", value), "Boolean(false)");
}

#[test]
fn test_null_value() {
    let value = SqlValue::Null;
    assert_eq!(format!("{:?}", value), "Null");
}

// ============================================================================
// SqlValue::is_null() Tests - Check if value is NULL
// ============================================================================

#[test]
fn test_null_is_null() {
    let value = SqlValue::Null;
    assert!(value.is_null());
}

#[test]
fn test_integer_is_not_null() {
    let value = SqlValue::Integer(42);
    assert!(!value.is_null());
}

#[test]
fn test_varchar_is_not_null() {
    let value = SqlValue::Varchar(sv("test"));
    assert!(!value.is_null());
}

// ============================================================================
// SqlValue::get_type() Tests - Get the type of a value
// ============================================================================

#[test]
fn test_integer_value_has_integer_type() {
    let value = SqlValue::Integer(42);
    assert_eq!(value.get_type(), DataType::Integer);
}

#[test]
fn test_varchar_value_has_varchar_type() {
    let value = SqlValue::Varchar(sv("test"));
    // Note: VARCHAR values don't have a specific max_length in the value itself
    // We'll handle this properly when we implement it
    match value.get_type() {
        DataType::Varchar { .. } => {} // Success
        _ => panic!("Expected Varchar type"),
    }
}

#[test]
fn test_boolean_value_has_boolean_type() {
    let value = SqlValue::Boolean(true);
    assert_eq!(value.get_type(), DataType::Boolean);
}

#[test]
fn test_null_value_has_null_type() {
    let value = SqlValue::Null;
    assert_eq!(value.get_type(), DataType::Null);
}

#[test]
fn test_smallint_value_has_smallint_type() {
    let value = SqlValue::Smallint(42);
    assert_eq!(value.get_type(), DataType::Smallint);
}

#[test]
fn test_bigint_value_has_bigint_type() {
    let value = SqlValue::Bigint(1000);
    assert_eq!(value.get_type(), DataType::Bigint);
}

#[test]
fn test_numeric_value_has_numeric_type() {
    let value = SqlValue::Numeric(123.45);
    match value.get_type() {
        DataType::Numeric { .. } => {} // Success
        _ => panic!("Expected Numeric type"),
    }
}

#[test]
fn test_float_value_has_float_type() {
    let value = SqlValue::Float(2.5);
    assert_eq!(value.get_type(), DataType::Float { precision: 53 });
}

#[test]
fn test_real_value_has_real_type() {
    let value = SqlValue::Real(2.71);
    assert_eq!(value.get_type(), DataType::Real);
}

#[test]
fn test_double_value_has_double_type() {
    let value = SqlValue::Double(123.456);
    assert_eq!(value.get_type(), DataType::DoublePrecision);
}

#[test]
fn test_character_value_has_character_type() {
    let value = SqlValue::Character(sv("hello"));
    match value.get_type() {
        DataType::Character { .. } => {} // Success
        _ => panic!("Expected Character type"),
    }
}

#[test]
fn test_date_value_has_date_type() {
    let date = "2024-01-01".parse::<Date>().unwrap();
    let value = SqlValue::Date(date);
    assert_eq!(value.get_type(), DataType::Date);
}

#[test]
fn test_time_value_has_time_type() {
    let time = "12:30:00".parse::<Time>().unwrap();
    let value = SqlValue::Time(time);
    match value.get_type() {
        DataType::Time { .. } => {} // Success
        _ => panic!("Expected Time type"),
    }
}

#[test]
fn test_timestamp_value_has_timestamp_type() {
    let timestamp = "2024-01-01 12:30:00".parse::<Timestamp>().unwrap();
    let value = SqlValue::Timestamp(timestamp);
    match value.get_type() {
        DataType::Timestamp { .. } => {} // Success
        _ => panic!("Expected Timestamp type"),
    }
}
