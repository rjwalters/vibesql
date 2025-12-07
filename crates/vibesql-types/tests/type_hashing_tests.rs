// ============================================================================

// Helper to create StringValue from &str (works with both Arc<str> and ArcStr)
fn sv(s: &str) -> vibesql_types::StringValue {
    vibesql_types::StringValue::from(s)
}

// Hash Implementation Tests (for DISTINCT operations)
// ============================================================================
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use vibesql_types::*;

fn calculate_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn test_integer_hash() {
    let v1 = SqlValue::Integer(42);
    let v2 = SqlValue::Integer(42);
    let v3 = SqlValue::Integer(43);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    assert_ne!(calculate_hash(&v1), calculate_hash(&v3));
}

#[test]
fn test_smallint_hash() {
    let v1 = SqlValue::Smallint(10);
    let v2 = SqlValue::Smallint(10);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_bigint_hash() {
    let v1 = SqlValue::Bigint(1000);
    let v2 = SqlValue::Bigint(1000);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_numeric_hash() {
    let v1 = SqlValue::Numeric(123.45);
    let v2 = SqlValue::Numeric(123.45);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_float_hash() {
    let v1 = SqlValue::Float(2.5);
    let v2 = SqlValue::Float(2.5);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_float_nan_hash() {
    // NaN values should hash to the same value for DISTINCT operations
    let nan1 = SqlValue::Float(f32::NAN);
    let nan2 = SqlValue::Float(f32::NAN);
    assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
}

#[test]
fn test_real_hash() {
    let v1 = SqlValue::Real(2.71);
    let v2 = SqlValue::Real(2.71);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_real_nan_hash() {
    let nan1 = SqlValue::Real(f32::NAN);
    let nan2 = SqlValue::Real(f32::NAN);
    assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
}

#[test]
fn test_double_hash() {
    let v1 = SqlValue::Double(123.456);
    let v2 = SqlValue::Double(123.456);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_double_nan_hash() {
    let nan1 = SqlValue::Double(f64::NAN);
    let nan2 = SqlValue::Double(f64::NAN);
    assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
}

#[test]
fn test_varchar_hash() {
    let v1 = SqlValue::Varchar(sv("hello"));
    let v2 = SqlValue::Varchar(sv("hello"));
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_character_hash() {
    let v1 = SqlValue::Character(sv("test"));
    let v2 = SqlValue::Character(sv("test"));
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_boolean_hash() {
    let v1 = SqlValue::Boolean(true);
    let v2 = SqlValue::Boolean(true);
    let v3 = SqlValue::Boolean(false);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    assert_ne!(calculate_hash(&v1), calculate_hash(&v3));
}

#[test]
fn test_date_hash() {
    let date1 = "2024-01-01".parse::<Date>().unwrap();
    let date2 = "2024-01-01".parse::<Date>().unwrap();
    let v1 = SqlValue::Date(date1);
    let v2 = SqlValue::Date(date2);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_time_hash() {
    let time1 = "12:30:00".parse::<Time>().unwrap();
    let time2 = "12:30:00".parse::<Time>().unwrap();
    let v1 = SqlValue::Time(time1);
    let v2 = SqlValue::Time(time2);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_timestamp_hash() {
    let ts1 = "2024-01-01 12:30:00".parse::<Timestamp>().unwrap();
    let ts2 = "2024-01-01 12:30:00".parse::<Timestamp>().unwrap();
    let v1 = SqlValue::Timestamp(ts1);
    let v2 = SqlValue::Timestamp(ts2);
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}

#[test]
fn test_null_hash() {
    let v1 = SqlValue::Null;
    let v2 = SqlValue::Null;
    // NULL values should hash consistently
    assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
}
