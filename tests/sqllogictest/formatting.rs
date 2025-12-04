//! SQL value formatting utilities for test output.

use sqllogictest::DefaultColumnType;
use vibesql_types::SqlValue;

/// Format SQL value for test output, applying type-specific formatting rules
pub fn format_sql_value(value: &SqlValue, expected_type: Option<&DefaultColumnType>) -> String {
    match value {
        SqlValue::Integer(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Smallint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Bigint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Unsigned(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Numeric(n) => {
            // Format based on expected type from test
            if matches!(expected_type, Some(DefaultColumnType::Integer)) {
                // Test expects integer format - round to nearest integer
                // MySQL behavior: DECIMAL values round to nearest when converted to INTEGER
                format!("{:.0}", n)
            } else if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                // Test expects floating point format - always use 3 decimal places
                format!("{:.3}", n)
            } else {
                // No type hint - use simple integer format for whole numbers (MySQL default)
                if n.fract() == 0.0 && n.abs() < 1e15 {
                    format!("{:.0}", n)
                } else {
                    n.to_string()
                }
            }
        }
        SqlValue::Float(f) | SqlValue::Real(f) => {
            // Always format with 3 decimal places for SQLLogicTest compatibility
            format!("{:.3}", f)
        }
        SqlValue::Double(f) => {
            // Always format with 3 decimal places for SQLLogicTest compatibility
            format!("{:.3}", f)
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
        SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Date(d) => d.to_string(),
        SqlValue::Time(d) => d.to_string(),
        SqlValue::Timestamp(d) => d.to_string(),
        SqlValue::Interval(d) => d.to_string(),
        SqlValue::Vector(v) => {
            let formatted: Vec<String> = v.iter().map(|f| f.to_string()).collect();
            format!("[{}]", formatted.join(", "))
        }
    }
}

/// Format value in canonical form for hashing (plain format without display decorations)
#[allow(dead_code)]
pub fn format_sql_value_canonical(
    value: &SqlValue,
    expected_type: Option<&DefaultColumnType>,
) -> String {
    match value {
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Smallint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Bigint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Unsigned(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Numeric(n) => {
            // Format based on expected type from test
            if matches!(expected_type, Some(DefaultColumnType::Integer)) {
                // Test expects integer format - round to nearest integer
                // MySQL behavior: DECIMAL values round to nearest when converted to INTEGER
                format!("{:.0}", n)
            } else if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                // Test expects floating point format - always use 3 decimal places
                format!("{:.3}", n)
            } else {
                // No type hint - use simple integer format for whole numbers (MySQL default)
                if n.fract() == 0.0 && n.abs() < 1e15 {
                    format!("{:.0}", n)
                } else {
                    n.to_string()
                }
            }
        }
        SqlValue::Float(f) | SqlValue::Real(f) => {
            // Always format with 3 decimal places for SQLLogicTest compatibility
            format!("{:.3}", f)
        }
        SqlValue::Double(f) => {
            // Always format with 3 decimal places for SQLLogicTest compatibility
            format!("{:.3}", f)
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
        SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Date(d) => d.to_string(),
        SqlValue::Time(d) => d.to_string(),
        SqlValue::Timestamp(d) => d.to_string(),
        SqlValue::Interval(d) => d.to_string(),
        SqlValue::Vector(v) => {
            let formatted: Vec<String> = v.iter().map(|f| f.to_string()).collect();
            format!("[{}]", formatted.join(", "))
        }
    }
}
