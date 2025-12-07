//! Decimal formatting functions
//!
//! Implements FORMAT function and helper utilities for number formatting.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// FORMAT(number, decimal_places) - Format number with thousand separators
pub fn format(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "FORMAT requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (number_val, SqlValue::Integer(decimals)) => {
            let decimals = (*decimals).max(0) as usize;

            // Convert number to f64
            let num = match number_val {
                SqlValue::Integer(n) => *n as f64,
                SqlValue::Bigint(n) => *n as f64,
                SqlValue::Smallint(n) => *n as f64,
                SqlValue::Float(f) => *f as f64,
                SqlValue::Double(f) => *f,
                SqlValue::Real(f) => *f as f64,
                SqlValue::Numeric(f) => *f,
                val => {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "FORMAT requires numeric argument, got {:?}",
                        val
                    )))
                }
            };

            let formatted = format_number(num, decimals);
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        (number, decimals) => Err(ExecutorError::UnsupportedFeature(format!(
            "FORMAT requires (number, integer) arguments, got {:?} and {:?}",
            number, decimals
        ))),
    }
}

/// Helper function to format a number with thousand separators and decimal places
fn format_number(n: f64, decimals: usize) -> String {
    // Handle special cases
    if n.is_nan() {
        return "NaN".to_string();
    }
    if n.is_infinite() {
        return if n.is_sign_positive() { "Infinity" } else { "-Infinity" }.to_string();
    }

    // Round to specified decimal places
    let multiplier = 10_f64.powi(decimals as i32);
    let rounded = (n * multiplier).round() / multiplier;

    // Format with fixed decimal places
    let formatted = format!("{:.prec$}", rounded, prec = decimals);

    // Split into integer and decimal parts
    let parts: Vec<&str> = formatted.split('.').collect();
    let integer_part = parts[0];
    let decimal_part = if parts.len() > 1 { parts[1] } else { "" };

    // Add thousand separators to integer part
    let with_commas = if let Some(positive_part) = integer_part.strip_prefix('-') {
        // Handle negative numbers
        let formatted_positive = add_thousand_separators(positive_part);
        format!("-{}", formatted_positive)
    } else {
        add_thousand_separators(integer_part)
    };

    // Combine with decimal part
    if decimals > 0 {
        format!("{}.{}", with_commas, decimal_part)
    } else {
        with_commas
    }
}

/// Helper to add thousand separators to integer string
fn add_thousand_separators(s: &str) -> String {
    s.chars().rev().enumerate().fold(String::new(), |acc, (i, c)| {
        if i > 0 && i % 3 == 0 {
            format!("{},{}", c, acc)
        } else {
            format!("{}{}", c, acc)
        }
    })
}
