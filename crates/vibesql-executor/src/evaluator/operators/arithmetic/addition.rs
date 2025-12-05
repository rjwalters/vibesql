//! Addition operator (+) implementation

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::evaluator::coercion::coerce_to_date;
use crate::evaluator::functions::datetime::date_add_subtract;

use super::coerce_numeric_values;

pub struct Addition;

impl Addition {
    /// Addition operator (+)
    #[inline]
    pub fn add(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // NULL propagation - SQL standard semantics
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Fast path for integers (both modes)
        if let (Integer(a), Integer(b)) = (left, right) {
            return Ok(Integer(a + b));
        }

        // Date + Interval arithmetic
        match (left, right) {
            // NULL handling for interval arithmetic
            (Null, Interval(_)) | (Interval(_), Null) => return Ok(Null),
            (Null, Date(_)) | (Date(_), Null) => return Ok(Null),
            (Null, Timestamp(_)) | (Timestamp(_), Null) => return Ok(Null),
            (Null, Varchar(_))
            | (Varchar(_), Null)
            | (Null, Character(_))
            | (Character(_), Null) => {
                // Check if this is date arithmetic with NULL
                if matches!(left, Interval(_)) || matches!(right, Interval(_)) {
                    return Ok(Null);
                }
            }

            // DATE + INTERVAL
            (Date(date), Interval(interval)) => {
                return apply_interval_to_date(&date.to_string(), interval, true);
            }

            // INTERVAL + DATE (commutative)
            (Interval(interval), Date(date)) => {
                return apply_interval_to_date(&date.to_string(), interval, true);
            }

            // TIMESTAMP + INTERVAL
            (Timestamp(ts), Interval(interval)) => {
                return apply_interval_to_date(&ts.to_string(), interval, true);
            }

            // INTERVAL + TIMESTAMP (commutative)
            (Interval(interval), Timestamp(ts)) => {
                return apply_interval_to_date(&ts.to_string(), interval, true);
            }

            // VARCHAR + INTERVAL (with coercion to DATE)
            (Varchar(_) | Character(_), Interval(interval)) => {
                let date_val = coerce_to_date(left)?;
                let date_str = date_val_to_string(&date_val)?;
                return apply_interval_to_date(&date_str, interval, true);
            }

            // INTERVAL + VARCHAR (commutative, with coercion to DATE)
            (Interval(interval), Varchar(_) | Character(_)) => {
                let date_val = coerce_to_date(right)?;
                let date_str = date_val_to_string(&date_val)?;
                return apply_interval_to_date(&date_str, interval, true);
            }

            _ => {}
        }

        // Use helper for numeric type coercion
        match coerce_numeric_values(left, right, "+")? {
            super::CoercedValues::ExactNumeric(a, b) => Ok(Integer(a + b)),
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a + b) as f32)),
            super::CoercedValues::Numeric(a, b) => Ok(Numeric(a + b)),
        }
    }
}

/// Apply an interval to a date/timestamp value
///
/// is_addition: true for +, false for -
fn apply_interval_to_date(
    date_str: &str,
    interval: &vibesql_types::Interval,
    is_addition: bool,
) -> Result<SqlValue, ExecutorError> {
    // Parse interval string format: "5 DAY", "1-6 YEAR_MONTH", etc.
    let parts: Vec<&str> = interval.value.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Invalid interval format: '{}'",
            interval.value
        )));
    }

    // Extract amount and unit
    let amount_str = parts[0];
    let unit_str = parts[1];

    // Parse amount (handle compound formats like "1-6" for YEAR_MONTH)
    let amount: i64 = if amount_str.contains('-') {
        // Compound interval: extract first value for now
        // TODO: Handle compound intervals properly in Phase 4
        let parts: Vec<&str> = amount_str.split('-').collect();
        parts[0].parse().map_err(|_| {
            ExecutorError::UnsupportedFeature(format!("Invalid interval amount: '{}'", amount_str))
        })?
    } else {
        amount_str.parse().map_err(|_| {
            ExecutorError::UnsupportedFeature(format!("Invalid interval amount: '{}'", amount_str))
        })?
    };

    // Negate amount for subtraction
    let signed_amount = if is_addition { amount } else { -amount };

    // Delegate to existing date arithmetic helper
    // Note: date_add_subtract expects unit like "DAY", "MONTH", etc.
    date_add_subtract(date_str, signed_amount, unit_str, true)
}

/// Convert a SqlValue (Date or Timestamp) to string representation
fn date_val_to_string(value: &SqlValue) -> Result<String, ExecutorError> {
    match value {
        SqlValue::Date(d) => Ok(d.to_string()),
        SqlValue::Timestamp(ts) => Ok(ts.to_string()),
        SqlValue::Null => Ok("NULL".to_string()), // Should not reach here due to NULL checks
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "Expected DATE or TIMESTAMP, got {:?}",
            val
        ))),
    }
}
