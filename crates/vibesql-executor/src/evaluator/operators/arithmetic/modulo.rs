//! Modulo operator (%) implementation

use vibesql_types::SqlValue;

use super::coerce_numeric_values;
use crate::errors::ExecutorError;

pub struct Modulo;

impl Modulo {
    /// Modulo operator (%)
    /// Returns the remainder of division
    #[inline]
    pub fn modulo(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // NULL propagation - SQL standard semantics
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Fast path for integers (both modes)
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                return Ok(SqlValue::Null);
            }
            // Handle i64::MIN % -1 which would overflow (result is 0)
            if *a == i64::MIN && *b == -1 {
                return Ok(Integer(0));
            }
            return Ok(Integer(a % b));
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "%")?;

        // Check for modulo by zero and return NULL (SQL standard behavior)
        let is_zero = match &coerced {
            super::CoercedValues::ExactNumeric(_, right) => *right == 0,
            super::CoercedValues::ApproximateNumeric(_, right) => *right == 0.0,
            super::CoercedValues::Numeric(_, right) => *right == 0.0,
        };

        if is_zero {
            return Ok(SqlValue::Null);
        }

        match coerced {
            super::CoercedValues::ExactNumeric(a, b) => {
                // Handle i64::MIN % -1 which would overflow (result is 0)
                if a == i64::MIN && b == -1 {
                    Ok(Integer(0))
                } else {
                    Ok(Integer(a % b))
                }
            }
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a % b) as f32)),
            super::CoercedValues::Numeric(a, b) => Ok(Numeric(a % b)),
        }
    }
}
