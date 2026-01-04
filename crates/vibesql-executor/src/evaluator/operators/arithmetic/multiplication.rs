//! Multiplication operator (*) implementation

use vibesql_types::SqlValue;

use super::coerce_numeric_values;
use crate::errors::ExecutorError;

pub struct Multiplication;

impl Multiplication {
    /// Multiplication operator (*)
    #[inline]
    pub fn multiply(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // NULL propagation - SQL standard semantics
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Fast path for integers (both modes)
        // SQLite converts to float on overflow instead of erroring
        if let (Integer(a), Integer(b)) = (left, right) {
            return Ok(a
                .checked_mul(*b)
                .map(Integer)
                .unwrap_or_else(|| Double(*a as f64 * *b as f64)));
        }

        // Use helper for type coercion
        // SQLite converts to float on overflow instead of erroring
        match coerce_numeric_values(left, right, "*")? {
            super::CoercedValues::ExactNumeric(a, b) => Ok(a
                .checked_mul(b)
                .map(Integer)
                .unwrap_or_else(|| super::nan_to_null(Double(a as f64 * b as f64)))),
            super::CoercedValues::ApproximateNumeric(a, b) => {
                Ok(super::nan_to_null(Float((a * b) as f32)))
            }
            super::CoercedValues::Numeric(a, b) => Ok(super::nan_to_null(Numeric(a * b))),
        }
    }
}
