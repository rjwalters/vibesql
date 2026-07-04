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
            // ApproximateNumeric multiplication returns Real (f64) for SQLite
            // compatibility. SQLite's REAL type is 8-byte IEEE floating point
            // (f64), not 4-byte (f32) - squeezing the product through f32 loses
            // precision that SQLite retains (e.g. 15*0.01 must be the f64
            // product 0.15000000000000002, not f32's 0.15000000596). Mirrors
            // the same fix in division.rs. (#5818)
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(super::nan_to_null(Real(a * b))),
            super::CoercedValues::Numeric(a, b) => Ok(super::nan_to_null(Numeric(a * b))),
        }
    }
}
