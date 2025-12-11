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
        if let (Integer(a), Integer(b)) = (left, right) {
            return Ok(Integer(a * b));
        }

        // Use helper for type coercion
        match coerce_numeric_values(left, right, "*")? {
            super::CoercedValues::ExactNumeric(a, b) => Ok(Integer(a * b)),
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a * b) as f32)),
            super::CoercedValues::Numeric(a, b) => Ok(Numeric(a * b)),
        }
    }
}
