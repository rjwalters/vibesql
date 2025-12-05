//! Division operators (/, //) implementation

use vibesql_types::{SqlValue, TypeBehavior, ValueType};

use crate::errors::ExecutorError;

use super::coerce_numeric_values;

pub struct Division;

impl Division {
    /// Division operator (/)
    #[inline]
    pub fn divide(
        left: &SqlValue,
        right: &SqlValue,
        sql_mode: vibesql_types::SqlMode,
    ) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // NULL propagation - SQL standard semantics
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Fast path for integers - behavior depends on SQL mode
        // Use TypeBehavior trait to determine result type
        // MySQL: INTEGER / INTEGER → NUMERIC (exact decimal division)
        // SQLite: INTEGER / INTEGER → INTEGER (truncated division)
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                // Check strict mode - MySQL behavior
                if let Some(flags) = sql_mode.mysql_flags() {
                    if flags.strict_mode {
                        return Err(ExecutorError::DivisionByZero);
                    }
                }
                return Ok(Null); // Non-strict mode: division by zero returns NULL
            }

            // Use TypeBehavior trait to determine result type
            let result_type = sql_mode.division_result_type(left, right);

            return match result_type {
                ValueType::Numeric => {
                    // MySQL mode: exact decimal division
                    let result = (*a as f64) / (*b as f64);
                    Ok(Numeric(result))
                }
                ValueType::Integer => {
                    // SQLite mode: integer division (truncated toward zero)
                    let result = ((*a as f64) / (*b as f64)).trunc() as i64;
                    Ok(Integer(result))
                }
                _ => unreachable!("Integer division should only return Numeric or Integer"),
            };
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "/")?;

        // Check for division by zero - behavior depends on strict mode
        let is_zero = match &coerced {
            super::CoercedValues::ExactNumeric(_, right) => *right == 0,
            super::CoercedValues::ApproximateNumeric(_, right) => *right == 0.0,
            super::CoercedValues::Numeric(_, right) => *right == 0.0,
        };

        if is_zero {
            // Check strict mode - MySQL behavior
            if let Some(flags) = sql_mode.mysql_flags() {
                if flags.strict_mode {
                    return Err(ExecutorError::DivisionByZero);
                }
            }
            return Ok(Null); // Non-strict mode: division by zero returns NULL
        }

        // Use TypeBehavior trait to determine result type based on original operands
        let result_type = sql_mode.division_result_type(left, right);

        // Perform division based on coerced values and convert to determined type
        match (coerced, result_type) {
            // ExactNumeric division - result type depends on SQL mode
            (super::CoercedValues::ExactNumeric(a, b), ValueType::Numeric) => {
                // MySQL mode: exact decimal division
                let result = (a as f64) / (b as f64);
                Ok(Numeric(result))
            }
            (super::CoercedValues::ExactNumeric(a, b), ValueType::Integer) => {
                // SQLite mode: integer division (truncated toward zero)
                let result = ((a as f64) / (b as f64)).trunc() as i64;
                Ok(Integer(result))
            }
            // ApproximateNumeric division - always returns Float
            (super::CoercedValues::ApproximateNumeric(a, b), ValueType::Float) => {
                Ok(Float((a / b) as f32))
            }
            // Numeric division - always returns Numeric
            (super::CoercedValues::Numeric(a, b), ValueType::Numeric) => Ok(Numeric(a / b)),
            // Handle edge case: if TypeBehavior returns Float for approximate operands
            // but coercion produced Numeric, convert to Numeric
            (super::CoercedValues::Numeric(a, b), ValueType::Float) => Ok(Numeric(a / b)),
            // ApproximateNumeric with Numeric result type (MySQL mode with Float operands)
            // MySQL always returns Numeric for division even with Float inputs
            (super::CoercedValues::ApproximateNumeric(a, b), ValueType::Numeric) => {
                Ok(Numeric(a / b))
            }
            // All other combinations should be unreachable due to type coercion rules
            _ => unreachable!("Unexpected combination of coerced type and result type"),
        }
    }

    /// Integer division operator (DIV) - MySQL-specific
    /// Returns integer result, truncating fractional part (truncates toward zero)
    #[inline]
    pub fn integer_divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // NULL propagation - SQL standard semantics
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Fast path for integers (both modes)
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                // SQL standard: division by zero returns NULL
                return Ok(Null);
            }
            // Integer division truncates toward zero (not floor division)
            let result = ((*a as f64) / (*b as f64)).trunc() as i64;
            return Ok(Integer(result));
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "DIV")?;

        // Check for division by zero and raise error
        let is_zero = match &coerced {
            super::CoercedValues::ExactNumeric(_, right) => *right == 0,
            super::CoercedValues::ApproximateNumeric(_, right) => *right == 0.0,
            super::CoercedValues::Numeric(_, right) => *right == 0.0,
        };

        if is_zero {
            // SQL standard: division by zero returns NULL
            return Ok(Null);
        }

        // Integer division truncates toward zero
        match coerced {
            super::CoercedValues::ExactNumeric(a, b) => {
                let result = ((a as f64) / (b as f64)).trunc() as i64;
                Ok(Integer(result))
            }
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Integer((a / b).trunc() as i64)),
            super::CoercedValues::Numeric(a, b) => Ok(Integer((a / b).trunc() as i64)),
        }
    }
}
