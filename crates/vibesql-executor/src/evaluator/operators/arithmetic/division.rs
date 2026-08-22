//! Division operators (/, //) implementation

use vibesql_types::{SqlValue, TypeBehavior, ValueType};

use super::coerce_numeric_values;
use crate::errors::ExecutorError;

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

        // Determine the result type.
        //
        // In SQLite mode (and the mysql_slt compatibility variant), integer vs
        // float division is a property of the *coerced* operands, not the
        // original SqlValue types. `division_result_type` inspects the original
        // operands via `is_float_value`, which returns `false` for a text
        // operand — so `'2245'/3` and `'2245.5'/3` would both be classified by
        // their surface type rather than their numeric affinity. That produced
        // two bugs:
        //   1. text-integer operands like `'2245'/3` yielded float in MySQL mode
        //      (`division_result_type` always returns Numeric there), diverging from sqlite3's
        //      integer division (748.33 vs 748).
        //   2. `'2245.5'/3` in SQLite mode hit the `(ApproximateNumeric, Integer)` unreachable!
        //      panic, because coercion says float but `division_result_type` says Integer for a
        //      Varchar operand.
        //
        // Mirror how Modulo derives its result type: use the CoercedValues
        // variant directly to pick Integer vs Float in SQLite-semantics modes.
        // MySQL mode without sqlite_division_semantics is unchanged (always
        // Numeric), preserving SQLLogicTest expectations.
        let result_type = match (&coerced, &sql_mode) {
            (super::CoercedValues::ExactNumeric(_, _), vibesql_types::SqlMode::SQLite) => {
                ValueType::Integer
            }
            (super::CoercedValues::ApproximateNumeric(_, _), vibesql_types::SqlMode::SQLite) => {
                ValueType::Float
            }
            (super::CoercedValues::ExactNumeric(_, _), vibesql_types::SqlMode::MySQL { flags })
                if flags.sqlite_division_semantics =>
            {
                ValueType::Integer
            }
            (
                super::CoercedValues::ApproximateNumeric(_, _),
                vibesql_types::SqlMode::MySQL { flags },
            ) if flags.sqlite_division_semantics => ValueType::Float,
            _ => sql_mode.division_result_type(left, right),
        };

        // Perform division based on coerced values and convert to determined type
        // Apply nan_to_null for SQLite compatibility (infinity/infinity = NaN → NULL)
        match (coerced, result_type) {
            // ExactNumeric division - result type depends on SQL mode
            (super::CoercedValues::ExactNumeric(a, b), ValueType::Numeric) => {
                // MySQL mode: exact decimal division
                let result = (a as f64) / (b as f64);
                Ok(super::nan_to_null(Numeric(result)))
            }
            (super::CoercedValues::ExactNumeric(a, b), ValueType::Integer) => {
                // SQLite mode: integer division (truncated toward zero)
                let result = ((a as f64) / (b as f64)).trunc() as i64;
                Ok(Integer(result))
            }
            // ApproximateNumeric division - returns Real (f64) for SQLite compatibility
            // SQLite's REAL type is 8-byte IEEE floating point (f64), not 4-byte (f32)
            (super::CoercedValues::ApproximateNumeric(a, b), ValueType::Float) => {
                Ok(super::nan_to_null(Real(a / b)))
            }
            // Numeric division - always returns Numeric
            (super::CoercedValues::Numeric(a, b), ValueType::Numeric) => {
                Ok(super::nan_to_null(Numeric(a / b)))
            }
            // Handle edge case: if TypeBehavior returns Float for approximate operands
            // but coercion produced Numeric, convert to Numeric
            (super::CoercedValues::Numeric(a, b), ValueType::Float) => {
                Ok(super::nan_to_null(Numeric(a / b)))
            }
            // ApproximateNumeric with Numeric result type (MySQL mode with Float operands)
            // MySQL always returns Numeric for division even with Float inputs
            (super::CoercedValues::ApproximateNumeric(a, b), ValueType::Numeric) => {
                Ok(super::nan_to_null(Numeric(a / b)))
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
