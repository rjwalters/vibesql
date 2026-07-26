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

        // Check for modulo by zero and return NULL (SQL standard behavior).
        // For the floating branches this must check the *truncated* divisor
        // (matching sqlite3's iA==0 check on the already-(i64)-cast right
        // operand below), not the raw float — e.g. `5 % 0.9` truncates the
        // divisor to 0 and must return NULL even though 0.9 != 0.0.
        let is_zero = match &coerced {
            super::CoercedValues::ExactNumeric(_, right) => *right == 0,
            super::CoercedValues::ApproximateNumeric(_, right) => (*right as i64) == 0,
            super::CoercedValues::Numeric(_, right) => (*right as i64) == 0,
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
            // SQLite's % operator always truncates BOTH operands to INTEGER
            // (i64) before computing the remainder, even when one or both
            // operands are floating point — it never computes a true
            // floating-point `fmod`. The result is then represented using
            // the coerced-out floating type (REAL/NUMERIC), not INTEGER
            // (matches sqlite3 vdbe.c's fp_math OP_Remainder path: iA=(i64)
            // rA; iB=(i64)rB; rB=(double)(iB%iA)). So `72.35 % 5` truncates
            // 72.35 -> 72 first, giving 72%5=2, printed as REAL `2.0` — not
            // a literal fmod(72.35, 5) = 2.35 (#6172).
            super::CoercedValues::ApproximateNumeric(a, b) => {
                let ia = a as i64;
                let ib = b as i64;
                let ib = if ib == -1 { 1 } else { ib };
                Ok(Float((ia % ib) as f32))
            }
            super::CoercedValues::Numeric(a, b) => {
                let ia = a as i64;
                let ib = b as i64;
                let ib = if ib == -1 { 1 } else { ib };
                Ok(Numeric((ia % ib) as f64))
            }
        }
    }
}
