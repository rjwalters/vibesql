//! Bitwise operator implementations
//!
//! Handles: |, &, ~, <<, >>
//! Supports: Integer, Smallint, Bigint types (coerces others to i64)
//! SQLite compatibility: All bitwise operations return Integer results

use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, evaluator::casting::to_i64};

/// Bitwise operations (SQLite compatible)
pub(crate) struct BitwiseOps;

impl BitwiseOps {
    /// Bitwise OR operator (|)
    #[inline]
    pub fn or(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        // NULL propagation is handled at the registry level
        let left_i64 = to_i64(left)?;
        let right_i64 = to_i64(right)?;
        Ok(SqlValue::Integer(left_i64 | right_i64))
    }

    /// Bitwise AND operator (&)
    #[inline]
    pub fn and(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        let left_i64 = to_i64(left)?;
        let right_i64 = to_i64(right)?;
        Ok(SqlValue::Integer(left_i64 & right_i64))
    }

    /// Bitwise NOT operator (~)
    /// This is a unary operator but we provide it here for consistency
    #[inline]
    #[allow(dead_code)] // Provided for completeness; will be used when unary ~ is implemented
    pub fn not(value: &SqlValue) -> Result<SqlValue, ExecutorError> {
        let val_i64 = to_i64(value)?;
        Ok(SqlValue::Integer(!val_i64))
    }

    /// Left shift operator (<<)
    #[inline]
    pub fn left_shift(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        let left_i64 = to_i64(left)?;
        let right_i64 = to_i64(right)?;

        // SQLite behavior for shifts:
        // - If shift amount is negative, shift in opposite direction
        // - If shift amount is >= 64, result is 0 (or -1 for >> on negative numbers)
        let result = if right_i64 < 0 {
            // Negative shift amount: shift in opposite direction
            let shift = (-right_i64) as u32;
            if shift >= 64 {
                if left_i64 < 0 {
                    -1 // Arithmetic right shift on negative number fills with 1s
                } else {
                    0
                }
            } else {
                left_i64 >> shift
            }
        } else {
            let shift = right_i64 as u32;
            if shift >= 64 {
                0
            } else {
                left_i64 << shift
            }
        };
        Ok(SqlValue::Integer(result))
    }

    /// Right shift operator (>>)
    /// SQLite uses arithmetic (signed) right shift
    #[inline]
    pub fn right_shift(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        let left_i64 = to_i64(left)?;
        let right_i64 = to_i64(right)?;

        let result = if right_i64 < 0 {
            // Negative shift amount: shift in opposite direction
            let shift = (-right_i64) as u32;
            if shift >= 64 {
                0
            } else {
                left_i64 << shift
            }
        } else {
            let shift = right_i64 as u32;
            if shift >= 64 {
                if left_i64 < 0 {
                    -1 // Arithmetic right shift on negative fills with 1s
                } else {
                    0
                }
            } else {
                left_i64 >> shift
            }
        };
        Ok(SqlValue::Integer(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitwise_or() {
        // 5 | 3 = 7 (0101 | 0011 = 0111)
        let result = BitwiseOps::or(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(7));
    }

    #[test]
    fn test_bitwise_and() {
        // 5 & 3 = 1 (0101 & 0011 = 0001)
        let result = BitwiseOps::and(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }

    #[test]
    fn test_bitwise_not() {
        // ~0 = -1 (all bits flipped)
        let result = BitwiseOps::not(&SqlValue::Integer(0)).unwrap();
        assert_eq!(result, SqlValue::Integer(-1));

        // ~-1 = 0
        let result = BitwiseOps::not(&SqlValue::Integer(-1)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));

        // ~1 = -2
        let result = BitwiseOps::not(&SqlValue::Integer(1)).unwrap();
        assert_eq!(result, SqlValue::Integer(-2));
    }

    #[test]
    fn test_left_shift() {
        // 1 << 4 = 16
        let result = BitwiseOps::left_shift(&SqlValue::Integer(1), &SqlValue::Integer(4)).unwrap();
        assert_eq!(result, SqlValue::Integer(16));

        // 5 << 2 = 20
        let result = BitwiseOps::left_shift(&SqlValue::Integer(5), &SqlValue::Integer(2)).unwrap();
        assert_eq!(result, SqlValue::Integer(20));
    }

    #[test]
    fn test_right_shift() {
        // 16 >> 4 = 1
        let result = BitwiseOps::right_shift(&SqlValue::Integer(16), &SqlValue::Integer(4)).unwrap();
        assert_eq!(result, SqlValue::Integer(1));

        // 20 >> 2 = 5
        let result = BitwiseOps::right_shift(&SqlValue::Integer(20), &SqlValue::Integer(2)).unwrap();
        assert_eq!(result, SqlValue::Integer(5));
    }

    #[test]
    fn test_negative_shift() {
        // Left shift by negative = right shift
        // 16 << -2 = 16 >> 2 = 4
        let result = BitwiseOps::left_shift(&SqlValue::Integer(16), &SqlValue::Integer(-2)).unwrap();
        assert_eq!(result, SqlValue::Integer(4));

        // Right shift by negative = left shift
        // 4 >> -2 = 4 << 2 = 16
        let result = BitwiseOps::right_shift(&SqlValue::Integer(4), &SqlValue::Integer(-2)).unwrap();
        assert_eq!(result, SqlValue::Integer(16));
    }

    #[test]
    fn test_large_shift() {
        // Shift by >= 64 bits
        let result = BitwiseOps::left_shift(&SqlValue::Integer(1), &SqlValue::Integer(64)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));

        let result = BitwiseOps::right_shift(&SqlValue::Integer(1), &SqlValue::Integer(64)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));

        // Negative number right shift by large amount
        let result =
            BitwiseOps::right_shift(&SqlValue::Integer(-1), &SqlValue::Integer(64)).unwrap();
        assert_eq!(result, SqlValue::Integer(-1));
    }

    #[test]
    fn test_mixed_types() {
        // Smallint | Integer
        let result = BitwiseOps::or(&SqlValue::Smallint(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(7));

        // Bigint & Integer
        let result = BitwiseOps::and(&SqlValue::Bigint(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }
}
