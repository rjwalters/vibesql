//! Math and query planner hint functions
//!
//! This module contains SQLite-compatible math functions and query hints:
//! - RANDOM() - Return pseudo-random 64-bit integer
//! - LIKELY(x) - Query planner hint (no-op)
//! - UNLIKELY(x) - Query planner hint (no-op)
//! - LIKELIHOOD(x, p) - Query planner hint with probability (no-op)

use rand::{Rng, RngExt};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// RANDOM() - Return a pseudo-random 64-bit signed integer
///
/// Returns a pseudo-random integer between -9223372036854775808 and +9223372036854775807.
/// The value changes on each call.
pub(crate) fn random(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RANDOM takes no arguments, got {}",
            args.len()
        )));
    }

    let value: i64 = rand::rng().random();
    Ok(SqlValue::Integer(value))
}

/// LIKELY(x) - Query planner hint that x is usually true
///
/// This is a no-op that returns its argument unchanged.
/// The hint is used by SQLite's query planner but has no effect on the result.
pub(crate) fn likely(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LIKELY requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    Ok(args[0].clone())
}

/// UNLIKELY(x) - Query planner hint that x is usually false
///
/// This is a no-op that returns its argument unchanged.
/// The hint is used by SQLite's query planner but has no effect on the result.
pub(crate) fn unlikely(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UNLIKELY requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    Ok(args[0].clone())
}

/// LIKELIHOOD(x, p) - Query planner hint with probability
///
/// This is a no-op that returns the first argument unchanged.
/// The second argument p is a probability between 0.0 and 1.0 (ignored).
pub(crate) fn likelihood(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LIKELIHOOD requires exactly 2 arguments, got {}",
            args.len()
        )));
    }
    // Just return the first argument, ignoring the probability hint
    Ok(args[0].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random() {
        // random() should return an integer
        let result = random(&[]).unwrap();
        assert!(matches!(result, SqlValue::Integer(_)));

        // Two calls should (almost certainly) return different values
        let result1 = random(&[]).unwrap();
        let result2 = random(&[]).unwrap();
        // This could theoretically fail but is astronomically unlikely
        assert_ne!(result1, result2);

        // Wrong number of arguments
        assert!(random(&[SqlValue::Integer(1)]).is_err());
    }

    #[test]
    fn test_likely_unlikely_likelihood() {
        let val = SqlValue::Boolean(true);
        assert_eq!(likely(std::slice::from_ref(&val)).unwrap(), val);
        assert_eq!(unlikely(std::slice::from_ref(&val)).unwrap(), val);
        assert_eq!(likelihood(&[val.clone(), SqlValue::Numeric(0.9)]).unwrap(), val);
    }
}
