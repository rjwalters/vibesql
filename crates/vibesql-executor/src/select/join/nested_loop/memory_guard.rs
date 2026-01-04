//! Memory limit checking for nested loop joins.
//!
//! This module provides utilities for checking whether join operations
//! would exceed memory limits before execution.

use crate::{errors::ExecutorError, limits::MAX_MEMORY_BYTES};

/// Maximum number of rows allowed in a join result to prevent memory exhaustion
/// With average row size of ~100 bytes, this allows up to ~10GB
pub const MAX_JOIN_RESULT_ROWS: usize = 100_000_000;

/// Check if a CROSS JOIN would exceed memory limits
/// Only used for true CROSS JOINs (no join condition)
pub fn check_cross_join_size_limit(
    left_count: usize,
    right_count: usize,
) -> Result<(), ExecutorError> {
    // CROSS JOIN creates Cartesian product
    let estimated_result_rows = left_count.saturating_mul(right_count);

    if estimated_result_rows > MAX_JOIN_RESULT_ROWS {
        // Estimate memory usage (conservative: 100 bytes per row average)
        let estimated_bytes = estimated_result_rows.saturating_mul(100);
        return Err(ExecutorError::MemoryLimitExceeded {
            used_bytes: estimated_bytes,
            max_bytes: MAX_MEMORY_BYTES,
        });
    }

    Ok(())
}
