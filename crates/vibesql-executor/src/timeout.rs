//! Timeout context for query execution
//!
//! This module provides a lightweight timeout checking mechanism that can be
//! passed to query execution functions (joins, scans, etc.) without requiring
//! the full `SelectExecutor` reference.
//!
//! ## Design Rationale
//!
//! The `SelectExecutor` owns the timeout configuration but many query execution
//! functions (especially joins) don't have access to it. Rather than threading
//! the executor through all call chains, we extract just the timeout-related
//! state into this lightweight struct.
//!
//! ## Usage Pattern
//!
//! ```text
//! // Create from executor
//! let timeout_ctx = TimeoutContext::from_executor(executor);
//!
//! // Pass to join/scan functions
//! nested_loop_join(left, right, condition, database, &timeout_ctx)?;
//!
//! // Check periodically in hot loops
//! for row in rows {
//!     if iteration % CHECK_INTERVAL == 0 {
//!         timeout_ctx.check()?;
//!     }
//!     // ... process row ...
//! }
//! ```

use instant::Instant;

use crate::errors::ExecutorError;
use crate::limits::MAX_QUERY_EXECUTION_SECONDS;

/// Recommended interval for timeout checks in hot loops.
/// Checking every 1000 iterations balances responsiveness with overhead.
pub const CHECK_INTERVAL: usize = 1000;

/// Lightweight timeout context for query execution.
///
/// This struct captures the timeout configuration from `SelectExecutor` and can
/// be passed to functions that need to check for timeouts but don't need the
/// full executor reference.
#[derive(Clone, Copy)]
pub struct TimeoutContext {
    /// When query execution started
    start_time: Instant,
    /// Maximum execution time in seconds
    timeout_seconds: u64,
}

impl TimeoutContext {
    /// Create a new timeout context with the given start time and timeout.
    pub fn new(start_time: Instant, timeout_seconds: u64) -> Self {
        Self { start_time, timeout_seconds }
    }

    /// Create a timeout context from a `SelectExecutor`.
    pub fn from_executor(executor: &crate::SelectExecutor<'_>) -> Self {
        Self { start_time: executor.start_time, timeout_seconds: executor.timeout_seconds }
    }

    /// Create a default timeout context (for use when no executor is available).
    ///
    /// This creates a context with the current time as start and default timeout.
    /// Use sparingly - prefer `from_executor` when an executor is available.
    pub fn new_default() -> Self {
        Self { start_time: Instant::now(), timeout_seconds: MAX_QUERY_EXECUTION_SECONDS }
    }

    /// Check if the timeout has been exceeded.
    ///
    /// Returns `Ok(())` if within timeout, `Err(QueryTimeoutExceeded)` if exceeded.
    ///
    /// Call this periodically in hot loops (every `CHECK_INTERVAL` iterations).
    #[inline]
    pub fn check(&self) -> Result<(), ExecutorError> {
        let elapsed = self.start_time.elapsed().as_secs();
        if elapsed >= self.timeout_seconds {
            return Err(ExecutorError::QueryTimeoutExceeded {
                elapsed_seconds: elapsed,
                max_seconds: self.timeout_seconds,
            });
        }
        Ok(())
    }

    /// Get the start time for this timeout context.
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// Get the timeout in seconds for this timeout context.
    pub fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }
}

impl Default for TimeoutContext {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_timeout_check_passes_within_limit() {
        let ctx = TimeoutContext::new(Instant::now(), 60);
        assert!(ctx.check().is_ok());
    }

    #[test]
    fn test_timeout_check_fails_after_timeout() {
        // Use a very short timeout
        let ctx = TimeoutContext::new(Instant::now(), 0);
        // Sleep briefly to ensure we're past the 0-second timeout
        sleep(Duration::from_millis(10));
        let result = ctx.check();
        assert!(result.is_err());
        if let Err(ExecutorError::QueryTimeoutExceeded { .. }) = result {
            // Expected
        } else {
            panic!("Expected QueryTimeoutExceeded error");
        }
    }

    #[test]
    fn test_default_timeout_context() {
        let ctx = TimeoutContext::default();
        assert_eq!(ctx.timeout_seconds(), MAX_QUERY_EXECUTION_SECONDS);
        assert!(ctx.check().is_ok());
    }
}
