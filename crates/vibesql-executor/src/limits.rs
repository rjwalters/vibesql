//! Execution limits and safeguards
//!
//! This module defines limits to prevent infinite loops, stack overflow, and runaway queries.
//! These limits follow industry best practices established by SQLite and other production
//! databases.
//!
//! ## Design Philosophy
//!
//! These limits serve multiple purposes:
//! 1. **Safety**: Prevent crashes from stack overflow or infinite recursion
//! 2. **Performance**: Kill runaway queries that would hang the system
//! 3. **Debugging**: Provide clear error messages when limits are exceeded
//! 4. **Compatibility**: Match SQLite's limit philosophy for consistency
//!
//! ## Limit Values
//!
//! Default values are chosen conservatively to allow legitimate queries while catching
//! pathological cases. They can be adjusted based on real-world usage patterns.

/// Maximum depth of expression tree evaluation (subqueries, nested expressions)
///
/// SQLite uses 1000 for SQLITE_MAX_EXPR_DEPTH
/// We use a more conservative 200 to prevent stack overflow
///
/// This limit is based on Rust's default stack size (typically 2MB on most platforms).
/// Testing shows that expression depths beyond ~200 can cause stack overflow during
/// evaluation even with depth tracking, as each recursive call consumes stack space.
///
/// This prevents stack overflow from deeply nested:
/// - Subqueries (IN, EXISTS, scalar subqueries)
/// - Arithmetic expressions
/// - Function calls
/// - Boolean logic (AND/OR chains)
pub const MAX_EXPRESSION_DEPTH: usize = 200;

/// Maximum number of compound SELECT terms
///
/// SQLite uses 500 for SQLITE_MAX_COMPOUND_SELECT
/// We match this value
///
/// This prevents stack overflow from deeply nested UNION/INTERSECT/EXCEPT chains
pub const MAX_COMPOUND_SELECT: usize = 500;

/// Maximum number of rows to process in a single query execution
///
/// This prevents infinite loops in:
/// - WHERE clause evaluation
/// - Join operations
/// - Aggregate processing
///
/// Set high enough for legitimate large queries (10 million rows)
pub const MAX_ROWS_PROCESSED: usize = 10_000_000;

/// Maximum execution time for a single query (in seconds)
///
/// This is a soft timeout - checked periodically, not enforced precisely
///
/// Set to 300 seconds (5 minutes) to allow complex multi-table joins to complete.
/// With predicate pushdown optimization (#1122), memory usage is under control (6.48 GB
/// vs 73+ GB before). Join reordering optimization is now enabled for 3-8 table joins,
/// which should significantly reduce execution time for complex analytical queries.
///
/// If join reordering proves effective, consider reducing this timeout to a lower value
/// (e.g., 60-120 seconds) to catch runaway queries more quickly.
pub const MAX_QUERY_EXECUTION_SECONDS: u64 = 300;

/// Maximum number of iterations in a single loop (e.g., WHERE filtering)
///
/// This catches infinite loops in iteration logic
/// Should be higher than MAX_ROWS_PROCESSED to avoid false positives
pub const MAX_LOOP_ITERATIONS: usize = 50_000_000;

/// Maximum memory usage per query execution
///
/// This prevents:
/// - Cartesian product explosions
/// - Exponential intermediate result growth
/// - System memory exhaustion
/// - Swapping/OOM kills
///
/// Large enough for legitimate queries, small enough to catch pathological cases
///
/// WASM targets have more constrained memory (typically 2-4 GB addressable),
/// so we use smaller limits there to avoid overflow
#[cfg(target_family = "wasm")]
pub const MAX_MEMORY_BYTES: usize = 1 * 1024 * 1024 * 1024; // 1 GB for WASM

#[cfg(not(target_family = "wasm"))]
pub const MAX_MEMORY_BYTES: usize = 10 * 1024 * 1024 * 1024; // 10 GB for native

/// Warning threshold - log when memory exceeds this
#[cfg(target_family = "wasm")]
pub const MEMORY_WARNING_BYTES: usize = 512 * 1024 * 1024; // 512 MB for WASM

#[cfg(not(target_family = "wasm"))]
pub const MEMORY_WARNING_BYTES: usize = 5 * 1024 * 1024 * 1024; // 5 GB for native

#[cfg(test)]
#[allow(clippy::assertions_on_constants)]
mod tests {
    use super::*;

    #[test]
    fn test_limits_are_reasonable() {
        // Expression depth should handle realistic queries
        assert!(MAX_EXPRESSION_DEPTH >= 100, "Expression depth too low");
        assert!(MAX_EXPRESSION_DEPTH <= 10000, "Expression depth too high");

        // Row limits should handle large but not infinite datasets
        assert!(MAX_ROWS_PROCESSED >= 1_000_000, "Row limit too low");
        assert!(MAX_ROWS_PROCESSED <= 1_000_000_000, "Row limit too high");

        // Query timeout should allow complex queries but catch hangs
        assert!(MAX_QUERY_EXECUTION_SECONDS >= 10, "Timeout too short");
        assert!(MAX_QUERY_EXECUTION_SECONDS <= 3600, "Timeout too long");
    }

    #[test]
    fn test_limits_relationship() {
        // Loop iterations should be higher than row processing limit
        assert!(
            MAX_LOOP_ITERATIONS > MAX_ROWS_PROCESSED,
            "Loop iterations should exceed row processing limit to avoid false positives"
        );
    }

    #[test]
    fn test_memory_limits_reasonable() {
        // Memory warning should be lower than hard limit
        assert!(
            MEMORY_WARNING_BYTES < MAX_MEMORY_BYTES,
            "Warning threshold should be less than hard limit"
        );

        // Memory limit should be substantial but not unlimited
        assert!(MAX_MEMORY_BYTES >= 1_000_000_000, "Memory limit too low (< 1 GB)");
        assert!(MAX_MEMORY_BYTES <= 100_000_000_000, "Memory limit too high (> 100 GB)");
    }
}
