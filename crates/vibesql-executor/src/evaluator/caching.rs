//! Caching strategies for expression evaluation
//!
//! This module provides caching mechanisms for:
//! - CSE (Common Subexpression Elimination)
//! - Subquery results
//!
//! Caches use LRU eviction and can be configured via environment variables.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
};

use lru::LruCache;

/// Default maximum size for CSE cache (entries)
/// Can be overridden via CSE_CACHE_SIZE environment variable
pub(crate) const DEFAULT_CSE_CACHE_SIZE: usize = 1000;

/// Default maximum size for subquery result cache (entries)
/// Increased from 100 to 5000 to handle complex test files with 400+ unique subqueries
/// (e.g., index/between/1000/slt_good_0.test has 401 subqueries)
/// Can be overridden via SUBQUERY_CACHE_SIZE environment variable
pub(crate) const DEFAULT_SUBQUERY_CACHE_SIZE: usize = 5000;

/// Check if CSE is enabled via environment variable
/// Defaults to true, can be disabled by setting CSE_ENABLED=false
pub(crate) fn is_cse_enabled() -> bool {
    std::env::var("CSE_ENABLED").map(|v| v.to_lowercase() != "false" && v != "0").unwrap_or(true)
    // Default: enabled
}

/// Get CSE cache size from environment variable
/// Defaults to DEFAULT_CSE_CACHE_SIZE, can be overridden by setting CSE_CACHE_SIZE
pub(crate) fn get_cse_cache_size() -> usize {
    std::env::var("CSE_CACHE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CSE_CACHE_SIZE)
}

/// Get subquery cache size from environment variable
/// Defaults to DEFAULT_SUBQUERY_CACHE_SIZE, can be overridden by setting SUBQUERY_CACHE_SIZE
pub(crate) fn get_subquery_cache_size() -> usize {
    std::env::var("SUBQUERY_CACHE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SUBQUERY_CACHE_SIZE)
}

/// Create a new CSE cache with configured size
pub(crate) fn create_cse_cache() -> LruCache<u64, vibesql_types::SqlValue> {
    LruCache::new(NonZeroUsize::new(get_cse_cache_size()).unwrap())
}

/// Create a new subquery result cache with configured size
pub(crate) fn create_subquery_cache() -> LruCache<u64, Vec<vibesql_storage::Row>> {
    LruCache::new(NonZeroUsize::new(get_subquery_cache_size()).unwrap())
}

// NOTE: Pointer-based hash caching was removed because it caused correctness bugs.
// See compute_subquery_hash() for details.

/// Compute a hash for a subquery to use as a cache key
///
/// # Implementation Note
///
/// Currently uses Debug format for hashing, which has trade-offs:
///
/// **Pros:**
/// - Simple and works with existing AST types
/// - Sufficient for typical queries in practice
/// - Hash collisions are rare
///
/// **Cons:**
/// - Fragile: Debug format could change with Rust versions
/// - Less efficient: Allocates string for each hash
/// - Not cryptographically secure (uses DefaultHasher)
///
/// **Future Improvement:**
/// Ideally, SelectStmt and child types should derive Hash for:
/// - Better performance (direct AST traversal)
/// - Stability (Hash trait is stable)
/// - Type safety (compiler-enforced consistency)
///
/// This requires adding Hash to ~15-20 AST types, which should be
/// done in a dedicated refactoring PR to minimize risk.
///
/// See: https://github.com/rjwalters/vibesql/issues/2137#hash-improvement
///
/// Compute hash directly without pointer-based caching.
///
/// While pointer-based caching can improve performance by avoiding repeated format!()
/// calls for the same AST within a query, it causes correctness bugs because:
/// 1. The thread-local cache persists across different query executions
/// 2. When memory is reused (same pointer for different AST nodes), stale hashes are returned
/// 3. This caused index/between tests to return wrong results
///
/// The performance cost of format!() is minimal compared to actual query execution.
/// For better performance, derive Hash for SelectStmt and related AST types.
pub(crate) fn compute_subquery_hash(subquery: &vibesql_ast::SelectStmt) -> u64 {
    let mut hasher = DefaultHasher::new();
    format!("{:?}", subquery).hash(&mut hasher);
    hasher.finish()
}

/// Clear the subquery hash cache (no-op).
///
/// Kept for API compatibility. Pointer-based hash caching has been removed
/// because it caused correctness bugs when memory was reused across queries.
#[inline]
pub fn clear_subquery_hash_cache() {
    // No-op: pointer-based caching removed
}

/// Compute a composite cache key for a correlated subquery
///
/// The cache key combines:
/// 1. The subquery hash (AST structure)
/// 2. The correlation values (column values from outer row)
///
/// This allows caching correlated subquery results when the correlation
/// values are the same across different rows.
pub(crate) fn compute_correlated_cache_key(
    subquery_hash: u64,
    correlation_values: &[(String, vibesql_types::SqlValue)],
) -> u64 {
    let mut hasher = DefaultHasher::new();
    subquery_hash.hash(&mut hasher);

    // Hash each correlation value in order
    for (name, value) in correlation_values {
        name.hash(&mut hasher);
        // Hash the value's Debug representation (consistent with subquery hashing)
        format!("{:?}", value).hash(&mut hasher);
    }

    hasher.finish()
}
