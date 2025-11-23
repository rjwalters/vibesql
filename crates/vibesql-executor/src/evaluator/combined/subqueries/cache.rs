//! Subquery caching utilities
//!
//! This module provides cache key computation for both uncorrelated and
//! correlated subqueries to enable efficient result caching.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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
pub(super) fn compute_subquery_hash(subquery: &vibesql_ast::SelectStmt) -> u64 {
    let mut hasher = DefaultHasher::new();
    // Use the debug format as a stable representation
    // This works because SelectStmt derives Debug and PartialEq
    format!("{:?}", subquery).hash(&mut hasher);
    hasher.finish()
}

/// Compute a composite cache key for a correlated subquery
///
/// The cache key combines:
/// 1. The subquery hash (AST structure)
/// 2. The correlation values (column values from outer row)
///
/// This allows caching correlated subquery results when the correlation
/// values are the same across different rows.
pub(super) fn compute_correlated_cache_key(
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
