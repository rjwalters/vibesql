//! Caching strategies for expression evaluation
//!
//! This module provides caching mechanisms for:
//! - CSE (Common Subexpression Elimination)
//! - Subquery results
//!
//! Caches use LRU eviction and can be configured via environment variables.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use lru::LruCache;

/// Default maximum size for CSE cache (entries)
/// Can be overridden via CSE_CACHE_SIZE environment variable
pub(super) const DEFAULT_CSE_CACHE_SIZE: usize = 1000;

/// Default maximum size for subquery result cache (entries)
/// Increased from 100 to 5000 to handle complex test files with 400+ unique subqueries
/// (e.g., index/between/1000/slt_good_0.test has 401 subqueries)
/// Can be overridden via SUBQUERY_CACHE_SIZE environment variable
pub(super) const DEFAULT_SUBQUERY_CACHE_SIZE: usize = 5000;

/// Check if CSE is enabled via environment variable
/// Defaults to true, can be disabled by setting CSE_ENABLED=false
pub(super) fn is_cse_enabled() -> bool {
    std::env::var("CSE_ENABLED")
        .map(|v| v.to_lowercase() != "false" && v != "0")
        .unwrap_or(true) // Default: enabled
}

/// Get CSE cache size from environment variable
/// Defaults to DEFAULT_CSE_CACHE_SIZE, can be overridden by setting CSE_CACHE_SIZE
pub(super) fn get_cse_cache_size() -> usize {
    std::env::var("CSE_CACHE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CSE_CACHE_SIZE)
}

/// Get subquery cache size from environment variable
/// Defaults to DEFAULT_SUBQUERY_CACHE_SIZE, can be overridden by setting SUBQUERY_CACHE_SIZE
pub(super) fn get_subquery_cache_size() -> usize {
    std::env::var("SUBQUERY_CACHE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SUBQUERY_CACHE_SIZE)
}

/// Create a new CSE cache with configured size
pub(super) fn create_cse_cache() -> LruCache<u64, vibesql_types::SqlValue> {
    LruCache::new(NonZeroUsize::new(get_cse_cache_size()).unwrap())
}

/// Create a new subquery result cache with configured size
pub(super) fn create_subquery_cache() -> LruCache<u64, Vec<vibesql_storage::Row>> {
    LruCache::new(NonZeroUsize::new(get_subquery_cache_size()).unwrap())
}

/// Compute a hash for a subquery to use as a cache key
///
/// Uses Debug format for hashing which is sufficient for typical queries.
/// See combined/subqueries/cache.rs for detailed documentation on trade-offs.
pub(super) fn compute_subquery_hash(subquery: &vibesql_ast::SelectStmt) -> u64 {
    let mut hasher = DefaultHasher::new();
    format!("{:?}", subquery).hash(&mut hasher);
    hasher.finish()
}
