//! Query result caching and invalidation.
//!
//! This module manages caching of query results to improve performance during testing.
//! It tracks cache hits/misses and handles table-level invalidation when data changes.
//!
//! Cache keys include the SQL mode (MySQL/SQLite) to ensure queries return correct
//! results when dialect switches occur during test execution.

use std::sync::Arc;
use vibesql_executor::cache::{QueryResultCache, QuerySignature};
use vibesql_types::SqlMode;

/// Cache manager for query results with invalidation tracking.
#[derive(Clone)]
pub struct CacheManager {
    pub enabled: bool,
    pub cache: Arc<QueryResultCache>,
    pub hits: usize,
    pub misses: usize,
}

impl CacheManager {
    /// Create a new cache manager.
    pub fn new(enabled: bool, cache_size: usize) -> Self {
        Self {
            enabled,
            cache: Arc::new(QueryResultCache::new(cache_size)),
            hits: 0,
            misses: 0,
        }
    }

    /// Record a cache hit.
    pub fn record_hit(&mut self) {
        self.hits += 1;
    }

    /// Record a cache miss.
    pub fn record_miss(&mut self) {
        self.misses += 1;
    }

    /// Get the query signature for a SQL string (dialect-agnostic, deprecated).
    #[allow(dead_code)]
    pub fn get_signature(&self, sql: &str) -> QuerySignature {
        QuerySignature::from_sql(sql)
    }

    /// Get the query signature for a SQL string with SqlMode included.
    ///
    /// This ensures queries in different dialects have different cache keys,
    /// preventing incorrect cached results when dialect switches occur.
    /// For example, `SELECT 5/2` returns 2.5 in MySQL mode but 2 in SQLite mode.
    pub fn get_signature_with_mode(&self, sql: &str, sql_mode: &SqlMode) -> QuerySignature {
        // Append mode marker to create a dialect-specific cache key
        let sql_with_mode = format!("{} /* __dialect:{} */", sql, sql_mode);
        QuerySignature::from_sql(&sql_with_mode)
    }

    /// Invalidate all cached queries that reference a specific table.
    pub fn invalidate_table(&self, table_name: &str) {
        if self.enabled {
            self.cache.invalidate_table(table_name);
        }
    }

    /// Print cache statistics to stderr.
    pub fn print_summary(&self) {
        if !self.enabled {
            return;
        }

        let total_requests = self.hits + self.misses;
        if total_requests == 0 {
            return;
        }

        let hit_rate = (self.hits as f64 / total_requests as f64) * 100.0;
        eprintln!("📊 Query Result Cache Summary:");
        eprintln!("  Cache hits: {}", self.hits);
        eprintln!("  Cache misses: {}", self.misses);
        eprintln!("  Hit rate: {:.2}%", hit_rate);
        eprintln!(
            "  Cache size: {} / {} entries",
            self.cache.stats().size,
            self.cache.max_size()
        );
    }
}
