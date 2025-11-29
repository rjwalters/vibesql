// ============================================================================
// Database Columnar Cache Integration
// ============================================================================

use super::core::Database;
use crate::columnar_cache::ColumnarCache;
use crate::StorageError;
use std::sync::Arc;

impl Database {
    // ============================================================================
    // Columnar Cache Methods
    // ============================================================================

    /// Get columnar representation of a table, using cache if available
    ///
    /// This method provides an Arc-wrapped columnar representation of the table,
    /// enabling zero-copy sharing between queries. The cache automatically manages
    /// memory via LRU eviction.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to get columnar representation for
    ///
    /// # Returns
    /// * `Ok(Some(Arc<ColumnarTable>))` - Cached or newly converted columnar data
    /// * `Ok(None)` - Table not found
    /// * `Err(StorageError)` - Conversion failed
    ///
    /// # Example
    /// ```rust,ignore
    /// if let Some(columnar) = db.get_columnar("lineitem")? {
    ///     // Use columnar data for SIMD operations
    /// }
    /// ```
    pub fn get_columnar(&self, table_name: &str) -> Result<Option<Arc<crate::ColumnarTable>>, StorageError> {
        // Check cache first
        if let Some(cached) = self.columnar_cache.get(table_name) {
            return Ok(Some(cached));
        }

        // Table not in cache - need to get table and convert
        let table = match self.get_table(table_name) {
            Some(t) => t,
            None => return Ok(None),
        };

        // Convert to columnar format
        let columnar = table.scan_columnar()?;

        // Insert into cache and return
        let cached = self.columnar_cache.insert(table_name, columnar);
        Ok(Some(cached))
    }

    /// Invalidate columnar cache entry for a table
    ///
    /// Called automatically when a table is modified (INSERT/UPDATE/DELETE)
    /// to ensure the cache doesn't serve stale data.
    pub fn invalidate_columnar_cache(&self, table_name: &str) {
        self.columnar_cache.invalidate(table_name);
    }

    /// Clear all columnar cache entries
    pub fn clear_columnar_cache(&self) {
        self.columnar_cache.clear();
    }

    /// Get columnar cache statistics
    ///
    /// Returns statistics about cache hits, misses, evictions, and conversions.
    /// Useful for monitoring cache effectiveness and tuning the cache budget.
    pub fn columnar_cache_stats(&self) -> crate::columnar_cache::CacheStats {
        self.columnar_cache.stats()
    }

    /// Get current columnar cache memory usage in bytes
    pub fn columnar_cache_memory_usage(&self) -> usize {
        self.columnar_cache.memory_usage()
    }

    /// Get columnar cache memory budget in bytes
    pub fn columnar_cache_budget(&self) -> usize {
        self.columnar_cache.max_memory()
    }

    /// Set the columnar cache memory budget
    ///
    /// Note: This creates a new cache, discarding all cached data.
    /// Call this before loading data for best results.
    pub fn set_columnar_cache_budget(&mut self, max_bytes: usize) {
        self.columnar_cache = Arc::new(ColumnarCache::new(max_bytes));
    }

    /// Pre-warm the columnar cache for specific tables
    ///
    /// This method eagerly converts row data to columnar format and populates
    /// the cache. Call this after data loading to avoid conversion overhead
    /// during query execution.
    ///
    /// # Arguments
    /// * `table_names` - Names of tables to pre-warm
    ///
    /// # Returns
    /// * `Ok(count)` - Number of tables successfully pre-warmed
    /// * `Err(StorageError)` - Conversion failed for a table
    ///
    /// # Example
    /// ```rust,ignore
    /// // After loading TPC-H data
    /// let warmed = db.pre_warm_columnar_cache(&["lineitem", "orders"])?;
    /// eprintln!("Pre-warmed {} tables", warmed);
    /// ```
    ///
    /// # Performance
    ///
    /// This method performs the row-to-columnar conversion once, eliminating
    /// the ~31% overhead that would otherwise occur on the first query.
    /// For a 600K row LINEITEM table, this saves ~40ms per query session.
    pub fn pre_warm_columnar_cache(&self, table_names: &[&str]) -> Result<usize, StorageError> {
        let mut count = 0;
        for table_name in table_names {
            // get_columnar will convert and cache if not already cached
            if self.get_columnar(table_name)?.is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Pre-warm the columnar cache for all tables in the database
    ///
    /// This method eagerly converts all tables to columnar format.
    /// Useful for benchmark scenarios where all tables will be queried.
    ///
    /// # Returns
    /// * `Ok(count)` - Number of tables successfully pre-warmed
    /// * `Err(StorageError)` - Conversion failed for a table
    ///
    /// # Example
    /// ```rust,ignore
    /// // After loading all benchmark data
    /// let warmed = db.pre_warm_all_columnar()?;
    /// eprintln!("Pre-warmed {} tables", warmed);
    /// ```
    pub fn pre_warm_all_columnar(&self) -> Result<usize, StorageError> {
        let table_names: Vec<String> = self.list_tables();
        let refs: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();
        self.pre_warm_columnar_cache(&refs)
    }
}
