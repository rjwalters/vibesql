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
    /// ```text
    /// if let Some(columnar) = db.get_columnar("lineitem")? {
    ///     // Use columnar data for SIMD operations
    /// }
    /// ```
    pub fn get_columnar(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<crate::ColumnarTable>>, StorageError> {
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
    /// ```text
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
    /// ```text
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    fn create_test_table_schema(name: &str) -> TableSchema {
        TableSchema::new(
            name.to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    true,
                ),
            ],
        )
    }

    fn create_test_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("name_{}", i))),
                ])
            })
            .collect()
    }

    #[test]
    fn test_pre_warm_columnar_cache_with_valid_tables() {
        let mut db = Database::new();

        // Create test tables
        db.create_table(create_test_table_schema("table1")).unwrap();
        db.create_table(create_test_table_schema("table2")).unwrap();

        // Insert some rows
        for row in create_test_rows(10) {
            db.insert_row("table1", row).unwrap();
        }
        for row in create_test_rows(5) {
            db.insert_row("table2", row).unwrap();
        }

        // Pre-warm specific tables
        let count = db.pre_warm_columnar_cache(&["table1", "table2"]).unwrap();
        assert_eq!(count, 2, "Should have pre-warmed 2 tables");

        // Verify stats show conversions occurred
        let stats = db.columnar_cache_stats();
        assert_eq!(stats.conversions, 2, "Should have converted 2 tables");
    }

    #[test]
    fn test_pre_warm_columnar_cache_nonexistent_table() {
        let db = Database::new();

        // Pre-warm with nonexistent tables
        let count = db.pre_warm_columnar_cache(&["nonexistent1", "nonexistent2"]).unwrap();
        assert_eq!(count, 0, "Should return 0 for nonexistent tables");

        // Verify no conversions occurred
        let stats = db.columnar_cache_stats();
        assert_eq!(stats.conversions, 0, "Should have 0 conversions for nonexistent tables");
    }

    #[test]
    fn test_pre_warm_columnar_cache_mixed_tables() {
        let mut db = Database::new();

        // Create only one table
        db.create_table(create_test_table_schema("exists")).unwrap();
        for row in create_test_rows(5) {
            db.insert_row("exists", row).unwrap();
        }

        // Pre-warm with mix of existing and nonexistent tables
        let count = db.pre_warm_columnar_cache(&["exists", "nonexistent"]).unwrap();
        assert_eq!(count, 1, "Should have pre-warmed only 1 existing table");
    }

    #[test]
    fn test_pre_warm_all_columnar() {
        let mut db = Database::new();

        // Create multiple test tables
        db.create_table(create_test_table_schema("table_a")).unwrap();
        db.create_table(create_test_table_schema("table_b")).unwrap();
        db.create_table(create_test_table_schema("table_c")).unwrap();

        // Insert some rows
        for row in create_test_rows(5) {
            db.insert_row("table_a", row).unwrap();
        }
        for row in create_test_rows(3) {
            db.insert_row("table_b", row).unwrap();
        }
        for row in create_test_rows(7) {
            db.insert_row("table_c", row).unwrap();
        }

        // Pre-warm all tables
        let count = db.pre_warm_all_columnar().unwrap();
        assert_eq!(count, 3, "Should have pre-warmed all 3 tables");

        // Verify stats
        let stats = db.columnar_cache_stats();
        assert_eq!(stats.conversions, 3, "Should have converted all 3 tables");
    }

    #[test]
    fn test_pre_warm_results_in_cache_hits() {
        let mut db = Database::new();

        // Create and populate a table
        db.create_table(create_test_table_schema("cached_table")).unwrap();
        for row in create_test_rows(10) {
            db.insert_row("cached_table", row).unwrap();
        }

        // Pre-warm the cache
        let count = db.pre_warm_columnar_cache(&["cached_table"]).unwrap();
        assert_eq!(count, 1);

        // Record stats after pre-warming
        let stats_before = db.columnar_cache_stats();
        let hits_before = stats_before.hits;

        // Access the columnar data again - should be a cache hit
        let _ = db.get_columnar("cached_table").unwrap();

        // Verify cache hit occurred
        let stats_after = db.columnar_cache_stats();
        assert_eq!(
            stats_after.hits,
            hits_before + 1,
            "Should have one more cache hit after accessing pre-warmed table"
        );
        assert_eq!(
            stats_after.conversions, stats_before.conversions,
            "Should not have additional conversions"
        );
    }

    #[test]
    fn test_pre_warm_empty_table_list() {
        let db = Database::new();

        // Pre-warm with empty list
        let count = db.pre_warm_columnar_cache(&[]).unwrap();
        assert_eq!(count, 0, "Should return 0 for empty table list");
    }

    #[test]
    fn test_pre_warm_all_empty_database() {
        let db = Database::new();

        // Pre-warm all on empty database
        let count = db.pre_warm_all_columnar().unwrap();
        assert_eq!(count, 0, "Should return 0 for empty database");
    }

    #[test]
    fn test_pre_warm_idempotent() {
        let mut db = Database::new();

        // Create and populate a table
        db.create_table(create_test_table_schema("test_table")).unwrap();
        for row in create_test_rows(5) {
            db.insert_row("test_table", row).unwrap();
        }

        // Pre-warm twice
        let count1 = db.pre_warm_columnar_cache(&["test_table"]).unwrap();
        let stats1 = db.columnar_cache_stats();

        let count2 = db.pre_warm_columnar_cache(&["test_table"]).unwrap();
        let stats2 = db.columnar_cache_stats();

        // Both should report success
        assert_eq!(count1, 1);
        assert_eq!(count2, 1);

        // But only one conversion should have occurred (second should be cache hit)
        assert_eq!(stats1.conversions, 1);
        assert_eq!(stats2.conversions, 1, "Second pre-warm should not cause additional conversion");
        assert_eq!(stats2.hits, stats1.hits + 1, "Second pre-warm should result in cache hit");
    }
}
