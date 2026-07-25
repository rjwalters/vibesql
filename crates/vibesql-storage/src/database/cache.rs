// ============================================================================
// Database Columnar Cache Integration
// ============================================================================

use std::sync::Arc;

use super::core::Database;
use crate::{columnar_cache::ColumnarCache, StorageError};

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
        // For native columnar tables, return data directly from the table
        // (no cache needed -- data is always hot and maintained incrementally)
        if let Some(table) = self.get_table(table_name) {
            if table.is_native_columnar() {
                let columnar = table.scan_columnar()?;
                return Ok(Some(Arc::new(columnar)));
            }
        }

        // #6199 Phase 0: a configured `columnar_cache_budget` of 0 disables the
        // representation cache entirely. For row-oriented tables there is then
        // no cached columnar form, so every consumer of `get_columnar` (the
        // SIMD scan filter, analytical columnar execution, and columnar joins)
        // receives `None` and falls back to the row path — matching
        // `VIBESQL_DISABLE_COLUMNAR=1` parity. Native columnar tables are
        // handled above and are unaffected (their data is table-resident, not
        // cache-managed).
        if self.columnar_cache.max_memory() == 0 {
            return Ok(None);
        }

        // #6199 Phase 2: structural hotness for this table, used to protect hot
        // analytical tables from eviction by colder newcomers. Derived from the
        // per-table access signal (never wall-clock).
        let hotness =
            super::columnar_policy::columnar_hotness(self.table_access_signal(table_name));

        // Check cache first (for row-oriented tables)
        if let Some(cached) = self.columnar_cache.get(table_name) {
            // Refresh the resident entry's eviction priority so it tracks the
            // table's evolving access pattern rather than the hotness captured
            // at first insert.
            self.columnar_cache.update_hotness(table_name, hotness);
            return Ok(Some(cached));
        }

        // Table not in cache - need to get table and convert
        let table = match self.get_table(table_name) {
            Some(t) => t,
            None => return Ok(None),
        };

        // Convert to columnar format
        let columnar = table.scan_columnar()?;

        // Insert into cache (hotness-aware eviction) and return
        let cached = self.columnar_cache.insert_with_hotness(table_name, columnar, hotness);
        Ok(Some(cached))
    }

    /// Structural (non-timing) decision — should a row-oriented table of
    /// `row_count` rows use its columnar representation for an analytical scan?
    ///
    /// #6199 Phase 2: this replaces the executor's former hardcoded
    /// `SIMD_COLUMNAR_THRESHOLD = 500` row-count-only gate. The decision is
    /// driven purely by structural signals — the row count and the per-table
    /// access-pattern signal (scan / point-lookup / write mix) — never by
    /// wall-clock timing. Point-lookup-dominated and write-thrashed tables stay
    /// on the row path even when large; hot analytical tables convert. See
    /// [`crate::database::should_use_columnar`] for the exact policy.
    ///
    /// Native `STORAGE COLUMNAR` tables are authoritative and are handled by
    /// their own always-resident path; this gate governs only the transparent
    /// row-table representation cache.
    pub fn should_use_columnar(&self, table_name: &str, row_count: usize) -> bool {
        super::columnar_policy::should_use_columnar(row_count, self.table_access_signal(table_name))
    }

    /// Invalidate columnar cache entry for a table
    ///
    /// Called automatically when a table is modified (INSERT/UPDATE/DELETE)
    /// to ensure the cache doesn't serve stale data.
    ///
    /// For native columnar tables, this is a no-op since the columnar data
    /// is maintained incrementally by the Table itself during DML operations.
    pub fn invalidate_columnar_cache(&self, table_name: &str) {
        // Phase 1 of #6199: this is the universal DML funnel (every
        // INSERT/UPDATE/DELETE/TRUNCATE/ALTER routes through it), so record the
        // write here — BEFORE the native-columnar early return below — so
        // native-columnar tables are counted too.
        self.record_write(table_name);

        // For native columnar tables, skip invalidation -- they maintain
        // their own columnar data incrementally during INSERT/UPDATE/DELETE
        if let Some(table) = self.get_table(table_name) {
            if table.is_native_columnar() {
                return;
            }
        }
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
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::Row;

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

    // ========================================================================
    // #6199 Phase 0 — user-configurable columnar cache budget behavior
    // ========================================================================

    /// A `columnar_cache_budget` of 0 disables the representation cache: for a
    /// row-oriented table, `get_columnar` returns `None` so every consumer
    /// falls back to the row path, and the cache never becomes resident
    /// (`CacheStats` stays all-zero, `memory_usage()` stays 0). The row data is
    /// still fully available via the row path — parity with the enabled cache.
    #[test]
    fn test_columnar_cache_budget_zero_disables_and_forces_row_path() {
        let mut config = crate::DatabaseConfig::server_default();
        config.columnar_cache_budget = 0;
        let mut db = Database::with_config(config);

        db.create_table(create_test_table_schema("t")).unwrap();
        for row in create_test_rows(100) {
            db.insert_row("t", row).unwrap();
        }

        // The representation cache is disabled: a row-oriented table yields no
        // columnar form, so consumers take the row path.
        assert_eq!(db.columnar_cache_budget(), 0, "budget should be reported as 0 (disabled)");
        assert!(
            db.get_columnar("t").unwrap().is_none(),
            "disabled cache must return None for a row-oriented table (row path)"
        );

        // Repeated access must never make the cache resident or accrue stats.
        let _ = db.get_columnar("t").unwrap();
        let stats = db.columnar_cache_stats();
        assert_eq!(stats.conversions, 0, "disabled cache must perform no conversions");
        assert_eq!(stats.hits, 0, "disabled cache must record no hits");
        assert_eq!(stats.evictions, 0, "disabled cache must record no evictions");
        assert_eq!(db.columnar_cache_memory_usage(), 0, "disabled cache must stay at 0 bytes");

        // Parity: the same rows are still fully available via the row path.
        let rows = db.get_table("t").expect("table exists").scan_live_vec();
        assert_eq!(rows.len(), 100, "row path must expose all rows when cache is disabled");

        // Contrast: an enabled cache DOES produce a columnar form for the same
        // data, proving the knob (not some unrelated condition) gates behavior.
        let mut enabled = Database::with_config(crate::DatabaseConfig::server_default());
        enabled.create_table(create_test_table_schema("t")).unwrap();
        for row in create_test_rows(100) {
            enabled.insert_row("t", row).unwrap();
        }
        assert!(
            enabled.get_columnar("t").unwrap().is_some(),
            "an enabled cache must produce a columnar representation"
        );
        assert!(
            enabled.columnar_cache_stats().conversions > 0,
            "an enabled cache must record at least one conversion"
        );
    }

    // ========================================================================
    // #6199 Phase 2 — adaptive dispatch + hotness-aware eviction
    // ========================================================================

    /// Warm `count` identical 100-row tables through `get_columnar` on a fresh
    /// generous-budget database and return the cached size of one table, so a
    /// test can pick a budget that holds exactly one table.
    fn one_table_cached_size() -> usize {
        let mut db = Database::new(); // default (256MB) budget — nothing evicts
        db.create_table(create_test_table_schema("probe")).unwrap();
        for row in create_test_rows(100) {
            db.insert_row("probe", row).unwrap();
        }
        db.get_columnar("probe").unwrap();
        let sz = db.columnar_cache_memory_usage();
        assert!(sz > 0, "probe table should occupy non-zero cache memory");
        sz
    }

    /// Hotness-aware eviction (structural, never wall-clock): an analytically
    /// hot table stays resident under cache pressure while a colder,
    /// point-lookup-dominated newcomer is admitted without displacing it.
    ///
    /// With pure LRU (the pre-Phase-2 behavior) the newcomer would evict the
    /// least-recently-used entry — the hot table — and the subsequent access to
    /// the hot table would re-convert it. Here the hot table must instead be a
    /// cache hit with no re-conversion, proving eviction is ordered by hotness.
    #[test]
    fn test_hot_analytical_table_survives_cache_pressure() {
        // Budget holds exactly one table, so admitting a second forces the
        // eviction path to run (and, here, to protect the hotter resident).
        let mut config = crate::DatabaseConfig::server_default();
        config.columnar_cache_budget = one_table_cached_size();
        let mut db = Database::with_config(config);

        for name in ["hot", "cold"] {
            db.create_table(create_test_table_schema(name)).unwrap();
            for row in create_test_rows(100) {
                db.insert_row(name, row).unwrap();
            }
        }

        // Structural access signal: `hot` is scan-dominated, `cold` is
        // point-lookup-dominated. (Recorded directly — the executor records the
        // same counters end-to-end; here we drive them to isolate the policy.)
        for _ in 0..50 {
            db.record_scan("hot");
        }
        for _ in 0..50 {
            db.record_point_lookup("cold");
        }

        // Warm `hot` into the cache (one conversion, now resident).
        db.get_columnar("hot").unwrap();

        // Admit the cold newcomer. It is colder than the resident hot table, so
        // hotness-aware eviction must NOT displace `hot` (it is admitted
        // over-budget instead). This converts `cold`.
        db.get_columnar("cold").unwrap();

        // `hot` must still be resident: accessing it is a cache hit with no
        // re-conversion. Under pure LRU it would have been evicted by the cold
        // insert and re-converted here.
        let before = db.columnar_cache_stats();
        db.get_columnar("hot").unwrap();
        let after = db.columnar_cache_stats();
        assert_eq!(
            after.conversions, before.conversions,
            "hot analytical table must stay resident (no re-conversion) under cache pressure"
        );
        assert_eq!(
            after.hits,
            before.hits + 1,
            "accessing the still-resident hot table must be a cache hit"
        );
    }

    /// A small non-zero budget forces LRU eviction once cached tables exceed the
    /// budget: `CacheStats.evictions` must be > 0 after populating several
    /// tables through `get_columnar`.
    #[test]
    fn test_small_columnar_cache_budget_forces_eviction() {
        // Tiny budget so a second resident table evicts the first.
        let mut config = crate::DatabaseConfig::server_default();
        config.columnar_cache_budget = 512; // bytes
        let mut db = Database::with_config(config);

        for name in ["t1", "t2", "t3"] {
            db.create_table(create_test_table_schema(name)).unwrap();
            for row in create_test_rows(100) {
                db.insert_row(name, row).unwrap();
            }
            // Populate the cache for this table (convert + insert).
            let cached = db.get_columnar(name).unwrap();
            assert!(cached.is_some(), "enabled (non-zero) cache should produce columnar data");
        }

        let stats = db.columnar_cache_stats();
        assert!(
            stats.evictions > 0,
            "a small budget must force at least one eviction (got {})",
            stats.evictions
        );
    }
}
