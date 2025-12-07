//! Cost estimation for query execution plans
//!
//! This module provides cost models for different access methods:
//! - Table scan (sequential scan)
//! - Index scan (B-tree lookup + random access)
//!
//! And DML operations:
//! - INSERT (row storage + index maintenance)
//! - UPDATE (row modification + selective index updates)
//! - DELETE (bitmap marking + index removal + potential compaction)
//!
//! Costs are estimated in arbitrary units representing relative work,
//! not absolute time. The optimizer uses these costs to compare different
//! execution strategies and choose the most efficient one.

use super::{ColumnStatistics, TableStatistics};

/// Cost estimator for access methods (scans, index lookups) and DML operations
///
/// Cost parameters are based on the PostgreSQL cost model:
/// - Sequential I/O is cheaper than random I/O
/// - Index scans have overhead for traversing the B-tree
/// - Cache effects are approximated by page costs
///
/// DML cost parameters are derived from TPC-C profiling (#3862):
/// - DELETE operations have significant index maintenance overhead
/// - Compaction occurs when >50% of rows are deleted
/// - Columnar cache invalidation/rebuild adds overhead
#[derive(Debug, Clone)]
pub struct CostEstimator {
    /// Cost of reading a page sequentially (default: 1.0)
    pub seq_page_cost: f64,

    /// Cost of reading a page randomly (default: 4.0, reflecting disk seek penalty)
    pub random_page_cost: f64,

    /// Cost of processing a single row (CPU cost, default: 0.01)
    pub cpu_tuple_cost: f64,

    /// Cost of processing a single index entry (default: 0.005)
    pub cpu_index_tuple_cost: f64,

    /// Estimated rows per page (default: 100 for 8KB pages)
    pub rows_per_page: f64,

    // ============================================================================
    // DML Cost Parameters
    // ============================================================================

    /// Base cost of inserting a single row (default: 0.1)
    /// Includes row storage and basic overhead
    pub insert_tuple_cost: f64,

    /// Cost of updating a hash index entry (PK/unique constraint) per row (default: 0.05)
    /// Applied per constraint index on insert/update/delete
    pub hash_index_update_cost: f64,

    /// Cost of updating a B-tree index entry per row (default: 0.15)
    /// B-tree operations are more expensive than hash updates due to tree rebalancing
    pub btree_index_update_cost: f64,

    /// Cost of updating/deleting a single row (default: 0.08)
    /// Includes bitmap marking and row modification
    pub update_tuple_cost: f64,

    /// Cost of deleting a single row (default: 0.05)
    /// Uses O(1) bitmap marking, cheaper than update
    pub delete_tuple_cost: f64,

    /// Cost multiplier when table compaction is likely (default: 2.0)
    /// Applied when deleted_ratio > 0.5
    pub compaction_cost_multiplier: f64,

    /// Base cost of rebuilding columnar representation (default: 0.02)
    /// Per-row cost for native columnar tables after DML
    pub columnar_rebuild_cost: f64,

    /// Cost of invalidating columnar cache (default: 0.1)
    /// Fixed cost for row-oriented tables with columnar cache
    pub columnar_cache_invalidation_cost: f64,

    // ============================================================================
    // WAL Cost Parameters (derived from TPC-C profiling #3862)
    // ============================================================================

    /// Cost of writing a single WAL entry per row (default: 0.12)
    /// Based on profiling showing WAL as 56% of DELETE operation time.
    /// WAL entries include: operation type, row data, and metadata.
    pub wal_write_cost: f64,

    /// Fixed overhead for WAL sync/flush operations (default: 0.5)
    /// Applied once per DML operation (amortized across batch operations).
    /// Includes fsync or equivalent durability guarantee.
    pub wal_sync_cost: f64,
}

impl Default for CostEstimator {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            // For in-memory databases (BTreeMap), random access is fast.
            // Using 1.5 instead of 4.0 (disk-based) to better reflect reality.
            random_page_cost: 1.5,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            rows_per_page: 100.0,
            // DML cost parameters derived from TPC-C profiling (#3862)
            insert_tuple_cost: 0.1,
            hash_index_update_cost: 0.05,
            btree_index_update_cost: 0.15,
            update_tuple_cost: 0.08,
            delete_tuple_cost: 0.05,
            compaction_cost_multiplier: 2.0,
            columnar_rebuild_cost: 0.02,
            columnar_cache_invalidation_cost: 0.1,
            // WAL cost parameters derived from TPC-C profiling (#3862)
            // WAL writes were 56% of DELETE time (600µs of 1.08ms total)
            // Row removal was 21% (230µs), so WAL is ~2.6x row removal cost
            wal_write_cost: 0.12,
            wal_sync_cost: 0.5,
        }
    }
}

/// Metadata about table indexes for DML cost estimation
#[derive(Debug, Clone, Default)]
pub struct TableIndexInfo {
    /// Number of hash indexes (PK + unique constraints)
    pub hash_index_count: usize,
    /// Number of user-defined B-tree indexes
    pub btree_index_count: usize,
    /// Whether the table uses native columnar storage
    pub is_native_columnar: bool,
    /// Current ratio of deleted rows (0.0 to 1.0)
    /// Used to estimate compaction probability
    pub deleted_ratio: f64,
}

impl TableIndexInfo {
    /// Create new table index info
    pub fn new(
        hash_index_count: usize,
        btree_index_count: usize,
        is_native_columnar: bool,
        deleted_ratio: f64,
    ) -> Self {
        Self {
            hash_index_count,
            btree_index_count,
            is_native_columnar,
            deleted_ratio,
        }
    }
}

impl CostEstimator {
    /// Create a cost estimator with custom read parameters (uses defaults for DML)
    pub fn new(
        seq_page_cost: f64,
        random_page_cost: f64,
        cpu_tuple_cost: f64,
        cpu_index_tuple_cost: f64,
    ) -> Self {
        let default = Self::default();
        Self {
            seq_page_cost,
            random_page_cost,
            cpu_tuple_cost,
            cpu_index_tuple_cost,
            rows_per_page: 100.0,
            // Use defaults for DML parameters
            insert_tuple_cost: default.insert_tuple_cost,
            hash_index_update_cost: default.hash_index_update_cost,
            btree_index_update_cost: default.btree_index_update_cost,
            update_tuple_cost: default.update_tuple_cost,
            delete_tuple_cost: default.delete_tuple_cost,
            compaction_cost_multiplier: default.compaction_cost_multiplier,
            columnar_rebuild_cost: default.columnar_rebuild_cost,
            columnar_cache_invalidation_cost: default.columnar_cache_invalidation_cost,
            wal_write_cost: default.wal_write_cost,
            wal_sync_cost: default.wal_sync_cost,
        }
    }

    /// Estimate cost of a sequential table scan
    ///
    /// A table scan reads all pages sequentially and processes all rows.
    /// Cost = (pages * seq_page_cost) + (rows * cpu_tuple_cost)
    ///
    /// # Arguments
    /// * `table_stats` - Statistics for the table being scanned
    ///
    /// # Example
    /// ```rust,ignore
    /// let cost = estimator.estimate_table_scan(&table_stats);
    /// // For 1000 rows: (10 pages * 1.0) + (1000 * 0.01) = 20.0
    /// ```
    pub fn estimate_table_scan(&self, table_stats: &TableStatistics) -> f64 {
        let row_count = table_stats.row_count as f64;
        let page_count = (row_count / self.rows_per_page).ceil();

        // I/O cost: sequential read of all pages
        let io_cost = page_count * self.seq_page_cost;

        // CPU cost: process every row
        let cpu_cost = row_count * self.cpu_tuple_cost;

        io_cost + cpu_cost
    }

    /// Estimate cost of an index scan
    ///
    /// Index scan cost has three components:
    /// 1. Index traversal (B-tree depth)
    /// 2. Index entries processed
    /// 3. Table rows fetched (random I/O)
    ///
    /// Cost = index_pages + (index_entries * cpu_index_cost) + (rows * random_page_cost)
    ///
    /// # Arguments
    /// * `table_stats` - Statistics for the table
    /// * `col_stats` - Statistics for the indexed column
    /// * `selectivity` - Fraction of rows matched by predicate (0.0 to 1.0)
    ///
    /// # Selectivity Examples
    /// - `WHERE id = 42` on unique column: selectivity = 1/row_count ≈ 0.001
    /// - `WHERE age > 18` with 80% adults: selectivity = 0.8
    /// - No predicate (index used for ORDER BY): selectivity = 1.0
    ///
    /// # Returns
    /// Estimated cost in arbitrary units. Lower is better.
    pub fn estimate_index_scan(
        &self,
        table_stats: &TableStatistics,
        col_stats: &ColumnStatistics,
        selectivity: f64,
    ) -> f64 {
        let row_count = table_stats.row_count as f64;
        let rows_fetched = row_count * selectivity;

        // 1. Index traversal cost (B-tree depth)
        // Typical B-tree depth is log_fanout(entries)
        // Assume fanout of 100 (typical for B-tree)
        let index_entries = col_stats.n_distinct as f64;
        let index_depth = (index_entries.log10() / 100_f64.log10()).ceil().max(1.0);
        let index_traversal_cost = index_depth * self.random_page_cost;

        // 2. Cost of scanning index entries
        // We scan entries proportional to selectivity * distinct values
        let index_entries_scanned = index_entries * selectivity;
        let index_scan_cost = index_entries_scanned * self.cpu_index_tuple_cost;

        // 3. Cost of fetching table rows (random I/O)
        // Each matched row requires a random page access
        // Apply correlation factor: sequential access is cheaper
        let table_fetch_cost = rows_fetched * self.random_page_cost;

        // 4. CPU cost of processing fetched rows
        let cpu_cost = rows_fetched * self.cpu_tuple_cost;

        index_traversal_cost + index_scan_cost + table_fetch_cost + cpu_cost
    }

    // ============================================================================
    // DML Cost Estimation
    // ============================================================================

    /// Estimate cost of inserting rows
    ///
    /// INSERT cost components:
    /// 1. Base tuple insertion cost (row storage)
    /// 2. Hash index updates (PK + unique constraints)
    /// 3. B-tree index updates (user-defined indexes)
    /// 4. Columnar storage overhead (if native columnar)
    /// 5. WAL write cost (per-row entry + sync overhead)
    ///
    /// # Arguments
    /// * `row_count` - Number of rows to insert
    /// * `table_stats` - Statistics for the target table
    /// * `index_info` - Information about table indexes
    ///
    /// # Example
    /// ```rust,ignore
    /// let cost = estimator.estimate_insert(100, &table_stats, &index_info);
    /// // For 100 rows with 1 PK and 2 B-tree indexes:
    /// // (100 * 0.1) + (100 * 1 * 0.05) + (100 * 2 * 0.15) + WAL = ~57
    /// ```
    pub fn estimate_insert(
        &self,
        row_count: usize,
        table_stats: &TableStatistics,
        index_info: &TableIndexInfo,
    ) -> f64 {
        let rows = row_count as f64;

        // 1. Base tuple insertion cost
        let tuple_cost = rows * self.insert_tuple_cost;

        // 2. Hash index update cost (PK + unique constraints)
        let hash_index_cost =
            rows * index_info.hash_index_count as f64 * self.hash_index_update_cost;

        // 3. B-tree index update cost
        let btree_index_cost =
            rows * index_info.btree_index_count as f64 * self.btree_index_update_cost;

        // 4. Columnar overhead
        let columnar_cost = if index_info.is_native_columnar {
            // Native columnar tables rebuild entirely on each DML
            table_stats.row_count as f64 * self.columnar_rebuild_cost
        } else {
            // Row-oriented tables just invalidate the cache
            self.columnar_cache_invalidation_cost
        };

        // 5. WAL write cost
        // Per-row WAL entry cost + fixed sync overhead (amortized for batches)
        let wal_cost = rows * self.wal_write_cost + self.wal_sync_cost;

        tuple_cost + hash_index_cost + btree_index_cost + columnar_cost + wal_cost
    }

    /// Estimate cost of updating rows
    ///
    /// UPDATE cost components:
    /// 1. Base tuple update cost
    /// 2. Hash index updates (only if indexed columns change)
    /// 3. B-tree index updates (only if indexed columns change)
    /// 4. Columnar storage overhead
    /// 5. WAL write cost (per-row entry + sync overhead)
    ///
    /// For selective updates (where only some columns change), the actual cost
    /// may be lower since indexes not involving changed columns are skipped.
    ///
    /// # Arguments
    /// * `row_count` - Number of rows to update
    /// * `table_stats` - Statistics for the target table
    /// * `index_info` - Information about table indexes
    /// * `indexes_affected_ratio` - Fraction of indexes affected by column changes (0.0 to 1.0)
    ///   Use 1.0 if all indexed columns might change, or a lower value for selective updates
    ///
    /// # Example
    /// ```rust,ignore
    /// // Full update (all columns may change)
    /// let cost = estimator.estimate_update(50, &table_stats, &index_info, 1.0);
    ///
    /// // Selective update (only non-indexed columns change)
    /// let cost = estimator.estimate_update(50, &table_stats, &index_info, 0.0);
    /// ```
    pub fn estimate_update(
        &self,
        row_count: usize,
        table_stats: &TableStatistics,
        index_info: &TableIndexInfo,
        indexes_affected_ratio: f64,
    ) -> f64 {
        let rows = row_count as f64;

        // 1. Base tuple update cost
        let tuple_cost = rows * self.update_tuple_cost;

        // 2. Hash index update cost (scaled by affected ratio)
        // UPDATE requires remove + insert = 2x the cost
        let hash_index_cost = rows
            * index_info.hash_index_count as f64
            * self.hash_index_update_cost
            * 2.0
            * indexes_affected_ratio;

        // 3. B-tree index update cost (scaled by affected ratio)
        // UPDATE requires remove + insert = 2x the cost
        let btree_index_cost = rows
            * index_info.btree_index_count as f64
            * self.btree_index_update_cost
            * 2.0
            * indexes_affected_ratio;

        // 4. Columnar overhead
        let columnar_cost = if index_info.is_native_columnar {
            table_stats.row_count as f64 * self.columnar_rebuild_cost
        } else {
            self.columnar_cache_invalidation_cost
        };

        // 5. WAL write cost
        // Per-row WAL entry cost + fixed sync overhead (amortized for batches)
        let wal_cost = rows * self.wal_write_cost + self.wal_sync_cost;

        tuple_cost + hash_index_cost + btree_index_cost + columnar_cost + wal_cost
    }

    /// Estimate cost of deleting rows
    ///
    /// DELETE cost components:
    /// 1. Base tuple deletion cost (bitmap marking - O(1) per row)
    /// 2. Hash index updates (removing entries)
    /// 3. B-tree index updates (removing entries)
    /// 4. Columnar storage overhead
    /// 5. Potential compaction cost (when >50% rows deleted)
    /// 6. WAL write cost (per-row entry + sync overhead)
    ///
    /// Per TPC-C profiling (#3862), WAL writes are 56% of DELETE time,
    /// making this the dominant cost component.
    ///
    /// The compaction cost is significant because it:
    /// - Rebuilds the entire row vector (O(n))
    /// - Rebuilds all internal hash indexes
    /// - Triggers user-defined index rebuilds at the database level
    ///
    /// # Arguments
    /// * `row_count` - Number of rows to delete
    /// * `table_stats` - Statistics for the target table
    /// * `index_info` - Information about table indexes
    ///
    /// # Example
    /// ```rust,ignore
    /// let cost = estimator.estimate_delete(100, &table_stats, &index_info);
    /// // Compaction multiplier is applied if deleted_ratio would exceed 50%
    /// ```
    pub fn estimate_delete(
        &self,
        row_count: usize,
        table_stats: &TableStatistics,
        index_info: &TableIndexInfo,
    ) -> f64 {
        let rows = row_count as f64;
        let total_rows = table_stats.row_count as f64;

        // 1. Base tuple deletion cost (O(1) bitmap marking per row)
        let tuple_cost = rows * self.delete_tuple_cost;

        // 2. Hash index update cost (removing entries)
        let hash_index_cost =
            rows * index_info.hash_index_count as f64 * self.hash_index_update_cost;

        // 3. B-tree index update cost (removing entries)
        let btree_index_cost =
            rows * index_info.btree_index_count as f64 * self.btree_index_update_cost;

        // 4. Columnar overhead
        let columnar_cost = if index_info.is_native_columnar {
            // Native columnar tables rebuild entirely on each DML
            (total_rows - rows).max(0.0) * self.columnar_rebuild_cost
        } else {
            self.columnar_cache_invalidation_cost
        };

        // 5. Compaction cost estimation
        // Compaction occurs when deleted_ratio > 0.5
        // Estimate the new deleted ratio after this delete
        let current_deleted = total_rows * index_info.deleted_ratio;
        let new_deleted = current_deleted + rows;
        let new_deleted_ratio = if total_rows > 0.0 { new_deleted / total_rows } else { 0.0 };

        let compaction_cost = if new_deleted_ratio > 0.5 {
            // Compaction will occur:
            // - Rebuild row vector: O(n) where n = remaining rows
            // - Rebuild hash indexes: proportional to remaining rows
            // - User-defined indexes rebuilt at database level (not counted here)
            let remaining_rows = (total_rows - new_deleted).max(0.0);
            let rebuild_cost = remaining_rows * self.cpu_tuple_cost * self.compaction_cost_multiplier;
            let hash_rebuild_cost = remaining_rows
                * index_info.hash_index_count as f64
                * self.hash_index_update_cost;
            rebuild_cost + hash_rebuild_cost
        } else {
            0.0
        };

        // 6. WAL write cost (dominant cost per profiling #3862)
        // Per-row WAL entry cost + fixed sync overhead (amortized for batches)
        let wal_cost = rows * self.wal_write_cost + self.wal_sync_cost;

        tuple_cost + hash_index_cost + btree_index_cost + columnar_cost + compaction_cost + wal_cost
    }

    /// Choose the best access method based on cost
    ///
    /// Compares table scan vs index scan costs and returns the cheaper option.
    ///
    /// # Arguments
    /// * `table_stats` - Statistics for the table
    /// * `col_stats` - Statistics for the indexed column (if index exists)
    /// * `selectivity` - Predicate selectivity (fraction of rows matched)
    ///
    /// # Returns
    /// - `AccessMethod::TableScan` if sequential scan is cheaper
    /// - `AccessMethod::IndexScan` if index scan is cheaper
    /// - `AccessMethod::TableScan` if no index statistics available
    pub fn choose_access_method(
        &self,
        table_stats: &TableStatistics,
        col_stats: Option<&ColumnStatistics>,
        selectivity: f64,
    ) -> AccessMethod {
        let table_scan_cost = self.estimate_table_scan(table_stats);

        if let Some(col_stats) = col_stats {
            let index_scan_cost = self.estimate_index_scan(table_stats, col_stats, selectivity);

            // Choose the access method with lower cost
            if index_scan_cost < table_scan_cost {
                AccessMethod::IndexScan {
                    estimated_cost: index_scan_cost,
                    estimated_rows: (table_stats.row_count as f64 * selectivity) as usize,
                }
            } else {
                AccessMethod::TableScan { estimated_cost: table_scan_cost }
            }
        } else {
            // No index available, must use table scan
            AccessMethod::TableScan { estimated_cost: table_scan_cost }
        }
    }
}

/// Represents the chosen access method for a query
#[derive(Debug, Clone, PartialEq)]
pub enum AccessMethod {
    /// Sequential scan of entire table
    TableScan {
        /// Estimated cost of this access method
        estimated_cost: f64,
    },

    /// Index scan with optional filtering
    IndexScan {
        /// Estimated cost of this access method
        estimated_cost: f64,
        /// Estimated number of rows to be returned
        estimated_rows: usize,
    },
}

impl AccessMethod {
    /// Get the estimated cost of this access method
    pub fn cost(&self) -> f64 {
        match self {
            AccessMethod::TableScan { estimated_cost } => *estimated_cost,
            AccessMethod::IndexScan { estimated_cost, .. } => *estimated_cost,
        }
    }

    /// Check if this is an index scan
    pub fn is_index_scan(&self) -> bool {
        matches!(self, AccessMethod::IndexScan { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    fn create_test_table_stats(row_count: usize) -> TableStatistics {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let rows: Vec<Row> =
            (0..row_count).map(|i| Row::new(vec![SqlValue::Integer(i as i64)])).collect();

        TableStatistics::compute(&rows, &schema)
    }

    #[test]
    fn test_table_scan_cost() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        let cost = estimator.estimate_table_scan(&table_stats);

        // Expected: (1000/100 pages * 1.0) + (1000 rows * 0.01) = 10 + 10 = 20
        assert!((cost - 20.0).abs() < 0.1);
    }

    #[test]
    fn test_index_scan_high_selectivity() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let col_stats = table_stats.columns.get("id").unwrap();

        // High selectivity (50% of rows match)
        let cost = estimator.estimate_index_scan(&table_stats, col_stats, 0.5);

        // Index scan should be expensive for high selectivity
        // because we do random I/O for each row
        assert!(cost > 100.0);
    }

    #[test]
    fn test_index_scan_low_selectivity() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let col_stats = table_stats.columns.get("id").unwrap();

        // Low selectivity (1% of rows match)
        let cost = estimator.estimate_index_scan(&table_stats, col_stats, 0.01);

        // Index scan should be cheap for low selectivity
        assert!(cost < 50.0);
    }

    #[test]
    fn test_choose_access_method_favors_index_for_low_selectivity() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);
        let col_stats = table_stats.columns.get("id").unwrap();

        // Very selective query (0.1% of rows)
        let method = estimator.choose_access_method(&table_stats, Some(col_stats), 0.001);

        assert!(method.is_index_scan());
    }

    #[test]
    fn test_choose_access_method_favors_table_scan_for_high_selectivity() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let col_stats = table_stats.columns.get("id").unwrap();

        // Non-selective query (90% of rows)
        let method = estimator.choose_access_method(&table_stats, Some(col_stats), 0.9);

        assert!(!method.is_index_scan());
    }

    #[test]
    fn test_choose_access_method_no_index() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        // No index available
        let method = estimator.choose_access_method(&table_stats, None, 0.1);

        assert!(!method.is_index_scan());
    }

    // ============================================================================
    // DML Cost Estimation Tests
    // ============================================================================

    #[test]
    fn test_insert_cost_basic() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(1, 0, false, 0.0);

        // Insert 100 rows with 1 hash index (PK)
        let cost = estimator.estimate_insert(100, &table_stats, &index_info);

        // Expected:
        // - Tuple cost: 100 * 0.1 = 10.0
        // - Hash index: 100 * 1 * 0.05 = 5.0
        // - Columnar invalidation: 0.1
        // - WAL cost: 100 * 0.12 + 0.5 = 12.5
        // Total: ~27.6
        assert!(cost > 27.0 && cost < 29.0, "Insert cost was {}", cost);
    }

    #[test]
    fn test_insert_cost_with_btree_indexes() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(1, 2, false, 0.0);

        // Insert 100 rows with 1 PK and 2 B-tree indexes
        let cost = estimator.estimate_insert(100, &table_stats, &index_info);

        // B-tree indexes add significant overhead
        let cost_no_btree = estimator.estimate_insert(
            100,
            &table_stats,
            &TableIndexInfo::new(1, 0, false, 0.0),
        );
        assert!(cost > cost_no_btree, "B-tree indexes should increase cost");
    }

    #[test]
    fn test_insert_cost_native_columnar() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        let row_index_info = TableIndexInfo::new(1, 0, false, 0.0);
        let columnar_index_info = TableIndexInfo::new(1, 0, true, 0.0);

        let row_cost = estimator.estimate_insert(10, &table_stats, &row_index_info);
        let columnar_cost = estimator.estimate_insert(10, &table_stats, &columnar_index_info);

        // Native columnar tables have higher overhead due to columnar rebuild
        assert!(
            columnar_cost > row_cost,
            "Columnar insert cost {} should be > row cost {}",
            columnar_cost,
            row_cost
        );
    }

    #[test]
    fn test_update_cost_basic() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(1, 1, false, 0.0);

        // Update 50 rows, all indexes affected
        let full_cost = estimator.estimate_update(50, &table_stats, &index_info, 1.0);

        // Update 50 rows, no indexes affected (only non-indexed columns changed)
        let selective_cost = estimator.estimate_update(50, &table_stats, &index_info, 0.0);

        // Full update should be more expensive than selective update
        assert!(
            full_cost > selective_cost,
            "Full update cost {} should be > selective update cost {}",
            full_cost,
            selective_cost
        );
    }

    #[test]
    fn test_update_cost_scales_with_affected_ratio() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(2, 3, false, 0.0);

        let cost_0 = estimator.estimate_update(100, &table_stats, &index_info, 0.0);
        let cost_50 = estimator.estimate_update(100, &table_stats, &index_info, 0.5);
        let cost_100 = estimator.estimate_update(100, &table_stats, &index_info, 1.0);

        // Costs should increase with affected ratio
        assert!(cost_50 > cost_0, "50% affected should cost more than 0%");
        assert!(cost_100 > cost_50, "100% affected should cost more than 50%");
    }

    #[test]
    fn test_delete_cost_basic() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(1, 1, false, 0.0);

        // Delete 100 rows (10% of table) - no compaction
        let cost = estimator.estimate_delete(100, &table_stats, &index_info);

        // Should be positive and reasonable
        assert!(cost > 0.0, "Delete cost should be positive");
        assert!(cost < 100.0, "Delete cost should be reasonable");
    }

    #[test]
    fn test_delete_cost_with_compaction() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        // Case 1: Delete 40% - no compaction yet
        let index_info_40 = TableIndexInfo::new(1, 0, false, 0.0);
        let cost_40 = estimator.estimate_delete(400, &table_stats, &index_info_40);

        // Case 2: Delete 10% when already at 45% deleted - will trigger compaction
        let index_info_trigger = TableIndexInfo::new(1, 0, false, 0.45);
        let cost_trigger = estimator.estimate_delete(100, &table_stats, &index_info_trigger);

        // Compaction should add overhead
        // Note: Even with fewer rows deleted, the compaction overhead makes it expensive
        assert!(
            cost_trigger > cost_40 * 0.1,
            "Delete with compaction {} should have meaningful overhead vs large delete without {}",
            cost_trigger,
            cost_40
        );
    }

    #[test]
    fn test_delete_more_expensive_with_more_indexes() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        let no_indexes = TableIndexInfo::new(0, 0, false, 0.0);
        let many_indexes = TableIndexInfo::new(2, 5, false, 0.0);

        let cost_no_indexes = estimator.estimate_delete(100, &table_stats, &no_indexes);
        let cost_many_indexes = estimator.estimate_delete(100, &table_stats, &many_indexes);

        assert!(
            cost_many_indexes > cost_no_indexes,
            "More indexes should increase delete cost: {} vs {}",
            cost_many_indexes,
            cost_no_indexes
        );
    }

    #[test]
    fn test_delete_cheaper_than_insert() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(1, 2, false, 0.0);

        // DELETE uses O(1) bitmap marking, INSERT adds to vector
        let delete_cost = estimator.estimate_delete(100, &table_stats, &index_info);
        let insert_cost = estimator.estimate_insert(100, &table_stats, &index_info);

        // Without compaction, DELETE should be cheaper due to O(1) bitmap vs vector append
        assert!(
            delete_cost < insert_cost,
            "Delete {} should be cheaper than insert {} (without compaction)",
            delete_cost,
            insert_cost
        );
    }

    #[test]
    fn test_dml_costs_scale_with_row_count() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);
        let index_info = TableIndexInfo::new(1, 1, false, 0.0);

        let insert_10 = estimator.estimate_insert(10, &table_stats, &index_info);
        let insert_100 = estimator.estimate_insert(100, &table_stats, &index_info);

        let delete_10 = estimator.estimate_delete(10, &table_stats, &index_info);
        let delete_100 = estimator.estimate_delete(100, &table_stats, &index_info);

        // Costs should scale roughly linearly with row count
        assert!(insert_100 > insert_10 * 5.0, "Insert should scale with rows");
        assert!(delete_100 > delete_10 * 5.0, "Delete should scale with rows");
    }

    // ============================================================================
    // WAL Cost Estimation Tests
    // ============================================================================

    #[test]
    fn test_wal_cost_included_in_insert() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(0, 0, false, 0.0);

        // Insert 100 rows with no indexes
        let cost = estimator.estimate_insert(100, &table_stats, &index_info);

        // WAL component: 100 * 0.12 + 0.5 = 12.5
        // Tuple: 100 * 0.1 = 10.0
        // Columnar: 0.1
        // Total: ~22.6
        assert!(cost > 22.0, "Insert cost should include WAL: {}", cost);

        // Verify WAL is a significant portion (should be >50% of base cost)
        let tuple_plus_columnar = 100.0 * 0.1 + 0.1; // 10.1
        let wal_cost = 100.0 * 0.12 + 0.5; // 12.5
        assert!(
            wal_cost > tuple_plus_columnar,
            "WAL cost ({}) should exceed base tuple cost ({})",
            wal_cost,
            tuple_plus_columnar
        );
    }

    #[test]
    fn test_wal_cost_included_in_update() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(0, 0, false, 0.0);

        // Update 50 rows with no index updates
        let cost = estimator.estimate_update(50, &table_stats, &index_info, 0.0);

        // WAL component: 50 * 0.12 + 0.5 = 6.5
        // Tuple: 50 * 0.08 = 4.0
        // Columnar: 0.1
        // Total: ~10.6
        assert!(cost > 10.0, "Update cost should include WAL: {}", cost);
    }

    #[test]
    fn test_wal_cost_included_in_delete() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(0, 0, false, 0.0);

        // Delete 100 rows with no indexes
        let cost = estimator.estimate_delete(100, &table_stats, &index_info);

        // WAL component: 100 * 0.12 + 0.5 = 12.5
        // Tuple: 100 * 0.05 = 5.0
        // Columnar: 0.1
        // Total: ~17.6
        assert!(cost > 17.0, "Delete cost should include WAL: {}", cost);
    }

    #[test]
    fn test_wal_cost_dominant_in_delete() {
        // Per profiling (#3862), WAL is 56% of DELETE time
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);
        let index_info = TableIndexInfo::new(1, 0, false, 0.0);

        // Calculate components
        let rows = 100.0;
        let tuple_cost = rows * estimator.delete_tuple_cost; // 5.0
        let hash_cost = rows * 1.0 * estimator.hash_index_update_cost; // 5.0
        let wal_cost = rows * estimator.wal_write_cost + estimator.wal_sync_cost; // 12.5

        // WAL should be the dominant cost component (>40% of non-columnar costs)
        let base_dml_cost = tuple_cost + hash_cost;
        assert!(
            wal_cost > base_dml_cost,
            "WAL cost ({}) should exceed base DML cost ({}) per profiling data",
            wal_cost,
            base_dml_cost
        );
    }

    #[test]
    fn test_wal_sync_cost_amortized_for_batches() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);
        let index_info = TableIndexInfo::new(1, 0, false, 0.0);

        // Single-row insert
        let cost_1 = estimator.estimate_insert(1, &table_stats, &index_info);

        // 100-row batch insert
        let cost_100 = estimator.estimate_insert(100, &table_stats, &index_info);

        // Per-row cost should be lower for batches due to amortized sync cost
        let per_row_single = cost_1;
        let per_row_batch = cost_100 / 100.0;

        assert!(
            per_row_batch < per_row_single,
            "Batch insert per-row cost ({}) should be less than single-row cost ({}) due to amortized WAL sync",
            per_row_batch,
            per_row_single
        );
    }

    #[test]
    fn test_wal_cost_proportional_to_rows() {
        let estimator = CostEstimator::default();

        // Calculate pure WAL costs (excluding sync overhead)
        let wal_10 = 10.0 * estimator.wal_write_cost;
        let wal_100 = 100.0 * estimator.wal_write_cost;

        // WAL cost should scale linearly with row count
        assert!(
            (wal_100 - wal_10 * 10.0).abs() < 0.001,
            "WAL write cost should scale linearly: 10x rows should be 10x cost"
        );
    }
}
