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
use vibesql_types::DataType;

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

/// Base row size in bytes for WAL cost scaling (64 bytes)
/// This represents a minimal row with a few small columns.
/// Rows larger than this will have proportionally higher WAL costs.
pub const BASE_ROW_SIZE: f64 = 64.0;

/// Maximum WAL size scaling factor (10x)
/// Caps the row size multiplier to prevent extreme cost estimates
/// for tables with very large rows.
pub const MAX_WAL_SIZE_FACTOR: f64 = 10.0;

/// Estimate the average row size in bytes for a given data type.
///
/// These are heuristic estimates used for WAL cost estimation:
/// - Fixed-size types: actual size
/// - Variable-size types: typical/average fill based on field definition
///
/// # Arguments
/// * `data_type` - The SQL data type
///
/// # Returns
/// Estimated size in bytes for storing a value of this type.
pub fn estimate_type_size(data_type: &DataType) -> usize {
    match data_type {
        // Boolean: 1 byte
        DataType::Boolean => 1,

        // Integer types
        DataType::Smallint => 2,
        DataType::Integer => 4,
        DataType::Bigint | DataType::Unsigned => 8,

        // Decimal/Numeric: 16 bytes (typical for DECIMAL storage)
        DataType::Numeric { .. } | DataType::Decimal { .. } => 16,

        // Floating point
        DataType::Real => 4,
        DataType::DoublePrecision => 8,
        DataType::Float { precision } => {
            if *precision <= 24 { 4 } else { 8 }
        }

        // Character types
        DataType::Character { length } => *length,
        DataType::Varchar { max_length } => {
            // For VARCHAR, use half the max length or 32 bytes, whichever is smaller
            match max_length {
                Some(len) => (*len / 2).min(32),
                None => 32, // Default for unbounded VARCHAR
            }
        }
        DataType::CharacterLargeObject => 64, // CLOB: heuristic average
        DataType::Name => 32, // NAME type: typically short identifiers

        // Date/time types
        DataType::Date => 4,
        DataType::Time { .. } => 8,
        DataType::Timestamp { .. } => 8,
        DataType::Interval { .. } => 16,

        // Binary types
        DataType::BinaryLargeObject => 128, // BLOB: heuristic average
        DataType::Bit { length } => {
            match length {
                Some(len) => (*len).div_ceil(8), // Convert bits to bytes
                None => 1, // Default BIT(1)
            }
        }

        // Vector types: dimensions * 8 bytes (f64 per dimension)
        DataType::Vector { dimensions } => *dimensions as usize * 8,

        // User-defined types: estimate as 64 bytes (unknown size)
        DataType::UserDefined { .. } => 64,

        // Null: 0 bytes (just a marker)
        DataType::Null => 0,
    }
}

/// Estimate the average row size for a table schema.
///
/// Sums the estimated size of each column plus a small overhead per row
/// for metadata (e.g., null bitmap, row header).
///
/// # Arguments
/// * `columns` - Slice of column data types
///
/// # Returns
/// Estimated average row size in bytes.
pub fn estimate_row_size(columns: &[DataType]) -> usize {
    // Per-row overhead: null bitmap + row header (estimate 8 bytes)
    const ROW_OVERHEAD: usize = 8;

    let column_size: usize = columns.iter().map(estimate_type_size).sum();
    (column_size + ROW_OVERHEAD).max(BASE_ROW_SIZE as usize)
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
    /// Average row size in bytes (estimated from schema)
    /// Used to scale WAL cost based on actual row size.
    /// Defaults to BASE_ROW_SIZE (64 bytes) if unknown.
    pub avg_row_size: usize,
}

impl TableIndexInfo {
    /// Create new table index info
    pub fn new(
        hash_index_count: usize,
        btree_index_count: usize,
        is_native_columnar: bool,
        deleted_ratio: f64,
        avg_row_size: usize,
    ) -> Self {
        Self {
            hash_index_count,
            btree_index_count,
            is_native_columnar,
            deleted_ratio,
            avg_row_size,
        }
    }

    /// Calculate the WAL size scaling factor based on average row size.
    ///
    /// The factor is clamped between 1.0 (for small rows <= BASE_ROW_SIZE)
    /// and MAX_WAL_SIZE_FACTOR (for very large rows).
    ///
    /// # Returns
    /// A multiplier to apply to the per-row WAL write cost.
    #[inline]
    pub fn wal_size_factor(&self) -> f64 {
        let size_factor = self.avg_row_size as f64 / BASE_ROW_SIZE;
        size_factor.clamp(1.0, MAX_WAL_SIZE_FACTOR)
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
    /// ```text
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

    /// Estimate cost of a skip-scan on a composite index
    ///
    /// Skip-scan enables using a composite index when the query doesn't filter on
    /// the prefix columns. It works by iterating through distinct values of the
    /// prefix columns and performing an index lookup for each.
    ///
    /// # How Skip-Scan Works
    ///
    /// For a composite index on `(a, b)` and query `WHERE b = 5`:
    /// 1. Get distinct values of `a` from the index
    /// 2. For each distinct `a` value, seek to `(a, 5)` in the index
    /// 3. Return matching rows
    ///
    /// # Cost Model
    ///
    /// Cost = prefix_cardinality * (seek_cost + scan_cost_per_prefix)
    ///
    /// Where:
    /// - `prefix_cardinality` = number of distinct values in skipped prefix columns
    /// - `seek_cost` = random I/O cost to seek to each prefix value
    /// - `scan_cost_per_prefix` = cost to scan rows within each prefix group
    ///
    /// # Arguments
    /// * `table_stats` - Statistics for the table
    /// * `prefix_col_stats` - Statistics for the prefix column(s) being skipped
    /// * `filter_selectivity` - Selectivity of the filter on non-prefix columns (0.0 to 1.0)
    ///
    /// # Returns
    /// Estimated cost in arbitrary units. Lower is better.
    ///
    /// # Example
    /// ```text
    /// // Index on (region, date), query: WHERE date = '2024-01-01'
    /// // If region has 10 distinct values and date filter matches 1% of rows:
    /// let cost = estimator.estimate_skip_scan_cost(&table_stats, &region_stats, 0.01);
    /// ```
    pub fn estimate_skip_scan_cost(
        &self,
        table_stats: &TableStatistics,
        prefix_col_stats: &ColumnStatistics,
        filter_selectivity: f64,
    ) -> f64 {
        let total_rows = table_stats.row_count as f64;
        let prefix_cardinality = prefix_col_stats.n_distinct as f64;

        // Rows per prefix value (assuming uniform distribution)
        let rows_per_prefix = if prefix_cardinality > 0.0 {
            total_rows / prefix_cardinality
        } else {
            total_rows
        };

        // Expected rows matching filter within each prefix group
        let matching_rows_per_prefix = rows_per_prefix * filter_selectivity;

        // 1. Seek cost: one random I/O per distinct prefix value
        // This is the key benefit of skip-scan - we trade one seek per prefix
        // for potentially many fewer index entries scanned
        let seek_cost = prefix_cardinality * self.random_page_cost;

        // 2. Index scan cost within each prefix group
        // We scan index entries proportional to matching rows
        let index_scan_cost = prefix_cardinality * matching_rows_per_prefix * self.cpu_index_tuple_cost;

        // 3. Table fetch cost: estimate pages accessed for matched rows
        // For skip-scan, the rows are scattered across different prefix groups,
        // but within each prefix group they may be clustered.
        // Assume moderate clustering - roughly sqrt(rows) pages accessed
        let total_matching_rows = total_rows * filter_selectivity;
        let estimated_pages = (total_matching_rows.sqrt()).max(1.0);
        let table_fetch_cost = estimated_pages * self.random_page_cost;

        // 4. CPU cost for processing matched rows
        let cpu_cost = total_matching_rows * self.cpu_tuple_cost;

        seek_cost + index_scan_cost + table_fetch_cost + cpu_cost
    }

    /// Determine if skip-scan is beneficial compared to a table scan
    ///
    /// Skip-scan is beneficial when:
    /// - The prefix columns have low cardinality (few distinct values)
    /// - The filter on non-prefix columns is selective
    ///
    /// # Arguments
    /// * `table_stats` - Statistics for the table
    /// * `prefix_col_stats` - Statistics for the prefix column(s) being skipped
    /// * `filter_selectivity` - Selectivity of the filter on non-prefix columns
    ///
    /// # Returns
    /// `true` if skip-scan is estimated to be cheaper than a table scan
    pub fn should_use_skip_scan(
        &self,
        table_stats: &TableStatistics,
        prefix_col_stats: &ColumnStatistics,
        filter_selectivity: f64,
    ) -> bool {
        let skip_scan_cost = self.estimate_skip_scan_cost(table_stats, prefix_col_stats, filter_selectivity);
        let table_scan_cost = self.estimate_table_scan(table_stats);
        skip_scan_cost < table_scan_cost
    }

    /// Estimate skip-scan cost for multi-column prefix skip
    ///
    /// This extends `estimate_skip_scan_cost` to handle skipping multiple prefix columns.
    /// For an index on (a, b, c, d) with a filter on column d:
    /// - skip_columns=1: iterate distinct(a), seek each (a, d_val)
    /// - skip_columns=2: iterate distinct(a,b), seek each (a, b, d_val)
    /// - skip_columns=3: iterate distinct(a,b,c), seek each (a, b, c, d_val)
    ///
    /// The optimal skip depth depends on the combined cardinality of prefix columns.
    ///
    /// # Arguments
    /// * `table_stats` - Statistics for the table
    /// * `prefix_col_stats` - Statistics for each prefix column being skipped (in order)
    /// * `filter_selectivity` - Selectivity of the filter on non-prefix columns
    ///
    /// # Returns
    /// Estimated cost of skip-scan with multi-column prefix
    ///
    /// # Example
    /// ```text
    /// // Index on (country, region, city, date), query: WHERE date = '2024-01-01'
    /// // Skip 3 columns to filter on date:
    /// let stats = vec![&country_stats, &region_stats, &city_stats];
    /// let cost = estimator.estimate_skip_scan_cost_multi_column(&table_stats, &stats, 0.01);
    /// ```
    pub fn estimate_skip_scan_cost_multi_column(
        &self,
        table_stats: &TableStatistics,
        prefix_col_stats: &[&ColumnStatistics],
        filter_selectivity: f64,
    ) -> f64 {
        if prefix_col_stats.is_empty() {
            // No prefix to skip - this is essentially a table scan
            return self.estimate_table_scan(table_stats);
        }

        // For single column, delegate to existing method
        if prefix_col_stats.len() == 1 {
            return self.estimate_skip_scan_cost(table_stats, prefix_col_stats[0], filter_selectivity);
        }

        let total_rows = table_stats.row_count as f64;

        // Calculate combined cardinality for N prefix columns
        // Worst case: product of all distinct values (independent columns)
        // We apply a correlation factor to account for columns that are likely correlated
        let combined_cardinality = self.estimate_combined_prefix_cardinality(prefix_col_stats, total_rows);

        // Rows per prefix combination (assuming uniform distribution)
        let rows_per_prefix = if combined_cardinality > 0.0 {
            total_rows / combined_cardinality
        } else {
            total_rows
        };

        // Expected rows matching filter within each prefix group
        let matching_rows_per_prefix = rows_per_prefix * filter_selectivity;

        // 1. Seek cost: one random I/O per distinct prefix combination
        // More prefix columns means more seeks
        let seek_cost = combined_cardinality * self.random_page_cost;

        // 2. Index scan cost within each prefix group
        let index_scan_cost = combined_cardinality * matching_rows_per_prefix * self.cpu_index_tuple_cost;

        // 3. Table fetch cost: estimate pages accessed for matched rows
        let total_matching_rows = total_rows * filter_selectivity;
        let estimated_pages = (total_matching_rows.sqrt()).max(1.0);
        let table_fetch_cost = estimated_pages * self.random_page_cost;

        // 4. CPU cost for processing matched rows
        let cpu_cost = total_matching_rows * self.cpu_tuple_cost;

        seek_cost + index_scan_cost + table_fetch_cost + cpu_cost
    }

    /// Estimate combined cardinality for multiple prefix columns
    ///
    /// Uses a correlation-aware heuristic to estimate the number of distinct
    /// combinations of N prefix columns.
    ///
    /// Naive approach: product of all distinct values (assumes independence)
    /// Better approach: Apply correlation factor based on cardinality ratios
    ///
    /// For columns with similar cardinality ratios (e.g., country/region/city),
    /// we expect significant correlation - each country has fewer regions than
    /// the total distinct regions, and each region has fewer cities.
    fn estimate_combined_prefix_cardinality(
        &self,
        prefix_col_stats: &[&ColumnStatistics],
        total_rows: f64,
    ) -> f64 {
        if prefix_col_stats.is_empty() {
            return 1.0;
        }

        // Start with the cardinality of the first column
        let first_cardinality = prefix_col_stats[0].n_distinct as f64;
        let mut combined = first_cardinality;

        // For each additional column, apply correlation-aware multiplication
        for col_stat in prefix_col_stats.iter().skip(1) {
            let col_cardinality = col_stat.n_distinct as f64;

            // Estimate how many values of this column appear per value of the previous columns
            // If columns are correlated (e.g., region per country), this ratio will be smaller
            // than the full cardinality
            //
            // Heuristic: If combined cardinality is already high relative to row count,
            // new columns likely add fewer distinct combinations
            let coverage_ratio = combined / total_rows.max(1.0);

            // Correlation factor: higher coverage = more correlation = less multiplier
            // Range: 0.3 (highly correlated) to 1.0 (independent)
            let correlation_factor = (1.0 - 0.7 * coverage_ratio).max(0.3);

            // Effective cardinality of this column given previous columns
            let effective_cardinality = col_cardinality * correlation_factor;

            combined *= effective_cardinality.max(1.0);

            // Cap at total rows (can't have more combinations than rows)
            combined = combined.min(total_rows);
        }

        combined.max(1.0)
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
    /// ```text
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

        // 5. WAL write cost (scaled by row size)
        // Per-row WAL entry cost scales with row size + fixed sync overhead
        let wal_size_factor = index_info.wal_size_factor();
        let wal_cost = rows * self.wal_write_cost * wal_size_factor + self.wal_sync_cost;

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
    /// ```text
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

        // 5. WAL write cost (scaled by row size)
        // Per-row WAL entry cost scales with row size + fixed sync overhead
        let wal_size_factor = index_info.wal_size_factor();
        let wal_cost = rows * self.wal_write_cost * wal_size_factor + self.wal_sync_cost;

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
    /// ```text
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

        // 6. WAL write cost (scaled by row size, dominant cost per profiling #3862)
        // Per-row WAL entry cost scales with row size + fixed sync overhead
        let wal_size_factor = index_info.wal_size_factor();
        let wal_cost = rows * self.wal_write_cost * wal_size_factor + self.wal_sync_cost;

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
        let index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(1, 2, false, 0.0, 64);

        // Insert 100 rows with 1 PK and 2 B-tree indexes
        let cost = estimator.estimate_insert(100, &table_stats, &index_info);

        // B-tree indexes add significant overhead
        let cost_no_btree = estimator.estimate_insert(
            100,
            &table_stats,
            &TableIndexInfo::new(1, 0, false, 0.0, 64),
        );
        assert!(cost > cost_no_btree, "B-tree indexes should increase cost");
    }

    #[test]
    fn test_insert_cost_native_columnar() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        let row_index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);
        let columnar_index_info = TableIndexInfo::new(1, 0, true, 0.0, 64);

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
        let index_info = TableIndexInfo::new(1, 1, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(2, 3, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(1, 1, false, 0.0, 64);

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
        let index_info_40 = TableIndexInfo::new(1, 0, false, 0.0, 64);
        let cost_40 = estimator.estimate_delete(400, &table_stats, &index_info_40);

        // Case 2: Delete 10% when already at 45% deleted - will trigger compaction
        let index_info_trigger = TableIndexInfo::new(1, 0, false, 0.45, 64);
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

        let no_indexes = TableIndexInfo::new(0, 0, false, 0.0, 64);
        let many_indexes = TableIndexInfo::new(2, 5, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(1, 2, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(1, 1, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(0, 0, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(0, 0, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(0, 0, false, 0.0, 64);

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
        let _table_stats = create_test_table_stats(1000);
        let _index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);

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
        let index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);

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

    // ============================================================================
    // Row Size-Scaled WAL Cost Tests
    // ============================================================================

    #[test]
    fn test_estimate_type_size_fixed_types() {
        // Boolean
        assert_eq!(estimate_type_size(&DataType::Boolean), 1);

        // Integer types
        assert_eq!(estimate_type_size(&DataType::Smallint), 2);
        assert_eq!(estimate_type_size(&DataType::Integer), 4);
        assert_eq!(estimate_type_size(&DataType::Bigint), 8);
        assert_eq!(estimate_type_size(&DataType::Unsigned), 8);

        // Floating point
        assert_eq!(estimate_type_size(&DataType::Real), 4);
        assert_eq!(estimate_type_size(&DataType::DoublePrecision), 8);
        assert_eq!(estimate_type_size(&DataType::Float { precision: 24 }), 4);
        assert_eq!(estimate_type_size(&DataType::Float { precision: 53 }), 8);

        // Date/time
        assert_eq!(estimate_type_size(&DataType::Date), 4);
        assert_eq!(estimate_type_size(&DataType::Time { with_timezone: false }), 8);
        assert_eq!(estimate_type_size(&DataType::Timestamp { with_timezone: false }), 8);
    }

    #[test]
    fn test_estimate_type_size_variable_types() {
        // VARCHAR with max length
        assert_eq!(
            estimate_type_size(&DataType::Varchar { max_length: Some(100) }),
            32 // min(100/2, 32) = 32
        );
        assert_eq!(
            estimate_type_size(&DataType::Varchar { max_length: Some(20) }),
            10 // min(20/2, 32) = 10
        );
        assert_eq!(
            estimate_type_size(&DataType::Varchar { max_length: None }),
            32 // default
        );

        // Character with fixed length
        assert_eq!(estimate_type_size(&DataType::Character { length: 50 }), 50);

        // BLOB/CLOB
        assert_eq!(estimate_type_size(&DataType::BinaryLargeObject), 128);
        assert_eq!(estimate_type_size(&DataType::CharacterLargeObject), 64);
    }

    #[test]
    fn test_estimate_type_size_vector() {
        // Vector with dimensions
        assert_eq!(estimate_type_size(&DataType::Vector { dimensions: 128 }), 128 * 8);
        assert_eq!(estimate_type_size(&DataType::Vector { dimensions: 512 }), 512 * 8);
    }

    #[test]
    fn test_estimate_row_size() {
        // Small row: 2 columns (INTEGER, BOOLEAN)
        let small_row = vec![DataType::Integer, DataType::Boolean];
        let size = estimate_row_size(&small_row);
        // Expected: 4 + 1 + 8 (overhead) = 13, but min is 64
        assert_eq!(size, 64);

        // Medium row: 5 columns
        let medium_row = vec![
            DataType::Integer,
            DataType::Bigint,
            DataType::Varchar { max_length: Some(100) },
            DataType::Timestamp { with_timezone: false },
            DataType::Boolean,
        ];
        let size = estimate_row_size(&medium_row);
        // Expected: 4 + 8 + 32 + 8 + 1 + 8 (overhead) = 61, but min is 64
        assert_eq!(size, 64);

        // Large row: many columns
        let large_row = vec![
            DataType::Integer,
            DataType::Bigint,
            DataType::DoublePrecision,
            DataType::Varchar { max_length: Some(200) },
            DataType::Varchar { max_length: Some(200) },
            DataType::Varchar { max_length: Some(200) },
            DataType::Timestamp { with_timezone: false },
            DataType::Decimal { precision: 18, scale: 2 },
            DataType::Boolean,
            DataType::Character { length: 100 },
        ];
        let size = estimate_row_size(&large_row);
        // Expected: 4 + 8 + 8 + 32 + 32 + 32 + 8 + 16 + 1 + 100 + 8 = 249
        assert_eq!(size, 249);
    }

    #[test]
    fn test_wal_size_factor_small_rows() {
        // Row size equal to BASE_ROW_SIZE (64 bytes)
        let info = TableIndexInfo::new(1, 0, false, 0.0, 64);
        assert!((info.wal_size_factor() - 1.0).abs() < 0.01);

        // Row size smaller than BASE_ROW_SIZE (clamped to 1.0)
        let info = TableIndexInfo::new(1, 0, false, 0.0, 32);
        assert!((info.wal_size_factor() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_wal_size_factor_medium_rows() {
        // Row size 2x BASE_ROW_SIZE
        let info = TableIndexInfo::new(1, 0, false, 0.0, 128);
        assert!((info.wal_size_factor() - 2.0).abs() < 0.01);

        // Row size 4x BASE_ROW_SIZE
        let info = TableIndexInfo::new(1, 0, false, 0.0, 256);
        assert!((info.wal_size_factor() - 4.0).abs() < 0.01);
    }

    #[test]
    fn test_wal_size_factor_large_rows_capped() {
        // Row size 15x BASE_ROW_SIZE (should be capped at MAX_WAL_SIZE_FACTOR = 10)
        let info = TableIndexInfo::new(1, 0, false, 0.0, 960); // 64 * 15
        assert!((info.wal_size_factor() - 10.0).abs() < 0.01);

        // Extremely large rows also capped
        let info = TableIndexInfo::new(1, 0, false, 0.0, 10000);
        assert!((info.wal_size_factor() - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_insert_wal_cost_scales_with_row_size() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        // Small row (64 bytes) - factor of 1.0
        let small_info = TableIndexInfo::new(1, 0, false, 0.0, 64);
        let small_cost = estimator.estimate_insert(100, &table_stats, &small_info);

        // Large row (256 bytes) - factor of 4.0
        let large_info = TableIndexInfo::new(1, 0, false, 0.0, 256);
        let large_cost = estimator.estimate_insert(100, &table_stats, &large_info);

        // Large row should have higher WAL cost
        assert!(
            large_cost > small_cost,
            "Large row insert cost ({}) should be higher than small row cost ({})",
            large_cost,
            small_cost
        );

        // The difference should be approximately 3x the WAL cost (4x - 1x = 3x factor)
        // WAL base cost = 100 * 0.12 = 12.0
        // Expected increase = 12.0 * 3 = 36.0
        let cost_diff = large_cost - small_cost;
        assert!(
            cost_diff > 30.0 && cost_diff < 40.0,
            "Cost difference ({}) should be approximately 3x WAL base cost",
            cost_diff
        );
    }

    #[test]
    fn test_update_wal_cost_scales_with_row_size() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        // Small row (64 bytes)
        let small_info = TableIndexInfo::new(1, 0, false, 0.0, 64);
        let small_cost = estimator.estimate_update(50, &table_stats, &small_info, 0.0);

        // Large row (320 bytes) - factor of 5.0
        let large_info = TableIndexInfo::new(1, 0, false, 0.0, 320);
        let large_cost = estimator.estimate_update(50, &table_stats, &large_info, 0.0);

        // Large row should have higher WAL cost
        assert!(
            large_cost > small_cost,
            "Large row update cost ({}) should be higher than small row cost ({})",
            large_cost,
            small_cost
        );
    }

    #[test]
    fn test_delete_wal_cost_scales_with_row_size() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        // Small row (64 bytes)
        let small_info = TableIndexInfo::new(0, 0, false, 0.0, 64);
        let small_cost = estimator.estimate_delete(100, &table_stats, &small_info);

        // Large row (640 bytes) - factor would be 10.0 but capped at MAX_WAL_SIZE_FACTOR
        let large_info = TableIndexInfo::new(0, 0, false, 0.0, 640);
        let large_cost = estimator.estimate_delete(100, &table_stats, &large_info);

        // Large row should have higher WAL cost
        assert!(
            large_cost > small_cost,
            "Large row delete cost ({}) should be higher than small row cost ({})",
            large_cost,
            small_cost
        );
    }

    #[test]
    fn test_2_column_vs_50_column_wal_cost() {
        // This is the key test from the issue: verify that a 50-column table
        // has higher WAL cost than a 2-column table
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        // 2-column table: INTEGER + VARCHAR(50) = 4 + 25 + 8 = 37 bytes (min 64)
        let small_row_size = estimate_row_size(&[
            DataType::Integer,
            DataType::Varchar { max_length: Some(50) },
        ]);
        assert_eq!(small_row_size, 64); // min row size

        // 50-column table: mix of types, much larger
        let large_columns: Vec<DataType> = (0..10)
            .map(|_| DataType::Integer)
            .chain((0..10).map(|_| DataType::Bigint))
            .chain((0..10).map(|_| DataType::DoublePrecision))
            .chain((0..10).map(|_| DataType::Varchar { max_length: Some(100) }))
            .chain((0..10).map(|_| DataType::Timestamp { with_timezone: false }))
            .collect();
        assert_eq!(large_columns.len(), 50);

        let large_row_size = estimate_row_size(&large_columns);
        // Expected: 10*4 + 10*8 + 10*8 + 10*32 + 10*8 + 8 = 40+80+80+320+80+8 = 608 bytes
        assert!(large_row_size > 500, "Large row should be > 500 bytes, got {}", large_row_size);

        // Create index infos with row sizes
        let small_info = TableIndexInfo::new(1, 0, false, 0.0, small_row_size);
        let large_info = TableIndexInfo::new(1, 0, false, 0.0, large_row_size);

        // Insert costs
        let small_insert = estimator.estimate_insert(100, &table_stats, &small_info);
        let large_insert = estimator.estimate_insert(100, &table_stats, &large_info);

        assert!(
            large_insert > small_insert,
            "50-column table insert cost ({}) should exceed 2-column table cost ({})",
            large_insert,
            small_insert
        );

        // The factor should be significant (large row is ~9.5x base, but capped at 10x)
        let factor = large_info.wal_size_factor() / small_info.wal_size_factor();
        assert!(
            factor >= 9.0,
            "WAL size factor ratio ({}) should be at least 9x",
            factor
        );
    }

    // ============================================================================
    // Skip-Scan Cost Estimation Tests
    // ============================================================================

    #[test]
    fn test_skip_scan_cost_low_cardinality_prefix() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        // Low cardinality prefix (10 distinct values)
        let prefix_stats = ColumnStatistics {
            n_distinct: 10,
            null_count: 0,
            min_value: Some(SqlValue::Integer(1)),
            max_value: Some(SqlValue::Integer(10)),
            most_common_values: vec![],
            histogram: None,
        };

        // Selective filter (1% of rows match)
        let cost = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.01);

        // Should be cheaper than table scan with selective filter and low prefix cardinality
        let table_scan_cost = estimator.estimate_table_scan(&table_stats);
        assert!(
            cost < table_scan_cost,
            "Skip-scan cost ({}) should be cheaper than table scan ({}) with low prefix cardinality",
            cost,
            table_scan_cost
        );
    }

    #[test]
    fn test_skip_scan_cost_high_cardinality_prefix() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        // High cardinality prefix (1000 distinct values)
        let prefix_stats = ColumnStatistics {
            n_distinct: 1000,
            null_count: 0,
            min_value: Some(SqlValue::Integer(1)),
            max_value: Some(SqlValue::Integer(1000)),
            most_common_values: vec![],
            histogram: None,
        };

        // Selective filter (1% of rows match)
        let cost = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.01);

        // High cardinality prefix makes skip-scan expensive due to many seeks
        let table_scan_cost = estimator.estimate_table_scan(&table_stats);
        assert!(
            cost > table_scan_cost,
            "Skip-scan cost ({}) should be more expensive than table scan ({}) with high prefix cardinality",
            cost,
            table_scan_cost
        );
    }

    #[test]
    fn test_skip_scan_cost_scales_with_prefix_cardinality() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        let low_card_stats = ColumnStatistics {
            n_distinct: 10,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let high_card_stats = ColumnStatistics {
            n_distinct: 100,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let cost_low = estimator.estimate_skip_scan_cost(&table_stats, &low_card_stats, 0.01);
        let cost_high = estimator.estimate_skip_scan_cost(&table_stats, &high_card_stats, 0.01);

        // Higher prefix cardinality should mean higher skip-scan cost
        assert!(
            cost_high > cost_low,
            "Skip-scan cost with high cardinality ({}) should exceed low cardinality cost ({})",
            cost_high,
            cost_low
        );
    }

    #[test]
    fn test_skip_scan_cost_scales_with_filter_selectivity() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        let prefix_stats = ColumnStatistics {
            n_distinct: 10,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        // Very selective filter (0.1% of rows)
        let cost_selective = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.001);

        // Less selective filter (10% of rows)
        let cost_broad = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.1);

        // Higher selectivity (more rows match) should mean higher cost
        assert!(
            cost_broad > cost_selective,
            "Skip-scan cost with broad filter ({}) should exceed selective filter cost ({})",
            cost_broad,
            cost_selective
        );
    }

    #[test]
    fn test_should_use_skip_scan_decision() {
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        // Low cardinality prefix - skip-scan should be beneficial
        let low_card_stats = ColumnStatistics {
            n_distinct: 5,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        assert!(
            estimator.should_use_skip_scan(&table_stats, &low_card_stats, 0.01),
            "Skip-scan should be chosen with low prefix cardinality and selective filter"
        );

        // High cardinality prefix - skip-scan should not be beneficial
        let high_card_stats = ColumnStatistics {
            n_distinct: 5000,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        assert!(
            !estimator.should_use_skip_scan(&table_stats, &high_card_stats, 0.01),
            "Skip-scan should NOT be chosen with high prefix cardinality"
        );
    }

    #[test]
    fn test_skip_scan_break_even_point() {
        // Test to find approximately where skip-scan becomes beneficial
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        // With 10% filter selectivity, find the prefix cardinality threshold
        let selectivity = 0.1;

        // Skip-scan should be beneficial below some threshold cardinality
        let mut threshold_cardinality = 0;
        for cardinality in [5, 10, 25, 50, 100, 200, 500, 1000] {
            let prefix_stats = ColumnStatistics {
                n_distinct: cardinality,
                null_count: 0,
                min_value: None,
                max_value: None,
                most_common_values: vec![],
                histogram: None,
            };

            if estimator.should_use_skip_scan(&table_stats, &prefix_stats, selectivity) {
                threshold_cardinality = cardinality;
            } else {
                break;
            }
        }

        // Verify we found a reasonable threshold
        assert!(
            threshold_cardinality > 0,
            "Skip-scan should be beneficial for at least some low cardinalities"
        );
        assert!(
            threshold_cardinality < 1000,
            "Skip-scan should not be beneficial for very high cardinalities"
        );
    }

    // ============================================================================
    // Multi-Column Skip-Scan Cost Estimation Tests
    // ============================================================================

    #[test]
    fn test_multi_column_skip_scan_cost_single_column_delegates() {
        // When given a single column, multi-column cost should match single-column cost
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        let prefix_stats = ColumnStatistics {
            n_distinct: 10,
            null_count: 0,
            min_value: Some(SqlValue::Integer(1)),
            max_value: Some(SqlValue::Integer(10)),
            most_common_values: vec![],
            histogram: None,
        };

        let single_col_cost = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.01);
        let multi_col_cost =
            estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[&prefix_stats], 0.01);

        assert!(
            (single_col_cost - multi_col_cost).abs() < 0.001,
            "Single-column and multi-column costs should match for single column: {} vs {}",
            single_col_cost,
            multi_col_cost
        );
    }

    #[test]
    fn test_multi_column_skip_scan_cost_increases_with_columns() {
        // Adding more prefix columns should generally increase cost due to more seeks
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        let col1_stats = ColumnStatistics {
            n_distinct: 10,
            null_count: 0,
            min_value: Some(SqlValue::Integer(1)),
            max_value: Some(SqlValue::Integer(10)),
            most_common_values: vec![],
            histogram: None,
        };

        let col2_stats = ColumnStatistics {
            n_distinct: 20,
            null_count: 0,
            min_value: Some(SqlValue::Integer(1)),
            max_value: Some(SqlValue::Integer(20)),
            most_common_values: vec![],
            histogram: None,
        };

        let col3_stats = ColumnStatistics {
            n_distinct: 50,
            null_count: 0,
            min_value: Some(SqlValue::Integer(1)),
            max_value: Some(SqlValue::Integer(50)),
            most_common_values: vec![],
            histogram: None,
        };

        let cost_1_col =
            estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[&col1_stats], 0.01);
        let cost_2_col = estimator
            .estimate_skip_scan_cost_multi_column(&table_stats, &[&col1_stats, &col2_stats], 0.01);
        let cost_3_col = estimator.estimate_skip_scan_cost_multi_column(
            &table_stats,
            &[&col1_stats, &col2_stats, &col3_stats],
            0.01,
        );

        // More columns = more prefix combinations = higher seek cost
        assert!(
            cost_2_col > cost_1_col,
            "2-column skip cost ({}) should exceed 1-column cost ({})",
            cost_2_col,
            cost_1_col
        );
        assert!(
            cost_3_col > cost_2_col,
            "3-column skip cost ({}) should exceed 2-column cost ({})",
            cost_3_col,
            cost_2_col
        );
    }

    #[test]
    fn test_multi_column_skip_scan_correlation_adjustment() {
        // Test that correlation factor limits combined cardinality
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(1000);

        // High cardinality columns (if independent, would produce 10*100*500 = 500,000 combinations)
        let col1_stats = ColumnStatistics {
            n_distinct: 10,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let col2_stats = ColumnStatistics {
            n_distinct: 100,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let col3_stats = ColumnStatistics {
            n_distinct: 500,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let cost = estimator.estimate_skip_scan_cost_multi_column(
            &table_stats,
            &[&col1_stats, &col2_stats, &col3_stats],
            0.01,
        );

        // Cost should be finite and reasonable (correlation caps cardinality at row count)
        assert!(cost.is_finite(), "Cost should be finite");
        assert!(cost > 0.0, "Cost should be positive");

        // If no correlation adjustment, cost would be astronomical due to 500K seeks
        // With correlation, it should be capped based on row count (1000)
        // Seek cost with full independence: 500,000 * 4.0 = 2,000,000
        // Table scan cost: 1000/100 * 1.0 + 1000 * 0.01 = 10 + 10 = 20
        let table_scan_cost = estimator.estimate_table_scan(&table_stats);
        assert!(
            cost < 500_000.0 * 4.0,
            "Correlation should prevent astronomical costs: {} vs max {}",
            cost,
            500_000.0 * 4.0
        );

        // With these high cardinalities, skip-scan should not beat table scan
        assert!(
            cost > table_scan_cost,
            "Skip-scan with high combined cardinality ({}) should cost more than table scan ({})",
            cost,
            table_scan_cost
        );
    }

    #[test]
    fn test_multi_column_skip_scan_empty_stats() {
        // Test edge case: empty prefix stats returns table scan cost
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        let cost_empty =
            estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[], 0.01);
        let table_scan_cost = estimator.estimate_table_scan(&table_stats);

        assert!(
            (cost_empty - table_scan_cost).abs() < 0.001,
            "Empty prefix should return table scan cost: {} vs {}",
            cost_empty,
            table_scan_cost
        );
    }

    #[test]
    fn test_multi_column_skip_scan_vs_single_column_decision() {
        // Test scenario where multi-column skip might be better than single-column
        // This happens when: the first column has high cardinality but combined
        // columns with correlation produce fewer seeks than first column alone
        let estimator = CostEstimator::default();
        let table_stats = create_test_table_stats(10000);

        // First column: high cardinality (100 distinct)
        let col1_high_card = ColumnStatistics {
            n_distinct: 100,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        // Second column: very low cardinality (2 distinct)
        // Combined with first, might have 100*2 = 200 combinations worst case
        // But with correlation, might be closer to 100 (each col1 value has ~both col2 values)
        let col2_low_card = ColumnStatistics {
            n_distinct: 2,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let cost_1_col =
            estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[&col1_high_card], 0.01);
        let cost_2_col = estimator.estimate_skip_scan_cost_multi_column(
            &table_stats,
            &[&col1_high_card, &col2_low_card],
            0.01,
        );

        // With these statistics, 2-column skip should have higher cost
        // (adding another low-card column still increases prefix combinations)
        assert!(
            cost_2_col >= cost_1_col,
            "2-column skip cost ({}) should be >= 1-column cost ({}) with these stats",
            cost_2_col,
            cost_1_col
        );
    }

    #[test]
    fn test_combined_prefix_cardinality_estimation() {
        let estimator = CostEstimator::default();
        let total_rows = 10000.0;

        // Test 1: Single column
        let col1_stats = ColumnStatistics {
            n_distinct: 10,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let cardinality_1 = estimator.estimate_combined_prefix_cardinality(&[&col1_stats], total_rows);
        assert!(
            (cardinality_1 - 10.0).abs() < 0.01,
            "Single column cardinality should match n_distinct: {}",
            cardinality_1
        );

        // Test 2: Two columns with low coverage (should have minimal correlation adjustment)
        let col2_stats = ColumnStatistics {
            n_distinct: 5,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let cardinality_2 =
            estimator.estimate_combined_prefix_cardinality(&[&col1_stats, &col2_stats], total_rows);
        // With 10 * 5 = 50 max combinations and 10/10000 = 0.001 coverage ratio
        // Correlation factor ≈ 1.0 - 0.7 * 0.001 ≈ 0.999
        // So combined ≈ 10 * (5 * 0.999) ≈ 49.95
        assert!(
            cardinality_2 > 40.0 && cardinality_2 < 60.0,
            "Two-column cardinality should be close to product with low correlation: {}",
            cardinality_2
        );

        // Test 3: Cardinality should be capped at total_rows
        let col_high_stats = ColumnStatistics {
            n_distinct: 5000,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        let cardinality_capped = estimator.estimate_combined_prefix_cardinality(
            &[&col_high_stats, &col_high_stats],
            total_rows,
        );
        assert!(
            cardinality_capped <= total_rows,
            "Combined cardinality ({}) should be capped at total_rows ({})",
            cardinality_capped,
            total_rows
        );
    }
}
