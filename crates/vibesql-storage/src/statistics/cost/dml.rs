//! DML cost estimation
//!
//! This module contains cost estimation for DML operations:
//! - INSERT (row storage + index maintenance)
//! - UPDATE (row modification + selective index updates)
//! - DELETE (bitmap marking + index removal + potential compaction)
//!
//! Costs are estimated in arbitrary units representing relative work,
//! not absolute time. The optimizer uses these costs to compare different
//! execution strategies and choose the most efficient one.

use super::estimator::CostEstimator;
use super::types::TableIndexInfo;
use crate::statistics::TableStatistics;

impl CostEstimator {
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
    /// * `indexes_affected_ratio` - Fraction of indexes affected by column changes (0.0 to 1.0) Use
    ///   1.0 if all indexed columns might change, or a lower value for selective updates
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
            let rebuild_cost =
                remaining_rows * self.cpu_tuple_cost * self.compaction_cost_multiplier;
            let hash_rebuild_cost =
                remaining_rows * index_info.hash_index_count as f64 * self.hash_index_update_cost;
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
}
