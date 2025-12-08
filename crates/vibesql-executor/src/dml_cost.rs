//! DML Cost-Based Optimization
//!
//! This module provides cost-based optimization decisions for DML operations (INSERT, UPDATE, DELETE).
//! It uses the cost estimation infrastructure from vibesql-storage to make runtime decisions about:
//! - Batch sizes for INSERT operations
//! - Strategy selection for DELETE operations
//! - Index update optimization for UPDATE operations
//!
//! # Usage
//!
//! The `DmlOptimizer` provides methods to compute optimization hints based on estimated costs:
//!
//! ```rust,no_run
//! use vibesql_executor::DmlOptimizer;
//! use vibesql_storage::Database;
//! use std::collections::HashSet;
//!
//! let db = Database::new();
//! let optimizer = DmlOptimizer::new(&db, "table_name");
//! let total_rows = 1000;
//! let batch_size = optimizer.optimal_insert_batch_size(total_rows);
//! let should_chunk = optimizer.should_chunk_delete(total_rows);
//! // Note: compute_indexes_affected_ratio requires a TableSchema
//! ```

use vibesql_storage::{
    statistics::{CostEstimator, TableIndexInfo, TableStatistics},
    Database,
};

/// Cost thresholds for optimization decisions
/// These are tuned based on TPC-C and Sysbench profiling data
pub mod thresholds {
    /// Cost per row above which we use smaller batch sizes for INSERT
    /// High-index tables (>3 B-tree indexes) benefit from smaller batches
    pub const HIGH_COST_INSERT_THRESHOLD: f64 = 0.5;

    /// Maximum batch size for high-cost tables (many indexes)
    pub const SMALL_BATCH_SIZE: usize = 100;

    /// Default batch size for low-cost tables
    pub const LARGE_BATCH_SIZE: usize = 1000;

    /// Deleted ratio above which early compaction may be beneficial
    pub const HIGH_DELETED_RATIO_THRESHOLD: f64 = 0.4;

    /// Row count above which chunked deletion should be considered
    pub const CHUNK_DELETE_ROW_THRESHOLD: usize = 10000;

    /// Chunk size for large deletes to avoid long locks
    pub const DELETE_CHUNK_SIZE: usize = 1000;
}

/// DML cost-based optimizer
///
/// Provides optimization decisions for DML operations based on cost estimation.
pub struct DmlOptimizer<'a> {
    /// Cost estimator with default parameters
    cost_estimator: CostEstimator,
    /// Table index information for cost calculation
    index_info: Option<TableIndexInfo>,
    /// Table statistics (may be None for new/small tables)
    table_stats: Option<TableStatistics>,
    /// Reference to database for additional lookups
    #[allow(dead_code)]
    db: &'a Database,
    /// Table name being optimized
    table_name: &'a str,
}

impl<'a> DmlOptimizer<'a> {
    /// Create a new DML optimizer for a table
    ///
    /// # Arguments
    /// * `db` - Database reference for metadata lookups
    /// * `table_name` - Name of the table being operated on
    ///
    /// # Returns
    /// DmlOptimizer instance with cost estimation configured
    pub fn new(db: &'a Database, table_name: &'a str) -> Self {
        let index_info = db.get_table_index_info(table_name);
        let table_stats = db
            .get_table(table_name)
            .and_then(|t| t.get_statistics().cloned());

        Self {
            cost_estimator: CostEstimator::default(),
            index_info,
            table_stats,
            db,
            table_name,
        }
    }

    /// Get or compute table statistics with fallback for missing stats
    ///
    /// When actual statistics are unavailable (new table, no ANALYZE run),
    /// this creates fallback statistics based on available metadata.
    pub fn get_stats_with_fallback(&self) -> TableStatistics {
        if let Some(ref stats) = self.table_stats {
            stats.clone()
        } else {
            // Create fallback statistics from available metadata
            self.create_fallback_stats()
        }
    }

    /// Create fallback statistics when actual stats are unavailable
    fn create_fallback_stats(&self) -> TableStatistics {
        // Get row count from table if available
        let row_count = self
            .db
            .get_table(self.table_name)
            .map(|t| t.row_count())
            .unwrap_or(0);

        // Create minimal statistics with all required fields
        TableStatistics {
            row_count,
            columns: std::collections::HashMap::new(),
            last_updated: instant::SystemTime::now(),
            is_stale: true, // Mark as stale since it's a fallback
            sample_metadata: None,
            avg_row_bytes: None, // No actual data sampled
        }
    }

    /// Determine optimal batch size for INSERT operations
    ///
    /// Uses cost estimation to decide between small and large batch sizes.
    /// High-cost tables (many indexes) benefit from smaller batches to avoid
    /// index maintenance overhead accumulating.
    ///
    /// # Arguments
    /// * `total_rows` - Total number of rows to insert
    ///
    /// # Returns
    /// Recommended batch size
    pub fn optimal_insert_batch_size(&self, total_rows: usize) -> usize {
        // If we don't have index info, use default large batch
        let index_info = match &self.index_info {
            Some(info) => info,
            None => return thresholds::LARGE_BATCH_SIZE.min(total_rows),
        };

        // Calculate estimated cost per row for a single insert
        let stats = self.get_stats_with_fallback();
        let single_row_cost = self.cost_estimator.estimate_insert(1, &stats, index_info);

        // Log cost for debugging (controlled by DML_COST_DEBUG env var)
        if std::env::var("DML_COST_DEBUG").is_ok() {
            eprintln!(
                "DML_COST_DEBUG: INSERT on {} - cost_per_row={:.3}, hash_indexes={}, btree_indexes={}",
                self.table_name,
                single_row_cost,
                index_info.hash_index_count,
                index_info.btree_index_count
            );
        }

        // High-cost tables benefit from smaller batches
        if single_row_cost > thresholds::HIGH_COST_INSERT_THRESHOLD {
            thresholds::SMALL_BATCH_SIZE.min(total_rows)
        } else {
            thresholds::LARGE_BATCH_SIZE.min(total_rows)
        }
    }

    /// Determine if DELETE should be chunked to avoid long locks
    ///
    /// Large deletes on high-cost tables can cause long pauses due to
    /// index maintenance. Chunking allows other operations to proceed
    /// between chunks.
    ///
    /// # Arguments
    /// * `rows_to_delete` - Number of rows to be deleted
    ///
    /// # Returns
    /// `true` if delete should be chunked, `false` for single operation
    pub fn should_chunk_delete(&self, rows_to_delete: usize) -> bool {
        // Don't chunk small deletes
        if rows_to_delete < thresholds::CHUNK_DELETE_ROW_THRESHOLD {
            return false;
        }

        let index_info = match &self.index_info {
            Some(info) => info,
            None => return false,
        };

        // Calculate delete cost
        let stats = self.get_stats_with_fallback();
        let delete_cost =
            self.cost_estimator.estimate_delete(rows_to_delete, &stats, index_info);

        // Log cost for debugging
        if std::env::var("DML_COST_DEBUG").is_ok() {
            eprintln!(
                "DML_COST_DEBUG: DELETE on {} - rows={}, cost={:.3}, deleted_ratio={:.2}",
                self.table_name, rows_to_delete, delete_cost, index_info.deleted_ratio
            );
        }

        // Chunk if:
        // 1. Many rows AND high deleted ratio (compaction likely)
        // 2. Many rows AND many indexes
        let high_deleted_ratio = index_info.deleted_ratio > thresholds::HIGH_DELETED_RATIO_THRESHOLD;
        let many_indexes = index_info.btree_index_count >= 3;

        high_deleted_ratio || many_indexes
    }

    /// Get recommended chunk size for chunked deletes
    pub fn delete_chunk_size(&self) -> usize {
        thresholds::DELETE_CHUNK_SIZE
    }

    /// Check if early compaction should be triggered
    ///
    /// Based on the current deleted ratio, determines if compaction
    /// should be triggered sooner than the default 50% threshold.
    ///
    /// # Returns
    /// `true` if early compaction is recommended
    pub fn should_trigger_early_compaction(&self) -> bool {
        let index_info = match &self.index_info {
            Some(info) => info,
            None => return false,
        };

        // Consider early compaction if deleted ratio is high and we have many indexes
        // Compaction rebuilds all indexes, so it's costly but reduces ongoing overhead
        index_info.deleted_ratio > thresholds::HIGH_DELETED_RATIO_THRESHOLD
            && index_info.btree_index_count >= 2
    }

    /// Compute the ratio of indexes affected by an UPDATE operation
    ///
    /// This is used for UPDATE cost estimation. If the update only touches
    /// non-indexed columns, the ratio is 0.0 and index maintenance is skipped.
    ///
    /// # Arguments
    /// * `changed_columns` - Set of column indices being modified
    /// * `schema` - Table schema for looking up index column info
    ///
    /// # Returns
    /// Ratio of indexes affected (0.0 to 1.0)
    pub fn compute_indexes_affected_ratio(
        &self,
        changed_columns: &std::collections::HashSet<usize>,
        schema: &vibesql_catalog::TableSchema,
    ) -> f64 {
        let index_info = match &self.index_info {
            Some(info) => info,
            None => return 0.0,
        };

        let total_indexes =
            index_info.hash_index_count + index_info.btree_index_count;
        if total_indexes == 0 {
            return 0.0;
        }

        let mut affected_indexes = 0;

        // Check PK (counts as 1 hash index)
        if let Some(pk_indices) = schema.get_primary_key_indices() {
            if pk_indices.iter().any(|i| changed_columns.contains(i)) {
                affected_indexes += 1;
            }
        }

        // Check unique constraints (each is a hash index)
        for unique_cols in &schema.unique_constraints {
            let unique_indices: Vec<usize> = unique_cols
                .iter()
                .filter_map(|name| schema.get_column_index(name))
                .collect();
            if unique_indices.iter().any(|i| changed_columns.contains(i)) {
                affected_indexes += 1;
            }
        }

        // For B-tree indexes, we need to check against user-defined indexes
        // This is done at the database level via has_index_on_column
        let changed_column_names: Vec<String> = changed_columns
            .iter()
            .filter_map(|&i| schema.columns.get(i).map(|c| c.name.clone()))
            .collect();

        for col_name in &changed_column_names {
            if self.db.has_index_on_column(self.table_name, col_name) {
                affected_indexes += 1;
                break; // Count each B-tree index only once
            }
        }

        affected_indexes as f64 / total_indexes as f64
    }

    /// Estimate the cost of an UPDATE operation
    ///
    /// # Arguments
    /// * `row_count` - Number of rows to update
    /// * `indexes_affected_ratio` - Ratio of indexes affected (from compute_indexes_affected_ratio)
    ///
    /// # Returns
    /// Estimated cost in arbitrary units
    pub fn estimate_update_cost(&self, row_count: usize, indexes_affected_ratio: f64) -> f64 {
        let index_info = match &self.index_info {
            Some(info) => info,
            None => return 0.0,
        };

        let stats = self.get_stats_with_fallback();
        let cost = self
            .cost_estimator
            .estimate_update(row_count, &stats, index_info, indexes_affected_ratio);

        if std::env::var("DML_COST_DEBUG").is_ok() {
            eprintln!(
                "DML_COST_DEBUG: UPDATE on {} - rows={}, affected_ratio={:.2}, cost={:.3}",
                self.table_name, row_count, indexes_affected_ratio, cost
            );
        }

        cost
    }

    /// Get the current table index info (for external use)
    pub fn index_info(&self) -> Option<&TableIndexInfo> {
        self.index_info.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn create_test_db_with_table(
        table_name: &str,
        with_pk: bool,
        btree_index_count: usize,
    ) -> Database {
        let mut db = Database::new();

        let schema = if with_pk {
            TableSchema::with_primary_key(
                table_name.to_string(),
                vec![
                    ColumnSchema::new("id".to_string(), DataType::Integer, false),
                    ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, false),
                    ColumnSchema::new("value".to_string(), DataType::Integer, true),
                ],
                vec!["id".to_string()],
            )
        } else {
            TableSchema::new(
                table_name.to_string(),
                vec![
                    ColumnSchema::new("id".to_string(), DataType::Integer, false),
                    ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, false),
                ],
            )
        };
        db.create_table(schema).unwrap();

        // Create user-defined B-tree indexes
        for i in 0..btree_index_count {
            db.create_index(
                format!("idx_{}_{}", table_name, i),
                table_name.to_string(),
                false,
                vec![vibesql_ast::IndexColumn {
                    column_name: "name".to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }],
            ).unwrap();
        }

        db
    }

    #[test]
    fn test_optimal_insert_batch_size_low_cost() {
        let db = create_test_db_with_table("test_table", true, 0);
        let optimizer = DmlOptimizer::new(&db, "test_table");

        // Table with just PK should return a reasonable batch size
        let batch_size = optimizer.optimal_insert_batch_size(5000);
        // Should return either SMALL_BATCH_SIZE or LARGE_BATCH_SIZE
        assert!(
            batch_size == thresholds::SMALL_BATCH_SIZE
                || batch_size == thresholds::LARGE_BATCH_SIZE
        );
    }

    #[test]
    fn test_optimal_insert_batch_size_high_cost() {
        let db = create_test_db_with_table("test_table", true, 5);
        let optimizer = DmlOptimizer::new(&db, "test_table");

        // Table with many B-tree indexes
        let batch_size = optimizer.optimal_insert_batch_size(5000);
        // Should return a valid batch size
        assert!(batch_size <= thresholds::LARGE_BATCH_SIZE);
        assert!(batch_size >= thresholds::SMALL_BATCH_SIZE);
    }

    #[test]
    fn test_optimal_insert_batch_size_more_indexes_smaller_batch() {
        // Table with many indexes should have equal or smaller batch than table with few indexes
        let db_few = create_test_db_with_table("table_few", true, 1);
        let db_many = create_test_db_with_table("table_many", true, 5);

        let optimizer_few = DmlOptimizer::new(&db_few, "table_few");
        let optimizer_many = DmlOptimizer::new(&db_many, "table_many");

        let batch_few = optimizer_few.optimal_insert_batch_size(5000);
        let batch_many = optimizer_many.optimal_insert_batch_size(5000);

        // More indexes should lead to equal or smaller batch size
        assert!(batch_many <= batch_few);
    }

    #[test]
    fn test_should_chunk_delete_small() {
        let db = create_test_db_with_table("test_table", true, 0);
        let optimizer = DmlOptimizer::new(&db, "test_table");

        // Small deletes should not be chunked
        assert!(!optimizer.should_chunk_delete(100));
    }

    #[test]
    fn test_compute_indexes_affected_ratio_no_indexes() {
        let db = create_test_db_with_table("test_table", false, 0);
        let optimizer = DmlOptimizer::new(&db, "test_table");

        let schema = db.catalog.get_table("test_table").unwrap();
        let changed_columns: std::collections::HashSet<usize> = [1].into_iter().collect();

        let ratio = optimizer.compute_indexes_affected_ratio(&changed_columns, schema);
        assert_eq!(ratio, 0.0);
    }

    #[test]
    fn test_compute_indexes_affected_ratio_pk_affected() {
        let db = create_test_db_with_table("test_table", true, 0);
        let optimizer = DmlOptimizer::new(&db, "test_table");

        let schema = db.catalog.get_table("test_table").unwrap();
        // Column 0 is the PK
        let changed_columns: std::collections::HashSet<usize> = [0].into_iter().collect();

        let ratio = optimizer.compute_indexes_affected_ratio(&changed_columns, schema);
        assert!(ratio > 0.0, "PK update should affect at least one index");
    }

    #[test]
    fn test_fallback_stats() {
        let db = create_test_db_with_table("test_table", true, 0);
        let optimizer = DmlOptimizer::new(&db, "test_table");

        // New table won't have computed statistics
        let stats = optimizer.get_stats_with_fallback();
        // Should return fallback stats with row_count = 0
        assert_eq!(stats.row_count, 0);
    }

    #[test]
    fn test_estimate_update_cost() {
        let db = create_test_db_with_table("test_table", true, 2);
        let optimizer = DmlOptimizer::new(&db, "test_table");

        // Full update (all indexes affected)
        let full_cost = optimizer.estimate_update_cost(100, 1.0);

        // Selective update (no indexes affected)
        let selective_cost = optimizer.estimate_update_cost(100, 0.0);

        assert!(
            full_cost > selective_cost,
            "Full update should cost more than selective"
        );
    }
}
