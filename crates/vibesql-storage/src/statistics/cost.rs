//! Cost estimation for query execution plans
//!
//! This module provides cost models for different access methods:
//! - Table scan (sequential scan)
//! - Index scan (B-tree lookup + random access)
//!
//! Costs are estimated in arbitrary units representing relative work,
//! not absolute time. The optimizer uses these costs to compare different
//! execution strategies and choose the most efficient one.

use super::{ColumnStatistics, TableStatistics};

/// Cost estimator for access methods (scans, index lookups)
///
/// Cost parameters are based on the PostgreSQL cost model:
/// - Sequential I/O is cheaper than random I/O
/// - Index scans have overhead for traversing the B-tree
/// - Cache effects are approximated by page costs
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
        }
    }
}

impl CostEstimator {
    /// Create a cost estimator with custom parameters
    pub fn new(
        seq_page_cost: f64,
        random_page_cost: f64,
        cpu_tuple_cost: f64,
        cpu_index_tuple_cost: f64,
    ) -> Self {
        Self {
            seq_page_cost,
            random_page_cost,
            cpu_tuple_cost,
            cpu_index_tuple_cost,
            rows_per_page: 100.0,
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
}
