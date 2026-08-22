//! Scan cost estimation
//!
//! This module contains cost estimation for different scan access methods:
//! - Table scan (sequential scan)
//! - Index scan (B-tree lookup + random access)
//! - Skip-scan (for composite indexes)

use super::{estimator::CostEstimator, types::AccessMethod};
use crate::statistics::{ColumnStatistics, TableStatistics};

impl CostEstimator {
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
        let rows_per_prefix =
            if prefix_cardinality > 0.0 { total_rows / prefix_cardinality } else { total_rows };

        // Expected rows matching filter within each prefix group
        let matching_rows_per_prefix = rows_per_prefix * filter_selectivity;

        // 1. Seek cost: one random I/O per distinct prefix value
        // This is the key benefit of skip-scan - we trade one seek per prefix
        // for potentially many fewer index entries scanned
        let seek_cost = prefix_cardinality * self.random_page_cost;

        // 2. Index scan cost within each prefix group
        // We scan index entries proportional to matching rows
        let index_scan_cost =
            prefix_cardinality * matching_rows_per_prefix * self.cpu_index_tuple_cost;

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
        let skip_scan_cost =
            self.estimate_skip_scan_cost(table_stats, prefix_col_stats, filter_selectivity);
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
            return self.estimate_skip_scan_cost(
                table_stats,
                prefix_col_stats[0],
                filter_selectivity,
            );
        }

        let total_rows = table_stats.row_count as f64;

        // Calculate combined cardinality for N prefix columns
        // Worst case: product of all distinct values (independent columns)
        // We apply a correlation factor to account for columns that are likely correlated
        let combined_cardinality =
            self.estimate_combined_prefix_cardinality(prefix_col_stats, total_rows);

        // Rows per prefix combination (assuming uniform distribution)
        let rows_per_prefix =
            if combined_cardinality > 0.0 { total_rows / combined_cardinality } else { total_rows };

        // Expected rows matching filter within each prefix group
        let matching_rows_per_prefix = rows_per_prefix * filter_selectivity;

        // 1. Seek cost: one random I/O per distinct prefix combination
        // More prefix columns means more seeks
        let seek_cost = combined_cardinality * self.random_page_cost;

        // 2. Index scan cost within each prefix group
        let index_scan_cost =
            combined_cardinality * matching_rows_per_prefix * self.cpu_index_tuple_cost;

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
    pub(crate) fn estimate_combined_prefix_cardinality(
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

    /// Choose an access method for an index whose leading column is an
    /// **expression** (e.g. `CREATE INDEX t1a1 ON t1(substr(a,1,12))`).
    ///
    /// Expression indexes carry no per-column statistics (we only collect stats
    /// for plain columns), so [`choose_access_method`] cannot cost them and would
    /// fall back to a table scan, leaving the expression index unable to compete
    /// in the planner's cost comparison. This variant lets the expression index
    /// participate: it synthesizes a conservative index-scan cost from the table
    /// statistics and a caller-supplied `selectivity` (typically the flat 0.33
    /// estimate VibeSQL uses for predicates with no column histogram), treating
    /// the index as if it had one distinct entry per row (the worst case for
    /// B-tree depth / entries scanned). If even that conservative estimate beats
    /// the table scan, the index wins.
    pub fn choose_access_method_no_col_stats(
        &self,
        table_stats: &TableStatistics,
        selectivity: f64,
    ) -> AccessMethod {
        let table_scan_cost = self.estimate_table_scan(table_stats);
        let index_scan_cost = self.estimate_index_scan_no_col_stats(table_stats, selectivity);

        if index_scan_cost < table_scan_cost {
            AccessMethod::IndexScan {
                estimated_cost: index_scan_cost,
                estimated_rows: (table_stats.row_count as f64 * selectivity) as usize,
            }
        } else {
            AccessMethod::TableScan { estimated_cost: table_scan_cost }
        }
    }

    /// Conservative index-scan cost when no column statistics are available
    /// (expression indexes). Mirrors [`estimate_index_scan`] but uses the table
    /// row count as a stand-in for the column's distinct-value count, which is
    /// the worst case for the index-traversal and entries-scanned terms.
    fn estimate_index_scan_no_col_stats(
        &self,
        table_stats: &TableStatistics,
        selectivity: f64,
    ) -> f64 {
        let row_count = table_stats.row_count as f64;
        let rows_fetched = row_count * selectivity;

        // Treat every row as a distinct index entry (no histogram available).
        let index_entries = row_count.max(1.0);
        let index_depth = (index_entries.log10() / 100_f64.log10()).ceil().max(1.0);
        let index_traversal_cost = index_depth * self.random_page_cost;

        let index_entries_scanned = index_entries * selectivity;
        let index_scan_cost = index_entries_scanned * self.cpu_index_tuple_cost;

        let table_fetch_cost = rows_fetched * self.random_page_cost;
        let cpu_cost = rows_fetched * self.cpu_tuple_cost;

        index_traversal_cost + index_scan_cost + table_fetch_cost + cpu_cost
    }
}
