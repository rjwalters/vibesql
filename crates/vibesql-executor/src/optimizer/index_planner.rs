//! Centralized index planning and strategy selection
//!
//! This module provides a unified entry point for all index-related decision making,
//! consolidating logic that was previously scattered across multiple locations:
//! - index_scan/selection.rs - Index selection logic
//! - index_scan/predicate.rs - Predicate extraction
//! - index_scan/execution.rs - Index usage decisions
//! - executor/index_optimization/where_filter.rs - Legacy IN clause optimization
//!
//! The IndexPlanner provides a clean API for determining if and how to use indexes
//! for a given query, including:
//! - Which index to use (if any)
//! - What predicate can be pushed down to the index
//! - Whether the index fully satisfies the WHERE clause (optimization)
//! - Estimated cost for cost-based decisions
//! - Skip-scan optimization for non-prefix index usage

use vibesql_ast::{Expression, OrderByItem};
use vibesql_storage::statistics::CostEstimator;
use vibesql_storage::Database;

/// Centralized index planner for query optimization
///
/// The IndexPlanner consolidates all index-related decision making into a single
/// component, providing a clean separation between index planning and execution.
///
/// # Example
/// ```text
/// let planner = IndexPlanner::new(&database);
/// if let Some(plan) = planner.plan_index_usage("users", where_clause, order_by) {
///     // Use the index plan for execution
///     execute_with_index(&plan);
/// } else {
///     // Fall back to table scan
///     execute_table_scan();
/// }
/// ```
#[allow(dead_code)]
pub struct IndexPlanner<'a> {
    database: &'a Database,
}

/// Comprehensive plan for index usage
///
/// Contains all information needed to execute an index scan:
/// - Which index to use
/// - What predicate to push down
/// - Whether additional WHERE filtering is needed
/// - Estimated selectivity and cost
#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexPlan {
    /// Name of the index to use
    pub index_name: String,

    /// Whether the index fully satisfies the WHERE clause
    ///
    /// If true, no additional WHERE filtering is needed after the index scan.
    /// This is an important optimization that skips redundant predicate evaluation.
    pub fully_satisfies_where: bool,

    /// Columns that will be pre-sorted by the index scan
    ///
    /// If Some, the index scan produces rows in the order specified by these columns,
    /// allowing ORDER BY optimization (no separate sorting needed).
    pub sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,

    /// Estimated selectivity of the index predicate (0.0 to 1.0)
    ///
    /// Represents what fraction of rows are expected to match the index predicate.
    /// Used for cost-based optimization decisions.
    pub estimated_selectivity: f64,

    /// Whether this plan uses skip-scan strategy
    ///
    /// If true, the index is being used with skip-scan because the WHERE clause
    /// filters on non-prefix columns. The skip_scan_info field contains details.
    pub is_skip_scan: bool,

    /// Skip-scan specific information (only set when is_skip_scan is true)
    pub skip_scan_info: Option<SkipScanInfo>,
}

/// Information about a skip-scan optimization
///
/// Skip-scan enables using a composite index when the query doesn't filter on
/// the prefix columns. It works by iterating through distinct values of the
/// prefix columns and performing an index lookup for each.
///
/// # Example
/// For an index on `(region, date)` and query `WHERE date = '2024-01-01'`:
/// - `skip_columns` = 1 (skipping `region`)
/// - `prefix_column` = "region"
/// - `filter_column` = "date"
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SkipScanInfo {
    /// Number of leading index columns being skipped
    pub skip_columns: usize,

    /// Name of the prefix column(s) being skipped
    pub prefix_columns: Vec<String>,

    /// Name of the column used for filtering
    pub filter_column: String,

    /// Estimated number of distinct values in the skipped prefix
    ///
    /// This determines how many seek operations will be performed.
    /// Lower values mean skip-scan is more efficient.
    pub prefix_cardinality: usize,

    /// Estimated cost of the skip-scan operation
    pub estimated_cost: f64,
}

#[allow(dead_code)]
impl<'a> IndexPlanner<'a> {
    /// Create a new index planner for the given database
    pub fn new(database: &'a Database) -> Self {
        IndexPlanner { database }
    }

    /// Plan index usage for a query
    ///
    /// Analyzes the query (WHERE clause and ORDER BY) to determine if an index should
    /// be used and which index is best. Uses cost-based optimization when statistics
    /// are available, falls back to rule-based heuristics otherwise.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table being queried
    /// * `where_clause` - Optional WHERE clause predicate
    /// * `order_by` - Optional ORDER BY clause
    ///
    /// # Returns
    /// - `Some(IndexPlan)` if an index should be used
    /// - `None` if a table scan is more appropriate
    ///
    /// # Decision Criteria
    /// The planner uses indexes when:
    /// 1. WHERE clause references an indexed column (with or without ORDER BY)
    /// 2. ORDER BY references an indexed column (even without WHERE)
    /// 3. Both WHERE and ORDER BY can use the same index
    ///
    /// When multiple indexes are applicable, the planner chooses based on:
    /// - Cost estimates (if statistics available)
    /// - Heuristics (first applicable index)
    pub fn plan_index_usage(
        &self,
        table_name: &str,
        where_clause: Option<&Expression>,
        order_by: Option<&[OrderByItem]>,
    ) -> Option<IndexPlan> {
        // Use the existing cost-based selection from index_scan module
        // This delegates to the current implementation but provides a clean API boundary
        let (index_name, sorted_columns) =
            crate::select::scan::index_scan::cost_based_index_selection(
                table_name,
                where_clause,
                order_by,
                self.database,
            )?;

        // Determine if WHERE clause is fully satisfied by the index
        let fully_satisfies_where = if let Some(where_expr) = where_clause {
            // Get the indexed column name
            if let Some(index_metadata) = self.database.get_index(&index_name) {
                if let Some(first_col) = index_metadata.columns.first() {
                    crate::select::scan::index_scan::predicate::where_clause_fully_satisfied_by_index(
                        where_expr,
                        &first_col.column_name,
                    )
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            true // No WHERE clause means index scan returns all rows (fully satisfied)
        };

        // Estimate selectivity for cost-based decisions
        let estimated_selectivity = self.estimate_selectivity(&index_name, where_clause);

        Some(IndexPlan {
            index_name,
            fully_satisfies_where,
            sorted_columns,
            estimated_selectivity,
            is_skip_scan: false,
            skip_scan_info: None,
        })
    }

    /// Plan skip-scan usage for a query
    ///
    /// Skip-scan enables using a composite index when the WHERE clause filters on
    /// non-prefix columns. This is beneficial when:
    /// - The prefix columns have low cardinality (few distinct values)
    /// - The filter on non-prefix columns is selective
    ///
    /// # Multi-Column Skip-Scan
    ///
    /// For an index on (a, b, c, d) with a filter on column d, the planner evaluates
    /// multiple skip depths and chooses the one with minimum cost:
    /// - skip_columns=1: iterate distinct(a), seek each (a, d_val)
    /// - skip_columns=2: iterate distinct(a,b), seek each (a, b, d_val)
    /// - skip_columns=3: iterate distinct(a,b,c), seek each (a, b, c, d_val)
    ///
    /// # Arguments
    /// * `table_name` - Name of the table being queried
    /// * `where_clause` - WHERE clause predicate (required for skip-scan)
    ///
    /// # Returns
    /// - `Some(IndexPlan)` with `is_skip_scan = true` if skip-scan is beneficial
    /// - `None` if no beneficial skip-scan is found
    ///
    /// # Example
    /// For an index on `(region, date)` and query `WHERE date = '2024-01-01'`:
    /// - Regular index scan cannot be used (no filter on `region`)
    /// - Skip-scan iterates through distinct `region` values
    /// - For each region, seeks to that region's '2024-01-01' entries
    pub fn plan_skip_scan(
        &self,
        table_name: &str,
        where_clause: &Expression,
    ) -> Option<IndexPlan> {
        // Get table and statistics
        let table = self.database.get_table(table_name)?;
        let table_stats = table.get_statistics()?;

        if table_stats.needs_refresh() {
            return None; // Need fresh statistics for cost-based decisions
        }

        // Get all indexes for this table
        let indexes = self.database.list_indexes_for_table(table_name);

        let cost_estimator = CostEstimator::default();
        let table_scan_cost = cost_estimator.estimate_table_scan(table_stats);

        let mut best_skip_scan: Option<(String, SkipScanInfo, f64)> = None;

        for index_name in &indexes {
            if let Some(index_metadata) = self.database.get_index(index_name) {
                // Skip single-column indexes (no prefix to skip)
                if index_metadata.columns.len() < 2 {
                    continue;
                }

                let first_col = &index_metadata.columns[0];

                // Check if WHERE clause filters on the first column
                // If yes, regular index scan should be used, not skip-scan
                let filters_first_col =
                    crate::select::scan::index_scan::selection::expression_filters_column(
                        where_clause,
                        &first_col.column_name,
                    );

                if filters_first_col {
                    continue; // Regular index scan is better
                }

                // Check if WHERE clause filters on any non-first column
                for (filter_col_idx, filter_col) in index_metadata.columns.iter().enumerate().skip(1) {
                    let filters_this_col =
                        crate::select::scan::index_scan::selection::expression_filters_column(
                            where_clause,
                            &filter_col.column_name,
                        );

                    if !filters_this_col {
                        continue;
                    }

                    // Found a potential skip-scan: filter on column at index filter_col_idx
                    // Estimate filter selectivity on the filter column
                    let filter_col_stats = table_stats.columns.get(&filter_col.column_name);
                    let filter_selectivity = filter_col_stats
                        .map(|stats| {
                            crate::select::scan::index_scan::selection::estimate_selectivity(
                                where_clause,
                                &filter_col.column_name,
                                stats,
                            )
                        })
                        .unwrap_or(0.33);

                    // Evaluate all possible skip depths (1 to filter_col_idx)
                    // and choose the one with minimum cost
                    for skip_depth in 1..=filter_col_idx {
                        // Collect statistics for prefix columns being skipped
                        let prefix_stats: Vec<_> = index_metadata.columns[..skip_depth]
                            .iter()
                            .filter_map(|c| table_stats.columns.get(&c.column_name))
                            .collect();

                        // Skip if we don't have stats for all prefix columns
                        if prefix_stats.len() != skip_depth {
                            continue;
                        }

                        // Calculate skip-scan cost for this skip depth
                        let skip_scan_cost = if skip_depth == 1 {
                            // Use existing single-column cost model
                            cost_estimator.estimate_skip_scan_cost(
                                table_stats,
                                prefix_stats[0],
                                filter_selectivity,
                            )
                        } else {
                            // Use multi-column cost model
                            cost_estimator.estimate_skip_scan_cost_multi_column(
                                table_stats,
                                &prefix_stats,
                                filter_selectivity,
                            )
                        };

                        // Debug output
                        if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
                            let prefix_names: Vec<_> = index_metadata.columns[..skip_depth]
                                .iter()
                                .map(|c| c.column_name.as_str())
                                .collect();
                            eprintln!(
                                "[SKIP_SCAN] index={}, skip_depth={}, prefix_cols={:?}, filter_col={}, filter_selectivity={:.4}, skip_scan_cost={:.2}, table_scan_cost={:.2}",
                                index_name,
                                skip_depth,
                                prefix_names,
                                filter_col.column_name,
                                filter_selectivity,
                                skip_scan_cost,
                                table_scan_cost
                            );
                        }

                        // Only consider if cheaper than table scan
                        if skip_scan_cost >= table_scan_cost {
                            continue;
                        }

                        // Track the best skip-scan option across all indexes and skip depths
                        let is_better = match &best_skip_scan {
                            None => true,
                            Some((_, _, best_cost)) => skip_scan_cost < *best_cost,
                        };

                        if is_better {
                            let prefix_columns: Vec<String> = index_metadata.columns[..skip_depth]
                                .iter()
                                .map(|c| c.column_name.clone())
                                .collect();

                            // Estimate combined cardinality for multi-column prefix
                            let prefix_cardinality = if skip_depth == 1 {
                                prefix_stats[0].n_distinct
                            } else {
                                // Use approximate combined cardinality
                                let combined = cost_estimator.estimate_skip_scan_cost_multi_column(
                                    table_stats,
                                    &prefix_stats,
                                    1.0, // Use selectivity=1 to isolate cardinality effect
                                );
                                // Extract approximate cardinality from cost
                                // (rough estimate based on seek cost component)
                                (combined / cost_estimator.random_page_cost).ceil() as usize
                            };

                            let skip_info = SkipScanInfo {
                                skip_columns: skip_depth,
                                prefix_columns,
                                filter_column: filter_col.column_name.clone(),
                                prefix_cardinality,
                                estimated_cost: skip_scan_cost,
                            };

                            best_skip_scan = Some((index_name.clone(), skip_info, skip_scan_cost));
                        }
                    }

                    // Only consider first matching filter column for this index
                    break;
                }
            }
        }

        // Return the best skip-scan plan if found
        best_skip_scan.map(|(index_name, skip_info, _)| {
            // Estimate overall selectivity
            let estimated_selectivity = skip_info.prefix_cardinality as f64
                / table_stats.row_count.max(1) as f64
                * 0.33; // Rough estimate

            IndexPlan {
                index_name,
                fully_satisfies_where: false, // Skip-scan doesn't fully satisfy WHERE
                sorted_columns: None,         // Skip-scan doesn't preserve order
                estimated_selectivity,
                is_skip_scan: true,
                skip_scan_info: Some(skip_info),
            }
        })
    }

    /// Estimate selectivity of index predicate
    ///
    /// Returns a value between 0.0 (no rows) and 1.0 (all rows) representing
    /// the estimated fraction of rows that will match the index predicate.
    ///
    /// Uses column statistics when available, falls back to conservative defaults.
    fn estimate_selectivity(&self, index_name: &str, where_clause: Option<&Expression>) -> f64 {
        // If no WHERE clause, selectivity is 1.0 (all rows)
        let where_expr = match where_clause {
            Some(expr) => expr,
            None => return 1.0,
        };

        // Get index metadata to find the indexed column
        let index_metadata = match self.database.get_index(index_name) {
            Some(meta) => meta,
            None => return 0.33, // Conservative default
        };

        let first_col = match index_metadata.columns.first() {
            Some(col) => col,
            None => return 0.33,
        };

        // Get table statistics
        let table_name = &index_metadata.table_name;
        let table = match self.database.get_table(table_name) {
            Some(t) => t,
            None => return 0.33,
        };

        let table_stats = match table.get_statistics() {
            Some(stats) if !stats.needs_refresh() => stats,
            _ => return 0.33, // No stats or stale stats
        };

        // Get column statistics
        let col_stats = match table_stats.columns.get(&first_col.column_name) {
            Some(stats) => stats,
            None => return 0.33,
        };

        // Use the existing selectivity estimation logic
        crate::select::scan::index_scan::selection::estimate_selectivity(
            where_expr,
            &first_col.column_name,
            col_stats,
        )
    }

    /// Check if a specific index can be used for the given query
    ///
    /// This is a lower-level API for checking individual indexes.
    /// Most callers should use `plan_index_usage()` instead.
    pub fn can_use_index(
        &self,
        index_name: &str,
        where_clause: Option<&Expression>,
        order_by: Option<&[OrderByItem]>,
    ) -> bool {
        let index_metadata = match self.database.get_index(index_name) {
            Some(meta) => meta,
            None => return false,
        };

        let first_col = match index_metadata.columns.first() {
            Some(col) => col,
            None => return false,
        };

        // Check if index can be used for WHERE
        let can_use_for_where = where_clause
            .map(|expr| {
                crate::select::scan::index_scan::selection::expression_filters_column(
                    expr,
                    &first_col.column_name,
                )
            })
            .unwrap_or(false);

        // Check if index can be used for ORDER BY
        let can_use_for_order = order_by
            .map(|items| {
                crate::select::scan::index_scan::selection::can_use_index_for_order_by(
                    items,
                    &index_metadata.columns,
                )
            })
            .unwrap_or(false);

        can_use_for_where || can_use_for_order
    }
}

#[cfg(test)]
mod tests {
    // Note: Full integration tests are in the index_scan module tests.
    // These tests verify the IndexPlanner API surface.

    #[test]
    fn test_index_planner_new() {
        // This is a basic smoke test - full tests require a Database instance
        // and are better suited for integration tests
    }
}
