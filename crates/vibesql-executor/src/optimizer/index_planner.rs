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

use vibesql_ast::{Expression, OrderByItem};
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

        Some(IndexPlan { index_name, fully_satisfies_where, sorted_columns, estimated_selectivity })
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
