//! FROM clause scanning logic
//!
//! Handles execution of FROM clauses including:
//! - Table scans (regular tables and CTEs)
//! - JOIN operations (delegates to join module)
//! - Derived tables (subqueries)
//! - Predicate pushdown for WHERE clause optimization
//! - Join order optimization (enabled by default for 3+ table joins)

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;

use super::{cte::CteResult, join::FromResult};
use crate::errors::ExecutorError;

// Strategy modules
mod derived;
pub(crate) mod index_scan;
mod join_scan;
mod predicates;
mod reorder;
mod table;

/// Execute a FROM clause (table, join, or subquery) and return combined schema and rows
///
/// This function handles all types of FROM clauses:
/// - Simple table references (with optional alias)
/// - CTEs (Common Table Expressions)
/// - JOIN operations (INNER, LEFT, RIGHT, FULL)
/// - Derived tables (subqueries with alias)
///
/// The WHERE clause is passed for predicate pushdown optimization:
/// - Table-local predicates are applied during table scan
/// - Equijoin predicates can be pushed into join operations
/// - Complex predicates remain in post-join WHERE
///
/// The ORDER BY clause is passed for index scan optimization:
/// - If an index matches the ORDER BY column, results can be returned pre-sorted
/// - This allows skipping expensive sorting in the SELECT executor
///
/// The LIMIT clause is passed for early termination optimization (#3253):
/// - When ORDER BY is satisfied by an index and no post-filter needed,
///   the index scan can stop early after fetching LIMIT rows
/// - This transforms O(all_matching_rows) to O(LIMIT)
///
/// Join reordering optimization (enabled by default):
/// - For multi-table joins (3-8 tables), analyzes join conditions
/// - Uses cost-based search to find optimal join order
/// - Minimizes intermediate result sizes
/// - Can be disabled via JOIN_REORDER_DISABLED environment variable
pub(super) fn execute_from_clause<F>(
    from: &vibesql_ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    limit: Option<usize>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&crate::schema::CombinedSchema>,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<super::SelectResult, ExecutorError> + Copy,
{
    // Check if this is a multi-table join that could benefit from reordering
    if matches!(from, vibesql_ast::FromClause::Join { .. }) {
        let table_count = reorder::count_tables_in_from(from);
        // Only apply reordering if:
        // 1. We have 2-8 tables (lowered from 3 to enable TPC-H Q19 optimization)
        // 2. All joins are CROSS (comma-list style: FROM t1, t2, t3)
        // 3. Not disabled via environment variable
        //
        // Note: We ONLY reorder comma-list syntax (CROSS joins) because reordering
        // changes column positions in results. Explicit JOIN syntax has defined
        // column ordering that must be preserved.
        //
        // 2-table joins benefit from choosing optimal build/probe sides when one
        // table has highly selective predicates.
        if reorder::should_apply_join_reordering(table_count) && reorder::all_joins_are_cross(from)
        {
            // Apply join reordering optimization
            return reorder::execute_with_join_reordering(
                from,
                cte_results,
                database,
                where_clause,
                outer_row,
                outer_schema,
                execute_subquery,
            );
        }

        // #3627: Handle SEMI/ANTI join at outer level with inner cross joins
        // Pattern: (cross_joins) SEMI/ANTI JOIN derived_table
        // This enables join reordering for the inner tables while applying
        // the semi-join filter early via the semi-join optimization path
        if let vibesql_ast::FromClause::Join {
            left,
            right,
            join_type: vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti,
            ..
        } = from
        {
            // Check if the LEFT side (inner tables) can benefit from reordering
            let inner_table_count = reorder::count_tables_in_from(left);
            if reorder::should_apply_join_reordering(inner_table_count)
                && reorder::all_joins_are_cross(left)
            {
                // The inner join can be reordered, and we have a semi/anti join
                // with a derived table. Use the extended optimization that includes
                // the derived table in the join reordering.
                if let vibesql_ast::FromClause::Subquery { .. } = right.as_ref() {
                    return reorder::execute_with_semi_join_reordering(
                        from,
                        cte_results,
                        database,
                        where_clause,
                        outer_row,
                        outer_schema,
                        execute_subquery,
                    );
                }
            }
        }
    }

    // Fall back to standard execution (recursive left-deep joins)
    match from {
        vibesql_ast::FromClause::Table { name, alias, column_aliases } => table::execute_table_scan(
            name,
            alias.as_ref(),
            column_aliases.as_ref(),
            cte_results,
            database,
            where_clause,
            order_by,
            limit,
            outer_row,
            outer_schema,
        ),
        vibesql_ast::FromClause::Join { left, right, join_type, condition, natural } => {
            join_scan::execute_join(
                left,
                right,
                join_type,
                condition,
                *natural,
                cte_results,
                database,
                where_clause,
                outer_row,
                outer_schema,
                execute_subquery,
            )
        }
        vibesql_ast::FromClause::Subquery { query, alias, column_aliases } => {
            derived::execute_derived_table(query, alias, column_aliases.as_ref(), execute_subquery)
        }
    }
}
