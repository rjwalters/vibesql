//! IN/NOT IN subquery to SEMI/ANTI join transformations
//!
//! This module handles converting IN and NOT IN subqueries into
//! SEMI and ANTI joins respectively.
//!
//! ## Examples
//!
//! ### IN → SEMI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### NOT IN → ANTI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey NOT IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders ANTI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### Aggregate IN → SEMI JOIN with Derived Table (TPC-H Q18)
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey IN (
//!     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
//! )
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN (
//!     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
//! ) AS __in_agg ON o_orderkey = __in_agg.l_orderkey
//! ```

use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType, SelectItem, SelectStmt};

use super::helpers::{is_self_join, qualify_outer_column_refs, rewrite_column_refs_with_alias};

/// Result of converting an IN subquery to a join
/// Contains the new FROM clause
pub(super) struct InToJoinResult {
    pub from: FromClause,
}

/// Try to convert an IN subquery to a SEMI or ANTI join
pub(super) fn try_convert_in_to_join(
    from: &FromClause,
    expr: &Expression,
    subquery: &SelectStmt,
    negated: bool,
) -> Option<InToJoinResult> {
    // Must have exactly one column in SELECT list
    if subquery.select_list.len() != 1 {
        return None;
    }

    let subquery_column = match &subquery.select_list[0] {
        SelectItem::Expression { expr, .. } => expr.clone(),
        _ => return None,
    };

    // Skip if subquery has LIMIT, OFFSET, or set operations (can't safely convert)
    if subquery.limit.is_some() || subquery.offset.is_some() || subquery.set_operation.is_some() {
        return None;
    }

    // Check if this is an aggregate subquery (GROUP BY or HAVING)
    // These need to be wrapped in a derived table for the semi-join
    let is_aggregate_subquery = subquery.group_by.is_some() || subquery.having.is_some();

    if is_aggregate_subquery {
        return try_convert_aggregate_in_to_join(from, expr, subquery, &subquery_column, negated);
    }

    // Simple subquery path: requires single table in FROM clause
    let (table_name, table_alias) = match &subquery.from {
        Some(FromClause::Table { name, alias, .. }) => (name.clone(), alias.clone()),
        _ => return None, // Complex FROM clause for non-aggregate, skip
    };

    // Detect self-join: check if subquery table name conflicts with outer query tables
    let needs_alias = is_self_join(from, &table_name, &table_alias);

    // Generate a unique alias for self-joins to avoid schema conflicts
    let (
        effective_alias,
        outer_expr_qualified,
        subquery_column_rewritten,
        subquery_where_rewritten,
    ) = if needs_alias {
        // Create a unique alias for the right side of the self-join
        let new_alias = format!("__subquery_{}", table_name);

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!(
                "[SUBQUERY_TRANSFORM] Self-join detected: table={}, new_alias={}",
                table_name, new_alias
            );
        }

        // For a self-join, we need to qualify the outer expression columns with the
        // original table name (not the leftmost table in the FROM clause).
        // Example: `i_item_id IN (SELECT i_item_id FROM item WHERE ...)`
        // The outer `i_item_id` refers to the outer `item` table, so we qualify it
        // as `item.i_item_id`, and the subquery columns get rewritten to `__subquery_item.i_item_id`
        let outer_table_name = table_alias.as_ref().unwrap_or(&table_name);

        // Qualify outer expression columns with the outer table name
        // This prevents ambiguity when both tables have the same column names
        let qualified_expr = qualify_outer_column_refs(expr, outer_table_name);

        // Use the table alias (if present) for matching column references, not just the table name
        // This is critical for Q21 where the subquery uses an alias like "l2" or "l3"
        // Column references like "l2.l_orderkey" need to match against "l2", not "LINEITEM"
        let old_table_ref = table_alias.as_ref().unwrap_or(&table_name);

        // Rewrite column references in the subquery column to use the new alias
        let rewritten_col =
            rewrite_column_refs_with_alias(&subquery_column, old_table_ref, &new_alias);

        // Rewrite column references in the subquery WHERE clause
        let rewritten_where = subquery
            .where_clause
            .as_ref()
            .map(|w| rewrite_column_refs_with_alias(w, old_table_ref, &new_alias));

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] outer_table_name={}", outer_table_name);
            eprintln!("[SUBQUERY_TRANSFORM] qualified_expr={:?}", qualified_expr);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_col={:?}", rewritten_col);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_where={:?}", rewritten_where);
        }

        (Some(new_alias), qualified_expr, rewritten_col, rewritten_where)
    } else {
        (table_alias.clone(), expr.clone(), subquery_column.clone(), subquery.where_clause.clone())
    };

    // Create the join condition: expr = subquery_column
    let join_condition = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(outer_expr_qualified),
        right: Box::new(subquery_column_rewritten),
    };

    // Combine join condition with subquery's WHERE clause if it exists
    let final_condition = if let Some(subquery_where) = subquery_where_rewritten {
        Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(join_condition),
            right: Box::new(subquery_where),
        })
    } else {
        Some(join_condition)
    };

    // Create the right side of the join
    let right_from =
        FromClause::Table { name: table_name, alias: effective_alias, column_aliases: None };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated { JoinType::Anti } else { JoinType::Semi };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: final_condition.clone(),
        natural: false,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Final condition: {:?}", final_condition);
        eprintln!("[SUBQUERY_TRANSFORM] New FROM: {:?}", new_from);
    }

    Some(InToJoinResult { from: new_from })
}

/// Convert an IN subquery with GROUP BY/HAVING to a SEMI/ANTI join with derived table
///
/// For aggregate subqueries like:
/// ```sql
/// WHERE o_orderkey IN (
///     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
/// )
/// ```
///
/// We convert to:
/// ```sql
/// SEMI JOIN (
///     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
/// ) AS __in_agg ON o_orderkey = __in_agg.l_orderkey
/// ```
///
/// This is more efficient than row-by-row IN evaluation because:
/// 1. The subquery is executed once (not per row)
/// 2. The join uses hash-based semi-join (O(1) probe per row)
fn try_convert_aggregate_in_to_join(
    from: &FromClause,
    outer_expr: &Expression,
    subquery: &SelectStmt,
    subquery_column: &Expression,
    negated: bool,
) -> Option<InToJoinResult> {
    // Extract the column name from the subquery's select list for the join condition
    // The column must be a simple column reference for us to build the join condition
    let column_name = match subquery_column {
        Expression::ColumnRef { column, .. } => column.clone(),
        // For aggregate subqueries, we could also handle expressions by giving them an alias,
        // but for now we only support simple column references
        _ => return None,
    };

    // Use a counter to generate unique aliases for nested cases
    // Thread-local counter ensures uniqueness within a query optimization pass
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let alias = format!("__in_agg_{}", COUNTER.fetch_add(1, Ordering::Relaxed));

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!(
            "[SUBQUERY_TRANSFORM] Converting aggregate IN subquery to derived table semi-join"
        );
        eprintln!("[SUBQUERY_TRANSFORM] Derived table alias: {}", alias);
        eprintln!("[SUBQUERY_TRANSFORM] Column for join: {}", column_name);
    }

    // Create the derived table from the subquery
    let right_from = FromClause::Subquery {
        query: Box::new(subquery.clone()),
        alias: alias.clone(),
        column_aliases: None,
    };

    // Create the join condition: outer_expr = __in_agg.column_name
    let join_condition = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(outer_expr.clone()),
        right: Box::new(Expression::ColumnRef { table: Some(alias.clone()), column: column_name }),
    };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated { JoinType::Anti } else { JoinType::Semi };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: Some(join_condition.clone()),
        natural: false,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Final condition: {:?}", join_condition);
        eprintln!("[SUBQUERY_TRANSFORM] New FROM: {:?}", new_from);
    }

    Some(InToJoinResult { from: new_from })
}
