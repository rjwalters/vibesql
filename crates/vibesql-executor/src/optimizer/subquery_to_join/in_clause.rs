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

use super::helpers::{is_self_join, is_simple_single_table_self_join, rewrite_column_refs_with_alias};

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

    // FIX for issue #4493: Skip transformation if SELECT list contains unqualified columns
    // that might be correlated references.
    //
    // Example that should NOT be transformed:
    //   SELECT x FROM t2, t1 WHERE x IN (SELECT x FROM t1 WHERE ...)
    //   If the inner `SELECT x` has `x` as a correlated reference (from t2, not t1),
    //   rewriting it to `__subquery_t1.x` will fail because that column doesn't exist in t1.
    //
    // We can only safely optimize unqualified columns when the outer query has a SINGLE
    // table that matches the subquery's table (pure self-join). If the outer query has
    // multiple tables, an unqualified column could reference any of them.
    //
    // Safe to optimize:
    //   SELECT col0 FROM tab0 WHERE col0 IN (SELECT col3 FROM tab0 WHERE ...)
    //   Single table 'tab0' in outer query, unqualified 'col3' must be from tab0.
    //
    // Must skip optimization:
    //   SELECT x FROM t2, t1 WHERE x IN (SELECT x FROM t1 WHERE ...)
    //   Multiple tables in outer query, unqualified 'x' could be from t2 (correlated).
    if let Expression::ColumnRef { table: None, .. } = &subquery_column {
        // Check if outer query has exactly one table and it matches the subquery's table
        let is_simple_self_join = is_simple_single_table_self_join(from, &table_name, &table_alias);

        if !is_simple_self_join {
            // Either not a self-join, or outer query has multiple tables.
            // Unqualified column might be correlated, skip optimization to be safe.
            return None;
        }
        // else: Simple self-join with single table - safe to optimize
    }

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

        // FIX for issue #4493: Don't qualify the outer expression!
        //
        // The original code tried to qualify the outer expression (left side of IN)
        // with the subquery's table name to handle self-joins. But this breaks when:
        // 1. The outer query has multiple tables (FROM t2, t1)
        // 2. The column belongs to a different table than the subquery references
        //
        // Example that FAILS with qualification:
        //   SELECT x FROM t2, t1 WHERE x IN (SELECT c FROM t1 WHERE ...)
        //   - Outer 'x' is from t2, not t1
        //   - Qualifying as 't1.x' causes "column not found" error
        //
        // Let SQL's normal resolution handle it. The join condition will be:
        //   t2.x = __subquery_t1.c  (resolved at runtime based on available columns)
        //
        // For true self-joins like `SELECT * FROM t1 WHERE id IN (SELECT id FROM t1)`,
        // the runtime resolution will correctly pick up t1.id for the outer reference.
        let qualified_expr = expr.clone();

        // Use the table alias (if present) for matching column references, not just the table name
        // This is critical for Q21 where the subquery uses an alias like "l2" or "l3"
        // Column references like "l2.l_orderkey" need to match against "l2", not "LINEITEM"
        let old_table_ref = table_alias.as_ref().unwrap_or(&table_name);

        // Rewrite column references in the subquery column to use the new alias
        let rewritten_col =
            rewrite_column_refs_with_alias(&subquery_column, old_table_ref, &new_alias);

        // FIX for issue #4493: Don't rewrite the WHERE clause!
        // The WHERE clause can contain correlated references to outer query tables.
        // Rewriting ALL unqualified columns breaks correlation for deeply nested subqueries.
        // Only the SELECT list column (return value) needs rewriting for the join.
        //
        // Example: WHERE x = c
        //   - 'c' exists in subquery's t1, would be rewritten to __subquery_t1.c
        //   - 'x' is correlated to outer t2.x, should NOT be rewritten
        //
        // The original code blindly rewrote both, breaking nested correlation.
        let rewritten_where = subquery.where_clause.clone();

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] qualified_expr={:?}", qualified_expr);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_col={:?}", rewritten_col);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_where={:?}", rewritten_where);
        }

        (Some(new_alias), qualified_expr, rewritten_col, rewritten_where)
    } else {
        // Even when not a self-join, we need to qualify the subquery column
        // to avoid ambiguity when both sides have the same column name.
        // Use the effective table name (alias if present, otherwise table name).
        let effective_table = table_alias.as_deref().unwrap_or(&table_name);
        let qualified_subquery_column =
            rewrite_column_refs_with_alias(&subquery_column, effective_table, effective_table);
        let qualified_subquery_where = subquery
            .where_clause
            .as_ref()
            .map(|w| rewrite_column_refs_with_alias(w, effective_table, effective_table));

        (table_alias.clone(), expr.clone(), qualified_subquery_column, qualified_subquery_where)
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
    let right_from = FromClause::Table {
        name: table_name,
        alias: effective_alias,
        column_aliases: None,
        quoted: false, // Synthesized from subquery, treat as unquoted
    };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated { JoinType::Anti } else { JoinType::Semi };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: final_condition.clone(),
        using_columns: None,
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
        using_columns: None,
        natural: false,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Final condition: {:?}", join_condition);
        eprintln!("[SUBQUERY_TRANSFORM] New FROM: {:?}", new_from);
    }

    Some(InToJoinResult { from: new_from })
}
