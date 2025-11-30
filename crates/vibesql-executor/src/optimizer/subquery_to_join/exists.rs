//! EXISTS/NOT EXISTS subquery to SEMI/ANTI join transformations
//!
//! This module handles converting EXISTS and NOT EXISTS subqueries into
//! SEMI and ANTI joins respectively.
//!
//! ## Examples
//!
//! ### EXISTS → SEMI JOIN
//! ```sql
//! -- Before (after decorrelation):
//! SELECT * FROM orders WHERE EXISTS (
//!   SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey
//! )
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN lineitem ON l_orderkey = o_orderkey
//! ```
//!
//! ### NOT EXISTS → ANTI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE NOT EXISTS (
//!   SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey
//! )
//!
//! -- After:
//! SELECT orders.* FROM orders ANTI JOIN lineitem ON l_orderkey = o_orderkey
//! ```

use vibesql_ast::{Expression, FromClause, JoinType, SelectStmt};

use super::helpers::{is_self_join, rewrite_column_refs_with_alias};

/// Try to convert an EXISTS subquery to a SEMI or ANTI join
pub(super) fn try_convert_exists_to_join(
    from: &FromClause,
    subquery: &SelectStmt,
    negated: bool,
) -> Option<(FromClause, Option<Expression>)> {
    // For EXISTS, we need to extract the correlation predicate from the WHERE clause
    // and use it as the join condition

    // Check for simple single-table subquery
    let (table_name, table_alias) = match &subquery.from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        _ => return None, // Complex FROM clause, skip
    };

    // EXISTS subqueries should have a WHERE clause with correlation
    let where_clause = subquery.where_clause.as_ref()?;

    // CRITICAL: Only transform correlated EXISTS subqueries to joins.
    // Uncorrelated EXISTS (e.g., EXISTS (SELECT 1 FROM t WHERE t.col = 5))
    // should NOT be converted to a join because the WHERE clause doesn't
    // correlate with the outer query - it's just a filter on the subquery's table.
    // Converting it would incorrectly use the filter as a join condition.
    if !crate::optimizer::subquery_rewrite::correlation::is_correlated(subquery) {
        return None;
    }

    // Skip if subquery has complex features
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.set_operation.is_some()
    {
        return None;
    }

    // Detect self-join: check if subquery table name conflicts with outer query tables
    let needs_alias = is_self_join(from, &table_name, &table_alias);

    // Handle self-join case: generate unique alias and rewrite column references
    let (effective_alias, rewritten_where) = if needs_alias {
        // Create a unique alias for the right side of the self-join
        let new_alias = format!("__subquery_{}", table_alias.as_ref().unwrap_or(&table_name));

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] EXISTS self-join detected: table={}, alias={:?}, new_alias={}",
                     table_name, table_alias, new_alias);
        }

        // Use the table alias (if present) for matching column references, not just the table name
        // This is critical for Q21 where the subquery uses an alias like "l2" or "l3"
        // Column references like "l2.l_orderkey" need to match against "l2", not "LINEITEM"
        let old_table_ref = table_alias.as_ref().unwrap_or(&table_name);

        // Rewrite column references in the WHERE clause to use the new alias
        let rewritten = rewrite_column_refs_with_alias(where_clause, old_table_ref, &new_alias);

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] EXISTS rewritten_where={:?}", rewritten);
        }

        (Some(new_alias), rewritten)
    } else {
        (table_alias.clone(), where_clause.clone())
    };

    // Create the right side of the join
    let right_from = FromClause::Table {
        name: table_name,
        alias: effective_alias,
    };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: Some(rewritten_where),
        natural: false,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] EXISTS new_from={:?}", new_from);
    }

    // EXISTS doesn't leave any residual WHERE clause
    Some((new_from, None))
}
