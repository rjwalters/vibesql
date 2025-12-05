//! EXISTS/NOT EXISTS subquery to SEMI/ANTI join transformations
//!
//! This module handles converting EXISTS and NOT EXISTS subqueries into
//! SEMI and ANTI joins respectively.
//!
//! ## Examples
//!
//! ### EXISTS → SEMI JOIN (simple single-table subquery)
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
//!
//! ### EXISTS with multi-table subquery (Q69 pattern)
//! ```sql
//! -- Before:
//! SELECT * FROM customer c WHERE EXISTS (
//!   SELECT 1 FROM store_sales, date_dim d
//!   WHERE c.c_customer_sk = ss_customer_sk
//!     AND ss_sold_date_sk = d.d_date_sk
//!     AND d.d_year = 2000
//! )
//!
//! -- After:
//! SELECT c.* FROM customer c SEMI JOIN (
//!   SELECT DISTINCT ss_customer_sk
//!   FROM store_sales, date_dim d
//!   WHERE ss_sold_date_sk = d.d_date_sk AND d.d_year = 2000
//! ) AS __exists_subq ON c.c_customer_sk = __exists_subq.ss_customer_sk
//! ```

use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType, SelectItem, SelectStmt};

use super::helpers::{is_self_join, rewrite_column_refs_with_alias};

/// Try to convert an EXISTS subquery to a SEMI or ANTI join
pub(super) fn try_convert_exists_to_join(
    from: &FromClause,
    subquery: &SelectStmt,
    negated: bool,
) -> Option<(FromClause, Option<Expression>)> {
    // For EXISTS, we need to extract the correlation predicate from the WHERE clause
    // and use it as the join condition

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
    if subquery.group_by.is_some() || subquery.having.is_some() || subquery.set_operation.is_some()
    {
        return None;
    }

    // Check for simple single-table subquery vs complex multi-table subquery
    match &subquery.from {
        Some(FromClause::Table { name, alias, .. }) => {
            // Simple single-table case: use existing logic
            try_convert_simple_exists_to_join(
                from,
                subquery,
                negated,
                name.clone(),
                alias.clone(),
                where_clause,
            )
        }
        Some(from_clause @ FromClause::Join { .. }) => {
            // Multi-table subquery (explicit or implicit joins): wrap as derived table
            try_convert_complex_exists_to_join(from, subquery, negated, from_clause, where_clause)
        }
        _ => None, // Subquery in FROM or no FROM clause, skip
    }
}

/// Convert a simple single-table EXISTS subquery to a semi/anti join
fn try_convert_simple_exists_to_join(
    from: &FromClause,
    _subquery: &SelectStmt,
    negated: bool,
    table_name: String,
    table_alias: Option<String>,
    where_clause: &Expression,
) -> Option<(FromClause, Option<Expression>)> {
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
    let right_from =
        FromClause::Table { name: table_name, alias: effective_alias, column_aliases: None };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated { JoinType::Anti } else { JoinType::Semi };

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

/// Convert a complex multi-table EXISTS subquery to a semi/anti join with derived table
///
/// For subqueries like:
/// ```sql
/// EXISTS (SELECT 1 FROM store_sales, date_dim d
///         WHERE c.c_customer_sk = ss_customer_sk
///           AND ss_sold_date_sk = d.d_date_sk
///           AND d.d_year = 2000)
/// ```
///
/// We transform to:
/// ```sql
/// SEMI JOIN (SELECT DISTINCT ss_customer_sk FROM store_sales, date_dim d
///            WHERE ss_sold_date_sk = d.d_date_sk AND d.d_year = 2000) AS __exists_subq
/// ON c.c_customer_sk = __exists_subq.ss_customer_sk
/// ```
fn try_convert_complex_exists_to_join(
    from: &FromClause,
    subquery: &SelectStmt,
    negated: bool,
    subquery_from: &FromClause,
    where_clause: &Expression,
) -> Option<(FromClause, Option<Expression>)> {
    // Extract correlation predicates and internal predicates from the WHERE clause
    // Correlation predicates reference outer tables, internal predicates don't
    let (correlation_preds, internal_preds) =
        split_predicates_by_correlation(where_clause, subquery);

    if correlation_preds.is_empty() {
        // No correlation predicates found - this shouldn't happen since we checked is_correlated
        return None;
    }

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!(
            "[SUBQUERY_TRANSFORM] Complex EXISTS: {} correlation preds, {} internal preds",
            correlation_preds.len(),
            internal_preds.len()
        );
    }

    // From the correlation predicates, extract the subquery column(s) that correlate with outer
    // For example, from `c.c_customer_sk = ss_customer_sk`, extract `ss_customer_sk`
    let (join_condition, subquery_columns) =
        build_join_condition_and_select_columns(&correlation_preds, subquery)?;

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Complex EXISTS: subquery_columns={:?}", subquery_columns);
    }

    // Build the internal WHERE clause for the derived table
    let internal_where = combine_predicates(internal_preds);

    // Generate a unique alias for the derived table
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let alias =
        format!("__exists_subq_{}", COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

    // Build the derived table SELECT statement
    // SELECT DISTINCT <subquery_columns> FROM <subquery_from> WHERE <internal_preds>
    let derived_select = SelectStmt {
        with_clause: None,
        distinct: true, // DISTINCT to deduplicate for semi-join semantics
        select_list: subquery_columns
            .iter()
            .map(|col| SelectItem::Expression {
                expr: Expression::ColumnRef {
                    table: col.table.clone(),
                    column: col.column.clone(),
                },
                alias: Some(col.column.clone()),
            })
            .collect(),
        into_table: None,
        into_variables: None,
        from: Some(subquery_from.clone()),
        where_clause: internal_where,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    // Create the derived table as FromClause::Subquery
    let right_from = FromClause::Subquery {
        query: Box::new(derived_select),
        alias: alias.clone(),
        column_aliases: None,
    };

    // Rewrite the join condition to reference the derived table alias
    let rewritten_join_condition =
        rewrite_join_condition_for_derived_table(&join_condition, &subquery_columns, &alias);

    // Create SEMI or ANTI join based on negation
    let join_type = if negated { JoinType::Anti } else { JoinType::Semi };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: Some(rewritten_join_condition),
        natural: false,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Complex EXISTS transformed successfully");
    }

    Some((new_from, None))
}

/// A simple column reference extracted from predicates
#[derive(Debug, Clone)]
struct ColumnRef {
    table: Option<String>,
    column: String,
}

/// Split WHERE clause predicates into correlation (external) and internal predicates
fn split_predicates_by_correlation(
    expr: &Expression,
    subquery: &SelectStmt,
) -> (Vec<Expression>, Vec<Expression>) {
    let mut correlation = Vec::new();
    let mut internal = Vec::new();

    // Flatten AND predicates
    let predicates = flatten_and_predicates(expr);

    for pred in predicates {
        if crate::optimizer::subquery_rewrite::correlation::has_external_column_refs(
            &pred, subquery,
        ) {
            correlation.push(pred);
        } else {
            internal.push(pred);
        }
    }

    (correlation, internal)
}

/// Flatten nested AND expressions into a list of predicates
fn flatten_and_predicates(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            let mut result = flatten_and_predicates(left);
            result.extend(flatten_and_predicates(right));
            result
        }
        Expression::Conjunction(children) => {
            children.iter().flat_map(flatten_and_predicates).collect()
        }
        _ => vec![expr.clone()],
    }
}

/// Combine a list of predicates with AND
fn combine_predicates(predicates: Vec<Expression>) -> Option<Expression> {
    if predicates.is_empty() {
        return None;
    }

    let mut iter = predicates.into_iter();
    let mut result = iter.next().unwrap();

    for pred in iter {
        result = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(result),
            right: Box::new(pred),
        };
    }

    Some(result)
}

/// From correlation predicates, extract the join condition and the subquery columns needed
///
/// For example, from `c.c_customer_sk = ss_customer_sk`:
/// - Join condition: `c.c_customer_sk = __exists_subq.ss_customer_sk` (after rewriting)
/// - Subquery column: `ss_customer_sk`
fn build_join_condition_and_select_columns(
    correlation_preds: &[Expression],
    subquery: &SelectStmt,
) -> Option<(Expression, Vec<ColumnRef>)> {
    let mut subquery_columns = Vec::new();

    // We need to analyze each correlation predicate to find the subquery-side column
    for pred in correlation_preds {
        if let Some(col) = extract_subquery_column_from_correlation(pred, subquery) {
            if !subquery_columns.iter().any(|c: &ColumnRef| c.column == col.column) {
                subquery_columns.push(col);
            }
        }
    }

    if subquery_columns.is_empty() {
        return None;
    }

    // Combine correlation predicates with AND for the join condition
    let join_condition = combine_predicates(correlation_preds.to_vec())?;

    Some((join_condition, subquery_columns))
}

/// Extract the subquery-side column from a correlation predicate
///
/// For `c.c_customer_sk = ss_customer_sk`, returns `ss_customer_sk` (the subquery column)
fn extract_subquery_column_from_correlation(
    pred: &Expression,
    subquery: &SelectStmt,
) -> Option<ColumnRef> {
    match pred {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Check if left is external and right is internal, or vice versa
            let left_external =
                crate::optimizer::subquery_rewrite::correlation::has_external_column_refs(
                    left, subquery,
                );
            let right_external =
                crate::optimizer::subquery_rewrite::correlation::has_external_column_refs(
                    right, subquery,
                );

            if left_external && !right_external {
                // Right side is the subquery column
                extract_column_ref(right)
            } else if !left_external && right_external {
                // Left side is the subquery column
                extract_column_ref(left)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract column reference from an expression
fn extract_column_ref(expr: &Expression) -> Option<ColumnRef> {
    match expr {
        Expression::ColumnRef { table, column } => {
            Some(ColumnRef { table: table.clone(), column: column.clone() })
        }
        _ => None,
    }
}

/// Rewrite the join condition to reference columns from the derived table alias
///
/// Changes `c.c_customer_sk = ss_customer_sk` to `c.c_customer_sk = __exists_subq.ss_customer_sk`
fn rewrite_join_condition_for_derived_table(
    condition: &Expression,
    subquery_columns: &[ColumnRef],
    alias: &str,
) -> Expression {
    match condition {
        Expression::ColumnRef { table, column } => {
            // Check if this column is one of the subquery columns
            if subquery_columns.iter().any(|c| {
                c.column == *column && (c.table.is_none() || c.table.as_ref() == table.as_ref())
            }) {
                Expression::ColumnRef { table: Some(alias.to_string()), column: column.clone() }
            } else {
                condition.clone()
            }
        }
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: op.clone(),
            left: Box::new(rewrite_join_condition_for_derived_table(left, subquery_columns, alias)),
            right: Box::new(rewrite_join_condition_for_derived_table(
                right,
                subquery_columns,
                alias,
            )),
        },
        Expression::Conjunction(children) => Expression::Conjunction(
            children
                .iter()
                .map(|c| rewrite_join_condition_for_derived_table(c, subquery_columns, alias))
                .collect(),
        ),
        _ => condition.clone(),
    }
}
