//! Scalar subquery decorrelation for aggregate patterns
//!
//! This module implements decorrelation of correlated scalar subqueries with
//! aggregates into CTE + JOIN patterns, transforming O(N * table_scan) execution
//! into O(table_scan + hash_join).
//!
//! ## Pattern Detected
//!
//! ```sql
//! outer.col OP K * (SELECT AGG(inner.col) FROM inner WHERE inner.corr = outer.corr)
//! ```
//!
//! ## Transformed To
//!
//! ```sql
//! WITH _subq_agg AS (
//!     SELECT corr, AGG(inner.col) AS _agg
//!     FROM inner
//!     GROUP BY corr
//! )
//! ... JOIN _subq_agg ON outer.corr = _subq_agg.corr
//! WHERE outer.col OP K * _subq_agg._agg
//! ```
//!
//! ## Example: TPC-DS Q6
//!
//! Before:
//! ```sql
//! SELECT a.ca_state, COUNT(*) FROM ...
//! WHERE ... AND i.i_current_price > 1.2 * (
//!     SELECT AVG(j.i_current_price) FROM item j WHERE j.i_category = i.i_category
//! )
//! ```
//!
//! After:
//! ```sql
//! WITH _item_avg AS (
//!     SELECT i_category, AVG(i_current_price) AS _avg_price
//!     FROM item
//!     GROUP BY i_category
//! )
//! SELECT a.ca_state, COUNT(*) FROM ...
//! JOIN _item_avg ON i.i_category = _item_avg.i_category
//! WHERE ... AND i.i_current_price > 1.2 * _item_avg._avg_price
//! ```

use std::sync::atomic::{AtomicU32, Ordering};

use vibesql_ast::{
    BinaryOperator, ColumnIdentifier, CommonTableExpr, CteMaterialization, Expression, FromClause,
    GroupByClause, JoinType, SelectItem, SelectStmt,
};

/// Global counter for generating unique CTE aliases
static CTE_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Result of a successful scalar subquery decorrelation
#[derive(Debug)]
struct DecorrelationResult {
    /// The CTE to add to the query's WITH clause
    cte: CommonTableExpr,
    /// The JOIN to add to the FROM clause
    join_on: Expression,
    /// The FROM clause for the CTE (table/alias)
    cte_table: FromClause,
    /// The replacement expression for the original scalar subquery
    replacement_expr: Expression,
}

/// Check if an operator is a comparison operator
fn is_comparison_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::Equal
            | BinaryOperator::NotEqual
    )
}

/// Check if an expression is a constant (literal or expression with no column refs)
fn is_constant(expr: &Expression) -> bool {
    match expr {
        Expression::Literal(_) => true,
        Expression::UnaryOp { expr, .. } => is_constant(expr),
        Expression::BinaryOp { left, right, .. } => is_constant(left) && is_constant(right),
        _ => false,
    }
}

/// Try to decorrelate a scalar subquery with an aggregate
fn try_decorrelate_subquery(
    subquery: &SelectStmt,
    outer_tables: &[String],
) -> Option<DecorrelationResult> {
    // Requirements for decorrelation:
    // 1. Single aggregate function in SELECT list
    // 2. No GROUP BY (or GROUP BY matches correlation column)
    // 3. WHERE clause contains a simple correlation predicate

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!(
            "[SCALAR_DECORRELATION] try_decorrelate_subquery: set_op={}, group_by={}, having={}, limit={}, offset={}",
            subquery.set_operation.is_some(),
            subquery.group_by.is_some(),
            subquery.having.is_some(),
            subquery.limit.is_some(),
            subquery.offset.is_some()
        );
    }

    // Skip subqueries with set operations, GROUP BY, HAVING, LIMIT, etc.
    if subquery.set_operation.is_some()
        || subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
    {
        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SCALAR_DECORRELATION] Skipping due to set_op/group_by/having/limit/offset");
        }
        return None;
    }

    // Must have a simple FROM clause (single table)
    let (inner_table, inner_alias) = match &subquery.from {
        Some(FromClause::Table { name, alias, .. }) => {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                eprintln!("[SCALAR_DECORRELATION] Inner table: {}, alias: {:?}", name, alias);
            }
            (name.clone(), alias.clone().unwrap_or_else(|| name.clone()))
        }
        _ => {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                eprintln!("[SCALAR_DECORRELATION] Skipping - FROM clause is not a simple table");
            }
            return None;
        }
    };

    // Must have exactly one aggregate function in SELECT list
    let (agg_func, _agg_alias) = match extract_single_aggregate(&subquery.select_list) {
        Some(result) => {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                eprintln!("[SCALAR_DECORRELATION] Found aggregate function");
            }
            result
        }
        None => {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                eprintln!("[SCALAR_DECORRELATION] Skipping - no single aggregate in SELECT list");
            }
            return None;
        }
    };

    // Must have a WHERE clause with a correlation predicate
    let where_clause = match subquery.where_clause.as_ref() {
        Some(w) => w,
        None => {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                eprintln!("[SCALAR_DECORRELATION] Skipping - no WHERE clause");
            }
            return None;
        }
    };

    // Extract correlation predicate: inner.col = outer.col
    let (inner_col, outer_col, remaining_predicates) =
        match extract_correlation_predicate(where_clause, &inner_alias, outer_tables) {
            Some(result) => {
                if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                    eprintln!("[SCALAR_DECORRELATION] Found correlation predicate");
                }
                result
            }
            None => {
                if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                    eprintln!(
                        "[SCALAR_DECORRELATION] Skipping - no correlation predicate found in WHERE"
                    );
                }
                return None;
            }
        };

    // Generate unique CTE alias
    let counter = CTE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let cte_alias = format!("_decorr_{}", counter);
    let agg_col_alias = format!("_agg_{}", counter);

    // Extract the correlation column name for building the CTE
    let inner_col_name = match &inner_col {
        Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
        _ => return None,
    };

    // Determine if the original inner table had an alias
    let original_alias = match &subquery.from {
        Some(FromClause::Table { alias, .. }) => alias.clone(),
        _ => None,
    };

    // Create a unique alias for the correlation column in the CTE to avoid ambiguity
    let corr_col_alias = format!("_corr_{}", counter);

    // Build the CTE: SELECT corr_col AS _corr_N, AGG(col) AS _agg_N FROM table [alias]
    // [WHERE remaining] GROUP BY corr_col
    // The CTE keeps the original table alias to ensure column references in the
    // aggregate function still work
    let cte_select = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![
            // Correlation column - use unqualified name and give it a unique alias
            SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple(&inner_col_name, false)),
                alias: Some(corr_col_alias.clone()),
                source_text: None,
            },
            // Aggregate expression - keep original (may have qualified column refs)
            SelectItem::Expression {
                expr: agg_func,
                alias: Some(agg_col_alias.clone()),
                source_text: None,
            },
        ],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: inner_table.clone(),
            alias: original_alias, // Keep original alias so aggregate's column refs work
            column_aliases: None,
            quoted: false,
        }),
        where_clause: remaining_predicates,
        group_by: Some(GroupByClause::Simple(vec![Expression::ColumnRef(
            ColumnIdentifier::simple(&inner_col_name, false),
        )])),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        values: None,
    };

    // Build the CTE
    let cte = CommonTableExpr {
        name: cte_alias.clone(),
        columns: None,
        query: Box::new(cte_select),
        recursive: false,
        materialization: CteMaterialization::Default,
    };

    // Build the JOIN condition: outer.corr = cte._corr_N
    let join_on = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(outer_col.clone()),
        right: Box::new(Expression::ColumnRef(ColumnIdentifier::qualified(
            &cte_alias,
            false,
            &corr_col_alias,
            false,
        ))),
    };

    // Build the CTE table reference for joining
    let cte_table = FromClause::Table {
        name: cte_alias.clone(),
        alias: None,
        column_aliases: None,
        quoted: false,
    };

    // Build the replacement expression: cte._agg
    let replacement_expr = Expression::ColumnRef(ColumnIdentifier::qualified(
        &cte_alias,
        false,
        &agg_col_alias,
        false,
    ));

    Some(DecorrelationResult { cte, join_on, cte_table, replacement_expr })
}

/// Extract a single aggregate function from the SELECT list
fn extract_single_aggregate(select_list: &[SelectItem]) -> Option<(Expression, String)> {
    if select_list.len() != 1 {
        return None;
    }

    match &select_list[0] {
        SelectItem::Expression { expr, alias, .. } => {
            if is_aggregate_expr(expr) {
                let agg_alias = alias.clone().unwrap_or_else(|| "agg".to_string());
                Some((expr.clone(), agg_alias))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Check if an expression is an aggregate function
fn is_aggregate_expr(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        // Handle wrapped aggregates like CAST(AVG(...) AS ...)
        Expression::Cast { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    }
}

/// Extract correlation predicate from WHERE clause
/// Returns (inner_col, outer_col, remaining_predicates)
fn extract_correlation_predicate(
    expr: &Expression,
    inner_table: &str,
    outer_tables: &[String],
) -> Option<(Expression, Expression, Option<Expression>)> {
    match expr {
        // Direct equality: inner.col = outer.col
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Try left=inner, right=outer
            if is_from_table(left, inner_table) && is_from_outer(right, outer_tables) {
                return Some(((**left).clone(), (**right).clone(), None));
            }
            // Try right=inner, left=outer
            if is_from_table(right, inner_table) && is_from_outer(left, outer_tables) {
                return Some(((**right).clone(), (**left).clone(), None));
            }
            None
        }

        // AND: correlation might be in one branch
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Try left branch for correlation
            if let Some((inner, outer, left_remaining)) =
                extract_correlation_predicate(left, inner_table, outer_tables)
            {
                let remaining = combine_predicates(left_remaining, Some((**right).clone()));
                return Some((inner, outer, remaining));
            }

            // Try right branch for correlation
            if let Some((inner, outer, right_remaining)) =
                extract_correlation_predicate(right, inner_table, outer_tables)
            {
                let remaining = combine_predicates(Some((**left).clone()), right_remaining);
                return Some((inner, outer, remaining));
            }

            None
        }

        _ => None,
    }
}

/// Check if an expression references a specific table
fn is_from_table(expr: &Expression, table: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => match col_id.table_canonical() {
            Some(t) => t.eq_ignore_ascii_case(table),
            None => true, // Unqualified could be from inner
        },
        _ => false,
    }
}

/// Check if an expression references one of the outer tables
fn is_from_outer(expr: &Expression, outer_tables: &[String]) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => match col_id.table_canonical() {
            Some(t) => outer_tables.iter().any(|ot| ot.eq_ignore_ascii_case(t)),
            None => false, // Unqualified without context - be conservative
        },
        _ => false,
    }
}

/// Combine two optional predicates with AND
fn combine_predicates(left: Option<Expression>, right: Option<Expression>) -> Option<Expression> {
    match (left, right) {
        (Some(l), Some(r)) => Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(l),
            right: Box::new(r),
        }),
        (Some(l), None) => Some(l),
        (None, Some(r)) => Some(r),
        (None, None) => None,
    }
}

/// Apply scalar subquery decorrelation to a SELECT statement
///
/// This transforms correlated scalar subqueries with aggregates into
/// CTE + JOIN patterns for better performance.
pub fn apply_scalar_decorrelation(stmt: &SelectStmt) -> SelectStmt {
    // Extract outer table names for correlation detection
    let outer_tables = extract_table_names(&stmt.from);

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SCALAR_DECORRELATION] Checking query, outer_tables: {:?}", outer_tables);
    }

    if outer_tables.is_empty() {
        return stmt.clone();
    }

    // Look for decorrelatable scalar subqueries in the WHERE clause
    let where_clause = match &stmt.where_clause {
        Some(w) => w,
        None => return stmt.clone(),
    };

    // Try to find and decorrelate scalar subqueries
    if let Some((new_where, decorrelations)) =
        find_and_decorrelate_in_where(where_clause, &outer_tables)
    {
        if decorrelations.is_empty() {
            return stmt.clone();
        }

        let mut result = stmt.clone();

        // Add CTEs to WITH clause
        let new_ctes: Vec<CommonTableExpr> = decorrelations.iter().map(|d| d.cte.clone()).collect();
        result.with_clause = match &stmt.with_clause {
            Some(existing_ctes) => Some(existing_ctes.iter().cloned().chain(new_ctes).collect()),
            None => Some(new_ctes),
        };

        // Add JOINs to FROM clause
        if let Some(from) = &result.from {
            let mut new_from = from.clone();
            for decorrelation in &decorrelations {
                new_from = FromClause::Join {
                    join_type: JoinType::Inner,
                    left: Box::new(new_from),
                    right: Box::new(decorrelation.cte_table.clone()),
                    condition: Some(decorrelation.join_on.clone()),
                    using_columns: None,
                    natural: false,
                    alias: None,
                };
            }
            result.from = Some(new_from);
        }

        // Replace WHERE clause with decorrelated version
        result.where_clause = new_where;

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!(
                "[SCALAR_DECORRELATION] Decorrelated {} scalar subqueries",
                decorrelations.len()
            );
        }

        result
    } else {
        stmt.clone()
    }
}

/// Find and decorrelate scalar subqueries in a WHERE clause expression
fn find_and_decorrelate_in_where(
    expr: &Expression,
    outer_tables: &[String],
) -> Option<(Option<Expression>, Vec<DecorrelationResult>)> {
    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SCALAR_DECORRELATION] find_and_decorrelate_in_where: checking expression");
    }
    let mut decorrelations = Vec::new();
    let new_expr = rewrite_expr_with_decorrelation(expr, outer_tables, &mut decorrelations);

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!(
            "[SCALAR_DECORRELATION] find_and_decorrelate_in_where: found {} decorrelations",
            decorrelations.len()
        );
    }

    if decorrelations.is_empty() {
        None
    } else {
        Some((Some(new_expr), decorrelations))
    }
}

/// Recursively rewrite an expression, decorrelating scalar subqueries
fn rewrite_expr_with_decorrelation(
    expr: &Expression,
    outer_tables: &[String],
    decorrelations: &mut Vec<DecorrelationResult>,
) -> Expression {
    match expr {
        // Check for comparison with scalar subquery
        Expression::BinaryOp { op, left, right } if is_comparison_op(op) => {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                eprintln!(
                    "[SCALAR_DECORRELATION] Found comparison op {:?}, checking for scalar subquery",
                    op
                );
            }
            // Try to decorrelate right side
            if let Some((original_subq, multiplier)) =
                extract_scalar_subquery_with_multiplier(right)
            {
                if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                    eprintln!("[SCALAR_DECORRELATION] Found scalar subquery on right side");
                }
                if let Some(decorrelation) = try_decorrelate_subquery(original_subq, outer_tables) {
                    // Build replacement expression with multiplier if present
                    let replacement = if let Some(mult) = multiplier {
                        Expression::BinaryOp {
                            op: BinaryOperator::Multiply,
                            left: Box::new(mult),
                            right: Box::new(decorrelation.replacement_expr.clone()),
                        }
                    } else {
                        decorrelation.replacement_expr.clone()
                    };

                    decorrelations.push(decorrelation);

                    return Expression::BinaryOp {
                        op: op.clone(),
                        left: left.clone(),
                        right: Box::new(replacement),
                    };
                }
            }

            // Try to decorrelate left side
            if let Some((original_subq, multiplier)) = extract_scalar_subquery_with_multiplier(left)
            {
                if let Some(decorrelation) = try_decorrelate_subquery(original_subq, outer_tables) {
                    let replacement = if let Some(mult) = multiplier {
                        Expression::BinaryOp {
                            op: BinaryOperator::Multiply,
                            left: Box::new(mult),
                            right: Box::new(decorrelation.replacement_expr.clone()),
                        }
                    } else {
                        decorrelation.replacement_expr.clone()
                    };

                    decorrelations.push(decorrelation);

                    return Expression::BinaryOp {
                        op: op.clone(),
                        left: Box::new(replacement),
                        right: right.clone(),
                    };
                }
            }

            // Recursively process both sides for nested expressions
            Expression::BinaryOp {
                op: op.clone(),
                left: Box::new(rewrite_expr_with_decorrelation(left, outer_tables, decorrelations)),
                right: Box::new(rewrite_expr_with_decorrelation(
                    right,
                    outer_tables,
                    decorrelations,
                )),
            }
        }

        // AND/OR - recurse into both sides
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: op.clone(),
            left: Box::new(rewrite_expr_with_decorrelation(left, outer_tables, decorrelations)),
            right: Box::new(rewrite_expr_with_decorrelation(right, outer_tables, decorrelations)),
        },

        // Other expressions - return as-is
        _ => expr.clone(),
    }
}

/// Extract a scalar subquery from an expression, along with any multiplier
fn extract_scalar_subquery_with_multiplier(
    expr: &Expression,
) -> Option<(&SelectStmt, Option<Expression>)> {
    match expr {
        Expression::ScalarSubquery(subquery) => Some((subquery, None)),
        Expression::BinaryOp { op: BinaryOperator::Multiply, left, right } => {
            if let Expression::ScalarSubquery(subquery) = right.as_ref() {
                if is_constant(left) {
                    return Some((subquery, Some((**left).clone())));
                }
            }
            if let Expression::ScalarSubquery(subquery) = left.as_ref() {
                if is_constant(right) {
                    return Some((subquery, Some((**right).clone())));
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract table names from a FROM clause
fn extract_table_names(from: &Option<FromClause>) -> Vec<String> {
    fn collect_tables(from: &FromClause, tables: &mut Vec<String>) {
        match from {
            FromClause::Table { name, alias, .. } => {
                tables.push(alias.clone().unwrap_or_else(|| name.clone()));
                tables.push(name.clone());
            }
            FromClause::Join { left, right, .. } => {
                collect_tables(left, tables);
                collect_tables(right, tables);
            }
            FromClause::Subquery { alias, .. } => {
                tables.push(alias.clone());
            }
            FromClause::Values { alias, .. } => {
                tables.push(alias.clone());
            }
        }
    }

    let mut tables = Vec::new();
    if let Some(from_clause) = from {
        collect_tables(from_clause, &mut tables);
    }
    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_constant() {
        assert!(is_constant(&Expression::Literal(vibesql_types::SqlValue::Integer(42))));
        assert!(is_constant(&Expression::Literal(vibesql_types::SqlValue::Float(1.2))));
        assert!(!is_constant(&Expression::ColumnRef(ColumnIdentifier::simple("x", false))));
    }
}
