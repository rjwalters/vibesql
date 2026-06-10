//! Scalar comparison subquery decorrelation
//!
//! This module transforms correlated scalar subqueries in comparison expressions
//! into efficient LEFT JOINs with pre-aggregated derived tables.
//!
//! ## Transformation Example (TPC-H Q20 pattern)
//!
//! ### Before:
//! ```sql
//! SELECT s_name, s_address
//! FROM supplier, nation, partsupp
//! WHERE ps_availqty > (
//!     SELECT 0.5 * SUM(l_quantity)
//!     FROM lineitem
//!     WHERE l_partkey = ps_partkey
//!       AND l_suppkey = ps_suppkey
//!       AND l_shipdate >= '1994-01-01'
//!       AND l_shipdate < '1995-01-01'
//! )
//! ```
//!
//! ### After:
//! ```sql
//! SELECT s_name, s_address
//! FROM supplier, nation, partsupp
//! LEFT JOIN (
//!     SELECT l_partkey, l_suppkey, 0.5 * SUM(l_quantity) AS __scalar_agg
//!     FROM lineitem
//!     WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//!     GROUP BY l_partkey, l_suppkey
//! ) AS __scalar_subq ON ps_partkey = __scalar_subq.l_partkey
//!                    AND ps_suppkey = __scalar_subq.l_suppkey
//! WHERE ps_availqty > __scalar_subq.__scalar_agg
//! ```
//!
//! ## NULL Semantics for Empty Groups
//!
//! When the correlated subquery matches no rows, a scalar aggregate like
//! `SUM`/`AVG`/`MIN`/`MAX` evaluates to NULL, so the comparison is NULL and the
//! outer row is excluded. The LEFT JOIN reproduces this: missing groups yield a
//! NULL `__scalar_agg`, and the comparison stays NULL. We must NOT wrap the
//! column in `COALESCE(.., 0)` — doing so would turn `x > NULL` (row excluded)
//! into `x > 0` (row usually included), which produced wrong results for
//! TPC-H Q20 (issue #5274).
//!
//! The one exception is a bare `COUNT(...)` (or SQLite's `TOTAL`), which
//! evaluates to 0 (not NULL) over an empty group. For those we keep
//! `COALESCE(__scalar_agg, 0)`. If COUNT/TOTAL appears nested inside a larger
//! expression (e.g. `COUNT(*) + 5`), neither NULL nor 0 is the correct
//! empty-group value, so we skip the transformation entirely and fall back to
//! per-row correlated execution.
//!
//! ## Why This Is Faster
//!
//! Without decorrelation, the scalar subquery is executed once per row in the outer query.
//! For TPC-H Q20, this means ~8000 executions of an aggregate query on ~60000 lineitem rows.
//!
//! With decorrelation:
//! 1. The aggregate is computed ONCE for all (partkey, suppkey) pairs
//! 2. Results are joined efficiently using hash join
//! 3. O(n + m) instead of O(n * m)

use std::sync::atomic::{AtomicU64, Ordering};

use vibesql_ast::{
    BinaryOperator, ColumnIdentifier, Expression, FromClause, FunctionIdentifier, GroupByClause,
    JoinType, SelectItem, SelectStmt,
};
use vibesql_types::SqlValue;

use super::helpers::collect_table_names;

/// Counter for generating unique subquery aliases
static SCALAR_SUBQ_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Result of a scalar comparison subquery transformation
pub(super) struct ScalarComparisonResult {
    /// The new FROM clause with the LEFT JOIN
    pub from: FromClause,
    /// The replacement expression comparing against the derived aggregate
    /// column (wrapped in COALESCE(.., 0) only for bare COUNT/TOTAL)
    pub replacement_expr: Expression,
}

/// Try to convert a comparison with a scalar subquery to a LEFT JOIN
///
/// Matches patterns like: `column > (SELECT agg(...) FROM ... WHERE correlated AND non_correlated)`
pub(super) fn try_convert_scalar_comparison_to_join(
    from: &FromClause,
    op: &BinaryOperator,
    left: &Expression,
    right: &Expression,
) -> Option<ScalarComparisonResult> {
    let verbose = std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok();

    if verbose {
        eprintln!("[SCALAR_COMPARISON] Checking: {:?} {:?} ...", left, op);
    }

    // Only handle comparison operators
    if !matches!(
        op,
        BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::Equal
            | BinaryOperator::NotEqual
    ) {
        if verbose {
            eprintln!("[SCALAR_COMPARISON] Not a comparison operator");
        }
        return None;
    }

    // Check if right side is a scalar subquery
    let subquery = match right {
        Expression::ScalarSubquery(sq) => sq.as_ref(),
        _ => {
            if verbose {
                eprintln!("[SCALAR_COMPARISON] Right side is not ScalarSubquery");
            }
            return None;
        }
    };

    if verbose {
        eprintln!("[SCALAR_COMPARISON] Found scalar subquery comparison");
    }

    // Must have exactly one column in SELECT list
    if subquery.select_list.len() != 1 {
        if verbose {
            eprintln!(
                "[SCALAR_COMPARISON] Subquery has {} items in SELECT list, expected 1",
                subquery.select_list.len()
            );
        }
        return None;
    }

    // Extract the aggregate expression
    let agg_expr = match &subquery.select_list[0] {
        SelectItem::Expression { expr, .. } => expr.clone(),
        _ => return None,
    };

    // Must contain an aggregate function (SUM, AVG, COUNT, etc.)
    if !contains_aggregate(&agg_expr) {
        if verbose {
            eprintln!("[SCALAR_COMPARISON] Expression does not contain aggregate function");
        }
        return None;
    }

    if verbose {
        eprintln!("[SCALAR_COMPARISON] Expression contains aggregate function");
    }

    // Determine how the aggregate behaves over an empty group, so the
    // replacement predicate preserves scalar-subquery NULL semantics.
    let empty_group_behavior = classify_empty_group_behavior(&agg_expr);
    if matches!(empty_group_behavior, EmptyGroupBehavior::Unsupported) {
        if verbose {
            eprintln!(
                "[SCALAR_COMPARISON] COUNT/TOTAL nested in expression - cannot represent \
                 empty-group value after LEFT JOIN, skipping"
            );
        }
        return None;
    }

    // Skip if subquery has LIMIT, OFFSET, GROUP BY, HAVING, or set operations
    // We'll add GROUP BY ourselves based on correlation columns
    if subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.set_operation.is_some()
    {
        if verbose {
            eprintln!(
                "[SCALAR_COMPARISON] Skipping - has LIMIT/OFFSET/GROUP BY/HAVING/set operations"
            );
        }
        return None;
    }

    // Get the subquery's table name
    let (inner_table_name, _inner_table_alias) = match &subquery.from {
        Some(FromClause::Table { name, alias, .. }) => {
            if verbose {
                eprintln!("[SCALAR_COMPARISON] Inner table: {}", name);
            }
            (name.clone(), alias.clone())
        }
        _ => {
            if verbose {
                eprintln!("[SCALAR_COMPARISON] Complex FROM clause, skipping");
            }
            return None;
        }
    };

    // Get outer table names for correlation detection
    let mut outer_tables = Vec::new();
    collect_table_names(from, &mut outer_tables);

    if verbose {
        eprintln!("[SCALAR_COMPARISON] Outer tables: {:?}", outer_tables);
    }

    // Extract correlation predicates and non-correlated predicates from WHERE
    let where_clause = subquery.where_clause.as_ref()?;
    let (correlation_predicates, non_correlated_predicates) =
        extract_correlation_predicates(where_clause, &outer_tables, &inner_table_name)?;

    if verbose {
        eprintln!(
            "[SCALAR_COMPARISON] Found {} correlation predicates, {} non-correlated",
            correlation_predicates.len(),
            non_correlated_predicates.len()
        );
    }

    // Must have at least one correlation predicate
    if correlation_predicates.is_empty() {
        if verbose {
            eprintln!("[SCALAR_COMPARISON] No correlation predicates found");
        }
        return None;
    }

    // Extract the correlation column pairs
    let correlation_columns =
        match extract_correlation_columns(&correlation_predicates, &inner_table_name) {
            Some(cols) => cols,
            None => {
                if verbose {
                    eprintln!("[SCALAR_COMPARISON] Failed to extract correlation columns");
                }
                return None;
            }
        };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!(
            "[SUBQUERY_TRANSFORM] Decorrelating scalar comparison subquery with {} correlation columns",
            correlation_columns.len()
        );
        for (outer, inner) in &correlation_columns {
            eprintln!("[SUBQUERY_TRANSFORM]   {} = {}", outer, inner);
        }
    }

    // Generate unique alias for the derived table
    let alias = format!("__scalar_subq_{}", SCALAR_SUBQ_COUNTER.fetch_add(1, Ordering::Relaxed));
    let agg_column_name = "__scalar_agg".to_string();

    // Build the decorrelated subquery:
    // SELECT inner_col1 AS __corr_0, inner_col2 AS __corr_1, ..., agg_expr AS __scalar_agg
    // FROM inner_table
    // WHERE non_correlated_predicates
    // GROUP BY inner_col1, inner_col2, ...
    let mut select_list = Vec::new();

    // Add correlation columns to SELECT list WITH ALIASES to avoid ambiguity
    // When the inner and outer tables have the same name (e.g., both "lineitem" in Q17),
    // the column names would conflict if we don't alias them
    let mut correlation_aliases: Vec<String> = Vec::new();
    for (i, (_, inner_col)) in correlation_columns.iter().enumerate() {
        let col_alias = format!("__corr_{}", i);
        correlation_aliases.push(col_alias.clone());
        select_list.push(SelectItem::Expression {
            expr: Expression::ColumnRef(ColumnIdentifier::simple(inner_col, false)),
            alias: Some(col_alias),
            source_text: None,
        });
    }

    // Add the aggregate expression with alias
    select_list.push(SelectItem::Expression {
        expr: agg_expr.clone(),
        alias: Some(agg_column_name.clone()),
        source_text: None,
    });

    // Build GROUP BY from correlation columns
    let group_by: Vec<Expression> = correlation_columns
        .iter()
        .map(|(_, inner_col)| Expression::ColumnRef(ColumnIdentifier::simple(inner_col, false)))
        .collect();

    // Build the new WHERE clause from non-correlated predicates
    let new_where = if non_correlated_predicates.is_empty() {
        None
    } else {
        Some(combine_predicates(&non_correlated_predicates))
    };

    // Create the decorrelated subquery
    let decorrelated_subquery = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list,
        into_table: None,
        into_variables: None,
        from: subquery.from.clone(),
        where_clause: new_where,
        group_by: Some(GroupByClause::Simple(group_by)),
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        values: None,
    };

    // Create the derived table
    let derived_table = FromClause::Subquery {
        query: Box::new(decorrelated_subquery),
        alias: alias.clone(),
        column_aliases: None,
    };

    // Build the join condition from correlation columns, using the aliases we created
    let join_condition = build_join_condition(&correlation_columns, &correlation_aliases, &alias);

    // Create LEFT JOIN (to preserve rows with no matching aggregate)
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(derived_table),
        join_type: JoinType::LeftOuter,
        condition: Some(join_condition),
        using_columns: None,
        natural: false,
        alias: None,
    };

    // Create the replacement expression: left op alias.__scalar_agg
    let agg_ref =
        Expression::ColumnRef(ColumnIdentifier::qualified(&alias, false, &agg_column_name, false));

    // For NULL-on-empty aggregates (SUM/AVG/MIN/MAX/...), compare directly:
    // a missing LEFT JOIN match yields NULL, the comparison evaluates to NULL,
    // and the row is excluded — matching correlated scalar subquery semantics.
    // Only a bare COUNT/TOTAL evaluates to 0 over an empty group, so only then
    // do we substitute 0 for the missing match via COALESCE.
    let comparison_rhs = match empty_group_behavior {
        EmptyGroupBehavior::PropagateNull => agg_ref,
        EmptyGroupBehavior::DefaultZero => Expression::Function {
            name: FunctionIdentifier::new("COALESCE"),
            args: vec![agg_ref, Expression::Literal(SqlValue::Integer(0))],
            character_unit: None,
        },
        // Rejected earlier in this function
        EmptyGroupBehavior::Unsupported => unreachable!(),
    };

    let replacement_expr = Expression::BinaryOp {
        op: op.clone(),
        left: Box::new(left.clone()),
        right: Box::new(comparison_rhs),
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Created LEFT JOIN with derived table alias: {}", alias);
        eprintln!("[SUBQUERY_TRANSFORM] Replacement expression: {:?}", replacement_expr);
    }

    Some(ScalarComparisonResult { from: new_from, replacement_expr })
}

/// Check if an expression contains an aggregate function
fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        // Direct aggregate function (SUM, COUNT, etc.)
        Expression::AggregateFunction { .. } => true,
        // Regular function - also check for aggregate function names
        Expression::Function { name, args, .. } => {
            let upper = name.canonical().to_uppercase();
            if matches!(
                upper.as_str(),
                "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
            ) {
                return true;
            }
            args.iter().any(contains_aggregate)
        }
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expression::Cast { expr, .. } => contains_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().map(|o| contains_aggregate(o)).unwrap_or(false)
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(contains_aggregate) || contains_aggregate(&w.result)
                })
                || else_result.as_ref().map(|e| contains_aggregate(e)).unwrap_or(false)
        }
        _ => false,
    }
}

/// How the subquery's SELECT expression behaves when the correlated group is
/// empty (i.e., the LEFT JOIN finds no matching derived-table row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmptyGroupBehavior {
    /// Aggregate yields NULL over an empty group (SUM/AVG/MIN/MAX/...),
    /// including when nested in a larger expression (e.g. `0.5 * SUM(x)`,
    /// which is NULL when SUM(x) is NULL). Compare directly against the
    /// joined column so NULL propagates and excludes the row.
    PropagateNull,
    /// Bare COUNT/TOTAL yields 0 (not NULL) over an empty group, so the
    /// missing LEFT JOIN match must be defaulted with COALESCE(.., 0).
    DefaultZero,
    /// COUNT/TOTAL nested inside a larger expression (e.g. `COUNT(*) + 5`):
    /// neither NULL nor 0 is the correct empty-group value, so the
    /// transformation must be skipped.
    Unsupported,
}

/// Classify how an aggregate SELECT expression behaves over an empty group
fn classify_empty_group_behavior(expr: &Expression) -> EmptyGroupBehavior {
    if is_bare_zero_on_empty_aggregate(expr) {
        EmptyGroupBehavior::DefaultZero
    } else if contains_zero_on_empty_aggregate(expr) {
        EmptyGroupBehavior::Unsupported
    } else {
        EmptyGroupBehavior::PropagateNull
    }
}

/// Check if a function name is an aggregate that returns 0 (not NULL) over an
/// empty group: COUNT and SQLite's TOTAL
fn is_zero_on_empty_name(name: &FunctionIdentifier) -> bool {
    matches!(name.canonical().to_uppercase().as_str(), "COUNT" | "TOTAL")
}

/// Check if the expression IS (at the top level) a bare COUNT/TOTAL call
fn is_bare_zero_on_empty_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { name, .. } => is_zero_on_empty_name(name),
        Expression::Function { name, .. } => is_zero_on_empty_name(name),
        _ => false,
    }
}

/// Check if the expression CONTAINS a COUNT/TOTAL call anywhere
fn contains_zero_on_empty_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { name, .. } => is_zero_on_empty_name(name),
        Expression::Function { name, args, .. } => {
            is_zero_on_empty_name(name) || args.iter().any(contains_zero_on_empty_aggregate)
        }
        Expression::BinaryOp { left, right, .. } => {
            contains_zero_on_empty_aggregate(left) || contains_zero_on_empty_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => contains_zero_on_empty_aggregate(expr),
        Expression::Cast { expr, .. } => contains_zero_on_empty_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().map(|o| contains_zero_on_empty_aggregate(o)).unwrap_or(false)
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(contains_zero_on_empty_aggregate)
                        || contains_zero_on_empty_aggregate(&w.result)
                })
                || else_result
                    .as_ref()
                    .map(|e| contains_zero_on_empty_aggregate(e))
                    .unwrap_or(false)
        }
        _ => false,
    }
}

/// Extract correlation predicates and non-correlated predicates from WHERE clause
///
/// Returns (correlation_predicates, non_correlated_predicates)
/// A correlation predicate is one that references both outer and inner tables
fn extract_correlation_predicates(
    expr: &Expression,
    outer_tables: &[String],
    inner_table: &str,
) -> Option<(Vec<Expression>, Vec<Expression>)> {
    let mut correlation = Vec::new();
    let mut non_correlated = Vec::new();

    extract_predicates_recursive(
        expr,
        outer_tables,
        inner_table,
        &mut correlation,
        &mut non_correlated,
    );

    Some((correlation, non_correlated))
}

fn extract_predicates_recursive(
    expr: &Expression,
    _outer_tables: &[String],
    inner_table: &str,
    correlation: &mut Vec<Expression>,
    non_correlated: &mut Vec<Expression>,
) {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            extract_predicates_recursive(
                left,
                _outer_tables,
                inner_table,
                correlation,
                non_correlated,
            );
            extract_predicates_recursive(
                right,
                _outer_tables,
                inner_table,
                correlation,
                non_correlated,
            );
        }
        Expression::Conjunction(children) => {
            for child in children {
                extract_predicates_recursive(
                    child,
                    _outer_tables,
                    inner_table,
                    correlation,
                    non_correlated,
                );
            }
        }
        _ => {
            // A correlation predicate is an equality that references both outer and inner tables
            // For the pattern: inner_col = outer_col
            // - One side should be from the inner table (lineitem)
            // - Other side should be from somewhere else (partsupp - the outer context)
            if is_correlation_predicate(expr, inner_table) {
                correlation.push(expr.clone());
            } else {
                non_correlated.push(expr.clone());
            }
        }
    }
}

/// Check if an expression is a correlation predicate (equality with mixed table references)
fn is_correlation_predicate(expr: &Expression, inner_table: &str) -> bool {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Check if one side is from inner table and other is from outer
            let left_inner = is_column_from_table(left, inner_table);
            let right_inner = is_column_from_table(right, inner_table);
            let left_outer = is_column_from_outer(left, inner_table);
            let right_outer = is_column_from_outer(right, inner_table);

            // Correlation: one from inner, one from outer
            (left_inner && right_outer) || (left_outer && right_inner)
        }
        _ => false,
    }
}

/// Check if a column reference is from the given table
fn is_column_from_table(expr: &Expression, table: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => {
            if let Some(t) = col_id.table_canonical() {
                t.eq_ignore_ascii_case(table)
            } else {
                // Unqualified - check if column name starts with table's typical prefix
                // For lineitem, columns start with l_
                // For partsupp, columns start with ps_
                let col = col_id.column_canonical();
                if table.eq_ignore_ascii_case("lineitem") {
                    col.starts_with("l_") || col.starts_with("L_")
                } else if table.eq_ignore_ascii_case("partsupp") {
                    col.starts_with("ps_") || col.starts_with("PS_")
                } else if table.eq_ignore_ascii_case("supplier") {
                    col.starts_with("s_") || col.starts_with("S_")
                } else if table.eq_ignore_ascii_case("part") {
                    col.starts_with("p_") || col.starts_with("P_")
                } else if table.eq_ignore_ascii_case("nation") {
                    col.starts_with("n_") || col.starts_with("N_")
                } else if table.eq_ignore_ascii_case("orders") {
                    col.starts_with("o_") || col.starts_with("O_")
                } else if table.eq_ignore_ascii_case("customer") {
                    col.starts_with("c_") || col.starts_with("C_")
                } else if table.eq_ignore_ascii_case("region") {
                    col.starts_with("r_") || col.starts_with("R_")
                } else {
                    false
                }
            }
        }
        _ => false,
    }
}

/// Check if a column reference is from outer scope (not from the inner table)
fn is_column_from_outer(expr: &Expression, inner_table: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => {
            if let Some(t) = col_id.table_canonical() {
                !t.eq_ignore_ascii_case(inner_table)
            } else {
                // Unqualified - check if it's NOT from the inner table
                !is_column_from_table(expr, inner_table)
            }
        }
        _ => false,
    }
}

/// Extract correlation column pairs from correlation predicates
///
/// Returns Vec<(outer_column, inner_column)>
/// The inner_column is from the subquery's table (lineitem),
/// the outer_column is from the outer context (partsupp)
fn extract_correlation_columns(
    predicates: &[Expression],
    inner_table: &str,
) -> Option<Vec<(String, String)>> {
    let mut result = Vec::new();

    for pred in predicates {
        match pred {
            Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
                // Match pattern: inner_col = outer_col or outer_col = inner_col
                if let (Expression::ColumnRef(left_col), Expression::ColumnRef(right_col)) =
                    (left.as_ref(), right.as_ref())
                {
                    let left_name = left_col.column_canonical().to_string();
                    let right_name = right_col.column_canonical().to_string();

                    // Determine which is outer and which is inner based on column prefix
                    let left_is_inner = is_column_from_table(left, inner_table);
                    let right_is_inner = is_column_from_table(right, inner_table);

                    if left_is_inner && !right_is_inner {
                        // left is inner (lineitem), right is outer (partsupp)
                        // Result: (outer_col, inner_col)
                        result.push((right_name, left_name));
                    } else if !left_is_inner && right_is_inner {
                        // left is outer (partsupp), right is inner (lineitem)
                        // Result: (outer_col, inner_col)
                        result.push((left_name, right_name));
                    } else {
                        // Can't determine, skip
                        return None;
                    }
                } else {
                    // Non-column equality, skip this predicate
                    return None;
                }
            }
            _ => {
                // Non-equality correlation predicate, can't decorrelate
                return None;
            }
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Combine multiple predicates with AND
fn combine_predicates(predicates: &[Expression]) -> Expression {
    if predicates.len() == 1 {
        return predicates[0].clone();
    }

    let mut result = predicates[0].clone();
    for pred in &predicates[1..] {
        result = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(result),
            right: Box::new(pred.clone()),
        };
    }
    result
}

/// Build join condition from correlation column pairs
///
/// Uses the aliased column names from the derived table to avoid ambiguity
/// when the inner and outer tables have the same name (e.g., both "lineitem" in Q17)
fn build_join_condition(
    correlation_columns: &[(String, String)],
    column_aliases: &[String],
    subquery_alias: &str,
) -> Expression {
    let conditions: Vec<Expression> = correlation_columns
        .iter()
        .zip(column_aliases.iter())
        .map(|((outer_col, _inner_col), col_alias)| {
            Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple(outer_col, false))),
                right: Box::new(Expression::ColumnRef(ColumnIdentifier::qualified(
                    subquery_alias,
                    false,
                    col_alias, // Use the alias (__corr_0, __corr_1, etc.) not the original column name
                    false,
                ))),
            }
        })
        .collect();

    combine_predicates(&conditions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::SelectItem;

    /// Build a Q20-style correlated scalar subquery:
    /// SELECT <select_expr> FROM lineitem WHERE l_partkey = ps_partkey
    fn correlated_lineitem_subquery(select_expr: Expression) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: select_expr,
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "lineitem".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("l_partkey", false))),
                right: Box::new(Expression::ColumnRef(ColumnIdentifier::simple(
                    "ps_partkey",
                    false,
                ))),
            }),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    /// Run the transform for: ps_availqty > (SELECT <select_expr> FROM lineitem
    /// WHERE l_partkey = ps_partkey), against outer table partsupp
    fn transform_with_select_expr(select_expr: Expression) -> Option<ScalarComparisonResult> {
        let from = FromClause::Table {
            index_hint: None,
            name: "partsupp".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        };
        let left = Expression::ColumnRef(ColumnIdentifier::simple("ps_availqty", false));
        let right = Expression::ScalarSubquery(Box::new(correlated_lineitem_subquery(select_expr)));
        try_convert_scalar_comparison_to_join(&from, &BinaryOperator::GreaterThan, &left, &right)
    }

    fn sum_quantity() -> Expression {
        Expression::AggregateFunction {
            name: FunctionIdentifier::new("SUM"),
            distinct: false,
            args: vec![Expression::ColumnRef(ColumnIdentifier::simple("l_quantity", false))],
            order_by: None,
            filter: None,
        }
    }

    fn count_star() -> Expression {
        Expression::AggregateFunction {
            name: FunctionIdentifier::new("COUNT"),
            distinct: false,
            args: vec![Expression::Wildcard],
            order_by: None,
            filter: None,
        }
    }

    /// Extract the right-hand side of the replacement comparison expression
    fn replacement_rhs(result: &ScalarComparisonResult) -> &Expression {
        match &result.replacement_expr {
            Expression::BinaryOp { right, .. } => right.as_ref(),
            other => panic!("Expected BinaryOp replacement, got {:?}", other),
        }
    }

    #[test]
    fn test_sum_comparison_has_no_coalesce() {
        // x > (SELECT SUM(...) correlated): SUM over an empty group is NULL,
        // so the comparison must reference the joined column directly —
        // COALESCE(.., 0) would wrongly include rows with no matching group
        // (TPC-H Q20 bug, issue #5274)
        let result = transform_with_select_expr(sum_quantity()).expect("transform should apply");
        match replacement_rhs(&result) {
            Expression::ColumnRef(col) => {
                assert_eq!(col.column_canonical(), "__scalar_agg");
            }
            other => panic!("Expected bare column ref (no COALESCE), got {:?}", other),
        }
    }

    #[test]
    fn test_sum_in_expression_has_no_coalesce() {
        // x > (SELECT 0.5 * SUM(...) correlated): NULL propagates through the
        // multiplication, so no COALESCE either (exact TPC-H Q20 shape)
        let half_sum = Expression::BinaryOp {
            op: BinaryOperator::Multiply,
            left: Box::new(Expression::Literal(SqlValue::Float(0.5))),
            right: Box::new(sum_quantity()),
        };
        let result = transform_with_select_expr(half_sum).expect("transform should apply");
        match replacement_rhs(&result) {
            Expression::ColumnRef(col) => {
                assert_eq!(col.column_canonical(), "__scalar_agg");
            }
            other => panic!("Expected bare column ref (no COALESCE), got {:?}", other),
        }
    }

    #[test]
    fn test_bare_count_comparison_keeps_coalesce_zero() {
        // x > (SELECT COUNT(*) correlated): COUNT over an empty group is 0,
        // so the missing LEFT JOIN match must default to 0 via COALESCE
        let result = transform_with_select_expr(count_star()).expect("transform should apply");
        match replacement_rhs(&result) {
            Expression::Function { name, args, .. } => {
                assert_eq!(name.canonical().to_uppercase(), "COALESCE");
                assert_eq!(args.len(), 2);
                assert!(
                    matches!(&args[1], Expression::Literal(SqlValue::Integer(0))),
                    "COALESCE default must be 0, got {:?}",
                    args[1]
                );
            }
            other => panic!("Expected COALESCE(.., 0), got {:?}", other),
        }
    }

    #[test]
    fn test_count_in_expression_skips_transformation() {
        // x > (SELECT COUNT(*) + 5 correlated): the empty-group value is 5
        // (neither NULL nor 0), which the LEFT JOIN cannot represent — must
        // bail out and fall back to per-row correlated execution
        let count_plus_five = Expression::BinaryOp {
            op: BinaryOperator::Plus,
            left: Box::new(count_star()),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        };
        assert!(
            transform_with_select_expr(count_plus_five).is_none(),
            "COUNT nested in expression must skip the transformation"
        );
    }

    #[test]
    fn test_classify_empty_group_behavior() {
        // Bare COUNT/TOTAL → DefaultZero
        assert_eq!(classify_empty_group_behavior(&count_star()), EmptyGroupBehavior::DefaultZero);
        let total = Expression::Function {
            name: FunctionIdentifier::new("TOTAL"),
            args: vec![Expression::ColumnRef(ColumnIdentifier::simple("qty", false))],
            character_unit: None,
        };
        assert_eq!(classify_empty_group_behavior(&total), EmptyGroupBehavior::DefaultZero);

        // SUM (bare or nested) → PropagateNull
        assert_eq!(
            classify_empty_group_behavior(&sum_quantity()),
            EmptyGroupBehavior::PropagateNull
        );
        let half_sum = Expression::BinaryOp {
            op: BinaryOperator::Multiply,
            left: Box::new(Expression::Literal(SqlValue::Float(0.5))),
            right: Box::new(sum_quantity()),
        };
        assert_eq!(classify_empty_group_behavior(&half_sum), EmptyGroupBehavior::PropagateNull);

        // COUNT nested in expression → Unsupported
        let count_plus_five = Expression::BinaryOp {
            op: BinaryOperator::Plus,
            left: Box::new(count_star()),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        };
        assert_eq!(
            classify_empty_group_behavior(&count_plus_five),
            EmptyGroupBehavior::Unsupported
        );
    }

    #[test]
    fn test_contains_aggregate() {
        // SUM is an aggregate
        let sum_expr = Expression::Function {
            name: FunctionIdentifier::new("SUM"),
            args: vec![Expression::ColumnRef(ColumnIdentifier::simple("qty", false))],
            character_unit: None,
        };
        assert!(contains_aggregate(&sum_expr));

        // 0.5 * SUM(...) contains aggregate
        let mul_sum = Expression::BinaryOp {
            op: BinaryOperator::Multiply,
            left: Box::new(Expression::Literal(SqlValue::Float(0.5))),
            right: Box::new(sum_expr),
        };
        assert!(contains_aggregate(&mul_sum));

        // Simple column is not aggregate
        let col = Expression::ColumnRef(ColumnIdentifier::simple("qty", false));
        assert!(!contains_aggregate(&col));
    }
}
