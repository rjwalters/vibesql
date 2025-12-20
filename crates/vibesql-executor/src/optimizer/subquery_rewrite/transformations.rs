//! IN subquery transformation strategies
//!
//! This module provides the core transformation logic for optimizing IN subqueries:
//! - Correlated IN → EXISTS with LIMIT 1 for early termination
//! - Uncorrelated IN → IN with DISTINCT to reduce duplicates

use vibesql_ast::{BinaryOperator, Expression, SelectItem, SelectStmt};

/// Rewrite correlated IN subquery to EXISTS with correlation predicate
///
/// Transforms:
/// ```sql
/// expr IN (SELECT col FROM table WHERE ...)
/// ```
///
/// To:
/// ```sql
/// EXISTS (SELECT 1 FROM table WHERE ... AND col = expr LIMIT 1)
/// ```
///
/// This allows the database to:
/// - Stop after finding first match (LIMIT 1 enables early termination)
/// - Better leverage indexes on the correlation column
/// - Potentially use better query plans
pub(super) fn rewrite_in_to_exists(
    in_expr: &Expression,
    subquery: &SelectStmt,
    negated: bool,
) -> Expression {
    // Create rewritten subquery
    let mut exists_subquery = subquery.clone();

    // Change SELECT list to SELECT 1 (EXISTS doesn't care about actual values)
    exists_subquery.select_list = vec![SelectItem::Expression {
        expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
        alias: None,
        source_text: None,
    }];

    // Add LIMIT 1 for early termination after first match
    // EXISTS only cares if ANY row matches, not all rows
    exists_subquery.limit = Some(Expression::Literal(vibesql_types::SqlValue::Integer(1)));

    // Add correlation predicate: subquery_col = outer_expr
    // We need to extract the subquery column from the original SELECT list
    // For now, we'll add the correlation predicate to the WHERE clause
    // The correlation will reference the original SELECT column

    // Extract the correlation column expression from original SELECT list
    // This assumes single-column subquery (already validated by executor)
    if let Some(SelectItem::Expression { expr: subquery_col, .. }) = subquery.select_list.first() {
        // Create correlation predicate: subquery_col = in_expr
        let correlation_predicate = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(subquery_col.clone()),
            right: Box::new(in_expr.clone()),
        };

        // Combine with existing WHERE clause using AND
        exists_subquery.where_clause = if let Some(existing_where) = &subquery.where_clause {
            Some(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(existing_where.clone()),
                right: Box::new(correlation_predicate),
            })
        } else {
            Some(correlation_predicate)
        };
    }

    // Create EXISTS expression
    Expression::Exists { subquery: Box::new(exists_subquery), negated }
}

/// Attempt to rewrite correlated EXISTS to uncorrelated IN
///
/// Transforms:
/// ```sql
/// EXISTS (SELECT * FROM inner WHERE inner.key = outer.key AND other_predicates)
/// ```
///
/// To:
/// ```sql
/// outer.key IN (SELECT DISTINCT inner.key FROM inner WHERE other_predicates)
/// ```
///
/// This decorrelation transforms O(n*m) nested loop execution into O(m) subquery
/// execution + O(n) hash lookups, providing massive performance improvement.
///
/// Returns Some((outer_expr, rewritten_subquery, negated)) if transformation is possible,
/// None if the EXISTS cannot be decorrelated.
pub(super) fn rewrite_exists_to_in(
    subquery: &SelectStmt,
    negated: bool,
    outer_tables: &[String],
) -> Option<(Expression, SelectStmt, bool)> {
    // Only handle simple single-table subqueries for now
    // Capture both table name and alias for proper column matching
    let (inner_table, inner_alias) = match &subquery.from {
        Some(vibesql_ast::FromClause::Table { name, alias, .. }) => (name.clone(), alias.clone()),
        _ => return None,
    };
    // Use alias if present, otherwise use table name for matching columns
    let inner_table_ref = inner_alias.as_ref().unwrap_or(&inner_table);

    // Extract correlation predicate from WHERE clause
    let where_clause = subquery.where_clause.as_ref()?;

    // Try to find and extract correlation predicate: inner.col = outer.col
    let (correlation, remaining_predicates) =
        extract_correlation_predicate(where_clause, inner_table_ref, outer_tables)?;

    // Build the decorrelated subquery
    let mut decorrelated = subquery.clone();
    decorrelated.distinct = true;

    // SELECT the inner correlation column
    decorrelated.select_list = vec![SelectItem::Expression {
        expr: correlation.inner_expr.clone(),
        alias: None,
        source_text: None,
    }];

    // Remove correlation predicate from WHERE, keep only remaining predicates
    decorrelated.where_clause = remaining_predicates;

    Some((correlation.outer_expr, decorrelated, negated))
}

/// Represents a correlation predicate: inner_expr = outer_expr
struct CorrelationPredicate {
    inner_expr: Expression,
    outer_expr: Expression,
}

/// Extract correlation predicate (inner.col = outer.col) from a WHERE clause
///
/// Returns the correlation info and remaining predicates (if any)
fn extract_correlation_predicate(
    expr: &Expression,
    inner_table: &str,
    outer_tables: &[String],
) -> Option<(CorrelationPredicate, Option<Expression>)> {
    match expr {
        // Direct equality: check if it's a correlation predicate
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            if let Some(correlation) =
                try_extract_correlation(left, right, inner_table, outer_tables)
            {
                return Some((correlation, None));
            }
            if let Some(correlation) =
                try_extract_correlation(right, left, inner_table, outer_tables)
            {
                return Some((correlation, None));
            }
            None
        }

        // AND: correlation might be one branch, other predicates in the other
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Try left branch for correlation
            if let Some((correlation, left_remaining)) =
                extract_correlation_predicate(left, inner_table, outer_tables)
            {
                let remaining = combine_predicates(left_remaining, Some((**right).clone()));
                return Some((correlation, remaining));
            }

            // Try right branch for correlation
            if let Some((correlation, right_remaining)) =
                extract_correlation_predicate(right, inner_table, outer_tables)
            {
                let remaining = combine_predicates(Some((**left).clone()), right_remaining);
                return Some((correlation, remaining));
            }

            None
        }

        _ => None,
    }
}

/// Try to identify a correlation predicate from two expressions
fn try_extract_correlation(
    left: &Expression,
    right: &Expression,
    inner_table: &str,
    outer_tables: &[String],
) -> Option<CorrelationPredicate> {
    // Check if left is from inner table and right is from outer table
    if is_from_table(left, inner_table) && is_from_outer_tables(right, outer_tables, inner_table) {
        return Some(CorrelationPredicate { inner_expr: left.clone(), outer_expr: right.clone() });
    }
    // Check reverse: right is from inner, left is from outer
    if is_from_table(right, inner_table) && is_from_outer_tables(left, outer_tables, inner_table) {
        return Some(CorrelationPredicate { inner_expr: right.clone(), outer_expr: left.clone() });
    }
    None
}

/// Check if an expression references a specific table
fn is_from_table(expr: &Expression, table: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() => {
            match col_id.table_canonical() {
                Some(t) => t.eq_ignore_ascii_case(table),
                None => true, // Unqualified could be from inner
            }
        }
        _ => false,
    }
}

/// Check if an expression references one of the outer tables
/// Also handles unqualified column names that start with outer table prefix (e.g., o_orderkey for
/// orders table)
fn is_from_outer_tables(expr: &Expression, outer_tables: &[String], inner_table: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() => {
            match col_id.table_canonical() {
                Some(t) => outer_tables.iter().any(|ot| ot.eq_ignore_ascii_case(t)),
                None => {
                    // Handle unqualified columns that use table prefix convention (e.g., o_orderkey for orders)
                    let column = col_id.column_canonical();
                    // Don't match if column starts with inner table's prefix
                    let inner_prefix =
                        inner_table.chars().next().unwrap_or('_').to_ascii_lowercase();
                    let col_prefix = column.chars().next().unwrap_or('_').to_ascii_lowercase();

                    // If column prefix matches an outer table's prefix but not inner table's prefix
                    if col_prefix != inner_prefix {
                        for outer in outer_tables {
                            let outer_prefix =
                                outer.chars().next().unwrap_or('_').to_ascii_lowercase();
                            if col_prefix == outer_prefix {
                                return true;
                            }
                        }
                    }
                    false
                }
            }
        }
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

/// Add DISTINCT to uncorrelated IN subquery to eliminate duplicates
///
/// Transforms:
/// ```sql
/// expr IN (SELECT col FROM table)
/// ```
///
/// To:
/// ```sql
/// expr IN (SELECT DISTINCT col FROM table)
/// ```
///
/// This reduces the number of comparisons by eliminating duplicate values
/// from the subquery result set.
///
/// Note: DISTINCT is NOT added when GROUP BY already guarantees uniqueness.
/// This allows the native columnar path (which doesn't support DISTINCT) to
/// be used for GROUP BY queries, providing significant performance benefits.
pub(super) fn add_distinct_to_in_subquery(subquery: &SelectStmt) -> SelectStmt {
    let mut optimized = subquery.clone();

    // Only add DISTINCT if not already present AND no GROUP BY
    // GROUP BY already guarantees distinct results when grouping columns
    // match the SELECT columns, so DISTINCT would be redundant.
    // Skipping DISTINCT enables the native columnar execution path for
    // GROUP BY + HAVING queries (Issue #4200: TPC-H Q18 optimization)
    if !subquery.distinct && !group_by_ensures_distinct(subquery) {
        optimized.distinct = true;
    }

    // Note: Recursive optimization is handled by the expression rewriter
    // to avoid circular dependencies between modules

    optimized
}

/// Check if GROUP BY clause ensures distinct results
///
/// Returns true when:
/// 1. Subquery has a GROUP BY clause
/// 2. SELECT list contains only columns from GROUP BY (or aggregates)
///
/// In these cases, GROUP BY guarantees uniqueness, making DISTINCT redundant.
fn group_by_ensures_distinct(subquery: &SelectStmt) -> bool {
    use vibesql_ast::{Expression, GroupByClause, SelectItem};

    // Must have GROUP BY
    let group_by_exprs = match &subquery.group_by {
        Some(GroupByClause::Simple(exprs)) => exprs,
        _ => return false,
    };

    if group_by_exprs.is_empty() {
        return false;
    }

    // Extract column names from GROUP BY expressions
    let group_by_cols: Vec<&str> = group_by_exprs
        .iter()
        .filter_map(|expr| match expr {
            Expression::ColumnRef(col_id) => Some(col_id.column_canonical()),
            _ => None,
        })
        .collect();

    // If GROUP BY contains non-column expressions, be conservative
    if group_by_cols.len() != group_by_exprs.len() {
        return false;
    }

    // Check that all SELECT items are either:
    // 1. Columns that appear in GROUP BY
    // 2. Aggregate functions (which are grouped by definition)
    for item in &subquery.select_list {
        match item {
            SelectItem::Expression { expr, .. } => match expr {
                Expression::ColumnRef(col_id) => {
                    // Column must be in GROUP BY
                    let column = col_id.column_canonical();
                    if !group_by_cols.iter().any(|&gc| gc.eq_ignore_ascii_case(column)) {
                        return false;
                    }
                }
                Expression::AggregateFunction { .. } => {
                    // Aggregates are always OK - they produce one value per group
                }
                _ => {
                    // Other expressions - be conservative
                    return false;
                }
            },
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                // Wildcards could include non-grouped columns
                return false;
            }
        }
    }

    true
}
