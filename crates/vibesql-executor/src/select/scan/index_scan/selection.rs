//! Index selection logic
//!
//! Determines when and which index to use for query optimization.
//! Supports both rule-based (simple) and cost-based (statistics-aware) selection.
//! Also includes skip-scan optimization for queries filtering on non-prefix columns.
//! Supports expression indexes (functional indexes) like CREATE INDEX idx ON t(lower(name)).

use vibesql_ast::{Expression, IndexColumn};
use vibesql_catalog::TableSchema;
use vibesql_storage::{
    statistics::{AccessMethod, CostEstimator},
    Database,
};

use crate::evaluator::expression_hash::ExpressionHasher;

use crate::optimizer::index_planner::{IndexPlanner, SkipScanInfo};

/// Result of index selection, distinguishing between regular and skip-scan
#[derive(Debug, Clone)]
pub(crate) enum IndexScanChoice {
    /// Regular index scan using prefix columns
    Regular {
        index_name: String,
        sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    },
    /// Skip-scan using non-prefix column filter
    SkipScan { index_name: String, skip_scan_info: SkipScanInfo },
}

/// Check if any ORDER BY column is nullable
///
/// BTreeMap orders NULLs first (NULL < everything), but SQL default is:
/// - NULLS LAST for ASC
/// - NULLS FIRST for DESC
///
/// When a nullable column is used for ORDER BY without explicit NULLS FIRST/LAST,
/// using an index would produce incorrect NULL ordering. This function helps
/// detect such cases to avoid using the index for ORDER BY.
fn any_order_by_column_nullable(
    order_items: &[vibesql_ast::OrderByItem],
    table_schema: &TableSchema,
) -> bool {
    for item in order_items {
        if let Expression::ColumnRef(col_id) = &item.expr {
            let column = col_id.column_canonical();
            // Look up column in schema (case-insensitive)
            if let Some(idx) = table_schema.get_column_index(column) {
                if table_schema.columns[idx].nullable {
                    return true;
                }
            }
        }
    }
    false
}

/// Determines if an index scan is beneficial for the given query
///
/// Returns Some((index_name, sorted_columns)) if an index should be used, None otherwise.
/// The sorted_columns vector indicates which columns are pre-sorted by the index scan.
#[allow(clippy::type_complexity)]
pub(crate) fn should_use_index_scan(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
    // We use indexes in three scenarios:
    // 1. WHERE clause references an indexed column (with or without ORDER BY)
    // 2. ORDER BY references an indexed column (even without WHERE)
    // 3. Both WHERE and ORDER BY use the same index
    //
    // Note: Index scans can provide partial optimization even for complex
    // predicates (including OR expressions). The full WHERE clause is always
    // applied as a post-filter in execute_index_scan() to ensure correctness.

    // Get all indexes for this table
    let table = database.get_table(table_name)?;
    let indexes = database.list_indexes_for_table(table_name);

    if indexes.is_empty() {
        return None;
    }

    // Find the best index (most pinned columns = better filtering)
    // We evaluate all applicable indexes and pick the one that covers the most WHERE columns
    let mut best_index: Option<(
        String,
        usize,
        bool,
        Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    )> = None;
    // (index_name, pinned_count, can_use_for_order, sorted_columns)

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            let first_indexed_column = index_metadata.columns.first()?;

            // Check if this index can be used for WHERE clause
            // Supports both column indexes and expression indexes
            let can_use_for_where = where_clause
                .map(|expr| index_column_can_filter(expr, first_indexed_column))
                .unwrap_or(false);

            // Count how many leading index columns are pinned by equality predicates
            // Note: For expression indexes, we count expression matches as pinned
            let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);

            // Check if this index can be used for ORDER BY clause
            let can_use_for_order = if let Some(order_items) = order_by {
                // Check if ORDER BY columns match the index columns (after skipping pinned columns)
                let columns_match = can_use_index_for_order_by_with_pinned(
                    order_items,
                    &index_metadata.columns,
                    pinned_columns,
                );

                // Don't use index for ORDER BY if any column is nullable
                // BTreeMap orders NULLs first, but SQL default is NULLS LAST for ASC
                // This would produce incorrect results for nullable columns
                if columns_match && any_order_by_column_nullable(order_items, &table.schema) {
                    false
                } else {
                    columns_match
                }
            } else {
                false
            };

            // Skip if this index can't help with WHERE or ORDER BY
            if !can_use_for_where && !can_use_for_order {
                continue;
            }

            // Build sorted_columns metadata if ORDER BY can be satisfied
            let sorted_columns = if can_use_for_order {
                let order_items = order_by.unwrap();
                Some(
                    order_items
                        .iter()
                        .map(|item| {
                            let col_name = match &item.expr {
                                Expression::ColumnRef(col_id) => {
                                    col_id.column_canonical().to_string()
                                }
                                _ => unreachable!(
                                    "can_use_index_for_order_by ensures simple column refs"
                                ),
                            };
                            (col_name, item.direction.clone())
                        })
                        .collect(),
                )
            } else {
                None
            };

            // Compare with best index so far
            // Prefer: more pinned columns > can satisfy ORDER BY > first found
            let is_better = match &best_index {
                None => true,
                Some((_, best_pinned, best_can_order, _)) => {
                    // More pinned columns = better (narrows down rows more)
                    if pinned_columns > *best_pinned {
                        true
                    } else if pinned_columns == *best_pinned {
                        // Same pinned count: prefer one that can satisfy ORDER BY
                        can_use_for_order && !*best_can_order
                    } else {
                        false
                    }
                }
            };

            if is_better {
                best_index =
                    Some((index_name.clone(), pinned_columns, can_use_for_order, sorted_columns));
            }
        }
    }

    // Return the best index if we found one
    if let Some((index_name, _, _, sorted_columns)) = best_index {
        return Some((index_name, sorted_columns));
    }

    None
}

/// Check if an expression filters a specific column
///
/// Returns true if the expression contains a predicate on the given column
/// For example: "WHERE age = 25" filters column "age"
pub(crate) fn expression_filters_column(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            // Check for comparison operators
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Index can only filter when comparing column to a LITERAL value
                    // NOT when comparing column to another column (equijoin conditions)
                    // e.g., `l_shipdate > '1995-03-15'` CAN use index
                    // e.g., `l_orderkey = o_orderkey` CANNOT use index
                    let left_is_col = is_column_reference(left, column_name);
                    let right_is_col = is_column_reference(right, column_name);
                    let left_is_literal = is_literal(left);
                    let right_is_literal = is_literal(right);

                    // column op literal OR literal op column
                    if (left_is_col && right_is_literal) || (left_is_literal && right_is_col) {
                        return true;
                    }
                }
                vibesql_ast::BinaryOperator::And | vibesql_ast::BinaryOperator::Or => {
                    // Recursively check sub-expressions for AND/OR
                    return expression_filters_column(left, column_name)
                        || expression_filters_column(right, column_name);
                }
                _ => {}
            }
            false
        }
        // IS / IS NOT (NULL-safe comparison)
        // negated=true means "IS NOT DISTINCT FROM" which is equivalent to "IS" (NULL-safe equals)
        // negated=false means "IS DISTINCT FROM" which is equivalent to "IS NOT" (NULL-safe not-equals)
        // We only use the index for IS (negated=true) as it's equivalent to =
        Expression::IsDistinctFrom { left, right, negated: true } => {
            let left_is_col = is_column_reference(left, column_name);
            let right_is_col = is_column_reference(right, column_name);
            let left_is_literal = is_literal(left);
            let right_is_literal = is_literal(right);
            // column IS literal OR literal IS column
            (left_is_col && right_is_literal) || (left_is_literal && right_is_col)
        }
        // IN with value list: col IN (1, 2, 3)
        Expression::InList { expr, .. } => is_column_reference(expr, column_name),
        // IN with subquery: col IN (SELECT ...)
        Expression::In { expr, .. } => is_column_reference(expr, column_name),
        // BETWEEN: col BETWEEN low AND high
        Expression::Between { expr, .. } => is_column_reference(expr, column_name),
        // Conjunction: AND
        Expression::Conjunction(exprs) => {
            exprs.iter().any(|e| expression_filters_column(e, column_name))
        }
        // Disjunction: OR
        Expression::Disjunction(exprs) => {
            exprs.iter().any(|e| expression_filters_column(e, column_name))
        }
        _ => false,
    }
}

/// Check if an expression matches an expression index
///
/// For expression indexes like `CREATE INDEX idx ON t(lower(name))`, this function
/// checks if the WHERE clause contains a predicate on the indexed expression.
/// For example: `WHERE lower(name) = 'john'` matches the index.
///
/// Uses structural hashing to compare expressions - two expressions are considered
/// equivalent if they have the same structure (same operations on same columns).
pub(crate) fn expression_filters_index_expression(
    where_expr: &Expression,
    index_expr: &Expression,
) -> bool {
    let index_hash = ExpressionHasher::hash(index_expr);
    expression_contains_matching_predicate(where_expr, index_hash)
}

/// Check if an expression contains a predicate on a specific expression (by hash)
///
/// Recursively searches through AND/OR combinations to find predicates.
fn expression_contains_matching_predicate(expr: &Expression, target_hash: u64) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Check if left or right side matches the indexed expression
                    let left_hash = ExpressionHasher::hash(left);
                    let right_hash = ExpressionHasher::hash(right);
                    let left_is_literal = is_literal(left);
                    let right_is_literal = is_literal(right);

                    // expr op literal OR literal op expr
                    if (left_hash == target_hash && right_is_literal)
                        || (right_hash == target_hash && left_is_literal)
                    {
                        return true;
                    }
                }
                vibesql_ast::BinaryOperator::And | vibesql_ast::BinaryOperator::Or => {
                    return expression_contains_matching_predicate(left, target_hash)
                        || expression_contains_matching_predicate(right, target_hash);
                }
                _ => {}
            }
            false
        }
        // IS / IS NOT (NULL-safe comparison)
        Expression::IsDistinctFrom { left, right, negated: true } => {
            let left_hash = ExpressionHasher::hash(left);
            let right_hash = ExpressionHasher::hash(right);
            let left_is_literal = is_literal(left);
            let right_is_literal = is_literal(right);
            (left_hash == target_hash && right_is_literal)
                || (right_hash == target_hash && left_is_literal)
        }
        // IN with value list: expr IN (1, 2, 3)
        Expression::InList { expr, .. } => ExpressionHasher::hash(expr) == target_hash,
        // IN with subquery: expr IN (SELECT ...)
        Expression::In { expr, .. } => ExpressionHasher::hash(expr) == target_hash,
        // BETWEEN: expr BETWEEN low AND high
        Expression::Between { expr, .. } => ExpressionHasher::hash(expr) == target_hash,
        // Conjunction: AND
        Expression::Conjunction(exprs) => exprs
            .iter()
            .any(|e| expression_contains_matching_predicate(e, target_hash)),
        // Disjunction: OR
        Expression::Disjunction(exprs) => exprs
            .iter()
            .any(|e| expression_contains_matching_predicate(e, target_hash)),
        _ => false,
    }
}

/// Check if an IndexColumn can be used for the given WHERE clause
///
/// This function handles both column indexes and expression indexes:
/// - For column indexes: delegates to `expression_filters_column`
/// - For expression indexes: uses `expression_filters_index_expression`
pub(crate) fn index_column_can_filter(where_expr: &Expression, index_col: &IndexColumn) -> bool {
    match index_col {
        IndexColumn::Column { column_name, .. } => {
            expression_filters_column(where_expr, column_name)
        }
        IndexColumn::Expression { expr, .. } => {
            expression_filters_index_expression(where_expr, expr)
        }
    }
}

/// Check if an expression is a reference to a specific column.
///
/// Uses case-insensitive comparison because:
/// - ColumnIdentifier.column_canonical() returns lowercase for unquoted identifiers
/// - Schema metadata (column_name) may not be consistently lowercased
/// - For SQL:1999 compliance with quoted identifiers, both sides should use canonical forms
///
/// TODO: Once schema metadata consistently uses canonical forms, change to direct equality
pub(super) fn is_column_reference(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => {
            col_id.column_canonical().eq_ignore_ascii_case(column_name)
        }
        _ => false,
    }
}

/// Check if an expression is a non-NULL literal value or parameter placeholder
///
/// Index scans can filter on columns compared to literal values or bound parameters,
/// not columns compared to other columns (which are equijoin conditions).
/// Parameter placeholders (?, $1, :name, etc.) are treated as literals for index selection
/// because they are resolved to concrete values at execution time.
///
/// **Important**: NULL literals are excluded because `col = NULL` can never return true
/// (per SQL three-valued logic), so using an index for this predicate is incorrect.
fn is_literal(expr: &Expression) -> bool {
    match expr {
        // Exclude NULL literals - col = NULL can never match rows
        Expression::Literal(vibesql_types::SqlValue::Null) => false,
        Expression::Literal(_) => true,
        Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => true,
        _ => false,
    }
}

/// Check if two expressions are structurally equivalent for index matching purposes.
///
/// This performs a normalized comparison of expressions, suitable for matching
/// WHERE clause expressions against expression index definitions.
///
/// Normalization includes:
/// - Case-insensitive comparison of column names and function names
/// - Order-independent comparison for commutative operators where applicable
///
/// # Example
/// ```text
/// Index: CREATE INDEX idx ON t(LOWER(email))
/// Query: SELECT * FROM t WHERE LOWER(email) = 'x'
///
/// expressions_match(LOWER(email), LOWER(email)) -> true
/// ```
pub(crate) fn expressions_match(expr1: &Expression, expr2: &Expression) -> bool {
    match (expr1, expr2) {
        // Column references: case-insensitive comparison
        (Expression::ColumnRef(col1), Expression::ColumnRef(col2)) => {
            col1.column_canonical().eq_ignore_ascii_case(&col2.column_canonical())
        }

        // Function calls: compare function name and arguments
        (
            Expression::Function { name: name1, args: args1, .. },
            Expression::Function { name: name2, args: args2, .. },
        ) => {
            // Function names are case-insensitive in SQL
            name1.canonical().eq_ignore_ascii_case(name2.canonical())
                && args1.len() == args2.len()
                && args1.iter().zip(args2.iter()).all(|(a1, a2)| expressions_match(a1, a2))
        }

        // Binary operations: compare operator and operands
        (
            Expression::BinaryOp { left: l1, op: op1, right: r1 },
            Expression::BinaryOp { left: l2, op: op2, right: r2 },
        ) => op1 == op2 && expressions_match(l1, l2) && expressions_match(r1, r2),

        // Unary operations
        (Expression::UnaryOp { op: op1, expr: e1 }, Expression::UnaryOp { op: op2, expr: e2 }) => {
            op1 == op2 && expressions_match(e1, e2)
        }

        // Cast expressions
        (
            Expression::Cast { expr: e1, data_type: t1 },
            Expression::Cast { expr: e2, data_type: t2 },
        ) => t1 == t2 && expressions_match(e1, e2),

        // Literals: exact match
        (Expression::Literal(v1), Expression::Literal(v2)) => v1 == v2,

        // CASE expressions
        (
            Expression::Case { operand: op1, when_clauses: wc1, else_result: er1 },
            Expression::Case { operand: op2, when_clauses: wc2, else_result: er2 },
        ) => {
            // Compare operand
            let op_match = match (op1, op2) {
                (Some(o1), Some(o2)) => expressions_match(o1, o2),
                (None, None) => true,
                _ => false,
            };
            // Compare else
            let else_match = match (er1, er2) {
                (Some(e1), Some(e2)) => expressions_match(e1, e2),
                (None, None) => true,
                _ => false,
            };
            // Compare when clauses (conditions is a Vec, result is Expression)
            op_match
                && else_match
                && wc1.len() == wc2.len()
                && wc1.iter().zip(wc2.iter()).all(|(w1, w2)| {
                    w1.conditions.len() == w2.conditions.len()
                        && w1
                            .conditions
                            .iter()
                            .zip(w2.conditions.iter())
                            .all(|(c1, c2)| expressions_match(c1, c2))
                        && expressions_match(&w1.result, &w2.result)
                })
        }

        // NULL handling
        (
            Expression::IsNull { expr: e1, negated: n1 },
            Expression::IsNull { expr: e2, negated: n2 },
        ) => n1 == n2 && expressions_match(e1, e2),

        // Conjunction (AND chain)
        (Expression::Conjunction(exprs1), Expression::Conjunction(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1.iter().zip(exprs2.iter()).all(|(e1, e2)| expressions_match(e1, e2))
        }

        // Disjunction (OR chain)
        (Expression::Disjunction(exprs1), Expression::Disjunction(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1.iter().zip(exprs2.iter()).all(|(e1, e2)| expressions_match(e1, e2))
        }

        // For all other expression types, they must be exactly equal
        // This is conservative but safe
        _ => expr1 == expr2,
    }
}

/// Case-insensitive lookup of column statistics
///
/// Returns the ColumnStatistics for a column name, ignoring case differences.
/// This is necessary because schema column names may use different casing than
/// index column names or query column references.
fn get_column_stats_ignore_case<'a>(
    columns: &'a std::collections::HashMap<String, vibesql_storage::statistics::ColumnStatistics>,
    column_name: &str,
) -> Option<&'a vibesql_storage::statistics::ColumnStatistics> {
    // First try exact match for efficiency
    if let Some(stats) = columns.get(column_name) {
        return Some(stats);
    }
    // Fall back to case-insensitive search
    columns.iter().find(|(key, _)| key.eq_ignore_ascii_case(column_name)).map(|(_, stats)| stats)
}

/// Check if an index can be used to satisfy an ORDER BY clause
///
/// Returns true if the ORDER BY columns match the index columns (after skipping
/// any prefix columns pinned by equality predicates) and the sort directions
/// are compatible (either all matching or all reversed).
///
/// Examples:
/// - ORDER BY col0 ASC can use index (col0 ASC)
/// - ORDER BY col0 DESC can use index (col0 DESC) via reversal
/// - ORDER BY col0, col1 can use index (col0, col1)
/// - WHERE col0 = 1 ORDER BY col1 can use index (col0, col1) - col0 is pinned
pub(crate) fn can_use_index_for_order_by(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
) -> bool {
    can_use_index_for_order_by_with_pinned(order_items, index_columns, 0)
}

/// Check if an index can be used for ORDER BY, accounting for pinned prefix columns
///
/// When a query has equality predicates on leading index columns (e.g., WHERE a = 1 AND b = 2),
/// those columns are "pinned" and the index is effectively sorted by the remaining columns.
/// This function skips over the pinned columns and checks if the ORDER BY matches.
///
/// For example, with index (a, b, c):
/// - WHERE a = 1 ORDER BY b, c → can use index (skip 1 pinned column)
/// - WHERE a = 1 AND b = 2 ORDER BY c → can use index (skip 2 pinned columns)
/// - WHERE a = 1 ORDER BY c → cannot use index (b must come before c)
pub(crate) fn can_use_index_for_order_by_with_pinned(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
    pinned_columns: usize,
) -> bool {
    // Skip pinned columns
    let remaining_index_columns = &index_columns[pinned_columns..];

    // ORDER BY must not have more columns than remaining index columns
    if order_items.len() > remaining_index_columns.len() {
        return false;
    }

    // If no ORDER BY columns, nothing to match
    if order_items.is_empty() {
        return false;
    }

    // Check if ORDER BY matches index direction or is completely reversed
    // (allowing reverse scan to satisfy DESC ordering)
    let mut all_match = true;
    let mut all_reversed = true;

    // Check each ORDER BY column against corresponding index column
    for (order_item, index_col) in order_items.iter().zip(remaining_index_columns.iter()) {
        // For expression indexes, we need to check if ORDER BY uses the same expression
        // For column indexes, ORDER BY must be a simple column reference
        match index_col {
            vibesql_ast::IndexColumn::Column { column_name, .. } => {
                // ORDER BY expression must be a simple column reference
                let order_col_name = match &order_item.expr {
                    Expression::ColumnRef(col_id)
                        if col_id.schema_canonical().is_none()
                            && col_id.table_canonical().is_none() =>
                    {
                        col_id.column_canonical()
                    }
                    _ => return false, // Complex expressions not supported for column indexes
                };

                // Column names must match (case-insensitive due to SQL identifier normalization)
                if !order_col_name.eq_ignore_ascii_case(column_name) {
                    return false;
                }
            }
            vibesql_ast::IndexColumn::Expression { expr: index_expr, .. } => {
                // For expression indexes, ORDER BY must use the exact same expression
                // e.g., ORDER BY LOWER(name) can use index on LOWER(name)
                if !expressions_match(&order_item.expr, index_expr) {
                    return false;
                }
            }
        }

        // Check sort directions
        let directions_match = order_item.direction == index_col.direction();
        let directions_opposite = matches!(
            (&order_item.direction, &index_col.direction()),
            (vibesql_ast::OrderDirection::Asc, vibesql_ast::OrderDirection::Desc)
                | (vibesql_ast::OrderDirection::Desc, vibesql_ast::OrderDirection::Asc)
        );

        if !directions_match {
            all_match = false;
        }
        if !directions_opposite {
            all_reversed = false;
        }
    }

    // Accept if all directions match OR all are reversed (reverse scan)
    all_match || all_reversed
}

/// Count how many leading index columns are pinned by equality predicates
///
/// A column is "pinned" if there's an equality predicate (col = value) in the WHERE clause.
/// For expression indexes, an expression is "pinned" if there's an equality predicate
/// (expr = value) where expr matches the indexed expression.
/// Returns the number of consecutive leading index columns that are pinned.
pub(crate) fn count_pinned_index_columns(
    where_clause: Option<&Expression>,
    index_columns: &[vibesql_ast::IndexColumn],
) -> usize {
    let where_clause = match where_clause {
        Some(expr) => expr,
        None => return 0,
    };

    // Collect all columns that have equality predicates
    let mut pinned_columns = std::collections::HashSet::new();
    collect_equality_columns(where_clause, &mut pinned_columns);

    // Collect all expressions that have equality predicates (for expression indexes)
    let mut pinned_expressions: Vec<&Expression> = Vec::new();
    collect_equality_expressions(where_clause, &mut pinned_expressions);

    // Count consecutive leading index columns that are pinned
    let mut count = 0;
    for index_col in index_columns {
        let is_pinned = match index_col {
            vibesql_ast::IndexColumn::Column { column_name, .. } => {
                // Check if this column is pinned (case-insensitive match)
                pinned_columns.iter().any(|c| c.eq_ignore_ascii_case(column_name))
            }
            vibesql_ast::IndexColumn::Expression { expr: index_expr, .. } => {
                // Check if any pinned expression matches the indexed expression
                pinned_expressions.iter().any(|e| expressions_match(e, index_expr))
            }
        };

        if is_pinned {
            count += 1;
        } else {
            break; // Stop at first non-pinned column
        }
    }
    count
}

/// Collect all column names that have equality predicates (col = literal or col IS literal)
fn collect_equality_columns(expr: &Expression, columns: &mut std::collections::HashSet<String>) {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal => {
                    // Check for column = literal pattern
                    if let Expression::ColumnRef(col_id) = &**left {
                        if is_literal(right) {
                            columns.insert(col_id.column_canonical().to_uppercase());
                        }
                    }
                    if let Expression::ColumnRef(col_id) = &**right {
                        if is_literal(left) {
                            columns.insert(col_id.column_canonical().to_uppercase());
                        }
                    }
                }
                vibesql_ast::BinaryOperator::And => {
                    // Recurse into both sides of AND
                    collect_equality_columns(left, columns);
                    collect_equality_columns(right, columns);
                }
                _ => {}
            }
        }
        // Handle IS (NULL-safe equals): negated=true means "IS NOT DISTINCT FROM" = "IS"
        Expression::IsDistinctFrom { left, right, negated: true } => {
            // Check for column IS literal pattern
            if let Expression::ColumnRef(col_id) = &**left {
                if is_literal(right) {
                    columns.insert(col_id.column_canonical().to_uppercase());
                }
            }
            if let Expression::ColumnRef(col_id) = &**right {
                if is_literal(left) {
                    columns.insert(col_id.column_canonical().to_uppercase());
                }
            }
        }
        // Recurse into Conjunction (AND)
        Expression::Conjunction(exprs) => {
            for e in exprs {
                collect_equality_columns(e, columns);
            }
        }
        _ => {}
    }
}

/// Collect all expressions that have equality predicates (expr = literal)
///
/// This is used for expression index support. It collects non-column expressions
/// that are compared to literals in equality predicates.
fn collect_equality_expressions<'a>(
    expr: &'a Expression,
    expressions: &mut Vec<&'a Expression>,
) {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal => {
                    // Check for expr = literal pattern (where expr is not a simple column)
                    if is_literal(right) && !matches!(&**left, Expression::ColumnRef(_)) {
                        expressions.push(left);
                    }
                    if is_literal(left) && !matches!(&**right, Expression::ColumnRef(_)) {
                        expressions.push(right);
                    }
                }
                vibesql_ast::BinaryOperator::And => {
                    // Recurse into both sides of AND
                    collect_equality_expressions(left, expressions);
                    collect_equality_expressions(right, expressions);
                }
                _ => {}
            }
        }
        // Handle IS (NULL-safe equals): negated=true means "IS NOT DISTINCT FROM" = "IS"
        Expression::IsDistinctFrom { left, right, negated: true } => {
            // Check for expr IS literal pattern
            if is_literal(right) && !matches!(&**left, Expression::ColumnRef(_)) {
                expressions.push(left);
            }
            if is_literal(left) && !matches!(&**right, Expression::ColumnRef(_)) {
                expressions.push(right);
            }
        }
        // Recurse into Conjunction (AND)
        Expression::Conjunction(exprs) => {
            for e in exprs {
                collect_equality_expressions(e, expressions);
            }
        }
        _ => {}
    }
}

/// Cost-based index selection using statistics
///
/// This function uses table and column statistics to make intelligent decisions
/// about whether to use an index scan or a table scan. It estimates the cost of
/// both access methods and chooses the cheaper one.
///
/// # Arguments
/// * `table_name` - Name of the table being queried
/// * `where_clause` - Optional WHERE clause predicate
/// * `order_by` - Optional ORDER BY clause
/// * `database` - Database reference for accessing statistics and indexes
///
/// # Returns
/// - `Some((index_name, sorted_columns))` if cost-based analysis suggests using an index
/// - `None` if table scan is cheaper or no statistics are available
///
/// # Fallback Behavior
/// If statistics are not available or stale, falls back to rule-based selection
/// using `should_use_index_scan()`.
#[allow(clippy::type_complexity)]
pub(crate) fn cost_based_index_selection(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
    // Get table statistics
    let table = database.get_table(table_name)?;
    let table_stats = table.get_statistics();

    // If no statistics or stale, fall back to rule-based selection
    if table_stats.is_none() || table_stats.as_ref().map(|s| s.needs_refresh()).unwrap_or(false) {
        return should_use_index_scan(table_name, where_clause, order_by, database);
    }

    let table_stats = table_stats.unwrap();
    let cost_estimator = CostEstimator::default();

    // Get all indexes for this table
    let indexes = database.list_indexes_for_table(table_name);
    if indexes.is_empty() {
        return None; // No indexes available
    }

    // Try each index and find the one with best score (pinned columns + cost)
    #[allow(clippy::type_complexity)]
    let mut best_index: Option<(
        String,
        AccessMethod,
        usize,
        Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    )> = None;
    // (index_name, access_method, pinned_count, sorted_columns)
    let mut has_applicable_index_without_stats = false;

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            let first_indexed_column = index_metadata.columns.first()?;

            // Check if this index can be used for WHERE or ORDER BY
            // Supports both column indexes and expression indexes
            let can_use_for_where = where_clause
                .map(|expr| index_column_can_filter(expr, first_indexed_column))
                .unwrap_or(false);

            // Count how many leading index columns are pinned by equality predicates
            let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);

            // Get column name for stats lookup (only for column indexes)
            let column_name = first_indexed_column.column_name();

            // Debug: trace index selection
            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                let col_debug = column_name.unwrap_or("<expression>");
                eprintln!(
                    "[INDEX_SELECT] table={}, index={}, first_col={}, can_use_for_where={}",
                    table_name, index_name, col_debug, can_use_for_where
                );
            }

            let can_use_for_order = if let Some(order_items) = order_by {
                // Check if ORDER BY columns match the index columns (after skipping pinned columns)
                let columns_match = can_use_index_for_order_by_with_pinned(
                    order_items,
                    &index_metadata.columns,
                    pinned_columns,
                );

                // Don't use index for ORDER BY if any column is nullable
                // BTreeMap orders NULLs first, but SQL default is NULLS LAST for ASC
                // This would produce incorrect results for nullable columns
                if columns_match && any_order_by_column_nullable(order_items, &table.schema) {
                    false
                } else {
                    columns_match
                }
            } else {
                false
            };

            // Skip this index if it can't help with WHERE or ORDER BY
            if !can_use_for_where && !can_use_for_order {
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] skipping {} - can't use for where or order",
                        index_name
                    );
                }
                continue;
            }

            // Debug: continue trace
            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!(
                    "[INDEX_SELECT] {} passed where/order check, checking stats...",
                    index_name
                );
            }

            // For expression indexes, we don't have column statistics
            // Fall back to rule-based selection for them
            if first_indexed_column.is_expression() {
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] {} is expression index, will use rule-based selection",
                        index_name
                    );
                }
                has_applicable_index_without_stats = true;
                continue;
            }

            // Get column statistics for the indexed column (case-insensitive lookup)
            // For expression indexes, we don't have direct column stats - fall back to rule-based
            let col_stats = column_name.and_then(|cn| {
                get_column_stats_ignore_case(&table_stats.columns, cn)
            });
            if col_stats.is_none() {
                // Track that we found an applicable index without column stats
                // We'll fall back to rule-based selection if cost-based fails
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    let col_debug = column_name.unwrap_or("<expression>");
                    eprintln!(
                        "[INDEX_SELECT] {} no column stats for {}, will fallback",
                        index_name, col_debug
                    );
                }
                has_applicable_index_without_stats = true;
                continue; // No stats for this column, try next index
            }
            let col_stats = col_stats.unwrap();
            // At this point, column_name must be Some since we got col_stats
            let column_name = column_name.unwrap();

            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!("[INDEX_SELECT] {} has column stats for {}", index_name, column_name);
            }

            // Estimate selectivity based on WHERE clause
            let selectivity = if let Some(where_expr) = where_clause {
                estimate_selectivity(where_expr, column_name, col_stats)
            } else {
                1.0 // No WHERE clause means all rows
            };

            // Use cost estimator to decide
            let access_method =
                cost_estimator.choose_access_method(table_stats, Some(col_stats), selectivity);

            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!(
                    "[INDEX_SELECT] {} selectivity={:.4}, access_method={:?}, is_index_scan={}",
                    index_name,
                    selectivity,
                    access_method,
                    access_method.is_index_scan()
                );
            }

            // Build sorted_columns metadata if ORDER BY can be satisfied
            let sorted_columns = if can_use_for_order {
                let order_items = order_by.unwrap();
                Some(
                    order_items
                        .iter()
                        .map(|item| {
                            let col_name = match &item.expr {
                                Expression::ColumnRef(col_id) => {
                                    col_id.column_canonical().to_string()
                                }
                                _ => unreachable!(
                                    "can_use_index_for_order_by ensures simple column refs"
                                ),
                            };
                            (col_name, item.direction.clone())
                        })
                        .collect(),
                )
            } else {
                None
            };

            // Track the best index
            // Priority: more pinned columns (better filtering) > lower cost
            if access_method.is_index_scan() {
                let is_better = match &best_index {
                    None => true,
                    Some((_, best_method, best_pinned, _)) => {
                        // More pinned columns is always better (filters more rows)
                        if pinned_columns > *best_pinned {
                            true
                        } else if pinned_columns == *best_pinned {
                            // Same pinned count: prefer lower cost
                            access_method.cost() < best_method.cost()
                        } else {
                            false
                        }
                    }
                };

                if is_better {
                    best_index =
                        Some((index_name.clone(), access_method, pinned_columns, sorted_columns));
                }
            } else if selectivity < 0.40 && can_use_for_where {
                // Cost-based chose table scan, but selectivity is good enough for index
                // The cost model may be too conservative for in-memory/prefix scans
                // Common fallback values: 0.33 (single predicate), 0.1089 (two predicates)
                // Fall back to rule-based selection for selective queries
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] {} selectivity={:.4} good, falling back to rule-based",
                        index_name, selectivity
                    );
                }
                return should_use_index_scan(table_name, where_clause, order_by, database);
            }
        }
    }

    // Return the best index if we found one
    if let Some((index_name, _, _, sorted_columns)) = best_index {
        if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
            eprintln!("[INDEX_SELECT] selected best_index={} for table={}", index_name, table_name);
        }
        return Some((index_name, sorted_columns));
    }

    // If we have applicable indexes but no column stats, fall back to rule-based selection
    // This ensures we use indexes even when statistics are incomplete
    if has_applicable_index_without_stats {
        if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
            eprintln!("[INDEX_SELECT] falling back to rule-based for table={}", table_name);
        }
        return should_use_index_scan(table_name, where_clause, order_by, database);
    }

    // No applicable indexes found
    if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
        eprintln!("[INDEX_SELECT] no index selected for table={}", table_name);
    }
    None
}

/// Estimate selectivity of a predicate on a specific column
///
/// Uses column statistics to estimate what fraction of rows will match the predicate.
/// Returns a value between 0.0 (no rows) and 1.0 (all rows).
pub(crate) fn estimate_selectivity(
    expr: &Expression,
    column_name: &str,
    col_stats: &vibesql_storage::statistics::ColumnStatistics,
) -> f64 {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal => {
                    // Check if this is a predicate on our column (case-insensitive)
                    // For literal values, use actual statistics
                    if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                        (&**left, &**right)
                    {
                        if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    if let (Expression::Literal(value), Expression::ColumnRef(col_id)) =
                        (&**left, &**right)
                    {
                        if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    // For placeholder parameters, estimate using 1/n_distinct
                    // This is more accurate than the generic 0.33 fallback for equality predicates
                    let left_is_col = is_column_reference(left, column_name);
                    let right_is_col = is_column_reference(right, column_name);
                    let left_is_lit = is_literal(left);
                    let right_is_lit = is_literal(right);

                    if (left_is_col && right_is_lit) || (left_is_lit && right_is_col) {
                        // Use 1/n_distinct as selectivity estimate for equality with parameter
                        if col_stats.n_distinct > 0 {
                            return 1.0 / col_stats.n_distinct as f64;
                        }
                    }
                    0.33 // Default fallback
                }
                vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Range predicates (case-insensitive column comparison)
                    if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                        (&**left, &**right)
                    {
                        if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                            let op_str = match op {
                                vibesql_ast::BinaryOperator::GreaterThan => ">",
                                vibesql_ast::BinaryOperator::GreaterThanOrEqual => ">=",
                                vibesql_ast::BinaryOperator::LessThan => "<",
                                vibesql_ast::BinaryOperator::LessThanOrEqual => "<=",
                                _ => unreachable!(),
                            };
                            return col_stats.estimate_range_selectivity(value, op_str);
                        }
                    }
                    // For placeholder parameters, use a conservative 0.25 (assume filtering ~75% of
                    // rows)
                    if (is_column_reference(left, column_name) && is_literal(right))
                        || (is_literal(left) && is_column_reference(right, column_name))
                    {
                        return 0.25;
                    }
                    0.33 // Default fallback
                }
                vibesql_ast::BinaryOperator::And => {
                    // For AND, multiply selectivities (assuming independence)
                    let left_sel = estimate_selectivity(left, column_name, col_stats);
                    let right_sel = estimate_selectivity(right, column_name, col_stats);
                    left_sel * right_sel
                }
                vibesql_ast::BinaryOperator::Or => {
                    // For OR, use formula: P(A OR B) = P(A) + P(B) - P(A AND B)
                    // Assuming independence: P(A OR B) = P(A) + P(B) - P(A)*P(B)
                    let left_sel = estimate_selectivity(left, column_name, col_stats);
                    let right_sel = estimate_selectivity(right, column_name, col_stats);
                    left_sel + right_sel - (left_sel * right_sel)
                }
                _ => 0.33, // Default fallback for other operators
            }
        }
        Expression::Between { expr, low, high, negated: _, symmetric: _ } => {
            if let Expression::ColumnRef(col_id) = &**expr {
                if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                    // Estimate BETWEEN as: P(col >= low AND col <= high)
                    if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                        (&**low, &**high)
                    {
                        let low_sel = col_stats.estimate_range_selectivity(low_val, ">=");
                        let high_sel = col_stats.estimate_range_selectivity(high_val, "<=");
                        return low_sel * high_sel; // Assuming independence
                    }
                }
            }
            0.33 // Default fallback
        }
        // IS (NULL-safe equals): negated=true means "IS NOT DISTINCT FROM" = "IS"
        // Treat IS like = for selectivity estimation
        Expression::IsDistinctFrom { left, right, negated: true } => {
            // Check if this is a predicate on our column (case-insensitive)
            // For literal values, use actual statistics
            if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                (&**left, &**right)
            {
                if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                    return col_stats.estimate_eq_selectivity(value);
                }
            }
            if let (Expression::Literal(value), Expression::ColumnRef(col_id)) =
                (&**left, &**right)
            {
                if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                    return col_stats.estimate_eq_selectivity(value);
                }
            }
            // For placeholder parameters, estimate using 1/n_distinct
            let left_is_col = is_column_reference(left, column_name);
            let right_is_col = is_column_reference(right, column_name);
            let left_is_lit = is_literal(left);
            let right_is_lit = is_literal(right);

            if (left_is_col && right_is_lit) || (left_is_lit && right_is_col) {
                if col_stats.n_distinct > 0 {
                    return 1.0 / col_stats.n_distinct as f64;
                }
            }
            0.33 // Default fallback
        }
        // Handle Conjunction (AND) similarly to BinaryOp::And
        Expression::Conjunction(exprs) => {
            let mut selectivity = 1.0;
            for e in exprs {
                selectivity *= estimate_selectivity(e, column_name, col_stats);
            }
            selectivity
        }
        // Handle Disjunction (OR) similarly to BinaryOp::Or
        Expression::Disjunction(exprs) => {
            let mut selectivity = 0.0;
            let mut product = 1.0;
            for e in exprs {
                let sel = estimate_selectivity(e, column_name, col_stats);
                selectivity += sel;
                product *= sel;
            }
            // P(A OR B OR C) ≈ P(A) + P(B) + P(C) - P(A)*P(B)*P(C) (simplified)
            (selectivity - product).min(1.0)
        }
        _ => 0.33, // Default fallback for unsupported expressions
    }
}

/// Unified index selection that returns IndexScanChoice
///
/// This function first tries regular index selection, and if no suitable index is found,
/// it attempts skip-scan optimization as a fallback. Skip-scan enables using composite
/// indexes when the WHERE clause filters on non-prefix columns.
///
/// # Arguments
/// * `table_name` - Name of the table being queried
/// * `where_clause` - Optional WHERE clause predicate
/// * `order_by` - Optional ORDER BY clause
/// * `database` - Database reference for accessing statistics and indexes
///
/// # Returns
/// - `Some(IndexScanChoice::Regular {...})` if a regular index scan should be used
/// - `Some(IndexScanChoice::SkipScan {...})` if skip-scan is beneficial
/// - `None` if table scan is more appropriate
///
/// # Example
/// ```text
/// // Query: SELECT * FROM sales WHERE date = '2024-01-01'
/// // Index: (region, date) - a composite index
/// //
/// // Regular index selection fails (no filter on 'region' prefix column)
/// // Skip-scan is considered: iterate through distinct 'region' values,
/// // for each region, seek to entries with date = '2024-01-01'
/// //
/// // If skip-scan cost < table scan cost, returns IndexScanChoice::SkipScan
/// ```
pub(crate) fn select_index_scan_method(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<IndexScanChoice> {
    // First, try regular cost-based index selection
    if let Some((index_name, sorted_columns)) =
        cost_based_index_selection(table_name, where_clause, order_by, database)
    {
        return Some(IndexScanChoice::Regular { index_name, sorted_columns });
    }

    // If regular index selection failed and we have a WHERE clause,
    // try skip-scan optimization
    if let Some(where_expr) = where_clause {
        let planner = IndexPlanner::new(database);
        if let Some(plan) = planner.plan_skip_scan(table_name, where_expr) {
            if plan.is_skip_scan {
                if let Some(skip_info) = plan.skip_scan_info {
                    if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
                        eprintln!(
                            "[SKIP_SCAN] Selected skip-scan for table={}, index={}, filter_col={}, prefix_cardinality={}",
                            table_name,
                            plan.index_name,
                            skip_info.filter_column,
                            skip_info.prefix_cardinality
                        );
                    }
                    return Some(IndexScanChoice::SkipScan {
                        index_name: plan.index_name,
                        skip_scan_info: skip_info,
                    });
                }
            }
        }
    }

    // No index or skip-scan option found
    None
}

#[cfg(test)]
mod tests {
    use vibesql_ast::BinaryOperator;
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_expression_filters_column_simple() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "age", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(25))),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_expression_filters_column_and() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "age", false,
                ))),
                right: Box::new(Expression::Literal(SqlValue::Integer(18))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "city", false,
                ))),
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(
                    "Boston",
                )))),
            }),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(expression_filters_column(&expr, "city"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference() {
        let expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("age", false));

        assert!(is_column_reference(&expr, "age"));
        assert!(!is_column_reference(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference_case_insensitive() {
        // SQL parser normalizes unquoted identifiers to uppercase
        // but index columns might be lowercase
        let expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("I_ID", false)); // Uppercase from parser

        // Should match regardless of case
        assert!(is_column_reference(&expr, "i_id")); // lowercase
        assert!(is_column_reference(&expr, "I_ID")); // exact match
        assert!(is_column_reference(&expr, "I_id")); // mixed case
        assert!(!is_column_reference(&expr, "other_column"));
    }

    #[test]
    fn test_expression_filters_column_case_insensitive() {
        // WHERE I_ID = 42 (uppercase from parser)
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "I_ID", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        // Should match lowercase index column name
        assert!(expression_filters_column(&expr, "i_id"));
        assert!(expression_filters_column(&expr, "I_ID"));
        assert!(!expression_filters_column(&expr, "other"));
    }
}
