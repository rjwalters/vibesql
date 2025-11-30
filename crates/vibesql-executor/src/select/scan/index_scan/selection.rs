//! Index selection logic
//!
//! Determines when and which index to use for query optimization.
//! Supports both rule-based (simple) and cost-based (statistics-aware) selection.

use vibesql_ast::Expression;
use vibesql_catalog::TableSchema;
use vibesql_storage::{Database, statistics::{CostEstimator, AccessMethod}};

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
        if let Expression::ColumnRef { column, .. } = &item.expr {
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

    // Try to find an index that satisfies both WHERE and ORDER BY (if present)
    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            let first_indexed_column = index_metadata.columns.first()?;

            // Check if this index can be used for WHERE clause
            let can_use_for_where = where_clause
                .map(|expr| expression_filters_column(expr, &first_indexed_column.column_name))
                .unwrap_or(false);

            // Check if this index can be used for ORDER BY clause
            let can_use_for_order = if let Some(order_items) = order_by {
                // Check if ORDER BY columns match the index columns
                let columns_match = can_use_index_for_order_by(order_items, &index_metadata.columns);

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

            // Use this index if it satisfies WHERE, ORDER BY, or both
            if can_use_for_where || can_use_for_order {
                // Build sorted_columns metadata if ORDER BY can be satisfied
                let sorted_columns = if can_use_for_order {
                    // Build sorted_columns from all ORDER BY columns that match the index
                    let order_items = order_by.unwrap();
                    Some(
                        order_items
                            .iter()
                            .map(|item| {
                                let col_name = match &item.expr {
                                    Expression::ColumnRef { column, .. } => column.clone(),
                                    _ => unreachable!("can_use_index_for_order_by ensures simple column refs"),
                                };
                                (col_name, item.direction.clone())
                            })
                            .collect(),
                    )
                } else {
                    None
                };

                return Some((index_name.clone(), sorted_columns));
            }
        }
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
        // IN with value list: col IN (1, 2, 3)
        Expression::InList { expr, .. } => is_column_reference(expr, column_name),
        // IN with subquery: col IN (SELECT ...)
        Expression::In { expr, .. } => is_column_reference(expr, column_name),
        // BETWEEN: col BETWEEN low AND high
        Expression::Between { expr, .. } => is_column_reference(expr, column_name),
        _ => false,
    }
}

/// Check if an expression is a reference to a specific column
///
/// Uses case-insensitive comparison because:
/// - SQL parser normalizes unquoted identifiers to uppercase (e.g., `i_id` → `I_ID`)
/// - Index column names may be lowercase or uppercase depending on how they're created
/// - Table column statistics are stored with the original column name case
pub(super) fn is_column_reference(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef { column, .. } => column.eq_ignore_ascii_case(column_name),
        _ => false,
    }
}

/// Check if an expression is a literal value
///
/// Index scans can only filter on columns compared to literal values,
/// not columns compared to other columns (which are equijoin conditions).
fn is_literal(expr: &Expression) -> bool {
    matches!(expr, Expression::Literal(_))
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
    columns
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(column_name))
        .map(|(_, stats)| stats)
}

/// Check if an index can be used to satisfy an ORDER BY clause
///
/// Returns true if the ORDER BY columns match a prefix of the index columns
/// and the sort directions are compatible (considering ASC/DESC on both sides).
///
/// Examples:
/// - ORDER BY col0 ASC can use index (col0 ASC)
/// - ORDER BY col0 DESC can use index (col0 DESC)
/// - ORDER BY col0, col1 can use index (col0, col1)
/// - ORDER BY col0 cannot use index (col0 DESC) - wrong direction
pub(crate) fn can_use_index_for_order_by(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
) -> bool {
    // ORDER BY must not have more columns than the index
    if order_items.len() > index_columns.len() {
        return false;
    }

    // Check each ORDER BY column against corresponding index column
    for (order_item, index_col) in order_items.iter().zip(index_columns.iter()) {
        // ORDER BY expression must be a simple column reference
        let order_col_name = match &order_item.expr {
            Expression::ColumnRef { table: None, column } => column,
            _ => return false, // Complex expressions not supported
        };

        // Column names must match (case-insensitive due to SQL identifier normalization)
        if !order_col_name.eq_ignore_ascii_case(&index_col.column_name) {
            return false;
        }

        // Sort directions must be compatible
        // Note: IndexColumn uses OrderDirection enum, same as OrderByItem
        if order_item.direction != index_col.direction {
            return false;
        }
    }

    true
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

    // Try each index and find the one with lowest cost
    #[allow(clippy::type_complexity)]
    let mut best_index: Option<(String, AccessMethod, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> = None;
    let mut has_applicable_index_without_stats = false;

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            let first_indexed_column = index_metadata.columns.first()?;
            let column_name = &first_indexed_column.column_name;

            // Check if this index can be used for WHERE or ORDER BY
            let can_use_for_where = where_clause
                .map(|expr| expression_filters_column(expr, column_name))
                .unwrap_or(false);

            let can_use_for_order = if let Some(order_items) = order_by {
                let columns_match = can_use_index_for_order_by(order_items, &index_metadata.columns);

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
                continue;
            }

            // Get column statistics for the indexed column (case-insensitive lookup)
            let col_stats = get_column_stats_ignore_case(&table_stats.columns, column_name);
            if col_stats.is_none() {
                // Track that we found an applicable index without column stats
                // We'll fall back to rule-based selection if cost-based fails
                has_applicable_index_without_stats = true;
                continue; // No stats for this column, try next index
            }
            let col_stats = col_stats.unwrap();

            // Estimate selectivity based on WHERE clause
            let selectivity = if let Some(where_expr) = where_clause {
                estimate_selectivity(where_expr, column_name, col_stats)
            } else {
                1.0 // No WHERE clause means all rows
            };

            // Use cost estimator to decide
            let access_method = cost_estimator.choose_access_method(
                table_stats,
                Some(col_stats),
                selectivity,
            );

            // Build sorted_columns metadata if ORDER BY can be satisfied
            let sorted_columns = if can_use_for_order {
                let order_items = order_by.unwrap();
                Some(
                    order_items
                        .iter()
                        .map(|item| {
                            let col_name = match &item.expr {
                                Expression::ColumnRef { column, .. } => column.clone(),
                                _ => unreachable!("can_use_index_for_order_by ensures simple column refs"),
                            };
                            (col_name, item.direction.clone())
                        })
                        .collect(),
                )
            } else {
                None
            };

            // Track the best index (lowest cost)
            if access_method.is_index_scan() {
                match &best_index {
                    None => {
                        best_index = Some((index_name.clone(), access_method, sorted_columns));
                    }
                    Some((_, best_method, _)) => {
                        if access_method.cost() < best_method.cost() {
                            best_index = Some((index_name.clone(), access_method, sorted_columns));
                        }
                    }
                }
            }
        }
    }

    // Return the best index if we found one
    if let Some((index_name, _, sorted_columns)) = best_index {
        return Some((index_name, sorted_columns));
    }

    // If we have applicable indexes but no column stats, fall back to rule-based selection
    // This ensures we use indexes even when statistics are incomplete
    if has_applicable_index_without_stats {
        return should_use_index_scan(table_name, where_clause, order_by, database);
    }

    // No applicable indexes found
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
                    if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) = (&**left, &**right) {
                        if column.eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) = (&**left, &**right) {
                        if column.eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    0.33 // Default fallback
                }
                vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Range predicates (case-insensitive column comparison)
                    if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) = (&**left, &**right) {
                        if column.eq_ignore_ascii_case(column_name) {
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
            if let Expression::ColumnRef { column, .. } = &**expr {
                if column.eq_ignore_ascii_case(column_name) {
                    // Estimate BETWEEN as: P(col >= low AND col <= high)
                    if let (Expression::Literal(low_val), Expression::Literal(high_val)) = (&**low, &**high) {
                        let low_sel = col_stats.estimate_range_selectivity(low_val, ">=");
                        let high_sel = col_stats.estimate_range_selectivity(high_val, "<=");
                        return low_sel * high_sel; // Assuming independence
                    }
                }
            }
            0.33 // Default fallback
        }
        _ => 0.33, // Default fallback for unsupported expressions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;
    use vibesql_types::SqlValue;

    #[test]
    fn test_expression_filters_column_simple() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "age".to_string(),
            }),
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
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(18))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "city".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Varchar("Boston".to_string()))),
            }),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(expression_filters_column(&expr, "city"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference() {
        let expr = Expression::ColumnRef {
            table: None,
            column: "age".to_string(),
        };

        assert!(is_column_reference(&expr, "age"));
        assert!(!is_column_reference(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference_case_insensitive() {
        // SQL parser normalizes unquoted identifiers to uppercase
        // but index columns might be lowercase
        let expr = Expression::ColumnRef {
            table: None,
            column: "I_ID".to_string(), // Uppercase from parser
        };

        // Should match regardless of case
        assert!(is_column_reference(&expr, "i_id"));  // lowercase
        assert!(is_column_reference(&expr, "I_ID"));  // exact match
        assert!(is_column_reference(&expr, "I_id"));  // mixed case
        assert!(!is_column_reference(&expr, "other_column"));
    }

    #[test]
    fn test_expression_filters_column_case_insensitive() {
        // WHERE I_ID = 42 (uppercase from parser)
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "I_ID".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        // Should match lowercase index column name
        assert!(expression_filters_column(&expr, "i_id"));
        assert!(expression_filters_column(&expr, "I_ID"));
        assert!(!expression_filters_column(&expr, "other"));
    }
}
