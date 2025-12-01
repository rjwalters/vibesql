//! Index predicate extraction
//!
//! Extracts range and IN predicates from WHERE clauses for index optimization.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::selection::is_column_reference;

/// Range predicate information extracted from WHERE clause
#[derive(Debug)]
pub(crate) struct RangePredicate {
    pub start: Option<SqlValue>,
    pub end: Option<SqlValue>,
    pub inclusive_start: bool,
    pub inclusive_end: bool,
}

/// Index predicate types that can be pushed down to storage layer
#[derive(Debug)]
pub(crate) enum IndexPredicate {
    /// Range scan with optional bounds (>, <, >=, <=, BETWEEN)
    Range(RangePredicate),
    /// Multi-value lookup (IN predicate)
    In(Vec<SqlValue>),
}

/// Extract range predicate bounds for an indexed column from WHERE clause
///
/// This extracts comparison operators (>, <, >=, <=, BETWEEN) that can be
/// pushed down to the storage layer's range_scan() method.
///
/// Returns None if no suitable range predicate found for the column.
fn extract_range_predicate(expr: &Expression, column_name: &str) -> Option<RangePredicate> {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                // Handle equality: col = value
                BinaryOperator::Equal => {
                    // Check if left side is our column and right side is a literal
                    if is_column_reference(left, column_name) {
                        if let Expression::Literal(value) = right.as_ref() {
                            // NULL comparisons always return no rows - can't optimize with index
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            // Equal is a range with same start and end, both inclusive
                            return Some(RangePredicate {
                                start: Some(value.clone()),
                                end: Some(value.clone()),
                                inclusive_start: true,
                                inclusive_end: true,
                            });
                        }
                    }
                    // Also handle reverse: value = col
                    if is_column_reference(right, column_name) {
                        if let Expression::Literal(value) = left.as_ref() {
                            // NULL comparisons always return no rows - can't optimize with index
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            return Some(RangePredicate {
                                start: Some(value.clone()),
                                end: Some(value.clone()),
                                inclusive_start: true,
                                inclusive_end: true,
                            });
                        }
                    }
                }
                // Handle simple comparisons: col > value, col < value, etc.
                BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual => {
                    // Check if left side is our column and right side is a literal
                    if is_column_reference(left, column_name) {
                        if let Expression::Literal(value) = right.as_ref() {
                            // NULL comparisons always return no rows - can't optimize with index
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            return Some(match op {
                                BinaryOperator::GreaterThan => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::GreaterThanOrEqual => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: true,
                                    inclusive_end: false,
                                },
                                BinaryOperator::LessThan => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::LessThanOrEqual => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: true,
                                },
                                _ => unreachable!(),
                            });
                        }
                    }
                    // Check if right side is our column and left side is a literal (flipped comparison)
                    else if is_column_reference(right, column_name) {
                        if let Expression::Literal(value) = left.as_ref() {
                            // NULL comparisons always return no rows - can't optimize with index
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            return Some(match op {
                                // Flip the comparison: value > col means col < value
                                BinaryOperator::GreaterThan => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::GreaterThanOrEqual => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: true,
                                },
                                BinaryOperator::LessThan => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::LessThanOrEqual => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: true,
                                    inclusive_end: false,
                                },
                                _ => unreachable!(),
                            });
                        }
                    }
                }
                // Handle AND: can combine range predicates (e.g., col > 10 AND col < 20)
                BinaryOperator::And => {
                    let left_range = extract_range_predicate(left, column_name);
                    let right_range = extract_range_predicate(right, column_name);

                    // Merge ranges if both sides have predicates on our column
                    match (left_range, right_range) {
                        (Some(mut l), Some(r)) => {
                            // Merge the bounds
                            if l.start.is_none() {
                                l.start = r.start;
                                l.inclusive_start = r.inclusive_start;
                            }
                            if l.end.is_none() {
                                l.end = r.end;
                                l.inclusive_end = r.inclusive_end;
                            }
                            return Some(l);
                        }
                        (Some(l), None) => return Some(l),
                        (None, Some(r)) => return Some(r),
                        (None, None) => {}
                    }
                }
                _ => {}
            }
        }
        // Handle BETWEEN: col BETWEEN low AND high
        // For SYMMETRIC: swap bounds if low > high
        Expression::Between { expr: col_expr, low, high, negated, symmetric } => {
            if !negated && is_column_reference(col_expr, column_name) {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    // NULL comparisons always return no rows - can't optimize with index
                    if matches!(low_val, SqlValue::Null) || matches!(high_val, SqlValue::Null) {
                        return None;
                    }

                    // Handle SYMMETRIC: swap bounds if low > high
                    let (effective_low, effective_high) = if *symmetric && low_val > high_val {
                        (high_val.clone(), low_val.clone())
                    } else {
                        (low_val.clone(), high_val.clone())
                    };

                    return Some(RangePredicate {
                        start: Some(effective_low),
                        end: Some(effective_high),
                        inclusive_start: true,
                        inclusive_end: true,
                    });
                }
            }
        }
        _ => {}
    }

    None
}

/// Extract equality predicates for ALL columns in a composite index
///
/// For a query like: `WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 42`
/// with index columns `[c_w_id, c_d_id, c_id]`, this returns `Some([1, 1, 42])`.
///
/// Returns None if:
/// - Any index column doesn't have an equality predicate
/// - The predicates use non-literal values
/// - The WHERE clause structure doesn't support extraction
///
/// # Arguments
/// * `expr` - The WHERE clause expression
/// * `column_names` - The index column names in order
///
/// # Returns
/// `Some(Vec<SqlValue>)` - Composite key values in index column order
/// `None` - Cannot extract composite key (fall back to single-column predicate)
pub(crate) fn extract_composite_equality_predicates(
    expr: &Expression,
    column_names: &[&str],
) -> Option<Vec<SqlValue>> {
    if column_names.is_empty() {
        return None;
    }

    // Collect all equality predicates from the WHERE clause
    let mut predicates: std::collections::HashMap<String, SqlValue> =
        std::collections::HashMap::new();
    collect_equality_predicates(expr, &mut predicates);

    // Build composite key in index column order
    let mut composite_key = Vec::with_capacity(column_names.len());
    for col_name in column_names {
        // Case-insensitive column matching
        let col_upper = col_name.to_uppercase();
        if let Some(value) = predicates.get(&col_upper) {
            composite_key.push(value.clone());
        } else {
            // Missing predicate for this column - can't use composite key
            return None;
        }
    }

    Some(composite_key)
}

/// Result of prefix equality predicate extraction
#[derive(Debug)]
pub(crate) struct PrefixPredicateResult {
    /// Prefix key values in index column order (may be shorter than index columns)
    pub prefix_key: Vec<SqlValue>,
    /// Column names that are covered by the prefix (case-insensitive, uppercase)
    pub covered_columns: std::collections::HashSet<String>,
}

/// Extract equality predicates for a PREFIX of columns in a composite index
///
/// Unlike `extract_composite_equality_predicates` which requires ALL columns,
/// this function extracts a prefix of matching columns starting from the first.
///
/// For example, with index `[c_w_id, c_d_id, c_id]`:
/// - `WHERE c_w_id = 1 AND c_d_id = 2 AND c_balance > 100` returns prefix `[1, 2]`
///   with covered columns `{c_w_id, c_d_id}`
/// - `WHERE c_w_id = 1 AND c_id = 3` returns prefix `[1]` (c_id skipped, not contiguous)
///
/// # Arguments
/// * `expr` - The WHERE clause expression
/// * `column_names` - The index column names in order
///
/// # Returns
/// `Some(PrefixPredicateResult)` - Prefix key and covered columns
/// `None` - No prefix could be extracted (first column has no equality predicate)
pub(crate) fn extract_prefix_equality_predicates(
    expr: &Expression,
    column_names: &[&str],
) -> Option<PrefixPredicateResult> {
    if column_names.is_empty() {
        return None;
    }

    // Collect all equality predicates from the WHERE clause
    let mut predicates: std::collections::HashMap<String, SqlValue> =
        std::collections::HashMap::new();
    collect_equality_predicates(expr, &mut predicates);

    // Build prefix key in index column order, stopping at first missing column
    let mut prefix_key = Vec::new();
    let mut covered_columns = std::collections::HashSet::new();

    for col_name in column_names {
        let col_upper = col_name.to_uppercase();
        if let Some(value) = predicates.get(&col_upper) {
            prefix_key.push(value.clone());
            covered_columns.insert(col_upper);
        } else {
            // Gap in prefix - stop here
            break;
        }
    }

    if prefix_key.is_empty() {
        // First column has no equality predicate - can't use prefix lookup
        return None;
    }

    Some(PrefixPredicateResult {
        prefix_key,
        covered_columns,
    })
}

/// Build a residual WHERE clause by removing predicates covered by index lookup
///
/// Given a WHERE clause and a set of covered column names, this removes the
/// equality predicates for those columns and returns only the uncovered predicates.
///
/// # Arguments
/// * `expr` - The original WHERE clause expression
/// * `covered_columns` - Set of column names (uppercase) covered by the index lookup
///
/// # Returns
/// `Some(Expression)` - The residual WHERE clause with uncovered predicates
/// `None` - All predicates are covered by the index (no filtering needed)
///
/// # Example
/// ```text
/// WHERE c_w_id = 1 AND c_d_id = 2 AND c_balance > 100
/// covered_columns = {C_W_ID, C_D_ID}
/// → Returns: c_balance > 100
/// ```
pub(crate) fn build_residual_where_clause(
    expr: &Expression,
    covered_columns: &std::collections::HashSet<String>,
) -> Option<Expression> {
    filter_expression(expr, covered_columns)
}

/// Recursively filter an expression, removing covered equality predicates
fn filter_expression(
    expr: &Expression,
    covered_columns: &std::collections::HashSet<String>,
) -> Option<Expression> {
    match expr {
        // Check if this is a covered equality predicate: col = literal or literal = col
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } => {
            // Check col = literal
            if let Expression::ColumnRef { column, .. } = left.as_ref() {
                if covered_columns.contains(&column.to_uppercase()) {
                    if matches!(right.as_ref(), Expression::Literal(_)) {
                        // This predicate is covered - remove it
                        return None;
                    }
                }
            }
            // Check literal = col
            if let Expression::ColumnRef { column, .. } = right.as_ref() {
                if covered_columns.contains(&column.to_uppercase()) {
                    if matches!(left.as_ref(), Expression::Literal(_)) {
                        // This predicate is covered - remove it
                        return None;
                    }
                }
            }
            // Not a covered predicate - keep it
            Some(expr.clone())
        }
        // Handle AND: filter both sides and recombine
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left_filtered = filter_expression(left, covered_columns);
            let right_filtered = filter_expression(right, covered_columns);

            match (left_filtered, right_filtered) {
                (Some(l), Some(r)) => Some(Expression::BinaryOp {
                    left: Box::new(l),
                    op: BinaryOperator::And,
                    right: Box::new(r),
                }),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None, // Both sides were covered
            }
        }
        // All other expressions are not covered equality predicates - keep them
        _ => Some(expr.clone()),
    }
}

/// Collect equality predicates from WHERE clause into a map
///
/// Recursively walks the expression tree to find all `column = literal` predicates.
/// Handles AND-connected predicates.
fn collect_equality_predicates(
    expr: &Expression,
    predicates: &mut std::collections::HashMap<String, SqlValue>,
) {
    match expr {
        // Handle equality: col = value or value = col
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } => {
            // Check col = literal (using ColumnRef variant)
            if let Expression::ColumnRef { column, .. } = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        predicates.insert(column.to_uppercase(), value.clone());
                    }
                }
            }
            // Check literal = col (reversed)
            if let Expression::ColumnRef { column, .. } = right.as_ref() {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        predicates.insert(column.to_uppercase(), value.clone());
                    }
                }
            }
        }
        // Recursively process AND predicates
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_equality_predicates(left, predicates);
            collect_equality_predicates(right, predicates);
        }
        _ => {}
    }
}

/// Extract index predicate (range or IN) for an indexed column from WHERE clause
///
/// This extracts predicates that can be pushed down to the storage layer:
/// - Range predicates: >, <, >=, <=, BETWEEN
/// - IN predicates: IN (value1, value2, ...)
///
/// Returns None if no suitable predicate found for the column.
pub(crate) fn extract_index_predicate(expr: &Expression, column_name: &str) -> Option<IndexPredicate> {
    // First try to extract a range predicate
    if let Some(range) = extract_range_predicate(expr, column_name) {
        return Some(IndexPredicate::Range(range));
    }

    // Then try to extract an IN predicate
    match expr {
        // Handle IN with value list: col IN (1, 2, 3)
        Expression::InList { expr: col_expr, values: value_list, negated } => {
            if !negated && is_column_reference(col_expr, column_name) {
                // Extract literal values from the IN list
                let mut values = Vec::new();
                let mut has_null = false;
                for item in value_list {
                    if let Expression::Literal(value) = item {
                        // Track if we encounter NULL in the list
                        if matches!(value, SqlValue::Null) {
                            has_null = true;
                        }
                        values.push(value.clone());
                    } else {
                        // If any item is not a literal, we can't optimize
                        return None;
                    }
                }

                // If IN list contains NULL, skip index optimization
                // Rationale: per SQL three-valued logic, when NULL is in the IN list:
                // - value IN (..., NULL) when value doesn't match → NULL (not FALSE)
                // The index lookup can't represent this NULL result, so we must fall back
                // to regular evaluation which handles three-valued logic correctly
                if has_null {
                    return None;
                }

                if !values.is_empty() {
                    return Some(IndexPredicate::In(values));
                }
            }
        }
        // Handle AND: try both sides
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            // Try left side first
            if let Some(pred) = extract_index_predicate(left, column_name) {
                return Some(pred);
            }
            // Then try right side
            if let Some(pred) = extract_index_predicate(right, column_name) {
                return Some(pred);
            }
        }
        _ => {}
    }

    None
}

/// Check if WHERE clause can be fully satisfied by index predicate
///
/// Returns true if the WHERE clause is simple enough that the index lookup
/// already guarantees all rows satisfy it (no additional filtering needed).
///
/// This optimization skips redundant WHERE clause re-evaluation for queries like:
/// - `WHERE col = 5` (exact match)
/// - `WHERE col BETWEEN 10 AND 20` (range)
/// - `WHERE col IN (1, 2, 3)` (multi-value)
/// - `WHERE col > 10 AND col < 20` (combined range)
#[allow(dead_code)]
pub(crate) fn where_clause_fully_satisfied_by_index(
    where_expr: &Expression,
    indexed_column: &str,
) -> bool {
    match where_expr {
        // Simple comparison on indexed column: col = value, col > value, etc.
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Check if this is a simple: column op literal
                    let left_is_col = is_column_reference(left, indexed_column);
                    let right_is_col = is_column_reference(right, indexed_column);
                    let left_is_literal = matches!(left.as_ref(), Expression::Literal(_));
                    let right_is_literal = matches!(right.as_ref(), Expression::Literal(_));

                    // Either (col op literal) or (literal op col)
                    (left_is_col && right_is_literal) || (left_is_literal && right_is_col)
                }
                // AND of range predicates on same column: col > 10 AND col < 20
                vibesql_ast::BinaryOperator::And => {
                    let left_satisfied = where_clause_fully_satisfied_by_index(left, indexed_column);
                    let right_satisfied = where_clause_fully_satisfied_by_index(right, indexed_column);
                    left_satisfied && right_satisfied
                }
                _ => false,
            }
        }
        // BETWEEN on indexed column: col BETWEEN low AND high
        // Only ASYMMETRIC BETWEEN (symmetric: false) can be fully satisfied by index
        // SYMMETRIC BETWEEN needs bounds swapping handled by evaluator
        Expression::Between { expr: col_expr, low, high, negated, symmetric } => {
            !negated
                && !symmetric
                && is_column_reference(col_expr, indexed_column)
                && matches!(low.as_ref(), Expression::Literal(_))
                && matches!(high.as_ref(), Expression::Literal(_))
        }
        // IN on indexed column: col IN (literal, literal, ...)
        Expression::InList { expr: col_expr, values, negated } => {
            !negated
                && is_column_reference(col_expr, indexed_column)
                && values.iter().all(|v| matches!(v, Expression::Literal(_)))
        }
        _ => false,
    }
}

#[cfg(test)]
#[path = "predicate_tests.rs"]
mod predicate_tests;
