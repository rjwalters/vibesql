//! Index predicate extraction
//!
//! Extracts range and IN predicates from WHERE clauses for index optimization.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::selection::is_column_reference;

/// Composite predicate types for multi-column index optimization
///
/// This represents the type of predicate on each column in a composite index,
/// supporting both equality (col = val) and IN (col IN (val1, val2, ...)) predicates.
#[derive(Debug, Clone)]
pub(crate) enum CompositePredicateType {
    /// Equality predicate: col = value
    Equality(SqlValue),
    /// IN predicate: col IN (value1, value2, ...)
    In(Vec<SqlValue>),
}

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
///
/// Note: This function is superseded by `extract_composite_predicates_with_in` which
/// also handles IN predicates. Kept for backward compatibility with tests.
#[allow(dead_code)]
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

/// Collect equality predicates from WHERE clause into a map
///
/// Recursively walks the expression tree to find all `column = literal` predicates.
/// Handles AND-connected predicates.
///
/// Note: This helper is used by `extract_composite_equality_predicates` which is
/// superseded by `extract_composite_predicates_with_in`. Kept for backward compatibility.
#[allow(dead_code)]
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

/// Extract composite predicates (equality or IN) for ALL columns in a composite index
///
/// For a query like: `WHERE c_w_id IN (1, 2) AND c_d_id = 5`
/// with index columns `[c_w_id, c_d_id]`, this returns `Some([In([1, 2]), Equality(5)])`.
///
/// Returns None if:
/// - Any index column doesn't have an equality or IN predicate
/// - The predicates use non-literal values
/// - The WHERE clause structure doesn't support extraction
///
/// # Arguments
/// * `expr` - The WHERE clause expression
/// * `column_names` - The index column names in order
///
/// # Returns
/// `Some(Vec<CompositePredicateType>)` - Predicate types in index column order
/// `None` - Cannot extract composite predicates
pub(crate) fn extract_composite_predicates_with_in(
    expr: &Expression,
    column_names: &[&str],
) -> Option<Vec<CompositePredicateType>> {
    if column_names.is_empty() {
        return None;
    }

    // Collect all predicates (equality and IN) from the WHERE clause
    let mut equality_predicates: std::collections::HashMap<String, SqlValue> =
        std::collections::HashMap::new();
    let mut in_predicates: std::collections::HashMap<String, Vec<SqlValue>> =
        std::collections::HashMap::new();
    collect_predicates_with_in(expr, &mut equality_predicates, &mut in_predicates);

    // Build composite predicate types in index column order
    let mut result = Vec::with_capacity(column_names.len());
    for col_name in column_names {
        let col_upper = col_name.to_uppercase();

        // Check for equality predicate first
        if let Some(value) = equality_predicates.get(&col_upper) {
            result.push(CompositePredicateType::Equality(value.clone()));
        }
        // Then check for IN predicate
        else if let Some(values) = in_predicates.get(&col_upper) {
            if values.is_empty() {
                return None; // Empty IN list
            }
            result.push(CompositePredicateType::In(values.clone()));
        } else {
            // Missing predicate for this column - can't use composite key
            return None;
        }
    }

    Some(result)
}

/// Collect both equality and IN predicates from WHERE clause
///
/// Recursively walks the expression tree to find:
/// - `column = literal` predicates
/// - `column IN (literal, ...)` predicates
///
/// Handles AND-connected predicates.
fn collect_predicates_with_in(
    expr: &Expression,
    equality_predicates: &mut std::collections::HashMap<String, SqlValue>,
    in_predicates: &mut std::collections::HashMap<String, Vec<SqlValue>>,
) {
    match expr {
        // Handle equality: col = value or value = col
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } => {
            // Check col = literal
            if let Expression::ColumnRef { column, .. } = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        equality_predicates.insert(column.to_uppercase(), value.clone());
                    }
                }
            }
            // Check literal = col (reversed)
            if let Expression::ColumnRef { column, .. } = right.as_ref() {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        equality_predicates.insert(column.to_uppercase(), value.clone());
                    }
                }
            }
        }
        // Handle IN list: col IN (val1, val2, ...)
        Expression::InList { expr: col_expr, values, negated } => {
            if !negated {
                if let Expression::ColumnRef { column, .. } = col_expr.as_ref() {
                    // Extract literal values from the IN list
                    let mut in_values = Vec::new();
                    let mut all_literals = true;
                    let mut has_null = false;

                    for item in values {
                        if let Expression::Literal(value) = item {
                            if matches!(value, SqlValue::Null) {
                                has_null = true;
                            }
                            in_values.push(value.clone());
                        } else {
                            all_literals = false;
                            break;
                        }
                    }

                    // Only use if all are literals and no NULL values
                    // (NULL in IN list has special three-valued logic)
                    if all_literals && !has_null && !in_values.is_empty() {
                        in_predicates.insert(column.to_uppercase(), in_values);
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
            collect_predicates_with_in(left, equality_predicates, in_predicates);
            collect_predicates_with_in(right, equality_predicates, in_predicates);
        }
        _ => {}
    }
}

/// Generate all composite keys from a list of predicate types
///
/// For predicates like `[In([1, 2]), Equality(5)]`, generates keys:
/// - `[1, 5]`
/// - `[2, 5]`
///
/// This is effectively a cartesian product of all IN values combined with equalities.
pub(crate) fn generate_composite_keys(predicates: &[CompositePredicateType]) -> Vec<Vec<SqlValue>> {
    if predicates.is_empty() {
        return vec![];
    }

    // Start with a single empty key
    let mut result: Vec<Vec<SqlValue>> = vec![vec![]];

    for pred in predicates {
        match pred {
            CompositePredicateType::Equality(value) => {
                // Append this value to all existing keys
                for key in &mut result {
                    key.push(value.clone());
                }
            }
            CompositePredicateType::In(values) => {
                // For each existing key, create N new keys (one per IN value)
                let mut new_result = Vec::with_capacity(result.len() * values.len());
                for key in &result {
                    for value in values {
                        let mut new_key = key.clone();
                        new_key.push(value.clone());
                        new_result.push(new_key);
                    }
                }
                result = new_result;
            }
        }
    }

    result
}

/// Check if WHERE clause is fully satisfied by composite index predicates
///
/// Returns true if the WHERE clause contains ONLY:
/// - Equality predicates on index columns (col = val)
/// - IN predicates on index columns (col IN (val1, val2, ...))
/// - AND connectors between these predicates
///
/// This allows skipping redundant WHERE clause re-evaluation when using
/// composite index lookup.
pub(crate) fn where_clause_fully_satisfied_by_composite_key(
    where_expr: &Expression,
    index_column_names: &[&str],
) -> bool {
    // Count predicates to verify WHERE contains exactly the right predicates
    let mut predicate_count = 0;
    let satisfied = check_composite_satisfaction(where_expr, index_column_names, &mut predicate_count);

    // WHERE is fully satisfied only if all parts were handled
    // and we found the expected number of predicates (one per column)
    satisfied && predicate_count == index_column_names.len()
}

/// Helper to check if an expression is fully satisfied by composite index
fn check_composite_satisfaction(
    expr: &Expression,
    index_column_names: &[&str],
    predicate_count: &mut usize,
) -> bool {
    match expr {
        // Equality predicate: col = val or val = col
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } => {
            let col_name = extract_column_name(left).or_else(|| extract_column_name(right));
            let has_literal = matches!(left.as_ref(), Expression::Literal(_))
                || matches!(right.as_ref(), Expression::Literal(_));

            if let Some(name) = col_name {
                let name_upper = name.to_uppercase();
                let is_index_col = index_column_names
                    .iter()
                    .any(|c| c.to_uppercase() == name_upper);

                if is_index_col && has_literal {
                    *predicate_count += 1;
                    return true;
                }
            }
            false
        }
        // IN predicate: col IN (val1, val2, ...)
        Expression::InList { expr: col_expr, values, negated } => {
            if *negated {
                return false;
            }

            if let Some(col_name) = extract_column_name(col_expr) {
                let name_upper = col_name.to_uppercase();
                let is_index_col = index_column_names
                    .iter()
                    .any(|c| c.to_uppercase() == name_upper);

                // All values must be literals (no NULL for optimization)
                let all_literals = values.iter().all(|v| {
                    matches!(v, Expression::Literal(val) if !matches!(val, SqlValue::Null))
                });

                if is_index_col && all_literals && !values.is_empty() {
                    *predicate_count += 1;
                    return true;
                }
            }
            false
        }
        // AND connector
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left_ok = check_composite_satisfaction(left, index_column_names, predicate_count);
            let right_ok = check_composite_satisfaction(right, index_column_names, predicate_count);
            left_ok && right_ok
        }
        _ => false,
    }
}

/// Extract column name from a ColumnRef expression
fn extract_column_name(expr: &Expression) -> Option<&str> {
    match expr {
        Expression::ColumnRef { column, .. } => Some(column.as_str()),
        _ => None,
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
