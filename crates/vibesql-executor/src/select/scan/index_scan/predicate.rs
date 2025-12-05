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

    Some(PrefixPredicateResult { prefix_key, covered_columns })
}

/// Result of prefix + trailing range predicate extraction
#[derive(Debug)]
pub(crate) struct PrefixWithRangeResult {
    /// Prefix key values (equality predicates on first N columns)
    pub prefix_key: Vec<SqlValue>,
    /// Lower bound value for the (N+1)th column (range predicate), if any
    pub lower_bound: Option<SqlValue>,
    /// Whether the lower bound is inclusive (>=) or exclusive (>)
    pub inclusive_lower: bool,
    /// Upper bound value for the (N+1)th column (range predicate), if any
    pub upper_bound: Option<SqlValue>,
    /// Whether the upper bound is inclusive (<=) or exclusive (<)
    pub inclusive_upper: bool,
    /// Column names covered by this lookup (prefix + range column)
    pub covered_columns: std::collections::HashSet<String>,
}

/// Extract prefix equality predicates + optional trailing range predicate
///
/// This is an optimization for queries like `WHERE col1 = 1 AND col2 < 10` on an
/// index `(col1, col2, col3)`. It extracts the equality prefix (`col1 = 1`) and
/// the range bound on the next column (`col2 < 10`).
///
/// This enables efficient bounded range scans instead of scanning all rows
/// matching the prefix and then filtering by the range predicate.
///
/// # Arguments
/// * `expr` - The WHERE clause expression
/// * `column_names` - The index column names in order
///
/// # Returns
/// `Some(PrefixWithRangeResult)` if prefix + trailing range found, `None` otherwise
///
/// # Example
/// ```text
/// Index: (s_w_id, s_quantity, s_i_id)
/// WHERE: s_w_id = 1 AND s_quantity < 10
/// Result: prefix_key=[1], upper_bound=10, inclusive_upper=false
/// ```
pub(crate) fn extract_prefix_with_trailing_range(
    expr: &Expression,
    column_names: &[&str],
) -> Option<PrefixWithRangeResult> {
    if column_names.len() < 2 {
        // Need at least 2 columns for prefix + range
        return None;
    }

    // Collect all equality predicates from the WHERE clause
    let mut equality_predicates: std::collections::HashMap<String, SqlValue> =
        std::collections::HashMap::new();
    collect_equality_predicates(expr, &mut equality_predicates);

    // Build prefix key in index column order, stopping at first missing column
    let mut prefix_key = Vec::new();
    let mut covered_columns = std::collections::HashSet::new();
    let mut prefix_end_idx = 0;

    for (idx, col_name) in column_names.iter().enumerate() {
        let col_upper = col_name.to_uppercase();
        if let Some(value) = equality_predicates.get(&col_upper) {
            prefix_key.push(value.clone());
            covered_columns.insert(col_upper);
            prefix_end_idx = idx + 1;
        } else {
            // Gap in prefix - stop here
            break;
        }
    }

    if prefix_key.is_empty() || prefix_end_idx >= column_names.len() {
        // No prefix found or prefix covers all columns (no room for range)
        return None;
    }

    // Check if the next column has a range predicate (<, <=, >, >=)
    let next_col = column_names[prefix_end_idx];
    let range = extract_range_predicate(expr, next_col);

    // Extract both lower and upper bounds for efficient bounded range scans
    // This handles queries like: WHERE col1 = 1 AND col2 >= 10 AND col2 < 20
    if let Some(range_pred) = range {
        // Need at least one bound to optimize
        if range_pred.start.is_some() || range_pred.end.is_some() {
            covered_columns.insert(next_col.to_uppercase());
            return Some(PrefixWithRangeResult {
                prefix_key,
                lower_bound: range_pred.start,
                inclusive_lower: range_pred.inclusive_start,
                upper_bound: range_pred.end,
                inclusive_upper: range_pred.inclusive_end,
                covered_columns,
            });
        }
    }

    None
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
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check col = literal
            if let Expression::ColumnRef { column, .. } = left.as_ref() {
                if covered_columns.contains(&column.to_uppercase())
                    && matches!(right.as_ref(), Expression::Literal(_))
                {
                    // This predicate is covered - remove it
                    return None;
                }
            }
            // Check literal = col
            if let Expression::ColumnRef { column, .. } = right.as_ref() {
                if covered_columns.contains(&column.to_uppercase())
                    && matches!(left.as_ref(), Expression::Literal(_))
                {
                    // This predicate is covered - remove it
                    return None;
                }
            }
            // Not a covered predicate - keep it
            Some(expr.clone())
        }
        // Handle AND: filter both sides and recombine
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
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
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
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
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
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
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
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
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
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
    let satisfied =
        check_composite_satisfaction(where_expr, index_column_names, &mut predicate_count);

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
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            let col_name = extract_column_name(left).or_else(|| extract_column_name(right));
            // Check for non-NULL literals (col = NULL requires special IS NULL handling)
            let has_non_null_literal = matches!(left.as_ref(), Expression::Literal(val) if !matches!(val, SqlValue::Null))
                || matches!(right.as_ref(), Expression::Literal(val) if !matches!(val, SqlValue::Null));

            if let Some(name) = col_name {
                let name_upper = name.to_uppercase();
                let is_index_col =
                    index_column_names.iter().any(|c| c.to_uppercase() == name_upper);

                if is_index_col && has_non_null_literal {
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
                let is_index_col =
                    index_column_names.iter().any(|c| c.to_uppercase() == name_upper);

                // All values must be literals (no NULL for optimization)
                let all_literals = values.iter().all(
                    |v| matches!(v, Expression::Literal(val) if !matches!(val, SqlValue::Null)),
                );

                if is_index_col && all_literals && !values.is_empty() {
                    *predicate_count += 1;
                    return true;
                }
            }
            false
        }
        // AND connector
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
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
pub(crate) fn extract_index_predicate(
    expr: &Expression,
    column_name: &str,
) -> Option<IndexPredicate> {
    // FIRST: Check for contradictions (e.g., col = 70 AND col IN (74, 69, 10))
    // This must happen before extract_range_predicate() since it returns early
    // when finding a range (like col = 70), bypassing contradiction checks.
    if let Expression::BinaryOp { op: BinaryOperator::And, .. } = expr {
        let mut equality_values: Vec<SqlValue> = Vec::new();
        let mut in_values: Option<Vec<SqlValue>> = None;
        let mut range_pred: Option<RangePredicate> = None;

        collect_column_predicates(
            expr,
            column_name,
            &mut equality_values,
            &mut in_values,
            &mut range_pred,
        );

        // Check for equality + IN contradiction
        if let Some(in_vals) = &in_values {
            if !equality_values.is_empty() {
                // If any equality value is NOT in the IN list, we have a contradiction
                for eq_val in &equality_values {
                    if !in_vals.contains(eq_val) {
                        // Contradiction: equality value not in IN list - no rows can match
                        // Return empty IN predicate to signal impossible query
                        return Some(IndexPredicate::In(vec![]));
                    }
                }
            }
        }
    }

    // Try to extract a range predicate
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
        // Handle AND: try to extract predicate from either side
        // Note: Contradiction checks are handled at the top of this function
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

/// Helper to collect equality values, IN values, and range predicates for a column
fn collect_column_predicates(
    expr: &Expression,
    column_name: &str,
    equality_values: &mut Vec<SqlValue>,
    in_values: &mut Option<Vec<SqlValue>>,
    range_pred: &mut Option<RangePredicate>,
) {
    match expr {
        // Equality: col = value
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            if is_column_reference(left, column_name) {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        equality_values.push(value.clone());
                        // Also set range predicate for consistency
                        if range_pred.is_none() {
                            *range_pred = Some(RangePredicate {
                                start: Some(value.clone()),
                                end: Some(value.clone()),
                                inclusive_start: true,
                                inclusive_end: true,
                            });
                        }
                    }
                }
            } else if is_column_reference(right, column_name) {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        equality_values.push(value.clone());
                        if range_pred.is_none() {
                            *range_pred = Some(RangePredicate {
                                start: Some(value.clone()),
                                end: Some(value.clone()),
                                inclusive_start: true,
                                inclusive_end: true,
                            });
                        }
                    }
                }
            }
        }
        // IN list: col IN (val1, val2, ...)
        Expression::InList { expr: col_expr, values: value_list, negated } => {
            if !negated && is_column_reference(col_expr, column_name) {
                let mut values = Vec::new();
                let mut has_null = false;
                for item in value_list {
                    if let Expression::Literal(value) = item {
                        if matches!(value, SqlValue::Null) {
                            has_null = true;
                        }
                        values.push(value.clone());
                    }
                }
                if !has_null && !values.is_empty() {
                    *in_values = Some(values);
                }
            }
        }
        // Recurse into AND
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            collect_column_predicates(left, column_name, equality_values, in_values, range_pred);
            collect_column_predicates(right, column_name, equality_values, in_values, range_pred);
        }
        _ => {}
    }
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
                    let left_satisfied =
                        where_clause_fully_satisfied_by_index(left, indexed_column);
                    let right_satisfied =
                        where_clause_fully_satisfied_by_index(right, indexed_column);
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
