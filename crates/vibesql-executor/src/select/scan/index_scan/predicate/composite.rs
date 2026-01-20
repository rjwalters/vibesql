//! Multi-column composite index predicate extraction
//!
//! This module handles extraction of predicates for composite (multi-column) indexes.
//! It supports equality predicates, IN predicates, prefix matching, and residual
//! clause building for partial index coverage.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::range::extract_range_predicate;

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

/// Result of prefix equality predicate extraction
#[derive(Debug)]
pub(crate) struct PrefixPredicateResult {
    /// Prefix key values in index column order (may be shorter than index columns)
    pub prefix_key: Vec<SqlValue>,
    /// Column names that are covered by the prefix (case-insensitive, uppercase)
    pub covered_columns: HashSet<String>,
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
    pub covered_columns: HashSet<String>,
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
    let mut predicates: HashMap<String, SqlValue> = HashMap::new();
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

/// Extract equality predicates for a PREFIX of columns in a composite index
///
/// Unlike `extract_composite_equality_predicates` which requires ALL columns,
/// this function extracts a prefix of matching columns starting from the first.
///
/// For example, with index `[c_w_id, c_d_id, c_id]`:
/// - `WHERE c_w_id = 1 AND c_d_id = 2 AND c_balance > 100` returns prefix `[1, 2]` with covered
///   columns `{c_w_id, c_d_id}`
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
    let mut predicates: HashMap<String, SqlValue> = HashMap::new();
    collect_equality_predicates(expr, &mut predicates);

    // Build prefix key in index column order, stopping at first missing column
    let mut prefix_key = Vec::new();
    let mut covered_columns = HashSet::new();

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
    let mut equality_predicates: HashMap<String, SqlValue> = HashMap::new();
    collect_equality_predicates(expr, &mut equality_predicates);

    // Build prefix key in index column order, stopping at first missing column
    let mut prefix_key = Vec::new();
    let mut covered_columns = HashSet::new();
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
    covered_columns: &HashSet<String>,
) -> Option<Expression> {
    filter_expression(expr, covered_columns)
}

/// Recursively filter an expression, removing covered equality predicates
fn filter_expression(expr: &Expression, covered_columns: &HashSet<String>) -> Option<Expression> {
    match expr {
        // Check if this is a covered equality predicate: col = literal or literal = col
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check col = literal
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                if covered_columns.contains(&col_id.column_canonical().to_uppercase())
                    && matches!(right.as_ref(), Expression::Literal(_))
                {
                    // This predicate is covered - remove it
                    return None;
                }
            }
            // Check literal = col
            if let Expression::ColumnRef(col_id) = right.as_ref() {
                if covered_columns.contains(&col_id.column_canonical().to_uppercase())
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
pub(super) fn collect_equality_predicates(
    expr: &Expression,
    predicates: &mut HashMap<String, SqlValue>,
) {
    match expr {
        // Handle equality: col = value or value = col
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check col = literal (using ColumnRef variant)
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        predicates.insert(col_id.column_canonical().to_uppercase(), value.clone());
                    }
                }
            }
            // Check literal = col (reversed)
            if let Expression::ColumnRef(col_id) = right.as_ref() {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        predicates.insert(col_id.column_canonical().to_uppercase(), value.clone());
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
    let mut equality_predicates: HashMap<String, SqlValue> = HashMap::new();
    let mut in_predicates: HashMap<String, Vec<SqlValue>> = HashMap::new();
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
    equality_predicates: &mut HashMap<String, SqlValue>,
    in_predicates: &mut HashMap<String, Vec<SqlValue>>,
) {
    match expr {
        // Handle equality: col = value or value = col
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check col = literal
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        equality_predicates
                            .insert(col_id.column_canonical().to_uppercase(), value.clone());
                    }
                }
            }
            // Check literal = col (reversed)
            if let Expression::ColumnRef(col_id) = right.as_ref() {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        equality_predicates
                            .insert(col_id.column_canonical().to_uppercase(), value.clone());
                    }
                }
            }
        }
        // Handle IN list: col IN (val1, val2, ...)
        Expression::InList { expr: col_expr, values, negated } => {
            if !negated {
                if let Expression::ColumnRef(col_id) = col_expr.as_ref() {
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
                        in_predicates.insert(col_id.column_canonical().to_uppercase(), in_values);
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
        Expression::ColumnRef(col_id) => Some(col_id.column_canonical()),
        _ => None,
    }
}
