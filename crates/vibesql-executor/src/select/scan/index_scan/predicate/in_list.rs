//! IN predicate extraction and index predicate main entry point
//!
//! This module handles IN predicate extraction from WHERE clauses and provides
//! the main entry point for extracting index predicates (combining range and IN).

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::{
    range::{extract_range_predicate, merge_range_predicates, value_satisfies_range},
    IndexPredicate, RangePredicate,
};
use crate::select::scan::index_scan::selection::is_column_reference;

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

        // Check for range + IN combined predicates (e.g., col IN (64, 4) AND col > 75)
        // Filter IN values by the range predicate and return the filtered list
        //
        // Issue #5823: `value_satisfies_range` compares with BINARY ordering and
        // is therefore NOT collation-aware. This is safe here because the result
        // only ever *narrows* the IN-list used as an index-probe hint — it can
        // drop the probe to a smaller/empty set but never fabricates rows. On a
        // non-BINARY-collated column the raw probe is already declined upstream
        // (execution.rs gates `IndexPredicate::In`/`Range`), so the query falls
        // back to the collation-aware full-scan WHERE evaluator regardless of any
        // over-filtering here. Do not treat this filtered list as a correct
        // collation-aware result on its own.
        if let (Some(in_vals), Some(ref range)) = (&in_values, &range_pred) {
            let satisfying_values: Vec<SqlValue> =
                in_vals.iter().filter(|v| value_satisfies_range(v, range)).cloned().collect();

            // Return the filtered IN list (may be empty for full contradiction)
            // This ensures BOTH predicates are satisfied by using only IN values that pass the
            // range
            return Some(IndexPredicate::In(satisfying_values));
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
pub(super) fn collect_column_predicates(
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
                                exclude_nulls: false,
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
                                exclude_nulls: false,
                            });
                        }
                    }
                }
            }
        }
        // Range predicates: col > value, col < value, etc.
        Expression::BinaryOp {
            left,
            op:
                op @ (BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual),
            right,
        } => {
            // Check if left side is our column and right side is a literal
            if is_column_reference(left, column_name) {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        let new_range = match op {
                            BinaryOperator::GreaterThan => RangePredicate {
                                start: Some(value.clone()),
                                end: None,
                                inclusive_start: false,
                                inclusive_end: false,
                                exclude_nulls: true,
                            },
                            BinaryOperator::GreaterThanOrEqual => RangePredicate {
                                start: Some(value.clone()),
                                end: None,
                                inclusive_start: true,
                                inclusive_end: false,
                                exclude_nulls: true,
                            },
                            BinaryOperator::LessThan => RangePredicate {
                                start: None,
                                end: Some(value.clone()),
                                inclusive_start: false,
                                inclusive_end: false,
                                exclude_nulls: true,
                            },
                            BinaryOperator::LessThanOrEqual => RangePredicate {
                                start: None,
                                end: Some(value.clone()),
                                inclusive_start: false,
                                inclusive_end: true,
                                exclude_nulls: true,
                            },
                            _ => unreachable!(),
                        };
                        // Merge with existing range if any
                        if let Some(existing) = range_pred.take() {
                            *range_pred = Some(merge_range_predicates(existing, new_range));
                        } else {
                            *range_pred = Some(new_range);
                        }
                    }
                }
            }
            // Check if right side is our column (flipped comparison)
            else if is_column_reference(right, column_name) {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, SqlValue::Null) {
                        // Flip the comparison: value > col means col < value
                        let new_range = match op {
                            BinaryOperator::GreaterThan => RangePredicate {
                                start: None,
                                end: Some(value.clone()),
                                inclusive_start: false,
                                inclusive_end: false,
                                exclude_nulls: true,
                            },
                            BinaryOperator::GreaterThanOrEqual => RangePredicate {
                                start: None,
                                end: Some(value.clone()),
                                inclusive_start: false,
                                inclusive_end: true,
                                exclude_nulls: true,
                            },
                            BinaryOperator::LessThan => RangePredicate {
                                start: Some(value.clone()),
                                end: None,
                                inclusive_start: false,
                                inclusive_end: false,
                                exclude_nulls: true,
                            },
                            BinaryOperator::LessThanOrEqual => RangePredicate {
                                start: Some(value.clone()),
                                end: None,
                                inclusive_start: true,
                                inclusive_end: false,
                                exclude_nulls: true,
                            },
                            _ => unreachable!(),
                        };
                        // Merge with existing range if any
                        if let Some(existing) = range_pred.take() {
                            *range_pred = Some(merge_range_predicates(existing, new_range));
                        } else {
                            *range_pred = Some(new_range);
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
                    // Merge with existing IN list by computing intersection
                    // col IN (1,2,3) AND col IN (2,3,4) => col IN (2,3)
                    if let Some(existing) = in_values.take() {
                        let intersection: Vec<SqlValue> =
                            existing.into_iter().filter(|v| values.contains(v)).collect();
                        *in_values = Some(intersection);
                    } else {
                        *in_values = Some(values);
                    }
                }
            }
        }
        // BETWEEN: col BETWEEN low AND high
        Expression::Between { expr: col_expr, low, high, negated, symmetric } => {
            if !negated && is_column_reference(col_expr, column_name) {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    if !matches!(low_val, SqlValue::Null) && !matches!(high_val, SqlValue::Null) {
                        // Handle SYMMETRIC: swap bounds if low > high
                        let (effective_low, effective_high) = if *symmetric && low_val > high_val {
                            (high_val.clone(), low_val.clone())
                        } else {
                            (low_val.clone(), high_val.clone())
                        };

                        let new_range = RangePredicate {
                            start: Some(effective_low),
                            end: Some(effective_high),
                            inclusive_start: true,
                            inclusive_end: true,
                            exclude_nulls: true,
                        };

                        // Merge with existing range if any
                        if let Some(existing) = range_pred.take() {
                            *range_pred = Some(merge_range_predicates(existing, new_range));
                        } else {
                            *range_pred = Some(new_range);
                        }
                    }
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
