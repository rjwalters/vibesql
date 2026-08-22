//! Expression index support
//!
//! This module handles predicate extraction for expression-based (functional) indexes.
//! For example, an index on `LOWER(email)` can be used to optimize queries with
//! `WHERE LOWER(email) = 'x'`.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::{
    in_list::{extract_index_predicate, where_clause_fully_satisfied_by_index},
    IndexPredicate, RangePredicate,
};
use crate::select::scan::index_scan::selection::expressions_match;

/// Check if an expression matches an indexed expression (for expression indexes)
fn is_expression_match(expr: &Expression, index_expr: &Expression) -> bool {
    expressions_match(expr, index_expr)
}

/// Extract range predicate for an expression index from WHERE clause
///
/// Analogous to `extract_range_predicate` but for expression indexes.
/// For `CREATE INDEX idx ON t(LOWER(email))` and `WHERE LOWER(email) = 'x'`,
/// this extracts the range predicate based on the value 'x'.
fn extract_range_predicate_for_expression(
    expr: &Expression,
    index_expr: &Expression,
) -> Option<RangePredicate> {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                // Handle equality: expr = value
                BinaryOperator::Equal => {
                    // Check if left side is our indexed expression and right side is a literal
                    if is_expression_match(left, index_expr) {
                        if let Expression::Literal(value) = right.as_ref() {
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            return Some(RangePredicate {
                                start: Some(value.clone()),
                                end: Some(value.clone()),
                                inclusive_start: true,
                                inclusive_end: true,
                                exclude_nulls: false,
                            });
                        }
                    }
                    // Also handle reverse: value = expr
                    if is_expression_match(right, index_expr) {
                        if let Expression::Literal(value) = left.as_ref() {
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            return Some(RangePredicate {
                                start: Some(value.clone()),
                                end: Some(value.clone()),
                                inclusive_start: true,
                                inclusive_end: true,
                                exclude_nulls: false,
                            });
                        }
                    }
                }
                // Handle simple comparisons: expr > value, expr < value, etc.
                BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual => {
                    // Check if left side is our expression and right side is a literal
                    if is_expression_match(left, index_expr) {
                        if let Expression::Literal(value) = right.as_ref() {
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            return Some(match op {
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
                            });
                        }
                    }
                    // Check if right side is our expression (flipped comparison)
                    else if is_expression_match(right, index_expr) {
                        if let Expression::Literal(value) = left.as_ref() {
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            return Some(match op {
                                // Flip: value > expr means expr < value
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
                            });
                        }
                    }
                }
                // Handle AND: can combine range predicates
                BinaryOperator::And => {
                    let left_range = extract_range_predicate_for_expression(left, index_expr);
                    let right_range = extract_range_predicate_for_expression(right, index_expr);

                    match (left_range, right_range) {
                        (Some(l), Some(r)) => {
                            // Merge the bounds by taking the tighter constraint
                            let (merged_start, merged_inclusive_start) = match (&l.start, &r.start)
                            {
                                (None, None) => (None, false),
                                (Some(v), None) => (Some(v.clone()), l.inclusive_start),
                                (None, Some(v)) => (Some(v.clone()), r.inclusive_start),
                                (Some(lv), Some(rv)) => {
                                    if lv > rv {
                                        (Some(lv.clone()), l.inclusive_start)
                                    } else if rv > lv {
                                        (Some(rv.clone()), r.inclusive_start)
                                    } else {
                                        (Some(lv.clone()), l.inclusive_start && r.inclusive_start)
                                    }
                                }
                            };

                            let (merged_end, merged_inclusive_end) = match (&l.end, &r.end) {
                                (None, None) => (None, false),
                                (Some(v), None) => (Some(v.clone()), l.inclusive_end),
                                (None, Some(v)) => (Some(v.clone()), r.inclusive_end),
                                (Some(lv), Some(rv)) => {
                                    if lv < rv {
                                        (Some(lv.clone()), l.inclusive_end)
                                    } else if rv < lv {
                                        (Some(rv.clone()), r.inclusive_end)
                                    } else {
                                        (Some(lv.clone()), l.inclusive_end && r.inclusive_end)
                                    }
                                }
                            };

                            // Check for contradiction: same logic as extract_range_predicate
                            // We don't use a special marker anymore - just return the merged values
                            // and let the storage layer handle inverted/impossible ranges.
                            // See comment in extract_range_predicate for full explanation.

                            return Some(RangePredicate {
                                start: merged_start,
                                end: merged_end,
                                inclusive_start: merged_inclusive_start,
                                inclusive_end: merged_inclusive_end,
                                exclude_nulls: l.exclude_nulls || r.exclude_nulls,
                            });
                        }
                        (Some(l), None) => return Some(l),
                        (None, Some(r)) => return Some(r),
                        (None, None) => {}
                    }
                }
                _ => {}
            }
        }
        // Handle BETWEEN: expr BETWEEN low AND high
        Expression::Between { expr: col_expr, low, high, negated, symmetric } => {
            if !negated && is_expression_match(col_expr, index_expr) {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    if matches!(low_val, SqlValue::Null) || matches!(high_val, SqlValue::Null) {
                        return None;
                    }

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
                        exclude_nulls: true,
                    });
                }
            }
        }
        _ => {}
    }

    None
}

/// Extract index predicate for an expression index from WHERE clause
///
/// Analogous to `extract_index_predicate` but for expression indexes.
pub(crate) fn extract_index_predicate_for_expression(
    expr: &Expression,
    index_expr: &Expression,
) -> Option<IndexPredicate> {
    // Try to extract a range predicate
    if let Some(range) = extract_range_predicate_for_expression(expr, index_expr) {
        return Some(IndexPredicate::Range(range));
    }

    // Then try to extract an IN predicate
    match expr {
        // Handle IN with value list: expr IN (1, 2, 3)
        Expression::InList { expr: in_expr, values: value_list, negated } => {
            if !negated && is_expression_match(in_expr, index_expr) {
                let mut values = Vec::new();
                let mut has_null = false;
                for item in value_list {
                    if let Expression::Literal(value) = item {
                        if matches!(value, SqlValue::Null) {
                            has_null = true;
                        }
                        values.push(value.clone());
                    } else {
                        return None;
                    }
                }

                if has_null {
                    return None;
                }

                if !values.is_empty() {
                    return Some(IndexPredicate::In(values));
                }
            }
        }
        // Handle AND: try to extract predicate from either side
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            if let Some(pred) = extract_index_predicate_for_expression(left, index_expr) {
                return Some(pred);
            }
            if let Some(pred) = extract_index_predicate_for_expression(right, index_expr) {
                return Some(pred);
            }
        }
        _ => {}
    }

    None
}

/// Extract index predicate for an indexed column (unified version)
///
/// This is a unified entry point that handles both column-based and expression-based indexes.
/// It dispatches to the appropriate extraction function based on the index column type.
pub(crate) fn extract_index_predicate_for_indexed_column(
    expr: &Expression,
    index_col: &vibesql_ast::IndexColumn,
) -> Option<IndexPredicate> {
    match index_col {
        vibesql_ast::IndexColumn::Column { column_name, .. } => {
            extract_index_predicate(expr, column_name)
        }
        vibesql_ast::IndexColumn::Expression { expr: index_expr, .. } => {
            extract_index_predicate_for_expression(expr, index_expr)
        }
    }
}

/// Check if WHERE clause is fully satisfied by an expression index predicate
///
/// Analogous to `where_clause_fully_satisfied_by_index` but for expression indexes.
pub(crate) fn where_clause_fully_satisfied_by_expression_index(
    where_expr: &Expression,
    index_expr: &Expression,
) -> bool {
    match where_expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Equal
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual => {
                let left_is_expr = is_expression_match(left, index_expr);
                let right_is_expr = is_expression_match(right, index_expr);
                let left_is_literal = matches!(left.as_ref(), Expression::Literal(_));
                let right_is_literal = matches!(right.as_ref(), Expression::Literal(_));

                (left_is_expr && right_is_literal) || (left_is_literal && right_is_expr)
            }
            BinaryOperator::And => {
                let left_satisfied =
                    where_clause_fully_satisfied_by_expression_index(left, index_expr);
                let right_satisfied =
                    where_clause_fully_satisfied_by_expression_index(right, index_expr);
                left_satisfied && right_satisfied
            }
            _ => false,
        },
        Expression::Between { expr: col_expr, low, high, negated, symmetric } => {
            !negated
                && !symmetric
                && is_expression_match(col_expr, index_expr)
                && matches!(low.as_ref(), Expression::Literal(_))
                && matches!(high.as_ref(), Expression::Literal(_))
        }
        Expression::InList { expr: in_expr, values, negated } => {
            !negated
                && is_expression_match(in_expr, index_expr)
                && values.iter().all(|v| matches!(v, Expression::Literal(_)))
        }
        _ => false,
    }
}

/// Check if WHERE clause is fully satisfied by indexed column (unified version)
///
/// This is a unified entry point that handles both column-based and expression-based indexes.
pub(crate) fn where_clause_fully_satisfied_by_indexed_column(
    where_expr: &Expression,
    index_col: &vibesql_ast::IndexColumn,
    index_predicate: &Option<IndexPredicate>,
) -> bool {
    match index_col {
        vibesql_ast::IndexColumn::Column { column_name, .. } => {
            // Use the existing function but only if we have a predicate
            if index_predicate.is_some() {
                where_clause_fully_satisfied_by_index(where_expr, column_name)
            } else {
                false
            }
        }
        vibesql_ast::IndexColumn::Expression { expr: index_expr, .. } => {
            where_clause_fully_satisfied_by_expression_index(where_expr, index_expr)
        }
    }
}
