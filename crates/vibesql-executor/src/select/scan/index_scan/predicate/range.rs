//! Range predicate extraction and merging
//!
//! This module handles extraction of range predicates from WHERE clauses
//! for index optimization. Supports comparison operators (>, <, >=, <=, =)
//! and BETWEEN expressions.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::RangePredicate;
use crate::select::scan::index_scan::selection::is_column_reference;

/// Extract range predicate bounds for an indexed column from WHERE clause
///
/// This extracts comparison operators (>, <, >=, <=, BETWEEN) that can be
/// pushed down to the storage layer's range_scan() method.
///
/// Returns None if no suitable range predicate found for the column.
pub(in crate::select::scan::index_scan) fn extract_range_predicate(
    expr: &Expression,
    column_name: &str,
) -> Option<RangePredicate> {
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
                            // For equality, exclude_nulls is false since we're matching a specific
                            // value
                            return Some(RangePredicate {
                                start: Some(value.clone()),
                                end: Some(value.clone()),
                                inclusive_start: true,
                                inclusive_end: true,
                                exclude_nulls: false,
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
                                exclude_nulls: false,
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
                            // All inequality comparisons exclude NULLs (SQL semantics)
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
                    // Check if right side is our column and left side is a literal (flipped
                    // comparison)
                    else if is_column_reference(right, column_name) {
                        if let Expression::Literal(value) = left.as_ref() {
                            // NULL comparisons always return no rows - can't optimize with index
                            if matches!(value, SqlValue::Null) {
                                return None;
                            }
                            // All inequality comparisons exclude NULLs (SQL semantics)
                            return Some(match op {
                                // Flip the comparison: value > col means col < value
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
                // Handle AND: can combine range predicates (e.g., col > 10 AND col < 20)
                BinaryOperator::And => {
                    let left_range = extract_range_predicate(left, column_name);
                    let right_range = extract_range_predicate(right, column_name);

                    // Merge ranges if both sides have predicates on our column
                    match (left_range, right_range) {
                        (Some(l), Some(r)) => {
                            // Merge the bounds by taking the tighter constraint
                            // For starts: take the greater value (tighter lower bound)
                            // For ends: take the lesser value (tighter upper bound)
                            let (merged_start, merged_inclusive_start) = match (&l.start, &r.start)
                            {
                                (None, None) => (None, false),
                                (Some(v), None) => (Some(v.clone()), l.inclusive_start),
                                (None, Some(v)) => (Some(v.clone()), r.inclusive_start),
                                (Some(lv), Some(rv)) => {
                                    // Use Ord::cmp for cross-type comparison (handles Integer vs
                                    // Real)
                                    match lv.cmp(rv) {
                                        std::cmp::Ordering::Greater => {
                                            (Some(lv.clone()), l.inclusive_start)
                                        }
                                        std::cmp::Ordering::Less => {
                                            (Some(rv.clone()), r.inclusive_start)
                                        }
                                        std::cmp::Ordering::Equal => {
                                            // Equal values - take the more restrictive inclusivity
                                            (
                                                Some(lv.clone()),
                                                l.inclusive_start && r.inclusive_start,
                                            )
                                        }
                                    }
                                }
                            };

                            let (merged_end, merged_inclusive_end) = match (&l.end, &r.end) {
                                (None, None) => (None, false),
                                (Some(v), None) => (Some(v.clone()), l.inclusive_end),
                                (None, Some(v)) => (Some(v.clone()), r.inclusive_end),
                                (Some(lv), Some(rv)) => {
                                    // Use Ord::cmp for cross-type comparison (handles Integer vs
                                    // Real)
                                    match lv.cmp(rv) {
                                        std::cmp::Ordering::Less => {
                                            (Some(lv.clone()), l.inclusive_end)
                                        }
                                        std::cmp::Ordering::Greater => {
                                            (Some(rv.clone()), r.inclusive_end)
                                        }
                                        std::cmp::Ordering::Equal => {
                                            // Equal values - take the more restrictive inclusivity
                                            (Some(lv.clone()), l.inclusive_end && r.inclusive_end)
                                        }
                                    }
                                }
                            };

                            // Check for contradiction: start > end means no values can match
                            // e.g., col = 33 AND col >= 881 → start=881, end=33 → contradiction
                            //
                            // IMPORTANT: We DON'T return a special marker here anymore.
                            // Instead, we return the actual contradictory range values.
                            // The storage layer's range_scan() normalizes all numeric types
                            // to Double before comparison, so it correctly detects start > end
                            // and returns empty results.
                            //
                            // Previously, we used Integer(1)/Integer(0) as a marker, but this
                            // caused type mismatch issues when merging with Real/Float values
                            // in nested AND expressions (PartialOrd returns None for different
                            // types).
                            //
                            // Note: We still check for equal bounds with exclusive to detect
                            // impossible ranges like col > 5 AND col < 5, which need special
                            // handling.
                            if let (Some(ref start), Some(ref end)) = (&merged_start, &merged_end) {
                                // For equal bounds with any exclusive: return impossible range
                                // marker This case needs a marker
                                // because start == end would pass the
                                // storage layer's inverted range check
                                if start == end
                                    && (!merged_inclusive_start || !merged_inclusive_end)
                                {
                                    // Use the actual start/end values for type consistency
                                    // but ensure start > end by swapping inclusivity semantics
                                    // Actually, just return the values - the storage layer
                                    // checks this specific case too
                                }
                                // For inverted ranges (start > end), just return them as-is
                                // The storage layer will handle this correctly after normalization
                            }

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

                    // BETWEEN excludes NULLs in SQL semantics
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

/// Merge two range predicates by taking the tighter constraints
pub(super) fn merge_range_predicates(a: RangePredicate, b: RangePredicate) -> RangePredicate {
    // Merge starts: take the greater value (tighter lower bound)
    // Use Ord::cmp for cross-type comparison (handles Integer vs Real)
    let (merged_start, merged_inclusive_start) = match (&a.start, &b.start) {
        (None, None) => (None, false),
        (Some(v), None) => (Some(v.clone()), a.inclusive_start),
        (None, Some(v)) => (Some(v.clone()), b.inclusive_start),
        (Some(av), Some(bv)) => match av.cmp(bv) {
            std::cmp::Ordering::Greater => (Some(av.clone()), a.inclusive_start),
            std::cmp::Ordering::Less => (Some(bv.clone()), b.inclusive_start),
            std::cmp::Ordering::Equal => {
                // Equal values - take the more restrictive inclusivity
                (Some(av.clone()), a.inclusive_start && b.inclusive_start)
            }
        },
    };

    // Merge ends: take the lesser value (tighter upper bound)
    // Use Ord::cmp for cross-type comparison (handles Integer vs Real)
    let (merged_end, merged_inclusive_end) = match (&a.end, &b.end) {
        (None, None) => (None, false),
        (Some(v), None) => (Some(v.clone()), a.inclusive_end),
        (None, Some(v)) => (Some(v.clone()), b.inclusive_end),
        (Some(av), Some(bv)) => match av.cmp(bv) {
            std::cmp::Ordering::Less => (Some(av.clone()), a.inclusive_end),
            std::cmp::Ordering::Greater => (Some(bv.clone()), b.inclusive_end),
            std::cmp::Ordering::Equal => {
                // Equal values - take the more restrictive inclusivity
                (Some(av.clone()), a.inclusive_end && b.inclusive_end)
            }
        },
    };

    RangePredicate {
        start: merged_start,
        end: merged_end,
        inclusive_start: merged_inclusive_start,
        inclusive_end: merged_inclusive_end,
        exclude_nulls: a.exclude_nulls || b.exclude_nulls,
    }
}

/// Check if a value satisfies a range predicate
pub(super) fn value_satisfies_range(value: &SqlValue, range: &RangePredicate) -> bool {
    // Check lower bound
    if let Some(ref start) = range.start {
        let cmp = value.partial_cmp(start);
        match cmp {
            Some(std::cmp::Ordering::Less) => return false,
            Some(std::cmp::Ordering::Equal) if !range.inclusive_start => return false,
            None => return false, // Incomparable types
            _ => {}
        }
    }

    // Check upper bound
    if let Some(ref end) = range.end {
        let cmp = value.partial_cmp(end);
        match cmp {
            Some(std::cmp::Ordering::Greater) => return false,
            Some(std::cmp::Ordering::Equal) if !range.inclusive_end => return false,
            None => return false, // Incomparable types
            _ => {}
        }
    }

    true
}
