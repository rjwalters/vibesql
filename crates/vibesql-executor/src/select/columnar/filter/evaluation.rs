use vibesql_types::SqlValue;

use super::{
    comparison::compare_values,
    predicates::{ColumnPredicate, CompareOp, PredicateTree},
};

/// Strict equality comparison without string-to-number coercion
///
/// Returns true if both values can be considered equal WITHOUT coercing
/// string values to numbers. This is used for columns with NONE or INTEGER
/// affinity in IN expressions, where string values should NOT be coerced.
///
/// Numeric types (Integer, Float, Real, Double, etc.) can still be compared
/// with each other since they're all in the same storage class.
fn strict_type_equal(a: &SqlValue, b: &SqlValue) -> bool {
    use SqlValue::*;

    // NULL never equals anything
    if matches!(a, Null) || matches!(b, Null) {
        return false;
    }

    // Helper to check if a value is a string type
    let is_string = |v: &SqlValue| matches!(v, Varchar(_) | Character(_));

    // Helper to check if a value is a numeric type
    let is_numeric = |v: &SqlValue| {
        matches!(
            v,
            Integer(_) | Bigint(_) | Smallint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
        )
    };

    // Different storage classes (string vs numeric) - NOT equal without coercion
    if (is_string(a) && is_numeric(b)) || (is_numeric(a) && is_string(b)) {
        return false;
    }

    // Same storage class - use normal comparison
    match (a, b) {
        // Same integer types
        (Integer(x), Integer(y)) => x == y,
        (Bigint(x), Bigint(y)) => x == y,
        (Smallint(x), Smallint(y)) => x == y,

        // Same string types (VARCHAR and CHAR are compatible)
        (Varchar(x), Varchar(y)) | (Character(x), Character(y)) => x == y,
        (Varchar(x), Character(y)) | (Character(x), Varchar(y)) => x == y,

        // Same float types
        (Float(x), Float(y)) => (x - y).abs() < f32::EPSILON,
        (Double(x), Double(y)) => (x - y).abs() < f64::EPSILON,
        (Real(x), Real(y)) => (x - y).abs() < f64::EPSILON,

        // Cross-type numeric comparisons (all numeric types are comparable)
        (Integer(x), Bigint(y)) | (Bigint(y), Integer(x)) => *x as i64 == *y,
        (Integer(x), Smallint(y)) | (Smallint(y), Integer(x)) => *x == *y as i64,
        (Bigint(x), Smallint(y)) | (Smallint(y), Bigint(x)) => *x == *y as i64,

        // Float types with integer
        (Float(x), Integer(y)) | (Integer(y), Float(x)) => {
            (*x as f64 - *y as f64).abs() < f64::EPSILON
        }
        (Double(x), Integer(y)) | (Integer(y), Double(x)) => (*x - *y as f64).abs() < f64::EPSILON,
        (Real(x), Integer(y)) | (Integer(y), Real(x)) => (*x - *y as f64).abs() < f64::EPSILON,

        // Float with Bigint
        (Float(x), Bigint(y)) | (Bigint(y), Float(x)) => {
            (*x as f64 - *y as f64).abs() < f64::EPSILON
        }
        (Double(x), Bigint(y)) | (Bigint(y), Double(x)) => (*x - *y as f64).abs() < f64::EPSILON,
        (Real(x), Bigint(y)) | (Bigint(y), Real(x)) => (*x - *y as f64).abs() < f64::EPSILON,

        // Mixed float types
        (Float(x), Double(y)) | (Double(y), Float(x)) => (*x as f64 - *y).abs() < f64::EPSILON,
        (Float(x), Real(y)) | (Real(y), Float(x)) => (*x as f64 - *y).abs() < f64::EPSILON,
        (Double(x), Real(y)) | (Real(y), Double(x)) => (*x - *y).abs() < f64::EPSILON,

        // Numeric type (f64) with integer types
        (Numeric(x), Integer(y)) | (Integer(y), Numeric(x)) => {
            (*x - *y as f64).abs() < f64::EPSILON
        }
        (Numeric(x), Bigint(y)) | (Bigint(y), Numeric(x)) => (*x - *y as f64).abs() < f64::EPSILON,
        (Numeric(x), Smallint(y)) | (Smallint(y), Numeric(x)) => {
            (*x - *y as f64).abs() < f64::EPSILON
        }

        // Numeric with float types
        (Numeric(x), Float(y)) | (Float(y), Numeric(x)) => (*x - *y as f64).abs() < f64::EPSILON,
        (Numeric(x), Double(y)) | (Double(y), Numeric(x)) => (*x - *y).abs() < f64::EPSILON,
        (Numeric(x), Real(y)) | (Real(y), Numeric(x)) => (*x - *y).abs() < f64::EPSILON,
        (Numeric(x), Numeric(y)) => (*x - *y).abs() < f64::EPSILON,

        // Different storage classes not handled above - NOT equal
        _ => false,
    }
}

/// Evaluate a predicate tree on a row
///
/// Returns true if the row satisfies the entire predicate tree.
/// Implements proper short-circuit semantics for AND/OR.
///
/// # Arguments
///
/// * `tree` - The predicate tree to evaluate
/// * `get_value` - Closure to get a value at a column index for the current row
///
/// # Returns
///
/// true if the row passes the predicate tree, false otherwise
pub fn evaluate_predicate_tree<'a, F>(tree: &PredicateTree, mut get_value: F) -> bool
where
    F: FnMut(usize) -> Option<&'a SqlValue>,
{
    evaluate_predicate_tree_impl(tree, &mut get_value)
}

/// Internal implementation of predicate tree evaluation
///
/// This helper function allows proper recursion with mutable closure references.
pub(super) fn evaluate_predicate_tree_impl<'a, F>(tree: &PredicateTree, get_value: &mut F) -> bool
where
    F: FnMut(usize) -> Option<&'a SqlValue>,
{
    match tree {
        PredicateTree::And(children) => {
            // All children must be true - short-circuit on first false
            for child in children {
                if !evaluate_predicate_tree_impl(child, get_value) {
                    return false;
                }
            }
            true
        }
        PredicateTree::Or(children) => {
            // At least one child must be true - short-circuit on first true
            for child in children {
                if evaluate_predicate_tree_impl(child, get_value) {
                    return true;
                }
            }
            false
        }
        PredicateTree::Leaf(predicate) => {
            // Handle ColumnCompare specially - needs two column values
            if let ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } =
                predicate
            {
                let left_val = get_value(*left_column_idx);
                let right_val = get_value(*right_column_idx);
                return evaluate_column_compare(*op, left_val, right_val);
            }

            // Get the column value and evaluate the leaf predicate
            let column_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. }
                | ColumnPredicate::GreaterThan { column_idx, .. }
                | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
                | ColumnPredicate::LessThanOrEqual { column_idx, .. }
                | ColumnPredicate::Equal { column_idx, .. }
                | ColumnPredicate::NotEqual { column_idx, .. }
                | ColumnPredicate::Between { column_idx, .. }
                | ColumnPredicate::Like { column_idx, .. }
                | ColumnPredicate::InList { column_idx, .. } => *column_idx,
                ColumnPredicate::ColumnCompare { .. } => unreachable!(), // Handled above
            };

            if let Some(value) = get_value(column_idx) {
                evaluate_predicate(predicate, value)
            } else {
                // NULL values fail all predicates
                false
            }
        }
    }
}

/// Evaluate a column-to-column comparison
///
/// Returns true if left op right is satisfied.
/// Returns false if either value is NULL (per SQL standard).
pub fn evaluate_column_compare(
    op: CompareOp,
    left: Option<&SqlValue>,
    right: Option<&SqlValue>,
) -> bool {
    use std::cmp::Ordering;

    // NULL handling: any comparison with NULL returns false
    let (left_val, right_val) = match (left, right) {
        (Some(l), Some(r)) => (l, r),
        _ => return false,
    };

    // Both values are NULL
    if matches!(left_val, SqlValue::Null) || matches!(right_val, SqlValue::Null) {
        return false;
    }

    let cmp_result = compare_values(left_val, right_val);

    match op {
        CompareOp::LessThan => cmp_result.equals(Ordering::Less),
        CompareOp::GreaterThan => cmp_result.equals(Ordering::Greater),
        CompareOp::LessThanOrEqual => cmp_result.matches(&[Ordering::Less, Ordering::Equal]),
        CompareOp::GreaterThanOrEqual => cmp_result.matches(&[Ordering::Greater, Ordering::Equal]),
        CompareOp::Equal => cmp_result.equals(Ordering::Equal),
        CompareOp::NotEqual => cmp_result.matches(&[Ordering::Less, Ordering::Greater]),
    }
}

/// Evaluate a column predicate on a specific value
///
/// Returns true if the value satisfies the predicate.
/// Returns false if either the value or predicate threshold is NULL (per SQL standard).
pub fn evaluate_predicate(predicate: &ColumnPredicate, value: &SqlValue) -> bool {
    use std::cmp::Ordering;

    match predicate {
        ColumnPredicate::LessThan { value: threshold, .. } => {
            compare_values(value, threshold).equals(Ordering::Less)
        }
        ColumnPredicate::GreaterThan { value: threshold, .. } => {
            compare_values(value, threshold).equals(Ordering::Greater)
        }
        ColumnPredicate::GreaterThanOrEqual { value: threshold, .. } => {
            compare_values(value, threshold).matches(&[Ordering::Greater, Ordering::Equal])
        }
        ColumnPredicate::LessThanOrEqual { value: threshold, .. } => {
            compare_values(value, threshold).matches(&[Ordering::Less, Ordering::Equal])
        }
        ColumnPredicate::Equal { value: target, .. } => {
            compare_values(value, target).equals(Ordering::Equal)
        }
        ColumnPredicate::NotEqual { value: target, .. } => {
            // NotEqual returns true for any ordering that is NOT Equal
            compare_values(value, target).matches(&[Ordering::Less, Ordering::Greater])
        }
        ColumnPredicate::Between { low, high, .. } => {
            // Both bounds must pass - if either comparison involves NULL, it returns false
            let passes_low =
                compare_values(value, low).matches(&[Ordering::Greater, Ordering::Equal]);
            let passes_high =
                compare_values(value, high).matches(&[Ordering::Less, Ordering::Equal]);
            passes_low && passes_high
        }
        ColumnPredicate::Like { pattern, negated, case_sensitive, escape, .. } => {
            // Issue #4913: SQLite coerces numeric types to strings for LIKE comparison
            let text_owned: String;
            let text: &str = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s,
                SqlValue::Null => return false,
                // SQLite coerces numeric types to strings for LIKE comparison
                SqlValue::Integer(i) => {
                    text_owned = i.to_string();
                    &text_owned
                }
                SqlValue::Bigint(i) => {
                    text_owned = i.to_string();
                    &text_owned
                }
                SqlValue::Float(f) => {
                    text_owned = f.to_string();
                    &text_owned
                }
                SqlValue::Double(f) => {
                    text_owned = f.to_string();
                    &text_owned
                }
                SqlValue::Real(f) => {
                    text_owned = f.to_string();
                    &text_owned
                }
                SqlValue::Blob(b) => {
                    text_owned = String::from_utf8_lossy(b).into_owned();
                    &text_owned
                }
                _ => return false,
            };

            let matches =
                crate::evaluator::pattern::like_match(text, pattern, *case_sensitive, *escape);
            if *negated {
                !matches
            } else {
                matches
            }
        }
        ColumnPredicate::InList {
            values: list_values, negated, use_strict_type_ordering, ..
        } => {
            use std::cmp::Ordering;
            use vibesql_types::SqlValue;

            // A NULL column value is UNKNOWN for both IN and NOT IN (issue
            // #5341): excluded in WHERE context. Without this check the
            // negated branch below returned TRUE for NULL values.
            if matches!(value, SqlValue::Null) {
                return false;
            }

            // Check if value matches any in the list
            let matches = list_values.iter().any(|list_val| {
                // Handle NULL in list
                if matches!(list_val, SqlValue::Null) {
                    return false;
                }

                if *use_strict_type_ordering {
                    // Use strict type ordering (no coercion) for NONE/INTEGER affinity
                    // Integer != Varchar even if they have the same numeric value
                    strict_type_equal(value, list_val)
                } else {
                    // Use normal comparison with type coercion
                    compare_values(value, list_val).equals(Ordering::Equal)
                }
            });

            if *negated {
                // For NOT IN, also check for NULL in the list
                // If any value in the list is NULL and no exact match was found,
                // the result is UNKNOWN (false in WHERE context)
                let has_null_in_list = list_values.iter().any(|v| matches!(v, SqlValue::Null));
                if has_null_in_list && !matches {
                    false // UNKNOWN in WHERE context
                } else {
                    !matches
                }
            } else {
                matches
            }
        }
        // ColumnCompare should be handled by evaluate_column_compare, not here
        // This case is included for exhaustiveness but shouldn't be reached in normal use
        ColumnPredicate::ColumnCompare { .. } => {
            // Cannot evaluate column-to-column with single value
            // Return false as fallback (this path shouldn't be hit)
            false
        }
    }
}

// NOTE: The old like_match() function was removed in favor of
// crate::evaluator::pattern::like_match() which supports case_sensitive
// and escape parameters correctly.
