use super::comparison::compare_values;
use super::predicates::{ColumnPredicate, PredicateTree};
use vibesql_types::SqlValue;

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
            // Get the column value and evaluate the leaf predicate
            let column_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. }
                | ColumnPredicate::GreaterThan { column_idx, .. }
                | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
                | ColumnPredicate::LessThanOrEqual { column_idx, .. }
                | ColumnPredicate::Equal { column_idx, .. }
                | ColumnPredicate::NotEqual { column_idx, .. }
                | ColumnPredicate::Between { column_idx, .. } => *column_idx,
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
            let passes_low = compare_values(value, low).matches(&[Ordering::Greater, Ordering::Equal]);
            let passes_high = compare_values(value, high).matches(&[Ordering::Less, Ordering::Equal]);
            passes_low && passes_high
        }
    }
}
