use super::comparison::compare_values;
use super::predicates::{ColumnPredicate, CompareOp, PredicateTree};
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
        ColumnPredicate::Like { pattern, negated, .. } => {
            // Extract string value
            let text = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                SqlValue::Null => return false,
                _ => return false, // Non-string types don't match LIKE patterns
            };

            let matches = like_match(text, pattern);
            if *negated {
                !matches
            } else {
                matches
            }
        }
        ColumnPredicate::InList { values: list_values, negated, .. } => {
            use std::cmp::Ordering;
            // Check if value matches any in the list
            let matches = list_values
                .iter()
                .any(|list_val| compare_values(value, list_val).equals(Ordering::Equal));
            if *negated {
                !matches
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

/// Match a string against a SQL LIKE pattern
///
/// Uses dynamic programming for pattern matching with `%` and `_` wildcards.
fn like_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    let m = text_chars.len();
    let n = pattern_chars.len();

    // dp[i][j] = true if text[0..i] matches pattern[0..j]
    let mut dp = vec![vec![false; n + 1]; m + 1];

    // Empty pattern matches empty text
    dp[0][0] = true;

    // Handle leading % in pattern (can match empty string)
    for j in 1..=n {
        if pattern_chars[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }

    // Fill the DP table
    for i in 1..=m {
        for j in 1..=n {
            let pc = pattern_chars[j - 1];
            let tc = text_chars[i - 1];

            if pc == '%' {
                // % matches zero or more characters
                dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
            } else if pc == '_' || pc == tc {
                // _ matches any single character, or exact match
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[m][n]
}
