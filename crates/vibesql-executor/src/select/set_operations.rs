//! Set operations (UNION, INTERSECT, EXCEPT) for SELECT queries

use std::collections::{HashMap, HashSet};

use super::helpers::apply_distinct;
use crate::errors::ExecutorError;

/// Normalize a row's values for comparison based on column collations.
/// This applies collation transformations (e.g., case folding for NOCASE).
fn normalize_row_for_comparison(
    values: &[vibesql_types::SqlValue],
    collations: &[Option<String>],
) -> Vec<vibesql_types::SqlValue> {
    use arcstr::ArcStr;
    use vibesql_types::SqlValue;

    values
        .iter()
        .enumerate()
        .map(|(i, val)| {
            let collation = collations.get(i).and_then(|c| c.as_ref());
            match collation.map(|s| s.to_uppercase()).as_deref() {
                Some("NOCASE") => {
                    // For NOCASE collation, normalize text to uppercase for comparison
                    match val {
                        SqlValue::Varchar(s) => SqlValue::Varchar(ArcStr::from(s.to_uppercase())),
                        SqlValue::Character(s) => {
                            SqlValue::Character(ArcStr::from(s.to_uppercase()))
                        }
                        _ => val.clone(),
                    }
                }
                Some("RTRIM") => {
                    // For RTRIM collation, trim trailing spaces
                    match val {
                        SqlValue::Varchar(s) => {
                            SqlValue::Varchar(ArcStr::from(s.trim_end().to_string()))
                        }
                        SqlValue::Character(s) => {
                            SqlValue::Character(ArcStr::from(s.trim_end().to_string()))
                        }
                        _ => val.clone(),
                    }
                }
                _ => val.clone(), // BINARY or default - no transformation
            }
        })
        .collect()
}

/// Apply a set operation (UNION, INTERSECT, EXCEPT) to two result sets
///
/// The `collations` parameter specifies the collation for each column (if any).
/// Collations are used for value comparison in set operations - for example,
/// NOCASE collation makes 'ABC' equivalent to 'abc'.
pub(super) fn apply_set_operation(
    left: Vec<vibesql_storage::Row>,
    right: Vec<vibesql_storage::Row>,
    set_op: &vibesql_ast::SetOperation,
    collations: &[Option<String>],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Validate that both result sets have the same number of columns
    if !left.is_empty() && !right.is_empty() {
        let left_cols = left[0].values.len();
        let right_cols = right[0].values.len();
        if left_cols != right_cols {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: left_cols,
                actual: right_cols,
            });
        }
    }

    let has_collations = collations.iter().any(|c| c.is_some());

    match set_op.op {
        vibesql_ast::SetOperator::Union => {
            if set_op.all {
                // UNION ALL: combine all rows from both queries (preserves insertion order)
                let mut result = left;
                result.extend(right);
                Ok(result)
            } else {
                // UNION (DISTINCT): combine, remove duplicates, and sort
                // Use collation-aware deduplication when collations are specified
                let mut result = left;
                result.extend(right);

                if has_collations {
                    // Deduplicate using normalized keys
                    let mut seen = HashSet::new();
                    result.retain(|row| {
                        let key = normalize_row_for_comparison(&row.values, collations);
                        seen.insert(key)
                    });
                } else {
                    result = apply_distinct(result);
                }

                result.sort_by(|a, b| a.values.cmp(&b.values));
                Ok(result)
            }
        }

        vibesql_ast::SetOperator::Intersect => {
            if set_op.all {
                // INTERSECT ALL: return rows that appear in both (with multiplicity)
                // Count occurrences in right side (using normalized keys)
                let mut right_counts = HashMap::new();
                for row in &right {
                    let key = if has_collations {
                        normalize_row_for_comparison(&row.values, collations)
                    } else {
                        row.values.to_vec()
                    };
                    *right_counts.entry(key).or_insert(0) += 1;
                }

                // For each left row, if it appears in right, include it and decrement count
                let mut result = Vec::new();
                for row in left {
                    let key = if has_collations {
                        normalize_row_for_comparison(&row.values, collations)
                    } else {
                        row.values.to_vec()
                    };
                    if let Some(count) = right_counts.get_mut(&key) {
                        if *count > 0 {
                            result.push(row);
                            *count -= 1;
                        }
                    }
                }
                Ok(result)
            } else {
                // INTERSECT (DISTINCT): return unique rows that appear in both
                let right_set: HashSet<_> = right
                    .iter()
                    .map(|row| {
                        if has_collations {
                            normalize_row_for_comparison(&row.values, collations)
                        } else {
                            row.values.to_vec()
                        }
                    })
                    .collect();

                let mut result = Vec::new();
                let mut seen = HashSet::new();
                for row in left {
                    let key = if has_collations {
                        normalize_row_for_comparison(&row.values, collations)
                    } else {
                        row.values.to_vec()
                    };
                    if right_set.contains(&key) && seen.insert(key) {
                        result.push(row);
                    }
                }
                Ok(result)
            }
        }

        vibesql_ast::SetOperator::Except => {
            if set_op.all {
                // EXCEPT ALL: return rows from left that don't appear in right (with multiplicity)
                // Count occurrences in right side (using normalized values for comparison)
                let mut right_counts = HashMap::new();
                for row in &right {
                    let key = if has_collations {
                        normalize_row_for_comparison(&row.values, collations)
                    } else {
                        row.values.to_vec()
                    };
                    *right_counts.entry(key).or_insert(0) += 1;
                }

                // For each left row, if it doesn't appear in right (or count exhausted), include it
                let mut result = Vec::new();
                for row in left {
                    let key = if has_collations {
                        normalize_row_for_comparison(&row.values, collations)
                    } else {
                        row.values.to_vec()
                    };
                    match right_counts.get_mut(&key) {
                        None => {
                            // Row not in right side, include it
                            result.push(row);
                        }
                        Some(count) if *count == 0 => {
                            // All instances from right side already used, include it
                            result.push(row);
                        }
                        Some(count) => {
                            // Row exists in right side, decrement count (exclude this instance)
                            *count -= 1;
                        }
                    }
                }
                Ok(result)
            } else {
                // EXCEPT (DISTINCT): return unique rows from left that don't appear in right
                // Use normalized values for comparison when collations are specified
                let right_set: HashSet<_> = right
                    .iter()
                    .map(|row| {
                        if has_collations {
                            normalize_row_for_comparison(&row.values, collations)
                        } else {
                            row.values.to_vec()
                        }
                    })
                    .collect();

                let mut result = Vec::new();
                let mut seen = HashSet::new();
                for row in left {
                    let key = if has_collations {
                        normalize_row_for_comparison(&row.values, collations)
                    } else {
                        row.values.to_vec()
                    };
                    if !right_set.contains(&key) && seen.insert(key) {
                        result.push(row);
                    }
                }
                Ok(result)
            }
        }
    }
}
