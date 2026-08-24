//! Set operations (UNION, INTERSECT, EXCEPT) for SELECT queries

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crate::errors::ExecutorError;

/// Canonicalize a single SqlValue for set-operation comparison.
///
/// SQLite treats UNION/INTERSECT/EXCEPT as following storage-class affinity:
/// values with the same numeric magnitude compare equal regardless of which
/// concrete numeric variant (Integer / Real / Numeric / Float / Double) they
/// hold. Without canonicalization, `Real(30.0)` and `Numeric(30.0)` would
/// compare via type-tag fallback and produce non-deterministic ordering — and
/// would also fail to deduplicate against each other.
///
/// Canonicalization rules:
/// - All exact integer types collapse to `Bigint`.
/// - All inexact numeric types (`Real`, `Float`, `Double`, `Numeric`) collapse to `Numeric(f64)`.
///   If the value is a finite whole number, it further collapses to `Bigint` so e.g. `Real(30.0)`
///   matches `Integer(30)`.
/// - All other values are returned unchanged.
fn canonicalize_numeric(val: &vibesql_types::SqlValue) -> vibesql_types::SqlValue {
    use vibesql_types::SqlValue;

    fn float_to_canonical(f: f64) -> SqlValue {
        if f.is_finite() && f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
            SqlValue::Bigint(f as i64)
        } else {
            SqlValue::Numeric(f)
        }
    }

    match val {
        SqlValue::Integer(n) => SqlValue::Bigint(*n),
        SqlValue::Smallint(n) => SqlValue::Bigint(*n as i64),
        SqlValue::Bigint(n) => SqlValue::Bigint(*n),
        SqlValue::Unsigned(n) => SqlValue::Bigint(*n as i64),
        SqlValue::Real(f) => float_to_canonical(*f as f64),
        SqlValue::Float(f) => float_to_canonical(*f as f64),
        SqlValue::Double(f) => float_to_canonical(*f),
        SqlValue::Numeric(f) => float_to_canonical(*f),
        _ => val.clone(),
    }
}

/// Normalize a row's values for comparison based on column collations.
/// This applies collation transformations (e.g., case folding for NOCASE)
/// AND canonicalizes numeric storage classes so that cross-type numerics
/// (e.g., Real(30.0) vs Numeric(30.0)) compare equal during dedup and sort.
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
                        _ => canonicalize_numeric(val),
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
                        _ => canonicalize_numeric(val),
                    }
                }
                // BINARY or default: still canonicalize numerics so cross-type
                // numerics compare equal (matches SQLite storage-class affinity).
                _ => canonicalize_numeric(val),
            }
        })
        .collect()
}

/// Compare two normalized rows lexicographically.
///
/// Used to sort UNION results after dedup. Falls back to type-tag ordering for
/// truly incomparable values via `SqlValue::cmp`.
fn compare_normalized_rows(
    a: &[vibesql_types::SqlValue],
    b: &[vibesql_types::SqlValue],
) -> Ordering {
    let len = a.len().min(b.len());
    for i in 0..len {
        let ord = a[i].cmp(&b[i]);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
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
            // Determine the operator string for the error message
            let operator = match (&set_op.op, set_op.all) {
                (vibesql_ast::SetOperator::Union, true) => "UNION ALL",
                (vibesql_ast::SetOperator::Union, false) => "UNION",
                (vibesql_ast::SetOperator::Intersect, true) => "INTERSECT ALL",
                (vibesql_ast::SetOperator::Intersect, false) => "INTERSECT",
                (vibesql_ast::SetOperator::Except, true) => "EXCEPT ALL",
                (vibesql_ast::SetOperator::Except, false) => "EXCEPT",
            };
            return Err(ExecutorError::SetOperationColumnMismatch {
                operator: operator.to_string(),
            });
        }
    }

    match set_op.op {
        vibesql_ast::SetOperator::Union => {
            if set_op.all {
                // UNION ALL: combine all rows from both queries (preserves insertion order)
                let mut result = left;
                result.extend(right);
                Ok(result)
            } else {
                // UNION (DISTINCT): combine, remove duplicates, and sort.
                // Use normalized keys (collation + numeric canonicalization) for both
                // dedup and sorting so that cross-type numerics (e.g. Real(30.0) and
                // Numeric(30.0) produced by sum() OVER()) are treated as equal and
                // sort by their numeric magnitude.
                //
                // Dedup precedence: last-occurrence wins. SQLite's UNION uses an
                // ephemeral B-tree with `OP_IdxInsert` semantics that *overwrite*
                // an existing entry whose key compares equal — even when the new
                // row's storage class differs (e.g. `INTEGER 0` vs `REAL 0.0`).
                // Concretely, `SELECT 0 UNION SELECT 0.0` returns `0.0` (REAL),
                // not `0` (INTEGER). To match this, walk the combined rows in
                // order, replacing any previously-seen row with the same
                // normalized key. (See window9.test 8.4 / issue #5105.)
                let mut combined = left;
                combined.extend(right);

                let mut index_by_key: HashMap<Vec<vibesql_types::SqlValue>, usize> = HashMap::new();
                let mut result: Vec<vibesql_storage::Row> = Vec::with_capacity(combined.len());
                for row in combined {
                    let key = normalize_row_for_comparison(&row.values, collations);
                    if let Some(&idx) = index_by_key.get(&key) {
                        result[idx] = row;
                    } else {
                        index_by_key.insert(key, result.len());
                        result.push(row);
                    }
                }

                result.sort_by(|a, b| {
                    let ka = normalize_row_for_comparison(&a.values, collations);
                    let kb = normalize_row_for_comparison(&b.values, collations);
                    compare_normalized_rows(&ka, &kb)
                });
                Ok(result)
            }
        }

        vibesql_ast::SetOperator::Intersect => {
            if set_op.all {
                // INTERSECT ALL: return rows that appear in both (with multiplicity)
                // Count occurrences in right side (using normalized keys)
                let mut right_counts = HashMap::new();
                for row in &right {
                    let key = normalize_row_for_comparison(&row.values, collations);
                    *right_counts.entry(key).or_insert(0) += 1;
                }

                // For each left row, if it appears in right, include it and decrement count
                let mut result = Vec::new();
                for row in left {
                    let key = normalize_row_for_comparison(&row.values, collations);
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
                    .map(|row| normalize_row_for_comparison(&row.values, collations))
                    .collect();

                let mut result = Vec::new();
                let mut seen = HashSet::new();
                for row in left {
                    let key = normalize_row_for_comparison(&row.values, collations);
                    if right_set.contains(&key) && seen.insert(key) {
                        result.push(row);
                    }
                }

                // Sort the deduplicated result. SQLite's INTERSECT uses an
                // ephemeral B-tree for deduplication, which naturally yields
                // sorted output; mirror that here (same sort as UNION DISTINCT)
                // so we don't leak left-branch scan order.
                result.sort_by(|a, b| {
                    let ka = normalize_row_for_comparison(&a.values, collations);
                    let kb = normalize_row_for_comparison(&b.values, collations);
                    compare_normalized_rows(&ka, &kb)
                });
                Ok(result)
            }
        }

        vibesql_ast::SetOperator::Except => {
            if set_op.all {
                // EXCEPT ALL: return rows from left that don't appear in right (with multiplicity)
                // Count occurrences in right side (using normalized values for comparison)
                let mut right_counts = HashMap::new();
                for row in &right {
                    let key = normalize_row_for_comparison(&row.values, collations);
                    *right_counts.entry(key).or_insert(0) += 1;
                }

                // For each left row, if it doesn't appear in right (or count exhausted), include it
                let mut result = Vec::new();
                for row in left {
                    let key = normalize_row_for_comparison(&row.values, collations);
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
                // Use normalized values for comparison
                let right_set: HashSet<_> = right
                    .iter()
                    .map(|row| normalize_row_for_comparison(&row.values, collations))
                    .collect();

                let mut result = Vec::new();
                let mut seen = HashSet::new();
                for row in left {
                    let key = normalize_row_for_comparison(&row.values, collations);
                    if !right_set.contains(&key) && seen.insert(key) {
                        result.push(row);
                    }
                }

                // Sort the deduplicated result. SQLite's EXCEPT uses an
                // ephemeral B-tree for deduplication, which naturally yields
                // sorted output; mirror that here (same sort as UNION DISTINCT)
                // so we don't leak left-branch scan order.
                result.sort_by(|a, b| {
                    let ka = normalize_row_for_comparison(&a.values, collations);
                    let kb = normalize_row_for_comparison(&b.values, collations);
                    compare_normalized_rows(&ka, &kb)
                });
                Ok(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{SelectStmt, SetOperation, SetOperator};
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    use super::apply_set_operation;

    /// Minimal placeholder SELECT for the `right` field of `SetOperation`.
    /// `apply_set_operation` only inspects `set_op.op` and `set_op.all`; the
    /// `right` statement is never read (rows are pre-evaluated), so an empty
    /// statement is sufficient.
    fn empty_select() -> SelectStmt {
        SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: Vec::new(),
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    fn set_op(op: SetOperator, all: bool) -> SetOperation {
        SetOperation { op, all, right: Box::new(empty_select()) }
    }

    fn int_row(n: i64) -> Row {
        Row::new(vec![SqlValue::Integer(n)])
    }

    fn first_cols(rows: &[Row]) -> Vec<i64> {
        rows.iter()
            .map(|r| match &r.values[0] {
                SqlValue::Integer(n) => *n,
                SqlValue::Bigint(n) => *n,
                other => panic!("unexpected value: {other:?}"),
            })
            .collect()
    }

    /// INTERSECT DISTINCT must return rows in SQL-canonical sorted order,
    /// not left-branch scan order. Regression for issue #5720 (Bug 1).
    #[test]
    fn intersect_distinct_returns_sorted_rows() {
        // Left scan order is 3, 9, 6 — deliberately unsorted.
        let left = vec![int_row(3), int_row(9), int_row(6)];
        let right = vec![int_row(6), int_row(9), int_row(3)];
        let collations = vec![None];

        let result =
            apply_set_operation(left, right, &set_op(SetOperator::Intersect, false), &collations)
                .expect("intersect distinct");

        assert_eq!(first_cols(&result), vec![3, 6, 9]);
    }

    /// EXCEPT DISTINCT must return rows in SQL-canonical sorted order,
    /// not left-branch scan order. Regression for issue #5720 (Bug 1).
    #[test]
    fn except_distinct_returns_sorted_rows() {
        // Left scan order 1, 5, 7, 2, 4, 8, 10 with 3,6,9 removed by right.
        let left = vec![
            int_row(1),
            int_row(5),
            int_row(7),
            int_row(2),
            int_row(4),
            int_row(8),
            int_row(10),
            int_row(3),
            int_row(6),
            int_row(9),
        ];
        let right = vec![int_row(3), int_row(6), int_row(9)];
        let collations = vec![None];

        let result =
            apply_set_operation(left, right, &set_op(SetOperator::Except, false), &collations)
                .expect("except distinct");

        assert_eq!(first_cols(&result), vec![1, 2, 4, 5, 7, 8, 10]);
    }

    /// EXCEPT DISTINCT where nothing is removed still sorts the left side.
    #[test]
    fn except_distinct_no_matches_still_sorted() {
        let left = vec![int_row(5), int_row(1), int_row(3)];
        let right = vec![int_row(99)];
        let collations = vec![None];

        let result =
            apply_set_operation(left, right, &set_op(SetOperator::Except, false), &collations)
                .expect("except distinct");

        assert_eq!(first_cols(&result), vec![1, 3, 5]);
    }

    /// INTERSECT DISTINCT with an empty left branch yields no rows.
    #[test]
    fn intersect_distinct_empty_left() {
        let left: Vec<Row> = Vec::new();
        let right = vec![int_row(1), int_row(2)];
        let collations = vec![None];

        let result =
            apply_set_operation(left, right, &set_op(SetOperator::Intersect, false), &collations)
                .expect("intersect distinct");

        assert!(result.is_empty());
    }
}
