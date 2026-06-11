//! Partition sorting for window functions
//!
//! Sorts rows within partitions according to ORDER BY specifications.

use std::cmp::Ordering;

use vibesql_ast::{Expression, NullsOrder, OrderByItem, OrderDirection};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::partitioning::Partition;

/// Sort a partition by ORDER BY clauses
///
/// Sorts rows within a partition according to ORDER BY specification.
/// Also keeps original_indices in sync with the sorted rows.
///
/// The `eval_fn` closure is used to evaluate ORDER BY expressions against rows.
/// This supports complex expressions (BinaryOp, Function, etc.), not just literals
/// and column references.
pub fn sort_partition<F>(partition: &mut Partition, order_by: &Option<Vec<OrderByItem>>, eval_fn: F)
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    sort_partition_with_collations(partition, order_by, &[], eval_fn);
}

/// Sort a partition by ORDER BY clauses, respecting column collations
///
/// `collations` maps each ORDER BY item index to its resolved collation (e.g., "NOCASE").
/// If collations is shorter than order_items, missing entries default to binary collation.
pub fn sort_partition_with_collations<F>(
    partition: &mut Partition,
    order_by: &Option<Vec<OrderByItem>>,
    collations: &[Option<String>],
    eval_fn: F,
) where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    // If no ORDER BY, keep original order
    let Some(order_items) = order_by else {
        return;
    };

    if order_items.is_empty() {
        return;
    }

    // Create indices for sorting without borrowing partition data
    let mut indices: Vec<usize> = (0..partition.rows.len()).collect();

    // Sort indices by evaluating order expressions on the rows
    let rows = &partition.rows; // Borrow for comparison only
    indices.sort_by(|&a, &b| {
        for (i, order_item) in order_items.iter().enumerate() {
            let val_a = eval_fn(&order_item.expr, &rows[a]).unwrap_or(SqlValue::Null);
            let val_b = eval_fn(&order_item.expr, &rows[b]).unwrap_or(SqlValue::Null);

            let collation = collations.get(i).and_then(|c| c.as_deref());

            // Explicit NULLS FIRST/LAST overrides the default NULL placement.
            // The placement is absolute (not affected by ASC/DESC), so it must
            // bypass the direction reversal below. The default behavior
            // (NULL-sorts-first + direction reversal) matches SQLite's
            // defaults: NULLS FIRST for ASC, NULLS LAST for DESC.
            let a_null = matches!(val_a, SqlValue::Null);
            let b_null = matches!(val_b, SqlValue::Null);
            let cmp = if a_null != b_null {
                if let Some(nulls_order) = order_item.nulls_order {
                    match (nulls_order, a_null) {
                        (NullsOrder::First, true) | (NullsOrder::Last, false) => Ordering::Less,
                        (NullsOrder::First, false) | (NullsOrder::Last, true) => Ordering::Greater,
                    }
                } else {
                    let cmp = compare_values(&val_a, &val_b);
                    match order_item.direction {
                        OrderDirection::Asc => cmp,
                        OrderDirection::Desc => cmp.reverse(),
                    }
                }
            } else {
                let cmp = compare_values_with_collation(&val_a, &val_b, collation);
                match order_item.direction {
                    OrderDirection::Asc => cmp,
                    OrderDirection::Desc => cmp.reverse(),
                }
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });

    // Now reorder both rows and original_indices using the sorted indices
    let old_rows = std::mem::take(&mut partition.rows);
    let old_indices = std::mem::take(&mut partition.original_indices);

    partition.rows = indices.iter().map(|&i| old_rows[i].clone()).collect();
    partition.original_indices = indices.iter().map(|&i| old_indices[i]).collect();
}

/// Compare two SQL values with optional collation
pub fn compare_values_with_collation(
    a: &SqlValue,
    b: &SqlValue,
    collation: Option<&str>,
) -> Ordering {
    if let Some(coll) = collation {
        if coll.eq_ignore_ascii_case("nocase") {
            let a = apply_nocase(a);
            let b = apply_nocase(b);
            return compare_values(&a, &b);
        }
    }
    compare_values(a, b)
}

/// Apply NOCASE collation by uppercasing string values
fn apply_nocase(val: &SqlValue) -> SqlValue {
    match val {
        SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase())),
        SqlValue::Character(s) => SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase())),
        other => other.clone(),
    }
}

/// Compare two SQL values for ordering
pub fn compare_values(a: &SqlValue, b: &SqlValue) -> Ordering {
    match (a, b) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => Ordering::Less, // NULL sorts first
        (_, SqlValue::Null) => Ordering::Greater,

        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Real(a), SqlValue::Real(b)) => {
            // Handle NaN carefully
            if a.is_nan() && b.is_nan() {
                Ordering::Equal
            } else if a.is_nan() {
                Ordering::Greater
            } else if b.is_nan() {
                Ordering::Less
            } else {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
        }
        (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
        (SqlValue::Character(a), SqlValue::Character(b)) => a.cmp(b),
        (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),

        // Type coercion for mixed integer/real (Real is now f64)
        (SqlValue::Integer(a), SqlValue::Real(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Real(a), SqlValue::Integer(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }

        // Numeric type handling (f64)
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => {
            if a.is_nan() && b.is_nan() {
                Ordering::Equal
            } else if a.is_nan() {
                Ordering::Greater
            } else if b.is_nan() {
                Ordering::Less
            } else {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
        }
        // Numeric coercion with Integer
        (SqlValue::Numeric(a), SqlValue::Integer(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Integer(a), SqlValue::Numeric(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        // Numeric coercion with Real
        (SqlValue::Numeric(a), SqlValue::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Real(a), SqlValue::Numeric(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),

        // Other type combinations: compare as strings
        _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
    }
}
