//! Value comparison logic for ORDER BY sorting.
//!
//! This module handles:
//! - SQL value comparison with proper NULL handling
//! - Collation support (NOCASE, etc.)
//! - Direction handling (ASC/DESC)

use std::{borrow::Cow, cmp::Ordering};

use super::grouping::compare_sql_values;
use super::SortKey;

/// Apply collation transformation to a SQL value for comparison.
///
/// For NOCASE collation, string values are uppercased for case-insensitive comparison.
/// Other values are returned unchanged.
fn apply_collation<'a>(
    val: &'a vibesql_types::SqlValue,
    collation: Option<&String>,
) -> Cow<'a, vibesql_types::SqlValue> {
    if let Some(coll) = collation {
        if coll.eq_ignore_ascii_case("nocase") {
            match val {
                vibesql_types::SqlValue::Varchar(s) => {
                    Cow::Owned(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                        s.to_uppercase(),
                    )))
                }
                vibesql_types::SqlValue::Character(s) => {
                    Cow::Owned(vibesql_types::SqlValue::Character(arcstr::ArcStr::from(
                        s.to_uppercase(),
                    )))
                }
                other => Cow::Borrowed(other),
            }
        } else {
            Cow::Borrowed(val)
        }
    } else {
        Cow::Borrowed(val)
    }
}

/// Compare two rows by their sort keys.
///
/// This function implements SQL ORDER BY semantics:
/// - NULL handling follows SQLite's semantics (NULL is smallest) with NULLS FIRST/LAST override
/// - Collation support for string comparisons
/// - ASC/DESC direction handling
///
/// Returns `Ordering::Less`, `Equal`, or `Greater` for use with sort functions.
pub(super) fn compare_rows_by_sort_keys(keys_a: &[SortKey], keys_b: &[SortKey]) -> Ordering {
    for ((val_a, dir, nulls_order, collation), (val_b, _, _, _)) in keys_a.iter().zip(keys_b.iter())
    {
        // Determine NULL ordering:
        // - If explicitly specified via NULLS FIRST/LAST, use that
        // - Default: SQLite treats NULL as smallest value, so:
        //   - ASC: NULL comes first (smallest first)
        //   - DESC: NULL comes last (smallest last)
        let nulls_first = match nulls_order {
            Some(vibesql_ast::NullsOrder::First) => true,
            Some(vibesql_ast::NullsOrder::Last) => false,
            None => matches!(dir, vibesql_ast::OrderDirection::Asc),
        };

        // Handle NULLs according to nulls_first setting
        let cmp = match (val_a.is_null(), val_b.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    return Ordering::Less; // NULL sorts before non-NULL
                } else {
                    return Ordering::Greater; // NULL sorts after non-NULL
                }
            }
            (false, true) => {
                if nulls_first {
                    return Ordering::Greater; // non-NULL sorts after NULL
                } else {
                    return Ordering::Less; // non-NULL sorts before NULL
                }
            }
            (false, false) => {
                // Apply collation transformation if needed
                let cmp_val_a = apply_collation(val_a, collation.as_ref());
                let cmp_val_b = apply_collation(val_b, collation.as_ref());

                // Compare non-NULL values, respecting direction
                match dir {
                    vibesql_ast::OrderDirection::Asc => {
                        compare_sql_values(&cmp_val_a, &cmp_val_b)
                    }
                    vibesql_ast::OrderDirection::Desc => {
                        compare_sql_values(&cmp_val_a, &cmp_val_b).reverse()
                    }
                }
            }
        };

        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}
