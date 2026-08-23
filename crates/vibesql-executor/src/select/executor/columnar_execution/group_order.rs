//! Terminal ORDER BY for columnar GROUP BY results (Issue #6009).
//!
//! Both the single-table and join columnar GROUP BY paths materialize their
//! grouped output as `Vec<Row>` in a fixed positional layout:
//!
//! ```text
//! [group_key_0, group_key_1, ..., agg_0, agg_1, ...]
//! ```
//!
//! After the positional SELECT-list validation performed by each path (each
//! non-aggregate SELECT item structurally equals the GROUP BY key at the same
//! position; all non-aggregates precede all aggregates), this layout is
//! *identical, column-for-column, to the SELECT output*. That means an ORDER BY
//! term that references an output column — a bare group key, a derived-key
//! expression (`a + b`), an aggregate (`SUM(x)`), a SELECT alias, or an ordinal
//! position — can be sorted purely positionally, reading the already-computed
//! value out of the result row.
//!
//! ## Why positional (not evaluate-against-schema)
//!
//! The prior join-path helper wrapped rows as `RowWithSortKeys` and called the
//! shared `apply_order_by`, which evaluates each ORDER BY expression against the
//! *base-table combined schema*. But grouped result rows are NOT in base-table
//! schema layout — a group key that came from column index 7 of the joined batch
//! now lives at output position 0. Re-evaluating `ORDER BY that_column` against
//! the schema reads index 7 of the (short) result row → wrong value or panic, and
//! an aggregate like `SUM(x)` cannot be evaluated against a materialized row at
//! all. This is the "mis-sorts projected/derived group keys" bug called out in
//! #6003. Resolving to an output position and reading the stored value avoids the
//! mismatch entirely and matches the row path exactly, because the stored values
//! ARE the row path's computed group keys / aggregates.
//!
//! ## Decline semantics
//!
//! If *any* ORDER BY term cannot be resolved to an output-column position (e.g.
//! `ORDER BY <expr not in the SELECT list>`), this returns `Ok(None)` and the
//! caller falls back to the row-oriented path, which handles arbitrary ORDER BY
//! expressions. We never emit a mis-sorted result.

use vibesql_ast::{Expression, OrderByItem, SelectItem};
use vibesql_storage::Row;

use crate::select::{
    grouping::expressions_equal,
    order::{apply_order_by_on_projected_output, extract_column_position, ColumnPositionResult},
};

/// Resolve every ORDER BY term to a 0-based output-column position in the
/// `[group_keys..., aggregates...]` grouped-result layout.
///
/// Returns `Some(indices)` only if *all* terms resolve; otherwise `None` (the
/// caller must decline to the row path). `select_col_count` is the number of
/// SELECT items (which equals the width of each grouped result row after the
/// positional validation each caller performs).
fn resolve_order_by_output_indices(
    order_by: &[OrderByItem],
    select_list: &[SelectItem],
    select_col_count: usize,
) -> Option<Vec<usize>> {
    let mut indices = Vec::with_capacity(order_by.len());
    for item in order_by {
        let idx = resolve_single_term(&item.expr, select_list, select_col_count)?;
        indices.push(idx);
    }
    Some(indices)
}

/// Resolve one ORDER BY expression to a 0-based output-column index.
fn resolve_single_term(
    expr: &Expression,
    select_list: &[SelectItem],
    select_col_count: usize,
) -> Option<usize> {
    // 1. Ordinal position: `ORDER BY 2`, `ORDER BY +2`.
    match extract_column_position(expr) {
        ColumnPositionResult::Position(pos) => {
            if pos >= 1 && (pos as usize) <= select_col_count {
                return Some((pos as usize) - 1);
            }
            // Out-of-range ordinal: decline (row path reports the proper error).
            return None;
        }
        // Negative ordinal is always invalid — decline to the row path so it
        // raises the canonical out-of-range error.
        ColumnPositionResult::Negative(_) => return None,
        ColumnPositionResult::NotAPosition => {}
    }

    // 2. SELECT alias match: `SELECT SUM(x) AS total ... ORDER BY total`.
    if let Expression::ColumnRef(col_id) = expr {
        // Only a bare, unqualified name can match a SELECT alias.
        if col_id.table_canonical().is_none() && col_id.schema_canonical().is_none() {
            let name = col_id.column_canonical();
            for (i, item) in select_list.iter().enumerate() {
                if let SelectItem::Expression { alias: Some(alias), .. } = item {
                    if alias.eq_ignore_ascii_case(name) {
                        return Some(i);
                    }
                }
            }
        }
    }

    // 3. Structural expression match against a SELECT item. This covers bare group keys (`ORDER BY
    //    a`), derived-key expressions (`ORDER BY a + b`), and aggregates (`ORDER BY SUM(x)`).
    //    `expressions_equal` is the same positional-matching helper used to validate the SELECT
    //    list against the GROUP BY keys, so a term that matches here addresses exactly the output
    //    column that holds its value.
    for (i, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr: select_expr, .. } = item {
            if expressions_equal(expr, select_expr) {
                return Some(i);
            }
        }
    }

    None
}

/// Apply a terminal ORDER BY to columnar GROUP BY result rows.
///
/// `rows` are the grouped results in `[group_keys..., aggregates...]` layout.
/// `select_list` is the query SELECT list (used to resolve ORDER BY terms to
/// output positions). On success returns `Some(sorted_rows)`. Returns `Ok(None)`
/// when any ORDER BY term cannot be resolved positionally, signalling the caller
/// to fall back to the row-oriented path.
pub(super) fn apply_group_by_order_by(
    rows: Vec<Row>,
    order_by: &[OrderByItem],
    select_list: &[SelectItem],
) -> Option<Vec<Row>> {
    if order_by.is_empty() {
        return Some(rows);
    }

    // Empty result: nothing to sort, but still confirm the ORDER BY is
    // resolvable so we don't silently accept an unsupported shape (which would
    // otherwise emit an empty result while a later, non-empty run would decline
    // — an inconsistency). `select_col_count` uses the SELECT list length, which
    // equals the grouped row width post-validation.
    let select_col_count = select_list.len();
    let output_indices = resolve_order_by_output_indices(order_by, select_list, select_col_count)?;

    if rows.is_empty() {
        return Some(rows);
    }

    Some(apply_order_by_on_projected_output(rows, order_by, &output_indices))
}
