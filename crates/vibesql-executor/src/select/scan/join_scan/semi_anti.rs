//! SEMI/ANTI join optimization logic
//!
//! This module contains the cost-based decision logic for choosing between
//! Index Nested Loop (INL) and hash join for SEMI/ANTI joins.

use std::collections::HashSet;

use crate::optimizer::where_pushdown::flatten_conjuncts;

use super::{INL_BASE_THRESHOLD, INL_MAX_THRESHOLD, INL_SIZE_RATIO_THRESHOLD};

/// Determine whether to use Index Nested Loop (INL) for a semi-join.
///
/// This implements a cost-based decision between INL and hash semi-join:
///
/// 1. **Always use INL** when left_rows < INL_BASE_THRESHOLD (1000)
///    - Small left side means few index lookups, definitely faster than hash
///
/// 2. **Use INL when right table is much larger** (right_rows / left_rows > ratio threshold)
///    - Hash join scans the entire right table to build hash table
///    - INL does left_rows index lookups, each returning ~(right_rows/distinct_keys) rows
///    - For semi-join, we stop at first match, so avg cost is even lower
///
/// 3. **Cap at INL_MAX_THRESHOLD** to prevent excessive random I/O
///
/// Example (TPC-H Q4):
/// - left_rows = 5,406 (orders after date filter)
/// - right_rows = 600,000 (lineitem)
/// - ratio = 600,000 / 5,406 ≈ 111 >> 10
/// - Decision: Use INL (index lookups) instead of hash (full lineitem scan)
pub(super) fn should_use_inl_for_semi_join(
    left_row_count: usize,
    right_from: &vibesql_ast::FromClause,
    _condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> bool {
    let debug = std::env::var("INL_DECISION_DEBUG").is_ok();

    // Rule 1: Always use INL for small left sides
    if left_row_count < INL_BASE_THRESHOLD {
        if debug {
            eprintln!(
                "[INL_DECISION] left_rows={} < base_threshold={}, using INL",
                left_row_count, INL_BASE_THRESHOLD
            );
        }
        return true;
    }

    // Rule 3: Never use INL for very large left sides (too much random I/O)
    if left_row_count > INL_MAX_THRESHOLD {
        if debug {
            eprintln!(
                "[INL_DECISION] left_rows={} > max_threshold={}, using hash join",
                left_row_count, INL_MAX_THRESHOLD
            );
        }
        return false;
    }

    // Rule 2: Use cost-based decision for medium-sized left sides
    // Get right table cardinality
    let right_row_count = match right_from {
        vibesql_ast::FromClause::Table { name, .. } => {
            database.get_table(name).map(|t| t.row_count()).unwrap_or(0)
        }
        _ => {
            // Complex right side (subquery/join), can't estimate, use hash join
            if debug {
                eprintln!("[INL_DECISION] Complex right side, using hash join");
            }
            return false;
        }
    };

    // Calculate size ratio
    let ratio = if left_row_count > 0 { right_row_count / left_row_count } else { 0 };

    // Decision: Use INL if right table is significantly larger than left
    let use_inl = ratio >= INL_SIZE_RATIO_THRESHOLD;

    if debug {
        eprintln!(
            "[INL_DECISION] left_rows={}, right_rows={}, ratio={}, threshold={}, decision={}",
            left_row_count,
            right_row_count,
            ratio,
            INL_SIZE_RATIO_THRESHOLD,
            if use_inl { "INL" } else { "hash" }
        );
    }

    use_inl
}

/// Parsed equi-join condition
pub(super) struct EquiJoinInfo {
    pub(super) left_col: String,
    pub(super) right_col: String,
}

/// Parse a semi-join condition to extract equi-join and right-side filters.
pub(super) fn parse_semi_join_condition(
    cond: &vibesql_ast::Expression,
    left_result: &super::super::FromResult,
    right_table_name: &str,
) -> Option<(EquiJoinInfo, Option<vibesql_ast::Expression>)> {
    let conjuncts = flatten_conjuncts(cond);

    let left_tables: HashSet<String> =
        left_result.schema.table_schemas.keys().map(|s| s.canonical().to_string()).collect();

    let right_table_lower = right_table_name.to_lowercase();

    let mut equi_join: Option<EquiJoinInfo> = None;
    let mut right_only_preds: Vec<vibesql_ast::Expression> = Vec::new();

    for pred in conjuncts {
        // Check if this is an equi-join predicate (col1 = col2)
        if let vibesql_ast::Expression::BinaryOp {
            left,
            op: vibesql_ast::BinaryOperator::Equal,
            right,
        } = &pred
        {
            if let (
                vibesql_ast::Expression::ColumnRef(left_col_id),
                vibesql_ast::Expression::ColumnRef(right_col_id),
            ) = (left.as_ref(), right.as_ref())
            {
                if left_col_id.schema_canonical().is_some()
                    || right_col_id.schema_canonical().is_some()
                {
                    // Schema-qualified, skip this predicate
                    right_only_preds.push(pred);
                    continue;
                }
                let left_tbl = left_col_id.table_canonical();
                let right_tbl = right_col_id.table_canonical();
                let left_col = left_col_id.column_canonical();
                let right_col = right_col_id.column_canonical();

                // Determine which column is from left and which from right
                let left_tbl_lower = left_tbl.map(|s| s.to_lowercase());
                let right_tbl_lower = right_tbl.map(|s| s.to_lowercase());

                let left_col_lower = left_col.to_lowercase();

                // Check if left_col is from left tables and right_col is from right table
                // When table qualifier is None, check if column exists in any left table's schema
                let left_is_left =
                    left_tbl_lower.as_ref().map(|t| left_tables.contains(t)).unwrap_or(false)
                        || left_result.schema.table_schemas.values().any(|(_, schema)| {
                            schema.columns.iter().any(|c| c.name.to_lowercase() == left_col_lower)
                        });
                let right_is_right =
                    right_tbl_lower.as_ref().map(|t| t == &right_table_lower).unwrap_or(true);

                if left_is_left && right_is_right && equi_join.is_none() {
                    equi_join = Some(EquiJoinInfo {
                        left_col: left_col.to_string(),
                        right_col: right_col.to_string(),
                    });
                    continue;
                }

                // Check the reverse: right_col from left, left_col from right
                let right_is_left =
                    right_tbl_lower.as_ref().map(|t| left_tables.contains(t)).unwrap_or(false);
                let left_is_right =
                    left_tbl_lower.as_ref().map(|t| t == &right_table_lower).unwrap_or(true);

                if right_is_left && left_is_right && equi_join.is_none() {
                    equi_join = Some(EquiJoinInfo {
                        left_col: right_col.to_string(),
                        right_col: left_col.to_string(),
                    });
                    continue;
                }
            }
        }

        // Check if this predicate references only the right table
        // (We'll add it to right_only_preds)
        right_only_preds.push(pred);
    }

    equi_join.map(|ej| {
        let filter = crate::optimizer::combine_with_and(right_only_preds);
        (ej, filter)
    })
}
