//! Equijoin detection for table elimination
//!
//! Functions for identifying tables that participate in equijoin conditions,
//! which should not be eliminated.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{BinaryOperator, Expression, FromClause};

use super::select_analysis::{
    collect_unqualified_columns_from_expr_single, extract_tables_from_expr,
    has_unqualified_column_ref,
};

/// Extract tables that participate in equijoin conditions from JOIN ON clauses (#3572)
///
/// Recursively walks the FROM clause tree and extracts tables referenced in ON conditions.
/// This ensures tables joined via explicit ON conditions are not incorrectly eliminated.
pub(super) fn extract_equijoin_tables_from_joins(
    from: &FromClause,
    table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) -> HashSet<String> {
    let mut tables = HashSet::new();
    extract_join_on_tables(from, &mut tables, table_names, table_prefixes);
    tables
}

fn extract_join_on_tables(
    from: &FromClause,
    tables: &mut HashSet<String>,
    table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) {
    match from {
        FromClause::Table { .. } => {
            // Leaf node - no ON conditions
        }
        FromClause::Join { left, right, condition, .. } => {
            // Recursively process left and right subtrees
            extract_join_on_tables(left, tables, table_names, table_prefixes);
            extract_join_on_tables(right, tables, table_names, table_prefixes);

            // Extract tables from this join's ON condition
            if let Some(cond) = condition {
                let on_tables = extract_equijoin_tables(cond, table_names, table_prefixes);
                tables.extend(on_tables);
            }
        }
        FromClause::Subquery { .. } => {
            // Subqueries are opaque - don't examine their internals
        }
        FromClause::Values { .. } => {
            // VALUES clauses are opaque - don't examine their internals
        }
        FromClause::TableFunction { .. } => {
            // Table functions are opaque - don't examine their internals
        }
    }
}

/// Extract tables that participate in equijoin conditions
pub(super) fn extract_equijoin_tables(
    expr: &Expression,
    table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) -> HashSet<String> {
    let mut tables = HashSet::new();
    find_equijoin_tables(expr, &mut tables, table_names, table_prefixes);
    tables
}

fn find_equijoin_tables(
    expr: &Expression,
    tables: &mut HashSet<String>,
    _table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            find_equijoin_tables(left, tables, _table_names, table_prefixes);
            find_equijoin_tables(right, tables, _table_names, table_prefixes);
        }
        // Also recurse into OR branches to find equijoins
        // This is critical for queries like TPC-H Q19 where the join condition
        // (p_partkey = l_partkey) appears inside multiple OR branches
        Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
            find_equijoin_tables(left, tables, _table_names, table_prefixes);
            find_equijoin_tables(right, tables, _table_names, table_prefixes);
        }
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Check if this is a join between two tables
            let mut left_tables = HashSet::new();
            let mut right_tables = HashSet::new();
            extract_tables_from_expr(left, &mut left_tables);
            extract_tables_from_expr(right, &mut right_tables);

            // Check if either side has unqualified column references
            let left_has_unqualified = has_unqualified_column_ref(left);
            let right_has_unqualified = has_unqualified_column_ref(right);

            // For unqualified columns, try to determine their tables via prefix matching
            if left_has_unqualified && left_tables.is_empty() {
                let left_cols = collect_unqualified_columns_from_expr_single(left);
                for col in left_cols {
                    let col_lower = col.to_lowercase();
                    for (table, prefix) in table_prefixes {
                        if col_lower.starts_with(prefix) {
                            left_tables.insert(table.clone());
                        }
                    }
                }
            }
            if right_has_unqualified && right_tables.is_empty() {
                let right_cols = collect_unqualified_columns_from_expr_single(right);
                for col in right_cols {
                    let col_lower = col.to_lowercase();
                    for (table, prefix) in table_prefixes {
                        if col_lower.starts_with(prefix) {
                            right_tables.insert(table.clone());
                        }
                    }
                }
            }

            // It's an equijoin if both sides reference different tables
            // (via qualified refs OR prefix-matched unqualified refs)
            if !left_tables.is_empty()
                && !right_tables.is_empty()
                && left_tables.is_disjoint(&right_tables)
            {
                tables.extend(left_tables);
                tables.extend(right_tables);
            }
        }
        _ => {}
    }
}
