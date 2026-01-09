//! Predicate helper functions for join operations
//!
//! Contains utilities for extracting and analyzing predicates from expressions
//! and FROM clauses.

use std::collections::HashSet;

/// Extract table names from a FROM clause
pub(super) fn extract_table_names_from_from_clause(from: &vibesql_ast::FromClause) -> Vec<String> {
    let mut tables = Vec::new();
    collect_table_names(from, &mut tables);
    tables
}

fn collect_table_names(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            // Use alias if present, otherwise use table name
            tables.push(alias.clone().unwrap_or_else(|| name.clone()));
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            collect_table_names(left, tables);
            collect_table_names(right, tables);
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            // Subquery alias is required (String, not Option<String>)
            tables.push(alias.clone());
        }
        vibesql_ast::FromClause::Values { alias, .. } => {
            // VALUES alias is required (String, not Option<String>)
            tables.push(alias.clone());
        }
    }
}

/// Simple check if a predicate references only tables from a given set.
/// This is a fallback for when extract_referenced_tables_branch returns None.
pub(super) fn predicate_references_only_tables(
    expr: &vibesql_ast::Expression,
    table_set: &HashSet<String>,
) -> bool {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_some() =>
        {
            let t = col_id.table_canonical().unwrap();
            let t_lower = t.to_lowercase();
            table_set.contains(t) || table_set.contains(&t_lower)
        }
        vibesql_ast::Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            // Unqualified column - can't determine which table, assume it might be from nullable
            // side. Return true to indicate "might reference only nullable tables" so that the
            // caller (filter_out_nullable_side_predicates) will REMOVE this predicate from the
            // pushdown set and evaluate it post-join.
            //
            // Bug fix for #4918: Previously returned false, which caused predicates like
            // `x IS NULL` to be pushed down to views in NATURAL FULL JOIN, incorrectly
            // filtering out all view rows before the join.
            true
        }
        vibesql_ast::Expression::ColumnRef(_) => {
            // Schema-qualified or other cases, be conservative
            false
        }
        vibesql_ast::Expression::IsNull { expr: inner, .. } => {
            predicate_references_only_tables(inner, table_set)
        }
        vibesql_ast::Expression::UnaryOp { expr: inner, .. } => {
            predicate_references_only_tables(inner, table_set)
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            // For binary ops, check if BOTH sides reference only the table set
            let left_only = predicate_references_only_tables(left, table_set);
            let right_only = predicate_references_only_tables(right, table_set);
            // If either side references other tables, this is not "only" nullable-side
            left_only && right_only
        }
        vibesql_ast::Expression::Literal(_) => {
            // Literals don't reference any table
            true
        }
        // For other expression types, be conservative
        _ => false,
    }
}
