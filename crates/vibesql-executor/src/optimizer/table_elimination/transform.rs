//! AST transformation for table elimination
//!
//! Functions for rebuilding FROM clauses, creating EXISTS checks,
//! and removing eliminated table predicates.

use std::collections::HashSet;

use vibesql_ast::{Expression, FromClause, JoinType, SelectItem, SelectStmt};

use super::predicate::{combine_predicates, flatten_and_chain};
use super::select_analysis::{collect_unqualified_columns_from_expr, extract_tables_from_expr};
use super::types::{EliminatedTable, TableInfo};

/// Flatten FROM clause into list of tables (simple tables only, not subqueries)
pub(super) fn flatten_from_clause(from: &FromClause, tables: &mut Vec<TableInfo>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            tables.push(TableInfo { name: name.clone(), alias: alias.clone() });
        }
        FromClause::Join { left, right, .. } => {
            flatten_from_clause(left, tables);
            flatten_from_clause(right, tables);
        }
        // Skip subqueries and VALUES - they can't be eliminated
        FromClause::Subquery { .. } => {}
        FromClause::Values { .. } => {}
    }
}

/// Rebuild FROM clause from kept tables
pub(super) fn rebuild_from_clause(tables: &[TableInfo]) -> Option<FromClause> {
    if tables.is_empty() {
        return None;
    }

    let mut iter = tables.iter();
    let first = iter.next()?;
    let mut result = FromClause::Table {
        name: first.name.clone(),
        alias: first.alias.clone(),
        column_aliases: None,
        quoted: false, // Synthesized, treat as unquoted
    };

    for table in iter {
        result = FromClause::Join {
            left: Box::new(result),
            right: Box::new(FromClause::Table {
                name: table.name.clone(),
                alias: table.alias.clone(),
                column_aliases: None,
                quoted: false, // Synthesized, treat as unquoted
            }),
            join_type: JoinType::Cross,
            condition: None,
            using_columns: None,
            natural: false,
        };
    }

    Some(result)
}

/// Build EXISTS checks for eliminated tables
pub(super) fn build_exists_checks(eliminated: &[EliminatedTable]) -> Vec<Expression> {
    eliminated
        .iter()
        .map(|table| {
            let subquery = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                    alias: None,
                    source_text: None,
                }],
                into_table: None,
                into_variables: None,
                from: Some(FromClause::Table {
                    name: table.name.clone(),
                    alias: table.alias.clone(),
                    column_aliases: None,
                    quoted: false, // Synthesized, treat as unquoted
                }),
                where_clause: table.filter.clone(),
                group_by: None,
                having: None,
                order_by: None,
                limit: Some(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
                offset: None,
                set_operation: None,
                values: None,
            };

            Expression::Exists { subquery: Box::new(subquery), negated: false }
        })
        .collect()
}

/// Add EXISTS checks to WHERE clause
pub(super) fn add_exists_to_where(
    where_clause: Option<&Expression>,
    exists_checks: Vec<Expression>,
) -> Option<Expression> {
    if exists_checks.is_empty() {
        return where_clause.cloned();
    }

    let combined_exists = combine_predicates(exists_checks);

    match where_clause {
        Some(existing) => Some(Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::And,
            left: Box::new(existing.clone()),
            right: Box::new(combined_exists),
        }),
        None => Some(combined_exists),
    }
}

/// Remove predicates that only reference eliminated tables
///
/// Uses both qualified table references and prefix matching for unqualified columns
pub(super) fn remove_eliminated_predicates(
    expr: &Expression,
    eliminated_tables: &HashSet<String>,
    eliminated_prefixes: &HashSet<String>,
) -> Option<Expression> {
    let predicates = flatten_and_chain(expr);
    let mut kept = Vec::new();

    for pred in predicates {
        // Get qualified table refs
        let mut qualified_refs = HashSet::new();
        extract_tables_from_expr(&pred, &mut qualified_refs);

        // Get unqualified columns
        let mut unqualified_cols = HashSet::new();
        collect_unqualified_columns_from_expr(&pred, &mut unqualified_cols);

        // Check if all qualified refs are to eliminated tables
        let qualified_all_eliminated = qualified_refs.is_empty()
            || qualified_refs.iter().all(|t| eliminated_tables.contains(t));

        // Check if all unqualified columns match eliminated table prefixes
        let unqualified_all_eliminated = unqualified_cols.is_empty()
            || unqualified_cols.iter().all(|col| {
                let col_lower = col.to_lowercase();
                eliminated_prefixes.iter().any(|prefix| col_lower.starts_with(prefix))
            });

        // A predicate should be removed if ALL its column references
        // (both qualified and unqualified) belong to eliminated tables
        let should_remove = qualified_all_eliminated
            && unqualified_all_eliminated
            && (!qualified_refs.is_empty() || !unqualified_cols.is_empty());

        if !should_remove {
            kept.push(pred);
        }
    }

    if kept.is_empty() {
        None
    } else {
        Some(combine_predicates(kept))
    }
}
