//! SELECT list analysis for table elimination
//!
//! Functions for extracting tables referenced in SELECT lists and
//! detecting global aggregates that prevent table elimination.

use std::collections::HashSet;

use vibesql_ast::{Expression, SelectItem};

/// Extract tables referenced in SELECT list (only qualified references)
///
/// Only extracts tables that are explicitly qualified in column references.
/// Unqualified columns are handled separately using prefix matching.
pub(super) fn extract_tables_from_select(
    select_list: &[SelectItem],
    table_names: &HashSet<String>,
) -> HashSet<String> {
    let mut tables = HashSet::new();

    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } => {
                // SELECT * references ALL tables
                tables.extend(table_names.iter().cloned());
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                tables.insert(qualifier.to_lowercase());
            }
            SelectItem::Expression { expr, .. } => {
                // Only extract qualified column references
                extract_tables_from_expr(expr, &mut tables);
            }
        }
    }

    tables
}

/// Check if the SELECT list contains "global" aggregate functions.
///
/// A global aggregate is one that:
/// 1. Is an aggregate function (COUNT, SUM, MIN, MAX, AVG, etc.)
/// 2. Does NOT reference any specific table columns (e.g., COUNT(*), MIN(42))
///
/// When such aggregates exist without GROUP BY, they operate over the entire
/// result set (including Cartesian products from cross joins). Eliminating tables
/// would incorrectly reduce the number of rows being aggregated.
pub(super) fn has_global_aggregates(
    select_list: &[SelectItem],
    table_names: &HashSet<String>,
) -> bool {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if expr_has_global_aggregate(expr, table_names) {
                return true;
            }
        }
    }
    false
}

/// Recursively check if an expression contains a global aggregate
fn expr_has_global_aggregate(expr: &Expression, _table_names: &HashSet<String>) -> bool {
    match expr {
        Expression::AggregateFunction { args, .. } => {
            // Check if this aggregate references any table columns
            let mut referenced_tables = HashSet::new();
            for arg in args {
                extract_tables_from_expr(arg, &mut referenced_tables);
            }
            // Also check for unqualified column references
            let mut has_column_ref = false;
            for arg in args {
                if has_any_column_ref(arg) {
                    has_column_ref = true;
                    break;
                }
            }
            // Global if no table refs AND no column refs (e.g., COUNT(*) or MIN(42))
            referenced_tables.is_empty() && !has_column_ref
        }
        Expression::BinaryOp { left, right, .. } => {
            expr_has_global_aggregate(left, _table_names)
                || expr_has_global_aggregate(right, _table_names)
        }
        Expression::UnaryOp { expr, .. } => expr_has_global_aggregate(expr, _table_names),
        Expression::Function { args, .. } => {
            args.iter().any(|a| expr_has_global_aggregate(a, _table_names))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|o| expr_has_global_aggregate(o, _table_names))
                || when_clauses.iter().any(|c| {
                    c.conditions.iter().any(|cond| expr_has_global_aggregate(cond, _table_names))
                        || expr_has_global_aggregate(&c.result, _table_names)
                })
                || else_result.as_ref().is_some_and(|e| expr_has_global_aggregate(e, _table_names))
        }
        _ => false,
    }
}

/// Check if expression contains any column reference (qualified or unqualified)
/// Note: The special "*" wildcard (used in COUNT(*)) is NOT considered a real column reference
pub(super) fn has_any_column_ref(expr: &Expression) -> bool {
    match expr {
        Expression::ColumnRef { column, .. } => {
            // The special "*" wildcard in COUNT(*) is not a real column reference
            column != "*"
        }
        Expression::BinaryOp { left, right, .. } => {
            has_any_column_ref(left) || has_any_column_ref(right)
        }
        Expression::UnaryOp { expr, .. } => has_any_column_ref(expr),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(has_any_column_ref)
        }
        Expression::Cast { expr, .. } => has_any_column_ref(expr),
        _ => false,
    }
}

/// Check if expression contains any unqualified column references
pub(super) fn has_unqualified_column_ref(expr: &Expression) -> bool {
    match expr {
        Expression::ColumnRef { table: None, .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            has_unqualified_column_ref(left) || has_unqualified_column_ref(right)
        }
        Expression::UnaryOp { expr, .. } => has_unqualified_column_ref(expr),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(has_unqualified_column_ref)
        }
        Expression::InList { expr, values, .. } => {
            has_unqualified_column_ref(expr) || values.iter().any(has_unqualified_column_ref)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|o| has_unqualified_column_ref(o))
                || when_clauses.iter().any(|c| {
                    c.conditions.iter().any(has_unqualified_column_ref)
                        || has_unqualified_column_ref(&c.result)
                })
                || else_result.as_ref().is_some_and(|e| has_unqualified_column_ref(e))
        }
        Expression::IsNull { expr, .. } => has_unqualified_column_ref(expr),
        Expression::Cast { expr, .. } => has_unqualified_column_ref(expr),
        _ => false,
    }
}

/// Extract tables referenced in an expression (only qualified column refs)
pub(super) fn extract_tables_from_expr(expr: &Expression, tables: &mut HashSet<String>) {
    match expr {
        Expression::ColumnRef { table: Some(t), .. } => {
            tables.insert(t.to_lowercase());
        }
        Expression::BinaryOp { left, right, .. } => {
            extract_tables_from_expr(left, tables);
            extract_tables_from_expr(right, tables);
        }
        Expression::UnaryOp { expr, .. } => {
            extract_tables_from_expr(expr, tables);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_tables_from_expr(arg, tables);
            }
        }
        Expression::InList { expr, values, .. } => {
            extract_tables_from_expr(expr, tables);
            for v in values {
                extract_tables_from_expr(v, tables);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_tables_from_expr(op, tables);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    extract_tables_from_expr(cond, tables);
                }
                extract_tables_from_expr(&clause.result, tables);
            }
            if let Some(else_res) = else_result {
                extract_tables_from_expr(else_res, tables);
            }
        }
        Expression::IsNull { expr, .. } => {
            extract_tables_from_expr(expr, tables);
        }
        Expression::Cast { expr, .. } => {
            extract_tables_from_expr(expr, tables);
        }
        _ => {}
    }
}

/// Collect all unqualified column names from SELECT list
pub(super) fn collect_unqualified_columns(select_list: &[SelectItem]) -> HashSet<String> {
    let mut columns = HashSet::new();
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            collect_unqualified_columns_from_expr(expr, &mut columns);
        }
    }
    columns
}

/// Collect unqualified columns from a single expression (helper)
pub(super) fn collect_unqualified_columns_from_expr_single(expr: &Expression) -> HashSet<String> {
    let mut cols = HashSet::new();
    collect_unqualified_columns_from_expr(expr, &mut cols);
    cols
}

pub(super) fn collect_unqualified_columns_from_expr(
    expr: &Expression,
    columns: &mut HashSet<String>,
) {
    match expr {
        Expression::ColumnRef { table: None, column } => {
            // Skip the special "*" wildcard (used in COUNT(*))
            if column != "*" {
                columns.insert(column.to_lowercase());
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_unqualified_columns_from_expr(left, columns);
            collect_unqualified_columns_from_expr(right, columns);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_unqualified_columns_from_expr(expr, columns);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                collect_unqualified_columns_from_expr(arg, columns);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_unqualified_columns_from_expr(op, columns);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    collect_unqualified_columns_from_expr(cond, columns);
                }
                collect_unqualified_columns_from_expr(&clause.result, columns);
            }
            if let Some(else_res) = else_result {
                collect_unqualified_columns_from_expr(else_res, columns);
            }
        }
        Expression::IsNull { expr, .. } | Expression::Cast { expr, .. } => {
            collect_unqualified_columns_from_expr(expr, columns);
        }
        _ => {}
    }
}
