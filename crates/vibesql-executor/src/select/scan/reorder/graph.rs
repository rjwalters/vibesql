//! Join graph construction and table reference analysis

#![allow(clippy::only_used_in_recursion)]

use std::collections::{HashMap, HashSet};
use vibesql_ast::{Expression, FromClause, JoinType};

/// Information about a table extracted from a FROM clause
#[derive(Debug, Clone)]
pub(super) struct TableRef {
    pub(super) name: String,
    pub(super) alias: Option<String>,
    #[allow(dead_code)]
    pub(super) is_cte: bool,
    pub(super) is_subquery: bool,
    pub(super) subquery: Option<Box<vibesql_ast::SelectStmt>>,
    /// SQL:1999 E051-09: Optional column aliases for derived tables
    pub(super) column_aliases: Option<Vec<String>>,
}

/// Join condition with its associated join type
#[derive(Debug, Clone)]
pub(super) struct JoinConditionWithType {
    pub(super) condition: Expression,
    pub(super) join_type: JoinType,
}

/// Flatten a nested join tree into a list of table references
pub(super) fn flatten_join_tree(from: &FromClause, tables: &mut Vec<TableRef>) {
    match from {
        FromClause::Table { name, alias, column_aliases } => {
            tables.push(TableRef {
                name: name.clone(),
                alias: alias.clone(),
                is_cte: false,
                is_subquery: false,
                subquery: None,
                column_aliases: column_aliases.clone(),
            });
        }
        FromClause::Subquery { query, alias, column_aliases } => {
            tables.push(TableRef {
                name: alias.clone(),
                alias: Some(alias.clone()),
                is_cte: false,
                is_subquery: true,
                subquery: Some(query.clone()),
                column_aliases: column_aliases.clone(),
            });
        }
        FromClause::Join { left, right, .. } => {
            flatten_join_tree(left, tables);
            flatten_join_tree(right, tables);
        }
    }
}

/// Extract all join conditions and WHERE predicates from a FROM clause
pub(super) fn extract_all_conditions(from: &FromClause, conditions: &mut Vec<Expression>) {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => {
            // No conditions in simple table refs
        }
        FromClause::Join { left, right, condition, .. } => {
            // Add this join's condition
            if let Some(cond) = condition {
                conditions.push(cond.clone());
            }
            // Recurse into nested joins
            extract_all_conditions(left, conditions);
            extract_all_conditions(right, conditions);
        }
    }
}

/// Extract all join conditions with their associated join types from a FROM clause
pub(super) fn extract_conditions_with_types(
    from: &FromClause,
    conditions: &mut Vec<JoinConditionWithType>,
) {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => {
            // No conditions in simple table refs
        }
        FromClause::Join { left, right, join_type, condition, .. } => {
            // Add this join's condition with its type
            if let Some(cond) = condition {
                conditions.push(JoinConditionWithType {
                    condition: cond.clone(),
                    join_type: join_type.clone(),
                });
            }
            // Recurse into nested joins
            extract_conditions_with_types(left, conditions);
            extract_conditions_with_types(right, conditions);
        }
    }
}

/// Extract all table names referenced in an expression using schema-based column resolution
///
/// This method uses actual database schema to resolve unqualified columns.
/// No heuristic fallbacks are used - unresolved columns are simply skipped.
///
/// # Parameters
/// - `expr`: The expression to analyze
/// - `output`: HashSet to populate with referenced table names
/// - `available_tables`: Set of FROM clause tables
/// - `column_to_table`: Schema-based column-to-table mapping
pub(super) fn extract_referenced_tables_with_schema(
    expr: &Expression,
    output: &mut HashSet<String>,
    available_tables: &HashSet<String>,
    column_to_table: &HashMap<String, String>,
) {
    match expr {
        Expression::ColumnRef { table: Some(table), .. } => {
            output.insert(table.to_lowercase());
        }
        Expression::ColumnRef { table: None, column } => {
            // Use schema-based lookup
            if let Some(table) = super::utils::resolve_column_with_fallback(column, column_to_table)
            {
                output.insert(table.to_lowercase());
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            extract_referenced_tables_with_schema(left, output, available_tables, column_to_table);
            extract_referenced_tables_with_schema(right, output, available_tables, column_to_table);
        }
        Expression::UnaryOp { expr, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_referenced_tables_with_schema(
                    arg,
                    output,
                    available_tables,
                    column_to_table,
                );
            }
        }
        Expression::InList { expr, values, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
            for item in values {
                extract_referenced_tables_with_schema(
                    item,
                    output,
                    available_tables,
                    column_to_table,
                );
            }
        }
        Expression::Between { expr, low, high, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
            extract_referenced_tables_with_schema(low, output, available_tables, column_to_table);
            extract_referenced_tables_with_schema(high, output, available_tables, column_to_table);
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_referenced_tables_with_schema(
                    op,
                    output,
                    available_tables,
                    column_to_table,
                );
            }
            for clause in when_clauses {
                for condition in &clause.conditions {
                    extract_referenced_tables_with_schema(
                        condition,
                        output,
                        available_tables,
                        column_to_table,
                    );
                }
                extract_referenced_tables_with_schema(
                    &clause.result,
                    output,
                    available_tables,
                    column_to_table,
                );
            }
            if let Some(else_res) = else_result {
                extract_referenced_tables_with_schema(
                    else_res,
                    output,
                    available_tables,
                    column_to_table,
                );
            }
        }
        Expression::IsNull { expr, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
        }
        Expression::Cast { expr, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
        }
        Expression::In { expr, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
            // Note: We don't traverse into subqueries as they reference different tables
        }
        Expression::Position { substring, string, .. } => {
            extract_referenced_tables_with_schema(
                substring,
                output,
                available_tables,
                column_to_table,
            );
            extract_referenced_tables_with_schema(
                string,
                output,
                available_tables,
                column_to_table,
            );
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                extract_referenced_tables_with_schema(
                    char_expr,
                    output,
                    available_tables,
                    column_to_table,
                );
            }
            extract_referenced_tables_with_schema(
                string,
                output,
                available_tables,
                column_to_table,
            );
        }
        Expression::Like { expr, pattern, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
            extract_referenced_tables_with_schema(
                pattern,
                output,
                available_tables,
                column_to_table,
            );
        }
        // For other expressions (literals, wildcards, subqueries, etc.), no direct column refs to extract
        _ => {}
    }
}
