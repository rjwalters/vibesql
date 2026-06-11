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
        FromClause::Table { name, alias, column_aliases, .. } => {
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
        FromClause::Values { alias, column_aliases, .. } => {
            tables.push(TableRef {
                name: alias.clone(),
                alias: Some(alias.clone()),
                is_cte: false,
                is_subquery: false,
                subquery: None,
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
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => {
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
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => {
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

/// Marker inserted into the referenced-tables set when an unqualified column
/// cannot be resolved to any local FROM table. Such columns are correlated
/// references to an outer query (or unknown columns that will error later);
/// the marker prevents the containing predicate from being classified as
/// table-local and pushed down to a single-table scan, where the outer-query
/// context needed to resolve the column is unavailable (fix for select1-18.1).
pub(super) const OUTER_REF_MARKER: &str = "__outer_ref__";

/// Extract all table names referenced in an expression using schema-based column resolution
///
/// This method uses actual database schema to resolve unqualified columns.
/// Unqualified columns that cannot be resolved insert [`OUTER_REF_MARKER`]
/// instead of a table name (see its documentation).
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
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_some() =>
        {
            output.insert(col_id.table_canonical().unwrap().to_lowercase());
        }
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            // Use schema-based lookup
            if let Some(table) = super::utils::resolve_column_with_fallback(
                col_id.column_canonical(),
                column_to_table,
            ) {
                output.insert(table.to_lowercase());
            } else {
                // Correlated outer reference: not resolvable from local tables,
                // so the predicate must be evaluated post-join where the merged
                // outer context is available.
                output.insert(OUTER_REF_MARKER.to_string());
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
            // The IN subquery may be correlated with outer tables (e.g.
            // `x IN (SELECT x FROM t2 WHERE x > c)` where `c` belongs to another
            // table in the same FROM). If the predicate is pushed down to a
            // single-table scan, those correlated references go out of scope and
            // column resolution fails. Insert the same post-join marker used for
            // ScalarSubquery/Exists so predicates containing IN subqueries are
            // treated as complex and evaluated after the join (fix for
            // select1-18.1). Uncorrelated IN subqueries that can be converted to
            // semi-joins are rewritten earlier by optimizer/subquery_to_join and
            // never reach this point as Expression::In.
            output.insert("__subquery__".to_string());
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
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            extract_referenced_tables_with_schema(expr, output, available_tables, column_to_table);
            extract_referenced_tables_with_schema(
                pattern,
                output,
                available_tables,
                column_to_table,
            );
        }
        // Scalar subqueries should NOT be pushed down to individual table scans
        // because they may reference CTEs or complex structures that aren't available
        // during table scan. By inserting a marker that won't match any real table,
        // we ensure predicates containing scalar subqueries are treated as complex
        // and applied post-join (fix for TPC-H Q15).
        Expression::ScalarSubquery(_)
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. } => {
            output.insert("__subquery__".to_string());
        }
        // For other expressions (literals, wildcards, etc.), no direct column refs to extract
        _ => {}
    }
}
