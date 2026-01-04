//! Schema-level validation for subqueries
//!
//! Validates that aggregates in subqueries don't reference outer columns.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{Expression, SelectItem, SelectStmt};
use vibesql_catalog::{TableIdentifier, TableSchema};

use super::aggregates::is_aggregate_function;
use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Check if an expression contains a column reference that belongs to the outer schema
/// (not the subquery's own tables)
///
/// Returns Some(column_name) if an outer column is found, None otherwise.
#[allow(dead_code)]
fn find_outer_column_in_expression(
    expr: &Expression,
    subquery_tables: &[String],
    inner_schema: Option<&CombinedSchema>,
    outer_schema: &CombinedSchema,
) -> Option<String> {
    match expr {
        Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();

            // Skip wildcard
            if column == "*" {
                return None;
            }

            if let Some(table_name) = table {
                // Qualified column reference - check if table is in subquery's tables
                let table_lower = table_name.to_lowercase();
                if !subquery_tables.iter().any(|t| t.to_lowercase() == table_lower) {
                    // Not in subquery's tables - this is an outer column
                    if outer_schema.get_column_index(Some(table_name), column).is_some() {
                        return Some(column.to_string());
                    }
                }
            } else {
                // Unqualified column reference - check if it's in outer schema but NOT in inner
                // First check if it's in the inner schema (subquery's tables)
                let in_inner = inner_schema
                    .map(|s| s.get_column_index(None, column).is_some())
                    .unwrap_or(false);

                if !in_inner {
                    // Not in inner schema - check if it's in outer schema
                    if outer_schema.get_column_index(None, column).is_some() {
                        return Some(column.to_string());
                    }
                }
            }
            None
        }
        Expression::BinaryOp { left, right, .. } => {
            find_outer_column_in_expression(left, subquery_tables, inner_schema, outer_schema)
                .or_else(|| {
                    find_outer_column_in_expression(
                        right,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
        }
        Expression::UnaryOp { expr, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                if let Some(col) = find_outer_column_in_expression(
                    arg,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(col);
                }
            }
            None
        }
        Expression::Cast { expr, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(col) = find_outer_column_in_expression(
                    op,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(col);
                }
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    if let Some(col) = find_outer_column_in_expression(
                        cond,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    ) {
                        return Some(col);
                    }
                }
                if let Some(col) = find_outer_column_in_expression(
                    &when_clause.result,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(col);
                }
            }
            if let Some(else_expr) = else_result {
                find_outer_column_in_expression(
                    else_expr,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                )
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Between { expr, low, high, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| {
                    find_outer_column_in_expression(
                        low,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
                .or_else(|| {
                    find_outer_column_in_expression(
                        high,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
        }
        Expression::InList { expr, values, .. } => {
            if let Some(col) = find_outer_column_in_expression(
                expr,
                subquery_tables,
                inner_schema,
                outer_schema,
            ) {
                return Some(col);
            }
            for val in values {
                if let Some(col) = find_outer_column_in_expression(
                    val,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(col);
                }
            }
            None
        }
        Expression::Like { expr, pattern, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| {
                    find_outer_column_in_expression(
                        pattern,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(col) = find_outer_column_in_expression(
                    child,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(col);
                }
            }
            None
        }
        // Terminals and subqueries (have their own scope)
        _ => None,
    }
}

/// Check if an aggregate function's arguments reference columns from the outer schema
///
/// Returns Some(aggregate_name) if misuse is found, None otherwise.
#[allow(dead_code)]
fn find_aggregate_with_outer_column(
    expr: &Expression,
    subquery_tables: &[String],
    inner_schema: Option<&CombinedSchema>,
    outer_schema: &CombinedSchema,
) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, args, .. } => {
            // Check if any argument references an outer column
            for arg in args {
                if find_outer_column_in_expression(arg, subquery_tables, inner_schema, outer_schema)
                    .is_some()
                {
                    return Some(name.to_string());
                }
            }
            None
        }
        Expression::Function { name, args, .. } => {
            // Check if this is an aggregate function
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                // Multi-arg MIN/MAX are scalar functions
                if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                    // Scalar function - check args recursively
                    for arg in args {
                        if let Some(found) = find_aggregate_with_outer_column(
                            arg,
                            subquery_tables,
                            inner_schema,
                            outer_schema,
                        ) {
                            return Some(found);
                        }
                    }
                    None
                } else {
                    // Aggregate function - check if args reference outer columns
                    for arg in args {
                        if find_outer_column_in_expression(
                            arg,
                            subquery_tables,
                            inner_schema,
                            outer_schema,
                        )
                        .is_some()
                        {
                            return Some(name.to_string());
                        }
                    }
                    None
                }
            } else {
                // Non-aggregate function - check args recursively
                for arg in args {
                    if let Some(found) = find_aggregate_with_outer_column(
                        arg,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    ) {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            find_aggregate_with_outer_column(left, subquery_tables, inner_schema, outer_schema)
                .or_else(|| {
                    find_aggregate_with_outer_column(
                        right,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
        }
        Expression::UnaryOp { expr, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Cast { expr, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = find_aggregate_with_outer_column(
                    op,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(found);
                }
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    if let Some(found) = find_aggregate_with_outer_column(
                        cond,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    ) {
                        return Some(found);
                    }
                }
                if let Some(found) = find_aggregate_with_outer_column(
                    &when_clause.result,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_aggregate_with_outer_column(
                    else_expr,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                )
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Between { expr, low, high, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| {
                    find_aggregate_with_outer_column(
                        low,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
                .or_else(|| {
                    find_aggregate_with_outer_column(
                        high,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
        }
        Expression::InList { expr, values, .. } => {
            if let Some(found) = find_aggregate_with_outer_column(
                expr,
                subquery_tables,
                inner_schema,
                outer_schema,
            ) {
                return Some(found);
            }
            for val in values {
                if let Some(found) = find_aggregate_with_outer_column(
                    val,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(found);
                }
            }
            None
        }
        Expression::Like { expr, pattern, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| {
                    find_aggregate_with_outer_column(
                        pattern,
                        subquery_tables,
                        inner_schema,
                        outer_schema,
                    )
                })
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = find_aggregate_with_outer_column(
                    child,
                    subquery_tables,
                    inner_schema,
                    outer_schema,
                ) {
                    return Some(found);
                }
            }
            None
        }
        // Subqueries have their own scope
        Expression::ScalarSubquery(_) | Expression::Exists { .. } | Expression::In { .. } => None,
        _ => None,
    }
}

/// Extract table names from a FROM clause
#[allow(dead_code)]
fn extract_table_names(from: Option<&vibesql_ast::FromClause>) -> Vec<String> {
    let mut tables = Vec::new();
    if let Some(from_clause) = from {
        extract_table_names_recursive(from_clause, &mut tables);
    }
    tables
}

/// Recursively extract table names from a FROM clause
#[allow(dead_code)]
fn extract_table_names_recursive(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            tables.push(alias.clone().unwrap_or_else(|| name.clone()));
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            extract_table_names_recursive(left, tables);
            extract_table_names_recursive(right, tables);
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            tables.push(alias.clone());
        }
        vibesql_ast::FromClause::Values { alias, .. } => {
            tables.push(alias.clone());
        }
    }
}

/// Build a CombinedSchema from table names in a FROM clause
///
/// Looks up each table in the database and builds a schema containing all columns.
#[allow(dead_code)]
fn build_schema_from_tables(
    tables: &[String],
    database: &vibesql_storage::Database,
) -> Option<CombinedSchema> {
    if tables.is_empty() {
        return None;
    }

    let mut total_columns = 0;
    let mut table_schemas = HashMap::<TableIdentifier, (usize, TableSchema)>::new();

    for table_name in tables {
        // Skip subquery aliases - they don't have schemas in the database
        // and we can't validate against them without executing the subquery
        if let Some(table) = database.get_table(table_name) {
            let table_schema = table.schema.clone();
            let table_id = TableIdentifier::unquoted(table_name);
            table_schemas.insert(table_id, (total_columns, table_schema.clone()));
            total_columns += table_schema.columns.len();
        }
    }

    if table_schemas.is_empty() {
        None
    } else {
        Some(CombinedSchema {
            table_schemas,
            total_columns,
            hidden_columns: HashSet::new(),
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
            joined_columns: HashSet::new(),
            using_coalesce_pairs: HashMap::new(),
            column_replacement_map: HashMap::new(),
        })
    }
}

/// Validate that aggregates in a subquery's SELECT list don't reference outer columns
///
/// When a subquery contains an aggregate function whose arguments reference columns
/// from an outer query (not from the subquery's own tables), it's a misuse of aggregate.
/// SQLite returns "misuse of aggregate: X()" for this case.
///
/// Example:
/// ```sql
/// SELECT max((SELECT count(x) FROM t35b)) FROM t35a;
/// -- Error: x is from outer t35a, not from t35b
/// ```
///
/// Returns an error if an aggregate references an outer column.
///
/// Note: This validation is skipped if the subquery's FROM clause contains CTEs or
/// subquery aliases, since we can't reliably determine the inner schema without
/// executing the CTE/subquery first.
#[allow(dead_code)]
pub fn validate_no_aggregate_with_outer_column(
    stmt: &SelectStmt,
    outer_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    // Get the tables in the subquery's FROM clause
    let subquery_tables = extract_table_names(stmt.from.as_ref());

    // Build the inner schema from the FROM clause tables
    let inner_schema = build_schema_from_tables(&subquery_tables, database);

    // If we have tables in FROM but couldn't build an inner schema,
    // those tables are likely CTEs or subquery aliases. Skip validation
    // since we can't reliably determine if a column is from inner or outer scope.
    if !subquery_tables.is_empty() && inner_schema.is_none() {
        return Ok(());
    }

    // Check each item in the select list
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(agg_name) = find_aggregate_with_outer_column(
                expr,
                &subquery_tables,
                inner_schema.as_ref(),
                outer_schema,
            ) {
                return Err(ExecutorError::MisuseOfAggregateContext { function_name: agg_name });
            }
        }
    }

    // Also check HAVING clause
    if let Some(having) = &stmt.having {
        if let Some(agg_name) = find_aggregate_with_outer_column(
            having,
            &subquery_tables,
            inner_schema.as_ref(),
            outer_schema,
        ) {
            return Err(ExecutorError::MisuseOfAggregateContext { function_name: agg_name });
        }
    }

    Ok(())
}
