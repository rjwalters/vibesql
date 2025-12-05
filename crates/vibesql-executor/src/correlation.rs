//! Correlation detection for subqueries
//!
//! This module provides utilities to determine if a subquery is correlated
//! (references columns from an outer query) or non-correlated (independent).

use crate::schema::CombinedSchema;
use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};

/// Check if a subquery is correlated with the outer query
///
/// A subquery is correlated if it references any columns from the outer schema
/// that aren't defined within the subquery itself.
///
/// # Arguments
/// * `subquery` - The subquery to analyze
/// * `outer_schema` - Schema of the outer query context
///
/// # Returns
/// * `true` if the subquery references outer columns (correlated)
/// * `false` if the subquery is independent (non-correlated)
pub fn is_correlated(subquery: &SelectStmt, outer_schema: &CombinedSchema) -> bool {
    // If there's no outer schema, the subquery can't be correlated
    if outer_schema.table_schemas.is_empty() {
        return false;
    }

    // Extract table names from the subquery's FROM clause
    let subquery_tables = extract_table_names_from_from_clause(subquery.from.as_ref());

    // Check all expressions in the subquery for column references
    // that belong to the outer schema (excluding subquery's own tables)
    is_select_stmt_correlated_impl(subquery, outer_schema, &subquery_tables)
}

/// Extract all table/alias names from a FROM clause
fn extract_table_names_from_from_clause(from: Option<&FromClause>) -> Vec<String> {
    let mut tables = Vec::new();
    if let Some(from_clause) = from {
        extract_table_names_recursive(from_clause, &mut tables);
    }
    tables
}

/// Recursively extract table names from a FROM clause
fn extract_table_names_recursive(from: &FromClause, tables: &mut Vec<String>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            // Use alias if present, otherwise use table name
            tables.push(alias.clone().unwrap_or_else(|| name.clone()));
        }
        FromClause::Join { left, right, .. } => {
            extract_table_names_recursive(left, tables);
            extract_table_names_recursive(right, tables);
        }
        FromClause::Subquery { alias, .. } => {
            // Derived tables are referenced by their alias
            tables.push(alias.clone());
        }
    }
}

/// Check if a SELECT statement references columns from outer schema
fn is_select_stmt_correlated_impl(
    stmt: &SelectStmt,
    outer_schema: &CombinedSchema,
    subquery_tables: &[String],
) -> bool {
    // Check SELECT list
    for item in &stmt.select_list {
        if is_select_item_correlated(item, outer_schema, subquery_tables) {
            return true;
        }
    }

    // Check WHERE clause
    if let Some(where_expr) = &stmt.where_clause {
        if is_expression_correlated(where_expr, outer_schema, subquery_tables) {
            return true;
        }
    }

    // Check GROUP BY
    if let Some(group_by) = &stmt.group_by {
        for expr in group_by.all_expressions() {
            if is_expression_correlated(expr, outer_schema, subquery_tables) {
                return true;
            }
        }
    }

    // Check HAVING
    if let Some(having) = &stmt.having {
        if is_expression_correlated(having, outer_schema, subquery_tables) {
            return true;
        }
    }

    // Check ORDER BY
    if let Some(order_by) = &stmt.order_by {
        for item in order_by {
            if is_expression_correlated(&item.expr, outer_schema, subquery_tables) {
                return true;
            }
        }
    }

    // Check FROM clause (subqueries in FROM can reference outer columns)
    if let Some(from) = &stmt.from {
        if is_from_clause_correlated(from, outer_schema, subquery_tables) {
            return true;
        }
    }

    // Check WITH clause (CTEs)
    if let Some(with_clause) = &stmt.with_clause {
        for cte in with_clause {
            // CTEs have their own scope, so we need to extract their tables too
            let cte_tables = extract_table_names_from_from_clause(cte.query.from.as_ref());
            if is_select_stmt_correlated_impl(&cte.query, outer_schema, &cte_tables) {
                return true;
            }
        }
    }

    // Check set operations (UNION, INTERSECT, EXCEPT)
    if let Some(set_op) = &stmt.set_operation {
        // Set operations combine results, but both sides should use same subquery tables
        if is_select_stmt_correlated_impl(&set_op.right, outer_schema, subquery_tables) {
            return true;
        }
    }

    false
}

/// Check if a SELECT item references columns from outer schema
fn is_select_item_correlated(
    item: &SelectItem,
    outer_schema: &CombinedSchema,
    subquery_tables: &[String],
) -> bool {
    match item {
        SelectItem::Expression { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
        }
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
            // Wildcards expand to columns from the subquery's own FROM clause
            // They don't directly reference outer columns
            false
        }
    }
}

/// Check if a FROM clause references columns from outer schema
fn is_from_clause_correlated(
    from: &FromClause,
    outer_schema: &CombinedSchema,
    subquery_tables: &[String],
) -> bool {
    match from {
        FromClause::Table { .. } => false,
        FromClause::Join { left, right, condition, .. } => {
            // Check left and right sides of join
            if is_from_clause_correlated(left, outer_schema, subquery_tables)
                || is_from_clause_correlated(right, outer_schema, subquery_tables)
            {
                return true;
            }
            // Check join condition
            if let Some(cond) = condition {
                if is_expression_correlated(cond, outer_schema, subquery_tables) {
                    return true;
                }
            }
            false
        }
        FromClause::Subquery { query, .. } => {
            // Subqueries in FROM can be correlated with the outer query
            // Extract their tables and check recursively
            let nested_tables = extract_table_names_from_from_clause(query.from.as_ref());
            is_select_stmt_correlated_impl(query, outer_schema, &nested_tables)
        }
    }
}

/// Check if an expression references columns from outer schema
fn is_expression_correlated(
    expr: &Expression,
    outer_schema: &CombinedSchema,
    subquery_tables: &[String],
) -> bool {
    match expr {
        Expression::Literal(_)
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::Wildcard => false,

        Expression::ColumnRef { table, column } => {
            // Check if this column reference belongs to the outer schema
            // But exclude references to the subquery's own tables

            // If table is explicitly specified, check if it's one of the subquery's own tables
            if let Some(table_name) = table {
                // Case-insensitive check if this table is in the subquery's FROM clause
                let table_lower = table_name.to_lowercase();
                if subquery_tables.iter().any(|t| t.to_lowercase() == table_lower) {
                    // This references the subquery's own table, not outer query
                    return false;
                }
            } else {
                // Unqualified column reference
                // If the subquery has its own FROM clause (has tables), assume the column
                // belongs to the subquery per SQL scoping rules (inner scope shadows outer)
                if !subquery_tables.is_empty() {
                    return false;
                }
            }

            // Check if this column exists in outer schema
            outer_schema.get_column_index(table.as_deref(), column).is_some()
        }

        Expression::BinaryOp { left, right, .. } => {
            is_expression_correlated(left, outer_schema, subquery_tables)
                || is_expression_correlated(right, outer_schema, subquery_tables)
        }

        Expression::UnaryOp { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
        }

        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(|arg| is_expression_correlated(arg, outer_schema, subquery_tables))
        }

        Expression::IsNull { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
        }

        Expression::Case { operand, when_clauses, else_result } => {
            // Check operand
            if let Some(op) = operand {
                if is_expression_correlated(op, outer_schema, subquery_tables) {
                    return true;
                }
            }
            // Check WHEN clauses
            for when in when_clauses {
                for condition in &when.conditions {
                    if is_expression_correlated(condition, outer_schema, subquery_tables) {
                        return true;
                    }
                }
                if is_expression_correlated(&when.result, outer_schema, subquery_tables) {
                    return true;
                }
            }
            // Check ELSE
            if let Some(else_expr) = else_result {
                if is_expression_correlated(else_expr, outer_schema, subquery_tables) {
                    return true;
                }
            }
            false
        }

        Expression::ScalarSubquery(subquery) => {
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables)
        }

        Expression::In { expr, subquery, .. } => {
            if is_expression_correlated(expr, outer_schema, subquery_tables) {
                return true;
            }
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables)
        }

        Expression::InList { expr, values, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
                || values
                    .iter()
                    .any(|val| is_expression_correlated(val, outer_schema, subquery_tables))
        }

        Expression::Between { expr, low, high, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
                || is_expression_correlated(low, outer_schema, subquery_tables)
                || is_expression_correlated(high, outer_schema, subquery_tables)
        }

        Expression::Like { expr, pattern, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
                || is_expression_correlated(pattern, outer_schema, subquery_tables)
        }

        Expression::Exists { subquery, .. } => {
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables)
        }

        Expression::QuantifiedComparison { expr, subquery, .. } => {
            if is_expression_correlated(expr, outer_schema, subquery_tables) {
                return true;
            }
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables)
        }

        Expression::Cast { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
        }

        Expression::Position { substring, string, .. } => {
            is_expression_correlated(substring, outer_schema, subquery_tables)
                || is_expression_correlated(string, outer_schema, subquery_tables)
        }

        Expression::Trim { removal_char, string, .. } => {
            removal_char
                .as_ref()
                .map(|c| is_expression_correlated(c, outer_schema, subquery_tables))
                .unwrap_or(false)
                || is_expression_correlated(string, outer_schema, subquery_tables)
        }

        Expression::Extract { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables)
        }

        Expression::WindowFunction { function, over } => {
            // Check window function arguments
            let func_correlated = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args
                    .iter()
                    .any(|arg| is_expression_correlated(arg, outer_schema, subquery_tables)),
            };

            // Check PARTITION BY
            let partition_correlated = over
                .partition_by
                .as_ref()
                .map(|parts| {
                    parts.iter().any(|p| is_expression_correlated(p, outer_schema, subquery_tables))
                })
                .unwrap_or(false);

            // Check ORDER BY
            let order_correlated = over
                .order_by
                .as_ref()
                .map(|orders| {
                    orders
                        .iter()
                        .any(|o| is_expression_correlated(&o.expr, outer_schema, subquery_tables))
                })
                .unwrap_or(false);

            func_correlated || partition_correlated || order_correlated
        }

        Expression::Interval { value, .. } => {
            is_expression_correlated(value, outer_schema, subquery_tables)
        }

        Expression::Conjunction(children) | Expression::Disjunction(children) => children
            .iter()
            .any(|child| is_expression_correlated(child, outer_schema, subquery_tables)),

        Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. }
        | Expression::DuplicateKeyValue { .. }
        | Expression::Default
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::NextValue { .. }
        | Expression::MatchAgainst { .. } => {
            // These don't reference outer query columns
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, Expression, SelectItem, SelectStmt};

    fn make_outer_schema() -> CombinedSchema {
        // Create a simple schema with one table "tab0" with columns: pk, col0, col1, col2, col3, col4
        let columns = vec![
            vibesql_catalog::ColumnSchema {
                name: "pk".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col0".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col3".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col4".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ];

        let table_schema = vibesql_catalog::TableSchema::new("tab0".to_string(), columns);
        CombinedSchema::from_table("tab0".to_string(), table_schema)
    }

    #[test]
    fn test_non_correlated_subquery() {
        let outer_schema = make_outer_schema();

        // SELECT col0 FROM tab0 WHERE col4 = 97.5
        // This is non-correlated - only references columns from its own FROM clause
        let subquery = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "col0".to_string() },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "tab0".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "col4".to_string() }),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Float(97.5))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // With the fixed implementation, this is correctly detected as non-correlated
        // because both col0 and col4 exist in the subquery's own FROM clause (tab0)
        assert!(!is_correlated(&subquery, &outer_schema));
    }

    #[test]
    fn test_correlated_subquery_with_outer_column() {
        let outer_schema = make_outer_schema();

        // SELECT 1 WHERE outer_table.col3 = 5
        // This references a column from the outer schema
        let subquery = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: Some("tab0".to_string()),
                    column: "col3".to_string(),
                }),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(5))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(is_correlated(&subquery, &outer_schema));
    }

    #[test]
    fn test_empty_outer_schema() {
        let outer_schema =
            CombinedSchema { table_schemas: std::collections::HashMap::new(), total_columns: 0 };

        let subquery = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(!is_correlated(&subquery, &outer_schema));
    }
}
