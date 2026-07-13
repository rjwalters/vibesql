//! Correlation detection for subqueries
//!
//! This module provides utilities to determine if a subquery is correlated
//! (references columns from an outer query) or non-correlated (independent).

use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};

use crate::schema::CombinedSchema;

/// Check whether an unqualified column resolves to one of the subquery's own
/// inner tables by consulting the actual database schema.
///
/// SQL name resolution is innermost-scope-first: an unqualified column that
/// exists on a table in the subquery's own FROM clause binds to that inner
/// table, not to a same-named column in the outer query, so it is NOT a
/// correlation reference. See issue #5880 — the outer-schema-only self-join
/// heuristic below misses the case where the inner and outer tables are
/// distinct tables that share a column name.
fn unqualified_column_in_inner_tables(
    database: Option<&vibesql_storage::Database>,
    subquery_tables: &[String],
    column: &str,
) -> bool {
    let Some(db) = database else {
        return false;
    };
    subquery_tables.iter().any(|table_name| {
        db.get_table(table_name).is_some_and(|table| table.schema.has_column(column))
    })
}

/// Check if a subquery is correlated with the outer query
///
/// A subquery is correlated if it references any columns from the outer schema
/// that aren't defined within the subquery itself.
///
/// # Arguments
/// * `subquery` - The subquery to analyze
/// * `outer_schema` - Schema of the outer query context
/// * `database` - Optional database handle used to resolve unqualified column names against the
///   subquery's own inner tables (innermost-scope-first, issue #5880). When `None`, falls back to
///   the outer-schema-only self-join heuristic.
///
/// # Returns
/// * `true` if the subquery references outer columns (correlated)
/// * `false` if the subquery is independent (non-correlated)
pub fn is_correlated(
    subquery: &SelectStmt,
    outer_schema: &CombinedSchema,
    database: Option<&vibesql_storage::Database>,
) -> bool {
    // If there's no outer schema, the subquery can't be correlated
    if outer_schema.table_schemas.is_empty() {
        return false;
    }

    // Extract table names from the subquery's FROM clause
    let subquery_tables = extract_table_names_from_from_clause(subquery.from.as_ref());

    // Check all expressions in the subquery for column references
    // that belong to the outer schema (excluding subquery's own tables)
    is_select_stmt_correlated_impl(subquery, outer_schema, &subquery_tables, database)
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
        FromClause::Values { alias, .. } => {
            // VALUES clauses are referenced by their alias
            tables.push(alias.clone());
        }
        FromClause::TableFunction { alias, .. } => {
            // Table functions are referenced by their alias when present
            if let Some(a) = alias {
                tables.push(a.clone());
            }
        }
    }
}

/// Helper to extract table names from a SelectStmt's FROM clause
/// Used for set operation traversal (issue #4749)
fn extract_table_names_recursive_from_select(stmt: &SelectStmt, tables: &mut Vec<String>) {
    if let Some(from_clause) = &stmt.from {
        extract_table_names_recursive(from_clause, tables);
    }
    // Recursively handle chained set operations
    if let Some(set_op) = &stmt.set_operation {
        extract_table_names_recursive_from_select(&set_op.right, tables);
    }
}

/// Check if a SELECT statement references columns from outer schema
fn is_select_stmt_correlated_impl(
    stmt: &SelectStmt,
    outer_schema: &CombinedSchema,
    subquery_tables: &[String],
    database: Option<&vibesql_storage::Database>,
) -> bool {
    // Check SELECT list
    for item in &stmt.select_list {
        if is_select_item_correlated(item, outer_schema, subquery_tables, database) {
            return true;
        }
    }

    // Check WHERE clause
    if let Some(where_expr) = &stmt.where_clause {
        if is_expression_correlated(where_expr, outer_schema, subquery_tables, database) {
            return true;
        }
    }

    // Check GROUP BY
    if let Some(group_by) = &stmt.group_by {
        for expr in group_by.all_expressions() {
            if is_expression_correlated(expr, outer_schema, subquery_tables, database) {
                return true;
            }
        }
    }

    // Check HAVING
    if let Some(having) = &stmt.having {
        if is_expression_correlated(having, outer_schema, subquery_tables, database) {
            return true;
        }
    }

    // Check ORDER BY
    if let Some(order_by) = &stmt.order_by {
        for item in order_by {
            if is_expression_correlated(&item.expr, outer_schema, subquery_tables, database) {
                return true;
            }
        }
    }

    // Check FROM clause (subqueries in FROM can reference outer columns)
    if let Some(from) = &stmt.from {
        if is_from_clause_correlated(from, outer_schema, subquery_tables, database) {
            return true;
        }
    }

    // Check WITH clause (CTEs)
    if let Some(with_clause) = &stmt.with_clause {
        for cte in with_clause {
            // CTEs have their own scope, so we need to extract their tables too
            let cte_tables = extract_table_names_from_from_clause(cte.query.from.as_ref());
            if is_select_stmt_correlated_impl(&cte.query, outer_schema, &cte_tables, database) {
                return true;
            }
        }
    }

    // Check set operations (UNION, INTERSECT, EXCEPT)
    // FIX for issue #4749: Collect tables from all branches of the set operation
    // Each branch has its own FROM clause, so we need to include those tables
    // when checking for correlation to distinguish inner vs outer columns.
    if let Some(set_op) = &stmt.set_operation {
        // Collect tables from the set operation's right side
        let mut all_subquery_tables = subquery_tables.to_vec();
        extract_table_names_recursive_from_select(&set_op.right, &mut all_subquery_tables);
        if is_select_stmt_correlated_impl(
            &set_op.right,
            outer_schema,
            &all_subquery_tables,
            database,
        ) {
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
    database: Option<&vibesql_storage::Database>,
) -> bool {
    match item {
        SelectItem::Expression { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
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
    database: Option<&vibesql_storage::Database>,
) -> bool {
    match from {
        FromClause::Table { .. } => false,
        FromClause::Join { left, right, condition, .. } => {
            // Check left and right sides of join
            if is_from_clause_correlated(left, outer_schema, subquery_tables, database)
                || is_from_clause_correlated(right, outer_schema, subquery_tables, database)
            {
                return true;
            }
            // Check join condition
            if let Some(cond) = condition {
                if is_expression_correlated(cond, outer_schema, subquery_tables, database) {
                    return true;
                }
            }
            false
        }
        FromClause::Subquery { query, .. } => {
            // Subqueries in FROM can be correlated with the outer query
            // Extract their tables and check recursively
            let nested_tables = extract_table_names_from_from_clause(query.from.as_ref());
            is_select_stmt_correlated_impl(query, outer_schema, &nested_tables, database)
        }
        FromClause::Values { rows, .. } => {
            // VALUES clauses can contain expressions that reference outer columns
            rows.iter().any(|row| {
                row.iter().any(|expr| {
                    is_expression_correlated(expr, outer_schema, subquery_tables, database)
                })
            })
        }
        FromClause::TableFunction { args, .. } => {
            // Table function args (e.g. json_each(t.j)) can reference outer columns
            args.iter()
                .any(|expr| is_expression_correlated(expr, outer_schema, subquery_tables, database))
        }
    }
}

/// Check if an expression references columns from outer schema
fn is_expression_correlated(
    expr: &Expression,
    outer_schema: &CombinedSchema,
    subquery_tables: &[String],
    database: Option<&vibesql_storage::Database>,
) -> bool {
    match expr {
        Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::Wildcard => false,

        Expression::ColumnRef(col_id) => {
            // Check if this column reference belongs to the outer schema
            // But exclude references to the subquery's own tables

            // If table is explicitly specified, check if it's one of the subquery's own tables
            if let Some(table_name) = col_id.table_canonical() {
                // Case-insensitive check if this table is in the subquery's FROM clause
                let table_lower = table_name.to_lowercase();
                if subquery_tables.iter().any(|t| t.to_lowercase() == table_lower) {
                    // This references the subquery's own table, not outer query
                    return false;
                }
            } else {
                // Unqualified column reference
                //
                // For self-join scenarios where the same table appears in both outer and
                // inner queries (e.g., SELECT * FROM tab0 WHERE x IN (SELECT y FROM tab0)),
                // unqualified columns will resolve to the innermost (subquery's) table by
                // SQL's standard scoping rules.
                //
                // Check if the subquery has a table that also exists in the outer schema.
                // If so, assume unqualified columns belong to the subquery (NOT correlated).
                // This enables caching optimization for self-join IN subqueries.
                //
                // Note: The outer-schema check below is a heuristic that only
                // catches the self-join case (inner table name == outer table name).
                for subquery_table in subquery_tables {
                    // Check if this subquery table exists in outer schema
                    // Compare using case-insensitive matching (lowercase subquery table vs canonical outer)
                    let subquery_table_lower = subquery_table.to_lowercase();
                    if outer_schema
                        .table_schemas
                        .keys()
                        .any(|outer_table| outer_table.canonical() == subquery_table_lower)
                    {
                        // Self-join detected: subquery table matches an outer table
                        // Unqualified columns will resolve to the subquery's table first
                        return false;
                    }
                }

                // FIX for issue #5880: The self-join heuristic above misses the
                // common case where the inner and outer tables are *distinct*
                // tables that happen to share a column name (e.g.
                // `SELECT MIN(x) FROM u WHERE u.y = y` with outer table `t`). The
                // inner table `u` is never present in the outer schema, so the
                // heuristic never fires and `y` is wrongly treated as correlated.
                // Consult the real database schema: if the unqualified column
                // exists on one of the subquery's own inner tables it resolves
                // innermost-first and is NOT a correlation reference.
                if unqualified_column_in_inner_tables(
                    database,
                    subquery_tables,
                    col_id.column_canonical(),
                ) {
                    return false;
                }
            }

            // Check if this column exists in outer schema
            // Only relevant for non-self-join cases where the column might be correlated
            outer_schema
                .get_column_index(col_id.table_canonical(), col_id.column_canonical())
                .is_some()
        }

        Expression::BinaryOp { left, right, .. } => {
            is_expression_correlated(left, outer_schema, subquery_tables, database)
                || is_expression_correlated(right, outer_schema, subquery_tables, database)
        }

        Expression::UnaryOp { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
        }

        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => args
            .iter()
            .any(|arg| is_expression_correlated(arg, outer_schema, subquery_tables, database)),

        Expression::IsNull { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
        }

        Expression::IsDistinctFrom { left, right, .. } => {
            is_expression_correlated(left, outer_schema, subquery_tables, database)
                || is_expression_correlated(right, outer_schema, subquery_tables, database)
        }

        Expression::IsTruthValue { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
        }

        Expression::Case { operand, when_clauses, else_result } => {
            // Check operand
            if let Some(op) = operand {
                if is_expression_correlated(op, outer_schema, subquery_tables, database) {
                    return true;
                }
            }
            // Check WHEN clauses
            for when in when_clauses {
                for condition in &when.conditions {
                    if is_expression_correlated(condition, outer_schema, subquery_tables, database)
                    {
                        return true;
                    }
                }
                if is_expression_correlated(&when.result, outer_schema, subquery_tables, database) {
                    return true;
                }
            }
            // Check ELSE
            if let Some(else_expr) = else_result {
                if is_expression_correlated(else_expr, outer_schema, subquery_tables, database) {
                    return true;
                }
            }
            false
        }

        Expression::ScalarSubquery(subquery) => {
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables, database)
        }

        Expression::In { expr, subquery, .. } => {
            if is_expression_correlated(expr, outer_schema, subquery_tables, database) {
                return true;
            }
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables, database)
        }

        Expression::InList { expr, values, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
                || values.iter().any(|val| {
                    is_expression_correlated(val, outer_schema, subquery_tables, database)
                })
        }

        Expression::Between { expr, low, high, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
                || is_expression_correlated(low, outer_schema, subquery_tables, database)
                || is_expression_correlated(high, outer_schema, subquery_tables, database)
        }

        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
                || is_expression_correlated(pattern, outer_schema, subquery_tables, database)
        }

        Expression::Exists { subquery, .. } => {
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables, database)
        }

        Expression::QuantifiedComparison { expr, subquery, .. } => {
            if is_expression_correlated(expr, outer_schema, subquery_tables, database) {
                return true;
            }
            let nested_tables = extract_table_names_from_from_clause(subquery.from.as_ref());
            is_select_stmt_correlated_impl(subquery, outer_schema, &nested_tables, database)
        }

        Expression::Cast { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
        }

        Expression::Position { substring, string, .. } => {
            is_expression_correlated(substring, outer_schema, subquery_tables, database)
                || is_expression_correlated(string, outer_schema, subquery_tables, database)
        }

        Expression::Trim { removal_char, string, .. } => {
            removal_char
                .as_ref()
                .map(|c| is_expression_correlated(c, outer_schema, subquery_tables, database))
                .unwrap_or(false)
                || is_expression_correlated(string, outer_schema, subquery_tables, database)
        }

        Expression::Extract { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
        }

        Expression::WindowFunction { function, over } => {
            // Check window function arguments
            let func_correlated = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args.iter().any(|arg| {
                    is_expression_correlated(arg, outer_schema, subquery_tables, database)
                }),
            };

            // Check PARTITION BY
            let partition_correlated = over
                .partition_by
                .as_ref()
                .map(|parts| {
                    parts.iter().any(|p| {
                        is_expression_correlated(p, outer_schema, subquery_tables, database)
                    })
                })
                .unwrap_or(false);

            // Check ORDER BY
            let order_correlated = over
                .order_by
                .as_ref()
                .map(|orders| {
                    orders.iter().any(|o| {
                        is_expression_correlated(&o.expr, outer_schema, subquery_tables, database)
                    })
                })
                .unwrap_or(false);

            func_correlated || partition_correlated || order_correlated
        }

        Expression::Interval { value, .. } => {
            is_expression_correlated(value, outer_schema, subquery_tables, database)
        }

        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => children
            .iter()
            .any(|child| is_expression_correlated(child, outer_schema, subquery_tables, database)),

        Expression::Collate { expr, .. } => {
            is_expression_correlated(expr, outer_schema, subquery_tables, database)
        }

        Expression::Raise { error_message, .. } => error_message.as_ref().is_some_and(|msg| {
            is_expression_correlated(msg, outer_schema, subquery_tables, database)
        }),

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
    use vibesql_ast::{BinaryOperator, Expression, SelectItem, SelectStmt};

    use super::*;

    fn make_outer_schema() -> CombinedSchema {
        // Create a simple schema with one table "tab0" with columns: pk, col0, col1, col2, col3,
        // col4
        let columns = vec![
            vibesql_catalog::ColumnSchema {
                name: "pk".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col0".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col3".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col4".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
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
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("col0", false)),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "tab0".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col4", false,
                ))),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Float(97.5))),
            }),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        // With the fixed implementation, this is correctly detected as non-correlated
        // because both col0 and col4 exist in the subquery's own FROM clause (tab0)
        assert!(!is_correlated(&subquery, &outer_schema, None));
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
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "tab0", false, "col3", false,
                ))),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(5))),
            }),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert!(is_correlated(&subquery, &outer_schema, None));
    }

    /// Regression test for issue #6045 (Root Cause #2): a scalar subquery whose
    /// WHERE clause compares a 2-element row-value against outer columns must be
    /// detected as correlated. Previously such a subquery was treated as
    /// non-correlated and cached, so every outer row received the first row's
    /// result.
    ///
    /// Models: `SELECT (SELECT rowid FROM a1 WHERE (a,b) = (x,y)) FROM a2`
    /// where `a1(a,b)` is the inner table and the outer schema is `a2(x,y)`.
    #[test]
    fn test_row_value_where_clause_is_correlated() {
        // Outer schema: table "a2" with columns x, y
        let outer_columns = vec![
            vibesql_catalog::ColumnSchema {
                name: "x".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "y".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ];
        let outer_schema = CombinedSchema::from_table(
            "a2".to_string(),
            vibesql_catalog::TableSchema::new("a2".to_string(), outer_columns),
        );

        // Inner subquery: SELECT 1 FROM a1 WHERE (a, b) = (x, y)
        // a, b are the inner table's columns; x, y are the outer table's columns.
        let where_clause = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::RowValueConstructor(vec![
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("a", false)),
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("b", false)),
            ])),
            right: Box::new(Expression::RowValueConstructor(vec![
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("x", false)),
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("y", false)),
            ])),
        };

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
                index_hint: None,
                name: "a1".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(where_clause),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        // x and y are outer columns, so this subquery IS correlated.
        assert!(
            is_correlated(&subquery, &outer_schema, None),
            "row-value WHERE clause referencing outer columns must be correlated"
        );
    }

    #[test]
    fn test_empty_outer_schema() {
        let outer_schema = CombinedSchema {
            table_schemas: std::collections::HashMap::new(),
            total_columns: 0,
            hidden_columns: std::collections::HashSet::new(),
            always_hidden_columns: std::collections::HashSet::new(),
            outer_schema: None,
            duplicate_aliases: std::collections::HashSet::new(),
            joined_columns: std::collections::HashSet::new(),
            using_coalesce_indices: std::collections::HashMap::new(),
            column_replacement_map: std::collections::HashMap::new(),
            alias_tables: std::collections::HashSet::new(),
            shadowed_tables: std::collections::HashMap::new(),
        };

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
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert!(!is_correlated(&subquery, &outer_schema, None));
    }
}
