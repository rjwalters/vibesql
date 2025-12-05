//! Extract table names from AST for cache invalidation

use std::collections::HashSet;

/// Extract all table names referenced in a SELECT statement
pub fn extract_tables_from_select(stmt: &vibesql_ast::SelectStmt) -> HashSet<String> {
    let mut tables = HashSet::new();

    // Extract from FROM clause
    if let Some(from_clause) = &stmt.from {
        extract_from_from_clause(from_clause, &mut tables);
    }

    // Extract from subqueries in SELECT list
    for select_item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = select_item {
            extract_from_expression(expr, &mut tables);
        }
    }

    // Extract from WHERE clause
    if let Some(where_clause) = &stmt.where_clause {
        extract_from_expression(where_clause, &mut tables);
    }

    // Extract from GROUP BY
    if let Some(group_by) = &stmt.group_by {
        for expr in group_by.all_expressions() {
            extract_from_expression(expr, &mut tables);
        }
    }

    // Extract from HAVING
    if let Some(having) = &stmt.having {
        extract_from_expression(having, &mut tables);
    }

    // Extract from ORDER BY
    if let Some(order_by) = &stmt.order_by {
        for order_item in order_by {
            extract_from_expression(&order_item.expr, &mut tables);
        }
    }

    // Extract from CTEs (WITH clause)
    if let Some(with_clause) = &stmt.with_clause {
        for cte in with_clause {
            let cte_tables = extract_tables_from_select(&cte.query);
            tables.extend(cte_tables);
        }
    }

    // Extract from set operations (UNION, INTERSECT, EXCEPT)
    if let Some(set_op) = &stmt.set_operation {
        let right_tables = extract_tables_from_select(&set_op.right);
        tables.extend(right_tables);
    }

    tables
}

/// Extract table names from a FROM clause
fn extract_from_from_clause(from: &vibesql_ast::FromClause, tables: &mut HashSet<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            // Handle qualified names (schema.table) - we want just the table name
            let table_name = if let Some(pos) = name.rfind('.') { &name[pos + 1..] } else { name };
            tables.insert(table_name.to_string());
        }
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            extract_from_from_clause(left, tables);
            extract_from_from_clause(right, tables);
            if let Some(cond) = condition {
                extract_from_expression(cond, tables);
            }
        }
        vibesql_ast::FromClause::Subquery { query, .. } => {
            let subquery_tables = extract_tables_from_select(query);
            tables.extend(subquery_tables);
        }
    }
}

/// Extract table names from an expression (for subqueries)
fn extract_from_expression(expr: &vibesql_ast::Expression, tables: &mut HashSet<String>) {
    match expr {
        vibesql_ast::Expression::ScalarSubquery(stmt) => {
            let subquery_tables = extract_tables_from_select(stmt);
            tables.extend(subquery_tables);
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            extract_from_expression(left, tables);
            extract_from_expression(right, tables);
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => {
            extract_from_expression(expr, tables);
        }
        vibesql_ast::Expression::Function { args, .. }
        | vibesql_ast::Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_from_expression(arg, tables);
            }
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result, .. } => {
            if let Some(op) = operand {
                extract_from_expression(op, tables);
            }
            for when_clause in when_clauses {
                for condition in &when_clause.conditions {
                    extract_from_expression(condition, tables);
                }
                extract_from_expression(&when_clause.result, tables);
            }
            if let Some(else_expr) = else_result {
                extract_from_expression(else_expr, tables);
            }
        }
        vibesql_ast::Expression::In { expr, subquery, .. } => {
            extract_from_expression(expr, tables);
            let subquery_tables = extract_tables_from_select(subquery);
            tables.extend(subquery_tables);
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            extract_from_expression(expr, tables);
            for val in values {
                extract_from_expression(val, tables);
            }
        }
        vibesql_ast::Expression::Exists { subquery, .. } => {
            let subquery_tables = extract_tables_from_select(subquery);
            tables.extend(subquery_tables);
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            extract_from_expression(expr, tables);
            extract_from_expression(low, tables);
            extract_from_expression(high, tables);
        }
        vibesql_ast::Expression::IsNull { expr, .. } => {
            extract_from_expression(expr, tables);
        }
        vibesql_ast::Expression::Cast { expr, .. } => {
            extract_from_expression(expr, tables);
        }
        vibesql_ast::Expression::Like { expr, pattern, .. } => {
            extract_from_expression(expr, tables);
            extract_from_expression(pattern, tables);
        }
        vibesql_ast::Expression::Position { substring, string, .. } => {
            extract_from_expression(substring, tables);
            extract_from_expression(string, tables);
        }
        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            if let Some(removal) = removal_char {
                extract_from_expression(removal, tables);
            }
            extract_from_expression(string, tables);
        }
        vibesql_ast::Expression::Extract { expr, .. } => {
            extract_from_expression(expr, tables);
        }
        vibesql_ast::Expression::QuantifiedComparison { expr, subquery, .. } => {
            extract_from_expression(expr, tables);
            let subquery_tables = extract_tables_from_select(subquery);
            tables.extend(subquery_tables);
        }
        vibesql_ast::Expression::Conjunction(children)
        | vibesql_ast::Expression::Disjunction(children) => {
            for child in children {
                extract_from_expression(child, tables);
            }
        }

        // Leaf expressions - no tables to extract
        vibesql_ast::Expression::Literal(_)
        | vibesql_ast::Expression::Placeholder(_)
        | vibesql_ast::Expression::NumberedPlaceholder(_)
        | vibesql_ast::Expression::NamedPlaceholder(_)
        | vibesql_ast::Expression::ColumnRef { .. }
        | vibesql_ast::Expression::Wildcard
        | vibesql_ast::Expression::CurrentDate
        | vibesql_ast::Expression::CurrentTime { .. }
        | vibesql_ast::Expression::CurrentTimestamp { .. }
        | vibesql_ast::Expression::Interval { .. }
        | vibesql_ast::Expression::Default
        | vibesql_ast::Expression::DuplicateKeyValue { .. }
        | vibesql_ast::Expression::WindowFunction { .. }
        | vibesql_ast::Expression::NextValue { .. }
        | vibesql_ast::Expression::MatchAgainst { .. }
        | vibesql_ast::Expression::PseudoVariable { .. }
        | vibesql_ast::Expression::SessionVariable { .. } => {}
    }
}

/// Extract table names from any statement (for comprehensive cache invalidation)
pub fn extract_tables_from_statement(stmt: &vibesql_ast::Statement) -> HashSet<String> {
    match stmt {
        vibesql_ast::Statement::Select(select) => extract_tables_from_select(select),
        vibesql_ast::Statement::Insert(insert) => {
            let mut tables = HashSet::new();
            // Extract table name being inserted into
            let table_name = if let Some(pos) = insert.table_name.rfind('.') {
                &insert.table_name[pos + 1..]
            } else {
                &insert.table_name
            };
            tables.insert(table_name.to_string());

            // Extract from source (VALUES or SELECT)
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    for row in values {
                        for expr in row {
                            extract_from_expression(expr, &mut tables);
                        }
                    }
                }
                vibesql_ast::InsertSource::Select(select) => {
                    let select_tables = extract_tables_from_select(select);
                    tables.extend(select_tables);
                }
            }

            tables
        }
        vibesql_ast::Statement::Update(update) => {
            let mut tables = HashSet::new();
            // Extract table being updated
            let table_name = if let Some(pos) = update.table_name.rfind('.') {
                &update.table_name[pos + 1..]
            } else {
                &update.table_name
            };
            tables.insert(table_name.to_string());

            // Extract from SET assignments
            for assignment in &update.assignments {
                extract_from_expression(&assignment.value, &mut tables);
            }

            // Extract from WHERE clause
            if let Some(vibesql_ast::WhereClause::Condition(expr)) = &update.where_clause {
                extract_from_expression(expr, &mut tables);
            }

            tables
        }
        vibesql_ast::Statement::Delete(delete) => {
            let mut tables = HashSet::new();
            // Extract table being deleted from
            let table_name = if let Some(pos) = delete.table_name.rfind('.') {
                &delete.table_name[pos + 1..]
            } else {
                &delete.table_name
            };
            tables.insert(table_name.to_string());

            // Extract from WHERE clause
            if let Some(vibesql_ast::WhereClause::Condition(expr)) = &delete.where_clause {
                extract_from_expression(expr, &mut tables);
            }

            tables
        }
        // DDL statements don't reference tables in a way that matters for SELECT caching
        _ => HashSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_parser::Parser;

    use super::*;

    #[test]
    fn test_extract_simple_select() {
        let sql = "SELECT * FROM users";
        let stmt = Parser::parse_sql(sql).unwrap();

        if let vibesql_ast::Statement::Select(select) = stmt {
            let tables = extract_tables_from_select(&select);
            assert_eq!(tables.len(), 1);
            // Parser uppercases identifiers
            assert!(tables.contains("USERS"));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_extract_join() {
        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
        let stmt = Parser::parse_sql(sql).unwrap();

        if let vibesql_ast::Statement::Select(select) = stmt {
            let tables = extract_tables_from_select(&select);
            assert_eq!(tables.len(), 2);
            // Parser uppercases identifiers
            assert!(tables.contains("USERS"));
            assert!(tables.contains("ORDERS"));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_extract_qualified_table_name() {
        let sql = "SELECT * FROM public.users";
        let stmt = Parser::parse_sql(sql).unwrap();

        if let vibesql_ast::Statement::Select(select) = stmt {
            let tables = extract_tables_from_select(&select);
            assert_eq!(tables.len(), 1);
            // Should extract just the table name, not the schema
            assert!(tables.contains("USERS"));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_extract_subquery_in_from() {
        let sql = "SELECT * FROM (SELECT * FROM users) AS u";
        let stmt = Parser::parse_sql(sql).unwrap();

        if let vibesql_ast::Statement::Select(select) = stmt {
            let tables = extract_tables_from_select(&select);
            assert_eq!(tables.len(), 1);
            assert!(tables.contains("USERS"));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_extract_from_insert() {
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let stmt = Parser::parse_sql(sql).unwrap();
        let tables = extract_tables_from_statement(&stmt);

        assert_eq!(tables.len(), 1);
        assert!(tables.contains("USERS"));
    }

    #[test]
    fn test_extract_from_update() {
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        let stmt = Parser::parse_sql(sql).unwrap();
        let tables = extract_tables_from_statement(&stmt);

        assert_eq!(tables.len(), 1);
        assert!(tables.contains("USERS"));
    }

    #[test]
    fn test_extract_from_delete() {
        let sql = "DELETE FROM users WHERE id = 1";
        let stmt = Parser::parse_sql(sql).unwrap();
        let tables = extract_tables_from_statement(&stmt);

        assert_eq!(tables.len(), 1);
        assert!(tables.contains("USERS"));
    }
}
