//! Table dependency extraction from SQL queries.
//!
//! This module provides functionality to extract all table names referenced
//! in a SQL query, which is used to determine which subscriptions need to be
//! re-evaluated when a table changes.

use std::collections::HashSet;

use vibesql_ast::{FromClause, SelectStmt, Statement};
use vibesql_parser::{ParseError, Parser};

/// Extract all table names referenced in a SQL query.
///
/// This function parses the query and recursively visits all FROM clauses,
/// JOINs, subqueries, CTEs, and set operations to collect the complete set
/// of table dependencies.
///
/// # Arguments
///
/// * `query` - The SQL query string to analyze
///
/// # Returns
///
/// A `HashSet<String>` containing all table names referenced in the query,
/// or a `ParseError` if the query cannot be parsed.
///
/// # Examples
///
/// ```text
/// use vibesql_server::subscription::extract_table_dependencies;
///
/// // Simple query
/// let deps = extract_table_dependencies("SELECT * FROM users")?;
/// assert!(deps.contains("users"));
///
/// // Query with JOIN
/// let deps = extract_table_dependencies(
///     "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
/// )?;
/// assert!(deps.contains("users"));
/// assert!(deps.contains("orders"));
/// ```
pub fn extract_table_dependencies(query: &str) -> Result<HashSet<String>, ParseError> {
    let stmt = Parser::parse_sql(query)?;
    let mut tables = HashSet::new();
    visit_statement(&stmt, &mut tables);
    Ok(tables)
}

/// Visit a statement and collect table references.
fn visit_statement(stmt: &Statement, tables: &mut HashSet<String>) {
    match stmt {
        Statement::Select(select) => visit_select(select, tables),
        Statement::Insert(insert) => {
            tables.insert(insert.table_name.clone());
            if let vibesql_ast::InsertSource::Select(select) = &insert.source {
                visit_select(select, tables);
            }
        }
        Statement::Update(update) => {
            tables.insert(update.table_name.clone());
        }
        Statement::Delete(delete) => {
            tables.insert(delete.table_name.clone());
        }
        _ => {}
    }
}

/// Visit a SELECT statement and collect table references.
fn visit_select(select: &SelectStmt, tables: &mut HashSet<String>) {
    // Visit CTEs (WITH clause)
    if let Some(ctes) = &select.with_clause {
        for cte in ctes {
            visit_select(&cte.query, tables);
        }
    }

    // Visit FROM clause
    if let Some(from) = &select.from {
        visit_from_clause(from, tables);
    }

    // Visit subqueries in select list
    for item in &select.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            visit_expression(expr, tables);
        }
    }

    // Visit subqueries in WHERE clause
    if let Some(where_clause) = &select.where_clause {
        visit_expression(where_clause, tables);
    }

    // Visit subqueries in HAVING clause
    if let Some(having) = &select.having {
        visit_expression(having, tables);
    }

    // Visit set operations (UNION, INTERSECT, EXCEPT)
    if let Some(set_op) = &select.set_operation {
        visit_select(&set_op.right, tables);
    }
}

/// Visit a FROM clause and collect table references.
fn visit_from_clause(from: &FromClause, tables: &mut HashSet<String>) {
    match from {
        FromClause::Table { name, .. } => {
            tables.insert(name.clone());
        }
        FromClause::Subquery { query, .. } => {
            visit_select(query, tables);
        }
        FromClause::Join { left, right, condition, .. } => {
            visit_from_clause(left, tables);
            visit_from_clause(right, tables);
            // Visit subqueries in join conditions
            if let Some(cond) = condition {
                visit_expression(cond, tables);
            }
        }
    }
}

/// Visit an expression and collect table references from subqueries.
fn visit_expression(expr: &vibesql_ast::Expression, tables: &mut HashSet<String>) {
    match expr {
        vibesql_ast::Expression::ScalarSubquery(select) => {
            visit_select(select, tables);
        }
        vibesql_ast::Expression::In { subquery, expr: inner, .. } => {
            visit_expression(inner, tables);
            visit_select(subquery, tables);
        }
        vibesql_ast::Expression::Exists { subquery, .. } => {
            visit_select(subquery, tables);
        }
        vibesql_ast::Expression::QuantifiedComparison { subquery, expr: inner, .. } => {
            visit_expression(inner, tables);
            visit_select(subquery, tables);
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            visit_expression(left, tables);
            visit_expression(right, tables);
        }
        vibesql_ast::Expression::Conjunction(children)
        | vibesql_ast::Expression::Disjunction(children) => {
            for child in children {
                visit_expression(child, tables);
            }
        }
        vibesql_ast::Expression::UnaryOp { expr: inner, .. } => {
            visit_expression(inner, tables);
        }
        vibesql_ast::Expression::Function { args, .. }
        | vibesql_ast::Expression::AggregateFunction { args, .. } => {
            for arg in args {
                visit_expression(arg, tables);
            }
        }
        vibesql_ast::Expression::IsNull { expr: inner, .. } => {
            visit_expression(inner, tables);
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                visit_expression(op, tables);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    visit_expression(cond, tables);
                }
                visit_expression(&clause.result, tables);
            }
            if let Some(else_expr) = else_result {
                visit_expression(else_expr, tables);
            }
        }
        vibesql_ast::Expression::InList { expr: inner, values, .. } => {
            visit_expression(inner, tables);
            for value in values {
                visit_expression(value, tables);
            }
        }
        vibesql_ast::Expression::Between { expr: inner, low, high, .. } => {
            visit_expression(inner, tables);
            visit_expression(low, tables);
            visit_expression(high, tables);
        }
        vibesql_ast::Expression::Cast { expr: inner, .. } => {
            visit_expression(inner, tables);
        }
        vibesql_ast::Expression::Like { expr: inner, pattern, .. } => {
            visit_expression(inner, tables);
            visit_expression(pattern, tables);
        }
        vibesql_ast::Expression::WindowFunction { function, over } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            for arg in args {
                visit_expression(arg, tables);
            }
            if let Some(partition_by) = &over.partition_by {
                for expr in partition_by {
                    visit_expression(expr, tables);
                }
            }
            if let Some(order_by) = &over.order_by {
                for item in order_by {
                    visit_expression(&item.expr, tables);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: The parser normalizes identifiers to uppercase per SQL standard.
    // All table name assertions use uppercase to match the parser output.

    #[test]
    fn test_simple_select() {
        let deps = extract_table_dependencies("SELECT * FROM users").unwrap();
        assert_eq!(deps.len(), 1);
        assert!(deps.contains("USERS"));
    }

    #[test]
    fn test_select_with_alias() {
        let deps = extract_table_dependencies("SELECT u.id, u.name FROM users AS u").unwrap();
        assert_eq!(deps.len(), 1);
        assert!(deps.contains("USERS"));
    }

    #[test]
    fn test_inner_join() {
        let deps = extract_table_dependencies(
            "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
    }

    #[test]
    fn test_left_join() {
        let deps = extract_table_dependencies(
            "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
    }

    #[test]
    fn test_multiple_joins() {
        let deps = extract_table_dependencies(
            "SELECT u.name, o.total, p.name AS product
             FROM users u
             JOIN orders o ON u.id = o.user_id
             JOIN products p ON o.product_id = p.id",
        )
        .unwrap();
        assert_eq!(deps.len(), 3);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
        assert!(deps.contains("PRODUCTS"));
    }

    #[test]
    fn test_subquery_in_from() {
        let deps = extract_table_dependencies(
            "SELECT * FROM (SELECT * FROM users WHERE active = TRUE) AS active_users",
        )
        .unwrap();
        assert_eq!(deps.len(), 1);
        assert!(deps.contains("USERS"));
    }

    #[test]
    fn test_subquery_in_where() {
        let deps = extract_table_dependencies(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
    }

    #[test]
    fn test_exists_subquery() {
        let deps = extract_table_dependencies(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
    }

    #[test]
    fn test_cte_simple() {
        let deps = extract_table_dependencies(
            "WITH active_users AS (SELECT * FROM users WHERE active = TRUE)
             SELECT * FROM active_users",
        )
        .unwrap();
        // CTE defines ACTIVE_USERS, but it references USERS table
        // The CTE name is also included since it appears in FROM clause
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ACTIVE_USERS"));
    }

    #[test]
    fn test_cte_multiple() {
        let deps = extract_table_dependencies(
            "WITH
             active_users AS (SELECT * FROM users WHERE active = TRUE),
             recent_orders AS (SELECT * FROM orders WHERE order_date > '2024-01-01')
             SELECT u.name, o.total
             FROM active_users u
             JOIN recent_orders o ON u.id = o.user_id",
        )
        .unwrap();
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
    }

    #[test]
    fn test_union() {
        let deps = extract_table_dependencies(
            "SELECT id, name FROM users UNION SELECT id, name FROM admins",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ADMINS"));
    }

    #[test]
    fn test_union_all() {
        let deps = extract_table_dependencies(
            "SELECT id, name FROM users UNION ALL SELECT id, name FROM guests",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("GUESTS"));
    }

    #[test]
    fn test_intersect() {
        let deps = extract_table_dependencies(
            "SELECT id FROM users INTERSECT SELECT user_id AS id FROM premium_members",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("PREMIUM_MEMBERS"));
    }

    #[test]
    fn test_except() {
        let deps = extract_table_dependencies(
            "SELECT id FROM users EXCEPT SELECT user_id AS id FROM banned_users",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("BANNED_USERS"));
    }

    #[test]
    fn test_scalar_subquery_in_select() {
        let deps = extract_table_dependencies(
            "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
    }

    #[test]
    fn test_complex_nested_query() {
        let deps = extract_table_dependencies(
            "WITH recent_orders AS (
                SELECT user_id, SUM(total) as total_spent
                FROM orders
                WHERE order_date > '2024-01-01'
                GROUP BY user_id
             )
             SELECT u.name, ro.total_spent,
                    (SELECT AVG(rating) FROM reviews r WHERE r.user_id = u.id) as avg_rating
             FROM users u
             LEFT JOIN recent_orders ro ON u.id = ro.user_id
             WHERE u.id IN (SELECT user_id FROM premium_members)
             ORDER BY ro.total_spent DESC",
        )
        .unwrap();
        assert!(deps.contains("USERS"));
        assert!(deps.contains("ORDERS"));
        assert!(deps.contains("REVIEWS"));
        assert!(deps.contains("PREMIUM_MEMBERS"));
    }

    #[test]
    fn test_cross_join() {
        let deps =
            extract_table_dependencies("SELECT * FROM products CROSS JOIN categories").unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("PRODUCTS"));
        assert!(deps.contains("CATEGORIES"));
    }

    #[test]
    fn test_self_join() {
        let deps = extract_table_dependencies(
            "SELECT e1.name, e2.name as manager
             FROM employees e1
             JOIN employees e2 ON e1.manager_id = e2.id",
        )
        .unwrap();
        // Self-join still only references one table
        assert_eq!(deps.len(), 1);
        assert!(deps.contains("EMPLOYEES"));
    }

    #[test]
    fn test_insert_statement() {
        let deps = extract_table_dependencies("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        assert_eq!(deps.len(), 1);
        assert!(deps.contains("USERS"));
    }

    #[test]
    fn test_insert_select() {
        let deps = extract_table_dependencies(
            "INSERT INTO archive_users SELECT * FROM users WHERE active = FALSE",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("ARCHIVE_USERS"));
        assert!(deps.contains("USERS"));
    }

    #[test]
    fn test_update_statement() {
        let deps =
            extract_table_dependencies("UPDATE users SET active = FALSE WHERE id = 1").unwrap();
        assert_eq!(deps.len(), 1);
        assert!(deps.contains("USERS"));
    }

    #[test]
    fn test_delete_statement() {
        let deps = extract_table_dependencies("DELETE FROM users WHERE id = 1").unwrap();
        assert_eq!(deps.len(), 1);
        assert!(deps.contains("USERS"));
    }

    #[test]
    fn test_empty_query_error() {
        let result = extract_table_dependencies("");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_query_error() {
        let result = extract_table_dependencies("SELECT FROM WHERE");
        assert!(result.is_err());
    }

    #[test]
    fn test_case_expression_with_subquery() {
        let deps = extract_table_dependencies(
            "SELECT id,
                    CASE WHEN id IN (SELECT user_id FROM premium_members) THEN 'premium'
                         ELSE 'regular'
                    END as tier
             FROM users",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("USERS"));
        assert!(deps.contains("PREMIUM_MEMBERS"));
    }

    #[test]
    fn test_quantified_comparison() {
        let deps = extract_table_dependencies(
            "SELECT * FROM products WHERE price > ALL (SELECT avg_price FROM price_history)",
        )
        .unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("PRODUCTS"));
        assert!(deps.contains("PRICE_HISTORY"));
    }

    #[test]
    fn test_case_insensitive_lookup() {
        // Verify that the function can be used with case-insensitive matching
        let deps = extract_table_dependencies("SELECT * FROM Users").unwrap();
        // Parser normalizes to uppercase
        assert!(deps.contains("USERS"));
        // Users can do case-insensitive matching by uppercasing their search term
        assert!(deps.iter().any(|t| t.eq_ignore_ascii_case("users")));
    }
}
