//! Table reference extraction from SQL AST
//!
//! This module provides functionality to extract all table names referenced
//! in a SQL query. This is used by the subscription manager to determine
//! which subscriptions might be affected by a change to a specific table.

use std::collections::HashSet;

use vibesql_ast::visitor::{walk_expression, ExpressionVisitor, VisitResult};
use vibesql_ast::{Expression, FromClause, SelectStmt, Statement};

// ============================================================================
// Table Extractor Visitor
// ============================================================================

/// Visitor that collects table names from a SQL statement
struct TableExtractor {
    /// Collected table names
    tables: HashSet<String>,
}

impl TableExtractor {
    fn new() -> Self {
        Self { tables: HashSet::new() }
    }

    /// Extract tables from a FROM clause recursively
    fn extract_from_clause(&mut self, from: &FromClause) {
        match from {
            FromClause::Table { name, .. } => {
                // Normalize table name to lowercase for consistent matching
                self.tables.insert(name.to_lowercase());
            }
            FromClause::Subquery { query, .. } => {
                // Recursively extract tables from subquery
                self.extract_select(query);
            }
            FromClause::Join { left, right, condition, .. } => {
                // Extract from both sides of the join
                self.extract_from_clause(left);
                self.extract_from_clause(right);
                // Also check join condition for subqueries
                if let Some(cond) = condition {
                    walk_expression(self, cond);
                }
            }
        }
    }

    /// Extract tables from a SELECT statement
    fn extract_select(&mut self, stmt: &SelectStmt) {
        // Extract from CTEs
        if let Some(ctes) = &stmt.with_clause {
            for cte in ctes {
                self.extract_select(&cte.query);
            }
        }

        // Extract from FROM clause
        if let Some(from) = &stmt.from {
            self.extract_from_clause(from);
        }

        // Walk select items for subqueries
        for item in &stmt.select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                walk_expression(self, expr);
            }
        }

        // Walk WHERE clause for subqueries
        if let Some(where_clause) = &stmt.where_clause {
            walk_expression(self, where_clause);
        }

        // Walk HAVING clause for subqueries
        if let Some(having) = &stmt.having {
            walk_expression(self, having);
        }

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        if let Some(set_op) = &stmt.set_operation {
            self.extract_select(&set_op.right);
        }
    }

    /// Extract tables from a subquery-containing expression
    fn extract_subquery(&mut self, select: &SelectStmt) {
        self.extract_select(select);
    }
}

impl ExpressionVisitor for TableExtractor {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        // Handle subquery expressions by extracting their tables
        match expr {
            Expression::ScalarSubquery(select) => {
                self.extract_subquery(select);
            }
            Expression::In { subquery, .. } => {
                self.extract_subquery(subquery);
            }
            Expression::Exists { subquery, .. } => {
                self.extract_subquery(subquery);
            }
            Expression::QuantifiedComparison { subquery, .. } => {
                self.extract_subquery(subquery);
            }
            _ => {}
        }
        VisitResult::Continue
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Extract all table names referenced in a SQL statement
///
/// This function parses the SQL and walks the AST to find all table references,
/// including those in:
/// - FROM clauses
/// - JOIN clauses
/// - Subqueries (in SELECT list, WHERE, etc.)
/// - Common Table Expressions (CTEs)
/// - Set operations (UNION, INTERSECT, EXCEPT)
///
/// # Arguments
///
/// * `stmt` - The parsed SQL statement
///
/// # Returns
///
/// A set of table names (lowercase for case-insensitive matching)
///
/// # Example
///
/// ```text
/// use vibesql_parser::Parser;
/// use vibesql_server::subscription::extract_table_refs;
///
/// let stmt = Parser::parse_sql("SELECT * FROM users u JOIN orders o ON u.id = o.user_id")?;
/// let tables = extract_table_refs(&stmt);
/// assert!(tables.contains("users"));
/// assert!(tables.contains("orders"));
/// ```
pub fn extract_table_refs(stmt: &Statement) -> HashSet<String> {
    let mut extractor = TableExtractor::new();

    match stmt {
        Statement::Select(select) => {
            extractor.extract_select(select);
        }
        Statement::Insert(insert) => {
            // INSERT targets a single table, but may have a SELECT source
            extractor.tables.insert(insert.table_name.to_lowercase());
            if let vibesql_ast::InsertSource::Select(select) = &insert.source {
                extractor.extract_select(select);
            }
        }
        Statement::Update(update) => {
            extractor.tables.insert(update.table_name.to_lowercase());
            // UPDATE may have subqueries in WHERE or assignment values
            // The visitor will handle these if we implement it
        }
        Statement::Delete(delete) => {
            extractor.tables.insert(delete.table_name.to_lowercase());
            // DELETE may have subqueries in WHERE
        }
        // DDL statements don't need subscription tracking
        _ => {}
    }

    extractor.tables
}

/// Extract tables from a query string
///
/// Convenience function that parses the SQL and extracts table references.
///
/// # Arguments
///
/// * `sql` - The SQL query string
///
/// # Returns
///
/// `Ok(tables)` with the set of referenced tables, or `Err` if parsing fails
#[allow(dead_code)]
pub fn extract_tables_from_sql(sql: &str) -> Result<HashSet<String>, String> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).map_err(|e| e.to_string())?;
    Ok(extract_table_refs(&stmt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let tables = extract_tables_from_sql("SELECT * FROM users").unwrap();
        assert_eq!(tables.len(), 1);
        assert!(tables.contains("users"));
    }

    #[test]
    fn test_select_with_join() {
        let tables =
            extract_tables_from_sql("SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
                .unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_select_with_multiple_joins() {
        let tables = extract_tables_from_sql(
            "SELECT * FROM users u
             JOIN orders o ON u.id = o.user_id
             LEFT JOIN products p ON o.product_id = p.id",
        )
        .unwrap();
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
        assert!(tables.contains("products"));
    }

    #[test]
    fn test_select_with_subquery_in_from() {
        let tables = extract_tables_from_sql(
            "SELECT * FROM (SELECT * FROM users WHERE active = TRUE) AS active_users",
        )
        .unwrap();
        assert!(tables.contains("users"));
    }

    #[test]
    fn test_select_with_where_subquery() {
        let tables = extract_tables_from_sql(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)",
        )
        .unwrap();
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_select_with_cte() {
        let tables = extract_tables_from_sql(
            "WITH recent_orders AS (SELECT * FROM orders WHERE amount > 100)
             SELECT * FROM users u JOIN recent_orders ro ON u.id = ro.user_id",
        )
        .unwrap();
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_select_with_union() {
        let tables =
            extract_tables_from_sql("SELECT id, name FROM users UNION SELECT id, name FROM admins")
                .unwrap();
        assert!(tables.contains("users"));
        assert!(tables.contains("admins"));
    }

    #[test]
    fn test_case_insensitive() {
        let tables1 = extract_tables_from_sql("SELECT * FROM Users").unwrap();
        let tables2 = extract_tables_from_sql("SELECT * FROM USERS").unwrap();
        let tables3 = extract_tables_from_sql("SELECT * FROM users").unwrap();

        // All should produce the same lowercase table name
        assert!(tables1.contains("users"));
        assert!(tables2.contains("users"));
        assert!(tables3.contains("users"));
    }

    #[test]
    fn test_insert_statement() {
        let tables =
            extract_tables_from_sql("INSERT INTO orders (user_id) SELECT id FROM users").unwrap();
        assert!(tables.contains("orders"));
        assert!(tables.contains("users"));
    }

    #[test]
    fn test_update_statement() {
        let tables =
            extract_tables_from_sql("UPDATE users SET active = FALSE WHERE id = 1").unwrap();
        assert!(tables.contains("users"));
    }

    #[test]
    fn test_delete_statement() {
        let tables = extract_tables_from_sql("DELETE FROM users WHERE id = 1").unwrap();
        assert!(tables.contains("users"));
    }

    #[test]
    fn test_exists_subquery() {
        let tables = extract_tables_from_sql(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
        )
        .unwrap();
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_scalar_subquery() {
        let tables = extract_tables_from_sql(
            "SELECT u.*, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u",
        )
        .unwrap();
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_invalid_sql() {
        let result = extract_tables_from_sql("SELECT * FROM");
        assert!(result.is_err());
    }
}
