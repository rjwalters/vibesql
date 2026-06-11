//! Subquery rewriting optimization for IN predicates and scalar subqueries
//!
//! This module implements subquery optimization:
//! - Phase 2 of IN subquery optimization (issue #2138):
//!   - Rewrites correlated IN subqueries to EXISTS with LIMIT 1 for early termination
//!   - Adds DISTINCT to uncorrelated IN subqueries to reduce duplicate processing
//! - Scalar subquery decorrelation (issue #4760):
//!   - Transforms correlated scalar subqueries with aggregates to CTE + JOIN
//!   - Enables hash-based join execution instead of row-by-row subquery evaluation
//!
//! These optimizations work in conjunction with Phase 1 (HashSet optimization, #2136)
//! to provide 5-50x speedup for IN subqueries, and 100-1000x speedup for correlated
//! scalar aggregate subqueries (e.g., TPC-DS Q6).
//!
//! ## Module Organization
//!
//! - `detection`: Fast detection of IN subqueries in SQL AST
//! - `correlation`: Analysis of whether subqueries reference outer columns
//! - `transformations`: Core optimization transformations (IN→EXISTS, DISTINCT)
//! - `expression`: Recursive expression rewriting throughout the AST
//! - `scalar_decorrelation`: Transform correlated scalar aggregate subqueries to CTE + JOIN

use vibesql_ast::{SelectItem, SelectStmt};

pub(crate) mod correlation;
mod detection;
mod expression;
pub mod scalar_decorrelation;
mod transformations;

use detection::has_in_subqueries;
use expression::{rewrite_expression_with_context, rewrite_from_clause};

/// Rewrite a SELECT statement to optimize IN subqueries
///
/// Applies two optimizations:
/// 1. Correlated IN → EXISTS: Better leverages indexes and allows early termination
/// 2. Uncorrelated IN → IN with DISTINCT: Reduces duplicate comparisons
///
/// # Examples
///
/// ```sql
/// -- Before: Correlated IN
/// SELECT * FROM orders WHERE customer_id IN (
///   SELECT customer_id FROM customers WHERE region = 'APAC'
/// )
///
/// -- After: Rewritten to EXISTS
/// SELECT * FROM orders WHERE EXISTS (
///   SELECT 1 FROM customers
///   WHERE region = 'APAC' AND customers.customer_id = orders.customer_id
///   LIMIT 1
/// )
/// ```
///
/// ```sql
/// -- Before: Uncorrelated IN without DISTINCT
/// SELECT * FROM orders WHERE status IN (SELECT status FROM valid_statuses)
///
/// -- After: Added DISTINCT to reduce comparisons
/// SELECT * FROM orders WHERE status IN (SELECT DISTINCT status FROM valid_statuses)
/// ```
pub fn rewrite_subquery_optimizations(stmt: &SelectStmt) -> SelectStmt {
    // Early exit: Skip expensive AST cloning if no IN subqueries present
    // This avoids performance overhead for queries without IN predicates
    // Note: We also need to check for EXISTS subqueries that can be decorrelated
    if !has_in_subqueries(stmt) && !has_exists_subqueries(stmt) {
        return stmt.clone();
    }

    // Extract outer table names for EXISTS decorrelation
    let outer_tables = extract_table_names(&stmt.from);

    let mut rewritten = stmt.clone();

    // Rewrite WHERE clause
    if let Some(where_clause) = &stmt.where_clause {
        rewritten.where_clause = Some(rewrite_expression_with_context(
            where_clause,
            &rewrite_subquery_optimizations,
            &outer_tables,
        ));
    }

    // Rewrite HAVING clause
    if let Some(having) = &stmt.having {
        rewritten.having = Some(rewrite_expression_with_context(
            having,
            &rewrite_subquery_optimizations,
            &outer_tables,
        ));
    }

    // Rewrite SELECT list expressions
    rewritten.select_list = stmt
        .select_list
        .iter()
        .map(|item| match item {
            SelectItem::Expression { expr, alias, .. } => SelectItem::Expression {
                expr: rewrite_expression_with_context(
                    expr,
                    &rewrite_subquery_optimizations,
                    &outer_tables,
                ),
                alias: alias.clone(),
                source_text: None,
            },
            other => other.clone(),
        })
        .collect();

    // Recursively rewrite subqueries in FROM clause
    if let Some(from) = &stmt.from {
        rewritten.from = Some(rewrite_from_clause(from, &rewrite_subquery_optimizations));
    }

    // Recursively rewrite set operations
    if let Some(set_op) = &stmt.set_operation {
        let mut new_set_op = set_op.clone();
        *new_set_op.right = rewrite_subquery_optimizations(&set_op.right);
        rewritten.set_operation = Some(new_set_op);
    }

    rewritten
}

/// Check if a statement contains EXISTS subqueries
fn has_exists_subqueries(stmt: &SelectStmt) -> bool {
    use vibesql_ast::Expression;

    fn expr_has_exists(expr: &Expression) -> bool {
        match expr {
            Expression::Exists { .. } => true,
            Expression::BinaryOp { left, right, .. } => {
                expr_has_exists(left) || expr_has_exists(right)
            }
            Expression::UnaryOp { expr, .. } => expr_has_exists(expr),
            Expression::IsNull { expr, .. } => expr_has_exists(expr),
            Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().is_some_and(|e| expr_has_exists(e))
                    || when_clauses.iter().any(|c| {
                        c.conditions.iter().any(expr_has_exists) || expr_has_exists(&c.result)
                    })
                    || else_result.as_ref().is_some_and(|e| expr_has_exists(e))
            }
            _ => false,
        }
    }

    if let Some(where_clause) = &stmt.where_clause {
        if expr_has_exists(where_clause) {
            return true;
        }
    }
    if let Some(having) = &stmt.having {
        if expr_has_exists(having) {
            return true;
        }
    }
    false
}

/// Extract table names from a FROM clause
fn extract_table_names(from: &Option<vibesql_ast::FromClause>) -> Vec<String> {
    fn collect_tables(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
        match from {
            vibesql_ast::FromClause::Table { name, alias, .. } => {
                // Use alias if present, otherwise use table name
                tables.push(alias.clone().unwrap_or_else(|| name.clone()));
                // Also add the original name when aliased, so qualified refs
                // using either form match. Avoid pushing duplicates for
                // unaliased tables: rewrite_expression_with_context uses the
                // entry count to detect multi-relation FROM clauses.
                if alias.is_some() {
                    tables.push(name.clone());
                }
            }
            vibesql_ast::FromClause::Join { left, right, .. } => {
                collect_tables(left, tables);
                collect_tables(right, tables);
            }
            vibesql_ast::FromClause::Subquery { alias, .. } => {
                tables.push(alias.clone());
            }
            vibesql_ast::FromClause::Values { alias, .. } => {
                tables.push(alias.clone());
            }
        }
    }

    let mut tables = Vec::new();
    if let Some(from_clause) = from {
        collect_tables(from_clause, &mut tables);
    }
    tables
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_types::SqlValue;

    use super::*;

    /// Helper to create a simple SELECT statement for testing
    fn simple_select(table: &str, column: &str) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(&column, false)),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(vibesql_ast::FromClause::Table {
                index_hint: None,
                name: table.to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    #[test]
    fn test_add_distinct_to_uncorrelated_in_subquery() {
        let subquery = simple_select("customers", "region");
        let in_expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("region", false));

        let mut stmt = simple_select("orders", "order_id");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(in_expr),
            subquery: Box::new(subquery),
            negated: false,
        });

        let optimized = rewrite_subquery_optimizations(&stmt);

        // Extract the IN expression from WHERE clause
        if let Some(Expression::In { subquery: optimized_subquery, .. }) = &optimized.where_clause {
            assert!(
                optimized_subquery.distinct,
                "DISTINCT should be added to uncorrelated subquery"
            );
        } else {
            panic!("Expected IN expression in WHERE clause");
        }
    }

    #[test]
    fn test_distinct_not_duplicated() {
        let mut subquery = simple_select("customers", "region");
        subquery.distinct = true;

        let in_expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("region", false));

        let mut stmt = simple_select("orders", "order_id");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(in_expr),
            subquery: Box::new(subquery),
            negated: false,
        });

        let optimized = rewrite_subquery_optimizations(&stmt);

        // Extract the IN expression from WHERE clause
        if let Some(Expression::In { subquery: optimized_subquery, .. }) = &optimized.where_clause {
            assert!(optimized_subquery.distinct, "DISTINCT should remain true");
        } else {
            panic!("Expected IN expression in WHERE clause");
        }
    }

    #[test]
    fn test_uncorrelated_subquery_detection() {
        let subquery = simple_select("customers", "region");
        let correlated = correlation::is_correlated(&subquery);

        assert!(!correlated, "Simple subquery should be uncorrelated");
    }

    #[test]
    fn test_correlated_subquery_detection() {
        let mut subquery = simple_select("customers", "customer_id");

        // Add WHERE clause with qualified column reference to outer table
        subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "customers",
                false,
                "region",
                false,
            ))),
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "orders", false, "region", false,
            ))),
        });

        let correlated = correlation::is_correlated(&subquery);

        assert!(correlated, "Subquery with qualified external column ref should be correlated");
    }

    #[test]
    fn test_in_to_exists_rewrite() {
        let in_expr =
            Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("customer_id", false));

        let mut subquery = simple_select("customers", "customer_id");
        // Make it correlated
        subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "customers",
                false,
                "region",
                false,
            ))),
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "orders", false, "region", false,
            ))),
        });

        let mut stmt = simple_select("orders", "order_id");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(in_expr),
            subquery: Box::new(subquery),
            negated: false,
        });

        let rewritten = rewrite_subquery_optimizations(&stmt);

        match &rewritten.where_clause {
            Some(Expression::Exists { subquery: exists_subquery, negated }) => {
                assert!(!negated, "Negation should match input");
                assert_eq!(
                    exists_subquery.limit,
                    Some(Expression::Literal(SqlValue::Integer(1))),
                    "LIMIT 1 should be added"
                );
                assert!(
                    exists_subquery.where_clause.is_some(),
                    "Correlation predicate should be added"
                );

                // Check that SELECT list is rewritten to SELECT 1
                assert_eq!(exists_subquery.select_list.len(), 1);
                if let SelectItem::Expression { expr, .. } = &exists_subquery.select_list[0] {
                    assert!(
                        matches!(expr, Expression::Literal(SqlValue::Integer(1))),
                        "SELECT list should be rewritten to SELECT 1"
                    );
                }
            }
            _ => panic!("Expected EXISTS expression"),
        }
    }

    #[test]
    fn test_complex_expression_skips_in_to_exists() {
        // Test that complex expressions in SELECT list skip IN → EXISTS transformation
        let in_expr =
            Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("customer_id", false));

        let mut subquery = simple_select("customers", "customer_id");
        // Add WHERE clause to make it correlated
        subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "customers",
                false,
                "region",
                false,
            ))),
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "orders", false, "region", false,
            ))),
        });

        // Replace SELECT with complex expression
        subquery.select_list = vec![SelectItem::Expression {
            expr: Expression::Function {
                name: vibesql_ast::FunctionIdentifier::new("UPPER"),
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "customer_id",
                    false,
                ))],
                character_unit: None,
            },
            alias: None,
            source_text: None,
        }];

        let mut stmt = simple_select("orders", "order_id");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(in_expr),
            subquery: Box::new(subquery),
            negated: false,
        });

        let result = rewrite_subquery_optimizations(&stmt);

        // Should remain as IN (not converted to EXISTS) but with DISTINCT added
        match &result.where_clause {
            Some(Expression::In { subquery: optimized_subquery, .. }) => {
                assert!(
                    optimized_subquery.distinct,
                    "Complex expression should get DISTINCT but not convert to EXISTS"
                );
            }
            Some(Expression::Exists { .. }) => {
                panic!("Complex expression should NOT be converted to EXISTS")
            }
            _ => panic!("Expected IN expression"),
        }
    }

    #[test]
    fn test_multi_column_in_skips_optimization() {
        // Test that multi-column IN subqueries are not optimized
        let in_expr =
            Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("customer_id", false));

        let mut subquery = simple_select("customers", "customer_id");
        // Add second column to SELECT list
        subquery.select_list.push(SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("region", false)),
            alias: None,
            source_text: None,
        });

        let mut stmt = simple_select("orders", "order_id");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(in_expr),
            subquery: Box::new(subquery),
            negated: false,
        });

        let result = rewrite_subquery_optimizations(&stmt);

        // Should remain as IN with no DISTINCT (since it's multi-column)
        match &result.where_clause {
            Some(Expression::In { subquery: optimized_subquery, .. }) => {
                assert_eq!(
                    optimized_subquery.select_list.len(),
                    2,
                    "Multi-column IN should preserve both columns"
                );
                // Note: We recursively optimize, so DISTINCT might be added
                // But the important part is it stays as IN, not EXISTS
            }
            Some(Expression::Exists { .. }) => {
                panic!("Multi-column IN should NOT be converted to EXISTS")
            }
            _ => panic!("Expected IN expression"),
        }
    }

    #[test]
    fn test_negated_in_preserved() {
        // Test that NOT IN negation is preserved
        let in_expr =
            Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("customer_id", false));

        let mut subquery = simple_select("customers", "customer_id");
        // Make it correlated
        subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "customers",
                false,
                "region",
                false,
            ))),
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "orders", false, "region", false,
            ))),
        });

        let mut stmt = simple_select("orders", "order_id");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(in_expr),
            subquery: Box::new(subquery),
            negated: true,
        });

        let rewritten = rewrite_subquery_optimizations(&stmt);

        match &rewritten.where_clause {
            Some(Expression::Exists { negated, .. }) => {
                assert!(negated, "NOT IN should preserve negation as NOT EXISTS");
            }
            _ => panic!("Expected EXISTS expression"),
        }
    }

    #[test]
    fn test_nested_in_subqueries() {
        // Test that nested IN subqueries are recursively optimized
        let mut outer_subquery = simple_select("customers", "customer_id");

        // Add nested IN subquery in WHERE clause
        let inner_subquery = simple_select("regions", "region_id");
        outer_subquery.where_clause = Some(Expression::In {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "region_id",
                false,
            ))),
            subquery: Box::new(inner_subquery),
            negated: false,
        });

        let in_expr =
            Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("customer_id", false));

        let mut stmt = simple_select("orders", "order_id");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(in_expr),
            subquery: Box::new(outer_subquery),
            negated: false,
        });

        let optimized = rewrite_subquery_optimizations(&stmt);

        // Outer subquery should have DISTINCT
        if let Some(Expression::In { subquery: outer_optimized, .. }) = &optimized.where_clause {
            assert!(outer_optimized.distinct, "Outer subquery should have DISTINCT");

            // Inner subquery should also be optimized
            if let Some(Expression::In { subquery: inner_optimized, .. }) =
                &outer_optimized.where_clause
            {
                assert!(inner_optimized.distinct, "Nested IN subquery should also have DISTINCT");
            } else {
                panic!("Expected nested IN subquery in WHERE clause");
            }
        } else {
            panic!("Expected IN expression in WHERE clause");
        }
    }

    #[test]
    fn test_early_exit_for_queries_without_in() {
        // Test that queries without IN subqueries skip expensive rewriting
        let stmt = simple_select("customers", "customer_id");

        // This should not panic and should return quickly
        let result = rewrite_subquery_optimizations(&stmt);

        // Should be essentially unchanged (just cloned)
        assert_eq!(result.select_list.len(), stmt.select_list.len());
        assert_eq!(result.distinct, stmt.distinct);
    }

    #[test]
    fn test_has_in_subqueries_detection() {
        // Test the early-exit detection function
        let stmt_without_in = simple_select("customers", "customer_id");
        assert!(!has_in_subqueries(&stmt_without_in), "Should detect no IN subqueries");

        let mut stmt_with_in = simple_select("orders", "order_id");
        stmt_with_in.where_clause = Some(Expression::In {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "customer_id",
                false,
            ))),
            subquery: Box::new(simple_select("customers", "customer_id")),
            negated: false,
        });

        assert!(has_in_subqueries(&stmt_with_in), "Should detect IN subquery");
    }

    /// Helper to create a SELECT statement with alias
    fn simple_select_with_alias(table: &str, alias: &str, column: &str) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    alias, false, column, false,
                )),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(vibesql_ast::FromClause::Table {
                index_hint: None,
                name: table.to_string(),
                alias: Some(alias.to_string()),
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    #[test]
    fn test_exists_decorrelation_with_alias() {
        // Test EXISTS subquery decorrelation when inner table has alias
        // This is the Q21 pattern: EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey =
        // l1.l_orderkey)
        let mut subquery = simple_select_with_alias("lineitem", "l2", "l_orderkey");
        // Add correlation predicate: l2.l_orderkey = l1.l_orderkey
        subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "l2",
                false,
                "l_orderkey",
                false,
            ))),
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "l1",
                false,
                "l_orderkey",
                false,
            ))),
        });

        // Outer query: SELECT * FROM lineitem l1
        let mut stmt = simple_select_with_alias("lineitem", "l1", "l_orderkey");
        stmt.where_clause =
            Some(Expression::Exists { subquery: Box::new(subquery), negated: false });

        let rewritten = rewrite_subquery_optimizations(&stmt);

        // EXISTS should be decorrelated to IN
        match &rewritten.where_clause {
            Some(Expression::In { expr, subquery, negated }) => {
                assert!(!negated, "Should not be negated");
                // The outer expression should be l1.l_orderkey
                match expr.as_ref() {
                    Expression::ColumnRef(col_id)
                        if col_id.schema_canonical().is_none()
                            && col_id.table_canonical().is_some() =>
                    {
                        assert_eq!(col_id.table_canonical().unwrap(), "l1");
                        assert_eq!(col_id.column_canonical(), "l_orderkey");
                    }
                    _ => panic!("Expected ColumnRef for outer expression"),
                }
                // Subquery should have DISTINCT and select l2.l_orderkey
                assert!(subquery.distinct, "Decorrelated subquery should have DISTINCT");
            }
            Some(Expression::Exists { .. }) => {
                panic!("EXISTS should have been decorrelated to IN");
            }
            _ => panic!("Expected IN expression after decorrelation"),
        }
    }

    #[test]
    fn test_not_exists_decorrelation_with_alias() {
        // Test NOT EXISTS subquery decorrelation
        // Pattern: NOT EXISTS (SELECT * FROM orders o WHERE o.o_custkey = c.c_custkey)
        let mut subquery = simple_select_with_alias("orders", "o", "o_custkey");
        subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "o",
                false,
                "o_custkey",
                false,
            ))),
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "c",
                false,
                "c_custkey",
                false,
            ))),
        });

        let mut stmt = simple_select_with_alias("customer", "c", "c_custkey");
        stmt.where_clause = Some(Expression::Exists {
            subquery: Box::new(subquery),
            negated: true, // NOT EXISTS
        });

        let rewritten = rewrite_subquery_optimizations(&stmt);

        // NOT EXISTS should be decorrelated to NOT IN
        match &rewritten.where_clause {
            Some(Expression::In { negated, .. }) => {
                assert!(*negated, "NOT EXISTS should become NOT IN");
            }
            Some(Expression::Exists { negated, .. }) => {
                panic!("NOT EXISTS should have been decorrelated to NOT IN, but got EXISTS (negated={})", negated);
            }
            _ => panic!("Expected IN expression after decorrelation"),
        }
    }
}
