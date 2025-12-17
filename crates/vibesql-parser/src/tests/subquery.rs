//! Tests for subquery parsing (both IN and scalar subqueries)

use vibesql_ast::Expression;

use crate::Parser;

// ============================================================================
// IN Operator Subquery Tests (from PR #96)
// ============================================================================

#[test]
fn test_parse_in_subquery() {
    let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Check WHERE clause contains IN expression
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery, negated } => {
                    assert!(!negated);
                    // Check left expression is 'id'
                    match *expr {
                        Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();
                            assert_eq!(table, None);
                            assert_eq!(column, "id");
                        }
                        _ => panic!("Expected ColumnRef"),
                    }
                    // Check subquery structure
                    assert_eq!(subquery.select_list.len(), 1);
                }
                _ => panic!("Expected IN expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_not_in_subquery() {
    let sql = "SELECT * FROM users WHERE status NOT IN (SELECT blocked_status FROM config)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Check WHERE clause contains NOT IN expression
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery: _, negated } => {
                    assert!(negated); // Should be negated
                                      // Check left expression is 'status'
                    match *expr {
                        Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();
                            assert_eq!(table, None);
                            assert_eq!(column, "status");
                        }
                        _ => panic!("Expected ColumnRef"),
                    }
                }
                _ => panic!("Expected IN expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_in_subquery_simple_column() {
    // Simpler test - just ensure IN works with a single column
    let sql = "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery: _, negated } => {
                    assert!(!negated);
                    match *expr {
                        Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();
                            assert_eq!(table, None);
                            assert_eq!(column, "user_id");
                        }
                        _ => panic!("Expected ColumnRef, got {:?}", expr),
                    }
                }
                other => panic!("Expected IN expression, got {:?}", other),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ============================================================================
// Scalar Subquery Tests (from PR #100)
// ============================================================================

#[test]
fn test_parse_scalar_subquery_simple() {
    // Test: (SELECT 1)
    let sql = "SELECT (SELECT 1)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    // Should be a ScalarSubquery
                    match expr {
                        Expression::ScalarSubquery(subquery) => {
                            // Verify the subquery structure
                            assert_eq!(subquery.select_list.len(), 1);
                            match &subquery.select_list[0] {
                                vibesql_ast::SelectItem::Expression { expr, .. } => {
                                    match expr {
                                        Expression::Literal(_) => {
                                            // Expected literal 1
                                        }
                                        _ => panic!("Expected literal in subquery"),
                                    }
                                }
                                _ => panic!("Expected expression in subquery select list"),
                            }
                        }
                        _ => panic!("Expected ScalarSubquery, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression in select list"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_scalar_subquery_in_where() {
    // Test: WHERE x > (SELECT AVG(y) FROM t)
    let sql = "SELECT * FROM users WHERE salary > (SELECT AVG(salary) FROM employees)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Check WHERE clause
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::BinaryOp { op: _, left: _, right } => {
                    // Right side should be the scalar subquery
                    match *right {
                        Expression::ScalarSubquery(subquery) => {
                            // Verify it's selecting AVG
                            assert_eq!(subquery.select_list.len(), 1);
                            match &subquery.select_list[0] {
                                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                                    Expression::AggregateFunction { name, .. } => {
                                        assert_eq!(name, "avg");
                                    }
                                    _ => panic!("Expected aggregate function call in subquery"),
                                },
                                _ => panic!("Expected expression in subquery"),
                            }
                        }
                        _ => panic!("Expected ScalarSubquery on right side of comparison"),
                    }
                }
                _ => panic!("Expected binary operation in WHERE clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_scalar_subquery_in_select() {
    // Test: SELECT id, (SELECT COUNT(*) FROM t2) FROM t1
    let sql = "SELECT id, (SELECT COUNT(*) FROM orders) FROM users";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should have 2 items in select list
            assert_eq!(select.select_list.len(), 2);

            // First should be id column
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    Expression::ColumnRef(col_id) => {
            let column = col_id.column_canonical();
                        assert_eq!(column, "id");
                    }
                    _ => panic!("Expected column reference"),
                },
                _ => panic!("Expected expression"),
            }

            // Second should be scalar subquery
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    Expression::ScalarSubquery(subquery) => {
                        // Verify it's COUNT(*)
                        assert_eq!(subquery.select_list.len(), 1);
                        match &subquery.select_list[0] {
                            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                                Expression::AggregateFunction { name, .. } => {
                                    assert_eq!(name, "count");
                                }
                                _ => panic!("Expected aggregate function"),
                            },
                            _ => panic!("Expected expression"),
                        }
                    }
                    _ => panic!("Expected ScalarSubquery, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ============================================================================
// Derived Table (Subquery in FROM) Tests - SQLite Compatibility
// Issue #4229: Allow derived tables without explicit aliases
// ============================================================================

#[test]
fn test_parse_derived_table_without_alias() {
    // SQLite allows derived tables without explicit aliases
    let sql = "SELECT * FROM (SELECT 1)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // FROM clause should be a Subquery with auto-generated alias
            match &select.from {
                Some(vibesql_ast::FromClause::Subquery { alias, .. }) => {
                    // Alias should start with (subquery-
                    assert!(
                        alias.starts_with("(subquery-"),
                        "Expected auto-generated alias starting with (subquery-, got: {}",
                        alias
                    );
                }
                _ => panic!("Expected Subquery in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_derived_table_with_explicit_alias() {
    // Explicit aliases should still work
    let sql = "SELECT * FROM (SELECT 1) AS subq";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.from {
            Some(vibesql_ast::FromClause::Subquery { alias, .. }) => {
                assert_eq!(alias, "subq");
            }
            _ => panic!("Expected Subquery in FROM clause"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_derived_table_comma_join_without_alias() {
    // From select1-17.1: SELECT * FROM t1,(SELECT * FROM t2 WHERE y=2 ORDER BY y,z);
    let sql = "SELECT * FROM t1, (SELECT * FROM t2 WHERE y=2 ORDER BY y, z)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should have a join in FROM clause
            match &select.from {
                Some(vibesql_ast::FromClause::Join { right, .. }) => {
                    // Right side should be the derived table
                    match right.as_ref() {
                        vibesql_ast::FromClause::Subquery { alias, .. } => {
                            assert!(
                                alias.starts_with("(subquery-"),
                                "Expected auto-generated alias, got: {}",
                                alias
                            );
                        }
                        _ => panic!("Expected Subquery on right side of join"),
                    }
                }
                _ => panic!("Expected Join in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_multiple_derived_tables_without_aliases() {
    // Multiple derived tables should get unique auto-generated aliases
    let sql = "SELECT * FROM (SELECT 1), (SELECT 2)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.from {
                Some(vibesql_ast::FromClause::Join { left, right, .. }) => {
                    // Both should have auto-generated aliases
                    let left_alias = match left.as_ref() {
                        vibesql_ast::FromClause::Subquery { alias, .. } => alias.clone(),
                        _ => panic!("Expected Subquery on left side"),
                    };
                    let right_alias = match right.as_ref() {
                        vibesql_ast::FromClause::Subquery { alias, .. } => alias.clone(),
                        _ => panic!("Expected Subquery on right side"),
                    };

                    // Both should start with (subquery- and be unique
                    assert!(left_alias.starts_with("(subquery-"));
                    assert!(right_alias.starts_with("(subquery-"));
                    assert_ne!(left_alias, right_alias, "Aliases should be unique");
                }
                _ => panic!("Expected Join in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_nested_derived_tables_without_aliases() {
    // Nested derived tables: SELECT * FROM (SELECT * FROM (SELECT 1))
    let sql = "SELECT * FROM (SELECT * FROM (SELECT 1))";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.from {
                Some(vibesql_ast::FromClause::Subquery { query, alias, .. }) => {
                    // Outer subquery should have auto-generated alias
                    assert!(alias.starts_with("(subquery-"));

                    // Inner subquery should also have auto-generated alias
                    match &query.from {
                        Some(vibesql_ast::FromClause::Subquery { alias: inner_alias, .. }) => {
                            assert!(inner_alias.starts_with("(subquery-"));
                        }
                        _ => panic!("Expected nested Subquery"),
                    }
                }
                _ => panic!("Expected Subquery in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_derived_table_with_limit_without_alias() {
    // From select1-17.2: SELECT * FROM t1,(SELECT * FROM t2 ORDER BY y LIMIT 4);
    let sql = "SELECT * FROM t1, (SELECT * FROM t2 ORDER BY y LIMIT 4)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.from {
                Some(vibesql_ast::FromClause::Join { right, .. }) => {
                    match right.as_ref() {
                        vibesql_ast::FromClause::Subquery { query, alias, .. } => {
                            assert!(alias.starts_with("(subquery-"));
                            // Verify LIMIT is parsed
                            assert!(query.limit.is_some());
                        }
                        _ => panic!("Expected Subquery on right side"),
                    }
                }
                _ => panic!("Expected Join in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}
