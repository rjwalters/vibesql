use super::*;

// ========================================================================
// IN Predicate with Value Lists Tests
// ========================================================================

#[test]
fn test_parse_in_with_integer_list() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (1, 2, 3);");
    assert!(result.is_ok(), "IN with integer list should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        // Should be an InList expression
        if let vibesql_ast::Expression::InList { expr, values, negated } =
            &select.where_clause.unwrap()
        {
            // Left side should be column reference
            assert!(matches!(**expr, vibesql_ast::Expression::ColumnRef(_)));

            // Should have 3 values
            assert_eq!(values.len(), 3);

            // All should be integer literals
            assert!(matches!(
                values[0],
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))
            ));
            assert!(matches!(
                values[1],
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2))
            ));
            assert!(matches!(
                values[2],
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3))
            ));

            // Not negated
            assert!(!(*negated));
        } else {
            panic!("Expected InList expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_in_with_string_list() {
    let result =
        Parser::parse_sql("SELECT * FROM users WHERE name IN ('Alice', 'Bob', 'Charlie');");
    assert!(result.is_ok(), "IN with string list should parse: {:?}", result);
}

#[test]
fn test_parse_in_with_mixed_types() {
    let result = Parser::parse_sql("SELECT * FROM data WHERE value IN (1, 'two', 3);");
    assert!(result.is_ok(), "IN with mixed types should parse: {:?}", result);
}

#[test]
fn test_parse_in_with_single_value() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (42);");
    assert!(result.is_ok(), "IN with single value should parse: {:?}", result);
}

#[test]
fn test_parse_not_in_with_value_list() {
    let result =
        Parser::parse_sql("SELECT * FROM users WHERE status NOT IN ('inactive', 'banned');");
    assert!(result.is_ok(), "NOT IN with value list should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        // Should be an InList expression with negated=true
        if let vibesql_ast::Expression::InList { negated, values, .. } =
            &select.where_clause.unwrap()
        {
            assert!(*negated, "NOT IN should set negated=true");
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected InList expression");
        }
    }
}

#[test]
fn test_parse_in_with_expressions() {
    // IN list can contain expressions, not just literals
    let result = Parser::parse_sql("SELECT * FROM products WHERE price IN (10 + 5, 20 * 2, 100);");
    assert!(result.is_ok(), "IN with expressions should parse: {:?}", result);
}

#[test]
fn test_parse_in_list_with_and() {
    let result = Parser::parse_sql(
        "SELECT * FROM users WHERE age > 18 AND status IN ('active', 'pending');",
    );
    assert!(result.is_ok(), "IN list with AND should parse: {:?}", result);
}

#[test]
fn test_parse_in_list_with_or() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE category IN ('electronics', 'computers') OR price < 100;",
    );
    assert!(result.is_ok(), "IN list with OR should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_in_lists() {
    let result = Parser::parse_sql(
        "SELECT * FROM data WHERE category IN ('A', 'B') AND status IN (1, 2, 3);",
    );
    assert!(result.is_ok(), "Multiple IN lists should parse: {:?}", result);
}

#[test]
fn test_parse_in_empty_list_allowed() {
    // Empty IN lists are allowed per SQL:1999
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN ();");
    assert!(result.is_ok(), "Empty IN list should parse successfully: {:?}", result);
}

#[test]
fn test_parse_in_list_with_null() {
    let result =
        Parser::parse_sql("SELECT * FROM users WHERE status IN ('active', NULL, 'pending');");
    assert!(result.is_ok(), "IN list with NULL should parse: {:?}", result);
}

#[test]
fn test_parse_in_list_complex_expression() {
    let result = Parser::parse_sql(
        "SELECT * FROM orders WHERE (customer_id IN (1, 2, 3) AND total > 100) OR status = 'vip';",
    );
    assert!(result.is_ok(), "Complex expression with IN list should parse: {:?}", result);
}

// ========================================================================
// IN table_name Syntax Tests (SQLite compatibility)
// ========================================================================

#[test]
fn test_parse_in_table_name() {
    // SQLite syntax: IN table_name is equivalent to IN (SELECT * FROM table_name)
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN t2;");
    assert!(result.is_ok(), "IN table_name should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        // Should be an In expression with subquery
        let where_clause = select.where_clause.unwrap();
        if let vibesql_ast::Expression::In { expr, subquery, negated } = &where_clause {
            // Left side should be column reference 'a'
            assert!(matches!(**expr, vibesql_ast::Expression::ColumnRef(_)));

            // Not negated
            assert!(!(*negated));

            // Subquery should be SELECT * FROM t2
            assert!(subquery.from.is_some());
            if let vibesql_ast::FromClause::Table { name, .. } = subquery.from.as_ref().unwrap() {
                assert_eq!(name, "t2");
            } else {
                panic!("Expected Table in FROM clause");
            }
        } else {
            panic!("Expected In expression, got {:?}", where_clause);
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_not_in_table_name() {
    // SQLite syntax: NOT IN table_name
    let result = Parser::parse_sql("SELECT b FROM t1 WHERE a NOT IN t2;");
    assert!(result.is_ok(), "NOT IN table_name should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::Expression::In { negated, .. } = &select.where_clause.unwrap() {
            assert!(*negated, "NOT IN should set negated=true");
        } else {
            panic!("Expected In expression");
        }
    }
}

#[test]
fn test_parse_in_qualified_table_name() {
    // IN with schema-qualified table name
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN schema.t2;");
    assert!(result.is_ok(), "IN with qualified table name should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::Expression::In { subquery, .. } = &select.where_clause.unwrap() {
            if let vibesql_ast::FromClause::Table { name, .. } = subquery.from.as_ref().unwrap() {
                assert_eq!(name, "SCHEMA.t2");
            } else {
                panic!("Expected Table in FROM clause");
            }
        } else {
            panic!("Expected In expression");
        }
    }
}

#[test]
fn test_parse_in_table_name_with_and() {
    // IN table_name combined with AND
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN t2 AND b > 10;");
    assert!(result.is_ok(), "IN table_name with AND should parse: {:?}", result);
}

#[test]
fn test_parse_in_table_name_with_or() {
    // IN table_name combined with OR
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN t2 OR a IN t3;");
    assert!(result.is_ok(), "IN table_name with OR should parse: {:?}", result);
}

#[test]
fn test_parse_in_table_name_not_confused_with_value_list() {
    // Make sure IN (1, 2, 3) still works
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN (1, 2, 3);");
    assert!(result.is_ok());

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        // Should be InList, not In
        assert!(matches!(&select.where_clause.unwrap(), vibesql_ast::Expression::InList { .. }));
    }
}

#[test]
fn test_parse_in_table_name_not_confused_with_subquery() {
    // Make sure IN (SELECT ...) still works
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN (SELECT b FROM t2);");
    assert!(result.is_ok());

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        // Should be In with subquery that has SELECT in its select_list
        if let vibesql_ast::Expression::In { subquery, .. } = &select.where_clause.unwrap() {
            // The subquery should have a proper SELECT list (not just *)
            assert!(!subquery.select_list.is_empty());
        } else {
            panic!("Expected In expression");
        }
    }
}

#[test]
fn test_parse_in_parenthesized_subquery() {
    // PostgreSQL-style parenthesized subquery: IN ((SELECT ...))
    // This should be treated as an IN subquery, not an IN list with a scalar subquery
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN ((SELECT b FROM t2));");
    assert!(result.is_ok(), "IN with parenthesized subquery should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        // Should be In (subquery form), not InList
        if let vibesql_ast::Expression::In { subquery, negated, .. } = &select.where_clause.unwrap()
        {
            assert!(!*negated);
            // The subquery should have a proper SELECT list
            assert!(!subquery.select_list.is_empty());
        } else {
            panic!("Expected In expression (subquery form), not InList");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_in_triple_parenthesized_subquery() {
    // Multiple levels of parentheses: IN (((SELECT ...)))
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a IN (((SELECT b FROM t2)));");
    assert!(result.is_ok(), "IN with triple-parenthesized subquery should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        // Should be In (subquery form), not InList
        assert!(
            matches!(&select.where_clause.unwrap(), vibesql_ast::Expression::In { .. }),
            "Expected In expression (subquery form)"
        );
    }
}

#[test]
fn test_parse_not_in_parenthesized_subquery() {
    // NOT IN with parenthesized subquery
    let result = Parser::parse_sql("SELECT * FROM t1 WHERE a NOT IN ((SELECT b FROM t2));");
    assert!(result.is_ok(), "NOT IN with parenthesized subquery should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::Expression::In { negated, .. } = &select.where_clause.unwrap() {
            assert!(*negated, "NOT IN should set negated=true");
        } else {
            panic!("Expected In expression (subquery form)");
        }
    }
}
