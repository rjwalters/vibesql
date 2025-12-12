use super::*;

// ========================================================================
// CASE Expression Tests
// ========================================================================

#[test]
fn test_parse_searched_case_simple() {
    let result =
        Parser::parse_sql("SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END FROM t;");
    assert!(result.is_ok(), "Simple searched CASE should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 1);

        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { operand, when_clauses, else_result } = expr {
                // Should be searched CASE (no operand)
                assert!(operand.is_none(), "Searched CASE should have no operand");

                // Should have one WHEN clause
                assert_eq!(when_clauses.len(), 1);

                // Should have ELSE clause
                assert!(else_result.is_some(), "Should have ELSE clause");
            } else {
                panic!("Expected Case expression");
            }
        } else {
            panic!("Expected Expression SelectItem");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_searched_case_multiple_when() {
    let result = Parser::parse_sql(
        "SELECT CASE
            WHEN x < 0 THEN 'negative'
            WHEN x = 0 THEN 'zero'
            WHEN x > 0 THEN 'positive'
         END FROM t;",
    );
    assert!(result.is_ok(), "Searched CASE with multiple WHEN should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { when_clauses, .. } = expr {
                assert_eq!(when_clauses.len(), 3, "Should have three WHEN clauses");
            } else {
                panic!("Expected Case expression");
            }
        }
    }
}

#[test]
fn test_parse_searched_case_no_else() {
    let result = Parser::parse_sql("SELECT CASE WHEN status = 'active' THEN 1 END FROM users;");
    assert!(result.is_ok(), "CASE without ELSE should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { else_result, .. } = expr {
                assert!(else_result.is_none(), "Should have no ELSE clause (defaults to NULL)");
            } else {
                panic!("Expected Case expression");
            }
        }
    }
}

#[test]
fn test_parse_simple_case() {
    let result = Parser::parse_sql(
        "SELECT CASE status
            WHEN 'active' THEN 1
            WHEN 'inactive' THEN 0
            ELSE 99
         END FROM users;",
    );
    assert!(result.is_ok(), "Simple CASE should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { operand, when_clauses, else_result } = expr {
                // Should be simple CASE (has operand)
                assert!(operand.is_some(), "Simple CASE should have operand");

                // Should have two WHEN clauses
                assert_eq!(when_clauses.len(), 2);

                // Should have ELSE clause
                assert!(else_result.is_some());
            } else {
                panic!("Expected Case expression");
            }
        }
    }
}

#[test]
fn test_parse_case_with_alias() {
    let result = Parser::parse_sql(
        "SELECT CASE WHEN price < 10 THEN 'cheap' ELSE 'expensive' END AS price_category FROM products;"
    );
    assert!(result.is_ok(), "CASE with alias should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { alias, .. } = &select.select_list[0] {
            assert_eq!(*alias, Some("price_category".to_string()));
        }
    }
}

#[test]
fn test_parse_case_in_where() {
    let result =
        Parser::parse_sql("SELECT * FROM t WHERE CASE WHEN x > 0 THEN TRUE ELSE FALSE END;");
    assert!(result.is_ok(), "CASE in WHERE clause should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        if let vibesql_ast::Expression::Case { .. } = &select.where_clause.unwrap() {
            // Success
        } else {
            panic!("Expected Case expression in WHERE");
        }
    }
}

#[test]
fn test_parse_case_in_order_by() {
    let result = Parser::parse_sql(
        "SELECT * FROM t ORDER BY CASE WHEN priority = 'high' THEN 1 ELSE 2 END;",
    );
    assert!(result.is_ok(), "CASE in ORDER BY should parse: {:?}", result);
}

#[test]
fn test_parse_nested_case() {
    let result = Parser::parse_sql(
        "SELECT CASE
            WHEN x > 0 THEN CASE WHEN y > 0 THEN 'both positive' ELSE 'x positive' END
            ELSE 'x not positive'
         END FROM t;",
    );
    assert!(result.is_ok(), "Nested CASE should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { when_clauses, .. } = expr {
                // Check that the result of the first WHEN is itself a CASE
                if let vibesql_ast::Expression::Case { .. } = &when_clauses[0].result {
                    // Success - nested CASE found
                } else {
                    panic!("Expected nested Case expression in THEN clause");
                }
            } else {
                panic!("Expected Case expression");
            }
        }
    }
}

#[test]
fn test_parse_case_with_complex_conditions() {
    let result = Parser::parse_sql(
        "SELECT CASE
            WHEN age >= 18 AND age < 65 THEN 'adult'
            WHEN age >= 65 THEN 'senior'
            ELSE 'minor'
         END FROM users;",
    );
    assert!(result.is_ok(), "CASE with complex conditions should parse: {:?}", result);
}

#[test]
fn test_parse_case_with_null() {
    let result = Parser::parse_sql("SELECT CASE WHEN x = NULL THEN 0 ELSE x END FROM t;");
    assert!(result.is_ok(), "CASE with NULL comparison should parse: {:?}", result);
}

#[test]
fn test_parse_case_with_function_calls() {
    let result = Parser::parse_sql(
        "SELECT CASE
            WHEN LENGTH(name) > 10 THEN SUBSTRING(name, 1, 10)
            ELSE name
         END FROM users;",
    );
    assert!(result.is_ok(), "CASE with function calls should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_case_in_select() {
    let result = Parser::parse_sql(
        "SELECT
            CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END AS x_sign,
            CASE WHEN y > 0 THEN 'pos' ELSE 'neg' END AS y_sign
         FROM t;",
    );
    assert!(result.is_ok(), "Multiple CASE in SELECT should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 2);
    }
}

#[test]
fn test_parse_case_web_demo_query() {
    // The exact query from issue #240
    let result = Parser::parse_sql(
        "SELECT
            product_name,
            unit_price,
            CASE
                WHEN unit_price < 10 THEN 'Budget'
                WHEN unit_price < 50 THEN 'Standard'
                ELSE 'Premium'
            END as price_category
        FROM products
        ORDER BY unit_price;",
    );
    assert!(result.is_ok(), "Web demo CASE query should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 3);

        // Third item should be the CASE expression
        if let vibesql_ast::SelectItem::Expression { expr, alias, .. } = &select.select_list[2] {
            if let vibesql_ast::Expression::Case { operand, when_clauses, else_result } = expr {
                assert!(operand.is_none(), "Should be searched CASE");
                assert_eq!(when_clauses.len(), 2);
                assert!(else_result.is_some());
                assert_eq!(*alias, Some("price_category".to_string()));
            } else {
                panic!("Expected Case expression");
            }
        }

        // Should have FROM clause
        assert!(select.from.is_some(), "Should have FROM clause");

        // Should have ORDER BY
        assert!(select.order_by.is_some(), "Should have ORDER BY clause");
    }
}

#[test]
fn test_parse_case_error_no_when() {
    let result = Parser::parse_sql("SELECT CASE ELSE 'default' END FROM t;");
    assert!(result.is_err(), "CASE without WHEN should fail");
    // The error can be either about missing WHEN or about unexpected ELSE keyword
}

#[test]
fn test_parse_case_error_missing_then() {
    let result = Parser::parse_sql("SELECT CASE WHEN x > 0 'positive' END FROM t;");
    assert!(result.is_err(), "CASE without THEN should fail");
}

#[test]
fn test_parse_case_error_missing_end() {
    let result = Parser::parse_sql("SELECT CASE WHEN x > 0 THEN 'positive' FROM t;");
    assert!(result.is_err(), "CASE without END should fail");
}

// Tests for comma-separated WHEN values (Issue #409)

#[test]
fn test_parse_case_comma_separated_when() {
    let result = Parser::parse_sql("SELECT CASE 0 WHEN 2, 2 THEN 1 ELSE 0 END;");
    assert!(result.is_ok(), "CASE with comma-separated WHEN should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { when_clauses, .. } = expr {
                // First WHEN should have 2 conditions
                assert_eq!(when_clauses[0].conditions.len(), 2);
            } else {
                panic!("Expected Case expression");
            }
        }
    }
}

#[test]
fn test_parse_case_multiple_values() {
    let result = Parser::parse_sql(
        "SELECT CASE status WHEN 'active', 'pending', 'new' THEN 'open' ELSE 'closed' END;",
    );
    assert!(result.is_ok(), "CASE with multiple values should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { when_clauses, .. } = expr {
                // First WHEN should have 3 conditions
                assert_eq!(when_clauses[0].conditions.len(), 3);
            } else {
                panic!("Expected Case expression");
            }
        }
    }
}

#[test]
fn test_parse_case_mixed_single_and_multiple() {
    let result = Parser::parse_sql("SELECT CASE x WHEN 1, 2 THEN 'low' WHEN 10 THEN 'high' END;");
    assert!(
        result.is_ok(),
        "CASE with mixed single and multiple values should parse: {:?}",
        result
    );

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Case { when_clauses, .. } = expr {
                // First WHEN should have 2 conditions, second should have 1
                assert_eq!(when_clauses[0].conditions.len(), 2);
                assert_eq!(when_clauses[1].conditions.len(), 1);
            } else {
                panic!("Expected Case expression");
            }
        }
    }
}
