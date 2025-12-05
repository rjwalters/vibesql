use super::*;

// ========================================================================
// EXISTS Predicate Tests
// ========================================================================

#[test]
fn test_parse_exists_simple() {
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE EXISTS (SELECT * FROM orders WHERE customer_id = 1);",
    );
    assert!(result.is_ok(), "Simple EXISTS should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        if let vibesql_ast::Expression::Exists { subquery, negated } = &select.where_clause.unwrap()
        {
            // Should not be negated
            assert!(!(*negated));

            // Should have a valid subquery
            assert_eq!(subquery.select_list.len(), 1);
        } else {
            panic!("Expected Exists expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_not_exists() {
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE NOT EXISTS (SELECT * FROM orders WHERE customer_id = 1);",
    );
    assert!(result.is_ok(), "NOT EXISTS should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        if let vibesql_ast::Expression::Exists { negated, .. } = &select.where_clause.unwrap() {
            // Should be negated
            assert!(*negated);
        } else {
            panic!("Expected Exists expression");
        }
    }
}

#[test]
fn test_parse_exists_with_correlated_subquery() {
    // Correlated subquery referencing outer table
    let result = Parser::parse_sql(
        "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);"
    );
    assert!(result.is_ok(), "EXISTS with correlated subquery should parse: {:?}", result);
}

#[test]
fn test_parse_exists_with_select_1() {
    // Common idiom: SELECT 1 in EXISTS (doesn't matter what's selected)
    let result = Parser::parse_sql("SELECT * FROM customers WHERE EXISTS (SELECT 1 FROM orders);");
    assert!(result.is_ok(), "EXISTS with SELECT 1 should parse: {:?}", result);
}

#[test]
fn test_parse_exists_with_and() {
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE id > 10 AND EXISTS (SELECT 1 FROM orders WHERE customer_id = 1);"
    );
    assert!(result.is_ok(), "EXISTS with AND should parse: {:?}", result);
}

#[test]
fn test_parse_exists_with_or() {
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE id < 5 OR EXISTS (SELECT 1 FROM orders WHERE customer_id = 1);"
    );
    assert!(result.is_ok(), "EXISTS with OR should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_exists() {
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE
         EXISTS (SELECT 1 FROM orders WHERE customer_id = 1) AND
         EXISTS (SELECT 1 FROM payments WHERE customer_id = 1);",
    );
    assert!(result.is_ok(), "Multiple EXISTS should parse: {:?}", result);
}

#[test]
fn test_parse_exists_in_select_list() {
    // EXISTS can also appear in SELECT list (less common)
    let result = Parser::parse_sql(
        "SELECT id, EXISTS (SELECT 1 FROM orders WHERE customer_id = id) AS has_orders FROM customers;"
    );
    assert!(result.is_ok(), "EXISTS in SELECT list should parse: {:?}", result);
}

#[test]
fn test_parse_nested_exists() {
    // Nested EXISTS - EXISTS within EXISTS subquery
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE EXISTS (
            SELECT 1 FROM orders WHERE customer_id = 1 AND EXISTS (
                SELECT 1 FROM order_items WHERE order_id = orders.id
            )
        );",
    );
    assert!(result.is_ok(), "Nested EXISTS should parse: {:?}", result);
}

#[test]
fn test_parse_exists_with_complex_subquery() {
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE EXISTS (
            SELECT 1 FROM orders
            WHERE customer_id = customers.id
            AND total > 100
            AND status IN ('shipped', 'delivered')
        );",
    );
    assert!(result.is_ok(), "EXISTS with complex subquery should parse: {:?}", result);
}

#[test]
fn test_parse_not_exists_anti_join_pattern() {
    // Classic anti-join pattern: find customers with NO orders
    let result = Parser::parse_sql(
        "SELECT * FROM customers c WHERE NOT EXISTS (
            SELECT 1 FROM orders o WHERE o.customer_id = c.id
        );",
    );
    assert!(result.is_ok(), "NOT EXISTS anti-join pattern should parse: {:?}", result);
}

#[test]
fn test_parse_exists_parenthesized() {
    let result =
        Parser::parse_sql("SELECT * FROM customers WHERE (EXISTS (SELECT 1 FROM orders));");
    assert!(result.is_ok(), "Parenthesized EXISTS should parse: {:?}", result);
}

#[test]
fn test_parse_exists_with_group_by() {
    let result = Parser::parse_sql(
        "SELECT * FROM customers WHERE EXISTS (
            SELECT 1 FROM orders
            WHERE customer_id = customers.id
            GROUP BY order_date
            HAVING COUNT(*) > 5
        );",
    );
    assert!(result.is_ok(), "EXISTS with GROUP BY should parse: {:?}", result);
}
