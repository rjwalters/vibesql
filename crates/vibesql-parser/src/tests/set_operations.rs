use super::*;

// ========================================================================
// UNION Tests
// ========================================================================

#[test]
fn test_parse_union_basic() {
    let result = Parser::parse_sql("SELECT id FROM users UNION SELECT id FROM customers;");
    assert!(result.is_ok(), "Basic UNION should parse: {:?}", result);
}

#[test]
fn test_parse_union_all() {
    let result = Parser::parse_sql("SELECT id FROM users UNION ALL SELECT id FROM customers;");
    assert!(result.is_ok(), "UNION ALL should parse: {:?}", result);
}

#[test]
fn test_parse_union_multiple_columns() {
    let result =
        Parser::parse_sql("SELECT id, name FROM users UNION SELECT id, name FROM customers;");
    assert!(result.is_ok(), "UNION with multiple columns should parse: {:?}", result);
}

#[test]
fn test_parse_union_with_where() {
    let result = Parser::parse_sql(
        "SELECT id FROM users WHERE age > 18 UNION SELECT id FROM customers WHERE status = 'active';"
    );
    assert!(result.is_ok(), "UNION with WHERE clauses should parse: {:?}", result);
}

#[test]
fn test_parse_union_with_order_by() {
    let result =
        Parser::parse_sql("SELECT name FROM users UNION SELECT name FROM customers ORDER BY name;");
    assert!(result.is_ok(), "UNION with ORDER BY should parse: {:?}", result);
}

#[test]
fn test_parse_union_three_queries() {
    let result = Parser::parse_sql(
        "SELECT id FROM users UNION SELECT id FROM customers UNION SELECT id FROM vendors;",
    );
    assert!(result.is_ok(), "UNION with three queries should parse: {:?}", result);
}

#[test]
fn test_parse_union_with_subquery() {
    let result = Parser::parse_sql(
        "SELECT id FROM users UNION SELECT id FROM (SELECT customer_id AS id FROM orders) AS subq;",
    );
    assert!(result.is_ok(), "UNION with subquery should parse: {:?}", result);
}

#[test]
fn test_parse_union_with_limit() {
    let result = Parser::parse_sql("SELECT id FROM users UNION SELECT id FROM customers LIMIT 10;");
    assert!(result.is_ok(), "UNION with LIMIT should parse: {:?}", result);
}

// ========================================================================
// INTERSECT Tests
// ========================================================================

#[test]
fn test_parse_intersect_basic() {
    let result = Parser::parse_sql("SELECT id FROM users INTERSECT SELECT id FROM customers;");
    assert!(result.is_ok(), "Basic INTERSECT should parse: {:?}", result);
}

#[test]
fn test_parse_intersect_all() {
    let result = Parser::parse_sql("SELECT id FROM users INTERSECT ALL SELECT id FROM customers;");
    assert!(result.is_ok(), "INTERSECT ALL should parse: {:?}", result);
}

#[test]
fn test_parse_intersect_with_where() {
    let result = Parser::parse_sql(
        "SELECT email FROM users WHERE active = TRUE INTERSECT SELECT email FROM newsletter WHERE subscribed = TRUE;"
    );
    assert!(result.is_ok(), "INTERSECT with WHERE should parse: {:?}", result);
}

#[test]
fn test_parse_intersect_multiple_columns() {
    let result = Parser::parse_sql(
        "SELECT first_name, last_name FROM users INTERSECT SELECT first_name, last_name FROM employees;"
    );
    assert!(result.is_ok(), "INTERSECT with multiple columns should parse: {:?}", result);
}

// ========================================================================
// EXCEPT Tests (SQL standard - same as MINUS in Oracle)
// ========================================================================

#[test]
fn test_parse_except_basic() {
    let result = Parser::parse_sql("SELECT id FROM users EXCEPT SELECT id FROM banned_users;");
    assert!(result.is_ok(), "Basic EXCEPT should parse: {:?}", result);
}

#[test]
fn test_parse_except_all() {
    let result = Parser::parse_sql("SELECT id FROM users EXCEPT ALL SELECT id FROM banned_users;");
    assert!(result.is_ok(), "EXCEPT ALL should parse: {:?}", result);
}

#[test]
fn test_parse_except_with_where() {
    let result = Parser::parse_sql(
        "SELECT email FROM all_users WHERE active = TRUE EXCEPT SELECT email FROM unsubscribed WHERE reason = 'spam';"
    );
    assert!(result.is_ok(), "EXCEPT with WHERE should parse: {:?}", result);
}

#[test]
fn test_parse_except_multiple_columns() {
    let result = Parser::parse_sql(
        "SELECT id, name FROM products EXCEPT SELECT id, name FROM discontinued_products;",
    );
    assert!(result.is_ok(), "EXCEPT with multiple columns should parse: {:?}", result);
}

// ========================================================================
// Mixed Set Operations Tests
// ========================================================================

#[test]
fn test_parse_union_and_intersect() {
    let result = Parser::parse_sql(
        "SELECT id FROM users UNION SELECT id FROM customers INTERSECT SELECT id FROM active_accounts;"
    );
    assert!(result.is_ok(), "UNION and INTERSECT mixed should parse: {:?}", result);
}

#[test]
fn test_parse_union_and_except() {
    let result = Parser::parse_sql(
        "SELECT id FROM all_users UNION SELECT id FROM all_customers EXCEPT SELECT id FROM deleted_accounts;"
    );
    assert!(result.is_ok(), "UNION and EXCEPT mixed should parse: {:?}", result);
}

#[test]
fn test_parse_set_operation_with_aliases() {
    let result = Parser::parse_sql("SELECT u.id FROM users u UNION SELECT c.id FROM customers c;");
    assert!(result.is_ok(), "Set operation with table aliases should parse: {:?}", result);
}

#[test]
fn test_parse_set_operation_with_join() {
    let result = Parser::parse_sql(
        "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id UNION SELECT c.id FROM customers c;"
    );
    assert!(result.is_ok(), "Set operation with JOIN should parse: {:?}", result);
}

#[test]
fn test_parse_set_operation_with_group_by() {
    let result = Parser::parse_sql(
        "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id UNION SELECT dept_id, COUNT(*) FROM contractors GROUP BY dept_id;"
    );
    assert!(result.is_ok(), "Set operation with GROUP BY should parse: {:?}", result);
}

#[test]
fn test_parse_set_operation_case_insensitive() {
    let sql_variants = vec![
        "SELECT id FROM t1 union SELECT id FROM t2;",
        "SELECT id FROM t1 UNION SELECT id FROM t2;",
        "SELECT id FROM t1 intersect SELECT id FROM t2;",
        "SELECT id FROM t1 INTERSECT SELECT id FROM t2;",
        "SELECT id FROM t1 except SELECT id FROM t2;",
        "SELECT id FROM t1 EXCEPT SELECT id FROM t2;",
    ];

    for sql in sql_variants {
        let result = Parser::parse_sql(sql);
        assert!(
            result.is_ok(),
            "Case-insensitive set operation should parse: {} -> {:?}",
            sql,
            result
        );
    }
}

#[test]
fn test_parse_complex_set_operation_chain() {
    let result = Parser::parse_sql(
        "SELECT id FROM a UNION SELECT id FROM b UNION SELECT id FROM c INTERSECT SELECT id FROM d EXCEPT SELECT id FROM e;"
    );
    assert!(result.is_ok(), "Complex set operation chain should parse: {:?}", result);
}

#[test]
fn test_parse_set_operation_with_distinct() {
    let result = Parser::parse_sql(
        "SELECT DISTINCT name FROM users UNION SELECT DISTINCT name FROM customers;",
    );
    assert!(result.is_ok(), "Set operation with DISTINCT should parse: {:?}", result);
}

// ========================================================================
// Explicit DISTINCT Quantifier Tests (SQL:1999 Features E071-01, E071-03)
// ========================================================================

#[test]
fn test_parse_union_distinct() {
    let result = Parser::parse_sql("SELECT id FROM users UNION DISTINCT SELECT id FROM customers;");
    assert!(result.is_ok(), "UNION DISTINCT should parse: {:?}", result);
}

#[test]
fn test_parse_except_distinct() {
    let result = Parser::parse_sql("SELECT a FROM t1 EXCEPT DISTINCT SELECT b FROM t2;");
    assert!(result.is_ok(), "EXCEPT DISTINCT should parse: {:?}", result);
}

#[test]
fn test_parse_intersect_distinct() {
    let result =
        Parser::parse_sql("SELECT id FROM users INTERSECT DISTINCT SELECT id FROM customers;");
    assert!(result.is_ok(), "INTERSECT DISTINCT should parse: {:?}", result);
}

// ========================================================================
// Parenthesized Subquery Tests (TPC-DS Q2 style)
// ========================================================================

#[test]
fn test_parse_union_all_parenthesized() {
    let result = Parser::parse_sql("SELECT id FROM users UNION ALL (SELECT id FROM customers);");
    assert!(result.is_ok(), "UNION ALL with parenthesized SELECT should parse: {:?}", result);
}

#[test]
fn test_parse_union_parenthesized() {
    let result = Parser::parse_sql("SELECT id FROM users UNION (SELECT id FROM customers);");
    assert!(result.is_ok(), "UNION with parenthesized SELECT should parse: {:?}", result);
}

#[test]
fn test_parse_intersect_parenthesized() {
    let result = Parser::parse_sql("SELECT id FROM users INTERSECT (SELECT id FROM customers);");
    assert!(result.is_ok(), "INTERSECT with parenthesized SELECT should parse: {:?}", result);
}

#[test]
fn test_parse_except_parenthesized() {
    let result = Parser::parse_sql("SELECT id FROM users EXCEPT (SELECT id FROM banned);");
    assert!(result.is_ok(), "EXCEPT with parenthesized SELECT should parse: {:?}", result);
}

#[test]
fn test_parse_union_all_parenthesized_complex() {
    // TPC-DS Q2 style: parenthesized SELECT in UNION ALL within CTE
    let sql = r#"
        WITH wscs AS (
            SELECT ws_sold_date_sk sold_date_sk
            FROM web_sales
            UNION ALL
            (SELECT cs_sold_date_sk sold_date_sk
             FROM catalog_sales)
        )
        SELECT * FROM wscs;
    "#;
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "TPC-DS Q2 style UNION ALL with parenthesized SELECT in CTE should parse: {:?}",
        result
    );
}
