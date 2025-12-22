use super::*;

// ========================================================================
// Common Table Expression (CTE) Tests
// ========================================================================

#[test]
fn test_parse_cte_basic() {
    let result = Parser::parse_sql(
        "WITH regional_sales AS (SELECT region, SUM(amount) FROM orders GROUP BY region) SELECT * FROM regional_sales;"
    );
    assert!(result.is_ok(), "Basic CTE should parse: {:?}", result);
}

#[test]
fn test_parse_cte_simple() {
    let result = Parser::parse_sql("WITH cte AS (SELECT id FROM users) SELECT * FROM cte;");
    assert!(result.is_ok(), "Simple CTE should parse: {:?}", result);
}

#[test]
fn test_parse_cte_multiple() {
    let result = Parser::parse_sql(
        "WITH cte1 AS (SELECT id FROM users), cte2 AS (SELECT id FROM orders) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id;"
    );
    assert!(result.is_ok(), "Multiple CTEs should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_column_list() {
    let result = Parser::parse_sql(
        "WITH cte (user_id, user_name) AS (SELECT id, name FROM users) SELECT * FROM cte;",
    );
    assert!(result.is_ok(), "CTE with column list should parse: {:?}", result);
}

#[test]
fn test_parse_cte_referencing_another() {
    let result = Parser::parse_sql(
        "WITH cte1 AS (SELECT id FROM users), cte2 AS (SELECT id FROM cte1 WHERE id > 10) SELECT * FROM cte2;"
    );
    assert!(result.is_ok(), "CTE referencing another CTE should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_where() {
    let result = Parser::parse_sql(
        "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE) SELECT * FROM active_users WHERE id > 100;"
    );
    assert!(result.is_ok(), "CTE with WHERE clause should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_join() {
    let result = Parser::parse_sql(
        "WITH user_orders AS (SELECT u.id, u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id) SELECT * FROM user_orders;"
    );
    assert!(result.is_ok(), "CTE with JOIN should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_aggregates() {
    let result = Parser::parse_sql(
        "WITH sales_summary AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) SELECT * FROM sales_summary WHERE total > 1000;"
    );
    assert!(result.is_ok(), "CTE with aggregates should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_order_by() {
    let result = Parser::parse_sql(
        "WITH sorted_users AS (SELECT id, name FROM users ORDER BY name) SELECT * FROM sorted_users LIMIT 10;"
    );
    assert!(result.is_ok(), "CTE with ORDER BY should parse: {:?}", result);
}

#[test]
fn test_parse_cte_in_subquery() {
    let result = Parser::parse_sql(
        "WITH high_value AS (SELECT user_id FROM orders WHERE amount > 1000) SELECT * FROM users WHERE id IN (SELECT user_id FROM high_value);"
    );
    assert!(result.is_ok(), "CTE used in subquery should parse: {:?}", result);
}

#[test]
fn test_parse_cte_multiple_references() {
    let result = Parser::parse_sql(
        "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE) SELECT * FROM active_users a1 JOIN active_users a2 ON a1.id != a2.id;"
    );
    assert!(result.is_ok(), "Multiple references to same CTE should parse: {:?}", result);
}

#[test]
fn test_parse_cte_three_levels() {
    let result = Parser::parse_sql(
        "WITH level1 AS (SELECT id FROM users), level2 AS (SELECT id FROM level1 WHERE id > 10), level3 AS (SELECT id FROM level2 WHERE id < 100) SELECT * FROM level3;"
    );
    assert!(result.is_ok(), "Three-level CTE chain should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_union() {
    let result = Parser::parse_sql(
        "WITH combined AS (SELECT id FROM users UNION SELECT id FROM customers) SELECT * FROM combined;"
    );
    assert!(result.is_ok(), "CTE with UNION should parse: {:?}", result);
}

#[test]
fn test_parse_cte_complex_query() {
    let result = Parser::parse_sql(
        "WITH regional_sales AS (
            SELECT region, SUM(amount) AS total_sales
            FROM orders
            GROUP BY region
        ),
        top_regions AS (
            SELECT region
            FROM regional_sales
            WHERE total_sales > 1000000
        )
        SELECT region, product, SUM(amount) AS product_sales
        FROM orders
        WHERE region IN (SELECT region FROM top_regions)
        GROUP BY region, product;",
    );
    assert!(result.is_ok(), "Complex multi-CTE query should parse: {:?}", result);
}

#[test]
fn test_parse_cte_case_insensitive() {
    let sql_variants = vec![
        "WITH cte AS (SELECT id FROM users) SELECT * FROM cte;",
        "with cte as (select id from users) select * from cte;",
        "WiTh CTE aS (SeLeCt id FrOm users) SeLeCt * FrOm cte;",
    ];

    for sql in sql_variants {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Case-insensitive WITH should parse: {} -> {:?}", sql, result);
    }
}

#[test]
fn test_parse_cte_with_distinct() {
    let result = Parser::parse_sql(
        "WITH unique_regions AS (SELECT DISTINCT region FROM sales) SELECT * FROM unique_regions;",
    );
    assert!(result.is_ok(), "CTE with DISTINCT should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_limit() {
    let result = Parser::parse_sql(
        "WITH top_users AS (SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10) SELECT * FROM top_users;"
    );
    assert!(result.is_ok(), "CTE with LIMIT should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_subquery_in_cte() {
    let result = Parser::parse_sql(
        "WITH high_spenders AS (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) SELECT * FROM high_spenders;"
    );
    assert!(result.is_ok(), "CTE with subquery inside should parse: {:?}", result);
}

#[test]
fn test_parse_cte_empty_column_list() {
    let result = Parser::parse_sql("WITH cte () AS (SELECT id FROM users) SELECT * FROM cte;");
    // Empty column list should fail
    assert!(result.is_err(), "CTE with empty column list should fail to parse");
}

#[test]
fn test_parse_cte_join_with_regular_table() {
    let result = Parser::parse_sql(
        "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE) SELECT * FROM active_users JOIN orders ON active_users.id = orders.user_id;"
    );
    assert!(result.is_ok(), "CTE joined with regular table should parse: {:?}", result);
}

// ========================================================================
// CTE with VALUES Tests (Issue #4546)
// ========================================================================

#[test]
fn test_parse_cte_with_values_single_row() {
    let result = Parser::parse_sql("WITH t AS (VALUES(1)) SELECT * FROM t;");
    assert!(result.is_ok(), "CTE with VALUES single row should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_values_multiple_rows() {
    let result = Parser::parse_sql("WITH t AS (VALUES(1), (2), (3)) SELECT * FROM t;");
    assert!(result.is_ok(), "CTE with VALUES multiple rows should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_values_multiple_columns() {
    let result =
        Parser::parse_sql("WITH t AS (VALUES(1, 'a'), (2, 'b'), (3, 'c')) SELECT * FROM t;");
    assert!(result.is_ok(), "CTE with VALUES multiple columns should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_values_and_column_names() {
    let result =
        Parser::parse_sql("WITH t(x, y) AS (VALUES(1, 'a'), (2, 'b')) SELECT x, y FROM t;");
    assert!(result.is_ok(), "CTE with VALUES and column names should parse: {:?}", result);
}

#[test]
fn test_parse_recursive_cte_with_values() {
    let result = Parser::parse_sql(
        "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt;",
    );
    assert!(result.is_ok(), "Recursive CTE with VALUES should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_values_union_select() {
    let result = Parser::parse_sql("WITH t AS (VALUES(1) UNION SELECT 2) SELECT * FROM t;");
    assert!(result.is_ok(), "CTE with VALUES UNION SELECT should parse: {:?}", result);
}

#[test]
fn test_parse_cte_with_values_sum() {
    let result = Parser::parse_sql("WITH t(x) AS (VALUES(1), (2), (3)) SELECT SUM(x) FROM t;");
    assert!(result.is_ok(), "CTE with VALUES and aggregate should parse: {:?}", result);
}

// ========================================================================
// CTE Materialization Hint Tests
// ========================================================================

#[test]
fn test_parse_cte_materialized() {
    let result = Parser::parse_sql(
        "WITH t(b) AS MATERIALIZED (SELECT b FROM t1 LEFT JOIN t2 ON c IN (SELECT x FROM t3)) SELECT * FROM t;",
    );
    assert!(result.is_ok(), "CTE with MATERIALIZED hint should parse: {:?}", result);
}

#[test]
fn test_parse_cte_not_materialized() {
    let result = Parser::parse_sql(
        "WITH t(b) AS NOT MATERIALIZED (SELECT b FROM t1 LEFT JOIN t2 ON c IN (SELECT x FROM t3)) SELECT * FROM t;",
    );
    assert!(result.is_ok(), "CTE with NOT MATERIALIZED hint should parse: {:?}", result);
}

#[test]
fn test_parse_cte_materialized_verify_ast() {
    use vibesql_ast::{CteMaterialization, Statement};

    let stmt = Parser::parse_sql("WITH t AS MATERIALIZED (SELECT 1) SELECT * FROM t;").unwrap();
    match stmt {
        Statement::Select(select) => {
            let cte = &select.with_clause.as_ref().unwrap()[0];
            assert_eq!(cte.materialization, CteMaterialization::Materialized);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cte_not_materialized_verify_ast() {
    use vibesql_ast::{CteMaterialization, Statement};

    let stmt = Parser::parse_sql("WITH t AS NOT MATERIALIZED (SELECT 1) SELECT * FROM t;").unwrap();
    match stmt {
        Statement::Select(select) => {
            let cte = &select.with_clause.as_ref().unwrap()[0];
            assert_eq!(cte.materialization, CteMaterialization::NotMaterialized);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cte_default_materialization() {
    use vibesql_ast::{CteMaterialization, Statement};

    let stmt = Parser::parse_sql("WITH t AS (SELECT 1) SELECT * FROM t;").unwrap();
    match stmt {
        Statement::Select(select) => {
            let cte = &select.with_clause.as_ref().unwrap()[0];
            assert_eq!(cte.materialization, CteMaterialization::Default);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cte_materialized_with_column_list() {
    let result = Parser::parse_sql(
        "WITH t(a, b) AS MATERIALIZED (SELECT 1, 2) SELECT * FROM t;",
    );
    assert!(result.is_ok(), "CTE with column list and MATERIALIZED should parse: {:?}", result);
}
