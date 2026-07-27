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
    let result = Parser::parse_sql("WITH t(a, b) AS MATERIALIZED (SELECT 1, 2) SELECT * FROM t;");
    assert!(result.is_ok(), "CTE with column list and MATERIALIZED should parse: {:?}", result);
}

// ========================================================================
// WITH ... VALUES Tests (issue #5353)
//
// SQLite treats a standalone VALUES as a SELECT form, so a WITH clause may
// precede a bare VALUES statement.
// ========================================================================

#[test]
fn test_parse_with_values_basic() {
    let stmt = Parser::parse_sql("WITH c AS (SELECT 1) VALUES((SELECT * FROM c));").unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.with_clause.is_some(), "WITH clause should be attached");
            assert_eq!(select.with_clause.as_ref().unwrap()[0].name, "c");
            let values = select.values.as_ref().expect("VALUES body should be set");
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].len(), 1);
            assert!(select.select_list.is_empty(), "VALUES form has no select list");
        }
        other => panic!("Expected SELECT statement with VALUES body, got {:?}", other),
    }
}

#[test]
fn test_parse_with_values_multi_row() {
    let stmt =
        Parser::parse_sql("WITH x AS (SELECT 5) VALUES(1),(2),((SELECT * FROM x));").unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.with_clause.is_some());
            assert_eq!(select.values.as_ref().unwrap().len(), 3);
        }
        other => panic!("Expected SELECT statement with VALUES body, got {:?}", other),
    }
}

#[test]
fn test_parse_with_recursive_values() {
    let result = Parser::parse_sql(
        "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<3) \
         VALUES((SELECT max(x) FROM cnt));",
    );
    assert!(result.is_ok(), "WITH RECURSIVE before VALUES should parse: {:?}", result);
}

#[test]
fn test_parse_with_values_union() {
    let stmt = Parser::parse_sql("WITH c AS (SELECT 9) VALUES((SELECT * FROM c)) UNION VALUES(2);")
        .unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.with_clause.is_some());
            assert!(select.values.is_some());
            assert!(select.set_operation.is_some(), "UNION should be attached");
        }
        other => panic!("Expected SELECT statement with VALUES body, got {:?}", other),
    }
}

#[test]
fn test_parse_with_multiple_ctes_then_values() {
    let result = Parser::parse_sql(
        "WITH a AS (SELECT 1), b AS (SELECT * FROM a) VALUES((SELECT * FROM b));",
    );
    assert!(result.is_ok(), "WITH multiple CTEs before VALUES should parse: {:?}", result);
}

#[test]
fn test_parse_with_followed_by_invalid_statement_still_errors() {
    // WITH may only precede SELECT, VALUES, INSERT, UPDATE, or DELETE
    let result = Parser::parse_sql("WITH c AS (SELECT 1) CREATE TABLE t(a INTEGER);");
    assert!(result.is_err(), "WITH before CREATE should not parse");
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("after WITH clause"), "Unexpected error message: {}", msg);
}

#[test]
fn test_parse_with_values_missing_rows_errors() {
    // VALUES keyword with no row list is still invalid
    let result = Parser::parse_sql("WITH c AS (SELECT 1) VALUES;");
    assert!(result.is_err(), "WITH ... VALUES without rows should not parse");
}

#[test]
fn test_parse_cte_named_rows() {
    // `ROWS` is a SQLite fallback keyword and must be usable as a CTE name.
    let result = Parser::parse_sql("WITH rows AS (SELECT 1) SELECT * FROM rows;");
    assert!(result.is_ok(), "CTE named `rows` should parse: {:?}", result);
}

#[test]
fn test_parse_cte_named_level() {
    let result = Parser::parse_sql("WITH level AS (SELECT 1) SELECT * FROM level;");
    assert!(result.is_ok(), "CTE named `level` should parse: {:?}", result);
}

#[test]
fn test_parse_cte_trailing_comma_reports_offending_token() {
    // A trailing comma in the WITH list leaves the parser expecting another CTE
    // name; SQLite reports the offending token, not an internal expectation
    // string: `near "SELECT": syntax error` (with1.test 3.6).
    let result = Parser::parse_sql("WITH tmp AS ( SELECT 1 ), SELECT * FROM tmp;");
    assert!(result.is_err(), "trailing comma in WITH list should not parse");
    let msg = result.unwrap_err().message;
    assert_eq!(msg, "near \"SELECT\": syntax error", "Unexpected error message: {}", msg);
}

#[test]
fn test_parse_cte_empty_column_list_is_syntax_error() {
    // An empty CTE column list `t()` is a syntax error reported against the `)`
    // token in SQLite: `near ")": syntax error` (with2.test 4.1).
    let result = Parser::parse_sql("WITH x() AS ( SELECT 1,2,3 ) SELECT * FROM x;");
    assert!(result.is_err(), "empty CTE column list should not parse");
    let msg = result.unwrap_err().message;
    assert_eq!(msg, "near \")\": syntax error", "Unexpected error message: {}", msg);
}

#[test]
fn test_parse_cte_empty_column_list_via_arena_fallback() {
    // The arena parser handles `WITH ...` first; it must reject an empty column
    // list so the statement falls back to the standard parser and surfaces the
    // SQLite-compatible `near ")": syntax error` rather than silently accepting a
    // zero-column CTE (with2.test 4.1).
    let result = crate::parse_with_arena_fallback("WITH x() AS ( SELECT 1,2,3 ) SELECT * FROM x;");
    assert!(result.is_err(), "empty CTE column list should not parse via arena fallback");
    let msg = result.unwrap_err().message;
    assert_eq!(msg, "near \")\": syntax error", "Unexpected error message: {}", msg);
}
