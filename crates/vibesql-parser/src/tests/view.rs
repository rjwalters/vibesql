//! Tests for VIEW DDL statements (CREATE VIEW, DROP VIEW)

use crate::Parser;

#[test]
fn test_create_view_simple() {
    let sql = "CREATE VIEW my_view AS SELECT * FROM users";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(stmt.columns.is_none());
        assert!(!stmt.with_check_option);
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_with_column_list() {
    let sql = "CREATE VIEW emp_view (id, name, dept) AS SELECT employee_id, employee_name, department FROM employees";
    let result = Parser::parse_sql(sql);

    match result {
        Ok(vibesql_ast::Statement::CreateView(stmt)) => {
            assert_eq!(stmt.view_name, "emp_view");
            assert!(stmt.columns.is_some());
            let cols = stmt.columns.unwrap();
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0], "id");
            assert_eq!(cols[1], "name");
            assert_eq!(cols[2], "dept");
            assert!(!stmt.with_check_option);
        }
        Ok(other) => panic!("Expected CreateView, got: {:?}", other),
        Err(e) => panic!("Parse error: {}", e),
    }
}

#[test]
fn test_create_view_with_where_clause() {
    let sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "active_users");
        assert!(stmt.columns.is_none());
        assert!(!stmt.with_check_option);
        // Query contains WHERE clause
        assert!(stmt.query.where_clause.is_some());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_with_join() {
    let sql = "CREATE VIEW order_details AS SELECT o.order_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "order_details");
        // Query contains FROM clause with JOIN
        assert!(stmt.query.from.is_some());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_with_group_by() {
    let sql = "CREATE VIEW dept_summary AS SELECT dept, COUNT(*) as emp_count FROM employees GROUP BY dept";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "dept_summary");
        // Query contains GROUP BY
        assert!(stmt.query.group_by.is_some());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_with_check_option() {
    let sql =
        "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true WITH CHECK OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "active_users");
        assert!(stmt.with_check_option);
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_qualified_name() {
    let sql = "CREATE VIEW myschema.my_view AS SELECT * FROM users";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "myschema.my_view");
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_drop_view_simple() {
    let sql = "DROP VIEW my_view";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(!stmt.if_exists);
        assert!(!stmt.cascade);
        assert!(!stmt.restrict); // Neither CASCADE nor RESTRICT specified
    } else {
        panic!("Expected DropView statement");
    }
}

#[test]
fn test_drop_view_if_exists() {
    let sql = "DROP VIEW IF EXISTS my_view";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(stmt.if_exists);
        assert!(!stmt.cascade);
        assert!(!stmt.restrict);
    } else {
        panic!("Expected DropView statement");
    }
}

#[test]
fn test_drop_view_cascade() {
    let sql = "DROP VIEW my_view CASCADE";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(!stmt.if_exists);
        assert!(stmt.cascade);
        assert!(!stmt.restrict);
    } else {
        panic!("Expected DropView statement");
    }
}

#[test]
fn test_drop_view_restrict() {
    let sql = "DROP VIEW my_view RESTRICT";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(!stmt.if_exists);
        assert!(!stmt.cascade);
        assert!(stmt.restrict); // RESTRICT explicitly specified
    } else {
        panic!("Expected DropView statement");
    }
}

#[test]
fn test_drop_view_if_exists_cascade() {
    let sql = "DROP VIEW IF EXISTS my_view CASCADE";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(stmt.if_exists);
        assert!(stmt.cascade);
        assert!(!stmt.restrict);
    } else {
        panic!("Expected DropView statement");
    }
}

#[test]
fn test_drop_view_qualified_name() {
    let sql = "DROP VIEW myschema.my_view";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropView(stmt)) = result {
        assert_eq!(stmt.view_name, "myschema.my_view");
    } else {
        panic!("Expected DropView statement");
    }
}

#[test]
fn test_create_temp_view() {
    let sql = "CREATE TEMP VIEW view2 AS SELECT x FROM t1 WHERE x>0";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "view2");
        assert!(stmt.temporary);
        assert!(!stmt.or_replace);
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_temporary_view() {
    let sql = "CREATE TEMPORARY VIEW view3 AS SELECT x FROM t1 WHERE x>0";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "view3");
        assert!(stmt.temporary);
        assert!(!stmt.or_replace);
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_or_replace_temp_view() {
    let sql = "CREATE OR REPLACE TEMP VIEW my_view AS SELECT * FROM users";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(stmt.temporary);
        assert!(stmt.or_replace);
        assert!(!stmt.if_not_exists);
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_if_not_exists() {
    let sql = "CREATE VIEW IF NOT EXISTS v1 AS SELECT a,b FROM t1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "v1");
        assert!(stmt.if_not_exists);
        assert!(!stmt.or_replace);
        assert!(!stmt.temporary);
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_temp_view_if_not_exists() {
    let sql = "CREATE TEMP VIEW IF NOT EXISTS my_view AS SELECT * FROM users";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "my_view");
        assert!(stmt.temporary);
        assert!(stmt.if_not_exists);
        assert!(!stmt.or_replace);
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// VALUES clause as view source (Issue #4799)
// ============================================================================

#[test]
fn test_create_view_with_values_single_row() {
    // Classic Oracle-style dual view
    let sql = "CREATE VIEW dual(dummy) AS VALUES('x')";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "dual");
        assert!(stmt.columns.is_some());
        let cols = stmt.columns.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0], "dummy");
        // Query should have values
        assert!(stmt.query.values.is_some());
        let values = stmt.query.values.as_ref().unwrap();
        assert_eq!(values.len(), 1); // Single row
        assert_eq!(values[0].len(), 1); // Single column
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_with_values_multiple_rows() {
    // View with multiple rows
    let sql = "CREATE VIEW numbers(n) AS VALUES(1),(2),(3)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "numbers");
        assert!(stmt.query.values.is_some());
        let values = stmt.query.values.as_ref().unwrap();
        assert_eq!(values.len(), 3); // Three rows
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_with_values_multiple_columns() {
    // View with multiple columns
    let sql = "CREATE VIEW pairs(a, b) AS VALUES(1, 'one'),(2, 'two')";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "pairs");
        assert!(stmt.columns.is_some());
        let cols = stmt.columns.unwrap();
        assert_eq!(cols.len(), 2);
        assert!(stmt.query.values.is_some());
        let values = stmt.query.values.as_ref().unwrap();
        assert_eq!(values.len(), 2); // Two rows
        assert_eq!(values[0].len(), 2); // Two columns per row
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_temp_view_with_values() {
    let sql = "CREATE TEMP VIEW constants(name, value) AS VALUES('PI', 3.14159),('E', 2.71828)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "constants");
        assert!(stmt.temporary);
        assert!(stmt.query.values.is_some());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_if_not_exists_with_values() {
    let sql = "CREATE VIEW IF NOT EXISTS singleton(x) AS VALUES(42)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "singleton");
        assert!(stmt.if_not_exists);
        assert!(stmt.query.values.is_some());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_create_view_values_without_column_list() {
    // Column names should be derived from VALUES expression positions
    let sql = "CREATE VIEW data AS VALUES(1, 2, 3)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "data");
        assert!(stmt.columns.is_none()); // No explicit column list
        assert!(stmt.query.values.is_some());
    } else {
        panic!("Expected CreateView statement");
    }
}

// ---------------------------------------------------------------------------
// Issue #6304: CREATE VIEW must reject bind-parameter placeholders at DDL
// time with SQLite's verbatim message `parameters are not allowed in views`
// (with4.test case 130). A view is read repeatedly with no bind context, so
// SQLite rejects any `?`, `?NNN`, `:name`, `@name`, `$name` reference anywhere
// in the view body -- select list, WHERE, subqueries, CTEs, set-operation
// arms, LIMIT/OFFSET. `CREATE TRIGGER` already enforces the analogous
// `trigger cannot use variables` check (see `tests/trigger.rs`); this suite
// covers the `CREATE VIEW` side only. `CREATE TABLE ... AS SELECT` (CTAS) is
// executed once at create time, so SQLite allows binds there and this suite
// verifies VibeSQL does not overreach into CTAS.
//
// A `@name` case IS included below (issue #6172): VibeSQL's expression
// grammar now accepts `@name` as a general bind-parameter expression atom
// (SQLite: `@AAAA` works exactly like `:AAAA`, R-49783-61279), not just
// inside the MySQL-style `SELECT ... INTO @var` procedural clause. This
// view's own rejection check is unaffected either way: it scans the raw
// token stream (`token_is_variable`, shared with `CREATE TRIGGER`'s check)
// rather than the parsed AST, so `@name` was already rejected before this
// fix and stays rejected now -- the test below now exercises a genuinely
// parseable expression atom instead of one that merely fails to lex into
// anything meaningful.
// ---------------------------------------------------------------------------

/// SQLite verbatim wording (see with4.test `set msg`).
const VIEW_VAR_ERR: &str = "parameters are not allowed in views";

fn assert_view_rejected_as_variable(sql: &str) {
    let result = Parser::parse_sql(sql);
    let err = result.expect_err(&format!("expected rejection for: {sql}"));
    assert_eq!(err.message, VIEW_VAR_ERR, "wrong message for: {sql}\n  got: {}", err.message);
}

fn assert_view_accepted(sql: &str) {
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "must be accepted: {sql}\n  err: {:?}", result.err());
}

#[test]
fn test_create_view_rejects_numbered_placeholder_in_select_list() {
    // Issue #6304 repro: CREATE VIEW v2 AS SELECT ?2, * FROM t1;
    assert_view_rejected_as_variable("CREATE VIEW v2 AS SELECT ?2, * FROM t1;");
}

#[test]
fn test_create_view_rejects_bare_placeholder() {
    assert_view_rejected_as_variable("CREATE VIEW v AS SELECT ? FROM t1;");
}

#[test]
fn test_create_view_rejects_named_colon_placeholder() {
    assert_view_rejected_as_variable("CREATE VIEW v AS SELECT * FROM t1 WHERE a = :x;");
}

#[test]
fn test_create_view_rejects_named_dollar_placeholder() {
    assert_view_rejected_as_variable("CREATE VIEW v AS SELECT * FROM t1 WHERE a = $x;");
}

#[test]
fn test_create_view_rejects_named_at_placeholder() {
    // Issue #6172: `@name` is now a general expression-grammar bind
    // parameter (see `parse_primary_expression`'s `Token::UserVariable`
    // case), same class as `:x` and `$x` above.
    assert_view_rejected_as_variable("CREATE VIEW v AS SELECT * FROM t1 WHERE a = @x;");
}

#[test]
fn test_create_view_rejects_placeholder_in_where() {
    assert_view_rejected_as_variable("CREATE VIEW v AS SELECT * FROM t1 WHERE a = ?;");
}

#[test]
fn test_create_view_rejects_placeholder_in_limit() {
    // SQLite rejects binds in every clause of the view body, including LIMIT.
    assert_view_rejected_as_variable("CREATE VIEW v3 AS SELECT * FROM t1 LIMIT ?;");
}

#[test]
fn test_create_view_rejects_placeholder_in_offset() {
    assert_view_rejected_as_variable("CREATE VIEW v AS SELECT * FROM t1 LIMIT 1 OFFSET ?;");
}

#[test]
fn test_create_view_rejects_placeholder_in_subquery() {
    assert_view_rejected_as_variable(
        "CREATE VIEW v AS SELECT * FROM (SELECT * FROM t1 WHERE a = ?);",
    );
}

#[test]
fn test_create_view_rejects_placeholder_in_set_operation() {
    assert_view_rejected_as_variable("CREATE VIEW v AS SELECT a FROM t1 UNION SELECT ? FROM t1;");
}

#[test]
fn test_create_view_rejects_placeholder_in_cte() {
    // with4.test case 130: CREATE VIEW v2 AS WITH v(m,n) AS (SELECT 5,?2) SELECT * FROM t1, v;
    assert_view_rejected_as_variable(
        "CREATE VIEW v2 AS WITH v(m,n) AS (SELECT 5,?2) SELECT * FROM t1, v;",
    );
}

#[test]
fn test_create_temp_view_rejects_placeholder() {
    assert_view_rejected_as_variable("CREATE TEMP VIEW v AS SELECT ? FROM t1;");
}

#[test]
fn test_create_view_if_not_exists_rejects_placeholder() {
    assert_view_rejected_as_variable("CREATE VIEW IF NOT EXISTS v AS SELECT ? FROM t1;");
}

#[test]
fn test_create_view_plain_still_accepted() {
    assert_view_accepted("CREATE VIEW v AS SELECT * FROM t1;");
}

#[test]
fn test_create_table_as_select_still_allows_placeholder() {
    // CTAS is executed once at create time, so SQLite (and VibeSQL) permit
    // binds there -- the CREATE VIEW rejection must not overreach into CTAS.
    assert_view_accepted("CREATE TABLE t2 AS SELECT ?1 AS x;");
}
