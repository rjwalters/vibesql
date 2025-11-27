//! Tests for VIEW DDL statements (CREATE VIEW, DROP VIEW)

use crate::Parser;

#[test]
fn test_create_view_simple() {
    let sql = "CREATE VIEW my_view AS SELECT * FROM users";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateView(stmt)) = result {
        assert_eq!(stmt.view_name, "MY_VIEW");
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
            assert_eq!(stmt.view_name, "EMP_VIEW");
            assert!(stmt.columns.is_some());
            let cols = stmt.columns.unwrap();
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0], "ID");
            assert_eq!(cols[1], "NAME");
            assert_eq!(cols[2], "DEPT");
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
        assert_eq!(stmt.view_name, "ACTIVE_USERS");
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
        assert_eq!(stmt.view_name, "ORDER_DETAILS");
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
        assert_eq!(stmt.view_name, "DEPT_SUMMARY");
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
        assert_eq!(stmt.view_name, "ACTIVE_USERS");
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
        assert_eq!(stmt.view_name, "MYSCHEMA.MY_VIEW");
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
        assert_eq!(stmt.view_name, "MY_VIEW");
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
        assert_eq!(stmt.view_name, "MY_VIEW");
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
        assert_eq!(stmt.view_name, "MY_VIEW");
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
        assert_eq!(stmt.view_name, "MY_VIEW");
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
        assert_eq!(stmt.view_name, "MY_VIEW");
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
        assert_eq!(stmt.view_name, "MYSCHEMA.MY_VIEW");
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
        assert_eq!(stmt.view_name, "VIEW2");
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
        assert_eq!(stmt.view_name, "VIEW3");
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
        assert_eq!(stmt.view_name, "MY_VIEW");
        assert!(stmt.temporary);
        assert!(stmt.or_replace);
    } else {
        panic!("Expected CreateView statement");
    }
}
