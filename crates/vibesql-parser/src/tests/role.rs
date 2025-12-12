//! Tests for role statement parsing

use vibesql_ast::*;

use crate::Parser;

#[test]
fn test_create_role() {
    let sql = "CREATE ROLE manager";
    let result = Parser::parse_sql(sql).unwrap();

    match result {
        Statement::CreateRole(stmt) => {
            assert_eq!(stmt.role_name, "manager");
        }
        _ => panic!("Expected CreateRole statement"),
    }
}

#[test]
fn test_create_role_case_insensitive() {
    let sql = "create role analyst";
    let result = Parser::parse_sql(sql).unwrap();

    match result {
        Statement::CreateRole(stmt) => {
            assert_eq!(stmt.role_name, "analyst");
        }
        _ => panic!("Expected CreateRole statement"),
    }
}

#[test]
fn test_drop_role() {
    let sql = "DROP ROLE manager";
    let result = Parser::parse_sql(sql).unwrap();

    match result {
        Statement::DropRole(stmt) => {
            assert_eq!(stmt.role_name, "manager");
        }
        _ => panic!("Expected DropRole statement"),
    }
}

#[test]
fn test_drop_role_case_insensitive() {
    let sql = "drop role analyst";
    let result = Parser::parse_sql(sql).unwrap();

    match result {
        Statement::DropRole(stmt) => {
            assert_eq!(stmt.role_name, "analyst");
        }
        _ => panic!("Expected DropRole statement"),
    }
}
