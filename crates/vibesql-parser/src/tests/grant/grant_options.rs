//! Tests for GRANT statement WITH GRANT OPTION clause
//!
//! SQL:1999 Core Feature E081-03: GRANT statement WITH GRANT OPTION

use vibesql_ast::*;

use crate::Parser;

#[test]
fn test_parse_grant_with_grant_option() {
    let sql = "GRANT SELECT ON TABLE users TO manager WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Select(None));
            assert_eq!(grant_stmt.object_type, ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "users");
            assert_eq!(grant_stmt.grantees, vec!["manager"]);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_without_grant_option() {
    let sql = "GRANT SELECT ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_schema_with_grant_option() {
    let sql = "GRANT USAGE ON SCHEMA public TO user_role WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "public");
            assert_eq!(grant_stmt.grantees, vec!["user_role"]);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_grant_with_grant_option_case_insensitive() {
    let sql = "grant select on table users to manager with grant option";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_grant_all_privileges_with_grant_option() {
    let sql = "GRANT ALL PRIVILEGES ON TABLE users TO manager WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::AllPrivileges);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
