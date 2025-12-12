//! Tests for GRANT statement parsing - Issue #561: GRANT/REVOKE on FUNCTION, PROCEDURE, ROUTINE
//! objects
//!
//! This module tests parsing of EXECUTE privilege on various routine-like objects:
//! - FUNCTION
//! - PROCEDURE
//! - ROUTINE
//! - METHOD (and variants: CONSTRUCTOR, STATIC, INSTANCE)
//!
//! These tests validate that the parser correctly handles:
//! - Different object types (FUNCTION, PROCEDURE, ROUTINE, METHOD variants)
//! - EXECUTE privilege on routine objects
//! - Implicit ROUTINE type when no object type is specified with EXECUTE
//! - WITH GRANT OPTION clause

use vibesql_ast::*;

use crate::Parser;

#[test]
fn test_parse_grant_execute_on_function() {
    let sql = "GRANT EXECUTE ON FUNCTION my_func TO user_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::Function);
            assert_eq!(grant_stmt.object_name.to_string(), "my_func");
            assert_eq!(grant_stmt.grantees, vec!["user_role"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_procedure() {
    let sql = "GRANT EXECUTE ON PROCEDURE proc_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::Procedure);
            assert_eq!(grant_stmt.object_name.to_string(), "proc_name");
            assert_eq!(grant_stmt.grantees, vec!["user1"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_routine() {
    let sql = "GRANT EXECUTE ON ROUTINE routine_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::Routine);
            assert_eq!(grant_stmt.object_name.to_string(), "routine_name");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_method() {
    let sql = "GRANT EXECUTE ON METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::Method);
            assert_eq!(grant_stmt.object_name.to_string(), "method_name");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_constructor_method() {
    let sql = "GRANT EXECUTE ON CONSTRUCTOR METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::ConstructorMethod);
            assert_eq!(grant_stmt.object_name.to_string(), "method_name");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_static_method() {
    let sql = "GRANT EXECUTE ON STATIC METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::StaticMethod);
            assert_eq!(grant_stmt.object_name.to_string(), "method_name");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_on_instance_method() {
    let sql = "GRANT EXECUTE ON INSTANCE METHOD method_name TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::InstanceMethod);
            assert_eq!(grant_stmt.object_name.to_string(), "method_name");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_implicit_routine() {
    // EXECUTE without object type should default to ROUTINE
    let sql = "GRANT EXECUTE ON my_func TO user1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::Routine);
            assert_eq!(grant_stmt.object_name.to_string(), "my_func");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_execute_with_grant_option() {
    let sql = "GRANT EXECUTE ON FUNCTION my_func TO user1 WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Execute);
            assert_eq!(grant_stmt.object_type, ObjectType::Function);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
