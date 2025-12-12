//! Tests for GRANT statement column-level privileges (SQL:1999 Feature E081-05, E081-07)

use vibesql_ast::*;

use crate::Parser;

#[test]
fn test_parse_grant_update_column_single() {
    let sql = "GRANT UPDATE(salary) ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Update(Some(vec!["salary".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "employees");
            assert_eq!(grant_stmt.grantees, vec!["manager"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_columns_multiple() {
    let sql = "GRANT UPDATE(salary, bonus, commission) ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Update(Some(vec![
                    "salary".to_string(),
                    "bonus".to_string(),
                    "commission".to_string()
                ]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_column_single() {
    let sql = "GRANT REFERENCES(id) ON TABLE users TO foreign_table";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::References(Some(vec!["id".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "users");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_columns_multiple() {
    let sql = "GRANT REFERENCES(user_id, account_id) ON TABLE accounts TO orders";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::References(Some(vec![
                    "user_id".to_string(),
                    "account_id".to_string()
                ]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_mixed_table_and_column_privileges() {
    let sql = "GRANT SELECT, UPDATE(salary), DELETE ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 3);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Select(None));
            assert_eq!(
                grant_stmt.privileges[1],
                PrivilegeType::Update(Some(vec!["salary".to_string()]))
            );
            assert_eq!(grant_stmt.privileges[2], PrivilegeType::Delete);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_column_with_grant_option() {
    let sql = "GRANT UPDATE(salary) ON TABLE employees TO manager WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Update(Some(vec!["salary".to_string()]))
            );
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_column_qualified_table() {
    let sql = "GRANT UPDATE(salary) ON TABLE hr.employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Update(Some(vec!["salary".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "HR.EMPLOYEES");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_column_case_insensitive() {
    let sql = "grant update(salary) on table employees to manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Update(Some(vec!["salary".to_string()]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_update_table_level() {
    // Test that UPDATE without columns still works (table-level privilege)
    let sql = "GRANT UPDATE ON TABLE employees TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Update(None));
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_references_table_level() {
    // Test that REFERENCES without columns still works (table-level privilege)
    let sql = "GRANT REFERENCES ON TABLE users TO foreign_table";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::References(None));
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_select_column_single() {
    // SQL:1999 Feature F031-03: Column-level SELECT privilege
    let sql = "GRANT SELECT(name) ON TABLE users TO analyst";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Select(Some(vec!["name".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "users");
            assert_eq!(grant_stmt.grantees, vec!["analyst"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_select_columns_multiple() {
    // SQL:1999 Feature F031-03: Multiple columns in SELECT privilege
    let sql = "GRANT SELECT(id, name, email) ON TABLE users TO analyst";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Select(Some(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "email".to_string()
                ]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_insert_column_single() {
    // SQL:1999 Feature F031-03: Column-level INSERT privilege
    let sql = "GRANT INSERT(name) ON TABLE users TO data_entry";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Insert(Some(vec!["name".to_string()]))
            );
            assert_eq!(grant_stmt.object_name.to_string(), "users");
            assert_eq!(grant_stmt.grantees, vec!["data_entry"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_insert_columns_multiple() {
    // SQL:1999 Feature F031-03: Multiple columns in INSERT privilege
    let sql = "GRANT INSERT(id, name, email) ON TABLE users TO data_entry";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Insert(Some(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "email".to_string()
                ]))
            );
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_select_table_level() {
    // Test that SELECT without columns still works (table-level privilege)
    let sql = "GRANT SELECT ON TABLE users TO analyst";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Select(None));
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_insert_table_level() {
    // Test that INSERT without columns still works (table-level privilege)
    let sql = "GRANT INSERT ON TABLE users TO data_entry";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Insert(None));
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_mixed_select_insert_column_privileges() {
    // Test mixing table-level and column-level SELECT/INSERT privileges
    let sql = "GRANT SELECT(id, name), INSERT(email), UPDATE ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 3);
            assert_eq!(
                grant_stmt.privileges[0],
                PrivilegeType::Select(Some(vec!["id".to_string(), "name".to_string()]))
            );
            assert_eq!(
                grant_stmt.privileges[1],
                PrivilegeType::Insert(Some(vec!["email".to_string()]))
            );
            assert_eq!(grant_stmt.privileges[2], PrivilegeType::Update(None));
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
