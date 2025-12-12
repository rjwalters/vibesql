use super::*;

// ========================================================================
// DELETE Statement Tests
// ========================================================================

#[test]
fn test_parse_delete_basic() {
    let result = Parser::parse_sql("DELETE FROM users WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Delete(delete) => {
            assert_eq!(delete.table_name, "users");
            assert!(delete.where_clause.is_some());
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_no_where() {
    let result = Parser::parse_sql("DELETE FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Delete(delete) => {
            assert_eq!(delete.table_name, "users");
            assert!(delete.where_clause.is_none());
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_only() {
    let result = Parser::parse_sql("DELETE FROM ONLY users WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Delete(delete) => {
            assert!(delete.only, "ONLY flag should be true");
            assert_eq!(delete.table_name, "users");
            assert!(delete.where_clause.is_some());
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_only_with_parentheses() {
    let result = Parser::parse_sql("DELETE FROM ONLY (users) WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Delete(delete) => {
            assert!(delete.only, "ONLY flag should be true");
            assert_eq!(delete.table_name, "users");
            assert!(delete.where_clause.is_some());
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_parentheses_no_only() {
    let result = Parser::parse_sql("DELETE FROM (users) WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Delete(delete) => {
            assert!(!delete.only, "ONLY flag should be false");
            assert_eq!(delete.table_name, "users");
            assert!(delete.where_clause.is_some());
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_where_current_of() {
    let result = Parser::parse_sql("DELETE FROM users WHERE CURRENT OF my_cursor;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Delete(delete) => {
            assert!(!delete.only, "ONLY flag should be false");
            assert_eq!(delete.table_name, "users");
            assert!(delete.where_clause.is_some());
            match delete.where_clause.unwrap() {
                vibesql_ast::WhereClause::CurrentOf(cursor) => {
                    assert_eq!(cursor, "my_cursor");
                }
                _ => panic!("Expected WHERE CURRENT OF clause"),
            }
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_only_with_parentheses_and_current_of() {
    // This is the full test case from issue #748
    let result = Parser::parse_sql(
        "DELETE FROM ONLY (TABLE_E121_07_01_01) WHERE CURRENT OF CUR_E121_07_01_01;",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Delete(delete) => {
            assert!(delete.only, "ONLY flag should be true");
            assert_eq!(delete.table_name, "table_e121_07_01_01");
            assert!(delete.where_clause.is_some());
            match delete.where_clause.unwrap() {
                vibesql_ast::WhereClause::CurrentOf(cursor) => {
                    assert_eq!(cursor, "cur_e121_07_01_01");
                }
                _ => panic!("Expected WHERE CURRENT OF clause"),
            }
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_mismatched_parentheses() {
    let result = Parser::parse_sql("DELETE FROM (users WHERE id = 1;");
    assert!(result.is_err(), "Should fail with mismatched parentheses");
}

#[test]
fn test_parse_delete_only_no_table() {
    let result = Parser::parse_sql("DELETE FROM ONLY;");
    assert!(result.is_err(), "Should fail when table name is missing after ONLY");
}
