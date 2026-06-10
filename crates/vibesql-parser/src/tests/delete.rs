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
            // Identifiers preserve original case from SQL
            assert_eq!(delete.table_name, "TABLE_E121_07_01_01");
            assert!(delete.where_clause.is_some());
            match delete.where_clause.unwrap() {
                vibesql_ast::WhereClause::CurrentOf(cursor) => {
                    assert_eq!(cursor, "CUR_E121_07_01_01");
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

// ========================================================================
// RETURNING Clause Tests (SQLite 3.35.0+, issue #5262)
// ========================================================================

#[test]
fn test_parse_delete_returning_star() {
    let result = Parser::parse_sql("DELETE FROM t1 WHERE a = 4 RETURNING *;");
    assert!(result.is_ok(), "RETURNING * should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Delete(delete) => {
            let returning = delete.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
            assert!(matches!(returning[0], vibesql_ast::SelectItem::Wildcard { .. }));
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_returning_expression_list() {
    let result = Parser::parse_sql("DELETE FROM t1 WHERE a = 4 RETURNING *, a + 1;");
    assert!(result.is_ok(), "RETURNING expr list should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Delete(delete) => {
            let returning = delete.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
            assert!(matches!(returning[0], vibesql_ast::SelectItem::Wildcard { .. }));
            assert!(matches!(returning[1], vibesql_ast::SelectItem::Expression { .. }));
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_returning_with_alias() {
    let result = Parser::parse_sql("DELETE FROM t1 RETURNING a AS old_a, a + 1 incremented;");
    assert!(result.is_ok(), "RETURNING aliases should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Delete(delete) => {
            let returning = delete.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
            match &returning[0] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_deref(), Some("old_a"));
                }
                other => panic!("Expected expression item, got {:?}", other),
            }
            match &returning[1] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_deref(), Some("incremented"));
                }
                other => panic!("Expected expression item, got {:?}", other),
            }
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_returning_after_order_by_limit() {
    let result = Parser::parse_sql("DELETE FROM t1 ORDER BY a DESC LIMIT 2 RETURNING a;");
    assert!(result.is_ok(), "RETURNING after ORDER BY/LIMIT should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Delete(delete) => {
            assert!(delete.order_by.is_some());
            assert!(delete.limit.is_some());
            let returning = delete.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_returning_with_cte() {
    let result = Parser::parse_sql(
        "WITH doomed AS (SELECT 1 AS x) DELETE FROM t1 WHERE a IN (SELECT x FROM doomed) RETURNING *;",
    );
    assert!(result.is_ok(), "RETURNING with CTE should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Delete(delete) => {
            assert!(delete.with_clause.is_some());
            let returning = delete.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_returning_arena_parser() {
    // The CLI path goes through parse_with_arena_fallback; make sure the
    // arena parser captures RETURNING too.
    let result = crate::parse_with_arena_fallback("DELETE FROM t1 WHERE a = 4 RETURNING *, a + 1");
    assert!(result.is_ok(), "arena parser should handle RETURNING: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Delete(delete) => {
            let returning = delete.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_without_returning_is_none() {
    let result = Parser::parse_sql("DELETE FROM t1 WHERE a = 4;");
    match result.unwrap() {
        vibesql_ast::Statement::Delete(delete) => assert!(delete.returning.is_none()),
        _ => panic!("Expected DELETE statement"),
    }
}
