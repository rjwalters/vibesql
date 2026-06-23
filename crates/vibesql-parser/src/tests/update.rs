use super::*;

// ========================================================================
// UPDATE Statement Tests
// ========================================================================

#[test]
fn test_parse_update_basic() {
    let result = Parser::parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.assignments.len(), 1);
            assert_eq!(update.assignments[0].column, "name");
            assert!(update.where_clause.is_some());
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_indexed_by() {
    // SQLite INDEXED BY hint on UPDATE (issue #5734, where9-6.8.x).
    let result = Parser::parse_sql("UPDATE t1 INDEXED BY t1b SET a = a + 100 WHERE a = 1;");
    assert!(result.is_ok(), "UPDATE ... INDEXED BY should parse: {result:?}");

    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "t1");
            assert_eq!(update.alias, None);
            assert_eq!(
                update.index_hint,
                Some(vibesql_ast::IndexHint::IndexedBy("t1b".to_string()))
            );
            assert_eq!(update.assignments.len(), 1);
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_not_indexed() {
    // SQLite NOT INDEXED hint on UPDATE (issue #5734).
    let result = Parser::parse_sql("UPDATE t1 NOT INDEXED SET a = 1;");
    assert!(result.is_ok(), "UPDATE ... NOT INDEXED should parse: {result:?}");

    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.index_hint, Some(vibesql_ast::IndexHint::NotIndexed));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_alias_still_parses_with_hint_path() {
    // Ensure adding the index-hint hook did not break plain alias parsing.
    let result = Parser::parse_sql("UPDATE t1 x SET a = 1;");
    assert!(result.is_ok(), "UPDATE with alias should still parse: {result:?}");

    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "t1");
            assert_eq!(update.alias, Some("x".to_string()));
            assert_eq!(update.index_hint, None);
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_multiple_columns() {
    let result = Parser::parse_sql("UPDATE users SET name = 'Bob', age = 30;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.assignments.len(), 2);
            assert_eq!(update.assignments[0].column, "name");
            assert_eq!(update.assignments[1].column, "age");
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_with_default() {
    let result = Parser::parse_sql("UPDATE users SET name = DEFAULT WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.assignments.len(), 1);
            assert_eq!(update.assignments[0].column, "name");
            // Value should be DEFAULT
            assert!(matches!(update.assignments[0].value, vibesql_ast::Expression::Default));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_multiple_defaults() {
    let result = Parser::parse_sql("UPDATE users SET name = DEFAULT, age = DEFAULT;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.assignments.len(), 2);
            // Both values should be DEFAULT
            assert!(matches!(update.assignments[0].value, vibesql_ast::Expression::Default));
            assert!(matches!(update.assignments[1].value, vibesql_ast::Expression::Default));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

// ========================================================================
// UPDATE OR Conflict Resolution Tests (SQLite Extension)
// ========================================================================

#[test]
fn test_parse_update_or_replace() {
    let result = Parser::parse_sql("UPDATE OR REPLACE users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.conflict_clause, Some(vibesql_ast::ConflictClause::Replace));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_or_ignore() {
    let result = Parser::parse_sql("UPDATE OR IGNORE users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_or_abort() {
    let result = Parser::parse_sql("UPDATE OR ABORT users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.conflict_clause, Some(vibesql_ast::ConflictClause::Abort));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_or_rollback() {
    let result = Parser::parse_sql("UPDATE OR ROLLBACK users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.conflict_clause, Some(vibesql_ast::ConflictClause::Rollback));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_or_fail() {
    let result = Parser::parse_sql("UPDATE OR FAIL users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.conflict_clause, Some(vibesql_ast::ConflictClause::Fail));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_without_conflict_clause() {
    let result = Parser::parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert!(update.conflict_clause.is_none());
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

// ========================================================================
// RETURNING Clause Tests (SQLite 3.35.0+, issue #5233)
// ========================================================================

#[test]
fn test_parse_update_returning_star() {
    let result = Parser::parse_sql("UPDATE t1 SET a = 5 WHERE a = 4 RETURNING *;");
    assert!(result.is_ok(), "RETURNING * should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            let returning = update.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
            assert!(matches!(returning[0], vibesql_ast::SelectItem::Wildcard { .. }));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_returning_expression_list() {
    let result = Parser::parse_sql("UPDATE t1 SET a = 5 WHERE a = 4 RETURNING *, a + 1;");
    assert!(result.is_ok(), "RETURNING expr list should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            let returning = update.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
            assert!(matches!(returning[0], vibesql_ast::SelectItem::Wildcard { .. }));
            assert!(matches!(returning[1], vibesql_ast::SelectItem::Expression { .. }));
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_returning_with_alias() {
    let result = Parser::parse_sql("UPDATE t1 SET a = 5 RETURNING a AS new_a, a + 1 incremented;");
    assert!(result.is_ok(), "RETURNING aliases should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            let returning = update.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
            match &returning[0] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_deref(), Some("new_a"));
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
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_returning_arena_parser() {
    // The CLI path goes through parse_with_arena_fallback; make sure the
    // arena parser captures RETURNING too.
    let result =
        crate::parse_with_arena_fallback("UPDATE t1 SET a = 5 WHERE a = 4 RETURNING *, a + 1");
    assert!(result.is_ok(), "arena parser should handle RETURNING: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            let returning = update.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_without_returning_is_none() {
    let result = Parser::parse_sql("UPDATE t1 SET a = 5 WHERE a = 4;");
    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => assert!(update.returning.is_none()),
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_returning_still_usable_as_identifier() {
    // RETURNING is a fallback keyword in SQLite: it must remain usable as a
    // column name outside the DML RETURNING position.
    let result = Parser::parse_sql("UPDATE t1 SET returning = 5 WHERE returning = 4;");
    assert!(result.is_ok(), "'returning' as column name should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Update(update) => {
            assert_eq!(update.assignments[0].column, "returning");
        }
        _ => panic!("Expected UPDATE statement"),
    }
}
