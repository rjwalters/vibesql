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
            assert_eq!(
                update.conflict_clause,
                Some(vibesql_ast::ConflictClause::Replace)
            );
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
            assert_eq!(
                update.conflict_clause,
                Some(vibesql_ast::ConflictClause::Ignore)
            );
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
            assert_eq!(
                update.conflict_clause,
                Some(vibesql_ast::ConflictClause::Abort)
            );
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
            assert_eq!(
                update.conflict_clause,
                Some(vibesql_ast::ConflictClause::Rollback)
            );
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
            assert_eq!(
                update.conflict_clause,
                Some(vibesql_ast::ConflictClause::Fail)
            );
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
