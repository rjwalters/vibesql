use super::*;

// ========================================================================
// INSERT Statement Tests
// ========================================================================

#[test]
fn test_parse_insert_basic() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "users");
            assert_eq!(insert.columns.len(), 2);
            assert_eq!(insert.columns[0], "id");
            assert_eq!(insert.columns[1], "name");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1); // One row
                    assert_eq!(values[0].len(), 2); // Two values
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_insert_with_default() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "users");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1); // One row
                    assert_eq!(values[0].len(), 2); // Two values
                                                    // Check that first value is DEFAULT
                    match &values[0][0] {
                        vibesql_ast::Expression::Default => {
                            // Success - parsed as Default expression
                        }
                        _ => panic!("Expected DEFAULT expression, got {:?}", values[0][0]),
                    }
                    // Check that second value is a string literal
                    match &values[0][1] {
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => {
                            assert_eq!(s.as_str(), "Alice");
                        }
                        _ => panic!("Expected string literal 'Alice', got {:?}", values[0][1]),
                    }
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_insert_multiple_defaults() {
    let result = Parser::parse_sql("INSERT INTO t VALUES (DEFAULT, DEFAULT);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "t");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1); // One row
                    assert_eq!(values[0].len(), 2); // Two values
                                                    // Check that both values are DEFAULT
                    match &values[0][0] {
                        vibesql_ast::Expression::Default => {
                            // Success - parsed as Default expression
                        }
                        _ => panic!(
                            "Expected DEFAULT expression for first value, got {:?}",
                            values[0][0]
                        ),
                    }
                    match &values[0][1] {
                        vibesql_ast::Expression::Default => {
                            // Success - parsed as Default expression
                        }
                        _ => panic!(
                            "Expected DEFAULT expression for second value, got {:?}",
                            values[0][1]
                        ),
                    }
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_multiple_rows() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "users");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 2); // Two rows
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_with_default() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "users");
            assert_eq!(insert.columns.len(), 2);
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(values[0].len(), 2);
                    // First value should be DEFAULT
                    assert!(matches!(values[0][0], vibesql_ast::Expression::Default));
                    // Second value should be a literal
                    assert!(matches!(values[0][1], vibesql_ast::Expression::Literal(_)));
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_all_defaults() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (DEFAULT, DEFAULT);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(values[0].len(), 2);
                    // Both values should be DEFAULT
                    assert!(matches!(values[0][0], vibesql_ast::Expression::Default));
                    assert!(matches!(values[0][1], vibesql_ast::Expression::Default));
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

/// Test that INSERT statement can include rowid in column list (SQLite compatibility)
#[test]
fn test_parse_insert_with_rowid_column() {
    let result = Parser::parse_sql("INSERT INTO t1 (rowid, a, b) VALUES (100, 1, 'hello');");
    assert!(result.is_ok(), "Failed to parse INSERT with rowid: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "t1");
            assert_eq!(insert.columns.len(), 3);
            assert_eq!(insert.columns[0], "rowid");
            assert_eq!(insert.columns[1], "a");
            assert_eq!(insert.columns[2], "b");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1); // One row
                    assert_eq!(values[0].len(), 3); // Three values
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

/// Test that REPLACE statement can include rowid in column list (SQLite compatibility)
#[test]
fn test_parse_replace_with_rowid_column() {
    let result = Parser::parse_sql("REPLACE INTO t1 (rowid, a) VALUES (100, 1);");
    assert!(result.is_ok(), "Failed to parse REPLACE with rowid: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "t1");
            assert_eq!(insert.columns.len(), 2);
            assert_eq!(insert.columns[0], "rowid");
            assert_eq!(insert.columns[1], "a");
        }
        _ => panic!("Expected INSERT statement"),
    }
}

// ========================================================================
// RETURNING Clause Tests (SQLite 3.35.0+, issue #5263)
// ========================================================================

#[test]
fn test_parse_insert_returning_star() {
    let result = Parser::parse_sql("INSERT INTO t1(b) VALUES ('x') RETURNING *;");
    assert!(result.is_ok(), "RETURNING * should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
            assert!(matches!(returning[0], vibesql_ast::SelectItem::Wildcard { .. }));
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_returning_expression_list() {
    let result = Parser::parse_sql("INSERT INTO t1 VALUES (1, 2) RETURNING a, b + 1;");
    assert!(result.is_ok(), "RETURNING expr list should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
            assert!(matches!(returning[0], vibesql_ast::SelectItem::Expression { .. }));
            assert!(matches!(returning[1], vibesql_ast::SelectItem::Expression { .. }));
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_returning_with_alias() {
    let result =
        Parser::parse_sql("INSERT INTO t1 VALUES (1) RETURNING a AS new_a, a + 1 incremented;");
    assert!(result.is_ok(), "RETURNING aliases should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            let returning = insert.returning.expect("returning clause should be captured");
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
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_returning_after_on_conflict() {
    let result = Parser::parse_sql(
        "INSERT INTO t1(a, b) VALUES (1, 2) ON CONFLICT(a) DO UPDATE SET b = 3 RETURNING *;",
    );
    assert!(result.is_ok(), "RETURNING after ON CONFLICT should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            assert!(insert.on_conflict.is_some());
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_returning_with_cte() {
    let result = Parser::parse_sql(
        "WITH src AS (SELECT 1 AS x) INSERT INTO t1 SELECT x FROM src RETURNING *;",
    );
    assert!(result.is_ok(), "RETURNING with CTE should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            assert!(insert.with_clause.is_some());
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_replace_into_returning() {
    let result = Parser::parse_sql("REPLACE INTO t1(a, b) VALUES (1, 'x') RETURNING a, b;");
    assert!(result.is_ok(), "REPLACE INTO RETURNING should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(
                insert.conflict_clause,
                Some(vibesql_ast::ConflictClause::Replace),
                "REPLACE INTO should set the Replace conflict clause"
            );
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_returning_arena_parser() {
    // The CLI path goes through parse_with_arena_fallback; make sure the
    // arena parser captures RETURNING too.
    let result = crate::parse_with_arena_fallback("INSERT INTO t1(b) VALUES ('x') RETURNING a, b");
    assert!(result.is_ok(), "arena parser should handle RETURNING: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 2);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_replace_returning_arena_parser() {
    let result =
        crate::parse_with_arena_fallback("REPLACE INTO t1(a, b) VALUES (1, 'x') RETURNING *");
    assert!(result.is_ok(), "arena parser should handle REPLACE RETURNING: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_without_returning_is_none() {
    let result = Parser::parse_sql("INSERT INTO t1 VALUES (1);");
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => assert!(insert.returning.is_none()),
        _ => panic!("Expected INSERT statement"),
    }
}
