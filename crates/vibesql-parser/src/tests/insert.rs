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
            assert_eq!(insert.on_conflict.len(), 1);
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

#[test]
fn test_parse_insert_select_returning() {
    // The RETURNING keyword terminates the embedded SELECT source and belongs
    // to the outer INSERT (issue #5263); this must keep working after the
    // bare-SELECT RETURNING fix (issue #5271).
    let result = Parser::parse_sql("INSERT INTO t1 SELECT a FROM t2 RETURNING a;");
    assert!(result.is_ok(), "INSERT ... SELECT ... RETURNING should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            assert!(matches!(insert.source, vibesql_ast::InsertSource::Select(_)));
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_compound_select_returning() {
    // RETURNING after a compound SELECT source: the keyword follows the right
    // side of the set operation, so the allow_returning flag must propagate
    // through nested set-operation parsing (issue #5271).
    let result = Parser::parse_sql("INSERT INTO t1 SELECT 1 UNION SELECT 2 RETURNING a;");
    assert!(result.is_ok(), "compound SELECT source + RETURNING should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_compound_values_returning() {
    let result = Parser::parse_sql("INSERT INTO t1 VALUES (1) UNION VALUES (2) RETURNING a;");
    assert!(result.is_ok(), "compound VALUES source + RETURNING should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => {
            let returning = insert.returning.expect("returning clause should be captured");
            assert_eq!(returning.len(), 1);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_compound_values_order_by() {
    // Regression for issue #5714: a trailing ORDER BY on a compound VALUES
    // source of an INSERT must be consumed by the VALUES parser rather than
    // left for the outer statement-end check. Previously the un-consumed
    // ORDER BY produced `near "ORDER": syntax error`, masking the real
    // column-count-mismatch error that the executor reports later.
    let result = Parser::parse_sql(
        "INSERT INTO t2(rowid) VALUES(2) UNION SELECT 3,4 UNION SELECT 5,6 ORDER BY 1;",
    );
    assert!(
        result.is_ok(),
        "compound VALUES source + trailing ORDER BY should parse (not a syntax error): {:?}",
        result.err()
    );
    match result.unwrap() {
        vibesql_ast::Statement::Insert(insert) => match &insert.source {
            vibesql_ast::InsertSource::Select(select) => {
                assert!(
                    select.set_operation.is_some(),
                    "compound VALUES source should carry a set operation"
                );
                assert!(
                    select.order_by.is_some(),
                    "trailing ORDER BY should be attached to the compound VALUES source"
                );
            }
            other => panic!("Expected Select source for compound VALUES, got {:?}", other),
        },
        _ => panic!("Expected INSERT statement"),
    }
}

// ========================================================================
// Generalized UPSERT: multiple ON CONFLICT clauses (upsert5, issue #5817)
// ========================================================================

/// Parse a statement and return the INSERT's ON CONFLICT clause list.
fn parse_on_conflict_clauses(sql: &str) -> Vec<vibesql_ast::OnConflictClause> {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Insert(insert)) => insert.on_conflict,
        Ok(other) => panic!("Expected INSERT statement, got {:?}", other),
        Err(e) => panic!("Statement should parse: {:?}", e),
    }
}

#[test]
fn test_parse_insert_two_on_conflict_clauses() {
    let clauses = parse_on_conflict_clauses(
        "INSERT INTO t1(a,b) VALUES(1,2) \
         ON CONFLICT(a) DO UPDATE SET b='a' \
         ON CONFLICT(b) DO NOTHING;",
    );
    assert_eq!(clauses.len(), 2);
    assert!(clauses[0].conflict_target.is_some());
    assert!(matches!(clauses[0].action, vibesql_ast::OnConflictAction::DoUpdate { .. }));
    assert!(clauses[1].conflict_target.is_some());
    assert!(matches!(clauses[1].action, vibesql_ast::OnConflictAction::DoNothing));
}

#[test]
fn test_parse_insert_many_on_conflict_clauses() {
    let clauses = parse_on_conflict_clauses(
        "INSERT INTO t1(a,b,c,d,e) VALUES(1,NULL,3,4,5) \
         ON CONFLICT(a) DO UPDATE SET b='a' \
         ON CONFLICT(c) DO UPDATE SET b='c' \
         ON CONFLICT(d) DO UPDATE SET b='d' \
         ON CONFLICT(e) DO UPDATE SET b='e';",
    );
    assert_eq!(clauses.len(), 4);
    for clause in &clauses {
        assert!(clause.conflict_target.is_some());
        assert!(matches!(clause.action, vibesql_ast::OnConflictAction::DoUpdate { .. }));
    }
}

#[test]
fn test_parse_insert_targetless_terminal_clause_ok() {
    // A target-less clause is the catch-all and is allowed in the terminal
    // position (upsert5 1.x.400).
    let clauses = parse_on_conflict_clauses(
        "INSERT INTO t1(a,b) VALUES(1,2) \
         ON CONFLICT(a) DO UPDATE SET b='a' \
         ON CONFLICT DO UPDATE SET b='x';",
    );
    assert_eq!(clauses.len(), 2);
    assert!(clauses[1].conflict_target.is_none());
}

#[test]
fn test_parse_insert_targetless_non_terminal_clause_is_error() {
    // sqlite3 3.51: a target-less ON CONFLICT clause must be the LAST
    // clause; a following ON CONFLICT is a syntax error.
    let result = Parser::parse_sql(
        "INSERT INTO t1(a,b) VALUES(1,2) \
         ON CONFLICT DO NOTHING \
         ON CONFLICT(a) DO UPDATE SET b='a';",
    );
    let err = result.expect_err("non-terminal target-less clause must be a syntax error");
    assert!(
        err.message.contains("near \"ON\": syntax error"),
        "expected SQLite's near-ON syntax error, got: {:?}",
        err
    );
}

#[test]
fn test_parse_replace_into_with_on_conflict_clauses() {
    // upsert5 section 3: REPLACE INTO with (redundant) ON CONFLICT clauses.
    let result = Parser::parse_sql(
        "REPLACE INTO t1 VALUES(11,33) \
         ON CONFLICT(bb) DO UPDATE SET aa = 44 \
         ON CONFLICT(bb) DO UPDATE SET aa = 44;",
    );
    match result.expect("REPLACE INTO with ON CONFLICT clauses should parse") {
        vibesql_ast::Statement::Insert(insert) => {
            assert!(matches!(insert.conflict_clause, Some(vibesql_ast::ConflictClause::Replace)));
            assert_eq!(insert.on_conflict.len(), 2);
        }
        other => panic!("Expected INSERT statement, got {:?}", other),
    }
}

#[test]
fn test_parse_insert_on_conflict_then_on_duplicate_is_error() {
    // MySQL's ON DUPLICATE KEY UPDATE cannot be mixed with SQLite ON
    // CONFLICT clauses.
    let result = Parser::parse_sql(
        "INSERT INTO t1(a,b) VALUES(1,2) \
         ON CONFLICT(a) DO NOTHING \
         ON DUPLICATE KEY UPDATE b = 3;",
    );
    assert!(result.is_err(), "mixing ON CONFLICT and ON DUPLICATE KEY UPDATE must not parse");
}
