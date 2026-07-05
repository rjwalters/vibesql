//! Tests for CREATE TRIGGER and DROP TRIGGER parsing

use vibesql_ast::{Expression, Statement, TriggerEvent, TriggerGranularity, TriggerTiming};

use crate::Parser;

#[test]
fn test_create_trigger_before_insert() {
    let sql = "CREATE TRIGGER my_trigger BEFORE INSERT ON my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            assert_eq!(trigger.timing, TriggerTiming::Before);
            assert_eq!(trigger.event, TriggerEvent::Insert);
            assert_eq!(trigger.table_name, "my_table");
            assert_eq!(trigger.granularity, TriggerGranularity::Row); // Default (SQLite compatible)
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_after_update() {
    let sql = "CREATE TRIGGER my_trigger AFTER UPDATE ON my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            assert_eq!(trigger.timing, TriggerTiming::After);
            assert!(matches!(trigger.event, TriggerEvent::Update(None)));
            assert_eq!(trigger.table_name, "my_table");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_instead_of_delete() {
    let sql = "CREATE TRIGGER my_trigger INSTEAD OF DELETE ON my_view BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            assert_eq!(trigger.timing, TriggerTiming::InsteadOf);
            assert_eq!(trigger.event, TriggerEvent::Delete);
            assert_eq!(trigger.table_name, "my_view");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_for_each_row() {
    let sql = "CREATE TRIGGER my_trigger BEFORE INSERT ON my_table FOR EACH ROW BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            assert_eq!(trigger.granularity, TriggerGranularity::Row);
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_for_each_statement_rejected() {
    // SQLite only supports `FOR EACH ROW`; `FOR EACH STATEMENT` is a syntax
    // error (`near "STATEMENT": syntax error`, trigger1-1.1.3). Verify we
    // reject it rather than silently accepting an unsupported granularity.
    let sql = "CREATE TRIGGER my_trigger BEFORE INSERT ON my_table FOR EACH STATEMENT BEGIN END;";
    let result = Parser::parse_sql(sql);
    let err = result.expect_err("FOR EACH STATEMENT should be rejected");
    assert_eq!(err.message, "near \"STATEMENT\": syntax error");
}

#[test]
fn test_create_trigger_if_not_exists_parsed() {
    // `CREATE TRIGGER IF NOT EXISTS ...` must parse and set the flag
    // (trigger1-1.2.0). Previously the `IF` keyword caused
    // `Expected identifier, found reserved keyword 'IF'`.
    let sql =
        "CREATE TRIGGER IF NOT EXISTS my_trigger AFTER INSERT ON my_table BEGIN SELECT 1; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateTrigger(trigger) => {
            assert!(trigger.if_not_exists, "if_not_exists should be true");
            assert_eq!(trigger.trigger_name, "my_trigger");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_without_if_not_exists_flag_absent() {
    // Without the clause, if_not_exists must be false.
    let sql = "CREATE TRIGGER my_trigger AFTER INSERT ON my_table BEGIN SELECT 1; END;";
    match Parser::parse_sql(sql).expect("should parse") {
        Statement::CreateTrigger(trigger) => {
            assert!(!trigger.if_not_exists, "if_not_exists should be false");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_if_not_exists_works_with_temp() {
    // The IF NOT EXISTS clause follows the optional TEMP modifier.
    let sql = "CREATE TEMP TRIGGER IF NOT EXISTS my_trigger AFTER INSERT ON my_table BEGIN SELECT 1; END;";
    match Parser::parse_sql(sql).expect("should parse") {
        Statement::CreateTrigger(trigger) => {
            assert!(trigger.if_not_exists);
            assert_eq!(trigger.trigger_name, "my_trigger");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_body_syntax_error_rejected_at_create_time() {
    // A genuine syntax error in a body statement (`SELECT * FROM;`) is
    // rejected by SQLite at CREATE TRIGGER time with `near ";": syntax error`
    // (trigger1-2.1). Verify we surface the same token-level syntax error
    // rather than silently accepting the body.
    let sql = "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN SELECT * FROM; END;";
    let err = Parser::parse_sql(sql).expect_err("body syntax error should be rejected");
    assert_eq!(err.message, "near \";\": syntax error");
}

#[test]
fn test_create_trigger_body_syntax_error_after_valid_statement() {
    // trigger1-2.2: first body statement is valid, second has the syntax
    // error; the error must still be raised at create time.
    let sql = "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN SELECT * FROM t1; SELECT * FROM; END;";
    let err = Parser::parse_sql(sql).expect_err("body syntax error should be rejected");
    assert_eq!(err.message, "near \";\": syntax error");
}

#[test]
fn test_create_trigger_with_when_condition() {
    let sql = "CREATE TRIGGER my_trigger BEFORE INSERT ON my_table WHEN (1 = 1) BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            assert!(trigger.when_condition.is_some(), "Expected WHEN condition");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_when_subquery() {
    // trigger2-3.2: SQLite accepts a subquery in the WHEN clause. The leading
    // `(` belongs to the scalar subquery, not a PostgreSQL-style wrapper, so the
    // full expression parser must see it (regression guard for the previous
    // optional-paren handling that mis-parsed `(SELECT ...) = 0`).
    let sql = "CREATE TRIGGER t2 BEFORE INSERT ON tbl \
               WHEN (SELECT count(*) FROM tbl) = 0 \
               BEGIN UPDATE log SET a = a + 1; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse WHEN subquery: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "t2");
            let when = trigger.when_condition.expect("Expected WHEN condition");
            // The condition is `<scalar subquery> = 0`.
            match when.as_ref() {
                Expression::BinaryOp { left, .. } => {
                    assert!(
                        matches!(left.as_ref(), Expression::ScalarSubquery(_)),
                        "Expected LHS of WHEN to be a scalar subquery, got {:?}",
                        left
                    );
                }
                other => panic!("Expected BinaryOp in WHEN, got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_update_of_columns() {
    // Standard SQLite (3.51.0): UNPARENTHESIZED comma-separated column list,
    // terminated by ON (trigger2-3.1). This is the canonical form.
    let sql = "CREATE TRIGGER my_trigger BEFORE UPDATE OF col1, col2 ON my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            match &trigger.event {
                TriggerEvent::Update(Some(cols)) => {
                    assert_eq!(cols.len(), 2);
                    assert_eq!(cols[0], "col1");
                    assert_eq!(cols[1], "col2");
                }
                _ => panic!("Expected UPDATE OF with columns"),
            }
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_update_of_single_column() {
    // Single unparenthesized column (sqlite3 accepts `UPDATE OF c`).
    let sql = "CREATE TRIGGER tr AFTER UPDATE OF c ON tbl BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateTrigger(trigger) => match &trigger.event {
            TriggerEvent::Update(Some(cols)) => {
                assert_eq!(cols, &vec!["c".to_string()]);
            }
            other => panic!("Expected UPDATE OF with single column, got {other:?}"),
        },
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_update_of_multi_after() {
    // Mirrors trigger2-3.1: AFTER UPDATE OF c, d ON tbl.
    let sql = "CREATE TRIGGER tbl_after_update_cd AFTER UPDATE OF c, d ON tbl BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.timing, TriggerTiming::After);
            assert_eq!(trigger.table_name, "tbl");
            match &trigger.event {
                TriggerEvent::Update(Some(cols)) => {
                    assert_eq!(cols, &vec!["c".to_string(), "d".to_string()]);
                }
                other => panic!("Expected UPDATE OF c, d, got {other:?}"),
            }
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_update_of_parenthesized_lenient() {
    // Lenient extension: VibeSQL still accepts the parenthesized form even
    // though sqlite3 3.51.0 rejects it. Retained for backwards compatibility.
    let sql = "CREATE TRIGGER my_trigger BEFORE UPDATE OF (col1, col2) ON my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateTrigger(trigger) => match &trigger.event {
            TriggerEvent::Update(Some(cols)) => {
                assert_eq!(cols, &vec!["col1".to_string(), "col2".to_string()]);
            }
            other => panic!("Expected UPDATE OF with columns, got {other:?}"),
        },
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_drop_trigger_basic() {
    let sql = "DROP TRIGGER my_trigger;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::DropTrigger(drop_trigger) => {
            assert_eq!(drop_trigger.trigger_name, "my_trigger");
            assert!(!drop_trigger.cascade); // Default is RESTRICT
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}

#[test]
fn test_drop_trigger_if_exists() {
    // `DROP TRIGGER IF EXISTS name` must parse (trigger1-1.4 / 1.6.1, #5497).
    let sql = "DROP TRIGGER IF EXISTS my_trigger;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::DropTrigger(drop_trigger) => {
            assert_eq!(drop_trigger.trigger_name, "my_trigger");
            assert!(drop_trigger.if_exists, "IF EXISTS flag should be set");
            assert!(!drop_trigger.cascade);
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}

#[test]
fn test_drop_trigger_basic_no_if_exists() {
    // Bare DROP TRIGGER leaves if_exists false.
    match Parser::parse_sql("DROP TRIGGER my_trigger;").unwrap() {
        Statement::DropTrigger(drop_trigger) => {
            assert!(!drop_trigger.if_exists);
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}

#[test]
fn test_drop_trigger_cascade() {
    let sql = "DROP TRIGGER my_trigger CASCADE;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::DropTrigger(drop_trigger) => {
            assert_eq!(drop_trigger.trigger_name, "my_trigger");
            assert!(drop_trigger.cascade);
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}

#[test]
fn test_drop_trigger_restrict() {
    let sql = "DROP TRIGGER my_trigger RESTRICT;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::DropTrigger(drop_trigger) => {
            assert_eq!(drop_trigger.trigger_name, "my_trigger");
            assert!(!drop_trigger.cascade);
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}

/// Test that omitting timing defaults to BEFORE (SQLite compatibility)
#[test]
fn test_create_trigger_optional_timing_defaults_to_before() {
    let sql = "CREATE TRIGGER my_trigger INSERT ON my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Should parse without timing: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            assert_eq!(trigger.timing, TriggerTiming::Before, "Default timing should be BEFORE");
            assert_eq!(trigger.event, TriggerEvent::Insert);
            assert_eq!(trigger.table_name, "my_table");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

/// Test omitting timing with UPDATE event
#[test]
fn test_create_trigger_optional_timing_update() {
    let sql = "CREATE TRIGGER my_trigger UPDATE ON my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Should parse without timing: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.timing, TriggerTiming::Before, "Default timing should be BEFORE");
            assert!(matches!(trigger.event, TriggerEvent::Update(None)));
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

/// Test omitting timing with DELETE event
#[test]
fn test_create_trigger_optional_timing_delete() {
    let sql = "CREATE TRIGGER my_trigger DELETE ON my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Should parse without timing: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.timing, TriggerTiming::Before, "Default timing should be BEFORE");
            assert_eq!(trigger.event, TriggerEvent::Delete);
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_missing_on() {
    let sql = "CREATE TRIGGER my_trigger BEFORE INSERT my_table BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "Should fail without ON keyword");
}

#[test]
fn test_create_trigger_missing_action() {
    let sql = "CREATE TRIGGER my_trigger BEFORE INSERT ON my_table;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "Should fail without triggered action");
}

#[test]
fn test_create_trigger_body_preserved_as_valid_sql() {
    use vibesql_ast::TriggerAction;

    let sql =
        "CREATE TRIGGER my_trigger AFTER INSERT ON my_table FOR EACH ROW BEGIN SELECT 1; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            match &trigger.triggered_action {
                TriggerAction::RawSql(body) => {
                    // Verify the body contains valid SQL that can be re-parsed
                    // Use case-insensitive checks since raw SQL keywords may be uppercase
                    let body_lower = body.to_lowercase();
                    assert!(body_lower.contains("select"), "Body should contain SELECT keyword");
                    assert!(body.contains("1"), "Body should contain the number 1");
                    assert!(body_lower.contains("begin"), "Body should contain BEGIN");
                    assert!(body_lower.contains("end"), "Body should contain END");
                    // Most importantly: body should NOT contain debug format like "Keyword(Select)"
                    assert!(
                        !body.contains("Keyword("),
                        "Body should NOT contain debug format. Got: {}",
                        body
                    );
                    assert!(
                        !body.contains("Number("),
                        "Body should NOT contain debug format. Got: {}",
                        body
                    );
                }
            }
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_body_with_string_literal() {
    use vibesql_ast::TriggerAction;

    let sql = "CREATE TRIGGER my_trigger AFTER INSERT ON my_table BEGIN INSERT INTO log VALUES ('test'); END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            match &trigger.triggered_action {
                TriggerAction::RawSql(body) => {
                    // Verify string literals are properly quoted
                    assert!(
                        body.contains("'test'"),
                        "Body should contain properly quoted string literal. Got: {}",
                        body
                    );
                }
            }
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_body_semicolon_inside_string_literal() {
    // Regression for #5408: a `;` inside a string literal must not split the
    // body into two malformed statements at create-time validation. SQLite
    // accepts this body; VibeSQL must parse it successfully and preserve the
    // full string (including the embedded `;`).
    use vibesql_ast::TriggerAction;

    let sql = "CREATE TRIGGER t AFTER INSERT ON x BEGIN \
               INSERT INTO log VALUES ('a;b'); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse body with ';' in string: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateTrigger(trigger) => match &trigger.triggered_action {
            TriggerAction::RawSql(body) => {
                assert!(
                    body.contains("'a;b'"),
                    "Body should preserve the full string literal containing ';'. Got: {}",
                    body
                );
            }
        },
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_body_escaped_quote_and_semicolon_in_string() {
    // #5408: SQLite escapes a single quote inside a string as `''`. A `;`
    // appearing after an escaped quote is still inside the literal and must
    // not split the statement.
    let sql = "CREATE TRIGGER t AFTER INSERT ON x BEGIN \
               INSERT INTO log VALUES ('o''reilly;jr'); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse body with escaped quote + ';' in string: {:?}",
        result.err()
    );
}

#[test]
fn test_create_trigger_multi_statement_body_with_semicolon_string() {
    // #5408: a multi-statement body where one statement contains a ';' in a
    // string must still split into exactly the real statements (the embedded
    // ';' is not a separator), and each must parse.
    let sql = "CREATE TRIGGER t AFTER INSERT ON x BEGIN \
               INSERT INTO log VALUES ('a;b'); \
               INSERT INTO log VALUES ('c'); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse multi-statement body: {:?}", result.err());
}

#[test]
fn test_create_temp_trigger_after_insert() {
    let sql = "CREATE TEMP TRIGGER tr2 AFTER INSERT ON t1 BEGIN UPDATE t1 SET b = 1; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "tr2");
            assert_eq!(trigger.timing, TriggerTiming::After);
            assert_eq!(trigger.event, TriggerEvent::Insert);
            assert_eq!(trigger.table_name, "t1");
            // The TEMP modifier places the trigger in the temp schema.
            assert_eq!(trigger.schema.as_deref(), Some("temp"));
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_temporary_trigger_after_insert() {
    let sql = "CREATE TEMPORARY TRIGGER tr2 AFTER INSERT ON t1 BEGIN UPDATE t1 SET b = 1; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "tr2");
            assert_eq!(trigger.timing, TriggerTiming::After);
            assert_eq!(trigger.event, TriggerEvent::Insert);
            assert_eq!(trigger.table_name, "t1");
            // TEMPORARY behaves identically to TEMP.
            assert_eq!(trigger.schema.as_deref(), Some("temp"));
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_temp_trigger_omitted_timing_defaults_to_before() {
    let sql = "CREATE TEMP TRIGGER tr3 UPDATE ON t1 BEGIN SELECT 1; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "tr3");
            assert_eq!(trigger.timing, TriggerTiming::Before);
            assert!(matches!(trigger.event, TriggerEvent::Update(None)));
            assert_eq!(trigger.table_name, "t1");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_create_trigger_schema_defaults_to_none() {
    // A plain CREATE TRIGGER (no TEMP, no schema prefix) has no schema.
    let sql = "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN SELECT 1; END;";
    match Parser::parse_sql(sql).expect("parse") {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "tr1");
            assert_eq!(trigger.schema, None);
        }
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_create_trigger_with_main_schema_qualifier() {
    // `CREATE TRIGGER main.r1 ...` (triggerD-3.1) parses with schema "main".
    let sql = "CREATE TRIGGER main.r1 AFTER INSERT ON t1 BEGIN SELECT 1; END;";
    match Parser::parse_sql(sql).expect("parse") {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "r1");
            assert_eq!(trigger.schema.as_deref(), Some("main"));
        }
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_create_trigger_with_temp_schema_qualifier() {
    // `CREATE TRIGGER temp.r1 ...` (triggerD-3.2) parses with the temp schema.
    // Note `temp` is a keyword, so this also exercises keyword-as-schema.
    let sql = "CREATE TRIGGER temp.r1 AFTER INSERT ON t1 BEGIN SELECT 1; END;";
    match Parser::parse_sql(sql).expect("parse") {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "r1");
            assert!(
                trigger.schema.as_deref().is_some_and(|s| s.eq_ignore_ascii_case("temp")),
                "expected temp schema, got {:?}",
                trigger.schema
            );
        }
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_create_temp_trigger_if_not_exists_sets_schema() {
    // TEMP modifier + IF NOT EXISTS: both flags set, schema is temp.
    let sql = "CREATE TEMP TRIGGER IF NOT EXISTS r1 AFTER INSERT ON t1 BEGIN SELECT 1; END;";
    match Parser::parse_sql(sql).expect("parse") {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "r1");
            assert!(trigger.if_not_exists);
            assert_eq!(trigger.schema.as_deref(), Some("temp"));
        }
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_create_temp_trigger_with_non_temp_schema_rejected() {
    // SQLite rejects combining `CREATE TEMP TRIGGER` with an explicit
    // non-temp schema, e.g. `CREATE TEMP TRIGGER main.r1`.
    let sql = "CREATE TEMP TRIGGER main.r1 AFTER INSERT ON t1 BEGIN SELECT 1; END;";
    assert!(
        Parser::parse_sql(sql).is_err(),
        "CREATE TEMP TRIGGER with explicit main schema must be rejected"
    );
}

#[test]
fn test_create_temp_table_and_view_still_parse() {
    // Regression checks: the CREATE TEMP dispatch must keep routing TABLE and
    // VIEW correctly after learning about TRIGGER.
    let table = Parser::parse_sql("CREATE TEMP TABLE t1(a INTEGER);");
    assert!(
        matches!(table, Ok(Statement::CreateTable(_))),
        "CREATE TEMP TABLE failed: {:?}",
        table
    );

    let temporary_table = Parser::parse_sql("CREATE TEMPORARY TABLE t1(a INTEGER);");
    assert!(
        matches!(temporary_table, Ok(Statement::CreateTable(_))),
        "CREATE TEMPORARY TABLE failed: {:?}",
        temporary_table
    );

    let view = Parser::parse_sql("CREATE TEMP VIEW v1 AS SELECT 1;");
    assert!(matches!(view, Ok(Statement::CreateView(_))), "CREATE TEMP VIEW failed: {:?}", view);
}

// --- Create-time trigger body validation (#5399) ---
//
// SQLite parses every trigger-body statement at CREATE TRIGGER time and
// rejects create-time errors then, rather than deferring them to fire
// time. VibeSQL stores the body as `RawSql` but now re-parses each body
// statement at create time so the same errors surface.

#[test]
fn test_create_trigger_body_nulls_in_conflict_target_rejected() {
    // nulls1.test 3.1.12: `NULLS FIRST/LAST` in an upsert ON CONFLICT target
    // is rejected for a direct statement, and must also be rejected inside a
    // trigger body at create time. SQLite: `unsupported use of NULLS FIRST`.
    let trigger_sql = "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN \
                 INSERT INTO t1 VALUES(1, 2, 3, 4) \
                 ON CONFLICT (b DESC NULLS FIRST) DO UPDATE SET a = a+1; \
               END;";
    let result = Parser::parse_sql(trigger_sql);
    let err = result.expect_err("trigger with NULLS in conflict target should be rejected");
    assert_eq!(
        err.message, "unsupported use of NULLS FIRST",
        "expected SQLite-compatible NULLS error, got: {}",
        err.message
    );
}

#[test]
fn test_create_trigger_body_unparseable_construct_tolerated() {
    // VibeSQL's parser does not yet support every construct valid inside a
    // SQLite trigger body, and SQLite accepts those at CREATE TRIGGER time.
    // Create-time validation must NOT hard-reject a body it merely cannot
    // parse — the body is preserved as RawSql and re-parsed at fire time,
    // exactly as before this validation existed. RAISE() (shown here) is now
    // parseable in a trigger body (#5409/#5416), so this also guards that the
    // create-time validation accepts a RAISE-bearing body.
    // (Regression guard for upsert1-1300, whose trigger body uses RAISE.)
    let sql = "CREATE TRIGGER tr2 BEFORE UPDATE ON t1 BEGIN \
               SELECT raise(ABORT, 'boom') WHERE old.y != new.y; \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "trigger body using a construct VibeSQL cannot parse (RAISE) should be \
         tolerated at create time (SQLite accepts it): {:?}",
        result.err()
    );
}

#[test]
fn test_create_trigger_valid_body_still_parses() {
    // A valid body must still parse and be stored as RawSql.
    let sql = "CREATE TRIGGER tr3 AFTER INSERT ON t1 BEGIN \
               INSERT INTO log VALUES('one'); \
               UPDATE t1 SET a = a + 1; \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "valid trigger body should parse: {:?}", result.err());
    match result.unwrap() {
        Statement::CreateTrigger(trigger) => match &trigger.triggered_action {
            vibesql_ast::TriggerAction::RawSql(body) => {
                assert!(body.to_uppercase().contains("INSERT INTO LOG"));
                assert!(body.to_uppercase().contains("UPDATE T1"));
            }
        },
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_create_trigger_body_referencing_unknown_table_still_parses() {
    // SQLite does NOT perform name resolution at CREATE TRIGGER time: a body
    // referencing a not-yet-created table is accepted. Create-time validation
    // is parse-only, so this must still parse successfully.
    let sql = "CREATE TRIGGER tr4 AFTER INSERT ON t1 BEGIN \
               INSERT INTO not_yet_created_table VALUES(1); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "trigger body referencing an unknown table should parse (no name resolution): {:?}",
        result.err()
    );
}

#[test]
fn test_create_trigger_empty_body_parses() {
    // An empty BEGIN/END body must remain valid (no statements to validate).
    let result = Parser::parse_sql("CREATE TRIGGER tr5 AFTER INSERT ON t1 BEGIN END;");
    assert!(result.is_ok(), "empty trigger body should parse: {:?}", result.err());
}

// --- CASE...END nesting in trigger bodies (#5439) ---
//
// The trigger-body token collector and statement splitter must track block
// nesting: a `CASE ... END` *expression* inside a body statement opens a block
// that its own `END` closes. The body's terminating `END` is the one at
// nesting depth 0 — so a body statement containing `CASE ... END` followed by
// another statement must not be truncated at the CASE's `END`.

#[test]
fn test_create_trigger_body_case_end_not_treated_as_body_terminator() {
    // Direct repro from #5439: a CASE ... END inside the first body statement
    // must not truncate the body — the trailing INSERT must be preserved.
    use vibesql_ast::TriggerAction;

    let sql = "CREATE TRIGGER trg INSTEAD OF INSERT ON vw BEGIN \
               SELECT CASE WHEN NEW.id = 1 THEN raise(IGNORE) END; \
               INSERT INTO base(id, v) VALUES(NEW.id, NEW.v); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "trigger body with CASE...END must not be truncated: {:?}",
        result.err()
    );

    match result.unwrap() {
        Statement::CreateTrigger(trigger) => match &trigger.triggered_action {
            TriggerAction::RawSql(body) => {
                let up = body.to_uppercase();
                // Both the CASE expression and the trailing INSERT survive.
                assert!(
                    up.contains("CASE"),
                    "Body should retain the CASE expression. Got: {}",
                    body
                );
                assert!(
                    up.contains("INSERT INTO BASE"),
                    "Body should retain the statement after the CASE...END. Got: {}",
                    body
                );
                // The body terminator END must be present (not consumed by CASE).
                assert!(up.trim_end().ends_with("END"), "Body should end with END. Got: {}", body);
            }
        },
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_create_trigger_body_multiple_case_expressions() {
    // Two separate body statements each containing a CASE...END.
    let sql = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
               UPDATE t SET v = CASE WHEN NEW.v > 0 THEN 'pos' ELSE 'neg' END; \
               INSERT INTO log VALUES(CASE WHEN NEW.v > 0 THEN 'a' ELSE 'b' END); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "trigger body with multiple CASE...END must parse: {:?}", result.err());
}

#[test]
fn test_create_trigger_body_nested_case_expressions() {
    // A nested CASE...END (CASE inside the THEN of an outer CASE) must close
    // both blocks before the body terminator.
    use vibesql_ast::TriggerAction;

    let sql = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
               UPDATE t SET v = CASE WHEN NEW.v > 0 \
                 THEN CASE WHEN NEW.v > 10 THEN 'big' ELSE 'small' END \
                 ELSE 'neg' END; \
               INSERT INTO log VALUES('done'); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "trigger body with nested CASE...END must parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateTrigger(trigger) => match &trigger.triggered_action {
            TriggerAction::RawSql(body) => {
                let up = body.to_uppercase();
                assert!(
                    up.contains("INSERT INTO LOG"),
                    "Body after nested CASE...END must be preserved. Got: {}",
                    body
                );
            }
        },
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_create_trigger_body_case_in_where_clause() {
    // CASE...END in a WHERE clause (different statement position).
    let sql = "CREATE TRIGGER trg AFTER UPDATE ON t BEGIN \
               UPDATE t SET v = 1 WHERE id = CASE WHEN NEW.id > 0 THEN NEW.id ELSE 0 END; \
               INSERT INTO log VALUES('w'); \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "trigger body with CASE...END in WHERE must parse: {:?}", result.err());
}

#[test]
fn test_create_trigger_body_case_as_last_statement_no_trailing_semicolon() {
    // A CASE...END in the final body statement (no trailing `;`) must not have
    // its END mistaken for the body terminator nor leave the body unterminated.
    let sql = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
               SELECT CASE WHEN NEW.v > 0 THEN 1 ELSE 0 END \
               END;";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "trigger body ending with CASE...END (no trailing ;) must parse: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// CREATE TRIGGER with a bound parameter / variable must be rejected at create
// time with SQLite's verbatim message `trigger cannot use variables`
// (triggerE.test 1.1.x / 1.2.x). A trigger program is compiled once and has no
// bind context, so SQLite rejects any `?`, `?NNN`, `:name`, `@name`, `$name`,
// `$NNN` reference in the WHEN clause or any body statement. NEW/OLD references
// are NOT variables and must still create fine.
// ---------------------------------------------------------------------------

/// SQLite 3.51.0 verbatim wording (see triggerE.test `set errmsg`).
const TRIGGER_VAR_ERR: &str = "trigger cannot use variables";

fn assert_rejected_as_variable(sql: &str) {
    let result = Parser::parse_sql(sql);
    let err = result.expect_err(&format!("expected rejection for: {sql}"));
    assert_eq!(err.message, TRIGGER_VAR_ERR, "wrong message for: {sql}\n  got: {}", err.message);
}

fn assert_trigger_accepted(sql: &str) {
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "trigger must be accepted: {sql}\n  err: {:?}", result.err());
}

#[test]
fn test_create_trigger_rejects_param_in_when_clause() {
    // triggerE-1.1.1: WHEN new.a = ?
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 WHEN new.a = ? BEGIN SELECT 1; END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_select_body() {
    // triggerE-1.1.2: SELECT ?
    assert_rejected_as_variable("CREATE TRIGGER tr1 BEFORE DELETE ON t1 BEGIN SELECT ?; END;");
}

#[test]
fn test_create_trigger_rejects_param_in_nested_subquery() {
    // triggerE-1.1.3: SELECT in nested subqueries
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE DELETE ON t1 BEGIN \
         SELECT * FROM (SELECT * FROM (SELECT ?)); END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_group_by() {
    // triggerE-1.1.5: GROUP BY ?
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE DELETE ON t1 BEGIN SELECT * FROM t2 GROUP BY ?; END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_limit() {
    // triggerE-1.1.6: LIMIT ?
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE DELETE ON t1 BEGIN SELECT * FROM t2 LIMIT ?; END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_order_by() {
    // triggerE-1.1.7: ORDER BY ?
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE DELETE ON t1 BEGIN SELECT * FROM t2 ORDER BY ?; END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_update_set() {
    // triggerE-1.1.8: UPDATE ... SET c = ?
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE UPDATE ON t1 BEGIN UPDATE t2 SET c = ?; END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_update_where() {
    // triggerE-1.1.9: UPDATE ... WHERE d = ?
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE UPDATE ON t1 BEGIN UPDATE t2 SET c = 1 WHERE d = ?; END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_function_arg() {
    // triggerE-1.1.10: function argument
    assert_rejected_as_variable("CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN SELECT abs(?); END;");
}

#[test]
fn test_create_trigger_rejects_numbered_param_dollar() {
    // triggerE-1.1.11 uses `$1` (multi-line window ORDER BY); cover the `$1`
    // form directly in an INSERT...SELECT body.
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE INSERT ON t1 BEGIN \
         INSERT INTO t1 SELECT max(b) OVER(ORDER BY $1) FROM t1; END;",
    );
}

#[test]
fn test_create_trigger_rejects_numbered_question_param() {
    // `?NNN` form.
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(?1, ?2); END;",
    );
}

#[test]
fn test_create_trigger_rejects_named_colon_param() {
    // `:name` form, in INSERT VALUES.
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(:x, :y); END;",
    );
}

#[test]
fn test_create_trigger_rejects_named_dollar_param() {
    // `$name` form, in WHERE.
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 BEFORE DELETE ON t1 BEGIN UPDATE t2 SET c = 1 WHERE d = $name; END;",
    );
}

#[test]
fn test_create_trigger_rejects_param_in_when_named() {
    // `:name` in the WHEN clause.
    assert_rejected_as_variable(
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 WHEN new.a = :v BEGIN SELECT 1; END;",
    );
}

#[test]
fn test_create_temp_trigger_rejects_param() {
    // triggerE-1.2.x: same rejection for CREATE TEMP TRIGGER.
    assert_rejected_as_variable(
        "CREATE TEMP TRIGGER tr1 AFTER INSERT ON t1 WHEN new.a = ? BEGIN SELECT 1; END;",
    );
}

// --- Negative controls: normal triggers must still be accepted. ---

#[test]
fn test_create_trigger_accepts_new_old_refs() {
    // NEW/OLD column references are NOT variables.
    assert_trigger_accepted(
        "CREATE TRIGGER tr1 AFTER UPDATE ON t1 WHEN NEW.a <> OLD.a BEGIN \
         INSERT INTO log VALUES(OLD.a, NEW.a); END;",
    );
}

#[test]
fn test_create_trigger_accepts_literals_and_columns() {
    // Literals, columns, functions, CASE — no variables.
    assert_trigger_accepted(
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 WHEN NEW.a = 1 BEGIN \
         UPDATE t2 SET c = NEW.a + 1 WHERE d = 'x'; \
         SELECT count(*) FROM t2 GROUP BY c ORDER BY c LIMIT 10; END;",
    );
}

#[test]
fn test_create_trigger_accepts_raise() {
    // RAISE() in the body is permitted and contains no variable.
    assert_trigger_accepted(
        "CREATE TRIGGER tr1 BEFORE INSERT ON t1 WHEN NEW.a < 0 BEGIN \
         SELECT RAISE(IGNORE); END;",
    );
}

// --- name_source: preserve the verbatim trigger-name spelling (issue #5527) ---
//
// SQLite echoes the trigger name *exactly as written* (including its quoting
// form) in the "trigger ... already exists" error (trigger1-1.2.2/1.2.3). The
// parser records that verbatim spelling in `CreateTriggerStmt::name_source`,
// while `trigger_name` holds the normalized (de-quoted) identifier.

fn parse_trigger_name_source(sql: &str) -> (String, Option<String>) {
    match Parser::parse_sql(sql).expect("parse failed") {
        Statement::CreateTrigger(t) => (t.trigger_name, t.name_source),
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_name_source_unquoted() {
    let (name, source) =
        parse_trigger_name_source("CREATE TRIGGER tr1 DELETE ON t1 BEGIN SELECT 1; END;");
    assert_eq!(name, "tr1");
    assert_eq!(source.as_deref(), Some("tr1"));
}

#[test]
fn test_name_source_double_quoted() {
    let (name, source) =
        parse_trigger_name_source("CREATE TRIGGER \"tr1\" DELETE ON t1 BEGIN SELECT 1; END;");
    assert_eq!(name, "tr1");
    assert_eq!(source.as_deref(), Some("\"tr1\""));
}

#[test]
fn test_name_source_bracket_quoted() {
    let (name, source) =
        parse_trigger_name_source("CREATE TRIGGER [tr1] DELETE ON t1 BEGIN SELECT 1; END;");
    assert_eq!(name, "tr1");
    assert_eq!(source.as_deref(), Some("[tr1]"));
}

#[test]
fn test_name_source_backtick_quoted() {
    let (name, source) =
        parse_trigger_name_source("CREATE TRIGGER `tr1` DELETE ON t1 BEGIN SELECT 1; END;");
    assert_eq!(name, "tr1");
    assert_eq!(source.as_deref(), Some("`tr1`"));
}

#[test]
fn test_drop_trigger_single_quoted_name() {
    // SQLite accepts a single-quoted string literal as the trigger name.
    let sql = "DROP TRIGGER 'my_trigger';";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::DropTrigger(drop_trigger) => {
            assert_eq!(drop_trigger.trigger_name, "my_trigger");
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}

#[test]
fn test_drop_trigger_if_exists_single_quoted_name() {
    let result = Parser::parse_sql("DROP TRIGGER IF EXISTS 'tr';");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::DropTrigger(drop_trigger) => {
            assert_eq!(drop_trigger.trigger_name, "tr");
            assert!(drop_trigger.if_exists);
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}
