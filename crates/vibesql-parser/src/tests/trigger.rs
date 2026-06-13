//! Tests for CREATE TRIGGER and DROP TRIGGER parsing

use vibesql_ast::{Statement, TriggerEvent, TriggerGranularity, TriggerTiming};

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
fn test_create_trigger_for_each_statement() {
    let sql = "CREATE TRIGGER my_trigger BEFORE INSERT ON my_table FOR EACH STATEMENT BEGIN END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.trigger_name, "my_trigger");
            assert_eq!(trigger.granularity, TriggerGranularity::Statement);
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
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
fn test_create_trigger_update_of_columns() {
    let sql = "CREATE TRIGGER my_trigger BEFORE UPDATE OF (col1, col2) ON my_table BEGIN END;";
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
                assert!(up.contains("CASE"), "Body should retain the CASE expression. Got: {}", body);
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
    assert!(
        result.is_ok(),
        "trigger body with multiple CASE...END must parse: {:?}",
        result.err()
    );
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
    assert!(
        result.is_ok(),
        "trigger body with nested CASE...END must parse: {:?}",
        result.err()
    );

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
    assert!(
        result.is_ok(),
        "trigger body with CASE...END in WHERE must parse: {:?}",
        result.err()
    );
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
