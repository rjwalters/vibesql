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
            assert_eq!(trigger.granularity, TriggerGranularity::Statement); // Default
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
