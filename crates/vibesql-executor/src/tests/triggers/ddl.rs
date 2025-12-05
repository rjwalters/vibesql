//! Tests for trigger DDL operations (CREATE TRIGGER, DROP TRIGGER)

use crate::CreateTableExecutor;
use vibesql_ast::{CreateTriggerStmt, DropTriggerStmt};
use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
use vibesql_storage::Database;

#[test]
fn test_create_trigger() {
    let mut db = Database::new();

    // Create a table first (since trigger references it)
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        trigger_name: "my_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create trigger: {:?}", result.err());

    // Verify trigger was created
    let trigger = db.catalog.get_trigger("my_trigger");
    assert!(trigger.is_some(), "Trigger not found after creation");
    let trigger = trigger.unwrap();
    assert_eq!(trigger.name, "my_trigger");
    assert_eq!(trigger.timing, TriggerTiming::Before);
    assert_eq!(trigger.event, TriggerEvent::Insert);
}

#[test]
fn test_create_trigger_duplicate_error() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        trigger_name: "my_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create trigger: {:?}", result.err());

    // Try to create another trigger with the same name
    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_err(), "Should fail when creating duplicate trigger");
}

#[test]
fn test_drop_trigger() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        trigger_name: "my_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    crate::advanced_objects::execute_create_trigger(&stmt, &mut db).unwrap();

    // Verify it was created
    assert!(db.catalog.get_trigger("my_trigger").is_some());

    // Drop the trigger
    let drop_stmt = DropTriggerStmt { trigger_name: "my_trigger".to_string(), cascade: false };

    let result = crate::advanced_objects::execute_drop_trigger(&drop_stmt, &mut db);
    assert!(result.is_ok(), "Failed to drop trigger: {:?}", result.err());

    // Verify it was dropped
    assert!(db.catalog.get_trigger("my_trigger").is_none(), "Trigger still exists after drop");
}

#[test]
fn test_drop_trigger_not_found() {
    let mut db = Database::new();

    let drop_stmt =
        DropTriggerStmt { trigger_name: "nonexistent_trigger".to_string(), cascade: false };

    let result = crate::advanced_objects::execute_drop_trigger(&drop_stmt, &mut db);
    assert!(result.is_err(), "Should fail when dropping non-existent trigger");
}

#[test]
fn test_create_trigger_all_variations() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Test different timing values
    let timings = vec![
        (TriggerTiming::Before, "before"),
        (TriggerTiming::After, "after"),
        (TriggerTiming::InsteadOf, "insteadof"),
    ];

    for (timing, suffix) in timings {
        let stmt = CreateTriggerStmt {
            trigger_name: format!("trigger_{}", suffix),
            timing: timing.clone(),
            event: TriggerEvent::Insert,
            table_name: "test_table".to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
        };

        let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
        assert!(result.is_ok(), "Failed to create {} trigger: {:?}", suffix, result.err());

        let trigger = db.catalog.get_trigger(&format!("trigger_{}", suffix)).unwrap();
        assert_eq!(trigger.timing, timing);
    }
}
