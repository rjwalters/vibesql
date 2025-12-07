//! Tests for INSERT trigger firing behavior

use super::{count_audit_rows, create_audit_table, create_users_table};
use crate::InsertExecutor;
use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

#[test]
fn test_after_insert_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create AFTER INSERT trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_insert".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('User inserted')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert a row - should fire trigger
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify trigger fired by checking audit log
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_after_insert_trigger_fires_for_each_row() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create AFTER INSERT trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_insert".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Insert')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert 3 rows - should fire trigger 3 times
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("bob"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("charlie"))),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify trigger fired 3 times (once per row)
    assert_eq!(count_audit_rows(&db), 3);
}

#[test]
fn test_before_insert_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create BEFORE INSERT trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_before_insert".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Before insert')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert a row - should fire trigger
    let insert = vibesql_ast::InsertStmt {
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}
