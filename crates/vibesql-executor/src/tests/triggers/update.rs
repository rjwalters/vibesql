//! Tests for UPDATE trigger firing behavior

use super::{count_audit_rows, create_audit_table, create_users_table};
use crate::{InsertExecutor, UpdateExecutor};
use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

#[test]
fn test_after_update_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Insert a user first
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

    // Create AFTER UPDATE trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_update".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Update(None),
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('User updated')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Update the user - should fire trigger
    let update = vibesql_ast::UpdateStmt {
        table_name: "USERS".to_string(),
        assignments: vec![vibesql_ast::Assignment {
            column: "username".to_string(),
            value: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice_updated"))),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(
            vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Equal,
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    column: "id".to_string(),
                    table: None,
                }),
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(1),
                )),
            },
        )),
    };
    UpdateExecutor::execute(&update, &mut db).expect("Failed to update");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

#[test]
fn test_before_update_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Insert a user first
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

    // Create BEFORE UPDATE trigger
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_before_update".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Update(None),
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Before update')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Update the user - should fire trigger
    let update = vibesql_ast::UpdateStmt {
        table_name: "USERS".to_string(),
        assignments: vec![vibesql_ast::Assignment {
            column: "username".to_string(),
            value: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice_updated"))),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(
            vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Equal,
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    column: "id".to_string(),
                    table: None,
                }),
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(1),
                )),
            },
        )),
    };
    UpdateExecutor::execute(&update, &mut db).expect("Failed to update");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}
