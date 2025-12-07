//! Tests for trigger error handling, rollback, and recursion prevention

use super::create_users_table;
use crate::{InsertExecutor, SelectExecutor};
use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

#[test]
fn test_trigger_failure_causes_rollback() {
    let mut db = Database::new();
    create_users_table(&mut db);

    // Create trigger that always fails (inserts into non-existent table)
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "failing_trigger".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO nonexistent_table (col) VALUES (1)".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Try to insert - should fail due to trigger error
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
    let result = InsertExecutor::execute(&mut db, &insert);

    // Verify insert failed
    assert!(result.is_err(), "Insert should have failed due to trigger error");

    // Verify row was NOT inserted (rollback occurred)
    let select = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "USERS".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let executor = SelectExecutor::new(&db);
    let rows = executor.execute(&select).expect("Failed to select");

    // Table should be empty (no rows inserted)
    assert_eq!(rows.len(), 0, "Table should be empty after failed trigger");
}

#[test]
fn test_recursion_prevention() {
    let mut db = Database::new();
    create_users_table(&mut db);

    // Create trigger that inserts into the same table (infinite loop)
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "recursive_trigger".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO users (id, username) VALUES (999, 'recursive')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Try to insert - should fail with recursion depth error
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
    let result = InsertExecutor::execute(&mut db, &insert);

    // Verify insert failed with recursion error
    assert!(result.is_err(), "Insert should have failed due to recursion limit");
    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("recursion") || err_msg.contains("depth"),
        "Error should mention recursion or depth: {}",
        err_msg
    );
}
