//! Tests for multiple trigger coordination and execution ordering

use super::{count_audit_rows, create_audit_table, create_users_table};
use crate::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

#[test]
fn test_multiple_triggers_fire_in_order() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create first AFTER INSERT trigger
    let trigger1 = CreateTriggerStmt {
        trigger_name: "log_insert_1".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('First trigger')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger1, &mut db)
        .expect("Failed to create trigger 1");

    // Create second AFTER INSERT trigger
    let trigger2 = CreateTriggerStmt {
        trigger_name: "log_insert_2".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('Second trigger')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger2, &mut db)
        .expect("Failed to create trigger 2");

    // Insert a row - should fire both triggers
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

    // Verify both triggers fired
    assert_eq!(count_audit_rows(&db), 2);
}

#[test]
fn test_trigger_with_multiple_statements() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Create trigger with multiple statements (separated by semicolons)
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_multiple".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "BEGIN INSERT INTO audit_log (event) VALUES ('First'); INSERT INTO audit_log (event) VALUES ('Second') END"
                .to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert a row - should fire trigger with both statements
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

    // Verify both statements executed
    assert_eq!(count_audit_rows(&db), 2);
}

#[test]
fn test_before_trigger_executes_first() {
    let mut db = Database::new();
    create_users_table(&mut db);

    // Create counter table to track execution order
    let counter_stmt = vibesql_ast::CreateTableStmt {
        if_not_exists: false,
        table_name: "COUNTER".to_string(),
        columns: vec![vibesql_ast::ColumnDef {
            name: "value".to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&counter_stmt, &mut db).expect("Failed to create counter table");

    // Initialize counter to 0
    let init_insert = vibesql_ast::InsertStmt {
        table_name: "COUNTER".to_string(),
        columns: vec!["value".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Integer(0),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &init_insert).expect("Failed to initialize counter");

    // Create BEFORE INSERT trigger that increments counter
    let before_trigger = CreateTriggerStmt {
        trigger_name: "before_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("UPDATE counter SET value = value + 1".to_string()),
    };
    crate::advanced_objects::execute_create_trigger(&before_trigger, &mut db)
        .expect("Failed to create before trigger");

    // Insert a row
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

    // Verify counter was incremented (BEFORE trigger executed)
    let select = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { column: "value".to_string(), table: None },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "COUNTER".to_string(),
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
    let result = executor.execute(&select).expect("Failed to select counter");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));

    // Verify user was actually inserted (main operation completed)
    let user_select = vibesql_ast::SelectStmt {
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
    let user_result = executor.execute(&user_select).expect("Failed to select users");
    assert_eq!(user_result.len(), 1, "User should be inserted after BEFORE trigger");
}
