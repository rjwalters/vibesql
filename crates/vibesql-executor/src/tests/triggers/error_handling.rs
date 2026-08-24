//! Tests for trigger error handling, rollback, and recursion prevention

use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

use super::create_users_table;
use crate::{InsertExecutor, SelectExecutor};

#[test]
fn test_trigger_failure_causes_rollback() {
    let mut db = Database::new();
    create_users_table(&mut db);

    // Create trigger that always fails (inserts into non-existent table)
    let trigger_stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "failing_trigger".to_string(),
        name_source: None,
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "users".to_string(),
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
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice"),
            )),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert);

    // Verify insert failed
    assert!(result.is_err(), "Insert should have failed due to trigger error");

    // Verify row was NOT inserted (rollback occurred)
    let select = vibesql_ast::SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            index_hint: None,
            quoted: false,
            name: "USERS".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        values: None,
    };
    let executor = SelectExecutor::new(&db);
    let rows = executor.execute(&select).expect("Failed to select");

    // Table should be empty (no rows inserted)
    assert_eq!(rows.len(), 0, "Table should be empty after failed trigger");
}

#[test]
fn test_recursion_prevention() {
    // The cap is `MAX_TRIGGER_RECURSION_DEPTH` (700, matching the order of
    // SQLite's SQLITE_MAX_TRIGGER_DEPTH; see #5479). An unconditional
    // self-inserting trigger recurses all the way to the cap before erroring,
    // and trigger recursion is native call-stack recursion (~50 KiB/level in
    // debug). 700 levels would overflow a default libtest worker stack, so run
    // the body on a thread with a generous stack: the cap must surface as a
    // clean error, not a stack overflow.
    std::thread::Builder::new()
        .name("test_recursion_prevention".to_string())
        .stack_size(96 * 1024 * 1024)
        .spawn(|| {
            let mut db = Database::new();
            // Recursion only happens with recursive_triggers ON; the default is
            // now OFF (SQLite default, #5840), which would suppress the re-entry
            // and let the insert succeed. Enable it to exercise the depth cap.
            db.set_recursive_triggers(true);
            create_users_table(&mut db);

            // Create trigger that inserts into the same table (infinite loop)
            let trigger_stmt = CreateTriggerStmt {
                if_not_exists: false,
                schema: None,
                trigger_name: "recursive_trigger".to_string(),
                name_source: None,
                timing: TriggerTiming::After,
                event: TriggerEvent::Insert,
                table_name: "users".to_string(),
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
                with_clause: None,
                schema_name: None,
                schema_quoted: false,
                table_quoted: false,
                table_name: "users".to_string(),
                columns: vec!["id".to_string(), "username".to_string()],
                source: vibesql_ast::InsertSource::Values(vec![vec![
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                        arcstr::ArcStr::from("alice"),
                    )),
                ]]),
                conflict_clause: None,
                on_conflict: vec![],
                on_duplicate_key_update: None,
                returning: None,
            };
            let result = InsertExecutor::execute(&mut db, &insert);

            // Verify insert failed with recursion error using SQLite's exact
            // wording (sqlite3 3.51.0, triggerC.test):
            // "too many levels of trigger recursion".
            assert!(result.is_err(), "Insert should have failed due to recursion limit");
            let err = result.unwrap_err();
            assert_eq!(err.to_string(), "too many levels of trigger recursion");
        })
        .expect("spawn recursion-test thread")
        .join()
        .expect("recursion cap must error, not overflow the stack");
}
