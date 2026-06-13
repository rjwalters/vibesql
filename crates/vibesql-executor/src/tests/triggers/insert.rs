//! Tests for INSERT trigger firing behavior

use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

use super::{count_audit_rows, create_audit_table, create_users_table};
use crate::{InsertExecutor, SelectExecutor};

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
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice"),
            )),
        ]]),
        conflict_clause: None,
        on_conflict: None,
        on_duplicate_key_update: None,
        returning: None,
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
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    arcstr::ArcStr::from("alice"),
                )),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    arcstr::ArcStr::from("bob"),
                )),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    arcstr::ArcStr::from("charlie"),
                )),
            ],
        ]),
        conflict_clause: None,
        on_conflict: None,
        on_duplicate_key_update: None,
        returning: None,
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
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice"),
            )),
        ]]),
        conflict_clause: None,
        on_conflict: None,
        on_duplicate_key_update: None,
        returning: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

/// Read the single `event` value from the audit_log (created by
/// `create_audit_table`). Panics if there is not exactly one row.
fn single_audit_event(db: &Database) -> String {
    let stmt = vibesql_parser::Parser::parse_sql("SELECT event FROM audit_log;").unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(stmt) => {
            SelectExecutor::new(db).execute_with_columns(&stmt).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    assert_eq!(result.rows.len(), 1, "expected exactly one audit row");
    match &result.rows[0].values[0] {
        vibesql_types::SqlValue::Varchar(s) => s.to_string(),
        other => panic!("expected Varchar audit value, got {:?}", other),
    }
}

#[test]
fn test_insert_trigger_body_semicolon_inside_string_literal_is_not_split() {
    // Regression for #5408: when a trigger body statement contains a `;`
    // inside a string literal, the fire-time splitter must NOT break it into
    // two malformed fragments. The full string (including the embedded `;`)
    // must be stored intact. Verified against sqlite3 3.51.x:
    //   sqlite> CREATE TABLE x(id INT);
    //   sqlite> CREATE TABLE audit_log(event TEXT);
    //   sqlite> CREATE TRIGGER t AFTER INSERT ON x BEGIN
    //             INSERT INTO audit_log(event) VALUES('a;b'); END;
    //   sqlite> INSERT INTO x VALUES(1);
    //   sqlite> SELECT event FROM audit_log;  -- => a;b
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_semicolon".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "BEGIN INSERT INTO audit_log (event) VALUES ('a;b'); END".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    let insert = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice"),
            )),
        ]]),
        conflict_clause: None,
        on_conflict: None,
        on_duplicate_key_update: None,
        returning: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Exactly one audit row, with the full string preserved (not truncated to
    // "a" and not split into a second malformed statement).
    assert_eq!(count_audit_rows(&db), 1);
    assert_eq!(single_audit_event(&db), "a;b");
}

#[test]
fn test_insert_trigger_body_escaped_quote_with_semicolon_is_intact() {
    // #5408: SQLite escapes a single quote inside a string as `''`. A `;`
    // after the escaped quote stays inside the literal. Verified against
    // sqlite3 3.51.x: the stored value is `o'reilly;jr`.
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_escaped".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "BEGIN INSERT INTO audit_log (event) VALUES ('o''reilly;jr'); END".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    let insert = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice"),
            )),
        ]]),
        conflict_clause: None,
        on_conflict: None,
        on_duplicate_key_update: None,
        returning: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    assert_eq!(count_audit_rows(&db), 1);
    assert_eq!(single_audit_event(&db), "o'reilly;jr");
}

#[test]
fn test_insert_trigger_multi_statement_body_with_semicolon_string() {
    // #5408: a multi-statement body where one statement has a `;` in a string
    // must split into exactly the real statements. Both audit rows must be
    // written, with the `;`-containing value preserved intact.
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_multi".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "USERS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "BEGIN \
             INSERT INTO audit_log (event) VALUES ('a;b'); \
             INSERT INTO audit_log (event) VALUES ('c'); \
             END"
                .to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    let insert = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "USERS".to_string(),
        columns: vec!["id".to_string(), "username".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice"),
            )),
        ]]),
        conflict_clause: None,
        on_conflict: None,
        on_duplicate_key_update: None,
        returning: None,
    };
    InsertExecutor::execute(&mut db, &insert).expect("Failed to insert");

    // Both statements ran: two audit rows (not one truncated, not three split).
    assert_eq!(count_audit_rows(&db), 2);
}
