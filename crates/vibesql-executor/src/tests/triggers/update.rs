//! Tests for UPDATE trigger firing behavior

use vibesql_ast::{
    CreateTriggerStmt, Statement, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::{count_audit_rows, create_audit_table, create_users_table};
use crate::{
    advanced_objects, CreateTableExecutor, InsertExecutor, SelectExecutor, UpdateExecutor,
};

#[test]
fn test_after_update_trigger_fires() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Insert a user first
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

    // Create AFTER UPDATE trigger
    let trigger_stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
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
        with_clause: None,
        quoted: false,
        alias: None,
        table_name: "USERS".to_string(),
        assignments: vec![vibesql_ast::Assignment {
            column: "username".to_string(),
            value: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice_updated"),
            )),
        }],
        from_clause: None,
        where_clause: Some(vibesql_ast::WhereClause::Condition(
            vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Equal,
                left: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::simple("id", false),
                )),
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(1),
                )),
            },
        )),
        conflict_clause: None,
        returning: None,
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

    // Create BEFORE UPDATE trigger
    let trigger_stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
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
        with_clause: None,
        quoted: false,
        alias: None,
        table_name: "USERS".to_string(),
        assignments: vec![vibesql_ast::Assignment {
            column: "username".to_string(),
            value: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("alice_updated"),
            )),
        }],
        from_clause: None,
        where_clause: Some(vibesql_ast::WhereClause::Condition(
            vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Equal,
                left: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::simple("id", false),
                )),
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(1),
                )),
            },
        )),
        conflict_clause: None,
        returning: None,
    };
    UpdateExecutor::execute(&update, &mut db).expect("Failed to update");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

/// Issue #5192 (triggerupfrom-4.3 regression):
///
/// UPDATE...FROM that targets a VIEW with an INSTEAD OF UPDATE trigger must
/// resolve column references to the FROM tables. Previously, the view-update
/// path built an evaluator from the view's schema only and silently dropped
/// `stmt.from_clause`, causing `UPDATE v1 SET a=map.v FROM map WHERE v1.k=map.k`
/// to fail with `no such column: map.k`.
///
/// This mirrors the failing test from
/// `docs/reference/sqlite/test/triggerupfrom.test` (test 4.3): an INSTEAD OF
/// UPDATE trigger on a view appends `(old.a,old.b)->(new.a,new.b)` entries to
/// a `log` table for each fired row, and the test verifies the log contains
/// the expected entries after `UPDATE v1 SET a=map.v FROM map WHERE v1.k=map.k`.
#[test]
fn test_update_from_on_view_with_instead_of_trigger() {
    let mut db = Database::new();

    // Setup: t1 (the underlying table), log (audit table), v1 (view over t1),
    // tr1 (INSTEAD OF UPDATE trigger on v1), map (the FROM-side table).
    let setup = [
        "CREATE TABLE t1(k VARCHAR(10), a INT, b VARCHAR(20))",
        "INSERT INTO t1 VALUES ('a', 1, 'one')",
        "INSERT INTO t1 VALUES ('b', 2, 'two')",
        "INSERT INTO t1 VALUES ('c', 3, 'three')",
        "INSERT INTO t1 VALUES ('d', 4, 'four')",
        "CREATE TABLE log(x VARCHAR(100))",
        // View renames b to __hidden__b so the trigger body uses both column
        // names — matching the SQLite test 4.3 setup that exercises the
        // ENABLE_HIDDEN_COLUMNS path.
        "CREATE VIEW v1 AS SELECT k, a, b AS __hidden__b FROM t1",
        "CREATE TABLE map(k VARCHAR(10), v VARCHAR(20))",
        "INSERT INTO map VALUES ('b', 'twelve')",
        "INSERT INTO map VALUES ('d', 'fourteen')",
    ];

    for sql in setup {
        let stmt = Parser::parse_sql(sql).expect("Failed to parse setup SQL");
        match stmt {
            Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, &mut db).expect("Failed CREATE TABLE");
            }
            Statement::CreateView(s) => {
                advanced_objects::execute_create_view(&s, &mut db).expect("Failed CREATE VIEW");
            }
            Statement::Insert(s) => {
                InsertExecutor::execute(&mut db, &s).expect("Failed INSERT");
            }
            other => panic!("Unexpected setup statement: {:?}", other),
        }
    }

    // Create the INSTEAD OF UPDATE trigger on v1. The trigger body uses raw
    // SQL with old/new references so it must run through the normal
    // trigger-body SQL execution path.
    let trigger_stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "tr1".to_string(),
        timing: TriggerTiming::InsteadOf,
        event: TriggerEvent::Update(None),
        table_name: "V1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO log VALUES('('||old.a||','||old.__hidden__b||')->('||new.a||','||new.__hidden__b||')')"
                .to_string(),
        ),
    };
    advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create INSTEAD OF trigger");

    // The bug: UPDATE v1 SET a=map.v FROM map WHERE v1.k=map.k previously
    // failed with "no such column: map.k" because execute_update_on_view
    // ignored stmt.from_clause and built a view-only evaluator.
    let update_sql = "UPDATE v1 SET a=map.v FROM map WHERE v1.k=map.k";
    let stmt = Parser::parse_sql(update_sql).expect("Failed to parse UPDATE...FROM");
    let update = match stmt {
        Statement::Update(s) => s,
        other => panic!("Expected Update statement, got {:?}", other),
    };

    let row_count = UpdateExecutor::execute(&update, &mut db)
        .expect("UPDATE v1 SET a=map.v FROM map WHERE v1.k=map.k should succeed");

    // Two view rows match (k='b' and k='d'), so two INSTEAD OF triggers
    // should fire.
    assert_eq!(row_count, 2, "Expected 2 rows processed (k='b' and k='d')");

    // Verify the log table received the expected entries.
    let select_log = "SELECT x FROM log ORDER BY x";
    let stmt = Parser::parse_sql(select_log).expect("Failed to parse SELECT");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("Expected Select statement, got {:?}", other),
    };
    let rows = SelectExecutor::new(&db).execute(&select).expect("SELECT failed");

    assert_eq!(rows.len(), 2, "log should contain 2 rows");

    // Sorted alphabetically: "(2,two)->(twelve,two)" < "(4,four)->(fourteen,four)"
    let extract_string = |row: &vibesql_storage::Row| -> String {
        match &row.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("Expected Varchar, got {:?}", other),
        }
    };

    assert_eq!(extract_string(&rows[0]), "(2,two)->(twelve,two)");
    assert_eq!(extract_string(&rows[1]), "(4,four)->(fourteen,four)");
}

/// Sanity-check for #5192: UPDATE...FROM on a view where the FROM-side has
/// zero matching rows should fire zero triggers and not error.
#[test]
fn test_update_from_on_view_zero_matches() {
    let mut db = Database::new();

    let setup = [
        "CREATE TABLE t1(k VARCHAR(10), a INT)",
        "INSERT INTO t1 VALUES ('a', 1)",
        "INSERT INTO t1 VALUES ('b', 2)",
        "CREATE TABLE log(x VARCHAR(100))",
        "CREATE VIEW v1 AS SELECT k, a FROM t1",
        "CREATE TABLE map(k VARCHAR(10), v INT)",
        // map is empty — no FROM-side rows.
    ];

    for sql in setup {
        let stmt = Parser::parse_sql(sql).expect("Failed to parse setup SQL");
        match stmt {
            Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, &mut db).expect("Failed CREATE TABLE");
            }
            Statement::CreateView(s) => {
                advanced_objects::execute_create_view(&s, &mut db).expect("Failed CREATE VIEW");
            }
            Statement::Insert(s) => {
                InsertExecutor::execute(&mut db, &s).expect("Failed INSERT");
            }
            other => panic!("Unexpected setup statement: {:?}", other),
        }
    }

    let trigger_stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "tr1".to_string(),
        timing: TriggerTiming::InsteadOf,
        event: TriggerEvent::Update(None),
        table_name: "V1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("INSERT INTO log VALUES('fired')".to_string()),
    };
    advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create INSTEAD OF trigger");

    let update_sql = "UPDATE v1 SET a=map.v FROM map WHERE v1.k=map.k";
    let stmt = Parser::parse_sql(update_sql).expect("Failed to parse UPDATE...FROM");
    let update = match stmt {
        Statement::Update(s) => s,
        other => panic!("Expected Update statement, got {:?}", other),
    };

    let row_count = UpdateExecutor::execute(&update, &mut db)
        .expect("UPDATE v1 FROM (empty map) should succeed with zero rows");
    assert_eq!(row_count, 0);

    // log should be empty.
    let select_log = "SELECT * FROM log";
    let stmt = Parser::parse_sql(select_log).expect("Failed to parse SELECT");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("Expected Select statement, got {:?}", other),
    };
    let rows = SelectExecutor::new(&db).execute(&select).expect("SELECT failed");
    assert_eq!(rows.len(), 0, "log should be empty when no FROM rows match");
}
