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
        name_source: None,
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
        index_hint: None,
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
        order_by: None,
        limit: None,
        offset: None,
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
        name_source: None,
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
        index_hint: None,
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
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
    };
    UpdateExecutor::execute(&update, &mut db).expect("Failed to update");

    // Verify trigger fired
    assert_eq!(count_audit_rows(&db), 1);
}

/// Issue #5577: `UPDATE OF <col>` firing-restriction semantics.
///
/// An `AFTER UPDATE OF username` trigger must fire only when the listed column
/// actually changes value. Updating an unlisted column (`id`) — or writing the
/// same value back to `username` — must NOT fire it. This exercises the parse
/// fix (unparenthesized column list) end-to-end together with the executor's
/// `should_fire_update_of` restriction.
#[test]
fn test_update_of_fires_only_for_listed_column() {
    let mut db = Database::new();
    create_users_table(&mut db);
    create_audit_table(&mut db);

    // Seed one user.
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

    // Parse the trigger with the standard unparenthesized column list to make
    // sure the parse fix and executor agree on the column.
    let trigger_stmt = match Parser::parse_sql(
        "CREATE TRIGGER log_username AFTER UPDATE OF username ON USERS \
         BEGIN INSERT INTO audit_log (event) VALUES ('username changed'); END;",
    )
    .expect("Failed to parse UPDATE OF trigger")
    {
        Statement::CreateTrigger(t) => t,
        other => panic!("Expected CreateTrigger, got {other:?}"),
    };
    assert_eq!(trigger_stmt.event, TriggerEvent::Update(Some(vec!["username".to_string()])));
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Helper: UPDATE one column of user id=1 to `value`.
    let update_col = |db: &mut Database, column: &str, value: vibesql_types::SqlValue| {
        let update = vibesql_ast::UpdateStmt {
            index_hint: None,
            with_clause: None,
            quoted: false,
            alias: None,
            table_name: "USERS".to_string(),
            assignments: vec![vibesql_ast::Assignment {
                column: column.to_string(),
                value: vibesql_ast::Expression::Literal(value),
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
            order_by: None,
            limit: None,
            offset: None,
            conflict_clause: None,
            returning: None,
        };
        UpdateExecutor::execute(&update, db).expect("Failed to update");
    };

    // Updating an unlisted column (id) must NOT fire the trigger.
    update_col(&mut db, "id", SqlValue::Integer(1));
    assert_eq!(count_audit_rows(&db), 0, "UPDATE OF must not fire for unlisted column");

    // Updating the listed column to a new value MUST fire the trigger.
    update_col(&mut db, "username", SqlValue::Varchar(arcstr::ArcStr::from("bob")));
    assert_eq!(count_audit_rows(&db), 1, "UPDATE OF must fire when listed column changes");
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
        name_source: None,
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

/// Regression test for #5703: an INSTEAD OF UPDATE trigger on a view whose
/// `SELECT *` over a JOIN expands to duplicate column names (two columns named
/// `c`) must resolve `new.c` to the FIRST `c` (the one targeted by the SET
/// assignment), not the last.
///
/// Root cause: `TableSchema::get_column_index` built its lookup cache via
/// last-write-wins `HashMap::collect`, while the view UPDATE row-builder uses
/// first-match `columns.iter().position()`. On a view with duplicate `c`
/// columns these disagreed, so the trigger body read the unchanged `c` slot and
/// the UPDATE became a no-op.
#[test]
fn test_instead_of_update_of_column_list_on_joined_view() {
    let mut db = Database::new();

    // t1 and t3 both have a column named `c`. `SELECT *` over the join expands
    // to columns (a, b, c, d, c, y) — two columns named `c`. v1.c (the first
    // one) is t1.c. The join on t1.a = t3.k pairs each t1 row with exactly one
    // t3 row, so each t1 row yields exactly one v1 row (one trigger firing).
    let setup = [
        "CREATE TABLE t1(a INT, b INT, c INT, d INT)",
        "CREATE TABLE t3(k INT, c INT, y INT)",
        "INSERT INTO t1 VALUES (1, 2, 230, 4)",
        "INSERT INTO t1 VALUES (5, 6, 236, 8)",
        "INSERT INTO t3 VALUES (1, 999, 30)",
        "INSERT INTO t3 VALUES (5, 998, 36)",
        // The view's SELECT * expands to (a, b, c, d, k, c, y) — two columns
        // named `c`. v1.c (the first match) is t1.c.
        "CREATE VIEW v1 AS SELECT * FROM t1 JOIN t3 ON t1.a = t3.k",
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

    // INSTEAD OF UPDATE OF c trigger: write new.c back into the underlying t1
    // row identified by old.a. If new.c resolves to the WRONG (last) `c` column,
    // it carries the unchanged t3.c value and t1.c will not reflect c+1000.
    let trigger_stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "v1r1".to_string(),
        name_source: None,
        timing: TriggerTiming::InsteadOf,
        event: TriggerEvent::Update(Some(vec!["c".to_string()])),
        table_name: "V1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "UPDATE t1 SET c = new.c WHERE a = old.a".to_string(),
        ),
    };
    advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create INSTEAD OF UPDATE OF c trigger");

    // UPDATE v1 SET c = c + 1000 — the SET targets the first `c` (t1.c, values
    // 230 and 236), so new.c should be 1230 and 1236.
    let update_sql = "UPDATE v1 SET c = c + 1000";
    let stmt = Parser::parse_sql(update_sql).expect("Failed to parse UPDATE");
    let update = match stmt {
        Statement::Update(s) => s,
        other => panic!("Expected Update statement, got {:?}", other),
    };
    UpdateExecutor::execute(&update, &mut db).expect("UPDATE v1 SET c=c+1000 should succeed");

    // Verify t1.c reflects c+1000 (1230, 1236) — proving new.c resolved to the
    // first-occurrence column and the trigger UPDATE took effect.
    let select_sql = "SELECT c FROM t1 ORDER BY a";
    let stmt = Parser::parse_sql(select_sql).expect("Failed to parse SELECT");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("Expected Select statement, got {:?}", other),
    };
    let rows = SelectExecutor::new(&db).execute(&select).expect("SELECT failed");

    let c_values: Vec<i64> = rows
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();

    assert_eq!(
        c_values,
        vec![1230, 1236],
        "t1.c must reflect c+1000 (new.c resolved to first-occurrence column); \
         unchanged 230/236 would indicate the split-brain bug (#5703)"
    );
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
        name_source: None,
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
