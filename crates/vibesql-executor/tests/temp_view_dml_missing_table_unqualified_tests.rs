//! INSERT/UPDATE/DELETE on a view whose body references a missing table must
//! report that table schema-qualified (`main.<name>`) for a **main** view but
//! *unqualified* (bare `<name>`) for a **temp** view, matching sqlite3 3.51.0.
//!
//! A view body resolves its tables against its own schema: a main-schema view
//! body resolves against the database schema, so sqlite3 names a missing table
//! with the implicit `main.` prefix; a temp view body resolves against the temp
//! schema, which sqlite3 reports *without* a prefix. The three INSTEAD OF
//! view-DML paths (`delete/executor.rs`, `insert/execution.rs`,
//! `update/triggers.rs`) build a view pseudo-schema and now guard the `main.`
//! qualifier behind `ViewDefinition::is_temp()`, mirroring the read-path guard in
//! `select/scan/table.rs` (#5584) for full read/write parity. See #5589.
//!
//! Reachability note: the issue's motivating sequence (CREATE temp view over a
//! temp table, then DROP that temp table) is now reachable in VibeSQL — the
//! unqualified `DROP TABLE` of a temp table succeeds (the dependent temp
//! view/trigger is left dangling), matching sqlite3 (fixed in #5596; see
//! `drop_temp_table_with_dependents_tests.rs`). These tests use the equally
//! valid and fully reachable scenario of a view whose body references a
//! *never-existing* table: sqlite (and
//! VibeSQL) allow `CREATE [TEMP] VIEW` over a missing table with deferred
//! resolution, and the missing-table error surfaces at DML time through the same
//! `build_view_schema` qualifier path. Verified against sqlite3 3.51.0:
//!
//! ```text
//! sqlite> create temp view tv as select id, b from nonexistent;
//! sqlite> create temp trigger tv_ins instead of insert on tv begin select 1; end;
//! sqlite> insert into tv values(1,2);
//! Error: in prepare, no such table: nonexistent          -- temp: bare
//!
//! sqlite> create view mv as select id, b from nonexistent;
//! sqlite> create trigger mv_ins instead of insert on mv begin select 1; end;
//! sqlite> insert into mv values(1,2);
//! Error: in prepare, no such table: main.nonexistent     -- main: qualified
//! ```

use vibesql_executor::{
    DeleteExecutor, ExecutorError, InsertExecutor, TriggerExecutor, UpdateExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Parse and execute a single DDL statement (CREATE VIEW / CREATE TRIGGER),
/// panicking on failure.
fn exec_ok(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateView(s) => {
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        other => panic!("unsupported statement in exec_ok: {other:?}"),
    }
}

/// Build a temp view over a never-existing table plus temp INSTEAD OF triggers.
fn setup_temp_view(db: &mut Database) {
    exec_ok(db, "create temp view tv as select id, b from nonexistent");
    exec_ok(db, "create temp trigger tv_ins instead of insert on tv begin select 1; end");
    exec_ok(db, "create temp trigger tv_upd instead of update on tv begin select 1; end");
    exec_ok(db, "create temp trigger tv_del instead of delete on tv begin select 1; end");
    assert!(
        db.catalog.get_view("tv").expect("temp view tv should exist").is_temp(),
        "tv must be a temp view for this test to exercise the temp (bare) branch"
    );
}

/// Build a main-schema view over a never-existing table plus INSTEAD OF triggers.
fn setup_main_view(db: &mut Database) {
    exec_ok(db, "create view mv as select id, b from nonexistent");
    exec_ok(db, "create trigger mv_ins instead of insert on mv begin select 1; end");
    exec_ok(db, "create trigger mv_upd instead of update on mv begin select 1; end");
    exec_ok(db, "create trigger mv_del instead of delete on mv begin select 1; end");
    assert!(
        !db.catalog.get_view("mv").expect("main view mv should exist").is_temp(),
        "mv must be a main-schema view for this test to exercise the qualified branch"
    );
}

fn insert_err(db: &mut Database, sql: &str) -> ExecutorError {
    let vibesql_ast::Statement::Insert(s) = Parser::parse_sql(sql).unwrap() else {
        panic!("expected INSERT")
    };
    InsertExecutor::execute(db, &s).expect_err("expected INSERT to fail")
}

fn update_err(db: &mut Database, sql: &str) -> ExecutorError {
    let vibesql_ast::Statement::Update(s) = Parser::parse_sql(sql).unwrap() else {
        panic!("expected UPDATE")
    };
    UpdateExecutor::execute(&s, db).expect_err("expected UPDATE to fail")
}

fn delete_err(db: &mut Database, sql: &str) -> ExecutorError {
    let vibesql_ast::Statement::Delete(s) = Parser::parse_sql(sql).unwrap() else {
        panic!("expected DELETE")
    };
    DeleteExecutor::execute(&s, db).expect_err("expected DELETE to fail")
}

// --- temp view: missing body table stays bare (the #5589 fix) ---

#[test]
fn insert_on_temp_view_with_missing_body_table_stays_unqualified() {
    let mut db = Database::new();
    setup_temp_view(&mut db);
    assert_eq!(
        insert_err(&mut db, "insert into tv values(1,2)"),
        ExecutorError::TableNotFound("nonexistent".to_string()),
        "temp-view INSERT must report the missing body table bare (sqlite3 parity)"
    );
}

#[test]
fn update_on_temp_view_with_missing_body_table_stays_unqualified() {
    let mut db = Database::new();
    setup_temp_view(&mut db);
    assert_eq!(
        update_err(&mut db, "update tv set b=4 where id=1"),
        ExecutorError::TableNotFound("nonexistent".to_string()),
        "temp-view UPDATE must report the missing body table bare (sqlite3 parity)"
    );
}

#[test]
fn delete_on_temp_view_with_missing_body_table_stays_unqualified() {
    let mut db = Database::new();
    setup_temp_view(&mut db);
    assert_eq!(
        delete_err(&mut db, "delete from tv where id=1"),
        ExecutorError::TableNotFound("nonexistent".to_string()),
        "temp-view DELETE must report the missing body table bare (sqlite3 parity)"
    );
}

// --- main view: missing body table stays qualified (#5569 must not regress) ---

#[test]
fn insert_on_main_view_with_missing_body_table_stays_qualified() {
    let mut db = Database::new();
    setup_main_view(&mut db);
    assert_eq!(
        insert_err(&mut db, "insert into mv values(1,2)"),
        ExecutorError::TableNotFound("main.nonexistent".to_string()),
        "main-view INSERT must still report main.<name> (no regression to #5569)"
    );
}

#[test]
fn update_on_main_view_with_missing_body_table_stays_qualified() {
    let mut db = Database::new();
    setup_main_view(&mut db);
    assert_eq!(
        update_err(&mut db, "update mv set b=4 where id=1"),
        ExecutorError::TableNotFound("main.nonexistent".to_string()),
        "main-view UPDATE must still report main.<name> (no regression to #5569)"
    );
}

#[test]
fn delete_on_main_view_with_missing_body_table_stays_qualified() {
    let mut db = Database::new();
    setup_main_view(&mut db);
    assert_eq!(
        delete_err(&mut db, "delete from mv where id=1"),
        ExecutorError::TableNotFound("main.nonexistent".to_string()),
        "main-view DELETE must still report main.<name> (no regression to #5569)"
    );
}
