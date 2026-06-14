//! `DROP TABLE` of a temp table must succeed even while a dependent temp
//! view/trigger references it, matching sqlite3 3.51.0. The dependent object's
//! body is left dangling and only errors when next accessed (bare missing-table
//! name, per #5589's qualifier guards).
//!
//! Root cause this exercises (see #5596): the catalog/storage `drop_table` paths
//! resolved an *unqualified* name only against the current (`main`) schema, so a
//! bare `DROP TABLE t` for a temp table failed with "no such table" — even with
//! no dependents at all. The fix follows SQLite name resolution (temp shadows
//! main) for unqualified DROP, so the temp table is found and removed from its
//! temp schema, leaving any same-named main table intact.
//!
//! Verified against sqlite3 3.51.0:
//!
//! ```text
//! sqlite> create temp table t2(id integer, b);
//! sqlite> create temp view tv as select id, b from t2;
//! sqlite> drop table t2;          -- succeeds (dependent view left dangling)
//! sqlite> select * from tv;
//! Error: in prepare, no such table: t2
//!
//! -- temp shadows main for an unqualified DROP:
//! sqlite> create table t(id integer); insert into t values(99);
//! sqlite> create temp table t(id integer); insert into t values(1);
//! sqlite> drop table t;           -- drops the temp t
//! sqlite> select * from t;        -- main t survives
//! 99
//! ```

use vibesql_executor::{
    CreateTableExecutor, DropTableExecutor, ExecutorError, InsertExecutor, SelectExecutor,
    TriggerExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Parse and execute a single DDL/DML statement, panicking on failure.
fn exec_ok(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateView(s) => {
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        other => panic!("unsupported statement in exec_ok: {other:?}"),
    }
}

fn drop_table(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let vibesql_ast::Statement::DropTable(s) = Parser::parse_sql(sql).unwrap() else {
        panic!("expected DROP TABLE")
    };
    DropTableExecutor::execute(&s, db)
}

fn select_err(db: &Database, sql: &str) -> ExecutorError {
    let vibesql_ast::Statement::Select(s) = Parser::parse_sql(sql).unwrap() else {
        panic!("expected SELECT")
    };
    SelectExecutor::new(db).execute(&s).expect_err("expected SELECT to fail")
}

fn select_row_count(db: &Database, sql: &str) -> usize {
    let vibesql_ast::Statement::Select(s) = Parser::parse_sql(sql).unwrap() else {
        panic!("expected SELECT")
    };
    SelectExecutor::new(db).execute(&s).expect("SELECT should succeed").len()
}

// --- temp table with a dependent temp VIEW ---

#[test]
fn drop_temp_table_with_dependent_temp_view_succeeds() {
    let mut db = Database::new();
    exec_ok(&mut db, "create temp table t2(id integer, b)");
    exec_ok(&mut db, "create temp view tv as select id, b from t2");

    // sqlite3 permits the drop; the dependent view is left dangling.
    assert!(
        drop_table(&mut db, "drop table t2").is_ok(),
        "DROP of a temp table with a dependent temp view must succeed (sqlite3 parity)"
    );

    // Catalog and storage are both cleared (no metadata- or data-leak).
    assert!(!db.catalog.table_exists("t2"), "temp table must be gone from the catalog");
    assert!(db.get_table("t2").is_none(), "temp table storage entry must be removed");

    // Accessing the now-dangling view errors at use time with the bare name
    // (#5589's qualifier guards keep temp-view bodies unqualified).
    assert_eq!(
        select_err(&db, "select * from tv"),
        ExecutorError::TableNotFound("t2".to_string()),
        "dangling temp view must report the missing body table bare on use"
    );
}

// --- temp table with a dependent temp TRIGGER (body references the table) ---

#[test]
fn drop_temp_table_referenced_by_temp_trigger_body_succeeds() {
    let mut db = Database::new();
    exec_ok(&mut db, "create table main_t(id integer)");
    exec_ok(&mut db, "create temp table t3(id integer, b)");
    // Trigger fires on a *different* table but its body references t3, so the
    // table is referenced, not the trigger's ON-target. sqlite3 leaves such a
    // trigger in place and lets the drop proceed.
    exec_ok(
        &mut db,
        "create temp trigger trg after insert on main_t begin insert into t3 values(new.id, 1); end",
    );

    assert!(
        drop_table(&mut db, "drop table t3").is_ok(),
        "DROP of a temp table referenced by a temp trigger body must succeed (sqlite3 parity)"
    );
    assert!(!db.catalog.table_exists("t3"));
    assert!(db.get_table("t3").is_none());
}

// --- no dependents: the plain temp DROP that was also broken ---

#[test]
fn drop_temp_table_without_dependents_succeeds() {
    let mut db = Database::new();
    exec_ok(&mut db, "create temp table tonly(id integer)");

    assert!(
        drop_table(&mut db, "drop table tonly").is_ok(),
        "DROP of a bare temp table name must resolve the temp schema and succeed"
    );
    assert!(!db.catalog.table_exists("tonly"));
    assert!(db.get_table("tonly").is_none());
}

// --- temp shadows main for an unqualified DROP ---

#[test]
fn unqualified_drop_removes_temp_then_leaves_main() {
    let mut db = Database::new();
    exec_ok(&mut db, "create table t(id integer)");
    exec_ok(&mut db, "insert into t values(99)");
    exec_ok(&mut db, "create temp table t(id integer)");
    exec_ok(&mut db, "insert into t values(1)");

    // First unqualified DROP removes the temp t (temp shadows main).
    assert!(drop_table(&mut db, "drop table t").is_ok());
    assert!(
        db.catalog.table_exists("t"),
        "the main table t must still exist after dropping temp t"
    );
    assert_eq!(
        select_row_count(&db, "select * from t"),
        1,
        "the surviving main t should still hold its single row (99)"
    );

    // A second unqualified DROP now removes the main t.
    assert!(drop_table(&mut db, "drop table t").is_ok());
    assert!(!db.catalog.table_exists("t"), "the main table t must be gone after the second drop");
}

// --- main-schema behavior must not regress ---

#[test]
fn drop_main_table_with_dependent_main_view_still_succeeds() {
    // sqlite3 3.51.0 also permits dropping a main table with a dependent main
    // view (the view is left dangling); VibeSQL already matched this and must
    // continue to.
    let mut db = Database::new();
    exec_ok(&mut db, "create table m1(id integer, b)");
    exec_ok(&mut db, "create view mv as select id, b from m1");

    assert!(drop_table(&mut db, "drop table m1").is_ok());
    assert!(!db.catalog.table_exists("m1"));

    // Dangling main view reports the missing body table with the main qualifier
    // (#5569 behavior preserved).
    assert_eq!(
        select_err(&db, "select * from mv"),
        ExecutorError::TableNotFound("main.m1".to_string()),
        "dangling main view must report main.<name> on use (no regression to #5569)"
    );
}
