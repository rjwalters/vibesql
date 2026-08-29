//! Regression test for issue #6658: re-inserting a row with the same PK
//! inside an `AFTER DELETE` trigger for that row must not hit a stale
//! UNIQUE/PRIMARY KEY index check.
//!
//! `t1(x COLLATE NOCASE PRIMARY KEY)` forces the collation-aware duplicate
//! scan in `RowValidator::validate_primary_key_uniqueness` /
//! `validate_unique_constraints` (the hash index can't surface NOCASE
//! duplicates). That scan previously iterated `Table::scan()` — the raw
//! physical row storage, including rows tombstoned by the deletion bitmap
//! but not yet compacted — instead of `Table::scan_visible()`. Within the
//! interleaved per-row DELETE loop, a row's bitmap tombstone is set BEFORE
//! its `AFTER DELETE` trigger fires (compaction is deferred to statement
//! end), so a trigger-driven `INSERT INTO t1 VALUES(old.x)` re-inserting the
//! very row just deleted saw its own tombstoned row as a live duplicate and
//! incorrectly raised `UNIQUE constraint failed`.
//!
//! Expected behavior (verified against sqlite3 3.51.x): the trigger's
//! INSERT succeeds, so `DELETE FROM t1` (which fires the trigger once per
//! deleted row) leaves the table exactly as it started.

use vibesql_executor::{
    CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor, TriggerExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    SelectExecutor::new(db)
        .execute(&select)
        .expect("SELECT failed")
        .into_iter()
        .map(|row| row.values.to_vec())
        .collect()
}

fn texts(rows: &[Vec<SqlValue>], col: usize) -> Vec<String> {
    rows.iter()
        .map(|r| match &r[col] {
            SqlValue::Character(s) | SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect()
}

/// The exact reproduction from issue #6658.
#[test]
fn after_delete_trigger_reinserts_nocase_pk_row_successfully() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(x COLLATE NOCASE PRIMARY KEY)");
    exec(&mut db, "CREATE TRIGGER tt1 AFTER DELETE ON t1 BEGIN INSERT INTO t1 VALUES(old.x); END");
    exec(&mut db, "INSERT INTO t1 VALUES('A')");
    exec(&mut db, "INSERT INTO t1 VALUES('B')");

    // Must not raise "UNIQUE constraint failed: t1.x".
    exec(&mut db, "DELETE FROM t1");

    let rows = query(&db, "SELECT * FROM t1 ORDER BY x");
    assert_eq!(texts(&rows, 0), vec!["A".to_string(), "B".to_string()]);
}

/// Same scenario but for a schema-level UNIQUE (non-PK) column under NOCASE,
/// exercising `RowValidator::validate_unique_constraints`'s collation-aware
/// branch instead of `validate_primary_key_uniqueness`.
#[test]
fn after_delete_trigger_reinserts_nocase_unique_row_successfully() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, x COLLATE NOCASE UNIQUE)");
    exec(
        &mut db,
        "CREATE TRIGGER tt1 AFTER DELETE ON t1 BEGIN INSERT INTO t1(id, x) VALUES(old.id, old.x); END",
    );
    exec(&mut db, "INSERT INTO t1(id, x) VALUES(1, 'A')");
    exec(&mut db, "INSERT INTO t1(id, x) VALUES(2, 'B')");

    exec(&mut db, "DELETE FROM t1");

    let rows = query(&db, "SELECT x FROM t1 ORDER BY x");
    assert_eq!(texts(&rows, 0), vec!["A".to_string(), "B".to_string()]);
}
