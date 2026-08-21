//! Regression tests for fkey2-13.1.2.* (Part of #6170): REPLACE conflict
//! resolution must also detect a collision on the table's own hidden
//! internal rowid, not just PRIMARY KEY / UNIQUE constraints.
//!
//! `REPLACE INTO t(rowid, ...) VALUES(id, ...)` on a table whose declared
//! PRIMARY KEY is a *different* column set (or none at all) still targets a
//! specific physical row via its rowid. Before this fix, `handle_replace_conflicts`
//! only scanned for PK/UNIQUE conflicts, so an explicit-rowid REPLACE that
//! changed only non-key columns silently inserted a *second* physical row
//! sharing the "same" rowid instead of overwriting the original — and, when a
//! child table had an FK referencing the row about to be overwritten, the
//! orphaning FK violation that SQLite raises (and rolls back) was never
//! detected at all.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, ExecutorError, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec(db: &mut Database, sql: &str) -> Result<usize, ExecutorError> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => CreateTableExecutor::execute(&s, db).map(|_| 0),
        Statement::Insert(s) => InsertExecutor::execute(db, &s),
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

fn setup() -> Database {
    // pp's PRIMARY KEY is the composite (b, c) — distinct from its hidden
    // rowid. cc references pp's PK (default REFERENCES pp with no column
    // list resolves to the parent's PK).
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    exec(&mut db, "CREATE TABLE pp(a UNIQUE, b, c, PRIMARY KEY(b, c))").unwrap();
    exec(&mut db, "CREATE TABLE cc(d, e, f UNIQUE, FOREIGN KEY(d, e) REFERENCES pp)").unwrap();
    exec(&mut db, "INSERT INTO pp VALUES(1, 2, 3)").unwrap();
    exec(&mut db, "INSERT INTO cc VALUES(2, 3, 1)").unwrap();
    db
}

/// fkey2-13.1.2.1/13.1.2.2: an explicit-rowid REPLACE that changes pp's PK
/// away from the value cc still references must fail with a FOREIGN KEY
/// violation and leave both tables completely unchanged (no orphan row, and
/// no stray extra physical row sharing the targeted rowid).
#[test]
fn explicit_rowid_replace_detects_pk_conflict_and_rejects_fk_violation() {
    let mut db = setup();

    let err = exec(&mut db, "REPLACE INTO pp(rowid, a, b, c) VALUES(1, 2, 3, 4)")
        .expect_err("REPLACE must fail: it would orphan cc's FK reference to pp(2,3)");
    assert!(
        err.to_string().contains("FOREIGN KEY constraint failed"),
        "unexpected error: {err}"
    );

    let pp = db.get_table("pp").unwrap();
    assert_eq!(pp.row_count(), 1, "pp must still have exactly one row after the rejected REPLACE");
    let cc = db.get_table("cc").unwrap();
    assert_eq!(cc.row_count(), 1, "cc must be untouched after the rejected REPLACE");
}

/// Same rowid-collision detection, but the replacement succeeds because the
/// new row's PK value happens to be the *same* key cc already references —
/// the conflict-delete-then-reinsert round-trips back to a satisfied FK.
#[test]
fn explicit_rowid_replace_allows_pk_preserving_update() {
    let mut db = setup();

    let affected = exec(&mut db, "REPLACE INTO pp(rowid, a, b, c) VALUES(1, 9, 2, 3)")
        .expect("REPLACE keeping the same (b,c) key must succeed");
    assert_eq!(affected, 1);

    let pp = db.get_table("pp").unwrap();
    assert_eq!(pp.row_count(), 1, "the REPLACE must overwrite the existing physical row, not add a second one");
}
