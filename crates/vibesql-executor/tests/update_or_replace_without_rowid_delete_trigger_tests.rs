//! Regression tests for issue #5490:
//! `UPDATE OR REPLACE` (and the REPLACE side of `INSERT OR REPLACE`) on a
//! WITHOUT ROWID table must
//!   (a) RESOLVE a PRIMARY KEY / UNIQUE conflict by deleting the conflicting
//!       existing row(s), and
//!   (b) FIRE the DELETE trigger(s) for those removed rows.
//!
//! Before the fix, a WITHOUT ROWID `UPDATE OR REPLACE` that hit a PRIMARY KEY
//! conflict took the single-row fast path, which has no conflict-resolution
//! logic, and raised `PRIMARY KEY constraint violation`. Even via the slow
//! path the conflicting row was tombstoned without firing its DELETE trigger.
//!
//! Behavior is verified against sqlite3 3.51.0 (recursive_triggers ON), which
//! is the semantics VibeSQL matches for REPLACE-conflict DELETE triggers (the
//! INSERT OR REPLACE path already did so — see `insert/replace.rs`).

use vibesql_executor::{
    CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor, TriggerExecutor,
    UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single SQL statement, dispatching on statement type.
fn exec(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger(db, &s).expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// Like [`exec`] but returns the Result of an UPDATE so a test can assert it
/// did NOT error (the pre-fix bug raised a PK violation here).
fn try_update(db: &mut Database, sql: &str) -> Result<usize, vibesql_executor::ExecutorError> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::Update(s) => UpdateExecutor::execute(&s, db),
        other => panic!("expected UPDATE, got {other:?}"),
    }
}

/// Run a SELECT and return its rows as `Vec<Vec<SqlValue>>` (live rows only).
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let executor = SelectExecutor::new(db);
    executor
        .execute(&select)
        .expect("SELECT failed")
        .into_iter()
        .map(|row| row.values.to_vec())
        .collect()
}

/// Run an unordered SELECT and sort the resulting rows in Rust by their first
/// column. Used instead of SQL `ORDER BY` because an index-ordered scan over a
/// freshly-compacted in-memory table currently returns the wrong rows (a
/// separate read-path bug filed as a follow-on; the persisting CLI path is
/// unaffected). Sorting in Rust keeps these tests focused on #5490's conflict
/// resolution + DELETE-trigger behavior.
fn sorted_rows(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let mut rows = query(db, sql);
    rows.sort_by(|a, b| format!("{:?}", a[0]).cmp(&format!("{:?}", b[0])));
    rows
}

/// Collect a single TEXT column from a SELECT as a `Vec<String>`.
fn text_column(db: &Database, sql: &str) -> Vec<String> {
    query(db, sql)
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected text value, got {other:?}"),
        })
        .collect()
}

/// triggerF.test case 1.2: AFTER DELETE trigger on a WITHOUT ROWID table.
/// The DELETE trigger must fire once for the plain DELETE and once each for
/// the INSERT OR REPLACE and UPDATE OR REPLACE conflict resolutions, with the
/// trigger body observing the post-delete `count(*)`.
#[test]
fn after_delete_trigger_fires_for_or_replace_conflicts_without_rowid() {
    let mut db = Database::new();
    // REPLACE conflict-resolution DELETE triggers fire only with
    // recursive_triggers ON (SQLite lang_conflict.html; #5840). triggerF.test
    // sets it on; match that here since these tests assert the trigger fires.
    db.set_recursive_triggers(true);
    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b) WITHOUT ROWID");
    exec(&mut db, "CREATE TABLE log(t)");
    exec(
        &mut db,
        "CREATE TRIGGER trd AFTER DELETE ON t1 BEGIN \
         INSERT INTO log VALUES(old.a || old.b || (SELECT count(*) FROM t1)); END",
    );

    exec(&mut db, "INSERT INTO t1 VALUES(1, 'one')");
    exec(&mut db, "INSERT INTO t1 VALUES(2, 'two')");
    exec(&mut db, "INSERT INTO t1 VALUES(3, 'three')");

    exec(&mut db, "DELETE FROM t1 WHERE a=1");
    exec(&mut db, "INSERT OR REPLACE INTO t1 VALUES(2, 'three')");
    // The pre-fix bug raised a PK violation on this statement.
    try_update(&mut db, "UPDATE OR REPLACE t1 SET a=3 WHERE a=2")
        .expect("UPDATE OR REPLACE must resolve the PK conflict, not raise");

    // DELETE trigger log matches sqlite3 3.51.0 exactly.
    assert_eq!(
        text_column(&db, "SELECT t FROM log"),
        vec!["1one2".to_string(), "2two1".to_string(), "3three1".to_string()],
    );

    // Conflict resolved: old row gone, replacement present.
    let rows = sorted_rows(&db, "SELECT a, b FROM t1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], SqlValue::Integer(3));
}

/// triggerF.test case 1.3: BEFORE DELETE trigger on a WITHOUT ROWID table.
#[test]
fn before_delete_trigger_fires_for_or_replace_conflicts_without_rowid() {
    let mut db = Database::new();
    db.set_recursive_triggers(true); // REPLACE conflict DELETE triggers fire only with recursion on (#5840)
    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b) WITHOUT ROWID");
    exec(&mut db, "CREATE TABLE log(t)");
    exec(
        &mut db,
        "CREATE TRIGGER trd BEFORE DELETE ON t1 BEGIN \
         INSERT INTO log VALUES(old.a || old.b || (SELECT count(*) FROM t1)); END",
    );

    exec(&mut db, "INSERT INTO t1 VALUES(1, 'one')");
    exec(&mut db, "INSERT INTO t1 VALUES(2, 'two')");
    exec(&mut db, "INSERT INTO t1 VALUES(3, 'three')");

    exec(&mut db, "DELETE FROM t1 WHERE a=1");
    exec(&mut db, "INSERT OR REPLACE INTO t1 VALUES(2, 'three')");
    try_update(&mut db, "UPDATE OR REPLACE t1 SET a=3 WHERE a=2")
        .expect("UPDATE OR REPLACE must resolve the PK conflict, not raise");

    // BEFORE DELETE observes the pre-delete count, matching sqlite3 3.51.0.
    assert_eq!(
        text_column(&db, "SELECT t FROM log"),
        vec!["1one3".to_string(), "2two2".to_string(), "3three2".to_string()],
    );
}

/// A WITHOUT ROWID row that conflicts on multiple UNIQUE constraints at once:
/// UPDATE OR REPLACE must delete every conflicting row and fire the DELETE
/// trigger for each, leaving exactly the updated row.
#[test]
fn update_or_replace_multi_unique_conflict_without_rowid() {
    let mut db = Database::new();
    db.set_recursive_triggers(true); // REPLACE conflict DELETE triggers fire only with recursion on (#5840)
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b UNIQUE, c UNIQUE) WITHOUT ROWID");
    exec(&mut db, "CREATE TABLE log(t)");
    exec(
        &mut db,
        "CREATE TRIGGER trd AFTER DELETE ON t BEGIN INSERT INTO log VALUES('del' || old.a); END",
    );
    exec(&mut db, "INSERT INTO t VALUES(1, 10, 100)");
    exec(&mut db, "INSERT INTO t VALUES(2, 20, 200)");
    exec(&mut db, "INSERT INTO t VALUES(3, 30, 300)");

    // Row a=1 -> b=20 (conflicts with a=2) and c=300 (conflicts with a=3).
    try_update(&mut db, "UPDATE OR REPLACE t SET b=20, c=300 WHERE a=1")
        .expect("multi-conflict UPDATE OR REPLACE must resolve, not raise");

    // Only the updated row remains.
    let rows = sorted_rows(&db, "SELECT a, b, c FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Integer(20), SqlValue::Integer(300)]
    );

    // Both conflicting rows' DELETE triggers fired (order-independent check).
    let mut log = text_column(&db, "SELECT t FROM log");
    log.sort();
    assert_eq!(log, vec!["del2".to_string(), "del3".to_string()]);
}

/// Regression guard for ROWID tables: UPDATE OR REPLACE must still resolve the
/// conflict AND fire the replaced row's DELETE trigger (no regression from the
/// fast-path change that now routes conflict clauses through the slow path).
#[test]
fn update_or_replace_rowid_table_still_resolves_and_fires_delete_trigger() {
    let mut db = Database::new();
    db.set_recursive_triggers(true); // REPLACE conflict DELETE triggers fire only with recursion on (#5840)
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b)");
    exec(&mut db, "CREATE TABLE log(t)");
    exec(
        &mut db,
        "CREATE TRIGGER trd AFTER DELETE ON t BEGIN \
         INSERT INTO log VALUES('del' || old.a || old.b); END",
    );
    exec(&mut db, "INSERT INTO t VALUES(1, 'one')");
    exec(&mut db, "INSERT INTO t VALUES(2, 'two')");
    exec(&mut db, "INSERT INTO t VALUES(3, 'three')");

    try_update(&mut db, "UPDATE OR REPLACE t SET a=3 WHERE a=2")
        .expect("ROWID UPDATE OR REPLACE must resolve the PK conflict");

    let rows = sorted_rows(&db, "SELECT a, b FROM t");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), text("one")]);
    assert_eq!(rows[1], vec![SqlValue::Integer(3), text("two")]);

    // The conflicting (3,'three') row was deleted and its trigger fired.
    assert_eq!(text_column(&db, "SELECT t FROM log"), vec!["del3three".to_string()]);
}

fn text(s: &str) -> SqlValue {
    SqlValue::Varchar(arcstr::ArcStr::from(s))
}

/// Issue #5490 (doctor blocker): a `BEFORE DELETE` trigger that runs
/// `RAISE(IGNORE)` abandons the REPLACE-conflict row's deletion, so the
/// conflicting row stays live. `UPDATE OR REPLACE` would then drive the updated
/// row onto a PK that another (surviving) row still holds — a duplicate PRIMARY
/// KEY. sqlite3 3.51.0 (recursive_triggers=ON) instead raises
/// `UNIQUE constraint failed: t1.a` and leaves the table unchanged. Before this
/// fix VibeSQL returned Ok and left two rows sharing PK a=3 (silent corruption).
#[test]
fn update_or_replace_before_delete_ignore_raises_unique_and_leaves_table_unchanged() {
    let mut db = Database::new();
    db.set_recursive_triggers(true); // REPLACE conflict DELETE triggers fire only with recursion on (#5840)
    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b) WITHOUT ROWID");
    // RAISE(IGNORE) only for the conflicting row (a=3), so the deletion that
    // UPDATE OR REPLACE wants to perform is abandoned.
    exec(
        &mut db,
        "CREATE TRIGGER trd BEFORE DELETE ON t1 WHEN old.a=3 BEGIN SELECT RAISE(IGNORE); END",
    );
    exec(&mut db, "INSERT INTO t1 VALUES(2, 'two')");
    exec(&mut db, "INSERT INTO t1 VALUES(3, 'three')");

    let err = try_update(&mut db, "UPDATE OR REPLACE t1 SET a=3 WHERE a=2")
        .expect_err("surviving conflict row must raise a UNIQUE error, not silently dup the PK");
    // Display localizes a "Constraint violation: " prefix; assert the SQLite
    // sub-message + the qualified PK column, matching sqlite3 3.51.0's
    // `UNIQUE constraint failed: t1.a`.
    let msg = err.to_string();
    assert!(msg.contains("UNIQUE constraint failed: t1.a"), "got error: {msg}");

    // Table unchanged — verified via a live SELECT (no duplicate PK).
    let rows = sorted_rows(&db, "SELECT a, b FROM t1");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(2), text("two")]);
    assert_eq!(rows[1], vec![SqlValue::Integer(3), text("three")]);
}

/// Same blocker via the `UPDATE ... FROM` path: the surviving BEFORE DELETE
/// RAISE(IGNORE) conflict must raise `UNIQUE constraint failed: t1.a` and leave
/// the table unchanged, matching sqlite3 3.51.0.
#[test]
fn update_or_replace_from_before_delete_ignore_raises_unique_and_leaves_table_unchanged() {
    let mut db = Database::new();
    db.set_recursive_triggers(true); // REPLACE conflict DELETE triggers fire only with recursion on (#5840)
    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b) WITHOUT ROWID");
    exec(
        &mut db,
        "CREATE TRIGGER trd BEFORE DELETE ON t1 WHEN old.a=3 BEGIN SELECT RAISE(IGNORE); END",
    );
    exec(&mut db, "INSERT INTO t1 VALUES(2, 'two')");
    exec(&mut db, "INSERT INTO t1 VALUES(3, 'three')");
    exec(&mut db, "CREATE TABLE src(x, y)");
    exec(&mut db, "INSERT INTO src VALUES(2, 3)");

    let err = try_update(&mut db, "UPDATE OR REPLACE t1 SET a=src.y FROM src WHERE t1.a=src.x")
        .expect_err("surviving conflict row must raise a UNIQUE error in the FROM path too");
    // Display localizes a "Constraint violation: " prefix; assert the SQLite
    // sub-message + the qualified PK column, matching sqlite3 3.51.0's
    // `UNIQUE constraint failed: t1.a`.
    let msg = err.to_string();
    assert!(msg.contains("UNIQUE constraint failed: t1.a"), "got error: {msg}");

    let rows = sorted_rows(&db, "SELECT a, b FROM t1");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(2), text("two")]);
    assert_eq!(rows[1], vec![SqlValue::Integer(3), text("three")]);
}

/// In-memory regression for the compaction-induced index drift exposed by the
/// fast-path bypass (#5490): a prior tombstone (`DELETE FROM t1 WHERE a=1`)
/// plus the REPLACE-conflict delete can compact the table mid-statement,
/// shifting the update target's physical index. Before the fix this raised
/// `StorageError("Column index ... out of bounds")` or silently dropped the
/// row. The CLI masked it because each statement reloads from disk.
#[test]
fn update_or_replace_after_tombstone_without_rowid_inmemory() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b) WITHOUT ROWID");
    exec(&mut db, "INSERT INTO t1 VALUES(1, 'one')");
    exec(&mut db, "INSERT INTO t1 VALUES(2, 'two')");
    exec(&mut db, "INSERT INTO t1 VALUES(3, 'three')");
    exec(&mut db, "DELETE FROM t1 WHERE a=1");

    // a=2 -> a=3 conflicts with the existing (3,'three'); REPLACE deletes it.
    try_update(&mut db, "UPDATE OR REPLACE t1 SET a=3 WHERE a=2")
        .expect("UPDATE OR REPLACE must not error on a compacted table");

    let rows = sorted_rows(&db, "SELECT a, b FROM t1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![SqlValue::Integer(3), text("two")]);
}

/// Same drift case but with an intervening INSERT OR REPLACE (the exact
/// triggerF.test 1.x.1 statement sequence) and no triggers.
#[test]
fn insert_then_update_or_replace_without_rowid_inmemory() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b) WITHOUT ROWID");
    exec(&mut db, "INSERT INTO t1 VALUES(1, 'one')");
    exec(&mut db, "INSERT INTO t1 VALUES(2, 'two')");
    exec(&mut db, "INSERT INTO t1 VALUES(3, 'three')");
    exec(&mut db, "DELETE FROM t1 WHERE a=1");
    exec(&mut db, "INSERT OR REPLACE INTO t1 VALUES(2, 'three')");

    try_update(&mut db, "UPDATE OR REPLACE t1 SET a=3 WHERE a=2")
        .expect("UPDATE OR REPLACE must not error");

    // NOTE: read with an unordered SELECT and sort in Rust. An `ORDER BY a`
    // index-ordered scan over this specific in-memory tombstone layout returns
    // the wrong rows — a SEPARATE pre-existing read-path bug (filed as a
    // follow-on), unrelated to #5490's conflict resolution. The persisting CLI
    // path is unaffected, which is why triggerF.test passes.
    let rows = sorted_rows(&db, "SELECT a, b FROM t1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![SqlValue::Integer(3), text("three")]);
}
