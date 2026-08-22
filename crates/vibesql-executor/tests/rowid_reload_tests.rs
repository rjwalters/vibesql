//! End-to-end regression tests for issue #5835: SQLite rowid semantics must
//! be stable across a binary `.vbsql` save/reload (the cross-process reopen
//! path used by the CLI and the TCL harness) and across WAL crash recovery.
//!
//! Before the fix:
//!   * `TableSchema::rowid_alias_column` was not rebuilt on load, so after a reopen `WHERE rowid=N`
//!     on an INTEGER PRIMARY KEY table returned zero rows — and `DELETE ... WHERE rowid=N` deleted
//!     the WRONG row (intpkey-2.6).
//!   * `Row::row_id` was not persisted (format v12), so explicit rowids were lost and implicit
//!     rowids were renumbered whenever tombstones were dropped at save time.
//!   * WAL `Insert` replay dropped the rowid, and the REPLACE conflict-delete emitted no WAL op at
//!     all — REPLACE resurrected the old conflicting row next to the new one after a restart
//!     (check-13.x, issue #5871).

use vibesql_ast::Statement;
use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::{
    wal::{PersistenceConfig, PersistenceEngine, RecoveryManager},
    Database,
};
use vibesql_types::SqlValue;

/// Create a table preserving the verbatim source text (issue #5619), the way
/// the CLI/load paths capture it. `sql_source` is what the reload path
/// re-parses to rehydrate the rowid alias, so tests must use this entry point.
fn create_with_source(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

/// Execute an INSERT / REPLACE / DELETE statement.
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse DML");
    match stmt {
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| ()).expect("INSERT/REPLACE");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map(|_| ()).expect("DELETE");
        }
        Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).expect("CREATE INDEX");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

/// Run a SELECT and return the result rows as `Vec<Vec<SqlValue>>`.
///
/// `rowid` on non-aliased tables surfaces as `Bigint`; normalize integer-family
/// values to `Integer` so assertions compare by value, not by width.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result
        .rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(|v| match v {
                    SqlValue::Bigint(n) => SqlValue::Integer(*n),
                    SqlValue::Smallint(n) => SqlValue::Integer(*n as i64),
                    other => other.clone(),
                })
                .collect()
        })
        .collect()
}

/// Save to a binary `.vbsql` file and reload — the cross-process reopen path.
fn reopen_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_5835_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded
}

fn int(v: i64) -> SqlValue {
    SqlValue::Integer(v)
}

// ---------------------------------------------------------------------------
// INTEGER PRIMARY KEY rowid aliasing across reload
// ---------------------------------------------------------------------------

/// The headline reproducer: `WHERE rowid=5` on an INTEGER PRIMARY KEY table
/// must keep returning the same row after a reopen.
#[test]
fn rowid_alias_survives_binary_reload() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE p(a INTEGER PRIMARY KEY, b INTEGER)");
    exec(&mut db, "INSERT INTO p VALUES(5, 50), (7, 70)");

    // Sanity: aliasing works in-memory before the reload.
    assert_eq!(query(&db, "SELECT b FROM p WHERE rowid=5"), vec![vec![int(50)]]);

    let reloaded = reopen_binary(&db, "alias");

    // The schema must remember the alias...
    assert_eq!(
        reloaded.get_table("p").expect("table p").schema.rowid_alias_column,
        Some(0),
        "rowid_alias_column must be rehydrated on binary load"
    );

    // ...and every rowid read must resolve through it.
    assert_eq!(query(&reloaded, "SELECT b FROM p WHERE rowid=5"), vec![vec![int(50)]]);
    assert_eq!(query(&reloaded, "SELECT b FROM p WHERE rowid=7"), vec![vec![int(70)]]);
    assert_eq!(
        query(&reloaded, "SELECT rowid FROM p ORDER BY a"),
        vec![vec![int(5)], vec![int(7)]],
        "SELECT rowid must return the IPK values, not physical positions"
    );
}

/// intpkey-2.6 shape: `DELETE ... WHERE rowid=N` after a reload must delete
/// the row whose rowid is N — before the fix it deleted the row at physical
/// position N instead (wrong-row DML, corruption class).
#[test]
fn delete_by_rowid_after_reload_targets_correct_row() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER)");
    exec(&mut db, "INSERT INTO t VALUES(5, 50), (7, 70), (9, 90)");

    let mut reloaded = reopen_binary(&db, "delete_by_rowid");
    exec(&mut reloaded, "DELETE FROM t WHERE rowid=7");

    assert_eq!(
        query(&reloaded, "SELECT a FROM t ORDER BY a"),
        vec![vec![int(5)], vec![int(9)]],
        "DELETE WHERE rowid=7 must remove exactly the a=7 row"
    );
}

// ---------------------------------------------------------------------------
// Implicit-rowid tables across reload
// ---------------------------------------------------------------------------

/// Implicit rowids must not be renumbered when a reload drops tombstoned
/// rows, and new inserts must not collide with (or reuse) surviving rowids.
#[test]
fn implicit_rowids_stable_across_reload_after_delete() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE u(x INTEGER)");
    exec(&mut db, "INSERT INTO u VALUES(100), (200), (300)");
    exec(&mut db, "DELETE FROM u WHERE x=200"); // tombstones rowid 2

    let mut reloaded = reopen_binary(&db, "implicit");

    assert_eq!(
        query(&reloaded, "SELECT rowid FROM u ORDER BY rowid"),
        vec![vec![int(1)], vec![int(3)]],
        "surviving implicit rowids must not shift down after the tombstone is dropped"
    );

    // SQLite allocates max(rowid)+1 = 4 here; before the fix the live-count
    // based allocation would hand out 3 — colliding with the existing row.
    exec(&mut reloaded, "INSERT INTO u VALUES(400)");
    assert_eq!(
        query(&reloaded, "SELECT rowid FROM u WHERE x=400"),
        vec![vec![int(4)]],
        "new insert must get a fresh rowid, not collide with the surviving rowid 3"
    );
}

/// Explicit rowids (INSERT INTO t(rowid, ...)) survive the reload, and the
/// next auto-allocated rowid continues past them (SQLite max+1 semantics).
#[test]
fn explicit_rowid_survives_reload_and_allocation_continues_past_it() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE w(x INTEGER)");
    exec(&mut db, "INSERT INTO w(rowid, x) VALUES(10, 1)");

    let mut reloaded = reopen_binary(&db, "explicit");

    assert_eq!(query(&reloaded, "SELECT rowid FROM w"), vec![vec![int(10)]]);

    exec(&mut reloaded, "INSERT INTO w VALUES(2)");
    assert_eq!(
        query(&reloaded, "SELECT rowid FROM w WHERE x=2"),
        vec![vec![int(11)]],
        "auto rowid must continue past the reloaded explicit rowid 10"
    );
}

// ---------------------------------------------------------------------------
// REPLACE INTO durability across WAL crash recovery (issues #5835 / #5871)
// ---------------------------------------------------------------------------

/// The #5871 reproducer, driven through the crash-recovery path: a REPLACE
/// whose conflict-delete is only in the WAL (no checkpoint) must recover to a
/// single row — before the fix the delete was never WAL-logged, so replay
/// resurrected the old row next to the new one (duplicate INTEGER PRIMARY
/// KEY).
#[test]
fn replace_conflict_delete_survives_crash_via_wal_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal_path = dir.path().join("replace_crash.wal");
    let checkpoint_dir = dir.path().join("replace_crash-checkpoints");

    // --- Session 1: run the REPLACE against a WAL-backed database, flush the
    // WAL, then "crash" (drop without ever writing a checkpoint).
    {
        let mut db = Database::new();
        let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default())
            .expect("persistence engine");
        db.enable_persistence(engine);

        create_with_source(&mut db, "CREATE TABLE t1(aa INTEGER PRIMARY KEY, bb INT)");
        exec(&mut db, "INSERT INTO t1 VALUES(11, 22)");
        exec(&mut db, "CREATE UNIQUE INDEX t1bb ON t1(bb)");
        exec(&mut db, "REPLACE INTO t1 VALUES(11, 33)");

        // In-memory state is correct: exactly one row.
        assert_eq!(query(&db, "SELECT aa, bb FROM t1"), vec![vec![int(11), int(33)]]);

        db.sync_persistence().expect("sync WAL");
        // `db` drops here — crash simulation: WAL only, no checkpoint.
    }

    // --- Session 2: recover purely from the WAL.
    let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
    let (recovered, _stats) = manager.recover().expect("recover");

    let table_name = recovered
        .list_tables()
        .into_iter()
        .find(|t| t.to_lowercase().contains("t1"))
        .expect("t1 must exist after recovery");
    let table = recovered.get_table(&table_name).expect("recovered table");

    let live: Vec<Vec<SqlValue>> = table.scan_live().map(|(_, r)| r.values.to_vec()).collect();
    assert_eq!(
        live,
        vec![vec![int(11), int(33)]],
        "REPLACE must recover to exactly one row — the conflict-delete must be \
         WAL-logged and replayed (issues #5835 / #5871)"
    );

    // The replayed row must also carry its rowid (WAL format v3).
    let rowid = table.scan_live().next().and_then(|(_, r)| r.row_id);
    assert_eq!(rowid, Some(11), "WAL replay must restore the row's effective rowid");
}

/// Plain inserts replayed from the WAL keep their explicit rowids too.
#[test]
fn insert_rowids_survive_crash_via_wal_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal_path = dir.path().join("insert_rowid_crash.wal");
    let checkpoint_dir = dir.path().join("insert_rowid_crash-checkpoints");

    {
        let mut db = Database::new();
        let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default())
            .expect("persistence engine");
        db.enable_persistence(engine);

        create_with_source(&mut db, "CREATE TABLE w(x INTEGER)");
        exec(&mut db, "INSERT INTO w(rowid, x) VALUES(10, 1)");
        db.sync_persistence().expect("sync WAL");
    }

    let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
    let (recovered, _stats) = manager.recover().expect("recover");

    let table_name = recovered
        .list_tables()
        .into_iter()
        .find(|t| t.to_lowercase().contains('w'))
        .expect("w must exist after recovery");
    let table = recovered.get_table(&table_name).expect("recovered table");
    let rowid = table.scan_live().next().and_then(|(_, r)| r.row_id);
    assert_eq!(
        rowid,
        Some(10),
        "explicit rowid must survive WAL crash recovery (WAL format v3, issue #5835)"
    );
}
