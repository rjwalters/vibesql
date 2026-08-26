//! End-to-end regression tests for issue #6599: `ALTER TABLE ... RENAME TO
//! ...` silently dropped every index (including `UNIQUE` indexes) on the
//! renamed table.
//!
//! Root cause: `execute_rename_table` (`vibesql-executor/src/alter/
//! table_options.rs`) implements RENAME as drop-old-table + create-new-table.
//! The generic `Database::drop_table` call CASCADE-drops every index on the
//! table being dropped — both the catalog-level `IndexMetadata` (used for
//! `sqlite_master`/introspection) and the storage-level `IndexManager` entry
//! (the live B-tree/hash body that actually enforces `UNIQUE` and backs
//! query planning) — and nothing recreated them for the new table identity
//! afterward. Three symptoms fell out of this single gap:
//!
//! 1. The physical index body was gone immediately after the rename (same session, no persistence
//!    involved) — so `UNIQUE` stopped being enforced right away, not just after a save/reload.
//! 2. The catalog-level index metadata was ALSO dropped by the generic drop, so `sqlite_master`
//!    lost the index row entirely in the same session.
//! 3. Because storage's `write_catalog` persistence path enumerates indexes from the
//!    (now-empty-for-this-table) storage-level `IndexManager`, a save/reload after the rename
//!    produced a database with the index gone for good — most seriously for a `UNIQUE` index,
//!    silently losing a data-integrity guarantee.
//!
//! A related but independently-triggered bug (also fixed alongside this):
//! `write_catalog` persisted a table's *canonical* (lowercased, for an
//! unquoted identifier) catalog key as its display name instead of
//! `table.schema.name` (the exact declared/renamed spelling) — so a renamed
//! table's exact case was silently lowercased across every save/reload,
//! independent of whether it had any indexes at all.

use vibesql_executor::{
    AlterTableExecutor, CreateIndexExecutor, CreateTableExecutor, ExecutorError, InsertExecutor,
    SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a single DDL/DML SQL statement, propagating errors (needed to
/// assert a `UNIQUE` violation is rejected rather than silently accepted).
fn exec(db: &mut Database, sql: &str) -> Result<(), ExecutorError> {
    match Parser::parse_sql(sql).expect("test SQL should parse") {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        vibesql_ast::Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, db).map(|_| ()),
        vibesql_ast::Statement::AlterTable(s) => {
            AlterTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(db, &s).map(|_| ()),
        other => panic!("unexpected statement in test: {other:?}"),
    }
}

/// Execute a statement expected to succeed; panics with the error otherwise.
fn exec_ok(db: &mut Database, sql: &str) {
    exec(db, sql).unwrap_or_else(|e| panic!("statement should succeed: {sql}: {e}"));
}

/// Run a SELECT and return the resulting row values.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result.rows.into_iter().map(|r| r.values.to_vec()).collect()
}

/// Save to the binary `.vbsql` format and reload — the exact save/reload
/// cycle the issue's repro exercises.
fn roundtrip_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_6599_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let reloaded =
        Database::load_binary(&path).expect("load_binary must succeed after RENAME TABLE (#6599)");
    std::fs::remove_file(&path).ok();
    reloaded
}

/// The issue's primary repro: a `UNIQUE` index must keep enforcing its
/// constraint across `ALTER TABLE ... RENAME TO ...` followed by a
/// save/reload cycle.
#[test]
fn renamed_table_unique_index_survives_binary_reload_and_still_enforces() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE t1(a, b)");
    exec_ok(&mut db, "CREATE UNIQUE INDEX t1u1 ON t1(b)");
    exec_ok(&mut db, "ALTER TABLE t1 RENAME TO T2");

    // Same-session: the physical index must still enforce UNIQUE immediately
    // after the rename, before any persistence is involved.
    exec_ok(&mut db, "INSERT INTO T2 VALUES(1, 5)");
    let dup = exec(&mut db, "INSERT INTO T2 VALUES(2, 5)");
    assert!(dup.is_err(), "UNIQUE(b) must still be enforced immediately after RENAME TABLE");

    // The reproducer's failing step: save + reopen from a binary snapshot.
    let mut reloaded = roundtrip_binary(&db, "unique");

    // The UNIQUE index must have survived the round trip and keep enforcing.
    let dup_after_reload = exec(&mut reloaded, "INSERT INTO T2 VALUES(3, 5)");
    assert!(
        dup_after_reload.is_err(),
        "UNIQUE(b) must still be enforced after RENAME TABLE + save/reload (#6599)"
    );

    // A non-colliding insert must still succeed.
    exec_ok(&mut reloaded, "INSERT INTO T2 VALUES(3, 6)");
    assert_eq!(query(&reloaded, "SELECT a FROM T2 WHERE b = 6"), vec![vec![SqlValue::Integer(3)]]);
}

/// A plain (non-`UNIQUE`) index must also survive the rename + reload, not
/// just `UNIQUE` ones — the same drop/never-recreated gap affected every
/// index kind.
#[test]
fn renamed_table_plain_index_survives_binary_reload() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE t1(a, b)");
    exec_ok(&mut db, "CREATE INDEX t1i1 ON t1(b)");
    exec_ok(&mut db, "ALTER TABLE t1 RENAME TO T2");
    exec_ok(&mut db, "INSERT INTO T2 VALUES(1, 5)");

    let reloaded = roundtrip_binary(&db, "plain");

    // Catalog metadata: sqlite_master must still list the index, retargeted
    // at the new table name.
    let rows =
        query(&reloaded, "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name='t1i1'");
    assert_eq!(rows, vec![vec![SqlValue::Varchar("T2".into())]]);

    // Storage metadata: the physical index still exists and is usable.
    assert!(reloaded.get_index("t1i1").is_some(), "physical index must survive reload");
    assert_eq!(query(&reloaded, "SELECT a FROM T2 WHERE b = 5"), vec![vec![SqlValue::Integer(1)]]);
}

/// Both a `UNIQUE` and a plain index on the same renamed table must both
/// survive — guards against a fix that only handles a single index per
/// table.
#[test]
fn renamed_table_multiple_indexes_all_survive_binary_reload() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE t1(a, b, c)");
    exec_ok(&mut db, "CREATE UNIQUE INDEX t1u1 ON t1(b)");
    exec_ok(&mut db, "CREATE INDEX t1i1 ON t1(c)");
    exec_ok(&mut db, "ALTER TABLE t1 RENAME TO T2");

    let reloaded = roundtrip_binary(&db, "multi");
    assert!(reloaded.get_index("t1u1").is_some());
    assert!(reloaded.get_index("t1i1").is_some());

    let names: Vec<String> =
        query(&reloaded, "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
            .into_iter()
            .map(|row| match &row[0] {
                SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect();
    assert_eq!(names, vec!["t1i1".to_string(), "t1u1".to_string()]);
}

/// Symptom 3 from the issue: within the SAME session (no persistence
/// involved), an index's `sqlite_master.tbl_name` must be updated to the new
/// table name immediately after `ALTER TABLE ... RENAME TO ...` — SQLite
/// updates `tbl_name` on all objects that reference the renamed table.
#[test]
fn renamed_table_index_tbl_name_updates_immediately_same_session() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE t1(a, b)");
    exec_ok(&mut db, "CREATE INDEX t1i1 ON t1(b)");
    exec_ok(&mut db, "ALTER TABLE t1 RENAME TO T2");

    let rows = query(&db, "SELECT tbl_name FROM sqlite_master WHERE name='t1i1'");
    assert_eq!(
        rows,
        vec![vec![SqlValue::Varchar("T2".into())]],
        "tbl_name must be updated to the new table name immediately after RENAME (same session)"
    );
}

/// Issue #6607 gap 1: an implicit `sqlite_autoindex_<table>_<n>` index name
/// (minted by `create_implicit_indexes`, `vibesql-executor/src/create_table.rs`,
/// for PRIMARY KEY/UNIQUE constraints with no explicit index name) must be
/// regenerated to match the table's new identity when the table is renamed —
/// not just have its `tbl_name` retargeted (which #6599 already covered).
/// SQLite's `sqlite_rename_table` does the same: the index's own `name`
/// embeds the table identity at mint time, so it is stale after a rename
/// unless explicitly rewritten. Verified same-session (immediately after the
/// ALTER) and after a binary save/reload round trip.
#[test]
fn renamed_table_implicit_autoindex_names_retarget_to_new_table_same_session() {
    let mut db = Database::new();
    // `b PRIMARY KEY` (no INTEGER type) is a real column-backed PK, not a
    // rowid alias, so it mints a genuine `sqlite_autoindex_*` slot (matching
    // the issue's own repro, drawn from SQLite's alter.test alter-1.1/1.2/1.5).
    exec_ok(&mut db, "CREATE TABLE t1(c UNIQUE, b PRIMARY KEY)");
    exec_ok(&mut db, "ALTER TABLE t1 RENAME TO T2");

    let mut names: Vec<String> =
        query(&db, "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
            .into_iter()
            .map(|row| match &row[0] {
                SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["sqlite_autoindex_T2_1".to_string(), "sqlite_autoindex_T2_2".to_string()],
        "implicit autoindex names must be regenerated onto the new table identity \
         immediately after RENAME (same session), not left naming the pre-rename table (#6607)"
    );

    // The physical index bodies must be reachable under their NEW names (the
    // catalog name and the physical storage key must never diverge)...
    assert!(db.get_index("sqlite_autoindex_T2_1").is_some());
    assert!(db.get_index("sqlite_autoindex_T2_2").is_some());
    // ...and the stale pre-rename names must be gone, not left as ghost
    // duplicates alongside the renamed entries.
    assert!(db.get_index("sqlite_autoindex_t1_1").is_none());
    assert!(db.get_index("sqlite_autoindex_t1_2").is_none());

    // Constraint enforcement must still work through the renamed indexes.
    exec_ok(&mut db, "INSERT INTO T2 VALUES(1, 10)");
    let dup_pk = exec(&mut db, "INSERT INTO T2 VALUES(2, 10)");
    assert!(
        dup_pk.is_err(),
        "PRIMARY KEY(b) must still be enforced after the implicit index rename"
    );
    let dup_unique = exec(&mut db, "INSERT INTO T2 VALUES(1, 11)");
    assert!(
        dup_unique.is_err(),
        "UNIQUE(c) must still be enforced after the implicit index rename"
    );
}

/// Same as above, but verified after a binary save/reload round trip — the
/// renamed autoindex names (and their enforcement) must survive persistence,
/// mirroring #6599's own reload coverage for explicitly-named indexes.
#[test]
fn renamed_table_implicit_autoindex_names_retarget_survives_binary_reload() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE t1(c UNIQUE, b PRIMARY KEY)");
    exec_ok(&mut db, "ALTER TABLE t1 RENAME TO T2");

    let mut reloaded = roundtrip_binary(&db, "autoindex_rename");

    let mut names: Vec<String> =
        query(&reloaded, "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
            .into_iter()
            .map(|row| match &row[0] {
                SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["sqlite_autoindex_T2_1".to_string(), "sqlite_autoindex_T2_2".to_string()],
        "renamed implicit autoindex names must survive a binary save/reload cycle (#6607)"
    );

    assert!(reloaded.get_index("sqlite_autoindex_T2_1").is_some());
    assert!(reloaded.get_index("sqlite_autoindex_T2_2").is_some());

    exec_ok(&mut reloaded, "INSERT INTO T2 VALUES(1, 10)");
    let dup_pk = exec(&mut reloaded, "INSERT INTO T2 VALUES(2, 10)");
    assert!(dup_pk.is_err(), "PRIMARY KEY(b) must still be enforced after reload");
}

/// Symptom 2 from the issue: the renamed table's exact-case name must
/// survive a save + reopen, independent of whether the table has any
/// indexes at all.
#[test]
fn renamed_table_exact_case_name_survives_binary_reload() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE t1(a, b)");
    exec_ok(&mut db, "ALTER TABLE t1 RENAME TO T2");

    // Same-session: exact case already correct (this part was never broken).
    let rows = query(&db, "SELECT name FROM sqlite_master WHERE type='table'");
    assert_eq!(rows, vec![vec![SqlValue::Varchar("T2".into())]]);

    let mut reloaded = roundtrip_binary(&db, "case");
    let rows = query(&reloaded, "SELECT name FROM sqlite_master WHERE type='table'");
    assert_eq!(
        rows,
        vec![vec![SqlValue::Varchar("T2".into())]],
        "renamed table's exact-case name must survive save/reload (#6599)"
    );
    // The renamed table must still be reachable under its new (case-folded)
    // name — SQLite identifier lookup stays case-insensitive.
    exec_ok(&mut reloaded, "INSERT INTO t2 VALUES(1, 2)");
}

/// Issue #6607 gap 2: minimal non-TCL repro isolating whether a TEMP table —
/// renamed or not — leaks into a *fresh* connection's persistent
/// `sqlite_master` view. `Database::save_binary` + `Database::load_binary` is
/// this crate's analogue of "close the connection; open a second, independent
/// connection to the same file", the exact operation SQLite's alter.test
/// alter-1.6 performs via `db close; sqlite3 db test.db`.
///
/// **Finding: this is a TCL-shim artifact, not an engine bug.** The engine
/// already isolates TEMP tables from persistence correctly —
/// `write_catalog`/`write_sql_dump_to_file`
/// (`vibesql-storage/src/persistence/{binary/catalog.rs,save.rs}`) skip every
/// temp schema (`Catalog::is_temp_schema`) when saving, and a freshly loaded
/// `Database` starts with no temp schema at all, so a TEMP table (renamed or
/// not) cannot survive a save/reload round trip by construction — this test
/// pins that down. The residual `TempTab`/`objlist` rows in alter-1.6's actual
/// TCL diff (#6607) come instead from `scripts/tester_vibesql.tcl`'s own
/// documented "TEMP TABLE emulation" strategy: because the shim spawns a fresh
/// VibeSQL CLI process per SQL batch, it *demotes* `CREATE TEMP TABLE` to a
/// genuinely persistent `CREATE TABLE` for any name that doesn't need to
/// coexist with a same-named main-schema table (the one case — issue #5591 —
/// where the shim instead keeps a real, session-scoped TEMP table and replays
/// its DDL across batches). `[temp table]`/`objlist` in alter.test hit the
/// demotion path, so they persist across the harness's simulated
/// `db close`/reopen as ordinary tables — a deliberate, documented tradeoff of
/// the shim's per-batch-process architecture (already flagged as a suspected
/// cause under #6574's "Bucket 4"), not a defect in
/// `execute_rename_table`/persistence. No engine fix is applicable here;
/// tracked as a shim-level follow-up under #6609.
#[test]
fn temp_table_does_not_leak_into_sqlite_master_after_binary_reload() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE main_table(a)");
    exec_ok(&mut db, "CREATE TEMP TABLE temp_table(a)");
    exec_ok(&mut db, "ALTER TABLE temp_table RENAME TO renamed_temp");

    // Same session: the renamed TEMP table is reachable and distinct from the
    // persistent table.
    exec_ok(&mut db, "INSERT INTO renamed_temp VALUES(1)");
    assert_eq!(query(&db, "SELECT a FROM renamed_temp"), vec![vec![SqlValue::Integer(1)]]);

    let reloaded = roundtrip_binary(&db, "temp_leak");

    // A fresh connection to the same file must see only the persistent
    // table — the (renamed) TEMP table must not have leaked in.
    let names: Vec<String> =
        query(&reloaded, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .into_iter()
            .map(|row| match &row[0] {
                SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect();
    assert_eq!(
        names,
        vec!["main_table".to_string()],
        "a TEMP table (renamed or not) must not appear in a fresh connection's \
         sqlite_master after reload — TEMP tables are session-scoped (#6607 gap 2)"
    );
}
