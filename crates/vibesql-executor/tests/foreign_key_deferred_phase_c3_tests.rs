//! Tests for Phase C3 of #5085 — deferred FK edge cases.
//!
//! Covers:
//!
//! 1. End-to-end savepoint queue truncation: a deferred violation is queued
//!    inside a savepoint, then `ROLLBACK TO SAVEPOINT` discards it; the
//!    outer COMMIT must succeed (Phase C2 wired the snapshot index;
//!    Phase C3 verifies the integration end-to-end).
//! 2. `DEFERRABLE INITIALLY IMMEDIATE` defers when the session pragma
//!    `defer_foreign_keys=ON` is set, mirroring SQLite's documented
//!    behaviour (the session pragma overrides per-constraint defaults).
//! 3. `DEFERRABLE INITIALLY IMMEDIATE` enforces immediately when the
//!    session pragma is OFF (the constraint default).
//! 4. Self-referential FK INSERT (`fkey8-3.0`): a single row that
//!    satisfies its own self-FK must succeed.
//! 5. Self-referential FK DELETE (`fkey8-3.1`): deleting the last
//!    surviving parent of a self-FK row must fail when the FK references
//!    a non-PK key.
//! 6. `SHOW CREATE TABLE` emits `DEFERRABLE INITIALLY {DEFERRED,IMMEDIATE}`
//!    for FKs whose deferral state is non-default; omits the clause for
//!    NOT-DEFERRABLE FKs.

use vibesql_ast::Statement;
use vibesql_executor::{
    BeginTransactionExecutor, CommitExecutor, CreateIndexExecutor, CreateTableExecutor,
    DeleteExecutor, InsertExecutor, IntrospectionExecutor, ReleaseSavepointExecutor,
    RollbackExecutor, RollbackToSavepointExecutor, SavepointExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn run(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, db)
            .map(|_| String::new())
            .map_err(|e| e.to_string()),
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::BeginTransaction(s) => {
            BeginTransactionExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Commit(s) => CommitExecutor::execute(&s, db).map_err(|e| e.to_string()),
        Statement::Rollback(s) => RollbackExecutor::execute(&s, db).map_err(|e| e.to_string()),
        Statement::Savepoint(s) => SavepointExecutor::execute(&s, db).map_err(|e| e.to_string()),
        Statement::RollbackToSavepoint(s) => {
            RollbackToSavepointExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::ReleaseSavepoint(s) => {
            ReleaseSavepointExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        other => Err(format!("unsupported statement type in test helper: {:?}", other)),
    }
}

fn fresh_db() -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    db
}

// ---------------------------------------------------------------------------
// 1. End-to-end savepoint queue truncation (Phase C2 integration verified
//    end-to-end here).
// ---------------------------------------------------------------------------

#[test]
fn savepoint_rollback_discards_deferred_violation_then_commit_succeeds() {
    // The exact scenario the issue calls out: queue a deferred FK
    // violation inside a savepoint, ROLLBACK TO the savepoint, then
    // COMMIT. The COMMIT must succeed because the queued violation is
    // discarded together with the savepoint's row mutations.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "INSERT INTO p VALUES (1)").unwrap();

    run(&mut db, "BEGIN").unwrap();
    run(&mut db, "SAVEPOINT sp1").unwrap();

    // Queue the violation inside the savepoint.
    run(&mut db, "INSERT INTO c VALUES (10, 999)").expect("deferred INSERT must succeed");
    assert_eq!(db.deferred_fk_violations().len(), 1, "violation must be queued");

    // ROLLBACK TO the savepoint — the queue must be truncated.
    run(&mut db, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    assert_eq!(
        db.deferred_fk_violations().len(),
        0,
        "ROLLBACK TO SAVEPOINT must truncate the deferred-FK queue"
    );

    // COMMIT must succeed: there is no outstanding violation.
    run(&mut db, "COMMIT").expect("COMMIT must succeed after the violation is discarded");

    let c = db.get_table("c").unwrap();
    assert_eq!(c.scan().len(), 0, "rolled-back child row must not be visible");
}

#[test]
fn nested_savepoint_only_inner_violations_discarded() {
    // Outer SAVEPOINT contains one queued violation; inner SAVEPOINT
    // contains a second. Rolling back to the inner savepoint discards
    // only the inner violation; rolling back to the outer discards both.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();

    run(&mut db, "SAVEPOINT outer_sp").unwrap();
    run(&mut db, "INSERT INTO c VALUES (1, 100)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 1);

    run(&mut db, "SAVEPOINT inner_sp").unwrap();
    run(&mut db, "INSERT INTO c VALUES (2, 200)").unwrap();
    run(&mut db, "INSERT INTO c VALUES (3, 300)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 3);

    // Rollback inner only.
    run(&mut db, "ROLLBACK TO SAVEPOINT inner_sp").unwrap();
    assert_eq!(
        db.deferred_fk_violations().len(),
        1,
        "ROLLBACK TO inner must discard only inner violations"
    );

    // Rollback outer next.
    run(&mut db, "ROLLBACK TO SAVEPOINT outer_sp").unwrap();
    assert_eq!(
        db.deferred_fk_violations().len(),
        0,
        "ROLLBACK TO outer must discard the remaining violation"
    );

    run(&mut db, "COMMIT").expect("COMMIT must succeed once all violations rolled back");
}

// ---------------------------------------------------------------------------
// 2. DEFERRABLE INITIALLY IMMEDIATE: session pragma overrides constraint
//    default.
// ---------------------------------------------------------------------------

#[test]
fn initially_immediate_defers_when_session_pragma_enabled() {
    // Per SQLite documentation: `PRAGMA defer_foreign_keys=ON` defers
    // *all* FK checks for the duration of the current transaction,
    // including INITIALLY IMMEDIATE. Confirm VibeSQL matches.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY IMMEDIATE)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();
    db.set_defer_foreign_keys(true);

    // Without the session pragma this would fail immediately. With the
    // pragma it must be queued for COMMIT-time re-check.
    run(&mut db, "INSERT INTO c VALUES (1, 99)")
        .expect("INITIALLY IMMEDIATE must defer when defer_foreign_keys=ON");
    assert_eq!(db.deferred_fk_violations().len(), 1);

    // The pragma stays effective: COMMIT then re-validates.
    let err = run(&mut db, "COMMIT").expect_err("COMMIT must fail with unresolved violation");
    assert!(err.contains("FOREIGN KEY"));
}

#[test]
fn initially_immediate_enforces_immediately_without_session_pragma() {
    // Default behaviour: INITIALLY IMMEDIATE acts like a non-deferrable
    // constraint when `defer_foreign_keys=OFF` (the SQL default).
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY IMMEDIATE)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();
    // No pragma; constraint default takes effect.
    let err = run(&mut db, "INSERT INTO c VALUES (1, 99)")
        .expect_err("INITIALLY IMMEDIATE must fail at INSERT when session pragma is off");
    assert!(err.contains("FOREIGN KEY"));
    assert_eq!(
        db.deferred_fk_violations().len(),
        0,
        "INITIALLY IMMEDIATE must not queue a violation when pragma is off"
    );
    run(&mut db, "ROLLBACK").unwrap();
}

// ---------------------------------------------------------------------------
// 3. Self-referential FK INSERT (fkey8-3.0).
// ---------------------------------------------------------------------------

#[test]
fn self_referential_fk_insert_self_satisfying_row_succeeds() {
    // Mirror of fkey8-3.0: a row whose FK columns refer back to its own
    // candidate key columns must be insertable when no prior parent row
    // exists. SQLite checks the parent index after the row is inserted;
    // VibeSQL's row-existence check now considers the new row itself
    // when child_table == parent_table.
    let mut db = fresh_db();
    run(
        &mut db,
        "CREATE TABLE t2 (a INTEGER PRIMARY KEY, b TEXT, c TEXT, d TEXT, e TEXT, FOREIGN KEY(b, c) REFERENCES t2(d, e))",
    )
    .unwrap();
    run(&mut db, "CREATE UNIQUE INDEX idx_t2_de ON t2(d, e)").unwrap();

    // First row: b/c match its own d/e — must succeed.
    run(&mut db, "INSERT INTO t2 VALUES (1, 'one', 'one', 'one', 'one')")
        .expect("self-satisfying self-FK row must insert");

    // Second row: b/c match row 1's d/e.
    run(&mut db, "INSERT INTO t2 VALUES (2, 'one', 'one', 'one', 'two')")
        .expect("second row referencing existing parent must insert");

    let t2 = db.get_table("t2").unwrap();
    assert_eq!(t2.scan().len(), 2);
}

#[test]
fn self_referential_fk_insert_with_no_match_fails() {
    // The self-FK fix must NOT silently allow rows whose b/c columns
    // do not match any (d, e) values — including their own.
    let mut db = fresh_db();
    run(
        &mut db,
        "CREATE TABLE t2 (a INTEGER PRIMARY KEY, b TEXT, c TEXT, d TEXT, e TEXT, FOREIGN KEY(b, c) REFERENCES t2(d, e))",
    )
    .unwrap();
    run(&mut db, "CREATE UNIQUE INDEX idx_t2_de ON t2(d, e)").unwrap();

    let err = run(&mut db, "INSERT INTO t2 VALUES (1, 'x', 'y', 'a', 'b')")
        .expect_err("non-matching self-FK row must fail");
    assert!(err.contains("FOREIGN KEY"));
}

// ---------------------------------------------------------------------------
// 4. Self-referential DELETE (fkey8-3.1).
// ---------------------------------------------------------------------------

#[test]
fn self_referential_fk_delete_orphans_child_row_fails() {
    // Mirror of fkey8-3.1. After the two inserts, deleting the only
    // row whose (d, e) matches row-2's (b, c) must fail with NO ACTION.
    let mut db = fresh_db();
    run(
        &mut db,
        "CREATE TABLE t2 (a INTEGER PRIMARY KEY, b TEXT, c TEXT, d TEXT, e TEXT, FOREIGN KEY(b, c) REFERENCES t2(d, e))",
    )
    .unwrap();
    run(&mut db, "CREATE UNIQUE INDEX idx_t2_de ON t2(d, e)").unwrap();
    run(&mut db, "INSERT INTO t2 VALUES (1, 'one', 'one', 'one', 'one')").unwrap();
    // Row 2's b/c reference row 1's d/e (NOT row 2's own d/e).
    run(&mut db, "INSERT INTO t2 VALUES (2, 'one', 'one', 'two', 'two')").unwrap();

    let err = run(&mut db, "DELETE FROM t2 WHERE a = 1")
        .expect_err("DELETE that orphans a self-referencing child must fail");
    assert!(err.contains("FOREIGN KEY"));
}

// ---------------------------------------------------------------------------
// 5. SHOW CREATE TABLE deferral output.
// ---------------------------------------------------------------------------

fn show_create(db: &Database, table: &str) -> String {
    let sql = format!("SHOW CREATE TABLE {}", table);
    let stmt = Parser::parse_sql(&sql).expect("parse SHOW CREATE TABLE");
    let s = match stmt {
        Statement::ShowCreateTable(s) => s,
        other => panic!("expected SHOW CREATE TABLE, got {:?}", other),
    };
    let executor = IntrospectionExecutor::new(db);
    let result = executor.execute_show_create_table(&s).expect("show create table");
    let row = &result.rows[0];
    match row.values.get(1) {
        Some(SqlValue::Varchar(v)) => v.to_string(),
        other => panic!("unexpected value for Create Table column: {:?}", other),
    }
}

#[test]
fn show_create_table_emits_initially_deferred() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    let sql = show_create(&mut db, "c");
    assert!(
        sql.contains("DEFERRABLE INITIALLY DEFERRED"),
        "expected DEFERRABLE INITIALLY DEFERRED in SHOW CREATE TABLE output, got: {}",
        sql
    );
}

#[test]
fn show_create_table_emits_initially_immediate_for_deferrable_fk() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY IMMEDIATE)",
    )
    .unwrap();

    let sql = show_create(&mut db, "c");
    assert!(
        sql.contains("DEFERRABLE INITIALLY IMMEDIATE"),
        "expected DEFERRABLE INITIALLY IMMEDIATE in SHOW CREATE TABLE output, got: {}",
        sql
    );
}

#[test]
fn show_create_table_omits_deferral_for_non_deferrable_fk() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(&mut db, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))").unwrap();

    let sql = show_create(&mut db, "c");
    assert!(
        !sql.contains("DEFERRABLE"),
        "non-deferrable FK must not emit a DEFERRABLE clause; got: {}",
        sql
    );
}

// ---------------------------------------------------------------------------
// 6. Resolution scenario from the issue body — deferred FK violation that
//    becomes valid before COMMIT.
// ---------------------------------------------------------------------------

#[test]
fn deferred_fk_violation_resolved_by_later_parent_insert_commits() {
    // Acceptance criterion: "Rust unit test: deferred FK violation that
    // becomes valid before COMMIT (parent inserted later) commits
    // successfully." There is already an analogous test in
    // foreign_key_deferred_tests.rs; this version exercises the
    // session-pragma path (rather than INITIALLY DEFERRED) to broaden
    // coverage.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(&mut db, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))").unwrap();

    run(&mut db, "BEGIN").unwrap();
    db.set_defer_foreign_keys(true);

    run(&mut db, "INSERT INTO c VALUES (1, 42)").unwrap();
    run(&mut db, "INSERT INTO p VALUES (42)").unwrap();

    run(&mut db, "COMMIT").expect("COMMIT must succeed once parent is inserted");

    let c = db.get_table("c").unwrap();
    let p = db.get_table("p").unwrap();
    assert_eq!(c.scan().len(), 1);
    assert_eq!(p.scan().len(), 1);
}
