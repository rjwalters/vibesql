//! Tests for deferred FOREIGN KEY enforcement (Phase C2 of #5085).
//!
//! These tests cover:
//! 1. INITIALLY DEFERRED FK constraints — child INSERT with missing parent succeeds initially and
//!    is re-checked at COMMIT.
//! 2. PRAGMA defer_foreign_keys=ON — session flag defers all FK checks until COMMIT (and is
//!    auto-reset on COMMIT, see fkey6-1.10.1).
//! 3. ROLLBACK TO savepoint — discards deferred violations queued after the savepoint.
//! 4. ROLLBACK — discards the entire deferred queue.
//! 5. Resolution — a deferred violation that is later "fixed" (parent inserted, or child deleted)
//!    does *not* abort the COMMIT.

use vibesql_ast::Statement;
use vibesql_executor::{
    BeginTransactionExecutor, CommitExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor,
    ReleaseSavepointExecutor, RollbackExecutor, RollbackToSavepointExecutor, SavepointExecutor,
    UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Execute a single SQL statement, panicking on parse failure but
/// returning the executor result so callers can assert on success or
/// failure.
fn run(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
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
// 1. INITIALLY DEFERRED constraint defers child INSERT until COMMIT
// ---------------------------------------------------------------------------

#[test]
fn deferred_child_insert_without_parent_fails_at_commit() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();

    // INSERT into child WITHOUT a matching parent — must succeed
    // (deferred), not fail immediately.
    run(&mut db, "INSERT INTO c VALUES (1, 99)").expect("deferred INSERT must succeed");

    // The violation must be queued.
    assert_eq!(db.deferred_fk_violations().len(), 1, "deferred FK violation must be queued");

    // COMMIT must fail because the violation was never resolved.
    let err = run(&mut db, "COMMIT").expect_err("COMMIT must fail when FK still violated");
    assert!(err.contains("FOREIGN KEY"), "expected FK error message, got: {}", err);

    // Per EVIDENCE-OF R-37736-42616, a COMMIT that fails on an outstanding
    // deferred FK violation does NOT force-roll-back the transaction — the
    // transaction stays OPEN so the caller can fix the violation and retry
    // (or explicitly ROLLBACK). The child row remains present in that still-
    // open transaction.
    assert!(db.in_transaction(), "transaction must remain open after failed COMMIT");
    let c = db.get_table("c").expect("table c");
    assert_eq!(c.scan_live().count(), 1, "child row remains present in the still-open transaction");

    // Resolve the violation by inserting the missing parent, then retry the
    // COMMIT — it must now succeed and persist the child row.
    run(&mut db, "INSERT INTO p VALUES (99)").unwrap();
    run(&mut db, "COMMIT").expect("COMMIT must succeed once the violation is resolved");
    assert!(!db.in_transaction(), "transaction closes after the successful retry COMMIT");
    let c = db.get_table("c").expect("table c");
    assert_eq!(c.scan_live().count(), 1, "child row is committed");
}

#[test]
fn deferred_child_insert_with_parent_inserted_later_commits() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();

    // Insert child first — would normally fail, but is deferred.
    run(&mut db, "INSERT INTO c VALUES (1, 42)").unwrap();
    // Now insert the parent — resolves the deferred violation.
    run(&mut db, "INSERT INTO p VALUES (42)").unwrap();

    // COMMIT must succeed because the parent now exists.
    run(&mut db, "COMMIT").expect("COMMIT must succeed when violation resolved");

    let c = db.get_table("c").unwrap();
    let p = db.get_table("p").unwrap();
    assert_eq!(c.scan().len(), 1);
    assert_eq!(p.scan().len(), 1);
}

#[test]
fn deferred_child_insert_followed_by_child_delete_commits() {
    // If the offending child row is deleted before COMMIT, the deferred
    // violation must NOT abort the commit.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();
    run(&mut db, "INSERT INTO c VALUES (1, 7)").unwrap();
    // Resolve the violation by deleting the offending child row.
    run(&mut db, "DELETE FROM c WHERE id = 1").unwrap();

    run(&mut db, "COMMIT").expect("COMMIT must succeed when child deleted before commit");
}

// ---------------------------------------------------------------------------
// 2. Non-deferred FK constraints still fail immediately
// ---------------------------------------------------------------------------

#[test]
fn non_deferrable_fk_still_fails_immediately() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(&mut db, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))").unwrap();

    run(&mut db, "BEGIN").unwrap();
    let err = run(&mut db, "INSERT INTO c VALUES (1, 99)")
        .expect_err("non-deferrable FK must fail at INSERT, not at COMMIT");
    assert!(err.contains("FOREIGN KEY"));
    assert_eq!(
        db.deferred_fk_violations().len(),
        0,
        "non-deferrable FK violation must not be queued"
    );
    run(&mut db, "ROLLBACK").unwrap();
}

// ---------------------------------------------------------------------------
// 3. PRAGMA defer_foreign_keys=ON defers all FK checks
// ---------------------------------------------------------------------------

#[test]
fn pragma_defer_foreign_keys_defers_non_deferrable_fk() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    // Note: NO `DEFERRABLE INITIALLY DEFERRED` clause — relies on the
    // session flag.
    run(&mut db, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))").unwrap();

    run(&mut db, "BEGIN").unwrap();
    db.set_defer_foreign_keys(true);

    // INSERT that would normally fail must now succeed.
    run(&mut db, "INSERT INTO c VALUES (1, 99)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 1);

    // COMMIT fails on the unresolved violation but, per EVIDENCE-OF
    // R-37736-42616, leaves the transaction OPEN (no force-rollback).
    let err = run(&mut db, "COMMIT").expect_err("COMMIT must fail with unresolved violation");
    assert!(err.contains("FOREIGN KEY"));
    assert!(db.in_transaction(), "failed COMMIT must not roll the transaction back");
}

#[test]
fn pragma_defer_foreign_keys_resets_at_commit() {
    // Per fkey6-1.10.1: defer_foreign_keys is automatically reset to
    // OFF on every COMMIT.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(&mut db, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))").unwrap();

    run(&mut db, "BEGIN").unwrap();
    db.set_defer_foreign_keys(true);
    assert!(db.defer_foreign_keys());

    // No violation pending — commit succeeds.
    run(&mut db, "INSERT INTO p VALUES (1)").unwrap();
    run(&mut db, "COMMIT").unwrap();

    // The flag must now be OFF.
    assert!(
        !db.defer_foreign_keys(),
        "defer_foreign_keys must reset to OFF at COMMIT (fkey6-1.10.1)"
    );
}

// ---------------------------------------------------------------------------
// 4. ROLLBACK / SAVEPOINT semantics
// ---------------------------------------------------------------------------

#[test]
fn rollback_clears_deferred_fk_queue() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();
    run(&mut db, "INSERT INTO c VALUES (1, 99)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 1);

    run(&mut db, "ROLLBACK").unwrap();
    // The transaction is gone — queue must be empty.
    assert_eq!(db.deferred_fk_violations().len(), 0);
}

#[test]
fn savepoint_rollback_discards_violations_queued_after_savepoint() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();

    // Queue one violation BEFORE the savepoint (this one survives ROLLBACK TO).
    run(&mut db, "INSERT INTO c VALUES (1, 100)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 1);

    run(&mut db, "SAVEPOINT sp1").unwrap();

    // Queue two more violations AFTER the savepoint.
    run(&mut db, "INSERT INTO c VALUES (2, 200)").unwrap();
    run(&mut db, "INSERT INTO c VALUES (3, 300)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 3);

    // ROLLBACK TO drops the two post-savepoint violations.
    run(&mut db, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    assert_eq!(
        db.deferred_fk_violations().len(),
        1,
        "ROLLBACK TO must truncate the deferred queue back to the savepoint"
    );

    // Resolve the surviving violation.
    run(&mut db, "INSERT INTO p VALUES (100)").unwrap();
    run(&mut db, "COMMIT").expect("COMMIT must succeed once surviving violation is resolved");
}

#[test]
fn release_savepoint_keeps_deferred_violations() {
    // RELEASE SAVEPOINT does NOT discard the queue — entries propagate
    // to the outer scope and are re-checked at COMMIT.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();
    run(&mut db, "SAVEPOINT sp1").unwrap();
    run(&mut db, "INSERT INTO c VALUES (1, 7)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 1);

    run(&mut db, "RELEASE SAVEPOINT sp1").unwrap();
    assert_eq!(
        db.deferred_fk_violations().len(),
        1,
        "RELEASE SAVEPOINT must NOT discard deferred violations"
    );

    // The unresolved violation must still abort the COMMIT.
    let err = run(&mut db, "COMMIT").expect_err("commit must fail");
    assert!(err.contains("FOREIGN KEY"));
}

// ---------------------------------------------------------------------------
// 5. Multi-row scenarios
// ---------------------------------------------------------------------------

#[test]
fn multiple_deferred_violations_first_unresolved_fails_commit() {
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    run(&mut db, "BEGIN").unwrap();
    run(&mut db, "INSERT INTO c VALUES (1, 10)").unwrap();
    run(&mut db, "INSERT INTO c VALUES (2, 20)").unwrap();
    run(&mut db, "INSERT INTO c VALUES (3, 30)").unwrap();
    assert_eq!(db.deferred_fk_violations().len(), 3);

    // Resolve only TWO of the three.
    run(&mut db, "INSERT INTO p VALUES (10)").unwrap();
    run(&mut db, "INSERT INTO p VALUES (20)").unwrap();

    let err = run(&mut db, "COMMIT")
        .expect_err("COMMIT must fail when at least one deferred violation remains");
    assert!(err.contains("FOREIGN KEY"));
}

#[test]
fn outside_transaction_deferred_constraint_still_enforced_immediately() {
    // Deferred enforcement requires a transaction context. Outside a
    // transaction (auto-commit), an INITIALLY DEFERRED constraint must
    // still fail immediately.
    let mut db = fresh_db();
    run(&mut db, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    run(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();

    let err = run(&mut db, "INSERT INTO c VALUES (1, 99)")
        .expect_err("auto-commit INSERT with no parent must fail immediately");
    assert!(err.contains("FOREIGN KEY"));
}
