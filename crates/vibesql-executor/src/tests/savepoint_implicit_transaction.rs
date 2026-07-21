//! Tests for SQLite's implicit-transaction-on-SAVEPOINT semantics (part of
//! #6170, fkey2-2.*). A `SAVEPOINT` issued outside an explicit `BEGIN` opens a
//! transaction; that transaction is auto-committed when the matching outermost
//! `RELEASE` empties the savepoint stack, and the commit runs the deferred-FK
//! re-check (so a `RELEASE` that finalizes a transaction carrying an
//! outstanding deferred violation fails just like `COMMIT`). A transaction
//! opened by an explicit `BEGIN` is unaffected: `RELEASE` of the last savepoint
//! leaves it open until an explicit `COMMIT`.

use vibesql_ast::Statement;
use vibesql_storage::Database;

fn exec_sql(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        Statement::Delete(s) => crate::DeleteExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) deleted", count))
            .map_err(|e| e.to_string()),
        Statement::BeginTransaction(s) => {
            crate::BeginTransactionExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Commit(s) => crate::CommitExecutor::execute(&s, db).map_err(|e| e.to_string()),
        Statement::Rollback(s) => {
            crate::RollbackExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Savepoint(s) => {
            crate::SavepointExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::RollbackToSavepoint(s) => {
            crate::RollbackToSavepointExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::ReleaseSavepoint(s) => {
            crate::ReleaseSavepointExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        other => Err(format!("Unsupported statement type: {:?}", other)),
    }
}

fn row_count(db: &Database, table: &str) -> usize {
    db.get_table(table).expect("table exists").row_count()
}

#[test]
fn savepoint_outside_begin_opens_implicit_transaction() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a PRIMARY KEY, b)").unwrap();

    assert!(!db.in_transaction());
    exec_sql(&mut db, "SAVEPOINT s1").unwrap();
    // The savepoint auto-started a transaction, and it is flagged implicit.
    assert!(db.in_transaction());
    assert!(db.is_implicit_savepoint_txn());
    assert_eq!(db.savepoint_depth(), 1);
}

#[test]
fn release_of_outermost_implicit_savepoint_commits() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a PRIMARY KEY, b)").unwrap();

    exec_sql(&mut db, "SAVEPOINT s1").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 10)").unwrap();
    exec_sql(&mut db, "RELEASE s1").unwrap();

    // RELEASE of the outermost implicit savepoint committed the transaction.
    assert!(!db.in_transaction());
    assert_eq!(row_count(&db, "t1"), 1);
}

#[test]
fn nested_release_only_commits_when_stack_empties() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a PRIMARY KEY, b)").unwrap();

    exec_sql(&mut db, "SAVEPOINT s1").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 10)").unwrap();
    exec_sql(&mut db, "SAVEPOINT s2").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(2, 20)").unwrap();
    // Releasing the inner savepoint does not commit — the outer one remains.
    exec_sql(&mut db, "RELEASE s2").unwrap();
    assert!(db.in_transaction());
    assert_eq!(db.savepoint_depth(), 1);
    // Releasing the outer savepoint empties the stack and commits.
    exec_sql(&mut db, "RELEASE s1").unwrap();
    assert!(!db.in_transaction());
    assert_eq!(row_count(&db, "t1"), 2);
}

#[test]
fn rollback_to_implicit_savepoint_keeps_transaction_open() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a PRIMARY KEY, b)").unwrap();

    exec_sql(&mut db, "SAVEPOINT s1").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 10)").unwrap();
    exec_sql(&mut db, "SAVEPOINT s2").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(2, 20)").unwrap();
    // ROLLBACK TO undoes the inner insert but leaves the transaction open.
    exec_sql(&mut db, "ROLLBACK TO s2").unwrap();
    assert!(db.in_transaction());
    exec_sql(&mut db, "RELEASE s1").unwrap();
    assert!(!db.in_transaction());
    assert_eq!(row_count(&db, "t1"), 1);
}

#[test]
fn release_runs_deferred_fk_check_and_can_fail() {
    // fkey2-2.40: a deferred FK violation queued under an implicit-savepoint
    // transaction must surface when the outermost RELEASE commits.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    exec_sql(
        &mut db,
        "CREATE TABLE node(nodeid PRIMARY KEY, parent REFERENCES node DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    exec_sql(&mut db, "INSERT INTO node VALUES(1, NULL)").unwrap();

    exec_sql(&mut db, "SAVEPOINT outer").unwrap();
    // References a non-existent parent (3) — deferred, so it does not fail now.
    exec_sql(&mut db, "INSERT INTO node VALUES(2, 3)").unwrap();
    // RELEASE commits, which runs the deferred check and fails.
    let err = exec_sql(&mut db, "RELEASE outer").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint failed"), "unexpected error: {err}");
    // The failed commit rolled the transaction back to autocommit.
    assert!(!db.in_transaction());
}

#[test]
fn explicit_begin_release_does_not_commit() {
    // With an explicit BEGIN, RELEASE of the last savepoint must NOT commit;
    // a subsequent ROLLBACK still undoes the whole transaction.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a PRIMARY KEY, b)").unwrap();

    exec_sql(&mut db, "BEGIN").unwrap();
    assert!(!db.is_implicit_savepoint_txn());
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 10)").unwrap();
    exec_sql(&mut db, "SAVEPOINT s1").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(2, 20)").unwrap();
    exec_sql(&mut db, "RELEASE s1").unwrap();
    // Still in the explicit transaction.
    assert!(db.in_transaction());
    exec_sql(&mut db, "ROLLBACK").unwrap();
    assert!(!db.in_transaction());
    assert_eq!(row_count(&db, "t1"), 0);
}
