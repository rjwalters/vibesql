//! Transaction tests including SAVEPOINT functionality

use vibesql_ast::Statement;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{
    BeginTransactionExecutor, CommitExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor,
    ReleaseSavepointExecutor, RollbackExecutor, RollbackToSavepointExecutor, SavepointExecutor,
    UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Execute a single SQL statement, panicking on parse or execution failure.
/// Covers the DDL/DML/transaction-control statements exercised by the
/// savepoint-undo tests below (#6278).
fn run(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse error for {sql:?}: {e:?}"));
    let result: Result<(), String> = match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::BeginTransaction(s) => {
            BeginTransactionExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Commit(s) => {
            CommitExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Rollback(s) => {
            RollbackExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Savepoint(s) => {
            SavepointExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::RollbackToSavepoint(s) => {
            RollbackToSavepointExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::ReleaseSavepoint(s) => {
            ReleaseSavepointExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        other => panic!("unsupported statement type in test helper: {other:?}"),
    };
    result.unwrap_or_else(|e| panic!("failed to execute {sql:?}: {e}"));
}

/// Same as [`run`], but returns the executor's `Err` instead of panicking —
/// for statements expected to fail (e.g. a `COMMIT` that must be rejected).
fn run_expect_err(db: &mut Database, sql: &str) -> String {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse error for {sql:?}: {e:?}"));
    match stmt {
        Statement::Commit(s) => {
            CommitExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        other => panic!("unsupported statement type in run_expect_err: {other:?}"),
    }
    .expect_err("expected statement to fail")
}

#[test]
fn test_basic_savepoint() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Begin transaction
    let begin_stmt = vibesql_ast::BeginStmt { durability: vibesql_ast::DurabilityHint::Default };
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Insert initial row
    let insert_stmt = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(1000)),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create savepoint
    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    // Insert another row
    let insert_stmt2 = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(500)),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Check we have 2 rows
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 2);

    // Rollback to savepoint
    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    // Should only have 1 row now
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    // Commit
    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_nested_savepoints() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Begin transaction
    let begin_stmt = vibesql_ast::BeginStmt { durability: vibesql_ast::DurabilityHint::Default };
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Insert initial row
    let insert_stmt = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(1000)),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create first savepoint
    let savepoint_stmt1 = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt1, &mut db).unwrap();

    // Insert second row
    let insert_stmt2 = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(500)),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Create second savepoint
    let savepoint_stmt2 = vibesql_ast::SavepointStmt { name: "sp2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt2, &mut db).unwrap();

    // Insert third row
    let insert_stmt3 = vibesql_ast::InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(200)),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt3).unwrap();

    // Check we have 3 rows
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 3);

    // Rollback to sp1 (should destroy sp2 and remove rows after sp1)
    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    // Should only have 1 row now
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    // Commit
    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ---------------------------------------------------------------------------
// #6278: ROLLBACK TO SAVEPOINT must undo DELETE/UPDATE too, not just INSERT.
// ---------------------------------------------------------------------------

/// Collect a single INTEGER column's live (non-deleted) values from a table,
/// sorted, for order-independent assertions.
fn live_int_column(db: &Database, table: &str, col_idx: usize) -> Vec<i64> {
    let mut values: Vec<i64> = db
        .get_table(table)
        .unwrap()
        .scan_live()
        .map(|(_, row)| match &row.values[col_idx] {
            SqlValue::Integer(i) => *i,
            other => panic!("expected INTEGER, got {other:?}"),
        })
        .collect();
    values.sort_unstable();
    values
}

/// The exact reproduction from issue #6278: a `DELETE` issued after a named
/// `SAVEPOINT`, on a table with a `DEFERRABLE INITIALLY DEFERRED` FK
/// referenced by a still-live child row, must be fully undone by
/// `ROLLBACK TO` — both the row itself *and* the deferred FK violation the
/// DELETE queued.
#[test]
fn test_savepoint_rollback_undoes_delete_6278_repro() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    run(
        &mut db,
        "CREATE TABLE node(nodeid INTEGER PRIMARY KEY, parent INTEGER REFERENCES node(nodeid) DEFERRABLE INITIALLY DEFERRED)",
    );
    run(
        &mut db,
        "CREATE TABLE leaf(cellid TEXT PRIMARY KEY, parent INTEGER REFERENCES node(nodeid) DEFERRABLE INITIALLY DEFERRED)",
    );
    run(&mut db, "INSERT INTO node VALUES (1, NULL)");
    run(&mut db, "INSERT INTO node VALUES (2, NULL)");
    run(&mut db, "INSERT INTO leaf VALUES ('a', 2)");

    run(&mut db, "BEGIN");
    run(&mut db, "INSERT INTO leaf VALUES ('b', 1)");
    run(&mut db, "SAVEPOINT save");
    run(&mut db, "DELETE FROM node WHERE nodeid = 1");

    // The DELETE is DEFERRABLE INITIALLY DEFERRED — it succeeds immediately
    // (leaf 'b' still references node 1) and queues a deferred FK check.
    assert_eq!(
        db.deferred_fk_violations().len(),
        1,
        "DELETE of a still-referenced DEFERRABLE parent should queue a deferred FK check"
    );

    run(&mut db, "ROLLBACK TO save");

    // Bug: previously ROLLBACK TO only undid INSERT, so node row 1 stayed
    // deleted and the query below returned only [2].
    assert_eq!(
        live_int_column(&db, "node", 0),
        vec![1, 2],
        "node row 1 must be restored by ROLLBACK TO save"
    );
    // The deferred violation the undone DELETE queued must be discarded too.
    assert_eq!(
        db.deferred_fk_violations().len(),
        0,
        "ROLLBACK TO must discard the deferred FK violation queued after the savepoint"
    );

    // COMMIT must now succeed — no outstanding FK violation.
    run(&mut db, "COMMIT");
    assert_eq!(live_int_column(&db, "node", 0), vec![1, 2]);
}

/// `ROLLBACK TO` must restore the *old* column values of an UPDATE issued
/// after the savepoint, not merely the row's presence.
#[test]
fn test_savepoint_rollback_undoes_update() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER)");
    run(&mut db, "INSERT INTO accounts VALUES (1, 1000)");

    run(&mut db, "BEGIN");
    run(&mut db, "SAVEPOINT sp1");
    run(&mut db, "UPDATE accounts SET balance = 1 WHERE id = 1");

    let table = db.get_table("accounts").unwrap();
    let row = table.scan_live().next().unwrap().1;
    assert_eq!(row.values[1], SqlValue::Integer(1), "UPDATE should have applied before rollback");

    run(&mut db, "ROLLBACK TO sp1");

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1, "UPDATE must not duplicate or drop the row");
    let row = table.scan_live().next().unwrap().1;
    assert_eq!(
        row.values[1],
        SqlValue::Integer(1000),
        "ROLLBACK TO must restore the pre-UPDATE balance, not just the row's presence"
    );

    run(&mut db, "COMMIT");
}

/// `ROLLBACK TO` must undo a `DELETE` triggered by `ON DELETE CASCADE` when
/// the cascade fires after the savepoint.
#[test]
fn test_savepoint_rollback_undoes_cascade_delete() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE p(x INTEGER PRIMARY KEY)");
    run(&mut db, "CREATE TABLE c(y INTEGER REFERENCES p(x) ON DELETE CASCADE, z INTEGER)");
    run(&mut db, "INSERT INTO p VALUES (1)");
    run(&mut db, "INSERT INTO c VALUES (1, 10)");

    run(&mut db, "BEGIN");
    run(&mut db, "SAVEPOINT sp1");
    run(&mut db, "DELETE FROM p WHERE x = 1");

    assert_eq!(db.get_table("p").unwrap().row_count(), 0);
    assert_eq!(
        db.get_table("c").unwrap().row_count(),
        0,
        "cascade should have deleted the child row"
    );

    run(&mut db, "ROLLBACK TO sp1");

    assert_eq!(
        live_int_column(&db, "p", 0),
        vec![1],
        "cascade-deleted parent row must be restored"
    );
    assert_eq!(live_int_column(&db, "c", 0), vec![1], "cascade-deleted child row must be restored");

    run(&mut db, "COMMIT");
}

/// `ROLLBACK TO` must undo the delete-then-insert performed by
/// `INSERT ... OR REPLACE` when it fires after the savepoint.
#[test]
fn test_savepoint_rollback_undoes_insert_or_replace() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
    run(&mut db, "INSERT INTO t VALUES (1, 100)");

    run(&mut db, "BEGIN");
    run(&mut db, "SAVEPOINT sp1");
    run(&mut db, "INSERT OR REPLACE INTO t VALUES (1, 999)");

    assert_eq!(live_int_column(&db, "t", 1), vec![999]);

    run(&mut db, "ROLLBACK TO sp1");

    let table = db.get_table("t").unwrap();
    assert_eq!(table.row_count(), 1, "REPLACE's delete-then-insert must not leave duplicate rows");
    let row = table.scan_live().next().unwrap().1;
    assert_eq!(
        row.values[1],
        SqlValue::Integer(100),
        "ROLLBACK TO must restore the pre-REPLACE row value"
    );

    run(&mut db, "COMMIT");
}

/// Nested savepoints: rolling back to an outer savepoint must undo
/// DELETE/UPDATE performed under all inner savepoints, mirroring the
/// existing INSERT-only nested case ([`test_nested_savepoints`]).
#[test]
fn test_nested_savepoints_undo_delete_and_update() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER)");
    run(&mut db, "INSERT INTO accounts VALUES (1, 1000)");
    run(&mut db, "INSERT INTO accounts VALUES (2, 500)");

    run(&mut db, "BEGIN");
    run(&mut db, "SAVEPOINT sp1");
    run(&mut db, "UPDATE accounts SET balance = 1 WHERE id = 1");
    run(&mut db, "SAVEPOINT sp2");
    run(&mut db, "DELETE FROM accounts WHERE id = 2");

    assert_eq!(db.get_table("accounts").unwrap().row_count(), 1);

    // Roll back to sp1: destroys sp2 and undoes both the inner DELETE and
    // the outer UPDATE.
    run(&mut db, "ROLLBACK TO sp1");

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 2, "DELETE under the inner savepoint must be undone");
    for (_, row) in table.scan_live() {
        if row.values[0] == SqlValue::Integer(1) {
            assert_eq!(
                row.values[1],
                SqlValue::Integer(1000),
                "UPDATE under the outer savepoint must also be undone"
            );
        }
    }

    run(&mut db, "COMMIT");
}

/// `RELEASE SAVEPOINT` must keep DELETE/UPDATE changes (they are not rolled
/// back) — regression guard against #6278 accidentally reversing this.
#[test]
fn test_release_savepoint_keeps_delete_and_update() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER)");
    run(&mut db, "INSERT INTO accounts VALUES (1, 1000)");
    run(&mut db, "INSERT INTO accounts VALUES (2, 500)");

    run(&mut db, "BEGIN");
    run(&mut db, "SAVEPOINT sp1");
    run(&mut db, "UPDATE accounts SET balance = 1 WHERE id = 1");
    run(&mut db, "DELETE FROM accounts WHERE id = 2");
    run(&mut db, "RELEASE SAVEPOINT sp1");

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1, "RELEASE must keep the DELETE");
    let row = table.scan_live().next().unwrap().1;
    assert_eq!(row.values[1], SqlValue::Integer(1), "RELEASE must keep the UPDATE");

    run(&mut db, "COMMIT");
}

/// A full-transaction `ROLLBACK` after a savepoint-scoped DELETE/UPDATE must
/// remain unaffected by the #6278 fix (it already used a separate,
/// already-correct wholesale-clone mechanism).
#[test]
fn test_full_rollback_after_savepoint_delete_and_update_unaffected() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER)");
    run(&mut db, "INSERT INTO accounts VALUES (1, 1000)");
    run(&mut db, "INSERT INTO accounts VALUES (2, 500)");

    run(&mut db, "BEGIN");
    run(&mut db, "SAVEPOINT sp1");
    run(&mut db, "UPDATE accounts SET balance = 1 WHERE id = 1");
    run(&mut db, "DELETE FROM accounts WHERE id = 2");
    run(&mut db, "ROLLBACK");

    assert!(!db.in_transaction());
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 2, "full ROLLBACK must restore both rows");
    for (_, row) in table.scan_live() {
        if row.values[0] == SqlValue::Integer(1) {
            assert_eq!(row.values[1], SqlValue::Integer(1000));
        }
    }
}

/// A `ROLLBACK TO` that undoes a deferred-FK-queuing DELETE, followed by a
/// second attempt that leaves the violation unresolved, must still fail
/// `COMMIT` — proving the fix doesn't over-discard the deferred-FK queue.
#[test]
fn test_savepoint_rollback_then_unresolved_deferred_fk_still_fails_commit() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(
        &mut db,
        "CREATE TABLE node(nodeid INTEGER PRIMARY KEY, parent INTEGER REFERENCES node(nodeid) DEFERRABLE INITIALLY DEFERRED)",
    );
    run(&mut db, "INSERT INTO node VALUES (1, NULL)");
    run(&mut db, "INSERT INTO node VALUES (2, 1)");

    run(&mut db, "BEGIN");
    run(&mut db, "SAVEPOINT save");
    run(&mut db, "DELETE FROM node WHERE nodeid = 1");
    run(&mut db, "ROLLBACK TO save");
    assert_eq!(db.deferred_fk_violations().len(), 0, "undone DELETE's violation must be discarded");

    // Issue the same violating DELETE again, this time without a further
    // ROLLBACK TO — the violation must still be live and COMMIT must fail.
    run(&mut db, "DELETE FROM node WHERE nodeid = 1");
    assert_eq!(db.deferred_fk_violations().len(), 1);
    let err = run_expect_err(&mut db, "COMMIT");
    assert!(err.contains("FOREIGN KEY"), "expected FK error, got: {err}");

    // Clean up: undo the failed commit's dangling transaction so the test
    // does not leak an active transaction.
    run(&mut db, "ROLLBACK");
}
