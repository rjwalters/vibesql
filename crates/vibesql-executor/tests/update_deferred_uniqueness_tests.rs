//! Regression tests for issue #5137: deferred UNIQUE / PRIMARY KEY checks during UPDATE
//!
//! SQLite defers UNIQUE constraint checks until statement end, so updates that shift PK
//! values within the existing key space — `UPDATE p SET a = a - 1`, swap-via-temp,
//! composite PK shifts — must succeed when the *final* state has no duplicates, even
//! if intermediate states transiently duplicate keys.
//!
//! These tests verify both:
//! 1. Statements that previously produced spurious "UNIQUE constraint failed" errors
//!    now succeed and produce the expected final state.
//! 2. Statements that produce a genuine final-state duplicate still fail, with the
//!    error reported at the end of the statement (post-statement deferred check).

use vibesql_executor::{
    CreateTableExecutor, DeleteExecutor, ExecutorError, InsertExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Run a sequence of SQL statements (`;`-separated), panicking on failure.
fn run_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        execute_statement(&stmt, db);
    }
}

/// Run a single UPDATE statement, returning the result so tests can assert success/failure.
fn run_update(db: &mut Database, sql: &str) -> Result<usize, ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse UPDATE");
    match stmt {
        vibesql_ast::Statement::Update(u) => UpdateExecutor::execute(&u, db),
        other => panic!("Expected UPDATE statement, got {:?}", other),
    }
}

fn execute_statement(stmt: &vibesql_ast::Statement, db: &mut Database) {
    use vibesql_ast::Statement;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(s, db).expect("CREATE TABLE failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, s).expect("INSERT failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(s, db).expect("DELETE failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(s, db).expect("UPDATE failed");
        }
        Statement::CreateIndex(s) => {
            vibesql_executor::CreateIndexExecutor::execute(s, db).expect("CREATE INDEX failed");
        }
        _ => panic!("Unsupported statement type in test helper"),
    }
}

fn get_rows(db: &Database, table: &str) -> Vec<Vec<SqlValue>> {
    db.get_table(table)
        .expect("table not found")
        .scan()
        .iter()
        .map(|r| r.values.to_vec())
        .collect()
}

// ---------------------------------------------------------------------------
// Issue reproducer: `UPDATE p SET a = a - 1`
// ---------------------------------------------------------------------------

#[test]
fn pk_shift_decrement_succeeds() {
    // From the issue body: must succeed in SQLite, currently errors in VibeSQL.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         INSERT INTO p VALUES (0, 'zero'); \
         INSERT INTO p VALUES (1, 'one');",
    );

    let count = run_update(&mut db, "UPDATE p SET a = a - 1").expect(
        "UPDATE p SET a = a - 1 should succeed; intermediate UNIQUE collisions must be \
         deferred until statement end (issue #5137)",
    );
    assert_eq!(count, 2);

    let rows = get_rows(&db, "p");
    assert_eq!(rows.len(), 2);
    // Row order in storage matches insertion order; values verify the shift happened.
    assert_eq!(rows[0][0], SqlValue::Integer(-1));
    assert_eq!(rows[1][0], SqlValue::Integer(0));
}

#[test]
fn pk_shift_increment_succeeds() {
    // Mirror of the decrement case — `UPDATE p SET a = a + 1` shifts upward, transiently
    // collides between rows, must still succeed.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO p VALUES (3, 'three');",
    );

    let count = run_update(&mut db, "UPDATE p SET a = a + 1").expect("PK increment should succeed");
    assert_eq!(count, 3);

    let rows = get_rows(&db, "p");
    let pks: Vec<i64> = rows
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!("expected integer pk"),
        })
        .collect();
    assert_eq!(pks, vec![2, 3, 4]);
}

#[test]
fn pk_negation_succeeds() {
    // `UPDATE t SET a = -a` is the classic swap-via-temp case: rows (1,2) become (-1,-2)
    // with no key reuse, but if checks aren't deferred the intermediate states can still
    // confuse a naive implementation.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); \
         INSERT INTO t VALUES (1, 'one'); \
         INSERT INTO t VALUES (2, 'two');",
    );

    let count = run_update(&mut db, "UPDATE t SET a = -a").expect("PK negation should succeed");
    assert_eq!(count, 2);

    let rows = get_rows(&db, "t");
    let pks: Vec<i64> = rows
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!("expected integer pk"),
        })
        .collect();
    assert_eq!(pks, vec![-1, -2]);
}

// ---------------------------------------------------------------------------
// Composite PK shifts
// ---------------------------------------------------------------------------

#[test]
fn composite_pk_shift_succeeds() {
    // Composite PK (a,b) — shift one column. Intermediate states transiently duplicate
    // (a,b) pairs across rows but the final state is unique.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE c(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b)); \
         INSERT INTO c VALUES (1, 0, 'r1'); \
         INSERT INTO c VALUES (1, 1, 'r2'); \
         INSERT INTO c VALUES (1, 2, 'r3');",
    );

    let count = run_update(&mut db, "UPDATE c SET b = b - 1").expect(
        "Composite PK shift on column b should succeed — final keys (1,-1)(1,0)(1,1) are \
         unique",
    );
    assert_eq!(count, 3);

    let rows = get_rows(&db, "c");
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => (*a, *b),
            _ => panic!("expected integer columns"),
        })
        .collect();
    assert_eq!(pairs, vec![(1, -1), (1, 0), (1, 1)]);
}

// ---------------------------------------------------------------------------
// User-defined UNIQUE index
// ---------------------------------------------------------------------------

#[test]
fn unique_index_shift_succeeds() {
    // `CREATE UNIQUE INDEX` exercises the user-defined UNIQUE index path
    // (database.list_indexes_for_table → index_data.get) which is separate from
    // the table-level UNIQUE constraint hash index path.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE u(id INTEGER PRIMARY KEY, k INTEGER); \
         CREATE UNIQUE INDEX u_k ON u(k); \
         INSERT INTO u VALUES (1, 10); \
         INSERT INTO u VALUES (2, 20); \
         INSERT INTO u VALUES (3, 30);",
    );

    let count = run_update(&mut db, "UPDATE u SET k = k - 10")
        .expect("Shift on a UNIQUE-indexed column must succeed when final state is unique");
    assert_eq!(count, 3);

    let rows = get_rows(&db, "u");
    let ks: Vec<i64> = rows
        .iter()
        .map(|r| match r[1] {
            SqlValue::Integer(n) => n,
            _ => panic!("expected integer k"),
        })
        .collect();
    assert_eq!(ks, vec![0, 10, 20]);
}

// ---------------------------------------------------------------------------
// Genuine final-state collisions still fail
// ---------------------------------------------------------------------------

#[test]
fn genuine_collision_still_fails() {
    // `UPDATE t SET a = 5` on multiple rows produces a real final-state duplicate.
    // The cross-update validator catches this case — verify it still errors after the
    // deferred-uniqueness refactor.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER UNIQUE); \
         INSERT INTO t VALUES (1, 10); \
         INSERT INTO t VALUES (2, 20);",
    );

    let result = run_update(&mut db, "UPDATE t SET a = 5 WHERE id IN (1, 2)");
    assert!(result.is_err(), "Genuine UNIQUE final-state collision must still error");
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("UNIQUE constraint failed") || msg.contains("multiple rows"),
        "Error should mention UNIQUE; got {}",
        msg
    );

    // Table state should be unchanged.
    let rows = get_rows(&db, "t");
    let avals: Vec<i64> = rows
        .iter()
        .map(|r| match r[1] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(avals, vec![10, 20]);
}

#[test]
fn collision_with_unmoved_row_still_fails() {
    // `UPDATE t SET a = 10 WHERE id = 2` lands on the existing PK of row id=1.
    // Row id=1 is NOT in the update set, so the deferred check should still detect
    // the collision against it.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); \
         INSERT INTO t VALUES (10, 'ten'); \
         INSERT INTO t VALUES (20, 'twenty');",
    );

    let result = run_update(&mut db, "UPDATE t SET a = 10 WHERE a = 20");
    assert!(
        result.is_err(),
        "UPDATE landing on an unmoved row's PK must still produce a UNIQUE error"
    );
}

#[test]
fn no_op_assignment_succeeds() {
    // `UPDATE t SET a = a` should be a no-op for PK and not trigger any UNIQUE error.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); \
         INSERT INTO t VALUES (1, 'one'); \
         INSERT INTO t VALUES (2, 'two');",
    );

    let count = run_update(&mut db, "UPDATE t SET a = a").expect("a = a should succeed");
    assert_eq!(count, 2);

    let rows = get_rows(&db, "t");
    let pks: Vec<i64> = rows
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(pks, vec![1, 2]);
}
