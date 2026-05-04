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

// ---------------------------------------------------------------------------
// Issue #5140: UPDATE FROM (multi-table UPDATE) — same deferred-UNIQUE bug class
// ---------------------------------------------------------------------------
//
// PR #5138 fixed the default UPDATE path for #5137 by deferring PK / UNIQUE
// checks to a post-statement pass. The judge on that PR flagged that the
// UPDATE FROM path (`execute_update_from`) still ran per-row UNIQUE checks
// inside the apply loop and would exhibit the same bug class. These tests
// mirror the default-path tests above against the FROM-clause form.

#[test]
fn update_from_pk_shift_succeeds() {
    // The exact reproducer from the issue body: a PK shift driven by a join
    // against a delta table. Without deferred UNIQUE checks, the second row's
    // new PK collides with the first row's not-yet-shifted PK.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, shift INTEGER); \
         INSERT INTO p VALUES (0, 'zero'); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO delta VALUES (0, -1); \
         INSERT INTO delta VALUES (1, -1);",
    );

    let count = run_update(
        &mut db,
        "UPDATE p SET a = a + delta.shift FROM delta WHERE p.a = delta.id",
    )
    .expect(
        "UPDATE FROM with PK shift driven by joined delta should succeed; intermediate \
         UNIQUE collisions must be deferred until statement end (issue #5140)",
    );
    assert_eq!(count, 2);

    let rows = get_rows(&db, "p");
    assert_eq!(rows.len(), 2);
    // Storage order matches insertion order: row 0 (a=0) -> -1, row 1 (a=1) -> 0
    assert_eq!(rows[0][0], SqlValue::Integer(-1));
    assert_eq!(rows[0][1], SqlValue::Varchar("zero".into()));
    assert_eq!(rows[1][0], SqlValue::Integer(0));
    assert_eq!(rows[1][1], SqlValue::Varchar("one".into()));
}

#[test]
fn update_from_pk_negation_succeeds() {
    // Mirror of `pk_negation_succeeds` (default path) using UPDATE FROM. The delta
    // table here just supplies a literal -1 multiplier per row, but the key shape
    // is the same: rows transition through transiently-overlapping PK values
    // before settling in their final unique state.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE m(id INTEGER, mult INTEGER); \
         INSERT INTO t VALUES (1, 'one'); \
         INSERT INTO t VALUES (2, 'two'); \
         INSERT INTO m VALUES (1, -1); \
         INSERT INTO m VALUES (2, -1);",
    );

    let count = run_update(
        &mut db,
        "UPDATE t SET a = t.a * m.mult FROM m WHERE t.a = m.id",
    )
    .expect("UPDATE FROM with PK negation should succeed");
    assert_eq!(count, 2);

    let pks: Vec<i64> = get_rows(&db, "t")
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!("expected integer pk"),
        })
        .collect();
    assert_eq!(pks, vec![-1, -2]);
}

#[test]
fn update_from_genuine_collision_still_fails() {
    // Two delta rows steer two distinct source rows to the same final PK value.
    // The cross-update validator must still catch this even though no per-row
    // check fires.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, target INTEGER); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO delta VALUES (1, 99); \
         INSERT INTO delta VALUES (2, 99);",
    );

    let result = run_update(
        &mut db,
        "UPDATE p SET a = delta.target FROM delta WHERE p.a = delta.id",
    );
    assert!(
        result.is_err(),
        "Genuine UNIQUE final-state collision via UPDATE FROM must still error"
    );
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("UNIQUE constraint failed") || msg.contains("multiple rows"),
        "Error should mention UNIQUE; got {}",
        msg
    );

    // Table state should be unchanged (both rows still have their original PKs).
    let pks: Vec<i64> = get_rows(&db, "p")
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(pks, vec![1, 2]);
}

#[test]
fn update_from_collision_with_unmoved_row_still_fails() {
    // Only one row in `p` is updated (a=2 -> a=10); the row at a=10 is NOT in
    // the update set, so the deferred check must still flag the collision
    // against it.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, target INTEGER); \
         INSERT INTO p VALUES (10, 'ten'); \
         INSERT INTO p VALUES (20, 'twenty'); \
         INSERT INTO delta VALUES (20, 10);",
    );

    let result = run_update(
        &mut db,
        "UPDATE p SET a = delta.target FROM delta WHERE p.a = delta.id",
    );
    assert!(
        result.is_err(),
        "UPDATE FROM landing on an unmoved row's PK must still produce a UNIQUE error"
    );
}

#[test]
fn update_from_unique_index_shift_succeeds() {
    // Exercise the user-defined UNIQUE index branch of validate_post_statement_uniqueness
    // through UPDATE FROM. Without the deferred semantics the shift would falsely error
    // on the index_data lookup.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE u(id INTEGER PRIMARY KEY, k INTEGER); \
         CREATE UNIQUE INDEX u_k ON u(k); \
         CREATE TABLE delta(id INTEGER, shift INTEGER); \
         INSERT INTO u VALUES (1, 10); \
         INSERT INTO u VALUES (2, 20); \
         INSERT INTO u VALUES (3, 30); \
         INSERT INTO delta VALUES (1, -10); \
         INSERT INTO delta VALUES (2, -10); \
         INSERT INTO delta VALUES (3, -10);",
    );

    let count = run_update(
        &mut db,
        "UPDATE u SET k = u.k + delta.shift FROM delta WHERE u.id = delta.id",
    )
    .expect("UPDATE FROM shift on a UNIQUE-indexed column must succeed when final state is unique");
    assert_eq!(count, 3);

    let ks: Vec<i64> = get_rows(&db, "u")
        .iter()
        .map(|r| match r[1] {
            SqlValue::Integer(n) => n,
            _ => panic!("expected integer k"),
        })
        .collect();
    assert_eq!(ks, vec![0, 10, 20]);
}

#[test]
fn update_from_no_match_succeeds_unchanged() {
    // Regression guard: when the FROM-clause join produces zero matches, no rows
    // should be touched and no UNIQUE errors should fire. Previously this path
    // already worked, but wrapping it confirms the deferred check correctly
    // short-circuits on an empty updates list.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, shift INTEGER); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two');",
    );

    let count =
        run_update(&mut db, "UPDATE p SET a = a + delta.shift FROM delta WHERE p.a = delta.id")
            .expect("UPDATE FROM with empty join result should succeed with 0 affected rows");
    assert_eq!(count, 0);

    let pks: Vec<i64> = get_rows(&db, "p")
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(pks, vec![1, 2]);
}

#[test]
fn update_from_non_pk_change_still_works() {
    // Regression guard: UPDATE FROM that touches only a non-PK column must continue
    // to succeed (deferred-UNIQUE refactor must not regress the common case).
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, label TEXT); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO delta VALUES (1, 'uno'); \
         INSERT INTO delta VALUES (2, 'dos');",
    );

    let count = run_update(
        &mut db,
        "UPDATE p SET b = delta.label FROM delta WHERE p.a = delta.id",
    )
    .expect("UPDATE FROM on non-PK column should succeed");
    assert_eq!(count, 2);

    let labels: Vec<String> = get_rows(&db, "p")
        .iter()
        .map(|r| match &r[1] {
            SqlValue::Varchar(s) => s.to_string(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(labels, vec!["uno", "dos"]);
}
