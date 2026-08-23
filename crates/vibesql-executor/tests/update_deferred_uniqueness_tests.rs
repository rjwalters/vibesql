//! Regression tests for issue #5137: deferred UNIQUE / PRIMARY KEY checks during UPDATE
//!
//! SQLite defers UNIQUE constraint checks until statement end, so updates that shift PK
//! values within the existing key space — `UPDATE p SET a = a - 1`, swap-via-temp,
//! composite PK shifts — must succeed when the *final* state has no duplicates, even
//! if intermediate states transiently duplicate keys.
//!
//! These tests verify both:
//! 1. Statements that previously produced spurious "UNIQUE constraint failed" errors now succeed
//!    and produce the expected final state.
//! 2. Statements that produce a genuine final-state duplicate still fail, with the error reported
//!    at the end of the statement (post-statement deferred check).

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
    db.get_table(table).expect("table not found").scan().iter().map(|r| r.values.to_vec()).collect()
}

/// Returns only live (non-tombstoned) rows for a table — used by tests that
/// trigger row deletion (e.g. UPDATE OR REPLACE eviction).
fn get_live_rows(db: &Database, table: &str) -> Vec<Vec<SqlValue>> {
    db.get_table(table)
        .expect("table not found")
        .scan_live()
        .map(|(_, r)| r.values.to_vec())
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
fn ipk_shift_increment_errors_intermediate_collision() {
    // `UPDATE p SET a = a + 1` on an INTEGER PRIMARY KEY table is NOT deferred in
    // sqlite3 3.51.0: the IPK IS the rowid, which sqlite3 checks IMMEDIATELY
    // row-by-row in ascending rowid order, not at statement end. Processing rowid
    // 1 -> 2 collides with the still-live rowid 2, so sqlite3 errors:
    //   Error: UNIQUE constraint failed: p.a
    // (verified against sqlite3 3.51.0; issue #5575). This is the ASCENDING-cascade
    // half of the same +1/-1 asymmetry exercised by the rowid relocation tests in
    // src/update/tests/set_rowid.rs — the `-1` descending counterpart
    // (`pk_shift_decrement_succeeds` above) still succeeds because each old slot
    // is vacated before the next row needs it.
    //
    // NOTE: this previously asserted SUCCESS (final state 2,3,4 has no duplicate),
    // modeling the IPK like a deferred regular UNIQUE column. That diverged from
    // sqlite3, which #5575 corrects. Regular (non-rowid) UNIQUE columns remain
    // deferred — see `composite_pk_shift_succeeds` / `unique_index_shift_succeeds`.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO p VALUES (3, 'three');",
    );

    let result = run_update(&mut db, "UPDATE p SET a = a + 1");
    let err = result.expect_err(
        "IPK ascending +1 cascade must error on intermediate collision (sqlite3 3.51.0)",
    );
    assert!(
        err.to_string().contains("UNIQUE constraint failed: p.a"),
        "expected `UNIQUE constraint failed: p.a`, got: {}",
        err
    );

    // Table unchanged after the aborted UPDATE.
    let pks: Vec<i64> = get_rows(&db, "p")
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!("expected integer pk"),
        })
        .collect();
    assert_eq!(pks, vec![1, 2, 3]);
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

    let count =
        run_update(&mut db, "UPDATE p SET a = a + delta.shift FROM delta WHERE p.a = delta.id")
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

    let count = run_update(&mut db, "UPDATE t SET a = t.a * m.mult FROM m WHERE t.a = m.id")
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

    let result =
        run_update(&mut db, "UPDATE p SET a = delta.target FROM delta WHERE p.a = delta.id");
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

    let result =
        run_update(&mut db, "UPDATE p SET a = delta.target FROM delta WHERE p.a = delta.id");
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

    let count =
        run_update(&mut db, "UPDATE u SET k = u.k + delta.shift FROM delta WHERE u.id = delta.id")
            .expect(
            "UPDATE FROM shift on a UNIQUE-indexed column must succeed when final state is unique",
        );
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

    let count = run_update(&mut db, "UPDATE p SET b = delta.label FROM delta WHERE p.a = delta.id")
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

#[test]
fn update_from_composite_pk_shift_succeeds() {
    // Composite PK (a,b) shift driven by UPDATE FROM. Mirrors the default-path
    // `composite_pk_shift_succeeds` test against the FROM-clause form: a join-driven
    // shift on column `b` produces intermediate states that transiently duplicate
    // (a,b) pairs, but the final state is unique. Deferred-UNIQUE semantics must hold
    // for composite PKs through the UPDATE FROM path as well.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE c(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b)); \
         CREATE TABLE delta(id INTEGER, shift INTEGER); \
         INSERT INTO c VALUES (1, 0, 'r1'); \
         INSERT INTO c VALUES (1, 1, 'r2'); \
         INSERT INTO c VALUES (1, 2, 'r3'); \
         INSERT INTO delta VALUES (0, -1); \
         INSERT INTO delta VALUES (1, -1); \
         INSERT INTO delta VALUES (2, -1);",
    );

    let count =
        run_update(&mut db, "UPDATE c SET b = c.b + delta.shift FROM delta WHERE c.b = delta.id")
            .expect(
                "UPDATE FROM composite-PK shift on column b should succeed — final keys \
         (1,-1)(1,0)(1,1) are unique even though intermediate states duplicate.",
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
// Issue #5144: UPDATE OR IGNORE/REPLACE ... FROM must honor conflict_clause
// ---------------------------------------------------------------------------

#[test]
fn update_from_or_ignore_skips_pk_collision() {
    // From the issue body: `UPDATE OR IGNORE p SET a = delta.target FROM delta
    // WHERE p.a = delta.id` should silently skip rows whose computed update
    // would collide with an existing PK. Before #5144 this errored with
    // "UNIQUE constraint failed" because the FROM dispatch path dropped the
    // conflict_clause.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, target INTEGER); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO delta VALUES (1, 2);",
    );

    // The single update would shift row (1,'one') to PK=2, colliding with the
    // existing (2,'two'). With OR IGNORE, the colliding update is silently
    // dropped — no error and the row stays as (1,'one').
    let count = run_update(
        &mut db,
        "UPDATE OR IGNORE p SET a = delta.target FROM delta WHERE p.a = delta.id",
    )
    .expect("UPDATE OR IGNORE ... FROM must skip PK collisions, not error");
    assert_eq!(count, 0);

    let rows = get_rows(&db, "p");
    let pks: Vec<i64> = rows
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    pks.iter().for_each(|p| assert!(*p == 1 || *p == 2));
    assert_eq!(pks.len(), 2);
}

#[test]
fn update_from_or_ignore_partial_skip() {
    // Mixed batch: one update collides (should be skipped), one doesn't (should apply).
    // Verifies IGNORE works per-row across the FROM-driven update set.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, target INTEGER); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO p VALUES (3, 'three'); \
         INSERT INTO delta VALUES (1, 2); \
         INSERT INTO delta VALUES (3, 10);",
    );

    let count = run_update(
        &mut db,
        "UPDATE OR IGNORE p SET a = delta.target FROM delta WHERE p.a = delta.id",
    )
    .expect("UPDATE OR IGNORE ... FROM must skip only colliding rows");
    assert_eq!(count, 1, "exactly one (non-colliding) update should apply");

    let pks: Vec<i64> = get_rows(&db, "p")
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    let mut sorted = pks.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2, 10]);
}

#[test]
fn update_from_or_ignore_skips_unique_index_collision() {
    // OR IGNORE must also skip rows that would violate a user-defined UNIQUE index
    // (separate from the PRIMARY KEY constraint).
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(id INTEGER PRIMARY KEY, code INTEGER); \
         CREATE UNIQUE INDEX idx_p_code ON p(code); \
         CREATE TABLE delta(target_id INTEGER, new_code INTEGER); \
         INSERT INTO p VALUES (1, 100); \
         INSERT INTO p VALUES (2, 200); \
         INSERT INTO delta VALUES (1, 200);",
    );

    let count = run_update(
        &mut db,
        "UPDATE OR IGNORE p SET code = delta.new_code FROM delta WHERE p.id = delta.target_id",
    )
    .expect("UPDATE OR IGNORE ... FROM must skip UNIQUE index collisions");
    assert_eq!(count, 0);

    let codes: Vec<i64> = get_rows(&db, "p")
        .iter()
        .map(|r| match r[1] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    let mut sorted = codes.clone();
    sorted.sort();
    assert_eq!(sorted, vec![100, 200]);
}

#[test]
fn update_from_or_ignore_skips_not_null_violation() {
    // OR IGNORE must skip rows whose computed assignment would violate NOT NULL.
    // From the acceptance criteria: "UPDATE OR IGNORE ... FROM skips a NOT NULL
    // violation row but applies the rest."
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(id INTEGER PRIMARY KEY, b TEXT NOT NULL); \
         CREATE TABLE delta(target_id INTEGER, new_b TEXT); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO delta VALUES (1, NULL); \
         INSERT INTO delta VALUES (2, 'TWO');",
    );

    let count = run_update(
        &mut db,
        "UPDATE OR IGNORE p SET b = delta.new_b FROM delta WHERE p.id = delta.target_id",
    )
    .expect("UPDATE OR IGNORE ... FROM must skip NOT NULL violations");
    assert_eq!(count, 1, "only the non-NULL update applies");

    let rows = get_rows(&db, "p");
    let mut pairs: Vec<(i64, String)> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (SqlValue::Integer(i), SqlValue::Varchar(s)) => (*i, s.to_string()),
            _ => panic!("unexpected row shape: {:?}", r),
        })
        .collect();
    pairs.sort();
    assert_eq!(pairs, vec![(1, "one".to_string()), (2, "TWO".to_string())]);
}

#[test]
fn update_from_or_replace_evicts_colliding_row() {
    // OR REPLACE must delete the pre-existing row whose PK collides with the
    // updated row, then apply the update. From acceptance criteria:
    // "UPDATE OR REPLACE ... FROM evicts a colliding pre-existing row and then
    // applies the update; row count reflects the eviction."
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT); \
         CREATE TABLE delta(id INTEGER, target INTEGER); \
         INSERT INTO p VALUES (1, 'one'); \
         INSERT INTO p VALUES (2, 'two'); \
         INSERT INTO delta VALUES (1, 2);",
    );

    let count = run_update(
        &mut db,
        "UPDATE OR REPLACE p SET a = delta.target FROM delta WHERE p.a = delta.id",
    )
    .expect("UPDATE OR REPLACE ... FROM must evict colliding rows and apply update");
    assert_eq!(count, 1, "one update applied");

    // Use live-row scan to exclude tombstoned (evicted) rows.
    let rows = get_live_rows(&db, "p");
    let live: Vec<(i64, String)> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (SqlValue::Integer(i), SqlValue::Varchar(s)) => (*i, s.to_string()),
            _ => panic!("unexpected row shape: {:?}", r),
        })
        .collect();
    assert_eq!(live, vec![(2, "one".to_string())]);
}

#[test]
fn update_from_or_replace_cross_update_collision() {
    // Cross-update REPLACE collision: two updates target the same final PK.
    // The earlier loser must be deleted, the later winner survives. Mirrors
    // `resolve_cross_update_conflicts_for_replace` semantics on the default path.
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

    let count = run_update(
        &mut db,
        "UPDATE OR REPLACE p SET a = delta.target FROM delta WHERE p.a = delta.id",
    )
    .expect("UPDATE OR REPLACE ... FROM must resolve cross-update collisions");
    assert_eq!(count, 1, "only the surviving update is counted");

    // Use live-row scan to exclude tombstoned (evicted) rows.
    let live: Vec<(i64, String)> = get_live_rows(&db, "p")
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (SqlValue::Integer(i), SqlValue::Varchar(s)) => (*i, s.to_string()),
            _ => panic!("unexpected row shape: {:?}", r),
        })
        .collect();
    assert_eq!(live.len(), 1, "exactly one row survives the cross-update collision");
    assert_eq!(live[0].0, 99);
}

#[test]
fn update_from_or_ignore_all_skipped_returns_zero() {
    // Edge case: when ALL candidate rows violate constraints under OR IGNORE,
    // the statement returns 0 rows updated without error.
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE p(a INTEGER PRIMARY KEY); \
         CREATE TABLE delta(id INTEGER, target INTEGER); \
         INSERT INTO p VALUES (1); \
         INSERT INTO p VALUES (2); \
         INSERT INTO delta VALUES (1, 2); \
         INSERT INTO delta VALUES (2, 1);",
    );

    // With OR IGNORE per-row validation against original table state, both
    // updates collide and are skipped — no error.
    let count = run_update(
        &mut db,
        "UPDATE OR IGNORE p SET a = delta.target FROM delta WHERE p.a = delta.id",
    )
    .expect("UPDATE OR IGNORE ... FROM with all-skip must return 0, not error");
    assert_eq!(count, 0);

    let pks: Vec<i64> = get_rows(&db, "p")
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    let mut sorted = pks.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2]);
}
