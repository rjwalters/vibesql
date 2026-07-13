//! End-to-end tests for SQLite tuple/row SET assignments (issue #5862).
//!
//! Covers `SET (a, b, ...) = (row-value | scalar-subquery)` in both the
//! regular UPDATE path and the ON CONFLICT ... DO UPDATE arm, including the
//! NULL-when-empty subquery semantics and the arity-mismatch error.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn setup() -> Database {
    let mut db = Database::new();
    for sql in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b, c)",
        "INSERT INTO t VALUES(1, 10, 'one')",
        "INSERT INTO t VALUES(2, 20, 'two')",
    ] {
        let stmt = Parser::parse_sql(sql).expect("parse");
        match stmt {
            vibesql_ast::Statement::CreateTable(ct) => {
                CreateTableExecutor::execute(&ct, &mut db).expect("create");
            }
            vibesql_ast::Statement::Insert(ins) => {
                InsertExecutor::execute(&mut db, &ins).expect("insert");
            }
            _ => panic!("unexpected setup statement"),
        }
    }
    db
}

fn run_update(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("parse") {
        vibesql_ast::Statement::Update(u) => UpdateExecutor::execute(&u, db).map(|_| ()),
        vibesql_ast::Statement::Insert(i) => InsertExecutor::execute(db, &i).map(|_| ()),
        _ => panic!("expected UPDATE/INSERT"),
    }
}

fn row(db: &Database, key: i64) -> Vec<SqlValue> {
    let table = db.get_table("t").expect("table");
    table
        .scan()
        .iter()
        .find(|r| r.values[0] == SqlValue::Integer(key))
        .map(|r| r.values.to_vec())
        .unwrap_or_default()
}

#[test]
fn update_tuple_set_row_value() {
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (b, c) = (99, 'x') WHERE a = 1").expect("update");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Integer(99));
    assert_eq!(r[2], SqlValue::Varchar("x".into()));
}

#[test]
fn update_tuple_set_reorders_columns() {
    // Column list order drives the mapping: `(c, b)` swaps the targets.
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (c, b) = ('z', 77) WHERE a = 2").expect("update");
    let r = row(&db, 2);
    assert_eq!(r[1], SqlValue::Integer(77));
    assert_eq!(r[2], SqlValue::Varchar("z".into()));
}

#[test]
fn update_tuple_set_subquery() {
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (b, c) = (SELECT 5, 'q') WHERE a = 1").expect("update");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Integer(5));
    assert_eq!(r[2], SqlValue::Varchar("q".into()));
}

#[test]
fn update_tuple_set_subquery_empty_yields_nulls() {
    // SQLite: a subquery that returns no rows assigns NULL to every target.
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (b, c) = (SELECT b, c FROM t WHERE a = 999) WHERE a = 1")
        .expect("update");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Null);
    assert_eq!(r[2], SqlValue::Null);
}

#[test]
fn update_tuple_set_arity_mismatch_is_error() {
    // Two target columns but a three-element row value is an arity error.
    let mut db = setup();
    let err = run_update(&mut db, "UPDATE t SET (b, c) = (1, 2, 3) WHERE a = 1");
    assert!(err.is_err(), "arity mismatch must be rejected");
}

#[test]
fn on_conflict_do_update_tuple_set() {
    // upsert4 1.x.8 shape: DO UPDATE SET (c, a) = ('four', 4).
    let mut db = setup();
    run_update(
        &mut db,
        "INSERT INTO t VALUES(2, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET (c, b) = ('four', 44)",
    )
    .expect("upsert");
    let r = row(&db, 2);
    assert_eq!(r[1], SqlValue::Integer(44));
    assert_eq!(r[2], SqlValue::Varchar("four".into()));
}

#[test]
fn on_conflict_do_update_tuple_set_subquery() {
    // upsert4 1.x.7 shape: DO UPDATE SET (b, c) = (SELECT 'x', 'y').
    let mut db = setup();
    run_update(
        &mut db,
        "INSERT INTO t VALUES(1, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET (b, c) = (SELECT 7, 'y')",
    )
    .expect("upsert");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Integer(7));
    assert_eq!(r[2], SqlValue::Varchar("y".into()));
}

// ============================================================================
// UPDATE ... FROM tuple/row SET assignments (issue #6047, root cause 2)
//
// `UPDATE t2 SET (a, b) = (...) FROM src` rewrites into a synthetic SELECT.
// A tuple assignment's RHS is a *row value*, not a scalar, in this position;
// projecting it as a single scalar column would wrongly raise
// "sub-select returns 2 columns - expected 1". Each target column must be
// projected as its own scalar in the join context.
// ============================================================================

/// Fixture for UPDATE…FROM: a target `t2(a, b)` and a source `t1(x, y, z)`.
fn setup_from() -> Database {
    let mut db = Database::new();
    for sql in [
        "CREATE TABLE t1(x, y, z)",
        "CREATE TABLE t2(a, b)",
        "INSERT INTO t1 VALUES(1000, 2000, 3000)",
        "INSERT INTO t2 VALUES(NULL, NULL)",
    ] {
        match Parser::parse_sql(sql).expect("parse") {
            vibesql_ast::Statement::CreateTable(ct) => {
                CreateTableExecutor::execute(&ct, &mut db).expect("create");
            }
            vibesql_ast::Statement::Insert(ins) => {
                InsertExecutor::execute(&mut db, &ins).expect("insert");
            }
            _ => panic!("unexpected setup statement"),
        }
    }
    db
}

fn t2_first_row(db: &Database) -> Vec<SqlValue> {
    let table = db.get_table("t2").expect("t2");
    table.scan().iter().next().map(|r| r.values.to_vec()).unwrap_or_default()
}

/// The minimized root-cause-2 reproducer: a bare (FROM-less) subquery RHS with
/// a target tuple, driven by an UPDATE…FROM. Used to fail with
/// "sub-select returns 2 columns - expected 1".
#[test]
fn update_from_tuple_set_bare_subquery() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT 1, 2) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(1));
    assert_eq!(r[1], SqlValue::Integer(2));
}

/// A literal row-value RHS (no subquery) driven by UPDATE…FROM.
#[test]
fn update_from_tuple_set_row_value() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (7, 8) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(7));
    assert_eq!(r[1], SqlValue::Integer(8));
}

/// A correlated tuple RHS that references the FROM table's columns. The subquery
/// projects `t1.x, t1.y`, so `t2` should receive `(1000, 2000)`.
#[test]
fn update_from_tuple_set_correlated_subquery() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT t1.x, t1.y) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(1000));
    assert_eq!(r[1], SqlValue::Integer(2000));
}

/// The FROM table's columns can be referenced directly (not via subquery) in the
/// tuple RHS.
#[test]
fn update_from_tuple_set_direct_column_refs() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (t1.z, t1.x) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(3000));
    assert_eq!(r[1], SqlValue::Integer(1000));
}

/// A single-column assignment under UPDATE…FROM must still work (no regression
/// to the ordinary non-tuple synthetic-SELECT path).
#[test]
fn update_from_single_column_assignment_unaffected() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET a = t1.y FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(2000));
}

// ---------------------------------------------------------------------------
// Single-evaluation semantics for a tuple *subquery* RHS (issue #6086).
//
// A splittable multi-column subquery must be evaluated ONCE per matched row and
// its row tuple distributed positionally, not re-executed per target column.
// Re-execution both diverged from SQLite for independent non-deterministic
// projections (`SET (a,b)=(SELECT random(), random())` → a=b) and wasted work.
// ---------------------------------------------------------------------------

/// Run an arbitrary DDL/DML statement, panicking on any parse/exec error. Used
/// to build the source tables the #6086 tests need beyond `setup_from`.
fn exec(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).expect("parse") {
        vibesql_ast::Statement::CreateTable(ct) => {
            CreateTableExecutor::execute(&ct, db).expect("create");
        }
        vibesql_ast::Statement::Insert(ins) => {
            InsertExecutor::execute(db, &ins).expect("insert");
        }
        vibesql_ast::Statement::Update(u) => {
            UpdateExecutor::execute(&u, db).expect("update");
        }
        other => panic!("unexpected statement: {other:?}"),
    }
}

/// `SET (a, b) = (SELECT random(), random())` must evaluate the subquery once,
/// so the two independent `random()` calls yield different values (a != b),
/// matching SQLite. Previously both narrowed subqueries collapsed through the
/// result cache and produced a == b. Probabilistic but effectively certain for
/// 64-bit random integers.
#[test]
fn update_from_tuple_set_subquery_random_is_evaluated_once() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT random(), random()) FROM t1")
        .expect("update");
    let r = t2_first_row(&db);
    assert_ne!(r[0], r[1], "independent random() projections must differ (single-eval)");
    assert_ne!(r[0], SqlValue::Null);
    assert_ne!(r[1], SqlValue::Null);
}

/// A tuple subquery over a table with `ORDER BY ... LIMIT 1` returning a 2-tuple
/// is evaluated once and both columns come from the SAME chosen row (single
/// first-row semantics), matching SQLite (`10, 20`).
#[test]
fn update_from_tuple_set_subquery_ordered_limit_single_row() {
    let mut db = setup_from();
    exec(&mut db, "CREATE TABLE src(p, q)");
    exec(&mut db, "INSERT INTO src VALUES(10, 20)");
    exec(&mut db, "INSERT INTO src VALUES(30, 40)");
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT p, q FROM src ORDER BY p LIMIT 1) FROM t1")
        .expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(10));
    assert_eq!(r[1], SqlValue::Integer(20));
}

/// A tuple subquery over a table whose WHERE correlates on a FROM-table column
/// resolves against the join row, evaluated once. `t1.x = 1000` selects the
/// matching `src` row `(30, 40)`; the subquery-local `src.k` refs are untouched.
#[test]
fn update_from_tuple_set_subquery_correlated_on_from_column() {
    let mut db = setup_from();
    exec(&mut db, "CREATE TABLE src(k, p, q)");
    exec(&mut db, "INSERT INTO src VALUES(999, 10, 20)");
    exec(&mut db, "INSERT INTO src VALUES(1000, 30, 40)");
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT p, q FROM src WHERE src.k = t1.x) FROM t1")
        .expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(30));
    assert_eq!(r[1], SqlValue::Integer(40));
}

/// An empty correlated tuple subquery yields a row of NULLs (SQLite semantics),
/// evaluated once.
#[test]
fn update_from_tuple_set_subquery_correlated_empty_yields_nulls() {
    let mut db = setup_from();
    exec(&mut db, "CREATE TABLE src(k, p, q)");
    exec(&mut db, "INSERT INTO src VALUES(1, 10, 20)");
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT p, q FROM src WHERE src.k = t1.x) FROM t1")
        .expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Null);
    assert_eq!(r[1], SqlValue::Null);
}

// ---------------------------------------------------------------------------
// Collation preservation across literal substitution (issue #6105).
//
// #6099's single-evaluation path substitutes an outer column reference into the
// (now uncorrelated) tuple subquery as a literal. That substituted value must
// carry the outer column's *implicit* declared collation so collation-sensitive
// comparisons inside the subquery match SQLite 3.51 rather than reverting to
// BINARY. The collation is IMPLICIT (like a column ref, datatype3 §7.1 rule 2 —
// left-operand precedence), NOT explicit COLLATE (rule 1 — wins anywhere).
//
// All expected values were confirmed against sqlite3 3.51.0.
// ---------------------------------------------------------------------------

/// Sets up a target `dst(id, s, n)` driven by UPDATE…FROM `o` (outer) with a
/// declared-NOCASE column `o.nc` and a plain-BINARY column `o.bc`, plus an
/// `inner_t` whose rows exercise case sensitivity. Each test issues its own
/// UPDATE and reads `dst.s` (sum) / `dst.n` (count).
fn setup_collation() -> Database {
    let mut db = Database::new();
    for sql in [
        "CREATE TABLE o(id INTEGER PRIMARY KEY, nc TEXT COLLATE NOCASE, bc TEXT)",
        "INSERT INTO o VALUES(1, 'Foo', 'Foo')",
        "CREATE TABLE inner_t(bin_col TEXT, nocase_col TEXT COLLATE NOCASE, v INTEGER)",
        "INSERT INTO inner_t VALUES('foo', 'foo', 100)",
        "INSERT INTO inner_t VALUES('FOO', 'FOO', 200)",
        "INSERT INTO inner_t VALUES('bar', 'bar', 300)",
        "CREATE TABLE dst(id INTEGER, s INTEGER, n INTEGER)",
        "INSERT INTO dst VALUES(1, -1, -1)",
    ] {
        exec(&mut db, sql);
    }
    db
}

fn dst_row(db: &Database) -> Vec<SqlValue> {
    let table = db.get_table("dst").expect("dst");
    table.scan().iter().next().map(|r| r.values.to_vec()).unwrap_or_default()
}

fn run_collation_update(where_clause: &str) -> Vec<SqlValue> {
    let mut db = setup_collation();
    let sql = format!(
        "UPDATE dst SET (s, n) = (SELECT sum(v), count(*) FROM inner_t WHERE {where_clause}) \
         FROM o WHERE dst.id = o.id"
    );
    run_update(&mut db, &sql).expect("update");
    dst_row(&db)
}

/// Left = inner BINARY column, right = substituted NOCASE outer column.
/// Rule 2: the left operand's declared BINARY collation wins → BINARY compare,
/// so 'Foo' matches neither 'foo' nor 'FOO' → no rows → sum NULL, count 0.
#[test]
fn collation_bin_lhs_vs_nocase_outer_rhs_binary() {
    let r = run_collation_update("inner_t.bin_col = o.nc");
    assert_eq!(r[1], SqlValue::Null);
    assert_eq!(r[2], SqlValue::Integer(0));
}

/// Left = substituted NOCASE outer column, right = inner BINARY column.
/// The left operand carries the outer column's implicit NOCASE → NOCASE compare,
/// so 'Foo' matches 'foo' and 'FOO' → sum 300, count 2.
#[test]
fn collation_nocase_outer_lhs_vs_bin_rhs_nocase() {
    let r = run_collation_update("o.nc = inner_t.bin_col");
    assert_eq!(r[1], SqlValue::Integer(300));
    assert_eq!(r[2], SqlValue::Integer(2));
}

/// The critical BINARY caveat: left = inner NOCASE column, right = substituted
/// BINARY outer column. The substituted BINARY outer column is still a
/// *collating operand* (a real column would be), but here the left operand's
/// NOCASE wins → NOCASE compare → sum 300, count 2.
#[test]
fn collation_nocase_col_lhs_vs_bin_outer_rhs_nocase() {
    let r = run_collation_update("inner_t.nocase_col = o.bc");
    assert_eq!(r[1], SqlValue::Integer(300));
    assert_eq!(r[2], SqlValue::Integer(2));
}

/// The critical BINARY caveat, reversed operands: left = substituted BINARY
/// outer column, right = inner NOCASE column. Because the substituted BINARY
/// column is a collating operand (NOT a bare literal), its left-operand default
/// BINARY BLOCKS fallthrough to the right's NOCASE → BINARY compare → no rows.
/// A naive plain-literal substitution would wrongly compare NOCASE here.
#[test]
fn collation_bin_outer_lhs_blocks_nocase_rhs_binary() {
    let r = run_collation_update("o.bc = inner_t.nocase_col");
    assert_eq!(r[1], SqlValue::Null);
    assert_eq!(r[2], SqlValue::Integer(0));
}

/// Explicit `COLLATE BINARY` on the substituted NOCASE outer column overrides
/// its implicit collation (rule 1) → BINARY compare → no rows.
#[test]
fn collation_explicit_binary_override() {
    let r = run_collation_update("inner_t.bin_col = o.nc COLLATE BINARY");
    assert_eq!(r[1], SqlValue::Null);
    assert_eq!(r[2], SqlValue::Integer(0));
}

/// Explicit `COLLATE NOCASE` on the substituted BINARY outer column overrides
/// its implicit BINARY (rule 1) → NOCASE compare → sum 300, count 2.
#[test]
fn collation_explicit_nocase_override() {
    let r = run_collation_update("inner_t.bin_col = o.bc COLLATE NOCASE");
    assert_eq!(r[1], SqlValue::Integer(300));
    assert_eq!(r[2], SqlValue::Integer(2));
}

/// Two implicit NOCASE operands: left inner NOCASE column, right substituted
/// NOCASE outer column → NOCASE compare → sum 300, count 2.
#[test]
fn collation_nocase_col_vs_nocase_outer() {
    let r = run_collation_update("inner_t.nocase_col = o.nc");
    assert_eq!(r[1], SqlValue::Integer(300));
    assert_eq!(r[2], SqlValue::Integer(2));
}
