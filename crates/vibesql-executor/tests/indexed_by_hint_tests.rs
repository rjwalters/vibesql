//! Tests for SQLite `INDEXED BY <name>` / `NOT INDEXED` hint enforcement
//! (issue #6405).
//!
//! Before this fix, VibeSQL's planner chose indexes independently of any
//! `INDEXED BY` hint on the FROM clause — the hint was validated (SQLite's
//! `no such index: <name>` error) but otherwise ignored, so `SELECT ... FROM t
//! INDEXED BY idx` with no WHERE clause fell back to a bare table scan and
//! returned rows in insertion order rather than the index's key order
//! (SQLite storage-class order: NULL < numeric < TEXT < BLOB).
//!
//! `NOT INDEXED` is intentionally left as a no-op (unchanged, pre-existing
//! behavior) — see the module doc on
//! `select/executor/validation/index_hints.rs`.

use vibesql_ast::Statement;
use vibesql_executor::{ExplainExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn run(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateIndex(s) => {
            vibesql_executor::CreateIndexExecutor::execute(&s, db).unwrap();
        }
        Statement::Insert(i) => {
            vibesql_executor::InsertExecutor::execute(db, &i).unwrap();
        }
        other => panic!("unsupported setup statement: {other:?}"),
    }
}

/// Run a SELECT and return the single-column result, in the order the
/// executor emits rows (no in-test sorting — that's the whole point of these
/// tests).
fn query_strings(db: &Database, sql: &str) -> Vec<String> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    let Statement::Select(select) = stmt else { panic!("not a SELECT: {sql}") };
    SelectExecutor::new(db)
        .execute(&select)
        .unwrap_or_else(|e| panic!("query failed {sql}: {e:?}"))
        .into_iter()
        .map(|row| match &row.values[0] {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
            other => panic!("expected string column, got {other:?}"),
        })
        .collect()
}

fn query_ints(db: &Database, sql: &str) -> Vec<i64> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    let Statement::Select(select) = stmt else { panic!("not a SELECT: {sql}") };
    SelectExecutor::new(db)
        .execute(&select)
        .unwrap_or_else(|e| panic!("query failed {sql}: {e:?}"))
        .into_iter()
        .map(|row| match &row.values[0] {
            SqlValue::Integer(n) => *n,
            SqlValue::Bigint(n) => *n,
            other => panic!("expected integer column, got {other:?}"),
        })
        .collect()
}

/// Run EXPLAIN QUERY PLAN and return the SQLite-style EQP output.
fn eqp(db: &Database, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
    let stmt = Parser::parse_sql(&explain_sql).expect("failed to parse EXPLAIN");
    let Statement::Explain(explain_stmt) = stmt else { panic!("expected EXPLAIN statement") };
    ExplainExecutor::execute(&explain_stmt, db).expect("EXPLAIN failed").to_sqlite_eqp()
}

/// Repro from the issue: an index on a column holding mixed SQLite storage
/// classes (NULL, INTEGER, TEXT, REAL) must be scanned in storage-class order
/// (NULL < numeric < TEXT) when forced via `INDEXED BY` with no WHERE clause,
/// not in insertion order.
#[test]
fn indexed_by_forces_storage_class_order_no_where_clause() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE t2(a)");
    run(&mut db, "CREATE INDEX x3 ON t2(a)");
    // Insertion order deliberately does NOT match storage-class order.
    run(&mut db, "INSERT INTO t2(a) VALUES(NULL)");
    run(&mut db, "INSERT INTO t2(a) VALUES(1)");
    run(&mut db, "INSERT INTO t2(a) VALUES('xyz')");
    run(&mut db, "INSERT INTO t2(a) VALUES(2)");
    run(&mut db, "INSERT INTO t2(a) VALUES(3.5)");

    let forced = query_strings(&db, "SELECT quote(a) FROM t2 INDEXED BY x3");
    assert_eq!(
        forced,
        vec!["NULL", "1", "2", "3.5", "'xyz'"],
        "INDEXED BY must return rows in the index's storage-class key order"
    );

    // EQP must show the named index actually being used, not a bare `SCAN t2`.
    let plan = eqp(&db, "SELECT quote(a) FROM t2 INDEXED BY x3");
    assert!(
        plan.contains("USING") && plan.contains("INDEX x3"),
        "expected EQP to show index x3 in use, got: {plan}"
    );
    assert!(!plan.trim_end().ends_with("SCAN t2"), "expected more than a bare SCAN t2: {plan}");
}

/// `NOT INDEXED` must remain a no-op: results are unaffected by this fix and
/// stay identical to a bare (hint-free) scan — physical/insertion order, not
/// storage-class order. This locks in the issue's explicit "edge case" that
/// `NOT INDEXED` behavior does not change.
#[test]
fn not_indexed_remains_a_noop() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE t2(a)");
    run(&mut db, "CREATE INDEX x3 ON t2(a)");
    run(&mut db, "INSERT INTO t2(a) VALUES(NULL)");
    run(&mut db, "INSERT INTO t2(a) VALUES(1)");
    run(&mut db, "INSERT INTO t2(a) VALUES('xyz')");
    run(&mut db, "INSERT INTO t2(a) VALUES(2)");
    run(&mut db, "INSERT INTO t2(a) VALUES(3.5)");

    let not_indexed = query_strings(&db, "SELECT quote(a) FROM t2 NOT INDEXED");
    let bare = query_strings(&db, "SELECT quote(a) FROM t2");
    assert_eq!(not_indexed, bare, "NOT INDEXED must not change scan order (remains a no-op)");
    // Insertion order, not storage-class order — confirms NOT INDEXED didn't
    // accidentally start forcing index use either.
    assert_eq!(not_indexed, vec!["NULL", "1", "'xyz'", "2", "3.5"]);
}

/// `INDEXED BY` must force use of the *named* index even when a WHERE clause
/// would otherwise lead the cost model to pick a different index — SQLite's
/// documented precedence for the hint.
#[test]
fn indexed_by_forces_named_index_over_cost_model_choice() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE t(a INTEGER, b INTEGER)");
    run(&mut db, "CREATE INDEX ia ON t(a)");
    run(&mut db, "CREATE INDEX ib ON t(b)");
    run(&mut db, "INSERT INTO t VALUES (1,5)");
    run(&mut db, "INSERT INTO t VALUES (2,5)");
    run(&mut db, "INSERT INTO t VALUES (3,5)");
    run(&mut db, "INSERT INTO t VALUES (4,9)");

    // Without a hint, the cost model picks `ib` (the only index that can
    // filter on `b`).
    let plan_unhinted = eqp(&db, "SELECT a FROM t WHERE b = 5");
    assert!(plan_unhinted.contains("INDEX ib"), "expected ib to be chosen: {plan_unhinted}");

    // With `INDEXED BY ia`, the *named* index must be used instead, even
    // though the WHERE clause filters on `b`, not `a`.
    let plan_hinted = eqp(&db, "SELECT a FROM t INDEXED BY ia WHERE b = 5");
    assert!(plan_hinted.contains("INDEX ia"), "expected ia to be forced: {plan_hinted}");
    assert!(!plan_hinted.contains("INDEX ib"), "ib must not be used when ia is forced");

    // Results must still be correct — the WHERE clause is applied as a
    // post-filter over the forced index's full scan.
    let mut rows = query_ints(&db, "SELECT a FROM t INDEXED BY ia WHERE b = 5");
    rows.sort_unstable();
    assert_eq!(rows, vec![1, 2, 3]);
}

/// An index on a single-storage-class column (already correctly ordered
/// before this fix, since numeric-only BTreeMap key order matches insertion
/// order for ascending inserts) must remain unaffected: same rows, same
/// (index key) order, forced or not.
#[test]
fn indexed_by_single_storage_class_column_unaffected() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE t3(a INTEGER)");
    run(&mut db, "CREATE INDEX ix ON t3(a)");
    // Insertion order is deliberately NOT sorted, so a correct index-order
    // scan is distinguishable from an (accidental) insertion-order scan.
    run(&mut db, "INSERT INTO t3 VALUES (5)");
    run(&mut db, "INSERT INTO t3 VALUES (3)");
    run(&mut db, "INSERT INTO t3 VALUES (1)");
    run(&mut db, "INSERT INTO t3 VALUES (4)");
    run(&mut db, "INSERT INTO t3 VALUES (2)");

    let forced = query_ints(&db, "SELECT a FROM t3 INDEXED BY ix");
    assert_eq!(forced, vec![1, 2, 3, 4, 5], "single-column INTEGER index must scan in key order");
}

/// `INDEXED BY` naming a nonexistent index is a validation error (issue
/// #5235 / SQLite parity), unaffected by this fix.
#[test]
fn indexed_by_nonexistent_index_errors() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE t3(a INTEGER)");
    run(&mut db, "CREATE INDEX ix ON t3(a)");
    run(&mut db, "INSERT INTO t3 VALUES (1)");

    let stmt = Parser::parse_sql("SELECT a FROM t3 INDEXED BY nope").unwrap();
    let Statement::Select(select) = stmt else { panic!("not a select") };
    let err = SelectExecutor::new(&db).execute(&select).unwrap_err();
    assert!(
        err.to_string().contains("no such index"),
        "expected `no such index` error, got: {err}"
    );
}

/// `INDEXED BY` naming a *partial* index whose predicate the query WHERE
/// clause does not imply must error with SQLite's `no query solution`
/// (`indexedby.test` 12.2/12.4), not silently scan the index and drop rows
/// outside its predicate. A partial index only contains rows matching its
/// own `WHERE` clause, so forcing one the query cannot prove covers every row
/// it needs would otherwise return incomplete results without any error.
#[test]
fn indexed_by_partial_index_not_implied_errors() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE o1(x INTEGER PRIMARY KEY, y, z)");
    run(&mut db, "CREATE INDEX p1 ON o1(z)");
    run(&mut db, "CREATE INDEX p2 ON o1(y) WHERE z=1");

    // No WHERE clause at all -> p2 cannot be proven to cover every row.
    let stmt = Parser::parse_sql("SELECT * FROM o1 INDEXED BY p2 ORDER BY 1").unwrap();
    let Statement::Select(select) = stmt else { panic!("not a select") };
    let err = SelectExecutor::new(&db).execute(&select).unwrap_err();
    assert_eq!(err.to_string(), "no query solution", "err was: {err}");

    // EXPLAIN QUERY PLAN must raise the same prepare-time error, not silently
    // render a plan for an index it cannot actually use.
    let explain_sql = "EXPLAIN QUERY PLAN SELECT * FROM o1 INDEXED BY p2 ORDER BY 1";
    let stmt = Parser::parse_sql(explain_sql).expect("failed to parse EXPLAIN");
    let Statement::Explain(explain_stmt) = stmt else { panic!("expected EXPLAIN statement") };
    let err = ExplainExecutor::execute(&explain_stmt, &db).unwrap_err();
    assert_eq!(err.to_string(), "no query solution", "err was: {err}");
}

/// The converse of the above: when the query WHERE clause *does* structurally
/// imply the partial index's predicate, `INDEXED BY` must succeed and
/// actually use the named partial index (not error, not silently fall back).
#[test]
fn indexed_by_partial_index_implied_succeeds() {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE o1(x INTEGER PRIMARY KEY, y, z)");
    run(&mut db, "CREATE INDEX p1 ON o1(z)");
    run(&mut db, "CREATE INDEX p2 ON o1(y) WHERE z=1");
    run(&mut db, "INSERT INTO o1 VALUES (1, 10, 1)");
    run(&mut db, "INSERT INTO o1 VALUES (2, 20, 2)");

    let rows = query_ints(&db, "SELECT x FROM o1 INDEXED BY p2 WHERE z=1");
    assert_eq!(rows, vec![1]);

    let plan = eqp(&db, "SELECT x FROM o1 INDEXED BY p2 WHERE z=1");
    assert!(plan.contains("INDEX p2"), "expected p2 to be used: {plan}");
}
