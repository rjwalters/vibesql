//! Regression tests for a self-referencing view crashing the process with a
//! stack overflow instead of reporting SQLite's `"view <name> is circularly
//! defined"` error (found while investigating `altertab3.test`, Part of
//! #6174).
//!
//! VibeSQL resolves a view referenced in a FROM clause by *executing* its
//! query (see `select::scan::table::execute_table_scan`'s view branch), so a
//! view whose body references itself — directly, or through a chain of other
//! views — recurses into the same code path with no base case unless guarded.
//! `ViewExpansionGuard` (in that same module) detects the re-entry one level
//! before it would repeat and returns a clean SQL error instead.
//!
//! A plain, un-obscured `CREATE VIEW v AS SELECT * FROM v` is caught eagerly
//! at CREATE-time (VibeSQL derives the view's column list immediately, and
//! `v` does not exist in the catalog yet while it is being defined) with a
//! "table not found"-shaped error, so it never reaches the crashing path. A
//! `WITH` clause in the view body changes the column-derivation code path
//! enough to let the self-referencing `CREATE VIEW` succeed — matching real
//! SQLite, which defers all view-body validation to first use regardless of
//! whether a `WITH` clause is present — so the crash (and this regression
//! test) is only reachable through that shape.

use vibesql_ast::Statement;
use vibesql_executor::{ExecutorError, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn run_ddl(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateView(s) => {
            vibesql_executor::advanced_objects::execute_create_view(&s, db).unwrap();
        }
        Statement::Insert(s) => {
            vibesql_executor::InsertExecutor::execute(db, &s).unwrap();
        }
        other => panic!("unsupported DDL in test: {:?}", other),
    }
}

fn run_select(db: &Database, sql: &str) -> Result<(), ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let executor = SelectExecutor::new(db);
    executor.execute_with_columns(&select).map(|_| ())
}

/// SQLite alterqf.test / altertab3.test's minimal repro: a view whose body
/// references itself both inside an (otherwise unused) CTE and directly in
/// its own FROM clause. Querying it must return SQLite's `"view v2 is
/// circularly defined"` error rather than crashing the process.
#[test]
fn self_referencing_view_errors_instead_of_crashing() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE v0(a)");
    run_ddl(
        &mut db,
        "CREATE VIEW v2(v3) AS WITH x1 AS (SELECT * FROM v2) SELECT v3 AS x, v3 AS y FROM v2",
    );

    let err = run_select(&db, "SELECT * FROM v2")
        .expect_err("self-referencing view must error, not execute or crash");
    assert_eq!(err.to_string(), "view v2 is circularly defined");
}

/// A non-circular view that merely contains an unrelated CTE (the same
/// `WITH` shape as the circular repro above, minus the self-reference) must
/// keep executing normally — the guard must not false-positive on ordinary
/// views.
#[test]
fn view_with_unrelated_cte_is_unaffected() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t(a)");
    run_ddl(&mut db, "INSERT INTO t(a) VALUES (1)");
    run_ddl(&mut db, "CREATE VIEW v AS WITH x1 AS (SELECT 1) SELECT a FROM t");

    run_select(&db, "SELECT * FROM v").expect("non-circular view with an unrelated CTE must run");
}

/// Repeated (non-circular) queries against the same view must keep working
/// after a *different* view raised the circular-definition error — the
/// thread-local expansion guard must be popped even on the error path, or a
/// later legitimate reference to the same view name would be permanently and
/// incorrectly rejected as "still active".
#[test]
fn guard_is_released_after_circular_error_so_later_queries_still_work() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE v0(a)");
    run_ddl(
        &mut db,
        "CREATE VIEW v2(v3) AS WITH x1 AS (SELECT * FROM v2) SELECT v3 AS x, v3 AS y FROM v2",
    );

    run_select(&db, "SELECT * FROM v2").expect_err("first query must report the cycle");
    // The guard must have been popped by the failed attempt above; a second,
    // independent query against the very same circular view must report the
    // exact same clean error again, not "already active" / never return.
    let err =
        run_select(&db, "SELECT * FROM v2").expect_err("second query must also report the cycle");
    assert_eq!(err.to_string(), "view v2 is circularly defined");
}
