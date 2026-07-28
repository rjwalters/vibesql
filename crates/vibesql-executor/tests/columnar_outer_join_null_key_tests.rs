//! Regression tests for a NULL-key false-match bug in the columnar hash-join
//! outer-join probe paths (issue #6173).
//!
//! `ColumnarHashTable::build_from_i64` (and friends) indexes every build-side
//! row by its *raw* value, including rows whose join-key column is actually
//! NULL — a NULL is stored as a placeholder value (0 for integers) alongside
//! a separate null bitmap. The inner-join probe (`probe_columnar`) already
//! guarded against this by skipping any hash-table hit whose build-side row is
//! flagged NULL, but `probe_columnar_left_outer` and `probe_columnar_right_outer`
//! were missing the same guard: a NULL build-side key whose placeholder value
//! (0) happened to equal a real probe key was reported as a genuine match
//! instead of NULL-padding the unmatched row — leaking that NULL row's *other*
//! column values (e.g. a generated column's real computed value) into a join
//! result row that SQL semantics say must be entirely NULL on that side.
//!
//! `gencol1-16.40` (SQLite's own generated-column test suite) is what
//! surfaced this: `SELECT c0, c1, c2 FROM t0 LEFT JOIN t1 ON c0=c1` where
//! `t1(c1, c2 AS (c1 ISNULL))` has a row `(NULL, 1)` — with the bug, an
//! unmatched `t0` row (`c0=0`) spuriously read `c2=1` (from the NULL row)
//! instead of `c2=NULL`. These tests reproduce the same false-match with
//! plain (non-generated) columns to pin the root cause directly.

use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn execute_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            vibesql_ast::Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            other => panic!("Unsupported statement type: {:?}", other),
        }
    }
}

fn select_rows(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = vibesql_executor::SelectExecutor::new(db);
        executor.execute(&select_stmt).expect("SELECT failed")
    } else {
        panic!("Expected SELECT statement");
    }
}

/// LEFT JOIN: a build-side (right) row whose join-key is NULL must never
/// false-match a probe-side (left) row whose key equals the NULL row's
/// placeholder storage value (0 for integers).
#[test]
fn left_outer_join_null_build_key_does_not_false_match_placeholder() {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t0(c0 INTEGER)");
    execute_sql(&mut db, "CREATE TABLE t1(c1 INTEGER, tag INTEGER)");
    execute_sql(&mut db, "INSERT INTO t0 VALUES(0)");
    // Right side has a real key (1) and a NULL key, whose placeholder value
    // (0) collides with the left row's actual key.
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1, 100)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES(NULL, 200)");

    let rows = select_rows(&db, "SELECT c0, c1, tag FROM t0 LEFT JOIN t1 ON c0 = c1");
    assert_eq!(rows.len(), 1, "left row must appear exactly once (no fan-out from a false match)");
    assert_eq!(rows[0].values[0], SqlValue::Integer(0));
    assert_eq!(rows[0].values[1], SqlValue::Null, "right join-key column must be NULL-padded");
    assert_eq!(
        rows[0].values[2],
        SqlValue::Null,
        "right non-key column must be NULL-padded too, not leaked from the NULL-keyed row"
    );
}

/// RIGHT JOIN: symmetric case — a build-side (left) row whose join-key is
/// NULL must never false-match a probe-side (right) row via the placeholder.
#[test]
fn right_outer_join_null_build_key_does_not_false_match_placeholder() {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(c1 INTEGER, tag INTEGER)");
    execute_sql(&mut db, "CREATE TABLE t0(c0 INTEGER)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1, 100)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES(NULL, 200)");
    execute_sql(&mut db, "INSERT INTO t0 VALUES(0)");

    let rows = select_rows(&db, "SELECT c0, c1, tag FROM t1 RIGHT JOIN t0 ON c1 = c0");
    assert_eq!(rows.len(), 1, "right row must appear exactly once (no fan-out from a false match)");
    assert_eq!(
        rows[0].values[0],
        SqlValue::Integer(0),
        "right table's own column is always present"
    );
    assert_eq!(rows[0].values[1], SqlValue::Null, "left join-key column must be NULL-padded");
    assert_eq!(
        rows[0].values[2],
        SqlValue::Null,
        "left non-key column must be NULL-padded too, not leaked from the NULL-keyed row"
    );
}

/// Same LEFT JOIN scenario with a STRING join key (NULL placeholder is "").
#[test]
fn left_outer_join_null_build_key_does_not_false_match_empty_string_placeholder() {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t0(c0 TEXT)");
    execute_sql(&mut db, "CREATE TABLE t1(c1 TEXT, tag INTEGER)");
    // Left key is empty string, which is the placeholder used for a NULL
    // string build-side value.
    execute_sql(&mut db, "INSERT INTO t0 VALUES('')");
    execute_sql(&mut db, "INSERT INTO t1 VALUES('x', 100)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES(NULL, 200)");

    let rows = select_rows(&db, "SELECT c0, c1, tag FROM t0 LEFT JOIN t1 ON c0 = c1");
    assert_eq!(rows.len(), 1, "left row must appear exactly once (no fan-out from a false match)");
    assert_eq!(rows[0].values[0], SqlValue::Varchar("".into()));
    assert_eq!(rows[0].values[1], SqlValue::Null, "right join-key column must be NULL-padded");
    assert_eq!(
        rows[0].values[2],
        SqlValue::Null,
        "right non-key column must be NULL-padded too, not leaked from the NULL-keyed row"
    );
}
