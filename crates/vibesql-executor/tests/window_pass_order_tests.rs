//! Regression tests for issue #5291 (window9.test 1.4, windowpushd.test 2.x.4.1)
//!
//! SQLite rewrites multi-window queries into nested sorting passes; when there
//! is no statement-level ORDER BY, output rows are left in the order produced
//! by the *last* window's sort pass. Plain GROUP BY output (without ORDER BY)
//! is emitted in group-key order.
//!
//! Covers three fixes:
//!
//! 1. `select/window/mod.rs`: capture row reordering from the *last* window function with PARTITION
//!    BY/ORDER BY, not the first (window9 1.4).
//! 2. `select/executor/aggregation/window.rs`: the window-over-GROUP-BY path reorders output rows
//!    into the last window pass's partition order (windowpushd 2.x.4.1, windowed instance).
//! 3. `select/columnar/aggregate/group_by.rs`: columnar GROUP BY output is sorted by group key
//!    instead of AHashMap iteration order (windowpushd 2.x.4.1, plain instance — previously
//!    nondeterministic per process).

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

/// Run a SELECT and return each row as a vector of display strings
/// (SQLite-style formatting), making assertions independent of the exact
/// numeric SqlValue variant.
fn query_strings(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<String>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e))
            .into_iter()
            .map(|row| row.values.iter().map(|v: &SqlValue| v.to_string()).collect())
            .collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn rows(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected.iter().map(|r| r.iter().map(|s| s.to_string()).collect()).collect()
}

/// window9.test 1.0/1.1 fixture
fn setup_fruits_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE fruits(name TEXT COLLATE NOCASE, color TEXT COLLATE NOCASE)");
    run_stmt(&mut db, "INSERT INTO fruits (name, color) VALUES ('apple', 'RED')");
    run_stmt(&mut db, "INSERT INTO fruits (name, color) VALUES ('APPLE', 'yellow')");
    run_stmt(&mut db, "INSERT INTO fruits (name, color) VALUES ('pear', 'YELLOW')");
    run_stmt(&mut db, "INSERT INTO fruits (name, color) VALUES ('PEAR', 'green')");
    db
}

/// windowpushd.test 2.0 t2 fixture
fn setup_t2_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t2(x, y, z)");
    run_stmt(
        &mut db,
        "INSERT INTO t2 VALUES('W', 3, 1), ('W', 2, 2), ('X', 1, 4), ('X', 5, 7), \
         ('Y', 1, 9), ('Y', 4, 2), ('Z', 3, 3), ('Z', 3, 4)",
    );
    db
}

#[test]
fn test_multi_window_rows_left_in_last_pass_order() {
    // window9.test 1.4: with two windows and no statement-level ORDER BY,
    // rows come out in the LAST window's sort pass order
    // (PARTITION BY name ORDER BY color, both NOCASE).
    let db = setup_fruits_db();
    let result = query_strings(
        &db,
        "SELECT name, color, \
           dense_rank() OVER (ORDER BY name), \
           dense_rank() OVER (PARTITION BY name ORDER BY color) \
         FROM fruits",
    );
    assert_eq!(
        result,
        rows(&[
            &["apple", "RED", "1", "1"],
            &["APPLE", "yellow", "1", "2"],
            &["PEAR", "green", "2", "1"],
            &["pear", "YELLOW", "2", "2"],
        ])
    );
}

#[test]
fn test_statement_order_by_overrides_window_pass_order() {
    // window9.test 1.5: an explicit statement-level ORDER BY still wins.
    let db = setup_fruits_db();
    let result = query_strings(
        &db,
        "SELECT name, color, \
           dense_rank() OVER (ORDER BY name), \
           dense_rank() OVER (PARTITION BY name ORDER BY color) \
         FROM fruits ORDER BY color",
    );
    assert_eq!(
        result,
        rows(&[
            &["PEAR", "green", "2", "1"],
            &["apple", "RED", "1", "1"],
            &["APPLE", "yellow", "1", "2"],
            &["pear", "YELLOW", "2", "2"],
        ])
    );
}

#[test]
fn test_single_window_order_unchanged() {
    // window9.test 1.3: single-window behavior is unchanged by the
    // last-pass capture (it is both the first and last pass).
    let db = setup_fruits_db();
    let result = query_strings(
        &db,
        "SELECT name, color, dense_rank() OVER (PARTITION BY name ORDER BY color) FROM fruits",
    );
    assert_eq!(
        result,
        rows(&[
            &["apple", "RED", "1"],
            &["APPLE", "yellow", "2"],
            &["PEAR", "green", "1"],
            &["pear", "YELLOW", "2"],
        ])
    );
}

#[test]
fn test_plain_group_by_emits_group_key_order() {
    // windowpushd.test 2.x.4.1 (plain instance): GROUP BY output without
    // ORDER BY comes out sorted by the group key. Run several times to catch
    // hash-iteration-order nondeterminism (AHashMap seeds vary per instance).
    let db = setup_t2_db();
    let expected = rows(&[&["W", "5", "2"], &["X", "6", "7"], &["Y", "5", "9"], &["Z", "6", "4"]]);
    for run in 0..5 {
        let result = query_strings(
            &db,
            "SELECT * FROM (SELECT x, sum(y) AS s, max(z) AS m FROM t2 GROUP BY x)",
        );
        assert_eq!(result, expected, "run {} produced wrong order", run);
    }
}

#[test]
fn test_window_over_group_by_rows_left_in_window_pass_order() {
    // windowpushd.test 2.x.4.1 (windowed instance): the window pass over the
    // GROUP BY result leaves rows in partition order (PARTITION BY sum(y):
    // 5 -> [W, Y], 6 -> [X, Z]).
    let db = setup_t2_db();
    let expected = rows(&[
        &["W", "5", "2", "9"],
        &["Y", "5", "9", "9"],
        &["X", "6", "7", "7"],
        &["Z", "6", "4", "7"],
    ]);
    for run in 0..5 {
        let result = query_strings(
            &db,
            "SELECT * FROM (\
               SELECT x, sum(y) AS s, max(z) AS m, \
                 max( max(z) ) OVER (PARTITION BY sum(y) \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
               FROM t2 GROUP BY x)",
        );
        assert_eq!(result, expected, "run {} produced wrong order", run);
    }
}

#[test]
fn test_window_over_group_by_with_order_by_overrides() {
    // windowpushd.test 2.x.4.2-style: statement-level ORDER BY overrides the
    // window pass order on the aggregation path too.
    let db = setup_t2_db();
    let result = query_strings(
        &db,
        "SELECT x, sum(y) AS s, max(z) AS m, \
           max( max(z) ) OVER (PARTITION BY sum(y) \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
         FROM t2 GROUP BY x ORDER BY x",
    );
    assert_eq!(
        result,
        rows(&[
            &["W", "5", "2", "9"],
            &["X", "6", "7", "7"],
            &["Y", "5", "9", "9"],
            &["Z", "6", "4", "7"],
        ])
    );
}
