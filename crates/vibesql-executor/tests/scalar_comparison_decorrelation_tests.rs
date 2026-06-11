//! Scalar-comparison decorrelation NULL semantics (issues #5274, PR #5281)
//!
//! End-to-end regression tests for `x > (SELECT <agg expr> ... WHERE correlated)`
//! empty-group semantics. The decorrelation rewrites the comparison against a
//! LEFT JOINed pre-aggregate; a missing match yields NULL. That is only
//! correct when the subquery's SELECT expression strictly propagates the
//! aggregate's empty-group NULL. NULL-absorbing idioms (COALESCE/IFNULL/CASE)
//! evaluate to a non-NULL default over an empty group and must NOT be
//! rewritten that way — they fall back to per-row correlated execution.
//!
//! All expected results below are SQLite ground truth (verified with
//! sqlite3 on the identical schema/data):
//!
//! ```sql
//! CREATE TABLE t(id INTEGER, grp INTEGER);
//! CREATE TABLE s(grp INTEGER, val INTEGER);
//! INSERT INTO t VALUES (1, 1), (2, 2);
//! INSERT INTO s VALUES (1, 5);
//! ```
//!
//! Row (2, 2) has an empty correlated group in `s`:
//! - bare `SUM(val)` threshold → NULL → row excluded
//! - `COALESCE(SUM(val), 0)` threshold → 0 → `2 > 0` → row included

use vibesql_executor::SelectExecutor;

/// Build the judge-repro schema/data for issue #5274 follow-up:
/// t(id, grp) = {(1,1), (2,2)}; s(grp, val) = {(1,5)}
fn setup_test_database() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    let t_schema = vibesql_catalog::TableSchema::new(
        "T".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "ID".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "GRP".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(t_schema).unwrap();

    let s_schema = vibesql_catalog::TableSchema::new(
        "S".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "GRP".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "VAL".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(s_schema).unwrap();

    for (id, grp) in [(1, 1), (2, 2)] {
        db.insert_row(
            "T",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(id),
                vibesql_types::SqlValue::Integer(grp),
            ]),
        )
        .unwrap();
    }
    db.insert_row(
        "S",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(5),
        ]),
    )
    .unwrap();

    db
}

/// Execute a SELECT and return the `id` values of the result rows
fn query_ids(db: &vibesql_storage::Database, sql: &str) -> Vec<i64> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("query should parse");
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };
    let executor = SelectExecutor::new(db);
    let rows = executor.execute(&select_stmt).expect("query should execute");
    rows.iter()
        .map(|row| match row.get(0) {
            Some(vibesql_types::SqlValue::Integer(v)) => *v,
            other => panic!("Expected integer id, got {:?}", other),
        })
        .collect()
}

#[test]
fn test_coalesce_sum_empty_group_compares_against_zero() {
    // SQLite: 1 row (id = 2). COALESCE(SUM(val), 0) over the empty group for
    // (2, 2) is 0, and 2 > 0 includes the row. Mis-classifying this as
    // NULL-propagating (PR #5281 first revision) returned 0 rows.
    let db = setup_test_database();
    let ids = query_ids(
        &db,
        "SELECT id FROM t WHERE id > (SELECT COALESCE(SUM(val), 0) FROM s WHERE s.grp = t.grp)",
    );
    assert_eq!(ids, vec![2], "COALESCE(SUM, 0) empty group must default to 0 (SQLite: id=2)");
}

#[test]
fn test_ifnull_sum_empty_group_compares_against_zero() {
    // SQLite: 1 row (id = 2) — same semantics as COALESCE
    let db = setup_test_database();
    let ids = query_ids(
        &db,
        "SELECT id FROM t WHERE id > (SELECT IFNULL(SUM(val), 0) FROM s WHERE s.grp = t.grp)",
    );
    assert_eq!(ids, vec![2], "IFNULL(SUM, 0) empty group must default to 0 (SQLite: id=2)");
}

#[test]
fn test_case_sum_empty_group_compares_against_zero() {
    // SQLite: 1 row (id = 2) — CASE absorbs the empty-group NULL into 0
    let db = setup_test_database();
    let ids = query_ids(
        &db,
        "SELECT id FROM t WHERE id > (SELECT CASE WHEN SUM(val) IS NULL THEN 0 ELSE SUM(val) END \
         FROM s WHERE s.grp = t.grp)",
    );
    assert_eq!(
        ids,
        vec![2],
        "CASE WHEN SUM IS NULL THEN 0 empty group must default to 0 (SQLite: id=2)"
    );
}

#[test]
fn test_coalesce_around_arithmetic_sum_empty_group_compares_against_zero() {
    // SQLite: 1 row (id = 2). The absorbing COALESCE wraps an arithmetic
    // composition — still 0 over the empty group.
    let db = setup_test_database();
    let ids = query_ids(
        &db,
        "SELECT id FROM t WHERE id > (SELECT COALESCE(0.5 * SUM(val), 0) FROM s \
         WHERE s.grp = t.grp)",
    );
    assert_eq!(ids, vec![2], "COALESCE(0.5 * SUM, 0) empty group must default to 0 (SQLite: id=2)");
}

#[test]
fn test_bare_sum_empty_group_excludes_row() {
    // SQLite: 0 rows. Bare SUM over the empty group for (2, 2) is NULL, so
    // `2 > NULL` excludes the row; (1, 1) has threshold 5 and 1 > 5 is false.
    // This is the Q20 shape (issue #5274) and must stay NULL-propagating.
    let db = setup_test_database();
    let ids =
        query_ids(&db, "SELECT id FROM t WHERE id > (SELECT SUM(val) FROM s WHERE s.grp = t.grp)");
    assert_eq!(ids, Vec::<i64>::new(), "bare SUM empty group must yield NULL (SQLite: 0 rows)");
}

#[test]
fn test_arithmetic_sum_empty_group_excludes_row() {
    // SQLite: 0 rows. NULL propagates through `0.5 * SUM(val)` (exact TPC-H
    // Q20 shape), so the empty group still excludes the row.
    let db = setup_test_database();
    let ids = query_ids(
        &db,
        "SELECT id FROM t WHERE id > (SELECT 0.5 * SUM(val) FROM s WHERE s.grp = t.grp)",
    );
    assert_eq!(
        ids,
        Vec::<i64>::new(),
        "0.5 * SUM empty group must propagate NULL (SQLite: 0 rows)"
    );
}
