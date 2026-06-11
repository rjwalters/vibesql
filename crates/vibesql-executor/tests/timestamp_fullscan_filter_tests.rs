//! Regression tests for full-scan WHERE filtering on temporal columns
//! (issue #5335).
//!
//! The columnar full-scan comparators (`select/columnar/filter/comparison.rs`
//! and the SIMD kernels) previously had **no match arms for Timestamp/Time at
//! all** — both Timestamp-vs-string and Timestamp-vs-Timestamp comparisons
//! fell through to a catch-all that returned `Ordering::Equal`, turning `=`,
//! `<=`, `>=`, and BETWEEN into tautologies (all rows returned) and `<`, `>`,
//! `!=` into contradictions (0 rows). Because `execute_table_scan` marks the
//! WHERE clause as consumed after columnar filtering, the (correct)
//! expression evaluator never re-checked the rows.
//!
//! These tests assert the #5329 semantics on full scans (no index):
//! - Timestamp/Time vs string: compare TEXT renderings lexicographically
//! - Timestamp vs Timestamp: ordinary temporal comparison
//! - Date vs parseable string: parse-first; unparseable string: type-mismatch
//!   error (same as the expression evaluator)
//!
//! Every WHERE result is also cross-checked against a per-row projection of
//! the same predicate (`SELECT id, <pred> FROM t`), which exercises the
//! expression evaluator — the canonical semantics. The matrix runs at two
//! table sizes: 2 rows (scalar columnar path, below SIMD_COLUMNAR_THRESHOLD
//! = 500) and 600 rows (SIMD / cached-columnar path) so the paths must agree
//! across the threshold.

use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute one or more non-SELECT SQL statements separated by ';'.
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

/// Execute a SELECT and return the resulting rows.
fn select_rows(db: &Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = vibesql_executor::SelectExecutor::new(db);
        executor.execute(&select_stmt).map_err(|e| e.to_string())
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Execute a SELECT and return the first column of each row as sorted i64s.
fn select_ints(db: &Database, sql: &str) -> Result<Vec<i64>, String> {
    let rows = select_rows(db, sql)?;
    let mut out: Vec<i64> = rows
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Integer(i) => *i,
            other => panic!("expected integer, got {:?}", other),
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// True if a projected predicate value is SQL true.
fn is_sql_true(v: &SqlValue) -> bool {
    match v {
        SqlValue::Boolean(b) => *b,
        SqlValue::Integer(i) => *i != 0,
        SqlValue::Null => false,
        other => panic!("expected boolean-ish predicate value, got {:?}", other),
    }
}

/// Assert that `WHERE <pred>` returns exactly the rows for which the per-row
/// projection `SELECT id, <pred>` (expression evaluator, canonical #5329
/// semantics) is true. Returns the WHERE result so callers can also pin
/// literal expected values.
fn assert_where_matches_projection(db: &Database, table: &str, pred: &str) -> Vec<i64> {
    let where_sql = format!("SELECT id FROM {table} WHERE {pred}");
    let where_ids = select_ints(db, &where_sql)
        .unwrap_or_else(|e| panic!("WHERE query failed: {e}\n  sql: {where_sql}"));

    let proj_sql = format!("SELECT id, {pred} FROM {table}");
    let proj_rows = select_rows(db, &proj_sql)
        .unwrap_or_else(|e| panic!("projection query failed: {e}\n  sql: {proj_sql}"));
    let mut proj_ids: Vec<i64> = proj_rows
        .iter()
        .filter(|row| is_sql_true(&row.values[1]))
        .map(|row| match &row.values[0] {
            SqlValue::Integer(i) => *i,
            other => panic!("expected integer id, got {:?}", other),
        })
        .collect();
    proj_ids.sort_unstable();

    assert_eq!(
        where_ids, proj_ids,
        "WHERE filtering diverged from per-row projection for predicate: {pred}\n  WHERE:      {where_ids:?}\n  projection: {proj_ids:?}"
    );
    where_ids
}

/// The full operator matrix from the issue's verified failure table.
/// Each entry: (predicate, expected ids for the 2-row table).
fn small_table_matrix() -> Vec<(&'static str, Vec<i64>)> {
    vec![
        // Timestamp vs unparseable string (TEXT-rendering semantics)
        ("ts = 'zzz'", vec![]),
        ("ts != 'zzz'", vec![1, 2]),
        ("ts < 'zzz'", vec![1, 2]),
        ("ts <= 'zzz'", vec![1, 2]),
        ("ts > 'zzz'", vec![]),
        ("ts >= 'zzz'", vec![]),
        // Timestamp vs canonical full rendering
        ("ts = '2017-07-20 15:30:00'", vec![1]),
        ("ts != '2017-07-20 15:30:00'", vec![2]),
        ("ts <= '2017-07-20 15:30:00'", vec![1]),
        ("ts > '2017-07-20 15:30:00'", vec![2]),
        // Timestamp vs date-only string (lexicographic prefix semantics)
        ("ts >= '2017-07-21'", vec![2]),
        ("ts < '2017-07-21'", vec![1]),
        ("ts BETWEEN '2017-07-19' AND '2017-07-21'", vec![1]),
        // BETWEEN with mixed parseable/unparseable bounds
        ("ts BETWEEN '2017-07-19' AND 'zzz'", vec![1, 2]),
        // Numeric-looking string: the expression evaluator's NUMERIC-affinity
        // rules coerce '1999' to a number first, and temporal-vs-numeric is
        // always false (evaluator/operators/comparison/mod.rs). The WHERE
        // path must match the evaluator - in particular it must NOT compare
        // raw microseconds against 1999.0 (the old SIMD i64 kernel did).
        ("ts < '1999'", vec![]),
        ("ts > '1999'", vec![]),
        ("ts = '1999'", vec![]),
        // Timestamp literals (new defect: tautology/contradiction on full scan)
        ("ts = TIMESTAMP '2017-07-20 15:30:00'", vec![1]),
        ("ts != TIMESTAMP '2017-07-20 15:30:00'", vec![2]),
        ("ts < TIMESTAMP '2017-07-22 08:00:00'", vec![1]),
        ("ts <= TIMESTAMP '2017-07-22 08:00:00'", vec![1, 2]),
        ("ts > TIMESTAMP '2017-07-20 15:30:00'", vec![2]),
        ("ts >= TIMESTAMP '2017-07-22 08:00:00'", vec![2]),
        ("ts BETWEEN TIMESTAMP '2017-07-19 00:00:00' AND TIMESTAMP '2017-07-21 00:00:00'", vec![1]),
        // IN list mixing strings and junk
        ("ts IN ('2017-07-20 15:30:00', 'zzz')", vec![1]),
        ("ts IN (TIMESTAMP '2017-07-22 08:00:00', 'zzz')", vec![2]),
    ]
}

fn small_timestamp_db() -> Database {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t4 (id INTEGER PRIMARY KEY, ts TIMESTAMP);
         INSERT INTO t4 VALUES (1, TIMESTAMP '2017-07-20 15:30:00');
         INSERT INTO t4 VALUES (2, TIMESTAMP '2017-07-22 08:00:00')",
    );
    db
}

/// 600-row table (above SIMD_COLUMNAR_THRESHOLD = 500) exercising the SIMD /
/// cached-columnar filter path. ids 1 and 2 carry the same timestamps as the
/// small table; ids 3..=600 are hourly timestamps starting 2017-08-01 so the
/// small-table expectations carry over for predicates bounded before August.
fn large_timestamp_db() -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t5 (id INTEGER PRIMARY KEY, ts TIMESTAMP)");
    execute_sql(
        &mut db,
        "INSERT INTO t5 VALUES (1, TIMESTAMP '2017-07-20 15:30:00');
         INSERT INTO t5 VALUES (2, TIMESTAMP '2017-07-22 08:00:00')",
    );
    let mut inserts = String::new();
    for id in 3..=600i64 {
        let offset_hours = id - 3;
        let day = 1 + (offset_hours / 24);
        let hour = offset_hours % 24;
        // 598 rows spread across 2017-08-01 .. 2017-08-25
        inserts.push_str(&format!(
            "INSERT INTO t5 VALUES ({id}, TIMESTAMP '2017-08-{day:02} {hour:02}:00:00');"
        ));
    }
    execute_sql(&mut db, &inserts);
    db
}

#[test]
fn small_table_scalar_path_matrix() {
    let db = small_timestamp_db();
    for (pred, expected) in small_table_matrix() {
        let ids = assert_where_matches_projection(&db, "t4", pred);
        assert_eq!(ids, expected, "wrong rows for small-table predicate: {pred}");
    }
}

#[test]
fn large_table_simd_path_matrix() {
    let db = large_timestamp_db();
    for (pred, expected) in small_table_matrix() {
        let ids = assert_where_matches_projection(&db, "t5", pred);
        // Predicates without an upper bound also sweep in the August rows;
        // only check the prefix ids that exist in the small table.
        let prefix: Vec<i64> = ids.iter().copied().filter(|&id| id <= 2).collect();
        assert_eq!(prefix, expected, "wrong (prefix) rows for large-table predicate: {pred}");
    }
}

#[test]
fn large_table_simd_counts() {
    let db = large_timestamp_db();

    // count(*) goes through the fused filter+aggregate path
    let count =
        select_ints(&db, "SELECT count(*) FROM t5 WHERE ts = 'zzz'").expect("count query failed");
    assert_eq!(count, vec![0], "count(*) with junk string must be 0");

    let count =
        select_ints(&db, "SELECT count(*) FROM t5 WHERE ts != 'zzz'").expect("count query failed");
    assert_eq!(count, vec![600], "count(*) != junk string must match every row");

    // Timestamp literal range on a full scan (previously returned 0)
    let count =
        select_ints(&db, "SELECT count(*) FROM t5 WHERE ts < TIMESTAMP '2017-08-01 00:00:00'")
            .expect("count query failed");
    assert_eq!(count, vec![2], "only ids 1,2 predate August");

    let count =
        select_ints(&db, "SELECT count(*) FROM t5 WHERE ts >= TIMESTAMP '2017-08-01 00:00:00'")
            .expect("count query failed");
    assert_eq!(count, vec![598]);

    // String bound spanning the August rows (TEXT-rendering semantics)
    let count = select_ints(&db, "SELECT count(*) FROM t5 WHERE ts < '2017-08-02'")
        .expect("count query failed");
    // ids 1, 2 plus the 24 hourly rows on 2017-08-01
    assert_eq!(count, vec![26]);
}

#[test]
fn null_timestamps_excluded_from_all_predicates() {
    let mut db = small_timestamp_db();
    execute_sql(&mut db, "INSERT INTO t4 VALUES (3, NULL)");

    for pred in
        ["ts < 'zzz'", "ts != 'zzz'", "ts <= TIMESTAMP '2017-07-22 08:00:00'", "ts >= '2017-07-01'"]
    {
        let ids = assert_where_matches_projection(&db, "t4", pred);
        assert!(!ids.contains(&3), "NULL timestamp row must not match predicate: {pred}");
    }
}

// ---------------------------------------------------------------------------
// TIME columns: same TEXT-rendering fallback semantics as Timestamp (#5329)
// ---------------------------------------------------------------------------

fn time_db(rows: usize) -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE tt (id INTEGER PRIMARY KEY, t TIME)");
    execute_sql(
        &mut db,
        "INSERT INTO tt VALUES (1, TIME '08:00:00');
         INSERT INTO tt VALUES (2, TIME '15:30:00')",
    );
    if rows > 2 {
        let mut inserts = String::new();
        for id in 3..=rows {
            let minute = (id - 3) % 60;
            let second = (id - 3) / 60;
            inserts.push_str(&format!(
                "INSERT INTO tt VALUES ({id}, TIME '20:{minute:02}:{second:02}');"
            ));
        }
        execute_sql(&mut db, &inserts);
    }
    db
}

fn time_matrix() -> Vec<(&'static str, Vec<i64>)> {
    vec![
        ("t = '08:00:00'", vec![1]),
        ("t = 'zzz'", vec![]),
        ("t != 'zzz'", vec![1, 2]),
        ("t < 'zzz'", vec![1, 2]),
        ("t > 'zzz'", vec![]),
        ("t >= '09:00'", vec![2]),
        ("t = TIME '15:30:00'", vec![2]),
        ("t < TIME '15:30:00'", vec![1]),
        ("t >= TIME '08:00:00'", vec![1, 2]),
    ]
}

#[test]
fn time_column_scalar_path_matrix() {
    let db = time_db(2);
    for (pred, expected) in time_matrix() {
        let ids = assert_where_matches_projection(&db, "tt", pred);
        assert_eq!(ids, expected, "wrong rows for TIME predicate: {pred}");
    }
}

#[test]
fn time_column_simd_path_matrix() {
    // 600 rows: ids 3.. all have times in the 20:xx:xx range
    let db = time_db(600);
    for (pred, expected) in time_matrix() {
        let ids = assert_where_matches_projection(&db, "tt", pred);
        let prefix: Vec<i64> = ids.iter().copied().filter(|&id| id <= 2).collect();
        assert_eq!(prefix, expected, "wrong (prefix) rows for TIME predicate: {pred}");
    }
}

// ---------------------------------------------------------------------------
// DATE columns: parse-first for parseable strings; unparseable strings raise
// the expression evaluator's type-mismatch error (#5329)
// ---------------------------------------------------------------------------

fn date_db(rows: usize) -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE td (id INTEGER PRIMARY KEY, d DATE)");
    execute_sql(
        &mut db,
        "INSERT INTO td VALUES (1, DATE '2017-07-20');
         INSERT INTO td VALUES (2, DATE '2017-07-22')",
    );
    if rows > 2 {
        let mut inserts = String::new();
        for id in 3..=rows {
            let day = 1 + ((id - 3) % 28);
            let month = 8 + ((id - 3) / 28) % 4;
            inserts.push_str(&format!(
                "INSERT INTO td VALUES ({id}, DATE '2017-{month:02}-{day:02}');"
            ));
        }
        execute_sql(&mut db, &inserts);
    }
    db
}

#[test]
fn date_column_parseable_strings_parse_first() {
    for rows in [2usize, 600] {
        let db = date_db(rows);
        let ids = assert_where_matches_projection(&db, "td", "d >= '2017-07-21'");
        let prefix: Vec<i64> = ids.iter().copied().filter(|&id| id <= 2).collect();
        assert_eq!(prefix, vec![2], "date range failed at {rows} rows");

        let ids = assert_where_matches_projection(&db, "td", "d = '2017-07-20'");
        assert_eq!(
            ids.iter().copied().filter(|&id| id <= 2).collect::<Vec<_>>(),
            vec![1],
            "date equality failed at {rows} rows"
        );
    }
}

#[test]
fn date_column_unparseable_string_raises_type_mismatch() {
    for rows in [2usize, 600] {
        let db = date_db(rows);
        // The expression evaluator raises a type-mismatch error for DATE vs
        // unparseable string; the full-scan WHERE path must match instead of
        // silently filtering.
        let result = select_ints(&db, "SELECT id FROM td WHERE d = 'not-a-date'");
        assert!(
            result.is_err(),
            "DATE vs unparseable string must raise the evaluator's type-mismatch error \
             ({rows} rows), got: {result:?}"
        );
        let result = select_ints(&db, "SELECT id FROM td WHERE d < 'zzz'");
        assert!(
            result.is_err(),
            "DATE vs unparseable string must raise the evaluator's type-mismatch error \
             ({rows} rows), got: {result:?}"
        );
    }
}
