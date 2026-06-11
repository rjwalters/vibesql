//! Regression tests for temporal index probes with string bounds (issue #5333).
//!
//! Index keys produced by `date()` / `datetime()` expression indexes — and by
//! plain `DATE`/`TIMESTAMP` column indexes — are temporal `SqlValue`s, while
//! WHERE-clause string literals stay `Varchar`. `SqlValue`'s total order
//! falls back to type-tag ordering for cross-type pairs (`Varchar`=10 <
//! `Date`=12 < `Timestamp`=14, cross-type equality = false), so before the
//! fix:
//!
//! - equality / BETWEEN / `<` / `<=` probes silently returned 0 rows, and
//! - `>` / `>=` probes over-returned (every temporal key sorts above every
//!   string), returning rows below the bound whenever the planner skipped the
//!   residual WHERE filter.
//!
//! The fix coerces string probe bounds to the stored temporal key type at
//! probe time, mirroring the executor comparison semantics from #5329:
//! `Date` vs string is parse-first; `Timestamp`/`Time` vs string compares the
//! TEXT renderings. The invariant under test: **every indexed query returns
//! exactly the same rows as the identical query after `DROP INDEX`.**

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DropIndexExecutor, InsertExecutor,
};
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
            vibesql_ast::Statement::CreateIndex(s) => {
                CreateIndexExecutor::execute(&s, db).expect("CREATE INDEX failed");
            }
            vibesql_ast::Statement::DropIndex(s) => {
                DropIndexExecutor::execute(&s, db).expect("DROP INDEX failed");
            }
            vibesql_ast::Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            other => panic!("Unsupported statement type: {:?}", other),
        }
    }
}

/// Execute a SELECT and return the first column of each row as sorted i64s.
fn select_ints(db: &Database, sql: &str) -> Result<Vec<i64>, String> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = vibesql_executor::SelectExecutor::new(db);
        let rows = executor.execute(&select_stmt).map_err(|e| e.to_string())?;
        let mut out: Vec<i64> = rows
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(i) => *i,
                other => panic!("expected integer, got {:?}", other),
            })
            .collect();
        out.sort_unstable();
        Ok(out)
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Assert that `sql` returns the same rows with the index present as after
/// dropping it (index probe == full scan invariant), and that both match
/// `expected`. Recreates the index afterwards so the caller can chain checks.
fn assert_index_matches_full_scan(
    db: &mut Database,
    create_index_sql: &str,
    drop_index_sql: &str,
    sql: &str,
    expected: &[i64],
) {
    let indexed =
        select_ints(db, sql).unwrap_or_else(|e| panic!("indexed query failed: {e}\n  sql: {sql}"));
    execute_sql(db, drop_index_sql);
    let full_scan = select_ints(db, sql)
        .unwrap_or_else(|e| panic!("full-scan query failed: {e}\n  sql: {sql}"));
    execute_sql(db, create_index_sql);

    assert_eq!(
        indexed, full_scan,
        "index probe diverged from full scan for: {sql}\n  indexed:   {indexed:?}\n  full scan: {full_scan:?}"
    );
    assert_eq!(indexed, expected, "wrong rows for: {sql}");
}

// ---------------------------------------------------------------------------
// Expression index on datetime(b) — Timestamp keys (TEXT-rendering semantics)
// ---------------------------------------------------------------------------

const T1_CREATE_INDEX: &str = "CREATE INDEX t1dt ON t1(datetime(b))";
const T1_DROP_INDEX: &str = "DROP INDEX t1dt";

/// Rows: julianday-style REALs; datetime(b) yields midnight timestamps
/// 2017-07-02 .. 2017-07-11 for x = 1..10 (2457936.5 = 2017-07-02 00:00:00).
fn timestamp_expression_db() -> Database {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t1 (x INTEGER PRIMARY KEY, b REAL);
         INSERT INTO t1 VALUES (1, 2457936.5);
         INSERT INTO t1 VALUES (2, 2457937.5);
         INSERT INTO t1 VALUES (3, 2457938.5);
         INSERT INTO t1 VALUES (4, 2457939.5);
         INSERT INTO t1 VALUES (5, 2457940.5);
         INSERT INTO t1 VALUES (6, 2457941.5);
         INSERT INTO t1 VALUES (7, 2457942.5);
         INSERT INTO t1 VALUES (8, 2457943.5);
         INSERT INTO t1 VALUES (9, 2457944.5);
         INSERT INTO t1 VALUES (10, 2457945.5)",
    );
    execute_sql(&mut db, T1_CREATE_INDEX);
    db
}

fn check_t1(db: &mut Database, sql: &str, expected: &[i64]) {
    assert_index_matches_full_scan(db, T1_CREATE_INDEX, T1_DROP_INDEX, sql, expected);
}

#[test]
fn expression_index_timestamp_equality_full_string() {
    let mut db = timestamp_expression_db();
    // Round-tripping canonical string: matches the midnight timestamp.
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) = '2017-07-05 00:00:00'", &[4]);
}

#[test]
fn expression_index_timestamp_equality_date_only_string_is_empty() {
    let mut db = timestamp_expression_db();
    // TEXT-rendering semantics (#5329): a midnight timestamp renders
    // '2017-07-05 00:00:00' which is NOT equal to the date-only string, so
    // equality matches nothing — both via the index and via the full scan.
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) = '2017-07-05'", &[]);
}

#[test]
fn expression_index_timestamp_between_date_only_strings() {
    let mut db = timestamp_expression_db();
    // date2-331 shape: '2017-07-04' midnight included (renders greater than
    // the date-only lower bound), '2017-07-08' midnight excluded (renders
    // greater than the upper bound too). x: 07-04→3 .. 07-07→6.
    check_t1(
        &mut db,
        "SELECT x FROM t1 WHERE datetime(b) BETWEEN '2017-07-04' AND '2017-07-08'",
        &[3, 4, 5, 6],
    );
}

#[test]
fn expression_index_timestamp_one_sided_ranges_date_only_strings() {
    let mut db = timestamp_expression_db();
    // Under TEXT semantics every timestamp on 2017-07-08 renders strictly
    // greater than '2017-07-08', so > and >= are the same set (midnight of
    // 07-08 = x 7 included in both), and < and <= are the same set.
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) >= '2017-07-08'", &[7, 8, 9, 10]);
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) > '2017-07-08'", &[7, 8, 9, 10]);
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) <= '2017-07-04'", &[1, 2]);
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) < '2017-07-04'", &[1, 2]);
}

#[test]
fn expression_index_timestamp_one_sided_ranges_full_strings() {
    let mut db = timestamp_expression_db();
    // Round-tripping canonical strings: exact bounds.
    check_t1(
        &mut db,
        "SELECT x FROM t1 WHERE datetime(b) >= '2017-07-08 00:00:00'",
        &[7, 8, 9, 10],
    );
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) > '2017-07-08 00:00:00'", &[8, 9, 10]);
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) <= '2017-07-04 00:00:00'", &[1, 2, 3]);
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) < '2017-07-04 00:00:00'", &[1, 2]);
}

#[test]
fn expression_index_timestamp_no_over_return_on_lower_bound() {
    let mut db = timestamp_expression_db();
    // Over-return guard: rows strictly below the bound must NOT be returned.
    // Before the fix, lower-bounded probes returned every temporal key.
    let rows = select_ints(&db, "SELECT x FROM t1 WHERE datetime(b) >= '2017-07-10'").unwrap();
    assert_eq!(rows, vec![9, 10], "rows below the lower bound must not be returned");
    drop(db);
}

#[test]
fn expression_index_timestamp_in_list() {
    let mut db = timestamp_expression_db();
    check_t1(
        &mut db,
        "SELECT x FROM t1 WHERE datetime(b) IN ('2017-07-03 00:00:00', '2017-07-09 00:00:00')",
        &[2, 8],
    );
    // Date-only / junk elements match nothing under TEXT-rendering equality.
    check_t1(
        &mut db,
        "SELECT x FROM t1 WHERE datetime(b) IN ('2017-07-03', 'junk', '2017-07-09 00:00:00')",
        &[8],
    );
}

#[test]
fn expression_index_timestamp_unparseable_bound_matches_full_scan() {
    let mut db = timestamp_expression_db();
    // Junk strings compare as text in the executor (never an error for
    // Timestamp): every rendering starts with a digit < 'h', so all rows
    // sort below 'hello'.
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) = 'hello'", &[]);
    check_t1(
        &mut db,
        "SELECT x FROM t1 WHERE datetime(b) < 'hello'",
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    );
    check_t1(&mut db, "SELECT x FROM t1 WHERE datetime(b) > 'hello'", &[]);
}

// ---------------------------------------------------------------------------
// Expression index on date(y) — Date keys (parse-first semantics)
// ---------------------------------------------------------------------------

const T2_CREATE_INDEX: &str = "CREATE INDEX t2y ON t2(date(y))";
const T2_DROP_INDEX: &str = "DROP INDEX t2y";

fn date_expression_db() -> Database {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t2 (x INTEGER PRIMARY KEY, y TEXT);
         INSERT INTO t2 VALUES (1, '2017-07-20 15:30:00');
         INSERT INTO t2 VALUES (2, '2017-07-22 08:00:00');
         INSERT INTO t2 VALUES (3, '2017-07-25 23:59:59')",
    );
    execute_sql(&mut db, T2_CREATE_INDEX);
    db
}

fn check_t2(db: &mut Database, sql: &str, expected: &[i64]) {
    assert_index_matches_full_scan(db, T2_CREATE_INDEX, T2_DROP_INDEX, sql, expected);
}

#[test]
fn expression_index_date_equality() {
    // Issue-body repro: was 0 rows via the index probe before the fix.
    let mut db = date_expression_db();
    check_t2(&mut db, "SELECT x FROM t2 WHERE date(y) = '2017-07-20'", &[1]);
}

#[test]
fn expression_index_date_between_and_one_sided() {
    let mut db = date_expression_db();
    check_t2(
        &mut db,
        "SELECT x FROM t2 WHERE date(y) BETWEEN '2017-07-20' AND '2017-07-22'",
        &[1, 2],
    );
    check_t2(&mut db, "SELECT x FROM t2 WHERE date(y) > '2017-07-20'", &[2, 3]);
    check_t2(&mut db, "SELECT x FROM t2 WHERE date(y) >= '2017-07-22'", &[2, 3]);
    check_t2(&mut db, "SELECT x FROM t2 WHERE date(y) < '2017-07-22'", &[1]);
    check_t2(&mut db, "SELECT x FROM t2 WHERE date(y) <= '2017-07-22'", &[1, 2]);
}

#[test]
fn expression_index_date_in_list() {
    let mut db = date_expression_db();
    check_t2(&mut db, "SELECT x FROM t2 WHERE date(y) IN ('2017-07-20', '2017-07-25')", &[1, 3]);
}

#[test]
fn expression_index_date_unparseable_bound_matches_full_scan_error() {
    // Date vs unparseable string is a type-mismatch error in the executor
    // (parse-first semantics). The indexed path must not silently return
    // 0 rows — it must surface the same outcome as the full scan.
    let db = date_expression_db();
    let indexed = select_ints(&db, "SELECT x FROM t2 WHERE date(y) = 'junk'");

    let mut db2 = date_expression_db();
    execute_sql(&mut db2, T2_DROP_INDEX);
    let full_scan = select_ints(&db2, "SELECT x FROM t2 WHERE date(y) = 'junk'");

    assert_eq!(
        indexed.is_err(),
        full_scan.is_err(),
        "indexed and full-scan outcomes must agree for unparseable Date bound: \
         indexed={indexed:?}, full_scan={full_scan:?}"
    );
    if let (Ok(a), Ok(b)) = (&indexed, &full_scan) {
        assert_eq!(a, b);
    }
}

// ---------------------------------------------------------------------------
// Plain TIMESTAMP column index (t4 repro from the issue)
//
// NOTE: these assert literal expected values (executor semantics) for the
// INDEXED queries instead of diffing against DROP INDEX, because the
// full-scan WHERE path has its own pre-existing temporal-vs-string bug (the
// columnar SIMD filter and CompiledPredicate lack the #5329 semantics and
// over-return — `WHERE ts = 'zzz'` returns every row via a full scan). That
// is tracked separately in issue #5335; once fixed, these can be upgraded to
// indexed-vs-full-scan comparisons like the expression-index tests above.
// ---------------------------------------------------------------------------

fn timestamp_column_db() -> Database {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t4 (id INTEGER PRIMARY KEY, ts TIMESTAMP);
         INSERT INTO t4 VALUES (1, TIMESTAMP '2017-07-20 15:30:00');
         INSERT INTO t4 VALUES (2, TIMESTAMP '2017-07-22 08:00:00');
         INSERT INTO t4 VALUES (3, TIMESTAMP '2017-07-25 23:59:59');
         CREATE INDEX t4ts ON t4(ts)",
    );
    db
}

fn check_t4(db: &Database, sql: &str, expected: &[i64]) {
    let rows = select_ints(db, sql).unwrap_or_else(|e| panic!("query failed: {e}\n  sql: {sql}"));
    assert_eq!(rows, expected, "wrong rows for indexed query: {sql}");
}

#[test]
fn column_index_timestamp_equality_string() {
    // Was 0 rows before the fix (row loss).
    let db = timestamp_column_db();
    check_t4(&db, "SELECT id FROM t4 WHERE ts = '2017-07-20 15:30:00'", &[1]);
    // TEXT-rendering semantics: a date-only string equals no timestamp.
    check_t4(&db, "SELECT id FROM t4 WHERE ts = '2017-07-20'", &[]);
}

#[test]
fn column_index_timestamp_no_over_return_on_lower_bound() {
    // The t4 repro: `ts >= '2017-07-21'` returned the 2017-07-20 row before
    // the fix because the probe over-returned and the planner skipped the
    // residual filter.
    let db = timestamp_column_db();
    check_t4(&db, "SELECT id FROM t4 WHERE ts >= '2017-07-21'", &[2, 3]);
    check_t4(&db, "SELECT id FROM t4 WHERE ts > '2017-07-21'", &[2, 3]);
}

#[test]
fn column_index_timestamp_between_and_upper_bounds() {
    let db = timestamp_column_db();
    check_t4(&db, "SELECT id FROM t4 WHERE ts BETWEEN '2017-07-21' AND '2017-07-23'", &[2]);
    check_t4(&db, "SELECT id FROM t4 WHERE ts < '2017-07-22'", &[1]);
    check_t4(&db, "SELECT id FROM t4 WHERE ts <= '2017-07-22 08:00:00'", &[1, 2]);
    check_t4(&db, "SELECT id FROM t4 WHERE ts < '2017-07-22 08:00:00'", &[1]);
}

#[test]
fn column_index_timestamp_in_list_and_typed_literals() {
    let db = timestamp_column_db();
    check_t4(
        &db,
        "SELECT id FROM t4 WHERE ts IN ('2017-07-20 15:30:00', '2017-07-25 23:59:59')",
        &[1, 3],
    );
    // Typed literals were already correct; pin that they stay correct.
    check_t4(&db, "SELECT id FROM t4 WHERE ts = TIMESTAMP '2017-07-22 08:00:00'", &[2]);
    check_t4(&db, "SELECT id FROM t4 WHERE ts >= TIMESTAMP '2017-07-22 08:00:00'", &[2, 3]);
}

#[test]
fn column_index_timestamp_unparseable_equality_is_empty() {
    // Junk strings never equal a timestamp rendering — no panic, no rows.
    // (Range operators with junk bounds are governed by the residual-filter
    // semantics tracked in #5335 and are not asserted here.)
    let db = timestamp_column_db();
    check_t4(&db, "SELECT id FROM t4 WHERE ts = 'hello'", &[]);
}

#[test]
fn column_index_timestamp_null_keys_excluded_from_ranges() {
    // NULL keys must not be swept into coerced upper-bounded ranges
    // (SQL semantics: NULL < x is NULL, not true).
    let mut db = timestamp_column_db();
    execute_sql(&mut db, "INSERT INTO t4 VALUES (4, NULL)");
    check_t4(&db, "SELECT id FROM t4 WHERE ts < '2017-07-23'", &[1, 2]);
    check_t4(&db, "SELECT id FROM t4 WHERE ts >= '2017-07-21'", &[2, 3]);
}
