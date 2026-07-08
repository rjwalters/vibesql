//! Integration tests for vectorized `IS NULL` / `IS NOT NULL` predicates on the
//! columnar full-scan filter path (issue #5993).
//!
//! Before this change the columnar SIMD filter had no representation for null
//! tests, so every `WHERE col IS [NOT] NULL` was forced back to row-at-a-time
//! execution. The `ColumnPredicate::IsNull`/`IsNotNull` variants evaluate the
//! test directly from the column's null bitmap.
//!
//! Correctness is cross-checked two ways:
//!  1. Every `WHERE <pred>` result is compared against a per-row projection of
//!     the same predicate (`SELECT id, <pred>`), which runs the expression
//!     evaluator — the canonical, row-path semantics.
//!  2. Literal expectations are pinned and were verified against sqlite3.
//!
//! The matrix runs at two table sizes — 2 rows (scalar columnar path, below the
//! SIMD threshold of 500) and 600 rows (SIMD / cached-columnar path) — so the
//! two paths must agree across the threshold.

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

fn select_rows(db: &Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = vibesql_executor::SelectExecutor::new(db);
        executor.execute(&select_stmt).map_err(|e| e.to_string())
    } else {
        panic!("Expected SELECT statement");
    }
}

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

fn is_sql_true(v: &SqlValue) -> bool {
    match v {
        SqlValue::Boolean(b) => *b,
        SqlValue::Integer(i) => *i != 0,
        SqlValue::Null => false,
        other => panic!("expected boolean-ish predicate value, got {:?}", other),
    }
}

/// Assert `WHERE <pred>` returns exactly the rows for which the per-row
/// projection `SELECT id, <pred>` (expression evaluator) is true.
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

/// Table `t(id, v)` with `rows` rows. Even ids get a NULL `v`; odd ids get
/// `v = id`. id 1 is always non-null so the column infers a typed (Int64)
/// array with a null bitmap rather than the Mixed fallback.
fn null_db(rows: usize) -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    let mut inserts = String::new();
    for id in 1..=rows as i64 {
        if id % 2 == 0 {
            inserts.push_str(&format!("INSERT INTO t VALUES ({id}, NULL);"));
        } else {
            inserts.push_str(&format!("INSERT INTO t VALUES ({id}, {id});"));
        }
    }
    execute_sql(&mut db, &inserts);
    db
}

fn expected_is_null(rows: usize) -> Vec<i64> {
    (1..=rows as i64).filter(|id| id % 2 == 0).collect()
}

fn expected_is_not_null(rows: usize) -> Vec<i64> {
    (1..=rows as i64).filter(|id| id % 2 == 1).collect()
}

fn run_matrix(rows: usize) {
    let db = null_db(rows);

    // Bare IS NULL / IS NOT NULL against row-path projection + pinned literals.
    let is_null = assert_where_matches_projection(&db, "t", "v IS NULL");
    assert_eq!(is_null, expected_is_null(rows), "IS NULL literal mismatch (rows={rows})");

    let is_not_null = assert_where_matches_projection(&db, "t", "v IS NOT NULL");
    assert_eq!(
        is_not_null,
        expected_is_not_null(rows),
        "IS NOT NULL literal mismatch (rows={rows})"
    );

    // count(*) form from the acceptance criteria.
    let cnt_null = select_ints(&db, "SELECT count(*) FROM t WHERE v IS NULL").unwrap();
    assert_eq!(cnt_null, vec![expected_is_null(rows).len() as i64]);
    let cnt_not_null = select_ints(&db, "SELECT count(*) FROM t WHERE v IS NOT NULL").unwrap();
    assert_eq!(cnt_not_null, vec![expected_is_not_null(rows).len() as i64]);

    // Compound predicates mixing a null test with a value comparison. The
    // null-test mask must AND / OR correctly with the value mask (which treats
    // NULL as non-matching). All cross-checked against the row path.
    assert_where_matches_projection(&db, "t", "v IS NOT NULL AND id > 3");
    assert_where_matches_projection(&db, "t", "v IS NULL AND id < 5");
    assert_where_matches_projection(&db, "t", "v IS NULL OR id = 3");
    assert_where_matches_projection(&db, "t", "v IS NOT NULL OR id = 2");

    // NOT (v IS NULL) is equivalent to v IS NOT NULL (IS NULL never yields
    // UNKNOWN, so three-valued NOT collapses cleanly).
    assert_where_matches_projection(&db, "t", "NOT (v IS NULL)");
}

#[test]
fn is_null_scalar_path_2_rows() {
    // 2 rows: below the SIMD threshold, exercises the scalar columnar path.
    run_matrix(2);
}

#[test]
fn is_null_simd_path_600_rows() {
    // 600 rows: above the SIMD threshold, exercises the SIMD / cached path.
    run_matrix(600);
}

#[test]
fn is_null_string_column() {
    // A NULLable TEXT column: IS NULL must consult the string column's null
    // bitmap, not attempt a value comparison.
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE ts (id INTEGER PRIMARY KEY, s TEXT)");
    execute_sql(
        &mut db,
        "INSERT INTO ts VALUES (1, 'a');
         INSERT INTO ts VALUES (2, NULL);
         INSERT INTO ts VALUES (3, 'c');
         INSERT INTO ts VALUES (4, NULL)",
    );
    let is_null = assert_where_matches_projection(&db, "ts", "s IS NULL");
    assert_eq!(is_null, vec![2, 4]);
    let is_not_null = assert_where_matches_projection(&db, "ts", "s IS NOT NULL");
    assert_eq!(is_not_null, vec![1, 3]);
}

/// Anti-join fixture: `l(id)` has ids 1..=n; `r(id, tag)` has a matching row
/// only for even ids. `l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL` is the
/// classic anti-join — it must return exactly the odd (unmatched) left ids.
fn anti_join_db(n: usize) -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (id INTEGER PRIMARY KEY)");
    execute_sql(&mut db, "CREATE TABLE r (id INTEGER PRIMARY KEY, tag INTEGER)");
    let mut l_inserts = String::new();
    let mut r_inserts = String::new();
    for id in 1..=n as i64 {
        l_inserts.push_str(&format!("INSERT INTO l VALUES ({id});"));
        if id % 2 == 0 {
            r_inserts.push_str(&format!("INSERT INTO r VALUES ({id}, {});", id * 10));
        }
    }
    execute_sql(&mut db, &l_inserts);
    execute_sql(&mut db, &r_inserts);
    db
}

fn assert_anti_join(n: usize) {
    let db = anti_join_db(n);
    let unmatched: Vec<i64> = (1..=n as i64).filter(|id| id % 2 == 1).collect();

    // Anti-join: unmatched (odd) left ids only.
    let got = select_ints(&db, "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL")
        .unwrap();
    assert_eq!(got, unmatched, "anti-join (r.id IS NULL) mismatch (n={n})");

    // Complementary semi-join: matched (even) left ids only.
    let matched: Vec<i64> = (1..=n as i64).filter(|id| id % 2 == 0).collect();
    let got =
        select_ints(&db, "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NOT NULL")
            .unwrap();
    assert_eq!(got, matched, "semi-join (r.id IS NOT NULL) mismatch (n={n})");

    // IS NULL on a non-key right column of the padded row is equally NULL.
    let got = select_ints(&db, "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.tag IS NULL")
        .unwrap();
    assert_eq!(got, unmatched, "anti-join on r.tag IS NULL mismatch (n={n})");
}

#[test]
fn anti_join_is_null_scalar_path() {
    // Small tables: scalar columnar path.
    assert_anti_join(6);
}

#[test]
fn anti_join_is_null_simd_path() {
    // Large left side crosses the SIMD threshold on the joined batch.
    assert_anti_join(600);
}

#[test]
fn is_null_no_nulls_present() {
    // Column with no NULLs at all (absent null bitmap): IS NULL is empty, IS NOT
    // NULL returns every row.
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE tn (id INTEGER PRIMARY KEY, v INTEGER)");
    let mut inserts = String::new();
    for id in 1..=600i64 {
        inserts.push_str(&format!("INSERT INTO tn VALUES ({id}, {id});"));
    }
    execute_sql(&mut db, &inserts);

    let is_null = select_ints(&db, "SELECT id FROM tn WHERE v IS NULL").unwrap();
    assert!(is_null.is_empty());
    let is_not_null = select_ints(&db, "SELECT id FROM tn WHERE v IS NOT NULL").unwrap();
    assert_eq!(is_not_null.len(), 600);
}
