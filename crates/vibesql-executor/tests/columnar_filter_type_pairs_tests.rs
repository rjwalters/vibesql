//! Regression tests for full-scan WHERE filtering on Blob/Boolean type pairs
//! (issue #5340) and IN/NOT IN lists containing NULL (issue #5341).
//!
//! Issue #5340: the columnar full-scan comparator
//! (`select/columnar/filter/comparison.rs`) reported
//! `CompareResult::Incomparable` for Blob-vs-string and Blob-vs-numeric
//! pairs, conservatively excluding every row, while the expression evaluator
//! orders them with SQLite's storage-class ordering (numeric < TEXT < BLOB) —
//! so `blob_col >= 'abc'` is true for every non-NULL blob. Boolean-vs-string
//! raises a type-mismatch error in the evaluator, which the columnar path
//! cannot (no error channel), so pushdown is declined for that pair.
//!
//! Issue #5341: the SIMD InList kernels skipped NULL list elements and then
//! blindly negated the match mask, so `x NOT IN ('x', NULL)` *included*
//! non-matching rows above SIMD_COLUMNAR_THRESHOLD (500 rows). SQL
//! three-valued logic: a NULL element poisons NOT IN — if no element matches
//! the result is UNKNOWN, so `x NOT IN (..., NULL)` is never TRUE. The
//! dispatch-level guard that previously masked this also excluded genuinely
//! matching rows for positive `x IN (a, NULL)`.
//!
//! Every WHERE result is cross-checked against a per-row projection of the
//! same predicate (`SELECT id, <pred>`), which exercises the expression
//! evaluator — the canonical semantics. The matrix runs at two table sizes:
//! 2 rows (scalar columnar path, below SIMD_COLUMNAR_THRESHOLD = 500) and
//! 600 rows (SIMD / cached-columnar path) so the paths must agree across the
//! threshold. All literal expectations were verified against sqlite3.

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
/// projection `SELECT id, <pred>` (expression evaluator, canonical
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

// ---------------------------------------------------------------------------
// Issue #5340: Blob column vs string/numeric literals
// (SQLite storage-class ordering: numeric < TEXT < BLOB)
// ---------------------------------------------------------------------------

/// Blob table with `rows` rows. ids 1 and 2 hold x'6162' ("ab" bytes) and
/// x'7a7a' ("zz" bytes); ids 3.. hold varying two-byte blobs.
fn blob_db(rows: usize) -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE tb (id INTEGER PRIMARY KEY, b BLOB)");
    execute_sql(
        &mut db,
        "INSERT INTO tb VALUES (1, x'6162');
         INSERT INTO tb VALUES (2, x'7a7a')",
    );
    if rows > 2 {
        let mut inserts = String::new();
        for id in 3..=rows {
            inserts.push_str(&format!(
                "INSERT INTO tb VALUES ({id}, x'{:02x}{:02x}');",
                id % 256,
                (id / 256) % 256
            ));
        }
        execute_sql(&mut db, &inserts);
    }
    db
}

/// (predicate, expected ids for the 2-row table) — sqlite3-verified:
/// every BLOB orders greater than every TEXT and every numeric.
fn blob_matrix() -> Vec<(&'static str, Vec<i64>)> {
    vec![
        // BLOB vs string: BLOB > TEXT for every row, regardless of bytes
        ("b >= 'abc'", vec![1, 2]),
        ("b > 'zzz'", vec![1, 2]),
        // even when the blob's bytes equal the string's bytes (x'6162' = 'ab')
        ("b = 'ab'", vec![]),
        ("b != 'abc'", vec![1, 2]),
        ("b < 'zzz'", vec![]),
        ("b <= 'ab'", vec![]),
        // BLOB vs numeric: BLOB > numeric for every row
        ("b > 5", vec![1, 2]),
        ("b >= 1000000", vec![1, 2]),
        ("b <= 5", vec![]),
        ("b = 5", vec![]),
        ("b != 5", vec![1, 2]),
        // BLOB vs BLOB: bytewise (sanity, pre-existing arm)
        ("b = x'6162'", vec![1]),
        ("b != x'6162'", vec![2]),
        ("b < x'7a7a'", vec![1]),
    ]
}

#[test]
fn blob_column_scalar_path_matrix() {
    let db = blob_db(2);
    for (pred, expected) in blob_matrix() {
        let ids = assert_where_matches_projection(&db, "tb", pred);
        assert_eq!(ids, expected, "wrong rows for blob predicate: {pred}");
    }
}

#[test]
fn blob_column_simd_path_matrix() {
    let db = blob_db(600);
    for (pred, expected) in blob_matrix() {
        let ids = assert_where_matches_projection(&db, "tb", pred);
        let prefix: Vec<i64> = ids.iter().copied().filter(|&id| id <= 2).collect();
        assert_eq!(prefix, expected, "wrong (prefix) rows for blob predicate: {pred}");
    }

    // Predicates true for *every* blob must sweep in all 600 rows
    let ids = assert_where_matches_projection(&db, "tb", "b >= 'abc'");
    assert_eq!(ids.len(), 600, "BLOB > TEXT must hold for every row");
    let ids = assert_where_matches_projection(&db, "tb", "b > 5");
    assert_eq!(ids.len(), 600, "BLOB > numeric must hold for every row");
}

#[test]
fn null_blobs_excluded_from_all_predicates() {
    for rows in [2usize, 600] {
        let mut db = blob_db(rows);
        execute_sql(&mut db, "INSERT INTO tb VALUES (100001, NULL)");
        for pred in ["b >= 'abc'", "b != 'abc'", "b > 5", "b != x'6162'"] {
            let ids = assert_where_matches_projection(&db, "tb", pred);
            assert!(
                !ids.contains(&100001),
                "NULL blob row must not match predicate ({rows} rows): {pred}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Boolean column vs string literal: since issue #5803 the evaluator
// normalizes Boolean to Integer 0/1 and applies SQLite storage-class
// ordering (numeric < TEXT), so these predicates order instead of raising a
// type-mismatch error. Pushdown remains declined for the pair (the columnar
// numeric-vs-string arm coerces parseable strings to numbers, which would
// diverge from the evaluator's strict type ordering), so the full-scan WHERE
// path must agree with the evaluator via the projection cross-check.
// ---------------------------------------------------------------------------

fn boolean_db(rows: usize) -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE tf (id INTEGER PRIMARY KEY, flag BOOLEAN)");
    execute_sql(
        &mut db,
        "INSERT INTO tf VALUES (1, TRUE);
         INSERT INTO tf VALUES (2, FALSE)",
    );
    if rows > 2 {
        let mut inserts = String::new();
        for id in 3..=rows {
            let v = if id % 2 == 0 { "FALSE" } else { "TRUE" };
            inserts.push_str(&format!("INSERT INTO tf VALUES ({id}, {v});"));
        }
        execute_sql(&mut db, &inserts);
    }
    db
}

#[test]
fn boolean_column_vs_string_orders_by_storage_class() {
    for rows in [2usize, 600] {
        let db = boolean_db(rows);
        // Issue #5803: Boolean normalizes to Integer 0/1, then SQLite type
        // ordering applies (numeric < TEXT). Verified against sqlite3 with a
        // NUMERIC-affinity column holding 0/1:
        //   flag = 'true'  -> 0 rows  (integer never equals text)
        //   flag != 'x'    -> all rows
        //   flag < 'zzz'   -> all rows (numeric < text)
        let ids = assert_where_matches_projection(&db, "tf", "flag = 'true'");
        assert!(ids.is_empty(), "flag = 'true' must match no rows ({rows} rows), got {ids:?}");

        let ids = assert_where_matches_projection(&db, "tf", "flag != 'x'");
        assert_eq!(ids.len(), rows, "flag != 'x' must match all rows ({rows} rows)");

        let ids = assert_where_matches_projection(&db, "tf", "flag < 'zzz'");
        assert_eq!(ids.len(), rows, "flag < 'zzz' must match all rows ({rows} rows)");
    }
}

#[test]
fn boolean_column_supported_pairs_still_filter() {
    for rows in [2usize, 600] {
        let db = boolean_db(rows);
        // Boolean vs Boolean and Boolean vs numeric (0/1 coercion) keep
        // working on both sides of the threshold.
        let ids = assert_where_matches_projection(&db, "tf", "flag = TRUE");
        assert!(ids.contains(&1) && !ids.contains(&2), "flag = TRUE wrong at {rows} rows");
        let ids = assert_where_matches_projection(&db, "tf", "flag = 1");
        assert!(ids.contains(&1) && !ids.contains(&2), "flag = 1 wrong at {rows} rows");
        let ids = assert_where_matches_projection(&db, "tf", "flag = 0");
        assert!(ids.contains(&2) && !ids.contains(&1), "flag = 0 wrong at {rows} rows");
    }
}

// ---------------------------------------------------------------------------
// Issue #5345: Boolean literal vs numeric column on the SIMD kernels.
// Booleans are integers (0/1) in SQLite storage semantics, so `v = TRUE`
// matches rows where v = 1. The SIMD path's `value_to_f64` had no Boolean
// arm and raised ColumnarTypeMismatch at/above SIMD_COLUMNAR_THRESHOLD while
// the scalar path (below threshold) filtered correctly.
// ---------------------------------------------------------------------------

/// Integer + double columns; `v` = id % 2 (the issue's repro shape) and
/// `f` = v as a float, so TRUE/FALSE literals partition the rows in half.
fn numeric_db(rows: usize) -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE ti (id INTEGER PRIMARY KEY, v INTEGER, f DOUBLE PRECISION)");
    let mut inserts = String::new();
    for id in 1..=rows {
        let v = id % 2;
        inserts.push_str(&format!("INSERT INTO ti VALUES ({id}, {v}, {v}.0);"));
        if id % 100 == 0 {
            execute_sql(&mut db, &inserts);
            inserts.clear();
        }
    }
    execute_sql(&mut db, &inserts);
    db
}

/// Boolean literals against INTEGER and DOUBLE columns must coerce to 0/1
/// on both sides of SIMD_COLUMNAR_THRESHOLD. sqlite3-verified: `v = TRUE`
/// returns the v = 1 rows, `v != FALSE` the v <> 0 rows, etc.
#[test]
fn boolean_literal_vs_numeric_column_across_threshold() {
    for rows in [2usize, 600] {
        let db = numeric_db(rows);
        let odd: Vec<i64> = (1..=rows as i64).filter(|id| id % 2 == 1).collect();
        let even: Vec<i64> = (1..=rows as i64).filter(|id| id % 2 == 0).collect();
        let all: Vec<i64> = (1..=rows as i64).collect();

        // Equal / NotEqual kernels (the issue's repro)
        let cases: Vec<(&str, &Vec<i64>)> = vec![
            ("v = TRUE", &odd),
            ("v = FALSE", &even),
            ("v != FALSE", &odd),
            ("v != TRUE", &even),
            // LessThan / GreaterThan / Less-or-eq / Greater-or-eq kernels
            ("v > FALSE", &odd),
            ("v < TRUE", &even),
            ("v >= TRUE", &odd),
            ("v <= FALSE", &even),
            ("v >= FALSE", &all),
            // f64 kernels (same value_to_f64 conversion)
            ("f = TRUE", &odd),
            ("f != FALSE", &odd),
            ("f < TRUE", &even),
        ];
        for (pred, expected) in cases {
            let ids = assert_where_matches_projection(&db, "ti", pred);
            assert_eq!(
                &ids, expected,
                "wrong rows for boolean-literal predicate ({rows} rows): {pred}"
            );
        }
    }
}

/// NULL numeric values stay excluded when compared against Boolean literals.
#[test]
fn boolean_literal_vs_numeric_column_null_rows_excluded() {
    for rows in [2usize, 600] {
        let mut db = numeric_db(rows);
        execute_sql(&mut db, "INSERT INTO ti VALUES (100001, NULL, NULL)");
        for pred in ["v = TRUE", "v != FALSE", "f != TRUE"] {
            let ids = assert_where_matches_projection(&db, "ti", pred);
            assert!(
                !ids.contains(&100001),
                "NULL row must not match boolean-literal predicate ({rows} rows): {pred}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Issue #5341: IN / NOT IN with NULL list elements (three-valued logic)
// `x NOT IN (a, NULL)` is never TRUE; `x IN (a, NULL)` is TRUE only on match.
// ---------------------------------------------------------------------------

/// Table with integer, float, string, date, and timestamp value columns.
/// `v` cycles 0..10, `f` = v + 0.5, `s` cycles 'a'..'e', dates/timestamps
/// cycle across five days in July 2017. id 100001 (when `with_nulls`) holds
/// NULL in every value column.
fn inlist_db(rows: usize, with_nulls: bool) -> Database {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE tn (id INTEGER PRIMARY KEY, v INTEGER, f DOUBLE PRECISION, \
         s VARCHAR(10), d DATE, ts TIMESTAMP)",
    );
    let mut inserts = String::new();
    for id in 1..=rows {
        let v = id % 10;
        let letter = (b'a' + (id % 5) as u8) as char;
        let day = 20 + (id % 5);
        inserts.push_str(&format!(
            "INSERT INTO tn VALUES ({id}, {v}, {v}.5, '{letter}', DATE '2017-07-{day}', \
             TIMESTAMP '2017-07-{day} 12:00:00');"
        ));
        if id % 100 == 0 {
            execute_sql(&mut db, &inserts);
            inserts.clear();
        }
    }
    execute_sql(&mut db, &inserts);
    if with_nulls {
        execute_sql(&mut db, "INSERT INTO tn VALUES (100001, NULL, NULL, NULL, NULL, NULL)");
    }
    db
}

/// NOT IN lists containing NULL match no rows; IN lists containing NULL
/// match only genuine matches. Exercises the i64, f64, string, date, and
/// timestamp kernels (600 rows) and the scalar columnar path (2 rows).
#[test]
fn in_list_with_null_three_valued_logic() {
    for rows in [2usize, 600] {
        let db = inlist_db(rows, false);

        // NOT IN with NULL in the list: never TRUE, for every kernel type
        for pred in [
            "v NOT IN (1, NULL)",
            "f NOT IN (1.5, NULL)",
            "s NOT IN ('a', NULL)",
            "d NOT IN (DATE '2017-07-21', NULL)",
            "ts NOT IN (TIMESTAMP '2017-07-21 12:00:00', NULL)",
            "v NOT IN (NULL)",
        ] {
            let ids = assert_where_matches_projection(&db, "tn", pred);
            assert_eq!(
                ids,
                Vec::<i64>::new(),
                "NOT IN with NULL must match no rows ({rows} rows): {pred}"
            );
        }

        // Positive IN with NULL in the list: matches only genuine matches
        // (the old dispatch guard excluded these too)
        let ids = assert_where_matches_projection(&db, "tn", "v IN (1, NULL)");
        let expected: Vec<i64> = (1..=rows as i64).filter(|id| id % 10 == 1).collect();
        assert_eq!(ids, expected, "IN with NULL must still match v = 1 ({rows} rows)");

        let ids = assert_where_matches_projection(&db, "tn", "s IN ('a', NULL)");
        let expected: Vec<i64> = (1..=rows as i64).filter(|id| id % 5 == 0).collect();
        assert_eq!(ids, expected, "IN with NULL must still match s = 'a' ({rows} rows)");

        // Sanity: NOT IN without NULL still filters normally
        let ids = assert_where_matches_projection(&db, "tn", "v NOT IN (1, 2)");
        let expected: Vec<i64> =
            (1..=rows as i64).filter(|id| id % 10 != 1 && id % 10 != 2).collect();
        assert_eq!(ids, expected, "plain NOT IN broken ({rows} rows)");
    }
}

/// NULL column values are UNKNOWN for both IN and NOT IN — excluded in WHERE
/// context (the string kernel's blind negation used to resurrect them, and
/// the scalar columnar evaluator returned TRUE for negated lists).
#[test]
fn null_column_values_excluded_from_in_and_not_in() {
    for rows in [2usize, 600] {
        let db = inlist_db(rows, true);
        for pred in [
            "v NOT IN (1, 2)",
            "s NOT IN ('a', 'b')",
            "f NOT IN (1.5)",
            "d NOT IN (DATE '2017-07-21')",
            "ts NOT IN (TIMESTAMP '2017-07-21 12:00:00')",
            "v IN (1, 2)",
            "s IN ('a', 'b')",
        ] {
            let ids = assert_where_matches_projection(&db, "tn", pred);
            assert!(
                !ids.contains(&100001),
                "NULL row must not match IN/NOT IN predicate ({rows} rows): {pred}"
            );
        }
    }
}

/// count(*) goes through the fused filter+aggregate path (packed masks).
#[test]
fn in_list_with_null_packed_mask_counts() {
    let db = inlist_db(600, false);

    let count = select_ints(&db, "SELECT count(*) FROM tn WHERE v NOT IN (1, NULL)")
        .expect("count query failed");
    assert_eq!(count, vec![0], "NOT IN with NULL must count 0 rows");

    let count = select_ints(&db, "SELECT count(*) FROM tn WHERE v IN (1, NULL)")
        .expect("count query failed");
    assert_eq!(count, vec![60], "IN with NULL must count the 60 rows where v = 1");

    let count = select_ints(&db, "SELECT count(*) FROM tn WHERE v NOT IN (1, 2)")
        .expect("count query failed");
    assert_eq!(count, vec![480], "plain NOT IN must count rows where v NOT IN (1,2)");
}
