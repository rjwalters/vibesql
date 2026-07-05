//! Regression tests for issue #5823: equality/range index probes must respect
//! the indexed column's declared collation.
//!
//! Sibling of #5806 (IN-list index probe). Index keys are stored raw
//! (BINARY-ordered), so a probe built from a raw literal against a column with
//! a non-BINARY collation (e.g. NOCASE) silently loses rows — the WHERE
//! post-filter can only remove rows, never restore missed ones. The fix
//! declines the probe/fast-path for non-BINARY-collated columns and falls back
//! to the collation-aware full-scan WHERE evaluator (#5805).
//!
//! Code paths covered:
//! - single-column equality probe (`IndexPredicate::Range` equality form)
//! - single-column range probes (`>`, `>=`, `<`, `<=`, BETWEEN)
//! - composite-key lookup with a NOCASE column
//! - prefix-equality lookup with a leading NOCASE column
//! - prefix + trailing-range lookup with a NOCASE range column
//! - BINARY / undeclared columns keep the fast probe (regression guard)
//!
//! Every expected value was verified against sqlite3 3.x with the same fixture.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::CreateIndex(create_index) => {
            vibesql_executor::CreateIndexExecutor::execute(&create_index, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

/// Run a single-column SELECT and collect the integer results.
fn query_ints(db: &vibesql_storage::Database, sql: &str) -> Vec<i64> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
        rows.iter()
            .map(|row| {
                assert_eq!(row.values.len(), 1, "Expected one column for: {}", sql);
                match &row.values[0] {
                    SqlValue::Integer(i) => *i,
                    SqlValue::Bigint(i) => *i,
                    SqlValue::Smallint(i) => i64::from(*i),
                    other => panic!("Expected integer result for {}: {:?}", sql, other),
                }
            })
            .collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn assert_rows(db: &vibesql_storage::Database, sql: &str, expected: &[i64]) {
    let actual = query_ints(db, sql);
    assert_eq!(actual, expected, "Query: {} -- expected {:?}, got {:?}", sql, expected, actual);
}

/// The `t4a` fixture from `in4.test`: `a` has default BINARY collation,
/// `b` is declared NOCASE.
fn db_with_t4a() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t4a(a TEXT, b TEXT COLLATE nocase, c)");
    run_stmt(&mut db, "INSERT INTO t4a VALUES('ABC','abc',1)");
    run_stmt(&mut db, "INSERT INTO t4a VALUES('def','xyz',2)");
    run_stmt(&mut db, "INSERT INTO t4a VALUES('ghi','ghi',3)");
    db
}

/// Same fixture with a single-column index on the NOCASE column (the exact
/// reproducer from the issue body).
fn db_with_t4a_indexed() -> vibesql_storage::Database {
    let mut db = db_with_t4a();
    run_stmt(&mut db, "CREATE INDEX i4ab ON t4a(b)");
    db
}

// ---------------------------------------------------------------------------
// Equality probe — the exact reproducer, with and without the index
// ---------------------------------------------------------------------------

#[test]
fn nocase_equality_without_index() {
    // Ground truth (#5805 full-scan collation-aware path).
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b = 'XYZ'", &[2]);
}

#[test]
fn nocase_equality_with_index_returns_row() {
    // The reproducer: with the index present, the equality probe must not lose
    // the row. Before #5823 this returned 0 rows.
    let db = db_with_t4a_indexed();
    assert_rows(&db, "SELECT c FROM t4a WHERE b = 'XYZ'", &[2]);
}

// ---------------------------------------------------------------------------
// Range probes on the indexed NOCASE column (>, >=, <, <=, BETWEEN)
// ---------------------------------------------------------------------------

#[test]
fn nocase_range_probes_with_index() {
    let db = db_with_t4a_indexed();
    assert_rows(&db, "SELECT c FROM t4a WHERE b >= 'xyz' ORDER BY c", &[2]);
    assert_rows(&db, "SELECT c FROM t4a WHERE b <= 'XYZ' ORDER BY c", &[1, 2, 3]);
    assert_rows(&db, "SELECT c FROM t4a WHERE b > 'GHI' ORDER BY c", &[2]);
    assert_rows(&db, "SELECT c FROM t4a WHERE b < 'GHI' ORDER BY c", &[1]);
    assert_rows(&db, "SELECT c FROM t4a WHERE b BETWEEN 'ABC' AND 'XYZ' ORDER BY c", &[1, 2, 3]);
}

#[test]
fn nocase_range_probes_match_unindexed_oracle() {
    // The indexed result must equal the (correct) full-scan result.
    let indexed = db_with_t4a_indexed();
    let unindexed = db_with_t4a();
    for sql in [
        "SELECT c FROM t4a WHERE b >= 'xyz' ORDER BY c",
        "SELECT c FROM t4a WHERE b <= 'XYZ' ORDER BY c",
        "SELECT c FROM t4a WHERE b > 'GHI' ORDER BY c",
        "SELECT c FROM t4a WHERE b < 'GHI' ORDER BY c",
        "SELECT c FROM t4a WHERE b BETWEEN 'ABC' AND 'XYZ' ORDER BY c",
        "SELECT c FROM t4a WHERE b = 'XYZ' ORDER BY c",
    ] {
        assert_eq!(
            query_ints(&indexed, sql),
            query_ints(&unindexed, sql),
            "indexed vs unindexed mismatch for: {}",
            sql
        );
    }
}

// ---------------------------------------------------------------------------
// Composite / prefix paths involving a NOCASE column
// ---------------------------------------------------------------------------

#[test]
fn composite_lookup_with_nocase_first_column() {
    // Index (b, a): b is NOCASE. Full composite key `b = 'XYZ' AND a = 'def'`.
    let mut db = db_with_t4a();
    run_stmt(&mut db, "CREATE INDEX i4ba ON t4a(b, a)");
    assert_rows(&db, "SELECT c FROM t4a WHERE b = 'XYZ' AND a = 'def'", &[2]);
}

#[test]
fn prefix_lookup_with_nocase_first_column() {
    // Index (b, a): b is NOCASE. Prefix equality on b only.
    let mut db = db_with_t4a();
    run_stmt(&mut db, "CREATE INDEX i4ba ON t4a(b, a)");
    assert_rows(&db, "SELECT c FROM t4a WHERE b = 'XYZ' ORDER BY c", &[2]);
}

#[test]
fn composite_lookup_with_nocase_trailing_column() {
    // Index (a, b): b (second column) is NOCASE. Full composite key.
    let mut db = db_with_t4a();
    run_stmt(&mut db, "CREATE INDEX i4ab2 ON t4a(a, b)");
    assert_rows(&db, "SELECT c FROM t4a WHERE a = 'def' AND b = 'XYZ'", &[2]);
}

#[test]
fn prefix_with_trailing_range_on_nocase_column() {
    // Index (a, b): equality on a (BINARY prefix) + range on b (NOCASE).
    let mut db = db_with_t4a();
    run_stmt(&mut db, "CREATE INDEX i4ab2 ON t4a(a, b)");
    assert_rows(&db, "SELECT c FROM t4a WHERE a = 'def' AND b > 'AAA' ORDER BY c", &[2]);
}

// ---------------------------------------------------------------------------
// BINARY / undeclared columns keep the fast probe (regression guard)
// ---------------------------------------------------------------------------

#[test]
fn binary_column_equality_stays_case_sensitive_with_index() {
    // Column a is BINARY with an index: the raw probe stays valid and
    // case-sensitive.
    let mut db = db_with_t4a();
    run_stmt(&mut db, "CREATE INDEX i4aa ON t4a(a)");
    assert_rows(&db, "SELECT c FROM t4a WHERE a = 'abc'", &[]);
    assert_rows(&db, "SELECT c FROM t4a WHERE a = 'ABC'", &[1]);
    assert_rows(&db, "SELECT c FROM t4a WHERE a >= 'ABC' AND a < 'ghi' ORDER BY c", &[1, 2]);
}

#[test]
fn binary_composite_index_keeps_fast_path() {
    // Two BINARY columns with a composite index: correctness preserved and the
    // fast path remains eligible (no non-BINARY column to gate on).
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t7(x TEXT, y TEXT, z)");
    run_stmt(&mut db, "INSERT INTO t7 VALUES('AA','BB',1)");
    run_stmt(&mut db, "INSERT INTO t7 VALUES('AA','cc',2)");
    run_stmt(&mut db, "INSERT INTO t7 VALUES('dd','ee',3)");
    run_stmt(&mut db, "CREATE INDEX i7xy ON t7(x, y)");
    assert_rows(&db, "SELECT z FROM t7 WHERE x = 'AA' AND y = 'BB'", &[1]);
    // Case-sensitive: 'bb' must not match 'BB'.
    assert_rows(&db, "SELECT z FROM t7 WHERE x = 'AA' AND y = 'bb'", &[]);
    assert_rows(&db, "SELECT z FROM t7 WHERE x = 'AA' ORDER BY z", &[1, 2]);
}
