//! Regression tests for issue #5806: scalar IN-list comparisons must use the
//! collating sequence of the LHS (SQLite datatype3.html section 7.2).
//!
//! `x IN (y, z, ...)` uses only the collation of `x` — an explicit COLLATE
//! operator, or the column's declared collation. List-element collations are
//! irrelevant. Four code paths are covered:
//!
//! - the slow-path `eval_in_list` (both the <=3-element linear scan and the >3-element HashSet
//!   lookup), in both evaluators;
//! - the columnar `ColumnPredicate::InList` pushdown (declined for non-BINARY-collated columns);
//! - the vectorized `try_compile_in_list` (same decline);
//! - the index-probe path (`IndexPredicate::In` is declined for a non-BINARY-collated indexed
//!   column, falling back to the collation-aware WHERE filter).
//!
//! Expected values verified against sqlite3 3.x with the same fixture.

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

/// Same fixture with an index on the NOCASE column (index-probe path).
fn db_with_t4a_indexed() -> vibesql_storage::Database {
    let mut db = db_with_t4a();
    run_stmt(&mut db, "CREATE INDEX i4ab ON t4a(b)");
    db
}

// ---------------------------------------------------------------------------
// The five verified reproducers from issue #5806 (no index)
// ---------------------------------------------------------------------------

#[test]
fn nocase_column_in_single_literal() {
    // sqlite3: b IN ('XYZ') matches 'xyz' (NOCASE), returns 2.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN ('XYZ')", &[2]);
}

#[test]
fn nocase_column_in_two_literals_linear_path() {
    // <=3 elements exercises the linear-scan path.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN ('ABC','GHI') ORDER BY c", &[1, 3]);
}

#[test]
fn explicit_collate_nocase_on_lhs() {
    // a COLLATE nocase IN ('abc'): explicit COLLATE on the LHS applies even
    // though column a's declared collation is BINARY. (Slow path only —
    // fast paths require a bare column LHS.)
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE a COLLATE nocase IN ('abc')", &[1]);
}

#[test]
fn nocase_column_in_four_literals_hashset_path() {
    // >3 elements exercises the HashSet path (values transformed before
    // insertion, probe value transformed before lookup).
    let db = db_with_t4a();
    assert_rows(
        &db,
        "SELECT c FROM t4a WHERE b IN ('ABC','GHI','QRS','XYZ') ORDER BY c",
        &[1, 2, 3],
    );
}

#[test]
fn nocase_column_not_in_hashset_path() {
    // NOT IN must be the exact complement: every row matches some element
    // under NOCASE, so the result is empty.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b NOT IN ('ABC','GHI','QRS','XYZ') ORDER BY c", &[]);
}

// ---------------------------------------------------------------------------
// Index-probe path (IN on an indexed NOCASE column)
// ---------------------------------------------------------------------------

#[test]
fn indexed_nocase_column_in_single_literal() {
    let db = db_with_t4a_indexed();
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN ('XYZ')", &[2]);
}

#[test]
fn indexed_nocase_column_in_four_literals() {
    let db = db_with_t4a_indexed();
    assert_rows(
        &db,
        "SELECT c FROM t4a WHERE b IN ('ABC','GHI','QRS','XYZ') ORDER BY c",
        &[1, 2, 3],
    );
}

#[test]
fn indexed_nocase_column_not_in() {
    let db = db_with_t4a_indexed();
    assert_rows(&db, "SELECT c FROM t4a WHERE b NOT IN ('ABC','GHI','QRS','XYZ') ORDER BY c", &[]);
}

#[test]
fn indexed_binary_column_in_keeps_exact_matching() {
    // BINARY column with an index: IN stays case-sensitive and the index
    // probe stays usable.
    let mut db = db_with_t4a_indexed();
    run_stmt(&mut db, "CREATE INDEX i4aa ON t4a(a)");
    assert_rows(&db, "SELECT c FROM t4a WHERE a IN ('abc','ghi') ORDER BY c", &[3]);
    assert_rows(&db, "SELECT c FROM t4a WHERE a IN ('ABC','ghi') ORDER BY c", &[1, 3]);
}

// ---------------------------------------------------------------------------
// BINARY / uncollated LHS keeps existing behavior
// ---------------------------------------------------------------------------

#[test]
fn binary_column_in_stays_case_sensitive() {
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE a IN ('abc')", &[]);
    assert_rows(&db, "SELECT c FROM t4a WHERE a IN ('ABC')", &[1]);
    // HashSet path (>3 elements): only the exact-case 'ghi' matches.
    assert_rows(&db, "SELECT c FROM t4a WHERE a IN ('abc','ghi','qrs','xyz') ORDER BY c", &[3]);
}

#[test]
fn concat_wrapped_nocase_lhs_is_binary() {
    // (b||'') has no collating sequence (implicit collation does not leak
    // through ||), so the IN comparison is BINARY.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE (b||'') IN ('XYZ')", &[]);
    assert_rows(&db, "SELECT c FROM t4a WHERE (b||'') IN ('xyz')", &[2]);
}

// ---------------------------------------------------------------------------
// NULL three-valued logic is preserved after the collation transform
// ---------------------------------------------------------------------------

#[test]
fn null_in_list_still_yields_unknown() {
    // b IN ('QRS', NULL): no match + NULL in list -> UNKNOWN -> row filtered.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN ('QRS', NULL) ORDER BY c", &[]);
    // A NOCASE match still wins over the NULL.
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN ('XYZ', NULL) ORDER BY c", &[2]);
    // NOT IN with a NULL in the list can never be TRUE.
    assert_rows(&db, "SELECT c FROM t4a WHERE b NOT IN ('QRS', NULL) ORDER BY c", &[]);
    // HashSet path (>3 elements) with NULL behaves identically.
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN ('QRS','TUV','WXY',NULL) ORDER BY c", &[]);
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN ('XYZ','TUV','WXY',NULL) ORDER BY c", &[2]);
}

#[test]
fn empty_in_list_unaffected() {
    // Empty IN list: FALSE for IN, TRUE for NOT IN (SQLite extension),
    // regardless of collation.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b IN () ORDER BY c", &[]);
    assert_rows(&db, "SELECT c FROM t4a WHERE b NOT IN () ORDER BY c", &[1, 2, 3]);
}

// ---------------------------------------------------------------------------
// RTRIM collation
// ---------------------------------------------------------------------------

#[test]
fn rtrim_column_in_ignores_trailing_whitespace() {
    // sqlite3: CREATE TABLE t5(x TEXT COLLATE rtrim, y);
    // 'xyz' IN ('xyz   ') matches under RTRIM (both sides right-trimmed).
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t5(x TEXT COLLATE rtrim, y)");
    run_stmt(&mut db, "INSERT INTO t5 VALUES('xyz',1)");
    run_stmt(&mut db, "INSERT INTO t5 VALUES('abc   ',2)");
    run_stmt(&mut db, "INSERT INTO t5 VALUES('def',3)");

    assert_rows(&db, "SELECT y FROM t5 WHERE x IN ('xyz   ')", &[1]);
    assert_rows(&db, "SELECT y FROM t5 WHERE x IN ('abc')", &[2]);
    // Case still matters under RTRIM.
    assert_rows(&db, "SELECT y FROM t5 WHERE x IN ('XYZ   ')", &[]);
    // HashSet path.
    assert_rows(&db, "SELECT y FROM t5 WHERE x IN ('abc','qrs','tuv','wxy') ORDER BY y", &[2]);
    // NOT IN complement.
    assert_rows(&db, "SELECT y FROM t5 WHERE x NOT IN ('xyz   ','abc','q','r') ORDER BY y", &[3]);
}

// ---------------------------------------------------------------------------
// Composition with affinity coercion (transform applies AFTER affinity)
// ---------------------------------------------------------------------------

#[test]
fn nocase_text_column_with_numeric_list_elements() {
    // TEXT affinity coerces numeric list elements to strings before the
    // collation transform; numbers have no case so matching is unaffected,
    // but the path must not panic or mis-match.
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t6(x TEXT COLLATE nocase, y)");
    run_stmt(&mut db, "INSERT INTO t6 VALUES('12',1)");
    run_stmt(&mut db, "INSERT INTO t6 VALUES('AbC',2)");

    assert_rows(&db, "SELECT y FROM t6 WHERE x IN (12)", &[1]);
    assert_rows(&db, "SELECT y FROM t6 WHERE x IN (12, 'aBc') ORDER BY y", &[1, 2]);
}
