//! Tests for the remaining row-value features from issue #5790:
//!
//! - Row-value `IN (list)` / `NOT IN (list)` with SQLite three-valued NULL
//!   semantics and per-column affinity.
//! - "row value misused" errors for row values in scalar contexts (compared
//!   against scalars, bare in a SELECT list, in ORDER BY / GROUP BY, mixed
//!   into BETWEEN), raised at prepare time even for empty tables.
//! - COLLATE (explicit and column-declared) honored inside row-value
//!   comparison, including the tuple-vs-subquery form.
//! - Row-value simple CASE (`CASE (2,2) WHEN (1,1) THEN ... END`).
//! - Row-value BETWEEN with a scalar-subquery operand and in multi-table
//!   (combined evaluator) contexts.
//! - Multi-column scalar subquery on the LHS of IN.
//! - `IN (VALUES(...))` table value constructors.
//! - Nested row values inside IS.
//! - Join WHERE-clause conjunct coverage: predicates the columnar join path
//!   cannot consume (e.g. `+col == other`) must not be silently dropped.
//! - Outer join (LEFT / RIGHT / FULL) with a row-value equality in the ON
//!   clause: an unmatched row-value predicate must null-extend the other side
//!   instead of dropping the driving row (rowvalue.test section 12).
//! - Scalar-subquery row on the LHS of `IN (subquery)` combined with a join
//!   (rowvalue.test section 18.4).
//!
//! Expected values verified against SQLite (rowvalue.test / rowvalue2.test).

use vibesql_executor::{ExecutorError, SelectExecutor};
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

/// Run a SELECT that yields a single scalar value; normalize Boolean to 0/1
/// Integer (SQLite has no boolean storage class).
fn query_scalar(db: &vibesql_storage::Database, sql: &str) -> SqlValue {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
        assert_eq!(rows.len(), 1, "Expected exactly one row for: {}", sql);
        assert_eq!(rows[0].values.len(), 1, "Expected exactly one column for: {}", sql);
        match &rows[0].values[0] {
            SqlValue::Boolean(b) => SqlValue::Integer(*b as i64),
            other => other.clone(),
        }
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn assert_scalar(db: &vibesql_storage::Database, sql: &str, expected: SqlValue) {
    let actual = query_scalar(db, sql);
    assert_eq!(actual, expected, "Query: {} -- expected {:?}, got {:?}", sql, expected, actual);
}

/// Run a SELECT and return every row as a vector of column values.
fn query_rows(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
        rows.iter().map(|r| r.values.to_vec()).collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

/// Run a SELECT and return the first column of every row.
fn query_column(db: &vibesql_storage::Database, sql: &str) -> Vec<SqlValue> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
        rows.iter().map(|r| r.values[0].clone()).collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

/// Run a SELECT expecting an error; return it.
fn query_err(db: &vibesql_storage::Database, sql: &str) -> ExecutorError {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        match executor.execute(&select_stmt) {
            Ok(_) => panic!("Expected error for: {}", sql),
            Err(e) => e,
        }
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn assert_misused(db: &vibesql_storage::Database, sql: &str) {
    let err = query_err(db, sql);
    assert!(
        matches!(err, ExecutorError::RowValueMisused),
        "Query: {} -- expected RowValueMisused, got {:?}",
        sql,
        err
    );
}

fn assert_in_element_arity(
    db: &vibesql_storage::Database,
    sql: &str,
    expected: usize,
    actual: usize,
) {
    let err = query_err(db, sql);
    assert!(
        matches!(err, ExecutorError::InElementArity { expected: e, actual: a } if e == expected && a == actual),
        "Query: {} -- expected InElementArity {{ expected: {}, actual: {} }}, got {:?}",
        sql,
        expected,
        actual,
        err
    );
}

const I: fn(i64) -> SqlValue = SqlValue::Integer;

/// Build a text `SqlValue` matching how the executor materializes a string
/// literal (`'x'`).
fn txt(s: &str) -> SqlValue {
    SqlValue::Varchar(arcstr::ArcStr::from(s))
}

// ─── Row-value IN (list) ────────────────────────────────────────────────────

#[test]
fn tuple_in_list_matches() {
    let db = vibesql_storage::Database::new();
    assert_scalar(&db, "SELECT (1,2) IN ((1,2),(3,4))", I(1));
    assert_scalar(&db, "SELECT (3,4) IN ((1,2),(3,4))", I(1));
    assert_scalar(&db, "SELECT (1,3) IN ((1,2),(3,4))", I(0));
    assert_scalar(&db, "SELECT (1,2) NOT IN ((1,2),(3,4))", I(0));
    assert_scalar(&db, "SELECT (5,6) NOT IN ((1,2),(3,4))", I(1));
}

#[test]
fn tuple_in_list_null_semantics() {
    let db = vibesql_storage::Database::new();
    // Partial match with NULL in the undecided position → UNKNOWN.
    assert_scalar(&db, "SELECT (1, NULL) IN ((1, 2))", SqlValue::Null);
    // A definitively unequal element makes the row FALSE, not UNKNOWN.
    assert_scalar(&db, "SELECT (1, NULL) IN ((2, 2))", I(0));
    // NULL on the candidate side behaves the same way.
    assert_scalar(&db, "SELECT (1, 2) IN ((1, NULL))", SqlValue::Null);
    assert_scalar(&db, "SELECT (1, 2) IN ((2, NULL))", I(0));
    // A definite match wins even when other candidates have NULLs.
    assert_scalar(&db, "SELECT (1, 2) IN ((1, NULL), (1, 2))", I(1));
    // NOT IN flips definite results; UNKNOWN stays NULL.
    assert_scalar(&db, "SELECT (1, NULL) NOT IN ((1, 2))", SqlValue::Null);
    assert_scalar(&db, "SELECT (1, NULL) NOT IN ((2, 2))", I(1));
}

#[test]
fn tuple_in_list_column_affinity() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a TEXT, b INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES('12', 3)");
    // TEXT-affinity column vs integer literal in the tuple: the literal is
    // coerced to text (mirrors scalar `a = 12`).
    assert_scalar(&db, "SELECT (a, b) IN ((12, 3)) FROM t", I(1));
    assert_scalar(&db, "SELECT (a, b) IN (('12', 3)) FROM t", I(1));
    assert_scalar(&db, "SELECT (a, b) IN (('12', '3')) FROM t", I(1));
}

#[test]
fn tuple_in_list_arity_mismatch_reports_element_arity() {
    let db = vibesql_storage::Database::new();
    // SQLite reports the element-arity error for a mismatched IN candidate
    // ("IN(...) element has N term(s) - expected M"), not a generic misuse.
    assert_in_element_arity(&db, "SELECT (1,2) IN ((1,2,3))", 2, 3);
    // A bare-scalar candidate has arity 1.
    assert_in_element_arity(&db, "SELECT (1,2) IN ((1,2), 3)", 2, 1);
}

#[test]
fn tuple_in_empty_list() {
    let db = vibesql_storage::Database::new();
    assert_scalar(&db, "SELECT (1,2) IN ()", I(0));
    assert_scalar(&db, "SELECT (1,2) NOT IN ()", I(1));
}

// ─── "row value misused" errors ─────────────────────────────────────────────

#[test]
fn row_value_vs_scalar_comparison_errors_even_on_empty_table() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t11(a)");
    // No rows: the error must still surface (SQLite raises at prepare time).
    for op in ["<=", "<", ">=", ">", "==", "<>"] {
        assert_misused(&db, &format!("SELECT * FROM t11 WHERE (a,a) {} 1", op));
    }
    assert_misused(&db, "SELECT * FROM t11 WHERE (a,a) IS 1");
    assert_misused(&db, "SELECT * FROM t11 WHERE (a,a) IS NOT 1");
}

#[test]
fn bare_row_value_contexts_are_misused() {
    let db = vibesql_storage::Database::new();
    assert_misused(&db, "SELECT (1,2) AS x WHERE x=3");
    assert_misused(&db, "SELECT (1,2) BETWEEN 1 AND 2");
    assert_misused(&db, "SELECT 1 BETWEEN (1,2) AND 2");
    assert_misused(&db, "SELECT 2 BETWEEN 1 AND (1,2)");
    assert_misused(&db, "SELECT (1,2) FROM (SELECT 1) ORDER BY 1");
    assert_misused(&db, "SELECT (1,2) FROM (SELECT 1) GROUP BY 1");
}

#[test]
fn row_value_compared_to_collated_subquery_is_misused() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE hh(a, b, c)");
    run_stmt(&mut db, "INSERT INTO hh VALUES('abc', 1, 'i')");
    // COLLATE applied to the subquery makes the RHS a scalar context.
    assert_misused(&db, "SELECT c FROM hh WHERE (a, b) = (SELECT 'abc', 1) COLLATE nocase");
    assert_misused(&db, "SELECT c FROM hh WHERE (a, b) = 1");
}

// ─── COLLATE inside row-value comparison ────────────────────────────────────

#[test]
fn explicit_collate_in_row_value_comparison() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t0(c0)");
    run_stmt(&mut db, "INSERT INTO t0(c0) VALUES('a')");
    // rowvalue.test 25.10–25.40: NOCASE from either element side.
    assert_scalar(&db, "SELECT (t0.c0, 0) < ('B' COLLATE NOCASE, 0) FROM t0", I(1));
    assert_scalar(&db, "SELECT ('B' COLLATE NOCASE, 0) > (t0.c0, 0) FROM t0", I(1));
    assert_scalar(&db, "SELECT ('B', 0) > (t0.c0 COLLATE nocase, 0) FROM t0", I(1));
    assert_scalar(&db, "SELECT (t0.c0 COLLATE nocase, 0) < ('B', 0) FROM t0", I(1));
    // Without the collation, binary comparison flips the result ('a' > 'B').
    assert_scalar(&db, "SELECT (t0.c0, 0) < ('B', 0) FROM t0", I(0));
}

#[test]
fn collate_in_row_value_vs_subquery() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE hh(a, b, c)");
    for row in [
        "('abc', 1, 'i')",
        "('ABC', 1, 'ii')",
        "('def', 2, 'iii')",
        "('DEF', 2, 'iv')",
        "('GHI', 3, 'v')",
        "('ghi', 3, 'vi')",
    ] {
        run_stmt(&mut db, &format!("INSERT INTO hh VALUES{}", row));
    }
    // rowvalue.test 6.8 / 6.9.
    let rows =
        query_column(&db, "SELECT c FROM hh WHERE (a COLLATE nocase, b) = (SELECT 'def', 2)");
    assert_eq!(
        rows,
        vec![SqlValue::Varchar("iii".into()), SqlValue::Varchar("iv".into())],
        "6.8: NOCASE must apply to the first tuple element"
    );
    let rows =
        query_column(&db, "SELECT c FROM hh WHERE (a COLLATE nocase, b) IS NOT (SELECT 'def', 2)");
    assert_eq!(
        rows,
        vec![
            SqlValue::Varchar("i".into()),
            SqlValue::Varchar("ii".into()),
            SqlValue::Varchar("v".into()),
            SqlValue::Varchar("vi".into())
        ],
        "6.9: NOCASE must apply to IS NOT against a subquery"
    );
}

// ─── Row-value CASE / BETWEEN ───────────────────────────────────────────────

#[test]
fn row_value_simple_case() {
    let db = vibesql_storage::Database::new();
    assert_scalar(&db, "SELECT CASE (2,2) WHEN (1,1) THEN 2 ELSE 1 END", I(1));
    assert_scalar(&db, "SELECT CASE (2,2) WHEN (2,2) THEN 2 ELSE 1 END", I(2));
    // Subquery operand (rowvalue.test 14.3).
    assert_scalar(&db, "SELECT CASE (SELECT 2,2) WHEN (1,1) THEN 2 ELSE 1 END", I(1));
    assert_scalar(&db, "SELECT CASE (SELECT 2,2) WHEN (2,2) THEN 2 ELSE 1 END", I(2));
}

#[test]
fn row_value_between_with_subquery_operand() {
    let db = vibesql_storage::Database::new();
    // rowvalue.test 14.4.
    assert_scalar(&db, "SELECT (SELECT 2,2) BETWEEN (1,1) AND (3,3)", I(1));
    assert_scalar(&db, "SELECT (SELECT 4,2) BETWEEN (1,1) AND (3,3)", I(0));
}

#[test]
fn row_value_between_in_where_clause() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t12(x)");
    run_stmt(&mut db, "INSERT INTO t12 VALUES(2)");
    run_stmt(&mut db, "INSERT INTO t12 VALUES(4)");
    // rowvalue.test 14.5 / 14.6: the predicate must not be dropped by the
    // columnar filter path.
    let rows = query_column(&db, "SELECT 1 FROM t12 WHERE (x,1) BETWEEN (1,1) AND (3,3)");
    assert_eq!(rows, vec![I(1)], "14.5: only x=2 satisfies the row-value BETWEEN");
    let rows = query_column(&db, "SELECT 1 FROM t12 WHERE (1,x) BETWEEN (1,1) AND (3,3)");
    assert_eq!(rows, vec![I(1), I(1)], "14.6: both rows satisfy the row-value BETWEEN");
}

// ─── Multi-column subquery LHS of IN / IN (VALUES ...) ─────────────────────

#[test]
fn multi_column_subquery_lhs_of_in() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE b3(a, b)");
    run_stmt(&mut db, "CREATE TABLE b5(a, b)");
    run_stmt(&mut db, "INSERT INTO b3 VALUES(1, 1)");
    run_stmt(&mut db, "INSERT INTO b3 VALUES(1, 2)");
    run_stmt(&mut db, "INSERT INTO b5 VALUES(1, 1)");
    run_stmt(&mut db, "INSERT INTO b5 VALUES(1, 2)");
    // rowvalue.test 18.1.
    let rows =
        query_column(&db, "SELECT a FROM b3 WHERE (SELECT b3.a, b3.b) IN (SELECT a, b FROM b5)");
    assert_eq!(rows, vec![I(1), I(1)]);
    // rowvalue.test 22.100: compound subquery LHS takes its first row.
    assert_scalar(&db, "SELECT (SELECT 3,4 UNION SELECT 5,6 ORDER BY 1) IN (SELECT 3,4)", I(1));
    assert_scalar(&db, "SELECT (SELECT 3,4 UNION SELECT 5,6 ORDER BY 1) IN (SELECT 5,6)", I(0));
    assert_scalar(
        &db,
        "SELECT (SELECT 3,4 UNION SELECT 5,6 ORDER BY 1 DESC) IN (SELECT 5,6)",
        I(1),
    );
}

#[test]
fn in_values_table_constructor() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(a, b)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES(1, 2)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES(3, 4)");
    // rowvalue.test 21.0-style.
    let rows = query_column(&db, "SELECT a FROM t1 WHERE (a,b) IN (VALUES(1,2))");
    assert_eq!(rows, vec![I(1)]);
    // Scalar LHS with VALUES.
    assert_scalar(&db, "SELECT 1 IN (VALUES(1))", I(1));
    assert_scalar(&db, "SELECT 2 IN (VALUES(1))", I(0));
}

// ─── Nested row values inside IS ────────────────────────────────────────────

#[test]
fn nested_row_value_is() {
    let db = vibesql_storage::Database::new();
    // rowvalue.test 20.1.
    assert_scalar(&db, "SELECT (2,(2,0)) IS (2,(2,0))", I(1));
    assert_scalar(&db, "SELECT (2,(2,0)) IS (2,(2,1))", I(0));
    assert_scalar(&db, "SELECT (2,(2,NULL)) IS (2,(2,NULL))", I(1));
}

// ─── Join WHERE conjunct coverage (unary-plus predicate must not drop) ─────

#[test]
fn join_where_conjunct_with_unary_plus_not_dropped() {
    let mut db = vibesql_storage::Database::new();
    // rowvalue2.test section 5 fixture.
    run_stmt(&mut db, "CREATE TABLE r1(a TEXT, iB TEXT)");
    run_stmt(&mut db, "CREATE TABLE r2(x TEXT, zY INTEGER)");
    run_stmt(&mut db, "INSERT INTO r1 VALUES(35, 35)");
    run_stmt(&mut db, "INSERT INTO r2 VALUES(35, 36)");
    run_stmt(&mut db, "INSERT INTO r2 VALUES(35, 4)");
    run_stmt(&mut db, "INSERT INTO r2 VALUES(35, 35)");

    // The scalar-expanded form: `+zY` strips affinity, so the INTEGER value is
    // converted to TEXT for comparison with the TEXT column iB — only the
    // zY=35 row matches. Before the coverage check, the columnar join dropped
    // the second conjunct and returned all three rows.
    let rows =
        query_column(&db, "SELECT zY FROM r1, r2 WHERE (x == a) AND (+zY == iB) ORDER BY zY");
    assert_eq!(rows, vec![I(35)]);

    // The row-value form must agree with the scalar-expanded form.
    let rows = query_column(&db, "SELECT zY FROM r1, r2 WHERE (x, +zY) == (a, iB) ORDER BY zY");
    assert_eq!(rows, vec![I(35)]);
}

// ─── Outer join with a row-value ON constraint (null-extension) ─────────────
//
// rowvalue.test section 12 (ticket fef4bb4bd9185ec8f): when a row-value
// equality in an outer-join ON clause does not match, the unmatched side must
// be null-extended rather than dropping the driving row entirely.

fn make_t1_t2(db: &mut vibesql_storage::Database) {
    run_stmt(db, "CREATE TABLE t1(a INT, b INT)");
    run_stmt(db, "INSERT INTO t1 VALUES(1, 2)");
    run_stmt(db, "CREATE TABLE t2(x INT, y INT)");
    run_stmt(db, "INSERT INTO t2 VALUES(3, 4)");
}

#[test]
fn left_join_row_value_on_null_extends() {
    let mut db = vibesql_storage::Database::new();
    make_t1_t2(&mut db);
    // rowvalue.test 12.1: {1 2 {} {} x}. The ON row-value never matches, so the
    // t2 columns null-extend and the t1 row is preserved.
    let rows = query_rows(&db, "SELECT a, b, x, y, 'x' FROM t1 LEFT JOIN t2 ON (a,b)=(x,y)");
    assert_eq!(rows, vec![vec![I(1), I(2), SqlValue::Null, SqlValue::Null, txt("x"),]]);
}

#[test]
fn right_join_row_value_on_null_extends() {
    let mut db = vibesql_storage::Database::new();
    make_t1_t2(&mut db);
    // rowvalue.test 12.2: {1 2 - -}. t1 is the preserved side of the RIGHT JOIN.
    let rows =
        query_rows(&db, "SELECT t1.a, t1.b, t2.x, t2.y FROM t2 RIGHT JOIN t1 ON (a,b)=(x,y)");
    assert_eq!(rows, vec![vec![I(1), I(2), SqlValue::Null, SqlValue::Null]]);
}

#[test]
fn full_join_row_value_on_null_extends() {
    let mut db = vibesql_storage::Database::new();
    make_t1_t2(&mut db);
    // rowvalue.test 12.3: both rows are null-extended because the row-value ON
    // predicate never matches — {1 2 - -} and {- - 3 4}.
    let rows = query_rows(
        &db,
        "SELECT t1.a, t1.b, t2.x, t2.y FROM t1 FULL JOIN t2 ON (a,b)=(x,y) \
         ORDER BY coalesce(a, x)",
    );
    assert_eq!(
        rows,
        vec![
            vec![I(1), I(2), SqlValue::Null, SqlValue::Null],
            vec![SqlValue::Null, SqlValue::Null, I(3), I(4)],
        ]
    );
}

#[test]
fn scalar_subquery_lhs_of_in_with_join() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE b3(a, b, PRIMARY KEY(a, b))");
    run_stmt(&mut db, "CREATE TABLE b4(a)");
    run_stmt(&mut db, "CREATE TABLE b5(a, b)");
    run_stmt(&mut db, "INSERT INTO b3 VALUES(1, 1)");
    run_stmt(&mut db, "INSERT INTO b3 VALUES(1, 2)");
    run_stmt(&mut db, "INSERT INTO b4 VALUES(1)");
    run_stmt(&mut db, "INSERT INTO b5 VALUES(1, 1)");
    run_stmt(&mut db, "INSERT INTO b5 VALUES(1, 2)");
    // rowvalue.test 18.4: a scalar-subquery row on the LHS of IN combined with a
    // join — {1 1 1} and {1 2 1}.
    let rows = query_rows(
        &db,
        "SELECT * FROM b3 JOIN b4 ON b4.a = b3.a \
         WHERE (SELECT b3.a, b3.b) IN (SELECT a, b FROM b5)",
    );
    assert_eq!(rows, vec![vec![I(1), I(1), I(1)], vec![I(1), I(2), I(1)]]);
}

// ─── SELECT-WHERE row-value / subquery arity misuse (#6079) ──────────────────
//
// A row-value arity misuse in a top-level SELECT WHERE clause must error at
// prepare time (even for an empty table) instead of silently returning 0 rows.
// SQLite 3.51 parity verified: a scalar compared against a multi-column subquery
// in a SELECT WHERE is `row value misused`; a nested scalar-vs-subquery arity
// misuse is `sub-select returns N columns - expected 1`.

fn assert_subquery_arity(
    db: &vibesql_storage::Database,
    sql: &str,
    expected: usize,
    actual: usize,
) {
    let err = query_err(db, sql);
    assert!(
        matches!(err, ExecutorError::SubqueryColumnCountMismatch { expected: e, actual: a } if e == expected && a == actual),
        "Query: {} -- expected SubqueryColumnCountMismatch {{ expected: {}, actual: {} }}, got {:?}",
        sql,
        expected,
        actual,
        err
    );
}

#[test]
fn select_where_scalar_vs_multicol_subquery_is_misused() {
    // Issue #6079 repro and rowvalue4.test 8.2: a plain scalar compared against a
    // multi-column subquery in a SELECT WHERE is `row value misused`, on an empty
    // table, regardless of operator or which side the subquery is on.
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a, b)");
    assert_misused(&db, "SELECT a FROM t WHERE a < (SELECT b, 2 FROM t)");
    assert_misused(&db, "SELECT a FROM t WHERE a = (SELECT b, 2 FROM t)");
    assert_misused(&db, "SELECT a FROM t WHERE (SELECT b, 2 FROM t) > a");
    // rowvalue4.test 8.2 exact shape: the misuse is inside one OR/AND branch.
    run_stmt(&mut db, "CREATE TABLE c1(x, y)");
    run_stmt(&mut db, "CREATE TABLE c2(a, b, c)");
    run_stmt(&mut db, "CREATE TABLE c3(d)");
    assert_misused(
        &db,
        "SELECT * FROM c2 CROSS JOIN c3 WHERE \
         ((a, b) == (SELECT x, y FROM c1) AND c3.d = c) OR \
         (c == (SELECT x, y FROM c1) AND c3.d = c)",
    );
}

#[test]
fn select_where_nested_in_subquery_arity_misuse() {
    // rowvalue9.test 8.2: the arity misuse lives inside the RHS subquery's
    // projection (`2 IN (SELECT 2,2)`), which never executes for an empty table.
    // It must still surface as `sub-select returns 2 columns - expected 1`.
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(a, b)");
    assert_subquery_arity(
        &db,
        "SELECT a FROM t1 WHERE (a, b) > (SELECT 2 IN (SELECT 2, 2), 2)",
        1,
        2,
    );
}

#[test]
fn select_where_valid_row_value_comparisons_not_flagged() {
    // No false positives: legal row-value / subquery WHERE comparisons keep
    // working on a populated table.
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a, b, c)");
    run_stmt(&mut db, "INSERT INTO t VALUES(1, 2, 3)");
    run_stmt(&mut db, "INSERT INTO t VALUES(4, 5, 6)");
    // row value vs matched-arity subquery / row value
    assert_eq!(query_column(&db, "SELECT a FROM t WHERE (a, b) = (SELECT 1, 2)"), vec![I(1)]);
    assert_eq!(query_column(&db, "SELECT a FROM t WHERE (a, b) < (2, 3)"), vec![I(1)]);
    // scalar vs single-column subquery (arity 1) — legal
    assert_eq!(query_column(&db, "SELECT a FROM t WHERE a = (SELECT 1)"), vec![I(1)]);
    // multi-column subquery LHS of IN vs matched-arity IN subquery — legal
    assert_eq!(
        query_rows(&db, "SELECT a FROM t WHERE (SELECT a, b) IN (SELECT a, b FROM t)"),
        vec![vec![I(1)], vec![I(4)]]
    );
    // row value IN subquery — legal
    assert_eq!(
        query_rows(&db, "SELECT a FROM t WHERE (a, b) IN (SELECT a, b FROM t)"),
        vec![vec![I(1)], vec![I(4)]]
    );
}
