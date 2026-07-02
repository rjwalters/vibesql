//! Regression tests for issue #5792: comparison collation resolution
//! (SQLite datatype3.html section 7.1) — `in4.test` subtests `in4-4.*`.
//!
//! Two defects are covered:
//!
//! - Defect A (slow path): implicit column collation must NOT propagate
//!   through `||` (or other operators/functions) — `(b||'')` has no
//!   collating sequence even when `b` is declared `COLLATE NOCASE`, so
//!   `(a||'')=(b||'')` compares BINARY.
//! - Defect B (fast paths): compiled/vectorized/columnar predicates compare
//!   raw values, so compilation must be declined for columns with a
//!   non-BINARY collation and fall back to the collation-aware evaluator
//!   (`b=a` must apply NOCASE; `b='XYZ'` must match 'xyz').
//!
//! Expected values verified against sqlite3 (and `in4.test` lines 255-292).

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

// ---------------------------------------------------------------------------
// The in4-4.* block (column vs column, concat-wrapped operands)
// ---------------------------------------------------------------------------

#[test]
fn in4_4_1_left_binary_column_blocks_right_nocase() {
    // a = b: `a` IS a column, so its default BINARY collation applies with
    // left precedence — b's NOCASE must not be used.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE a=b ORDER BY c", &[3]);
}

#[test]
fn in4_4_2_left_nocase_column_wins() {
    // b = a: left column's NOCASE collation applies ('ABC' matches 'abc').
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b=a ORDER BY c", &[1, 3]);
}

#[test]
fn in4_4_3_concat_left_falls_through_to_right_column() {
    // (a||'') = b: the left operand is not a column (no collation), so the
    // right column's NOCASE applies.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE (a||'')=b ORDER BY c", &[1, 3]);
}

#[test]
fn in4_4_4_nocase_does_not_leak_through_concat() {
    // (a||'') = (b||''): neither operand has a collating sequence — b's
    // NOCASE must NOT propagate through `||` — so the comparison is BINARY.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE (a||'')=(b||'') ORDER BY c", &[3]);
}

#[test]
fn in4_4_5_in_single_element_uses_lhs_collation() {
    // a IN (b): IN uses the LHS collation (BINARY for column a).
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE a IN (b) ORDER BY c", &[3]);
}

#[test]
fn in4_4_6_concat_lhs_in_is_binary() {
    // (a||'') IN (b): LHS has no collation -> BINARY.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE (a||'') IN (b) ORDER BY c", &[3]);
}

// ---------------------------------------------------------------------------
// Column vs literal (fast-path decline coverage)
// ---------------------------------------------------------------------------

#[test]
fn nocase_column_vs_literal_matches_case_insensitively() {
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b='XYZ' ORDER BY c", &[2]);
    // Reversed operand order behaves the same.
    assert_rows(&db, "SELECT c FROM t4a WHERE 'XYZ'=b ORDER BY c", &[2]);
}

#[test]
fn binary_column_vs_literal_stays_case_sensitive() {
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE a='abc' ORDER BY c", &[]);
    assert_rows(&db, "SELECT c FROM t4a WHERE a='ABC' ORDER BY c", &[1]);
}

#[test]
fn nocase_column_inequality_uses_collation() {
    // b <> 'XYZ' must exclude the nocase-equal row 2.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b<>'XYZ' ORDER BY c", &[1, 3]);
}

// ---------------------------------------------------------------------------
// Explicit COLLATE and rule-2 wrappers (unary +, CAST)
// ---------------------------------------------------------------------------

#[test]
fn explicit_collate_on_right_beats_implicit_left() {
    // a = (b COLLATE nocase): explicit COLLATE (rule 1) wins over the left
    // column's implicit BINARY.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE a=(b COLLATE nocase) ORDER BY c", &[1, 3]);
}

#[test]
fn explicit_collate_propagates_through_concat() {
    // (a COLLATE nocase)||'' keeps the explicit collation, so the comparison
    // is NOCASE even though both operands are concat expressions.
    let db = db_with_t4a();
    assert_rows(
        &db,
        "SELECT c FROM t4a WHERE ((a COLLATE nocase)||'')=(b||'') ORDER BY c",
        &[1, 3],
    );
}

#[test]
fn unary_plus_preserves_column_collation() {
    // +b is still "column b" for collation purposes (datatype3 §7.1 rule 2).
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE +b=a ORDER BY c", &[1, 3]);
}

#[test]
fn cast_preserves_column_collation() {
    // CAST(b AS TEXT) is still "column b" for collation purposes.
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE CAST(b AS TEXT)=a ORDER BY c", &[1, 3]);
}

// ---------------------------------------------------------------------------
// BETWEEN on a collated column (vectorized fast path must decline)
// ---------------------------------------------------------------------------

#[test]
fn between_on_nocase_column_applies_collation() {
    // b BETWEEN 'XYZ' AND 'XYZ' is equivalent to b='XYZ' -> row 2 (nocase).
    let db = db_with_t4a();
    assert_rows(&db, "SELECT c FROM t4a WHERE b BETWEEN 'XYZ' AND 'XYZ' ORDER BY c", &[2]);
}
