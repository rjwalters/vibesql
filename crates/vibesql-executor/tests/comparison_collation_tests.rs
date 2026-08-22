//! Regression tests for issue #5792: comparison collation resolution
//! (SQLite datatype3.html section 7.1) — `in4.test` subtests `in4-4.*`.
//!
//! Two defects are covered:
//!
//! - Defect A (slow path): implicit column collation must NOT propagate through `||` (or other
//!   operators/functions) — `(b||'')` has no collating sequence even when `b` is declared `COLLATE
//!   NOCASE`, so `(a||'')=(b||'')` compares BINARY.
//! - Defect B (fast paths): compiled/vectorized/columnar predicates compare raw values, so
//!   compilation must be declined for columns with a non-BINARY collation and fall back to the
//!   collation-aware evaluator (`b=a` must apply NOCASE; `b='XYZ'` must match 'xyz').
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
        vibesql_ast::Statement::CreateView(view) => {
            vibesql_executor::advanced_objects::execute_create_view(&view, db).unwrap();
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
                    // A bare comparison (`B < a`) yields a Boolean; SQLite
                    // surfaces these as 0/1 integers.
                    SqlValue::Boolean(b) => i64::from(*b),
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

// ---------------------------------------------------------------------------
// Issue #5864: view column collation must propagate to outer-query comparisons
// (SQLite ticket a7debbe0ad1, `tkt-a7debbe0.test` subtests 1.2.3 / 1.2.4).
//
// A view materialized its runtime schema with `collation: None` for every
// column, so `explicit COLLATE` (and an underlying column's declared
// collation) in the view body was silently dropped. Expected values below
// are all verified against sqlite3 3.51.0.
// ---------------------------------------------------------------------------

/// Base table the tkt-a7debbe0 views select from.
fn db_with_t0() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t0(xyz INTEGER)");
    run_stmt(&mut db, "INSERT INTO t0(xyz) VALUES(456)");
    db
}

#[test]
fn view_explicit_collate_left_operand_reproducer() {
    // Reproducer from issue #5864 (tkt-a7debbe0 1.2.3):
    //   CREATE VIEW v2(a,B) AS SELECT 'a','B' COLLATE NOCASE FROM t0;
    //   SELECT B < a FROM v2  ->  0  (NOCASE: 'B' < 'a' is false)
    // Before the fix VibeSQL returned 1 (BINARY: 0x42 < 0x61).
    let mut db = db_with_t0();
    run_stmt(&mut db, "CREATE VIEW v2(a, B) AS SELECT 'a', 'B' COLLATE NOCASE FROM t0");
    assert_rows(&db, "SELECT B < a FROM v2", &[0]);
    // And the symmetric >= comparison (tkt-a7debbe0 1.1.3) stays 1.
    assert_rows(&db, "SELECT a >= B FROM v2", &[1]);
}

#[test]
fn view_explicit_collate_both_operands() {
    // tkt-a7debbe0 1.2.4:
    //   CREATE VIEW v3(a,B) AS SELECT 'a' COLLATE BINARY, 'B' COLLATE NOCASE FROM t0;
    //   SELECT B < a FROM v3  ->  0
    let mut db = db_with_t0();
    run_stmt(
        &mut db,
        "CREATE VIEW v3(a, B) AS SELECT 'a' COLLATE BINARY, 'B' COLLATE NOCASE FROM t0",
    );
    assert_rows(&db, "SELECT B < a FROM v3", &[0]);
    assert_rows(&db, "SELECT a >= B FROM v3", &[1]);
}

#[test]
fn view_explicit_collate_through_cast_and_concat() {
    // Explicit COLLATE propagates out through unary `+`, CAST, and `||`
    // (tkt-a7debbe0 v4/v5). With B on the left both must compare NOCASE -> 0.
    let mut db = db_with_t0();
    run_stmt(
        &mut db,
        "CREATE VIEW v4(a, B) AS SELECT 'a', +CAST('B' COLLATE NOCASE AS TEXT) FROM t0",
    );
    run_stmt(&mut db, "CREATE VIEW v5(a, B) AS SELECT 'a', ('B' COLLATE NOCASE) || '' FROM t0");
    assert_rows(&db, "SELECT B < a FROM v4", &[0]);
    assert_rows(&db, "SELECT B < a FROM v5", &[0]);
    // Plain column `a` on the left blocks the right's NOCASE -> BINARY -> 0.
    assert_rows(&db, "SELECT a < B FROM v4", &[0]);
    assert_rows(&db, "SELECT a < B FROM v5", &[0]);
}

#[test]
fn view_bare_column_ref_propagates_underlying_collation() {
    // A bare column reference to an underlying NOCASE column carries that
    // collation through the view (ticket a7debbe0ad1). Verified vs sqlite3:
    //   CREATE TABLE t2(a, B COLLATE NOCASE); INSERT VALUES('a','B');
    //   CREATE VIEW vc(a,B) AS SELECT a, B FROM t2;
    //   SELECT B < a FROM vc  ->  0
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t2(a TEXT, B TEXT COLLATE NOCASE)");
    run_stmt(&mut db, "INSERT INTO t2 VALUES('a', 'B')");
    run_stmt(&mut db, "CREATE VIEW vc(a, B) AS SELECT a, B FROM t2");
    assert_rows(&db, "SELECT B < a FROM vc", &[0]);
    assert_rows(&db, "SELECT a >= B FROM vc", &[1]);
}

#[test]
fn view_of_view_propagates_collation() {
    // Collation survives a second layer of view materialization.
    let mut db = db_with_t0();
    run_stmt(&mut db, "CREATE VIEW v2(a, B) AS SELECT 'a', 'B' COLLATE NOCASE FROM t0");
    run_stmt(&mut db, "CREATE VIEW v2b(a, B) AS SELECT a, B FROM v2");
    assert_rows(&db, "SELECT B < a FROM v2b", &[0]);
}

#[test]
fn view_plain_literal_column_is_binary() {
    // Regression: a view column that is a bare literal (no COLLATE) still
    // compares BINARY. 'A' = 'a' under BINARY is false -> 0.
    let mut db = db_with_t0();
    run_stmt(&mut db, "CREATE VIEW vlit(a, b) AS SELECT 'A', 'a' FROM t0");
    assert_rows(&db, "SELECT a = b FROM vlit", &[0]);
}

// ---------------------------------------------------------------------------
// Issue #5925: wildcard (`SELECT *`) view bodies must propagate the underlying
// column's declared collation to outer-query comparisons. Follow-up to #5864,
// which only aligned collation when the body's select-item count matched the
// output column count (explicit select lists). Expected values verified against
// sqlite3 3.51.0 (see the oracle in the issue body).
// ---------------------------------------------------------------------------

/// `t2(a TEXT, B TEXT COLLATE NOCASE)` seeded with one row `('a','B')` — the
/// fixture used by the sqlite3 oracle in issue #5925.
fn db_with_t2_nocase() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t2(a TEXT, B TEXT COLLATE NOCASE)");
    run_stmt(&mut db, "INSERT INTO t2 VALUES('a', 'B')");
    db
}

#[test]
fn view_wildcard_propagates_underlying_collation() {
    // Bare wildcard body: `SELECT * FROM t2`. The oracle:
    //   SELECT B < a FROM vstar  ->  0  (NOCASE: 'B' < 'a' is false)
    //   SELECT a >= B FROM vstar ->  1
    // Before this fix VibeSQL degraded every wildcard column to BINARY and
    // returned 1 / 0 respectively.
    let mut db = db_with_t2_nocase();
    run_stmt(&mut db, "CREATE VIEW vstar AS SELECT * FROM t2");
    assert_rows(&db, "SELECT B < a FROM vstar", &[0]);
    assert_rows(&db, "SELECT a >= B FROM vstar", &[1]);
}

#[test]
fn view_qualified_wildcard_propagates_underlying_collation() {
    // Qualified wildcard body: `SELECT t2.* FROM t2` behaves identically.
    let mut db = db_with_t2_nocase();
    run_stmt(&mut db, "CREATE VIEW vqstar AS SELECT t2.* FROM t2");
    assert_rows(&db, "SELECT B < a FROM vqstar", &[0]);
    assert_rows(&db, "SELECT a >= B FROM vqstar", &[1]);
}

#[test]
fn view_of_wildcard_view_propagates_collation() {
    // Two levels of nesting: an explicit-select view over a wildcard view.
    // NOCASE must still reach the outer comparison.
    let mut db = db_with_t2_nocase();
    run_stmt(&mut db, "CREATE VIEW vstar AS SELECT * FROM t2");
    run_stmt(&mut db, "CREATE VIEW vstar2 AS SELECT a, B FROM vstar");
    assert_rows(&db, "SELECT B < a FROM vstar2", &[0]);
    assert_rows(&db, "SELECT a >= B FROM vstar2", &[1]);
}

#[test]
fn view_of_wildcard_of_wildcard_view_propagates_collation() {
    // Three levels: wildcard over wildcard over base table.
    let mut db = db_with_t2_nocase();
    run_stmt(&mut db, "CREATE VIEW vstar AS SELECT * FROM t2");
    run_stmt(&mut db, "CREATE VIEW vstar2 AS SELECT * FROM vstar");
    assert_rows(&db, "SELECT B < a FROM vstar2", &[0]);
    assert_rows(&db, "SELECT a >= B FROM vstar2", &[1]);
}

#[test]
fn view_explicit_column_list_over_wildcard_body_propagates_collation() {
    // Explicit view column list + wildcard body. `view_output_column_index`
    // positions the column via `view.columns`, but the derived collation is
    // `[None]`; the Path 2 fallback resolves it by name from the inner FROM.
    let mut db = db_with_t2_nocase();
    run_stmt(&mut db, "CREATE VIEW vstar (a, B) AS SELECT * FROM t2");
    assert_rows(&db, "SELECT B < a FROM vstar", &[0]);
    assert_rows(&db, "SELECT a >= B FROM vstar", &[1]);
}

#[test]
fn view_multi_join_wildcard_propagates_single_collated_column() {
    // Multi-table wildcard body where only one joined table has a collated
    // column. `find_collation_in_from` recurses through the JOIN and resolves
    // `B` from t2 while `x`/`y` stay BINARY.
    let mut db = db_with_t2_nocase();
    run_stmt(&mut db, "CREATE TABLE t1(x TEXT, y TEXT)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES('a', 'a')");
    run_stmt(&mut db, "CREATE VIEW vjoin AS SELECT * FROM t1 JOIN t2 ON t1.x = t2.a");
    // B (NOCASE) vs a (from t2) -> NOCASE -> 0.
    assert_rows(&db, "SELECT B < a FROM vjoin", &[0]);
    // x and y are plain BINARY columns from t1: 'a' = 'a' -> 1.
    assert_rows(&db, "SELECT x = y FROM vjoin", &[1]);
}

#[test]
fn view_wildcard_simple_retrieval_unchanged() {
    // Plain row retrieval from a wildcard view (no outer comparison) still
    // returns the stored values unchanged.
    let mut db = db_with_t2_nocase();
    run_stmt(&mut db, "CREATE VIEW vstar AS SELECT * FROM t2");
    let stmt = vibesql_parser::Parser::parse_sql("SELECT a, B FROM vstar").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 1);
        let as_str = |v: &SqlValue| -> String {
            match v {
                SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                other => panic!("Expected text value, got {:?}", other),
            }
        };
        assert_eq!(as_str(&rows[0].values[0]), "a");
        assert_eq!(as_str(&rows[0].values[1]), "B");
    } else {
        panic!("Expected SELECT");
    }
}
