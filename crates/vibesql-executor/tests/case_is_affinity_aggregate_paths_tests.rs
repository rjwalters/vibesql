//! Affinity-aware CASE / IS DISTINCT FROM in the aggregate, ROLLUP, and
//! columnar-HAVING evaluation paths.
//!
//! The main expression evaluators (`evaluator/expressions/*`,
//! `evaluator/combined/*`, `evaluator/arena.rs`) were fixed so that CASE's
//! simple form and scalar `IS`/`IS NOT` no longer guess numeric<->text
//! equality without SQLite column affinity (datatype3 §4.2, e_expr-23.1.6:
//! `CASE 55 WHEN '55' THEN 'A' ELSE 'B' END` -> 'B'; `55 IS '55'` -> 0).
//!
//! There are three *other* reachable evaluators that reimplement the same
//! CASE / IS-DISTINCT-FROM logic and were initially missed:
//!   1. aggregation/evaluation/case.rs      — CASE with an aggregate operand
//!   2. aggregation/evaluation/mod.rs       — ROLLUP/CUBE/GROUPING SETS
//!   3. columnar_execution/having.rs        — HAVING-clause CASE (columnar)
//!
//! These tests reproduce the pre-fix bug through each path and assert the
//! affinity-aware (storage-class) semantics the main evaluators already have.

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

fn setup_db() -> Database {
    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE t (id INTEGER, x TEXT)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Two rows with x='55' (so COUNT(*) WHERE x='55' == 2) and one with x='99'.
    for sql in [
        "INSERT INTO t VALUES (1, '55')",
        "INSERT INTO t VALUES (2, '55')",
        "INSERT INTO t VALUES (3, '99')",
    ] {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    db
}

fn execute_query(db: &Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

fn text(val: &SqlValue) -> String {
    match val {
        SqlValue::Varchar(s) => s.to_string(),
        other => panic!("Expected text, got {other:?}"),
    }
}

fn int(val: &SqlValue) -> i64 {
    match val {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Numeric(n) => *n as i64,
        SqlValue::Double(d) => *d as i64,
        SqlValue::Boolean(b) => *b as i64,
        other => panic!("Expected integer, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Path 1: CASE with an aggregate operand (aggregation/evaluation/case.rs)
// ---------------------------------------------------------------------------

#[test]
fn case_over_aggregate_bare_text_literal_does_not_match_integer() {
    let db = setup_db();

    // COUNT(*) is INTEGER 2; '2' is a bare TEXT literal with no affinity, so
    // per SQLite datatype3 §4.2 the branch must NOT match (SQLite: 'no-match').
    let rows = execute_query(
        &db,
        "SELECT CASE COUNT(*) WHEN '2' THEN 'matched-2' ELSE 'no-match' END FROM t WHERE x='55'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(text(&rows[0].values[0]), "no-match");
}

#[test]
fn case_over_aggregate_matching_integer_literal_still_matches() {
    let db = setup_db();

    // Control: integer-literal WHEN against integer COUNT(*) still matches.
    let rows = execute_query(
        &db,
        "SELECT CASE COUNT(*) WHEN 2 THEN 'matched-2' ELSE 'no-match' END FROM t WHERE x='55'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(text(&rows[0].values[0]), "matched-2");
}

// ---------------------------------------------------------------------------
// Path 2: ROLLUP/CUBE/GROUPING SETS (aggregation/evaluation/mod.rs)
// ---------------------------------------------------------------------------

#[test]
fn rollup_case_over_aggregate_bare_text_literal_does_not_match() {
    let db = setup_db();

    // The x='55' group has COUNT(*)=2; the bare TEXT '2' must not match it.
    let rows = execute_query(
        &db,
        "SELECT x, CASE COUNT(*) WHEN '2' THEN 'matched' ELSE 'no-match' END \
         FROM t GROUP BY ROLLUP(x)",
    );

    for row in &rows {
        // Every group (including the grand-total NULL group) must report
        // 'no-match': no group's INTEGER count is affinity-equal to TEXT '2'.
        assert_eq!(
            text(&row.values[1]),
            "no-match",
            "row for x={:?} wrongly matched bare TEXT '2'",
            row.values[0]
        );
    }
}

#[test]
fn rollup_is_distinct_from_bare_text_literal_is_distinct_from_integer() {
    let db = setup_db();

    // `COUNT(*) IS NOT '2'` (IS DISTINCT FROM): INTEGER 2 is a different
    // storage class than bare TEXT '2', so it IS distinct -> r = 1 (true)
    // for the x='55' group (count 2), matching the main-evaluator fix.
    let rows = execute_query(
        &db,
        "SELECT x, COUNT(*) AS c, (COUNT(*) IS NOT '2') AS r FROM t GROUP BY ROLLUP(x)",
    );

    let mut checked_count_two = false;
    for row in &rows {
        let c = int(&row.values[1]);
        let r = int(&row.values[2]);
        if c == 2 {
            assert_eq!(r, 1, "INTEGER 2 must be IS-DISTINCT-FROM bare TEXT '2'");
            checked_count_two = true;
        }
    }
    assert!(checked_count_two, "expected a group with COUNT(*)=2");
}

// ---------------------------------------------------------------------------
// Path 3: columnar HAVING CASE (columnar_execution/having.rs)
// ---------------------------------------------------------------------------

#[test]
fn having_case_bare_text_literal_does_not_match_integer_count() {
    let db = setup_db();

    // HAVING CASE c WHEN '2' THEN 1 ELSE 0 END: the x='55' group has c=2, but
    // bare TEXT '2' must not match INTEGER 2, so the HAVING predicate is 0
    // (false) for every group -> no rows survive.
    let rows = execute_query(
        &db,
        "SELECT x, COUNT(*) c FROM t GROUP BY x HAVING CASE COUNT(*) WHEN '2' THEN 1 ELSE 0 END",
    );
    assert!(
        rows.is_empty(),
        "no group should survive HAVING (bare TEXT '2' must not match INTEGER 2), got {rows:?}"
    );
}

#[test]
fn having_case_matching_integer_literal_keeps_group() {
    let db = setup_db();

    // Control: integer-literal WHEN against integer count still matches, so
    // the x='55' group (count 2) survives.
    let rows = execute_query(
        &db,
        "SELECT x, COUNT(*) c FROM t GROUP BY x HAVING CASE COUNT(*) WHEN 2 THEN 1 ELSE 0 END",
    );
    assert_eq!(rows.len(), 1, "the count-2 group should survive HAVING");
    assert_eq!(text(&rows[0].values[0]), "55");
    assert_eq!(int(&rows[0].values[1]), 2);
}
