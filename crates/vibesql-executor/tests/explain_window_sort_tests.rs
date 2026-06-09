//! Tests for EXPLAIN QUERY PLAN window-sort entries
//!
//! SQLite's `do_ordercount_test` (window1.test section 23) counts `ORDER`
//! occurrences in EXPLAIN QUERY PLAN output. Each window function sorting
//! pass that is not satisfied by an index contributes one
//! `USE TEMP B-TREE FOR ORDER BY` entry. Semantics:
//!
//! - The sort key is PARTITION BY exprs (as ASC) + the window ORDER BY items
//! - Distinct keys are deduplicated structurally
//! - Frame clauses are ignored
//! - Keys satisfied by an index emit no entry

use vibesql_ast::Statement;
use vibesql_executor::ExplainExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Create the window1.test section-23 schema:
/// CREATE TABLE t5(a, b, c); CREATE INDEX t5ab ON t5(a, b);
fn setup_db() -> Database {
    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE t5 (a INTEGER, b INTEGER, c INTEGER)").unwrap();
    if let Statement::CreateTable(stmt) = create {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    } else {
        panic!("Expected CREATE TABLE statement");
    }

    let create_index = Parser::parse_sql("CREATE INDEX t5ab ON t5(a, b)").unwrap();
    if let Statement::CreateIndex(stmt) = create_index {
        vibesql_executor::CreateIndexExecutor::execute(&stmt, &mut db).unwrap();
    } else {
        panic!("Expected CREATE INDEX statement");
    }

    db
}

/// Run EXPLAIN QUERY PLAN and count occurrences of "ORDER" in the output,
/// mirroring `do_ordercount_test` from window1.test.
fn order_count(db: &Database, sql: &str) -> usize {
    let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let stmt = Parser::parse_sql(&explain_sql).expect("Failed to parse SQL");

    if let Statement::Explain(explain_stmt) = stmt {
        let result = ExplainExecutor::execute(&explain_stmt, db).expect("EXPLAIN failed");
        result.to_sqlite_eqp().matches("ORDER").count()
    } else {
        panic!("Expected EXPLAIN statement");
    }
}

// window1.test 23.1: both windows have combined key (a, b), which is
// satisfied by index t5ab(a, b) — no sort entries.
#[test]
fn test_index_satisfied_window_sort_emits_no_entry() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY a, b),
            sum(c) OVER (PARTITION BY a ORDER BY b)
         FROM t5",
    );
    assert_eq!(count, 0);
}

// window1.test 23.2: both windows have combined key (b, a) — dedup to 1 sort.
#[test]
fn test_duplicate_window_keys_dedup_to_one_entry() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY b, a),
            sum(c) OVER (PARTITION BY b ORDER BY a)
         FROM t5",
    );
    assert_eq!(count, 1);
}

// window1.test 23.3: keys (b, a) and (c, b) are distinct — 2 sorts.
#[test]
fn test_distinct_window_keys_emit_separate_entries() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY b, a),
            sum(c) OVER (ORDER BY c, b)
         FROM t5",
    );
    assert_eq!(count, 2);
}

// window1.test 23.4: same ORDER BY key with three different frame clauses —
// frames are ignored, 1 sort.
#[test]
fn test_frame_clauses_ignored_for_dedup() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            sum(c) OVER (ORDER BY b RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            sum(c) OVER (ORDER BY b GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
         FROM t5",
    );
    assert_eq!(count, 1);
}

// window1.test 23.5: same expression key (b+1) across three windows — 1 sort.
#[test]
fn test_same_expression_key_dedups() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY b+1 ROWS UNBOUNDED PRECEDING),
            sum(c) OVER (ORDER BY b+1 RANGE UNBOUNDED PRECEDING),
            sum(c) OVER (ORDER BY b+1 GROUPS UNBOUNDED PRECEDING)
         FROM t5",
    );
    assert_eq!(count, 1);
}

// window1.test 23.6: distinct expression keys b+1, b+2, b+3 — 3 sorts.
#[test]
fn test_distinct_expression_keys_emit_separate_entries() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY b+1 ROWS UNBOUNDED PRECEDING),
            sum(c) OVER (ORDER BY b+2 RANGE UNBOUNDED PRECEDING),
            sum(c) OVER (ORDER BY b+3 GROUPS UNBOUNDED PRECEDING)
         FROM t5",
    );
    assert_eq!(count, 3);
}

// window1.test 24.1/24.2: OVER () has no partition or order key — no sort.
#[test]
fn test_empty_over_clause_emits_no_entry() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER () FROM t5");
    assert_eq!(count, 0);
}

// A window sort and a statement-level ORDER BY are separate sorting passes —
// both are counted (no cross-dedup).
#[test]
fn test_window_sort_and_statement_order_by_both_counted() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (ORDER BY c) FROM t5 ORDER BY b");
    assert_eq!(count, 2);
}

// Named windows (WINDOW w AS (...)) are resolved before key extraction.
#[test]
fn test_named_window_resolved_for_key() {
    let db = setup_db();

    // Named window with key (c) — not satisfied by index t5ab — 1 sort,
    // deduped against an inline window with the same key.
    let count = order_count(
        &db,
        "SELECT sum(c) OVER w, sum(b) OVER (ORDER BY c) FROM t5 WINDOW w AS (ORDER BY c)",
    );
    assert_eq!(count, 1);

    // Named window whose key (a, b) is satisfied by the index — 0 sorts.
    let count =
        order_count(&db, "SELECT sum(c) OVER w FROM t5 WINDOW w AS (PARTITION BY a ORDER BY b)");
    assert_eq!(count, 0);
}

// Window functions nested inside arithmetic expressions are still collected.
#[test]
fn test_window_function_nested_in_arithmetic() {
    let db = setup_db();
    let count = order_count(&db, "SELECT 1 + sum(c) OVER (ORDER BY c) FROM t5");
    assert_eq!(count, 1);
}

// DESC vs ASC on the same column are different keys — no dedup.
#[test]
fn test_direction_distinguishes_keys() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY c ASC),
            sum(c) OVER (ORDER BY c DESC)
         FROM t5",
    );
    assert_eq!(count, 2);
}
