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

/// Create the window1.test section-23 schema (extended with a `d` column
/// for the multi-window index-position tests; sqlite3 verification used
/// t5(a,b,c,d) and the EQP output for the original cases is unchanged):
/// CREATE TABLE t5(a, b, c, d); CREATE INDEX t5ab ON t5(a, b);
fn setup_db() -> Database {
    let mut db = Database::new();

    let create =
        Parser::parse_sql("CREATE TABLE t5 (a INTEGER, b INTEGER, c INTEGER, d INTEGER)").unwrap();
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

// ---------------------------------------------------------------------------
// Statement-level ORDER BY suppression (issue #5246)
//
// SQLite suppresses the statement-level USE TEMP B-TREE FOR ORDER BY entry
// when the statement ORDER BY is a structural prefix of (or equal to) the
// combined sort key of the FIRST window in the SELECT list: the co-routine
// rewrite places the first window's sorting pass outermost, so its key is
// the final output order. Expected counts below verified against
// sqlite3 3.51.0 with the same schema.
// ---------------------------------------------------------------------------

// Case 1: statement ORDER BY identical to the window sort key — suppressed.
#[test]
fn test_statement_order_by_identical_to_window_key_suppressed() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (ORDER BY c) FROM t5 ORDER BY c");
    assert_eq!(count, 1);
}

// Case 2: statement ORDER BY extends the window key — NOT suppressed.
#[test]
fn test_statement_order_by_extending_window_key_not_suppressed() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (ORDER BY c) FROM t5 ORDER BY c, b");
    assert_eq!(count, 2);
}

// Case 3: statement ORDER BY is a prefix of the window key — suppressed.
#[test]
fn test_statement_order_by_prefix_of_window_key_suppressed() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (ORDER BY c, b) FROM t5 ORDER BY c");
    assert_eq!(count, 1);
}

// Case 4: direction mismatch (window ASC, statement DESC) — NOT suppressed.
#[test]
fn test_statement_order_by_direction_mismatch_not_suppressed() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (ORDER BY c) FROM t5 ORDER BY c DESC");
    assert_eq!(count, 2);
}

// Case 5: DESC exact match — suppressed.
#[test]
fn test_statement_order_by_desc_exact_match_suppressed() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (ORDER BY c DESC) FROM t5 ORDER BY c DESC");
    assert_eq!(count, 1);
}

// Case 6: DESC prefix match — suppressed.
#[test]
fn test_statement_order_by_desc_prefix_suppressed() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (ORDER BY c DESC, b) FROM t5 ORDER BY c DESC");
    assert_eq!(count, 1);
}

// Case 7: statement ORDER BY matches the window PARTITION BY key — suppressed.
#[test]
fn test_statement_order_by_matching_partition_key_suppressed() {
    let db = setup_db();
    let count = order_count(&db, "SELECT sum(c) OVER (PARTITION BY c) FROM t5 ORDER BY c");
    assert_eq!(count, 1);
}

// Case 8: statement ORDER BY is a prefix via the PARTITION BY portion of the
// combined key (PARTITION BY c ORDER BY b => key (c, b)) — suppressed.
#[test]
fn test_statement_order_by_partition_prefix_suppressed() {
    let db = setup_db();
    let count =
        order_count(&db, "SELECT sum(c) OVER (PARTITION BY c ORDER BY b) FROM t5 ORDER BY c");
    assert_eq!(count, 1);
}

// Case 9: COLLATE mismatch — NOT suppressed.
#[test]
fn test_statement_order_by_collate_mismatch_not_suppressed() {
    let db = setup_db();
    let count =
        order_count(&db, "SELECT sum(c) OVER (ORDER BY c) FROM t5 ORDER BY c COLLATE NOCASE");
    assert_eq!(count, 2);
}

// Case 10: multiple windows — statement ORDER BY matching the FIRST window's
// key is suppressed.
#[test]
fn test_statement_order_by_matching_first_window_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY c),
            sum(c) OVER (ORDER BY b)
         FROM t5 ORDER BY c",
    );
    assert_eq!(count, 2);
}

// Case 11: multiple windows — statement ORDER BY matching only the SECOND
// window's key is NOT suppressed (only the first window's key governs).
#[test]
fn test_statement_order_by_matching_second_window_not_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY c),
            sum(c) OVER (ORDER BY b)
         FROM t5 ORDER BY b",
    );
    assert_eq!(count, 3);
}

// Case 12: first window has an empty key (OVER ()) — never suppresses, even
// if a later window's key matches the statement ORDER BY.
#[test]
fn test_empty_first_window_key_never_suppresses() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (),
            sum(c) OVER (ORDER BY c)
         FROM t5 ORDER BY c",
    );
    assert_eq!(count, 2);
}

// Reversed-window-order control for cases 10/11: with the windows swapped,
// suppression follows the new first window's key.
#[test]
fn test_suppression_follows_window_order_in_select_list() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY b),
            sum(c) OVER (ORDER BY c)
         FROM t5 ORDER BY b",
    );
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------------
// Multi-window index suppression (issue #5248)
//
// SQLite's nested co-routine rewrite emits window sorting passes in reverse
// SELECT-list order: the LAST distinct window key (by first occurrence) is
// the INNERMOST pass — the only one that scans the base table and can use an
// index. All outer passes read co-routine output and always need a temp
// B-tree, even when their key matches an index. Expected counts below
// verified against sqlite3 3.51.0 with schema t5(a,b,c,d), index t5ab(a,b).
// ---------------------------------------------------------------------------

// Two windows, index-matching key (a, b) FIRST: its pass is outermost and
// reads a co-routine, so the index is NOT usable — both keys count.
#[test]
fn test_index_key_first_of_two_windows_not_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY a, b),
            sum(a) OVER (ORDER BY c)
         FROM t5",
    );
    assert_eq!(count, 2);
}

// Two windows, index-matching key (a, b) LAST: its pass is innermost, scans
// t5 via index t5ab, and is suppressed — only key (c) counts.
#[test]
fn test_index_key_last_of_two_windows_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(a) OVER (ORDER BY c),
            sum(c) OVER (ORDER BY a, b)
         FROM t5",
    );
    assert_eq!(count, 1);
}

// Three windows, index-matching key (a, b) FIRST: outermost pass — all
// three keys count.
#[test]
fn test_index_key_first_of_three_windows_not_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(d) OVER (ORDER BY a, b),
            sum(d) OVER (ORDER BY c),
            sum(d) OVER (ORDER BY d)
         FROM t5",
    );
    assert_eq!(count, 3);
}

// Three windows, index-matching key (a, b) in the MIDDLE: still an outer
// pass — all three keys count.
#[test]
fn test_index_key_middle_of_three_windows_not_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(d) OVER (ORDER BY c),
            sum(d) OVER (ORDER BY a, b),
            sum(d) OVER (ORDER BY d)
         FROM t5",
    );
    assert_eq!(count, 3);
}

// Three windows, index-matching key (a, b) LAST: innermost pass uses the
// index — only the two outer keys count.
#[test]
fn test_index_key_last_of_three_windows_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(d) OVER (ORDER BY c),
            sum(d) OVER (ORDER BY d),
            sum(d) OVER (ORDER BY a, b)
         FROM t5",
    );
    assert_eq!(count, 2);
}

// Dedup keeps FIRST-occurrence position: (a,b), (c), (a,b) dedups to
// [(a,b), (c)] — innermost key is (c), index NOT used, both keys count.
#[test]
fn test_repeated_index_key_keeps_first_position_not_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(c) OVER (ORDER BY a, b),
            sum(a) OVER (ORDER BY c),
            sum(b) OVER (ORDER BY a, b)
         FROM t5",
    );
    assert_eq!(count, 2);
}

// Dedup control: (c), (a,b), (c) dedups to [(c), (a,b)] — innermost key is
// (a,b), index used — only key (c) counts.
#[test]
fn test_repeated_outer_key_leaves_index_key_innermost_suppressed() {
    let db = setup_db();
    let count = order_count(
        &db,
        "SELECT
            sum(a) OVER (ORDER BY c),
            sum(c) OVER (ORDER BY a, b),
            sum(b) OVER (ORDER BY c)
         FROM t5",
    );
    assert_eq!(count, 1);
}
