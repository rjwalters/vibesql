//! Tests for EXPLAIN QUERY PLAN temp-structure annotations (#5367, #5371):
//! `USE TEMP B-TREE FOR GROUP BY`, `USE TEMP B-TREE FOR DISTINCT`, the
//! `<OP> USING TEMP B-TREE` compound dedup branch labels, the
//! `CO-ROUTINE <alias>` wrapper for non-flattenable derived tables,
//! ordering-index scan lines (`SCAN t USING [COVERING ]INDEX i`), and the
//! compound + ORDER BY shape.
//!
//! Every expected shape below was verified live against sqlite3 3.51.0
//! (`.eqp on`); divergences are noted per test. Truthfulness (per the
//! #5355/#5360/#5366 precedent):
//! - GROUP BY always executes as hash aggregation followed by a sort of the
//!   groups by key (select/grouping/hash.rs), so the GROUP BY line
//!   truthfully describes the temp grouping structure. Its suppression when
//!   an index delivers group order mirrors SQLite's EQP — the same
//!   permissive EQP-level convention as the existing ORDER BY
//!   stabilization-sort suppression (`needs_temp_btree_for_order_by_eqp`).
//! - DISTINCT always executes as a hash dedup preserving input order
//!   (select/helpers.rs `apply_distinct`); the DISTINCT line truthfully
//!   describes that temp structure, suppressed like SQLite when an index
//!   delivers SELECT-list order.
//! - Compound dedup (UNION/INTERSECT/EXCEPT) executes via temp hash
//!   structures (select/set_operations.rs), so `USING TEMP B-TREE` labels
//!   are truthful.
//! - Where a GROUP BY/DISTINCT/ORDER BY temp line is suppressed because an
//!   index delivers the key order, the scan line shows that index like
//!   sqlite3 (`SCAN t USING COVERING INDEX i` when it covers all read
//!   columns) — the same permissive EQP-level convention (#5371).
//! - A compound's statement-level ORDER BY renders as a trailing
//!   `USE TEMP B-TREE FOR ORDER BY` line after the COMPOUND QUERY block:
//!   the runtime materializes the combined result and sorts it in one pass
//!   (`sort_set_operation_results`). sqlite3 3.51.0 instead renders a
//!   `MERGE (<OP>)` block with per-branch ORDER BY lines (sorted branches
//!   merged at read time, verified live) — a documented divergence, since
//!   rendering MERGE would fabricate a plan VibeSQL never executes (#5371).
//! - Partial-prefix shapes (#5373): when an index satisfies only a PREFIX
//!   of the ordering work (partial GROUP BY/DISTINCT/ORDER BY key, mixed
//!   ASC/DESC directions), sqlite3 still rides the index on the scan line —
//!   its sorter benefits from partial order — and keeps the temp line
//!   (`USE TEMP B-TREE FOR LAST TERM OF ORDER BY` for sorts). VibeSQL's
//!   runtime gains nothing from partial order: the scan layer
//!   (`cost_based_index_selection`) only accepts a full direction-uniform
//!   structural match and otherwise seq-scans, and the GROUP BY/DISTINCT
//!   paths pass no ordering hint to the scan at all. The bare `SCAN t` +
//!   temp line is therefore the truthful rendering — a documented
//!   divergence (see the #5373 section below); revisit if the runtime
//!   learns to exploit partial index order.

use vibesql_ast::Statement;
use vibesql_executor::ExplainExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn run_ddl(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateIndex(s) => {
            vibesql_executor::CreateIndexExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateView(s) => {
            vibesql_executor::advanced_objects::execute_create_view(&s, db).unwrap();
        }
        other => panic!("unsupported DDL in test: {:?}", other),
    }
}

/// Run EXPLAIN QUERY PLAN and return the SQLite-style EQP output.
fn eqp(db: &Database, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let stmt = Parser::parse_sql(&explain_sql).expect("Failed to parse SQL");

    if let Statement::Explain(explain_stmt) = stmt {
        let result = ExplainExecutor::execute(&explain_stmt, db).expect("EXPLAIN failed");
        result.to_sqlite_eqp()
    } else {
        panic!("Expected EXPLAIN statement");
    }
}

/// t1(a, b, c) with a single-column index on `a`.
fn setup_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)");
    run_ddl(&mut db, "CREATE INDEX i1a ON t1(a)");
    db
}

/// tab(a, b, c) with a composite index on (a, b).
fn setup_composite_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE tab(a INTEGER, b INTEGER, c INTEGER)");
    run_ddl(&mut db, "CREATE INDEX iab ON tab(a, b)");
    db
}

/// Two plain (unindexed) tables for compound-query shapes.
fn setup_compound_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE u1(a INTEGER, b INTEGER)");
    run_ddl(&mut db, "CREATE TABLE u2(a INTEGER, b INTEGER)");
    db
}

// ---------------------------------------------------------------------------
// GROUP BY
// ---------------------------------------------------------------------------

// Unindexed GROUP BY. sqlite3 — identical:
//   |--SCAN t1
//   `--USE TEMP B-TREE FOR GROUP BY
#[test]
fn test_group_by_unindexed_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b, count(*) FROM t1 GROUP BY b");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR GROUP BY\n"
    );
}

// Indexed GROUP BY: the i1a index delivers group order, so the line is
// suppressed and the scan rides the index (#5371). The `count(*)`
// pseudo-column reads nothing, so i1a covers the SELECT list. sqlite3 —
// identical:
//   `--SCAN t1 USING COVERING INDEX i1a
#[test]
fn test_group_by_indexed_suppresses_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, count(*) FROM t1 GROUP BY a");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN t1 USING COVERING INDEX i1a\n"
    );
}

// Indexed GROUP BY whose SELECT list reads a column outside the index: the
// scan rides the ordering index without the COVERING tag. sqlite3 —
// identical:
//   `--SCAN t1 USING INDEX i1a
#[test]
fn test_group_by_indexed_non_covering_scan_line() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, c FROM t1 GROUP BY a");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN t1 USING INDEX i1a\n"
    );
}

// GROUP BY on two columns when the index only covers the first. sqlite3:
//   |--SCAN t1 USING INDEX i1a        [scan-line divergence: SCAN t1]
//   `--USE TEMP B-TREE FOR GROUP BY
// Scan-line divergence (documented, #5371/#5373): sqlite3 still rides i1a
// for the partial prefix of the group key so its sorter benefits from
// partial order; VibeSQL's runtime seq-scans and hash-groups, and the
// #5371 scan rendering only fires when the temp-line suppression fires, so
// the bare `SCAN t1` is the truthful rendering here. #5373 confirmed the
// access path: the aggregation path passes no ordering hint to the scan
// (executor/aggregation/mod.rs `execute_from_with_where(..., None, ...)`),
// so the runtime never traverses i1a for this query.
#[test]
fn test_group_by_partially_indexed_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, b, count(*) FROM t1 GROUP BY a, b");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR GROUP BY\n"
    );
}

// Equality-pinned GROUP BY column: the index search delivers a single
// group, so no temp structure line. sqlite3 — identical shape:
//   `--SEARCH t1 USING COVERING INDEX i1a (a=?)
#[test]
fn test_group_by_pinned_by_where_suppresses_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, count(*) FROM t1 WHERE a = 5 GROUP BY a");

    assert!(output.contains("SEARCH t1"), "expected index search:\n{}", output);
    assert!(!output.contains("USE TEMP B-TREE"), "pinned GROUP BY must suppress:\n{}", output);
}

// Grouping is order-insensitive: SQLite reorders `GROUP BY b, a` to match
// the (a, b) index, suppresses the line, and rides the covering index.
// VibeSQL retries the group key permuted into each index's column order
// and renders the matched index (#5371). sqlite3 — identical:
//   `--SCAN tab USING COVERING INDEX iab
#[test]
fn test_group_by_order_insensitive_index_match_suppresses() {
    let db = setup_composite_db();
    let output = eqp(&db, "SELECT b, a, count(*) FROM tab GROUP BY b, a");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN tab USING COVERING INDEX iab\n"
    );
}

// ROLLUP (and other grouping-set forms) always build temp structures.
// (Non-standard in SQLite — no reference output; the line is the truthful
// rendering of the runtime's grouping-sets execution.)
#[test]
fn test_group_by_rollup_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, b, count(*) FROM t1 GROUP BY ROLLUP(a, b)");

    assert!(
        output.contains("USE TEMP B-TREE FOR GROUP BY"),
        "ROLLUP grouping needs the temp structure:\n{}",
        output
    );
}

// Plain aggregate without GROUP BY: no temp-structure line. sqlite3 agrees
// (`SCAN t1 USING COVERING INDEX i1a` for count(*) — scan-line divergence:
// sqlite3 counts over the smallest index; VibeSQL's runtime seq-scans, and
// with no grouping/dedup/ordering key there is no ordering index to ride).
#[test]
fn test_aggregate_without_group_by_no_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT count(*) FROM t1");

    assert!(!output.contains("USE TEMP B-TREE"), "plain aggregate needs no line:\n{}", output);
}

// ---------------------------------------------------------------------------
// GROUP BY + ORDER BY interaction
// ---------------------------------------------------------------------------

// ORDER BY matching the GROUP BY key exactly is satisfied by the grouping
// output (the runtime sorts groups by key). sqlite3 — identical:
//   |--SCAN t1
//   `--USE TEMP B-TREE FOR GROUP BY
#[test]
fn test_order_by_matching_group_key_suppressed() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b, count(*) FROM t1 GROUP BY b ORDER BY b");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR GROUP BY\n"
    );
}

// ORDER BY on a non-group expression needs its own sorting pass. sqlite3 —
// identical:
//   |--SCAN t1
//   |--USE TEMP B-TREE FOR GROUP BY
//   `--USE TEMP B-TREE FOR ORDER BY
#[test]
fn test_order_by_not_matching_group_key_emits_both() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b, count(*) FROM t1 GROUP BY b ORDER BY count(*)");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         |--USE TEMP B-TREE FOR GROUP BY\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// Directions are ignored in the GROUP BY match: sqlite3 suppresses the
// ORDER BY line even for mixed ASC/DESC over the exact group key (verified
// live: only the GROUP BY line renders).
#[test]
fn test_order_by_group_key_mixed_directions_suppressed() {
    let db = setup_db();
    let output =
        eqp(&db, "SELECT b, c, count(*) FROM t1 GROUP BY b, c ORDER BY b ASC, c DESC");

    assert!(output.contains("USE TEMP B-TREE FOR GROUP BY"), "missing GROUP BY:\n{}", output);
    assert!(
        !output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "exact-key ORDER BY (directions ignored) must suppress:\n{}",
        output
    );
}

// A bare prefix of the group key does NOT suppress (verified live: sqlite3
// emits both lines for `GROUP BY b, c ORDER BY b`).
#[test]
fn test_order_by_group_key_prefix_not_suppressed() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b, c, count(*) FROM t1 GROUP BY b, c ORDER BY b");

    assert!(output.contains("USE TEMP B-TREE FOR GROUP BY"), "missing GROUP BY:\n{}", output);
    assert!(
        output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "prefix-only ORDER BY needs its own pass:\n{}",
        output
    );
}

// A permutation of the group key does NOT suppress either (verified live).
#[test]
fn test_order_by_group_key_permutation_not_suppressed() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b, c, count(*) FROM t1 GROUP BY b, c ORDER BY c, b");

    assert!(
        output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "permuted ORDER BY needs its own pass:\n{}",
        output
    );
}

// Output ordinals resolve before the match: `ORDER BY 1` is `ORDER BY b`
// here. sqlite3 — only the GROUP BY line (verified live).
#[test]
fn test_order_by_ordinal_resolves_against_group_key() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b, count(*) FROM t1 GROUP BY b ORDER BY 1");

    assert!(output.contains("USE TEMP B-TREE FOR GROUP BY"), "missing GROUP BY:\n{}", output);
    assert!(
        !output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "ordinal ORDER BY over the group key must suppress:\n{}",
        output
    );
}

// Output aliases resolve too: `ORDER BY z` where `b AS z`. sqlite3 — only
// the GROUP BY line (verified live).
#[test]
fn test_order_by_alias_resolves_against_group_key() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b AS z, count(*) FROM t1 GROUP BY b ORDER BY z");

    assert!(output.contains("USE TEMP B-TREE FOR GROUP BY"), "missing GROUP BY:\n{}", output);
    assert!(
        !output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "alias ORDER BY over the group key must suppress:\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// DISTINCT
// ---------------------------------------------------------------------------

// Unindexed DISTINCT. sqlite3 — identical:
//   |--SCAN t1
//   `--USE TEMP B-TREE FOR DISTINCT
#[test]
fn test_distinct_unindexed_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT b FROM t1");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR DISTINCT\n"
    );
}

// Indexed DISTINCT: rows arrive in SELECT-list order via i1a, so the line
// is suppressed and the dedup rides the covering index (#5371). sqlite3 —
// identical:
//   `--SCAN t1 USING COVERING INDEX i1a
#[test]
fn test_distinct_indexed_suppresses_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM t1");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN t1 USING COVERING INDEX i1a\n"
    );
}

// DISTINCT + ORDER BY on the exact (all-ASC) SELECT list: the dedup
// structure delivers the order; one line only. sqlite3 — identical:
//   |--SCAN t1
//   `--USE TEMP B-TREE FOR DISTINCT
#[test]
fn test_distinct_order_by_exact_asc_one_line() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT b FROM t1 ORDER BY b");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR DISTINCT\n"
    );
}

// DESC breaks the DISTINCT match (unlike the GROUP BY rule — verified
// live): sqlite3 emits both lines.
//   |--SCAN t1
//   |--USE TEMP B-TREE FOR DISTINCT
//   `--USE TEMP B-TREE FOR ORDER BY
#[test]
fn test_distinct_order_by_desc_two_lines() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT b FROM t1 ORDER BY b DESC");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         |--USE TEMP B-TREE FOR DISTINCT\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// A bare prefix of the SELECT list does not suppress the ORDER BY line
// (verified live), but the full list does.
#[test]
fn test_distinct_order_by_prefix_vs_full_list() {
    let db = setup_db();

    let prefix = eqp(&db, "SELECT DISTINCT b, c FROM t1 ORDER BY b");
    assert!(prefix.contains("USE TEMP B-TREE FOR DISTINCT"), "missing DISTINCT:\n{}", prefix);
    assert!(
        prefix.contains("USE TEMP B-TREE FOR ORDER BY"),
        "prefix-only ORDER BY needs its own pass:\n{}",
        prefix
    );

    let full = eqp(&db, "SELECT DISTINCT b, c FROM t1 ORDER BY b, c");
    assert!(full.contains("USE TEMP B-TREE FOR DISTINCT"), "missing DISTINCT:\n{}", full);
    assert!(
        !full.contains("USE TEMP B-TREE FOR ORDER BY"),
        "full-list ASC ORDER BY must suppress:\n{}",
        full
    );
}

// Ordinals resolve in the DISTINCT match too: `ORDER BY 1` is `ORDER BY b`
// (verified live: only the DISTINCT line renders).
#[test]
fn test_distinct_order_by_ordinal_suppressed() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT b FROM t1 ORDER BY 1");

    assert!(output.contains("USE TEMP B-TREE FOR DISTINCT"), "missing DISTINCT:\n{}", output);
    assert!(
        !output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "ordinal ORDER BY over the SELECT list must suppress:\n{}",
        output
    );
}

// DISTINCT + GROUP BY emit both lines, GROUP BY first. sqlite3 — identical
// (it does not prove distinctness from the grouping):
//   |--SCAN t1
//   |--USE TEMP B-TREE FOR GROUP BY
//   `--USE TEMP B-TREE FOR DISTINCT
#[test]
fn test_distinct_with_group_by_emits_both_in_order() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT b, count(*) FROM t1 GROUP BY b");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         |--USE TEMP B-TREE FOR GROUP BY\n\
         `--USE TEMP B-TREE FOR DISTINCT\n"
    );
}

// With GROUP BY present the index never suppresses DISTINCT (the grouping
// pass intervenes): sqlite3 keeps the DISTINCT line for
// `SELECT DISTINCT a ... GROUP BY a` even with the index, while the
// grouping itself rides the covering index (verified live, #5371) —
// identical:
//   |--SCAN t1 USING COVERING INDEX i1a
//   `--USE TEMP B-TREE FOR DISTINCT
#[test]
fn test_distinct_not_suppressed_by_index_when_grouped() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM t1 GROUP BY a");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1 USING COVERING INDEX i1a\n\
         `--USE TEMP B-TREE FOR DISTINCT\n"
    );
}

// Indexed DISTINCT + reverse ORDER BY: the dedup rides the index and the
// reverse traversal satisfies the DESC order — no lines at all, and the
// scan shows the covering index (#5371). sqlite3 — identical:
//   `--SCAN t1 USING COVERING INDEX i1a
#[test]
fn test_distinct_indexed_order_by_desc_no_lines() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM t1 ORDER BY a DESC");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN t1 USING COVERING INDEX i1a\n"
    );
}

// Constant SELECT lists never suppress (verified live: sqlite3 keeps the
// DISTINCT line).
#[test]
fn test_distinct_literal_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT 1 FROM t1");

    assert!(output.contains("USE TEMP B-TREE FOR DISTINCT"), "missing DISTINCT:\n{}", output);
}

// Wildcard SELECT lists never suppress either.
#[test]
fn test_distinct_wildcard_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT * FROM t1");

    assert!(output.contains("USE TEMP B-TREE FOR DISTINCT"), "missing DISTINCT:\n{}", output);
}

// ---------------------------------------------------------------------------
// Compound dedup branch labels
// ---------------------------------------------------------------------------

// UNION dedups through a temp structure. sqlite3 — identical:
//   `--COMPOUND QUERY
//      |--LEFT-MOST SUBQUERY
//      |  `--SCAN u1
//      `--UNION USING TEMP B-TREE
//         `--SCAN u2
#[test]
fn test_union_branch_labeled_using_temp_btree() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT a FROM u1 UNION SELECT a FROM u2");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--COMPOUND QUERY\n   \
            |--LEFT-MOST SUBQUERY\n   \
            |  `--SCAN u1\n   \
            `--UNION USING TEMP B-TREE\n      \
               `--SCAN u2\n"
    );
}

// UNION ALL stays bare. sqlite3 — identical.
#[test]
fn test_union_all_branch_stays_bare() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT a FROM u1 UNION ALL SELECT a FROM u2");

    assert!(output.contains("`--UNION ALL\n"), "UNION ALL must stay bare:\n{}", output);
    assert!(!output.contains("USING TEMP B-TREE"), "no dedup structure:\n{}", output);
}

// INTERSECT and EXCEPT take the same suffix. sqlite3 — identical labels.
#[test]
fn test_intersect_and_except_labels() {
    let db = setup_compound_db();

    let intersect = eqp(&db, "SELECT a FROM u1 INTERSECT SELECT a FROM u2");
    assert!(
        intersect.contains("INTERSECT USING TEMP B-TREE"),
        "missing INTERSECT label:\n{}",
        intersect
    );

    let except = eqp(&db, "SELECT a FROM u1 EXCEPT SELECT a FROM u2");
    assert!(
        except.contains("EXCEPT USING TEMP B-TREE"),
        "missing EXCEPT label:\n{}",
        except
    );
}

// Mixed chains label each branch independently. sqlite3 — identical:
//   `--COMPOUND QUERY
//      |--LEFT-MOST SUBQUERY
//      |  `--SCAN u1
//      |--UNION ALL
//      |  `--SCAN u2
//      `--UNION USING TEMP B-TREE
//         `--SCAN u1
#[test]
fn test_mixed_compound_chain_labels() {
    let db = setup_compound_db();
    let output =
        eqp(&db, "SELECT a FROM u1 UNION ALL SELECT a FROM u2 UNION SELECT b FROM u1");

    assert!(output.contains("|--UNION ALL\n"), "missing bare UNION ALL branch:\n{}", output);
    assert!(
        output.contains("`--UNION USING TEMP B-TREE\n"),
        "missing dedup UNION branch:\n{}",
        output
    );
}

// Branch-level GROUP BY/DISTINCT lines render INSIDE the branch block.
// sqlite3 — identical:
//   `--COMPOUND QUERY
//      |--LEFT-MOST SUBQUERY
//      |  |--SCAN u1
//      |  `--USE TEMP B-TREE FOR GROUP BY
//      `--UNION ALL
//         `--SCAN u2
#[test]
fn test_compound_branch_group_by_line_inside_branch() {
    let db = setup_compound_db();
    let output =
        eqp(&db, "SELECT a, count(*) FROM u1 GROUP BY a UNION ALL SELECT a, b FROM u2");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--COMPOUND QUERY\n   \
            |--LEFT-MOST SUBQUERY\n   \
            |  |--SCAN u1\n   \
            |  `--USE TEMP B-TREE FOR GROUP BY\n   \
            `--UNION ALL\n      \
               `--SCAN u2\n"
    );
}

// sqlite3 — identical shape for a DISTINCT left branch.
#[test]
fn test_compound_branch_distinct_line_inside_branch() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM u1 UNION ALL SELECT a FROM u2");

    assert!(
        output.contains("|  `--USE TEMP B-TREE FOR DISTINCT\n"),
        "DISTINCT line must render inside the branch block:\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// CO-ROUTINE wrapper for non-flattenable derived tables
// ---------------------------------------------------------------------------

// Dedup-compound derived tables cannot be flattened; sqlite3 wraps the
// COMPOUND QUERY in a CO-ROUTINE (verified live) — identical:
//   |--CO-ROUTINE q
//   |  `--COMPOUND QUERY
//   |     |--LEFT-MOST SUBQUERY
//   |     |  `--SCAN u1
//   |     `--UNION USING TEMP B-TREE
//   |        `--SCAN u2
//   `--SCAN q
#[test]
fn test_dedup_compound_derived_table_wrapped_in_coroutine() {
    let db = setup_compound_db();
    let output =
        eqp(&db, "SELECT * FROM (SELECT a FROM u1 UNION SELECT a FROM u2) AS q");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE q\n\
         |  `--COMPOUND QUERY\n\
         |     |--LEFT-MOST SUBQUERY\n\
         |     |  `--SCAN u1\n\
         |     `--UNION USING TEMP B-TREE\n\
         |        `--SCAN u2\n\
         `--SCAN q\n"
    );
}

// UNION-ALL-only derived tables flatten (sqlite3 shows the bare COMPOUND
// QUERY with no co-routine and no alias — verified live) — identical.
#[test]
fn test_union_all_derived_table_not_wrapped() {
    let db = setup_compound_db();
    let output =
        eqp(&db, "SELECT * FROM (SELECT a FROM u1 UNION ALL SELECT a FROM u2) AS q");

    assert!(!output.contains("CO-ROUTINE"), "UNION ALL derived must flatten:\n{}", output);
    assert!(output.contains("COMPOUND QUERY"), "missing compound block:\n{}", output);
}

// GROUP BY derived tables wrap, with the grouping line inside the block.
// sqlite3 — identical:
//   |--CO-ROUTINE g
//   |  |--SCAN t1
//   |  `--USE TEMP B-TREE FOR GROUP BY
//   `--SCAN g
#[test]
fn test_group_by_derived_table_wrapped_in_coroutine() {
    let db = setup_db();
    let output =
        eqp(&db, "SELECT * FROM (SELECT b, count(*) AS c FROM t1 GROUP BY b) AS g");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE g\n\
         |  |--SCAN t1\n\
         |  `--USE TEMP B-TREE FOR GROUP BY\n\
         `--SCAN g\n"
    );
}

// DISTINCT derived tables wrap too. sqlite3 — identical:
//   |--CO-ROUTINE d
//   |  |--SCAN t1
//   |  `--USE TEMP B-TREE FOR DISTINCT
//   `--SCAN d
#[test]
fn test_distinct_derived_table_wrapped_in_coroutine() {
    let db = setup_db();
    let output = eqp(&db, "SELECT * FROM (SELECT DISTINCT b FROM t1) AS d");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE d\n\
         |  |--SCAN t1\n\
         |  `--USE TEMP B-TREE FOR DISTINCT\n\
         `--SCAN d\n"
    );
}

// Scalar-aggregate derived tables wrap (sqlite3: `CO-ROUTINE d` — verified
// live) — identical.
#[test]
fn test_aggregate_derived_table_wrapped_in_coroutine() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT * FROM (SELECT count(*) AS n FROM u2) AS d");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE d\n\
         |  `--SCAN u2\n\
         `--SCAN d\n"
    );
}

// LIMIT-only derived tables flatten in sqlite3 (verified live: `SCAN u1`
// alone); VibeSQL keeps the pre-existing flat rendering — identical.
#[test]
fn test_limit_derived_table_not_wrapped() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT * FROM (SELECT a FROM u1 LIMIT 5) AS l");

    assert!(!output.contains("CO-ROUTINE"), "LIMIT-only derived must flatten:\n{}", output);
    assert!(output.contains("SCAN u1"), "missing flattened scan:\n{}", output);
}

// ---------------------------------------------------------------------------
// GROUP BY / DISTINCT views inside CO-ROUTINE blocks
// ---------------------------------------------------------------------------

// Unindexed GROUP BY view: the grouping line renders INSIDE the CO-ROUTINE
// block, exactly once. sqlite3 — identical:
//   |--CO-ROUTINE gvu
//   |  |--SCAN t1
//   |  `--USE TEMP B-TREE FOR GROUP BY
//   `--SCAN gvu
#[test]
fn test_group_by_view_temp_btree_inside_coroutine_once() {
    let mut db = setup_db();
    run_ddl(&mut db, "CREATE VIEW gvu AS SELECT b, count(*) AS c FROM t1 GROUP BY b");
    let output = eqp(&db, "SELECT * FROM gvu");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE gvu\n\
         |  |--SCAN t1\n\
         |  `--USE TEMP B-TREE FOR GROUP BY\n\
         `--SCAN gvu\n"
    );
    assert_eq!(
        output.matches("USE TEMP B-TREE FOR GROUP BY").count(),
        1,
        "no double emission:\n{}",
        output
    );
}

// Unindexed DISTINCT view. sqlite3 — identical:
//   |--CO-ROUTINE dvu
//   |  |--SCAN t1
//   |  `--USE TEMP B-TREE FOR DISTINCT
//   `--SCAN dvu
#[test]
fn test_distinct_view_temp_btree_inside_coroutine_once() {
    let mut db = setup_db();
    run_ddl(&mut db, "CREATE VIEW dvu AS SELECT DISTINCT b FROM t1");
    let output = eqp(&db, "SELECT * FROM dvu");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE dvu\n\
         |  |--SCAN t1\n\
         |  `--USE TEMP B-TREE FOR DISTINCT\n\
         `--SCAN dvu\n"
    );
    assert_eq!(
        output.matches("USE TEMP B-TREE FOR DISTINCT").count(),
        1,
        "no double emission:\n{}",
        output
    );
}

// Outer ORDER BY over a GROUP BY view: the view's grouping line stays
// inside the block; the outer sort renders outside. sqlite3 — identical:
//   |--CO-ROUTINE gvu
//   |  |--SCAN t1
//   |  `--USE TEMP B-TREE FOR GROUP BY
//   |--SCAN gvu
//   `--USE TEMP B-TREE FOR ORDER BY
#[test]
fn test_outer_order_by_over_group_by_view() {
    let mut db = setup_db();
    run_ddl(&mut db, "CREATE VIEW gvu AS SELECT b, count(*) AS c FROM t1 GROUP BY b");
    let output = eqp(&db, "SELECT * FROM gvu ORDER BY c");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE gvu\n\
         |  |--SCAN t1\n\
         |  `--USE TEMP B-TREE FOR GROUP BY\n\
         |--SCAN gvu\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// ---------------------------------------------------------------------------
// Ordering-index scan lines (#5371)
// ---------------------------------------------------------------------------
// Where a temp-line suppression fires because an index delivers the key
// order, the scan line shows that index like sqlite3. The runtime truthfully
// hash-groups/dedups over its scan; the index rendering follows the same
// permissive EQP-level convention as the suppression itself (#5367).

// ORDER BY satisfied by a covering index. sqlite3 — identical:
//   `--SCAN t1 USING COVERING INDEX i1a
#[test]
fn test_order_by_indexed_covering_scan_line() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a FROM t1 ORDER BY a");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN t1 USING COVERING INDEX i1a\n"
    );
}

// ORDER BY satisfied by a non-covering index (c is not in i1a). sqlite3 —
// identical:
//   `--SCAN t1 USING INDEX i1a
#[test]
fn test_order_by_indexed_non_covering_scan_line() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, c FROM t1 ORDER BY a");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN t1 USING INDEX i1a\n"
    );
}

// GROUP BY + matching ORDER BY both ride the index: one covering scan, no
// temp lines. sqlite3 — identical:
//   `--SCAN t1 USING COVERING INDEX i1a
#[test]
fn test_group_by_order_by_indexed_covering_scan_line() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, count(*) FROM t1 GROUP BY a ORDER BY a");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN t1 USING COVERING INDEX i1a\n"
    );
}

// Unindexed keys keep the bare scan: the #5371 rendering only fires with
// the suppression. sqlite3 — identical (no index on b).
#[test]
fn test_unindexed_order_by_keeps_bare_scan() {
    let db = setup_db();
    let output = eqp(&db, "SELECT b FROM t1 ORDER BY b");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// ---------------------------------------------------------------------------
// Partial-prefix ordering-index divergences (#5373) — documented
// ---------------------------------------------------------------------------
// sqlite3 3.51.0 rides an index whose columns satisfy only a PREFIX of the
// ordering work and still emits the temp line: its external sorter exploits
// partial input order. VibeSQL's runtime cannot — the scan layer
// (`cost_based_index_selection`, select/scan/index_scan/selection.rs)
// accepts an index for ordering only on a full direction-uniform structural
// match (`can_use_index_for_order_by_with_pinned`: partial prefixes fail the
// length check, mixed ASC/DESC fail the all-match/all-reversed check), the
// aggregation path passes no ordering hint to the scan at all
// (executor/aggregation/mod.rs), and DISTINCT hash-dedups over whatever the
// scan returns (select/helpers.rs `apply_distinct`). In every shape below
// the runtime performs a bare sequential scan, so rendering
// `SCAN t USING INDEX i` would misstate the access path; the bare scan +
// temp line is the truthful rendering. Each sqlite3 shape verified live.

// DISTINCT over a SELECT list whose first column is indexed but whose
// second is not. sqlite3:
//   |--SCAN t1 USING INDEX i1a        [scan-line divergence: SCAN t1]
//   `--USE TEMP B-TREE FOR DISTINCT
// Runtime: non-aggregate SELECT passes only the statement ORDER BY (none
// here) as the scan's ordering hint, so the scan is sequential and the
// dedup is a hash structure — `SCAN t1` is what executes.
#[test]
fn test_distinct_partial_prefix_keeps_bare_scan_and_temp_line() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT a, c FROM t1");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR DISTINCT\n"
    );
}

// ORDER BY extending past the index columns (i1a covers `a` only). sqlite3
// rides the prefix AND renames the temp line for the unsatisfied suffix:
//   |--SCAN t1 USING INDEX i1a        [scan-line divergence: SCAN t1]
//   `--USE TEMP B-TREE FOR LAST TERM OF ORDER BY   [line-text divergence]
// Runtime: `can_use_index_for_order_by_with_pinned` rejects (a, b) against
// index (a) — more ORDER BY terms than index columns — so the scan is
// sequential and ONE full sort pass runs (`apply_order_by`); the plain
// `USE TEMP B-TREE FOR ORDER BY` line truthfully describes that single
// full sort (no LAST TERM partial-sort pass exists to describe).
#[test]
fn test_order_by_partial_prefix_keeps_bare_scan_and_temp_line() {
    let db = setup_db();
    let output = eqp(&db, "SELECT * FROM t1 ORDER BY a, b");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// Mixed-direction ORDER BY over the composite index (a, b). sqlite3 rides
// iab for the leading term and sorts within each prefix group:
//   |--SCAN tab USING INDEX iab       [scan-line divergence: SCAN tab]
//   `--USE TEMP B-TREE FOR LAST TERM OF ORDER BY   [line-text divergence]
// Runtime: mixed ASC/DESC fails the all-match/all-reversed direction check,
// so the scan is sequential and one full sort pass runs.
#[test]
fn test_order_by_mixed_directions_keeps_bare_scan_and_temp_line() {
    let db = setup_composite_db();
    let output = eqp(&db, "SELECT * FROM tab ORDER BY a ASC, b DESC");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN tab\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// ---------------------------------------------------------------------------
// ORDER BY rowid / INTEGER PRIMARY KEY alias (#5375)
// ---------------------------------------------------------------------------
// sqlite3 3.51.0 shows a bare `SCAN t` with NO temp line for ORDER BY on the
// rowid (or its INTEGER PRIMARY KEY alias): its table B-tree is keyed by
// rowid, so traversal natively delivers rowid order (DESC via reverse
// traversal). Verified live for: bare rowid/_rowid_/oid ASC and DESC, the
// IPK alias column ASC and DESC, qualified `t.id`, trailing terms after the
// unique first key (`ORDER BY id, y`, `ORDER BY id ASC, y DESC` — never
// evaluated, fully suppressed), and a non-indexed WHERE filter.
//
// VibeSQL splits on the storage scan-order guarantee (#5375 investigation):
//
// - Tables WITH a rowid alias (INTEGER PRIMARY KEY): the executor sorts
//   every sequential scan's output by the IPK column (#4926,
//   `sort_rows_by_integer_primary_key`), so rowid order IS the scan's
//   guaranteed natural output order and the runtime ORDER BY sort is
//   order-equivalent to it (its exact reverse for DESC — same convention as
//   index reverse-traversal suppression). The temp line is suppressed,
//   matching sqlite3 (`needs_temp_btree_for_order_by_eqp`).
//
// - Plain tables (NO rowid alias): the sequential scan yields physical
//   insertion order, which `INSERT INTO t(rowid, ...)` with out-of-order
//   values and `UPDATE t SET rowid = ...` (both supported, verified by
//   probe) decouple from rowid order. The ORDER BY sort genuinely reorders
//   rows, so the temp line stays — a documented divergence from sqlite3's
//   bare `SCAN t`.

/// r1(id INTEGER PRIMARY KEY, y TEXT) — rowid-alias table, no other indexes.
fn setup_rowid_alias_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE r1(id INTEGER PRIMARY KEY, y TEXT)");
    db
}

// Plain table, ORDER BY rowid. sqlite3: `--SCAN t1 (no temp line).
// DIVERGENCE (documented above): VibeSQL's plain-table scan order is
// insertion order, not rowid order, so the sort is real and the line stays.
#[test]
fn test_order_by_rowid_plain_table_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT * FROM t1 ORDER BY rowid");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// Plain table, ORDER BY rowid DESC. sqlite3: `--SCAN t1 (no temp line,
// reverse traversal). DIVERGENCE: same scan-order reasoning as ASC.
#[test]
fn test_order_by_rowid_desc_plain_table_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT * FROM t1 ORDER BY rowid DESC");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t1\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// Plain table, _rowid_ / oid keyword spellings. sqlite3: `--SCAN t1 for
// both (no temp line). DIVERGENCE: same scan-order reasoning.
#[test]
fn test_order_by_rowid_keyword_spellings_plain_table_emit_temp_btree() {
    let db = setup_db();
    for sql in ["SELECT * FROM t1 ORDER BY _rowid_", "SELECT * FROM t1 ORDER BY oid"] {
        let output = eqp(&db, sql);
        assert_eq!(
            output,
            "QUERY PLAN\n\
             |--SCAN t1\n\
             `--USE TEMP B-TREE FOR ORDER BY\n",
            "query: {}",
            sql
        );
    }
}

// A real column named `rowid` shadows the pseudo-column (the evaluator
// checks real columns first). sqlite3 — identical (verified live):
//   |--SCAN t8
//   `--USE TEMP B-TREE FOR ORDER BY
#[test]
fn test_order_by_shadowed_rowid_column_emits_temp_btree() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t8(rowid INT, y TEXT)");
    let output = eqp(&db, "SELECT * FROM t8 ORDER BY rowid");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN t8\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// IPK alias, ORDER BY id. sqlite3 — identical: `--SCAN r1 (no temp line).
// The #4926 scan sort guarantees rowid order, so the suppression is the
// stabilization-sort convention, not a fabrication.
#[test]
fn test_order_by_ipk_alias_suppresses_temp_btree() {
    let db = setup_rowid_alias_db();
    let output = eqp(&db, "SELECT * FROM r1 ORDER BY id");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN r1\n"
    );
}

// IPK alias, ORDER BY id DESC. sqlite3 — identical (reverse traversal).
#[test]
fn test_order_by_ipk_alias_desc_suppresses_temp_btree() {
    let db = setup_rowid_alias_db();
    let output = eqp(&db, "SELECT * FROM r1 ORDER BY id DESC");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN r1\n"
    );
}

// IPK alias via the rowid keyword spellings (rowid/_rowid_/oid resolve to
// the alias column), ASC and DESC. sqlite3 — identical for all.
#[test]
fn test_order_by_rowid_keywords_on_ipk_table_suppress_temp_btree() {
    let db = setup_rowid_alias_db();
    for sql in [
        "SELECT * FROM r1 ORDER BY rowid",
        "SELECT * FROM r1 ORDER BY rowid DESC",
        "SELECT * FROM r1 ORDER BY _rowid_",
        "SELECT * FROM r1 ORDER BY oid",
    ] {
        let output = eqp(&db, sql);
        assert_eq!(
            output,
            "QUERY PLAN\n\
             `--SCAN r1\n",
            "query: {}",
            sql
        );
    }
}

// Trailing terms after the unique IPK first key are never evaluated, so
// sqlite3 fully suppresses (verified live for `ORDER BY id, y` and
// `ORDER BY id ASC, y DESC`) — identical here.
#[test]
fn test_order_by_ipk_with_trailing_terms_suppresses_temp_btree() {
    let db = setup_rowid_alias_db();
    for sql in ["SELECT * FROM r1 ORDER BY id, y", "SELECT * FROM r1 ORDER BY id ASC, y DESC"] {
        let output = eqp(&db, sql);
        assert_eq!(
            output,
            "QUERY PLAN\n\
             `--SCAN r1\n",
            "query: {}",
            sql
        );
    }
}

// IPK NOT first: `ORDER BY y, id` needs a real sort. sqlite3 — identical:
//   |--SCAN r1
//   `--USE TEMP B-TREE FOR ORDER BY
#[test]
fn test_order_by_ipk_not_first_emits_temp_btree() {
    let db = setup_rowid_alias_db();
    let output = eqp(&db, "SELECT * FROM r1 ORDER BY y, id");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN r1\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// Table-qualified reference to the IPK alias. sqlite3 — identical.
#[test]
fn test_order_by_qualified_ipk_alias_suppresses_temp_btree() {
    let db = setup_rowid_alias_db();
    let output = eqp(&db, "SELECT * FROM r1 ORDER BY r1.id");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN r1\n"
    );
}

// Non-indexed WHERE + ORDER BY id: the filter preserves the scan's rowid
// order. sqlite3 — identical: `--SCAN r1 (no temp line).
#[test]
fn test_where_filter_order_by_ipk_suppresses_temp_btree() {
    let db = setup_rowid_alias_db();
    let output = eqp(&db, "SELECT * FROM r1 WHERE y > 'a' ORDER BY id");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         `--SCAN r1\n"
    );
}

// Expression over the IPK (`id+0`) is not the column itself. sqlite3 —
// identical (verified live: `ORDER BY rowid+0` / `ORDER BY id+0` keep the
// temp line):
//   |--SCAN r1
//   `--USE TEMP B-TREE FOR ORDER BY
#[test]
fn test_order_by_ipk_expression_emits_temp_btree() {
    let db = setup_rowid_alias_db();
    let output = eqp(&db, "SELECT * FROM r1 ORDER BY id+0");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN r1\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// When the WHERE clause rides an index, rows arrive in INDEX order, not
// rowid order, so the suppression does not apply and the temp line stays.
// sqlite3 (verified live) suppresses even here:
//   `--SEARCH r2 USING COVERING INDEX r2y (y=?)
// because its index entries are (key, rowid) pairs — within an equality
// group, traversal is rowid-ordered. VibeSQL's index buckets carry no such
// guarantee and the runtime genuinely re-sorts, so the temp line is the
// truthful rendering. DIVERGENCE (documented).
#[test]
fn test_indexed_where_order_by_ipk_keeps_temp_btree() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE r2(id INTEGER PRIMARY KEY, y TEXT)");
    run_ddl(&mut db, "CREATE INDEX r2y ON r2(y)");
    let output = eqp(&db, "SELECT * FROM r2 WHERE y='a' ORDER BY id");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SEARCH r2 USING INDEX r2y (y=?)\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// WITHOUT ROWID with a single-column exact-INTEGER PRIMARY KEY: VibeSQL
// still records the rowid-alias column and the #4926 scan sort applies, so
// ORDER BY on the PK is the scan's natural order. sqlite3 — identical
// (verified live; its WITHOUT ROWID B-tree is keyed by the PK):
//   `--SCAN r3
// The rowid KEYWORD spelling does not get the exemption on WITHOUT ROWID
// tables (no rowid pseudo-column there, #4953), so that spelling keeps the
// temp line.
#[test]
fn test_without_rowid_order_by_pk_suppresses_temp_btree() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE r3(a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID");

    assert_eq!(
        eqp(&db, "SELECT * FROM r3 ORDER BY a"),
        "QUERY PLAN\n\
         `--SCAN r3\n"
    );
    assert_eq!(
        eqp(&db, "SELECT * FROM r3 ORDER BY rowid"),
        "QUERY PLAN\n\
         |--SCAN r3\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// ---------------------------------------------------------------------------
// Compound + ORDER BY (#5371)
// ---------------------------------------------------------------------------
// The runtime materializes the combined result and sorts it in ONE pass
// (`sort_set_operation_results`, select/executor/execute.rs) for every set
// operator, so the truthful shape is the COMPOUND QUERY block plus a single
// trailing `USE TEMP B-TREE FOR ORDER BY` line.
//
// Documented divergence: sqlite3 3.51.0 renders `MERGE (UNION)` /
// `MERGE (UNION ALL)` / `MERGE (INTERSECT)` / `MERGE (EXCEPT)` with LEFT /
// RIGHT branch blocks each carrying its own ORDER BY line (verified live):
//   `--MERGE (UNION)
//      |--LEFT
//      |  |--SCAN u1
//      |  `--USE TEMP B-TREE FOR ORDER BY
//      `--RIGHT
//         |--SCAN u2
//         `--USE TEMP B-TREE FOR ORDER BY
// because its runtime sorts each branch and merges the sorted streams.
// VibeSQL never merges pre-sorted branches, so rendering MERGE would
// fabricate a plan shape that never executes (#5355/#5360/#5366 precedent).

// UNION + ORDER BY: dedup branch label plus one trailing sort line.
#[test]
fn test_union_order_by_trailing_temp_btree() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY 1");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--COMPOUND QUERY\n\
         |  |--LEFT-MOST SUBQUERY\n\
         |  |  `--SCAN u1\n\
         |  `--UNION USING TEMP B-TREE\n\
         |     `--SCAN u2\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// UNION ALL + ORDER BY: bare branch label, same trailing sort line (the
// runtime sorts ALL-variants identically).
#[test]
fn test_union_all_order_by_trailing_temp_btree() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT a FROM u1 UNION ALL SELECT a FROM u2 ORDER BY 1");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--COMPOUND QUERY\n\
         |  |--LEFT-MOST SUBQUERY\n\
         |  |  `--SCAN u1\n\
         |  `--UNION ALL\n\
         |     `--SCAN u2\n\
         `--USE TEMP B-TREE FOR ORDER BY\n"
    );
}

// INTERSECT and EXCEPT take the same trailing line; ORDER BY by name
// resolves like ordinals.
#[test]
fn test_intersect_except_order_by_trailing_temp_btree() {
    let db = setup_compound_db();

    let intersect = eqp(&db, "SELECT a FROM u1 INTERSECT SELECT a FROM u2 ORDER BY a");
    assert!(
        intersect.ends_with("`--USE TEMP B-TREE FOR ORDER BY\n"),
        "INTERSECT + ORDER BY needs the trailing sort line:\n{}",
        intersect
    );

    let except = eqp(&db, "SELECT a FROM u1 EXCEPT SELECT a FROM u2 ORDER BY 1");
    assert!(
        except.ends_with("`--USE TEMP B-TREE FOR ORDER BY\n"),
        "EXCEPT + ORDER BY needs the trailing sort line:\n{}",
        except
    );
}

// A compound WITHOUT ORDER BY keeps the bare COMPOUND QUERY shape (no
// trailing line) — sqlite3 identical (no MERGE without an ORDER BY).
#[test]
fn test_compound_without_order_by_no_trailing_line() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT a FROM u1 UNION SELECT a FROM u2");

    assert!(
        !output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "no ORDER BY, no sort line:\n{}",
        output
    );
}

// Dedup-compound derived table with an inner ORDER BY: the trailing sort
// line renders INSIDE the CO-ROUTINE block (the body sorts before the
// outer query reads it). sqlite3 nests its MERGE block in the co-routine
// the same way (divergence as above).
#[test]
fn test_compound_order_by_inside_coroutine() {
    let db = setup_compound_db();
    let output =
        eqp(&db, "SELECT * FROM (SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY 1) AS q");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--CO-ROUTINE q\n\
         |  |--COMPOUND QUERY\n\
         |  |  |--LEFT-MOST SUBQUERY\n\
         |  |  |  `--SCAN u1\n\
         |  |  `--UNION USING TEMP B-TREE\n\
         |  |     `--SCAN u2\n\
         |  `--USE TEMP B-TREE FOR ORDER BY\n\
         `--SCAN q\n"
    );
}

// ---------------------------------------------------------------------------
// DISTINCT with WHERE-equality-pinned columns (orderby5, issue #5713)
// ---------------------------------------------------------------------------
//
// A column constrained to one value by a WHERE equality is constant across the
// scan output and contributes nothing to distinctness, so SQLite drops it from
// the DISTINCT key before deciding whether an index delivers the remaining key.
// `SELECT DISTINCT a, b, c FROM t1 WHERE a=0` reduces its key to (b, c) which
// the index t1bc(b, c) covers, so no `USE TEMP B-TREE FOR DISTINCT` line.
// Every shape below was verified live against sqlite3 3.51.0.

/// t1(a, b, c) with a composite index on (b, c) — the orderby5 fixture.
fn setup_orderby5_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t1(a, b, c)");
    run_ddl(&mut db, "CREATE INDEX t1bc ON t1(b, c)");
    db
}

// orderby5 1.1: WHERE pins `a`; reduced key (b, c) is delivered by t1bc.
#[test]
fn test_distinct_where_pinned_leading_column_suppressed() {
    let db = setup_orderby5_db();
    let output = eqp(&db, "SELECT DISTINCT a, b, c FROM t1 WHERE a=0");

    assert!(
        !output.contains("USE TEMP B-TREE FOR DISTINCT"),
        "WHERE-pinned `a` reduces DISTINCT key to (b, c), covered by t1bc:\n{}",
        output
    );
}

// orderby5 1.2.1 / 1.5 / 1.6: DISTINCT is order-insensitive, so the reduced
// key (c, b) is reordered to (b, c) to match t1bc.
#[test]
fn test_distinct_where_pinned_permuted_key_suppressed() {
    let db = setup_orderby5_db();

    for sql in [
        "SELECT DISTINCT a, c, b FROM t1 WHERE a=0",
        "SELECT DISTINCT c, a, b FROM t1 WHERE a=0",
        "SELECT DISTINCT c, b, a FROM t1 WHERE a=0",
        "SELECT DISTINCT b, a, c FROM t1 WHERE a=0",
        "SELECT DISTINCT b, c, a FROM t1 WHERE a=0",
    ] {
        let output = eqp(&db, sql);
        assert!(
            !output.contains("USE TEMP B-TREE FOR DISTINCT"),
            "reduced/permuted key must be delivered by t1bc for `{}`:\n{}",
            sql,
            output
        );
    }
}

// All DISTINCT columns pinned → at most one distinct row, no temp structure.
#[test]
fn test_distinct_all_columns_pinned_suppressed() {
    let db = setup_orderby5_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM t1 WHERE a=0");

    assert!(
        !output.contains("USE TEMP B-TREE FOR DISTINCT"),
        "fully-pinned DISTINCT key needs no temp structure:\n{}",
        output
    );
}

// orderby5 1.2.2: WHERE pins `a` under nocase, but the DISTINCT key uses
// BINARY-collated `a` — the pin does NOT make BINARY `a` constant, so the
// DISTINCT line MUST stay (collation guard).
#[test]
fn test_distinct_where_pin_collation_mismatch_keeps_line() {
    let db = setup_orderby5_db();
    let output = eqp(&db, "SELECT DISTINCT a, c, b FROM t1 WHERE a='xyz' COLLATE nocase");

    assert!(
        output.contains("USE TEMP B-TREE FOR DISTINCT"),
        "nocase WHERE pin must not suppress a BINARY DISTINCT key:\n{}",
        output
    );
}

// orderby5 1.2.3: the DISTINCT key uses `a COLLATE nocase` but WHERE pins
// BINARY `a` — also a mismatch; the line MUST stay.
#[test]
fn test_distinct_key_collation_mismatch_keeps_line() {
    let db = setup_orderby5_db();
    let output = eqp(&db, "SELECT DISTINCT a COLLATE nocase, c, b FROM t1 WHERE a='xyz'");

    assert!(
        output.contains("USE TEMP B-TREE FOR DISTINCT"),
        "BINARY WHERE pin must not suppress a nocase DISTINCT key:\n{}",
        output
    );
}

// orderby5 1.2.4: both the WHERE pin and the DISTINCT key use nocase — the
// collations match, `a COLLATE nocase` is constant, reduced key (c, b) is
// delivered by t1bc, and the line is suppressed.
#[test]
fn test_distinct_key_collation_match_suppressed() {
    let db = setup_orderby5_db();
    let output =
        eqp(&db, "SELECT DISTINCT a COLLATE nocase, c, b FROM t1 WHERE a='xyz' COLLATE nocase");

    assert!(
        !output.contains("USE TEMP B-TREE FOR DISTINCT"),
        "matching nocase pin/key must suppress the DISTINCT line:\n{}",
        output
    );
}

// orderby5 1.7: `+a=0` is not a bare equality (the unary `+` blocks the pin),
// so `a` stays in the key; (c, b, a) is not deliverable by t1bc → line stays.
#[test]
fn test_distinct_non_equality_predicate_keeps_line() {
    let db = setup_orderby5_db();
    let output = eqp(&db, "SELECT DISTINCT c, b, a FROM t1 WHERE +a=0");

    assert!(
        output.contains("USE TEMP B-TREE FOR DISTINCT"),
        "`+a=0` does not pin `a`, so the key is not deliverable:\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// Constant-only compound + ORDER BY (orderby1 5.1, issue #5713)
// ---------------------------------------------------------------------------

// orderby1 5.1: every branch is a constant-row query, so SQLite sorts the tiny
// result with no temp B-tree. `SELECT 5 UNION ALL SELECT 3 ORDER BY 1` shows no
// trailing `USE TEMP B-TREE FOR ORDER BY` line.
#[test]
fn test_constant_compound_order_by_no_temp_btree() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT 5 UNION ALL SELECT 3 ORDER BY 1");

    assert!(
        !output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "all-constant compound ORDER BY needs no temp B-tree:\n{}",
        output
    );
}

// A compound with at least one table-backed branch still emits the trailing
// sort line (the carve-out is narrow — only all-constant compounds qualify).
#[test]
fn test_table_backed_compound_order_by_keeps_temp_btree() {
    let db = setup_compound_db();
    let output = eqp(&db, "SELECT a FROM u1 UNION ALL SELECT 3 ORDER BY 1");

    assert!(
        output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "a table-backed branch must keep the trailing sort line:\n{}",
        output
    );
}
