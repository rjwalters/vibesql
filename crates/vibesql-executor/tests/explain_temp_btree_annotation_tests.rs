//! Tests for EXPLAIN QUERY PLAN temp-structure annotations (#5367):
//! `USE TEMP B-TREE FOR GROUP BY`, `USE TEMP B-TREE FOR DISTINCT`, the
//! `<OP> USING TEMP B-TREE` compound dedup branch labels, and the
//! `CO-ROUTINE <alias>` wrapper for non-flattenable derived tables.
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
// suppressed (sqlite3 suppresses too: `SCAN t1 USING COVERING INDEX i1a`
// alone). Scan-line divergence: VibeSQL's runtime hash-groups over a plain
// scan, so the pre-existing base-scan rendering shows `SCAN t1` (#5355
// notes); the suppression itself matches SQLite.
#[test]
fn test_group_by_indexed_suppresses_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, count(*) FROM t1 GROUP BY a");

    assert!(!output.contains("USE TEMP B-TREE"), "indexed GROUP BY must suppress:\n{}", output);
}

// GROUP BY on two columns when the index only covers the first. sqlite3:
//   |--SCAN t1 USING INDEX i1a        [scan-line divergence: SCAN t1]
//   `--USE TEMP B-TREE FOR GROUP BY
#[test]
fn test_group_by_partially_indexed_emits_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT a, b, count(*) FROM t1 GROUP BY a, b");

    assert!(
        output.contains("USE TEMP B-TREE FOR GROUP BY"),
        "partially indexed GROUP BY needs the temp structure:\n{}",
        output
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
// the (a, b) index and suppresses the line (verified live:
// `SCAN tab USING COVERING INDEX iab` alone). VibeSQL retries the group key
// permuted into each index's column order.
#[test]
fn test_group_by_order_insensitive_index_match_suppresses() {
    let db = setup_composite_db();
    let output = eqp(&db, "SELECT b, a, count(*) FROM tab GROUP BY b, a");

    assert!(
        !output.contains("USE TEMP B-TREE"),
        "reordered GROUP BY matching the (a, b) index must suppress:\n{}",
        output
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
// (`SCAN t1 USING COVERING INDEX i1a` for count(*) — scan-line divergence).
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
// is suppressed (sqlite3: `SCAN t1 USING COVERING INDEX i1a` alone —
// scan-line divergence as usual).
#[test]
fn test_distinct_indexed_suppresses_temp_btree() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM t1");

    assert!(!output.contains("USE TEMP B-TREE"), "indexed DISTINCT must suppress:\n{}", output);
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
// `SELECT DISTINCT a ... GROUP BY a` even with the index (verified live).
#[test]
fn test_distinct_not_suppressed_by_index_when_grouped() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM t1 GROUP BY a");

    assert!(
        !output.contains("USE TEMP B-TREE FOR GROUP BY"),
        "indexed group key must suppress the GROUP BY line:\n{}",
        output
    );
    assert!(
        output.contains("USE TEMP B-TREE FOR DISTINCT"),
        "DISTINCT after grouping still dedups:\n{}",
        output
    );
}

// Indexed DISTINCT + reverse ORDER BY: the dedup rides the index and the
// reverse traversal satisfies the DESC order — no lines at all. sqlite3 —
// no temp B-tree lines (verified live; scan-line divergence as usual).
#[test]
fn test_distinct_indexed_order_by_desc_no_lines() {
    let db = setup_db();
    let output = eqp(&db, "SELECT DISTINCT a FROM t1 ORDER BY a DESC");

    assert!(
        !output.contains("USE TEMP B-TREE"),
        "index-backed dedup + reversible order needs no temp lines:\n{}",
        output
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
