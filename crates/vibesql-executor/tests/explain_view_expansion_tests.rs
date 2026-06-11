//! Tests for EXPLAIN QUERY PLAN expansion of views and subqueries
//! (#5347/#5355, windowpushd.test)
//!
//! SQLite renders a view/subquery containing window functions as a
//! `CO-ROUTINE <name>` block whose inner plan shows the real table access
//! path — including index probes for predicates pushed down by the window
//! WHERE push-down (#5292) and index scans chosen purely to deliver
//! PARTITION BY order. Expected shapes below verified against sqlite3
//! (modulo SQLite's extra nested `(subquery-N)` co-routine layers, which
//! VibeSQL flattens into a single block).
//!
//! Plain (window-free) flattenable views inline their body's plan into the
//! outer output with no mention of the view name (#5355), matching SQLite's
//! flattener. One documented divergence: SQLite pushes the OUTER WHERE into
//! the flattened body (`SEARCH t USING INDEX ... (x=?)`); VibeSQL's runtime
//! materializes views and post-filters, so EQP truthfully shows the body's
//! own plan (`SCAN t`) without fabricating an index probe.

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

/// windowpushd.test section-1 schema: INTEGER PRIMARY KEY (rowid alias)
/// plus an index on the partition column.
fn setup_section1_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, grp_id INTEGER)");
    run_ddl(&mut db, "CREATE INDEX i1 ON t1(grp_id)");
    run_ddl(
        &mut db,
        "CREATE VIEW lll AS SELECT row_number() OVER (PARTITION BY grp_id), grp_id, id FROM t1",
    );
    db
}

/// windowpushd.test section-2 schema: plain rowid table, two single-column
/// indexes, window views over each.
fn setup_section2_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t2(a INTEGER, b INTEGER, c INTEGER, d INTEGER)");
    run_ddl(&mut db, "CREATE INDEX i2a ON t2(a)");
    run_ddl(&mut db, "CREATE INDEX i2b ON t2(b)");
    run_ddl(
        &mut db,
        "CREATE VIEW v3 AS SELECT b, d, max(d) OVER (PARTITION BY b), \
         row_number() OVER (PARTITION BY b) FROM t2",
    );
    db
}

// ---------------------------------------------------------------------------
// View expansion
// ---------------------------------------------------------------------------

// windowpushd.test 1.4: the pushed equality predicate probes the index, and
// the INTEGER PRIMARY KEY (rowid alias) does not disqualify the covering
// index. SQLite: SEARCH t1 USING COVERING INDEX i1 (grp_id=?).
#[test]
fn test_view_expansion_with_pushed_predicate_covering_index() {
    let db = setup_section1_db();
    let output = eqp(&db, "SELECT * FROM lll WHERE grp_id = 2");

    assert!(output.contains("CO-ROUTINE lll"), "missing CO-ROUTINE block:\n{}", output);
    assert!(
        output.contains("SEARCH t1 USING COVERING INDEX i1 (grp_id=?)"),
        "missing inner index probe:\n{}",
        output
    );
    assert!(output.contains("SCAN lll"), "missing outer scan of the co-routine:\n{}", output);
    // The index delivers PARTITION BY grp_id order — no window sort.
    assert!(!output.contains("USE TEMP B-TREE"), "unexpected temp B-tree:\n{}", output);
}

// windowpushd.test 2.1.3.3: equality push-down through a non-covering index
// (d is not in i2b). SQLite: SEARCH t1 USING INDEX i2 (b=?).
#[test]
fn test_view_expansion_pushed_equality_non_covering() {
    let db = setup_section2_db();
    let output = eqp(&db, "SELECT * FROM v3 WHERE b = 5");

    assert!(output.contains("CO-ROUTINE v3"), "missing CO-ROUTINE block:\n{}", output);
    assert!(
        output.contains("SEARCH t2 USING INDEX i2b (b=?)"),
        "missing inner index probe:\n{}",
        output
    );
}

// windowpushd.test 2.1.3.4: range push-down. The range scan delivers index
// order for PARTITION BY b — no temp B-tree (verified against sqlite3).
#[test]
fn test_view_expansion_pushed_range_no_window_sort() {
    let db = setup_section2_db();
    let output = eqp(&db, "SELECT * FROM v3 WHERE b > 3");

    assert!(
        output.contains("SEARCH t2 USING INDEX i2b (b>?)"),
        "missing inner range probe:\n{}",
        output
    );
    assert!(!output.contains("USE TEMP B-TREE"), "unexpected temp B-tree:\n{}", output);
}

// windowpushd.test 2.1.3.6: nothing is pushable (d is not a PARTITION BY
// column), but the index is chosen because it delivers PARTITION BY b order
// inside the expanded view. SQLite: SCAN t1 USING INDEX i2.
#[test]
fn test_view_expansion_index_for_partition_order_without_push() {
    let db = setup_section2_db();
    let output = eqp(&db, "SELECT * FROM v3 WHERE d < 0.55");

    assert!(output.contains("CO-ROUTINE v3"), "missing CO-ROUTINE block:\n{}", output);
    assert!(output.contains("SCAN t2 USING INDEX i2b"), "missing ordering index scan:\n{}", output);
    assert!(
        !output.contains("SEARCH t2"),
        "no predicate was pushed; inner scan must not be a SEARCH:\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// Subquery expansion
// ---------------------------------------------------------------------------

// An inline window subquery renders the same CO-ROUTINE block as a view.
#[test]
fn test_window_subquery_renders_coroutine_with_pushed_predicate() {
    let db = setup_section1_db();
    let output = eqp(
        &db,
        "SELECT * FROM (SELECT grp_id, id, row_number() OVER (PARTITION BY grp_id) FROM t1) AS w \
         WHERE grp_id = 2",
    );

    assert!(output.contains("CO-ROUTINE w"), "missing CO-ROUTINE block:\n{}", output);
    assert!(
        output.contains("SEARCH t1 USING COVERING INDEX i1 (grp_id=?)"),
        "missing inner index probe:\n{}",
        output
    );
    assert!(output.contains("SCAN w"), "missing outer scan of the co-routine:\n{}", output);
}

// ---------------------------------------------------------------------------
// Plain-view flattening (#5355)
// ---------------------------------------------------------------------------

/// Schema for plain-view flattening tests: base table, index, and a set of
/// flattenable and non-flattenable (blocked) views.
fn setup_plain_view_db() -> Database {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t3(x INTEGER, y INTEGER)");
    run_ddl(&mut db, "CREATE INDEX i3x ON t3(x)");
    run_ddl(&mut db, "CREATE VIEW pv AS SELECT x, y FROM t3");
    run_ddl(&mut db, "CREATE VIEW fv AS SELECT x, y FROM t3 WHERE x = 5");
    run_ddl(&mut db, "CREATE VIEW av AS SELECT x, count(*) AS c FROM t3 GROUP BY x");
    run_ddl(&mut db, "CREATE VIEW sv AS SELECT count(*) AS c FROM t3");
    run_ddl(&mut db, "CREATE VIEW lv AS SELECT x FROM t3 LIMIT 5");
    run_ddl(&mut db, "CREATE VIEW dv AS SELECT DISTINCT x FROM t3");
    run_ddl(&mut db, "CREATE VIEW cv AS SELECT x FROM t3 UNION SELECT y FROM t3");
    run_ddl(&mut db, "CREATE VIEW pv2 AS SELECT x FROM pv");
    run_ddl(&mut db, "CREATE VIEW ov AS SELECT x, y FROM t3 ORDER BY y");
    run_ddl(&mut db, "CREATE VIEW ovx AS SELECT x, y FROM t3 ORDER BY x");
    run_ddl(&mut db, "CREATE VIEW ov2 AS SELECT x, y FROM ov");
    db
}

// A plain view's body is inlined: the underlying table scan appears with no
// mention of the view. SQLite (no usable outer predicate): `SCAN t3` —
// identical shape.
#[test]
fn test_plain_view_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM pv");

    assert!(!output.contains("CO-ROUTINE"), "plain view must not co-routine:\n{}", output);
    assert!(!output.contains("pv"), "view name must not appear:\n{}", output);
    assert!(output.contains("SCAN t3"), "expected flattened inner scan:\n{}", output);
}

// Outer WHERE on a flattened view: SQLite pushes the predicate into the
// flattened body (`SEARCH t3 USING INDEX i3x (x=?)`). VibeSQL's runtime
// materializes the view and post-filters, so EQP truthfully shows the body's
// own access path (`SCAN t3`) — documented divergence (#5355).
#[test]
fn test_plain_view_flattened_outer_where_not_fabricated() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM pv WHERE x = 1");

    assert!(!output.contains("CO-ROUTINE"), "plain view must not co-routine:\n{}", output);
    assert!(!output.contains("SCAN pv"), "view name must not appear:\n{}", output);
    assert!(output.contains("SCAN t3"), "expected truthful inner scan:\n{}", output);
    assert!(
        !output.contains("SEARCH t3"),
        "runtime does not push the outer WHERE into the view; no index probe:\n{}",
        output
    );
}

// A view body with its OWN indexed WHERE shows the real index probe.
// SQLite: `SEARCH t3 USING INDEX i3x (x=?)` — identical shape (the probe is
// part of the body, which the runtime genuinely executes).
#[test]
fn test_plain_view_flattened_body_where_uses_index() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM fv");

    assert!(
        output.contains("SEARCH t3 USING COVERING INDEX i3x (x=?)")
            || output.contains("SEARCH t3 USING INDEX i3x (x=?)"),
        "expected inner index probe from the view body's own WHERE:\n{}",
        output
    );
    assert!(!output.contains("SCAN fv"), "view name must not appear:\n{}", output);
}

// Nested plain view-on-view flattens all the way down to the base table.
// SQLite: `SCAN t3` — identical shape.
#[test]
fn test_nested_plain_view_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM pv2");

    assert!(output.contains("SCAN t3"), "expected base-table scan:\n{}", output);
    assert!(!output.contains("pv"), "no view name may appear:\n{}", output);
}

// A view over a join flattens to the join's scans. SQLite shows both base
// tables (modulo automatic-index/bloom-filter lines).
#[test]
fn test_plain_join_view_flattened() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t3(x INTEGER, y INTEGER)");
    run_ddl(&mut db, "CREATE TABLE t4(a INTEGER, b INTEGER)");
    run_ddl(&mut db, "CREATE VIEW jv AS SELECT t3.x, t4.b FROM t3, t4 WHERE t3.y = t4.a");
    let output = eqp(&db, "SELECT * FROM jv");

    assert!(!output.contains("SCAN jv"), "view name must not appear:\n{}", output);
    assert!(output.contains("t3"), "expected left base table:\n{}", output);
    assert!(output.contains("t4"), "expected right base table:\n{}", output);
}

// An ORDER BY view body still flattens, and the body's sorting pass keeps
// its temp B-tree line (the runtime genuinely sorts: views are
// materialized). SQLite: `SCAN t3` + `USE TEMP B-TREE FOR ORDER BY` —
// identical shape (verified against sqlite3 3.51.0).
#[test]
fn test_order_by_view_flattened_keeps_temp_btree() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM ov");

    assert!(!output.contains("SCAN ov"), "view name must not appear:\n{}", output);
    assert!(output.contains("SCAN t3"), "expected flattened inner scan:\n{}", output);
    assert!(
        output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "body ORDER BY sorts at runtime; temp B-tree line must render:\n{}",
        output
    );
}

// An ORDER BY view whose sort is satisfied by an index flattens with NO
// temp B-tree, and the flattened plan is identical to the bare body's plan.
// SQLite annotates the ordering index (`SCAN t3 USING INDEX i3x`); VibeSQL's
// pre-existing base-scan rendering shows `SCAN t3` for pure ordering indexes
// (same for the bare body — unrelated to view flattening).
#[test]
fn test_order_by_view_flattened_index_satisfied_no_temp_btree() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM ovx");
    let bare = eqp(&db, "SELECT x, y FROM t3 ORDER BY x");

    assert!(!output.contains("SCAN ovx"), "view name must not appear:\n{}", output);
    assert!(output.contains("SCAN t3"), "expected flattened inner scan:\n{}", output);
    assert!(!output.contains("USE TEMP B-TREE"), "index satisfies the sort:\n{}", output);
    assert_eq!(output, bare, "flattened plan must match the bare body's plan");
}

// A nested plain view over an ORDER BY view: the inner body's sort flag must
// survive the extra Subquery nesting level. SQLite: `SCAN t3` +
// `USE TEMP B-TREE FOR ORDER BY` — identical shape.
#[test]
fn test_nested_order_by_view_flattened_keeps_temp_btree() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM ov2");

    assert!(!output.contains("SCAN ov"), "no view name may appear:\n{}", output);
    assert!(output.contains("SCAN t3"), "expected flattened inner scan:\n{}", output);
    assert!(
        output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "inner body ORDER BY sorts at runtime; temp B-tree line must render:\n{}",
        output
    );
}

// Outer-query ORDER BY over a flattened plain view (no ORDER BY in the
// body): the outer sort's temp B-tree line renders. SQLite: `SCAN t3` +
// `USE TEMP B-TREE FOR ORDER BY` — identical shape.
#[test]
fn test_outer_order_by_over_flattened_view_keeps_temp_btree() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM pv ORDER BY y");

    assert!(!output.contains("SCAN pv"), "view name must not appear:\n{}", output);
    assert!(output.contains("SCAN t3"), "expected flattened inner scan:\n{}", output);
    assert!(
        output.contains("USE TEMP B-TREE FOR ORDER BY"),
        "outer ORDER BY needs a sorting pass:\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// Blocked view bodies keep the opaque rendering (#5355)
// ---------------------------------------------------------------------------
// SQLite renders all of these as `CO-ROUTINE <view>` blocks; VibeSQL
// conservatively keeps the pre-existing opaque `SCAN <view>` line rather
// than guessing (follow-on work can add the CO-ROUTINE shape per case).

#[test]
fn test_aggregate_group_by_view_not_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM av WHERE x = 1");

    assert!(output.contains("SCAN av"), "expected opaque view scan:\n{}", output);
    assert!(!output.contains("SCAN t3"), "blocked body must not inline:\n{}", output);
}

#[test]
fn test_scalar_aggregate_view_not_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM sv");

    assert!(output.contains("SCAN sv"), "expected opaque view scan:\n{}", output);
    assert!(!output.contains("SCAN t3"), "blocked body must not inline:\n{}", output);
}

#[test]
fn test_limit_view_not_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM lv WHERE x = 1");

    assert!(output.contains("SCAN lv"), "expected opaque view scan:\n{}", output);
    assert!(!output.contains("SCAN t3"), "blocked body must not inline:\n{}", output);
}

#[test]
fn test_distinct_view_not_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM dv WHERE x = 1");

    assert!(output.contains("SCAN dv"), "expected opaque view scan:\n{}", output);
    assert!(!output.contains("SCAN t3"), "blocked body must not inline:\n{}", output);
}

#[test]
fn test_compound_view_not_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "SELECT * FROM cv WHERE x = 1");

    assert!(output.contains("SCAN cv"), "expected opaque view scan:\n{}", output);
    assert!(!output.contains("SCAN t3"), "blocked body must not inline:\n{}", output);
}

// A WITH-clause CTE shadows a same-named plain view: the view body must NOT
// be inlined (CTE precedence, same gate as the window-view path).
#[test]
fn test_cte_shadowing_plain_view_not_flattened() {
    let db = setup_plain_view_db();
    let output = eqp(&db, "WITH pv AS (SELECT 1 AS x, 2 AS y) SELECT * FROM pv WHERE x = 1");

    assert!(!output.contains("SCAN t3"), "view body must not be planned:\n{}", output);
}

// ---------------------------------------------------------------------------
// No expansion
// ---------------------------------------------------------------------------

// Plain derived tables keep the existing flat rendering (SQLite flattens
// them — no CO-ROUTINE, no SCAN of the alias).
#[test]
fn test_plain_subquery_not_rendered_as_coroutine() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t3(x INTEGER, y INTEGER)");
    let output = eqp(&db, "SELECT * FROM (SELECT x, y FROM t3) AS s WHERE x = 1");

    assert!(!output.contains("CO-ROUTINE"), "plain subquery must not expand:\n{}", output);
    assert!(output.contains("SCAN t3"), "expected flattened inner scan:\n{}", output);
}

// A WITH-clause CTE shadows a same-named window view: the CTE must NOT be
// expanded as the view (CTE precedence, judge regression on PR #5349).
#[test]
fn test_cte_shadowing_window_view_not_expanded() {
    let db = setup_section1_db();
    let output = eqp(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         SELECT * FROM lll WHERE grp_id = 2",
    );

    assert!(
        !output.contains("CO-ROUTINE lll"),
        "CTE shadows the view; no expansion allowed:\n{}",
        output
    );
    assert!(!output.contains("SEARCH t1"), "view body must not be planned:\n{}", output);
}
