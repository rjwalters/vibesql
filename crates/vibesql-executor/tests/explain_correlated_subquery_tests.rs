//! Tests for EXPLAIN QUERY PLAN rendering of **un-flattenable
//! `EXISTS`/`IN`/scalar-subquery expressions** (#6647 — existsexpr.test
//! 3.7.1/3.9.1/4.4.1).
//!
//! SQLite's exists-to-join optimizer rewrites a *simple* correlated
//! WHERE-clause `EXISTS`/`IN` (single base table in the subquery's FROM, no
//! bare aggregate) into a semi-join that leaves no trace in the EQP output —
//! just the bare outer `SCAN`. Everything it cannot rewrite keeps its own
//! labelled plan entry with the subquery's own plan nested underneath:
//!
//! ```text
//! QUERY PLAN
//! |--SCAN y1
//! `--CORRELATED SCALAR SUBQUERY 1
//!    `--SCAN y2
//! ```
//!
//! The cases covered here are exactly the four existsexpr.test failures the
//! issue enumerates, plus the *negative* cases that pin down the boundary —
//! most importantly the regression guard in
//! `test_where_exists_with_nested_non_aggregate_subquery_has_no_subquery_node`,
//! which proves the aggregate-without-GROUP-BY gate uses the narrow
//! `contains_real_aggregate_function` helper rather than the conservative
//! `contains_aggregate_function` (which reports "aggregate" for *any* nested
//! subquery expression and would therefore emit a spurious node here).
//!
//! Label caveat: VibeSQL currently renders every such entry as
//! `CORRELATED SCALAR SUBQUERY <n>`; sqlite3 distinguishes correlated from
//! uncorrelated (`SCALAR SUBQUERY <n>`) and numbers entries from its own
//! internal subquery-id counter. existsexpr.test only asserts the substring
//! `SUBQUERY`, so the distinction is not yet conformance-visible; these tests
//! pin the text VibeSQL actually emits so a future change to it is deliberate.

use vibesql_ast::Statement;
use vibesql_executor::ExplainExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn run(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateIndex(s) => {
            vibesql_executor::CreateIndexExecutor::execute(&s, db).unwrap();
        }
        Statement::Insert(i) => {
            vibesql_executor::InsertExecutor::execute(db, &i).unwrap();
        }
        other => panic!("unsupported setup statement: {other:?}"),
    }
}

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

/// The existsexpr.test fixture: `y1(a,b,c)` / `y2(x,y,z)` for the WHERE-clause
/// cases (section 3), `tx1(x)` / `tx2(y)` for the SELECT-list cases
/// (section 4). No indexes — sqlite3's own existsexpr.test section 3 runs
/// without them for these sub-tests, and the outer access path is a plain
/// SCAN either way.
fn setup() -> Database {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE y1(a, b, c)");
    run(&mut db, "CREATE TABLE y2(x, y, z)");
    run(&mut db, "CREATE TABLE tx1(x)");
    run(&mut db, "CREATE TABLE tx2(y)");
    db
}

// ---------------------------------------------------------------------------
// Positive cases — the four existsexpr.test failures from #6647
// ---------------------------------------------------------------------------

/// existsexpr.test 3.7.1: an aggregate-without-GROUP-BY subquery always
/// returns exactly one row, so it can never become a per-outer-row join
/// predicate — it keeps its own entry.
#[test]
fn test_where_exists_aggregate_without_group_by_renders_subquery_node() {
    let db = setup();
    let output =
        eqp(&db, "SELECT * FROM y1 WHERE EXISTS (SELECT count(*) FROM y2 WHERE z=a-1 AND y=a-1)");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN y1\n\
         `--CORRELATED SCALAR SUBQUERY 1\n\
        \x20  `--SCAN y2\n"
    );
}

/// existsexpr.test 3.9.1: a multi-table subquery FROM clause is outside the
/// single-base-table shape the exists-to-join rewrite handles.
#[test]
fn test_where_exists_multi_table_from_renders_subquery_node() {
    let db = setup();
    let output = eqp(
        &db,
        "SELECT * FROM y1 WHERE EXISTS (\
         SELECT 1 FROM y2 one, y2 two WHERE one.z=a-1 AND one.y=a-1)",
    );

    // Both subquery-side scans nest underneath the subquery entry rather than
    // becoming siblings of the outer scan. VibeSQL renders each by base-table
    // name (`SCAN y2`) where sqlite3 uses the FROM-clause alias
    // (`SCAN one` / `SCAN two`) — a pre-existing, separate alias-rendering gap
    // that existsexpr.test's substring assertion does not exercise.
    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN y1\n\
         `--CORRELATED SCALAR SUBQUERY 1\n\
        \x20  |--SCAN y2\n\
        \x20  `--SCAN y2\n"
    );
}

/// existsexpr.test 4.4.1 (1st occurrence): a SELECT-list `EXISTS` is never
/// rewritten into a join, correlated or not.
#[test]
fn test_select_list_exists_renders_subquery_node() {
    let db = setup();
    let output = eqp(&db, "SELECT EXISTS ( SELECT x FROM tx1 ) FROM tx2");

    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SCAN tx2\n\
         `--CORRELATED SCALAR SUBQUERY 1\n\
        \x20  `--SCAN tx1\n"
    );
}

/// existsexpr.test 4.4.1 (2nd occurrence): an `EXISTS` nested inside a
/// SELECT-list scalar subquery. The outer scalar subquery gets the entry and
/// the inner `EXISTS` nests inside it via the recursive `explain_select` walk.
#[test]
fn test_select_list_scalar_subquery_wrapping_exists_renders_nested_subquery_nodes() {
    let db = setup();
    let output = eqp(&db, "SELECT (SELECT EXISTS ( SELECT x FROM tx1 ) WHERE 1) FROM tx2");

    assert!(
        output.starts_with("QUERY PLAN\n|--SCAN tx2\n"),
        "expected outer scan first:\n{}",
        output
    );
    assert_eq!(
        output.matches("SUBQUERY").count(),
        2,
        "expected the outer scalar subquery and the nested EXISTS to each get an entry:\n{}",
        output
    );
    assert!(output.contains("SCAN tx1"), "expected the inner EXISTS body's scan:\n{}", output);
}

// ---------------------------------------------------------------------------
// Negative cases — the flattening boundary must stay where it is
// ---------------------------------------------------------------------------

/// The common case: a simple single-table correlated WHERE-clause `EXISTS`
/// with no aggregate is folded into a semi-join and leaves no EQP trace.
#[test]
fn test_where_exists_simple_correlated_has_no_subquery_node() {
    let db = setup();
    let output = eqp(&db, "SELECT * FROM y1 WHERE EXISTS (SELECT 1 FROM y2 WHERE z=a-1)");

    assert_eq!(output, "QUERY PLAN\n`--SCAN y1\n");
}

/// Regression guard for the false positive #6647 called out and the Judge
/// caught on the first cut of this PR: the subquery's SELECT list contains a
/// *nested, non-aggregate* subquery expression. The conservative
/// `contains_aggregate_function` helper reports "contains aggregate" for any
/// `ScalarSubquery`/`In`/`Exists`, so reusing it here would misclassify this
/// perfectly flattenable subquery as "aggregate without GROUP BY" and emit a
/// spurious `CORRELATED SCALAR SUBQUERY` entry. The narrow
/// `contains_real_aggregate_function` helper must not.
#[test]
fn test_where_exists_with_nested_non_aggregate_subquery_has_no_subquery_node() {
    let db = setup();
    let output =
        eqp(&db, "SELECT * FROM y1 WHERE EXISTS (SELECT (SELECT 1) FROM y2 WHERE y2.z = y1.a)");

    assert!(
        !output.contains("SUBQUERY"),
        "a nested non-aggregate subquery in the subquery's SELECT list must not \
         be mistaken for an aggregate (#6647):\n{}",
        output
    );
    assert_eq!(output, "QUERY PLAN\n`--SCAN y1\n");
}

/// A real aggregate *with* `GROUP BY` returns one row per group, so it can
/// still drive a semi-join — no entry.
#[test]
fn test_where_exists_aggregate_with_group_by_has_no_subquery_node() {
    let db = setup();
    let output =
        eqp(&db, "SELECT * FROM y1 WHERE EXISTS (SELECT count(*) FROM y2 WHERE z=a-1 GROUP BY y)");

    assert!(
        !output.contains("SUBQUERY"),
        "an aggregate subquery with GROUP BY stays flattenable:\n{}",
        output
    );
}

/// A nested non-aggregate subquery buried inside a larger SELECT-list
/// expression is also not an aggregate — the narrow helper must stay narrow
/// through the whole expression walk, not just at the top level.
#[test]
fn test_where_exists_with_nested_subquery_inside_expression_has_no_subquery_node() {
    let db = setup();
    let output = eqp(
        &db,
        "SELECT * FROM y1 WHERE EXISTS (SELECT (SELECT 1) + y2.x FROM y2 WHERE y2.z = y1.a)",
    );

    assert!(
        !output.contains("SUBQUERY"),
        "nested subquery inside a larger expression is still not an aggregate:\n{}",
        output
    );
}

/// ...but a real aggregate nested inside a larger SELECT-list expression IS
/// one, so the walk must still find it.
#[test]
fn test_where_exists_aggregate_inside_expression_renders_subquery_node() {
    let db = setup();
    let output =
        eqp(&db, "SELECT * FROM y1 WHERE EXISTS (SELECT count(*) + 1 FROM y2 WHERE y2.z = y1.a)");

    assert!(
        output.contains("CORRELATED SCALAR SUBQUERY 1"),
        "an aggregate nested in a larger expression still blocks flattening:\n{}",
        output
    );
}

/// A plain query with no subquery expression at all is untouched by the new
/// code path.
#[test]
fn test_plain_select_unchanged() {
    let db = setup();
    assert_eq!(eqp(&db, "SELECT * FROM y1"), "QUERY PLAN\n`--SCAN y1\n");
    assert_eq!(eqp(&db, "SELECT a FROM y1 WHERE b = 1"), "QUERY PLAN\n`--SCAN y1\n");
}
