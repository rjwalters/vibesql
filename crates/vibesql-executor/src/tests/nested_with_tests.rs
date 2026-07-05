//! Tests for WITH clauses nested inside CTE bodies (issue #5838, PR A)
//!
//! A CTE body may carry its own WITH clause. Nested CTE names shadow outer
//! CTEs, sibling CTEs, and real tables; unreferenced CTEs are never executed
//! (SQLite lazy expansion); references that can only resolve to a CTE whose
//! definition is still in progress are circular. Scenarios mirror SQLite's
//! with1.test / with2.test and were verified against sqlite3.

use vibesql_ast::Statement;
use vibesql_parser::Parser;

use super::super::*;

fn execute_sql(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let stmt = Parser::parse_sql(sql).map_err(|e| ExecutorError::ParseError(format!("{:?}", e)))?;
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)?;
            Ok(vec![])
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)?;
            Ok(vec![])
        }
        Statement::Select(select_stmt) => {
            let result = SelectExecutor::new(db).execute(&select_stmt)?;
            Ok(result)
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Unsupported statement type: {:?}",
            stmt
        ))),
    }
}

fn int(value: i64) -> vibesql_types::SqlValue {
    vibesql_types::SqlValue::Integer(value)
}

/// Issue reproducer: a nested CTE shadowing a real table must return the CTE's
/// rows, not the table's rows. sqlite3 returns 5 here.
#[test]
fn test_nested_with_shadows_real_table() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE b(y INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO b VALUES(999)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH a(x) AS (WITH b(y) AS (SELECT 5) SELECT y FROM b) SELECT x FROM a",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], int(5));
}

/// with1.test 3.4: a nested CTE shadows an outer sibling CTE of the same name.
#[test]
fn test_nested_with_shadows_outer_sibling() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t3(x INTEGER)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t4(x INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO t3 VALUES(3)").unwrap();
    execute_sql(&mut db, "INSERT INTO t4 VALUES(4)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH tmp AS (SELECT * FROM t3),
              tmp2 AS (WITH tmp AS (SELECT * FROM t4) SELECT * FROM tmp)
         SELECT * FROM tmp2",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], int(4));
}

/// with1.test 3.5 (regression guard): a nested CTE with a *different* name
/// leaves references to the outer CTE intact.
#[test]
fn test_nested_with_does_not_shadow_different_name() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t3(x INTEGER)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t4(x INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO t3 VALUES(3)").unwrap();
    execute_sql(&mut db, "INSERT INTO t4 VALUES(4)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH tmp AS (SELECT * FROM t3),
              tmp2 AS (WITH xxxx AS (SELECT * FROM t4) SELECT * FROM tmp)
         SELECT * FROM tmp2",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], int(3));
}

/// with1.test 17.6: a CTE body may reference outer CTEs materialized *before*
/// the shadowing definition existed (x2 keeps seeing the outer x1).
#[test]
fn test_nested_with_outer_cte_precedence() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH
           x1 AS (SELECT 10),
           x2 AS (SELECT * FROM x1),
           x3 AS (
             WITH x1 AS (SELECT 11)
             SELECT * FROM x2 UNION ALL SELECT * FROM x1
           )
         SELECT * FROM x3",
    )
    .unwrap();
    let values: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(values, vec![int(10), int(11)]);
}

/// with1.test 17.1: the nested CTE is referenced from the recursive-looking
/// (UNION ALL) compound body of the outer CTE.
#[test]
fn test_nested_with_in_compound_body() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH x(a) AS (
           WITH y(b) AS (SELECT 10)
           SELECT 9 UNION ALL SELECT * FROM y
         )
         SELECT * FROM x",
    )
    .unwrap();
    let values: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(values, vec![int(9), int(10)]);
}

/// Three-level nesting: the innermost definition wins.
#[test]
fn test_three_level_nested_with() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t(v INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO t VALUES(1)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH a AS (
           WITH b AS (
             WITH t AS (SELECT 42 AS v)
             SELECT v FROM t
           )
           SELECT v FROM b
         )
         SELECT v FROM a",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], int(42));
}

/// with1.test 21.1: a RECURSIVE keyword plus a nested CTE that shadows the
/// outer name means the outer CTE is NOT recursive - the inner reference
/// resolves to the nested CTE.
#[test]
fn test_recursive_keyword_with_inner_shadow_is_not_recursive() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE t21(a,b) AS (
           WITH t21(x) AS (VALUES(1))
           SELECT x, x FROM t21 ORDER BY 1
         )
         SELECT * FROM t21 AS tA, t21 AS tB",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.to_vec(), vec![int(1), int(1), int(1), int(1)]);
}

/// with2.test 1.11: within a nested WITH list, sibling references resolve
/// regardless of declaration order (j references the *later* sibling i, which
/// shadows the outer i being defined).
#[test]
fn test_nested_with_forward_sibling_reference() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(x INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(2)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH i(x) AS (
           WITH j(x) AS (SELECT * FROM i),
                i(x) AS (SELECT * FROM t1)
           SELECT * FROM j
         )
         SELECT * FROM i ORDER BY 1",
    )
    .unwrap();
    let values: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(values, vec![int(1), int(2)]);
}

/// with2.test 3.5: a nested CTE referencing the outer CTE currently being
/// defined is a circular reference.
#[test]
fn test_nested_with_circular_reference_to_enclosing() {
    let mut db = vibesql_storage::Database::new();
    let err = execute_sql(
        &mut db,
        "WITH i(x) AS (
           WITH j(x) AS (SELECT * FROM i)
           SELECT * FROM j
         )
         SELECT * FROM i",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: i");
}

/// with1.test 17.3: a recursive CTE may only reference itself in the recursive
/// term; a base-term self-reference is circular.
#[test]
fn test_recursive_cte_base_term_self_reference_is_circular() {
    let mut db = vibesql_storage::Database::new();
    let err = execute_sql(
        &mut db,
        "WITH i AS (
           WITH j AS (SELECT 5)
           SELECT 5 FROM i UNION SELECT 8 FROM i
         )
         SELECT * FROM i",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: i");
}

/// with2.test 11.5-style: an unreferenced CTE is never executed, so errors
/// inside its body are never reported (SQLite lazy expansion).
#[test]
fn test_unreferenced_cte_is_not_executed() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(55)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH x AS (
           WITH y AS (
             WITH z AS (SELECT * FROM t1)
             SELECT * FROM no_such_table
           ) SELECT a
         )
         SELECT * FROM t1",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], int(55));
}

/// with2.test 9.1: a nested WITH inside a CTE that is referenced multiple
/// times, including from a compound derived table.
#[test]
fn test_nested_with_multiple_references() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH xyz(a) AS (
           WITH abc AS (SELECT 1234) SELECT * FROM abc
         )
         SELECT * FROM xyz AS one, xyz AS two, (
           SELECT * FROM xyz UNION ALL SELECT * FROM xyz
         )",
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.values.to_vec(), vec![int(1234), int(1234), int(1234)]);
    }
}

/// Regression guard: plain recursive CTEs still work (referenced CTE, nested
/// WITH absent).
#[test]
fn test_plain_recursive_cte_still_works() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE cnt(x) AS (
           SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5
         )
         SELECT x FROM cnt",
    )
    .unwrap();
    let values: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(values, vec![int(1), int(2), int(3), int(4), int(5)]);
}

/// Regression guard: a recursive CTE nested inside another CTE's WITH clause.
#[test]
fn test_recursive_cte_inside_nested_with() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH outer_cte AS (
           WITH RECURSIVE cnt(x) AS (
             SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 3
           )
           SELECT * FROM cnt
         )
         SELECT * FROM outer_cte",
    )
    .unwrap();
    let values: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(values, vec![int(1), int(2), int(3)]);
}

/// Issue #5838, item 3: the RECURSIVE keyword is advisory. A CTE in a
/// `WITH RECURSIVE` list that does not reference itself runs as an ordinary
/// CTE instead of erroring "must use UNION ALL". sqlite3 returns 1 here. This
/// is the shape that blocks the mandelbrot/sudoku showcase queries, where a
/// RECURSIVE list mixes recursive and non-recursive members.
#[test]
fn test_recursive_keyword_on_non_self_referential_cte() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(&mut db, "WITH RECURSIVE c(v) AS (SELECT 1) SELECT v FROM c").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], int(1));
}

/// Issue #5838, item 3 (multi-member form): a RECURSIVE list where only the
/// second CTE actually recurses; the first is a plain seed CTE it consumes.
/// This mirrors the structure of the mandelbrot query (with1.test 8.1).
#[test]
fn test_recursive_list_mixes_recursive_and_plain_members() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE
           seed(lo, hi) AS (SELECT 1, 4),
           series(n) AS (
             SELECT lo FROM seed
             UNION ALL
             SELECT n + 1 FROM series, seed WHERE n < hi
           )
         SELECT n FROM series ORDER BY 1",
    )
    .unwrap();
    let values: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(values, vec![int(1), int(2), int(3), int(4)]);
}

/// Issue #5838, item 6: a top-level WITH clause resolves forward references
/// between siblings (not just nested lists). `tmp2` references `tmp1`, declared
/// after it. sqlite3 returns {1 2} (with1.test 2.5).
#[test]
fn test_top_level_forward_sibling_reference() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(x INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(2)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH tmp2(x) AS (SELECT * FROM tmp1),
              tmp1(a) AS (SELECT * FROM t1)
         SELECT * FROM tmp2 ORDER BY 1",
    )
    .unwrap();
    let values: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(values, vec![int(1), int(2)]);
}

/// Issue #5838, item 7: a recursive CTE using plain UNION (not UNION ALL)
/// deduplicates the base term as well as recursive-term rows. The seed table
/// holds a duplicate ('a', 1) row; without base-term dedup the recursion would
/// emit the 'a' chain twice (with1.test 26.2). sqlite3 emits each distinct
/// (label, step) once.
#[test]
fn test_recursive_union_dedups_base_term() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t(label VARCHAR(10), step INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO t VALUES('a', 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO t VALUES('a', 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO t VALUES('b', 1)").unwrap();

    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE cte(label, step) AS (
             SELECT * FROM t
           UNION
             SELECT label, step + 1 FROM cte WHERE step < 3
         )
         SELECT label, step FROM cte ORDER BY label, step",
    )
    .unwrap();
    let tuples: Vec<(vibesql_types::SqlValue, vibesql_types::SqlValue)> =
        rows.iter().map(|r| (r.values[0].clone(), r.values[1].clone())).collect();
    let s = |v: &str| vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(v));
    assert_eq!(
        tuples,
        vec![
            (s("a"), int(1)),
            (s("a"), int(2)),
            (s("a"), int(3)),
            (s("b"), int(1)),
            (s("b"), int(2)),
            (s("b"), int(3)),
        ]
    );
}
