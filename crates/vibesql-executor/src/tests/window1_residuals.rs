//! Regression tests for the window1.test residual fixes (issue #5264)
//!
//! Covers:
//! - 61.1-A: `CAST(x AS )` (empty type name) gets NUMERIC affinity
//! - 61.1-B: bitwise operators coerce text operands SQLite-style
//! - 61.1-B: window sum/total/avg coerce text arguments instead of skipping
//! - 61.1-B: nested correlated references bind to the nearest enclosing scope (merged outer
//!   schema/row alignment)
//! - 71.0: outer-correlated aggregate misuse detected in UNION arms of a scalar subquery

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

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
        Statement::Select(select_stmt) => Ok(SelectExecutor::new(db).execute(&select_stmt)?),
        other => {
            Err(ExecutorError::UnsupportedFeature(format!("Unsupported statement: {:?}", other)))
        }
    }
}

fn single_value(db: &mut vibesql_storage::Database, sql: &str) -> SqlValue {
    let rows = execute_sql(db, sql).unwrap();
    assert_eq!(rows.len(), 1, "expected one row from {sql}");
    rows[0].values[0].clone()
}

// ------------------------------------------------------------------------
// 61.1-A — empty CAST type name gets NUMERIC affinity
// ------------------------------------------------------------------------

#[test]
fn test_cast_empty_typename_numeric_affinity_text() {
    let mut db = vibesql_storage::Database::new();
    // SQLite 3.51: CAST('seventeen' AS ) → 0 (typeof integer)
    assert_eq!(single_value(&mut db, "SELECT CAST('seventeen' AS )"), SqlValue::Integer(0));
}

#[test]
fn test_cast_empty_typename_numeric_affinity_real_text() {
    let mut db = vibesql_storage::Database::new();
    // SQLite 3.51: CAST('5.5' AS ) → 5.5 (typeof real)
    assert_eq!(single_value(&mut db, "SELECT CAST('5.5' AS )"), SqlValue::Numeric(5.5));
}

#[test]
fn test_cast_empty_typename_sum_over_mixed_column() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(5),(NULL),('seventeen')").unwrap();
    // window1.test 61.1 L0 layer: 'seventeen' → 0, NULL skipped → 5
    assert_eq!(single_value(&mut db, "SELECT sum(CAST(a AS )) FROM t1"), SqlValue::Integer(5));
}

// ------------------------------------------------------------------------
// 61.1-B — bitwise operators coerce text SQLite-style
// ------------------------------------------------------------------------

#[test]
fn test_bitwise_and_coerces_text() {
    let mut db = vibesql_storage::Database::new();
    // SQLite: SELECT 'seventeen' & 5 → 0
    assert_eq!(single_value(&mut db, "SELECT 'seventeen' & 5"), SqlValue::Integer(0));
    // Numeric text prefix coerces: '5' | 2 → 7
    assert_eq!(single_value(&mut db, "SELECT '5' | 2"), SqlValue::Integer(7));
}

// ------------------------------------------------------------------------
// 61.1-B — window sum/total/avg coerce text arguments
// ------------------------------------------------------------------------

#[test]
fn test_window_sum_text_coercion() {
    let mut db = vibesql_storage::Database::new();
    // SQLite: sum('seventeen') OVER() → 0.0 (real)
    assert_eq!(single_value(&mut db, "SELECT sum('seventeen') OVER()"), SqlValue::Numeric(0.0));
    // SQLite: sum('5') OVER() → 5 (integer — fully-integer text stays integer)
    assert_eq!(single_value(&mut db, "SELECT sum('5') OVER()"), SqlValue::Integer(5));
    // SQLite: sum('5.0') OVER() → 5.0 (real)
    assert_eq!(single_value(&mut db, "SELECT sum('5.0') OVER()"), SqlValue::Numeric(5.0));
}

#[test]
fn test_window_total_and_avg_text_coercion() {
    let mut db = vibesql_storage::Database::new();
    assert_eq!(single_value(&mut db, "SELECT total('seventeen') OVER()"), SqlValue::Numeric(0.0));
    assert_eq!(single_value(&mut db, "SELECT avg('seventeen') OVER()"), SqlValue::Numeric(0.0));
}

// ------------------------------------------------------------------------
// 61.1-B — nearest-enclosing-scope binding for nested correlated refs
// ------------------------------------------------------------------------

#[test]
fn test_nested_correlated_ref_binds_to_nearest_scope() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(5),(NULL),(17)").unwrap();

    // The inner `sum(a) OVER()` must bind `a` to the *middle* t1 scope (the
    // scalar subquery's own FROM), not the outermost t1 row. SQLite: 2 2 2.
    let rows = execute_sql(
        &mut db,
        "SELECT (SELECT sum(a IN(SELECT sum(a) OVER() FROM (SELECT 1 AS x))) FROM t1) FROM t1",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(
            row.values[0],
            SqlValue::Integer(2),
            "inner correlated `a` bound to the wrong scope"
        );
    }
}

#[test]
fn test_in_subquery_null_three_valued_logic_guard() {
    let mut db = vibesql_storage::Database::new();
    // Regression guard: 5 IN (SELECT NULL) → NULL
    assert_eq!(single_value(&mut db, "SELECT 5 IN (SELECT NULL)"), SqlValue::Null);
}

// ------------------------------------------------------------------------
// 71.0 — misuse of aggregate detected in UNION arms
// ------------------------------------------------------------------------

#[test]
fn test_misuse_of_aggregate_in_union_arm() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t0(a)").unwrap();

    // Misuse lives in the *second* UNION arm; must error even with an empty table.
    let err =
        execute_sql(&mut db, "SELECT a FROM t0 WHERE (a,1)=(SELECT 2,2 UNION SELECT sum(a),1)")
            .unwrap_err();
    assert!(
        err.to_string().contains("misuse of aggregate"),
        "expected misuse-of-aggregate error, got: {err}"
    );
}

#[test]
fn test_misuse_of_aggregate_in_third_compound_arm() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t0(a)").unwrap();

    let err = execute_sql(
        &mut db,
        "SELECT a FROM t0 WHERE (a,1)=(SELECT 2,2 UNION SELECT 3,3 UNION SELECT sum(a),1)",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("misuse of aggregate"),
        "expected misuse-of-aggregate error, got: {err}"
    );
}

#[test]
fn test_window1_71_0_full_query_errors() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t0(a)").unwrap();

    // window1.test 71.0: expects *any* prepare-time error.
    let result = execute_sql(
        &mut db,
        "SELECT a FROM t0, (SELECT a AS b FROM t0) \
         WHERE (a,1)=(SELECT 2,2 UNION SELECT sum(b),max(b) OVER(ORDER BY b) ORDER BY 2) \
           AND b=4 \
         ORDER BY b",
    );
    assert!(result.is_err(), "window1.test 71.0 must produce an error, got {result:?}");
}

#[test]
fn test_from_bearing_union_arm_with_aggregate_not_flagged() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t0(a)").unwrap();
    execute_sql(&mut db, "INSERT INTO t0 VALUES(2)").unwrap();

    // The aggregate in the second arm has its own FROM — NOT a misuse.
    let rows =
        execute_sql(&mut db, "SELECT a FROM t0 WHERE a=(SELECT 2 UNION SELECT sum(a) FROM t0)");
    assert!(rows.is_ok(), "FROM-bearing UNION arm must not be flagged: {rows:?}");
}

#[test]
fn test_non_correlated_aggregate_in_union_arm_not_flagged() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t0(a)").unwrap();

    // sum(5) doesn't reference an outer column — stays accepted (matches SQLite).
    let rows =
        execute_sql(&mut db, "SELECT a FROM t0 WHERE (a,1)=(SELECT 2,2 UNION SELECT sum(5),1)");
    assert!(rows.is_ok(), "non-correlated aggregate in UNION arm must not be flagged: {rows:?}");
}

// ------------------------------------------------------------------------
// 52.2-52.4 — multi-window output order: a partitioned window's sort order
// takes precedence over a trailing ORDER-BY-only window when there is no
// statement-level ORDER BY (window1.test 52.2-52.4).
//
// Previously the final output order was captured from the textually-last
// window that had any ORDER BY (here `count(a) OVER (ORDER BY b)`), so rows
// came out in `b` order. SQLite leaves the rows in the partitioned window's
// (`win2 = PARTITION BY 6 ORDER BY a`) `a` order instead.
// ------------------------------------------------------------------------

fn first_column_ints(db: &mut vibesql_storage::Database, sql: &str) -> Vec<i64> {
    execute_sql(db, sql)
        .unwrap()
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("expected integer first column, got {other:?}"),
        })
        .collect()
}

fn setup_w52(db: &mut vibesql_storage::Database) {
    execute_sql(db, "CREATE TABLE t1(a, b, c)").unwrap();
    execute_sql(db, "INSERT INTO t1 VALUES('AA','bb',356)").unwrap();
    execute_sql(db, "INSERT INTO t1 VALUES('CC','aa',158)").unwrap();
    execute_sql(db, "INSERT INTO t1 VALUES('BB','aa',399)").unwrap();
    execute_sql(db, "INSERT INTO t1 VALUES('FF','bb',938)").unwrap();
}

#[test]
fn test_multi_window_partitioned_order_wins_over_trailing_orderby() {
    let mut db = vibesql_storage::Database::new();
    setup_w52(&mut db);
    // window1.test 52.2: `count() OVER win1` is a running count over ORDER BY a,
    // so a-sorted output must read 1,2,3,4 (AA,BB,CC,FF) — NOT b-sorted order.
    let counts = first_column_ints(
        &mut db,
        "SELECT count() OVER win1, sum(c) OVER win2, first_value(c) OVER win2, \
                count(a) OVER (ORDER BY b) \
         FROM t1 \
         WINDOW win1 AS (ORDER BY a), \
                win2 AS (PARTITION BY 6 ORDER BY a \
                         RANGE BETWEEN 5 PRECEDING AND 0 PRECEDING)",
    );
    assert_eq!(counts, vec![1, 2, 3, 4], "output must be in win2's `a` order");
}

#[test]
fn test_multi_window_orderby_only_falls_back_to_last() {
    // Regression guard for window9.test 1.4: when NO window is partitioned, the
    // last ORDER-BY-only window still determines output order. Here both windows
    // share ORDER BY a, so the running count reads 1,2,3,4.
    let mut db = vibesql_storage::Database::new();
    setup_w52(&mut db);
    let counts = first_column_ints(
        &mut db,
        "SELECT count() OVER (ORDER BY a), count(a) OVER (ORDER BY a) FROM t1",
    );
    assert_eq!(counts, vec![1, 2, 3, 4]);
}
