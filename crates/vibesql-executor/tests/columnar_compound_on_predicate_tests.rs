//! Regression tests for issue #5702
//!
//! The columnar join fast path extracts only equi-join key pairs from the ON
//! clause and silently dropped any additional non-key conjunct (e.g.
//! `ON t1.b = t2.x AND t1.c = 1`). A left row whose key matched but whose extra
//! ON predicate was false should be NULL-padded, but the columnar path emitted
//! the matched right values instead.
//!
//! The fix makes `try_columnar_join_execution` bail out to the row-based path
//! whenever an ON clause carries a residual (non-equi-join) conjunct. These
//! tests assert correct results regardless of which execution path is chosen,
//! and cover INNER / LEFT OUTER / RIGHT OUTER joins.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Build the exact schema/data from the issue #5702 reproduction.
///
/// ```sql
/// CREATE TABLE t1(a integer primary key, b integer, c integer);
/// CREATE TABLE t2(x integer primary key, y);
/// INSERT INTO t2 VALUES(11,'t2-11');
/// INSERT INTO t2 VALUES(12,'t2-12');
/// INSERT INTO t1 VALUES(1, 5, 0);
/// INSERT INTO t1 VALUES(2, 11, 2);
/// INSERT INTO t1 VALUES(3, 12, 1);
/// ```
fn setup_issue_5702_db() -> Database {
    let mut db = Database::new();

    let t1_schema = TableSchema::with_primary_key(
        "T1".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
            ColumnSchema::new("c".to_string(), DataType::Integer, true),
        ],
        vec!["a".to_string()],
    );
    db.create_table(t1_schema).unwrap();

    let t2_schema = TableSchema::with_primary_key(
        "T2".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, false),
            ColumnSchema::new("y".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["x".to_string()],
    );
    db.create_table(t2_schema).unwrap();

    db.insert_row("T2", Row::new(vec![SqlValue::Integer(11), SqlValue::Varchar("t2-11".into())]))
        .unwrap();
    db.insert_row("T2", Row::new(vec![SqlValue::Integer(12), SqlValue::Varchar("t2-12".into())]))
        .unwrap();

    db.insert_row(
        "T1",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(0)]),
    )
    .unwrap();
    db.insert_row(
        "T1",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(11), SqlValue::Integer(2)]),
    )
    .unwrap();
    db.insert_row(
        "T1",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(12), SqlValue::Integer(1)]),
    )
    .unwrap();

    db
}

fn run(db: &Database, sql: &str) -> Vec<Row> {
    let select = parse_select(sql);
    SelectExecutor::new(db).execute(&select).unwrap()
}

/// The headline reproduction: LEFT JOIN with a compound ON clause.
///
/// Expected (SQLite / row-based path):
/// ```
/// 1 5  0 {}  {}
/// 2 11 2 {}  {}     <- c=2, so "AND t1.c=1" is false -> NULL-padded
/// 3 12 1 12  t2-12
/// ```
#[test]
fn test_left_join_compound_on_null_pads_failing_residual() {
    let db = setup_issue_5702_db();

    let sql = "SELECT * FROM t1 LEFT JOIN t2 ON t1.b = t2.x AND t1.c = 1 ORDER BY t1.a";
    let result = run(&db, sql);

    assert_eq!(result.len(), 3, "LEFT JOIN must preserve all 3 left rows");

    // Row 1: a=1, b=5, c=0 — key (b=5) has no match -> NULL-padded.
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(5));
    assert_eq!(result[0].values[2], SqlValue::Integer(0));
    assert_eq!(result[0].values[3], SqlValue::Null, "row 1 t2.x must be NULL");
    assert_eq!(result[0].values[4], SqlValue::Null, "row 1 t2.y must be NULL");

    // Row 2: a=2, b=11, c=2 — key (b=11) matches t2.x=11, but c=2 fails the
    // `AND t1.c = 1` conjunct, so the row must be NULL-padded (the bug emitted
    // 2 11 2 11 t2-11 here).
    assert_eq!(result[1].values[0], SqlValue::Integer(2));
    assert_eq!(result[1].values[1], SqlValue::Integer(11));
    assert_eq!(result[1].values[2], SqlValue::Integer(2));
    assert_eq!(
        result[1].values[3],
        SqlValue::Null,
        "row 2 must be NULL-padded: c=2 fails AND t1.c=1"
    );
    assert_eq!(result[1].values[4], SqlValue::Null, "row 2 t2.y must be NULL");

    // Row 3: a=3, b=12, c=1 — key (b=12) matches t2.x=12 and c=1 passes -> matched.
    assert_eq!(result[2].values[0], SqlValue::Integer(3));
    assert_eq!(result[2].values[1], SqlValue::Integer(12));
    assert_eq!(result[2].values[2], SqlValue::Integer(1));
    assert_eq!(result[2].values[3], SqlValue::Integer(12), "row 3 t2.x must be 12");
    assert_eq!(result[2].values[4], SqlValue::Varchar("t2-12".into()), "row 3 t2.y must be t2-12");
}

/// INNER JOIN with a compound ON clause must also honor the residual conjunct:
/// only the row where the key matches AND `t1.c = 1` holds is emitted.
#[test]
fn test_inner_join_compound_on_applies_residual() {
    let db = setup_issue_5702_db();

    let sql = "SELECT * FROM t1 INNER JOIN t2 ON t1.b = t2.x AND t1.c = 1 ORDER BY t1.a";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1, "Only a=3 satisfies key match AND c=1");
    assert_eq!(result[0].values[0], SqlValue::Integer(3));
    assert_eq!(result[0].values[3], SqlValue::Integer(12));
    assert_eq!(result[0].values[4], SqlValue::Varchar("t2-12".into()));
}

/// INNER JOIN whose extra ON conjunct is an inequality on the right table
/// (`t2.x > 0`) — the joinD.test pattern. All key matches pass `t2.x > 0`,
/// so this is mostly a guard that the residual is not dropped and results
/// stay correct.
#[test]
fn test_inner_join_compound_on_right_inequality() {
    let db = setup_issue_5702_db();

    let sql = "SELECT t1.a, t2.x FROM t1 INNER JOIN t2 ON t1.b = t2.x AND t2.x > 11 ORDER BY t1.a";
    let result = run(&db, sql);

    // b=11 matches x=11 but fails x>11; b=12 matches x=12 and passes.
    assert_eq!(result.len(), 1, "Only a=3 (x=12) passes x>11");
    assert_eq!(result[0].values[0], SqlValue::Integer(3));
    assert_eq!(result[0].values[1], SqlValue::Integer(12));
}

/// RIGHT OUTER JOIN with a compound ON clause: every right row is preserved,
/// and a left row that matches the key but fails the residual must not be
/// joined (the right row stays, left side NULL-padded).
#[test]
fn test_right_join_compound_on_applies_residual() {
    let db = setup_issue_5702_db();

    // t1 RIGHT JOIN t2: preserve all t2 rows (x=11, x=12).
    // x=11 matches t1.b=11 (a=2) on key, but t1.c=2 fails `AND t1.c=1` -> left NULL-padded.
    // x=12 matches t1.b=12 (a=3) and t1.c=1 passes -> joined.
    let sql = "SELECT t2.x, t1.a FROM t1 RIGHT JOIN t2 ON t1.b = t2.x AND t1.c = 1 ORDER BY t2.x";
    let result = run(&db, sql);

    assert_eq!(result.len(), 2, "Both t2 rows preserved");

    // x=11: left side NULL-padded because c=2 fails residual.
    assert_eq!(result[0].values[0], SqlValue::Integer(11));
    assert_eq!(result[0].values[1], SqlValue::Null, "x=11 left side must be NULL (c=2 fails)");

    // x=12: matched with t1.a=3.
    assert_eq!(result[1].values[0], SqlValue::Integer(12));
    assert_eq!(result[1].values[1], SqlValue::Integer(3));
}

/// Pure equi-join LEFT JOIN (no residual conjunct) must still produce correct
/// results — this is the case that stays on the columnar fast path.
#[test]
fn test_left_join_pure_equijoin_unaffected() {
    let db = setup_issue_5702_db();

    let sql = "SELECT t1.a, t2.x FROM t1 LEFT JOIN t2 ON t1.b = t2.x ORDER BY t1.a";
    let result = run(&db, sql);

    assert_eq!(result.len(), 3);
    // a=1 (b=5): no match.
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Null);
    // a=2 (b=11): matches x=11.
    assert_eq!(result[1].values[0], SqlValue::Integer(2));
    assert_eq!(result[1].values[1], SqlValue::Integer(11));
    // a=3 (b=12): matches x=12.
    assert_eq!(result[2].values[0], SqlValue::Integer(3));
    assert_eq!(result[2].values[1], SqlValue::Integer(12));
}
