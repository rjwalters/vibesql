//! Regression tests for issue #5880
//!
//! SQL name resolution is innermost-scope-first: an unqualified column in a
//! subquery's WHERE clause that exists on one of the subquery's own FROM tables
//! binds to that *inner* table, NOT to a same-named column in the outer query.
//!
//! VibeSQL's correlation detection used to consult only the *outer* schema to
//! decide whether an unqualified name belonged to an inner table. That check
//! only ever succeeded for the self-join case (inner table name == outer table
//! name); when the inner and outer tables were *distinct* tables that happened
//! to share a column name, the inner table was never in the outer schema, so
//! the column was misclassified as an outer correlation reference. The subquery
//! was then run in the correlated branch, coupling its result to the outer
//! row's value instead of the inner column's — producing wrong results.
//!
//! Canonical reproducer (returns 4 rows in sqlite3, used to return 0 here):
//!
//! ```sql
//! CREATE TABLE t(id, x, y); CREATE TABLE u(x, y);
//! SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE u.y = y);
//! ```
//!
//! `u.y = y` must resolve as `u.y = u.y` (an always-true tautology), NOT as
//! `u.y = t.y`. The fix threads the `Database` into the correlation walk so the
//! inner table's real column list can be consulted.
//!
//! All expected row sets below were verified against sqlite3 3.x.

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

fn run(db: &Database, sql: &str) -> Vec<Row> {
    let select = parse_select(sql);
    SelectExecutor::new(db).execute(&select).unwrap()
}

/// Collect the integer values of the first projected column, sorted ascending
/// (row order is not significant for these assertions).
fn ids(rows: &[Row]) -> Vec<i64> {
    let mut out: Vec<i64> = rows
        .iter()
        .map(|r| match r.get(0) {
            Some(SqlValue::Integer(n)) => *n,
            Some(SqlValue::Bigint(n)) => *n,
            other => panic!("expected integer at index 0, got {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

/// Build the canonical fixture:
///
/// ```sql
/// CREATE TABLE t(id, x, y); CREATE TABLE u(x, y);
/// INSERT INTO t VALUES(1,2,5),(2,7,5),(3,4,10),(4,9,10);
/// INSERT INTO u VALUES(1,5),(3,5),(2,10),(6,10);
/// ```
fn setup_db() -> Database {
    let mut db = Database::new();

    let t_schema = TableSchema::new(
        "t".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t_schema).unwrap();

    let u_schema = TableSchema::new(
        "u".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(u_schema).unwrap();

    let mut ins_t = |id: i64, x: i64, y: i64| {
        db.insert_row(
            "t",
            Row::new(vec![SqlValue::Integer(id), SqlValue::Integer(x), SqlValue::Integer(y)]),
        )
        .unwrap();
    };
    ins_t(1, 2, 5);
    ins_t(2, 7, 5);
    ins_t(3, 4, 10);
    ins_t(4, 9, 10);

    let mut ins_u = |x: i64, y: i64| {
        db.insert_row("u", Row::new(vec![SqlValue::Integer(x), SqlValue::Integer(y)])).unwrap();
    };
    ins_u(1, 5);
    ins_u(3, 5);
    ins_u(2, 10);
    ins_u(6, 10);

    db
}

/// The canonical reproducer. `u.y = y` resolves inner-first as `u.y = u.y`, a
/// tautology, so `MIN(x)` over all of `u` is 1 and every `t` row (x in
/// {2,7,4,9}) qualifies. Expect all four ids.
#[test]
fn unqualified_inner_column_resolves_inner_first() {
    let db = setup_db();
    let rows = run(&db, "SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE u.y = y)");
    assert_eq!(ids(&rows), vec![1, 2, 3, 4]);
}

/// The fully-qualified tautology form must return exactly the same rows as the
/// unqualified form above (acceptance criterion).
#[test]
fn qualified_tautology_matches_unqualified_form() {
    let db = setup_db();
    let unqualified =
        ids(&run(&db, "SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE u.y = y)"));
    let qualified =
        ids(&run(&db, "SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE u.y = u.y)"));
    assert_eq!(unqualified, qualified);
    assert_eq!(qualified, vec![1, 2, 3, 4]);
}

/// A genuinely-correlated unqualified reference: `id` exists only in the outer
/// table `t` (not in inner `u`), so it must still be detected as correlated and
/// evaluated per outer row. For each row the inner `MIN(x)` is taken over
/// `u.x = t.id`; only ids 1..3 have a matching `u.x` and pass `x > MIN`.
#[test]
fn genuine_outer_only_correlation_preserved() {
    let db = setup_db();
    let rows = run(&db, "SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE u.x = id)");
    assert_eq!(ids(&rows), vec![1, 2, 3]);
}

/// A qualified outer reference `u.y = t.y` is genuine cross-table equality and
/// must be unaffected by the fix.
#[test]
fn qualified_outer_reference_unchanged() {
    let db = setup_db();
    let rows = run(&db, "SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE u.y = t.y)");
    assert_eq!(ids(&rows), vec![1, 2, 3, 4]);
}

/// Nested three levels deep, with the same-name unqualified `y` at each level.
/// Every occurrence must resolve to its own innermost `u`, so the whole
/// predicate reduces to a tautology and all four ids qualify.
#[test]
fn three_level_nesting_resolves_each_scope_inner_first() {
    let db = setup_db();
    let rows = run(
        &db,
        "SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE u.y = y \
         AND x >= (SELECT MIN(x) FROM u WHERE u.y = y))",
    );
    assert_eq!(ids(&rows), vec![1, 2, 3, 4]);
}

/// Control: an unqualified column that exists *only* in the inner table (`x`
/// with a literal comparison) is trivially local. Guards against the fix
/// accidentally treating clearly-inner columns as correlated.
#[test]
fn unqualified_inner_only_literal_predicate() {
    let db = setup_db();
    // Inner subquery selects MIN over u rows where u.x < 3 → {1, 2} → MIN = 1.
    let rows = run(&db, "SELECT id FROM t WHERE x > (SELECT MIN(x) FROM u WHERE x < 3)");
    assert_eq!(ids(&rows), vec![1, 2, 3, 4]);
}

/// Self-join control (issue #4761 path): the inner and outer tables are the
/// same table `t`, so an unqualified column still resolves to the inner `t`.
/// This is the case the old outer-schema heuristic already handled; verify it
/// is not broken by the new database-backed check.
#[test]
fn self_join_unqualified_still_resolves_inner() {
    let db = setup_db();
    // `SELECT MIN(x) FROM t WHERE y = y` is a tautology → MIN(x) over t = 2.
    // Outer rows with x > 2 are ids 2 (7), 3 (4), 4 (9).
    let rows = run(&db, "SELECT id FROM t a WHERE a.x > (SELECT MIN(x) FROM t WHERE y = y)");
    assert_eq!(ids(&rows), vec![2, 3, 4]);
}
