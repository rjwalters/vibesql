//! Regression tests for issue #5927
//!
//! An ambiguous unqualified column reference used in a **non-equijoin WHERE
//! predicate** (e.g. `WHERE id > 0`) on the columnar explicit-JOIN path was
//! silently resolved to the leftmost table instead of erroring.
//!
//! Root cause: the columnar join path
//! (`columnar_execution::join::try_columnar_join_execution`) applied the
//! residual WHERE via SIMD without running `validate_select_columns_with_context`
//! against the combined schema. The single-table columnar path and the
//! row-oriented path both run that validation; the columnar join path did not.
//! Because the ambiguous `id` in `id > 0` is not an equijoin key, it was never
//! extracted as a join condition and never hit the ambiguity guard added in
//! #5870 — the SIMD WHERE filter then resolved it leftmost.
//!
//! The row-oriented path (`VIBESQL_DISABLE_COLUMNAR_JOIN=1`) was always correct;
//! these tests pin the fixed behavior on the columnar (default) path.

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

fn run_err(db: &Database, sql: &str) -> String {
    let select = parse_select(sql);
    match SelectExecutor::new(db).execute(&select) {
        Ok(rows) => {
            panic!("expected an error for `{}`, but query returned {} row(s)", sql, rows.len())
        }
        Err(e) => e.to_string(),
    }
}

/// Schema/data from the issue #5927 reproduction, extended with a third table
/// so the 3-table join case can be exercised.
///
/// ```sql
/// CREATE TABLE a1(id INTEGER PRIMARY KEY, v);
/// CREATE TABLE b1(id INTEGER PRIMARY KEY, aid, v);
/// CREATE TABLE c1(id INTEGER PRIMARY KEY, bid, v);
/// INSERT INTO a1 VALUES(1,'a1'),(2,'a2');
/// INSERT INTO b1 VALUES(10,1,'b1'),(11,2,'b2');
/// INSERT INTO c1 VALUES(100,10,'c1');
/// ```
///
/// Every table has an `id` column, so an unqualified `id` in a WHERE predicate
/// is ambiguous across a1/b1(/c1).
fn setup_issue_5927_db() -> Database {
    let mut db = Database::new();

    let a1_schema = TableSchema::with_primary_key(
        "A1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(a1_schema).unwrap();

    let b1_schema = TableSchema::with_primary_key(
        "B1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("aid".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(b1_schema).unwrap();

    let c1_schema = TableSchema::with_primary_key(
        "C1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("bid".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(c1_schema).unwrap();

    for (id, v) in [(1, "a1"), (2, "a2")] {
        db.insert_row("A1", Row::new(vec![SqlValue::Integer(id), SqlValue::Varchar(v.into())]))
            .unwrap();
    }
    for (id, aid, v) in [(10, 1, "b1"), (11, 2, "b2")] {
        db.insert_row(
            "B1",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Integer(aid),
                SqlValue::Varchar(v.into()),
            ]),
        )
        .unwrap();
    }
    db.insert_row(
        "C1",
        Row::new(vec![
            SqlValue::Integer(100),
            SqlValue::Integer(10),
            SqlValue::Varchar("c1".into()),
        ]),
    )
    .unwrap();

    db
}

/// Primary reproduction: ambiguous unqualified `id` in a non-equijoin WHERE
/// predicate must error on the columnar (default) path, matching sqlite3 and
/// the row path.
#[test]
fn test_ambiguous_unqualified_where_errors() {
    let db = setup_issue_5927_db();

    let sql = "SELECT a1.id FROM a1 JOIN b1 ON b1.aid=a1.id WHERE id>0";
    let err = run_err(&db, sql);

    assert!(
        err.contains("ambiguous column name: id"),
        "expected `ambiguous column name: id`, got: {}",
        err
    );
}

/// Qualified reference in the WHERE predicate is unambiguous and must still
/// work correctly.
#[test]
fn test_qualified_where_still_works() {
    let db = setup_issue_5927_db();

    let sql = "SELECT a1.id FROM a1 JOIN b1 ON b1.aid=a1.id WHERE a1.id>0 ORDER BY 1";
    let result = run(&db, sql);

    assert_eq!(result.len(), 2, "both a1 rows join and satisfy a1.id > 0");
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[1].values[0], SqlValue::Integer(2));
}

/// Unqualified reference to a column that exists in only one table is
/// unambiguous and must still work correctly.
#[test]
fn test_unambiguous_unqualified_where_still_works() {
    let db = setup_issue_5927_db();

    // `aid` exists only in b1, so it is unambiguous even unqualified.
    let sql = "SELECT a1.id FROM a1 JOIN b1 ON b1.aid=a1.id WHERE aid>0 ORDER BY 1";
    let result = run(&db, sql);

    assert_eq!(result.len(), 2, "both rows join and satisfy aid > 0");
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[1].values[0], SqlValue::Integer(2));
}

/// 3-table join: the ambiguous unqualified `id` in the WHERE predicate must
/// error on the columnar path.
#[test]
fn test_ambiguous_unqualified_where_three_table_join_errors() {
    let db = setup_issue_5927_db();

    let sql = "SELECT a1.id FROM a1 JOIN b1 ON b1.aid=a1.id \
               JOIN c1 ON c1.bid=b1.id WHERE id>0";
    let err = run_err(&db, sql);

    assert!(
        err.contains("ambiguous column name: id"),
        "expected `ambiguous column name: id`, got: {}",
        err
    );
}

/// A non-equijoin WHERE predicate after a LEFT JOIN must also error when the
/// unqualified reference is ambiguous.
#[test]
fn test_ambiguous_unqualified_where_after_left_join_errors() {
    let db = setup_issue_5927_db();

    let sql = "SELECT a1.id FROM a1 LEFT JOIN b1 ON b1.aid=a1.id WHERE id>0";
    let err = run_err(&db, sql);

    assert!(
        err.contains("ambiguous column name: id"),
        "expected `ambiguous column name: id`, got: {}",
        err
    );
}
