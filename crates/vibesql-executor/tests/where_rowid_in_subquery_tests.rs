//! Regression tests for issue #6088.
//!
//! A WHERE-clause `rowid IN (SELECT ...)` filter returned an empty result set
//! even though the equivalent SELECT-list projection of the same predicate
//! (and the scalar `rowid = (SELECT ...)` form) worked:
//!
//! ```sql
//! CREATE TABLE e1(a TEXT, c NUMERIC); INSERT INTO e1 VALUES(2, 2);
//! CREATE TABLE e2(x BLOB, y BLOB); INSERT INTO e2 VALUES('2',2),('2','2'),('2','2.0');
//! SELECT rowid FROM e2 WHERE rowid IN (SELECT +c FROM e1);  -- rowvalue9 5.2 → 2
//! ```
//!
//! Root cause: the IN→SEMI/ANTI-join rewrite (`subquery_to_join`) folded the
//! outer LHS `rowid` verbatim into the synthesized join ON condition
//! (`e2 SEMI JOIN e1 ON rowid = e1.c`). In the join's combined schema
//! (`e2` + `e1`) a *bare* `rowid` no longer resolves to `e2`'s rowid — it
//! evaluated to NULL, so the predicate never matched and the filter dropped
//! every row. Qualifying the bare rowid to its single outer table
//! (`e2.rowid = e1.c`) — or aborting the transform and falling back to
//! row-by-row evaluation when that is not safely possible — fixes it.
//!
//! Expected values verified against sqlite3 3.51.0.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::CreateIndex(create_index) => {
            vibesql_executor::CreateIndexExecutor::execute(&create_index, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

/// Run a SELECT and return the integer value of result column 0 for each row.
fn query_col0_ints(db: &vibesql_storage::Database, sql: &str) -> Vec<i64> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement: {}", sql);
    };
    SelectExecutor::new(db)
        .execute(&select_stmt)
        .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e))
        .into_iter()
        .map(|row| match row.get(0) {
            Some(SqlValue::Integer(n)) | Some(SqlValue::Bigint(n)) => *n,
            other => panic!("expected integer at index 0, got {other:?}"),
        })
        .collect()
}

/// Build the rowvalue9 §5 fixture: `e1(a TEXT, c NUMERIC)` with one row and
/// `e2(x BLOB, y BLOB)` with three rows (rowids 1,2,3), plus the `e1(c)` index.
fn setup_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE e1(a TEXT, c NUMERIC)");
    run_stmt(&mut db, "CREATE TABLE e2(x BLOB, y BLOB)");
    run_stmt(&mut db, "INSERT INTO e1 VALUES(2, 2)");
    run_stmt(&mut db, "INSERT INTO e2 VALUES ('2', 2)");
    run_stmt(&mut db, "INSERT INTO e2 VALUES ('2', '2')");
    run_stmt(&mut db, "INSERT INTO e2 VALUES ('2', '2.0')");
    run_stmt(&mut db, "CREATE INDEX e1c ON e1(c)");
    db
}

/// rowvalue9 5.2: `rowid IN (SELECT +c FROM e1)` → 2.
#[test]
fn rowid_in_subquery_unary_plus() {
    let db = setup_db();
    let sql = "SELECT rowid FROM e2 WHERE rowid IN (SELECT +c FROM e1)";
    assert_eq!(query_col0_ints(&db, sql), vec![2]);
}

/// rowvalue9 5.3: `rowid IN (SELECT 0+c FROM e1)` → 2.
#[test]
fn rowid_in_subquery_zero_plus() {
    let db = setup_db();
    let sql = "SELECT rowid FROM e2 WHERE rowid IN (SELECT 0+c FROM e1)";
    assert_eq!(query_col0_ints(&db, sql), vec![2]);
}

/// The plain column form `rowid IN (SELECT c FROM e1)` also matched the scalar
/// `=` path but not the WHERE-path IN filter before the fix.
#[test]
fn rowid_in_subquery_plain_column() {
    let db = setup_db();
    let sql = "SELECT rowid FROM e2 WHERE rowid IN (SELECT c FROM e1)";
    assert_eq!(query_col0_ints(&db, sql), vec![2]);
}

/// The `oid` and `_rowid_` spellings of the pseudo-column behave identically.
#[test]
fn rowid_aliases_in_subquery() {
    let db = setup_db();
    assert_eq!(
        query_col0_ints(&db, "SELECT rowid FROM e2 WHERE oid IN (SELECT c FROM e1)"),
        vec![2]
    );
    assert_eq!(
        query_col0_ints(&db, "SELECT rowid FROM e2 WHERE _rowid_ IN (SELECT c FROM e1)"),
        vec![2]
    );
}

/// `NOT IN` (the ANTI-join path) keeps the complementary rows: {1, 3}.
#[test]
fn rowid_not_in_subquery_anti_join() {
    let db = setup_db();
    let sql = "SELECT rowid FROM e2 WHERE rowid NOT IN (SELECT c FROM e1)";
    assert_eq!(query_col0_ints(&db, sql), vec![1, 3]);
}

/// An INTEGER PRIMARY KEY table exposes its rowid via the alias column; the
/// bare-rowid qualification must resolve there too.
#[test]
fn rowid_in_subquery_integer_primary_key_table() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE e1(a TEXT, c NUMERIC)");
    run_stmt(&mut db, "INSERT INTO e1 VALUES(2, 2)");
    run_stmt(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT)");
    run_stmt(&mut db, "INSERT INTO p VALUES(2, 'x')");
    run_stmt(&mut db, "INSERT INTO p VALUES(5, 'y')");
    let sql = "SELECT id FROM p WHERE rowid IN (SELECT c FROM e1)";
    assert_eq!(query_col0_ints(&db, sql), vec![2]);
}

/// A qualified outer rowid across multiple outer tables never hit the bug and
/// must keep working (it does not use the bare-rowid qualification path).
#[test]
fn qualified_rowid_multi_table_outer() {
    let db = setup_db();
    let sql = "SELECT e2.rowid FROM e2, e1 WHERE e2.rowid IN (SELECT c FROM e1)";
    assert_eq!(query_col0_ints(&db, sql), vec![2]);
}
