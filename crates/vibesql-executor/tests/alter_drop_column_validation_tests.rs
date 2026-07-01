//! Regression tests for issue #5784: `ALTER TABLE ... DROP COLUMN` rejects
//! drops that SQLite rejects, using SQLite's verbatim error strings (verified
//! against sqlite3 3.51.0 / `alterdropcol.test`).
//!
//! Covered rejections:
//!   * a UNIQUE column                  -> `cannot drop UNIQUE column: "z"`
//!   * a PRIMARY KEY column             -> `cannot drop PRIMARY KEY column: "x"`
//!   * a plain-index-referenced column  -> `error in index t2y after drop column: no such column: y`
//!   * an expression-index column       -> `error in index t3rs after drop column: no such column: s`
//!   * a column of a VIEW               -> `cannot drop column from view "v1"`
//!   * the schema table                 -> `table sqlite_master may not be altered`
//!   * a missing column                 -> `no such column: "d"`
//!   * the only remaining column        -> `cannot drop column "a": no other columns exist`
//!
//! And a positive control: dropping an unconstrained column still succeeds and
//! rewrites the verbatim `sqlite_master.sql` text in place.

use vibesql_executor::{
    AlterTableExecutor, CreateIndexExecutor, CreateTableExecutor, SelectExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    let vibesql_ast::Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

fn create_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE INDEX");
    let vibesql_ast::Statement::CreateIndex(idx) = stmt else {
        panic!("expected CREATE INDEX");
    };
    CreateIndexExecutor::execute(&idx, db).expect("CREATE INDEX");
}

fn create_view(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE VIEW");
    let vibesql_ast::Statement::CreateView(view) = stmt else {
        panic!("expected CREATE VIEW");
    };
    ViewExecutor::execute_create_view(&view, db).expect("CREATE VIEW");
}

/// Run an ALTER and return its `Result` (so tests can assert on the error text).
fn try_alter(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).expect("parse ALTER");
    let vibesql_ast::Statement::AlterTable(a) = stmt else {
        panic!("expected ALTER TABLE");
    };
    AlterTableExecutor::execute_with_source(&a, db, Some(sql)).map_err(|e| e.to_string())
}

fn drop_column_err(db: &mut Database, sql: &str) -> String {
    try_alter(db, sql).expect_err(&format!("expected DROP COLUMN to fail: {sql}"))
}

fn table_sql(db: &Database, table: &str) -> String {
    let query = format!("SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'");
    let stmt = Parser::parse_sql(&query).expect("parse SELECT");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    assert_eq!(result.rows.len(), 1, "expected one row for table {table}");
    match &result.rows[0].values[0] {
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            s.to_string()
        }
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn drop_unique_column_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y, z UNIQUE)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t2 DROP COLUMN z"),
        "cannot drop UNIQUE column: \"z\""
    );
}

#[test]
fn drop_primary_key_column_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y, z UNIQUE)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t2 DROP COLUMN x"),
        "cannot drop PRIMARY KEY column: \"x\""
    );
}

#[test]
fn drop_column_referenced_by_plain_index_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y, z UNIQUE)");
    create_index(&mut db, "CREATE INDEX t2y ON t2(y)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t2 DROP COLUMN y"),
        "error in index t2y after drop column: no such column: y"
    );
}

#[test]
fn drop_column_referenced_by_expression_index_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t3(q, r, s)");
    create_index(&mut db, "CREATE INDEX t3rs ON t3(r+s)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t3 DROP COLUMN s"),
        "error in index t3rs after drop column: no such column: s"
    );
}

#[test]
fn drop_column_from_view_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE v1 DROP COLUMN c"),
        "cannot drop column from view \"v1\""
    );
}

#[test]
fn drop_column_from_schema_table_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE sqlite_schema DROP COLUMN sql"),
        "table sqlite_master may not be altered"
    );
}

#[test]
fn drop_missing_column_reports_quoted_name() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    assert_eq!(drop_column_err(&mut db, "ALTER TABLE t1 DROP COLUMN d"), "no such column: \"d\"");
}

#[test]
fn drop_only_remaining_column_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t1 DROP COLUMN a"),
        "cannot drop column \"a\": no other columns exist"
    );
}

#[test]
fn drop_unconstrained_column_succeeds_and_rewrites_schema_text() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    // Dropping an unconstrained middle column succeeds and edits the verbatim
    // CREATE TABLE text in place, matching sqlite3 (whitespace preserved).
    try_alter(&mut db, "ALTER TABLE t1 DROP COLUMN b").expect("DROP COLUMN b");
    assert_eq!(table_sql(&db, "t1"), "CREATE TABLE t1(a, c)");
}
