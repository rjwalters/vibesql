//! Integration tests for `CREATE TABLE ... AS SELECT` row routing when the
//! target table's bare name is shadowed by a same-named table in another
//! schema (Part of #6174, alter4.test section 5).
//!
//! SQLite's canonical `alter4.test` 5.1 does:
//!
//! ```sql
//! CREATE TEMP TABLE t1(a, b);
//! INSERT INTO t1 VALUES(1, 'one');
//! INSERT INTO t1 VALUES(2, 'two');
//! ATTACH 'test2.db' AS aux;
//! CREATE TABLE aux.t1 AS SELECT * FROM t1;
//! ```
//!
//! and later expects `SELECT * FROM aux.t1` to return the copied rows and
//! `SELECT * FROM t1` (the temp table) to still hold exactly its original two
//! rows. Before the fix, the CTAS executor created `aux.t1` correctly but then
//! inserted the SELECT's result rows by the BARE name `t1`, which follows the
//! temp-shadows-main/attached lookup — so every row landed back in `temp.t1`
//! (doubling it) and `aux.t1` stayed empty. An explicit `main.t1` target was
//! misrouted the same way.
//!
//! (An unqualified `CREATE TABLE t1 ...` while only `temp.t1` exists is still
//! rejected with "already exists" by the general CREATE TABLE existence check
//! — a separate, pre-existing defect not covered here.)
//!
//! The multi-process TCL shim demotes `CREATE TEMP TABLE` to a persistent main
//! table, so these in-process engine tests pin the behavior directly.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec_create_table(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}")) {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap_or_else(|e| panic!("{sql:?}: {e:?}"))
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

fn exec_insert(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}")) {
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).unwrap_or_else(|e| panic!("{sql:?}: {e:?}"));
        }
        other => panic!("expected INSERT, got {other:?}"),
    }
}

/// Run a SELECT and return its rows as plain value vectors.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    match Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}")) {
        vibesql_ast::Statement::Select(s) => SelectExecutor::new(db)
            .execute_with_columns(&s)
            .unwrap_or_else(|e| panic!("{sql:?}: {e:?}"))
            .rows
            .into_iter()
            .map(|r| r.values.to_vec())
            .collect(),
        other => panic!("expected SELECT, got {other:?}"),
    }
}

fn count(db: &Database, table: &str) -> i64 {
    let rows = query(db, &format!("SELECT count(*) FROM {table}"));
    match rows[0][0] {
        SqlValue::Integer(n) => n,
        SqlValue::Bigint(n) => n,
        ref other => panic!("unexpected count value {other:?}"),
    }
}

/// A database with `temp.t1(a, b)` holding the two alter4-5.1 rows.
fn setup_temp_t1() -> Database {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TEMP TABLE t1(a, b)");
    exec_insert(&mut db, "INSERT INTO t1 VALUES(1, 'one')");
    exec_insert(&mut db, "INSERT INTO t1 VALUES(2, 'two')");
    db
}

/// alter4-5.1/5.3/5.9: `CREATE TABLE aux.t1 AS SELECT * FROM t1` must copy the
/// rows INTO `aux.t1` and leave the shadowing `temp.t1` untouched.
#[test]
fn ctas_into_attached_schema_routes_rows_to_attached_table() {
    let mut db = setup_temp_t1();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH aux");

    exec_create_table(&mut db, "CREATE TABLE aux.t1 AS SELECT * FROM t1");

    assert_eq!(count(&db, "aux.t1"), 2, "CTAS rows must land in aux.t1");
    assert_eq!(count(&db, "temp.t1"), 2, "temp.t1 must not receive the CTAS rows");
    assert_eq!(count(&db, "t1"), 2, "unqualified t1 still resolves to the unchanged temp.t1");
}

/// Explicit `main.` qualifier with a same-named temp table: rows go to main.
#[test]
fn ctas_into_main_qualified_routes_rows_to_main_table() {
    let mut db = setup_temp_t1();

    exec_create_table(&mut db, "CREATE TABLE main.t1 AS SELECT * FROM t1");

    assert_eq!(count(&db, "main.t1"), 2, "CTAS rows must land in main.t1");
    assert_eq!(count(&db, "temp.t1"), 2, "temp.t1 must not receive the CTAS rows");
}

/// Attached-schema target whose bare name does NOT resolve elsewhere (no
/// shadowing table anywhere): rows still land in the attached table.
#[test]
fn ctas_into_attached_schema_without_shadow_populates_attached_table() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH aux");
    exec_create_table(&mut db, "CREATE TABLE src(a, b)");
    exec_insert(&mut db, "INSERT INTO src VALUES(1, 'one')");

    exec_create_table(&mut db, "CREATE TABLE aux.t9 AS SELECT * FROM src");

    assert_eq!(count(&db, "aux.t9"), 1);
    assert_eq!(count(&db, "src"), 1);
}

/// Regression guard for the common case: no shadowing, plain CTAS still works.
#[test]
fn ctas_without_shadowing_still_populates_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE src(a, b)");
    exec_insert(&mut db, "INSERT INTO src VALUES(1, 'one')");
    exec_create_table(&mut db, "CREATE TABLE dst AS SELECT * FROM src");
    assert_eq!(count(&db, "dst"), 1);
    assert_eq!(query(&db, "SELECT a FROM dst"), vec![vec![SqlValue::Integer(1)]]);
}
