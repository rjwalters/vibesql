//! End-to-end regression tests for issue #5861: a generated column added via
//! `ALTER TABLE ... ADD COLUMN ... GENERATED ALWAYS AS (expr)` must compute its
//! value, not read back as NULL.
//!
//! Before the fix two gaps combined:
//!   * the ALTER parser dropped the `GENERATED ALWAYS AS (expr)` clause
//!     (`generated_expr` stayed `None`), and
//!   * the ADD COLUMN executor stored a plain column and backfilled existing
//!     rows with a static NULL.
//!
//! Parity target: sqlite3 3.51.0 (the TCL conformance reference), which returns
//! the computed value for both the new-row path and the pre-existing-row
//! backfill path, and — unlike older releases — accepts both STORED and VIRTUAL
//! generated columns via ALTER TABLE ADD COLUMN. VibeSQL materializes generated
//! columns at write time, so STORED and VIRTUAL are handled identically.

use vibesql_ast::Statement;
use vibesql_executor::{
    AlterTableExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec_ddl_dml(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute_with_source(&s, db, Some(sql)).expect("CREATE TABLE");
        }
        Statement::AlterTable(s) => {
            AlterTableExecutor::execute(&s, db).expect("ALTER TABLE");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| ()).expect("INSERT");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

/// Run a single-column SELECT and return the flattened column values.
fn query_col(db: &Database, sql: &str) -> Vec<SqlValue> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result
        .rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Bigint(n) => SqlValue::Integer(*n),
            SqlValue::Smallint(n) => SqlValue::Integer(*n as i64),
            other => other.clone(),
        })
        .collect()
}

fn int(v: i64) -> SqlValue {
    SqlValue::Integer(v)
}

/// The headline reproducer from the issue: ADD a typed generated column, then
/// INSERT — the new row must compute the expression.
#[test]
fn add_generated_column_typed_computes_on_insert() {
    let mut db = Database::new();
    exec_ddl_dml(&mut db, "CREATE TABLE g(x INTEGER)");
    exec_ddl_dml(&mut db, "ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1)");
    exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(4)");
    assert_eq!(query_col(&db, "SELECT y FROM g"), vec![int(5)], "expected sqlite3 result 5");
}

/// Typeless short form `ADD COLUMN y AS (x+1)`.
#[test]
fn add_generated_column_typeless_short_form_computes_on_insert() {
    let mut db = Database::new();
    exec_ddl_dml(&mut db, "CREATE TABLE g(x INTEGER)");
    exec_ddl_dml(&mut db, "ALTER TABLE g ADD COLUMN y AS (x+1)");
    exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(4)");
    assert_eq!(query_col(&db, "SELECT y FROM g"), vec![int(5)]);
}

/// Pre-existing rows must be backfilled with the computed value, not NULL.
#[test]
fn add_generated_column_backfills_existing_rows() {
    let mut db = Database::new();
    exec_ddl_dml(&mut db, "CREATE TABLE g(x INTEGER)");
    exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(10)");
    exec_ddl_dml(&mut db, "ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1)");
    // Pre-existing row is backfilled...
    assert_eq!(query_col(&db, "SELECT y FROM g"), vec![int(11)], "expected sqlite3 result 11");
    // ...and a subsequent insert still computes.
    exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(20)");
    assert_eq!(query_col(&db, "SELECT y FROM g ORDER BY x"), vec![int(11), int(21)]);
}

/// sqlite3 3.51.0 accepts both STORED and VIRTUAL via ALTER and computes the
/// value; VibeSQL must match (both materialized at write time).
#[test]
fn add_generated_column_stored_and_virtual_both_compute() {
    for keyword in ["STORED", "VIRTUAL"] {
        let mut db = Database::new();
        exec_ddl_dml(&mut db, "CREATE TABLE g(x INTEGER)");
        exec_ddl_dml(
            &mut db,
            &format!("ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1) {keyword}"),
        );
        exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(4)");
        assert_eq!(query_col(&db, "SELECT y FROM g"), vec![int(5)], "{keyword} must compute");
    }
}

/// A plain (non-generated) added column must keep its NULL default behavior.
#[test]
fn add_plain_column_still_defaults_to_null() {
    let mut db = Database::new();
    exec_ddl_dml(&mut db, "CREATE TABLE g(x INTEGER)");
    exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(4)");
    exec_ddl_dml(&mut db, "ALTER TABLE g ADD COLUMN z INTEGER");
    assert_eq!(query_col(&db, "SELECT z FROM g"), vec![SqlValue::Null]);
}

/// The CREATE TABLE generated-column path must not regress.
#[test]
fn create_table_generated_column_still_works() {
    let mut db = Database::new();
    exec_ddl_dml(&mut db, "CREATE TABLE t(x INT, y INTEGER GENERATED ALWAYS AS (x+1))");
    exec_ddl_dml(&mut db, "INSERT INTO t(x) VALUES(4)");
    assert_eq!(query_col(&db, "SELECT y FROM t"), vec![int(5)]);
}

/// Persistence round-trip: the generated expression must survive a binary
/// save/reload (catalog already serializes `generated_expr`), and inserts after
/// the reload must still compute.
#[test]
fn add_generated_column_survives_binary_reload() {
    let mut db = Database::new();
    exec_ddl_dml(&mut db, "CREATE TABLE g(x INTEGER)");
    exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(10)");
    exec_ddl_dml(&mut db, "ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1)");

    let path = std::env::temp_dir()
        .join(format!("vibesql_5861_reload_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let mut reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();

    // The generated expression must be rehydrated on load.
    let col = reloaded
        .get_table("g")
        .expect("table g")
        .schema
        .columns
        .iter()
        .find(|c| c.name == "y")
        .expect("column y");
    assert!(col.generated_expr.is_some(), "generated_expr must survive reload");

    // Backfilled value survives, and new inserts still compute post-reload.
    assert_eq!(query_col(&reloaded, "SELECT y FROM g"), vec![int(11)]);
    exec_ddl_dml(&mut reloaded, "INSERT INTO g(x) VALUES(20)");
    assert_eq!(query_col(&reloaded, "SELECT y FROM g ORDER BY x"), vec![int(11), int(21)]);
}
