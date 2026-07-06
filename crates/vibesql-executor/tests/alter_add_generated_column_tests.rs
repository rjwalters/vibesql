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
//! backfill path. A VIRTUAL generated column (the default when neither keyword
//! is given) may be added to a populated table; a STORED generated column may
//! only be added while the table is empty (`cannot add a STORED column`
//! otherwise), and DEFAULT may not be combined with a generated clause
//! (`cannot use DEFAULT on a generated column`). VibeSQL matches all four.

use vibesql_ast::Statement;
use vibesql_executor::{AlterTableExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor};
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

/// On an *empty* table sqlite3 3.51.0 accepts both STORED and VIRTUAL via ALTER
/// and computes the value on the subsequent insert; VibeSQL must match (both
/// materialized at write time). The STORED-on-populated case is rejected — see
/// `add_generated_stored_column_on_populated_table_errors`.
#[test]
fn add_generated_column_stored_and_virtual_on_empty_table_compute() {
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

/// Parse a statement, expecting success.
fn parse_ok(sql: &str) {
    Parser::parse_sql(sql).unwrap_or_else(|e| panic!("expected {sql:?} to parse: {e:?}"));
}

/// Parse a statement, expecting a ParseError whose message contains `needle`.
fn parse_err_contains(sql: &str, needle: &str) {
    match Parser::parse_sql(sql) {
        Ok(stmt) => panic!("expected {sql:?} to be a parse error, got {stmt:?}"),
        Err(e) => assert!(
            e.message.contains(needle),
            "expected error for {sql:?} to contain {needle:?}, got {:?}",
            e.message
        ),
    }
}

/// Adding an explicit STORED generated column to a *populated* table must be
/// rejected, matching sqlite3 3.51.0's `cannot add a STORED column` (a STORED
/// column would require rewriting persisted row data). A VIRTUAL add on the same
/// populated table stays allowed (backfilled at read/write time).
#[test]
fn add_generated_stored_column_on_populated_table_errors() {
    let mut db = Database::new();
    exec_ddl_dml(&mut db, "CREATE TABLE g(x INTEGER)");
    exec_ddl_dml(&mut db, "INSERT INTO g(x) VALUES(10)");

    let stmt =
        Parser::parse_sql("ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1) STORED")
            .expect("parse STORED add");
    let Statement::AlterTable(alter) = stmt else { panic!("expected ALTER TABLE") };
    let err = AlterTableExecutor::execute(&alter, &mut db)
        .expect_err("STORED add on populated table must error");
    assert!(err.to_string().contains("cannot add a STORED column"), "unexpected error: {err}");

    // The rejected ALTER must not have mutated the schema.
    assert!(
        db.get_table("g").unwrap().schema.get_column_index("y").is_none(),
        "column y must not have been added after the rejected STORED add"
    );

    // A VIRTUAL add on the same populated table is still accepted and backfills.
    exec_ddl_dml(&mut db, "ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1) VIRTUAL");
    assert_eq!(query_col(&db, "SELECT y FROM g"), vec![int(11)]);
}

/// A generated column that also declares DEFAULT is invalid SQL. sqlite3 3.51.0
/// rejects it with `cannot use DEFAULT on a generated column`, in either clause
/// order and for both the ALTER TABLE and CREATE TABLE paths.
#[test]
fn generated_column_with_default_is_rejected() {
    // ALTER: GENERATED ... DEFAULT
    parse_err_contains(
        "ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1) DEFAULT 7",
        "cannot use DEFAULT on a generated column",
    );
    // ALTER: short-form AS (...) DEFAULT
    parse_err_contains(
        "ALTER TABLE g ADD COLUMN y INTEGER AS (x+1) DEFAULT 7",
        "cannot use DEFAULT on a generated column",
    );
    // ALTER: DEFAULT ... GENERATED (reverse order)
    parse_err_contains(
        "ALTER TABLE g ADD COLUMN y INTEGER DEFAULT 7 GENERATED ALWAYS AS (x+1)",
        "cannot use DEFAULT on a generated column",
    );
    // CREATE TABLE: GENERATED ... DEFAULT
    parse_err_contains(
        "CREATE TABLE g(x INTEGER, y INTEGER GENERATED ALWAYS AS (x+1) DEFAULT 7)",
        "cannot use DEFAULT on a generated column",
    );

    // A generated column without DEFAULT, and a plain column with DEFAULT, are
    // both still valid (the guard must not over-reject).
    parse_ok("ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1)");
    parse_ok("ALTER TABLE g ADD COLUMN y INTEGER DEFAULT 7");
    parse_ok("CREATE TABLE g(x INTEGER, y INTEGER GENERATED ALWAYS AS (x+1))");
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

    let path =
        std::env::temp_dir().join(format!("vibesql_5861_reload_{}.vbsql", std::process::id()));
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
