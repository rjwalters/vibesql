//! Regression tests for issue #5893: `DEFAULT <literal> NOT NULL` parses
//! (the column records `notnull=1`, `dflt_value=<literal>`) but the NOT NULL
//! constraint was silently dropped for an *explicitly* supplied `NULL`.
//!
//! Root cause was executor-only: `apply_default_values_with_batch_context`
//! ran before the NOT NULL check and could not distinguish a column omitted
//! from the INSERT (NULL = "not provided" → default applies) from a column
//! the caller explicitly set to NULL (NULL = "user said NULL" → NOT NULL must
//! fire). The fix passes the `assigned_columns` set as an exclusion mask so an
//! explicit NULL survives to the constraint check.
//!
//! sqlite3 reference semantics (verified against /usr/bin/sqlite3):
//!   CREATE TABLE t(a INT DEFAULT 5 NOT NULL);
//!   INSERT INTO t VALUES(NULL);          -- NOT NULL constraint failed: t.a
//!   INSERT INTO t DEFAULT VALUES;        -- a = 5
//!   INSERT INTO t(a,b) VALUES(NULL,1);   -- NOT NULL constraint failed: t.a
//!   INSERT INTO t(b) VALUES(1);          -- a = 5 (omitted → default)
//!   UPDATE t SET a=NULL;                 -- NOT NULL constraint failed: t.a

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Create a table preserving the verbatim source text, the way the CLI/load
/// paths capture it (so `sql_source` re-parses correctly on reload).
fn create_with_source(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

/// Execute a DML statement, returning Ok(()) or the error's Display text.
fn exec(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {e:?}"))?;
    match stmt {
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

/// Read a single-column single-row scalar (used to assert the applied default).
fn scalar(db: &Database, sql: &str) -> vibesql_types::SqlValue {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result.rows[0].values[0].clone()
}

fn count(db: &Database, table: &str) -> i64 {
    match scalar(db, &format!("SELECT COUNT(*) FROM {table}")) {
        vibesql_types::SqlValue::Integer(n) | vibesql_types::SqlValue::Bigint(n) => n,
        other => panic!("unexpected COUNT value: {other:?}"),
    }
}

/// Save to a binary `.vbsql` file and reload — the cross-process reopen path
/// (#5878 rehydration).
fn reopen_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_5893_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded
}

fn assert_not_null_error(res: Result<(), String>, table_col: &str) {
    let err = res.expect_err("expected NOT NULL constraint violation");
    assert!(
        err.contains("NOT NULL constraint failed"),
        "expected NOT NULL error, got: {err}"
    );
    assert!(err.contains(table_col), "expected `{table_col}` in error, got: {err}");
}

// ---------------------------------------------------------------------------
// Explicit NULL must hit the NOT NULL check (was silently swapped for default)
// ---------------------------------------------------------------------------

#[test]
fn positional_explicit_null_is_rejected() {
    // sqlite3: INSERT INTO t VALUES(NULL) -> NOT NULL constraint failed: t.a
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL)");
    assert_not_null_error(exec(&mut db, "INSERT INTO t VALUES(NULL)"), "t.a");
    assert_eq!(count(&db, "t"), 0, "rejected row must not be inserted");
}

#[test]
fn column_list_explicit_null_is_rejected() {
    // sqlite3: INSERT INTO t(a,b) VALUES(NULL,1) -> NOT NULL constraint failed: t.a
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL, b INT)");
    assert_not_null_error(exec(&mut db, "INSERT INTO t(a, b) VALUES(NULL, 1)"), "t.a");
    assert_eq!(count(&db, "t"), 0);
}

// ---------------------------------------------------------------------------
// Omitted column still receives its default (must not regress)
// ---------------------------------------------------------------------------

#[test]
fn omitted_column_gets_default() {
    // sqlite3: INSERT INTO t(b) VALUES(1) -> a = 5
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL, b INT)");
    exec(&mut db, "INSERT INTO t(b) VALUES(1)").expect("omitted column insert should succeed");
    assert_eq!(scalar(&db, "SELECT a FROM t"), vibesql_types::SqlValue::Integer(5));
}

#[test]
fn default_values_gets_default() {
    // sqlite3: INSERT INTO t DEFAULT VALUES -> a = 5
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL)");
    exec(&mut db, "INSERT INTO t DEFAULT VALUES").expect("DEFAULT VALUES should succeed");
    assert_eq!(scalar(&db, "SELECT a FROM t"), vibesql_types::SqlValue::Integer(5));
}

#[test]
fn explicit_default_keyword_gets_default() {
    // The DEFAULT keyword resolves to 5 (non-NULL) before the default pass runs,
    // so the NOT NULL column is satisfied. (VibeSQL supports VALUES(DEFAULT);
    // the bundled sqlite3 CLI rejects it as a syntax error, so this is a
    // VibeSQL-only assertion, not a differential one.)
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL, b INT)");
    exec(&mut db, "INSERT INTO t(a, b) VALUES(DEFAULT, 1)").expect("VALUES(DEFAULT) should succeed");
    assert_eq!(scalar(&db, "SELECT a FROM t"), vibesql_types::SqlValue::Integer(5));
}

// ---------------------------------------------------------------------------
// Multi-row: a mix of provided-and-explicit-NULL rows aborts the statement;
// a mix of omitted rows all receive the default.
// ---------------------------------------------------------------------------

#[test]
fn multi_row_mixed_explicit_null_aborts() {
    // sqlite3: INSERT INTO t(a,b) VALUES(10,1),(NULL,2) -> NOT NULL constraint failed: t.a
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL, b INT)");
    assert_not_null_error(
        exec(&mut db, "INSERT INTO t(a, b) VALUES(10, 1), (NULL, 2)"),
        "t.a",
    );
    // Statement aborts atomically; no partial rows survive.
    assert_eq!(count(&db, "t"), 0);
}

#[test]
fn multi_row_all_omitted_get_default() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL, b INT)");
    exec(&mut db, "INSERT INTO t(b) VALUES(1), (2)").expect("omitted-column multi-row insert");
    assert_eq!(count(&db, "t"), 2);
    // Both rows received the default a = 5.
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM t WHERE a = 5"),
        vibesql_types::SqlValue::Integer(2)
    );
}

// ---------------------------------------------------------------------------
// UPDATE path: assigning NULL to a NOT NULL/DEFAULT column must also error.
// UPDATE has its own NOT NULL check on the new row and does not route through
// apply_default_values, so it already enforced correctly — this guards against
// a future regression that would apply defaults over an explicit UPDATE NULL.
// ---------------------------------------------------------------------------

#[test]
fn update_set_null_is_rejected() {
    // sqlite3: UPDATE t SET a=NULL -> NOT NULL constraint failed: t.a
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL, b INT)");
    exec(&mut db, "INSERT INTO t(b) VALUES(1)").expect("seed row");
    assert_not_null_error(exec(&mut db, "UPDATE t SET a = NULL"), "t.a");
    // Row keeps its previous (defaulted) value, unchanged.
    assert_eq!(scalar(&db, "SELECT a FROM t"), vibesql_types::SqlValue::Integer(5));
}

// ---------------------------------------------------------------------------
// Enforcement survives a binary save/reload (#5878 rehydration path).
// ---------------------------------------------------------------------------

#[test]
fn enforcement_survives_reload() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INT DEFAULT 5 NOT NULL, b INT)");
    exec(&mut db, "INSERT INTO t(b) VALUES(1)").expect("seed default-applied row");

    let mut reloaded = reopen_binary(&db, "notnull");

    // Default column persisted correctly.
    assert_eq!(scalar(&reloaded, "SELECT a FROM t"), vibesql_types::SqlValue::Integer(5));

    // Explicit NULL is still rejected after reopen.
    assert_not_null_error(
        exec(&mut reloaded, "INSERT INTO t(a, b) VALUES(NULL, 2)"),
        "t.a",
    );
    // Omitted column still receives the default after reopen.
    exec(&mut reloaded, "INSERT INTO t(b) VALUES(3)").expect("omitted insert after reload");
    assert_eq!(
        scalar(&reloaded, "SELECT COUNT(*) FROM t WHERE a = 5"),
        vibesql_types::SqlValue::Integer(2)
    );
}
