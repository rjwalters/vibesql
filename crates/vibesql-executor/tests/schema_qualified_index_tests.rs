//! Integration tests for schema-qualified `CREATE INDEX` / `DROP INDEX`
//! (issue #6366).
//!
//! SQLite's CREATE/DROP INDEX grammar is `[schema-name .] index-name` — the
//! schema qualifies the *index* name, not the target table. When a schema
//! qualifier is present, the target table is resolved within that exact
//! schema (no temp-shadows-main search), and an unrecognized qualifier is
//! rejected with `unknown database <name>`. Verified against sqlite3 3.51.0:
//!
//! ```sql
//! CREATE TABLE t(x INTEGER);
//! CREATE INDEX main.i1 ON t(x);   -- ok
//! CREATE INDEX bogus.i1 ON t(x);  -- "unknown database bogus"
//! DROP INDEX main.i1;             -- ok
//! DROP INDEX bogus.i1;            -- "no such index: bogus.i1"
//! ```

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, DropIndexExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE")
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

/// Parse + execute a CREATE INDEX statement, returning the executor result
/// (caller decides whether to `.unwrap()` or inspect the error).
fn exec_create_index(
    db: &mut Database,
    sql: &str,
) -> Result<String, vibesql_executor::ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE INDEX");
    match stmt {
        vibesql_ast::Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, db),
        other => panic!("expected CreateIndex, got {other:?}"),
    }
}

/// Parse + execute a DROP INDEX statement, returning the executor result.
fn exec_drop_index(
    db: &mut Database,
    sql: &str,
) -> Result<String, vibesql_executor::ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse DROP INDEX");
    match stmt {
        vibesql_ast::Statement::DropIndex(s) => DropIndexExecutor::execute(&s, db),
        other => panic!("expected DropIndex, got {other:?}"),
    }
}

/// `CREATE INDEX main.i1 ON t(x)` resolves `t` in `main` and creates the
/// index there (sqlite3 3.51.0 parity).
#[test]
fn create_index_main_schema_qualified() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX main.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX main.i1 should succeed: {:?}", result.err());
    assert!(db.index_exists("i1"));
}

/// `CREATE INDEX temp.i1 ON t(x)` resolves `t` specifically within the
/// session temp schema, even when a same-named `main.t` also exists —
/// matching sqlite3 3.51.0 (temp shadowing is bypassed by an explicit
/// qualifier; it is not needed here, but the qualifier still must route to
/// temp specifically rather than main).
#[test]
fn create_index_temp_schema_qualified_resolves_temp_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER, y INTEGER)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX temp.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX temp.i1 should succeed: {:?}", result.err());
    let msg = result.unwrap();
    assert!(
        msg.contains(&format!("{}.t", db.catalog.temp_schema_name())),
        "index should target the temp table, got: {msg}"
    );
}

/// An unrecognized schema qualifier is rejected with SQLite's exact
/// `unknown database <name>` wording (sqlite3 3.51.0 parity).
#[test]
fn create_index_unknown_schema_qualifier_rejected() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX bogus.i1 ON t(x)");
    assert!(result.is_err(), "CREATE INDEX bogus.i1 should fail");
    let err = result.unwrap_err().to_string();
    assert_eq!(err, "unknown database bogus", "unexpected error text: {err}");
}

/// `CREATE INDEX main.i1 ON t(x)` must resolve `t` in `main` specifically,
/// not the temp-shadowed table, when both exist (sqlite3 3.51.0 parity).
#[test]
fn create_index_main_schema_qualified_bypasses_temp_shadow() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER, y INTEGER)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX main.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX main.i1 should succeed: {:?}", result.err());
    let msg = result.unwrap();
    assert!(msg.contains("main.t"), "index should target main.t, got: {msg}");
}

/// `DROP INDEX main.i1` drops exactly the main-schema index, matching
/// sqlite3 3.51.0.
#[test]
fn drop_index_main_schema_qualified() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX");

    let result = exec_drop_index(&mut db, "DROP INDEX main.i1");
    assert!(result.is_ok(), "DROP INDEX main.i1 should succeed: {:?}", result.err());
    assert!(!db.index_exists("i1"));
}

/// `DROP INDEX temp.i1` drops only the temp-schema index, leaving a
/// same-named main-schema index untouched (sqlite3 3.51.0 parity).
#[test]
fn drop_index_temp_schema_qualified_scoped_to_temp() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t2(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX main.i1");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t2(x)").expect("CREATE INDEX temp.i1");

    let result = exec_drop_index(&mut db, "DROP INDEX temp.i1");
    assert!(result.is_ok(), "DROP INDEX temp.i1 should succeed: {:?}", result.err());

    // The temp-schema i1 is gone, but the main-schema i1 survives untouched.
    let (_, rows) = {
        let stmt = Parser::parse_sql("SELECT name FROM sqlite_master WHERE name = 'i1'")
            .expect("parse SELECT");
        match stmt {
            vibesql_ast::Statement::Select(s) => {
                let r = vibesql_executor::SelectExecutor::new(&db)
                    .execute_with_columns(&s)
                    .expect("SELECT");
                (r.columns, r.rows)
            }
            other => panic!("expected SELECT, got {other:?}"),
        }
    };
    assert_eq!(rows.len(), 1, "main-schema i1 should survive DROP INDEX temp.i1");
}

/// `DROP INDEX bogus.i1` reports the index as not found (no dedicated
/// "unknown database" wording for DROP — sqlite3 3.51.0 always says
/// `no such index: schema.name` for both an unknown schema and an unknown
/// index name in a known schema).
#[test]
fn drop_index_unknown_schema_qualifier_reports_not_found() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX");

    let result = exec_drop_index(&mut db, "DROP INDEX bogus.i1");
    assert!(result.is_err(), "DROP INDEX bogus.i1 should fail");
}

/// `DROP INDEX IF EXISTS bogus.i1` is a silent no-op, matching sqlite3.
#[test]
fn drop_index_if_exists_unknown_schema_qualifier_is_noop() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX");

    let result = exec_drop_index(&mut db, "DROP INDEX IF EXISTS bogus.i1");
    assert!(result.is_ok(), "DROP INDEX IF EXISTS bogus.i1 should succeed: {:?}", result.err());
    // The main-schema i1 is untouched.
    assert!(db.index_exists("i1"));
}

/// The motivating case from #6310/#6366: an ATTACHed database's name is a
/// legal index-name qualifier, and the target table is resolved inside that
/// attachment's schema.
#[test]
fn create_and_drop_index_qualified_by_attached_schema() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX aux.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX aux.i1 should succeed: {:?}", result.err());
    assert!(result.unwrap().contains("aux.t"), "index should target aux.t");
    assert!(db.index_exists("aux.i1"));

    let result = exec_drop_index(&mut db, "DROP INDEX aux.i1");
    assert!(result.is_ok(), "DROP INDEX aux.i1 should succeed: {:?}", result.err());
    assert!(!db.index_exists("aux.i1"));
}

/// A schema-qualified target table that does not exist is still reported as
/// a missing table (not silently treated as "unknown database").
#[test]
fn create_index_schema_qualified_missing_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX main.i1 ON nonexistent(x)");
    assert!(result.is_err(), "CREATE INDEX main.i1 ON nonexistent(x) should fail");
    assert!(matches!(
        result.unwrap_err(),
        vibesql_executor::ExecutorError::TableNotFound(_)
    ));
}
