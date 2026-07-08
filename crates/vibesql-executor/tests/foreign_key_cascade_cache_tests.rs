//! Tests for FK cascade actions + columnar cache invalidation
//!
//! Referential actions (ON DELETE / ON UPDATE CASCADE / SET NULL / SET DEFAULT)
//! mutate child tables through the low-level `Table` API, bypassing the
//! `Database`-level DML path that invalidates the columnar cache. If the child
//! table's columnar representation was primed (converted) *before* the parent
//! mutation, subsequent reads used to serve stale cached data.
//!
//! These tests prime the child table's columnar cache before the parent
//! mutation and verify the post-cascade columnar read reflects the mutation.
//! Expected values match SQLite (`sqlite3`) semantics for each action.
//!
//! Regression: #5876

use vibesql_executor::{CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Build a database with FK enforcement enabled and execute the given
/// (semicolon-separated) setup SQL.
fn setup_db(sql: &str) -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse setup SQL");
        execute_statement(&stmt, &mut db);
    }
    db
}

/// Execute a single parsed statement against the database.
fn execute_statement(stmt: &vibesql_ast::Statement, db: &mut Database) {
    use vibesql_ast::Statement;
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(create_stmt, db).expect("Failed to execute CREATE TABLE");
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, insert_stmt).expect("Failed to execute INSERT");
        }
        Statement::Delete(delete_stmt) => {
            DeleteExecutor::execute(delete_stmt, db).expect("Failed to execute DELETE");
        }
        Statement::Update(update_stmt) => {
            UpdateExecutor::execute(update_stmt, db).expect("Failed to execute UPDATE");
        }
        _ => panic!("Unsupported statement type in test setup"),
    }
}

/// Execute one DML statement (parent mutation) against the database.
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    execute_statement(&stmt, db);
}

/// Prime the columnar cache for a table by forcing a conversion, returning the
/// row count observed. This is the "read before the cascade" that used to leave
/// a stale entry in the cache.
fn prime_columnar(db: &mut Database, table: &str) -> usize {
    db.get_columnar(table).unwrap().expect("Table should exist").row_count()
}

/// Read all values of a single column from the (post-mutation) columnar cache.
fn columnar_column(db: &mut Database, table: &str, column: &str) -> Vec<SqlValue> {
    let columnar = db.get_columnar(table).unwrap().expect("Table should exist");
    let col = columnar.get_column(column).expect("Column should exist");
    (0..columnar.row_count()).map(|i| col.get(i)).collect()
}

/// The exact reproducer from issue #5876.
///
/// Prime `COUNT(*)`-style columnar read on child `c` (2 rows), delete the parent
/// row (cascade deletes one child row), then read the child columnar cache again:
/// it must report 1 row, not the stale 2.
#[test]
fn on_delete_cascade_invalidates_child_columnar_cache() {
    let mut db = setup_db(
        r#"
        CREATE TABLE p (x INTEGER PRIMARY KEY);
        CREATE TABLE c (y INTEGER REFERENCES p(x) ON DELETE CASCADE, z INTEGER);
        INSERT INTO p VALUES (1);
        INSERT INTO p VALUES (2);
        INSERT INTO c VALUES (1, 10);
        INSERT INTO c VALUES (2, 20);
        "#,
    );

    // Prime the columnar cache for c (this is what made the bug observable).
    assert_eq!(prime_columnar(&mut db, "c"), 2, "child should start with 2 rows");
    let conversions_before = db.columnar_cache_stats().conversions;

    // Cascade delete c(1, 10) via the parent.
    exec(&mut db, "DELETE FROM p WHERE x = 1");

    // Post-cascade columnar read must reflect the deletion (SQLite: 1 row).
    let columnar = db.get_columnar("c").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 1, "stale cache served: cascade delete not reflected");

    // The surviving row is c(2, 20); the cascaded row's key (1) must be gone.
    let ys = columnar_column(&mut db, "c", "y");
    assert!(!ys.contains(&SqlValue::Integer(1)), "cascaded child key 1 should be gone");
    assert!(ys.contains(&SqlValue::Integer(2)), "unaffected child key 2 should remain");

    // Cache must have been re-converted (proves invalidation, not a stale hit).
    assert!(
        db.columnar_cache_stats().conversions > conversions_before,
        "columnar cache should have been invalidated and re-converted"
    );
}

/// ON DELETE SET NULL: the child row survives but its FK column becomes NULL.
/// A columnar read primed before the cascade must show the NULL, not the stale
/// prior value.
#[test]
fn on_delete_set_null_invalidates_child_columnar_cache() {
    let mut db = setup_db(
        r#"
        CREATE TABLE p (x INTEGER PRIMARY KEY);
        CREATE TABLE c (y INTEGER REFERENCES p(x) ON DELETE SET NULL, z INTEGER);
        INSERT INTO p VALUES (1);
        INSERT INTO p VALUES (2);
        INSERT INTO c VALUES (1, 10);
        INSERT INTO c VALUES (2, 20);
        "#,
    );

    assert_eq!(prime_columnar(&mut db, "c"), 2);

    exec(&mut db, "DELETE FROM p WHERE x = 1");

    // Row count unchanged; the FK value for the affected row is now NULL.
    let ys = columnar_column(&mut db, "c", "y");
    assert_eq!(ys.len(), 2, "SET NULL keeps the child row");
    assert!(ys.contains(&SqlValue::Null), "affected FK column should read NULL, not stale 1");
    assert!(!ys.contains(&SqlValue::Integer(1)), "stale FK value 1 must not be served");
    assert!(ys.contains(&SqlValue::Integer(2)), "unaffected FK value 2 should remain");
}

/// ON DELETE SET DEFAULT: the child FK column is reset to its column default.
/// A primed columnar read must show the default, not the stale prior value.
#[test]
fn on_delete_set_default_invalidates_child_columnar_cache() {
    let mut db = setup_db(
        r#"
        CREATE TABLE p (x INTEGER PRIMARY KEY);
        CREATE TABLE c (
            y INTEGER DEFAULT 0 REFERENCES p(x) ON DELETE SET DEFAULT,
            z INTEGER
        );
        INSERT INTO p VALUES (0);
        INSERT INTO p VALUES (1);
        INSERT INTO p VALUES (2);
        INSERT INTO c VALUES (1, 10);
        INSERT INTO c VALUES (2, 20);
        "#,
    );

    assert_eq!(prime_columnar(&mut db, "c"), 2);

    exec(&mut db, "DELETE FROM p WHERE x = 1");

    // The affected child row's FK is reset to the default (0).
    let ys = columnar_column(&mut db, "c", "y");
    assert_eq!(ys.len(), 2, "SET DEFAULT keeps the child row");
    assert!(ys.contains(&SqlValue::Integer(0)), "affected FK column should read default 0");
    assert!(!ys.contains(&SqlValue::Integer(1)), "stale FK value 1 must not be served");
    assert!(ys.contains(&SqlValue::Integer(2)), "unaffected FK value 2 should remain");
}

/// ON UPDATE CASCADE: updating the parent PK rewrites the child FK column.
/// A primed columnar read must show the new value, not the stale old one.
#[test]
fn on_update_cascade_invalidates_child_columnar_cache() {
    let mut db = setup_db(
        r#"
        CREATE TABLE p (x INTEGER PRIMARY KEY);
        CREATE TABLE c (y INTEGER REFERENCES p(x) ON UPDATE CASCADE, z INTEGER);
        INSERT INTO p VALUES (1);
        INSERT INTO p VALUES (2);
        INSERT INTO c VALUES (1, 10);
        INSERT INTO c VALUES (2, 20);
        "#,
    );

    assert_eq!(prime_columnar(&mut db, "c"), 2);
    let conversions_before = db.columnar_cache_stats().conversions;

    exec(&mut db, "UPDATE p SET x = 99 WHERE x = 1");

    // Child FK 1 -> 99; row count unchanged.
    let ys = columnar_column(&mut db, "c", "y");
    assert_eq!(ys.len(), 2, "cascade UPDATE should not change child row count");
    assert!(ys.contains(&SqlValue::Integer(99)), "cascaded FK value 99 should be served");
    assert!(!ys.contains(&SqlValue::Integer(1)), "stale FK value 1 must not be served");
    assert!(ys.contains(&SqlValue::Integer(2)), "unaffected FK value 2 should remain");

    assert!(
        db.columnar_cache_stats().conversions > conversions_before,
        "columnar cache should have been invalidated and re-converted"
    );
}

/// Regression guard: when the cache is NOT primed before the cascade, results
/// were already correct (fresh conversion after the mutation). This must not
/// regress.
#[test]
fn cache_correct_when_no_pre_delete_read() {
    let mut db = setup_db(
        r#"
        CREATE TABLE p (x INTEGER PRIMARY KEY);
        CREATE TABLE c (y INTEGER REFERENCES p(x) ON DELETE CASCADE, z INTEGER);
        INSERT INTO p VALUES (1);
        INSERT INTO p VALUES (2);
        INSERT INTO c VALUES (1, 10);
        INSERT INTO c VALUES (2, 20);
        "#,
    );

    // No pre-delete columnar read (cache never primed).
    exec(&mut db, "DELETE FROM p WHERE x = 1");

    let columnar = db.get_columnar("c").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 1, "fresh-conversion count must remain correct");
}
