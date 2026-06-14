//! Integration tests for schema-aware catalog indexes and `sqlite_temp_master`.
//!
//! Issue #5513: A temp-table index lives in the session temp schema. It must be
//! listed in `sqlite_temp_master` (not `sqlite_master`), dropped with its temp
//! table, and able to coexist with a same-named main-schema index. This matches
//! SQLite 3.51.0:
//!
//! ```sql
//! CREATE TEMP TABLE t(a);
//! CREATE INDEX i ON t(a);
//! SELECT name,type FROM sqlite_temp_master;  -- lists t and i
//! SELECT name FROM sqlite_master;            -- lists neither
//! ```

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DropTableExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

fn exec_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE")
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

fn exec_create_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE INDEX");
    match stmt {
        vibesql_ast::Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).expect("CREATE INDEX")
        }
        other => panic!("expected CREATE INDEX, got {other:?}"),
    };
}

fn exec_drop_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse DROP TABLE");
    match stmt {
        vibesql_ast::Statement::DropTable(s) => {
            DropTableExecutor::execute(&s, db).expect("DROP TABLE")
        }
        other => panic!("expected DROP TABLE, got {other:?}"),
    };
}

fn select(db: &Database, sql: &str) -> (Vec<String>, Vec<Row>) {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    match stmt {
        vibesql_ast::Statement::Select(s) => {
            let executor = SelectExecutor::new(db);
            let r = executor.execute_with_columns(&s).expect("SELECT");
            (r.columns, r.rows)
        }
        other => panic!("expected SELECT, got {other:?}"),
    }
}

/// Pull the `name` (column 0) string values out of a result set.
fn names(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected text name, got {other:?}"),
        })
        .collect()
}

/// sqlite3 parity: a temp table and its index appear in `sqlite_temp_master`.
#[test]
fn temp_table_and_index_listed_in_temp_master() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");
    exec_create_index(&mut db, "CREATE INDEX i ON t(a)");

    let (_, rows) = select(&db, "SELECT name FROM sqlite_temp_master ORDER BY name");
    let listed = names(&rows);
    assert!(listed.contains(&"t".to_string()), "temp table t should be in sqlite_temp_master: {listed:?}");
    assert!(listed.contains(&"i".to_string()), "temp index i should be in sqlite_temp_master: {listed:?}");
}

/// sqlite3 parity: temp objects are absent from `sqlite_master`.
#[test]
fn temp_index_absent_from_sqlite_master() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");
    exec_create_index(&mut db, "CREATE INDEX i ON t(a)");

    let (_, rows) = select(&db, "SELECT name FROM sqlite_master");
    let listed = names(&rows);
    assert!(!listed.contains(&"t".to_string()), "temp table must NOT be in sqlite_master: {listed:?}");
    assert!(!listed.contains(&"i".to_string()), "temp index must NOT be in sqlite_master: {listed:?}");
}

/// A main-table index stays in `sqlite_master` and is absent from temp_master.
#[test]
fn main_index_in_master_not_temp_master() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a)");
    exec_create_index(&mut db, "CREATE INDEX mi ON base(a)");

    let (_, master) = select(&db, "SELECT name FROM sqlite_master");
    assert!(names(&master).contains(&"mi".to_string()), "main index should be in sqlite_master");

    let (_, temp) = select(&db, "SELECT name FROM sqlite_temp_master");
    assert!(!names(&temp).contains(&"mi".to_string()), "main index must NOT be in sqlite_temp_master");
}

/// Catalog-level `main.i` / `temp.i` coexistence (same name, even same table)
/// is proven in `vibesql-catalog`'s `test_temp_and_main_index_coexist`.
///
/// At the SQL layer it is not yet reachable because the **storage** index
/// manager keys indexes by bare name (a global namespace), so a second
/// `CREATE INDEX i ...` is rejected before the catalog is consulted. Tracked as
/// a follow-on (storage index-manager schema-tagging). This test documents the
/// current SQL-level behaviour so the follow-on has a clear before/after.
#[test]
fn sql_level_same_name_index_across_schemas_currently_rejected() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");

    exec_create_index(&mut db, "CREATE INDEX i ON base(a)"); // main.i on base

    // Second index of the same name (on the temp table) is rejected today by
    // the storage-layer name namespace.
    let stmt = Parser::parse_sql("CREATE INDEX i ON t(a)").unwrap();
    let result = match stmt {
        vibesql_ast::Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, &mut db),
        other => panic!("expected CREATE INDEX, got {other:?}"),
    };
    assert!(
        result.is_err(),
        "documents current SQL-level limitation; see storage schema-tagging follow-on"
    );
}

/// A temp index is dropped together with its temp table, and dropping the temp
/// table does not disturb a main index on another table.
#[test]
fn temp_index_dropped_with_temp_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");
    exec_create_index(&mut db, "CREATE INDEX mi ON base(a)"); // main index
    exec_create_index(&mut db, "CREATE INDEX ti ON t(a)"); // temp index

    // Before: temp_master lists the temp index; master lists the main index.
    let (_, before) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'");
    assert!(names(&before).contains(&"ti".to_string()));

    // Drop the temp table. Use the schema-qualified name because an unqualified
    // DROP TABLE does not yet resolve into the temp schema (separate pre-existing
    // limitation, unrelated to #5513's index work).
    exec_drop_table(&mut db, "DROP TABLE temp.t");

    // After: temp index gone from temp_master.
    let (_, after_temp) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'");
    assert!(
        !names(&after_temp).contains(&"ti".to_string()),
        "temp index should be dropped with the temp table"
    );

    // Main index on the other table survives.
    let (_, after_main) = select(&db, "SELECT name FROM sqlite_master WHERE type='index'");
    assert!(
        names(&after_main).contains(&"mi".to_string()),
        "main index must survive dropping the temp table"
    );
}
