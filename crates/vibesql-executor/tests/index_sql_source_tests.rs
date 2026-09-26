//! End-to-end tests for issue #6734: `sqlite_master.sql` for an index must be
//! the verbatim `CREATE INDEX` text (as SQLite stores it), with renamed
//! identifiers spliced in place on `ALTER TABLE ... RENAME TO` / `RENAME
//! COLUMN`, and the text must survive both persistence formats.
//!
//! Expected strings are the outputs of sqlite3 3.54.0 for the same statements.

use vibesql_executor::{
    load_sql_dump, AlterTableExecutor, CreateIndexExecutor, CreateTableExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a single statement, passing its verbatim text as source (the CLI
/// path).
fn exec(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e}")) {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute_with_source(&s, db, Some(sql)).expect("CREATE TABLE");
        }
        vibesql_ast::Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute_with_source(&s, db, Some(sql)).expect("CREATE INDEX");
        }
        vibesql_ast::Statement::AlterTable(s) => {
            AlterTableExecutor::execute_with_source(&s, db, Some(sql)).expect("ALTER TABLE");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result.rows.into_iter().map(|r| r.values.to_vec()).collect()
}

/// The `sql` text for the named index from `sqlite_master`.
fn index_sql(db: &Database, index: &str) -> String {
    let rows =
        query(db, &format!("SELECT sql FROM sqlite_master WHERE type='index' AND name='{index}'"));
    assert_eq!(rows.len(), 1, "expected one sqlite_master row for index {index}");
    match &rows[0][0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn temp_path(tag: &str, ext: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("vibesql_6734_{tag}_{}.{ext}", std::process::id()))
}

fn roundtrip_binary(db: &Database, tag: &str) -> Database {
    let path = temp_path(tag, "vbsql");
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded
}

fn roundtrip_sql_dump(db: &Database, tag: &str) -> Database {
    let path = temp_path(tag, "sql");
    db.save_sql_dump(&path).expect("save_sql_dump");
    let reloaded = load_sql_dump(&path).expect("load_sql_dump");
    std::fs::remove_file(&path).ok();
    reloaded
}

#[test]
fn create_index_text_is_verbatim() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(c0)");
    exec(&mut db, "CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )");
    assert_eq!(index_sql(&db, "i3"), "CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )");
}

#[test]
fn create_index_prefix_is_normalized_like_sqlite() {
    // SQLite records "CREATE[ UNIQUE] INDEX " + the source from the index name
    // onward: IF NOT EXISTS, the schema qualifier, keyword case/spacing, and
    // the trailing semicolon are not stored.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b)");
    exec(&mut db, "create  unique index if not exists main.u1 ON t1(a,  b) ;");
    assert_eq!(index_sql(&db, "u1"), "CREATE UNIQUE INDEX u1 ON t1(a,  b)");
}

#[test]
fn rename_table_quotes_new_name_in_index_text() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0)");
    exec(&mut db, "CREATE INDEX i0 ON t0(c0)");
    exec(&mut db, "CREATE INDEX i2 ON t0((c0+1))");
    exec(&mut db, "ALTER TABLE t0 RENAME TO t1");
    assert_eq!(index_sql(&db, "i0"), "CREATE INDEX i0 ON \"t1\"(c0)");
    assert_eq!(index_sql(&db, "i2"), "CREATE INDEX i2 ON \"t1\"((c0+1))");
}

#[test]
fn rename_column_splices_index_text() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t2 (c0, other)");
    exec(&mut db, "CREATE INDEX i2 ON t2((LIKELIHOOD(c0, 1.0) > 0))");
    exec(&mut db, "CREATE INDEX i3 ON t2 ( c0  DESC ) WHERE c0 > 'c0'");
    exec(&mut db, "CREATE INDEX i4 ON t2(other)");
    exec(&mut db, "ALTER TABLE t2 RENAME COLUMN c0 TO c1");
    assert_eq!(index_sql(&db, "i2"), "CREATE INDEX i2 ON t2((LIKELIHOOD(c1, 1.0) > 0))");
    assert_eq!(index_sql(&db, "i3"), "CREATE INDEX i3 ON t2 ( c1  DESC ) WHERE c1 > 'c0'");
    // An index that does not reference the renamed column is untouched.
    assert_eq!(index_sql(&db, "i4"), "CREATE INDEX i4 ON t2(other)");
}

#[test]
fn rename_column_quotes_when_replaced_token_was_quoted() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a, b)");
    exec(&mut db, "CREATE INDEX i ON t(\"a\", b)");
    exec(&mut db, "ALTER TABLE t RENAME COLUMN a TO z");
    assert_eq!(index_sql(&db, "i"), "CREATE INDEX i ON t(\"z\", b)");
}

#[test]
fn index_text_survives_binary_reload_after_renames() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0)");
    exec(&mut db, "CREATE INDEX i0 ON  t0 ( (c0 * 2) , c0 DESC )");
    exec(&mut db, "ALTER TABLE t0 RENAME TO t1");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN c0 TO c1");
    let expected = "CREATE INDEX i0 ON  \"t1\" ( (c1 * 2) , c1 DESC )";
    assert_eq!(index_sql(&db, "i0"), expected);

    let reloaded = roundtrip_binary(&db, "renames");
    assert_eq!(index_sql(&reloaded, "i0"), expected);
}

#[test]
fn index_text_survives_sql_dump_reload() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(c0)");
    exec(&mut db, "CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )");
    let reloaded = roundtrip_sql_dump(&db, "dump");
    assert_eq!(index_sql(&reloaded, "i3"), "CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )");
}

#[test]
fn index_without_source_still_reconstructs_after_rename() {
    // An index created without source text (programmatic AST, or loaded from a
    // pre-v19 file) keeps the reconstruction fallback, including after renames.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0)");
    let Ok(vibesql_ast::Statement::CreateIndex(stmt)) =
        Parser::parse_sql("CREATE INDEX i0 ON t0(c0)")
    else {
        panic!("expected CREATE INDEX");
    };
    CreateIndexExecutor::execute(&stmt, &mut db).expect("CREATE INDEX");
    assert_eq!(index_sql(&db, "i0"), "CREATE INDEX i0 ON t0(c0)");
    exec(&mut db, "ALTER TABLE t0 RENAME TO t1");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN c0 TO c1");
    assert_eq!(index_sql(&db, "i0"), "CREATE INDEX i0 ON t1(c1)");
    let reloaded = roundtrip_binary(&db, "nosource");
    assert_eq!(index_sql(&reloaded, "i0"), "CREATE INDEX i0 ON t1(c1)");
}
