//! End-to-end regression tests for issue #6303: `ALTER TABLE <old> RENAME TO
//! <new>` must propagate the rename into dependent VIEW definitions that
//! reference the old table name — mirroring the cascade that already exists
//! for `ALTER TABLE ... RENAME COLUMN` (`rewrite_views_for_column_rename`).
//!
//! Before the fix, `execute_rename_table` cascaded the rename into triggers
//! (`rewrite_triggers_for_rename`) and child foreign keys
//! (`rebind_child_foreign_keys`) but never touched views. A dependent view kept
//! its stale `query` AST and `sql_definition` pointing at the old table name,
//! so the next read through the view failed with a stale-name lookup error
//! (`Table 'main.t1' not found`).

use vibesql_ast::Statement;
use vibesql_executor::{
    AlterTableExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a single SQL statement (CREATE TABLE / CREATE VIEW / ALTER TABLE /
/// INSERT), preserving verbatim source text where the executor supports it.
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(create) => {
            CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
        }
        Statement::CreateView(mut create) => {
            // Match the CLI path (vibesql-cli/src/executor/mod.rs): the parser
            // itself never populates `sql_definition` (it is `None` until a
            // caller stamps the verbatim source text on), so the view's
            // `sqlite_master.sql` text — and the rename-cascade rewrite under
            // test — reflects the real, verbatim `CREATE VIEW` text rather than
            // a `ToSql`-reconstructed (differently formatted) approximation.
            create.sql_definition = Some(sql.to_string());
            ViewExecutor::execute_create_view(&create, db).expect("CREATE VIEW");
        }
        Statement::AlterTable(alter) => {
            AlterTableExecutor::execute_with_source(&alter, db, Some(sql)).expect("ALTER TABLE");
        }
        Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert).expect("INSERT");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

/// Run a SELECT and return the resulting rows.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result.rows.into_iter().map(|r| r.values.to_vec()).collect()
}

/// Return the `sql` text for the named view from `sqlite_master`.
fn object_sql(db: &Database, name: &str) -> String {
    let rows = query(db, &format!("SELECT sql FROM sqlite_master WHERE name='{name}'"));
    assert_eq!(rows.len(), 1, "expected one sqlite_master row for {name}");
    match &rows[0][0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// Save to the binary `.vbsql` format and reload — exercises the persistence
/// round-trip so a rewritten view survives a save + reopen.
fn roundtrip_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_6303_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path)
        .expect("load_binary must succeed after RENAME TO with a dependent view (#6303)");
    std::fs::remove_file(&path).ok();
    reloaded
}

#[test]
fn plain_view_survives_table_rename() {
    // The exact reproducer from issue #6303.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "INSERT INTO t1 VALUES(1)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    assert_eq!(query(&db, "SELECT * FROM v1"), vec![vec![SqlValue::Integer(1)]]);
    assert_eq!(object_sql(&db, "v1"), "CREATE VIEW v1 AS SELECT * FROM \"t1x\"");
}

#[test]
fn qualified_column_reference_is_rewritten() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "INSERT INTO t1 VALUES(5)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT t1.a FROM t1");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    assert_eq!(query(&db, "SELECT * FROM v1"), vec![vec![SqlValue::Integer(5)]]);
    assert_eq!(object_sql(&db, "v1"), "CREATE VIEW v1 AS SELECT \"t1x\".a FROM \"t1x\"");
}

#[test]
fn only_renamed_table_reference_is_rewritten_in_multi_table_view() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "CREATE TABLE t2(b)");
    exec(&mut db, "INSERT INTO t1 VALUES(1)");
    exec(&mut db, "INSERT INTO t2 VALUES(2)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT t1.a, t2.b FROM t1, t2");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    assert_eq!(
        query(&db, "SELECT * FROM v1"),
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(2)]]
    );
    assert_eq!(object_sql(&db, "v1"), "CREATE VIEW v1 AS SELECT \"t1x\".a, t2.b FROM \"t1x\", t2");
}

#[test]
fn view_unaffected_by_rename_is_byte_for_byte_untouched() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "CREATE TABLE t2(b)");
    exec(&mut db, "CREATE VIEW v2 AS SELECT * FROM t2");
    let before = object_sql(&db, "v2");

    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    assert_eq!(object_sql(&db, "v2"), before);
    assert_eq!(object_sql(&db, "v2"), "CREATE VIEW v2 AS SELECT * FROM t2");
}

#[test]
fn rename_with_zero_views_defined_is_not_a_regression() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "INSERT INTO t1 VALUES(9)");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    assert_eq!(query(&db, "SELECT * FROM t1x"), vec![vec![SqlValue::Integer(9)]]);
}

#[test]
fn bare_column_named_like_old_table_right_after_first_on_is_not_rewritten() {
    // Guards the exact hazard the trigger-body rewriter's "first ON is a
    // header target" heuristic would introduce if it were mistakenly reused
    // for views: a *column* named the same as the renamed table appearing
    // immediately after the view body's first `ON` (a JOIN condition, not a
    // trigger header) must be left alone — only `rewrite_table_refs_in_view_sql`
    // (which disables that heuristic) must be used to cascade table renames
    // into view text (issue #6303).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "CREATE TABLE t2(t1, y)");
    exec(&mut db, "CREATE TABLE t3(z)");
    exec(&mut db, "INSERT INTO t2 VALUES(5, 20)");
    exec(&mut db, "INSERT INTO t3 VALUES(5)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT t2.y, t3.z FROM t2 JOIN t3 ON t1 = t3.z");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    // The bare column `t1` (owned by t2, unrelated to the renamed table) is
    // untouched; the view keeps resolving and executing correctly.
    assert_eq!(
        query(&db, "SELECT * FROM v1"),
        vec![vec![SqlValue::Integer(20), SqlValue::Integer(5)]]
    );
    assert_eq!(
        object_sql(&db, "v1"),
        "CREATE VIEW v1 AS SELECT t2.y, t3.z FROM t2 JOIN t3 ON t1 = t3.z"
    );
}

#[test]
fn rewrite_survives_binary_persistence_round_trip() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "INSERT INTO t1 VALUES(1)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    let reloaded = roundtrip_binary(&db, "plain");
    assert_eq!(query(&reloaded, "SELECT * FROM v1"), vec![vec![SqlValue::Integer(1)]]);
    assert_eq!(object_sql(&reloaded, "v1"), "CREATE VIEW v1 AS SELECT * FROM \"t1x\"");
}

#[test]
fn nested_cte_in_view_references_are_rewritten() {
    // A shape close to with2.test 12.1: the view body wraps its reference to
    // the renamed table inside a CTE. The token-level rewriter scans the whole
    // text, so this is covered without any special-casing.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "INSERT INTO t1 VALUES(7)");
    exec(&mut db, "CREATE VIEW v1 AS WITH cte AS (SELECT * FROM t1) SELECT * FROM cte");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1x");

    assert_eq!(query(&db, "SELECT * FROM v1"), vec![vec![SqlValue::Integer(7)]]);
    assert_eq!(
        object_sql(&db, "v1"),
        "CREATE VIEW v1 AS WITH cte AS (SELECT * FROM \"t1x\") SELECT * FROM cte"
    );
}
