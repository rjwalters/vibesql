//! End-to-end regression tests for issue #5877: `ALTER TABLE ... RENAME
//! COLUMN` must propagate the rename into dependent index metadata — plain
//! column lists, expression-index ASTs, and partial-index WHERE predicates —
//! in both the catalog copy and the storage copy.
//!
//! Before the fix, the index metadata written to the binary snapshot still
//! named the old column, so the next open failed fail-closed with
//! `Failed to create index: Column '<old>' not found in table '<t>'` — any
//! file-backed database that renamed an indexed column became unopenable
//! after its next checkpoint (the dominant altercol.test failure mode).

use vibesql_executor::{
    AlterTableExecutor, CreateIndexExecutor, CreateTableExecutor, SelectExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a single SQL statement (CREATE TABLE / CREATE INDEX / CREATE VIEW /
/// ALTER TABLE / INSERT).
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse");
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
        }
        vibesql_ast::Statement::CreateIndex(create) => {
            CreateIndexExecutor::execute(&create, db).expect("CREATE INDEX");
        }
        vibesql_ast::Statement::CreateView(create) => {
            ViewExecutor::execute_create_view(&create, db).expect("CREATE VIEW");
        }
        vibesql_ast::Statement::AlterTable(alter) => {
            AlterTableExecutor::execute_with_source(&alter, db, Some(sql)).expect("ALTER TABLE");
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).expect("INSERT");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

/// Return the `sql` text for the named table/view from `sqlite_master`.
fn object_sql(db: &Database, name: &str) -> String {
    let rows = query(db, &format!("SELECT sql FROM sqlite_master WHERE name='{name}'"));
    assert_eq!(rows.len(), 1, "expected one sqlite_master row for {name}");
    match &rows[0][0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// Run a SELECT and return the resulting rows.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result.rows.into_iter().map(|r| r.values.to_vec()).collect()
}

/// Return the `sql` text for the named index from `sqlite_master`.
fn index_sql(db: &Database, index: &str) -> String {
    let rows =
        query(db, &format!("SELECT sql FROM sqlite_master WHERE type='index' AND name='{index}'"));
    assert_eq!(rows.len(), 1, "expected one sqlite_master row for index {index}");
    match &rows[0][0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// Save to the binary `.vbsql` format and reload. Pre-#5877 this is exactly
/// the step that failed after renaming an indexed column.
fn roundtrip_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_5877_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path)
        .expect("load_binary must succeed after RENAME COLUMN of an indexed column (#5877)");
    std::fs::remove_file(&path).ok();
    reloaded
}

#[test]
fn rename_indexed_column_updates_metadata_and_survives_binary_reload() {
    // The exact reproducer from issue #5877.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b)");
    exec(&mut db, "CREATE INDEX i1 ON t1(b)");
    exec(&mut db, "INSERT INTO t1 VALUES(1, 2)");
    exec(&mut db, "ALTER TABLE t1 RENAME b TO z");

    // Catalog metadata now names the new column (drives sqlite_master).
    assert_eq!(index_sql(&db, "i1"), "CREATE INDEX i1 ON t1(z)");
    let meta = db.catalog.find_index_by_name("i1").expect("catalog index");
    assert_eq!(meta.columns[0].column_name(), Some("z"));

    // Storage metadata names the new column (drives persistence/maintenance).
    let storage_meta = db.get_index("i1").expect("storage index");
    match &storage_meta.columns[0] {
        vibesql_ast::IndexColumn::Column { column_name, .. } => assert_eq!(column_name, "z"),
        other => panic!("expected plain column, got {other:?}"),
    }

    // The reproducer's failing step: reopen from a binary snapshot.
    let reloaded = roundtrip_binary(&db, "plain");
    assert_eq!(index_sql(&reloaded, "i1"), "CREATE INDEX i1 ON t1(z)");
    assert_eq!(query(&reloaded, "SELECT z FROM t1"), vec![vec![SqlValue::Integer(2)]]);

    // The index keeps working after the rename + reload.
    let mut reloaded = reloaded;
    exec(&mut reloaded, "INSERT INTO t1 VALUES(3, 4)");
    assert_eq!(query(&reloaded, "SELECT a FROM t1 WHERE z = 4"), vec![vec![SqlValue::Integer(3)]]);
}

#[test]
fn rename_leaves_indexes_on_other_columns_untouched() {
    // altercol.test 1.10: index on (a, c), rename b — index must not change.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, c)");
    exec(&mut db, "CREATE INDEX t1i ON t1(a, c)");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    assert_eq!(index_sql(&db, "t1i"), "CREATE INDEX t1i ON t1(a, c)");
    let reloaded = roundtrip_binary(&db, "untouched");
    assert_eq!(index_sql(&reloaded, "t1i"), "CREATE INDEX t1i ON t1(a, c)");
}

#[test]
fn rename_updates_composite_index_column_list() {
    // altercol.test 1.11: index on (b, c), rename b → d.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, c)");
    exec(&mut db, "CREATE INDEX t1i ON t1(b, c)");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    assert_eq!(index_sql(&db, "t1i"), "CREATE INDEX t1i ON t1(d, c)");
    let reloaded = roundtrip_binary(&db, "composite");
    assert_eq!(index_sql(&reloaded, "t1i"), "CREATE INDEX t1i ON t1(d, c)");
}

#[test]
fn rename_updates_expression_index_and_partial_where() {
    // altercol.test 1.12 shape: expression index with a partial WHERE.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, c)");
    exec(&mut db, "CREATE INDEX t1i ON t1(b+b+b+b, c) WHERE b>0");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    // Catalog: expression AST and WHERE predicate both renamed.
    let meta = db.catalog.find_index_by_name("t1i").expect("catalog index");
    use vibesql_ast::pretty_print::ToSql;
    let expr_sql = meta.columns[0].get_expression().expect("expression column").to_sql();
    assert!(
        expr_sql.contains('d') && !expr_sql.contains('b'),
        "expression not renamed: {expr_sql}"
    );
    let where_sql = meta.where_clause.as_ref().expect("partial predicate").to_sql();
    assert!(where_sql.contains('d') && !where_sql.contains('b'), "WHERE not renamed: {where_sql}");

    // Storage copy: expression AST renamed too. (The storage copy of an
    // expression index does not carry the WHERE predicate — persistence
    // reads it from the catalog — so only the expression is checked here;
    // the storage-side WHERE is covered by the plain-partial test below.)
    let storage_meta = db.get_index("t1i").expect("storage index");
    match &storage_meta.columns[0] {
        vibesql_ast::IndexColumn::Expression { expr, .. } => {
            let sql = expr.to_sql();
            assert!(sql.contains('d') && !sql.contains('b'), "storage expr not renamed: {sql}");
        }
        other => panic!("expected expression column, got {other:?}"),
    }

    // Persisted metadata parses and re-binds against the renamed table.
    let reloaded = roundtrip_binary(&db, "expr_partial");
    let meta = reloaded.catalog.find_index_by_name("t1i").expect("reloaded catalog index");
    let where_sql = meta.where_clause.as_ref().expect("reloaded predicate").to_sql();
    assert!(where_sql.contains('d') && !where_sql.contains('b'));
}

#[test]
fn rename_updates_plain_partial_index_where_in_both_copies() {
    // Plain (non-expression) partial index: the storage copy carries the
    // WHERE predicate (used by DML partial-index maintenance) and must be
    // renamed alongside the catalog copy.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, c)");
    exec(&mut db, "CREATE INDEX t1p ON t1(b) WHERE b>0");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    use vibesql_ast::pretty_print::ToSql;
    let meta = db.catalog.find_index_by_name("t1p").expect("catalog index");
    assert_eq!(meta.columns[0].column_name(), Some("d"));
    let where_sql = meta.where_clause.as_ref().expect("catalog predicate").to_sql();
    assert!(where_sql.contains('d') && !where_sql.contains('b'), "WHERE not renamed: {where_sql}");

    let storage_meta = db.get_index("t1p").expect("storage index");
    let storage_where =
        storage_meta.where_clause.as_ref().expect("storage partial predicate").to_sql();
    assert!(
        storage_where.contains('d') && !storage_where.contains('b'),
        "storage WHERE not renamed: {storage_where}"
    );

    let reloaded = roundtrip_binary(&db, "plain_partial");
    let meta = reloaded.catalog.find_index_by_name("t1p").expect("reloaded catalog index");
    let where_sql = meta.where_clause.as_ref().expect("reloaded predicate").to_sql();
    assert!(where_sql.contains('d') && !where_sql.contains('b'));
}

#[test]
fn rename_updates_unique_autoindex_from_primary_key() {
    // altercol.test 1.7 shape: PRIMARY KEY(b, c) creates an implicit
    // sqlite_autoindex whose column list must follow the rename too.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER, b TEXT, c BLOB, PRIMARY KEY(b, c))");
    exec(&mut db, "INSERT INTO t1 VALUES(1, 2, 3)");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    // Every index on t1 (implicit autoindexes included) must reference `d`,
    // never `b`, in both metadata copies.
    for meta in db.catalog.get_table_indexes("t1") {
        for col in &meta.columns {
            if let Some(name) = col.column_name() {
                assert_ne!(name, "b", "catalog index {} still names old column", meta.name);
            }
        }
    }
    for index_name in db.list_indexes() {
        let meta = db.get_index(&index_name).expect("storage index");
        if !meta.table_name.eq_ignore_ascii_case("t1") {
            continue;
        }
        for col in &meta.columns {
            if let vibesql_ast::IndexColumn::Column { column_name, .. } = col {
                assert_ne!(column_name, "b", "storage index {index_name} still names old column");
            }
        }
    }

    // And the reload must not fail-closed on the autoindex.
    let reloaded = roundtrip_binary(&db, "autoindex");
    assert_eq!(query(&reloaded, "SELECT * FROM t1").len(), 1);
}

// --- Issue #5897: verbatim SQL text rewriting for CHECK/PK/FK clauses, child
// foreign keys, views, and the partial-index WHERE render. ---

#[test]
fn rename_rewrites_own_constraint_refs_and_survives_reload() {
    // altercol.test 1.3/1.7/1.13: the column name inside CHECK/PK/FK clauses in
    // the table's own persisted sql_source must be rewritten. Before #5897 only
    // the definition-position token changed, so a checkpoint reload later failed
    // fail-closed on the stale FK/constraint column ("FK column 'b' ... not
    // found").
    let mut db = Database::new();
    exec(
        &mut db,
        "CREATE TABLE t1(a INTEGER, b TEXT, c BLOB, CHECK(b!=''), PRIMARY KEY(b, c), FOREIGN KEY(b) REFERENCES t2)",
    );
    exec(&mut db, "INSERT INTO t1 VALUES(1, 'x', 2)");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    let expected =
        "CREATE TABLE t1(a INTEGER, d TEXT, c BLOB, CHECK(d!=''), PRIMARY KEY(d, c), FOREIGN KEY(d) REFERENCES t2)";
    assert_eq!(object_sql(&db, "t1"), expected);

    // Constraints rehydrate from the rewritten sql_source on binary reload.
    let reloaded = roundtrip_binary(&db, "constraints");
    assert_eq!(object_sql(&reloaded, "t1"), expected);
    assert_eq!(query(&reloaded, "SELECT d FROM t1"), vec![vec![SqlValue::Varchar("x".into())]]);
}

#[test]
fn rename_preserves_quoted_column_and_renders_partial_index_where() {
    // altercol.test 1.2 (quoted def stays quoted) + 1.12 (partial-index WHERE
    // must appear in the rendered index sql).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER, x TEXT, \"b\" BLOB)");
    exec(&mut db, "CREATE INDEX t1i ON t1(a, x) WHERE a>0");
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");
    // Quoted `"b"` def becomes quoted `"d"` (bQuote rule).
    assert_eq!(object_sql(&db, "t1"), "CREATE TABLE t1(a INTEGER, x TEXT, \"d\" BLOB)");
    // Rename the indexed column and confirm the WHERE clause renders.
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN a TO aa");
    assert_eq!(index_sql(&db, "t1i"), "CREATE INDEX t1i ON t1(aa, x) WHERE aa>0");
}

#[test]
fn rename_parent_column_rewrites_child_foreign_key() {
    // altercol.test 4.1/4.4: renaming a PARENT column rewrites the child's
    // REFERENCES parent(col_list) text and the FK metadata, surviving reload.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE p1(c, d, PRIMARY KEY(c, d))");
    exec(&mut db, "CREATE TABLE c1(a, b, FOREIGN KEY (a, b) REFERENCES p1(c, d))");
    exec(&mut db, "ALTER TABLE p1 RENAME d TO reasonable");

    assert_eq!(object_sql(&db, "p1"), "CREATE TABLE p1(c, reasonable, PRIMARY KEY(c, reasonable))");
    assert_eq!(
        object_sql(&db, "c1"),
        "CREATE TABLE c1(a, b, FOREIGN KEY (a, b) REFERENCES p1(c, reasonable))"
    );

    // In-memory FK metadata now names the new parent column.
    let c1 = db.get_table("c1").expect("c1");
    assert!(
        c1.schema.foreign_keys[0].parent_column_names.iter().any(|c| c == "reasonable"),
        "child FK parent_column_names should be updated"
    );

    // Round-trip: both tables rehydrate from the rewritten sql_source.
    let reloaded = roundtrip_binary(&db, "childfk");
    assert_eq!(
        object_sql(&reloaded, "c1"),
        "CREATE TABLE c1(a, b, FOREIGN KEY (a, b) REFERENCES p1(c, reasonable))"
    );
}

#[test]
fn rename_rewrites_dependent_view_text_and_query() {
    // altercol.test 8.1/8.5: a view referencing the renamed column has both its
    // sqlite_master text and its executable query AST rewritten.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE a1(x INTEGER, y TEXT, z BLOB, PRIMARY KEY(x))");
    exec(&mut db, "INSERT INTO a1 VALUES(1, 'hi', 2)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT x, y, z FROM a1");
    exec(&mut db, "ALTER TABLE a1 RENAME y TO yyy");

    // sqlite_master text is rewritten (and the trailing `;` stripped).
    assert_eq!(object_sql(&db, "v1"), "CREATE VIEW v1 AS SELECT x, yyy, z FROM a1");
    // The view still executes: its stored query AST now names the new column.
    assert_eq!(
        query(&db, "SELECT z FROM v1 WHERE yyy = 'hi'"),
        vec![vec![SqlValue::Integer(2)]]
    );
}
