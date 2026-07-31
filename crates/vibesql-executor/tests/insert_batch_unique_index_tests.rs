//! Regression tests for issue #6346: intra-statement (in-batch) UNIQUE
//! duplicate detection for uniqueness enforced through unique INDEX metadata.
//!
//! Multi-row INSERT validates every row before inserting any, so the stored
//! index bodies cannot see earlier rows of the same statement. Before the
//! fix, uniqueness backed solely by unique-index metadata (a user-defined
//! `CREATE UNIQUE INDEX`, or the implicit `sqlite_autoindex_*` of a UNIQUE
//! column on a reopened/WAL-recovered table whose schema-level
//! `unique_constraints` were lost on reload) had no in-batch tracking:
//! `INSERT ... VALUES(1),(1)` silently wrote both rows, and
//! `INSERT OR IGNORE` inserted two rows instead of one.
//!
//! Also covers the reopen path itself: `TableSchema::unique_constraints` is
//! now rehydrated from the persisted `sql_source` on binary reload.

use vibesql_ast::Statement;
use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Create a table preserving the verbatim source text (issue #5619), the way
/// the CLI/load paths capture it. `sql_source` is what the reload path
/// re-parses, so reopen tests must go through this entry point.
fn create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

fn create_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE INDEX");
    let Statement::CreateIndex(create) = stmt else {
        panic!("expected CREATE INDEX");
    };
    CreateIndexExecutor::execute(&create, db).expect("CREATE INDEX");
}

/// Execute an INSERT, returning Ok(()) or the error's Display text.
fn insert(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {e:?}"))?;
    let Statement::Insert(s) = stmt else {
        panic!("expected INSERT");
    };
    InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
}

/// COUNT(*) helper.
fn count(db: &Database, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let stmt = Parser::parse_sql(&sql).expect("parse SELECT");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    match &result.rows[0].values[0] {
        vibesql_types::SqlValue::Integer(n) => *n,
        vibesql_types::SqlValue::Bigint(n) => *n,
        other => panic!("unexpected COUNT value: {other:?}"),
    }
}

/// Save to a binary `.vbsql` file and reload — the cross-process reopen path
/// from the issue's repro.
fn reopen_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_6346_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded
}

fn assert_unique_violation(err: &str) {
    assert!(
        err.contains("UNIQUE constraint failed"),
        "expected UNIQUE violation wording, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Fresh process + CREATE UNIQUE INDEX (no reopen needed — the same hole)
// ---------------------------------------------------------------------------

#[test]
fn fresh_unique_index_multi_row_insert_aborts() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(c)");

    // Default ABORT: the whole statement rolls back, nothing persisted.
    let err = insert(&mut db, "INSERT INTO t VALUES(1),(1)")
        .expect_err("in-batch duplicate must fail");
    assert_unique_violation(&err);
    assert_eq!(count(&db, "t"), 0, "ABORT must persist no rows");
}

#[test]
fn fresh_unique_index_or_fail_keeps_first_row() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(c)");

    // OR FAIL: stop at the offending row but keep the rows before it.
    let err = insert(&mut db, "INSERT OR FAIL INTO t VALUES(1),(1)")
        .expect_err("in-batch duplicate must fail");
    assert_unique_violation(&err);
    assert_eq!(count(&db, "t"), 1, "OR FAIL keeps the first row only");
}

#[test]
fn fresh_unique_index_or_ignore_inserts_exactly_one() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(c)");

    // OR IGNORE: the second row is skipped, not an error.
    insert(&mut db, "INSERT OR IGNORE INTO t VALUES(1),(1)").expect("OR IGNORE never errors");
    assert_eq!(count(&db, "t"), 1, "OR IGNORE inserts exactly one row");
}

#[test]
fn fresh_unique_index_numeric_variants_collide() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(c)");

    // 1 and 1.0 are equal for uniqueness (index keys are normalized), so the
    // in-batch working set must normalize the same way.
    let err = insert(&mut db, "INSERT INTO t VALUES(1),(1.0)")
        .expect_err("1 and 1.0 must collide on a unique index");
    assert_unique_violation(&err);
    assert_eq!(count(&db, "t"), 0);
}

#[test]
fn fresh_unique_index_multiple_nulls_allowed() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(c)");

    insert(&mut db, "INSERT INTO t VALUES(NULL),(NULL)")
        .expect("multiple NULLs never conflict on a unique index");
    assert_eq!(count(&db, "t"), 2);
}

#[test]
fn fresh_composite_unique_index_in_batch() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(a, b)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(a, b)");

    // Distinct composite keys pass...
    insert(&mut db, "INSERT INTO t VALUES(1,2),(1,3)").expect("distinct composite keys");
    assert_eq!(count(&db, "t"), 2);

    // ...identical composite keys in one statement fail.
    let err = insert(&mut db, "INSERT INTO t VALUES(4,5),(4,5)")
        .expect_err("identical composite keys must fail");
    assert_unique_violation(&err);
    assert_eq!(count(&db, "t"), 2);
}

#[test]
fn fresh_partial_unique_index_in_batch() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(c) WHERE c > 0");

    // Rows outside the predicate never enter the index: duplicates allowed.
    insert(&mut db, "INSERT INTO t VALUES(-1),(-1)")
        .expect("rows outside the partial predicate never conflict");
    assert_eq!(count(&db, "t"), 2);

    // Rows inside the predicate conflict in-batch.
    let err = insert(&mut db, "INSERT INTO t VALUES(5),(5)")
        .expect_err("in-predicate duplicate must fail");
    assert_unique_violation(&err);
    assert_eq!(count(&db, "t"), 2);
}

#[test]
fn fresh_unique_expression_index_in_batch() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(a, b)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(a+b)");

    // (1,2) and (0,3) both evaluate a+b = 3 — must collide in one statement.
    let err = insert(&mut db, "INSERT INTO t VALUES(1,2),(0,3)")
        .expect_err("equal expression keys must fail in-batch");
    assert!(
        err.contains("UNIQUE constraint failed: index 'i'"),
        "expected expression-index violation wording, got: {err}"
    );
    assert_eq!(count(&db, "t"), 0);

    // Distinct expression keys pass.
    insert(&mut db, "INSERT INTO t VALUES(1,2),(1,3)").expect("distinct expression keys");
    assert_eq!(count(&db, "t"), 2);
}

// ---------------------------------------------------------------------------
// Reopened (binary save/load) table with a UNIQUE column — the filed repro
// ---------------------------------------------------------------------------

#[test]
fn reopened_schema_rehydrates_unique_constraints() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c UNIQUE)");
    assert_eq!(
        db.catalog.get_table("t").expect("t").unique_constraints,
        vec![vec!["c".to_string()]],
        "precondition: fresh schema carries the UNIQUE constraint"
    );

    let db2 = reopen_binary(&db, "rehydrate");
    assert_eq!(
        db2.catalog.get_table("t").expect("t").unique_constraints,
        vec![vec!["c".to_string()]],
        "unique_constraints must be rehydrated from sql_source on reload"
    );
}

#[test]
fn reopened_unique_column_or_fail_keeps_first_row() {
    // The exact repro from issue #6346.
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c UNIQUE)");
    let mut db2 = reopen_binary(&db, "or_fail");

    let err = insert(&mut db2, "INSERT OR FAIL INTO t VALUES(1),(1)")
        .expect_err("in-batch duplicate must fail after reopen");
    assert_unique_violation(&err);
    assert_eq!(count(&db2, "t"), 1, "OR FAIL keeps the first row only");
}

#[test]
fn reopened_unique_column_plain_insert_aborts() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c UNIQUE)");
    let mut db2 = reopen_binary(&db, "abort");

    let err = insert(&mut db2, "INSERT INTO t VALUES(1),(1)")
        .expect_err("in-batch duplicate must fail after reopen");
    assert_unique_violation(&err);
    assert_eq!(count(&db2, "t"), 0, "default ABORT persists no rows");
}

#[test]
fn reopened_unique_column_or_ignore_inserts_exactly_one() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c UNIQUE)");
    let mut db2 = reopen_binary(&db, "or_ignore");

    insert(&mut db2, "INSERT OR IGNORE INTO t VALUES(1),(1)").expect("OR IGNORE never errors");
    assert_eq!(count(&db2, "t"), 1, "OR IGNORE inserts exactly one row");
}

#[test]
fn reopened_unique_column_cross_statement_detection_unchanged() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c UNIQUE)");
    let mut db2 = reopen_binary(&db, "cross_stmt");

    insert(&mut db2, "INSERT INTO t VALUES(1)").expect("first insert");
    let err = insert(&mut db2, "INSERT INTO t VALUES(1)")
        .expect_err("cross-statement duplicate must still fail");
    assert_unique_violation(&err);
    assert_eq!(count(&db2, "t"), 1);
}

#[test]
fn reopened_unique_index_multi_row_insert_aborts() {
    // Fresh-process CREATE UNIQUE INDEX, then reopen: the user-defined index
    // metadata is serialized, and the in-batch working set must cover it too.
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(c)");
    create_index(&mut db, "CREATE UNIQUE INDEX i ON t(c)");
    let mut db2 = reopen_binary(&db, "user_index");

    let err = insert(&mut db2, "INSERT INTO t VALUES(1),(1)")
        .expect_err("in-batch duplicate must fail after reopen");
    assert_unique_violation(&err);
    assert_eq!(count(&db2, "t"), 0);
}

#[test]
fn reopened_table_level_unique_composite_in_batch() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t(a, b, UNIQUE(a, b))");
    let mut db2 = reopen_binary(&db, "composite");

    insert(&mut db2, "INSERT INTO t VALUES(1,2),(1,3)").expect("distinct composite keys");
    let err = insert(&mut db2, "INSERT INTO t VALUES(4,5),(4,5)")
        .expect_err("identical composite keys must fail after reopen");
    assert_unique_violation(&err);
    assert_eq!(count(&db2, "t"), 2);
}
