//! Regression tests for issue #5568: an index created in the **same** statement
//! batch as its table must be persisted to the SQL dump and recreated on reload.
//!
//! Background: the original symptom was that creating a table and an index on it
//! in one CLI `-c` invocation produced a dump whose `-- Indexes` section was
//! empty (so the index was silently lost on reload), while creating them in two
//! separate invocations persisted the index. The root cause was the dump writer
//! emitting the index DDL with **unquoted** identifiers (fixed in #5567): an
//! index named `x1 (b Asc, c Asc)` on table `t` was written as
//! `CREATE INDEX x1 (b asc, c asc) ON t (...)`, which is not a valid CREATE
//! INDEX statement and so did not survive the round-trip. There was never a
//! same-batch *registration* problem — `Database::list_indexes()` returns the
//! index regardless of when it was created relative to the table.
//!
//! These tests pin the behaviour end-to-end: create table + index in the same
//! batch through the executor (the path the CLI uses), `save_sql_dump`, replay
//! the dump into a fresh database, and assert the index is present **and usable**
//! by a SELECT. They also cover multiple indexes in one batch and an index
//! created in a later, separate batch (no regression).

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{persistence::load::read_sql_dump, Database};
use vibesql_types::SqlValue;

/// Execute one or more `;`-separated non-SELECT statements through the executor,
/// mirroring how the CLI applies a `-c "...; ..."` batch. Comment-only and blank
/// fragments (as found in a saved dump) are skipped.
fn exec_batch(db: &mut Database, sql: &str) {
    for fragment in sql.split(';') {
        let trimmed = strip_dump_comments(fragment);
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(&trimmed)
            .unwrap_or_else(|e| panic!("Failed to parse `{trimmed}`: {e}"));
        match stmt {
            vibesql_ast::Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            vibesql_ast::Statement::CreateIndex(s) => {
                CreateIndexExecutor::execute(&s, db).expect("CREATE INDEX failed");
            }
            vibesql_ast::Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            // The dump may contain schema/role/view sections we don't exercise
            // here; ignore anything else so replay stays focused on table+index.
            _ => {}
        }
    }
}

/// Remove leading whole-line `--` comments (and blank lines) from a dump
/// fragment, returning the remaining SQL text trimmed.
fn strip_dump_comments(fragment: &str) -> String {
    fragment
        .lines()
        .filter(|line| {
            let l = line.trim_start();
            !l.is_empty() && !l.starts_with("--")
        })
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

/// Run a SELECT and return the first column of every row as sorted i64s.
fn select_ints(db: &Database, sql: &str) -> Vec<i64> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT");
    };
    let executor = SelectExecutor::new(db);
    let rows = executor.execute(&select_stmt).expect("SELECT failed");
    let mut out: Vec<i64> = rows
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

/// Save `db` to a unique temp path, replay the dump into a fresh database, and
/// return the reloaded database. The temp file is removed before returning.
fn save_and_reload(db: &Database, tag: &str) -> Database {
    let path = std::env::temp_dir().join(format!(
        "vibesql_5568_{tag}_{}_{}.sql",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    db.save_sql_dump(&path).expect("save_sql_dump failed");
    let dump = read_sql_dump(&path).expect("read_sql_dump failed");
    std::fs::remove_file(&path).ok();

    let mut reloaded = Database::new();
    exec_batch(&mut reloaded, &dump);
    reloaded
}

/// Same-batch table + index → save → reload: index present and usable.
#[test]
fn same_batch_index_persists_and_is_usable_after_reload() {
    let mut db = Database::new();
    // Table and index created in the SAME batch — the exact #5568 scenario.
    exec_batch(
        &mut db,
        "CREATE TABLE t (a INTEGER, b INTEGER); \
         CREATE INDEX ix ON t (b); \
         INSERT INTO t VALUES (1, 10); \
         INSERT INTO t VALUES (2, 20); \
         INSERT INTO t VALUES (3, 20);",
    );

    // Sanity: the index is registered before save.
    assert!(
        db.list_indexes().iter().any(|i| i.eq_ignore_ascii_case("ix")),
        "index should be registered in the source database"
    );

    let reloaded = save_and_reload(&db, "same_batch");

    // The index must survive the round-trip.
    assert!(
        reloaded.list_indexes().iter().any(|i| i.eq_ignore_ascii_case("ix")),
        "same-batch index was dropped from the dump; reloaded indexes: {:?}",
        reloaded.list_indexes()
    );
    // ...attached to the correct table...
    assert!(
        reloaded.list_indexes_for_table("t").iter().any(|i| i.eq_ignore_ascii_case("ix")),
        "reloaded index not associated with table t"
    );
    // ...and usable: a predicate on the indexed column returns the right rows.
    assert_eq!(select_ints(&reloaded, "SELECT a FROM t WHERE b = 20"), vec![2, 3]);
    assert_eq!(select_ints(&reloaded, "SELECT a FROM t WHERE b = 10"), vec![1]);
}

/// Same-batch index whose name contains spaces/parens (the literal #5568 repro)
/// must round-trip via quoted identifiers (guards against the #5567 regression
/// from the same-batch entry point).
#[test]
fn same_batch_quoted_index_name_roundtrips() {
    let mut db = Database::new();
    exec_batch(
        &mut db,
        "CREATE TABLE t (c INTEGER, b INTEGER); \
         CREATE INDEX \"x1 (b Asc, c Asc)\" ON t (b DESC, c); \
         INSERT INTO t VALUES (7, 1); \
         INSERT INTO t VALUES (8, 2);",
    );

    let reloaded = save_and_reload(&db, "quoted");

    assert!(
        reloaded.list_indexes().iter().any(|i| i.eq_ignore_ascii_case("x1 (b asc, c asc)")),
        "quoted same-batch index lost on reload; indexes: {:?}",
        reloaded.list_indexes()
    );
    assert_eq!(select_ints(&reloaded, "SELECT c FROM t WHERE b = 2"), vec![8]);
}

/// A table with multiple indexes created in one batch: all must persist.
#[test]
fn multiple_same_batch_indexes_all_persist() {
    let mut db = Database::new();
    exec_batch(
        &mut db,
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER); \
         CREATE INDEX i_a ON t (a); \
         CREATE INDEX i_bc ON t (b, c); \
         INSERT INTO t VALUES (1, 5, 9); \
         INSERT INTO t VALUES (2, 5, 8);",
    );

    let reloaded = save_and_reload(&db, "multi");

    let names: Vec<String> = reloaded.list_indexes().iter().map(|s| s.to_lowercase()).collect();
    assert!(names.iter().any(|n| n == "i_a"), "i_a missing: {names:?}");
    assert!(names.iter().any(|n| n == "i_bc"), "i_bc missing: {names:?}");
    assert_eq!(select_ints(&reloaded, "SELECT a FROM t WHERE b = 5 AND c = 8"), vec![2]);
    assert_eq!(select_ints(&reloaded, "SELECT b FROM t WHERE a = 1"), vec![5]);
}

/// No regression for the historically-working path: an index created in a
/// *separate* batch after the table must still persist on reload.
#[test]
fn separate_batch_index_still_persists() {
    let mut db = Database::new();
    // Batch 1: just the table + data.
    exec_batch(
        &mut db,
        "CREATE TABLE t (a INTEGER, b INTEGER); \
         INSERT INTO t VALUES (1, 100); \
         INSERT INTO t VALUES (2, 200);",
    );
    // Batch 2: the index, created later.
    exec_batch(&mut db, "CREATE INDEX ix_later ON t (b);");

    let reloaded = save_and_reload(&db, "separate");

    assert!(
        reloaded.list_indexes().iter().any(|i| i.eq_ignore_ascii_case("ix_later")),
        "separately-created index lost on reload; indexes: {:?}",
        reloaded.list_indexes()
    );
    assert_eq!(select_ints(&reloaded, "SELECT a FROM t WHERE b = 200"), vec![2]);
}
