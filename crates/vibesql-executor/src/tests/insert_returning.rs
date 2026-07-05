//! Regression tests for issue #5263: INSERT ... RETURNING (SQLite 3.35.0+).
//!
//! SQLite semantics: RETURNING yields one result row per inserted row,
//! evaluated against the NEW row values as ACTUALLY inserted (defaults,
//! generated columns, and auto INTEGER PRIMARY KEY materialized; REPLACE
//! rowid rewrites included). Rows skipped by `OR IGNORE` / `ON CONFLICT DO
//! NOTHING` are omitted; `ON DUPLICATE KEY UPDATE` contributes the
//! post-UPDATE row. The bulk-transfer fast path for `INSERT INTO t SELECT`
//! is gated when a RETURNING clause is present.
//!
//! Modeled on `delete_returning.rs` (#5262) / `update_returning.rs` (#5260).

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use super::super::*;

fn execute_sql(db: &mut vibesql_storage::Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            vec![]
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
            vec![]
        }
        Statement::CreateView(s) => {
            crate::advanced_objects::execute_create_view(&s, db).expect("CREATE VIEW failed");
            vec![]
        }
        Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).expect("CREATE TRIGGER failed");
            vec![]
        }
        Statement::Select(s) => SelectExecutor::new(db).execute(&s).expect("SELECT failed"),
        other => panic!("Unsupported statement in test helper: {:?}", other),
    }
}

/// Run an INSERT and return (count, RETURNING result).
fn execute_insert_returning(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> (usize, Option<crate::select::SelectResult>) {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Insert(s) => {
            let outcome = InsertExecutor::execute_returning(db, &s).expect("INSERT failed");
            (outcome.affected_rows, outcome.returning)
        }
        other => panic!("Expected INSERT, got {:?}", other),
    }
}

fn int_rows(result: &crate::select::SelectResult) -> Vec<Vec<i64>> {
    result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|v| match v {
                    SqlValue::Integer(n) => *n,
                    SqlValue::Bigint(n) => *n,
                    SqlValue::Smallint(n) => *n as i64,
                    other => panic!("Expected integer value, got {:?}", other),
                })
                .collect()
        })
        .collect()
}

fn string_value(v: &SqlValue) -> String {
    match v {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => panic!("Expected string value, got {:?}", other),
    }
}

#[test]
fn test_insert_returning_new_values() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT, b INT)");

    let (count, returning) =
        execute_insert_returning(&mut db, "INSERT INTO t1 VALUES (1, 10) RETURNING *, a + 1");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["a".to_string(), "b".to_string(), "a+1".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![1, 10, 2]]);

    // The row is actually inserted.
    let rows = execute_sql(&mut db, "SELECT a FROM t1");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_insert_returning_multi_row_values_in_order() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT, b INT)");

    // Multi-row VALUES takes the batch insert path; one result row per
    // inserted row, in insertion order.
    let (count, returning) = execute_insert_returning(
        &mut db,
        "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30) RETURNING b",
    );
    assert_eq!(count, 3);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["b".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![10], vec![20], vec![30]]);
}

#[test]
fn test_insert_returning_auto_ipk_and_defaults() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c INT DEFAULT 7)");

    // SQLite: RETURNING projects the NEW row with the auto-assigned INTEGER
    // PRIMARY KEY and DEFAULT values materialized.
    let (count, returning) =
        execute_insert_returning(&mut db, "INSERT INTO t1(b) VALUES ('x') RETURNING a, b, c");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    assert_eq!(returning.rows.len(), 1);
    assert_eq!(returning.rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(string_value(&returning.rows[0].values[1]), "x");
    assert_eq!(returning.rows[0].values[2], SqlValue::Integer(7));

    // Second insert auto-assigns the next IPK.
    let (_, returning) =
        execute_insert_returning(&mut db, "INSERT INTO t1(b) VALUES ('y') RETURNING a");
    let returning = returning.unwrap();
    assert_eq!(int_rows(&returning), vec![vec![2]]);
}

#[test]
fn test_insert_returning_default_values() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT DEFAULT 5, b INT DEFAULT 6)");

    let (count, returning) =
        execute_insert_returning(&mut db, "INSERT INTO t1 DEFAULT VALUES RETURNING *");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![5, 6]]);
}

#[test]
fn test_insert_select_returning_gates_bulk_transfer() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE src(a INT, b INT)");
    execute_sql(&mut db, "CREATE TABLE dst(a INT, b INT)");
    execute_sql(&mut db, "INSERT INTO src VALUES (1, 10), (2, 20)");

    // INSERT INTO dst SELECT (no column list) is bulk-transfer eligible;
    // the fast path must be gated so the NEW rows can be projected.
    let (count, returning) =
        execute_insert_returning(&mut db, "INSERT INTO dst SELECT * FROM src RETURNING a, b");
    assert_eq!(count, 2);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![1, 10], vec![2, 20]]);

    let rows = execute_sql(&mut db, "SELECT a FROM dst");
    assert_eq!(rows.len(), 2, "rows must still be inserted");
}

#[test]
fn test_insert_or_ignore_returning_skips_conflicting_rows() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10)");

    // SQLite: rows skipped by OR IGNORE do not appear in RETURNING output.
    let (count, returning) = execute_insert_returning(
        &mut db,
        "INSERT OR IGNORE INTO t1 VALUES (1, 99), (2, 20), (3, 30) RETURNING a",
    );
    assert_eq!(count, 2, "conflicting row must be skipped");
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![2], vec![3]]);
}

#[test]
fn test_insert_or_ignore_returning_all_skipped_is_empty_with_headers() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10)");

    let (count, returning) =
        execute_insert_returning(&mut db, "INSERT OR IGNORE INTO t1 VALUES (1, 99) RETURNING a, b");
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should still produce a (empty) result");
    assert_eq!(returning.columns, vec!["a".to_string(), "b".to_string()]);
    assert!(returning.rows.is_empty(), "no rows inserted, no rows returned");
}

#[test]
fn test_insert_on_conflict_do_nothing_returning_empty() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10)");

    // ON CONFLICT DO NOTHING: the conflicting row returns nothing.
    let (count, returning) = execute_insert_returning(
        &mut db,
        "INSERT INTO t1 VALUES (1, 99) ON CONFLICT(a) DO NOTHING RETURNING *",
    );
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should still produce a (empty) result");
    assert!(returning.rows.is_empty());
}

#[test]
fn test_insert_on_duplicate_key_update_returning_post_update_row() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10)");

    // MySQL-style upsert: the update arm fires for the conflicting row and
    // RETURNING projects the post-UPDATE row.
    let (count, returning) = execute_insert_returning(
        &mut db,
        "INSERT INTO t1 VALUES (1, 99) ON DUPLICATE KEY UPDATE b = 42 RETURNING a, b",
    );
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![1, 42]]);
}

#[test]
fn test_replace_into_returning_new_row() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10)");

    // SQLite: REPLACE returns the newly inserted row.
    let (count, returning) =
        execute_insert_returning(&mut db, "REPLACE INTO t1 VALUES (1, 99) RETURNING a, b");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![1, 99]]);

    // Old row is gone, new row in place.
    let rows = execute_sql(&mut db, "SELECT b FROM t1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(99));
}

#[test]
fn test_insert_or_replace_returning_new_row() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10)");

    let (count, returning) =
        execute_insert_returning(&mut db, "INSERT OR REPLACE INTO t1 VALUES (1, 77) RETURNING b");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![77]]);
}

#[test]
fn test_insert_returning_expression_with_alias() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT, b INT)");

    let (count, returning) = execute_insert_returning(
        &mut db,
        "INSERT INTO t1 VALUES (3, 4) RETURNING a + b AS total, a * b product",
    );
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["total".to_string(), "product".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![7, 12]]);
}

#[test]
fn test_insert_without_returning_yields_none() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");

    let (count, returning) = execute_insert_returning(&mut db, "INSERT INTO t1 VALUES (1)");
    assert_eq!(count, 1);
    assert!(returning.is_none(), "no RETURNING clause, no result expected");
}

#[test]
fn test_insert_returning_through_instead_of_trigger() {
    // Issue #5272: view path parity with delete_returning.rs. INSERT into a
    // view with an INSTEAD OF INSERT trigger projects RETURNING against the
    // NEW view rows, one result row per trigger fire.
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE base(a INT, b TEXT)");
    execute_sql(&mut db, "CREATE VIEW v1 AS SELECT a, b FROM base");
    execute_sql(
        &mut db,
        "CREATE TRIGGER v1_tr INSTEAD OF INSERT ON v1 \
         BEGIN INSERT INTO base VALUES(NEW.a, NEW.b); END",
    );

    let (count, returning) = execute_insert_returning(
        &mut db,
        "INSERT INTO v1 VALUES (1, 'x'), (2, 'y') RETURNING a, b",
    );
    // changes() after DML on a view is always 0 (SQLite R-09813-48563, #5840);
    // the two INSTEAD OF trigger fires are verified via RETURNING below.
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(returning.rows.len(), 2, "one RETURNING row per INSTEAD OF trigger fire");
    assert_eq!(returning.rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(string_value(&returning.rows[0].values[1]), "x");
    assert_eq!(returning.rows[1].values[0], SqlValue::Integer(2));
    assert_eq!(string_value(&returning.rows[1].values[1]), "y");

    // The trigger actually inserted both rows into the base table.
    let rows = execute_sql(&mut db, "SELECT a FROM base");
    assert_eq!(rows.len(), 2, "INSTEAD OF trigger should fire once per inserted view row");
}

#[test]
fn test_insert_returning_count_reflects_affected_rows_not_projection() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");

    // The affected-row count (used for changes()) is independent of the
    // RETURNING projection.
    let (count, returning) =
        execute_insert_returning(&mut db, "INSERT INTO t1 VALUES (1), (2) RETURNING a");
    assert_eq!(count, 2);
    assert_eq!(returning.unwrap().rows.len(), 2);
}
