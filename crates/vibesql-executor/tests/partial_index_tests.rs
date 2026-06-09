//! Tests for partial indexes (`CREATE INDEX ... WHERE predicate`).
//!
//! These tests verify the storage-level invariant that the index body only
//! contains rows whose WHERE predicate is truthy. See issue #5214.

use std::collections::BTreeMap;

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::{database::indexes::IndexData, Database};
use vibesql_types::SqlValue;

/// Helper to execute one or more SQL statements separated by ';'.
fn execute_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        execute_statement(&stmt, db);
    }
}

fn execute_statement(stmt: &vibesql_ast::Statement, db: &mut Database) {
    use vibesql_ast::Statement;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(s, db).expect("CREATE INDEX failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(s, db).expect("UPDATE failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(s, db).expect("DELETE failed");
        }
        _ => panic!("Unsupported statement type"),
    }
}

/// Collect the contents of an index body as a sorted (key, row_indices) map.
///
/// This bypasses the planner (which conservatively skips partial indexes)
/// and inspects the storage layer's index body directly so the test can
/// observe which rows are actually in the index.
fn index_body(db: &Database, index_name: &str) -> BTreeMap<Vec<SqlValue>, Vec<usize>> {
    let data = db.get_index_data(index_name).expect("index not found");
    match data {
        IndexData::InMemory { data, .. } => {
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        }
        _ => panic!("expected InMemory index body for this test"),
    }
}

/// Returns the row indices currently stored in the partial index, sorted.
fn index_row_indices(db: &Database, index_name: &str) -> Vec<usize> {
    let mut all: Vec<usize> = index_body(db, index_name).into_values().flatten().collect();
    all.sort_unstable();
    all
}

#[test]
fn create_partial_index_excludes_non_matching_rows_at_build_time() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        INSERT INTO orders VALUES (1, 0);
        INSERT INTO orders VALUES (2, 1);
        INSERT INTO orders VALUES (3, 1);
        INSERT INTO orders VALUES (4, 0);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    // Only rows with status = 1 (row indices 1 and 2) should be in the index.
    let entries = index_row_indices(&db, "idx_open");
    assert_eq!(entries, vec![1, 2], "partial index body should exclude non-matching rows");
}

#[test]
fn insert_into_partial_index_evaluates_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    // Empty so far.
    assert!(index_body(&db, "idx_open").is_empty());

    execute_sql(
        &mut db,
        r#"
        INSERT INTO orders VALUES (1, 0);
        INSERT INTO orders VALUES (2, 1);
        INSERT INTO orders VALUES (3, 0);
        INSERT INTO orders VALUES (4, 1);
        "#,
    );

    let entries = index_row_indices(&db, "idx_open");
    assert_eq!(entries, vec![1, 3], "only status=1 rows should be in the partial index");
}

#[test]
fn update_partial_index_handles_predicate_transition() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        INSERT INTO orders VALUES (1, 0);
        INSERT INTO orders VALUES (2, 1);
        INSERT INTO orders VALUES (3, 0);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    // Initially only row #1 (id=2) is in the index.
    assert_eq!(index_row_indices(&db, "idx_open"), vec![1]);

    // Out -> In: change status to 1 for id=1.
    execute_sql(&mut db, "UPDATE orders SET status = 1 WHERE id = 1");
    assert_eq!(index_row_indices(&db, "idx_open"), vec![0, 1]);

    // In -> Out: change status back to 0 for id=2.
    execute_sql(&mut db, "UPDATE orders SET status = 0 WHERE id = 2");
    assert_eq!(index_row_indices(&db, "idx_open"), vec![0]);

    // In -> In, key change: change id of an included row.
    execute_sql(&mut db, "UPDATE orders SET id = 99 WHERE id = 1");
    let body = index_body(&db, "idx_open");
    assert_eq!(body.len(), 1);
    let only_key = body.keys().next().unwrap();
    // The retained key should be 99, not 1 — the storage normalizes
    // integers to `Double` for consistent comparison, so accept either form.
    let key_is_99 = match only_key.first() {
        Some(SqlValue::Integer(n)) => *n == 99,
        Some(SqlValue::Bigint(n)) => *n == 99,
        Some(SqlValue::Smallint(n)) => *n == 99,
        Some(SqlValue::Double(n)) => (*n - 99.0).abs() < 1e-9,
        Some(SqlValue::Real(n)) => (*n - 99.0).abs() < 1e-9,
        Some(SqlValue::Float(n)) => (*n - 99.0).abs() < 1e-9,
        _ => false,
    };
    assert!(
        key_is_99,
        "after key change, partial-index entry should be re-keyed to 99 (saw {:?})",
        only_key
    );
}

#[test]
fn delete_removes_partial_index_entry_when_predicate_was_truthy() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        INSERT INTO orders VALUES (1, 1);
        INSERT INTO orders VALUES (2, 0);
        INSERT INTO orders VALUES (3, 1);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    assert_eq!(index_row_indices(&db, "idx_open"), vec![0, 2]);

    // Delete a row that was IN the index.
    execute_sql(&mut db, "DELETE FROM orders WHERE id = 1");
    assert_eq!(
        index_row_indices(&db, "idx_open"),
        vec![2],
        "deleting an included row should remove its index entry"
    );

    // Delete a row that was NOT in the index — index body must be unchanged.
    execute_sql(&mut db, "DELETE FROM orders WHERE id = 2");
    assert_eq!(
        index_row_indices(&db, "idx_open"),
        vec![2],
        "deleting an excluded row must not touch the partial index body"
    );
}

#[test]
fn partial_unique_index_allows_duplicates_outside_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER, sku INTEGER);
        CREATE UNIQUE INDEX idx_open_sku ON orders(sku) WHERE status = 1;
        INSERT INTO orders VALUES (1, 0, 42);
        INSERT INTO orders VALUES (2, 0, 42);
        "#,
    );

    // Two status=0 rows with sku=42 are allowed because neither satisfies
    // the WHERE predicate; the partial UNIQUE index does not enforce
    // uniqueness over them.
    let table = db.get_table("orders").expect("table missing");
    assert_eq!(table.row_count(), 2);
    // Neither row should be in the index body.
    assert!(
        index_body(&db, "idx_open_sku").is_empty(),
        "partial index must be empty when no row matches the predicate"
    );
}

#[test]
fn partial_unique_index_rejects_duplicates_inside_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER, sku INTEGER);
        CREATE UNIQUE INDEX idx_open_sku ON orders(sku) WHERE status = 1;
        INSERT INTO orders VALUES (1, 1, 42);
        "#,
    );

    // The second insert satisfies the predicate AND collides on sku=42.
    let stmt = Parser::parse_sql("INSERT INTO orders VALUES (2, 1, 42)").unwrap();
    let result = match &stmt {
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(&mut db, s),
        _ => unreachable!(),
    };
    assert!(
        result.is_err(),
        "partial UNIQUE index should reject duplicate keys within the predicate"
    );

    // The original row is still there; the conflicting one was not added.
    let table = db.get_table("orders").expect("table missing");
    assert_eq!(table.row_count(), 1);
}

#[test]
fn partial_index_does_not_index_predicate_falsy_rows_on_insert() {
    // Regression test: before issue #5214 the partial index body was
    // populated with every row regardless of the predicate.
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER);
        CREATE INDEX idx_flag ON t(id) WHERE flag = 1;
        INSERT INTO t VALUES (1, 0);
        INSERT INTO t VALUES (2, 1);
        INSERT INTO t VALUES (3, NULL);
        "#,
    );

    let entries = index_row_indices(&db, "idx_flag");
    assert_eq!(
        entries,
        vec![1],
        "only the row whose predicate evaluated to truthy should be indexed; \
         NULL (which is not truthy) and 0 (which is not equal to 1) must be excluded"
    );
}

#[test]
fn batch_insert_into_partial_index_evaluates_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER);
        CREATE INDEX idx_flag ON t(id) WHERE flag = 1;
        "#,
    );

    // Batch insert path (multiple rows, no triggers).
    execute_sql(
        &mut db,
        "INSERT INTO t VALUES (1, 1), (2, 0), (3, 1), (4, 0), (5, 1)",
    );

    let entries = index_row_indices(&db, "idx_flag");
    // Row indices 0 (id=1), 2 (id=3), 4 (id=5) have flag=1.
    assert_eq!(entries, vec![0, 2, 4]);
}
