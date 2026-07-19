//! Tests for WITHOUT ROWID implicit PRIMARY KEY row ordering (Issue #6171)
//!
//! For a `WITHOUT ROWID` table the PRIMARY KEY *is* the table's clustering
//! btree, so SQLite returns rows from a full table scan in ascending PRIMARY
//! KEY order when no `ORDER BY` is specified — not insertion order (which is
//! what an ordinary rowid table yields). Composite keys sort lexicographically
//! by key part, and text key parts honor their effective collating sequence.
//!
//! This mirrors the INTEGER PRIMARY KEY ordering guarantee (Issue #4926) tested
//! in `integer_primary_key_order_tests.rs`.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::CreateTable(create) = stmt {
        CreateTableExecutor::execute(&create, db).unwrap();
    } else {
        panic!("Expected CREATE TABLE statement");
    }
}

fn insert(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(db, &insert_stmt).unwrap();
    } else {
        panic!("Expected INSERT statement");
    }
}

fn execute_query(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

fn int_col(rows: &[vibesql_storage::Row], col: usize) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.get(col).unwrap() {
            SqlValue::Integer(v) => *v,
            SqlValue::Bigint(v) => *v,
            other => panic!("Expected integer, got {other:?}"),
        })
        .collect()
}

fn text_col(rows: &[vibesql_storage::Row], col: usize) -> Vec<String> {
    rows.iter()
        .map(|row| match row.get(col).unwrap() {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
            other => panic!("Expected text, got {other:?}"),
        })
        .collect()
}

#[test]
fn test_single_column_pk_scan_is_pk_ordered() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t (a INTEGER PRIMARY KEY, b) WITHOUT ROWID");

    // Insert out of PK order.
    insert(&mut db, "INSERT INTO t VALUES (5, 'five')");
    insert(&mut db, "INSERT INTO t VALUES (1, 'one')");
    insert(&mut db, "INSERT INTO t VALUES (3, 'three')");
    insert(&mut db, "INSERT INTO t VALUES (2, 'two')");

    let rows = execute_query(&db, "SELECT a FROM t");
    assert_eq!(
        int_col(&rows, 0),
        vec![1, 2, 3, 5],
        "WITHOUT ROWID scan must return rows in ascending PRIMARY KEY order"
    );
}

#[test]
fn test_composite_pk_scan_is_lexicographically_ordered() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t (a, b, c, PRIMARY KEY(c, a, b)) WITHOUT ROWID");

    // Matches the without_rowid4-4.2 recursive-trigger scenario: rows inserted
    // as (1,2,4) then (1,2,3); PK is (c,a,b) so the scan must yield (1,2,3)
    // before (1,2,4).
    insert(&mut db, "INSERT INTO t VALUES (1, 2, 4)");
    insert(&mut db, "INSERT INTO t VALUES (1, 2, 3)");

    let rows = execute_query(&db, "SELECT a, b, c FROM t");
    let c_values = int_col(&rows, 2);
    assert_eq!(c_values, vec![3, 4], "composite PK ordered by (c, a, b): c=3 row precedes c=4 row");
}

#[test]
fn test_composite_pk_multi_part_tiebreak() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t (a, b, PRIMARY KEY(a, b)) WITHOUT ROWID");

    insert(&mut db, "INSERT INTO t VALUES (2, 1)");
    insert(&mut db, "INSERT INTO t VALUES (1, 2)");
    insert(&mut db, "INSERT INTO t VALUES (1, 1)");
    insert(&mut db, "INSERT INTO t VALUES (2, 0)");

    let rows = execute_query(&db, "SELECT a, b FROM t");
    let pairs: Vec<(i64, i64)> = int_col(&rows, 0).into_iter().zip(int_col(&rows, 1)).collect();
    assert_eq!(
        pairs,
        vec![(1, 1), (1, 2), (2, 0), (2, 1)],
        "second key part breaks ties within equal first key part"
    );
}

#[test]
fn test_pk_scan_with_where_is_pk_ordered() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t (a INTEGER PRIMARY KEY, b) WITHOUT ROWID");

    insert(&mut db, "INSERT INTO t VALUES (5, 'five')");
    insert(&mut db, "INSERT INTO t VALUES (1, 'one')");
    insert(&mut db, "INSERT INTO t VALUES (3, 'three')");
    insert(&mut db, "INSERT INTO t VALUES (2, 'two')");

    let rows = execute_query(&db, "SELECT a FROM t WHERE a >= 2");
    assert_eq!(
        int_col(&rows, 0),
        vec![2, 3, 5],
        "filtered WITHOUT ROWID scan must still be PK-ordered"
    );
}

#[test]
fn test_explicit_order_by_overrides_implicit_pk_order() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t (a INTEGER PRIMARY KEY, b) WITHOUT ROWID");

    insert(&mut db, "INSERT INTO t VALUES (1, 'one')");
    insert(&mut db, "INSERT INTO t VALUES (2, 'two')");
    insert(&mut db, "INSERT INTO t VALUES (3, 'three')");

    let rows = execute_query(&db, "SELECT a FROM t ORDER BY a DESC");
    assert_eq!(
        int_col(&rows, 0),
        vec![3, 2, 1],
        "explicit ORDER BY DESC overrides the implicit PK order"
    );
}

#[test]
fn test_text_pk_scan_respects_nocase_collation() {
    let mut db = Database::new();
    // Key-part COLLATE nocase: ordering must be case-insensitive.
    create_table(
        &mut db,
        "CREATE TABLE t (a TEXT, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID",
    );

    insert(&mut db, "INSERT INTO t VALUES ('banana', 1)");
    insert(&mut db, "INSERT INTO t VALUES ('Apple', 2)");
    insert(&mut db, "INSERT INTO t VALUES ('cherry', 3)");

    let rows = execute_query(&db, "SELECT a FROM t");
    assert_eq!(
        text_col(&rows, 0),
        vec!["Apple".to_string(), "banana".to_string(), "cherry".to_string()],
        "NOCASE PK collation orders 'Apple' before 'banana' case-insensitively"
    );
}
