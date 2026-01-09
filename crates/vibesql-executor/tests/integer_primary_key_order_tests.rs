//! Tests for INTEGER PRIMARY KEY implicit row ordering (Issue #4926)
//!
//! SQLite guarantees that tables with INTEGER PRIMARY KEY return rows in rowid order
//! when no ORDER BY is specified. This is because INTEGER PRIMARY KEY is an alias
//! for the rowid, and the B-tree naturally stores rows in rowid order.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn setup_db() -> Database {
    let mut db = Database::new();

    // Create table with INTEGER PRIMARY KEY
    let create = Parser::parse_sql("CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT, c TEXT)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Insert rows out of order (by primary key)
    let inserts = [
        "INSERT INTO t1 VALUES (5, 'hello', 'world')",
        "INSERT INTO t1 VALUES (6, 'second', 'entry')",
        "INSERT INTO t1 VALUES (4, 'one', 'two')",
        "INSERT INTO t1 VALUES (10, 'ten', 'value')",
        "INSERT INTO t1 VALUES (1, 'first', 'row')",
    ];

    for sql in inserts {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    db
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

#[test]
fn test_select_star_returns_rowid_order() {
    let db = setup_db();

    // SELECT * should return rows in rowid (INTEGER PRIMARY KEY) order
    let rows = execute_query(&db, "SELECT * FROM t1");

    assert_eq!(rows.len(), 5);

    // Extract the 'a' (INTEGER PRIMARY KEY) values
    let a_values: Vec<i64> = rows
        .iter()
        .map(|row| match row.get(0).unwrap() {
            SqlValue::Integer(v) => *v,
            _ => panic!("Expected Integer"),
        })
        .collect();

    // Should be in ascending rowid order: 1, 4, 5, 6, 10
    assert_eq!(a_values, vec![1, 4, 5, 6, 10], "Rows should be in INTEGER PRIMARY KEY order");
}

#[test]
fn test_select_rowid_returns_rowid_order() {
    let db = setup_db();

    // SELECT rowid should also return in rowid order
    let rows = execute_query(&db, "SELECT rowid FROM t1");

    assert_eq!(rows.len(), 5);

    let rowid_values: Vec<i64> = rows
        .iter()
        .map(|row| match row.get(0).unwrap() {
            SqlValue::Integer(v) => *v,
            SqlValue::Bigint(v) => *v,
            _ => panic!("Expected Integer or Bigint"),
        })
        .collect();

    assert_eq!(rowid_values, vec![1, 4, 5, 6, 10], "ROWID should be in ascending order");
}

#[test]
fn test_select_with_where_returns_rowid_order() {
    let db = setup_db();

    // SELECT with WHERE should still return in rowid order
    let rows = execute_query(&db, "SELECT a, b FROM t1 WHERE a > 3");

    assert_eq!(rows.len(), 4);

    let a_values: Vec<i64> = rows
        .iter()
        .map(|row| match row.get(0).unwrap() {
            SqlValue::Integer(v) => *v,
            _ => panic!("Expected Integer"),
        })
        .collect();

    // Should be in ascending rowid order: 4, 5, 6, 10
    assert_eq!(a_values, vec![4, 5, 6, 10], "Filtered rows should be in INTEGER PRIMARY KEY order");
}

#[test]
fn test_explicit_order_by_overrides_implicit() {
    let db = setup_db();

    // Explicit ORDER BY DESC should override the implicit rowid order
    let rows = execute_query(&db, "SELECT a FROM t1 ORDER BY a DESC");

    assert_eq!(rows.len(), 5);

    let a_values: Vec<i64> = rows
        .iter()
        .map(|row| match row.get(0).unwrap() {
            SqlValue::Integer(v) => *v,
            _ => panic!("Expected Integer"),
        })
        .collect();

    // Should be in descending order: 10, 6, 5, 4, 1
    assert_eq!(a_values, vec![10, 6, 5, 4, 1], "ORDER BY DESC should override implicit order");
}

#[test]
fn test_non_integer_primary_key_no_implicit_order() {
    // Tables without INTEGER PRIMARY KEY don't have guaranteed order
    let mut db = Database::new();

    // Create table with TEXT primary key (not INTEGER PRIMARY KEY)
    let create = Parser::parse_sql("CREATE TABLE t2 (id TEXT PRIMARY KEY, val INTEGER)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    let inserts = [
        "INSERT INTO t2 VALUES ('c', 3)",
        "INSERT INTO t2 VALUES ('a', 1)",
        "INSERT INTO t2 VALUES ('b', 2)",
    ];

    for sql in inserts {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    let rows = execute_query(&db, "SELECT * FROM t2");
    assert_eq!(rows.len(), 3);
    // For TEXT primary key, order is not guaranteed (insertion order is acceptable)
}

#[test]
fn test_integer_primary_key_with_negative_values() {
    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE t3 (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    let inserts = [
        "INSERT INTO t3 VALUES (5, 'five')",
        "INSERT INTO t3 VALUES (-3, 'neg three')",
        "INSERT INTO t3 VALUES (0, 'zero')",
        "INSERT INTO t3 VALUES (-10, 'neg ten')",
        "INSERT INTO t3 VALUES (2, 'two')",
    ];

    for sql in inserts {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    let rows = execute_query(&db, "SELECT id FROM t3");
    assert_eq!(rows.len(), 5);

    let id_values: Vec<i64> = rows
        .iter()
        .map(|row| match row.get(0).unwrap() {
            SqlValue::Integer(v) => *v,
            _ => panic!("Expected Integer"),
        })
        .collect();

    // Should be in ascending order: -10, -3, 0, 2, 5
    assert_eq!(id_values, vec![-10, -3, 0, 2, 5], "Negative rowids should also be ordered");
}
