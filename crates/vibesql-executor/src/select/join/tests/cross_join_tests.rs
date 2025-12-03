//! Tests for cross join (cartesian product) optimization (#3388)
//!
//! These tests verify that cross joins with no condition use the fast path
//! that avoids evaluator overhead, and that cross joins with single-table
//! predicates (pushed to table scans) work correctly.

use crate::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Helper to execute SQL and handle statements
fn exec_sql(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap();
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).unwrap();
        }
        _ => panic!("Unexpected statement type in test"),
    }
}

#[test]
fn test_cross_join_no_condition() {
    // Basic cross join (cartesian product) with no WHERE clause
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (1)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (2)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (3)");

    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    exec_sql(&mut db, "INSERT INTO t2 VALUES (10)");
    exec_sql(&mut db, "INSERT INTO t2 VALUES (20)");

    // Cross join: 3 * 2 = 6 rows
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql("SELECT * FROM t1, t2").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    assert_eq!(result.len(), 6);
}

#[test]
fn test_cross_join_with_single_table_predicates() {
    // Cross join with WHERE predicates that only reference single tables
    // These predicates should be pushed down to table scans, resulting in
    // a cross join of filtered results with no join condition.
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t1 VALUES ({})", i));
    }

    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t2 VALUES ({})", i));
    }

    // Predicates a < 4 and b > 7 are single-table predicates
    // After pushdown: t1 has 3 rows (1,2,3), t2 has 3 rows (8,9,10)
    // Cross join: 3 * 3 = 9 rows
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql("SELECT * FROM t1, t2 WHERE a < 4 AND b > 7").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    assert_eq!(result.len(), 9);

    // Verify all a values are < 4 and all b values are > 7
    for row in &result {
        let a = match &row.values[0] {
            vibesql_types::SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        };
        let b = match &row.values[1] {
            vibesql_types::SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        };
        assert!(a < 4, "a={} should be < 4", a);
        assert!(b > 7, "b={} should be > 7", b);
    }
}

#[test]
fn test_multi_table_cross_join_with_single_table_predicates() {
    // Test the select4.test pattern: 4-way cross join with single-table predicates
    let mut db = Database::new();

    // Create 4 tables with 10 rows each
    exec_sql(&mut db, "CREATE TABLE t1 (a1 INTEGER)");
    exec_sql(&mut db, "CREATE TABLE t2 (a2 INTEGER)");
    exec_sql(&mut db, "CREATE TABLE t3 (a3 INTEGER)");
    exec_sql(&mut db, "CREATE TABLE t4 (a4 INTEGER)");

    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t1 VALUES ({})", i));
        exec_sql(&mut db, &format!("INSERT INTO t2 VALUES ({})", i));
        exec_sql(&mut db, &format!("INSERT INTO t3 VALUES ({})", i));
        exec_sql(&mut db, &format!("INSERT INTO t4 VALUES ({})", i));
    }

    // Query with single-table predicates on each table
    // Each predicate filters to 2 rows: a1 IN (1,2), a2 IN (3,4), etc.
    // Result: 2 * 2 * 2 * 2 = 16 rows
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql(
        "SELECT * FROM t1, t2, t3, t4 WHERE a1 IN (1,2) AND a2 IN (3,4) AND a3 IN (5,6) AND a4 IN (7,8)",
    )
    .unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    assert_eq!(result.len(), 16);
}

#[test]
fn test_cross_join_empty_table() {
    // Cross join with an empty table should produce empty result
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (1)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (2)");

    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    // t2 is empty

    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql("SELECT * FROM t1, t2").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    // 2 * 0 = 0 rows
    assert_eq!(result.len(), 0);
}

#[test]
fn test_cross_join_single_row_tables() {
    // Cross join of single-row tables
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (1)");

    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    exec_sql(&mut db, "INSERT INTO t2 VALUES (2)");

    exec_sql(&mut db, "CREATE TABLE t3 (c INTEGER)");
    exec_sql(&mut db, "INSERT INTO t3 VALUES (3)");

    // 1 * 1 * 1 = 1 row
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql("SELECT * FROM t1, t2, t3").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values.len(), 3);
}

#[test]
fn test_cross_join_with_in_predicates() {
    // Test IN predicates (common in select4.test) with cross join
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (e INTEGER)");
    for i in 1..=20 {
        exec_sql(&mut db, &format!("INSERT INTO t1 VALUES ({})", i));
    }

    exec_sql(&mut db, "CREATE TABLE t2 (f INTEGER)");
    for i in 1..=20 {
        exec_sql(&mut db, &format!("INSERT INTO t2 VALUES ({})", i));
    }

    // e IN (1,5,10) matches 3 rows, f IN (2,4,6,8) matches 4 rows
    // Cross join: 3 * 4 = 12 rows
    let executor = SelectExecutor::new(&db);
    let select_stmt =
        Parser::parse_sql("SELECT * FROM t1, t2 WHERE e IN (1,5,10) AND f IN (2,4,6,8)").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    assert_eq!(result.len(), 12);
}

#[test]
fn test_cross_join_with_or_predicates() {
    // Test OR predicates (common in select4.test) with cross join
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t1 VALUES ({})", i));
    }

    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t2 VALUES ({})", i));
    }

    // (a=1 OR a=5) matches 2 rows, (b=2 OR b=8) matches 2 rows
    // Cross join: 2 * 2 = 4 rows
    let executor = SelectExecutor::new(&db);
    let select_stmt =
        Parser::parse_sql("SELECT * FROM t1, t2 WHERE (a=1 OR a=5) AND (b=2 OR b=8)").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    assert_eq!(result.len(), 4);
}
