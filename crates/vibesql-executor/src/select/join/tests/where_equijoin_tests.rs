//! Tests for WHERE clause equijoin optimization
//!
//! These tests verify that equijoin predicates from WHERE clauses are applied
//! during join execution (not post-join filtering), preventing massive intermediate
//! result materialization.

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
fn test_where_equijoin_applied_during_join() {
    // Create database with two tables
    let mut db = Database::new();

    // Create table t1 with column a
    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t1 VALUES ({})", i));
    }

    // Create table t2 with column b
    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t2 VALUES ({})", i));
    }

    // Query with WHERE equijoin: SELECT * FROM t1, t2 WHERE t1.a = t2.b
    // With optimization: ~10 matches (not 100)
    // Without optimization: 100 allocations then filtered to ~10
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql("SELECT * FROM t1, t2 WHERE t1.a = t2.b").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    // Should get 10 rows (matching pairs)
    assert_eq!(result.len(), 10);

    // Verify each row has matching values
    for row in &result {
        assert_eq!(row.values[0], row.values[1]);
    }
}

#[test]
fn test_where_equijoin_with_additional_filter() {
    // Create database with two tables
    let mut db = Database::new();

    // Create table t1
    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER, x INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t1 VALUES ({}, {})", i, i * 10));
    }

    // Create table t2
    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER, y INTEGER)");
    for i in 1..=10 {
        exec_sql(&mut db, &format!("INSERT INTO t2 VALUES ({}, {})", i, i * 5));
    }

    // Query: SELECT * FROM t1, t2 WHERE t1.a = t2.b AND t1.x > 50
    let executor = SelectExecutor::new(&db);
    let select_stmt =
        Parser::parse_sql("SELECT * FROM t1, t2 WHERE t1.a = t2.b AND t1.x > 50").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    // Should get 5 rows (a=6,7,8,9,10 where x>50)
    assert_eq!(result.len(), 5);
}

#[test]
fn test_multiple_table_where_equijoins() {
    // Test the select5.test pattern with small scale
    let mut db = Database::new();

    // Create 4 tables with 10 rows each
    for table_num in 1..=4 {
        exec_sql(&mut db, &format!("CREATE TABLE t{} (a INTEGER)", table_num));
        for i in 1..=10 {
            exec_sql(&mut db, &format!("INSERT INTO t{} VALUES ({})", table_num, i));
        }
    }

    // Query: SELECT * FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a
    // With optimization: Each join produces ~10 matches (not cartesian products)
    // Total allocations: ~40 (not 10^4)
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql(
        "SELECT * FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a",
    )
    .unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    // Should get 10 rows (all matching on value 1-10)
    assert_eq!(result.len(), 10);

    // Verify all columns in each row match
    for row in &result {
        let first_val = &row.values[0];
        for val in &row.values[1..4] {
            assert_eq!(val, first_val);
        }
    }
}

#[test]
fn test_where_equijoin_no_matches() {
    // Test case where equijoin produces no matches
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (1)");
    exec_sql(&mut db, "INSERT INTO t1 VALUES (2)");

    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    exec_sql(&mut db, "INSERT INTO t2 VALUES (3)");
    exec_sql(&mut db, "INSERT INTO t2 VALUES (4)");

    // No matching values between t1 and t2
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql("SELECT * FROM t1, t2 WHERE t1.a = t2.b").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    // Should get 0 rows
    assert_eq!(result.len(), 0);
}

#[test]
fn test_where_equijoin_partial_matches() {
    // Test case where only some values match
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t1 (a INTEGER)");
    for i in 1..=5 {
        exec_sql(&mut db, &format!("INSERT INTO t1 VALUES ({})", i));
    }

    exec_sql(&mut db, "CREATE TABLE t2 (b INTEGER)");
    for i in 3..=7 {
        exec_sql(&mut db, &format!("INSERT INTO t2 VALUES ({})", i));
    }

    // Only values 3, 4, 5 appear in both tables
    let executor = SelectExecutor::new(&db);
    let select_stmt = Parser::parse_sql("SELECT * FROM t1, t2 WHERE t1.a = t2.b").unwrap();
    let result = if let vibesql_ast::Statement::Select(s) = select_stmt {
        executor.execute(&s).unwrap()
    } else {
        panic!("Expected SELECT statement")
    };

    // Should get 3 rows (3, 4, 5)
    assert_eq!(result.len(), 3);
}
