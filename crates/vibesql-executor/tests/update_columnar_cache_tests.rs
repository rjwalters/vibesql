//! Tests for UPDATE + columnar cache invalidation
//!
//! This module tests that UPDATE operations correctly invalidate the columnar cache,
//! ensuring that subsequent reads via `Database::get_columnar()` return the updated data
//! rather than stale cached data.
//!
//! Related: #3884, #3890, #3891, #3911


mod common;

use common::setup_test_table;
use vibesql_ast::{Assignment, BinaryOperator, Expression, UpdateStmt, WhereClause};
use vibesql_executor::UpdateExecutor;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Regression test for issue #3911:
/// Verify that UPDATE correctly invalidates the database-level columnar cache.
///
/// This test:
/// 1. Creates a table and populates it with data
/// 2. Warms the columnar cache via `database.get_columnar()`
/// 3. Executes an UPDATE statement that modifies existing rows
/// 4. Verifies the columnar cache returns the updated data (not stale cached data)
#[test]
fn test_update_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("employees").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 3);

    // Verify initial cache statistics
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1, "Should have done one conversion");

    // Verify initial data in the columnar cache
    let salary_col = initial_columnar.get_column("salary").expect("Column should exist");
    // Salaries should be 45000, 48000, 42000 (Alice, Bob, Charlie)
    let salaries: Vec<i64> = (0..3)
        .map(|i| match salary_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    assert_eq!(salaries, vec![45000, 48000, 42000], "Initial salaries should be 45000, 48000, 42000");

    // Execute UPDATE - give Alice a raise
    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(55000)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };
    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1, "Should update 1 row");

    // Get columnar data again - should reflect the updated data
    let updated_columnar = db.get_columnar("employees").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 3, "Should still have 3 rows");

    let updated_salary_col = updated_columnar.get_column("salary").expect("Column should exist");
    let updated_salaries: Vec<i64> = (0..3)
        .map(|i| match updated_salary_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();

    // Verify the updated value is present
    assert!(
        updated_salaries.contains(&55000),
        "New salary 55000 should exist after UPDATE"
    );
    assert!(
        !updated_salaries.contains(&45000),
        "Old salary 45000 should no longer exist after UPDATE"
    );
    assert!(
        updated_salaries.contains(&48000),
        "Unchanged salary 48000 should still exist"
    );
    assert!(
        updated_salaries.contains(&42000),
        "Unchanged salary 42000 should still exist"
    );

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after UPDATE (conversions: {})",
        final_stats.conversions
    );
}

/// Test that UPDATE invalidates cache even when using pre_warm_columnar_cache
#[test]
fn test_update_invalidates_prewarmed_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Pre-warm the cache
    let warmed = db.pre_warm_columnar_cache(&["employees"]).unwrap();
    assert_eq!(warmed, 1, "Should warm 1 table");

    // Verify pre-warmed data
    let initial_columnar = db.get_columnar("employees").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 3);

    // Cache hit should have occurred for the second access
    let stats_after_get = db.columnar_cache_stats();
    assert_eq!(stats_after_get.hits, 1, "Second get should be a cache hit");

    // UPDATE to change Bob's salary
    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(75000)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Bob")))),
        })),
    };
    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1, "Should update 1 row");

    // Get columnar data - should reflect updated values, not stale cache
    let updated_columnar = db.get_columnar("employees").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 3, "Should still have 3 rows");

    // Verify the new salary is present
    let salary_col = updated_columnar.get_column("salary").expect("Column should exist");
    let has_new_salary = (0..3).any(|i| matches!(salary_col.get(i), SqlValue::Integer(75000)));
    assert!(has_new_salary, "New salary 75000 should be visible after UPDATE");

    let has_old_salary = (0..3).any(|i| matches!(salary_col.get(i), SqlValue::Integer(48000)));
    assert!(!has_old_salary, "Old salary 48000 should NOT be visible after UPDATE");
}

/// Test that UPDATE of all rows invalidates cache
#[test]
fn test_update_all_rows_invalidates_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Warm the cache
    let _ = db.get_columnar("employees").unwrap();

    // UPDATE all rows - give everyone the same salary
    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(60000)),
        }],
        where_clause: None,
    };
    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 3, "Should update 3 rows");

    // Verify updated columnar data
    let columnar = db.get_columnar("employees").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 3, "Should still have 3 rows");

    let salary_col = columnar.get_column("salary").expect("Column should exist");
    let all_updated = (0..3).all(|i| matches!(salary_col.get(i), SqlValue::Integer(60000)));
    assert!(all_updated, "All salaries should be 60000 after UPDATE");
}

/// Test that UPDATE with no matching rows does NOT invalidate cache unnecessarily
#[test]
fn test_update_no_match_does_not_invalidate_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Warm the cache
    let _ = db.get_columnar("employees").unwrap();
    let initial_stats = db.columnar_cache_stats();
    let initial_conversions = initial_stats.conversions;

    // UPDATE with WHERE clause that matches no rows
    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(999999)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(999))), // No such id
        })),
    };
    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 0, "Should update 0 rows");

    // Get columnar data - should be a cache hit since nothing changed
    let _ = db.get_columnar("employees").unwrap();
    let final_stats = db.columnar_cache_stats();

    // Since no rows were updated, cache should not have been invalidated
    assert_eq!(
        final_stats.conversions, initial_conversions,
        "Cache should not be re-converted when no rows are updated (conversions: {} vs {})",
        final_stats.conversions, initial_conversions
    );
}

/// Test that multiple sequential UPDATEs correctly invalidate cache each time
#[test]
fn test_multiple_updates_invalidate_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Warm the cache
    let _ = db.get_columnar("employees").unwrap();

    // First UPDATE
    let stmt1 = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(50000)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };
    UpdateExecutor::execute(&stmt1, &mut db).unwrap();

    // Verify first update
    let columnar1 = db.get_columnar("employees").unwrap().expect("Table should exist");
    let salary_col1 = columnar1.get_column("salary").expect("Column should exist");
    assert!(
        (0..3).any(|i| matches!(salary_col1.get(i), SqlValue::Integer(50000))),
        "First update salary 50000 should be visible"
    );

    // Second UPDATE
    let stmt2 = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(55000)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        })),
    };
    UpdateExecutor::execute(&stmt2, &mut db).unwrap();

    // Verify second update AND first update still visible
    let columnar2 = db.get_columnar("employees").unwrap().expect("Table should exist");
    let salary_col2 = columnar2.get_column("salary").expect("Column should exist");
    let salaries: Vec<i64> = (0..3)
        .filter_map(|i| match salary_col2.get(i) {
            SqlValue::Integer(v) => Some(v),
            _ => None,
        })
        .collect();

    assert!(salaries.contains(&50000), "First update (50000) should still be visible");
    assert!(salaries.contains(&55000), "Second update (55000) should be visible");
    assert!(salaries.contains(&42000), "Unchanged salary (42000) should be visible");
}

/// Test that UPDATE with multiple columns invalidates cache correctly
#[test]
fn test_update_multiple_columns_invalidates_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Warm the cache
    let _ = db.get_columnar("employees").unwrap();

    // UPDATE multiple columns at once
    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![
            Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Integer(70000)),
            },
            Assignment {
                column: "department".to_string(),
                value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Management"))),
            },
        ],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };
    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1, "Should update 1 row");

    // Verify both columns are updated in columnar data
    let columnar = db.get_columnar("employees").unwrap().expect("Table should exist");

    let salary_col = columnar.get_column("salary").expect("Column should exist");
    assert!(
        (0..3).any(|i| matches!(salary_col.get(i), SqlValue::Integer(70000))),
        "New salary 70000 should be visible"
    );

    let dept_col = columnar.get_column("department").expect("Column should exist");
    assert!(
        (0..3).any(|i| matches!(dept_col.get(i), SqlValue::Varchar(ref s) if s.as_str() == "Management")),
        "New department 'Management' should be visible"
    );
}
