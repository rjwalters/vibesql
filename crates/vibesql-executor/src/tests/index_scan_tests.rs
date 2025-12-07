//! Integration tests for index scan optimization
//!
//! These tests verify that the executor correctly uses indexes for query optimization
//! when appropriate indexes exist and WHERE clauses can benefit from them.

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::SelectExecutor;

/// Create a test database with users table
fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create users table
    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "city".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(users_schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            SqlValue::Integer(25),
            SqlValue::Varchar(arcstr::ArcStr::from("Boston")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("bob@example.com")),
            SqlValue::Integer(30),
            SqlValue::Varchar(arcstr::ArcStr::from("New York")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("charlie@example.com")),
            SqlValue::Integer(25),
            SqlValue::Varchar(arcstr::ArcStr::from("Boston")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("diana@example.com")),
            SqlValue::Integer(35),
            SqlValue::Varchar(arcstr::ArcStr::from("Chicago")),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_index_scan_with_email_index() {
    let mut db = create_test_db();

    // Create index on email column
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        false, // not unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query that should use the email index
    let query = "SELECT * FROM users WHERE email = 'alice@example.com'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return exactly 1 row
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], SqlValue::Integer(1));
        assert_eq!(result[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_scan_with_age_index() {
    let mut db = create_test_db();

    // Create index on age column
    db.create_index(
        "idx_users_age".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "age".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query that should use the age index
    let query = "SELECT id, email FROM users WHERE age = 25";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return 2 rows (Alice and Charlie, both age 25)
        assert_eq!(result.len(), 2);

        // Verify we got the correct users
        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&1)); // Alice
        assert!(ids.contains(&3)); // Charlie
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_table_scan_without_index() {
    let db = create_test_db();
    // Note: No index created, should fall back to table scan

    let executor = SelectExecutor::new(&db);

    // Query without any index available
    let query = "SELECT * FROM users WHERE city = 'Boston'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should still work correctly with table scan
        assert_eq!(result.len(), 2); // Alice and Charlie in Boston
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_scan_with_comparison_operator() {
    let mut db = create_test_db();

    // Create index on age column
    db.create_index(
        "idx_users_age".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "age".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with comparison operator (should still use index scan path)
    let query = "SELECT id FROM users WHERE age > 28";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return Bob (30) and Diana (35)
        assert_eq!(result.len(), 2);

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&2)); // Bob (age 30)
        assert!(ids.contains(&4)); // Diana (age 35)
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_scan_with_and_condition() {
    let mut db = create_test_db();

    // Create index on age column
    db.create_index(
        "idx_users_age".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "age".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with AND condition (index on age, regular filter on city)
    let query = "SELECT * FROM users WHERE age = 25 AND city = 'Boston'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return 2 rows (Alice and Charlie are both 25 and in Boston)
        assert_eq!(result.len(), 2);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_unique_index_enforcement() {
    let mut db = create_test_db();

    // Create unique index on email
    let result = db.create_index(
        "idx_users_email_unique".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    );

    // Should succeed initially
    assert!(result.is_ok());

    // Now try to insert a duplicate email
    // The implementation now enforces uniqueness on user-defined indexes
    let duplicate_result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")), // duplicate
            SqlValue::Integer(40),
            SqlValue::Varchar(arcstr::ArcStr::from("Seattle")),
        ]),
    );

    // Should reject duplicates in unique indexes
    assert!(duplicate_result.is_err(), "Unique indexes should prevent duplicate values");
}

/// Regression test for issue #1838: Double WHERE filtering with index scans + ORDER BY
///
/// **Bug**: Queries with WHERE on indexed columns + ORDER BY were losing rows because
/// `try_index_based_where_filtering()` applied WHERE filtering a second time on rows
/// that were already filtered by `execute_index_scan()`.
///
/// **Example**: `SELECT pk FROM tab1 WHERE col3 > 221 ORDER BY pk DESC` returned 7 rows
/// instead of 8 (missing pk=2).
///
/// **Fix**: Disabled redundant `try_index_based_where_filtering()` call since index scans
/// handle WHERE filtering correctly at the scan level.
#[test]
fn test_index_scan_where_order_by_no_double_filtering() {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table matching the original bug report
    let schema = TableSchema::new(
        "tab1".to_string(),
        vec![
            ColumnSchema::new("pk".to_string(), DataType::Integer, false),
            ColumnSchema::new("col1".to_string(), DataType::Real, false),
            ColumnSchema::new("col2".to_string(), DataType::Real, false),
            ColumnSchema::new("col3".to_string(), DataType::Real, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create index on col3 (the WHERE filter column)
    db.create_index(
        "idx_col3".to_string(),
        "tab1".to_string(),
        false,
        vec![IndexColumn {
            column_name: "col3".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert test data - pk=2 with col3=652.0 should be included (col3 > 221)
    let test_data = vec![
        (0, 846.0),
        (1, 562.0),
        (2, 652.0),
        (3, 51.0),
        (4, 256.0),
        (5, 957.0),
        (6, 656.0),
        (7, 294.0),
        (8, 878.0),
        (9, 105.0),
    ];

    for (pk, col3_val) in test_data {
        db.insert_row(
            "tab1",
            Row::new(vec![
                SqlValue::Integer(pk),
                SqlValue::Real(pk as f32 * 10.0),
                SqlValue::Real(pk as f32 * 20.0),
                SqlValue::Real(col3_val as f32),
            ]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Query that triggered the bug: WHERE on indexed column + ORDER BY
    // Note: Using 221.0 instead of 221 to ensure type compatibility with REAL column
    let query = "SELECT pk FROM tab1 WHERE col3 > 221.0 ORDER BY pk DESC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Expected: 8 rows where col3 > 221 (values: 846, 562, 652, 256, 957, 656, 294, 878)
        assert_eq!(result.len(), 8, "Should return 8 rows where col3 > 221");

        // Extract PKs and verify correct DESC ordering
        let pks: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(pk) => *pk,
                _ => panic!("Expected integer PK"),
            })
            .collect();

        assert_eq!(pks, vec![8, 7, 6, 5, 4, 2, 1, 0], "PKs should be in DESC order");

        // Specifically verify pk=2 is present (this row was lost in bug #1838)
        assert!(
            pks.contains(&2),
            "Row with pk=2 (col3=652.0) must be present - was lost in double filtering bug"
        );
    } else {
        panic!("Expected SELECT statement");
    }
}
