//! Integration tests for non-unique disk-backed indexes with duplicate keys
//!
//! These tests verify that non-unique indexes correctly handle duplicate key values
//! in real-world SQL scenarios. Tests cover:
//! - Basic duplicate key handling
//! - Multi-column indexes with duplicates
//! - Range queries
//! - Updates and deletes
//! - Heavy duplicate scenarios (disk-backed with #[ignore])
//!
//! Related: PR #1571 (duplicate key support), Issue #1562 (original bug)

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::SelectExecutor;

/// Helper to create employees table for testing
fn create_employees_table() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "department".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();
    db
}

#[test]
fn test_basic_non_unique_index_with_duplicates() {
    // Test Scenario 1: Basic Non-Unique Index with Duplicates
    // Verifies that non-unique indexes correctly return all rows with duplicate keys
    let mut db = create_employees_table();

    // Insert test data with duplicate departments
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(100000),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(120000),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(80000),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(110000),
            SqlValue::Varchar(arcstr::ArcStr::from("Diana")),
        ]),
    )
    .unwrap();

    // Create non-unique index on department column
    db.create_index(
        "idx_dept".to_string(),
        "employees".to_string(),
        false, // non-unique
        vec![IndexColumn {
            column_name: "department".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query for Engineering department - should return 3 rows
    let query = "SELECT * FROM employees WHERE department = 'Engineering'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Verify we got all 3 Engineering employees
        assert_eq!(result.len(), 3, "Should return all 3 Engineering employees");

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&1)); // Alice
        assert!(ids.contains(&2)); // Bob
        assert!(ids.contains(&4)); // Diana
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_multi_column_non_unique_index() {
    // Test Scenario 2: Multi-Column Non-Unique Index
    // Verifies that composite indexes handle duplicate key combinations correctly
    let mut db = create_employees_table();

    // Insert test data
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(100000),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(120000),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(80000),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
        ]),
    )
    .unwrap();

    // Insert another employee with same (department, salary) as Alice
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(100000),
            SqlValue::Varchar(arcstr::ArcStr::from("Eve")),
        ]),
    )
    .unwrap();

    // Create multi-column non-unique index
    db.create_index(
        "idx_dept_salary".to_string(),
        "employees".to_string(),
        false, // non-unique
        vec![
            IndexColumn {
                column_name: "department".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "salary".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query for Engineering employees with salary 100000 - should return 2 rows
    let query = "SELECT * FROM employees WHERE department = 'Engineering' AND salary = 100000";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Verify we got both employees with this combination
        assert_eq!(result.len(), 2, "Should return 2 employees (Alice and Eve)");

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&1)); // Alice
        assert!(ids.contains(&5)); // Eve
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_range_queries_with_duplicates() {
    // Test Scenario 3: Range Queries with Duplicates
    // Verifies that range scans work correctly with duplicate keys
    let mut db = create_employees_table();

    // Insert test data with multiple employees at various salary levels
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(100000),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(120000),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(110000),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(100000),
            SqlValue::Varchar(arcstr::ArcStr::from("Diana")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(90000),
            SqlValue::Varchar(arcstr::ArcStr::from("Eve")),
        ]),
    )
    .unwrap();

    // Create non-unique index on salary
    db.create_index(
        "idx_salary".to_string(),
        "employees".to_string(),
        false, // non-unique
        vec![IndexColumn {
            column_name: "salary".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Range query: salary BETWEEN 100000 AND 120000
    let query =
        "SELECT * FROM employees WHERE salary >= 100000 AND salary <= 120000 ORDER BY salary, id";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return 4 employees (Alice, Diana at 100K, Charlie at 110K, Bob at 120K)
        assert_eq!(result.len(), 4, "Should return 4 employees in salary range");

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&1)); // Alice - 100K
        assert!(ids.contains(&4)); // Diana - 100K
        assert!(ids.contains(&3)); // Charlie - 110K
        assert!(ids.contains(&2)); // Bob - 120K
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_updates_affecting_non_unique_indexes() {
    // Test Scenario 4: Updates Affecting Non-Unique Indexes
    // Verifies that UPDATE operations correctly maintain non-unique indexes
    let mut db = create_employees_table();

    // Insert test data
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(100000),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(120000),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(80000),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(110000),
            SqlValue::Varchar(arcstr::ArcStr::from("Diana")),
        ]),
    )
    .unwrap();

    // Create non-unique index on department
    db.create_index(
        "idx_dept".to_string(),
        "employees".to_string(),
        false, // non-unique
        vec![IndexColumn {
            column_name: "department".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Update Bob's department from Engineering to Marketing
    let update_query = "UPDATE employees SET department = 'Marketing' WHERE id = 2";
    let update_stmt = Parser::parse_sql(update_query).unwrap();

    if let vibesql_ast::Statement::Update(stmt) = update_stmt {
        let updated = crate::update::UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(updated, 1, "Should update 1 row");
    } else {
        panic!("Expected UPDATE statement");
    }

    let executor = SelectExecutor::new(&db);

    // Query Engineering department - should now return only 2 rows (Alice and Diana)
    let query = "SELECT * FROM employees WHERE department = 'Engineering'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 2, "Should return 2 Engineering employees after update");

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&1)); // Alice
        assert!(ids.contains(&4)); // Diana
        assert!(!ids.contains(&2)); // Bob moved to Marketing
    } else {
        panic!("Expected SELECT statement");
    }

    // Query Marketing department - should return 1 row (Bob)
    let query2 = "SELECT * FROM employees WHERE department = 'Marketing'";
    let stmt2 = Parser::parse_sql(query2).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt2 {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 1, "Should return 1 Marketing employee");
        assert_eq!(result[0].values[0], SqlValue::Integer(2)); // Bob
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_deletes_with_duplicates() {
    // Test Scenario 5: Deletes with Duplicates
    // Verifies that DELETE operations correctly update non-unique indexes
    let mut db = create_employees_table();

    // Insert test data with duplicate departments
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(100000),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(120000),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(110000),
            SqlValue::Varchar(arcstr::ArcStr::from("Diana")),
        ]),
    )
    .unwrap();

    // Create non-unique index on department
    db.create_index(
        "idx_dept".to_string(),
        "employees".to_string(),
        false, // non-unique
        vec![IndexColumn {
            column_name: "department".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Delete Alice (id=1)
    let delete_query = "DELETE FROM employees WHERE id = 1";
    let delete_stmt = Parser::parse_sql(delete_query).unwrap();

    if let vibesql_ast::Statement::Delete(stmt) = delete_stmt {
        let deleted = crate::delete::DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 1, "Should delete 1 row");
    } else {
        panic!("Expected DELETE statement");
    }

    let executor = SelectExecutor::new(&db);

    // Query Engineering department - should return 2 rows (Bob and Diana)
    let query = "SELECT * FROM employees WHERE department = 'Engineering'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 2, "Should return 2 Engineering employees after delete");

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(!ids.contains(&1)); // Alice deleted
        assert!(ids.contains(&2)); // Bob
        assert!(ids.contains(&4)); // Diana
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_heavy_duplicate_scenario_in_memory() {
    // Test Scenario 6 (In-Memory Version): Heavy Duplicate Scenario
    // Tests performance with many duplicates using in-memory index
    // This runs fast for CI/CD but tests the same logic
    let mut db = create_employees_table();

    // Insert 1000 rows with the same department (in-memory index)
    for i in 0..1000 {
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
                SqlValue::Integer(100000),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("Employee{}", i))),
            ]),
        )
        .unwrap();
    }

    // Create non-unique index on department
    db.create_index(
        "idx_dept".to_string(),
        "employees".to_string(),
        false, // non-unique
        vec![IndexColumn {
            column_name: "department".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query for all Engineering employees - should return all 1000 rows efficiently
    let query = "SELECT COUNT(*) FROM employees WHERE department = 'Engineering'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Verify count
        assert_eq!(result.len(), 1, "COUNT should return 1 row");
        match &result[0].values[0] {
            SqlValue::Integer(count) => {
                assert_eq!(*count, 1000, "Should return count of 1000 employees");
            }
            _ => panic!("Expected integer count"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_heavy_duplicate_scenario_disk_backed() {
    // Test Scenario 6 (Disk-Backed Version): Heavy Duplicate Scenario
    // This test verifies non-unique indexes with many duplicates per key.
    // Reduced from 100,500 rows to 5,025 rows (20x reduction) for faster tests.
    // See issue #3382 for future optimization of bulk loading with heavy duplicates.
    let mut db = create_employees_table();

    // Insert 5,025 rows - still tests heavy duplicates (~1,675 per department)
    // Original test used 100,500 rows but was too slow (>10 minutes)
    for i in 0..5_025 {
        let dept = match i % 3 {
            0 => "Engineering",
            1 => "Sales",
            _ => "Marketing",
        };

        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(dept)),
                SqlValue::Integer(80000 + (i % 50000) as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("Employee{}", i))),
            ]),
        )
        .unwrap();
    }

    // Create non-unique index on department - should use disk-backed storage
    db.create_index(
        "idx_dept".to_string(),
        "employees".to_string(),
        false, // non-unique
        vec![IndexColumn {
            column_name: "department".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query for all Engineering employees
    let query = "SELECT COUNT(*) FROM employees WHERE department = 'Engineering'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Verify count (every 3rd row is Engineering)
        assert_eq!(result.len(), 1, "COUNT should return 1 row");
        match &result[0].values[0] {
            SqlValue::Integer(count) => {
                // Should be exactly 5_025 / 3 = 1,675
                assert_eq!(
                    *count, 1675,
                    "Should return exactly 1,675 Engineering employees, got {}",
                    count
                );
            }
            _ => panic!("Expected integer count"),
        }
    } else {
        panic!("Expected SELECT statement");
    }

    // Test that individual row lookups work correctly too
    let query2 = "SELECT * FROM employees WHERE department = 'Sales' LIMIT 10";
    let stmt2 = Parser::parse_sql(query2).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt2 {
        let result = executor.execute(&select_stmt).unwrap();
        assert_eq!(result.len(), 10, "Should return 10 Sales employees");
    } else {
        panic!("Expected SELECT statement");
    }
}
