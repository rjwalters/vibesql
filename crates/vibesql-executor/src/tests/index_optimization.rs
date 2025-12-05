//! Integration tests for index optimization scenarios
//!
//! These tests verify that the executor correctly handles edge cases in index optimization
//! that previously caused failures in sqllogictest suite (issue #1807).
//!
//! Test coverage:
//! - BETWEEN with mixed numeric types (Integer, Real, Numeric, Double)
//! - IN operator with large value sets (100+ values)
//! - Commuted comparison operators (col > 5 vs 5 < col)
//! - Numeric type normalization in index range scans

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::SelectExecutor;

/// Create a test database with a table containing various numeric types
fn create_numeric_types_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table with columns of different numeric types
    let schema = TableSchema::new(
        "measurements".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("int_value".to_string(), DataType::Integer, false),
            ColumnSchema::new("real_value".to_string(), DataType::Real, false),
            ColumnSchema::new("double_value".to_string(), DataType::DoublePrecision, false),
            ColumnSchema::new(
                "numeric_value".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with various numeric values
    // Values chosen to test range scans and type normalization
    db.insert_row(
        "measurements",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Real(10.5),
            SqlValue::Double(10.75),
            SqlValue::Numeric(10.9),
        ]),
    )
    .unwrap();

    db.insert_row(
        "measurements",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(20),
            SqlValue::Real(20.5),
            SqlValue::Double(20.75),
            SqlValue::Numeric(20.9),
        ]),
    )
    .unwrap();

    db.insert_row(
        "measurements",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(30),
            SqlValue::Real(30.5),
            SqlValue::Double(30.75),
            SqlValue::Numeric(30.9),
        ]),
    )
    .unwrap();

    db.insert_row(
        "measurements",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Integer(40),
            SqlValue::Real(40.5),
            SqlValue::Double(40.75),
            SqlValue::Numeric(40.9),
        ]),
    )
    .unwrap();

    db.insert_row(
        "measurements",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Integer(50),
            SqlValue::Real(50.5),
            SqlValue::Double(50.75),
            SqlValue::Numeric(50.9),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_index_range_scan_between_numeric_types() {
    let mut db = create_numeric_types_db();

    // Create index on int_value column
    // Create all indexes first
    db.create_index(
        "idx_int_value".to_string(),
        "measurements".to_string(),
        false, // not unique
        vec![IndexColumn {
            column_name: "int_value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_real_value".to_string(),
        "measurements".to_string(),
        false,
        vec![IndexColumn {
            column_name: "real_value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_double_value".to_string(),
        "measurements".to_string(),
        false,
        vec![IndexColumn {
            column_name: "double_value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Now create executor after all indexes are created
    let executor = SelectExecutor::new(&db);

    // Test BETWEEN on integer column with index
    let query = "SELECT id, int_value FROM measurements WHERE int_value BETWEEN 15 AND 35";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return rows with int_value 20 and 30 (ids 2 and 3)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], SqlValue::Integer(2));
        assert_eq!(result[0].values[1], SqlValue::Integer(20));
        assert_eq!(result[1].values[0], SqlValue::Integer(3));
        assert_eq!(result[1].values[1], SqlValue::Integer(30));
    } else {
        panic!("Expected SELECT statement");
    }

    // Test BETWEEN on Real column with index
    let query = "SELECT id, real_value FROM measurements WHERE real_value BETWEEN 15.0 AND 35.0";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return rows with real_value 20.5 and 30.5 (ids 2 and 3)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], SqlValue::Integer(2));
        assert_eq!(result[1].values[0], SqlValue::Integer(3));
    } else {
        panic!("Expected SELECT statement");
    }

    // Test BETWEEN on Double column with index
    let query =
        "SELECT id, double_value FROM measurements WHERE double_value BETWEEN 15.0 AND 35.0";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return rows with double_value 20.75 and 30.75 (ids 2 and 3)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], SqlValue::Integer(2));
        assert_eq!(result[1].values[0], SqlValue::Integer(3));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_multi_lookup_large_value_set() {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table with many rows
    let schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 150 rows
    for i in 0..150 {
        db.insert_row("items", Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i * 10)]))
            .unwrap();
    }

    // Create index on value column
    db.create_index(
        "idx_items_value".to_string(),
        "items".to_string(),
        false,
        vec![IndexColumn {
            column_name: "value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test IN operator with 100+ values
    // This tests index multi-lookup optimization
    let mut in_values = Vec::new();
    for i in (0..100).step_by(2) {
        // Even values from 0 to 98
        in_values.push(i * 10);
    }
    let in_list = in_values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");

    let query = format!("SELECT id FROM items WHERE value IN ({}) ORDER BY id", in_list);
    let stmt = Parser::parse_sql(&query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return 50 rows (even numbers from 0 to 98)
        assert_eq!(result.len(), 50);

        // Verify first few results
        assert_eq!(result[0].values[0], SqlValue::Integer(0));
        assert_eq!(result[1].values[0], SqlValue::Integer(2));
        assert_eq!(result[2].values[0], SqlValue::Integer(4));

        // Verify last result
        assert_eq!(result[49].values[0], SqlValue::Integer(98));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_commuted_comparisons() {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create simple table
    let schema = TableSchema::new(
        "numbers".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    for i in 1..=10 {
        db.insert_row("numbers", Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i * 10)]))
            .unwrap();
    }

    // Create index on value column
    db.create_index(
        "idx_numbers_value".to_string(),
        "numbers".to_string(),
        false,
        vec![IndexColumn {
            column_name: "value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test: value > 50 (normal form)
    let query1 = "SELECT id FROM numbers WHERE value > 50 ORDER BY id";
    let stmt1 = Parser::parse_sql(query1).unwrap();

    let result1 = if let vibesql_ast::Statement::Select(select_stmt) = stmt1 {
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    };

    // Test: 50 < value (commuted form)
    let query2 = "SELECT id FROM numbers WHERE 50 < value ORDER BY id";
    let stmt2 = Parser::parse_sql(query2).unwrap();

    let result2 = if let vibesql_ast::Statement::Select(select_stmt) = stmt2 {
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    };

    // Both queries should return identical results
    assert_eq!(result1.len(), result2.len());
    assert_eq!(result1.len(), 5); // ids 6, 7, 8, 9, 10

    for i in 0..result1.len() {
        assert_eq!(result1[i].values[0], result2[i].values[0]);
    }

    // Verify actual values
    assert_eq!(result1[0].values[0], SqlValue::Integer(6));
    assert_eq!(result1[4].values[0], SqlValue::Integer(10));

    // Test >= and <= commutation
    let query3 = "SELECT id FROM numbers WHERE value >= 60 ORDER BY id";
    let stmt3 = Parser::parse_sql(query3).unwrap();

    let result3 = if let vibesql_ast::Statement::Select(select_stmt) = stmt3 {
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    };

    let query4 = "SELECT id FROM numbers WHERE 60 <= value ORDER BY id";
    let stmt4 = Parser::parse_sql(query4).unwrap();

    let result4 = if let vibesql_ast::Statement::Select(select_stmt) = stmt4 {
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    };

    // Both queries should return identical results
    assert_eq!(result3.len(), result4.len());
    assert_eq!(result3.len(), 5); // ids 6, 7, 8, 9, 10

    for i in 0..result3.len() {
        assert_eq!(result3[i].values[0], result4[i].values[0]);
    }
}

#[test]
fn test_index_numeric_type_normalization() {
    let mut db = create_numeric_types_db();

    // Create all indexes first
    db.create_index(
        "idx_int".to_string(),
        "measurements".to_string(),
        false,
        vec![IndexColumn {
            column_name: "int_value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_real".to_string(),
        "measurements".to_string(),
        false,
        vec![IndexColumn {
            column_name: "real_value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_double".to_string(),
        "measurements".to_string(),
        false,
        vec![IndexColumn {
            column_name: "double_value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_numeric".to_string(),
        "measurements".to_string(),
        false,
        vec![IndexColumn {
            column_name: "numeric_value".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Now create executor after all indexes are created
    let executor = SelectExecutor::new(&db);

    // Test that Integer column can be compared with Real literal using index
    // This tests normalize_for_comparison() in index_operations.rs:20
    let query = "SELECT id, int_value FROM measurements WHERE int_value > 25.0";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return rows with int_value 30, 40, 50 (ids 3, 4, 5)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values[0], SqlValue::Integer(3));
        assert_eq!(result[1].values[0], SqlValue::Integer(4));
        assert_eq!(result[2].values[0], SqlValue::Integer(5));
    } else {
        panic!("Expected SELECT statement");
    }

    // Test that Real column can be compared with Integer literal using index
    let query = "SELECT id, real_value FROM measurements WHERE real_value < 25";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return rows with real_value 10.5 and 20.5 (ids 1 and 2)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], SqlValue::Integer(1));
        assert_eq!(result[1].values[0], SqlValue::Integer(2));
    } else {
        panic!("Expected SELECT statement");
    }

    // Test that Double column works with Numeric comparison
    let query = "SELECT id FROM measurements WHERE double_value >= 30.75 AND double_value <= 40.75";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return rows with double_value 30.75 and 40.75 (ids 3 and 4)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], SqlValue::Integer(3));
        assert_eq!(result[1].values[0], SqlValue::Integer(4));
    } else {
        panic!("Expected SELECT statement");
    }

    // Test range scan on Numeric column
    let query = "SELECT id FROM measurements WHERE numeric_value BETWEEN 20.0 AND 40.0";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return rows with numeric_value 20.9 and 30.9 (ids 2 and 3)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], SqlValue::Integer(2));
        assert_eq!(result[1].values[0], SqlValue::Integer(3));
    } else {
        panic!("Expected SELECT statement");
    }
}
