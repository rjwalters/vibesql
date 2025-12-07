//! Integration tests for secondary index fast path optimization
//!
//! These tests verify that the executor correctly uses secondary index lookups
//! for queries with equality predicates on all columns of a secondary index.
//!
//! Test coverage:
//! - Single-column secondary index lookup
//! - Composite secondary index lookup (TPC-C customer-by-last-name pattern)
//! - Secondary index with ORDER BY
//! - Secondary index with projection

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::SelectExecutor;

/// Create a test database with a customer table similar to TPC-C
fn create_customer_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table similar to TPC-C customer table
    // (c_id, c_w_id, c_d_id, c_last, c_first, c_balance)
    let mut schema = TableSchema::new(
        "customer".to_string(),
        vec![
            ColumnSchema::new("c_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "c_last".to_string(),
                DataType::Varchar { max_length: Some(16) },
                false,
            ),
            ColumnSchema::new(
                "c_first".to_string(),
                DataType::Varchar { max_length: Some(16) },
                false,
            ),
            ColumnSchema::new(
                "c_balance".to_string(),
                DataType::Numeric { precision: 12, scale: 2 },
                false,
            ),
        ],
    );

    // Set primary key on (c_w_id, c_d_id, c_id)
    schema.primary_key = Some(vec!["c_w_id".to_string(), "c_d_id".to_string(), "c_id".to_string()]);

    db.create_table(schema).unwrap();

    // Insert test data with different customers
    let customers = vec![
        (1, 1, 1, "SMITH", "Alice", 100.00),
        (2, 1, 1, "SMITH", "Bob", 200.00),
        (3, 1, 1, "JONES", "Charlie", 300.00),
        (4, 1, 2, "SMITH", "Diana", 400.00),
        (5, 2, 1, "SMITH", "Eve", 500.00),
        (6, 1, 1, "BROWN", "Frank", 600.00),
    ];

    for (c_id, c_w_id, c_d_id, c_last, c_first, c_balance) in customers {
        db.insert_row(
            "customer",
            Row::new(vec![
                SqlValue::Integer(c_id),
                SqlValue::Integer(c_w_id),
                SqlValue::Integer(c_d_id),
                SqlValue::Varchar(arcstr::ArcStr::from(c_last)),
                SqlValue::Varchar(arcstr::ArcStr::from(c_first)),
                SqlValue::Numeric(c_balance),
            ]),
        )
        .unwrap();
    }

    db
}

#[test]
fn test_secondary_index_single_column_lookup() {
    let mut db = create_customer_db();

    // Create secondary index on c_last
    db.create_index(
        "idx_customer_last".to_string(),
        "customer".to_string(),
        false, // not unique
        vec![IndexColumn {
            column_name: "c_last".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query using secondary index on c_last
    let query = "SELECT c_id, c_first FROM customer WHERE c_last = 'SMITH'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return all SMITH customers (ids 1, 2, 4, 5)
        assert_eq!(result.len(), 4);

        let ids: Vec<i64> = result
            .iter()
            .filter_map(|r| if let SqlValue::Integer(id) = r.values[0] { Some(id) } else { None })
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&4));
        assert!(ids.contains(&5));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_secondary_index_composite_lookup() {
    let mut db = create_customer_db();

    // Create composite secondary index on (c_w_id, c_d_id, c_last) - TPC-C pattern
    db.create_index(
        "idx_customer_name".to_string(),
        "customer".to_string(),
        false, // not unique
        vec![
            IndexColumn {
                column_name: "c_w_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "c_d_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "c_last".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query using composite secondary index (TPC-C customer-by-last-name pattern)
    let query = "SELECT c_id, c_first, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'SMITH'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return SMITH customers in warehouse 1, district 1 (ids 1, 2)
        assert_eq!(result.len(), 2);

        let ids: Vec<i64> = result
            .iter()
            .filter_map(|r| if let SqlValue::Integer(id) = r.values[0] { Some(id) } else { None })
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_secondary_index_with_order_by() {
    let mut db = create_customer_db();

    // Create secondary index on (c_w_id, c_d_id, c_last)
    db.create_index(
        "idx_customer_name".to_string(),
        "customer".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "c_w_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "c_d_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "c_last".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY on non-indexed column
    let query = "SELECT c_id, c_first, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'SMITH' ORDER BY c_first";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return SMITH customers ordered by first name (Alice, Bob)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
        assert_eq!(result[1].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_secondary_index_select_star() {
    let mut db = create_customer_db();

    // Create secondary index on c_last
    db.create_index(
        "idx_customer_last".to_string(),
        "customer".to_string(),
        false,
        vec![IndexColumn {
            column_name: "c_last".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with SELECT *
    let query = "SELECT * FROM customer WHERE c_last = 'BROWN'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return one row (Frank Brown)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], SqlValue::Integer(6)); // c_id
        assert_eq!(result[0].values[3], SqlValue::Varchar(arcstr::ArcStr::from("BROWN"))); // c_last
        assert_eq!(result[0].values[4], SqlValue::Varchar(arcstr::ArcStr::from("Frank")));
    // c_first
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_secondary_index_no_match() {
    let mut db = create_customer_db();

    // Create secondary index on c_last
    db.create_index(
        "idx_customer_last".to_string(),
        "customer".to_string(),
        false,
        vec![IndexColumn {
            column_name: "c_last".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query for non-existent value
    let query = "SELECT c_id FROM customer WHERE c_last = 'NOTEXIST'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return empty result
        assert_eq!(result.len(), 0);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_secondary_index_with_limit() {
    let mut db = create_customer_db();

    // Create secondary index on c_last
    db.create_index(
        "idx_customer_last".to_string(),
        "customer".to_string(),
        false,
        vec![IndexColumn {
            column_name: "c_last".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with LIMIT
    let query =
        "SELECT c_id, c_first FROM customer WHERE c_last = 'SMITH' ORDER BY c_first LIMIT 2";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return only 2 rows
        assert_eq!(result.len(), 2);
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Regression test for issue #3354: Secondary index fast path must apply residual predicates
///
/// When using a secondary index for lookup, any predicates NOT covered by the index
/// (i.e., non-equality predicates or predicates on columns not in the index) must still
/// be applied as a filter.
///
/// Bug: Query `WHERE col3 = 81814 AND col0 < 1013` with index on col3 would:
/// 1. Use index to find rows where col3 = 81814
/// 2. Return rows WITHOUT checking `col0 < 1013`
///
/// This test verifies that residual predicates are correctly applied.
#[test]
fn test_secondary_index_residual_predicate_filter() {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create simple table with pk, col0, col3
    let schema = TableSchema::new(
        "tab1".to_string(),
        vec![
            ColumnSchema::new("pk".to_string(), DataType::Integer, false),
            ColumnSchema::new("col0".to_string(), DataType::Integer, false),
            ColumnSchema::new("col3".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create secondary index on col3 only
    db.create_index(
        "idx_tab1_3".to_string(),
        "tab1".to_string(),
        false,
        vec![IndexColumn {
            column_name: "col3".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert row: pk=881, col0=20123, col3=81814
    // Note: col0=20123 does NOT satisfy col0 < 1013
    db.insert_row(
        "tab1",
        Row::new(vec![SqlValue::Integer(881), SqlValue::Integer(20123), SqlValue::Integer(81814)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query: col3 = 81814 (uses index) AND col0 < 1013 (residual filter)
    // The row has col0=20123 which does NOT satisfy col0 < 1013, so should return 0 rows
    let query = "SELECT pk, col0, col3 FROM tab1 WHERE col3 = 81814 AND col0 < 1013";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return 0 rows because col0=20123 does NOT satisfy col0 < 1013
        assert_eq!(result.len(), 0, "Residual predicate col0 < 1013 should filter out the row");
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test that rows matching both index predicate AND residual predicate are returned
#[test]
fn test_secondary_index_residual_predicate_match() {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create simple table with pk, col0, col3
    let schema = TableSchema::new(
        "tab1".to_string(),
        vec![
            ColumnSchema::new("pk".to_string(), DataType::Integer, false),
            ColumnSchema::new("col0".to_string(), DataType::Integer, false),
            ColumnSchema::new("col3".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create secondary index on col3 only
    db.create_index(
        "idx_tab1_3".to_string(),
        "tab1".to_string(),
        false,
        vec![IndexColumn {
            column_name: "col3".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert two rows with same col3 value but different col0 values
    // Row 881: col0=20123 (does NOT satisfy col0 < 1013)
    // Row 882: col0=500 (DOES satisfy col0 < 1013)
    db.insert_row(
        "tab1",
        Row::new(vec![SqlValue::Integer(881), SqlValue::Integer(20123), SqlValue::Integer(81814)]),
    )
    .unwrap();

    db.insert_row(
        "tab1",
        Row::new(vec![SqlValue::Integer(882), SqlValue::Integer(500), SqlValue::Integer(81814)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query: col3 = 81814 (uses index, finds both rows) AND col0 < 1013 (residual filter)
    // Only row 882 should be returned (col0=500 < 1013)
    let query = "SELECT pk, col0, col3 FROM tab1 WHERE col3 = 81814 AND col0 < 1013";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return only 1 row (pk=882)
        assert_eq!(result.len(), 1, "Should return only the row matching residual predicate");
        assert_eq!(result[0].values[0], SqlValue::Integer(882), "Should return pk=882");
        assert_eq!(result[0].values[1], SqlValue::Integer(500), "Should return col0=500");
    } else {
        panic!("Expected SELECT statement");
    }
}
