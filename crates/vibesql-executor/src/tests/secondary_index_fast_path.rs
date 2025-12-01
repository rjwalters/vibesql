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
            ColumnSchema::new("c_last".to_string(), DataType::Varchar { max_length: Some(16) }, false),
            ColumnSchema::new("c_first".to_string(), DataType::Varchar { max_length: Some(16) }, false),
            ColumnSchema::new("c_balance".to_string(), DataType::Numeric { precision: 12, scale: 2 }, false),
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
                SqlValue::Varchar(c_last.to_string()),
                SqlValue::Varchar(c_first.to_string()),
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

        let ids: Vec<i64> = result.iter()
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

        let ids: Vec<i64> = result.iter()
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
        assert_eq!(result[0].values[1], SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[1].values[1], SqlValue::Varchar("Bob".to_string()));
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
        assert_eq!(result[0].values[3], SqlValue::Varchar("BROWN".to_string())); // c_last
        assert_eq!(result[0].values[4], SqlValue::Varchar("Frank".to_string())); // c_first
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
    let query = "SELECT c_id, c_first FROM customer WHERE c_last = 'SMITH' ORDER BY c_first LIMIT 2";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return only 2 rows
        assert_eq!(result.len(), 2);
    } else {
        panic!("Expected SELECT statement");
    }
}
