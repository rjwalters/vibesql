//! Tests for covering index scan (index-only scan) optimization
//!
//! These tests verify that when all SELECT columns are part of an index key,
//! the executor can retrieve data directly from the index without table access.
//! This optimization is critical for queries like TPC-C Stock-Level.

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::SelectExecutor;

/// Create a test database simulating TPC-C stock table
fn create_stock_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create stock table similar to TPC-C
    let stock_schema = TableSchema::new(
        "stock".to_string(),
        vec![
            ColumnSchema::new("s_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_i_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_quantity".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "s_data".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(stock_schema).unwrap();

    // Insert test data with varying quantities
    // Warehouse 1, items 1-10 with various quantities
    for i in 1..=10 {
        db.insert_row(
            "stock",
            Row::new(vec![
                SqlValue::Integer(1),                             // s_w_id
                SqlValue::Integer(i),                             // s_i_id
                SqlValue::Integer(5 + (i % 20)),                  // s_quantity
                SqlValue::Varchar(arcstr::ArcStr::from(format!("Stock data {}", i))),   // s_data
            ]),
        )
        .unwrap();
    }

    // Warehouse 2, items 1-5 with various quantities
    for i in 1..=5 {
        db.insert_row(
            "stock",
            Row::new(vec![
                SqlValue::Integer(2),                             // s_w_id
                SqlValue::Integer(i),                             // s_i_id
                SqlValue::Integer(10 + i),                        // s_quantity (11-15)
                SqlValue::Varchar(arcstr::ArcStr::from(format!("Stock data W2-{}", i))),
            ]),
        )
        .unwrap();
    }

    db
}

#[test]
fn test_covering_index_single_column_select() {
    let mut db = create_stock_db();

    // Create covering index on (s_w_id, s_quantity, s_i_id)
    // This covers the Stock-Level query pattern: SELECT s_i_id WHERE s_w_id = ? AND s_quantity < ?
    db.create_index(
        "idx_stock_quantity".to_string(),
        "stock".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "s_w_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "s_quantity".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "s_i_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    // Query that should use covering index: SELECT s_i_id WHERE s_w_id = 1 AND s_quantity < 15
    let executor = SelectExecutor::new(&db);
    let query = "SELECT s_i_id FROM stock WHERE s_w_id = 1 AND s_quantity < 15";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Items with s_w_id=1 and s_quantity < 15:
        // i=1: quantity=6, i=2: quantity=7, i=3: quantity=8, i=4: quantity=9, i=5: quantity=10
        // i=6: quantity=11, i=7: quantity=12, i=8: quantity=13, i=9: quantity=14
        // So s_i_id values: 1, 2, 3, 4, 5, 6, 7, 8, 9 (9 items)
        assert!(!result.is_empty());

        // Verify we got the right item IDs
        let item_ids: Vec<i64> = result.iter()
            .map(|r| match &r.values[0] {
                SqlValue::Integer(v) => *v,
                _ => panic!("Expected integer"),
            })
            .collect();

        // Should have items 1-9 (those with quantity < 15)
        for id in &item_ids {
            assert!(*id >= 1 && *id <= 9, "Unexpected item id: {}", id);
        }
        assert_eq!(item_ids.len(), 9);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_covering_index_multiple_columns_select() {
    let mut db = create_stock_db();

    // Create covering index on (s_w_id, s_quantity, s_i_id)
    db.create_index(
        "idx_stock_covering".to_string(),
        "stock".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "s_w_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "s_quantity".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "s_i_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    // Query selecting multiple columns that are all in the index
    let executor = SelectExecutor::new(&db);
    let query = "SELECT s_i_id, s_quantity FROM stock WHERE s_w_id = 1 AND s_quantity < 10";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Items with s_w_id=1 and s_quantity < 10:
        // i=1: quantity=6, i=2: quantity=7, i=3: quantity=8, i=4: quantity=9
        assert_eq!(result.len(), 4);

        // Verify column values
        for row in &result {
            let s_i_id = match &row.values[0] {
                SqlValue::Integer(v) => *v,
                _ => panic!("Expected integer for s_i_id"),
            };
            let s_quantity = match &row.values[1] {
                SqlValue::Integer(v) => *v,
                _ => panic!("Expected integer for s_quantity"),
            };

            // Verify quantity matches the pattern: quantity = 5 + (s_i_id % 20)
            assert!(s_quantity < 10);
            assert!((1..=4).contains(&s_i_id));
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_non_covering_index_falls_back_to_table_fetch() {
    let mut db = create_stock_db();

    // Create index that doesn't cover s_data column
    db.create_index(
        "idx_stock_partial".to_string(),
        "stock".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "s_w_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "s_i_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    // Query selecting s_data which is NOT in the index - should still work via table fetch
    let executor = SelectExecutor::new(&db);
    let query = "SELECT s_data FROM stock WHERE s_w_id = 1";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return all 10 items from warehouse 1
        assert_eq!(result.len(), 10);

        // Verify we got actual data (not NULL or empty)
        for row in &result {
            match &row.values[0] {
                SqlValue::Varchar(s) => assert!(s.starts_with("Stock data")),
                _ => panic!("Expected varchar for s_data"),
            };
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_covering_index_prefix_equality_range() {
    let mut db = create_stock_db();

    // Create covering index
    db.create_index(
        "idx_stock_wid_qty_iid".to_string(),
        "stock".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "s_w_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "s_quantity".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "s_i_id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    // Query with equality on first column and range on second
    let executor = SelectExecutor::new(&db);
    let query = "SELECT s_i_id FROM stock WHERE s_w_id = 2 AND s_quantity > 12";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Warehouse 2 has items 1-5 with quantities 11-15
        // Items with quantity > 12: i=3 (qty=13), i=4 (qty=14), i=5 (qty=15)
        assert_eq!(result.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_covering_index_check_function() {
    use crate::select::scan::index_scan::covering::check_covering_index;

    // Index covers all needed columns
    let index_cols = vec!["s_w_id", "s_quantity", "s_i_id"];
    let needed = vec!["s_i_id".to_string()];
    let result = check_covering_index(&index_cols, &needed);
    assert!(result.is_some());

    // Index doesn't cover all needed columns
    let index_cols = vec!["s_w_id", "s_quantity"];
    let needed = vec!["s_i_id".to_string(), "s_data".to_string()];
    let result = check_covering_index(&index_cols, &needed);
    assert!(result.is_none());

    // Case-insensitive matching
    let index_cols = vec!["S_W_ID", "S_QUANTITY", "S_I_ID"];
    let needed = vec!["s_i_id".to_string()];
    let result = check_covering_index(&index_cols, &needed);
    assert!(result.is_some());
}
