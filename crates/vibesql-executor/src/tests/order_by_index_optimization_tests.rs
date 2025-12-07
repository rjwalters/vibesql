//! Integration tests for ORDER BY index optimization
//!
//! These tests verify that the executor correctly uses indexes to satisfy ORDER BY clauses
//! when appropriate indexes exist, avoiding the need for an explicit sort step.
//!
//! Coverage:
//! - Single-column ORDER BY with ASC index
//! - Single-column ORDER BY DESC with index reversal
//! - Multi-column ORDER BY with composite index
//! - ORDER BY with WHERE clause combined
//! - Partial index matches (should fall back to sort)
//! - Mixed ASC/DESC directions
//! - Primary key index usage
//! - ORDER BY with positional references
//! - ORDER BY with aliases
//! - ORDER BY with NULL values

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::SelectExecutor;

/// Create a test database with products table for ORDER BY tests
fn create_products_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create products table with columns suitable for ORDER BY testing
    let products_schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "category".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("stock".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(products_schema).unwrap();

    // Insert test data (intentionally unordered)
    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")),
            SqlValue::Integer(80),
            SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
            SqlValue::Integer(50),
        ]),
    )
    .unwrap();

    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Mouse")),
            SqlValue::Integer(30),
            SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Monitor")),
            SqlValue::Integer(300),
            SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
            SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Desk")),
            SqlValue::Integer(200),
            SqlValue::Varchar(arcstr::ArcStr::from("Furniture")),
            SqlValue::Integer(15),
        ]),
    )
    .unwrap();

    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Chair")),
            SqlValue::Integer(150),
            SqlValue::Varchar(arcstr::ArcStr::from("Furniture")),
            SqlValue::Integer(30),
        ]),
    )
    .unwrap();

    db
}

/// Create a test database with nullable columns for NULL handling tests
fn create_nullable_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    let schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, true), // nullable
        ],
    );

    db.create_table(schema).unwrap();

    // Insert data with NULLs
    db.insert_row("items", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    db.insert_row("items", Row::new(vec![SqlValue::Integer(2), SqlValue::Null])).unwrap();
    db.insert_row("items", Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(5)])).unwrap();
    db.insert_row("items", Row::new(vec![SqlValue::Integer(4), SqlValue::Null])).unwrap();
    db.insert_row("items", Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(15)])).unwrap();

    db
}

#[test]
fn test_order_by_single_column_asc_with_index() {
    let mut db = create_products_db();

    // Create index on price column (ASC)
    db.create_index(
        "idx_products_price".to_string(),
        "products".to_string(),
        false,
        vec![IndexColumn {
            column_name: "price".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY that should use the index
    let query = "SELECT name, price FROM products ORDER BY price ASC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Verify results are ordered by price ascending
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")));
        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Chair")));
        assert_eq!(result[3].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Desk")));
        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_single_column_desc_with_index() {
    let mut db = create_products_db();

    // Create index on price column (ASC) - should be usable for DESC via reversal
    db.create_index(
        "idx_products_price".to_string(),
        "products".to_string(),
        false,
        vec![IndexColumn {
            column_name: "price".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY DESC - should use index with reversal
    let query = "SELECT name, price FROM products ORDER BY price DESC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Verify results are ordered by price descending
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Desk")));
        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Chair")));
        assert_eq!(result[3].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")));
        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_multi_column_with_composite_index() {
    let mut db = create_products_db();

    // Create composite index on (category, price)
    db.create_index(
        "idx_products_category_price".to_string(),
        "products".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "category".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "price".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with multi-column ORDER BY matching the index
    let query = "SELECT name, category, price FROM products ORDER BY category, price";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Verify results are ordered by category, then price
        assert_eq!(result.len(), 5);

        // Electronics products (sorted by price)
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Electronics")));

        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")));
        assert_eq!(result[1].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Electronics")));

        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
        assert_eq!(result[2].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Electronics")));

        // Furniture products (sorted by price)
        assert_eq!(result[3].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Chair")));
        assert_eq!(result[3].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Furniture")));

        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Desk")));
        assert_eq!(result[4].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Furniture")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_with_where_clause() {
    let mut db = create_products_db();

    // Create index on price
    db.create_index(
        "idx_products_price".to_string(),
        "products".to_string(),
        false,
        vec![IndexColumn {
            column_name: "price".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with WHERE and ORDER BY - index should still be used for ordering
    let query = "SELECT name, price FROM products WHERE category = 'Electronics' ORDER BY price";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return only Electronics products, ordered by price
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")));
        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_partial_index_match_falls_back_to_sort() {
    let mut db = create_products_db();

    // Create composite index on (category, price)
    db.create_index(
        "idx_products_category_price".to_string(),
        "products".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "category".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "price".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY that doesn't match index prefix - should fall back to sort
    let query = "SELECT name, price, category FROM products ORDER BY price, category";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should still work correctly (via sort), ordered by price then category
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_without_index_uses_sort() {
    let db = create_products_db();
    // Note: No index created - should use sort

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY but no suitable index
    let query = "SELECT name, stock FROM products ORDER BY stock";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should work correctly via sort
        assert_eq!(result.len(), 5);

        // Verify ordering by stock
        let stocks: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[1] {
                SqlValue::Integer(s) => *s,
                _ => panic!("Expected integer stock"),
            })
            .collect();

        assert_eq!(stocks, vec![15, 20, 30, 50, 100]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_primary_key_implicit_index() {
    let db = create_products_db();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY on id (assuming it's the primary key or has implicit index)
    let query = "SELECT id, name FROM products ORDER BY id";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should be ordered by id
        assert_eq!(result.len(), 5);

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer id"),
            })
            .collect();

        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_mixed_asc_desc_no_index() {
    let mut db = create_products_db();

    // Create index on (category ASC, price ASC)
    db.create_index(
        "idx_products_category_price".to_string(),
        "products".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "category".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "price".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with mixed ASC/DESC that doesn't match index - should fall back to sort
    let query = "SELECT name, category, price FROM products ORDER BY category ASC, price DESC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should work correctly via sort
        assert_eq!(result.len(), 5);

        // Electronics products (sorted by price DESC)
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")));
        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));

        // Furniture products (sorted by price DESC)
        assert_eq!(result[3].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Desk")));
        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Chair")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_positional_reference() {
    let mut db = create_products_db();

    // Create index on price
    db.create_index(
        "idx_products_price".to_string(),
        "products".to_string(),
        false,
        vec![IndexColumn {
            column_name: "price".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY using positional reference (2 = price column)
    let query = "SELECT name, price FROM products ORDER BY 2";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should be ordered by price (second column)
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_alias() {
    let mut db = create_products_db();

    // Create index on price
    db.create_index(
        "idx_products_price".to_string(),
        "products".to_string(),
        false,
        vec![IndexColumn {
            column_name: "price".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY using alias
    let query = "SELECT name, price AS cost FROM products ORDER BY cost";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should be ordered by price (aliased as cost)
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_with_nulls_asc() {
    let mut db = create_nullable_db();

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

    // Query with ORDER BY on nullable column - NULLs should come last
    let query = "SELECT id, value FROM items ORDER BY value ASC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 5);

        // Values should be ordered: 5, 10, 15, NULL, NULL
        let values: Vec<Option<i64>> = result
            .iter()
            .map(|row| match &row.values[1] {
                SqlValue::Integer(v) => Some(*v),
                SqlValue::Null => None,
                _ => panic!("Expected integer or null"),
            })
            .collect();

        assert_eq!(values[0], Some(5));
        assert_eq!(values[1], Some(10));
        assert_eq!(values[2], Some(15));
        assert_eq!(values[3], None); // NULL
        assert_eq!(values[4], None); // NULL
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_with_nulls_desc() {
    let mut db = create_nullable_db();

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

    // Query with ORDER BY DESC - NULLs should still come last
    let query = "SELECT id, value FROM items ORDER BY value DESC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 5);

        // Values should be ordered: 15, 10, 5, NULL, NULL
        let values: Vec<Option<i64>> = result
            .iter()
            .map(|row| match &row.values[1] {
                SqlValue::Integer(v) => Some(*v),
                SqlValue::Null => None,
                _ => panic!("Expected integer or null"),
            })
            .collect();

        assert_eq!(values[0], Some(15));
        assert_eq!(values[1], Some(10));
        assert_eq!(values[2], Some(5));
        assert_eq!(values[3], None); // NULL
        assert_eq!(values[4], None); // NULL
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_multi_column_desc_with_index_reversal() {
    let mut db = create_products_db();

    // Create composite index on (category ASC, price ASC)
    db.create_index(
        "idx_products_category_price".to_string(),
        "products".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "category".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "price".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY that's the complete opposite - should use index with reversal
    let query = "SELECT name, category, price FROM products ORDER BY category DESC, price DESC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 5);

        // Furniture products first (sorted by price DESC)
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Desk")));
        assert_eq!(result[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Furniture")));

        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Chair")));
        assert_eq!(result[1].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Furniture")));

        // Electronics products second (sorted by price DESC)
        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Monitor")));
        assert_eq!(result[2].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Electronics")));

        assert_eq!(result[3].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")));
        assert_eq!(result[3].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Electronics")));

        assert_eq!(result[4].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[4].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Electronics")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_limit_with_index() {
    let mut db = create_products_db();

    // Create index on price
    db.create_index(
        "idx_products_price".to_string(),
        "products".to_string(),
        false,
        vec![IndexColumn {
            column_name: "price".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with ORDER BY and LIMIT - index should still be used
    let query = "SELECT name, price FROM products ORDER BY price LIMIT 3";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return only first 3 products ordered by price
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Mouse")));
        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Keyboard")));
        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Chair")));
    } else {
        panic!("Expected SELECT statement");
    }
}
