//! Tests for ON DUPLICATE KEY UPDATE + columnar cache invalidation
//!
//! This module tests that ON DUPLICATE KEY UPDATE operations correctly invalidate
//! the columnar cache, ensuring that subsequent reads via `Database::get_columnar()`
//! return the updated data rather than stale cached data.
//!
//! Related: #3884, #3890, #3913

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::InsertExecutor;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Sets up a products table with PRIMARY KEY for ON DUPLICATE KEY UPDATE testing
fn setup_products_table(db: &mut Database) {
    let schema = TableSchema::with_primary_key(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
            ColumnSchema::new("stock".to_string(), DataType::Integer, true),
        ],
        vec!["id".to_string()], // id is PRIMARY KEY
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a row into products table
fn insert_product(db: &mut Database, id: i64, name: &str, stock: i64) {
    let stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(id)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(name))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(stock)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(db, &stmt).unwrap();
}

/// Helper to execute an INSERT ... ON DUPLICATE KEY UPDATE statement
fn upsert_product(db: &mut Database, id: i64, name: &str, stock: i64) {
    let stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(id)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(name))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(stock)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: Some(vec![
            vibesql_ast::Assignment {
                column: "name".to_string(),
                value: vibesql_ast::Expression::DuplicateKeyValue { column: "name".to_string() },
            },
            vibesql_ast::Assignment {
                column: "stock".to_string(),
                value: vibesql_ast::Expression::DuplicateKeyValue { column: "stock".to_string() },
            },
        ]),
    };
    InsertExecutor::execute(db, &stmt).unwrap();
}

/// Regression test for issue #3913:
/// Verify that ON DUPLICATE KEY UPDATE correctly invalidates the database-level columnar cache.
///
/// This test:
/// 1. Creates a table and populates it with data
/// 2. Warms the columnar cache via `database.get_columnar()`
/// 3. Executes an INSERT ... ON DUPLICATE KEY UPDATE statement that modifies existing rows
/// 4. Verifies the columnar cache returns the updated data (not stale cached data)
#[test]
fn test_on_duplicate_key_update_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Widget", 100);
    insert_product(&mut db, 2, "Gadget", 200);
    insert_product(&mut db, 3, "Gizmo", 300);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 3);

    // Verify initial cache statistics
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1, "Should have done one conversion");

    // Verify initial data in the columnar cache
    let stock_col = initial_columnar.get_column("stock").expect("Column should exist");
    let stocks: Vec<i64> = (0..3)
        .map(|i| match stock_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    assert_eq!(stocks, vec![100, 200, 300], "Initial stocks should be 100, 200, 300");

    // Execute ON DUPLICATE KEY UPDATE - this should update row id=1 in place
    upsert_product(&mut db, 1, "Super Widget", 150);

    // Get columnar data again - should reflect the updated data
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");

    // After ON DUPLICATE KEY UPDATE: row 1 is updated in place, order should be preserved
    assert_eq!(updated_columnar.row_count(), 3, "Should still have 3 rows");

    let updated_stock_col = updated_columnar.get_column("stock").expect("Column should exist");
    let updated_stocks: Vec<i64> = (0..3)
        .map(|i| match updated_stock_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();

    // The updated row should now have stock 150 instead of 100
    assert!(
        !updated_stocks.contains(&100),
        "Old stock 100 should no longer exist after ON DUPLICATE KEY UPDATE"
    );
    assert!(
        updated_stocks.contains(&150),
        "New stock 150 should exist after ON DUPLICATE KEY UPDATE"
    );
    assert!(
        updated_stocks.contains(&200),
        "Unchanged stock 200 should still exist"
    );
    assert!(
        updated_stocks.contains(&300),
        "Unchanged stock 300 should still exist"
    );

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after ON DUPLICATE KEY UPDATE (conversions: {})",
        final_stats.conversions
    );
}

/// Test that ON DUPLICATE KEY UPDATE invalidates cache even when using pre_warm_columnar_cache
#[test]
fn test_on_duplicate_key_update_invalidates_prewarmed_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Item A", 50);
    insert_product(&mut db, 2, "Item B", 75);

    // Pre-warm the cache
    let warmed = db.pre_warm_columnar_cache(&["products"]).unwrap();
    assert_eq!(warmed, 1, "Should warm 1 table");

    // Verify pre-warmed data
    let initial_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 2);

    // Cache hit should have occurred for the second access
    let stats_after_get = db.columnar_cache_stats();
    assert_eq!(stats_after_get.hits, 1, "Second get should be a cache hit");

    // ON DUPLICATE KEY UPDATE to change Item A's stock
    upsert_product(&mut db, 1, "Item A Updated", 999);

    // Get columnar data - should reflect updated values, not stale cache
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 2, "Should still have 2 rows");

    // Verify the new stock is present
    let stock_col = updated_columnar.get_column("stock").expect("Column should exist");
    let has_new_stock = (0..2).any(|i| matches!(stock_col.get(i), SqlValue::Integer(999)));
    assert!(has_new_stock, "New stock 999 should be visible after ON DUPLICATE KEY UPDATE");

    let has_old_stock = (0..2).any(|i| matches!(stock_col.get(i), SqlValue::Integer(50)));
    assert!(!has_old_stock, "Old stock 50 should NOT be visible after ON DUPLICATE KEY UPDATE");
}

/// Test that ON DUPLICATE KEY UPDATE on UNIQUE constraint also invalidates cache
#[test]
fn test_on_duplicate_key_update_unique_constraint_invalidates_cache() {
    let mut db = Database::new();

    // Create table with UNIQUE constraint on name
    let schema = TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
            ColumnSchema::new("score".to_string(), DataType::Integer, true),
        ],
        Some(vec!["id".to_string()]),                // Primary key
        vec![vec!["name".to_string()]],              // Unique constraint on name
    );
    db.create_table(schema).unwrap();

    // Insert users
    let insert = |db: &mut Database, id: i64, name: &str, score: i64| {
        let stmt = vibesql_ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: vibesql_ast::InsertSource::Values(vec![vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(id)),
                vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(name))),
                vibesql_ast::Expression::Literal(SqlValue::Integer(score)),
            ]]),
            conflict_clause: None,
            on_duplicate_key_update: None,
        };
        InsertExecutor::execute(db, &stmt).unwrap();
    };

    insert(&mut db, 1, "alice", 100);
    insert(&mut db, 2, "bob", 200);

    // Warm cache
    let _ = db.get_columnar("users").unwrap();

    // ON DUPLICATE KEY UPDATE with same name (unique key conflict) - should update existing row
    let upsert_stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)), // Different id
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("alice"))), // Same name (conflict)
            vibesql_ast::Expression::Literal(SqlValue::Integer(999)), // New score
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: Some(vec![vibesql_ast::Assignment {
            column: "score".to_string(),
            value: vibesql_ast::Expression::DuplicateKeyValue { column: "score".to_string() },
        }]),
    };
    InsertExecutor::execute(&mut db, &upsert_stmt).unwrap();

    // Get updated columnar data
    let columnar = db.get_columnar("users").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 2, "Should still have 2 rows");

    // Verify alice's new score is visible
    let score_col = columnar.get_column("score").expect("Column should exist");
    let has_new_score = (0..2).any(|i| matches!(score_col.get(i), SqlValue::Integer(999)));
    assert!(
        has_new_score,
        "Alice's new score 999 should be visible after ON DUPLICATE KEY UPDATE on UNIQUE conflict"
    );

    let has_old_score = (0..2).any(|i| matches!(score_col.get(i), SqlValue::Integer(100)));
    assert!(
        !has_old_score,
        "Alice's old score 100 should NOT be visible after ON DUPLICATE KEY UPDATE"
    );
}

/// Test ON DUPLICATE KEY UPDATE with no conflict (just inserts) still works correctly with cache
#[test]
fn test_on_duplicate_key_update_no_conflict_works_with_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Existing", 100);

    // Warm cache
    let _ = db.get_columnar("products").unwrap();

    // ON DUPLICATE KEY UPDATE with different id (no conflict - just inserts)
    upsert_product(&mut db, 2, "New Item", 200);

    // Verify both rows are visible in columnar data
    let columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 2, "Should have 2 rows");

    let stock_col = columnar.get_column("stock").expect("Column should exist");
    let stocks: Vec<i64> = (0..2)
        .filter_map(|i| match stock_col.get(i) {
            SqlValue::Integer(v) => Some(v),
            _ => None,
        })
        .collect();

    assert!(stocks.contains(&100), "Original stock 100 should exist");
    assert!(stocks.contains(&200), "New stock 200 should exist");
}

/// Test ON DUPLICATE KEY UPDATE with arithmetic expression invalidates cache correctly
#[test]
fn test_on_duplicate_key_update_arithmetic_invalidates_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Widget", 10);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    let initial_stock_col = initial_columnar.get_column("stock").expect("Column should exist");
    assert_eq!(initial_stock_col.get(0), SqlValue::Integer(10));

    // ON DUPLICATE KEY UPDATE with arithmetic: stock = stock + VALUES(stock)
    let upsert_stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Widget"))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(30)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: Some(vec![vibesql_ast::Assignment {
            column: "stock".to_string(),
            value: vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Plus,
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "stock".to_string(),
                }),
                right: Box::new(vibesql_ast::Expression::DuplicateKeyValue {
                    column: "stock".to_string(),
                }),
            },
        }]),
    };
    InsertExecutor::execute(&mut db, &upsert_stmt).unwrap();

    // Get columnar data - should reflect the updated value (10 + 30 = 40)
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    let updated_stock_col = updated_columnar.get_column("stock").expect("Column should exist");

    assert_eq!(
        updated_stock_col.get(0),
        SqlValue::Integer(40),
        "Stock should be 10 + 30 = 40 after arithmetic ON DUPLICATE KEY UPDATE"
    );
}
