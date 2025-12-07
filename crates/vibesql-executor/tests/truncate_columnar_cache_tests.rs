//! Tests for TRUNCATE TABLE + columnar cache invalidation
//!
//! This module tests that TRUNCATE TABLE operations correctly invalidate the columnar cache,
//! ensuring that subsequent reads via `Database::get_columnar()` return empty data
//! rather than stale cached data.
//!
//! Related: #3932

use vibesql_ast::{TruncateCascadeOption, TruncateTableStmt};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::TruncateTableExecutor;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Sets up a products table for TRUNCATE testing
fn setup_products_table(db: &mut Database) {
    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a row into products table
fn insert_product(db: &mut Database, id: i64, name: &str, price: i64) {
    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(id),
            SqlValue::Varchar(name.to_string()),
            SqlValue::Integer(price),
        ]),
    )
    .unwrap();
}

/// Regression test for issue #3932:
/// Verify that TRUNCATE TABLE correctly invalidates the database-level columnar cache.
///
/// This test:
/// 1. Creates a table and populates it with data
/// 2. Warms the columnar cache via `database.get_columnar()`
/// 3. Executes TRUNCATE TABLE
/// 4. Verifies the columnar cache returns empty data (not stale cached data)
#[test]
fn test_truncate_invalidates_columnar_cache() {
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
    let price_col = initial_columnar.get_column("price").expect("Column should exist");
    let prices: Vec<i64> = (0..3)
        .map(|i| match price_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    assert_eq!(prices, vec![100, 200, 300], "Initial prices should be 100, 200, 300");

    // Execute TRUNCATE TABLE
    let stmt = TruncateTableStmt {
        table_names: vec!["products".to_string()],
        if_exists: false,
        cascade: None,
    };
    let deleted = TruncateTableExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 3, "Should have truncated 3 rows");

    // Get columnar data again - should be empty, not stale cached data
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");

    // After TRUNCATE: table should be empty
    assert_eq!(
        updated_columnar.row_count(),
        0,
        "Columnar cache should return 0 rows after TRUNCATE, not stale data"
    );

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after TRUNCATE (conversions: {})",
        final_stats.conversions
    );
}

/// Test that TRUNCATE invalidates cache even when using pre_warm_columnar_cache
#[test]
fn test_truncate_invalidates_prewarmed_cache() {
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

    // Execute TRUNCATE TABLE
    let stmt = TruncateTableStmt {
        table_names: vec!["products".to_string()],
        if_exists: false,
        cascade: None,
    };
    TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

    // Get columnar data - should be empty, not stale cache
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(
        updated_columnar.row_count(),
        0,
        "Prewarmed cache should be invalidated after TRUNCATE"
    );
}

/// Test that inserting after TRUNCATE returns correct data in columnar cache
#[test]
fn test_truncate_then_insert_updates_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Old Item", 100);

    // Warm the columnar cache
    let _ = db.get_columnar("products").unwrap();

    // TRUNCATE
    let stmt = TruncateTableStmt {
        table_names: vec!["products".to_string()],
        if_exists: false,
        cascade: None,
    };
    TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

    // Insert new data
    insert_product(&mut db, 2, "New Item", 999);

    // Get columnar data - should show new data only
    let columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 1, "Should have 1 new row");

    let price_col = columnar.get_column("price").expect("Column should exist");
    let price = match price_col.get(0) {
        SqlValue::Integer(v) => v,
        other => panic!("Expected Integer, got {:?}", other),
    };

    assert_eq!(price, 999, "New price 999 should be visible");
}

/// Test that TRUNCATE on empty table still works correctly with cache
#[test]
fn test_truncate_empty_table_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Warm the columnar cache with empty table
    let initial_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 0);

    // TRUNCATE empty table
    let stmt = TruncateTableStmt {
        table_names: vec!["products".to_string()],
        if_exists: false,
        cascade: None,
    };
    let deleted = TruncateTableExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 0);

    // Get columnar data again - should still be empty
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 0);
}

/// Test TRUNCATE with CASCADE also invalidates columnar cache
#[test]
fn test_truncate_cascade_invalidates_columnar_cache() {
    let mut db = Database::new();

    // Create parent table
    let parent_schema = TableSchema::with_primary_key(
        "categories".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(parent_schema).unwrap();

    // Create child table with ON DELETE CASCADE
    let child_schema = TableSchema::with_foreign_keys(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("category_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
        ],
        vec![vibesql_catalog::ForeignKeyConstraint {
            name: Some("fk_items_categories".to_string()),
            column_names: vec!["category_id".to_string()],
            column_indices: vec![1],
            parent_table: "categories".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: vibesql_catalog::ReferentialAction::Cascade,
            on_update: vibesql_catalog::ReferentialAction::NoAction,
        }],
    );
    db.create_table(child_schema).unwrap();

    // Insert data into parent
    db.insert_row(
        "categories",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Electronics".to_string()),
        ]),
    )
    .unwrap();

    // Insert data into child
    db.insert_row(
        "items",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(500),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Integer(300),
        ]),
    )
    .unwrap();

    // Warm the columnar cache for both tables
    let parent_columnar = db.get_columnar("categories").unwrap().expect("Table should exist");
    assert_eq!(parent_columnar.row_count(), 1);

    let child_columnar = db.get_columnar("items").unwrap().expect("Table should exist");
    assert_eq!(child_columnar.row_count(), 2);

    // TRUNCATE with CASCADE
    let stmt = TruncateTableStmt {
        table_names: vec!["categories".to_string()],
        if_exists: false,
        cascade: Some(TruncateCascadeOption::Cascade),
    };
    TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

    // Both tables should show empty in columnar cache
    let parent_after = db.get_columnar("categories").unwrap().expect("Table should exist");
    assert_eq!(
        parent_after.row_count(),
        0,
        "Parent columnar cache should be empty after CASCADE TRUNCATE"
    );

    let child_after = db.get_columnar("items").unwrap().expect("Table should exist");
    assert_eq!(
        child_after.row_count(),
        0,
        "Child columnar cache should be empty after CASCADE TRUNCATE"
    );
}
