//! Tests for DELETE + columnar cache invalidation
//!
//! This module tests that DELETE operations correctly invalidate the columnar cache,
//! ensuring that subsequent reads via `Database::get_columnar()` return the updated data
//! rather than stale cached data.
//!
//! Related: #3884, #3912

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::DeleteExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Sets up a PRODUCTS table for DELETE testing
fn setup_products_table(db: &mut Database) {
    let schema = TableSchema::with_primary_key(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
            ColumnSchema::new("PRICE".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()], // ID is PRIMARY KEY
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a row into PRODUCTS table
fn insert_product(db: &mut Database, id: i64, name: &str, price: i64) {
    db.insert_row(
        "PRODUCTS",
        vibesql_storage::Row::new(vec![
            SqlValue::Integer(id),
            SqlValue::Varchar(arcstr::ArcStr::from(name)),
            SqlValue::Integer(price),
        ]),
    )
    .unwrap();
}

/// Helper to execute a DELETE statement via SQL
fn delete_sql(db: &mut Database, sql: &str) -> usize {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Delete(delete_stmt) = stmt {
        DeleteExecutor::execute(&delete_stmt, db).unwrap()
    } else {
        panic!("Expected DELETE statement");
    }
}

/// Regression test for issue #3912:
/// Verify that DELETE correctly invalidates the database-level columnar cache.
///
/// This test:
/// 1. Creates a table and populates it with data
/// 2. Warms the columnar cache via `database.get_columnar()`
/// 3. Executes a DELETE statement that removes rows
/// 4. Verifies the columnar cache returns the correct data (deleted rows should not appear)
#[test]
fn test_delete_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Widget", 100);
    insert_product(&mut db, 2, "Gadget", 200);
    insert_product(&mut db, 3, "Gizmo", 300);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 3);

    // Verify initial cache statistics
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1, "Should have done one conversion");

    // Verify initial data in the columnar cache
    let price_col = initial_columnar.get_column("PRICE").expect("Column should exist");
    let prices: Vec<i64> = (0..3)
        .map(|i| match price_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    assert_eq!(prices, vec![100, 200, 300], "Initial prices should be 100, 200, 300");

    // Execute DELETE - remove the first row
    let deleted = delete_sql(&mut db, "DELETE FROM products WHERE id = 1");
    assert_eq!(deleted, 1, "Should delete 1 row");

    // Get columnar data again - should reflect the updated data (without deleted row)
    let updated_columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");

    // After DELETE: row 1 (Widget, 100) is removed
    assert_eq!(updated_columnar.row_count(), 2, "Should have 2 rows after DELETE");

    let updated_price_col = updated_columnar.get_column("PRICE").expect("Column should exist");
    let updated_prices: Vec<i64> = (0..2)
        .map(|i| match updated_price_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();

    // The deleted row's price (100) should no longer exist
    assert!(
        !updated_prices.contains(&100),
        "Deleted price 100 should no longer exist after DELETE"
    );
    assert!(
        updated_prices.contains(&200),
        "Unchanged price 200 should still exist"
    );
    assert!(
        updated_prices.contains(&300),
        "Unchanged price 300 should still exist"
    );

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after DELETE (conversions: {})",
        final_stats.conversions
    );
}

/// Test that DELETE invalidates cache even when using pre_warm_columnar_cache
#[test]
fn test_delete_invalidates_prewarmed_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Item A", 50);
    insert_product(&mut db, 2, "Item B", 75);
    insert_product(&mut db, 3, "Item C", 100);

    // Pre-warm the cache
    let warmed = db.pre_warm_columnar_cache(&["PRODUCTS"]).unwrap();
    assert_eq!(warmed, 1, "Should warm 1 table");

    // Verify pre-warmed data
    let initial_columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 3);

    // Cache hit should have occurred for the second access
    let stats_after_get = db.columnar_cache_stats();
    assert_eq!(stats_after_get.hits, 1, "Second get should be a cache hit");

    // DELETE to remove Item A
    let deleted = delete_sql(&mut db, "DELETE FROM PRODUCTS WHERE ID = 1");
    assert_eq!(deleted, 1);

    // Get columnar data - should reflect updated values, not stale cache
    let updated_columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 2, "Should have 2 rows after DELETE");

    // Verify the deleted row's price is gone
    let price_col = updated_columnar.get_column("PRICE").expect("Column should exist");
    let has_deleted_price = (0..2).any(|i| matches!(price_col.get(i), SqlValue::Integer(50)));
    assert!(!has_deleted_price, "Deleted price 50 should NOT be visible after DELETE");

    let has_remaining_prices = (0..2).all(|i| {
        matches!(price_col.get(i), SqlValue::Integer(75) | SqlValue::Integer(100))
    });
    assert!(has_remaining_prices, "Remaining prices 75 and 100 should be visible");
}

/// Test that DELETE with WHERE clause deleting multiple rows invalidates cache
#[test]
fn test_delete_multiple_rows_invalidates_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data with varied prices
    insert_product(&mut db, 1, "Cheap 1", 10);
    insert_product(&mut db, 2, "Cheap 2", 20);
    insert_product(&mut db, 3, "Expensive", 500);
    insert_product(&mut db, 4, "Cheap 3", 30);

    // Warm cache
    let _ = db.get_columnar("PRODUCTS").unwrap();

    // Delete all cheap products (price < 100)
    let deleted = delete_sql(&mut db, "DELETE FROM PRODUCTS WHERE PRICE < 100");
    assert_eq!(deleted, 3, "Should delete 3 cheap products");

    // Verify cache reflects the deletion
    let columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 1, "Should have 1 row remaining");

    let price_col = columnar.get_column("PRICE").expect("Column should exist");
    match price_col.get(0) {
        SqlValue::Integer(500) => {} // Expected
        other => panic!("Expected price 500, got {:?}", other),
    }
}

/// Test that DELETE FROM (truncate-style) invalidates cache
#[test]
fn test_delete_all_rows_invalidates_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Widget", 100);
    insert_product(&mut db, 2, "Gadget", 200);

    // Warm cache
    let initial = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(initial.row_count(), 2);

    // Delete all rows (no WHERE clause - triggers truncate optimization)
    let deleted = delete_sql(&mut db, "DELETE FROM PRODUCTS");
    assert_eq!(deleted, 2, "Should delete all 2 rows");

    // Verify cache reflects the empty table
    let columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 0, "Table should be empty after DELETE");
}

/// Test that DELETE followed by INSERT shows correct data in cache
#[test]
fn test_delete_then_insert_cache_consistency() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Original", 100);
    insert_product(&mut db, 2, "Keeper", 200);

    // Warm cache
    let _ = db.get_columnar("PRODUCTS").unwrap();

    // Delete one row
    let deleted = delete_sql(&mut db, "DELETE FROM PRODUCTS WHERE ID = 1");
    assert_eq!(deleted, 1);

    // Insert a new row
    insert_product(&mut db, 3, "Newcomer", 300);

    // Verify cache shows correct state
    let columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 2, "Should have 2 rows (1 deleted, 1 kept, 1 added)");

    let price_col = columnar.get_column("PRICE").expect("Column should exist");
    let prices: Vec<i64> = (0..2)
        .filter_map(|i| match price_col.get(i) {
            SqlValue::Integer(v) => Some(v),
            _ => None,
        })
        .collect();

    assert!(!prices.contains(&100), "Deleted price 100 should not exist");
    assert!(prices.contains(&200), "Kept price 200 should exist");
    assert!(prices.contains(&300), "New price 300 should exist");
}

/// Test DELETE with complex WHERE condition invalidates cache
#[test]
fn test_delete_complex_where_invalidates_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert varied data
    insert_product(&mut db, 1, "Widget A", 150);
    insert_product(&mut db, 2, "Widget B", 250);
    insert_product(&mut db, 3, "Gadget A", 150);
    insert_product(&mut db, 4, "Gadget B", 350);

    // Warm cache
    let initial = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(initial.row_count(), 4);

    // Delete rows matching complex condition: PRICE = 150 AND NAME LIKE 'Widget%'
    let deleted = delete_sql(&mut db, "DELETE FROM PRODUCTS WHERE PRICE = 150 AND NAME LIKE 'Widget%'");
    assert_eq!(deleted, 1, "Should delete 1 row (Widget A)");

    // Verify cache reflects the deletion
    let columnar = db.get_columnar("PRODUCTS").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 3, "Should have 3 rows remaining");

    // Gadget A with price 150 should still exist
    let price_col = columnar.get_column("PRICE").expect("Column should exist");
    let name_col = columnar.get_column("NAME").expect("Column should exist");

    // Check that we still have a row with price 150 (Gadget A)
    let has_gadget_a = (0..3).any(|i| {
        matches!(price_col.get(i), SqlValue::Integer(150))
            && matches!(name_col.get(i), SqlValue::Varchar(ref n) if n.as_str() == "Gadget A")
    });
    assert!(has_gadget_a, "Gadget A with price 150 should still exist");

    // Widget A should be gone
    let has_widget_a = (0..3).any(|i| {
        matches!(name_col.get(i), SqlValue::Varchar(ref n) if n.as_str() == "Widget A")
    });
    assert!(!has_widget_a, "Widget A should have been deleted");
}
