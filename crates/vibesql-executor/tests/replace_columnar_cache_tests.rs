//! Tests for REPLACE + columnar cache invalidation
//!
//! This module tests that REPLACE operations correctly invalidate the columnar cache,
//! ensuring that subsequent reads via `Database::get_columnar()` return the updated data
//! rather than stale cached data.
//!
//! Related: #3884, #3890, #3891

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::InsertExecutor;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Sets up a products table with PRIMARY KEY for REPLACE testing
fn setup_products_table(db: &mut Database) {
    let schema = TableSchema::with_primary_key(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
        ],
        vec!["id".to_string()], // id is PRIMARY KEY
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a row into products table
fn insert_product(db: &mut Database, id: i64, name: &str, price: i64) {
    let stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(id)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(name))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(price)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(db, &stmt).unwrap();
}

/// Helper to execute a REPLACE statement
fn replace_product(db: &mut Database, id: i64, name: &str, price: i64) {
    let stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(id)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(name))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(price)),
        ]]),
        conflict_clause: Some(vibesql_ast::ConflictClause::Replace),
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(db, &stmt).unwrap();
}

/// Regression test for issue #3884 / PR #3890:
/// Verify that REPLACE correctly invalidates the database-level columnar cache.
///
/// This test:
/// 1. Creates a table and populates it with data
/// 2. Warms the columnar cache via `database.get_columnar()`
/// 3. Executes a REPLACE statement that modifies existing rows
/// 4. Verifies the columnar cache returns the updated data (not stale cached data)
#[test]
fn test_replace_invalidates_columnar_cache() {
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
    // Prices should be 100, 200, 300 (in order of insertion)
    let prices: Vec<i64> = (0..3)
        .map(|i| match price_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    assert_eq!(prices, vec![100, 200, 300], "Initial prices should be 100, 200, 300");

    // Execute REPLACE - this should delete row id=1 and insert a new row with updated values
    replace_product(&mut db, 1, "Super Widget", 150);

    // Get columnar data again - should reflect the updated data
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");

    // After REPLACE: row 1 (old Widget) is deleted, new row with id=1 is inserted at the end
    // So the order should be: id=2 (Gadget, 200), id=3 (Gizmo, 300), id=1 (Super Widget, 150)
    assert_eq!(updated_columnar.row_count(), 3, "Should still have 3 rows");

    let updated_price_col = updated_columnar.get_column("price").expect("Column should exist");
    let updated_prices: Vec<i64> = (0..3)
        .map(|i| match updated_price_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();

    // The replaced row should now have price 150 instead of 100
    // After REPLACE, the row ordering may change depending on implementation
    // What matters is that the price 100 is gone and 150 is present
    assert!(
        !updated_prices.contains(&100),
        "Old price 100 should no longer exist after REPLACE"
    );
    assert!(
        updated_prices.contains(&150),
        "New price 150 should exist after REPLACE"
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
        "Should have re-converted after REPLACE (conversions: {})",
        final_stats.conversions
    );
}

/// Test that REPLACE invalidates cache even when using pre_warm_columnar_cache
#[test]
fn test_replace_invalidates_prewarmed_cache() {
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

    // REPLACE to change Item A's price
    replace_product(&mut db, 1, "Item A Updated", 999);

    // Get columnar data - should reflect updated values, not stale cache
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 2, "Should still have 2 rows");

    // Verify the new price is present
    let price_col = updated_columnar.get_column("price").expect("Column should exist");
    let has_new_price = (0..2).any(|i| matches!(price_col.get(i), SqlValue::Integer(999)));
    assert!(has_new_price, "New price 999 should be visible after REPLACE");

    let has_old_price = (0..2).any(|i| matches!(price_col.get(i), SqlValue::Integer(50)));
    assert!(!has_old_price, "Old price 50 should NOT be visible after REPLACE");
}

/// Test that REPLACE on UNIQUE constraint also invalidates cache
#[test]
fn test_replace_unique_constraint_invalidates_cache() {
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

    // REPLACE with same name (unique key conflict) - should delete old row and insert new
    let replace_stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)), // Different id
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("alice"))), // Same name
            vibesql_ast::Expression::Literal(SqlValue::Integer(999)), // New score
        ]]),
        conflict_clause: Some(vibesql_ast::ConflictClause::Replace),
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &replace_stmt).unwrap();

    // Get updated columnar data
    let columnar = db.get_columnar("users").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 2, "Should still have 2 rows");

    // Verify alice's new score is visible
    let score_col = columnar.get_column("score").expect("Column should exist");
    let has_new_score = (0..2).any(|i| matches!(score_col.get(i), SqlValue::Integer(999)));
    assert!(
        has_new_score,
        "Alice's new score 999 should be visible after REPLACE on UNIQUE conflict"
    );

    let has_old_score = (0..2).any(|i| matches!(score_col.get(i), SqlValue::Integer(100)));
    assert!(
        !has_old_score,
        "Alice's old score 100 should NOT be visible after REPLACE"
    );
}

/// Test REPLACE with no conflict (just inserts) still works correctly with cache
#[test]
fn test_replace_no_conflict_works_with_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Existing", 100);

    // Warm cache
    let _ = db.get_columnar("products").unwrap();

    // REPLACE with different id (no conflict - just inserts)
    replace_product(&mut db, 2, "New Item", 200);

    // Verify both rows are visible in columnar data
    let columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 2, "Should have 2 rows");

    let price_col = columnar.get_column("price").expect("Column should exist");
    let prices: Vec<i64> = (0..2)
        .filter_map(|i| match price_col.get(i) {
            SqlValue::Integer(v) => Some(v),
            _ => None,
        })
        .collect();

    assert!(prices.contains(&100), "Original price 100 should exist");
    assert!(prices.contains(&200), "New price 200 should exist");
}
