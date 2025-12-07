//! Tests for INSERT + columnar cache invalidation
//!
//! This module tests that INSERT operations correctly invalidate the columnar cache,
//! ensuring that subsequent reads via `Database::get_columnar()` return the updated data
//! rather than stale cached data.
//!
//! Related: #3915

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::InsertExecutor;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Sets up a products table for INSERT testing
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

/// Regression test for issue #3915:
/// Verify that INSERT correctly invalidates the database-level columnar cache.
///
/// This test:
/// 1. Creates a table and populates it with initial data
/// 2. Warms the columnar cache via `database.get_columnar()`
/// 3. Executes an INSERT statement that adds new rows
/// 4. Verifies the columnar cache returns the updated data (not stale cached data)
#[test]
fn test_insert_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Widget", 100);
    insert_product(&mut db, 2, "Gadget", 200);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 2);

    // Verify initial cache statistics
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1, "Should have done one conversion");

    // Verify initial data in the columnar cache
    let price_col = initial_columnar.get_column("price").expect("Column should exist");
    let prices: Vec<i64> = (0..2)
        .map(|i| match price_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    assert_eq!(prices, vec![100, 200], "Initial prices should be 100, 200");

    // Insert a new row - this should invalidate the cache
    insert_product(&mut db, 3, "Gizmo", 300);

    // Get columnar data again - should reflect the updated data
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 3, "Should have 3 rows after insert");

    let updated_price_col = updated_columnar.get_column("price").expect("Column should exist");
    let updated_prices: Vec<i64> = (0..3)
        .map(|i| match updated_price_col.get(i) {
            SqlValue::Integer(v) => v,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();

    // The new price 300 should be visible
    assert!(
        updated_prices.contains(&300),
        "New price 300 should exist after INSERT"
    );
    assert!(
        updated_prices.contains(&100),
        "Existing price 100 should still exist"
    );
    assert!(
        updated_prices.contains(&200),
        "Existing price 200 should still exist"
    );

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after INSERT (conversions: {})",
        final_stats.conversions
    );
}

/// Test that INSERT invalidates cache even when using pre_warm_columnar_cache
#[test]
fn test_insert_invalidates_prewarmed_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Item A", 50);

    // Pre-warm the cache
    let warmed = db.pre_warm_columnar_cache(&["products"]).unwrap();
    assert_eq!(warmed, 1, "Should warm 1 table");

    // Verify pre-warmed data
    let initial_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 1);

    // Cache hit should have occurred for the second access
    let stats_after_get = db.columnar_cache_stats();
    assert_eq!(stats_after_get.hits, 1, "Second get should be a cache hit");

    // INSERT a new row
    insert_product(&mut db, 2, "Item B", 75);

    // Get columnar data - should reflect updated values, not stale cache
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 2, "Should have 2 rows");

    // Verify the new row is present
    let price_col = updated_columnar.get_column("price").expect("Column should exist");
    let has_new_price = (0..2).any(|i| matches!(price_col.get(i), SqlValue::Integer(75)));
    assert!(has_new_price, "New price 75 should be visible after INSERT");
}

/// Test multi-row INSERT invalidates cache correctly
#[test]
fn test_multi_row_insert_invalidates_cache() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Insert initial data
    insert_product(&mut db, 1, "Initial", 100);

    // Warm cache
    let _ = db.get_columnar("products").unwrap();
    let stats_before = db.columnar_cache_stats();

    // Multi-row INSERT
    let stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Product 2"))),
                vibesql_ast::Expression::Literal(SqlValue::Integer(200)),
            ],
            vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Product 3"))),
                vibesql_ast::Expression::Literal(SqlValue::Integer(300)),
            ],
            vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(4)),
                vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Product 4"))),
                vibesql_ast::Expression::Literal(SqlValue::Integer(400)),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Get updated columnar data
    let columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(columnar.row_count(), 4, "Should have 4 rows after multi-row INSERT");

    // Verify all prices are visible
    let price_col = columnar.get_column("price").expect("Column should exist");
    let prices: Vec<i64> = (0..4)
        .filter_map(|i| match price_col.get(i) {
            SqlValue::Integer(v) => Some(v),
            _ => None,
        })
        .collect();

    assert!(prices.contains(&100), "Original price 100 should exist");
    assert!(prices.contains(&200), "New price 200 should exist");
    assert!(prices.contains(&300), "New price 300 should exist");
    assert!(prices.contains(&400), "New price 400 should exist");

    // Cache should have been invalidated
    let stats_after = db.columnar_cache_stats();
    assert!(
        stats_after.conversions > stats_before.conversions,
        "Should have re-converted after multi-row INSERT"
    );
}

/// Test INSERT INTO ... SELECT invalidates cache (bulk transfer path)
#[test]
fn test_insert_select_invalidates_cache() {
    let mut db = Database::new();

    // Create source table
    let source_schema = TableSchema::new(
        "source_products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(source_schema).unwrap();

    // Create destination table
    setup_products_table(&mut db);

    // Populate source table
    let insert_source = |db: &mut Database, id: i64, name: &str, price: i64| {
        let stmt = vibesql_ast::InsertStmt {
            table_name: "source_products".to_string(),
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
    };

    insert_source(&mut db, 1, "Source Item 1", 111);
    insert_source(&mut db, 2, "Source Item 2", 222);

    // Warm destination table cache (empty)
    let initial_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 0, "Destination should start empty");

    // INSERT INTO products SELECT * FROM source_products
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "source_products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Select(Box::new(select_stmt)),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Get updated columnar data - should show the inserted rows
    let updated_columnar = db.get_columnar("products").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 2, "Should have 2 rows after INSERT SELECT");

    // Verify prices from source are visible
    let price_col = updated_columnar.get_column("price").expect("Column should exist");
    let has_price_111 = (0..2).any(|i| matches!(price_col.get(i), SqlValue::Integer(111)));
    let has_price_222 = (0..2).any(|i| matches!(price_col.get(i), SqlValue::Integer(222)));

    assert!(has_price_111, "Price 111 from source should be visible");
    assert!(has_price_222, "Price 222 from source should be visible");
}
