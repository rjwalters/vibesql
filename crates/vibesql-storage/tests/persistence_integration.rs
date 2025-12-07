use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{parse_sql_statements, read_sql_dump, Database};
use vibesql_types::DataType;

#[test]
fn test_database_save_and_load_roundtrip() {
    let temp_file = "/tmp/test_db_roundtrip.sql";

    // Clean up any existing test file
    let _ = std::fs::remove_file(temp_file);

    // Step 1: Create database with some tables and data
    let mut db = Database::new();

    // Create a schema
    let schema = TableSchema::new(
        "test_users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, true),
        ],
    );

    db.create_table(schema.clone()).unwrap();

    // Insert some rows using the table directly
    let table = db.get_table_mut("test_users").unwrap();
    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(30),
        ]))
        .unwrap();

    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Null,
        ]))
        .unwrap();

    // Step 2: Save database to SQL dump
    db.save_sql_dump(temp_file).unwrap();

    // Step 3: Verify file was created and has content
    assert!(std::path::Path::new(temp_file).exists(), "SQL dump file should exist");

    let content = std::fs::read_to_string(temp_file).unwrap();
    assert!(content.contains("CREATE TABLE test_users"), "SQL dump should contain CREATE TABLE");
    assert!(content.contains("Alice"), "SQL dump should contain inserted data");
    assert!(content.contains("Bob"), "SQL dump should contain inserted data");

    // Step 4: Load database from SQL dump using the load utilities
    let sql_content = read_sql_dump(temp_file).unwrap();
    let statements = parse_sql_statements(&sql_content).unwrap();

    // Verify we got the expected statements
    assert!(!statements.is_empty(), "Should have parsed statements from SQL dump");

    // Step 5: Verify we can parse the statements
    for (idx, stmt_sql) in statements.iter().enumerate() {
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        // Just verify it parses - we don't execute in this test
        let result = vibesql_parser::Parser::parse_sql(trimmed);
        assert!(
            result.is_ok(),
            "Statement {} should parse successfully: {}\nError: {:?}",
            idx,
            trimmed,
            result.err()
        );
    }

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_binary_format_roundtrip() {
    let temp_file = "/tmp/test_db_binary_roundtrip.vbsql";

    // Clean up any existing test file
    let _ = std::fs::remove_file(temp_file);

    // Step 1: Create database with schemas, tables, indexes, and data
    let mut db = Database::new();

    // Create a custom schema
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create first table with various data types
    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, true),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(users_schema).unwrap();

    // Create second table
    let products_schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "product_name".to_string(),
                DataType::Varchar { max_length: Some(200) },
                false,
            ),
            ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    // Insert data into users table
    let users_table = db.get_table_mut("users").unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(30),
            vibesql_types::SqlValue::Boolean(true),
        ]))
        .unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Null,
            vibesql_types::SqlValue::Boolean(false),
        ]))
        .unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Integer(25),
            vibesql_types::SqlValue::Boolean(true),
        ]))
        .unwrap();

    // Insert data into products table
    let products_table = db.get_table_mut("products").unwrap();
    products_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget")),
            vibesql_types::SqlValue::Double(19.99),
        ]))
        .unwrap();
    products_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Gadget")),
            vibesql_types::SqlValue::Double(29.99),
        ]))
        .unwrap();

    // Step 2: Save database to binary format
    db.save_binary(temp_file).unwrap();

    // Step 3: Verify file was created and has content
    assert!(std::path::Path::new(temp_file).exists(), "Binary file should exist");

    let metadata = std::fs::metadata(temp_file).unwrap();
    assert!(metadata.len() > 100, "Binary file should have substantial content");

    // Step 4: Load database from binary format
    let db2 = Database::load_binary(temp_file).unwrap();

    // Step 5: Verify all data was preserved

    // Check schema exists
    assert!(db2.catalog.list_schemas().contains(&"test_schema".to_string()));

    // Check tables exist
    let table_names = db2.catalog.list_tables();
    assert!(table_names.contains(&"users".to_string()));
    assert!(table_names.contains(&"products".to_string()));

    // Check users table structure and data
    let users_table2 = db2.get_table("users").unwrap();
    assert_eq!(users_table2.schema.columns.len(), 4);
    assert_eq!(users_table2.schema.columns[0].name, "id");
    assert_eq!(users_table2.schema.columns[1].name, "name");
    assert_eq!(users_table2.schema.columns[2].name, "age");
    assert_eq!(users_table2.schema.columns[3].name, "active");
    assert_eq!(users_table2.row_count(), 3);

    // Verify specific data values
    let users_rows = users_table2.scan();
    assert_eq!(users_rows[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(users_rows[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    assert_eq!(users_rows[0].values[2], vibesql_types::SqlValue::Integer(30));
    assert_eq!(users_rows[0].values[3], vibesql_types::SqlValue::Boolean(true));

    // Verify NULL handling
    assert_eq!(users_rows[1].values[2], vibesql_types::SqlValue::Null);

    // Check products table structure and data
    let products_table2 = db2.get_table("products").unwrap();
    assert_eq!(products_table2.schema.columns.len(), 3);
    assert_eq!(products_table2.row_count(), 2);

    let products_rows = products_table2.scan();
    assert_eq!(products_rows[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget")));
    assert_eq!(products_rows[0].values[2], vibesql_types::SqlValue::Double(19.99));

    // Step 6: Test auto-detection via Database::load()
    let db3 = Database::load(temp_file).unwrap();
    let users_table3 = db3.get_table("users").unwrap();
    assert_eq!(users_table3.row_count(), 3);

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_parse_sql_statements_with_comments() {
    let content = r#"
-- This is a comment
CREATE TABLE users (id INTEGER);

-- Another comment
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
    "#;

    let statements = parse_sql_statements(content).unwrap();

    // Should have 3 statements (CREATE TABLE, 2 INSERTs)
    // Line comments (--) are filtered out
    assert_eq!(statements.len(), 3, "Should parse 3 SQL statements");
}

#[test]
fn test_parse_sql_statements_with_string_literals() {
    let content = r#"
INSERT INTO users VALUES (1, 'Alice; Bob');
INSERT INTO users VALUES (2, "Charlie; Dave");
    "#;

    let statements = parse_sql_statements(content).unwrap();

    assert_eq!(statements.len(), 2, "Should parse 2 INSERT statements");
    assert!(statements[0].contains("Alice; Bob"), "Should preserve semicolons in string literals");
    assert!(statements[1].contains("Charlie; Dave"), "Should handle double-quoted strings");
}

#[test]
fn test_parse_multiline_create_table() {
    let content = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200)
);
    "#;

    let statements = parse_sql_statements(content).unwrap();

    assert_eq!(statements.len(), 1, "Should parse 1 CREATE TABLE statement");
    let stmt = &statements[0];
    assert!(stmt.contains("id INTEGER"), "Should preserve column definitions");
    assert!(stmt.contains("name VARCHAR"), "Should preserve all columns");
    assert!(stmt.contains("email VARCHAR"), "Should preserve all columns");
}

#[test]
fn test_compressed_binary_format_roundtrip() {
    let temp_file = "/tmp/test_db_compressed_roundtrip.vbsqlz";

    // Clean up any existing test file
    let _ = std::fs::remove_file(temp_file);

    // Create database with test data
    let mut db = Database::new();

    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(users_schema).unwrap();

    // Insert data
    let users_table = db.get_table_mut("users").unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(30),
        ]))
        .unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Null,
        ]))
        .unwrap();

    // Save in compressed format
    db.save_compressed(temp_file).unwrap();

    // Verify file was created
    assert!(std::path::Path::new(temp_file).exists(), "Compressed file should exist");

    // Load from compressed format
    let loaded_db = Database::load_compressed(temp_file).unwrap();

    // Verify table exists
    assert!(loaded_db.get_table("users").is_some(), "Users table should exist");

    // Verify data was loaded correctly
    let loaded_table = loaded_db.get_table("users").unwrap();
    assert_eq!(loaded_table.row_count(), 2, "Should have 2 rows");

    // Verify first row
    let rows = loaded_table.scan();
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    assert_eq!(rows[0].values[2], vibesql_types::SqlValue::Integer(30));

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_default_save_method_creates_compressed() {
    let temp_file = "/tmp/test_db_default_save.vbsqlz";

    // Clean up
    let _ = std::fs::remove_file(temp_file);

    // Create simple database
    let mut db = Database::new();
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Save using default method (should create compressed)
    db.save(temp_file).unwrap();

    // Verify file exists
    assert!(std::path::Path::new(temp_file).exists());

    // Load it back using auto-detection
    let loaded_db = Database::load(temp_file).unwrap();
    assert!(loaded_db.get_table("test").is_some());

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_compression_reduces_file_size() {
    let compressed_file = "/tmp/test_db_compressed.vbsqlz";
    let uncompressed_file = "/tmp/test_db_uncompressed.vbsql";

    // Clean up
    let _ = std::fs::remove_file(compressed_file);
    let _ = std::fs::remove_file(uncompressed_file);

    // Create database with some data
    let mut db = Database::new();
    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "data".to_string(),
                DataType::Varchar { max_length: Some(1000) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert repetitive data (compresses well)
    let table = db.get_table_mut("test").unwrap();
    for i in 0..100 {
        table
            .insert(vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("A".repeat(100))),
            ]))
            .unwrap();
    }

    // Save both formats
    db.save_compressed(compressed_file).unwrap();
    db.save_uncompressed(uncompressed_file).unwrap();

    // Get file sizes
    let compressed_size = std::fs::metadata(compressed_file).unwrap().len();
    let uncompressed_size = std::fs::metadata(uncompressed_file).unwrap().len();

    // Verify compression reduces size
    println!("Uncompressed: {} bytes, Compressed: {} bytes", uncompressed_size, compressed_size);
    assert!(compressed_size < uncompressed_size, "Compressed file should be smaller");
    assert!(
        compressed_size < uncompressed_size / 2,
        "Should compress by at least 50% with repetitive data"
    );

    // Clean up
    std::fs::remove_file(compressed_file).unwrap();
    std::fs::remove_file(uncompressed_file).unwrap();
}

#[test]
fn test_load_auto_detects_compressed_format() {
    let compressed_file = "/tmp/test_db_auto_detect.vbsqlz";
    let uncompressed_file = "/tmp/test_db_auto_detect.vbsql";

    // Clean up
    let _ = std::fs::remove_file(compressed_file);
    let _ = std::fs::remove_file(uncompressed_file);

    // Create database
    let mut db = Database::new();
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Save in both formats
    db.save_compressed(compressed_file).unwrap();
    db.save_uncompressed(uncompressed_file).unwrap();

    // Load both using generic load() method (should auto-detect)
    let loaded_compressed = Database::load(compressed_file).unwrap();
    let loaded_uncompressed = Database::load(uncompressed_file).unwrap();

    // Verify both loaded correctly
    assert!(loaded_compressed.get_table("test").is_some());
    assert!(loaded_uncompressed.get_table("test").is_some());

    // Clean up
    std::fs::remove_file(compressed_file).unwrap();
    std::fs::remove_file(uncompressed_file).unwrap();
}

#[test]
fn test_primary_key_persistence() {
    let temp_file = "/tmp/test_db_primary_key.vbsql";

    // Clean up any existing test file
    let _ = std::fs::remove_file(temp_file);

    // Create database with a primary key
    let mut db = Database::new();

    let schema = TableSchema::with_primary_key(
        "orders".to_string(),
        vec![
            ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "customer".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("total".to_string(), DataType::DoublePrecision, false),
        ],
        vec!["order_id".to_string()],
    );

    db.create_table(schema).unwrap();

    // Verify primary key is set before save
    let table_before = db.get_table("orders").unwrap();
    assert_eq!(
        table_before.schema.primary_key,
        Some(vec!["order_id".to_string()]),
        "Primary key should be set before save"
    );

    // Insert some test data
    let table = db.get_table_mut("orders").unwrap();
    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Double(99.99),
        ]))
        .unwrap();
    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Double(149.99),
        ]))
        .unwrap();

    // Save to binary format
    db.save_binary(temp_file).unwrap();

    // Load from binary format
    let db2 = Database::load_binary(temp_file).unwrap();

    // Verify primary key was preserved
    let table_after = db2.get_table("orders").unwrap();
    assert_eq!(
        table_after.schema.primary_key,
        Some(vec!["order_id".to_string()]),
        "Primary key should be preserved after load"
    );

    // Verify data was preserved
    assert_eq!(table_after.row_count(), 2);
    let rows = table_after.scan();
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], vibesql_types::SqlValue::Integer(2));

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_composite_primary_key_persistence() {
    let temp_file = "/tmp/test_db_composite_pk.vbsql";

    // Clean up
    let _ = std::fs::remove_file(temp_file);

    // Create database with composite primary key
    let mut db = Database::new();

    let schema = TableSchema::with_primary_key(
        "order_items".to_string(),
        vec![
            ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("line_number".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "product".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("quantity".to_string(), DataType::Integer, false),
        ],
        vec!["order_id".to_string(), "line_number".to_string()],
    );

    db.create_table(schema).unwrap();

    // Verify composite PK before save
    let table_before = db.get_table("order_items").unwrap();
    assert_eq!(
        table_before.schema.primary_key,
        Some(vec!["order_id".to_string(), "line_number".to_string()]),
        "Composite primary key should be set before save"
    );

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let db2 = Database::load_binary(temp_file).unwrap();

    // Verify composite PK was preserved
    let table_after = db2.get_table("order_items").unwrap();
    assert_eq!(
        table_after.schema.primary_key,
        Some(vec!["order_id".to_string(), "line_number".to_string()]),
        "Composite primary key should be preserved after load"
    );

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_no_primary_key_persistence() {
    let temp_file = "/tmp/test_db_no_pk.vbsql";

    // Clean up
    let _ = std::fs::remove_file(temp_file);

    // Create database without primary key
    let mut db = Database::new();

    let schema = TableSchema::new(
        "logs".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("message".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Verify no primary key before save
    let table_before = db.get_table("logs").unwrap();
    assert_eq!(table_before.schema.primary_key, None, "No primary key should be set");

    // Save and reload
    db.save_binary(temp_file).unwrap();
    let db2 = Database::load_binary(temp_file).unwrap();

    // Verify no primary key after load
    let table_after = db2.get_table("logs").unwrap();
    assert_eq!(table_after.schema.primary_key, None, "No primary key should be preserved as None");

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

/// Test that indexes are populated correctly after database save/load.
/// This is a regression test for issue #3602 where ORDER BY on indexed
/// columns returned empty results after loading a database from disk.
#[test]
fn test_index_data_populated_after_load() {
    let temp_file = "/tmp/test_db_index_after_load.vbsql";

    // Clean up any existing test file
    let _ = std::fs::remove_file(temp_file);

    // Step 1: Create database with index
    let mut db = Database::new();

    let schema = TableSchema::with_primary_key(
        "test_orders".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "customer".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("amount".to_string(), DataType::DoublePrecision, false),
        ],
        vec!["id".to_string()],
    );

    db.create_table(schema).unwrap();

    // Create an explicit index on the customer column
    db.create_index(
        "idx_customer".to_string(),
        "test_orders".to_string(),
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "customer".to_string(),
            direction: vibesql_ast::OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Insert test data
    let table = db.get_table_mut("test_orders").unwrap();
    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Double(100.0),
        ]))
        .unwrap();
    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Double(200.0),
        ]))
        .unwrap();
    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Double(300.0),
        ]))
        .unwrap();

    // Rebuild indexes for the table (simulating what happens during normal operations)
    db.rebuild_indexes("test_orders");

    // Verify index works before save
    let index_data_before = db.get_index_data("idx_customer").expect("Index should exist");
    let alice_lookup_before = index_data_before.get(&[vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
    assert!(
        alice_lookup_before.is_some() && !alice_lookup_before.unwrap().is_empty(),
        "Index should return results before save"
    );

    // Step 2: Save to binary format
    db.save_binary(temp_file).unwrap();

    // Step 3: Load from binary format
    let db2 = Database::load_binary(temp_file).unwrap();

    // Step 4: Verify index exists after load
    assert!(db2.index_exists("idx_customer"), "Index should exist after load");

    // Step 5: Verify index data is populated after load
    let index_data_after = db2.get_index_data("idx_customer").expect("Index data should exist");
    let alice_lookup_after = index_data_after.get(&[vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
    assert!(
        alice_lookup_after.is_some() && !alice_lookup_after.unwrap().is_empty(),
        "Index should return results after load (regression test for issue #3602)"
    );

    // Verify all entries are in the index
    let bob_lookup = index_data_after.get(&[vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]);
    let charlie_lookup =
        index_data_after.get(&[vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]);
    assert!(
        bob_lookup.is_some() && !bob_lookup.unwrap().is_empty(),
        "Index should contain Bob"
    );
    assert!(
        charlie_lookup.is_some() && !charlie_lookup.unwrap().is_empty(),
        "Index should contain Charlie"
    );

    // Step 6: Verify lookup_by_index works (high-level API)
    let rows = db2.lookup_by_index("idx_customer", &[vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
    assert!(rows.is_ok(), "lookup_by_index should succeed");
    let rows = rows.unwrap();
    assert!(rows.is_some(), "lookup_by_index should find Alice");
    let rows = rows.unwrap();
    assert_eq!(rows.len(), 1, "Should find exactly one row for Alice");
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}
