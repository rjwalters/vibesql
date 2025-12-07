// ============================================================================
// SQL Dump Tests
// ============================================================================

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::Database;

#[test]
fn test_save_sql_dump() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("test").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_db.sql";
    db.save_sql_dump(path).unwrap();

    // Verify file exists and contains expected content
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
    assert!(content.contains("Alice"));
    assert!(content.contains("Bob"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_value_to_literal() {
    use crate::persistence::save::sql_value_to_literal;

    assert_eq!(sql_value_to_literal(&SqlValue::Null), "NULL");
    assert_eq!(sql_value_to_literal(&SqlValue::Integer(42)), "42");
    assert_eq!(sql_value_to_literal(&SqlValue::Varchar(arcstr::ArcStr::from("test"))), "'test'");
    assert_eq!(sql_value_to_literal(&SqlValue::Varchar(arcstr::ArcStr::from("test's"))), "'test''s'");
    assert_eq!(sql_value_to_literal(&SqlValue::Boolean(true)), "TRUE");
    assert_eq!(sql_value_to_literal(&SqlValue::Boolean(false)), "FALSE");
}

#[test]
fn test_sql_dump_with_nulls() {
    let mut db = Database::new();

    // Create test schema with nullable column
    let schema = TableSchema::new(
        "test_nulls".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "nullable_col".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with NULL values
    let table = db.get_table_mut("test_nulls").unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("text"))]))
        .unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(3), SqlValue::Null])).unwrap();

    // Save SQL dump
    let path = "/tmp/test_nulls.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains NULL literal
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("NULL"), "SQL dump should contain NULL literal");
    assert!(content.contains("INSERT INTO test_nulls VALUES (1, NULL)"));
    assert!(content.contains("INSERT INTO test_nulls VALUES (2, 'text')"));
    assert!(content.contains("INSERT INTO test_nulls VALUES (3, NULL)"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_with_quotes() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test_quotes".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "text".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with various quote types
    let table = db.get_table_mut("test_quotes").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("it's"))]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("say \"hello\"")),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("it's a \"test\"")),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar(arcstr::ArcStr::from(""))]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_quotes.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL properly escapes quotes
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("'it''s'"), "Single quotes should be escaped as ''");
    assert!(
        content.contains("'say \"hello\"'"),
        "Double quotes should be preserved in single-quoted strings"
    );
    assert!(content.contains("'it''s a \"test\"'"), "Mixed quotes should be handled correctly");
    assert!(content.contains("''"), "Empty string should be represented");

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_empty_database() {
    let db = Database::new();

    // Save SQL dump of empty database
    let path = "/tmp/test_empty_db.sql";
    db.save_sql_dump(path).unwrap();

    // Verify file exists and contains valid SQL structure
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("-- VibeSQL Database Dump"));
    assert!(content.contains("-- Schemas"));
    assert!(content.contains("-- Roles"));
    assert!(content.contains("-- Tables and Data"));
    assert!(content.contains("-- Indexes"));
    assert!(content.contains("-- End of dump"));

    // Verify no CREATE TABLE statements in empty database
    assert!(!content.contains("CREATE TABLE"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_empty_table() {
    let mut db = Database::new();

    // Create test schema but insert no data
    let schema = TableSchema::new(
        "empty_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Save SQL dump
    let path = "/tmp/test_empty_table.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains CREATE TABLE but no INSERT statements
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("CREATE TABLE empty_table"));
    assert!(
        !content.contains("INSERT INTO empty_table"),
        "Empty table should have no INSERT statements"
    );

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_with_indexes() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test_indexes".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Create indexes (use fully qualified table name)
    let idx1 = vibesql_ast::IndexColumn {
        column_name: "name".to_string(),
        direction: vibesql_ast::OrderDirection::Asc,
        prefix_length: None,
    };
    db.create_index("idx_name".to_string(), "public.test_indexes".to_string(), false, vec![idx1])
        .unwrap();

    let idx2 = vibesql_ast::IndexColumn {
        column_name: "email".to_string(),
        direction: vibesql_ast::OrderDirection::Asc,
        prefix_length: None,
    };
    db.create_index(
        "idx_email_unique".to_string(),
        "public.test_indexes".to_string(),
        true,
        vec![idx2],
    )
    .unwrap();

    // Save SQL dump
    let path = "/tmp/test_indexes.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains CREATE INDEX statements
    let content = std::fs::read_to_string(path).unwrap();
    // Index names are normalized to uppercase
    assert!(content.contains("CREATE INDEX IDX_NAME"), "Should contain CREATE INDEX IDX_NAME");
    assert!(
        content.contains("CREATE UNIQUE INDEX IDX_EMAIL_UNIQUE"),
        "Should contain CREATE UNIQUE INDEX IDX_EMAIL_UNIQUE"
    );
    assert!(content.contains("ON public.test_indexes"), "Should reference test_indexes table");
    assert!(content.contains("Asc"), "Index direction should be included");

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_all_data_types() {
    let mut db = Database::new();

    // Create test schema with all data types
    let schema = TableSchema::new(
        "all_types".to_string(),
        vec![
            // Integer types
            ColumnSchema::new("col_int".to_string(), DataType::Integer, true),
            ColumnSchema::new("col_smallint".to_string(), DataType::Smallint, true),
            ColumnSchema::new("col_bigint".to_string(), DataType::Bigint, true),
            // Float types
            ColumnSchema::new("col_float".to_string(), DataType::Float { precision: 24 }, true),
            ColumnSchema::new("col_real".to_string(), DataType::Real, true),
            ColumnSchema::new("col_double".to_string(), DataType::DoublePrecision, true),
            // String types
            ColumnSchema::new(
                "col_varchar".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("col_char".to_string(), DataType::Character { length: 10 }, true),
            // Boolean
            ColumnSchema::new("col_bool".to_string(), DataType::Boolean, true),
            // Numeric
            ColumnSchema::new(
                "col_numeric".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                true,
            ),
            ColumnSchema::new(
                "col_decimal".to_string(),
                DataType::Decimal { precision: 5, scale: 0 },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with various values
    let table = db.get_table_mut("all_types").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(42),
            SqlValue::Smallint(100),
            SqlValue::Bigint(999999),
            SqlValue::Float(3.14),
            SqlValue::Real(2.718),
            SqlValue::Double(1.414),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
            SqlValue::Character(arcstr::ArcStr::from("fixed")),
            SqlValue::Boolean(true),
            SqlValue::Numeric(123.45),
            SqlValue::Numeric(999.0),
        ]))
        .unwrap();

    // Test special float values (NaN, Infinity)
    table
        .insert(crate::Row::new(vec![
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Float(f32::NAN),
            SqlValue::Real(f32::INFINITY),
            SqlValue::Double(f64::NEG_INFINITY),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Boolean(false),
            SqlValue::Null,
            SqlValue::Null,
        ]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_all_types.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains all data type declarations
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("INTEGER"));
    assert!(content.contains("SMALLINT"));
    assert!(content.contains("BIGINT"));
    assert!(content.contains("FLOAT(24)"));
    assert!(content.contains("REAL"));
    assert!(content.contains("DOUBLE PRECISION"));
    assert!(content.contains("VARCHAR(100)"));
    assert!(content.contains("CHAR(10)"));
    assert!(content.contains("BOOLEAN"));
    assert!(content.contains("NUMERIC(10, 2)"));
    assert!(content.contains("DECIMAL(5, 0)"));

    // Verify special float values
    assert!(content.contains("'NaN'"));
    assert!(content.contains("'Infinity'"));
    assert!(content.contains("'-Infinity'"));
    assert!(content.contains("TRUE"));
    assert!(content.contains("FALSE"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_large_dataset() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "large_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "value".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert 10,000 rows
    let table = db.get_table_mut("large_table").unwrap();
    for i in 0..10000 {
        table
            .insert(crate::Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("value_{}", i))),
            ]))
            .unwrap();
    }

    // Save SQL dump and measure
    let path = "/tmp/test_large.sql";
    let start = std::time::Instant::now();
    db.save_sql_dump(path).unwrap();
    let duration = start.elapsed();

    // Verify export completed in reasonable time (should be fast)
    assert!(duration.as_secs() < 10, "Export took too long: {:?}", duration);

    // Verify file exists and has content
    let metadata = std::fs::metadata(path).unwrap();
    assert!(metadata.len() > 0, "File should have content");

    // Verify row count in file
    let content = std::fs::read_to_string(path).unwrap();
    let insert_count = content.matches("INSERT INTO large_table").count();
    assert_eq!(insert_count, 10000, "Should have 10,000 INSERT statements");

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_read_sql_dump() {
    use crate::persistence::load::read_sql_dump;

    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test_load".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("test_load").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Test"))]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_read_dump.sql";
    db.save_sql_dump(path).unwrap();

    // Read the dump back
    let content = read_sql_dump(path).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
    assert!(content.contains("Test"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_read_sql_dump_file_not_found() {
    use crate::persistence::load::read_sql_dump;

    let result = read_sql_dump("/tmp/nonexistent_file_xyz123.sql");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}
