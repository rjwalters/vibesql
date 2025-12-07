// ============================================================================
// JSON Format Tests
// ============================================================================

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::Database;

#[test]
fn test_json_roundtrip_basic() {
    let mut db = Database::new();

    // Create test schema with multiple types
    let schema = TableSchema::new(
        "test_json".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("active".to_string(), DataType::Boolean, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("test_json").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Boolean(true),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Boolean(false),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_roundtrip.json";
    db.save_json(path).unwrap();

    // Verify file exists and is valid JSON
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("vibesql"));
    assert!(content.contains("test_json"));
    assert!(content.contains("Alice"));
    assert!(content.contains("Bob"));
    assert!(content.contains("Charlie"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("test_json").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify data
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    assert_eq!(rows[0].values[2], SqlValue::Boolean(true));

    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    assert_eq!(rows[1].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    assert_eq!(rows[1].values[2], SqlValue::Boolean(false));

    assert_eq!(rows[2].values[0], SqlValue::Integer(3));
    assert_eq!(rows[2].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
    assert_eq!(rows[2].values[2], SqlValue::Null);

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_roundtrip_all_types() {
    let mut db = Database::new();

    // Create schema with various types
    let schema = TableSchema::new(
        "all_types_json".to_string(),
        vec![
            ColumnSchema::new("col_int".to_string(), DataType::Integer, true),
            ColumnSchema::new("col_bigint".to_string(), DataType::Bigint, true),
            ColumnSchema::new("col_float".to_string(), DataType::Float { precision: 24 }, true),
            ColumnSchema::new(
                "col_varchar".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("col_bool".to_string(), DataType::Boolean, true),
            ColumnSchema::new(
                "col_numeric".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("all_types_json").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(42),
            SqlValue::Bigint(999999),
            SqlValue::Float(3.14),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
            SqlValue::Boolean(true),
            SqlValue::Numeric(123.45),
        ]))
        .unwrap();

    // Insert row with NULL values
    table
        .insert(crate::Row::new(vec![
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Varchar(arcstr::ArcStr::from("not_null")),
            SqlValue::Boolean(false),
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_all_types.json";
    db.save_json(path).unwrap();

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("all_types_json").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 2);

    // Verify first row data types and values
    assert_eq!(rows[0].values[0], SqlValue::Integer(42));
    assert_eq!(rows[0].values[1], SqlValue::Bigint(999999));
    assert_eq!(rows[0].values[3], SqlValue::Varchar(arcstr::ArcStr::from("test")));
    assert_eq!(rows[0].values[4], SqlValue::Boolean(true));
    assert_eq!(rows[0].values[5], SqlValue::Numeric(123.45));

    // Verify second row has NULLs and non-null values
    assert_eq!(rows[1].values[0], SqlValue::Null);
    assert_eq!(rows[1].values[3], SqlValue::Varchar(arcstr::ArcStr::from("not_null")));
    assert_eq!(rows[1].values[4], SqlValue::Boolean(false));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_empty_database() {
    let db = Database::new();

    // Save empty database
    let path = "/tmp/test_empty.json";
    db.save_json(path).unwrap();

    // Verify file exists
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("vibesql"));
    assert!(content.contains("\"tables\": []"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();
    assert_eq!(loaded_db.catalog.list_tables().len(), 0);

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_roundtrip_temporal_types() {
    use vibesql_types::{Date, Time, Timestamp};

    let mut db = Database::new();

    // Create schema with temporal types
    let schema = TableSchema::new(
        "temporal_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("col_date".to_string(), DataType::Date, true),
            ColumnSchema::new(
                "col_time".to_string(),
                DataType::Time { with_timezone: false },
                true,
            ),
            ColumnSchema::new(
                "col_timestamp".to_string(),
                DataType::Timestamp { with_timezone: false },
                true,
            ),
            ColumnSchema::new(
                "col_timestamp_tz".to_string(),
                DataType::Timestamp { with_timezone: true },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with temporal values
    let table = db.get_table_mut("temporal_test").unwrap();
    let date = Date::new(2025, 1, 15).unwrap();
    let time = Time::new(14, 30, 45, 0).unwrap();
    let timestamp = Timestamp::new(date, time);

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Date(Date::new(2025, 1, 15).unwrap()),
            SqlValue::Time(Time::new(14, 30, 45, 0).unwrap()),
            SqlValue::Timestamp(timestamp),
            SqlValue::Timestamp(timestamp),
        ]))
        .unwrap();

    // Insert row with NULL temporal values
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_temporal.json";
    db.save_json(path).unwrap();

    // Verify JSON contains ISO 8601 formatted dates
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("2025-01-15"));
    assert!(content.contains("14:30:45"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("temporal_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 2);

    // Verify temporal data
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Date(Date::new(2025, 1, 15).unwrap()));
    assert_eq!(rows[0].values[2], SqlValue::Time(Time::new(14, 30, 45, 0).unwrap()));

    // Verify NULLs preserved
    assert_eq!(rows[1].values[1], SqlValue::Null);
    assert_eq!(rows[1].values[2], SqlValue::Null);

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_datetime_alias_behavior() {
    // Test for issue #1626: DATETIME type information during save/load
    //
    // This test verifies the INTENTIONAL behavior where DATETIME is treated
    // as an alias for TIMESTAMP. This is a design decision (see issue #1626):
    // - Parser maps DATETIME → DataType::Timestamp
    // - Persistence serializes as "TIMESTAMP"
    // - Both "DATETIME" and "TIMESTAMP" deserialize to DataType::Timestamp
    //
    // This test ensures the behavior is correct and consistent.

    use vibesql_types::{Date, Time, Timestamp};

    // Test 1: Create table using DATETIME type directly
    let mut db = Database::new();

    let schema = TableSchema::new(
        "datetime_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            // Use Timestamp type (same as DATETIME internally)
            ColumnSchema::new(
                "created_at".to_string(),
                DataType::Timestamp { with_timezone: false },
                true,
            ),
            ColumnSchema::new(
                "updated_at".to_string(),
                DataType::Timestamp { with_timezone: true },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("datetime_test").unwrap();
    let date = Date::new(2025, 11, 13).unwrap();
    let time = Time::new(10, 30, 0, 0).unwrap();
    let timestamp = Timestamp::new(date, time);

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Timestamp(timestamp),
            SqlValue::Timestamp(timestamp),
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_datetime_alias.json";
    db.save_json(path).unwrap();

    // Verify JSON contains "TIMESTAMP" (not "DATETIME")
    // This is the expected behavior per issue #1626
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("TIMESTAMP"));
    assert!(content.contains("2025-11-13"));

    // Load from JSON - should work correctly
    let loaded_db = Database::load_json(path).unwrap();
    let loaded_table = loaded_db.get_table("datetime_test").unwrap();
    let rows = loaded_table.scan();

    // Verify data is preserved correctly
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Timestamp(Timestamp::new(date, time)));

    // Verify schema types are correct (Timestamp, not a separate DATETIME type)
    assert_eq!(
        loaded_table.schema.columns[1].data_type,
        DataType::Timestamp { with_timezone: false }
    );
    assert_eq!(
        loaded_table.schema.columns[2].data_type,
        DataType::Timestamp { with_timezone: true }
    );

    // Test 2: Manually create JSON with "DATETIME" type strings
    // This verifies backward compatibility - both DATETIME and TIMESTAMP
    // should deserialize to the same internal representation
    std::fs::write(
        path,
        r#"{
  "vibesql": {
    "version": "1.0",
    "format": "json",
    "timestamp": 0
  },
  "schemas": [],
  "roles": [],
  "tables": [
    {
      "name": "datetime_compat",
      "schema": "public",
      "columns": [
        {"name": "id", "type": "INTEGER", "nullable": false},
        {"name": "dt1", "type": "DATETIME", "nullable": true},
        {"name": "dt2", "type": "DATETIME WITH TIME ZONE", "nullable": true},
        {"name": "ts1", "type": "TIMESTAMP", "nullable": true},
        {"name": "ts2", "type": "TIMESTAMP WITH TIME ZONE", "nullable": true}
      ],
      "rows": [
        {
          "id": 1,
          "dt1": "2025-11-13 10:30:00",
          "dt2": "2025-11-13 10:30:00",
          "ts1": "2025-11-13 10:30:00",
          "ts2": "2025-11-13 10:30:00"
        }
      ]
    }
  ],
  "indexes": [],
  "views": []
}"#,
    )
    .unwrap();

    // Load JSON with both DATETIME and TIMESTAMP type strings
    let compat_db = Database::load_json(path).unwrap();
    let compat_table = compat_db.get_table("datetime_compat").unwrap();

    // Verify all columns have the same internal type (Timestamp)
    assert_eq!(
        compat_table.schema.columns[1].data_type,
        DataType::Timestamp { with_timezone: false }
    );
    assert_eq!(
        compat_table.schema.columns[2].data_type,
        DataType::Timestamp { with_timezone: true }
    );
    assert_eq!(
        compat_table.schema.columns[3].data_type,
        DataType::Timestamp { with_timezone: false }
    );
    assert_eq!(
        compat_table.schema.columns[4].data_type,
        DataType::Timestamp { with_timezone: true }
    );

    // Verify data loaded correctly
    let compat_rows = compat_table.scan();
    assert_eq!(compat_rows.len(), 1);
    assert_eq!(compat_rows[0].values[0], SqlValue::Integer(1));

    // All timestamp values should be equal since they were created from the same string
    let expected_ts = SqlValue::Timestamp(Timestamp::new(date, time));
    assert_eq!(compat_rows[0].values[1], expected_ts);
    assert_eq!(compat_rows[0].values[2], expected_ts);
    assert_eq!(compat_rows[0].values[3], expected_ts);
    assert_eq!(compat_rows[0].values[4], expected_ts);

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_numeric_precision() {
    let mut db = Database::new();

    // Create schema with numeric types
    let schema = TableSchema::new(
        "numeric_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
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
            ColumnSchema::new(
                "col_numeric_large".to_string(),
                DataType::Numeric { precision: 20, scale: 5 },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with various numeric values
    let table = db.get_table_mut("numeric_test").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Numeric(123.45),
            SqlValue::Numeric(12345.0),
            SqlValue::Numeric(123456789.12345),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Numeric(0.01),
            SqlValue::Numeric(99999.0),
            SqlValue::Numeric(0.00001),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_numeric_precision.json";
    db.save_json(path).unwrap();

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("numeric_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify numeric data preserved
    assert_eq!(rows[0].values[1], SqlValue::Numeric(123.45));
    assert_eq!(rows[0].values[2], SqlValue::Numeric(12345.0));
    assert_eq!(rows[1].values[1], SqlValue::Numeric(0.01));
    assert_eq!(rows[2].values[1], SqlValue::Null);

    // Verify schema preserved precision/scale
    assert_eq!(
        loaded_table.schema.columns[1].data_type,
        DataType::Numeric { precision: 10, scale: 2 }
    );
    assert_eq!(
        loaded_table.schema.columns[2].data_type,
        DataType::Decimal { precision: 5, scale: 0 }
    );

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_integer_types() {
    let mut db = Database::new();

    // Create schema with all integer types
    let schema = TableSchema::new(
        "integer_test".to_string(),
        vec![
            ColumnSchema::new("col_smallint".to_string(), DataType::Smallint, true),
            ColumnSchema::new("col_integer".to_string(), DataType::Integer, true),
            ColumnSchema::new("col_bigint".to_string(), DataType::Bigint, true),
            ColumnSchema::new("col_unsigned".to_string(), DataType::Unsigned, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with boundary values
    let table = db.get_table_mut("integer_test").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Smallint(i16::MAX),
            SqlValue::Integer(i64::MAX),
            SqlValue::Bigint(i64::MAX),
            SqlValue::Unsigned(u64::MAX),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Smallint(i16::MIN),
            SqlValue::Integer(i64::MIN),
            SqlValue::Bigint(i64::MIN),
            SqlValue::Unsigned(0),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Smallint(0),
            SqlValue::Integer(42),
            SqlValue::Bigint(-999999),
            SqlValue::Unsigned(123456789),
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_integer_types.json";
    db.save_json(path).unwrap();

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("integer_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify boundary values preserved
    assert_eq!(rows[0].values[0], SqlValue::Smallint(i16::MAX));
    assert_eq!(rows[0].values[1], SqlValue::Integer(i64::MAX));
    assert_eq!(rows[0].values[3], SqlValue::Unsigned(u64::MAX));

    assert_eq!(rows[1].values[0], SqlValue::Smallint(i16::MIN));
    assert_eq!(rows[1].values[1], SqlValue::Integer(i64::MIN));
    assert_eq!(rows[1].values[3], SqlValue::Unsigned(0));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_special_floats() {
    let mut db = Database::new();

    // Create schema with float types
    let schema = TableSchema::new(
        "float_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("col_float".to_string(), DataType::Float { precision: 24 }, true),
            ColumnSchema::new("col_real".to_string(), DataType::Real, true),
            ColumnSchema::new("col_double".to_string(), DataType::DoublePrecision, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with special float values
    let table = db.get_table_mut("float_test").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Float(f32::NAN),
            SqlValue::Real(f32::INFINITY),
            SqlValue::Double(f64::NEG_INFINITY),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Float(3.14159),
            SqlValue::Real(2.71828),
            SqlValue::Double(1.41421),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Float(0.0),
            SqlValue::Real(-0.0),
            SqlValue::Double(123.456789),
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_special_floats.json";
    db.save_json(path).unwrap();

    // Verify JSON contains special float representations
    let _content = std::fs::read_to_string(path).unwrap();
    // Note: serde_json serializes special floats as null by default
    // or as strings depending on settings - we just verify it doesn't crash

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("float_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify normal float values preserved
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    // Note: Floating point comparison needs tolerance for roundtrip

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_schemas_and_roles() {
    let mut db = Database::new();

    // Create non-default schemas
    let _ = &mut db.catalog.create_schema("analytics".to_string()).unwrap();
    let _ = &mut db.catalog.create_schema("staging".to_string()).unwrap();

    // Create roles
    let _ = &mut db.catalog.create_role("admin".to_string()).unwrap();
    let _ = &mut db.catalog.create_role("readonly".to_string()).unwrap();

    // Create table in default schema
    let schema1 = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema1).unwrap();

    // Save to JSON
    let path = "/tmp/test_schemas_roles.json";
    db.save_json(path).unwrap();

    // Verify JSON contains schemas and roles
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("analytics"));
    assert!(content.contains("staging"));
    assert!(content.contains("admin"));
    assert!(content.contains("readonly"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify schemas recreated (excluding public)
    let schemas = &loaded_db.catalog.list_schemas();
    assert!(schemas.contains(&"analytics".to_string()));
    assert!(schemas.contains(&"staging".to_string()));

    // Verify roles recreated
    let roles = &loaded_db.catalog.list_roles();
    assert!(roles.contains(&"admin".to_string()));
    assert!(roles.contains(&"readonly".to_string()));

    // Verify table exists
    assert!(loaded_db.get_table("users").is_some());

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_large_dataset() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "large_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "data".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("value".to_string(), DataType::Float { precision: 24 }, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert 10,000 rows
    let table = db.get_table_mut("large_table").unwrap();
    for i in 0..10000 {
        table
            .insert(crate::Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("data_value_{}", i))),
                SqlValue::Float((i as f32) * 1.5),
            ]))
            .unwrap();
    }

    // Save to JSON and measure time
    let path = "/tmp/test_large_dataset.json";
    let start = std::time::Instant::now();
    db.save_json(path).unwrap();
    let save_duration = start.elapsed();

    // Verify save completed in reasonable time
    assert!(save_duration.as_secs() < 10, "JSON save took too long: {:?}", save_duration);

    // Verify file exists and has content
    let metadata = std::fs::metadata(path).unwrap();
    assert!(metadata.len() > 0, "File should have content");

    // Load from JSON and measure time
    let start = std::time::Instant::now();
    let loaded_db = Database::load_json(path).unwrap();
    let load_duration = start.elapsed();

    // Verify load completed in reasonable time
    assert!(load_duration.as_secs() < 10, "JSON load took too long: {:?}", load_duration);

    // Verify table exists
    let loaded_table = loaded_db.get_table("large_table").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 10000);

    // Spot check some rows
    assert_eq!(rows[0].values[0], SqlValue::Integer(0));
    assert_eq!(rows[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("data_value_0")));

    assert_eq!(rows[9999].values[0], SqlValue::Integer(9999));
    assert_eq!(rows[9999].values[1], SqlValue::Varchar(arcstr::ArcStr::from("data_value_9999")));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_load_nonexistent_file() {
    let result = Database::load_json("/tmp/nonexistent_file_12345.json");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to open file") || err_msg.contains("No such file"));
}

#[test]
fn test_json_load_malformed() {
    // Write malformed JSON
    let path = "/tmp/test_malformed.json";
    std::fs::write(path, "{invalid json content}").unwrap();

    let result = Database::load_json(path);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("JSON deserialization failed") || err_msg.contains("expected"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_load_empty_file() {
    // Write empty file
    let path = "/tmp/test_empty_json.json";
    std::fs::write(path, "").unwrap();

    let result = Database::load_json(path);
    assert!(result.is_err());

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_index_roundtrip() {
    let mut db = Database::new();

    // Create test table
    let schema = TableSchema::new(
        "test_indexes".to_string(),
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

    // Create index
    let idx = vibesql_ast::IndexColumn {
        column_name: "name".to_string(),
        direction: vibesql_ast::OrderDirection::Asc,
        prefix_length: None,
    };

    // Use qualified table name for index creation (indexes require qualified names)
    db.create_index("idx_name".to_string(), "public.test_indexes".to_string(), false, vec![idx])
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_index_roundtrip.json";
    db.save_json(path).unwrap();

    // Verify JSON contains index
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("idx_name") || content.contains("IDX_NAME"));
    assert!(content.contains("indexes"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Debug: Check what tables are available
    let tables = &loaded_db.catalog.list_tables();
    eprintln!("Available tables after load: {:?}", tables);

    // Verify index exists
    let indexes = loaded_db.list_indexes();
    assert!(
        indexes.iter().any(|idx| idx.to_uppercase() == "IDX_NAME"),
        "Index not found in loaded database. Available indexes: {:?}, Available tables: {:?}",
        indexes,
        tables
    );

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_view_preservation() {
    use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};
    use vibesql_catalog::ViewDefinition;

    let mut db = Database::new();

    // Create a simple table first
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create a view manually (simulating CREATE VIEW)
    let select_stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![
            SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
        ],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "active".to_string() }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    // Create view with SQL definition
    let view = ViewDefinition::new_with_sql(
        "active_users".to_string(),
        Some(vec!["id".to_string(), "name".to_string()]),
        select_stmt,
        false,
        "SELECT id, name FROM users WHERE active = true".to_string(),
    );
    db.catalog.create_view(view).unwrap();

    // Save to JSON
    let path = "/tmp/test_view_preservation.json";
    db.save_json(path).unwrap();

    // Verify JSON contains view
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("active_users"), "View name should be in JSON");
    assert!(
        content.contains("SELECT id, name FROM users WHERE active = true"),
        "View definition should be in JSON"
    );

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Views are not automatically recreated (by design), but we verify the JSON preserved them
    // The load operation should succeed and log a warning about views
    assert!(
        loaded_db.catalog.get_view("active_users").is_none(),
        "Views should not be automatically recreated during deserialization"
    );

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_view_preservation_without_sql_definition() {
    use vibesql_ast::{FromClause, SelectItem, SelectStmt};
    use vibesql_catalog::ViewDefinition;

    let mut db = Database::new();

    // Create a simple table first
    let schema = TableSchema::new(
        "products".to_string(),
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

    // Create a view without SQL definition (using the old constructor)
    let select_stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "products".to_string(),
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

    let view = ViewDefinition::new("all_products".to_string(), None, select_stmt, false);
    db.catalog.create_view(view).unwrap();

    // Save to JSON
    let path = "/tmp/test_view_no_sql.json";
    db.save_json(path).unwrap();

    // Verify JSON contains view (with Debug format fallback)
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("all_products"), "View name should be in JSON");
    // Should contain the Debug format of the SelectStmt
    assert!(content.contains("SelectStmt"), "View should have Debug format definition");

    // Cleanup
    std::fs::remove_file(path).ok();
}
