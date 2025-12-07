use super::*;

#[test]
fn test_row_creation() {
    let row = Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    assert_eq!(row.len(), 2);
    assert_eq!(row.get(0), Some(&vibesql_types::SqlValue::Integer(1)));
}

#[test]
fn test_table_creation() {
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    let table = Table::new(schema);
    assert_eq!(table.row_count(), 0);
    assert_eq!(table.schema.name, "users");
}

#[test]
fn test_table_insert() {
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    let mut table = Table::new(schema);

    let row = Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);

    let result = table.insert(row);
    assert!(result.is_ok());
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_table_insert_wrong_column_count() {
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    let mut table = Table::new(schema);

    // Only 1 value when table expects 2
    let row = Row::new(vec![vibesql_types::SqlValue::Integer(1)]);

    let result = table.insert(row);
    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::ColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
        }
        _ => panic!("Expected ColumnCountMismatch error"),
    }
}

#[test]
fn test_table_scan() {
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );
    let mut table = Table::new(schema);

    table.insert(Row::new(vec![vibesql_types::SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![vibesql_types::SqlValue::Integer(2)])).unwrap();
    table.insert(Row::new(vec![vibesql_types::SqlValue::Integer(3)])).unwrap();

    let rows = table.scan();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get(0), Some(&vibesql_types::SqlValue::Integer(1)));
    assert_eq!(rows[1].get(0), Some(&vibesql_types::SqlValue::Integer(2)));
    assert_eq!(rows[2].get(0), Some(&vibesql_types::SqlValue::Integer(3)));
}

#[test]
fn test_database_create_table() {
    let mut db = Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );

    let result = db.create_table(schema);
    assert!(result.is_ok());
    assert!(db.catalog.table_exists("users"));
    assert!(db.get_table("users").is_some());
}

#[test]
fn test_database_insert_row() {
    let mut db = Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let row = Row::new(vec![vibesql_types::SqlValue::Integer(1)]);
    let result = db.insert_row("users", row);
    assert!(result.is_ok());

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_database_insert_into_nonexistent_table() {
    let mut db = Database::new();
    let row = Row::new(vec![vibesql_types::SqlValue::Integer(1)]);
    let result = db.insert_row("missing", row);

    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::TableNotFound(name) => assert_eq!(name, "missing"),
        _ => panic!("Expected TableNotFound error"),
    }
}

#[test]
fn test_database_drop_table() {
    let mut db = Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    assert!(db.catalog.table_exists("users"));

    let result = db.drop_table("users");
    assert!(result.is_ok());
    assert!(!db.catalog.table_exists("users"));
    assert!(db.get_table("users").is_none());
}

#[test]
fn test_database_multiple_tables() {
    let mut db = Database::new();

    // Create users table
    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(users_schema).unwrap();

    // Create orders table
    let orders_schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(orders_schema).unwrap();

    // Insert into both
    db.insert_row("users", Row::new(vec![vibesql_types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("orders", Row::new(vec![vibesql_types::SqlValue::Integer(100)])).unwrap();

    assert_eq!(db.get_table("users").unwrap().row_count(), 1);
    assert_eq!(db.get_table("orders").unwrap().row_count(), 1);

    let tables = db.list_tables();
    assert_eq!(tables.len(), 2);
}

// ========================================================================
// Diagnostic Tools Tests
// ========================================================================

#[test]
fn test_debug_info() {
    let mut db = Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    let debug = db.debug_info();
    assert!(debug.contains("Database Debug Info"));
    assert!(debug.contains("Tables: 1"));
    assert!(debug.contains("users"));
    assert!(debug.contains("1 rows"));
    assert!(debug.contains("2 columns"));
}

#[test]
fn test_dump_table() {
    let mut db = Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();

    let dump = db.dump_table("users").unwrap();
    assert!(dump.contains("Table: users"));
    assert!(dump.contains("id | name"));
    assert!(dump.contains("1 | Alice"));
    assert!(dump.contains("2 | Bob"));
    assert!(dump.contains("(2 rows)"));
}

#[test]
fn test_dump_table_not_found() {
    let db = Database::new();
    let result = db.dump_table("missing");
    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::TableNotFound(name) => assert_eq!(name, "missing"),
        _ => panic!("Expected TableNotFound error"),
    }
}

#[test]
fn test_dump_tables() {
    let mut db = Database::new();

    // Create and populate users table
    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row("users", Row::new(vec![vibesql_types::SqlValue::Integer(1)])).unwrap();

    // Create and populate orders table
    let orders_schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(orders_schema).unwrap();
    db.insert_row("orders", Row::new(vec![vibesql_types::SqlValue::Integer(100)])).unwrap();

    let dump = db.dump_tables();
    assert!(dump.contains("Table: users") || dump.contains("Table: orders"));
    assert!(dump.contains("1") || dump.contains("100"));
}
// Tests for selective index update optimization

#[test]
fn test_update_row_selective_non_indexed_column() {
    // Create table with primary key and unique constraint
    let schema = vibesql_catalog::TableSchema::with_primary_key_and_unique(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "email".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
        vec!["id".to_string()],
        vec![vec!["email".to_string()]],
    );
    let mut table = Table::new(schema);

    // Insert initial row
    let row1 = Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    table.insert_row(row1).unwrap();

    // Update only the 'name' column (non-indexed)
    let updated_row = Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")),
    ]);
    let mut changed_columns = std::collections::HashSet::new();
    changed_columns.insert(2); // 'name' column index

    table.update_row_selective(0, updated_row, &changed_columns).unwrap();

    // Verify row was updated
    let row = table.scan().iter().next().unwrap();
    assert_eq!(row.get(2), Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith"))));
}

#[test]
fn test_update_row_selective_primary_key_column() {
    // Create table with primary key
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
        vec!["id".to_string()],
    );
    let mut table = Table::new(schema);

    // Insert initial rows
    table
        .insert_row(Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]))
        .unwrap();
    table
        .insert_row(Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]))
        .unwrap();

    // Update primary key column
    let updated_row = Row::new(vec![
        vibesql_types::SqlValue::Integer(10), // Changed PK
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    let mut changed_columns = std::collections::HashSet::new();
    changed_columns.insert(0); // 'id' column index

    table.update_row_selective(0, updated_row, &changed_columns).unwrap();

    // Verify primary key index was updated
    assert_eq!(table.row_count(), 2);
    let row = table.scan().iter().next().unwrap();
    assert_eq!(row.get(0), Some(&vibesql_types::SqlValue::Integer(10)));
}

#[test]
fn test_update_row_selective_unique_constraint_column() {
    // Create table with unique constraint
    let schema = vibesql_catalog::TableSchema::with_unique_constraints(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "email".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
        vec![vec!["email".to_string()]],
    );
    let mut table = Table::new(schema);

    // Insert initial rows
    table
        .insert_row(Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]))
        .unwrap();

    // Update unique constraint column
    let updated_row = Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice.smith@example.com")), // Changed email
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    let mut changed_columns = std::collections::HashSet::new();
    changed_columns.insert(1); // 'email' column index

    table.update_row_selective(0, updated_row, &changed_columns).unwrap();

    // Verify unique index was updated
    let row = table.scan().iter().next().unwrap();
    assert_eq!(
        row.get(1),
        Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice.smith@example.com")))
    );
}

#[test]
fn test_update_row_selective_vs_full_correctness() {
    // Verify both methods produce the same result
    let schema1 = vibesql_catalog::TableSchema::with_primary_key_and_unique(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "email".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
        vec!["id".to_string()],
        vec![vec!["email".to_string()]],
    );
    let mut table1 = Table::new(schema1.clone());

    let schema2 = schema1.clone();
    let mut table2 = Table::new(schema2);

    // Insert same initial row into both tables
    let initial_row = Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    table1.insert_row(initial_row.clone()).unwrap();
    table2.insert_row(initial_row).unwrap();

    // Update with selective method
    let updated_row1 = Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")),
    ]);
    let mut changed_columns = std::collections::HashSet::new();
    changed_columns.insert(2); // 'name' column
    table1.update_row_selective(0, updated_row1.clone(), &changed_columns).unwrap();

    // Update with full method
    table2.update_row(0, updated_row1).unwrap();

    // Both tables should be identical
    let row1 = table1.scan().iter().next().unwrap();
    let row2 = table2.scan().iter().next().unwrap();
    assert_eq!(row1.get(0), row2.get(0));
    assert_eq!(row1.get(1), row2.get(1));
    assert_eq!(row1.get(2), row2.get(2));
}
