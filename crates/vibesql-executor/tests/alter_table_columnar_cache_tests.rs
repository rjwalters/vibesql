//! Tests for ALTER TABLE + columnar cache invalidation
//!
//! This module tests that ALTER TABLE operations correctly invalidate the columnar cache,
//! ensuring that subsequent reads via `Database::get_columnar()` return the updated data
//! rather than stale cached data with outdated schema or values.
//!
//! Related: #3933

use vibesql_ast::{
    AddColumnStmt, AlterColumnStmt, AlterTableStmt, ChangeColumnStmt, ColumnDef,
    DropColumnStmt, ModifyColumnStmt,
};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{AlterTableExecutor, InsertExecutor};
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Sets up a simple test table for ALTER TABLE testing
fn setup_test_table(db: &mut Database) {
    let schema = TableSchema::with_primary_key(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
            ColumnSchema::new("value".to_string(), DataType::Integer, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a row into test_table
fn insert_row(db: &mut Database, id: i64, name: &str, value: i64) {
    let stmt = vibesql_ast::InsertStmt {
        table_name: "test_table".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(id)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(name))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(value)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(db, &stmt).unwrap();
}

/// Test that ADD COLUMN invalidates the columnar cache
///
/// This verifies that after adding a column:
/// 1. The columnar cache is invalidated
/// 2. Subsequent reads return data with the new column
/// 3. The new column contains the default value
#[test]
fn test_add_column_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert initial data
    insert_row(&mut db, 1, "Widget", 100);
    insert_row(&mut db, 2, "Gadget", 200);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 2);
    assert_eq!(initial_columnar.column_count(), 3, "Should have 3 columns initially");

    // Verify initial cache statistics
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1, "Should have done one conversion");

    // ADD COLUMN
    let add_col_stmt = AlterTableStmt::AddColumn(AddColumnStmt {
        table_name: "test_table".to_string(),
        column_def: ColumnDef {
            name: "new_col".to_string(),
            data_type: DataType::Integer,
            nullable: true,
            default_value: Some(Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(42)))),
            constraints: vec![],
            comment: None,
        },
    });
    AlterTableExecutor::execute(&add_col_stmt, &mut db).unwrap();

    // Get columnar data again - should reflect the new column
    let updated_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");

    // Verify the new column is present
    assert_eq!(updated_columnar.column_count(), 4, "Should have 4 columns after ADD COLUMN");
    assert_eq!(updated_columnar.row_count(), 2, "Should still have 2 rows");

    // Verify the new column contains the default value
    let new_col = updated_columnar.get_column("new_col").expect("New column should exist");
    for i in 0..2 {
        match new_col.get(i) {
            SqlValue::Integer(v) => assert_eq!(v, 42, "Default value should be 42"),
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after ADD COLUMN (conversions: {})",
        final_stats.conversions
    );
}

/// Test that DROP COLUMN invalidates the columnar cache
///
/// This verifies that after dropping a column:
/// 1. The columnar cache is invalidated
/// 2. Subsequent reads return data without the dropped column
#[test]
fn test_drop_column_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert initial data
    insert_row(&mut db, 1, "Widget", 100);
    insert_row(&mut db, 2, "Gadget", 200);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.column_count(), 3, "Should have 3 columns initially");
    assert!(initial_columnar.get_column("value").is_some(), "value column should exist");

    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // DROP COLUMN
    let drop_col_stmt = AlterTableStmt::DropColumn(DropColumnStmt {
        table_name: "test_table".to_string(),
        column_name: "value".to_string(),
        if_exists: false,
    });
    AlterTableExecutor::execute(&drop_col_stmt, &mut db).unwrap();

    // Get columnar data again - should not have the dropped column
    let updated_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");

    // Verify the column was dropped
    assert_eq!(updated_columnar.column_count(), 2, "Should have 2 columns after DROP COLUMN");
    assert!(
        updated_columnar.get_column("value").is_none(),
        "value column should no longer exist"
    );
    assert!(updated_columnar.get_column("id").is_some(), "id column should still exist");
    assert!(updated_columnar.get_column("name").is_some(), "name column should still exist");

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after DROP COLUMN (conversions: {})",
        final_stats.conversions
    );
}

/// Test that MODIFY COLUMN invalidates the columnar cache
///
/// This verifies that after modifying a column's type:
/// 1. The columnar cache is invalidated
/// 2. Subsequent reads return data with the new type
#[test]
fn test_modify_column_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert initial data
    insert_row(&mut db, 1, "Widget", 100);
    insert_row(&mut db, 2, "Gadget", 200);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(initial_columnar.row_count(), 2);

    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // MODIFY COLUMN - change value from Integer to BigInt
    let modify_col_stmt = AlterTableStmt::ModifyColumn(ModifyColumnStmt {
        table_name: "test_table".to_string(),
        column_name: "value".to_string(),
        new_column_def: ColumnDef {
            name: "value".to_string(),
            data_type: DataType::Bigint,
            nullable: true,
            default_value: None,
            constraints: vec![],
            comment: None,
        },
    });
    AlterTableExecutor::execute(&modify_col_stmt, &mut db).unwrap();

    // Get columnar data again - should reflect the type change
    let updated_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 2, "Should still have 2 rows");

    // Verify values are accessible and correct
    let value_col = updated_columnar.get_column("value").expect("value column should exist");
    let values: Vec<i64> = (0..2)
        .filter_map(|i| match value_col.get(i) {
            SqlValue::Integer(v) | SqlValue::Bigint(v) => Some(v),
            _ => None,
        })
        .collect();
    assert!(values.contains(&100), "Value 100 should exist");
    assert!(values.contains(&200), "Value 200 should exist");

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after MODIFY COLUMN (conversions: {})",
        final_stats.conversions
    );
}

/// Test that CHANGE COLUMN (rename + modify) invalidates the columnar cache
///
/// This verifies that after changing a column's name and type:
/// 1. The columnar cache is invalidated
/// 2. Subsequent reads return data with the new column name
#[test]
fn test_change_column_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert initial data
    insert_row(&mut db, 1, "Widget", 100);
    insert_row(&mut db, 2, "Gadget", 200);

    // Warm the columnar cache
    let initial_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert!(initial_columnar.get_column("value").is_some(), "value column should exist");
    assert!(
        initial_columnar.get_column("amount").is_none(),
        "amount column should not exist yet"
    );

    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // CHANGE COLUMN - rename 'value' to 'amount' and change type
    let change_col_stmt = AlterTableStmt::ChangeColumn(ChangeColumnStmt {
        table_name: "test_table".to_string(),
        old_column_name: "value".to_string(),
        new_column_def: ColumnDef {
            name: "amount".to_string(),
            data_type: DataType::Bigint,
            nullable: true,
            default_value: None,
            constraints: vec![],
            comment: None,
        },
    });
    AlterTableExecutor::execute(&change_col_stmt, &mut db).unwrap();

    // Get columnar data again - should have new column name
    let updated_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");

    // Verify the column was renamed
    assert!(
        updated_columnar.get_column("value").is_none(),
        "old 'value' column should no longer exist"
    );
    assert!(
        updated_columnar.get_column("amount").is_some(),
        "new 'amount' column should exist"
    );

    // Verify values are preserved
    let amount_col = updated_columnar.get_column("amount").expect("amount column should exist");
    let amounts: Vec<i64> = (0..2)
        .filter_map(|i| match amount_col.get(i) {
            SqlValue::Integer(v) | SqlValue::Bigint(v) => Some(v),
            _ => None,
        })
        .collect();
    assert!(amounts.contains(&100), "Value 100 should exist in renamed column");
    assert!(amounts.contains(&200), "Value 200 should exist in renamed column");

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after CHANGE COLUMN (conversions: {})",
        final_stats.conversions
    );
}

/// Test that ALTER COLUMN SET NOT NULL invalidates the columnar cache
///
/// While SET NOT NULL only changes schema metadata, the columnar representation
/// may encode nullability, so we should invalidate for safety.
#[test]
fn test_alter_column_set_not_null_invalidates_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert data (no NULLs so SET NOT NULL will succeed)
    insert_row(&mut db, 1, "Widget", 100);
    insert_row(&mut db, 2, "Gadget", 200);

    // Warm the columnar cache
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // ALTER COLUMN SET NOT NULL
    let alter_stmt = AlterTableStmt::AlterColumn(AlterColumnStmt::SetNotNull {
        table_name: "test_table".to_string(),
        column_name: "value".to_string(),
    });
    AlterTableExecutor::execute(&alter_stmt, &mut db).unwrap();

    // Get columnar data again
    let updated_columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(updated_columnar.row_count(), 2);

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after ALTER COLUMN SET NOT NULL (conversions: {})",
        final_stats.conversions
    );
}

/// Test that ALTER COLUMN DROP NOT NULL invalidates the columnar cache
#[test]
fn test_alter_column_drop_not_null_invalidates_cache() {
    let mut db = Database::new();

    // Create table with NOT NULL column
    let schema = TableSchema::with_primary_key(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("required_val".to_string(), DataType::Integer, false), // NOT NULL
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert data
    let stmt = vibesql_ast::InsertStmt {
        table_name: "test_table".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(100)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Warm the columnar cache
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // ALTER COLUMN DROP NOT NULL
    let alter_stmt = AlterTableStmt::AlterColumn(AlterColumnStmt::DropNotNull {
        table_name: "test_table".to_string(),
        column_name: "required_val".to_string(),
    });
    AlterTableExecutor::execute(&alter_stmt, &mut db).unwrap();

    // Get columnar data again
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after ALTER COLUMN DROP NOT NULL (conversions: {})",
        final_stats.conversions
    );
}

/// Test that ALTER COLUMN SET DEFAULT invalidates the columnar cache
///
/// While SET DEFAULT only changes schema metadata, we invalidate for consistency.
#[test]
fn test_alter_column_set_default_invalidates_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    insert_row(&mut db, 1, "Widget", 100);

    // Warm the columnar cache
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // ALTER COLUMN SET DEFAULT
    let alter_stmt = AlterTableStmt::AlterColumn(AlterColumnStmt::SetDefault {
        table_name: "test_table".to_string(),
        column_name: "value".to_string(),
        default: vibesql_ast::Expression::Literal(SqlValue::Integer(999)),
    });
    AlterTableExecutor::execute(&alter_stmt, &mut db).unwrap();

    // Get columnar data again
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after ALTER COLUMN SET DEFAULT (conversions: {})",
        final_stats.conversions
    );
}

/// Test that ALTER COLUMN DROP DEFAULT invalidates the columnar cache
#[test]
fn test_alter_column_drop_default_invalidates_cache() {
    let mut db = Database::new();

    // Create table with default value
    let mut col_schema = ColumnSchema::new("value".to_string(), DataType::Integer, true);
    col_schema.set_default(vibesql_ast::Expression::Literal(SqlValue::Integer(0)));

    let schema = TableSchema::with_primary_key(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            col_schema,
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert data
    let stmt = vibesql_ast::InsertStmt {
        table_name: "test_table".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(100)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Warm the columnar cache
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // ALTER COLUMN DROP DEFAULT
    let alter_stmt = AlterTableStmt::AlterColumn(AlterColumnStmt::DropDefault {
        table_name: "test_table".to_string(),
        column_name: "value".to_string(),
    });
    AlterTableExecutor::execute(&alter_stmt, &mut db).unwrap();

    // Get columnar data again
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");

    // Verify cache was invalidated and re-converted
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 2,
        "Should have re-converted after ALTER COLUMN DROP DEFAULT (conversions: {})",
        final_stats.conversions
    );
}

/// Test that multiple ALTER operations in sequence properly invalidate cache
#[test]
fn test_multiple_alter_operations_invalidate_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    insert_row(&mut db, 1, "Widget", 100);
    insert_row(&mut db, 2, "Gadget", 200);

    // Warm cache
    let _ = db.get_columnar("test_table").unwrap();

    // ADD COLUMN
    let add_stmt = AlterTableStmt::AddColumn(AddColumnStmt {
        table_name: "test_table".to_string(),
        column_def: ColumnDef {
            name: "extra".to_string(),
            data_type: DataType::Integer,
            nullable: true,
            default_value: Some(Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(0)))),
            constraints: vec![],
            comment: None,
        },
    });
    AlterTableExecutor::execute(&add_stmt, &mut db).unwrap();

    // Check columnar has new column
    let columnar1 = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(columnar1.column_count(), 4);

    // DROP COLUMN (the original 'value' column)
    let drop_stmt = AlterTableStmt::DropColumn(DropColumnStmt {
        table_name: "test_table".to_string(),
        column_name: "value".to_string(),
        if_exists: false,
    });
    AlterTableExecutor::execute(&drop_stmt, &mut db).unwrap();

    // Check columnar reflects drop
    let columnar2 = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(columnar2.column_count(), 3);
    assert!(columnar2.get_column("value").is_none());
    assert!(columnar2.get_column("extra").is_some());

    // Final stats should show multiple conversions
    let final_stats = db.columnar_cache_stats();
    assert!(
        final_stats.conversions >= 3,
        "Should have multiple conversions after multiple ALTER operations (conversions: {})",
        final_stats.conversions
    );
}

/// Test that RENAME TABLE invalidates the columnar cache for both old and new names
#[test]
fn test_rename_table_invalidates_columnar_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    insert_row(&mut db, 1, "Widget", 100);
    insert_row(&mut db, 2, "Gadget", 200);

    // Warm the columnar cache for the old table name
    let _ = db.get_columnar("test_table").unwrap().expect("Table should exist");
    let initial_stats = db.columnar_cache_stats();
    assert_eq!(initial_stats.conversions, 1);

    // RENAME TABLE
    let rename_stmt = vibesql_ast::AlterTableStmt::RenameTable(vibesql_ast::RenameTableStmt {
        table_name: "test_table".to_string(),
        new_table_name: "renamed_table".to_string(),
    });
    AlterTableExecutor::execute(&rename_stmt, &mut db).unwrap();

    // Old table should no longer exist
    assert!(db.get_columnar("test_table").unwrap().is_none());

    // New table should have the data
    let renamed_columnar = db.get_columnar("renamed_table").unwrap().expect("Renamed table should exist");
    assert_eq!(renamed_columnar.row_count(), 2);
    assert_eq!(renamed_columnar.column_count(), 3);

    // Verify values are preserved
    let value_col = renamed_columnar.get_column("value").expect("value column should exist");
    let values: Vec<i64> = (0..2)
        .filter_map(|i| match value_col.get(i) {
            SqlValue::Integer(v) => Some(v),
            _ => None,
        })
        .collect();
    assert!(values.contains(&100), "Value 100 should exist after rename");
    assert!(values.contains(&200), "Value 200 should exist after rename");
}

/// Test pre-warmed cache is properly invalidated by ALTER TABLE
#[test]
fn test_alter_invalidates_prewarmed_cache() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    insert_row(&mut db, 1, "Widget", 100);

    // Pre-warm the cache
    let warmed = db.pre_warm_columnar_cache(&["test_table"]).unwrap();
    assert_eq!(warmed, 1);

    // Verify it's cached (second access should be a hit)
    let _ = db.get_columnar("test_table").unwrap();
    let stats_after_get = db.columnar_cache_stats();
    assert_eq!(stats_after_get.hits, 1, "Second get should be a cache hit");

    // ADD COLUMN to invalidate
    let add_stmt = AlterTableStmt::AddColumn(AddColumnStmt {
        table_name: "test_table".to_string(),
        column_def: ColumnDef {
            name: "new_col".to_string(),
            data_type: DataType::Integer,
            nullable: true,
            default_value: None,
            constraints: vec![],
            comment: None,
        },
    });
    AlterTableExecutor::execute(&add_stmt, &mut db).unwrap();

    // Should get fresh data with new column
    let columnar = db.get_columnar("test_table").unwrap().expect("Table should exist");
    assert_eq!(columnar.column_count(), 4, "Should have 4 columns after ADD");
    assert!(
        columnar.get_column("new_col").is_some(),
        "New column should be visible"
    );
}
