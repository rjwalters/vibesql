//! Tests for TRUNCATE optimization (DELETE FROM table with no WHERE)

use vibesql_ast::{DeleteStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
use vibesql_catalog::{
    ColumnSchema, ForeignKeyConstraint, ReferentialAction, TableSchema, TriggerDefinition,
};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::DeleteExecutor;

#[test]
fn test_truncate_optimization_basic() {
    let mut db = Database::new();

    // Create large table
    let schema = TableSchema::new(
        "large_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "data".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert many rows (would be slow with row-by-row deletion)
    for i in 0..1000 {
        db.insert_row(
            "large_table",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("data_{}", i)))]),
        )
        .unwrap();
    }

    assert_eq!(db.get_table("large_table").unwrap().row_count(), 1000);

    // DELETE FROM large_table (no WHERE) - should use TRUNCATE fast path
    let stmt =
        DeleteStmt { only: false, table_name: "large_table".to_string(), where_clause: None };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 1000);

    // Verify all rows deleted
    let table = db.get_table("large_table").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_truncate_blocked_by_fk_reference() {
    let mut db = Database::new();

    // Create parent table with primary key
    let parent_schema = TableSchema::with_primary_key(
        "parent".to_string(),
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

    // Create child table with foreign key referencing parent
    let child_schema = TableSchema::with_foreign_keys(
        "child".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("parent_id".to_string(), DataType::Integer, false),
        ],
        vec![ForeignKeyConstraint {
            name: Some("fk_child_parent".to_string()),
            column_names: vec!["parent_id".to_string()],
            column_indices: vec![1],
            parent_table: "parent".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        }],
    );
    db.create_table(child_schema).unwrap();

    // Insert parent rows
    db.insert_row(
        "parent",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Parent 1"))]),
    )
    .unwrap();
    db.insert_row(
        "parent",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Parent 2"))]),
    )
    .unwrap();

    // Insert child row referencing parent
    db.insert_row("child", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1)])).unwrap();

    // DELETE FROM parent (no WHERE)
    // Should NOT use TRUNCATE because child references exist
    // Should fail with FK constraint violation
    let stmt = DeleteStmt { only: false, table_name: "parent".to_string(), where_clause: None };

    let result = DeleteExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("FOREIGN KEY constraint violation"));

    // Verify parent rows still exist
    assert_eq!(db.get_table("parent").unwrap().row_count(), 2);
}

#[test]
fn test_truncate_allowed_when_no_fk_references() {
    let mut db = Database::new();

    // Create parent table with primary key
    let parent_schema = TableSchema::with_primary_key(
        "parent".to_string(),
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

    // Create child table with FK (but no child rows)
    let child_schema = TableSchema::with_foreign_keys(
        "child".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("parent_id".to_string(), DataType::Integer, false),
        ],
        vec![ForeignKeyConstraint {
            name: Some("fk_child_parent".to_string()),
            column_names: vec!["parent_id".to_string()],
            column_indices: vec![1],
            parent_table: "parent".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        }],
    );
    db.create_table(child_schema).unwrap();

    // Insert parent rows only (NO child references)
    for i in 1..=100 {
        db.insert_row(
            "parent",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("Parent {}", i)))]),
        )
        .unwrap();
    }

    // DELETE FROM parent (no WHERE)
    // Cannot use TRUNCATE because FK constraint exists (even though no child rows reference it)
    // This is conservative but correct - we don't scan child table to check
    let stmt = DeleteStmt { only: false, table_name: "parent".to_string(), where_clause: None };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 100);

    // Verify all parent rows deleted
    assert_eq!(db.get_table("parent").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_blocked_by_delete_trigger() {
    let mut db = Database::new();

    // Create table
    let schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "data".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Create DELETE trigger on the table
    let trigger = TriggerDefinition::new(
        "audit_delete".to_string(),
        TriggerTiming::After,
        TriggerEvent::Delete,
        "test_table".to_string(),
        TriggerGranularity::Row,
        None,
        TriggerAction::RawSql("-- audit logic here".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Insert rows
    for i in 0..10 {
        db.insert_row(
            "test_table",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("data_{}", i)))]),
        )
        .unwrap();
    }

    // DELETE FROM test_table (no WHERE)
    // Should NOT use TRUNCATE because DELETE trigger exists
    // Should use row-by-row deletion (which currently doesn't execute triggers, but that's
    // separate)
    let stmt = DeleteStmt { only: false, table_name: "test_table".to_string(), where_clause: None };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 10);

    // Verify all rows deleted
    assert_eq!(db.get_table("test_table").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_allowed_with_insert_trigger() {
    let mut db = Database::new();

    // Create table
    let schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "data".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Create INSERT trigger (not DELETE) - should not block TRUNCATE
    let trigger = TriggerDefinition::new(
        "audit_insert".to_string(),
        TriggerTiming::After,
        TriggerEvent::Insert,
        "test_table".to_string(),
        TriggerGranularity::Row,
        None,
        TriggerAction::RawSql("-- audit logic here".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Insert rows
    for i in 0..100 {
        db.insert_row(
            "test_table",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("data_{}", i)))]),
        )
        .unwrap();
    }

    // DELETE FROM test_table (no WHERE)
    // Should use TRUNCATE because only INSERT trigger exists (not DELETE)
    let stmt = DeleteStmt { only: false, table_name: "test_table".to_string(), where_clause: None };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 100);

    // Verify all rows deleted
    assert_eq!(db.get_table("test_table").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_performance() {
    // This test verifies the optimization works, but performance is best measured with benchmarks
    let mut db = Database::new();

    let schema = TableSchema::new(
        "large_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "data".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 10,000 rows
    for i in 0..10_000 {
        db.insert_row(
            "large_table",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("data_{}", i)))]),
        )
        .unwrap();
    }

    let stmt =
        DeleteStmt { only: false, table_name: "large_table".to_string(), where_clause: None };

    // Time the deletion
    let start = std::time::Instant::now();
    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    let duration = start.elapsed();

    assert_eq!(deleted, 10_000);
    assert_eq!(db.get_table("large_table").unwrap().row_count(), 0);

    // With TRUNCATE optimization, this should complete in milliseconds
    // Without it, it would take much longer (seconds)
    // We don't assert on exact time as it varies, but this documents the expected behavior
    println!("Deleted 10,000 rows in {:?}", duration);
}
