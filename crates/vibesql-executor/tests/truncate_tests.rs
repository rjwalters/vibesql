//! Tests for TRUNCATE TABLE statement execution
//!
//! Covers truncate functionality including:
//! - Basic truncate operations
//! - IF EXISTS handling
//! - Safety checks (DELETE triggers, foreign keys)
//! - CASCADE behavior
//! - Multi-table truncate
//! - Table structure preservation

use vibesql_ast::{
    ColumnDef, CreateTableStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
    TruncateTableStmt,
};
use vibesql_catalog::{
    ColumnSchema, ForeignKeyConstraint, ReferentialAction, TableSchema, TriggerDefinition,
};
use vibesql_executor::errors::ExecutorError;
use vibesql_executor::{CreateTableExecutor, TruncateTableExecutor};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// Helper function to create a simple table
fn create_test_table(db: &mut Database, table_name: &str) {
    let create_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: table_name.to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "data".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, db).unwrap();
}

// ============================================================================
// Basic Functionality Tests
// ============================================================================

#[test]
fn test_truncate_basic() {
    let mut db = Database::new();
    create_test_table(&mut db, "test_table");

    // Insert test data
    db.insert_row(
        "test_table",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("row1"))]),
    )
    .unwrap();
    db.insert_row(
        "test_table",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("row2"))]),
    )
    .unwrap();
    db.insert_row(
        "test_table",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("row3"))]),
    )
    .unwrap();

    assert_eq!(db.get_table("test_table").unwrap().row_count(), 3);

    // Execute truncate
    let stmt = TruncateTableStmt {
        table_names: vec!["test_table".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3); // 3 rows deleted
    assert_eq!(db.get_table("test_table").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_empty_table() {
    let mut db = Database::new();
    create_test_table(&mut db, "empty_table");

    // Don't insert any data
    assert_eq!(db.get_table("empty_table").unwrap().row_count(), 0);

    // Execute truncate on empty table
    let stmt = TruncateTableStmt {
        table_names: vec!["empty_table".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0); // 0 rows deleted
    assert_eq!(db.get_table("empty_table").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_if_exists_nonexistent() {
    let mut db = Database::new();

    // Truncate nonexistent table with IF EXISTS
    let stmt = TruncateTableStmt {
        table_names: vec!["nonexistent".to_string()],
        if_exists: true,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0); // Silently succeeds with 0 rows
}

#[test]
fn test_truncate_if_exists_existing() {
    let mut db = Database::new();
    create_test_table(&mut db, "existing_table");

    // Insert data
    db.insert_row(
        "existing_table",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("data"))]),
    )
    .unwrap();

    // Truncate with IF EXISTS
    let stmt = TruncateTableStmt {
        table_names: vec!["existing_table".to_string()],
        if_exists: true,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1); // 1 row deleted
    assert_eq!(db.get_table("existing_table").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_missing_table() {
    let mut db = Database::new();

    // Truncate nonexistent table WITHOUT IF EXISTS
    let stmt = TruncateTableStmt {
        table_names: vec!["missing_table".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
}

// ============================================================================
// Safety Check Tests - DELETE Triggers
// ============================================================================

#[test]
fn test_truncate_blocked_by_delete_triggers() {
    let mut db = Database::new();

    // Create table via catalog API (to add triggers)
    let schema = TableSchema::new(
        "triggered_table".to_string(),
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

    // Create BEFORE DELETE trigger
    let trigger = TriggerDefinition::new(
        "before_delete_trigger".to_string(),
        TriggerTiming::Before,
        TriggerEvent::Delete,
        "triggered_table".to_string(),
        TriggerGranularity::Row,
        None,
        TriggerAction::RawSql("-- audit logic".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Insert rows
    db.insert_row(
        "triggered_table",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("data1"))]),
    )
    .unwrap();

    // Attempt truncate - should be blocked by DELETE trigger
    let stmt = TruncateTableStmt {
        table_names: vec!["triggered_table".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("DELETE triggers") || error_msg.contains("foreign keys"));

    // Verify data still exists
    assert_eq!(db.get_table("triggered_table").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_blocked_by_after_delete_trigger() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "audit_table".to_string(),
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

    // Create AFTER DELETE trigger
    let trigger = TriggerDefinition::new(
        "after_delete_audit".to_string(),
        TriggerTiming::After,
        TriggerEvent::Delete,
        "audit_table".to_string(),
        TriggerGranularity::Row,
        None,
        TriggerAction::RawSql("INSERT INTO audit_log ...".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    db.insert_row(
        "audit_table",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("test"))]),
    )
    .unwrap();

    // Attempt truncate - should be blocked
    let stmt = TruncateTableStmt {
        table_names: vec!["audit_table".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_err());
    assert_eq!(db.get_table("audit_table").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_allowed_with_insert_trigger() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "insert_triggered".to_string(),
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

    // Create INSERT trigger (not DELETE) - should NOT block TRUNCATE
    let trigger = TriggerDefinition::new(
        "insert_trigger".to_string(),
        TriggerTiming::After,
        TriggerEvent::Insert,
        "insert_triggered".to_string(),
        TriggerGranularity::Row,
        None,
        TriggerAction::RawSql("-- insert audit".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    // Insert rows
    for i in 0..10 {
        db.insert_row(
            "insert_triggered",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("data{}", i)))]),
        )
        .unwrap();
    }

    // Truncate should succeed because only INSERT trigger exists
    let stmt = TruncateTableStmt {
        table_names: vec!["insert_triggered".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 10);
    assert_eq!(db.get_table("insert_triggered").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_allowed_with_update_trigger() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "update_triggered".to_string(),
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

    // Create UPDATE trigger - should NOT block TRUNCATE
    let trigger = TriggerDefinition::new(
        "update_trigger".to_string(),
        TriggerTiming::Before,
        TriggerEvent::Update(None), // No column list
        "update_triggered".to_string(),
        TriggerGranularity::Row,
        None,
        TriggerAction::RawSql("-- update validation".to_string()),
    );
    db.catalog.create_trigger(trigger).unwrap();

    db.insert_row(
        "update_triggered",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("data"))]),
    )
    .unwrap();

    // Truncate should succeed
    let stmt = TruncateTableStmt {
        table_names: vec!["update_triggered".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
    assert_eq!(db.get_table("update_triggered").unwrap().row_count(), 0);
}

// ============================================================================
// Safety Check Tests - Foreign Keys
// ============================================================================

#[test]
fn test_truncate_blocked_by_fk_references() {
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
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Parent1"))]),
    )
    .unwrap();

    // Attempt to truncate parent - should be blocked because child table references it
    let stmt = TruncateTableStmt {
        table_names: vec!["parent".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("DELETE triggers") || error_msg.contains("foreign keys"));

    // Verify parent data still exists
    assert_eq!(db.get_table("parent").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_allowed_no_fk_references() {
    let mut db = Database::new();

    // Create standalone table with no FK references
    let schema = TableSchema::new(
        "standalone".to_string(),
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

    // Insert data
    for i in 0..100 {
        db.insert_row(
            "standalone",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("data{}", i)))]),
        )
        .unwrap();
    }

    // Truncate should succeed
    let stmt = TruncateTableStmt {
        table_names: vec!["standalone".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 100);
    assert_eq!(db.get_table("standalone").unwrap().row_count(), 0);
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_truncate_preserves_table_structure() {
    let mut db = Database::new();

    // Create table with specific structure
    let create_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "structured_table".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert data
    db.insert_row(
        "structured_table",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("test@example.com")),
            SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    // Get original table schema
    let original_schema = db.catalog.get_table("structured_table").unwrap();
    let original_columns = original_schema.columns.clone();

    // Truncate
    let stmt = TruncateTableStmt {
        table_names: vec!["structured_table".to_string()],
        if_exists: false,
        cascade: None,
    };
    TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

    // Verify table structure is intact
    let table_after = db.catalog.get_table("structured_table").unwrap();
    assert_eq!(table_after.columns.len(), 3);
    assert_eq!(table_after.columns, original_columns);

    // Verify we can still insert with same structure
    db.insert_row(
        "structured_table",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("new@example.com")),
            SqlValue::Integer(30),
        ]),
    )
    .unwrap();

    assert_eq!(db.get_table("structured_table").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_clears_all_data() {
    let mut db = Database::new();
    create_test_table(&mut db, "large_table");

    // Insert many rows
    for i in 0..1000 {
        db.insert_row(
            "large_table",
            Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("data_{}", i)))]),
        )
        .unwrap();
    }

    assert_eq!(db.get_table("large_table").unwrap().row_count(), 1000);

    // Truncate
    let stmt = TruncateTableStmt {
        table_names: vec!["large_table".to_string()],
        if_exists: false,
        cascade: None,
    };
    let deleted = TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

    assert_eq!(deleted, 1000);
    assert_eq!(db.get_table("large_table").unwrap().row_count(), 0);

    // Verify we can insert new data
    db.insert_row(
        "large_table",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("new_data"))]),
    )
    .unwrap();
    assert_eq!(db.get_table("large_table").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_returns_correct_row_count() {
    let mut db = Database::new();
    create_test_table(&mut db, "count_test");

    // Test various row counts
    let test_counts = vec![0, 1, 5, 100, 1000];

    for count in test_counts {
        // Clear and insert specific number of rows
        db.get_table_mut("count_test").unwrap().clear();

        for i in 0..count {
            db.insert_row(
                "count_test",
                Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("row_{}", i)))]),
            )
            .unwrap();
        }

        // Truncate and verify count
        let stmt = TruncateTableStmt {
            table_names: vec!["count_test".to_string()],
            if_exists: false,
            cascade: None,
        };
        let deleted = TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

        assert_eq!(deleted, count as usize, "Failed for count {}", count);
        assert_eq!(db.get_table("count_test").unwrap().row_count(), 0);
    }
}
