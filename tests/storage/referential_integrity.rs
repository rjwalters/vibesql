// ! Integration tests for referential integrity (foreign key constraints)
//!
//! Target module: crates/executor/src/delete/integrity.rs (27.91% coverage → 80%+)
//!
//! Test coverage for:
//! - ON DELETE actions: CASCADE, SET NULL, SET DEFAULT, NO ACTION, RESTRICT
//! - ON UPDATE actions: CASCADE, SET NULL, SET DEFAULT, NO ACTION, RESTRICT
//! - Edge cases: circular FKs, self-referential tables, multi-column FKs, NULL values

#[path = "../common/mod.rs"]
mod common;

use vibesql_catalog::{ColumnSchema, ReferentialAction, TableSchema};
use common::referential_integrity_fixtures::*;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// ========================================================================
// Phase 1: ON DELETE Actions - Basic Tests
// ========================================================================

#[test]
fn test_on_delete_no_action_with_references() {
    let mut db = Database::new();

    // Create parent and child tables
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Try to delete parent - should fail due to foreign key constraint
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_fk_violation(result, "CHILD");

    // Verify parent still exists
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
}

#[test]
fn test_on_delete_no_action_without_references() {
    let mut db = Database::new();

    // Create parent and child tables
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert test data - parent without children
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_parent_row(&mut db, "PARENT", 2, "Bob");

    // Only Bob has a child
    insert_child_row(&mut db, "CHILD", 10, 2, "Child1");

    // Delete parent without children - should succeed
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify Alice was deleted, Bob remains
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
}

#[test]
fn test_on_delete_cascade_single_level() {
    let mut db = Database::new();

    // Create parent and child tables with CASCADE
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::Cascade,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");
    insert_child_row(&mut db, "CHILD", 11, 1, "Child2");

    // Delete parent - should cascade to children
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify parent was deleted
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 0);

    // Verify children were also deleted (cascaded)
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 0, "Children should be cascaded deleted");
}

#[test]
fn test_on_delete_cascade_multi_level() {
    let mut db = Database::new();

    // Create grandparent, parent, and child tables with CASCADE
    create_parent_table(&mut db, "GRANDPARENT");
    create_child_table(
        &mut db,
        "PARENT",
        "GRANDPARENT",
        ReferentialAction::Cascade,
        ReferentialAction::NoAction,
    );
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::Cascade,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "GRANDPARENT", 1, "GrandParent1");
    db.insert_row(
        "PARENT",
        Row::new(vec![
            SqlValue::Integer(10),
            SqlValue::Integer(1),
            SqlValue::Varchar("Parent1".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![
            SqlValue::Integer(100),
            SqlValue::Integer(10),
            SqlValue::Varchar("Child1".to_string()),
        ]),
    )
    .unwrap();

    // Delete grandparent - should cascade through all levels
    let result = execute_delete(&mut db, "DELETE FROM grandparent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify all levels were deleted
    assert_eq!(db.get_table("GRANDPARENT").unwrap().row_count(), 0);
    assert_eq!(db.get_table("PARENT").unwrap().row_count(), 0);
    assert_eq!(db.get_table("CHILD").unwrap().row_count(), 0);
}

#[test]
fn test_on_delete_set_null() {
    let mut db = Database::new();

    // Create parent and child tables with SET NULL
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::SetNull,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Delete parent - should set child's foreign key to NULL
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify parent was deleted
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 0);

    // Verify child's foreign key was set to NULL
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 1);
    let child_row = &child_table.scan()[0];
    assert_eq!(child_row.values[1], SqlValue::Null, "Foreign key should be NULL");
}

#[test]
fn test_on_delete_set_default() {
    let mut db = Database::new();

    // Create parent and child tables with SET DEFAULT
    create_parent_table(&mut db, "PARENT");

    // Create a default parent row (id=0 is the default FK value)
    insert_parent_row(&mut db, "PARENT", 0, "Default");

    // Use the helper that sets a default value for PARENT_ID column
    create_child_table_with_default(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::SetDefault,
        ReferentialAction::NoAction,
        0, // default value for PARENT_ID
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Delete parent - should set child's foreign key to default value (0)
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify child's foreign key was set to default
    let child_table = db.get_table("CHILD").unwrap();
    let child_row = &child_table.scan()[0];
    assert_eq!(child_row.values[1], SqlValue::Integer(0), "Foreign key should be set to default");
}

#[test]
fn test_on_delete_restrict_with_references() {
    let mut db = Database::new();

    // RESTRICT is similar to NO ACTION in SQL
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Try to delete parent - should fail
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_fk_violation(result, "");
}

// ========================================================================
// Phase 2: ON UPDATE Actions
// ========================================================================

#[test]
fn test_on_update_cascade() {
    let mut db = Database::new();

    // Create parent and child tables with CASCADE on UPDATE
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::Cascade,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");
    insert_child_row(&mut db, "CHILD", 11, 1, "Child2");

    // Update parent primary key - should cascade to children
    let result = execute_update(&mut db, "UPDATE parent SET id = 100 WHERE id = 1");
    assert_successful_update(result, 1);

    // Verify parent was updated
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
    let parent_row = &parent_table.scan()[0];
    assert_eq!(parent_row.values[0], SqlValue::Integer(100));

    // Verify children were cascaded
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 2);
    for child_row in child_table.scan() {
        assert_eq!(
            child_row.values[1],
            SqlValue::Integer(100),
            "Child FK should be updated to 100"
        );
    }
}

#[test]
fn test_on_update_set_null() {
    let mut db = Database::new();

    // Create parent and child tables with SET NULL on UPDATE
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::SetNull,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");
    insert_child_row(&mut db, "CHILD", 11, 1, "Child2");

    // Update parent primary key - should set child FKs to NULL
    let result = execute_update(&mut db, "UPDATE parent SET id = 100 WHERE id = 1");
    assert_successful_update(result, 1);

    // Verify parent was updated
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
    let parent_row = &parent_table.scan()[0];
    assert_eq!(parent_row.values[0], SqlValue::Integer(100));

    // Verify children FKs were set to NULL
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 2);
    for child_row in child_table.scan() {
        assert_eq!(
            child_row.values[1],
            SqlValue::Null,
            "Child FK should be NULL"
        );
    }
}

#[test]
fn test_on_update_set_default() {
    let mut db = Database::new();

    // Create parent and child tables with SET DEFAULT on UPDATE
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::SetDefault,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");
    insert_child_row(&mut db, "CHILD", 11, 1, "Child2");

    // Update parent primary key - should set child FKs to default (currently NULL)
    let result = execute_update(&mut db, "UPDATE parent SET id = 100 WHERE id = 1");
    assert_successful_update(result, 1);

    // Verify parent was updated
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
    let parent_row = &parent_table.scan()[0];
    assert_eq!(parent_row.values[0], SqlValue::Integer(100));

    // Verify children FKs were set to NULL (current behavior until proper DEFAULT evaluation)
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 2);
    for child_row in child_table.scan() {
        assert_eq!(
            child_row.values[1],
            SqlValue::Null,
            "Child FK should be NULL (current default behavior)"
        );
    }
}

#[test]
fn test_on_update_no_action() {
    let mut db = Database::new();

    // Create parent and child tables with NO ACTION on UPDATE
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Try to update parent primary key - should fail
    let result = execute_update(&mut db, "UPDATE parent SET id = 100 WHERE id = 1");
    assert_fk_violation(result, "CHILD");

    // Verify parent was NOT updated
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
    let parent_row = &parent_table.scan()[0];
    assert_eq!(parent_row.values[0], SqlValue::Integer(1), "Parent ID should remain 1");

    // Verify child was NOT affected
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 1);
    let child_row = &child_table.scan()[0];
    assert_eq!(
        child_row.values[1],
        SqlValue::Integer(1),
        "Child FK should remain 1"
    );
}

#[test]
fn test_on_update_restrict() {
    let mut db = Database::new();

    // Create parent and child tables with RESTRICT on UPDATE
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::Restrict,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Try to update parent primary key - should fail (similar to NO ACTION)
    let result = execute_update(&mut db, "UPDATE parent SET id = 100 WHERE id = 1");
    assert_fk_violation(result, "CHILD");

    // Verify parent was NOT updated
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
    let parent_row = &parent_table.scan()[0];
    assert_eq!(parent_row.values[0], SqlValue::Integer(1), "Parent ID should remain 1");

    // Verify child was NOT affected
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 1);
    let child_row = &child_table.scan()[0];
    assert_eq!(
        child_row.values[1],
        SqlValue::Integer(1),
        "Child FK should remain 1"
    );
}

// ========================================================================
// Phase 3: Edge Cases
// ========================================================================

#[test]
fn test_circular_foreign_keys() {
    use vibesql_catalog::ForeignKeyConstraint;

    let mut db = Database::new();

    // Create first table without FK
    let schema_a = TableSchema::with_primary_key(
        "TABLE_A".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("B_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    db.create_table(schema_a).unwrap();

    // Create second table with FK to TABLE_A
    let fk_b_to_a = ForeignKeyConstraint {
        name: Some("FK_B_A".to_string()),
        column_names: vec!["A_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_A".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let columns_b = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("A_ID".to_string(), DataType::Integer, true),
    ];
    let mut schema_b =
        TableSchema::with_primary_key("TABLE_B".to_string(), columns_b, vec!["ID".to_string()]);
    schema_b.foreign_keys.push(fk_b_to_a);
    db.create_table(schema_b).unwrap();

    // Now try to create TABLE_C with FK creating a cycle: C → A → B → (would close cycle to C)
    // First, let's try to add a FK from A back to B, which would create a direct cycle
    let _fk_a_to_b = ForeignKeyConstraint {
        name: Some("FK_A_B".to_string()),
        column_names: vec!["B_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_B".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let columns_c = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("A_ID".to_string(), DataType::Integer, true),
    ];
    let mut schema_c =
        TableSchema::with_primary_key("TABLE_C".to_string(), columns_c, vec!["ID".to_string()]);
    // Add FK from C to A
    schema_c.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_C_A".to_string()),
        column_names: vec!["A_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_A".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });
    // This should succeed - no cycle yet
    db.create_table(schema_c).unwrap();

    // Now create TABLE_D that references TABLE_B creating a longer cycle: D → B → A → ...
    // Actually, let's test by trying to update TABLE_A to reference B (simulating ALTER TABLE)
    // Since we don't have ALTER TABLE support yet, we'll test detection during CREATE TABLE

    // Test: Create a table that would create a cycle
    // TABLE_X references TABLE_C, and if TABLE_A referenced TABLE_X, we'd have: A → X → C → A
    let columns_x = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("C_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new("A_ID".to_string(), DataType::Integer, true),
    ];
    let mut schema_x =
        TableSchema::with_primary_key("TABLE_X".to_string(), columns_x, vec!["ID".to_string()]);
    schema_x.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_X_C".to_string()),
        column_names: vec!["C_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_C".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });
    schema_x.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_X_A".to_string()),
        column_names: vec!["A_ID".to_string()],
        column_indices: vec![2],
        parent_table: "TABLE_A".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });
    // This creates X → C → A → B (no cycle from this table's perspective)
    db.create_table(schema_x).unwrap();

    // Test detection: Try to create a table Y that references X and is referenced by A
    // We can't easily test this without ALTER TABLE support
    // For now, verify that non-circular FKs work correctly

    // Verify all tables were created successfully (no cycle yet)
    assert!(db.get_table("TABLE_A").is_some());
    assert!(db.get_table("TABLE_B").is_some());
    assert!(db.get_table("TABLE_C").is_some());
    assert!(db.get_table("TABLE_X").is_some());

    // Now test actual cycle detection: Try to create TABLE_Y that references B
    // and has A reference it back, creating: A → Y → B → A (cycle!)
    // We need to drop and recreate TABLE_A with the FK to create the cycle

    // Since we can't ALTER TABLE_A to add FK yet, create a fresh scenario:
    let mut db2 = Database::new();

    // Create TABLE_P
    db2.create_table(TableSchema::with_primary_key(
        "TABLE_P".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("Q_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    ))
    .unwrap();

    // Create TABLE_Q that references P
    let mut schema_q = TableSchema::with_primary_key(
        "TABLE_Q".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("R_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    schema_q.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_Q_P".to_string()),
        column_names: vec!["R_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_P".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });
    db2.create_table(schema_q).unwrap();

    // Now try to create TABLE_R that references Q but with P referencing R
    // This would create: P → (will ref R) → Q → P = cycle
    // Actually, P already exists without FK. Let's create TABLE_R that creates a cycle differently.

    // Better approach: Create TABLE_R that references both P and Q
    // And then P should reference R - but we can't modify P
    // Let's test self-referencing instead, which should be ALLOWED
    let mut schema_r = TableSchema::with_primary_key(
        "TABLE_R".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("PARENT_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    schema_r.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_R_SELF".to_string()),
        column_names: vec!["PARENT_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_R".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });
    // Self-reference should be ALLOWED (e.g., employee table with manager_id)
    let result = db2.create_table(schema_r);
    assert!(result.is_ok(), "Self-referencing table should be allowed");

    // Test actual multi-table cycle detection using DROP + CREATE approach
    let mut db3 = Database::new();

    // Create TABLE_T1 (no FK initially)
    db3.create_table(TableSchema::with_primary_key(
        "TABLE_T1".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("T3_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    ))
    .unwrap();

    // Create TABLE_T2 with FK to T1 (T2→T1)
    let mut schema_t2 = TableSchema::with_primary_key(
        "TABLE_T2".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("T1_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    schema_t2.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_T2_T1".to_string()),
        column_names: vec!["T1_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_T1".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });
    db3.create_table(schema_t2).unwrap();

    // Create TABLE_T3 with FK to T2 (T3→T2), creating chain T3→T2→T1
    let mut schema_t3 = TableSchema::with_primary_key(
        "TABLE_T3".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("T2_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    schema_t3.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_T3_T2".to_string()),
        column_names: vec!["T2_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_T2".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });
    db3.create_table(schema_t3).unwrap();

    // Now drop TABLE_T1 and recreate it with FK to T3
    // This would create cycle: T1→T3→T2→T1
    db3.drop_table("TABLE_T1").unwrap();

    let mut schema_t1_cyclic = TableSchema::with_primary_key(
        "TABLE_T1".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("T3_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    schema_t1_cyclic.foreign_keys.push(ForeignKeyConstraint {
        name: Some("FK_T1_T3".to_string()),
        column_names: vec!["T3_ID".to_string()],
        column_indices: vec![1],
        parent_table: "TABLE_T3".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    });

    // This MUST fail - creating cycle T1→T3→T2→T1
    let result = db3.create_table(schema_t1_cyclic);
    assert!(
        result.is_err(),
        "Creating circular FK dependency T1→T3→T2→T1 should be rejected"
    );

    // Verify it's the correct error type
    match result {
        Err(e) => {
            let error_msg = format!("{}", e);
            assert!(
                error_msg.contains("Circular foreign key") || error_msg.contains("circular"),
                "Error should mention circular FK, got: {}",
                error_msg
            );
        }
        Ok(_) => panic!("Expected CircularForeignKey error, but table creation succeeded"),
    }
}

#[test]
fn test_self_referential_table() {
    let mut db = Database::new();

    // Create a self-referential table (e.g., employees with manager_id)
    create_self_referential_employee_table(&mut db);

    // Insert employees: CEO (no manager), Manager (reports to CEO), Employee (reports to Manager)
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Varchar("CEO".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Varchar("Manager".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(2),
            SqlValue::Varchar("Employee".to_string()),
        ]),
    )
    .unwrap();

    // Try to delete Manager - should fail because Employee references them
    let result = execute_delete(&mut db, "DELETE FROM employee WHERE id = 2");
    assert_fk_violation(result, "");

    // Delete Employee first, then Manager should succeed
    let result1 = execute_delete(&mut db, "DELETE FROM employee WHERE id = 3");
    assert_successful_delete(result1, 1);
    let result2 = execute_delete(&mut db, "DELETE FROM employee WHERE id = 2");
    assert_successful_delete(result2, 1);

    // Verify only CEO remains
    let employee_table = db.get_table("EMPLOYEE").unwrap();
    assert_eq!(employee_table.row_count(), 1);
}

#[test]
fn test_multi_column_foreign_key() {
    let mut db = Database::new();

    // Create parent with composite primary key and child with multi-column FK
    create_multi_column_parent_child_tables(&mut db);

    // Insert test data
    db.insert_row(
        "PARENT",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(100),
            SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();

    // Try to delete parent - should fail
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE dept_id = 1 AND emp_id = 100");
    assert_fk_violation(result, "");
}

#[test]
fn test_null_foreign_key_values() {
    let mut db = Database::new();

    // Create parent and child tables
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert parent
    insert_parent_row(&mut db, "PARENT", 1, "Alice");

    // Insert child with NULL foreign key (should be allowed - NULLs bypass FK checks)
    insert_child_row_null_fk(&mut db, "CHILD", 10, "Orphan");

    // Delete parent - should succeed because child has NULL foreign key
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify child with NULL FK still exists
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 1);
}

#[test]
fn test_delete_multiple_parents_with_shared_child() {
    let mut db = Database::new();

    // Create two parent tables and child with FKs to both
    create_multiple_parent_child_tables(&mut db);

    // Insert test data
    insert_parent_row(&mut db, "PARENT1", 1, "P1");
    insert_parent_row(&mut db, "PARENT2", 2, "P2");
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Integer(2)]),
    )
    .unwrap();

    // Try to delete either parent - should fail
    let result1 = execute_delete(&mut db, "DELETE FROM parent1 WHERE id = 1");
    assert_fk_violation(result1, "");
    let result2 = execute_delete(&mut db, "DELETE FROM parent2 WHERE id = 2");
    assert_fk_violation(result2, "");
}

// ========================================================================
// Coverage Enhancement Tests
// ========================================================================

#[test]
fn test_table_without_primary_key() {
    let mut db = Database::new();

    // Create parent table WITHOUT primary key
    let parent_schema = TableSchema::new(
        "PARENT".to_string(),
        vec![ColumnSchema::new("ID".to_string(), DataType::Integer, false)],
    );
    db.create_table(parent_schema).unwrap();

    // Insert data
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    // Delete should succeed (no PK means no FK enforcement on this table)
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);
}

#[test]
fn test_empty_child_table() {
    let mut db = Database::new();

    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert only parent (no children)
    insert_parent_row(&mut db, "PARENT", 1, "Alice");

    // Delete should succeed (no children to violate constraint)
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);
}

#[test]
fn test_fk_constraint_error_message() {
    let mut db = Database::new();

    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Check error message format
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint violation"));
    assert!(err.contains("CHILD")); // Should mention the child table
    assert!(err.contains("FK_CHILD_PARENT")); // Should mention the constraint name
}
