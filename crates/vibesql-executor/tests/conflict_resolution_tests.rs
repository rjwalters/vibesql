//! Tests for INSERT/UPDATE conflict resolution clauses (OR IGNORE, OR REPLACE, etc.)
//!
//! These tests verify the SQLite-compatible conflict resolution behavior:
//! - OR IGNORE: Skip rows that would cause constraint violations
//! - OR REPLACE: Delete conflicting rows before inserting/updating
//!
//! Reference: https://www.sqlite.org/lang_conflict.html

use vibesql_ast::{Assignment, ConflictClause, Expression, InsertSource, InsertStmt, UpdateStmt};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{InsertExecutor, UpdateExecutor};
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Helper to create a test database with a users table with PK and UNIQUE constraint
fn setup_users_table(db: &mut Database) {
    let schema = TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ],
        Some(vec!["id".to_string()]),    // Primary key
        vec![vec!["email".to_string()]], // Unique constraint on email
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a user
fn insert_user(db: &mut Database, id: i64, email: &str, name: &str) {
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(id)),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(email))),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(name))),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    InsertExecutor::execute(db, &stmt).unwrap();
}

// ============================================================================
// INSERT OR IGNORE Tests
// ============================================================================

#[test]
fn test_insert_or_ignore_primary_key_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert initial row
    insert_user(&mut db, 1, "alice@test.com", "Alice");

    // Try INSERT OR IGNORE with conflicting primary key
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(1)), // Same id
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("bob@test.com"))),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
        ]]),
        conflict_clause: Some(ConflictClause::Ignore),
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 0, "Row with duplicate PK should be ignored");

    // Verify original row is unchanged
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    let row = &table.scan()[0];
    assert_eq!(row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com")));
}

#[test]
fn test_insert_or_ignore_unique_constraint_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert initial row
    insert_user(&mut db, 1, "alice@test.com", "Alice");

    // Try INSERT OR IGNORE with conflicting unique constraint (email)
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(2)), // Different id
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com"))), // Same email
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice 2"))),
        ]]),
        conflict_clause: Some(ConflictClause::Ignore),
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 0, "Row with duplicate UNIQUE should be ignored");

    // Verify original row is unchanged
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_or_ignore_multi_row() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert initial row
    insert_user(&mut db, 1, "alice@test.com", "Alice");

    // Try INSERT OR IGNORE with multiple rows - some conflict, some don't
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![
            vec![
                Expression::Literal(SqlValue::Integer(1)), // Conflicts with PK
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("conflict1@test.com"))),
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Conflict1"))),
            ],
            vec![
                Expression::Literal(SqlValue::Integer(2)), // OK
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("bob@test.com"))),
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
            ],
            vec![
                Expression::Literal(SqlValue::Integer(3)), // OK id but conflicts email
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com"))),
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice Clone"))),
            ],
            vec![
                Expression::Literal(SqlValue::Integer(4)), // OK
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("charlie@test.com"))),
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))),
            ],
        ]),
        conflict_clause: Some(ConflictClause::Ignore),
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 2, "Only non-conflicting rows should be inserted");

    // Verify final state: original Alice, Bob, and Charlie
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_insert_or_ignore_no_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // INSERT OR IGNORE with no conflicts should insert normally
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(1)),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com"))),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: Some(ConflictClause::Ignore),
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 1, "Non-conflicting row should be inserted");

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

// ============================================================================
// UPDATE OR IGNORE Tests
// ============================================================================

#[test]
fn test_update_or_ignore_primary_key_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert two rows
    insert_user(&mut db, 1, "alice@test.com", "Alice");
    insert_user(&mut db, 2, "bob@test.com", "Bob");

    // Try UPDATE OR IGNORE to change Alice's id to 2 (conflicts with Bob)
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        table_name: "users".to_string(),
        quoted: false,
        alias: None,
        assignments: vec![Assignment {
            column: "id".to_string(),
            value: Expression::Literal(SqlValue::Integer(2)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: Some(ConflictClause::Ignore),
        returning: None,
    };

    let rows_updated = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(rows_updated, 0, "Conflicting update should be ignored");

    // Verify both rows are unchanged
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);

    // Alice should still have id=1
    let alice = table
        .scan()
        .iter()
        .find(|r| r.values[1] == SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com")))
        .unwrap()
        .clone();
    assert_eq!(alice.values[0], SqlValue::Integer(1));
}

#[test]
fn test_update_or_ignore_unique_constraint_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert two rows
    insert_user(&mut db, 1, "alice@test.com", "Alice");
    insert_user(&mut db, 2, "bob@test.com", "Bob");

    // Try UPDATE OR IGNORE to change Alice's email to Bob's (conflicts with UNIQUE)
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        table_name: "users".to_string(),
        quoted: false,
        alias: None,
        assignments: vec![Assignment {
            column: "email".to_string(),
            value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("bob@test.com"))),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: Some(ConflictClause::Ignore),
        returning: None,
    };

    let rows_updated = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(rows_updated, 0, "Conflicting update should be ignored");

    // Verify Alice's email is unchanged
    let table = db.get_table("users").unwrap();
    let alice = table.scan().iter().find(|r| r.values[0] == SqlValue::Integer(1)).unwrap().clone();
    assert_eq!(alice.values[1], SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com")));
}

#[test]
fn test_update_or_ignore_no_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    insert_user(&mut db, 1, "alice@test.com", "Alice");

    // UPDATE OR IGNORE with no conflicts should work normally
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        table_name: "users".to_string(),
        quoted: false,
        alias: None,
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice Updated"))),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: Some(ConflictClause::Ignore),
        returning: None,
    };

    let rows_updated = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(rows_updated, 1, "Non-conflicting update should succeed");

    // Verify name is updated
    let table = db.get_table("users").unwrap();
    let alice = &table.scan()[0];
    assert_eq!(alice.values[2], SqlValue::Varchar(arcstr::ArcStr::from("Alice Updated")));
}

// ============================================================================
// UPDATE OR REPLACE Tests
// ============================================================================

#[test]
fn test_update_or_replace_primary_key_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert two rows
    insert_user(&mut db, 1, "alice@test.com", "Alice");
    insert_user(&mut db, 2, "bob@test.com", "Bob");

    // UPDATE OR REPLACE to change Alice's id to 2 (should delete Bob first)
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        table_name: "users".to_string(),
        quoted: false,
        alias: None,
        assignments: vec![Assignment {
            column: "id".to_string(),
            value: Expression::Literal(SqlValue::Integer(2)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "email", false,
            ))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(
                "alice@test.com",
            )))),
        })),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: Some(ConflictClause::Replace),
        returning: None,
    };

    let rows_updated = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(rows_updated, 1, "Update should succeed after deleting conflicting row");

    // Verify Bob is deleted and Alice now has id=2
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1, "Bob should have been deleted");

    let row = &table.scan()[0];
    assert_eq!(row.values[0], SqlValue::Integer(2), "Alice should now have id=2");
    assert_eq!(row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com")));
}

#[test]
fn test_update_or_replace_unique_constraint_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert two rows
    insert_user(&mut db, 1, "alice@test.com", "Alice");
    insert_user(&mut db, 2, "bob@test.com", "Bob");

    // UPDATE OR REPLACE to change Alice's email to Bob's (should delete Bob first)
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        table_name: "users".to_string(),
        quoted: false,
        alias: None,
        assignments: vec![Assignment {
            column: "email".to_string(),
            value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("bob@test.com"))),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: Some(ConflictClause::Replace),
        returning: None,
    };

    let rows_updated = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(rows_updated, 1, "Update should succeed after deleting conflicting row");

    // Verify Bob is deleted and Alice now has Bob's email
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1, "Bob should have been deleted");

    let row = &table.scan()[0];
    assert_eq!(row.values[0], SqlValue::Integer(1), "Alice should keep her id=1");
    assert_eq!(
        row.values[1],
        SqlValue::Varchar(arcstr::ArcStr::from("bob@test.com")),
        "Alice should have Bob's email"
    );
}

#[test]
fn test_update_or_replace_no_conflict() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    insert_user(&mut db, 1, "alice@test.com", "Alice");
    insert_user(&mut db, 2, "bob@test.com", "Bob");

    // UPDATE OR REPLACE with no conflicts should work normally without deleting anyone
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        table_name: "users".to_string(),
        quoted: false,
        alias: None,
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice Updated"))),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: Some(ConflictClause::Replace),
        returning: None,
    };

    let rows_updated = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(rows_updated, 1);

    // Verify both rows still exist
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2, "No rows should be deleted when there's no conflict");
}

// ============================================================================
// NOT NULL with IGNORE
// ============================================================================

#[test]
fn test_insert_or_ignore_not_null_violation() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    // Try INSERT OR IGNORE with NULL in NOT NULL column (email)
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(1)),
            Expression::Literal(SqlValue::Null), // NOT NULL violation
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: Some(ConflictClause::Ignore),
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 0, "Row with NOT NULL violation should be ignored");

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_update_or_ignore_not_null_violation() {
    let mut db = Database::new();
    setup_users_table(&mut db);

    insert_user(&mut db, 1, "alice@test.com", "Alice");

    // Try UPDATE OR IGNORE to set email to NULL (NOT NULL violation)
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        table_name: "users".to_string(),
        quoted: false,
        alias: None,
        assignments: vec![Assignment {
            column: "email".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: Some(ConflictClause::Ignore),
        returning: None,
    };

    let rows_updated = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(rows_updated, 0, "Update with NOT NULL violation should be ignored");

    // Verify email is unchanged
    let table = db.get_table("users").unwrap();
    let alice = &table.scan()[0];
    assert_eq!(alice.values[1], SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com")));
}

// ============================================================================
// ON CONFLICT ... DO NOTHING Tests (SQLite Upsert Clause)
// ============================================================================

#[test]
fn test_insert_on_conflict_do_nothing_primary_key() {
    use vibesql_ast::{OnConflictAction, OnConflictClause};

    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert initial row
    insert_user(&mut db, 1, "alice@test.com", "Alice");

    // Try to insert conflicting row with ON CONFLICT DO NOTHING
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(1)), // Duplicate id
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("duplicate@test.com"))),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Duplicate"))),
        ]]),
        conflict_clause: None,
        on_conflict: vec![OnConflictClause {
            conflict_target: Some(vec![vibesql_ast::ConflictTargetItem::Column("id".to_string())]),
            target_where: None,
            target_inexact: false,
            action: OnConflictAction::DoNothing,
        }],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 0, "Conflicting insert should be ignored by ON CONFLICT DO NOTHING");

    // Verify original row is unchanged
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
    let alice = &table.scan()[0];
    assert_eq!(alice.values[1], SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com")));
}

#[test]
fn test_insert_on_conflict_do_nothing_without_target() {
    use vibesql_ast::{OnConflictAction, OnConflictClause};

    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert initial row
    insert_user(&mut db, 1, "alice@test.com", "Alice");

    // Try to insert conflicting row with ON CONFLICT DO NOTHING (no conflict target)
    // This should ignore any constraint violation
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(1)), // Duplicate id
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("dup@test.com"))),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Dup"))),
        ]]),
        conflict_clause: None,
        on_conflict: vec![OnConflictClause {
            conflict_target: None, // No specific conflict target
            target_where: None,
            target_inexact: false,
            action: OnConflictAction::DoNothing,
        }],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 0, "Conflicting insert should be ignored by ON CONFLICT DO NOTHING");

    // Verify original row is unchanged
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_on_conflict_do_nothing_no_conflict() {
    use vibesql_ast::{OnConflictAction, OnConflictClause};

    let mut db = Database::new();
    setup_users_table(&mut db);

    // Insert with ON CONFLICT DO NOTHING when there's no conflict - should succeed
    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "users".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(vec![vec![
            Expression::Literal(SqlValue::Integer(1)),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("alice@test.com"))),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_conflict: vec![OnConflictClause {
            conflict_target: Some(vec![vibesql_ast::ConflictTargetItem::Column("id".to_string())]),
            target_where: None,
            target_inexact: false,
            action: OnConflictAction::DoNothing,
        }],
        on_duplicate_key_update: None,
        returning: None,
    };

    let rows_inserted = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows_inserted, 1, "Non-conflicting insert should succeed");

    // Verify row was inserted
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}
