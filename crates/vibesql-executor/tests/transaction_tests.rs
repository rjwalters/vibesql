//! Transaction tests including SAVEPOINT functionality

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{
    BeginTransactionExecutor, CommitExecutor, RollbackToSavepointExecutor, SavepointExecutor,
};
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_basic_savepoint() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Begin transaction
    let begin_stmt = vibesql_ast::BeginStmt {
        durability: vibesql_ast::DurabilityHint::Default,
    };
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Insert initial row
    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(1000)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create savepoint
    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    // Insert another row
    let insert_stmt2 = vibesql_ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(500)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Check we have 2 rows
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 2);

    // Rollback to savepoint
    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    // Should only have 1 row now
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    // Commit
    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_nested_savepoints() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Begin transaction
    let begin_stmt = vibesql_ast::BeginStmt {
        durability: vibesql_ast::DurabilityHint::Default,
    };
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Insert initial row
    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(1000)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create first savepoint
    let savepoint_stmt1 = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt1, &mut db).unwrap();

    // Insert second row
    let insert_stmt2 = vibesql_ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(500)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Create second savepoint
    let savepoint_stmt2 = vibesql_ast::SavepointStmt { name: "sp2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt2, &mut db).unwrap();

    // Insert third row
    let insert_stmt3 = vibesql_ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(200)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt3).unwrap();

    // Check we have 3 rows
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 3);

    // Rollback to sp1 (should destroy sp2 and remove rows after sp1)
    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    // Should only have 1 row now
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    // Commit
    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}
