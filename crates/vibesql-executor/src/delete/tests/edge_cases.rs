//! Edge cases and error handling tests for DELETE operations

use vibesql_ast::{BinaryOperator, DeleteStmt, Expression, WhereClause};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

use super::common::setup_users_table_with_active as setup_test_table;
use crate::{errors::ExecutorError, DeleteExecutor};

#[test]
fn test_delete_table_not_found() {
    let mut db = Database::new();

    let stmt =
        DeleteStmt { only: false, table_name: "nonexistent".to_string(), where_clause: None };

    let result = DeleteExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
}

#[test]
fn test_delete_no_matching_rows() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE id = 999;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(999))),
        })),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 0);

    // All rows should still exist
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_delete_from_empty_table() {
    let mut db = Database::new();

    // Create empty table
    let schema = TableSchema::new(
        "empty_users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // DELETE FROM empty_users;
    let stmt =
        DeleteStmt { only: false, table_name: "empty_users".to_string(), where_clause: None };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 0);
}

#[test]
fn test_delete_column_not_found() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE nonexistent_column = 1;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "nonexistent_column".to_string(),
            }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };

    // Error should be caught during evaluation, rows kept (safe default)
    let result = DeleteExecutor::execute(&stmt, &mut db);

    // Should succeed with 0 deletions (errors kept rows safe)
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);

    // All rows should still exist
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}
