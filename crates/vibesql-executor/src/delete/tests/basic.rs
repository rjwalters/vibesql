//! Basic DELETE operation tests

use super::common::setup_users_table_with_active as setup_test_table;
use vibesql_ast::{BinaryOperator, DeleteStmt, Expression, WhereClause};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::DeleteExecutor;

#[test]
fn test_delete_all_rows() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users;
    let stmt = DeleteStmt { only: false, table_name: "users".to_string(), where_clause: None };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 3);

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_delete_with_simple_where() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE id = 2;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        })),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 1);

    // Verify Bob is deleted, Alice and Charlie remain
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);

    // Use scan_live() to get only non-deleted rows
    let remaining: Vec<i64> = table
        .scan_live()
        .map(|(_, row)| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
        .collect();

    assert!(remaining.contains(&1)); // Alice
    assert!(remaining.contains(&3)); // Charlie
    assert!(!remaining.contains(&2)); // Bob deleted
}

#[test]
fn test_delete_with_boolean_where() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE active = TRUE;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "active".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        })),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 2); // Alice and Charlie

    // Verify only Bob remains
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    // Use scan_live() to get only non-deleted rows
    let remaining_id = table
        .scan_live()
        .next()
        .map(|(_, row)| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
        .unwrap_or(0);
    assert_eq!(remaining_id, 2); // Bob
}

#[test]
fn test_delete_multiple_rows() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE id > 1;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 2); // Bob and Charlie

    // Verify only Alice remains
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    let remaining_name = if let SqlValue::Varchar(name) = table.scan()[0].get(1).unwrap() {
        name.clone()
    } else { arcstr::ArcStr::from("") };
    assert_eq!(remaining_name.as_str(), "Alice");
}
