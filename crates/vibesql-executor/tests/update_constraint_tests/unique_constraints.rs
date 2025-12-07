use vibesql_ast::{Assignment, BinaryOperator, Expression, UpdateStmt};
use vibesql_executor::{ExecutorError, UpdateExecutor};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use super::constraint_test_utils::{
    create_update_with_id_clause, create_users_table_with_composite_unique,
    create_users_table_with_unique_email,
};

#[test]
fn test_update_unique_constraint_duplicate() {
    let mut db = Database::new();
    create_users_table_with_unique_email(&mut db);

    // Insert two users
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com"))]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("bob@example.com"))]),
    )
    .unwrap();

    // Try to update Bob's email to Alice's email (should fail - UNIQUE violation)
    let stmt = create_update_with_id_clause(
        "users",
        "email",
        SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
        2,
    );

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE"));
            assert!(msg.contains("email"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_unique_constraint_to_unique_value() {
    let mut db = Database::new();
    create_users_table_with_unique_email(&mut db);

    // Insert two users
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com"))]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("bob@example.com"))]),
    )
    .unwrap();

    // Update Bob's email to a new unique value - should succeed
    let stmt = create_update_with_id_clause(
        "users",
        "email",
        SqlValue::Varchar(arcstr::ArcStr::from("robert@example.com")),
        2,
    );

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify the update
    let table = db.get_table("users").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Varchar(arcstr::ArcStr::from("robert@example.com")));
}

#[test]
fn test_update_unique_constraint_allows_null() {
    let mut db = Database::new();
    create_users_table_with_unique_email(&mut db);

    // Insert two users with email
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com"))]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("bob@example.com"))]),
    )
    .unwrap();

    // Update first user's email to NULL - should succeed
    let stmt1 = create_update_with_id_clause("users", "email", SqlValue::Null, 1);
    let count = UpdateExecutor::execute(&stmt1, &mut db).unwrap();
    assert_eq!(count, 1);

    // Update second user's email to NULL too - should succeed (multiple NULLs allowed)
    let stmt2 = create_update_with_id_clause("users", "email", SqlValue::Null, 2);
    let count2 = UpdateExecutor::execute(&stmt2, &mut db).unwrap();
    assert_eq!(count2, 1);

    // Verify both emails are NULL
    let table = db.get_table("users").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Null);
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Null);
}

#[test]
fn test_update_unique_constraint_composite() {
    let mut db = Database::new();
    create_users_table_with_composite_unique(&mut db);

    // Insert two users
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Varchar(arcstr::ArcStr::from("Smith")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Varchar(arcstr::ArcStr::from("Jones")),
        ]),
    )
    .unwrap();

    // Try to update Bob to have the same first_name and last_name as Alice (should fail)
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![
            Assignment {
                column: "first_name".to_string(),
                value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
            },
            Assignment {
                column: "last_name".to_string(),
                value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Smith"))),
            },
        ],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        })),
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}
