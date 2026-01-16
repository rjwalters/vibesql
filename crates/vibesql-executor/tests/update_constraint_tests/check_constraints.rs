use vibesql_ast::{Assignment, Expression, UpdateStmt};
use vibesql_executor::{ExecutorError, UpdateExecutor};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use super::constraint_test_utils::{
    create_employees_table_with_check_bonus, create_products_table_with_check_price,
    create_products_table_with_nullable_price,
};

#[test]
fn test_update_check_constraint_passes() {
    let mut db = Database::new();
    create_products_table_with_check_price(&mut db);

    // Insert a row
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Update to valid price (should succeed)
    let stmt = UpdateStmt { with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Integer(100)),
        }],
        where_clause: None,
        conflict_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_update_check_constraint_violation() {
    let mut db = Database::new();
    create_products_table_with_check_price(&mut db);

    // Insert a row
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Try to update to negative price (should fail)
    let stmt = UpdateStmt { with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Integer(-10)),
        }],
        where_clause: None,
        conflict_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SqliteCompatError(msg) => {
            // SQLite-compatible error format: "CHECK constraint failed: <name_or_expr>"
            assert!(msg.contains("CHECK constraint failed"));
            assert!(msg.contains("price_positive"));
        }
        other => panic!("Expected SqliteCompatError, got {:?}", other),
    }
}

#[test]
fn test_update_check_constraint_with_null() {
    let mut db = Database::new();
    create_products_table_with_nullable_price(&mut db);

    // Insert a row
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Update to NULL (should succeed - NULL is treated as UNKNOWN which passes CHECK)
    let stmt = UpdateStmt { with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: None,
        conflict_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_update_check_constraint_with_expression() {
    let mut db = Database::new();
    create_employees_table_with_check_bonus(&mut db);

    // Insert a row
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10000)]),
    )
    .unwrap();

    // Update bonus to still be less than salary (should succeed)
    let stmt1 = UpdateStmt { with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            value: Expression::Literal(SqlValue::Integer(15000)),
        }],
        where_clause: None,
        conflict_clause: None,
    };
    let count = UpdateExecutor::execute(&stmt1, &mut db).unwrap();
    assert_eq!(count, 1);

    // Try to update bonus to be >= salary (should fail)
    let stmt2 = UpdateStmt { with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            value: Expression::Literal(SqlValue::Integer(60000)),
        }],
        where_clause: None,
        conflict_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt2, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SqliteCompatError(msg) => {
            // SQLite-compatible error format: "CHECK constraint failed: <name_or_expr>"
            assert!(msg.contains("CHECK constraint failed"));
            assert!(msg.contains("bonus_less_than_salary"));
        }
        other => panic!("Expected SqliteCompatError, got {:?}", other),
    }
}
