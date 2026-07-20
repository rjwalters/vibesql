use vibesql_ast::{Assignment, BinaryOperator, Expression, UpdateStmt, WhereClause};
use vibesql_executor::{ExecutorError, UpdateExecutor};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use super::constraint_test_utils::{
    create_employees_table_with_check_bonus, create_products_table_with_check_price,
    create_products_table_with_nullable_price, create_products_table_with_pk_and_check_price,
};

/// Build `WHERE id = <val>` — a simple single-column PK equality that routes
/// the UPDATE through the fast path in `update::fast_path`.
fn where_id_eq(val: i64) -> WhereClause {
    WhereClause::Condition(Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("id", false))),
        op: BinaryOperator::Equal,
        right: Box::new(Expression::Literal(SqlValue::Integer(val))),
    })
}

#[test]
fn test_update_check_constraint_passes() {
    let mut db = Database::new();
    create_products_table_with_check_price(&mut db);

    // Insert a row
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Update to valid price (should succeed)
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            columns: Vec::new(),
            value: Expression::Literal(SqlValue::Integer(100)),
        }],
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
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
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            columns: Vec::new(),
            value: Expression::Literal(SqlValue::Integer(-10)),
        }],
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
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
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            columns: Vec::new(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
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
    let stmt1 = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            columns: Vec::new(),
            value: Expression::Literal(SqlValue::Integer(15000)),
        }],
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
    };
    let count = UpdateExecutor::execute(&stmt1, &mut db).unwrap();
    assert_eq!(count, 1);

    // Try to update bonus to be >= salary (should fail)
    let stmt2 = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            columns: Vec::new(),
            value: Expression::Literal(SqlValue::Integer(60000)),
        }],
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
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

/// Regression: a single-row `WHERE id = ?` UPDATE on a table with a PRIMARY KEY
/// takes the fast path (`update::fast_path`). That path previously validated
/// NOT NULL only and silently bypassed CHECK constraints, so an invalid value
/// was written without error (check.test 9.2/9.3). The CHECK must now fire.
#[test]
fn test_update_check_constraint_violation_via_pk_fast_path() {
    let mut db = Database::new();
    create_products_table_with_pk_and_check_price(&mut db);

    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Update the row selected by its primary key to a value that violates the
    // `price >= 0` CHECK. This must be rejected, not silently applied.
    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            columns: Vec::new(),
            value: Expression::Literal(SqlValue::Integer(-5)),
        }],
        where_clause: Some(where_id_eq(1)),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err(), "CHECK must be enforced on the PK fast path");
    match result.unwrap_err() {
        ExecutorError::SqliteCompatError(msg) => {
            assert!(msg.contains("CHECK constraint failed"), "unexpected message: {msg}");
            assert!(msg.contains("price_positive"), "unexpected message: {msg}");
        }
        other => panic!("Expected SqliteCompatError, got {:?}", other),
    }

    // The invalid write must not have landed: the row still holds 50.
    let rows = db.get_table("products").unwrap().scan();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[1], SqlValue::Integer(50));
}

/// A valid single-row `WHERE id = ?` UPDATE through the PK fast path must still
/// succeed (CHECK enforcement must not reject legal values).
#[test]
fn test_update_check_constraint_passes_via_pk_fast_path() {
    let mut db = Database::new();
    create_products_table_with_pk_and_check_price(&mut db);

    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    let stmt = UpdateStmt {
        index_hint: None,
        with_clause: None,
        from_clause: None,
        quoted: false,
        alias: None,
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            columns: Vec::new(),
            value: Expression::Literal(SqlValue::Integer(100)),
        }],
        where_clause: Some(where_id_eq(1)),
        order_by: None,
        limit: None,
        offset: None,
        conflict_clause: None,
        returning: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
    let rows = db.get_table("products").unwrap().scan();
    assert_eq!(rows[0].values[1], SqlValue::Integer(100));
}
