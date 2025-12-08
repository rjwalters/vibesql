//! Tests for ONEPASS UPDATE optimizations
//!
//! These tests verify the ONEPASS optimization paths:
//! 1. SUPER-FAST path: Multiple literal assignments to non-indexed columns
//! 2. Composite PK detection: WHERE pk1 = val1 AND pk2 = val2

mod common;

use vibesql_ast::{Assignment, BinaryOperator, Expression, UpdateStmt, WhereClause};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::UpdateExecutor;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Create a table with a single-column PK and non-indexed columns for SUPER-FAST path testing
fn setup_simple_pk_table(db: &mut Database) {
    let mut schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
            ColumnSchema::new("quantity".to_string(), DataType::Integer, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    // Insert test data
    for i in 1..=3i64 {
        db.insert_row(
            "items",
            Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("Item {}", i))),
                SqlValue::Integer(100 * i),
                SqlValue::Integer(10 * i),
            ]),
        )
        .unwrap();
    }
}

/// Create a table with a composite PK for multi-column PK testing
fn setup_composite_pk_table(db: &mut Database) {
    let mut schema = TableSchema::new(
        "order_items".to_string(),
        vec![
            ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("item_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("quantity".to_string(), DataType::Integer, true),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
        ],
    );
    schema.primary_key = Some(vec!["order_id".to_string(), "item_id".to_string()]);
    db.create_table(schema).unwrap();

    // Insert test data
    // Order 1 has items 1, 2
    db.insert_row(
        "order_items",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(5),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "order_items",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
            SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    // Order 2 has item 1
    db.insert_row(
        "order_items",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Integer(150),
        ]),
    )
    .unwrap();
}

// =============================================================================
// SUPER-FAST PATH TESTS: Multiple literal assignments
// =============================================================================

#[test]
fn test_onepass_multiple_literal_assignments() {
    let mut db = Database::new();
    setup_simple_pk_table(&mut db);

    // UPDATE items SET name = 'Updated', price = 999, quantity = 50 WHERE id = 1
    let stmt = UpdateStmt {
        table_name: "items".to_string(),
        assignments: vec![
            Assignment {
                column: "name".to_string(),
                value: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Updated"))),
            },
            Assignment {
                column: "price".to_string(),
                value: Expression::Literal(SqlValue::Integer(999)),
            },
            Assignment {
                column: "quantity".to_string(),
                value: Expression::Literal(SqlValue::Integer(50)),
            },
        ],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify all columns were updated
    let table = db.get_table("items").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Varchar(arcstr::ArcStr::from("Updated")));
    assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(999));
    assert_eq!(row.get(3).unwrap(), &SqlValue::Integer(50));

    // Verify other rows weren't affected
    let row2 = &table.scan()[1];
    assert_eq!(row2.get(1).unwrap(), &SqlValue::Varchar(arcstr::ArcStr::from("Item 2")));
    assert_eq!(row2.get(2).unwrap(), &SqlValue::Integer(200));
}

#[test]
fn test_onepass_single_literal_assignment() {
    let mut db = Database::new();
    setup_simple_pk_table(&mut db);

    // UPDATE items SET price = 777 WHERE id = 2
    let stmt = UpdateStmt {
        table_name: "items".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Integer(777)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    let table = db.get_table("items").unwrap();
    let row = &table.scan()[1];
    assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(777));
}

#[test]
fn test_onepass_pk_not_found_returns_zero() {
    let mut db = Database::new();
    setup_simple_pk_table(&mut db);

    // UPDATE items SET price = 999 WHERE id = 999 (non-existent)
    let stmt = UpdateStmt {
        table_name: "items".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Integer(999)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(999))),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_onepass_not_null_constraint_enforced() {
    let mut db = Database::new();

    // Create table with non-null column
    let mut schema = TableSchema::new(
        "required".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("required_field".to_string(), DataType::Integer, false), // NOT NULL
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    db.insert_row(
        "required",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();

    // Try to set NOT NULL column to NULL
    let stmt = UpdateStmt {
        table_name: "required".to_string(),
        assignments: vec![Assignment {
            column: "required_field".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("NOT NULL"));
}

// =============================================================================
// COMPOSITE PK TESTS: Multi-column primary key updates
// =============================================================================

#[test]
fn test_composite_pk_update_both_columns_specified() {
    let mut db = Database::new();
    setup_composite_pk_table(&mut db);

    // UPDATE order_items SET quantity = 99 WHERE order_id = 1 AND item_id = 1
    let stmt = UpdateStmt {
        table_name: "order_items".to_string(),
        assignments: vec![Assignment {
            column: "quantity".to_string(),
            value: Expression::Literal(SqlValue::Integer(99)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "order_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "item_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify the correct row was updated
    let table = db.get_table("order_items").unwrap();
    // Row (1,1) should have quantity=99
    let row = &table.scan()[0];
    assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(99));

    // Row (1,2) should be unchanged
    let row2 = &table.scan()[1];
    assert_eq!(row2.get(2).unwrap(), &SqlValue::Integer(3));
}

#[test]
fn test_composite_pk_update_reversed_order() {
    let mut db = Database::new();
    setup_composite_pk_table(&mut db);

    // UPDATE order_items SET price = 500 WHERE item_id = 2 AND order_id = 1
    // (columns in reverse order from PK definition)
    let stmt = UpdateStmt {
        table_name: "order_items".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Integer(500)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "item_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "order_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify row (1,2) was updated
    let table = db.get_table("order_items").unwrap();
    let row = &table.scan()[1]; // Row (1,2) is second
    assert_eq!(row.get(3).unwrap(), &SqlValue::Integer(500));
}

#[test]
fn test_composite_pk_partial_match_uses_scan() {
    let mut db = Database::new();
    setup_composite_pk_table(&mut db);

    // UPDATE order_items SET quantity = 1 WHERE order_id = 1
    // Only one PK column specified - should update multiple rows
    let stmt = UpdateStmt {
        table_name: "order_items".to_string(),
        assignments: vec![Assignment {
            column: "quantity".to_string(),
            value: Expression::Literal(SqlValue::Integer(1)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "order_id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 2); // Both (1,1) and (1,2) should be updated

    let table = db.get_table("order_items").unwrap();
    // Both rows for order_id=1 should have quantity=1
    assert_eq!(table.scan()[0].get(2).unwrap(), &SqlValue::Integer(1));
    assert_eq!(table.scan()[1].get(2).unwrap(), &SqlValue::Integer(1));
    // Row for order_id=2 should be unchanged
    assert_eq!(table.scan()[2].get(2).unwrap(), &SqlValue::Integer(10));
}

#[test]
fn test_composite_pk_not_found_returns_zero() {
    let mut db = Database::new();
    setup_composite_pk_table(&mut db);

    // UPDATE order_items SET quantity = 999 WHERE order_id = 99 AND item_id = 99
    let stmt = UpdateStmt {
        table_name: "order_items".to_string(),
        assignments: vec![Assignment {
            column: "quantity".to_string(),
            value: Expression::Literal(SqlValue::Integer(999)),
        }],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "order_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(99))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "item_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(99))),
            }),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_composite_pk_multiple_columns_updated() {
    let mut db = Database::new();
    setup_composite_pk_table(&mut db);

    // UPDATE order_items SET quantity = 100, price = 1000 WHERE order_id = 2 AND item_id = 1
    let stmt = UpdateStmt {
        table_name: "order_items".to_string(),
        assignments: vec![
            Assignment {
                column: "quantity".to_string(),
                value: Expression::Literal(SqlValue::Integer(100)),
            },
            Assignment {
                column: "price".to_string(),
                value: Expression::Literal(SqlValue::Integer(1000)),
            },
        ],
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "order_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "item_id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    let table = db.get_table("order_items").unwrap();
    let row = &table.scan()[2]; // Row (2,1) is third
    assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(100));
    assert_eq!(row.get(3).unwrap(), &SqlValue::Integer(1000));
}
