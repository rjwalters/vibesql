//! Common fixtures for insert constraint testing
//!
//! This module provides reusable table schemas, insert statement builders,
//! and assertion helpers for testing insert constraints across the test suite.

use vibesql_executor::ExecutorError;

/// Creates a basic users table with NOT NULL columns
/// Schema: users (id INT NOT NULL, name VARCHAR(50) NOT NULL)
#[allow(dead_code)]
pub fn create_not_null_table(db: &mut vibesql_storage::Database, table_name: &str) {
    let schema = vibesql_catalog::TableSchema::new(
        table_name.to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
}

/// Creates a users table with single-column primary key
/// Schema: users (id INT PRIMARY KEY, name VARCHAR(50))
#[allow(dead_code)]
pub fn create_single_pk_table(db: &mut vibesql_storage::Database) {
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Creates a table with composite primary key
/// Schema: order_items (order_id INT, item_id INT, qty INT, PRIMARY KEY (order_id, item_id))
#[allow(dead_code)]
pub fn create_composite_pk_table(db: &mut vibesql_storage::Database) {
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "order_items".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "order_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "item_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "qty".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
        vec!["order_id".to_string(), "item_id".to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Creates a users table with UNIQUE constraint on email
/// Schema: users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE)
#[allow(dead_code)]
pub fn create_unique_email_table(db: &mut vibesql_storage::Database) {
    let schema = vibesql_catalog::TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "email".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()]],
    );
    db.create_table(schema).unwrap();
}

/// Creates a table with composite UNIQUE constraint
/// Schema: enrollments (student_id INT, course_id INT, grade INT, UNIQUE (student_id, course_id))
#[allow(dead_code)]
pub fn create_composite_unique_table(db: &mut vibesql_storage::Database) {
    let schema = vibesql_catalog::TableSchema::with_unique_constraints(
        "enrollments".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "student_id".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "course_id".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "grade".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
        vec![vec!["student_id".to_string(), "course_id".to_string()]],
    );
    db.create_table(schema).unwrap();
}

/// Creates a users table with multiple UNIQUE constraints
/// Schema: users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE, username VARCHAR(50) UNIQUE)
#[allow(dead_code)]
pub fn create_multi_unique_table(db: &mut vibesql_storage::Database) {
    let schema = vibesql_catalog::TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "email".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "username".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()], vec!["username".to_string()]],
    );
    db.create_table(schema).unwrap();
}

/// Creates a products table with CHECK constraint (price >= 0)
#[allow(dead_code)]
pub fn create_check_price_table(db: &mut vibesql_storage::Database, nullable_price: bool) {
    let schema = vibesql_catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "price".to_string(),
                vibesql_types::DataType::Integer,
                nullable_price,
            ),
        ],
        None,
        Vec::new(),
        vec![(
            "price_positive".to_string(),
            vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(0),
                )),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();
}

/// Creates a products table with multiple CHECK constraints (price >= 0, quantity >= 0)
#[allow(dead_code)]
pub fn create_multi_check_table(db: &mut vibesql_storage::Database) {
    let schema = vibesql_catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "price".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "quantity".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
        None,
        Vec::new(),
        vec![
            (
                "price_positive".to_string(),
                vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                    }),
                    op: vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(0),
                    )),
                },
            ),
            (
                "quantity_positive".to_string(),
                vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "quantity".to_string(),
                    }),
                    op: vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(0),
                    )),
                },
            ),
        ],
        Vec::new(),
    );
    db.create_table(schema).unwrap();
}

/// Creates an employees table with CHECK constraint comparing columns (bonus < salary)
#[allow(dead_code)]
pub fn create_check_comparison_table(db: &mut vibesql_storage::Database) {
    let schema = vibesql_catalog::TableSchema::with_all_constraint_types(
        "employees".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "salary".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "bonus".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
        None,
        Vec::new(),
        vec![(
            "bonus_less_than_salary".to_string(),
            vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "bonus".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::LessThan,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();
}

/// Builds an insert statement with all columns
#[allow(dead_code)]
pub fn build_insert_values(
    table_name: &str,
    values: Vec<vibesql_types::SqlValue>,
) -> vibesql_ast::InsertStmt {
    vibesql_ast::InsertStmt {
        table_name: table_name.to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![values
            .into_iter()
            .map(vibesql_ast::Expression::Literal)
            .collect()]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    }
}

/// Builds an insert statement with specific columns
#[allow(dead_code)]
pub fn build_insert_columns(
    table_name: &str,
    columns: Vec<&str>,
    values: Vec<vibesql_types::SqlValue>,
) -> vibesql_ast::InsertStmt {
    vibesql_ast::InsertStmt {
        table_name: table_name.to_string(),
        columns: columns.iter().map(|s| s.to_string()).collect(),
        source: vibesql_ast::InsertSource::Values(vec![values
            .into_iter()
            .map(vibesql_ast::Expression::Literal)
            .collect()]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    }
}

/// Asserts that an error is a ConstraintViolation containing expected text
#[allow(dead_code)]
pub fn assert_constraint_violation(
    result: Result<usize, ExecutorError>,
    expected_fragments: &[&str],
) {
    assert!(result.is_err(), "Expected constraint violation, but operation succeeded");
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            for fragment in expected_fragments {
                assert!(
                    msg.contains(fragment),
                    "Expected error message to contain '{}', but got: {}",
                    fragment,
                    msg
                );
            }
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

/// Asserts that an error is a NOT NULL constraint violation
#[allow(dead_code)]
pub fn assert_not_null_violation(result: Result<usize, ExecutorError>, column: &str, table: &str) {
    assert_constraint_violation(
        result,
        &["NOT NULL constraint violation", column, table, "cannot be NULL"],
    );
}

/// Asserts that an error is a PRIMARY KEY constraint violation
#[allow(dead_code)]
pub fn assert_primary_key_violation(result: Result<usize, ExecutorError>) {
    assert_constraint_violation(result, &["PRIMARY KEY"]);
}

/// Asserts that an error is a UNIQUE constraint violation
#[allow(dead_code)]
pub fn assert_unique_violation(result: Result<usize, ExecutorError>, column: &str) {
    assert_constraint_violation(result, &["UNIQUE", column]);
}

/// Asserts that an error is a CHECK constraint violation
#[allow(dead_code)]
pub fn assert_check_violation(result: Result<usize, ExecutorError>, constraint_name: &str) {
    assert_constraint_violation(result, &["CHECK", constraint_name]);
}
