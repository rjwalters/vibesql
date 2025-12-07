//! Common test utilities for executor tests

pub mod insert_constraint_fixtures;

use vibesql_executor::ExpressionEvaluator;

/// Creates a test evaluator with a simple schema for testing.
/// Returns an evaluator and a simple test row.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn create_test_evaluator() -> (ExpressionEvaluator<'static>, vibesql_storage::Row) {
    let schema = Box::leak(Box::new(vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    )));

    let evaluator = ExpressionEvaluator::new(schema);
    let row = vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]);

    (evaluator, row)
}

/// Sets up the standard employees test table with sample data.
/// This table is used across multiple update test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_test_table(db: &mut vibesql_storage::Database) {
    // Create table schema
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
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
            vibesql_catalog::ColumnSchema::new(
                "salary".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "department".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(45000),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Integer(48000),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Integer(42000),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
        ]),
    )
    .unwrap();
}

/// Sets up a simple users test table with just id and name columns.
/// This table is used across multiple insert and transaction test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_users_table(db: &mut vibesql_storage::Database) {
    // CREATE TABLE users (id INT, name VARCHAR(50))
    let schema = vibesql_catalog::TableSchema::new(
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
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
}

/// Sets up a users test table with id, name, and active columns.
/// This table is used across delete test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_users_table_with_active(db: &mut vibesql_storage::Database) {
    // CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)
    let schema = vibesql_catalog::TableSchema::new(
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
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "active".to_string(),
                vibesql_types::DataType::Boolean,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Boolean(true),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Boolean(false),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Boolean(true),
        ]),
    )
    .unwrap();
}

/// Sets up a timestamps test table for timestamp format tests.
/// Returns the table name on success, or an error message.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_timestamps_table(db: &mut vibesql_storage::Database) -> Result<String, String> {
    use vibesql_ast::Statement;
    use vibesql_executor::CreateTableExecutor;
    use vibesql_parser::Parser;

    let sql = "CREATE TABLE timestamps (id INTEGER, ts TIMESTAMP)";
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(create_stmt) => CreateTableExecutor::execute(&create_stmt, db)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected CREATE TABLE statement, got {:?}", other)),
    }
}
