//! Executor error tests
//!
//! Tests for executor-level errors including division by zero, type mismatches,
//! constraints, and other execution-related errors.

use vibesql_ast::Statement;
use vibesql_executor::{
    AlterTableExecutor, CreateTableExecutor, ExecutorError, InsertExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_division_by_zero_error() {
    use vibesql_storage::DatabaseConfig;
    use vibesql_types::{MySqlModeFlags, SqlMode};

    // Create database with strict mode enabled to raise errors on division by zero
    let config = DatabaseConfig {
        sql_mode: SqlMode::MySQL {
            flags: MySqlModeFlags::with_strict_mode(),
        },
        ..DatabaseConfig::test_default()
    };
    let mut db = Database::with_config(config);

    // Create table with numeric data
    let sql = "CREATE TABLE numbers (value INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Insert test data
    let sql = "INSERT INTO numbers VALUES (10)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert should succeed");
    }

    // Try division by zero
    let sql = "SELECT value / 0 FROM numbers";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with DivisionByZero");

        match result {
            Err(ExecutorError::DivisionByZero) => {
                let error_msg = format!("{}", ExecutorError::DivisionByZero);
                assert!(error_msg.contains("Division by zero"));
            }
            other => panic!("Expected DivisionByZero error, got: {:?}", other),
        }
    }
}

#[test]
fn test_subquery_returned_multiple_rows_error() {
    let mut db = Database::new();

    // Create and populate table
    let sql = "CREATE TABLE items (id INTEGER, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Insert multiple rows
    let sql = "INSERT INTO items VALUES (1, 'Item1'), (2, 'Item2')";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert should succeed");
    }

    // Scalar subquery returning multiple rows
    let sql = "SELECT (SELECT id FROM items) AS single_value";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with SubqueryReturnedMultipleRows");

        match result {
            Err(ExecutorError::SubqueryReturnedMultipleRows { expected, actual }) => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
                let error_msg =
                    format!("{}", ExecutorError::SubqueryReturnedMultipleRows { expected, actual });
                assert!(error_msg.contains("returned") && error_msg.contains("rows"));
            }
            other => panic!("Expected SubqueryReturnedMultipleRows error, got: {:?}", other),
        }
    }
}

#[test]
fn test_type_mismatch_error() {
    let mut db = Database::new();

    // Create table
    let sql = "CREATE TABLE data (num INTEGER, text VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Insert test data
    let sql = "INSERT INTO data VALUES (42, 'hello')";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert should succeed");
    }

    // Try to add number and string (type mismatch)
    let sql = "SELECT num + text FROM data";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with TypeMismatch");

        match result {
            Err(ExecutorError::TypeMismatch { .. }) => {
                // TypeMismatch error successfully triggered
            }
            other => panic!("Expected TypeMismatch error, got: {:?}", other),
        }
    }
}

#[test]
fn test_constraint_violation_error() {
    let mut db = Database::new();

    // Create table with NOT NULL constraint
    let sql = "CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to insert NULL into NOT NULL column
    let sql = "INSERT INTO users (id, name) VALUES (NULL, 'John')";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        let result = InsertExecutor::execute(&mut db, &insert_stmt);
        assert!(result.is_err(), "Should fail with ConstraintViolation");

        match result {
            Err(ExecutorError::ConstraintViolation(msg)) => {
                let error_msg = format!("{}", ExecutorError::ConstraintViolation(msg.clone()));
                assert!(error_msg.contains("Constraint violation"));
            }
            other => panic!("Expected ConstraintViolation error, got: {:?}", other),
        }
    }
}

#[test]
fn test_cannot_drop_column_error() {
    let mut db = Database::new();

    // Create table with single column
    let sql = "CREATE TABLE minimal (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to drop the only column (should fail)
    let sql = "ALTER TABLE minimal DROP COLUMN id";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::AlterTable(alter_stmt) = stmt {
        let result = AlterTableExecutor::execute(&alter_stmt, &mut db);
        assert!(result.is_err(), "Should fail with CannotDropColumn");

        match result {
            Err(ExecutorError::CannotDropColumn(msg)) => {
                let error_msg = format!("{}", ExecutorError::CannotDropColumn(msg.clone()));
                assert!(error_msg.contains("Cannot drop column"));
            }
            other => panic!("Expected CannotDropColumn error, got: {:?}", other),
        }
    }
}

#[test]
fn test_cast_error() {
    // Test CastError display format
    let error = ExecutorError::CastError {
        from_type: "VARCHAR".to_string(),
        to_type: "INTEGER".to_string(),
    };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Cannot cast"));
    assert!(error_msg.contains("VARCHAR"));
    assert!(error_msg.contains("INTEGER"));
}

#[test]
fn test_column_index_out_of_bounds_error() {
    // Test ColumnIndexOutOfBounds display format
    let error = ExecutorError::ColumnIndexOutOfBounds { index: 99 };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column index"));
    assert!(error_msg.contains("99"));
    assert!(error_msg.contains("out of bounds"));
}

#[test]
fn test_permission_denied_error() {
    // Test PermissionDenied display format
    let error = ExecutorError::PermissionDenied {
        role: "guest".to_string(),
        privilege: "DELETE".to_string(),
        object: "users".to_string(),
    };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Permission denied"));
    assert!(error_msg.contains("guest"));
    assert!(error_msg.contains("DELETE"));
    assert!(error_msg.contains("users"));
}

#[test]
fn test_unsupported_expression_error() {
    // Test UnsupportedExpression display format
    let error = ExecutorError::UnsupportedExpression("LATERAL JOIN".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Unsupported expression"));
    assert!(error_msg.contains("LATERAL JOIN"));
}

#[test]
fn test_unsupported_feature_error() {
    // Test UnsupportedFeature display format
    let error = ExecutorError::UnsupportedFeature("Recursive CTEs".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Unsupported feature"));
    assert!(error_msg.contains("Recursive CTEs"));
}
