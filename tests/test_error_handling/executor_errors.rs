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
        sql_mode: SqlMode::MySQL { flags: MySqlModeFlags::with_strict_mode() },
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
fn test_subquery_multiple_rows_returns_first() {
    // SQLite-compatible behavior: When a scalar subquery returns multiple rows,
    // return the first row's value instead of erroring.
    // See: https://www.sqlite.org/lang_expr.html#scalar_subqueries
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

    // Scalar subquery returning multiple rows - should return the first value (1)
    let sql = "SELECT (SELECT id FROM items) AS single_value";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_ok(), "Should succeed with SQLite-compatible behavior");

        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        // First row's value should be 1 (the first id inserted)
        assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(1));
    }
}

#[test]
fn test_implicit_string_to_number_coercion() {
    // SQLite-compatible behavior: strings are implicitly coerced to numbers in arithmetic
    // Non-numeric strings become 0, numeric strings become their numeric value
    let mut db = Database::new();

    // Create table
    let sql = "CREATE TABLE data (num INTEGER, text VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Insert test data with non-numeric string
    let sql = "INSERT INTO data VALUES (42, 'hello')";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert should succeed");
    }

    // num + text should succeed: 'hello' coerces to 0, result is 42
    let sql = "SELECT num + text FROM data";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_ok(), "Should succeed with implicit coercion");
    }
}

#[test]
fn test_type_mismatch_error() {
    // Test TypeMismatch error display format
    let error = ExecutorError::TypeMismatch {
        left: vibesql_types::SqlValue::Integer(42),
        op: "+".to_string(),
        right: vibesql_types::SqlValue::Null,
    };

    let error_msg = format!("{}", error);
    assert!(
        error_msg.contains("Type mismatch")
            || error_msg.contains("type")
            || error_msg.contains("+")
    );
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
        assert!(result.is_err(), "Should fail with a NOT NULL constraint error");

        // NOT NULL violations report SQLite's exact wording via
        // `ExecutorError::SqliteCompatError` (see `row_validator.rs`), not the
        // generic `ConstraintViolation` variant.
        match result {
            Err(ExecutorError::SqliteCompatError(msg)) => {
                assert!(msg.contains("NOT NULL constraint failed"));
            }
            other => panic!("Expected SqliteCompatError(NOT NULL ...), got: {:?}", other),
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
        assert!(result.is_err(), "Should fail when dropping the only remaining column");

        // Dropping the last column reports SQLite-style wording via
        // `ExecutorError::Other` (see `execute_drop_column` in
        // `crates/vibesql-executor/src/alter/columns.rs`); the `CannotDropColumn`
        // variant is not constructed on this path.
        match result {
            Err(ExecutorError::Other(msg)) => {
                assert!(msg.contains("cannot drop column"));
                assert!(msg.contains("no other columns exist"));
            }
            other => panic!("Expected Other(\"cannot drop column ...\"), got: {:?}", other),
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
