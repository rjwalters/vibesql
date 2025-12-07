//! Tests for ExecutorError Display implementation

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

#[test]
fn test_table_not_found_display() {
    let error = ExecutorError::TableNotFound("users".to_string());
    assert_eq!(error.to_string(), "Table 'users' not found");
}

#[test]
fn test_table_already_exists_display() {
    let error = ExecutorError::TableAlreadyExists("products".to_string());
    assert_eq!(error.to_string(), "Table 'products' already exists");
}

#[test]
fn test_column_not_found_display() {
    // Test basic error without search info
    let error = ExecutorError::ColumnNotFound {
        column_name: "email".to_string(),
        table_name: "users".to_string(),
        searched_tables: vec![],
        available_columns: vec![],
    };
    assert_eq!(error.to_string(), "Column 'email' not found in table 'users'");

    // Test error with search info
    let error_with_info = ExecutorError::ColumnNotFound {
        column_name: "email".to_string(),
        table_name: "users".to_string(),
        searched_tables: vec!["users".to_string(), "profiles".to_string()],
        available_columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
    };
    assert_eq!(
        error_with_info.to_string(),
        "Column 'email' not found (searched tables: users, profiles). Available columns: id, name, age"
    );
}

#[test]
fn test_column_index_out_of_bounds_display() {
    let error = ExecutorError::ColumnIndexOutOfBounds { index: 5 };
    assert_eq!(error.to_string(), "Column index 5 out of bounds");
}

#[test]
fn test_type_mismatch_display() {
    let error = ExecutorError::TypeMismatch {
        left: SqlValue::Integer(42),
        op: "+".to_string(),
        right: SqlValue::Varchar(arcstr::ArcStr::from("hello")),
    };
    assert!(error.to_string().contains("Type mismatch"));
    assert!(error.to_string().contains("+"));
}

#[test]
fn test_division_by_zero_display() {
    let error = ExecutorError::DivisionByZero;
    assert_eq!(error.to_string(), "Division by zero");
}

#[test]
fn test_invalid_where_clause_display() {
    let error = ExecutorError::InvalidWhereClause("bad condition".to_string());
    assert_eq!(error.to_string(), "Invalid WHERE clause: bad condition");
}

#[test]
fn test_unsupported_expression_display() {
    let error = ExecutorError::UnsupportedExpression("CASE WHEN".to_string());
    assert_eq!(error.to_string(), "Unsupported expression: CASE WHEN");
}

#[test]
fn test_unsupported_feature_display() {
    let error = ExecutorError::UnsupportedFeature("window functions".to_string());
    assert_eq!(error.to_string(), "Unsupported feature: window functions");
}

#[test]
fn test_storage_error_display() {
    let error = ExecutorError::StorageError("disk full".to_string());
    assert_eq!(error.to_string(), "Storage error: disk full");
}

#[test]
fn test_subquery_returned_multiple_rows_display() {
    let error = ExecutorError::SubqueryReturnedMultipleRows { expected: 1, actual: 5 };
    assert_eq!(error.to_string(), "Scalar subquery returned 5 rows, expected 1");
}

#[test]
fn test_subquery_column_count_mismatch_display() {
    let error = ExecutorError::SubqueryColumnCountMismatch { expected: 1, actual: 3 };
    assert_eq!(error.to_string(), "Subquery returned 3 columns, expected 1");
}

#[test]
fn test_error_trait_implementation() {
    let error = ExecutorError::DivisionByZero;
    // Test that ExecutorError implements std::error::Error
    let _: &dyn std::error::Error = &error;
}
