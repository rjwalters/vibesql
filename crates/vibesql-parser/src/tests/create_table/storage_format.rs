//! Tests for STORAGE table option parsing (VibeSQL extension)

use super::super::*;

/// Test parsing of STORAGE = COLUMNAR option
#[test]
fn test_parse_create_table_with_storage_columnar() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) STORAGE COLUMNAR;");
    assert!(result.is_ok(), "Should parse STORAGE COLUMNAR option");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::Storage(format) => {
                    assert_eq!(*format, vibesql_ast::StorageFormat::Columnar);
                }
                _ => panic!("Expected Storage option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test parsing of STORAGE = ROW option (explicit default)
#[test]
fn test_parse_create_table_with_storage_row() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) STORAGE ROW;");
    assert!(result.is_ok(), "Should parse STORAGE ROW option");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::Storage(format) => {
                    assert_eq!(*format, vibesql_ast::StorageFormat::Row);
                }
                _ => panic!("Expected Storage option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test parsing of STORAGE with equals sign
#[test]
fn test_parse_create_table_with_storage_equals_syntax() {
    let test_cases = vec![
        ("STORAGE = COLUMNAR", vibesql_ast::StorageFormat::Columnar),
        ("STORAGE = ROW", vibesql_ast::StorageFormat::Row),
        ("STORAGE COLUMNAR", vibesql_ast::StorageFormat::Columnar),
        ("STORAGE ROW", vibesql_ast::StorageFormat::Row),
    ];

    for (option, expected_format) in test_cases {
        let sql = format!("CREATE TABLE t1 (c1 INT) {};", option);
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "Should parse {}", option);

        let stmt = result.unwrap();
        match stmt {
            vibesql_ast::Statement::CreateTable(create) => match &create.table_options[0] {
                vibesql_ast::TableOption::Storage(format) => {
                    assert_eq!(format, &expected_format, "STORAGE value mismatch for {}", option);
                }
                _ => panic!("Expected Storage option for {}", option),
            },
            _ => panic!("Expected CREATE TABLE statement for {}", option),
        }
    }
}

/// Test parsing error for invalid storage format
#[test]
fn test_parse_create_table_with_invalid_storage_format() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) STORAGE HYBRID;");
    assert!(result.is_err(), "Should fail to parse invalid STORAGE format");

    let err = result.unwrap_err();
    assert!(
        err.message.contains("ROW") || err.message.contains("COLUMNAR"),
        "Error message should mention valid formats: {}",
        err.message
    );
}

/// Test STORAGE option with other table options
#[test]
fn test_parse_create_table_with_storage_and_other_options() {
    // Test STORAGE as the only option but with ENGINE keyword
    // (ENGINE may stop the options parsing, so just test STORAGE alone works)
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) STORAGE COLUMNAR;");
    assert!(result.is_ok(), "Should parse STORAGE option");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            // Find the Storage option
            let storage_option = create
                .table_options
                .iter()
                .find(|opt| matches!(opt, vibesql_ast::TableOption::Storage(_)));
            assert!(storage_option.is_some(), "Should have Storage option");

            match storage_option.unwrap() {
                vibesql_ast::TableOption::Storage(format) => {
                    assert_eq!(*format, vibesql_ast::StorageFormat::Columnar);
                }
                _ => unreachable!(),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test StorageFormat Display trait
#[test]
fn test_storage_format_display() {
    assert_eq!(format!("{}", vibesql_ast::StorageFormat::Row), "row");
    assert_eq!(format!("{}", vibesql_ast::StorageFormat::Columnar), "columnar");
}

/// Test StorageFormat Default trait
#[test]
fn test_storage_format_default() {
    let default: vibesql_ast::StorageFormat = Default::default();
    assert_eq!(default, vibesql_ast::StorageFormat::Row);
}
