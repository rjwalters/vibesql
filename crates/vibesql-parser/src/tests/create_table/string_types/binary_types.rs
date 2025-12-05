use super::super::super::*;

// ========================================================================
// BINARY and VARBINARY Type Tests (MySQL compatibility)
// ========================================================================

#[test]
fn test_parse_varbinary_with_size() {
    // VARBINARY(n) without space before parenthesis
    let result = Parser::parse_sql("CREATE TABLE t (x VARBINARY(4));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varbinary_with_size_and_space() {
    // VARBINARY (4) with space before parenthesis - this is the main issue #1662
    let result = Parser::parse_sql("CREATE TABLE t (x VARBINARY (4));");
    assert!(result.is_ok(), "Failed to parse VARBINARY with space: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varbinary_without_size() {
    // VARBINARY without size specification
    let result = Parser::parse_sql("CREATE TABLE t (x VARBINARY);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_binary_with_size() {
    // BINARY(n) without space before parenthesis
    let result = Parser::parse_sql("CREATE TABLE t (x BINARY(8));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "BINARY");
                }
                _ => panic!("Expected BINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_binary_with_size_and_space() {
    // BINARY (8) with space before parenthesis
    let result = Parser::parse_sql("CREATE TABLE t (x BINARY (8));");
    assert!(result.is_ok(), "Failed to parse BINARY with space: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "BINARY");
                }
                _ => panic!("Expected BINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_binary_without_size() {
    // BINARY without size specification
    let result = Parser::parse_sql("CREATE TABLE t (x BINARY);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "BINARY");
                }
                _ => panic!("Expected BINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varbinary_with_key_constraint() {
    // From the failing test case: VARBINARY (4) KEY
    let result = Parser::parse_sql("CREATE TABLE t (c1 VARBINARY (4) KEY);");
    assert!(result.is_ok(), "Failed to parse VARBINARY with KEY: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "C1");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
            // Also verify the KEY constraint was parsed
            assert!(create.columns[0]
                .constraints
                .iter()
                .any(|c| matches!(&c.kind, vibesql_ast::ColumnConstraintKind::Key)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
