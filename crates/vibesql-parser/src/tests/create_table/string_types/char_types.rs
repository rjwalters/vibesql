use super::super::super::*;

// ========================================================================
// CHAR and CHARACTER Type Tests
// ========================================================================

#[test]
fn test_parse_char_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR(10 CHARACTERS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_char_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR(10 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_char_without_modifier_still_works() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR(10));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_char_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (A CHAR);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "A");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - defaults to 1
                _ => panic!("Expected CHAR(1) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_character_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (A CHARACTER);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "A");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - defaults to 1
                _ => panic!("Expected CHARACTER(1) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// NCHAR Tests
// ========================================================================

#[test]
fn test_parse_nchar_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR(20));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 20 } => {} // Success
                _ => panic!("Expected CHAR(20) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - default is 1
                _ => panic!("Expected CHAR(1) data type (default)"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR(10 CHARACTERS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// NATIONAL CHARACTER Tests
// ========================================================================

#[test]
fn test_parse_national_character_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHARACTER(15));");
    assert!(result.is_ok(), "Failed to parse NATIONAL CHARACTER: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 15 } => {} // Success
                _ => panic!("Expected CHAR(15) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_character_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHARACTER);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - default is 1
                _ => panic!("Expected CHAR(1) data type (default)"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_char_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHAR(10));");
    assert!(result.is_ok(), "Failed to parse NATIONAL CHAR: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_character_equivalence() {
    // NATIONAL CHARACTER should be identical to NCHAR
    let national_result = Parser::parse_sql("CREATE TABLE t1 (x NATIONAL CHARACTER(10));");
    let nchar_result = Parser::parse_sql("CREATE TABLE t2 (x NCHAR(10));");

    assert!(national_result.is_ok());
    assert!(nchar_result.is_ok());

    let national_stmt = national_result.unwrap();
    let nchar_stmt = nchar_result.unwrap();

    match (national_stmt, nchar_stmt) {
        (
            vibesql_ast::Statement::CreateTable(national_create),
            vibesql_ast::Statement::CreateTable(nchar_create),
        ) => {
            // Both should produce the same data type
            assert_eq!(national_create.columns[0].data_type, nchar_create.columns[0].data_type);
        }
        _ => panic!("Expected CREATE TABLE statements"),
    }
}

#[test]
fn test_parse_national_character_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHARACTER(12 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "x");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 12 } => {} // Success
                _ => panic!("Expected CHAR(12) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
