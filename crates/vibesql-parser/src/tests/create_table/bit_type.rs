use super::super::*;

// ========================================================================
// BIT Data Type - MySQL Binary Bit String Type
// ========================================================================

#[test]
fn test_parse_create_table_bit_no_length() {
    // BIT without length defaults to BIT(1)
    let result = Parser::parse_sql("CREATE TABLE test (flags BIT);");
    assert!(result.is_ok(), "Should parse BIT without length");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);

            match &create.columns[0].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, None, "Expected BIT without length (defaults to 1)");
                }
                _ => panic!("Expected BIT, got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_bit_with_length() {
    // BIT(n) with explicit length
    let result = Parser::parse_sql("CREATE TABLE test (flags BIT(8));");
    assert!(result.is_ok(), "Should parse BIT(8)");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);

            match &create.columns[0].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, Some(8), "Expected BIT(8)");
                }
                _ => panic!("Expected BIT(8), got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_bit_multiple_columns() {
    // Multiple BIT columns with different lengths
    let result =
        Parser::parse_sql("CREATE TABLE test (flags1 BIT, flags2 BIT(16), flags3 BIT(64));");
    assert!(result.is_ok(), "Should parse multiple BIT columns");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match &create.columns[0].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, None, "Expected BIT");
                }
                _ => panic!("Expected BIT, got {:?}", create.columns[0].data_type),
            }

            match &create.columns[1].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, Some(16), "Expected BIT(16)");
                }
                _ => panic!("Expected BIT(16), got {:?}", create.columns[1].data_type),
            }

            match &create.columns[2].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, Some(64), "Expected BIT(64)");
                }
                _ => panic!("Expected BIT(64), got {:?}", create.columns[2].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_bit_with_comment() {
    // BIT with COMMENT clause (from the failing test case)
    let result =
        Parser::parse_sql("CREATE TABLE t (c1 BIT COMMENT 'test', c2 BIT COMMENT 'test2');");
    assert!(result.is_ok(), "Should parse BIT with COMMENT");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match &create.columns[0].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, None, "Expected BIT");
                }
                _ => panic!("Expected BIT, got {:?}", create.columns[0].data_type),
            }

            match &create.columns[1].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, None, "Expected BIT");
                }
                _ => panic!("Expected BIT, got {:?}", create.columns[1].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_bit_max_length() {
    // MySQL BIT supports up to 64 bits
    let result = Parser::parse_sql("CREATE TABLE test (flags BIT(64));");
    assert!(result.is_ok(), "Should parse BIT(64)");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);

            match &create.columns[0].data_type {
                vibesql_types::DataType::Bit { length } => {
                    assert_eq!(*length, Some(64), "Expected BIT(64)");
                }
                _ => panic!("Expected BIT(64), got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
