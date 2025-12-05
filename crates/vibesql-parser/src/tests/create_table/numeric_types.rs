use super::super::*;

// ========================================================================
// Numeric Data Types - Integer, Float, and Decimal Types
// ========================================================================

#[test]
fn test_parse_create_table_integer_types() {
    let result =
        Parser::parse_sql("CREATE TABLE numbers (small SMALLINT, medium INTEGER, big BIGINT);");
    assert!(result.is_ok(), "Should parse integer types");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match create.columns[0].data_type {
                vibesql_types::DataType::Smallint => {} // Success
                _ => panic!("Expected SMALLINT, got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                vibesql_types::DataType::Integer => {} // Success
                _ => panic!("Expected INTEGER, got {:?}", create.columns[1].data_type),
            }

            match create.columns[2].data_type {
                vibesql_types::DataType::Bigint => {} // Success
                _ => panic!("Expected BIGINT, got {:?}", create.columns[2].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_long_type() {
    // LONG is an alias for BIGINT (MySQL/Oracle compatibility)
    let result = Parser::parse_sql("CREATE TABLE test (id LONG, value LONG COMMENT 'test');");
    assert!(result.is_ok(), "Should parse LONG type");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match create.columns[0].data_type {
                vibesql_types::DataType::Bigint => {} // LONG maps to Bigint
                _ => {
                    panic!("Expected LONG to map to Bigint, got {:?}", create.columns[0].data_type)
                }
            }

            match create.columns[1].data_type {
                vibesql_types::DataType::Bigint => {} // LONG maps to Bigint
                _ => {
                    panic!("Expected LONG to map to Bigint, got {:?}", create.columns[1].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_float_types() {
    let result = Parser::parse_sql("CREATE TABLE floats (a FLOAT, b REAL, c DOUBLE PRECISION);");
    assert!(result.is_ok(), "Should parse floating point types");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match create.columns[0].data_type {
                vibesql_types::DataType::Float { .. } => {} // Success
                _ => panic!("Expected FLOAT, got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                vibesql_types::DataType::Real => {} // Success
                _ => panic!("Expected REAL, got {:?}", create.columns[1].data_type),
            }

            match create.columns[2].data_type {
                vibesql_types::DataType::DoublePrecision => {} // Success
                _ => panic!("Expected DOUBLE PRECISION, got {:?}", create.columns[2].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_double_without_precision() {
    let result = Parser::parse_sql("CREATE TABLE test (value DOUBLE);");
    assert!(result.is_ok(), "DOUBLE without PRECISION should parse");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                vibesql_types::DataType::DoublePrecision => {} // Success
                _ => panic!("Expected DOUBLE to be treated as DOUBLE PRECISION"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_numeric_with_precision_and_scale() {
    let result =
        Parser::parse_sql("CREATE TABLE prices (amount NUMERIC(10, 2), total DECIMAL(15, 4));");
    assert!(result.is_ok(), "Should parse NUMERIC with precision and scale");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match create.columns[0].data_type {
                vibesql_types::DataType::Numeric { precision: 10, scale: 2 } => {} // Success
                _ => panic!("Expected NUMERIC(10, 2), got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                vibesql_types::DataType::Numeric { precision: 15, scale: 4 } => {} // Success
                _ => panic!("Expected NUMERIC(15, 4), got {:?}", create.columns[1].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_numeric_with_precision_only() {
    let result = Parser::parse_sql("CREATE TABLE test (price NUMERIC(10));");
    assert!(result.is_ok(), "Should parse NUMERIC with precision only");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                vibesql_types::DataType::Numeric { precision: 10, scale: 0 } => {} // Scale defaults to 0
                _ => panic!("Expected NUMERIC(10, 0), got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_numeric_without_parameters() {
    let result = Parser::parse_sql("CREATE TABLE test (price NUMERIC, amount DECIMAL);");
    assert!(result.is_ok(), "Should parse NUMERIC/DECIMAL without parameters");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            // Should default to (38, 0) per SQL standard
            match create.columns[0].data_type {
                vibesql_types::DataType::Numeric { precision: 38, scale: 0 } => {} // Success
                _ => {
                    panic!("Expected NUMERIC(38, 0) default, got {:?}", create.columns[0].data_type)
                }
            }

            match create.columns[1].data_type {
                vibesql_types::DataType::Numeric { precision: 38, scale: 0 } => {} // Success
                _ => {
                    panic!("Expected NUMERIC(38, 0) default, got {:?}", create.columns[1].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
