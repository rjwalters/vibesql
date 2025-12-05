//! Tests for advanced SQL object DDL parsing (SQL:1999 features)
//!
//! Tests for SEQUENCE, TYPE, COLLATION, CHARACTER SET, and TRANSLATION statements.

use vibesql_ast::{Statement, TypeDefinition};
use vibesql_parser::Parser;

#[test]
fn test_create_sequence_basic() {
    let sql = "CREATE SEQUENCE test_seq";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SEQUENCE");

    match stmt {
        Statement::CreateSequence(create_stmt) => {
            assert_eq!(create_stmt.sequence_name, "TEST_SEQ");
            assert_eq!(create_stmt.start_with, None);
            assert_eq!(create_stmt.increment_by, 1);
            assert_eq!(create_stmt.min_value, None);
            assert_eq!(create_stmt.max_value, None);
            assert!(!create_stmt.cycle);
        }
        _ => panic!("Expected CreateSequence statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_sequence_with_options() {
    let sql =
        "CREATE SEQUENCE test_seq START WITH 100 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 CYCLE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SEQUENCE with options");

    match stmt {
        Statement::CreateSequence(create_stmt) => {
            assert_eq!(create_stmt.sequence_name, "TEST_SEQ");
            assert_eq!(create_stmt.start_with, Some(100));
            assert_eq!(create_stmt.increment_by, 5);
            assert_eq!(create_stmt.min_value, Some(1));
            assert_eq!(create_stmt.max_value, Some(1000));
            assert!(create_stmt.cycle);
        }
        _ => panic!("Expected CreateSequence statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_sequence_no_min_max() {
    let sql = "CREATE SEQUENCE test_seq NO MINVALUE NO MAXVALUE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SEQUENCE with NO MIN/MAX");

    match stmt {
        Statement::CreateSequence(create_stmt) => {
            assert_eq!(create_stmt.sequence_name, "TEST_SEQ");
            assert_eq!(create_stmt.min_value, None);
            assert_eq!(create_stmt.max_value, None);
        }
        _ => panic!("Expected CreateSequence statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_sequence_no_cycle() {
    let sql = "CREATE SEQUENCE test_seq NO CYCLE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SEQUENCE NO CYCLE");

    match stmt {
        Statement::CreateSequence(create_stmt) => {
            assert_eq!(create_stmt.sequence_name, "TEST_SEQ");
            assert!(!create_stmt.cycle);
        }
        _ => panic!("Expected CreateSequence statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_sequence_basic() {
    let sql = "DROP SEQUENCE test_seq";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SEQUENCE");

    match stmt {
        Statement::DropSequence(drop_stmt) => {
            assert_eq!(drop_stmt.sequence_name, "TEST_SEQ");
            assert!(!drop_stmt.cascade); // RESTRICT is default
        }
        _ => panic!("Expected DropSequence statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_sequence_cascade() {
    let sql = "DROP SEQUENCE test_seq CASCADE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SEQUENCE CASCADE");

    match stmt {
        Statement::DropSequence(drop_stmt) => {
            assert_eq!(drop_stmt.sequence_name, "TEST_SEQ");
            assert!(drop_stmt.cascade);
        }
        _ => panic!("Expected DropSequence statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_sequence_restrict() {
    let sql = "DROP SEQUENCE test_seq RESTRICT";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SEQUENCE RESTRICT");

    match stmt {
        Statement::DropSequence(drop_stmt) => {
            assert_eq!(drop_stmt.sequence_name, "TEST_SEQ");
            assert!(!drop_stmt.cascade);
        }
        _ => panic!("Expected DropSequence statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_type_distinct() {
    let sql = "CREATE TYPE test_type AS DISTINCT INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE TYPE DISTINCT");

    match stmt {
        Statement::CreateType(create_stmt) => {
            assert_eq!(create_stmt.type_name, "TEST_TYPE");
            match create_stmt.definition {
                TypeDefinition::Distinct { base_type } => {
                    assert!(matches!(base_type, vibesql_types::DataType::Integer));
                }
                _ => panic!("Expected Distinct type definition"),
            }
        }
        _ => panic!("Expected CreateType statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_type_structured() {
    let sql = "CREATE TYPE person_type AS (name VARCHAR(100), age INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE TYPE structured");

    match stmt {
        Statement::CreateType(create_stmt) => {
            assert_eq!(create_stmt.type_name, "PERSON_TYPE");
            match create_stmt.definition {
                TypeDefinition::Structured { attributes } => {
                    assert_eq!(attributes.len(), 2);
                    assert_eq!(attributes[0].name, "NAME");
                    assert_eq!(attributes[1].name, "AGE");
                    // Check data types
                    assert!(matches!(
                        attributes[0].data_type,
                        vibesql_types::DataType::Varchar { .. }
                    ));
                    assert!(matches!(attributes[1].data_type, vibesql_types::DataType::Integer));
                }
                _ => panic!("Expected Structured type definition"),
            }
        }
        _ => panic!("Expected CreateType statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_type_basic() {
    let sql = "DROP TYPE test_type";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP TYPE");

    match stmt {
        Statement::DropType(drop_stmt) => {
            assert_eq!(drop_stmt.type_name, "TEST_TYPE");
            assert!(matches!(drop_stmt.behavior, vibesql_ast::DropBehavior::Restrict));
        }
        _ => panic!("Expected DropType statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_type_cascade() {
    let sql = "DROP TYPE test_type CASCADE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP TYPE CASCADE");

    match stmt {
        Statement::DropType(drop_stmt) => {
            assert_eq!(drop_stmt.type_name, "TEST_TYPE");
            assert!(matches!(drop_stmt.behavior, vibesql_ast::DropBehavior::Cascade));
        }
        _ => panic!("Expected DropType statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_type_restrict() {
    let sql = "DROP TYPE test_type RESTRICT";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP TYPE RESTRICT");

    match stmt {
        Statement::DropType(drop_stmt) => {
            assert_eq!(drop_stmt.type_name, "TEST_TYPE");
            assert!(matches!(drop_stmt.behavior, vibesql_ast::DropBehavior::Restrict));
        }
        _ => panic!("Expected DropType statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_collation() {
    let sql = "CREATE COLLATION test_collation";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE COLLATION");

    match stmt {
        Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "TEST_COLLATION");
        }
        _ => panic!("Expected CreateCollation statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_collation() {
    let sql = "DROP COLLATION test_collation";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP COLLATION");

    match stmt {
        Statement::DropCollation(drop_stmt) => {
            assert_eq!(drop_stmt.collation_name, "TEST_COLLATION");
        }
        _ => panic!("Expected DropCollation statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_character_set() {
    let sql = "CREATE CHARACTER SET test_charset";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE CHARACTER SET");

    match stmt {
        Statement::CreateCharacterSet(create_stmt) => {
            assert_eq!(create_stmt.charset_name, "TEST_CHARSET");
        }
        _ => panic!("Expected CreateCharacterSet statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_character_set() {
    let sql = "DROP CHARACTER SET test_charset";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP CHARACTER SET");

    match stmt {
        Statement::DropCharacterSet(drop_stmt) => {
            assert_eq!(drop_stmt.charset_name, "TEST_CHARSET");
        }
        _ => panic!("Expected DropCharacterSet statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_translation() {
    let sql = "CREATE TRANSLATION test_translation";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE TRANSLATION");

    match stmt {
        Statement::CreateTranslation(create_stmt) => {
            assert_eq!(create_stmt.translation_name, "TEST_TRANSLATION");
        }
        _ => panic!("Expected CreateTranslation statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_translation() {
    let sql = "DROP TRANSLATION test_translation";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP TRANSLATION");

    match stmt {
        Statement::DropTranslation(drop_stmt) => {
            assert_eq!(drop_stmt.translation_name, "TEST_TRANSLATION");
        }
        _ => panic!("Expected DropTranslation statement, got {:?}", stmt),
    }
}

#[test]
fn test_advanced_objects_case_insensitive() {
    // Test case insensitivity for keywords
    let sql_variants = vec![
        "create sequence test",
        "CREATE SEQUENCE test",
        "Create Sequence test",
        "drop sequence test",
        "DROP SEQUENCE test",
        "create type test as distinct integer",
        "CREATE TYPE test AS DISTINCT INTEGER",
        "drop type test",
        "DROP TYPE test",
        "create collation test",
        "CREATE COLLATION test",
        "drop collation test",
        "DROP COLLATION test",
        "create character set test",
        "CREATE CHARACTER SET test",
        "drop character set test",
        "DROP CHARACTER SET test",
        "create translation test",
        "CREATE TRANSLATION test",
        "drop translation test",
        "DROP TRANSLATION test",
    ];

    for sql in sql_variants {
        Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse case variant: {}", sql));
    }
}

#[test]
fn test_sequence_parse_errors() {
    let invalid_statements = vec![
        "CREATE SEQUENCE", // Missing sequence name
        "DROP SEQUENCE",   // Missing sequence name
    ];

    for sql in invalid_statements {
        let result = Parser::parse_sql(sql);
        assert!(result.is_err(), "Expected parse error for: {}", sql);
    }
}

#[test]
fn test_type_parse_errors() {
    let invalid_statements = vec![
        "CREATE TYPE",                  // Missing type name
        "CREATE TYPE test AS",          // Missing type definition
        "CREATE TYPE test AS INVALID",  // Invalid type definition
        "CREATE TYPE test AS DISTINCT", // Missing base type
        "CREATE TYPE test AS (",        // Incomplete structured type
        "CREATE TYPE test AS (name)",   // Missing data type
        "DROP TYPE",                    // Missing type name
    ];

    for sql in invalid_statements {
        let result = Parser::parse_sql(sql);
        assert!(result.is_err(), "Expected parse error for: {}", sql);
    }
}

#[test]
fn test_collation_character_set_translation_parse_errors() {
    let invalid_statements = vec![
        "CREATE COLLATION",     // Missing collation name
        "DROP COLLATION",       // Missing collation name
        "CREATE CHARACTER SET", // Missing charset name
        "DROP CHARACTER SET",   // Missing charset name
        "CREATE TRANSLATION",   // Missing translation name
        "DROP TRANSLATION",     // Missing translation name
    ];

    for sql in invalid_statements {
        let result = Parser::parse_sql(sql);
        assert!(result.is_err(), "Expected parse error for: {}", sql);
    }
}

#[test]
fn test_sequence_complex_options() {
    // Test various combinations of sequence options
    let sql = "CREATE SEQUENCE complex_seq START WITH 50 INCREMENT BY -1 MINVALUE 1 MAXVALUE 100 NO CYCLE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse complex sequence");

    match stmt {
        Statement::CreateSequence(create_stmt) => {
            assert_eq!(create_stmt.sequence_name, "COMPLEX_SEQ");
            assert_eq!(create_stmt.start_with, Some(50));
            assert_eq!(create_stmt.increment_by, -1);
            assert_eq!(create_stmt.min_value, Some(1));
            assert_eq!(create_stmt.max_value, Some(100));
            assert!(!create_stmt.cycle);
        }
        _ => panic!("Expected CreateSequence statement"),
    }
}

#[test]
fn test_type_structured_complex() {
    let sql = "CREATE TYPE address_type AS (street VARCHAR(255), city VARCHAR(100), zip_code CHAR(5), country VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse complex structured type");

    match stmt {
        Statement::CreateType(create_stmt) => {
            assert_eq!(create_stmt.type_name, "ADDRESS_TYPE");
            match create_stmt.definition {
                TypeDefinition::Structured { attributes } => {
                    assert_eq!(attributes.len(), 4);
                    assert_eq!(attributes[0].name, "STREET");
                    assert_eq!(attributes[1].name, "CITY");
                    assert_eq!(attributes[2].name, "ZIP_CODE");
                    assert_eq!(attributes[3].name, "COUNTRY");
                }
                _ => panic!("Expected Structured type definition"),
            }
        }
        _ => panic!("Expected CreateType statement"),
    }
}
