use super::super::*;

// ========================================================================
// Spatial Data Types (SQL/MM Standard) - Issue #781
// These are not part of SQL:1999 but should parse gracefully as UserDefined types
// ========================================================================

#[test]
fn test_parse_create_table_multipolygon() {
    let result = Parser::parse_sql("CREATE TABLE t1(c1 MULTIPOLYGON, c2 MULTIPOLYGON);");
    assert!(result.is_ok(), "Should parse MULTIPOLYGON type");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!(
                    "Expected UserDefined MULTIPOLYGON, got {:?}",
                    create.columns[0].data_type
                ),
            }

            match &create.columns[1].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!(
                    "Expected UserDefined MULTIPOLYGON, got {:?}",
                    create.columns[1].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_all_spatial_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE spatial (
            pt POINT,
            line LINESTRING,
            poly POLYGON,
            mpt MULTIPOINT,
            mline MULTILINESTRING,
            mpoly MULTIPOLYGON,
            geom GEOMETRY,
            geomcoll GEOMETRYCOLLECTION
        );",
    );
    assert!(result.is_ok(), "Should parse all spatial types");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 8);

            let expected_types = [
                "POINT",
                "LINESTRING",
                "POLYGON",
                "MULTIPOINT",
                "MULTILINESTRING",
                "MULTIPOLYGON",
                "GEOMETRY",
                "GEOMETRYCOLLECTION",
            ];

            for (i, expected_type) in expected_types.iter().enumerate() {
                match &create.columns[i].data_type {
                    vibesql_types::DataType::UserDefined { type_name } => {
                        assert_eq!(
                            type_name, expected_type,
                            "Column {} should have type {}",
                            i, expected_type
                        );
                    }
                    _ => panic!(
                        "Expected UserDefined {}, got {:?}",
                        expected_type, create.columns[i].data_type
                    ),
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_spatial_types_case_insensitive() {
    // Test lowercase, uppercase, and mixed case
    let lowercase_result = Parser::parse_sql("CREATE TABLE t1(c1 multipolygon);");
    let uppercase_result = Parser::parse_sql("CREATE TABLE t2(c1 MULTIPOLYGON);");
    let mixed_result = Parser::parse_sql("CREATE TABLE t3(c1 MultiPolygon);");

    assert!(lowercase_result.is_ok(), "lowercase should parse");
    assert!(uppercase_result.is_ok(), "uppercase should parse");
    assert!(mixed_result.is_ok(), "mixed case should parse");

    // All should produce the same normalized type
    if let Ok(vibesql_ast::Statement::CreateTable(t1)) = lowercase_result {
        if let Ok(vibesql_ast::Statement::CreateTable(t2)) = uppercase_result {
            if let Ok(vibesql_ast::Statement::CreateTable(t3)) = mixed_result {
                match (&t1.columns[0].data_type, &t2.columns[0].data_type, &t3.columns[0].data_type)
                {
                    (
                        vibesql_types::DataType::UserDefined { type_name: name1 },
                        vibesql_types::DataType::UserDefined { type_name: name2 },
                        vibesql_types::DataType::UserDefined { type_name: name3 },
                    ) => {
                        assert_eq!(name1, "MULTIPOLYGON", "Should normalize to uppercase");
                        assert_eq!(name2, "MULTIPOLYGON", "Should normalize to uppercase");
                        assert_eq!(name3, "MULTIPOLYGON", "Should normalize to uppercase");
                    }
                    _ => panic!("Expected UserDefined types for all variants"),
                }
            }
        }
    }
}

#[test]
fn test_unknown_type_still_fails() {
    // Non-spatial unknown types should still fail
    let result = Parser::parse_sql("CREATE TABLE t1(c1 UNKNOWNTYPE);");
    assert!(result.is_err(), "Unknown non-spatial types should fail");

    if let Err(e) = result {
        assert!(
            e.to_string().contains("Unknown data type: UNKNOWNTYPE"),
            "Error should mention unknown type"
        );
    }
}

#[test]
fn test_sqllogictest_multipolygon_with_comment() {
    // Test from actual SQLLogicTest suite
    // Note: This test is currently ignored because COMMENT clause may not be fully supported
    let result = Parser::parse_sql(
        "CREATE TABLE `t1710a` (`c1` MULTIPOLYGON COMMENT 'text155459', `c2` MULTIPOLYGON COMMENT 'text155461');",
    );

    if let Err(ref e) = result {
        eprintln!("Parse error: {}", e);
    }

    assert!(result.is_ok(), "Should parse MULTIPOLYGON with COMMENT clause");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            // Check first column
            assert_eq!(create.columns[0].name, "c1");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!(
                    "Expected UserDefined MULTIPOLYGON, got {:?}",
                    create.columns[0].data_type
                ),
            }
            assert_eq!(create.columns[0].comment, Some("text155459".to_string()));

            // Check second column
            assert_eq!(create.columns[1].name, "c2");
            match &create.columns[1].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!(
                    "Expected UserDefined MULTIPOLYGON, got {:?}",
                    create.columns[1].data_type
                ),
            }
            assert_eq!(create.columns[1].comment, Some("text155461".to_string()));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_sqllogictest_multipolygon_basic() {
    // Simplified test without COMMENT clause to verify core spatial type parsing
    let result = Parser::parse_sql("CREATE TABLE t1710a (c1 MULTIPOLYGON, c2 MULTIPOLYGON);");
    assert!(result.is_ok(), "Should parse MULTIPOLYGON columns");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!(
                    "Expected UserDefined MULTIPOLYGON, got {:?}",
                    create.columns[0].data_type
                ),
            }

            match &create.columns[1].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!(
                    "Expected UserDefined MULTIPOLYGON, got {:?}",
                    create.columns[1].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_default_before_comment_mysql_standard() {
    // Test MySQL standard order: DEFAULT before COMMENT
    // Per MySQL 8.4 Reference Manual:
    // column_definition: data_type [DEFAULT {literal | (expr)}] [COMMENT 'string']
    let result = Parser::parse_sql("CREATE TABLE t (col INT DEFAULT 5 COMMENT 'test column');");

    if let Err(ref e) = result {
        eprintln!("Parse error: {}", e);
    }

    assert!(result.is_ok(), "Should parse DEFAULT before COMMENT (MySQL standard order)");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);

            let col = &create.columns[0];
            assert_eq!(col.name, "COL");

            // Verify DEFAULT value is parsed
            assert!(col.default_value.is_some(), "Should have default value");

            // Verify COMMENT is parsed
            assert_eq!(col.comment, Some("test column".to_string()));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_int_with_null_constraint() {
    // Test if NULL constraint works with regular types
    let result = Parser::parse_sql("CREATE TABLE t (c1 INT NULL);");
    if let Err(ref e) = result {
        eprintln!("INT NULL Parse error: {}", e);
    }
    assert!(result.is_ok(), "INT NULL should parse");
}

#[test]
fn test_set_type_parsing() {
    // Test MySQL SET type with values
    let result = Parser::parse_sql("CREATE TABLE t (c2 SET ('a', 'b') UNIQUE);");
    if let Err(ref e) = result {
        eprintln!("SET type Parse error: {}", e);
    }
    assert!(result.is_ok(), "SET type should parse");
}

#[test]
fn test_set_type_with_binary_literals() {
    // Test MySQL SET type with binary literal values
    let result = Parser::parse_sql("CREATE TABLE t (c2 SET ('0b10', '0b1001') UNIQUE KEY);");
    if let Err(ref e) = result {
        eprintln!("SET type with binary literals Parse error: {}", e);
    }
    assert!(result.is_ok(), "SET type with binary literals should parse");
}

#[test]
fn test_polygon_with_null_constraint() {
    // Test spatial type POLYGON with NULL column constraint
    // Issue #1315: Support MySQL table options in sqllogictest
    let result = Parser::parse_sql("CREATE TABLE t (c1 POLYGON NULL);");

    if let Err(ref e) = result {
        eprintln!("Parse error: {}", e);
    }

    assert!(result.is_ok(), "Should parse POLYGON with NULL constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "C1");
            assert!(create.columns[0].nullable, "Column should be nullable");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
