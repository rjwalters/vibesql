use super::super::*;

// ========================================================================
// SQLite Type Aliases - Multi-word type names
// ========================================================================
// SQLite accepts ANY type name and determines affinity based on the
// type name string. Multi-word types like "LARGE BLOB", "NATIVE CHARACTER",
// "VARYING CHARACTER" should be accepted and stored as UserDefined types.
//
// See: https://www.sqlite.org/datatype3.html#type_affinity

#[test]
fn test_parse_large_blob() {
    let result = Parser::parse_sql("CREATE TABLE t1(a LARGE BLOB);");
    assert!(result.is_ok(), "Should parse LARGE BLOB type: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    // Check case-insensitively since lexer may normalize differently
                    assert!(
                        type_name.to_uppercase().contains("LARGE"),
                        "Type should contain LARGE: {}",
                        type_name
                    );
                    assert!(
                        type_name.to_uppercase().contains("BLOB"),
                        "Type should contain BLOB: {}",
                        type_name
                    );
                }
                other => panic!("Expected UserDefined, got {:?}", other),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_native_character() {
    let result = Parser::parse_sql("CREATE TABLE t2(b NATIVE CHARACTER(70));");
    assert!(result.is_ok(), "Should parse NATIVE CHARACTER type: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    // Type name includes the identifier parts
                    assert!(
                        type_name.to_uppercase().contains("NATIVE"),
                        "Type should contain NATIVE"
                    );
                    assert!(
                        type_name.to_uppercase().contains("CHARACTER"),
                        "Type should contain CHARACTER"
                    );
                }
                other => panic!("Expected UserDefined, got {:?}", other),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varying_character() {
    let result = Parser::parse_sql("CREATE TABLE t3(c VARYING CHARACTER(255));");
    assert!(result.is_ok(), "Should parse VARYING CHARACTER type: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert!(
                        type_name.to_uppercase().contains("VARYING"),
                        "Type should contain VARYING"
                    );
                    assert!(
                        type_name.to_uppercase().contains("CHARACTER"),
                        "Type should contain CHARACTER"
                    );
                }
                other => panic!("Expected UserDefined, got {:?}", other),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// Note: UNSIGNED BIG INT and LONG VARCHAR are special cases.
// UNSIGNED and LONG are recognized as types (Unsigned and Bigint respectively),
// so multi-word types starting with these words would need additional handling.
// SQLite does accept these, but our current implementation parses them differently.
// The primary fix in this PR handles cases where the first word is unknown
// (like LARGE, NATIVE, VARYING).

#[test]
fn test_sqlite_type_affinity_blob() {
    // LARGE BLOB should have BLOB/NONE affinity because it contains "BLOB"
    let user_defined = vibesql_types::DataType::UserDefined { type_name: "large blob".to_string() };
    assert_eq!(
        user_defined.sqlite_affinity(),
        vibesql_types::TypeAffinity::None,
        "LARGE BLOB should have NONE affinity"
    );
}

#[test]
fn test_sqlite_type_affinity_character() {
    // NATIVE CHARACTER should have TEXT affinity because it contains "CHAR"
    let user_defined =
        vibesql_types::DataType::UserDefined { type_name: "native character".to_string() };
    assert_eq!(
        user_defined.sqlite_affinity(),
        vibesql_types::TypeAffinity::Text,
        "NATIVE CHARACTER should have TEXT affinity"
    );
}

#[test]
fn test_sqlite_type_affinity_integer() {
    // UNSIGNED BIG INT should have INTEGER affinity because it contains "INT"
    let user_defined =
        vibesql_types::DataType::UserDefined { type_name: "unsigned big int".to_string() };
    assert_eq!(
        user_defined.sqlite_affinity(),
        vibesql_types::TypeAffinity::Integer,
        "UNSIGNED BIG INT should have INTEGER affinity"
    );
}

#[test]
fn test_sqlite_type_affinity_clob() {
    // CLOB types should have TEXT affinity
    let user_defined = vibesql_types::DataType::UserDefined { type_name: "large clob".to_string() };
    assert_eq!(
        user_defined.sqlite_affinity(),
        vibesql_types::TypeAffinity::Text,
        "LARGE CLOB should have TEXT affinity"
    );
}

#[test]
fn test_sqlite_type_affinity_real() {
    // DOUBLE types should have REAL affinity
    let user_defined =
        vibesql_types::DataType::UserDefined { type_name: "my double type".to_string() };
    assert_eq!(
        user_defined.sqlite_affinity(),
        vibesql_types::TypeAffinity::Real,
        "Types containing DOUB should have REAL affinity"
    );
}

#[test]
fn test_sqlite_type_affinity_numeric_fallback() {
    // Unknown types without affinity keywords should have NUMERIC affinity
    let user_defined =
        vibesql_types::DataType::UserDefined { type_name: "custom_type".to_string() };
    assert_eq!(
        user_defined.sqlite_affinity(),
        vibesql_types::TypeAffinity::Numeric,
        "Unknown types should have NUMERIC affinity"
    );
}

#[test]
fn test_multiple_columns_with_sqlite_types() {
    // Test the types mentioned in the issue that should now work
    let result = Parser::parse_sql(
        "CREATE TABLE types2 (
            a LARGE BLOB,
            b NATIVE CHARACTER(70),
            c VARYING CHARACTER(255),
            d INTEGER
        );",
    );
    assert!(result.is_ok(), "Should parse table with SQLite type aliases: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 4, "Should have 4 columns");
            assert_eq!(create.table_name, "types2");

            // Verify affinities are correct per SQLite rules
            // LARGE BLOB → NONE affinity (contains BLOB)
            assert_eq!(
                create.columns[0].data_type.sqlite_affinity(),
                vibesql_types::TypeAffinity::None
            );
            // NATIVE CHARACTER → TEXT affinity (contains CHAR)
            assert_eq!(
                create.columns[1].data_type.sqlite_affinity(),
                vibesql_types::TypeAffinity::Text
            );
            // VARYING CHARACTER → TEXT affinity (contains CHAR)
            assert_eq!(
                create.columns[2].data_type.sqlite_affinity(),
                vibesql_types::TypeAffinity::Text
            );
            // INTEGER → INTEGER affinity
            assert_eq!(
                create.columns[3].data_type.sqlite_affinity(),
                vibesql_types::TypeAffinity::Integer
            );
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
