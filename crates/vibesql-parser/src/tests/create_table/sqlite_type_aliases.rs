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

// ========================================================================
// Multi-argument / signed type size specifiers (table-8.10)
// ========================================================================
// SQLite accepts arbitrary, possibly-signed, multi-argument size specifiers
// on any type name. The arguments do not affect affinity; the type *name*
// alone determines storage. See table.test table-8.10.

#[test]
fn test_parse_varchar_two_size_args() {
    let result = Parser::parse_sql("CREATE TABLE t(c VARCHAR(1,10));");
    assert!(result.is_ok(), "Should parse VARCHAR(1,10): {:?}", result.err());
}

#[test]
fn test_parse_varchar_signed_size_args() {
    let result = Parser::parse_sql("CREATE TABLE t(d VARCHAR(+1,-10), e VARCHAR (+1,-10));");
    assert!(result.is_ok(), "Should parse signed VARCHAR size args: {:?}", result.err());
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);
            // Both remain TEXT-affinity VARCHAR regardless of the (ignored) args.
            for col in &create.columns {
                assert_eq!(col.data_type.sqlite_affinity(), vibesql_types::TypeAffinity::Text);
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_unknown_type_signed_size_args() {
    // NUMBER(5,10)-style two-argument specifier on an unrecognized type name.
    let result = Parser::parse_sql("CREATE TABLE t(a NUMBER(+5,-10));");
    assert!(result.is_ok(), "Should parse NUMBER(+5,-10): {:?}", result.err());
}

// ========================================================================
// Delimited (quoted / bracketed) type names (table-8.9, table-8.10)
// ========================================================================
// A quoted or bracketed type name is taken verbatim as an opaque SQLite-style
// type whose storage is governed by affinity only.

#[test]
fn test_parse_bracket_quoted_type_name() {
    // table-8.9: CREATE TABLE t10("col.1" [char.3])
    let result = Parser::parse_sql(r#"CREATE TABLE t10("col.1" [char.3]);"#);
    assert!(result.is_ok(), "Should parse bracketed type name: {:?}", result.err());
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "char.3");
                    // Contains "char" → TEXT affinity.
                    assert_eq!(
                        create.columns[0].data_type.sqlite_affinity(),
                        vibesql_types::TypeAffinity::Text
                    );
                }
                other => panic!("Expected UserDefined, got {:?}", other),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_double_quoted_type_name_with_args_in_text() {
    // table-8.10: f "VARCHAR (+1,-10, 5)" — the entire parenthesized spec is
    // inside the quotes, so it is a single verbatim type name.
    let result = Parser::parse_sql(r#"CREATE TABLE t(f "VARCHAR (+1,-10, 5)");"#);
    assert!(result.is_ok(), "Should parse quoted type name verbatim: {:?}", result.err());
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.columns[0].data_type {
            vibesql_types::DataType::UserDefined { type_name } => {
                assert_eq!(type_name, "VARCHAR (+1,-10, 5)");
            }
            other => panic!("Expected UserDefined, got {:?}", other),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
