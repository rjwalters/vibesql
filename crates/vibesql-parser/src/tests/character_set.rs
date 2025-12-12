//! Tests for CREATE CHARACTER SET and DROP CHARACTER SET parsing

use crate::Parser;

#[test]
fn test_create_character_set_minimal() {
    let sql = "CREATE CHARACTER SET utf8";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCharacterSet(create_stmt) => {
            assert_eq!(create_stmt.charset_name, "utf8");
            assert_eq!(create_stmt.source, None);
            assert_eq!(create_stmt.collation, None);
        }
        _ => panic!("Expected CreateCharacterSet statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_character_set_with_as() {
    let sql = "CREATE CHARACTER SET my_charset AS";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCharacterSet(create_stmt) => {
            assert_eq!(create_stmt.charset_name, "my_charset");
            assert_eq!(create_stmt.source, None);
            assert_eq!(create_stmt.collation, None);
        }
        _ => panic!("Expected CreateCharacterSet statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_character_set_with_get() {
    let sql = "CREATE CHARACTER SET my_charset GET utf8";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCharacterSet(create_stmt) => {
            assert_eq!(create_stmt.charset_name, "my_charset");
            assert_eq!(create_stmt.source, Some("utf8".to_string()));
            assert_eq!(create_stmt.collation, None);
        }
        _ => panic!("Expected CreateCharacterSet statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_character_set_with_collation() {
    let sql = "CREATE CHARACTER SET my_charset COLLATE FROM unicode";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCharacterSet(create_stmt) => {
            assert_eq!(create_stmt.charset_name, "my_charset");
            assert_eq!(create_stmt.source, None);
            assert_eq!(create_stmt.collation, Some("unicode".to_string()));
        }
        _ => panic!("Expected CreateCharacterSet statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_character_set_full_syntax() {
    let sql = "CREATE CHARACTER SET my_charset AS GET utf8 COLLATE FROM unicode";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCharacterSet(create_stmt) => {
            assert_eq!(create_stmt.charset_name, "my_charset");
            assert_eq!(create_stmt.source, Some("utf8".to_string()));
            assert_eq!(create_stmt.collation, Some("unicode".to_string()));
        }
        _ => panic!("Expected CreateCharacterSet statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_character_set_without_as() {
    let sql = "CREATE CHARACTER SET my_charset GET utf8 COLLATE FROM unicode";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCharacterSet(create_stmt) => {
            assert_eq!(create_stmt.charset_name, "my_charset");
            assert_eq!(create_stmt.source, Some("utf8".to_string()));
            assert_eq!(create_stmt.collation, Some("unicode".to_string()));
        }
        _ => panic!("Expected CreateCharacterSet statement, got: {:?}", stmt),
    }
}

#[test]
fn test_drop_character_set() {
    let sql = "DROP CHARACTER SET my_charset";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::DropCharacterSet(drop_stmt) => {
            assert_eq!(drop_stmt.charset_name, "my_charset");
        }
        _ => panic!("Expected DropCharacterSet statement, got: {:?}", stmt),
    }
}
