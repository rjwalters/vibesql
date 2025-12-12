//! Tests for CREATE TRANSLATION and DROP TRANSLATION parsing

use crate::Parser;

#[test]
fn test_create_translation_minimal() {
    let sql = "CREATE TRANSLATION my_trans";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTranslation(create_stmt) => {
            assert_eq!(create_stmt.translation_name, "my_trans");
            assert_eq!(create_stmt.source_charset, None);
            assert_eq!(create_stmt.target_charset, None);
            assert_eq!(create_stmt.translation_source, None);
        }
        _ => panic!("Expected CreateTranslation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_translation_with_for_to() {
    let sql = "CREATE TRANSLATION my_trans FOR utf8 TO latin1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTranslation(create_stmt) => {
            assert_eq!(create_stmt.translation_name, "my_trans");
            assert_eq!(create_stmt.source_charset, Some("UTF8".to_string()));
            assert_eq!(create_stmt.target_charset, Some("LATIN1".to_string()));
            assert_eq!(create_stmt.translation_source, None);
        }
        _ => panic!("Expected CreateTranslation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_translation_with_from() {
    let sql = "CREATE TRANSLATION my_trans FROM converter";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTranslation(create_stmt) => {
            assert_eq!(create_stmt.translation_name, "my_trans");
            assert_eq!(create_stmt.source_charset, None);
            assert_eq!(create_stmt.target_charset, None);
            assert_eq!(create_stmt.translation_source, Some("converter".to_string()));
        }
        _ => panic!("Expected CreateTranslation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_translation_full_syntax() {
    let sql = "CREATE TRANSLATION my_trans FOR utf8 TO latin1 FROM converter";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTranslation(create_stmt) => {
            assert_eq!(create_stmt.translation_name, "my_trans");
            assert_eq!(create_stmt.source_charset, Some("UTF8".to_string()));
            assert_eq!(create_stmt.target_charset, Some("LATIN1".to_string()));
            assert_eq!(create_stmt.translation_source, Some("converter".to_string()));
        }
        _ => panic!("Expected CreateTranslation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_translation_for_without_to() {
    // FOR requires TO, but if only FOR is present, should still parse with just source_charset
    let sql = "CREATE TRANSLATION my_trans FOR utf8";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTranslation(create_stmt) => {
            assert_eq!(create_stmt.translation_name, "my_trans");
            assert_eq!(create_stmt.source_charset, Some("UTF8".to_string()));
            assert_eq!(create_stmt.target_charset, None);
            assert_eq!(create_stmt.translation_source, None);
        }
        _ => panic!("Expected CreateTranslation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_drop_translation() {
    let sql = "DROP TRANSLATION my_trans";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::DropTranslation(drop_stmt) => {
            assert_eq!(drop_stmt.translation_name, "my_trans");
        }
        _ => panic!("Expected DropTranslation statement, got: {:?}", stmt),
    }
}
