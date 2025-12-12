//! Tests for CREATE COLLATION and DROP COLLATION parsing

use crate::Parser;

#[test]
fn test_create_collation_minimal() {
    let sql = "CREATE COLLATION my_collation";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, None);
            assert_eq!(create_stmt.source_collation, None);
            assert_eq!(create_stmt.pad_space, None);
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_collation_with_for() {
    let sql = "CREATE COLLATION my_collation FOR utf8";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, Some("utf8".to_string()));
            assert_eq!(create_stmt.source_collation, None);
            assert_eq!(create_stmt.pad_space, None);
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_collation_with_from() {
    let sql = "CREATE COLLATION my_collation FROM unicode";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, None);
            assert_eq!(create_stmt.source_collation, Some("unicode".to_string()));
            assert_eq!(create_stmt.pad_space, None);
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_collation_with_from_string_literal() {
    let sql = "CREATE COLLATION my_collation FROM 'de_DE'";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, None);
            assert_eq!(create_stmt.source_collation, Some("de_DE".to_string()));
            assert_eq!(create_stmt.pad_space, None);
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_collation_with_pad_space() {
    let sql = "CREATE COLLATION my_collation PAD SPACE";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, None);
            assert_eq!(create_stmt.source_collation, None);
            assert_eq!(create_stmt.pad_space, Some(true));
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_collation_with_no_pad() {
    let sql = "CREATE COLLATION my_collation NO PAD";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, None);
            assert_eq!(create_stmt.source_collation, None);
            assert_eq!(create_stmt.pad_space, Some(false));
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_collation_full_syntax() {
    let sql = "CREATE COLLATION my_collation FOR utf8 FROM unicode PAD SPACE";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, Some("utf8".to_string()));
            assert_eq!(create_stmt.source_collation, Some("unicode".to_string()));
            assert_eq!(create_stmt.pad_space, Some(true));
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_create_collation_full_with_no_pad() {
    let sql = "CREATE COLLATION my_collation FOR utf8 FROM unicode NO PAD";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateCollation(create_stmt) => {
            assert_eq!(create_stmt.collation_name, "my_collation");
            assert_eq!(create_stmt.character_set, Some("utf8".to_string()));
            assert_eq!(create_stmt.source_collation, Some("unicode".to_string()));
            assert_eq!(create_stmt.pad_space, Some(false));
        }
        _ => panic!("Expected CreateCollation statement, got: {:?}", stmt),
    }
}

#[test]
fn test_drop_collation() {
    let sql = "DROP COLLATION my_collation";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::DropCollation(drop_stmt) => {
            assert_eq!(drop_stmt.collation_name, "my_collation");
        }
        _ => panic!("Expected DropCollation statement, got: {:?}", stmt),
    }
}
