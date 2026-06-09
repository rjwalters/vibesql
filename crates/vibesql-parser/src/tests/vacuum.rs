//! Tests for VACUUM statement parsing (SQLite compatibility)

use bumpalo::Bump;
use vibesql_ast::Statement;

use crate::{arena_parser::ArenaParser, Parser};

#[test]
fn test_vacuum_simple() {
    let sql = "VACUUM";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Vacuum(stmt) => {
            assert!(stmt.schema_name.is_none());
            assert!(stmt.into_file.is_none());
        }
        other => panic!("Expected Vacuum, got: {:?}", other),
    }
}

#[test]
fn test_vacuum_with_semicolon() {
    let sql = "VACUUM;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Vacuum(stmt) => {
            assert!(stmt.schema_name.is_none());
            assert!(stmt.into_file.is_none());
        }
        other => panic!("Expected Vacuum, got: {:?}", other),
    }
}

#[test]
fn test_vacuum_with_schema_name() {
    let sql = "VACUUM main";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Vacuum(stmt) => {
            let schema = stmt.schema_name.expect("expected schema name");
            assert!(schema.eq_ignore_ascii_case("main"), "got: {schema}");
            assert!(stmt.into_file.is_none());
        }
        other => panic!("Expected Vacuum, got: {:?}", other),
    }
}

#[test]
fn test_vacuum_into_file() {
    let sql = "VACUUM INTO 'foo.db'";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Vacuum(stmt) => {
            assert!(stmt.schema_name.is_none());
            assert_eq!(stmt.into_file.as_deref(), Some("foo.db"));
        }
        other => panic!("Expected Vacuum, got: {:?}", other),
    }
}

#[test]
fn test_vacuum_schema_into_file() {
    let sql = "VACUUM main INTO 'backup.db'";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Vacuum(stmt) => {
            let schema = stmt.schema_name.expect("expected schema name");
            assert!(schema.eq_ignore_ascii_case("main"), "got: {schema}");
            assert_eq!(stmt.into_file.as_deref(), Some("backup.db"));
        }
        other => panic!("Expected Vacuum, got: {:?}", other),
    }
}

#[test]
fn test_vacuum_into_without_filename_is_error() {
    let sql = "VACUUM INTO";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "Expected parse error for VACUUM INTO without filename");
}

#[test]
fn test_arena_vacuum_simple() {
    let arena = Bump::new();
    let result = ArenaParser::parse_sql("VACUUM", &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        vibesql_ast::arena::Statement::Vacuum(stmt) => {
            assert!(stmt.schema_name.is_none());
            assert!(stmt.into_file.is_none());
        }
        other => panic!("Expected Vacuum, got: {:?}", other),
    };
}
