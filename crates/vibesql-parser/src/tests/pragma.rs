//! Tests for PRAGMA statement parsing (SQLite compatibility)

use vibesql_ast::{PragmaValue, Statement};

use crate::Parser;

#[test]
fn test_pragma_simple() {
    let sql = "PRAGMA table_info";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "table_info");
            assert!(stmt.value.is_none());
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_with_function_value() {
    let sql = "PRAGMA table_info(users)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "table_info");
            match &stmt.value {
                Some(PragmaValue::Identifier(v)) => assert_eq!(v, "users"),
                other => panic!("Expected Identifier value, got: {:?}", other),
            }
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_with_assignment() {
    let sql = "PRAGMA cache_size = 2000";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "cache_size");
            match &stmt.value {
                Some(PragmaValue::Number(v)) => assert_eq!(v, "2000"),
                other => panic!("Expected Number value, got: {:?}", other),
            }
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_with_database_qualifier() {
    let sql = "PRAGMA main.cache_size";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert_eq!(stmt.database.as_deref(), Some("main"));
            assert_eq!(stmt.name, "cache_size");
            assert!(stmt.value.is_none());
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_with_database_and_assignment() {
    let sql = "PRAGMA main.cache_size = 5000";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert_eq!(stmt.database.as_deref(), Some("main"));
            assert_eq!(stmt.name, "cache_size");
            match &stmt.value {
                Some(PragmaValue::Number(v)) => assert_eq!(v, "5000"),
                other => panic!("Expected Number value, got: {:?}", other),
            }
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_with_string_value() {
    let sql = "PRAGMA journal_mode = 'DELETE'";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "journal_mode");
            match &stmt.value {
                Some(PragmaValue::String(v)) => assert_eq!(v, "DELETE"),
                other => panic!("Expected String value, got: {:?}", other),
            }
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_with_identifier_value() {
    let sql = "PRAGMA synchronous = OFF";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "synchronous");
            match &stmt.value {
                // Identifiers preserve original case from SQL
                Some(PragmaValue::Identifier(v)) => assert_eq!(v, "OFF"),
                other => panic!("Expected Identifier value, got: {:?}", other),
            }
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_with_negative_number() {
    let sql = "PRAGMA cache_size = -2000";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "cache_size");
            match &stmt.value {
                Some(PragmaValue::SignedNumber(v)) => assert_eq!(v, "-2000"),
                other => panic!("Expected SignedNumber value, got: {:?}", other),
            }
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_encoding() {
    let sql = "PRAGMA encoding = 'UTF-8'";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "encoding");
            match &stmt.value {
                Some(PragmaValue::String(v)) => assert_eq!(v, "UTF-8"),
                other => panic!("Expected String value, got: {:?}", other),
            }
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}

#[test]
fn test_pragma_integrity_check() {
    let sql = "PRAGMA integrity_check";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Pragma(stmt) => {
            assert!(stmt.database.is_none());
            assert_eq!(stmt.name, "integrity_check");
            assert!(stmt.value.is_none());
        }
        other => panic!("Expected Pragma, got: {:?}", other),
    }
}
