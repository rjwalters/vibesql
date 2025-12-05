//! Tests for prepared statement parsing (PREPARE, EXECUTE, DEALLOCATE)
//! SQL:1999 Feature E141 - Basic integrity constraints
//!
//! Note: Unquoted SQL identifiers are normalized to uppercase by the parser

use crate::Parser;
use vibesql_ast::{DeallocateTarget, PreparedStatementBody, Statement};

// ============================================================================
// PREPARE statement tests
// ============================================================================

#[test]
fn test_parse_prepare_from_string() {
    let result = Parser::parse_sql("PREPARE my_select FROM 'SELECT * FROM users WHERE id = ?'");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Prepare(stmt) => {
            // Identifiers are normalized to uppercase
            assert_eq!(stmt.name, "MY_SELECT");
            assert!(stmt.param_types.is_none());
            match stmt.statement {
                PreparedStatementBody::SqlString(sql) => {
                    assert_eq!(sql, "SELECT * FROM users WHERE id = ?");
                }
                _ => panic!("Expected SqlString body"),
            }
        }
        other => panic!("Expected Prepare, got {:?}", other),
    }
}

#[test]
fn test_parse_prepare_as_select() {
    let result = Parser::parse_sql("PREPARE my_stmt AS SELECT * FROM users");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Prepare(stmt) => {
            assert_eq!(stmt.name, "MY_STMT");
            assert!(stmt.param_types.is_none());
            match stmt.statement {
                PreparedStatementBody::ParsedStatement(inner) => {
                    assert!(matches!(*inner, Statement::Select(_)));
                }
                _ => panic!("Expected ParsedStatement body"),
            }
        }
        other => panic!("Expected Prepare, got {:?}", other),
    }
}

#[test]
fn test_parse_prepare_with_param_types() {
    let result =
        Parser::parse_sql("PREPARE my_insert(INT, VARCHAR) AS INSERT INTO users VALUES ($1, $2)");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Prepare(stmt) => {
            assert_eq!(stmt.name, "MY_INSERT");
            let param_types = stmt.param_types.expect("Expected param_types");
            assert_eq!(param_types.len(), 2);
            assert_eq!(param_types[0], "INT");
            assert_eq!(param_types[1], "VARCHAR");
        }
        other => panic!("Expected Prepare, got {:?}", other),
    }
}

#[test]
fn test_parse_prepare_as_insert() {
    let result =
        Parser::parse_sql("PREPARE my_insert AS INSERT INTO users (id, name) VALUES (?, ?)");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Prepare(stmt) => {
            assert_eq!(stmt.name, "MY_INSERT");
            match stmt.statement {
                PreparedStatementBody::ParsedStatement(inner) => {
                    assert!(matches!(*inner, Statement::Insert(_)));
                }
                _ => panic!("Expected ParsedStatement body"),
            }
        }
        other => panic!("Expected Prepare, got {:?}", other),
    }
}

#[test]
fn test_parse_prepare_as_update() {
    let result = Parser::parse_sql("PREPARE my_update AS UPDATE users SET name = ? WHERE id = ?");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Prepare(stmt) => {
            assert_eq!(stmt.name, "MY_UPDATE");
            match stmt.statement {
                PreparedStatementBody::ParsedStatement(inner) => {
                    assert!(matches!(*inner, Statement::Update(_)));
                }
                _ => panic!("Expected ParsedStatement body"),
            }
        }
        other => panic!("Expected Prepare, got {:?}", other),
    }
}

#[test]
fn test_parse_prepare_as_delete() {
    let result = Parser::parse_sql("PREPARE my_delete AS DELETE FROM users WHERE id = ?");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Prepare(stmt) => {
            assert_eq!(stmt.name, "MY_DELETE");
            match stmt.statement {
                PreparedStatementBody::ParsedStatement(inner) => {
                    assert!(matches!(*inner, Statement::Delete(_)));
                }
                _ => panic!("Expected ParsedStatement body"),
            }
        }
        other => panic!("Expected Prepare, got {:?}", other),
    }
}

// ============================================================================
// EXECUTE statement tests
// ============================================================================

#[test]
fn test_parse_execute_no_params() {
    let result = Parser::parse_sql("EXECUTE my_select");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Execute(stmt) => {
            assert_eq!(stmt.name, "MY_SELECT");
            assert!(stmt.params.is_empty());
        }
        other => panic!("Expected Execute, got {:?}", other),
    }
}

#[test]
fn test_parse_execute_using_params() {
    let result = Parser::parse_sql("EXECUTE my_insert USING 1, 'Alice'");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Execute(stmt) => {
            assert_eq!(stmt.name, "MY_INSERT");
            assert_eq!(stmt.params.len(), 2);
        }
        other => panic!("Expected Execute, got {:?}", other),
    }
}

#[test]
fn test_parse_execute_parenthesized_params() {
    let result = Parser::parse_sql("EXECUTE my_insert(1, 'Alice')");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Execute(stmt) => {
            assert_eq!(stmt.name, "MY_INSERT");
            assert_eq!(stmt.params.len(), 2);
        }
        other => panic!("Expected Execute, got {:?}", other),
    }
}

#[test]
fn test_parse_execute_empty_parentheses() {
    let result = Parser::parse_sql("EXECUTE my_select()");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Execute(stmt) => {
            assert_eq!(stmt.name, "MY_SELECT");
            assert!(stmt.params.is_empty());
        }
        other => panic!("Expected Execute, got {:?}", other),
    }
}

// ============================================================================
// DEALLOCATE statement tests
// ============================================================================

#[test]
fn test_parse_deallocate_name() {
    let result = Parser::parse_sql("DEALLOCATE my_select");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Deallocate(stmt) => match stmt.target {
            DeallocateTarget::Name(name) => assert_eq!(name, "MY_SELECT"),
            _ => panic!("Expected Name target"),
        },
        other => panic!("Expected Deallocate, got {:?}", other),
    }
}

#[test]
fn test_parse_deallocate_prepare_name() {
    let result = Parser::parse_sql("DEALLOCATE PREPARE my_select");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Deallocate(stmt) => match stmt.target {
            DeallocateTarget::Name(name) => assert_eq!(name, "MY_SELECT"),
            _ => panic!("Expected Name target"),
        },
        other => panic!("Expected Deallocate, got {:?}", other),
    }
}

#[test]
fn test_parse_deallocate_all() {
    let result = Parser::parse_sql("DEALLOCATE ALL");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Deallocate(stmt) => match stmt.target {
            DeallocateTarget::All => (),
            _ => panic!("Expected All target"),
        },
        other => panic!("Expected Deallocate, got {:?}", other),
    }
}

// ============================================================================
// Case insensitivity tests
// ============================================================================

#[test]
fn test_keywords_case_insensitive() {
    // Test PREPARE
    let result1 = Parser::parse_sql("prepare stmt from 'select 1'");
    assert!(result1.is_ok(), "Failed to parse lowercase prepare");

    // Test EXECUTE
    let result2 = Parser::parse_sql("execute stmt");
    assert!(result2.is_ok(), "Failed to parse lowercase execute");

    // Test DEALLOCATE
    let result3 = Parser::parse_sql("deallocate stmt");
    assert!(result3.is_ok(), "Failed to parse lowercase deallocate");

    // Test DEALLOCATE ALL
    let result4 = Parser::parse_sql("deallocate all");
    assert!(result4.is_ok(), "Failed to parse lowercase deallocate all");
}
