//! Tests for transaction control statement parsing (BEGIN, COMMIT, ROLLBACK)

use crate::Parser;

#[test]
fn test_parse_begin() {
    let result = Parser::parse_sql("BEGIN");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(_) => (),
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_transaction() {
    let result = Parser::parse_sql("BEGIN TRANSACTION");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(_) => (),
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_start_transaction() {
    let result = Parser::parse_sql("START TRANSACTION");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(_) => (),
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_commit() {
    let result = Parser::parse_sql("COMMIT");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::Commit(_) => (),
        other => panic!("Expected Commit, got {:?}", other),
    }
}

#[test]
fn test_parse_rollback() {
    let result = Parser::parse_sql("ROLLBACK");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::Rollback(_) => (),
        other => panic!("Expected Rollback, got {:?}", other),
    }
}

#[test]
fn test_transaction_keywords_case_insensitive() {
    // Test case insensitivity
    let result1 = Parser::parse_sql("begin");
    assert!(result1.is_ok());

    let result2 = Parser::parse_sql("COMMIT");
    assert!(result2.is_ok());

    let result3 = Parser::parse_sql("rollback");
    assert!(result3.is_ok());
}

// ============================================================================
// Durability Hint Tests
// ============================================================================

#[test]
fn test_parse_begin_with_durability_default() {
    let result = Parser::parse_sql("BEGIN WITH DURABILITY = DEFAULT");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Default);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_with_durability_durable() {
    let result = Parser::parse_sql("BEGIN WITH DURABILITY = DURABLE");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Durable);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_with_durability_lazy() {
    let result = Parser::parse_sql("BEGIN WITH DURABILITY = LAZY");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Lazy);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_with_durability_volatile() {
    let result = Parser::parse_sql("BEGIN WITH DURABILITY = VOLATILE");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Volatile);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_transaction_with_durability() {
    let result = Parser::parse_sql("BEGIN TRANSACTION WITH DURABILITY = DURABLE");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Durable);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_start_transaction_with_durability() {
    let result = Parser::parse_sql("START TRANSACTION WITH DURABILITY = LAZY");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Lazy);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_durability_without_equals_sign() {
    // Test that durability hint works without the optional = sign
    let result = Parser::parse_sql("BEGIN WITH DURABILITY VOLATILE");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Volatile);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_durability_case_insensitive() {
    // Test lowercase durability mode
    let result = Parser::parse_sql("begin with durability = durable");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Durable);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_without_durability_defaults() {
    // Test that BEGIN without durability hint defaults to Default
    let result = Parser::parse_sql("BEGIN");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(stmt) => {
            assert_eq!(stmt.durability, vibesql_ast::DurabilityHint::Default);
        }
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_invalid_durability_mode() {
    // Test that invalid durability mode returns error
    let result = Parser::parse_sql("BEGIN WITH DURABILITY = INVALID");
    assert!(result.is_err());
}
