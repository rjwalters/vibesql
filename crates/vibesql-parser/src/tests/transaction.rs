//! Tests for transaction control statement parsing (BEGIN, COMMIT, ROLLBACK)

use crate::Parser;

#[test]
fn test_parse_begin() {
    let result = Parser::parse_sql("begin");
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
    let result = Parser::parse_sql("commit");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::Commit(_) => (),
        other => panic!("Expected Commit, got {:?}", other),
    }
}

#[test]
fn test_parse_rollback() {
    let result = Parser::parse_sql("rollback");
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

    let result2 = Parser::parse_sql("commit");
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
    let result = Parser::parse_sql("begin");
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

#[test]
fn test_parse_rollback_to_savepoint_keyword() {
    // Full form with SAVEPOINT keyword.
    let result = Parser::parse_sql("ROLLBACK TO SAVEPOINT sp1");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::RollbackToSavepoint(stmt) => assert_eq!(stmt.name, "sp1"),
        other => panic!("Expected RollbackToSavepoint, got {:?}", other),
    }
}

#[test]
fn test_parse_rollback_to_without_savepoint_keyword() {
    // SQLite shorthand: ROLLBACK TO <name> (SAVEPOINT keyword omitted).
    let result = Parser::parse_sql("ROLLBACK TO sp1");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::RollbackToSavepoint(stmt) => assert_eq!(stmt.name, "sp1"),
        other => panic!("Expected RollbackToSavepoint, got {:?}", other),
    }
}

#[test]
fn test_parse_release_with_savepoint_keyword() {
    let result = Parser::parse_sql("RELEASE SAVEPOINT sp1");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::ReleaseSavepoint(stmt) => assert_eq!(stmt.name, "sp1"),
        other => panic!("Expected ReleaseSavepoint, got {:?}", other),
    }
}

#[test]
fn test_parse_release_without_savepoint_keyword() {
    // SQLite shorthand: RELEASE <name> (SAVEPOINT keyword omitted).
    let result = Parser::parse_sql("RELEASE sp1");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::ReleaseSavepoint(stmt) => assert_eq!(stmt.name, "sp1"),
        other => panic!("Expected ReleaseSavepoint, got {:?}", other),
    }
}

#[test]
fn test_parse_savepoint_join_keyword_name() {
    // SQLite's `nm` grammar rule accepts JOIN_KW (OUTER, INNER, ...) as a
    // savepoint name (fkey2-2.38: `SAVEPOINT outer`). The keyword is
    // lowercased like any unquoted identifier.
    for (sql, expected) in
        [("SAVEPOINT outer", "outer"), ("SAVEPOINT inner", "inner"), ("SAVEPOINT LEFT", "left")]
    {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "{sql} err: {:?}", result);
        match result.unwrap() {
            vibesql_ast::Statement::Savepoint(stmt) => assert_eq!(stmt.name, expected),
            other => panic!("Expected Savepoint for {sql}, got {:?}", other),
        }
    }
}

#[test]
fn test_parse_release_and_rollback_to_join_keyword_name() {
    // `RELEASE outer` and `ROLLBACK TO outer` must resolve to the same
    // lowercased name as `SAVEPOINT outer` (fkey2-2.40 / fkey2-2.48).
    match Parser::parse_sql("RELEASE outer").unwrap() {
        vibesql_ast::Statement::ReleaseSavepoint(stmt) => assert_eq!(stmt.name, "outer"),
        other => panic!("Expected ReleaseSavepoint, got {:?}", other),
    }
    match Parser::parse_sql("ROLLBACK TO outer").unwrap() {
        vibesql_ast::Statement::RollbackToSavepoint(stmt) => assert_eq!(stmt.name, "outer"),
        other => panic!("Expected RollbackToSavepoint, got {:?}", other),
    }
}

#[test]
fn test_parse_savepoint_reserved_keyword_still_rejected() {
    // Truly-reserved words that would create grammar ambiguity (e.g. SELECT)
    // are still not valid unquoted savepoint names: `parse_savepoint_name`
    // reuses the same contextual-keyword set as column names, which excludes
    // them. They remain usable only via delimited identifiers.
    assert!(Parser::parse_sql("SAVEPOINT select").is_err());
    assert!(Parser::parse_sql("SAVEPOINT where").is_err());
}
