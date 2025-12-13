//! Tests for transaction control statement parsing (SQL:1999 features)
//!
//! Tests for BEGIN, COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO SAVEPOINT,
//! and RELEASE SAVEPOINT statements.

use vibesql_ast::Statement;
use vibesql_parser::Parser;

#[test]
fn test_begin_transaction() {
    let sql = "BEGIN";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse BEGIN");

    match stmt {
        Statement::BeginTransaction(_) => {
            // Successfully parsed
        }
        _ => panic!("Expected BeginTransaction statement, got {:?}", stmt),
    }
}

#[test]
fn test_begin_transaction_with_keyword() {
    let sql = "BEGIN TRANSACTION";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse BEGIN TRANSACTION");

    match stmt {
        Statement::BeginTransaction(_) => {
            // Successfully parsed
        }
        _ => panic!("Expected BeginTransaction statement, got {:?}", stmt),
    }
}

#[test]
fn test_start_transaction() {
    let sql = "START TRANSACTION";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse START TRANSACTION");

    match stmt {
        Statement::BeginTransaction(_) => {
            // Successfully parsed
        }
        _ => panic!("Expected BeginTransaction statement, got {:?}", stmt),
    }
}

#[test]
fn test_commit() {
    let sql = "COMMIT";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse COMMIT");

    match stmt {
        Statement::Commit(_) => {
            // Successfully parsed
        }
        _ => panic!("Expected Commit statement, got {:?}", stmt),
    }
}

#[test]
fn test_rollback() {
    let sql = "ROLLBACK";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse ROLLBACK");

    match stmt {
        Statement::Rollback(_) => {
            // Successfully parsed
        }
        _ => panic!("Expected Rollback statement, got {:?}", stmt),
    }
}

#[test]
fn test_savepoint() {
    let sql = "SAVEPOINT my_savepoint";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SAVEPOINT");

    match stmt {
        Statement::Savepoint(savepoint_stmt) => {
            assert_eq!(savepoint_stmt.name, "my_savepoint");
        }
        _ => panic!("Expected Savepoint statement, got {:?}", stmt),
    }
}

#[test]
fn test_savepoint_quoted() {
    let sql = "SAVEPOINT \"my_savepoint\"";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SAVEPOINT with quoted name");

    match stmt {
        Statement::Savepoint(savepoint_stmt) => {
            assert_eq!(savepoint_stmt.name, "my_savepoint");
        }
        _ => panic!("Expected Savepoint statement, got {:?}", stmt),
    }
}

#[test]
fn test_rollback_to_savepoint() {
    let sql = "ROLLBACK TO SAVEPOINT my_savepoint";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse ROLLBACK TO SAVEPOINT");

    match stmt {
        Statement::RollbackToSavepoint(rollback_stmt) => {
            assert_eq!(rollback_stmt.name, "my_savepoint");
        }
        _ => panic!("Expected RollbackToSavepoint statement, got {:?}", stmt),
    }
}

#[test]
fn test_release_savepoint() {
    let sql = "RELEASE SAVEPOINT my_savepoint";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse RELEASE SAVEPOINT");

    match stmt {
        Statement::ReleaseSavepoint(release_stmt) => {
            assert_eq!(release_stmt.name, "my_savepoint");
        }
        _ => panic!("Expected ReleaseSavepoint statement, got {:?}", stmt),
    }
}

#[test]
fn test_nested_transaction_example() {
    // Test a sequence of transaction statements that might be used together

    let statements = vec![
        "BEGIN",
        "SAVEPOINT sp1",
        "SAVEPOINT sp2",
        "ROLLBACK TO SAVEPOINT sp1",
        "RELEASE SAVEPOINT sp2",
        "COMMIT",
    ];

    for sql in statements {
        let stmt = Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse: {}", sql));
        // Just verify it parses without error - the specific type checking is done above
        assert!(matches!(
            stmt,
            Statement::BeginTransaction(_)
                | Statement::Commit(_)
                | Statement::Rollback(_)
                | Statement::Savepoint(_)
                | Statement::RollbackToSavepoint(_)
                | Statement::ReleaseSavepoint(_)
        ));
    }
}

#[test]
fn test_transaction_statement_case_insensitive() {
    // Test case insensitivity for keywords
    let sql_variants = vec![
        "begin",
        "BEGIN",
        "Begin",
        "commit",
        "COMMIT",
        "Commit",
        "rollback",
        "ROLLBACK",
        "Rollback",
        "savepoint my_sp",
        "SAVEPOINT my_sp",
        "Savepoint my_sp",
    ];

    for sql in sql_variants {
        Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse case variant: {}", sql));
    }
}

#[test]
fn test_savepoint_names_various() {
    let test_cases = vec![
        ("SAVEPOINT abc", "abc"), // unquoted identifiers are lowercased
        ("SAVEPOINT ABC", "abc"),
        ("SAVEPOINT save_point_123", "save_point_123"),
        ("SAVEPOINT \"quoted name\"", "quoted name"), // quoted identifiers preserve case
    ];

    for (sql, expected_name) in test_cases {
        let stmt = Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse: {}", sql));
        match stmt {
            Statement::Savepoint(savepoint_stmt) => {
                assert_eq!(savepoint_stmt.name, expected_name, "Failed for SQL: {}", sql);
            }
            _ => panic!("Expected Savepoint statement for: {}", sql),
        }
    }
}

#[test]
fn test_transaction_parse_errors() {
    // Test various parse error cases

    let invalid_statements = vec![
        "SAVEPOINT",             // Missing savepoint name
        "ROLLBACK TO",           // Missing SAVEPOINT keyword
        "ROLLBACK TO SAVEPOINT", // Missing savepoint name
        "RELEASE",               // Missing SAVEPOINT keyword
        "RELEASE SAVEPOINT",     // Missing savepoint name
    ];

    for sql in invalid_statements {
        let result = Parser::parse_sql(sql);
        assert!(result.is_err(), "Expected parse error for: {}", sql);
    }
}

#[test]
fn test_transaction_isolation_levels() {
    // Note: The current parser doesn't handle isolation levels yet,
    // but we can test that basic transaction parsing still works
    // when they might be added in the future

    let sql = "BEGIN";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse basic BEGIN");

    match stmt {
        Statement::BeginTransaction(_) => {
            // Successfully parsed
        }
        _ => panic!("Expected BeginTransaction statement"),
    }
}
