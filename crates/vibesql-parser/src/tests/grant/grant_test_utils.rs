//! Helper utilities for GRANT statement tests

use vibesql_ast::*;

use crate::Parser;

/// Parses SQL and extracts the Grant statement, panicking with descriptive error if parsing fails
/// or if the result is not a Grant statement.
pub fn parse_grant(sql: &str) -> GrantStmt {
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => grant_stmt,
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

/// Asserts that the Grant statement has the expected structure.
///
/// # Arguments
/// * `grant` - The Grant statement to check
/// * `expected_privileges` - Expected privilege types
/// * `expected_object_type` - Expected object type (Table, Schema, Function, etc.)
/// * `expected_object_name` - Expected object name (will be compared as uppercase string)
/// * `expected_grantees` - Expected grantee names
/// * `expected_with_grant_option` - Whether WITH GRANT OPTION should be present
pub fn assert_grant_structure(
    grant: &GrantStmt,
    expected_privileges: &[PrivilegeType],
    expected_object_type: ObjectType,
    expected_object_name: &str,
    expected_grantees: &[&str],
    expected_with_grant_option: bool,
) {
    assert_eq!(grant.privileges.len(), expected_privileges.len());
    for (i, expected_priv) in expected_privileges.iter().enumerate() {
        assert_eq!(grant.privileges[i], *expected_priv);
    }
    assert_eq!(grant.object_type, expected_object_type);
    assert_eq!(grant.object_name.to_string(), expected_object_name);
    assert_eq!(grant.grantees.len(), expected_grantees.len());
    assert_eq!(grant.grantees, expected_grantees);
    assert_eq!(grant.with_grant_option, expected_with_grant_option);
}

/// Asserts that the privilege list matches the expected privileges.
pub fn assert_privileges(grant: &GrantStmt, expected: &[PrivilegeType]) {
    assert_eq!(grant.privileges.len(), expected.len());
    for (i, expected_priv) in expected.iter().enumerate() {
        assert_eq!(grant.privileges[i], *expected_priv);
    }
}

/// Asserts that the object type and name match expectations.
pub fn assert_object(grant: &GrantStmt, expected_type: ObjectType, expected_name: &str) {
    assert_eq!(grant.object_type, expected_type);
    assert_eq!(grant.object_name.to_string(), expected_name);
}

/// Asserts that the grantees match the expected list.
pub fn assert_grantees(grant: &GrantStmt, expected: &[&str]) {
    assert_eq!(grant.grantees.len(), expected.len());
    assert_eq!(grant.grantees, expected);
}

/// Asserts that the WITH GRANT OPTION flag matches the expected value.
#[allow(dead_code)]
pub fn assert_with_grant_option(grant: &GrantStmt, expected: bool) {
    assert_eq!(grant.with_grant_option, expected);
}
