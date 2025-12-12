//! Tests for GRANT statement table-level privilege parsing
//!
//! This module contains tests for basic table privileges (SELECT, INSERT, UPDATE, DELETE)
//! and the ALL PRIVILEGES syntax. These tests verify the parser correctly handles:
//! - Individual table privileges (SELECT, INSERT, UPDATE, DELETE)
//! - Multiple privileges in a single GRANT statement
//! - ALL PRIVILEGES keyword variations
//! - REFERENCES privilege at table level
//! - Case insensitive parsing
//! - WITH GRANT OPTION combinations

use vibesql_ast::*;

use super::grant_test_utils::*;

#[test]
fn test_parse_grant_select_on_table() {
    let grant = parse_grant("GRANT SELECT ON TABLE users TO manager");
    assert_grant_structure(
        &grant,
        &[PrivilegeType::Select(None)],
        ObjectType::Table,
        "users",
        &["manager"],
        false,
    );
}

#[test]
fn test_parse_grant_multiple_privileges() {
    let grant = parse_grant("GRANT SELECT, INSERT, UPDATE ON TABLE users TO manager");
    assert_privileges(
        &grant,
        &[PrivilegeType::Select(None), PrivilegeType::Insert(None), PrivilegeType::Update(None)],
    );
    assert_object(&grant, ObjectType::Table, "users");
    assert_grantees(&grant, &["manager"]);
}

#[test]
fn test_parse_grant_all_privilege_types() {
    let grant = parse_grant("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE orders TO clerk");
    assert_privileges(
        &grant,
        &[
            PrivilegeType::Select(None),
            PrivilegeType::Insert(None),
            PrivilegeType::Update(None),
            PrivilegeType::Delete,
        ],
    );
}

#[test]
fn test_parse_grant_all_privileges_with_keyword() {
    let grant = parse_grant("GRANT ALL PRIVILEGES ON TABLE users TO manager");
    assert_grant_structure(
        &grant,
        &[PrivilegeType::AllPrivileges],
        ObjectType::Table,
        "users",
        &["manager"],
        false,
    );
}

#[test]
fn test_parse_grant_references_on_table() {
    let grant = parse_grant("GRANT REFERENCES ON TABLE users TO manager");
    assert_grant_structure(
        &grant,
        &[PrivilegeType::References(None)],
        ObjectType::Table,
        "users",
        &["manager"],
        false,
    );
}

#[test]
fn test_parse_grant_references_combined_with_select() {
    let grant = parse_grant("GRANT REFERENCES, SELECT ON TABLE products TO buyer");
    assert_privileges(&grant, &[PrivilegeType::References(None), PrivilegeType::Select(None)]);
    assert_object(&grant, ObjectType::Table, "products");
    assert_grantees(&grant, &["buyer"]);
}

#[test]
fn test_parse_grant_all_without_privileges_keyword() {
    let grant = parse_grant("GRANT ALL ON TABLE users TO manager");
    assert_grant_structure(
        &grant,
        &[PrivilegeType::AllPrivileges],
        ObjectType::Table,
        "users",
        &["manager"],
        false,
    );
}

#[test]
fn test_parse_grant_all_case_insensitive() {
    let grant = parse_grant("grant all on table employees to clerk");
    assert_privileges(&grant, &[PrivilegeType::AllPrivileges]);
    assert_object(&grant, ObjectType::Table, "employees");
    assert_grantees(&grant, &["clerk"]);
}

#[test]
fn test_parse_grant_references_case_insensitive() {
    let grant = parse_grant("grant references on table employees to clerk");
    assert_privileges(&grant, &[PrivilegeType::References(None)]);
    assert_object(&grant, ObjectType::Table, "employees");
    assert_grantees(&grant, &["clerk"]);
}

#[test]
fn test_parse_grant_references_with_grant_option_table_level() {
    let grant = parse_grant("GRANT REFERENCES ON TABLE orders TO manager WITH GRANT OPTION");
    assert_grant_structure(
        &grant,
        &[PrivilegeType::References(None)],
        ObjectType::Table,
        "orders",
        &["manager"],
        true,
    );
}
