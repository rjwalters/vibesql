//! Tests for SQLite index hints in FROM clauses: INDEXED BY / NOT INDEXED
//!
//! VibeSQL parses these hints and carries them through the AST so the
//! executor can validate them (issue #5235: `no such index: X`). The planner
//! chooses indexes independently. Regression coverage for window1.test 77.2.

use super::super::*;

fn assert_table_from(sql: &str, expected_name: &str, expected_alias: Option<&str>) {
    assert_table_from_with_hint(sql, expected_name, expected_alias, None);
}

fn assert_table_from_with_hint(
    sql: &str,
    expected_name: &str,
    expected_alias: Option<&str>,
    expected_hint: Option<vibesql_ast::IndexHint>,
) {
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "should parse: {sql}: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::Select(select) => match select.from {
            Some(vibesql_ast::FromClause::Table { name, alias, index_hint, .. }) => {
                assert_eq!(name, expected_name);
                assert_eq!(alias.as_deref(), expected_alias);
                assert_eq!(index_hint, expected_hint);
            }
            other => panic!("Expected Table FROM clause, got {:?}", other),
        },
        other => panic!("Expected SELECT statement, got {:?}", other),
    }
}

#[test]
fn test_indexed_by_hint_is_captured() {
    assert_table_from_with_hint(
        "SELECT * FROM t1 INDEXED BY t1x;",
        "t1",
        None,
        Some(vibesql_ast::IndexHint::IndexedBy("t1x".to_string())),
    );
}

#[test]
fn test_indexed_by_hint_with_where() {
    // window1.test 77.2 shape: INDEXED BY followed by GROUP BY
    let result =
        Parser::parse_sql("SELECT max(x) FILTER (WHERE true) FROM t1 INDEXED BY t1x GROUP BY x;");
    assert!(result.is_ok(), "INDEXED BY with GROUP BY should parse: {:?}", result);
}

#[test]
fn test_indexed_by_hint_after_alias() {
    assert_table_from_with_hint(
        "SELECT * FROM t1 AS a INDEXED BY t1x;",
        "t1",
        Some("a"),
        Some(vibesql_ast::IndexHint::IndexedBy("t1x".to_string())),
    );
}

#[test]
fn test_not_indexed_hint_is_captured() {
    assert_table_from_with_hint(
        "SELECT * FROM t1 NOT INDEXED;",
        "t1",
        None,
        Some(vibesql_ast::IndexHint::NotIndexed),
    );
}

#[test]
fn test_not_indexed_hint_with_where() {
    let result = Parser::parse_sql("SELECT * FROM t1 NOT INDEXED WHERE x > 1;");
    assert!(result.is_ok(), "NOT INDEXED with WHERE should parse: {:?}", result);
}

#[test]
fn test_indexed_as_plain_alias_still_works() {
    // "indexed" not followed by BY is a regular implicit alias
    assert_table_from("SELECT * FROM t1 indexed;", "t1", Some("indexed"));
}

#[test]
fn test_with_scalar_subquery_in_expression() {
    // window1.test 15.2: WITH-prefixed scalar subquery in an expression
    let result =
        Parser::parse_sql("SELECT( WITH c AS( VALUES(1) ) SELECT '' FROM c,c ) x WHERE x+x;");
    assert!(result.is_ok(), "WITH scalar subquery should parse: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                assert_eq!(alias.as_deref(), Some("x"));
                assert!(
                    matches!(expr, vibesql_ast::Expression::ScalarSubquery(_)),
                    "Expected scalar subquery, got {:?}",
                    expr
                );
            }
            other => panic!("Expected expression select item, got {:?}", other),
        },
        other => panic!("Expected SELECT statement, got {:?}", other),
    }
}

#[test]
fn test_with_subquery_in_in_predicate() {
    let result = Parser::parse_sql("SELECT 1 WHERE 1 IN (WITH c AS (VALUES(1)) SELECT * FROM c);");
    assert!(result.is_ok(), "IN (WITH ... SELECT ...) should parse: {:?}", result);
}

// ============================================================================
// Arena parser mirrors
// ============================================================================

#[test]
fn test_arena_indexed_by_hint_is_captured() {
    let result = crate::arena_parser::parse_select_to_owned("SELECT * FROM t1 INDEXED BY t1x");
    assert!(result.is_ok(), "arena parser should accept INDEXED BY: {:?}", result);
    match result.unwrap().from {
        Some(vibesql_ast::FromClause::Table { index_hint, .. }) => {
            assert_eq!(
                index_hint,
                Some(vibesql_ast::IndexHint::IndexedBy("t1x".to_string())),
                "arena parser should carry the INDEXED BY hint through conversion"
            );
        }
        other => panic!("Expected Table FROM clause, got {:?}", other),
    }
}

#[test]
fn test_arena_not_indexed_hint_is_captured() {
    let result = crate::arena_parser::parse_select_to_owned("SELECT * FROM t1 NOT INDEXED");
    assert!(result.is_ok(), "arena parser should accept NOT INDEXED: {:?}", result);
    match result.unwrap().from {
        Some(vibesql_ast::FromClause::Table { index_hint, .. }) => {
            assert_eq!(index_hint, Some(vibesql_ast::IndexHint::NotIndexed));
        }
        other => panic!("Expected Table FROM clause, got {:?}", other),
    }
}

#[test]
fn test_arena_cast_empty_typename() {
    let result = crate::arena_parser::parse_select_to_owned("SELECT CAST(a AS ) FROM t1");
    assert!(result.is_ok(), "arena parser should accept CAST(a AS ): {:?}", result);
}

#[test]
fn test_arena_with_scalar_subquery_in_expression() {
    // Note: the arena parser does not yet support VALUES as a CTE body
    // (it falls back to the standard parser in production), so this uses a
    // SELECT-bodied CTE to exercise the WITH-scalar-subquery path.
    let result = crate::arena_parser::parse_select_to_owned(
        "SELECT( WITH c AS( SELECT 1 ) SELECT '' FROM c ) x",
    );
    assert!(result.is_ok(), "arena parser should accept WITH scalar subquery: {:?}", result);
}
