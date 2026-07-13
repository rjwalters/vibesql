//! Tests for table-valued functions (`json_each` / `json_tree`) in FROM
//! position (JSON1 Phase 3, ADR-0005 step 2).
//!
//! These cover the standard parser, the arena parser, and std/arena parity.
//! No executor support exists yet — these tests assert only on the parsed AST
//! shape (`FromClause::TableFunction`).

use bumpalo::Bump;
use vibesql_ast::{arena::Converter, Expression, FromClause, Statement};

use crate::arena_parser::ArenaParser;
use crate::Parser;

/// Parse `sql` with the standard parser and return its FROM clause.
fn std_from(sql: &str) -> FromClause {
    match Parser::parse_sql(sql).expect("standard parse should succeed") {
        Statement::Select(select) => select.from.expect("SELECT should have a FROM clause"),
        other => panic!("expected SELECT, got {other:?}"),
    }
}

/// Parse `sql` with the arena parser, convert to the standard AST, and return
/// its FROM clause. This exercises the arena `parse_table_function` path and
/// the arena→standard `TableFunction` conversion.
fn arena_from(sql: &str) -> FromClause {
    let arena = Bump::new();
    let (stmt, interner) =
        ArenaParser::parse_select_with_interner(sql, &arena).expect("arena parse should succeed");
    let converter = Converter::new(&interner);
    let std_stmt = converter.convert_select(stmt);
    std_stmt.from.expect("SELECT should have a FROM clause")
}

// ============================================================================
// Standard parser
// ============================================================================

#[test]
fn std_json_each_single_literal_arg() {
    let from = std_from("SELECT * FROM json_each('[1,2,3]')");
    match from {
        FromClause::TableFunction { name, args, alias, column_aliases } => {
            assert_eq!(name, "json_each");
            assert_eq!(args.len(), 1);
            assert!(matches!(args[0], Expression::Literal(_)));
            assert_eq!(alias, None);
            assert_eq!(column_aliases, None);
        }
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn std_json_tree_single_literal_arg() {
    let from = std_from(r#"SELECT * FROM json_tree('{"a":1}')"#);
    match from {
        FromClause::TableFunction { name, args, .. } => {
            assert_eq!(name, "json_tree");
            assert_eq!(args.len(), 1);
        }
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn std_json_each_two_args_value_and_path() {
    // FROM json_each(x, '$.a') — column-ref value + literal path.
    let from = std_from("SELECT * FROM json_each(x, '$.a')");
    match from {
        FromClause::TableFunction { name, args, .. } => {
            assert_eq!(name, "json_each");
            assert_eq!(args.len(), 2);
            assert!(matches!(args[0], Expression::ColumnRef(_)));
            assert!(matches!(args[1], Expression::Literal(_)));
        }
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn std_alias_and_column_alias_list() {
    // ... AS je(k, v)
    let from = std_from("SELECT * FROM json_each('[1,2,3]') AS je(k, v)");
    match from {
        FromClause::TableFunction { name, alias, column_aliases, .. } => {
            assert_eq!(name, "json_each");
            assert_eq!(alias.as_deref(), Some("je"));
            assert_eq!(column_aliases, Some(vec!["k".to_string(), "v".to_string()]));
        }
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn std_bare_alias_without_as() {
    let from = std_from("SELECT * FROM json_each('[1,2,3]') je");
    match from {
        FromClause::TableFunction { alias, .. } => {
            assert_eq!(alias.as_deref(), Some("je"));
        }
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn std_name_normalized_to_lowercase() {
    let from = std_from("SELECT * FROM JSON_EACH('[1]')");
    match from {
        FromClause::TableFunction { name, .. } => assert_eq!(name, "json_each"),
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn std_non_allow_listed_function_still_errors() {
    // A non-allow-listed `ident(` in FROM must remain a parse error, exactly as
    // before this feature landed.
    assert!(Parser::parse_sql("SELECT * FROM foo('[1,2,3]')").is_err());
    assert!(Parser::parse_sql("SELECT * FROM generate_series(1, 10)").is_err());
    // json_extract is a scalar JSON function, not a TVF — not allow-listed.
    assert!(Parser::parse_sql("SELECT * FROM json_extract('{}', '$.a')").is_err());
}

#[test]
fn std_quoted_name_is_a_table_not_a_tvf() {
    // A delimited/quoted identifier is a table name, never a TVF. `"json_each"`
    // followed by `(` should not be treated as a table-valued function; without
    // an executor it simply parses as a plain table reference (the `(` starts
    // the next token which is not consumed here — so this must NOT be a
    // TableFunction).
    let from = std_from(r#"SELECT * FROM "json_each""#);
    assert!(matches!(from, FromClause::Table { .. }));
}

// ============================================================================
// Parenthesized single-term FROM with a trailing `AS alias` (issue #6051)
// ============================================================================
//
// `parse_table_reference`'s parenthesized-fallback branch previously reattached
// a post-`)` `AS alias` only when the inner content was a `Join`. A single
// parenthesized table or table-valued function silently dropped the alias, so
// `SELECT xyz.* FROM (t1) AS xyz` / `FROM (json_each(...)) AS xyz` later failed
// to resolve `xyz`. These assert the alias survives on both variants.

#[test]
fn std_paren_plain_table_with_alias() {
    // FROM (t1) AS xyz  — the inner term is a plain table; the alias must land on
    // the Table node, not be discarded.
    let from = std_from("SELECT xyz.* FROM (t1) AS xyz");
    match from {
        FromClause::Table { name, alias, .. } => {
            assert_eq!(name, "t1");
            assert_eq!(alias.as_deref(), Some("xyz"));
        }
        other => panic!("expected Table with alias, got {other:?}"),
    }
}

#[test]
fn std_paren_tvf_with_alias() {
    // FROM (json_each('{"a":1}')) AS xyz — the inner term is a TVF; the alias
    // must land on the TableFunction node.
    let from = std_from(r#"SELECT xyz.* FROM (json_each('{"a":1}')) AS xyz"#);
    match from {
        FromClause::TableFunction { name, alias, .. } => {
            assert_eq!(name, "json_each");
            assert_eq!(alias.as_deref(), Some("xyz"));
        }
        other => panic!("expected TableFunction with alias, got {other:?}"),
    }
}

#[test]
fn std_paren_join_with_alias_unregressed() {
    // The pre-existing Join case must still attach its alias.
    let from = std_from("SELECT * FROM (t1 JOIN t2 USING(id)) AS j1");
    match from {
        FromClause::Join { alias, .. } => {
            assert_eq!(alias.as_deref(), Some("j1"));
        }
        other => panic!("expected Join with alias, got {other:?}"),
    }
}

#[test]
fn std_paren_plain_table_without_alias_unregressed() {
    // FROM (t1) with no alias still parses to a bare Table.
    let from = std_from("SELECT * FROM (t1)");
    match from {
        FromClause::Table { name, alias, .. } => {
            assert_eq!(name, "t1");
            assert_eq!(alias, None);
        }
        other => panic!("expected Table, got {other:?}"),
    }
}

// ============================================================================
// Arena parser
// ============================================================================

#[test]
fn arena_json_each_single_literal_arg() {
    let from = arena_from("SELECT * FROM json_each('[1,2,3]')");
    match from {
        FromClause::TableFunction { name, args, alias, column_aliases } => {
            assert_eq!(name, "json_each");
            assert_eq!(args.len(), 1);
            assert_eq!(alias, None);
            assert_eq!(column_aliases, None);
        }
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn arena_non_allow_listed_function_still_errors() {
    let arena = Bump::new();
    assert!(ArenaParser::parse_select_with_interner("SELECT * FROM foo('[1]')", &arena).is_err());
}

// ============================================================================
// std / arena parity
// ============================================================================

/// Every acceptance form must parse identically under both parsers (compared
/// after arena→standard conversion).
#[test]
fn std_arena_parity_across_acceptance_forms() {
    let cases = [
        "SELECT * FROM json_each('[1,2,3]')",
        r#"SELECT * FROM json_tree('{"a":1}')"#,
        "SELECT * FROM json_each(x, '$.a')",
        "SELECT * FROM json_each('[1,2,3]') AS je(k, v)",
        "SELECT * FROM json_tree(x, '$.a') AS jt(k, v)",
        "SELECT * FROM json_each('[1]') je",
    ];
    for sql in cases {
        assert_eq!(std_from(sql), arena_from(sql), "parser mismatch for: {sql}");
    }
}

#[test]
fn std_arena_parity_negative_case() {
    // Both parsers reject a non-allow-listed `ident(` in FROM.
    let sql = "SELECT * FROM foo('[1,2,3]')";
    let arena = Bump::new();
    assert!(Parser::parse_sql(sql).is_err());
    assert!(ArenaParser::parse_select_with_interner(sql, &arena).is_err());
}
