//! Tests for `@name` as a bind-parameter expression atom (issue #6172).
//!
//! SQLite documents `@AAAA` as working "exactly like a colon [`:AAAA`]"
//! parameter (R-49783-61279): `lang_expr.html`, exercised by the SQLite TCL
//! conformance suite's `e_expr.test` (e.g. `e_expr-11.3.*`, `e_expr-12.3.11.1`,
//! `e_expr-11.7.*`). The lexer already tokenized `@name` as
//! `Token::UserVariable` (shared with the MySQL-style `SELECT ... INTO @var`
//! procedural clause), but nothing consumed that token in ordinary expression
//! position, so `SELECT @hello` failed to parse at all
//! (`near "@hello": syntax error`) instead of being treated as a bind
//! parameter equivalent to `:hello`.
//!
//! `CREATE VIEW`/`CREATE TRIGGER` still reject `@name` in their bodies via a
//! raw-token scan (`token_is_variable`) that runs independently of expression
//! parsing -- see `tests/view.rs`'s `test_create_view_rejects_named_at_placeholder`.

use bumpalo::Bump;
use vibesql_ast::{Expression, SelectItem};

use crate::{arena_parser::ArenaParser, Parser};

#[test]
fn test_parse_at_placeholder_in_select_list() {
    let result = Parser::parse_sql("SELECT @hello");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Select(stmt) => match &stmt.select_list[0] {
            SelectItem::Expression { expr: Expression::NamedPlaceholder(name), .. } => {
                assert_eq!(name, "hello")
            }
            other => panic!("Expected NamedPlaceholder(\"hello\"), got {:?}", other),
        },
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[test]
fn test_parse_at_placeholder_in_where_clause() {
    // SQLite: an "at" sign works exactly like a colon (R-49783-61279).
    let colon = Parser::parse_sql("SELECT * FROM t WHERE a = :x");
    let at = Parser::parse_sql("SELECT * FROM t WHERE a = @x");
    assert!(colon.is_ok(), "Failed to parse :x form: {:?}", colon.err());
    assert!(at.is_ok(), "Failed to parse @x form: {:?}", at.err());
}

#[test]
fn test_parse_at_placeholder_distinct_names() {
    // Multiple distinct @-named parameters in one statement.
    let result = Parser::parse_sql("SELECT @a1, @123, @__");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Select(stmt) => {
            let names: Vec<&str> = stmt
                .select_list
                .iter()
                .map(|item| match item {
                    SelectItem::Expression { expr: Expression::NamedPlaceholder(name), .. } => {
                        name.as_str()
                    }
                    other => panic!("Expected NamedPlaceholder, got {:?}", other),
                })
                .collect();
            assert_eq!(names, vec!["a1", "123", "__"]);
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[test]
fn test_arena_parse_select_with_at_placeholder() {
    let arena = Bump::new();
    let sql = "SELECT @hello";
    let result = ArenaParser::parse_select_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    match &stmt.select_list[0] {
        vibesql_ast::arena::SelectItem::Expression {
            expr: vibesql_ast::arena::Expression::NamedPlaceholder(name),
            ..
        } => {
            assert_eq!(interner.resolve(*name), "hello");
        }
        other => panic!("Expected NamedPlaceholder(\"hello\"), got {:?}", other),
    }
}
