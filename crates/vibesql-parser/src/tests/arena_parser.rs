//! Tests for arena-allocated parser.
//!
//! Note: The lexer normalizes identifiers to uppercase, so tests use uppercase
//! strings for identifier comparisons.

use bumpalo::Bump;
use vibesql_ast::{
    arena::Converter, DeleteStmt, InsertSource, InsertStmt, UpdateStmt, WhereClause,
};

use crate::arena_parser::ArenaParser;

// ============================================================================
// DELETE Tests
// ============================================================================

#[test]
fn test_arena_parse_delete_simple() {
    let arena = Bump::new();
    let sql = "DELETE FROM users";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "users");
    assert!(!stmt.only);
    assert!(stmt.where_clause.is_none());
}

#[test]
fn test_arena_parse_delete_with_where() {
    let arena = Bump::new();
    let sql = "DELETE FROM users WHERE id = 1";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "users");
    assert!(stmt.where_clause.is_some());
}

#[test]
fn test_arena_parse_delete_with_only() {
    let arena = Bump::new();
    let sql = "DELETE FROM ONLY users WHERE id = 1";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert!(stmt.only);
    assert_eq!(interner.resolve(stmt.table_name), "users");
}

#[test]
fn test_arena_parse_delete_with_alias() {
    // SQLite 3.24+ DELETE target alias (issue #5752). Arena parser parity with
    // the string parser for both `AS alias` and bare-alias forms.
    let arena = Bump::new();
    let (stmt, interner) =
        ArenaParser::parse_delete_with_interner("DELETE FROM t1 AS a WHERE a.x = 1", &arena)
            .expect("DELETE ... AS alias should parse");
    assert_eq!(interner.resolve(stmt.table_name), "t1");
    assert_eq!(stmt.alias.map(|s| interner.resolve(s)), Some("a"));

    let arena2 = Bump::new();
    let (bare, interner2) =
        ArenaParser::parse_delete_with_interner("DELETE FROM t1 a WHERE a.x = 1", &arena2)
            .expect("DELETE bare alias should parse");
    assert_eq!(bare.alias.map(|s| interner2.resolve(s)), Some("a"));

    // No alias: a following WHERE keyword must not be consumed as an alias.
    let arena3 = Bump::new();
    let (no_alias, _) =
        ArenaParser::parse_delete_with_interner("DELETE FROM t1 WHERE x = 1", &arena3)
            .expect("DELETE without alias should parse");
    assert_eq!(no_alias.alias, None);
}

#[test]
fn test_arena_parse_delete_convert_to_standard() {
    let arena = Bump::new();
    let sql = "DELETE FROM users WHERE id = 1";
    let (arena_stmt, interner) = ArenaParser::parse_delete_with_interner(sql, &arena).unwrap();

    // Convert to standard AST using the Converter
    let converter = Converter::new(&interner);
    let std_stmt: DeleteStmt = converter.convert_delete(arena_stmt);
    assert_eq!(std_stmt.table_name, "users");
    assert!(matches!(std_stmt.where_clause, Some(WhereClause::Condition(_))));
}

// ============================================================================
// UPDATE Tests
// ============================================================================

#[test]
fn test_arena_parse_update_simple() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John'";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "users");
    assert_eq!(stmt.assignments.len(), 1);
    assert_eq!(interner.resolve(stmt.assignments[0].column), "name");
    assert!(stmt.where_clause.is_none());
}

#[test]
fn test_arena_parse_update_multiple_assignments() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John', age = 30";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(stmt.assignments.len(), 2);
    assert_eq!(interner.resolve(stmt.assignments[0].column), "name");
    assert_eq!(interner.resolve(stmt.assignments[1].column), "age");
}

#[test]
fn test_arena_parse_update_with_where() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John' WHERE id = 1";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(stmt.where_clause.is_some());
}

#[test]
fn test_arena_parse_update_convert_to_standard() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John', age = 30 WHERE id = 1";
    let (arena_stmt, interner) = ArenaParser::parse_update_with_interner(sql, &arena).unwrap();

    // Convert to standard AST using the Converter
    let converter = Converter::new(&interner);
    let std_stmt: UpdateStmt = converter.convert_update(arena_stmt);
    assert_eq!(std_stmt.table_name, "users");
    assert_eq!(std_stmt.assignments.len(), 2);
    assert!(matches!(std_stmt.where_clause, Some(WhereClause::Condition(_))));
}

// ============================================================================
// INSERT Tests
// ============================================================================

#[test]
fn test_arena_parse_insert_simple() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES ('John', 30)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "users");
    assert_eq!(stmt.columns.len(), 2);
    assert_eq!(interner.resolve(stmt.columns[0]), "name");
    assert_eq!(interner.resolve(stmt.columns[1]), "age");

    match &stmt.source {
        vibesql_ast::arena::InsertSource::Values(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
        }
        _ => panic!("Expected Values source"),
    }
}

#[test]
fn test_arena_parse_insert_multiple_rows() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES ('John', 30), ('Jane', 25)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    match &stmt.source {
        vibesql_ast::arena::InsertSource::Values(rows) => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("Expected Values source"),
    }
}

#[test]
fn test_arena_parse_insert_no_columns() {
    let arena = Bump::new();
    let sql = "INSERT INTO users VALUES ('John', 30)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert_eq!(stmt.columns.len(), 0);
}

#[test]
fn test_arena_parse_insert_or_replace() {
    let arena = Bump::new();
    let sql = "INSERT OR REPLACE INTO users (name) VALUES ('John')";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(matches!(stmt.conflict_clause, Some(vibesql_ast::arena::ConflictClause::Replace)));
}

#[test]
fn test_arena_parse_insert_or_ignore() {
    let arena = Bump::new();
    let sql = "INSERT OR IGNORE INTO users (name) VALUES ('John')";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(matches!(stmt.conflict_clause, Some(vibesql_ast::arena::ConflictClause::Ignore)));
}

#[test]
fn test_arena_parse_replace() {
    let arena = Bump::new();
    let sql = "REPLACE INTO users (name) VALUES ('John')";
    let result = ArenaParser::parse_replace_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(matches!(stmt.conflict_clause, Some(vibesql_ast::arena::ConflictClause::Replace)));
}

#[test]
fn test_arena_parse_insert_with_select() {
    let arena = Bump::new();
    let sql = "INSERT INTO users_backup (name, age) SELECT name, age FROM users";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    match &stmt.source {
        vibesql_ast::arena::InsertSource::Select(query) => {
            assert!(query.from.is_some());
        }
        _ => panic!("Expected Select source"),
    }
}

#[test]
fn test_arena_parse_insert_convert_to_standard() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES ('John', 30)";
    let (arena_stmt, interner) = ArenaParser::parse_insert_with_interner(sql, &arena).unwrap();

    // Convert to standard AST using the Converter
    let converter = Converter::new(&interner);
    let std_stmt: InsertStmt = converter.convert_insert(arena_stmt);
    assert_eq!(std_stmt.table_name, "users");
    assert_eq!(std_stmt.columns.len(), 2);
    assert!(matches!(std_stmt.source, InsertSource::Values(_)));
}

// ============================================================================
// Placeholder Tests
// ============================================================================

#[test]
fn test_arena_parse_delete_with_placeholder() {
    let arena = Bump::new();
    let sql = "DELETE FROM users WHERE id = ?";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(stmt.where_clause.is_some());
    if let Some(vibesql_ast::arena::WhereClause::Condition(
        vibesql_ast::arena::Expression::BinaryOp { right, .. },
    )) = &stmt.where_clause
    {
        // The right side should be a placeholder
        assert!(matches!(right, vibesql_ast::arena::Expression::Placeholder(_)));
    }
}

#[test]
fn test_arena_parse_update_with_placeholder() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = ? WHERE id = ?";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    // First placeholder in SET
    assert!(matches!(stmt.assignments[0].value, vibesql_ast::arena::Expression::Placeholder(0)));
}

#[test]
fn test_arena_parse_insert_with_placeholder() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES (?, ?)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    match &stmt.source {
        vibesql_ast::arena::InsertSource::Values(rows) => {
            assert!(matches!(rows[0][0], vibesql_ast::arena::Expression::Placeholder(0)));
            assert!(matches!(rows[0][1], vibesql_ast::arena::Expression::Placeholder(1)));
        }
        _ => panic!("Expected Values source"),
    }
}

// ============================================================================
// Source Text Preservation Tests
// ============================================================================

#[test]
fn test_source_text_preserves_original_case() {
    use crate::Lexer;

    // Test that tokenize_with_spans captures the right byte ranges
    let input = "SELECT f1+F2 FROM test1";
    let mut lexer = Lexer::new(input);
    let tokens_with_spans = lexer.tokenize_with_spans().unwrap();

    // SELECT (0-6), f1 (7-9), + (9-10), F2 (10-12), FROM (13-17), test1 (18-23), EOF
    // Check that f1 span extracts "f1" (original case)
    let (_, f1_span) = &tokens_with_spans[1];
    assert_eq!(f1_span.extract(input), "f1", "f1 span should preserve original case");

    // Check that F2 span extracts "F2" (original case)
    let (_, f2_span) = &tokens_with_spans[3];
    assert_eq!(f2_span.extract(input), "F2", "F2 span should preserve original case");
}

#[test]
fn test_arena_parser_source_text_in_select_item() {
    let arena = Bump::new();
    let sql = "SELECT f1+F2 FROM test1";
    let result = ArenaParser::parse_select_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    // Check that the select item has source_text preserved
    if let vibesql_ast::arena::SelectItem::Expression { source_text, .. } = &stmt.select_list[0] {
        assert!(source_text.is_some(), "source_text should be set");
        assert_eq!(source_text.unwrap(), "f1+F2", "source_text should preserve original case");
    } else {
        panic!("Expected Expression select item");
    }
}

// ============================================================================
// IN / NOT IN Composability Tests (issue #5801)
//
// Mirror of the standard-parser tests in tests/in_list.rs: the arena parser
// must agree with the standard parser so the arena fast path never diverges
// (parse_with_arena_fallback would otherwise silently fall back). Per SQLite,
// IN is a left-associative comparison-tier operator with a syntactically
// closed right operand, so both another comparison-tier operator and a
// tighter-binding operator (+, *, ...) may follow an IN node.
// ============================================================================

/// Parse `SELECT <expr> ...` with the arena parser and pass the first
/// select-list expression to the given assertion callback.
fn with_arena_select_expr(sql: &str, check: impl FnOnce(&vibesql_ast::arena::Expression<'_>)) {
    let arena = Bump::new();
    let result = ArenaParser::parse_select_with_interner(sql, &arena);
    assert!(result.is_ok(), "arena parser should parse {:?}: {:?}", sql, result.err());
    let (stmt, _interner) = result.unwrap();
    if let vibesql_ast::arena::SelectItem::Expression { expr, .. } = &stmt.select_list[0] {
        check(expr);
    } else {
        panic!("expected Expression select item for {:?}", sql);
    }
}

#[test]
fn test_arena_in_subquery_chained_not_in_subquery() {
    // sqlite3: SELECT 1 IN (SELECT 1) NOT IN (SELECT 2); -- 1
    with_arena_select_expr("SELECT 1 IN (SELECT 1) NOT IN (SELECT 2)", |expr| {
        let vibesql_ast::arena::Expression::Extended(ext) = expr else {
            panic!("expected Extended expression, got {:?}", expr);
        };
        let vibesql_ast::arena::ExtendedExpr::In { expr: inner, negated, .. } = ext else {
            panic!("expected outer In, got {:?}", ext);
        };
        assert!(*negated, "outer NOT IN should be negated");
        assert!(
            matches!(
                inner,
                vibesql_ast::arena::Expression::Extended(vibesql_ast::arena::ExtendedExpr::In {
                    negated: false,
                    ..
                })
            ),
            "left operand should be the inner IN node: {:?}",
            inner
        );
    });
}

#[test]
fn test_arena_in_subquery_followed_by_plus() {
    // sqlite3: SELECT 1 IN (SELECT 1) + 1; -- 2
    with_arena_select_expr("SELECT 1 IN (SELECT 1) + 1", |expr| {
        let vibesql_ast::arena::Expression::BinaryOp { op, left, .. } = expr else {
            panic!("expected BinaryOp Plus at top level, got {:?}", expr);
        };
        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
        assert!(
            matches!(
                left,
                vibesql_ast::arena::Expression::Extended(
                    vibesql_ast::arena::ExtendedExpr::In { .. }
                )
            ),
            "left operand of + should be the IN node: {:?}",
            left
        );
    });
}

#[test]
fn test_arena_in_list_chained_not_in_subquery() {
    // sqlite3: SELECT 1 IN (1,2) NOT IN (SELECT 2); -- 1
    with_arena_select_expr("SELECT 1 IN (1,2) NOT IN (SELECT 2)", |expr| {
        let vibesql_ast::arena::Expression::Extended(ext) = expr else {
            panic!("expected Extended expression, got {:?}", expr);
        };
        let vibesql_ast::arena::ExtendedExpr::In { expr: inner, negated, .. } = ext else {
            panic!("expected outer In, got {:?}", ext);
        };
        assert!(*negated);
        assert!(
            matches!(
                inner,
                vibesql_ast::arena::Expression::Extended(
                    vibesql_ast::arena::ExtendedExpr::InList { negated: false, .. }
                )
            ),
            "left operand should be the inner IN list node: {:?}",
            inner
        );
    });
}

#[test]
fn test_arena_in_lhs_precedence_preserved() {
    // sqlite3: SELECT 1 + 2 IN (3); -- 1, i.e. (1 + 2) IN (3)
    with_arena_select_expr("SELECT 1 + 2 IN (3)", |expr| {
        let vibesql_ast::arena::Expression::Extended(ext) = expr else {
            panic!("expected Extended expression, got {:?}", expr);
        };
        let vibesql_ast::arena::ExtendedExpr::InList { expr: inner, negated, .. } = ext else {
            panic!("expected InList at top level, got {:?}", ext);
        };
        assert!(!negated);
        assert!(
            matches!(
                inner,
                vibesql_ast::arena::Expression::BinaryOp {
                    op: vibesql_ast::BinaryOperator::Plus,
                    ..
                }
            ),
            "left operand of IN should be (1 + 2): {:?}",
            inner
        );
    });
}

#[test]
fn test_arena_chained_in_lists() {
    // sqlite3: SELECT 1 IN (2) IN (0); -- 1, i.e. ((1 IN (2)) IN (0))
    with_arena_select_expr("SELECT 1 IN (2) IN (0)", |expr| {
        let vibesql_ast::arena::Expression::Extended(ext) = expr else {
            panic!("expected Extended expression, got {:?}", expr);
        };
        let vibesql_ast::arena::ExtendedExpr::InList { expr: inner, .. } = ext else {
            panic!("expected outer InList, got {:?}", ext);
        };
        assert!(
            matches!(
                inner,
                vibesql_ast::arena::Expression::Extended(
                    vibesql_ast::arena::ExtendedExpr::InList { .. }
                )
            ),
            "IN chains must be left-associative: {:?}",
            inner
        );
    });
}

#[test]
fn test_arena_in_followed_by_comparison() {
    // sqlite3: SELECT 1 IN (1) = 1; -- 1
    with_arena_select_expr("SELECT 1 IN (1) = 1", |expr| {
        let vibesql_ast::arena::Expression::BinaryOp { op, left, .. } = expr else {
            panic!("expected BinaryOp Equal at top level, got {:?}", expr);
        };
        assert_eq!(*op, vibesql_ast::BinaryOperator::Equal);
        assert!(matches!(
            left,
            vibesql_ast::arena::Expression::Extended(
                vibesql_ast::arena::ExtendedExpr::InList { .. }
            )
        ));
    });
}

#[test]
fn test_arena_not_in_chained_not_in() {
    // sqlite3: SELECT 1 NOT IN (SELECT 1) NOT IN (SELECT 0); -- 0
    with_arena_select_expr("SELECT 1 NOT IN (SELECT 1) NOT IN (SELECT 0)", |expr| {
        let vibesql_ast::arena::Expression::Extended(ext) = expr else {
            panic!("expected Extended expression, got {:?}", expr);
        };
        let vibesql_ast::arena::ExtendedExpr::In { expr: inner, negated, .. } = ext else {
            panic!("expected outer In, got {:?}", ext);
        };
        assert!(*negated);
        assert!(matches!(
            inner,
            vibesql_ast::arena::Expression::Extended(vibesql_ast::arena::ExtendedExpr::In {
                negated: true,
                ..
            })
        ));
    });
}

// ============================================================================
// BETWEEN/LIKE bounds with shift operators (issue #5813)
//
// The arena parser has no shift tier, so expressions with << / >> in BETWEEN
// bounds or LIKE patterns must FAIL arena parsing (never silently truncate),
// and parse_with_arena_fallback must route them to the standard parser,
// which (post-#5813) accepts shift-tier operands in both the negated and
// non-negated forms.
// ============================================================================

#[test]
fn test_arena_between_shift_bound_fails_no_silent_truncation() {
    // Must be a hard arena-parse error, NOT a successful parse of the
    // truncated prefix "SELECT 1 BETWEEN 0 AND 1".
    let arena = Bump::new();
    let result = ArenaParser::parse_select_with_interner("SELECT 1 BETWEEN 0 AND 1<<2", &arena);
    assert!(
        result.is_err(),
        "arena parser has no shift tier; BETWEEN with << bound must fail arena parse, got: {:?}",
        result.map(|(stmt, _)| format!("{:?}", stmt.select_list[0]))
    );
}

#[test]
fn test_arena_like_shift_pattern_fails_no_silent_truncation() {
    let arena = Bump::new();
    let result = ArenaParser::parse_select_with_interner("SELECT 2 LIKE 1<<1", &arena);
    assert!(
        result.is_err(),
        "arena parser has no shift tier; LIKE with << pattern must fail arena parse, got: {:?}",
        result.map(|(stmt, _)| format!("{:?}", stmt.select_list[0]))
    );
}

#[test]
fn test_arena_fallback_between_shift_bound() {
    // sqlite3: SELECT 1 BETWEEN 0 AND 1<<2; -- 1
    // End-to-end through the arena-with-fallback entry point: arena parse
    // fails, standard parser (fixed by #5813) produces the Between node.
    let stmt = crate::parse_with_arena_fallback("SELECT 1 BETWEEN 0 AND 1<<2;")
        .expect("fallback path should parse BETWEEN with shift bound");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT statement");
    };
    let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] else {
        panic!("expected expression select item");
    };
    let vibesql_ast::Expression::Between { high, negated, .. } = expr else {
        panic!("expected Between expression, got {:?}", expr);
    };
    assert!(!negated);
    assert!(
        matches!(
            **high,
            vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::LeftShift, .. }
        ),
        "high bound should be 1<<2, got {:?}",
        high
    );
}

#[test]
fn test_arena_fallback_like_shift_pattern() {
    // sqlite3: SELECT 2 LIKE 1<<1; -- 1
    let stmt = crate::parse_with_arena_fallback("SELECT 2 LIKE 1<<1;")
        .expect("fallback path should parse LIKE with shift pattern");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT statement");
    };
    let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] else {
        panic!("expected expression select item");
    };
    let vibesql_ast::Expression::Like { pattern, negated, .. } = expr else {
        panic!("expected Like expression, got {:?}", expr);
    };
    assert!(!negated);
    assert!(
        matches!(
            **pattern,
            vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::LeftShift, .. }
        ),
        "pattern should be 1<<1, got {:?}",
        pattern
    );
}

// ============================================================================
// Mirrored chained-IN matrix (issue #5812)
//
// This matrix mirrors tests/in_list.rs (test_matrix_*): the arena parser must
// produce ASTs equivalent to the standard parser for everything it accepts.
// Shift-tier cases (<<) are outside the arena parser's grammar and must fail
// arena parsing (never silently truncate) and succeed end-to-end through
// parse_with_arena_fallback. All expected values were verified against sqlite3.
// ============================================================================

/// Assert the arena expression is `Extended(InList { .. })`.
fn assert_arena_in_list(expr: &vibesql_ast::arena::Expression<'_>, context: &str) {
    assert!(
        matches!(
            expr,
            vibesql_ast::arena::Expression::Extended(
                vibesql_ast::arena::ExtendedExpr::InList { .. }
            )
        ),
        "{}: expected IN list node, got {:?}",
        context,
        expr
    );
}

#[test]
fn test_arena_matrix_in_list_plus_literal() {
    // sqlite3: SELECT 1 IN (1) + 1; -- 2
    with_arena_select_expr("SELECT 1 IN (1) + 1", |expr| {
        let vibesql_ast::arena::Expression::BinaryOp { op, left, .. } = expr else {
            panic!("expected BinaryOp Plus at top level, got {:?}", expr);
        };
        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
        assert_arena_in_list(left, "left operand of +");
    });
}

#[test]
fn test_arena_matrix_in_plus_then_multiply() {
    // sqlite3: SELECT 1 IN (1) + 2 * 3; -- 7, i.e. (x IN (1)) + (2 * 3)
    with_arena_select_expr("SELECT x IN (1) + 2 * 3", |expr| {
        let vibesql_ast::arena::Expression::BinaryOp { op, left, right } = expr else {
            panic!("expected BinaryOp Plus at top level, got {:?}", expr);
        };
        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
        assert_arena_in_list(left, "left operand of +");
        assert!(
            matches!(
                right,
                vibesql_ast::arena::Expression::BinaryOp {
                    op: vibesql_ast::BinaryOperator::Multiply,
                    ..
                }
            ),
            "right operand of + should be (2 * 3): {:?}",
            right
        );
    });
}

#[test]
fn test_arena_matrix_in_concat() {
    // sqlite3: SELECT 1 IN (1) || 'x'; -- '1x'
    with_arena_select_expr("SELECT 1 IN (1) || 'x'", |expr| {
        let vibesql_ast::arena::Expression::BinaryOp { op, left, .. } = expr else {
            panic!("expected BinaryOp Concat at top level, got {:?}", expr);
        };
        assert_eq!(*op, vibesql_ast::BinaryOperator::Concat);
        assert_arena_in_list(left, "left operand of ||");
    });
}

#[test]
fn test_arena_matrix_in_multiply_then_plus() {
    // sqlite3: SELECT 1 IN (1) * 2 + 3; -- 5, i.e. ((1 IN (1)) * 2) + 3
    with_arena_select_expr("SELECT 1 IN (1) * 2 + 3", |expr| {
        let vibesql_ast::arena::Expression::BinaryOp { op, left, .. } = expr else {
            panic!("expected BinaryOp Plus at top level, got {:?}", expr);
        };
        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
        let vibesql_ast::arena::Expression::BinaryOp { op: inner_op, left: inner_left, .. } = left
        else {
            panic!("left operand of + should be ((1 IN (1)) * 2): {:?}", left);
        };
        assert_eq!(*inner_op, vibesql_ast::BinaryOperator::Multiply);
        assert_arena_in_list(inner_left, "innermost left");
    });
}

#[test]
fn test_arena_matrix_in_plus_minus_chain() {
    // sqlite3: SELECT 5 IN (5) + 10 - 3; -- 8, i.e. ((5 IN (5)) + 10) - 3
    with_arena_select_expr("SELECT 5 IN (5) + 10 - 3", |expr| {
        let vibesql_ast::arena::Expression::BinaryOp { op, left, .. } = expr else {
            panic!("expected BinaryOp Minus at top level, got {:?}", expr);
        };
        assert_eq!(*op, vibesql_ast::BinaryOperator::Minus);
        let vibesql_ast::arena::Expression::BinaryOp { op: inner_op, left: inner_left, .. } = left
        else {
            panic!("left operand of - should be ((5 IN (5)) + 10): {:?}", left);
        };
        assert_eq!(*inner_op, vibesql_ast::BinaryOperator::Plus);
        assert_arena_in_list(inner_left, "innermost left");
    });
}

#[test]
fn test_arena_matrix_not_in_subquery_shift_falls_back() {
    // sqlite3: SELECT 1 NOT IN (SELECT 1) << 2; -- 0
    // The arena parser has no shift tier: it must fail (never silently
    // truncate to just the NOT IN node), and the fallback path must produce
    // the standard parser's AST.
    let arena = Bump::new();
    let result = ArenaParser::parse_select_with_interner("SELECT 1 NOT IN (SELECT 1) << 2", &arena);
    assert!(
        result.is_err(),
        "arena parser has no shift tier; << after NOT IN must fail arena parse, got: {:?}",
        result.map(|(stmt, _)| format!("{:?}", stmt.select_list[0]))
    );

    let stmt = crate::parse_with_arena_fallback("SELECT 1 NOT IN (SELECT 1) << 2;")
        .expect("fallback path should parse NOT IN followed by <<");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT statement");
    };
    let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] else {
        panic!("expected expression select item");
    };
    let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr else {
        panic!("expected BinaryOp LeftShift at top level, got {:?}", expr);
    };
    assert_eq!(*op, vibesql_ast::BinaryOperator::LeftShift);
    assert!(
        matches!(**left, vibesql_ast::Expression::In { negated: true, .. }),
        "left operand of << should be the NOT IN node: {:?}",
        left
    );
}

#[test]
fn test_arena_matrix_in_shift_falls_back() {
    // sqlite3: SELECT 1 IN (1) << 2; -- 4
    let arena = Bump::new();
    let result = ArenaParser::parse_select_with_interner("SELECT 1 IN (1) << 2", &arena);
    assert!(
        result.is_err(),
        "arena parser has no shift tier; << after IN must fail arena parse, got: {:?}",
        result.map(|(stmt, _)| format!("{:?}", stmt.select_list[0]))
    );

    let stmt = crate::parse_with_arena_fallback("SELECT 1 IN (1) << 2;")
        .expect("fallback path should parse IN followed by <<");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT statement");
    };
    let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] else {
        panic!("expected expression select item");
    };
    let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr else {
        panic!("expected BinaryOp LeftShift at top level, got {:?}", expr);
    };
    assert_eq!(*op, vibesql_ast::BinaryOperator::LeftShift);
    assert!(
        matches!(**left, vibesql_ast::Expression::InList { .. }),
        "left operand of << should be the IN list node: {:?}",
        left
    );
}

#[test]
fn test_arena_matrix_in_between_shift_bound_falls_back() {
    // sqlite3: SELECT 1 IN (1) BETWEEN 0 AND 1<<2; -- 1
    // Chained-IN with a shift-tier BETWEEN bound (post-#5813): the arena
    // parser must fail on the << bound, and the fallback must produce
    // Between { expr: InList, high: (1 << 2) }.
    let arena = Bump::new();
    let result =
        ArenaParser::parse_select_with_interner("SELECT 1 IN (1) BETWEEN 0 AND 1<<2", &arena);
    assert!(
        result.is_err(),
        "arena parser has no shift tier; BETWEEN with << bound must fail arena parse, got: {:?}",
        result.map(|(stmt, _)| format!("{:?}", stmt.select_list[0]))
    );

    let stmt = crate::parse_with_arena_fallback("SELECT 1 IN (1) BETWEEN 0 AND 1<<2;")
        .expect("fallback path should parse chained IN with shift BETWEEN bound");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT statement");
    };
    let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] else {
        panic!("expected expression select item");
    };
    let vibesql_ast::Expression::Between { expr: inner, high, negated, .. } = expr else {
        panic!("expected Between at top level, got {:?}", expr);
    };
    assert!(!negated);
    assert!(
        matches!(**inner, vibesql_ast::Expression::InList { .. }),
        "BETWEEN subject should be the IN list node: {:?}",
        inner
    );
    assert!(
        matches!(
            **high,
            vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::LeftShift, .. }
        ),
        "high bound should be (1 << 2): {:?}",
        high
    );
}
