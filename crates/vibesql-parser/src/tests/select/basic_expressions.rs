use super::super::*;

// ============================================================================

#[test]
fn test_parse_select_42() {
    let result = Parser::parse_sql("SELECT 42;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42)) => {} /* Success */
                        _ => panic!("Expected Integer(42), got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
            assert!(select.where_clause.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_string() {
    let result = Parser::parse_sql("SELECT 'hello';");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s))
                        if s.as_str() == "hello" => {} /* Success */
                    _ => panic!("Expected Varchar('hello'), got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_arithmetic() {
    let result = Parser::parse_sql("SELECT 1 + 2;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::BinaryOp { op, left, right } => {
                        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
                        match **left {
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                                1,
                            )) => {}
                            _ => panic!("Expected left = 1"),
                        }
                        match **right {
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                                2,
                            )) => {}
                            _ => panic!("Expected right = 2"),
                        }
                    }
                    _ => panic!("Expected BinaryOp, got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_star() {
    let result = Parser::parse_sql("SELECT *;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Wildcard { alias: _ } => {} // Success
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_from_table() {
    let result = Parser::parse_sql("SELECT * FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match &select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Table { name, alias, .. } => {
                    assert_eq!(name, "users");
                    assert!(alias.is_none());
                }
                _ => panic!("Expected table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_columns() {
    let result = Parser::parse_sql("SELECT id, name, age FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // Check first column (id)
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef(col_id)
                        if col_id.column_canonical() == "id" => {}
                    _ => panic!("Expected id column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check second column (name)
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef(col_id)
                        if col_id.column_canonical() == "name" => {}
                    _ => panic!("Expected name column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check third column (age)
            match &select.select_list[2] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef(col_id)
                        if col_id.column_canonical() == "age" => {}
                    _ => panic!("Expected age column"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_date() {
    let result = Parser::parse_sql("SELECT CURRENT_DATE;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::CurrentDate => {}
                        _ => panic!("Expected CurrentDate, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_time() {
    let result = Parser::parse_sql("SELECT CURRENT_TIME;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::CurrentTime { precision } => {
                            assert_eq!(*precision, None);
                        }
                        _ => panic!("Expected CurrentTime, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_timestamp() {
    let result = Parser::parse_sql("SELECT CURRENT_TIMESTAMP;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::CurrentTimestamp { precision } => {
                            assert_eq!(*precision, None);
                        }
                        _ => panic!("Expected CurrentTimestamp, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_qualified_wildcard() {
    let result = Parser::parse_sql("SELECT table_name.* FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                    assert_eq!(qualifier, "table_name");
                }
                _ => panic!(
                    "Expected QualifiedWildcard select item, got {:?}",
                    select.select_list[0]
                ),
            }
            // Check FROM clause exists
            assert!(select.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_qualified_wildcard_alias() {
    let result = Parser::parse_sql("SELECT alias.* FROM table_name AS alias;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                    assert_eq!(qualifier, "alias");
                }
                _ => panic!("Expected QualifiedWildcard select item"),
            }
            assert!(select.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ============================================================================
// Regression tests for issue #5302: contextual keywords (M, YEAR, WINDOW, ...)
// were rejected (classic parser) or silently dropped (arena parser) when used
// as qualified-wildcard qualifiers, e.g. `SELECT m.* FROM map AS m`.
// ============================================================================

/// Assert the classic parser yields a QualifiedWildcard with the expected qualifier.
fn assert_classic_qualified_wildcard(sql: &str, expected_qualifier: &str) {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                assert_eq!(qualifier, expected_qualifier, "wrong qualifier for {:?}", sql);
            }
            other => panic!("Expected QualifiedWildcard for {:?}, got {:?}", sql, other),
        },
        _ => panic!("Expected SELECT statement for {:?}", sql),
    }
}

/// Assert the arena parser yields a QualifiedWildcard with the expected qualifier
/// (i.e. the qualifier is NOT silently dropped to a bare Wildcard).
fn assert_arena_qualified_wildcard(sql: &str, expected_qualifier: &str) {
    let arena = bumpalo::Bump::new();
    let (stmt, interner) =
        crate::arena_parser::ArenaParser::parse_select_with_interner(sql, &arena)
            .unwrap_or_else(|e| panic!("Failed to arena-parse {:?}: {:?}", sql, e));
    match &stmt.select_list[0] {
        vibesql_ast::arena::SelectItem::QualifiedWildcard { qualifier, .. } => {
            assert_eq!(
                interner.resolve(*qualifier),
                expected_qualifier,
                "wrong qualifier for {:?}",
                sql
            );
        }
        other => panic!("Expected QualifiedWildcard for {:?}, got {:?}", sql, other),
    }
}

#[test]
fn test_parse_select_qualified_wildcard_all_single_letter_aliases() {
    // Every single letter (a-z, A-Z) must work as a wildcard qualifier.
    // `m`/`M` lex as the contextual HNSW keyword Keyword::M and are normalized
    // to lowercase (SQL:1999 unquoted-identifier folding, matching
    // parse_identifier_expression); plain identifiers preserve source case.
    for c in ('a'..='z').chain('A'..='Z') {
        let sql = format!("SELECT {c}.* FROM map AS {c};");
        let expected = if c.eq_ignore_ascii_case(&'m') { "m".to_string() } else { c.to_string() };
        assert_classic_qualified_wildcard(&sql, &expected);
        assert_arena_qualified_wildcard(&sql, &expected);
    }
}

#[test]
fn test_parse_select_qualified_wildcard_keyword_qualifiers() {
    // Any can_be_identifier() keyword must be usable as a wildcard qualifier.
    for kw in ["year", "month", "out", "window", "range", "rowid", "temp", "filter"] {
        let sql = format!("SELECT {kw}.* FROM some_table AS {kw};");
        assert_classic_qualified_wildcard(&sql, kw);
        assert_arena_qualified_wildcard(&sql, kw);
    }
}

#[test]
fn test_parse_select_qualified_wildcard_m_in_join_not_degraded() {
    // Arena parser previously degraded `m.*` to a bare Wildcard, which in a
    // join would expand ALL tables' columns instead of just m's.
    let sql = "SELECT m.*, t.x FROM map AS m JOIN t ON m.id = t.id";

    // Classic parser
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                    assert_eq!(qualifier, "m");
                }
                other => panic!("Expected QualifiedWildcard, got {:?}", other),
            }
            assert!(matches!(&select.select_list[1], vibesql_ast::SelectItem::Expression { .. }));
        }
        _ => panic!("Expected SELECT statement"),
    }

    // Arena parser: qualifier must be preserved, not dropped to bare Wildcard
    let arena = bumpalo::Bump::new();
    let (stmt, interner) =
        crate::arena_parser::ArenaParser::parse_select_with_interner(sql, &arena)
            .unwrap_or_else(|e| panic!("Failed to arena-parse {:?}: {:?}", sql, e));
    assert_eq!(stmt.select_list.len(), 2);
    match &stmt.select_list[0] {
        vibesql_ast::arena::SelectItem::QualifiedWildcard { qualifier, .. } => {
            assert_eq!(interner.resolve(*qualifier), "m");
        }
        other => {
            panic!("Expected QualifiedWildcard (qualifier must not be dropped), got {:?}", other)
        }
    }
}

#[test]
fn test_parse_select_qualified_wildcard_keyword_case_variants() {
    // Uppercase keyword qualifier and case-mismatched alias both normalize to
    // lowercase; executor table lookup is case-insensitive.
    assert_classic_qualified_wildcard("SELECT M.* FROM map AS m;", "m");
    assert_arena_qualified_wildcard("SELECT M.* FROM map AS m;", "m");
    assert_arena_qualified_wildcard("SELECT m.* FROM map AS M;", "m");
    // Implicit alias without AS: the classic parser accepts keyword implicit
    // aliases (`FROM map m`); the arena parser does not (pre-existing
    // limitation — it errors and the caller falls back to the classic parser),
    // so only the classic parser is asserted for this variant.
    assert_classic_qualified_wildcard("SELECT m.* FROM map m;", "m");
}

#[test]
fn test_hnsw_index_with_m_parameter_still_parses() {
    // Keyword::M's legitimate use: HNSW index parameters must keep working.
    for sql in [
        "CREATE INDEX idx ON t USING HNSW (v) WITH (M = 16, EF_CONSTRUCTION = 64);",
        "CREATE INDEX idx ON t USING HNSW (v) WITH (m = 16, ef_construction = 64);",
    ] {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Failed to parse {:?}: {:?}", sql, result.err());
    }
}
