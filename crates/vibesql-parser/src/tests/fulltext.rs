use super::*;

// ========================================================================
// MATCH...AGAINST Full-Text Search Tests
// ========================================================================

#[test]
fn test_parse_match_against_natural_language() {
    let result = Parser::parse_sql(
        "SELECT * FROM articles WHERE MATCH(title, body) AGAINST ('database search');",
    );
    assert!(result.is_ok(), "MATCH...AGAINST should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some(), "Should have WHERE clause");

        if let vibesql_ast::Expression::MatchAgainst { columns, mode, .. } =
            &select.where_clause.unwrap()
        {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "title");
            assert_eq!(columns[1], "body");
            assert_eq!(*mode, vibesql_ast::FulltextMode::NaturalLanguage);
        } else {
            panic!("Expected MatchAgainst expression in WHERE clause");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_match_against_boolean_mode() {
    let result = Parser::parse_sql(
        "SELECT * FROM articles WHERE MATCH(title, body) AGAINST ('+mysql -oracle' IN BOOLEAN MODE);",
    );
    assert!(result.is_ok(), "MATCH...AGAINST with IN BOOLEAN MODE should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::Expression::MatchAgainst { columns, mode, .. } =
            &select.where_clause.unwrap()
        {
            assert_eq!(columns.len(), 2);
            assert_eq!(*mode, vibesql_ast::FulltextMode::Boolean);
        } else {
            panic!("Expected MatchAgainst expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_match_against_query_expansion() {
    let result = Parser::parse_sql(
        "SELECT * FROM articles WHERE MATCH(title) AGAINST ('database' WITH QUERY EXPANSION);",
    );
    assert!(result.is_ok(), "MATCH...AGAINST WITH QUERY EXPANSION should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::Expression::MatchAgainst { columns, mode, .. } =
            &select.where_clause.unwrap()
        {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "title");
            assert_eq!(*mode, vibesql_ast::FulltextMode::QueryExpansion);
        } else {
            panic!("Expected MatchAgainst expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_match_against_single_column() {
    let result = Parser::parse_sql("SELECT * FROM articles WHERE MATCH(title) AGAINST ('search');");
    assert!(result.is_ok(), "Single column MATCH should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::Expression::MatchAgainst { columns, .. } = &select.where_clause.unwrap()
        {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "title");
        } else {
            panic!("Expected MatchAgainst expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_match_against_in_select_list() {
    let result = Parser::parse_sql(
        "SELECT id, title, MATCH(title, body) AGAINST ('search') AS relevance FROM articles;",
    );
    assert!(result.is_ok(), "MATCH in SELECT list should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 3);

        // Check the third item (MATCH expression with alias)
        if let vibesql_ast::SelectItem::Expression { expr, alias, .. } = &select.select_list[2] {
            if let vibesql_ast::Expression::MatchAgainst { columns, .. } = expr {
                assert_eq!(columns.len(), 2);
                assert_eq!(*alias, Some("relevance".to_string()));
            } else {
                panic!("Expected MatchAgainst expression");
            }
        } else {
            panic!("Expected Expression SelectItem");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_match_against_mixed_case() {
    let result =
        Parser::parse_sql("SELECT * FROM Articles WHERE MATCH(Title, Body) AGAINST ('search');");
    assert!(result.is_ok(), "Mixed case MATCH should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::Expression::MatchAgainst { columns, .. } = &select.where_clause.unwrap()
        {
            // Identifiers preserve their original case from the SQL
            assert_eq!(columns[0], "Title");
            assert_eq!(columns[1], "Body");
        } else {
            panic!("Expected MatchAgainst expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_create_fulltext_index() {
    let result = Parser::parse_sql("CREATE FULLTEXT INDEX ft_title ON articles(title);");
    assert!(result.is_ok(), "CREATE FULLTEXT INDEX should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::CreateIndex(idx_stmt) = stmt {
        assert_eq!(idx_stmt.index_name, "ft_title");
        assert_eq!(idx_stmt.table_name, "articles");
        assert_eq!(idx_stmt.columns.len(), 1);
        assert_eq!(idx_stmt.columns[0].expect_column_name(), "title");

        match &idx_stmt.index_type {
            vibesql_ast::IndexType::Fulltext => {
                // Expected
            }
            other => panic!("Expected Fulltext index type, got: {:?}", other),
        }
    } else {
        panic!("Expected CreateIndex statement");
    }
}

#[test]
fn test_parse_create_fulltext_index_multi_column() {
    let result = Parser::parse_sql("CREATE FULLTEXT INDEX ft_search ON articles(title, body);");
    assert!(result.is_ok(), "Multi-column FULLTEXT INDEX should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::CreateIndex(idx_stmt) = stmt {
        assert_eq!(idx_stmt.index_name, "ft_search");
        assert_eq!(idx_stmt.table_name, "articles");
        assert_eq!(idx_stmt.columns.len(), 2);
        assert_eq!(idx_stmt.columns[0].expect_column_name(), "title");
        assert_eq!(idx_stmt.columns[1].expect_column_name(), "body");

        match &idx_stmt.index_type {
            vibesql_ast::IndexType::Fulltext => {
                // Expected
            }
            other => panic!("Expected Fulltext index type, got: {:?}", other),
        }
    } else {
        panic!("Expected CreateIndex statement");
    }
}

#[test]
fn test_parse_create_fulltext_index_in_create_table() {
    let result = Parser::parse_sql(
        r#"CREATE TABLE articles (
            id INT PRIMARY KEY,
            title VARCHAR(200),
            body TEXT,
            FULLTEXT INDEX ft_search (title, body)
        );"#,
    );
    assert!(result.is_ok(), "FULLTEXT INDEX in CREATE TABLE should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::CreateTable(table_stmt) = stmt {
        // Check constraints for FULLTEXT index
        let has_fulltext = table_stmt.table_constraints.iter().any(|c| {
            if let vibesql_ast::TableConstraintKind::Fulltext { columns, .. } = &c.kind {
                columns.len() == 2
            } else {
                false
            }
        });
        assert!(has_fulltext, "Should have FULLTEXT constraint");
    } else {
        panic!("Expected CreateTable statement");
    }
}

// ========================================================================
// match() as a Regular Function Tests (Issue #4693)
// ========================================================================

/// Issue #4693: `match(a, b)` should be parsed as a regular function call,
/// not as a MATCH...AGAINST full-text search expression.
#[test]
fn test_match_as_function_call() {
    // This was failing with: "near 'FROM': syntax error"
    // because the parser treated MATCH as the start of MATCH...AGAINST
    let result = Parser::parse_sql("SELECT match(a, b) FROM t1 WHERE 0;");
    assert!(result.is_ok(), "match() as function should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 1);
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Function { name, args, .. } = expr {
                assert_eq!(name.canonical(), "match");
                assert_eq!(args.len(), 2);
            } else {
                panic!("Expected Function expression, got {:?}", expr);
            }
        } else {
            panic!("Expected Expression SelectItem");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_match_as_function_with_single_arg() {
    let result = Parser::parse_sql("SELECT match(x) FROM t;");
    assert!(result.is_ok(), "match(x) should parse: {:?}", result);
}

#[test]
fn test_match_as_function_in_where() {
    let result = Parser::parse_sql("SELECT * FROM t WHERE match(a, b) = 1;");
    assert!(result.is_ok(), "match() in WHERE should parse: {:?}", result);
}

#[test]
fn test_match_against_still_works() {
    // Ensure we haven't broken MATCH...AGAINST syntax
    let result = Parser::parse_sql("SELECT * FROM articles WHERE MATCH(title) AGAINST ('search');");
    assert!(result.is_ok(), "MATCH...AGAINST should still work: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let Some(vibesql_ast::Expression::MatchAgainst { columns, .. }) = &select.where_clause {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "title");
        } else {
            panic!("Expected MatchAgainst in WHERE clause");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}
