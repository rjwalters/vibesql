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
            assert_eq!(columns[0], "TITLE");
            assert_eq!(columns[1], "BODY");
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
            assert_eq!(columns[0], "TITLE");
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
            assert_eq!(columns[0], "TITLE");
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
        if let vibesql_ast::SelectItem::Expression { expr, alias } = &select.select_list[2] {
            if let vibesql_ast::Expression::MatchAgainst { columns, .. } = expr {
                assert_eq!(columns.len(), 2);
                assert_eq!(*alias, Some("RELEVANCE".to_string()));
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
            assert_eq!(columns[0], "TITLE");
            assert_eq!(columns[1], "BODY");
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
        assert_eq!(idx_stmt.index_name, "FT_TITLE");
        assert_eq!(idx_stmt.table_name, "ARTICLES");
        assert_eq!(idx_stmt.columns.len(), 1);
        assert_eq!(idx_stmt.columns[0].column_name, "TITLE");

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
        assert_eq!(idx_stmt.index_name, "FT_SEARCH");
        assert_eq!(idx_stmt.table_name, "ARTICLES");
        assert_eq!(idx_stmt.columns.len(), 2);
        assert_eq!(idx_stmt.columns[0].column_name, "TITLE");
        assert_eq!(idx_stmt.columns[1].column_name, "BODY");

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
