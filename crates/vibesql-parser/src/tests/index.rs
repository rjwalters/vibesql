//! Tests for INDEX DDL statements (CREATE INDEX, DROP INDEX)

use vibesql_ast::Statement;

use crate::Parser;

#[test]
fn test_create_index_simple() {
    let sql = "CREATE INDEX idx ON users(email)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(!unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].expect_column_name(), "email");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_unique_index() {
    let sql = "CREATE UNIQUE INDEX idx ON users(email)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => {
                    assert!(*unique, "Expected unique=true")
                }
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].expect_column_name(), "email");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_multi_column() {
    let sql = "CREATE INDEX idx ON users(first_name, last_name, email)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(!unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.columns.len(), 3);
            assert_eq!(stmt.columns[0].expect_column_name(), "first_name");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
            assert_eq!(stmt.columns[1].expect_column_name(), "last_name");
            assert_eq!(stmt.columns[1].direction(), vibesql_ast::OrderDirection::Asc);
            assert_eq!(stmt.columns[2].expect_column_name(), "email");
            assert_eq!(stmt.columns[2].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_unique_index_multi_column() {
    let sql = "CREATE UNIQUE INDEX idx ON orders(customer_id, order_date)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(*unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "orders");
            assert_eq!(stmt.columns.len(), 2);
            assert_eq!(stmt.columns[0].expect_column_name(), "customer_id");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
            assert_eq!(stmt.columns[1].expect_column_name(), "order_date");
            assert_eq!(stmt.columns[1].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_mixed_case_identifiers() {
    let sql = "CREATE INDEX MyIndex ON MyTable(MyColumn)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "MyIndex");
            assert_eq!(stmt.table_name, "MyTable");
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].expect_column_name(), "MyColumn");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_single_column() {
    let sql = "CREATE INDEX pk ON users(id)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "pk");
            assert_eq!(stmt.table_name, "users");
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].expect_column_name(), "id");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_with_desc() {
    let sql = "CREATE INDEX idx ON users(email DESC, created_at ASC)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(!unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.columns.len(), 2);
            assert_eq!(stmt.columns[0].expect_column_name(), "email");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Desc);
            assert_eq!(stmt.columns[1].expect_column_name(), "created_at");
            assert_eq!(stmt.columns[1].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_drop_index_simple() {
    let sql = "DROP INDEX idx";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::DropIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
        }
        other => panic!("Expected DropIndex, got: {:?}", other),
    }
}

#[test]
fn test_drop_index_mixed_case() {
    let sql = "DROP INDEX MyIndex";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::DropIndex(stmt) => {
            assert_eq!(stmt.index_name, "MyIndex");
        }
        other => panic!("Expected DropIndex, got: {:?}", other),
    }
}

// Error cases

#[test]
fn test_create_index_missing_index_name() {
    let sql = "CREATE INDEX ON table(col)";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_create_index_missing_table_name() {
    let sql = "CREATE INDEX idx (col)";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_create_index_empty_column_list() {
    let sql = "CREATE INDEX idx ON table()";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_drop_index_missing_index_name() {
    let sql = "DROP INDEX";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_create_spatial_index() {
    let sql = "CREATE SPATIAL INDEX idx_location ON places(geom)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx_location");
            assert_eq!(stmt.table_name, "places");
            assert!(
                matches!(stmt.index_type, vibesql_ast::IndexType::Spatial),
                "Expected Spatial index, got: {:?}",
                stmt.index_type
            );
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].expect_column_name(), "geom");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_spatial_index_if_not_exists() {
    let sql = "CREATE SPATIAL INDEX IF NOT EXISTS idx_boundary ON parcels(boundary)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert!(stmt.if_not_exists);
            assert_eq!(stmt.index_name, "idx_boundary");
            assert_eq!(stmt.table_name, "parcels");
            assert!(matches!(stmt.index_type, vibesql_ast::IndexType::Spatial));
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].expect_column_name(), "boundary");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

// ============================================================================
// Expression/Functional Index Tests
// ============================================================================

#[test]
fn test_create_index_expression_function_call() {
    // SQLite/PostgreSQL style functional index
    let sql = "CREATE INDEX idx_lower_name ON users((lower(name)))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx_lower_name");
            assert_eq!(stmt.table_name, "users");
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
            assert!(stmt.columns[0].get_expression().is_some());
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_expression_arithmetic() {
    // Arithmetic expression index
    let sql = "CREATE INDEX idx_sum ON numbers((a + b))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx_sum");
            assert_eq!(stmt.table_name, "numbers");
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_expression_with_desc() {
    // Expression index with DESC
    let sql = "CREATE INDEX idx_lower_name ON users((lower(name)) DESC)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Desc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_mixed_columns_and_expressions() {
    // Mix of regular columns and expression indexes
    let sql = "CREATE INDEX idx_mixed ON t(a, (lower(b)), c DESC)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 3);

            // First: column 'a'
            assert!(!stmt.columns[0].is_expression());
            assert_eq!(stmt.columns[0].expect_column_name(), "a");

            // Second: expression lower(b)
            assert!(stmt.columns[1].is_expression());
            assert!(stmt.columns[1].column_name().is_none());

            // Third: column 'c' DESC
            assert!(!stmt.columns[2].is_expression());
            assert_eq!(stmt.columns[2].expect_column_name(), "c");
            assert_eq!(stmt.columns[2].direction(), vibesql_ast::OrderDirection::Desc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_unique_index_expression() {
    // Unique expression index
    let sql = "CREATE UNIQUE INDEX idx_unique_lower ON users((lower(email)))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => {
                    assert!(*unique, "Expected unique=true");
                }
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_expression_coalesce() {
    // Expression with COALESCE
    let sql = "CREATE INDEX idx_coalesce ON t((coalesce(a, b, 0)))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_expression_complex() {
    // Complex expression
    let sql = "CREATE INDEX idx_complex ON t((a * 2 + b - 1))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

// ============================================================================
// Expression Indexes Without Outer Parentheses (Issue #4715)
// ============================================================================
// SQLite allows expression indexes without the outer parentheses, e.g.:
// CREATE INDEX idx ON t(abs(b)) -- instead of ((abs(b)))
// This should NOT be confused with prefix length syntax: name(10)

#[test]
fn test_create_index_expression_function_no_parens() {
    // SQLite-style: abs(b) without extra outer parens
    // This was previously misinterpreted as column "abs" with prefix length "b"
    let sql = "CREATE INDEX t1b ON t1(abs(b))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "t1b");
            assert_eq!(stmt.table_name, "t1");
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression(), "Expected expression index, got column");
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_unique_index_expression_no_parens() {
    // Test case from issue #4715
    let sql = "CREATE UNIQUE INDEX t1b ON t1(abs(b))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => {
                    assert!(*unique, "Expected unique=true");
                }
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_expression_lower_no_parens() {
    // Common case: lower() function for case-insensitive index
    let sql = "CREATE INDEX idx_lower ON users(lower(email))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_expression_with_desc_no_parens() {
    // Expression index with DESC, no outer parens
    let sql = "CREATE INDEX idx ON t(abs(x) DESC)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
            assert_eq!(stmt.columns[0].direction(), vibesql_ast::OrderDirection::Desc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_mixed_expression_no_parens_and_columns() {
    // Mix of expression (no outer parens) and regular columns
    let sql = "CREATE INDEX idx ON t(a, lower(b), c DESC)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 3);

            // First: column 'a'
            assert!(!stmt.columns[0].is_expression());
            assert_eq!(stmt.columns[0].expect_column_name(), "a");

            // Second: expression lower(b)
            assert!(stmt.columns[1].is_expression());

            // Third: column 'c' DESC
            assert!(!stmt.columns[2].is_expression());
            assert_eq!(stmt.columns[2].expect_column_name(), "c");
            assert_eq!(stmt.columns[2].direction(), vibesql_ast::OrderDirection::Desc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_prefix_length_still_works() {
    // Ensure prefix length syntax still works: column_name(integer)
    let sql = "CREATE INDEX idx ON users(name(10))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(!stmt.columns[0].is_expression());
            assert_eq!(stmt.columns[0].expect_column_name(), "name");
            assert_eq!(stmt.columns[0].prefix_length(), Some(10));
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_multi_arg_function_no_parens() {
    // Multi-argument function
    let sql = "CREATE INDEX idx ON t(substr(name, 1, 3))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_nested_function_no_parens() {
    // Nested function calls
    let sql = "CREATE INDEX idx ON t(lower(trim(name)))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.columns[0].is_expression());
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

// Partial-index WHERE clause parsing (issue #5091).
//
// SQLite supports `CREATE INDEX ... WHERE <predicate>` for partial indexes.
// The parser captures the predicate expression so downstream validation can
// reject misuses (e.g. window functions in the predicate).

#[test]
fn test_create_index_with_where_clause() {
    let sql = "CREATE INDEX idx ON t(a) WHERE b > 5";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.columns.len(), 1);
            assert!(stmt.where_clause.is_some(), "expected WHERE clause to be parsed");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_without_where_clause_keeps_none() {
    let sql = "CREATE INDEX idx ON t(a)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert!(stmt.where_clause.is_none(), "WHERE clause should default to None");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_unique_index_with_where_clause() {
    // SQLite allows partial UNIQUE indexes.
    let sql = "CREATE UNIQUE INDEX idx ON t(a) WHERE b IS NOT NULL";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(*unique),
                other => panic!("Expected BTree(unique=true), got: {:?}", other),
            }
            assert!(stmt.where_clause.is_some(), "expected WHERE clause to be parsed");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_with_window_function_in_where_rejected_at_validation() {
    // The parser itself accepts the WHERE expression — semantic rejection of
    // window functions happens in the executor's validation layer (see issue
    // #5091, test window1-11.1). Verify the AST captures the call so that
    // `find_window_function_in_expression` has something to walk.
    let sql = "CREATE INDEX idx ON t(a) WHERE sum(b) OVER ()";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert!(stmt.where_clause.is_some(), "expected WHERE clause to be parsed");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

// ========================================================================
// SQLite fallback keywords as index names (keyword1.test, issue #5816)
// ========================================================================

#[test]
fn test_create_index_with_fallback_keyword_name() {
    // keyword1.test .2 shape: `CREATE INDEX abort ON t1(a)`. SQLite accepts
    // any fallback keyword as an index name; the parsed name is lowercased
    // like any unquoted identifier.
    for kw in ["abort", "end", "view", "with", "temp", "cast", "current_date"] {
        let sql = format!("CREATE INDEX {} ON t1(a)", kw);
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "CREATE INDEX {:?} must parse: {:?}", kw, result.err());
        match result.unwrap() {
            Statement::CreateIndex(stmt) => {
                assert_eq!(stmt.index_name, *kw);
                assert_eq!(stmt.table_name, "t1");
            }
            other => panic!("Expected CreateIndex, got: {:?}", other),
        }
    }
}

#[test]
fn test_create_index_reserved_word_name_rejected() {
    // Truly-reserved words are not fallback keywords and stay rejected,
    // matching SQLite. Bare `if` is also rejected (consumed by the
    // IF NOT EXISTS check) — keyword1.test quotes it as `"if"`.
    for kw in ["primary", "select", "not", "if"] {
        let sql = format!("CREATE INDEX {} ON t1(a)", kw);
        assert!(
            Parser::parse_sql(&sql).is_err(),
            "CREATE INDEX {:?} must stay a parse error (SQLite parity)",
            kw
        );
    }
    // The quoted form works.
    let result = Parser::parse_sql("CREATE INDEX \"if\" ON t1(a)");
    assert!(result.is_ok(), "CREATE INDEX \"if\" must parse: {:?}", result.err());
}

#[test]
fn test_drop_index_with_fallback_keyword_name() {
    for kw in ["abort", "end", "view"] {
        let sql = format!("DROP INDEX {}", kw);
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "DROP INDEX {:?} must parse: {:?}", kw, result.err());
        match result.unwrap() {
            Statement::DropIndex(stmt) => assert_eq!(stmt.index_name, *kw),
            other => panic!("Expected DropIndex, got: {:?}", other),
        }
    }
}

#[test]
fn test_indexed_by_with_fallback_keyword_name() {
    // keyword1.test .2 shape: `SELECT b FROM t1 INDEXED BY abort WHERE a=2`,
    // including bare `if` (which the test does NOT quote in this position).
    for kw in ["abort", "if", "end", "with", "cast"] {
        let sql = format!("SELECT b FROM t1 INDEXED BY {} WHERE a = 2", kw);
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "INDEXED BY {:?} must parse: {:?}", kw, result.err());
        match result.unwrap() {
            Statement::Select(stmt) => {
                let from = stmt.from.expect("FROM clause expected");
                match from {
                    vibesql_ast::FromClause::Table { index_hint, .. } => {
                        assert_eq!(
                            index_hint,
                            Some(vibesql_ast::IndexHint::IndexedBy(kw.to_string())),
                            "hint name must be lowercased for catalog lookup"
                        );
                    }
                    other => panic!("Expected Table from-clause, got: {:?}", other),
                }
            }
            other => panic!("Expected Select, got: {:?}", other),
        }
    }
}
