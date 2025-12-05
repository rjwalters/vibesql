use super::*;

// ========================================================================
// LIKE Pattern Matching Tests
// ========================================================================

#[test]
fn test_parse_like_exact_match() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE name LIKE 'John';");
    assert!(result.is_ok(), "LIKE with exact pattern should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        // Should be a LIKE expression
        if let vibesql_ast::Expression::Like { expr, pattern, negated } =
            &select.where_clause.unwrap()
        {
            // Left side should be column reference
            assert!(matches!(**expr, vibesql_ast::Expression::ColumnRef { .. }));

            // Pattern should be string literal
            assert!(matches!(
                **pattern,
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(_))
            ));

            // Not negated
            assert!(!(*negated));
        } else {
            panic!("Expected LIKE expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_like_with_percent_wildcard() {
    let result = Parser::parse_sql("SELECT * FROM products WHERE name LIKE 'Widget%';");
    assert!(result.is_ok(), "LIKE with % wildcard should parse: {:?}", result);
}

#[test]
fn test_parse_like_with_underscore_wildcard() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE code LIKE 'A_C';");
    assert!(result.is_ok(), "LIKE with _ wildcard should parse: {:?}", result);
}

#[test]
fn test_parse_like_starts_with() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE name LIKE 'John%';");
    assert!(result.is_ok(), "LIKE starts-with pattern should parse: {:?}", result);
}

#[test]
fn test_parse_like_ends_with() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE email LIKE '%@example.com';");
    assert!(result.is_ok(), "LIKE ends-with pattern should parse: {:?}", result);
}

#[test]
fn test_parse_like_contains() {
    let result = Parser::parse_sql("SELECT * FROM products WHERE description LIKE '%special%';");
    assert!(result.is_ok(), "LIKE contains pattern should parse: {:?}", result);
}

#[test]
fn test_parse_like_complex_pattern() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE name LIKE 'J%_n';");
    assert!(result.is_ok(), "LIKE with mixed wildcards should parse: {:?}", result);
}

#[test]
fn test_parse_not_like() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE name NOT LIKE 'Admin%';");
    assert!(result.is_ok(), "NOT LIKE should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        // Should be a LIKE expression with negated=true
        if let vibesql_ast::Expression::Like { negated, .. } = &select.where_clause.unwrap() {
            assert!(*negated, "NOT LIKE should set negated=true");
        } else {
            panic!("Expected LIKE expression");
        }
    }
}

#[test]
fn test_parse_like_with_and() {
    let result =
        Parser::parse_sql("SELECT * FROM users WHERE name LIKE 'J%' AND email LIKE '%@gmail.com';");
    assert!(result.is_ok(), "LIKE with AND should parse: {:?}", result);
}

#[test]
fn test_parse_like_with_or() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE name LIKE 'Widget%' OR name LIKE 'Gadget%';",
    );
    assert!(result.is_ok(), "LIKE with OR should parse: {:?}", result);
}

#[test]
fn test_parse_like_in_complex_expression() {
    let result = Parser::parse_sql(
        "SELECT * FROM users WHERE (name LIKE 'A%' OR name LIKE 'B%') AND active = TRUE;",
    );
    assert!(result.is_ok(), "LIKE in complex expression should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_like_conditions() {
    let result = Parser::parse_sql(
        "SELECT * FROM files WHERE filename LIKE '%.pdf' AND path NOT LIKE '/tmp/%';",
    );
    assert!(result.is_ok(), "Multiple LIKE conditions should parse: {:?}", result);
}

#[test]
fn test_parse_like_case_sensitive() {
    // LIKE is case-sensitive in standard SQL
    let result = Parser::parse_sql("SELECT * FROM users WHERE name LIKE 'john';");
    assert!(result.is_ok(), "Lowercase LIKE pattern should parse: {:?}", result);
}
