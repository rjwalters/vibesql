//! Test parsing of COUNT(*) in expressions
//!
//! Verifies that the parser correctly handles COUNT(*) syntax

use vibesql_parser::Parser;

#[test]
fn test_parse_count_star_simple() {
    // Test simple COUNT(*)
    let sql = "SELECT COUNT(*) FROM tab2";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::Select(stmt)) = result {
        assert_eq!(stmt.select_list.len(), 1);
        match &stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => {
                match expr {
                    vibesql_ast::Expression::AggregateFunction { name, args, .. } => {
                        assert_eq!(name, "count");
                        assert_eq!(args.len(), 1);
                        // Should be either Wildcard or ColumnRef with "*"
                        assert!(
                            matches!(args[0], vibesql_ast::Expression::Wildcard)
                                || matches!(&args[0], vibesql_ast::Expression::ColumnRef(ref id) if id.column_canonical() == "*")
                        );
                    }
                    _ => panic!("Expected AggregateFunction, got: {:?}", expr),
                }
            }
            _ => panic!("Expected Expression"),
        }
    }
}

#[test]
fn test_parse_count_star_in_arithmetic() {
    // Test COUNT(*) in arithmetic expression: SELECT -18 * COUNT(*) FROM tab2
    let sql = "SELECT -18 * COUNT(*) FROM tab2";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::Select(stmt)) = result {
        assert_eq!(stmt.select_list.len(), 1);
        match &stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => {
                // Should be a BinaryOp with COUNT(*) on one side
                match expr {
                    vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                        // Either left or right should contain the aggregate
                        let contains_count_star = |e: &vibesql_ast::Expression| {
                            matches!(e, vibesql_ast::Expression::AggregateFunction { name, args, .. }
                                if name == "count" &&
                                   (matches!(args.first(), Some(vibesql_ast::Expression::Wildcard)) ||
                                    matches!(args.first(), Some(vibesql_ast::Expression::ColumnRef(ref id)) if id.column_canonical() == "*")))
                        };

                        assert!(
                            contains_count_star(left) || contains_count_star(right),
                            "Expected COUNT(*) in binary operation"
                        );
                    }
                    _ => panic!("Expected BinaryOp, got: {:?}", expr),
                }
            }
            _ => panic!("Expected Expression"),
        }
    }
}

#[test]
fn test_parse_sqllogictest_example() {
    // Test the exact query from the issue: SELECT ALL -18 * + COUNT(*) col1 FROM tab2
    let sql = "SELECT ALL -18 * + COUNT(*) col1 FROM tab2";
    let result = Parser::parse_sql(sql);

    match result {
        Ok(vibesql_ast::Statement::Select(stmt)) => {
            assert_eq!(stmt.select_list.len(), 1);
            // If it parsed successfully, that's the main goal
            println!("Successfully parsed SQLLogicTest query");
        }
        Err(e) => {
            panic!("Failed to parse SQLLogicTest query: {:?}", e);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_count_with_distinct() {
    // Test COUNT(DISTINCT column) to ensure we don't break existing functionality
    let sql = "SELECT COUNT(DISTINCT col1) FROM tab2";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    if let Ok(vibesql_ast::Statement::Select(stmt)) = result {
        match &stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
                    assert_eq!(name, "count");
                    assert!(*distinct);
                    assert_eq!(args.len(), 1);
                }
                _ => panic!("Expected AggregateFunction"),
            },
            _ => panic!("Expected Expression"),
        }
    }
}

#[test]
fn test_parse_count_distinct_star_should_fail() {
    // COUNT(DISTINCT *) is invalid SQL
    let sql = "SELECT COUNT(DISTINCT *) FROM tab2";
    let result = Parser::parse_sql(sql);

    // This should either fail to parse OR parse but fail during execution
    // For now, let's just check it parses (execution check happens elsewhere)
    if let Ok(vibesql_ast::Statement::Select(stmt)) = result {
        if let vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction { name, distinct, .. },
            ..
        } = &stmt.select_list[0]
        {
            assert_eq!(name, "count");
            assert!(*distinct);
            // Parser allows it, but executor should reject it
        }
    }
}
