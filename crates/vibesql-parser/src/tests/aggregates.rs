use super::*;

// ========================================================================
// Aggregate Function Tests
// ========================================================================

#[test]
fn test_parse_count_star() {
    let result = Parser::parse_sql("SELECT COUNT(*) FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
                        assert_eq!(name, "count");
                        assert!(!(*distinct));
                        assert_eq!(args.len(), 1);
                        // COUNT(*) is represented as a special wildcard expression
                    }
                    _ => panic!("Expected aggregate function call"),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_count_column() {
    let result = Parser::parse_sql("SELECT COUNT(id) FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
                    assert_eq!(name, "count");
                    assert!(!(*distinct));
                    assert_eq!(args.len(), 1);
                    match &args[0] {
                        vibesql_ast::Expression::ColumnRef(col_id)
                            if col_id.column_canonical() == "id" => {}
                        _ => panic!("Expected column reference"),
                    }
                }
                _ => panic!("Expected aggregate function call"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_sum_function() {
    let result = Parser::parse_sql("SELECT SUM(amount) FROM orders;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
                    assert_eq!(name, "sum");
                    assert!(!(*distinct));
                    assert_eq!(args.len(), 1);
                }
                _ => panic!("Expected SUM aggregate function"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_avg_function() {
    let result = Parser::parse_sql("SELECT AVG(price) FROM products;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, .. } => {
                    assert_eq!(name, "avg");
                }
                _ => panic!("Expected AVG aggregate function"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_min_max_functions() {
    let result = Parser::parse_sql("SELECT MIN(price), MAX(price) FROM products;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);

            // Check MIN
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "min");
                    }
                    _ => panic!("Expected MIN aggregate function"),
                },
                _ => panic!("Expected expression"),
            }

            // Check MAX
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "max");
                    }
                    _ => panic!("Expected MAX aggregate function"),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_scalar_min_max_functions() {
    // Multi-argument MIN/MAX should be parsed as regular (scalar) functions, not aggregates
    let result = Parser::parse_sql("SELECT min(11, 22), max(1, 2, 3);");
    assert!(result.is_ok(), "Failed to parse scalar min/max: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);

            // Check min(11, 22) - should be a scalar Function, not AggregateFunction
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::Function { name, args, .. } => {
                        assert_eq!(name, "min");
                        assert_eq!(args.len(), 2, "min(11, 22) should have 2 arguments");
                    }
                    vibesql_ast::Expression::AggregateFunction { .. } => {
                        panic!("Multi-argument min should be parsed as scalar Function, not AggregateFunction");
                    }
                    _ => panic!("Expected scalar Function, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }

            // Check max(1, 2, 3) - should be a scalar Function, not AggregateFunction
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::Function { name, args, .. } => {
                        assert_eq!(name, "max");
                        assert_eq!(args.len(), 3, "max(1, 2, 3) should have 3 arguments");
                    }
                    vibesql_ast::Expression::AggregateFunction { .. } => {
                        panic!("Multi-argument max should be parsed as scalar Function, not AggregateFunction");
                    }
                    _ => panic!("Expected scalar Function, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_aggregate_with_alias() {
    let result = Parser::parse_sql("SELECT COUNT(*) AS total FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "count");
                    }
                    _ => panic!("Expected aggregate function"),
                }
                assert_eq!(alias.as_ref().unwrap(), "total");
            }
            _ => panic!("Expected expression with alias"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_aggregate_with_alias_without_as() {
    let result = Parser::parse_sql("SELECT COUNT(*) total FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "count");
                    }
                    _ => panic!("Expected aggregate function"),
                }
                assert_eq!(alias.as_ref().unwrap(), "total");
            }
            _ => panic!("Expected expression with alias"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_multiple_aggregates() {
    let result = Parser::parse_sql("SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // Verify all are aggregate functions
            for item in &select.select_list {
                match item {
                    vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                        vibesql_ast::Expression::AggregateFunction { .. } => {} // Success
                        _ => panic!("Expected aggregate function"),
                    },
                    _ => panic!("Expected expression"),
                }
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

// ========================================================================
// FILTER Clause Tests (SQL:2003)
// ========================================================================

#[test]
fn test_parse_count_with_filter() {
    let result = Parser::parse_sql("SELECT COUNT(*) FILTER (WHERE active = 1) FROM users;");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::AggregateFunction { name, filter, .. } => {
                        assert_eq!(name.canonical(), "count");
                        assert!(filter.is_some(), "Expected FILTER clause");
                    }
                    _ => panic!("Expected aggregate function"),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_sum_with_filter() {
    let result =
        Parser::parse_sql("SELECT SUM(amount) FILTER (WHERE status = 'completed') FROM orders;");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, filter, .. } => {
                    assert_eq!(name.canonical(), "sum");
                    assert!(filter.is_some(), "Expected FILTER clause");
                }
                _ => panic!("Expected aggregate function"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_window_aggregate_with_filter() {
    let result = Parser::parse_sql(
        "SELECT COUNT(*) FILTER (WHERE x > 0) OVER (PARTITION BY dept) FROM employees;",
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { function, .. } => match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { name, filter, .. } => {
                        assert_eq!(name.canonical(), "count");
                        assert!(filter.is_some(), "Expected FILTER clause in window aggregate");
                    }
                    _ => panic!("Expected window aggregate function"),
                },
                _ => panic!("Expected window function"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_filter_not_allowed_on_non_aggregate() {
    // FILTER should only be allowed on aggregate functions
    let result = Parser::parse_sql("SELECT UPPER(name) FILTER (WHERE active = 1) FROM users;");
    assert!(result.is_err(), "Expected parse error for FILTER on non-aggregate");
}

#[test]
fn test_order_by_not_allowed_on_non_aggregate() {
    // Issue #5712 (aggorderby-1.3): ORDER BY inside a non-aggregate function
    // must produce the SQLite-compatible message
    // "ORDER BY may not be used with non-aggregate <F>()" rather than a generic
    // `near "ORDER": syntax error`. The ORDER BY keyword must be parsed
    // unconditionally so the post-parse aggregate check can emit the error.
    let result = Parser::parse_sql("SELECT abs(a ORDER BY max(d)) FROM t1;");
    assert!(result.is_err(), "Expected parse error for ORDER BY on non-aggregate");
    let msg = result.unwrap_err().to_string();
    // SQLite reports the function name in its original case: `abs()`.
    assert!(
        msg.contains("ORDER BY may not be used with non-aggregate abs()"),
        "Unexpected error message: {msg}"
    );
}

// ========================================================================
// WITHIN GROUP (ORDER BY ...) ordered-set aggregate tests (issue #5852)
// ========================================================================

/// Extract the single aggregate function from `SELECT <agg> FROM t1;`.
fn parse_single_aggregate(sql: &str) -> (String, bool, Vec<vibesql_ast::Expression>) {
    let stmt = Parser::parse_sql(sql).expect("expected successful parse");
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
                    (name.to_string(), *distinct, args.clone())
                }
                other => panic!("Expected aggregate function, got {other:?}"),
            },
            _ => panic!("Expected expression select item"),
        },
        _ => panic!("Expected SELECT"),
    }
}

fn assert_column(expr: &vibesql_ast::Expression, expected: &str) {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) if col_id.column_canonical() == expected => {}
        other => panic!("Expected column reference {expected}, got {other:?}"),
    }
}

#[test]
fn test_within_group_percentile_cont_rewrite() {
    // percentile_cont(0.5) WITHIN GROUP (ORDER BY x) rewrites to the two-arg
    // form percentile_cont(x, 0.5): the ORDER BY expr becomes the leading Y arg.
    let (name, distinct, args) =
        parse_single_aggregate("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t1;");
    assert_eq!(name, "percentile_cont");
    assert!(!distinct);
    assert_eq!(args.len(), 2, "expected rewritten two-arg call");
    assert_column(&args[0], "x");
    // Second argument is the original fraction literal.
    assert!(
        matches!(&args[1], vibesql_ast::Expression::Literal(_)),
        "expected fraction literal as second arg, got {:?}",
        args[1]
    );
}

#[test]
fn test_within_group_percentile_rewrite() {
    // percentile(25) WITHIN GROUP (ORDER BY x) rewrites to percentile(x, 25).
    let (name, _distinct, args) =
        parse_single_aggregate("SELECT percentile(25) WITHIN GROUP (ORDER BY x) FROM t1;");
    assert_eq!(name, "percentile");
    assert_eq!(args.len(), 2);
    assert_column(&args[0], "x");
}

#[test]
fn test_within_group_median_zero_arg_rewrite() {
    // median() WITHIN GROUP (ORDER BY x) is the zero-arg outer form; it rewrites
    // to the one-arg call median(x).
    let (name, distinct, args) =
        parse_single_aggregate("SELECT median() WITHIN GROUP (ORDER BY x) FROM t1;");
    assert_eq!(name, "median");
    assert!(!distinct);
    assert_eq!(args.len(), 1, "expected rewritten one-arg call");
    assert_column(&args[0], "x");
}

#[test]
fn test_within_group_ignores_desc_and_nulls() {
    // ASC/DESC and NULLS FIRST/LAST are accepted and ignored by the rewrite.
    let (name, _distinct, args) = parse_single_aggregate(
        "SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY x DESC NULLS LAST) FROM t1;",
    );
    assert_eq!(name, "percentile_disc");
    assert_eq!(args.len(), 2);
    assert_column(&args[0], "x");
}

#[test]
fn test_within_group_distinct_rejected() {
    // DISTINCT is not allowed on the ordered-set form (percentile-1.1.distinct.2).
    // The function name is reported in lowercase: `percentile()`.
    let result =
        Parser::parse_sql("SELECT percentile(DISTINCT 50) WITHIN GROUP (ORDER BY x) FROM t1;");
    assert!(result.is_err(), "Expected parse error for DISTINCT ordered-set aggregate");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("DISTINCT not allowed on ordered-set aggregate percentile()"),
        "Unexpected error message: {msg}"
    );
}

#[test]
fn test_median_distinct_still_legal() {
    // The non-WITHIN-GROUP form median(DISTINCT x) remains valid.
    let (name, distinct, args) = parse_single_aggregate("SELECT median(DISTINCT x) FROM t1;");
    assert_eq!(name, "median");
    assert!(distinct);
    assert_eq!(args.len(), 1);
    assert_column(&args[0], "x");
}

#[test]
fn test_within_still_usable_as_identifier() {
    // WITHIN is a non-reserved keyword: it must remain usable as a plain
    // column/table identifier (non-regression).
    let result = Parser::parse_sql("SELECT within FROM within;");
    assert!(result.is_ok(), "Expected `within` to parse as an identifier: {result:?}");

    let result2 = Parser::parse_sql("SELECT within AS w FROM t1;");
    assert!(result2.is_ok(), "Expected `within` column reference to parse: {result2:?}");
}

#[test]
fn test_within_group_rejected_on_non_ordered_set_aggregate() {
    // WITHIN GROUP is only meaningful for the percentile family; other
    // aggregates reject it.
    let result = Parser::parse_sql("SELECT sum(x) WITHIN GROUP (ORDER BY y) FROM t1;");
    assert!(result.is_err(), "Expected parse error for WITHIN GROUP on sum()");
}
