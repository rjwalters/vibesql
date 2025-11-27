use super::*;
use vibesql_ast::{GroupByClause, GroupingElement, GroupingSet};

// ========================================================================
// GROUP BY and HAVING Tests
// ========================================================================

#[test]
fn test_parse_group_by_single_column() {
    let result = Parser::parse_sql("SELECT name, COUNT(*) FROM users GROUP BY name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            let group_by = select.group_by.unwrap();
            match group_by {
                GroupByClause::Simple(exprs) => {
                    assert_eq!(exprs.len(), 1);
                    match &exprs[0] {
                        vibesql_ast::Expression::ColumnRef { column, .. } if column == "NAME" => {}
                        _ => panic!("Expected column reference 'name'"),
                    }
                }
                _ => panic!("Expected simple GROUP BY"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_multiple_columns() {
    let result =
        Parser::parse_sql("SELECT dept, user_role, COUNT(*) FROM users GROUP BY dept, user_role;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            let group_by = select.group_by.unwrap();
            match group_by {
                GroupByClause::Simple(exprs) => {
                    assert_eq!(exprs.len(), 2);
                    match &exprs[0] {
                        vibesql_ast::Expression::ColumnRef { column, .. } if column == "DEPT" => {}
                        _ => panic!("Expected column reference 'dept'"),
                    }
                    match &exprs[1] {
                        vibesql_ast::Expression::ColumnRef { column, .. }
                            if column == "USER_ROLE" => {}
                        _ => panic!("Expected column reference 'user_role'"),
                    }
                }
                _ => panic!("Expected simple GROUP BY"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_having_clause() {
    let result =
        Parser::parse_sql("SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 5;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            assert!(select.having.is_some());

            // HAVING should contain a comparison expression
            match select.having.as_ref().unwrap() {
                vibesql_ast::Expression::BinaryOp { op, .. } => {
                    assert_eq!(*op, vibesql_ast::BinaryOperator::GreaterThan);
                }
                _ => panic!("Expected comparison in HAVING clause"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_with_where_and_having() {
    let result = Parser::parse_sql(
        "SELECT dept, COUNT(*) FROM users WHERE active = true GROUP BY dept HAVING COUNT(*) > 10;",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should have WHERE, GROUP BY, and HAVING
            assert!(select.where_clause.is_some());
            assert!(select.group_by.is_some());
            assert!(select.having.is_some());
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_qualified_columns() {
    let result = Parser::parse_sql("SELECT u.dept, COUNT(*) FROM users u GROUP BY u.dept;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            let group_by = select.group_by.unwrap();
            match group_by {
                GroupByClause::Simple(exprs) => {
                    assert_eq!(exprs.len(), 1);
                    match &exprs[0] {
                        vibesql_ast::Expression::ColumnRef { table, column } => {
                            assert_eq!(table.as_ref().unwrap(), "U");
                            assert_eq!(column, "DEPT");
                        }
                        _ => panic!("Expected qualified column reference"),
                    }
                }
                _ => panic!("Expected simple GROUP BY"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_with_order_by() {
    let result = Parser::parse_sql(
        "SELECT name, COUNT(*) as cnt FROM users GROUP BY name ORDER BY cnt DESC;",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should have both GROUP BY and ORDER BY
            assert!(select.group_by.is_some());
            assert!(select.order_by.is_some());
        }
        _ => panic!("Expected SELECT"),
    }
}

// ========================================================================
// ROLLUP, CUBE, GROUPING SETS Tests (SQL:1999 OLAP Extensions)
// ========================================================================

#[test]
fn test_parse_group_by_rollup() {
    let result =
        Parser::parse_sql("SELECT year, quarter, SUM(sales) FROM data GROUP BY ROLLUP(year, quarter);");
    assert!(result.is_ok(), "Failed to parse ROLLUP: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            let group_by = select.group_by.unwrap();
            match group_by {
                GroupByClause::Rollup(elements) => {
                    assert_eq!(elements.len(), 2);
                    match &elements[0] {
                        GroupingElement::Single(vibesql_ast::Expression::ColumnRef {
                            column,
                            ..
                        }) => assert_eq!(column, "YEAR"),
                        _ => panic!("Expected column reference 'year'"),
                    }
                    match &elements[1] {
                        GroupingElement::Single(vibesql_ast::Expression::ColumnRef {
                            column,
                            ..
                        }) => assert_eq!(column, "QUARTER"),
                        _ => panic!("Expected column reference 'quarter'"),
                    }
                }
                _ => panic!("Expected ROLLUP clause"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_cube() {
    let result =
        Parser::parse_sql("SELECT region, product, SUM(sales) FROM data GROUP BY CUBE(region, product);");
    assert!(result.is_ok(), "Failed to parse CUBE: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            let group_by = select.group_by.unwrap();
            match group_by {
                GroupByClause::Cube(elements) => {
                    assert_eq!(elements.len(), 2);
                    match &elements[0] {
                        GroupingElement::Single(vibesql_ast::Expression::ColumnRef {
                            column,
                            ..
                        }) => assert_eq!(column, "REGION"),
                        _ => panic!("Expected column reference 'region'"),
                    }
                    match &elements[1] {
                        GroupingElement::Single(vibesql_ast::Expression::ColumnRef {
                            column,
                            ..
                        }) => assert_eq!(column, "PRODUCT"),
                        _ => panic!("Expected column reference 'product'"),
                    }
                }
                _ => panic!("Expected CUBE clause"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_grouping_sets() {
    let result = Parser::parse_sql(
        "SELECT year, quarter, SUM(sales) FROM data GROUP BY GROUPING SETS((year, quarter), (year), ());",
    );
    assert!(result.is_ok(), "Failed to parse GROUPING SETS: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            let group_by = select.group_by.unwrap();
            match group_by {
                GroupByClause::GroupingSets(sets) => {
                    assert_eq!(sets.len(), 3);
                    // First set: (year, quarter)
                    assert_eq!(sets[0].columns.len(), 2);
                    // Second set: (year)
                    assert_eq!(sets[1].columns.len(), 1);
                    // Third set: () - grand total
                    assert_eq!(sets[2].columns.len(), 0);
                }
                _ => panic!("Expected GROUPING SETS clause"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_rollup_with_composite() {
    // ROLLUP((a, b), c) - (a, b) treated as single grouping unit
    let result = Parser::parse_sql(
        "SELECT a, b, c, SUM(d) FROM data GROUP BY ROLLUP((a, b), c);",
    );
    assert!(result.is_ok(), "Failed to parse ROLLUP with composite: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.group_by.is_some());
            let group_by = select.group_by.unwrap();
            match group_by {
                GroupByClause::Rollup(elements) => {
                    assert_eq!(elements.len(), 2);
                    // First element should be composite (a, b)
                    match &elements[0] {
                        GroupingElement::Composite(exprs) => {
                            assert_eq!(exprs.len(), 2);
                        }
                        _ => panic!("Expected composite element (a, b)"),
                    }
                    // Second element should be single c
                    match &elements[1] {
                        GroupingElement::Single(_) => {}
                        _ => panic!("Expected single element c"),
                    }
                }
                _ => panic!("Expected ROLLUP clause"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_grouping_function() {
    let result = Parser::parse_sql(
        "SELECT year, GROUPING(year), SUM(sales) FROM data GROUP BY ROLLUP(year);",
    );
    assert!(result.is_ok(), "Failed to parse GROUPING function: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Second select item should be GROUPING(year)
            assert!(select.select_list.len() >= 2);
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => {
                    match expr {
                        vibesql_ast::Expression::Function { name, args, .. } => {
                            assert_eq!(name.to_uppercase(), "GROUPING");
                            assert_eq!(args.len(), 1);
                        }
                        _ => panic!("Expected GROUPING function"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_group_by_clause_len() {
    // Test the len() helper method
    let simple = GroupByClause::Simple(vec![
        vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
        vibesql_ast::Expression::ColumnRef { table: None, column: "b".to_string() },
    ]);
    assert_eq!(simple.len(), 2);

    let rollup = GroupByClause::Rollup(vec![
        GroupingElement::Single(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "a".to_string(),
        }),
        GroupingElement::Composite(vec![
            vibesql_ast::Expression::ColumnRef { table: None, column: "b".to_string() },
            vibesql_ast::Expression::ColumnRef { table: None, column: "c".to_string() },
        ]),
    ]);
    assert_eq!(rollup.len(), 3); // a, b, c = 3 total expressions
}

#[test]
fn test_group_by_clause_is_simple() {
    let simple = GroupByClause::Simple(vec![]);
    assert!(simple.is_simple());

    let rollup = GroupByClause::Rollup(vec![]);
    assert!(!rollup.is_simple());

    let cube = GroupByClause::Cube(vec![]);
    assert!(!cube.is_simple());

    let grouping_sets = GroupByClause::GroupingSets(vec![]);
    assert!(!grouping_sets.is_simple());
}
