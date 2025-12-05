//! Tests for window function parsing (OVER clause)

use super::*;

#[test]
fn test_empty_over_clause() {
    let sql = "SELECT ROW_NUMBER() OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::WindowFunction { function, over } => {
                        match function {
                            vibesql_ast::WindowFunctionSpec::Ranking { name, .. } => {
                                assert_eq!(name, "ROW_NUMBER");
                            }
                            _ => panic!("Expected Ranking window function"),
                        }
                        assert!(over.partition_by.is_none());
                        assert!(over.order_by.is_none());
                        assert!(over.frame.is_none());
                    }
                    _ => panic!("Expected WindowFunction expression"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_partition_by_single() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    let partition = over.partition_by.as_ref().unwrap();
                    assert_eq!(partition.len(), 1);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_partition_by_multiple() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept, region) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    let partition = over.partition_by.as_ref().unwrap();
                    assert_eq!(partition.len(), 2);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_order_by_single() {
    let sql = "SELECT RANK() OVER (ORDER BY salary DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.order_by.is_some());
                    let order = over.order_by.as_ref().unwrap();
                    assert_eq!(order.len(), 1);
                    assert_eq!(order[0].direction, vibesql_ast::OrderDirection::Desc);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_order_by_multiple() {
    let sql = "SELECT RANK() OVER (ORDER BY dept ASC, salary DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.order_by.is_some());
                    let order = over.order_by.as_ref().unwrap();
                    assert_eq!(order.len(), 2);
                    assert_eq!(order[0].direction, vibesql_ast::OrderDirection::Asc);
                    assert_eq!(order[1].direction, vibesql_ast::OrderDirection::Desc);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_partition_and_order() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    assert!(over.order_by.is_some());
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_rows_unbounded_preceding() {
    let sql = "SELECT SUM(x) OVER (ROWS UNBOUNDED PRECEDING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.frame.is_some());
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.unit, vibesql_ast::FrameUnit::Rows);
                    assert_eq!(frame.start, vibesql_ast::FrameBound::UnboundedPreceding);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_rows_between_and() {
    let sql = "SELECT SUM(x) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.frame.is_some());
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.unit, vibesql_ast::FrameUnit::Rows);

                    match &frame.start {
                        vibesql_ast::FrameBound::Preceding(_) => {}
                        _ => panic!("Expected Preceding frame start"),
                    }

                    assert!(frame.end.is_some());
                    assert_eq!(frame.end.as_ref().unwrap(), &vibesql_ast::FrameBound::CurrentRow);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_range_frame() {
    let sql = "SELECT SUM(x) OVER (RANGE UNBOUNDED PRECEDING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.frame.is_some());
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.unit, vibesql_ast::FrameUnit::Range);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_aggregate_as_window_function() {
    let sql = "SELECT SUM(salary) OVER (PARTITION BY dept) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { function, .. } => match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { name, args } => {
                        assert_eq!(name, "SUM");
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("Expected Aggregate window function"),
                },
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_ranking_functions() {
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT RANK() OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT DENSE_RANK() OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT NTILE(4) OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());
}

#[test]
fn test_value_functions() {
    let sql = "SELECT LAG(salary, 1) OVER (ORDER BY hire_date) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT LEAD(salary, 2) OVER (ORDER BY hire_date) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());
}

#[test]
fn test_complex_window_spec() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    assert!(over.order_by.is_some());
                    assert!(over.frame.is_some());

                    let frame = over.frame.as_ref().unwrap();
                    assert!(frame.end.is_some());
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_multiple_window_functions() {
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);

            for item in &select.select_list {
                match item {
                    vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                        vibesql_ast::Expression::WindowFunction { .. } => {}
                        _ => panic!("Expected WindowFunction"),
                    },
                    _ => panic!("Expected Expression"),
                }
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_unbounded_following() {
    let sql = "SELECT SUM(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.start, vibesql_ast::FrameBound::CurrentRow);
                    assert_eq!(
                        frame.end.as_ref().unwrap(),
                        &vibesql_ast::FrameBound::UnboundedFollowing
                    );
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_n_following() {
    let sql = "SELECT AVG(price) OVER (ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    let frame = over.frame.as_ref().unwrap();

                    match &frame.start {
                        vibesql_ast::FrameBound::Preceding(_) => {}
                        _ => panic!("Expected Preceding"),
                    }

                    match &frame.end {
                        Some(vibesql_ast::FrameBound::Following(_)) => {}
                        _ => panic!("Expected Following"),
                    }
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}
