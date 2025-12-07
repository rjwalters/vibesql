//! Parser tests for EXTRACT(field FROM expr) syntax
//! SQL:1999 Section 6.18: Datetime value function

use super::super::*;

/// Test parsing: SELECT EXTRACT(YEAR FROM '2024-01-15')
#[test]
fn test_parse_extract_year_from_literal() {
    let result = Parser::parse_sql("SELECT EXTRACT(YEAR FROM '2024-01-15');");
    if result.is_err() {
        eprintln!("Parse error: {:?}", result.as_ref().err());
    }
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::Extract { field, expr } => {
                        assert_eq!(*field, vibesql_ast::IntervalUnit::Year);
                        assert_eq!(
                            **expr,
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                                arcstr::ArcStr::from("2024-01-15")
                            ))
                        );
                    }
                    _ => panic!("Expected Extract expression, got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT EXTRACT(MONTH FROM order_date) FROM orders
#[test]
fn test_parse_extract_month_from_column() {
    let result = Parser::parse_sql("SELECT EXTRACT(MONTH FROM order_date) FROM orders;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::Extract { field, expr } => {
                        assert_eq!(*field, vibesql_ast::IntervalUnit::Month);
                        match &**expr {
                            vibesql_ast::Expression::ColumnRef { column, .. } => {
                                assert_eq!(column, "ORDER_DATE");
                            }
                            _ => panic!("Expected column reference in Extract"),
                        }
                    }
                    _ => panic!("Expected Extract expression"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT EXTRACT(DAY FROM '2024-01-15')
#[test]
fn test_parse_extract_day() {
    let result = Parser::parse_sql("SELECT EXTRACT(DAY FROM '2024-01-15');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => match &select_stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Extract { field, .. } => {
                    assert_eq!(*field, vibesql_ast::IntervalUnit::Day);
                }
                _ => panic!("Expected Extract expression"),
            },
            _ => panic!("Expected Expression select item"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT EXTRACT(HOUR FROM timestamp_column)
#[test]
fn test_parse_extract_hour() {
    let result = Parser::parse_sql("SELECT EXTRACT(HOUR FROM timestamp_column);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => match &select_stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Extract { field, .. } => {
                    assert_eq!(*field, vibesql_ast::IntervalUnit::Hour);
                }
                _ => panic!("Expected Extract expression"),
            },
            _ => panic!("Expected Expression select item"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT EXTRACT(MINUTE FROM time_col)
#[test]
fn test_parse_extract_minute() {
    let result = Parser::parse_sql("SELECT EXTRACT(MINUTE FROM time_col);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => match &select_stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Extract { field, .. } => {
                    assert_eq!(*field, vibesql_ast::IntervalUnit::Minute);
                }
                _ => panic!("Expected Extract expression"),
            },
            _ => panic!("Expected Expression select item"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT EXTRACT(SECOND FROM time_col)
#[test]
fn test_parse_extract_second() {
    let result = Parser::parse_sql("SELECT EXTRACT(SECOND FROM time_col);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => match &select_stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Extract { field, .. } => {
                    assert_eq!(*field, vibesql_ast::IntervalUnit::Second);
                }
                _ => panic!("Expected Extract expression"),
            },
            _ => panic!("Expected Expression select item"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: EXTRACT with nested function call
/// SELECT EXTRACT(YEAR FROM CURRENT_DATE)
#[test]
fn test_parse_extract_from_current_date() {
    let result = Parser::parse_sql("SELECT EXTRACT(YEAR FROM CURRENT_DATE);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => match &select_stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Extract { field, expr } => {
                    assert_eq!(*field, vibesql_ast::IntervalUnit::Year);
                    assert!(matches!(**expr, vibesql_ast::Expression::CurrentDate));
                }
                _ => panic!("Expected Extract expression"),
            },
            _ => panic!("Expected Expression select item"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test case-insensitivity of EXTRACT and field names
#[test]
fn test_parse_extract_case_insensitive() {
    let result = Parser::parse_sql("SELECT extract(year FROM date_col);");
    assert!(result.is_ok());

    let result2 = Parser::parse_sql("SELECT EXTRACT(Year FROM date_col);");
    assert!(result2.is_ok());
}

/// Test EXTRACT with table-qualified column
#[test]
fn test_parse_extract_with_qualified_column() {
    let result = Parser::parse_sql("SELECT EXTRACT(MONTH FROM orders.order_date) FROM orders;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => match &select_stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Extract { field, expr } => {
                    assert_eq!(*field, vibesql_ast::IntervalUnit::Month);
                    match &**expr {
                        vibesql_ast::Expression::ColumnRef { table, column } => {
                            assert_eq!(table.as_deref(), Some("ORDERS"));
                            assert_eq!(column, "ORDER_DATE");
                        }
                        _ => panic!("Expected qualified column reference"),
                    }
                }
                _ => panic!("Expected Extract expression"),
            },
            _ => panic!("Expected Expression select item"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test TPC-H Q7 style query with EXTRACT in SELECT and GROUP BY
/// This tests the exact pattern used in TPC-H queries Q7, Q8, Q9
#[test]
fn test_parse_tpch_q7_style_extract() {
    let query = r#"
        SELECT
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            EXTRACT(YEAR FROM l_shipdate) as l_year,
            SUM(l_extendedprice * (1 - l_discount)) as revenue
        FROM supplier, lineitem, orders, customer, nation n1, nation n2
        WHERE s_suppkey = l_suppkey
        GROUP BY n1.n_name, n2.n_name, EXTRACT(YEAR FROM l_shipdate)
        ORDER BY supp_nation, cust_nation, l_year
    "#;

    let result = Parser::parse_sql(query);
    if result.is_err() {
        eprintln!("Parse error: {:?}", result.as_ref().err());
    }
    assert!(result.is_ok(), "TPC-H Q7 style query should parse successfully");
}

/// Test TPC-H Q8 style query with EXTRACT in SELECT and GROUP BY
#[test]
fn test_parse_tpch_q8_style_extract() {
    let query = r#"
        SELECT
            EXTRACT(YEAR FROM o_orderdate) as o_year,
            SUM(l_extendedprice * (1 - l_discount)) as mkt_share
        FROM part, supplier, lineitem, orders
        WHERE p_partkey = l_partkey
        GROUP BY EXTRACT(YEAR FROM o_orderdate)
        ORDER BY o_year
    "#;

    let result = Parser::parse_sql(query);
    if result.is_err() {
        eprintln!("Parse error: {:?}", result.as_ref().err());
    }
    assert!(result.is_ok(), "TPC-H Q8 style query should parse successfully");
}

/// Test TPC-H Q9 style query with EXTRACT in SELECT and GROUP BY
#[test]
fn test_parse_tpch_q9_style_extract() {
    let query = r#"
        SELECT
            n_name as nation,
            EXTRACT(YEAR FROM o_orderdate) as o_year,
            SUM(l_extendedprice * (1 - l_discount)) as sum_profit
        FROM part, supplier, lineitem, orders, nation
        WHERE s_suppkey = l_suppkey
        GROUP BY n_name, EXTRACT(YEAR FROM o_orderdate)
        ORDER BY nation, o_year DESC
    "#;

    let result = Parser::parse_sql(query);
    if result.is_err() {
        eprintln!("Parse error: {:?}", result.as_ref().err());
    }
    assert!(result.is_ok(), "TPC-H Q9 style query should parse successfully");
}
