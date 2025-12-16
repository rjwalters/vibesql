//! Tests for SQL predicates (BETWEEN, LIKE, etc.)

use super::*;

// ============================================================================
// SQLite compatibility: == operator
// ============================================================================

#[test]
fn test_double_equals_sqlite_compat() {
    // SQLite uses == as a synonym for = (equality)
    let sql = "SELECT * FROM users WHERE id == 1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        // Verify it's an equality comparison
        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, left, right } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::Equal, "== should parse as Equal");

                // Check left is 'id'
                match *left {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "id");
                    }
                    _ => panic!("Expected ColumnRef for left"),
                }

                // Check right is 1
                match *right {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 1);
                    }
                    _ => panic!("Expected Integer literal for right"),
                }
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_double_equals_in_expression() {
    // Test == in more complex expressions
    let sql = "SELECT a, b FROM t WHERE a == b AND c == 'test'";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        // If it parses without error, the == operator is working
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_integer() {
    let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        // Check WHERE clause contains BETWEEN
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        // Verify it's a BETWEEN expression
        match where_expr {
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Check expr is 'age'
                match *expr {
                    vibesql_ast::Expression::ColumnRef { table, column, .. } => {
                        assert_eq!(table, None);
                        assert_eq!(column, "age");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }

                // Check low is 18
                match *low {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 18);
                    }
                    _ => panic!("Expected Integer literal for low"),
                }

                // Check high is 65
                match *high {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 65);
                    }
                    _ => panic!("Expected Integer literal for high"),
                }
            }
            _ => panic!("Expected Between expression, got {:?}", where_expr),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between() {
    let sql = "SELECT * FROM products WHERE price NOT BETWEEN 10.0 AND 20.0";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { expr, low: _, high: _, negated, symmetric } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Check expr is 'price'
                match *expr {
                    vibesql_ast::Expression::ColumnRef { table, column, .. } => {
                        assert_eq!(table, None);
                        assert_eq!(column, "price");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_with_expressions() {
    let sql = "SELECT * FROM orders WHERE total BETWEEN price * 0.9 AND price * 1.1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated);
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Verify expr is 'total'
                match *expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "total");
                    }
                    _ => panic!("Expected ColumnRef"),
                }

                // Verify low and high are multiplication expressions
                match *low {
                    vibesql_ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
                    }
                    _ => panic!("Expected BinaryOp for low"),
                }

                match *high {
                    vibesql_ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
                    }
                    _ => panic!("Expected BinaryOp for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    }
}

#[test]
fn test_between_with_column_references() {
    let sql = "SELECT * FROM data WHERE value BETWEEN min_val AND max_val";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { low, high, .. } => {
                // Verify low is min_val
                match *low {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "min_val");
                    }
                    _ => panic!("Expected ColumnRef for low"),
                }

                // Verify high is max_val
                match *high {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "max_val");
                    }
                    _ => panic!("Expected ColumnRef for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    }
}

#[test]
fn test_between_asymmetric_explicit() {
    let sql = "SELECT * FROM t WHERE x BETWEEN ASYMMETRIC 1 AND 5";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { negated, symmetric, .. } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_symmetric() {
    let sql = "SELECT * FROM t WHERE x BETWEEN SYMMETRIC 1 AND 5";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(symmetric, "Should be SYMMETRIC");

                // Check expr is 'x'
                match *expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "x");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }

                // Check low is 1
                match *low {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 1);
                    }
                    _ => panic!("Expected Integer literal for low"),
                }

                // Check high is 5
                match *high {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 5);
                    }
                    _ => panic!("Expected Integer literal for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between_symmetric() {
    let sql = "SELECT * FROM t WHERE x NOT BETWEEN SYMMETRIC 10 AND 1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { negated, symmetric, .. } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(symmetric, "Should be SYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between_asymmetric() {
    let sql = "SELECT * FROM t WHERE x NOT BETWEEN ASYMMETRIC 1 AND 10";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { negated, symmetric, .. } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}
