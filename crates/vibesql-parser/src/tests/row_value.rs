//! Tests for SQL row value constructors (tuples)
//!
//! Row value constructors allow comparing tuples element by element.
//! SQL:1999 Section 7.1: Row value constructor
//!
//! Examples:
//! - (a, b) = (1, 2)
//! - (rowid, 1) <= (5, 0)
//! - (a, b, c) BETWEEN (1, 2, 3) AND (4, 5, 6)

use super::*;

// ============================================================================
// Basic row value constructor parsing
// ============================================================================

#[test]
fn test_row_value_two_elements() {
    let sql = "SELECT * FROM t WHERE (a, b) = (1, 2)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        // Verify it's a comparison between two row value constructors
        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, left, right } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::Equal);

                // Check left is a row value constructor (a, b)
                match *left {
                    vibesql_ast::Expression::RowValueConstructor(ref values) => {
                        assert_eq!(values.len(), 2);
                    }
                    _ => panic!("Expected RowValueConstructor for left, got {:?}", left),
                }

                // Check right is a row value constructor (1, 2)
                match *right {
                    vibesql_ast::Expression::RowValueConstructor(ref values) => {
                        assert_eq!(values.len(), 2);
                    }
                    _ => panic!("Expected RowValueConstructor for right, got {:?}", right),
                }
            }
            _ => panic!("Expected BinaryOp expression, got {:?}", where_expr),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_row_value_less_than_or_equal() {
    // This is the failing case from the issue
    let sql = "SELECT a FROM t WHERE (rowid, 1) <= (5, 0)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, left, right } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::LessThanOrEqual);

                // Check left is (rowid, 1)
                match *left {
                    vibesql_ast::Expression::RowValueConstructor(ref values) => {
                        assert_eq!(values.len(), 2);
                        // First element should be column 'rowid'
                        match &values[0] {
                            vibesql_ast::Expression::ColumnRef(col_id) => {
            let column = col_id.column_canonical();
                                assert_eq!(column, "rowid");
                            }
                            _ => panic!("Expected ColumnRef for first element"),
                        }
                        // Second element should be literal 1
                        match &values[1] {
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)) => {
                            }
                            _ => panic!("Expected Integer(1) for second element"),
                        }
                    }
                    _ => panic!("Expected RowValueConstructor for left"),
                }

                // Check right is (5, 0)
                match *right {
                    vibesql_ast::Expression::RowValueConstructor(ref values) => {
                        assert_eq!(values.len(), 2);
                    }
                    _ => panic!("Expected RowValueConstructor for right"),
                }
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_row_value_three_elements() {
    let sql = "SELECT * FROM t WHERE (a, b, c) = (1, 2, 3)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, left, right } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::Equal);

                // Check both sides have 3 elements
                match *left {
                    vibesql_ast::Expression::RowValueConstructor(ref values) => {
                        assert_eq!(values.len(), 3);
                    }
                    _ => panic!("Expected RowValueConstructor for left"),
                }

                match *right {
                    vibesql_ast::Expression::RowValueConstructor(ref values) => {
                        assert_eq!(values.len(), 3);
                    }
                    _ => panic!("Expected RowValueConstructor for right"),
                }
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_row_value_less_than() {
    let sql = "SELECT * FROM t WHERE (a, b) < (c, d)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, .. } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::LessThan);
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_row_value_greater_than() {
    let sql = "SELECT * FROM t WHERE (a, b) > (1, 2)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, .. } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::GreaterThan);
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_row_value_greater_than_or_equal() {
    let sql = "SELECT * FROM t WHERE (a, b) >= (1, 2)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, .. } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::GreaterThanOrEqual);
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_row_value_not_equal() {
    let sql = "SELECT * FROM t WHERE (a, b) <> (1, 2)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, .. } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::NotEqual);
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

// ============================================================================
// Row value constructors with expressions
// ============================================================================

#[test]
fn test_row_value_with_expressions() {
    let sql = "SELECT * FROM t WHERE (a + 1, b * 2) = (3, 4)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { left, .. } => {
                match *left {
                    vibesql_ast::Expression::RowValueConstructor(ref values) => {
                        assert_eq!(values.len(), 2);
                        // First element should be a + 1
                        match &values[0] {
                            vibesql_ast::Expression::BinaryOp { op, .. } => {
                                assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
                            }
                            _ => panic!("Expected BinaryOp for first element"),
                        }
                        // Second element should be b * 2
                        match &values[1] {
                            vibesql_ast::Expression::BinaryOp { op, .. } => {
                                assert_eq!(*op, vibesql_ast::BinaryOperator::Multiply);
                            }
                            _ => panic!("Expected BinaryOp for second element"),
                        }
                    }
                    _ => panic!("Expected RowValueConstructor for left"),
                }
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

// ============================================================================
// Row value constructors preserve parenthesized single expression behavior
// ============================================================================

#[test]
fn test_parenthesized_single_expression_unchanged() {
    // Single expression in parens should NOT be a row value constructor
    let sql = "SELECT * FROM t WHERE (a) = 1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::BinaryOp { left, .. } => {
                // Left should be a column ref, not a row value constructor
                match *left {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
            let column = col_id.column_canonical();
                        assert_eq!(column, "a");
                    }
                    vibesql_ast::Expression::RowValueConstructor(_) => {
                        panic!("Single element should NOT be a RowValueConstructor");
                    }
                    _ => panic!("Expected ColumnRef for left"),
                }
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

// ============================================================================
// Pretty print tests
// ============================================================================

#[test]
fn test_row_value_to_sql() {
    use vibesql_ast::pretty_print::ToSql;

    let sql = "SELECT * FROM t WHERE (a, b) = (1, 2)";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        let where_expr = select.where_clause.unwrap();
        let output = where_expr.to_sql();
        // Should contain the row value constructors in the output
        assert!(output.contains("(") && output.contains(")"));
        assert!(output.contains(","));
    }
}
