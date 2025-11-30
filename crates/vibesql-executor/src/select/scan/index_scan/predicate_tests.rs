//! Tests for predicate extraction logic

use super::*;
use vibesql_ast::BinaryOperator;
use vibesql_types::SqlValue;

#[test]
fn test_extract_range_predicate_greater_than() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(Expression::ColumnRef {
            table: None,
            column: "col0".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Integer(60))),
    };

    let range = extract_range_predicate(&expr, "col0").unwrap();
    assert_eq!(range.start, Some(SqlValue::Integer(60)));
    assert_eq!(range.end, None);
    assert!(!range.inclusive_start);
}

#[test]
fn test_extract_range_predicate_less_than_or_equal() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::LessThanOrEqual,
        left: Box::new(Expression::ColumnRef {
            table: None,
            column: "col0".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Integer(43))),
    };

    let range = extract_range_predicate(&expr, "col0").unwrap();
    assert_eq!(range.start, None);
    assert_eq!(range.end, Some(SqlValue::Integer(43)));
    assert!(range.inclusive_end);
}

#[test]
fn test_extract_range_predicate_between() {
    let expr = Expression::Between {
        expr: Box::new(Expression::ColumnRef {
            table: None,
            column: "col0".to_string(),
        }),
        low: Box::new(Expression::Literal(SqlValue::Integer(10))),
        high: Box::new(Expression::Literal(SqlValue::Integer(20))),
        negated: false,
        symmetric: false,
    };

    let range = extract_range_predicate(&expr, "col0").unwrap();
    assert_eq!(range.start, Some(SqlValue::Integer(10)));
    assert_eq!(range.end, Some(SqlValue::Integer(20)));
    assert!(range.inclusive_start);
    assert!(range.inclusive_end);
}

#[test]
fn test_extract_range_predicate_combined_and() {
    // col0 > 10 AND col0 < 20
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(20))),
        }),
    };

    let range = extract_range_predicate(&expr, "col0").unwrap();
    assert_eq!(range.start, Some(SqlValue::Integer(10)));
    assert_eq!(range.end, Some(SqlValue::Integer(20)));
    assert!(!range.inclusive_start);
    assert!(!range.inclusive_end);
}

#[test]
fn test_extract_range_predicate_flipped_comparison() {
    // 60 < col0 (same as col0 > 60)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::LessThan,
        left: Box::new(Expression::Literal(SqlValue::Integer(60))),
        right: Box::new(Expression::ColumnRef {
            table: None,
            column: "col0".to_string(),
        }),
    };

    let range = extract_range_predicate(&expr, "col0").unwrap();
    assert_eq!(range.start, Some(SqlValue::Integer(60)));
    assert_eq!(range.end, None);
    assert!(!range.inclusive_start);
}

#[test]
fn test_where_clause_fully_satisfied_simple_equal() {
    // col0 = 5
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: None,
            column: "col0".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Integer(5))),
    };

    assert!(where_clause_fully_satisfied_by_index(&expr, "col0"));
}

#[test]
fn test_where_clause_fully_satisfied_between() {
    // col0 BETWEEN 10 AND 20
    let expr = Expression::Between {
        expr: Box::new(Expression::ColumnRef {
            table: None,
            column: "col0".to_string(),
        }),
        low: Box::new(Expression::Literal(SqlValue::Integer(10))),
        high: Box::new(Expression::Literal(SqlValue::Integer(20))),
        negated: false,
        symmetric: false,
    };

    assert!(where_clause_fully_satisfied_by_index(&expr, "col0"));
}

#[test]
fn test_where_clause_fully_satisfied_in_list() {
    // col0 IN (1, 2, 3)
    let expr = Expression::InList {
        expr: Box::new(Expression::ColumnRef {
            table: None,
            column: "col0".to_string(),
        }),
        values: vec![
            Expression::Literal(SqlValue::Integer(1)),
            Expression::Literal(SqlValue::Integer(2)),
            Expression::Literal(SqlValue::Integer(3)),
        ],
        negated: false,
    };

    assert!(where_clause_fully_satisfied_by_index(&expr, "col0"));
}

#[test]
fn test_where_clause_fully_satisfied_combined_range() {
    // col0 > 10 AND col0 < 20
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(20))),
        }),
    };

    assert!(where_clause_fully_satisfied_by_index(&expr, "col0"));
}

#[test]
fn test_where_clause_not_fully_satisfied_multiple_columns() {
    // col0 = 5 AND col1 = 10 (involves non-indexed column)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col1".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        }),
    };

    assert!(!where_clause_fully_satisfied_by_index(&expr, "col0"));
}

#[test]
fn test_where_clause_not_fully_satisfied_or() {
    // col0 = 5 OR col0 = 10 (OR not optimized for index pushdown)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        }),
    };

    assert!(!where_clause_fully_satisfied_by_index(&expr, "col0"));
}

// Tests for composite index predicate extraction

#[test]
fn test_extract_composite_predicates_full_match() {
    // WHERE c_w_id = 1 AND c_d_id = 2 AND c_id = 3
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "c_w_id".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "c_d_id".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "c_id".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    let result = extract_composite_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let key = result.unwrap();
    assert_eq!(key.len(), 3);
    assert_eq!(key[0], SqlValue::Integer(1));
    assert_eq!(key[1], SqlValue::Integer(2));
    assert_eq!(key[2], SqlValue::Integer(3));
}

#[test]
fn test_extract_composite_predicates_partial_match() {
    // WHERE c_w_id = 1 AND c_d_id = 2 (missing c_id predicate)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "c_w_id".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "c_d_id".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    let result = extract_composite_equality_predicates(&expr, &columns);

    // Should return None since c_id predicate is missing
    assert!(result.is_none());
}

#[test]
fn test_extract_composite_predicates_case_insensitive() {
    // WHERE C_W_ID = 1 AND C_D_ID = 2 (uppercase in query)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "C_W_ID".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "C_D_ID".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"]; // lowercase index columns
    let result = extract_composite_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let key = result.unwrap();
    assert_eq!(key.len(), 2);
    assert_eq!(key[0], SqlValue::Integer(1));
    assert_eq!(key[1], SqlValue::Integer(2));
}

#[test]
fn test_extract_composite_predicates_with_string_values() {
    // WHERE department = 'Engineering' AND name = 'Alice'
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "department".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Varchar("Engineering".to_string()))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Varchar("Alice".to_string()))),
        }),
    };

    let columns = vec!["department", "name"];
    let result = extract_composite_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let key = result.unwrap();
    assert_eq!(key.len(), 2);
    assert_eq!(key[0], SqlValue::Varchar("Engineering".to_string()));
    assert_eq!(key[1], SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_extract_composite_predicates_empty_columns() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: None,
            column: "col".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    };

    let columns: Vec<&str> = vec![];
    let result = extract_composite_equality_predicates(&expr, &columns);

    // Should return None for empty column list
    assert!(result.is_none());
}
