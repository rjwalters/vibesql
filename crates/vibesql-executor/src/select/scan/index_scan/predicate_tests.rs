//! Tests for predicate extraction logic

use super::*;
use vibesql_ast::BinaryOperator;
use vibesql_types::SqlValue;

#[test]
fn test_extract_range_predicate_greater_than() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
        left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
        expr: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
        right: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
        left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(5))),
    };

    assert!(where_clause_fully_satisfied_by_index(&expr, "col0"));
}

#[test]
fn test_where_clause_fully_satisfied_between() {
    // col0 BETWEEN 10 AND 20
    let expr = Expression::Between {
        expr: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
        expr: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "col1".to_string() }),
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
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
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
                left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_id".to_string() }),
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
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
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
            left: Box::new(Expression::ColumnRef { table: None, column: "C_W_ID".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "C_D_ID".to_string() }),
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
            left: Box::new(Expression::ColumnRef { table: None, column: "department".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Engineering")))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice")))),
        }),
    };

    let columns = vec!["department", "name"];
    let result = extract_composite_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let key = result.unwrap();
    assert_eq!(key.len(), 2);
    assert_eq!(key[0], SqlValue::Varchar(arcstr::ArcStr::from("Engineering")));
    assert_eq!(key[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
}

#[test]
fn test_extract_composite_predicates_empty_columns() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef { table: None, column: "col".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    };

    let columns: Vec<&str> = vec![];
    let result = extract_composite_equality_predicates(&expr, &columns);

    // Should return None for empty column list
    assert!(result.is_none());
}

// ============================================================================
// Tests for prefix equality predicate extraction (Issue #3175)
// ============================================================================

#[test]
fn test_extract_prefix_predicates_partial_match() {
    // WHERE c_w_id = 1 AND c_d_id = 2 AND c_balance > 100
    // Index: [c_w_id, c_d_id, c_id]
    // Expected prefix: [1, 2] covering {c_w_id, c_d_id}
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_balance".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(100))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    let result = extract_prefix_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let prefix = result.unwrap();
    assert_eq!(prefix.prefix_key.len(), 2);
    assert_eq!(prefix.prefix_key[0], SqlValue::Integer(1));
    assert_eq!(prefix.prefix_key[1], SqlValue::Integer(2));
    assert!(prefix.covered_columns.contains("C_W_ID"));
    assert!(prefix.covered_columns.contains("C_D_ID"));
    assert!(!prefix.covered_columns.contains("C_ID"));
}

#[test]
fn test_extract_prefix_predicates_single_column() {
    // WHERE c_w_id = 1 AND c_balance > 100
    // Index: [c_w_id, c_d_id, c_id]
    // Expected prefix: [1] covering {c_w_id}
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_balance".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(100))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    let result = extract_prefix_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let prefix = result.unwrap();
    assert_eq!(prefix.prefix_key.len(), 1);
    assert_eq!(prefix.prefix_key[0], SqlValue::Integer(1));
    assert!(prefix.covered_columns.contains("C_W_ID"));
}

#[test]
fn test_extract_prefix_predicates_gap_in_columns() {
    // WHERE c_w_id = 1 AND c_id = 3 (missing c_d_id - gap in prefix)
    // Index: [c_w_id, c_d_id, c_id]
    // Expected prefix: [1] (stops at c_d_id gap)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    let result = extract_prefix_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let prefix = result.unwrap();
    assert_eq!(prefix.prefix_key.len(), 1); // Only c_w_id, stopped at gap
    assert_eq!(prefix.prefix_key[0], SqlValue::Integer(1));
}

#[test]
fn test_extract_prefix_predicates_no_first_column() {
    // WHERE c_d_id = 2 (missing first column c_w_id)
    // Index: [c_w_id, c_d_id, c_id]
    // Expected: None (can't use prefix without first column)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(2))),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    let result = extract_prefix_equality_predicates(&expr, &columns);

    assert!(result.is_none());
}

#[test]
fn test_extract_prefix_predicates_full_match() {
    // WHERE c_w_id = 1 AND c_d_id = 2 AND c_id = 3 (all columns)
    // Index: [c_w_id, c_d_id, c_id]
    // Expected prefix: [1, 2, 3] covering all
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    let result = extract_prefix_equality_predicates(&expr, &columns);

    assert!(result.is_some());
    let prefix = result.unwrap();
    assert_eq!(prefix.prefix_key.len(), 3);
}

// ============================================================================
// Tests for residual WHERE clause building (Issue #3175)
// ============================================================================

#[test]
fn test_build_residual_where_clause_partial_covered() {
    // WHERE c_w_id = 1 AND c_d_id = 2 AND c_balance > 100
    // Covered: {c_w_id, c_d_id}
    // Expected residual: c_balance > 100
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_balance".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(100))),
        }),
    };

    let mut covered = std::collections::HashSet::new();
    covered.insert("C_W_ID".to_string());
    covered.insert("C_D_ID".to_string());

    let residual = build_residual_where_clause(&expr, &covered);

    assert!(residual.is_some());
    // Verify the residual is just c_balance > 100
    match residual.unwrap() {
        Expression::BinaryOp { left, op: BinaryOperator::GreaterThan, right } => {
            match left.as_ref() {
                Expression::ColumnRef { column, .. } => {
                    assert_eq!(column, "c_balance");
                }
                _ => panic!("Expected column reference"),
            }
            match right.as_ref() {
                Expression::Literal(SqlValue::Integer(100)) => {}
                _ => panic!("Expected literal 100"),
            }
        }
        _ => panic!("Expected binary op"),
    }
}

#[test]
fn test_build_residual_where_clause_all_covered() {
    // WHERE c_w_id = 1 AND c_d_id = 2
    // Covered: {c_w_id, c_d_id}
    // Expected residual: None (all covered)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let mut covered = std::collections::HashSet::new();
    covered.insert("C_W_ID".to_string());
    covered.insert("C_D_ID".to_string());

    let residual = build_residual_where_clause(&expr, &covered);

    assert!(residual.is_none()); // All predicates covered
}

#[test]
fn test_build_residual_where_clause_none_covered() {
    // WHERE c_balance > 100 AND c_credit = 'BC'
    // Covered: {} (empty)
    // Expected residual: original expression
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_balance".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(100))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_credit".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("BC")))),
        }),
    };

    let covered = std::collections::HashSet::new(); // Empty

    let residual = build_residual_where_clause(&expr, &covered);

    assert!(residual.is_some()); // Nothing covered, full expression returned
}

#[test]
fn test_build_residual_where_clause_multiple_uncovered() {
    // WHERE c_w_id = 1 AND c_balance > 100 AND c_credit = 'BC'
    // Covered: {c_w_id}
    // Expected residual: c_balance > 100 AND c_credit = 'BC'
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "c_balance".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(100))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_credit".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("BC")))),
        }),
    };

    let mut covered = std::collections::HashSet::new();
    covered.insert("C_W_ID".to_string());

    let residual = build_residual_where_clause(&expr, &covered);

    assert!(residual.is_some());
    // Verify it's an AND of the two uncovered predicates
    match residual.unwrap() {
        Expression::BinaryOp { op: BinaryOperator::And, .. } => {}
        _ => panic!("Expected AND expression"),
    }
}
// Tests for composite IN predicates

#[test]
fn test_extract_composite_predicates_with_in_basic() {
    // WHERE c_w_id IN (1, 2, 3) AND c_d_id = 5
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::InList {
            expr: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(3)),
            ],
            negated: false,
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    let result = extract_composite_predicates_with_in(&expr, &columns);

    assert!(result.is_some());
    let predicates = result.unwrap();
    assert_eq!(predicates.len(), 2);

    // First predicate should be IN
    match &predicates[0] {
        CompositePredicateType::In(values) => {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], SqlValue::Integer(1));
            assert_eq!(values[1], SqlValue::Integer(2));
            assert_eq!(values[2], SqlValue::Integer(3));
        }
        _ => panic!("Expected IN predicate"),
    }

    // Second predicate should be Equality
    match &predicates[1] {
        CompositePredicateType::Equality(value) => {
            assert_eq!(*value, SqlValue::Integer(5));
        }
        _ => panic!("Expected Equality predicate"),
    }
}

#[test]
fn test_extract_composite_predicates_with_in_reversed() {
    // WHERE c_d_id = 5 AND c_w_id IN (1, 2)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
        right: Box::new(Expression::InList {
            expr: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Integer(2)),
            ],
            negated: false,
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    let result = extract_composite_predicates_with_in(&expr, &columns);

    assert!(result.is_some());
    let predicates = result.unwrap();
    assert_eq!(predicates.len(), 2);

    // First column (c_w_id) should have IN predicate
    match &predicates[0] {
        CompositePredicateType::In(values) => {
            assert_eq!(values.len(), 2);
        }
        _ => panic!("Expected IN predicate for c_w_id"),
    }

    // Second column (c_d_id) should have Equality
    match &predicates[1] {
        CompositePredicateType::Equality(value) => {
            assert_eq!(*value, SqlValue::Integer(5));
        }
        _ => panic!("Expected Equality predicate for c_d_id"),
    }
}

#[test]
fn test_generate_composite_keys_all_equality() {
    // All equality predicates: [Eq(1), Eq(2), Eq(3)]
    let predicates = vec![
        CompositePredicateType::Equality(SqlValue::Integer(1)),
        CompositePredicateType::Equality(SqlValue::Integer(2)),
        CompositePredicateType::Equality(SqlValue::Integer(3)),
    ];

    let keys = generate_composite_keys(&predicates);

    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)]);
}

#[test]
fn test_generate_composite_keys_single_in() {
    // IN on first column: [In([1, 2, 3]), Eq(5)]
    let predicates = vec![
        CompositePredicateType::In(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
        ]),
        CompositePredicateType::Equality(SqlValue::Integer(5)),
    ];

    let keys = generate_composite_keys(&predicates);

    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], vec![SqlValue::Integer(1), SqlValue::Integer(5)]);
    assert_eq!(keys[1], vec![SqlValue::Integer(2), SqlValue::Integer(5)]);
    assert_eq!(keys[2], vec![SqlValue::Integer(3), SqlValue::Integer(5)]);
}

#[test]
fn test_generate_composite_keys_in_on_second_column() {
    // IN on second column: [Eq(1), In([2, 3])]
    let predicates = vec![
        CompositePredicateType::Equality(SqlValue::Integer(1)),
        CompositePredicateType::In(vec![SqlValue::Integer(2), SqlValue::Integer(3)]),
    ];

    let keys = generate_composite_keys(&predicates);

    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
    assert_eq!(keys[1], vec![SqlValue::Integer(1), SqlValue::Integer(3)]);
}

#[test]
fn test_generate_composite_keys_multiple_in() {
    // Multiple IN predicates: [In([1, 2]), In([3, 4])]
    let predicates = vec![
        CompositePredicateType::In(vec![SqlValue::Integer(1), SqlValue::Integer(2)]),
        CompositePredicateType::In(vec![SqlValue::Integer(3), SqlValue::Integer(4)]),
    ];

    let keys = generate_composite_keys(&predicates);

    // Should produce cartesian product: 2 * 2 = 4 keys
    assert_eq!(keys.len(), 4);
    assert_eq!(keys[0], vec![SqlValue::Integer(1), SqlValue::Integer(3)]);
    assert_eq!(keys[1], vec![SqlValue::Integer(1), SqlValue::Integer(4)]);
    assert_eq!(keys[2], vec![SqlValue::Integer(2), SqlValue::Integer(3)]);
    assert_eq!(keys[3], vec![SqlValue::Integer(2), SqlValue::Integer(4)]);
}

#[test]
fn test_where_clause_satisfied_by_composite_key_equality_only() {
    // WHERE c_w_id = 1 AND c_d_id = 2
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    assert!(where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_where_clause_satisfied_by_composite_key_with_in() {
    // WHERE c_w_id IN (1, 2, 3) AND c_d_id = 5
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::InList {
            expr: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(3)),
            ],
            negated: false,
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    assert!(where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_where_clause_not_satisfied_extra_predicate() {
    // WHERE c_w_id = 1 AND c_d_id = 2 AND extra_col = 3
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "extra_col".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    // Should return false because extra_col is not in the index
    assert!(!where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_where_clause_not_satisfied_negated_in() {
    // WHERE c_w_id NOT IN (1, 2, 3) AND c_d_id = 5
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::InList {
            expr: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(3)),
            ],
            negated: true, // NOT IN
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    // Should return false because NOT IN cannot be optimized
    assert!(!where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

// Additional tests from main branch for composite key satisfaction

#[test]
fn test_composite_key_satisfaction_full_match() {
    // WHERE c_w_id = 1 AND c_d_id = 2 AND c_id = 3 with index [c_w_id, c_d_id, c_id]
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    assert!(where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_composite_key_satisfaction_missing_predicate() {
    // WHERE c_w_id = 1 AND c_d_id = 2 with index [c_w_id, c_d_id, c_id]
    // Should NOT be satisfied because c_id predicate is missing
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id", "c_id"];
    assert!(!where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_composite_key_satisfaction_range_predicate() {
    // WHERE c_w_id = 1 AND c_d_id > 2 with index [c_w_id, c_d_id]
    // Should NOT be satisfied because c_d_id uses a range predicate, not equality
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    assert!(!where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_composite_key_satisfaction_or_predicate() {
    // WHERE c_w_id = 1 OR c_d_id = 2 with index [c_w_id, c_d_id]
    // Should NOT be satisfied because OR is not supported
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_w_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "c_d_id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    assert!(!where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_composite_key_satisfaction_single_column() {
    // WHERE c_id = 42 with index [c_id]
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef { table: None, column: "c_id".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };

    let columns = vec!["c_id"];
    assert!(where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_composite_key_satisfaction_case_insensitive() {
    // WHERE C_W_ID = 1 AND C_D_ID = 2 with index [c_w_id, c_d_id]
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "C_W_ID".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "C_D_ID".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let columns = vec!["c_w_id", "c_d_id"];
    assert!(where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}

#[test]
fn test_composite_key_satisfaction_null_value() {
    // WHERE c_id = NULL should NOT be satisfied (NULL comparisons need special handling)
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef { table: None, column: "c_id".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Null)),
    };

    let columns = vec!["c_id"];
    assert!(!where_clause_fully_satisfied_by_composite_key(&expr, &columns));
}
