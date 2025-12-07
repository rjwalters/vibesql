//! Test suite for WHERE clause predicate pushdown optimizer

use super::table_refs::flatten_conjuncts;
use super::*;

#[test]
fn test_flatten_conjuncts_single() {
    let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
    let conjuncts = flatten_conjuncts(&expr);
    assert_eq!(conjuncts.len(), 1);
}

#[test]
fn test_flatten_conjuncts_multiple() {
    // (a AND b) AND c should flatten to 3 conjuncts
    let a = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
    let b = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
    let ab = vibesql_ast::Expression::BinaryOp {
        op: vibesql_ast::BinaryOperator::And,
        left: Box::new(a),
        right: Box::new(b),
    };
    let c = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false));
    let abc = vibesql_ast::Expression::BinaryOp {
        op: vibesql_ast::BinaryOperator::And,
        left: Box::new(ab),
        right: Box::new(c),
    };

    let conjuncts = flatten_conjuncts(&abc);
    assert_eq!(conjuncts.len(), 3);
}

#[test]
fn test_decomposition_empty() {
    let decomp = PredicateDecomposition::empty();
    assert!(decomp.is_empty());
}

#[test]
fn test_rebuild_empty() {
    let decomp = PredicateDecomposition::empty();
    assert!(decomp.rebuild_where_clause().is_none());
}

#[test]
fn test_combine_with_and() {
    // Test empty list
    assert_eq!(combine_with_and(vec![]), None);

    // Test single expression
    let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
    assert_eq!(combine_with_and(vec![expr.clone()]), Some(expr));

    // Test multiple expressions
    let exprs = vec![
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true)),
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
    ];
    let result = combine_with_and(exprs);
    assert!(result.is_some());
}

#[test]
fn test_or_filter_extraction() {
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::SqlValue;

    // Create schema with two nation tables (n1, n2)
    let n1_schema = TableSchema::new(
        "n1".to_string(),
        vec![ColumnSchema::new(
            "n_name".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(25) },
            false,
        )],
    );
    let n2_schema = TableSchema::new(
        "n2".to_string(),
        vec![ColumnSchema::new(
            "n_name".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(25) },
            false,
        )],
    );
    // Build CombinedSchema properly
    let schema = CombinedSchema::combine(
        CombinedSchema::from_table("n1".to_string(), n1_schema),
        "n2".to_string(),
        n2_schema,
    );

    // Build Q7-style OR predicate:
    // (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
    let n1_france = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: Some("n1".to_string()),
            column: "n_name".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("FRANCE")))),
    };
    let n2_germany = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: Some("n2".to_string()),
            column: "n_name".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("GERMANY")))),
    };
    let n1_germany = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: Some("n1".to_string()),
            column: "n_name".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("GERMANY")))),
    };
    let n2_france = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: Some("n2".to_string()),
            column: "n_name".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("FRANCE")))),
    };

    // (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
    let left_branch = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(n1_france),
        right: Box::new(n2_germany),
    };

    // (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
    let right_branch = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(n1_germany),
        right: Box::new(n2_france),
    };

    // Full OR predicate
    let or_predicate = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(left_branch),
        right: Box::new(right_branch),
    };

    // Extract filters
    let filters = or_conditions::extract_table_filters_from_or(&or_predicate, &schema);
    assert!(filters.is_some(), "Should extract filters from OR predicate");

    let filters = filters.unwrap();
    assert_eq!(filters.len(), 2, "Should extract 2 table filters (n1 and n2)");

    // Check that both n1 and n2 have filters
    let table_names: HashSet<_> = filters.iter().map(|(t, _)| t.as_str()).collect();
    assert!(table_names.contains("n1"), "Should have filter for n1");
    assert!(table_names.contains("n2"), "Should have filter for n2");
}

#[test]
fn test_or_filter_extraction_multi_branch() {
    // Test case 1: Multi-branch OR predicates (more than 2 OR branches)
    // (A AND B) OR (C AND D) OR (E AND F)
    // Current implementation only handles binary OR, so nested ORs like:
    // ((A AND B) OR (C AND D)) OR (E AND F)
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::SqlValue;

    // Create schema with tables t1, t2, t3
    let t1_schema = TableSchema::new(
        "t1".to_string(),
        vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
    );
    let t2_schema = TableSchema::new(
        "t2".to_string(),
        vec![ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false)],
    );
    let t3_schema = TableSchema::new(
        "t3".to_string(),
        vec![ColumnSchema::new("c".to_string(), vibesql_types::DataType::Integer, false)],
    );

    let schema = CombinedSchema::combine(
        CombinedSchema::combine(
            CombinedSchema::from_table("t1".to_string(), t1_schema),
            "t2".to_string(),
            t2_schema,
        ),
        "t3".to_string(),
        t3_schema,
    );

    // Build: ((t1.a = 1 AND t2.b = 2) OR (t1.a = 3 AND t2.b = 4)) OR (t1.a = 5 AND t2.b = 6)
    let branch1 = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "b".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let branch2 = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "b".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(4))),
        }),
    };

    let branch3 = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "b".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(6))),
        }),
    };

    // Create nested OR: (branch1 OR branch2) OR branch3
    let inner_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(branch1),
        right: Box::new(branch2),
    };

    let outer_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(inner_or.clone()),
        right: Box::new(branch3),
    };

    // Extract filters from the inner OR first
    let inner_filters = or_conditions::extract_table_filters_from_or(&inner_or, &schema);
    assert!(inner_filters.is_some(), "Should extract filters from inner OR");
    let inner_filters = inner_filters.unwrap();
    assert_eq!(inner_filters.len(), 2, "Inner OR should extract 2 table filters (t1 and t2)");

    // Test outer OR: The left branch is an OR expression (which becomes a complex predicate)
    // The right branch is a simple AND expression
    // Because the function works on binary OR only and doesn't recursively expand,
    // the outer OR has one branch as a complex filter, limiting extraction
    let outer_filters = or_conditions::extract_table_filters_from_or(&outer_or, &schema);

    // The current implementation will see the inner_or as a single complex predicate
    // in the left branch, and branch3 as an AND in the right branch.
    // It can only extract filters for tables that appear in BOTH branches as simple predicates.
    // Since the left branch has no simple table predicates (it's an OR), no filters are extracted.
    assert!(outer_filters.is_none(),
        "Outer OR should return None because left branch is a complex OR predicate, not simple AND predicates");
}

#[test]
fn test_or_filter_extraction_nested_or() {
    // Test case 2: Nested OR predicates
    // ((A OR B) AND C) OR ((D OR E) AND F)
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::SqlValue;

    let t1_schema = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false),
        ],
    );

    let schema = CombinedSchema::from_table("t1".to_string(), t1_schema);

    // Build: ((t1.a = 1 OR t1.a = 2) AND t1.b = 10) OR ((t1.a = 3 OR t1.a = 4) AND t1.b = 20)
    let left_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let left_branch = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(left_or),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "b".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        }),
    };

    let right_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(4))),
        }),
    };

    let right_branch = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(right_or),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "b".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(20))),
        }),
    };

    let nested_predicate = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(left_branch),
        right: Box::new(right_branch),
    };

    // This tests how the function handles nested OR structures
    // The current implementation will see the inner ORs as single predicates
    let filters = or_conditions::extract_table_filters_from_or(&nested_predicate, &schema);

    // Should extract t1.b filter: (t1.b = 10) OR (t1.b = 20)
    // The nested OR predicates for t1.a are treated as complex predicates
    assert!(filters.is_some(), "Should extract some filters from nested OR");
    let filters = filters.unwrap();
    assert_eq!(filters.len(), 1, "Should extract filter for t1.b");
    assert_eq!(filters[0].0, "t1", "Filter should be for table t1");
}

#[test]
fn test_or_filter_extraction_asymmetric() {
    // Test case 3: Asymmetric OR predicates - tables appear in only one branch
    // (t1.a = 1 AND t2.b = 2) OR (t1.a = 3)
    // Should only extract filter for t1 (appears in both branches), not t2
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::SqlValue;

    let t1_schema = TableSchema::new(
        "t1".to_string(),
        vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
    );
    let t2_schema = TableSchema::new(
        "t2".to_string(),
        vec![ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false)],
    );

    let schema = CombinedSchema::combine(
        CombinedSchema::from_table("t1".to_string(), t1_schema),
        "t2".to_string(),
        t2_schema,
    );

    // Left branch: t1.a = 1 AND t2.b = 2
    let left_branch = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "b".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    // Right branch: t1.a = 3 (only t1, no t2)
    let right_branch = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: Some("t1".to_string()),
            column: "a".to_string(),
        }),
        right: Box::new(Expression::Literal(SqlValue::Integer(3))),
    };

    let asymmetric_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(left_branch),
        right: Box::new(right_branch),
    };

    let filters = or_conditions::extract_table_filters_from_or(&asymmetric_or, &schema);
    assert!(filters.is_some(), "Should extract filters from asymmetric OR");

    let filters = filters.unwrap();
    assert_eq!(filters.len(), 1, "Should extract only 1 table filter (for t1)");
    assert_eq!(filters[0].0, "t1", "Filter should be for table t1");

    // t2 should NOT be in the filters since it doesn't appear in the right branch
    let table_names: HashSet<_> = filters.iter().map(|(t, _)| t.as_str()).collect();
    assert!(!table_names.contains("t2"), "Should NOT have filter for t2");
}

#[test]
fn test_or_filter_extraction_single_table() {
    // Test case 4: Single-table OR - Extracts filter successfully
    // t1.a = 1 OR t1.a = 2
    // This is a valid case that extracts a simple OR filter for one table
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::SqlValue;

    let t1_schema = TableSchema::new(
        "t1".to_string(),
        vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
    );

    let schema = CombinedSchema::from_table("t1".to_string(), t1_schema);

    // Build: t1.a = 1 OR t1.a = 2
    let single_table_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let filters = or_conditions::extract_table_filters_from_or(&single_table_or, &schema);

    // Extracts filter: (t1.a = 1) OR (t1.a = 2)
    // This is valid and useful - a simple OR filter for one table
    assert!(filters.is_some(), "Should extract filter from single-table OR");
    let filters = filters.unwrap();
    assert_eq!(filters.len(), 1, "Should extract 1 table filter");
    assert_eq!(filters[0].0, "t1", "Filter should be for table t1");
}

#[test]
fn test_or_filter_extraction_no_common_tables() {
    // Test case 5: No common tables - Should return None
    // (t1.a = 1 AND t2.b = 2) OR (t3.c = 3 AND t4.d = 4)
    // No tables appear in both branches
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::SqlValue;

    let t1_schema = TableSchema::new(
        "t1".to_string(),
        vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
    );
    let t2_schema = TableSchema::new(
        "t2".to_string(),
        vec![ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false)],
    );
    let t3_schema = TableSchema::new(
        "t3".to_string(),
        vec![ColumnSchema::new("c".to_string(), vibesql_types::DataType::Integer, false)],
    );
    let t4_schema = TableSchema::new(
        "t4".to_string(),
        vec![ColumnSchema::new("d".to_string(), vibesql_types::DataType::Integer, false)],
    );

    let schema = CombinedSchema::combine(
        CombinedSchema::combine(
            CombinedSchema::combine(
                CombinedSchema::from_table("t1".to_string(), t1_schema),
                "t2".to_string(),
                t2_schema,
            ),
            "t3".to_string(),
            t3_schema,
        ),
        "t4".to_string(),
        t4_schema,
    );

    // Left branch: t1.a = 1 AND t2.b = 2
    let left_branch = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "b".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    // Right branch: t3.c = 3 AND t4.d = 4
    let right_branch = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t3".to_string()),
                column: "c".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t4".to_string()),
                column: "d".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(4))),
        }),
    };

    let no_common_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(left_branch),
        right: Box::new(right_branch),
    };

    let filters = or_conditions::extract_table_filters_from_or(&no_common_or, &schema);
    assert!(filters.is_none(), "Should return None when no tables appear in both branches");
}

#[test]
fn test_or_filter_extraction_empty_branches() {
    // Test case 6: Empty branches - Should handle gracefully
    // TRUE OR FALSE
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::SqlValue;

    let t1_schema = TableSchema::new(
        "t1".to_string(),
        vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
    );

    let schema = CombinedSchema::from_table("t1".to_string(), t1_schema);

    // Build: TRUE OR FALSE (no table references)
    let empty_or = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
    };

    let filters = or_conditions::extract_table_filters_from_or(&empty_or, &schema);
    assert!(filters.is_none(), "Should return None for empty branches with no table references");
}
