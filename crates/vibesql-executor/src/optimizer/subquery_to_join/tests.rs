//! Tests for subquery-to-join transformations

use vibesql_ast::{BinaryOperator, GroupByClause, JoinType, SelectItem};

use super::*;

fn simple_table_from(name: &str) -> FromClause {
    FromClause::Table { name: name.to_string(), alias: None, column_aliases: None }
}

fn column_ref(column: &str) -> Expression {
    Expression::ColumnRef { table: None, column: column.to_string() }
}

fn simple_select(table: &str, column: &str) -> SelectStmt {
    SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Expression { expr: column_ref(column), alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(simple_table_from(table)),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    }
}

#[test]
fn test_in_subquery_to_semi_join() {
    let mut stmt = simple_select("orders", "o_orderkey");
    let subquery = simple_select("lineitem", "l_orderkey");

    stmt.where_clause = Some(Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery),
        negated: false,
    });

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created a SEMI JOIN
    assert!(transformed.where_clause.is_none(), "WHERE clause should be removed");
    match transformed.from {
        Some(FromClause::Join { join_type, .. }) => {
            assert!(matches!(join_type, JoinType::Semi), "Should be SEMI join");
        }
        _ => panic!("Expected JOIN in FROM clause"),
    }
}

#[test]
fn test_not_in_subquery_to_anti_join() {
    let mut stmt = simple_select("orders", "o_orderkey");
    let subquery = simple_select("lineitem", "l_orderkey");

    stmt.where_clause = Some(Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery),
        negated: true,
    });

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created an ANTI JOIN
    assert!(transformed.where_clause.is_none(), "WHERE clause should be removed");
    match transformed.from {
        Some(FromClause::Join { join_type, .. }) => {
            assert!(matches!(join_type, JoinType::Anti), "Should be ANTI join");
        }
        _ => panic!("Expected JOIN in FROM clause"),
    }
}

#[test]
fn test_complex_subquery_unchanged() {
    let mut stmt = simple_select("orders", "o_orderkey");
    let mut subquery = simple_select("lineitem", "l_orderkey");
    // Add LIMIT to make it complex (LIMIT subqueries can't be safely transformed)
    subquery.limit = Some(10);

    stmt.where_clause = Some(Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery),
        negated: false,
    });

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should be unchanged because subquery has LIMIT
    assert!(
        transformed.where_clause.is_some(),
        "Complex subquery with LIMIT should remain in WHERE"
    );
    match transformed.from {
        Some(FromClause::Table { .. }) => {} // Good, no join created
        _ => panic!("Complex subquery with LIMIT should not create JOIN"),
    }
}

#[test]
fn test_aggregate_subquery_transforms_to_derived_table() {
    // Test TPC-H Q18-like pattern: IN subquery with GROUP BY/HAVING
    let mut stmt = simple_select("orders", "o_orderkey");
    let mut subquery = simple_select("lineitem", "l_orderkey");
    // Add GROUP BY and HAVING - this should now be transformed using derived table
    subquery.group_by = Some(GroupByClause::Simple(vec![column_ref("l_orderkey")]));
    subquery.having = Some(Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![column_ref("l_quantity")],
        }),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(300))),
    });

    stmt.where_clause = Some(Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery),
        negated: false,
    });

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should be transformed: WHERE clause removed (IN -> semi-join)
    assert!(transformed.where_clause.is_none(), "Aggregate IN subquery should be transformed");

    // Check that we got a SEMI JOIN with a derived table (Subquery)
    match transformed.from {
        Some(FromClause::Join { join_type, right, .. }) => {
            assert!(matches!(join_type, JoinType::Semi), "Should be SEMI join");
            // Right side should be a Subquery (derived table)
            match right.as_ref() {
                FromClause::Subquery { alias, .. } => {
                    assert!(alias.starts_with("__in_agg_"), "Should have __in_agg_ alias");
                }
                _ => panic!("Expected Subquery (derived table) on right side of JOIN"),
            }
        }
        _ => panic!("Expected SEMI JOIN in FROM clause"),
    }
}

#[test]
fn test_multiple_subqueries_to_joins() {
    // Test Q21-like pattern with multiple IN subqueries in a deep AND chain
    // WHERE a = b AND c = d AND x IN (...) AND y NOT IN (...)
    let mut stmt = simple_select("orders", "o_orderkey");
    let subquery1 = simple_select("lineitem", "l_orderkey");
    let subquery2 = simple_select("supplier", "s_suppkey");

    // Build WHERE: o_custkey = 1 AND o_orderkey IN (subquery1) AND o_custkey NOT IN (subquery2)
    let predicate1 = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(column_ref("o_custkey")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
    };

    let in_subquery = Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery1),
        negated: false,
    };

    let not_in_subquery = Expression::In {
        expr: Box::new(column_ref("o_custkey")),
        subquery: Box::new(subquery2),
        negated: true,
    };

    // Build: predicate1 AND in_subquery AND not_in_subquery
    let combined_where = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(predicate1),
            right: Box::new(in_subquery),
        }),
        right: Box::new(not_in_subquery),
    };

    stmt.where_clause = Some(combined_where);

    // Transform should extract BOTH subqueries iteratively
    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have two joins (SEMI and ANTI)
    match transformed.from {
        Some(FromClause::Join { left: inner_join_box, join_type: outer_join_type, .. }) => {
            // Outer join should be either SEMI or ANTI
            assert!(
                matches!(outer_join_type, JoinType::Semi | JoinType::Anti),
                "Outer join should be SEMI or ANTI, got: {:?}",
                outer_join_type
            );

            // Inner join should also be a join (not just a table)
            match *inner_join_box {
                FromClause::Join { join_type: inner_join_type, .. } => {
                    assert!(
                        matches!(inner_join_type, JoinType::Semi | JoinType::Anti),
                        "Inner join should be SEMI or ANTI, got: {:?}",
                        inner_join_type
                    );
                }
                _ => panic!("Expected nested JOIN, got table"),
            }
        }
        _ => panic!("Expected JOIN in FROM clause"),
    }

    // WHERE should only have the simple predicate left
    assert!(transformed.where_clause.is_some(), "Simple predicate should remain in WHERE");
}

#[test]
fn test_nested_in_subquery_self_join_column_qualification() {
    // Test that nested IN subqueries in self-joins properly qualify the outer expression
    // This is the bug from issue #2630:
    // SELECT pk FROM tab0 WHERE col3 IN (SELECT col0 FROM tab0 WHERE col0 IN (...) AND col4 >=
    // 7680.91)
    //
    // When the outer IN is transformed to a SEMI JOIN, the nested IN's outer column (col0)
    // should be qualified with the subquery alias (__subquery_TAB0), not left unqualified.

    // Create a nested IN subquery pattern
    let innermost_subquery = simple_select("tab0", "col3"); // SELECT col3 FROM tab0

    // Middle subquery: SELECT col0 FROM tab0 WHERE col0 IN (innermost) AND col4 >= 7680
    let mut middle_subquery = simple_select("tab0", "col0");
    middle_subquery.where_clause = Some(Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::In {
            expr: Box::new(column_ref("col0")), // This should get qualified!
            subquery: Box::new(innermost_subquery),
            negated: false,
        }),
        right: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThanOrEqual,
            left: Box::new(column_ref("col4")),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(7680))),
        }),
    });

    // Outer query: SELECT pk FROM tab0 WHERE col3 IN (middle_subquery)
    let mut stmt = simple_select("tab0", "pk");
    stmt.where_clause = Some(Expression::In {
        expr: Box::new(column_ref("col3")),
        subquery: Box::new(middle_subquery),
        negated: false,
    });

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have transformed to a SEMI JOIN
    match &transformed.from {
        Some(FromClause::Join { join_type, condition, right, .. }) => {
            assert!(matches!(join_type, JoinType::Semi), "Should be SEMI join");

            // Check that the right side has the alias
            match right.as_ref() {
                FromClause::Table { alias: Some(alias), .. } => {
                    assert!(alias.starts_with("__subquery_"), "Should have subquery alias");
                }
                _ => panic!("Expected aliased table on right side"),
            }

            // Check the join condition - nested IN's outer column should be qualified
            if let Some(cond) = condition {
                // The condition should contain a nested IN expression
                // with a qualified column reference for col0
                fn check_nested_in_qualification(expr: &Expression) -> bool {
                    match expr {
                        Expression::In { expr: inner_expr, .. } => {
                            // The outer expression of the nested IN should be qualified
                            match inner_expr.as_ref() {
                                Expression::ColumnRef { table: Some(t), column: c } => {
                                    t.starts_with("__subquery_") && c.eq_ignore_ascii_case("col0")
                                }
                                _ => false,
                            }
                        }
                        Expression::BinaryOp { left, right, .. } => {
                            check_nested_in_qualification(left)
                                || check_nested_in_qualification(right)
                        }
                        _ => false,
                    }
                }

                assert!(
                    check_nested_in_qualification(cond),
                    "Nested IN subquery's outer column should be qualified with subquery alias. Condition: {:?}",
                    cond
                );
            }
        }
        _ => panic!("Expected SEMI JOIN in FROM clause"),
    }
}

fn table_from_with_alias(name: &str, alias: &str) -> FromClause {
    FromClause::Table {
        name: name.to_string(),
        alias: Some(alias.to_string()),
        column_aliases: None,
    }
}

fn qualified_column_ref(table: &str, column: &str) -> Expression {
    Expression::ColumnRef { table: Some(table.to_string()), column: column.to_string() }
}

#[test]
fn test_exists_self_join_column_qualification() {
    // Test EXISTS with self-join aliasing, similar to TPC-H Q21 pattern:
    // SELECT * FROM lineitem l1 WHERE EXISTS (
    //   SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <>
    // l1.l_suppkey )
    //
    // The EXISTS subquery references the same table with a different alias.
    // After transformation to a SEMI join, the join condition should properly
    // reference both the outer alias (l1) and the subquery's alias.

    // Create outer query: SELECT * FROM lineitem l1
    let outer_from = table_from_with_alias("lineitem", "l1");
    let mut stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(outer_from.clone()),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    // Create correlated EXISTS subquery:
    // EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <>
    // l1.l_suppkey)
    let exists_subquery = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(table_from_with_alias("lineitem", "l2")),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(qualified_column_ref("l2", "l_orderkey")),
                right: Box::new(qualified_column_ref("l1", "l_orderkey")),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::NotEqual,
                left: Box::new(qualified_column_ref("l2", "l_suppkey")),
                right: Box::new(qualified_column_ref("l1", "l_suppkey")),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    stmt.where_clause =
        Some(Expression::Exists { subquery: Box::new(exists_subquery), negated: false });

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created a SEMI JOIN
    assert!(
        transformed.where_clause.is_none(),
        "EXISTS should be fully transformed, no WHERE clause should remain"
    );

    match &transformed.from {
        Some(FromClause::Join { join_type, condition, right, .. }) => {
            assert!(
                matches!(join_type, JoinType::Semi),
                "EXISTS should transform to SEMI join, got: {:?}",
                join_type
            );

            // Check that the right side has the rewritten alias for self-join
            // Self-joins get a unique alias like "__subquery_l2" to avoid conflicts
            match right.as_ref() {
                FromClause::Table { name, alias, .. } => {
                    assert_eq!(name, "lineitem", "Table name should be lineitem");
                    assert_eq!(
                        alias.as_deref(),
                        Some("__subquery_l2"),
                        "Self-join alias should be rewritten to __subquery_l2"
                    );
                }
                _ => panic!("Expected Table on right side of join"),
            }

            // Verify the join condition includes the correlation predicate
            // The condition should have column refs rewritten to use __subquery_l2
            assert!(condition.is_some(), "Join should have a condition");

            if let Some(cond) = condition {
                fn contains_rewritten_alias(expr: &Expression) -> bool {
                    match expr {
                        Expression::ColumnRef { table: Some(t), .. } => t == "__subquery_l2",
                        Expression::BinaryOp { left, right, .. } => {
                            contains_rewritten_alias(left) || contains_rewritten_alias(right)
                        }
                        _ => false,
                    }
                }

                assert!(
                    contains_rewritten_alias(cond),
                    "Join condition should have column refs rewritten to __subquery_l2. Condition: {:?}",
                    cond
                );
            }
        }
        _ => panic!("Expected SEMI JOIN in FROM clause"),
    }
}

#[test]
fn test_not_exists_self_join_column_qualification() {
    // Test NOT EXISTS with self-join aliasing, similar to TPC-H Q21 pattern:
    // SELECT * FROM lineitem l1 WHERE NOT EXISTS (
    //   SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_receiptdate >
    // l3.l_commitdate )
    //
    // NOT EXISTS should transform to an ANTI join with proper alias handling.

    // Create outer query: SELECT * FROM lineitem l1
    let outer_from = table_from_with_alias("lineitem", "l1");
    let mut stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(outer_from.clone()),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    // Create correlated NOT EXISTS subquery:
    // NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND
    // l3.l_receiptdate > l3.l_commitdate)
    let not_exists_subquery = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(table_from_with_alias("lineitem", "l3")),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(qualified_column_ref("l3", "l_orderkey")),
                right: Box::new(qualified_column_ref("l1", "l_orderkey")),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(qualified_column_ref("l3", "l_receiptdate")),
                right: Box::new(qualified_column_ref("l3", "l_commitdate")),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    stmt.where_clause = Some(Expression::Exists {
        subquery: Box::new(not_exists_subquery),
        negated: true, // NOT EXISTS
    });

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created an ANTI JOIN
    assert!(
        transformed.where_clause.is_none(),
        "NOT EXISTS should be fully transformed, no WHERE clause should remain"
    );

    match &transformed.from {
        Some(FromClause::Join { join_type, condition, right, .. }) => {
            assert!(
                matches!(join_type, JoinType::Anti),
                "NOT EXISTS should transform to ANTI join, got: {:?}",
                join_type
            );

            // Check that the right side has the rewritten alias for self-join
            // Self-joins get a unique alias like "__subquery_l3" to avoid conflicts
            match right.as_ref() {
                FromClause::Table { name, alias, .. } => {
                    assert_eq!(name, "lineitem", "Table name should be lineitem");
                    assert_eq!(
                        alias.as_deref(),
                        Some("__subquery_l3"),
                        "Self-join alias should be rewritten to __subquery_l3"
                    );
                }
                _ => panic!("Expected Table on right side of join"),
            }

            // Verify the join condition includes the correlation predicate
            // The condition should have column refs rewritten to use __subquery_l3
            assert!(condition.is_some(), "Join should have a condition");

            // Verify the condition contains rewritten column references
            if let Some(cond) = condition {
                fn contains_rewritten_l3_ref(expr: &Expression) -> bool {
                    match expr {
                        Expression::ColumnRef { table: Some(t), .. } => t == "__subquery_l3",
                        Expression::BinaryOp { left, right, .. } => {
                            contains_rewritten_l3_ref(left) || contains_rewritten_l3_ref(right)
                        }
                        _ => false,
                    }
                }

                assert!(
                    contains_rewritten_l3_ref(cond),
                    "Join condition should have column refs rewritten to __subquery_l3. Condition: {:?}",
                    cond
                );
            }
        }
        _ => panic!("Expected ANTI JOIN in FROM clause"),
    }
}

// =============================================================================
// Tests for Expression::Conjunction handling (arena parser output)
// =============================================================================
// The arena parser produces Expression::Conjunction for AND chains instead of
// nested BinaryOp::And. These tests verify the optimizer handles both forms.

#[test]
fn test_conjunction_exists_to_semi_join() {
    // Test EXISTS inside a Conjunction (arena parser output for TPC-H Q4 pattern)
    // WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01' AND EXISTS (...)
    let outer_from = simple_table_from("orders");
    let mut stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(outer_from),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    // Create correlated EXISTS subquery
    let exists_subquery = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(simple_table_from("lineitem")),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(column_ref("l_orderkey")),
            right: Box::new(column_ref("o_orderkey")),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    // Build Conjunction: [predicate1, predicate2, EXISTS(...)]
    // This is what the arena parser produces for: pred1 AND pred2 AND EXISTS(...)
    let predicate1 = Expression::BinaryOp {
        op: BinaryOperator::GreaterThanOrEqual,
        left: Box::new(column_ref("o_orderdate")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Varchar("1993-07-01".into()))),
    };
    let predicate2 = Expression::BinaryOp {
        op: BinaryOperator::LessThan,
        left: Box::new(column_ref("o_orderdate")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Varchar("1993-10-01".into()))),
    };
    let exists_expr = Expression::Exists { subquery: Box::new(exists_subquery), negated: false };

    stmt.where_clause = Some(Expression::Conjunction(vec![predicate1, predicate2, exists_expr]));

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created a SEMI JOIN
    match &transformed.from {
        Some(FromClause::Join { join_type, .. }) => {
            assert!(
                matches!(join_type, JoinType::Semi),
                "EXISTS in Conjunction should transform to SEMI join, got: {:?}",
                join_type
            );
        }
        _ => panic!("Expected SEMI JOIN in FROM clause"),
    }

    // Remaining WHERE should be a Conjunction with the two predicates
    assert!(transformed.where_clause.is_some(), "Other predicates should remain in WHERE clause");
    match &transformed.where_clause {
        Some(Expression::Conjunction(children)) => {
            assert_eq!(children.len(), 2, "Should have 2 remaining predicates");
        }
        Some(Expression::BinaryOp { .. }) => {
            // Also acceptable if there's only one predicate left after further transformations
        }
        other => panic!("Expected Conjunction or BinaryOp in WHERE, got: {:?}", other),
    }
}

#[test]
fn test_conjunction_not_exists_to_anti_join() {
    // Test NOT EXISTS inside a Conjunction
    let outer_from = simple_table_from("orders");
    let mut stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(outer_from),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    // Create correlated NOT EXISTS subquery
    let not_exists_subquery = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(simple_table_from("lineitem")),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(column_ref("l_orderkey")),
            right: Box::new(column_ref("o_orderkey")),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
            values: None,
    };

    // Build Conjunction with NOT EXISTS
    let predicate = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(column_ref("o_orderstatus")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Varchar("F".into()))),
    };
    let not_exists_expr =
        Expression::Exists { subquery: Box::new(not_exists_subquery), negated: true };

    stmt.where_clause = Some(Expression::Conjunction(vec![predicate, not_exists_expr]));

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created an ANTI JOIN
    match &transformed.from {
        Some(FromClause::Join { join_type, .. }) => {
            assert!(
                matches!(join_type, JoinType::Anti),
                "NOT EXISTS in Conjunction should transform to ANTI join, got: {:?}",
                join_type
            );
        }
        _ => panic!("Expected ANTI JOIN in FROM clause"),
    }

    // Remaining WHERE should have the single predicate (not a Conjunction anymore)
    assert!(transformed.where_clause.is_some(), "Other predicate should remain in WHERE clause");
}

#[test]
fn test_conjunction_in_to_semi_join() {
    // Test IN subquery inside a Conjunction
    let mut stmt = simple_select("orders", "o_orderkey");
    let subquery = simple_select("lineitem", "l_orderkey");

    // Build Conjunction: [predicate, IN(...)]
    let predicate = Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(column_ref("o_totalprice")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1000))),
    };
    let in_expr = Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery),
        negated: false,
    };

    stmt.where_clause = Some(Expression::Conjunction(vec![predicate, in_expr]));

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created a SEMI JOIN
    match &transformed.from {
        Some(FromClause::Join { join_type, .. }) => {
            assert!(
                matches!(join_type, JoinType::Semi),
                "IN in Conjunction should transform to SEMI join, got: {:?}",
                join_type
            );
        }
        _ => panic!("Expected SEMI JOIN in FROM clause"),
    }

    // Remaining WHERE should have just the predicate
    assert!(transformed.where_clause.is_some(), "Other predicate should remain in WHERE clause");
}

#[test]
fn test_conjunction_not_in_to_anti_join() {
    // Test NOT IN subquery inside a Conjunction
    let mut stmt = simple_select("orders", "o_orderkey");
    let subquery = simple_select("lineitem", "l_orderkey");

    // Build Conjunction: [predicate, NOT IN(...)]
    let predicate = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(column_ref("o_orderstatus")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Varchar("O".into()))),
    };
    let not_in_expr = Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery),
        negated: true,
    };

    stmt.where_clause = Some(Expression::Conjunction(vec![predicate, not_in_expr]));

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created an ANTI JOIN
    match &transformed.from {
        Some(FromClause::Join { join_type, .. }) => {
            assert!(
                matches!(join_type, JoinType::Anti),
                "NOT IN in Conjunction should transform to ANTI join, got: {:?}",
                join_type
            );
        }
        _ => panic!("Expected ANTI JOIN in FROM clause"),
    }

    // Remaining WHERE should have just the predicate
    assert!(transformed.where_clause.is_some(), "Other predicate should remain in WHERE clause");
}

#[test]
fn test_conjunction_preserves_all_other_predicates() {
    // Test that all non-subquery predicates are preserved in the residual WHERE
    let mut stmt = simple_select("orders", "o_orderkey");
    let subquery = simple_select("lineitem", "l_orderkey");

    // Build Conjunction with multiple predicates and one subquery
    let pred1 = Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(column_ref("o_totalprice")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1000))),
    };
    let pred2 = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(column_ref("o_orderstatus")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Varchar("F".into()))),
    };
    let pred3 = Expression::BinaryOp {
        op: BinaryOperator::LessThan,
        left: Box::new(column_ref("o_orderdate")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Varchar("1995-01-01".into()))),
    };
    let in_expr = Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery),
        negated: false,
    };

    stmt.where_clause =
        Some(Expression::Conjunction(vec![pred1.clone(), pred2.clone(), pred3.clone(), in_expr]));

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have created a SEMI JOIN
    match &transformed.from {
        Some(FromClause::Join { join_type, .. }) => {
            assert!(matches!(join_type, JoinType::Semi), "IN should transform to SEMI join");
        }
        _ => panic!("Expected SEMI JOIN in FROM clause"),
    }

    // Remaining WHERE should be a Conjunction with exactly 3 predicates
    match &transformed.where_clause {
        Some(Expression::Conjunction(children)) => {
            assert_eq!(children.len(), 3, "Should have all 3 non-subquery predicates remaining");
        }
        _ => panic!("Expected Conjunction with 3 predicates in WHERE clause"),
    }
}

#[test]
fn test_conjunction_multiple_subqueries_iterative() {
    // Test that multiple subqueries in a Conjunction are handled iteratively
    let mut stmt = simple_select("orders", "o_orderkey");
    let subquery1 = simple_select("lineitem", "l_orderkey");
    let subquery2 = simple_select("supplier", "s_suppkey");

    // Build Conjunction: [predicate, IN(...), NOT IN(...)]
    let predicate = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(column_ref("o_orderstatus")),
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Varchar("F".into()))),
    };
    let in_expr = Expression::In {
        expr: Box::new(column_ref("o_orderkey")),
        subquery: Box::new(subquery1),
        negated: false,
    };
    let not_in_expr = Expression::In {
        expr: Box::new(column_ref("o_custkey")),
        subquery: Box::new(subquery2),
        negated: true,
    };

    stmt.where_clause = Some(Expression::Conjunction(vec![predicate, in_expr, not_in_expr]));

    let transformed = transform_subqueries_to_joins(&stmt);

    // Should have two joins (nested)
    match &transformed.from {
        Some(FromClause::Join { left: inner_join_box, join_type: outer_type, .. }) => {
            assert!(
                matches!(outer_type, JoinType::Semi | JoinType::Anti),
                "Outer join should be SEMI or ANTI"
            );

            match inner_join_box.as_ref() {
                FromClause::Join { join_type: inner_type, .. } => {
                    assert!(
                        matches!(inner_type, JoinType::Semi | JoinType::Anti),
                        "Inner join should be SEMI or ANTI"
                    );
                }
                _ => panic!("Expected nested JOIN"),
            }
        }
        _ => panic!("Expected JOIN in FROM clause"),
    }

    // Remaining WHERE should have just the one predicate
    assert!(transformed.where_clause.is_some(), "Simple predicate should remain in WHERE clause");
}
