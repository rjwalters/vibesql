//! Tests for table elimination optimizer pass

#[cfg(test)]
mod derive_prefix_from_table_name_tests {
    use super::super::prefix::derive_prefix_from_table_name;

    #[test]
    fn short_aliases_two_chars() {
        // TPC-DS style 2-char aliases
        assert_eq!(derive_prefix_from_table_name("ss"), Some("ss_".to_string()));
        assert_eq!(derive_prefix_from_table_name("ws"), Some("ws_".to_string()));
        assert_eq!(derive_prefix_from_table_name("cs"), Some("cs_".to_string()));
    }

    #[test]
    fn short_aliases_three_chars() {
        // 3-char aliases
        assert_eq!(derive_prefix_from_table_name("inv"), Some("inv_".to_string()));
    }

    #[test]
    fn dimension_tables() {
        // Tables ending with _dim use first letter
        assert_eq!(derive_prefix_from_table_name("date_dim"), Some("d_".to_string()));
        assert_eq!(derive_prefix_from_table_name("time_dim"), Some("t_".to_string()));
        assert_eq!(derive_prefix_from_table_name("item_dim"), Some("i_".to_string()));
    }

    #[test]
    fn multi_word_tables() {
        // Multi-word tables use acronym (first letter of each word)
        assert_eq!(derive_prefix_from_table_name("customer_address"), Some("ca_".to_string()));
        assert_eq!(derive_prefix_from_table_name("store_sales"), Some("ss_".to_string()));
        assert_eq!(derive_prefix_from_table_name("web_returns"), Some("wr_".to_string()));
        assert_eq!(derive_prefix_from_table_name("catalog_page"), Some("cp_".to_string()));
    }

    #[test]
    fn single_word_tables() {
        // Single word tables use first letter
        assert_eq!(derive_prefix_from_table_name("customer"), Some("c_".to_string()));
        assert_eq!(derive_prefix_from_table_name("item"), Some("i_".to_string()));
        assert_eq!(derive_prefix_from_table_name("store"), Some("s_".to_string()));
        assert_eq!(derive_prefix_from_table_name("warehouse"), Some("w_".to_string()));
    }

    #[test]
    fn case_insensitive() {
        // Should handle mixed case
        assert_eq!(derive_prefix_from_table_name("DATE_DIM"), Some("d_".to_string()));
        assert_eq!(derive_prefix_from_table_name("Customer"), Some("c_".to_string()));
        assert_eq!(derive_prefix_from_table_name("Store_Sales"), Some("ss_".to_string()));
    }

    #[test]
    fn empty_string() {
        // Empty string falls through to short alias logic (len <= 3)
        // and returns "_" (empty + underscore)
        assert_eq!(derive_prefix_from_table_name(""), Some("_".to_string()));
    }
}

#[cfg(test)]
mod find_common_prefix_tests {
    use super::super::prefix::find_common_prefix;

    #[test]
    fn common_underscore_prefix() {
        let cols = vec!["d_year".to_string(), "d_date_sk".to_string(), "d_month".to_string()];
        assert_eq!(find_common_prefix(&cols), Some("d_".to_string()));
    }

    #[test]
    fn two_char_prefix() {
        let cols = vec!["ca_state".to_string(), "ca_city".to_string(), "ca_zip".to_string()];
        assert_eq!(find_common_prefix(&cols), Some("ca_".to_string()));
    }

    #[test]
    fn no_common_prefix() {
        let cols = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        assert_eq!(find_common_prefix(&cols), None);
    }

    #[test]
    fn empty_columns() {
        let cols: Vec<String> = vec![];
        assert_eq!(find_common_prefix(&cols), None);
    }

    #[test]
    fn single_column() {
        let cols = vec!["d_year".to_string()];
        assert_eq!(find_common_prefix(&cols), Some("d_".to_string()));
    }

    #[test]
    fn partial_match_not_all_columns() {
        // First two columns share prefix, but third doesn't
        let cols = vec!["d_year".to_string(), "d_month".to_string(), "t_hour".to_string()];
        assert_eq!(find_common_prefix(&cols), None);
    }

    #[test]
    fn fallback_to_two_char_prefix() {
        // No underscore, but shares first 2 chars
        let cols = vec!["item1".to_string(), "item2".to_string()];
        assert_eq!(find_common_prefix(&cols), Some("it".to_string()));
    }
}

#[cfg(test)]
mod eliminate_unused_tables_tests {
    use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType, SelectItem, SelectStmt};
    use vibesql_types::SqlValue;

    use super::super::eliminate_unused_tables;

    fn make_column_ref(table: Option<&str>, column: &str) -> Expression {
        Expression::ColumnRef { table: table.map(|t| t.to_string()), column: column.to_string() }
    }

    fn make_table(name: &str, alias: Option<&str>) -> FromClause {
        FromClause::Table {
            name: name.to_string(),
            alias: alias.map(|a| a.to_string()),
            column_aliases: None,
        quoted: false,
        }
    }

    fn make_cross_join(left: FromClause, right: FromClause) -> FromClause {
        FromClause::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Cross,
            condition: None,
            using_columns: None,
            natural: false,
        }
    }

    #[test]
    fn single_table_unchanged() {
        // Single table should not be eliminated
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: make_column_ref(Some("t1"), "col1"),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(make_table("table1", Some("t1"))),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        assert!(matches!(result.from, Some(FromClause::Table { .. })));
    }

    #[test]
    fn table_in_select_not_eliminated() {
        // Table referenced in SELECT should not be eliminated
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: make_column_ref(Some("t1"), "col1"),
                    alias: None,
                },
                SelectItem::Expression {
                    expr: make_column_ref(Some("t2"), "col2"),
                    alias: None,
                },
            ],
            into_table: None,
            into_variables: None,
            from: Some(make_cross_join(
                make_table("table1", Some("t1")),
                make_table("table2", Some("t2")),
            )),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        // Both tables should be kept since both are in SELECT
        assert!(matches!(result.from, Some(FromClause::Join { .. })));
    }

    #[test]
    fn table_in_equijoin_not_eliminated() {
        // Table in equijoin should not be eliminated
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: make_column_ref(Some("t1"), "col1"),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(make_cross_join(
                make_table("table1", Some("t1")),
                make_table("table2", Some("t2")),
            )),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(make_column_ref(Some("t1"), "id")),
                right: Box::new(make_column_ref(Some("t2"), "id")),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        // Both tables should be kept since they're in equijoin
        assert!(matches!(result.from, Some(FromClause::Join { .. })));
    }

    #[test]
    fn unused_table_with_filter_eliminated() {
        // Table not in SELECT and not in equijoin should be eliminated
        // with its filter converted to EXISTS
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: make_column_ref(Some("t1"), "col1"),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(make_cross_join(
                make_table("table1", Some("t1")),
                make_table("date_dim", Some("d")),
            )),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(make_column_ref(Some("d"), "d_year")),
                right: Box::new(Expression::Literal(SqlValue::Integer(2000))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);

        // FROM should now only have table1
        match &result.from {
            Some(FromClause::Table { name, .. }) => {
                assert_eq!(name, "table1");
            }
            _ => panic!("Expected single table, got: {:?}", result.from),
        }

        // WHERE should contain EXISTS check
        match &result.where_clause {
            Some(Expression::Exists { negated, .. }) => {
                assert!(!negated);
            }
            _ => panic!("Expected EXISTS clause, got: {:?}", result.where_clause),
        }
    }

    #[test]
    fn select_literal_subquery_unchanged() {
        // SELECT 1 FROM ... subqueries should not be optimized
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(1)),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(make_cross_join(
                make_table("table1", Some("t1")),
                make_table("table2", Some("t2")),
            )),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        // Should be unchanged (both tables kept)
        assert!(matches!(result.from, Some(FromClause::Join { .. })));
    }

    #[test]
    fn no_from_clause_unchanged() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(42)),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        assert!(result.from.is_none());
    }

    #[test]
    fn select_star_references_all_tables() {
        // SELECT * should reference all tables, preventing elimination
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(make_cross_join(
                make_table("table1", Some("t1")),
                make_table("table2", Some("t2")),
            )),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        // Both tables should be kept due to SELECT *
        assert!(matches!(result.from, Some(FromClause::Join { .. })));
    }

    #[test]
    fn cross_join_without_filter_preserved() {
        // Regression test: tables in cross joins without filters should NOT be eliminated
        // because cross joins multiply rows intentionally.
        // Example: SELECT 86 * - cor0.col2 FROM tab1, tab2 AS cor0
        // This should return 9 rows (3x3), not 3 rows.
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            // SELECT only references cor0.col2
            select_list: vec![SelectItem::Expression {
                expr: Expression::BinaryOp {
                    op: BinaryOperator::Multiply,
                    left: Box::new(Expression::Literal(SqlValue::Integer(86))),
                    right: Box::new(Expression::UnaryOp {
                        op: vibesql_ast::UnaryOperator::Minus,
                        expr: Box::new(make_column_ref(Some("cor0"), "col2")),
                    }),
                },
                alias: Some("col0".to_string()),
            }],
            into_table: None,
            into_variables: None,
            // Cross join - tab1 is NOT referenced but has no filter
            from: Some(make_cross_join(make_table("tab1", None), make_table("tab2", Some("cor0")))),
            // No WHERE clause
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        // Both tables should be kept - tab1 has no filter, so cross join
        // must be preserved to maintain correct row count
        assert!(
            matches!(result.from, Some(FromClause::Join { .. })),
            "Expected cross join to be preserved, got {:?}",
            result.from
        );
    }

    #[test]
    fn all_tables_eliminable_keeps_unchanged() {
        // Regression test: when ALL tables could be eliminated,
        // we should keep them all to preserve FROM clause.
        // This ensures WHERE clauses like NULL IS NOT NULL work correctly.
        // Example: SELECT - 0 FROM tab0, tab0 cor0 WHERE NULL IS NOT NULL
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            // SELECT with literal (no column refs)
            select_list: vec![SelectItem::Expression {
                expr: Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr: Box::new(Expression::Literal(SqlValue::Integer(0))),
                },
                alias: Some("col3".to_string()),
            }],
            into_table: None,
            into_variables: None,
            // Cross join of same table
            from: Some(make_cross_join(make_table("tab0", None), make_table("tab0", Some("cor0")))),
            // WHERE clause with no column refs (NULL IS NOT NULL)
            where_clause: Some(Expression::IsNull {
                expr: Box::new(Expression::Literal(SqlValue::Null)),
                negated: true,
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        let result = eliminate_unused_tables(&stmt);
        // Both tables should be kept (not eliminated) because eliminating
        // both would leave no FROM clause
        assert!(
            matches!(result.from, Some(FromClause::Join { .. })),
            "Expected FROM clause to be preserved, got {:?}",
            result.from
        );
    }
}

#[cfg(test)]
mod helper_function_tests {
    use vibesql_ast::{BinaryOperator, Expression, SelectItem};
    use vibesql_types::SqlValue;

    use super::super::predicate::{combine_predicates, flatten_and_chain};
    use super::super::select_analysis::{collect_unqualified_columns, has_unqualified_column_ref};

    #[test]
    fn flatten_and_chain_single() {
        let expr = Expression::Literal(SqlValue::Integer(1));
        let result = flatten_and_chain(&expr);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn flatten_and_chain_multiple() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::Literal(SqlValue::Integer(1))),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(Expression::Literal(SqlValue::Integer(2))),
                right: Box::new(Expression::Literal(SqlValue::Integer(3))),
            }),
        };
        let result = flatten_and_chain(&expr);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn combine_predicates_empty() {
        let preds: Vec<Expression> = vec![];
        let result = combine_predicates(preds);
        assert!(matches!(result, Expression::Literal(SqlValue::Boolean(true))));
    }

    #[test]
    fn combine_predicates_single() {
        let preds = vec![Expression::Literal(SqlValue::Integer(42))];
        let result = combine_predicates(preds);
        assert!(matches!(result, Expression::Literal(SqlValue::Integer(42))));
    }

    #[test]
    fn combine_predicates_multiple() {
        let preds = vec![
            Expression::Literal(SqlValue::Integer(1)),
            Expression::Literal(SqlValue::Integer(2)),
        ];
        let result = combine_predicates(preds);
        assert!(matches!(result, Expression::BinaryOp { op: BinaryOperator::And, .. }));
    }

    #[test]
    fn collect_unqualified_columns_finds_refs() {
        let select_list = vec![
            SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "col1".to_string() },
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "col2".to_string(),
                },
                alias: None,
            },
        ];
        let result = collect_unqualified_columns(&select_list);
        assert!(result.contains("col1"));
        assert!(!result.contains("col2")); // Qualified column should not be included
    }

    #[test]
    fn has_unqualified_column_ref_detects_unqualified() {
        let qualified =
            Expression::ColumnRef { table: Some("t1".to_string()), column: "col1".to_string() };
        assert!(!has_unqualified_column_ref(&qualified));

        let unqualified = Expression::ColumnRef { table: None, column: "col1".to_string() };
        assert!(has_unqualified_column_ref(&unqualified));
    }
}
