use vibesql_ast::*;
use vibesql_types::SqlValue;

// ============================================================================
// SelectStmt Tests - SELECT statement structure
// ============================================================================

#[test]
fn test_select_star() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert_eq!(select.select_list.len(), 1);
    match &select.select_list[0] {
        SelectItem::Wildcard { alias: _ } => {} // Success
        _ => panic!("Expected wildcard"),
    }
}

#[test]
fn test_select_with_columns() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![
            SelectItem::Expression {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("id", false)),
                alias: None,
                source_text: None,
            },
            SelectItem::Expression {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("name", false)),
                alias: None,
                source_text: None,
            },
        ],
        into_table: None,
        into_variables: None,
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert_eq!(select.select_list.len(), 2);
}

#[test]
fn test_select_with_alias() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("id", false)),
            alias: Some("user_id".to_string()),
            source_text: None,
        }],
        into_table: None,
        into_variables: None,
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    match &select.select_list[0] {
        SelectItem::Expression { alias: Some(a), .. } if a == "user_id" => {} // Success
        _ => panic!("Expected aliased expression"),
    }
}

#[test]
fn test_select_from_table() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    match &select.from {
        Some(FromClause::Table { name, .. }) if name == "users" => {} // Success
        _ => panic!("Expected table in FROM clause"),
    }
}

#[test]
fn test_select_with_where() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert!(select.where_clause.is_some());
}

#[test]
fn test_select_with_order_by() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: Some(vec![OrderByItem {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("name", false)),
            direction: OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };

    assert!(select.order_by.is_some());
    assert_eq!(select.order_by.as_ref().unwrap().len(), 1);
}

// ============================================================================
// SELECT Advanced Features
// ============================================================================

#[test]
fn test_select_distinct() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: true,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("country", false)),
            alias: None,
            source_text: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.distinct);
}

#[test]
fn test_select_with_group_by() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::Function {
                name: vibesql_ast::FunctionIdentifier::new("COUNT"),
                args: vec![Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
            source_text: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "orders".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: Some(GroupByClause::Simple(vec![Expression::ColumnRef(
            vibesql_ast::ColumnIdentifier::simple("customer_id", false),
        )])),
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.group_by.is_some());
}

#[test]
fn test_select_with_having() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: Some(GroupByClause::Simple(vec![Expression::ColumnRef(
            vibesql_ast::ColumnIdentifier::simple("region", false),
        )])),
        having: Some(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::Function {
                name: vibesql_ast::FunctionIdentifier::new("SUM"),
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "amount", false,
                ))],
                character_unit: None,
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1000))),
        }),
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.having.is_some());
}

#[test]
fn test_select_with_limit() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: Some(Expression::Literal(SqlValue::Integer(10))),
        offset: None,
    };
    assert_eq!(select.limit, Some(Expression::Literal(SqlValue::Integer(10))));
}

#[test]
fn test_select_with_offset() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "items".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: Some(Expression::Literal(SqlValue::Integer(20))),
        offset: Some(Expression::Literal(SqlValue::Integer(100))),
    };
    assert_eq!(select.offset, Some(Expression::Literal(SqlValue::Integer(100))));
}

#[test]
fn test_order_by_desc() {
    let select = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "posts".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: Some(vec![OrderByItem {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("created_at", false)),
            direction: OrderDirection::Desc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };
    let order = select.order_by.as_ref().unwrap();
    assert_eq!(order[0].direction, OrderDirection::Desc);
}

// ============================================================================
// JOIN Tests
// ============================================================================

#[test]
fn test_inner_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table {
            index_hint: None,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        right: Box::new(FromClause::Table {
            index_hint: None,
            name: "orders".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        join_type: JoinType::Inner,
        condition: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "users", false, "id", false,
            ))),
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "orders", false, "user_id", false,
            ))),
        }),
        using_columns: None,
        natural: false,
        alias: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::Inner, .. } => {} // Success
        _ => panic!("Expected INNER JOIN"),
    }
}

#[test]
fn test_left_outer_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table {
            index_hint: None,
            name: "customers".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        right: Box::new(FromClause::Table {
            index_hint: None,
            name: "orders".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        join_type: JoinType::LeftOuter,
        condition: None,
        using_columns: None,
        natural: false,
        alias: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::LeftOuter, .. } => {} // Success
        _ => panic!("Expected LEFT OUTER JOIN"),
    }
}

#[test]
fn test_right_outer_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table {
            index_hint: None,
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        right: Box::new(FromClause::Table {
            index_hint: None,
            name: "categories".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        join_type: JoinType::RightOuter,
        condition: None,
        using_columns: None,
        natural: false,
        alias: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::RightOuter, .. } => {} // Success
        _ => panic!("Expected RIGHT OUTER JOIN"),
    }
}

#[test]
fn test_full_outer_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table {
            index_hint: None,
            name: "table1".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        right: Box::new(FromClause::Table {
            index_hint: None,
            name: "table2".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        join_type: JoinType::FullOuter,
        condition: None,
        using_columns: None,
        natural: false,
        alias: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::FullOuter, .. } => {} // Success
        _ => panic!("Expected FULL OUTER JOIN"),
    }
}

#[test]
fn test_cross_join() {
    let from = FromClause::Join {
        left: Box::new(FromClause::Table {
            index_hint: None,
            name: "colors".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        right: Box::new(FromClause::Table {
            index_hint: None,
            name: "sizes".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        join_type: JoinType::Cross,
        condition: None,
        using_columns: None,
        natural: false,
        alias: None,
    };
    match from {
        FromClause::Join { join_type: JoinType::Cross, .. } => {} // Success
        _ => panic!("Expected CROSS JOIN"),
    }
}

// ============================================================================
// FROM Subquery Tests
// ============================================================================

#[test]
fn test_from_subquery() {
    let subquery = SelectStmt {
        hints: Vec::new(),
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            index_hint: None,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "active", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        }),
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let from = FromClause::Subquery {
        query: Box::new(subquery),
        alias: "active_users".to_string(),
        column_aliases: None,
    };
    match from {
        FromClause::Subquery { alias, .. } if alias == "active_users" => {} // Success
        _ => panic!("Expected subquery in FROM clause"),
    }
}

#[test]
fn test_table_with_alias() {
    let from = FromClause::Table {
        index_hint: None,
        name: "employees".to_string(),
        alias: Some("e".to_string()),
        column_aliases: None,
        quoted: false,
    };
    match from {
        FromClause::Table { alias: Some(a), .. } if a == "e" => {} // Success
        _ => panic!("Expected table with alias"),
    }
}
