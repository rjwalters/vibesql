use vibesql_ast::*;
use vibesql_types::SqlValue;

// ============================================================================
// SelectStmt Tests - SELECT statement structure
// ============================================================================

#[test]
fn test_select_star() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
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
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
        ],
        into_table: None,
        into_variables: None,
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert_eq!(select.select_list.len(), 2);
}

#[test]
fn test_select_with_alias() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: Some("user_id".to_string()),
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
    };

    match &select.select_list[0] {
        SelectItem::Expression { alias: Some(a), .. } if a == "user_id" => {} // Success
        _ => panic!("Expected aliased expression"),
    }
}

#[test]
fn test_select_from_table() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
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
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    assert!(select.where_clause.is_some());
}

#[test]
fn test_select_with_order_by() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![OrderByItem {
            expr: Expression::ColumnRef { table: None, column: "name".to_string() },
            direction: OrderDirection::Asc,
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
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "country".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.distinct);
}

#[test]
fn test_select_with_group_by() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::Function {
                name: "COUNT".to_string(),
                args: vec![Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "orders".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: Some(GroupByClause::Simple(vec![Expression::ColumnRef {
            table: None,
            column: "customer_id".to_string(),
        }])),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.group_by.is_some());
}

#[test]
fn test_select_with_having() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: Some(GroupByClause::Simple(vec![Expression::ColumnRef {
            table: None,
            column: "region".to_string(),
        }])),
        having: Some(Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::Function {
                name: "SUM".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "amount".to_string() }],
                character_unit: None,
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1000))),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(select.having.is_some());
}

#[test]
fn test_select_with_limit() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: Some(10),
        offset: None,
    };
    assert_eq!(select.limit, Some(10));
}

#[test]
fn test_select_with_offset() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "items".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: Some(20),
        offset: Some(100),
    };
    assert_eq!(select.offset, Some(100));
}

#[test]
fn test_order_by_desc() {
    let select = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "posts".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![OrderByItem {
            expr: Expression::ColumnRef { table: None, column: "created_at".to_string() },
            direction: OrderDirection::Desc,
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
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        right: Box::new(FromClause::Table {
            name: "orders".to_string(),
            alias: None,
            column_aliases: None,
        }),
        join_type: JoinType::Inner,
        condition: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("users".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("orders".to_string()),
                column: "user_id".to_string(),
            }),
        }),
        natural: false,
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
            name: "customers".to_string(),
            alias: None,
            column_aliases: None,
        }),
        right: Box::new(FromClause::Table {
            name: "orders".to_string(),
            alias: None,
            column_aliases: None,
        }),
        join_type: JoinType::LeftOuter,
        condition: None,
        natural: false,
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
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        right: Box::new(FromClause::Table {
            name: "categories".to_string(),
            alias: None,
            column_aliases: None,
        }),
        join_type: JoinType::RightOuter,
        condition: None,
        natural: false,
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
            name: "table1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        right: Box::new(FromClause::Table {
            name: "table2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        join_type: JoinType::FullOuter,
        condition: None,
        natural: false,
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
            name: "colors".to_string(),
            alias: None,
            column_aliases: None,
        }),
        right: Box::new(FromClause::Table {
            name: "sizes".to_string(),
            alias: None,
            column_aliases: None,
        }),
        join_type: JoinType::Cross,
        condition: None,
        natural: false,
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
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "active".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        }),
        group_by: None,
        having: None,
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
        name: "employees".to_string(),
        alias: Some("e".to_string()),
        column_aliases: None,
    };
    match from {
        FromClause::Table { alias: Some(a), .. } if a == "e" => {} // Success
        _ => panic!("Expected table with alias"),
    }
}
