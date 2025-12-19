//! Basic SELECT ORDER BY tests
//!
//! Tests for ORDER BY functionality including single and multiple column ordering.

use super::super::*;

/// Test ORDER BY single column ascending
#[test]
fn test_order_by_single_column_asc() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("age", false)),
            direction: vibesql_ast::OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(20));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(25));
    assert_eq!(result[2].values[1], vibesql_types::SqlValue::Integer(30));
}

/// Test ORDER BY multiple columns with different directions
#[test]
fn test_order_by_multiple_columns() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "dept".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(35),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![
            vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("dept", false)),
                direction: vibesql_ast::OrderDirection::Asc,
                nulls_order: None,
            },
            vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("age", false)),
                direction: vibesql_ast::OrderDirection::Desc,
                nulls_order: None,
            },
        ]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(30));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[2], vibesql_types::SqlValue::Integer(25));
    assert_eq!(result[2].values[1], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[2].values[2], vibesql_types::SqlValue::Integer(35));
    assert_eq!(result[3].values[1], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[3].values[2], vibesql_types::SqlValue::Integer(20));
}

/// Test ORDER BY with join (issue #4552)
/// ORDER BY should work correctly when there's an equi-join condition in WHERE clause
#[test]
fn test_order_by_with_join_issue_4552() {
    let mut db = vibesql_storage::Database::new();

    // Create t1 table
    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "a".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema1).unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]),
    )
    .unwrap();

    // Create t2 table
    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "d".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "e".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
        ],
    );
    db.create_table(schema2).unwrap();
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("c")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("a")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // SELECT * FROM t1, t2 WHERE d=a ORDER BY e
    let stmt = vibesql_ast::SelectStmt {
        into_variables: None,
        into_table: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Cross,
            condition: None,
            using_columns: None,
            natural: false,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::simple("d", false),
            )),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::simple("a", false),
            )),
        }),
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "e", false,
            )),
            direction: vibesql_ast::OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);
    // Should be sorted alphabetically by 'e': a, b, c
    assert_eq!(
        result[0].values[2],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("a"))
    );
    assert_eq!(
        result[1].values[2],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b"))
    );
    assert_eq!(
        result[2].values[2],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("c"))
    );
}
