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
        hints: Vec::new(),
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            index_hint: None,
            quoted: false,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "age", false,
            )),
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
        hints: Vec::new(),
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            index_hint: None,
            quoted: false,
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: Some(vec![
            vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "dept", false,
                )),
                direction: vibesql_ast::OrderDirection::Asc,
                nulls_order: None,
            },
            vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "age", false,
                )),
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
        vec![vibesql_catalog::ColumnSchema::new(
            "a".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema1).unwrap();
    db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
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
        hints: Vec::new(),
        into_variables: None,
        into_table: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                index_hint: None,
                quoted: false,
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                index_hint: None,
                quoted: false,
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Cross,
            condition: None,
            using_columns: None,
            natural: false,
            alias: None,
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
        window_definitions: None,
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
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("a")));
    assert_eq!(result[1].values[2], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b")));
    assert_eq!(result[2].values[2], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("c")));
}

/// Test ORDER BY is preserved in derived tables (issue #4734)
/// When a derived table has ORDER BY, the order should be preserved
/// in the outer query if the outer query has no conflicting ORDER BY.
#[test]
fn test_order_by_preserved_in_derived_table_issue_4734() {
    let mut db = vibesql_storage::Database::new();

    // Create table with insertion order different from sorted order
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "a".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "b".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert in non-sorted order: 3, 1, 2
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("three")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("one")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("two")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query: SELECT * FROM (SELECT * FROM t1 ORDER BY a) AS derived
    // The ORDER BY inside the derived table should be preserved
    let inner_query = Box::new(vibesql_ast::SelectStmt {
        hints: Vec::new(),
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            index_hint: None,
            quoted: false,
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            )),
            direction: vibesql_ast::OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        hints: Vec::new(),
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Subquery {
            query: inner_query,
            alias: "derived".to_string(),
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None, // Outer query has NO ORDER BY
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);

    // Order should be preserved from inner ORDER BY: 1, 2, 3
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("one")));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("two")));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(
        result[2].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("three"))
    );
}

/// Test outer ORDER BY takes precedence over derived table ORDER BY (issue #4734)
#[test]
fn test_outer_order_by_takes_precedence_issue_4734() {
    let mut db = vibesql_storage::Database::new();

    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "a".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "b".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert: 3, 1, 2 (insertion order)
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("three")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("one")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("two")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query: SELECT * FROM (SELECT * FROM t1 ORDER BY a) AS derived ORDER BY b
    // Outer ORDER BY (b) should take precedence over inner ORDER BY (a)
    let inner_query = Box::new(vibesql_ast::SelectStmt {
        hints: Vec::new(),
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            index_hint: None,
            quoted: false,
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            )),
            direction: vibesql_ast::OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        hints: Vec::new(),
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Subquery {
            query: inner_query,
            alias: "derived".to_string(),
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "b", false,
            )),
            direction: vibesql_ast::OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);

    // Order should be by column b alphabetically: one, three, two
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("one")));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(
        result[1].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("three"))
    );
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[2].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("two")));
}
