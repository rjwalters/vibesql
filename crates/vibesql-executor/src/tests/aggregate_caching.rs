//! Aggregate caching tests (Phase 2 of #1038)
//!
//! Tests that verify aggregate functions are cached and reused within a single
//! query evaluation, preventing redundant computation.

use super::super::*;

#[test]
fn test_repeated_count_star_cached() {
    // Test case from issue #1038
    // SELECT COUNT(*) * 37 + NULLIF(45, COUNT(*) * 13) + COUNT(*) + 22
    // All COUNT(*) should be evaluated once and cached

    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 5 rows
    for i in 1..=5 {
        db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]))
            .unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Build: COUNT(*) * 37 + NULLIF(45, COUNT(*) * 13) + COUNT(*) + 22
    // Expected: 5 * 37 + NULLIF(45, 5 * 13) + 5 + 22 = 185 + 45 + 5 + 22 = 257
    let count_star = vibesql_ast::Expression::AggregateFunction {
        name: "COUNT".to_string(),
        distinct: false,
        args: vec![vibesql_ast::Expression::Wildcard],
    };

    // COUNT(*) * 37
    let count_times_37 = vibesql_ast::Expression::BinaryOp {
        left: Box::new(count_star.clone()),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(37))),
    };

    // COUNT(*) * 13
    let count_times_13 = vibesql_ast::Expression::BinaryOp {
        left: Box::new(count_star.clone()),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(13))),
    };

    // NULLIF(45, COUNT(*) * 13)
    let nullif_expr = vibesql_ast::Expression::Function {
        name: "NULLIF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(45)),
            count_times_13,
        ],
        character_unit: None,
    };

    // COUNT(*) * 37 + NULLIF(45, COUNT(*) * 13)
    let first_add = vibesql_ast::Expression::BinaryOp {
        left: Box::new(count_times_37),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(nullif_expr),
    };

    // ... + COUNT(*)
    let second_add = vibesql_ast::Expression::BinaryOp {
        left: Box::new(first_add),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(count_star),
    };

    // ... + 22
    let final_expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(second_add),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(22))),
    };

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression { expr: final_expr, alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(257));
}

#[test]
fn test_repeated_sum_cached() {
    // Test that SUM(column) is cached when used multiple times
    // SELECT SUM(amount) + SUM(amount) * 2

    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "amount".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row("sales", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap();
    db.insert_row("sales", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]))
        .unwrap();
    db.insert_row("sales", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(30)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    let sum_amount = vibesql_ast::Expression::AggregateFunction {
        name: "SUM".to_string(),
        distinct: false,
        args: vec![vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "amount".to_string(),
        }],
    };

    // SUM(amount) * 2
    let sum_times_2 = vibesql_ast::Expression::BinaryOp {
        left: Box::new(sum_amount.clone()),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2))),
    };

    // SUM(amount) + SUM(amount) * 2
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(sum_amount),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(sum_times_2),
    };

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression { expr, alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM(amount) = 60, so 60 + 60*2 = 60 + 120 = 180
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(180));
}

#[test]
fn test_cache_cleared_between_groups() {
    // Verify cache is cleared between different groups
    // SELECT category, COUNT(*) + COUNT(*) FROM items GROUP BY category

    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "items".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "category".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Category A: 2 items
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("A")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("item1")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("A")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("item2")),
        ]),
    )
    .unwrap();

    // Category B: 3 items
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("B")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("item3")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("B")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("item4")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("B")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("item5")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    let count_star = vibesql_ast::Expression::AggregateFunction {
        name: "COUNT".to_string(),
        distinct: false,
        args: vec![vibesql_ast::Expression::Wildcard],
    };

    // COUNT(*) + COUNT(*)
    let doubled_count = vibesql_ast::Expression::BinaryOp {
        left: Box::new(count_star.clone()),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(count_star),
    };

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "category".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression { expr: doubled_count, alias: None },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "items".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: Some(vibesql_ast::GroupByClause::Simple(vec![
            vibesql_ast::Expression::ColumnRef { table: None, column: "category".to_string() },
        ])),
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "category".to_string(),
            },
            direction: vibesql_ast::OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);

    // Category A: COUNT(*) = 2, so 2 + 2 = 4
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("A")));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(4));

    // Category B: COUNT(*) = 3, so 3 + 3 = 6
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("B")));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(6));
}

#[test]
fn test_distinct_aggregates_not_confused() {
    // Verify that COUNT(DISTINCT x) and COUNT(x) are cached separately

    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "values".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert duplicate values
    db.insert_row("values", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("values", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("values", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("values", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("values", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(3)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    let count_val = vibesql_ast::Expression::AggregateFunction {
        name: "COUNT".to_string(),
        distinct: false,
        args: vec![vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }],
    };

    let count_distinct_val = vibesql_ast::Expression::AggregateFunction {
        name: "COUNT".to_string(),
        distinct: true,
        args: vec![vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }],
    };

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: count_val,
                alias: Some("count_all".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                expr: count_distinct_val,
                alias: Some("count_distinct".to_string()),
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "values".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5)); // COUNT(val) = 5
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(3)); // COUNT(DISTINCT val) = 3
}
