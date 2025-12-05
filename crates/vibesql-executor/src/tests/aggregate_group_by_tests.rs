//! GROUP BY clause tests with aggregates
//!
//! Tests for aggregate functions combined with GROUP BY.

use super::super::*;

/// Test GROUP BY referencing SELECT list aliases (issue #2910)
///
/// Standard SQL allows GROUP BY to reference column aliases defined in SELECT.
/// Example: SELECT dept AS department, COUNT(*) FROM sales GROUP BY department
#[test]
fn test_group_by_select_alias() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
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
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(150),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT dept AS department, COUNT(*) FROM sales GROUP BY department
    // GROUP BY uses alias "department" which should resolve to column "dept"
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
                    column: "dept".to_string(),
                },
                alias: Some("department".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Wildcard],
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        // GROUP BY "department" - uses alias from SELECT list
        group_by: Some(vibesql_ast::GroupByClause::Simple(vec![
            vibesql_ast::Expression::ColumnRef { table: None, column: "department".to_string() },
        ])),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);

    let mut results = result
        .into_iter()
        .map(|row| (row.values[0].clone(), row.values[1].clone()))
        .collect::<Vec<_>>();
    results.sort_by(|(dept_a, _), (dept_b, _)| match (dept_a, dept_b) {
        (vibesql_types::SqlValue::Integer(a), vibesql_types::SqlValue::Integer(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    });
    // Dept 1 has 2 rows, dept 2 has 1 row
    assert_eq!(
        results[0],
        (vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Integer(2))
    );
    assert_eq!(
        results[1],
        (vibesql_types::SqlValue::Integer(2), vibesql_types::SqlValue::Integer(1))
    );
}

/// Test GROUP BY with numeric column position (GROUP BY 1)
#[test]
fn test_group_by_numeric_position() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
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
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(150),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT dept, COUNT(*) FROM sales GROUP BY 1
    // GROUP BY 1 refers to the first SELECT item (dept)
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
                    column: "dept".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Wildcard],
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        // GROUP BY 1 - first column in SELECT list
        group_by: Some(vibesql_ast::GroupByClause::Simple(vec![vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Integer(1),
        )])),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);

    let mut results = result
        .into_iter()
        .map(|row| (row.values[0].clone(), row.values[1].clone()))
        .collect::<Vec<_>>();
    results.sort_by(|(dept_a, _), (dept_b, _)| match (dept_a, dept_b) {
        (vibesql_types::SqlValue::Integer(a), vibesql_types::SqlValue::Integer(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    });
    assert_eq!(
        results[0],
        (vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Integer(2))
    );
    assert_eq!(
        results[1],
        (vibesql_types::SqlValue::Integer(2), vibesql_types::SqlValue::Integer(1))
    );
}

#[test]
fn test_group_by_with_count() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
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
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(150),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
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
                    column: "dept".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![vibesql_ast::Expression::Wildcard],
                    character_unit: None,
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: Some(vibesql_ast::GroupByClause::Simple(vec![
            vibesql_ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
        ])),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    let mut results = result
        .into_iter()
        .map(|row| (row.values[0].clone(), row.values[1].clone()))
        .collect::<Vec<_>>();
    results.sort_by(|(dept_a, _), (dept_b, _)| match (dept_a, dept_b) {
        (vibesql_types::SqlValue::Integer(a), vibesql_types::SqlValue::Integer(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    });
    assert_eq!(
        results[0],
        (vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Integer(2))
    );
    assert_eq!(
        results[1],
        (vibesql_types::SqlValue::Integer(2), vibesql_types::SqlValue::Integer(1))
    );
}
