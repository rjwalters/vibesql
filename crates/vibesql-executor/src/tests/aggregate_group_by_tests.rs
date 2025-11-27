//! GROUP BY clause tests with aggregates
//!
//! Tests for aggregate functions combined with GROUP BY.

use super::super::*;

#[test]
fn test_group_by_with_count() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("dept".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("amount".to_string(), vibesql_types::DataType::Integer, false),
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
        into_variables: None,        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
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
        from: Some(vibesql_ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vibesql_ast::GroupByClause::Simple(vec![vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "dept".to_string(),
        }])),
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
    assert_eq!(results[0], (vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Integer(2)));
    assert_eq!(results[1], (vibesql_types::SqlValue::Integer(2), vibesql_types::SqlValue::Integer(1)));
}
