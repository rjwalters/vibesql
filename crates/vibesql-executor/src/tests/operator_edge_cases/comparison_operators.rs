//! Comparison operator edge case tests
//!
//! Tests for complex nested comparison expressions,
//! logical AND/OR combinations, and comparison edge cases.

use crate::*;

#[test]
fn test_nested_comparisons() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE (val > 10) AND (val < 20)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::GreaterThan,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(10),
                )),
            }),
            op: vibesql_ast::BinaryOperator::And,
            right: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::LessThan,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(20),
                )),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(15));
}
