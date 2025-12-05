//! Correlated scalar subquery tests
//!
//! Tests for scalar subqueries that reference columns from the outer query:
//! - Basic correlated subqueries with outer references
//! - Correlated subqueries with aggregation

use super::super::*;

#[test]
fn test_correlated_subquery_basic() {
    // Test: Correlated subquery that references outer query column
    // SELECT e.name, e.salary FROM employees e
    // WHERE e.salary > (SELECT AVG(salary) FROM employees WHERE department = e.department)
    let mut db = vibesql_storage::Database::new();

    // Create employees table with department
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "department".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "salary".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    // Engineering: Alice (50000), Bob (80000) - avg 65000
    // Sales: Charlie (40000), Diana (60000) - avg 50000
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("Alice".to_string()),
            vibesql_types::SqlValue::Varchar("Engineering".to_string()),
            vibesql_types::SqlValue::Integer(50000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("Bob".to_string()),
            vibesql_types::SqlValue::Varchar("Engineering".to_string()),
            vibesql_types::SqlValue::Integer(80000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar("Charlie".to_string()),
            vibesql_types::SqlValue::Varchar("Sales".to_string()),
            vibesql_types::SqlValue::Integer(40000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar("Diana".to_string()),
            vibesql_types::SqlValue::Varchar("Sales".to_string()),
            vibesql_types::SqlValue::Integer(60000),
        ]),
    )
    .unwrap();

    // Build correlated subquery: SELECT AVG(salary) FROM employees WHERE department = e.department
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "department".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "department".to_string(),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT name, salary FROM employees e WHERE salary > (correlated subquery)
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
                    column: "name".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: Some("e".to_string()),
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "salary".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(vibesql_ast::Expression::ScalarSubquery(subquery)),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return Bob (80000 > 65000) and Diana (60000 > 50000)
    assert_eq!(result.len(), 2);

    // Check Bob
    let bob = &result[0];
    assert_eq!(bob.get(0).unwrap(), &vibesql_types::SqlValue::Varchar("Bob".to_string()));
    assert_eq!(bob.get(1).unwrap(), &vibesql_types::SqlValue::Integer(80000));

    // Check Diana
    let diana = &result[1];
    assert_eq!(diana.get(0).unwrap(), &vibesql_types::SqlValue::Varchar("Diana".to_string()));
    assert_eq!(diana.get(1).unwrap(), &vibesql_types::SqlValue::Integer(60000));
}
