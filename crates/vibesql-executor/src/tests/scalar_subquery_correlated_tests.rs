//! Correlated scalar subquery tests
//!
//! Tests for scalar subqueries that reference columns from the outer query:
//! - Basic correlated subqueries with outer references
//! - Correlated subqueries with aggregation
//! - Case sensitivity handling (#4111)

use super::super::*;

/// Test for issue #4111: Column resolution in correlated subqueries with case mismatch
/// The parser uppercases identifiers (e.g., j -> J, i_current_price -> I_CURRENT_PRICE)
/// while the schema may store lowercase column names. This test verifies that column
/// lookups work correctly with case-insensitive matching.
///
/// Reproduces TPC-DS Q6 pattern:
/// ```sql
/// SELECT AVG(j.i_current_price)
/// FROM item j
/// WHERE j.i_category = i.i_category
/// ```
#[test]
fn test_correlated_subquery_uppercase_identifiers_issue_4111() {
    let mut db = vibesql_storage::Database::new();

    // Create table with LOWERCASE column names (simulating TPC-DS data loader)
    let schema = vibesql_catalog::TableSchema::new(
        "item".to_string(), // lowercase table name
        vec![
            vibesql_catalog::ColumnSchema::new(
                "i_item_sk".to_string(), // lowercase
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "i_current_price".to_string(), // lowercase - the problematic column
                vibesql_types::DataType::DoublePrecision,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "i_category".to_string(), // lowercase
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "item",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Double(10.00), // 10.00
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "item",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Double(20.00), // 20.00
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "item",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Double(30.00), // 30.00
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Books")),
        ]),
    )
    .unwrap();

    // Build correlated subquery with UPPERCASE identifiers (as parser would produce):
    // SELECT AVG(J.I_CURRENT_PRICE) FROM ITEM J WHERE J.I_CATEGORY = I.I_CATEGORY
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "AVG".to_string(),
                distinct: false,
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: Some("J".to_string()), // UPPERCASE alias
                    column: "I_CURRENT_PRICE".to_string(), // UPPERCASE column - this was failing
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "ITEM".to_string(), // UPPERCASE table
            alias: Some("J".to_string()), // UPPERCASE alias
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("J".to_string()), // UPPERCASE alias
                column: "I_CATEGORY".to_string(), // UPPERCASE column
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("I".to_string()), // UPPERCASE outer alias
                column: "I_CATEGORY".to_string(), // UPPERCASE column
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT I_ITEM_SK FROM ITEM I WHERE I.I_CURRENT_PRICE > (correlated subquery)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: Some("I".to_string()),
                column: "I_ITEM_SK".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "ITEM".to_string(),
            alias: Some("I".to_string()),
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("I".to_string()),
                column: "I_CURRENT_PRICE".to_string(),
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
    let result = executor.execute(&stmt);

    // The test verifies that column resolution works despite case mismatch
    assert!(
        result.is_ok(),
        "Correlated subquery should resolve J.I_CURRENT_PRICE despite lowercase schema columns. Error: {:?}",
        result.err()
    );

    let rows = result.unwrap();
    // Electronics avg = 15.00, so item 2 (20.00 > 15.00) passes
    // Books avg = 30.00, so item 3 (30.00 > 30.00) fails (not strictly greater)
    assert_eq!(rows.len(), 1, "Only item with price > category avg should be returned");
    assert_eq!(
        rows[0].get(0).unwrap(),
        &vibesql_types::SqlValue::Integer(2),
        "Item 2 should be returned (20.00 > 15.00 avg for Electronics)"
    );
}

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
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            vibesql_types::SqlValue::Integer(50000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            vibesql_types::SqlValue::Integer(80000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            vibesql_types::SqlValue::Integer(40000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Diana")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
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
    assert_eq!(bob.get(0).unwrap(), &vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    assert_eq!(bob.get(1).unwrap(), &vibesql_types::SqlValue::Integer(80000));

    // Check Diana
    let diana = &result[1];
    assert_eq!(diana.get(0).unwrap(), &vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Diana")));
    assert_eq!(diana.get(1).unwrap(), &vibesql_types::SqlValue::Integer(60000));
}
