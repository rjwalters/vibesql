//! HAVING clause tests with aggregates
//!
//! Tests for filtering aggregate results with HAVING.

use super::super::*;
use vibesql_ast::Statement;
use vibesql_parser::Parser;

#[test]
fn test_having_with_count_star_simple() {
    // Simplified test to debug COUNT(*) in HAVING clause
    let mut db = vibesql_storage::Database::new();

    // Create simple table
    let create_sql = "CREATE TABLE test (id INTEGER, status VARCHAR(20))";
    let stmt = Parser::parse_sql(create_sql).unwrap();
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CREATE TABLE"),
    }

    // Insert 7 rows with status 'A' and 3 rows with status 'B'
    for i in 1..=7 {
        let insert_sql = format!("INSERT INTO test VALUES ({}, 'A')", i);
        let stmt = Parser::parse_sql(&insert_sql).unwrap();
        match stmt {
            Statement::Insert(insert_stmt) => {
                InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
            _ => panic!("Expected INSERT"),
        }
    }

    for i in 8..=10 {
        let insert_sql = format!("INSERT INTO test VALUES ({}, 'B')", i);
        let stmt = Parser::parse_sql(&insert_sql).unwrap();
        match stmt {
            Statement::Insert(insert_stmt) => {
                InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
            _ => panic!("Expected INSERT"),
        }
    }

    // Test without HAVING - should return 2 groups
    let query1 = "SELECT status, COUNT(*) FROM test GROUP BY status";
    let stmt = Parser::parse_sql(query1).unwrap();
    match stmt {
        Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            assert_eq!(rows.len(), 2, "Should have 2 groups without HAVING");
        }
        _ => panic!("Expected SELECT"),
    }

    // Test with HAVING COUNT(*) > 5 - should return 1 group (status 'A' with 7 rows)
    let query2 = "SELECT status, COUNT(*) FROM test GROUP BY status HAVING COUNT(*) > 5";
    let stmt = Parser::parse_sql(query2).unwrap();
    match stmt {
        Statement::Select(select_stmt) => {
            eprintln!("\n=== HAVING clause debug ===");
            eprintln!("HAVING: {:?}", select_stmt.having);
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            eprintln!("Result rows: {}", rows.len());
            for (i, row) in rows.iter().enumerate() {
                eprintln!("  Row {}: {:?}", i, row.values);
            }
            assert_eq!(rows.len(), 1, "Should have 1 group with count > 5");
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_having_clause() {
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
            vibesql_types::SqlValue::Integer(50),
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
                    name: "SUM".to_string(),
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
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
        having: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
                character_unit: None,
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                150,
            ))),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(300));
}
