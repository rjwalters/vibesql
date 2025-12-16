//! HAVING clause tests with aggregates
//!
//! Tests for filtering aggregate results with HAVING.

use vibesql_ast::Statement;
use vibesql_parser::Parser;

use super::super::*;

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
            values: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("dept", false)),
                alias: None, source_text: None },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("amount", false))],
                    character_unit: None,
                },
                alias: None, source_text: None },
        ],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: Some(vibesql_ast::GroupByClause::Simple(vec![
            vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("dept", false)),
        ])),
        having: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("amount", false))],
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
    // SQLite's SUM() always returns REAL (float)
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(300));
}

/// Test for issue #4432: Detect misuse of aliased aggregates in HAVING clause
///
/// SQLite reports "misuse of aliased aggregate" when an aggregate alias
/// (e.g., `m` from `min(f1) AS m`) is used inside another aggregate function
/// in the HAVING clause (e.g., `HAVING max(m) < 10`).
#[test]
fn test_having_misuse_of_aliased_aggregate() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let create_sql = "CREATE TABLE test1 (f1 INTEGER, f2 INTEGER)";
    let stmt = Parser::parse_sql(create_sql).unwrap();
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CREATE TABLE"),
    }

    // Insert some data
    let insert_sql = "INSERT INTO test1 VALUES (11, 22)";
    let stmt = Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
        _ => panic!("Expected INSERT"),
    }

    let insert_sql = "INSERT INTO test1 VALUES (33, 44)";
    let stmt = Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
        _ => panic!("Expected INSERT"),
    }

    // Test case: SELECT min(f1) AS m FROM test1 GROUP BY f1 HAVING max(m+5)<10
    // This should fail with "misuse of aliased aggregate m"
    let query = "SELECT min(f1) AS m FROM test1 GROUP BY f1 HAVING max(m+5)<10";
    let stmt = Parser::parse_sql(query).unwrap();
    match stmt {
        Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let result = executor.execute(&select_stmt);
            assert!(result.is_err(), "Should error for aliased aggregate misuse");
            let err = result.unwrap_err();
            let err_str = format!("{}", err);
            assert!(
                err_str.contains("misuse of aliased aggregate m"),
                "Expected 'misuse of aliased aggregate m', got: {}",
                err_str
            );
        }
        _ => panic!("Expected SELECT"),
    }
}

/// Test that HAVING uses GROUP BY column values, not SELECT aliases with same name
///
/// Issue: When SELECT has "-col AS col", HAVING "col > 5" should reference the
/// GROUP BY column (original col value), not the SELECT alias (-col).
/// For example:
///   SELECT -x AS x FROM t GROUP BY x HAVING x > 15
/// Should filter by the original x values (20, 30 pass), not the alias (-20, -30 fail).
#[test]
fn test_having_uses_group_by_column_not_select_alias() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let create_sql = "CREATE TABLE t (x INTEGER)";
    let stmt = Parser::parse_sql(create_sql).unwrap();
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CREATE TABLE"),
    }

    // Insert values 10, 20, 30
    for val in [10, 20, 30] {
        let insert_sql = format!("INSERT INTO t VALUES ({})", val);
        let stmt = Parser::parse_sql(&insert_sql).unwrap();
        match stmt {
            Statement::Insert(insert_stmt) => {
                InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
            _ => panic!("Expected INSERT"),
        }
    }

    // Test: SELECT -x AS x FROM t GROUP BY x HAVING x > 15
    // Should return -20, -30 (x values 20 and 30 pass the filter)
    // NOT 0 rows (which would happen if HAVING used alias value -x)
    let query = "SELECT -x AS x FROM t GROUP BY x HAVING x > 15";
    let stmt = Parser::parse_sql(query).unwrap();
    match stmt {
        Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let result = executor.execute(&select_stmt).unwrap();
            assert_eq!(
                result.len(),
                2,
                "HAVING x > 15 should pass x=20 and x=30 (using GROUP BY column, not alias)"
            );
            // Results are -20 and -30 (the alias values)
            let values: Vec<i64> = result
                .iter()
                .map(|r| match &r.values[0] {
                    vibesql_types::SqlValue::Integer(v) => *v,
                    _ => panic!("Expected integer"),
                })
                .collect();
            assert!(values.contains(&-20), "Should contain -20 (from x=20)");
            assert!(values.contains(&-30), "Should contain -30 (from x=30)");
        }
        _ => panic!("Expected SELECT"),
    }
}

/// Test that valid uses of aggregate aliases still work
#[test]
fn test_having_valid_aggregate_alias_in_order_by() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let create_sql = "CREATE TABLE test1 (f1 INTEGER, f2 INTEGER)";
    let stmt = Parser::parse_sql(create_sql).unwrap();
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CREATE TABLE"),
    }

    // Insert some data
    let insert_sql = "INSERT INTO test1 VALUES (11, 22)";
    let stmt = Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
        _ => panic!("Expected INSERT"),
    }

    let insert_sql = "INSERT INTO test1 VALUES (33, 44)";
    let stmt = Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
        _ => panic!("Expected INSERT"),
    }

    // Valid use: aggregate alias in ORDER BY (not inside another aggregate)
    let query = "SELECT min(f1) AS m FROM test1 GROUP BY f1 ORDER BY m";
    let stmt = Parser::parse_sql(query).unwrap();
    match stmt {
        Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let result = executor.execute(&select_stmt);
            assert!(result.is_ok(), "Should succeed for valid aggregate alias use in ORDER BY");
            let rows = result.unwrap();
            assert_eq!(rows.len(), 2);
            // First row should have m=11, second should have m=33 (ascending order)
            assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(11));
            assert_eq!(rows[1].values[0], vibesql_types::SqlValue::Integer(33));
        }
        _ => panic!("Expected SELECT"),
    }
}
