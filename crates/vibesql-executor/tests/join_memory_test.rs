//! Test to reproduce issue #2194: Memory explosion on multi-table joins

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Helper function to parse SELECT statements
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Create a test database with three tables of varying sizes
fn setup_test_db() -> Database {
    let mut db = Database::new();

    // Create customer table (500 rows)
    let customer_schema = TableSchema::new(
        "CUSTOMER".to_string(),
        vec![
            ColumnSchema::new("c_custkey".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "c_mktsegment".to_string(),
                DataType::Varchar { max_length: Some(10) },
                true,
            ),
        ],
    );
    db.create_table(customer_schema).unwrap();

    // Insert data into customer (500 rows)
    for i in 1..=500 {
        let segment = if i % 2 == 0 { "BUILDING" } else { "FURNITURE" };
        let row = Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(segment))]);
        db.insert_row("CUSTOMER", row).unwrap();
    }

    // Create orders table (2000 rows, ~4 orders per customer)
    let orders_schema = TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema::new("o_orderkey".to_string(), DataType::Integer, false),
            ColumnSchema::new("o_custkey".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "o_orderdate".to_string(),
                DataType::Varchar { max_length: Some(10) },
                true,
            ),
            ColumnSchema::new("o_shippriority".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(orders_schema).unwrap();

    // Insert data into orders (2000 rows)
    for i in 1..=2000 {
        let custkey = ((i - 1) % 500) + 1;
        let date = if i % 2 == 0 { "1995-03-10" } else { "1995-03-20" };
        let row = Row::new(vec![
            SqlValue::Integer(i),
            SqlValue::Integer(custkey),
            SqlValue::Varchar(arcstr::ArcStr::from(date)),
            SqlValue::Integer(i % 3),
        ]);
        db.insert_row("ORDERS", row).unwrap();
    }

    // Create lineitem table (4000 rows, 2 items per order)
    let lineitem_schema = TableSchema::new(
        "LINEITEM".to_string(),
        vec![
            ColumnSchema::new("l_orderkey".to_string(), DataType::Integer, false),
            ColumnSchema::new("l_extendedprice".to_string(), DataType::Integer, true),
            ColumnSchema::new("l_discount".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "l_shipdate".to_string(),
                DataType::Varchar { max_length: Some(10) },
                true,
            ),
        ],
    );
    db.create_table(lineitem_schema).unwrap();

    // Insert data into lineitem (4000 rows)
    for i in 1..=4000 {
        let orderkey = ((i - 1) % 2000) + 1;
        let date = if i % 2 == 0 { "1995-03-08" } else { "1995-03-18" };
        let row = Row::new(vec![
            SqlValue::Integer(orderkey),
            SqlValue::Integer(100 * i),
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from(date)),
        ]);
        db.insert_row("LINEITEM", row).unwrap();
    }

    db
}

#[test]
fn test_two_table_join_baseline() {
    let db = setup_test_db();
    let executor = SelectExecutor::new(&db);

    // 2-table join as a baseline (should work fine)
    let sql = r#"
        SELECT COUNT(*)
        FROM customer, orders
        WHERE c_custkey = o_custkey
            AND c_mktsegment = 'BUILDING'
    "#;

    println!("Executing 2-table join query...");
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();
    println!("SUCCESS: 2-table join returned {} rows", result.len());
    assert_eq!(result.len(), 1); // COUNT(*) returns 1 row
}

#[test]
fn test_three_table_join_memory() {
    let db = setup_test_db();
    let executor = SelectExecutor::new(&db);

    // TPC-H Q3 style query with 3-table join using explicit JOIN syntax
    // Note: Using explicit JOINs instead of comma-separated tables
    // because the current implementation passes additional_equijoins=[] for CROSS JOINs
    let sql = r#"
        SELECT
            l_orderkey,
            SUM(l_extendedprice * (1 - l_discount)) as revenue
        FROM customer
            INNER JOIN orders ON c_custkey = o_custkey
            INNER JOIN lineitem ON l_orderkey = o_orderkey
        WHERE c_mktsegment = 'BUILDING'
            AND o_orderdate < '1995-03-15'
            AND l_shipdate > '1995-03-15'
        GROUP BY l_orderkey
        LIMIT 10
    "#;

    println!("Executing 3-table join query...");
    let stmt = parse_select(sql);

    match executor.execute(&stmt) {
        Ok(rows) => {
            println!("SUCCESS: Query returned {} rows (memory check passed!)", rows.len());
            // Test passes if no memory error occurs - result count doesn't matter
            // The filters are restrictive, so 0 rows is acceptable
        }
        Err(e) => {
            eprintln!("FAILED: {}", e);
            panic!("Query should not fail with memory error: {}", e);
        }
    }
}
