//! Test to reproduce issue #2415 - Silent crashes in TPC-H queries
//!
//! This test reproduces the segfault that occurs with queries containing:
//! - JOINs (implicit or explicit)
//! - GROUP BY + aggregates
//!
//! Affected queries: Q2, Q3, Q7, Q11, Q13, Q20, Q21

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_simple_join_with_group_by() {
    eprintln!("[TEST] Starting simple join with GROUP BY test");

    // Create minimal database
    let mut db = Database::new();

    // Create orders table (uppercase to match SQL parser)
    let orders_schema = TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema::new("O_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("O_CUSTOMER".to_string(), DataType::Integer, false),
            ColumnSchema::new("O_TOTAL".to_string(), DataType::Real, false),
        ],
    );
    db.create_table(orders_schema).unwrap();

    // Create customers table (uppercase to match SQL parser)
    let customers_schema = TableSchema::new(
        "CUSTOMERS".to_string(),
        vec![
            ColumnSchema::new("C_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "C_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(customers_schema).unwrap();

    eprintln!("[TEST] Tables created");

    // Insert minimal data
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Real(50.0)]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(100), SqlValue::Real(75.0)]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(200), SqlValue::Real(100.0)]),
    )
    .unwrap();

    db.insert_row(
        "CUSTOMERS",
        Row::new(vec![SqlValue::Integer(100), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "CUSTOMERS",
        Row::new(vec![SqlValue::Integer(200), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    )
    .unwrap();

    eprintln!("[TEST] Data inserted");

    // This query structure matches the crashing TPC-H queries:
    // - Implicit JOIN (comma-separated FROM)
    // - WHERE clause with join condition
    // - GROUP BY with aggregate
    let sql = "SELECT o_customer, SUM(o_total) as total \
               FROM orders, customers \
               WHERE o_customer = c_id \
               GROUP BY o_customer";

    eprintln!("[TEST] Parsing query: {}", sql);
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => panic!("Expected SELECT statement"),
        Err(e) => panic!("Parse error: {}", e),
    };

    eprintln!("[TEST] Query parsed successfully");
    eprintln!("[TEST] Creating executor");
    let executor = SelectExecutor::new(&db);

    eprintln!("[TEST] Executing query...");

    // This should crash with segfault if bug is present
    let result = executor.execute(&stmt);

    eprintln!("[TEST] Query executed, result: {:?}", result);

    match result {
        Ok(rows) => {
            eprintln!("[TEST] SUCCESS: Got {} rows", rows.len());
            assert_eq!(rows.len(), 2, "Expected 2 rows (one per customer)");
        }
        Err(e) => {
            eprintln!("[TEST] ERROR: {}", e);
            panic!("Query failed: {}", e);
        }
    }
}

#[test]
fn test_even_simpler_group_by() {
    eprintln!("[TEST] Starting simple GROUP BY test (no join)");

    let mut db = Database::new();

    let sales_schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new("CUSTOMER_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Real, false),
        ],
    );
    db.create_table(sales_schema).unwrap();

    db.insert_row("SALES", Row::new(vec![SqlValue::Integer(1), SqlValue::Real(100.0)])).unwrap();
    db.insert_row("SALES", Row::new(vec![SqlValue::Integer(1), SqlValue::Real(200.0)])).unwrap();
    db.insert_row("SALES", Row::new(vec![SqlValue::Integer(2), SqlValue::Real(150.0)])).unwrap();

    let sql = "SELECT customer_id, SUM(amount) FROM sales GROUP BY customer_id";

    eprintln!("[TEST] Parsing: {}", sql);
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        _ => panic!("Parse failed"),
    };

    eprintln!("[TEST] Executing...");
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt);

    eprintln!("[TEST] Result: {:?}", result);

    match result {
        Ok(rows) => {
            eprintln!("[TEST] SUCCESS: Got {} rows", rows.len());
            assert_eq!(rows.len(), 2);
        }
        Err(e) => panic!("Query failed: {}", e),
    }
}
