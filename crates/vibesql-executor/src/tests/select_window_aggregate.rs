//! Tests for aggregate window functions in SELECT statements

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

use crate::SelectExecutor;

fn create_test_db() -> Database {
    let mut db = Database::new();

    // Create a simple test table
    // Table and column names must be uppercase to match SQL identifier normalization
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Integer, false),
            ColumnSchema::new("DAY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("SALES").unwrap();
    use vibesql_storage::Row;
    table
        .insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Integer(1)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200), SqlValue::Integer(2)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(300), SqlValue::Integer(3)]))
        .unwrap();

    db
}

#[test]
fn test_count_star_window_function() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, COUNT(*) OVER () as total_count FROM sales
    let query = "SELECT id, COUNT(*) OVER () as total_count FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows (one for each row in sales)
        assert_eq!(result.len(), 3);

        // Each row should have 2 columns: id and total_count
        assert_eq!(result[0].values.len(), 2);

        // All rows should have count = 3 (total rows)
        assert_eq!(result[0].values[1], SqlValue::Numeric(3.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(3.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(3.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_sum_window_running_total() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, SUM(amount) OVER (ORDER BY "day") as running_total FROM sales
    let query = "SELECT id, SUM(amount) OVER (ORDER BY \"day\") as running_total FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Each row should have 2 columns: id and running_total
        assert_eq!(result[0].values.len(), 2);

        // Verify running totals: 100, 300, 600
        assert_eq!(result[0].values[1], SqlValue::Numeric(100.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(300.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(600.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_avg_window_function() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, AVG(amount) OVER () as avg_amount FROM sales
    let query = "SELECT id, AVG(amount) OVER () as avg_amount FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 2);

        // Average of 100, 200, 300 = 200
        assert_eq!(result[0].values[1], SqlValue::Numeric(200.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(200.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(200.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_min_max_window_functions() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, MIN(amount) OVER () as min_amt, MAX(amount) OVER () as max_amt FROM sales
    let query =
        "SELECT id, MIN(amount) OVER () as min_amt, MAX(amount) OVER () as max_amt FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 3); // id, min_amt, max_amt

        // MIN = 100, MAX = 300 for all rows
        assert_eq!(result[0].values[1], SqlValue::Integer(100));
        assert_eq!(result[0].values[2], SqlValue::Integer(300));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_function_in_expression() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT amount, amount * 100 / SUM(amount) OVER () as percentage FROM sales
    // This tests window functions in complex expressions
    let query = "SELECT amount, amount * 100 / SUM(amount) OVER () as percentage FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 2);

        // Total sum = 600
        // Row 1: 100 * 100 / 600 = 16.666...
        // Row 2: 200 * 100 / 600 = 33.333...
        // Row 3: 300 * 100 / 600 = 50
        assert_eq!(result[0].values[1], SqlValue::Numeric(16.666666666666668));
        assert_eq!(result[1].values[1], SqlValue::Numeric(33.333333333333336));
        assert_eq!(result[2].values[1], SqlValue::Numeric(50.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_function_with_moving_frame() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as
    // moving_avg FROM sales
    let query = "SELECT id, AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_avg FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);

        // Row 1: AVG(100) = 100
        // Row 2: AVG(100, 200) = 150
        // Row 3: AVG(200, 300) = 250
        assert_eq!(result[0].values[1], SqlValue::Numeric(100.0));
        assert_eq!(result[1].values[1], SqlValue::Numeric(150.0));
        assert_eq!(result[2].values[1], SqlValue::Numeric(250.0));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_multiple_window_functions_same_query() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // SELECT id, COUNT(*) OVER () as cnt, SUM(amount) OVER () as total FROM sales
    let query = "SELECT id, COUNT(*) OVER () as cnt, SUM(amount) OVER () as total FROM sales";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values.len(), 3); // id, cnt, total

        // All rows should have cnt=3 and total=600
        for row in &result {
            assert_eq!(row.values[1], SqlValue::Numeric(3.0));
            assert_eq!(row.values[2], SqlValue::Numeric(600.0));
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_function_with_partition_by() {
    let mut db = Database::new();

    // Create table with department data
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("DEPT".to_string(), DataType::Integer, false),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data - 2 departments
    use vibesql_storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(50000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Integer(60000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(2),
            SqlValue::Integer(70000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Integer(2),
            SqlValue::Integer(80000),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT dept, AVG(salary) OVER (PARTITION BY dept) as dept_avg FROM employees
    let query = "SELECT dept, AVG(salary) OVER (PARTITION BY dept) as dept_avg FROM employees";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 4);

        // Department 1: AVG(50000, 60000) = 55000
        // Department 2: AVG(70000, 80000) = 75000
        // Check that rows with dept=1 have avg=55000 and dept=2 have avg=75000
        for row in &result {
            if row.values[0] == SqlValue::Integer(1) {
                assert_eq!(row.values[1], SqlValue::Numeric(55000.0));
            } else if row.values[0] == SqlValue::Integer(2) {
                assert_eq!(row.values[1], SqlValue::Numeric(75000.0));
            }
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_with_window_function() {
    use vibesql_storage::Row;
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Integer(50000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Integer(60000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            SqlValue::Integer(70000),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test ORDER BY referencing a window function from SELECT
    let query = "SELECT name, ROW_NUMBER() OVER (ORDER BY salary) as rn FROM employees ORDER BY ROW_NUMBER() OVER (ORDER BY salary)";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);

        // Results should be ordered by ROW_NUMBER (which is 1, 2, 3)
        // Since ROW_NUMBER is ordered by salary ASC, Alice (50000) should be first
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
        assert_eq!(result[0].values[1], SqlValue::Integer(1));

        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
        assert_eq!(result[1].values[1], SqlValue::Integer(2));

        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
        assert_eq!(result[2].values[1], SqlValue::Integer(3));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_order_by_with_window_function_not_in_select() {
    use vibesql_storage::Row;
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Integer(50000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Integer(60000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            SqlValue::Integer(70000),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test ORDER BY with window function NOT in SELECT list
    let query = "SELECT name FROM employees ORDER BY ROW_NUMBER() OVER (ORDER BY salary DESC)";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        assert_eq!(result.len(), 3);

        // Results should be ordered by ROW_NUMBER with salary DESC
        // Charlie (70000) gets ROW_NUMBER=1, Bob (60000) gets 2, Alice (50000) gets 3
        assert_eq!(result[0].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
        assert_eq!(result[1].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
        assert_eq!(result[2].values[0], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_function_with_group_by_aggregate() {
    use vibesql_storage::Row;
    let mut db = Database::new();

    // Create sales table with categories
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "CATEGORY".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    let table = db.get_table_mut("SALES").unwrap();
    // Category A has 3 sales: 100, 200, 300 = 600 total
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Integer(100),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Integer(200),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Integer(300),
        ]))
        .unwrap();
    // Category B has 2 sales: 400, 500 = 900 total
    table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("B")),
            SqlValue::Integer(400),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("B")),
            SqlValue::Integer(500),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test: Q12-style nested aggregate in window function
    // SUM(SUM(amount)) OVER (PARTITION BY category)
    // This is the pattern from TPC-DS Q12:
    // - Inner SUM(amount) is computed per GROUP BY
    // - Outer SUM() OVER() computes the window aggregate over the grouped results
    let query = r#"
        SELECT 
            category,
            SUM(amount) as total,
            SUM(SUM(amount)) OVER (PARTITION BY category) as partition_total
        FROM sales
        GROUP BY category
    "#;
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt);

        // This should succeed - currently may fail with UnsupportedExpression
        assert!(result.is_ok(), "Expected Q12-style window function to work: {:?}", result.err());

        let rows = result.unwrap();
        assert_eq!(rows.len(), 2); // Two categories

    // Category A: total=600, partition_total=600 (only one group in partition)
    // Category B: total=900, partition_total=900 (only one group in partition)
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test TPC-DS Q12 exact pattern:
/// SUM(amount) * 100 / SUM(SUM(amount)) OVER (PARTITION BY class)
#[test]
fn test_tpcds_q12_revenue_ratio_pattern() {
    use vibesql_storage::Row;
    let mut db = Database::new();

    // Create web_sales-like table
    let schema = TableSchema::new(
        "WEB_SALES".to_string(),
        vec![
            ColumnSchema::new("WS_ITEM_SK".to_string(), DataType::Integer, false),
            ColumnSchema::new("WS_EXT_SALES_PRICE".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Create item-like table
    let schema = TableSchema::new(
        "ITEM".to_string(),
        vec![
            ColumnSchema::new("I_ITEM_SK".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "I_ITEM_ID".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "I_CLASS".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert items with different classes
    let item_table = db.get_table_mut("ITEM").unwrap();
    // Class "electronics" - items 1 and 2
    item_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("ITEM001")),
            SqlValue::Varchar(arcstr::ArcStr::from("electronics")),
        ]))
        .unwrap();
    item_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("ITEM002")),
            SqlValue::Varchar(arcstr::ArcStr::from("electronics")),
        ]))
        .unwrap();
    // Class "sports" - item 3
    item_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("ITEM003")),
            SqlValue::Varchar(arcstr::ArcStr::from("sports")),
        ]))
        .unwrap();

    // Insert web sales
    let sales_table = db.get_table_mut("WEB_SALES").unwrap();
    // Item 1 (electronics): sales of 100, 200 = 300 total
    sales_table.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)])).unwrap();
    sales_table.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(200)])).unwrap();
    // Item 2 (electronics): sales of 300, 400 = 700 total
    sales_table.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(300)])).unwrap();
    sales_table.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(400)])).unwrap();
    // Item 3 (sports): sales of 500
    sales_table.insert(Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(500)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // TPC-DS Q12 pattern: revenue ratio within class
    // SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class)
    let query = r#"
        SELECT
            i_item_id,
            i_class,
            SUM(ws_ext_sales_price) AS itemrevenue,
            SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
        FROM web_sales, item
        WHERE ws_item_sk = i_item_sk
        GROUP BY i_item_id, i_class
    "#;
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt);

        assert!(result.is_ok(), "TPC-DS Q12 pattern should work: {:?}", result.err());

        let rows = result.unwrap();
        assert_eq!(rows.len(), 3); // 3 items

    // Electronics class total: 300 + 700 = 1000
    // ITEM001: 300 / 1000 * 100 = 30%
    // ITEM002: 700 / 1000 * 100 = 70%
    // Sports class total: 500
    // ITEM003: 500 / 500 * 100 = 100%
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test AVG(SUM(...)) pattern - minimal reproduction of TPC-DS Q57 issue
/// This is the exact pattern that fails in Q57:
/// AVG(SUM(cs_sales_price)) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
#[test]
fn test_window_function_avg_sum_nested() {
    use vibesql_storage::Row;
    let mut db = Database::new();

    // Create sales table with categories and months
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "CATEGORY".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("MONTH".to_string(), DataType::Integer, false),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    let table = db.get_table_mut("SALES").unwrap();
    // Category A, Month 1: sales 100, 200 = 300 total
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Integer(1),
            SqlValue::Integer(100),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Integer(1),
            SqlValue::Integer(200),
        ]))
        .unwrap();
    // Category A, Month 2: sales 300 = 300 total
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Integer(2),
            SqlValue::Integer(300),
        ]))
        .unwrap();
    // Category A, Month 3: sales 400 = 400 total
    table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Integer(3),
            SqlValue::Integer(400),
        ]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test AVG(SUM(amount)) - this is the Q57 pattern
    let query = r#"
        SELECT
            category,
            month,
            SUM(amount) as total,
            AVG(SUM(amount)) OVER (PARTITION BY category) as avg_monthly
        FROM sales
        GROUP BY category, month
    "#;
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt);

        assert!(result.is_ok(), "AVG(SUM()) should work: {:?}", result.err());

        let rows = result.unwrap();
        // 3 months for category A
        assert_eq!(rows.len(), 3);

        // Debug output
        eprintln!("=== AVG(SUM()) test results ===");
        for (i, row) in rows.iter().enumerate() {
            eprintln!("Row {}: {:?}", i, row.values);
        }

        // Month 1: total=300
        // Month 2: total=300
        // Month 3: total=400
        // AVG = (300 + 300 + 400) / 3 = 333.33...
        for row in &rows {
            // avg_monthly should be ~333.33 for all rows
            if let SqlValue::Numeric(avg) = row.values[3] {
                assert!(
                    (avg - 333.333).abs() < 1.0,
                    "Expected avg ~333.33, got {}",
                    avg
                );
            } else {
                panic!("Expected Numeric for avg_monthly, got {:?}", row.values[3]);
            }
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test AVG(SUM(...)) with frame specification like Q57
#[test]
fn test_window_function_avg_sum_with_frame() {
    use vibesql_storage::Row;
    let mut db = Database::new();

    // Create sales table with categories and months
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "CATEGORY".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("MONTH".to_string(), DataType::Integer, false),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    let table = db.get_table_mut("SALES").unwrap();
    // 5 months of data for category A
    for month in 1..=5 {
        table
            .insert(Row::new(vec![
                SqlValue::Integer(month),
                SqlValue::Varchar(arcstr::ArcStr::from("A")),
                SqlValue::Integer(month),
                SqlValue::Integer(month * 100), // 100, 200, 300, 400, 500
            ]))
            .unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Test AVG(SUM(amount)) with frame - exact Q57 pattern
    let query = r#"
        SELECT
            category,
            month,
            SUM(amount) as total,
            AVG(SUM(amount)) OVER (
                PARTITION BY category
                ORDER BY month
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ) as moving_avg
        FROM sales
        GROUP BY category, month
        ORDER BY month
    "#;
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt);

        assert!(
            result.is_ok(),
            "AVG(SUM()) with frame should work: {:?}",
            result.err()
        );

        let rows = result.unwrap();
        assert_eq!(rows.len(), 5);

        // Month 1: AVG(100, 200) = 150
        // Month 2: AVG(100, 200, 300) = 200
        // Month 3: AVG(200, 300, 400) = 300
        // Month 4: AVG(300, 400, 500) = 400
        // Month 5: AVG(400, 500) = 450
    } else {
        panic!("Expected SELECT statement");
    }
}
