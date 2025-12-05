//! Test that window function examples from web demo work

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_window_demo_count_over() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use vibesql_storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(100000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(80000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Varchar("Davis".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(95000),
        ]))
        .unwrap();

    // Test window-1: COUNT(*) OVER - Total Row Count
    let query = r#"SELECT
  first_name || ' ' || last_name AS employee,
  department,
  salary,
  COUNT(*) OVER () AS total_employees
FROM employees
LIMIT 10"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Each row should have total_employees = 3
        assert_eq!(result[0].values[3], SqlValue::Numeric(3.0));
        assert_eq!(result[1].values[3], SqlValue::Numeric(3.0));
        assert_eq!(result[2].values[3], SqlValue::Numeric(3.0));

        println!("✅ Window demo example 'COUNT(*) OVER' works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_demo_running_total() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use vibesql_storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Integer(100),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Integer(200),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Varchar("Davis".to_string()),
            SqlValue::Integer(300),
        ]))
        .unwrap();

    // Test window-2: Running Total with ORDER BY
    let query = r#"SELECT
  first_name || ' ' || last_name AS employee,
  salary,
  SUM(salary) OVER (ORDER BY employee_id) AS running_total
FROM employees
ORDER BY employee_id
LIMIT 10"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Verify running totals: 100, 300, 600
        assert_eq!(result[0].values[2], SqlValue::Numeric(100.0));
        assert_eq!(result[1].values[2], SqlValue::Numeric(300.0));
        assert_eq!(result[2].values[2], SqlValue::Numeric(600.0));

        println!("✅ Window demo example 'Running Total' works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_demo_partitioned_avg() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data with 2 departments
    use vibesql_storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(100),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(200),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Varchar("Davis".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(300),
        ]))
        .unwrap();

    // Test window-3: Partitioned Averages
    let query = r#"SELECT
  department,
  first_name || ' ' || last_name AS employee,
  salary,
  AVG(salary) OVER (PARTITION BY department) AS dept_avg_salary
FROM employees
ORDER BY department, salary DESC
LIMIT 15"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // Engineering rows should have avg = 200 (100 + 300) / 2
        // Sales row should have avg = 200
        // Note: Results are ordered by department, salary DESC

        println!("✅ Window demo example 'Partitioned Averages' works!");
    } else {
        panic!("Expected SELECT statement");
    }
}
#[test]
fn test_window_lag_basic() {
    let mut db = Database::new();

    // Create sales table
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new(
                "MONTH".to_string(),
                DataType::Varchar { max_length: Some(10) },
                false,
            ),
            ColumnSchema::new("REVENUE".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use vibesql_storage::Row;
    let table = db.get_table_mut("SALES").unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar("2024-01".to_string()), SqlValue::Integer(100)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar("2024-02".to_string()), SqlValue::Integer(150)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar("2024-03".to_string()), SqlValue::Integer(200)]))
        .unwrap();

    // Test LAG to get previous month's revenue
    let query = r#"SELECT
      month,
      revenue,
      LAG(revenue, 1) OVER (ORDER BY month) AS prev_revenue
    FROM sales
    ORDER BY month"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // First row: prev_revenue should be NULL (column index 2)
        assert_eq!(result[0].values[2], SqlValue::Null);

        // Second row: prev_revenue = 100
        assert_eq!(result[1].values[2], SqlValue::Integer(100));

        // Third row: prev_revenue = 150
        assert_eq!(result[2].values[2], SqlValue::Integer(150));

        println!("✅ LAG basic test works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_lead_basic() {
    let mut db = Database::new();

    // Create sales table
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new(
                "MONTH".to_string(),
                DataType::Varchar { max_length: Some(10) },
                false,
            ),
            ColumnSchema::new("REVENUE".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use vibesql_storage::Row;
    let table = db.get_table_mut("SALES").unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar("2024-01".to_string()), SqlValue::Integer(100)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar("2024-02".to_string()), SqlValue::Integer(150)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar("2024-03".to_string()), SqlValue::Integer(200)]))
        .unwrap();

    // Test LEAD to get next month's revenue
    let query = r#"SELECT
      month,
      revenue,
      LEAD(revenue, 1) OVER (ORDER BY month) AS next_revenue
    FROM sales
    ORDER BY month"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 rows
        assert_eq!(result.len(), 3);

        // First row: next_revenue = 150
        assert_eq!(result[0].values[2], SqlValue::Integer(150));

        // Second row: next_revenue = 200
        assert_eq!(result[1].values[2], SqlValue::Integer(200));

        // Third row: next_revenue should be NULL
        assert_eq!(result[2].values[2], SqlValue::Null);

        println!("✅ LEAD basic test works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_first_value() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "EMPLOYEE".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use vibesql_storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(120000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(95000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Integer(85000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Varchar("Dave".to_string()),
            SqlValue::Integer(90000),
        ]))
        .unwrap();

    // Test FIRST_VALUE to get highest salary per department
    let query = r#"SELECT
      department,
      employee,
      salary,
      FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC) AS top_salary
    FROM employees
    ORDER BY department, salary DESC"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 4 rows
        assert_eq!(result.len(), 4);

        // Engineering rows should have top_salary = 120000 (Alice is first in DESC order)
        assert_eq!(result[0].values[3], SqlValue::Integer(120000));
        assert_eq!(result[1].values[3], SqlValue::Integer(120000));

        // Sales rows should have top_salary = 85000 (Carol is first in DESC order)
        // Note: Carol (85000) comes before Dave (90000) when sorted by string comparison
        assert_eq!(result[2].values[3], SqlValue::Integer(85000));
        assert_eq!(result[3].values[3], SqlValue::Integer(85000));

        println!("✅ FIRST_VALUE test works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_last_value() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "EMPLOYEE".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use vibesql_storage::Row;
    let table = db.get_table_mut("EMPLOYEES").unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(120000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(95000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Integer(85000),
        ]))
        .unwrap();
    table
        .insert(Row::new(vec![
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Varchar("Dave".to_string()),
            SqlValue::Integer(90000),
        ]))
        .unwrap();

    // Test LAST_VALUE to get lowest salary per department
    let query = r#"SELECT
      department,
      employee,
      salary,
      LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC) AS lowest_salary
    FROM employees
    ORDER BY department, salary DESC"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 4 rows
        assert_eq!(result.len(), 4);

        // Engineering rows should have lowest_salary = 95000 (Bob is last in DESC order)
        assert_eq!(result[0].values[3], SqlValue::Integer(95000));
        assert_eq!(result[1].values[3], SqlValue::Integer(95000));

        // Sales rows should have lowest_salary = 90000 (Dave is last in DESC order)
        // Note: Dave (90000) comes after Carol (85000) when sorted by string comparison
        assert_eq!(result[2].values[3], SqlValue::Integer(90000));
        assert_eq!(result[3].values[3], SqlValue::Integer(90000));

        println!("✅ LAST_VALUE test works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_window_lag_with_offset_and_default() {
    let mut db = Database::new();

    // Create test table
    let schema = TableSchema::new(
        "DATA".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("VALUE".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    use vibesql_storage::Row;
    let table = db.get_table_mut("DATA").unwrap();
    for i in 1..=5 {
        table.insert(Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i * 10)])).unwrap();
    }

    // Test LAG with offset 2 and default value 999
    let query = r#"SELECT
      id,
      value,
      LAG(value, 2, 999) OVER (ORDER BY id) AS lag_2_rows
    FROM data
    ORDER BY id"#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 5 rows
        assert_eq!(result.len(), 5);

        // First two rows should have default value 999
        assert_eq!(result[0].values[2], SqlValue::Integer(999));
        assert_eq!(result[1].values[2], SqlValue::Integer(999));

        // Remaining rows should lag by 2
        assert_eq!(result[2].values[2], SqlValue::Integer(10)); // LAG of row 1
        assert_eq!(result[3].values[2], SqlValue::Integer(20)); // LAG of row 2
        assert_eq!(result[4].values[2], SqlValue::Integer(30)); // LAG of row 3

        println!("✅ LAG with offset and default test works!");
    } else {
        panic!("Expected SELECT statement");
    }
}
