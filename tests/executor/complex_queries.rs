//! End-to-end integration tests for complex SQL queries.
//!
//! Tests quantified comparisons (ANY/ALL/SOME) with subqueries.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// ========================================================================
// Quantified Comparisons Tests (ANY/ALL/SOME)
// ========================================================================

#[test]
fn test_e2e_quantified_comparisons() {
    let mut db = Database::new();

    // Create employees table: id, name, salary, dept_id
    let employees_schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Integer, false),
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(employees_schema.clone()).unwrap();

    // Insert test data
    // Department 1: salaries 50000, 60000, 70000
    // Department 2: salaries 80000, 90000
    db.insert_row(
        "EMPLOYEES",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(std::sync::Arc::from("Alice")),
            SqlValue::Integer(50000),
            SqlValue::Integer(1),
        ]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEES",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(std::sync::Arc::from("Bob")),
            SqlValue::Integer(60000),
            SqlValue::Integer(1),
        ]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEES",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(std::sync::Arc::from("Charlie")),
            SqlValue::Integer(70000),
            SqlValue::Integer(1),
        ]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEES",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(std::sync::Arc::from("David")),
            SqlValue::Integer(80000),
            SqlValue::Integer(2),
        ]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEES",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(std::sync::Arc::from("Eve")),
            SqlValue::Integer(90000),
            SqlValue::Integer(2),
        ]),
    )
    .unwrap();

    // Test 1: > ALL - salary greater than all dept 1 salaries
    // Only David (80000) and Eve (90000) have salary > ALL dept 1 salaries (50000, 60000, 70000)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("David")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(std::sync::Arc::from("Eve")));

    // Test 2: < ALL - salary less than all dept 2 salaries
    // Alice (50000), Bob (60000), Charlie (70000) have salary < ALL dept 2 salaries (80000, 90000)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary < ALL (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("Alice")));

    // Test 3: > ANY - salary greater than at least one dept 1 salary
    // Everyone except Alice (50000 is not > any value in dept 1 starting with 50000)
    // Actually, Bob (60000) is > 50000, Charlie (70000) is > 50000 and 60000, etc.
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ANY (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 4); // Bob, Charlie, David, Eve

    // Test 4: < ANY - salary less than at least one dept 2 salary
    // All dept 1 employees (Alice, Bob, Charlie) are < 90000
    // David (80000) is < 90000
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary < ANY (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 4); // Alice, Bob, Charlie, David

    // Test 5: = ANY - salary equals any dept 1 salary
    // Only dept 1 employees match
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary = ANY (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("Alice")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(std::sync::Arc::from("Bob")));
    assert_eq!(results[2].values[0], SqlValue::Varchar(std::sync::Arc::from("Charlie")));

    // Test 6: SOME is synonym for ANY
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary = SOME (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 3); // Same as = ANY

    // Test 7: Empty subquery with ALL (should return TRUE - vacuously true)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE dept_id = 999)"
    ).unwrap();
    assert_eq!(results.len(), 5); // All employees pass

    // Test 8: Empty subquery with ANY (should return FALSE - no rows to match)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ANY (SELECT salary FROM employees WHERE dept_id = 999)"
    ).unwrap();
    assert_eq!(results.len(), 0); // No employees pass

    // Test 9: Quantified comparison in SELECT list
    let results = execute_select(&db,
        "SELECT name, salary > ALL (SELECT salary FROM employees WHERE dept_id = 1) AS is_highest FROM employees WHERE id = 5"
    ).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("Eve")));
    assert_eq!(results[0].values[1], SqlValue::Boolean(true)); // Eve's 90000 > all dept 1 salaries

    // Test 10: Complex query with AND
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ANY (SELECT salary FROM employees WHERE dept_id = 1) AND salary < ALL (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 2); // Bob (60000) and Charlie (70000)
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("Bob")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(std::sync::Arc::from("Charlie")));

    // Test 11: >= ALL
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary >= ALL (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 3); // Charlie, David, Eve (>= 70000)

    // Test 12: <= ANY
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary <= ANY (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 5); // All employees (<= 90000)

    // Test 13: != ALL (not equal to all values)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary <> ALL (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 2); // David and Eve (not in dept 1)
}
