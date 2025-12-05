//! Tests for column name derivation in SELECT queries

use vibesql_executor::SelectExecutor;

fn create_test_database() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    // Create employees table
    let create_stmt = vibesql_parser::Parser::parse_sql(
        "CREATE TABLE employees (
            id INTEGER,
            name VARCHAR(100),
            department VARCHAR(50),
            salary INTEGER
        )",
    )
    .unwrap();

    if let vibesql_ast::Statement::CreateTable(create_table) = create_stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
    }

    // Insert some test data
    let insert_stmt = vibesql_parser::Parser::parse_sql(
        "INSERT INTO employees (id, name, department, salary) VALUES
        (1, 'John Smith', 'Engineering', 75000),
        (2, 'Jane Doe', 'Sales', 82000),
        (3, 'Bob Wilson', 'Engineering', 68000)",
    )
    .unwrap();

    if let vibesql_ast::Statement::Insert(insert) = insert_stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert).unwrap();
    }

    db
}

#[test]
fn test_column_names_simple_select() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT id, name FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns, vec!["ID", "NAME"]);
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_with_alias() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT name AS employee_name, salary AS annual_salary FROM employees",
    )
    .unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns, vec!["EMPLOYEE_NAME", "ANNUAL_SALARY"]);
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_star_expansion() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT * FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns, vec!["ID", "NAME", "DEPARTMENT", "SALARY"]);
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_functions() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT COUNT(*), AVG(salary) FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert!(result.columns[0].contains("COUNT"));
        assert!(result.columns[1].contains("AVG"));
        assert_eq!(result.rows.len(), 1);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_expressions() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT salary * 12 FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns.len(), 1);
        // Expression should have a generated name
        assert!(result.columns[0].contains("salary") || result.columns[0].contains("*"));
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_mixed() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT id, name AS emp_name, salary * 12 FROM employees",
    )
    .unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0], "ID");
        assert_eq!(result.columns[1], "EMP_NAME");
        // Third column is an expression
        assert!(result.columns[2].contains("salary") || result.columns[2].contains("*"));
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_function_with_alias() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT COUNT(*) AS total_employees FROM employees")
            .unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns, vec!["TOTAL_EMPLOYEES"]);
        assert_eq!(result.rows.len(), 1);
    } else {
        panic!("Expected SELECT statement");
    }
}
