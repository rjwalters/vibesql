//! Tests for CREATE VIEW and DROP VIEW (SQL:1999 views)

use vibesql_ast::Statement;
use vibesql_executor::advanced_objects::{execute_create_view, execute_drop_view};
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_create_view_simple() {
    let mut db = Database::new();

    // Create view - note: this will derive column names even though table doesn't exist
    // In practice, views require the table to exist at creation time
    let sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE VIEW");

    match stmt {
        Statement::CreateView(view_stmt) => {
            // This will fail because table doesn't exist, but that's expected
            let result = execute_create_view(&view_stmt, &mut db);

            // View creation should fail when table doesn't exist
            assert!(result.is_err(), "Creating view from non-existent table should fail");
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_create_view_with_column_list() {
    let mut db = Database::new();

    let sql = "CREATE VIEW emp_view (employee_id, employee_name) AS SELECT id, name FROM employees";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::CreateView(view_stmt) => {
            execute_create_view(&view_stmt, &mut db).expect("Failed to create view");

            let view = db.catalog.get_view("emp_view");
            assert!(view.is_some());

            let view = view.unwrap();
            assert_eq!(view.name, "emp_view");
            assert!(view.columns.is_some());

            let cols = view.columns.as_ref().unwrap();
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0], "employee_id");
            assert_eq!(cols[1], "employee_name");
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_create_view_with_check_option() {
    let mut db = Database::new();

    let sql =
        "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true WITH CHECK OPTION";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::CreateView(view_stmt) => {
            // This will fail because table doesn't exist
            let result = execute_create_view(&view_stmt, &mut db);
            assert!(result.is_err(), "Creating view from non-existent table should fail");

            // Verify the WITH CHECK OPTION flag is properly parsed
            assert!(view_stmt.with_check_option, "with_check_option should be true");
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_drop_view_simple() {
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::DataType;

    let mut db = Database::new();

    // Create underlying table first
    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            default_value: None,
            generated_expr: None,
            is_exact_integer_type: true,
            collation: None,
        }],
    );
    db.create_table(users_schema).expect("Failed to create table");

    // Create view
    let sql = "CREATE VIEW my_view AS SELECT * FROM users";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(view_stmt) = stmt {
        execute_create_view(&view_stmt, &mut db).expect("Failed to create view");
    }

    // Verify view exists
    assert!(db.catalog.get_view("my_view").is_some());

    // Drop view
    let sql = "DROP VIEW my_view";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP VIEW");

    match stmt {
        Statement::DropView(drop_stmt) => {
            execute_drop_view(&drop_stmt, &mut db).expect("Failed to drop view");

            // Verify view no longer exists
            assert!(db.catalog.get_view("my_view").is_none());
        }
        _ => panic!("Expected DropView statement"),
    }
}

#[test]
fn test_create_view_duplicate_error() {
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::DataType;

    let mut db = Database::new();

    // Create underlying tables first
    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            default_value: None,
            generated_expr: None,
            is_exact_integer_type: true,
            collation: None,
        }],
    );
    db.create_table(users_schema).expect("Failed to create users table");

    let employees_schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            default_value: None,
            generated_expr: None,
            is_exact_integer_type: true,
            collation: None,
        }],
    );
    db.create_table(employees_schema).expect("Failed to create employees table");

    // Create first view
    let sql = "CREATE VIEW my_view AS SELECT * FROM users";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(view_stmt) = stmt {
        execute_create_view(&view_stmt, &mut db).expect("Failed to create view");
    }

    // Try to create duplicate view
    let sql = "CREATE VIEW my_view AS SELECT * FROM employees";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::CreateView(view_stmt) => {
            let result = execute_create_view(&view_stmt, &mut db);
            assert!(result.is_err(), "Should fail when creating duplicate view");

            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("already exists"), "Error should mention view already exists");
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_drop_view_not_found_error() {
    let mut db = Database::new();

    let sql = "DROP VIEW nonexistent_view";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::DropView(drop_stmt) => {
            let result = execute_drop_view(&drop_stmt, &mut db);
            assert!(result.is_err(), "Should fail when dropping non-existent view");

            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("not found"), "Error should mention view not found");
        }
        _ => panic!("Expected DropView statement"),
    }
}

#[test]
fn test_drop_view_if_exists_nonexistent() {
    let mut db = Database::new();

    // DROP VIEW IF EXISTS should succeed even if view doesn't exist
    let sql = "DROP VIEW IF EXISTS nonexistent_view";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    match stmt {
        Statement::DropView(drop_stmt) => {
            assert!(drop_stmt.if_exists, "if_exists flag should be true");

            let result = execute_drop_view(&drop_stmt, &mut db);
            assert!(result.is_ok(), "DROP VIEW IF EXISTS should succeed for non-existent view");
        }
        _ => panic!("Expected DropView statement"),
    }
}

#[test]
fn test_view_preserves_column_names_with_select_star() {
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::DataType;

    let mut db = Database::new();

    // Manually create a table with specific column names
    let table_schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![
            ColumnSchema {
                name: "pk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: true,
                collation: None,
            },
            ColumnSchema {
                name: "col0".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: true,
                collation: None,
            },
            ColumnSchema {
                name: "col1".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: true,
                collation: None,
            },
        ],
    );
    db.create_table(table_schema).expect("Failed to create table");

    // Create view with SELECT *
    let create_view_sql = "CREATE VIEW my_view AS SELECT * FROM tab0";
    let stmt = Parser::parse_sql(create_view_sql).expect("Failed to parse CREATE VIEW");

    match stmt {
        Statement::CreateView(view_stmt) => {
            execute_create_view(&view_stmt, &mut db).expect("Failed to create view");

            // Verify view has columns stored
            let view = db.catalog.get_view("my_view").expect("View should exist");
            assert!(view.columns.is_some(), "View should have columns defined");

            let cols = view.columns.as_ref().unwrap();
            assert_eq!(cols.len(), 3, "View should have 3 columns");
            assert_eq!(cols[0], "pk", "First column should be PK");
            assert_eq!(cols[1], "col0", "Second column should be COL0");
            assert_eq!(cols[2], "col1", "Third column should be COL1");
        }
        _ => panic!("Expected CreateView statement"),
    }
}
