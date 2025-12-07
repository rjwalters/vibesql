//! Tests for OLD and NEW pseudo-variable references in triggers

use crate::{CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor, UpdateExecutor};
use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

#[test]
fn test_new_in_insert_trigger() {
    // Test that NEW pseudo-variable works in INSERT triggers
    let mut db = Database::new();

    // Create employees table
    let create_table_sql = "CREATE TABLE employees (id INT, name VARCHAR(50), salary INT);";
    let stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create audit table
    let create_audit_sql = "CREATE TABLE audit (msg VARCHAR(200));";
    let stmt = vibesql_parser::Parser::parse_sql(create_audit_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create trigger that uses NEW to log inserted employee
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_new_employee".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "EMPLOYEES".to_string(), // Use uppercase to match parser normalization
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit (msg) VALUES (NEW.name);".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db).unwrap();

    // Insert a row
    let insert_sql = "INSERT INTO employees VALUES (1, 'Alice', 50000);";
    let stmt = vibesql_parser::Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Insert(stmt) => {
            InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        _ => panic!("Expected Insert"),
    }

    // Verify trigger logged the name from NEW
    let select_sql = "SELECT msg FROM audit;";
    let stmt = vibesql_parser::Parser::parse_sql(select_sql).unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(stmt) => {
            let executor = SelectExecutor::new(&db);
            executor.execute_with_columns(&stmt).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
}

#[test]
fn test_old_and_new_in_update_trigger() {
    // Test that both OLD and NEW pseudo-variables work in UPDATE triggers
    let mut db = Database::new();

    // Create employees table
    let create_table_sql = "CREATE TABLE employees (id INT, name VARCHAR(50), salary INT);";
    let stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Insert initial row
    let insert_sql = "INSERT INTO employees VALUES (1, 'Alice', 50000);";
    let stmt = vibesql_parser::Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Insert(stmt) => {
            InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        _ => panic!("Expected Insert"),
    }

    // Create audit table
    let create_audit_sql = "CREATE TABLE audit (old_salary INT, new_salary INT);";
    let stmt = vibesql_parser::Parser::parse_sql(create_audit_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create trigger that uses both OLD and NEW
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_salary_change".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Update(None), // No specific column list
        table_name: "EMPLOYEES".to_string(), // Use uppercase to match parser normalization
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit (old_salary, new_salary) VALUES (OLD.salary, NEW.salary);"
                .to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db).unwrap();

    // Update salary
    let update_sql = "UPDATE employees SET salary = 55000 WHERE id = 1;";
    let stmt = vibesql_parser::Parser::parse_sql(update_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Update(stmt) => {
            UpdateExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected Update"),
    }

    // Verify trigger logged both OLD and NEW salaries
    let select_sql = "SELECT old_salary, new_salary FROM audit;";
    let stmt = vibesql_parser::Parser::parse_sql(select_sql).unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(stmt) => {
            let executor = SelectExecutor::new(&db);
            executor.execute_with_columns(&stmt).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Integer(50000)); // OLD.salary
    assert_eq!(result.rows[0].values[1], vibesql_types::SqlValue::Integer(55000));
    // NEW.salary
}

#[test]
fn test_old_in_delete_trigger() {
    // Test that OLD pseudo-variable works in DELETE triggers
    let mut db = Database::new();

    // Create employees table
    let create_table_sql = "CREATE TABLE employees (id INT, name VARCHAR(50));";
    let stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Insert row
    let insert_sql = "INSERT INTO employees VALUES (1, 'Alice');";
    let stmt = vibesql_parser::Parser::parse_sql(insert_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Insert(stmt) => {
            InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        _ => panic!("Expected Insert"),
    }

    // Create audit table
    let create_audit_sql = "CREATE TABLE audit (deleted_name VARCHAR(50));";
    let stmt = vibesql_parser::Parser::parse_sql(create_audit_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create trigger that uses OLD
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_deletion".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Delete,
        table_name: "EMPLOYEES".to_string(), // Use uppercase to match parser normalization
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit (deleted_name) VALUES (OLD.name);".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db).unwrap();

    // Delete row
    let delete_sql = "DELETE FROM employees WHERE id = 1;";
    let stmt = vibesql_parser::Parser::parse_sql(delete_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Delete(stmt) => {
            DeleteExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected Delete"),
    }

    // Verify trigger logged the deleted name from OLD
    let select_sql = "SELECT deleted_name FROM audit;";
    let stmt = vibesql_parser::Parser::parse_sql(select_sql).unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(stmt) => {
            let executor = SelectExecutor::new(&db);
            executor.execute_with_columns(&stmt).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
}
