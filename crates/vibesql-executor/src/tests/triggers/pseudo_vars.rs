//! Tests for OLD and NEW pseudo-variable references in triggers

use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

use crate::{CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor, UpdateExecutor};

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
        if_not_exists: false,
        trigger_name: "log_new_employee".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "employees".to_string(), // Use lowercase to match parser normalization
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
    assert_eq!(
        result.rows[0].values[0],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))
    );
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
        if_not_exists: false,
        trigger_name: "log_salary_change".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Update(None), // No specific column list
        table_name: "employees".to_string(), // Use lowercase to match parser normalization
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
        if_not_exists: false,
        trigger_name: "log_deletion".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Delete,
        table_name: "employees".to_string(), // Use lowercase to match parser normalization
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
    assert_eq!(
        result.rows[0].values[0],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))
    );
}

/// Execute a single statement parsed from SQL (helper for IPK trigger tests).
fn exec(db: &mut Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap();
        }
        vibesql_ast::Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).unwrap();
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).unwrap();
        }
        vibesql_ast::Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).unwrap();
        }
        vibesql_ast::Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).unwrap();
        }
        other => panic!("Unsupported statement in test helper: {:?}", other),
    }
}

/// Read all rows of `SELECT id, v, rowid FROM audit ORDER BY id` as (id, v, rowid).
fn audit_rows(db: &Database) -> Vec<(i64, i64, i64)> {
    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT id, v, rowid FROM audit ORDER BY id;").unwrap();
    let result = match stmt {
        vibesql_ast::Statement::Select(s) => {
            SelectExecutor::new(db).execute_with_columns(&s).unwrap()
        }
        _ => panic!("Expected Select"),
    };
    result
        .rows
        .iter()
        .map(|r| {
            let as_i64 = |v: &vibesql_types::SqlValue| match v {
                vibesql_types::SqlValue::Integer(i) => *i,
                vibesql_types::SqlValue::Bigint(i) => *i,
                other => panic!("expected integer, got {:?}", other),
            };
            (as_i64(&r.values[0]), as_i64(&r.values[1]), as_i64(&r.values[2]))
        })
        .collect()
}

/// Regression for #5397: a parsed trigger body inserting NEW.col into an
/// INTEGER PRIMARY KEY column must resolve the pseudo-variable. Previously the
/// IPK value-evaluation path used a bare evaluator without trigger context and
/// failed with "Pseudo-variable NEW.id is only valid within trigger bodies".
///
/// Verified against sqlite3 3.51.0: NEW.id lands as both the audit.id value and
/// the audit rowid.
#[test]
fn test_new_into_ipk_column_in_insert_trigger() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);");
    exec(&mut db, "CREATE TABLE audit (id INTEGER PRIMARY KEY, v INTEGER);");
    exec(
        &mut db,
        "CREATE TRIGGER trg AFTER INSERT ON t \
         BEGIN INSERT INTO audit (id, v) VALUES (NEW.id, NEW.v); END;",
    );

    exec(&mut db, "INSERT INTO t VALUES (1, 42);");
    exec(&mut db, "INSERT INTO t VALUES (7, 99);");

    // NEW.id is the IPK value AND the rowid (matches sqlite3 3.51.0).
    assert_eq!(audit_rows(&db), vec![(1, 42, 1), (7, 99, 7)]);
}

/// Regression for #5397: OLD.col into an INTEGER PRIMARY KEY column from a
/// DELETE trigger body. Verified against sqlite3 3.51.0.
#[test]
fn test_old_into_ipk_column_in_delete_trigger() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);");
    exec(&mut db, "CREATE TABLE audit (id INTEGER PRIMARY KEY, v INTEGER);");
    exec(
        &mut db,
        "CREATE TRIGGER trg_del AFTER DELETE ON t \
         BEGIN INSERT INTO audit (id, v) VALUES (OLD.id, OLD.v); END;",
    );

    exec(&mut db, "INSERT INTO t VALUES (5, 500);");
    exec(&mut db, "INSERT INTO t VALUES (8, 800);");
    exec(&mut db, "DELETE FROM t WHERE id = 5;");

    // OLD.id is the IPK value AND the rowid (matches sqlite3 3.51.0).
    assert_eq!(audit_rows(&db), vec![(5, 500, 5)]);
}

/// Regression for #5397: OLD.col / NEW.col into an INTEGER PRIMARY KEY column
/// from an UPDATE trigger body. Verified against sqlite3 3.51.0.
#[test]
fn test_old_and_new_into_ipk_column_in_update_trigger() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);");
    // audit.id is INTEGER PRIMARY KEY and receives NEW.v as its rowid.
    exec(&mut db, "CREATE TABLE audit (id INTEGER PRIMARY KEY, v INTEGER);");
    exec(
        &mut db,
        "CREATE TRIGGER trg_upd AFTER UPDATE ON t \
         BEGIN INSERT INTO audit (id, v) VALUES (NEW.v, OLD.v); END;",
    );

    exec(&mut db, "INSERT INTO t VALUES (1, 100);");
    exec(&mut db, "UPDATE t SET v = 200 WHERE id = 1;");

    // NEW.v (200) becomes the audit IPK/rowid; OLD.v (100) is the value column.
    assert_eq!(audit_rows(&db), vec![(200, 100, 200)]);
}
