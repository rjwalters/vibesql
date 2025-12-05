#![no_main]

//! Query executor fuzz target
//!
//! This fuzzer exercises the full query execution pipeline:
//! - Parser → AST → Executor → Results
//!
//! Unlike the parser-only fuzzer, this catches bugs in:
//! - Query planning and optimization
//! - Expression evaluation
//! - Type coercion during execution
//! - Memory management in execution pipelines
//!
//! The fuzzer covers SELECT, INSERT, UPDATE, and DELETE statements using
//! transaction rollback to maintain database isolation between fuzz iterations.

use libfuzzer_sys::fuzz_target;
use std::sync::{Arc, Mutex, OnceLock};

// Import the vibesql crates
use vibesql_executor::{DeleteExecutor, InsertExecutor, SelectExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Get a mutable reference to the shared test database
fn get_test_database() -> Arc<Mutex<Database>> {
    static DB: OnceLock<Arc<Mutex<Database>>> = OnceLock::new();
    DB.get_or_init(|| Arc::new(Mutex::new(create_test_database())))
        .clone()
}

fn create_test_database() -> Database {
    let mut db = Database::new();

    // Setup queries to create test schema
    let setup_queries = [
        // Simple table for basic queries
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name VARCHAR(100), value NUMERIC)",
        "INSERT INTO t1 VALUES (1, 'one', 1.0)",
        "INSERT INTO t1 VALUES (2, 'two', 2.5)",
        "INSERT INTO t1 VALUES (3, 'three', 3.14159)",
        // Table with various types
        "CREATE TABLE types (i INTEGER, b BOOLEAN, d DATE, t TIME, ts TIMESTAMP, v VARCHAR(255))",
        "INSERT INTO types VALUES (1, TRUE, '2024-01-15', '10:30:00', '2024-01-15 10:30:00', 'test')",
        "INSERT INTO types VALUES (2, FALSE, '2024-06-20', '14:45:30', '2024-06-20 14:45:30', 'hello')",
        "INSERT INTO types VALUES (NULL, NULL, NULL, NULL, NULL, NULL)",
        // Tables for join testing
        "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)",
        "INSERT INTO orders VALUES (1, 1, 100.00)",
        "INSERT INTO orders VALUES (2, 1, 50.00)",
        "INSERT INTO orders VALUES (3, 2, 75.25)",
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(100))",
        "INSERT INTO customers VALUES (1, 'Alice')",
        "INSERT INTO customers VALUES (2, 'Bob')",
        // Table with nulls for NULL handling tests
        "CREATE TABLE nulls (a INTEGER, b INTEGER, c VARCHAR(50))",
        "INSERT INTO nulls VALUES (1, 2, 'both')",
        "INSERT INTO nulls VALUES (1, NULL, 'b_null')",
        "INSERT INTO nulls VALUES (NULL, 2, 'a_null')",
        "INSERT INTO nulls VALUES (NULL, NULL, 'both_null')",
    ];

    for sql in setup_queries {
        execute_sql(&mut db, sql);
    }

    db
}

/// Execute a single SQL statement against the database
fn execute_sql(db: &mut Database, sql: &str) {
    if let Ok(stmt) = Parser::parse_sql(sql) {
        match &stmt {
            vibesql_ast::Statement::CreateTable(create) => {
                let _ = vibesql_executor::CreateTableExecutor::execute(create, db);
            }
            vibesql_ast::Statement::Insert(insert) => {
                let _ = InsertExecutor::execute(db, insert);
            }
            _ => {}
        }
    }
}

fuzz_target!(|data: &[u8]| {
    // Convert to UTF-8 string
    let sql = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Skip empty or very long inputs
    if sql.is_empty() || sql.len() > 10_000 {
        return;
    }

    // Parse the SQL
    let stmt = match Parser::parse_sql(sql) {
        Ok(stmt) => stmt,
        Err(_) => return, // Parse errors are expected, not crashes
    };

    // Get our test database
    let db_arc = get_test_database();
    let mut db = match db_arc.lock() {
        Ok(guard) => guard,
        Err(_) => return, // Poisoned lock, skip this iteration
    };

    // Execute different statement types
    match &stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            // SELECT doesn't modify state, no transaction needed
            let executor = SelectExecutor::new(&*db);
            let _ = executor.execute(select_stmt);
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            // BEGIN transaction to isolate INSERT
            if db.begin_transaction().is_ok() {
                let _ = InsertExecutor::execute(&mut *db, insert_stmt);
                // ROLLBACK to restore original state
                let _ = db.rollback_transaction();
            }
        }
        vibesql_ast::Statement::Update(update_stmt) => {
            // BEGIN transaction to isolate UPDATE
            if db.begin_transaction().is_ok() {
                let _ = UpdateExecutor::execute(update_stmt, &mut *db);
                // ROLLBACK to restore original state
                let _ = db.rollback_transaction();
            }
        }
        vibesql_ast::Statement::Delete(delete_stmt) => {
            // BEGIN transaction to isolate DELETE
            if db.begin_transaction().is_ok() {
                let _ = DeleteExecutor::execute(delete_stmt, &mut *db);
                // ROLLBACK to restore original state
                let _ = db.rollback_transaction();
            }
        }
        _ => {
            // Skip other statement types (DDL, etc.)
        }
    }
});
