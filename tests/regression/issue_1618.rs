//! Reproduction test for issue #1618: Index scans return 0 rows after database pooling
//!
//! This test mimics the SQLLogicTest runner's approach:
//! 1. Thread-local database pooling with reset() between test cycles
//! 2. Using InsertExecutor and SelectExecutor (not direct database calls)
//! 3. Critical pattern: empty table → create index → INSERT...SELECT → query

use std::cell::RefCell;

use vibesql_executor::{InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

thread_local! {
    static EXECUTOR_TEST_POOL: RefCell<Option<Database>> = const { RefCell::new(None) };
}

fn get_pooled_database() -> Database {
    EXECUTOR_TEST_POOL.with(|pool| {
        let mut pool_ref = pool.borrow_mut();
        match pool_ref.take() {
            Some(mut db) => {
                eprintln!("  [POOL] Reusing pooled database after reset");
                db.reset();
                db
            }
            None => {
                eprintln!("  [POOL] Creating fresh database");
                Database::new()
            }
        }
    })
}

fn return_to_pool(db: Database) {
    EXECUTOR_TEST_POOL.with(|pool| {
        let mut pool_ref = pool.borrow_mut();
        if pool_ref.is_none() {
            eprintln!("  [POOL] Returning database to pool");
            *pool_ref = Some(db);
        }
    });
}

/// Execute SQL using the executor layer (like SQLLogicTest does)
fn execute_sql(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            vibesql_executor::CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| format!("CreateTable error: {:?}", e))?;
            Ok(0)
        }
        vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
            vibesql_executor::IndexExecutor::execute(&create_index_stmt, db)
                .map_err(|e| format!("CreateIndex error: {:?}", e))?;
            Ok(0)
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            let rows = InsertExecutor::execute(db, &insert_stmt)
                .map_err(|e| format!("Insert error: {:?}", e))?;
            Ok(rows)
        }
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            let rows =
                executor.execute(&select_stmt).map_err(|e| format!("Select error: {:?}", e))?;
            Ok(rows.len())
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

fn run_test_cycle(cycle_num: usize) -> Result<(), String> {
    eprintln!("\n=== Cycle {} ===", cycle_num);

    let mut db = get_pooled_database();

    // Step 1: Create source table with data
    execute_sql(&mut db, "CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER)")?;
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (1, 100)")?;
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (2, 200)")?;
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (3, 300)")?;
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (4, 400)")?;
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (5, 500)")?;
    eprintln!("  Created tab0 with 5 rows");

    // Step 2: Create empty target table
    execute_sql(&mut db, "CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER)")?;
    eprintln!("  Created empty tab1");

    // Step 3: Create index on EMPTY table (this is the critical pattern!)
    execute_sql(&mut db, "CREATE INDEX idx_col0 ON tab1 (col0)")?;
    eprintln!("  Created index on EMPTY tab1");

    // Step 4: Insert data via INSERT...SELECT using executor
    let inserted = execute_sql(&mut db, "INSERT INTO tab1 SELECT * FROM tab0")?;
    eprintln!("  Inserted {} rows via INSERT...SELECT", inserted);

    // Step 4a: Diagnostic - Check total rows in tab1
    let total_rows = execute_sql(&mut db, "SELECT * FROM tab1")?;
    eprintln!("  [DIAG] Total rows in tab1 (via SELECT *): {}", total_rows);

    // Step 4b: Diagnostic - Check index data directly via storage layer
    eprintln!("  [DIAG] Checking index contents at storage layer...");
    if let Some(index_data) = db.get_index_data("idx_col0") {
        eprintln!("  [DIAG] Index 'idx_col0' exists: {:?}", index_data);
    } else {
        eprintln!("  [DIAG] WARNING: Index 'idx_col0' not found!");
    }

    // Step 5: Query using index (through executor)
    let result_count = execute_sql(&mut db, "SELECT pk FROM tab1 WHERE col0 > 250")?;
    eprintln!("  Query WITH WHERE returned {} rows (expected 3)", result_count);

    // Return to pool
    return_to_pool(db);

    if result_count != 3 {
        return Err(format!(
            "Cycle {}: Expected 3 rows, got {}. This reproduces issue #1618!",
            cycle_num, result_count
        ));
    }

    eprintln!("  ✓ Cycle {} PASSED", cycle_num);
    Ok(())
}

#[test]
fn test_executor_with_pooling() {
    // Cycle 1: Fresh database (should PASS)
    run_test_cycle(1).expect("Cycle 1 should pass (fresh database)");

    // Cycle 2: After reset (should FAIL if bug exists)
    match run_test_cycle(2) {
        Ok(()) => {
            eprintln!("\n✅ Cycle 2 passed! The bug may have been fixed.");
        }
        Err(e) => {
            panic!("\n❌ REPRODUCED ISSUE #1618:\n{}", e);
        }
    }

    // Cycle 3: One more time to be sure
    match run_test_cycle(3) {
        Ok(()) => {
            eprintln!("\n✅ Cycle 3 passed!");
        }
        Err(e) => {
            panic!("\n❌ STILL FAILING ON CYCLE 3:\n{}", e);
        }
    }
}
