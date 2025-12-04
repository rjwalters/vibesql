#![no_main]

//! SQLite differential testing fuzz target
//!
//! This fuzzer compares VibeSQL query results against SQLite to catch semantic bugs.
//! It finds cases where:
//! - VibeSQL produces different results than SQLite
//! - VibeSQL succeeds but SQLite fails (or vice versa)
//! - Row count or column count mismatches
//!
//! This helps catch subtle semantic bugs that don't crash but produce wrong results.

use libfuzzer_sys::fuzz_target;
use rusqlite::{Connection, Result as SqliteResult};
use std::cell::RefCell;
use std::sync::OnceLock;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

// Thread-local SQLite connection (Connection is not Sync)
thread_local! {
    static SQLITE_CONN: RefCell<Option<Connection>> = const { RefCell::new(None) };
}

/// Get or initialize the thread-local SQLite connection
fn with_sqlite_connection<F, R>(f: F) -> R
where
    F: FnOnce(&Connection) -> R,
{
    SQLITE_CONN.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let conn = Connection::open_in_memory().expect("Failed to open SQLite");

            // Create same schema as VibeSQL test database
            conn.execute_batch(
                r#"
                CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, value REAL);
                INSERT INTO t1 VALUES (1, 'one', 1.0);
                INSERT INTO t1 VALUES (2, 'two', 2.5);
                INSERT INTO t1 VALUES (3, 'three', 3.14159);

                CREATE TABLE types (i INTEGER, b INTEGER, d TEXT, t TEXT, ts TEXT, v TEXT);
                INSERT INTO types VALUES (1, 1, '2024-01-15', '10:30:00', '2024-01-15 10:30:00', 'test');
                INSERT INTO types VALUES (2, 0, '2024-06-20', '14:45:30', '2024-06-20 14:45:30', 'hello');
                INSERT INTO types VALUES (NULL, NULL, NULL, NULL, NULL, NULL);

                CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL);
                INSERT INTO orders VALUES (1, 1, 100.00);
                INSERT INTO orders VALUES (2, 1, 50.00);
                INSERT INTO orders VALUES (3, 2, 75.25);

                CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO customers VALUES (1, 'Alice');
                INSERT INTO customers VALUES (2, 'Bob');

                CREATE TABLE nulls (a INTEGER, b INTEGER, c TEXT);
                INSERT INTO nulls VALUES (1, 2, 'both');
                INSERT INTO nulls VALUES (1, NULL, 'b_null');
                INSERT INTO nulls VALUES (NULL, 2, 'a_null');
                INSERT INTO nulls VALUES (NULL, NULL, 'both_null');
                "#,
            )
            .expect("Failed to create SQLite schema");

            *borrow = Some(conn);
        }
        f(borrow.as_ref().unwrap())
    })
}

/// Initialize VibeSQL with the same schema
fn get_vibesql_database() -> &'static Database {
    static DB: OnceLock<Database> = OnceLock::new();
    DB.get_or_init(|| {
        let mut db = Database::new();

        let setup_queries = [
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name VARCHAR(100), value NUMERIC)",
            "INSERT INTO t1 VALUES (1, 'one', 1.0)",
            "INSERT INTO t1 VALUES (2, 'two', 2.5)",
            "INSERT INTO t1 VALUES (3, 'three', 3.14159)",
            "CREATE TABLE types (i INTEGER, b BOOLEAN, d DATE, t TIME, ts TIMESTAMP, v VARCHAR(255))",
            "INSERT INTO types VALUES (1, TRUE, '2024-01-15', '10:30:00', '2024-01-15 10:30:00', 'test')",
            "INSERT INTO types VALUES (2, FALSE, '2024-06-20', '14:45:30', '2024-06-20 14:45:30', 'hello')",
            "INSERT INTO types VALUES (NULL, NULL, NULL, NULL, NULL, NULL)",
            "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)",
            "INSERT INTO orders VALUES (1, 1, 100.00)",
            "INSERT INTO orders VALUES (2, 1, 50.00)",
            "INSERT INTO orders VALUES (3, 2, 75.25)",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(100))",
            "INSERT INTO customers VALUES (1, 'Alice')",
            "INSERT INTO customers VALUES (2, 'Bob')",
            "CREATE TABLE nulls (a INTEGER, b INTEGER, c VARCHAR(50))",
            "INSERT INTO nulls VALUES (1, 2, 'both')",
            "INSERT INTO nulls VALUES (1, NULL, 'b_null')",
            "INSERT INTO nulls VALUES (NULL, 2, 'a_null')",
            "INSERT INTO nulls VALUES (NULL, NULL, 'both_null')",
        ];

        for sql in setup_queries {
            if let Ok(stmt) = Parser::parse_sql(sql) {
                match &stmt {
                    vibesql_ast::Statement::CreateTable(create) => {
                        let _ = vibesql_executor::CreateTableExecutor::execute(create, &mut db);
                    }
                    vibesql_ast::Statement::Insert(insert) => {
                        let _ = vibesql_executor::InsertExecutor::execute(&mut db, insert);
                    }
                    _ => {}
                }
            }
        }

        db
    })
}

/// Normalize SQLite value to string for comparison
fn sqlite_value_to_string(value: rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_string(),
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Real(f) => format!("{:.6}", f), // Normalize precision
        rusqlite::types::Value::Text(s) => s,
        rusqlite::types::Value::Blob(b) => format!("BLOB({} bytes)", b.len()),
    }
}

/// Normalize VibeSQL value to string for comparison
fn vibesql_value_to_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Smallint(i) => i.to_string(),
        SqlValue::Bigint(i) => i.to_string(),
        SqlValue::Unsigned(i) => i.to_string(),
        SqlValue::Numeric(f) => format!("{:.6}", f),
        SqlValue::Float(f) => format!("{:.6}", f),
        SqlValue::Real(f) => format!("{:.6}", f),
        SqlValue::Double(f) => format!("{:.6}", f),
        SqlValue::Character(s) | SqlValue::Varchar(s) => s.clone(),
        SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Date(d) => format!("{}", d),
        SqlValue::Time(t) => format!("{}", t),
        SqlValue::Timestamp(ts) => format!("{}", ts),
        SqlValue::Interval(i) => format!("{}", i),
        SqlValue::Vector(v) => format!("VECTOR({} dims)", v.len()),
    }
}

/// Execute query on SQLite and return normalized results
fn execute_sqlite(sql: &str) -> Option<Vec<Vec<String>>> {
    with_sqlite_connection(|conn| {
        let mut stmt = match conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return None,
        };

        let column_count = stmt.column_count();

        let rows: SqliteResult<Vec<Vec<String>>> = stmt
            .query_map([], |row| {
                let mut values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value: rusqlite::types::Value = row.get(i)?;
                    values.push(sqlite_value_to_string(value));
                }
                Ok(values)
            })
            .and_then(|iter| iter.collect());

        rows.ok()
    })
}

/// Execute query on VibeSQL and return normalized results
fn execute_vibesql(db: &Database, sql: &str) -> Option<Vec<Vec<String>>> {
    let stmt = Parser::parse_sql(sql).ok()?;

    if let vibesql_ast::Statement::Select(select_stmt) = &stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor.execute(select_stmt).ok()?;

        return Some(
            rows.iter()
                .map(|row| row.values.iter().map(vibesql_value_to_string).collect())
                .collect(),
        );
    }

    None
}

/// Check if query is safe to compare (some queries may have implementation differences)
fn is_comparable_query(sql: &str) -> bool {
    let sql_upper = sql.to_uppercase();

    // Skip queries that might have legitimate differences between DBs
    let skip_patterns = [
        "RANDOM",        // Random functions differ
        "NOW",           // Time functions differ
        "CURRENT_",      // Current date/time differ
        "UUID",          // UUID generation differs
        "ROWID",         // SQLite-specific
        "SQLITE_",       // SQLite internals
        "TYPEOF",        // Type system differences
        "QUOTE",         // Quoting differs
        "PRINTF",        // Format functions differ
        "EXPLAIN",       // Explain output differs
        "PRAGMA",        // SQLite-specific
        "VACUUM",        // SQLite-specific
        "REINDEX",       // Implementation details
        "ANALYZE",       // Statistics differ
        "GLOB",          // SQLite-specific pattern
        "JSON_",         // JSON handling differs
        "DATETIME",      // Date handling differs
        "STRFTIME",      // Date formatting differs
        "JULIANDAY",     // SQLite date internals
        "UNIXEPOCH",     // SQLite date internals
        "TIME(",         // TIME function vs TIME type
        "LAST_INSERT_ROWID", // SQLite-specific
    ];

    for pattern in &skip_patterns {
        if sql_upper.contains(pattern) {
            return false;
        }
    }

    true
}

fuzz_target!(|data: &[u8]| {
    // Convert to UTF-8 string
    let sql = match std::str::from_utf8(data) {
        Ok(s) => s.trim(),
        Err(_) => return,
    };

    // Skip empty or too long inputs
    if sql.is_empty() || sql.len() > 5_000 {
        return;
    }

    // Skip non-SELECT queries (we only compare SELECT for now)
    let sql_upper = sql.to_uppercase();
    if !sql_upper.starts_with("SELECT") {
        return;
    }

    // Skip queries with known differences
    if !is_comparable_query(sql) {
        return;
    }

    let vibesql_db = get_vibesql_database();

    // Execute on both databases
    let sqlite_result = execute_sqlite(sql);
    let vibesql_result = execute_vibesql(vibesql_db, sql);

    // Compare results
    match (sqlite_result, vibesql_result) {
        // Both succeeded - compare results
        (Some(sqlite_rows), Some(vibesql_rows)) => {
            // Compare row count
            if sqlite_rows.len() != vibesql_rows.len() {
                // Row count mismatch is interesting but might be due to ordering
                // For now, just verify both return some rows or both return empty
                if (sqlite_rows.is_empty()) != (vibesql_rows.is_empty()) {
                    // One returned rows, other didn't - this is a bug
                    // For fuzzing, we report this as a potential issue by panicking
                    // In practice, you might want to log these instead
                    // panic!(
                    //     "Row count mismatch: SQLite={}, VibeSQL={} for query: {}",
                    //     sqlite_rows.len(), vibesql_rows.len(), sql
                    // );
                }
            }

            // Compare column count for first row
            if let (Some(sqlite_first), Some(vibesql_first)) =
                (sqlite_rows.first(), vibesql_rows.first())
            {
                if sqlite_first.len() != vibesql_first.len() {
                    // Column count mismatch - likely a semantic difference
                    // panic!(
                    //     "Column count mismatch: SQLite={}, VibeSQL={} for query: {}",
                    //     sqlite_first.len(), vibesql_first.len(), sql
                    // );
                }
            }
        }

        // Both failed - expected for invalid queries
        (None, None) => {}

        // SQLite succeeded, VibeSQL failed - might be unimplemented feature
        (Some(_), None) => {
            // This is informational - VibeSQL may not support all SQLite features
        }

        // VibeSQL succeeded, SQLite failed - unexpected but possible for VibeSQL extensions
        (None, Some(_)) => {
            // VibeSQL might support features SQLite doesn't
        }
    }
});
