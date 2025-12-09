//! Sysbench Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for sysbench
//! benchmark tables across multiple database engines (VibeSQL, SQLite, DuckDB, MySQL).

use super::data::SysbenchData;

// =============================================================================
// SQL Statement Constants
// =============================================================================
// Note: Column is named "padding" instead of "pad" because PAD is a SQL keyword.
// These constants ensure consistency across all engines.

/// Insert statement for sysbench table (parameterized)
/// Parameters: id, k, c, padding
pub const INSERT_SQL: &str = "INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)";

/// Insert statement for SQLite/DuckDB (numbered parameters)
#[cfg(any(feature = "sqlite-comparison", feature = "duckdb-comparison"))]
pub const INSERT_SQL_NUMBERED: &str =
    "INSERT INTO sbtest1 (id, k, c, padding) VALUES (?1, ?2, ?3, ?4)";

use vibesql_storage::Database as VibeDB;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::{Pool, PooledConn};
#[cfg(feature = "sqlite-comparison")]
use rusqlite::Connection as SqliteConn;

// =============================================================================
// Database Loaders
// =============================================================================

/// Load VibeSQL with sysbench schema and data
///
/// # Arguments
/// * `table_size` - Number of rows to generate (default: 10,000)
pub fn load_vibesql(table_size: usize) -> VibeDB {
    let mut db = VibeDB::new();
    let mut data = SysbenchData::new(table_size);

    // Create schema
    create_sbtest_schema_vibesql(&mut db);

    // Load data
    load_sbtest_vibesql(&mut db, &mut data);

    // Create indexes (primary key + secondary index on k)
    create_sbtest_indexes_vibesql(&mut db);

    // Compute statistics for query optimization
    if let Some(table) = db.get_table_mut("SBTEST1") {
        table.analyze();
    }

    db
}

/// Load SQLite with sysbench schema and data
#[cfg(feature = "sqlite-comparison")]
pub fn load_sqlite(table_size: usize) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = SysbenchData::new(table_size);

    // Create schema
    create_sbtest_schema_sqlite(&conn);

    // Load data
    load_sbtest_sqlite(&conn, &mut data);

    // Compute statistics for query optimization (ensures fair comparison with VibeSQL)
    conn.execute("ANALYZE", []).unwrap();

    conn
}

/// Load DuckDB with sysbench schema and data
#[cfg(feature = "duckdb-comparison")]
pub fn load_duckdb(table_size: usize) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = SysbenchData::new(table_size);

    // Create schema
    create_sbtest_schema_duckdb(&conn);

    // Load data
    load_sbtest_duckdb(&conn, &mut data);

    // Compute statistics for query optimization (ensures fair comparison with VibeSQL)
    conn.execute_batch("ANALYZE").unwrap();

    conn
}

/// Load MySQL with sysbench schema and data
/// Requires MYSQL_URL environment variable (e.g., mysql://root:password@localhost:3306/sysbench)
/// Returns None if MYSQL_URL is not set or connection fails
#[cfg(feature = "mysql-comparison")]
pub fn load_mysql(table_size: usize) -> Option<PooledConn> {
    let url = std::env::var("MYSQL_URL").ok()?;
    let pool = Pool::new(url.as_str()).ok()?;
    let mut conn = pool.get_conn().ok()?;

    // Disable HeatWave secondary engine to avoid errors on complex queries
    let _ = conn.query_drop("SET SESSION use_secondary_engine=OFF");

    let mut data = SysbenchData::new(table_size);

    // Create schema (drops and recreates table)
    create_sbtest_schema_mysql(&mut conn);

    // Load data
    load_sbtest_mysql(&mut conn, &mut data);

    // Compute statistics for query optimization (ensures fair comparison with VibeSQL)
    conn.query_drop("ANALYZE TABLE sbtest1").unwrap();

    Some(conn)
}

// =============================================================================
// Schema Creation - VibeSQL
// =============================================================================

fn create_sbtest_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // sbtest1 table (standard sysbench OLTP table)
    // Using Varchar for string columns for compatibility with SQL UPDATE statements
    // (the SQL parser produces SqlValue::Varchar for string literals)
    // Note: default values are handled at insert time, not schema level
    // Use with_primary_key to create a proper PK constraint for efficient lookups
    db.create_table(TableSchema::with_primary_key(
        "SBTEST1".to_string(),
        vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "k".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "c".to_string(),
                data_type: DataType::Varchar { max_length: Some(120) },
                nullable: false,
                default_value: None,
            },
            // Note: renamed from "pad" to "padding" because PAD is a SQL keyword
            ColumnSchema {
                name: "padding".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: false,
                default_value: None,
            },
        ],
        vec!["id".to_string()], // PRIMARY KEY on id column
    ))
    .unwrap();
}

fn create_sbtest_indexes_vibesql(db: &mut VibeDB) {
    use vibesql_ast::{IndexColumn, OrderDirection};

    // Primary key index on id column
    db.create_index(
        "idx_sbtest1_pk".to_string(),
        "SBTEST1".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Secondary index on k column (k_1)
    db.create_index(
        "k_1".to_string(),
        "SBTEST1".to_string(),
        false, // not unique
        vec![IndexColumn {
            column_name: "k".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
}

fn load_sbtest_vibesql(db: &mut VibeDB, data: &mut SysbenchData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    while let Some((id, k, c, padding)) = data.next_row() {
        let row = Row::new(vec![
            SqlValue::Integer(id),
            SqlValue::Integer(k),
            SqlValue::Varchar(arcstr::ArcStr::from(c)),
            SqlValue::Varchar(arcstr::ArcStr::from(padding)),
        ]);
        db.insert_row("SBTEST1", row).unwrap();
    }
}

// =============================================================================
// Schema Creation - SQLite
// =============================================================================

#[cfg(feature = "sqlite-comparison")]
fn create_sbtest_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE sbtest1 (
            id INTEGER PRIMARY KEY,
            k INTEGER NOT NULL DEFAULT 0,
            c CHAR(120) NOT NULL DEFAULT '',
            padding CHAR(60) NOT NULL DEFAULT ''
        );
        CREATE INDEX k_1 ON sbtest1(k);
        "#,
    )
    .unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn load_sbtest_sqlite(conn: &SqliteConn, data: &mut SysbenchData) {
    let mut stmt =
        conn.prepare("INSERT INTO sbtest1 (id, k, c, padding) VALUES (?1, ?2, ?3, ?4)").unwrap();

    while let Some((id, k, c, padding)) = data.next_row() {
        stmt.execute(rusqlite::params![id, k, c, padding]).unwrap();
    }
}

// =============================================================================
// Schema Creation - DuckDB
// =============================================================================

#[cfg(feature = "duckdb-comparison")]
fn create_sbtest_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE sbtest1 (
            id INTEGER PRIMARY KEY,
            k INTEGER NOT NULL DEFAULT 0,
            c VARCHAR(120) NOT NULL DEFAULT '',
            padding VARCHAR(60) NOT NULL DEFAULT ''
        );
        CREATE INDEX k_1 ON sbtest1(k);
        "#,
    )
    .unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn load_sbtest_duckdb(conn: &DuckDBConn, data: &mut SysbenchData) {
    let mut stmt =
        conn.prepare("INSERT INTO sbtest1 (id, k, c, padding) VALUES (?1, ?2, ?3, ?4)").unwrap();

    while let Some((id, k, c, padding)) = data.next_row() {
        stmt.execute(duckdb::params![id, k, c, padding]).unwrap();
    }
}

// =============================================================================
// Schema Creation - MySQL
// =============================================================================

#[cfg(feature = "mysql-comparison")]
fn create_sbtest_schema_mysql(conn: &mut PooledConn) {
    // Drop table if exists
    conn.query_drop("DROP TABLE IF EXISTS sbtest1").unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE sbtest1 (
            id INTEGER PRIMARY KEY,
            k INTEGER NOT NULL DEFAULT 0,
            c VARCHAR(120) NOT NULL DEFAULT '',
            padding VARCHAR(60) NOT NULL DEFAULT ''
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    // Create index on k column
    conn.query_drop("CREATE INDEX k_1 ON sbtest1(k)").unwrap();
}

#[cfg(feature = "mysql-comparison")]
fn load_sbtest_mysql(conn: &mut PooledConn, data: &mut SysbenchData) {
    while let Some((id, k, c, padding)) = data.next_row() {
        conn.exec_drop(
            "INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)",
            (id, k, &c, &padding),
        )
        .unwrap();
    }
}
