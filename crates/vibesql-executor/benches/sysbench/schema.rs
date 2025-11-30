//! Sysbench Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for sysbench
//! benchmark tables across multiple database engines (VibeSQL, SQLite, DuckDB).

use super::data::SysbenchData;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
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
#[cfg(feature = "benchmark-comparison")]
pub fn load_sqlite(table_size: usize) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = SysbenchData::new(table_size);

    // Create schema
    create_sbtest_schema_sqlite(&conn);

    // Load data
    load_sbtest_sqlite(&conn, &mut data);

    conn
}

/// Load DuckDB with sysbench schema and data
#[cfg(feature = "benchmark-comparison")]
pub fn load_duckdb(table_size: usize) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = SysbenchData::new(table_size);

    // Create schema
    create_sbtest_schema_duckdb(&conn);

    // Load data
    load_sbtest_duckdb(&conn, &mut data);

    conn
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
            ColumnSchema {
                name: "pad".to_string(),
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

    while let Some((id, k, c, pad)) = data.next_row() {
        let row = Row::new(vec![
            SqlValue::Integer(id),
            SqlValue::Integer(k),
            SqlValue::Varchar(c),
            SqlValue::Varchar(pad),
        ]);
        db.insert_row("SBTEST1", row).unwrap();
    }
}

// =============================================================================
// Schema Creation - SQLite
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn create_sbtest_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE sbtest1 (
            id INTEGER PRIMARY KEY,
            k INTEGER NOT NULL DEFAULT 0,
            c CHAR(120) NOT NULL DEFAULT '',
            pad CHAR(60) NOT NULL DEFAULT ''
        );
        CREATE INDEX k_1 ON sbtest1(k);
        "#,
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_sbtest_sqlite(conn: &SqliteConn, data: &mut SysbenchData) {
    let mut stmt = conn
        .prepare("INSERT INTO sbtest1 (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)")
        .unwrap();

    while let Some((id, k, c, pad)) = data.next_row() {
        stmt.execute(rusqlite::params![id, k, c, pad]).unwrap();
    }
}

// =============================================================================
// Schema Creation - DuckDB
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn create_sbtest_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE sbtest1 (
            id INTEGER PRIMARY KEY,
            k INTEGER NOT NULL DEFAULT 0,
            c VARCHAR(120) NOT NULL DEFAULT '',
            pad VARCHAR(60) NOT NULL DEFAULT ''
        );
        CREATE INDEX k_1 ON sbtest1(k);
        "#,
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_sbtest_duckdb(conn: &DuckDBConn, data: &mut SysbenchData) {
    let mut stmt = conn
        .prepare("INSERT INTO sbtest1 (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)")
        .unwrap();

    while let Some((id, k, c, pad)) = data.next_row() {
        stmt.execute(duckdb::params![id, k, c, pad]).unwrap();
    }
}
