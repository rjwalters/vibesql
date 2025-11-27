//! Sysbench Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for sysbench
//! OLTP benchmarks across multiple database engines (VibeSQL, SQLite, DuckDB).
//!
//! The schema matches the standard sysbench `sbtest` table:
//! ```sql
//! CREATE TABLE sbtest1 (
//!   id INTEGER NOT NULL PRIMARY KEY,
//!   k INTEGER NOT NULL DEFAULT 0,
//!   c CHAR(120) NOT NULL DEFAULT '',
//!   pad CHAR(60) NOT NULL DEFAULT ''
//! );
//! CREATE INDEX k_idx ON sbtest1(k);
//! ```

use super::data::SysbenchData;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;

// =============================================================================
// Database Loaders
// =============================================================================

/// Load a VibeSQL database with sysbench schema and data.
pub fn load_vibesql(table_size: usize) -> VibeDB {
    let mut db = VibeDB::new();
    let mut data = SysbenchData::new(table_size);

    create_sysbench_schema_vibesql(&mut db);
    load_sbtest_vibesql(&mut db, &mut data);
    create_sysbench_indexes_vibesql(&mut db);

    // Compute statistics for query optimization
    if let Some(table) = db.get_table_mut("SBTEST1") {
        table.analyze();
    }

    db
}

/// Load a SQLite database with sysbench schema and data.
#[cfg(feature = "benchmark-comparison")]
pub fn load_sqlite(table_size: usize) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = SysbenchData::new(table_size);

    create_sysbench_schema_sqlite(&conn);
    load_sbtest_sqlite(&conn, &mut data);

    conn
}

/// Load a DuckDB database with sysbench schema and data.
#[cfg(feature = "benchmark-comparison")]
pub fn load_duckdb(table_size: usize) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = SysbenchData::new(table_size);

    create_sysbench_schema_duckdb(&conn);
    load_sbtest_duckdb(&conn, &mut data);

    conn
}

// =============================================================================
// Schema Creation
// =============================================================================

fn create_sysbench_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // Table/column names are uppercase to match SQL parser behavior
    // (SQL identifiers are case-insensitive and normalized to uppercase)
    db.create_table(TableSchema::new(
        "SBTEST1".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "K".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(120),
                },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "PAD".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(60),
                },
                nullable: false,
                default_value: None,
            },
        ],
    ))
    .unwrap();
}

fn create_sysbench_indexes_vibesql(db: &mut VibeDB) {
    use vibesql_ast::{IndexColumn, OrderDirection};

    // Primary key index on id
    db.create_index(
        "idx_sbtest1_pk".to_string(),
        "SBTEST1".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "ID".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Secondary index on k (standard sysbench index)
    db.create_index(
        "k_idx".to_string(),
        "SBTEST1".to_string(),
        false, // not unique
        vec![IndexColumn {
            column_name: "K".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn create_sysbench_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE sbtest1 (
            id INTEGER NOT NULL PRIMARY KEY,
            k INTEGER NOT NULL DEFAULT 0,
            c TEXT NOT NULL DEFAULT '',
            pad TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX k_idx ON sbtest1(k);
    "#,
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn create_sysbench_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE sbtest1 (
            id INTEGER NOT NULL PRIMARY KEY,
            k INTEGER NOT NULL DEFAULT 0,
            c VARCHAR(120) NOT NULL DEFAULT '',
            pad VARCHAR(60) NOT NULL DEFAULT ''
        );
        CREATE INDEX k_idx ON sbtest1(k);
    "#,
    )
    .unwrap();
}

// =============================================================================
// Data Loading
// =============================================================================

fn load_sbtest_vibesql(db: &mut VibeDB, data: &mut SysbenchData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.table_size {
        // k is a random value in [1, table_size] (sysbench default)
        let k = (i * 499) % data.table_size + 1; // Deterministic pseudo-random

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer(k as i64),
            SqlValue::Varchar(data.generate_c()),
            SqlValue::Varchar(data.generate_pad()),
        ]);
        db.insert_row("SBTEST1", row).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_sbtest_sqlite(conn: &SqliteConn, data: &mut SysbenchData) {
    let mut stmt = conn
        .prepare("INSERT INTO sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?)")
        .unwrap();

    for i in 1..=data.table_size {
        let k = (i * 499) % data.table_size + 1;

        stmt.execute(rusqlite::params![
            i as i64,
            k as i64,
            data.generate_c(),
            data.generate_pad(),
        ])
        .unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_sbtest_duckdb(conn: &DuckDBConn, data: &mut SysbenchData) {
    let mut stmt = conn
        .prepare("INSERT INTO sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?)")
        .unwrap();

    for i in 1..=data.table_size {
        let k = (i * 499) % data.table_size + 1;

        stmt.execute(duckdb::params![
            i as i64,
            k as i64,
            data.generate_c(),
            data.generate_pad(),
        ])
        .unwrap();
    }
}
