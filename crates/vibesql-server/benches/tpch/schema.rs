// ============================================================================
// ⚠️  BENCHMARK INTEGRITY WARNING ⚠️
// ============================================================================
// DO NOT add "fast paths", "optimizations", or shortcuts that bypass SQL
// execution in benchmark code. Benchmarks MUST execute actual SQL to produce
// meaningful results. "Optimizing" benchmarks this way is cheating.
// ============================================================================

//! TPC-H Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for TPC-H
//! benchmark tables across multiple database engines (VibeSQL, SQLite, DuckDB).

use super::data::{TPCHData, NATIONS, PRIORITIES, REGIONS, SEGMENTS, SHIP_MODES};
use vibesql_storage::Database as VibeDB;
use vibesql_types::Date;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::{Pool, PooledConn};
#[cfg(feature = "sqlite-comparison")]
use rusqlite::Connection as SqliteConn;

use std::str::FromStr;

/// Batch size for bulk inserts - matches TPC-DS for consistency
const BATCH_SIZE: usize = 5000;

// =============================================================================
// Database Loaders
// =============================================================================

pub fn load_vibesql(scale_factor: f64) -> VibeDB {
    let mut db = VibeDB::new();
    let mut data = TPCHData::new(scale_factor);

    eprintln!("Loading TPC-H database (SF {})...", scale_factor);
    eprintln!("  Creating schema... ");

    // Create schema
    create_tpch_schema_vibesql(&mut db);
    eprintln!("  Creating schema... done");

    // Load data with progress logging
    eprint!("  Loading region (5 rows)... ");
    load_region_vibesql(&mut db);
    eprintln!("done");

    eprint!("  Loading nation (25 rows)... ");
    load_nation_vibesql(&mut db);
    eprintln!("done");

    eprint!("  Loading customer ({} rows)... ", data.customer_count);
    load_customer_vibesql(&mut db, &mut data);
    eprintln!("done");

    eprint!("  Loading supplier ({} rows)... ", data.supplier_count);
    load_supplier_vibesql(&mut db, &mut data);
    eprintln!("done");

    eprint!("  Loading part ({} rows)... ", data.part_count);
    load_part_vibesql(&mut db, &mut data);
    eprintln!("done");

    eprint!("  Loading partsupp ({} rows)... ", data.part_count * 4);
    load_partsupp_vibesql(&mut db, &mut data);
    eprintln!("done");

    eprint!("  Loading orders ({} rows)... ", data.orders_count);
    load_orders_vibesql(&mut db, &mut data);
    eprintln!("done");

    eprint!("  Loading lineitem ({} rows)... ", data.lineitem_count);
    load_lineitem_vibesql(&mut db, &mut data);
    eprintln!("done");

    // Create indexes to match SQLite benchmark (for fair comparison)
    eprint!("  Creating indexes... ");
    create_tpch_indexes_vibesql(&mut db);
    eprintln!("done");

    // Compute statistics for join order optimization
    // This enables the cost-based optimizer to make better decisions
    for table_name in ["region", "nation", "customer", "supplier", "orders", "lineitem"] {
        if let Some(table) = db.get_table_mut(table_name) {
            table.analyze();
        }
    }

    // Pre-warm the columnar cache for large tables used in analytical queries
    // This eliminates the ~31% row-to-columnar conversion overhead on first query
    // See: https://github.com/vibesql/vibesql/issues/2970
    let _ = db.pre_warm_columnar_cache(&[
        "LINEITEM", // Q1, Q3, Q5, Q6, Q7, Q10, Q12, Q14, Q15, Q17, Q18, Q19, Q20, Q21
        "ORDERS",   // Q3, Q4, Q5, Q7, Q8, Q9, Q10, Q12, Q13, Q18, Q21, Q22
        "CUSTOMER", // Q3, Q5, Q7, Q8, Q10, Q13, Q18, Q22
    ]);

    db
}

#[cfg(feature = "sqlite-comparison")]
pub fn load_sqlite(scale_factor: f64) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = TPCHData::new(scale_factor);

    // Create schema
    create_tpch_schema_sqlite(&conn);

    // Load data
    load_region_sqlite(&conn);
    load_nation_sqlite(&conn);
    load_customer_sqlite(&conn, &mut data);
    load_supplier_sqlite(&conn, &mut data);
    load_part_sqlite(&conn, &mut data);
    load_partsupp_sqlite(&conn, &mut data);
    load_orders_sqlite(&conn, &mut data);
    load_lineitem_sqlite(&conn, &mut data);

    conn
}

#[cfg(feature = "duckdb-comparison")]
pub fn load_duckdb(scale_factor: f64) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = TPCHData::new(scale_factor);

    // Create schema
    create_tpch_schema_duckdb(&conn);

    // Load data
    load_region_duckdb(&conn);
    load_nation_duckdb(&conn);
    load_customer_duckdb(&conn, &mut data);
    load_supplier_duckdb(&conn, &mut data);
    load_part_duckdb(&conn, &mut data);
    load_partsupp_duckdb(&conn, &mut data);
    load_orders_duckdb(&conn, &mut data);
    load_lineitem_duckdb(&conn, &mut data);

    conn
}

/// Load MySQL TPC-H database
/// Requires MYSQL_URL environment variable (e.g., mysql://root:password@localhost:3306/tpch)
/// Returns None if MYSQL_URL is not set or connection fails
#[cfg(feature = "mysql-comparison")]
pub fn load_mysql(scale_factor: f64) -> Option<PooledConn> {
    use mysql::prelude::Queryable;

    let url = std::env::var("MYSQL_URL").ok()?;
    let pool = Pool::new(url.as_str()).ok()?;
    let mut conn = pool.get_conn().ok()?;

    // Disable HeatWave secondary engine to avoid errors on complex queries
    let _ = conn.query_drop("SET SESSION use_secondary_engine=OFF");

    let mut data = TPCHData::new(scale_factor);

    // Create schema (drops and recreates tables)
    create_tpch_schema_mysql(&mut conn);

    // Load data
    load_region_mysql(&mut conn);
    load_nation_mysql(&mut conn);
    load_customer_mysql(&mut conn, &mut data);
    load_supplier_mysql(&mut conn, &mut data);
    load_part_mysql(&mut conn, &mut data);
    load_partsupp_mysql(&mut conn, &mut data);
    load_orders_mysql(&mut conn, &mut data);
    load_lineitem_mysql(&mut conn, &mut data);

    Some(conn)
}

// =============================================================================
// Schema Creation
// =============================================================================

fn create_tpch_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // REGION table
    db.create_table(TableSchema::new(
        "REGION".to_string(),
        vec![
            ColumnSchema {
                name: "R_REGIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "R_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "R_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(152) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // NATION table
    db.create_table(TableSchema::new(
        "NATION".to_string(),
        vec![
            ColumnSchema {
                name: "N_NATIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "N_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "N_REGIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "N_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(152) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CUSTOMER table
    db.create_table(TableSchema::new(
        "CUSTOMER".to_string(),
        vec![
            ColumnSchema {
                name: "C_CUSTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_ADDRESS".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_NATIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_PHONE".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_ACCTBAL".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_MKTSEGMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "C_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(117) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // ORDERS table
    db.create_table(TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema {
                name: "O_ORDERKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_CUSTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_ORDERSTATUS".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_TOTALPRICE".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_ORDERDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_ORDERPRIORITY".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_CLERK".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_SHIPPRIORITY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "O_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(79) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // LINEITEM table
    db.create_table(TableSchema::new(
        "LINEITEM".to_string(),
        vec![
            ColumnSchema {
                name: "L_ORDERKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_PARTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SUPPKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_LINENUMBER".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_QUANTITY".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_EXTENDEDPRICE".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_DISCOUNT".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_TAX".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_RETURNFLAG".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_LINESTATUS".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SHIPDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_COMMITDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_RECEIPTDATE".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SHIPINSTRUCT".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_SHIPMODE".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "L_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(44) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // SUPPLIER table
    db.create_table(TableSchema::new(
        "SUPPLIER".to_string(),
        vec![
            ColumnSchema {
                name: "S_SUPPKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_ADDRESS".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_NATIONKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_PHONE".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_ACCTBAL".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "S_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(101) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // PART table
    db.create_table(TableSchema::new(
        "PART".to_string(),
        vec![
            ColumnSchema {
                name: "P_PARTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(55) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_MFGR".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_BRAND".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_TYPE".to_string(),
                data_type: DataType::Varchar { max_length: Some(25) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_SIZE".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_CONTAINER".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_RETAILPRICE".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "P_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(23) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // PARTSUPP table
    db.create_table(TableSchema::new(
        "PARTSUPP".to_string(),
        vec![
            ColumnSchema {
                name: "PS_PARTKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "PS_SUPPKEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "PS_AVAILQTY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "PS_SUPPLYCOST".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "PS_COMMENT".to_string(),
                data_type: DataType::Varchar { max_length: Some(199) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();
}

/// Create indexes on TPC-H tables to match SQLite/DuckDB benchmark setup
///
/// This creates PRIMARY KEY equivalent indexes on the join columns that SQLite uses.
/// Without these indexes, VibeSQL must do full table scans while SQLite uses index seeks,
/// making the comparison unfair (276x performance gap on Q2).
fn create_tpch_indexes_vibesql(db: &mut VibeDB) {
    use std::time::Instant;
    use vibesql_ast::{IndexColumn, OrderDirection};

    // Region table: PRIMARY KEY (r_regionkey)
    eprint!("    idx_region_pk... ");
    let start = Instant::now();
    db.create_index(
        "idx_region_pk".to_string(),
        "REGION".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "R_REGIONKEY".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());

    // Nation table: PRIMARY KEY (n_nationkey)
    eprint!("    idx_nation_pk... ");
    let start = Instant::now();
    db.create_index(
        "idx_nation_pk".to_string(),
        "NATION".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "N_NATIONKEY".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());

    // Customer table: PRIMARY KEY (c_custkey)
    eprint!("    idx_customer_pk... ");
    let start = Instant::now();
    db.create_index(
        "idx_customer_pk".to_string(),
        "CUSTOMER".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "C_CUSTKEY".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());

    // Supplier table: PRIMARY KEY (s_suppkey)
    eprint!("    idx_supplier_pk... ");
    let start = Instant::now();
    db.create_index(
        "idx_supplier_pk".to_string(),
        "SUPPLIER".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "S_SUPPKEY".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());

    // Orders table: PRIMARY KEY (o_orderkey)
    eprint!("    idx_orders_pk... ");
    let start = Instant::now();
    db.create_index(
        "idx_orders_pk".to_string(),
        "ORDERS".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "O_ORDERKEY".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());

    // Lineitem table: PRIMARY KEY (l_orderkey, l_linenumber)
    eprint!("    idx_lineitem_pk (600K rows)... ");
    let start = Instant::now();
    db.create_index(
        "idx_lineitem_pk".to_string(),
        "LINEITEM".to_string(),
        true, // unique
        vec![
            IndexColumn {
                column_name: "L_ORDERKEY".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "L_LINENUMBER".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());

    // Part table: PRIMARY KEY (p_partkey)
    eprint!("    idx_part_pk... ");
    let start = Instant::now();
    db.create_index(
        "idx_part_pk".to_string(),
        "PART".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "P_PARTKEY".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());

    // Partsupp table: PRIMARY KEY (ps_partkey, ps_suppkey)
    eprint!("    idx_partsupp_pk... ");
    let start = Instant::now();
    db.create_index(
        "idx_partsupp_pk".to_string(),
        "PARTSUPP".to_string(),
        true, // unique
        vec![
            IndexColumn {
                column_name: "PS_PARTKEY".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "PS_SUPPKEY".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();
    eprintln!("{:?}", start.elapsed());
}

#[cfg(feature = "sqlite-comparison")]
fn create_tpch_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name TEXT NOT NULL,
            r_comment TEXT
        );

        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name TEXT NOT NULL,
            n_regionkey INTEGER NOT NULL,
            n_comment TEXT
        );

        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name TEXT NOT NULL,
            c_address TEXT NOT NULL,
            c_nationkey INTEGER NOT NULL,
            c_phone TEXT NOT NULL,
            c_acctbal REAL NOT NULL,
            c_mktsegment TEXT NOT NULL,
            c_comment TEXT
        );

        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER NOT NULL,
            o_orderstatus TEXT NOT NULL,
            o_totalprice REAL NOT NULL,
            o_orderdate TEXT NOT NULL,
            o_orderpriority TEXT NOT NULL,
            o_clerk TEXT NOT NULL,
            o_shippriority INTEGER NOT NULL,
            o_comment TEXT
        );

        CREATE TABLE lineitem (
            l_orderkey INTEGER NOT NULL,
            l_partkey INTEGER NOT NULL,
            l_suppkey INTEGER NOT NULL,
            l_linenumber INTEGER NOT NULL,
            l_quantity REAL NOT NULL,
            l_extendedprice REAL NOT NULL,
            l_discount REAL NOT NULL,
            l_tax REAL NOT NULL,
            l_returnflag TEXT NOT NULL,
            l_linestatus TEXT NOT NULL,
            l_shipdate TEXT NOT NULL,
            l_commitdate TEXT NOT NULL,
            l_receiptdate TEXT NOT NULL,
            l_shipinstruct TEXT NOT NULL,
            l_shipmode TEXT NOT NULL,
            l_comment TEXT,
            PRIMARY KEY (l_orderkey, l_linenumber)
        );

        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name TEXT NOT NULL,
            s_address TEXT NOT NULL,
            s_nationkey INTEGER NOT NULL,
            s_phone TEXT NOT NULL,
            s_acctbal REAL NOT NULL,
            s_comment TEXT
        );

        CREATE TABLE part (
            p_partkey INTEGER PRIMARY KEY,
            p_name TEXT NOT NULL,
            p_mfgr TEXT NOT NULL,
            p_brand TEXT NOT NULL,
            p_type TEXT NOT NULL,
            p_size INTEGER NOT NULL,
            p_container TEXT NOT NULL,
            p_retailprice REAL NOT NULL,
            p_comment TEXT
        );

        CREATE TABLE partsupp (
            ps_partkey INTEGER NOT NULL,
            ps_suppkey INTEGER NOT NULL,
            ps_availqty INTEGER NOT NULL,
            ps_supplycost REAL NOT NULL,
            ps_comment TEXT,
            PRIMARY KEY (ps_partkey, ps_suppkey)
        );
    "#,
    )
    .unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn create_tpch_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name VARCHAR(25) NOT NULL,
            r_comment VARCHAR(152)
        );

        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name VARCHAR(25) NOT NULL,
            n_regionkey INTEGER NOT NULL,
            n_comment VARCHAR(152)
        );

        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name VARCHAR(25) NOT NULL,
            c_address VARCHAR(40) NOT NULL,
            c_nationkey INTEGER NOT NULL,
            c_phone VARCHAR(15) NOT NULL,
            c_acctbal DECIMAL(15,2) NOT NULL,
            c_mktsegment VARCHAR(10) NOT NULL,
            c_comment VARCHAR(117)
        );

        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER NOT NULL,
            o_orderstatus VARCHAR(1) NOT NULL,
            o_totalprice DECIMAL(15,2) NOT NULL,
            o_orderdate DATE NOT NULL,
            o_orderpriority VARCHAR(15) NOT NULL,
            o_clerk VARCHAR(15) NOT NULL,
            o_shippriority INTEGER NOT NULL,
            o_comment VARCHAR(79)
        );

        CREATE TABLE lineitem (
            l_orderkey INTEGER NOT NULL,
            l_partkey INTEGER NOT NULL,
            l_suppkey INTEGER NOT NULL,
            l_linenumber INTEGER NOT NULL,
            l_quantity DECIMAL(15,2) NOT NULL,
            l_extendedprice DECIMAL(15,2) NOT NULL,
            l_discount DECIMAL(15,2) NOT NULL,
            l_tax DECIMAL(15,2) NOT NULL,
            l_returnflag VARCHAR(1) NOT NULL,
            l_linestatus VARCHAR(1) NOT NULL,
            l_shipdate DATE NOT NULL,
            l_commitdate DATE NOT NULL,
            l_receiptdate DATE NOT NULL,
            l_shipinstruct VARCHAR(25) NOT NULL,
            l_shipmode VARCHAR(10) NOT NULL,
            l_comment VARCHAR(44),
            PRIMARY KEY (l_orderkey, l_linenumber)
        );

        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name VARCHAR(25) NOT NULL,
            s_address VARCHAR(40) NOT NULL,
            s_nationkey INTEGER NOT NULL,
            s_phone VARCHAR(15) NOT NULL,
            s_acctbal DECIMAL(15,2) NOT NULL,
            s_comment VARCHAR(101)
        );

        CREATE TABLE part (
            p_partkey INTEGER PRIMARY KEY,
            p_name VARCHAR(55) NOT NULL,
            p_mfgr VARCHAR(25) NOT NULL,
            p_brand VARCHAR(10) NOT NULL,
            p_type VARCHAR(25) NOT NULL,
            p_size INTEGER NOT NULL,
            p_container VARCHAR(10) NOT NULL,
            p_retailprice DECIMAL(15,2) NOT NULL,
            p_comment VARCHAR(23)
        );

        CREATE TABLE partsupp (
            ps_partkey INTEGER NOT NULL,
            ps_suppkey INTEGER NOT NULL,
            ps_availqty INTEGER NOT NULL,
            ps_supplycost DECIMAL(15,2) NOT NULL,
            ps_comment VARCHAR(199),
            PRIMARY KEY (ps_partkey, ps_suppkey)
        );
    "#,
    )
    .unwrap();
}

#[cfg(feature = "mysql-comparison")]
fn create_tpch_schema_mysql(conn: &mut PooledConn) {
    // Drop tables if they exist (in reverse dependency order)
    let drop_tables = [
        "DROP TABLE IF EXISTS lineitem",
        "DROP TABLE IF EXISTS partsupp",
        "DROP TABLE IF EXISTS orders",
        "DROP TABLE IF EXISTS customer",
        "DROP TABLE IF EXISTS supplier",
        "DROP TABLE IF EXISTS part",
        "DROP TABLE IF EXISTS nation",
        "DROP TABLE IF EXISTS region",
    ];
    for stmt in drop_tables {
        conn.query_drop(stmt).unwrap();
    }

    conn.query_drop(
        r#"
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name VARCHAR(25) NOT NULL,
            r_comment VARCHAR(152)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name VARCHAR(25) NOT NULL,
            n_regionkey INTEGER NOT NULL,
            n_comment VARCHAR(152)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name VARCHAR(25) NOT NULL,
            c_address VARCHAR(40) NOT NULL,
            c_nationkey INTEGER NOT NULL,
            c_phone VARCHAR(15) NOT NULL,
            c_acctbal DECIMAL(15,2) NOT NULL,
            c_mktsegment VARCHAR(10) NOT NULL,
            c_comment VARCHAR(117)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER NOT NULL,
            o_orderstatus VARCHAR(1) NOT NULL,
            o_totalprice DECIMAL(15,2) NOT NULL,
            o_orderdate DATE NOT NULL,
            o_orderpriority VARCHAR(15) NOT NULL,
            o_clerk VARCHAR(15) NOT NULL,
            o_shippriority INTEGER NOT NULL,
            o_comment VARCHAR(79)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE lineitem (
            l_orderkey INTEGER NOT NULL,
            l_partkey INTEGER NOT NULL,
            l_suppkey INTEGER NOT NULL,
            l_linenumber INTEGER NOT NULL,
            l_quantity DECIMAL(15,2) NOT NULL,
            l_extendedprice DECIMAL(15,2) NOT NULL,
            l_discount DECIMAL(15,2) NOT NULL,
            l_tax DECIMAL(15,2) NOT NULL,
            l_returnflag VARCHAR(1) NOT NULL,
            l_linestatus VARCHAR(1) NOT NULL,
            l_shipdate DATE NOT NULL,
            l_commitdate DATE NOT NULL,
            l_receiptdate DATE NOT NULL,
            l_shipinstruct VARCHAR(25) NOT NULL,
            l_shipmode VARCHAR(10) NOT NULL,
            l_comment VARCHAR(44),
            PRIMARY KEY (l_orderkey, l_linenumber)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name VARCHAR(25) NOT NULL,
            s_address VARCHAR(40) NOT NULL,
            s_nationkey INTEGER NOT NULL,
            s_phone VARCHAR(15) NOT NULL,
            s_acctbal DECIMAL(15,2) NOT NULL,
            s_comment VARCHAR(101)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE part (
            p_partkey INTEGER PRIMARY KEY,
            p_name VARCHAR(55) NOT NULL,
            p_mfgr VARCHAR(25) NOT NULL,
            p_brand VARCHAR(10) NOT NULL,
            p_type VARCHAR(25) NOT NULL,
            p_size INTEGER NOT NULL,
            p_container VARCHAR(10) NOT NULL,
            p_retailprice DECIMAL(15,2) NOT NULL,
            p_comment VARCHAR(23)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE partsupp (
            ps_partkey INTEGER NOT NULL,
            ps_suppkey INTEGER NOT NULL,
            ps_availqty INTEGER NOT NULL,
            ps_supplycost DECIMAL(15,2) NOT NULL,
            ps_comment VARCHAR(199),
            PRIMARY KEY (ps_partkey, ps_suppkey)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();
}

// =============================================================================
// Data Loading (REGION - simple reference data)
// =============================================================================

fn load_region_vibesql(db: &mut VibeDB) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for (i, &name) in REGIONS.iter().enumerate() {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(name)),
            SqlValue::Varchar(arcstr::ArcStr::from("comment")),
        ]);
        db.insert_row("REGION", row).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_region_sqlite(conn: &SqliteConn) {
    for (i, &name) in REGIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO region VALUES (?, ?, ?)",
            rusqlite::params![i as i64, name, "comment"],
        )
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_region_duckdb(conn: &DuckDBConn) {
    for (i, &name) in REGIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO region VALUES (?, ?, ?)",
            duckdb::params![i as i64, name, "comment"],
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_region_mysql(conn: &mut PooledConn) {
    for (i, &name) in REGIONS.iter().enumerate() {
        conn.exec_drop("INSERT INTO region VALUES (?, ?, ?)", (i as i64, name, "comment")).unwrap();
    }
}

// =============================================================================
// Data Loading (NATION - simple reference data)
// =============================================================================

fn load_nation_vibesql(db: &mut VibeDB) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for (i, &(name, region_key)) in NATIONS.iter().enumerate() {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(name)),
            SqlValue::Integer(region_key as i64),
            SqlValue::Varchar(arcstr::ArcStr::from("comment")),
        ]);
        db.insert_row("NATION", row).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_nation_sqlite(conn: &SqliteConn) {
    for (i, &(name, region_key)) in NATIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO nation VALUES (?, ?, ?, ?)",
            rusqlite::params![i as i64, name, region_key as i64, "comment"],
        )
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_nation_duckdb(conn: &DuckDBConn) {
    for (i, &(name, region_key)) in NATIONS.iter().enumerate() {
        conn.execute(
            "INSERT INTO nation VALUES (?, ?, ?, ?)",
            duckdb::params![i as i64, name, region_key as i64, "comment"],
        )
        .unwrap();
    }
}

// =============================================================================
// Data Loading (CUSTOMER - generated data)
// =============================================================================

fn load_customer_vibesql(db: &mut VibeDB, data: &mut TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut rows = Vec::with_capacity(BATCH_SIZE);

    for i in 0..data.customer_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 17.3) % 10000.0 - 999.99;
        let row = Row::new(vec![
            SqlValue::Integer(i as i64 + 1),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("Customer#{:09}", i + 1))),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(40))),
            SqlValue::Integer(nation_key as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_phone(nation_key))),
            SqlValue::Numeric(acctbal),
            SqlValue::Varchar(arcstr::ArcStr::from(SEGMENTS[i % SEGMENTS.len()])),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(117))),
        ]);
        rows.push(row);

        if rows.len() >= BATCH_SIZE {
            db.insert_rows_batch("CUSTOMER", std::mem::take(&mut rows)).unwrap();
            rows = Vec::with_capacity(BATCH_SIZE);
        }
    }

    if !rows.is_empty() {
        db.insert_rows_batch("CUSTOMER", rows).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_customer_sqlite(conn: &SqliteConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.customer_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 17.3) % 10000.0 - 999.99;
        stmt.execute(rusqlite::params![
            i as i64 + 1,
            format!("Customer#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            SEGMENTS[i % SEGMENTS.len()],
            data.random_varchar(117),
        ])
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_customer_duckdb(conn: &DuckDBConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.customer_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 17.3) % 10000.0 - 999.99;
        stmt.execute(duckdb::params![
            i as i64 + 1,
            format!("Customer#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            SEGMENTS[i % SEGMENTS.len()],
            data.random_varchar(117),
        ])
        .unwrap();
    }
}

// =============================================================================
// Data Loading (SUPPLIER - generated data)
// =============================================================================

fn load_supplier_vibesql(db: &mut VibeDB, data: &mut TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut rows = Vec::with_capacity(BATCH_SIZE);

    for i in 0..data.supplier_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 13.7) % 10000.0 - 999.99;
        let row = Row::new(vec![
            SqlValue::Integer(i as i64 + 1),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("Supplier#{:09}", i + 1))),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(40))),
            SqlValue::Integer(nation_key as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_phone(nation_key))),
            SqlValue::Numeric(acctbal),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(101))),
        ]);
        rows.push(row);

        if rows.len() >= BATCH_SIZE {
            db.insert_rows_batch("SUPPLIER", std::mem::take(&mut rows)).unwrap();
            rows = Vec::with_capacity(BATCH_SIZE);
        }
    }

    if !rows.is_empty() {
        db.insert_rows_batch("SUPPLIER", rows).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_supplier_sqlite(conn: &SqliteConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.supplier_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 13.7) % 10000.0 - 999.99;
        stmt.execute(rusqlite::params![
            i as i64 + 1,
            format!("Supplier#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            data.random_varchar(101),
        ])
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_supplier_duckdb(conn: &DuckDBConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.supplier_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 13.7) % 10000.0 - 999.99;
        stmt.execute(duckdb::params![
            i as i64 + 1,
            format!("Supplier#{:09}", i + 1),
            data.random_varchar(40),
            nation_key as i64,
            data.random_phone(nation_key),
            acctbal,
            data.random_varchar(101),
        ])
        .unwrap();
    }
}

// =============================================================================
// Data Loading (PART - generated data)
// =============================================================================

fn load_part_vibesql(db: &mut VibeDB, data: &mut TPCHData) {
    use super::data::{COLORS, CONTAINERS, TYPES};
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut rows = Vec::with_capacity(BATCH_SIZE);

    for i in 0..data.part_count {
        let color1 = COLORS[i % COLORS.len()];
        let color2 = COLORS[(i * 7) % COLORS.len()];
        let p_name = format!("{} {} {}", color1, TYPES[i % TYPES.len()], color2);
        let retailprice = (90000.0 + (i as f64 / 10.0) % 10000.0) / 100.0;
        let row = Row::new(vec![
            SqlValue::Integer(i as i64 + 1),
            SqlValue::Varchar(arcstr::ArcStr::from(p_name)),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("Manufacturer#{}", (i % 5) + 1))),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("Brand#{}{}", (i % 5) + 1, (i / 5 % 5) + 1))),
            SqlValue::Varchar(arcstr::ArcStr::from(TYPES[i % TYPES.len()].to_string())),
            SqlValue::Integer(((i % 50) + 1) as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(CONTAINERS[i % CONTAINERS.len()].to_string())),
            SqlValue::Numeric(retailprice),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(23))),
        ]);
        rows.push(row);

        if rows.len() >= BATCH_SIZE {
            db.insert_rows_batch("PART", std::mem::take(&mut rows)).unwrap();
            rows = Vec::with_capacity(BATCH_SIZE);
        }
    }

    if !rows.is_empty() {
        db.insert_rows_batch("PART", rows).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_part_sqlite(conn: &SqliteConn, data: &mut TPCHData) {
    use super::data::{COLORS, CONTAINERS, TYPES};

    let mut stmt = conn.prepare("INSERT INTO part VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.part_count {
        let color1 = COLORS[i % COLORS.len()];
        let color2 = COLORS[(i * 7) % COLORS.len()];
        let p_name = format!("{} {} {}", color1, TYPES[i % TYPES.len()], color2);
        let retailprice = (90000.0 + (i as f64 / 10.0) % 10000.0) / 100.0;

        stmt.execute(rusqlite::params![
            i as i64 + 1,
            p_name,
            format!("Manufacturer#{}", (i % 5) + 1),
            format!("Brand#{}{}", (i % 5) + 1, (i / 5 % 5) + 1),
            TYPES[i % TYPES.len()],
            ((i % 50) + 1) as i64,
            CONTAINERS[i % CONTAINERS.len()],
            retailprice,
            data.random_varchar(23),
        ])
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_part_duckdb(conn: &DuckDBConn, data: &mut TPCHData) {
    use super::data::{COLORS, CONTAINERS, TYPES};

    let mut stmt = conn.prepare("INSERT INTO part VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.part_count {
        let color1 = COLORS[i % COLORS.len()];
        let color2 = COLORS[(i * 7) % COLORS.len()];
        let p_name = format!("{} {} {}", color1, TYPES[i % TYPES.len()], color2);
        let retailprice = (90000.0 + (i as f64 / 10.0) % 10000.0) / 100.0;

        stmt.execute(duckdb::params![
            i as i64 + 1,
            p_name,
            format!("Manufacturer#{}", (i % 5) + 1),
            format!("Brand#{}{}", (i % 5) + 1, (i / 5 % 5) + 1),
            TYPES[i % TYPES.len()],
            ((i % 50) + 1) as i64,
            CONTAINERS[i % CONTAINERS.len()],
            retailprice,
            data.random_varchar(23),
        ])
        .unwrap();
    }
}

// =============================================================================
// Data Loading (PARTSUPP - generated data)
// =============================================================================

fn load_partsupp_vibesql(db: &mut VibeDB, data: &mut TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut rows = Vec::with_capacity(BATCH_SIZE);

    // Each part is supplied by 4 suppliers
    for part_key in 1..=data.part_count {
        for j in 0..4 {
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, j);
            let availqty = ((part_key * 17 + j * 31) % 9999) + 1;
            let supplycost = ((part_key * 13 + j * 7) % 100000) as f64 / 100.0 + 1.0;
            let row = Row::new(vec![
                SqlValue::Integer(part_key as i64),
                SqlValue::Integer(supp_key as i64),
                SqlValue::Integer(availqty as i64),
                SqlValue::Numeric(supplycost),
                SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(199))),
            ]);
            rows.push(row);

            if rows.len() >= BATCH_SIZE {
                db.insert_rows_batch("PARTSUPP", std::mem::take(&mut rows)).unwrap();
                rows = Vec::with_capacity(BATCH_SIZE);
            }
        }
    }

    if !rows.is_empty() {
        db.insert_rows_batch("PARTSUPP", rows).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_partsupp_sqlite(conn: &SqliteConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO partsupp VALUES (?, ?, ?, ?, ?)").unwrap();

    // Each part is supplied by 4 suppliers
    for part_key in 1..=data.part_count {
        for j in 0..4 {
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, j);
            let availqty = ((part_key * 17 + j * 31) % 9999) + 1;
            let supplycost = ((part_key * 13 + j * 7) % 100000) as f64 / 100.0 + 1.0;

            stmt.execute(rusqlite::params![
                part_key as i64,
                supp_key as i64,
                availqty as i64,
                supplycost,
                data.random_varchar(199),
            ])
            .unwrap();
        }
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_partsupp_duckdb(conn: &DuckDBConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO partsupp VALUES (?, ?, ?, ?, ?)").unwrap();

    // Each part is supplied by 4 suppliers
    for part_key in 1..=data.part_count {
        for j in 0..4 {
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, j);
            let availqty = ((part_key * 17 + j * 31) % 9999) + 1;
            let supplycost = ((part_key * 13 + j * 7) % 100000) as f64 / 100.0 + 1.0;

            stmt.execute(duckdb::params![
                part_key as i64,
                supp_key as i64,
                availqty as i64,
                supplycost,
                data.random_varchar(199),
            ])
            .unwrap();
        }
    }
}

// =============================================================================
// Data Loading (ORDERS - generated data)
// =============================================================================

fn load_orders_vibesql(db: &mut VibeDB, data: &mut TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut rows = Vec::with_capacity(BATCH_SIZE);

    for i in 0..data.orders_count {
        let cust_key = (i % data.customer_count) + 1;
        let totalprice = (i as f64 * 271.3) % 500000.0 + 1000.0;
        let order_date = data.random_date("1992-01-01", "1998-12-31");

        let row = Row::new(vec![
            SqlValue::Integer(i as i64 + 1),
            SqlValue::Integer(cust_key as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(["O", "F", "P"][i % 3])),
            SqlValue::Numeric(totalprice),
            SqlValue::Date(Date::from_str(&order_date).unwrap()),
            SqlValue::Varchar(arcstr::ArcStr::from(PRIORITIES[i % PRIORITIES.len()].to_string())),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("Clerk#{:09}", (i * 7) % 1000 + 1))),
            SqlValue::Integer(0),
            SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(79))),
        ]);
        rows.push(row);

        if rows.len() >= BATCH_SIZE {
            db.insert_rows_batch("ORDERS", std::mem::take(&mut rows)).unwrap();
            rows = Vec::with_capacity(BATCH_SIZE);
        }
    }

    if !rows.is_empty() {
        db.insert_rows_batch("ORDERS", rows).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_orders_sqlite(conn: &SqliteConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.orders_count {
        let cust_key = (i % data.customer_count) + 1;
        let totalprice = (i as f64 * 271.3) % 500000.0 + 1000.0;
        let order_date = data.random_date("1992-01-01", "1998-12-31");

        stmt.execute(rusqlite::params![
            i as i64 + 1,
            cust_key as i64,
            ["O", "F", "P"][i % 3],
            totalprice,
            order_date,
            PRIORITIES[i % PRIORITIES.len()],
            format!("Clerk#{:09}", (i * 7) % 1000 + 1),
            0,
            data.random_varchar(79),
        ])
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_orders_duckdb(conn: &DuckDBConn, data: &mut TPCHData) {
    let mut stmt = conn.prepare("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    for i in 0..data.orders_count {
        let cust_key = (i % data.customer_count) + 1;
        let totalprice = (i as f64 * 271.3) % 500000.0 + 1000.0;
        let order_date = data.random_date("1992-01-01", "1998-12-31");

        stmt.execute(duckdb::params![
            i as i64 + 1,
            cust_key as i64,
            ["O", "F", "P"][i % 3],
            totalprice,
            order_date,
            PRIORITIES[i % PRIORITIES.len()],
            format!("Clerk#{:09}", (i * 7) % 1000 + 1),
            0,
            data.random_varchar(79),
        ])
        .unwrap();
    }
}

// =============================================================================
// Data Loading (LINEITEM - generated data, largest table)
// =============================================================================

/// Returns one of the 4 valid supplier keys for a given part_key.
/// This matches the supplier generation logic in load_partsupp.
///
/// Uses a formula that guarantees 4 unique suppliers per part at any scale factor.
/// The base supplier is determined by (part_key - 1) % supplier_count, then
/// we add evenly-spaced offsets (0, 1/4, 2/4, 3/4 of supplier_count) for each j.
fn get_valid_supplier_for_part(
    part_key: usize,
    supplier_count: usize,
    supplier_idx: usize,
) -> usize {
    let j = supplier_idx % 4; // 0-3
    let base = (part_key - 1) % supplier_count;
    let offset = (j * supplier_count) / 4;
    ((base + offset) % supplier_count) + 1
}

fn load_lineitem_vibesql(db: &mut VibeDB, data: &mut TPCHData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut rows = Vec::with_capacity(BATCH_SIZE);
    let mut line_id = 0;

    'outer: for order_num in 1..=data.orders_count {
        let num_lines = (order_num * 3 % 7) + 1; // 1-7 lines per order

        for line_num in 1..=num_lines {
            if line_id >= data.lineitem_count {
                break 'outer;
            }

            let part_key = (line_id * 13) % data.part_count + 1;
            // Use a valid supplier for this part (must match partsupp FK constraint)
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, line_id);

            let quantity = ((line_id * 11) % 50 + 1) as f64;
            let extendedprice = quantity * ((line_id * 97) as f64 % 100000.0 + 900.0);
            let discount = ((line_id * 7) % 10) as f64 / 100.0;
            let tax = ((line_id * 3) % 8) as f64 / 100.0;
            let ship_date = data.random_date("1992-01-01", "1998-12-31");
            let commit_date = data.random_date("1992-01-01", "1998-12-31");
            let receipt_date = data.random_date("1992-01-01", "1998-12-31");

            let row = Row::new(vec![
                SqlValue::Integer(order_num as i64),
                SqlValue::Integer(part_key as i64),
                SqlValue::Integer(supp_key as i64),
                SqlValue::Integer(line_num as i64),
                SqlValue::Numeric(quantity),
                SqlValue::Numeric(extendedprice),
                SqlValue::Numeric(discount),
                SqlValue::Numeric(tax),
                SqlValue::Varchar(arcstr::ArcStr::from(["N", "R", "A"][line_id % 3])),
                SqlValue::Varchar(arcstr::ArcStr::from(["O", "F"][line_id % 2])),
                SqlValue::Date(Date::from_str(&ship_date).unwrap()),
                SqlValue::Date(Date::from_str(&commit_date).unwrap()),
                SqlValue::Date(Date::from_str(&receipt_date).unwrap()),
                SqlValue::Varchar(arcstr::ArcStr::from("DELIVER IN PERSON")),
                SqlValue::Varchar(arcstr::ArcStr::from(SHIP_MODES[line_id % SHIP_MODES.len()].to_string())),
                SqlValue::Varchar(arcstr::ArcStr::from(data.random_varchar(44))),
            ]);
            rows.push(row);

            if rows.len() >= BATCH_SIZE {
                db.insert_rows_batch("LINEITEM", std::mem::take(&mut rows)).unwrap();
                rows = Vec::with_capacity(BATCH_SIZE);
            }

            line_id += 1;
        }
    }

    if !rows.is_empty() {
        db.insert_rows_batch("LINEITEM", rows).unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_lineitem_sqlite(conn: &SqliteConn, data: &mut TPCHData) {
    let mut stmt = conn
        .prepare("INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .unwrap();

    let mut line_id = 0;
    for order_num in 1..=data.orders_count {
        let num_lines = (order_num * 3 % 7) + 1;

        for line_num in 1..=num_lines {
            if line_id >= data.lineitem_count {
                break;
            }

            let part_key = (line_id * 13) % data.part_count + 1;
            // Use a valid supplier for this part (must match partsupp FK constraint)
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, line_id);

            let quantity = ((line_id * 11) % 50 + 1) as f64;
            let extendedprice = quantity * ((line_id * 97) as f64 % 100000.0 + 900.0);
            let discount = ((line_id * 7) % 10) as f64 / 100.0;
            let tax = ((line_id * 3) % 8) as f64 / 100.0;
            let ship_date = data.random_date("1992-01-01", "1998-12-31");
            let commit_date = data.random_date("1992-01-01", "1998-12-31");
            let receipt_date = data.random_date("1992-01-01", "1998-12-31");

            stmt.execute(rusqlite::params![
                order_num as i64,
                part_key as i64,
                supp_key as i64,
                line_num as i64,
                quantity,
                extendedprice,
                discount,
                tax,
                ["N", "R", "A"][line_id % 3],
                ["O", "F"][line_id % 2],
                ship_date,
                commit_date,
                receipt_date,
                "DELIVER IN PERSON",
                SHIP_MODES[line_id % SHIP_MODES.len()],
                data.random_varchar(44),
            ])
            .unwrap();

            line_id += 1;
        }
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_lineitem_duckdb(conn: &DuckDBConn, data: &mut TPCHData) {
    let mut stmt = conn
        .prepare("INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .unwrap();

    let mut line_id = 0;
    for order_num in 1..=data.orders_count {
        let num_lines = (order_num * 3 % 7) + 1;

        for line_num in 1..=num_lines {
            if line_id >= data.lineitem_count {
                break;
            }

            let part_key = (line_id * 13) % data.part_count + 1;
            // Use a valid supplier for this part (must match partsupp FK constraint)
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, line_id);

            let quantity = ((line_id * 11) % 50 + 1) as f64;
            let extendedprice = quantity * ((line_id * 97) as f64 % 100000.0 + 900.0);
            let discount = ((line_id * 7) % 10) as f64 / 100.0;
            let tax = ((line_id * 3) % 8) as f64 / 100.0;
            let ship_date = data.random_date("1992-01-01", "1998-12-31");
            let commit_date = data.random_date("1992-01-01", "1998-12-31");
            let receipt_date = data.random_date("1992-01-01", "1998-12-31");

            stmt.execute(duckdb::params![
                order_num as i64,
                part_key as i64,
                supp_key as i64,
                line_num as i64,
                quantity,
                extendedprice,
                discount,
                tax,
                ["N", "R", "A"][line_id % 3],
                ["O", "F"][line_id % 2],
                ship_date,
                commit_date,
                receipt_date,
                "DELIVER IN PERSON",
                SHIP_MODES[line_id % SHIP_MODES.len()],
                data.random_varchar(44),
            ])
            .unwrap();

            line_id += 1;
        }
    }
}

// =============================================================================
// MySQL Data Loading Functions
// =============================================================================

#[cfg(feature = "mysql-comparison")]
fn load_nation_mysql(conn: &mut PooledConn) {
    for (i, &(name, region_key)) in NATIONS.iter().enumerate() {
        conn.exec_drop(
            "INSERT INTO nation VALUES (?, ?, ?, ?)",
            (i as i64, name, region_key as i64, "comment"),
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_customer_mysql(conn: &mut PooledConn, data: &mut TPCHData) {
    for i in 0..data.customer_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 17.3) % 10000.0 - 999.99;
        conn.exec_drop(
            "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                i as i64 + 1,
                format!("Customer#{:09}", i + 1),
                data.random_varchar(40),
                nation_key as i64,
                data.random_phone(nation_key),
                acctbal,
                SEGMENTS[i % SEGMENTS.len()],
                data.random_varchar(117),
            ),
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_supplier_mysql(conn: &mut PooledConn, data: &mut TPCHData) {
    for i in 0..data.supplier_count {
        let nation_key = i % 25;
        let acctbal = (i as f64 * 13.7) % 10000.0 - 999.99;
        conn.exec_drop(
            "INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                i as i64 + 1,
                format!("Supplier#{:09}", i + 1),
                data.random_varchar(40),
                nation_key as i64,
                data.random_phone(nation_key),
                acctbal,
                data.random_varchar(101),
            ),
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_part_mysql(conn: &mut PooledConn, data: &mut TPCHData) {
    use super::data::{COLORS, CONTAINERS, TYPES};

    for i in 0..data.part_count {
        let color1 = COLORS[i % COLORS.len()];
        let color2 = COLORS[(i * 7) % COLORS.len()];
        let p_name = format!("{} {} {}", color1, TYPES[i % TYPES.len()], color2);
        let retailprice = (90000.0 + (i as f64 / 10.0) % 10000.0) / 100.0;

        conn.exec_drop(
            "INSERT INTO part VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                i as i64 + 1,
                p_name,
                format!("Manufacturer#{}", (i % 5) + 1),
                format!("Brand#{}{}", (i % 5) + 1, (i / 5 % 5) + 1),
                TYPES[i % TYPES.len()],
                ((i % 50) + 1) as i64,
                CONTAINERS[i % CONTAINERS.len()],
                retailprice,
                data.random_varchar(23),
            ),
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_partsupp_mysql(conn: &mut PooledConn, data: &mut TPCHData) {
    for part_key in 1..=data.part_count {
        for j in 0..4 {
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, j);
            let availqty = ((part_key * 17 + j * 31) % 9999) + 1;
            let supplycost = ((part_key * 13 + j * 7) % 100000) as f64 / 100.0 + 1.0;

            conn.exec_drop(
                "INSERT INTO partsupp VALUES (?, ?, ?, ?, ?)",
                (
                    part_key as i64,
                    supp_key as i64,
                    availqty as i64,
                    supplycost,
                    data.random_varchar(199),
                ),
            )
            .unwrap();
        }
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_orders_mysql(conn: &mut PooledConn, data: &mut TPCHData) {
    for i in 0..data.orders_count {
        let cust_key = (i % data.customer_count) + 1;
        let totalprice = (i as f64 * 271.3) % 500000.0 + 1000.0;
        let order_date = data.random_date("1992-01-01", "1998-12-31");

        conn.exec_drop(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                i as i64 + 1,
                cust_key as i64,
                ["O", "F", "P"][i % 3],
                totalprice,
                order_date,
                PRIORITIES[i % PRIORITIES.len()],
                format!("Clerk#{:09}", (i * 7) % 1000 + 1),
                0,
                data.random_varchar(79),
            ),
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_lineitem_mysql(conn: &mut PooledConn, data: &mut TPCHData) {
    use mysql::Value;

    let mut line_id = 0;
    for order_num in 1..=data.orders_count {
        let num_lines = (order_num * 3 % 7) + 1;

        for line_num in 1..=num_lines {
            if line_id >= data.lineitem_count {
                break;
            }

            let part_key = (line_id * 13) % data.part_count + 1;
            let supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, line_id);

            let quantity = ((line_id * 11) % 50 + 1) as f64;
            let extendedprice = quantity * ((line_id * 97) as f64 % 100000.0 + 900.0);
            let discount = ((line_id * 7) % 10) as f64 / 100.0;
            let tax = ((line_id * 3) % 8) as f64 / 100.0;
            let ship_date = data.random_date("1992-01-01", "1998-12-31");
            let commit_date = data.random_date("1992-01-01", "1998-12-31");
            let receipt_date = data.random_date("1992-01-01", "1998-12-31");

            // MySQL Params doesn't support 16-element tuples, so use Vec<Value>
            let params: Vec<Value> = vec![
                Value::from(order_num as i64),
                Value::from(part_key as i64),
                Value::from(supp_key as i64),
                Value::from(line_num as i64),
                Value::from(quantity),
                Value::from(extendedprice),
                Value::from(discount),
                Value::from(tax),
                Value::from(["N", "R", "A"][line_id % 3]),
                Value::from(["O", "F"][line_id % 2]),
                Value::from(ship_date),
                Value::from(commit_date),
                Value::from(receipt_date),
                Value::from("DELIVER IN PERSON"),
                Value::from(SHIP_MODES[line_id % SHIP_MODES.len()]),
                Value::from(data.random_varchar(44)),
            ];

            conn.exec_drop(
                "INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params,
            )
            .unwrap();

            line_id += 1;
        }
    }
}
